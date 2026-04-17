//! Serialized sync engine state.
//!
//! For now, the engine owns the simple bootstrap path: build the current tree from the local
//! sync root and publish it to the coordinator. Runtime-driven local change handling can layer on
//! top later without changing the core startup flow.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::core::{
    BlobHash, ChangeSet, Config, DeviceId, DeviceState, File, Object, ObjectId, Snapshot,
    SnapshotAnnouncement, SnapshotHash, Tree, TreeDiff,
};
use crate::index::{TreeIndex, TreeUpdate};
use crate::local::build_snapshot;
use crate::local::util::{encode_hash, walk_files};
use crate::services::{
    Applier, ApplyOp, ApplyPlan, BlobStore, BlobTransferWorker, Chunker, Coordinator,
    CoordinatorNotification, MetaStore, ObjStore, Result, TreeBuilder, WatcherEvent,
};

/// Serialized device-side sync state machine.
///
/// The engine stays single-threaded and owns the local sync state. Startup is intentionally
/// direct: read the local tree, persist the immutable objects, and publish a snapshot if the
/// published tip changed.
pub struct SyncEngine {
    pub config: Config,
    pub state: DeviceState,
    pub tree: Tree,
    pub index: TreeIndex,
    pub meta_store: Arc<dyn MetaStore>,
    pub obj_store: Arc<dyn ObjStore>,
    pub blob_store: Arc<dyn BlobStore>,
    pub blob_worker: Arc<dyn BlobTransferWorker>,
    pub applier: Arc<dyn Applier>,
    pub coordinator: Arc<dyn Coordinator>,
    pub tree_builder: Arc<dyn TreeBuilder>,
    pub chunker: Arc<dyn Chunker>,
}

/// Upload or download request for referenced blob content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobTransferJob {
    pub snapshot: SnapshotHash,
    pub blobs: Vec<BlobHash>,
}

/// Outcome of a blob upload or download batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobTransferResult {
    pub snapshot: SnapshotHash,
    pub blobs: Vec<BlobHash>,
}

/// Request to apply a reconciled filesystem plan locally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyJob {
    pub plan: ApplyPlan,
}

/// Result of building one local file version and staging any new blobs it references.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileBuild {
    file: File,
}

/// Runtime event delivered into the serialized sync engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncEvent {
    Local(WatcherEvent),
    Remote(CoordinatorNotification),
}

impl SyncEngine {
    /// Returns the known remote tips that differ from the current local snapshot.
    fn candidate_remote_tip_set(&self) -> Vec<(DeviceId, SnapshotHash)> {
        let mut tips = Vec::new();
        for (device_id, snapshot) in &self.state.frontier.device_snapshots {
            if device_id == &self.config.device_id {
                continue;
            }
            if *snapshot == [0; 32] || *snapshot == self.state.snapshot {
                continue;
            }
            tips.push((device_id.clone(), *snapshot));
        }
        tips.sort();
        tips
    }

    /// Returns the maximal remote frontier tips after pruning tips that are behind others.
    pub async fn candidate_remote_tips(&self) -> Result<Vec<(DeviceId, SnapshotHash)>> {
        let tips = self.candidate_remote_tip_set();
        let mut maximal = Vec::new();

        for (device_id, snapshot) in &tips {
            let mut dominated = false;
            for (_, other_snapshot) in &tips {
                if snapshot == other_snapshot {
                    continue;
                }
                if self
                    .is_snapshot_ancestor(*snapshot, *other_snapshot)
                    .await?
                {
                    dominated = true;
                    break;
                }
            }

            if !dominated {
                maximal.push((device_id.clone(), *snapshot));
            }
        }

        maximal.sort();
        Ok(maximal)
    }

    /// Picks the next maximal remote frontier tip to reconcile toward.
    pub async fn next_remote_target(&self) -> Result<Option<(DeviceId, SnapshotHash)>> {
        Ok(self.candidate_remote_tips().await?.into_iter().next())
    }

    /// Builds a basic fast-forward apply plan for the next selected remote target.
    pub async fn reconcile_next_remote_target(&self) -> Result<Option<ApplyPlan>> {
        let Some((device_id, target)) = self.next_remote_target().await? else {
            return Ok(None);
        };

        self.plan_remote_reconcile(&device_id, target).await
    }

    pub async fn open(
        config: Config,
        meta_store: Arc<dyn MetaStore>,
        obj_store: Arc<dyn ObjStore>,
        blob_store: Arc<dyn BlobStore>,
        blob_worker: Arc<dyn BlobTransferWorker>,
        applier: Arc<dyn Applier>,
        coordinator: Arc<dyn Coordinator>,
        tree_builder: Arc<dyn TreeBuilder>,
        chunker: Arc<dyn Chunker>,
    ) -> Result<Self> {
        let state = meta_store
            .load_state(&config.repo_id, &config.device_id)
            .await?;
        let current_tree = if state.snapshot == [0; 32] {
            Tree::empty()
        } else {
            Self::load_saved_tree(&*obj_store, state.snapshot).await?
        };

        Ok(Self {
            config,
            state,
            tree: current_tree,
            index: TreeIndex::empty(),
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            coordinator,
            tree_builder,
            chunker,
        })
    }

    /// Builds the current local tree and publishes a new snapshot if it changed.
    pub async fn start(&mut self) -> Result<SnapshotHash> {
        self.log("registering device with coordinator");
        self.coordinator.register_device(&self.config).await?;
        self.full_sync("startup").await
    }

    /// Applies one runtime sync event.
    pub async fn handle_event(&mut self, event: SyncEvent) -> Result<Option<SnapshotHash>> {
        match event {
            SyncEvent::Local(WatcherEvent::FileChanged(path)) => {
                self.log(&format!("local file changed: {}", path.display()));
                self.sync_changed_file(&path).await.map(Some)
            }
            SyncEvent::Local(WatcherEvent::FileDeleted(path)) => {
                self.log(&format!("local file deleted: {}", path.display()));
                self.sync_removed_path(&path, "local file delete")
                    .await
                    .map(Some)
            }
            SyncEvent::Local(WatcherEvent::DirectoryCreated(path)) => {
                self.log(&format!("local directory created: {}", path.display()));
                self.sync_created_directory(&path).await.map(Some)
            }
            SyncEvent::Local(WatcherEvent::DirectoryDeleted(path)) => {
                self.log(&format!("local directory deleted: {}", path.display()));
                self.sync_removed_path(&path, "local directory delete")
                    .await
                    .map(Some)
            }
            SyncEvent::Local(WatcherEvent::PathMoved { from, to }) => {
                self.log(&format!(
                    "local path moved: {} -> {}",
                    from.display(),
                    to.display()
                ));
                self.sync_moved_path(&from, &to).await.map(Some)
            }
            SyncEvent::Local(WatcherEvent::RescanRequested) => {
                self.log("local rescan requested");
                Ok(Some(self.full_sync("local rescan").await?))
            }
            SyncEvent::Remote(notification) => {
                self.handle_remote_notification(notification).await?;
                Ok(None)
            }
        }
    }

    async fn handle_remote_notification(
        &mut self,
        notification: CoordinatorNotification,
    ) -> Result<()> {
        match notification {
            CoordinatorNotification::Snapshot(announcement) => {
                self.handle_remote_snapshot(announcement).await
            }
            CoordinatorNotification::PeerAvailable(peer) => {
                self.log(&format!("peer available: {:?}", peer));
                self.state
                    .frontier
                    .device_snapshots
                    .insert(peer.device, peer.snapshot);
                Ok(())
            }
        }
    }

    async fn handle_remote_snapshot(&mut self, announcement: SnapshotAnnouncement) -> Result<()> {
        if announcement.repo_id != self.config.repo_id {
            self.log(&format!(
                "ignoring remote snapshot for different repo: {}",
                announcement.repo_id
            ));
            return Ok(());
        }

        if announcement.device == self.config.device_id {
            self.log("ignoring remote snapshot from local device");
            return Ok(());
        }

        self.log(&format!(
            "remote snapshot announced by {}: {}",
            announcement.device,
            encode_hash(&announcement.snapshot)
        ));

        let snapshot = self
            .coordinator
            .fetch_snapshot(&self.config.repo_id, &announcement.snapshot)
            .await?;
        let base = snapshot.parents.first().copied().unwrap_or([0; 32]);
        let change_set = self
            .coordinator
            .fetch_change_set(&self.config.repo_id, &base, &snapshot.hash)
            .await?;
        self.obj_store
            .save_object(&Object::Snapshot(snapshot.clone()))
            .await?;

        self.log(&format!(
            "fetched remote snapshot {} with base {}",
            encode_hash(&snapshot.hash),
            encode_hash(&change_set.base)
        ));
        self.state
            .frontier
            .device_snapshots
            .insert(announcement.device.clone(), snapshot.hash);

        match self.next_remote_target().await? {
            Some((device_id, target)) => {
                self.log(&format!(
                    "next remote reconcile target: {} {}",
                    device_id,
                    encode_hash(&target)
                ));
                match self.plan_remote_reconcile(&device_id, target).await? {
                    Some(plan) => self.log(&format!(
                        "planned remote reconcile to {} with {} ops",
                        encode_hash(&plan.target_snapshot),
                        plan.ops.len()
                    )),
                    None => self.log("remote reconcile target is not directly applicable yet"),
                }
                if self.apply_next_remote_target().await? {
                    self.log("remote reconcile apply complete");
                }
            }
            None => self.log("no remote reconcile target after frontier update"),
        }

        if snapshot.hash == self.state.snapshot {
            self.log("remote snapshot already matches local snapshot");
            return Ok(());
        }

        if snapshot.hash == self.state.published_snapshot {
            self.log("remote snapshot already matches published snapshot");
            return Ok(());
        }

        self.log(&format!(
            "remote snapshot {} from {} is pending reconcile",
            encode_hash(&snapshot.hash),
            announcement.device
        ));
        Ok(())
    }

    /// Plans a fast-forward reconcile from the current local snapshot to a remote target.
    async fn plan_remote_reconcile(
        &self,
        device_id: &DeviceId,
        target: SnapshotHash,
    ) -> Result<Option<ApplyPlan>> {
        let snapshot = self.load_snapshot(target).await?;
        let base = snapshot.parents.first().copied().unwrap_or([0; 32]);

        if base != self.state.snapshot {
            self.log(&format!(
                "remote target {} from {} is not a fast-forward of local {}",
                encode_hash(&target),
                device_id,
                encode_hash(&self.state.snapshot)
            ));
            return Ok(None);
        }

        let change_set = self
            .coordinator
            .fetch_change_set(&self.config.repo_id, &base, &target)
            .await?;

        if change_set.base != base || change_set.target != target {
            return Err(crate::services::SyncError::InvalidState(
                "coordinator returned mismatched change set".into(),
            ));
        }

        Ok(Some(Self::build_apply_plan(target, &change_set.diff)))
    }

    /// Applies the next fast-forward remote target and advances local state on success.
    async fn apply_next_remote_target(&mut self) -> Result<bool> {
        let Some((device_id, target)) = self.next_remote_target().await? else {
            return Ok(false);
        };
        let Some(plan) = self.plan_remote_reconcile(&device_id, target).await? else {
            return Ok(false);
        };
        let snapshot = self.load_snapshot(target).await?;

        self.ensure_plan_blobs(&plan).await?;
        self.applier.apply(&plan).await?;

        let tree = self.tree_builder.build_tree(&self.config.sync_root).await?;
        if tree.hash != snapshot.tree_hash {
            return Err(crate::services::SyncError::InvalidState(
                "applied tree does not match target snapshot".into(),
            ));
        }

        self.obj_store
            .save_object(&Object::Tree(tree.clone()))
            .await?;
        self.index = self.build_index_from_disk().await?;
        self.tree = tree;
        self.state.snapshot = snapshot.hash;
        self.save_state().await?;
        self.log(&format!(
            "applied remote snapshot {} from {}",
            encode_hash(&snapshot.hash),
            device_id
        ));
        Ok(true)
    }

    async fn full_sync(&mut self, reason: &str) -> Result<SnapshotHash> {
        self.log(&format!("{reason} sync begin"));

        // Build the current tree directly from the local sync root.
        self.log("building current tree from sync root");
        let tree = self.tree_builder.build_tree(&self.config.sync_root).await?;
        self.log(&format!("built tree {}", encode_hash(&tree.hash)));
        let index = self.build_index_from_disk().await?;
        if self.tree.hash == tree.hash {
            self.tree = tree;
            self.index = index;
            self.log(&format!("{reason} tree unchanged"));
            return Ok(self.state.snapshot);
        }

        self.index = index;
        let update = self.index.materialize_all()?;
        self.publish_update(reason, update).await
    }

    /// Publishes a tree update if it changed from the current in-memory root.
    async fn publish_update(&mut self, reason: &str, update: TreeUpdate) -> Result<SnapshotHash> {
        let tree = update.root.clone();
        if self.tree.hash == tree.hash {
            self.log(&format!("{reason} tree unchanged"));
            return Ok(self.state.snapshot);
        }

        // Otherwise create a new snapshot and diff it against the last published tip.
        self.log("tree changed; creating new snapshot");
        let published_snapshot = self.state.published_snapshot;
        let snapshot = build_snapshot(&self.config.device_id, tree.hash, Some(&published_snapshot));

        // Persist the immutable objects locally before advertising the new snapshot.
        self.log("saving tree metadata to object db");
        for file in &update.files {
            self.obj_store
                .save_object(&Object::File(file.clone()))
                .await?;
        }
        self.obj_store
            .save_object(&Object::Tree(tree.clone()))
            .await?;
        let change_set = ChangeSet {
            base: published_snapshot,
            target: snapshot.hash,
            diff: self.build_change_diff(published_snapshot).await?,
        };
        self.log("saving snapshot metadata to object db");
        self.obj_store
            .save_object(&Object::Snapshot(snapshot.clone()))
            .await?;

        // Publish the new snapshot, then advance the local published state.
        self.log("publishing snapshot to coordinator");
        self.coordinator
            .publish_snapshot(&snapshot, &change_set)
            .await?;

        let device_id = self.config.device_id.clone();
        self.tree = tree;
        self.state.snapshot = snapshot.hash;
        self.state.published_snapshot = snapshot.hash;
        self.state
            .frontier
            .device_snapshots
            .insert(device_id, snapshot.hash);
        self.log("saving updated device state");
        self.save_state().await?;
        self.log(&format!(
            "{reason} sync complete {}",
            encode_hash(&snapshot.hash)
        ));

        Ok(snapshot.hash)
    }

    async fn sync_changed_file(&mut self, path: &Path) -> Result<SnapshotHash> {
        let relative = match self.relative_path(path) {
            Some(relative) => relative,
            None => return self.full_sync("local change").await,
        };

        if !self.config.sync_root.join(&relative).is_file() {
            return self.sync_removed_path(path, "local change").await;
        }

        let build = self.build_file(&relative).await?;
        let update = self.index.upsert_file(&relative, build.file)?;
        self.publish_update("local change", update).await
    }

    async fn sync_created_directory(&mut self, path: &Path) -> Result<SnapshotHash> {
        let relative = match self.relative_path(path) {
            Some(relative) => relative,
            None => return self.full_sync("local directory create").await,
        };

        let update = match self.index.ensure_directory(&relative) {
            Ok(update) => update,
            Err(_) => return self.full_sync("local directory create").await,
        };
        self.publish_update("local directory create", update).await
    }

    async fn sync_removed_path(&mut self, path: &Path, reason: &str) -> Result<SnapshotHash> {
        let relative = match self.relative_path(path) {
            Some(relative) => relative,
            None => return self.full_sync(reason).await,
        };

        let update = self.index.remove_path(&relative)?;
        self.publish_update(reason, update).await
    }

    async fn sync_moved_path(&mut self, from: &Path, to: &Path) -> Result<SnapshotHash> {
        let from_relative = match self.relative_path(from) {
            Some(relative) => relative,
            None => return self.full_sync("local change").await,
        };
        let to_relative = match self.relative_path(to) {
            Some(relative) => relative,
            None => return self.full_sync("local change").await,
        };

        let update = self.index.move_path(&from_relative, &to_relative)?;
        self.publish_update("local change", update).await
    }

    async fn save_state(&self) -> Result<()> {
        self.meta_store
            .save_state(&self.config.repo_id, &self.config.device_id, &self.state)
            .await
    }

    async fn build_change_diff(&self, base_snapshot: SnapshotHash) -> Result<TreeDiff> {
        let previous_files = self.load_snapshot_files(base_snapshot).await?;
        let current_files = self.load_index_files()?;
        Ok(crate::core::Tree::diff(&previous_files, &current_files))
    }

    fn load_index_files(&self) -> Result<BTreeMap<String, File>> {
        let update = self.index.materialize_all()?;
        let mut files = BTreeMap::new();
        for file in update.files {
            files.insert(file.path.clone(), file);
        }
        Ok(files)
    }

    async fn build_file(&self, relative: &Path) -> Result<FileBuild> {
        let full_path = self.config.sync_root.join(relative);
        let mut reader = self.chunker.open(&full_path).await?;
        let mut blobs = Vec::new();
        while let Some(chunk) = reader.next_chunk().await? {
            blobs.push(chunk.blob.hash);
            if self.blob_store.has_blob(&chunk.full_blob.hash).await? {
                continue;
            }
            self.blob_store.put_blob(&chunk.full_blob).await?;
        }

        let mut file = File {
            path: relative.to_string_lossy().into_owned(),
            hash: [0; 32],
            blobs,
        };
        file.update_hash();
        Ok(FileBuild { file })
    }

    async fn build_index_from_disk(&self) -> Result<TreeIndex> {
        let mut index = TreeIndex::empty();
        let mut paths = Vec::new();
        walk_files(&self.config.sync_root, &self.config.sync_root, &mut paths)?;

        for path in paths {
            let build = self.build_file(&path).await?;
            let _ = index.upsert_file(&path, build.file)?;
        }

        Ok(index)
    }

    fn relative_path(&self, path: &Path) -> Option<PathBuf> {
        path.strip_prefix(&self.config.sync_root)
            .ok()
            .map(Path::to_path_buf)
    }

    /// Ensures blob content for file writes in a plan is available locally.
    async fn ensure_plan_blobs(&self, plan: &ApplyPlan) -> Result<()> {
        let mut blobs = Vec::new();
        let mut seen = BTreeSet::new();
        for op in &plan.ops {
            let ApplyOp::WriteFile { file, .. } = op else {
                continue;
            };
            for blob in &file.blobs {
                if self.blob_store.has_blob(blob).await? || !seen.insert(*blob) {
                    continue;
                }
                blobs.push(*blob);
            }
        }

        if blobs.is_empty() {
            return Ok(());
        }

        let result = self
            .blob_worker
            .download_blobs(&BlobTransferJob {
                snapshot: plan.target_snapshot,
                blobs: blobs.clone(),
            })
            .await?;

        let downloaded: BTreeSet<_> = result.blobs.into_iter().collect();
        for blob in blobs {
            if !downloaded.contains(&blob) || !self.blob_store.has_blob(&blob).await? {
                return Err(crate::services::SyncError::NotFound);
            }
        }

        Ok(())
    }

    /// Converts a tree diff into a simple apply plan.
    fn build_apply_plan(target_snapshot: SnapshotHash, diff: &TreeDiff) -> ApplyPlan {
        let mut removes = Vec::new();
        let mut directories = BTreeSet::new();
        let mut writes = Vec::new();

        Self::collect_apply_ops(
            PathBuf::new(),
            diff,
            &mut removes,
            &mut directories,
            &mut writes,
        );

        removes.sort_by(|left, right| {
            right
                .components()
                .count()
                .cmp(&left.components().count())
                .then_with(|| left.cmp(right))
        });

        let mut ops = Vec::new();
        for path in removes {
            ops.push(ApplyOp::RemovePath { path });
        }
        for path in directories {
            if path.as_os_str().is_empty() {
                continue;
            }
            ops.push(ApplyOp::CreateDir { path });
        }
        writes.sort_by(|left, right| left.0.cmp(&right.0));
        for (path, file) in writes {
            ops.push(ApplyOp::WriteFile { path, file });
        }

        ApplyPlan {
            target_snapshot,
            ops,
        }
    }

    fn collect_apply_ops(
        prefix: PathBuf,
        diff: &TreeDiff,
        removes: &mut Vec<PathBuf>,
        directories: &mut BTreeSet<PathBuf>,
        writes: &mut Vec<(PathBuf, File)>,
    ) {
        for (name, change) in &diff.entries {
            let path = prefix.join(name);
            match change {
                crate::core::TreeChange::Delete => removes.push(path),
                crate::core::TreeChange::Tree(child) => {
                    Self::collect_apply_ops(path, child, removes, directories, writes);
                }
                crate::core::TreeChange::File(file) => {
                    let mut parent = path.parent();
                    while let Some(dir) = parent {
                        if dir.as_os_str().is_empty() {
                            break;
                        }
                        directories.insert(dir.to_path_buf());
                        parent = dir.parent();
                    }
                    writes.push((path, file.clone()));
                }
            }
        }
    }

    async fn load_snapshot(&self, hash: SnapshotHash) -> Result<Snapshot> {
        match Self::load_saved_snapshot(&*self.obj_store, hash).await {
            Ok(snapshot) => Ok(snapshot),
            Err(crate::services::SyncError::NotFound) => {
                let snapshot = self
                    .coordinator
                    .fetch_snapshot(&self.config.repo_id, &hash)
                    .await?;
                self.obj_store
                    .save_object(&Object::Snapshot(snapshot.clone()))
                    .await?;
                Ok(snapshot)
            }
            Err(error) => Err(error),
        }
    }

    async fn is_snapshot_ancestor(
        &self,
        ancestor: SnapshotHash,
        descendant: SnapshotHash,
    ) -> Result<bool> {
        if ancestor == descendant {
            return Ok(false);
        }

        let mut pending = vec![descendant];
        let mut seen = std::collections::BTreeSet::new();

        while let Some(current) = pending.pop() {
            if !seen.insert(current) {
                continue;
            }
            let snapshot = self.load_snapshot(current).await?;
            for parent in snapshot.parents {
                if parent == ancestor {
                    return Ok(true);
                }
                if parent != [0; 32] {
                    pending.push(parent);
                }
            }
        }

        Ok(false)
    }

    async fn load_saved_snapshot(obj_store: &dyn ObjStore, hash: SnapshotHash) -> Result<Snapshot> {
        match obj_store.load_object(&ObjectId::Snapshot(hash)).await? {
            Object::Snapshot(snapshot) => Ok(snapshot),
            _ => Err(crate::services::SyncError::InvalidState(
                "snapshot object lookup returned wrong type".into(),
            )),
        }
    }

    async fn load_saved_tree(
        obj_store: &dyn ObjStore,
        snapshot_hash: SnapshotHash,
    ) -> Result<Tree> {
        let snapshot = Self::load_saved_snapshot(obj_store, snapshot_hash).await?;
        match obj_store
            .load_object(&ObjectId::Tree(snapshot.tree_hash))
            .await?
        {
            Object::Tree(tree) => Ok(tree),
            _ => Err(crate::services::SyncError::InvalidState(
                "tree object lookup returned wrong type".into(),
            )),
        }
    }

    async fn load_snapshot_files(
        &self,
        snapshot_hash: SnapshotHash,
    ) -> Result<BTreeMap<String, File>> {
        if snapshot_hash == [0; 32] {
            return Ok(BTreeMap::new());
        }

        let snapshot = self.load_snapshot(snapshot_hash).await?;
        self.load_tree_files(snapshot.tree_hash, PathBuf::new())
            .await
    }

    async fn load_tree_files(
        &self,
        tree_hash: crate::core::TreeHash,
        prefix: PathBuf,
    ) -> Result<BTreeMap<String, File>> {
        let tree = match self
            .obj_store
            .load_object(&ObjectId::Tree(tree_hash))
            .await?
        {
            Object::Tree(tree) => tree,
            _ => {
                return Err(crate::services::SyncError::InvalidState(
                    "tree object lookup returned wrong type".into(),
                ));
            }
        };

        let mut files = BTreeMap::new();
        let mut pending = vec![(tree, prefix)];

        while let Some((tree, prefix)) = pending.pop() {
            for entry in tree.entries {
                let path = prefix.join(&entry.name);
                match entry.hash {
                    crate::core::ObjectHash::Tree(hash) => {
                        let child = match self.obj_store.load_object(&ObjectId::Tree(hash)).await? {
                            Object::Tree(tree) => tree,
                            _ => {
                                return Err(crate::services::SyncError::InvalidState(
                                    "tree object lookup returned wrong type".into(),
                                ));
                            }
                        };
                        pending.push((child, path));
                    }
                    crate::core::ObjectHash::File(hash) => {
                        let file = match self.obj_store.load_object(&ObjectId::File(hash)).await? {
                            Object::File(file) => file,
                            _ => {
                                return Err(crate::services::SyncError::InvalidState(
                                    "file object lookup returned wrong type".into(),
                                ));
                            }
                        };
                        files.insert(path.to_string_lossy().into_owned(), file);
                    }
                }
            }
        }

        Ok(files)
    }

    fn log(&self, message: &str) {
        eprintln!("[engine {}] {message}", self.config.device_id);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use tempfile::tempdir;

    use super::SyncEngine;
    use crate::core::{
        ChangeSet, Config, DeviceCredentials, DeviceState, File, FileKind, Frontier, FullBlob,
        Object, ObjectHash, ObjectId, Snapshot, SnapshotAnnouncement, SnapshotHash, Tree,
        TreeChange, TreeDiff, TreeEntry,
    };
    use crate::local::util::hash_bytes;
    use crate::local::{LocalApplier, LocalChunker, build_snapshot};
    use crate::services::{
        Applier, ApplyOp, ApplyPlan, BlobStore, BlobTransferWorker, Coordinator,
        CoordinatorNotification, CoordinatorNotificationStream, MetaStore, ObjStore, Result,
        SyncError, TreeBuilder, WatcherEvent,
    };

    struct MemoryMetaStore {
        state: Mutex<DeviceState>,
    }

    impl MemoryMetaStore {
        fn new(state: DeviceState) -> Self {
            Self {
                state: Mutex::new(state),
            }
        }
    }

    #[async_trait]
    impl MetaStore for MemoryMetaStore {
        async fn load_config(&self) -> Result<Config> {
            Err(SyncError::InvalidState("unused in tests".into()))
        }

        async fn load_state(&self, _repo_id: &String, _device_id: &String) -> Result<DeviceState> {
            Ok(self.state.lock().unwrap().clone())
        }

        async fn save_state(
            &self,
            _repo_id: &String,
            _device_id: &String,
            state: &DeviceState,
        ) -> Result<()> {
            *self.state.lock().unwrap() = state.clone();
            Ok(())
        }
    }

    struct MemoryObjStore {
        objects: Mutex<BTreeMap<String, Object>>,
    }

    impl MemoryObjStore {
        fn new() -> Self {
            Self {
                objects: Mutex::new(BTreeMap::new()),
            }
        }

        fn insert(&self, object: Object) {
            self.objects
                .lock()
                .unwrap()
                .insert(format!("{:?}", object.id()), object);
        }
    }

    #[async_trait]
    impl ObjStore for MemoryObjStore {
        async fn load_object(&self, id: &ObjectId) -> Result<Object> {
            self.objects
                .lock()
                .unwrap()
                .get(&format!("{id:?}"))
                .cloned()
                .ok_or(SyncError::NotFound)
        }

        async fn save_object(&self, object: &Object) -> Result<()> {
            self.objects
                .lock()
                .unwrap()
                .insert(format!("{:?}", object.id()), object.clone());
            Ok(())
        }
    }

    struct MemoryBlobStore {
        blobs: Mutex<BTreeMap<[u8; 32], Vec<u8>>>,
    }

    impl MemoryBlobStore {
        fn new() -> Self {
            Self {
                blobs: Mutex::new(BTreeMap::new()),
            }
        }

        fn len(&self) -> usize {
            self.blobs.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl BlobStore for MemoryBlobStore {
        async fn put_blob(&self, blob: &FullBlob) -> Result<()> {
            self.blobs
                .lock()
                .unwrap()
                .insert(blob.hash, blob.data.clone());
            Ok(())
        }

        async fn get_blob(&self, hash: &[u8; 32]) -> Result<Vec<u8>> {
            self.blobs
                .lock()
                .unwrap()
                .get(hash)
                .cloned()
                .ok_or(SyncError::NotFound)
        }

        async fn has_blob(&self, hash: &[u8; 32]) -> Result<bool> {
            Ok(self.blobs.lock().unwrap().contains_key(hash))
        }
    }

    struct MemoryApplier;

    #[async_trait]
    impl Applier for MemoryApplier {
        async fn apply(&self, _plan: &ApplyPlan) -> Result<()> {
            Ok(())
        }
    }

    struct MemoryBlobTransferWorker {
        blob_store: std::sync::Arc<dyn BlobStore>,
    }

    impl MemoryBlobTransferWorker {
        fn new(blob_store: std::sync::Arc<dyn BlobStore>) -> Self {
            Self { blob_store }
        }
    }

    #[async_trait]
    impl BlobTransferWorker for MemoryBlobTransferWorker {
        async fn upload_blobs(
            &self,
            job: &super::BlobTransferJob,
        ) -> Result<super::BlobTransferResult> {
            Ok(super::BlobTransferResult {
                snapshot: job.snapshot,
                blobs: job.blobs.clone(),
            })
        }

        async fn download_blobs(
            &self,
            job: &super::BlobTransferJob,
        ) -> Result<super::BlobTransferResult> {
            let mut present = Vec::new();
            for blob in &job.blobs {
                if self.blob_store.has_blob(blob).await? {
                    present.push(*blob);
                }
            }
            Ok(super::BlobTransferResult {
                snapshot: job.snapshot,
                blobs: present,
            })
        }
    }

    struct SeededBlobTransferWorker {
        blob_store: std::sync::Arc<dyn BlobStore>,
        blobs: BTreeMap<[u8; 32], Vec<u8>>,
    }

    #[async_trait]
    impl BlobTransferWorker for SeededBlobTransferWorker {
        async fn upload_blobs(
            &self,
            job: &super::BlobTransferJob,
        ) -> Result<super::BlobTransferResult> {
            Ok(super::BlobTransferResult {
                snapshot: job.snapshot,
                blobs: job.blobs.clone(),
            })
        }

        async fn download_blobs(
            &self,
            job: &super::BlobTransferJob,
        ) -> Result<super::BlobTransferResult> {
            let mut downloaded = Vec::new();
            for blob in &job.blobs {
                let Some(data) = self.blobs.get(blob) else {
                    continue;
                };
                self.blob_store
                    .put_blob(&crate::core::FullBlob {
                        hash: *blob,
                        size: data.len() as u64,
                        data: data.clone(),
                    })
                    .await?;
                downloaded.push(*blob);
            }
            Ok(super::BlobTransferResult {
                snapshot: job.snapshot,
                blobs: downloaded,
            })
        }
    }

    fn test_blob_worker(
        blob_store: std::sync::Arc<dyn BlobStore>,
    ) -> std::sync::Arc<dyn BlobTransferWorker> {
        std::sync::Arc::new(MemoryBlobTransferWorker::new(blob_store))
    }

    fn test_applier() -> std::sync::Arc<dyn Applier> {
        std::sync::Arc::new(MemoryApplier)
    }

    struct MemoryCoordinator {
        register_calls: Mutex<usize>,
        published: Mutex<Vec<(Snapshot, ChangeSet)>>,
        snapshots: Mutex<BTreeMap<[u8; 32], Snapshot>>,
        change_sets: Mutex<BTreeMap<([u8; 32], [u8; 32]), ChangeSet>>,
    }

    impl MemoryCoordinator {
        fn new() -> Self {
            Self {
                register_calls: Mutex::new(0),
                published: Mutex::new(Vec::new()),
                snapshots: Mutex::new(BTreeMap::new()),
                change_sets: Mutex::new(BTreeMap::new()),
            }
        }

        fn insert_snapshot(&self, snapshot: Snapshot) {
            self.snapshots
                .lock()
                .unwrap()
                .insert(snapshot.hash, snapshot);
        }

        fn insert_change_set(&self, change_set: ChangeSet) {
            self.change_sets
                .lock()
                .unwrap()
                .insert((change_set.base, change_set.target), change_set);
        }
    }

    #[async_trait]
    impl Coordinator for MemoryCoordinator {
        async fn register_device(&self, _config: &Config) -> Result<()> {
            *self.register_calls.lock().unwrap() += 1;
            Ok(())
        }

        async fn subscribe(
            &self,
            _repo_id: &String,
            _device_id: &String,
        ) -> Result<CoordinatorNotificationStream> {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Ok(rx)
        }

        async fn publish_snapshot(
            &self,
            snapshot: &Snapshot,
            change_set: &ChangeSet,
        ) -> Result<()> {
            self.published
                .lock()
                .unwrap()
                .push((snapshot.clone(), change_set.clone()));
            Ok(())
        }

        async fn fetch_snapshot(&self, _repo_id: &String, hash: &SnapshotHash) -> Result<Snapshot> {
            self.snapshots
                .lock()
                .unwrap()
                .get(hash)
                .cloned()
                .ok_or(SyncError::NotFound)
        }

        async fn fetch_change_set(
            &self,
            _repo_id: &String,
            base: &SnapshotHash,
            target: &SnapshotHash,
        ) -> Result<ChangeSet> {
            self.change_sets
                .lock()
                .unwrap()
                .get(&(*base, *target))
                .cloned()
                .ok_or(SyncError::NotFound)
        }

        async fn fetch_frontier(&self, _repo_id: &String) -> Result<Frontier> {
            Ok(Frontier::default())
        }
    }

    struct FixedTreeBuilder {
        tree: Tree,
    }

    #[async_trait]
    impl TreeBuilder for FixedTreeBuilder {
        async fn build_tree(&self, _root: &Path) -> Result<Tree> {
            Ok(self.tree.clone())
        }
    }

    struct CountingTreeBuilder {
        tree: Tree,
        calls: AtomicUsize,
    }

    impl CountingTreeBuilder {
        fn new(tree: Tree) -> Self {
            Self {
                tree,
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl TreeBuilder for CountingTreeBuilder {
        async fn build_tree(&self, _root: &Path) -> Result<Tree> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.tree.clone())
        }
    }

    #[tokio::test]
    async fn start_publishes_new_snapshot_when_tree_changes() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();
        std::fs::write(sync_root.join("hello.txt"), b"hello").unwrap();

        let (config, file, tree) = sample_config_and_tree(sync_root.clone());
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: [0; 32],
            published_snapshot: [0; 32],
            frontier: Frontier::default(),
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder { tree: tree.clone() });
        let chunker = LocalChunker::open();

        let mut engine = SyncEngine::open(
            config,
            meta_store.clone(),
            obj_store.clone(),
            blob_store.clone(),
            blob_worker,
            applier,
            coordinator.clone(),
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        let snapshot_hash = engine.start().await.unwrap();

        assert_ne!(snapshot_hash, [0; 32]);
        assert_eq!(engine.state.snapshot, snapshot_hash);
        assert_eq!(engine.tree.hash, tree.hash);
        assert_eq!(coordinator.published.lock().unwrap().len(), 1);
        assert!(blob_store.has_blob(&file.blobs[0]).await.unwrap());

        let saved_snapshot = match obj_store
            .load_object(&ObjectId::Snapshot(snapshot_hash))
            .await
            .unwrap()
        {
            Object::Snapshot(snapshot) => snapshot,
            _ => panic!("expected snapshot object"),
        };
        assert_eq!(saved_snapshot.tree_hash, tree.hash);
    }

    #[tokio::test]
    async fn start_skips_publish_when_tree_is_unchanged() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();
        std::fs::write(sync_root.join("hello.txt"), b"hello").unwrap();

        let (config, file, tree) = sample_config_and_tree(sync_root);
        let snapshot = build_snapshot(&config.device_id, tree.hash, Some(&[0; 32]));
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: snapshot.hash,
            published_snapshot: snapshot.hash,
            frontier: Frontier::default(),
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(tree.clone()));
        obj_store.insert(Object::Snapshot(snapshot.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder { tree: tree.clone() });
        let chunker = LocalChunker::open();

        let mut engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            coordinator.clone(),
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        let snapshot_hash = engine.start().await.unwrap();

        assert_eq!(snapshot_hash, snapshot.hash);
        assert_eq!(coordinator.published.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn file_change_updates_tree_without_rebuild() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();
        let file_path = sync_root.join("hello.txt");
        std::fs::write(&file_path, b"hello").unwrap();

        let (config, file, tree) = sample_config_and_tree(sync_root.clone());
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: [0; 32],
            published_snapshot: [0; 32],
            frontier: Frontier::default(),
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(CountingTreeBuilder::new(tree.clone()));
        let chunker = LocalChunker::open();

        let mut engine = SyncEngine::open(
            config,
            meta_store,
            obj_store.clone(),
            blob_store,
            blob_worker,
            applier,
            coordinator,
            tree_builder.clone(),
            chunker,
        )
        .await
        .unwrap();

        engine.start().await.unwrap();
        assert_eq!(tree_builder.calls(), 1);

        std::fs::write(&file_path, b"hello updated").unwrap();
        let before = engine.state.snapshot;
        let after = engine
            .handle_event(super::SyncEvent::Local(WatcherEvent::FileChanged(
                file_path,
            )))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(tree_builder.calls(), 1);
        assert_ne!(after, before);
        assert_ne!(engine.tree.hash, tree.hash);
        assert!(matches!(
            obj_store
                .load_object(&ObjectId::Snapshot(after))
                .await
                .unwrap(),
            Object::Snapshot(_)
        ));
    }

    #[tokio::test]
    async fn directory_delete_updates_tree_without_rebuild() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(sync_root.join("docs")).unwrap();
        std::fs::write(sync_root.join("docs/guide.md"), b"guide").unwrap();

        let (config, file, docs_tree, root_tree) = sample_nested_config_and_tree(sync_root.clone());
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: [0; 32],
            published_snapshot: [0; 32],
            frontier: Frontier::default(),
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(docs_tree));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(CountingTreeBuilder::new(root_tree.clone()));
        let chunker = LocalChunker::open();

        let mut engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            coordinator,
            tree_builder.clone(),
            chunker,
        )
        .await
        .unwrap();

        engine.start().await.unwrap();
        assert_eq!(tree_builder.calls(), 1);

        std::fs::remove_dir_all(sync_root.join("docs")).unwrap();
        let before = engine.state.snapshot;
        let after = engine
            .handle_event(super::SyncEvent::Local(WatcherEvent::DirectoryDeleted(
                sync_root.join("docs"),
            )))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(tree_builder.calls(), 1);
        assert_ne!(after, before);
        assert!(engine.tree.entries.is_empty());
    }

    #[tokio::test]
    async fn file_delete_updates_tree_without_rebuild() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(sync_root.join("docs")).unwrap();
        let file_path = sync_root.join("docs/guide.md");
        std::fs::write(&file_path, b"guide").unwrap();

        let (config, file, docs_tree, root_tree) = sample_nested_config_and_tree(sync_root.clone());
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: [0; 32],
            published_snapshot: [0; 32],
            frontier: Frontier::default(),
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(docs_tree));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(CountingTreeBuilder::new(root_tree.clone()));
        let chunker = LocalChunker::open();

        let mut engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            coordinator,
            tree_builder.clone(),
            chunker,
        )
        .await
        .unwrap();

        engine.start().await.unwrap();
        assert_eq!(tree_builder.calls(), 1);

        std::fs::remove_file(&file_path).unwrap();
        let before = engine.state.snapshot;
        let after = engine
            .handle_event(super::SyncEvent::Local(WatcherEvent::FileDeleted(
                file_path.clone(),
            )))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(tree_builder.calls(), 1);
        assert_ne!(after, before);
        assert!(
            engine
                .index
                .resolve_path(Path::new("docs/guide.md"))
                .unwrap()
                .is_none()
        );
        assert!(
            engine
                .index
                .resolve_path(Path::new("docs"))
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn move_updates_metadata_without_restaging_blob() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(sync_root.join("docs")).unwrap();
        let from_path = sync_root.join("docs/guide.md");
        std::fs::write(&from_path, b"guide").unwrap();

        let (config, file, docs_tree, root_tree) = sample_nested_config_and_tree(sync_root.clone());
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: [0; 32],
            published_snapshot: [0; 32],
            frontier: Frontier::default(),
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(docs_tree));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(CountingTreeBuilder::new(root_tree.clone()));
        let chunker = LocalChunker::open();

        let mut engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store.clone(),
            blob_worker,
            applier,
            coordinator,
            tree_builder.clone(),
            chunker,
        )
        .await
        .unwrap();

        engine.start().await.unwrap();
        assert_eq!(tree_builder.calls(), 1);
        assert_eq!(blob_store.len(), 1);

        let to_dir = sync_root.join("guides");
        std::fs::create_dir_all(&to_dir).unwrap();
        let to_path = to_dir.join("guide.md");
        std::fs::rename(&from_path, &to_path).unwrap();

        let before = engine.state.snapshot;
        let after = engine
            .handle_event(super::SyncEvent::Local(WatcherEvent::PathMoved {
                from: from_path,
                to: to_path,
            }))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(tree_builder.calls(), 1);
        assert_ne!(after, before);
        assert_eq!(blob_store.len(), 1);
    }

    #[tokio::test]
    async fn remote_snapshot_notification_fetches_metadata_and_updates_frontier() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, _file, _tree) = sample_config_and_tree(sync_root.clone());
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: [0; 32],
            published_snapshot: [0; 32],
            frontier: Frontier::default(),
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder {
            tree: Tree::empty(),
        });
        let chunker = LocalChunker::open();

        let mut engine = SyncEngine::open(
            config.clone(),
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            coordinator.clone(),
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        let remote_snapshot =
            build_snapshot(&"peer-1".to_string(), Tree::empty().hash, Some(&[0; 32]));
        let remote_change_set = ChangeSet {
            base: [0; 32],
            target: remote_snapshot.hash,
            diff: crate::core::TreeDiff {
                entries: BTreeMap::new(),
            },
        };
        coordinator.insert_snapshot(remote_snapshot.clone());
        coordinator.insert_change_set(remote_change_set);

        engine
            .handle_event(super::SyncEvent::Remote(CoordinatorNotification::Snapshot(
                SnapshotAnnouncement {
                    repo_id: config.repo_id.clone(),
                    snapshot: remote_snapshot.hash,
                    device: "peer-1".into(),
                },
            )))
            .await
            .unwrap();

        assert_eq!(
            engine.state.frontier.device_snapshots.get("peer-1"),
            Some(&remote_snapshot.hash)
        );
        assert!(matches!(
            engine
                .obj_store
                .load_object(&ObjectId::Snapshot(remote_snapshot.hash))
                .await
                .unwrap(),
            Object::Snapshot(_)
        ));
        assert_eq!(engine.next_remote_target().await.unwrap(), None);
    }

    #[tokio::test]
    async fn remote_snapshot_notification_selects_maximal_frontier_tip() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, _file, tree) = sample_config_and_tree(sync_root.clone());
        let base_snapshot = build_snapshot(&"peer-a".to_string(), [4; 32], Some(&[0; 32]));
        let tip_snapshot =
            build_snapshot(&"peer-b".to_string(), [5; 32], Some(&base_snapshot.hash));
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: [0; 32],
            published_snapshot: [0; 32],
            frontier: Frontier {
                device_snapshots: BTreeMap::from([("peer-a".into(), base_snapshot.hash)]),
            },
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::Snapshot(base_snapshot.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        coordinator.insert_snapshot(tip_snapshot.clone());
        coordinator.insert_change_set(ChangeSet {
            base: base_snapshot.hash,
            target: tip_snapshot.hash,
            diff: crate::core::TreeDiff {
                entries: BTreeMap::new(),
            },
        });
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder { tree });
        let chunker = LocalChunker::open();

        let mut engine = SyncEngine::open(
            config.clone(),
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            coordinator,
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        engine
            .handle_event(super::SyncEvent::Remote(CoordinatorNotification::Snapshot(
                SnapshotAnnouncement {
                    repo_id: config.repo_id.clone(),
                    snapshot: tip_snapshot.hash,
                    device: "peer-b".into(),
                },
            )))
            .await
            .unwrap();

        assert_eq!(
            engine.next_remote_target().await.unwrap(),
            Some(("peer-b".into(), tip_snapshot.hash))
        );
    }

    #[tokio::test]
    async fn reconcile_next_remote_target_builds_fast_forward_plan() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, _file, tree) = sample_config_and_tree(sync_root.clone());
        let base_snapshot = build_snapshot(&config.device_id, tree.hash, Some(&[0; 32]));
        let target_snapshot =
            build_snapshot(&"peer-1".to_string(), [3; 32], Some(&base_snapshot.hash));
        let mut diff = TreeDiff {
            entries: BTreeMap::new(),
        };
        let docs_file = File {
            path: "docs/readme.txt".into(),
            hash: [8; 32],
            blobs: vec![[9; 32]],
        };
        diff.insert_path(Path::new("stale.txt"), TreeChange::Delete);
        diff.insert_path(
            Path::new("docs/readme.txt"),
            TreeChange::File(docs_file.clone()),
        );

        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: base_snapshot.hash,
            published_snapshot: base_snapshot.hash,
            frontier: Frontier {
                device_snapshots: BTreeMap::from([("peer-1".into(), target_snapshot.hash)]),
            },
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::Tree(tree.clone()));
        obj_store.insert(Object::Snapshot(base_snapshot.clone()));
        obj_store.insert(Object::Snapshot(target_snapshot.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        coordinator.insert_change_set(ChangeSet {
            base: base_snapshot.hash,
            target: target_snapshot.hash,
            diff,
        });
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder { tree });
        let chunker = LocalChunker::open();

        let engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            coordinator,
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        let plan = engine
            .reconcile_next_remote_target()
            .await
            .unwrap()
            .expect("expected fast-forward plan");

        assert_eq!(plan.target_snapshot, target_snapshot.hash);
        assert_eq!(
            plan.ops,
            vec![
                ApplyOp::RemovePath {
                    path: PathBuf::from("stale.txt"),
                },
                ApplyOp::CreateDir {
                    path: PathBuf::from("docs"),
                },
                ApplyOp::WriteFile {
                    path: PathBuf::from("docs/readme.txt"),
                    file: docs_file,
                },
            ]
        );
    }

    #[tokio::test]
    async fn reconcile_next_remote_target_skips_non_fast_forward_tip() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, _file, tree) = sample_config_and_tree(sync_root.clone());
        let local_snapshot = build_snapshot(&config.device_id, tree.hash, Some(&[0; 32]));
        let remote_base = build_snapshot(&"peer-1".to_string(), [4; 32], Some(&[0; 32]));
        let remote_tip = build_snapshot(&"peer-1".to_string(), [5; 32], Some(&remote_base.hash));

        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: local_snapshot.hash,
            published_snapshot: local_snapshot.hash,
            frontier: Frontier {
                device_snapshots: BTreeMap::from([("peer-1".into(), remote_tip.hash)]),
            },
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::Tree(tree.clone()));
        obj_store.insert(Object::Snapshot(local_snapshot));
        obj_store.insert(Object::Snapshot(remote_tip));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder { tree });
        let chunker = LocalChunker::open();

        let engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            coordinator,
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        assert_eq!(engine.reconcile_next_remote_target().await.unwrap(), None);
    }

    #[tokio::test]
    async fn remote_snapshot_notification_applies_fast_forward_target() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, _file, _tree) = sample_config_and_tree(sync_root.clone());
        let base_snapshot = build_snapshot(&config.device_id, Tree::empty().hash, Some(&[0; 32]));
        let docs_blob = hash_bytes(b"remote docs");
        let mut docs_file = File {
            path: "docs/readme.txt".into(),
            hash: [0; 32],
            blobs: vec![docs_blob],
        };
        docs_file.update_hash();
        let mut target_tree = Tree {
            hash: [0; 32],
            entries: vec![TreeEntry {
                name: "docs".into(),
                kind: FileKind::Folder,
                hash: ObjectHash::Tree({
                    let mut docs_tree = Tree {
                        hash: [0; 32],
                        entries: vec![TreeEntry {
                            name: "readme.txt".into(),
                            kind: FileKind::File,
                            hash: ObjectHash::File(docs_file.hash),
                        }],
                    };
                    docs_tree.update_hash();
                    docs_tree.hash
                }),
            }],
        };
        target_tree.update_hash();
        let target_snapshot = build_snapshot(
            &"peer-1".to_string(),
            target_tree.hash,
            Some(&base_snapshot.hash),
        );
        let mut diff = TreeDiff {
            entries: BTreeMap::new(),
        };
        diff.insert_path(
            Path::new("docs/readme.txt"),
            TreeChange::File(docs_file.clone()),
        );

        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: base_snapshot.hash,
            published_snapshot: base_snapshot.hash,
            frontier: Frontier::default(),
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::Tree(Tree::empty()));
        obj_store.insert(Object::Snapshot(base_snapshot.clone()));
        obj_store.insert(Object::Snapshot(target_snapshot.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = std::sync::Arc::new(SeededBlobTransferWorker {
            blob_store: blob_store.clone(),
            blobs: BTreeMap::from([(docs_blob, b"remote docs".to_vec())]),
        });
        let applier = std::sync::Arc::new(LocalApplier::new(sync_root.clone(), blob_store.clone()));
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        coordinator.insert_snapshot(target_snapshot.clone());
        coordinator.insert_change_set(ChangeSet {
            base: base_snapshot.hash,
            target: target_snapshot.hash,
            diff,
        });
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder {
            tree: target_tree.clone(),
        });
        let chunker = LocalChunker::open();

        let mut engine = SyncEngine::open(
            config.clone(),
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            coordinator,
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        engine
            .handle_event(super::SyncEvent::Remote(CoordinatorNotification::Snapshot(
                SnapshotAnnouncement {
                    repo_id: config.repo_id.clone(),
                    snapshot: target_snapshot.hash,
                    device: "peer-1".into(),
                },
            )))
            .await
            .unwrap();

        assert_eq!(engine.state.snapshot, target_snapshot.hash);
        assert_eq!(engine.tree.hash, target_tree.hash);
        assert_eq!(
            std::fs::read_to_string(sync_root.join("docs/readme.txt")).unwrap(),
            "remote docs"
        );
    }

    #[tokio::test]
    async fn remote_snapshot_notification_ignores_local_device() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, _file, tree) = sample_config_and_tree(sync_root.clone());
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: [0; 32],
            published_snapshot: [0; 32],
            frontier: Frontier::default(),
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder { tree });
        let chunker = LocalChunker::open();

        let mut engine = SyncEngine::open(
            config.clone(),
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            coordinator.clone(),
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        let remote_snapshot = build_snapshot(&config.device_id, [7; 32], Some(&[0; 32]));
        coordinator.insert_snapshot(remote_snapshot.clone());

        engine
            .handle_event(super::SyncEvent::Remote(CoordinatorNotification::Snapshot(
                SnapshotAnnouncement {
                    repo_id: config.repo_id.clone(),
                    snapshot: remote_snapshot.hash,
                    device: config.device_id.clone(),
                },
            )))
            .await
            .unwrap();

        assert!(engine.state.frontier.device_snapshots.is_empty());
    }

    #[tokio::test]
    async fn remote_snapshot_notification_recognizes_current_snapshot() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();
        std::fs::write(sync_root.join("hello.txt"), b"hello").unwrap();

        let (config, file, tree) = sample_config_and_tree(sync_root.clone());
        let local_snapshot = build_snapshot(&config.device_id, tree.hash, Some(&[0; 32]));
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: local_snapshot.hash,
            published_snapshot: local_snapshot.hash,
            frontier: Frontier::default(),
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(tree.clone()));
        obj_store.insert(Object::Snapshot(local_snapshot.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder { tree });
        let chunker = LocalChunker::open();

        let mut engine = SyncEngine::open(
            config.clone(),
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            coordinator.clone(),
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        let remote_snapshot = Snapshot {
            hash: local_snapshot.hash,
            parents: local_snapshot.parents.clone(),
            tree_hash: local_snapshot.tree_hash,
            origin: "peer-1".into(),
            timestamp: local_snapshot.timestamp,
        };
        let remote_change_set = ChangeSet {
            base: local_snapshot.parents.first().copied().unwrap_or([0; 32]),
            target: remote_snapshot.hash,
            diff: crate::core::TreeDiff {
                entries: BTreeMap::new(),
            },
        };
        coordinator.insert_snapshot(remote_snapshot.clone());
        coordinator.insert_change_set(remote_change_set);

        engine
            .handle_event(super::SyncEvent::Remote(CoordinatorNotification::Snapshot(
                SnapshotAnnouncement {
                    repo_id: config.repo_id.clone(),
                    snapshot: remote_snapshot.hash,
                    device: "peer-1".into(),
                },
            )))
            .await
            .unwrap();

        assert_eq!(
            engine.state.frontier.device_snapshots.get("peer-1"),
            Some(&remote_snapshot.hash)
        );
        assert_eq!(engine.state.snapshot, local_snapshot.hash);
        assert_eq!(engine.state.published_snapshot, local_snapshot.hash);
    }

    #[tokio::test]
    async fn candidate_remote_tips_filters_self_and_current_snapshot() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, file, tree) = sample_config_and_tree(sync_root.clone());
        let local_snapshot = build_snapshot(&config.device_id, tree.hash, Some(&[0; 32]));
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: local_snapshot.hash,
            published_snapshot: local_snapshot.hash,
            frontier: Frontier {
                device_snapshots: BTreeMap::from([
                    (config.device_id.clone(), local_snapshot.hash),
                    ("peer-a".into(), local_snapshot.hash),
                    ("peer-b".into(), [9; 32]),
                    ("peer-c".into(), [0; 32]),
                ]),
            },
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(tree.clone()));
        obj_store.insert(Object::Snapshot(local_snapshot.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder { tree });
        let chunker = LocalChunker::open();

        let engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            coordinator,
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        assert_eq!(
            engine.candidate_remote_tips().await.unwrap(),
            vec![("peer-b".into(), [9; 32])]
        );
    }

    #[tokio::test]
    async fn next_remote_target_is_deterministic() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, _file, tree) = sample_config_and_tree(sync_root.clone());
        let snapshot_a = build_snapshot(&"peer-a".to_string(), [2; 32], Some(&[0; 32]));
        let snapshot_b = build_snapshot(&"peer-b".to_string(), [3; 32], Some(&[0; 32]));
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: [0; 32],
            published_snapshot: [0; 32],
            frontier: Frontier {
                device_snapshots: BTreeMap::from([
                    ("peer-b".into(), snapshot_b.hash),
                    ("peer-a".into(), snapshot_a.hash),
                ]),
            },
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::Snapshot(snapshot_a.clone()));
        obj_store.insert(Object::Snapshot(snapshot_b.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder { tree });
        let chunker = LocalChunker::open();

        let engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            coordinator,
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        assert_eq!(
            engine.next_remote_target().await.unwrap(),
            Some(("peer-a".into(), snapshot_a.hash))
        );
    }

    #[tokio::test]
    async fn candidate_remote_tips_prunes_ancestor_tips() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, file, tree) = sample_config_and_tree(sync_root.clone());
        let base_snapshot = build_snapshot(&"peer-base".to_string(), [2; 32], Some(&[0; 32]));
        let tip_snapshot =
            build_snapshot(&"peer-tip".to_string(), [3; 32], Some(&base_snapshot.hash));
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            snapshot: [0; 32],
            published_snapshot: [0; 32],
            frontier: Frontier {
                device_snapshots: BTreeMap::from([
                    ("peer-a".into(), base_snapshot.hash),
                    ("peer-b".into(), tip_snapshot.hash),
                ]),
            },
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(tree));
        obj_store.insert(Object::Snapshot(base_snapshot.clone()));
        obj_store.insert(Object::Snapshot(tip_snapshot.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder {
            tree: Tree::empty(),
        });
        let chunker = LocalChunker::open();

        let engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            coordinator,
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        assert_eq!(
            engine.candidate_remote_tips().await.unwrap(),
            vec![("peer-b".into(), tip_snapshot.hash)]
        );
    }

    fn sample_config_and_tree(sync_root: PathBuf) -> (Config, File, Tree) {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config = Config {
            sync_root,
            repo_id: format!("repo-{unique}"),
            device_id: format!("device-{unique}"),
            credentials: DeviceCredentials {
                public_key: "pub".into(),
                private_key_path: PathBuf::from("/tmp/device.key"),
            },
        };

        let blob_hash = hash_bytes(b"hello");
        let mut file = File {
            path: "hello.txt".into(),
            hash: [0; 32],
            blobs: vec![blob_hash],
        };
        file.update_hash();

        let mut tree = Tree {
            hash: [0; 32],
            entries: vec![TreeEntry {
                name: "hello.txt".into(),
                kind: FileKind::File,
                hash: ObjectHash::File(file.hash),
            }],
        };
        tree.update_hash();

        (config, file, tree)
    }

    fn sample_nested_config_and_tree(sync_root: PathBuf) -> (Config, File, Tree, Tree) {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config = Config {
            sync_root,
            repo_id: format!("repo-{unique}"),
            device_id: format!("device-{unique}"),
            credentials: DeviceCredentials {
                public_key: "pub".into(),
                private_key_path: PathBuf::from("/tmp/device.key"),
            },
        };

        let blob_hash = hash_bytes(b"guide");
        let mut file = File {
            path: "docs/guide.md".into(),
            hash: [0; 32],
            blobs: vec![blob_hash],
        };
        file.update_hash();

        let mut docs_tree = Tree {
            hash: [0; 32],
            entries: vec![TreeEntry {
                name: "guide.md".into(),
                kind: FileKind::File,
                hash: ObjectHash::File(file.hash),
            }],
        };
        docs_tree.update_hash();

        let mut root_tree = Tree {
            hash: [0; 32],
            entries: vec![TreeEntry {
                name: "docs".into(),
                kind: FileKind::Folder,
                hash: ObjectHash::Tree(docs_tree.hash),
            }],
        };
        root_tree.update_hash();

        (config, file, docs_tree, root_tree)
    }
}
