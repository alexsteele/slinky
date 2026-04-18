//! Serialized sync engine state.
//!
//! For now, the engine owns the simple bootstrap path: build the current tree from the local
//! sync root and publish it to the relay. Runtime-driven local change handling can layer on
//! top later without changing the core startup flow.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use crate::core::{
    BlobHash, ChangeSet, Config, Delta, DeltaWindow, DeviceId, DeviceState, File, FileChange,
    FileOp, Object, ObjectId, PendingDelta, Revision, Snapshot, SnapshotAnnouncement,
    SnapshotHash, Tree, TreeDiff,
};
use crate::index::{TreeIndex, TreeUpdate};
use crate::local::build_snapshot;
use crate::local::util::{encode_hash, walk_files};
use crate::relay::{Relay, RelayEvent};
use crate::services::{
    Applier, ApplyOp, ApplyPlan, BlobStore, BlobTransferWorker, Chunker, MetaStore, ObjStore,
    Result, TreeBuilder, WatcherEvent,
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
    pub relay: Arc<dyn Relay>,
    pub tree_builder: Arc<dyn TreeBuilder>,
    pub chunker: Arc<dyn Chunker>,
    pub pending_deltas: Vec<PendingDelta>,
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
    Remote(RelayEvent),
}

impl SyncEngine {
    /// Returns the replay window needed to advance from one applied seqno to a newer head.
    pub fn delta_window(
        from_exclusive: crate::core::SeqNo,
        to_inclusive: crate::core::SeqNo,
    ) -> Option<DeltaWindow> {
        if to_inclusive <= from_exclusive {
            return None;
        }

        Some(DeltaWindow {
            from_exclusive,
            to_inclusive,
        })
    }

    /// Fetches the relay head and derives the replay window after one applied seqno.
    pub async fn delta_window_to_head(
        &self,
        applied_seqno: crate::core::SeqNo,
    ) -> Result<Option<DeltaWindow>> {
        let head_seqno = self.relay.fetch_head_seqno(&self.config.repo_id).await?;
        Ok(Self::delta_window(applied_seqno, head_seqno))
    }

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
        relay: Arc<dyn Relay>,
        tree_builder: Arc<dyn TreeBuilder>,
        chunker: Arc<dyn Chunker>,
    ) -> Result<Self> {
        let state = meta_store
            .load_state(&config.repo_id, &config.device_id)
            .await?;
        let (current_tree, index) = if state.snapshot == [0; 32] {
            (Tree::empty(), TreeIndex::empty())
        } else {
            let tree = Self::load_saved_tree(&*obj_store, state.snapshot).await?;
            let index = TreeIndex::hydrate(tree.clone(), &*obj_store).await?;
            (tree, index)
        };

        Ok(Self {
            config,
            state,
            tree: current_tree,
            index,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            relay,
            tree_builder,
            chunker,
            pending_deltas: Vec::new(),
        })
    }

    /// Builds the current local tree and publishes a checkpoint snapshot if it changed.
    pub async fn start(&mut self) -> Result<SnapshotHash> {
        self.log("registering device with relay");
        self.relay.register_device(&self.config).await?;
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

    async fn handle_remote_notification(&mut self, notification: RelayEvent) -> Result<()> {
        match notification {
            RelayEvent::Snapshot(announcement) => self.handle_remote_snapshot(announcement).await,
            RelayEvent::Delta(announcement) => self.handle_remote_delta(announcement).await,
            RelayEvent::Checkpoint(announcement) => {
                self.log(&format!(
                    "checkpoint announcement scaffold: {} {}",
                    announcement.device, announcement.checkpoint.seqno
                ));
                Ok(())
            }
            RelayEvent::PeerAvailable(peer) => {
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
            .relay
            .fetch_snapshot(&self.config.repo_id, &announcement.snapshot)
            .await?;
        let base = snapshot.parents.first().copied().unwrap_or([0; 32]);
        let change_set = self
            .relay
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

    async fn handle_remote_delta(&mut self, delta: Delta) -> Result<()> {
        if delta.device_id == self.config.device_id {
            self.log("ignoring remote delta from local device");
            return Ok(());
        }

        if delta.seqno <= self.state.accepted_seqno {
            self.log(&format!(
                "ignoring stale remote delta {} from {}",
                delta.seqno, delta.device_id
            ));
            return Ok(());
        }

        if delta.seqno == self.state.accepted_seqno + 1 {
            self.apply_remote_deltas(&delta.device_id, std::slice::from_ref(&delta))
                .await?;
            self.log(&format!(
                "accepted contiguous remote delta {} from {}",
                delta.seqno, delta.device_id
            ));
            return Ok(());
        }

        let Some(window) = Self::delta_window(self.state.accepted_seqno, delta.seqno) else {
            return Ok(());
        };
        let deltas = self
            .relay
            .fetch_delta_window(&self.config.repo_id, &window)
            .await?;
        let fetched = self.apply_remote_deltas(&delta.device_id, &deltas).await?;
        if fetched == 0 {
            self.log("remote delta fetch returned no applicable deltas");
            return Ok(());
        }

        self.log(&format!(
            "fetched {} remote deltas from {} up to seqno {}",
            fetched, delta.device_id, window.to_inclusive
        ));
        Ok(())
    }

    /// Validates and applies one remote delta batch for the simple hot path.
    async fn apply_remote_deltas(
        &mut self,
        device_id: &DeviceId,
        deltas: &[Delta],
    ) -> Result<usize> {
        let mut expected = self.state.accepted_seqno + 1;
        for delta in deltas {
            if delta.device_id != *device_id {
                return Err(crate::services::SyncError::InvalidState(
                    "relay returned delta for the wrong device".into(),
                ));
            }
            if delta.seqno != expected {
                return Err(crate::services::SyncError::InvalidState(
                    "relay returned a non-contiguous delta window".into(),
                ));
            }
            expected += 1;
        }

        let plan = self.build_delta_apply_plan(deltas)?;
        let (next_index, updates) = self.stage_remote_deltas(deltas)?;
        self.ensure_plan_blobs(&plan).await?;
        self.applier.apply(&plan).await?;
        for update in &updates {
            for object in update.objects() {
                self.obj_store.save_object(&object).await?;
            }
        }
        if let Some(last_update) = updates.last() {
            self.tree = last_update.root.clone();
        }
        self.index = next_index;
        if let Some(last) = deltas.last() {
            self.state.accepted_seqno = last.seqno;
            self.state.applied_seqno = last.seqno;
            self.save_state().await?;
        }
        Ok(deltas.len())
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
            .relay
            .fetch_change_set(&self.config.repo_id, &base, &target)
            .await?;

        if change_set.base != base || change_set.target != target {
            return Err(crate::services::SyncError::InvalidState(
                "relay returned mismatched change set".into(),
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

    /// Publishes a full checkpoint snapshot for the current tree update.
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
        for object in update.objects() {
            self.obj_store.save_object(&object).await?;
        }
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
        self.log("publishing snapshot to relay");
        self.relay.publish_snapshot(&snapshot, &change_set).await?;

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

        let base_file = self.index.file_at_path(&relative)?.map(|file| file.hash);
        let build = self.build_file(&relative).await?;
        let update = self.index.upsert_file(&relative, build.file)?;
        let changed_file = update.files.last().cloned().ok_or_else(|| {
            crate::services::SyncError::InvalidState(
                "file change update is missing file metadata".into(),
            )
        })?;
        self.apply_local_update(
            "local change",
            update,
            vec![FileOp::Modify(FileChange {
                path: relative.to_string_lossy().into_owned(),
                base_file,
                file: changed_file,
            })],
        )
        .await
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
        self.apply_local_update(
            "local directory create",
            update,
            vec![FileOp::CreateDir {
                path: relative.to_string_lossy().into_owned(),
            }],
        )
        .await
    }

    async fn sync_removed_path(&mut self, path: &Path, reason: &str) -> Result<SnapshotHash> {
        let relative = match self.relative_path(path) {
            Some(relative) => relative,
            None => return self.full_sync(reason).await,
        };

        let update = self.index.remove_path(&relative)?;
        self.apply_local_update(
            reason,
            update,
            vec![FileOp::Remove {
                path: relative.to_string_lossy().into_owned(),
            }],
        )
        .await
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
        self.apply_local_update(
            "local change",
            update,
            vec![FileOp::Move {
                from: from_relative.to_string_lossy().into_owned(),
                to: to_relative.to_string_lossy().into_owned(),
            }],
        )
        .await
    }

    /// Applies one local tree mutation, records a delta, and keeps the live tree current.
    async fn apply_local_update(
        &mut self,
        reason: &str,
        update: TreeUpdate,
        changes: Vec<FileOp>,
    ) -> Result<SnapshotHash> {
        let tree = update.root.clone();
        if self.tree.hash == tree.hash {
            self.log(&format!("{reason} tree unchanged"));
            return Ok(self.state.snapshot);
        }

        self.log("saving local tree metadata");
        for object in update.objects() {
            self.obj_store.save_object(&object).await?;
        }

        self.tree = tree;
        self.record_local_delta(changes)?;
        self.save_state().await?;
        self.flush_pending_deltas().await?;
        self.maybe_checkpoint().await?;
        self.log(&format!(
            "{reason} applied locally without checkpoint; pending deltas={}",
            self.pending_deltas.len()
        ));
        Ok(self.state.snapshot)
    }

    /// Records one local delta against the current accepted relay position.
    fn record_local_delta(&mut self, changes: Vec<FileOp>) -> Result<Revision> {
        if changes.is_empty() {
            return Err(crate::services::SyncError::InvalidState(
                "local delta is missing changes".into(),
            ));
        }

        let revision = self.state.next_revision;
        let delta = PendingDelta {
            device_id: self.config.device_id.clone(),
            revision,
            base_seqno: self.state.accepted_seqno,
            timestamp: SystemTime::now(),
            changes,
        };
        // TODO: Crash recovery currently relies on startup re-diff from the last published
        // snapshot. Unpublished in-memory deltas are intentionally dropped on restart.
        self.pending_deltas.push(delta);
        self.state.next_revision += 1;
        Ok(revision)
    }

    /// Flushes pending deltas toward the relay.
    ///
    async fn flush_pending_deltas(&mut self) -> Result<()> {
        if self.pending_deltas.is_empty() {
            return Ok(());
        }

        while !self.pending_deltas.is_empty() {
            let next_seqno = self.relay.fetch_head_seqno(&self.config.repo_id).await? + 1;
            let pending = self.pending_deltas.remove(0);
            let mut delta = Delta {
                hash: [0; 32],
                seqno: next_seqno,
                base_seqno: pending.base_seqno,
                device_id: pending.device_id.clone(),
                revision: pending.revision,
                timestamp: pending.timestamp,
                changes: pending.changes,
            };
            delta.update_hash();

            // TODO: A real relay should assign and acknowledge seqnos instead of trusting the
            // device to propose one from the current head.
            self.relay.publish_delta(&delta).await?;
            self.state.accepted_seqno = delta.seqno;
            self.state.applied_seqno = delta.seqno;
            self.log(&format!(
                "published local delta revision {} at seqno {}",
                delta.revision, delta.seqno
            ));
        }
        Ok(())
    }

    /// Publishes an occasional checkpoint snapshot for the current live tree.
    ///
    /// The current scaffold keeps checkpoint policy disabled while the hot delta path is taking
    /// shape.
    async fn maybe_checkpoint(&mut self) -> Result<()> {
        Ok(())
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

    /// Projects one remote delta batch onto the in-memory index without touching disk.
    fn stage_remote_deltas(&self, deltas: &[Delta]) -> Result<(TreeIndex, Vec<TreeUpdate>)> {
        let mut index = self.index.clone();
        let mut updates = Vec::new();

        for delta in deltas {
            for change in &delta.changes {
                let update = match change {
                    FileOp::CreateDir { path } => index.ensure_directory(Path::new(path))?,
                    FileOp::Remove { path } => index.remove_path(Path::new(path))?,
                    FileOp::Move { from, to } => {
                        index.move_path(Path::new(from), Path::new(to))?
                    }
                    FileOp::Modify(change) => {
                        index.upsert_file(Path::new(&change.path), change.file.clone())?
                    }
                };
                updates.push(update);
            }
        }

        Ok((index, updates))
    }

    fn relative_path(&self, path: &Path) -> Option<PathBuf> {
        if let Ok(relative) = path.strip_prefix(&self.config.sync_root) {
            return Some(relative.to_path_buf());
        }

        let root = self.config.sync_root.as_path();
        let private_prefix = Path::new("/private");
        if let Ok(trimmed_path) = path.strip_prefix(private_prefix) {
            if let Ok(relative) = trimmed_path.strip_prefix(root) {
                return Some(relative.to_path_buf());
            }
        }
        if let Ok(trimmed_root) = root.strip_prefix(private_prefix) {
            if let Ok(relative) = path.strip_prefix(trimmed_root) {
                return Some(relative.to_path_buf());
            }
        }

        let canonical_root = std::fs::canonicalize(&self.config.sync_root).ok()?;
        let canonical_path = std::fs::canonicalize(path).ok()?;
        canonical_path
            .strip_prefix(canonical_root)
            .ok()
            .map(Path::to_path_buf)
    }

    fn build_delta_apply_plan(&self, deltas: &[Delta]) -> Result<ApplyPlan> {
        let mut ops = Vec::new();
        let mut target_snapshot = self.state.snapshot;

        for delta in deltas {
            target_snapshot = [delta.seqno as u8; 32];
            for change in &delta.changes {
                match change {
                    FileOp::CreateDir { path } => ops.push(ApplyOp::CreateDir {
                        path: PathBuf::from(path),
                    }),
                    FileOp::Remove { path } => ops.push(ApplyOp::RemovePath {
                        path: PathBuf::from(path),
                    }),
                    FileOp::Move { from, to } => ops.push(ApplyOp::MovePath {
                        from: PathBuf::from(from),
                        to: PathBuf::from(to),
                    }),
                    FileOp::Modify(change) => ops.push(ApplyOp::WriteFile {
                        path: PathBuf::from(&change.path),
                        file: change.file.clone(),
                    }),
                }
            }
        }

        Ok(ApplyPlan {
            target_snapshot,
            ops,
        })
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
                    .relay
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
        ChangeSet, Config, Delta, DeviceCredentials, DeviceState, File, FileChange, FileKind,
        Frontier, FullBlob, Object, ObjectHash, ObjectId, Snapshot, SnapshotAnnouncement,
        SnapshotHash, Tree, TreeChange, TreeDiff, TreeEntry,
    };
    use crate::local::util::hash_bytes;
    use crate::local::{LocalApplier, LocalChunker, LocalTreeBuilder, build_snapshot};
    use crate::relay::{Relay, RelayEvent, RelayEventStream};
    use crate::services::{
        Applier, ApplyOp, ApplyPlan, BlobStore, BlobTransferWorker, MetaStore, ObjStore, Result,
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

    struct MemoryRelay {
        register_calls: Mutex<usize>,
        published: Mutex<Vec<(Snapshot, ChangeSet)>>,
        snapshots: Mutex<BTreeMap<[u8; 32], Snapshot>>,
        change_sets: Mutex<BTreeMap<([u8; 32], [u8; 32]), ChangeSet>>,
        deltas: Mutex<BTreeMap<u64, Delta>>,
        subscribers: Mutex<Vec<tokio::sync::mpsc::Sender<RelayEvent>>>,
        head_seqno: Mutex<u64>,
    }

    impl MemoryRelay {
        fn new() -> Self {
            Self {
                register_calls: Mutex::new(0),
                published: Mutex::new(Vec::new()),
                snapshots: Mutex::new(BTreeMap::new()),
                change_sets: Mutex::new(BTreeMap::new()),
                deltas: Mutex::new(BTreeMap::new()),
                subscribers: Mutex::new(Vec::new()),
                head_seqno: Mutex::new(0),
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

        fn insert_delta(&self, delta: Delta) {
            self.deltas.lock().unwrap().insert(delta.seqno, delta);
        }
    }

    #[async_trait]
    impl Relay for MemoryRelay {
        async fn register_device(&self, _config: &Config) -> Result<()> {
            *self.register_calls.lock().unwrap() += 1;
            Ok(())
        }

        async fn subscribe(
            &self,
            _repo_id: &String,
            _device_id: &String,
        ) -> Result<RelayEventStream> {
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            self.subscribers.lock().unwrap().push(tx);
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

        async fn publish_delta(&self, delta: &Delta) -> Result<()> {
            self.insert_delta(delta.clone());
            *self.head_seqno.lock().unwrap() = delta.seqno;
            let subscribers = self.subscribers.lock().unwrap().clone();
            for subscriber in subscribers {
                let _ = subscriber.send(RelayEvent::Delta(delta.clone())).await;
            }
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

        async fn fetch_deltas(
            &self,
            _repo_id: &String,
            from_exclusive: crate::core::SeqNo,
            to_inclusive: crate::core::SeqNo,
        ) -> Result<Vec<Delta>> {
            let deltas = self.deltas.lock().unwrap();
            let mut fetched = Vec::new();
            for seqno in (from_exclusive + 1)..=to_inclusive {
                let Some(delta) = deltas.get(&seqno) else {
                    break;
                };
                fetched.push(delta.clone());
            }
            Ok(fetched)
        }

        async fn fetch_head_seqno(&self, _repo_id: &String) -> Result<crate::core::SeqNo> {
            Ok(*self.head_seqno.lock().unwrap())
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
        let meta_store =
            std::sync::Arc::new(MemoryMetaStore::new(test_device_state([0; 32], [0; 32])));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(test_device_state(
            snapshot.hash,
            snapshot.hash,
        )));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(tree.clone()));
        obj_store.insert(Object::Snapshot(snapshot.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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
        let meta_store =
            std::sync::Arc::new(MemoryMetaStore::new(test_device_state([0; 32], [0; 32])));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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
        assert_eq!(after, before);
        assert_eq!(engine.state.snapshot, before);
        assert_ne!(engine.tree.hash, tree.hash);
        assert!(engine.pending_deltas.is_empty());
        assert_eq!(engine.state.accepted_seqno, 1);
        assert!(matches!(
            obj_store
                .load_object(&ObjectId::Tree(engine.tree.hash))
                .await
                .unwrap(),
            Object::Tree(_)
        ));
    }

    #[tokio::test]
    async fn open_hydrates_saved_tree_index_before_start() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, file, docs_tree, root_tree) = sample_nested_config_and_tree(sync_root.clone());
        let snapshot = build_snapshot(&config.device_id, root_tree.hash, Some(&[0; 32]));
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(test_device_state(
            snapshot.hash,
            snapshot.hash,
        )));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file.clone()));
        obj_store.insert(Object::Tree(docs_tree.clone()));
        obj_store.insert(Object::Tree(root_tree.clone()));
        obj_store.insert(Object::Snapshot(snapshot.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let relay = std::sync::Arc::new(MemoryRelay::new());
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder {
            tree: root_tree.clone(),
        });
        let chunker = LocalChunker::open();

        let engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            relay,
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        assert_eq!(engine.tree.hash, root_tree.hash);
        assert!(
            engine
                .index
                .resolve_path(Path::new("docs/guide.md"))
                .unwrap()
                .is_some()
        );
        assert_eq!(
            engine
                .index
                .file_at_path(Path::new("docs/guide.md"))
                .unwrap(),
            Some(file)
        );
    }

    #[tokio::test]
    async fn directory_delete_updates_tree_without_rebuild() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(sync_root.join("docs")).unwrap();
        std::fs::write(sync_root.join("docs/guide.md"), b"guide").unwrap();

        let (config, file, docs_tree, root_tree) = sample_nested_config_and_tree(sync_root.clone());
        let meta_store =
            std::sync::Arc::new(MemoryMetaStore::new(test_device_state([0; 32], [0; 32])));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(docs_tree));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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
        assert_eq!(after, before);
        assert!(engine.tree.entries.is_empty());
        assert!(engine.pending_deltas.is_empty());
        assert_eq!(engine.state.accepted_seqno, 1);
    }

    #[tokio::test]
    async fn file_delete_updates_tree_without_rebuild() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(sync_root.join("docs")).unwrap();
        let file_path = sync_root.join("docs/guide.md");
        std::fs::write(&file_path, b"guide").unwrap();

        let (config, file, docs_tree, root_tree) = sample_nested_config_and_tree(sync_root.clone());
        let meta_store =
            std::sync::Arc::new(MemoryMetaStore::new(test_device_state([0; 32], [0; 32])));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(docs_tree));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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
        assert_eq!(after, before);
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
        assert!(engine.pending_deltas.is_empty());
        assert_eq!(engine.state.accepted_seqno, 1);
    }

    #[tokio::test]
    async fn move_updates_metadata_without_restaging_blob() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(sync_root.join("docs")).unwrap();
        let from_path = sync_root.join("docs/guide.md");
        std::fs::write(&from_path, b"guide").unwrap();

        let (config, file, docs_tree, root_tree) = sample_nested_config_and_tree(sync_root.clone());
        let meta_store =
            std::sync::Arc::new(MemoryMetaStore::new(test_device_state([0; 32], [0; 32])));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(docs_tree));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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
        assert_eq!(after, before);
        assert_eq!(blob_store.len(), 1);
        assert!(engine.pending_deltas.is_empty());
        assert_eq!(engine.state.accepted_seqno, 1);
    }

    #[tokio::test]
    async fn local_change_publishes_delta_to_relay() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();
        let file_path = sync_root.join("hello.txt");
        std::fs::write(&file_path, b"hello").unwrap();

        let (config, file, tree) = sample_config_and_tree(sync_root.clone());
        let meta_store =
            std::sync::Arc::new(MemoryMetaStore::new(test_device_state([0; 32], [0; 32])));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let relay = std::sync::Arc::new(MemoryRelay::new());
        let tree_builder = std::sync::Arc::new(CountingTreeBuilder::new(tree.clone()));
        let chunker = LocalChunker::open();

        let mut engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            relay.clone(),
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        engine.start().await.unwrap();
        std::fs::write(&file_path, b"hello updated").unwrap();
        engine
            .handle_event(super::SyncEvent::Local(WatcherEvent::FileChanged(
                file_path.clone(),
            )))
            .await
            .unwrap();

        assert!(engine.pending_deltas.is_empty());
        assert_eq!(engine.state.accepted_seqno, 1);
        assert_eq!(engine.state.applied_seqno, 1);
        assert!(relay.deltas.lock().unwrap().contains_key(&1));
    }

    #[tokio::test]
    async fn remote_snapshot_notification_fetches_metadata_and_updates_frontier() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, _file, _tree) = sample_config_and_tree(sync_root.clone());
        let meta_store =
            std::sync::Arc::new(MemoryMetaStore::new(test_device_state([0; 32], [0; 32])));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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
            .handle_event(super::SyncEvent::Remote(RelayEvent::Snapshot(
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
            frontier: Frontier {
                device_snapshots: BTreeMap::from([("peer-a".into(), base_snapshot.hash)]),
            },
            ..test_device_state([0; 32], [0; 32])
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::Snapshot(base_snapshot.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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
            .handle_event(super::SyncEvent::Remote(RelayEvent::Snapshot(
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

        let (config, file, tree) = sample_config_and_tree(sync_root.clone());
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
            frontier: Frontier {
                device_snapshots: BTreeMap::from([("peer-1".into(), target_snapshot.hash)]),
            },
            ..test_device_state(base_snapshot.hash, base_snapshot.hash)
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(tree.clone()));
        obj_store.insert(Object::Snapshot(base_snapshot.clone()));
        obj_store.insert(Object::Snapshot(target_snapshot.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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

        let (config, file, tree) = sample_config_and_tree(sync_root.clone());
        let local_snapshot = build_snapshot(&config.device_id, tree.hash, Some(&[0; 32]));
        let remote_base = build_snapshot(&"peer-1".to_string(), [4; 32], Some(&[0; 32]));
        let remote_tip = build_snapshot(&"peer-1".to_string(), [5; 32], Some(&remote_base.hash));

        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            frontier: Frontier {
                device_snapshots: BTreeMap::from([("peer-1".into(), remote_tip.hash)]),
            },
            ..test_device_state(local_snapshot.hash, local_snapshot.hash)
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(tree.clone()));
        obj_store.insert(Object::Snapshot(local_snapshot));
        obj_store.insert(Object::Snapshot(remote_tip));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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

        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(test_device_state(
            base_snapshot.hash,
            base_snapshot.hash,
        )));
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
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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
            .handle_event(super::SyncEvent::Remote(RelayEvent::Snapshot(
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
        let meta_store =
            std::sync::Arc::new(MemoryMetaStore::new(test_device_state([0; 32], [0; 32])));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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
            .handle_event(super::SyncEvent::Remote(RelayEvent::Snapshot(
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
    async fn remote_delta_notification_fetches_missing_window_and_advances_seqno() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, _file, tree) = sample_config_and_tree(sync_root.clone());
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            accepted_seqno: 4,
            ..test_device_state([0; 32], [0; 32])
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let relay = std::sync::Arc::new(MemoryRelay::new());
        relay.insert_delta(sample_delta("peer-1", 5));
        relay.insert_delta(sample_delta("peer-1", 6));
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder { tree });
        let chunker = LocalChunker::open();

        let mut engine = SyncEngine::open(
            config.clone(),
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            relay,
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        engine
            .handle_event(super::SyncEvent::Remote(RelayEvent::Delta(sample_delta(
                "peer-1", 6,
            ))))
            .await
            .unwrap();

        assert_eq!(engine.state.accepted_seqno, 6);
    }

    #[tokio::test]
    async fn remote_delta_notification_applies_file_change_to_disk() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, _file, _tree) = sample_config_and_tree(sync_root.clone());
        let meta_store =
            std::sync::Arc::new(MemoryMetaStore::new(test_device_state([0; 32], [0; 32])));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let docs_blob = hash_bytes(b"remote docs");
        let mut docs_file = File {
            path: "docs/readme.txt".into(),
            hash: [0; 32],
            blobs: vec![docs_blob],
        };
        docs_file.update_hash();
        let blob_worker = std::sync::Arc::new(SeededBlobTransferWorker {
            blob_store: blob_store.clone(),
            blobs: BTreeMap::from([(docs_blob, b"remote docs".to_vec())]),
        });
        let applier = std::sync::Arc::new(LocalApplier::new(sync_root.clone(), blob_store.clone()));
        let relay = std::sync::Arc::new(MemoryRelay::new());
        let chunker = LocalChunker::open();
        let tree_builder = std::sync::Arc::new(LocalTreeBuilder::new(chunker.clone()));

        let mut engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            relay,
            tree_builder,
            chunker,
        )
        .await
        .unwrap();
        engine.start().await.unwrap();

        let mut delta = Delta {
            hash: [0; 32],
            seqno: 1,
            base_seqno: 0,
            device_id: "peer-1".into(),
            revision: 1,
            timestamp: SystemTime::now(),
            changes: vec![crate::core::FileOp::Modify(FileChange {
                path: "docs/readme.txt".into(),
                base_file: None,
                file: docs_file.clone(),
            })],
        };
        delta.update_hash();

        engine
            .handle_event(super::SyncEvent::Remote(RelayEvent::Delta(delta)))
            .await
            .unwrap();

        assert_eq!(engine.state.accepted_seqno, 1);
        assert_eq!(engine.state.applied_seqno, 1);
        assert_eq!(
            std::fs::read_to_string(sync_root.join("docs/readme.txt")).unwrap(),
            "remote docs"
        );
        assert!(
            engine
                .index
                .resolve_path(Path::new("docs/readme.txt"))
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn remote_delta_notification_applies_directory_create_to_disk() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, _file, _tree) = sample_config_and_tree(sync_root.clone());
        let meta_store =
            std::sync::Arc::new(MemoryMetaStore::new(test_device_state([0; 32], [0; 32])));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = std::sync::Arc::new(LocalApplier::new(sync_root.clone(), blob_store.clone()));
        let relay = std::sync::Arc::new(MemoryRelay::new());
        let chunker = LocalChunker::open();
        let tree_builder = std::sync::Arc::new(LocalTreeBuilder::new(chunker.clone()));

        let mut engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            relay,
            tree_builder,
            chunker,
        )
        .await
        .unwrap();
        engine.start().await.unwrap();

        let mut delta = Delta {
            hash: [0; 32],
            seqno: 1,
            base_seqno: 0,
            device_id: "peer-1".into(),
            revision: 1,
            timestamp: SystemTime::now(),
            changes: vec![crate::core::FileOp::CreateDir {
                path: "docs/guides".into(),
            }],
        };
        delta.update_hash();

        engine
            .handle_event(super::SyncEvent::Remote(RelayEvent::Delta(delta)))
            .await
            .unwrap();

        assert_eq!(engine.state.accepted_seqno, 1);
        assert!(sync_root.join("docs/guides").is_dir());
        assert!(
            engine
                .index
                .resolve_path(Path::new("docs/guides"))
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn remote_delta_notification_applies_path_remove_to_disk() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(sync_root.join("docs")).unwrap();
        std::fs::write(sync_root.join("docs/old.txt"), b"stale").unwrap();

        let (config, _file, _tree) = sample_config_and_tree(sync_root.clone());
        let meta_store =
            std::sync::Arc::new(MemoryMetaStore::new(test_device_state([0; 32], [0; 32])));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = std::sync::Arc::new(LocalApplier::new(sync_root.clone(), blob_store.clone()));
        let relay = std::sync::Arc::new(MemoryRelay::new());
        let chunker = LocalChunker::open();
        let tree_builder = std::sync::Arc::new(LocalTreeBuilder::new(chunker.clone()));

        let mut engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            relay,
            tree_builder,
            chunker,
        )
        .await
        .unwrap();
        engine.start().await.unwrap();

        let mut delta = Delta {
            hash: [0; 32],
            seqno: 1,
            base_seqno: 0,
            device_id: "peer-1".into(),
            revision: 1,
            timestamp: SystemTime::now(),
            changes: vec![crate::core::FileOp::Remove {
                path: "docs/old.txt".into(),
            }],
        };
        delta.update_hash();

        engine
            .handle_event(super::SyncEvent::Remote(RelayEvent::Delta(delta)))
            .await
            .unwrap();

        assert_eq!(engine.state.accepted_seqno, 1);
        assert!(!sync_root.join("docs/old.txt").exists());
        assert!(
            engine
                .index
                .resolve_path(Path::new("docs/old.txt"))
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn remote_delta_notification_applies_path_move_to_disk() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(sync_root.join("docs")).unwrap();
        std::fs::write(sync_root.join("docs/old.txt"), b"remote move").unwrap();

        let (config, _file, _tree) = sample_config_and_tree(sync_root.clone());
        let meta_store =
            std::sync::Arc::new(MemoryMetaStore::new(test_device_state([0; 32], [0; 32])));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = std::sync::Arc::new(LocalApplier::new(sync_root.clone(), blob_store.clone()));
        let relay = std::sync::Arc::new(MemoryRelay::new());
        let chunker = LocalChunker::open();
        let tree_builder = std::sync::Arc::new(LocalTreeBuilder::new(chunker.clone()));

        let mut engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            relay,
            tree_builder,
            chunker,
        )
        .await
        .unwrap();
        engine.start().await.unwrap();

        let mut delta = Delta {
            hash: [0; 32],
            seqno: 1,
            base_seqno: 0,
            device_id: "peer-1".into(),
            revision: 1,
            timestamp: SystemTime::now(),
            changes: vec![crate::core::FileOp::Move {
                from: "docs/old.txt".into(),
                to: "docs/new.txt".into(),
            }],
        };
        delta.update_hash();

        engine
            .handle_event(super::SyncEvent::Remote(RelayEvent::Delta(delta)))
            .await
            .unwrap();

        assert_eq!(engine.state.accepted_seqno, 1);
        assert!(!sync_root.join("docs/old.txt").exists());
        assert_eq!(
            std::fs::read_to_string(sync_root.join("docs/new.txt")).unwrap(),
            "remote move"
        );
        assert!(
            engine
                .index
                .resolve_path(Path::new("docs/old.txt"))
                .unwrap()
                .is_none()
        );
        assert!(
            engine
                .index
                .resolve_path(Path::new("docs/new.txt"))
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn remote_delta_notification_ignores_self_and_stale_seqnos() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, _file, tree) = sample_config_and_tree(sync_root.clone());
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(DeviceState {
            accepted_seqno: 8,
            ..test_device_state([0; 32], [0; 32])
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let relay = std::sync::Arc::new(MemoryRelay::new());
        relay.insert_delta(sample_delta("peer-1", 9));
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder { tree });
        let chunker = LocalChunker::open();

        let mut engine = SyncEngine::open(
            config.clone(),
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            relay,
            tree_builder,
            chunker,
        )
        .await
        .unwrap();

        engine
            .handle_event(super::SyncEvent::Remote(RelayEvent::Delta(sample_delta(
                &config.device_id,
                9,
            ))))
            .await
            .unwrap();
        assert_eq!(engine.state.accepted_seqno, 8);

        engine
            .handle_event(super::SyncEvent::Remote(RelayEvent::Delta(sample_delta(
                "peer-1", 8,
            ))))
            .await
            .unwrap();
        assert_eq!(engine.state.accepted_seqno, 8);
    }

    #[tokio::test]
    async fn remote_snapshot_notification_recognizes_current_snapshot() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();
        std::fs::write(sync_root.join("hello.txt"), b"hello").unwrap();

        let (config, file, tree) = sample_config_and_tree(sync_root.clone());
        let local_snapshot = build_snapshot(&config.device_id, tree.hash, Some(&[0; 32]));
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(test_device_state(
            local_snapshot.hash,
            local_snapshot.hash,
        )));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(tree.clone()));
        obj_store.insert(Object::Snapshot(local_snapshot.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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
            .handle_event(super::SyncEvent::Remote(RelayEvent::Snapshot(
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
            frontier: Frontier {
                device_snapshots: BTreeMap::from([
                    (config.device_id.clone(), local_snapshot.hash),
                    ("peer-a".into(), local_snapshot.hash),
                    ("peer-b".into(), [9; 32]),
                    ("peer-c".into(), [0; 32]),
                ]),
            },
            ..test_device_state(local_snapshot.hash, local_snapshot.hash)
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(tree.clone()));
        obj_store.insert(Object::Snapshot(local_snapshot.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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
            frontier: Frontier {
                device_snapshots: BTreeMap::from([
                    ("peer-b".into(), snapshot_b.hash),
                    ("peer-a".into(), snapshot_a.hash),
                ]),
            },
            ..test_device_state([0; 32], [0; 32])
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::Snapshot(snapshot_a.clone()));
        obj_store.insert(Object::Snapshot(snapshot_b.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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
            frontier: Frontier {
                device_snapshots: BTreeMap::from([
                    ("peer-a".into(), base_snapshot.hash),
                    ("peer-b".into(), tip_snapshot.hash),
                ]),
            },
            ..test_device_state([0; 32], [0; 32])
        }));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(tree));
        obj_store.insert(Object::Snapshot(base_snapshot.clone()));
        obj_store.insert(Object::Snapshot(tip_snapshot.clone()));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
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

    #[test]
    fn delta_window_uses_from_exclusive_to_inclusive_semantics() {
        assert_eq!(SyncEngine::delta_window(7, 7), None);
        assert_eq!(
            SyncEngine::delta_window(7, 10),
            Some(crate::core::DeltaWindow {
                from_exclusive: 7,
                to_inclusive: 10,
            })
        );
    }

    #[tokio::test]
    async fn delta_window_to_head_uses_relay_head_seqno() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();

        let (config, file, tree) = sample_config_and_tree(sync_root.clone());
        let snapshot = build_snapshot(&config.device_id, tree.hash, Some(&[0; 32]));
        let meta_store = std::sync::Arc::new(MemoryMetaStore::new(test_device_state(
            snapshot.hash,
            snapshot.hash,
        )));
        let obj_store = std::sync::Arc::new(MemoryObjStore::new());
        obj_store.insert(Object::File(file));
        obj_store.insert(Object::Tree(tree.clone()));
        obj_store.insert(Object::Snapshot(snapshot));
        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let blob_worker = test_blob_worker(blob_store.clone());
        let applier = test_applier();
        let coordinator = std::sync::Arc::new(MemoryRelay::new());
        *coordinator.head_seqno.lock().unwrap() = 12;
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
            engine.delta_window_to_head(9).await.unwrap(),
            Some(crate::core::DeltaWindow {
                from_exclusive: 9,
                to_inclusive: 12,
            })
        );
    }

    fn sample_delta(device_id: &str, seqno: crate::core::SeqNo) -> Delta {
        let mut delta = Delta {
            hash: [0; 32],
            seqno,
            base_seqno: seqno.saturating_sub(1),
            device_id: device_id.into(),
            revision: seqno,
            timestamp: SystemTime::now(),
            changes: vec![crate::core::FileOp::CreateDir {
                path: format!("docs-{seqno}"),
            }],
        };
        delta.update_hash();
        delta
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

    fn test_device_state(snapshot: SnapshotHash, published_snapshot: SnapshotHash) -> DeviceState {
        DeviceState {
            snapshot,
            published_snapshot,
            local_snapshot: (snapshot != [0; 32]).then_some(snapshot),
            applied_seqno: 0,
            accepted_seqno: 0,
            next_revision: 1,
            frontier: Frontier::default(),
        }
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
