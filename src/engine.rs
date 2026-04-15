//! Serialized sync engine state.
//!
//! For now, the engine owns the simple bootstrap path: build the current tree from the local
//! sync root and publish it to the coordinator. Runtime-driven local change handling can layer on
//! top later without changing the core startup flow.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::core::{
    BlobHash, ChangeSet, Config, DeviceState, File, FullBlob, Object, ObjectId, Snapshot,
    SnapshotHash, Tree, TreeDiff,
};
use crate::local::build_snapshot;
use crate::local::util::{encode_hash, hash_bytes, walk_files};
use crate::services::{
    ApplyPlan, BlobStore, Coordinator, CoordinatorNotification, MetaStore, ObjStore, Result,
    TreeBuilder, WatcherEvent,
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
    pub meta_store: Arc<dyn MetaStore>,
    pub obj_store: Arc<dyn ObjStore>,
    pub blob_store: Arc<dyn BlobStore>,
    pub coordinator: Arc<dyn Coordinator>,
    pub tree_builder: Arc<dyn TreeBuilder>,
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

/// Runtime event delivered into the serialized sync engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncEvent {
    Local(WatcherEvent),
    Remote(CoordinatorNotification),
}

impl SyncEngine {
    pub async fn open(
        config: Config,
        meta_store: Arc<dyn MetaStore>,
        obj_store: Arc<dyn ObjStore>,
        blob_store: Arc<dyn BlobStore>,
        coordinator: Arc<dyn Coordinator>,
        tree_builder: Arc<dyn TreeBuilder>,
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
            meta_store,
            obj_store,
            blob_store,
            coordinator,
            tree_builder,
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
                Ok(Some(self.full_sync("local change").await?))
            }
            SyncEvent::Local(WatcherEvent::FileDeleted(path)) => {
                self.log(&format!("local file deleted: {}", path.display()));
                Ok(Some(self.full_sync("local change").await?))
            }
            SyncEvent::Local(WatcherEvent::DirectoryCreated(path)) => {
                self.log(&format!("local directory created: {}", path.display()));
                Ok(Some(self.full_sync("local change").await?))
            }
            SyncEvent::Local(WatcherEvent::DirectoryDeleted(path)) => {
                self.log(&format!("local directory deleted: {}", path.display()));
                Ok(Some(self.full_sync("local change").await?))
            }
            SyncEvent::Local(WatcherEvent::PathMoved { from, to }) => {
                self.log(&format!(
                    "local path moved: {} -> {}",
                    from.display(),
                    to.display()
                ));
                Ok(Some(self.full_sync("local change").await?))
            }
            SyncEvent::Local(WatcherEvent::RescanRequested) => {
                self.log("local rescan requested");
                Ok(Some(self.full_sync("local rescan").await?))
            }
            SyncEvent::Remote(notification) => {
                self.log(&format!("remote event received: {notification:?}"));
                Ok(None)
            }
        }
    }

    async fn full_sync(&mut self, reason: &str) -> Result<SnapshotHash> {
        self.log(&format!("{reason} sync begin"));

        // Build the current tree directly from the local sync root.
        self.log("building current tree from sync root");
        let tree = self.tree_builder.build_tree(&self.config.sync_root).await?;
        self.log(&format!("built tree {}", encode_hash(&tree.hash)));

        // If the current tree already matches the filesystem, there is nothing to do.
        if self.tree.hash == tree.hash {
            self.log(&format!("{reason} tree unchanged"));
            return Ok(self.state.snapshot);
        }

        // Otherwise create a new snapshot and diff it against the last published tip.
        self.log("tree changed; creating new snapshot");
        let published_snapshot = self.state.published_snapshot;
        let snapshot = build_snapshot(&self.config.device_id, tree.hash, Some(&published_snapshot));

        // Stage local blob bytes separately from object metadata.
        self.log("staging local blobs");
        self.stage_local_blobs().await?;

        // Persist the immutable objects locally before advertising the new snapshot.
        self.log("saving tree metadata to object db");
        self.obj_store
            .save_object(&Object::Tree(tree.clone()))
            .await?;
        let change_set = ChangeSet {
            base: published_snapshot,
            target: snapshot.hash,
            diff: self
                .build_change_diff(published_snapshot, tree.hash)
                .await?,
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

    async fn save_state(&self) -> Result<()> {
        self.meta_store
            .save_state(&self.config.repo_id, &self.config.device_id, &self.state)
            .await
    }

    async fn stage_local_blobs(&self) -> Result<()> {
        let mut paths = Vec::new();
        walk_files(&self.config.sync_root, &self.config.sync_root, &mut paths)?;

        for path in paths {
            let full_path = self.config.sync_root.join(&path);
            let data = std::fs::read(&full_path)?;
            let hash = hash_bytes(&data);
            self.blob_store
                .put_blob(&FullBlob {
                    hash,
                    size: data.len() as u64,
                    data,
                })
                .await?;
        }

        Ok(())
    }

    async fn build_change_diff(
        &self,
        base_snapshot: SnapshotHash,
        current_tree: crate::core::TreeHash,
    ) -> Result<TreeDiff> {
        let previous_files = self.load_snapshot_files(base_snapshot).await?;
        let current_files = self.load_tree_files(current_tree, PathBuf::new()).await?;
        Ok(crate::core::Tree::diff(&previous_files, &current_files))
    }

    async fn load_snapshot(&self, hash: SnapshotHash) -> Result<Snapshot> {
        Self::load_saved_snapshot(&*self.obj_store, hash).await
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
    use std::time::{SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use tempfile::tempdir;

    use super::SyncEngine;
    use crate::core::{
        ChangeSet, Config, DeviceCredentials, DeviceState, File, FileKind, Frontier, FullBlob,
        Object, ObjectHash, ObjectId, Snapshot, SnapshotHash, Tree, TreeEntry,
    };
    use crate::local::build_snapshot;
    use crate::local::util::hash_bytes;
    use crate::services::{
        BlobStore, Coordinator, CoordinatorNotificationStream, MetaStore, ObjStore, Result,
        SyncError, TreeBuilder,
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
    }

    #[async_trait]
    impl BlobStore for MemoryBlobStore {
        async fn put_blob(&self, blob: &FullBlob) -> Result<()> {
            self.blobs.lock().unwrap().insert(blob.hash, blob.data.clone());
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

    struct MemoryCoordinator {
        register_calls: Mutex<usize>,
        published: Mutex<Vec<(Snapshot, ChangeSet)>>,
    }

    impl MemoryCoordinator {
        fn new() -> Self {
            Self {
                register_calls: Mutex::new(0),
                published: Mutex::new(Vec::new()),
            }
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

        async fn publish_snapshot(&self, snapshot: &Snapshot, change_set: &ChangeSet) -> Result<()> {
            self.published
                .lock()
                .unwrap()
                .push((snapshot.clone(), change_set.clone()));
            Ok(())
        }

        async fn fetch_snapshot(&self, _repo_id: &String, _hash: &SnapshotHash) -> Result<Snapshot> {
            Err(SyncError::NotFound)
        }

        async fn fetch_change_set(
            &self,
            _repo_id: &String,
            _base: &SnapshotHash,
            _target: &SnapshotHash,
        ) -> Result<ChangeSet> {
            Err(SyncError::NotFound)
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
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder { tree: tree.clone() });

        let mut engine = SyncEngine::open(
            config,
            meta_store.clone(),
            obj_store.clone(),
            blob_store.clone(),
            coordinator.clone(),
            tree_builder,
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
        let coordinator = std::sync::Arc::new(MemoryCoordinator::new());
        let tree_builder = std::sync::Arc::new(FixedTreeBuilder { tree: tree.clone() });

        let mut engine = SyncEngine::open(
            config,
            meta_store,
            obj_store,
            blob_store,
            coordinator.clone(),
            tree_builder,
        )
        .await
        .unwrap();

        let snapshot_hash = engine.start().await.unwrap();

        assert_eq!(snapshot_hash, snapshot.hash);
        assert_eq!(coordinator.published.lock().unwrap().len(), 0);
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
}
