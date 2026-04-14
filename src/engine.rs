//! Serialized sync engine state machine and runtime job types.
//!
//! The engine owns decisions and state transitions. External runtime tasks feed it events and
//! execute the jobs it emits.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::core::{
    BlobHash, ChangeSet, Config, DeviceState, File, Frontier, Object, ObjectId, Snapshot,
    SnapshotHash, TreeDiff,
};
use crate::local::build_snapshot;
use crate::services::{
    Applier, BlobStore, Coordinator, CoordinatorNotification, CoordinatorNotificationStream,
    WatcherEvent, MetaStore, ObjStore, Reconciler, Result, Scanner, StagedSnapshot, TreeBuilder,
    Watcher, ApplyPlan,
};

/// Serialized device-side sync state machine.
///
/// The engine owns decisions and state transitions, while external tasks feed it events and
/// execute the jobs it returns. This keeps the core sync logic single-threaded and easier to
/// test, even though hashing, transfer, and apply work may happen concurrently around it.
pub struct SyncEngine {
    pub config: Config,
    pub state: DeviceState,
    pub meta_store: Arc<dyn MetaStore>,
    pub obj_store: Arc<dyn ObjStore>,
    pub blob_store: Arc<dyn BlobStore>,
    pub coordinator: Arc<dyn Coordinator>,
    pub scanner: Arc<dyn Scanner>,
    pub watcher: Arc<dyn Watcher>,
    pub tree_builder: Arc<dyn TreeBuilder>,
    pub reconciler: Arc<dyn Reconciler>,
    pub applier: Arc<dyn Applier>,
    pub phase: SyncPhase,
    pub frontier: Option<Frontier>,
    pub coordinator_notifications: CoordinatorNotificationStream,
    pub local_change_rx: mpsc::Receiver<WatcherEvent>,
    pub engine_rx: mpsc::Receiver<EngineEvent>,
    pub job_tx: mpsc::Sender<EngineJob>,
}


/// Coarse runtime state for the engine loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncPhase {
    Starting,
    Running,
    Failed,
    Stopping,
}

/// Inputs that can advance the engine state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    Startup,
    LocalChange(WatcherEvent),
    RemoteChange(CoordinatorNotification),
    StageJobCompleted(StageJobResult),
    StageJobFailed(SnapshotHash),
    BlobUploadCompleted(BlobTransferResult),
    BlobUploadFailed(BlobTransferJob),
    BlobDownloadCompleted(BlobTransferResult),
    BlobDownloadFailed(BlobTransferJob),
    RemoteFetched(RemoteFetch),
    ApplyCompleted(SnapshotHash),
    ApplyFailed(SnapshotHash),
    Shutdown,
}

/// Work requests emitted by the engine for runtime workers to execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineJob {
    Stage(StageJob),
    UploadBlobs(BlobTransferJob),
    DownloadBlobs(BlobTransferJob),
    Apply(ApplyJob),
}

/// Request to build staged local metadata from filesystem changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageJob {
    pub device_id: String,
    pub sync_root: PathBuf,
    pub base: SnapshotHash,
}

/// Result of local staging work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageJobResult {
    pub staged: StagedSnapshot,
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

/// Remote snapshot metadata fetched from the coordinator for reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteFetch {
    pub base_snapshot: SnapshotHash,
    pub target_snapshot: SnapshotHash,
    pub snapshot: Snapshot,
    pub change_set: ChangeSet,
}

/// Request to apply a reconciled filesystem plan locally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyJob {
    pub plan: ApplyPlan,
}


impl SyncEngine {
    pub fn new(
        config: Config,
        state: DeviceState,
        meta_store: Arc<dyn MetaStore>,
        obj_store: Arc<dyn ObjStore>,
        blob_store: Arc<dyn BlobStore>,
        coordinator: Arc<dyn Coordinator>,
        scanner: Arc<dyn Scanner>,
        watcher: Arc<dyn Watcher>,
        tree_builder: Arc<dyn TreeBuilder>,
        reconciler: Arc<dyn Reconciler>,
        applier: Arc<dyn Applier>,
        phase: SyncPhase,
        frontier: Option<Frontier>,
        coordinator_notifications: CoordinatorNotificationStream,
        local_change_rx: mpsc::Receiver<WatcherEvent>,
        engine_rx: mpsc::Receiver<EngineEvent>,
        job_tx: mpsc::Sender<EngineJob>,
    ) -> Self {
        Self {
            config,
            state,
            meta_store,
            obj_store,
            blob_store,
            coordinator,
            scanner,
            watcher,
            tree_builder,
            reconciler,
            applier,
            phase,
            frontier,
            coordinator_notifications,
            local_change_rx,
            engine_rx,
            job_tx,
        }
    }

    pub async fn load_context(&self) -> Result<(Config, DeviceState)> {
        Ok((self.config.clone(), self.state.clone()))
    }

    /// Builds and publishes the current local filesystem state as a new snapshot.
    pub async fn publish_local_snapshot(&mut self) -> Result<SnapshotHash> {
        self.coordinator.register_device(&self.config).await?;
        let staged = self.stage_local_snapshot(self.state.snapshot).await?;
        self.publish_staged_snapshot(&staged).await
    }

    /// Persists and publishes a previously staged snapshot, then advances local device state.
    pub async fn publish_staged_snapshot(&mut self, staged: &StagedSnapshot) -> Result<SnapshotHash> {
        let mut staged = staged.clone();
        staged.change_set = ChangeSet {
            base: self.state.snapshot,
            target: staged.snapshot.hash,
            diff: self.build_change_diff(self.state.snapshot, &staged.tree).await?,
        };

        self.persist_staged_snapshot(&staged).await?;
        self.coordinator
            .publish_snapshot(&staged.snapshot, &staged.change_set)
            .await?;

        self.state.snapshot = staged.snapshot.hash;
        self.state
            .frontier
            .device_snapshots
            .insert(self.config.device_id.clone(), self.state.snapshot);
        self.meta_store
            .save_state(&self.config.repo_id, &self.config.device_id, &self.state)
            .await?;

        Ok(self.state.snapshot)
    }

    /// Applies one engine event and returns any work that should be scheduled externally.
    pub fn handle_event(&mut self, event: EngineEvent) -> Vec<EngineJob> {
        match event {
            EngineEvent::Startup => {
                self.phase = SyncPhase::Running;
                vec![EngineJob::Stage(self.make_stage_job())]
            }
            EngineEvent::LocalChange(_change) => {
                vec![EngineJob::Stage(self.make_stage_job())]
            }
            EngineEvent::RemoteChange(_notification) => {
                // - decide whether to fetch a newer remote base/target pair
                Vec::new()
            }
            EngineEvent::StageJobCompleted(_result) => {
                // - decide which blobs need upload
                // - schedule upload jobs
                Vec::new()
            }
            EngineEvent::StageJobFailed(_base) => {
                self.phase = SyncPhase::Failed;
                Vec::new()
            }
            EngineEvent::BlobUploadCompleted(_result) => {
                // - publish the staged snapshot once required blobs are available
                Vec::new()
            }
            EngineEvent::BlobUploadFailed(_job) => {
                self.phase = SyncPhase::Failed;
                Vec::new()
            }
            EngineEvent::BlobDownloadCompleted(_result) => {
                // - once required blobs are local, schedule apply
                Vec::new()
            }
            EngineEvent::BlobDownloadFailed(_job) => {
                self.phase = SyncPhase::Failed;
                Vec::new()
            }
            EngineEvent::RemoteFetched(_remote) => {
                // - reconcile remote diff against local frontier
                // - schedule apply or download jobs as needed
                Vec::new()
            }
            EngineEvent::ApplyCompleted(_snapshot) => {
                // - advance local frontier/state
                Vec::new()
            }
            EngineEvent::ApplyFailed(_snapshot) => {
                self.phase = SyncPhase::Failed;
                Vec::new()
            }
            EngineEvent::Shutdown => {
                self.phase = SyncPhase::Stopping;
                Vec::new()
            }
        }
    }

    /// Persists immutable objects for a staged snapshot before publication.
    async fn persist_staged_snapshot(&self, staged: &StagedSnapshot) -> Result<()> {
        for object in &staged.tree.objects {
            self.obj_store.save_object(object).await?;
        }
        self.obj_store
            .save_object(&Object::Tree(staged.tree.root.clone()))
            .await?;
        self.obj_store
            .save_object(&Object::Snapshot(staged.snapshot.clone()))
            .await?;
        Ok(())
    }

    /// Builds a staged snapshot from the current local sync root and previous tip.
    async fn stage_local_snapshot(
        &self,
        base_snapshot: SnapshotHash,
    ) -> Result<StagedSnapshot> {
        let tree = self.tree_builder.build_tree(&self.config.sync_root).await?;
        let snapshot = build_snapshot(&self.config.device_id, tree.root.hash, Some(&base_snapshot));
        let diff = self.build_change_diff(base_snapshot, &tree).await?;

        Ok(StagedSnapshot {
            change_set: ChangeSet {
                base: base_snapshot,
                target: snapshot.hash,
                diff,
            },
            tree,
            snapshot,
        })
    }

    async fn build_change_diff(
        &self,
        base_snapshot: SnapshotHash,
        tree: &crate::services::StagedTree,
    ) -> Result<TreeDiff> {
        let previous_files = self.load_snapshot_files(base_snapshot).await?;
        let current_files = self.collect_staged_files(tree);
        Ok(crate::core::Tree::diff(&previous_files, &current_files))
    }

    async fn load_snapshot_files(&self, snapshot_hash: SnapshotHash) -> Result<BTreeMap<String, File>> {
        if snapshot_hash == [0; 32] {
            return Ok(BTreeMap::new());
        }

        let snapshot = match self
            .obj_store
            .load_object(&ObjectId::Snapshot(snapshot_hash))
            .await?
        {
            Object::Snapshot(snapshot) => snapshot,
            _ => {
                return Err(crate::services::SyncError::InvalidState(
                    "snapshot object lookup returned wrong type".into(),
                ))
            }
        };

        self.load_tree_files(snapshot.tree_hash, PathBuf::new()).await
    }

    async fn load_tree_files(
        &self,
        tree_hash: crate::core::TreeHash,
        prefix: PathBuf,
    ) -> Result<BTreeMap<String, File>> {
        let tree = match self.obj_store.load_object(&ObjectId::Tree(tree_hash)).await? {
            Object::Tree(tree) => tree,
            _ => {
                return Err(crate::services::SyncError::InvalidState(
                    "tree object lookup returned wrong type".into(),
                ))
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
                                ))
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
                                ))
                            }
                        };
                        files.insert(path.to_string_lossy().into_owned(), file);
                    }
                }
            }
        }

        Ok(files)
    }

    fn collect_staged_files(&self, tree: &crate::services::StagedTree) -> BTreeMap<String, File> {
        tree.objects
            .iter()
            .filter_map(|object| match object {
                Object::File(file) => Some((file.path.clone(), file.clone())),
                _ => None,
            })
            .collect()
    }

    fn make_stage_job(&self) -> StageJob {
        StageJob {
            device_id: self.config.device_id.clone(),
            sync_root: self.config.sync_root.clone(),
            base: self.state.snapshot,
        }
    }
}
