//! Serialized sync engine state machine and runtime job types.
//!
//! The engine owns decisions and state transitions. External runtime tasks feed it events and
//! execute the jobs it emits.

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::core::{
    BlobHash, ChangeSet, Config, DeviceState, Frontier, Object, Snapshot, SnapshotHash,
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
    pub base: SnapshotHash,
    pub changed_paths: Vec<std::path::PathBuf>,
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
        let config = self.meta_store.load_config().await?;
        let state = self
            .meta_store
            .load_state(&config.repo_id, &config.device_id)
            .await?;
        Ok((config, state))
    }

    /// Builds and publishes the current local filesystem state as a new snapshot.
    pub async fn publish_local_snapshot(&self) -> Result<SnapshotHash> {
        let (config, mut state) = self.load_context().await?;

        self.coordinator.register_device(&config).await?;
        let staged = self.build_local_snapshot(&config, state.snapshot).await?;

        self.persist_staged_snapshot(&staged).await?;
        self.coordinator
            .publish_snapshot(&staged.snapshot, &staged.change_set)
            .await?;

        state.snapshot = staged.snapshot.hash;
        state
            .frontier
            .device_snapshots
            .insert(config.device_id.clone(), state.snapshot);
        self.meta_store
            .save_state(&config.repo_id, &config.device_id, &state)
            .await?;

        Ok(state.snapshot)
    }

    /// Applies one engine event and returns any work that should be scheduled externally.
    pub fn handle_event(&mut self, event: EngineEvent) -> Vec<EngineJob> {
        match event {
            EngineEvent::Startup => {
                // - load any startup context needed for the main loop
                // - potentially schedule an initial stage or fetch job
                Vec::new()
            }
            EngineEvent::LocalChange(_change) => {
                // - coalesce local watcher changes
                // - schedule a stage job when ready
                Vec::new()
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
    async fn build_local_snapshot(
        &self,
        config: &Config,
        base_snapshot: SnapshotHash,
    ) -> Result<StagedSnapshot> {
        let tree = self.tree_builder.build_tree(&config.sync_root).await?;
        let snapshot = build_snapshot(&config.device_id, tree.root.hash, Some(&base_snapshot));
        let mut diff = crate::core::TreeDiff {
            entries: std::collections::BTreeMap::new(),
        };

        for object in &tree.objects {
            if let Object::File(file) = object {
                diff.insert_path(
                    std::path::Path::new(&file.path),
                    crate::core::TreeChange::File(file.clone()),
                );
            }
        }

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
}
