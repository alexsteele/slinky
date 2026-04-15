//! Service boundaries for the sync engine.
//!
//! These traits define the seams between the serialized engine and the outside world: persistence,
//! coordinator I/O, filesystem observation, tree construction, reconciliation, and apply.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::core::{
    Blob, BlobHash, ChangeSet, Config, DeviceId, DeviceState, File, Frontier, FullBlob, Object,
    ObjectId, PeerState, RepoId, Snapshot, SnapshotAnnouncement, SnapshotHash, Tree,
};
use crate::engine::{ApplyJob, BlobTransferJob, BlobTransferResult};

pub type Result<T> = std::result::Result<T, SyncError>;

#[async_trait]
pub trait Coordinator: Send + Sync {
    /// Ensures the coordinator knows about this device before sync begins.
    /// Register a device with the coordinator for its repo.
    async fn register_device(&self, config: &Config) -> Result<()>;

    /// Subscribe to pushed coordinator notifications for a device.
    async fn subscribe(
        &self,
        repo_id: &RepoId,
        device_id: &DeviceId,
    ) -> Result<CoordinatorNotificationStream>;

    /// Publish a new snapshot plus its journal diff to the coordinator.
    async fn publish_snapshot(&self, snapshot: &Snapshot, change_set: &ChangeSet) -> Result<()>;

    /// Fetch snapshot metadata by hash.
    async fn fetch_snapshot(&self, repo_id: &RepoId, hash: &SnapshotHash) -> Result<Snapshot>;

    /// Fetch the ChangeSet between `base` and`target`.
    async fn fetch_change_set(
        &self,
        repo_id: &RepoId,
        base: &SnapshotHash,
        target: &SnapshotHash,
    ) -> Result<ChangeSet>;

    /// Fetch the latest known device frontier for a repo.
    async fn fetch_frontier(&self, repo_id: &RepoId) -> Result<Frontier>;
}

/// Push notification from the coordinator control plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorNotification {
    Snapshot(SnapshotAnnouncement),
    PeerAvailable(PeerState),
}

pub type CoordinatorNotificationStream = mpsc::Receiver<CoordinatorNotification>;

/// Shared sync error type used across local services and runtime workers.
#[derive(Debug)]
pub enum SyncError {
    NotFound,
    Conflict(String),
    InvalidState(String),
    Io(std::io::Error),
}

impl From<std::io::Error> for SyncError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[async_trait]
pub trait MetaStore: Send + Sync {
    /// Loads mutable device-local config and progress state.
    async fn load_config(&self) -> Result<Config>;
    async fn load_state(&self, repo_id: &RepoId, device_id: &DeviceId) -> Result<DeviceState>;
    async fn save_state(
        &self,
        repo_id: &RepoId,
        device_id: &DeviceId,
        state: &DeviceState,
    ) -> Result<()>;
}

#[async_trait]
pub trait ObjStore: Send + Sync {
    /// Stores immutable sync objects by typed hash.
    async fn load_object(&self, id: &ObjectId) -> Result<Object>;
    async fn save_object(&self, object: &Object) -> Result<()>;
}

/// Stores local blob content for staging, upload, and download completion.
#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn put_blob(&self, blob: &FullBlob) -> Result<()>;
    async fn get_blob(&self, hash: &BlobHash) -> Result<Vec<u8>>;
    async fn has_blob(&self, hash: &BlobHash) -> Result<bool>;
}

#[async_trait]
pub trait Scanner: Send + Sync {
    /// Enumerates files under a local root.
    async fn scan_root(&self, root: &Path) -> Result<Vec<PathBuf>>;
}

#[async_trait]
pub trait Watcher: Send + Sync {
    /// Starts pushing filesystem change events for a root into the engine runtime.
    async fn start(&self, root: &Path, tx: mpsc::Sender<WatcherEvent>) -> Result<()>;
}

/// Filesystem-originated change notifications delivered to the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatcherEvent {
    PathsChanged(Vec<PathBuf>),
    RescanRequested,
}

#[async_trait]
pub trait Chunker: Send + Sync {
    /// Splits a file into blob metadata according to the chunking policy.
    async fn chunk_file(&self, path: &Path) -> Result<Vec<Blob>>;
}

#[async_trait]
pub trait TreeBuilder: Send + Sync {
    /// Build the current root tree for a local sync root.
    ///
    /// This is the simple full-root/bootstrap path. Persistence is kept separate so the engine can
    /// decide when to save the resulting immutable objects.
    async fn build_tree(&self, root: &Path) -> Result<Tree>;
}

#[async_trait]
pub trait Reconciler: Send + Sync {
    /// Plans the local filesystem changes needed to move toward a remote snapshot.
    async fn plan(
        &self,
        local: Option<&Snapshot>,
        remote: &Snapshot,
        frontier: &Frontier,
    ) -> Result<ApplyPlan>;
}

#[async_trait]
pub trait Applier: Send + Sync {
    /// Applies a previously reconciled plan to the local filesystem.
    async fn apply(&self, plan: &ApplyPlan) -> Result<()>;
}

/// Executes blob upload/download jobs against remote transfer backends.
#[async_trait]
pub trait BlobTransferWorker: Send + Sync {
    async fn upload_blobs(&self, job: &BlobTransferJob) -> Result<BlobTransferResult>;
    async fn download_blobs(&self, job: &BlobTransferJob) -> Result<BlobTransferResult>;
}

/// Executes apply jobs against the local filesystem.
#[async_trait]
pub trait ApplyWorker: Send + Sync {
    async fn run_apply_job(&self, job: &ApplyJob) -> Result<()>;
}

/// Filesystem operations chosen by reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyPlan {
    pub target_snapshot: SnapshotHash,
    pub ops: Vec<ApplyOp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOp {
    WriteFile {
        path: PathBuf,
        file: File,
    },
    CreateDir {
        path: PathBuf,
    },
    RemovePath {
        path: PathBuf,
    },
    MaterializeConflict {
        original_path: PathBuf,
        conflict_path: PathBuf,
        file: File,
    },
}
