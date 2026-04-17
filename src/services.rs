//! Service boundaries for the sync engine.
//!
//! These traits define the seams between the serialized engine and the outside world: persistence,
//! coordinator I/O, filesystem observation, tree construction, reconciliation, and apply.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use crate::core::{
    Blob, BlobHash, ChangeSet, Checkpoint, CheckpointAnnouncement, Config, Delta,
    DeltaAnnouncement, DeviceId, DeviceState, File, Frontier, FullBlob, Object, ObjectId,
    PeerState, RepoId, SeqNo, Snapshot, SnapshotAnnouncement, SnapshotHash, Tree,
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

    /// Publish one ordered delta into the coordinator-backed log.
    ///
    /// The hybrid model expects this to become the normal hot path once the engine is migrated.
    async fn publish_delta(&self, _delta: &Delta) -> Result<()> {
        Err(SyncError::InvalidState(
            "publish_delta not implemented".into(),
        ))
    }

    /// Publish a checkpoint that binds a local snapshot to a coordinator seqno.
    async fn publish_checkpoint(&self, _checkpoint: &Checkpoint) -> Result<()> {
        Err(SyncError::InvalidState(
            "publish_checkpoint not implemented".into(),
        ))
    }

    /// Fetch snapshot metadata by hash.
    async fn fetch_snapshot(&self, repo_id: &RepoId, hash: &SnapshotHash) -> Result<Snapshot>;

    /// Fetch the ChangeSet between `base` and`target`.
    async fn fetch_change_set(
        &self,
        repo_id: &RepoId,
        base: &SnapshotHash,
        target: &SnapshotHash,
    ) -> Result<ChangeSet>;

    /// Fetch one delta by its ordered seqno.
    async fn fetch_delta(&self, _repo_id: &RepoId, _seqno: SeqNo) -> Result<Delta> {
        Err(SyncError::NotFound)
    }

    /// Fetch deltas after one seqno, up to and including another.
    async fn fetch_deltas(
        &self,
        _repo_id: &RepoId,
        _from_exclusive: SeqNo,
        _to_inclusive: SeqNo,
    ) -> Result<Vec<Delta>> {
        Ok(Vec::new())
    }

    /// Fetch the latest checkpoint at or before the requested seqno.
    async fn fetch_checkpoint(
        &self,
        _repo_id: &RepoId,
        _upto_seqno: SeqNo,
    ) -> Result<Option<Checkpoint>> {
        Ok(None)
    }

    /// Fetch the coordinator's current ordered log head for a repo.
    async fn fetch_head_seqno(&self, _repo_id: &RepoId) -> Result<SeqNo> {
        Ok(0)
    }

    /// Fetch the latest known device frontier for a repo.
    async fn fetch_frontier(&self, repo_id: &RepoId) -> Result<Frontier>;
}

/// Push notification from the coordinator control plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorNotification {
    Snapshot(SnapshotAnnouncement),
    Delta(DeltaAnnouncement),
    Checkpoint(CheckpointAnnouncement),
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
    ///
    /// Send on `ready` after the watch is registered so the runtime can return from startup only
    /// when local change delivery is live.
    async fn start(
        &self,
        root: &Path,
        tx: mpsc::Sender<WatcherEvent>,
        ready: oneshot::Sender<()>,
        shutdown: oneshot::Receiver<()>,
    ) -> Result<()>;
}

/// Filesystem-originated change notifications delivered to the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatcherEvent {
    /// File content or metadata changed, or a new file appeared.
    FileChanged(PathBuf),
    /// A file disappeared from the watched root.
    FileDeleted(PathBuf),
    /// A directory appeared under the watched root.
    DirectoryCreated(PathBuf),
    /// A directory disappeared from the watched root.
    DirectoryDeleted(PathBuf),
    /// A path moved within the watched root.
    PathMoved { from: PathBuf, to: PathBuf },
    /// The watcher lost enough fidelity that the engine should rebuild from disk.
    RescanRequested,
}

/// One staged chunk produced by a file chunker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub blob: Blob,
    pub full_blob: FullBlob,
}

#[async_trait]
pub trait ChunkReader: Send {
    /// Returns the next chunk for a file, or `None` once the file is fully consumed.
    async fn next_chunk(&mut self) -> Result<Option<Chunk>>;
}

#[async_trait]
pub trait Chunker: Send + Sync {
    /// Opens a streaming chunk reader for a file.
    async fn open(&self, path: &Path) -> Result<Box<dyn ChunkReader>>;
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
