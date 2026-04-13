use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::core::{
    Blob, BlobHash, Config, DeviceId, DeviceState, FetchedSnapshot, File, Frontier, PeerState,
    RepoId, Snapshot, SnapshotAnnouncement, SnapshotHash, StorageBackend, Tree,
};

pub type Result<T> = std::result::Result<T, SyncError>;

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
pub trait MetadataStore: Send + Sync {
    async fn load_config(&self) -> Result<Config>;
    async fn load_state(&self, repo_id: &RepoId, device_id: &DeviceId) -> Result<DeviceState>;
    async fn save_state(
        &self,
        repo_id: &RepoId,
        device_id: &DeviceId,
        state: &DeviceState,
    ) -> Result<()>;
    async fn save_snapshot(&self, snapshot: &Snapshot) -> Result<()>;
    async fn save_tree(&self, tree: &Tree) -> Result<()>;
    async fn save_file(&self, file: &File) -> Result<()>;
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put_blob(&self, blob: &Blob, bytes: &[u8]) -> Result<()>;
    async fn get_blob(&self, hash: &BlobHash) -> Result<Vec<u8>>;
    async fn has_blob(&self, hash: &BlobHash) -> Result<bool>;
}

#[async_trait]
pub trait Coordinator: Send + Sync {
    async fn register_device(&self, config: &Config) -> Result<()>;
    async fn publish_snapshot(&self, snapshot: &Snapshot) -> Result<()>;
    async fn subscribe(&self, repo_id: &RepoId, device_id: &DeviceId)
    -> Result<NotificationStream>;
    async fn fetch_snapshot(
        &self,
        repo_id: &RepoId,
        hash: &SnapshotHash,
    ) -> Result<FetchedSnapshot>;
    async fn fetch_frontier(&self, repo_id: &RepoId) -> Result<Frontier>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Notification {
    Snapshot(SnapshotAnnouncement),
    PeerAvailable(PeerState),
    // LocalPathsChanged(Vec<PathBuf>),
}

pub type NotificationStream = Vec<Notification>;

#[async_trait]
pub trait Scanner: Send + Sync {
    async fn scan_root(&self, root: &Path) -> Result<Vec<PathBuf>>;
}

#[async_trait]
pub trait Watcher: Send + Sync {
    async fn watch_root(&self, root: &Path) -> Result<NotificationStream>;
}

#[async_trait]
pub trait Chunker: Send + Sync {
    async fn chunk_file(&self, path: &Path) -> Result<Vec<Blob>>;
}

#[async_trait]
pub trait SnapshotBuilder: Send + Sync {
    async fn build_snapshot(
        &self,
        repo_id: &RepoId,
        device_id: &DeviceId,
        changed_paths: &[PathBuf],
        parent_snapshot: Option<&SnapshotHash>,
    ) -> Result<StagedSnapshot>;
}

#[async_trait]
pub trait Reconciler: Send + Sync {
    async fn plan(
        &self,
        local: Option<&Snapshot>,
        remote: &Snapshot,
        frontier: &Frontier,
    ) -> Result<ApplyPlan>;
}

#[async_trait]
pub trait Applier: Send + Sync {
    async fn apply(&self, plan: &ApplyPlan) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedSnapshot {
    pub snapshot: Snapshot,
    pub trees: Vec<Tree>,
    pub files: Vec<File>,
    pub blobs: Vec<Blob>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyPlan {
    pub target_snapshot: SnapshotHash,
    pub writes: Vec<PathBuf>,
    pub removes: Vec<PathBuf>,
    pub conflicts: Vec<PathBuf>,
    pub source: StorageBackend,
}
