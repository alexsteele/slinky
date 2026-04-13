use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::SystemTime;

pub type RepoId = String;
pub type DeviceId = String;
pub type Sha256 = [u8; 32];
pub type SnapshotHash = Sha256;
pub type TreeHash = Sha256;
pub type FileHash = Sha256;
pub type BlobHash = Sha256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub sync_root: PathBuf,
    pub repo_id: RepoId,
    pub device_id: DeviceId,
    pub credentials: DeviceCredentials,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceState {
    pub snapshot: SnapshotHash,
    pub frontier: Frontier,
    // TODO: Do we need these?
    // pub transfers: Vec<PendingTransfer>,
    // pub conflicts: Vec<ConflictRecord>,
    // pub tombstones: Vec<Tombstone>,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerState {
    pub device: DeviceId,
    pub snapshot: SnapshotHash,
}

// TODO: Proper types
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceCredentials {
    pub public_key: String,
    pub private_key_path: PathBuf,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackend {
    CoordinatorManaged,
    CloudObjectStore,
    PeerToPeer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub hash: SnapshotHash,
    pub parents: Vec<SnapshotHash>,
    pub tree_hash: TreeHash,
    pub origin: DeviceId,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tree {
    pub hash: TreeHash,
    pub entries: Vec<TreeEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    pub name: String,
    pub kind: FileKind,
    pub hash: ObjectHash,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileKind {
    Folder,
    File,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectHash {
    Tree(TreeHash),
    File(FileHash),
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct File {
    pub path: String,
    pub hash: FileHash,
    pub blobs: Vec<BlobHash>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob {
    pub hash: BlobHash,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullBlob {
    pub hash: BlobHash,
    pub size: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Frontier {
    pub device_snapshots: BTreeMap<DeviceId, SnapshotHash>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTransfer {
    pub blob_hash: BlobHash,
    pub direction: TransferDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferDirection {
    Upload,
    Download,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferCheckpoint {
    pub blob_hash: BlobHash,
    pub bytes_complete: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    pub path: String,
    pub kind: ConflictKind,
    pub base: Option<SnapshotHash>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictKind {
    EditEdit,
    EditDelete,
    RenameEdit,
    RenameRename,
    DeleteRename,
    FileDirectory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tombstone {
    pub path: String,
    pub deleted_in: SnapshotHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotAnnouncement {
    pub repo_id: RepoId,
    pub snapshot: SnapshotHash,
    pub device: DeviceId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedSnapshot {
    pub snapshot: Snapshot,
    pub source: StorageBackend,
}
