//! Core sync model types.
//!
//! These types define the immutable object graph, history objects, and small shared identifiers
//! that the rest of the system passes around.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256 as Sha256Hasher};

pub type RepoId = String;
pub type DeviceId = String;
pub type Sha256 = [u8; 32];
pub type SnapshotHash = Sha256;
pub type TreeHash = Sha256;
pub type FileHash = Sha256;
pub type BlobHash = Sha256;

/// Local device configuration needed to join and run a repo.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub sync_root: PathBuf,
    pub repo_id: RepoId,
    pub device_id: DeviceId,
    pub credentials: DeviceCredentials,
}

/// Mutable device-local sync state that is advanced as snapshots are published or applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceState {
    pub snapshot: SnapshotHash,
    pub frontier: Frontier,
    // TODO: Do we need these?
    // pub transfers: Vec<PendingTransfer>,
    // pub conflicts: Vec<ConflictRecord>,
    // pub tombstones: Vec<Tombstone>,
}


/// A peer's advertised tip snapshot within the repo frontier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerState {
    pub device: DeviceId,
    pub snapshot: SnapshotHash,
}

// TODO: Proper types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceCredentials {
    pub public_key: String,
    pub private_key_path: PathBuf,
}


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageBackend {
    CoordinatorManaged,
    CloudObjectStore,
    PeerToPeer,
}

/// Any immutable object in the sync object graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Object {
    Snapshot(Snapshot),
    Tree(Tree),
    File(File),
    Blob(Blob),
}

impl Object {
    /// Returns the typed content hash used to persist and look up this object.
    pub fn id(&self) -> ObjectId {
        match self {
            Self::Snapshot(snapshot) => ObjectId::Snapshot(snapshot.hash),
            Self::Tree(tree) => ObjectId::Tree(tree.hash),
            Self::File(file) => ObjectId::File(file.hash),
            Self::Blob(blob) => ObjectId::Blob(blob.hash),
        }
    }
}

/// Typed identifier for objects persisted in the object store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectId {
    Snapshot(SnapshotHash),
    Tree(TreeHash),
    File(FileHash),
    Blob(BlobHash),
}

/// A history node that points at a root tree and one or more parents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub hash: SnapshotHash,
    pub parents: Vec<SnapshotHash>,
    pub tree_hash: TreeHash,
    pub origin: DeviceId,
    pub timestamp: SystemTime,
}

impl Snapshot {
    /// Computes the canonical snapshot hash from parent links, root tree, and origin device.
    pub fn compute_hash(&self) -> SnapshotHash {
        let mut hasher = Sha256Hasher::new();
        for parent in &self.parents {
            hasher.update(parent);
        }
        hasher.update(self.tree_hash);
        hasher.update(self.origin.as_bytes());
        hasher.finalize().into()
    }

    /// Recomputes and stores the canonical snapshot hash.
    pub fn update_hash(&mut self) -> SnapshotHash {
        self.hash = self.compute_hash();
        self.hash
    }
}

/// A journal entry describing how to move from one snapshot to another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSet {
    pub base: SnapshotHash,
    pub target: SnapshotHash,
    pub diff: TreeDiff,
}

/// Recursive tree diff keyed by path segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeDiff {
    pub entries: BTreeMap<String, TreeChange>,
}

/// A change to a single tree entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TreeChange {
    Delete,
    Tree(TreeDiff),
    File(File),
}

/// Immutable directory object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tree {
    pub hash: TreeHash,
    pub entries: Vec<TreeEntry>,
}

impl Tree {
    /// Computes the canonical tree hash from its ordered entries.
    pub fn compute_hash(&self) -> TreeHash {
        let mut hasher = Sha256Hasher::new();
        for entry in &self.entries {
            hasher.update(entry.name.as_bytes());
            match entry.hash {
                ObjectHash::Tree(hash) => {
                    hasher.update(b"tree");
                    hasher.update(hash);
                }
                ObjectHash::File(hash) => {
                    hasher.update(b"file");
                    hasher.update(hash);
                }
            }
        }
        hasher.finalize().into()
    }

    /// Recomputes and stores the canonical tree hash.
    pub fn update_hash(&mut self) -> TreeHash {
        self.hash = self.compute_hash();
        self.hash
    }
}

/// One named edge from a tree to either a child tree or a file object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeEntry {
    pub name: String,
    pub kind: FileKind,
    pub hash: ObjectHash,
}


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileKind {
    Folder,
    File,
}


/// Typed hash reference stored inside tree entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectHash {
    Tree(TreeHash),
    File(FileHash),
}


/// Immutable file metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct File {
    pub path: String,
    pub hash: FileHash,
    pub blobs: Vec<BlobHash>,
}

impl File {
    /// Computes the canonical file hash from its relative path and referenced blobs.
    pub fn compute_hash(&self) -> FileHash {
        let mut hasher = Sha256Hasher::new();
        hasher.update(self.path.as_bytes());
        for blob in &self.blobs {
            hasher.update(blob);
        }
        hasher.finalize().into()
    }

    /// Recomputes and stores the canonical file hash.
    pub fn update_hash(&mut self) -> FileHash {
        self.hash = self.compute_hash();
        self.hash
    }
}

/// Immutable blob metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blob {
    pub hash: BlobHash,
    pub size: u64,
}

/// Full blob content used when staging, uploading, or materializing data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullBlob {
    pub hash: BlobHash,
    pub size: u64,
    pub data: Vec<u8>,
}

/// Latest snapshot known for each device.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Frontier {
    pub device_snapshots: BTreeMap<DeviceId, SnapshotHash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingTransfer {
    pub blob_hash: BlobHash,
    pub direction: TransferDirection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferDirection {
    Upload,
    Download,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferCheckpoint {
    pub blob_hash: BlobHash,
    pub bytes_complete: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conflict {
    pub path: String,
    pub kind: ConflictKind,
    pub base: Option<SnapshotHash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictKind {
    EditEdit,
    EditDelete,
    RenameEdit,
    RenameRename,
    DeleteRename,
    FileDirectory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    pub path: String,
    pub deleted_in: SnapshotHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotAnnouncement {
    pub repo_id: RepoId,
    pub snapshot: SnapshotHash,
    pub device: DeviceId,
}
