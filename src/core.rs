//! Core sync model types.
//!
//! These types define the immutable object graph, history objects, and small shared identifiers
//! that the rest of the system passes around.

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256 as Sha256Hasher};

pub type RepoId = String;
pub type DeviceId = String;
pub type Sha256 = [u8; 32];
pub type SeqNo = u64;
pub type DeltaHash = Sha256;
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
    pub published_snapshot: SnapshotHash,
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

/// Binds a content-addressed snapshot to an ordered coordinator log position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub snapshot: SnapshotHash,
    pub seqno: SeqNo,
}

/// One ordered replay window in the coordinator-backed delta log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaWindow {
    pub from_exclusive: SeqNo,
    pub to_inclusive: SeqNo,
}

/// One ordered log entry in the coordinator-backed delta stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Delta {
    pub hash: DeltaHash,
    pub seqno: SeqNo,
    pub base_seqno: SeqNo,
    pub device_id: DeviceId,
    pub device_seqno: u64,
    pub timestamp: SystemTime,
    pub changes: Vec<Change>,
}

impl Delta {
    /// Computes the canonical delta hash from ordering metadata and change payload.
    pub fn compute_hash(&self) -> DeltaHash {
        let mut hasher = Sha256Hasher::new();
        hasher.update(self.seqno.to_le_bytes());
        hasher.update(self.base_seqno.to_le_bytes());
        hasher.update(self.device_id.as_bytes());
        hasher.update(self.device_seqno.to_le_bytes());
        for change in &self.changes {
            change.hash_into(&mut hasher);
        }
        hasher.finalize().into()
    }

    /// Recomputes and stores the canonical delta hash.
    pub fn update_hash(&mut self) -> DeltaHash {
        self.hash = self.compute_hash();
        self.hash
    }
}

/// One logical filesystem change in a delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Change {
    CreateDir { path: String },
    DeletePath { path: String },
    MovePath { from: String, to: String },
    UpdateFile(FileChange),
}

impl Change {
    fn hash_into(&self, hasher: &mut Sha256Hasher) {
        match self {
            Self::CreateDir { path } => {
                hasher.update(b"create_dir");
                hasher.update(path.as_bytes());
            }
            Self::DeletePath { path } => {
                hasher.update(b"delete_path");
                hasher.update(path.as_bytes());
            }
            Self::MovePath { from, to } => {
                hasher.update(b"move_path");
                hasher.update(from.as_bytes());
                hasher.update(to.as_bytes());
            }
            Self::UpdateFile(change) => {
                hasher.update(b"update_file");
                hasher.update(change.path.as_bytes());
                hasher.update(change.base_file.unwrap_or([0; 32]));
                hasher.update(change.file.hash);
            }
        }
    }
}

/// Describes one file version transition inside a delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileChange {
    pub path: String,
    pub base_file: Option<FileHash>,
    pub file: File,
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

impl TreeDiff {
    /// Inserts a change at a relative path, creating nested tree diff nodes as needed.
    pub fn insert_path(&mut self, path: &Path, change: TreeChange) {
        let mut components = path.components();
        if let Some(component) = components.next() {
            let name = component.as_os_str().to_string_lossy().into_owned();
            if components.as_path().as_os_str().is_empty() {
                self.entries.insert(name, change);
                return;
            }

            let entry = self.entries.entry(name).or_insert_with(|| {
                TreeChange::Tree(TreeDiff {
                    entries: BTreeMap::new(),
                })
            });

            match entry {
                TreeChange::Tree(child) => child.insert_path(components.as_path(), change),
                _ => {
                    let mut child = TreeDiff {
                        entries: BTreeMap::new(),
                    };
                    child.insert_path(components.as_path(), change);
                    *entry = TreeChange::Tree(child);
                }
            }
        }
    }
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
    /// Returns an empty root tree.
    pub fn empty() -> Self {
        Self {
            hash: [0; 32],
            entries: Vec::new(),
        }
    }

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

    /// Computes a path-based diff between two materialized tree views.
    ///
    /// The trees themselves only store child hashes, so callers provide the resolved file maps
    /// for the previous and current roots.
    pub fn diff(previous: &BTreeMap<String, File>, current: &BTreeMap<String, File>) -> TreeDiff {
        let mut diff = TreeDiff {
            entries: BTreeMap::new(),
        };

        for path in previous.keys() {
            if !current.contains_key(path) {
                diff.insert_path(Path::new(path), TreeChange::Delete);
            }
        }

        for (path, file) in current {
            match previous.get(path) {
                Some(previous_file) if previous_file.hash == file.hash => {}
                _ => diff.insert_path(Path::new(path), TreeChange::File(file.clone())),
            }
        }

        diff
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaAnnouncement {
    pub repo_id: RepoId,
    pub seqno: SeqNo,
    pub device: DeviceId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointAnnouncement {
    pub repo_id: RepoId,
    pub checkpoint: Checkpoint,
    pub device: DeviceId,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::UNIX_EPOCH;

    use super::{Change, Delta, File, FileChange, Tree, TreeChange};

    fn file(path: &str, tag: u8) -> File {
        let mut file = File {
            path: path.to_string(),
            hash: [0; 32],
            blobs: vec![[tag; 32]],
        };
        file.update_hash();
        file
    }

    #[test]
    fn tree_diff_reports_add_change_and_delete() {
        let previous = BTreeMap::from([
            ("alpha.txt".to_string(), file("alpha.txt", 1)),
            ("beta.txt".to_string(), file("beta.txt", 2)),
        ]);
        let current = BTreeMap::from([
            ("alpha.txt".to_string(), file("alpha.txt", 3)),
            ("gamma.txt".to_string(), file("gamma.txt", 4)),
        ]);

        let diff = Tree::diff(&previous, &current);

        match diff.entries.get("alpha.txt") {
            Some(TreeChange::File(file)) => assert_eq!(file.path, "alpha.txt"),
            other => panic!("expected changed alpha.txt file entry, got {other:?}"),
        }
        match diff.entries.get("beta.txt") {
            Some(TreeChange::Delete) => {}
            other => panic!("expected beta.txt delete entry, got {other:?}"),
        }
        match diff.entries.get("gamma.txt") {
            Some(TreeChange::File(file)) => assert_eq!(file.path, "gamma.txt"),
            other => panic!("expected gamma.txt file entry, got {other:?}"),
        }
    }

    #[test]
    fn tree_diff_nests_changes_under_directories() {
        let previous = BTreeMap::from([
            ("docs/guide.md".to_string(), file("docs/guide.md", 1)),
            ("docs/old.md".to_string(), file("docs/old.md", 2)),
        ]);
        let current = BTreeMap::from([
            ("docs/guide.md".to_string(), file("docs/guide.md", 3)),
            ("docs/new.md".to_string(), file("docs/new.md", 4)),
        ]);

        let diff = Tree::diff(&previous, &current);
        let docs = match diff.entries.get("docs") {
            Some(TreeChange::Tree(diff)) => diff,
            other => panic!("expected nested docs tree diff, got {other:?}"),
        };

        match docs.entries.get("guide.md") {
            Some(TreeChange::File(file)) => assert_eq!(file.path, "docs/guide.md"),
            other => panic!("expected changed guide.md file entry, got {other:?}"),
        }
        match docs.entries.get("old.md") {
            Some(TreeChange::Delete) => {}
            other => panic!("expected old.md delete entry, got {other:?}"),
        }
        match docs.entries.get("new.md") {
            Some(TreeChange::File(file)) => assert_eq!(file.path, "docs/new.md"),
            other => panic!("expected new.md file entry, got {other:?}"),
        }
    }

    #[test]
    fn delta_hash_changes_when_payload_changes() {
        let mut first = Delta {
            hash: [0; 32],
            seqno: 7,
            base_seqno: 6,
            device_id: "device-a".into(),
            device_seqno: 3,
            timestamp: UNIX_EPOCH,
            changes: vec![Change::UpdateFile(FileChange {
                path: "hello.txt".into(),
                base_file: None,
                file: file("hello.txt", 1),
            })],
        };
        let first_hash = first.update_hash();

        let mut second = first.clone();
        second.changes = vec![Change::UpdateFile(FileChange {
            path: "hello.txt".into(),
            base_file: None,
            file: file("hello.txt", 2),
        })];
        let second_hash = second.update_hash();

        assert_ne!(first_hash, [0; 32]);
        assert_ne!(first_hash, second_hash);
    }
}
