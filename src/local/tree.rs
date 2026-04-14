//! Local tree construction helpers.
//!
//! This module turns a local sync root into immutable tree/file/blob metadata. The tree builder
//! here is intentionally the simple full-root path used for bootstrap and rebuild flows.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path};
use std::sync::Arc;

use async_trait::async_trait;
use crate::core::{
    Blob, BlobHash, File, FileKind, FullBlob, Object, ObjectHash, Tree, TreeEntry,
};
use crate::engine::{StageJob, StageJobResult};
use crate::local::util::{hash_bytes, walk_files};
use crate::services::{BlobStore, Result, StageWorker, StagedTree, SyncError, TreeBuilder};

/// Builds a full local tree view from the configured sync root.
///
/// As each file is read and hashed, blob bytes are staged into the local blob store so later
/// upload work can run by hash. This favors a simple bootstrap path; incremental steady-state
/// staging can be introduced separately.
pub struct LocalTreeBuilder {
    blob_store: Arc<dyn BlobStore>,
}

impl LocalTreeBuilder {
    pub fn new(blob_store: Arc<dyn BlobStore>) -> Self {
        Self { blob_store }
    }
}

#[async_trait]
impl TreeBuilder for LocalTreeBuilder {
    async fn build_tree(&self, root_path: &Path) -> Result<StagedTree> {
        let mut objects = Vec::new();
        let mut blob_hashes = Vec::new();
        let mut root_paths = Vec::new();
        let mut pending_root = PendingTree::default();

        walk_files(root_path, root_path, &mut root_paths)?;

        for path in root_paths {
            let full_path = root_path.join(&path);
            let data = fs::read(&full_path)?;
            let blob_hash: BlobHash = hash_bytes(&data);
            let mut file = File {
                path: path.to_string_lossy().into_owned(),
                hash: [0; 32],
                blobs: vec![blob_hash],
            };
            file.update_hash();
            let blob = Blob {
                hash: blob_hash,
                size: data.len() as u64,
            };
            self.blob_store
                .put_blob(&FullBlob {
                    hash: blob_hash,
                    size: data.len() as u64,
                    data,
                })
                .await?;

            blob_hashes.push(blob_hash);
            objects.push(Object::Blob(blob));
            pending_root.insert(&path, file)?;
        }

        let (root, mut tree_objects) = pending_root.finalize();
        objects.append(&mut tree_objects);

        Ok(StagedTree {
            root,
            objects,
            blob_hashes,
        })
    }
}

#[async_trait]
impl StageWorker for LocalTreeBuilder {
    async fn run_stage_job(&self, _job: &StageJob) -> Result<StageJobResult> {
        Err(SyncError::InvalidState(
            "stage worker flow needs an explicit root/subtree plan".into(),
        ))
    }
}

#[derive(Default)]
struct PendingTree {
    trees: BTreeMap<String, PendingTree>,
    files: BTreeMap<String, File>,
}

impl PendingTree {
    /// Inserts one relative file path into the in-memory tree being assembled.
    fn insert(&mut self, path: &Path, file: File) -> Result<()> {
        let mut current = self;
        let mut components = path.components().peekable();
        let mut pending_file = Some(file);

        while let Some(component) = components.next() {
            let name = component_name(component)?;
            if components.peek().is_some() {
                current = current.trees.entry(name).or_default();
            } else {
                let file = pending_file.take().ok_or_else(|| {
                    SyncError::InvalidState("missing file metadata during tree insert".into())
                })?;
                current.files.insert(name, file);
            }
        }

        Ok(())
    }

    /// Materializes immutable tree and file objects bottom-up so each tree hash is stable.
    fn finalize(self) -> (Tree, Vec<Object>) {
        let mut entries = Vec::new();
        let mut objects = Vec::new();

        for (name, child) in self.trees {
            let (tree, mut child_objects) = child.finalize();
            entries.push(TreeEntry {
                name,
                kind: FileKind::Folder,
                hash: ObjectHash::Tree(tree.hash),
            });
            objects.append(&mut child_objects);
            objects.push(Object::Tree(tree));
        }

        for (name, file) in self.files {
            entries.push(TreeEntry {
                name,
                kind: FileKind::File,
                hash: ObjectHash::File(file.hash),
            });
            objects.push(Object::File(file));
        }

        let mut tree = Tree {
            hash: [0; 32],
            entries,
        };
        tree.update_hash();
        (tree, objects)
    }
}

fn component_name(component: Component<'_>) -> Result<String> {
    match component {
        Component::Normal(name) => Ok(name.to_string_lossy().into_owned()),
        _ => Err(SyncError::InvalidState(
            "tree builder expected relative file paths".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use tempfile::tempdir;

    use super::LocalTreeBuilder;
    use crate::core::{BlobHash, FileKind, FullBlob, Object, ObjectHash};
    use crate::services::{BlobStore, Result, TreeBuilder};

    struct MemoryBlobStore {
        blobs: Mutex<std::collections::BTreeMap<BlobHash, Vec<u8>>>,
    }

    impl MemoryBlobStore {
        fn new() -> Self {
            Self {
                blobs: Mutex::new(std::collections::BTreeMap::new()),
            }
        }
    }

    #[async_trait]
    impl BlobStore for MemoryBlobStore {
        async fn put_blob(&self, blob: &FullBlob) -> Result<()> {
            self.blobs.lock().unwrap().insert(blob.hash, blob.data.clone());
            Ok(())
        }

        async fn get_blob(&self, hash: &BlobHash) -> Result<Vec<u8>> {
            self.blobs
                .lock()
                .unwrap()
                .get(hash)
                .cloned()
                .ok_or(crate::services::SyncError::NotFound)
        }

        async fn has_blob(&self, hash: &BlobHash) -> Result<bool> {
            Ok(self.blobs.lock().unwrap().contains_key(hash))
        }
    }

    #[tokio::test]
    async fn builds_tree_for_flat_root() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("alpha.txt"), b"alpha").unwrap();
        std::fs::write(dir.path().join("beta.txt"), b"beta").unwrap();

        let builder = LocalTreeBuilder::new(std::sync::Arc::new(MemoryBlobStore::new()));
        let staged = builder.build_tree(dir.path()).await.unwrap();

        assert_eq!(staged.root.entries.len(), 2);
        assert_eq!(staged.blob_hashes.len(), 2);

        let names = staged
            .root
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(names, BTreeSet::from(["alpha.txt", "beta.txt"]));
        assert!(staged
            .root
            .entries
            .iter()
            .all(|entry| entry.kind == FileKind::File));

        let file_paths = staged
            .objects
            .iter()
            .filter_map(|object| match object {
                Object::File(file) => Some(file.path.as_str()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(file_paths, BTreeSet::from(["alpha.txt", "beta.txt"]));
    }

    #[tokio::test]
    async fn builds_nested_tree_and_stages_blob_content() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("docs/guides")).unwrap();
        std::fs::write(dir.path().join("docs/readme.md"), b"readme").unwrap();
        std::fs::write(dir.path().join("docs/guides/intro.md"), b"intro").unwrap();

        let blob_store = std::sync::Arc::new(MemoryBlobStore::new());
        let builder = LocalTreeBuilder::new(blob_store.clone());
        let staged = builder.build_tree(dir.path()).await.unwrap();

        assert_eq!(staged.blob_hashes.len(), 2);

        let docs_entry = staged
            .root
            .entries
            .iter()
            .find(|entry| entry.name == "docs")
            .unwrap();
        assert_eq!(docs_entry.kind, FileKind::Folder);

        let docs_tree_hash = match docs_entry.hash {
            ObjectHash::Tree(hash) => hash,
            ObjectHash::File(_) => panic!("expected docs to be a tree"),
        };

        let docs_tree = staged
            .objects
            .iter()
            .find_map(|object| match object {
                Object::Tree(tree) if tree.hash == docs_tree_hash => Some(tree),
                _ => None,
            })
            .unwrap();

        let docs_names = docs_tree
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(docs_names, BTreeSet::from(["guides", "readme.md"]));

        for hash in &staged.blob_hashes {
            assert!(blob_store.has_blob(hash).await.unwrap());
            assert!(!blob_store.get_blob(hash).await.unwrap().is_empty());
        }
    }

    #[allow(dead_code)]
    fn _assert_relative(path: &str) {
        assert!(Path::new(path).is_relative());
    }
}
