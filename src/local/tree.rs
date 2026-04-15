//! Local tree construction helpers.

use std::collections::BTreeMap;
use std::path::{Component, Path};
use std::sync::Arc;

use async_trait::async_trait;

use crate::core::{File, FileKind, Object, ObjectHash, Tree, TreeEntry};
use crate::local::util::walk_files;
use crate::services::{Chunker, Result, SyncError, TreeBuilder};

/// Builds the current root tree for a local sync root.
pub struct LocalTreeBuilder {
    chunker: Arc<dyn Chunker>,
}

impl LocalTreeBuilder {
    pub fn new(chunker: Arc<dyn Chunker>) -> Self {
        Self { chunker }
    }
}

#[async_trait]
impl TreeBuilder for LocalTreeBuilder {
    async fn build_tree(&self, root_path: &Path) -> Result<Tree> {
        let mut root_paths = Vec::new();
        let mut pending_root = PendingTree::default();

        walk_files(root_path, root_path, &mut root_paths)?;

        for path in root_paths {
            let full_path = root_path.join(&path);
            let mut reader = self.chunker.open(&full_path).await?;
            let mut blobs = Vec::new();
            while let Some(chunk) = reader.next_chunk().await? {
                blobs.push(chunk.blob.hash);
            }
            let mut file = File {
                path: path.to_string_lossy().into_owned(),
                hash: [0; 32],
                blobs,
            };
            file.update_hash();
            pending_root.insert(&path, file)?;
        }

        let (root, _objects) = pending_root.finalize();
        Ok(root)
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
        if self.trees.is_empty() && self.files.is_empty() {
            return (Tree::empty(), Vec::new());
        }

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
    use tempfile::tempdir;

    use super::LocalTreeBuilder;
    use crate::core::FileKind;
    use crate::services::TreeBuilder;

    #[tokio::test]
    async fn builds_tree_for_flat_root() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("alpha.txt"), b"alpha").unwrap();
        std::fs::write(dir.path().join("beta.txt"), b"beta").unwrap();

        let builder = LocalTreeBuilder::new(crate::local::LocalChunker::open());
        let tree = builder.build_tree(dir.path()).await.unwrap();

        assert_eq!(tree.entries.len(), 2);

        let names = tree
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(names, BTreeSet::from(["alpha.txt", "beta.txt"]));
        assert!(
            tree.entries
                .iter()
                .all(|entry| entry.kind == FileKind::File)
        );
    }

    #[tokio::test]
    async fn builds_nested_tree() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("docs/guides")).unwrap();
        std::fs::write(dir.path().join("docs/readme.md"), b"readme").unwrap();
        std::fs::write(dir.path().join("docs/guides/intro.md"), b"intro").unwrap();

        let builder = LocalTreeBuilder::new(crate::local::LocalChunker::open());
        let tree = builder.build_tree(dir.path()).await.unwrap();

        let docs_entry = tree
            .entries
            .iter()
            .find(|entry| entry.name == "docs")
            .unwrap();
        assert_eq!(docs_entry.kind, FileKind::Folder);
    }

    #[allow(dead_code)]
    fn _assert_relative(path: &str) {
        assert!(Path::new(path).is_relative());
    }
}
