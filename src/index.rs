//! In-memory tree index for path lookup and incremental updates.
//!
//! The persisted `Tree` objects are a good storage format, but they are awkward to mutate path by
//! path. `TreeIndex` sketches the engine-side structure we can use to resolve paths, update a
//! subtree, and then re-materialize hashed `Tree` objects for persistence.

use std::collections::BTreeMap;
use std::path::Path;

use crate::core::{File, Object, Tree, TreeHash};
use crate::services::{Result, SyncError};

pub type NodeId = u64;

/// Mutable index over the current tree state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeIndex {
    pub root: NodeId,
    pub next_id: NodeId,
    pub nodes: BTreeMap<NodeId, Node>,
}

/// Result of applying one tree mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeUpdate {
    pub root: Tree,
    pub trees: Vec<Tree>,
    pub files: Vec<File>,
    pub deleted_paths: Vec<String>,
}

/// One indexed file or directory node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub parent: Option<NodeId>,
    pub name: String,
    pub kind: NodeKind,
}

/// In-memory content for a file or directory node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeKind {
    Tree {
        hash: TreeHash,
        children: BTreeMap<String, NodeId>,
    },
    File {
        file: File,
    },
}

impl TreeUpdate {
    /// Flatten the changed metadata into object-store save order.
    pub fn objects(&self) -> Vec<Object> {
        let mut objects = Vec::with_capacity(self.files.len() + self.trees.len());
        for file in &self.files {
            objects.push(Object::File(file.clone()));
        }
        for tree in &self.trees {
            objects.push(Object::Tree(tree.clone()));
        }
        objects
    }
}

impl Node {
    fn root() -> Self {
        Self {
            parent: None,
            name: String::new(),
            kind: NodeKind::Tree {
                hash: TreeHash::default(),
                children: BTreeMap::new(),
            },
        }
    }
}

impl TreeIndex {
    /// Create an empty index with a single root directory node.
    pub fn empty() -> Self {
        let root = 1;
        let mut nodes = BTreeMap::new();
        nodes.insert(root, Node::root());

        Self {
            root,
            next_id: root + 1,
            nodes,
        }
    }

    /// Build an index from a persisted root tree.
    pub fn load_from_root(_root: Tree) -> Result<Self> {
        todo!("build a tree index from the persisted root tree")
    }

    /// Resolve a relative path to the indexed node id.
    pub fn resolve_path(&self, path: &Path) -> Result<Option<NodeId>> {
        let mut current = self.root;

        for component in path.components() {
            let name = match component {
                std::path::Component::Normal(name) => name.to_string_lossy(),
                _ => {
                    return Err(SyncError::InvalidState(
                        "tree index expected relative file paths".into(),
                    ));
                }
            };

            let node = self.nodes.get(&current).ok_or_else(|| {
                SyncError::InvalidState("tree index node is missing from the node map".into())
            })?;

            let NodeKind::Tree { children, .. } = &node.kind else {
                return Ok(None);
            };

            let Some(next) = children.get(name.as_ref()) else {
                return Ok(None);
            };
            current = *next;
        }

        Ok(Some(current))
    }

    /// Insert or replace a file node at a relative path.
    pub fn upsert_file(&mut self, _path: &Path, _file: File) -> Result<TreeUpdate> {
        todo!("upsert a file and refresh ancestor tree hashes")
    }

    /// Remove a file or subtree at a relative path.
    pub fn remove_path(&mut self, _path: &Path) -> Result<TreeUpdate> {
        todo!("remove a path, prune empty directories, and refresh ancestor tree hashes")
    }

    /// Move a file or subtree to a new relative path.
    pub fn move_path(&mut self, _from: &Path, _to: &Path) -> Result<TreeUpdate> {
        todo!("move a path and refresh both affected ancestor chains")
    }

    /// Materialize the current indexed state into persisted metadata objects.
    pub fn materialize_all(&self) -> Result<TreeUpdate> {
        todo!("materialize the indexed state into persisted tree and file objects")
    }
}

#[cfg(test)]
mod tests {
    use super::{NodeKind, TreeIndex, TreeUpdate};
    use crate::core::{File, Object, Tree};

    #[test]
    fn empty_index_has_root_tree_node() {
        let index = TreeIndex::empty();
        let root = index.nodes.get(&index.root).unwrap();

        assert_eq!(index.root, 1);
        assert_eq!(index.next_id, 2);
        assert_eq!(root.parent, None);
        assert_eq!(root.name, "");
        assert!(matches!(
            root.kind,
            NodeKind::Tree {
                ref children,
                ..
            } if children.is_empty()
        ));
    }

    #[test]
    fn tree_update_flattens_files_before_trees() {
        let file = File {
            path: "hello.txt".into(),
            hash: [1; 32],
            blobs: vec![[2; 32]],
        };
        let tree = Tree {
            hash: [3; 32],
            entries: Vec::new(),
        };
        let update = TreeUpdate {
            root: tree.clone(),
            trees: vec![tree.clone()],
            files: vec![file.clone()],
            deleted_paths: vec!["old.txt".into()],
        };

        assert_eq!(
            update.objects(),
            vec![Object::File(file), Object::Tree(tree)]
        );
    }
}
