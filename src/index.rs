//! In-memory tree index for path lookup and incremental updates.
//!
//! The persisted `Tree` objects are a good storage format, but they are awkward to mutate path by
//! path. `TreeIndex` keeps the current directory graph in memory so the engine can resolve paths,
//! update a subtree, recompute ancestor hashes, and collect the metadata objects that now need to
//! be saved.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

use crate::core::{File, FileHash, FileKind, Object, ObjectHash, Tree, TreeEntry, TreeHash};
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
    pub fn load_from_root<LoadTree, LoadFile>(
        root: Tree,
        load_tree: &mut LoadTree,
        load_file: &mut LoadFile,
    ) -> Result<Self>
    where
        LoadTree: FnMut(TreeHash) -> Result<Tree>,
        LoadFile: FnMut(FileHash) -> Result<File>,
    {
        let mut index = Self::empty();
        index.load_tree_node(index.root, root, load_tree, load_file)?;
        Ok(index)
    }

    /// Resolve a relative path to the indexed node id.
    pub fn resolve_path(&self, path: &Path) -> Result<Option<NodeId>> {
        let mut current = self.root;

        for name in path_components(path)? {
            let node = self.nodes.get(&current).ok_or_else(|| {
                SyncError::InvalidState("tree index node is missing from the node map".into())
            })?;

            let NodeKind::Tree { children, .. } = &node.kind else {
                return Ok(None);
            };

            let Some(next) = children.get(&name) else {
                return Ok(None);
            };
            current = *next;
        }

        Ok(Some(current))
    }

    /// Returns the current file metadata at one relative path, if it exists as a file.
    pub fn file_at_path(&self, path: &Path) -> Result<Option<File>> {
        let Some(node_id) = self.resolve_path(path)? else {
            return Ok(None);
        };

        let node = self.node(node_id)?;
        match &node.kind {
            NodeKind::File { file } => Ok(Some(file.clone())),
            NodeKind::Tree { .. } => Ok(None),
        }
    }

    /// Insert or replace a file node at a relative path.
    pub fn upsert_file(&mut self, path: &Path, file: File) -> Result<TreeUpdate> {
        let components = path_components(path)?;
        let Some(name) = components.last().cloned() else {
            return Err(SyncError::InvalidState(
                "tree index expected a non-empty file path".into(),
            ));
        };

        let parent = self.ensure_tree_path(&components[..components.len() - 1])?;
        let mut changed_files = BTreeSet::new();
        let deleted_paths = Vec::new();

        let replaced = self.child(parent, &name)?;
        if let Some(existing) = replaced {
            self.remove_subtree(existing);
        }

        let file_id = self.alloc_node(Node {
            parent: Some(parent),
            name: name.clone(),
            kind: NodeKind::File { file },
        });
        self.insert_child(parent, name, file_id)?;
        changed_files.insert(file_id);

        let changed_trees = self.rehash_ancestors(parent)?;
        self.build_update(changed_trees, changed_files, deleted_paths)
    }

    /// Ensure a directory exists at a relative path.
    pub fn ensure_directory(&mut self, path: &Path) -> Result<TreeUpdate> {
        let components = path_components(path)?;
        if components.is_empty() {
            return self.build_update(BTreeSet::new(), BTreeSet::new(), Vec::new());
        }

        let mut current = self.root;
        let mut created = false;

        for name in components {
            if let Some(child) = self.child(current, &name)? {
                let child_node = self.node(child)?;
                match child_node.kind {
                    NodeKind::Tree { .. } => {
                        current = child;
                        continue;
                    }
                    NodeKind::File { .. } => {
                        return Err(SyncError::InvalidState(
                            "tree index expected a directory path".into(),
                        ));
                    }
                }
            }

            let child_id = self.alloc_node(Node {
                parent: Some(current),
                name: name.clone(),
                kind: NodeKind::Tree {
                    hash: TreeHash::default(),
                    children: BTreeMap::new(),
                },
            });
            self.insert_child(current, name, child_id)?;
            current = child_id;
            created = true;
        }

        if !created {
            return self.build_update(BTreeSet::new(), BTreeSet::new(), Vec::new());
        }

        let changed_trees = self.rehash_ancestors(current)?;
        self.build_update(changed_trees, BTreeSet::new(), Vec::new())
    }

    /// Remove a file or subtree at a relative path.
    pub fn remove_path(&mut self, path: &Path) -> Result<TreeUpdate> {
        let components = path_components(path)?;
        let Some(node_id) = self.resolve_path(path)? else {
            return self.build_update(BTreeSet::new(), BTreeSet::new(), Vec::new());
        };
        if node_id == self.root {
            return Err(SyncError::InvalidState(
                "tree index cannot remove the root path".into(),
            ));
        }

        let deleted_path = join_components(&components);
        let parent = self
            .parent(node_id)?
            .ok_or_else(|| SyncError::InvalidState("tree index node is missing a parent".into()))?;
        let name = self.node(node_id)?.name.clone();
        self.remove_child(parent, &name)?;
        self.remove_subtree(node_id);

        let changed_trees = self.prune_and_rehash(parent)?;
        self.build_update(changed_trees, BTreeSet::new(), vec![deleted_path])
    }

    /// Move a file or subtree to a new relative path.
    pub fn move_path(&mut self, from: &Path, to: &Path) -> Result<TreeUpdate> {
        let from_components = path_components(from)?;
        let to_components = path_components(to)?;
        if from_components.is_empty() || to_components.is_empty() {
            return Err(SyncError::InvalidState(
                "tree index expected non-empty move paths".into(),
            ));
        }

        let node_id = self.resolve_path(from)?.ok_or(SyncError::NotFound)?;
        if node_id == self.root {
            return Err(SyncError::InvalidState(
                "tree index cannot move the root path".into(),
            ));
        }

        if self.path_is_descendant(node_id, &to_components)? {
            return Err(SyncError::InvalidState(
                "tree index cannot move a path into its own subtree".into(),
            ));
        }

        let old_parent = self
            .parent(node_id)?
            .ok_or_else(|| SyncError::InvalidState("tree index node is missing a parent".into()))?;
        let old_name = self.node(node_id)?.name.clone();
        self.remove_child(old_parent, &old_name)?;

        let new_parent = self.ensure_tree_path(&to_components[..to_components.len() - 1])?;
        let new_name = to_components.last().cloned().unwrap();
        if let Some(existing) = self.child(new_parent, &new_name)? {
            self.remove_child(new_parent, &new_name)?;
            self.remove_subtree(existing);
        }

        {
            let node = self.node_mut(node_id)?;
            node.parent = Some(new_parent);
            node.name = new_name.clone();
        }
        self.insert_child(new_parent, new_name, node_id)?;

        let mut changed_files = BTreeSet::new();
        let mut changed_trees = self.refresh_subtree_paths(node_id, &mut changed_files)?;
        changed_trees.extend(self.prune_and_rehash(old_parent)?);
        changed_trees.extend(self.rehash_ancestors(new_parent)?);

        self.build_update(
            changed_trees,
            changed_files,
            vec![join_components(&from_components)],
        )
    }

    /// Materialize the current indexed state into persisted metadata objects.
    pub fn materialize_all(&self) -> Result<TreeUpdate> {
        let mut files = Vec::new();
        let mut trees = Vec::new();
        self.collect_materialized(self.root, &mut files, &mut trees)?;

        let root = trees
            .iter()
            .find(|tree| tree.hash == self.tree_hash(self.root).unwrap_or_default())
            .cloned()
            .unwrap_or_else(Tree::empty);

        Ok(TreeUpdate {
            root,
            trees,
            files,
            deleted_paths: Vec::new(),
        })
    }

    fn load_tree_node<LoadTree, LoadFile>(
        &mut self,
        node_id: NodeId,
        tree: Tree,
        load_tree: &mut LoadTree,
        load_file: &mut LoadFile,
    ) -> Result<()>
    where
        LoadTree: FnMut(TreeHash) -> Result<Tree>,
        LoadFile: FnMut(FileHash) -> Result<File>,
    {
        let mut children = BTreeMap::new();

        for entry in tree.entries {
            match entry.hash {
                ObjectHash::Tree(hash) => {
                    let child_id = self.alloc_node(Node {
                        parent: Some(node_id),
                        name: entry.name.clone(),
                        kind: NodeKind::Tree {
                            hash,
                            children: BTreeMap::new(),
                        },
                    });
                    children.insert(entry.name.clone(), child_id);
                    let child_tree = load_tree(hash)?;
                    self.load_tree_node(child_id, child_tree, load_tree, load_file)?;
                }
                ObjectHash::File(hash) => {
                    let file = load_file(hash)?;
                    let child_id = self.alloc_node(Node {
                        parent: Some(node_id),
                        name: entry.name.clone(),
                        kind: NodeKind::File { file },
                    });
                    children.insert(entry.name, child_id);
                }
            }
        }

        let node = self.node_mut(node_id)?;
        node.kind = NodeKind::Tree {
            hash: tree.hash,
            children,
        };
        Ok(())
    }

    fn ensure_tree_path(&mut self, components: &[String]) -> Result<NodeId> {
        let mut current = self.root;
        for name in components {
            if let Some(child) = self.child(current, name)? {
                current = child;
                continue;
            }

            let child_id = self.alloc_node(Node {
                parent: Some(current),
                name: name.clone(),
                kind: NodeKind::Tree {
                    hash: TreeHash::default(),
                    children: BTreeMap::new(),
                },
            });
            self.insert_child(current, name.clone(), child_id)?;
            current = child_id;
        }
        Ok(current)
    }

    fn prune_and_rehash(&mut self, mut current: NodeId) -> Result<BTreeSet<NodeId>> {
        while current != self.root && self.child_count(current)? == 0 {
            let parent = self.parent(current)?.ok_or_else(|| {
                SyncError::InvalidState("tree index node is missing a parent".into())
            })?;
            let name = self.node(current)?.name.clone();
            self.remove_child(parent, &name)?;
            self.nodes.remove(&current);
            current = parent;
        }

        self.rehash_ancestors(current)
    }

    fn refresh_subtree_paths(
        &mut self,
        node_id: NodeId,
        changed_files: &mut BTreeSet<NodeId>,
    ) -> Result<BTreeSet<NodeId>> {
        let mut changed_trees = BTreeSet::new();
        match self.node(node_id)?.kind.clone() {
            NodeKind::File { mut file } => {
                let path = self.node_path(node_id)?;
                file.path = path;
                file.update_hash();
                self.node_mut(node_id)?.kind = NodeKind::File { file };
                changed_files.insert(node_id);
            }
            NodeKind::Tree { children, .. } => {
                for child_id in children.into_values() {
                    changed_trees.extend(self.refresh_subtree_paths(child_id, changed_files)?);
                }
                self.rehash_tree(node_id)?;
                changed_trees.insert(node_id);
            }
        }
        Ok(changed_trees)
    }

    fn rehash_ancestors(&mut self, mut current: NodeId) -> Result<BTreeSet<NodeId>> {
        let mut changed = BTreeSet::new();
        loop {
            self.rehash_tree(current)?;
            changed.insert(current);

            let Some(parent) = self.parent(current)? else {
                break;
            };
            current = parent;
        }
        Ok(changed)
    }

    fn rehash_tree(&mut self, node_id: NodeId) -> Result<()> {
        let tree = self.materialize_tree(node_id)?;
        let node = self.node_mut(node_id)?;
        let NodeKind::Tree { hash, .. } = &mut node.kind else {
            return Err(SyncError::InvalidState(
                "tree index expected a tree node during rehash".into(),
            ));
        };
        *hash = tree.hash;
        Ok(())
    }

    fn materialize_tree(&self, node_id: NodeId) -> Result<Tree> {
        let node = self.node(node_id)?;
        let NodeKind::Tree { children, .. } = &node.kind else {
            return Err(SyncError::InvalidState(
                "tree index expected a tree node during materialization".into(),
            ));
        };

        if node_id == self.root && children.is_empty() {
            return Ok(Tree::empty());
        }

        let mut entries = Vec::with_capacity(children.len());
        for (name, child_id) in children {
            let child = self.node(*child_id)?;
            let hash = match &child.kind {
                NodeKind::Tree { hash, .. } => ObjectHash::Tree(*hash),
                NodeKind::File { file } => ObjectHash::File(file.hash),
            };
            let kind = match child.kind {
                NodeKind::Tree { .. } => FileKind::Folder,
                NodeKind::File { .. } => FileKind::File,
            };
            entries.push(TreeEntry {
                name: name.clone(),
                kind,
                hash,
            });
        }

        let mut tree = Tree {
            hash: [0; 32],
            entries,
        };
        tree.update_hash();
        Ok(tree)
    }

    fn collect_materialized(
        &self,
        node_id: NodeId,
        files: &mut Vec<File>,
        trees: &mut Vec<Tree>,
    ) -> Result<()> {
        match &self.node(node_id)?.kind {
            NodeKind::File { file } => files.push(file.clone()),
            NodeKind::Tree { children, .. } => {
                for child_id in children.values() {
                    self.collect_materialized(*child_id, files, trees)?;
                }
                trees.push(self.materialize_tree(node_id)?);
            }
        }
        Ok(())
    }

    fn build_update(
        &self,
        changed_trees: BTreeSet<NodeId>,
        changed_files: BTreeSet<NodeId>,
        deleted_paths: Vec<String>,
    ) -> Result<TreeUpdate> {
        let root = self.materialize_tree(self.root)?;
        let mut trees = Vec::new();
        for node_id in changed_trees {
            trees.push(self.materialize_tree(node_id)?);
        }

        let mut files = Vec::new();
        for node_id in changed_files {
            let NodeKind::File { file } = &self.node(node_id)?.kind else {
                return Err(SyncError::InvalidState(
                    "tree index expected a file node while building the update".into(),
                ));
            };
            files.push(file.clone());
        }

        Ok(TreeUpdate {
            root,
            trees,
            files,
            deleted_paths,
        })
    }

    fn path_is_descendant(&self, node_id: NodeId, path: &[String]) -> Result<bool> {
        let moved_root = self.node_path(node_id)?;
        let target = join_components(path);
        Ok(!moved_root.is_empty()
            && target != moved_root
            && target.starts_with(&(moved_root.clone() + "/")))
    }

    fn node_path(&self, node_id: NodeId) -> Result<String> {
        let mut names = Vec::new();
        let mut current = node_id;
        loop {
            let node = self.node(current)?;
            if current != self.root {
                names.push(node.name.clone());
            }
            let Some(parent) = node.parent else {
                break;
            };
            current = parent;
        }
        names.reverse();
        Ok(names.join("/"))
    }

    fn alloc_node(&mut self, node: Node) -> NodeId {
        let node_id = self.next_id;
        self.next_id += 1;
        self.nodes.insert(node_id, node);
        node_id
    }

    fn remove_subtree(&mut self, node_id: NodeId) {
        let Some(node) = self.nodes.remove(&node_id) else {
            return;
        };

        if let NodeKind::Tree { children, .. } = node.kind {
            for child_id in children.into_values() {
                self.remove_subtree(child_id);
            }
        }
    }

    fn child(&self, parent: NodeId, name: &str) -> Result<Option<NodeId>> {
        let node = self.node(parent)?;
        let NodeKind::Tree { children, .. } = &node.kind else {
            return Err(SyncError::InvalidState(
                "tree index expected a tree parent".into(),
            ));
        };
        Ok(children.get(name).copied())
    }

    fn insert_child(&mut self, parent: NodeId, name: String, child: NodeId) -> Result<()> {
        let node = self.node_mut(parent)?;
        let NodeKind::Tree { children, .. } = &mut node.kind else {
            return Err(SyncError::InvalidState(
                "tree index expected a tree parent".into(),
            ));
        };
        children.insert(name, child);
        Ok(())
    }

    fn remove_child(&mut self, parent: NodeId, name: &str) -> Result<()> {
        let node = self.node_mut(parent)?;
        let NodeKind::Tree { children, .. } = &mut node.kind else {
            return Err(SyncError::InvalidState(
                "tree index expected a tree parent".into(),
            ));
        };
        children.remove(name);
        Ok(())
    }

    fn child_count(&self, node_id: NodeId) -> Result<usize> {
        let node = self.node(node_id)?;
        let NodeKind::Tree { children, .. } = &node.kind else {
            return Err(SyncError::InvalidState(
                "tree index expected a tree node".into(),
            ));
        };
        Ok(children.len())
    }

    fn tree_hash(&self, node_id: NodeId) -> Result<TreeHash> {
        let node = self.node(node_id)?;
        let NodeKind::Tree { hash, .. } = node.kind else {
            return Err(SyncError::InvalidState(
                "tree index expected a tree node".into(),
            ));
        };
        Ok(hash)
    }

    fn parent(&self, node_id: NodeId) -> Result<Option<NodeId>> {
        Ok(self.node(node_id)?.parent)
    }

    fn node(&self, node_id: NodeId) -> Result<&Node> {
        self.nodes
            .get(&node_id)
            .ok_or_else(|| SyncError::InvalidState("tree index node is missing".into()))
    }

    fn node_mut(&mut self, node_id: NodeId) -> Result<&mut Node> {
        self.nodes
            .get_mut(&node_id)
            .ok_or_else(|| SyncError::InvalidState("tree index node is missing".into()))
    }
}

fn path_components(path: &Path) -> Result<Vec<String>> {
    path.components().map(component_name).collect()
}

fn component_name(component: Component<'_>) -> Result<String> {
    match component {
        Component::Normal(name) => Ok(name.to_string_lossy().into_owned()),
        _ => Err(SyncError::InvalidState(
            "tree index expected relative file paths".into(),
        )),
    }
}

fn join_components(components: &[String]) -> String {
    components.join("/")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{NodeKind, TreeIndex, TreeUpdate};
    use crate::core::{File, FileHash, FileKind, Object, ObjectHash, Tree, TreeEntry, TreeHash};
    use crate::services::{Result, SyncError};

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

    #[test]
    fn loads_nested_tree_and_resolves_paths() {
        let (root, objects) = nested_objects();
        let mut load_tree = |hash| load_tree_object(&objects, hash);
        let mut load_file = |hash| load_file_object(&objects, hash);
        let index =
            TreeIndex::load_from_root(root.clone(), &mut load_tree, &mut load_file).unwrap();

        assert_eq!(index.resolve_path(Path::new("")).unwrap(), Some(index.root));
        assert!(index.resolve_path(Path::new("docs")).unwrap().is_some());
        assert!(
            index
                .resolve_path(Path::new("docs/guide.md"))
                .unwrap()
                .is_some()
        );
        assert!(
            index
                .resolve_path(Path::new("missing.txt"))
                .unwrap()
                .is_none()
        );
        assert_eq!(index.materialize_all().unwrap().root, root);
    }

    #[test]
    fn upsert_file_creates_missing_parents_and_updates_root() {
        let mut index = TreeIndex::empty();
        let file = file("docs/guide.md", 1);

        let update = index
            .upsert_file(Path::new("docs/guide.md"), file.clone())
            .unwrap();

        assert!(index.resolve_path(Path::new("docs")).unwrap().is_some());
        assert!(
            index
                .resolve_path(Path::new("docs/guide.md"))
                .unwrap()
                .is_some()
        );
        assert_eq!(update.files, vec![file.clone()]);
        assert_eq!(update.deleted_paths, Vec::<String>::new());
        assert_eq!(update.root, materialized_root(&index));
        assert_eq!(update.trees.len(), 2);
    }

    #[test]
    fn ensure_directory_creates_missing_parents_and_updates_root() {
        let mut index = TreeIndex::empty();

        let update = index.ensure_directory(Path::new("docs/guides")).unwrap();

        assert!(index.resolve_path(Path::new("docs")).unwrap().is_some());
        assert!(
            index
                .resolve_path(Path::new("docs/guides"))
                .unwrap()
                .is_some()
        );
        assert_eq!(update.files, Vec::<File>::new());
        assert_eq!(update.deleted_paths, Vec::<String>::new());
        assert_eq!(update.root, materialized_root(&index));
        assert_eq!(update.trees.len(), 3);
    }

    #[test]
    fn ensure_directory_is_noop_for_existing_directory() {
        let mut index = TreeIndex::empty();
        index.ensure_directory(Path::new("docs")).unwrap();

        let update = index.ensure_directory(Path::new("docs")).unwrap();

        assert_eq!(update.files, Vec::<File>::new());
        assert_eq!(update.trees, Vec::<Tree>::new());
        assert_eq!(update.deleted_paths, Vec::<String>::new());
        assert_eq!(update.root, materialized_root(&index));
    }

    #[test]
    fn remove_path_prunes_empty_directories() {
        let (root, objects) = nested_objects();
        let mut load_tree = |hash| load_tree_object(&objects, hash);
        let mut load_file = |hash| load_file_object(&objects, hash);
        let mut index = TreeIndex::load_from_root(root, &mut load_tree, &mut load_file).unwrap();

        let update = index.remove_path(Path::new("docs/guide.md")).unwrap();

        assert!(index.resolve_path(Path::new("docs")).unwrap().is_none());
        assert!(
            index
                .resolve_path(Path::new("docs/guide.md"))
                .unwrap()
                .is_none()
        );
        assert_eq!(update.deleted_paths, vec!["docs/guide.md".to_string()]);
        assert_eq!(update.files, Vec::<File>::new());
        assert_eq!(update.root, Tree::empty());
        assert_eq!(update.trees, vec![Tree::empty()]);
    }

    #[test]
    fn move_path_updates_descendant_file_paths() {
        let (root, objects) = nested_objects();
        let mut load_tree = |hash| load_tree_object(&objects, hash);
        let mut load_file = |hash| load_file_object(&objects, hash);
        let mut index = TreeIndex::load_from_root(root, &mut load_tree, &mut load_file).unwrap();

        let update = index
            .move_path(Path::new("docs"), Path::new("guides"))
            .unwrap();

        assert!(index.resolve_path(Path::new("docs")).unwrap().is_none());
        assert!(index.resolve_path(Path::new("guides")).unwrap().is_some());
        assert!(
            index
                .resolve_path(Path::new("guides/guide.md"))
                .unwrap()
                .is_some()
        );
        assert_eq!(update.deleted_paths, vec!["docs".to_string()]);
        assert_eq!(update.files.len(), 1);
        assert_eq!(update.files[0].path, "guides/guide.md");
        assert_eq!(update.root, materialized_root(&index));
    }

    fn nested_objects() -> (Tree, Vec<Object>) {
        let file = file("docs/guide.md", 1);
        let mut docs = Tree {
            hash: [0; 32],
            entries: vec![TreeEntry {
                name: "guide.md".into(),
                kind: FileKind::File,
                hash: ObjectHash::File(file.hash),
            }],
        };
        docs.update_hash();

        let mut root = Tree {
            hash: [0; 32],
            entries: vec![TreeEntry {
                name: "docs".into(),
                kind: FileKind::Folder,
                hash: ObjectHash::Tree(docs.hash),
            }],
        };
        root.update_hash();

        let objects = vec![
            Object::Tree(root.clone()),
            Object::Tree(docs),
            Object::File(file),
        ];
        (root, objects)
    }

    fn file(path: &str, tag: u8) -> File {
        let mut file = File {
            path: path.into(),
            hash: [0; 32],
            blobs: vec![[tag; 32]],
        };
        file.update_hash();
        file
    }

    fn load_tree_object(objects: &[Object], hash: TreeHash) -> Result<Tree> {
        for object in objects {
            if let Object::Tree(tree) = object {
                if tree.hash == hash {
                    return Ok(tree.clone());
                }
            }
        }
        Err(SyncError::NotFound)
    }

    fn load_file_object(objects: &[Object], hash: FileHash) -> Result<File> {
        for object in objects {
            if let Object::File(file) = object {
                if file.hash == hash {
                    return Ok(file.clone());
                }
            }
        }
        Err(SyncError::NotFound)
    }

    fn materialized_root(index: &TreeIndex) -> Tree {
        index.materialize_all().unwrap().root
    }
}
