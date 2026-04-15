use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{from_str, to_string};

use crate::core::{BlobHash, Config, File, FileKind, Object, ObjectHash, Tree, TreeEntry};
use crate::local::util::{device_root, encode_hash, hash_bytes, walk_files};
use crate::services::{ObjStore, Result, SyncError};

pub struct LocalObjStore {
    config: Config,
    conn: Mutex<Connection>,
}

impl LocalObjStore {
    pub fn open(config: &Config) -> Result<Arc<dyn ObjStore>> {
        let root = device_root(config)?;
        std::fs::create_dir_all(&root)?;
        let db_path = root.join("objects.db");
        let conn =
            Connection::open(db_path).map_err(|err| SyncError::InvalidState(err.to_string()))?;
        conn.execute_batch(
            r#"
            create table if not exists objects (
                id text primary key,
                kind text not null,
                data text not null
            );
            "#,
        )
        .map_err(|err| SyncError::InvalidState(err.to_string()))?;

        Ok(Arc::new(Self {
            config: config.clone(),
            conn: Mutex::new(conn),
        }))
    }

    async fn save_tree_from_root(&self, expected: &Tree) -> Result<()> {
        let mut objects = Vec::new();
        let mut root_paths = Vec::new();
        let mut pending_root = PendingTree::default();

        walk_files(
            &self.config.sync_root,
            &self.config.sync_root,
            &mut root_paths,
        )?;

        for path in root_paths {
            let full_path = self.config.sync_root.join(&path);
            let data = fs::read(&full_path)?;
            let blob_hash: BlobHash = hash_bytes(&data);
            let mut file = File {
                path: path.to_string_lossy().into_owned(),
                hash: [0; 32],
                blobs: vec![blob_hash],
            };
            file.update_hash();
            pending_root.insert(&path, file)?;
        }

        let (root, mut tree_objects) = pending_root.finalize();
        if root.hash != expected.hash {
            return Err(SyncError::InvalidState(
                "persisted tree did not match built tree".into(),
            ));
        }

        objects.append(&mut tree_objects);
        for object in &objects {
            self.insert_object(object)?;
        }
        self.insert_object(&Object::Tree(root))?;

        Ok(())
    }

    fn insert_object(&self, object: &Object) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let data = to_string(object).map_err(|err| SyncError::InvalidState(err.to_string()))?;
        conn.execute(
            "insert or replace into objects (id, kind, data) values (?1, ?2, ?3)",
            params![object_key(&object.id()), object_kind(object), data],
        )
        .map_err(|err| SyncError::InvalidState(err.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl ObjStore for LocalObjStore {
    async fn load_object(&self, id: &crate::core::ObjectId) -> Result<Object> {
        let conn = self.conn.lock().unwrap();
        let data = conn
            .query_row(
                "select data from objects where id = ?1",
                params![object_key(id)],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|err| SyncError::InvalidState(err.to_string()))?;

        match data {
            Some(data) => from_str(&data).map_err(|err| SyncError::InvalidState(err.to_string())),
            None => Err(SyncError::NotFound),
        }
    }

    async fn save_object(&self, object: &Object) -> Result<()> {
        match object {
            Object::Tree(tree) => self.save_tree_from_root(tree).await,
            _ => self.insert_object(object),
        }
    }
}

#[derive(Default)]
struct PendingTree {
    trees: BTreeMap<String, PendingTree>,
    files: BTreeMap<String, File>,
}

impl PendingTree {
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

fn object_key(id: &crate::core::ObjectId) -> String {
    match id {
        crate::core::ObjectId::Snapshot(hash) => format!("snapshot:{}", encode_hash(hash)),
        crate::core::ObjectId::Tree(hash) => format!("tree:{}", encode_hash(hash)),
        crate::core::ObjectId::File(hash) => format!("file:{}", encode_hash(hash)),
        crate::core::ObjectId::Blob(hash) => format!("blob:{}", encode_hash(hash)),
    }
}

fn object_kind(object: &Object) -> &'static str {
    match object {
        Object::Snapshot(_) => "snapshot",
        Object::Tree(_) => "tree",
        Object::File(_) => "file",
        Object::Blob(_) => "blob",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tempfile::tempdir;

    use super::LocalObjStore;
    use crate::core::{Config, DeviceCredentials, Object, ObjectHash, ObjectId};
    use crate::local::LocalTreeBuilder;
    use crate::services::TreeBuilder;

    #[tokio::test]
    async fn saves_tree_objects_without_blob_data() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("docs/guides")).unwrap();
        std::fs::write(dir.path().join("docs/readme.md"), b"readme").unwrap();
        std::fs::write(dir.path().join("docs/guides/intro.md"), b"intro").unwrap();

        let builder = LocalTreeBuilder::new(crate::local::LocalChunker::open());
        let obj_store = LocalObjStore::open(&Config {
            sync_root: dir.path().to_path_buf(),
            repo_id: format!("repo-{}", dir.path().display()),
            device_id: format!("device-{}", dir.path().display()),
            credentials: DeviceCredentials {
                public_key: "pub".into(),
                private_key_path: dir.path().join("device.key"),
            },
        })
        .unwrap();

        let tree = builder.build_tree(dir.path()).await.unwrap();
        obj_store
            .save_object(&Object::Tree(tree.clone()))
            .await
            .unwrap();

        let docs_entry = tree
            .entries
            .iter()
            .find(|entry| entry.name == "docs")
            .unwrap();
        let docs_tree_hash = match docs_entry.hash {
            ObjectHash::Tree(hash) => hash,
            ObjectHash::File(_) => panic!("expected docs to be a tree"),
        };

        let docs_tree = match obj_store
            .load_object(&ObjectId::Tree(docs_tree_hash))
            .await
            .unwrap()
        {
            Object::Tree(tree) => tree,
            _ => panic!("expected tree object"),
        };

        let docs_names = docs_tree
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(docs_names, BTreeSet::from(["guides", "readme.md"]));

        let readme_hash = match docs_tree
            .entries
            .iter()
            .find(|entry| entry.name == "readme.md")
            .unwrap()
            .hash
        {
            ObjectHash::File(hash) => hash,
            ObjectHash::Tree(_) => panic!("expected file object"),
        };
        let guides_hash = match docs_tree
            .entries
            .iter()
            .find(|entry| entry.name == "guides")
            .unwrap()
            .hash
        {
            ObjectHash::Tree(hash) => hash,
            ObjectHash::File(_) => panic!("expected tree object"),
        };
        let guides_tree = match obj_store
            .load_object(&ObjectId::Tree(guides_hash))
            .await
            .unwrap()
        {
            Object::Tree(tree) => tree,
            _ => panic!("expected tree object"),
        };
        let intro_hash = match guides_tree.entries[0].hash {
            ObjectHash::File(hash) => hash,
            ObjectHash::Tree(_) => panic!("expected file object"),
        };

        let file_paths = [
            obj_store
                .load_object(&ObjectId::File(readme_hash))
                .await
                .unwrap(),
            obj_store
                .load_object(&ObjectId::File(intro_hash))
                .await
                .unwrap(),
        ]
        .into_iter()
        .filter_map(|object| match object {
            Object::File(file) => Some(file.path),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
        assert_eq!(
            file_paths,
            BTreeSet::from([
                "docs/guides/intro.md".to_string(),
                "docs/readme.md".to_string()
            ])
        );

        let intro_file = match obj_store
            .load_object(&ObjectId::File(intro_hash))
            .await
            .unwrap()
        {
            Object::File(file) => file,
            _ => panic!("expected file object"),
        };
        for hash in &intro_file.blobs {
            assert!(matches!(
                obj_store.load_object(&ObjectId::Blob(*hash)).await,
                Err(crate::services::SyncError::NotFound)
            ));
        }
    }
}
