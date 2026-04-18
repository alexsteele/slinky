use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{from_str, to_string};

use crate::core::{Config, Object};
use crate::local::util::{device_root, encode_hash};
use crate::services::{ObjStore, Result, SyncError};

pub struct LocalObjStore {
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
            conn: Mutex::new(conn),
        }))
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
        self.insert_object(object)
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
    use crate::core::{
        Config, DeviceCredentials, File, FileKind, Object, ObjectHash, ObjectId, Tree, TreeEntry,
    };
    use crate::local::util::hash_bytes;

    #[tokio::test]
    async fn saves_tree_objects_without_blob_data() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("docs/guides")).unwrap();
        std::fs::write(dir.path().join("docs/readme.md"), b"readme").unwrap();
        std::fs::write(dir.path().join("docs/guides/intro.md"), b"intro").unwrap();

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

        let mut readme = File {
            path: "docs/readme.md".into(),
            hash: [0; 32],
            blobs: vec![hash_bytes(b"readme")],
        };
        readme.update_hash();
        let mut intro = File {
            path: "docs/guides/intro.md".into(),
            hash: [0; 32],
            blobs: vec![hash_bytes(b"intro")],
        };
        intro.update_hash();

        let mut guides_tree = Tree {
            hash: [0; 32],
            entries: vec![TreeEntry {
                name: "intro.md".into(),
                kind: FileKind::File,
                hash: ObjectHash::File(intro.hash),
            }],
        };
        guides_tree.update_hash();

        let mut docs_tree = Tree {
            hash: [0; 32],
            entries: vec![
                TreeEntry {
                    name: "guides".into(),
                    kind: FileKind::Folder,
                    hash: ObjectHash::Tree(guides_tree.hash),
                },
                TreeEntry {
                    name: "readme.md".into(),
                    kind: FileKind::File,
                    hash: ObjectHash::File(readme.hash),
                },
            ],
        };
        docs_tree.update_hash();

        let mut root = Tree {
            hash: [0; 32],
            entries: vec![TreeEntry {
                name: "docs".into(),
                kind: FileKind::Folder,
                hash: ObjectHash::Tree(docs_tree.hash),
            }],
        };
        root.update_hash();

        for object in [
            Object::File(readme.clone()),
            Object::File(intro.clone()),
            Object::Tree(guides_tree.clone()),
            Object::Tree(docs_tree.clone()),
            Object::Tree(root.clone()),
        ] {
            obj_store.save_object(&object).await.unwrap();
        }

        let docs_entry = root
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
