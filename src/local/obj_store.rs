use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{from_str, to_string};

use crate::core::{Config, Object, ObjectId};
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
        let conn = Connection::open(db_path).map_err(|err| SyncError::InvalidState(err.to_string()))?;
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
}

#[async_trait]
impl ObjStore for LocalObjStore {
    async fn load_object(&self, id: &ObjectId) -> Result<Object> {
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

fn object_key(id: &ObjectId) -> String {
    match id {
        ObjectId::Snapshot(hash) => format!("snapshot:{}", encode_hash(hash)),
        ObjectId::Tree(hash) => format!("tree:{}", encode_hash(hash)),
        ObjectId::File(hash) => format!("file:{}", encode_hash(hash)),
        ObjectId::Blob(hash) => format!("blob:{}", encode_hash(hash)),
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
