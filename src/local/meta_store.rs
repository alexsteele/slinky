use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params};

use crate::core::{Config, DeviceState, Frontier};
use crate::local::util::{decode_hash, device_root, encode_hash};
use crate::services::{MetaStore, Result, SyncError};

pub struct LocalMetaStore {
    config: Config,
    conn: Mutex<Connection>,
}

impl LocalMetaStore {
    pub fn open(config: Config) -> Result<Arc<dyn MetaStore>> {
        let root = device_root(&config)?;
        std::fs::create_dir_all(&root)?;
        let db_path = root.join("meta.db");
        let conn = Connection::open(db_path).map_err(|err| SyncError::InvalidState(err.to_string()))?;
        conn.execute_batch(
            r#"
            create table if not exists device_state (
                repo_id text not null,
                device_id text not null,
                snapshot text not null,
                primary key (repo_id, device_id)
            );
            "#,
        )
        .map_err(|err| SyncError::InvalidState(err.to_string()))?;

        Ok(Arc::new(Self {
            config,
            conn: Mutex::new(conn),
        }))
    }
}

#[async_trait]
impl MetaStore for LocalMetaStore {
    async fn load_config(&self) -> Result<Config> {
        Ok(self.config.clone())
    }

    async fn load_state(&self, repo_id: &String, device_id: &String) -> Result<DeviceState> {
        let conn = self.conn.lock().unwrap();
        let snapshot = conn
            .query_row(
                "select snapshot from device_state where repo_id = ?1 and device_id = ?2",
                params![repo_id, device_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|err| SyncError::InvalidState(err.to_string()))?;

        Ok(DeviceState {
            snapshot: snapshot
                .map(|value| decode_hash(&value))
                .transpose()?
                .unwrap_or([0; 32]),
            frontier: Frontier::default(),
        })
    }

    async fn save_state(
        &self,
        repo_id: &String,
        device_id: &String,
        state: &DeviceState,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            insert into device_state (repo_id, device_id, snapshot)
            values (?1, ?2, ?3)
            on conflict(repo_id, device_id) do update set snapshot = excluded.snapshot
            "#,
            params![repo_id, device_id, encode_hash(&state.snapshot)],
        )
        .map_err(|err| SyncError::InvalidState(err.to_string()))?;
        Ok(())
    }
}
