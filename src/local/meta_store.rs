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
        let conn =
            Connection::open(db_path).map_err(|err| SyncError::InvalidState(err.to_string()))?;
        conn.execute_batch(
            r#"
            create table if not exists device_state (
                repo_id text not null,
                device_id text not null,
                snapshot text not null,
                published_snapshot text not null,
                primary key (repo_id, device_id)
            );
            "#,
        )
        .map_err(|err| SyncError::InvalidState(err.to_string()))?;
        ensure_column(
            &conn,
            "device_state",
            "published_snapshot",
            "text not null default ''",
        )?;

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
        let state = conn
            .query_row(
                "select snapshot, published_snapshot from device_state where repo_id = ?1 and device_id = ?2",
                params![repo_id, device_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                    ))
                },
            )
            .optional()
            .map_err(|err| SyncError::InvalidState(err.to_string()))?;

        let (snapshot, published_snapshot) = match state {
            Some((snapshot, published_snapshot)) => (
                decode_hash(&snapshot)?,
                if published_snapshot.is_empty() {
                    decode_hash(&snapshot)?
                } else {
                    decode_hash(&published_snapshot)?
                },
            ),
            None => ([0; 32], [0; 32]),
        };

        Ok(DeviceState {
            snapshot,
            published_snapshot,
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
            insert into device_state (repo_id, device_id, snapshot, published_snapshot)
            values (?1, ?2, ?3, ?4)
            on conflict(repo_id, device_id) do update set
                snapshot = excluded.snapshot,
                published_snapshot = excluded.published_snapshot
            "#,
            params![
                repo_id,
                device_id,
                encode_hash(&state.snapshot),
                encode_hash(&state.published_snapshot),
            ],
        )
        .map_err(|err| SyncError::InvalidState(err.to_string()))?;
        Ok(())
    }
}

fn ensure_column(conn: &Connection, table: &str, column: &str, definition: &str) -> Result<()> {
    let mut stmt = conn
        .prepare(&format!("pragma table_info({table})"))
        .map_err(|err| SyncError::InvalidState(err.to_string()))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|err| SyncError::InvalidState(err.to_string()))?;

    for existing in columns {
        let existing = existing.map_err(|err| SyncError::InvalidState(err.to_string()))?;
        if existing == column {
            return Ok(());
        }
    }

    conn.execute(
        &format!("alter table {table} add column {column} {definition}"),
        [],
    )
    .map_err(|err| SyncError::InvalidState(err.to_string()))?;
    Ok(())
}
