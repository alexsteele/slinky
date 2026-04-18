use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params};

use crate::core::{Config, DeviceState, Frontier, SeqNo, SnapshotHash};
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
                local_snapshot text,
                applied_seqno integer not null default 0,
                accepted_seqno integer not null default 0,
                next_revision integer not null default 1,
                primary key (repo_id, device_id)
            );

            create table if not exists relay_state (
                repo_id text primary key,
                head_seqno integer not null
            );

            create table if not exists checkpoints (
                repo_id text not null,
                seqno integer not null,
                snapshot text not null,
                primary key (repo_id, seqno)
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
        ensure_column(&conn, "device_state", "local_snapshot", "text")?;
        ensure_column(
            &conn,
            "device_state",
            "applied_seqno",
            "integer not null default 0",
        )?;
        ensure_column(
            &conn,
            "device_state",
            "accepted_seqno",
            "integer not null default 0",
        )?;
        ensure_column(
            &conn,
            "device_state",
            "next_revision",
            "integer not null default 1",
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
                r#"
                select
                    snapshot,
                    published_snapshot,
                    local_snapshot,
                    applied_seqno,
                    accepted_seqno,
                    next_revision
                from device_state
                where repo_id = ?1 and device_id = ?2
                "#,
                params![repo_id, device_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(|err| SyncError::InvalidState(err.to_string()))?;

        let (
            snapshot,
            published_snapshot,
            local_snapshot,
            applied_seqno,
            accepted_seqno,
            next_revision,
        ) = match state {
            Some((
                snapshot,
                published_snapshot,
                local_snapshot,
                applied_seqno,
                accepted_seqno,
                _next_revision,
            )) => {
                let snapshot = decode_hash(&snapshot)?;
                let published_snapshot = if published_snapshot.is_empty() {
                    snapshot
                } else {
                    decode_hash(&published_snapshot)?
                };
                let local_snapshot = decode_optional_hash(local_snapshot)?;
                (
                    snapshot,
                    published_snapshot,
                    local_snapshot.or(Some(snapshot)),
                    applied_seqno as SeqNo,
                    accepted_seqno as SeqNo,
                    1,
                )
            }
            None => ([0; 32], [0; 32], None, 0, 0, 1),
        };

        Ok(DeviceState {
            snapshot,
            published_snapshot,
            local_snapshot,
            applied_seqno,
            accepted_seqno,
            next_revision,
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
            insert into device_state (
                repo_id,
                device_id,
                snapshot,
                published_snapshot,
                local_snapshot,
                applied_seqno,
                accepted_seqno,
                next_revision
            )
            values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            on conflict(repo_id, device_id) do update set
                snapshot = excluded.snapshot,
                published_snapshot = excluded.published_snapshot,
                local_snapshot = excluded.local_snapshot,
                applied_seqno = excluded.applied_seqno,
                accepted_seqno = excluded.accepted_seqno,
                next_revision = excluded.next_revision
            "#,
            params![
                repo_id,
                device_id,
                encode_hash(&state.snapshot),
                encode_hash(&state.published_snapshot),
                encode_optional_hash(state.local_snapshot.as_ref()),
                state.applied_seqno as i64,
                state.accepted_seqno as i64,
                state.next_revision as i64,
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

fn decode_optional_hash(value: Option<String>) -> Result<Option<SnapshotHash>> {
    match value {
        Some(value) if !value.is_empty() => Ok(Some(decode_hash(&value)?)),
        _ => Ok(None),
    }
}

fn encode_optional_hash(value: Option<&SnapshotHash>) -> Option<String> {
    value.map(encode_hash)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::LocalMetaStore;
    use crate::core::{Config, DeviceCredentials};
    use crate::local::util::device_root;

    #[test]
    fn opens_meta_db_with_hybrid_tables_and_columns() {
        let dir = tempdir().unwrap();
        let config = Config {
            sync_root: dir.path().join("sync"),
            repo_id: "repo-1".into(),
            device_id: "device-1".into(),
            credentials: DeviceCredentials {
                public_key: "pub".into(),
                private_key_path: dir.path().join("keys/device.key"),
            },
        };
        std::fs::create_dir_all(config.credentials.private_key_path.parent().unwrap()).unwrap();

        LocalMetaStore::open(config.clone()).unwrap();

        let conn = Connection::open(device_root(&config).unwrap().join("meta.db")).unwrap();
        let tables = load_table_names(&conn);
        assert!(tables.contains("device_state"));
        assert!(tables.contains("relay_state"));
        assert!(tables.contains("checkpoints"));

        let columns = load_column_names(&conn, "device_state");
        assert!(columns.contains("snapshot"));
        assert!(columns.contains("published_snapshot"));
        assert!(columns.contains("local_snapshot"));
        assert!(columns.contains("applied_seqno"));
        assert!(columns.contains("accepted_seqno"));
        assert!(columns.contains("next_revision"));
    }

    fn load_table_names(conn: &Connection) -> BTreeSet<String> {
        let mut stmt = conn
            .prepare("select name from sqlite_master where type = 'table'")
            .unwrap();
        stmt.query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|row| row.unwrap())
            .collect()
    }

    fn load_column_names(conn: &Connection, table: &str) -> BTreeSet<String> {
        let mut stmt = conn
            .prepare(&format!("pragma table_info({table})"))
            .unwrap();
        stmt.query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(|row| row.unwrap())
            .collect()
    }
}
