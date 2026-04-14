use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::core::{ChangeSet, Config, Frontier, Snapshot, SnapshotHash};
use crate::services::{Coordinator, CoordinatorNotificationStream, Result, SyncError};

pub struct NoopCoordinator;

#[async_trait]
impl Coordinator for NoopCoordinator {
    async fn register_device(&self, _config: &Config) -> Result<()> {
        Ok(())
    }

    async fn subscribe(
        &self,
        _repo_id: &String,
        _device_id: &String,
    ) -> Result<CoordinatorNotificationStream> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(rx)
    }

    async fn publish_snapshot(&self, _snapshot: &Snapshot, _change_set: &ChangeSet) -> Result<()> {
        Ok(())
    }

    async fn fetch_snapshot(&self, _repo_id: &String, _hash: &SnapshotHash) -> Result<Snapshot> {
        Err(SyncError::NotFound)
    }

    async fn fetch_change_set(
        &self,
        _repo_id: &String,
        _base: &SnapshotHash,
        _target: &SnapshotHash,
    ) -> Result<ChangeSet> {
        Err(SyncError::NotFound)
    }

    async fn fetch_frontier(&self, _repo_id: &String) -> Result<Frontier> {
        Ok(Frontier::default())
    }
}
