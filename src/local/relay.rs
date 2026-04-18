use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::core::{ChangeSet, Checkpoint, Config, Delta, Frontier, SeqNo, Snapshot, SnapshotHash};
use crate::relay::{Relay, RelayEventStream};
use crate::services::{Result, SyncError};

pub struct NoopRelay;

#[async_trait]
impl Relay for NoopRelay {
    async fn register_device(&self, _config: &Config) -> Result<()> {
        Ok(())
    }

    async fn subscribe(&self, _repo_id: &String, _device_id: &String) -> Result<RelayEventStream> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(rx)
    }

    async fn publish_snapshot(&self, _snapshot: &Snapshot, _change_set: &ChangeSet) -> Result<()> {
        Ok(())
    }

    async fn publish_delta(&self, _delta: &Delta) -> Result<()> {
        Ok(())
    }

    async fn publish_checkpoint(&self, _checkpoint: &Checkpoint) -> Result<()> {
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

    async fn fetch_delta(&self, _repo_id: &String, _seqno: SeqNo) -> Result<Delta> {
        Err(SyncError::NotFound)
    }

    async fn fetch_deltas(
        &self,
        _repo_id: &String,
        _from_exclusive: SeqNo,
        _to_inclusive: SeqNo,
    ) -> Result<Vec<Delta>> {
        Ok(Vec::new())
    }

    async fn fetch_checkpoint(
        &self,
        _repo_id: &String,
        _upto_seqno: SeqNo,
    ) -> Result<Option<Checkpoint>> {
        Ok(None)
    }

    async fn fetch_head_seqno(&self, _repo_id: &String) -> Result<SeqNo> {
        Ok(0)
    }

    async fn fetch_frontier(&self, _repo_id: &String) -> Result<Frontier> {
        Ok(Frontier::default())
    }
}
