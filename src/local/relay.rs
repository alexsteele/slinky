use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::core::{
    ChangeSet, Checkpoint, Config, Delta, Frontier, SeqNo, Snapshot, SnapshotAnnouncement,
    SnapshotHash,
};
use crate::relay::{Relay, RelayEvent, RelayEventStream};
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

/// Shared in-memory relay for multi-device tests and local end-to-end runs.
pub struct MemoryRelay {
    state: Mutex<MemoryRelayState>,
}

struct MemoryRelayState {
    device_repos: BTreeMap<String, String>,
    snapshots: BTreeMap<[u8; 32], Snapshot>,
    change_sets: BTreeMap<([u8; 32], [u8; 32]), ChangeSet>,
    deltas: BTreeMap<u64, Delta>,
    subscribers: BTreeMap<String, Vec<mpsc::Sender<RelayEvent>>>,
    frontier: BTreeMap<String, Frontier>,
    head_seqno: u64,
}

impl MemoryRelay {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(MemoryRelayState {
                device_repos: BTreeMap::new(),
                snapshots: BTreeMap::new(),
                change_sets: BTreeMap::new(),
                deltas: BTreeMap::new(),
                subscribers: BTreeMap::new(),
                frontier: BTreeMap::new(),
                head_seqno: 0,
            }),
        })
    }

    fn repo_for_device(state: &MemoryRelayState, device_id: &str) -> Result<String> {
        state
            .device_repos
            .get(device_id)
            .cloned()
            .ok_or_else(|| SyncError::InvalidState("device is not registered with relay".into()))
    }
}

#[async_trait]
impl Relay for MemoryRelay {
    async fn register_device(&self, config: &Config) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state
            .device_repos
            .insert(config.device_id.clone(), config.repo_id.clone());
        state.frontier.entry(config.repo_id.clone()).or_default();
        Ok(())
    }

    async fn subscribe(&self, repo_id: &String, _device_id: &String) -> Result<RelayEventStream> {
        let (tx, rx) = mpsc::channel(32);
        let mut state = self.state.lock().unwrap();
        state
            .subscribers
            .entry(repo_id.clone())
            .or_default()
            .push(tx);
        Ok(rx)
    }

    async fn publish_snapshot(&self, snapshot: &Snapshot, change_set: &ChangeSet) -> Result<()> {
        let (repo_id, subscribers) = {
            let mut state = self.state.lock().unwrap();
            let repo_id = Self::repo_for_device(&state, &snapshot.origin)?;
            state.snapshots.insert(snapshot.hash, snapshot.clone());
            state
                .change_sets
                .insert((change_set.base, change_set.target), change_set.clone());
            state
                .frontier
                .entry(repo_id.clone())
                .or_default()
                .device_snapshots
                .insert(snapshot.origin.clone(), snapshot.hash);
            let subscribers = state.subscribers.get(&repo_id).cloned().unwrap_or_default();
            (repo_id, subscribers)
        };

        let event = RelayEvent::Snapshot(SnapshotAnnouncement {
            repo_id,
            snapshot: snapshot.hash,
            device: snapshot.origin.clone(),
        });
        for subscriber in subscribers {
            let _ = subscriber.send(event.clone()).await;
        }
        Ok(())
    }

    async fn publish_delta(&self, delta: &Delta) -> Result<()> {
        let subscribers = {
            let mut state = self.state.lock().unwrap();
            let repo_id = Self::repo_for_device(&state, &delta.device_id)?;
            state.head_seqno = state.head_seqno.max(delta.seqno);
            state.deltas.insert(delta.seqno, delta.clone());
            state.subscribers.get(&repo_id).cloned().unwrap_or_default()
        };

        for subscriber in subscribers {
            let _ = subscriber.send(RelayEvent::Delta(delta.clone())).await;
        }
        Ok(())
    }

    async fn publish_checkpoint(&self, _checkpoint: &Checkpoint) -> Result<()> {
        Ok(())
    }

    async fn fetch_snapshot(&self, _repo_id: &String, hash: &SnapshotHash) -> Result<Snapshot> {
        self.state
            .lock()
            .unwrap()
            .snapshots
            .get(hash)
            .cloned()
            .ok_or(SyncError::NotFound)
    }

    async fn fetch_change_set(
        &self,
        _repo_id: &String,
        base: &SnapshotHash,
        target: &SnapshotHash,
    ) -> Result<ChangeSet> {
        self.state
            .lock()
            .unwrap()
            .change_sets
            .get(&(*base, *target))
            .cloned()
            .ok_or(SyncError::NotFound)
    }

    async fn fetch_delta(&self, _repo_id: &String, seqno: SeqNo) -> Result<Delta> {
        self.state
            .lock()
            .unwrap()
            .deltas
            .get(&seqno)
            .cloned()
            .ok_or(SyncError::NotFound)
    }

    async fn fetch_deltas(
        &self,
        _repo_id: &String,
        from_exclusive: SeqNo,
        to_inclusive: SeqNo,
    ) -> Result<Vec<Delta>> {
        let state = self.state.lock().unwrap();
        let mut deltas = Vec::new();
        for seqno in (from_exclusive + 1)..=to_inclusive {
            let Some(delta) = state.deltas.get(&seqno) else {
                break;
            };
            deltas.push(delta.clone());
        }
        Ok(deltas)
    }

    async fn fetch_checkpoint(
        &self,
        _repo_id: &String,
        _upto_seqno: SeqNo,
    ) -> Result<Option<Checkpoint>> {
        Ok(None)
    }

    async fn fetch_head_seqno(&self, _repo_id: &String) -> Result<SeqNo> {
        Ok(self.state.lock().unwrap().head_seqno)
    }

    async fn fetch_frontier(&self, repo_id: &String) -> Result<Frontier> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .frontier
            .get(repo_id)
            .cloned()
            .unwrap_or_default())
    }
}
