//! Relay interface for publishing and receiving sync history.
//!
//! This module holds the network-facing contract between devices and the relay. Keeping it
//! separate from the other service traits makes the relay boundary easier to find and evolve.

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::core::{
    ChangeSet, Checkpoint, CheckpointAnnouncement, Config, Delta, DeltaAnnouncement, DeltaWindow,
    DeviceId, Frontier, PeerState, RepoId, SeqNo, Snapshot, SnapshotAnnouncement, SnapshotHash,
};
use crate::services::{Result, SyncError};

#[async_trait]
pub trait Relay: Send + Sync {
    /// Starts a device authentication handshake with a relay-issued challenge.
    async fn auth_challenge(
        &self,
        _repo_id: &RepoId,
        _device_id: &DeviceId,
    ) -> Result<AuthChallenge> {
        Err(SyncError::InvalidState(
            "request_auth_challenge not implemented".into(),
        ))
    }

    /// Completes a challenge-response handshake and returns an authenticated session.
    async fn authenticate(&self, _response: &AuthResponse) -> Result<RelaySession> {
        Err(SyncError::InvalidState(
            "authenticate not implemented".into(),
        ))
    }

    /// Ensures the relay knows about this device before sync begins.
    async fn register_device(&self, config: &Config) -> Result<()>;

    /// Subscribe to pushed relay notifications for a device.
    async fn subscribe(&self, repo_id: &RepoId, device_id: &DeviceId) -> Result<RelayEventStream>;

    /// Publish a new snapshot plus its journal diff to the relay.
    async fn publish_snapshot(&self, snapshot: &Snapshot, change_set: &ChangeSet) -> Result<()>;

    /// Publish one ordered delta into the relay-backed log.
    ///
    /// The hybrid model expects this to become the normal hot path once the engine is migrated.
    async fn publish_delta(&self, _delta: &Delta) -> Result<()> {
        Err(SyncError::InvalidState(
            "publish_delta not implemented".into(),
        ))
    }

    /// Publish a checkpoint that binds a local snapshot to a relay seqno.
    async fn publish_checkpoint(&self, _checkpoint: &Checkpoint) -> Result<()> {
        Err(SyncError::InvalidState(
            "publish_checkpoint not implemented".into(),
        ))
    }

    /// Fetch snapshot metadata by hash.
    async fn fetch_snapshot(&self, repo_id: &RepoId, hash: &SnapshotHash) -> Result<Snapshot>;

    /// Fetch the ChangeSet between `base` and `target`.
    async fn fetch_change_set(
        &self,
        repo_id: &RepoId,
        base: &SnapshotHash,
        target: &SnapshotHash,
    ) -> Result<ChangeSet>;

    /// Fetch one delta by its ordered seqno.
    async fn fetch_delta(&self, _repo_id: &RepoId, _seqno: SeqNo) -> Result<Delta> {
        Err(SyncError::NotFound)
    }

    /// Fetch deltas after one seqno, up to and including another.
    async fn fetch_deltas(
        &self,
        _repo_id: &RepoId,
        _from_exclusive: SeqNo,
        _to_inclusive: SeqNo,
    ) -> Result<Vec<Delta>> {
        Ok(Vec::new())
    }

    /// Fetch deltas for one replay window using `(from, to]` semantics.
    async fn fetch_delta_window(
        &self,
        repo_id: &RepoId,
        window: &DeltaWindow,
    ) -> Result<Vec<Delta>> {
        self.fetch_deltas(repo_id, window.from_exclusive, window.to_inclusive)
            .await
    }

    /// Fetch the latest checkpoint at or before the requested seqno.
    async fn fetch_checkpoint(
        &self,
        _repo_id: &RepoId,
        _upto_seqno: SeqNo,
    ) -> Result<Option<Checkpoint>> {
        Ok(None)
    }

    /// Fetch the relay's current ordered log head for a repo.
    async fn fetch_head_seqno(&self, _repo_id: &RepoId) -> Result<SeqNo> {
        Ok(0)
    }

    /// Fetch the latest known device frontier for a repo.
    async fn fetch_frontier(&self, repo_id: &RepoId) -> Result<Frontier>;
}

/// Push notification from the relay control plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayEvent {
    Snapshot(SnapshotAnnouncement),
    Delta(DeltaAnnouncement),
    Checkpoint(CheckpointAnnouncement),
    PeerAvailable(PeerState),
}

pub type RelayEventStream = mpsc::Receiver<RelayEvent>;

/// One relay-issued authentication challenge for a device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthChallenge {
    pub challenge_id: String,
    pub repo_id: RepoId,
    pub device_id: DeviceId,
    pub nonce: String,
}

/// One signed response to a relay challenge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthResponse {
    pub challenge_id: String,
    pub public_key: String,
    pub signature: String,
}

/// One authenticated relay session for subsequent requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelaySession {
    pub session_id: String,
    pub device_id: DeviceId,
    pub repo_id: RepoId,
}
