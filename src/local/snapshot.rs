//! Local snapshot construction helpers.
//!
//! This module wraps an existing root tree hash in snapshot metadata.

use std::time::SystemTime;

use crate::core::{Snapshot, SnapshotHash};

/// Assembles snapshot metadata once a root tree hash is already known.
pub fn build_snapshot(
    device_id: &str,
    tree_hash: [u8; 32],
    parent_snapshot: Option<&SnapshotHash>,
) -> Snapshot {
    let base = parent_snapshot.copied().unwrap_or([0; 32]);
    let mut snapshot = Snapshot {
        hash: [0; 32],
        parents: vec![base],
        tree_hash,
        origin: device_id.to_string(),
        timestamp: SystemTime::now(),
    };
    snapshot.update_hash();
    snapshot
}
