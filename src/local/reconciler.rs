use async_trait::async_trait;

use crate::core::{Frontier, Snapshot};
use crate::services::{ApplyPlan, Reconciler, Result};

pub struct NoopReconciler;

#[async_trait]
impl Reconciler for NoopReconciler {
    async fn plan(
        &self,
        _local: Option<&Snapshot>,
        remote: &Snapshot,
        _frontier: &Frontier,
    ) -> Result<ApplyPlan> {
        Ok(ApplyPlan {
            target_snapshot: remote.hash,
            ops: Vec::new(),
        })
    }
}
