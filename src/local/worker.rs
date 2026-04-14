use std::sync::Arc;

use async_trait::async_trait;

use crate::engine::{ApplyJob, BlobTransferJob, BlobTransferResult};
use crate::services::{Applier, ApplyWorker, BlobStore, BlobTransferWorker, Result};

pub struct NoopApplier;

#[async_trait]
impl Applier for NoopApplier {
    async fn apply(&self, _plan: &crate::services::ApplyPlan) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ApplyWorker for NoopApplier {
    async fn run_apply_job(&self, _job: &ApplyJob) -> Result<()> {
        Ok(())
    }
}

pub struct LocalBlobTransferWorker {
    blob_store: Arc<dyn BlobStore>,
}

impl LocalBlobTransferWorker {
    pub fn new(blob_store: Arc<dyn BlobStore>) -> Self {
        Self { blob_store }
    }
}

#[async_trait]
impl BlobTransferWorker for LocalBlobTransferWorker {
    async fn upload_blobs(&self, job: &BlobTransferJob) -> Result<BlobTransferResult> {
        let mut present = Vec::new();
        for blob in &job.blobs {
            if self.blob_store.has_blob(blob).await? {
                present.push(*blob);
            }
        }
        Ok(BlobTransferResult {
            snapshot: job.snapshot,
            blobs: present,
        })
    }

    async fn download_blobs(&self, job: &BlobTransferJob) -> Result<BlobTransferResult> {
        Ok(BlobTransferResult {
            snapshot: job.snapshot,
            blobs: job.blobs.clone(),
        })
    }
}
