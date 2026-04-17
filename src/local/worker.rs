use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;

use crate::engine::{ApplyJob, BlobTransferJob, BlobTransferResult};
use crate::services::{
    Applier, ApplyOp, ApplyPlan, ApplyWorker, BlobStore, BlobTransferWorker, Result,
};

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

pub struct LocalApplier {
    sync_root: PathBuf,
    blob_store: Arc<dyn BlobStore>,
}

impl LocalApplier {
    pub fn new(sync_root: PathBuf, blob_store: Arc<dyn BlobStore>) -> Self {
        Self {
            sync_root,
            blob_store,
        }
    }

    fn full_path(&self, path: &std::path::Path) -> PathBuf {
        self.sync_root.join(path)
    }

    async fn write_file(&self, path: &std::path::Path, file: &crate::core::File) -> Result<()> {
        let full_path = self.full_path(path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut data = Vec::new();
        for blob in &file.blobs {
            data.extend_from_slice(&self.blob_store.get_blob(blob).await?);
        }
        std::fs::write(full_path, data)?;
        Ok(())
    }

    fn remove_path(&self, path: &std::path::Path) -> Result<()> {
        let full_path = self.full_path(path);
        if !full_path.exists() {
            return Ok(());
        }
        let metadata = std::fs::symlink_metadata(&full_path)?;
        if metadata.is_dir() {
            std::fs::remove_dir_all(full_path)?;
        } else {
            std::fs::remove_file(full_path)?;
        }
        Ok(())
    }
}

#[async_trait]
impl Applier for LocalApplier {
    async fn apply(&self, plan: &ApplyPlan) -> Result<()> {
        for op in &plan.ops {
            match op {
                ApplyOp::RemovePath { path } => self.remove_path(path)?,
                ApplyOp::CreateDir { path } => {
                    std::fs::create_dir_all(self.full_path(path))?;
                }
                ApplyOp::WriteFile { path, file } => self.write_file(path, file).await?,
                ApplyOp::MaterializeConflict { .. } => {}
            }
        }
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use async_trait::async_trait;
    use tempfile::tempdir;

    use super::LocalApplier;
    use crate::core::FullBlob;
    use crate::services::{Applier, ApplyOp, ApplyPlan, BlobStore};

    struct MemoryBlobStore {
        blobs: std::sync::Mutex<std::collections::BTreeMap<[u8; 32], Vec<u8>>>,
    }

    #[async_trait]
    impl BlobStore for MemoryBlobStore {
        async fn put_blob(&self, blob: &FullBlob) -> crate::services::Result<()> {
            self.blobs
                .lock()
                .unwrap()
                .insert(blob.hash, blob.data.clone());
            Ok(())
        }

        async fn get_blob(&self, hash: &[u8; 32]) -> crate::services::Result<Vec<u8>> {
            self.blobs
                .lock()
                .unwrap()
                .get(hash)
                .cloned()
                .ok_or(crate::services::SyncError::NotFound)
        }

        async fn has_blob(&self, hash: &[u8; 32]) -> crate::services::Result<bool> {
            Ok(self.blobs.lock().unwrap().contains_key(hash))
        }
    }

    #[tokio::test]
    async fn local_applier_creates_writes_and_removes_paths() {
        let dir = tempdir().unwrap();
        let blob_store = Arc::new(MemoryBlobStore {
            blobs: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        });
        let blob_hash = [7; 32];
        blob_store
            .put_blob(&FullBlob {
                hash: blob_hash,
                size: 5,
                data: b"hello".to_vec(),
            })
            .await
            .unwrap();
        let applier = LocalApplier::new(dir.path().to_path_buf(), blob_store);

        applier
            .apply(&ApplyPlan {
                target_snapshot: [1; 32],
                ops: vec![
                    ApplyOp::CreateDir {
                        path: PathBuf::from("docs"),
                    },
                    ApplyOp::WriteFile {
                        path: PathBuf::from("docs/hello.txt"),
                        file: crate::core::File {
                            path: "docs/hello.txt".into(),
                            hash: [2; 32],
                            blobs: vec![blob_hash],
                        },
                    },
                ],
            })
            .await
            .unwrap();

        assert_eq!(
            std::fs::read_to_string(dir.path().join("docs/hello.txt")).unwrap(),
            "hello"
        );

        applier
            .apply(&ApplyPlan {
                target_snapshot: [2; 32],
                ops: vec![ApplyOp::RemovePath {
                    path: PathBuf::from("docs"),
                }],
            })
            .await
            .unwrap();

        assert!(!dir.path().join("docs").exists());
    }
}
