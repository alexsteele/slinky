use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;

use crate::core::{BlobHash, Config, FullBlob};
use crate::local::util::{device_root, encode_hash};
use crate::services::{BlobStore, Result};

pub struct LocalBlobStore {
    root: PathBuf,
}

impl LocalBlobStore {
    pub fn open(config: &Config) -> Result<Arc<dyn BlobStore>> {
        let root = device_root(config)?.join("blobs");
        std::fs::create_dir_all(&root)?;
        Ok(Arc::new(Self { root }))
    }

    fn blob_path(&self, hash: &BlobHash) -> PathBuf {
        self.root.join(encode_hash(hash))
    }
}

#[async_trait]
impl BlobStore for LocalBlobStore {
    async fn put_blob(&self, blob: &FullBlob) -> Result<()> {
        let path = self.blob_path(&blob.hash);
        if !path.exists() {
            std::fs::write(path, &blob.data)?;
        }
        Ok(())
    }

    async fn get_blob(&self, hash: &BlobHash) -> Result<Vec<u8>> {
        std::fs::read(self.blob_path(hash)).map_err(Into::into)
    }

    async fn has_blob(&self, hash: &BlobHash) -> Result<bool> {
        Ok(self.blob_path(hash).exists())
    }
}
