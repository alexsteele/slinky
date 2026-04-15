use std::sync::Arc;

use crate::core::Config;
use crate::engine::SyncEngine;
use crate::local::{
    LocalBlobStore, LocalMetaStore, LocalObjStore, LocalTreeBuilder, NoopCoordinator, NoopWatcher,
    load_config,
};
use crate::runtime::SyncService;
use crate::services::{BlobStore, Coordinator, MetaStore, ObjStore, Result, TreeBuilder, Watcher};

/// Device is the top-level assembled local node.
///
/// Responsibilities:
/// - own the local device config
/// - connect to the coordinator using that config
/// - construct the serialized sync engine
/// - expose the sync service lifecycle
pub struct Device {
    pub config: Config,
    pub service: SyncService,
}

impl Device {
    pub async fn open(config: Config) -> Result<Self> {
        let meta_store = Self::open_meta_store(&config).await?;
        let blob_store = Self::open_blob_store(&config).await?;
        let obj_store = Self::open_obj_store(&config).await?;
        let coordinator = Self::connect_coordinator(&config).await?;
        let tree_builder = Self::open_tree_builder().await?;
        let watcher = Self::open_watcher().await?;

        let engine = SyncEngine::open(
            config.clone(),
            meta_store,
            obj_store,
            blob_store,
            coordinator,
            tree_builder,
        )
        .await?;

        let service = SyncService::new(engine, watcher, config.sync_root.clone());

        Ok(Self { config, service })
    }

    pub async fn open_path(config_path: &std::path::Path) -> Result<Self> {
        let config = load_config(config_path)?;
        Self::open(config).await
    }

    pub async fn start(&mut self) -> Result<()> {
        self.service.start().await
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.service.stop().await
    }

    pub async fn join(&mut self) -> Result<()> {
        self.service.join().await
    }

    pub async fn run(&mut self) -> Result<()> {
        self.service.run().await
    }

    async fn open_meta_store(_config: &Config) -> Result<Arc<dyn MetaStore>> {
        LocalMetaStore::open(_config.clone())
    }

    async fn open_obj_store(_config: &Config) -> Result<Arc<dyn ObjStore>> {
        LocalObjStore::open(_config)
    }

    async fn open_blob_store(_config: &Config) -> Result<Arc<dyn BlobStore>> {
        LocalBlobStore::open(_config)
    }

    async fn connect_coordinator(_config: &Config) -> Result<Arc<dyn Coordinator>> {
        Ok(Arc::new(NoopCoordinator))
    }

    async fn open_tree_builder() -> Result<Arc<dyn TreeBuilder>> {
        Ok(Arc::new(LocalTreeBuilder::new()))
    }

    async fn open_watcher() -> Result<Arc<dyn Watcher>> {
        Ok(Arc::new(NoopWatcher))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use tempfile::tempdir;

    use super::Device;
    use crate::core::{Config, DeviceCredentials, Object, ObjectId};
    use crate::local::util::{device_root, encode_hash, hash_bytes};

    #[tokio::test]
    async fn device_start_persists_snapshot_tree_and_blob() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();
        let file_path = sync_root.join("hello.txt");
        std::fs::write(&file_path, b"hello").unwrap();

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config = Config {
            sync_root: sync_root.clone(),
            repo_id: format!("repo-{unique}"),
            device_id: format!("device-{unique}"),
            credentials: DeviceCredentials {
                public_key: "pub".into(),
                private_key_path: dir.path().join("device.key"),
            },
        };

        let mut device = Device::open(config.clone()).await.unwrap();
        device.start().await.unwrap();
        device.join().await.unwrap();
        let engine = device.service.engine.as_ref().unwrap();
        let snapshot_hash = engine.state.snapshot;

        assert_ne!(snapshot_hash, [0; 32]);
        assert_eq!(engine.state.snapshot, snapshot_hash);
        assert_eq!(engine.tree.hash, engine.tree.compute_hash());

        let snapshot = match engine
            .obj_store
            .load_object(&ObjectId::Snapshot(snapshot_hash))
            .await
            .unwrap()
        {
            Object::Snapshot(snapshot) => snapshot,
            _ => panic!("expected snapshot object"),
        };
        assert_eq!(snapshot.tree_hash, engine.tree.hash);

        let blob_hash = hash_bytes(b"hello");
        let blob_path = device_root(&config)
            .unwrap()
            .join("blobs")
            .join(encode_hash(&blob_hash));
        assert!(blob_path.exists());
        assert_eq!(std::fs::read(blob_path).unwrap(), b"hello");
    }
}
