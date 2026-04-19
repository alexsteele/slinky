use std::sync::Arc;

use crate::core::Config;
use crate::engine::SyncEngine;
use crate::local::{
    FsWatcher, LocalApplier, LocalBlobStore, LocalBlobTransferWorker, LocalChunker, LocalMetaStore,
    LocalObjStore, LocalTreeBuilder, NoopRelay, load_config,
};
use crate::relay::Relay;
use crate::runtime::SyncService;
use crate::services::{
    Applier, BlobStore, BlobTransferWorker, Chunker, MetaStore, ObjStore, Result, TreeBuilder,
    Watcher,
};

/// Device is the top-level assembled local node.
///
/// Responsibilities:
/// - own the local device config
/// - connect to the relay using that config
/// - construct the serialized sync engine
/// - expose the sync service lifecycle
pub struct Device {
    pub config: Config,
    pub service: SyncService,
}

/// Optional dependency overrides for device assembly.
pub struct DeviceOptions {
    pub config: Config,
    pub relay: Option<Arc<dyn Relay>>,
    pub watcher: Option<Arc<dyn Watcher>>,
}

impl DeviceOptions {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            relay: None,
            watcher: None,
        }
    }
}

impl Device {
    pub async fn open(options: DeviceOptions) -> Result<Self> {
        let DeviceOptions {
            config,
            relay,
            watcher,
        } = options;
        let meta_store = Self::open_meta_store(&config).await?;
        let blob_store = Self::open_blob_store(&config).await?;
        let blob_worker = Self::open_blob_worker(blob_store.clone()).await?;
        let applier = Self::open_applier(&config, blob_store.clone()).await?;
        let obj_store = Self::open_obj_store(&config).await?;
        let chunker = Self::open_chunker().await?;
        let tree_builder = Self::open_tree_builder(chunker.clone()).await?;
        let relay = match relay {
            Some(relay) => relay,
            None => Arc::new(NoopRelay),
        };
        let watcher = match watcher {
            Some(watcher) => watcher,
            None => Self::open_watcher().await?,
        };

        let engine = SyncEngine::open(
            config.clone(),
            meta_store,
            obj_store,
            blob_store,
            blob_worker,
            applier,
            relay,
            tree_builder,
            chunker,
        )
        .await?;

        let service = SyncService::new(engine, watcher, config.sync_root.clone());

        Ok(Self { config, service })
    }

    pub async fn open_path(config_path: &std::path::Path) -> Result<Self> {
        let config = load_config(config_path)?;
        Self::open(DeviceOptions::new(config)).await
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

    async fn open_blob_worker(
        blob_store: Arc<dyn BlobStore>,
    ) -> Result<Arc<dyn BlobTransferWorker>> {
        Ok(Arc::new(LocalBlobTransferWorker::new(blob_store)))
    }

    async fn open_applier(
        config: &Config,
        blob_store: Arc<dyn BlobStore>,
    ) -> Result<Arc<dyn Applier>> {
        Ok(Arc::new(LocalApplier::new(
            config.sync_root.clone(),
            blob_store,
        )))
    }

    async fn open_chunker() -> Result<Arc<dyn Chunker>> {
        Ok(LocalChunker::open())
    }

    async fn open_tree_builder(chunker: Arc<dyn Chunker>) -> Result<Arc<dyn TreeBuilder>> {
        Ok(Arc::new(LocalTreeBuilder::new(chunker)))
    }

    async fn open_watcher() -> Result<Arc<dyn Watcher>> {
        Ok(Arc::new(FsWatcher))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    use tempfile::tempdir;
    use tokio::time::{Duration, sleep};

    use super::{Device, DeviceOptions};
    use crate::core::{Config, DeviceCredentials, FileKind, Object, ObjectId};
    use crate::local::{LocalMetaStore, MemoryRelay, NoopWatcher};
    use crate::local::util::{device_root, encode_hash, hash_bytes};
    use crate::services::WatcherEvent;

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
        device.stop().await.unwrap();
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

    #[tokio::test]
    async fn device_watcher_updates_engine_tree_after_local_change() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();
        std::fs::write(sync_root.join("hello.txt"), b"hello").unwrap();

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

        let mut device = Device::open(DeviceOptions::new(config)).await.unwrap();
        device.start().await.unwrap();

        std::fs::write(sync_root.join("hello.txt"), b"hello updated").unwrap();
        sleep(Duration::from_millis(500)).await;

        device.stop().await.unwrap();
        device.join().await.unwrap();

        let engine = device.service.engine.as_ref().unwrap();
        assert_ne!(engine.state.next_revision, 1);
        assert!(engine.pending_deltas.is_empty());
        assert!(
            engine
                .tree
                .entries
                .iter()
                .any(|entry| entry.name == "hello.txt" && entry.kind == FileKind::File)
        );
    }

    #[tokio::test]
    async fn device_watcher_updates_engine_tree_after_local_delete() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(sync_root.join("docs")).unwrap();
        std::fs::write(sync_root.join("docs/guide.md"), b"guide").unwrap();

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

        let mut device = Device::open(DeviceOptions::new(config)).await.unwrap();
        device.start().await.unwrap();

        std::fs::remove_dir_all(sync_root.join("docs")).unwrap();
        sleep(Duration::from_millis(500)).await;

        device.stop().await.unwrap();
        device.join().await.unwrap();

        let engine = device.service.engine.as_ref().unwrap();
        assert!(engine.tree.entries.is_empty());
    }

    #[tokio::test]
    async fn device_watcher_updates_engine_tree_after_local_file_delete() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(sync_root.join("docs")).unwrap();
        std::fs::write(sync_root.join("docs/guide.md"), b"guide").unwrap();

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

        let mut device = Device::open(DeviceOptions::new(config)).await.unwrap();
        device.start().await.unwrap();

        std::fs::remove_file(sync_root.join("docs/guide.md")).unwrap();
        sleep(Duration::from_millis(500)).await;

        device.stop().await.unwrap();
        device.join().await.unwrap();

        let engine = device.service.engine.as_ref().unwrap();
        assert!(
            engine
                .index
                .resolve_path(std::path::Path::new("docs/guide.md"))
                .unwrap()
                .is_none()
        );
        assert!(
            engine
                .index
                .resolve_path(std::path::Path::new("docs"))
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn device_watcher_updates_engine_tree_after_empty_directory_delete() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(sync_root.join("docs")).unwrap();
        std::fs::write(sync_root.join("hello.txt"), b"hello").unwrap();

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

        let mut device = Device::open(DeviceOptions::new(config)).await.unwrap();
        device.start().await.unwrap();

        let meta_store = LocalMetaStore::open(device.config.clone()).unwrap();
        let initial_state = meta_store
            .load_state(&device.config.repo_id, &device.config.device_id)
            .await
            .unwrap();

        std::fs::remove_dir(sync_root.join("docs")).unwrap();
        sleep(Duration::from_millis(500)).await;

        device.stop().await.unwrap();
        device.join().await.unwrap();

        let engine = device.service.engine.as_ref().unwrap();
        assert_eq!(engine.state.snapshot, initial_state.snapshot);
        assert!(
            engine
                .index
                .resolve_path(std::path::Path::new("docs"))
                .unwrap()
                .is_none()
        );
        assert!(
            engine
                .tree
                .entries
                .iter()
                .any(|entry| entry.name == "hello.txt" && entry.kind == FileKind::File)
        );
    }

    #[tokio::test]
    async fn device_watcher_updates_engine_tree_after_local_move() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(sync_root.join("docs")).unwrap();
        let from_path = sync_root.join("docs/guide.md");
        std::fs::write(&from_path, b"guide").unwrap();

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

        let mut device = Device::open(DeviceOptions::new(config)).await.unwrap();
        device.start().await.unwrap();

        std::fs::create_dir_all(sync_root.join("guides")).unwrap();
        let to_path = sync_root.join("guides/guide.md");
        std::fs::rename(&from_path, &to_path).unwrap();
        sleep(Duration::from_millis(500)).await;

        device.stop().await.unwrap();
        device.join().await.unwrap();

        let engine = device.service.engine.as_ref().unwrap();
        assert!(
            engine
                .tree
                .entries
                .iter()
                .any(|entry| entry.name == "guides" && entry.kind == FileKind::Folder)
        );
        assert!(!engine.tree.entries.iter().any(|entry| entry.name == "docs"));
    }

    #[tokio::test]
    async fn device_watcher_updates_engine_tree_after_local_directory_create_and_file_add() {
        let dir = tempdir().unwrap();
        let sync_root = dir.path().join("sync");
        std::fs::create_dir_all(&sync_root).unwrap();
        std::fs::write(sync_root.join("hello.txt"), b"hello").unwrap();

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

        let mut device = Device::open(DeviceOptions::new(config)).await.unwrap();
        device.start().await.unwrap();

        std::fs::create_dir_all(sync_root.join("docs")).unwrap();
        std::fs::write(sync_root.join("docs/guide.md"), b"guide").unwrap();
        sleep(Duration::from_millis(500)).await;

        device.stop().await.unwrap();
        device.join().await.unwrap();

        let engine = device.service.engine.as_ref().unwrap();
        assert_ne!(engine.state.next_revision, 1);
        assert!(engine.pending_deltas.is_empty());

        let docs_id = engine
            .index
            .resolve_path(std::path::Path::new("docs"))
            .unwrap();
        assert!(docs_id.is_some());

        let guide_id = engine
            .index
            .resolve_path(std::path::Path::new("docs/guide.md"))
            .unwrap();
        assert!(guide_id.is_some());
    }

    #[tokio::test]
    async fn two_devices_sync_file_change_through_memory_relay() {
        let dir = tempdir().unwrap();
        let relay = MemoryRelay::new();
        let repo_id = format!(
            "repo-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let sync_root_a = dir.path().join("sync-a");
        let sync_root_b = dir.path().join("sync-b");
        std::fs::create_dir_all(&sync_root_a).unwrap();
        std::fs::create_dir_all(&sync_root_b).unwrap();

        let config_a = Config {
            sync_root: sync_root_a.clone(),
            repo_id: repo_id.clone(),
            device_id: "device-a".into(),
            credentials: DeviceCredentials {
                public_key: "pub-a".into(),
                private_key_path: dir.path().join("device-a.key"),
            },
        };
        let config_b = Config {
            sync_root: sync_root_b.clone(),
            repo_id,
            device_id: "device-b".into(),
            credentials: DeviceCredentials {
                public_key: "pub-b".into(),
                private_key_path: dir.path().join("device-b.key"),
            },
        };

        let mut device_a = Device::open_with_options(
            config_a,
            DeviceOptions {
                relay: Some(relay.clone()),
                watcher: Some(std::sync::Arc::new(NoopWatcher)),
            },
        )
        .await
        .unwrap();
        let mut device_b = Device::open_with_options(
            config_b,
            DeviceOptions {
                relay: Some(relay),
                watcher: Some(std::sync::Arc::new(NoopWatcher)),
            },
        )
        .await
        .unwrap();

        device_a.start().await.unwrap();
        device_b.start().await.unwrap();

        let source_path = sync_root_a.join("hello.txt");
        std::fs::write(&source_path, b"hello relay").unwrap();
        device_a
            .service
            .send_local_event(WatcherEvent::FileChanged(source_path.clone()))
            .await
            .unwrap();

        let target_path = sync_root_b.join("hello.txt");
        let mut synced = false;
        for _ in 0..20 {
            sleep(Duration::from_millis(100)).await;
            if target_path.exists()
                && std::fs::read_to_string(&target_path).unwrap_or_default() == "hello relay"
            {
                synced = true;
                break;
            }
        }

        device_a.stop().await.unwrap();
        device_b.stop().await.unwrap();
        device_a.join().await.unwrap();
        device_b.join().await.unwrap();

        assert!(synced, "device B did not receive the file update from device A");
        assert_eq!(std::fs::read_to_string(&target_path).unwrap(), "hello relay");
        let engine_b = device_b.service.engine.as_ref().unwrap();
        assert!(
            engine_b
                .index
                .resolve_path(Path::new("hello.txt"))
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn two_devices_sync_file_delete_through_memory_relay() {
        let dir = tempdir().unwrap();
        let relay = MemoryRelay::new();
        let repo_id = format!(
            "repo-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let sync_root_a = dir.path().join("sync-a");
        let sync_root_b = dir.path().join("sync-b");
        std::fs::create_dir_all(&sync_root_a).unwrap();
        std::fs::create_dir_all(&sync_root_b).unwrap();
        std::fs::write(sync_root_a.join("delete-me.txt"), b"bye").unwrap();
        std::fs::write(sync_root_b.join("delete-me.txt"), b"bye").unwrap();

        let config_a = Config {
            sync_root: sync_root_a.clone(),
            repo_id: repo_id.clone(),
            device_id: "device-a".into(),
            credentials: DeviceCredentials {
                public_key: "pub-a".into(),
                private_key_path: dir.path().join("device-a.key"),
            },
        };
        let config_b = Config {
            sync_root: sync_root_b.clone(),
            repo_id,
            device_id: "device-b".into(),
            credentials: DeviceCredentials {
                public_key: "pub-b".into(),
                private_key_path: dir.path().join("device-b.key"),
            },
        };

        let mut device_a = Device::open_with_options(
            config_a,
            DeviceOptions {
                relay: Some(relay.clone()),
                watcher: None,
            },
        )
        .await
        .unwrap();
        let mut device_b = Device::open_with_options(
            config_b,
            DeviceOptions {
                relay: Some(relay),
                watcher: None,
            },
        )
        .await
        .unwrap();

        device_a.start().await.unwrap();
        device_b.start().await.unwrap();

        let target_path_b = sync_root_b.join("delete-me.txt");
        let source_path_a = sync_root_a.join("delete-me.txt");

        std::fs::remove_file(&source_path_a).unwrap();
        device_a
            .service
            .send_local_event(WatcherEvent::FileDeleted(source_path_a.clone()))
            .await
            .unwrap();

        let mut deleted = false;
        for _ in 0..20 {
            sleep(Duration::from_millis(100)).await;
            if !target_path_b.exists() {
                deleted = true;
                break;
            }
        }

        device_a.stop().await.unwrap();
        device_b.stop().await.unwrap();
        device_a.join().await.unwrap();
        device_b.join().await.unwrap();

        assert!(deleted, "device B did not remove the file after device A deleted it");
        let engine_b = device_b.service.engine.as_ref().unwrap();
        assert!(
            engine_b
                .index
                .resolve_path(Path::new("delete-me.txt"))
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn two_devices_sync_file_move_through_memory_relay() {
        let dir = tempdir().unwrap();
        let relay = MemoryRelay::new();
        let repo_id = format!(
            "repo-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let sync_root_a = dir.path().join("sync-a");
        let sync_root_b = dir.path().join("sync-b");
        std::fs::create_dir_all(sync_root_a.join("docs")).unwrap();
        std::fs::create_dir_all(sync_root_b.join("docs")).unwrap();
        std::fs::write(sync_root_a.join("docs/guide.md"), b"move me").unwrap();
        std::fs::write(sync_root_b.join("docs/guide.md"), b"move me").unwrap();

        let config_a = Config {
            sync_root: sync_root_a.clone(),
            repo_id: repo_id.clone(),
            device_id: "device-a".into(),
            credentials: DeviceCredentials {
                public_key: "pub-a".into(),
                private_key_path: dir.path().join("device-a.key"),
            },
        };
        let config_b = Config {
            sync_root: sync_root_b.clone(),
            repo_id,
            device_id: "device-b".into(),
            credentials: DeviceCredentials {
                public_key: "pub-b".into(),
                private_key_path: dir.path().join("device-b.key"),
            },
        };

        let mut device_a = Device::open_with_options(
            config_a,
            DeviceOptions {
                relay: Some(relay.clone()),
                watcher: Some(std::sync::Arc::new(NoopWatcher)),
            },
        )
        .await
        .unwrap();
        let mut device_b = Device::open_with_options(
            config_b,
            DeviceOptions {
                relay: Some(relay),
                watcher: Some(std::sync::Arc::new(NoopWatcher)),
            },
        )
        .await
        .unwrap();

        device_a.start().await.unwrap();
        device_b.start().await.unwrap();

        let from_a = sync_root_a.join("docs/guide.md");
        let from_b = sync_root_b.join("docs/guide.md");
        let to_a = sync_root_a.join("guides/guide.md");
        let to_b = sync_root_b.join("guides/guide.md");

        std::fs::create_dir_all(to_a.parent().unwrap()).unwrap();
        std::fs::rename(&from_a, &to_a).unwrap();
        device_a
            .service
            .send_local_event(WatcherEvent::PathMoved {
                from: from_a.clone(),
                to: to_a.clone(),
            })
            .await
            .unwrap();

        let mut moved = false;
        for _ in 0..20 {
            sleep(Duration::from_millis(100)).await;
            if !from_b.exists()
                && to_b.exists()
                && std::fs::read_to_string(&to_b).unwrap_or_default() == "move me"
            {
                moved = true;
                break;
            }
        }

        device_a.stop().await.unwrap();
        device_b.stop().await.unwrap();
        device_a.join().await.unwrap();
        device_b.join().await.unwrap();

        assert!(moved, "device B did not move the file after device A renamed it");
        let engine_b = device_b.service.engine.as_ref().unwrap();
        assert!(
            engine_b
                .index
                .resolve_path(Path::new("docs/guide.md"))
                .unwrap()
                .is_none()
        );
        assert!(
            engine_b
                .index
                .resolve_path(Path::new("guides/guide.md"))
                .unwrap()
                .is_some()
        );
    }
}
