use std::sync::Arc;

use tokio::sync::mpsc;

use crate::core::Config;
use crate::engine::{SyncEngine, SyncPhase};
use crate::local::{
    FsScanner, LocalBlobStore, LocalMetaStore, LocalObjStore, LocalTreeBuilder, NoopApplier,
    NoopCoordinator, NoopReconciler, NoopWatcher, load_config,
};
use crate::runtime::{RuntimeChannels, SyncService};
use crate::services::{
    Applier, BlobStore, Coordinator, MetaStore, ObjStore, Reconciler, Result, Scanner, StageWorker,
    TreeBuilder, Watcher,
};

/// Device is the top-level assembled local node.
///
/// Responsibilities:
/// - own the local device config
/// - connect to the coordinator using that config
/// - assemble runtime channels
/// - construct the serialized sync engine
/// - expose the sync service lifecycle
pub struct Device {
    pub config: Config,
    pub service: SyncService,
}

impl Device {
    pub async fn open(config: Config) -> Result<Self> {
        let meta_store = Self::open_meta_store(&config).await?;
        let state = meta_store
            .load_state(&config.repo_id, &config.device_id)
            .await?;
        let obj_store = Self::open_obj_store(&config).await?;
        let blob_store = Self::open_blob_store(&config).await?;
        let coordinator = Self::connect_coordinator(&config).await?;
        let scanner = Self::open_scanner(&config).await?;
        let watcher = Self::open_watcher(&config).await?;
        let (tree_builder, stage_worker) = Self::open_tree_builder(blob_store.clone()).await?;
        let reconciler = Self::open_reconciler(&config).await?;
        let applier = Self::open_applier(&config).await?;

        let coordinator_notifications = coordinator
            .subscribe(&config.repo_id, &config.device_id)
            .await?;
        let (local_change_tx, local_change_rx) =
            mpsc::channel(crate::runtime::DEFAULT_CHANNEL_CAPACITY);
        let (engine_event_tx, engine_event_rx) =
            mpsc::channel(crate::runtime::DEFAULT_CHANNEL_CAPACITY);
        let (job_tx, job_rx) = mpsc::channel(crate::runtime::DEFAULT_CHANNEL_CAPACITY);

        let engine = SyncEngine::new(
            config.clone(),
            state,
            meta_store,
            obj_store,
            blob_store,
            coordinator,
            scanner,
            watcher,
            tree_builder,
            reconciler,
            applier,
            SyncPhase::Starting,
            None,
            coordinator_notifications,
            local_change_rx,
            engine_event_rx,
            job_tx,
        );

        let channels = RuntimeChannels::new(local_change_tx, engine_event_tx, job_rx);

        let service = SyncService::new(engine, stage_worker, channels);

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

    async fn open_scanner(_config: &Config) -> Result<Arc<dyn Scanner>> {
        Ok(Arc::new(FsScanner))
    }

    async fn open_watcher(_config: &Config) -> Result<Arc<dyn Watcher>> {
        Ok(Arc::new(NoopWatcher))
    }

    async fn open_tree_builder(
        blob_store: Arc<dyn BlobStore>,
    ) -> Result<(Arc<dyn TreeBuilder>, Arc<dyn StageWorker>)> {
        let worker = Arc::new(LocalTreeBuilder::new(blob_store));
        let tree_builder: Arc<dyn TreeBuilder> = worker.clone();
        let stage_worker: Arc<dyn StageWorker> = worker;
        Ok((tree_builder, stage_worker))
    }

    async fn open_reconciler(_config: &Config) -> Result<Arc<dyn Reconciler>> {
        Ok(Arc::new(NoopReconciler))
    }

    async fn open_applier(_config: &Config) -> Result<Arc<dyn Applier>> {
        Ok(Arc::new(NoopApplier))
    }
}
