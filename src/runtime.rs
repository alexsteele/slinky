use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::engine::{SyncEngine, SyncEvent};
use crate::services::{Result, SyncError, Watcher};

pub const DEFAULT_CHANNEL_CAPACITY: usize = 128;

/// SyncService is the top-level lifecycle object for the local sync process.
///
/// The runtime stays small: it runs startup once, then forwards local watcher events into the
/// serialized engine loop.
pub struct SyncService {
    pub engine: Option<SyncEngine>,
    pub watcher: Arc<dyn Watcher>,
    pub sync_root: PathBuf,
    started: bool,
    shutdown_tx: Option<oneshot::Sender<()>>,
    watcher_shutdown_tx: Option<oneshot::Sender<()>>,
    coordinator_shutdown_tx: Option<oneshot::Sender<()>>,
    engine_task: Option<JoinHandle<Result<SyncEngine>>>,
    watcher_task: Option<JoinHandle<Result<()>>>,
    coordinator_task: Option<JoinHandle<Result<()>>>,
}

impl SyncService {
    pub fn new(engine: SyncEngine, watcher: Arc<dyn Watcher>, sync_root: PathBuf) -> Self {
        Self {
            engine: Some(engine),
            watcher,
            sync_root,
            started: false,
            shutdown_tx: None,
            watcher_shutdown_tx: None,
            coordinator_shutdown_tx: None,
            engine_task: None,
            watcher_task: None,
            coordinator_task: None,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        if self.started {
            return Err(SyncError::InvalidState(
                "sync service already started".into(),
            ));
        }

        let mut engine = self
            .engine
            .take()
            .ok_or_else(|| SyncError::InvalidState("sync engine is not available".into()))?;
        eprintln!("[service] starting engine");
        engine.start().await?;

        let (event_tx, mut event_rx) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY);
        let watcher = self.watcher.clone();
        let sync_root = self.sync_root.clone();
        let coordinator = engine.coordinator.clone();
        let repo_id = engine.config.repo_id.clone();
        let device_id = engine.config.device_id.clone();
        let watcher_event_tx = event_tx.clone();
        let coordinator_event_tx = event_tx.clone();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let (watcher_shutdown_tx, watcher_shutdown_rx) = oneshot::channel();
        let (coordinator_shutdown_tx, mut coordinator_shutdown_rx) = oneshot::channel();
        let (watcher_ready_tx, watcher_ready_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);
        self.watcher_shutdown_tx = Some(watcher_shutdown_tx);
        self.coordinator_shutdown_tx = Some(coordinator_shutdown_tx);

        self.watcher_task = Some(tokio::spawn(async move {
            eprintln!("[service] starting watcher task");
            let (local_tx, mut local_rx) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY);
            let forwarder = tokio::spawn(async move {
                while let Some(event) = local_rx.recv().await {
                    if watcher_event_tx
                        .send(SyncEvent::Local(event))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
            watcher
                .start(&sync_root, local_tx, watcher_ready_tx, watcher_shutdown_rx)
                .await?;
            forwarder.abort();
            Ok(())
        }));

        self.coordinator_task = Some(tokio::spawn(async move {
            eprintln!("[service] subscribing to coordinator notifications");
            let mut stream = coordinator.subscribe(&repo_id, &device_id).await?;
            loop {
                tokio::select! {
                    _ = &mut coordinator_shutdown_rx => {
                        eprintln!("[service] coordinator task shutting down");
                        break;
                    }
                    notification = stream.recv() => {
                        let Some(notification) = notification else {
                            break;
                        };
                        eprintln!(
                            "[service] received coordinator notification: {:?}",
                            notification
                        );
                        if coordinator_event_tx
                            .send(SyncEvent::Remote(notification))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            }
            Ok(())
        }));

        self.engine_task = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        eprintln!("[service] engine task shutting down");
                        break
                    },
                    event = event_rx.recv() => {
                        let Some(event) = event else {
                            eprintln!("[service] event channel closed");
                            break;
                        };
                        eprintln!("[service] forwarding engine event: {:?}", event);
                        engine.handle_event(event).await?;
                    }
                }
            }
            Ok(engine)
        }));

        watcher_ready_rx
            .await
            .map_err(|_| SyncError::InvalidState("watcher failed to report readiness".into()))?;

        eprintln!("[service] watcher ready");
        self.started = true;
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        eprintln!("[service] stopping");
        self.started = false;
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(watcher_shutdown_tx) = self.watcher_shutdown_tx.take() {
            let _ = watcher_shutdown_tx.send(());
        }
        if let Some(coordinator_shutdown_tx) = self.coordinator_shutdown_tx.take() {
            let _ = coordinator_shutdown_tx.send(());
        }
        Ok(())
    }

    pub async fn join(&mut self) -> Result<()> {
        if let Some(watcher_task) = self.watcher_task.take() {
            match watcher_task.await {
                Ok(result) => result?,
                Err(join_error) => {
                    return Err(SyncError::InvalidState(format!(
                        "watcher task failed to join: {join_error}"
                    )));
                }
            }
        }

        if let Some(coordinator_task) = self.coordinator_task.take() {
            match coordinator_task.await {
                Ok(result) => result?,
                Err(join_error) => {
                    return Err(SyncError::InvalidState(format!(
                        "coordinator task failed to join: {join_error}"
                    )));
                }
            }
        }

        if let Some(engine_task) = self.engine_task.take() {
            match engine_task.await {
                Ok(result) => {
                    self.engine = Some(result?);
                }
                Err(join_error) => {
                    return Err(SyncError::InvalidState(format!(
                        "engine task failed to join: {join_error}"
                    )));
                }
            }
        }

        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        self.start().await?;
        self.join().await
    }

    #[cfg(test)]
    pub async fn send_local_event(&mut self, event: crate::services::WatcherEvent) -> Result<()> {
        let engine = self
            .engine
            .as_mut()
            .ok_or_else(|| SyncError::InvalidState("sync engine is not available".into()))?;
        engine.handle_event(SyncEvent::Local(event)).await?;
        Ok(())
    }
}
