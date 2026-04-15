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
    engine_task: Option<JoinHandle<Result<SyncEngine>>>,
    watcher_task: Option<JoinHandle<Result<()>>>,
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
            engine_task: None,
            watcher_task: None,
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
        engine.start().await?;

        let (event_tx, mut event_rx) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY);
        let watcher = self.watcher.clone();
        let sync_root = self.sync_root.clone();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let (watcher_shutdown_tx, watcher_shutdown_rx) = oneshot::channel();
        let (watcher_ready_tx, watcher_ready_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);
        self.watcher_shutdown_tx = Some(watcher_shutdown_tx);

        self.watcher_task = Some(tokio::spawn(async move {
            watcher
                .start(&sync_root, event_tx, watcher_ready_tx, watcher_shutdown_rx)
                .await
        }));

        self.engine_task = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    event = event_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };
                        engine.handle_event(SyncEvent::Local(event)).await?;
                    }
                }
            }
            Ok(engine)
        }));

        watcher_ready_rx
            .await
            .map_err(|_| SyncError::InvalidState("watcher failed to report readiness".into()))?;

        self.started = true;
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.started = false;
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(watcher_shutdown_tx) = self.watcher_shutdown_tx.take() {
            let _ = watcher_shutdown_tx.send(());
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
