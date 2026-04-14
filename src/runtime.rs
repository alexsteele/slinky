use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::engine::{EngineEvent, EngineJob, SyncEngine};
use crate::services::{Result, StageWorker, SyncError, WatcherEvent};

pub const DEFAULT_CHANNEL_CAPACITY: usize = 128;

/// RuntimeChannels define the process-level wiring outside the serialized engine.
///
/// Event flow into the engine:
/// - watcher task -> `local_change_tx`
/// - worker tasks -> `engine_event_tx`
///
/// Job flow out of the engine:
/// - stage/upload/download/apply workers <- `job_rx`
pub struct RuntimeChannels {
    pub local_change_tx: mpsc::Sender<WatcherEvent>,
    pub engine_event_tx: mpsc::Sender<EngineEvent>,
    pub job_rx: Option<mpsc::Receiver<EngineJob>>,
}

impl RuntimeChannels {
    pub fn new(
        local_change_tx: mpsc::Sender<WatcherEvent>,
        engine_event_tx: mpsc::Sender<EngineEvent>,
        job_rx: mpsc::Receiver<EngineJob>,
    ) -> Self {
        Self {
            local_change_tx,
            engine_event_tx,
            job_rx: Some(job_rx),
        }
    }
}

/// SyncService is the top-level lifecycle object for the local sync process.
///
/// Responsibilities:
/// - own runtime senders and worker receivers
/// - start watcher/coordinator/worker tasks
/// - drive the engine event loop by feeding `EngineEvent`s into `SyncEngine::handle_event`
/// - support `start`, `stop`, and `join`
pub struct SyncService {
    pub engine: Option<SyncEngine>,
    pub stage_worker: Arc<dyn StageWorker>,
    pub channels: RuntimeChannels,
    pub shutdown_tx: Option<oneshot::Sender<()>>,
    pub engine_task: Option<JoinHandle<Result<()>>>,
    pub watcher_task: Option<JoinHandle<Result<()>>>,
    pub worker_tasks: Vec<JoinHandle<Result<()>>>,
}

impl SyncService {
    pub fn new(
        engine: SyncEngine,
        stage_worker: Arc<dyn StageWorker>,
        channels: RuntimeChannels,
    ) -> Self {
        Self {
            engine: Some(engine),
            stage_worker,
            channels,
            shutdown_tx: None,
            engine_task: None,
            watcher_task: None,
            worker_tasks: Vec::new(),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        if self.engine_task.is_some() {
            return Err(SyncError::InvalidState("sync service already started".into()));
        }

        let engine = self
            .engine
            .as_ref()
            .ok_or_else(|| SyncError::InvalidState("sync engine is not available".into()))?;
        let (config, _) = engine.load_context().await?;

        let watcher = engine.watcher.clone();
        let local_change_tx = self.channels.local_change_tx.clone();
        let sync_root = config.sync_root.clone();
        let engine_event_tx = self.channels.engine_event_tx.clone();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        self.watcher_task = Some(tokio::spawn(async move {
            watcher.start(&sync_root, local_change_tx).await
        }));

        let mut job_rx = self
            .channels
            .job_rx
            .take()
            .ok_or_else(|| SyncError::InvalidState("job receiver is not available".into()))?;
        let stage_worker = self.stage_worker.clone();
        let engine_event_tx_for_jobs = self.channels.engine_event_tx.clone();
        self.worker_tasks.push(tokio::spawn(async move {
            while let Some(job) = job_rx.recv().await {
                match job {
                    EngineJob::Stage(job) => match stage_worker.run_stage_job(&job).await {
                        Ok(result) => {
                            engine_event_tx_for_jobs
                                .send(EngineEvent::StageJobCompleted(result))
                                .await
                                .map_err(|_| SyncError::InvalidState("engine event channel closed".into()))?;
                        }
                        Err(_) => {
                            engine_event_tx_for_jobs
                                .send(EngineEvent::StageJobFailed(job.base))
                                .await
                                .map_err(|_| SyncError::InvalidState("engine event channel closed".into()))?;
                        }
                    },
                    _ => {}
                }
            }
            Ok(())
        }));

        let engine = self
            .engine
            .take()
            .ok_or_else(|| SyncError::InvalidState("sync engine already moved".into()))?;
        self.engine_task = Some(tokio::spawn(Self::run_engine_loop(engine, shutdown_rx)));
        engine_event_tx
            .send(EngineEvent::Startup)
            .await
            .map_err(|_| SyncError::InvalidState("engine event channel closed".into()))?;

        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        Ok(())
    }

    pub async fn join(&mut self) -> Result<()> {
        if let Some(engine_task) = self.engine_task.take() {
            match engine_task.await {
                Ok(result) => result?,
                Err(join_error) => {
                    return Err(SyncError::InvalidState(format!(
                        "engine task failed to join: {join_error}"
                    )));
                }
            }
        }

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

        for worker_task in self.worker_tasks.drain(..) {
            match worker_task.await {
                Ok(result) => result?,
                Err(join_error) => {
                    return Err(SyncError::InvalidState(format!(
                        "worker task failed to join: {join_error}"
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

    async fn run_engine_loop(
        mut engine: SyncEngine,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<()> {
        loop {
            let event = tokio::select! {
                _ = &mut shutdown_rx => Some(EngineEvent::Shutdown),
                notification = engine.coordinator_notifications.recv() => {
                    notification.map(EngineEvent::RemoteChange)
                }
                change = engine.local_change_rx.recv() => {
                    change.map(EngineEvent::LocalChange)
                }
                event = engine.engine_rx.recv() => event,
            };

            let Some(event) = event else {
                break;
            };

            let should_stop = matches!(event, EngineEvent::Shutdown);

            if let EngineEvent::StageJobCompleted(result) = &event {
                engine.publish_staged_snapshot(&result.staged).await?;
            }

            let jobs = engine.handle_event(event);
            for job in jobs {
                engine
                    .job_tx
                    .send(job)
                    .await
                    .map_err(|_| SyncError::InvalidState("engine job channel closed".into()))?;
            }

            if should_stop {
                break;
            }
        }

        Ok(())
    }
}
