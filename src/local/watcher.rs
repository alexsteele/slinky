use std::path::Path;

use async_trait::async_trait;
use notify::{recommended_watcher, RecommendedWatcher, RecursiveMode, Watcher as NotifyWatcher};
use tokio::sync::{mpsc, oneshot};

use crate::services::{Result, SyncError, Watcher, WatcherEvent};

pub struct FsWatcher;

#[async_trait]
impl Watcher for FsWatcher {
    async fn start(
        &self,
        root: &Path,
        tx: mpsc::Sender<WatcherEvent>,
        ready: oneshot::Sender<()>,
        mut shutdown: oneshot::Receiver<()>,
    ) -> Result<()> {
        let root = std::fs::canonicalize(root)?;
        let tx = tx.clone();
        let mut watcher: RecommendedWatcher = recommended_watcher(
            move |result: notify::Result<notify::Event>| match result {
                Ok(event) => {
                    let message = if event.paths.is_empty() {
                        WatcherEvent::RescanRequested
                    } else {
                        WatcherEvent::PathsChanged(event.paths)
                    };
                    let _ = tx.blocking_send(message);
                }
                Err(error) => {
                    eprintln!("[watcher] notify error: {error}");
                    let _ = tx.blocking_send(WatcherEvent::RescanRequested);
                }
            },
        )
        .map_err(|err| SyncError::InvalidState(err.to_string()))?;

        watcher
            .watch(&root, RecursiveMode::Recursive)
            .map_err(|err| SyncError::InvalidState(err.to_string()))?;
        let _ = ready.send(());

        let _ = (&mut shutdown).await;
        Ok(())
    }
}

pub struct NoopWatcher;

#[async_trait]
impl Watcher for NoopWatcher {
    async fn start(
        &self,
        _root: &Path,
        _tx: mpsc::Sender<WatcherEvent>,
        ready: oneshot::Sender<()>,
        mut shutdown: oneshot::Receiver<()>,
    ) -> Result<()> {
        let _ = ready.send(());
        let _ = (&mut shutdown).await;
        Ok(())
    }
}
