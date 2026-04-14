use std::path::Path;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::services::{Result, Watcher, WatcherEvent};

pub struct NoopWatcher;

#[async_trait]
impl Watcher for NoopWatcher {
    async fn start(&self, _root: &Path, _tx: mpsc::Sender<WatcherEvent>) -> Result<()> {
        Ok(())
    }
}
