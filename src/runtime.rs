use crate::engine::SyncEngine;
use crate::services::{Result, SyncError};

/// SyncService is the top-level lifecycle object for the local sync process.
///
/// The runtime is intentionally thin right now. `start()` performs the simple bootstrap flow by
/// asking the engine to publish the current tree. Longer-lived watcher and transfer tasks can be
/// wired in once we flesh out steady-state sync behavior.
pub struct SyncService {
    pub engine: SyncEngine,
    started: bool,
}

impl SyncService {
    pub fn new(engine: SyncEngine) -> Self {
        Self {
            engine,
            started: false,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        if self.started {
            return Err(SyncError::InvalidState(
                "sync service already started".into(),
            ));
        }

        self.engine.start().await?;
        self.started = true;
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.started = false;
        Ok(())
    }

    pub async fn join(&mut self) -> Result<()> {
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        self.start().await?;
        self.join().await
    }
}
