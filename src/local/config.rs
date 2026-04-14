use std::path::Path;

use crate::core::Config;
use crate::services::{Result, SyncError};

pub fn load_config(path: &Path) -> Result<Config> {
    let contents = std::fs::read_to_string(path)?;
    toml::from_str(&contents).map_err(|err| SyncError::InvalidState(err.to_string()))
}
