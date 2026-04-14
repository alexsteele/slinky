use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::local::util::walk_files;
use crate::services::{Result, Scanner};

pub struct FsScanner;

#[async_trait]
impl Scanner for FsScanner {
    async fn scan_root(&self, root: &Path) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        walk_files(root, root, &mut files)?;
        Ok(files)
    }
}
