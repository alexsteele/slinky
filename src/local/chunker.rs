//! Local file chunking helpers.
//!
//! This starts with a simple fixed-size chunker so the engine can stream multi-blob files today.
//! TODO: Replace fixed-size boundaries with rolling chunking without changing the engine flow.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

use crate::core::{Blob, FullBlob};
use crate::local::util::hash_bytes;
use crate::services::{Chunk, ChunkReader, Chunker, Result};

const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;

/// Chunks local files into fixed-size blobs.
pub struct LocalChunker {
    chunk_size: usize,
}

struct LocalChunkReader {
    file: File,
    chunk_size: usize,
    done: bool,
}

impl LocalChunker {
    pub fn new(chunk_size: usize) -> Self {
        Self { chunk_size }
    }

    pub fn open() -> Arc<dyn Chunker> {
        Arc::new(Self::new(DEFAULT_CHUNK_SIZE))
    }
}

#[async_trait]
impl Chunker for LocalChunker {
    async fn open(&self, path: &Path) -> Result<Box<dyn ChunkReader>> {
        let file = File::open(path)?;
        Ok(Box::new(LocalChunkReader {
            file,
            chunk_size: self.chunk_size,
            done: false,
        }))
    }
}

#[async_trait]
impl ChunkReader for LocalChunkReader {
    async fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        if self.done {
            return Ok(None);
        }

        let mut buf = vec![0; self.chunk_size];
        let read = self.file.read(&mut buf)?;
        if read == 0 {
            self.done = true;
            return Ok(None);
        }

        buf.truncate(read);
        let hash = hash_bytes(&buf);
        Ok(Some(Chunk {
            blob: Blob {
                hash,
                size: read as u64,
            },
            full_blob: FullBlob {
                hash,
                size: read as u64,
                data: buf,
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::LocalChunker;
    use crate::services::Chunker;

    #[tokio::test]
    async fn streams_large_file_into_multiple_blobs() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("large.bin");
        std::fs::write(&path, vec![7_u8; 10]).unwrap();

        let chunker = LocalChunker::new(4);
        let mut reader = chunker.open(&path).await.unwrap();

        let first = reader.next_chunk().await.unwrap().unwrap();
        let second = reader.next_chunk().await.unwrap().unwrap();
        let third = reader.next_chunk().await.unwrap().unwrap();
        let end = reader.next_chunk().await.unwrap();

        assert_eq!(first.blob.size, 4);
        assert_eq!(second.blob.size, 4);
        assert_eq!(third.blob.size, 2);
        assert!(end.is_none());
    }

    #[tokio::test]
    async fn streams_empty_file_to_no_blobs() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.bin");
        std::fs::write(&path, b"").unwrap();

        let chunker = LocalChunker::new(4);
        let mut reader = chunker.open(&path).await.unwrap();

        assert!(reader.next_chunk().await.unwrap().is_none());
    }
}
