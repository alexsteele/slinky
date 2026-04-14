use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::core::{BlobHash, Config};
use crate::services::{Result, SyncError};

pub fn device_root(config: &Config) -> Result<PathBuf> {
    let home = std::env::var("HOME")
        .map(PathBuf::from)
        .map_err(|_| SyncError::InvalidState("HOME is not set".into()))?;
    Ok(home.join(".slinky").join(&config.repo_id))
}

pub fn walk_files(root: &Path, dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            walk_files(root, &path, files)?;
        } else if file_type.is_file() {
            files.push(path.strip_prefix(root).unwrap_or(&path).to_path_buf());
        }
    }
    files.sort();
    Ok(())
}

pub fn hash_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

pub fn encode_hash(hash: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in hash {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn decode_hash(value: &str) -> Result<BlobHash> {
    if value.len() != 64 {
        return Err(SyncError::InvalidState("invalid hash length".into()));
    }

    let mut out = [0; 32];
    for (idx, chunk) in value.as_bytes().chunks(2).enumerate() {
        let hi = decode_hex(chunk[0])?;
        let lo = decode_hex(chunk[1])?;
        out[idx] = (hi << 4) | lo;
    }
    Ok(out)
}

fn decode_hex(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(SyncError::InvalidState("invalid hex digit".into())),
    }
}
