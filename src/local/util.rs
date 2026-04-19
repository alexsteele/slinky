use std::io::empty;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::core::{BlobHash, Config};
use crate::services::{Result, SyncError};

/// Returns the root directory for local repo state.
pub fn device_root(config: &Config) -> Result<PathBuf> {
    let key_dir = config
        .credentials
        .private_key_path
        .parent()
        .ok_or_else(|| SyncError::InvalidState("device key path has no parent".into()))?;
    let root = match key_dir.file_name().and_then(|name| name.to_str()) {
        Some("keys") => key_dir.parent().unwrap_or(key_dir),
        _ => key_dir,
    };
    Ok(root.join("repos").join(&config.repo_id))
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::device_root;
    use crate::core::{Config, DeviceCredentials};

    #[test]
    fn device_root_uses_parent_of_keys_directory() {
        let config = Config {
            sync_root: PathBuf::from("/tmp/sync"),
            repo_id: "repo-1".into(),
            device_id: "device-1".into(),
            credentials: DeviceCredentials {
                public_key: "pub".into(),
                private_key_path: PathBuf::from("/tmp/node/keys/device.key"),
            },
        };

        assert_eq!(
            device_root(&config).unwrap(),
            PathBuf::from("/tmp/node/repos/repo-1"),
        );
    }

    #[test]
    fn device_root_uses_key_directory_when_no_keys_folder_exists() {
        let config = Config {
            sync_root: PathBuf::from("/tmp/sync"),
            repo_id: "repo-1".into(),
            device_id: "device-1".into(),
            credentials: DeviceCredentials {
                public_key: "pub".into(),
                private_key_path: PathBuf::from("/tmp/node/device.key"),
            },
        };

        assert_eq!(
            device_root(&config).unwrap(),
            PathBuf::from("/tmp/node/repos/repo-1"),
        );
    }
}
