//! Files: the endpoint key and the allowlist of paired devices.
use std::fs;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use iroh::{EndpointId, SecretKey};
use serde::{Deserialize, Serialize};

/// `$ETD_HOME`, else the platform config directory + `etd`
/// (`~/.config/etd` on Linux, `~/Library/Application Support/etd` on macOS).
pub fn home() -> anyhow::Result<PathBuf> {
    if let Some(dir) = std::env::var_os("ETD_HOME") {
        return Ok(PathBuf::from(dir));
    }
    Ok(dirs::config_dir().context("no config directory")?.join("etd"))
}

/// Loads the 32-byte key at `path`, or makes one (mode 0600).
pub fn load_or_create_key(path: &Path) -> anyhow::Result<SecretKey> {
    match fs::read(path) {
        Ok(bytes) => {
            let bytes: [u8; 32] = bytes.try_into().ok().with_context(|| format!("{}: not a 32-byte key", path.display()))?;
            if fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
                bail!("{}: key is readable by others; run chmod 600", path.display());
            }
            Ok(SecretKey::from_bytes(&bytes))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let key = SecretKey::generate();
            if let Some(dir) = path.parent() {
                fs::create_dir_all(dir)?;
            }
            let mut file = fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(path)?;
            file.write_all(&key.to_bytes())?;
            Ok(key)
        }
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Client {
    pub id: EndpointId,
    pub name: String,
}

fn clients_path(home: &Path) -> PathBuf {
    home.join("clients.json")
}

/// Paired devices. A missing file is an empty list.
pub fn load_clients(home: &Path) -> anyhow::Result<Vec<Client>> {
    let path = clients_path(home);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

/// Writes through a temporary file, so a reader never sees half a list.
pub fn save_clients(home: &Path, clients: &[Client]) -> anyhow::Result<()> {
    fs::create_dir_all(home)?;
    let path = clients_path(home);
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(clients)?)?;
    fs::rename(&tmp, &path)?;
    Ok(())
}
