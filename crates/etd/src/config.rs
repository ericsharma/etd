//! Files: the endpoint key, paired devices, pending pairings, and status.
use std::fs;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context};
use iroh::{EndpointAddr, EndpointId, SecretKey};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Where etd keeps its files.
#[derive(Debug, Clone)]
pub struct Paths {
    /// `secret.key`, `clients.json`.
    pub config: PathBuf,
    /// `pending/`, `status.json`.
    pub state: PathBuf,
}

impl Paths {
    /// `$ETD_HOME` for both, else the platform directories + `etd`
    /// (Linux: `~/.config/etd`, `~/.local/state/etd`; macOS: both in
    /// `~/Library/Application Support/etd`).
    pub fn from_env() -> anyhow::Result<Self> {
        if let Some(dir) = std::env::var_os("ETD_HOME") {
            return Ok(Self::at(PathBuf::from(dir)));
        }
        let config = dirs::config_dir().context("no config directory")?.join("etd");
        let state = dirs::state_dir().map(|d| d.join("etd")).unwrap_or_else(|| config.clone());
        Ok(Self { config, state })
    }

    pub fn at(dir: PathBuf) -> Self {
        Self { config: dir.clone(), state: dir }
    }

    pub fn key(&self) -> PathBuf {
        self.config.join("secret.key")
    }

    /// Paired devices before the network existed; moved into it once.
    pub fn clients(&self) -> PathBuf {
        self.config.join("clients.json")
    }

    /// The signed network document.
    pub fn network(&self) -> PathBuf {
        self.config.join("network.json")
    }

    /// The network key's secret half (machines only).
    pub fn network_key(&self) -> PathBuf {
        self.config.join("network.key")
    }

    /// The network's key changes, oldest first, to send to machines that
    /// missed one.
    pub fn rotations(&self) -> PathBuf {
        self.config.join("rotations.json")
    }

    /// Held while the network document is read, changed and written.
    pub fn network_lock(&self) -> PathBuf {
        self.config.join("network.lock")
    }

    pub fn pending(&self) -> PathBuf {
        self.state.join("pending")
    }

    pub fn status(&self) -> PathBuf {
        self.state.join("status.json")
    }

    /// One JSON line per delivery of files to this machine (`Shared`).
    pub fn deliveries(&self) -> PathBuf {
        self.state.join("deliveries.jsonl")
    }

    /// Held by the running daemon, so a second etd with these files stops.
    pub fn daemon_lock(&self) -> PathBuf {
        self.state.join("etd.lock")
    }
}

/// The running daemon's hold on its files. Released when the process ends,
/// however it ends.
pub struct DaemonLock {
    _file: fs::File,
}

impl DaemonLock {
    /// Fails if another etd runs with the same files (the same iroh key).
    pub fn take(paths: &Paths) -> anyhow::Result<Self> {
        use std::os::fd::AsRawFd;
        fs::create_dir_all(&paths.state)?;
        let path = paths.daemon_lock();
        let file = fs::OpenOptions::new().create(true).truncate(false).write(true).mode(0o600).open(&path)?;
        // SAFETY: flock on a file descriptor we own; it is released on close.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let other = running_status(paths).ok().flatten();
            let who = other.map(|s| format!(" (pid {}, {})", s.pid, s.exe.as_deref().unwrap_or("?"))).unwrap_or_default();
            bail!("another etd already runs with {}{who}; stop it first", paths.state.display());
        }
        Ok(Self { _file: file })
    }
}

pub fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
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

/// Reads JSON. A missing file is `None`.
pub fn read_json<T: DeserializeOwned>(path: &Path) -> anyhow::Result<Option<T>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

/// Writes through a temporary file, so a reader never sees half a file.
pub fn write_json<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(value)?)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Client {
    pub id: EndpointId,
    pub name: String,
    /// Unix seconds.
    #[serde(default)]
    pub paired: u64,
}

/// Paired devices. A missing file is an empty list.
pub fn load_clients(paths: &Paths) -> anyhow::Result<Vec<Client>> {
    Ok(read_json(&paths.clients())?.unwrap_or_default())
}

pub fn save_clients(paths: &Paths, clients: &[Client]) -> anyhow::Result<()> {
    write_json(&paths.clients(), &clients)
}

/// Adds or renames a device.
pub fn add_client(paths: &Paths, id: EndpointId, name: &str) -> anyhow::Result<()> {
    let mut clients = load_clients(paths)?;
    clients.retain(|c| c.id != id);
    clients.push(Client { id, name: name.into(), paired: now_secs() });
    save_clients(paths, &clients)
}

/// Removes devices whose name or id (or id prefix of 8+ characters) is
/// `who`. Returns the removed ones.
pub fn remove_clients(paths: &Paths, who: &str) -> anyhow::Result<Vec<Client>> {
    let clients = load_clients(paths)?;
    let matches = |c: &Client| c.name == who || (who.len() >= 8 && c.id.to_string().starts_with(who));
    let (removed, kept): (Vec<_>, Vec<_>) = clients.into_iter().partition(matches);
    if !removed.is_empty() {
        save_clients(paths, &kept)?;
    }
    Ok(removed)
}

/// What the running daemon writes for `etd status` and `etd pair`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    pub pid: u32,
    pub version: String,
    pub host_name: String,
    pub addr: EndpointAddr,
    pub tmux: String,
    /// Names of connected devices.
    pub connected: Vec<String>,
    /// Unix seconds.
    pub updated: u64,
    /// The running program, to tell who manages it (`etd status --json`).
    #[serde(default)]
    pub exe: Option<String>,
}

/// The status of a running daemon. `None`: no daemon runs for these paths.
pub fn running_status(paths: &Paths) -> anyhow::Result<Option<Status>> {
    let Some(status) = read_json::<Status>(&paths.status())? else {
        return Ok(None);
    };
    // SAFETY: kill with signal 0 only checks that the process exists.
    let alive = unsafe { libc::kill(status.pid as libc::pid_t, 0) } == 0;
    Ok(alive.then_some(status))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_daemon_with_the_same_files_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::at(dir.path().into());
        let first = DaemonLock::take(&paths).unwrap();
        let err = DaemonLock::take(&paths).err().expect("second lock");
        assert!(format!("{err:#}").contains("another etd already runs"), "{err:#}");
        drop(first);
        DaemonLock::take(&paths).expect("free again after the first one ends");
        let other = tempfile::tempdir().unwrap();
        DaemonLock::take(&Paths::at(other.path().into())).expect("other files, other lock");
    }
}
