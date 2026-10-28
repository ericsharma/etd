//! An archive's copy of the transcripts (PLAN-logbook §4.3):
//! `logbook/<machine>/<session id>.jsonl`, and `index.json` with what is
//! known about each. The folders are for this user only (0700), the
//! transcripts too (0600): they hold all a session's tool output.
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use et_proto::logbook::{valid_session_id, LogMeta};
use serde::{Deserialize, Serialize};

use crate::config;

/// One transcript on the archive.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Entry {
    /// The network's name for the source machine.
    pub machine: String,
    pub session_id: String,
    pub meta: LogMeta,
    /// How far into the source's file the archive is, skipped bytes
    /// included. The source sends from here.
    pub offset: u64,
    /// The size of the file here.
    pub bytes: u64,
    /// Unix seconds: when the archive first and last heard of it.
    pub first: u64,
    pub last: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Index {
    /// By `<machine>/<session id>`.
    entries: BTreeMap<String, Entry>,
    /// `<machine>/<manymux session>` not to record.
    #[serde(default)]
    paused: BTreeSet<String>,
}

#[derive(Debug)]
pub struct Store {
    dir: PathBuf,
    index: Index,
}

/// Written in place of the bytes a source skipped (a session that must not
/// be recorded): readers see that something is missing, and how much.
fn gap_line(skipped: u64) -> String {
    format!("{{\"type\":\"etd-gap\",\"skipped\":{skipped}}}\n")
}

fn make_dir(dir: &Path) -> anyhow::Result<()> {
    fs::DirBuilder::new().recursive(true).mode(0o700).create(dir).with_context(|| format!("make {}", dir.display()))
}

impl Store {
    pub fn open(dir: &Path) -> anyhow::Result<Self> {
        make_dir(dir)?;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
        let index = config::read_json(&Self::index_path(dir))?.unwrap_or_default();
        Ok(Self { dir: dir.into(), index })
    }

    fn index_path(dir: &Path) -> PathBuf {
        dir.join("index.json")
    }

    /// The entries of the archive at `dir`, without opening it for writing
    /// (`etd logbook`).
    pub fn read_entries(dir: &Path) -> anyhow::Result<Vec<Entry>> {
        let index: Index = config::read_json(&Self::index_path(dir))?.unwrap_or_default();
        Ok(index.entries.into_values().collect())
    }

    pub fn entries(&self) -> impl Iterator<Item = &Entry> {
        self.index.entries.values()
    }

    pub fn path(&self, machine: &str, session_id: &str) -> PathBuf {
        self.dir.join(machine).join(format!("{session_id}.jsonl"))
    }

    /// `LogAppend` from `machine`. Returns the source offset the archive has
    /// reached for that transcript.
    pub fn append(&mut self, machine: &str, session_id: &str, meta: LogMeta, offset: u64, bytes: &[u8], now: u64) -> anyhow::Result<u64> {
        if !valid_session_id(session_id) {
            bail!("not a session id: {session_id:?}");
        }
        // Network names of machines are letters, digits, '.', '_', '-'.
        if machine.is_empty() || machine.starts_with('.') || machine.contains('/') {
            bail!("not a machine name: {machine:?}");
        }
        let path = self.path(machine, session_id);
        let key = format!("{machine}/{session_id}");
        let entry = self.index.entries.entry(key).or_insert_with(|| Entry {
            machine: machine.into(),
            session_id: session_id.into(),
            meta: meta.clone(),
            offset: 0,
            bytes: 0,
            first: now,
            last: now,
        });
        let mut changed = entry.meta != meta;
        entry.meta = meta;
        let skipped = offset.saturating_sub(entry.offset);
        // Behind the archive: the source resends what it is not sure of.
        if offset >= entry.offset && (skipped > 0 || !bytes.is_empty()) {
            let mut text = Vec::with_capacity(bytes.len() + 48);
            if skipped > 0 {
                text.extend_from_slice(gap_line(skipped).as_bytes());
            }
            text.extend_from_slice(bytes);
            make_dir(path.parent().expect("a machine folder"))?;
            let mut file = fs::OpenOptions::new().create(true).append(true).mode(0o600).open(&path)?;
            // Bytes written before a crash, but not in the index: the source
            // sends them again, so drop them here.
            if file.metadata()?.len() != entry.bytes {
                file.set_len(entry.bytes)?;
            }
            file.write_all(&text)?;
            file.sync_data()?;
            entry.offset = offset + bytes.len() as u64;
            entry.bytes += text.len() as u64;
            entry.last = now;
            changed = true;
        }
        let reached = entry.offset;
        if changed {
            self.save()?;
        }
        Ok(reached)
    }

    fn save(&self) -> anyhow::Result<()> {
        config::write_json(&Self::index_path(&self.dir), &self.index)
    }

    /// The manymux sessions of `machine` that are not recorded.
    pub fn paused_for(&self, machine: &str) -> Vec<String> {
        self.index.paused.iter().filter_map(|p| p.strip_prefix(machine)?.strip_prefix('/')).map(String::from).collect()
    }

    pub fn set_paused(&mut self, machine: &str, session: &str, paused: bool) -> anyhow::Result<()> {
        let key = format!("{machine}/{session}");
        let changed = if paused { self.index.paused.insert(key) } else { self.index.paused.remove(&key) };
        if changed {
            self.save()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(status: &str) -> LogMeta {
        LogMeta { agent: "claude".into(), mm_session: "claude".into(), status: status.into(), ..Default::default() }
    }

    const ID: &str = "23eba963-d720-4ef2-be81-4c77fa6c9f7b";

    #[test]
    fn appends_in_order_and_acks_the_offset() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        // A new source asks where to start.
        assert_eq!(store.append("gmktec", ID, meta("busy"), 0, b"", 10).unwrap(), 0);
        assert_eq!(store.append("gmktec", ID, meta("busy"), 0, b"{\"a\":1}\n", 11).unwrap(), 8);
        // Sent again after a lost answer: nothing written twice.
        assert_eq!(store.append("gmktec", ID, meta("busy"), 0, b"{\"a\":1}\n", 12).unwrap(), 8);
        assert_eq!(store.append("gmktec", ID, meta("idle"), 8, b"{\"b\":2}\n", 13).unwrap(), 16);
        let path = store.path("gmktec", ID);
        assert_eq!(fs::read_to_string(&path).unwrap(), "{\"a\":1}\n{\"b\":2}\n");
        assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777, 0o700);
        // The index survives a restart.
        let again = Store::open(dir.path()).unwrap();
        let entry = again.entries().next().unwrap();
        assert_eq!((entry.offset, entry.bytes, entry.first, entry.last), (16, 16, 10, 13));
        assert_eq!(entry.meta.status, "idle");
    }

    #[test]
    fn skipped_bytes_leave_a_gap_line() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        store.append("gmktec", ID, meta("busy"), 0, b"one\n", 1).unwrap();
        // Paused from 4 to 100, then recorded again.
        assert_eq!(store.append("gmktec", ID, meta("busy"), 100, b"", 2).unwrap(), 100);
        assert_eq!(store.append("gmktec", ID, meta("busy"), 100, b"two\n", 3).unwrap(), 104);
        let text = fs::read_to_string(store.path("gmktec", ID)).unwrap();
        assert_eq!(text, "one\n{\"type\":\"etd-gap\",\"skipped\":96}\ntwo\n");
        assert_eq!(store.entries().next().unwrap().bytes, text.len() as u64);
    }

    #[test]
    fn bytes_not_in_the_index_are_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        store.append("gmktec", ID, meta("busy"), 0, b"one\n", 1).unwrap();
        // A crash after the write, before the index: the file is longer.
        fs::OpenOptions::new().append(true).open(store.path("gmktec", ID)).unwrap().write_all(b"two\n").unwrap();
        assert_eq!(store.append("gmktec", ID, meta("busy"), 4, b"two\n", 2).unwrap(), 8);
        assert_eq!(fs::read_to_string(store.path("gmktec", ID)).unwrap(), "one\ntwo\n");
    }

    #[test]
    fn refuses_names_that_leave_the_folder() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        assert!(store.append("gmktec", "../../x", meta("busy"), 0, b"x\n", 1).is_err());
        assert!(store.append("..", ID, meta("busy"), 0, b"x\n", 1).is_err());
        assert!(store.append("a/b", ID, meta("busy"), 0, b"x\n", 1).is_err());
    }

    #[test]
    fn paused_sessions_per_machine() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        store.set_paused("gmktec", "secret", true).unwrap();
        store.set_paused("gmktec-2", "other", true).unwrap();
        assert_eq!(store.paused_for("gmktec"), ["secret"]);
        let mut again = Store::open(dir.path()).unwrap();
        assert_eq!(again.paused_for("gmktec"), ["secret"]);
        again.set_paused("gmktec", "secret", false).unwrap();
        assert!(again.paused_for("gmktec").is_empty());
    }
}
