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
use et_proto::logbook::{valid_session_id, LogEntry, LogMeta};
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
    /// Unix seconds: when the archive first heard of it, and when it last
    /// got lines of it.
    pub first: u64,
    pub last: u64,
    /// Unix seconds: the last change of any kind (lines, state, pause), for
    /// `LogSessions { since }`.
    #[serde(default)]
    pub changed: u64,
    /// The agent's own title (Claude's last `ai-title` line).
    #[serde(default)]
    pub title: Option<String>,
}

/// The title in a Claude `ai-title` line.
fn ai_title(line: &[u8]) -> Option<String> {
    if !line.starts_with(br#"{"type":"ai-title""#) {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(line).ok()?;
    Some(value.get("aiTitle")?.as_str()?.trim().to_string()).filter(|t| !t.is_empty())
}

/// Whole lines of `path` from `from`, at most about `max` bytes (a longer
/// line comes whole): `(start, bytes)`. `from` inside a line: from the next
/// line start. `bytes` empty: nothing whole after `start` yet.
pub fn read_lines(path: &Path, from: u64, max: usize) -> std::io::Result<(u64, Vec<u8>)> {
    use std::io::{Read, Seek, SeekFrom};
    let mut start = from;
    if from > 0 {
        let mut file = fs::File::open(path)?;
        file.seek(SeekFrom::Start(from - 1))?;
        let mut before = [0u8; 1];
        file.read_exact(&mut before)?;
        if before[0] != b'\n' {
            // Find the end of this line.
            let mut buf = vec![0u8; 64 * 1024];
            let mut at = from;
            loop {
                let n = file.read(&mut buf)?;
                if n == 0 {
                    return Ok((at, Vec::new()));
                }
                if let Some(i) = buf[..n].iter().position(|&b| b == b'\n') {
                    start = at + i as u64 + 1;
                    break;
                }
                at += n as u64;
            }
        }
    }
    Ok((start, super::capture::lines_from(path, start, max)?))
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
            changed: now,
            title: None,
        });
        // A session paused here: its source learns it within a minute
        // (`LogInfo`); until then the archive drops the lines itself.
        let paused = self.index.paused.contains(&format!("{machine}/{}", meta.mm_session));
        // A link once known stays: a back-filled transcript (capture.rs)
        // comes with none.
        let mut meta = meta;
        if meta.previous.is_none() {
            meta.previous = entry.meta.previous.clone();
        }
        let mut changed = entry.meta != meta;
        entry.meta = meta;
        // Behind the archive: the source resends what it is not sure of.
        if offset >= entry.offset {
            let (skipped, kept) = match paused {
                true => (offset + bytes.len() as u64 - entry.offset, &[][..]),
                false => (offset - entry.offset, bytes),
            };
            if skipped > 0 || !kept.is_empty() {
                let mut text = Vec::with_capacity(kept.len() + 48);
                if skipped > 0 {
                    text.extend_from_slice(gap_line(skipped).as_bytes());
                }
                text.extend_from_slice(kept);
                make_dir(path.parent().expect("a machine folder"))?;
                let mut file = fs::OpenOptions::new().create(true).append(true).mode(0o600).open(&path)?;
                // Bytes written before a crash, but not in the index: the
                // source sends them again, so drop them here.
                if file.metadata()?.len() != entry.bytes {
                    file.set_len(entry.bytes)?;
                }
                file.write_all(&text)?;
                file.sync_data()?;
                entry.offset = offset + bytes.len() as u64;
                entry.bytes += text.len() as u64;
                entry.last = now;
                if let Some(title) = kept.split(|&b| b == b'\n').rev().find_map(ai_title) {
                    entry.title = Some(title);
                }
                changed = true;
            }
        }
        if changed {
            entry.changed = now;
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

    pub fn set_paused(&mut self, machine: &str, session: &str, paused: bool, now: u64) -> anyhow::Result<()> {
        let key = format!("{machine}/{session}");
        let changed = if paused { self.index.paused.insert(key) } else { self.index.paused.remove(&key) };
        if changed {
            for entry in self.index.entries.values_mut().filter(|e| e.machine == machine && e.meta.mm_session == session) {
                entry.changed = now;
            }
            self.save()?;
        }
        Ok(())
    }

    pub fn entry(&self, machine: &str, session_id: &str) -> Option<&Entry> {
        self.index.entries.get(&format!("{machine}/{session_id}"))
    }

    /// `LogSessions`: the transcripts that changed at or after `since`.
    pub fn list(&self, since: u64) -> Vec<LogEntry> {
        self.entries()
            .filter(|e| e.changed.max(e.last) >= since)
            .map(|e| LogEntry {
                machine: e.machine.clone(),
                session_id: e.session_id.clone(),
                meta: e.meta.clone(),
                bytes: e.bytes,
                first: e.first,
                last: e.last,
                paused: self.index.paused.contains(&format!("{}/{}", e.machine, e.meta.mm_session)),
                title: e.title.clone(),
            })
            .collect()
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
    fn a_known_link_stays() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let linked = LogMeta { previous: Some("a1".into()), ..meta("busy") };
        store.append("gmktec", ID, linked, 0, b"", 1).unwrap();
        // Back-filled later, with no link.
        store.append("gmktec", ID, meta("gone"), 0, b"", 2).unwrap();
        let entry = store.entry("gmktec", ID).unwrap();
        assert_eq!((entry.meta.status.as_str(), entry.meta.previous.as_deref()), ("gone", Some("a1")));
    }

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
        store.set_paused("gmktec", "secret", true, 1).unwrap();
        store.set_paused("gmktec-2", "other", true, 1).unwrap();
        assert_eq!(store.paused_for("gmktec"), ["secret"]);
        let mut again = Store::open(dir.path()).unwrap();
        assert_eq!(again.paused_for("gmktec"), ["secret"]);
        again.set_paused("gmktec", "secret", false, 2).unwrap();
        assert!(again.paused_for("gmktec").is_empty());
    }

    #[test]
    fn the_archive_drops_lines_of_a_paused_session() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        store.append("gmktec", ID, meta("busy"), 0, b"one\n", 1).unwrap();
        // Paused before the source heard of it: it still sends.
        store.set_paused("gmktec", "claude", true, 2).unwrap();
        assert_eq!(store.append("gmktec", ID, meta("busy"), 4, b"secret\n", 3).unwrap(), 11);
        store.set_paused("gmktec", "claude", false, 4).unwrap();
        assert_eq!(store.append("gmktec", ID, meta("busy"), 11, b"two\n", 5).unwrap(), 15);
        let text = fs::read_to_string(store.path("gmktec", ID)).unwrap();
        assert_eq!(text, "one\n{\"type\":\"etd-gap\",\"skipped\":7}\ntwo\n");
    }

    #[test]
    fn titles_and_changes_since() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let lines = "{\"type\":\"user\"}\n{\"type\":\"ai-title\",\"aiTitle\":\"First\",\"sessionId\":\"x\"}\n{\"type\":\"ai-title\",\"aiTitle\":\"Logbook read API\",\"sessionId\":\"x\"}\n";
        store.append("gmktec", ID, meta("busy"), 0, lines.as_bytes(), 10).unwrap();
        store.append("trigkey", ID, meta("busy"), 0, b"{}\n", 20).unwrap();
        let all = store.list(0);
        assert_eq!(all.len(), 2);
        let gmktec = all.iter().find(|e| e.machine == "gmktec").unwrap();
        assert_eq!(gmktec.title.as_deref(), Some("Logbook read API"));
        assert_eq!(gmktec.bytes, lines.len() as u64);
        assert_eq!(store.list(15).len(), 1, "only trigkey changed since 15");
        // A new state is a change; so is a pause.
        store.append("gmktec", ID, meta("idle"), lines.len() as u64, b"", 30).unwrap();
        assert_eq!(store.list(25).len(), 1);
        store.set_paused("trigkey", "claude", true, 40).unwrap();
        let since = store.list(35);
        assert_eq!((since.len(), since[0].paused), (1, true));
    }

    #[test]
    fn reading_from_inside_a_line_starts_at_the_next() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.jsonl");
        fs::write(&path, "aaaa\nbbbb\ncc").unwrap();
        assert_eq!(read_lines(&path, 0, 1024).unwrap(), (0, b"aaaa\nbbbb\n".to_vec()));
        assert_eq!(read_lines(&path, 5, 1024).unwrap(), (5, b"bbbb\n".to_vec()));
        assert_eq!(read_lines(&path, 2, 1024).unwrap(), (5, b"bbbb\n".to_vec()), "from inside a line");
        assert_eq!(read_lines(&path, 7, 1024).unwrap(), (10, Vec::new()), "only half a line after");
        assert_eq!(read_lines(&path, 11, 1024).unwrap(), (12, Vec::new()), "inside the last, unfinished line: the end");
    }
}
