//! The Logbook's sending side (PLAN-logbook §4.1). Each tick: which agents
//! run inside this machine's manymux sessions, and the new whole lines of
//! their transcripts, to each archive.
//!
//! An agent belongs to the manymux session whose leader is its parent, or
//! its parent's parent, and so on. Not `MM_SESSION`: an agent whose session
//! ended keeps it (L0: orphans on mac-mini). An agent in no live session is
//! not recorded.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use et_proto::logbook::LogMeta;
use et_proto::{read_frame, write_frame, Request, Response};
use iroh::{Endpoint, EndpointId};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use super::claude::{Live, Reader};
use super::store::Store;
use crate::config;
use crate::local::Connections;
use crate::tmux::Env;

/// Most bytes in one `LogAppend`, unless one line is longer.
pub const BATCH: usize = 256 * 1024;
/// A line longer than this goes in pieces (the frame limit is 16 MiB).
const LONGEST: usize = 15 * 1024 * 1024;
/// After an archive did not answer, leave it this long.
const BACKOFF: Duration = Duration::from_secs(30);
/// A transcript that ended is dropped after this, even if an archive never
/// got all of it (that archive has the rest when the source comes back:
/// only live transcripts are sent).
const KEEP_GONE: Duration = Duration::from_secs(3600);
/// Deepest parent chain walked from an agent to its session's leader.
const MAX_DEPTH: usize = 64;

/// Where transcripts go.
#[derive(Debug, Clone)]
pub enum Archive {
    /// This machine is an archive.
    Here(Arc<Mutex<Store>>),
    /// Another machine of the network. `paused`: this machine's manymux
    /// sessions it does not record.
    There { id: EndpointId, name: String, paused: HashSet<String> },
}

impl Archive {
    pub fn name<'a>(&'a self, me: &'a str) -> &'a str {
        match self {
            Archive::Here(_) => me,
            Archive::There { name, .. } => name,
        }
    }

    fn paused(&self, session: &str, here: &HashSet<String>) -> bool {
        match self {
            Archive::Here(_) => here.contains(session),
            Archive::There { paused, .. } => paused.contains(session),
        }
    }
}

/// How a source reaches the archives on other machines.
#[derive(Debug, Clone)]
pub struct Net {
    pub endpoint: Endpoint,
    pub connections: Arc<Connections>,
    /// The network's name for this machine.
    pub me: String,
}

/// `ps -A -o pid= -o ppid=`: each process's parent. Linux and macOS.
pub async fn parents(env: &Env) -> anyhow::Result<HashMap<u32, u32>> {
    let out = Command::new("ps")
        .args(["-A", "-o", "pid=", "-o", "ppid="])
        .env_clear()
        .envs(env)
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .context("run ps")?;
    if !out.status.success() {
        bail!("ps: {}", out.status);
    }
    Ok(parse_ps(&String::from_utf8_lossy(&out.stdout)))
}

pub fn parse_ps(text: &str) -> HashMap<u32, u32> {
    text.lines()
        .filter_map(|line| {
            let mut words = line.split_whitespace();
            Some((words.next()?.parse().ok()?, words.next()?.parse().ok()?))
        })
        .collect()
}

/// The manymux session (by leader pid) that `pid` runs in, if any.
pub fn owner(pid: u32, leaders: &HashMap<u32, String>, parents: &HashMap<u32, u32>) -> Option<u32> {
    let mut at = pid;
    for _ in 0..MAX_DEPTH {
        if leaders.contains_key(&at) {
            return Some(at);
        }
        at = *parents.get(&at)?;
        if at <= 1 {
            return None;
        }
    }
    None
}

/// Up to `BATCH` bytes of `path` from `from`, cut after the last newline.
/// A line longer than `BATCH` comes whole (up to `LONGEST`). Empty: no
/// whole line yet.
pub fn whole_lines(path: &Path, from: u64) -> std::io::Result<Vec<u8>> {
    let mut file = fs::File::open(path)?;
    file.seek(SeekFrom::Start(from))?;
    let mut buf = Vec::new();
    (&mut file).take(BATCH as u64).read_to_end(&mut buf)?;
    if let Some(end) = buf.iter().rposition(|&b| b == b'\n') {
        buf.truncate(end + 1);
        return Ok(buf);
    }
    if buf.len() < BATCH {
        return Ok(Vec::new());
    }
    // One long line: read on to its end.
    let mut rest = Vec::new();
    (&mut file).take((LONGEST - BATCH) as u64).read_to_end(&mut rest)?;
    match rest.iter().position(|&b| b == b'\n') {
        Some(end) => buf.extend_from_slice(&rest[..=end]),
        None if buf.len() + rest.len() >= LONGEST => buf.extend_from_slice(&rest),
        None => return Ok(Vec::new()),
    }
    Ok(buf)
}

/// What one archive has of one transcript.
#[derive(Debug, Default)]
struct Sent {
    /// None until the archive said where it is.
    offset: Option<u64>,
    meta: Option<LogMeta>,
}

#[derive(Debug)]
struct Tracked {
    path: PathBuf,
    meta: LogMeta,
    /// By archive name.
    sent: HashMap<String, Sent>,
    gone_since: Option<Instant>,
}

/// For `etd logbook` (`logbook-status.json`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TranscriptStatus {
    pub session_id: String,
    pub mm_session: String,
    pub status: String,
    pub size: u64,
    /// Bytes each archive has, by archive name.
    pub sent: BTreeMap<String, u64>,
    pub paused: bool,
}

pub struct Capture {
    readers: Vec<Box<dyn Reader>>,
    env: Env,
    /// By session id.
    tracked: BTreeMap<String, Tracked>,
    /// The last transcript seen in each manymux session, for `previous`.
    last: HashMap<String, String>,
    /// Agent pid → its session's leader pid, while both live.
    owners: HashMap<u32, u32>,
    /// Archives that did not answer, until when to leave them.
    resting: HashMap<String, Instant>,
    /// Git facts per directory: (repo, branch).
    git: HashMap<String, (Option<String>, Option<String>)>,
}

impl Capture {
    pub fn new(readers: Vec<Box<dyn Reader>>, env: Env) -> Self {
        Self {
            readers,
            env,
            tracked: BTreeMap::new(),
            last: HashMap::new(),
            owners: HashMap::new(),
            resting: HashMap::new(),
            git: HashMap::new(),
        }
    }

    /// One round. `sessions`: this machine's manymux sessions (name, leader
    /// pid). `here_paused`: sessions this machine's own archive does not
    /// record.
    pub async fn tick(
        &mut self,
        sessions: &[(String, u32)],
        archives: &[Archive],
        net: &Net,
        here_paused: &HashSet<String>,
    ) -> Vec<TranscriptStatus> {
        let leaders: HashMap<u32, String> = sessions.iter().map(|(name, pid)| (*pid, name.clone())).collect();
        let live: Vec<Live> = self.readers.iter_mut().flat_map(|r| r.live()).collect();
        self.owners.retain(|agent, leader| leaders.contains_key(leader) && live.iter().any(|l| l.pid == *agent));
        let mut table = None;
        let mut current = HashSet::new();
        for agent in live {
            let leader = match self.owners.get(&agent.pid) {
                Some(leader) => *leader,
                None => {
                    if table.is_none() {
                        table = Some(parents(&self.env).await.unwrap_or_else(|e| {
                            warn!("logbook: {e:#}");
                            HashMap::new()
                        }));
                    }
                    let Some(leader) = owner(agent.pid, &leaders, table.as_ref().expect("just set")) else {
                        continue;
                    };
                    self.owners.insert(agent.pid, leader);
                    leader
                }
            };
            let mm_session = leaders[&leader].clone();
            current.insert(agent.session_id.clone());
            self.track(agent, mm_session).await;
        }
        for (id, tracked) in &mut self.tracked {
            if !current.contains(id) && tracked.gone_since.is_none() {
                tracked.meta.status = "gone".into();
                tracked.gone_since = Some(Instant::now());
            }
        }
        let mut statuses = Vec::new();
        let ids: Vec<String> = self.tracked.keys().cloned().collect();
        for id in ids {
            let mut synced = true;
            for archive in archives {
                synced &= self.sync(&id, archive, net, here_paused).await;
            }
            let tracked = &self.tracked[&id];
            let size = fs::metadata(&tracked.path).map(|m| m.len()).unwrap_or(0);
            let paused = archives.iter().any(|a| a.paused(&tracked.meta.mm_session, here_paused));
            statuses.push(TranscriptStatus {
                session_id: id.clone(),
                mm_session: tracked.meta.mm_session.clone(),
                status: tracked.meta.status.clone(),
                size,
                sent: tracked.sent.iter().filter_map(|(name, s)| Some((name.clone(), s.offset?))).collect(),
                paused,
            });
            if let Some(since) = tracked.gone_since {
                if synced || since.elapsed() > KEEP_GONE {
                    debug!("logbook: {id} done");
                    self.tracked.remove(&id);
                }
            }
        }
        statuses
    }

    /// Starts following a transcript, or updates what is known of it.
    async fn track(&mut self, agent: Live, mm_session: String) {
        if let Some(tracked) = self.tracked.get_mut(&agent.session_id) {
            tracked.meta.status = agent.status;
            tracked.meta.cwd = agent.cwd;
            tracked.gone_since = None;
            return;
        }
        let (repo, branch) = match self.git.get(&agent.cwd) {
            Some(known) => known.clone(),
            None => {
                let found = git_facts(&self.env, &agent.cwd).await;
                self.git.insert(agent.cwd.clone(), found.clone());
                found
            }
        };
        let previous = self.last.insert(mm_session.clone(), agent.session_id.clone()).filter(|p| *p != agent.session_id);
        info!("logbook: {} in {mm_session}: {}", agent.agent, agent.session_id);
        let meta = LogMeta { agent: agent.agent.into(), mm_session, cwd: agent.cwd, repo, branch, status: agent.status, previous };
        self.tracked.insert(agent.session_id, Tracked { path: agent.transcript, meta, sent: HashMap::new(), gone_since: None });
    }

    /// Sends what `archive` does not have of transcript `id`. True when it
    /// has all of it.
    async fn sync(&mut self, id: &str, archive: &Archive, net: &Net, here_paused: &HashSet<String>) -> bool {
        let name = archive.name(&net.me).to_string();
        if self.resting.get(&name).is_some_and(|until| Instant::now() < *until) {
            return false;
        }
        let tracked = self.tracked.get_mut(id).expect("a tracked id");
        let paused = archive.paused(&tracked.meta.mm_session, here_paused);
        let result = sync_one(id, tracked, &name, archive, net, paused).await;
        match result {
            Ok(done) => done,
            Err(e) => {
                warn!("logbook: {name}: {e:#}; again in {BACKOFF:?}");
                self.resting.insert(name.clone(), Instant::now() + BACKOFF);
                // Not sure what arrived: ask again next time.
                for tracked in self.tracked.values_mut() {
                    tracked.sent.remove(&name);
                }
                false
            }
        }
    }
}

async fn sync_one(id: &str, tracked: &mut Tracked, name: &str, archive: &Archive, net: &Net, paused: bool) -> anyhow::Result<bool> {
    let size = match fs::metadata(&tracked.path) {
        Ok(m) => m.len(),
        // Deleted: nothing more to send.
        Err(_) => return Ok(true),
    };
    let sent = tracked.sent.entry(name.to_string()).or_default();
    let mut offset = match sent.offset {
        Some(offset) => offset,
        None => {
            let reached = append(archive, net, id, &tracked.meta, 0, Vec::new()).await?;
            sent.meta = Some(tracked.meta.clone());
            reached
        }
    };
    if paused {
        // Skip to the end of the last whole line: those bytes never leave.
        if offset < size && ends_with_newline(&tracked.path, size) {
            offset = append(archive, net, id, &tracked.meta, size, Vec::new()).await?;
            sent.meta = Some(tracked.meta.clone());
        }
    } else {
        while offset < size {
            let bytes = whole_lines(&tracked.path, offset)?;
            if bytes.is_empty() {
                break;
            }
            let reached = append(archive, net, id, &tracked.meta, offset, bytes).await?;
            sent.meta = Some(tracked.meta.clone());
            if reached <= offset {
                bail!("the archive did not take {id} at {offset}");
            }
            offset = reached;
        }
    }
    if offset > size {
        // The file is shorter than what the archive has: it was replaced.
        debug!("logbook: {id}: archive {name} has {offset} bytes, the file {size}");
    }
    if sent.meta.as_ref() != Some(&tracked.meta) {
        offset = append(archive, net, id, &tracked.meta, offset, Vec::new()).await?;
        sent.meta = Some(tracked.meta.clone());
    }
    sent.offset = Some(offset);
    Ok(offset >= size)
}

fn ends_with_newline(path: &Path, size: u64) -> bool {
    let read = || -> std::io::Result<bool> {
        let mut file = fs::File::open(path)?;
        file.seek(SeekFrom::Start(size.saturating_sub(1)))?;
        let mut last = [0u8; 1];
        file.read_exact(&mut last)?;
        Ok(last[0] == b'\n')
    };
    size > 0 && read().unwrap_or(false)
}

/// One `LogAppend`; the archive's offset.
async fn append(archive: &Archive, net: &Net, id: &str, meta: &LogMeta, offset: u64, bytes: Vec<u8>) -> anyhow::Result<u64> {
    match archive {
        Archive::Here(store) => store.lock().await.append(&net.me, id, meta.clone(), offset, &bytes, config::now_secs()),
        Archive::There { id: machine, .. } => {
            let request = Request::LogAppend { session_id: id.into(), meta: meta.clone(), offset, bytes };
            let asked = async {
                let conn = net.connections.to(&net.endpoint, *machine).await?;
                let (mut send, mut recv) = conn.open_bi().await?;
                write_frame(&mut send, &request).await?;
                send.finish()?;
                read_frame::<Response>(&mut recv).await?.context("no answer")
            };
            match tokio::time::timeout(Duration::from_secs(30), asked).await.context("timed out")?? {
                Response::LogAck { offset } => Ok(offset),
                Response::Error(e) => bail!("{e}"),
                other => bail!("unexpected answer: {other:?}"),
            }
        }
    }
}

/// The repository of `cwd` (its `origin` URL, else its root) and the branch.
async fn git_facts(env: &Env, cwd: &str) -> (Option<String>, Option<String>) {
    let git = |args: &'static [&'static str]| {
        let mut cmd = Command::new("git");
        cmd.arg("-C").arg(cwd).args(args).env_clear().envs(env).stdin(std::process::Stdio::null()).stderr(std::process::Stdio::null());
        async move {
            let out = cmd.output().await.ok().filter(|o| o.status.success())?;
            Some(String::from_utf8_lossy(&out.stdout).trim().to_string()).filter(|s| !s.is_empty())
        }
    };
    let repo = match git(&["config", "--get", "remote.origin.url"]).await {
        Some(url) => Some(url),
        None => git(&["rev-parse", "--show-toplevel"]).await,
    };
    let branch = if repo.is_some() { git(&["branch", "--show-current"]).await } else { None };
    (repo, branch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn ps_output_and_owners() {
        // trigkey: claude under the node; mac-mini: claude under `sh -mc`
        // under the node; an orphan whose parent is launchd.
        let table = parse_ps("    1     0\n  100     1\n  200   100\n  300   100\n  301   300\n  400     1\n garbage\n");
        assert_eq!(table.len(), 6);
        let leaders: HashMap<u32, String> = [(200, "claude".to_string()), (300, "claude2".to_string())].into_iter().collect();
        assert_eq!(owner(200, &leaders, &table), Some(200), "the agent is the leader");
        assert_eq!(owner(301, &leaders, &table), Some(300), "one level down");
        assert_eq!(owner(400, &leaders, &table), None, "an orphan");
        assert_eq!(owner(999, &leaders, &table), None, "unknown");
    }

    #[test]
    fn batches_end_at_a_line_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.jsonl");
        fs::write(&path, "one\ntwo\nthr").unwrap();
        assert_eq!(whole_lines(&path, 0).unwrap(), b"one\ntwo\n");
        assert_eq!(whole_lines(&path, 4).unwrap(), b"two\n");
        assert!(whole_lines(&path, 8).unwrap().is_empty(), "half a line waits");
        fs::OpenOptions::new().append(true).open(&path).unwrap().write_all(b"ee\n").unwrap();
        assert_eq!(whole_lines(&path, 8).unwrap(), b"three\n");
        // Many lines: one batch is at most BATCH bytes.
        let line = format!("{}\n", "x".repeat(999));
        fs::write(&path, line.repeat(600)).unwrap();
        let first = whole_lines(&path, 0).unwrap();
        assert!(first.len() <= BATCH && first.len().is_multiple_of(1000) && !first.is_empty());
        // One line longer than BATCH comes whole.
        let long = format!("{}\nnext\n", "y".repeat(BATCH + 10));
        fs::write(&path, &long).unwrap();
        assert_eq!(whole_lines(&path, 0).unwrap().len(), BATCH + 11);
    }

    #[test]
    fn newline_at_the_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        fs::write(&path, "a\n").unwrap();
        assert!(ends_with_newline(&path, 2));
        fs::write(&path, "a\nb").unwrap();
        assert!(!ends_with_newline(&path, 3));
        assert!(!ends_with_newline(&path, 0));
    }
}
