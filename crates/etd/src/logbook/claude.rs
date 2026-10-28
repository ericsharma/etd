//! Claude Code as a Logbook source. Each running `claude` writes
//! `<claude dir>/sessions/<pid>.json` with its session id, working directory
//! and state; its transcript is `<claude dir>/projects/<dir>/<session id>.jsonl`.
//! The session id in the pid file changes while the process runs (`/clear`),
//! so it is read again each time (PLAN-logbook §5, L0).
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use et_proto::logbook::valid_session_id;
use serde::Deserialize;
use tracing::debug;

use crate::tmux::Env;

/// A running agent and its transcript.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Live {
    pub agent: &'static str,
    pub pid: u32,
    pub session_id: String,
    pub cwd: String,
    /// As the agent says: `busy`, `idle`, …
    pub status: String,
    pub transcript: PathBuf,
}

/// An agent whose transcripts the Logbook records (D5: Claude Code first).
pub trait Reader: Send {
    /// The running agents of this user that have a transcript.
    fn live(&mut self) -> Vec<Live>;
}

#[derive(Debug, Deserialize)]
struct PidFile {
    pid: u32,
    #[serde(rename = "sessionId")]
    session_id: String,
    #[serde(default)]
    cwd: String,
    #[serde(default)]
    status: Option<String>,
}

#[derive(Debug)]
pub struct Claude {
    dir: PathBuf,
    /// Transcript paths already found, by session id.
    found: HashMap<String, PathBuf>,
}

impl Claude {
    /// `$CLAUDE_CONFIG_DIR`, else `~/.claude`, of the login environment.
    pub fn from_env(env: &Env) -> Option<Self> {
        let dir = match env.get("CLAUDE_CONFIG_DIR").filter(|d| !d.is_empty()) {
            Some(dir) => PathBuf::from(dir),
            None => crate::ops::home(env).ok()?.join(".claude"),
        };
        Some(Self::at(dir))
    }

    pub fn at(dir: PathBuf) -> Self {
        Self { dir, found: HashMap::new() }
    }

    /// The transcript of `session_id`: the project folder is named after the
    /// directory Claude started in, so look in each.
    fn transcript(&mut self, session_id: &str) -> Option<PathBuf> {
        if let Some(path) = self.found.get(session_id).filter(|p| p.is_file()) {
            return Some(path.clone());
        }
        let name = format!("{session_id}.jsonl");
        let path = fs::read_dir(self.dir.join("projects")).ok()?.flatten().map(|d| d.path().join(&name)).find(|p| p.is_file())?;
        self.found.insert(session_id.into(), path.clone());
        Some(path)
    }
}

fn alive(pid: u32) -> bool {
    // SAFETY: kill with signal 0 only checks that the process exists.
    pid > 0 && unsafe { libc::kill(pid as libc::pid_t, 0) } == 0
}

fn read_pid_file(path: &Path) -> Option<PidFile> {
    let file: PidFile = serde_json::from_slice(&fs::read(path).ok()?).ok()?;
    // The file is named after its process.
    (path.file_stem()?.to_str()? == file.pid.to_string()).then_some(file)
}

impl Reader for Claude {
    fn live(&mut self) -> Vec<Live> {
        let Ok(dir) = fs::read_dir(self.dir.join("sessions")) else {
            return Vec::new();
        };
        let mut live = Vec::new();
        for entry in dir.flatten() {
            let path = entry.path();
            if path.extension().is_none_or(|e| e != "json") {
                continue;
            }
            // A pid file outlives a process that was killed.
            let Some(file) = read_pid_file(&path).filter(|f| alive(f.pid)) else {
                continue;
            };
            if !valid_session_id(&file.session_id) {
                debug!("{}: not a session id", path.display());
                continue;
            }
            // No transcript until the first prompt.
            let Some(transcript) = self.transcript(&file.session_id) else {
                continue;
            };
            live.push(Live {
                agent: "claude",
                pid: file.pid,
                session_id: file.session_id,
                cwd: file.cwd,
                status: status(file.status.as_deref()),
                transcript,
            });
        }
        live.sort_by_key(|l| l.pid);
        live
    }
}

/// The Logbook's status (`busy`, `idle`) for Claude's own: `shell` (a shell
/// command runs) is busy; `waiting` (a question to the owner) is idle.
fn status(claude: Option<&str>) -> String {
    match claude {
        Some("busy" | "shell") => "busy",
        Some("idle" | "waiting") => "idle",
        Some(other) => other,
        None => "unknown",
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_statuses_become_busy_or_idle() {
        let got: Vec<String> = [Some("busy"), Some("shell"), Some("idle"), Some("waiting"), None].map(status).into();
        assert_eq!(got, ["busy", "busy", "idle", "idle", "unknown"]);
    }

    #[test]
    fn live_agents_with_transcripts_only() {
        let dir = tempfile::tempdir().unwrap();
        let claude = dir.path();
        fs::create_dir_all(claude.join("sessions")).unwrap();
        fs::create_dir_all(claude.join("projects/-home-eric")).unwrap();
        let me = std::process::id();
        let id = "23eba963-d720-4ef2-be81-4c77fa6c9f7b";
        // This process is alive; 2 000 000 000 is above every pid limit.
        let write = |pid: u32, sid: &str| {
            let text = format!(r#"{{"pid":{pid},"sessionId":"{sid}","cwd":"/home/eric","status":"busy","kind":"interactive"}}"#);
            fs::write(claude.join(format!("sessions/{pid}.json")), text).unwrap();
        };
        write(me, id);
        write(2_000_000_000, "dead-session");
        let mut reader = Claude::at(claude.into());
        assert!(reader.live().is_empty(), "no transcript yet");
        let transcript = claude.join(format!("projects/-home-eric/{id}.jsonl"));
        fs::write(&transcript, "{}\n").unwrap();
        let live = reader.live();
        assert_eq!(live.len(), 1, "{live:?}");
        assert_eq!((live[0].pid, live[0].session_id.as_str(), live[0].status.as_str()), (me, id, "busy"));
        assert_eq!(live[0].transcript, transcript);
        // A pid file whose name is not its pid is ignored.
        fs::rename(claude.join(format!("sessions/{me}.json")), claude.join("sessions/12.json")).unwrap();
        assert!(reader.live().is_empty());
    }

    #[test]
    fn config_dir_from_the_environment() {
        let env: Env = [("HOME".to_string(), "/h".to_string())].into_iter().collect();
        assert_eq!(Claude::from_env(&env).unwrap().dir, Path::new("/h/.claude"));
        let env: Env = [("HOME".to_string(), "/h".to_string()), ("CLAUDE_CONFIG_DIR".to_string(), "/c".to_string())].into_iter().collect();
        assert_eq!(Claude::from_env(&env).unwrap().dir, Path::new("/c"));
    }
}
