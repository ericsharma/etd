//! The Logbook (et-ios docs/PLAN-logbook.md): each machine's etd sends the
//! transcripts of the AI agents (Claude Code) that run in its manymux
//! sessions to the archive machines of the network.
//!
//! A transcript is append-only JSONL. The source sends it in order, in whole
//! lines, with the offset in its own file where the bytes start. The archive
//! keeps the source offset it has reached for each transcript and answers it
//! (`LogAck`), so a source that restarts or reconnects goes on from there.
use serde::{Deserialize, Serialize};

/// What a source knows about one transcript, sent with each `LogAppend`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogMeta {
    /// The agent that writes the transcript: `claude`.
    pub agent: String,
    /// The manymux session it runs in, on the source machine.
    pub mm_session: String,
    pub cwd: String,
    /// The git remote URL of `cwd`, else its repository root.
    pub repo: Option<String>,
    pub branch: Option<String>,
    /// `busy`, `idle` (as the agent says), or `gone` (its process ended, or
    /// it went on in another transcript).
    pub status: String,
    /// The transcript before this one in the same manymux session (after
    /// `/clear` or a resume).
    pub previous: Option<String>,
}

/// One transcript on an archive (`LogSessions`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    /// The network's name for the source machine.
    pub machine: String,
    pub session_id: String,
    pub meta: LogMeta,
    /// The size of the archive's copy: offsets for `LogRead` are in it.
    pub bytes: u64,
    /// Unix seconds: when the archive first and last heard of it.
    pub first: u64,
    pub last: u64,
    /// Its manymux session is not recorded ("do not record").
    pub paused: bool,
    /// The agent's own title for it (Claude's last `ai-title` line).
    pub title: Option<String>,
}

/// What `LogSend` sends.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogSelection {
    /// Exchanges of one transcript, by the uuid of each prompt.
    Turns { machine: String, session_id: String, turn_ids: Vec<String> },
    /// One project of a report (L3).
    ReportSection { report_id: String, project_id: String },
}

/// The answer to `Report { refresh: false }` before the first report.
pub const NO_REPORT: &str = r#"{"id":null,"taken":0,"projects":[],"unassigned":[],"proposals":[]}"#;

/// A transcript id: an agent's session id. Also its file name on the
/// archive, so only letters, digits, `-` and `_`.
pub fn valid_session_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 64 && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_ids_are_file_names() {
        assert!(valid_session_id("23eba963-d720-4ef2-be81-4c77fa6c9f7b"));
        for bad in ["", "../x", "a/b", "a b", ".", &"x".repeat(65)] {
            assert!(!valid_session_id(bad), "{bad:?}");
        }
    }
}
