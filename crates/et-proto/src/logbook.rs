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
