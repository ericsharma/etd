//! The AI agents for the apps (et-ios docs/PLAN-multi-agent.md §4.1): the
//! built-in profiles, and what each machine has.
use et_proto::{Request, Response};

use crate::{unexpected, LinkError, RemoteHost};

/// What E.T. knows about one agent CLI: how to start it, find it, type to
/// it, and show it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, uniffi::Record)]
pub struct AgentProfile {
    /// "claude", "codex", "gemini", "grok": also the Logbook's agent.
    pub id: String,
    pub name: String,
    /// One letter, shown on the accent color.
    pub mark: String,
    /// A Catppuccin Mocha color name: "peach", "green", "blue", "mauve".
    pub accent: String,
    pub launch: Vec<String>,
    /// Program names that mean "this agent runs".
    pub processes: Vec<String>,
    pub version_args: Vec<String>,
    /// The key wheel set.
    pub keys: Vec<String>,
    /// "bracketed-paste" | "line-by-line"
    pub multiline: String,
    /// Dictated "at <file>" becomes this prefix + path ("@").
    pub mention: Option<String>,
    /// What starts the agent's reply on screen ("⏺").
    pub reply_marker: Option<String>,
    pub headless: Vec<String>,
}

/// One agent on one machine.
#[derive(Debug, Clone, PartialEq, Eq, Hash, uniffi::Record)]
pub struct AgentOnMachine {
    pub id: String,
    /// None: not installed. Empty: installed, version unknown.
    pub version: Option<String>,
    pub path: Option<String>,
    /// The Logbook reads its transcripts.
    pub transcript: bool,
    /// Where its live state comes from: "own", "hooks", "transcript", "none".
    pub state: String,
    /// "Needs you" reaches etd.
    pub alerts: bool,
    /// What to do for a missing part.
    pub setup_hint: Option<String>,
}

impl From<et_proto::agents::AgentProfile> for AgentProfile {
    fn from(p: et_proto::agents::AgentProfile) -> Self {
        AgentProfile {
            id: p.id,
            name: p.name,
            mark: p.mark,
            accent: p.accent,
            launch: p.launch,
            processes: p.processes,
            version_args: p.version_args,
            keys: p.keys,
            multiline: p.multiline,
            mention: p.mention,
            reply_marker: p.reply_marker,
            headless: p.headless,
        }
    }
}

impl From<et_proto::agents::AgentOnMachine> for AgentOnMachine {
    fn from(a: et_proto::agents::AgentOnMachine) -> Self {
        AgentOnMachine {
            id: a.id,
            version: a.version,
            path: a.path,
            transcript: a.transcript,
            state: a.state,
            alerts: a.alerts,
            setup_hint: a.setup_hint,
        }
    }
}

/// The built-in profiles: Claude Code, Codex, Gemini CLI, Grok Build.
#[uniffi::export]
pub fn agent_profiles() -> Vec<AgentProfile> {
    et_proto::agents::builtin().into_iter().map(Into::into).collect()
}

#[uniffi::export(async_runtime = "tokio")]
impl RemoteHost {
    /// The agents this machine has, one per built-in profile.
    pub async fn agents(&self) -> Result<Vec<AgentOnMachine>, LinkError> {
        match self.call(Request::Agents).await? {
            Response::Agents { json } => {
                let info: et_proto::agents::AgentsInfo =
                    serde_json::from_str(&json).map_err(|e| LinkError::Protocol { message: format!("agents: {e}") })?;
                Ok(info.agents.into_iter().map(Into::into).collect())
            }
            other => Err(unexpected(other)),
        }
    }
}
