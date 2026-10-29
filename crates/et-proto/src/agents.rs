//! The terminal AI agents E.T. knows (et-ios docs/PLAN-multi-agent.md §4.1):
//! the built-in profiles, and what a machine answers to `Agents`.
//!
//! The answer goes as JSON (`Response::Agents { json }`), as the Logbook
//! report does: a field added later does not break an older app or etd.
use serde::{Deserialize, Serialize};

/// What E.T. and etd know about one agent CLI.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AgentProfile {
    /// `claude`, `codex`, `gemini`, `grok`: also `LogMeta::agent`.
    pub id: String,
    pub name: String,
    /// One letter, shown on the accent color (D8: no vendor logos).
    pub mark: String,
    /// A Catppuccin Mocha color name.
    pub accent: String,
    /// The command that starts it in a new session.
    pub launch: Vec<String>,
    /// Program names that mean "this agent runs" (the executable's file
    /// name, or for a script the file name of its first argument).
    pub processes: Vec<String>,
    pub version_args: Vec<String>,
    /// The key wheel set.
    pub keys: Vec<String>,
    /// How multi-line text goes in: `bracketed-paste` or `line-by-line`.
    pub multiline: String,
    /// The file mention prefix: dictated "at <file>" becomes `@path`.
    pub mention: Option<String>,
    /// What starts the agent's reply on screen (Speaker, when there is no
    /// transcript).
    pub reply_marker: Option<String>,
    /// The command for one prompt with no UI (the Logbook watcher).
    pub headless: Vec<String>,
}

/// One agent on one machine.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AgentOnMachine {
    pub id: String,
    /// None: not installed.
    pub version: Option<String>,
    /// Where the launch command is.
    pub path: Option<String>,
    /// etd reads its transcripts (a Logbook reader exists).
    pub transcript: bool,
    /// Where its live state comes from: `own` (its state file), `hooks`,
    /// `transcript`, or `none`.
    pub state: String,
    /// "Needs you" reaches etd.
    pub alerts: bool,
    /// What to do for a missing part, if anything.
    pub setup_hint: Option<String>,
}

/// The answer to `Agents`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AgentsInfo {
    /// One per built-in profile, installed or not.
    pub agents: Vec<AgentOnMachine>,
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

/// The built-in profiles (D1). Codex, Gemini and Grok keys, mentions and
/// headless commands are not checked yet (M0).
pub fn builtin() -> Vec<AgentProfile> {
    let claude_keys = ["esc", "⇧tab", "1", "2", "3", "↑", "↓", "enter", "^C"];
    vec![
        AgentProfile {
            id: "claude".into(),
            name: "Claude Code".into(),
            mark: "C".into(),
            accent: "peach".into(),
            launch: strings(&["claude"]),
            processes: strings(&["claude"]),
            version_args: strings(&["--version"]),
            keys: strings(&claude_keys),
            multiline: "bracketed-paste".into(),
            mention: Some("@".into()),
            reply_marker: Some("⏺".into()),
            headless: strings(&["claude", "-p", "--output-format", "json"]),
        },
        AgentProfile {
            id: "codex".into(),
            name: "Codex".into(),
            mark: "X".into(),
            accent: "green".into(),
            launch: strings(&["codex"]),
            processes: strings(&["codex"]),
            version_args: strings(&["--version"]),
            keys: strings(&claude_keys),
            multiline: "bracketed-paste".into(),
            mention: Some("@".into()),
            reply_marker: None,
            headless: strings(&["codex", "exec"]),
        },
        AgentProfile {
            id: "gemini".into(),
            name: "Gemini CLI".into(),
            mark: "G".into(),
            accent: "blue".into(),
            launch: strings(&["gemini"]),
            // A node script: its process is `node …/gemini`.
            processes: strings(&["gemini"]),
            version_args: strings(&["--version"]),
            keys: strings(&claude_keys),
            multiline: "bracketed-paste".into(),
            mention: Some("@".into()),
            reply_marker: None,
            headless: strings(&["gemini", "--output-format", "json", "-p"]),
        },
        AgentProfile {
            id: "grok".into(),
            name: "Grok Build".into(),
            mark: "K".into(),
            accent: "mauve".into(),
            launch: strings(&["grok"]),
            processes: strings(&["grok"]),
            version_args: strings(&["--version"]),
            keys: strings(&claude_keys),
            multiline: "bracketed-paste".into(),
            mention: Some("@".into()),
            reply_marker: None,
            headless: strings(&["grok", "-p"]),
        },
    ]
}

/// The version in a `--version` line: the first word that starts with a
/// digit and has a dot ("2.1.4 (Claude Code)", "codex-cli 0.166.0",
/// "grok 0.2.11 (3247ce8f8)").
pub fn version_in(output: &str) -> Option<String> {
    output
        .split_whitespace()
        .map(|w| w.trim_start_matches('v').trim_end_matches([',', ';', ')']))
        .find(|w| w.starts_with(|c: char| c.is_ascii_digit()) && w.contains('.'))
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_ids_are_unique_and_complete() {
        let all = builtin();
        let ids: Vec<&str> = all.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(ids, ["claude", "codex", "gemini", "grok"]);
        for p in &all {
            assert_eq!(p.mark.chars().count(), 1, "{}", p.id);
            assert!(!p.launch.is_empty() && !p.processes.is_empty() && !p.keys.is_empty(), "{}", p.id);
        }
    }

    #[test]
    fn versions_come_out_of_each_agents_line() {
        assert_eq!(version_in("2.1.4 (Claude Code)").as_deref(), Some("2.1.4"));
        assert_eq!(version_in("codex-cli 0.166.0").as_deref(), Some("0.166.0"));
        assert_eq!(version_in("0.66.0\n").as_deref(), Some("0.66.0"));
        assert_eq!(version_in("grok 0.2.11 (3247ce8f8)").as_deref(), Some("0.2.11"));
        assert_eq!(version_in("v1.2.3").as_deref(), Some("1.2.3"));
        assert_eq!(version_in("no version here"), None);
    }

    #[test]
    fn unknown_fields_and_missing_fields_decode() {
        let info: AgentsInfo = serde_json::from_str(r#"{"agents":[{"id":"codex","later":1}],"more":true}"#).unwrap();
        assert_eq!(info.agents[0].id, "codex");
        assert_eq!(info.agents[0].version, None);
    }
}
