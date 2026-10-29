//! `Agents` (et-ios docs/PLAN-multi-agent.md §4.1): which AI agent CLIs this
//! machine has, their versions, and what etd does with each.
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use et_proto::agents::{builtin, version_in, AgentOnMachine, AgentProfile, AgentsInfo};
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::tmux::Env;

const VERSION_TIMEOUT: Duration = Duration::from_secs(5);
/// Installs change seldom; `--version` of a node CLI takes a second.
const KEEP: Duration = Duration::from_secs(60);

/// The answer, from the last 60 s if there is one.
pub async fn info(env: &Env) -> AgentsInfo {
    static LAST: OnceLock<Mutex<Option<(Instant, AgentsInfo)>>> = OnceLock::new();
    let mut last = LAST.get_or_init(|| Mutex::new(None)).lock().await;
    if let Some((_, info)) = last.as_ref().filter(|(at, _)| at.elapsed() < KEEP) {
        return info.clone();
    }
    let info = look(env).await;
    *last = Some((Instant::now(), info.clone()));
    info
}

/// Asks each built-in agent now, all at once; in the built-in order.
pub async fn look(env: &Env) -> AgentsInfo {
    let mut tasks = tokio::task::JoinSet::new();
    for (i, profile) in builtin().into_iter().enumerate() {
        let env = env.clone();
        tasks.spawn(async move { (i, on_machine(&profile, &env).await) });
    }
    let mut agents = tasks.join_all().await;
    agents.sort_by_key(|(i, _)| *i);
    AgentsInfo { agents: agents.into_iter().map(|(_, agent)| agent).collect() }
}

async fn on_machine(profile: &AgentProfile, env: &Env) -> AgentOnMachine {
    let mut agent = AgentOnMachine { id: profile.id.clone(), state: "none".into(), ..Default::default() };
    let Some(path) = profile.launch.first().and_then(|program| find(program, env)) else {
        return agent;
    };
    agent.version = version(&path, &profile.version_args, env).await.or_else(|| Some(String::new()));
    agent.path = Some(path.display().to_string());
    // The Logbook reads Claude only (PLAN-multi-agent M2 adds the others).
    if profile.id == "claude" {
        agent.transcript = true;
        agent.state = "own".into();
        agent.alerts = true;
    } else {
        agent.setup_hint = Some(format!("{}: transcript, live state and alerts come in a later etd", profile.name));
    }
    agent
}

/// `program` in the login `PATH`.
fn find(program: &str, env: &Env) -> Option<PathBuf> {
    let path = env.get("PATH")?;
    std::env::split_paths(path).map(|dir| dir.join(program)).find(|p| executable(p))
}

fn executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    path.metadata().map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0).unwrap_or(false)
}

/// The version it prints; None if it prints none in time.
async fn version(path: &Path, args: &[String], env: &Env) -> Option<String> {
    let run = Command::new(path).args(args).env_clear().envs(env).stdin(Stdio::null()).stderr(Stdio::null()).kill_on_drop(true).output();
    let output = tokio::time::timeout(VERSION_TIMEOUT, run).await.ok()?.ok()?;
    version_in(&String::from_utf8_lossy(&output.stdout))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    fn script(dir: &Path, name: &str, body: &str) {
        let path = dir.join(name);
        fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[tokio::test]
    async fn finds_agents_on_the_path_with_their_versions() {
        let bin = tempfile::tempdir().unwrap();
        script(bin.path(), "claude", "echo '2.1.4 (Claude Code)'");
        script(bin.path(), "codex", "echo 'codex-cli 0.166.0'");
        // Prints no version: installed, version unknown.
        script(bin.path(), "grok", "echo hello");
        // Not executable: not installed.
        fs::write(bin.path().join("gemini"), "x").unwrap();
        let env: Env = [("PATH".to_string(), format!("/nonexistent:{}", bin.path().display()))].into();

        let info = look(&env).await;
        let by_id = |id: &str| info.agents.iter().find(|a| a.id == id).unwrap().clone();
        assert_eq!(info.agents.len(), 5);
        let claude = by_id("claude");
        assert_eq!(
            (claude.version.as_deref(), claude.transcript, claude.state.as_str(), claude.alerts),
            (Some("2.1.4"), true, "own", true)
        );
        assert_eq!(claude.path, Some(bin.path().join("claude").display().to_string()));
        let codex = by_id("codex");
        assert_eq!((codex.version.as_deref(), codex.transcript, codex.state.as_str()), (Some("0.166.0"), false, "none"));
        assert!(codex.setup_hint.is_some());
        assert_eq!(by_id("grok").version.as_deref(), Some(""));
        assert_eq!(by_id("gemini").version, None);
    }
}
