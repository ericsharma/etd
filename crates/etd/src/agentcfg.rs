//! Agent instruction files (et-ios docs/PLAN-logbook-v2.md D9, package A3):
//! `AgentConfig` reads this machine's allow-listed files (`CLAUDE.md`,
//! memory, skills, each agent profile's instruction file); `ConfigWrite`
//! replaces one, with a base hash check and a backup. On the archive,
//! `etd meta drift` compares all machines.
//!
//! W0: the requests are on the wire; they answer "not built yet" until A3.
use et_proto::Response;

use crate::config::Paths;
use crate::tmux::Env;

/// `AgentConfig`: `AgentConfig { json }`.
pub async fn info(_env: &Env) -> Response {
    Response::Error("not built yet: AgentConfig (PLAN-logbook-v2 A3)".into())
}

/// `ConfigWrite`: `ConfigWritten { backup }`.
pub async fn write(_env: &Env, _paths: &Paths, _path: &str, _base: &str, _content: &str) -> Response {
    Response::Error("not built yet: ConfigWrite (PLAN-logbook-v2 A3)".into())
}

/// `etd meta drift`.
pub async fn drift_cli(_paths: &Paths) -> anyhow::Result<()> {
    anyhow::bail!("not built yet: etd meta drift (PLAN-logbook-v2 A3)")
}
