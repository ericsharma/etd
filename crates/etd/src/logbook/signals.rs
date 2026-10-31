//! Friction signals (et-ios docs/PLAN-logbook-v2.md §4.3, package A2): the
//! places in each transcript where the owner interrupted, denied a tool or
//! corrected the agent, and where tools failed. Found with no model.
//!
//! W0: the request is on the wire; this answers "not built yet" until A2.
use et_proto::Response;

use crate::config::Paths;
use crate::server::State;

/// `LogSignals { since }`: `Signals { json }`.
pub async fn serve(_state: &State, _since: u64) -> Response {
    Response::Error("not built yet: LogSignals (PLAN-logbook-v2 A2)".into())
}

/// `etd meta signals`.
pub async fn cli(_paths: &Paths) -> anyhow::Result<()> {
    anyhow::bail!("not built yet: etd meta signals (PLAN-logbook-v2 A2)")
}
