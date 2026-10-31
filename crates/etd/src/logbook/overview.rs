//! `LogOverview` (et-ios docs/PLAN-logbook-v2.md §4.5, package A1): for each
//! transcript on the archive, a headline, the last prompt, the first line of
//! the last reply, and counts, so the apps show what a session does without
//! a model.
//!
//! W0: the request is on the wire; this answers "not built yet" until A1.
use et_proto::Response;

use crate::config::Paths;
use crate::server::State;

/// `LogOverview { since }`: `Overview { json }`.
pub async fn serve(_state: &State, _since: u64) -> Response {
    Response::Error("not built yet: LogOverview (PLAN-logbook-v2 A1)".into())
}

/// `etd logbook overview [--json]`.
pub async fn cli(_paths: &Paths, _json: bool) -> anyhow::Result<()> {
    anyhow::bail!("not built yet: etd logbook overview (PLAN-logbook-v2 A1)")
}
