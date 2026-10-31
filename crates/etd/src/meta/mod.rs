//! The meta agent (et-ios docs/PLAN-logbook-v2.md §4.3, packages A4 + A5):
//! on the archive, a headless agent with no tools reads the friction
//! signals, the digests and every machine's agent instruction files, and
//! proposes changes to them. The owner decides each one (`MetaDecision`).
//!
//! W0: the requests are on the wire; they answer "not built yet" until A4.
use et_proto::{write_frame, Response};
use iroh::endpoint::SendStream;

use crate::config::Paths;
use crate::server::State;

fn not_built(what: &str) -> String {
    format!("not built yet: {what} (PLAN-logbook-v2 A4)")
}

/// `Meta { refresh }`: the last findings, or a new run with
/// `ReportProgress` frames on the way, then `Meta { json }`.
pub async fn serve(_state: &State, _refresh: bool, mut send: SendStream) -> anyhow::Result<()> {
    write_frame(&mut send, &Response::Error(not_built("Meta"))).await?;
    send.finish()?;
    Ok(())
}

/// `MetaDecision`: `Pong`.
pub async fn decision(_state: &State, _json: &str) -> Response {
    Response::Error(not_built("MetaDecision"))
}

/// `LogSend` of `Finding`: the file name and the Markdown.
pub fn finding(_paths: &Paths, _run_id: &str, _finding_id: &str) -> anyhow::Result<(String, String)> {
    anyhow::bail!(not_built("Send of a finding"))
}

/// `etd meta run [--refresh]`.
pub async fn cli(_paths: &Paths, _refresh: bool) -> anyhow::Result<()> {
    anyhow::bail!(not_built("etd meta run"))
}

/// `etd meta schedule on|off` (A5).
pub async fn schedule_cli(_paths: &Paths, _on: bool) -> anyhow::Result<()> {
    anyhow::bail!("not built yet: etd meta schedule (PLAN-logbook-v2 A5)")
}
