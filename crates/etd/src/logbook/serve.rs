//! The Logbook's reading side, for the apps (PLAN-logbook §4.2): which
//! machines are archives, the list of transcripts, a transcript followed
//! live, "do not record", and sending a part of a transcript or of a report
//! into a session (reports: watcher.rs).
use std::time::Duration;

use et_proto::logbook::LogSelection;
use et_proto::{write_frame, Response};
use iroh::endpoint::{RecvStream, SendStream};
use tracing::{debug, info};

use super::capture::Net;
use super::{send, settings, store};
use crate::config;
use crate::server::State;

/// Bytes in one `LogChunk` (a longer line comes whole).
const CHUNK: usize = 1024 * 1024;
/// A follower looks at the file at least this often, even if no append
/// woke it.
const LOOK_EVERY: Duration = Duration::from_secs(5);

fn not_archive() -> Response {
    Response::Error("not a Logbook archive (etd logbook archive on)".into())
}

/// `LogArchives`: from the capture's last look, else asked now.
pub async fn archive_names(state: &State) -> Response {
    if let Some(names) = state.logbook().fresh_names() {
        return Response::Machines(names);
    }
    let me = crate::network::load(state.paths())
        .ok()
        .flatten()
        .and_then(|s| s.network.member(&state.endpoint().id()).map(|m| m.name.to_string()))
        .unwrap_or_default();
    let net = Net { endpoint: state.endpoint().clone(), connections: state.connections().clone(), me };
    let (archives, _) = super::archives(state.paths(), state.logbook(), &net).await;
    let names: Vec<String> = archives.iter().map(|a| a.name(&net.me).to_string()).collect();
    state.logbook().set_names(names.clone());
    Response::Machines(names)
}

/// `LogSessions`.
pub async fn sessions(state: &State, since: u64) -> Response {
    if !settings(state.paths()).archive {
        return not_archive();
    }
    match state.logbook().get(state.paths()).await {
        Ok(store) => Response::LogList(store.lock().await.list(since)),
        Err(e) => Response::Error(format!("{e:#}")),
    }
}

/// `LogPause`.
pub async fn pause(state: &State, machine: &str, mm_session: &str, paused: bool) -> Response {
    if !settings(state.paths()).archive {
        return not_archive();
    }
    let set = async { state.logbook().get(state.paths()).await?.lock().await.set_paused(machine, mm_session, paused, config::now_secs()) };
    match set.await {
        Ok(()) => {
            info!("logbook: {machine}/{mm_session} {}", if paused { "not recorded" } else { "recorded" });
            Response::Pong
        }
        Err(e) => Response::Error(format!("{e:#}")),
    }
}

/// `LogRead`: `LogChunk` frames on `send` until the asker finishes `recv`.
pub async fn follow(
    state: &State,
    machine: &str,
    session_id: &str,
    from: u64,
    mut send: SendStream,
    mut recv: RecvStream,
) -> anyhow::Result<()> {
    if !settings(state.paths()).archive {
        write_frame(&mut send, &not_archive()).await?;
        return Ok(send.finish()?);
    }
    let found = async {
        let store = state.logbook().get(state.paths()).await?;
        let store = store.lock().await;
        let entry = store.entry(machine, session_id).ok_or_else(|| anyhow::anyhow!("no transcript {machine}/{session_id} here"))?;
        anyhow::Ok((store.path(machine, session_id), entry.bytes))
    };
    let (path, stored) = match found.await {
        Ok(found) => found,
        Err(e) => {
            write_frame(&mut send, &Response::Error(format!("{e:#}"))).await?;
            return Ok(send.finish()?);
        }
    };
    let mut offset = from.min(stored);
    let mut sent_live = false;
    let mut asker_done = [0u8; 64];
    loop {
        // Made before the look, so an append in between still wakes us.
        let appended = state.logbook().appended.notified();
        let (start, bytes) = store::read_lines(&path, offset, CHUNK)?;
        offset = start + bytes.len() as u64;
        if !bytes.is_empty() || !sent_live {
            // An empty chunk only when the stored bytes ended at a chunk's
            // end: the asker then knows it has caught up.
            let live = bytes.is_empty() || offset >= stored;
            sent_live |= live;
            if write_frame(&mut send, &Response::LogChunk { offset, bytes, live }).await.is_err() {
                break;
            }
            continue;
        }
        tokio::select! {
            _ = appended => {}
            _ = tokio::time::sleep(LOOK_EVERY) => {}
            // The asker finished its side (or the stream broke): stop.
            read = recv.read(&mut asker_done) => {
                if !matches!(read, Ok(Some(_))) {
                    break;
                }
            }
        }
    }
    debug!("logbook: follow of {machine}/{session_id} ended at {offset}");
    let _ = send.finish();
    Ok(())
}

/// `LogSend`: the selection as a Markdown file, then where it goes.
pub async fn markdown(state: &State, selection: &LogSelection) -> anyhow::Result<(String, String)> {
    if !settings(state.paths()).archive {
        anyhow::bail!("not a Logbook archive (etd logbook archive on)");
    }
    match selection {
        LogSelection::Turns { machine, session_id, turn_ids } => {
            let (path, entry) = {
                let store = state.logbook().get(state.paths()).await?;
                let store = store.lock().await;
                let entry = store
                    .entry(machine, session_id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("no transcript {machine}/{session_id} here"))?;
                (store.path(machine, session_id), entry)
            };
            let text = tokio::fs::read(&path).await?;
            let doc = send::markdown(&entry, &text, turn_ids)?;
            let short: String = session_id.chars().take(8).collect();
            Ok((format!("logbook-{machine}-{short}-{}.md", config::now_secs()), doc))
        }
        LogSelection::ReportSection { report_id, project_id } => {
            let entries: Vec<store::Entry> = state.logbook().get(state.paths()).await?.lock().await.entries().cloned().collect();
            super::watcher::section(&state.paths().logbook(), &entries, report_id, project_id)
        }
    }
}
