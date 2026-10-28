//! The Logbook for the apps (et-ios docs/PLAN-logbook.md §4.5): the
//! transcripts on an archive machine, followed live, "do not record", the
//! report, and "Send to session". Call these on a `RemoteHost` connected to
//! an archive (`NetworkInfo.archive`, first name).
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use et_proto::{read_frame, write_frame, Request, Response};
use iroh::endpoint::{RecvStream, SendStream};
use tokio::sync::Mutex;

use crate::{closed, no_answer, stream_error, unexpected, Delivered, LinkError, RemoteHost, SendProgress};

/// A transcript: its machine and the agent's session id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, uniffi::Record)]
pub struct LogKey {
    pub machine: String,
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, uniffi::Record)]
pub struct LogEntry {
    pub key: LogKey,
    /// The manymux session name on that machine.
    pub mm_session: String,
    pub cwd: String,
    /// The git remote URL, else the repo root path.
    pub repo: Option<String>,
    pub branch: Option<String>,
    /// "busy" | "idle" | "gone"
    pub status: String,
    /// Unix seconds.
    pub first: u64,
    pub last: u64,
    /// The size of the archive's copy; `logbook_follow` offsets are in it.
    pub bytes: u64,
    /// The session id before /clear or --resume.
    pub previous: Option<String>,
    /// Do not record.
    pub paused: bool,
    /// Claude's own title for the session (its ai-title lines).
    pub title: Option<String>,
}

/// Whole JSONL lines only. `offset`: the byte offset after the last line
/// (the first chunk's start is `offset` minus its bytes: a follow from
/// inside a line starts at the next one). `live`: the stored bytes are all
/// sent (true on the chunk that reaches their end, or on an empty chunk).
#[derive(Debug, Clone, PartialEq, Eq, Hash, uniffi::Record)]
pub struct LogChunk {
    pub offset: u64,
    pub lines: Vec<String>,
    pub live: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, uniffi::Enum)]
pub enum LogSelection {
    /// Claude entry uuids of the prompts.
    Turns {
        key: LogKey,
        turn_ids: Vec<String>,
    },
    ReportSection {
        report_id: String,
        project_id: String,
    },
}

/// Progress of `report(refresh: true)`.
#[uniffi::export(with_foreign)]
pub trait ReportProgress: Send + Sync {
    fn on_progress(&self, stage: String, done: u32, total: u32);
}

impl From<et_proto::logbook::LogEntry> for LogEntry {
    fn from(e: et_proto::logbook::LogEntry) -> Self {
        LogEntry {
            key: LogKey { machine: e.machine, session_id: e.session_id },
            mm_session: e.meta.mm_session,
            cwd: e.meta.cwd,
            repo: e.meta.repo,
            branch: e.meta.branch,
            status: e.meta.status,
            first: e.first,
            last: e.last,
            bytes: e.bytes,
            previous: e.meta.previous,
            paused: e.paused,
            title: e.title,
        }
    }
}

/// The lines of a chunk's bytes (each ends in a newline).
fn lines(bytes: &[u8]) -> Vec<String> {
    let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    if bytes.is_empty() {
        return Vec::new();
    }
    bytes.split(|&b| b == b'\n').map(|l| String::from_utf8_lossy(l).into_owned()).collect()
}

/// A followed transcript: the stored bytes first, then live.
#[derive(uniffi::Object)]
pub struct LogStream {
    send: std::sync::Mutex<Option<SendStream>>,
    recv: Mutex<RecvStream>,
    closed: AtomicBool,
}

#[uniffi::export(async_runtime = "tokio")]
impl LogStream {
    /// The next chunk. Waits for new lines once the stored ones are sent.
    pub async fn next(&self) -> Result<LogChunk, LinkError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(LinkError::Closed { message: "log stream closed".into() });
        }
        let mut recv = self.recv.lock().await;
        match read_frame::<Response>(&mut recv).await.map_err(stream_error)? {
            Some(Response::LogChunk { offset, bytes, live }) => Ok(LogChunk { offset, lines: lines(&bytes), live }),
            Some(Response::Error(message)) => Err(LinkError::Remote { message }),
            Some(other) => Err(unexpected(other)),
            None => Err(LinkError::Closed { message: "log stream ended".into() }),
        }
    }

    /// Stops the follow: the machine ends the stream; `next` then fails.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        if let Some(mut send) = self.send.lock().unwrap().take() {
            let _ = send.finish();
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl RemoteHost {
    /// The transcripts that changed at or after `since` (Unix seconds; 0 = all).
    pub async fn logbook_sessions(&self, since: u64) -> Result<Vec<LogEntry>, LinkError> {
        match self.call(Request::LogSessions { since }).await? {
            Response::LogList(list) => Ok(list.into_iter().map(LogEntry::from).collect()),
            Response::Error(message) => Err(LinkError::Remote { message }),
            other => Err(unexpected(other)),
        }
    }

    /// Follows a transcript from byte `from` of the archive's copy.
    pub async fn logbook_follow(&self, key: LogKey, from: u64) -> Result<Arc<LogStream>, LinkError> {
        let (mut send, recv) = self.conn.open_bi().await.map_err(|e| closed(&e))?;
        let request = Request::LogRead { machine: key.machine, session_id: key.session_id, from };
        write_frame(&mut send, &request).await.map_err(stream_error)?;
        Ok(Arc::new(LogStream { send: std::sync::Mutex::new(Some(send)), recv: Mutex::new(recv), closed: AtomicBool::new(false) }))
    }

    /// Records a manymux session of `machine`, or not ("do not record").
    pub async fn logbook_pause(&self, machine: String, mm_session: String, paused: bool) -> Result<(), LinkError> {
        match self.call(Request::LogPause { machine, mm_session, paused }).await? {
            Response::Pong => Ok(()),
            Response::Error(message) => Err(LinkError::Remote { message }),
            other => Err(unexpected(other)),
        }
    }

    /// The §4.4 JSON plus "id" and "taken": the last report, or with
    /// `refresh` a new one ("id": null before the first).
    pub async fn report(&self, refresh: bool, progress: Arc<dyn ReportProgress>) -> Result<String, LinkError> {
        let (mut send, mut recv) = self.conn.open_bi().await.map_err(|e| closed(&e))?;
        write_frame(&mut send, &Request::Report { refresh }).await.map_err(stream_error)?;
        send.finish().map_err(|e| LinkError::Closed { message: e.to_string() })?;
        loop {
            match read_frame::<Response>(&mut recv).await.map_err(stream_error)? {
                Some(Response::ReportProgress { stage, done, total }) => progress.on_progress(stage, done, total),
                Some(Response::Report { json }) => return Ok(json),
                Some(Response::Error(message)) => return Err(LinkError::Remote { message }),
                Some(other) => return Err(unexpected(other)),
                None => return Err(no_answer()),
            }
        }
    }

    /// One §4.4 proposal, accepted or edited (JSON).
    pub async fn grouping(&self, change_json: String) -> Result<(), LinkError> {
        match self.call(Request::Grouping { change_json }).await? {
            Response::Pong => Ok(()),
            Response::Error(message) => Err(LinkError::Remote { message }),
            other => Err(unexpected(other)),
        }
    }

    /// The selection as a Markdown file in `~/etd-downloads/` on the machine
    /// `to`, its path typed into `session` there (empty: not typed).
    pub async fn logbook_send(
        &self,
        selection: LogSelection,
        to: String,
        session: String,
        progress: Arc<dyn SendProgress>,
    ) -> Result<Delivered, LinkError> {
        let selection = match selection {
            LogSelection::Turns { key, turn_ids } => {
                et_proto::logbook::LogSelection::Turns { machine: key.machine, session_id: key.session_id, turn_ids }
            }
            LogSelection::ReportSection { report_id, project_id } => {
                et_proto::logbook::LogSelection::ReportSection { report_id, project_id }
            }
        };
        let (mut up, mut down) = self.conn.open_bi().await.map_err(|e| closed(&e))?;
        write_frame(&mut up, &Request::LogSend { selection, to, session }).await.map_err(stream_error)?;
        up.finish().map_err(|e| LinkError::Closed { message: e.to_string() })?;
        loop {
            match read_frame::<Response>(&mut down).await.map_err(stream_error)? {
                Some(Response::Progress { done, total }) => progress.on_progress(done, total),
                Some(Response::Delivered { paths, typed }) => return Ok(Delivered { paths, typed }),
                Some(Response::Error(message)) => return Err(LinkError::Remote { message }),
                Some(other) => return Err(unexpected(other)),
                None => return Err(no_answer()),
            }
        }
    }
}

impl RemoteHost {
    /// The archives this machine knows of; empty when it does not answer
    /// soon (an older etd, say).
    pub(crate) async fn archive_names(&self) -> Vec<String> {
        let asked = tokio::time::timeout(std::time::Duration::from_secs(3), self.call(Request::LogArchives)).await;
        match asked {
            Ok(Ok(Response::Machines(names))) => names,
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_bytes_to_lines() {
        assert_eq!(lines(b"{\"a\":1}\n{\"b\":2}\n"), ["{\"a\":1}", "{\"b\":2}"]);
        assert!(lines(b"").is_empty());
        assert!(lines(b"\n").is_empty(), "a blank line carries nothing");
    }
}
