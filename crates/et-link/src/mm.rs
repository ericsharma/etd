//! manymux through etd: sessions on every machine, for Swift.
//!
//! Each call opens an etd `Agent` stream, which is `mm agent` on the hub or
//! `ssh <machine> mm agent` from it. A manymux node answers one request per
//! stream, except `Attach` (a live session) and `Events` (a feed).
//!
//! On the way down, etd wraps the agent's bytes in `ControlFrame`s; a task
//! unwraps them into a pipe that manymux's own frame reader reads. On the way
//! up, manymux frames go straight onto the iroh stream.
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use et_proto::{read_frame, write_frame, ControlFrame, Request, Response};
use iroh::endpoint::SendStream;
use manymux::proto::{self as mp, tag, FrameReader};
use serde::Serialize;
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio::sync::Mutex;

use crate::{closed, stream_error, unexpected, ExecResult, LinkError, RemoteHost};

/// Bytes kept in the pipe between the iroh stream and the frame reader.
const PIPE: usize = 256 * 1024;

fn protocol(e: anyhow::Error) -> LinkError {
    LinkError::Protocol {
        message: format!("{e:#}"),
    }
}

fn decode<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, LinkError> {
    mp::decode(body).map_err(protocol)
}

/// One `Agent` stream, as manymux frames.
struct Agent {
    send: Mutex<Option<SendStream>>,
    reader: Mutex<FrameReader<DuplexStream>>,
    /// The agent's stderr (for example an ssh error), for when it ends early.
    stderr: Arc<std::sync::Mutex<Vec<String>>>,
}

impl Agent {
    async fn open(host: &RemoteHost, machine: Option<String>) -> Result<Self, LinkError> {
        let (mut send, mut recv) = host.conn.open_bi().await.map_err(|e| closed(&e))?;
        write_frame(&mut send, &Request::Agent { host: machine })
            .await
            .map_err(stream_error)?;
        match read_frame::<Response>(&mut recv)
            .await
            .map_err(stream_error)?
        {
            Some(Response::AgentStarted) => {}
            Some(other) => return Err(unexpected(other)),
            None => {
                return Err(LinkError::Protocol {
                    message: "no answer to Agent".into(),
                })
            }
        }
        let (mut pipe, reader) = tokio::io::duplex(PIPE);
        let stderr = Arc::new(std::sync::Mutex::new(Vec::new()));
        let lines = stderr.clone();
        tokio::spawn(async move {
            // Ends on Exit, a closed stream, or a reader that went away;
            // dropping `pipe` is the end of input for the frame reader.
            while let Ok(Some(frame)) = read_frame::<ControlFrame>(&mut recv).await {
                match frame {
                    ControlFrame::Stdout(bytes) => {
                        if pipe.write_all(&bytes).await.is_err() {
                            break;
                        }
                    }
                    ControlFrame::Stderr(line) => {
                        let mut lines = lines.lock().unwrap();
                        lines.push(line);
                        let excess = lines.len().saturating_sub(20);
                        lines.drain(..excess);
                    }
                    ControlFrame::Exit(_) => break,
                }
            }
        });
        Ok(Self {
            send: Mutex::new(Some(send)),
            reader: Mutex::new(FrameReader::new(reader)),
            stderr,
        })
    }

    async fn frame(&self, tag: u8, body: &[u8]) -> Result<(), LinkError> {
        let mut send = self.send.lock().await;
        let send = send.as_mut().ok_or_else(|| LinkError::Closed {
            message: "already detached".into(),
        })?;
        mp::write_frame(send, tag, body).await.map_err(stream_error)
    }

    async fn msg<T: Serialize>(&self, tag: u8, msg: &T) -> Result<(), LinkError> {
        self.frame(tag, &mp::encode(msg).map_err(protocol)?).await
    }

    /// The next frame. `None`: the agent ended cleanly.
    async fn next(&self) -> Result<Option<mp::Frame>, LinkError> {
        let next = self.reader.lock().await.next().await;
        match next {
            Ok(Some(frame)) => Ok(Some(frame)),
            Ok(None) => {
                let stderr = self.stderr.lock().unwrap().join("\n");
                if stderr.trim().is_empty() {
                    Ok(None)
                } else {
                    Err(LinkError::Remote { message: stderr })
                }
            }
            Err(e) => Err(protocol(e)),
        }
    }

    async fn finish(&self) {
        if let Some(mut send) = self.send.lock().await.take() {
            let _ = send.finish();
        }
    }

    /// Sends `request` and returns the one answer (`Error` as `Remote`).
    async fn request(&self, request: &mp::Request) -> Result<mp::Response, LinkError> {
        self.msg(tag::REQUEST, request).await?;
        loop {
            match self.next().await? {
                Some(frame) if frame.tag == tag::RESPONSE => {
                    return match mp::decode(&frame.body).map_err(protocol)? {
                        mp::Response::Error(message) => Err(LinkError::Remote { message }),
                        response => Ok(response),
                    };
                }
                Some(_) => continue,
                None => {
                    return Err(LinkError::Closed {
                        message: "mm agent ended without an answer".into(),
                    })
                }
            }
        }
    }
}

/// One request on its own stream, as the node expects.
async fn one(
    host: &RemoteHost,
    machine: Option<String>,
    request: mp::Request,
) -> Result<mp::Response, LinkError> {
    let agent = Agent::open(host, machine).await?;
    let response = agent.request(&request).await;
    agent.finish().await;
    response
}

fn wrong(response: mp::Response) -> LinkError {
    LinkError::Protocol {
        message: format!("unexpected manymux answer: {response:?}"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct MmSession {
    /// The name in `machine/name`; the user can change it.
    pub name: String,
    /// What the program says it is doing (its terminal title).
    pub title: String,
    pub command: String,
    pub pid: u32,
    pub cols: u16,
    pub rows: u16,
    pub attached: u32,
    /// Seconds since the last output.
    pub idle_secs: u64,
    /// Bells since somebody last looked.
    pub bells: u64,
    /// Unix seconds.
    pub started: u64,
}

impl From<mp::SessionInfo> for MmSession {
    fn from(s: mp::SessionInfo) -> Self {
        Self {
            name: s.name,
            title: s.title,
            command: s.command,
            pid: s.pid,
            cols: s.size.cols,
            rows: s.size.rows,
            attached: s.attached as u32,
            idle_secs: s.idle,
            bells: s.bells,
            started: s
                .started
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct MmSpawn {
    /// None: named after the command.
    pub name: Option<String>,
    /// Empty: the user's login shell.
    pub command: Vec<String>,
    /// None: the home directory.
    pub cwd: Option<String>,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct MmPeek {
    pub name: String,
    pub cols: u16,
    pub rows: u16,
    /// Terminal bytes that paint the screen; feed them to a terminal of
    /// `cols` x `rows`.
    pub screen: String,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum MmEventKind {
    Bell,
    Notify { title: String, body: String },
    TitleChanged { title: String },
    Exited { code: i32 },
    Started,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct MmEvent {
    /// The machine, as the hub's node names it (its host name for itself).
    pub machine: String,
    pub session: String,
    pub title: String,
    pub kind: MmEventKind,
    /// Clients attached to that session when it happened; 0 = nobody saw it.
    pub attached: u32,
}

impl From<mp::HostedEvent> for MmEvent {
    fn from(e: mp::HostedEvent) -> Self {
        let kind = match e.event.kind {
            mp::EventKind::Bell => MmEventKind::Bell,
            mp::EventKind::Notify { title, body } => MmEventKind::Notify { title, body },
            mp::EventKind::TitleChanged(title) => MmEventKind::TitleChanged { title },
            mp::EventKind::Exited(code) => MmEventKind::Exited { code },
            mp::EventKind::Started => MmEventKind::Started,
        };
        Self {
            machine: e.host,
            session: e.event.session,
            title: e.event.title,
            kind,
            attached: e.event.attached as u32,
        }
    }
}

/// `machine`: None is the hub (the machine etd runs on), else a name from
/// `machines()`.
#[uniffi::export(async_runtime = "tokio")]
impl RemoteHost {
    /// The machines the hub's manymux node watches (not the hub itself).
    pub async fn machines(&self) -> Result<Vec<String>, LinkError> {
        match self.call(Request::Machines).await? {
            Response::Machines(list) => Ok(list),
            other => Err(unexpected(other)),
        }
    }

    /// `exec` on another machine, over ssh from the hub.
    pub async fn exec_on(&self, machine: String, script: String) -> Result<ExecResult, LinkError> {
        match self
            .call(Request::ExecOn {
                host: machine,
                script,
            })
            .await?
        {
            Response::ExecResult {
                output,
                exit_code,
                truncated,
            } => Ok(ExecResult {
                output,
                exit_code,
                truncated,
            }),
            other => Err(unexpected(other)),
        }
    }

    pub async fn mm_list(&self, machine: Option<String>) -> Result<Vec<MmSession>, LinkError> {
        match one(self, machine, mp::Request::List).await? {
            mp::Response::Sessions(list) => Ok(list.into_iter().map(MmSession::from).collect()),
            other => Err(wrong(other)),
        }
    }

    /// Starts a session. Returns the name it got.
    pub async fn mm_spawn(
        &self,
        machine: Option<String>,
        spec: MmSpawn,
    ) -> Result<String, LinkError> {
        let spec = mp::SpawnSpec {
            name: spec.name,
            command: spec.command,
            cwd: spec.cwd,
            size: mp::Size::new(spec.cols, spec.rows).sane(),
            label: None,
        };
        match one(self, machine, mp::Request::Spawn(spec)).await? {
            mp::Response::Spawned { name } => Ok(name),
            other => Err(wrong(other)),
        }
    }

    /// Ends a session (SIGHUP to its process group).
    pub async fn mm_kill(&self, machine: Option<String>, name: String) -> Result<(), LinkError> {
        match one(self, machine, mp::Request::Kill { name }).await? {
            mp::Response::Ok => Ok(()),
            other => Err(wrong(other)),
        }
    }

    /// Renames a session. The node cleans the name (letters, digits, `-_.`);
    /// list again to see what it became.
    pub async fn mm_rename(
        &self,
        machine: Option<String>,
        name: String,
        to: String,
    ) -> Result<(), LinkError> {
        match one(self, machine, mp::Request::Rename { name, to }).await? {
            mp::Response::Ok => Ok(()),
            other => Err(wrong(other)),
        }
    }

    /// Screens without attaching (thumbnails). Empty `names`: every session.
    pub async fn mm_peek(
        &self,
        machine: Option<String>,
        names: Vec<String>,
    ) -> Result<Vec<MmPeek>, LinkError> {
        match one(self, machine, mp::Request::Peek { names }).await? {
            mp::Response::Peeked(list) => Ok(list
                .into_iter()
                .map(|p| MmPeek {
                    name: p.name,
                    cols: p.size.cols,
                    rows: p.size.rows,
                    screen: p.screen,
                })
                .collect()),
            other => Err(wrong(other)),
        }
    }

    /// Attaches to a session. `history`: lines of scrollback to send first.
    /// `read_only`: watch without typing and without changing the size.
    pub async fn mm_attach(
        &self,
        machine: Option<String>,
        name: String,
        cols: u16,
        rows: u16,
        history: u32,
        read_only: bool,
    ) -> Result<Arc<MmAttachment>, LinkError> {
        let agent = Agent::open(self, machine).await?;
        let request = mp::Request::Attach {
            name,
            size: mp::Size::new(cols, rows).sane(),
            history,
            read_only,
        };
        match agent.request(&request).await? {
            mp::Response::Attached {
                size,
                read_only: honoured,
                ..
            } => {
                if read_only && !honoured {
                    agent.finish().await;
                    return Err(LinkError::Remote {
                        message: "this machine's manymux cannot attach read-only".into(),
                    });
                }
                Ok(Arc::new(MmAttachment {
                    agent,
                    cols: size.cols,
                    rows: size.rows,
                    read_only,
                }))
            }
            other => {
                agent.finish().await;
                Err(wrong(other))
            }
        }
    }

    /// Bells, notifications, titles, starts and exits on the hub and every
    /// machine its node watches, as one feed.
    pub async fn mm_events(&self) -> Result<Arc<MmEvents>, LinkError> {
        let agent = Agent::open(self, None).await?;
        match agent.request(&mp::Request::Events).await? {
            mp::Response::Ok => Ok(Arc::new(MmEvents { agent })),
            other => {
                agent.finish().await;
                Err(wrong(other))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum MmOutput {
    /// Terminal output. The first one after attaching repaints the screen.
    Data { bytes: Vec<u8> },
    /// Scrollback lines, before the first `Data` (when `history` > 0).
    History { bytes: Vec<u8> },
    /// The whole screen again, after `resync()`.
    Resync { bytes: Vec<u8> },
    /// The size the session took (the smallest of all attached clients).
    Size { cols: u16, rows: u16 },
    /// Something in another session on the same machine.
    Event { event: MmEvent },
    /// The answer to `rename()`: the new name, or why not.
    Renamed {
        name: Option<String>,
        refused: Option<String>,
    },
    /// The session's process ended. Nothing follows.
    Exit { code: i32 },
}

/// A live session. Read with `next`; type with `write`.
#[derive(uniffi::Object)]
pub struct MmAttachment {
    agent: Agent,
    cols: u16,
    rows: u16,
    read_only: bool,
}

#[uniffi::export(async_runtime = "tokio")]
impl MmAttachment {
    /// The size the session has now (from the attach answer).
    pub fn cols(&self) -> u16 {
        self.cols
    }

    pub fn rows(&self) -> u16 {
        self.rows
    }

    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// The next output. Liveness pings are answered here. After `Exit`, or a
    /// detach, an error.
    pub async fn next(&self) -> Result<MmOutput, LinkError> {
        loop {
            let Some(frame) = self.agent.next().await? else {
                return Err(LinkError::Closed {
                    message: "session stream ended".into(),
                });
            };
            return Ok(match frame.tag {
                tag::DATA => MmOutput::Data { bytes: frame.body },
                tag::HISTORY => MmOutput::History { bytes: frame.body },
                tag::RESYNC => MmOutput::Resync { bytes: frame.body },
                tag::SIZE => {
                    let size: mp::Size = decode(&frame.body)?;
                    MmOutput::Size {
                        cols: size.cols,
                        rows: size.rows,
                    }
                }
                tag::EVENT => MmOutput::Event {
                    event: decode::<mp::HostedEvent>(&frame.body)?.into(),
                },
                tag::RENAME => match decode::<mp::Renamed>(&frame.body)? {
                    mp::Renamed::Name(name) => MmOutput::Renamed {
                        name: Some(name),
                        refused: None,
                    },
                    mp::Renamed::Refused(why) => MmOutput::Renamed {
                        name: None,
                        refused: Some(why),
                    },
                },
                tag::EXIT => MmOutput::Exit {
                    code: decode(&frame.body)?,
                },
                tag::PING => {
                    // Answering once means answering always: the node then
                    // detaches a client silent for 45 s (a suspended app).
                    self.agent.frame(tag::PONG, &[]).await?;
                    continue;
                }
                // PARK and anything newer than this build.
                _ => continue,
            });
        }
    }

    /// Keystrokes (and pastes of text).
    pub async fn write(&self, bytes: Vec<u8>) -> Result<(), LinkError> {
        self.agent.frame(tag::DATA, &bytes).await
    }

    /// Asks for a size; `Size` in `next` says what the session took.
    pub async fn resize(&self, cols: u16, rows: u16) -> Result<(), LinkError> {
        self.agent
            .msg(tag::RESIZE, &mp::Size::new(cols, rows).sane())
            .await
    }

    /// Asks for a new name; the answer is `Renamed` in `next`.
    pub async fn rename(&self, to: String) -> Result<(), LinkError> {
        self.agent.msg(tag::RENAME, &to).await
    }

    /// Asks for the whole screen again; it comes as `Resync`.
    pub async fn resync(&self) -> Result<(), LinkError> {
        self.agent.frame(tag::RESYNC, &[]).await
    }

    /// Sends an image file. The machine saves it and types its path into the
    /// session. `kind`: the file extension (`png`, `jpg`, `gif`, `webp`).
    pub async fn paste(&self, kind: String, bytes: Vec<u8>) -> Result<(), LinkError> {
        if bytes.len() > mp::MAX_PASTE {
            return Err(LinkError::Invalid {
                message: format!("{} bytes; the limit is {}", bytes.len(), mp::MAX_PASTE),
            });
        }
        for chunk in bytes.chunks(mp::PASTE_CHUNK) {
            self.agent.frame(tag::PASTE, chunk).await?;
        }
        self.agent
            .msg(tag::PASTE_END, &mp::PasteInfo { kind })
            .await
    }

    /// Leaves the session running and closes this stream.
    pub async fn detach(&self) {
        let _ = self.agent.frame(tag::DETACH, &[]).await;
        self.agent.finish().await;
    }
}

/// The event feed from `mm_events`.
#[derive(uniffi::Object)]
pub struct MmEvents {
    agent: Agent,
}

#[uniffi::export(async_runtime = "tokio")]
impl MmEvents {
    pub async fn next(&self) -> Result<MmEvent, LinkError> {
        loop {
            let Some(frame) = self.agent.next().await? else {
                return Err(LinkError::Closed {
                    message: "event feed ended".into(),
                });
            };
            if frame.tag == tag::EVENT {
                return Ok(mp::decode::<mp::HostedEvent>(&frame.body)
                    .map_err(protocol)?
                    .into());
            }
        }
    }

    pub async fn close(&self) {
        self.agent.finish().await;
    }
}
