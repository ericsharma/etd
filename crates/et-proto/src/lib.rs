//! et/1: the protocol between E.T. and `etd`, over iroh.
//!
//! - One iroh connection per E.T. session. Each operation opens one bi-stream.
//! - The first frame on a stream is a [`Request`]; `etd` answers with one
//!   [`Response`] frame. `Control` then carries [`ControlFrame`]s down and raw
//!   bytes up (tmux stdin). `Upload` sends its raw bytes after the request.
//!   `Deliver` answers `Progress` frames, then `Delivered`.
//! - A frame is a big-endian `u32` length, then a postcard body.
//! - postcard is positional. New variants go at the end of an enum, so an
//!   older peer still decodes what it knows. Any other change (reorder,
//!   remove, a new field) is a new ALPN. No users yet: no compatibility code.
//! - A device that is not paired may send only `Pair`.
use anyhow::{bail, Context};
use iroh::endpoint::{RecvStream, SendStream};
use iroh::EndpointAddr;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub mod files;
pub mod logbook;
pub mod network;

pub const ALPN: &[u8] = b"et/1";

/// Frames larger than this are refused.
pub const MAX_FRAME: usize = 16 * 1024 * 1024;

/// Largest `Upload`.
pub const MAX_UPLOAD: u64 = 32 * 1024 * 1024;

/// `Exec` output beyond this is cut off.
pub const MAX_EXEC_OUTPUT: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Request {
    Hello {
        app: String,
    },
    ListSessions,
    Control {
        target: Target,
    },
    Ping,
    /// The secret from a pairing [`Ticket`].
    Pair {
        secret: [u8; 32],
        device_name: String,
    },
    /// `/bin/sh -c` with the user's login environment; stderr joined to stdout.
    Exec {
        script: String,
    },
    /// `len` raw bytes follow. `path` is absolute or starts with `~/`, and must
    /// be inside the user's home directory. Parent directories are made.
    Upload {
        path: String,
        len: u64,
    },
    /// A pipe to `mm agent` (manymux): on this machine (`None`), or on the
    /// machine of the network called `host`, through this one. After
    /// `AgentStarted` the stream carries `ControlFrame`s down and raw bytes
    /// up, as `Control`.
    Agent {
        host: Option<String>,
    },
    /// `Exec` on the machine of the network called `host`, through this one.
    ExecOn {
        host: String,
        script: String,
    },
    /// The machines this machine's manymux node watches (`mm hosts`).
    Machines,
    /// Join this machine's network with an invite's secret. A device that is
    /// not a member may send this (as `Pair`). `Pair` is `Join` with
    /// `kind: Device`.
    Join {
        secret: [u8; 32],
        name: String,
        kind: network::Kind,
    },
    /// The network document (any member).
    Network,
    /// A newer document from another machine of the network (machines only).
    NetworkUpdate {
        network: network::Signed,
    },
    /// An invite link for a new member of `kind` (any member).
    Invite {
        kind: network::Kind,
    },
    /// A session on the sending machine wants someone (machines only). A
    /// machine with a push command runs it; others ignore it.
    Notify {
        note: Note,
    },
    /// The app is open until `until` (Unix seconds): no pushes until then
    /// (any member; the app sends it every 20 s while open).
    Foreground {
        until: u64,
    },
    /// The network key changed because a machine was revoked (machines
    /// only). `chain`: every key change of the network, oldest first; `key`:
    /// the secret half of the last one's new key.
    NetworkRotate {
        chain: Vec<network::Rotation>,
        key: [u8; 32],
    },
    /// Take the files of iroh-blobs collection `hash` from the sender: this
    /// etd fetches it over this same connection (the sender serves it there),
    /// then writes it to `~/etd-downloads/`. `Progress` frames, then
    /// `Delivered`. `session`: also type the paths into that manymux session.
    Deliver {
        hash: [u8; 32],
        session: Option<String>,
    },
    /// Local socket only (`etd send`): send `paths` (files or folders) to the
    /// machine of the network called `to`, with `Deliver`. `Progress` frames,
    /// then the machine's `Delivered`.
    Send {
        paths: Vec<String>,
        to: String,
        session: Option<String>,
    },
    /// The files delivered to this machine that are still on disk, newest
    /// first. On the local socket (`etd files`): those of every machine.
    Shared,
    /// Offer `path` (inside `~/etd-downloads/`) for a `Blobs` fetch, while
    /// this stream stays open.
    Offer {
        path: String,
    },
    /// The rest of this stream is an iroh-blobs request, served from the
    /// offered collections only.
    Blobs,
    /// Use a shared file again: type `path` into `session` here (`to` is
    /// this machine), or send it to the machine `to` and type it there.
    /// `Progress` frames, then `Delivered`.
    Share {
        path: String,
        to: String,
        session: Option<String>,
    },
    /// Delete a shared file or folder (inside `~/etd-downloads/`).
    Unshare {
        path: String,
    },
    /// Is this machine a Logbook archive, and which manymux sessions of the
    /// asking machine must it not record (any member).
    LogInfo,
    /// Logbook (machines only, to an archive): `bytes` of the transcript
    /// `session_id` of the sending machine, from `offset` in its file. Whole
    /// lines only. Empty `bytes`: only `meta`, and the archive's offset.
    /// `offset` above the archive's: the bytes between are skipped (not
    /// recorded). Below: nothing is written. `LogAck` either way.
    LogAppend {
        session_id: String,
        meta: logbook::LogMeta,
        offset: u64,
        bytes: Vec<u8>,
    },
}

/// A file or folder delivered to a machine (`Shared`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedItem {
    /// The network's name for the machine it is on.
    pub machine: String,
    /// Its top-level path there.
    pub path: String,
    pub name: String,
    pub size: u64,
    pub is_dir: bool,
    /// The sender's member name.
    pub from: String,
    /// Unix seconds.
    pub received: u64,
    pub session: Option<String>,
}

/// What a push says. `machine` is the network's name for the machine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Note {
    /// `bell`, `notify` or `exit`.
    pub kind: String,
    pub machine: String,
    pub session: String,
    pub title: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Target {
    /// Attach to an existing session by exact name.
    Attach(String),
    /// Attach, or create the session if it does not exist.
    AttachOrCreate(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Response {
    HostInfo(HostInfo),
    Sessions(Vec<SessionInfo>),
    /// `tmux -C` started; `ControlFrame`s follow.
    ControlStarted,
    Pong,
    Error(String),
    Paired {
        host_name: String,
    },
    /// Wrong or used secret, or too many tries. The connection closes after it.
    Rejected(String),
    /// `exit_code` None: killed by a signal or by the time limit.
    ExecResult {
        output: Vec<u8>,
        exit_code: Option<i32>,
        truncated: bool,
    },
    /// The absolute path written.
    Uploaded {
        path: String,
    },
    /// `mm agent` started; `ControlFrame`s follow.
    AgentStarted,
    Machines(Vec<String>),
    /// The network after the join. `network_key`: the network key's secret
    /// half, for a machine only.
    Joined {
        network: network::Signed,
        network_key: Option<[u8; 32]>,
    },
    NetworkDoc(network::Signed),
    Invitation {
        url: String,
    },
    /// Payload bytes of a `Deliver` so far.
    Progress {
        done: u64,
        total: u64,
    },
    /// The top-level paths written (one per file or folder sent).
    /// `typed`: they were typed into the session.
    Delivered {
        paths: Vec<String>,
        typed: bool,
    },
    SharedList(Vec<SharedItem>),
    /// The offered collection's root hash and its size in bytes.
    Offered {
        hash: [u8; 32],
        total: u64,
    },
    /// `paused`: the asker's manymux sessions that this archive does not
    /// record. Empty when `archive` is false.
    LogInfo {
        archive: bool,
        paused: Vec<String>,
    },
    /// The source offset the archive has for that transcript: send from here.
    LogAck {
        offset: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostInfo {
    pub host_name: String,
    pub os: String,
    pub etd_version: String,
    pub tmux: TmuxStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TmuxStatus {
    Found(TmuxInstall),
    /// What the search tried, for the "tmux not found" screen.
    NotFound(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TmuxInstall {
    pub path: String,
    pub version: String,
    /// The `TMUX_TMPDIR` that tmux runs with. None: tmux's default.
    pub tmpdir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionInfo {
    /// tmux's `$n` id.
    pub id: String,
    pub name: String,
    pub windows: u32,
    pub attached: u32,
}

/// Down a `Control` or `Agent` stream, after `ControlStarted` or `AgentStarted`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ControlFrame {
    Stdout(Vec<u8>),
    Stderr(String),
    /// The process (tmux, or `mm agent`) exited. None: killed by a signal.
    Exit(Option<i32>),
}

/// What the pairing QR code carries, as `et://pair?t=<BASE32>`. Upper case
/// lets the QR code use its compact alphanumeric mode for the payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ticket {
    pub addr: EndpointAddr,
    pub secret: [u8; 32],
    /// Unix seconds.
    pub expires: u64,
    pub host_name: String,
}

const TICKET_PREFIX: &str = "et://pair?t=";

impl Ticket {
    pub fn to_url(&self) -> anyhow::Result<String> {
        let bytes = postcard::to_stdvec(self)?;
        Ok(format!("{TICKET_PREFIX}{}", data_encoding::BASE32_NOPAD.encode(&bytes)))
    }

    pub fn from_url(url: &str) -> anyhow::Result<Self> {
        let body = url.trim().strip_prefix(TICKET_PREFIX).context("not an et://pair link")?;
        let bytes = data_encoding::BASE32_NOPAD.decode(body.to_ascii_uppercase().as_bytes()).context("bad ticket")?;
        postcard::from_bytes(&bytes).context("bad ticket")
    }
}

pub fn encode<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    let body = postcard::to_stdvec(value)?;
    if body.len() > MAX_FRAME {
        bail!("frame too large: {} bytes", body.len());
    }
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

pub async fn write_frame<T: Serialize>(send: &mut SendStream, value: &T) -> anyhow::Result<()> {
    send.write_all(&encode(value)?).await.context("write frame")?;
    Ok(())
}

/// Reads one frame. `Ok(None)`: the stream finished cleanly before a frame.
pub async fn read_frame<T: DeserializeOwned>(recv: &mut RecvStream) -> anyhow::Result<Option<T>> {
    let mut len = [0u8; 4];
    match recv.read_exact(&mut len).await {
        Ok(()) => {}
        Err(iroh::endpoint::ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(e) => return Err(e).context("read frame length"),
    }
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        bail!("frame too large: {len} bytes");
    }
    let mut body = vec![0u8; len];
    recv.read_exact(&mut body).await.context("read frame body")?;
    Ok(Some(postcard::from_bytes(&body).context("decode frame")?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip() {
        let frame = ControlFrame::Stdout(b"%begin 1 2 1\n".to_vec());
        let bytes = encode(&frame).unwrap();
        assert_eq!(u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize, bytes.len() - 4);
        let back: ControlFrame = postcard::from_bytes(&bytes[4..]).unwrap();
        assert_eq!(back, frame);
    }

    #[test]
    fn ticket_round_trip() {
        let id = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let ticket = Ticket {
            addr: EndpointAddr::new(id)
                .with_relay_url("https://relay.example./".parse().unwrap())
                .with_ip_addr("192.168.0.2:4000".parse().unwrap()),
            secret: [9; 32],
            expires: 1_800_000_000,
            host_name: "box".into(),
        };
        let url = ticket.to_url().unwrap();
        assert!(url.starts_with("et://pair?t="));
        assert_eq!(Ticket::from_url(&url).unwrap(), ticket);
        assert!(Ticket::from_url("et://pair?t=zz1").is_err());
    }
}
