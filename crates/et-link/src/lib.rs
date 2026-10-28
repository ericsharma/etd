//! et-link: the E.T. app's side of et/1, for Swift through UniFFI.
//!
//! - [`Link`]: the phone's one iroh endpoint. Start it once at app launch
//!   (it takes seconds on iOS) and share it for all machines.
//! - [`RemoteHost`]: one connection to one machine's etd. It maps to the app's
//!   `SSHConnection` calls: hello (findTmux), sessions, exec (run), upload,
//!   open_control (openControlChannel).
//! - [`ControlStream`]: `tmux -C` bytes; it backs the app's `ControlChannel`.
//! - [`RemoteHost::send_files`]: files to a machine's `~/etd-downloads/`.
use std::sync::Arc;
use std::time::{Duration, Instant};

use et_proto::files::{Files, GC_EVERY};
use et_proto::{read_frame, write_frame, ControlFrame, Request, Response, Ticket, ALPN};
use iroh::endpoint::{presets, ConnectionError, QuicTransportConfig, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayUrl, SecretKey};
use tokio::sync::Mutex;

mod logbook;
mod mm;
pub use logbook::*;
pub use mm::*;

uniffi::setup_scaffolding!();

const KEEP_ALIVE: Duration = Duration::from_secs(5);
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, uniffi::Error)]
pub enum LinkError {
    /// This device is not paired with the machine (or was revoked).
    NotPaired { message: String },
    /// The machine refused a pairing code: wrong, used, expired, or too many tries.
    PairingRefused { message: String },
    /// The machine could not be reached.
    Unreachable { message: String },
    /// The connection closed.
    Closed { message: String },
    /// etd answered with an error (for example "tmux not found").
    Remote { message: String },
    /// Bad input from the app (a bad link or id).
    Invalid { message: String },
    /// An unexpected answer: probably different etd and app versions.
    Protocol { message: String },
}

impl std::fmt::Display for LinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (kind, message) = match self {
            Self::NotPaired { message } => ("not paired", message),
            Self::PairingRefused { message } => ("pairing refused", message),
            Self::Unreachable { message } => ("unreachable", message),
            Self::Closed { message } => ("closed", message),
            Self::Remote { message } => ("etd", message),
            Self::Invalid { message } => ("invalid", message),
            Self::Protocol { message } => ("protocol", message),
        };
        write!(f, "{kind}: {message}")
    }
}

impl std::error::Error for LinkError {}

fn invalid(e: impl std::fmt::Display) -> LinkError {
    LinkError::Invalid { message: e.to_string() }
}

/// Maps a lost connection to a typed error, by etd's close reasons.
fn closed(e: &ConnectionError) -> LinkError {
    if let ConnectionError::ApplicationClosed(close) = e {
        let reason = String::from_utf8_lossy(&close.reason).into_owned();
        if reason == "not paired" || reason == "revoked" {
            return LinkError::NotPaired { message: reason };
        }
        return LinkError::Closed { message: reason };
    }
    LinkError::Closed { message: e.to_string() }
}

/// An anyhow error from et-proto framing: a closed connection if it was one.
fn stream_error(e: anyhow::Error) -> LinkError {
    for cause in e.chain() {
        if let Some(c) = cause.downcast_ref::<ConnectionError>() {
            return closed(c);
        }
        if let Some(iroh::endpoint::ReadError::ConnectionLost(c)) = cause.downcast_ref() {
            return closed(c);
        }
        if let Some(iroh::endpoint::WriteError::ConnectionLost(c)) = cause.downcast_ref() {
            return closed(c);
        }
        if let Some(iroh::endpoint::ReadExactError::ReadError(iroh::endpoint::ReadError::ConnectionLost(c))) = cause.downcast_ref() {
            return closed(c);
        }
    }
    LinkError::Protocol { message: format!("{e:#}") }
}

/// A new 32-byte device key. Keep it in the Keychain (this device only).
#[uniffi::export]
pub fn generate_secret_key() -> Vec<u8> {
    SecretKey::generate().to_bytes().to_vec()
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TicketInfo {
    pub host_name: String,
    pub endpoint_id: String,
    pub relay_url: Option<String>,
    /// Unix seconds.
    pub expires: u64,
}

/// Reads an `et://pair?t=…` link, for the confirm sheet. No network.
#[uniffi::export]
pub fn parse_ticket(url: String) -> Result<TicketInfo, LinkError> {
    let t = Ticket::from_url(&url).map_err(invalid)?;
    let relay_url = t.addr.relay_urls().next().map(|u| u.to_string());
    Ok(TicketInfo { host_name: t.host_name, endpoint_id: t.addr.id.to_string(), relay_url, expires: t.expires })
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct Paired {
    pub host_name: String,
    /// Save these two with the server; `Link::connect` takes them.
    pub endpoint_id: String,
    pub relay_url: Option<String>,
}

/// The phone's iroh endpoint, shared by all machines.
#[derive(uniffi::Object)]
pub struct Link {
    endpoint: Endpoint,
}

#[uniffi::export(async_runtime = "tokio")]
impl Link {
    /// Binds the endpoint with the device key from `generate_secret_key`.
    #[uniffi::constructor]
    pub async fn start(secret_key: Vec<u8>) -> Result<Arc<Self>, LinkError> {
        let bytes: [u8; 32] = secret_key.try_into().map_err(|_| invalid("the device key must be 32 bytes"))?;
        let transport = QuicTransportConfig::builder()
            .keep_alive_interval(KEEP_ALIVE)
            .max_idle_timeout(Some(IDLE_TIMEOUT.try_into().map_err(invalid)?))
            .build();
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(SecretKey::from_bytes(&bytes))
            .transport_config(transport)
            .bind()
            .await
            .map_err(|e| LinkError::Unreachable { message: format!("start iroh: {e:#}") })?;
        Ok(Arc::new(Self { endpoint }))
    }

    /// This device's id, as etd lists it in `etd clients`.
    pub fn endpoint_id(&self) -> String {
        self.endpoint.id().to_string()
    }

    /// Pairs with the machine in an `et://pair` link. The returned host can
    /// be used at once.
    pub async fn pair(&self, url: String, device_name: String) -> Result<Paired, LinkError> {
        let ticket = Ticket::from_url(&url).map_err(invalid)?;
        let relay_url = ticket.addr.relay_urls().next().map(|u| u.to_string());
        let host = self.dial(ticket.addr.clone()).await?;
        match host.call(Request::Pair { secret: ticket.secret, device_name }).await? {
            Response::Paired { host_name } => Ok(Paired { host_name, endpoint_id: ticket.addr.id.to_string(), relay_url }),
            Response::Rejected(message) => Err(LinkError::PairingRefused { message }),
            other => Err(unexpected(other)),
        }
    }

    /// Connects to a paired machine. `relay_url` from `Paired` makes the
    /// first connect quicker; without it the id is looked up.
    pub async fn connect(&self, endpoint_id: String, relay_url: Option<String>) -> Result<Arc<RemoteHost>, LinkError> {
        let id: EndpointId = endpoint_id.parse().map_err(invalid)?;
        let mut addr = EndpointAddr::new(id);
        if let Some(url) = relay_url {
            addr = addr.with_relay_url(url.parse::<RelayUrl>().map_err(invalid)?);
        }
        Ok(Arc::new(self.dial(addr).await?))
    }

    /// Closes all connections.
    pub async fn shutdown(&self) {
        self.endpoint.close().await;
    }
}

impl Link {
    async fn dial(&self, addr: EndpointAddr) -> Result<RemoteHost, LinkError> {
        let conn = self.endpoint.connect(addr, ALPN).await.map_err(|e| LinkError::Unreachable { message: format!("{e:#}") })?;
        Ok(RemoteHost { conn })
    }

    /// For tests: a link on an endpoint made by the caller.
    #[doc(hidden)]
    pub fn from_endpoint(endpoint: Endpoint) -> Arc<Self> {
        Arc::new(Self { endpoint })
    }
}

fn unexpected(response: Response) -> LinkError {
    match response {
        Response::Error(message) => LinkError::Remote { message },
        other => LinkError::Protocol { message: format!("unexpected answer: {other:?}") },
    }
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct TmuxInstall {
    pub path: String,
    pub version: String,
    /// The `TMUX_TMPDIR` tmux runs with. None: tmux's default.
    pub tmpdir: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct HostInfo {
    pub host_name: String,
    pub os: String,
    pub etd_version: String,
    /// None: tmux was not found; `tmux_error` says what was tried.
    pub tmux: Option<TmuxInstall>,
    pub tmux_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct SessionInfo {
    pub id: String,
    pub name: String,
    pub windows: u32,
    pub attached: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct ExecResult {
    pub output: Vec<u8>,
    /// None: killed (by a signal or the 120 s limit).
    pub exit_code: Option<i32>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct NetworkMember {
    /// Connect to a machine with `Link::connect(endpoint_id, nil)`.
    pub endpoint_id: String,
    pub name: String,
    /// A machine runs etd (and has sessions); otherwise a phone or tablet.
    pub is_machine: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct NetworkInfo {
    pub name: String,
    /// The network key (the network's id).
    pub key: String,
    pub members: Vec<NetworkMember>,
    /// The Logbook archives (machine names), the one to use first. Empty:
    /// none, or the machine did not say.
    pub archive: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum ControlTarget {
    Attach { name: String },
    AttachOrCreate { name: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum PathKind {
    Direct,
    Relay,
    None,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct PathInfo {
    pub kind: PathKind,
    pub rtt_ms: f64,
}

/// Progress of `send_files`: payload bytes the machine has, of `total`.
#[uniffi::export(with_foreign)]
pub trait SendProgress: Send + Sync {
    fn on_progress(&self, done: u64, total: u64);
}

/// What `send_files` delivered.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct Delivered {
    /// The top-level paths on the machine, one per file or folder sent.
    pub paths: Vec<String>,
    /// etd typed them into the session.
    pub typed: bool,
}

/// A file or folder sent to a machine (`shared_files`).
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct SharedFile {
    /// The network's name for the machine it is on.
    pub machine: String,
    /// Its path there (use it with `fetch_file`, `share_file`, `unshare_file`).
    pub path: String,
    pub name: String,
    pub size: u64,
    pub is_dir: bool,
    /// Who sent it (member name).
    pub from: String,
    /// Unix seconds.
    pub received: u64,
    /// The session it was typed into, if any.
    pub session: Option<String>,
}

/// Fetches collection `hash` over `conn` (inside an et/1 `Blobs` stream)
/// and writes it under `dir`. Returns the top-level item's path.
async fn fetch_into(
    conn: &iroh::endpoint::Connection,
    hash: iroh_blobs::Hash,
    dir: &std::path::Path,
    total: u64,
    progress: &dyn SendProgress,
) -> Result<String, LinkError> {
    use iroh_blobs::api::blobs::{ExportMode, ExportOptions};
    use iroh_blobs::api::remote::GetProgressItem;
    use n0_future::StreamExt;
    let files = files().await?;
    let store = files.store();
    let content = iroh_blobs::HashAndFormat::hash_seq(hash);
    let tag = format!("fetch/{hash}");
    let other = |e: anyhow::Error| LinkError::Protocol { message: format!("{e:#}") };
    store.tags().set(&tag, content).await.map_err(|e| other(e.into()))?;
    let result = async {
        let (mut up, down) = conn.open_bi().await.map_err(|e| closed(&e))?;
        write_frame(&mut up, &Request::Blobs).await.map_err(stream_error)?;
        let pair = iroh_blobs::get::StreamPair::new(conn.stable_id() as u64, down, up);
        let mut fetch = store.remote().fetch(pair, content).stream();
        loop {
            match fetch.next().await {
                Some(GetProgressItem::Progress(done)) => progress.on_progress(done, total),
                Some(GetProgressItem::Done(_)) => break,
                Some(GetProgressItem::Error(e)) => return Err(LinkError::Closed { message: format!("fetch: {e}") }),
                None => return Err(LinkError::Closed { message: "fetch ended".into() }),
            }
        }
        progress.on_progress(total, total);
        let collection =
            iroh_blobs::format::collection::Collection::load(hash, store.as_ref()).await.map_err(|e| other(anyhow::anyhow!("{e}")))?;
        let mut top = None;
        for (name, item) in collection.iter() {
            let parts: Vec<&str> = name.split('/').collect();
            if parts.iter().any(|p| p.is_empty() || *p == "." || *p == ".." || p.contains('\\')) {
                return Err(LinkError::Protocol { message: format!("bad file name {name:?}") });
            }
            top.get_or_insert_with(|| dir.join(parts[0]));
            let target = parts.iter().fold(dir.to_path_buf(), |p, part| p.join(part));
            if target.exists() {
                continue;
            }
            std::fs::create_dir_all(target.parent().unwrap_or(dir)).map_err(invalid)?;
            store
                .export_with_opts(ExportOptions { hash: *item, target: target.clone(), mode: ExportMode::Copy })
                .await
                .map_err(|e| other(anyhow::anyhow!("write {}: {e}", target.display())))?;
        }
        top.map(|t| t.display().to_string()).ok_or_else(|| LinkError::Protocol { message: "empty".into() })
    }
    .await;
    let _ = store.tags().delete(&tag).await;
    result
}

/// The app's blob store for sending, in its temporary directory: it holds
/// only references to the files being sent.
static FILES: tokio::sync::OnceCell<Arc<Files>> = tokio::sync::OnceCell::const_new();

async fn files() -> Result<Arc<Files>, LinkError> {
    FILES
        .get_or_try_init(|| async { Files::open(&std::env::temp_dir().join("et-link-files"), GC_EVERY).await })
        .await
        .cloned()
        .map_err(|e| LinkError::Invalid { message: format!("file store: {e:#}") })
}

/// One connection to one machine's etd.
#[derive(uniffi::Object)]
pub struct RemoteHost {
    conn: iroh::endpoint::Connection,
}

#[uniffi::export(async_runtime = "tokio")]
impl RemoteHost {
    /// Host and tmux facts. Replaces `findTmux`.
    pub async fn hello(&self, app: String) -> Result<HostInfo, LinkError> {
        match self.call(Request::Hello { app }).await? {
            Response::HostInfo(info) => {
                let (tmux, tmux_error) = match info.tmux {
                    et_proto::TmuxStatus::Found(t) => (Some(TmuxInstall { path: t.path, version: t.version, tmpdir: t.tmpdir }), None),
                    et_proto::TmuxStatus::NotFound(why) => (None, Some(why)),
                };
                Ok(HostInfo { host_name: info.host_name, os: info.os, etd_version: info.etd_version, tmux, tmux_error })
            }
            other => Err(unexpected(other)),
        }
    }

    pub async fn list_sessions(&self) -> Result<Vec<SessionInfo>, LinkError> {
        match self.call(Request::ListSessions).await? {
            Response::Sessions(list) => {
                Ok(list.into_iter().map(|s| SessionInfo { id: s.id, name: s.name, windows: s.windows, attached: s.attached }).collect())
            }
            other => Err(unexpected(other)),
        }
    }

    /// `/bin/sh -c` on the machine; stderr joined to stdout. Replaces `run`.
    pub async fn exec(&self, script: String) -> Result<ExecResult, LinkError> {
        match self.call(Request::Exec { script }).await? {
            Response::ExecResult { output, exit_code, truncated } => Ok(ExecResult { output, exit_code, truncated }),
            other => Err(unexpected(other)),
        }
    }

    /// Writes `data` to `path` (absolute or `~/…`, inside home). Returns the
    /// absolute path written.
    pub async fn upload(&self, path: String, data: Vec<u8>) -> Result<String, LinkError> {
        let (mut send, mut recv) = self.conn.open_bi().await.map_err(|e| closed(&e))?;
        write_frame(&mut send, &Request::Upload { path, len: data.len() as u64 }).await.map_err(stream_error)?;
        send.write_all(&data).await.map_err(|e| stream_error(e.into()))?;
        send.finish().map_err(|e| LinkError::Closed { message: e.to_string() })?;
        match read_frame::<Response>(&mut recv).await.map_err(stream_error)? {
            Some(Response::Uploaded { path }) => Ok(path),
            Some(other) => Err(unexpected(other)),
            None => Err(LinkError::Protocol { message: "no answer".into() }),
        }
    }

    /// Starts `tmux -C`. Replaces `openControlChannel`.
    pub async fn open_control(&self, target: ControlTarget) -> Result<Arc<ControlStream>, LinkError> {
        let target = match target {
            ControlTarget::Attach { name } => et_proto::Target::Attach(name),
            ControlTarget::AttachOrCreate { name } => et_proto::Target::AttachOrCreate(name),
        };
        let (mut send, mut recv) = self.conn.open_bi().await.map_err(|e| closed(&e))?;
        write_frame(&mut send, &Request::Control { target }).await.map_err(stream_error)?;
        match read_frame::<Response>(&mut recv).await.map_err(stream_error)? {
            Some(Response::ControlStarted) => Ok(Arc::new(ControlStream { send: Mutex::new(Some(send)), recv: Mutex::new(recv) })),
            Some(other) => Err(unexpected(other)),
            None => Err(LinkError::Protocol { message: "no answer".into() }),
        }
    }

    /// One request round trip, in milliseconds.
    pub async fn ping(&self) -> Result<f64, LinkError> {
        let start = Instant::now();
        match self.call(Request::Ping).await? {
            Response::Pong => Ok(start.elapsed().as_secs_f64() * 1000.0),
            other => Err(unexpected(other)),
        }
    }

    /// The path in use now: direct or relay, and its round-trip time.
    pub fn path(&self) -> PathInfo {
        let paths = self.conn.paths();
        match paths.iter().find(|p| p.is_selected()) {
            Some(p) => {
                PathInfo { kind: if p.is_relay() { PathKind::Relay } else { PathKind::Direct }, rtt_ms: p.rtt().as_secs_f64() * 1000.0 }
            }
            None => PathInfo { kind: PathKind::None, rtt_ms: 0.0 },
        }
    }

    /// The network this machine belongs to: every machine and device. The
    /// network key's signature is checked here, so a machine cannot show a
    /// member list the network did not sign.
    pub async fn network(&self) -> Result<NetworkInfo, LinkError> {
        match self.call(Request::Network).await? {
            Response::NetworkDoc(signed) => {
                signed.verify().map_err(|e| LinkError::Protocol { message: format!("{e:#}") })?;
                let members = signed
                    .network
                    .members()
                    .map(|m| NetworkMember {
                        endpoint_id: m.id.to_string(),
                        name: m.name.to_string(),
                        is_machine: m.kind == et_proto::network::Kind::Machine,
                    })
                    .collect();
                let archive = self.archive_names().await;
                Ok(NetworkInfo { name: signed.network.name.clone(), key: signed.network.key.to_string(), members, archive })
            }
            other => Err(unexpected(other)),
        }
    }

    /// An invite link (`et://pair?t=…`) for a new device, or with `machine`
    /// for a new machine (`etd join <link>` there). Valid 10 minutes, once.
    pub async fn invite(&self, machine: bool) -> Result<String, LinkError> {
        let kind = if machine { et_proto::network::Kind::Machine } else { et_proto::network::Kind::Device };
        match self.call(Request::Invite { kind }).await? {
            Response::Invitation { url } => Ok(url),
            other => Err(unexpected(other)),
        }
    }

    /// "The app is open" for the next `seconds` (at most 120): this machine
    /// sends no pushes until then. Send it every 20 s while open, to every
    /// machine.
    pub async fn foreground(&self, seconds: u32) -> Result<(), LinkError> {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        match self.call(Request::Foreground { until: now + u64::from(seconds) }).await? {
            Response::Pong => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    /// Sends files and folders (local paths) to `~/etd-downloads/` on this
    /// machine (PLAN-files). `session`: also type the paths into that
    /// manymux session. Keep access to the files (security-scoped URLs)
    /// until it returns. Cancel the task to stop; sending again resumes.
    pub async fn send_files(
        &self,
        paths: Vec<String>,
        session: Option<String>,
        progress: Arc<dyn SendProgress>,
    ) -> Result<Delivered, LinkError> {
        let files = files().await?;
        let paths: Vec<std::path::PathBuf> = paths.into_iter().map(Into::into).collect();
        let outgoing = files.import(&paths).await.map_err(|e| invalid(format!("{e:#}")))?;
        progress.on_progress(0, outgoing.total);
        let answer =
            files.send(&self.conn, &outgoing, session, |done, total| progress.on_progress(done, total)).await.map_err(stream_error)?;
        match answer {
            Response::Delivered { paths, typed } => Ok(Delivered { paths, typed }),
            other => Err(unexpected(other)),
        }
    }

    /// The files sent to this machine that are still there, newest first.
    pub async fn shared_files(&self) -> Result<Vec<SharedFile>, LinkError> {
        match self.call(Request::Shared).await? {
            Response::SharedList(list) => Ok(list
                .into_iter()
                .map(|i| SharedFile {
                    machine: i.machine,
                    path: i.path,
                    name: i.name,
                    size: i.size,
                    is_dir: i.is_dir,
                    from: i.from,
                    received: i.received,
                    session: i.session,
                })
                .collect()),
            other => Err(unexpected(other)),
        }
    }

    /// Copies a shared file or folder from this machine into the local
    /// directory `into` (for a preview), and returns its local path. The same
    /// content fetched again is not fetched twice.
    pub async fn fetch_file(&self, path: String, into: String, progress: Arc<dyn SendProgress>) -> Result<String, LinkError> {
        // The machine offers it while this stream stays open.
        let (mut offer_up, mut offer_down) = self.conn.open_bi().await.map_err(|e| closed(&e))?;
        write_frame(&mut offer_up, &Request::Offer { path }).await.map_err(stream_error)?;
        let (hash, total) = match read_frame::<Response>(&mut offer_down).await.map_err(stream_error)? {
            Some(Response::Offered { hash, total }) => (iroh_blobs::Hash::from_bytes(hash), total),
            Some(other) => return Err(unexpected(other)),
            None => return Err(LinkError::Protocol { message: "no answer".into() }),
        };
        progress.on_progress(0, total);
        let dir = std::path::PathBuf::from(into).join(&hash.to_hex()[..16]);
        let fetched = fetch_into(&self.conn, hash, &dir, total, progress.as_ref()).await;
        let _ = offer_up.finish();
        fetched
    }

    /// Uses a shared file again: types it into `session` on its machine (`to`
    /// is this machine), or sends it from here to the machine `to` and types
    /// it there. No bytes go through the app.
    pub async fn share_file(
        &self,
        path: String,
        to: String,
        session: Option<String>,
        progress: Arc<dyn SendProgress>,
    ) -> Result<Delivered, LinkError> {
        let (mut up, mut down) = self.conn.open_bi().await.map_err(|e| closed(&e))?;
        write_frame(&mut up, &Request::Share { path, to, session }).await.map_err(stream_error)?;
        up.finish().map_err(|e| LinkError::Closed { message: e.to_string() })?;
        loop {
            match read_frame::<Response>(&mut down).await.map_err(stream_error)? {
                Some(Response::Progress { done, total }) => progress.on_progress(done, total),
                Some(Response::Delivered { paths, typed }) => return Ok(Delivered { paths, typed }),
                Some(other) => return Err(unexpected(other)),
                None => return Err(LinkError::Protocol { message: "no answer".into() }),
            }
        }
    }

    /// Deletes a shared file or folder on this machine.
    pub async fn unshare_file(&self, path: String) -> Result<(), LinkError> {
        match self.call(Request::Unshare { path }).await? {
            Response::Pong => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    /// Waits until the connection closes, and says why.
    pub async fn closed(&self) -> LinkError {
        closed(&self.conn.closed().await)
    }

    pub fn close(&self) {
        self.conn.close(0u32.into(), b"bye");
    }
}

impl RemoteHost {
    async fn call(&self, request: Request) -> Result<Response, LinkError> {
        let (mut send, mut recv) = self.conn.open_bi().await.map_err(|e| closed(&e))?;
        write_frame(&mut send, &request).await.map_err(stream_error)?;
        send.finish().map_err(|e| LinkError::Closed { message: e.to_string() })?;
        match read_frame::<Response>(&mut recv).await {
            Ok(Some(response)) => Ok(response),
            // etd closes the connection of a device it does not know.
            Ok(None) => Err(self.conn.close_reason().map(|e| closed(&e)).unwrap_or(LinkError::Protocol { message: "no answer".into() })),
            Err(e) => Err(stream_error(e)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum ControlEvent {
    /// tmux -C output bytes. A line can be split across events.
    Stdout {
        bytes: Vec<u8>,
    },
    Stderr {
        line: String,
    },
    /// tmux exited (None: killed by a signal). No more events follow.
    Exit {
        code: Option<i32>,
    },
}

/// A running `tmux -C`. Read with `next`, write with `write`.
#[derive(uniffi::Object)]
pub struct ControlStream {
    send: Mutex<Option<SendStream>>,
    recv: Mutex<RecvStream>,
}

#[uniffi::export(async_runtime = "tokio")]
impl ControlStream {
    /// The next event. After `Exit`, or when the stream ends, an error.
    pub async fn next(&self) -> Result<ControlEvent, LinkError> {
        let mut recv = self.recv.lock().await;
        match read_frame::<ControlFrame>(&mut recv).await.map_err(stream_error)? {
            Some(ControlFrame::Stdout(bytes)) => Ok(ControlEvent::Stdout { bytes }),
            Some(ControlFrame::Stderr(line)) => Ok(ControlEvent::Stderr { line }),
            Some(ControlFrame::Exit(code)) => Ok(ControlEvent::Exit { code }),
            None => Err(LinkError::Closed { message: "control stream ended".into() }),
        }
    }

    /// Bytes for tmux's stdin (commands, each ending in a newline).
    pub async fn write(&self, bytes: Vec<u8>) -> Result<(), LinkError> {
        let mut send = self.send.lock().await;
        let send = send.as_mut().ok_or_else(|| LinkError::Closed { message: "control stream finished".into() })?;
        send.write_all(&bytes).await.map_err(|e| stream_error(e.into()))
    }

    /// Closes tmux's stdin: tmux detaches, the session keeps running. Read
    /// `next` until `Exit` to see it end.
    pub async fn finish(&self) {
        if let Some(mut send) = self.send.lock().await.take() {
            let _ = send.finish();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticket_parses_for_the_confirm_sheet() {
        let id = SecretKey::from_bytes(&[3; 32]).public();
        let ticket = Ticket {
            addr: EndpointAddr::new(id).with_relay_url("https://relay.example./".parse().unwrap()),
            secret: [1; 32],
            expires: 42,
            host_name: "box".into(),
        };
        let info = parse_ticket(ticket.to_url().unwrap()).unwrap();
        assert_eq!(info.host_name, "box");
        assert_eq!(info.endpoint_id, id.to_string());
        assert_eq!(info.relay_url.as_deref(), Some("https://relay.example./"));
        assert!(matches!(parse_ticket("https://example.com".into()), Err(LinkError::Invalid { .. })));
    }

    #[test]
    fn device_keys_are_32_bytes() {
        assert_eq!(generate_secret_key().len(), 32);
    }
}
