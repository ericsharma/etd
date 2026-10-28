//! The iroh side: pair devices, accept paired devices, serve et/1 streams.
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::Context;
use et_proto::network::{Kind, Network, Signed};
use et_proto::{read_frame, write_frame, ControlFrame, HostInfo, Request, Response, Target, TmuxStatus, ALPN};
use iroh::endpoint::{presets, Connection, QuicTransportConfig, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointId, SecretKey};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::{self, Paths, Status};
use crate::tmux::{self, Env};
use crate::{events, files, local, logbook, mm, network, ops, pair};

const KEEP_ALIVE: Duration = Duration::from_secs(5);
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const ONLINE_TIMEOUT: Duration = Duration::from_secs(15);
const READ_CHUNK: usize = 64 * 1024;
/// How often revokes are applied and `status.json` is written.
const WATCH_EVERY: Duration = Duration::from_secs(2);
/// Time for an unpaired device to send its `Pair` request.
const PAIR_TIMEOUT: Duration = Duration::from_secs(10);
const PAIR_TRIES: usize = 5;
const PAIR_WINDOW: Duration = Duration::from_secs(60);

/// How often the network document goes to the other machines even when
/// nothing changed here (one that was down catches up).
const RESEND_EVERY: u32 = 150; // × WATCH_EVERY = 5 minutes
/// After a send to some machine failed: try again this soon, then twice as
/// long each time, up to `RESEND_EVERY`.
const RETRY_FIRST: u32 = 5; // × WATCH_EVERY = 10 seconds

#[derive(Debug)]
pub struct State {
    paths: Paths,
    env: Env,
    /// Set by `serve`: for invites (our address) and for sending the network.
    endpoint: OnceLock<Endpoint>,
    /// Open connections of members, for revokes and `etd status`.
    live: Mutex<HashMap<EndpointId, (String, Vec<Connection>)>>,
    /// Recent `Pair`/`Join` tries, per device.
    tries: Mutex<HashMap<EndpointId, Vec<Instant>>>,
    /// The push command (`$ETD_NOTIFY_COMMAND`), if this machine sends pushes.
    notify: OnceLock<String>,
    /// Until when (Unix seconds) each device said the app is open.
    foreground: Mutex<HashMap<EndpointId, u64>>,
    /// Connections to the other machines, for requests sent on to them
    /// (`etd-ssh`, and `Agent`/`ExecOn` for another machine).
    connections: Arc<local::Connections>,
    /// This machine's Logbook archive, if it is one.
    logbook: logbook::Local,
}

/// The longest a single `Foreground` holds pushes back.
const FOREGROUND_MAX: u64 = 120;

impl State {
    pub fn new(paths: Paths, env: Env) -> Arc<Self> {
        Arc::new(Self {
            paths,
            env,
            endpoint: OnceLock::new(),
            live: Mutex::default(),
            tries: Mutex::default(),
            notify: OnceLock::new(),
            foreground: Mutex::default(),
            connections: Arc::default(),
            logbook: logbook::Local::default(),
        })
    }

    /// This machine sends pushes with `command`.
    pub fn set_notify_command(&self, command: String) {
        let _ = self.notify.set(command);
    }

    pub(crate) fn notify_command(&self) -> Option<&str> {
        self.notify.get().map(String::as_str)
    }

    /// A device said the app is open, and that time has not passed.
    pub(crate) fn app_open(&self) -> bool {
        let now = config::now_secs();
        self.foreground.lock().unwrap().values().any(|until| *until > now)
    }

    pub(crate) fn paths(&self) -> &Paths {
        &self.paths
    }

    pub(crate) fn env(&self) -> &Env {
        &self.env
    }

    pub(crate) fn endpoint(&self) -> &Endpoint {
        self.endpoint.get().expect("set by serve")
    }

    pub(crate) fn connections(&self) -> &Arc<local::Connections> {
        &self.connections
    }

    pub(crate) fn logbook(&self) -> &logbook::Local {
        &self.logbook
    }

    fn add_live(&self, id: EndpointId, name: &str, conn: &Connection) {
        let mut live = self.live.lock().unwrap();
        let entry = live.entry(id).or_insert_with(|| (name.to_string(), Vec::new()));
        entry.1.push(conn.clone());
    }

    fn remove_live(&self, id: EndpointId, conn: &Connection) {
        let mut live = self.live.lock().unwrap();
        if let Some((_, conns)) = live.get_mut(&id) {
            conns.retain(|c| c.stable_id() != conn.stable_id());
            if conns.is_empty() {
                live.remove(&id);
            }
        }
    }

    /// False after `PAIR_TRIES` tries within `PAIR_WINDOW`.
    fn may_try_pairing(&self, id: EndpointId) -> bool {
        let mut tries = self.tries.lock().unwrap();
        let list = tries.entry(id).or_default();
        list.retain(|t| t.elapsed() < PAIR_WINDOW);
        if list.len() >= PAIR_TRIES {
            return false;
        }
        list.push(Instant::now());
        true
    }

    /// Closes connections of members that are no longer in the network.
    fn apply_revokes(&self, network: &Network) {
        let mut live = self.live.lock().unwrap();
        live.retain(|id, (name, conns)| {
            let keep = network.member(id).is_some();
            if !keep {
                info!("{name} ({}) revoked: closing {} connection(s)", id.fmt_short(), conns.len());
                for conn in conns.iter() {
                    conn.close(2u32.into(), b"revoked");
                }
            }
            keep
        });
    }

    fn connected_names(&self) -> Vec<String> {
        self.live.lock().unwrap().values().map(|(name, _)| name.clone()).collect()
    }
}

/// Binds the endpoint and serves until SIGINT or SIGTERM.
/// `show_pairing`: print a pairing QR code once online (a first run by hand).
pub async fn run(paths: Paths, key: SecretKey, show_pairing: bool) -> anyhow::Result<()> {
    // One etd per key: a second one (Nix and the app, say) would fight over it.
    let _lock = config::DaemonLock::take(&paths)?;
    let env = tmux::login_env().await;
    let tmux_text = match tmux::find(&env).await {
        TmuxStatus::Found(t) => format!("{} ({}), TMUX_TMPDIR={}", t.path, t.version, t.tmpdir.as_deref().unwrap_or("default")),
        TmuxStatus::NotFound(why) => format!("not found yet: {why}"),
    };
    info!("tmux: {tmux_text}");
    let transport = QuicTransportConfig::builder().keep_alive_interval(KEEP_ALIVE).max_idle_timeout(Some(IDLE_TIMEOUT.try_into()?)).build();
    let endpoint = Endpoint::builder(presets::N0).secret_key(key).alpns(vec![ALPN.to_vec()]).transport_config(transport).bind().await?;
    info!("endpoint id: {}", endpoint.id());
    // The first start makes the network (and moves paired devices into it).
    let signed = network::ensure(&paths, endpoint.id(), &tmux::host_name())?;
    info!("network {}: {} member(s)", signed.network.name, signed.network.members().count());
    let state_paths = paths.clone();
    let has_mm = mm::available(&env);
    let state = State::new(paths, env);
    if let Some(command) = std::env::var("ETD_NOTIFY_COMMAND").ok().filter(|c| !c.trim().is_empty()) {
        state.set_notify_command(command);
    }
    let router = serve(endpoint.clone(), state.clone());
    // Pushes for manymux sessions (events.rs): follow this machine's node.
    if has_mm {
        tokio::spawn(events::follow(state.clone()));
        // The Logbook: agents' transcripts to the archives (logbook/).
        tokio::spawn(logbook::run(state.clone()));
    } else {
        info!("no manymux (mm) here: no pushes for manymux sessions from this machine");
    }
    let status_path = state.paths.status();
    let connections = state.connections.clone();
    let watcher = tokio::spawn(watch(endpoint.clone(), state, tmux_text));
    // etd-ssh (manymux's MM_SSH) reaches the other machines through this.
    let local = local::spawn(endpoint.clone(), state_paths.clone(), connections);
    if tokio::time::timeout(ONLINE_TIMEOUT, endpoint.online()).await.is_err() {
        warn!("no relay after {ONLINE_TIMEOUT:?}; direct connections only until it comes up");
    }
    info!("online; run `etd invite` to add a device, `etd invite --machine` to add a machine");
    if show_pairing {
        if let Err(e) = pair::print(&state_paths, &endpoint.addr(), &tmux::host_name(), Kind::Device, false) {
            warn!("pairing code: {e:#}");
        }
    }

    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    info!("stopping");
    watcher.abort();
    local.abort();
    local::remove(&state_paths);
    let _ = std::fs::remove_file(status_path);
    router.shutdown().await?;
    Ok(())
}

/// Accepts et/1 on `endpoint`. Revokes, status and network sync run in [`watch`].
pub fn serve(endpoint: Endpoint, state: Arc<State>) -> Router {
    let _ = state.endpoint.set(endpoint.clone());
    Router::builder(endpoint).accept(ALPN, Handler(state)).spawn()
}

/// Every `WATCH_EVERY`: applies revokes, sends a changed network document to
/// the other machines (and every 5 minutes anyway), writes `status.json`.
pub async fn watch(endpoint: Endpoint, state: Arc<State>, tmux: String) {
    let mut sent: Option<Signed> = None;
    let mut ticks = 0u32;
    // manymux's machine list follows the network only when asked: until a
    // user's machines are in the network, that list may hold ssh names.
    let manage_mm_hosts = std::env::var("ETD_MANYMUX_HOSTS").is_ok_and(|v| v == "1");
    // A machine that missed the document (down, or restarting for a join)
    // gets it again soon, not only at the 5-minute resend.
    let failed = Arc::new(AtomicBool::new(false));
    let mut retry_at: Option<u32> = None;
    let mut backoff = RETRY_FIRST;
    // A machine that was offline catches up at once: ask the others.
    {
        let (endpoint, paths) = (endpoint.clone(), state.paths.clone());
        tokio::spawn(async move { network::pull(&endpoint, &paths).await });
    }
    loop {
        if failed.swap(false, Ordering::Relaxed) {
            retry_at = Some(ticks.wrapping_add(backoff));
            backoff = (backoff * 2).min(RESEND_EVERY);
        }
        match network::load(&state.paths) {
            Ok(Some(signed)) => {
                state.apply_revokes(&signed.network);
                if sent.as_ref() != Some(&signed) && manage_mm_hosts {
                    let me = endpoint.id();
                    let names: Vec<String> = signed.network.machines().filter(|m| m.id != me).map(|m| m.name.to_string()).collect();
                    match mm::hosts_file(&state.env).map(|path| mm::write_hosts(&path, &names)) {
                        Some(Ok(true)) => info!("manymux machines: {}", names.join(", ")),
                        Some(Err(e)) => warn!("manymux hosts.toml: {e:#}"),
                        _ => {}
                    }
                }
                let changed = sent.as_ref() != Some(&signed);
                let retry_due = retry_at == Some(ticks);
                if changed || retry_due || ticks.is_multiple_of(RESEND_EVERY) {
                    if changed {
                        backoff = RETRY_FIRST;
                    }
                    retry_at = None;
                    let (endpoint, doc, failed, paths) = (endpoint.clone(), signed.clone(), failed.clone(), state.paths.clone());
                    tokio::spawn(async move {
                        if !network::push(&endpoint, &paths, &doc).await {
                            failed.store(true, Ordering::Relaxed);
                        }
                    });
                    sent = Some(signed);
                }
            }
            Ok(None) => {}
            Err(e) => warn!("network: {e:#}"),
        }
        ticks = ticks.wrapping_add(1);
        let status = Status {
            pid: std::process::id(),
            version: env!("CARGO_PKG_VERSION").into(),
            host_name: tmux::host_name(),
            addr: endpoint.addr(),
            tmux: tmux.clone(),
            connected: state.connected_names(),
            updated: config::now_secs(),
            exe: std::env::current_exe().ok().map(|p| p.display().to_string()),
        };
        if let Err(e) = config::write_json(&state.paths.status(), &status) {
            warn!("write status: {e:#}");
        }
        tokio::time::sleep(WATCH_EVERY).await;
    }
}

#[derive(Debug, Clone)]
struct Handler(Arc<State>);

impl ProtocolHandler for Handler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let state = &self.0;
        let id = conn.remote_id();
        // Read on each connection, so joins and revokes apply at once.
        let network = network::load(&state.paths).unwrap_or_else(|e| {
            warn!("{e:#}");
            None
        });
        let member = network.as_ref().and_then(|s| s.network.member(&id)).map(|m| (m.name.to_string(), m.kind));
        let (name, kind) = match member {
            Some(member) => member,
            None => match admission(state, &conn).await {
                Some(member) => member,
                None => return Ok(()),
            },
        };
        info!("{name} ({}) connected", id.fmt_short());
        state.add_live(id, &name, &conn);
        loop {
            let (send, recv) = match conn.accept_bi().await {
                Ok(streams) => streams,
                Err(e) => {
                    info!("{name} disconnected: {e}");
                    break;
                }
            };
            let (state, conn) = (state.clone(), conn.clone());
            tokio::spawn(async move {
                if let Err(e) = serve_stream(&state, &conn, id, kind, send, recv).await {
                    debug!("stream ended: {e:#}");
                }
            });
        }
        state.remove_live(id, &conn);
        Ok(())
    }
}

fn kind_word(kind: Kind) -> &'static str {
    match kind {
        Kind::Machine => "machine",
        Kind::Device => "device",
    }
}

/// Not a member: the first stream must be `Join` (or `Pair`, a device's join)
/// with a valid invite secret. Returns the new member's name and kind;
/// otherwise the connection is closed.
async fn admission(state: &State, conn: &Connection) -> Option<(String, Kind)> {
    let id = conn.remote_id();
    let first = tokio::time::timeout(PAIR_TIMEOUT, async {
        let (send, mut recv) = conn.accept_bi().await.ok()?;
        let request = read_frame::<Request>(&mut recv).await.ok()??;
        Some((send, request))
    })
    .await;
    let Ok(Some((mut send, request))) = first else {
        conn.close(1u32.into(), b"not paired");
        return None;
    };
    // `Pair` (the app's) answers `Paired`; `Join` answers `Joined`.
    let (secret, name, wanted, is_pair) = match request {
        Request::Pair { secret, device_name } => (secret, device_name, Kind::Device, true),
        Request::Join { secret, name, kind } => (secret, name, kind, false),
        _ => {
            info!("refused {}: not paired", id.fmt_short());
            conn.close(1u32.into(), b"not paired");
            return None;
        }
    };
    let me = state.endpoint().id();
    let reply = if !state.may_try_pairing(id) {
        Err("too many tries; wait a minute".to_string())
    } else {
        match pair::redeem(&state.paths, &secret) {
            None => Err("this invite is wrong, used, or expired; run `etd invite` again".to_string()),
            Some(Kind::Device) if wanted == Kind::Machine => {
                Err("this link is for a phone or tablet, not a machine; make one with `etd invite --machine`".to_string())
            }
            Some(kind) if kind != wanted => Err(format!("this invite is for a {}, not a {}", kind_word(kind), kind_word(wanted))),
            Some(kind) => {
                let name = if name.trim().is_empty() { id.fmt_short().to_string() } else { name };
                match network::admit(&state.paths, me, id, &name, kind) {
                    Ok(signed) => {
                        let name = signed.network.member(&id).map(|m| m.name.to_string()).unwrap_or(name);
                        Ok((name, kind, signed))
                    }
                    Err(e) => Err(format!("{e:#}")),
                }
            }
        }
    };
    match reply {
        Ok((name, kind, signed)) => {
            info!("{name} ({}) joined as a {}", id.fmt_short(), kind_word(kind));
            let response = if is_pair {
                Response::Paired { host_name: tmux::host_name() }
            } else {
                // A machine gets the network key: it may change the network.
                let network_key = match kind {
                    Kind::Machine => network::load_key(&state.paths).ok().flatten().map(|k| k.to_bytes()),
                    Kind::Device => None,
                };
                Response::Joined { network: signed, network_key }
            };
            let _ = write_frame(&mut send, &response).await;
            let _ = send.finish();
            Some((name, kind))
        }
        Err(why) => {
            info!("pairing refused for {}: {why}", id.fmt_short());
            let _ = write_frame(&mut send, &Response::Rejected(why)).await;
            let _ = send.finish();
            // Give the reply time to arrive before the close.
            let _ = tokio::time::timeout(Duration::from_secs(2), send.stopped()).await;
            conn.close(1u32.into(), b"pairing refused");
            None
        }
    }
}

async fn serve_stream(
    state: &State,
    conn: &Connection,
    peer: EndpointId,
    kind: Kind,
    mut send: SendStream,
    mut recv: RecvStream,
) -> anyhow::Result<()> {
    let request = match read_frame::<Request>(&mut recv).await {
        Ok(Some(request)) => request,
        Ok(None) => return Ok(()),
        // Most often a request from a newer app or etd: say so.
        Err(e) => {
            let why = format!("etd {} here does not know this request ({e:#}); update etd on this machine", env!("CARGO_PKG_VERSION"));
            let _ = write_frame(&mut send, &Response::Error(why)).await;
            let _ = send.finish();
            return Ok(());
        }
    };
    match &request {
        Request::Upload { path, len } => debug!("request: Upload {path} ({len} bytes)"),
        Request::LogAppend { session_id, offset, bytes, .. } => {
            debug!("request: LogAppend {session_id} at {offset} ({} bytes)", bytes.len())
        }
        Request::Pair { device_name, .. } => debug!("request: Pair from {device_name}"),
        other => debug!("request: {other:?}"),
    }
    let reply = match request {
        Request::Hello { .. } => Response::HostInfo(HostInfo {
            host_name: tmux::host_name(),
            os: std::env::consts::OS.into(),
            etd_version: env!("CARGO_PKG_VERSION").into(),
            // Found again each time: tmux may have started after etd.
            tmux: tmux::find(&state.env).await,
        }),
        Request::ListSessions => match tmux::find(&state.env).await {
            TmuxStatus::Found(t) => match tmux::list_sessions(&state.env, &t).await {
                Ok(sessions) => Response::Sessions(sessions),
                Err(e) => Response::Error(format!("{e:#}")),
            },
            TmuxStatus::NotFound(why) => Response::Error(format!("tmux not found: {why}")),
        },
        Request::Control { target } => return control(state, target, send, recv).await,
        Request::Ping => Response::Pong,
        // Already paired.
        Request::Pair { .. } => Response::Paired { host_name: tmux::host_name() },
        Request::Exec { script } => ops::exec(&state.env, &script).await,
        Request::Upload { path, len } => ops::upload(&state.env, &path, len, &mut recv).await,
        Request::Agent { host: None } => {
            return pipe(state, mm::agent_local(&state.env), Response::AgentStarted, "mm agent on this machine", send, recv).await
        }
        // Another machine of the network: sent on over iroh.
        request @ (Request::Agent { host: Some(_) } | Request::ExecOn { .. }) => {
            return local::forward(request, recv, send, state.endpoint(), &state.paths, &state.connections).await
        }
        Request::Join { .. } => Response::Error("already a member of this network".into()),
        Request::Network => match network::load(&state.paths) {
            Ok(Some(signed)) => {
                // A machine asks at its start. If it missed a key change, the
                // document alone is no use to it: send it the change as well.
                if kind == Kind::Machine {
                    let (endpoint, paths) = (state.endpoint().clone(), state.paths.clone());
                    tokio::spawn(async move {
                        if let Err(e) = network::push_to(&endpoint, &paths, peer).await {
                            debug!("network not sent to {}: {e:#}", peer.fmt_short());
                        }
                    });
                }
                Response::NetworkDoc(signed)
            }
            Ok(None) => Response::Error("no network yet".into()),
            Err(e) => Response::Error(format!("{e:#}")),
        },
        // Only machines change the network (D1).
        Request::NetworkUpdate { .. } if kind != Kind::Machine => Response::Error("only machines send network updates".into()),
        Request::NetworkUpdate { network } => match network::apply(&state.paths, &network) {
            Ok(changed) => {
                if changed {
                    info!("network updated by {}", peer.fmt_short());
                }
                Response::Pong
            }
            Err(e) => Response::Error(format!("{e:#}")),
        },
        Request::NetworkRotate { .. } if kind != Kind::Machine => Response::Error("only machines change the network key".into()),
        Request::NetworkRotate { chain, key } => match network::apply_rotation(&state.paths, state.endpoint().id(), &chain, &key) {
            Ok(changed) => {
                if changed {
                    info!("network key changed by {}", peer.fmt_short());
                }
                Response::Pong
            }
            Err(e) => {
                warn!("network key change from {} refused: {e:#}", peer.fmt_short());
                Response::Error(format!("{e:#}"))
            }
        },
        Request::Notify { .. } if kind != Kind::Machine => Response::Error("only machines send notes".into()),
        Request::Notify { note } => {
            debug!("note from {}: {} {}/{}", peer.fmt_short(), note.kind, note.machine, note.session);
            events::deliver(state, &note).await;
            Response::Pong
        }
        Request::Foreground { until } => {
            let until = until.min(config::now_secs() + FOREGROUND_MAX);
            state.foreground.lock().unwrap().insert(peer, until);
            Response::Pong
        }
        Request::Invite { kind } => match pair::invite_url(&state.paths, &state.endpoint().addr(), &tmux::host_name(), kind) {
            Ok(url) => Response::Invitation { url },
            Err(e) => Response::Error(format!("{e:#}")),
        },
        Request::Machines => match mm::machines(&state.env).await {
            Ok(list) => Response::Machines(list),
            Err(e) => Response::Error(format!("{e:#}")),
        },
        Request::Deliver { hash, session } => return deliver(state, conn, peer, hash, session, send).await,
        Request::Send { .. } => Response::Error("Send is for the local socket (`etd send`)".into()),
        Request::Shared => Response::SharedList(files::shared(&state.paths, &member_name(state, state.endpoint().id()))),
        Request::Offer { path } => return offer(state, &path, send, recv).await,
        Request::Blobs => {
            let files = state.connections.files.get(&state.paths).await?;
            return files.serve_stream(conn.stable_id() as u64, recv, send).await;
        }
        Request::Share { path, to, session } => return share(state, &path, &to, session, send).await,
        Request::Unshare { path } => match files::downloads(&state.env).and_then(|d| files::unshare(&state.paths, &d, &path)) {
            Ok(()) => {
                info!("{} deleted {path}", member_name(state, peer));
                Response::Pong
            }
            Err(e) => Response::Error(format!("{e:#}")),
        },
        Request::LogInfo => logbook::info(&state.paths, &state.logbook, &member_name(state, peer)).await,
        Request::LogAppend { .. } if kind != Kind::Machine => Response::Error("only machines send transcripts".into()),
        Request::LogAppend { session_id, meta, offset, bytes } => {
            logbook::append(&state.paths, &state.logbook, &member_name(state, peer), &session_id, meta, offset, &bytes).await
        }
        Request::LogArchives => logbook::serve::archive_names(state).await,
        Request::LogSessions { since } => logbook::serve::sessions(state, since).await,
        Request::LogRead { machine, session_id, from } => {
            return logbook::serve::follow(state, &machine, &session_id, from, send, recv).await
        }
        Request::LogPause { machine, mm_session, paused } => logbook::serve::pause(state, &machine, &mm_session, paused).await,
        Request::Report { refresh } => return logbook::watcher::serve(state, refresh, send).await,
        Request::Grouping { change_json } => logbook::watcher::grouping(state, &change_json).await,
        Request::LogSend { selection, to, session } => return log_send(state, selection, &to, session, send).await,
    };
    write_frame(&mut send, &reply).await?;
    send.finish()?;
    Ok(())
}

/// Fetches a collection from the member at the other end of `conn` into
/// `~/etd-downloads/` (files.rs): `Progress` frames, then `Delivered`.
async fn deliver(
    state: &State,
    conn: &Connection,
    peer: EndpointId,
    hash: [u8; 32],
    session: Option<String>,
    mut send: SendStream,
) -> anyhow::Result<()> {
    let hash = iroh_blobs::Hash::from_bytes(hash);
    let delivered = async {
        let files = state.connections.files.get(&state.paths).await?;
        let downloads = files::downloads(&state.env)?;
        files::deliver(&files, conn, hash, &downloads, &mut send).await
    }
    .await;
    let reply = match delivered {
        Ok(tops) => {
            let paths: Vec<String> = tops.iter().map(|p| p.display().to_string()).collect();
            let from = member_name(state, peer);
            info!("files from {from}: {}", paths.join(", "));
            if let Err(e) = files::log_delivery(&state.paths, &tops, &from, session.as_deref()) {
                warn!("delivery log: {e:#}");
            }
            let mut typed = false;
            if let Some(session) = session {
                match mm::type_into(&state.env, &session, &mm::typed_paths(&paths)).await {
                    Ok(()) => typed = true,
                    Err(e) => warn!("paths not typed into {session}: {e:#}"),
                }
            }
            Response::Delivered { paths, typed }
        }
        Err(e) => {
            warn!("files from {}: {e:#}", peer.fmt_short());
            Response::Error(format!("{e:#}"))
        }
    };
    write_frame(&mut send, &reply).await?;
    send.finish()?;
    Ok(())
}

/// The network's name for a member, else its short id.
fn member_name(state: &State, id: EndpointId) -> String {
    network::load(&state.paths)
        .ok()
        .flatten()
        .and_then(|s| s.network.member(&id).map(|m| m.name.to_string()))
        .unwrap_or_else(|| id.fmt_short().to_string())
}

/// `Offer`: offers a shared file or folder for `Blobs` until the asker
/// closes this stream.
async fn offer(state: &State, path: &str, mut send: SendStream, mut recv: RecvStream) -> anyhow::Result<()> {
    let offered = async {
        let full = files::inside(&files::downloads(&state.env)?, path)?;
        let files = state.connections.files.get(&state.paths).await?;
        files.import(&[full]).await
    }
    .await;
    let outgoing = match offered {
        Ok(outgoing) => outgoing,
        Err(e) => {
            write_frame(&mut send, &Response::Error(format!("{e:#}"))).await?;
            send.finish()?;
            return Ok(());
        }
    };
    write_frame(&mut send, &Response::Offered { hash: *outgoing.hash().as_bytes(), total: outgoing.total }).await?;
    // Held until the asker finishes or drops the stream (at most an hour).
    let _ = tokio::time::timeout(Duration::from_secs(3600), recv.read_to_end(1024)).await;
    drop(outgoing);
    let _ = send.finish();
    Ok(())
}

/// `Share`: type a shared path into a session here, or send it to another
/// machine (typed into `session` there).
async fn share(state: &State, path: &str, to: &str, session: Option<String>, mut send: SendStream) -> anyhow::Result<()> {
    let me = member_name(state, state.endpoint().id());
    let shared = async {
        let full = files::inside(&files::downloads(&state.env)?, path)?;
        if to == me {
            let session = session.context("say which session to type into")?;
            let paths = vec![full.display().to_string()];
            mm::type_into(&state.env, &session, &mm::typed_paths(&paths)).await?;
            return Ok(Response::Delivered { paths, typed: true });
        }
        local::send_files(state.endpoint(), &state.paths, &state.connections, &[full], to, session, &mut send).await
    }
    .await;
    let reply = shared.unwrap_or_else(|e| Response::Error(format!("{e:#}")));
    write_frame(&mut send, &reply).await?;
    send.finish()?;
    Ok(())
}

/// `LogSend`: a part of a transcript as a Markdown file on the machine `to`,
/// typed into `session` there (empty: not typed). As `Share`.
async fn log_send(
    state: &State,
    selection: et_proto::logbook::LogSelection,
    to: &str,
    session: String,
    mut send: SendStream,
) -> anyhow::Result<()> {
    let me = member_name(state, state.endpoint().id());
    let session = Some(session).filter(|s| !s.is_empty());
    let sent = async {
        let (name, doc) = logbook::serve::markdown(state, &selection).await?;
        if to == me {
            let dir = files::downloads(&state.env)?;
            std::fs::create_dir_all(&dir)?;
            let path = dir.join(name);
            std::fs::write(&path, doc)?;
            let paths = vec![path.display().to_string()];
            let typed = match session {
                Some(session) => {
                    mm::type_into(&state.env, &session, &mm::typed_paths(&paths)).await?;
                    true
                }
                None => false,
            };
            return Ok(Response::Delivered { paths, typed });
        }
        // Sent from a temporary copy: the target keeps the file.
        let dir = state.paths.state.join("logbook-send");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(name);
        std::fs::write(&path, doc)?;
        let delivered =
            local::send_files(state.endpoint(), &state.paths, &state.connections, std::slice::from_ref(&path), to, session, &mut send)
                .await;
        let _ = std::fs::remove_file(&path);
        delivered
    }
    .await;
    let reply = sent.unwrap_or_else(|e| Response::Error(format!("{e:#}")));
    write_frame(&mut send, &reply).await?;
    send.finish()?;
    Ok(())
}

/// `tmux -C` without a PTY (et-ios SPEC.md §2.1 step 4). When the phone closes
/// its side, tmux's stdin closes and tmux detaches; the session keeps running.
async fn control(state: &State, target: Target, mut send: SendStream, recv: RecvStream) -> anyhow::Result<()> {
    let tmux = match tmux::find(&state.env).await {
        TmuxStatus::Found(t) => t,
        TmuxStatus::NotFound(why) => {
            write_frame(&mut send, &Response::Error(format!("tmux not found: {why}"))).await?;
            send.finish()?;
            return Ok(());
        }
    };
    let mut cmd = tmux::command(&state.env, &tmux);
    match &target {
        Target::Attach(name) => cmd.args(["-C", "attach-session", "-t", &format!("={name}")]),
        Target::AttachOrCreate(name) => cmd.args(["-C", "new-session", "-A", "-s", name]),
    };
    // TERM=et-ios marks E.T.'s control clients (#{client_termname}).
    cmd.env("TERM", "et-ios");
    pipe(state, cmd, Response::ControlStarted, &format!("tmux -C {target:?}"), send, recv).await
}

/// Runs `cmd` with its stdin fed from the phone, and its stdout and stderr
/// sent down as `ControlFrame`s, then `Exit`. When the phone closes its side,
/// stdin closes: tmux detaches, `mm agent` ends; the sessions keep running.
async fn pipe(
    state: &State,
    mut cmd: Command,
    started: Response,
    label: &str,
    mut send: SendStream,
    mut recv: RecvStream,
) -> anyhow::Result<()> {
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
    if let Ok(home) = ops::home(&state.env) {
        cmd.current_dir(home);
    }
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            write_frame(&mut send, &Response::Error(format!("start {label}: {e}"))).await?;
            send.finish()?;
            return Ok(());
        }
    };
    write_frame(&mut send, &started).await?;
    info!("{label} started");

    let mut stdin = child.stdin.take().context("stdin")?;
    let mut stdout = child.stdout.take().context("stdout")?;
    let stderr = child.stderr.take().context("stderr")?;
    let (frames, mut frames_rx) = mpsc::channel::<ControlFrame>(64);

    // Phone → process. Ends when the phone finishes or drops the stream.
    let up = tokio::spawn(async move {
        let mut buf = vec![0u8; READ_CHUNK];
        while let Ok(Some(n)) = recv.read(&mut buf).await {
            if stdin.write_all(&buf[..n]).await.is_err() {
                break;
            }
        }
        // Dropping stdin sends EOF: tmux detaches, mm agent ends.
    });
    let out_frames = frames.clone();
    let out = tokio::spawn(async move {
        let mut buf = vec![0u8; READ_CHUNK];
        while let Ok(n) = stdout.read(&mut buf).await {
            if n == 0 || out_frames.send(ControlFrame::Stdout(buf[..n].to_vec())).await.is_err() {
                break;
            }
        }
    });
    let err_frames = frames.clone();
    let err = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if err_frames.send(ControlFrame::Stderr(line)).await.is_err() {
                break;
            }
        }
    });
    drop(frames);
    let writer = tokio::spawn(async move {
        while let Some(frame) = frames_rx.recv().await {
            if write_frame(&mut send, &frame).await.is_err() {
                return None;
            }
        }
        Some(send)
    });

    let _ = out.await;
    let _ = err.await;
    let status = child.wait().await?;
    up.abort();
    info!("{label} exited: {status}");
    if let Ok(Some(mut send)) = writer.await {
        write_frame(&mut send, &ControlFrame::Exit(status.code())).await?;
        send.finish()?;
    }
    Ok(())
}
