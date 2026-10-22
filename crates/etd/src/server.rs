//! The iroh side: pair devices, accept paired devices, serve et/1 streams.
use std::collections::HashMap;
use std::process::Stdio;
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
use crate::{events, local, mm, network, ops, pair};

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
    let env = tmux::login_env().await;
    let tmux_text = match tmux::find(&env).await {
        TmuxStatus::Found(t) => format!("{} ({}), TMUX_TMPDIR={}", t.path, t.version, t.tmpdir.as_deref().unwrap_or("default")),
        TmuxStatus::NotFound(why) => format!("not found yet: {why}"),
    };
    info!("tmux: {tmux_text}");
    let transport = QuicTransportConfig::builder()
        .keep_alive_interval(KEEP_ALIVE)
        .max_idle_timeout(Some(IDLE_TIMEOUT.try_into()?))
        .build();
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(key)
        .alpns(vec![ALPN.to_vec()])
        .transport_config(transport)
        .bind()
        .await?;
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
    } else {
        info!("no manymux (mm) here: no pushes for manymux sessions from this machine");
    }
    let status_path = state.paths.status();
    let watcher = tokio::spawn(watch(endpoint.clone(), state, tmux_text));
    // etd-ssh (manymux's MM_SSH) reaches the other machines through this.
    let local = local::spawn(endpoint.clone(), state_paths.clone());
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
    loop {
        match network::load(&state.paths) {
            Ok(Some(signed)) => {
                state.apply_revokes(&signed.network);
                if sent.as_ref() != Some(&signed) && manage_mm_hosts {
                    let me = endpoint.id();
                    let names: Vec<String> =
                        signed.network.machines().filter(|m| m.id != me).map(|m| m.name.to_string()).collect();
                    match mm::hosts_file(&state.env).map(|path| mm::write_hosts(&path, &names)) {
                        Some(Ok(true)) => info!("manymux machines: {}", names.join(", ")),
                        Some(Err(e)) => warn!("manymux hosts.toml: {e:#}"),
                        _ => {}
                    }
                }
                if sent.as_ref() != Some(&signed) || ticks.is_multiple_of(RESEND_EVERY) {
                    let endpoint = endpoint.clone();
                    let doc = signed.clone();
                    tokio::spawn(async move { network::push(&endpoint, &doc).await });
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
            let state = state.clone();
            tokio::spawn(async move {
                if let Err(e) = serve_stream(&state, id, kind, send, recv).await {
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
            Some(kind) if kind != wanted => {
                Err(format!("this invite is for a {}, not a {}", kind_word(kind), kind_word(wanted)))
            }
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

async fn serve_stream(state: &State, peer: EndpointId, kind: Kind, mut send: SendStream, mut recv: RecvStream) -> anyhow::Result<()> {
    let Some(request) = read_frame::<Request>(&mut recv).await? else { return Ok(()) };
    match &request {
        Request::Upload { path, len } => debug!("request: Upload {path} ({len} bytes)"),
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
        Request::Agent { host } => return agent(state, host, send, recv).await,
        Request::ExecOn { host, script } => match mm::check_host(&state.env, &host).await {
            Ok(()) => ops::exec(&state.env, &mm::exec_on_script(&state.env, &host, &script)).await,
            Err(e) => Response::Error(format!("{e:#}")),
        },
        Request::Join { .. } => Response::Error("already a member of this network".into()),
        Request::Network => match network::load(&state.paths) {
            Ok(Some(signed)) => Response::NetworkDoc(signed),
            Ok(None) => Response::Error("no network yet".into()),
            Err(e) => Response::Error(format!("{e:#}")),
        },
        // Only machines change the network (D1).
        Request::NetworkUpdate { .. } if kind != Kind::Machine => {
            Response::Error("only machines send network updates".into())
        }
        Request::NetworkUpdate { network } => match network::apply(&state.paths, &network) {
            Ok(changed) => {
                if changed {
                    info!("network updated by {}", peer.fmt_short());
                }
                Response::Pong
            }
            Err(e) => Response::Error(format!("{e:#}")),
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
        Request::Invite { kind } => {
            match pair::invite_url(&state.paths, &state.endpoint().addr(), &tmux::host_name(), kind) {
                Ok(url) => Response::Invitation { url },
                Err(e) => Response::Error(format!("{e:#}")),
            }
        }
        Request::Machines => match mm::machines(&state.env).await {
            Ok(list) => Response::Machines(list),
            Err(e) => Response::Error(format!("{e:#}")),
        },
    };
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

/// `mm agent` here, or `ssh <host> mm agent` for a machine the node watches.
/// etd does not read the manymux protocol; it only moves the bytes.
async fn agent(state: &State, host: Option<String>, mut send: SendStream, recv: RecvStream) -> anyhow::Result<()> {
    let label = format!("mm agent on {}", host.as_deref().unwrap_or("this machine"));
    let cmd = match host {
        None => mm::agent_local(&state.env),
        Some(host) => match mm::check_host(&state.env, &host).await {
            Ok(()) => mm::agent_remote(&state.env, &host),
            Err(e) => {
                write_frame(&mut send, &Response::Error(format!("{e:#}"))).await?;
                send.finish()?;
                return Ok(());
            }
        },
    };
    pipe(state, cmd, Response::AgentStarted, &label, send, recv).await
}

/// Runs `cmd` with its stdin fed from the phone, and its stdout and stderr
/// sent down as `ControlFrame`s, then `Exit`. When the phone closes its side,
/// stdin closes: tmux detaches, `mm agent` ends; the sessions keep running.
async fn pipe(state: &State, mut cmd: Command, started: Response, label: &str, mut send: SendStream, mut recv: RecvStream) -> anyhow::Result<()> {
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
