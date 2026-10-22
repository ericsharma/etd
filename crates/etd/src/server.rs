//! The iroh side: pair devices, accept paired devices, serve et/1 streams.
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
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
use crate::{mm, ops, pair};

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

#[derive(Debug)]
pub struct State {
    paths: Paths,
    env: Env,
    /// Open connections of paired devices, for revokes and `etd status`.
    live: Mutex<HashMap<EndpointId, (String, Vec<Connection>)>>,
    /// Recent `Pair` tries, per device.
    tries: Mutex<HashMap<EndpointId, Vec<Instant>>>,
}

impl State {
    pub fn new(paths: Paths, env: Env) -> Arc<Self> {
        Arc::new(Self { paths, env, live: Mutex::default(), tries: Mutex::default() })
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

    /// Closes connections of devices that are no longer in `clients.json`.
    fn apply_revokes(&self) {
        let Ok(clients) = config::load_clients(&self.paths) else { return };
        let mut live = self.live.lock().unwrap();
        live.retain(|id, (name, conns)| {
            let keep = clients.iter().any(|c| c.id == *id);
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
    let state_paths = paths.clone();
    let state = State::new(paths, env);
    let router = serve(endpoint.clone(), state.clone());
    let status_path = state.paths.status();
    let watcher = tokio::spawn(watch(endpoint.clone(), state, tmux_text));
    if tokio::time::timeout(ONLINE_TIMEOUT, endpoint.online()).await.is_err() {
        warn!("no relay after {ONLINE_TIMEOUT:?}; direct connections only until it comes up");
    }
    info!("online; run `etd pair` to add a device");
    if show_pairing {
        if let Err(e) = pair::print(&state_paths, &endpoint.addr(), &tmux::host_name(), false) {
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
    let _ = std::fs::remove_file(status_path);
    router.shutdown().await?;
    Ok(())
}

/// Accepts et/1 on `endpoint`. Revokes and status run in [`watch`].
pub fn serve(endpoint: Endpoint, state: Arc<State>) -> Router {
    Router::builder(endpoint).accept(ALPN, Handler(state)).spawn()
}

/// Applies revokes and writes `status.json`, every `WATCH_EVERY`.
pub async fn watch(endpoint: Endpoint, state: Arc<State>, tmux: String) {
    loop {
        state.apply_revokes();
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
        // Read on each connection, so pairings and revokes apply at once.
        let clients = config::load_clients(&state.paths).unwrap_or_else(|e| {
            warn!("{e:#}");
            Vec::new()
        });
        let name = match clients.into_iter().find(|c| c.id == id) {
            Some(client) => client.name,
            None => match pairing(state, &conn).await {
                Some(name) => name,
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
                if let Err(e) = serve_stream(&state, send, recv).await {
                    debug!("stream ended: {e:#}");
                }
            });
        }
        state.remove_live(id, &conn);
        Ok(())
    }
}

/// An unpaired device: its first stream must be a valid `Pair`. Returns the
/// device name when it is now paired; otherwise the connection is closed.
async fn pairing(state: &State, conn: &Connection) -> Option<String> {
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
    let reply = match request {
        Request::Pair { secret, device_name } => {
            if !state.may_try_pairing(id) {
                Err("too many tries; wait a minute".to_string())
            } else if !pair::redeem(&state.paths, &secret) {
                Err("this pairing code is wrong, used, or expired; run `etd pair` again".to_string())
            } else {
                let name = if device_name.trim().is_empty() { id.fmt_short().to_string() } else { device_name.trim().to_string() };
                match config::add_client(&state.paths, id, &name) {
                    Ok(()) => Ok(name),
                    Err(e) => Err(format!("save device: {e:#}")),
                }
            }
        }
        _ => {
            info!("refused {}: not paired", id.fmt_short());
            conn.close(1u32.into(), b"not paired");
            return None;
        }
    };
    match reply {
        Ok(name) => {
            info!("paired {name} ({})", id.fmt_short());
            let _ = write_frame(&mut send, &Response::Paired { host_name: tmux::host_name() }).await;
            let _ = send.finish();
            Some(name)
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

async fn serve_stream(state: &State, mut send: SendStream, mut recv: RecvStream) -> anyhow::Result<()> {
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
