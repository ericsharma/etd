//! The iroh side: accept paired devices, serve et/1 streams.
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use et_proto::{read_frame, write_frame, ControlFrame, HostInfo, Request, Response, Target, TmuxStatus, ALPN};
use iroh::endpoint::{presets, Connection, QuicTransportConfig, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, SecretKey};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config;
use crate::tmux::{self, Env};

const KEEP_ALIVE: Duration = Duration::from_secs(5);
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const ONLINE_TIMEOUT: Duration = Duration::from_secs(15);
const READ_CHUNK: usize = 64 * 1024;

#[derive(Debug)]
struct State {
    home: PathBuf,
    env: Env,
}

#[derive(Debug, Clone)]
struct Handler(Arc<State>);

pub async fn run(home: PathBuf, key: SecretKey) -> anyhow::Result<()> {
    let env = tmux::login_env().await;
    match tmux::find(&env).await {
        TmuxStatus::Found(t) => info!("tmux: {} ({}), TMUX_TMPDIR={}", t.path, t.version, t.tmpdir.as_deref().unwrap_or("default")),
        TmuxStatus::NotFound(why) => warn!("tmux not found yet: {why}"),
    }
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
    let router = Router::builder(endpoint.clone()).accept(ALPN, Handler(Arc::new(State { home, env }))).spawn();
    if tokio::time::timeout(ONLINE_TIMEOUT, endpoint.online()).await.is_err() {
        warn!("no relay after {ONLINE_TIMEOUT:?}; direct connections only until it comes up");
    }
    info!("addr: {:?}", endpoint.addr());
    tokio::signal::ctrl_c().await?;
    info!("stopping");
    router.shutdown().await?;
    Ok(())
}

impl ProtocolHandler for Handler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let id = conn.remote_id();
        // Read on each connection, so `etd allow` and revokes apply at once.
        let clients = config::load_clients(&self.0.home).unwrap_or_else(|e| {
            warn!("{e:#}");
            Vec::new()
        });
        let Some(client) = clients.into_iter().find(|c| c.id == id) else {
            info!("refused {}: not paired", id.fmt_short());
            conn.close(1u32.into(), b"not paired");
            return Ok(());
        };
        info!("{} ({}) connected", client.name, id.fmt_short());
        loop {
            let (send, recv) = match conn.accept_bi().await {
                Ok(streams) => streams,
                Err(e) => {
                    info!("{} disconnected: {e}", client.name);
                    break;
                }
            };
            let state = self.0.clone();
            tokio::spawn(async move {
                if let Err(e) = serve(&state, send, recv).await {
                    debug!("stream ended: {e:#}");
                }
            });
        }
        Ok(())
    }
}

async fn serve(state: &State, mut send: SendStream, mut recv: RecvStream) -> anyhow::Result<()> {
    let Some(request) = read_frame::<Request>(&mut recv).await? else { return Ok(()) };
    debug!("request: {request:?}");
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
    };
    write_frame(&mut send, &reply).await?;
    send.finish()?;
    Ok(())
}

/// `tmux -C` without a PTY (et-ios SPEC.md §2.1 step 4). When the phone closes
/// its side, tmux's stdin closes and tmux detaches; the session keeps running.
async fn control(state: &State, target: Target, mut send: SendStream, mut recv: RecvStream) -> anyhow::Result<()> {
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
    cmd.env("TERM", "et-ios").stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            write_frame(&mut send, &Response::Error(format!("start tmux: {e}"))).await?;
            send.finish()?;
            return Ok(());
        }
    };
    write_frame(&mut send, &Response::ControlStarted).await?;
    info!("tmux -C {target:?} started");

    let mut stdin = child.stdin.take().context("tmux stdin")?;
    let mut stdout = child.stdout.take().context("tmux stdout")?;
    let stderr = child.stderr.take().context("tmux stderr")?;
    let (frames, mut frames_rx) = mpsc::channel::<ControlFrame>(64);

    // Phone → tmux. Ends when the phone finishes or drops the stream.
    let up = tokio::spawn(async move {
        let mut buf = vec![0u8; READ_CHUNK];
        while let Ok(Some(n)) = recv.read(&mut buf).await {
            if stdin.write_all(&buf[..n]).await.is_err() {
                break;
            }
        }
        // Dropping stdin sends EOF: tmux detaches.
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
    info!("tmux -C exited: {status}");
    if let Ok(Some(mut send)) = writer.await {
        write_frame(&mut send, &ControlFrame::Exit(status.code())).await?;
        send.finish()?;
    }
    Ok(())
}
