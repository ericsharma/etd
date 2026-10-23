//! The local socket: how `etd-ssh` (manymux's `MM_SSH`) reaches the other
//! machines of the network through this machine's etd, which keeps its iroh
//! connections open. The socket is in etd's state directory, mode 0600, so
//! only this user can use it.
//!
//! The same et/1 frames as over iroh, with two requests:
//! - `Agent { host: Some(name) }`: a pipe to `mm agent` on the machine
//!   `name`. After `AgentStarted`, `ControlFrame`s come back and raw bytes go.
//! - `ExecOn { host: name, script }`: `Exec` on that machine.
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context};
use et_proto::network::Kind;
use et_proto::{read_frame, write_frame, ControlFrame, Request, Response, ALPN, MAX_FRAME};
use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointId};
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::config::Paths;
use crate::network;

/// `$ETD_SOCKET`, else `etd.sock` in etd's state directory.
pub fn socket_path(paths: &Paths) -> PathBuf {
    std::env::var_os("ETD_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| paths.state.join("etd.sock"))
}

/// A frame (as `et_proto::encode`) to any writer.
pub async fn send<W: AsyncWrite + Unpin, T: Serialize>(w: &mut W, value: &T) -> anyhow::Result<()> {
    w.write_all(&et_proto::encode(value)?).await?;
    Ok(())
}

/// A frame from any reader. `Ok(None)`: the stream ended cleanly first.
pub async fn receive<R: AsyncRead + Unpin, T: DeserializeOwned>(
    r: &mut R,
) -> anyhow::Result<Option<T>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        bail!("frame too large: {len} bytes");
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    Ok(Some(postcard::from_bytes(&body)?))
}

/// Open iroh connections to other machines, reused across requests.
#[derive(Debug, Default)]
struct Connections(Mutex<HashMap<EndpointId, Connection>>);

impl Connections {
    async fn to(&self, endpoint: &Endpoint, id: EndpointId) -> anyhow::Result<Connection> {
        let mut map = self.0.lock().await;
        if let Some(conn) = map.get(&id) {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
        }
        let conn = endpoint
            .connect(id, ALPN)
            .await
            .context("reach the machine")?;
        map.insert(id, conn.clone());
        Ok(conn)
    }
}

/// Serves the local socket until the task is dropped.
pub async fn serve(endpoint: Endpoint, paths: Paths) -> anyhow::Result<()> {
    let path = socket_path(&paths);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    let _ = std::fs::remove_file(&path);
    let listener =
        UnixListener::bind(&path).with_context(|| format!("listen on {}", path.display()))?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    let connections = Arc::new(Connections::default());
    loop {
        let (stream, _) = listener.accept().await?;
        let (endpoint, paths, connections) = (endpoint.clone(), paths.clone(), connections.clone());
        tokio::spawn(async move {
            if let Err(e) = handle(stream, &endpoint, &paths, &connections).await {
                debug!("local request: {e:#}");
            }
        });
    }
}

/// The machine member called `name`.
fn machine(paths: &Paths, me: EndpointId, name: &str) -> anyhow::Result<EndpointId> {
    let signed = network::load(paths)?.context("no network yet")?;
    let found = signed
        .network
        .machines()
        .find(|m| m.name == name)
        .with_context(|| {
            let names: Vec<String> = signed
                .network
                .machines()
                .map(|m| m.name.to_string())
                .collect();
            format!(
                "{name} is not a machine of the network (machines: {})",
                names.join(", ")
            )
        })?;
    if found.id == me {
        bail!("{name} is this machine");
    }
    debug_assert_eq!(found.kind, Kind::Machine);
    Ok(found.id)
}

async fn handle(
    mut stream: UnixStream,
    endpoint: &Endpoint,
    paths: &Paths,
    connections: &Connections,
) -> anyhow::Result<()> {
    let Some(request) = receive::<_, Request>(&mut stream).await? else {
        return Ok(());
    };
    let (host, remote) = match request {
        Request::Agent { host: Some(host) } => (host, Request::Agent { host: None }),
        Request::ExecOn { host, script } => (host, Request::Exec { script }),
        other => {
            send(
                &mut stream,
                &Response::Error(format!("not a local request: {other:?}")),
            )
            .await?;
            return Ok(());
        }
    };
    let id = match machine(paths, endpoint.id(), &host) {
        Ok(id) => id,
        Err(e) => {
            send(&mut stream, &Response::Error(format!("{e:#}"))).await?;
            return Ok(());
        }
    };
    let is_agent = matches!(remote, Request::Agent { .. });
    let opened = async {
        let conn = connections.to(endpoint, id).await?;
        let (mut tx, mut rx) = conn.open_bi().await?;
        write_frame(&mut tx, &remote).await?;
        if !is_agent {
            tx.finish()?;
        }
        let reply = read_frame::<Response>(&mut rx)
            .await?
            .context("no answer")?;
        anyhow::Ok((tx, rx, reply))
    }
    .await;
    let (mut tx, mut rx, reply) = match opened {
        Ok(opened) => opened,
        Err(e) => {
            send(&mut stream, &Response::Error(format!("{host}: {e:#}"))).await?;
            return Ok(());
        }
    };
    let started = matches!(reply, Response::AgentStarted);
    send(&mut stream, &reply).await?;
    if !started {
        return Ok(());
    }
    // The pipe: frames down as they come, raw bytes up.
    let (mut local_rx, mut local_tx) = stream.into_split();
    let up = tokio::spawn(async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match local_rx.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if tx.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = tx.finish();
    });
    while let Some(frame) = read_frame::<ControlFrame>(&mut rx).await? {
        let last = matches!(frame, ControlFrame::Exit(_));
        if send(&mut local_tx, &frame).await.is_err() {
            break;
        }
        if last {
            break;
        }
    }
    up.abort();
    Ok(())
}

/// Starts the socket server next to the daemon; errors are logged.
pub fn spawn(endpoint: Endpoint, paths: Paths) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = serve(endpoint, paths).await {
            warn!("local socket: {e:#}");
        }
    })
}

/// Removes the socket file (at stop).
pub fn remove(paths: &Paths) {
    let path: &Path = &socket_path(paths);
    let _ = std::fs::remove_file(path);
}
