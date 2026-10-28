//! Requests for another machine of the network, sent on through this
//! machine's etd, which keeps its iroh connections open ([`forward`]):
//! - `Agent { host: Some(name) }`: a pipe to `mm agent` on the machine
//!   `name`. After `AgentStarted`, `ControlFrame`s come back and raw bytes go.
//! - `ExecOn { host: name, script }`: `Exec` on that machine.
//! - `Send { paths, to, session }` (`etd send`, socket only): files to that
//!   machine (files.rs).
//!
//! They come from the local socket (`etd-ssh`, manymux's `MM_SSH`), with the
//! same et/1 frames as over iroh, or from a member over iroh. The socket is
//! in etd's state directory, mode 0600, so only this user can use it.
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
use crate::{files, network};

/// `$ETD_SOCKET`, else `etd.sock` in etd's state directory.
pub fn socket_path(paths: &Paths) -> PathBuf {
    std::env::var_os("ETD_SOCKET").map(PathBuf::from).unwrap_or_else(|| paths.state.join("etd.sock"))
}

/// A frame (as `et_proto::encode`) to any writer.
pub async fn send<W: AsyncWrite + Unpin, T: Serialize>(w: &mut W, value: &T) -> anyhow::Result<()> {
    w.write_all(&et_proto::encode(value)?).await?;
    Ok(())
}

/// A frame from any reader. `Ok(None)`: the stream ended cleanly first.
pub async fn receive<R: AsyncRead + Unpin, T: DeserializeOwned>(r: &mut R) -> anyhow::Result<Option<T>> {
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

/// Open iroh connections to other machines, reused across requests, and
/// the file store that sends over them.
#[derive(Debug, Default)]
pub struct Connections {
    open: Mutex<HashMap<EndpointId, Connection>>,
    pub files: files::Lazy,
}

/// A machine that is off must not hold `etd-ssh` up for long.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

impl Connections {
    pub(crate) async fn to(&self, endpoint: &Endpoint, id: EndpointId) -> anyhow::Result<Connection> {
        if let Some(conn) = self.open.lock().await.get(&id).filter(|c| c.close_reason().is_none()) {
            return Ok(conn.clone());
        }
        // Not under the lock: a machine that is off would hold up the others.
        let conn = tokio::time::timeout(CONNECT_TIMEOUT, endpoint.connect(id, ALPN))
            .await
            .context("reach the machine: timed out")?
            .context("reach the machine")?;
        self.open.lock().await.insert(id, conn.clone());
        Ok(conn)
    }
}

/// Serves the local socket until the task is dropped.
pub async fn serve(endpoint: Endpoint, paths: Paths, connections: Arc<Connections>) -> anyhow::Result<()> {
    let path = socket_path(&paths);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).with_context(|| format!("listen on {}", path.display()))?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
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
    let found = signed.network.machines().find(|m| m.name == name).with_context(|| {
        let names: Vec<String> = signed.network.machines().map(|m| m.name.to_string()).collect();
        format!("{name} is not a machine of the network (machines: {})", names.join(", "))
    })?;
    if found.id == me {
        bail!("{name} is this machine");
    }
    debug_assert_eq!(found.kind, Kind::Machine);
    Ok(found.id)
}

async fn handle(mut stream: UnixStream, endpoint: &Endpoint, paths: &Paths, connections: &Connections) -> anyhow::Result<()> {
    let Some(request) = receive::<_, Request>(&mut stream).await? else {
        return Ok(());
    };
    if let Request::Send { paths: sent, to, session } = request {
        let sent: Vec<PathBuf> = sent.iter().map(PathBuf::from).collect();
        let reply = match send_files(endpoint, paths, connections, &sent, &to, session, &mut stream).await {
            Ok(reply) => reply,
            Err(e) => Response::Error(format!("{e:#}")),
        };
        send(&mut stream, &reply).await?;
        return Ok(stream.shutdown().await?);
    }
    if let Request::Shared = request {
        let reply = Response::SharedList(shared_everywhere(endpoint, paths, connections).await);
        send(&mut stream, &reply).await?;
        return Ok(stream.shutdown().await?);
    }
    let (rx, tx) = stream.into_split();
    forward(request, rx, tx, endpoint, paths, connections).await
}

/// The `Shared` lists of this machine and every other machine of the network
/// (a machine that does not answer is left out).
async fn shared_everywhere(endpoint: &Endpoint, paths: &Paths, connections: &Connections) -> Vec<et_proto::SharedItem> {
    let me = endpoint.id();
    let Ok(Some(signed)) = network::load(paths) else { return Vec::new() };
    let mine = signed.network.member(&me).map(|m| m.name.to_string()).unwrap_or_default();
    let mut all = crate::files::shared(paths, &mine);
    for m in signed.network.machines().filter(|m| m.id != me) {
        let asked = async {
            let conn = connections.to(endpoint, m.id).await?;
            let (mut up, mut down) = conn.open_bi().await?;
            write_frame(&mut up, &Request::Shared).await?;
            up.finish()?;
            read_frame::<Response>(&mut down).await?.context("no answer")
        };
        match tokio::time::timeout(std::time::Duration::from_secs(10), asked).await {
            Ok(Ok(Response::SharedList(list))) => all.extend(list),
            other => debug!("no shared list from {}: {other:?}", m.name),
        }
    }
    all.sort_by_key(|i| std::cmp::Reverse(i.received));
    all
}

/// `etd send` (and `Share` to another machine): `Progress` frames to `out`;
/// returns the machine's answer.
pub async fn send_files<W: AsyncWrite + Unpin>(
    endpoint: &Endpoint,
    paths: &Paths,
    connections: &Connections,
    sent: &[PathBuf],
    to: &str,
    session: Option<String>,
    out: &mut W,
) -> anyhow::Result<Response> {
    let id = machine(paths, endpoint.id(), to)?;
    let files = connections.files.get(paths).await?;
    let conn = connections.to(endpoint, id).await.with_context(|| to.to_string())?;
    let outgoing = files.import(sent).await?;
    let (progress, mut updates) = tokio::sync::mpsc::unbounded_channel();
    let sending = files.send(&conn, &outgoing, session, move |done, total| {
        let _ = progress.send((done, total));
    });
    tokio::pin!(sending);
    loop {
        tokio::select! {
            result = &mut sending => return result,
            Some((done, total)) = updates.recv() => send(out, &Response::Progress { done, total }).await?,
        }
    }
}

/// Sends `request` (`Agent { host: Some }` or `ExecOn`) on to that machine,
/// and its answer back on `tx`; for an agent, then the pipe: frames down,
/// raw bytes from `rx` up.
pub async fn forward<R, W>(
    request: Request,
    mut rx: R,
    mut tx: W,
    endpoint: &Endpoint,
    paths: &Paths,
    connections: &Connections,
) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin,
{
    let (host, remote) = match request {
        Request::Agent { host: Some(host) } => (host, Request::Agent { host: None }),
        Request::ExecOn { host, script } => (host, Request::Exec { script }),
        other => {
            send(&mut tx, &Response::Error(format!("not a request for another machine: {other:?}"))).await?;
            return Ok(tx.shutdown().await?);
        }
    };
    let id = match machine(paths, endpoint.id(), &host) {
        Ok(id) => id,
        Err(e) => {
            send(&mut tx, &Response::Error(format!("{e:#}"))).await?;
            return Ok(tx.shutdown().await?);
        }
    };
    let is_agent = matches!(remote, Request::Agent { .. });
    let opened = async {
        let conn = connections.to(endpoint, id).await?;
        let (mut up, mut down) = conn.open_bi().await?;
        write_frame(&mut up, &remote).await?;
        if !is_agent {
            up.finish()?;
        }
        let reply = read_frame::<Response>(&mut down).await?.context("no answer")?;
        anyhow::Ok((up, down, reply))
    }
    .await;
    let (mut up, mut down, reply) = match opened {
        Ok(opened) => opened,
        Err(e) => {
            send(&mut tx, &Response::Error(format!("{host}: {e:#}"))).await?;
            return Ok(tx.shutdown().await?);
        }
    };
    let started = matches!(reply, Response::AgentStarted);
    send(&mut tx, &reply).await?;
    if !started {
        return Ok(tx.shutdown().await?);
    }
    // The pipe: frames down as they come, raw bytes up.
    let pump = tokio::spawn(async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match rx.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if up.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = up.finish();
    });
    while let Some(frame) = read_frame::<ControlFrame>(&mut down).await? {
        let last = matches!(frame, ControlFrame::Exit(_));
        if send(&mut tx, &frame).await.is_err() {
            break;
        }
        if last {
            break;
        }
    }
    pump.abort();
    let _ = tx.shutdown().await;
    Ok(())
}

/// Starts the socket server next to the daemon; errors are logged.
pub fn spawn(endpoint: Endpoint, paths: Paths, connections: Arc<Connections>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = serve(endpoint, paths, connections).await {
            warn!("local socket: {e:#}");
        }
    })
}

/// Removes the socket file (at stop).
pub fn remove(paths: &Paths) {
    let path: &Path = &socket_path(paths);
    let _ = std::fs::remove_file(path);
}
