//! Sending files to a machine of the network, shared by etd (`etd send`) and
//! et-link (the apps). PROTOCOL.md "Sending files".
//!
//! The sender imports its files into an iroh-blobs store as one collection
//! and serves that collection on a connection IT opened to the receiver, so
//! a phone never has to accept connections. It then sends `Deliver { hash }`
//! on that connection; the receiving etd fetches the collection over the same
//! connection (only the chunks it does not have yet: a resend resumes).
//!
//! Blobs are kept only while a send or a delivery runs (tags); the store's
//! garbage collection removes the rest. The sender imports by reference, so
//! its files are not copied; the store never deletes a file it references.
use std::collections::{BTreeMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context};
use iroh::endpoint::Connection;
use iroh_blobs::api::blobs::{AddPathOptions, ImportMode};
use iroh_blobs::api::TempTag;
use iroh_blobs::format::collection::Collection;
use iroh_blobs::provider::events::{AbortReason, ConnectMode, EventMask, EventSender, ObserveMode, ProviderMessage, RequestMode};
use iroh_blobs::store::fs::options::Options;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::store::GcConfig;
use iroh_blobs::{BlobFormat, Hash};

use crate::{read_frame, write_frame, Request, Response};

/// How often blobs that no send or delivery holds are removed.
pub const GC_EVERY: Duration = Duration::from_secs(10 * 60);
/// Tags that keep a delivery's blobs while it runs.
pub const RECV_TAG: &str = "recv/";

/// A blob store, and the collections offered from it.
pub struct Files {
    store: FsStore,
    dir: PathBuf,
    /// Root hashes being sent: the only ones served.
    offered: Arc<Mutex<HashSet<Hash>>>,
    /// Connections served on (stable ids).
    serving: Arc<Mutex<HashSet<usize>>>,
}

impl std::fmt::Debug for Files {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Files").field("dir", &self.dir).field("offered", &self.offered.lock().unwrap().len()).finish()
    }
}

/// A collection being sent; while it lives, its blobs are kept and served.
pub struct Outgoing {
    files: Arc<Files>,
    root: TempTag,
    pub total: u64,
}

impl Outgoing {
    pub fn hash(&self) -> Hash {
        self.root.hash()
    }
}

impl Drop for Outgoing {
    fn drop(&mut self) {
        self.files.offered.lock().unwrap().remove(&self.root.hash());
    }
}

impl Files {
    pub async fn open(dir: &Path, gc: Duration) -> anyhow::Result<Arc<Self>> {
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let mut options = Options::new(dir);
        options.gc = Some(GcConfig { interval: gc, add_protected: None });
        let store = FsStore::load_with_opts(dir.join("blobs.db"), options).await.context("open the file store")?;
        // Deliveries cut by a restart: the sender sends again.
        store.tags().delete_prefix(RECV_TAG).await?;
        Ok(Arc::new(Self { store, dir: dir.to_path_buf(), offered: Arc::default(), serving: Arc::default() }))
    }

    pub fn store(&self) -> &FsStore {
        &self.store
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Serves the offered collections on `conn`, a connection this side
    /// opened; the other side fetches over it after `Deliver`.
    pub fn serve_on(&self, conn: &Connection) {
        let id = conn.stable_id();
        if !self.serving.lock().unwrap().insert(id) {
            return;
        }
        let (store, conn, events, serving) =
            ((*self.store).clone(), conn.clone(), only_offered(self.offered.clone()), self.serving.clone());
        tokio::spawn(async move {
            iroh_blobs::provider::handle_connection(conn, store, events).await;
            serving.lock().unwrap().remove(&id);
        });
    }

    /// Imports `paths` (files or folders) as one collection, by reference.
    /// Names are relative to each path's parent: `a.txt`, `out/x/y.bin`.
    pub async fn import(self: &Arc<Self>, paths: &[PathBuf]) -> anyhow::Result<Outgoing> {
        let mut sources = BTreeMap::new();
        for path in paths {
            let path = path.canonicalize().with_context(|| format!("{}", path.display()))?;
            let parent = path.parent().context("cannot send /")?.to_path_buf();
            for file in walk(&path)? {
                let name = name_of(file.strip_prefix(&parent)?)?;
                if sources.insert(name.clone(), file).is_some() {
                    bail!("two files would be called {name}");
                }
            }
        }
        if sources.is_empty() {
            bail!("nothing to send (empty folders only)");
        }
        let mut items = Vec::new();
        let mut total = 0;
        for (name, path) in sources {
            total += std::fs::metadata(&path)?.len();
            let tag = self
                .store
                .add_path_with_opts(AddPathOptions { path: path.clone(), mode: ImportMode::TryReference, format: BlobFormat::Raw })
                .temp_tag()
                .await
                .with_context(|| format!("read {}", path.display()))?;
            items.push((name, tag));
        }
        let collection: Collection = items.iter().map(|(name, tag)| (name.clone(), tag.hash())).collect();
        // The collection's tag keeps the items.
        let root = collection.store(&self.store).await?;
        self.offered.lock().unwrap().insert(root.hash());
        Ok(Outgoing { files: self.clone(), root, total })
    }

    /// Sends an imported collection to the machine at the other end of
    /// `conn`, a connection this side opened. `progress(done, total)` for
    /// each `Progress` frame. Returns the machine's last answer (`Delivered`
    /// or `Error`).
    pub async fn send(
        &self,
        conn: &Connection,
        outgoing: &Outgoing,
        session: Option<String>,
        mut progress: impl FnMut(u64, u64),
    ) -> anyhow::Result<Response> {
        self.serve_on(conn);
        let (mut up, mut down) = conn.open_bi().await?;
        write_frame(&mut up, &Request::Deliver { hash: *outgoing.hash().as_bytes(), session }).await?;
        up.finish()?;
        loop {
            match read_frame::<Response>(&mut down).await?.context("the machine closed the stream")? {
                Response::Progress { done, total } => progress(done, total),
                last => return Ok(last),
            }
        }
    }
}

/// Serves only offered roots (whole collections, with their children).
fn only_offered(offered: Arc<Mutex<HashSet<Hash>>>) -> EventSender {
    let mask = EventMask {
        connected: ConnectMode::None,
        get: RequestMode::Intercept,
        get_many: RequestMode::Disabled,
        observe: ObserveMode::Intercept,
        ..EventMask::DEFAULT
    };
    let (tx, mut rx) = EventSender::channel(16, mask);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let ProviderMessage::GetRequestReceived(msg) = msg {
                let ok = offered.lock().unwrap().contains(&msg.request.hash);
                msg.tx.send(if ok { Ok(()) } else { Err(AbortReason::Permission) }).await.ok();
            } else if let ProviderMessage::ObserveRequestReceived(msg) = msg {
                msg.tx.send(Err(AbortReason::Permission)).await.ok();
            }
        }
    });
    tx
}

/// Every file under `path` (or `path` itself), skipping symlinks, sorted.
pub fn walk(path: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let meta = std::fs::symlink_metadata(path)?;
    if meta.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }
    let mut out = Vec::new();
    if meta.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(path)?.collect::<Result<_, _>>()?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            out.extend(walk(&entry.path())?);
        }
    }
    Ok(out)
}

/// A relative path as a collection name: `a/b.txt`.
fn name_of(rel: &Path) -> anyhow::Result<String> {
    let parts: Vec<String> = rel
        .components()
        .map(|c| match c {
            Component::Normal(s) => s.to_str().map(str::to_string).with_context(|| format!("not UTF-8: {}", rel.display())),
            _ => bail!("bad path {}", rel.display()),
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(parts.join("/"))
}
