//! The receiving side of sending files (`Deliver`), and the store for
//! `etd send` (et_proto::files has the sending side, shared with the apps).
//! Files land in `~/etd-downloads/`.
use std::collections::BTreeMap;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use et_proto::files::{Files, GC_EVERY, RECV_TAG};
use et_proto::{Response, SharedItem};
use iroh::endpoint::Connection;
use iroh_blobs::api::blobs::{ExportMode, ExportOptions};
use iroh_blobs::api::remote::GetProgressItem;
use iroh_blobs::format::collection::Collection;
use iroh_blobs::{Hash, HashAndFormat};
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWrite;
use tokio::sync::OnceCell;
use tracing::{debug, info};

use crate::config::Paths;
use crate::local;
use crate::ops;
use crate::tmux::Env;

/// A delivery must leave this much free on the disk.
const FREE_MARGIN: u64 = 2 * 1024 * 1024 * 1024;
/// Most files in one send.
const MAX_ITEMS: u64 = 100_000;
/// `Progress` frames at most this often.
const PROGRESS_EVERY: Duration = Duration::from_millis(250);

/// The store, opened at first use (most etds never send a file).
#[derive(Debug, Default)]
pub struct Lazy(OnceCell<Arc<Files>>);

impl Lazy {
    pub async fn get(&self, paths: &Paths) -> anyhow::Result<Arc<Files>> {
        let dir = paths.state.join("blobs");
        self.0.get_or_try_init(|| async move { Files::open(&dir, GC_EVERY).await }).await.cloned()
    }
}

/// `Deliver`: fetches collection `hash` over `conn` (from the member at its
/// other end), writes it under `downloads`, and returns the top-level paths.
/// `Progress` frames go to `out`.
pub async fn deliver<W: AsyncWrite + Unpin>(
    files: &Files,
    conn: &Connection,
    hash: Hash,
    downloads: &Path,
    out: &mut W,
) -> anyhow::Result<Vec<PathBuf>> {
    let content = HashAndFormat::hash_seq(hash);
    let (_, sizes) = iroh_blobs::get::request::get_hash_seq_and_sizes(conn, &hash, 32 * MAX_ITEMS, None)
        .await
        .context("ask the sender for the sizes")?;
    let total: u64 = sizes.iter().sum();
    for dir in [files.dir(), downloads] {
        if let Some(free) = available(dir) {
            if free < total.saturating_add(FREE_MARGIN) {
                bail!("not enough space on {}: {} free, {} needed + 2 GB", dir.display(), mb(free), mb(total));
            }
        }
    }
    let tag = format!("{RECV_TAG}{hash}");
    files.store().tags().set(&tag, content).await?;
    let result = fetch_and_export(files, conn, content, total, downloads, out).await;
    files.store().tags().delete(&tag).await?;
    result
}

async fn fetch_and_export<W: AsyncWrite + Unpin>(
    files: &Files,
    conn: &Connection,
    content: HashAndFormat,
    total: u64,
    downloads: &Path,
    out: &mut W,
) -> anyhow::Result<Vec<PathBuf>> {
    let store = files.store();
    let started = Instant::now();
    let mut last = Instant::now() - PROGRESS_EVERY;
    let mut fetch = store.remote().fetch(conn.clone(), content).stream();
    let stats = loop {
        match fetch.next().await.context("the fetch ended")? {
            GetProgressItem::Progress(done) => {
                if last.elapsed() >= PROGRESS_EVERY {
                    last = Instant::now();
                    local::send(out, &Response::Progress { done, total }).await?;
                }
            }
            GetProgressItem::Done(stats) => break stats,
            GetProgressItem::Error(e) => bail!("fetch: {e}"),
        }
    };
    local::send(out, &Response::Progress { done: total, total }).await?;
    info!("fetched {} of {} in {:?}", mb(stats.payload_bytes_read), mb(total), started.elapsed());

    let collection = Collection::load(content.hash, store.as_ref()).await?;
    let names: Vec<String> = collection.iter().map(|(name, _)| name.clone()).collect();
    let (targets, tops) = plan(&names, downloads)?;
    for ((_, hash), target) in collection.iter().zip(targets) {
        std::fs::create_dir_all(target.parent().context("no parent")?)?;
        // Copy, never reference: the user may change or delete the file.
        store
            .export_with_opts(ExportOptions { hash: *hash, target: target.clone(), mode: ExportMode::Copy })
            .await
            .with_context(|| format!("write {}", target.display()))?;
        debug!("wrote {}", target.display());
    }
    Ok(tops)
}

/// One line of `deliveries.jsonl`.
#[derive(Debug, Serialize, Deserialize)]
struct Logged {
    path: String,
    from: String,
    received: u64,
    session: Option<String>,
}

/// Notes a delivery for `Shared`.
pub fn log_delivery(paths: &Paths, tops: &[PathBuf], from: &str, session: Option<&str>) -> anyhow::Result<()> {
    let mut lines = String::new();
    for top in tops {
        let line = Logged {
            path: top.display().to_string(),
            from: from.to_string(),
            received: crate::config::now_secs(),
            session: session.map(str::to_string),
        };
        lines.push_str(&serde_json::to_string(&line)?);
        lines.push('\n');
    }
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(paths.deliveries())?;
    std::io::Write::write_all(&mut file, lines.as_bytes())?;
    Ok(())
}

fn logged(paths: &Paths) -> Vec<Logged> {
    let text = std::fs::read_to_string(paths.deliveries()).unwrap_or_default();
    text.lines().filter_map(|l| serde_json::from_str(l).ok()).collect()
}

/// The deliveries still on disk, newest first; a path delivered twice
/// counts once (the latest).
pub fn shared(paths: &Paths, machine: &str) -> Vec<SharedItem> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for l in logged(paths).into_iter().rev() {
        let path = PathBuf::from(&l.path);
        if !seen.insert(l.path.clone()) {
            continue;
        }
        let Ok(meta) = std::fs::symlink_metadata(&path) else { continue };
        out.push(SharedItem {
            machine: machine.to_string(),
            name: path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default(),
            size: if meta.is_dir() {
                et_proto::files::walk(&path).unwrap_or_default().iter().filter_map(|f| f.metadata().ok()).map(|m| m.len()).sum()
            } else {
                meta.len()
            },
            is_dir: meta.is_dir(),
            path: l.path,
            from: l.from,
            received: l.received,
            session: l.session,
        });
    }
    out
}

/// `path` if it is inside `downloads` (after symlinks), not `downloads` itself.
pub fn inside(downloads: &Path, path: &str) -> anyhow::Result<PathBuf> {
    let full = Path::new(path).canonicalize().with_context(|| path.to_string())?;
    let base = downloads.canonicalize().with_context(|| downloads.display().to_string())?;
    if full == base || !full.starts_with(&base) {
        bail!("{path} is not in {}", downloads.display());
    }
    Ok(full)
}

/// Deletes a shared file or folder, and its log lines.
pub fn unshare(paths: &Paths, downloads: &Path, path: &str) -> anyhow::Result<()> {
    let full = inside(downloads, path)?;
    if full.is_dir() {
        std::fs::remove_dir_all(&full)?;
    } else {
        std::fs::remove_file(&full)?;
    }
    let keep: Vec<Logged> = logged(paths).into_iter().filter(|l| l.path != path && Path::new(&l.path) != full).collect();
    let mut text = String::new();
    for l in keep {
        text.push_str(&serde_json::to_string(&l)?);
        text.push('\n');
    }
    let tmp = paths.deliveries().with_extension("tmp");
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, paths.deliveries())?;
    Ok(())
}

/// `$ETD_DOWNLOADS` (a leading `~/` is the home directory), else
/// `~/etd-downloads`.
pub fn downloads(env: &Env) -> anyhow::Result<PathBuf> {
    let home = ops::home(env)?;
    let set = std::env::var("ETD_DOWNLOADS").ok().or_else(|| env.get("ETD_DOWNLOADS").cloned()).filter(|s| !s.is_empty());
    Ok(match set {
        Some(dir) => match dir.strip_prefix("~/") {
            Some(rest) => home.join(rest),
            None => PathBuf::from(dir),
        },
        None => home.join("etd-downloads"),
    })
}

/// A name part from the sender: no `..`, no separators, not empty.
fn checked(part: &str) -> anyhow::Result<&str> {
    if part.is_empty() || part == "." || part == ".." || part.contains(['/', '\\', '\0']) {
        bail!("bad file name from the sender: {part:?}");
    }
    Ok(part)
}

/// Where each item goes under `dir`, and the top-level paths. A top-level
/// name that is taken gets ` (2)`, ` (3)`… (`a (2).txt`, `out (2)`).
fn plan(names: &[String], dir: &Path) -> anyhow::Result<(Vec<PathBuf>, Vec<PathBuf>)> {
    let mut tops: BTreeMap<String, PathBuf> = BTreeMap::new();
    let mut order = Vec::new();
    let mut targets = Vec::new();
    for name in names {
        let parts: Vec<&str> = name.split('/').map(checked).collect::<anyhow::Result<_>>()?;
        let is_file = parts.len() == 1;
        let top = tops.entry(parts[0].to_string()).or_insert_with(|| {
            let free = free_name(dir, parts[0], is_file);
            order.push(free.clone());
            free
        });
        targets.push(parts[1..].iter().fold(top.clone(), |p, part| p.join(part)));
    }
    Ok((targets, order))
}

fn free_name(dir: &Path, name: &str, is_file: bool) -> PathBuf {
    let taken = |p: &Path| std::fs::symlink_metadata(p).is_ok();
    let first = dir.join(name);
    if !taken(&first) {
        return first;
    }
    let (stem, ext) = match name.rsplit_once('.') {
        Some((stem, ext)) if is_file && !stem.is_empty() => (stem, format!(".{ext}")),
        _ => (name, String::new()),
    };
    (2..).map(|n| dir.join(format!("{stem} ({n}){ext}"))).find(|p| !taken(p)).expect("some name is free")
}

/// Free bytes for this user on the disk of `path` (or its nearest parent).
fn available(path: &Path) -> Option<u64> {
    let mut p = path;
    while !p.exists() {
        p = p.parent()?;
    }
    let c = CString::new(p.as_os_str().as_bytes()).ok()?;
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: a valid C string and a statvfs to write into.
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
        return None;
    }
    Some(s.f_bavail as u64 * s.f_frsize as u64)
}

fn mb(bytes: u64) -> String {
    format!("{:.1} MB", bytes as f64 / 1_000_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_keeps_trees_and_renames_taken_names() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), "x").unwrap();
        std::fs::create_dir(dir.path().join("out")).unwrap();
        let names = ["a.txt", "out/x/y.bin", "out/z", "Makefile"].map(String::from);
        let (targets, tops) = plan(&names, dir.path()).unwrap();
        let d = dir.path();
        assert_eq!(targets, [d.join("a (2).txt"), d.join("out (2)/x/y.bin"), d.join("out (2)/z"), d.join("Makefile")]);
        assert_eq!(tops, [d.join("a (2).txt"), d.join("out (2)"), d.join("Makefile")]);
        for bad in ["../x", "a//b", "/abs", "a/./b", "a\\b"] {
            assert!(plan(&[bad.to_string()], d).is_err(), "{bad} was allowed");
        }
    }

    /// Deliver between two stores, then let both collect garbage: the
    /// sender's original (imported by reference) and the delivered copy stay.
    #[tokio::test(flavor = "multi_thread")]
    async fn garbage_collection_keeps_user_files() {
        use iroh::endpoint::presets;
        let dir = tempfile::tempdir().unwrap();
        let original = dir.path().join("src/report.bin");
        std::fs::create_dir_all(original.parent().unwrap()).unwrap();
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8).collect();
        std::fs::write(&original, &data).unwrap();

        let gc = Duration::from_millis(300);
        let sender = Files::open(&dir.path().join("a"), gc).await.unwrap();
        let receiver = Files::open(&dir.path().join("b"), gc).await.unwrap();
        let a = iroh::Endpoint::builder(presets::Minimal).bind().await.unwrap();
        let b = iroh::Endpoint::builder(presets::Minimal).alpns(vec![b"t".to_vec()]).bind().await.unwrap();
        let port = b.bound_sockets().iter().find(|s| s.is_ipv4()).unwrap().port();
        let addr = iroh::EndpointAddr::new(b.id()).with_ip_addr(([127, 0, 0, 1], port).into());
        let accepting = tokio::spawn({
            let b = b.clone();
            async move { b.accept().await.unwrap().await.unwrap() }
        });
        let conn = a.connect(addr, b"t").await.unwrap();
        let incoming = accepting.await.unwrap();

        let outgoing = sender.import(std::slice::from_ref(&original)).await.unwrap();
        sender.serve_on(&conn);
        let downloads = dir.path().join("etd-downloads");
        let paths = deliver(&receiver, &incoming, outgoing.hash(), &downloads, &mut tokio::io::sink()).await.unwrap();
        assert_eq!(paths, [downloads.join("report.bin")]);

        // Not offered any more: a second fetch is refused.
        let hash = outgoing.hash();
        drop(outgoing);
        let other = tempfile::tempdir().unwrap();
        assert!(deliver(&receiver, &incoming, hash, other.path(), &mut tokio::io::sink()).await.is_err());

        let big_in_store = |store: &str| {
            et_proto::files::walk(&dir.path().join(store))
                .unwrap()
                .iter()
                .filter(|p| std::fs::metadata(p).unwrap().len() >= 3_000_000)
                .count()
        };
        assert_eq!(big_in_store("b"), 1, "the receiver's store holds its copy");
        tokio::time::sleep(gc * 5).await;
        assert_eq!(big_in_store("b"), 0, "garbage collection did not run");
        assert_eq!(std::fs::read(&original).unwrap(), data, "the sender's original changed");
        assert_eq!(std::fs::read(downloads.join("report.bin")).unwrap(), data, "the delivered copy changed");
    }

    #[test]
    fn downloads_default_and_override() {
        let env: Env = [("HOME".to_string(), "/home/u".to_string())].into_iter().collect();
        if std::env::var_os("ETD_DOWNLOADS").is_none() {
            assert_eq!(downloads(&env).unwrap(), Path::new("/home/u/etd-downloads"));
        }
        let mut env2 = env.clone();
        env2.insert("ETD_DOWNLOADS".into(), "~/in".into());
        if std::env::var_os("ETD_DOWNLOADS").is_none() {
            assert_eq!(downloads(&env2).unwrap(), Path::new("/home/u/in"));
        }
    }
}
