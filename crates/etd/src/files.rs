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
    enough_space(files.dir(), downloads, total)?;
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
    let written = async {
        for ((_, hash), target) in collection.iter().zip(targets) {
            std::fs::create_dir_all(target.parent().context("no parent")?)?;
            // A file left by an earlier try (or a symlink) is not written through.
            if tops.iter().any(|t| t.part.as_ref() == Some(&target)) {
                let _ = std::fs::remove_file(&target);
            }
            // Copy, never reference: the user may change or delete the file.
            store
                .export_with_opts(ExportOptions { hash: *hash, target: target.clone(), mode: ExportMode::Copy })
                .await
                .with_context(|| format!("write {}", target.display()))?;
            debug!("wrote {}", target.display());
        }
        for top in &tops {
            if let Some(part) = &top.part {
                // Over this delivery's own placeholder.
                std::fs::rename(part, &top.path).with_context(|| format!("write {}", top.path.display()))?;
            }
        }
        anyhow::Ok(())
    }
    .await;
    if let Err(e) = written {
        tops.iter().for_each(Top::release);
        return Err(e);
    }
    Ok(tops.into_iter().map(|t| t.path).collect())
}

/// Held while `deliveries.jsonl` is appended to or rewritten (one etd
/// process writes it, from tasks that run at the same time).
static DELIVERIES: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
    let _log = DELIVERIES.lock().unwrap_or_else(|e| e.into_inner());
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
    // Held across the read and the rename, so no delivery logged meanwhile is lost.
    let _log = DELIVERIES.lock().unwrap_or_else(|e| e.into_inner());
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

/// A top-level path a delivery has claimed under the downloads folder.
#[derive(Debug, PartialEq)]
struct Top {
    path: PathBuf,
    /// A file: its bytes go to this hidden name first, then are renamed
    /// over the empty placeholder at `path`. None: a folder.
    part: Option<PathBuf>,
}

impl Top {
    /// Removes what the delivery claimed (after a failure).
    fn release(&self) {
        match &self.part {
            Some(part) => {
                let _ = std::fs::remove_file(part);
                let _ = std::fs::remove_file(&self.path);
            }
            None => {
                let _ = std::fs::remove_dir_all(&self.path);
            }
        }
    }
}

/// Where each item is written under `dir`, and the top-level paths. A
/// top-level name that is taken gets ` (2)`, ` (3)`… (`a (2).txt`, `out
/// (2)`). Each top-level path is claimed on disk here (a new folder, or an
/// empty file), so two deliveries at the same time never pick the same one.
/// Names are checked before anything is created.
fn plan(names: &[String], dir: &Path) -> anyhow::Result<(Vec<PathBuf>, Vec<Top>)> {
    let split: Vec<Vec<&str>> =
        names.iter().map(|name| name.split('/').map(checked).collect::<anyhow::Result<_>>()).collect::<anyhow::Result<_>>()?;
    // A name that is both a file and a folder (`notes`, `notes/a.txt`) would
    // fail half way through writing.
    let files: std::collections::HashSet<&str> = names.iter().map(String::as_str).collect();
    if files.len() != names.len() {
        bail!("the sender's collection has a name twice");
    }
    for name in names {
        for (i, _) in name.match_indices('/') {
            if files.contains(&name[..i]) {
                bail!("the sender's collection has {:?} as a file and as a folder", &name[..i]);
            }
        }
    }
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let mut claimed: BTreeMap<&str, usize> = BTreeMap::new();
    let mut tops: Vec<Top> = Vec::new();
    let mut targets = Vec::new();
    for parts in &split {
        let index = match claimed.get(parts[0]) {
            Some(&i) => i,
            None => match claim(dir, parts[0], parts.len() == 1) {
                Ok(top) => {
                    tops.push(top);
                    claimed.insert(parts[0], tops.len() - 1);
                    tops.len() - 1
                }
                Err(e) => {
                    tops.iter().for_each(Top::release);
                    return Err(e);
                }
            },
        };
        let top = &tops[index];
        targets.push(match &top.part {
            Some(part) => part.clone(),
            None => parts[1..].iter().fold(top.path.clone(), |p, part| p.join(part)),
        });
    }
    Ok((targets, tops))
}

/// Claims the first free name of `name`, `name (2)`… in `dir`: creates the
/// folder or an empty file, which fails if another delivery (or anything
/// else) made it first; then the next name is tried.
fn claim(dir: &Path, name: &str, is_file: bool) -> anyhow::Result<Top> {
    let (stem, ext) = match name.rsplit_once('.') {
        Some((stem, ext)) if is_file && !stem.is_empty() => (stem, format!(".{ext}")),
        _ => (name, String::new()),
    };
    let candidates = std::iter::once(name.to_string()).chain((2..).map(|n| format!("{stem} ({n}){ext}")));
    for candidate in candidates.take(10_000) {
        let path = dir.join(&candidate);
        let made = if is_file {
            std::fs::OpenOptions::new().write(true).create_new(true).open(&path).map(drop)
        } else {
            std::fs::create_dir(&path)
        };
        match made {
            Ok(()) => {
                let part = is_file.then(|| dir.join(format!(".{candidate}.etd-part")));
                return Ok(Top { path, part });
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e).with_context(|| format!("create {}", path.display())),
        }
    }
    bail!("no free name for {name} in {}", dir.display())
}

/// Refuses a delivery of `total` bytes that would leave less than
/// `FREE_MARGIN` free. The bytes are written twice, into the store and then
/// copied to downloads (until garbage collection), so one disk that holds
/// both needs room for both. A disk whose free space cannot be read counts
/// as full.
fn enough_space(store: &Path, downloads: &Path, total: u64) -> anyhow::Result<()> {
    let (store_disk, store_free) = disk(store).with_context(|| format!("free space on {}", store.display()))?;
    let (downloads_disk, downloads_free) = disk(downloads).with_context(|| format!("free space on {}", downloads.display()))?;
    let checks: &[(&Path, u64, u64)] = if store_disk == downloads_disk {
        &[(downloads, downloads_free, total.saturating_mul(2))]
    } else {
        &[(store, store_free, total), (downloads, downloads_free, total)]
    };
    for &(dir, free, need) in checks {
        if free < need.saturating_add(FREE_MARGIN) {
            bail!("not enough space on {}: {} free, {} needed + 2 GB", dir.display(), mb(free), mb(need));
        }
    }
    Ok(())
}

/// The disk (device id) of `path` (or its nearest parent), and the bytes
/// free on it for this user.
fn disk(path: &Path) -> anyhow::Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let mut p = path;
    while std::fs::symlink_metadata(p).is_err() {
        p = p.parent().context("no existing parent")?;
    }
    let dev = std::fs::metadata(p)?.dev();
    let c = CString::new(p.as_os_str().as_bytes())?;
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: a valid C string and a statvfs to write into.
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok((dev, s.f_bavail as u64 * s.f_frsize as u64))
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
        assert_eq!(targets, [d.join(".a (2).txt.etd-part"), d.join("out (2)/x/y.bin"), d.join("out (2)/z"), d.join(".Makefile.etd-part")]);
        let paths: Vec<_> = tops.iter().map(|t| t.path.clone()).collect();
        assert_eq!(paths, [d.join("a (2).txt"), d.join("out (2)"), d.join("Makefile")]);
        // Claimed on disk: a second delivery at the same time picks other names.
        assert!(d.join("a (2).txt").is_file() && d.join("out (2)").is_dir() && d.join("Makefile").is_file());
        let (_, again) = plan(&names, dir.path()).unwrap();
        let paths: Vec<_> = again.iter().map(|t| t.path.clone()).collect();
        assert_eq!(paths, [d.join("a (3).txt"), d.join("out (3)"), d.join("Makefile (2)")]);
        again.iter().for_each(Top::release);
        assert!(!d.join("out (3)").exists() && !d.join("Makefile (2)").exists());
    }

    /// Bad names, and a name that is both a file and a folder, are refused
    /// before anything is created.
    #[test]
    fn plan_refuses_bad_collections_and_creates_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        for bad in ["../x", "a//b", "/abs", "a/./b", "a\\b"] {
            assert!(plan(&["ok.txt".to_string(), bad.to_string()], d).is_err(), "{bad} was allowed");
        }
        for names in [&["notes", "notes/a.txt"][..], &["x/notes/a.txt", "x/notes", "first.txt"], &["a", "a"]] {
            let names: Vec<String> = names.iter().map(|s| s.to_string()).collect();
            assert!(plan(&names, d).is_err(), "{names:?} was allowed");
        }
        assert_eq!(std::fs::read_dir(d).unwrap().count(), 0, "something was created");
    }

    /// One disk holding the store and downloads needs room for two copies.
    #[test]
    fn space_counts_both_copies_on_one_disk() {
        let dir = tempfile::tempdir().unwrap();
        let (store, downloads) = (dir.path().join("blobs"), dir.path().join("etd-downloads"));
        let (_, free) = disk(dir.path()).unwrap();
        assert!(enough_space(&store, &downloads, 1).is_ok() || free < FREE_MARGIN + 2);
        // Fits once, not twice (with room for the free space to move a little).
        if free > FREE_MARGIN + 300_000_000 {
            let total = (free - FREE_MARGIN) * 2 / 3;
            assert!(enough_space(&store, &downloads, total).is_err());
        }
        assert!(enough_space(&store, &downloads, u64::MAX / 2).is_err());
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

        // A second send of the same bytes is still served after the first ends.
        let hash = outgoing.hash();
        let again = sender.import(std::slice::from_ref(&original)).await.unwrap();
        assert_eq!(again.hash(), hash);
        drop(outgoing);
        let paths = deliver(&receiver, &incoming, hash, &downloads, &mut tokio::io::sink()).await.unwrap();
        assert_eq!(paths, [downloads.join("report (2).bin")]);

        // Not offered any more: a later fetch is refused.
        drop(again);
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
