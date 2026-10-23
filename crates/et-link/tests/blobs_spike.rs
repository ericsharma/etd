//! F0 spike (docs/PLAN-files.md in et-ios): the receiver pulls a folder from
//! the sender over the connection the SENDER opened. The sender (a phone) runs
//! iroh-blobs' provider on its outgoing connection, so it never accepts
//! connections; the receiver (etd) uses `fetch`, which asks only for what it
//! does not have yet (resume).
use std::path::{Path, PathBuf};
use std::time::Duration;

use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr};
use iroh_blobs::api::blobs::{AddPathOptions, ExportMode, ExportOptions, ImportMode};
use iroh_blobs::api::remote::GetProgressItem;
use iroh_blobs::api::TempTag;
use iroh_blobs::format::collection::Collection;
use iroh_blobs::provider::events::{AbortReason, ConnectMode, EventMask, EventSender, ProviderMessage, RequestMode};
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::{BlobFormat, Hash, HashAndFormat};
use n0_future::StreamExt;

const ALPN: &[u8] = b"spike/0";
const BIG: usize = 200 * 1024 * 1024;

/// Cheap, incompressible-looking bytes (xorshift).
fn fill(len: usize) -> Vec<u8> {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// `folder/` with a big file and a small one in a subfolder.
fn make_folder(dir: &Path) -> PathBuf {
    let root = dir.join("folder");
    std::fs::create_dir_all(root.join("sub")).unwrap();
    std::fs::write(root.join("big.bin"), fill(BIG)).unwrap();
    std::fs::write(root.join("sub/note.txt"), b"look at big.bin").unwrap();
    root
}

/// The sender's side: import by reference (no copy), one Collection named
/// relative to the folder's parent, as sendme does.
async fn import(store: &FsStore, root: &Path) -> (TempTag, Vec<TempTag>) {
    let mut items = Vec::new();
    let mut tags = Vec::new();
    for rel in ["big.bin", "sub/note.txt"] {
        let tt = store
            .add_path_with_opts(AddPathOptions { path: root.join(rel), mode: ImportMode::TryReference, format: BlobFormat::Raw })
            .temp_tag()
            .await
            .unwrap();
        items.push((format!("folder/{rel}"), tt.hash()));
        tags.push(tt);
    }
    let collection: Collection = items.into_iter().collect();
    (collection.store(store).await.unwrap(), tags)
}

/// The sender serves only `allowed`, as a hash sequence, to whoever is on
/// the other end of its own connection.
fn only(allowed: Hash) -> EventSender {
    let mask =
        EventMask { connected: ConnectMode::None, get: RequestMode::Intercept, get_many: RequestMode::Disabled, ..EventMask::DEFAULT };
    let (tx, mut rx) = EventSender::channel(16, mask);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let ProviderMessage::GetRequestReceived(msg) = msg {
                let ok = msg.request.hash == allowed;
                msg.tx.send(if ok { Ok(()) } else { Err(AbortReason::Permission) }).await.ok();
            }
        }
    });
    tx
}

async fn endpoint(alpns: Vec<Vec<u8>>) -> Endpoint {
    Endpoint::builder(presets::Minimal).alpns(alpns).bind().await.unwrap()
}

fn local_addr(ep: &Endpoint) -> EndpointAddr {
    let port = ep.bound_sockets().iter().find(|a| a.is_ipv4()).unwrap().port();
    EndpointAddr::new(ep.id()).with_ip_addr(([127, 0, 0, 1], port).into())
}

/// The receiver's side after the transfer: write each item under `out`.
async fn export(store: &FsStore, root: Hash, out: &Path) -> Vec<PathBuf> {
    let collection = Collection::load(root, store.as_ref()).await.unwrap();
    let mut paths = Vec::new();
    for (name, hash) in collection.iter() {
        let target = out.join(name);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        store.export_with_opts(ExportOptions { hash: *hash, target: target.clone(), mode: ExportMode::Copy }).await.unwrap();
        paths.push(target);
    }
    paths
}

#[tokio::test(flavor = "multi_thread")]
async fn pull_over_the_senders_connection_with_resume() {
    let dir = tempfile::tempdir().unwrap();
    let folder = make_folder(dir.path());

    // Sender: a store, a Collection, and a connection to the receiver.
    let send_store = FsStore::load(dir.path().join("send-store")).await.unwrap();
    let (root_tag, _tags) = import(&send_store, &folder).await;
    let root = root_tag.hash();
    let sender = endpoint(vec![]).await;

    // Receiver: accepts the connection, reads the root hash (stands in for
    // et/1 `Deliver`), then pulls over that same connection.
    let receiver = endpoint(vec![ALPN.to_vec()]).await;
    let recv_store = FsStore::load(dir.path().join("recv-store")).await.unwrap();
    let addr = local_addr(&receiver);

    // iroh finishes the handshake only once the receiver accepts.
    let accepting = tokio::spawn({
        let receiver = receiver.clone();
        async move { receiver.accept().await.unwrap().await.unwrap() }
    });
    let conn = sender.connect(addr, ALPN).await.unwrap();
    tokio::spawn(iroh_blobs::provider::handle_connection(conn.clone(), (*send_store).clone(), only(root)));
    let mut uni = conn.open_uni().await.unwrap();
    uni.write_all(root.as_bytes()).await.unwrap();
    uni.finish().unwrap();

    let incoming = accepting.await.unwrap();
    let mut uni = incoming.accept_uni().await.unwrap();
    let asked = Hash::from_bytes(uni.read_to_end(32).await.unwrap().try_into().unwrap());
    assert_eq!(asked, root);
    let content = HashAndFormat::hash_seq(asked);

    // 1. A different hash is refused by the sender.
    let other = HashAndFormat::hash_seq(Hash::new(b"not offered"));
    let refused = recv_store.remote().fetch(incoming.clone(), other).complete().await;
    assert!(refused.is_err(), "the sender served a hash it did not offer");

    // 2. Cut the first pull at about 50 %.
    let mut first = recv_store.remote().fetch(incoming.clone(), content).stream();
    let mut got = 0;
    while let Some(item) = first.next().await {
        match item {
            GetProgressItem::Progress(n) => {
                got = n;
                if n >= (BIG / 2) as u64 {
                    break;
                }
            }
            other => panic!("first pull ended early: {other:?}"),
        }
    }
    drop(first);
    assert!(got >= (BIG / 2) as u64);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 3. The second pull asks only for the rest.
    let t = std::time::Instant::now();
    let stats = recv_store.remote().fetch(incoming.clone(), content).complete().await.unwrap();
    eprintln!("second pull: {} payload bytes in {:?} (first pull had {got})", stats.payload_bytes_read, t.elapsed());
    assert!(stats.payload_bytes_read < (BIG as u64) * 6 / 10, "resume fetched {} bytes", stats.payload_bytes_read);

    // 4. The files come out whole, with their tree.
    let out = dir.path().join("etd-downloads");
    let paths = export(&recv_store, root, &out).await;
    assert_eq!(paths, [out.join("folder/big.bin"), out.join("folder/sub/note.txt")]);
    assert_eq!(std::fs::read(out.join("folder/sub/note.txt")).unwrap(), b"look at big.bin");
    assert!(std::fs::read(out.join("folder/big.bin")).unwrap() == std::fs::read(folder.join("big.bin")).unwrap());

    conn.close(0u32.into(), b"done");
    send_store.shutdown().await.unwrap();
    recv_store.shutdown().await.unwrap();
}
