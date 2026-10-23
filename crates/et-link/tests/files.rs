//! `RemoteHost::send_files` against an in-process etd: a folder and a file
//! land in the machine's `~/etd-downloads/`; a send cancelled half way
//! succeeds when sent again; a missing file is `Invalid`.
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use et_link::{Link, LinkError, SendProgress};
use et_proto::ALPN;
use etd::config::Paths;
use etd::{pair, server};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, SecretKey};

#[derive(Default)]
struct Seen {
    done: AtomicU64,
    total: AtomicU64,
}

impl SendProgress for Seen {
    fn on_progress(&self, done: u64, total: u64) {
        self.done.store(done, Ordering::Relaxed);
        self.total.store(total, Ordering::Relaxed);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn send_files_to_a_machine() {
    let home = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let env = [("HOME".to_string(), home.path().display().to_string())].into_iter().collect();
    let paths = Paths::at(state_dir.path().into());
    let book = MemoryLookup::new();
    let endpoint = Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).address_lookup(book.clone()).bind().await.unwrap();
    book.add_endpoint_info(local_addr(&endpoint));
    etd::network::ensure(&paths, endpoint.id(), "hub").unwrap();
    let _router = server::serve(endpoint.clone(), server::State::new(paths.clone(), env));
    let port = endpoint.bound_sockets().iter().find(|a| a.is_ipv4()).unwrap().port();
    let addr = EndpointAddr::new(endpoint.id()).with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)));
    let url = pair::create(&paths, addr, "hub".into(), et_proto::network::Kind::Device).unwrap().to_url().unwrap();

    let phone = Endpoint::builder(presets::Minimal).secret_key(SecretKey::generate()).bind().await.unwrap();
    let link = Link::from_endpoint(phone);
    let paired = link.pair(url, "test phone".into()).await.unwrap();
    let host = link.connect(paired.endpoint_id, None).await.unwrap();

    let src = tempfile::tempdir().unwrap();
    let shots = src.path().join("shots");
    std::fs::create_dir_all(&shots).unwrap();
    std::fs::write(shots.join("a.png"), b"png").unwrap();
    let big: Vec<u8> = (0..120_000_000u32).map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8).collect();
    let video = src.path().join("clip.mov");
    std::fs::write(&video, &big).unwrap();
    let sent = vec![shots.display().to_string(), video.display().to_string()];
    let d = home.path().join("etd-downloads");

    // Cancel half way (the app drops the task), then send again.
    let seen = Arc::new(Seen::default());
    let mut first = Box::pin(host.send_files(sent.clone(), None, seen.clone()));
    loop {
        tokio::select! {
            result = &mut first => panic!("finished before the cancel: {result:?}"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(5)) => {
                if seen.done.load(Ordering::Relaxed) > 40_000_000 { break; }
            }
        }
    }
    drop(first);
    // etd notices at its next progress write, and always before it writes.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert!(!d.exists() || std::fs::read_dir(&d).unwrap().next().is_none(), "a cancelled send wrote files");

    let seen = Arc::new(Seen::default());
    let delivered = host.send_files(sent, None, seen.clone()).await.unwrap();
    assert_eq!(delivered.paths, [d.join("clip.mov").display().to_string(), d.join("shots").display().to_string()]);
    assert!(!delivered.typed);
    assert_eq!(seen.done.load(Ordering::Relaxed), seen.total.load(Ordering::Relaxed));
    assert_eq!(std::fs::read(d.join("clip.mov")).unwrap(), big);
    assert_eq!(std::fs::read(d.join("shots/a.png")).unwrap(), b"png");

    let missing = host.send_files(vec!["/no/such/file".into()], None, Arc::new(Seen::default())).await;
    assert!(matches!(missing, Err(LinkError::Invalid { .. })), "{missing:?}");

    // The Shared Files screen: list, preview, send on, delete.
    let list = host.shared_files().await.unwrap();
    let names: Vec<(&str, &str, bool)> = list.iter().map(|f| (f.name.as_str(), f.from.as_str(), f.is_dir)).collect();
    assert!(names.contains(&("clip.mov", "test phone", false)) && names.contains(&("shots", "test phone", true)), "{names:?}");
    let clip = list.iter().find(|f| f.name == "clip.mov").unwrap();
    assert_eq!((clip.machine.as_str(), clip.size), ("hub", big.len() as u64));

    let cache = tempfile::tempdir().unwrap();
    let seen = Arc::new(Seen::default());
    let local = host.fetch_file(clip.path.clone(), cache.path().display().to_string(), seen.clone()).await.unwrap();
    assert_eq!(std::fs::read(&local).unwrap(), big);
    assert_eq!(seen.done.load(Ordering::Relaxed), big.len() as u64);
    let shots = list.iter().find(|f| f.name == "shots").unwrap();
    let local = host.fetch_file(shots.path.clone(), cache.path().display().to_string(), Arc::new(Seen::default())).await.unwrap();
    assert_eq!(std::fs::read(std::path::Path::new(&local).join("a.png")).unwrap(), b"png");
    let outside = host
        .fetch_file(home.path().join(".bashrc").display().to_string(), cache.path().display().to_string(), Arc::new(Seen::default()))
        .await;
    assert!(matches!(outside, Err(LinkError::Remote { .. })), "{outside:?}");

    // Send on to a second machine: machine to machine, nothing through here.
    let other_home = tempfile::tempdir().unwrap();
    let other_state = tempfile::tempdir().unwrap();
    let other_paths = Paths::at(other_state.path().into());
    let other = Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).address_lookup(book.clone()).bind().await.unwrap();
    book.add_endpoint_info(local_addr(&other));
    let other_env = [("HOME".to_string(), other_home.path().display().to_string())].into_iter().collect();
    etd::network::ensure(&other_paths, other.id(), "box").unwrap();
    let _other_router = server::serve(other.clone(), server::State::new(other_paths.clone(), other_env));
    let invite = pair::invite_url(&paths, &local_addr(&endpoint), "hub", et_proto::network::Kind::Machine).unwrap();
    etd::network::join(&other, &other_paths, &invite, "box").await.unwrap();
    let sent_on = host.share_file(clip.path.clone(), "box".into(), None, Arc::new(Seen::default())).await.unwrap();
    assert_eq!(sent_on.paths, [other_home.path().join("etd-downloads/clip.mov").display().to_string()]);
    assert_eq!(std::fs::read(other_home.path().join("etd-downloads/clip.mov")).unwrap(), big);
    let no_session = host.share_file(clip.path.clone(), "hub".into(), None, Arc::new(Seen::default())).await;
    assert!(matches!(no_session, Err(LinkError::Remote { .. })), "{no_session:?}");

    host.unshare_file(shots.path.clone()).await.unwrap();
    assert!(!d.join("shots").exists());
    assert!(host.shared_files().await.unwrap().iter().all(|f| f.name != "shots"));
}

fn local_addr(endpoint: &Endpoint) -> EndpointAddr {
    let port = endpoint.bound_sockets().iter().find(|a| a.is_ipv4()).unwrap().port();
    EndpointAddr::new(endpoint.id()).with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
}
