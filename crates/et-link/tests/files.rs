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
    let endpoint = Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).bind().await.unwrap();
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
}
