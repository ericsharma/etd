//! et-link against an in-process etd, on offline localhost endpoints.
//! No tmux here: only pairing, exec, upload, and connection errors.
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use et_link::{Link, LinkError};
use et_proto::ALPN;
use etd::config::Paths;
use etd::{pair, server};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, SecretKey};

async fn phone() -> std::sync::Arc<Link> {
    let endpoint = Endpoint::builder(presets::Minimal).secret_key(SecretKey::generate()).bind().await.unwrap();
    Link::from_endpoint(endpoint)
}

#[tokio::test(flavor = "multi_thread")]
async fn link_end_to_end() {
    let home = tempfile::tempdir().unwrap();
    let files = tempfile::tempdir().unwrap();
    let env = [("HOME".to_string(), home.path().display().to_string()), ("PATH".to_string(), "/run/current-system/sw/bin:/usr/bin:/bin".to_string())]
        .into_iter()
        .collect();
    let paths = Paths::at(files.path().into());
    let endpoint = Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).bind().await.unwrap();
    etd::network::ensure(&paths, endpoint.id(), "hub").unwrap();
    let state = server::State::new(paths.clone(), env);
    let _router = server::serve(endpoint.clone(), state.clone());
    let watcher = tokio::spawn(server::watch(endpoint.clone(), state, "test".into()));
    let port = endpoint.bound_sockets().iter().find(|a| a.is_ipv4()).unwrap().port();
    let addr = EndpointAddr::new(endpoint.id()).with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)));
    let url = pair::create(&paths, addr, "testhost".into(), et_proto::network::Kind::Device).unwrap().to_url().unwrap();

    // Pair, then use the same connection's machine by id.
    let link = phone().await;
    let paired = link.pair(url.clone(), "test phone".into()).await.unwrap();
    assert_eq!(paired.host_name, etd::tmux::host_name());
    assert_eq!(paired.endpoint_id, endpoint.id().to_string());
    let host = link.connect(paired.endpoint_id.clone(), None).await.unwrap();

    let result = host.exec("echo hi; exit 2".into()).await.unwrap();
    assert_eq!(result.output, b"hi\n");
    assert_eq!(result.exit_code, Some(2));
    let written = host.upload("~/up/a.txt".into(), b"data".to_vec()).await.unwrap();
    assert_eq!(std::fs::read(written).unwrap(), b"data");
    assert!(matches!(host.upload("/etc/x".into(), b"x".to_vec()).await, Err(LinkError::Remote { .. })));
    assert!(host.ping().await.unwrap() >= 0.0);

    // The network: checked on the phone, with this phone in it.
    let net = host.network().await.unwrap();
    let mut names: Vec<(String, bool)> = net.members.iter().map(|m| (m.name.clone(), m.is_machine)).collect();
    names.sort();
    assert_eq!(names, [("hub".to_string(), true), ("test phone".to_string(), false)]);
    assert!(host.invite(false).await.unwrap().starts_with("et://pair?t="));
    assert!(host.invite(true).await.unwrap().starts_with("et://pair?t="));
    host.foreground(60).await.unwrap();

    // The link works once only.
    let other = phone().await;
    assert!(matches!(other.pair(url, "other".into()).await, Err(LinkError::PairingRefused { .. })));

    // A revoke closes the open connection with NotPaired.
    etd::network::revoke(&paths, endpoint.id(), "test phone").unwrap();
    let why = tokio::time::timeout(Duration::from_secs(5), host.closed()).await.expect("closed after revoke");
    assert!(matches!(why, LinkError::NotPaired { .. }), "{why:?}");

    watcher.abort();
}
