//! et-ios docs/PLAN-logbook-v2.md W0: the new requests are on the wire, and
//! each answers a clear "not built yet" until its package is in.
use std::net::{Ipv4Addr, SocketAddr};

use et_proto::network::Kind;
use et_proto::{read_frame, write_frame, Request, Response, ALPN};
use etd::config::Paths;
use etd::{network, pair, server};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr};

fn local_addr(endpoint: &Endpoint) -> EndpointAddr {
    let port = endpoint.bound_sockets().iter().find(|a| a.is_ipv4()).unwrap().port();
    EndpointAddr::new(endpoint.id()).with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
}

#[tokio::test(flavor = "multi_thread")]
async fn new_requests_answer_not_built_yet() {
    let book = MemoryLookup::new();
    let dir = tempfile::tempdir().unwrap();
    let paths = Paths::at(dir.path().into());
    let endpoint = Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).address_lookup(book.clone()).bind().await.unwrap();
    book.add_endpoint_info(local_addr(&endpoint));
    network::ensure(&paths, endpoint.id(), "trigkey").unwrap();
    let env = [("HOME".to_string(), dir.path().display().to_string())].into_iter().collect();
    let _router = server::serve(endpoint.clone(), server::State::new(paths.clone(), env));

    let other_dir = tempfile::tempdir().unwrap();
    let other_paths = Paths::at(other_dir.path().into());
    let other = Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).address_lookup(book.clone()).bind().await.unwrap();
    let url = pair::invite_url(&paths, &local_addr(&endpoint), "trigkey", Kind::Machine).unwrap();
    network::join(&other, &other_paths, &url, "gmktec").await.unwrap();
    let conn = other.connect(local_addr(&endpoint), ALPN).await.unwrap();

    let requests = [
        (Request::AgentConfig, "A3"),
        (Request::ConfigWrite { path: "~/.claude/CLAUDE.md".into(), base: String::new(), content: "x".into() }, "A3"),
        (Request::Meta { refresh: false }, "A4"),
        (Request::MetaDecision { json: "{}".into() }, "A4"),
    ];
    for (request, package) in requests {
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        write_frame(&mut send, &request).await.unwrap();
        send.finish().unwrap();
        let answer: Option<Response> = read_frame(&mut recv).await.unwrap();
        let Some(Response::Error(message)) = answer else { panic!("{request:?}: an error, got {answer:?}") };
        assert!(message.starts_with("not built yet") && message.contains(package), "{request:?}: {message}");
    }
    assert!(!dir.path().join(".claude").exists(), "ConfigWrite wrote nothing");
}
