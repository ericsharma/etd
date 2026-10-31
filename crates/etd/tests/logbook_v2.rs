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

    let requests = [(Request::Meta { refresh: false }, "A4"), (Request::MetaDecision { json: "{}".into() }, "A4")];
    for (request, package) in requests {
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        write_frame(&mut send, &request).await.unwrap();
        send.finish().unwrap();
        let answer: Option<Response> = read_frame(&mut recv).await.unwrap();
        let Some(Response::Error(message)) = answer else { panic!("{request:?}: an error, got {answer:?}") };
        assert!(message.starts_with("not built yet") && message.contains(package), "{request:?}: {message}");
    }

    // A3: read and write the agent instruction files.
    let ask = |request: Request| {
        let conn = conn.clone();
        async move {
            let (mut send, mut recv) = conn.open_bi().await.unwrap();
            write_frame(&mut send, &request).await.unwrap();
            send.finish().unwrap();
            read_frame::<Response>(&mut recv).await.unwrap().unwrap()
        }
    };
    let files = |r: Response| {
        let Response::AgentConfig { json } = r else { panic!("an AgentConfig answer, got {r:?}") };
        serde_json::from_str::<serde_json::Value>(&json).unwrap()["files"].as_array().unwrap().clone()
    };
    assert!(files(ask(Request::AgentConfig).await).is_empty());
    let write = |base: String, content: &str| Request::ConfigWrite { path: "~/.claude/CLAUDE.md".into(), base, content: content.into() };
    assert_eq!(ask(write(String::new(), "Be brief.\n")).await, Response::ConfigWritten { backup: String::new() });
    let listed = files(ask(Request::AgentConfig).await);
    assert_eq!(
        (listed.len(), listed[0]["path"].as_str(), listed[0]["content"].as_str()),
        (1, Some("~/.claude/CLAUDE.md"), Some("Be brief.\n"))
    );
    let sha = listed[0]["sha256"].as_str().unwrap().to_string();
    let Response::ConfigWritten { backup } = ask(write(sha.clone(), "Be very brief.\n")).await else { panic!("written") };
    assert_eq!(std::fs::read_to_string(&backup).unwrap(), "Be brief.\n");
    assert!(backup.starts_with(&dir.path().display().to_string()), "backups go to etd's data folder");
    let Response::Error(e) = ask(write(sha, "again")).await else { panic!("the old base is refused") };
    assert!(e.contains("changed since"), "{e}");
    let settings = Request::ConfigWrite { path: "~/.claude/settings.json".into(), base: String::new(), content: "{}".into() };
    let Response::Error(e) = ask(settings).await else { panic!("settings are refused") };
    assert!(e.contains("not an agent instruction file"), "{e}");
}
