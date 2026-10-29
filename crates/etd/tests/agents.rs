//! `Agents` over et/1 (et-ios docs/PLAN-multi-agent.md §4.1): a member asks
//! a machine which agent CLIs it has.
use std::net::{Ipv4Addr, SocketAddr};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use et_proto::agents::AgentsInfo;
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

fn script(dir: &Path, name: &str, body: &str) {
    let path = dir.join(name);
    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_member_asks_which_agents_a_machine_has() {
    let book = MemoryLookup::new();
    let bin = tempfile::tempdir().unwrap();
    script(bin.path(), "claude", "echo '2.1.4 (Claude Code)'");
    script(bin.path(), "gemini", "echo 0.66.0");

    let dir = tempfile::tempdir().unwrap();
    let paths = Paths::at(dir.path().into());
    let endpoint = Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).address_lookup(book.clone()).bind().await.unwrap();
    book.add_endpoint_info(local_addr(&endpoint));
    network::ensure(&paths, endpoint.id(), "trigkey").unwrap();
    let env = [("HOME".to_string(), dir.path().display().to_string()), ("PATH".to_string(), bin.path().display().to_string())]
        .into_iter()
        .collect();
    let _router = server::serve(endpoint.clone(), server::State::new(paths.clone(), env));

    let other_dir = tempfile::tempdir().unwrap();
    let other_paths = Paths::at(other_dir.path().into());
    let other = Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).address_lookup(book.clone()).bind().await.unwrap();
    let url = pair::invite_url(&paths, &local_addr(&endpoint), "trigkey", Kind::Machine).unwrap();
    network::join(&other, &other_paths, &url, "gmktec").await.unwrap();

    let conn = other.connect(local_addr(&endpoint), ALPN).await.unwrap();
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    write_frame(&mut send, &Request::Agents).await.unwrap();
    send.finish().unwrap();
    let Some(Response::Agents { json }) = read_frame(&mut recv).await.unwrap() else { panic!("an Agents answer") };
    let info: AgentsInfo = serde_json::from_str(&json).unwrap();
    let found: Vec<(&str, Option<&str>)> = info.agents.iter().map(|a| (a.id.as_str(), a.version.as_deref())).collect();
    assert_eq!(found, [("claude", Some("2.1.4")), ("codex", None), ("gemini", Some("0.66.0")), ("grok", None)]);
}
