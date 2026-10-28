//! The Logbook end to end: a Claude transcript in a "manymux session" of
//! gmktec goes to the archive trigkey, line by line, through restarts,
//! pauses and the end of the agent. The session leader is a real `sh`
//! process, the agent a real `sleep` under it, so the parent walk runs on
//! the real process table.
use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;

use et_proto::network::Kind;
use et_proto::ALPN;
use etd::config::Paths;
use etd::logbook::capture::{self, Archive, Capture, Net};
use etd::logbook::claude::Claude;
use etd::logbook::store::Store;
use etd::logbook::{self, Settings};
use etd::{network, pair, server};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr};

fn local_addr(endpoint: &Endpoint) -> EndpointAddr {
    let port = endpoint.bound_sockets().iter().find(|a| a.is_ipv4()).unwrap().port();
    EndpointAddr::new(endpoint.id()).with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
}

struct Machine {
    endpoint: Endpoint,
    paths: Paths,
    _dir: tempfile::TempDir,
    _router: iroh::protocol::Router,
}

impl Machine {
    async fn start(name: &str, book: &MemoryLookup) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::at(dir.path().into());
        let endpoint = Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).address_lookup(book.clone()).bind().await.unwrap();
        book.add_endpoint_info(local_addr(&endpoint));
        network::ensure(&paths, endpoint.id(), name).unwrap();
        let env = [("HOME".to_string(), dir.path().display().to_string())].into_iter().collect();
        let router = server::serve(endpoint.clone(), server::State::new(paths.clone(), env));
        Self { endpoint, paths, _dir: dir, _router: router }
    }
}

/// Killed when dropped.
struct Proc(Child);

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn append(path: &Path, text: &str) {
    use std::io::Write;
    std::fs::OpenOptions::new().append(true).open(path).unwrap().write_all(text.as_bytes()).unwrap();
}

fn archived(trigkey: &Machine, id: &str) -> String {
    std::fs::read_to_string(trigkey.paths.logbook().join("gmktec").join(format!("{id}.jsonl"))).unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
async fn transcripts_go_to_the_archive() {
    let book = MemoryLookup::new();
    let trigkey = Machine::start("trigkey", &book).await;
    let gmktec = Machine::start("gmktec", &book).await;
    let url = pair::invite_url(&trigkey.paths, &local_addr(&trigkey.endpoint), "trigkey", Kind::Machine).unwrap();
    network::join(&gmktec.endpoint, &gmktec.paths, &url, "gmktec").await.unwrap();
    let env: etd::tmux::Env = std::env::vars().collect();
    let net = Net { endpoint: gmktec.endpoint.clone(), connections: Arc::default(), me: "gmktec".into() };

    // No archive yet: nothing to send to.
    let (none, _) = logbook::archives(&gmktec.paths, &Default::default(), &net).await;
    assert!(none.is_empty());
    logbook::save_settings(&trigkey.paths, &Settings { archive: true }).unwrap();
    let (archives, _) = logbook::archives(&gmktec.paths, &Default::default(), &net).await;
    assert_eq!(archives.iter().map(|a| a.name("gmktec")).collect::<Vec<_>>(), ["trigkey"]);

    // The session: `sh` (the leader) with the agent under it. `; true`
    // keeps sh from replacing itself with sleep.
    let leader = Proc(Command::new("sh").args(["-c", "sleep 60; true"]).spawn().unwrap());
    let leader_pid = leader.0.id();
    let mut agent_pid = None;
    for _ in 0..50 {
        let table = capture::parents(&env).await.unwrap();
        agent_pid = table.iter().find(|(_, parent)| **parent == leader_pid).map(|(pid, _)| *pid);
        if agent_pid.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let agent_pid = agent_pid.expect("sleep under sh");
    // An orphan: an agent in no session (its parent is this test).
    let orphan = Proc(Command::new("sleep").arg("60").spawn().unwrap());

    let claude = tempfile::tempdir().unwrap();
    let id = "23eba963-d720-4ef2-be81-4c77fa6c9f7b";
    let orphan_id = "0cbe0b81-5b3b-4e27-a8d5-f8d6155c86f3";
    std::fs::create_dir_all(claude.path().join("sessions")).unwrap();
    std::fs::create_dir_all(claude.path().join("projects/-home-eric")).unwrap();
    let cwd = claude.path().display().to_string();
    for (pid, sid) in [(agent_pid, id), (orphan.0.id(), orphan_id)] {
        let text = format!(r#"{{"pid":{pid},"sessionId":"{sid}","cwd":"{cwd}","status":"busy"}}"#);
        std::fs::write(claude.path().join(format!("sessions/{pid}.json")), text).unwrap();
    }
    let transcript: PathBuf = claude.path().join(format!("projects/-home-eric/{id}.jsonl"));
    std::fs::write(&transcript, "{\"type\":\"user\",\"n\":1}\n{\"type\":\"assistant\",\"n\":2}\n").unwrap();
    std::fs::write(claude.path().join(format!("projects/-home-eric/{orphan_id}.jsonl")), "{\"orphan\":true}\n").unwrap();

    let sessions = vec![("claude".to_string(), leader_pid)];
    let none_paused = HashSet::new();
    let mut capture = Capture::new(vec![Box::new(Claude::at(claude.path().into()))], env.clone());
    let statuses = capture.tick(&sessions, &archives, &net, &none_paused).await;
    assert_eq!(statuses.len(), 1, "the orphan is not recorded: {statuses:?}");
    assert_eq!((statuses[0].mm_session.as_str(), statuses[0].session_id.as_str()), ("claude", id));
    assert_eq!(archived(&trigkey, id), std::fs::read_to_string(&transcript).unwrap());
    assert!(!trigkey.paths.logbook().join(format!("gmktec/{orphan_id}.jsonl")).exists());

    // Half a line waits for its end.
    append(&transcript, "{\"type\":\"user\",");
    capture.tick(&sessions, &archives, &net, &none_paused).await;
    assert_eq!(archived(&trigkey, id).lines().count(), 2);
    append(&transcript, "\"n\":3}\n");
    capture.tick(&sessions, &archives, &net, &none_paused).await;
    assert_eq!(archived(&trigkey, id), std::fs::read_to_string(&transcript).unwrap());

    // etd restarts on gmktec: it asks where the archive is, sends nothing twice.
    let mut capture = Capture::new(vec![Box::new(Claude::at(claude.path().into()))], env.clone());
    append(&transcript, "{\"n\":4}\n");
    capture.tick(&sessions, &archives, &net, &none_paused).await;
    assert_eq!(archived(&trigkey, id), std::fs::read_to_string(&transcript).unwrap());

    // "Do not record" this session: the lines stay on gmktec; a gap line
    // says how much is missing; recording goes on after it.
    let Archive::There { id: trigkey_id, name, .. } = &archives[0] else { panic!("an archive on another machine") };
    let paused = vec![Archive::There { id: *trigkey_id, name: name.clone(), paused: ["claude".to_string()].into() }];
    append(&transcript, "{\"secret\":\"hunter2\"}\n");
    capture.tick(&sessions, &paused, &net, &none_paused).await;
    let text = archived(&trigkey, id);
    assert!(!text.contains("hunter2"), "{text}");
    assert!(text.ends_with("{\"type\":\"etd-gap\",\"skipped\":21}\n"), "{text}");
    append(&transcript, "{\"n\":5}\n");
    capture.tick(&sessions, &archives, &net, &none_paused).await;
    assert!(archived(&trigkey, id).ends_with("\"skipped\":21}\n{\"n\":5}\n"));

    // The agent ends: the archive hears "gone", gmktec stops following it.
    assert!(Command::new("kill").arg(agent_pid.to_string()).status().unwrap().success());
    let mut statuses = Vec::new();
    for _ in 0..50 {
        statuses = capture.tick(&sessions, &archives, &net, &none_paused).await;
        if statuses.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(statuses.is_empty(), "{statuses:?}");
    let entries = Store::read_entries(&trigkey.paths.logbook()).unwrap();
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!((entry.machine.as_str(), entry.meta.status.as_str(), entry.meta.agent.as_str()), ("gmktec", "gone", "claude"));
    assert_eq!(entry.meta.cwd, cwd);
    assert_eq!(entry.offset, std::fs::metadata(&transcript).unwrap().len());
}
