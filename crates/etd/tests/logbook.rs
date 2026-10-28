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
        // The watcher's `claude -p` is a script (`fake_claude`).
        let env = [
            ("HOME".to_string(), dir.path().display().to_string()),
            ("PATH".to_string(), std::env::var("PATH").unwrap_or_default()),
            ("ETD_WATCHER_COMMAND".to_string(), dir.path().join("claude").display().to_string()),
        ]
        .into_iter()
        .collect();
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

/// The next frame of a stream, within 10 s.
async fn next(down: &mut iroh::endpoint::RecvStream) -> Option<et_proto::Response> {
    tokio::time::timeout(Duration::from_secs(10), et_proto::read_frame(down)).await.expect("a frame in time").unwrap()
}

/// One et/1 request and its first answer.
async fn ask(conn: &iroh::endpoint::Connection, request: et_proto::Request) -> et_proto::Response {
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    et_proto::write_frame(&mut send, &request).await.unwrap();
    send.finish().unwrap();
    et_proto::read_frame(&mut recv).await.unwrap().expect("an answer")
}

/// What the apps do (PLAN-logbook §4.5): find the archive, list, follow a
/// transcript live, pause, and send exchanges into another machine.
#[tokio::test(flavor = "multi_thread")]
async fn the_apps_read_the_archive() {
    use et_proto::logbook::LogSelection;
    use et_proto::{read_frame, write_frame, Request, Response};

    let book = MemoryLookup::new();
    let trigkey = Machine::start("trigkey", &book).await;
    let gmktec = Machine::start("gmktec", &book).await;
    let url = pair::invite_url(&trigkey.paths, &local_addr(&trigkey.endpoint), "trigkey", Kind::Machine).unwrap();
    network::join(&gmktec.endpoint, &gmktec.paths, &url, "gmktec").await.unwrap();
    logbook::save_settings(&trigkey.paths, &Settings { archive: true }).unwrap();
    let env: etd::tmux::Env = std::env::vars().collect();
    let net = Net { endpoint: gmktec.endpoint.clone(), connections: Arc::default(), me: "gmktec".into() };
    let (archives, _) = logbook::archives(&gmktec.paths, &Default::default(), &net).await;

    // The agent is the session's leader here (as `ws` starts claude).
    let agent = Proc(Command::new("sleep").arg("60").spawn().unwrap());
    let sessions = vec![("claude".to_string(), agent.0.id())];
    let claude = tempfile::tempdir().unwrap();
    let id = "23eba963-d720-4ef2-be81-4c77fa6c9f7b";
    std::fs::create_dir_all(claude.path().join("sessions")).unwrap();
    std::fs::create_dir_all(claude.path().join("projects/-home-eric")).unwrap();
    let pid = agent.0.id();
    std::fs::write(
        claude.path().join(format!("sessions/{pid}.json")),
        format!(r#"{{"pid":{pid},"sessionId":"{id}","cwd":"/tmp","status":"busy"}}"#),
    )
    .unwrap();
    let transcript = claude.path().join(format!("projects/-home-eric/{id}.jsonl"));
    let first = r#"{"type":"user","uuid":"p1","message":{"role":"user","content":"Why does CI fail?"}}"#;
    let reply = r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"nixfmt."}]}}"#;
    let title = r#"{"type":"ai-title","aiTitle":"Fix the build"}"#;
    std::fs::write(&transcript, format!("{first}\n{reply}\n{title}\n")).unwrap();
    let mut capture = Capture::new(vec![Box::new(Claude::at(claude.path().into()))], env);
    let none_paused = HashSet::new();
    capture.tick(&sessions, &archives, &net, &none_paused).await;

    // A device would do the same; gmktec's endpoint is a member too.
    let conn = gmktec.endpoint.connect(local_addr(&trigkey.endpoint), ALPN).await.unwrap();
    assert_eq!(ask(&conn, Request::LogArchives).await, Response::Machines(vec!["trigkey".into()]));
    let Response::LogList(list) = ask(&conn, Request::LogSessions { since: 0 }).await else { panic!("a list") };
    assert_eq!(list.len(), 1);
    assert_eq!(
        (list[0].machine.as_str(), list[0].meta.mm_session.as_str(), list[0].title.as_deref()),
        ("gmktec", "claude", Some("Fix the build"))
    );
    let stored = list[0].bytes;
    assert_eq!(ask(&conn, Request::Report { refresh: false }).await, Response::Report { json: et_proto::logbook::NO_REPORT.into() });

    // Follow from inside the first line: it starts at the second; the
    // stored bytes come in one chunk, which says "live".
    let (mut up, mut down) = conn.open_bi().await.unwrap();
    write_frame(&mut up, &Request::LogRead { machine: "gmktec".into(), session_id: id.into(), from: 5 }).await.unwrap();
    let Some(Response::LogChunk { offset, bytes, live }) = next(&mut down).await else { panic!("a chunk") };
    assert_eq!(String::from_utf8(bytes.clone()).unwrap(), format!("{reply}\n{title}\n"));
    assert_eq!((offset, offset - bytes.len() as u64, live), (stored, first.len() as u64 + 1, true));
    // A new line on gmktec arrives at once (the append wakes the follower).
    let later = r#"{"type":"user","uuid":"p2","message":{"role":"user","content":"Deploy it."}}"#;
    append(&transcript, &format!("{later}\n"));
    capture.tick(&sessions, &archives, &net, &none_paused).await;
    let Some(Response::LogChunk { bytes, live, .. }) = next(&mut down).await else { panic!("a live chunk") };
    assert_eq!((String::from_utf8(bytes).unwrap(), live), (format!("{later}\n"), true));
    // The app closes its side: the archive ends the stream.
    up.finish().unwrap();
    assert_eq!(next(&mut down).await, None);

    // Do not record: the archive drops the lines even before gmktec knows.
    assert_eq!(ask(&conn, Request::LogPause { machine: "gmktec".into(), mm_session: "claude".into(), paused: true }).await, Response::Pong);
    append(&transcript, "{\"secret\":\"hunter2\"}\n");
    capture.tick(&sessions, &archives, &net, &none_paused).await;
    assert!(!archived(&trigkey, id).contains("hunter2"));
    let Response::LogList(list) = ask(&conn, Request::LogSessions { since: 0 }).await else { panic!("a list") };
    assert!(list[0].paused);

    // Send the first exchange into gmktec (not typed: no session given).
    let selection = LogSelection::Turns { machine: "gmktec".into(), session_id: id.into(), turn_ids: vec!["p1".into()] };
    let (mut up, mut down) = conn.open_bi().await.unwrap();
    write_frame(&mut up, &Request::LogSend { selection: selection.clone(), to: "gmktec".into(), session: String::new() }).await.unwrap();
    up.finish().unwrap();
    let paths = loop {
        match read_frame::<Response>(&mut down).await.unwrap() {
            Some(Response::Progress { .. }) => continue,
            Some(Response::Delivered { paths, typed }) => {
                assert!(!typed);
                break paths;
            }
            other => panic!("{other:?}"),
        }
    };
    assert!(paths[0].starts_with(&gmktec._dir.path().join("etd-downloads").display().to_string()), "{paths:?}");
    let doc = std::fs::read_to_string(&paths[0]).unwrap();
    assert!(doc.contains("# From the Logbook: gmktec/claude") && doc.contains("Why does CI fail?") && doc.contains("nixfmt."), "{doc}");
    assert!(!doc.contains("Deploy it."), "only the chosen exchange");
    // To the archive itself.
    let (mut up, mut down) = conn.open_bi().await.unwrap();
    write_frame(&mut up, &Request::LogSend { selection, to: "trigkey".into(), session: String::new() }).await.unwrap();
    up.finish().unwrap();
    let Some(Response::Delivered { paths, .. }) = read_frame::<Response>(&mut down).await.unwrap() else { panic!("delivered") };
    assert!(std::fs::read_to_string(&paths[0]).unwrap().contains("Why does CI fail?"));

    // Refresh the report: progress, then the report.
    fake_claude(trigkey._dir.path(), &format!("gmktec/{id}"));
    let (mut up, mut down) = conn.open_bi().await.unwrap();
    write_frame(&mut up, &Request::Report { refresh: true }).await.unwrap();
    up.finish().unwrap();
    let mut stages = Vec::new();
    let json = loop {
        match next(&mut down).await {
            Some(Response::ReportProgress { stage, done, total }) => stages.push(format!("{stage} {done}/{total}")),
            Some(Response::Report { json }) => break json,
            other => panic!("{other:?}"),
        }
    };
    assert!(stages.contains(&"Report 0/1".to_string()), "{stages:?}");
    let report: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(report["projects"][0]["sessions"][0]["key"], format!("gmktec/{id}"));
    let report_id = report["id"].as_str().unwrap().to_string();

    // The owner renames the project; the last report shows it at once.
    let rename = r#"{"decision":"edit","kind":"rename","name":"Green CI","project":"ci"}"#;
    assert_eq!(ask(&conn, Request::Grouping { change_json: rename.into() }).await, Response::Pong);
    assert!(matches!(ask(&conn, Request::Grouping { change_json: "{}".into() }).await, Response::Error(_)));
    let Response::Report { json } = ask(&conn, Request::Report { refresh: false }).await else { panic!("a report") };
    assert!(json.contains(r#""name":"Green CI""#), "{json}");

    // Send the project's card into gmktec.
    let selection = LogSelection::ReportSection { report_id, project_id: "ci".into() };
    let (mut up, mut down) = conn.open_bi().await.unwrap();
    write_frame(&mut up, &Request::LogSend { selection, to: "gmktec".into(), session: String::new() }).await.unwrap();
    up.finish().unwrap();
    let paths = loop {
        match read_frame::<Response>(&mut down).await.unwrap() {
            Some(Response::Progress { .. }) => continue,
            Some(Response::Delivered { paths, .. }) => break paths,
            other => panic!("{other:?}"),
        }
    };
    let doc = std::fs::read_to_string(&paths[0]).unwrap();
    assert!(doc.starts_with("# From the Logbook report: Green CI") && doc.contains("gmktec/claude (Fix the build): works"), "{doc}");
}

/// A stand-in for `claude -p` (the archive's `ETD_WATCHER_COMMAND`): a
/// digest, or a report with one project that holds `key`.
fn fake_claude(dir: &Path, key: &str) {
    use std::os::unix::fs::PermissionsExt;
    let report = format!(
        r#"{{\"projects\":[{{\"id\":\"ci\",\"name\":\"CI\",\"goal\":\"green CI\",\"status\":\"active\",\"sessions\":[{{\"key\":\"{key}\",\"role\":\"works\"}}],\"progress\":\"ok\",\"next\":[]}}]}}"#
    );
    let script = format!(
        "#!/bin/sh\ncase \"$(cat)\" in\n  *\"progress report of the owner\"*) printf '%s' '{{\"type\":\"result\",\"is_error\":false,\"result\":\"{report}\"}}' ;;\n  *) printf '%s' '{{\"type\":\"result\",\"is_error\":false,\"result\":\"Goal: green CI\"}}' ;;\nesac\n"
    );
    let path = dir.join("claude");
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// `claude --resume <id>` writes a new transcript without the old one: the
/// capture sends the old one too, as ended, and links the new one to it.
#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_session_brings_its_earlier_transcript() {
    let book = MemoryLookup::new();
    let trigkey = Machine::start("trigkey", &book).await;
    let gmktec = Machine::start("gmktec", &book).await;
    let url = pair::invite_url(&trigkey.paths, &local_addr(&trigkey.endpoint), "trigkey", Kind::Machine).unwrap();
    network::join(&gmktec.endpoint, &gmktec.paths, &url, "gmktec").await.unwrap();
    logbook::save_settings(&trigkey.paths, &Settings { archive: true }).unwrap();
    let env: etd::tmux::Env = std::env::vars().collect();
    let net = Net { endpoint: gmktec.endpoint.clone(), connections: Arc::default(), me: "gmktec".into() };
    let (archives, _) = logbook::archives(&gmktec.paths, &Default::default(), &net).await;

    let old = "14bbbb56-d376-4aeb-868a-1e3b09abb04e";
    let new = "fcca3b1b-6946-4e74-98f0-44205e969389";
    // The agent (and session leader): its command line says `--resume <old>`.
    let agent = Proc(Command::new("sh").args(["-c", "sleep 60; true", "claude", "--resume", old]).spawn().unwrap());
    let pid = agent.0.id();
    let sessions = vec![("claude2".to_string(), pid)];
    let claude = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(claude.path().join("sessions")).unwrap();
    std::fs::create_dir_all(claude.path().join("projects/-Users-x")).unwrap();
    std::fs::write(
        claude.path().join(format!("sessions/{pid}.json")),
        format!(r#"{{"pid":{pid},"sessionId":"{new}","cwd":"/tmp","status":"busy"}}"#),
    )
    .unwrap();
    let earlier = "{\"type\":\"user\",\"uuid\":\"p0\",\"message\":{\"role\":\"user\",\"content\":\"Plan the Logbook\"}}\n";
    std::fs::write(claude.path().join(format!("projects/-Users-x/{old}.jsonl")), earlier).unwrap();
    std::fs::write(claude.path().join(format!("projects/-Users-x/{new}.jsonl")), "{\"a\":1}\n").unwrap();

    let mut capture = Capture::new(vec![Box::new(Claude::at(claude.path().into()))], env);
    let none = HashSet::new();
    capture.tick(&sessions, &archives, &net, &none).await;
    assert_eq!(archived(&trigkey, old), earlier, "the earlier transcript, whole");
    assert_eq!(archived(&trigkey, new), "{\"a\":1}\n");
    let entries = Store::read_entries(&trigkey.paths.logbook()).unwrap();
    let get = |id: &str| entries.iter().find(|e| e.session_id == id).unwrap().clone();
    assert_eq!((get(old).meta.status.as_str(), get(old).meta.mm_session.as_str()), ("gone", "claude2"));
    assert_eq!(get(new).meta.previous.as_deref(), Some(old));

    // A restart of etd: nothing twice, and the link stays.
    let mut again = Capture::new(vec![Box::new(Claude::at(claude.path().into()))], std::env::vars().collect());
    again.tick(&sessions, &archives, &net, &none).await;
    assert_eq!(archived(&trigkey, old), earlier);
    let entries = Store::read_entries(&trigkey.paths.logbook()).unwrap();
    assert_eq!(entries.iter().find(|e| e.session_id == new).unwrap().meta.previous.as_deref(), Some(old));
}
