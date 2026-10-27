//! `Agent`, `ExecOn` and `Machines` against a throwaway manymux node.
//!
//! Isolation: every `mm` here is a wrapper that always passes
//! `--socket <temp>`, with `MM_CONFIG_DIR=<temp>`. The node's `MM_SSH` is
//! `false`. The real node (`/tmp/manymux-<uid>`) and the real machines are
//! never reached. "fakehost" is a second etd in the network, on the same node.
use std::net::{Ipv4Addr, SocketAddr};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Duration;

use et_proto::network::Kind;
use et_proto::{read_frame, write_frame, ControlFrame, Request, Response, ALPN};
use etd::config::Paths;
use etd::{network, pair, server};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{presets, Connection};
use iroh::{Endpoint, EndpointAddr, SecretKey};

fn real_mm() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path).map(|d| d.join("mm")).find(|p| p.is_file())
}

fn script(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// A manymux node on its own socket, stopped on drop.
struct Node {
    dir: tempfile::TempDir,
    child: Child,
}

impl Node {
    fn bin(&self) -> PathBuf {
        self.dir.path().join("bin")
    }

    fn config(&self) -> PathBuf {
        self.dir.path().join("config")
    }

    fn start(real: &Path) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("mm.sock");
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        script(&bin.join("mm"), &format!("#!/bin/sh\nexec '{}' --socket '{}' \"$@\"\n", real.display(), socket.display()));
        let child = Command::new(bin.join("mm"))
            .args(["daemon", "--no-notify"])
            .env("MM_CONFIG_DIR", dir.path().join("config"))
            // The node logs to $XDG_STATE_HOME/manymux: keep it out of the real one.
            .env("XDG_STATE_HOME", dir.path().join("state"))
            // The node never reaches the machine it watches (fakehost).
            .env("MM_SSH", "false")
            .env_remove("TMUX")
            .spawn()
            .unwrap();
        for _ in 0..100 {
            if socket.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(socket.exists(), "test node did not start");
        Self { dir, child }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn call(conn: &Connection, request: Request) -> Response {
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    write_frame(&mut send, &request).await.unwrap();
    send.finish().unwrap();
    read_frame(&mut recv).await.unwrap().unwrap()
}

/// Opens `Agent`, sends manymux `List`, and returns the decoded answer.
#[allow(clippy::result_large_err)] // test helper
async fn agent_list(conn: &Connection, host: Option<&str>) -> Result<manymux::proto::Response, Response> {
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    write_frame(&mut send, &Request::Agent { host: host.map(String::from) }).await.unwrap();
    match read_frame::<Response>(&mut recv).await.unwrap().unwrap() {
        Response::AgentStarted => {}
        other => return Err(other),
    }
    let mut request = Vec::new();
    manymux::proto::write_msg(&mut request, manymux::proto::tag::REQUEST, &manymux::proto::Request::List).await.unwrap();
    send.write_all(&request).await.unwrap();
    // manymux frames: [tag: u8][len: u32 BE][body], inside our ControlFrames.
    let mut bytes = Vec::new();
    loop {
        if bytes.len() >= 5 {
            let len = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
            if bytes.len() >= 5 + len {
                assert_eq!(bytes[0], manymux::proto::tag::RESPONSE);
                let answer = manymux::proto::decode(&bytes[5..5 + len]).unwrap();
                send.finish().unwrap();
                return Ok(answer);
            }
        }
        match tokio::time::timeout(Duration::from_secs(10), read_frame::<ControlFrame>(&mut recv)).await.unwrap().unwrap().unwrap() {
            ControlFrame::Stdout(b) => bytes.extend(b),
            ControlFrame::Stderr(line) => eprintln!("mm agent stderr: {line}"),
            ControlFrame::Exit(code) => panic!("mm agent exited early: {code:?}"),
        }
    }
}

fn local_addr(endpoint: &Endpoint) -> EndpointAddr {
    let port = endpoint.bound_sockets().iter().find(|a| a.is_ipv4()).unwrap().port();
    EndpointAddr::new(endpoint.id()).with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
}

/// An etd called `name` with `env`, reachable through `book`.
async fn etd_on(book: &MemoryLookup, name: &str, env: etd::tmux::Env) -> (Endpoint, tempfile::TempDir, iroh::protocol::Router) {
    let files = tempfile::tempdir().unwrap();
    let paths = Paths::at(files.path().into());
    let endpoint = Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).address_lookup(book.clone()).bind().await.unwrap();
    book.add_endpoint_info(local_addr(&endpoint));
    network::ensure(&paths, endpoint.id(), name).unwrap();
    let router = server::serve(endpoint.clone(), server::State::new(paths, env));
    (endpoint, files, router)
}

#[tokio::test(flavor = "multi_thread")]
async fn manymux_through_etd() {
    let Some(real) = real_mm() else {
        eprintln!("skipped: no `mm` on PATH");
        return;
    };
    let node = Node::start(&real);

    // The node watches one machine, "fakehost" (for `Machines`). Written
    // by hand: `mm add` would check it over ssh first.
    std::fs::create_dir_all(node.config()).unwrap();
    std::fs::write(node.config().join("hosts.toml"), "hosts = [\"fakehost\"]\n").unwrap();

    let home = tempfile::tempdir().unwrap();
    let mut env: etd::tmux::Env = std::env::vars().filter(|(k, _)| k != "TMUX" && k != "TMUX_PANE").collect();
    env.insert("PATH".into(), format!("{}:{}", node.bin().display(), env["PATH"]));
    env.insert("MM_CONFIG_DIR".into(), node.config().display().to_string());
    env.insert("HOME".into(), home.path().display().to_string());

    // Two etds on the same node: "hub", and "fakehost" in its network.
    let book = MemoryLookup::new();
    let (hub, hub_files, _hub_router) = etd_on(&book, "hub", env.clone()).await;
    let (fake, fake_files, _fake_router) = etd_on(&book, "fakehost", env).await;
    let hub_paths = Paths::at(hub_files.path().into());
    let url = pair::invite_url(&hub_paths, &local_addr(&hub), "hub", Kind::Machine).unwrap();
    network::join(&fake, &Paths::at(fake_files.path().into()), &url, "fakehost").await.unwrap();
    let phone = SecretKey::generate();
    network::admit(&hub_paths, hub.id(), phone.public(), "phone", Kind::Device).unwrap();
    let client = Endpoint::builder(presets::Minimal).secret_key(phone).bind().await.unwrap();
    let conn = client.connect(local_addr(&hub), ALPN).await.unwrap();

    assert_eq!(call(&conn, Request::Machines).await, Response::Machines(vec!["fakehost".into()]));

    // This machine's node, and fakehost's (the same test node, through the
    // network).
    for host in [None, Some("fakehost")] {
        match agent_list(&conn, host).await {
            Ok(manymux::proto::Response::Sessions(list)) => {
                assert!(list.is_empty(), "{host:?}: {list:?}")
            }
            other => panic!("{host:?}: {other:?}"),
        }
    }

    // A name that is not a machine of the network is refused.
    match agent_list(&conn, Some("evil.example")).await {
        Err(Response::Error(e)) => assert!(e.contains("not a machine"), "{e}"),
        other => panic!("{other:?}"),
    }
    match call(&conn, Request::ExecOn { host: "evil.example".into(), script: "true".into() }).await {
        Response::Error(e) => assert!(e.contains("not a machine"), "{e}"),
        other => panic!("{other:?}"),
    }

    // ExecOn: quoting survives, stderr is joined, the exit code comes back.
    match call(&conn, Request::ExecOn { host: "fakehost".into(), script: "echo $((1+2)); echo 'it'\"'\"'s' >&2; exit 4".into() }).await {
        Response::ExecResult { output, exit_code, .. } => {
            assert_eq!(String::from_utf8_lossy(&output), "3\nit's\n");
            assert_eq!(exit_code, Some(4));
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn bells_run_the_notify_command() {
    let Some(real) = real_mm() else {
        eprintln!("skipped: no `mm` on PATH");
        return;
    };
    let node = Node::start(&real);
    let notes = node.dir.path().join("notes");
    let mut env: etd::tmux::Env = std::env::vars().filter(|(k, _)| k != "TMUX" && k != "TMUX_PANE").collect();
    env.insert("PATH".into(), format!("{}:{}", node.bin().display(), env["PATH"]));
    env.insert("MM_CONFIG_DIR".into(), node.config().display().to_string());
    env.insert("XDG_STATE_HOME".into(), node.dir.path().join("state").display().to_string());
    env.insert("ETD_MM_SOCKET".into(), node.dir.path().join("mm.sock").display().to_string());
    let command = format!("echo \"$ETD_KIND|$ETD_SESSION|$ETD_BODY\" >> '{}'", notes.display());
    // A machine alone in its network: its own session's bell is pushed here.
    let files = tempfile::tempdir().unwrap();
    let paths = Paths::at(files.path().into());
    let endpoint = Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).bind().await.unwrap();
    etd::network::ensure(&paths, endpoint.id(), "hub").unwrap();
    let state = server::State::new(paths, env.clone());
    state.set_notify_command(command);
    let _router = server::serve(endpoint, state.clone());
    let follower = tokio::spawn(etd::events::follow(state));
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Nobody attached: the bell is worth a note.
    let new = Command::new(node.bin().join("mm"))
        .args(["new", "-d", "-n", "ringer", "sh", "-c", "sleep 1; printf '\\a'; sleep 3"])
        .envs(env.iter())
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();
    assert!(new.status.success(), "mm new: {}", String::from_utf8_lossy(&new.stderr));
    let mut seen = String::new();
    for _ in 0..100 {
        seen = std::fs::read_to_string(&notes).unwrap_or_default();
        if seen.contains("bell|ringer|") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    follower.abort();
    assert!(seen.contains("bell|ringer|"), "notes: {seen:?}");
}

/// With no node running, etd waits for one and starts none: a node started by
/// `mm agent` takes the socket from the user's service, which then restarts
/// forever.
#[tokio::test(flavor = "multi_thread")]
async fn the_follower_starts_no_node() {
    let Some(real) = real_mm() else {
        eprintln!("skipped: no `mm` on PATH");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("mm.sock");
    let bin = dir.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    script(&bin.join("mm"), &format!("#!/bin/sh\nexec '{}' --socket '{}' \"$@\"\n", real.display(), socket.display()));
    let mut env: etd::tmux::Env = std::env::vars().filter(|(k, _)| k != "TMUX" && k != "TMUX_PANE").collect();
    env.insert("PATH".into(), format!("{}:{}", bin.display(), env["PATH"]));
    env.insert("MM_CONFIG_DIR".into(), dir.path().join("config").display().to_string());
    env.insert("XDG_STATE_HOME".into(), dir.path().join("state").display().to_string());
    env.insert("ETD_MM_SOCKET".into(), socket.display().to_string());
    let files = tempfile::tempdir().unwrap();
    let state = server::State::new(Paths::at(files.path().into()), env.clone());
    let follower = tokio::spawn(etd::events::follow(state));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    follower.abort();
    let started = socket.exists();
    if started {
        let _ = Command::new(bin.join("mm")).arg("stop").envs(env.iter()).status();
    }
    assert!(!started, "the follower started a node");
}
