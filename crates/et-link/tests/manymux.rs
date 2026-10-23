//! et-link's manymux API against an in-process etd and a throwaway node.
//!
//! Isolation: every `mm` is a wrapper that always passes `--socket <temp>`,
//! with private `MM_CONFIG_DIR` and `XDG_STATE_HOME`, and `MM_SSH=false`.
//! "fakehost" is a second etd in the network, on the same node. The real node
//! is never reached. No paste:
//! manymux writes pasted files under `/tmp/manymux-<uid>` whatever the socket.
use std::net::{Ipv4Addr, SocketAddr};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;

use et_link::{Link, MmAttachment, MmEventKind, MmOutput, MmSpawn, RemoteHost};
use et_proto::ALPN;
use etd::config::Paths;
use etd::server;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, SecretKey};

fn real_mm() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path).map(|d| d.join("mm")).find(|p| p.is_file())
}

fn script(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// A manymux node on its own socket, stopped on drop (its sessions with it).
struct Node {
    dir: tempfile::TempDir,
    child: Child,
}

impl Node {
    fn start(real: &Path) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("mm.sock");
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        script(&bin.join("mm"), &format!("#!/bin/sh\nexec '{}' --socket '{}' \"$@\"\n", real.display(), socket.display()));
        let child = Command::new(bin.join("mm"))
            .args(["daemon", "--no-notify"])
            .envs(Self::env_of(dir.path()))
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

    fn env_of(dir: &Path) -> Vec<(String, String)> {
        vec![
            ("PATH".into(), format!("{}:{}", dir.join("bin").display(), std::env::var("PATH").unwrap())),
            ("MM_CONFIG_DIR".into(), dir.join("config").display().to_string()),
            ("XDG_STATE_HOME".into(), dir.join("state").display().to_string()),
        ]
    }

    fn env(&self) -> Vec<(String, String)> {
        Self::env_of(self.dir.path())
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Reads output until `text` appears; returns everything read.
async fn until(attachment: &MmAttachment, text: &str) -> String {
    let mut seen = String::new();
    while !seen.contains(text) {
        match tokio::time::timeout(Duration::from_secs(10), attachment.next()).await.expect("output in time").unwrap() {
            MmOutput::Data { bytes } | MmOutput::History { bytes } | MmOutput::Resync { bytes } => {
                seen.push_str(&String::from_utf8_lossy(&bytes))
            }
            MmOutput::Exit { code } => panic!("exited ({code}) before {text:?}: {seen:?}"),
            _ => {}
        }
    }
    seen
}

fn spawn(command: &str, name: &str) -> MmSpawn {
    MmSpawn { name: Some(name.into()), command: vec!["sh".into(), "-c".into(), command.into()], cwd: None, cols: 80, rows: 24 }
}

async fn names(host: &RemoteHost, machine: Option<String>) -> Vec<String> {
    host.mm_list(machine).await.unwrap().into_iter().map(|s| s.name).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn manymux_through_et_link() {
    let Some(real) = real_mm() else {
        eprintln!("skipped: no `mm` on PATH");
        return;
    };
    let node = Node::start(&real);
    // The node watches "fakehost" (for `machines`). By hand: `mm add` would
    // check it over ssh first.
    std::fs::create_dir_all(node.dir.path().join("config")).unwrap();
    std::fs::write(node.dir.path().join("config/hosts.toml"), "hosts = [\"fakehost\"]\n").unwrap();

    let home = tempfile::tempdir().unwrap();
    let mut env: etd::tmux::Env = std::env::vars().filter(|(k, _)| k != "TMUX" && k != "TMUX_PANE").collect();
    env.extend(node.env());
    env.insert("HOME".into(), home.path().display().to_string());

    // Two etds on the same node: "hub", and "fakehost" in its network.
    let book = MemoryLookup::new();
    let start = |name: &'static str, env: etd::tmux::Env| {
        let book = book.clone();
        async move {
            let files = tempfile::tempdir().unwrap();
            let paths = Paths::at(files.path().into());
            let endpoint =
                Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).address_lookup(book.clone()).bind().await.unwrap();
            let port = endpoint.bound_sockets().iter().find(|a| a.is_ipv4()).unwrap().port();
            let addr = EndpointAddr::new(endpoint.id()).with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)));
            book.add_endpoint_info(addr.clone());
            etd::network::ensure(&paths, endpoint.id(), name).unwrap();
            let router = server::serve(endpoint.clone(), server::State::new(paths.clone(), env));
            (endpoint, paths, addr, files, router)
        }
    };
    let (endpoint, paths, addr, _files, _router) = start("hub", env.clone()).await;
    let (fake, fake_paths, _, _fake_files, _fake_router) = start("fakehost", env).await;
    let invite = etd::pair::invite_url(&paths, &addr, "hub", et_proto::network::Kind::Machine).unwrap();
    etd::network::join(&fake, &fake_paths, &invite, "fakehost").await.unwrap();
    let phone = SecretKey::generate();
    etd::network::admit(&paths, endpoint.id(), phone.public(), "phone", et_proto::network::Kind::Device).unwrap();
    let url = etd::pair::create(&paths, addr, "hub".into(), et_proto::network::Kind::Device).unwrap().to_url().unwrap();
    let link = Link::from_endpoint(Endpoint::builder(presets::Minimal).secret_key(phone).bind().await.unwrap());
    let paired = link.pair(url, "phone".into()).await.unwrap();
    let host: Arc<RemoteHost> = link.connect(paired.endpoint_id, None).await.unwrap();

    assert_eq!(host.machines().await.unwrap(), ["fakehost"]);
    let r = host.exec_on("fakehost".into(), "echo on-$((40+2))".into()).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&r.output), "on-42\n");

    // The event feed sees a start and a bell.
    let events = host.mm_events().await.unwrap();

    // Start, attach, type, see the answer, and the exit code.
    let name = host.mm_spawn(None, spawn("echo ready; read x; echo got:$x; exit 7", "t1")).await.unwrap();
    assert_eq!(name, "t1");
    assert!(names(&host, None).await.contains(&"t1".to_string()));
    let t1 = host.mm_attach(None, "t1".into(), 80, 24, 0, false).await.unwrap();
    assert!(!t1.read_only());
    until(&t1, "ready").await;
    t1.write(b"hi\r".to_vec()).await.unwrap();
    until(&t1, "got:hi").await;
    loop {
        match tokio::time::timeout(Duration::from_secs(10), t1.next()).await.unwrap().unwrap() {
            MmOutput::Exit { code } => {
                assert_eq!(code, 7);
                break;
            }
            _ => continue,
        }
    }

    // The same node through the network, as "fakehost".
    host.mm_spawn(Some("fakehost".into()), spawn("printf '\\a'; echo peekme; sleep 30", "t2")).await.unwrap();
    assert!(names(&host, Some("fakehost".into())).await.contains(&"t2".to_string()));
    tokio::time::sleep(Duration::from_millis(500)).await;
    let peeks = host.mm_peek(None, vec!["t2".into()]).await.unwrap();
    assert_eq!(peeks.len(), 1);
    assert!(peeks[0].screen.contains("peekme"), "{:?}", peeks[0].screen);

    let mut kinds = Vec::new();
    while !(kinds.iter().any(|k| matches!(k, MmEventKind::Started)) && kinds.iter().any(|k| matches!(k, MmEventKind::Bell))) {
        let event = tokio::time::timeout(Duration::from_secs(10), events.next()).await.expect("events in time").unwrap();
        if event.session == "t2" {
            kinds.push(event.kind);
        }
    }
    events.close().await;

    // Watch without typing; the node honours it.
    let viewer = host.mm_attach(None, "t2".into(), 40, 10, 0, true).await.unwrap();
    assert!(viewer.read_only());
    until(&viewer, "peekme").await;
    viewer.detach().await;

    // Rename and end.
    host.mm_rename(None, "t2".into(), "renamed".into()).await.unwrap();
    assert!(names(&host, None).await.contains(&"renamed".to_string()));
    host.mm_kill(None, "renamed".into()).await.unwrap();
    for _ in 0..50 {
        if !names(&host, None).await.contains(&"renamed".to_string()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!names(&host, None).await.contains(&"renamed".to_string()));

    // Errors from the node arrive as Remote.
    assert!(matches!(host.mm_attach(None, "nope".into(), 80, 24, 0, false).await, Err(et_link::LinkError::Remote { .. })));
}
