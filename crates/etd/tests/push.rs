//! Pushes across the network (N4): a bell on one machine runs the push
//! command on another, and "the app is open" holds pushes back.
//!
//! Isolation: the manymux node has its own socket, config and log
//! directories; endpoints are offline and find each other in memory.
use std::net::{Ipv4Addr, SocketAddr};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use et_proto::network::Kind;
use et_proto::{read_frame, write_frame, Request, Response, ALPN};
use etd::config::{now_secs, Paths};
use etd::{events, network, pair, server};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, SecretKey};

fn real_mm() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path).map(|d| d.join("mm")).find(|p| p.is_file())
}

/// A manymux node on its own socket, with `bin/mm` always passing it.
struct Node {
    dir: tempfile::TempDir,
    child: Child,
}

impl Node {
    fn start(real: &Path) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("mm.sock");
        std::fs::create_dir(dir.path().join("bin")).unwrap();
        let mm = dir.path().join("bin/mm");
        std::fs::write(&mm, format!("#!/bin/sh\nexec '{}' --socket '{}' \"$@\"\n", real.display(), socket.display())).unwrap();
        std::fs::set_permissions(&mm, std::fs::Permissions::from_mode(0o755)).unwrap();
        let child = Command::new(&mm)
            .args(["daemon", "--no-notify"])
            .envs(Self::env_of(dir.path()))
            .env_remove("TMUX")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
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

    fn ring(&self, name: &str) {
        let out = Command::new(self.dir.path().join("bin/mm"))
            .args(["new", "-d", "-n", name, "sh", "-c", "sleep 1; printf '\\a'; sleep 3"])
            .envs(Self::env_of(self.dir.path()))
            .env_remove("TMUX")
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(out.status.success(), "mm new: {}", String::from_utf8_lossy(&out.stderr));
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn local_addr(endpoint: &Endpoint) -> EndpointAddr {
    let port = endpoint.bound_sockets().iter().find(|a| a.is_ipv4()).unwrap().port();
    EndpointAddr::new(endpoint.id()).with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
}

struct Machine {
    endpoint: Endpoint,
    paths: Paths,
    state: Arc<server::State>,
    _dir: tempfile::TempDir,
    _router: iroh::protocol::Router,
}

impl Machine {
    async fn start(name: &str, book: &MemoryLookup, env: Vec<(String, String)>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::at(dir.path().into());
        let endpoint = Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).address_lookup(book.clone()).bind().await.unwrap();
        book.add_endpoint_info(local_addr(&endpoint));
        network::ensure(&paths, endpoint.id(), name).unwrap();
        let mut full: etd::tmux::Env = std::env::vars().filter(|(k, _)| k != "TMUX" && k != "TMUX_PANE").collect();
        full.extend(env);
        full.insert("HOME".into(), dir.path().display().to_string());
        let state = server::State::new(paths.clone(), full);
        let router = server::serve(endpoint.clone(), state.clone());
        Self { endpoint, paths, state, _dir: dir, _router: router }
    }
}

async fn wait_for(path: &Path, text: &str) -> String {
    let mut seen = String::new();
    for _ in 0..100 {
        seen = std::fs::read_to_string(path).unwrap_or_default();
        if seen.contains(text) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    seen
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bell_on_one_machine_is_pushed_by_another() {
    let Some(real) = real_mm() else {
        eprintln!("skipped: no `mm` on PATH");
        return;
    };
    let node = Node::start(&real);
    let book = MemoryLookup::new();
    // trigkey sends pushes (it has the command); gmktec has the sessions.
    let hub = Machine::start("trigkey", &book, vec![]).await;
    let notes = hub._dir.path().join("notes");
    hub.state.set_notify_command(format!("echo \"$ETD_KIND|$ETD_MACHINE|$ETD_SESSION\" >> '{}'", notes.display()));
    let gmktec = Machine::start("gmktec", &book, Node::env_of(node.dir.path())).await;
    let url = pair::invite_url(&hub.paths, &local_addr(&hub.endpoint), "trigkey", Kind::Machine).unwrap();
    network::join(&gmktec.endpoint, &gmktec.paths, &url, "gmktec").await.unwrap();
    let follower = tokio::spawn(events::follow(gmktec.state.clone()));
    tokio::time::sleep(Duration::from_millis(800)).await;

    // gmktec's bell reaches trigkey's push command, under the network name.
    node.ring("first");
    let seen = wait_for(&notes, "bell|gmktec|first").await;
    assert!(seen.contains("bell|gmktec|first"), "notes: {seen:?}");

    // A phone says the app is open: no push for the next bell.
    let phone = Endpoint::builder(presets::Minimal).secret_key(SecretKey::generate()).address_lookup(book.clone()).bind().await.unwrap();
    network::admit(&hub.paths, hub.endpoint.id(), phone.id(), "iPhone", Kind::Device).unwrap();
    let conn = phone.connect(hub.endpoint.id(), ALPN).await.unwrap();
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    write_frame(&mut send, &Request::Foreground { until: now_secs() + 60 }).await.unwrap();
    send.finish().unwrap();
    assert_eq!(read_frame::<Response>(&mut recv).await.unwrap(), Some(Response::Pong));
    node.ring("second");
    tokio::time::sleep(Duration::from_secs(4)).await;
    let after = std::fs::read_to_string(&notes).unwrap_or_default();
    assert!(!after.contains("second"), "pushed while the app was open: {after:?}");

    follower.abort();
}
