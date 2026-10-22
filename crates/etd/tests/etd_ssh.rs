//! `etd-ssh` end to end: manymux's own CLI (`MM_SSH=etd-ssh`) lists a
//! session on another machine of the network, with no ssh anywhere:
//! mm → etd-ssh → etd (local socket) → iroh → the other etd → mm agent.
//!
//! Isolation: every manymux node here has its own socket (`--socket`),
//! config and log directories; `ETD_SOCKET` is set only for child processes.
use std::net::{Ipv4Addr, SocketAddr};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use et_proto::network::Kind;
use et_proto::ALPN;
use etd::config::Paths;
use etd::{local, network, pair, server};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr};

const ETD_SSH: &str = env!("CARGO_BIN_EXE_etd-ssh");

fn real_mm() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path).map(|d| d.join("mm")).find(|p| p.is_file())
}

fn script(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// A manymux node on its own socket, with `bin/mm` always passing it.
struct Node {
    dir: tempfile::TempDir,
    child: Child,
}

impl Node {
    fn start(real: &Path, extra: &[(&str, String)]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("mm.sock");
        std::fs::create_dir(dir.path().join("bin")).unwrap();
        script(&dir.path().join("bin/mm"), &format!("#!/bin/sh\nexec '{}' --socket '{}' \"$@\"\n", real.display(), socket.display()));
        let child = Command::new(dir.path().join("bin/mm"))
            .args(["daemon", "--no-notify"])
            .envs(Self::env_of(dir.path()))
            .envs(extra.iter().map(|(k, v)| (*k, v.clone())))
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

    fn env(&self) -> Vec<(String, String)> {
        Self::env_of(self.dir.path())
    }

    fn mm(&self) -> Command {
        let mut cmd = Command::new(self.dir.path().join("bin/mm"));
        cmd.envs(self.env()).env_remove("TMUX").stdin(Stdio::null());
        cmd
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
    _dir: tempfile::TempDir,
    _router: iroh::protocol::Router,
}

impl Machine {
    async fn start(name: &str, book: &MemoryLookup, env: Vec<(String, String)>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::at(dir.path().into());
        let endpoint =
            Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).address_lookup(book.clone()).bind().await.unwrap();
        book.add_endpoint_info(local_addr(&endpoint));
        network::ensure(&paths, endpoint.id(), name).unwrap();
        let mut full: etd::tmux::Env = std::env::vars().filter(|(k, _)| k != "TMUX" && k != "TMUX_PANE").collect();
        full.extend(env);
        full.insert("HOME".into(), dir.path().display().to_string());
        let router = server::serve(endpoint.clone(), server::State::new(paths.clone(), full));
        Self { endpoint, paths, _dir: dir, _router: router }
    }
}

fn etd_ssh(socket: &Path, args: &[&str]) -> std::process::Output {
    Command::new(ETD_SSH).args(args).env("ETD_SOCKET", socket).stdin(Stdio::null()).output().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn manymux_over_etd_ssh() {
    let Some(real) = real_mm() else {
        eprintln!("skipped: no `mm` on PATH");
        return;
    };
    // gmktec's node, with one session.
    let remote = Node::start(&real, &[]);
    let new = remote.mm().args(["new", "-d", "-n", "t1", "sh", "-c", "sleep 60"]).output().unwrap();
    assert!(new.status.success(), "mm new: {}", String::from_utf8_lossy(&new.stderr));

    let book = MemoryLookup::new();
    let hub = Machine::start("trigkey", &book, vec![]).await;
    let gmktec = Machine::start("gmktec", &book, remote.env()).await;
    let url = pair::invite_url(&hub.paths, &local_addr(&hub.endpoint), "trigkey", Kind::Machine).unwrap();
    network::join(&gmktec.endpoint, &gmktec.paths, &url, "gmktec").await.unwrap();

    let socket = local::socket_path(&hub.paths);
    let _local = local::spawn(hub.endpoint.clone(), hub.paths.clone());
    for _ in 0..50 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Commands: output and exit code come back, as with ssh.
    let out = etd_ssh(&socket, &["-T", "-o", "BatchMode=yes", "gmktec", "--", "echo", "$((40+2))"]);
    assert_eq!(String::from_utf8_lossy(&out.stdout), "42\n", "stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(etd_ssh(&socket, &["gmktec", "--", "exit 3"]).status.code(), Some(3));
    let unknown = etd_ssh(&socket, &["laptop", "--", "true"]);
    assert_eq!(unknown.status.code(), Some(255));
    assert!(String::from_utf8_lossy(&unknown.stderr).contains("not a machine"), "{}", String::from_utf8_lossy(&unknown.stderr));

    // manymux's CLI, on a node of its own, with MM_SSH=etd-ssh.
    let client = Node::start(&real, &[("MM_SSH", ETD_SSH.into()), ("ETD_SOCKET", socket.display().to_string())]);
    std::fs::create_dir_all(client.dir.path().join("config")).unwrap();
    std::fs::write(client.dir.path().join("config/hosts.toml"), "hosts = [\"gmktec\"]\n").unwrap();
    let ls = client
        .mm()
        .args(["ls", "gmktec"])
        .env("MM_SSH", ETD_SSH)
        .env("ETD_SOCKET", &socket)
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&ls.stdout);
    assert!(text.contains("gmktec/t1"), "mm ls: {text}\nstderr: {}", String::from_utf8_lossy(&ls.stderr));
}
