//! `etd send` end to end: the CLI → the local etd (socket) → iroh → the
//! other machine's etd, which fetches the files over that connection and
//! writes them to its `~/etd-downloads/`.
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use et_proto::network::Kind;
use et_proto::ALPN;
use etd::config::Paths;
use etd::{local, network, pair, server};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr};

const ETD: &str = env!("CARGO_BIN_EXE_etd");

fn local_addr(endpoint: &Endpoint) -> EndpointAddr {
    let port = endpoint.bound_sockets().iter().find(|a| a.is_ipv4()).unwrap().port();
    EndpointAddr::new(endpoint.id()).with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
}

struct Machine {
    endpoint: Endpoint,
    paths: Paths,
    dir: tempfile::TempDir,
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
        Self { endpoint, paths, dir, _router: router }
    }

    fn downloads(&self) -> std::path::PathBuf {
        self.dir.path().join("etd-downloads")
    }
}

fn etd_send(home: &Path, socket: &Path, args: &[&str]) -> std::process::Output {
    Command::new(ETD)
        .arg("send")
        .args(args)
        .env("ETD_HOME", home)
        .env("ETD_SOCKET", socket)
        .env_remove("ETD_DOWNLOADS")
        .stdin(Stdio::null())
        .output()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn etd_send_files_and_folders() {
    let book = MemoryLookup::new();
    let hub = Machine::start("trigkey", &book).await;
    let gmktec = Machine::start("gmktec", &book).await;
    let url = pair::invite_url(&hub.paths, &local_addr(&hub.endpoint), "trigkey", Kind::Machine).unwrap();
    network::join(&gmktec.endpoint, &gmktec.paths, &url, "gmktec").await.unwrap();

    let socket = local::socket_path(&hub.paths);
    let _local = local::spawn(hub.endpoint.clone(), hub.paths.clone(), Default::default());
    for _ in 0..50 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A folder with a tree, and a single file, in one send.
    let src = tempfile::tempdir().unwrap();
    let out = src.path().join("out");
    std::fs::create_dir_all(out.join("logs")).unwrap();
    let big: Vec<u8> = (0..20_000_000u32).map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8).collect();
    std::fs::write(out.join("big.bin"), &big).unwrap();
    std::fs::write(out.join("logs/run.txt"), "ok\n").unwrap();
    std::fs::write(src.path().join("notes.md"), "# look at out/\n").unwrap();
    let (out_s, notes_s) = (out.display().to_string(), src.path().join("notes.md").display().to_string());

    let sent = etd_send(hub.dir.path(), &socket, &[&out_s, &notes_s, "gmktec"]);
    assert!(sent.status.success(), "etd send: {}", String::from_utf8_lossy(&sent.stderr));
    let d = gmktec.downloads();
    let printed: Vec<String> = String::from_utf8_lossy(&sent.stdout).lines().map(str::to_string).collect();
    assert_eq!(printed, [d.join("notes.md").display().to_string(), d.join("out").display().to_string()]);
    assert_eq!(std::fs::read(d.join("out/big.bin")).unwrap(), big);
    assert_eq!(std::fs::read_to_string(d.join("out/logs/run.txt")).unwrap(), "ok\n");
    assert_eq!(std::fs::read_to_string(d.join("notes.md")).unwrap(), "# look at out/\n");

    // The same again: new names, nothing overwritten.
    let again = etd_send(hub.dir.path(), &socket, &[&notes_s, "gmktec"]);
    assert!(again.status.success(), "etd send: {}", String::from_utf8_lossy(&again.stderr));
    assert_eq!(String::from_utf8_lossy(&again.stdout).trim(), d.join("notes (2).md").display().to_string());

    // `etd files` on trigkey lists what gmktec received, newest first.
    let files = Command::new(ETD).arg("files").env("ETD_HOME", hub.dir.path()).env("ETD_SOCKET", &socket).output().unwrap();
    let text = String::from_utf8_lossy(&files.stdout);
    let rows: Vec<&str> = text.lines().skip(1).collect();
    assert_eq!(rows.len(), 3, "{text}");
    assert!(rows[0].contains("gmktec") && rows[0].contains("trigkey") && rows[0].ends_with("notes (2).md"), "{text}");
    assert!(text.contains(&format!("{}/", d.join("out").display())), "{text}");

    // A name that is not a machine of the network.
    let wrong = etd_send(hub.dir.path(), &socket, &[&notes_s, "laptop"]);
    assert!(!wrong.status.success());
    assert!(String::from_utf8_lossy(&wrong.stderr).contains("not a machine of the network"), "{}", String::from_utf8_lossy(&wrong.stderr));
}

/// A manymux node on its own socket (never the user's), with `bin/mm`
/// always passing that socket.
struct Node {
    dir: tempfile::TempDir,
    child: std::process::Child,
}

impl Node {
    fn start(real: &Path) -> Self {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("mm.sock");
        std::fs::create_dir(dir.path().join("bin")).unwrap();
        let wrapper = dir.path().join("bin/mm");
        std::fs::write(&wrapper, format!("#!/bin/sh\nexec '{}' --socket '{}' \"$@\"\n", real.display(), socket.display())).unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let child = Command::new(&wrapper)
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

    fn mm(&self) -> Command {
        let mut cmd = Command::new(self.dir.path().join("bin/mm"));
        cmd.envs(Self::env_of(self.dir.path())).env_remove("TMUX").stdin(Stdio::null());
        cmd
    }

    /// The session's size, from manymux's own protocol.
    async fn size(&self, name: &str) -> (u16, u16) {
        use manymux::proto::{self as mp, tag, FrameReader};
        let mut agent = tokio::process::Command::new(self.dir.path().join("bin/mm"))
            .arg("agent")
            .envs(Self::env_of(self.dir.path()))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdin = agent.stdin.take().unwrap();
        let mut reader = FrameReader::new(agent.stdout.take().unwrap());
        mp::write_msg(&mut stdin, tag::REQUEST, &mp::Request::List).await.unwrap();
        loop {
            let frame = reader.next().await.unwrap().unwrap();
            if frame.tag == tag::RESPONSE {
                let mp::Response::Sessions(list) = mp::decode(&frame.body).unwrap() else { panic!("not a list") };
                let s = list.into_iter().find(|s| s.name == name).unwrap();
                return (s.size.cols, s.size.rows);
            }
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn etd_send_types_the_paths_into_a_session() {
    let Some(real) = std::env::var_os("PATH").and_then(|p| std::env::split_paths(&p).map(|d| d.join("mm")).find(|p| p.is_file())) else {
        eprintln!("skipped: no `mm` on PATH");
        return;
    };
    let node = Node::start(&real);
    let book = MemoryLookup::new();
    let hub = Machine::start("trigkey", &book).await;
    // gmktec's etd uses the test node.
    let gmktec = {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::at(dir.path().into());
        let endpoint = Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]).address_lookup(book.clone()).bind().await.unwrap();
        book.add_endpoint_info(local_addr(&endpoint));
        network::ensure(&paths, endpoint.id(), "gmktec").unwrap();
        let mut env: etd::tmux::Env = Node::env_of(node.dir.path()).into_iter().collect();
        env.insert("HOME".into(), dir.path().display().to_string());
        let router = server::serve(endpoint.clone(), server::State::new(paths.clone(), env));
        Machine { endpoint, paths, dir, _router: router }
    };
    let url = pair::invite_url(&hub.paths, &local_addr(&hub.endpoint), "trigkey", Kind::Machine).unwrap();
    network::join(&gmktec.endpoint, &gmktec.paths, &url, "gmktec").await.unwrap();
    let socket = local::socket_path(&hub.paths);
    let _local = local::spawn(hub.endpoint.clone(), hub.paths.clone(), Default::default());
    for _ in 0..50 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A session that writes down every byte typed into it, as typed.
    let src = tempfile::tempdir().unwrap();
    let file = src.path().join("shot one.png");
    std::fs::write(&file, b"png").unwrap();
    let expected = format!("'{}' ", gmktec.downloads().join("shot one.png").display());
    let typed = node.dir.path().join("typed.txt");
    let script = format!("stty -icanon -echo; head -c {} > '{}'; sleep 30", expected.len(), typed.display());
    let new = node.mm().args(["new", "-d", "-n", "claude", "sh", "-c", &script]).output().unwrap();
    assert!(new.status.success(), "mm new: {}", String::from_utf8_lossy(&new.stderr));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = node.size("claude").await;

    let sent = etd_send(hub.dir.path(), &socket, &[&file.display().to_string(), "gmktec:claude"]);
    assert!(sent.status.success(), "etd send: {}", String::from_utf8_lossy(&sent.stderr));
    assert!(
        sent.stderr.is_empty() || !String::from_utf8_lossy(&sent.stderr).contains("not typed"),
        "{}",
        String::from_utf8_lossy(&sent.stderr)
    );
    for _ in 0..50 {
        if std::fs::read(&typed).is_ok_and(|t| t.len() == expected.len()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(std::fs::read_to_string(&typed).unwrap_or_default(), expected);
    assert_eq!(node.size("claude").await, before, "typing resized the session");

    // A session that is not there: delivered, not typed.
    let missing = etd_send(hub.dir.path(), &socket, &[&file.display().to_string(), "gmktec:nope"]);
    assert!(missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("not typed"), "{}", String::from_utf8_lossy(&missing.stderr));
}
