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

    // A name that is not a machine of the network.
    let wrong = etd_send(hub.dir.path(), &socket, &[&notes_s, "laptop"]);
    assert!(!wrong.status.success());
    assert!(String::from_utf8_lossy(&wrong.stderr).contains("not a machine of the network"), "{}", String::from_utf8_lossy(&wrong.stderr));
}
