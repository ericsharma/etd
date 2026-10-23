//! The daemon end to end: offline iroh endpoints on localhost, a throwaway
//! tmux server on a private socket, files in temporary directories.
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use et_proto::{
    read_frame, write_frame, ControlFrame, Request, Response, Target, TmuxStatus, ALPN,
};
use etd::config::{self, Paths};
use etd::{pair, server, tmux};
use iroh::endpoint::{presets, Connection};
use iroh::{Endpoint, EndpointAddr, SecretKey};

/// A tmux server under its own TMUX_TMPDIR, killed on drop.
///
/// Every tmux command here names the socket with `-S` and removes `$TMUX`.
/// Tests often run inside a tmux pane: without this, `$TMUX` sends the
/// command to the user's real server (`kill-server` once ended it).
struct Tmux {
    tmpdir: tempfile::TempDir,
}

impl Tmux {
    fn socket(tmpdir: &Path) -> std::path::PathBuf {
        // SAFETY: getuid has no preconditions and cannot fail.
        tmpdir
            .join(format!("tmux-{}", unsafe { libc::getuid() }))
            .join("default")
    }

    fn tmux(&self) -> Command {
        let mut cmd = Command::new("tmux");
        cmd.arg("-S")
            .arg(Self::socket(self.tmpdir.path()))
            .env_remove("TMUX")
            .env_remove("TMUX_PANE");
        cmd
    }

    fn start(home: &Path) -> Self {
        let tmpdir = tempfile::tempdir().unwrap();
        let socket_dir = Self::socket(tmpdir.path()).parent().unwrap().to_path_buf();
        std::fs::create_dir(&socket_dir).unwrap();
        std::fs::set_permissions(
            &socket_dir,
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let server = Self { tmpdir };
        let ok = server
            .tmux()
            .args([
                "-f",
                "/dev/null",
                "new-session",
                "-d",
                "-s",
                "work",
                "-x",
                "80",
                "-y",
                "24",
            ])
            .env("HOME", home)
            .status()
            .expect("tmux must be on PATH (nix develop)");
        assert!(ok.success());
        server
    }
}

impl Drop for Tmux {
    fn drop(&mut self) {
        let _ = self.tmux().arg("kill-server").status();
    }
}

struct Daemon {
    endpoint: Endpoint,
    paths: Paths,
}

impl Daemon {
    fn addr(&self) -> EndpointAddr {
        let port = self
            .endpoint
            .bound_sockets()
            .iter()
            .find(|a| a.is_ipv4())
            .unwrap()
            .port();
        EndpointAddr::new(self.endpoint.id())
            .with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
    }

    fn ticket_secret(&self) -> [u8; 32] {
        pair::create(
            &self.paths,
            self.addr(),
            "test".into(),
            et_proto::network::Kind::Device,
        )
        .unwrap()
        .secret
    }
}

async fn connect(daemon: &Daemon, key: &SecretKey) -> (Endpoint, Connection) {
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(key.clone())
        .bind()
        .await
        .unwrap();
    let conn = endpoint.connect(daemon.addr(), ALPN).await.unwrap();
    (endpoint, conn)
}

async fn call(conn: &Connection, request: Request) -> anyhow::Result<Response> {
    let (mut send, mut recv) = conn.open_bi().await?;
    write_frame(&mut send, &request).await?;
    send.finish()?;
    read_frame(&mut recv)
        .await?
        .ok_or_else(|| anyhow::anyhow!("no response"))
}

async fn pair_with(
    daemon: &Daemon,
    key: &SecretKey,
    secret: [u8; 32],
    name: &str,
) -> (Endpoint, Connection, Response) {
    let (endpoint, conn) = connect(daemon, key).await;
    let reply = call(
        &conn,
        Request::Pair {
            secret,
            device_name: name.into(),
        },
    )
    .await
    .unwrap();
    (endpoint, conn, reply)
}

#[tokio::test(flavor = "multi_thread")]
async fn daemon_end_to_end() {
    let home = tempfile::tempdir().unwrap();
    let files = tempfile::tempdir().unwrap();
    let tmux_server = Tmux::start(home.path());
    let mut env: tmux::Env = std::env::vars()
        .filter(|(k, _)| k != "TMUX" && k != "TMUX_PANE")
        .collect();
    env.insert(
        "TMUX_TMPDIR".into(),
        tmux_server.tmpdir.path().display().to_string(),
    );
    env.insert("HOME".into(), home.path().display().to_string());

    let paths = Paths::at(files.path().into());
    let endpoint = Endpoint::builder(presets::Minimal)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    etd::network::ensure(&paths, endpoint.id(), "hub").unwrap();
    let state = server::State::new(paths.clone(), env);
    let _router = server::serve(endpoint.clone(), state.clone());
    let watcher = tokio::spawn(server::watch(endpoint.clone(), state, "test".into()));
    let daemon = Daemon { endpoint, paths };

    // A device that is not paired gets nothing but a close.
    let stranger = SecretKey::generate();
    let (_e, conn) = connect(&daemon, &stranger).await;
    assert!(call(&conn, Request::Hello { app: "t".into() })
        .await
        .is_err());

    // A wrong secret is refused.
    let (_e, _c, reply) = pair_with(&daemon, &stranger, [1; 32], "stranger").await;
    assert!(matches!(reply, Response::Rejected(_)), "{reply:?}");

    // Pairing with a real secret works once.
    let phone = SecretKey::generate();
    let secret = daemon.ticket_secret();
    let (_phone_ep, conn, reply) = pair_with(&daemon, &phone, secret, "phone").await;
    assert!(matches!(reply, Response::Paired { .. }), "{reply:?}");
    let network = etd::network::load(&daemon.paths).unwrap().unwrap().network;
    let devices: Vec<String> = network
        .members()
        .filter(|m| m.kind == et_proto::network::Kind::Device)
        .map(|m| m.name.to_string())
        .collect();
    assert_eq!(devices, ["phone"]);
    let other = SecretKey::generate();
    let (_e, _c, reply) = pair_with(&daemon, &other, secret, "other").await;
    assert!(
        matches!(reply, Response::Rejected(_)),
        "a used secret must fail: {reply:?}"
    );

    // The pairing connection carries on as a paired one.
    match call(&conn, Request::Hello { app: "t".into() })
        .await
        .unwrap()
    {
        Response::HostInfo(info) => match info.tmux {
            TmuxStatus::Found(t) => assert_eq!(
                t.tmpdir.as_deref(),
                Some(tmux_server.tmpdir.path().to_str().unwrap())
            ),
            other => panic!("tmux not found: {other:?}"),
        },
        other => panic!("{other:?}"),
    }
    match call(&conn, Request::ListSessions).await.unwrap() {
        Response::Sessions(s) => assert_eq!(
            s.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
            ["work"]
        ),
        other => panic!("{other:?}"),
    }

    // Control: a command and its reply, then a clean detach.
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    write_frame(
        &mut send,
        &Request::Control {
            target: Target::Attach("work".into()),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        read_frame::<Response>(&mut recv).await.unwrap(),
        Some(Response::ControlStarted)
    );
    send.write_all(b"display-message -p etd-says-hi\n")
        .await
        .unwrap();
    let mut seen = Vec::new();
    while !String::from_utf8_lossy(&seen).contains("etd-says-hi") {
        match read_frame::<ControlFrame>(&mut recv)
            .await
            .unwrap()
            .unwrap()
        {
            ControlFrame::Stdout(bytes) => seen.extend(bytes),
            other => panic!("{other:?}"),
        }
    }
    send.finish().unwrap();
    loop {
        match read_frame::<ControlFrame>(&mut recv)
            .await
            .unwrap()
            .unwrap()
        {
            ControlFrame::Exit(code) => {
                assert_eq!(code, Some(0));
                break;
            }
            _ => continue,
        }
    }

    // Exec: stdout and stderr together, exit code, home directory.
    match call(
        &conn,
        Request::Exec {
            script: "echo out; echo err >&2; pwd; exit 3".into(),
        },
    )
    .await
    .unwrap()
    {
        Response::ExecResult {
            output,
            exit_code,
            truncated,
        } => {
            let text = String::from_utf8(output).unwrap();
            assert_eq!(text, format!("out\nerr\n{}\n", home.path().display()));
            assert_eq!(exit_code, Some(3));
            assert!(!truncated);
        }
        other => panic!("{other:?}"),
    }

    // Upload: inside home only.
    let upload = |path: &'static str, data: &'static [u8]| {
        let conn = conn.clone();
        async move {
            let (mut send, mut recv) = conn.open_bi().await.unwrap();
            write_frame(
                &mut send,
                &Request::Upload {
                    path: path.into(),
                    len: data.len() as u64,
                },
            )
            .await
            .unwrap();
            send.write_all(data).await.unwrap();
            send.finish().unwrap();
            read_frame::<Response>(&mut recv).await.unwrap().unwrap()
        }
    };
    let reply = upload("~/.cache/et-uploads/a.png", b"png bytes").await;
    assert!(matches!(reply, Response::Uploaded { .. }), "{reply:?}");
    assert_eq!(
        std::fs::read(home.path().join(".cache/et-uploads/a.png")).unwrap(),
        b"png bytes"
    );
    assert!(matches!(
        upload("/etc/etd-test", b"x").await,
        Response::Error(_)
    ));
    assert!(matches!(
        upload("~/../escape", b"x").await,
        Response::Error(_)
    ));
    std::os::unix::fs::symlink("/tmp", home.path().join("out")).unwrap();
    assert!(matches!(
        upload("~/out/escape", b"x").await,
        Response::Error(_)
    ));

    // Status file lists the connected device.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let status: config::Status = config::read_json(&daemon.paths.status()).unwrap().unwrap();
    assert_eq!(status.connected, ["phone"]);

    // Revoke closes the open connection.
    assert_eq!(
        etd::network::revoke(&daemon.paths, daemon.endpoint.id(), "phone")
            .unwrap()
            .len(),
        1
    );
    let reason = tokio::time::timeout(Duration::from_secs(5), conn.closed())
        .await
        .expect("closed after revoke");
    assert!(format!("{reason}").contains("revoked"), "{reason}");

    // Too many wrong tries: even a good secret is refused for a while.
    let guesser = SecretKey::generate();
    for _ in 0..5 {
        let (_e, _c, reply) = pair_with(&daemon, &guesser, [2; 32], "guesser").await;
        assert!(matches!(reply, Response::Rejected(_)));
    }
    let (_e, _c, reply) = pair_with(&daemon, &guesser, daemon.ticket_secret(), "guesser").await;
    assert!(
        matches!(&reply, Response::Rejected(why) if why.contains("too many")),
        "{reply:?}"
    );

    watcher.abort();
}
