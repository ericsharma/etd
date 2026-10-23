use std::io::IsTerminal;

use anyhow::{bail, Context};
use et_proto::network::Kind;
use etd::config::{self, Paths};
use etd::{network, pair, server, service, tmux};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointId};
use tracing_subscriber::EnvFilter;

const USAGE: &str = "\
usage:
  etd                          run the daemon (shows a QR code if no device is in the network yet)
  etd install [--no-service]   run etd as a user service (systemd or launchd) and start it, with
                               the manymux node; --no-service: only manymux (E.T. for macOS)
  etd uninstall                stop and remove the user service
  etd invite [--machine] [--url]
                               invite a phone or tablet (QR code), or with --machine another
                               machine (`etd join …` to run there); valid 10 minutes, one use
  etd join <link> [--name N]   join the network of the machine that made the link
  etd members                  list the network's machines and devices
  etd revoke <name|id>         remove a member everywhere and close its connections
  etd send <path>… <machine>[:<session>]
                               send files or folders to ~/etd-downloads/ on another machine
                               of the network; prints the paths there
  etd files                    list the files sent to every machine of the network
  etd status [--json]          show the daemon's id, addresses, and connected members
  etd id                       print this machine's endpoint id
  etd pair [--url]             the same as `etd invite` (a device)
  etd allow <id> [name]        add a device without a QR code (for tests)

files: $ETD_HOME, else ~/.config/etd and ~/.local/state/etd
       (macOS: ~/Library/Application Support/etd)";

/// `$ETD_LOG_FILE`, opened to append. A leading `~/` means the home
/// directory (a login item's plist cannot know it).
fn log_file() -> anyhow::Result<Option<std::fs::File>> {
    let Some(path) = std::env::var("ETD_LOG_FILE").ok().filter(|p| !p.is_empty()) else {
        return Ok(None);
    };
    let path = match path.strip_prefix("~/") {
        Some(rest) => dirs::home_dir().context("no home directory")?.join(rest),
        None => path.into(),
    };
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let file = std::fs::OpenOptions::new().create(true).append(true).open(&path).with_context(|| format!("open {}", path.display()))?;
    Ok(Some(file))
}

/// `etd status --json`, for E.T. for macOS: whether etd runs, who manages
/// it, and this machine's network.
fn status_json(paths: &Paths) -> anyhow::Result<String> {
    let mut out = serde_json::json!({ "running": false });
    if let Some(s) = config::running_status(paths)? {
        let manager = s.exe.as_deref().map(|e| service::manager(std::path::Path::new(e)));
        out = serde_json::json!({
            "running": true,
            "pid": s.pid,
            "version": s.version,
            "id": s.addr.id.to_string(),
            "host": s.host_name,
            "exe": s.exe,
            "manager": manager.unwrap_or("unknown"),
            "connected": s.connected,
        });
    }
    if let Some(signed) = network::load(paths)? {
        let members: Vec<_> = signed
            .network
            .members()
            .map(|m| serde_json::json!({ "name": m.name.to_string(), "kind": kind_word(m.kind), "id": m.id.to_string() }))
            .collect();
        out["network"] = serde_json::json!({ "name": signed.network.name.to_string(), "members": members });
    }
    Ok(serde_json::to_string_pretty(&out)?)
}

fn kind_word(kind: Kind) -> &'static str {
    match kind {
        Kind::Machine => "machine",
        Kind::Device => "device",
    }
}

/// `etd send <path>… <machine>[:<session>]`, through the running etd.
async fn send(paths: &Paths, args: &[&str]) -> anyhow::Result<()> {
    let (target, files) = args.split_last().context(USAGE)?;
    let (to, session) = match target.split_once(':') {
        Some((machine, session)) => (machine.to_string(), Some(session.to_string())),
        None => (target.to_string(), None),
    };
    let files = files
        .iter()
        .map(|f| std::path::Path::new(f).canonicalize().map(|p| p.display().to_string()).with_context(|| f.to_string()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let socket = etd::local::socket_path(paths);
    let mut stream =
        tokio::net::UnixStream::connect(&socket).await.with_context(|| format!("etd is not running here ({})", socket.display()))?;
    etd::local::send(&mut stream, &et_proto::Request::Send { paths: files, to: to.clone(), session: session.clone() }).await?;
    let show = std::io::stderr().is_terminal();
    loop {
        match etd::local::receive::<_, et_proto::Response>(&mut stream).await?.context("etd closed the connection")? {
            et_proto::Response::Progress { done, total } => {
                if show {
                    let pct = (done * 100).checked_div(total).unwrap_or(100);
                    eprint!("\r{to}: {pct:>3}%  {:.1} / {:.1} MB ", done as f64 / 1e6, total as f64 / 1e6);
                }
            }
            et_proto::Response::Delivered { paths, typed } => {
                if show {
                    eprintln!();
                }
                for path in paths {
                    println!("{path}");
                }
                if let (Some(session), false) = (session, typed) {
                    eprintln!("(not typed into {to}:{session}: no such manymux session there, or see etd's log on {to})");
                }
                return Ok(());
            }
            et_proto::Response::Error(e) => {
                if show {
                    eprintln!();
                }
                bail!("{e}")
            }
            other => bail!("unexpected answer: {other:?}"),
        }
    }
}

/// `etd files`: what every machine of the network has received.
async fn files(paths: &Paths) -> anyhow::Result<()> {
    let socket = etd::local::socket_path(paths);
    let mut stream =
        tokio::net::UnixStream::connect(&socket).await.with_context(|| format!("etd is not running here ({})", socket.display()))?;
    etd::local::send(&mut stream, &et_proto::Request::Shared).await?;
    let list = match etd::local::receive::<_, et_proto::Response>(&mut stream).await?.context("etd closed the connection")? {
        et_proto::Response::SharedList(list) => list,
        et_proto::Response::Error(e) => bail!("{e}"),
        other => bail!("unexpected answer: {other:?}"),
    };
    if list.is_empty() {
        println!("no files yet; send some with `etd send <path> <machine>`");
        return Ok(());
    }
    let now = config::now_secs();
    println!("{:<8} {:<12} {:<14} {:>9}  PATH", "WHEN", "MACHINE", "FROM", "SIZE");
    for item in list {
        let ago = now.saturating_sub(item.received);
        let when = match ago {
            0..60 => format!("{ago}s"),
            60..3600 => format!("{}m", ago / 60),
            3600..86400 => format!("{}h", ago / 3600),
            _ => format!("{}d", ago / 86400),
        };
        let size =
            if item.size >= 1_000_000 { format!("{:.1} MB", item.size as f64 / 1e6) } else { format!("{:.1} kB", item.size as f64 / 1e3) };
        let path = if item.is_dir { format!("{}/", item.path) } else { item.path };
        println!("{when:<8} {:<12} {:<14} {size:>9}  {path}", item.machine, item.from);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("etd=info"));
    match log_file()? {
        // A login item (E.T. for macOS) has no place for stderr.
        Some(file) => tracing_subscriber::fmt().with_env_filter(filter).with_ansi(false).with_writer(std::sync::Mutex::new(file)).init(),
        None => tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr).init(),
    }
    let paths = Paths::from_env()?;
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        [] | ["run"] => {
            let key = config::load_or_create_key(&paths.key())?;
            // The out-of-the-box path: run `etd` by hand, scan, done.
            let alone = network::load(&paths)?.is_none_or(|s| s.network.members().count() <= 1);
            let show_pairing = alone && config::load_clients(&paths)?.is_empty() && std::io::stdout().is_terminal();
            server::run(paths, key, show_pairing).await
        }
        [cmd @ ("invite" | "pair"), rest @ ..] => {
            let mut kind = Kind::Device;
            let mut url_only = false;
            for arg in rest {
                match *arg {
                    "--machine" if *cmd == "invite" => kind = Kind::Machine,
                    "--url" => url_only = true,
                    _ => bail!("{USAGE}"),
                }
            }
            let status = config::running_status(&paths)?.context("etd is not running; start it first (run `etd`)")?;
            pair::print(&paths, &status.addr, &status.host_name, kind, url_only)
        }
        ["install"] => service::install(&paths, true),
        ["install", "--no-service"] => service::install(&paths, false),
        ["uninstall"] => service::uninstall(),
        ["join", url, rest @ ..] => {
            let name = match rest {
                [] => tmux::host_name(),
                ["--name", name] => name.to_string(),
                _ => bail!("{USAGE}"),
            };
            let key = config::load_or_create_key(&paths.key())?;
            let endpoint = Endpoint::builder(presets::N0).secret_key(key).bind().await?;
            let joined = network::join(&endpoint, &paths, url, &name).await;
            endpoint.close().await;
            let signed = joined?;
            println!("joined the network {} as {name}", signed.network.name);
            for m in signed.network.members() {
                println!("  {:<20} {}", m.name, kind_word(m.kind));
            }
            if config::running_status(&paths)?.is_none() {
                println!("\netd is not running here. Start its service (it reads the new network when it starts),\nor install one with `etd install`, or run `etd` in a terminal.");
            }
            Ok(())
        }
        [cmd @ ("members" | "clients")] => {
            let _ = cmd;
            let signed = network::load(&paths)?.context("no network yet; start etd once")?;
            let me = config::load_or_create_key(&paths.key())?.public();
            println!("network {} ({})", signed.network.name, signed.network.key.fmt_short());
            for m in signed.network.members() {
                let here = if m.id == me { "  (this machine)" } else { "" };
                println!("  {:<20} {:<8} {}{here}", m.name, kind_word(m.kind), m.id);
            }
            Ok(())
        }
        ["revoke", who @ ..] if !who.is_empty() => {
            let me = config::load_or_create_key(&paths.key())?.public();
            for (id, name) in network::revoke(&paths, me, &who.join(" "))? {
                println!("revoked {name} ({id})");
            }
            println!("Every machine of the network closes its connections within seconds of hearing it.");
            Ok(())
        }
        ["status", "--json"] => {
            println!("{}", status_json(&paths)?);
            Ok(())
        }
        ["status"] => {
            let Some(s) = config::running_status(&paths)? else {
                println!("etd is not running");
                return Ok(());
            };
            println!("running     pid {}, etd {}", s.pid, s.version);
            println!("host        {}", s.host_name);
            println!("id          {}", s.addr.id);
            for url in s.addr.relay_urls() {
                println!("relay       {url}");
            }
            for ip in s.addr.ip_addrs() {
                println!("direct      {ip}");
            }
            println!("tmux        {}", s.tmux);
            if let Some(signed) = network::load(&paths)? {
                let machines = signed.network.machines().count();
                let all = signed.network.members().count();
                println!("network     {}: {machines} machine(s), {} device(s)", signed.network.name, all - machines);
            }
            println!("connected   {}", if s.connected.is_empty() { "none".into() } else { s.connected.join(", ") });
            Ok(())
        }
        ["id"] => {
            println!("{}", config::load_or_create_key(&paths.key())?.public());
            Ok(())
        }
        ["allow", id, rest @ ..] => {
            let id: EndpointId = id.parse().context("not an endpoint id")?;
            let name = if rest.is_empty() { id.fmt_short().to_string() } else { rest.join(" ") };
            let me = config::load_or_create_key(&paths.key())?.public();
            network::ensure(&paths, me, &tmux::host_name())?;
            network::admit(&paths, me, id, &name, Kind::Device)?;
            println!("allowed {name} ({id})");
            Ok(())
        }
        ["send", rest @ ..] if rest.len() >= 2 => send(&paths, rest).await,
        ["files"] => files(&paths).await,
        ["-h" | "--help" | "help"] => {
            println!("{USAGE}");
            Ok(())
        }
        _ => bail!("{USAGE}"),
    }
}
