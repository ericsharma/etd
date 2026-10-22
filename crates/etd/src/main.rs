use std::io::IsTerminal;

use anyhow::{bail, Context};
use etd::config::{self, Paths};
use etd::{pair, server, service};
use iroh::EndpointId;
use tracing_subscriber::EnvFilter;

const USAGE: &str = "\
usage:
  etd                     run the daemon (shows a QR code if no device is paired yet)
  etd install             run etd as a user service (systemd or launchd) and start it
  etd uninstall           stop and remove the user service
  etd pair [--url]        show a QR code that adds a device (valid 10 minutes, one use)
  etd status              show the daemon's id, addresses, and connected devices
  etd clients             list paired devices
  etd revoke <name|id>    remove a device and close its connections
  etd id                  print this machine's endpoint id
  etd allow <id> [name]   add a device without a QR code (for tests)

files: $ETD_HOME, else ~/.config/etd and ~/.local/state/etd
       (macOS: ~/Library/Application Support/etd)";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("etd=info")))
        .with_writer(std::io::stderr)
        .init();
    let paths = Paths::from_env()?;
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        [] | ["run"] => {
            let key = config::load_or_create_key(&paths.key())?;
            // The out-of-the-box path: run `etd` by hand, scan, done.
            let show_pairing = config::load_clients(&paths)?.is_empty() && std::io::stdout().is_terminal();
            server::run(paths, key, show_pairing).await
        }
        ["pair", rest @ ..] => {
            let url_only = match rest {
                [] => false,
                ["--url"] => true,
                _ => bail!("{USAGE}"),
            };
            let status = config::running_status(&paths)?.context("etd is not running; start it first (run `etd`)")?;
            pair::print(&paths, &status.addr, &status.host_name, url_only)
        }
        ["install"] => service::install(&paths),
        ["uninstall"] => service::uninstall(),
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
            println!("paired      {}", config::load_clients(&paths)?.len());
            println!("connected   {}", if s.connected.is_empty() { "none".into() } else { s.connected.join(", ") });
            Ok(())
        }
        ["clients"] => {
            for c in config::load_clients(&paths)? {
                println!("{}  {}", c.id, c.name);
            }
            Ok(())
        }
        ["revoke", who @ ..] if !who.is_empty() => {
            let removed = config::remove_clients(&paths, &who.join(" "))?;
            if removed.is_empty() {
                bail!("no paired device matches {:?}; see `etd clients`", who.join(" "));
            }
            for c in removed {
                println!("revoked {} ({})", c.name, c.id);
            }
            Ok(())
        }
        ["id"] => {
            println!("{}", config::load_or_create_key(&paths.key())?.public());
            Ok(())
        }
        ["allow", id, rest @ ..] => {
            let id: EndpointId = id.parse().context("not an endpoint id")?;
            let name = if rest.is_empty() { id.fmt_short().to_string() } else { rest.join(" ") };
            config::add_client(&paths, id, &name)?;
            println!("allowed {name} ({id})");
            Ok(())
        }
        ["-h" | "--help" | "help"] => {
            println!("{USAGE}");
            Ok(())
        }
        _ => bail!("{USAGE}"),
    }
}
