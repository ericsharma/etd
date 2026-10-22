//! etd: reach tmux on this machine from E.T. over iroh.
use anyhow::{bail, Context};
use iroh::EndpointId;
use tracing_subscriber::EnvFilter;

mod config;
mod server;
mod tmux;

const USAGE: &str = "\
usage:
  etd                     run the daemon
  etd id                  print this machine's endpoint id
  etd allow <id> [name]   allow a device to connect (phase 0; QR pairing comes later)
  etd clients             list allowed devices";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("etd=info")))
        .init();
    let home = config::home()?;
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        [] | ["run"] => {
            let key = config::load_or_create_key(&home.join("secret.key"))?;
            server::run(home, key).await
        }
        ["id"] => {
            println!("{}", config::load_or_create_key(&home.join("secret.key"))?.public());
            Ok(())
        }
        ["allow", id, rest @ ..] => {
            let id: EndpointId = id.parse().context("not an endpoint id")?;
            let name = if rest.is_empty() { id.fmt_short().to_string() } else { rest.join(" ") };
            let mut clients = config::load_clients(&home)?;
            clients.retain(|c| c.id != id);
            clients.push(config::Client { id, name: name.clone() });
            config::save_clients(&home, &clients)?;
            println!("allowed {name} ({id})");
            Ok(())
        }
        ["clients"] => {
            for c in config::load_clients(&home)? {
                println!("{}  {}", c.id, c.name);
            }
            Ok(())
        }
        ["-h" | "--help" | "help"] => {
            println!("{USAGE}");
            Ok(())
        }
        _ => bail!("{USAGE}"),
    }
}
