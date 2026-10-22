//! etd-probe: a small et/1 client for testing etd without the app.
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use et_proto::{read_frame, write_frame, ControlFrame, Request, Response, Target, ALPN};
use iroh::endpoint::{presets, Connection, QuicTransportConfig, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[allow(dead_code)]
#[path = "../config.rs"]
mod config;

const USAGE: &str = "\
usage: etd-probe [--relay-only] <command>
  id                          print this probe's endpoint id (allow it with `etd allow`)
  hello <etd-id>              print host info
  sessions <etd-id>           list tmux sessions
  attach <etd-id> <session>   print tmux -C output; stdin lines are tmux commands
  bench <etd-id> <session> [n]  time n tmux command round trips (default 20)";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let relay_only = args.first().is_some_and(|a| a == "--relay-only");
    if relay_only {
        args.remove(0);
    }
    let home = config::home()?;
    let key = config::load_or_create_key(&home.join("probe.key"))?;
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let (command, target, rest) = match args.as_slice() {
        ["id"] => {
            println!("{}", key.public());
            return Ok(());
        }
        [command, id, rest @ ..] => (*command, id.parse::<EndpointId>().context("not an endpoint id")?, rest),
        _ => bail!("{USAGE}"),
    };

    let start = Instant::now();
    let transport = QuicTransportConfig::builder()
        .keep_alive_interval(Duration::from_secs(5))
        .max_idle_timeout(Some(Duration::from_secs(30).try_into()?))
        .build();
    let builder = Endpoint::builder(presets::N0).secret_key(key).transport_config(transport);
    let builder = if relay_only { builder.clear_ip_transports() } else { builder };
    let endpoint = builder.bind().await?;
    let bound = start.elapsed();
    let conn = endpoint.connect(EndpointAddr::new(target), ALPN).await.context("connect")?;
    eprintln!("bind {:?}, connect {:?}, path: {}", bound, start.elapsed() - bound, path(&conn));

    match (command, rest) {
        ("hello", []) => println!("{:#?}", call(&conn, Request::Hello { app: "etd-probe".into() }).await?),
        ("sessions", []) => println!("{:#?}", call(&conn, Request::ListSessions).await?),
        ("attach", [session]) => attach(&conn, session).await?,
        ("bench", [session, n @ ..]) => {
            let n = n.first().map(|n| n.parse()).transpose()?.unwrap_or(20);
            bench(&conn, session, n).await?;
        }
        _ => bail!("{USAGE}"),
    }
    conn.close(0u32.into(), b"bye");
    endpoint.close().await;
    Ok(())
}

/// The selected path: "direct <addr>" or "relay <url>", with its RTT.
fn path(conn: &Connection) -> String {
    let paths = conn.paths();
    let Some(p) = paths.iter().find(|p| p.is_selected()) else { return "none yet".into() };
    let kind = if p.is_relay() { "relay" } else { "direct" };
    format!("{kind} {:?} rtt {:?}", p.remote_addr(), p.rtt())
}

async fn call(conn: &Connection, request: Request) -> anyhow::Result<Response> {
    let (mut send, mut recv) = conn.open_bi().await?;
    write_frame(&mut send, &request).await?;
    send.finish()?;
    read_frame(&mut recv).await?.context("no response")
}

async fn open_control(conn: &Connection, session: &str) -> anyhow::Result<(SendStream, RecvStream)> {
    let (mut send, mut recv) = conn.open_bi().await?;
    write_frame(&mut send, &Request::Control { target: Target::Attach(session.into()) }).await?;
    match read_frame::<Response>(&mut recv).await?.context("no response")? {
        Response::ControlStarted => Ok((send, recv)),
        other => bail!("control refused: {other:?}"),
    }
}

async fn attach(conn: &Connection, session: &str) -> anyhow::Result<()> {
    let (mut send, mut recv) = open_control(conn, session).await?;
    let up = tokio::spawn(async move {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if send.write_all(format!("{line}\n").as_bytes()).await.is_err() {
                break;
            }
        }
        let _ = send.finish();
    });
    let mut stdout = tokio::io::stdout();
    while let Some(frame) = read_frame::<ControlFrame>(&mut recv).await? {
        match frame {
            ControlFrame::Stdout(bytes) => {
                stdout.write_all(&bytes).await?;
                stdout.flush().await?;
            }
            ControlFrame::Stderr(line) => eprintln!("tmux stderr: {line}"),
            ControlFrame::Exit(code) => {
                eprintln!("tmux exited: {code:?}");
                break;
            }
        }
    }
    up.abort();
    Ok(())
}

/// Splits tmux -C output into lines across frame borders.
struct Lines {
    buf: Vec<u8>,
}

impl Lines {
    async fn next(&mut self, recv: &mut RecvStream) -> anyhow::Result<String> {
        loop {
            if let Some(i) = self.buf.iter().position(|&b| b == b'\n') {
                let line = String::from_utf8_lossy(&self.buf[..i]).into_owned();
                self.buf.drain(..=i);
                return Ok(line);
            }
            match read_frame::<ControlFrame>(recv).await?.context("stream ended")? {
                ControlFrame::Stdout(bytes) => self.buf.extend_from_slice(&bytes),
                ControlFrame::Stderr(line) => eprintln!("tmux stderr: {line}"),
                ControlFrame::Exit(code) => bail!("tmux exited: {code:?}"),
            }
        }
    }
}

/// A reply block end: `%end <time> <number> <flags>` (or `%error`).
fn reply_end(line: &str) -> Option<bool> {
    let mut f = line.split(' ');
    let tag = f.next()?;
    if tag != "%end" && tag != "%error" {
        return None;
    }
    Some(f.nth(2) == Some("1"))
}

async fn bench(conn: &Connection, session: &str, n: usize) -> anyhow::Result<()> {
    let (mut send, mut recv) = open_control(conn, session).await?;
    let mut lines = Lines { buf: Vec::new() };
    // tmux answers the attach itself first (flags 0).
    while reply_end(&lines.next(&mut recv).await?) != Some(false) {}
    let mut times = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        send.write_all(b"display-message -p etd-bench\n").await?;
        while reply_end(&lines.next(&mut recv).await?) != Some(true) {}
        times.push(t.elapsed());
    }
    send.finish()?;
    times.sort();
    let mean = times.iter().sum::<Duration>() / n as u32;
    println!(
        "{n} round trips: min {:?}, median {:?}, mean {mean:?}, max {:?}; path now: {}",
        times[0],
        times[n / 2],
        times[n - 1],
        path(conn)
    );
    Ok(())
}
