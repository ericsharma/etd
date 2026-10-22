//! Lists a machine's manymux sessions through etd's `Agent` request. Read
//! only: it sends manymux `List` and nothing else.
//!
//!     cargo run --example agent_ls -- <etd-id> [host]
//!
//! Uses the probe key (`etd-probe id`), which must be paired with that etd.
use std::time::Duration;

use anyhow::{bail, Context};
use et_proto::{read_frame, write_frame, ControlFrame, Request, Response, ALPN};
use etd::config::{self, Paths};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, EndpointId};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (id, host) = match args.as_slice() {
        [id] => (id, None),
        [id, host] => (id, Some(host.clone())),
        _ => bail!("usage: agent_ls <etd-id> [host]"),
    };
    let id: EndpointId = id.parse().context("not an endpoint id")?;
    let key = config::load_or_create_key(&Paths::from_env()?.config.join("probe.key"))?;
    let endpoint = Endpoint::builder(presets::N0).secret_key(key).bind().await?;
    let conn = endpoint.connect(EndpointAddr::new(id), ALPN).await?;

    let (mut send, mut recv) = conn.open_bi().await?;
    write_frame(&mut send, &Request::Agent { host: host.clone() }).await?;
    match read_frame::<Response>(&mut recv).await?.context("no answer")? {
        Response::AgentStarted => {}
        other => bail!("{other:?}"),
    }
    let mut request = Vec::new();
    manymux::proto::write_msg(&mut request, manymux::proto::tag::REQUEST, &manymux::proto::Request::List).await?;
    send.write_all(&request).await?;
    let mut bytes = Vec::new();
    let answer: manymux::proto::Response = loop {
        if bytes.len() >= 5 {
            let len = u32::from_be_bytes(bytes[1..5].try_into()?) as usize;
            if bytes.len() >= 5 + len {
                break manymux::proto::decode(&bytes[5..5 + len])?;
            }
        }
        match tokio::time::timeout(Duration::from_secs(20), read_frame::<ControlFrame>(&mut recv)).await??.context("stream ended")? {
            ControlFrame::Stdout(b) => bytes.extend(b),
            ControlFrame::Stderr(line) => eprintln!("stderr: {line}"),
            ControlFrame::Exit(code) => bail!("mm agent exited: {code:?}"),
        }
    };
    send.finish()?;
    match answer {
        manymux::proto::Response::Sessions(list) => {
            println!("{}: {} session(s)", host.as_deref().unwrap_or("this machine"), list.len());
            for s in list {
                println!("  {}  {}  idle {}s  attached {}", s.name, s.title, s.idle, s.attached);
            }
        }
        other => println!("{other:?}"),
    }
    conn.close(0u32.into(), b"bye");
    endpoint.close().await;
    Ok(())
}
