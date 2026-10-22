//! Pushes from manymux (PLAN-network §4.6). Each etd follows its own
//! machine's node. For a session that wants someone while nobody is attached:
//! - its own session: run the push command here, if this machine has one,
//!   and send `Notify` to the other machines of the network (a machine with
//!   a push command runs it, the others ignore it);
//! - a machine this node reaches by ssh that is NOT in the network (the setup
//!   before N6): run the push command here, since no etd there reports it;
//! - a machine in the network: skip; its own etd reports it.
//!
//! The push command (`$ETD_NOTIFY_COMMAND`) runs through `/bin/sh -c` with the
//! login environment and `ETD_KIND` (`bell`, `notify`, `exit`), `ETD_MACHINE`
//! (the network's name for the machine), `ETD_SESSION`, `ETD_TITLE`,
//! `ETD_BODY`. It does not run while the app says it is open (`Foreground`).
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use et_proto::network::Network;
use et_proto::{read_frame, write_frame, Note, Request, Response, ALPN};
use iroh::EndpointId;
use manymux::proto::{self as mp, tag, FrameReader};
use tokio::process::Command;
use tracing::{debug, info, warn};

use crate::server::State;
use crate::tmux::Env;
use crate::{mm, network, tmux};

/// What a notification says, or None when an event is not worth one.
/// `machine` is the event's host as the node names it.
pub fn note(event: &mp::HostedEvent) -> Option<Note> {
    let e = &event.event;
    // Somebody is looking at it: they saw it.
    if e.attached > 0 {
        return None;
    }
    let (kind, body) = match &e.kind {
        mp::EventKind::Bell => ("bell", e.title.clone()),
        mp::EventKind::Notify { title, body } => ("notify", if body.is_empty() { title.clone() } else { body.clone() }),
        // A clean exit is not worth an interruption; a failure is.
        mp::EventKind::Exited(code) if *code != 0 => ("exit", format!("ended with code {code}")),
        _ => return None,
    };
    Some(Note { kind: kind.into(), machine: event.host.clone(), session: e.session.clone(), title: e.title.clone(), body })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// This machine's session: push here and tell the other machines.
    Own(Note),
    /// A machine reached by ssh that is not in the network: push here only.
    Legacy(Note),
    Skip,
}

/// Where one event goes. The node names its own machine by its host name,
/// and watched machines by the name they were added with.
pub fn route(event: &mp::HostedEvent, host_name: &str, network: Option<&Network>, me: EndpointId) -> Route {
    let Some(mut note) = note(event) else { return Route::Skip };
    if event.host == host_name {
        if let Some(name) = network.and_then(|n| n.member(&me)).map(|m| m.name.to_string()) {
            note.machine = name;
        }
        return Route::Own(note);
    }
    if network.is_some_and(|n| n.machines().any(|m| m.name == event.host)) {
        return Route::Skip;
    }
    Route::Legacy(note)
}

/// Runs the push command for `note`, unless there is none or the app is open.
pub async fn deliver(state: &State, note: &Note) {
    let Some(command) = state.notify_command() else { return };
    if state.app_open() {
        info!("no push for {}/{}: E.T. is open", note.machine, note.session);
        return;
    }
    run(state.env(), command, note).await;
}

/// Runs `command` for one note. Its output goes to the log.
pub async fn run(env: &Env, command: &str, note: &Note) {
    let result = Command::new("/bin/sh")
        .arg("-c")
        .arg(command)
        .env_clear()
        .envs(env)
        .env("ETD_KIND", &note.kind)
        .env("ETD_MACHINE", &note.machine)
        .env("ETD_SESSION", &note.session)
        .env("ETD_TITLE", &note.title)
        .env("ETD_BODY", &note.body)
        .stdin(Stdio::null())
        .output()
        .await;
    match result {
        Ok(out) if out.status.success() => info!("notified: {} {}/{}", note.kind, note.machine, note.session),
        Ok(out) => warn!("notify command: {}: {}", out.status, String::from_utf8_lossy(&out.stderr).trim()),
        Err(e) => warn!("notify command: {e}"),
    }
}

/// Sends `note` to every other machine of the network.
async fn forward(state: &State, note: &Note) {
    let endpoint = state.endpoint().clone();
    let me = endpoint.id();
    let Ok(Some(signed)) = network::load(state.paths()) else { return };
    let mut tasks = tokio::task::JoinSet::new();
    for id in signed.network.machines().map(|m| m.id).filter(|id| *id != me) {
        let (endpoint, note) = (endpoint.clone(), note.clone());
        tasks.spawn(async move {
            let sent = async {
                let conn = tokio::time::timeout(Duration::from_secs(15), endpoint.connect(id, ALPN)).await.context("timed out")??;
                let (mut send, mut recv) = conn.open_bi().await?;
                write_frame(&mut send, &Request::Notify { note }).await?;
                send.finish()?;
                let reply = read_frame::<Response>(&mut recv).await?;
                conn.close(0u32.into(), b"sent");
                match reply {
                    Some(Response::Pong) => anyhow::Ok(()),
                    other => bail!("{other:?}"),
                }
            };
            if let Err(e) = sent.await {
                warn!("note not sent to {}: {e:#}", id.fmt_short());
            }
        });
    }
    tasks.join_all().await;
}

/// Follows this machine's node until the feed ends. Errors say why.
async fn follow_once(state: &State) -> anyhow::Result<()> {
    let mut child = mm::agent_local(state.env())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("start `mm agent`")?;
    let mut stdin = child.stdin.take().context("stdin")?;
    let mut reader = FrameReader::new(child.stdout.take().context("stdout")?);
    mp::write_msg(&mut stdin, tag::REQUEST, &mp::Request::Events).await?;
    match reader.next().await?.context("mm agent ended")? {
        frame if frame.tag == tag::RESPONSE => match mp::decode::<mp::Response>(&frame.body)? {
            mp::Response::Ok => {}
            other => bail!("events refused: {other:?}"),
        },
        frame => bail!("unexpected frame {:#x}", frame.tag),
    }
    info!("following manymux events");
    let host_name = tmux::host_name();
    let me = state.endpoint().id();
    // stdin stays open: closing it would end the feed.
    while let Some(frame) = reader.next().await? {
        if frame.tag != tag::EVENT {
            continue;
        }
        let event: mp::HostedEvent = mp::decode(&frame.body)?;
        debug!("event: {} {}: {:?}", event.host, event.event.session, event.event.kind);
        let signed = network::load(state.paths()).ok().flatten();
        match route(&event, &host_name, signed.as_ref().map(|s| &s.network), me) {
            Route::Own(note) => {
                deliver(state, &note).await;
                forward(state, &note).await;
            }
            Route::Legacy(note) => deliver(state, &note).await,
            Route::Skip => {}
        }
    }
    Ok(())
}

/// Follows forever, starting again after an error or a node restart.
pub async fn follow(state: Arc<State>) {
    let mut delay = Duration::from_secs(2);
    loop {
        match follow_once(&state).await {
            Ok(()) => {
                info!("manymux event feed ended; following again");
                delay = Duration::from_secs(2);
            }
            Err(e) => {
                warn!("manymux events: {e:#}; again in {delay:?}");
                delay = (delay * 2).min(Duration::from_secs(60));
            }
        }
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use et_proto::network::{Kind, State as Member};

    fn event(host: &str, kind: mp::EventKind, attached: usize) -> mp::HostedEvent {
        mp::HostedEvent {
            host: host.into(),
            event: mp::SessionEvent { session: "claude".into(), title: "fix the build".into(), kind, attached, host_attached: 0 },
        }
    }

    #[test]
    fn only_unseen_events_worth_a_push() {
        let bell = note(&event("gmktec", mp::EventKind::Bell, 0)).unwrap();
        assert_eq!((bell.kind.as_str(), bell.machine.as_str(), bell.session.as_str(), bell.body.as_str()), ("bell", "gmktec", "claude", "fix the build"));
        assert!(note(&event("gmktec", mp::EventKind::Bell, 1)).is_none(), "someone is attached");
        let n = note(&event("gmktec", mp::EventKind::Notify { title: "t".into(), body: "b".into() }, 0)).unwrap();
        assert_eq!((n.kind.as_str(), n.body.as_str()), ("notify", "b"));
        assert_eq!(note(&event("gmktec", mp::EventKind::Exited(3), 0)).unwrap().body, "ended with code 3");
        assert!(note(&event("gmktec", mp::EventKind::Exited(0), 0)).is_none());
        assert!(note(&event("gmktec", mp::EventKind::TitleChanged("x".into()), 0)).is_none());
        assert!(note(&event("gmktec", mp::EventKind::Started, 0)).is_none());
    }

    #[test]
    fn routes_own_legacy_and_network_machines() {
        let key = iroh::SecretKey::generate();
        let me = iroh::SecretKey::generate().public();
        let gmktec = iroh::SecretKey::generate().public();
        let mut net = Network::new(&key, "home", me, "mac-mini");
        // Own events carry the host name; the note uses the network's name.
        match route(&event("m1-mini", mp::EventKind::Bell, 0), "m1-mini", Some(&net), me) {
            Route::Own(n) => assert_eq!(n.machine, "mac-mini"),
            other => panic!("{other:?}"),
        }
        // gmktec reached by ssh, not in the network: this etd pushes for it.
        assert!(matches!(route(&event("gmktec", mp::EventKind::Bell, 0), "m1-mini", Some(&net), me), Route::Legacy(_)));
        // Once gmktec is in the network, its own etd does.
        net.set(gmktec, Member::Member { name: "gmktec".into(), kind: Kind::Machine }, me);
        assert_eq!(route(&event("gmktec", mp::EventKind::Bell, 0), "m1-mini", Some(&net), me), Route::Skip);
        assert_eq!(route(&event("m1-mini", mp::EventKind::Bell, 1), "m1-mini", Some(&net), me), Route::Skip);
    }
}
