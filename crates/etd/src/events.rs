//! Notifications from manymux: etd follows this machine's node, whose event
//! feed also carries the machines it watches, and runs `$ETD_NOTIFY_COMMAND`
//! for a session that wants someone while nobody is attached to it.
//!
//! The command runs through `/bin/sh -c` with the login environment and:
//! `ETD_KIND` (`bell`, `notify` or `exit`), `ETD_MACHINE` (a host name; this
//! machine's own for its sessions), `ETD_SESSION`, `ETD_TITLE`, `ETD_BODY`.
//! etd does not rate-limit or check whether an app is open: the command does.
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context};
use manymux::proto::{self as mp, tag, FrameReader};
use tokio::process::Command;
use tracing::{debug, info, warn};

use crate::mm;
use crate::tmux::Env;

/// What a notification says, or None when an event is not worth one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
    pub kind: &'static str,
    pub machine: String,
    pub session: String,
    pub title: String,
    pub body: String,
}

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
    Some(Note { kind, machine: event.host.clone(), session: e.session.clone(), title: e.title.clone(), body })
}

/// Runs `command` for one note. Its output goes to the log.
pub async fn run(env: &Env, command: &str, note: &Note) {
    let result = Command::new("/bin/sh")
        .arg("-c")
        .arg(command)
        .env_clear()
        .envs(env)
        .env("ETD_KIND", note.kind)
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

/// Follows the node's event feed until it ends. Errors say why.
async fn follow_once(env: &Env, command: &str) -> anyhow::Result<()> {
    let mut child = mm::agent_local(env)
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
    // stdin stays open: closing it would end the feed.
    while let Some(frame) = reader.next().await? {
        if frame.tag != tag::EVENT {
            continue;
        }
        let event: mp::HostedEvent = mp::decode(&frame.body)?;
        debug!("event: {} {}: {:?}", event.host, event.event.session, event.event.kind);
        if let Some(note) = note(&event) {
            run(env, command, &note).await;
        }
    }
    Ok(())
}

/// Follows forever, starting again after an error or a node restart.
pub async fn follow(env: Env, command: String) {
    let mut delay = Duration::from_secs(2);
    loop {
        match follow_once(&env, &command).await {
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

    fn event(kind: mp::EventKind, attached: usize) -> mp::HostedEvent {
        mp::HostedEvent {
            host: "gmktec".into(),
            event: mp::SessionEvent { session: "claude".into(), title: "fix the build".into(), kind, attached, host_attached: 0 },
        }
    }

    #[test]
    fn only_unseen_events_worth_a_push() {
        let bell = note(&event(mp::EventKind::Bell, 0)).unwrap();
        assert_eq!((bell.kind, bell.machine.as_str(), bell.session.as_str(), bell.body.as_str()), ("bell", "gmktec", "claude", "fix the build"));
        assert!(note(&event(mp::EventKind::Bell, 1)).is_none(), "someone is attached");
        let n = note(&event(mp::EventKind::Notify { title: "t".into(), body: "b".into() }, 0)).unwrap();
        assert_eq!((n.kind, n.body.as_str()), ("notify", "b"));
        assert_eq!(note(&event(mp::EventKind::Exited(3), 0)).unwrap().body, "ended with code 3");
        assert!(note(&event(mp::EventKind::Exited(0), 0)).is_none());
        assert!(note(&event(mp::EventKind::TitleChanged("x".into()), 0)).is_none());
        assert!(note(&event(mp::EventKind::Started, 0)).is_none());
    }
}
