//! The Logbook (et-ios docs/PLAN-logbook.md): the transcripts of the AI
//! agents in this machine's manymux sessions go to the archive machines of
//! the network (capture.rs); an archive keeps them (store.rs).
//!
//! A machine is an archive after `etd logbook archive on` there
//! (`logbook.json`). The other machines ask each machine with `LogInfo`, at
//! start and every minute. With no archive in the network, nothing is read
//! and nothing is sent.
pub mod capture;
pub mod claude;
pub mod send;
pub mod serve;
pub mod store;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use et_proto::logbook::LogMeta;
use et_proto::{read_frame, write_frame, Request, Response};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify, OnceCell};
use tracing::{debug, info, warn};

use crate::config::{self, Paths};
use crate::server::State;
use crate::{mm, network};
use capture::{Archive, Capture, Net, TranscriptStatus};
use store::Store;

/// How often the capture looks for new lines.
const TICK: Duration = Duration::from_secs(2);
/// Ask the machines which are archives every this many ticks (1 minute).
const DISCOVER_EVERY: u32 = 30;

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Settings {
    /// This machine keeps the transcripts of the network.
    #[serde(default)]
    pub archive: bool,
}

pub fn settings(paths: &Paths) -> Settings {
    config::read_json(&paths.logbook_settings())
        .unwrap_or_else(|e| {
            warn!("logbook: {e:#}");
            None
        })
        .unwrap_or_default()
}

pub fn save_settings(paths: &Paths, settings: &Settings) -> anyhow::Result<()> {
    config::write_json(&paths.logbook_settings(), settings)
}

/// This machine's archive, opened at first use, and what the capture last
/// found out about the network's archives.
#[derive(Debug, Default)]
pub struct Local {
    store: OnceCell<Arc<Mutex<Store>>>,
    /// Woken at each append, for `LogRead` followers.
    appended: Arc<Notify>,
    /// The archives' names, and when they were found.
    names: std::sync::Mutex<Option<(Instant, Vec<String>)>>,
}

/// `LogArchives` answers from the capture's last look if it is this fresh.
const NAMES_FRESH: Duration = Duration::from_secs(90);

impl Local {
    pub async fn get(&self, paths: &Paths) -> anyhow::Result<Arc<Mutex<Store>>> {
        let dir = paths.logbook();
        self.store.get_or_try_init(|| async move { Ok(Arc::new(Mutex::new(Store::open(&dir)?))) }).await.cloned()
    }

    fn set_names(&self, names: Vec<String>) {
        *self.names.lock().unwrap() = Some((Instant::now(), names));
    }

    fn fresh_names(&self) -> Option<Vec<String>> {
        self.names.lock().unwrap().as_ref().filter(|(when, _)| when.elapsed() < NAMES_FRESH).map(|(_, names)| names.clone())
    }
}

/// `LogInfo` from the member called `asker`.
pub async fn info(paths: &Paths, local: &Local, asker: &str) -> Response {
    if !settings(paths).archive {
        return Response::LogInfo { archive: false, paused: Vec::new() };
    }
    match local.get(paths).await {
        Ok(store) => Response::LogInfo { archive: true, paused: store.lock().await.paused_for(asker) },
        Err(e) => Response::Error(format!("{e:#}")),
    }
}

/// `LogAppend` from the machine called `from`.
pub async fn append(paths: &Paths, local: &Local, from: &str, session_id: &str, meta: LogMeta, offset: u64, bytes: &[u8]) -> Response {
    if !settings(paths).archive {
        return Response::Error("not a Logbook archive (etd logbook archive on)".into());
    }
    let appended = async { local.get(paths).await?.lock().await.append(from, session_id, meta, offset, bytes, config::now_secs()) };
    match appended.await {
        Ok(offset) => {
            local.appended.notify_waiters();
            Response::LogAck { offset }
        }
        Err(e) => {
            warn!("logbook: {from}/{session_id}: {e:#}");
            Response::Error(format!("{e:#}"))
        }
    }
}

/// The archives of the network now, and the sessions this machine's own
/// archive does not record. A machine that does not answer is left out.
pub async fn archives(paths: &Paths, local: &Local, net: &Net) -> (Vec<Archive>, HashSet<String>) {
    let me = net.endpoint.id();
    let mut found = Vec::new();
    let mut here_paused = HashSet::new();
    if settings(paths).archive {
        match local.get(paths).await {
            Ok(store) => {
                here_paused = store.lock().await.paused_for(&net.me).into_iter().collect();
                found.push(Archive::Here { store, appended: local.appended.clone() });
            }
            Err(e) => warn!("logbook: this machine's archive: {e:#}"),
        }
    }
    let Ok(Some(signed)) = network::load(paths) else {
        return (found, here_paused);
    };
    let mut asks = tokio::task::JoinSet::new();
    for m in signed.network.machines().filter(|m| m.id != me) {
        let (id, name, net) = (m.id, m.name.to_string(), net.clone());
        asks.spawn(async move {
            let asked = async {
                let conn = net.connections.to(&net.endpoint, id).await?;
                let (mut send, mut recv) = conn.open_bi().await?;
                write_frame(&mut send, &Request::LogInfo).await?;
                send.finish()?;
                read_frame::<Response>(&mut recv).await?.context("no answer")
            };
            match tokio::time::timeout(Duration::from_secs(10), asked).await {
                Ok(Ok(Response::LogInfo { archive: true, paused })) => {
                    Some(Archive::There { id, name, paused: paused.into_iter().collect() })
                }
                Ok(Ok(_)) => None,
                other => {
                    debug!("logbook: no LogInfo from {name}: {other:?}");
                    None
                }
            }
        });
    }
    found.extend(asks.join_all().await.into_iter().flatten());
    (found, here_paused)
}

/// What `etd logbook` shows (`logbook-status.json`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Status {
    pub archives: Vec<String>,
    pub transcripts: Vec<TranscriptStatus>,
    /// Why nothing is sent, if so.
    pub note: Option<String>,
    pub updated: u64,
}

/// The capture, until etd stops.
pub async fn run(state: Arc<State>) {
    let Some(claude) = claude::Claude::from_env(state.env()) else {
        info!("logbook: no home directory; no capture");
        return;
    };
    let mut capture = Capture::new(vec![Box::new(claude)], state.env().clone());
    let (mut archives, mut here_paused) = (Vec::new(), HashSet::new());
    let mut names: Vec<String> = Vec::new();
    let mut ticks = 0u32;
    loop {
        let me =
            network::load(state.paths()).ok().flatten().and_then(|s| s.network.member(&state.endpoint().id()).map(|m| m.name.to_string()));
        let mut status = Status { updated: config::now_secs(), ..Default::default() };
        match me {
            None => status.note = Some("this machine is not in a network".into()),
            Some(me) => {
                let net = Net { endpoint: state.endpoint().clone(), connections: state.connections().clone(), me };
                if ticks.is_multiple_of(DISCOVER_EVERY) {
                    (archives, here_paused) = self::archives(state.paths(), state.logbook(), &net).await;
                    let now: Vec<String> = archives.iter().map(|a| a.name(&net.me).to_string()).collect();
                    if now != names {
                        info!("logbook: archives: {}", if now.is_empty() { "none".into() } else { now.join(", ") });
                        names = now;
                    }
                    state.logbook().set_names(names.clone());
                }
                ticks = ticks.wrapping_add(1);
                status.archives = names.clone();
                if archives.is_empty() {
                    status.note = Some("no archive in the network (etd logbook archive on)".into());
                } else {
                    match mm::sessions(state.env()).await {
                        Ok(sessions) => status.transcripts = capture.tick(&sessions, &archives, &net, &here_paused).await,
                        Err(e) => status.note = Some(format!("{e:#}")),
                    }
                }
            }
        }
        if let Err(e) = config::write_json(&state.paths().logbook_status(), &status) {
            warn!("logbook status: {e:#}");
        }
        tokio::time::sleep(TICK).await;
    }
}
