//! `LogOverview` (et-ios docs/PLAN-logbook-v2.md §4.5, package A1): for each
//! transcript on the archive, a headline, the last prompt, the first line of
//! the last reply, and counts, so the apps show what a session does without
//! a model.
//!
//! Each transcript is read once: `overview.json` keeps how far it is read
//! and what was found, and the next ask reads only the new lines.
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use et_proto::Response;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::warn;

use super::settings;
use super::store::{read_lines, Entry, Store};
use crate::config::{self, Paths};
use crate::server::State;

/// Bytes read at a time.
const CHUNK: usize = 4 * 1024 * 1024;
/// Longest `asked` / `said`.
const LINE: usize = 200;
/// Longest headline made from a prompt or a digest.
const HEADLINE: usize = 100;

/// What one transcript's lines say, as far as they are read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Seen {
    /// Read up to here in the archive's copy.
    offset: u64,
    first_prompt: Option<String>,
    asked: Option<String>,
    said: Option<String>,
    prompts: u32,
}

/// One transcript in the `Overview` JSON (§4.5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    /// `machine/session id`.
    pub key: String,
    pub agent: String,
    pub machine: String,
    /// The manymux session; empty if not known.
    pub mm: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headline: Option<String>,
    /// `digest`, `title` or `prompt`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headline_from: Option<String>,
    /// The first line of the last prompt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asked: Option<String>,
    /// The first line of the agent's last text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub said: Option<String>,
    /// `busy`, `idle` or `gone`.
    pub state: String,
    pub prompts: u32,
    /// Ended with fewer than 2 prompts: the apps hide it.
    pub trivial: bool,
    /// Signal kind → count (A2); empty until then.
    pub friction: BTreeMap<String, u32>,
    pub first: u64,
    pub last: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Overview {
    pub sessions: Vec<Session>,
}

fn cache_path(logbook: &Path) -> PathBuf {
    logbook.join("overview.json")
}

/// The first line of `text` that says something (not only an XML-like tag,
/// as a paste's wrapper), on one line, at most `max` characters.
fn one_line(text: &str, max: usize) -> Option<String> {
    let line = text.lines().map(str::trim).find(|l| !l.is_empty() && !(l.starts_with('<') && l.ends_with('>')))?;
    Some(cut(line, max))
}

/// At most `max` characters, cut at a word with "…".
fn cut(text: &str, max: usize) -> String {
    let text: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.chars().count() <= max {
        return text;
    }
    let head: String = text.chars().take(max - 1).collect();
    let head = match head.rfind(' ') {
        Some(at) if at > max / 2 => &head[..at],
        _ => &head,
    };
    format!("{}…", head.trim_end_matches([',', ';', ':', '.', ' ']))
}

/// A user line that is not the owner's prompt: a slash command or its
/// output, an interrupt, a background task's note to the agent.
fn not_a_prompt(text: &str) -> bool {
    ["<command-", "<local-command", "<task-notification>", "[Request interrupted by user", "Caveat:"].iter().any(|p| text.starts_with(p))
}

impl Seen {
    /// Takes in whole JSONL lines (Claude Code's format).
    fn read(&mut self, jsonl: &[u8]) {
        for line in jsonl.split(|&b| b == b'\n') {
            let has = |needle: &[u8]| line.windows(needle.len()).any(|w| w == needle);
            if !has(br#""type":"user""#) && !has(br#""type":"assistant""#) {
                continue;
            }
            let Ok(entry) = serde_json::from_slice::<Value>(line) else { continue };
            let flag = |name: &str| entry.get(name).and_then(Value::as_bool) == Some(true);
            if flag("isSidechain") || flag("isMeta") || flag("isCompactSummary") {
                continue;
            }
            let text = super::send::text_of(entry.pointer("/message/content").unwrap_or(&Value::Null));
            if text.is_empty() {
                continue;
            }
            match entry.get("type").and_then(Value::as_str) {
                Some("user") if !not_a_prompt(&text) => {
                    self.prompts += 1;
                    self.asked = one_line(&text, LINE);
                    if self.first_prompt.is_none() {
                        self.first_prompt = one_line(&text, HEADLINE);
                    }
                }
                Some("assistant") => {
                    if let Some(said) = one_line(&text, LINE) {
                        self.said = Some(said);
                    }
                }
                _ => {}
            }
        }
    }
}

/// The digest's "Goal:" line (watcher stage 1), if the report made one.
fn goals(logbook: &Path) -> BTreeMap<String, String> {
    let path = logbook.join("watcher").join("digests.json");
    let all: BTreeMap<String, Value> = config::read_json(&path).ok().flatten().unwrap_or_default();
    all.into_iter()
        .filter_map(|(key, digest)| {
            let text = digest.get("text")?.as_str()?;
            let goal = text.lines().find_map(|l| l.trim().strip_prefix("Goal:"))?.trim();
            Some((key, cut(goal, HEADLINE))).filter(|(_, g)| !g.is_empty())
        })
        .collect()
}

/// Reads what is new in each transcript, then makes the overview.
fn build(logbook: &Path, entries: &[(Entry, PathBuf)]) -> anyhow::Result<Overview> {
    let path = cache_path(logbook);
    let mut cache: BTreeMap<String, Seen> = config::read_json(&path)
        .unwrap_or_else(|e| {
            warn!("logbook overview: {e:#}; reading all transcripts again");
            None
        })
        .unwrap_or_default();
    let goals = goals(logbook);
    let mut changed = false;
    let mut sessions = Vec::with_capacity(entries.len());
    for (e, file) in entries {
        let key = format!("{}/{}", e.machine, e.session_id);
        let seen = cache.entry(key.clone()).or_default();
        if seen.offset > e.bytes {
            // The archive's copy is new (not the one read before).
            *seen = Seen::default();
        }
        while seen.offset < e.bytes {
            let (start, bytes) = read_lines(file, seen.offset, CHUNK)?;
            if bytes.is_empty() {
                break;
            }
            seen.read(&bytes);
            seen.offset = start + bytes.len() as u64;
            changed = true;
        }
        let (headline, from) = match (goals.get(&key), &e.title, &seen.first_prompt) {
            (Some(goal), _, _) => (Some(goal.clone()), Some("digest")),
            (None, Some(title), _) => (Some(title.clone()), Some("title")),
            (None, None, Some(prompt)) => (Some(prompt.clone()), Some("prompt")),
            _ => (None, None),
        };
        sessions.push(Session {
            key,
            agent: if e.meta.agent.is_empty() { "claude".into() } else { e.meta.agent.clone() },
            machine: e.machine.clone(),
            mm: e.meta.mm_session.clone(),
            headline,
            headline_from: from.map(String::from),
            asked: seen.asked.clone(),
            said: seen.said.clone(),
            state: e.meta.status.clone(),
            prompts: seen.prompts,
            trivial: e.meta.status == "gone" && seen.prompts < 2,
            friction: BTreeMap::new(),
            first: e.first,
            last: e.last,
        });
    }
    if changed {
        config::write_json(&path, &cache)?;
    }
    sessions.sort_by(|a, b| b.last.cmp(&a.last).then_with(|| a.key.cmp(&b.key)));
    Ok(Overview { sessions })
}

/// One build at a time: they share `overview.json`.
async fn overview(logbook: PathBuf, entries: Vec<(Entry, PathBuf)>) -> anyhow::Result<Overview> {
    static BUSY: OnceLock<Mutex<()>> = OnceLock::new();
    let _one = BUSY.get_or_init(|| Mutex::new(())).lock().await;
    tokio::task::spawn_blocking(move || build(&logbook, &entries)).await?
}

/// `LogOverview { since }`: `Overview { json }`.
pub async fn serve(state: &State, since: u64) -> Response {
    if !settings(state.paths()).archive {
        return Response::Error("not a Logbook archive (etd logbook archive on)".into());
    }
    let made = async {
        let entries: Vec<(Entry, PathBuf)> = {
            let store = state.logbook().get(state.paths()).await?;
            let store = store.lock().await;
            store.entries().filter(|e| e.changed.max(e.last) >= since).map(|e| (e.clone(), store.path(&e.machine, &e.session_id))).collect()
        };
        let overview = overview(state.paths().logbook(), entries).await?;
        anyhow::Ok(serde_json::to_string(&overview)?)
    };
    match made.await {
        Ok(json) => Response::Overview { json },
        Err(e) => Response::Error(format!("{e:#}")),
    }
}

/// "3m", "2h", "5d".
fn ago(secs: u64) -> String {
    match secs {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m", s / 60),
        s if s < 86_400 => format!("{}h", s / 3600),
        s => format!("{}d", s / 86_400),
    }
}

/// `etd logbook overview [--json]`.
pub async fn cli(paths: &Paths, json: bool) -> anyhow::Result<()> {
    if !settings(paths).archive {
        anyhow::bail!("not a Logbook archive (etd logbook archive on)");
    }
    let logbook = paths.logbook();
    let entries: Vec<(Entry, PathBuf)> = Store::read_entries(&logbook)?
        .into_iter()
        .map(|e| {
            let file = logbook.join(&e.machine).join(format!("{}.jsonl", e.session_id));
            (e, file)
        })
        .collect();
    let overview = overview(logbook, entries).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&overview)?);
        return Ok(());
    }
    let now = config::now_secs();
    let mut hidden = 0;
    for s in &overview.sessions {
        if s.trivial {
            hidden += 1;
            continue;
        }
        let dot = match s.state.as_str() {
            "busy" => "●",
            "idle" => "○",
            _ => " ",
        };
        let mut out =
            format!("{dot} {}  {} · {}", s.headline.as_deref().unwrap_or("(no prompt yet)"), s.machine, ago(now.saturating_sub(s.last)));
        if let Some(asked) = &s.asked {
            let _ = write!(out, "\n    You: {}", cut(asked, 100));
        }
        if let Some(said) = &s.said {
            let _ = write!(out, "\n    {}: {}", s.agent, cut(said, 100));
        }
        println!("{out}");
    }
    if hidden > 0 {
        println!("({hidden} ended with fewer than 2 prompts; --json shows them)");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(text: &str) -> String {
        serde_json::json!({"type":"user","uuid":"u","message":{"role":"user","content":text}}).to_string()
    }

    fn assistant(text: &str) -> String {
        serde_json::json!({"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":text}]}}).to_string()
    }

    fn tool_result() -> String {
        serde_json::json!({"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t","content":"ok"}]}})
            .to_string()
    }

    fn jsonl(lines: &[String]) -> Vec<u8> {
        lines.iter().flat_map(|l| format!("{l}\n").into_bytes()).collect()
    }

    #[test]
    fn prompts_and_replies_not_commands_or_notes() {
        let mut seen = Seen::default();
        seen.read(&jsonl(&[
            user("<command-name>/clear</command-name>"),
            user("fix the login\nmore detail"),
            assistant("Looking.\nSecond line."),
            tool_result(),
            user("[Request interrupted by user]"),
            user("<task-notification>\n<status>completed</status>"),
            user("<pasted_content id=\"ab\">\nrun the tests\n</pasted_content id=\"ab\">"),
            assistant("All 12 tests pass."),
            serde_json::json!({"type":"user","isMeta":true,"message":{"content":"meta"}}).to_string(),
        ]));
        assert_eq!(seen.prompts, 2);
        assert_eq!(seen.first_prompt.as_deref(), Some("fix the login"));
        assert_eq!(seen.asked.as_deref(), Some("run the tests"));
        assert_eq!(seen.said.as_deref(), Some("All 12 tests pass."));
    }

    #[test]
    fn long_text_is_cut_at_a_word() {
        let text = "word ".repeat(60);
        let line = cut(&text, 40);
        assert!(line.chars().count() <= 40 && line.ends_with('…'), "{line}");
        assert!(!line.contains("  "));
        assert_eq!(cut("short", 40), "short");
    }

    fn entry(session_id: &str, status: &str, bytes: u64, title: Option<&str>) -> Entry {
        Entry {
            machine: "gmktec".into(),
            session_id: session_id.into(),
            meta: et_proto::logbook::LogMeta {
                agent: "claude".into(),
                mm_session: "claude".into(),
                status: status.into(),
                ..Default::default()
            },
            offset: bytes,
            bytes,
            first: 100,
            last: 200,
            changed: 200,
            title: title.map(String::from),
        }
    }

    #[test]
    fn headline_from_digest_then_title_then_prompt_and_reads_only_new_lines() {
        let dir = tempfile::tempdir().unwrap();
        let logbook = dir.path();
        let file = logbook.join("a.jsonl");
        let mut lines = vec![user("first prompt"), assistant("ok")];
        std::fs::write(&file, jsonl(&lines)).unwrap();
        let size = |f: &Path| std::fs::metadata(f).unwrap().len();

        let one = build(logbook, &[(entry("a", "busy", size(&file), None), file.clone())]).unwrap();
        let s = &one.sessions[0];
        assert_eq!((s.headline.as_deref(), s.headline_from.as_deref()), (Some("first prompt"), Some("prompt")));
        assert_eq!((s.prompts, s.trivial), (1, false), "a live session is never trivial");
        assert_eq!(s.key, "gmktec/a");

        // More lines: only they are read (the cache has the first prompt).
        lines.push(user("second prompt"));
        lines.push(assistant("done"));
        std::fs::write(&file, jsonl(&lines)).unwrap();
        let cached: BTreeMap<String, Seen> = config::read_json(&cache_path(logbook)).unwrap().unwrap();
        assert_eq!(cached["gmktec/a"].prompts, 1);
        let two = build(logbook, &[(entry("a", "gone", size(&file), Some("Login fix")), file.clone())]).unwrap();
        let s = &two.sessions[0];
        assert_eq!((s.headline.as_deref(), s.headline_from.as_deref()), (Some("Login fix"), Some("title")));
        assert_eq!((s.prompts, s.asked.as_deref(), s.said.as_deref()), (2, Some("second prompt"), Some("done")));

        std::fs::create_dir_all(logbook.join("watcher")).unwrap();
        let digests = serde_json::json!({"gmktec/a": {"offset": 1, "at": 1, "text": "Goal: Fix the login page.\nDone: x\nOpen: y\nBlocked: nothing"}});
        std::fs::write(logbook.join("watcher/digests.json"), digests.to_string()).unwrap();
        let three = build(logbook, &[(entry("a", "gone", size(&file), Some("Login fix")), file.clone())]).unwrap();
        assert_eq!(three.sessions[0].headline.as_deref(), Some("Fix the login page."));
        assert_eq!(three.sessions[0].headline_from.as_deref(), Some("digest"));
    }

    #[test]
    fn an_ended_session_with_one_prompt_is_trivial() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("b.jsonl");
        std::fs::write(&file, jsonl(&[user("status?"), assistant("all good")])).unwrap();
        let bytes = std::fs::metadata(&file).unwrap().len();
        let o = build(dir.path(), &[(entry("b", "gone", bytes, None), file)]).unwrap();
        assert!(o.sessions[0].trivial);
        let json = serde_json::to_value(&o).unwrap();
        assert_eq!(json["sessions"][0]["friction"], serde_json::json!({}), "friction is an empty object until A2");
    }
}
