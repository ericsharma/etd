//! Friction signals (et-ios docs/PLAN-logbook-v2.md §4.3, package A2): the
//! places in each transcript where the owner interrupted, denied a tool or
//! corrected the agent, and where tools failed. Found with no model.
//!
//! Each signal names its turn as the app's `Transcript.parse` does (the
//! `uuid` of the last owner prompt before it, `leading` before the first),
//! so the app can jump to it. Each transcript is read once:
//! `signals.json` keeps how far it is read, the scan's state, and what was
//! found.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use et_proto::Response;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

use super::settings;
use super::store::{read_lines, Entry, Store};
use crate::config::{self, Paths};
use crate::server::State;

/// Bytes read at a time.
const CHUNK: usize = 4 * 1024 * 1024;
/// Longest excerpt.
const EXCERPT: usize = 300;
/// Failures of one tool call in a row that make a `repeat-error`.
const REPEATS: u32 = 3;
/// The turn id before the first prompt (the app's `Transcript.leadingTurnID`).
const LEADING: &str = "leading";

/// One signal (B's JSON in PLAN-logbook-v2 §8, B5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signal {
    /// `interrupt`, `denied`, `tool-error`, `repeat-error`, `correction`,
    /// `compact` or `asked-needlessly`.
    pub kind: String,
    /// `machine/session id`.
    pub key: String,
    /// The `uuid` of the prompt that starts the turn, or `leading`.
    pub turn: String,
    /// The `uuid` of the line with the signal.
    pub uuid: String,
    /// Unix seconds (0: the line had no time).
    pub time: u64,
    pub excerpt: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Signals {
    pub signals: Vec<Signal>,
}

/// One transcript's scan, as far as it is read.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Scan {
    /// Read up to here in the archive's copy.
    offset: u64,
    /// The turn the next line belongs to (empty: `leading`).
    turn: String,
    /// The agent wrote text in this turn.
    replied: bool,
    /// The last line of the agent's last text in this turn.
    last_line: Option<String>,
    /// Tool calls with no result yet: id → (name, input in one line).
    pending: BTreeMap<String, (String, String)>,
    /// The last failed call and how many times in a row it failed.
    streak: Option<(String, String, u32)>,
    found: Vec<Signal>,
}

fn cache_path(logbook: &Path) -> PathBuf {
    logbook.join("signals.json")
}

/// One line, at most `max` characters.
fn excerpt(text: &str, max: usize) -> String {
    let text: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.chars().count() <= max {
        return text;
    }
    let head: String = text.chars().take(max - 1).collect();
    format!("{head}…")
}

/// The text of a tool result's content.
fn result_text(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        Value::Array(blocks) => blocks.iter().filter_map(|b| b.get("text").and_then(Value::as_str)).collect::<Vec<_>>().join("\n"),
        _ => String::new(),
    }
}

/// A tool input in one line: the field that says what it does (as the
/// app's `Transcript.summary`).
fn tool_summary(input: &Value) -> String {
    let what = ["command", "file_path", "path", "pattern", "url", "query", "description", "prompt", "skill"]
        .iter()
        .find_map(|field| input.get(field).and_then(Value::as_str))
        .unwrap_or_default();
    excerpt(what.lines().next().unwrap_or_default(), 200)
}

/// `2026-10-28T18:35:26.783Z` → Unix seconds.
fn unix_time(iso: &str) -> Option<u64> {
    let num = |range: std::ops::Range<usize>| iso.get(range)?.parse::<i64>().ok();
    let (y, m, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (hh, mm, ss) = (num(11..13)?, num(14..16)?, num(17..19)?);
    // Days from 1970-01-01 (Howard Hinnant's days_from_civil).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    u64::try_from(days * 86_400 + hh * 3600 + mm * 60 + ss).ok()
}

/// Not a turn of its own in the app (`Transcript.parse`).
fn not_a_turn(text: &str) -> bool {
    ["<command-", "<local-command", "[Request interrupted"].iter().any(|p| text.starts_with(p))
}

/// An owner prompt that pushes back on what the agent did.
fn is_correction(prompt: &str) -> bool {
    let p = prompt.trim().to_lowercase().replace('’', "'");
    [
        "no",
        "nope",
        "don't",
        "dont",
        "do not",
        "stop",
        "wrong",
        "that's wrong",
        "thats wrong",
        "that's not",
        "thats not",
        "not what",
        "i said",
        "again",
        "you didn't",
        "you did not",
        "why did you",
    ]
    .iter()
    .any(|start| p.strip_prefix(start).is_some_and(|rest| !rest.starts_with(|c: char| c.is_alphanumeric())))
}

/// An owner prompt that only says "go on".
fn is_go_ahead(prompt: &str) -> bool {
    let p = prompt.trim().to_lowercase();
    let p = p.trim_end_matches(['.', '!', ' ']);
    [
        "y",
        "yes",
        "yep",
        "yeah",
        "yes please",
        "ok",
        "okay",
        "k",
        "sure",
        "go",
        "go ahead",
        "do it",
        "proceed",
        "continue",
        "please",
        "yes do it",
        "ok do it",
    ]
    .contains(&p)
}

impl Scan {
    fn add(&mut self, kind: &str, key: &str, line: &Value, excerpt_text: &str) {
        self.found.push(Signal {
            kind: kind.into(),
            key: key.into(),
            turn: if self.turn.is_empty() { LEADING.into() } else { self.turn.clone() },
            uuid: line.get("uuid").and_then(Value::as_str).unwrap_or_default().into(),
            time: line.get("timestamp").and_then(Value::as_str).and_then(unix_time).unwrap_or(0),
            excerpt: excerpt(excerpt_text, EXCERPT),
        });
    }

    /// Takes in whole JSONL lines (Claude Code's format).
    fn read(&mut self, key: &str, jsonl: &[u8]) {
        for line in jsonl.split(|&b| b == b'\n') {
            let has = |needle: &[u8]| line.windows(needle.len()).any(|w| w == needle);
            let compact = has(br#""subtype":"compact_boundary""#);
            if !compact && !has(br#""type":"user""#) && !has(br#""type":"assistant""#) {
                continue;
            }
            let Ok(entry) = serde_json::from_slice::<Value>(line) else { continue };
            let flag = |name: &str| entry.get(name).and_then(Value::as_bool) == Some(true);
            if flag("isSidechain") {
                continue;
            }
            if compact {
                let trigger = entry.pointer("/compactMetadata/trigger").and_then(Value::as_str).unwrap_or("auto");
                self.add("compact", key, &entry, &format!("Conversation compacted ({trigger})"));
                continue;
            }
            if flag("isMeta") {
                continue;
            }
            let Some(content) = entry.pointer("/message/content") else { continue };
            match entry.get("type").and_then(Value::as_str) {
                Some("assistant") => self.assistant(content),
                Some("user") => self.user(key, &entry, content),
                _ => {}
            }
        }
    }

    fn assistant(&mut self, content: &Value) {
        for block in content.as_array().into_iter().flatten() {
            match block.get("type").and_then(Value::as_str) {
                Some("text") => {
                    let text = block.get("text").and_then(Value::as_str).unwrap_or_default().trim();
                    if let Some(last) = text.lines().map(str::trim).rfind(|l| !l.is_empty()) {
                        self.replied = true;
                        self.last_line = Some(excerpt(last, 200));
                    }
                }
                Some("tool_use") => {
                    let id = block.get("id").and_then(Value::as_str).unwrap_or_default();
                    let name = block.get("name").and_then(Value::as_str).unwrap_or("tool");
                    let summary = tool_summary(block.get("input").unwrap_or(&Value::Null));
                    self.pending.insert(id.into(), (name.into(), summary));
                }
                _ => {}
            }
        }
    }

    fn user(&mut self, key: &str, entry: &Value, content: &Value) {
        // Tool results.
        for block in content.as_array().into_iter().flatten() {
            if block.get("type").and_then(Value::as_str) != Some("tool_result") {
                continue;
            }
            let id = block.get("tool_use_id").and_then(Value::as_str).unwrap_or_default();
            let (name, input) = self.pending.remove(id).unwrap_or_else(|| ("tool".into(), String::new()));
            let failed = block.get("is_error").and_then(Value::as_bool) == Some(true);
            let text = result_text(block.get("content").unwrap_or(&Value::Null));
            if text.contains("The user doesn't want to proceed with this tool use") {
                self.add("denied", key, entry, &format!("{name}: {input}"));
                self.streak = None;
            } else if failed {
                let first = text.lines().map(str::trim).find(|l| !l.is_empty()).unwrap_or_default();
                self.add("tool-error", key, entry, &format!("{name}: {input} → {first}"));
                let count = match &self.streak {
                    Some((n, i, count)) if *n == name && *i == input => count + 1,
                    _ => 1,
                };
                if count == REPEATS {
                    self.add("repeat-error", key, entry, &format!("{name} failed {REPEATS} times in a row: {input}"));
                }
                self.streak = Some((name, input, count));
            } else {
                self.streak = None;
            }
        }
        let text = super::send::text_of(content);
        if text.is_empty() {
            return;
        }
        if text.starts_with("[Request interrupted by user") {
            self.add("interrupt", key, entry, &text);
            return;
        }
        if not_a_turn(&text) {
            return;
        }
        // A new turn: the signals about how the owner answered go in it.
        let (replied, question) = (self.replied, self.last_line.take());
        self.turn = entry.get("uuid").and_then(Value::as_str).unwrap_or_default().into();
        self.replied = false;
        self.pending.clear();
        if replied && is_correction(&text) {
            self.add("correction", key, entry, &text);
        }
        if let Some(question) = question.filter(|q| q.ends_with('?')) {
            if is_go_ahead(&text) {
                self.add("asked-needlessly", key, entry, &format!("{question} → {}", text.trim()));
            }
        }
    }
}

/// Reads what is new in each transcript: its signals, by key.
pub(super) fn scan(logbook: &Path, entries: &[(Entry, PathBuf)]) -> anyhow::Result<BTreeMap<String, Vec<Signal>>> {
    static BUSY: OnceLock<Mutex<()>> = OnceLock::new();
    let _one = BUSY.get_or_init(|| Mutex::new(())).lock().unwrap_or_else(|e| e.into_inner());
    let path = cache_path(logbook);
    let mut cache: BTreeMap<String, Scan> = config::read_json(&path)
        .unwrap_or_else(|e| {
            warn!("logbook signals: {e:#}; reading all transcripts again");
            None
        })
        .unwrap_or_default();
    let mut changed = false;
    let mut out = BTreeMap::new();
    for (e, file) in entries {
        let key = format!("{}/{}", e.machine, e.session_id);
        let scan = cache.entry(key.clone()).or_default();
        if scan.offset > e.bytes {
            *scan = Scan::default();
        }
        while scan.offset < e.bytes {
            let (start, bytes) = read_lines(file, scan.offset, CHUNK)?;
            if bytes.is_empty() {
                break;
            }
            scan.read(&key, &bytes);
            scan.offset = start + bytes.len() as u64;
            changed = true;
        }
        out.insert(key, scan.found.clone());
    }
    if changed {
        config::write_json(&path, &cache)?;
    }
    Ok(out)
}

/// The signal counts by kind, for the overview's `friction`.
pub(super) fn counts(signals: &[Signal]) -> BTreeMap<String, u32> {
    let mut counts = BTreeMap::new();
    for s in signals {
        *counts.entry(s.kind.clone()).or_insert(0) += 1;
    }
    counts
}

fn flat(found: BTreeMap<String, Vec<Signal>>) -> Signals {
    let mut signals: Vec<Signal> = found.into_values().flatten().collect();
    signals.sort_by(|a, b| a.time.cmp(&b.time).then_with(|| a.key.cmp(&b.key)));
    Signals { signals }
}

/// `LogSignals { since }`: `Signals { json }`, all signals of each
/// transcript that changed at or after `since`, oldest first.
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
        let logbook = state.paths().logbook();
        let found = tokio::task::spawn_blocking(move || scan(&logbook, &entries)).await??;
        anyhow::Ok(serde_json::to_string(&flat(found))?)
    };
    match made.await {
        Ok(json) => Response::Signals { json },
        Err(e) => Response::Error(format!("{e:#}")),
    }
}

/// `etd meta signals`: counts by kind, then the last signals.
pub async fn cli(paths: &Paths) -> anyhow::Result<()> {
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
    let signals = flat(tokio::task::spawn_blocking(move || scan(&logbook, &entries)).await??).signals;
    let counts = counts(&signals);
    let total: u32 = counts.values().sum();
    println!("{total} signal(s): {}", counts.iter().map(|(k, n)| format!("{k} {n}")).collect::<Vec<_>>().join(", "));
    for s in signals.iter().rev().take(20) {
        let short: String = s.key.chars().take(s.key.find('/').map_or(0, |i| i + 9)).collect();
        println!("  {:<17} {:<17} {}", s.kind, short, excerpt(&s.excerpt, 90));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn prompt(uuid: &str, text: &str) -> Value {
        json!({"type":"user","uuid":uuid,"timestamp":"2026-10-28T18:35:26.783Z","message":{"role":"user","content":text}})
    }

    fn says(text: &str) -> Value {
        json!({"type":"assistant","uuid":"a","message":{"role":"assistant","content":[{"type":"text","text":text}]}})
    }

    fn call(id: &str, command: &str) -> Value {
        json!({"type":"assistant","uuid":"c","message":{"role":"assistant","content":[{"type":"tool_use","id":id,"name":"Bash","input":{"command":command}}]}})
    }

    fn result(uuid: &str, id: &str, text: &str, error: bool) -> Value {
        json!({"type":"user","uuid":uuid,"message":{"role":"user","content":[{"type":"tool_result","tool_use_id":id,"content":text,"is_error":error}]}})
    }

    fn scan_of(lines: &[Value]) -> Vec<Signal> {
        let jsonl: Vec<u8> = lines.iter().flat_map(|l| format!("{l}\n").into_bytes()).collect();
        let mut scan = Scan::default();
        scan.read("gmktec/s", &jsonl);
        scan.found
    }

    fn kinds(found: &[Signal]) -> Vec<(&str, &str)> {
        found.iter().map(|s| (s.kind.as_str(), s.turn.as_str())).collect()
    }

    #[test]
    fn each_kind_in_its_turn() {
        let found = scan_of(&[
            call("t0", "ls"),
            result("r0", "t0", "boom", true),
            prompt("p1", "fix the build"),
            says("On it."),
            call("t1", "rm -rf build"),
            result("r1", "t1", "The user doesn't want to proceed with this tool use. The tool use was rejected", true),
            prompt("i1", "[Request interrupted by user for tool use]"),
            prompt("p2", "no, don't delete it"),
            says("Understood.\nShall I run the tests?"),
            prompt("p3", "yes"),
            json!({"type":"system","subtype":"compact_boundary","uuid":"cb","compactMetadata":{"trigger":"auto"}}),
            prompt("p4", "<command-name>/clear</command-name>"),
            json!({"type":"user","uuid":"m","isMeta":true,"message":{"content":"[Request interrupted by user]"}}),
        ]);
        assert_eq!(
            kinds(&found),
            [
                ("tool-error", "leading"),
                ("denied", "p1"),
                ("interrupt", "p1"),
                ("correction", "p2"),
                ("asked-needlessly", "p3"),
                ("compact", "p3"),
            ]
        );
        assert_eq!(found[0].excerpt, "Bash: ls → boom");
        assert_eq!(found[1].excerpt, "Bash: rm -rf build");
        assert_eq!(found[4].excerpt, "Shall I run the tests? → yes");
        assert_eq!((found[2].uuid.as_str(), found[2].time), ("i1", 1_793_212_526));
    }

    #[test]
    fn three_failures_of_one_call_in_a_row() {
        let mut lines = vec![prompt("p1", "deploy")];
        for i in 0..4 {
            lines.push(call(&format!("t{i}"), "nixos-rebuild switch"));
            lines.push(result(&format!("r{i}"), &format!("t{i}"), "error: lacks a signature", true));
        }
        let found = scan_of(&lines);
        let repeats: Vec<_> = found.iter().filter(|s| s.kind == "repeat-error").collect();
        assert_eq!(repeats.len(), 1, "one per streak");
        assert_eq!(repeats[0].uuid, "r2");
        assert_eq!(found.iter().filter(|s| s.kind == "tool-error").count(), 4);

        // A success in between ends the streak.
        let found = scan_of(&[
            call("a", "make"),
            result("ra", "a", "x", true),
            call("b", "make"),
            result("rb", "b", "x", true),
            call("c", "make"),
            result("rc", "c", "ok", false),
            call("d", "make"),
            result("rd", "d", "x", true),
        ]);
        assert!(!found.iter().any(|s| s.kind == "repeat-error"));
    }

    #[test]
    fn a_first_prompt_or_a_new_question_is_not_a_correction() {
        assert!(is_correction("No. Use the other branch"));
        assert!(is_correction("don't push yet"));
        assert!(!is_correction("nothing changed?"), "a word that starts with no");
        assert!(!is_correction("stopwatch feature"));
        assert!(is_go_ahead("Yes."));
        assert!(!is_go_ahead("yes but use pnpm"));
        // "no" with no reply before it: the first prompt.
        assert!(scan_of(&[prompt("p1", "no idea why CI fails, look")]).is_empty());
        // A question answered with more than "go on" is fine.
        assert!(scan_of(&[prompt("p1", "x"), says("Which file?"), prompt("p2", "the README")]).is_empty());
    }

    #[test]
    fn iso_times() {
        assert_eq!(unix_time("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(unix_time("2000-03-01T00:00:00.000Z"), Some(951_868_800));
        assert_eq!(unix_time("garbage"), None);
    }

    #[test]
    fn scan_keeps_its_state_across_reads() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("s.jsonl");
        let write = |lines: &[Value]| {
            std::fs::write(&file, lines.iter().map(|l| format!("{l}\n")).collect::<String>()).unwrap();
        };
        let entry = |bytes: u64| Entry {
            machine: "gmktec".into(),
            session_id: "s".into(),
            meta: Default::default(),
            offset: bytes,
            bytes,
            first: 0,
            last: 0,
            changed: 0,
            title: None,
        };
        let mut lines = vec![prompt("p1", "build it"), says("Should I deploy too?")];
        write(&lines);
        let size = || std::fs::metadata(&file).unwrap().len();
        assert!(scan(dir.path(), &[(entry(size()), file.clone())]).unwrap()["gmktec/s"].is_empty());
        // The answer comes in a later read: the question was kept.
        lines.push(prompt("p2", "go ahead"));
        write(&lines);
        let found = scan(dir.path(), &[(entry(size()), file.clone())]).unwrap();
        assert_eq!(kinds(&found["gmktec/s"]), [("asked-needlessly", "p2")]);
        assert_eq!(counts(&found["gmktec/s"]), BTreeMap::from([("asked-needlessly".to_string(), 1)]));
    }
}
