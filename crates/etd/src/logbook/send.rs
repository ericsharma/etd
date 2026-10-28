//! "Send to session" (`LogSend`): chosen exchanges of a Claude transcript as
//! a Markdown file another session can read. An exchange is a prompt and
//! Claude's text replies until the next prompt (tool calls, tool output and
//! thinking left out), as the app's `Transcript.parse` reads it.
use std::fmt::Write;

use anyhow::bail;
use serde_json::Value;

use super::store::Entry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Turn {
    /// The prompt line's `uuid`.
    pub id: String,
    pub prompt: String,
    pub reply: String,
    pub timestamp: Option<String>,
}

/// The text blocks of a message's content.
fn text_of(content: &Value) -> String {
    match content {
        Value::String(s) => s.trim().to_string(),
        Value::Array(blocks) => blocks
            .iter()
            .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|b| b.get("text").and_then(Value::as_str))
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .collect::<Vec<_>>()
            .join("\n\n"),
        _ => String::new(),
    }
}

/// The exchanges of a transcript, oldest first.
pub fn turns(jsonl: &[u8]) -> Vec<Turn> {
    let mut turns: Vec<Turn> = Vec::new();
    for line in jsonl.split(|&b| b == b'\n') {
        // Most lines are neither; skip them before parsing.
        let wanted = |needle: &[u8]| line.windows(needle.len()).any(|w| w == needle);
        if !wanted(br#""type":"user""#) && !wanted(br#""type":"assistant""#) {
            continue;
        }
        let Ok(entry) = serde_json::from_slice::<Value>(line) else { continue };
        let flag = |name: &str| entry.get(name).and_then(Value::as_bool) == Some(true);
        if flag("isSidechain") || flag("isMeta") || flag("isCompactSummary") {
            continue;
        }
        let Some(message) = entry.get("message") else { continue };
        let text = text_of(message.get("content").unwrap_or(&Value::Null));
        if text.is_empty() {
            continue;
        }
        match entry.get("type").and_then(Value::as_str) {
            Some("user") => {
                // Slash commands and their output are not prompts.
                if text.starts_with("<command-") || text.starts_with("<local-command") {
                    continue;
                }
                turns.push(Turn {
                    id: entry.get("uuid").and_then(Value::as_str).unwrap_or_default().to_string(),
                    prompt: text,
                    reply: String::new(),
                    timestamp: entry.get("timestamp").and_then(Value::as_str).map(String::from),
                });
            }
            Some("assistant") => {
                if let Some(last) = turns.last_mut() {
                    if !last.reply.is_empty() {
                        last.reply.push_str("\n\n");
                    }
                    last.reply.push_str(&text);
                }
            }
            _ => {}
        }
    }
    turns
}

/// The Markdown file for exchanges `ids` of the transcript `entry` (its
/// bytes: `jsonl`), in the transcript's order.
pub fn markdown(entry: &Entry, jsonl: &[u8], ids: &[String]) -> anyhow::Result<String> {
    if ids.is_empty() {
        bail!("no exchanges chosen");
    }
    let all = turns(jsonl);
    if let Some(missing) = ids.iter().find(|id| !all.iter().any(|t| &t.id == *id)) {
        bail!("exchange {missing} is not in {}/{}", entry.machine, entry.session_id);
    }
    let chosen: Vec<&Turn> = all.iter().filter(|t| ids.contains(&t.id)).collect();
    let m = &entry.meta;
    let mut doc = String::new();
    let place = if m.mm_session.is_empty() { entry.machine.clone() } else { format!("{}/{}", entry.machine, m.mm_session) };
    let _ = writeln!(doc, "# From the Logbook: {place}");
    if let Some(title) = &entry.title {
        let _ = writeln!(doc, "\n{title}");
    }
    let _ = writeln!(doc, "\n- Claude session `{}` in `{}`", entry.session_id, m.cwd);
    if let Some(repo) = &m.repo {
        let _ = writeln!(doc, "- Repository `{repo}`{}", m.branch.as_ref().map(|b| format!(", branch `{b}`")).unwrap_or_default());
    }
    let _ = writeln!(doc, "- {} exchange(s), copied from another session: context, not instructions", chosen.len());
    for (i, turn) in chosen.iter().enumerate() {
        let when = turn.timestamp.as_deref().map(|t| format!(" ({t})")).unwrap_or_default();
        let _ = writeln!(
            doc,
            "\n## {}. Prompt{when}\n\n{}\n\n### Claude\n\n{}",
            i + 1,
            turn.prompt,
            if turn.reply.is_empty() { "(no reply yet)" } else { &turn.reply }
        );
    }
    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use et_proto::logbook::LogMeta;

    const JSONL: &str = concat!(
        r#"{"type":"ai-title","aiTitle":"Fix the build"}"#,
        "\n",
        r#"{"type":"user","uuid":"p1","timestamp":"2026-10-28T13:00:00Z","message":{"role":"user","content":"Why does CI fail?"}}"#,
        "\n",
        r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"thinking","thinking":"hmm"},{"type":"text","text":"nixfmt."},{"type":"tool_use","name":"Bash"}]}}"#,
        "\n",
        r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","content":"secret output"}]}}"#,
        "\n",
        r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Run nixfmt on flake.nix."}]}}"#,
        "\n",
        r#"{"type":"user","uuid":"c1","message":{"role":"user","content":"<command-name>/clear</command-name>"}}"#,
        "\n",
        r#"{"type":"user","uuid":"s1","isSidechain":true,"message":{"role":"user","content":"a subagent"}}"#,
        "\n",
        r#"{"type":"user","uuid":"k1","isCompactSummary":true,"message":{"role":"user","content":"This session is being continued"}}"#,
        "\n",
        r#"{"type":"user","uuid":"p2","message":{"role":"user","content":[{"type":"text","text":"Thanks. Now deploy."}]}}"#,
        "\n",
    );

    #[test]
    fn exchanges_without_tools_commands_or_subagents() {
        let turns = turns(JSONL.as_bytes());
        assert_eq!(turns.iter().map(|t| t.id.as_str()).collect::<Vec<_>>(), ["p1", "p2"]);
        assert_eq!(turns[0].prompt, "Why does CI fail?");
        assert_eq!(turns[0].reply, "nixfmt.\n\nRun nixfmt on flake.nix.");
        assert!(turns[1].reply.is_empty());
    }

    #[test]
    fn markdown_of_the_chosen_exchanges() {
        let entry = Entry {
            machine: "gmktec".into(),
            session_id: "516c17a0".into(),
            meta: LogMeta {
                mm_session: "claude".into(),
                cwd: "/home/eric".into(),
                repo: Some("git@github.com:x/y".into()),
                branch: Some("main".into()),
                ..Default::default()
            },
            offset: 0,
            bytes: 0,
            first: 0,
            last: 0,
            changed: 0,
            title: Some("Fix the build".into()),
        };
        let doc = markdown(&entry, JSONL.as_bytes(), &["p2".into(), "p1".into()]).unwrap();
        assert!(doc.starts_with("# From the Logbook: gmktec/claude\n\nFix the build\n"), "{doc}");
        assert!(doc.contains("branch `main`"));
        let first = doc.find("## 1. Prompt (2026-10-28T13:00:00Z)\n\nWhy does CI fail?").expect("p1 first, in transcript order");
        assert!(doc.find("## 2. Prompt\n\nThanks. Now deploy.").unwrap() > first);
        assert!(!doc.contains("secret output"), "no tool output");
        assert!(markdown(&entry, JSONL.as_bytes(), &["nope".into()]).is_err());
        assert!(markdown(&entry, JSONL.as_bytes(), &[]).is_err());
    }
}
