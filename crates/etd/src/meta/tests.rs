use super::*;
use crate::agentcfg::File;

fn signal(kind: &str, key: &str, turn: &str) -> Signal {
    Signal { kind: kind.into(), key: key.into(), turn: turn.into(), uuid: "u".into(), time: 100, excerpt: format!("{kind} here") }
}

fn file(id: &str, path: &str, kind: &str, content: &str, git: bool) -> File {
    File {
        id: id.into(),
        path: path.into(),
        kind: kind.into(),
        agent: "claude".into(),
        sha256: format!("sha-{}", content.len()),
        size: content.len() as u64,
        mtime: 0,
        git,
        link: None,
        content: Some(content.into()),
        too_big: false,
    }
}

fn snapshots() -> Vec<Snapshot> {
    vec![
        Snapshot {
            machine: "trigkey".into(),
            files: vec![
                file("~/.claude/CLAUDE.md", "~/.claude/CLAUDE.md", "instructions", "Rules.\n", false),
                file(
                    "~/.claude/projects/<home>/memory/MEMORY.md",
                    "~/.claude/projects/-home-eric/memory/MEMORY.md",
                    "memory",
                    "- [a](a.md)\n",
                    false,
                ),
                file("~/.claude/skills/x/SKILL.md", "~/.claude/skills/x/SKILL.md", "skill", "description: x\n", true),
            ],
            error: None,
        },
        Snapshot {
            machine: "mac-mini".into(),
            files: vec![file("~/.claude/CLAUDE.md", "~/.claude/CLAUDE.md", "instructions", "Rules.\nMore.\n", false)],
            error: None,
        },
        Snapshot { machine: "gmktec".into(), error: Some("etd 0.0.12".into()), ..Default::default() },
    ]
}

fn answer(json: serde_json::Value) -> Answer {
    serde_json::from_value(json).unwrap()
}

#[test]
fn settle_keeps_only_grounded_proposals_that_apply() {
    let signals = vec![signal("asked-needlessly", "trigkey/a", "p1"), signal("asked-needlessly", "mac-mini/b", "p9")];
    let decided = vec![Decision {
        decision: "reject".into(),
        finding: "old-idea".into(),
        proposal: None,
        machines: vec![],
        note: None,
        run: None,
        at: 1,
    }];
    let a = answer(serde_json::json!({"findings": [
        {"id": "Asks before tests", "title": "Asks before running tests", "kind": "asked-needlessly", "why": "w",
         "evidence": [{"key": "trigkey/a", "turn": "p1"}, {"key": "mac-mini/b", "turn": "p9"}, {"key": "x/y", "turn": "zz"}],
         "proposals": [
            {"route": "change", "file": "~/.claude/CLAUDE.md", "machines": ["all"], "find": "", "replace": "- Run tests without asking.", "why": "", "expect": "asked-needlessly drops"},
            {"route": "change", "file": "~/.claude/projects/<home>/memory/tests.md", "machines": ["all"], "find": "", "replace": "Tests: cargo test", "why": "", "expect": ""},
            {"route": "change", "file": "~/.claude/skills/x/SKILL.md", "machines": ["trigkey"], "find": "description: x", "replace": "description: y", "why": "", "expect": ""}
         ]},
        {"id": "made-up", "title": "No evidence", "evidence": [{"key": "trigkey/a", "turn": "nope"}],
         "proposals": [{"route": "note", "replace": "x"}]},
        {"id": "old-idea", "title": "Rejected before", "evidence": [{"key": "trigkey/a", "turn": "p1"}],
         "proposals": [{"route": "note", "replace": "x"}]},
        {"id": "bad-changes", "title": "Only bad changes", "evidence": [{"key": "trigkey/a", "turn": "p1"}],
         "proposals": [
            {"route": "change", "file": "~/.claude/CLAUDE.md", "find": "", "replace": "key sk-ant-123"},
            {"route": "change", "file": "~/.claude/CLAUDE.md", "find": "Not there", "replace": "x"},
            {"route": "change", "file": "~/.claude/settings.json", "find": "", "replace": "x"}
         ]}
    ]}));
    let run = settle(a, &signals, &snapshots(), &decided, "7".into(), 7);
    let ids: Vec<&str> = run.findings.iter().map(|f| f.id.as_str()).collect();
    assert_eq!(ids, ["asks-before-tests"], "{:#?}", run.dropped);
    let f = &run.findings[0];
    assert_eq!((f.count, f.sessions), (2, 2), "the unknown evidence is left out");

    let [p1, p2, p3] = &f.proposals[..] else { panic!("3 proposals: {:#?}", f.proposals) };
    // CLAUDE.md on both machines that answered, each from its own content.
    assert_eq!(p1.route, "write");
    let on: Vec<(&str, &str, &str)> = p1.targets.iter().map(|t| (t.machine.as_str(), t.base.as_str(), t.content.as_str())).collect();
    assert_eq!(
        on,
        [
            ("trigkey", "sha-7", "Rules.\n- Run tests without asking.\n"),
            ("mac-mini", "sha-13", "Rules.\nMore.\n- Run tests without asking.\n")
        ]
    );
    assert!(p1.patch.contains("+- Run tests without asking."), "{}", p1.patch);
    // A new memory file: in trigkey's home project folder; mac-mini has none.
    assert_eq!(p2.targets.len(), 1);
    assert_eq!((p2.targets[0].path.as_str(), p2.targets[0].base.as_str()), ("~/.claude/projects/-home-eric/memory/tests.md", ""));
    // A skill is in git: an agent changes it.
    assert_eq!((p3.route.as_str(), p3.targets[0].content.as_str()), ("agent", "description: y\n"));

    let dropped = run.dropped.join("\n");
    for why in [
        "gmktec: no instruction files",
        "made-up: no evidence",
        "old-idea: rejected",
        "secret-like",
        "not in ~/.claude/CLAUDE.md",
        "has no ~/.claude/settings.json",
    ] {
        assert!(dropped.contains(why), "{why} in:\n{dropped}");
    }
    assert_eq!(run.drift.len(), 3, "CLAUDE.md differs; MEMORY.md and the skill are only on trigkey");
}

#[test]
fn decisions_are_checked_and_kept() {
    let dir = tempfile::tempdir().unwrap();
    record(dir.path(), r#"{"decision":"reject","finding":"Asks Before Tests","note":"I like the question"}"#, 5).unwrap();
    record(dir.path(), r#"{"decision":"applied","finding":"x","proposal":"p1","machines":["trigkey"],"run":"7"}"#, 6).unwrap();
    assert!(record(dir.path(), r#"{"decision":"maybe","finding":"x"}"#, 7).is_err());
    assert!(record(dir.path(), "not json", 7).is_err());
    let all = decisions(dir.path()).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!((all[0].finding.as_str(), all[0].at), ("asks-before-tests", 5));
}

/// A stand-in for `claude -p`: one finding about the interrupt it sees.
fn fake_claude(dir: &Path) -> String {
    let script = dir.join("claude");
    std::fs::write(
        &script,
        r#"#!/bin/sh
prompt=$(cat)
printf '%s' "$prompt" > "$(dirname "$0")/prompt"
key=$(printf '%s\n' "$prompt" | sed -n 's/^- key \([^ ]*\) turn \([^ ]*\) .*/\1/p' | head -1)
turn=$(printf '%s\n' "$prompt" | sed -n 's/^- key \([^ ]*\) turn \([^ ]*\) .*/\2/p' | head -1)
json=$(printf '{"findings":[{"id":"stop-deleting","title":"Deletes without asking","kind":"interrupt","why":"w","evidence":[{"key":"%s","turn":"%s"}],"proposals":[{"route":"change","file":"~/.claude/CLAUDE.md","machines":["all"],"find":"","replace":"- Ask before rm -rf.","why":"w","expect":"interrupt drops"}]}]}' "$key" "$turn" | sed 's/"/\\"/g')
printf '{"type":"result","is_error":false,"result":"%s"}' "$json"
"#,
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    script.display().to_string()
}

#[tokio::test]
async fn a_run_reads_signals_asks_the_model_and_keeps_the_findings() {
    let tmp = tempfile::tempdir().unwrap();
    let logbook = tmp.path().join("logbook");
    std::fs::create_dir_all(&logbook).unwrap();
    let jsonl = [
        r#"{"type":"user","uuid":"p1","message":{"role":"user","content":"clean the build folder"}}"#,
        r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Deleting it."}]}}"#,
        r#"{"type":"user","uuid":"i1","message":{"role":"user","content":"[Request interrupted by user]"}}"#,
        r#"{"type":"user","uuid":"p2","message":{"role":"user","content":"ask me first"}}"#,
    ]
    .join("\n")
        + "\n";
    let transcript = tmp.path().join("s.jsonl");
    std::fs::write(&transcript, &jsonl).unwrap();
    let now = config::now_secs();
    let entry = Entry {
        machine: "trigkey".into(),
        session_id: "s".into(),
        meta: Default::default(),
        offset: jsonl.len() as u64,
        bytes: jsonl.len() as u64,
        first: now,
        last: now,
        changed: now,
        title: None,
    };
    let bin = tempfile::tempdir().unwrap();
    let env: Env = std::env::vars().filter(|(k, _)| k == "PATH").collect();
    let model = Model { program: fake_claude(bin.path()), env, dir: tmp.path().join("model") };
    let run = build(&logbook, &model, vec![(entry, transcript)], snapshots(), &|_, _, _| {}).await.unwrap();

    let prompt = std::fs::read_to_string(bin.path().join("prompt")).unwrap();
    assert!(prompt.contains("[this turn] Owner: clean the build folder") && prompt.contains("[next turn] Owner: ask me first"), "{prompt}");
    assert!(prompt.contains("### ~/.claude/CLAUDE.md"), "the files are in the prompt");
    assert_eq!(run.findings.len(), 1, "{:#?}", run.dropped);
    assert_eq!(run.findings[0].evidence[0].turn, "p1");
    assert_eq!(latest(&logbook).unwrap().unwrap(), run);

    let paths = Paths { config: tmp.path().into(), state: tmp.path().into(), data: tmp.path().into() };
    let (name, doc) = finding(&paths, &run.id, "stop-deleting").unwrap();
    assert!(name.starts_with("meta-stop-deleting-") && name.ends_with(".md"));
    assert!(doc.contains("# From the meta agent: Deletes without asking") && doc.contains("+- Ask before rm -rf."), "{doc}");
    assert!(finding(&paths, &run.id, "nope").is_err());
    assert!(finding(&paths, "../x", "stop-deleting").is_err());
}

#[test]
fn targets_carry_what_undo_needs() {
    let signals = vec![signal("interrupt", "trigkey/a", "p1")];
    let a = answer(serde_json::json!({"findings": [{"id": "f", "title": "t", "evidence": [{"key": "trigkey/a", "turn": "p1"}],
        "proposals": [{"route": "change", "file": "~/.claude/CLAUDE.md", "machines": ["trigkey"], "find": "Rules.", "replace": "Rules!"}]}]}));
    let run = settle(a, &signals, &snapshots(), &[], "1".into(), 1);
    let t = &run.findings[0].proposals[0].targets[0];
    assert_eq!((t.old.as_str(), t.content.as_str()), ("Rules.\n", "Rules!\n"));
    assert_eq!(t.after, agentcfg::sha256_hex(b"Rules!\n"));
}

#[test]
fn followup_counts_the_kinds_on_the_machines_before_and_after() {
    let dir = tempfile::tempdir().unwrap();
    let logbook = dir.path();
    let run = MetaRun {
        id: "50".into(),
        taken: 50,
        findings: vec![Finding {
            id: "stop-deleting".into(),
            title: "Deletes without asking".into(),
            kind: "interrupt".into(),
            why: String::new(),
            count: 1,
            sessions: 1,
            evidence: vec![Evidence { key: "trigkey/a".into(), turn: "p1".into(), kind: "interrupt".into(), excerpt: String::new() }],
            proposals: vec![],
        }],
        drift: vec![],
        followup: vec![],
        dropped: vec![],
    };
    config::write_json(&runs(logbook).join("50.json"), &run).unwrap();
    let applied = 10_000;
    let decision = |what: &str, machines: &[&str], at: u64| Decision {
        decision: what.into(),
        finding: "stop-deleting".into(),
        proposal: Some("p1".into()),
        machines: machines.iter().map(|m| m.to_string()).collect(),
        note: None,
        run: Some("50".into()),
        at,
    };
    let at = |kind: &str, key: &str, time: u64| Signal { time, ..signal(kind, key, "x") };
    let signals = vec![
        at("interrupt", "trigkey/a", applied - 900),
        at("interrupt", "gmktec/b", applied - 800),
        at("interrupt", "trigkey/a", applied - 5_000), // before the window
        at("interrupt", "trigkey/c", applied + 300),
        at("interrupt", "mac-mini/d", applied + 400), // not applied there
        at("tool-error", "trigkey/a", applied + 500), // another kind
        at("interrupt", "trigkey/a", 0),              // no time
    ];
    let decided = vec![decision("applied", &["trigkey"], applied), decision("applied", &["gmktec"], applied + 50)];
    let rows = followup(logbook, &decided, &signals, applied + 1_000);
    assert_eq!(rows.len(), 1, "one row for both machines");
    let f = &rows[0];
    assert_eq!(
        (f.title.as_str(), f.machines.clone(), f.kinds.clone()),
        ("Deletes without asking", vec!["gmktec".to_string(), "trigkey".into()], vec!["interrupt".to_string()])
    );
    assert_eq!((f.span, f.before, f.after, f.undone), (1_000, 2, 1, false));

    let mut decided = decided;
    decided.push(decision("undo", &["trigkey"], applied + 600));
    assert!(followup(logbook, &decided, &signals, applied + 1_000)[0].undone);
}
