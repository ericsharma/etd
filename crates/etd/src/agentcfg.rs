//! Agent instruction files (et-ios docs/PLAN-logbook-v2.md D9, package A3):
//! `AgentConfig` reads this machine's allow-listed files (`CLAUDE.md` and
//! what it imports, memory, skills, each agent's global instruction file);
//! `ConfigWrite` replaces one, with a base hash check and a backup. Over the
//! local socket, `AgentConfig` asks every machine, and `etd meta drift`
//! compares them.
//!
//! Never on the list: settings, hooks, shell files, keys. A file inside a
//! git repository (the skills repo) is listed but not written: it changes
//! through an agent, which commits it (D8).
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context};
use et_proto::{read_frame, write_frame, Request, Response};
use iroh::Endpoint;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info};

use crate::config::{self, Paths};
use crate::local::Connections;
use crate::network;
use crate::tmux::Env;

/// Content larger than this is not sent (`too_big`).
const MAX_CONTENT: u64 = 64 * 1024;
/// Largest `ConfigWrite`.
const MAX_WRITE: usize = 256 * 1024;
/// `@` imports followed from `CLAUDE.md`, as Claude Code does.
const IMPORT_DEPTH: usize = 5;
/// Folders looked into under `~/.claude/skills`.
const SKILL_DEPTH: usize = 4;
/// Other agents' global instruction files: (agent, path from home).
const OTHER_AGENTS: [(&str, &str); 2] = [("codex", ".codex/AGENTS.md"), ("gemini", ".gemini/GEMINI.md")];
/// Text that must never go into an instruction file.
pub(crate) const SECRETS: [&str; 9] =
    ["-----BEGIN", "PRIVATE KEY", "ghp_", "github_pat_", "sk-ant-", "AKIA", "xoxb-", "xoxp-", "AGE-SECRET-KEY-"];

/// One allow-listed file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct File {
    /// The same file on every machine: `path` with this machine's
    /// Claude project folder for the home directory as `<home>`.
    pub id: String,
    /// From the home directory: `~/.claude/CLAUDE.md`.
    pub path: String,
    /// `instructions`, `import`, `memory` or `skill`.
    pub kind: String,
    pub agent: String,
    pub sha256: String,
    pub size: u64,
    /// Unix seconds.
    pub mtime: u64,
    /// Inside a git repository: change it through an agent (D8).
    pub git: bool,
    /// A link (a file Home Manager makes): where it points. Listed, never
    /// written: change its source (nixos-config) through an agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
    /// Absent when `too_big`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub too_big: bool,
}

/// `AgentConfig`'s JSON. `machine` is filled in by the asker.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub machine: String,
    pub files: Vec<File>,
    /// Why this machine has no list (over the local socket only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn home(env: &Env) -> anyhow::Result<PathBuf> {
    env.get("HOME").map(PathBuf::from).or_else(dirs::home_dir).context("no home directory")
}

/// Claude Code's folder name for a working directory.
fn project_slug(dir: &Path) -> String {
    dir.display().to_string().chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '-' }).collect()
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect()
}

/// `~/x` for a path in `home`.
fn tilde(home: &Path, path: &Path) -> String {
    match path.strip_prefix(home) {
        Ok(rest) => format!("~/{}", rest.display()),
        Err(_) => path.display().to_string(),
    }
}

/// A folder above `path`, below `home`, has `.git`.
fn in_git(home: &Path, path: &Path) -> bool {
    path.ancestors().skip(1).take_while(|d| d.starts_with(home) && *d != home).any(|d| d.join(".git").exists())
}

/// The `@path` imports in a `CLAUDE.md`, as files that exist in `home`.
fn imports(home: &Path, file: &Path, text: &str) -> Vec<PathBuf> {
    let dir = file.parent().unwrap_or(home);
    let mut in_code = false;
    let mut found = Vec::new();
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            in_code = !in_code;
        }
        if in_code {
            continue;
        }
        for word in line.split_whitespace() {
            let Some(target) = word.strip_prefix('@') else { continue };
            let target = target.trim_end_matches(['.', ',', ';', ')']);
            if target.is_empty() || target.contains('`') {
                continue;
            }
            let path = match target.strip_prefix("~/") {
                Some(rest) => home.join(rest),
                None => dir.join(target),
            };
            if listable(home, &path) && path.is_file() {
                found.push(path);
            }
        }
    }
    found
}

/// An `@` import that may be listed (and so written): a Markdown file in
/// home, not in a hidden folder other than an agent's own, not settings,
/// not in a hooks folder. Anything else a `CLAUDE.md` names
/// (`@~/.claude/settings.json`, `@~/.ssh/id_ed25519`) stays off the list,
/// so `ConfigWrite` can never reach it through an import.
fn instruction_shaped(home: &Path, path: &Path) -> bool {
    let Ok(rel) = path.strip_prefix(home) else { return false };
    let md = path.extension().is_some_and(|x| x.eq_ignore_ascii_case("md"));
    let settings = path.file_name().is_some_and(|n| n.to_string_lossy().to_ascii_lowercase().starts_with("settings"));
    let folders: Vec<String> =
        rel.parent().into_iter().flat_map(|p| p.components()).map(|c| c.as_os_str().to_string_lossy().into_owned()).collect();
    let hidden =
        folders.iter().enumerate().any(|(i, f)| f.starts_with('.') && !(i == 0 && [".claude", ".codex", ".gemini"].contains(&f.as_str())));
    let hooks = folders.iter().any(|f| f == "hooks");
    md && !settings && !hidden && !hooks
}

/// `path` has no `..` and is a regular file or folder in `home`, not a
/// link.
fn safe_in(home: &Path, path: &Path) -> bool {
    if path.components().any(|c| matches!(c, Component::ParentDir)) || !path.starts_with(home) {
        return false;
    }
    let mut at = home.to_path_buf();
    for part in path.strip_prefix(home).map(|p| p.components().collect::<Vec<_>>()).unwrap_or_default() {
        at.push(part);
        match at.symlink_metadata() {
            Ok(m) if m.file_type().is_symlink() => return false,
            Ok(_) => {}
            Err(_) => break,
        }
    }
    true
}

/// Where `path` links to, if it is a link that may be listed (not
/// written): to the Nix store (a file Home Manager makes) or into `home`,
/// in a folder with no links.
fn listed_link(home: &Path, path: &Path) -> Option<PathBuf> {
    if !path.symlink_metadata().ok()?.file_type().is_symlink() || !safe_in(home, path.parent()?) {
        return None;
    }
    let target = path.canonicalize().ok()?;
    let home = home.canonicalize().ok()?;
    (target.starts_with("/nix/store/") || target.starts_with(&home)).then_some(target)
}

/// May be listed: a plain file in `home`, or a link `listed_link` takes.
fn listable(home: &Path, path: &Path) -> bool {
    safe_in(home, path) || listed_link(home, path).is_some()
}

/// Every allow-listed file that exists: (path, kind, agent).
fn allow_list(home: &Path) -> Vec<(PathBuf, &'static str, &'static str)> {
    let mut list = Vec::new();
    let claude = home.join(".claude");
    // CLAUDE.md and its imports.
    let main = claude.join("CLAUDE.md");
    if main.is_file() {
        list.push((main.clone(), "instructions", "claude"));
        let mut seen = BTreeSet::from([main.clone()]);
        let mut todo = vec![(main, 0)];
        while let Some((file, depth)) = todo.pop() {
            let Ok(text) = std::fs::read_to_string(&file) else { continue };
            for import in imports(home, &file, &text) {
                if !instruction_shaped(home, &import) {
                    debug!("agent instructions: {} imports {}: not listed", file.display(), import.display());
                    continue;
                }
                if depth < IMPORT_DEPTH && seen.insert(import.clone()) {
                    list.push((import.clone(), "import", "claude"));
                    todo.push((import, depth + 1));
                }
            }
        }
    }
    // Memory of every project.
    for project in std::fs::read_dir(claude.join("projects")).into_iter().flatten().flatten() {
        let memory = project.path().join("memory");
        let mut files: Vec<PathBuf> = std::fs::read_dir(&memory)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "md") && p.is_file())
            .collect();
        files.sort();
        list.extend(files.into_iter().map(|p| (p, "memory", "claude")));
    }
    // Skills.
    let mut dirs = vec![(claude.join("skills"), 0)];
    let mut skills = Vec::new();
    while let Some((dir, depth)) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let path = entry.path();
            let name = entry.file_name();
            if name.to_string_lossy().starts_with('.') {
                continue;
            }
            match entry.file_type() {
                Ok(t) if t.is_dir() && depth < SKILL_DEPTH => dirs.push((path, depth + 1)),
                Ok(t) if t.is_file() && name == "SKILL.md" => skills.push(path),
                _ => {}
            }
        }
    }
    skills.sort();
    list.extend(skills.into_iter().map(|p| (p, "skill", "claude")));
    for (agent, rel) in OTHER_AGENTS {
        let path = home.join(rel);
        if path.is_file() {
            list.push((path, "instructions", agent));
        }
    }
    list.retain(|(p, ..)| listable(home, p));
    list
}

/// This machine's allow-listed files.
pub fn snapshot(home: &Path) -> Snapshot {
    let own_project = format!("~/.claude/projects/{}/", project_slug(home));
    let files = allow_list(home)
        .into_iter()
        .filter_map(|(path, kind, agent)| {
            let meta = std::fs::metadata(&path).ok()?;
            let bytes = if meta.len() <= MAX_CONTENT { Some(std::fs::read(&path).ok()?) } else { None };
            let sha = match &bytes {
                Some(b) => sha256_hex(b),
                None => sha256_hex(&std::fs::read(&path).ok()?),
            };
            let shown = tilde(home, &path);
            let id = match shown.strip_prefix(&own_project) {
                Some(rest) => format!("~/.claude/projects/<home>/{rest}"),
                None => shown.clone(),
            };
            Some(File {
                id,
                path: shown,
                kind: kind.into(),
                agent: agent.into(),
                sha256: sha,
                size: meta.len(),
                mtime: meta.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map_or(0, |d| d.as_secs()),
                git: in_git(home, &path),
                link: listed_link(home, &path).map(|t| t.display().to_string()),
                content: bytes.map(|b| String::from_utf8_lossy(&b).into_owned()),
                too_big: meta.len() > MAX_CONTENT,
            })
        })
        .collect();
    Snapshot { files, ..Default::default() }
}

/// `AgentConfig`: `AgentConfig { json }`.
pub async fn info(env: &Env) -> Response {
    let made = async {
        let home = home(env)?;
        let snapshot = tokio::task::spawn_blocking(move || snapshot(&home)).await?;
        anyhow::Ok(serde_json::to_string(&snapshot)?)
    };
    match made.await {
        Ok(json) => Response::AgentConfig { json },
        Err(e) => Response::Error(format!("{e:#}")),
    }
}

/// Where `path` (`~/…` or absolute) is, if `ConfigWrite` may write it.
fn writable(home: &Path, path: &str) -> anyhow::Result<PathBuf> {
    let full = match path.strip_prefix("~/") {
        Some(rest) => home.join(rest),
        None => PathBuf::from(path),
    };
    if !safe_in(home, &full) {
        bail!("{path} is not a plain file in the home directory");
    }
    let listed = allow_list(home).into_iter().any(|(p, ..)| p == full);
    // A new file may be: a memory file of a project, or an agent's
    // instruction file that does not exist yet.
    let new_memory = full.extension().is_some_and(|x| x == "md")
        && full.parent().is_some_and(|m| m.file_name().is_some_and(|n| n == "memory"))
        && full.parent().and_then(Path::parent).and_then(Path::parent).map(Path::to_path_buf) == Some(home.join(".claude/projects"));
    let new_instructions = full == home.join(".claude/CLAUDE.md") || OTHER_AGENTS.iter().any(|(_, rel)| full == home.join(rel));
    if !listed && !((new_memory || new_instructions) && !full.exists()) {
        bail!("{path} is not an agent instruction file etd may change (PLAN-logbook-v2 D9)");
    }
    if in_git(home, &full) {
        bail!("{path} is in a git repository: change it through an agent, which commits it (PLAN-logbook-v2 D8)");
    }
    Ok(full)
}

/// Replaces `path` with `content` if its sha256 is still `base` (empty: it
/// must not exist; empty `content`: remove it). The old file goes to
/// `backups/<time>/` first: returns that copy's path, or an empty string
/// for a new file.
fn write_file(home: &Path, backups: &Path, path: &str, base: &str, content: &str, now: u64) -> anyhow::Result<String> {
    if content.len() > MAX_WRITE {
        bail!("{} bytes is too large for an instruction file (at most {MAX_WRITE})", content.len());
    }
    if let Some(secret) = SECRETS.iter().find(|s| content.contains(*s)) {
        bail!("the new content looks like it holds a secret ({secret}…): not written");
    }
    let full = writable(home, path)?;
    // An instruction file that would import a file no agent should read
    // (a key, settings): Claude Code would load it into every session.
    if let Some(bad) = imports(home, &full, content).into_iter().find(|p| !instruction_shaped(home, p)) {
        bail!("the new content imports {}, which is not an agent instruction file: not written", tilde(home, &bad));
    }
    // One write at a time: the hash check, the backup and the rename of two
    // writes of one file must not interleave.
    static WRITING: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _writing = WRITING.lock().unwrap_or_else(|e| e.into_inner());
    let old = match std::fs::read(&full) {
        Ok(old) => Some(old),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e).with_context(|| format!("read {path}")),
    };
    match (&old, base) {
        (None, "") if content.is_empty() => bail!("{path} does not exist: nothing to remove"),
        (None, "") => {}
        (None, _) => bail!("{path} does not exist any more"),
        (Some(_), "") => bail!("{path} exists now: read it again"),
        (Some(old), base) if sha256_hex(old) != base => bail!("{path} changed since it was read: read it again"),
        _ => {}
    }
    let mode = std::fs::metadata(&full).map(|m| m.permissions().mode() & 0o777).unwrap_or(0o644);
    let mut backup = String::new();
    if let Some(old) = &old {
        let rel = full.strip_prefix(home)?;
        let mut dir = backups.join(now.to_string());
        let mut n = 1;
        while dir.join(rel).exists() {
            n += 1;
            dir = backups.join(format!("{now}-{n}"));
        }
        let copy = dir.join(rel);
        std::fs::create_dir_all(copy.parent().context("backup folder")?)?;
        std::fs::write(&copy, old)?;
        std::fs::set_permissions(&copy, std::fs::Permissions::from_mode(0o600))?;
        backup = copy.display().to_string();
    }
    // Empty content removes the file (the undo of a new file).
    if content.is_empty() {
        std::fs::remove_file(&full)?;
        return Ok(backup);
    }
    let parent = full.parent().context("no folder")?;
    std::fs::create_dir_all(parent)?;
    // A new file of its own (never through a link, never another write's).
    let tmp = parent.join(format!(".{}.{}-{now}.etd-tmp", full.file_name().context("no file name")?.to_string_lossy(), std::process::id()));
    let written = (|| {
        let mut file = std::fs::OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        std::io::Write::write_all(&mut file, content.as_bytes())?;
        file.set_permissions(std::fs::Permissions::from_mode(mode))?;
        std::fs::rename(&tmp, &full)
    })();
    if let Err(e) = written {
        let _ = std::fs::remove_file(&tmp);
        return Err(e).with_context(|| format!("write {path}"));
    }
    Ok(backup)
}

fn backups(paths: &Paths) -> PathBuf {
    paths.data.join("config-backups")
}

/// `ConfigWrite`: `ConfigWritten { backup }`.
pub async fn write(env: &Env, paths: &Paths, path: &str, base: &str, content: &str) -> Response {
    let written = home(env).and_then(|home| write_file(&home, &backups(paths), path, base, content, config::now_secs()));
    match written {
        Ok(backup) => {
            info!("agent instructions: {path} written (backup: {})", if backup.is_empty() { "none, a new file" } else { &backup });
            Response::ConfigWritten { backup }
        }
        Err(e) => Response::Error(format!("{e:#}")),
    }
}

/// `AgentConfig` over the local socket: this machine's and every other
/// machine's (one that does not answer has an `error`), in the network's
/// order.
pub async fn everywhere(endpoint: &Endpoint, paths: &Paths, connections: &Connections) -> Vec<Snapshot> {
    everywhere_of(endpoint, paths, connections, None, std::time::Duration::from_secs(15)).await
}

/// `everywhere`, for the machines in `only` (all: `None`), each with `limit`.
pub async fn everywhere_of(
    endpoint: &Endpoint,
    paths: &Paths,
    connections: &Connections,
    only: Option<&BTreeSet<String>>,
    limit: std::time::Duration,
) -> Vec<Snapshot> {
    let me = endpoint.id();
    let Ok(Some(signed)) = network::load(paths) else { return Vec::new() };
    // All at once: a machine that is off costs its time limit once.
    let wanted = |name: &str| only.is_none_or(|o| o.contains(name));
    let asks = signed.network.machines().filter(|m| wanted(m.name)).map(|m| async move {
        let mut snapshot = if m.id == me {
            match home(&Env::new()) {
                Ok(home) => tokio::task::spawn_blocking(move || snapshot(&home)).await.unwrap_or_default(),
                Err(e) => Snapshot { error: Some(format!("{e:#}")), ..Default::default() },
            }
        } else {
            let asked = async {
                let conn = connections.to(endpoint, m.id).await?;
                let (mut up, mut down) = conn.open_bi().await?;
                write_frame(&mut up, &Request::AgentConfig).await?;
                up.finish()?;
                read_frame::<Response>(&mut down).await?.context("no answer")
            };
            match tokio::time::timeout(limit, asked).await {
                Ok(Ok(Response::AgentConfig { json })) => serde_json::from_str(&json)
                    .unwrap_or_else(|e| Snapshot { error: Some(format!("bad answer: {e}")), ..Default::default() }),
                Ok(Ok(Response::Error(e))) => Snapshot { error: Some(e), ..Default::default() },
                other => {
                    debug!("no agent config from {}: {other:?}", m.name);
                    let why = match other {
                        Err(_) => format!("no answer in {} s", limit.as_secs()),
                        Ok(Err(e)) => format!("{e:#}"),
                        Ok(Ok(r)) => format!("unexpected answer {r:?}"),
                    };
                    Snapshot { error: Some(why), ..Default::default() }
                }
            }
        };
        snapshot.machine = m.name.to_string();
        snapshot
    });
    n0_future::join_all(asks).await
}

/// One file that is not the same on every machine that answered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Drift {
    pub id: String,
    pub kind: String,
    /// Machine → sha256, `None`: the machine does not have it.
    pub machines: BTreeMap<String, Option<String>>,
}

/// The files that differ, or that some machines do not have.
pub fn drift(snapshots: &[Snapshot]) -> Vec<Drift> {
    let answered: Vec<&Snapshot> = snapshots.iter().filter(|s| s.error.is_none()).collect();
    let mut by_id: BTreeMap<&str, (&str, BTreeMap<String, Option<String>>)> = BTreeMap::new();
    for s in &answered {
        for f in &s.files {
            by_id.entry(&f.id).or_insert_with(|| (&f.kind, BTreeMap::new())).1.insert(s.machine.clone(), Some(f.sha256.clone()));
        }
    }
    by_id
        .into_iter()
        .filter_map(|(id, (kind, mut machines))| {
            for s in &answered {
                machines.entry(s.machine.clone()).or_insert(None);
            }
            let shas: BTreeSet<&Option<String>> = machines.values().collect();
            (shas.len() > 1).then(|| Drift { id: id.into(), kind: kind.into(), machines })
        })
        .collect()
}

/// `etd meta drift`: the files that differ between machines.
pub async fn drift_cli(paths: &Paths) -> anyhow::Result<()> {
    let socket = crate::local::socket_path(paths);
    let mut stream =
        tokio::net::UnixStream::connect(&socket).await.with_context(|| format!("etd is not running here ({})", socket.display()))?;
    crate::local::send(&mut stream, &Request::AgentConfig).await?;
    let json = match crate::local::receive::<_, Response>(&mut stream).await?.context("etd closed the connection")? {
        Response::AgentConfig { json } => json,
        Response::Error(e) => bail!("{e}"),
        other => bail!("unexpected answer: {other:?}"),
    };
    let snapshots: Vec<Snapshot> = serde_json::from_str::<Everywhere>(&json)?.machines;
    for s in &snapshots {
        match &s.error {
            Some(e) => println!("{:<12} no answer: {e}", s.machine),
            None => {
                let count = |kind: &str| s.files.iter().filter(|f| f.kind == kind).count();
                println!(
                    "{:<12} {} instruction file(s), {} import(s), {} memory, {} skill(s)",
                    s.machine,
                    count("instructions"),
                    count("import"),
                    count("memory"),
                    count("skill")
                );
            }
        }
    }
    let drift = drift(&snapshots);
    if drift.is_empty() {
        println!("no differences");
        return Ok(());
    }
    println!("\n{} file(s) differ:", drift.len());
    for d in &drift {
        let mut groups: BTreeMap<String, Vec<&str>> = BTreeMap::new();
        for (machine, sha) in &d.machines {
            let label = sha.as_ref().map_or_else(|| "missing".to_string(), |s| s[..8].to_string());
            groups.entry(label).or_default().push(machine);
        }
        let parts: Vec<String> = groups.iter().map(|(label, ms)| format!("{} {label}", ms.join("+"))).collect();
        println!("  {:<60} {}", d.id, parts.join(" · "));
    }
    Ok(())
}

/// `AgentConfig`'s JSON over the local socket.
#[derive(Debug, Serialize, Deserialize)]
pub struct Everywhere {
    pub machines: Vec<Snapshot>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(path: &Path, text: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    /// A home with every kind of file, and files that must not be listed.
    fn home_dir() -> tempfile::TempDir {
        let home = tempfile::tempdir().unwrap();
        let h = home.path();
        put(&h.join(".claude/CLAUDE.md"), "Rules.\n@adhd-rules.md\n```\n@not-this.md\n```\nSee @~/notes/extra.md.\n");
        put(&h.join(".claude/adhd-rules.md"), "Be brief. @nested.md");
        put(&h.join(".claude/nested.md"), "x");
        put(&h.join(".claude/not-this.md"), "x");
        put(&h.join("notes/extra.md"), "x");
        put(&h.join(format!(".claude/projects/{}/memory/MEMORY.md", project_slug(h))), "- [a](a.md)");
        put(&h.join(".claude/projects/-srv-app/memory/a.md"), "fact");
        put(&h.join(".claude/skills/garage/SKILL.md"), "skill");
        std::fs::create_dir_all(h.join(".claude/skills/.git")).unwrap();
        put(&h.join(".claude/settings.json"), "{}");
        put(&h.join(".codex/AGENTS.md"), "codex rules");
        home
    }

    #[test]
    fn lists_instructions_imports_memory_skills_not_settings() {
        let home = home_dir();
        let s = snapshot(home.path());
        let got: Vec<(&str, &str, &str, bool)> =
            s.files.iter().map(|f| (f.id.as_str(), f.kind.as_str(), f.agent.as_str(), f.git)).collect();
        let mut want = vec![
            ("~/.claude/CLAUDE.md", "instructions", "claude", false),
            ("~/.claude/adhd-rules.md", "import", "claude", false),
            ("~/notes/extra.md", "import", "claude", false),
            ("~/.claude/nested.md", "import", "claude", false),
            ("~/.claude/projects/-srv-app/memory/a.md", "memory", "claude", false),
            ("~/.claude/projects/<home>/memory/MEMORY.md", "memory", "claude", false),
            ("~/.claude/skills/garage/SKILL.md", "skill", "claude", true),
            ("~/.codex/AGENTS.md", "instructions", "codex", false),
        ];
        let sort = |v: &mut Vec<(&str, &str, &str, bool)>| v.sort();
        let mut got = got;
        sort(&mut got);
        sort(&mut want);
        assert_eq!(got, want);
        let main = s.files.iter().find(|f| f.kind == "instructions" && f.agent == "claude").unwrap();
        assert_eq!(main.sha256, sha256_hex(main.content.as_ref().unwrap().as_bytes()));
    }

    #[test]
    fn writes_with_the_base_and_keeps_a_backup() {
        let home = home_dir();
        let h = home.path();
        let backups = h.join("backups");
        let base = sha256_hex(&std::fs::read(h.join(".claude/CLAUDE.md")).unwrap());
        let backup = write_file(h, &backups, "~/.claude/CLAUDE.md", &base, "New rules.\n", 1000).unwrap();
        assert_eq!(std::fs::read_to_string(h.join(".claude/CLAUDE.md")).unwrap(), "New rules.\n");
        assert!(std::fs::read_to_string(&backup).unwrap().starts_with("Rules."));
        assert_eq!(PathBuf::from(&backup), backups.join("1000/.claude/CLAUDE.md"));
        // The same base again: the file changed since.
        let err = write_file(h, &backups, "~/.claude/CLAUDE.md", &base, "x", 1001).unwrap_err();
        assert!(err.to_string().contains("changed since"), "{err}");
        // A new memory file: empty base, no backup.
        let new = format!("~/.claude/projects/{}/memory/new.md", project_slug(h));
        assert_eq!(write_file(h, &backups, &new, "", "a fact\n", 1002).unwrap(), "");
        let err = write_file(h, &backups, &new, "", "again", 1003).unwrap_err();
        assert!(err.to_string().contains("exists now"), "{err}");
        // Undo of the new file: empty content removes it, with a backup.
        let full = h.join(new.strip_prefix("~/").unwrap());
        let backup = write_file(h, &backups, &new, &sha256_hex(b"a fact\n"), "", 1004).unwrap();
        assert!(!full.exists());
        assert_eq!(std::fs::read_to_string(backup).unwrap(), "a fact\n");
        let err = write_file(h, &backups, &new, "", "", 1005).unwrap_err();
        assert!(err.to_string().contains("nothing to remove"), "{err}");
    }

    #[test]
    fn refuses_what_is_not_an_instruction_file() {
        let home = home_dir();
        let h = home.path();
        let b = h.join("backups");
        let sha = |rel: &str| sha256_hex(&std::fs::read(h.join(rel)).unwrap());
        for (path, base, why) in [
            ("~/.claude/settings.json", sha(".claude/settings.json"), "not an agent instruction file"),
            ("~/.bashrc", String::new(), "not an agent instruction file"),
            ("~/.claude/skills/garage/SKILL.md", sha(".claude/skills/garage/SKILL.md"), "git repository"),
            ("~/.claude/../.ssh/authorized_keys", String::new(), "not a plain file"),
            ("/etc/passwd", String::new(), "not a plain file"),
            ("~/.claude/not-this.md", sha(".claude/not-this.md"), "not an agent instruction file"),
        ] {
            let err = write_file(h, &b, path, &base, "x", 1).unwrap_err().to_string();
            assert!(err.contains(why), "{path}: {err}");
        }
        let base = sha(".claude/CLAUDE.md");
        let err = write_file(h, &b, "~/.claude/CLAUDE.md", &base, "token sk-ant-abc", 1).unwrap_err().to_string();
        assert!(err.contains("secret"), "{err}");
        // A link to a file elsewhere is not followed.
        let outside = tempfile::tempdir().unwrap();
        put(&outside.path().join("target.md"), "x");
        std::fs::remove_file(h.join(".codex/AGENTS.md")).unwrap();
        std::os::unix::fs::symlink(outside.path().join("target.md"), h.join(".codex/AGENTS.md")).unwrap();
        let err = write_file(h, &b, "~/.codex/AGENTS.md", "", "x", 1).unwrap_err().to_string();
        assert!(err.contains("not a plain file"), "{err}");
        assert_eq!(std::fs::read_to_string(outside.path().join("target.md")).unwrap(), "x");
    }

    /// An `@` import of settings, a key or a hook is not listed, so it can
    /// not be written; new content that imports one is refused.
    #[test]
    fn imports_of_settings_or_keys_are_not_listed_or_written() {
        let home = home_dir();
        let h = home.path();
        put(&h.join(".ssh/id_ed25519"), "key");
        put(&h.join(".claude/hooks/pre.md"), "hook");
        let main = "@~/.claude/settings.json @~/.ssh/id_ed25519 @hooks/pre.md @adhd-rules.md\n";
        put(&h.join(".claude/CLAUDE.md"), main);
        let s = snapshot(h);
        let paths: Vec<&str> = s.files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"~/.claude/adhd-rules.md"), "{paths:?}");
        for never in ["~/.claude/settings.json", "~/.ssh/id_ed25519", "~/.claude/hooks/pre.md"] {
            assert!(!paths.contains(&never), "{never} listed: {paths:?}");
            let err = write_file(h, &h.join("backups"), never, &sha256_hex(b"{}"), "x", 1).unwrap_err().to_string();
            assert!(err.contains("may change") || err.contains("changed"), "{never}: {err}");
        }
        assert_eq!(std::fs::read_to_string(h.join(".claude/settings.json")).unwrap(), "{}");
        let base = sha256_hex(main.as_bytes());
        let err = write_file(h, &h.join("backups"), "~/.claude/CLAUDE.md", &base, "@~/.ssh/id_ed25519\n", 1).unwrap_err().to_string();
        assert!(err.contains("imports ~/.ssh/id_ed25519"), "{err}");
        write_file(h, &h.join("backups"), "~/.claude/CLAUDE.md", &base, "@adhd-rules.md\nBe brief.\n", 1).unwrap();
    }

    #[test]
    fn a_home_manager_link_is_listed_but_not_written() {
        let home = home_dir();
        let h = home.path();
        put(&h.join(".claude/CLAUDE.md"), "@rules.md\n");
        // As Home Manager does it, with a folder in the home for the test.
        put(&h.join("store/rules.md"), "Be brief.");
        std::os::unix::fs::symlink(h.join("store/rules.md"), h.join(".claude/rules.md")).unwrap();
        let s = snapshot(h);
        let rules = s.files.iter().find(|f| f.path == "~/.claude/rules.md").expect("the import is listed");
        assert_eq!((rules.kind.as_str(), rules.content.as_deref()), ("import", Some("Be brief.")));
        assert_eq!(rules.link.as_deref(), Some(&*h.canonicalize().unwrap().join("store/rules.md").display().to_string()));
        let err = write_file(h, &h.join("backups"), "~/.claude/rules.md", &rules.sha256, "x", 1).unwrap_err().to_string();
        assert!(err.contains("not a plain file"), "{err}");
    }

    #[test]
    fn drift_names_files_that_differ_or_are_missing() {
        let file = |id: &str, sha: &str| File {
            id: id.into(),
            path: id.into(),
            kind: "memory".into(),
            agent: "claude".into(),
            sha256: sha.into(),
            size: 1,
            mtime: 0,
            git: false,
            content: None,
            link: None,
            too_big: false,
        };
        let snap = |machine: &str, files: Vec<File>| Snapshot { machine: machine.into(), files, error: None };
        let snapshots = vec![
            snap("trigkey", vec![file("a", "1"), file("b", "2"), file("c", "3")]),
            snap("mac-mini", vec![file("a", "1"), file("b", "9")]),
            Snapshot { machine: "gmktec".into(), error: Some("off".into()), ..Default::default() },
        ];
        let d = drift(&snapshots);
        let ids: Vec<&str> = d.iter().map(|d| d.id.as_str()).collect();
        assert_eq!(ids, ["b", "c"], "a is the same; gmktec did not answer");
        assert_eq!(d[1].machines["mac-mini"], None);
    }
}
