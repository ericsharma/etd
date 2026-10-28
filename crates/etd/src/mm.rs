//! manymux on this machine: `mm hosts`, `mm agent`, manymux's machine list,
//! and typing into a session (`type_into`). For the app, etd only starts
//! `mm agent` and moves the bytes. Other machines are reached through the
//! network (local.rs).
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context};
use manymux::proto::{self as mp, tag, FrameReader};
use tokio::process::{ChildStdin, Command};

use crate::tmux::Env;

fn command(env: &Env, program: &str) -> Command {
    let mut cmd = Command::new(program);
    cmd.env_clear().envs(env);
    cmd
}

/// `mm hosts`: the machines the node here watches (`mm add <host>`).
pub async fn machines(env: &Env) -> anyhow::Result<Vec<String>> {
    let output = command(env, "mm")
        .arg("hosts")
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .context("run `mm hosts` (is manymux installed?)")?;
    if !output.status.success() {
        bail!("mm hosts: {}: {}", output.status, String::from_utf8_lossy(&output.stderr).trim());
    }
    Ok(String::from_utf8_lossy(&output.stdout).lines().map(str::trim).filter(|l| !l.is_empty()).map(String::from).collect())
}

/// Where this machine's node listens: manymux's own path, the same from every
/// login (manymux `config::runtime_dir`). `ETD_MM_SOCKET` moves it, for tests
/// whose `mm` passes its own `--socket`.
pub fn node_socket(env: &Env) -> PathBuf {
    match env.get("ETD_MM_SOCKET") {
        Some(socket) => socket.into(),
        // SAFETY: getuid has no preconditions and cannot fail.
        None => format!("/tmp/manymux-{}/manymux.sock", unsafe { libc::getuid() }).into(),
    }
}

/// A node answers on this machine's socket. `mm agent` starts a node when
/// none does; etd asks this first, so the node stays the user's service's.
pub async fn node_running(env: &Env) -> bool {
    tokio::net::UnixStream::connect(node_socket(env)).await.is_ok()
}

/// `mm` is on the login environment's PATH.
pub fn available(env: &Env) -> bool {
    env.get("PATH").is_some_and(|path| std::env::split_paths(path).any(|d| d.join("mm").is_file()))
}

pub fn agent_local(env: &Env) -> Command {
    let mut cmd = command(env, "mm");
    cmd.arg("agent");
    cmd
}

/// Types `text` into the session `name` on this machine's node, as a
/// keyboard would. A session's size is the smallest of its attached
/// clients' sizes, so attaching at the size it has already changes nothing:
/// list it for its size, attach at that size, type, detach.
pub async fn type_into(env: &Env, name: &str, text: &str) -> anyhow::Result<()> {
    let (_list, mut stdin, mut reader) = agent(env)?;
    mp::write_msg(&mut stdin, tag::REQUEST, &mp::Request::List).await?;
    let size = match answer(&mut reader).await? {
        mp::Response::Sessions(list) => {
            list.into_iter().find(|s| s.name == name).map(|s| s.size).with_context(|| format!("no session named {name} here"))?
        }
        other => bail!("mm list: {other:?}"),
    };
    let (mut child, mut stdin, mut reader) = agent(env)?;
    let attach = mp::Request::Attach { name: name.to_string(), size, history: 0, read_only: false };
    mp::write_msg(&mut stdin, tag::REQUEST, &attach).await?;
    match answer(&mut reader).await? {
        mp::Response::Attached { .. } => {}
        other => bail!("attach {name}: {other:?}"),
    }
    mp::write_frame(&mut stdin, tag::DATA, text.as_bytes()).await?;
    mp::write_frame(&mut stdin, tag::DETACH, &[]).await?;
    drop(stdin);
    // The screen comes back while attached; nothing here needs it.
    let drain = async { while let Ok(Some(_)) = reader.next().await {} };
    let _ = tokio::time::timeout(Duration::from_secs(5), drain).await;
    let _ = child.kill().await;
    Ok(())
}

/// Lists this machine's sessions again and again over one `mm agent`
/// (the Logbook asks every 2 seconds).
pub struct Lister {
    _child: tokio::process::Child,
    stdin: ChildStdin,
    reader: FrameReader<tokio::process::ChildStdout>,
}

impl Lister {
    /// Never starts a node: see `events::follow_once`.
    pub async fn start(env: &Env) -> anyhow::Result<Self> {
        if !node_running(env).await {
            bail!("no manymux node yet");
        }
        let (child, stdin, reader) = agent(env)?;
        Ok(Self { _child: child, stdin, reader })
    }

    /// (name, leader pid) of each session.
    pub async fn sessions(&mut self) -> anyhow::Result<Vec<(String, u32)>> {
        mp::write_msg(&mut self.stdin, tag::REQUEST, &mp::Request::List).await?;
        match answer(&mut self.reader).await? {
            mp::Response::Sessions(list) => Ok(list.into_iter().map(|s| (s.name, s.pid)).collect()),
            other => bail!("mm list: {other:?}"),
        }
    }
}

/// A local `mm agent` with its pipes. The child dies when dropped.
fn agent(env: &Env) -> anyhow::Result<(tokio::process::Child, ChildStdin, FrameReader<tokio::process::ChildStdout>)> {
    let mut child = agent_local(env)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("start `mm agent`")?;
    let stdin = child.stdin.take().context("stdin")?;
    let reader = FrameReader::new(child.stdout.take().context("stdout")?);
    Ok((child, stdin, reader))
}

/// The node's answer to a request, skipping other frames.
async fn answer(reader: &mut FrameReader<tokio::process::ChildStdout>) -> anyhow::Result<mp::Response> {
    let read = async {
        loop {
            let frame = reader.next().await?.context("mm agent ended")?;
            if frame.tag == tag::RESPONSE {
                return match mp::decode::<mp::Response>(&frame.body)? {
                    mp::Response::Error(e) => Err(anyhow::anyhow!("{e}")),
                    response => Ok(response),
                };
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), read).await.context("mm agent did not answer")?
}

/// Paths as one line for a prompt: each shell-quoted when it needs it, a
/// space after each, no Enter.
pub fn typed_paths(paths: &[String]) -> String {
    let safe = |c: char| c.is_ascii_alphanumeric() || "/._-+~@%:,=".contains(c);
    paths
        .iter()
        .map(|p| if !p.is_empty() && p.chars().all(safe) { format!("{p} ") } else { format!("'{}' ", p.replace('\'', "'\\''")) })
        .collect()
}

/// manymux's machine list: `$MM_CONFIG_DIR/hosts.toml`, else
/// `<config dir>/manymux/hosts.toml`, as manymux finds it.
pub fn hosts_file(env: &Env) -> Option<std::path::PathBuf> {
    match env.get("MM_CONFIG_DIR").filter(|d| !d.is_empty()) {
        Some(dir) => Some(std::path::PathBuf::from(dir).join("hosts.toml")),
        None => Some(dirs::config_dir()?.join("manymux").join("hosts.toml")),
    }
}

/// The text of a `hosts.toml` listing `names`.
pub fn hosts_toml(names: &[String]) -> String {
    let mut text = String::from("hosts = [\n");
    for name in names {
        text.push_str(&format!("    \"{}\",\n", name.replace('\\', "\\\\").replace('"', "\\\"")));
    }
    text.push_str("]\n");
    text
}

/// Writes `names` as manymux's machine list, if it differs. The node watches
/// the file and starts following new machines (through `MM_SSH`). True if
/// it changed.
pub fn write_hosts(path: &std::path::Path, names: &[String]) -> anyhow::Result<bool> {
    let text = hosts_toml(names);
    if std::fs::read_to_string(path).is_ok_and(|old| old == text) {
        return Ok(false);
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, path)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_paths_quote_only_when_needed() {
        let paths = ["/h/etd-downloads/a.png".to_string(), "/h/etd-downloads/notes (2).md".to_string(), "/h/it's".to_string()];
        assert_eq!(typed_paths(&paths), "/h/etd-downloads/a.png '/h/etd-downloads/notes (2).md' '/h/it'\\''s' ");
    }

    #[test]
    fn hosts_file_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manymux/hosts.toml");
        let names = vec!["gmktec".to_string(), "mac-mini".to_string()];
        assert!(write_hosts(&path, &names).unwrap());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "hosts = [\n    \"gmktec\",\n    \"mac-mini\",\n]\n");
        assert!(!write_hosts(&path, &names).unwrap(), "unchanged");
        assert_eq!(hosts_toml(&[]), "hosts = [\n]\n");
        let env: Env = [("MM_CONFIG_DIR".to_string(), "/x".to_string())].into_iter().collect();
        assert_eq!(hosts_file(&env).unwrap(), std::path::Path::new("/x/hosts.toml"));
    }
}
