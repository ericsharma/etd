//! Finding tmux and its server, with the user's login environment.
//!
//! A service manager starts `etd` with a small environment, so tmux and the
//! socket directory are looked up with the login shell's environment, the
//! same search E.T. runs over SSH (et-ios SPEC.md §2.1).
use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context};
use et_proto::{SessionInfo, TmuxInstall, TmuxStatus};
use tokio::process::Command;
use tracing::warn;

pub type Env = HashMap<String, String>;

const LOGIN_ENV_TIMEOUT: Duration = Duration::from_secs(5);
const ENV_MARKER: &str = "__ETD_ENV__";

/// The login shell's environment, else this process's own.
pub async fn login_env() -> Env {
    let own: Env = std::env::vars().collect();
    let shell = own.get("SHELL").cloned().unwrap_or_else(|| "/bin/sh".into());
    // The marker skips anything a profile prints before `env` runs.
    let script = format!("printf '\\n{ENV_MARKER}\\n'; env -0");
    let run = Command::new(&shell).args(["-l", "-c", &script]).stdin(Stdio::null()).stderr(Stdio::null()).output();
    let output = match tokio::time::timeout(LOGIN_ENV_TIMEOUT, run).await {
        Ok(Ok(output)) if output.status.success() => output.stdout,
        Ok(Ok(output)) => {
            warn!("{shell} -l exited with {}; using etd's own environment", output.status);
            return own;
        }
        Ok(Err(e)) => {
            warn!("{shell} -l failed: {e}; using etd's own environment");
            return own;
        }
        Err(_) => {
            warn!("{shell} -l took over {LOGIN_ENV_TIMEOUT:?}; using etd's own environment");
            return own;
        }
    };
    let text = String::from_utf8_lossy(&output);
    let Some((_, vars)) = text.split_once(&format!("\n{ENV_MARKER}\n")) else { return own };
    let env: Env = vars.split('\0').filter_map(|kv| kv.split_once('=')).map(|(k, v)| (k.into(), v.into())).collect();
    if env.contains_key("PATH") { env } else { own }
}

fn uid() -> u32 {
    // SAFETY: getuid has no preconditions and cannot fail.
    unsafe { libc::getuid() }
}

pub fn host_name() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: the buffer is valid for its length; the result is NUL-terminated on success.
    if unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) } != 0 {
        return String::new();
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

fn candidates(env: &Env) -> Vec<String> {
    let home = env.get("HOME").cloned().unwrap_or_default();
    let user = env.get("USER").cloned().unwrap_or_default();
    let mut list: Vec<String> = env
        .get("PATH")
        .map(|p| p.split(':').filter(|d| !d.is_empty()).map(|d| format!("{d}/tmux")).collect())
        .unwrap_or_default();
    list.extend([
        "/opt/homebrew/bin/tmux".into(),
        "/usr/local/bin/tmux".into(),
        "/usr/bin/tmux".into(),
        "/run/current-system/sw/bin/tmux".into(),
        format!("/etc/profiles/per-user/{user}/bin/tmux"),
        format!("{home}/.nix-profile/bin/tmux"),
        "/snap/bin/tmux".into(),
    ]);
    list
}

fn is_executable(path: &str) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0).unwrap_or(false)
}

/// A tmux command with the login environment and the found socket directory.
pub fn command(env: &Env, tmux: &TmuxInstall) -> Command {
    let mut cmd = Command::new(&tmux.path);
    // $TMUX wins over TMUX_TMPDIR. If etd starts inside a tmux pane, an
    // inherited $TMUX would send every command to that pane's server.
    cmd.env_clear().envs(env).env_remove("TMUX").env_remove("TMUX_PANE").stdin(Stdio::null());
    match &tmux.tmpdir {
        Some(dir) => cmd.env("TMUX_TMPDIR", dir),
        None => cmd.env_remove("TMUX_TMPDIR"),
    };
    cmd
}

/// Finds tmux, and the socket directory where its server answers. With no
/// server running, the login `TMUX_TMPDIR` (where a new server should go).
pub async fn find(env: &Env) -> TmuxStatus {
    let mut tried = Vec::new();
    let Some(path) = candidates(env).into_iter().find(|p| {
        tried.push(p.clone());
        is_executable(p)
    }) else {
        return TmuxStatus::NotFound(format!("no executable tmux in:\n{}", tried.join("\n")));
    };
    let version = match Command::new(&path).arg("-V").env_clear().envs(env).output().await {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        Ok(o) => return TmuxStatus::NotFound(format!("{path} -V: {}", o.status)),
        Err(e) => return TmuxStatus::NotFound(format!("{path} -V: {e}")),
    };
    let uid = uid();
    let login_tmpdir = env.get("TMUX_TMPDIR").cloned();
    let dirs = [
        login_tmpdir.clone(),
        std::env::var("TMUX_TMPDIR").ok(),
        env.get("XDG_RUNTIME_DIR").cloned(),
        Some(format!("/run/user/{uid}")),
        env.get("TMPDIR").map(|d| d.trim_end_matches('/').to_string()),
        Some("/tmp".into()),
    ];
    for dir in dirs.into_iter().flatten().filter(|d| !d.is_empty()) {
        if !Path::new(&format!("{dir}/tmux-{uid}/default")).exists() {
            continue;
        }
        let install = TmuxInstall { path: path.clone(), version: version.clone(), tmpdir: Some(dir) };
        let ok = command(env, &install).arg("list-sessions").stdout(Stdio::null()).stderr(Stdio::null()).status().await;
        if ok.map(|s| s.success()).unwrap_or(false) {
            return TmuxStatus::Found(install);
        }
    }
    TmuxStatus::Found(TmuxInstall { path, version, tmpdir: login_tmpdir.filter(|d| !d.is_empty()) })
}

/// Sessions on the server. Empty when no server is running.
pub async fn list_sessions(env: &Env, tmux: &TmuxInstall) -> anyhow::Result<Vec<SessionInfo>> {
    let format = "#{session_id} #{session_windows} #{session_attached} #{session_name}";
    let output = command(env, tmux).args(["list-sessions", "-F", format]).output().await.context("run tmux")?;
    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        if ["no server running", "error connecting to", "No such file or directory"].iter().any(|s| err.contains(s)) {
            return Ok(Vec::new());
        }
        bail!("tmux list-sessions: {}: {}", output.status, err.trim());
    }
    Ok(parse_sessions(&String::from_utf8_lossy(&output.stdout)))
}

fn parse_sessions(text: &str) -> Vec<SessionInfo> {
    text.lines()
        .filter_map(|line| {
            let mut f = line.splitn(4, ' ');
            let id = f.next()?;
            let windows = f.next()?.parse().ok()?;
            let attached = f.next()?.parse().ok()?;
            let name = f.next()?;
            id.starts_with('$').then(|| SessionInfo { id: id.into(), name: name.into(), windows, attached })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sessions_keep_spaces_in_names() {
        let s = parse_sessions("$0 6 1 0\n$3 1 0 my work\nbad line\n");
        assert_eq!(s.len(), 2);
        assert_eq!(s[1].name, "my work");
        assert_eq!(s[1].windows, 1);
    }
}
