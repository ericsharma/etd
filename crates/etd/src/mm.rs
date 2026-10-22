//! manymux: the machines this machine's node watches, and the commands that
//! reach them. etd never reads the manymux protocol; it starts `mm agent`
//! (here or over ssh) and moves the bytes.
use anyhow::{bail, Context};
use tokio::process::Command;

use crate::tmux::Env;

/// No prompts (a prompt would hang the phone), a short connect, and a dead
/// connection noticed within a minute.
const SSH_OPTIONS: [&str; 8] =
    ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3"];

/// manymux answers one request per stream, so the app opens many short
/// `ssh <host> mm agent`s. One shared connection per machine (kept 2 minutes
/// after the last use) makes each of them a round trip, not a login. The
/// control socket needs a private runtime directory.
fn ssh_options(env: &Env) -> Vec<String> {
    let mut options: Vec<String> = SSH_OPTIONS.iter().map(|o| o.to_string()).collect();
    if let Some(dir) = env.get("XDG_RUNTIME_DIR").filter(|d| !d.is_empty()) {
        options.extend([
            "-o".into(),
            "ControlMaster=auto".into(),
            "-o".into(),
            format!("ControlPath={dir}/etd-ssh-%C"),
            "-o".into(),
            "ControlPersist=120".into(),
        ]);
    }
    options
}

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

/// Only machines the node watches. A paired device must not be able to make
/// this machine ssh to any address it names.
pub async fn check_host(env: &Env, host: &str) -> anyhow::Result<()> {
    if machines(env).await?.iter().any(|m| m == host) {
        Ok(())
    } else {
        bail!("{host} is not a machine that manymux here watches; add it with `mm add {host}`")
    }
}

pub fn agent_local(env: &Env) -> Command {
    let mut cmd = command(env, "mm");
    cmd.arg("agent");
    cmd
}

/// `ssh <host> mm agent`. The host comes from `check_host`.
pub fn agent_remote(env: &Env, host: &str) -> Command {
    let mut cmd = command(env, "ssh");
    cmd.args(ssh_options(env)).args(["--", host, "mm agent"]);
    cmd
}

/// A local `/bin/sh` script that runs `script` on `host` through ssh, for
/// `ops::exec`. The remote login shell reads `/bin/sh -c '<script>'`.
pub fn exec_on_script(env: &Env, host: &str, script: &str) -> String {
    let remote = format!("/bin/sh -c {}", quote(script));
    let options: Vec<String> = ssh_options(env).iter().map(|o| quote(o)).collect();
    format!("exec ssh {} -- {} {}", options.join(" "), quote(host), quote(&remote))
}

/// Single quotes for POSIX sh (also bash, zsh, fish): `'` becomes `'\''`.
pub fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', r"'\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting_survives_single_quotes() {
        assert_eq!(quote("it's"), r"'it'\''s'");
    }

    #[test]
    fn exec_on_runs_the_script_through_ssh() {
        let line = exec_on_script(&Env::new(), "gmktec", "echo 'hi'");
        assert!(line.starts_with("exec ssh '-o' 'BatchMode=yes' "), "{line}");
        assert!(line.contains(" -- 'gmktec' "), "{line}");
        // The remote shell gets: /bin/sh -c 'echo '\''hi'\'''
        assert!(line.ends_with(&quote(&format!("/bin/sh -c {}", quote("echo 'hi'")))), "{line}");
    }

    #[test]
    fn one_ssh_connection_per_machine_when_there_is_a_runtime_dir() {
        let env: Env = [("XDG_RUNTIME_DIR".to_string(), "/run/user/1000".to_string())].into_iter().collect();
        let options = ssh_options(&env).join(" ");
        assert!(options.contains("ControlMaster=auto"), "{options}");
        assert!(options.contains("ControlPath=/run/user/1000/etd-ssh-%C"), "{options}");
        assert!(!ssh_options(&Env::new()).join(" ").contains("ControlMaster"));
    }
}
