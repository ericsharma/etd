//! manymux on this machine: `mm hosts`, `mm agent`, and manymux's machine
//! list. etd never reads the manymux protocol; it starts `mm agent` and moves
//! the bytes. Other machines are reached through the network (local.rs).
use anyhow::{bail, Context};
use tokio::process::Command;

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

/// `mm` is on the login environment's PATH.
pub fn available(env: &Env) -> bool {
    env.get("PATH").is_some_and(|path| std::env::split_paths(path).any(|d| d.join("mm").is_file()))
}

pub fn agent_local(env: &Env) -> Command {
    let mut cmd = command(env, "mm");
    cmd.arg("agent");
    cmd
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
