//! `etd install` / `etd uninstall`, without Nix (Nix users enable the
//! flake's Home Manager module instead).
//!
//! `etd install` sets up two user services, systemd on Linux, launchd on
//! macOS (not tested on macOS yet):
//! - etd itself;
//! - the manymux node (`mm daemon --no-notify`) with `MM_SSH=etd-ssh`, after
//!   installing `mm` (manymux's installer, pinned) when it is missing.
//!
//! It also adds one marked line that sets `MM_SSH` to the user's shell
//! startup file (D5). It never restarts a running manymux node and
//! `etd uninstall` never stops one: a node restart ends every session.
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{bail, Context};

use crate::config::{self, Paths};

const ETD_UNIT: &str = "etd.service";
const MM_UNIT: &str = "manymux.service";
const ETD_LABEL: &str = "xyz.ericsharma.etd";
const MM_LABEL: &str = "xyz.ericsharma.etd.manymux";
/// In every file etd writes, so `uninstall` removes only its own.
const MARK: &str = "Installed by etd install";
/// The manymux release etd is built and tested with.
const MM_VERSION: &str = "0.2.2";

/// A user service: what runs, with which environment.
#[derive(Debug, Clone)]
pub struct Service {
    pub description: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    /// Restart on any exit (etd), or only on a failure (the manymux node).
    pub always: bool,
    pub log: PathBuf,
}

fn quote_systemd(text: &str) -> String {
    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
}

pub fn systemd_unit(s: &Service) -> String {
    let exec: Vec<String> = s.args.iter().map(|a| quote_systemd(a)).collect();
    let mut text = format!(
        "# {MARK}\n[Unit]\nDescription={}\n\n[Service]\nExecStart={}\n",
        s.description,
        exec.join(" ")
    );
    for (k, v) in &s.env {
        text.push_str(&format!(
            "Environment={}\n",
            quote_systemd(&format!("{k}={v}"))
        ));
    }
    text.push_str(&format!(
        "Restart={}\nRestartSec=2\n\n[Install]\nWantedBy=default.target\n",
        if s.always { "always" } else { "on-failure" }
    ));
    text
}

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

pub fn launchd_plist(label: &str, s: &Service) -> String {
    let args: String = s
        .args
        .iter()
        .map(|a| format!("<string>{}</string>", xml_escape(a)))
        .collect();
    let env: String = s
        .env
        .iter()
        .map(|(k, v)| {
            format!(
                "<key>{}</key><string>{}</string>",
                xml_escape(k),
                xml_escape(v)
            )
        })
        .collect();
    let log = xml_escape(&s.log.display().to_string());
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!-- {MARK} -->
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{label}</string>
  <key>ProgramArguments</key><array>{args}</array>
  <key>EnvironmentVariables</key><dict>{env}</dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>{log}</string>
  <key>StandardErrorPath</key><string>{log}</string>
</dict>
</plist>
"#
    )
}

const BEGIN: &str = "# >>> etd: manymux reaches the network's machines through etd >>>";
const END: &str = "# <<< etd <<<";

/// `text` without etd's block, and with `block` added at the end if given.
pub fn with_block(text: &str, block: Option<&str>) -> String {
    let mut out = String::new();
    let mut inside = false;
    for line in text.lines() {
        if line == BEGIN {
            inside = true;
        } else if line == END && inside {
            inside = false;
        } else if !inside {
            out.push_str(line);
            out.push('\n');
        }
    }
    if let Some(block) = block {
        if !out.is_empty() && !out.ends_with("\n\n") {
            out.push('\n');
        }
        out.push_str(&format!("{BEGIN}\n{block}\n{END}\n"));
    }
    out
}

fn home() -> anyhow::Result<PathBuf> {
    dirs::home_dir().context("no home directory")
}

/// The startup file and the line that sets `MM_SSH` in the user's shell.
fn shell_file(shell: &str, etd_ssh: &Path) -> anyhow::Result<Option<(PathBuf, String)>> {
    let home = home()?;
    let value = etd_ssh.display().to_string().replace('\'', "'\\''");
    Ok(match shell.rsplit('/').next().unwrap_or("") {
        // ~/.zshenv: read by every zsh, interactive or not.
        "zsh" => Some((home.join(".zshenv"), format!("export MM_SSH='{value}'"))),
        "bash" => Some((home.join(".bashrc"), format!("export MM_SSH='{value}'"))),
        "fish" => Some((
            home.join(".config/fish/conf.d/etd.fish"),
            format!("set -gx MM_SSH '{value}'"),
        )),
        _ => None,
    })
}

fn login_shell() -> String {
    std::env::var("SHELL").unwrap_or_default()
}

fn unit_dir() -> anyhow::Result<PathBuf> {
    Ok(dirs::config_dir()
        .context("no config directory")?
        .join("systemd/user"))
}

fn plist_path(label: &str) -> anyhow::Result<PathBuf> {
    Ok(home()?
        .join("Library/LaunchAgents")
        .join(format!("{label}.plist")))
}

fn run(cmd: &str, args: &[&str]) -> anyhow::Result<()> {
    let status = Command::new(cmd)
        .args(args)
        .status()
        .with_context(|| format!("run {cmd}"))?;
    if !status.success() {
        bail!("{cmd} {}: {status}", args.join(" "));
    }
    Ok(())
}

fn uid() -> u32 {
    // SAFETY: getuid has no preconditions and cannot fail.
    unsafe { libc::getuid() }
}

/// `mm` on PATH, or where manymux's installer puts it.
fn find_mm() -> Option<PathBuf> {
    let mut places: Vec<PathBuf> = std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).map(|d| d.join("mm")).collect())
        .unwrap_or_default();
    if let Ok(home) = home() {
        places.push(home.join(".local/bin/mm"));
    }
    places.push("/usr/local/bin/mm".into());
    places.into_iter().find(|p| p.is_file())
}

fn mm_version(mm: &Path) -> Option<String> {
    let out = Command::new(mm).arg("--version").output().ok()?;
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// `mm`, installed with manymux's own installer (it checks the checksum and
/// signature) when it is missing.
fn ensure_mm() -> anyhow::Result<PathBuf> {
    if let Some(mm) = find_mm() {
        let version = mm_version(&mm).unwrap_or_default();
        if !version.starts_with(&format!("mm {MM_VERSION}")) {
            eprintln!(
                "note: {} is {version:?}; etd is tested with manymux {MM_VERSION}",
                mm.display()
            );
        }
        return Ok(mm);
    }
    println!("installing manymux {MM_VERSION} (mm)…");
    let script = format!(
        "curl -fsSL https://raw.githubusercontent.com/rayfish/manymux/v{MM_VERSION}/install.sh | MM_VERSION=v{MM_VERSION} sh"
    );
    run("/bin/sh", &["-c", &script])?;
    find_mm().context(
        "manymux's installer finished, but mm is not in PATH, ~/.local/bin or /usr/local/bin",
    )
}

/// A manymux node answers on the default socket.
fn node_running() -> bool {
    let socket = PathBuf::from(format!("/tmp/manymux-{}/manymux.sock", uid()));
    UnixStream::connect(socket).is_ok()
}

/// manymux's machine list, if it already lists machines. etd manages the list
/// only when it is empty or missing, so a user's own `mm add` list stays.
fn own_mm_hosts() -> Option<String> {
    let path = std::env::var_os("MM_CONFIG_DIR")
        .map(PathBuf::from)
        .or_else(|| dirs::config_dir().map(|d| d.join("manymux")))?
        .join("hosts.toml");
    let text = std::fs::read_to_string(path).ok()?;
    text.contains('"').then_some(text)
}

fn written_by_etd(path: &Path) -> bool {
    std::fs::read_to_string(path).is_ok_and(|t| t.contains(MARK))
}

pub fn install(paths: &Paths) -> anyhow::Result<()> {
    let exe = std::env::current_exe()?.canonicalize()?;
    if exe.starts_with("/nix/store") {
        eprintln!(
            "note: {} is in the Nix store; a garbage collection can remove it.",
            exe.display()
        );
        eprintln!("      With Nix, prefer the flake's module (services.etd.enable = true).");
    }
    let etd_ssh = exe.with_file_name("etd-ssh");
    if !etd_ssh.is_file() {
        bail!("{} is missing: it is installed with etd", etd_ssh.display());
    }
    let mm = ensure_mm()?;
    let home = home()?;
    let logs = if cfg!(target_os = "macos") {
        home.join("Library/Logs")
    } else {
        paths.state.clone()
    };

    let mut etd_env = Vec::new();
    match own_mm_hosts() {
        None => etd_env.push(("ETD_MANYMUX_HOSTS".to_string(), "1".to_string())),
        Some(_) => {
            println!("note: manymux already lists machines (~/.config/manymux/hosts.toml), so etd leaves that list alone.");
            println!("      Empty it and run `etd install` again to let etd keep it equal to the network.");
        }
    }
    let etd = Service {
        description: "etd: reach this machine from E.T. over iroh".into(),
        args: vec![exe.display().to_string()],
        env: etd_env,
        always: true,
        log: logs.join("etd.log"),
    };
    let node = Service {
        description: "manymux node: persistent terminal sessions (MM_SSH=etd-ssh)".into(),
        args: vec![
            mm.display().to_string(),
            "daemon".into(),
            "--no-notify".into(),
        ],
        env: vec![("MM_SSH".into(), etd_ssh.display().to_string())],
        always: false,
        log: logs.join("manymux-daemon.log"),
    };
    let ours = if cfg!(target_os = "macos") {
        plist_path(MM_LABEL)?
    } else {
        unit_dir()?.join(MM_UNIT)
    };
    let start_node = if node_running() && !written_by_etd(&ours) {
        println!("note: a manymux node already runs, not started by etd. It stays as it is.");
        println!(
            "      For it to reach the network's machines, give it MM_SSH={}",
            etd_ssh.display()
        );
        false
    } else {
        true
    };

    if cfg!(target_os = "macos") {
        install_launchd(ETD_LABEL, &etd, true)?;
        if start_node {
            install_launchd(MM_LABEL, &node, false)?;
        }
    } else {
        install_systemd(ETD_UNIT, &etd, true)?;
        if start_node {
            install_systemd(MM_UNIT, &node, false)?;
        }
        let linger = Command::new("loginctl")
            .args(["show-user", &uid().to_string(), "-p", "Linger"])
            .output();
        if linger.is_ok_and(|o| String::from_utf8_lossy(&o.stdout).trim() == "Linger=no") {
            println!("note: etd and manymux stop when you log out. To keep them running, run:");
            println!("      loginctl enable-linger $USER");
        }
    }

    match shell_file(&login_shell(), &etd_ssh)? {
        Some((file, line)) => {
            let old = std::fs::read_to_string(&file).unwrap_or_default();
            let new = with_block(&old, Some(&line));
            if new != old {
                if let Some(dir) = file.parent() {
                    std::fs::create_dir_all(dir)?;
                }
                std::fs::write(&file, new)?;
                println!(
                    "added MM_SSH to {} (open a new terminal for it)",
                    file.display()
                );
            }
        }
        None => println!(
            "note: set MM_SSH={} in your shell's startup file",
            etd_ssh.display()
        ),
    }

    for _ in 0..40 {
        if config::running_status(paths)?.is_some() {
            println!(
                "etd is running. `etd invite` adds a phone; `etd invite --machine` adds a machine."
            );
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    bail!("etd did not start in 20 s; see {}", etd.log.display())
}

/// Writes the unit. etd restarts; the manymux node only starts if it is
/// not running (a restart would end its sessions).
fn install_systemd(name: &str, service: &Service, restart: bool) -> anyhow::Result<()> {
    let path = unit_dir()?.join(name);
    std::fs::create_dir_all(path.parent().unwrap())?;
    let text = systemd_unit(service);
    let changed = std::fs::read_to_string(&path).map_or(true, |old| old != text);
    std::fs::write(&path, &text)?;
    run("systemctl", &["--user", "daemon-reload"])?;
    run("systemctl", &["--user", "enable", name])?;
    if restart {
        run("systemctl", &["--user", "restart", name])?;
    } else {
        run("systemctl", &["--user", "start", name])?;
        if changed {
            println!("note: {name} changed; a running node uses it from its next start (a restart ends its sessions)");
        }
    }
    println!("installed {}", path.display());
    Ok(())
}

fn install_launchd(label: &str, service: &Service, restart: bool) -> anyhow::Result<()> {
    let path = plist_path(label)?;
    std::fs::create_dir_all(path.parent().unwrap())?;
    std::fs::write(&path, launchd_plist(label, service))?;
    let domain = format!("gui/{}", uid());
    let loaded = Command::new("launchctl")
        .args(["print", &format!("{domain}/{label}")])
        .output()
        .is_ok_and(|o| o.status.success());
    if loaded && restart {
        let _ = Command::new("launchctl")
            .args(["bootout", &format!("{domain}/{label}")])
            .status();
    }
    if !loaded || restart {
        run(
            "launchctl",
            &["bootstrap", &domain, &path.display().to_string()],
        )?;
    } else {
        println!("note: {label} is loaded; it uses the new settings from its next start (a restart ends its sessions)");
    }
    println!("installed {}", path.display());
    Ok(())
}

pub fn uninstall() -> anyhow::Result<()> {
    if cfg!(target_os = "macos") {
        let _ = Command::new("launchctl")
            .args(["bootout", &format!("gui/{}/{ETD_LABEL}", uid())])
            .status();
        for label in [ETD_LABEL, MM_LABEL] {
            let path = plist_path(label)?;
            if written_by_etd(&path) {
                std::fs::remove_file(&path)?;
                println!("removed {}", path.display());
            }
        }
    } else {
        let _ = Command::new("systemctl")
            .args(["--user", "disable", "--now", ETD_UNIT])
            .status();
        let _ = Command::new("systemctl")
            .args(["--user", "disable", MM_UNIT])
            .status();
        for name in [ETD_UNIT, MM_UNIT] {
            let path = unit_dir()?.join(name);
            if written_by_etd(&path) {
                std::fs::remove_file(&path)?;
                println!("removed {}", path.display());
            }
        }
        run("systemctl", &["--user", "daemon-reload"])?;
    }
    if let Some((file, _)) = shell_file(&login_shell(), Path::new("etd-ssh"))? {
        if let Ok(old) = std::fs::read_to_string(&file) {
            let new = with_block(&old, None);
            if new != old {
                std::fs::write(&file, new)?;
                println!("removed MM_SSH from {}", file.display());
            }
        }
    }
    println!("The manymux node keeps running (stopping it would end its sessions); it stops at the next reboot.");
    println!("Your key and the network are kept. Delete the etd config directory to remove them.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service() -> Service {
        Service {
            description: "manymux node".into(),
            args: vec![
                "/home/a b/.local/bin/mm".into(),
                "daemon".into(),
                "--no-notify".into(),
            ],
            env: vec![("MM_SSH".into(), "/home/a b/.local/bin/etd-ssh".into())],
            always: false,
            log: "/tmp/x.log".into(),
        }
    }

    #[test]
    fn unit_quotes_paths_and_environment() {
        let unit = systemd_unit(&service());
        assert!(unit.starts_with(&format!("# {MARK}\n")));
        assert!(unit.contains("ExecStart=\"/home/a b/.local/bin/mm\" \"daemon\" \"--no-notify\"\n"));
        assert!(unit.contains("Environment=\"MM_SSH=/home/a b/.local/bin/etd-ssh\"\n"));
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("WantedBy=default.target"));
    }

    #[test]
    fn plist_escapes_xml_and_carries_the_environment() {
        let mut s = service();
        s.args[0] = "/Users/a&b/mm".into();
        let plist = launchd_plist(MM_LABEL, &s);
        assert!(plist.contains("<string>/Users/a&amp;b/mm</string><string>daemon</string>"));
        assert!(plist.contains("<key>MM_SSH</key><string>/home/a b/.local/bin/etd-ssh</string>"));
        assert!(plist.contains(MARK));
        assert!(!plist.contains("a&b"));
    }

    #[test]
    fn shell_block_is_added_once_and_removed_cleanly() {
        let original = "export PATH=$PATH:~/bin\n";
        let added = with_block(original, Some("export MM_SSH='/x/etd-ssh'"));
        assert!(added.starts_with(original));
        assert_eq!(added.matches(BEGIN).count(), 1);
        let again = with_block(&added, Some("export MM_SSH='/y/etd-ssh'"));
        assert_eq!(again.matches(BEGIN).count(), 1, "replaced, not added twice");
        assert!(again.contains("/y/etd-ssh") && !again.contains("/x/etd-ssh"));
        assert_eq!(with_block(&again, None).trim_end(), original.trim_end());
    }
}
