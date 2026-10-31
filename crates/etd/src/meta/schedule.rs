//! `etd meta schedule on|off` (et-ios docs/PLAN-logbook-v2.md D7, package
//! A5): a nightly `etd meta run --refresh` on the archive, as a systemd
//! user timer (Linux) or a launchd agent (macOS). It only makes findings;
//! the owner still decides each one.
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context};

/// In every file this writes, so `off` removes only its own.
const MARK: &str = "Installed by etd meta schedule";
const SERVICE: &str = "etd-meta.service";
const TIMER: &str = "etd-meta.timer";
const LABEL: &str = "xyz.ericsharma.etd.meta";
/// After the night's backups (trigkey: 01:30–04:30).
const HOUR: u32 = 5;

/// The command the job runs: through a login shell, so it finds `etd`
/// and `claude` on the login `PATH` (a Nix store path of today's etd can
/// be gone tomorrow).
pub fn command(exe: &Path, etd_home: Option<&str>) -> String {
    let etd = if exe.starts_with("/nix/store") { "etd".to_string() } else { format!("'{}'", exe.display()) };
    let home = etd_home.map(|h| format!("ETD_HOME='{h}' ")).unwrap_or_default();
    format!("{home}exec {etd} meta run --refresh")
}

pub fn systemd_service(command: &str) -> String {
    let quoted = command.replace('\\', "\\\\").replace('"', "\\\"");
    format!("# {MARK}\n[Unit]\nDescription=etd meta agent: findings from the Logbook\n\n[Service]\nType=oneshot\nExecStart=/bin/sh -lc \"{quoted}\"\n")
}

pub fn systemd_timer() -> String {
    format!(
        "# {MARK}\n[Unit]\nDescription=etd meta agent, nightly\n\n[Timer]\nOnCalendar=*-*-* {HOUR:02}:00:00\nPersistent=true\n\n[Install]\nWantedBy=timers.target\n"
    )
}

pub fn launchd_plist(command: &str, log: &Path) -> String {
    let esc = |t: &str| t.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;");
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!-- {MARK} -->
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{LABEL}</string>
  <key>ProgramArguments</key><array><string>/bin/sh</string><string>-lc</string><string>{}</string></array>
  <key>StartCalendarInterval</key><dict><key>Hour</key><integer>{HOUR}</integer><key>Minute</key><integer>0</integer></dict>
  <key>StandardOutPath</key><string>{log}</string>
  <key>StandardErrorPath</key><string>{log}</string>
</dict>
</plist>
"#,
        esc(command),
        log = esc(&log.display().to_string()),
    )
}

fn run(cmd: &str, args: &[&str]) -> anyhow::Result<()> {
    let status = Command::new(cmd).args(args).status().with_context(|| format!("run {cmd}"))?;
    if !status.success() {
        bail!("{cmd} {}: {status}", args.join(" "));
    }
    Ok(())
}

fn ours(path: &Path) -> bool {
    std::fs::read_to_string(path).is_ok_and(|t| t.contains(MARK))
}

/// Writes `path` unless a file etd did not write is there.
fn write(path: &Path, text: &str) -> anyhow::Result<()> {
    if path.exists() && !ours(path) {
        bail!("{} exists and etd did not write it: not changed", path.display());
    }
    std::fs::create_dir_all(path.parent().context("no folder")?)?;
    std::fs::write(path, text)?;
    Ok(())
}

fn systemd_dir() -> anyhow::Result<PathBuf> {
    Ok(dirs::config_dir().context("no config directory")?.join("systemd/user"))
}

fn plist() -> anyhow::Result<PathBuf> {
    Ok(dirs::home_dir().context("no home directory")?.join("Library/LaunchAgents").join(format!("{LABEL}.plist")))
}

fn domain() -> String {
    // SAFETY: getuid has no preconditions and cannot fail.
    format!("gui/{}", unsafe { libc::getuid() })
}

/// `etd meta schedule on|off`.
pub async fn cli(paths: &crate::config::Paths, on: bool) -> anyhow::Result<()> {
    if on && !crate::logbook::settings(paths).archive {
        bail!("not a Logbook archive (etd logbook archive on): the meta agent runs on the archive");
    }
    let exe = std::env::current_exe().context("where is etd")?;
    let command = command(&exe, std::env::var("ETD_HOME").ok().as_deref());
    if cfg!(target_os = "macos") {
        let path = plist()?;
        let _ = Command::new("launchctl").args(["bootout", &format!("{}/{LABEL}", domain())]).status();
        if on {
            write(&path, &launchd_plist(&command, &paths.state.join("meta.log")))?;
            run("launchctl", &["bootstrap", &domain(), &path.display().to_string()])?;
            println!(
                "The meta agent runs every night at {HOUR:02}:00 ({}). Its log: {}",
                path.display(),
                paths.state.join("meta.log").display()
            );
        } else if ours(&path) {
            std::fs::remove_file(&path)?;
            println!("The nightly meta agent run is off.");
        }
        return Ok(());
    }
    let dir = systemd_dir()?;
    if on {
        write(&dir.join(SERVICE), &systemd_service(&command))?;
        write(&dir.join(TIMER), &systemd_timer())?;
        run("systemctl", &["--user", "daemon-reload"])?;
        run("systemctl", &["--user", "enable", "--now", TIMER])?;
        println!("The meta agent runs every night at {HOUR:02}:00 ({TIMER}). Its log: journalctl --user -u {SERVICE}");
    } else {
        let _ = Command::new("systemctl").args(["--user", "disable", "--now", TIMER]).status();
        for name in [TIMER, SERVICE] {
            if ours(&dir.join(name)) {
                std::fs::remove_file(dir.join(name))?;
            }
        }
        run("systemctl", &["--user", "daemon-reload"])?;
        println!("The nightly meta agent run is off.");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_job_runs_etd_through_a_login_shell() {
        assert_eq!(command(Path::new("/nix/store/abc-etd/bin/etd"), None), "exec etd meta run --refresh");
        assert_eq!(
            command(Path::new("/home/u/.local/bin/etd"), Some("/tmp/e")),
            "ETD_HOME='/tmp/e' exec '/home/u/.local/bin/etd' meta run --refresh"
        );
        let unit = systemd_service("exec etd meta run --refresh");
        assert!(unit.contains("Type=oneshot") && unit.contains("ExecStart=/bin/sh -lc \"exec etd meta run --refresh\""), "{unit}");
        assert!(systemd_timer().contains("OnCalendar=*-*-* 05:00:00"));
        let plist = launchd_plist("a && b", Path::new("/l/meta.log"));
        assert!(plist.contains("<string>a &amp;&amp; b</string>") && plist.contains("<key>Hour</key><integer>5</integer>"), "{plist}");
    }

    #[test]
    fn a_file_etd_did_not_write_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("etd-meta.timer");
        std::fs::write(&path, "mine").unwrap();
        assert!(write(&path, &systemd_timer()).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "mine");
        std::fs::remove_file(&path).unwrap();
        write(&path, &systemd_timer()).unwrap();
        assert!(ours(&path));
    }
}
