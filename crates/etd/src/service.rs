//! `etd install` / `etd uninstall`: run etd as a user service, without Nix.
//! Linux: a systemd user unit. macOS: a launchd agent (not tested yet).
//! Nix users enable the flake's module instead.
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{bail, Context};

use crate::config::{self, Paths};

const UNIT: &str = "etd.service";
const LABEL: &str = "xyz.ericsharma.etd";

pub fn systemd_unit(exe: &Path) -> String {
    format!(
        "[Unit]\n\
         Description=etd: reach tmux on this machine from E.T. over iroh\n\
         \n\
         [Service]\n\
         ExecStart=\"{}\"\n\
         Restart=always\n\
         RestartSec=2\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        exe.display()
    )
}

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

pub fn launchd_plist(exe: &Path, log: &Path) -> String {
    let exe = xml_escape(&exe.display().to_string());
    let log = xml_escape(&log.display().to_string());
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{LABEL}</string>
  <key>ProgramArguments</key><array><string>{exe}</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>{log}</string>
  <key>StandardErrorPath</key><string>{log}</string>
</dict>
</plist>
"#
    )
}

fn home() -> anyhow::Result<PathBuf> {
    dirs::home_dir().context("no home directory")
}

fn unit_path() -> anyhow::Result<PathBuf> {
    Ok(dirs::config_dir().context("no config directory")?.join("systemd/user").join(UNIT))
}

fn plist_path() -> anyhow::Result<PathBuf> {
    Ok(home()?.join("Library/LaunchAgents").join(format!("{LABEL}.plist")))
}

fn run(cmd: &str, args: &[&str]) -> anyhow::Result<()> {
    let status = Command::new(cmd).args(args).status().with_context(|| format!("run {cmd}"))?;
    if !status.success() {
        bail!("{cmd} {}: {status}", args.join(" "));
    }
    Ok(())
}

fn uid() -> String {
    // SAFETY: getuid has no preconditions and cannot fail.
    unsafe { libc::getuid() }.to_string()
}

pub fn install(paths: &Paths) -> anyhow::Result<()> {
    let exe = std::env::current_exe()?.canonicalize()?;
    if exe.starts_with("/nix/store") {
        eprintln!("note: {} is in the Nix store; a garbage collection can remove it.", exe.display());
        eprintln!("      With Nix, prefer the flake's module (services.etd.enable = true).");
    }
    if cfg!(target_os = "macos") {
        let plist = plist_path()?;
        let log = home()?.join("Library/Logs/etd.log");
        std::fs::create_dir_all(plist.parent().unwrap())?;
        std::fs::write(&plist, launchd_plist(&exe, &log))?;
        let domain = format!("gui/{}", uid());
        let _ = Command::new("launchctl").args(["bootout", &format!("{domain}/{LABEL}")]).status();
        run("launchctl", &["bootstrap", &domain, &plist.display().to_string()])?;
        println!("installed {}", plist.display());
    } else {
        let unit = unit_path()?;
        std::fs::create_dir_all(unit.parent().unwrap())?;
        std::fs::write(&unit, systemd_unit(&exe))?;
        run("systemctl", &["--user", "daemon-reload"])?;
        run("systemctl", &["--user", "enable", "--now", UNIT])?;
        run("systemctl", &["--user", "restart", UNIT])?;
        println!("installed {}", unit.display());
        let linger = Command::new("loginctl").args(["show-user", &uid(), "-p", "Linger"]).output();
        if linger.is_ok_and(|o| String::from_utf8_lossy(&o.stdout).trim() == "Linger=no") {
            println!("note: etd stops when you log out. To keep it running, run:");
            println!("      loginctl enable-linger $USER");
        }
    }
    for _ in 0..40 {
        if config::running_status(paths)?.is_some() {
            println!("etd is running. Run `etd pair` to add a device.");
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    bail!("etd did not start in 20 s; see {}", if cfg!(target_os = "macos") { "~/Library/Logs/etd.log" } else { "journalctl --user -u etd" })
}

pub fn uninstall() -> anyhow::Result<()> {
    if cfg!(target_os = "macos") {
        let plist = plist_path()?;
        let _ = Command::new("launchctl").args(["bootout", &format!("gui/{}/{LABEL}", uid())]).status();
        if plist.exists() {
            std::fs::remove_file(&plist)?;
            println!("removed {}", plist.display());
        }
    } else {
        let unit = unit_path()?;
        let _ = Command::new("systemctl").args(["--user", "disable", "--now", UNIT]).status();
        if unit.exists() {
            std::fs::remove_file(&unit)?;
            run("systemctl", &["--user", "daemon-reload"])?;
            println!("removed {}", unit.display());
        }
    }
    println!("Your key and paired devices are kept. Delete the etd config directory to remove them.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_quotes_the_path() {
        let unit = systemd_unit(Path::new("/home/a b/.local/bin/etd"));
        assert!(unit.contains("ExecStart=\"/home/a b/.local/bin/etd\"\n"));
        assert!(unit.contains("WantedBy=default.target"));
    }

    #[test]
    fn plist_escapes_xml() {
        let plist = launchd_plist(Path::new("/Users/a&b/etd"), Path::new("/Users/a&b/Library/Logs/etd.log"));
        assert!(plist.contains("<string>/Users/a&amp;b/etd</string>"));
        assert!(!plist.contains("a&b"));
    }
}
