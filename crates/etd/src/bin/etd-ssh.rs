//! etd-ssh: an `ssh` stand-in for manymux (`MM_SSH=etd-ssh`). It reaches
//! another machine of the etd network through this machine's etd daemon, so
//! manymux needs no ssh keys, no ssh config and no `mm add`.
//!
//! manymux runs it as `etd-ssh [-T|-t] [-o opt]… <machine> -- <command…>`:
//! - `mm agent` → a pipe to `mm agent` on that machine
//! - anything else (`true`, a checkpoint's `/proc` reads) → run it there;
//!   output to stdout, the exit code as ours
//!
//! Exit code 255 means etd-ssh itself failed (as ssh does).
use std::process::ExitCode;

use anyhow::{bail, Context};
use et_proto::{ControlFrame, Request, Response};
use etd::config::Paths;
use etd::local::{receive, send, socket_path};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

/// `(machine, command words)` from an ssh-style command line.
fn parse(args: &[String]) -> anyhow::Result<(String, Vec<String>)> {
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--" => {
                i += 1;
                break;
            }
            // Options with a value, as ssh has them.
            "-o" | "-p" | "-l" | "-i" | "-F" | "-J" | "-S" | "-E" | "-b" | "-c" | "-m" | "-O"
            | "-W" | "-w" => i += 2,
            flag if flag.starts_with('-') => i += 1,
            _ => break,
        }
    }
    let Some(host) = args.get(i) else {
        bail!("usage: etd-ssh [options] <machine> [--] <command…>")
    };
    let mut rest = &args[i + 1..];
    if rest.first().is_some_and(|a| a == "--") {
        rest = &rest[1..];
    }
    Ok((host.clone(), rest.to_vec()))
}

/// `mm agent`, whatever the path to `mm` is.
fn is_agent(command: &[String]) -> bool {
    command.len() == 2 && command[1] == "agent" && command[0].rsplit('/').next() == Some("mm")
}

async fn run() -> anyhow::Result<u8> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (host, command) = parse(&args)?;
    if command.is_empty() {
        bail!("etd-ssh runs a command; it does not open a login shell");
    }
    let socket = socket_path(&Paths::from_env()?);
    let mut stream = UnixStream::connect(&socket).await.with_context(|| {
        format!(
            "reach the etd daemon at {} (is etd running?)",
            socket.display()
        )
    })?;

    if !is_agent(&command) {
        send(
            &mut stream,
            &Request::ExecOn {
                host,
                script: command.join(" "),
            },
        )
        .await?;
        return match receive::<_, Response>(&mut stream)
            .await?
            .context("no answer from etd")?
        {
            Response::ExecResult {
                output, exit_code, ..
            } => {
                let mut stdout = tokio::io::stdout();
                stdout.write_all(&output).await?;
                stdout.flush().await?;
                Ok(exit_code.map(|c| c.clamp(0, 254) as u8).unwrap_or(255))
            }
            Response::Error(e) => bail!("{e}"),
            other => bail!("unexpected answer: {other:?}"),
        };
    }

    send(&mut stream, &Request::Agent { host: Some(host) }).await?;
    match receive::<_, Response>(&mut stream)
        .await?
        .context("no answer from etd")?
    {
        Response::AgentStarted => {}
        Response::Error(e) => bail!("{e}"),
        other => bail!("unexpected answer: {other:?}"),
    }
    let (mut rx, mut tx) = stream.into_split();
    // stdin → the remote `mm agent`.
    tokio::spawn(async move {
        let mut stdin = tokio::io::stdin();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match stdin.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if tx.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = tx.shutdown().await;
    });
    let mut stdout = tokio::io::stdout();
    while let Some(frame) = receive::<_, ControlFrame>(&mut rx).await? {
        match frame {
            ControlFrame::Stdout(bytes) => {
                stdout.write_all(&bytes).await?;
                stdout.flush().await?;
            }
            ControlFrame::Stderr(line) => eprintln!("{line}"),
            ControlFrame::Exit(code) => {
                return Ok(code.map(|c| c.clamp(0, 254) as u8).unwrap_or(255))
            }
        }
    }
    Ok(255)
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("etd-ssh: {e:#}");
            ExitCode::from(255)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(s: &str) -> Vec<String> {
        s.split_whitespace().map(String::from).collect()
    }

    #[test]
    fn reads_manymux_command_lines() {
        let (host, command) = parse(&args(
            "-T -o ControlMaster=auto -o ControlPath=/x -o BatchMode=yes gmktec -- mm agent",
        ))
        .unwrap();
        assert_eq!(host, "gmktec");
        assert!(is_agent(&command));
        let (host, command) = parse(&args("-t mac-mini -- true")).unwrap();
        assert_eq!(
            (host.as_str(), command),
            ("mac-mini", vec!["true".to_string()])
        );
        let (_, command) = parse(&args("gmktec /home/u/.local/bin/mm agent")).unwrap();
        assert!(is_agent(&command));
        assert!(!is_agent(&args("echo agent")));
        assert!(parse(&args("-T -o X")).is_err());
    }
}
