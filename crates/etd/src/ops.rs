//! `Exec` and `Upload`.
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context};
use et_proto::{Response, MAX_EXEC_OUTPUT, MAX_UPLOAD};
use iroh::endpoint::RecvStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

use crate::tmux::Env;

pub const EXEC_TIMEOUT: Duration = Duration::from_secs(120);

/// The user's home directory, from the login environment.
pub fn home(env: &Env) -> anyhow::Result<PathBuf> {
    env.get("HOME").map(PathBuf::from).or_else(dirs::home_dir).context("no home directory")
}

/// `/bin/sh -c` in the home directory, stderr joined to stdout, with a time limit.
pub async fn exec(env: &Env, script: &str) -> Response {
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c")
        .arg(format!("exec 2>&1\n{script}"))
        .env_clear()
        .envs(env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    if let Ok(home) = home(env) {
        cmd.current_dir(home);
    }
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => return Response::Error(format!("start /bin/sh: {e}")),
    };
    let mut stdout = child.stdout.take().expect("piped");
    let mut output = Vec::new();
    let mut truncated = false;
    let run = async {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match stdout.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let room = MAX_EXEC_OUTPUT - output.len();
                    if n > room {
                        truncated = true;
                    }
                    output.extend_from_slice(&buf[..n.min(room)]);
                }
            }
        }
        child.wait().await
    };
    match tokio::time::timeout(EXEC_TIMEOUT, run).await {
        Ok(Ok(status)) => Response::ExecResult { output, exit_code: status.code(), truncated },
        Ok(Err(e)) => Response::Error(format!("wait: {e}")),
        // Dropping the child kills it.
        Err(_) => {
            output.extend_from_slice(format!("\netd: killed after {EXEC_TIMEOUT:?}\n").as_bytes());
            Response::ExecResult { output, exit_code: None, truncated }
        }
    }
}

/// `path` as an absolute path inside `home`: absolute or `~/`, no `..`.
pub fn resolve(home: &Path, path: &str) -> anyhow::Result<PathBuf> {
    let full = if let Some(rest) = path.strip_prefix("~/") {
        home.join(rest)
    } else if path.starts_with('/') {
        PathBuf::from(path)
    } else {
        bail!("path must be absolute or start with ~/");
    };
    if full.components().any(|c| matches!(c, Component::ParentDir)) {
        bail!("path must not contain ..");
    }
    if !full.starts_with(home) || full == home || full.file_name().is_none() {
        bail!("path must be a file inside {}", home.display());
    }
    Ok(full)
}

/// Reads `len` bytes from `recv` into `path`, through a temporary file.
pub async fn upload(env: &Env, path: &str, len: u64, recv: &mut RecvStream) -> Response {
    match try_upload(env, path, len, recv).await {
        Ok(path) => Response::Uploaded { path: path.display().to_string() },
        Err(e) => Response::Error(format!("{e:#}")),
    }
}

async fn try_upload(env: &Env, path: &str, len: u64, recv: &mut RecvStream) -> anyhow::Result<PathBuf> {
    if len > MAX_UPLOAD {
        bail!("upload too large: {len} bytes (max {MAX_UPLOAD})");
    }
    let home = home(env)?;
    let target = resolve(&home, path)?;
    let parent = target.parent().context("no parent directory")?;
    tokio::fs::create_dir_all(parent).await.with_context(|| format!("create {}", parent.display()))?;
    // A symlink in the path could point out of home.
    let real_home = tokio::fs::canonicalize(&home).await?;
    if !tokio::fs::canonicalize(parent).await?.starts_with(&real_home) {
        bail!("{} is outside {}", parent.display(), home.display());
    }
    let name = target.file_name().context("no file name")?.to_string_lossy();
    let tmp = parent.join(format!(".{name}.etd-upload"));
    let mut file = tokio::fs::File::create(&tmp).await.with_context(|| format!("create {}", tmp.display()))?;
    let mut left = len;
    let mut buf = vec![0u8; 64 * 1024];
    while left > 0 {
        let want = buf.len().min(left as usize);
        let read = recv.read(&mut buf[..want]).await;
        let n = match read {
            Ok(Some(n)) => n,
            Ok(None) | Err(_) => {
                drop(file);
                let _ = tokio::fs::remove_file(&tmp).await;
                bail!("stream ended after {} of {len} bytes", len - left);
            }
        };
        file.write_all(&buf[..n]).await?;
        left -= n as u64;
    }
    file.flush().await?;
    drop(file);
    tokio::fs::rename(&tmp, &target).await?;
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_keeps_paths_inside_home() {
        let home = Path::new("/home/u");
        assert_eq!(resolve(home, "~/a/b.png").unwrap(), Path::new("/home/u/a/b.png"));
        assert_eq!(resolve(home, "/home/u/x").unwrap(), Path::new("/home/u/x"));
        assert!(resolve(home, "/etc/passwd").is_err());
        assert!(resolve(home, "~/../v/x").is_err());
        assert!(resolve(home, "/home/u/../v/x").is_err());
        assert!(resolve(home, "/home/uv/x").is_err());
        assert!(resolve(home, "a/b").is_err());
        assert!(resolve(home, "~/").is_err());
    }
}
