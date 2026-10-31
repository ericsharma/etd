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
            let notice = format!("\netd: killed after {EXEC_TIMEOUT:?}\n");
            // The notice counts toward the cap: cut the output to make room.
            let keep = MAX_EXEC_OUTPUT - notice.len();
            if output.len() > keep {
                output.truncate(keep);
                truncated = true;
            }
            output.extend_from_slice(notice.as_bytes());
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
    let name = target.file_name().context("no file name")?.to_owned();
    // From here on, the checked directory with no symlinks in its path
    // (the answer keeps the path as asked).
    let parent = make_parent(&home, parent).await?;
    let real_target = parent.join(&name);
    // A new file (O_EXCL never follows a symlink), with a name no other
    // upload uses at the same time.
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = parent.join(format!(".{}.{}-{n}.etd-upload", name.to_string_lossy(), std::process::id()));
    let mut file =
        tokio::fs::OpenOptions::new().write(true).create_new(true).open(&tmp).await.with_context(|| format!("create {}", tmp.display()))?;
    let written = async {
        let mut left = len;
        let mut buf = vec![0u8; 64 * 1024];
        while left > 0 {
            let want = buf.len().min(left as usize);
            let n = match recv.read(&mut buf[..want]).await {
                Ok(Some(n)) => n,
                Ok(None) | Err(_) => bail!("stream ended after {} of {len} bytes", len - left),
            };
            file.write_all(&buf[..n]).await?;
            left -= n as u64;
        }
        file.flush().await?;
        drop(file);
        // Replaces a symlink at `target` itself, never what it points to.
        tokio::fs::rename(&tmp, &real_target).await?;
        anyhow::Ok(())
    }
    .await;
    if written.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    written.map(|()| target)
}

/// Creates `parent` (a directory inside `home`) and returns it with symlinks
/// resolved. A symlink in the path could point out of home: the part that
/// exists is checked before anything is created (`create_dir_all` follows
/// symlinks), and the whole path again after.
async fn make_parent(home: &Path, parent: &Path) -> anyhow::Result<PathBuf> {
    let real_home = tokio::fs::canonicalize(home).await?;
    let mut existing = parent;
    while tokio::fs::symlink_metadata(existing).await.is_err() {
        existing = existing.parent().context("no existing parent directory")?;
    }
    if !tokio::fs::canonicalize(existing).await?.starts_with(&real_home) {
        bail!("{} is outside {}", parent.display(), home.display());
    }
    tokio::fs::create_dir_all(parent).await.with_context(|| format!("create {}", parent.display()))?;
    let real = tokio::fs::canonicalize(parent).await?;
    if !real.starts_with(&real_home) {
        bail!("{} is outside {}", parent.display(), home.display());
    }
    Ok(real)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A symlink out of home is refused before any directory is made.
    #[tokio::test]
    async fn make_parent_creates_nothing_outside_home() {
        let dir = tempfile::tempdir().unwrap();
        let (home, outside) = (dir.path().join("home"), dir.path().join("outside"));
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, home.join("link")).unwrap();
        assert!(make_parent(&home, &home.join("link/new")).await.is_err());
        assert!(!outside.join("new").exists(), "a directory was made outside home");
        let made = make_parent(&home, &home.join("a/b")).await.unwrap();
        assert_eq!(made, home.canonicalize().unwrap().join("a/b"));
        assert!(made.is_dir());
    }

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
