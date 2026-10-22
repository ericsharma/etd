//! et/1: the protocol between E.T. and `etd`, over iroh.
//!
//! - One iroh connection per E.T. session. Each operation opens one bi-stream.
//! - The first frame on a stream is a [`Request`]; `etd` answers with one
//!   [`Response`] frame. `Control` then carries [`ControlFrame`]s down and raw
//!   bytes up (tmux stdin).
//! - A frame is a big-endian `u32` length, then a postcard body.
//! - postcard is positional: any change to these types is a new ALPN.
use anyhow::{bail, Context};
use iroh::endpoint::{RecvStream, SendStream};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub const ALPN: &[u8] = b"et/1";

/// Frames larger than this are refused.
pub const MAX_FRAME: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Request {
    Hello { app: String },
    ListSessions,
    Control { target: Target },
    Ping,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Target {
    /// Attach to an existing session by exact name.
    Attach(String),
    /// Attach, or create the session if it does not exist.
    AttachOrCreate(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Response {
    HostInfo(HostInfo),
    Sessions(Vec<SessionInfo>),
    /// `tmux -C` started; `ControlFrame`s follow.
    ControlStarted,
    Pong,
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostInfo {
    pub host_name: String,
    pub os: String,
    pub etd_version: String,
    pub tmux: TmuxStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TmuxStatus {
    Found(TmuxInstall),
    /// What the search tried, for the "tmux not found" screen.
    NotFound(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TmuxInstall {
    pub path: String,
    pub version: String,
    /// The `TMUX_TMPDIR` that tmux runs with. None: tmux's default.
    pub tmpdir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionInfo {
    /// tmux's `$n` id.
    pub id: String,
    pub name: String,
    pub windows: u32,
    pub attached: u32,
}

/// Down the `Control` stream, after `ControlStarted`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ControlFrame {
    Stdout(Vec<u8>),
    Stderr(String),
    /// tmux exited. None: killed by a signal.
    Exit(Option<i32>),
}

pub fn encode<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    let body = postcard::to_stdvec(value)?;
    if body.len() > MAX_FRAME {
        bail!("frame too large: {} bytes", body.len());
    }
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

pub async fn write_frame<T: Serialize>(send: &mut SendStream, value: &T) -> anyhow::Result<()> {
    send.write_all(&encode(value)?).await.context("write frame")?;
    Ok(())
}

/// Reads one frame. `Ok(None)`: the stream finished cleanly before a frame.
pub async fn read_frame<T: DeserializeOwned>(recv: &mut RecvStream) -> anyhow::Result<Option<T>> {
    let mut len = [0u8; 4];
    match recv.read_exact(&mut len).await {
        Ok(()) => {}
        Err(iroh::endpoint::ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(e) => return Err(e).context("read frame length"),
    }
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        bail!("frame too large: {len} bytes");
    }
    let mut body = vec![0u8; len];
    recv.read_exact(&mut body).await.context("read frame body")?;
    Ok(Some(postcard::from_bytes(&body).context("decode frame")?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip() {
        let frame = ControlFrame::Stdout(b"%begin 1 2 1\n".to_vec());
        let bytes = encode(&frame).unwrap();
        assert_eq!(u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize, bytes.len() - 4);
        let back: ControlFrame = postcard::from_bytes(&bytes[4..]).unwrap();
        assert_eq!(back, frame);
    }
}
