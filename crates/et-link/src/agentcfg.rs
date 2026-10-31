//! Agent instruction files for the apps (et-ios docs/PLAN-logbook-v2.md D9):
//! read a machine's allow-listed files, and replace one. Call these on a
//! `RemoteHost` connected to the machine itself, not to the archive.
use et_proto::{Request, Response};

use crate::{unexpected, LinkError, RemoteHost};

#[uniffi::export(async_runtime = "tokio")]
impl RemoteHost {
    /// This machine's agent instruction files (JSON): path, sha256, size,
    /// time, content, and whether git tracks it.
    pub async fn agent_config(&self) -> Result<String, LinkError> {
        match self.call(Request::AgentConfig).await? {
            Response::AgentConfig { json } => Ok(json),
            Response::Error(message) => Err(LinkError::Remote { message }),
            other => Err(unexpected(other)),
        }
    }

    /// Replaces an allow-listed file with `content` if its sha256 is still
    /// `base` (empty: it must not exist yet). Returns the backup's path
    /// (empty: there was no old file).
    pub async fn config_write(&self, path: String, base: String, content: String) -> Result<String, LinkError> {
        match self.call(Request::ConfigWrite { path, base, content }).await? {
            Response::ConfigWritten { backup } => Ok(backup),
            Response::Error(message) => Err(LinkError::Remote { message }),
            other => Err(unexpected(other)),
        }
    }
}
