//! One-time pairing secrets. `etd pair` writes the hash of a secret to
//! `pending/`; the daemon removes it when a device redeems it. The two share
//! files only.
use std::fs;
use std::time::Duration;

use anyhow::Context;
use et_proto::Ticket;
use iroh::EndpointAddr;
use qrcode::render::unicode::Dense1x2;
use qrcode::QrCode;
use sha2::{Digest, Sha256};

use crate::config::{now_secs, Paths};

pub const TTL: Duration = Duration::from_secs(10 * 60);

fn hash_hex(secret: &[u8; 32]) -> String {
    Sha256::digest(secret).iter().map(|b| format!("{b:02x}")).collect()
}

/// Makes a secret valid for `TTL` and the ticket that carries it.
pub fn create(paths: &Paths, addr: EndpointAddr, host_name: String) -> anyhow::Result<Ticket> {
    let mut secret = [0u8; 32];
    getrandom::fill(&mut secret).map_err(|e| anyhow::anyhow!("random: {e}"))?;
    let expires = now_secs() + TTL.as_secs();
    let dir = paths.pending();
    fs::create_dir_all(&dir)?;
    fs::write(dir.join(hash_hex(&secret)), expires.to_string()).context("write pending secret")?;
    Ok(Ticket { addr, secret, expires, host_name })
}

/// True if `secret` is pending and not expired. It can be used once only.
pub fn redeem(paths: &Paths, secret: &[u8; 32]) -> bool {
    remove_expired(paths);
    let path = paths.pending().join(hash_hex(secret));
    let Ok(text) = fs::read_to_string(&path) else { return false };
    // Removed first: a second try with the same secret fails even if this one does.
    if fs::remove_file(&path).is_err() {
        return false;
    }
    text.trim().parse::<u64>().is_ok_and(|expires| expires > now_secs())
}

fn remove_expired(paths: &Paths) {
    let Ok(entries) = fs::read_dir(paths.pending()) else { return };
    let now = now_secs();
    for entry in entries.flatten() {
        let expired = fs::read_to_string(entry.path()).ok().and_then(|t| t.trim().parse::<u64>().ok()).is_none_or(|e| e <= now);
        if expired {
            let _ = fs::remove_file(entry.path());
        }
    }
}

/// The QR code as terminal text (two rows of modules per line).
pub fn qr(url: &str) -> anyhow::Result<String> {
    let code = QrCode::new(url.as_bytes())?;
    Ok(code.render::<Dense1x2>().dark_color(Dense1x2::Light).light_color(Dense1x2::Dark).quiet_zone(true).build())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_works_once() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::at(dir.path().into());
        let id = iroh::SecretKey::generate().public();
        let ticket = create(&paths, EndpointAddr::new(id), "box".into()).unwrap();
        assert!(!redeem(&paths, &[0; 32]));
        assert!(redeem(&paths, &ticket.secret));
        assert!(!redeem(&paths, &ticket.secret));
    }

    #[test]
    fn expired_secret_fails() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::at(dir.path().into());
        let id = iroh::SecretKey::generate().public();
        let ticket = create(&paths, EndpointAddr::new(id), "box".into()).unwrap();
        fs::write(paths.pending().join(hash_hex(&ticket.secret)), (now_secs() - 1).to_string()).unwrap();
        assert!(!redeem(&paths, &ticket.secret));
    }
}
