//! One-time invite secrets. `etd invite` writes the hash of a secret and the
//! kind of member it admits to `pending/`; the daemon removes it when the new
//! member redeems it. The two share files only.
use std::fs;
use std::time::Duration;

use anyhow::Context;
use et_proto::network::Kind;
use et_proto::Ticket;
use iroh::{EndpointAddr, TransportAddr};
use qrcode::render::unicode::Dense1x2;
use qrcode::QrCode;
use sha2::{Digest, Sha256};

use crate::config::{now_secs, Paths};

pub const TTL: Duration = Duration::from_secs(10 * 60);

fn hash_hex(secret: &[u8; 32]) -> String {
    Sha256::digest(secret)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Machine => "machine",
        Kind::Device => "device",
    }
}

/// Makes a secret for one `kind` member, valid for `TTL`, and the ticket
/// that carries it. The kind stays here, not in the ticket.
pub fn create(
    paths: &Paths,
    addr: EndpointAddr,
    host_name: String,
    kind: Kind,
) -> anyhow::Result<Ticket> {
    let mut secret = [0u8; 32];
    getrandom::fill(&mut secret).map_err(|e| anyhow::anyhow!("random: {e}"))?;
    let expires = now_secs() + TTL.as_secs();
    let dir = paths.pending();
    fs::create_dir_all(&dir)?;
    fs::write(
        dir.join(hash_hex(&secret)),
        format!("{expires} {}", kind_name(kind)),
    )
    .context("write pending secret")?;
    Ok(Ticket {
        addr,
        secret,
        expires,
        host_name,
    })
}

/// The kind of member `secret` admits, if it is pending and not expired. It
/// can be used once only.
pub fn redeem(paths: &Paths, secret: &[u8; 32]) -> Option<Kind> {
    remove_expired(paths);
    let path = paths.pending().join(hash_hex(secret));
    let text = fs::read_to_string(&path).ok()?;
    // Removed first: a second try with the same secret fails even if this one does.
    fs::remove_file(&path).ok()?;
    let mut fields = text.split_whitespace();
    let expires: u64 = fields.next()?.parse().ok()?;
    if expires <= now_secs() {
        return None;
    }
    // Secrets from before kinds existed admit a device.
    Some(match fields.next() {
        Some("machine") => Kind::Machine,
        _ => Kind::Device,
    })
}

fn remove_expired(paths: &Paths) {
    let Ok(entries) = fs::read_dir(paths.pending()) else {
        return;
    };
    let now = now_secs();
    for entry in entries.flatten() {
        let expires = fs::read_to_string(entry.path()).ok().and_then(|t| {
            t.split_whitespace()
                .next()
                .and_then(|e| e.parse::<u64>().ok())
        });
        if expires.is_none_or(|e| e <= now) {
            let _ = fs::remove_file(entry.path());
        }
    }
}

/// The invite link for `addr`. Only the relay goes in: direct addresses
/// (bridges, VPNs) make the QR code too big, and hole punching finds the
/// direct path after the first connect.
pub fn invite_url(
    paths: &Paths,
    addr: &EndpointAddr,
    host_name: &str,
    kind: Kind,
) -> anyhow::Result<String> {
    let addr =
        EndpointAddr::new(addr.id).with_addrs(addr.relay_urls().cloned().map(TransportAddr::Relay));
    create(paths, addr, host_name.to_string(), kind)?.to_url()
}

/// Makes an invite and prints it: a QR code with instructions, or only the link.
pub fn print(
    paths: &Paths,
    addr: &EndpointAddr,
    host_name: &str,
    kind: Kind,
    url_only: bool,
) -> anyhow::Result<()> {
    let url = invite_url(paths, addr, host_name, kind)?;
    if url_only {
        println!("{url}");
        return Ok(());
    }
    let minutes = TTL.as_secs() / 60;
    match kind {
        Kind::Device => {
            println!("{}", qr(&url)?);
            println!("Scan with the iPhone camera to add it to {host_name}'s network.");
            println!("Valid for {minutes} minutes, for one device.\n");
            println!("Or paste this link in E.T.:\n{url}\n");
        }
        Kind::Machine => {
            println!("On the new machine, run (valid for {minutes} minutes, for one machine):\n");
            println!("  etd join {url}\n");
        }
    }
    Ok(())
}

/// The QR code as terminal text (two rows of modules per line).
pub fn qr(url: &str) -> anyhow::Result<String> {
    let code = QrCode::new(url.as_bytes())?;
    Ok(code
        .render::<Dense1x2>()
        .dark_color(Dense1x2::Light)
        .light_color(Dense1x2::Dark)
        .quiet_zone(true)
        .build())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr() -> EndpointAddr {
        EndpointAddr::new(iroh::SecretKey::generate().public())
    }

    #[test]
    fn secret_works_once_and_keeps_its_kind() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::at(dir.path().into());
        let device = create(&paths, addr(), "box".into(), Kind::Device).unwrap();
        let machine = create(&paths, addr(), "box".into(), Kind::Machine).unwrap();
        assert_eq!(redeem(&paths, &[0; 32]), None);
        assert_eq!(redeem(&paths, &machine.secret), Some(Kind::Machine));
        assert_eq!(redeem(&paths, &device.secret), Some(Kind::Device));
        assert_eq!(redeem(&paths, &device.secret), None);
    }

    #[test]
    fn expired_secret_fails() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::at(dir.path().into());
        let ticket = create(&paths, addr(), "box".into(), Kind::Device).unwrap();
        fs::write(
            paths.pending().join(hash_hex(&ticket.secret)),
            format!("{} device", now_secs() - 1),
        )
        .unwrap();
        assert_eq!(redeem(&paths, &ticket.secret), None);
    }

    #[test]
    fn old_secrets_admit_a_device() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::at(dir.path().into());
        let ticket = create(&paths, addr(), "box".into(), Kind::Machine).unwrap();
        fs::write(
            paths.pending().join(hash_hex(&ticket.secret)),
            (now_secs() + 60).to_string(),
        )
        .unwrap();
        assert_eq!(redeem(&paths, &ticket.secret), Some(Kind::Device));
    }
}
