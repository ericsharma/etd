//! Which relays an endpoint uses, and whether it uses n0's services.
//!
//! - No relays set: `presets::N0`, as before: n0's public relays, and the
//!   endpoint's address published to and looked up in n0's DNS (pkarr).
//! - Relays set (`ETD_RELAYS`, or the relays of a ticket or network): only
//!   those relays, and no address lookup at all. Every dial then carries the
//!   relay URLs (from the ticket or the member's record), so nothing needs
//!   n0, and nothing contacts it.
use anyhow::{bail, Context};
use iroh::endpoint::{presets, Builder};
use iroh::{Endpoint, RelayMode, RelayUrl};

/// The domain of every n0 service iroh 1.4 knows: the relays
/// (`*.relay.n0.iroh.link`, `*.staging-relay.n0.iroh.link`) and the DNS and
/// pkarr server (`dns.iroh.link`, `staging-dns.iroh.link`).
pub const N0_DOMAIN: &str = "iroh.link";

/// True for a host name in n0's domain.
pub fn is_n0_host(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    host == N0_DOMAIN || host.ends_with(&format!(".{N0_DOMAIN}"))
}

/// True for one of n0's relays.
pub fn is_n0(url: &RelayUrl) -> bool {
    url.host_str().is_some_and(is_n0_host)
}

/// Relay URLs from a setting: separated by commas or white space. Empty:
/// none (n0's). A URL of n0's own is refused: with it, n0 would be used
/// without its address lookup, which is neither mode.
pub fn parse(text: &str) -> anyhow::Result<Vec<RelayUrl>> {
    let mut relays: Vec<RelayUrl> = Vec::new();
    for word in text.split(|c: char| c == ',' || c.is_whitespace()).filter(|w| !w.is_empty()) {
        let url: RelayUrl = word.parse().with_context(|| format!("not a relay URL: {word}"))?;
        if !matches!(url.scheme(), "http" | "https") {
            bail!("a relay URL starts with https:// (or http:// for a test relay): {word}");
        }
        if is_n0(&url) {
            bail!("{word} is one of n0's relays; leave the relay setting empty to use n0's");
        }
        if !relays.contains(&url) {
            relays.push(url);
        }
    }
    Ok(relays)
}

/// The endpoint builder for `relays`: `presets::N0` when empty, else those
/// relays only and no address lookup (no pkarr publishing, no DNS lookups
/// of endpoint ids).
pub fn builder(relays: &[RelayUrl]) -> Builder {
    if relays.is_empty() {
        return Endpoint::builder(presets::N0);
    }
    // `Minimal` sets only the crypto provider: no address lookup, and the
    // relay mode is ours. A relay's QUIC address discovery (port 7842) is
    // tried and may fail (a relay behind an HTTP proxy); nothing needs it.
    Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Custom(relays.iter().cloned().collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_n0() {
        assert!(parse("").unwrap().is_empty());
        assert!(parse("  ,\n").unwrap().is_empty());
        let r = parse("https://relay.one.example, https://relay.two.example/\nhttps://relay.one.example").unwrap();
        assert_eq!(r.len(), 2);
        assert!(parse("https://use1-1.relay.n0.iroh.link./").is_err(), "n0's relays are not a custom setting");
        assert!(parse("ftp://x.example").is_err());
        assert!(parse("not a url").is_err());
        assert!(is_n0_host("dns.iroh.link"));
        assert!(is_n0_host("euc1-1.relay.n0.iroh.link."));
        assert!(!is_n0_host("iroh.link.example"));
        assert!(!is_n0_host("notiroh.link"));
        for url in iroh::defaults::prod::default_relay_map().urls::<Vec<_>>() {
            assert!(is_n0(&url), "{url}");
        }
    }
}
