//! Test support: a relay in this process, and a DNS resolver that records
//! every name looked up (to prove that nothing contacts n0).
//!
//! The relay serves plain HTTP on localhost (iroh speaks `ws://` to an
//! `http://` relay URL): no certificates, no QUIC address discovery.
#![allow(dead_code)]
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex};

use iroh::dns::{BoxIter, DnsError, DnsResolver, Resolver, TxtRecordData};
use iroh::endpoint::Builder;
use iroh::RelayUrl;
use iroh_relay::server::{RelayConfig, Server, ServerConfig};
use n0_future::boxed::BoxFuture;

/// A relay on localhost, until dropped.
pub struct Relay {
    pub url: RelayUrl,
    server: Option<Server>,
}

impl Relay {
    pub async fn start() -> Self {
        let mut config = ServerConfig::default();
        config.relay = Some(RelayConfig::new((Ipv4Addr::LOCALHOST, 0)));
        let server = Server::spawn(config).await.unwrap();
        let url = format!("http://{}", server.http_addr().unwrap()).parse().unwrap();
        Self { url, server: Some(server) }
    }

    /// Stops it: nobody is reached through it after this.
    pub async fn stop(&mut self) {
        if let Some(server) = self.server.take() {
            server.shutdown().await.unwrap();
        }
    }
}

/// Answers localhost and IP literals; records every name it is asked.
#[derive(Debug, Clone, Default)]
pub struct Dns {
    seen: Arc<Mutex<Vec<String>>>,
}

impl Dns {
    pub fn resolver(&self) -> DnsResolver {
        DnsResolver::custom(self.clone())
    }

    pub fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }

    /// Names in n0's domain that were looked up.
    pub fn n0(&self) -> Vec<String> {
        self.seen().into_iter().filter(|h| et_proto::relays::is_n0_host(h)).collect()
    }

    fn ip(&self, host: &str) -> Option<IpAddr> {
        self.seen.lock().unwrap().push(host.to_string());
        if host.trim_end_matches('.') == "localhost" {
            return Some(Ipv4Addr::LOCALHOST.into());
        }
        host.trim_matches(|c| c == '[' || c == ']').parse().ok()
    }
}

impl Resolver for Dns {
    fn lookup_ipv4(&self, host: String) -> BoxFuture<Result<BoxIter<Ipv4Addr>, DnsError>> {
        let found: Vec<Ipv4Addr> = match self.ip(&host) {
            Some(IpAddr::V4(ip)) => vec![ip],
            _ => vec![],
        };
        Box::pin(async move { Ok(Box::new(found.into_iter()) as BoxIter<Ipv4Addr>) })
    }

    fn lookup_ipv6(&self, host: String) -> BoxFuture<Result<BoxIter<Ipv6Addr>, DnsError>> {
        let found: Vec<Ipv6Addr> = match self.ip(&host) {
            Some(IpAddr::V6(ip)) => vec![ip],
            _ => vec![],
        };
        Box::pin(async move { Ok(Box::new(found.into_iter()) as BoxIter<Ipv6Addr>) })
    }

    fn lookup_txt(&self, host: String) -> BoxFuture<Result<BoxIter<TxtRecordData>, DnsError>> {
        self.seen.lock().unwrap().push(host);
        Box::pin(async move { Ok(Box::new(std::iter::empty()) as BoxIter<TxtRecordData>) })
    }

    fn clear_cache(&self) {}

    fn reset(&self) -> Box<dyn Resolver> {
        Box::new(self.clone())
    }
}

/// etd's endpoint builder for `relays` (as `etd` and `Link::start` make
/// it), with every DNS lookup going to `dns`.
pub fn builder(relays: &[RelayUrl], dns: &Dns) -> Builder {
    et_proto::relays::builder(relays).dns_resolver(dns.resolver())
}
