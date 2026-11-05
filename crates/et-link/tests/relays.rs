//! The app library on machines with their own relays: pairing by the
//! ticket's relay with no address lookup, and a paired phone that follows a
//! machine to a new relay through the network document (no new pairing).
use std::sync::Arc;
use std::time::Duration;

use et_link::Link;
use et_proto::network::Kind;
use et_proto::ALPN;
use etd::config::Paths;
use etd::{network, pair, server};
use iroh::{Endpoint, RelayUrl, SecretKey};

#[path = "../../etd/tests/support/relay.rs"]
mod relay;
use relay::{builder, Dns, Relay};

struct Machine {
    name: &'static str,
    endpoint: Endpoint,
    state: Arc<server::State>,
    paths: Paths,
    _router: iroh::protocol::Router,
    watcher: tokio::task::JoinHandle<()>,
}

impl Drop for Machine {
    fn drop(&mut self) {
        self.watcher.abort();
    }
}

impl Machine {
    /// As `etd` starts with `ETD_RELAYS` = `relays`.
    async fn start(name: &'static str, dir: &tempfile::TempDir, key: SecretKey, relays: &[RelayUrl], dns: &Dns) -> Self {
        let paths = Paths::at(dir.path().into());
        let endpoint = builder(relays, dns).secret_key(key).alpns(vec![ALPN.to_vec()]).bind().await.unwrap();
        network::ensure(&paths, endpoint.id(), name).unwrap();
        let env = [("HOME".to_string(), dir.path().display().to_string())].into_iter().collect();
        let state = server::State::new(paths.clone(), env);
        state.set_relays(relays.to_vec());
        let router = server::serve(endpoint.clone(), state.clone());
        let watcher = tokio::spawn(server::watch(endpoint.clone(), state.clone(), "test".into()));
        Self { name, endpoint, state, paths, _router: router, watcher }
    }

    fn invite(&self, kind: Kind) -> String {
        pair::invite_url(&self.paths, &self.state.public_addr(&self.endpoint), self.name, kind).unwrap()
    }

    fn relays_of(&self, id: iroh::EndpointId) -> Vec<RelayUrl> {
        network::load(&self.paths).unwrap().unwrap().network.member(&id).map(|m| m.relays.to_vec()).unwrap_or_default()
    }
}

async fn eventually(what: &str, check: impl Fn() -> bool) {
    for _ in 0..150 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("not in time: {what}");
}

/// A first start (no saved relays: n0 mode) pairs with a machine on its
/// own relay: the link moves to that relay and drops n0's address lookup.
#[tokio::test(flavor = "multi_thread")]
async fn first_pairing_moves_the_link_to_the_machines_relay() {
    let relay = Relay::start().await;
    let dns = Dns::default();
    let dir = tempfile::tempdir().unwrap();
    let m = Machine::start("trigkey", &dir, SecretKey::generate(), std::slice::from_ref(&relay.url), &dns).await;

    let phone_dns = Dns::default();
    let endpoint = builder(&[], &phone_dns).secret_key(SecretKey::generate()).bind().await.unwrap();
    assert!(!endpoint.address_lookup().unwrap().is_empty(), "n0 mode before the first pairing");
    let link = Link::from_endpoint(endpoint.clone());
    assert!(link.relays().await.is_empty());
    let paired = link.pair(m.invite(Kind::Device), "iPhone".into()).await.unwrap();
    assert_eq!(paired.relay_url.as_deref(), Some(relay.url.as_str()));
    assert_eq!(link.relays().await, vec![relay.url.to_string()], "the app saves these for the next start");
    assert!(endpoint.address_lookup().unwrap().is_empty(), "no pkarr publishing or lookups after the move");
    let host = link.connect(paired.endpoint_id, paired.relay_url).await.unwrap();
    assert!(host.ping().await.unwrap() >= 0.0);
}

/// The app's later starts (`Link::start` with the saved relays): pair, list
/// the network with each machine's relays, and never look up an n0 name.
#[tokio::test(flavor = "multi_thread")]
async fn pairing_over_own_relay_without_lookup() {
    let relay = Relay::start().await;
    let relays = [relay.url.clone()];
    let dns = Dns::default();
    let dir = tempfile::tempdir().unwrap();
    let m = Machine::start("trigkey", &dir, SecretKey::generate(), &relays, &dns).await;
    let me = m.endpoint.id();
    eventually("the machine's record lists its relay", || m.relays_of(me) == relays).await;

    let endpoint = builder(&relays, &dns).secret_key(SecretKey::generate()).bind().await.unwrap();
    assert!(endpoint.address_lookup().unwrap().is_empty());
    let link = Link::from_endpoint_with(endpoint, relays.to_vec());
    let paired = link.pair(m.invite(Kind::Device), "iPhone".into()).await.unwrap();
    let host = link.connect(paired.endpoint_id.clone(), paired.relay_url.clone()).await.unwrap();
    let net = host.network().await.unwrap();
    let machine = net.members.iter().find(|x| x.is_machine).unwrap();
    assert_eq!(machine.relay_urls, vec![relay.url.to_string()]);
    let phone = net.members.iter().find(|x| !x.is_machine).unwrap();
    assert!(phone.relay_urls.is_empty(), "a device has no relays of its own");
    assert_eq!(link.relays().await, vec![relay.url.to_string()]);
    assert!(dns.n0().is_empty(), "looked up n0 names: {:?}", dns.n0());
}

/// Two machines on relay one, a phone paired with the first. The first
/// moves to relay two (its `ETD_RELAYS` changed), and relay one goes away.
/// The phone learns the move from the network document of the second
/// machine, and reaches the first again with its old saved relay URL.
#[tokio::test(flavor = "multi_thread")]
async fn a_paired_phone_follows_a_machine_to_a_new_relay() {
    let (mut one, two) = (Relay::start().await, Relay::start().await);
    let dns = Dns::default();
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let key_a = SecretKey::generate();
    let a = Machine::start("trigkey", &dir_a, key_a.clone(), std::slice::from_ref(&one.url), &dns).await;
    let b = Machine::start("gmktec", &dir_b, SecretKey::generate(), std::slice::from_ref(&one.url), &dns).await;
    network::join(&b.endpoint, &b.paths, &a.invite(Kind::Machine), "gmktec").await.unwrap();
    let (ia, ib) = (a.endpoint.id(), b.endpoint.id());
    eventually("both machines list relay one", || b.relays_of(ia) == [one.url.clone()] && a.relays_of(ib) == [one.url.clone()]).await;

    // The phone pairs with a (and saves a's relay URL, as the app does).
    let endpoint = builder(std::slice::from_ref(&one.url), &dns).secret_key(SecretKey::generate()).bind().await.unwrap();
    let link = Link::from_endpoint_with(endpoint, vec![one.url.clone()]);
    let paired = link.pair(a.invite(Kind::Device), "iPhone".into()).await.unwrap();
    let saved = paired.relay_url.clone();
    assert_eq!(saved.as_deref(), Some(one.url.as_str()));
    let phone_id: iroh::EndpointId = link.endpoint_id().parse().unwrap();
    eventually("b knows the phone", || network::load(&b.paths).unwrap().unwrap().network.member(&phone_id).is_some()).await;

    // a moves to relay two.
    a.endpoint.close().await;
    drop(a);
    let _a = Machine::start("trigkey", &dir_a, key_a, std::slice::from_ref(&two.url), &dns).await;
    eventually("b hears that a moved", || b.relays_of(ia) == [two.url.clone()]).await;

    // The phone asks b for the network (b is still on relay one).
    let net_b = b.relays_of(ib);
    let to_b = link.connect(ib.to_string(), net_b.first().map(|u| u.to_string())).await.unwrap();
    let net = to_b.network().await.unwrap();
    let member_a = net.members.iter().find(|m| m.endpoint_id == ia.to_string()).unwrap();
    assert_eq!(member_a.relay_urls, vec![two.url.to_string()]);
    let mut relays = link.relays().await;
    relays.sort();
    let mut want = vec![one.url.to_string(), two.url.to_string()];
    want.sort();
    assert_eq!(relays, want, "the link follows the network's relays");

    // Relay one goes away. The app still has a's old URL; it works.
    to_b.close();
    one.stop().await;
    let to_a = tokio::time::timeout(Duration::from_secs(30), link.connect(ia.to_string(), saved)).await.unwrap().unwrap();
    assert!(to_a.ping().await.unwrap() >= 0.0);
    assert!(dns.n0().is_empty(), "looked up n0 names: {:?}", dns.n0());
}
