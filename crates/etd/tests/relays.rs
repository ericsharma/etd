//! Own relays (`ETD_RELAYS`): machines reach each other through a relay in
//! this process, with no address lookup, and nothing looks up an n0 name.
//! With no relays set, etd is on n0's relays and lookup, as before.
use std::time::Duration;

use et_proto::network::{Kind, State as Record};
use et_proto::{read_frame, write_frame, Request, Response, ALPN};
use etd::config::Paths;
use etd::{network, pair, server};
use iroh::endpoint::Connection;
use iroh::{Endpoint, RelayUrl, SecretKey};

#[path = "support/relay.rs"]
mod relay;
use relay::{builder, Dns, Relay};

struct Machine {
    name: &'static str,
    endpoint: Endpoint,
    state: std::sync::Arc<server::State>,
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

    fn network(&self) -> et_proto::network::Network {
        network::load(&self.paths).unwrap().unwrap().network
    }

    /// An invite as `etd invite` makes it: the relays only, no direct address.
    fn invite(&self, kind: Kind) -> String {
        pair::invite_url(&self.paths, &self.state.public_addr(&self.endpoint), self.name, kind).unwrap()
    }
}

async fn call(conn: &Connection, request: Request) -> anyhow::Result<Response> {
    let (mut send, mut recv) = conn.open_bi().await?;
    write_frame(&mut send, &request).await?;
    send.finish()?;
    read_frame(&mut recv).await?.ok_or_else(|| anyhow::anyhow!("no answer"))
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

/// Two machines and a phone on one relay, with no address lookup: the
/// join, the network sync, and `ExecOn` (forwarded by the first machine to
/// the second) all dial with the relays from the ticket or the records.
#[tokio::test(flavor = "multi_thread")]
async fn machines_over_own_relay_without_lookup() {
    let relay = Relay::start().await;
    let relays = [relay.url.clone()];
    let dns = Dns::default();
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = Machine::start("trigkey", &dir_a, SecretKey::generate(), &relays, &dns).await;
    let b = Machine::start("gmktec", &dir_b, SecretKey::generate(), &relays, &dns).await;
    for m in [&a, &b] {
        assert!(m.endpoint.address_lookup().unwrap().is_empty(), "own relays: no address lookup");
    }

    // The invite carries the relay and no direct address.
    let url = a.invite(Kind::Machine);
    let ticket = et_proto::Ticket::from_url(&url).unwrap();
    assert_eq!(ticket.addr.relay_urls().collect::<Vec<_>>(), vec![&relay.url]);
    assert_eq!(ticket.addr.ip_addrs().count(), 0);
    network::join(&b.endpoint, &b.paths, &url, "gmktec").await.unwrap();

    // Each machine writes its relays into its own record; both documents
    // agree (sent between the machines by their records' relays).
    let (ia, ib) = (a.endpoint.id(), b.endpoint.id());
    let listed = |m: &Machine, id| m.network().member(&id).is_some_and(|x| x.relays == relays);
    eventually("both records list the relay, on both machines", || listed(&a, ia) && listed(&a, ib) && listed(&b, ia) && listed(&b, ib))
        .await;
    assert!(matches!(a.network().records[&ib].state, Record::MemberAt { .. }));

    // A phone pairs with a by the ticket alone, then asks a to run a
    // command on b: a dials b at the relay of b's record.
    let phone = builder(&relays, &dns).secret_key(SecretKey::generate()).bind().await.unwrap();
    let ticket = et_proto::Ticket::from_url(&a.invite(Kind::Device)).unwrap();
    let conn = phone.connect(ticket.addr.clone(), ALPN).await.unwrap();
    let reply = call(&conn, Request::Pair { secret: ticket.secret, device_name: "iPhone".into() }).await.unwrap();
    assert!(matches!(reply, Response::Paired { .. }), "{reply:?}");
    let reply = call(&conn, Request::ExecOn { host: "gmktec".into(), script: "echo from-b".into() }).await.unwrap();
    match reply {
        Response::ExecResult { output, exit_code, .. } => {
            assert_eq!(String::from_utf8_lossy(&output).trim(), "from-b");
            assert_eq!(exit_code, Some(0));
        }
        other => panic!("{other:?}"),
    }
    // And the phone reaches b directly, from b's record in the document.
    let doc = match call(&conn, Request::Network).await.unwrap() {
        Response::NetworkDoc(signed) => signed,
        other => panic!("{other:?}"),
    };
    let phone_id = phone.id();
    eventually("b knows the phone", || b.network().member(&phone_id).is_some()).await;
    let to_b = phone.connect(doc.network.addr(ib), ALPN).await.unwrap();
    assert!(matches!(call(&to_b, Request::Ping).await.unwrap(), Response::Pong));

    let seen = dns.seen();
    assert!(dns.n0().is_empty(), "looked up n0 names: {:?}", dns.n0());
    assert!(seen.iter().all(|h| !h.contains("iroh")), "{seen:?}");
}

/// A machine that moves to another relay: its record changes, the other
/// machine learns it from the document and reaches it there.
#[tokio::test(flavor = "multi_thread")]
async fn a_machine_moves_to_another_relay() {
    let (mut one, two) = (Relay::start().await, Relay::start().await);
    let dns = Dns::default();
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let key_b = SecretKey::generate();
    let a = Machine::start("trigkey", &dir_a, SecretKey::generate(), std::slice::from_ref(&one.url), &dns).await;
    let b = Machine::start("gmktec", &dir_b, key_b.clone(), std::slice::from_ref(&one.url), &dns).await;
    network::join(&b.endpoint, &b.paths, &a.invite(Kind::Machine), "gmktec").await.unwrap();
    let ib = b.endpoint.id();
    eventually("a has b's relay", || a.network().member(&ib).is_some_and(|m| m.relays == [one.url.clone()])).await;

    // b restarts on relay two (ETD_RELAYS changed); a hears of it.
    b.endpoint.close().await;
    drop(b);
    let _b = Machine::start("gmktec", &dir_b, key_b, std::slice::from_ref(&two.url), &dns).await;
    eventually("a has b's new relay", || a.network().member(&ib).is_some_and(|m| m.relays == [two.url.clone()])).await;

    // Relay one goes away; a still reaches b, at relay two only.
    one.stop().await;
    let addr = a.network().addr(ib);
    assert_eq!(addr.relay_urls().collect::<Vec<_>>(), vec![&two.url]);
    let conn = tokio::time::timeout(Duration::from_secs(20), a.endpoint.connect(addr, ALPN)).await.unwrap().unwrap();
    assert!(matches!(call(&conn, Request::Ping).await.unwrap(), Response::Pong));
    assert!(dns.n0().is_empty(), "{:?}", dns.n0());
}

/// No relays set: n0's relays and n0's address lookup, as before; the
/// machine's record stays a plain `Member`, and `status.json` says so.
#[tokio::test(flavor = "multi_thread")]
async fn no_relays_set_is_n0_as_before() {
    assert!(et_proto::relays::parse("").unwrap().is_empty());
    let dns = Dns::default();
    let dir = tempfile::tempdir().unwrap();
    let m = Machine::start("trigkey", &dir, SecretKey::generate(), &[], &dns).await;
    assert!(!m.endpoint.address_lookup().unwrap().is_empty(), "n0's address lookup (pkarr, DNS)");
    let me = m.endpoint.id();
    // The watch loop runs (status.json), and leaves the record alone.
    eventually("status.json", || m.paths.status().exists()).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(matches!(m.network().records[&me].state, Record::Member { .. }));
    assert!(m.network().relays().is_empty());
    let status = etd::config::running_status(&m.paths).unwrap().unwrap();
    assert!(status.relays.is_empty());
    // It really is n0: the endpoint looks up n0's names (the lookups fail
    // here, which is what keeps this test offline). This is also the check
    // that the recorder sees n0 contact when there is some.
    eventually("an n0 name is looked up", || !dns.n0().is_empty()).await;
}
