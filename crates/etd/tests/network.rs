//! Three etds in one process: join, sync, concurrent changes, revoke.
//! Offline endpoints that find each other through an in-memory address book.
//! No tmux, no manymux: only the network.
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use et_proto::network::{Kind, Network};
use et_proto::{read_frame, write_frame, Request, Response, ALPN};
use etd::config::Paths;
use etd::{network, pair, server};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{presets, Connection};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};

struct Machine {
    name: &'static str,
    endpoint: Endpoint,
    paths: Paths,
    _dir: tempfile::TempDir,
    _router: iroh::protocol::Router,
    watcher: tokio::task::JoinHandle<()>,
}

impl Machine {
    async fn start(name: &'static str, book: &MemoryLookup) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::at(dir.path().into());
        let endpoint = Endpoint::builder(presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .address_lookup(book.clone())
            .bind()
            .await
            .unwrap();
        book.add_endpoint_info(local_addr(&endpoint));
        // As on a first start: every machine begins with its own network.
        network::ensure(&paths, endpoint.id(), name).unwrap();
        let env = [("HOME".to_string(), dir.path().display().to_string())].into_iter().collect();
        let state = server::State::new(paths.clone(), env);
        let router = server::serve(endpoint.clone(), state.clone());
        let watcher = tokio::spawn(server::watch(endpoint.clone(), state, "test".into()));
        Self { name, endpoint, paths, _dir: dir, _router: router, watcher }
    }

    fn network(&self) -> Network {
        network::load(&self.paths).unwrap().unwrap().network
    }

    fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.network().members().map(|m| m.name.to_string()).collect();
        names.sort();
        names
    }

    fn invite(&self, kind: Kind) -> String {
        pair::invite_url(&self.paths, &local_addr(&self.endpoint), self.name, kind).unwrap()
    }
}

impl Drop for Machine {
    fn drop(&mut self) {
        self.watcher.abort();
    }
}

fn local_addr(endpoint: &Endpoint) -> EndpointAddr {
    let port = endpoint.bound_sockets().iter().find(|a| a.is_ipv4()).unwrap().port();
    EndpointAddr::new(endpoint.id()).with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
}

async fn phone(book: &MemoryLookup) -> Endpoint {
    let endpoint = Endpoint::builder(presets::Minimal).secret_key(SecretKey::generate()).address_lookup(book.clone()).bind().await.unwrap();
    book.add_endpoint_info(local_addr(&endpoint));
    endpoint
}

async fn call(conn: &Connection, request: Request) -> anyhow::Result<Response> {
    let (mut send, mut recv) = conn.open_bi().await?;
    write_frame(&mut send, &request).await?;
    send.finish()?;
    read_frame(&mut recv).await?.ok_or_else(|| anyhow::anyhow!("no answer"))
}

/// Waits until `check` holds on every machine (the watch loop sends every 2 s).
async fn eventually(what: &str, machines: &[&Machine], check: impl Fn(&Machine) -> bool) {
    for _ in 0..100 {
        if machines.iter().all(|m| check(m)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let states: Vec<String> = machines.iter().map(|m| format!("{}: {:?}", m.name, m.names())).collect();
    panic!("not on every machine in time: {what}\n{}", states.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn three_machines_one_network() {
    let book = MemoryLookup::new();
    let a = Machine::start("trigkey", &book).await;
    let b = Machine::start("gmktec", &book).await;
    let c = Machine::start("mac-mini", &book).await;

    // b joins a's network; the invite works once.
    let url = a.invite(Kind::Machine);
    let joined = network::join(&b.endpoint, &b.paths, &url, "gmktec").await.unwrap();
    assert_eq!(joined.network.key, a.network().key);
    assert!(network::load_key(&b.paths).unwrap().is_some(), "a machine gets the network key");
    assert!(network::join(&c.endpoint, &c.paths, &url, "mac-mini").await.is_err(), "used invite");
    assert_eq!(a.names(), ["gmktec", "trigkey"]);

    // A device invite cannot make a machine.
    let device_url = a.invite(Kind::Device);
    let err = network::join(&c.endpoint, &c.paths, &device_url, "mac-mini").await.unwrap_err();
    assert!(format!("{err:#}").contains("for a device"), "{err:#}");

    // A phone joins on a; b learns of it.
    let phone_ep = phone(&book).await;
    let phone_id = phone_ep.id();
    let conn = phone_ep.connect(a.endpoint.id(), ALPN).await.unwrap();
    let secret = et_proto::Ticket::from_url(&a.invite(Kind::Device)).unwrap().secret;
    let reply = call(&conn, Request::Pair { secret, device_name: "iPhone".into() }).await.unwrap();
    assert!(matches!(reply, Response::Paired { .. }), "{reply:?}");
    eventually("the phone is a member", &[&a, &b], |m| m.network().member(&phone_id).is_some()).await;

    // c joins through b (b holds the network key too); a learns of it.
    network::join(&c.endpoint, &c.paths, &b.invite(Kind::Machine), "mac-mini").await.unwrap();
    eventually("three machines", &[&a, &b, &c], |m| m.network().machines().count() == 3).await;

    // The phone, admitted by a, may use b and c too.
    let to_c = phone_ep.connect(c.endpoint.id(), ALPN).await.unwrap();
    match call(&to_c, Request::Network).await.unwrap() {
        Response::NetworkDoc(signed) => assert!(signed.network.member(&phone_id).is_some()),
        other => panic!("{other:?}"),
    }
    // A device may not change the network.
    let doc = network::load(&a.paths).unwrap().unwrap();
    match call(&to_c, Request::NetworkUpdate { network: doc }).await.unwrap() {
        Response::Error(e) => assert!(e.contains("only machines"), "{e}"),
        other => panic!("{other:?}"),
    }
    // A device may invite (D1).
    match call(&to_c, Request::Invite { kind: Kind::Device }).await.unwrap() {
        Response::Invitation { url } => assert!(url.starts_with("et://pair?t=")),
        other => panic!("{other:?}"),
    }

    // Two machines change the network at the same moment: both changes stay,
    // and every machine ends with the same document.
    let d1 = SecretKey::generate().public();
    let d2 = SecretKey::generate().public();
    network::admit(&a.paths, a.endpoint.id(), d1, "iPad", Kind::Device).unwrap();
    network::admit(&b.paths, b.endpoint.id(), d2, "laptop", Kind::Device).unwrap();
    let both = |m: &Machine| m.network().member(&d1).is_some() && m.network().member(&d2).is_some();
    eventually("both concurrent changes", &[&a, &b, &c], both).await;
    eventually("the same document everywhere", &[&a, &b, &c], |m| m.network() == a.network()).await;

    // A revoke on c reaches a, and closes the phone's connection there.
    let removed: Vec<EndpointId> = network::revoke(&c.paths, c.endpoint.id(), "iPhone").unwrap().into_iter().map(|(id, _)| id).collect();
    assert_eq!(removed, [phone_id]);
    eventually("the phone is removed", &[&a, &b, &c], |m| m.network().member(&phone_id).is_none()).await;
    let why = tokio::time::timeout(Duration::from_secs(10), conn.closed()).await.expect("closed after revoke");
    assert!(format!("{why}").contains("revoked"), "{why}");
    // And it cannot come back without a new invite.
    let again = phone_ep.connect(b.endpoint.id(), ALPN).await.unwrap();
    assert!(call(&again, Request::Network).await.is_err(), "not a member any more");

    // A machine already in a network with others cannot join another one.
    let other = Machine::start("stranger", &book).await;
    assert!(network::join(&a.endpoint, &a.paths, &other.invite(Kind::Machine), "trigkey").await.is_err());
}
