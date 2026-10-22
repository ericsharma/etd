//! This machine's copy of the network (et-proto `network`): files, the first
//! start, joining, and documents from other machines.
//!
//! Every read-change-write holds `network.lock`, so the daemon and a CLI
//! command (`etd revoke`) cannot overwrite each other's change.
use std::fs;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;

use anyhow::{bail, Context};
use et_proto::network::{Kind, Network, Signed, State};
use et_proto::{read_frame, write_frame, Request, Response, Ticket, ALPN};
use iroh::{Endpoint, EndpointId, SecretKey};
use tracing::{debug, info, warn};

use crate::config::{self, Paths};

/// An exclusive lock on `network.lock` until dropped (closing the file
/// releases it).
struct Lock {
    _file: fs::File,
}

impl Lock {
    fn take(paths: &Paths) -> anyhow::Result<Self> {
        fs::create_dir_all(&paths.config)?;
        let file = fs::OpenOptions::new().create(true).truncate(false).write(true).mode(0o600).open(paths.network_lock())?;
        // SAFETY: flock on a file descriptor we own; it is released on close.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
            bail!("lock {}: {}", paths.network_lock().display(), std::io::Error::last_os_error());
        }
        Ok(Self { _file: file })
    }
}

/// The network document, checked. `None` before the first start.
pub fn load(paths: &Paths) -> anyhow::Result<Option<Signed>> {
    let Some(signed) = config::read_json::<Signed>(&paths.network())? else { return Ok(None) };
    signed.verify().with_context(|| format!("{}", paths.network().display()))?;
    Ok(Some(signed))
}

fn save(paths: &Paths, signed: &Signed) -> anyhow::Result<()> {
    config::write_json(&paths.network(), signed)
}

pub fn load_key(paths: &Paths) -> anyhow::Result<Option<SecretKey>> {
    match fs::read(paths.network_key()) {
        Ok(bytes) => {
            let bytes: [u8; 32] = bytes.try_into().ok().context("network.key: not a 32-byte key")?;
            Ok(Some(SecretKey::from_bytes(&bytes)))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).context("read network.key"),
    }
}

fn save_key(paths: &Paths, key: &SecretKey) -> anyhow::Result<()> {
    let tmp = paths.network_key().with_extension("tmp");
    let mut file = fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&tmp)?;
    file.write_all(&key.to_bytes())?;
    fs::rename(&tmp, paths.network_key())?;
    Ok(())
}

/// The network, made on the first start: this machine makes the network key
/// and becomes the first member, and the devices paired before the network
/// existed (`clients.json`) become members, so none must pair again.
pub fn ensure(paths: &Paths, me: EndpointId, host_name: &str) -> anyhow::Result<Signed> {
    let _lock = Lock::take(paths)?;
    if let Some(signed) = load(paths)? {
        return Ok(signed);
    }
    let key = SecretKey::generate();
    let mut network = Network::new(&key, host_name, me, host_name);
    let clients = config::load_clients(paths)?;
    for client in &clients {
        network.set(client.id, State::Member { name: client.name.clone(), kind: Kind::Device }, me);
    }
    let signed = network.sign(&key)?;
    save_key(paths, &key)?;
    save(paths, &signed)?;
    if !clients.is_empty() {
        fs::rename(paths.clients(), paths.clients().with_extension("json.migrated"))?;
    }
    info!("made network {} with {} member(s)", short(&signed.network.key), signed.network.members().count());
    Ok(signed)
}

fn short(key: &iroh::PublicKey) -> String {
    key.fmt_short().to_string()
}

/// Changes the network on this machine (it must hold the network key), signs
/// it, and saves it. Returns the new document.
pub fn change(paths: &Paths, f: impl FnOnce(&mut Network) -> anyhow::Result<()>) -> anyhow::Result<Signed> {
    let _lock = Lock::take(paths)?;
    let signed = load(paths)?.context("no network yet; start etd once")?;
    let key = load_key(paths)?.context("this machine has no network key, so it cannot change the network")?;
    let mut network = signed.network;
    f(&mut network)?;
    let signed = network.sign(&key)?;
    save(paths, &signed)?;
    Ok(signed)
}

/// A name for a new member: machines need a unique name (`etd-ssh` and
/// manymux address them by it); a device with a taken name gets " 2", " 3"…
pub fn unique_name(network: &Network, wanted: &str, kind: Kind) -> anyhow::Result<String> {
    let wanted = wanted.trim();
    let taken = |name: &str| network.members().any(|m| m.name == name);
    if wanted.is_empty() {
        bail!("a member needs a name");
    }
    if !taken(wanted) {
        return Ok(wanted.into());
    }
    if kind == Kind::Machine {
        bail!("a member called {wanted} already exists; give this machine another name (etd join --name …)");
    }
    Ok((2..).map(|n| format!("{wanted} {n}")).find(|name| !taken(name)).expect("an unused number"))
}

/// Adds a member that redeemed an invite. Returns the new document.
pub fn admit(paths: &Paths, me: EndpointId, id: EndpointId, name: &str, kind: Kind) -> anyhow::Result<Signed> {
    change(paths, |network| {
        if network.member(&id).is_some() {
            return Ok(());
        }
        let name = unique_name(network, name, kind)?;
        network.set(id, State::Member { name, kind }, me);
        Ok(())
    })
}

/// Removes members by name or id prefix (machines only: D1). Returns them.
pub fn revoke(paths: &Paths, me: EndpointId, who: &str) -> anyhow::Result<Vec<(EndpointId, String)>> {
    let mut removed = Vec::new();
    change(paths, |network| {
        let found: Vec<(EndpointId, String)> = network.find(who).iter().map(|m| (m.id, m.name.to_string())).collect();
        if found.is_empty() {
            bail!("no member matches {who:?}; see `etd members`");
        }
        if found.iter().any(|(id, _)| *id == me) {
            bail!("{who} is this machine; revoke it from another machine");
        }
        for (id, _) in &found {
            network.set(*id, State::Removed, me);
        }
        removed = found;
        Ok(())
    })?;
    Ok(removed)
}

/// A document from another machine: checks it, merges it, and saves the
/// result signed again. True if this machine's copy changed.
pub fn apply(paths: &Paths, incoming: &Signed) -> anyhow::Result<bool> {
    incoming.verify()?;
    let _lock = Lock::take(paths)?;
    let signed = load(paths)?.context("no network yet")?;
    let mut network = signed.network.clone();
    if !network.merge(&incoming.network)? {
        return Ok(false);
    }
    // A merge makes a document nobody signed yet: sign it with our copy of
    // the network key. A machine without it keeps the newer of the two.
    let signed = match load_key(paths)? {
        Some(key) => network.sign(&key)?,
        None if incoming.network.counter() > signed.network.counter() => incoming.clone(),
        None => return Ok(false),
    };
    save(paths, &signed)?;
    Ok(true)
}

/// Joins the network of the machine that made `url` (`etd invite --machine`),
/// as `name`, with `endpoint` (this machine's key). Replaces this machine's
/// own network, which must have no other members.
pub async fn join(endpoint: &Endpoint, paths: &Paths, url: &str, name: &str) -> anyhow::Result<Signed> {
    let me = endpoint.id();
    if let Some(own) = load(paths)? {
        if own.network.members().any(|m| m.id != me) {
            bail!(
                "this machine is already in a network with other members ({}); \
                 joining another would leave them. Remove ~/.config/etd/network.json first if you mean it",
                own.network.members().map(|m| m.name.to_string()).collect::<Vec<_>>().join(", ")
            );
        }
    }
    let ticket = Ticket::from_url(url)?;
    let conn = endpoint.connect(ticket.addr.clone(), ALPN).await.context("reach the inviting machine")?;
    let (mut send, mut recv) = conn.open_bi().await?;
    write_frame(&mut send, &Request::Join { secret: ticket.secret, name: name.into(), kind: Kind::Machine }).await?;
    send.finish()?;
    let reply = read_frame::<Response>(&mut recv).await?.context("no answer to Join")?;
    conn.close(0u32.into(), b"joined");
    let (signed, key) = match reply {
        Response::Joined { network, network_key: Some(key) } => (network, SecretKey::from_bytes(&key)),
        Response::Joined { network_key: None, .. } => bail!("the invite was for a device, not a machine"),
        Response::Rejected(why) => bail!("{why}"),
        other => bail!("unexpected answer: {other:?}"),
    };
    signed.verify()?;
    if key.public() != signed.network.key {
        bail!("the network key does not match the network");
    }
    if signed.network.member(&me).is_none_or(|m| m.kind != Kind::Machine) {
        bail!("the network document does not list this machine");
    }
    let _lock = Lock::take(paths)?;
    save_key(paths, &key)?;
    save(paths, &signed)?;
    Ok(signed)
}

/// Sends `signed` to every other machine of the network. Errors are logged:
/// a machine that is down gets it on its next contact.
pub async fn push(endpoint: &Endpoint, signed: &Signed) {
    let me = endpoint.id();
    let mut tasks = tokio::task::JoinSet::new();
    for id in signed.network.machines().map(|m| m.id).filter(|id| *id != me) {
        let endpoint = endpoint.clone();
        let signed = signed.clone();
        tasks.spawn(async move {
            match push_one(&endpoint, id, signed).await {
                Ok(()) => debug!("network sent to {}", id.fmt_short()),
                Err(e) => warn!("network not sent to {}: {e:#}", id.fmt_short()),
            }
        });
    }
    tasks.join_all().await;
}

async fn push_one(endpoint: &Endpoint, id: EndpointId, signed: Signed) -> anyhow::Result<()> {
    let conn = tokio::time::timeout(std::time::Duration::from_secs(15), endpoint.connect(id, ALPN)).await.context("timed out")??;
    let (mut send, mut recv) = conn.open_bi().await?;
    write_frame(&mut send, &Request::NetworkUpdate { network: signed }).await?;
    send.finish()?;
    let reply = read_frame::<Response>(&mut recv).await?;
    conn.close(0u32.into(), b"sent");
    match reply {
        Some(Response::Pong) => Ok(()),
        Some(other) => bail!("{other:?}"),
        None => bail!("no answer"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths() -> (tempfile::TempDir, Paths) {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::at(dir.path().into());
        (dir, paths)
    }

    #[test]
    fn first_start_moves_paired_devices_in() {
        let (_dir, paths) = paths();
        let me = SecretKey::generate().public();
        let phone = SecretKey::generate().public();
        config::add_client(&paths, phone, "iPhone").unwrap();
        let signed = ensure(&paths, me, "trigkey").unwrap();
        let names: Vec<_> = signed.network.members().map(|m| (m.name.to_string(), m.kind)).collect();
        assert!(names.contains(&("trigkey".into(), Kind::Machine)));
        assert!(names.contains(&("iPhone".into(), Kind::Device)));
        assert!(!paths.clients().exists(), "clients.json is moved aside");
        assert!(load_key(&paths).unwrap().is_some());
        // A second start keeps the same network.
        assert_eq!(ensure(&paths, me, "trigkey").unwrap(), signed);
    }

    #[test]
    fn names_and_revokes() {
        let (_dir, paths) = paths();
        let me = SecretKey::generate().public();
        ensure(&paths, me, "trigkey").unwrap();
        let p1 = SecretKey::generate().public();
        let p2 = SecretKey::generate().public();
        admit(&paths, me, p1, "iPhone", Kind::Device).unwrap();
        let signed = admit(&paths, me, p2, "iPhone", Kind::Device).unwrap();
        assert_eq!(signed.network.member(&p2).unwrap().name, "iPhone 2");
        let m = SecretKey::generate().public();
        assert!(admit(&paths, me, m, "trigkey", Kind::Machine).is_err(), "machine names are unique");
        assert!(revoke(&paths, me, "trigkey").is_err(), "not this machine");
        assert_eq!(revoke(&paths, me, "iPhone 2").unwrap().len(), 1);
        assert!(load(&paths).unwrap().unwrap().network.member(&p2).is_none());
    }

    #[test]
    fn apply_merges_and_refuses_forgeries() {
        let (_dir_a, a) = paths();
        let (_dir_b, b) = paths();
        let ma = SecretKey::generate().public();
        let mb = SecretKey::generate().public();
        ensure(&a, ma, "trigkey").unwrap();
        // b has the same network (as after a join): copy the files.
        admit(&a, ma, mb, "gmktec", Kind::Machine).unwrap();
        fs::create_dir_all(&b.config).unwrap();
        fs::copy(a.network(), b.network()).unwrap();
        save_key(&b, &load_key(&a).unwrap().unwrap()).unwrap();
        // Both change at once; each applies the other's.
        let phone = SecretKey::generate().public();
        let laptop = SecretKey::generate().public();
        let from_a = admit(&a, ma, phone, "iPhone", Kind::Device).unwrap();
        let from_b = admit(&b, mb, laptop, "laptop", Kind::Machine).unwrap();
        assert!(apply(&a, &from_b).unwrap());
        assert!(apply(&b, &from_a).unwrap());
        assert_eq!(load(&a).unwrap().unwrap().network, load(&b).unwrap().unwrap().network);
        assert!(!apply(&a, &from_b).unwrap(), "nothing new");
        // A document signed with another key is refused.
        let other = Network::new(&SecretKey::generate(), "x", ma, "x").sign(&SecretKey::generate());
        assert!(other.is_err());
        let mut forged = from_a.clone();
        forged.network.set(SecretKey::generate().public(), State::Member { name: "evil".into(), kind: Kind::Machine }, ma);
        assert!(apply(&a, &forged).is_err());
    }
}
