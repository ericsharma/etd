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
use et_proto::network::{Kind, Network, Rotation, Signed, State};
use et_proto::{read_frame, write_frame, Request, Response, Ticket, ALPN};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayUrl, SecretKey};
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
    let Some(signed) = config::read_json::<Signed>(&paths.network())? else {
        return Ok(None);
    };
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
    change_locked(paths, f)
}

/// `change`, for a caller that holds the lock.
fn change_locked(paths: &Paths, f: impl FnOnce(&mut Network) -> anyhow::Result<()>) -> anyhow::Result<Signed> {
    let signed = load(paths)?.context("no network yet; start etd once")?;
    let key = load_key(paths)?.context("this machine has no network key, so it cannot change the network")?;
    let mut network = signed.network;
    f(&mut network)?;
    let signed = network.sign(&key)?;
    save(paths, &signed)?;
    Ok(signed)
}

/// Writes `relays` into this machine's own record if it lists others.
/// True if the document changed (it then goes to the other machines).
pub fn set_own_relays(paths: &Paths, me: EndpointId, relays: &[RelayUrl]) -> anyhow::Result<bool> {
    let _lock = Lock::take(paths)?;
    let Some(signed) = load(paths)? else { return Ok(false) };
    if signed.network.member(&me).is_none_or(|m| m.relays == relays) {
        return Ok(false);
    }
    change_locked(paths, |network| {
        network.set_relays(me, relays, me);
        Ok(())
    })?;
    Ok(true)
}

/// A name for a new member: machines need a unique name (`etd-ssh` and
/// manymux address them by it); a device with a taken name gets " 2", " 3"…
pub fn unique_name(network: &Network, wanted: &str, kind: Kind) -> anyhow::Result<String> {
    let wanted = wanted.trim();
    let taken = |name: &str| network.members().any(|m| m.name == name);
    if wanted.is_empty() {
        bail!("a member needs a name");
    }
    if wanted.chars().any(char::is_control) {
        bail!("a name must not contain control characters");
    }
    // manymux writes a session on another machine as `<machine>/<session>`,
    // and manymux's hosts.toml and etd-ssh take the name as one word.
    if kind == Kind::Machine && !wanted.chars().all(|c| c.is_ascii_alphanumeric() || "._-".contains(c)) {
        bail!("a machine name may have only letters, digits, '.', '_' and '-': {wanted:?}");
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
/// Revoking a machine also moves the network to a new network key, because
/// the revoked machine holds the old one.
pub fn revoke(paths: &Paths, me: EndpointId, who: &str) -> anyhow::Result<Vec<(EndpointId, String)>> {
    let _lock = Lock::take(paths)?;
    let mut removed = Vec::new();
    let mut a_machine = false;
    let signed = change_locked(paths, |network| {
        let found: Vec<(EndpointId, String, Kind)> = network.find(who).iter().map(|m| (m.id, m.name.to_string(), m.kind)).collect();
        if found.is_empty() {
            bail!("no member matches {who:?}; see `etd members`");
        }
        if found.iter().any(|(id, _, _)| *id == me) {
            bail!("{who} is this machine; revoke it from another machine");
        }
        for (id, _, _) in &found {
            network.set(*id, State::Removed, me);
        }
        a_machine = found.iter().any(|(_, _, kind)| *kind == Kind::Machine);
        removed = found.into_iter().map(|(id, name, _)| (id, name)).collect();
        Ok(())
    })?;
    if a_machine {
        let key = SecretKey::generate();
        let mut network = signed.network.clone();
        network.key = key.public();
        let rotation = Rotation { old: signed, new: network.sign(&key)? };
        let mut chain = load_rotations(paths)?;
        chain.push(rotation.clone());
        config::write_json(&paths.rotations(), &chain)?;
        save_key(paths, &key)?;
        save(paths, &rotation.new)?;
        info!("network key changed: {} → {}", short(&rotation.old.network.key), short(&rotation.new.network.key));
    }
    Ok(removed)
}

/// The network's key changes, oldest first (empty if it never changed).
pub fn load_rotations(paths: &Paths) -> anyhow::Result<Vec<Rotation>> {
    Ok(config::read_json(&paths.rotations())?.unwrap_or_default())
}

/// Key changes from another machine (`NetworkRotate`): `chain` is every key
/// change of the network, `key` the secret half of the last new key. This
/// machine moves from its key to the last one. True if its copy changed.
///
/// Refused: a chain that does not pass through this machine's key (two
/// machines changed keys at once: a fork, fixed by hand), and a chain that
/// removes this machine. A chain that ends at a key this machine already left
/// changes nothing (the sender is behind; it gets this machine's chain).
pub fn apply_rotation(paths: &Paths, me: EndpointId, chain: &[Rotation], key: &[u8; 32]) -> anyhow::Result<bool> {
    let last = chain.last().context("an empty key change")?;
    for (i, step) in chain.iter().enumerate() {
        step.verify()?;
        if i > 0 && chain[i - 1].new.network.key != step.old.network.key {
            bail!("the key changes do not follow each other");
        }
    }
    let key = SecretKey::from_bytes(key);
    if key.public() != last.new.network.key {
        bail!("the new network key does not match the new document");
    }
    let _lock = Lock::take(paths)?;
    let local = load(paths)?.context("no network")?;
    let current = local.network.key;
    if current == last.new.network.key {
        // Already moved: only the records can be new.
        return apply_locked(paths, &last.new);
    }
    if load_rotations(paths)?.iter().any(|r| r.old.network.key == last.new.network.key) {
        return Ok(false);
    }
    // Where this machine's key is in the chain.
    if !chain.iter().any(|r| r.old.network.key == current || r.new.network.key == current) {
        bail!(
            "network key fork: this machine has {}, the key changes go {}",
            short(&current),
            chain.iter().map(|r| short(&r.old.network.key)).chain([short(&last.new.network.key)]).collect::<Vec<_>>().join(" → ")
        );
    }
    let mut network = last.new.network.clone();
    if network.member(&me).is_none() {
        bail!("this machine is removed from the network");
    }
    // Keep changes made here under the old key meanwhile, except those by
    // or about a removed member: a machine about to be revoked may have
    // added a helper, or itself again with a high counter.
    let removed = |id: &EndpointId| network.records.get(id).is_some_and(|r| r.state == State::Removed);
    let mine: Vec<_> = local
        .network
        .records
        .iter()
        .filter(|(id, r)| !removed(id) && !removed(&r.stamp.author))
        .filter(|(id, r)| network.records.get(*id).is_none_or(|theirs| r.stamp > theirs.stamp))
        .map(|(id, r)| (*id, r.clone()))
        .collect();
    network.records.extend(mine);
    let signed = network.sign(&key)?;
    config::write_json(&paths.rotations(), &chain)?;
    save_key(paths, &key)?;
    save(paths, &signed)?;
    info!("network key changed: {} → {}", short(&current), short(&last.new.network.key));
    Ok(true)
}

/// A document from another machine: checks it, merges it, and saves the
/// result signed again. True if this machine's copy changed.
pub fn apply(paths: &Paths, incoming: &Signed) -> anyhow::Result<bool> {
    let _lock = Lock::take(paths)?;
    apply_locked(paths, incoming)
}

fn apply_locked(paths: &Paths, incoming: &Signed) -> anyhow::Result<bool> {
    incoming.verify()?;
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
/// True if every machine got it.
pub async fn push(endpoint: &Endpoint, paths: &Paths, signed: &Signed) -> bool {
    let me = endpoint.id();
    let mut tasks = tokio::task::JoinSet::new();
    for id in signed.network.machines().map(|m| m.id).filter(|id| *id != me) {
        let endpoint = endpoint.clone();
        let signed = signed.clone();
        let paths = paths.clone();
        tasks.spawn(async move {
            match push_one(&endpoint, &paths, id, signed).await {
                Ok(()) => {
                    debug!("network sent to {}", id.fmt_short());
                    true
                }
                Err(e) => {
                    warn!("network not sent to {}: {e:#}", id.fmt_short());
                    false
                }
            }
        });
    }
    tasks.join_all().await.into_iter().all(|sent| sent)
}

/// Where to dial member `id`: its relays from this machine's document (none
/// in n0 mode: the address lookup finds it by id).
pub fn addr_of(paths: &Paths, id: EndpointId) -> EndpointAddr {
    match load(paths) {
        Ok(Some(signed)) => signed.network.addr(id),
        _ => EndpointAddr::new(id),
    }
}

/// Sends this machine's document (and the last key change) to one machine.
pub async fn push_to(endpoint: &Endpoint, paths: &Paths, id: EndpointId) -> anyhow::Result<()> {
    let signed = load(paths)?.context("no network")?;
    push_one(endpoint, paths, id, signed).await
}

async fn push_one(endpoint: &Endpoint, paths: &Paths, id: EndpointId, signed: Signed) -> anyhow::Result<()> {
    let conn = tokio::time::timeout(std::time::Duration::from_secs(15), endpoint.connect(signed.network.addr(id), ALPN))
        .await
        .context("timed out")??;
    // A machine that has not moved to the current key gets the key changes
    // first; for one that has, they change nothing.
    let chain = load_rotations(paths)?;
    if chain.last().is_some_and(|r| r.new.network.key == signed.network.key) {
        let key = key_for(paths, id)?.with_context(|| format!("{} is not a machine of the network now", id.fmt_short()))?;
        call(&conn, Request::NetworkRotate { chain, key }).await?;
    }
    call(&conn, Request::NetworkUpdate { network: signed }).await?;
    conn.close(0u32.into(), b"sent");
    Ok(())
}

/// The network key's secret, for machine `id`: only if `id` is a machine of
/// the document on disk now, and the key on disk is that document's key.
/// A revoke writes the new key, then the new document, so a read between
/// the two sees keys that differ and sends nothing; the caller's copy of the
/// document (read earlier) is never trusted for this.
fn key_for(paths: &Paths, id: EndpointId) -> anyhow::Result<Option<[u8; 32]>> {
    let Some(current) = load(paths)? else { return Ok(None) };
    let Some(key) = load_key(paths)? else { return Ok(None) };
    let machine = current.network.member(&id).is_some_and(|m| m.kind == Kind::Machine);
    Ok((machine && key.public() == current.network.key).then(|| key.to_bytes()))
}

async fn call(conn: &iroh::endpoint::Connection, request: Request) -> anyhow::Result<()> {
    let (mut send, mut recv) = conn.open_bi().await?;
    write_frame(&mut send, &request).await?;
    send.finish()?;
    match read_frame::<Response>(&mut recv).await? {
        Some(Response::Pong) => Ok(()),
        Some(other) => bail!("{other:?}"),
        None => bail!("no answer"),
    }
}

/// Asks every other machine for its document and applies it (at start, so a
/// machine that was offline catches up at once, not when one sends to it).
pub async fn pull(endpoint: &Endpoint, paths: &Paths) {
    let me = endpoint.id();
    let Ok(Some(signed)) = load(paths) else {
        return;
    };
    let mut tasks = tokio::task::JoinSet::new();
    for addr in signed.network.machines().filter(|m| m.id != me).map(|m| m.addr()) {
        let (endpoint, paths, id) = (endpoint.clone(), paths.clone(), addr.id);
        tasks.spawn(async move {
            let pulled = async {
                let conn =
                    tokio::time::timeout(std::time::Duration::from_secs(15), endpoint.connect(addr, ALPN)).await.context("timed out")??;
                let (mut send, mut recv) = conn.open_bi().await?;
                write_frame(&mut send, &Request::Network).await?;
                send.finish()?;
                let reply = read_frame::<Response>(&mut recv).await?;
                conn.close(0u32.into(), b"pulled");
                match reply {
                    // Another key: that machine sends the key change itself
                    // (it answers a machine's `Network` with a push).
                    Some(Response::NetworkDoc(doc)) => apply(&paths, &doc).or_else(|e| {
                        debug!("document of {} not applied: {e:#}", id.fmt_short());
                        Ok(false)
                    }),
                    Some(other) => bail!("{other:?}"),
                    None => bail!("no answer"),
                }
            };
            match pulled.await {
                Ok(true) => info!("network updated from {}", id.fmt_short()),
                Ok(false) => {}
                Err(e) => debug!("network not pulled from {}: {e:#}", id.fmt_short()),
            }
        });
    }
    tasks.join_all().await;
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

    /// The network key goes only to a machine of the network as it is on
    /// disk now: not to a revoked one, not to a device, and not while the key
    /// and the document disagree (half way through a revoke).
    #[test]
    fn key_only_for_current_machines() {
        let (_dir, paths) = paths();
        let me = SecretKey::generate().public();
        ensure(&paths, me, "trigkey").unwrap();
        let (gone, kept, phone) = (SecretKey::generate().public(), SecretKey::generate().public(), SecretKey::generate().public());
        admit(&paths, me, gone, "gmktec", Kind::Machine).unwrap();
        admit(&paths, me, kept, "mac-mini", Kind::Machine).unwrap();
        admit(&paths, me, phone, "iPhone", Kind::Device).unwrap();
        let old = load_key(&paths).unwrap().unwrap().to_bytes();
        assert_eq!(key_for(&paths, gone).unwrap(), Some(old));
        assert_eq!(key_for(&paths, phone).unwrap(), None, "a device never gets the key");

        revoke(&paths, me, "gmktec").unwrap();
        let new = load_key(&paths).unwrap().unwrap().to_bytes();
        assert_ne!(new, old, "revoking a machine changes the key");
        assert_eq!(key_for(&paths, gone).unwrap(), None, "the revoked machine gets nothing");
        assert_eq!(key_for(&paths, kept).unwrap(), Some(new));

        // A key on disk that is not the document's (a revoke between its two writes).
        save_key(&paths, &SecretKey::generate()).unwrap();
        assert_eq!(key_for(&paths, kept).unwrap(), None);
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
