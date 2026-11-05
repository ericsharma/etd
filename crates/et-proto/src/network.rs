//! The network: who belongs, signed by the network key (after rayfish's
//! `GroupBlob`). Machines hold the network key's secret half; devices do not,
//! so only machines can change the document.
//!
//! Each member id has one record. Changing a record gives it a stamp one
//! higher than any in the document; merging keeps the record with the higher
//! stamp (counter, then author). So two machines that change the document at
//! the same time both keep their changes, without clocks. A removal is a
//! record too, so it travels like any other change.
use std::collections::BTreeMap;

use anyhow::{bail, Context};
use iroh::{EndpointAddr, EndpointId, PublicKey, RelayUrl, SecretKey, Signature};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    /// Runs etd; holds the network key; may revoke.
    Machine,
    /// A phone or tablet; may invite, may not revoke.
    Device,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum State {
    Member {
        name: String,
        kind: Kind,
    },
    Removed,
    /// A member and the relays it is reached on (since 0.0.26): a machine
    /// with its own relays (`ETD_RELAYS`) writes its record this way, so
    /// the others dial it without an address lookup. A machine on n0's
    /// relays keeps `Member`.
    MemberAt {
        name: String,
        kind: Kind,
        relays: Vec<RelayUrl>,
    },
}

impl State {
    /// A member record, with `relays` (`Member` when there are none).
    pub fn member(name: String, kind: Kind, relays: Vec<RelayUrl>) -> Self {
        if relays.is_empty() {
            Self::Member { name, kind }
        } else {
            Self::MemberAt { name, kind, relays }
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Stamp {
    pub counter: u64,
    pub author: EndpointId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Record {
    pub state: State,
    pub stamp: Stamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Network {
    /// The network key's public half. Also the network's id.
    pub key: PublicKey,
    pub name: String,
    pub records: BTreeMap<EndpointId, Record>,
}

/// A network document and the network key's signature over it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Signed {
    pub network: Network,
    pub signature: Signature,
}

/// One key change, after a machine was revoked: `old` (the document with
/// that machine removed, signed by the old key) and `new` (the same document
/// with the new key, signed by it).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Rotation {
    pub old: Signed,
    pub new: Signed,
}

impl Rotation {
    /// Both signed, and `new` is `old` under another key.
    pub fn verify(&self) -> anyhow::Result<()> {
        self.old.verify()?;
        self.new.verify()?;
        let mut same = self.new.network.clone();
        same.key = self.old.network.key;
        if same != self.old.network || self.new.network.key == self.old.network.key {
            bail!("a key change must keep the document and change the key");
        }
        Ok(())
    }
}

/// Counters above this are refused: `set` must always find a higher one.
const MAX_COUNTER: u64 = u64::MAX / 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member<'a> {
    pub id: EndpointId,
    pub name: &'a str,
    pub kind: Kind,
    /// Where it is reached: empty for a member on n0's relays (found by
    /// its id) and for devices.
    pub relays: &'a [RelayUrl],
}

impl Member<'_> {
    /// The address to dial: the id and the member's relays.
    pub fn addr(&self) -> EndpointAddr {
        self.relays.iter().fold(EndpointAddr::new(self.id), |addr, url| addr.with_relay_url(url.clone()))
    }
}

impl Network {
    /// A new network with one member: the machine that makes it.
    pub fn new(key: &SecretKey, name: &str, founder: EndpointId, founder_name: &str) -> Self {
        let mut network = Self { key: key.public(), name: name.into(), records: BTreeMap::new() };
        network.set(founder, State::Member { name: founder_name.into(), kind: Kind::Machine }, founder);
        network
    }

    /// The highest counter in the document.
    pub fn counter(&self) -> u64 {
        self.records.values().map(|r| r.stamp.counter).max().unwrap_or(0)
    }

    /// Changes `id`'s record, with a stamp above every other.
    pub fn set(&mut self, id: EndpointId, state: State, author: EndpointId) {
        let stamp = Stamp { counter: self.counter() + 1, author };
        self.records.insert(id, Record { state, stamp });
    }

    pub fn member(&self, id: &EndpointId) -> Option<Member<'_>> {
        match &self.records.get(id)?.state {
            State::Member { name, kind } => Some(Member { id: *id, name, kind: *kind, relays: &[] }),
            State::MemberAt { name, kind, relays } => Some(Member { id: *id, name, kind: *kind, relays }),
            State::Removed => None,
        }
    }

    /// The address to dial `id` at: its relays from its record, if any.
    pub fn addr(&self, id: EndpointId) -> EndpointAddr {
        self.member(&id).map(|m| m.addr()).unwrap_or_else(|| EndpointAddr::new(id))
    }

    /// The relays of every machine, in order, once each: what a device's
    /// endpoint uses. Empty: the network is on n0's relays.
    pub fn relays(&self) -> Vec<RelayUrl> {
        let mut all: Vec<RelayUrl> = Vec::new();
        for url in self.machines().flat_map(|m| m.relays.iter()) {
            if !all.contains(url) {
                all.push(url.clone());
            }
        }
        all
    }

    /// Sets member `id`'s relays (a new record, by `author`) if they
    /// differ. True if the document changed.
    pub fn set_relays(&mut self, id: EndpointId, relays: &[RelayUrl], author: EndpointId) -> bool {
        let Some(m) = self.member(&id) else { return false };
        if m.relays == relays {
            return false;
        }
        let state = State::member(m.name.to_string(), m.kind, relays.to_vec());
        self.set(id, state, author);
        true
    }

    pub fn members(&self) -> impl Iterator<Item = Member<'_>> {
        self.records.keys().filter_map(|id| self.member(id))
    }

    pub fn machines(&self) -> impl Iterator<Item = Member<'_>> {
        self.members().filter(|m| m.kind == Kind::Machine)
    }

    /// Members whose name is `who`, or whose id starts with `who` (8+ chars).
    pub fn find(&self, who: &str) -> Vec<Member<'_>> {
        self.members().filter(|m| m.name == who || (who.len() >= 8 && m.id.to_string().starts_with(who))).collect()
    }

    /// Takes every record of `other` that has a higher stamp. True if
    /// anything changed. Both must be the same network.
    pub fn merge(&mut self, other: &Network) -> anyhow::Result<bool> {
        if other.key != self.key {
            bail!("a document of another network");
        }
        if other.counter() > MAX_COUNTER {
            bail!("a document with a counter out of range");
        }
        let mut changed = false;
        for (id, theirs) in &other.records {
            let newer = self.records.get(id).is_none_or(|ours| theirs.stamp > ours.stamp);
            if newer {
                self.records.insert(*id, theirs.clone());
                changed = true;
            }
        }
        Ok(changed)
    }

    fn signing_bytes(&self) -> anyhow::Result<Vec<u8>> {
        // A BTreeMap keeps the encoding the same for the same content.
        Ok(postcard::to_stdvec(self)?)
    }

    pub fn sign(self, key: &SecretKey) -> anyhow::Result<Signed> {
        if key.public() != self.key {
            bail!("not this network's key");
        }
        let signature = key.sign(&self.signing_bytes()?);
        Ok(Signed { network: self, signature })
    }
}

impl Signed {
    /// Checks that the network key signed this document.
    pub fn verify(&self) -> anyhow::Result<()> {
        let bytes = self.network.signing_bytes()?;
        self.network.key.verify(&bytes, &self.signature).ok().context("the network document's signature is not valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(seed: u8) -> (SecretKey, EndpointId) {
        let key = SecretKey::from_bytes(&[seed; 32]);
        let id = key.public();
        (key, id)
    }

    #[test]
    fn sign_and_verify() {
        let (net, _) = id(1);
        let (_, a) = id(2);
        let signed = Network::new(&net, "home", a, "trigkey").sign(&net).unwrap();
        signed.verify().unwrap();
        let mut forged = signed.clone();
        forged.network.set(id(3).1, State::Member { name: "evil".into(), kind: Kind::Machine }, a);
        assert!(forged.verify().is_err(), "a changed document must not verify");
        assert!(Network::new(&net, "home", a, "t").sign(&id(9).0).is_err(), "only the network key signs");
    }

    #[test]
    fn concurrent_changes_both_survive() {
        let (net, _) = id(1);
        let (_, a) = id(2);
        let (_, b) = id(3);
        let mut on_a = Network::new(&net, "home", a, "trigkey");
        on_a.set(b, State::Member { name: "gmktec".into(), kind: Kind::Machine }, a);
        let mut on_b = on_a.clone();
        // At the same time: a adds a phone, b adds a laptop.
        on_a.set(id(4).1, State::Member { name: "phone".into(), kind: Kind::Device }, a);
        on_b.set(id(5).1, State::Member { name: "laptop".into(), kind: Kind::Machine }, b);
        let mut merged_a = on_a.clone();
        assert!(merged_a.merge(&on_b).unwrap());
        let mut merged_b = on_b.clone();
        assert!(merged_b.merge(&on_a).unwrap());
        assert_eq!(merged_a, merged_b, "both ends agree");
        assert_eq!(merged_a.members().count(), 4);
        assert!(!merged_a.clone().merge(&merged_b).unwrap(), "nothing new");
    }

    #[test]
    fn a_removal_wins_and_a_new_invite_comes_back() {
        let (net, _) = id(1);
        let (_, a) = id(2);
        let (_, phone) = id(4);
        let mut doc = Network::new(&net, "home", a, "trigkey");
        doc.set(phone, State::Member { name: "phone".into(), kind: Kind::Device }, a);
        let before = doc.clone();
        doc.set(phone, State::Removed, a);
        assert!(doc.member(&phone).is_none());
        // An old copy cannot bring it back.
        assert!(!doc.merge(&before).unwrap());
        assert!(doc.member(&phone).is_none());
        // A newer add (a new invite) does.
        doc.set(phone, State::Member { name: "phone".into(), kind: Kind::Device }, a);
        assert!(doc.member(&phone).is_some());
        assert_eq!(doc.find("phone").len(), 1);
        assert_eq!(doc.machines().count(), 1);
    }

    #[test]
    fn another_network_is_refused() {
        let (_, a) = id(2);
        let mut one = Network::new(&id(1).0, "one", a, "t");
        let two = Network::new(&id(7).0, "two", a, "t");
        assert!(one.merge(&two).is_err());
    }

    #[test]
    fn relays_in_records() {
        let (net, _) = id(1);
        let (_, a) = id(2);
        let (_, phone) = id(4);
        let mut doc = Network::new(&net, "home", a, "trigkey");
        doc.set(phone, State::Member { name: "phone".into(), kind: Kind::Device }, a);
        let plain = postcard::to_stdvec(&doc).unwrap();
        assert!(doc.relays().is_empty(), "no relays: n0");
        assert!(!doc.set_relays(a, &[], a), "nothing to change");
        let relay: RelayUrl = "https://relay.example./".parse().unwrap();
        assert!(doc.set_relays(a, std::slice::from_ref(&relay), a));
        assert!(!doc.set_relays(a, std::slice::from_ref(&relay), a), "the same relays: no new record");
        assert_eq!(doc.relays(), vec![relay.clone()]);
        assert_eq!(doc.member(&a).unwrap().name, "trigkey");
        assert_eq!(doc.addr(a).relay_urls().collect::<Vec<_>>(), vec![&relay]);
        assert_eq!(doc.addr(phone).relay_urls().count(), 0);
        // The new variant is the last one: older peers decode old records.
        assert_eq!(postcard::to_stdvec(&doc.records[&a].state).unwrap()[0], 2);
        let back: Network = postcard::from_bytes(&plain).unwrap();
        assert_eq!(back.relays(), Vec::<RelayUrl>::new());
        // Back to n0: a plain `Member` again.
        assert!(doc.set_relays(a, &[], a));
        assert!(matches!(doc.records[&a].state, State::Member { .. }));
        let signed = doc.sign(&net).unwrap();
        signed.verify().unwrap();
    }
}
