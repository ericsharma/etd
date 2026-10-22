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
use iroh::{EndpointId, PublicKey, SecretKey, Signature};
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
    Member { name: String, kind: Kind },
    Removed,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member<'a> {
    pub id: EndpointId,
    pub name: &'a str,
    pub kind: Kind,
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
            State::Member { name, kind } => Some(Member { id: *id, name, kind: *kind }),
            State::Removed => None,
        }
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
}
