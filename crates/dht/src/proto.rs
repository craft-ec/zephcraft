//! DHT wire protocol — a small, self-contained message set over its own ALPN.
//!
//! Kept out of `zeph-wire` because contacts carry a `PeerAddr` (a `zeph-transport` type,
//! which `zeph-wire` sits below). Messages are postcard-encoded; one request → one reply per
//! bi-stream, read to end (no length prefix needed). Addresses travel as their canonical
//! text form so the protocol stays independent of iroh's in-memory address type.

use serde::{Deserialize, Serialize};
use zeph_core::NodeId;
use zeph_transport::PeerAddr;

use crate::record::StoredRecord;
use crate::table::Contact;

/// A contact as it travels on the wire: id + its dialable address (text form).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireContact {
    pub id: [u8; 32],
    pub addr: String,
}

impl From<&Contact> for WireContact {
    fn from(c: &Contact) -> Self {
        WireContact {
            id: c.id.0,
            addr: c.addr.to_string(),
        }
    }
}

impl WireContact {
    /// Parse back into a dialable `Contact`. `None` if the address is malformed (a peer sent
    /// junk) — such contacts are simply skipped, never trusted.
    pub fn into_contact(self) -> Option<Contact> {
        let addr: PeerAddr = self.addr.parse().ok()?;
        Some(Contact {
            id: NodeId(self.id),
            addr,
        })
    }
}

/// DHT request/response messages: overlay formation (Ping + FindNode) and the
/// record operations (Store / FindValue / Value).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DhtMessage {
    /// Liveness + "here I am" — the sender includes itself so the receiver can learn it.
    Ping { from: WireContact },
    /// Ack of a ping.
    Pong { from: WireContact },
    /// "Who are the K closest contacts you know to `target`?" — includes the asker so the
    /// receiver adds it to its table (Kademlia learns from every query).
    FindNode { from: WireContact, target: [u8; 32] },
    /// The K closest contacts the responder knows to the queried target.
    Nodes { contacts: Vec<WireContact> },
    /// "Store this signed record" — sent to the K nodes closest to `record.key`.
    Store { record: StoredRecord },
    /// Ack of a store (false = rejected: bad signature or stale seq).
    StoreAck { stored: bool },
    /// "Give me the records under `key`, and your K closest to it (so I can recurse)."
    FindValue { from: WireContact, key: [u8; 32] },
    /// Records the responder holds for the key, plus its K closest contacts for recursion.
    Value {
        records: Vec<StoredRecord>,
        closer: Vec<WireContact>,
    },
}

impl DhtMessage {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("postcard encode of a dht message cannot fail")
    }
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn real_contact() -> Contact {
        let id = zeph_crypto::NodeIdentity::generate();
        Contact {
            id: NodeId(id.node_id().0),
            addr: format!("{}@127.0.0.1:9100", hex::encode(id.node_id().0))
                .parse()
                .expect("addr"),
        }
    }

    #[test]
    fn contact_survives_wire_roundtrip() {
        let c = real_contact();
        let w: WireContact = (&c).into();
        let back = w.into_contact().expect("roundtrip");
        assert_eq!(back.id, c.id);
        assert_eq!(back.addr.to_string(), c.addr.to_string());
    }

    #[test]
    fn message_roundtrips_through_postcard() {
        let c = real_contact();
        let msg = DhtMessage::FindNode {
            from: (&c).into(),
            target: [7u8; 32],
        };
        let bytes = msg.encode();
        match DhtMessage::decode(&bytes).expect("decode") {
            DhtMessage::FindNode { target, from } => {
                assert_eq!(target, [7u8; 32]);
                assert_eq!(from.id, c.id.0);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn malformed_addr_is_skipped_not_trusted() {
        let w = WireContact {
            id: [1u8; 32],
            addr: "not-an-address".to_string(),
        };
        assert!(w.into_contact().is_none());
    }
}
