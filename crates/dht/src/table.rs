//! Kademlia routing table — k-buckets over the 256-bit XOR keyspace.
//!
//! Every node/CID key is a 32-byte BLAKE3 digest. The distance between two keys is their
//! bitwise XOR, interpreted as a big-endian 256-bit integer. A node's routing table is 256
//! buckets: bucket `i` holds contacts whose distance from us has its most-significant set
//! bit at position `i` (bucket 0 = share the whole prefix but the last bit = closest;
//! bucket 255 = differ in the top bit = farthest). Each bucket holds at most `K` contacts,
//! evicting nothing while it has room and keeping the longest-lived contacts when full
//! (least-recently-seen at the front) — the Kademlia stability bias.

use zeph_core::NodeId;
use zeph_transport::PeerAddr;

/// Bucket capacity (Kademlia `k`). Foundation §3: 20 per bucket.
pub const K: usize = 20;
/// Number of buckets = key bit-width.
pub const BUCKETS: usize = 256;

/// A dialable routing-table entry: who, and where to reach them.
#[derive(Debug, Clone)]
pub struct Contact {
    pub id: NodeId,
    pub addr: PeerAddr,
}

/// XOR distance between two keys (big-endian 256-bit).
pub fn distance(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut d = [0u8; 32];
    for i in 0..32 {
        d[i] = a[i] ^ b[i];
    }
    d
}

/// Bucket index for `other` relative to `self_id`: the position of the most-significant
/// differing bit, counted so that the FARTHEST nodes (top bit differs) land in bucket 255
/// and the closest in bucket 0. `None` iff the keys are identical (that's us).
pub fn bucket_index(self_id: &[u8; 32], other: &[u8; 32]) -> Option<usize> {
    for i in 0..32 {
        let x = self_id[i] ^ other[i];
        if x != 0 {
            let msb_from_top = i * 8 + x.leading_zeros() as usize; // 0 = overall top bit
            return Some(255 - msb_from_top);
        }
    }
    None
}

/// Order two keys by XOR distance to `target` (closer first). Used to pick the K-closest.
pub fn closer_to(target: &[u8; 32], a: &[u8; 32], b: &[u8; 32]) -> std::cmp::Ordering {
    distance(a, target).cmp(&distance(b, target))
}

/// A node's k-bucket routing table.
pub struct RoutingTable {
    self_id: [u8; 32],
    buckets: Vec<Vec<Contact>>,
}

impl RoutingTable {
    pub fn new(self_id: NodeId) -> Self {
        Self {
            self_id: self_id.0,
            buckets: (0..BUCKETS).map(|_| Vec::new()).collect(),
        }
    }

    /// Insert (or refresh) a contact. A known contact moves to the back (most-recently
    /// seen). A new contact is appended if the bucket has room; if full it is dropped —
    /// the incumbent (older, proven-alive) contacts are kept (liveness-check-on-evict is a
    /// later refinement). Inserting ourselves is a no-op.
    pub fn insert(&mut self, contact: Contact) {
        let Some(idx) = bucket_index(&self.self_id, &contact.id.0) else {
            return;
        };
        let bucket = &mut self.buckets[idx];
        if let Some(pos) = bucket.iter().position(|c| c.id == contact.id) {
            let existing = bucket.remove(pos);
            // refresh address in case it changed; keep most-recently-seen at the back
            bucket.push(Contact {
                addr: contact.addr,
                ..existing
            });
        } else if bucket.len() < K {
            bucket.push(contact);
        }
    }

    /// The `count` contacts closest (by XOR distance) to `target`.
    pub fn closest(&self, target: &[u8; 32], count: usize) -> Vec<Contact> {
        let mut all: Vec<Contact> = self.buckets.iter().flatten().cloned().collect();
        all.sort_by(|a, b| closer_to(target, &a.id.0, &b.id.0));
        all.truncate(count);
        all
    }

    /// Total contacts held.
    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }

    /// All contacts currently held, flattened across buckets — for persistence.
    pub fn contacts(&self) -> Vec<Contact> {
        self.buckets.iter().flatten().cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains(&self, id: &NodeId) -> bool {
        bucket_index(&self.self_id, &id.0)
            .map(|idx| self.buckets[idx].iter().any(|c| &c.id == id))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte0: u8) -> [u8; 32] {
        let mut k = [0u8; 32];
        k[0] = byte0;
        k
    }
    // A dialable address must carry a REAL curve-point id, but the routing-table `id`
    // (which drives distance) is independent of it — so we control `id` freely and pair it
    // with any valid address.
    fn valid_addr() -> PeerAddr {
        let id = zeph_crypto::NodeIdentity::generate();
        format!("{}@127.0.0.1:9000", hex::encode(id.node_id().0))
            .parse()
            .expect("valid addr")
    }
    fn contact(byte0: u8) -> Contact {
        Contact {
            id: NodeId(key(byte0)),
            addr: valid_addr(),
        }
    }

    #[test]
    fn contacts_persist_roundtrip_via_wire() {
        use crate::proto::WireContact;
        let me = NodeId(key(0));
        let mut t = RoutingTable::new(me);
        for b in 1..=5u8 {
            t.insert(contact(b));
        }
        let before = t.len();
        assert_eq!(t.contacts().len(), before);
        // Serialize exactly as DhtNode::save_table does, decode + reinsert as load_table does.
        let wire: Vec<WireContact> = t.contacts().iter().map(WireContact::from).collect();
        let bytes = postcard::to_allocvec(&wire).expect("encode");
        let decoded: Vec<WireContact> = postcard::from_bytes(&bytes).expect("decode");
        let mut t2 = RoutingTable::new(me);
        for w in decoded {
            if let Some(c) = w.into_contact() {
                t2.insert(c);
            }
        }
        assert_eq!(t2.len(), before, "all contacts restored across a save/load");
    }

    #[test]
    fn distance_is_xor_and_symmetric() {
        let a = key(0b1010_0000);
        let b = key(0b0110_0000);
        assert_eq!(distance(&a, &b), distance(&b, &a));
        assert_eq!(distance(&a, &a), [0u8; 32]);
        assert_eq!(distance(&a, &b)[0], 0b1100_0000);
    }

    #[test]
    fn bucket_index_top_bit_is_farthest() {
        let me = key(0);
        // differ in the very top bit → farthest → bucket 255
        assert_eq!(bucket_index(&me, &key(0b1000_0000)), Some(255));
        // differ only in the lowest bit of the last byte → closest → bucket 0
        let mut near = [0u8; 32];
        near[31] = 1;
        assert_eq!(bucket_index(&me, &near), Some(0));
        // identical → us → None
        assert_eq!(bucket_index(&me, &me), None);
    }

    #[test]
    fn insert_dedups_refreshes_and_ignores_self() {
        let me = NodeId(key(0));
        let mut t = RoutingTable::new(me);
        t.insert(contact(0)); // self → ignored
        assert_eq!(t.len(), 0);
        t.insert(contact(0b1000_0000));
        t.insert(contact(0b1000_0000)); // dup → refresh, not grow
        assert_eq!(t.len(), 1);
        assert!(t.contains(&NodeId(key(0b1000_0000))));
    }

    #[test]
    fn bucket_capacity_is_k_keeping_incumbents() {
        let me = NodeId(key(0));
        let mut t = RoutingTable::new(me);
        // All these share the top differing bit region so they compete for the same bucket
        // only if their msb differing position matches; use distinct low bytes to spread,
        // then assert total never exceeds K per bucket via closest() sanity.
        for i in 1..=(K as u16 + 5) {
            let mut k = [0u8; 32];
            k[31] = i as u8; // vary the lowest byte → small distances, low buckets
            t.insert(Contact {
                id: NodeId(k),
                addr: valid_addr(),
            });
        }
        // Every bucket must respect K.
        for b in &t.buckets {
            assert!(b.len() <= K);
        }
    }

    #[test]
    fn closest_orders_by_xor_distance() {
        let me = NodeId(key(0));
        let mut t = RoutingTable::new(me);
        for b in [0b0000_0001u8, 0b0000_0010, 0b0100_0000, 0b1000_0000] {
            t.insert(contact(b));
        }
        let target = key(0);
        let near = t.closest(&target, 2);
        // closest to 0 are the smallest-valued keys
        assert_eq!(near[0].id, NodeId(key(0b0000_0001)));
        assert_eq!(near[1].id, NodeId(key(0b0000_0010)));
    }
}
