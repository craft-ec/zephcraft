//! `zeph-dht` — a Kademlia DHT for CraftOBJ content routing.
//!
//! Replaces the tracker for the *scalable* routing operations (provider records, wants,
//! owner-keyed heads) with a self-organizing overlay: each record lives on the K nodes
//! closest (XOR distance) to its key, is signed (reusing `zeph_wire::SignedRecord`), and is
//! discovered by an iterative α-parallel lookup. Node/relay census and bootstrap stay with
//! the tracker (a DHT cannot enumerate globally). Runs behind the existing
//! `zeph_routing::ContentRouting` trait so callers are unchanged.
//!
//! Foundation §3: 256 k-buckets, k=20, α=3, 32-byte keyspace, provider records ≤1 KiB,
//! 48h TTL / 22h republish.

mod node;
mod proto;
mod table;

pub use node::DhtNode;
pub use proto::{DhtMessage, WireContact};
pub use table::{bucket_index, closer_to, distance, Contact, RoutingTable, BUCKETS, K};

/// Concurrency of iterative lookups (Kademlia α).
pub const ALPHA: usize = 3;

/// ALPN for the DHT protocol (distinct from the tracker's `/craftec/tracker/1`).
pub const ALPN: &[u8] = b"/craftec/dht/1";
