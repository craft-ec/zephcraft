//! `zeph-dht` — a Kademlia DHT for CraftOBJ content routing.
//!
//! THE content-routing backend (the interim tracker service is retired): a self-organizing
//! overlay where each record (provider, want, meta, owner-keyed head) lives on the K nodes
//! closest (XOR distance) to its key, is signed (reusing `zeph_wire::SignedRecord`), and is
//! discovered by an iterative α-parallel lookup. Node census is SWIM membership and bootstrap
//! is the configured `dht_seeds` (a DHT cannot enumerate globally). Runs behind the
//! `zeph_routing::ContentRouting` trait so callers are unchanged.
//!
//! Foundation §3: 256 k-buckets, k=20, α=3, 32-byte keyspace, provider records ≤1 KiB,
//! 48h TTL / 22h republish.

mod node;
mod proto;
mod record;
mod table;

pub use node::DhtNode;
pub use proto::{DhtMessage, WireContact};
pub use record::{RecordStore, StoredRecord};
pub use table::{bucket_index, closer_to, distance, Contact, RoutingTable, BUCKETS, K};

/// Concurrency of iterative lookups (Kademlia α).
pub const ALPHA: usize = 3;

/// ALPN for the DHT protocol (distinct from the tracker's `/craftec/tracker/1`).
pub const ALPN: &[u8] = b"/craftec/dht/1";
