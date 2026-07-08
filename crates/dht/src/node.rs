//! `DhtNode` — the Kademlia overlay: serve inbound queries, and run iterative α-parallel
//! lookups to find the K nodes closest to any key. Phase 1 is overlay formation only
//! (FIND_NODE + bootstrap); record storage layers on in Phase 2.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use zeph_core::NodeId;
use zeph_crypto::NodeIdentity;
use zeph_transport::{Connection, Transport};

use crate::proto::{DhtMessage, WireContact};
use crate::record::{RecordStore, StoredRecord};
use crate::table::{closer_to, Contact, RoutingTable, K};
use crate::{ALPHA, ALPN};

const MAX_FRAME: usize = 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Consecutive failed requests before a (non-seed) contact is evicted as dead.
const EVICT_AFTER: u32 = 3;
/// How long an evicted-dead contact stays un-relearnable (ms). Breaks the re-teach storm:
/// after eviction a peer or provider record can hand the dead id right back, so we refuse to
/// re-insert it until the tombstone expires. 10 min — long enough to converge, short enough
/// that a node which genuinely returns is picked back up.
const TOMBSTONE_TTL_MS: u64 = 600_000;

pub struct DhtNode {
    identity: Arc<NodeIdentity>,
    me: Contact,
    table: Mutex<RoutingTable>,
    store: RecordStore,
    transport: Arc<Transport>,
    /// Cached outbound DHT connections keyed by peer NodeId. Reused across requests (a fresh
    /// stream per request) instead of connect+close per op — the per-op teardown re-ran the
    /// QUIC/multipath handshake every time and stormed the network during the cutover.
    conns: Mutex<HashMap<[u8; 32], Connection>>,
    /// Consecutive failed-request count per peer; crossing `EVICT_AFTER` evicts it.
    failures: Mutex<HashMap<[u8; 32], u32>>,
    /// Evicted-dead peers → expiry millis. `learn` refuses to re-add a live tombstone, so a
    /// peer or record re-teaching a dead node can't immediately re-storm it.
    tombstones: Mutex<HashMap<[u8; 32], u64>>,
    /// Seed (bootstrap) node ids — never evicted or tombstoned, so a transient network blip
    /// can never drop the nodes we rely on to rejoin the overlay.
    seeds: Mutex<HashSet<[u8; 32]>>,
}

impl DhtNode {
    pub fn new(
        identity: Arc<NodeIdentity>,
        transport: Arc<Transport>,
        record_ttl_millis: u64,
    ) -> Arc<Self> {
        let me = Contact {
            id: NodeId(identity.node_id().0),
            addr: transport.addr(),
        };
        Arc::new(Self {
            table: Mutex::new(RoutingTable::new(me.id)),
            store: RecordStore::new(record_ttl_millis),
            identity,
            me,
            transport,
            conns: Mutex::new(HashMap::new()),
            failures: Mutex::new(HashMap::new()),
            tombstones: Mutex::new(HashMap::new()),
            seeds: Mutex::new(HashSet::new()),
        })
    }

    /// A request to `id` succeeded — clear any accumulated failure count.
    fn note_alive(&self, id: &[u8; 32]) {
        self.failures.lock().expect("failures").remove(id);
    }

    /// A request to `peer` failed (both attempts). Count it; once a NON-seed peer crosses
    /// `EVICT_AFTER` consecutive failures, evict it from the routing table, drop its cached
    /// connection, and tombstone it so peers/records can't immediately re-teach it. Seeds are
    /// never evicted — they are the bootstrap path.
    fn note_dead(&self, peer: &Contact) {
        let id = peer.id.0;
        if self.seeds.lock().expect("seeds").contains(&id) {
            return;
        }
        let mut failures = self.failures.lock().expect("failures");
        let n = failures.entry(id).or_insert(0);
        *n += 1;
        if *n >= EVICT_AFTER {
            failures.remove(&id);
            drop(failures);
            self.table.lock().expect("table").remove(&peer.id);
            self.conns.lock().expect("conns").remove(&id);
            self.tombstones
                .lock()
                .expect("tombstones")
                .insert(id, self.now_millis() + TOMBSTONE_TTL_MS);
        }
    }

    /// Is `id` currently tombstoned (evicted-dead, not yet expired)? Expired entries are
    /// swept as they're checked.
    fn tombstoned(&self, id: &[u8; 32]) -> bool {
        let now = self.now_millis();
        let mut tomb = self.tombstones.lock().expect("tombstones");
        match tomb.get(id) {
            Some(&expiry) if expiry > now => true,
            Some(_) => {
                tomb.remove(id);
                false
            }
            None => false,
        }
    }

    /// This node's own dialable contact.
    pub fn contact(&self) -> Contact {
        self.me.clone()
    }

    /// Wall-clock milliseconds — the record store's TTL reference.
    fn now_millis(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Records held locally (this node is among the K closest to their keys).
    pub fn stored_len(&self) -> usize {
        self.store.len()
    }

    /// Drop expired records — called periodically by the owner (records live 48h; the
    /// publisher republishes every 22h, a policy owned by the routing layer).
    pub fn expire(&self) -> usize {
        self.store.expire(self.now_millis())
    }

    /// Snapshot the record store to `path` (atomic). See [`RecordStore::save`].
    pub fn save_records(&self, path: &std::path::Path) -> std::io::Result<usize> {
        self.store.save(path)
    }

    /// Restore the record store from `path`, dropping expired + re-verifying every signature.
    /// See [`RecordStore::load_from`].
    pub fn load_records(&self, path: &std::path::Path) -> usize {
        self.store.load_from(path, self.now_millis())
    }

    /// Snapshot the routing table's contacts to `path`, atomically. On restart the overlay
    /// re-forms instantly from these instead of re-bootstrapping from seeds — stale contacts are
    /// self-evicted by liveness. Returns how many contacts were written.
    pub fn save_table(&self, path: &std::path::Path) -> std::io::Result<usize> {
        let wire: Vec<WireContact> = {
            let t = self.table.lock().expect("table");
            t.contacts().iter().map(WireContact::from).collect()
        };
        let bytes = postcard::to_allocvec(&wire)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, path)?;
        Ok(wire.len())
    }

    /// Restore routing-table contacts from `path` (malformed addresses skipped, never trusted).
    /// A missing or corrupt file leaves the table empty. Returns how many contacts were loaded.
    pub fn load_table(&self, path: &std::path::Path) -> usize {
        let Ok(bytes) = std::fs::read(path) else {
            return 0;
        };
        let Ok(wire) = postcard::from_bytes::<Vec<WireContact>>(&bytes) else {
            return 0;
        };
        let mut t = self.table.lock().expect("table");
        let mut loaded = 0;
        for w in wire {
            if let Some(c) = w.into_contact() {
                t.insert(c);
                loaded += 1;
            }
        }
        loaded
    }

    fn self_wire(&self) -> WireContact {
        (&self.me).into()
    }

    /// Fold contacts into the routing table (never ourselves, never a live tombstone). The
    /// tombstone check is what stops a peer or provider record from re-teaching a dead node
    /// we just evicted — without it, eviction alone would thrash (evict → re-learn → re-dial).
    /// Tombstones are checked before the table lock is taken, so the two locks never nest.
    fn learn(&self, contacts: impl IntoIterator<Item = Contact>) {
        let fresh: Vec<Contact> = contacts
            .into_iter()
            .filter(|c| c.id != self.me.id && !self.tombstoned(&c.id.0))
            .collect();
        let mut t = self.table.lock().expect("table");
        for c in fresh {
            t.insert(c);
        }
    }

    pub fn table_len(&self) -> usize {
        self.table.lock().expect("table").len()
    }

    /// The K closest contacts we currently know to `target` (local, no network).
    pub fn closest_local(&self, target: &[u8; 32]) -> Vec<Contact> {
        self.table.lock().expect("table").closest(target, K)
    }

    // ---- serving inbound queries -------------------------------------------------

    /// Compute the reply to an inbound message, learning the sender (Kademlia learns from
    /// every query). Pure w.r.t. the network — only touches the routing table.
    fn handle(&self, msg: DhtMessage) -> DhtMessage {
        match msg {
            DhtMessage::Ping { from } => {
                if let Some(c) = from.into_contact() {
                    self.learn([c]);
                }
                DhtMessage::Pong {
                    from: self.self_wire(),
                }
            }
            DhtMessage::FindNode { from, target } => {
                if let Some(c) = from.into_contact() {
                    self.learn([c]);
                }
                let closest = self.closest_local(&target);
                DhtMessage::Nodes {
                    contacts: closest.iter().map(WireContact::from).collect(),
                }
            }
            DhtMessage::Store { record } => {
                let stored = self.store.put(record, self.now_millis());
                DhtMessage::StoreAck { stored }
            }
            DhtMessage::FindValue { from, key } => {
                if let Some(c) = from.into_contact() {
                    self.learn([c]);
                }
                let records = self.store.get(&key, self.now_millis());
                let closer = self
                    .closest_local(&key)
                    .iter()
                    .map(WireContact::from)
                    .collect();
                DhtMessage::Value { records, closer }
            }
            other => {
                tracing::debug!(
                    ?other,
                    "unexpected inbound dht message (reply on a request path)"
                );
                DhtMessage::Nodes {
                    contacts: Vec::new(),
                }
            }
        }
    }

    /// Serve inbound DHT connections handed over by the transport's ALPN dispatcher. One
    /// task per connection; one request→one reply per accepted bi-stream.
    pub fn serve(self: Arc<Self>, mut conns: mpsc::Receiver<Connection>) {
        tokio::spawn(async move {
            while let Some(conn) = conns.recv().await {
                let node = self.clone();
                tokio::spawn(async move {
                    while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                        let Ok(bytes) = recv.read_to_end(MAX_FRAME).await else {
                            break;
                        };
                        let Some(msg) = DhtMessage::decode(&bytes) else {
                            break;
                        };
                        let reply = node.handle(msg);
                        if send.write_all(&reply.encode()).await.is_err() {
                            break;
                        }
                        let _ = send.finish();
                    }
                });
            }
        });
    }

    // ---- issuing queries ---------------------------------------------------------

    /// One request → one reply over the DHT ALPN. `None` on any failure (dead peer, bad
    /// reply) — callers treat that as "no answer", never an error to propagate.
    async fn request(&self, peer: &Contact, msg: &DhtMessage) -> Option<DhtMessage> {
        // Reuse a cached connection (a fresh stream per request); if the cached one is dead,
        // drop it and reconnect once. No per-request close → no handshake/multipath churn.
        for attempt in 0..2 {
            if let Some(conn) = self.conn_for(peer, attempt == 1).await {
                if let Some(reply) = Self::request_on(&conn, msg).await {
                    self.note_alive(&peer.id.0);
                    return Some(reply);
                }
                self.conns.lock().expect("conns").remove(&peer.id.0);
            }
        }
        self.note_dead(peer);
        None
    }

    /// A live connection to `peer` — cached, or freshly dialed and cached. `force_new` bypasses
    /// the cache to recover from a connection that just failed a request.
    async fn conn_for(&self, peer: &Contact, force_new: bool) -> Option<Connection> {
        if !force_new {
            let cached = self.conns.lock().expect("conns").get(&peer.id.0).cloned();
            if let Some(c) = cached {
                return Some(c);
            }
        }
        let conn = self.transport.connect(&peer.addr, ALPN).await.ok()?;
        self.conns
            .lock()
            .expect("conns")
            .insert(peer.id.0, conn.clone());
        Some(conn)
    }

    /// One request/response over an existing connection (opens a fresh bi-stream; the
    /// connection itself is left open for reuse).
    async fn request_on(conn: &Connection, msg: &DhtMessage) -> Option<DhtMessage> {
        let (mut send, mut recv) = conn.open_bi().await.ok()?;
        send.write_all(&msg.encode()).await.ok()?;
        send.finish().ok()?;
        let bytes = tokio::time::timeout(REQUEST_TIMEOUT, recv.read_to_end(MAX_FRAME))
            .await
            .ok()?
            .ok()?;
        DhtMessage::decode(&bytes)
    }

    /// Ask one peer for its K closest to `target`.
    async fn find_node(&self, peer: &Contact, target: &[u8; 32]) -> Vec<Contact> {
        let msg = DhtMessage::FindNode {
            from: self.self_wire(),
            target: *target,
        };
        match self.request(peer, &msg).await {
            Some(DhtMessage::Nodes { contacts }) => contacts
                .into_iter()
                .filter_map(|w| w.into_contact())
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Iterative α-parallel lookup: converge on the K contacts closest to `target`. Learns
    /// every contact discovered into the routing table along the way.
    pub async fn lookup(&self, target: [u8; 32]) -> Vec<Contact> {
        let mut known: HashMap<NodeId, Contact> = HashMap::new();
        for c in self.closest_local(&target) {
            known.insert(c.id, c);
        }
        let mut queried: HashSet<NodeId> = HashSet::new();

        loop {
            let mut shortlist: Vec<Contact> = known.values().cloned().collect();
            shortlist.sort_by(|a, b| closer_to(&target, &a.id.0, &b.id.0));
            shortlist.truncate(K);

            let batch: Vec<Contact> = shortlist
                .iter()
                .filter(|c| !queried.contains(&c.id))
                .take(ALPHA)
                .cloned()
                .collect();
            if batch.is_empty() {
                return shortlist;
            }
            for c in &batch {
                queried.insert(c.id);
            }

            let rounds =
                futures::future::join_all(batch.iter().map(|c| self.find_node(c, &target))).await;
            let discovered: Vec<Contact> = rounds.into_iter().flatten().collect();
            self.learn(discovered.iter().cloned());
            for c in discovered {
                known.insert(c.id, c);
            }
        }
    }

    /// Join the overlay: seed the table with known contacts, then self-lookup so peers learn
    /// us and we fill our buckets with the neighbourhood around our own id.
    pub async fn bootstrap(&self, seeds: Vec<Contact>) {
        {
            let mut s = self.seeds.lock().expect("seeds");
            for c in &seeds {
                s.insert(c.id.0);
            }
        }
        self.learn(seeds);
        let found = self.lookup(self.me.id.0).await;
        self.learn(found);
    }

    // ---- records -----------------------------------------------------------------

    /// Publish a signed record (`value` under `key`, at `seq`) to the K nodes closest to
    /// `key`. Signs with our identity, keeps a local copy, and stores on the responsible
    /// nodes found by an iterative lookup.
    pub async fn put(&self, key: [u8; 32], seq: u64, value: Vec<u8>) {
        let record = StoredRecord::sign(&self.identity, key, seq, value);
        self.store.put(record.clone(), self.now_millis());
        let targets = self.lookup(key).await;
        let msg = DhtMessage::Store { record };
        futures::future::join_all(targets.iter().map(|c| self.request(c, &msg))).await;
    }

    /// Fetch all records under `key` from the overlay: iterative FIND_VALUE, verifying every
    /// returned record and keeping the highest seq per publisher (many publishers coexist).
    pub async fn get(&self, key: [u8; 32]) -> Vec<StoredRecord> {
        let mut known: HashMap<NodeId, Contact> = HashMap::new();
        for c in self.closest_local(&key) {
            known.insert(c.id, c);
        }
        let mut queried: HashSet<NodeId> = HashSet::new();
        let mut best: HashMap<[u8; 32], StoredRecord> = HashMap::new();
        for r in self.store.get(&key, self.now_millis()) {
            merge(&mut best, r);
        }

        loop {
            let mut shortlist: Vec<Contact> = known.values().cloned().collect();
            shortlist.sort_by(|a, b| closer_to(&key, &a.id.0, &b.id.0));
            shortlist.truncate(K);
            let batch: Vec<Contact> = shortlist
                .iter()
                .filter(|c| !queried.contains(&c.id))
                .take(ALPHA)
                .cloned()
                .collect();
            if batch.is_empty() {
                break;
            }
            for c in &batch {
                queried.insert(c.id);
            }
            let replies = futures::future::join_all(batch.iter().map(|c| {
                let msg = DhtMessage::FindValue {
                    from: self.self_wire(),
                    key,
                };
                async move { self.request(c, &msg).await }
            }))
            .await;
            for reply in replies.into_iter().flatten() {
                if let DhtMessage::Value { records, closer } = reply {
                    for r in records {
                        if r.key == key && r.verify() {
                            merge(&mut best, r);
                        }
                    }
                    let contacts: Vec<Contact> = closer
                        .into_iter()
                        .filter_map(|w| w.into_contact())
                        .collect();
                    self.learn(contacts.iter().cloned());
                    for c in contacts {
                        known.insert(c.id, c);
                    }
                }
            }
        }
        best.into_values().collect()
    }
}

/// Keep the highest-seq record per publisher.
fn merge(best: &mut HashMap<[u8; 32], StoredRecord>, r: StoredRecord) {
    match best.get(&r.publisher) {
        Some(existing) if existing.seq >= r.seq => {}
        _ => {
            best.insert(r.publisher, r);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeph_transport::Reach;

    async fn spawn_node() -> Arc<DhtNode> {
        let id = Arc::new(NodeIdentity::generate());
        let transport = Arc::new(
            Transport::bind(
                id.secret_key_bytes(),
                Reach::LocalOnly,
                vec![ALPN.to_vec()],
                0,
            )
            .await
            .expect("bind"),
        );
        let node = DhtNode::new(id, transport.clone(), 3_600_000);
        let (tx, rx) = mpsc::channel(64);
        let t = transport.clone();
        tokio::spawn(async move { t.serve(vec![(ALPN.to_vec(), tx)]).await });
        node.clone().serve(rx);
        node
    }

    /// One seed + `joiners` nodes that bootstrap off it in turn.
    async fn overlay(joiners: usize) -> Vec<Arc<DhtNode>> {
        let seed = spawn_node().await;
        let seed_contact = seed.contact();
        let mut nodes = vec![seed];
        for _ in 0..joiners {
            let n = spawn_node().await;
            n.bootstrap(vec![seed_contact.clone()]).await;
            nodes.push(n);
        }
        nodes
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn overlay_bootstraps_and_locates_peers() {
        let nodes = overlay(4).await;
        assert!(
            nodes[0].table_len() >= 4,
            "seed should know all joiners, has {}",
            nodes[0].table_len()
        );
        // The last joiner locates the first purely by iterative lookup through the overlay.
        let target = nodes[1].contact().id.0;
        let found = nodes[4].lookup(target).await;
        assert!(
            found.iter().any(|c| c.id.0 == target),
            "iterative lookup should locate the target peer"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn record_put_is_gettable_across_the_overlay() {
        let nodes = overlay(4).await;
        // A node that did NOT publish can fetch a record published by another.
        let key = [42u8; 32];
        nodes[1].put(key, 1, b"i-hold-cid-X".to_vec()).await;
        let got = nodes[4].get(key).await;
        assert_eq!(got.len(), 1, "record retrievable across the overlay");
        assert_eq!(got[0].value, b"i-hold-cid-X");
        assert!(got[0].verify());
    }
}
