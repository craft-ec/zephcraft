//! `DhtNode` — the Kademlia overlay: serve inbound queries, and run iterative α-parallel
//! lookups to find the K nodes closest to any key. Phase 1 is overlay formation only
//! (FIND_NODE + bootstrap); record storage layers on in Phase 2.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use zeph_core::NodeId;
use zeph_transport::{Connection, PeerAddr, Transport};

use crate::proto::{DhtMessage, WireContact};
use crate::table::{closer_to, Contact, RoutingTable, K};
use crate::{ALPHA, ALPN};

const MAX_FRAME: usize = 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub struct DhtNode {
    me: Contact,
    table: Mutex<RoutingTable>,
    transport: Arc<Transport>,
}

impl DhtNode {
    pub fn new(me: Contact, transport: Arc<Transport>) -> Arc<Self> {
        Arc::new(Self {
            table: Mutex::new(RoutingTable::new(me.id)),
            me,
            transport,
        })
    }

    fn self_wire(&self) -> WireContact {
        (&self.me).into()
    }

    /// Fold contacts into the routing table (never ourselves).
    fn learn(&self, contacts: impl IntoIterator<Item = Contact>) {
        let mut t = self.table.lock().expect("table");
        for c in contacts {
            if c.id != self.me.id {
                t.insert(c);
            }
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
    async fn request(&self, addr: &PeerAddr, msg: &DhtMessage) -> Option<DhtMessage> {
        let conn = self.transport.connect(addr, ALPN).await.ok()?;
        let (mut send, mut recv) = conn.open_bi().await.ok()?;
        send.write_all(&msg.encode()).await.ok()?;
        send.finish().ok()?;
        let bytes = tokio::time::timeout(REQUEST_TIMEOUT, recv.read_to_end(MAX_FRAME))
            .await
            .ok()?
            .ok()?;
        conn.close(0u32.into(), b"done");
        DhtMessage::decode(&bytes)
    }

    /// Ask one peer for its K closest to `target`.
    async fn find_node(&self, peer: &Contact, target: &[u8; 32]) -> Vec<Contact> {
        let msg = DhtMessage::FindNode {
            from: self.self_wire(),
            target: *target,
        };
        match self.request(&peer.addr, &msg).await {
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
        self.learn(seeds);
        let found = self.lookup(self.me.id.0).await;
        self.learn(found);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeph_crypto::NodeIdentity;
    use zeph_transport::Reach;

    async fn spawn_node() -> (Arc<DhtNode>, Contact) {
        let id = NodeIdentity::generate();
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
        let me = Contact {
            id: NodeId(id.node_id().0),
            addr: transport.addr(),
        };
        let node = DhtNode::new(me.clone(), transport.clone());
        let (tx, rx) = mpsc::channel(64);
        let t = transport.clone();
        tokio::spawn(async move { t.serve(vec![(ALPN.to_vec(), tx)]).await });
        node.clone().serve(rx);
        (node, me)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn overlay_bootstraps_and_locates_peers() {
        // One seed; four joiners bootstrap off it in turn.
        let (seed, seed_contact) = spawn_node().await;
        let mut joiners = Vec::new();
        let mut contacts = Vec::new();
        for _ in 0..4 {
            let (n, c) = spawn_node().await;
            n.bootstrap(vec![seed_contact.clone()]).await;
            joiners.push(n);
            contacts.push(c);
        }

        // The seed learned every joiner from their FIND_NODE(self) queries.
        assert!(
            seed.table_len() >= 4,
            "seed should know all joiners, has {}",
            seed.table_len()
        );

        // The last joiner can locate the first joiner by id via iterative lookup through the
        // overlay (it only ever knew the seed directly).
        let target = contacts[0].id.0;
        let found = joiners[3].lookup(target).await;
        assert!(
            found.iter().any(|c| c.id.0 == target),
            "iterative lookup should locate the target peer"
        );
    }
}
