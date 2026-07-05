//! Phase 4g — the LIVE committee chain: genesis bootstrap + epoch rollover, stored as
//! durable content (fetched on demand, not replicated per node). Each epoch the current
//! committee endorses the next; an assembler (lowest node-id in the eligible set)
//! gathers a k-of-n quorum of endorsements into a checkpoint, appends it, and publishes
//! the chain. Other nodes fetch the published chain and verify it from genesis.
//!
//! The epoch length is `ZEPH_EPOCH_MS` (default 1h; set short to watch rollover live).

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use zeph_com::{
    committee_hash, endorse_checkpoint, epoch_of, pda, registry_program_cid, request_endorsement,
    select_committee, Committee, CommitteeChain, CommitteeCheckpoint, EndorseRequest,
    REGISTRY_SEED,
};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;
use zeph_membership::Membership;
use zeph_obj::{ConsumeMode, ObjEngine};
use zeph_routing::ContentRouting;
use zeph_transport::{PeerAddr, Transport};

const COMMITTEE_N: usize = 5;
const COMMITTEE_K: usize = 3;
/// Reserved app-name under which the chain HEAD pointer is announced (owner=assembler).
const COMMITTEE_HEAD_NAME: &str = "\u{1}committee-chain";

fn epoch_ms() -> u64 {
    std::env::var("ZEPH_EPOCH_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3_600_000)
}

pub struct CommitteeChainStore {
    identity: Arc<NodeIdentity>,
    obj: Arc<ObjEngine>,
    routing: Arc<dyn ContentRouting>,
    transport: Arc<Transport>,
    membership: RwLock<Option<Arc<Membership>>>,
    self_id: [u8; 32],
    epoch_ms: u64,
    chain: RwLock<Option<CommitteeChain>>,
}

impl CommitteeChainStore {
    pub fn new(
        identity: Arc<NodeIdentity>,
        obj: Arc<ObjEngine>,
        routing: Arc<dyn ContentRouting>,
        transport: Arc<Transport>,
    ) -> Self {
        let self_id = identity.node_id().0;
        Self {
            identity,
            obj,
            routing,
            transport,
            membership: RwLock::new(None),
            self_id,
            epoch_ms: epoch_ms(),
            chain: RwLock::new(None),
        }
    }

    /// Wire membership once it is running (until then, ticks are no-ops).
    pub async fn set_membership(&self, membership: Arc<Membership>) {
        *self.membership.write().await = Some(membership);
    }

    /// The configured epoch length (ms) — for the dashboard countdown.
    pub fn epoch_len_ms(&self) -> u64 {
        self.epoch_ms
    }

    /// The eligible pool (self + alive peers) and a node-id → dial-address map.
    async fn eligible(&self) -> (Vec<[u8; 32]>, HashMap<[u8; 32], PeerAddr>) {
        let Some(m) = self.membership.read().await.clone() else {
            return (vec![self.self_id], HashMap::new());
        };
        let snap = m.snapshot().await;
        let mut ids = vec![self.self_id];
        let mut addr = HashMap::new();
        for (nid, ps) in &snap.active {
            if ps.alive {
                ids.push(nid.0);
                addr.insert(nid.0, ps.addr.clone());
            }
        }
        (ids, addr)
    }

    /// Publish the chain as durable content + announce its head (owner = this node).
    async fn publish(&self, chain: &CommitteeChain, version: u64) {
        if let Ok(cid) = self.obj.publish_system(&chain.encode()).await {
            // version = monotonic wall-clock (not epoch) so it survives epoch-length
            // changes and the newest published chain always wins the announce CAS.
            let _ = self
                .routing
                .announce_app(COMMITTEE_HEAD_NAME, cid, version)
                .await;
        }
    }

    /// Fetch a peer's published chain (by its announced head) and adopt it if it is
    /// valid and longer than ours — content pulled on demand, not replicated eagerly.
    async fn fetch(&self, from: [u8; 32]) -> Option<CommitteeChain> {
        let rec = self
            .routing
            .resolve_app(NodeId(from), COMMITTEE_HEAD_NAME)
            .await
            .ok()??;
        let raw = self.obj.get(rec.wasm_cid, ConsumeMode::Drop).await.ok()?;
        let bytes = match zeph_obj::Manifest::decode(&raw) {
            Some(zeph_obj::Manifest::File { content, .. }) => {
                self.obj.get(Cid(content), ConsumeMode::Drop).await.ok()?
            }
            _ => raw,
        };
        let chain = CommitteeChain::decode(&bytes)?;
        chain.current()?; // must verify from genesis
        Some(chain)
    }

    /// Serve an endorsement request: recompute the proposed committee from OUR own
    /// membership and, only if it matches, sign the hand-off.
    pub async fn endorse(&self, req: &EndorseRequest) -> Option<zeph_com::Endorsement> {
        let (eligible, _) = self.eligible().await;
        let mine = select_committee(&eligible, req.epoch, COMMITTEE_N, COMMITTEE_K);
        if mine.members != req.committee_members {
            return None; // our view disagrees — don't endorse
        }
        let next = Committee {
            epoch: req.epoch,
            members: req.committee_members.clone(),
            k: req.k,
        };
        Some(endorse_checkpoint(&self.identity, &next, req.prev_hash))
    }

    /// One tick: sync the chain from the network, then bootstrap genesis or roll the
    /// epoch over if we are the assembler.
    pub async fn tick(&self, now_millis: u64) {
        let epoch = epoch_of(now_millis, self.epoch_ms);
        let (eligible, addr_of) = self.eligible().await;
        if eligible.len() < 2 {
            return; // wait for peers before forming a committee
        }
        let assembler = *eligible.iter().min().unwrap();

        // Adopt the assembler's published chain if it's ahead of ours.
        if assembler != self.self_id {
            if let Some(fetched) = self.fetch(assembler).await {
                let mut chain = self.chain.write().await;
                let ahead = match chain.as_ref().and_then(|c| c.current()) {
                    Some(cur) => fetched
                        .current()
                        .map(|f| f.epoch > cur.epoch)
                        .unwrap_or(false),
                    None => true,
                };
                if ahead {
                    *chain = Some(fetched);
                }
            }
            return; // non-assemblers only follow
        }

        // We are the assembler: bootstrap genesis, or roll over.
        let mut chain = self.chain.write().await;
        if chain.is_none() {
            let genesis = select_committee(&eligible, epoch, COMMITTEE_N, COMMITTEE_K);
            let c = CommitteeChain::new(genesis);
            self.publish(&c, now_millis).await;
            tracing::info!(
                epoch,
                size = c.genesis.members.len(),
                "committee chain genesis"
            );
            *chain = Some(c);
            return;
        }
        let c = chain.as_mut().unwrap();
        let current = match c.current() {
            Some(c) => c,
            None => return,
        };
        if epoch <= current.epoch {
            return; // same epoch — nothing to do
        }
        if let Some(cp) = self.build_checkpoint(&current, epoch, &addr_of).await {
            if c.append(cp) {
                self.publish(c, now_millis).await;
                tracing::info!(epoch, "committee chain rolled to new epoch");
            }
        }
    }

    /// Gather a k-of-n quorum of endorsements from the current committee for the next.
    async fn build_checkpoint(
        &self,
        current: &Committee,
        epoch: u64,
        addr_of: &HashMap<[u8; 32], PeerAddr>,
    ) -> Option<CommitteeCheckpoint> {
        let (eligible, _) = self.eligible().await;
        let next = select_committee(&eligible, epoch, COMMITTEE_N, COMMITTEE_K);
        let prev_hash = committee_hash(current);
        let req = EndorseRequest {
            epoch,
            committee_members: next.members.clone(),
            k: next.k,
            prev_hash,
        };
        let mut endorsements = Vec::new();
        for m in &current.members {
            if *m == self.self_id {
                endorsements.push(endorse_checkpoint(&self.identity, &next, prev_hash));
            } else if let Some(addr) = addr_of.get(m) {
                if let Ok(e) = request_endorsement(&self.transport, addr, &req).await {
                    endorsements.push(e);
                }
            }
        }
        Some(CommitteeCheckpoint {
            committee: next,
            prev_hash,
            endorsements,
        })
    }

    /// Status for the dashboard: (chain length incl. genesis, current committee epoch,
    /// current committee size, chain root, the committee PDA account).
    pub async fn status(&self) -> (usize, u64, usize, String, String) {
        let account = pda(&registry_program_cid(), REGISTRY_SEED);
        match self.chain.read().await.as_ref() {
            Some(c) => {
                let cur = c.current();
                (
                    1 + c.checkpoints.len(),
                    cur.as_ref().map(|c| c.epoch).unwrap_or(0),
                    cur.as_ref().map(|c| c.members.len()).unwrap_or(0),
                    hex::encode(c.root()),
                    hex::encode(account.0),
                )
            }
            None => (0, 0, 0, String::new(), hex::encode(account.0)),
        }
    }
}
