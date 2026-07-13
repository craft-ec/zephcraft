//! Governance chain store — the durable, self-verifying source of truth for BOTH the
//! governor set and the program registry. Instead of per-node state gossiped around, one
//! [`GovernanceChain`] (genesis + ordered approvals) is published as durable content and
//! resolved cross-node on demand: every node folds the chain from genesis and derives the
//! identical governor set + program registry. A change (`gov-propose`) appends an approval
//! and republishes; peers adopt the **longest valid chain** — no gossip, no propagation.
//!
//! Persists to `<data_dir>/governance.chain`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;
use zeph_com::{
    registry_program_cid, GovAction, GovernanceApproval, GovernanceChain, GovernanceProposal,
    GovernanceSet,
};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;
use zeph_membership::Membership;
use zeph_obj::{ConsumeMode, ObjEngine};
use zeph_routing::ContentRouting;

/// Reserved app-name under which a node announces its governance-chain HEAD pointer.
/// Contains a control char so it can never collide with a user app name.
const GOV_HEAD_NAME: &str = "\u{1}governance-chain";

pub struct GovernanceChainStore {
    identity: Arc<NodeIdentity>,
    obj: Arc<ObjEngine>,
    routing: Arc<dyn ContentRouting>,
    membership: RwLock<Option<Arc<Membership>>>,
    self_id: [u8; 32],
    chain: RwLock<GovernanceChain>,
    path: PathBuf,
    /// `(seq, when)` of the last announce — gates [`Self::publish`] to on-change + a periodic
    /// heartbeat instead of every tick (an unconditional announce every tick was constant DHT-put
    /// chatter that, with the unversioned fetches, congested slow links; see `tick`).
    last_announce: tokio::sync::Mutex<Option<(u64, std::time::Instant)>>,
}

/// Re-announce the chain head at least this often even when unchanged (keeps the DHT record fresh
/// well inside its 48 h TTL). Announces also fire immediately on any seq change.
const ANNOUNCE_HEARTBEAT: std::time::Duration = std::time::Duration::from_secs(600);

impl GovernanceChainStore {
    /// Open the store: load the persisted chain (only if its genesis matches config and it
    /// verifies), else start a fresh chain from genesis. Genesis defaults to **1-of-1 with
    /// this node's key**; config `governance_governors`/threshold override it (and MUST be
    /// identical across nodes for cross-node convergence).
    pub fn open(
        identity: Arc<NodeIdentity>,
        data_dir: &Path,
        governors: &[[u8; 32]],
        threshold: usize,
        obj: Arc<ObjEngine>,
        routing: Arc<dyn ContentRouting>,
    ) -> Self {
        let self_id = identity.node_id().0;
        let genesis = if governors.is_empty() {
            GovernanceSet::genesis(vec![self_id], 1)
        } else {
            GovernanceSet::genesis(governors.to_vec(), threshold.max(1))
        };
        let path = data_dir.join("governance.chain");
        let chain = std::fs::read(&path)
            .ok()
            .and_then(|b| GovernanceChain::decode(&b))
            .filter(|c| c.genesis == genesis && c.current().is_some())
            .unwrap_or_else(|| GovernanceChain::new(genesis));
        Self {
            identity,
            obj,
            routing,
            membership: RwLock::new(None),
            self_id,
            chain: RwLock::new(chain),
            path,
            last_announce: tokio::sync::Mutex::new(None),
        }
    }

    pub async fn set_membership(&self, membership: Arc<Membership>) {
        *self.membership.write().await = Some(membership);
    }

    /// The current governor set (derived by folding the chain from genesis).
    pub async fn current(&self) -> GovernanceSet {
        let c = self.chain.read().await;
        c.current().unwrap_or_else(|| c.genesis.clone())
    }

    pub async fn is_governor(&self) -> bool {
        self.current().await.is_member(&self.self_id)
    }

    /// Draft a proposal at the next seq, signed with THIS node's key (if a governor).
    pub async fn draft(&self, action: GovAction) -> GovernanceApproval {
        let seq = self.chain.read().await.seq() + 1;
        let proposal = GovernanceProposal { action, seq };
        let sig = proposal.sign(&self.identity);
        GovernanceApproval {
            signatures: vec![sig],
            proposal,
        }
    }

    /// Add THIS node's signature to an existing approval (for k-of-n collection).
    pub async fn cosign(&self, approval: &mut GovernanceApproval) -> anyhow::Result<()> {
        if !self.is_governor().await {
            anyhow::bail!("this node is not a governor");
        }
        let sig = approval.proposal.sign(&self.identity);
        if !approval.signatures.iter().any(|s| s.member == sig.member) {
            approval.signatures.push(sig);
        }
        Ok(())
    }

    /// Append an approval to the chain (if it validly extends it), persist, and publish the
    /// new chain so peers converge. Returns the advanced governor set.
    pub async fn submit(&self, approval: &GovernanceApproval) -> anyhow::Result<GovernanceSet> {
        let set = {
            let mut chain = self.chain.write().await;
            if !chain.append(approval.clone()) {
                anyhow::bail!(
                    "approval invalid: bad quorum, wrong seq, or non-governor signatures"
                );
            }
            std::fs::write(&self.path, chain.encode())?;
            tracing::info!(seq = chain.seq(), "governance chain advanced");
            chain.current().unwrap_or_else(|| chain.genesis.clone())
        };
        self.publish().await;
        Ok(set)
    }

    /// Resolve a network-owned program's canonical cid from the derived program registry. Read
    /// side of the `SetProgram` governance feature; currently unused in-tree (the registry program
    /// went native — memory `registry-native-validation-not-wasm-hook`), kept as the generic API.
    #[allow(dead_code)]
    pub async fn resolve(&self, name: &str) -> Option<[u8; 32]> {
        self.chain
            .read()
            .await
            .program_registry()
            .resolve(name)
            .or_else(|| (name == "app-registry").then(registry_program_cid))
    }

    /// Resolve a protocol config value from the derived config registry (`None` = unset, so the
    /// consumer applies its built-in default). The value is cluster-agreed: every node folds the
    /// same governance chain, so all nodes read the identical value. Set via a `SetConfig`
    /// governance approval. Used e.g. for the registry `shard_bits` (the live shard-count exponent).
    pub async fn resolve_config(&self, key: &str) -> Option<i64> {
        self.chain.read().await.config_registry().resolve(key)
    }

    /// `(name, cid_hex, version)` rows of the derived program registry (dashboard).
    pub async fn rows(&self) -> Vec<(String, String, u64)> {
        self.chain
            .read()
            .await
            .program_registry()
            .entries()
            .iter()
            .map(|(n, c, v)| (n.clone(), hex::encode(c), *v))
            .collect()
    }

    /// Publish our chain as durable content + announce our head. The announce version MUST
    /// strictly increase with the chain seq, because the DHT record store keeps the
    /// highest-seq-per-publisher and REJECTS an equal seq (record.rs: `existing.seq >= rec.seq`).
    /// Use `seq + 1` (never 0, and monotonic): a bare `seq` would announce genesis at 0, while the
    /// old `seq.max(1)` floored BOTH seq 0 and seq 1 to version 1 — so the very first governance
    /// change (0→1) could never supersede the genesis record in the DHT and never propagated.
    async fn publish(&self) {
        let (bytes, seq) = {
            let c = self.chain.read().await;
            (c.encode(), c.seq())
        };
        if let Ok(cid) = self.obj.publish_system(&bytes).await {
            let _ = self.routing.announce_app(GOV_HEAD_NAME, cid, seq + 1).await;
            *self.last_announce.lock().await = Some((seq, std::time::Instant::now()));
        }
    }

    /// Publish only when the chain seq CHANGED since the last announce, or the
    /// [`ANNOUNCE_HEARTBEAT`] elapsed (record freshness). The old unconditional
    /// publish-every-tick was a DHT put per node per tick — pure churn, since governance
    /// changes are rare and human-initiated.
    async fn publish_if_due(&self) {
        let seq = self.chain.read().await.seq();
        let due = match *self.last_announce.lock().await {
            Some((last_seq, at)) => last_seq != seq || at.elapsed() >= ANNOUNCE_HEARTBEAT,
            None => true,
        };
        if due {
            self.publish().await;
        }
    }

    /// Fetch a peer's published governance chain — but ONLY if its announced version says the
    /// peer's chain is LONGER than ours (announce version = seq + 1). The announce resolve is one
    /// DHT get; the content fetch (provider resolve + piece requests, possibly twice through a
    /// manifest) is the expensive part and is skipped when there is nothing to adopt. The old
    /// unconditional fetch-every-peer-every-tick was a constant stream of QUIC handshakes that
    /// congested slow links — measured on the relay-Mac as membership ping timeouts (3 s) while
    /// ICMP on the same path was 0 % loss: self-inflicted churn, not packet loss.
    async fn fetch_if_newer(&self, from: [u8; 32], local_seq: u64) -> Option<GovernanceChain> {
        let rec = self
            .routing
            .resolve_app(NodeId(from), GOV_HEAD_NAME)
            .await
            .ok()??;
        if rec.version <= local_seq + 1 {
            return None; // peer's chain is not longer than ours — nothing to adopt
        }
        let raw = self.obj.get(rec.wasm_cid, ConsumeMode::Drop).await.ok()?;
        let bytes = match zeph_obj::Manifest::decode(&raw) {
            Some(zeph_obj::Manifest::File { content, .. }) => {
                self.obj.get(Cid(content), ConsumeMode::Drop).await.ok()?
            }
            _ => raw,
        };
        GovernanceChain::decode(&bytes)
    }

    /// One anti-entropy tick: publish ours, then pull each peer's chain and adopt the
    /// **longest valid** one that shares our genesis. No gossip — durable content, pulled.
    ///
    /// Pull targets = the CONVERGED census ∪ the current governor set (minus self):
    /// - **census, not the HyParView active view.** The active view is bounded (~5) and diverges
    ///   per node, so a governor may be absent from a given node's active view — then that node
    ///   never pulls the governor's chain and a change never propagates. The census is the
    ///   converged, union-merged member set (the same set registry election was moved onto), so
    ///   every node reaches every member. This is the ROOT-CAUSE hardening behind the earlier
    ///   symptom fix (the announce-version floor): even with a correct version, active-view-only
    ///   pulling could strand propagation on a sparse/WAN topology.
    /// - **plus the governors explicitly.** Governors are the SOURCE of every change; a
    ///   flaky/relay-only governor can sit right at the census-TTL edge and drop out of the census
    ///   transiently, so we always include the governor ids as pull targets. `fetch` resolves a
    ///   peer's head via the DHT, so a governor need not be a direct active peer to be pulled from.
    ///
    /// Cost is O(targets) `fetch`es per tick — fine at 10s–100s of nodes (governance is tiny and
    /// changes rarely); a digest/sampled pull is the scale follow-up (mirrors the membership note).
    pub async fn tick(&self) {
        self.publish_if_due().await;
        let mut targets: Vec<[u8; 32]> = Vec::new();
        {
            let guard = self.membership.read().await;
            let Some(m) = guard.as_ref() else {
                return;
            };
            for (n, _addr) in m.census().await {
                if n.0 != self.self_id && !targets.contains(&n.0) {
                    targets.push(n.0);
                }
            }
        }
        for g in self.current().await.members {
            if g != self.self_id && !targets.contains(&g) {
                targets.push(g);
            }
        }
        let genesis = self.chain.read().await.genesis.clone();
        for p in targets {
            // Re-read the local seq each iteration — an adoption earlier in the loop raises the
            // bar for the remaining peers (their equal-length chains no longer warrant a fetch).
            let local_seq = self.chain.read().await.seq();
            let Some(fetched) = self.fetch_if_newer(p, local_seq).await else {
                continue;
            };
            if fetched.genesis != genesis || fetched.current().is_none() {
                continue; // different genesis or tampered chain — never adopt
            }
            let mut chain = self.chain.write().await;
            if fetched.seq() > chain.seq() {
                let _ = std::fs::write(&self.path, fetched.encode());
                tracing::info!(seq = fetched.seq(), "adopted longer governance chain");
                *chain = fetched;
            }
        }
    }
}
