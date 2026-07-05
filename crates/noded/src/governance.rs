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
}

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
        self.current().await.is_governor(&self.self_id)
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
        if !approval
            .signatures
            .iter()
            .any(|s| s.governor == sig.governor)
        {
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

    /// Resolve a network-owned program's canonical cid from the derived program registry.
    pub async fn resolve(&self, name: &str) -> Option<[u8; 32]> {
        self.chain
            .read()
            .await
            .program_registry()
            .resolve(name)
            .or_else(|| (name == "app-registry").then(registry_program_cid))
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

    /// Publish our chain as durable content + announce our head (version = seq, so the
    /// longest chain wins the announce CAS).
    async fn publish(&self) {
        let (bytes, seq) = {
            let c = self.chain.read().await;
            (c.encode(), c.seq())
        };
        if let Ok(cid) = self.obj.publish_system(&bytes).await {
            let _ = self
                .routing
                .announce_app(GOV_HEAD_NAME, cid, seq.max(1))
                .await;
        }
    }

    /// Fetch a peer's published governance chain (by its announced head).
    async fn fetch(&self, from: [u8; 32]) -> Option<GovernanceChain> {
        let rec = self
            .routing
            .resolve_app(NodeId(from), GOV_HEAD_NAME)
            .await
            .ok()??;
        let raw = self.obj.get(rec.wasm_cid, ConsumeMode::Drop).await.ok()?;
        let bytes = match zeph_obj::Manifest::decode(&raw) {
            Some(zeph_obj::Manifest::File { content, .. }) => {
                self.obj.get(Cid(content), ConsumeMode::Drop).await.ok()?
            }
            _ => raw,
        };
        GovernanceChain::decode(&bytes)
    }

    /// One anti-entropy tick: publish ours, then pull each live peer's chain and adopt the
    /// **longest valid** one that shares our genesis. No gossip — durable content, pulled.
    pub async fn tick(&self) {
        self.publish().await;
        let peers: Vec<[u8; 32]> = match self.membership.read().await.as_ref() {
            Some(m) => m
                .snapshot()
                .await
                .active
                .iter()
                .filter(|(_, ps)| ps.alive)
                .map(|(n, _)| n.0)
                .collect(),
            None => return,
        };
        let genesis = self.chain.read().await.genesis.clone();
        for p in peers {
            let Some(fetched) = self.fetch(p).await else {
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
