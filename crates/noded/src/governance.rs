//! Governance store — the live, seeded governor set. Seeded **1-of-1 with this node's
//! key** by default (configurable via `governance_governors`/`governance_threshold`),
//! and evolved ONLY through the governance process: every change is a
//! [`GovernanceApproval`] verified against the current set and applied. Persists to
//! `<data_dir>/governance.state`.
//!
//! This is the *authority* the protocol registries (program/config, later) check
//! their write-approvals against.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;
use zeph_com::{GovAction, GovernanceApproval, GovernanceProposal, GovernanceSet};
use zeph_crypto::NodeIdentity;

pub struct GovernanceStore {
    identity: Arc<NodeIdentity>,
    set: RwLock<GovernanceSet>,
    path: PathBuf,
}

impl GovernanceStore {
    /// Open the governance store: load the persisted set, or seed a genesis from config
    /// (`governors`/`threshold`), or default to **1-of-1 with this node's own key**.
    pub fn open(
        identity: Arc<NodeIdentity>,
        data_dir: &Path,
        governors: &[[u8; 32]],
        threshold: usize,
    ) -> Self {
        let path = data_dir.join("governance.state");
        let set = std::fs::read(&path)
            .ok()
            .and_then(|b| postcard::from_bytes::<GovernanceSet>(&b).ok())
            .unwrap_or_else(|| {
                if governors.is_empty() {
                    // seed 1-of-1 with our own key
                    GovernanceSet::genesis(vec![identity.node_id().0], 1)
                } else {
                    GovernanceSet::genesis(governors.to_vec(), threshold.max(1))
                }
            });
        Self {
            identity,
            set: RwLock::new(set),
            path,
        }
    }

    pub async fn current(&self) -> GovernanceSet {
        self.set.read().await.clone()
    }

    /// Is this node a governor in the current set?
    pub async fn is_governor(&self) -> bool {
        self.set
            .read()
            .await
            .is_governor(&self.identity.node_id().0)
    }

    /// Draft a proposal at the next seq and sign it with THIS node's key (if a governor).
    /// Returns a partial approval to collect further signatures on (or, at 1-of-1,
    /// already sufficient).
    pub async fn draft(&self, action: GovAction) -> GovernanceApproval {
        let seq = self.set.read().await.seq + 1;
        let proposal = GovernanceProposal { action, seq };
        let sig = proposal.sign(&self.identity);
        GovernanceApproval {
            signatures: vec![sig],
            proposal,
        }
    }

    /// Add THIS node's signature to an existing approval (for k-of-n collection).
    pub async fn cosign(&self, approval: &mut GovernanceApproval) -> anyhow::Result<()> {
        if !self
            .set
            .read()
            .await
            .is_governor(&self.identity.node_id().0)
        {
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

    /// Submit an approval: verify against the current set and apply it, persisting the
    /// advanced set. Returns the new set, or an error if the approval is invalid (bad
    /// quorum / wrong seq / non-governor signatures).
    pub async fn submit(&self, approval: &GovernanceApproval) -> anyhow::Result<GovernanceSet> {
        let mut guard = self.set.write().await;
        let next = guard.apply(approval).ok_or_else(|| {
            anyhow::anyhow!("approval invalid: bad quorum, wrong seq, or non-governor signatures")
        })?;
        std::fs::write(&self.path, postcard::to_allocvec(&next)?)?;
        tracing::info!(
            seq = next.seq,
            governors = next.governors.len(),
            "governance advanced"
        );
        *guard = next.clone();
        Ok(next)
    }
}
