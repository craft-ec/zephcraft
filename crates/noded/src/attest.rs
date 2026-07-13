//! The attestation store — per-program **quorum-authority** chains (`ATTESTATION_DESIGN.md`,
//! phase P3, Package A). It is [`governance`](crate::governance) generalized to app scope: instead
//! of the single network [`zeph_com::QuorumChain`], one chain PER program (keyed by its content
//! cid), holding that program's declared quorum + the statements it has authorized.
//!
//! Collection is **governance-style** (agreement, not aggregation — see
//! `VERIFICATION_ATTESTATION_MODEL.md §5.3`): a statement is proposed, the NAMED quorum members
//! cosign it (manual, out-of-band — real judgment, like a governor's `gov_sign`), and the k-of-n
//! [`Attestation`] is appended to the program's chain. The `attest` host fn then just CHECKS
//! `is_authorized` — decoupling the synchronous host call from the asynchronous human signing.
//!
//! P3-1 is the local store + the [`AttestBackend`] impl (`attest` = `is_authorized`). Cross-node
//! solicitation of remote members' cosigns + chain gossip is P3-2.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;
use zeph_com::{AttestAction, AttestBackend, AttestProposal, Attestation, Quorum, QuorumChain};
use zeph_crypto::NodeIdentity;

/// Per-program attestation chains. `attest` reads them; the control plane (P3-2) bootstraps a
/// quorum, proposes statements, and collects the members' cosigns into them.
pub struct AttestStore {
    /// Signs this node's cosigns (P3-2 control plane). Unused in the bin until that CLI wiring lands.
    #[allow(dead_code)]
    identity: Arc<NodeIdentity>,
    /// `program_cid` → that program's quorum-authority chain.
    chains: RwLock<HashMap<[u8; 32], QuorumChain>>,
    /// Where per-program chains persist (read on `open`; written by the P3-2 control plane).
    #[allow(dead_code)]
    dir: PathBuf,
}

impl AttestStore {
    /// Open the store, loading any persisted per-program chains from `<data_dir>/attest/`.
    pub fn open(identity: Arc<NodeIdentity>, data_dir: &Path) -> Arc<Self> {
        let dir = data_dir.join("attest");
        let _ = std::fs::create_dir_all(&dir);
        let mut chains = HashMap::new();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for e in entries.flatten() {
                let path = e.path();
                if path.extension().and_then(|x| x.to_str()) != Some("chain") {
                    continue;
                }
                let Some(cid) = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|h| hex::decode(h).ok())
                    .and_then(|b| <[u8; 32]>::try_from(b.as_slice()).ok())
                else {
                    continue;
                };
                // Only adopt a chain that folds validly (a tampered/partial file is ignored).
                if let Some(chain) = std::fs::read(&path)
                    .ok()
                    .and_then(|b| QuorumChain::decode(&b))
                    .filter(|c| c.current().is_some())
                {
                    chains.insert(cid, chain);
                }
            }
        }
        Arc::new(Self {
            identity,
            chains: RwLock::new(chains),
            dir,
        })
    }

    #[allow(dead_code)] // P3-2 control plane (persistence for bootstrap/submit)
    fn chain_path(&self, program_cid: &[u8; 32]) -> PathBuf {
        self.dir.join(format!("{}.chain", hex::encode(program_cid)))
    }

    /// Bootstrap a program's quorum (genesis members + threshold). Idempotent — if a chain already
    /// exists it is left untouched. (Owner-signature gating of the bootstrap is a P3-2 refinement;
    /// here the caller — the program owner via the control plane — installs it.)
    #[allow(dead_code)] // P3-2 control plane
    pub async fn bootstrap(&self, program_cid: [u8; 32], members: Vec<[u8; 32]>, threshold: usize) {
        let mut chains = self.chains.write().await;
        let chain = chains
            .entry(program_cid)
            .or_insert_with(|| QuorumChain::new(Quorum::genesis(members, threshold)));
        let _ = std::fs::write(self.chain_path(&program_cid), chain.encode());
    }

    /// Draft a statement proposal at the program's next seq, signed with THIS node's key (a member
    /// contributes its own signature; more are collected via `cosign`).
    #[allow(dead_code)] // P3-2 control plane
    pub async fn propose(&self, program_cid: [u8; 32], statement: Vec<u8>) -> Option<Attestation> {
        let seq = self.chains.read().await.get(&program_cid)?.seq() + 1;
        let proposal = AttestProposal {
            action: AttestAction::Statement(statement),
            seq,
        };
        Some(Attestation {
            signatures: vec![proposal.sign(&self.identity)],
            proposal,
        })
    }

    /// Add THIS node's signature to an in-flight attestation (a member cosigning — dedup by member).
    #[allow(dead_code)] // P3-2 control plane
    pub async fn cosign(&self, att: &mut Attestation) {
        let sig = att.proposal.sign(&self.identity);
        if !att.signatures.iter().any(|s| s.member == sig.member) {
            att.signatures.push(sig);
        }
    }

    /// Submit a collected attestation: append to the program's chain (iff it validly extends it —
    /// next seq + k-of-n distinct members) and persist. Returns whether it was accepted.
    #[allow(dead_code)] // P3-2 control plane
    pub async fn submit(&self, program_cid: [u8; 32], att: Attestation) -> bool {
        let mut chains = self.chains.write().await;
        let Some(chain) = chains.get_mut(&program_cid) else {
            return false; // quorum not bootstrapped
        };
        if !chain.append(att) {
            return false;
        }
        let _ = std::fs::write(self.chain_path(&program_cid), chain.encode());
        true
    }

    /// Whether the program's quorum has authorized `statement`.
    pub async fn is_authorized(&self, program_cid: &[u8; 32], statement: &[u8]) -> bool {
        self.chains
            .read()
            .await
            .get(program_cid)
            .is_some_and(|c| c.is_authorized(statement))
    }
}

#[async_trait::async_trait]
impl AttestBackend for AttestStore {
    /// The `attest` host fn's producer path: is this statement authorized by the program's quorum?
    /// (A check — signing happens out-of-band; §5.3.)
    async fn attest(&self, program_cid: [u8; 32], statement: Vec<u8>) -> bool {
        self.is_authorized(&program_cid, &statement).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn quorum_authorizes_a_statement_and_attest_reflects_it() {
        let dir = tempfile::tempdir().unwrap();
        let node = Arc::new(NodeIdentity::generate());
        let store = AttestStore::open(node, dir.path());

        let program = [9u8; 32];
        let a = NodeIdentity::generate();
        let b = NodeIdentity::generate();
        store
            .bootstrap(program, vec![a.node_id().0, b.node_id().0], 2)
            .await;

        // not authorized before any attestation
        assert!(!store.attest(program, b"deploy v2".to_vec()).await);

        // members A + B cosign the statement (k=2), then it's submitted
        let proposal = AttestProposal {
            action: AttestAction::Statement(b"deploy v2".to_vec()),
            seq: 1,
        };
        let att = Attestation {
            signatures: vec![proposal.sign(&a), proposal.sign(&b)],
            proposal,
        };
        assert!(
            store.submit(program, att).await,
            "k-of-n attestation accepted"
        );

        // now the app's attest() sees it authorized
        assert!(store.attest(program, b"deploy v2".to_vec()).await);
        // a different, unattested statement is not authorized
        assert!(!store.attest(program, b"raise quota".to_vec()).await);
    }

    #[tokio::test]
    async fn a_sub_threshold_attestation_is_rejected_and_persists_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let node = Arc::new(NodeIdentity::generate());
        let store = AttestStore::open(node, dir.path());
        let program = [1u8; 32];
        let (a, b) = (NodeIdentity::generate(), NodeIdentity::generate());
        store
            .bootstrap(program, vec![a.node_id().0, b.node_id().0], 2)
            .await;

        // only 1 of 2 signs → rejected
        let proposal = AttestProposal {
            action: AttestAction::Statement(b"x".to_vec()),
            seq: 1,
        };
        let att = Attestation {
            signatures: vec![proposal.sign(&a)],
            proposal,
        };
        assert!(!store.submit(program, att).await, "1-of-2 rejected");
        assert!(!store.attest(program, b"x".to_vec()).await);

        // reopening the store finds no authorized statement (nothing was persisted)
        drop(store);
        let store2 = AttestStore::open(Arc::new(NodeIdentity::generate()), dir.path());
        assert!(!store2.attest(program, b"x".to_vec()).await);
    }
}
