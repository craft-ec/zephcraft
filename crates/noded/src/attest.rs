//! The attestation store — per-program **quorum-authority** chains (`ATTESTATION_DESIGN.md`,
//! phase P3). It is [`governance`](crate::governance) generalized to app scope: instead of the single
//! network [`zeph_com::QuorumChain`], one chain PER (owner, program), holding that program's declared
//! quorum + the statements it has authorized.
//!
//! Collection is **governance-style** (agreement, not aggregation — `VERIFICATION_ATTESTATION_MODEL
//! §5.3`): a statement is proposed, the NAMED members cosign it (manual, out-of-band — real
//! judgment, like `gov_sign`), and the k-of-n [`Attestation`] is appended to the program's chain.
//! The `attest` host fn then CHECKS `is_authorized`.
//!
//! **GLOBAL + owner-authenticated, like governance.** Each program's chain is published as durable
//! content + a per-(owner,program) DHT head, and pulled cross-node — the same publish/pull
//! anti-entropy `GovernanceChainStore` uses. So `attest()` works on ANY node, not just the collector.
//! The trust root: the genesis quorum is signed by the program's OWNER (the identity that registered
//! the program), bound to the program cid — an [`AttestedChain`] envelope. A fetching node adopts a
//! chain ONLY if that owner signature verifies AND the envelope's owner equals the program owner the
//! node independently resolved from the (owner-signed) registry. This mirrors how governance pins its
//! genesis from local config (operator trust); here the trust root is the program's registered owner
//! — so a malicious peer can only ever publish a quorum under its OWN key, never spoof the real
//! owner's. Chain *extensions* are already self-authenticating (they fold against the genesis member
//! keys), so only the genesis needs the owner signature.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;
use zeph_com::{
    AttestAction, AttestBackend, AttestProposal, Attestation, AttestedChain, Quorum, QuorumChain,
};
use zeph_core::NodeId;
use zeph_crypto::NodeIdentity;
use zeph_membership::Membership;
use zeph_obj::{ConsumeMode, ObjEngine};
use zeph_routing::ContentRouting;

/// The reserved app-name a node announces a program's attestation-chain head under, keyed by BOTH
/// owner and program so each owner's declaration for each program resolves independently (and a
/// malicious peer's chain under a different owner never collides with the real one). The leading
/// control char keeps it out of the user app-name space.
fn attest_head_name(owner: &[u8; 32], program_cid: &[u8; 32]) -> String {
    format!(
        "\u{1}attest-chain-{}-{}",
        hex::encode(owner),
        hex::encode(program_cid)
    )
}

/// Map key: `(owner, program_cid)`.
type ChainKey = ([u8; 32], [u8; 32]);

/// Per-(owner, program) attestation chains, published + pulled cross-node so `attest()` is GLOBAL.
/// The map value is the owner-signed [`AttestedChain`] envelope so a node that ADOPTED a chain it
/// can't itself sign still preserves the owner signature for re-publish.
pub struct AttestStore {
    identity: Arc<NodeIdentity>,
    /// `(owner, program_cid)` → that program's owner-signed quorum-authority chain.
    chains: RwLock<HashMap<ChainKey, AttestedChain>>,
    dir: PathBuf,
    obj: Arc<ObjEngine>,
    routing: Arc<dyn ContentRouting>,
    /// Census source for pull targets; injected after construction (mirrors governance).
    membership: RwLock<Option<Arc<Membership>>>,
}

impl AttestStore {
    /// Open the store, loading any persisted per-(owner,program) chains from `<data_dir>/attest/`.
    /// A chain is loaded only if its owner signature verifies and it folds.
    pub fn open(
        identity: Arc<NodeIdentity>,
        data_dir: &Path,
        obj: Arc<ObjEngine>,
        routing: Arc<dyn ContentRouting>,
    ) -> Arc<Self> {
        let dir = data_dir.join("attest");
        let _ = std::fs::create_dir_all(&dir);
        let mut chains = HashMap::new();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for e in entries.flatten() {
                let path = e.path();
                if path.extension().and_then(|x| x.to_str()) != Some("chain") {
                    continue;
                }
                // Filename is `<owner_hex>_<program_hex>.chain`.
                let Some((owner, program)) = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.split_once('_'))
                    .and_then(|(o, p)| Some((parse32(o)?, parse32(p)?)))
                else {
                    continue;
                };
                if let Some(att) = std::fs::read(&path)
                    .ok()
                    .and_then(|b| AttestedChain::decode(&b))
                    .filter(|a| a.owner == owner && a.verify(&program))
                {
                    chains.insert((owner, program), att);
                }
            }
        }
        Arc::new(Self {
            identity,
            chains: RwLock::new(chains),
            dir,
            obj,
            routing,
            membership: RwLock::new(None),
        })
    }

    /// Inject the membership handle whose `census()` supplies pull targets.
    pub async fn set_membership(&self, m: Arc<Membership>) {
        *self.membership.write().await = Some(m);
    }

    /// This node's identity = the owner when it bootstraps/collects its own program's quorum.
    pub fn owner(&self) -> [u8; 32] {
        self.identity.node_id().0
    }

    fn chain_path(&self, owner: &[u8; 32], program_cid: &[u8; 32]) -> PathBuf {
        self.dir.join(format!(
            "{}_{}.chain",
            hex::encode(owner),
            hex::encode(program_cid)
        ))
    }

    fn persist(&self, att: &AttestedChain, program_cid: &[u8; 32]) {
        let _ = std::fs::write(self.chain_path(&att.owner, program_cid), att.encode());
    }

    /// Bootstrap a program's quorum (genesis members + threshold). The owner is THIS node (it signs
    /// the genesis); for the invoke-time check to authenticate, the program must be registered under
    /// this same identity. Idempotent. Publishes the fresh chain so peers can resolve it.
    pub async fn bootstrap(&self, program_cid: [u8; 32], members: Vec<[u8; 32]>, threshold: usize) {
        let owner = self.owner();
        {
            let mut chains = self.chains.write().await;
            let att = chains.entry((owner, program_cid)).or_insert_with(|| {
                let chain = QuorumChain::new(Quorum::genesis(members, threshold));
                AttestedChain::new(&self.identity, &program_cid, chain)
            });
            self.persist(att, &program_cid);
        }
        self.publish(&owner, &program_cid).await;
    }

    /// Draft a statement proposal at the program's next seq, signed with THIS node's key. Uses this
    /// node's own (owner) chain.
    pub async fn propose(&self, program_cid: [u8; 32], statement: Vec<u8>) -> Option<Attestation> {
        let owner = self.owner();
        let seq = self
            .chains
            .read()
            .await
            .get(&(owner, program_cid))?
            .chain
            .seq()
            + 1;
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
    pub async fn cosign(&self, att: &mut Attestation) {
        let sig = att.proposal.sign(&self.identity);
        if !att.signatures.iter().any(|s| s.member == sig.member) {
            att.signatures.push(sig);
        }
    }

    /// Submit a collected attestation: append to THIS node's (owner) chain (iff it validly extends
    /// it) and publish. The owner signature covers only the genesis, so it is preserved unchanged.
    /// Returns whether it was accepted.
    pub async fn submit(&self, program_cid: [u8; 32], att: Attestation) -> bool {
        let owner = self.owner();
        {
            let mut chains = self.chains.write().await;
            let Some(entry) = chains.get_mut(&(owner, program_cid)) else {
                return false; // quorum not bootstrapped on this node
            };
            if !entry.chain.append(att) {
                return false;
            }
            self.persist(entry, &program_cid);
        }
        self.publish(&owner, &program_cid).await;
        true
    }

    /// Whether `owner`'s quorum for `program_cid` has authorized `statement` — SYNCING the chain from
    /// peers first, so this is a global check (`attest()` works on any node, not just the collector).
    pub async fn is_authorized(
        &self,
        owner: &[u8; 32],
        program_cid: &[u8; 32],
        statement: &[u8],
    ) -> bool {
        self.sync(owner, program_cid).await;
        self.chains
            .read()
            .await
            .get(&(*owner, *program_cid))
            .is_some_and(|a| a.chain.is_authorized(statement))
    }

    /// The CURRENT folded quorum for `(owner, program_cid)` — syncing from peers first, so any node
    /// sees the same declared members + threshold. This is the shared quorum a program declares once
    /// (via [`bootstrap`](Self::bootstrap)) and reuses for BOTH authority (`attest`) and ordering (the
    /// sequencer, `SequenceStore`): one program, one quorum. `None` if the program has not declared one
    /// or the chain does not fold.
    pub async fn current_quorum(&self, owner: &[u8; 32], program_cid: &[u8; 32]) -> Option<Quorum> {
        self.sync(owner, program_cid).await;
        self.chains
            .read()
            .await
            .get(&(*owner, *program_cid))
            .and_then(|a| a.chain.current())
    }

    /// List all locally-known attestation chains for the dashboard: `(owner, program_cid, current
    /// quorum, seq)` — the user-declared k-of-n quorums and their activity.
    pub async fn list(&self) -> Vec<([u8; 32], [u8; 32], Option<Quorum>, u64)> {
        self.chains
            .read()
            .await
            .iter()
            .map(|((owner, program), a)| (*owner, *program, a.chain.current(), a.chain.seq()))
            .collect()
    }

    /// Publish an (owner, program) chain: durable content + a per-(owner,program) DHT head (version =
    /// seq + 1, strictly increasing). Publishes the owner-signed envelope. Mirrors governance's
    /// `publish`.
    async fn publish(&self, owner: &[u8; 32], program_cid: &[u8; 32]) {
        let Some((bytes, seq)) = self
            .chains
            .read()
            .await
            .get(&(*owner, *program_cid))
            .map(|a| (a.encode(), a.chain.seq()))
        else {
            return;
        };
        if let Ok(cid) = self.obj.publish_system(&bytes).await {
            let _ = self
                .routing
                .announce_app(&attest_head_name(owner, program_cid), cid, seq + 1)
                .await;
        }
    }

    /// Fetch a peer's published owner-signed chain for `(owner, program_cid)`, but only if its
    /// announced version is longer than `local_seq`. Mirrors governance's `fetch_if_newer`.
    async fn fetch_if_newer(
        &self,
        owner: &[u8; 32],
        program_cid: &[u8; 32],
        from: [u8; 32],
        local_seq: u64,
    ) -> Option<AttestedChain> {
        let rec = self
            .routing
            .resolve_app(NodeId(from), &attest_head_name(owner, program_cid))
            .await
            .ok()??;
        if rec.version <= local_seq + 1 {
            return None;
        }
        let bytes = self
            .obj
            .get_following_manifest(rec.wasm_cid, ConsumeMode::Drop)
            .await
            .ok()?;
        AttestedChain::decode(&bytes)
    }

    /// Pull `owner`'s chain for `program_cid` from census peers and adopt the longest one that (a) is
    /// signed by `owner` for this program, (b) folds, and (c) shares any genesis we already hold. The
    /// owner-signature check is the trust root — a peer can only serve a chain the real owner signed.
    /// No-op when no membership is set (e.g. tests) — local-only.
    async fn sync(&self, owner: &[u8; 32], program_cid: &[u8; 32]) {
        let targets: Vec<[u8; 32]> = {
            let guard = self.membership.read().await;
            let Some(m) = guard.as_ref() else {
                return;
            };
            m.census()
                .await
                .into_iter()
                .map(|(n, _)| n.0)
                .filter(|id| *id != self.owner())
                .collect()
        };
        for peer in targets {
            // Re-derive seq + genesis from `chains` each iteration so a chain adopted from an earlier
            // peer in this loop pins later peers (genesis is per-program, starts absent).
            let (local_seq, local_genesis) = {
                let chains = self.chains.read().await;
                let a = chains.get(&(*owner, *program_cid));
                (
                    a.map_or(0, |a| a.chain.seq()),
                    a.map(|a| a.chain.genesis.clone()),
                )
            };
            let Some(fetched) = self
                .fetch_if_newer(owner, program_cid, peer, local_seq)
                .await
            else {
                continue;
            };
            // Trust root: the envelope must be signed by exactly `owner` for exactly this program,
            // and fold. `verify` checks the owner sig over the genesis AND that the chain folds.
            if fetched.owner != *owner || !fetched.verify(program_cid) {
                continue;
            }
            if let Some(g) = &local_genesis {
                if fetched.chain.genesis != *g {
                    continue; // owner equivocated on the genesis — never flip an already-held one
                }
            }
            let mut chains = self.chains.write().await;
            let cur_seq = chains
                .get(&(*owner, *program_cid))
                .map_or(0, |a| a.chain.seq());
            if fetched.chain.seq() > cur_seq {
                self.persist(&fetched, program_cid);
                chains.insert((*owner, *program_cid), fetched);
            }
        }
    }
}

/// Parse a 64-hex string into a `[u8; 32]`.
fn parse32(hex: &str) -> Option<[u8; 32]> {
    hex::decode(hex)
        .ok()
        .and_then(|b| <[u8; 32]>::try_from(b.as_slice()).ok())
}

#[async_trait::async_trait]
impl AttestBackend for AttestStore {
    /// The `attest` host fn's producer path: is `statement` authorized by `owner`'s quorum for
    /// `program_cid`? (`owner` is the registry-authenticated program owner; syncs cross-node first.)
    async fn attest(&self, owner: [u8; 32], program_cid: [u8; 32], statement: Vec<u8>) -> bool {
        self.is_authorized(&owner, &program_cid, &statement).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeph_core::Cid;
    use zeph_obj::{ObjConfig, PeerSource};
    use zeph_routing::{MetaRecord, ProviderRecord};
    use zeph_store::Store;
    use zeph_transport::{PeerAddr, Reach, Transport};

    struct NullRouting;
    #[async_trait::async_trait]
    impl ContentRouting for NullRouting {
        async fn announce(&self, _: Cid, _: u32, _: bool) -> zeph_routing::Result<()> {
            Ok(())
        }
        async fn resolve(&self, _: Cid) -> zeph_routing::Result<Vec<ProviderRecord>> {
            Ok(vec![])
        }
        async fn withdraw(&self, _: Cid) -> zeph_routing::Result<()> {
            Ok(())
        }
        async fn announce_want(&self, _: Cid) -> zeph_routing::Result<()> {
            Ok(())
        }
        async fn withdraw_want(&self, _: Cid) -> zeph_routing::Result<()> {
            Ok(())
        }
        async fn is_wanted(&self, _: Cid) -> zeph_routing::Result<bool> {
            Ok(false)
        }
        async fn announce_meta(
            &self,
            _: Cid,
            _: u64,
            _: Option<String>,
        ) -> zeph_routing::Result<()> {
            Ok(())
        }
        async fn withdraw_meta(&self, _: Cid) -> zeph_routing::Result<()> {
            Ok(())
        }
        async fn metas(&self, _: Cid) -> zeph_routing::Result<Vec<MetaRecord>> {
            Ok(vec![])
        }
    }
    struct NullPeers;
    #[async_trait::async_trait]
    impl PeerSource for NullPeers {
        async fn peers(&self) -> Vec<(NodeId, PeerAddr)> {
            vec![]
        }
    }

    async fn open_store() -> (Arc<AttestStore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let node = Arc::new(NodeIdentity::generate());
        let transport = Arc::new(
            Transport::bind(
                node.secret_key_bytes(),
                Reach::LocalOnly,
                vec![zeph_transport::MUX_ALPN.to_vec()],
                0,
            )
            .await
            .unwrap(),
        );
        let store = Arc::new(Store::open(dir.path()).unwrap());
        let engine = ObjEngine::with_peer_source(
            transport,
            store,
            Arc::new(NullRouting),
            Arc::new(NullPeers),
            ObjConfig::default(),
        );
        // membership is left unset → sync() is a local no-op, so these tests exercise local logic.
        (
            AttestStore::open(node, dir.path(), engine, Arc::new(NullRouting)),
            dir,
        )
    }

    #[tokio::test]
    async fn quorum_authorizes_a_statement_and_attest_reflects_it() {
        let (store, _dir) = open_store().await;
        let owner = store.owner();
        let program = [9u8; 32];
        let a = NodeIdentity::generate();
        let b = NodeIdentity::generate();
        store
            .bootstrap(program, vec![a.node_id().0, b.node_id().0], 2)
            .await;

        assert!(!store.attest(owner, program, b"deploy v2".to_vec()).await);

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

        assert!(store.attest(owner, program, b"deploy v2".to_vec()).await);
        assert!(!store.attest(owner, program, b"raise quota".to_vec()).await);
        // Authorization is bound to the owner: a DIFFERENT owner's (empty) quorum authorizes nothing.
        let stranger = NodeIdentity::generate().node_id().0;
        assert!(!store.attest(stranger, program, b"deploy v2".to_vec()).await);
    }

    #[tokio::test]
    async fn a_sub_threshold_attestation_is_rejected() {
        let (store, _dir) = open_store().await;
        let owner = store.owner();
        let program = [1u8; 32];
        let (a, b) = (NodeIdentity::generate(), NodeIdentity::generate());
        store
            .bootstrap(program, vec![a.node_id().0, b.node_id().0], 2)
            .await;

        let proposal = AttestProposal {
            action: AttestAction::Statement(b"x".to_vec()),
            seq: 1,
        };
        let att = Attestation {
            signatures: vec![proposal.sign(&a)],
            proposal,
        };
        assert!(!store.submit(program, att).await, "1-of-2 rejected");
        assert!(!store.attest(owner, program, b"x".to_vec()).await);
    }

    #[tokio::test]
    async fn persisted_chain_reloads_with_owner_signature_intact() {
        let dir = tempfile::tempdir().unwrap();
        let node = Arc::new(NodeIdentity::generate());
        let owner = node.node_id().0;
        let program = [3u8; 32];
        let a = NodeIdentity::generate();
        let b = NodeIdentity::generate();

        // Build a store, bootstrap + authorize, then drop it.
        {
            let store = build_store(node.clone(), dir.path()).await;
            store
                .bootstrap(program, vec![a.node_id().0, b.node_id().0], 2)
                .await;
            let proposal = AttestProposal {
                action: AttestAction::Statement(b"ok".to_vec()),
                seq: 1,
            };
            let att = Attestation {
                signatures: vec![proposal.sign(&a), proposal.sign(&b)],
                proposal,
            };
            assert!(store.submit(program, att).await);
        }
        // Reopen from disk — the owner-signed envelope reloads and still authorizes.
        let store = build_store(node, dir.path()).await;
        assert!(store.attest(owner, program, b"ok".to_vec()).await);
    }

    async fn build_store(node: Arc<NodeIdentity>, dir: &Path) -> Arc<AttestStore> {
        let transport = Arc::new(
            Transport::bind(
                node.secret_key_bytes(),
                Reach::LocalOnly,
                vec![zeph_transport::MUX_ALPN.to_vec()],
                0,
            )
            .await
            .unwrap(),
        );
        let store = Arc::new(Store::open(dir).unwrap());
        let engine = ObjEngine::with_peer_source(
            transport,
            store,
            Arc::new(NullRouting),
            Arc::new(NullPeers),
            ObjConfig::default(),
        );
        AttestStore::open(node, dir, engine, Arc::new(NullRouting))
    }
}
