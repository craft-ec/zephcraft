//! Phase 4c/4d — the live app-name registry backing on the node. `deploy` registers a
//! signed head into a durable [`RegistryState`]; resolution reads it. The state
//! persists to `<data_dir>/registry.state` and its encoded blob is published as a
//! durable system object.
//!
//! Authority (phase 4d): a registration is attested by a **membership-derived rotating
//! committee** — the node fans the deterministic registry transition out to the epoch
//! committee over `ATTEST_ALPN`, and a k-of-n quorum authorizes the advance. If no
//! committee can form yet (a lone node / not enough live peers), it falls back to the
//! v1 ramp (self-attest, n=1) — additive, so nothing breaks as the cluster grows.
//!
//! This runs ALONGSIDE the `KIND_APP` tracker path (network discovery) during migration.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;
use zeph_com::{
    attest_transition, epoch_of, pda, registry_program_cid, request_attestation, select_committee,
    verify_quorum, AttestRequest, HeadSubmission, RegistryState, REGISTRY_SEED,
};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;
use zeph_membership::Membership;
use zeph_obj::ObjEngine;
use zeph_transport::Transport;

/// Target committee size + quorum, and the epoch length. `select_committee` clamps `n`
/// and `k` to the live eligible pool, so a small cluster still forms a valid committee.
const COMMITTEE_N: usize = 5;
const COMMITTEE_K: usize = 3;
const EPOCH_MILLIS: u64 = 3_600_000; // 1h — the committee is stable within an epoch

/// Live-committee coordination inputs (set once membership is up).
struct Coordinator {
    transport: Arc<Transport>,
    membership: Arc<Membership>,
    self_id: [u8; 32],
}

/// The node's durable app-name registry.
pub struct AppRegistry {
    identity: Arc<NodeIdentity>,
    obj: Arc<ObjEngine>,
    state: RwLock<RegistryState>,
    path: PathBuf,
    coord: RwLock<Option<Coordinator>>,
}

impl AppRegistry {
    /// Open the registry, loading any persisted state from `<data_dir>/registry.state`.
    pub fn open(identity: Arc<NodeIdentity>, obj: Arc<ObjEngine>, data_dir: &Path) -> Self {
        let path = data_dir.join("registry.state");
        let state = std::fs::read(&path)
            .ok()
            .and_then(|b| RegistryState::decode(&b))
            .unwrap_or_default();
        Self {
            identity,
            obj,
            state: RwLock::new(state),
            path,
            coord: RwLock::new(None),
        }
    }

    /// Wire the live committee coordinator (call once membership is running). Until then
    /// registrations self-attest (v1 ramp).
    pub async fn set_coordinator(&self, transport: Arc<Transport>, membership: Arc<Membership>) {
        let self_id = self.identity.node_id().0;
        *self.coord.write().await = Some(Coordinator {
            transport,
            membership,
            self_id,
        });
    }

    /// The registry PDA account (program-derived; no keyholder).
    pub fn account(&self) -> NodeId {
        pda(&registry_program_cid(), REGISTRY_SEED)
    }

    /// Register (or advance) an app head under THIS node's identity. Attests via the
    /// membership committee when one can form, else self-attests (v1 ramp). Persists the
    /// new state locally + publishes it as a durable object. Returns the new root.
    pub async fn register(
        &self,
        name: &str,
        cid: [u8; 32],
        version: u64,
        now_millis: u64,
    ) -> anyhow::Result<[u8; 32]> {
        let sub = HeadSubmission::sign(&self.identity, name, cid, version);
        let mut guard = self.state.write().await;
        let prev = guard.clone();
        // Committee attestation first; fall back to self-attest (n=1) if none forms.
        let (next, mode) = match self.try_committee(&sub, &prev, now_millis).await {
            Some(n) => (n, "committee"),
            None => {
                let n = prev
                    .apply(&sub)
                    .map_err(|e| anyhow::anyhow!("registry: {e}"))?;
                let _ = attest_transition(
                    &self.identity,
                    registry_program_cid(),
                    prev.root(),
                    &sub.encode(),
                    &n.encode(),
                );
                (n, "self")
            }
        };
        let encoded = next.encode();
        std::fs::write(&self.path, &encoded)?;
        let _ = self.obj.publish_system(&encoded).await;
        let root = next.root();
        *guard = next;
        tracing::info!(name, version, mode, "registry head advanced");
        Ok(root)
    }

    /// Attempt a committee-attested advance: derive the epoch committee from live
    /// membership, fan the transition out over `ATTEST_ALPN`, and require a k-of-n quorum
    /// on the SAME deterministic output. Returns the new state iff the quorum holds.
    async fn try_committee(
        &self,
        sub: &HeadSubmission,
        prev: &RegistryState,
        now_millis: u64,
    ) -> Option<RegistryState> {
        let coord = self.coord.read().await;
        let coord = coord.as_ref()?;
        let snap = coord.membership.snapshot().await;

        // Eligible pool = self + live active peers (with their dial addresses).
        let mut eligible = vec![coord.self_id];
        let mut addr_of = HashMap::new();
        for (nid, ps) in &snap.active {
            if ps.alive {
                eligible.push(nid.0);
                addr_of.insert(nid.0, ps.addr.clone());
            }
        }
        let epoch = epoch_of(now_millis, EPOCH_MILLIS);
        let committee = select_committee(&eligible, epoch, COMMITTEE_N, COMMITTEE_K);
        if committee.members.len() < 2 {
            return None; // no real committee yet — caller self-attests
        }

        let next = prev.apply(sub).ok()?;
        let program = registry_program_cid();
        let prev_root = prev.root();
        let request = sub.encode();
        let req = AttestRequest {
            program_cid: program,
            prev_root,
            func: String::new(),
            request: request.clone(),
            prev_state: prev.encode(),
        };

        // Each committee member attests: self locally, others over the wire.
        let mut atts = Vec::new();
        for m in &committee.members {
            if *m == coord.self_id {
                atts.push(attest_transition(
                    &self.identity,
                    program,
                    prev_root,
                    &request,
                    &next.encode(),
                ));
            } else if let Some(addr) = addr_of.get(m) {
                if let Ok(att) = request_attestation(&coord.transport, addr, &req).await {
                    atts.push(att);
                }
            }
        }

        let request_hash = Cid::of(&request).0;
        let agreed = verify_quorum(
            &atts,
            &program,
            &prev_root,
            &request_hash,
            &committee.members,
            committee.k,
        )?;
        (agreed == next.root()).then_some(next)
    }

    /// Resolve a name published by `owner` to its current cid.
    pub async fn resolve(&self, owner: [u8; 32], name: &str) -> Option<[u8; 32]> {
        self.state.read().await.resolve(&owner, name).map(|e| e.cid)
    }

    /// Snapshot the registry as `(owner_hex, name, cid_hex, version)` rows for the UI.
    pub async fn rows(&self) -> Vec<(String, String, String, u64)> {
        self.state
            .read()
            .await
            .entries()
            .iter()
            .map(|e| {
                (
                    hex::encode(e.owner),
                    e.name.clone(),
                    hex::encode(e.cid),
                    e.version,
                )
            })
            .collect()
    }

    /// The number of registered heads + the current root (for status).
    pub async fn summary(&self) -> (usize, String) {
        let s = self.state.read().await;
        (s.len(), hex::encode(s.root()))
    }
}
