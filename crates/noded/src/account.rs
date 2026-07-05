//! Generic attested accounts — the primitive behind the app-registry, exposed for ANY
//! user program. An account is `pda(program_cid, seed)`; its state is advanced by running
//! the program's WASM on `(prev_state, request)` under a k-of-n committee. A wrong output
//! can't reach quorum, so no keyholder is trusted — the program itself defines who may
//! write and what's valid (it validates the request). The app-registry is one instance of
//! this; a user counter / tally / shared-state program is another.
//!
//! State model (first cut): current-state + last attestation, persisted per account under
//! `<data_dir>/accounts/<account>.state`, and published as durable content. Optimistic:
//! one advance at a time per account (a conflicting advance re-runs against the new state).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;
use zeph_com::{
    attest_transition, epoch_of, pda, request_attestation, select_committee, verify_quorum,
    AttestRequest, AttestedRuntime, DEFAULT_FUEL,
};
use zeph_core::Cid;
use zeph_crypto::NodeIdentity;
use zeph_membership::Membership;
use zeph_obj::{ConsumeMode, ObjEngine};
use zeph_routing::ContentRouting;
use zeph_transport::Transport;

const COMMITTEE_N: usize = 5;
const COMMITTEE_K: usize = 3;
const EPOCH_MILLIS: u64 = 3_600_000;

/// Live-committee coordination inputs (set once membership is up).
struct Coordinator {
    transport: Arc<Transport>,
    membership: Arc<Membership>,
    self_id: [u8; 32],
}

/// The outcome of advancing an account.
pub struct AdvanceResult {
    pub account: [u8; 32],
    pub new_root: [u8; 32],
    pub mode: &'static str,
}

/// The node's generic attested-account store.
pub struct AttestedAccountStore {
    identity: Arc<NodeIdentity>,
    obj: Arc<ObjEngine>,
    routing: Arc<dyn ContentRouting>,
    runtime: AttestedRuntime,
    coord: RwLock<Option<Coordinator>>,
    dir: PathBuf,
}

impl AttestedAccountStore {
    pub fn open(
        identity: Arc<NodeIdentity>,
        obj: Arc<ObjEngine>,
        routing: Arc<dyn ContentRouting>,
        data_dir: &Path,
    ) -> Self {
        let dir = data_dir.join("accounts");
        let _ = std::fs::create_dir_all(&dir);
        Self {
            identity,
            obj,
            routing,
            runtime: AttestedRuntime::new().expect("attested runtime"),
            coord: RwLock::new(None),
            dir,
        }
    }

    pub async fn set_coordinator(&self, transport: Arc<Transport>, membership: Arc<Membership>) {
        let self_id = self.identity.node_id().0;
        *self.coord.write().await = Some(Coordinator {
            transport,
            membership,
            self_id,
        });
    }

    fn state_path(&self, account: [u8; 32]) -> PathBuf {
        self.dir.join(format!("{}.state", hex::encode(account)))
    }
    fn head_name(account: [u8; 32]) -> String {
        format!("\u{1}acct/{}", hex::encode(account))
    }
    fn load_state(&self, account: [u8; 32]) -> Vec<u8> {
        std::fs::read(self.state_path(account)).unwrap_or_default()
    }

    /// Fetch a program's WASM bytes by cid (following a File manifest to its content).
    async fn fetch_program(&self, cid: [u8; 32]) -> Option<Vec<u8>> {
        let raw = self.obj.get(Cid(cid), ConsumeMode::Drop).await.ok()?;
        match zeph_obj::Manifest::decode(&raw) {
            Some(zeph_obj::Manifest::File { content, .. }) => {
                self.obj.get(Cid(content), ConsumeMode::Drop).await.ok()
            }
            _ => Some(raw),
        }
    }

    /// Run the program on `(prev, request)`. `None` = the program rejected the request
    /// (empty output) or its wasm is unavailable.
    async fn run(&self, program_cid: [u8; 32], prev: &[u8], request: &[u8]) -> Option<Vec<u8>> {
        let wasm = self.fetch_program(program_cid).await?;
        let out = self
            .runtime
            .run_transition(&wasm, "run", prev, request, DEFAULT_FUEL)
            .ok()?;
        (!out.is_empty()).then_some(out)
    }

    /// Advance `account = pda(program_cid, seed)` by running its program under committee
    /// attestation. Persists the new state, publishes it durably, and returns the new root
    /// + the authority mode ("committee" or "self").
    pub async fn advance(
        &self,
        program_cid: [u8; 32],
        seed: &[u8],
        request: &[u8],
        now_millis: u64,
    ) -> anyhow::Result<AdvanceResult> {
        let account = pda(&program_cid, seed).0;
        let prev = self.load_state(account);
        let new_state = self
            .run(program_cid, &prev, request)
            .await
            .ok_or_else(|| anyhow::anyhow!("program rejected the request"))?;

        let mode = if self
            .committee_attest(program_cid, &prev, request, &new_state, now_millis)
            .await
        {
            "committee"
        } else {
            // self-attest fallback (n=1) — additive while the committee ramps up
            let _ = attest_transition(
                &self.identity,
                program_cid,
                Cid::of(&prev).0,
                request,
                &new_state,
            );
            "self"
        };

        std::fs::write(self.state_path(account), &new_state)?;
        if let Ok(cid) = self.obj.publish_system(&new_state).await {
            let _ = self
                .routing
                .announce_app(&Self::head_name(account), cid, now_millis)
                .await;
        }
        Ok(AdvanceResult {
            account,
            new_root: Cid::of(&new_state).0,
            mode,
        })
    }

    /// Fan the transition out to the epoch committee; return true iff a k-of-n quorum
    /// attests the SAME `new_state`.
    async fn committee_attest(
        &self,
        program_cid: [u8; 32],
        prev: &[u8],
        request: &[u8],
        new_state: &[u8],
        now_millis: u64,
    ) -> bool {
        let coord = self.coord.read().await;
        let Some(coord) = coord.as_ref() else {
            return false;
        };
        let snap = coord.membership.snapshot().await;
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
            return false;
        }
        let prev_root = Cid::of(prev).0;
        let req = AttestRequest {
            program_cid,
            prev_root,
            func: "run".to_string(),
            request: request.to_vec(),
            prev_state: prev.to_vec(),
        };
        let mut atts = Vec::new();
        for m in &committee.members {
            if *m == coord.self_id {
                atts.push(attest_transition(
                    &self.identity,
                    program_cid,
                    prev_root,
                    request,
                    new_state,
                ));
            } else if let Some(addr) = addr_of.get(m) {
                if let Ok(att) = request_attestation(&coord.transport, addr, &req).await {
                    atts.push(att);
                }
            }
        }
        let request_hash = Cid::of(request).0;
        matches!(
            verify_quorum(&atts, &program_cid, &prev_root, &request_hash, &committee.members, committee.k),
            Some(agreed) if agreed == Cid::of(new_state).0
        )
    }

    /// Re-announce all local account heads — TTL keep-alive + backend migration (tracker→DHT).
    pub async fn republish_all(&self, now_millis: u64) {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("state") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let Ok(state) = std::fs::read(&path) else {
                continue;
            };
            let Some(id) = hex::decode(stem)
                .ok()
                .and_then(|b| <[u8; 32]>::try_from(b).ok())
            else {
                continue;
            };
            if let Ok(cid) = self.obj.publish_system(&state).await {
                let _ = self
                    .routing
                    .announce_app(&Self::head_name(id), cid, now_millis)
                    .await;
            }
        }
    }

    /// The current state of `pda(program_cid, seed)` (local copy).
    pub async fn resolve(&self, program_cid: [u8; 32], seed: &[u8]) -> Vec<u8> {
        self.load_state(pda(&program_cid, seed).0)
    }
}
