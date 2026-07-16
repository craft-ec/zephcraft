//! K1 anchor dispatcher — the read/dispatch half of K1 (ECONOMIC_LAYER_DESIGN.md §5; TOKEN_LEDGER_BUILD.md §3).
//!
//! Governance already folds `SetProgram`/`SetConfig` into a name→cid table + a config table (the plural
//! anchor table — [`GovernanceChainStore::resolve`]/[`resolve_config`](GovernanceChainStore::resolve_config));
//! this is only the WRITE side. The dispatcher adds the missing READ/dispatch path: resolve a canonical
//! NAME to its currently-pinned program cid + interface version, and invoke it through the existing
//! [`InvokeService`].
//!
//! A K1-anchored program is **network-owned** — it has no owner keypair. Its `program_owner` (which drives
//! the `attest`/`sequence` host fns' quorum lookup) resolves to a deterministic **sentinel**
//! `pda(cid, ...)`, never a real keyholder; the quorum for that sentinel is answered by the epoch committee
//! (§10.5 / phase 4e), not an owner-signed `AttestStore`. This keeps the owner-signed attestation trust
//! model untouched for *user* programs while routing anchored protocol programs around it.

use std::sync::Arc;

use zeph_com::{pda, InvokeRequest, InvokeService};

use crate::governance::GovernanceChainStore;

/// Canonical protocol-program anchor names (governance-pinned name → program cid). Centralized here so the
/// token / economy-\* split (docs/ECONOMY_PROGRAMS_DESIGN.md) can't drift across genesis/control/dashboard.
///
/// P5 re-pinned both (was "token-ledger" / "reward"): `token` is the VALUE program — the program of every
/// account chain, so its cid is the chain's identity — and `economy-egress` is the POLICY/record program
/// (the egress valuation, formerly the standalone `reward` program, whose bytes it is). Future siblings:
/// `economy-storage`, … — each a separate anchor reusing the one `token`.
pub const TOKEN_ANCHOR: &str = "token";
pub const ECONOMY_EGRESS_ANCHOR: &str = "economy-egress";

/// A resolved anchor: the program cid governance currently pins to a name, plus its interface version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnchorResolution {
    pub cid: [u8; 32],
    pub interface_version: u32,
}

/// Fixed seed for the deterministic sentinel owner of an anchored (network-owned) program.
const ANCHOR_OWNER_SEED: &[u8] = b"craftec/anchor-owner/1";

pub struct AnchorDispatcher {
    governance: Arc<GovernanceChainStore>,
    invoke: Arc<InvokeService>,
}

impl AnchorDispatcher {
    pub fn new(governance: Arc<GovernanceChainStore>, invoke: Arc<InvokeService>) -> Self {
        Self { governance, invoke }
    }

    /// Resolve a canonical anchor NAME → its currently-pinned program cid + interface version. `None`
    /// if governance pins no program to that name. The interface version is the governed `SetConfig`
    /// value at key `anchor:<name>:iface`, defaulting to `1` when unset (or set to a negative sentinel).
    pub async fn resolve(&self, name: &str) -> Option<AnchorResolution> {
        let cid = self.governance.resolve(name).await?;
        let interface_version = self
            .governance
            .resolve_config(&format!("anchor:{name}:iface"))
            .await
            .filter(|v| *v >= 0)
            .map(|v| v as u32)
            .unwrap_or(1);
        Some(AnchorResolution {
            cid,
            interface_version,
        })
    }

    /// The deterministic **sentinel owner** of an anchored program — a PDA of its cid, with no keypair.
    /// Passed as `program_owner` so the `attest`/`sequence` quorum lookup targets the epoch committee
    /// (phase 4e), never a real keyholder (a network-owned program has none). Deterministic, so every
    /// node derives the identical sentinel.
    pub fn anchor_owner(cid: &[u8; 32]) -> [u8; 32] {
        pda(cid, ANCHOR_OWNER_SEED).0
    }

    /// Resolve `name` and invoke `func` on the pinned program, with the sentinel owner driving the
    /// quorum lookup. Returns the program's committed output. Errors if nothing is anchored at `name`.
    pub async fn invoke_anchor(
        &self,
        name: &str,
        func: &str,
        input: Vec<u8>,
        caller: [u8; 32],
    ) -> anyhow::Result<Vec<u8>> {
        let res = self
            .resolve(name)
            .await
            .ok_or_else(|| anyhow::anyhow!("no program anchored at `{name}`"))?;
        let owner = Self::anchor_owner(&res.cid);
        let req = InvokeRequest {
            app_ns: name.to_string(),
            wasm_cid: res.cid,
            func: func.to_string(),
            input,
        };
        self.invoke.invoke(&req, caller, Some(owner)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sentinel_owner_is_deterministic_and_cid_bound() {
        let cid_a = [1u8; 32];
        let cid_b = [2u8; 32];
        // Deterministic: same cid → same sentinel every time (every node derives the identical owner).
        assert_eq!(
            AnchorDispatcher::anchor_owner(&cid_a),
            AnchorDispatcher::anchor_owner(&cid_a)
        );
        // Cid-bound: a different program cid → a different sentinel owner.
        assert_ne!(
            AnchorDispatcher::anchor_owner(&cid_a),
            AnchorDispatcher::anchor_owner(&cid_b)
        );
        // The sentinel is never the zero/identity account.
        assert_ne!(AnchorDispatcher::anchor_owner(&cid_a), [0u8; 32]);
    }
}
