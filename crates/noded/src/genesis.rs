//! Genesis activation for the canonical protocol programs (TOKEN_LEDGER_BUILD.md §4; step 4 activation).
//! It makes the ledger + reward programs LIVE on the protocol:
//!
//! 1. **Publish** the embedded program wasm to obj as durable system objects, so any node — a VERIFIER
//!    re-running the canonical program, or a fresh joiner — can fetch it by cid. Content-addressed, so a
//!    publish lands at exactly the program's cid.
//! 2. **Pin** each program's K1 anchor via governance (`SetProgram name → cid`), so `token-ledger` /
//!    `reward` resolve to the current program and governance can later swap them.
//!
//! Idempotent + safe to run on every startup: publishing the same bytes is a no-op at the same cid, and
//! the anchor is only (re)pinned when this node governs AND the name isn't already at the right cid.

use std::sync::Arc;

use zeph_com::GovAction;
use zeph_core::Cid;
use zeph_obj::ObjEngine;

use crate::governance::GovernanceChainStore;
use crate::ledger::{token_program_cid, LedgerService};

/// The canonical economy-egress program (built from `apps/economy-egress-wasm`) — the referent a
/// verifier re-runs to check an epoch record. It is the egress VALUATION (formerly the standalone
/// `reward` program, whose bytes it is); balances live in the separate `token` program.
const ECONOMY_EGRESS_WASM: &[u8] = include_bytes!("../economy-egress.wasm");

/// The economy-egress program's canonical cid = the content hash of its embedded wasm.
pub fn economy_egress_program_cid() -> [u8; 32] {
    Cid::of(ECONOMY_EGRESS_WASM).0
}

use crate::anchor::{ECONOMY_EGRESS_ANCHOR, TOKEN_ANCHOR};

/// Publish the embedded protocol-program wasm to obj and pin their governance anchors. See the module
/// docs — idempotent, safe every startup.
pub async fn activate(engine: &Arc<ObjEngine>, governance: &Arc<GovernanceChainStore>) {
    let programs: [(&str, &[u8], [u8; 32]); 2] = [
        (TOKEN_ANCHOR, LedgerService::wasm(), token_program_cid()),
        (
            ECONOMY_EGRESS_ANCHOR,
            ECONOMY_EGRESS_WASM,
            economy_egress_program_cid(),
        ),
    ];

    // 1. Publish the wasm so verifiers can fetch the canonical program by cid (durable system objects).
    for (name, wasm, cid) in &programs {
        match engine.publish_system(wasm).await {
            Ok(published) if published.0 == *cid => {
                tracing::info!(anchor = name, cid = %Cid(*cid), "published protocol program wasm");
            }
            Ok(published) => {
                // Should never happen (content-addressed) — flags a stale embedded blob vs its cid.
                tracing::warn!(anchor = name, expected = %Cid(*cid), got = %published,
                    "published program cid mismatch");
            }
            Err(e) => tracing::warn!(anchor = name, error = %e, "failed to publish program wasm"),
        }
    }

    // 2. Pin the anchors (governance SetProgram), only if this node governs and the name isn't already at
    //    the right cid — so at a 1-of-1 genesis this self-applies, and it's a no-op once pinned.
    if governance.is_governor().await {
        for (name, _wasm, cid) in &programs {
            if governance.resolve(name).await == Some(*cid) {
                continue;
            }
            let approval = governance
                .draft(GovAction::SetProgram {
                    name: name.to_string(),
                    cid: *cid,
                })
                .await;
            match governance.submit(&approval).await {
                Ok(_) => {
                    tracing::info!(anchor = name, cid = %Cid(*cid), "pinned protocol-program anchor")
                }
                Err(e) => tracing::warn!(anchor = name, error = %e,
                    "anchor pin not applied (needs a k-of-n quorum?)"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn economy_egress_program_cid_is_stable_and_distinct_from_token() {
        // Content-addressed → a publish lands at exactly this cid, which the anchor pins.
        assert_eq!(economy_egress_program_cid(), economy_egress_program_cid());
        assert_ne!(economy_egress_program_cid(), [0u8; 32]);
        assert_ne!(economy_egress_program_cid(), token_program_cid());
    }
}
