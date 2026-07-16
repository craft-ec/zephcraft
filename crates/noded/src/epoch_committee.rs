//! `EpochCommitteeSource` — the rotating epoch committee (ECONOMIC_LAYER_DESIGN.md §10.5;
//! TOKEN_LEDGER_BUILD.md §7). Computes a k-of-n [`Quorum`] **deterministically** from the converged
//! census + the HLC epoch, via BLAKE3 rendezvous over `(program_cid, epoch, node_id)`. Every node
//! derives the identical committee with no election messages, and the FULL committee shifts each
//! epoch. Reuses the rendezvous + epoch + converged-census pattern proven in `headreg`'s writer
//! election (but headreg rotates ONE writer within a stable set; here the whole committee rotates).
//!
//! **MVP scope (staged):** rotation works. A commit is verified against the CURRENTLY-computed
//! committee, so a commit from a PAST epoch — after the membership set has changed — may not
//! re-verify. The robust fix (per-epoch committee SNAPSHOTS + cross-epoch hand-off) is a follow-on
//! slice; until then, the ledger should gate writes on a converged census (as `headreg` does).

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use zeph_com::Quorum;
use zeph_core::hlc::Clock;
use zeph_core::Cid;
use zeph_membership::Membership;

use crate::quorum_source::QuorumSource;

/// Committee epoch length (ms) — matches `headreg`'s writer-election epoch so the two rotate in step.
const EPOCH_MILLIS: u64 = 30_000;
/// Default committee size / threshold (§10.4; a governed config value later). `2k>n` holds (6 > 4).
const N_DEFAULT: usize = 4;
const K_DEFAULT: usize = 3;
/// Rendezvous domain tag — distinct from `headreg`'s writer election so the two derive different sets.
const COMMITTEE_TAG: &[u8] = b"craftec/epoch-committee/1";

pub struct EpochCommitteeSource {
    self_id: [u8; 32],
    clock: Arc<Clock>,
    membership: RwLock<Option<Arc<Membership>>>,
}

impl EpochCommitteeSource {
    pub fn new(self_id: [u8; 32], clock: Arc<Clock>) -> Self {
        Self {
            self_id,
            clock,
            membership: RwLock::new(None),
        }
    }

    pub async fn set_membership(&self, m: Arc<Membership>) {
        *self.membership.write().await = Some(m);
    }

    /// The current committee epoch = `now / EPOCH_MILLIS`.
    fn epoch(&self) -> u64 {
        self.clock.now().millis() / EPOCH_MILLIS
    }

    /// The converged eligible set: self + every live member of the census (which excludes self), so
    /// every node computes the identical set — exactly `headreg`'s `eligible()`.
    async fn eligible(&self) -> Vec<[u8; 32]> {
        let mut ids = vec![self.self_id];
        if let Some(m) = self.membership.read().await.as_ref() {
            for (n, _addr) in m.census().await {
                if n.0 != self.self_id {
                    ids.push(n.0);
                }
            }
        }
        ids
    }

    /// The deterministic committee for `(program_cid, epoch)`: rendezvous-sort the eligible ids ASC by
    /// `blake3(TAG ‖ program_cid ‖ epoch_le ‖ node_id)`, take the low `n`, threshold `k`. Clamped to
    /// the live set so a small network still forms a valid intersection quorum (`2k>n`, majority floor).
    /// `None` if there are no eligible members.
    pub fn committee_for(
        program_cid: &[u8; 32],
        epoch: u64,
        eligible: &[[u8; 32]],
    ) -> Option<Quorum> {
        if eligible.is_empty() {
            return None;
        }
        let mut ids: Vec<[u8; 32]> = eligible.to_vec();
        ids.sort_by_key(|id| {
            Cid::of(&[COMMITTEE_TAG, program_cid, &epoch.to_le_bytes(), id].concat()).0
        });
        let n = N_DEFAULT.min(ids.len()).max(1);
        ids.truncate(n);
        // k ≤ n, and 2k>n (quorum intersection) via a majority floor for small n.
        let k = K_DEFAULT.min(n).max(n / 2 + 1);
        Some(Quorum::genesis(ids, k))
    }
}

#[async_trait]
impl QuorumSource for EpochCommitteeSource {
    async fn quorum_for(&self, _owner: &[u8; 32], program_cid: &[u8; 32]) -> Option<Quorum> {
        let eligible = self.eligible().await;
        Self::committee_for(program_cid, self.epoch(), &eligible)
    }
}

impl EpochCommitteeSource {
    /// The committee for a SPECIFIC (program, epoch) over the current converged census — used to attest /
    /// verify a past epoch's record (the `current` `quorum_for` only serves the live epoch).
    pub async fn committee_for_epoch(&self, program_cid: &[u8; 32], epoch: u64) -> Option<Quorum> {
        let eligible = self.eligible().await;
        Self::committee_for(program_cid, epoch, &eligible)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> [u8; 32] {
        [n; 32]
    }

    #[test]
    fn committee_is_deterministic_and_rotates_by_epoch() {
        let cid = [9u8; 32];
        let eligible: Vec<[u8; 32]> = (0..6).map(id).collect();
        // Deterministic: same (cid, epoch, set) → identical committee on every node.
        let a = EpochCommitteeSource::committee_for(&cid, 100, &eligible).unwrap();
        let b = EpochCommitteeSource::committee_for(&cid, 100, &eligible).unwrap();
        assert_eq!(a.members, b.members);
        assert_eq!(a.threshold, b.threshold);
        // Default size/threshold with a comfortable eligible set.
        assert_eq!(a.members.len(), N_DEFAULT);
        assert_eq!(a.threshold, K_DEFAULT);
        assert!(
            2 * a.threshold > a.members.len(),
            "intersection sizing 2k>n"
        );
        // Rotates: a different epoch generally re-selects the membership.
        let later = EpochCommitteeSource::committee_for(&cid, 101, &eligible).unwrap();
        assert!(
            later.members != a.members || eligible.len() <= N_DEFAULT,
            "the committee membership shifts across epochs"
        );
    }

    #[test]
    fn small_network_clamps_to_a_valid_intersection_quorum() {
        let cid = [1u8; 32];
        // 2 eligible → n=2, k=2 (2k>n). 1 eligible → n=1, k=1. Empty → None.
        let two = EpochCommitteeSource::committee_for(&cid, 0, &[id(1), id(2)]).unwrap();
        assert_eq!(two.members.len(), 2);
        assert_eq!(two.threshold, 2);
        let one = EpochCommitteeSource::committee_for(&cid, 0, &[id(1)]).unwrap();
        assert_eq!(one.members.len(), 1);
        assert_eq!(one.threshold, 1);
        assert!(EpochCommitteeSource::committee_for(&cid, 0, &[]).is_none());
    }
}
