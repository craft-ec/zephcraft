//! `zeph-reward` — the reward-valuation policy as a pure, deterministic function + wire schemas
//! (ECONOMIC_LAYER_DESIGN.md §10.1; TOKEN_LEDGER_BUILD.md §5/§6). This is a **separate governed
//! program** (its own K1 anchor, swappable): the node runs it once per epoch at settlement close, its
//! output is independently verified (k nodes re-run this pure `compute`), and it becomes the **epoch
//! reward RECORD** that providers claim against. `#![no_std]` so the identical crate compiles for
//! the `economy-egress` program (`apps/economy-egress-wasm`) and for native node/tests.
//!
//! **Model — contribution ratio, no overflow.** Each provider's share is its ratio of the payment
//! pool: `pool × bytes_i / Σ bytes` (a uniform per-byte rate, so a provider earns the same regardless
//! of which consumer it was assigned — fair under producer-randomization). The pool is *fully*
//! attributed by ratio; there is no cost-reimbursed overflow band. Aggregate-bounded: `Σ shares ≤
//! pool` always (integer floor division; the dust `pool − Σ shares` is left unallocated → rolls
//! forward), so it can never mint more than was paid in.

#![no_std]

extern crate alloc;

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use serde::{Deserialize, Serialize};

/// One provider's rewardable contribution this epoch — its PAID-serving bytes (identified by
/// `allocate_quota` on the node from the cheques; free/reciprocal serving is excluded), plus its running
/// cumulative total. The cumulative is node-supplied state carried through to the record so a provider
/// can read its own settled figure from one row (see [`Share::cumulative_bytes`]).
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct Contribution {
    pub provider: [u8; 32],
    pub bytes: u64,
    #[serde(default)]
    pub cumulative_bytes: u64,
}

/// One consumer's egress subscription AFTER this epoch: how much entitlement it spent, and how much is
/// left unexpired.
///
/// `remaining` is deliberately the resulting STATE, not just the delta. A consumer's remaining balance
/// cannot be replayed from deltas alone: grants are bought at the GOVERNED `bytes_per_token`, which can
/// change and is recorded nowhere, so a later replay would price old purchases at today's rate and get a
/// different answer. Recording the state makes a consumer's own view a single lookup off the durable
/// chain — no replay, no price history, and correct for a node that never settles.
///
/// A row appears for every consumer that spent OR still holds entitlement, so an idle subscriber's
/// balance stays visible instead of vanishing the moment it stops consuming.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Entitlement {
    pub consumer: [u8; 32],
    /// Entitlement spent THIS epoch (funded rewardable serving).
    pub spent: u64,
    /// Unexpired entitlement remaining after this epoch — the subscriber's balance.
    pub remaining: u64,
}

/// The reward-valuation input: the epoch, its payment pool (Σ consumers' paid egress), every provider's
/// contribution (duplicate provider entries are summed), and every consumer's entitlement spend.
///
/// `entitlements` is node-supplied (the per-consumer FCFS allocation happens in the node's settlement,
/// not in this pure function) and travels through to the record verbatim. It is part of the INPUT
/// precisely so the record stays a pure function of it — a verifier re-derives the same input from
/// committed chains and re-runs `compute` to the identical record. Without that, carrying it in the
/// record only would make the record unreproducible and break verification.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct RewardInput {
    pub epoch: u64,
    pub pool: u64,
    pub contributions: Vec<Contribution>,
    #[serde(default)]
    pub entitlements: Vec<Entitlement>,
}

/// One provider's computed reward share (the amount it may CLAIM for this epoch) plus the REWARDABLE
/// BYTES that earned it. The bytes are already known here (they are the ratio's numerator) and were
/// previously discarded — carrying them makes the record a complete, self-describing summary of the
/// epoch, so any node can report its own served/settled figures from the durable records chain instead
/// of only from local settle state it may never build (settling is committee-gated).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Share {
    pub provider: [u8; 32],
    pub amount: u64,
    /// Rewardable bytes THIS epoch — the ratio's numerator, previously computed then discarded.
    #[serde(default)]
    pub bytes: u64,
    /// CUMULATIVE rewardable bytes across all epochs — the running "settled" figure. State, not a delta,
    /// for the same reason as [`Entitlement::remaining`]: summing deltas would mean reading every record
    /// ever written, whereas the state makes a provider's own view one lookup of its latest row.
    #[serde(default)]
    pub cumulative_bytes: u64,
}

/// The epoch reward RECORD — per-provider shares and per-consumer spends, both sorted by id (canonical).
/// Verified, then providers claim their share against it (`LedgerOp::RewardClaim{epoch}`).
///
/// This is the ATTESTED, durable summary of an epoch, so it carries everything needed to describe that
/// epoch without re-running the settle: who earned what (`shares.amount`), for how many bytes
/// (`shares.bytes`), and which consumers' entitlements funded it (`spends`). The records chain persists,
/// so a node that never settles can still reconstruct its own view from it.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct RewardRecord {
    pub epoch: u64,
    pub shares: Vec<Share>,
    #[serde(default)]
    pub entitlements: Vec<Entitlement>,
}

impl RewardRecord {
    /// The share owed to `provider` this epoch (0 if absent) — the node resolves this for a claim.
    pub fn share_of(&self, provider: &[u8; 32]) -> u64 {
        self.shares
            .iter()
            .find(|s| &s.provider == provider)
            .map(|s| s.amount)
            .unwrap_or(0)
    }

    /// The REWARDABLE bytes `provider` served this epoch (0 if absent) — the "settled" figure, readable
    /// from the durable record by any node rather than only by one that settled.
    pub fn bytes_of(&self, provider: &[u8; 32]) -> u64 {
        self.shares
            .iter()
            .find(|s| &s.provider == provider)
            .map(|s| s.bytes)
            .unwrap_or(0)
    }

    /// The egress entitlement `consumer` spent this epoch (0 if absent).
    pub fn spent_by(&self, consumer: &[u8; 32]) -> u64 {
        self.entitlements
            .iter()
            .find(|s| &s.consumer == consumer)
            .map(|s| s.spent)
            .unwrap_or(0)
    }

    /// `consumer`'s subscription balance after this epoch, if this record carries a row for it.
    /// `None` (not 0) when absent, so a caller can walk back to the consumer's most recent row rather
    /// than mistake "no row here" for "balance is zero".
    pub fn remaining_for(&self, consumer: &[u8; 32]) -> Option<u64> {
        self.entitlements
            .iter()
            .find(|s| &s.consumer == consumer)
            .map(|s| s.remaining)
    }

    /// `provider`'s CUMULATIVE rewardable bytes as of this epoch, if this record carries a row for it.
    /// `None` when absent — same reasoning as [`remaining_for`](Self::remaining_for).
    pub fn cumulative_bytes_of(&self, provider: &[u8; 32]) -> Option<u64> {
        self.shares
            .iter()
            .find(|s| &s.provider == provider)
            .map(|s| s.cumulative_bytes)
    }
}

/// Compute the epoch reward record: each provider's share = its **contribution ratio** of the pool,
/// `pool × bytes / Σ bytes` (uniform rate). Pure + deterministic (integer floor division; providers
/// aggregated + sorted via `BTreeMap`), so every node and every verifier re-run produces the identical
/// record. A zero pool or zero total contribution → all-zero shares. `Σ shares ≤ pool` always.
pub fn compute(input: &RewardInput) -> RewardRecord {
    // (bytes THIS epoch, cumulative). Bytes SUM across duplicate entries (they are deltas); the
    // cumulative takes the MAX — it is state, so summing duplicates would double it. Both are
    // order-independent, which is what keeps the record canonical.
    let mut by_provider: BTreeMap<[u8; 32], (u128, u64)> = BTreeMap::new();
    for c in &input.contributions {
        let e = by_provider.entry(c.provider).or_default();
        e.0 += c.bytes as u128;
        e.1 = e.1.max(c.cumulative_bytes);
    }
    let total: u128 = by_provider.values().map(|(b, _)| *b).sum();
    let shares = by_provider
        .into_iter() // BTreeMap iterates in sorted key order → canonical output
        .map(|(provider, (bytes, cumulative_bytes))| {
            let amount = if total == 0 {
                0
            } else {
                ((input.pool as u128) * bytes / total) as u64
            };
            Share {
                provider,
                amount,
                bytes: bytes as u64, // the ratio numerator, kept instead of discarded
                cumulative_bytes,
            }
        })
        .collect();
    // Entitlements travel through verbatim, but CANONICALLY: merged per consumer and sorted, so two
    // nodes handed the same allocation in a different order still produce byte-identical records (the
    // record is signed + compared by hash, so ordering is correctness, not cosmetics). `spent` sums
    // (delta); `remaining` takes the max (state) — same reasoning as the cumulative above.
    let mut by_consumer: BTreeMap<[u8; 32], (u64, u64)> = BTreeMap::new();
    for e in &input.entitlements {
        let row = by_consumer.entry(e.consumer).or_default();
        row.0 = row.0.saturating_add(e.spent);
        row.1 = row.1.max(e.remaining);
    }
    let entitlements = by_consumer
        .into_iter()
        .map(|(consumer, (spent, remaining))| Entitlement {
            consumer,
            spent,
            remaining,
        })
        .collect();
    RewardRecord {
        epoch: input.epoch,
        shares,
        entitlements,
    }
}

/// The whole program body: decode a [`RewardInput`], compute, and return the encoded [`RewardRecord`]
/// to commit. `zeph-economy-egress` (the program) and the native node both call this, so results match.
pub fn run_reward(input: &[u8]) -> Option<alloc::vec::Vec<u8>> {
    let inp: RewardInput = postcard::from_bytes(input).ok()?;
    postcard::to_allocvec(&compute(&inp)).ok()
}

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;

    fn prov(n: u8) -> [u8; 32] {
        [n; 32]
    }

    fn contrib(p: u8, bytes: u64) -> Contribution {
        Contribution {
            provider: prov(p),
            bytes,
            // 0: this helper serves the RATIO tests. Tying the cumulative to `bytes` would make a
            // duplicate-entry input claim different STATE than its pre-summed equivalent, breaking the
            // canonical-aggregation property those tests check. Cumulative-carrying tests set it.
            cumulative_bytes: 0,
        }
    }

    #[test]
    fn shares_are_the_contribution_ratio_and_bounded_by_pool() {
        // Pool 100, contributions 60/40 → shares 60/40.
        let rec = compute(&RewardInput {
            epoch: 1,
            pool: 100,
            contributions: alloc::vec![contrib(1, 60), contrib(2, 40)],
            ..Default::default()
        });
        assert_eq!(rec.share_of(&prov(1)), 60);
        assert_eq!(rec.share_of(&prov(2)), 40);
        let total: u64 = rec.shares.iter().map(|s| s.amount).sum();
        assert!(total <= 100, "Σ shares never exceeds the pool");
    }

    #[test]
    fn uniform_rate_regardless_of_which_consumer_served() {
        // Two providers with equal bytes get equal shares (the fairness property).
        let rec = compute(&RewardInput {
            epoch: 2,
            pool: 50,
            contributions: alloc::vec![contrib(1, 10), contrib(2, 10)],
            ..Default::default()
        });
        assert_eq!(rec.share_of(&prov(1)), 25);
        assert_eq!(rec.share_of(&prov(2)), 25);
    }

    #[test]
    fn empty_pool_or_zero_contribution_yields_zero_shares() {
        let no_pool = compute(&RewardInput {
            epoch: 3,
            pool: 0,
            contributions: alloc::vec![contrib(1, 100)],
            ..Default::default()
        });
        assert_eq!(no_pool.share_of(&prov(1)), 0);
        let no_bytes = compute(&RewardInput {
            epoch: 3,
            pool: 100,
            contributions: alloc::vec![contrib(1, 0), contrib(2, 0)],
            ..Default::default()
        });
        assert_eq!(no_bytes.shares.iter().map(|s| s.amount).sum::<u64>(), 0);
    }

    #[test]
    fn duplicates_aggregate_and_output_is_canonical() {
        // A provider appearing twice is summed; the record is sorted by provider id regardless of order.
        let a = compute(&RewardInput {
            epoch: 4,
            pool: 90,
            contributions: alloc::vec![contrib(2, 10), contrib(1, 10), contrib(1, 10)],
            ..Default::default()
        });
        let b = compute(&RewardInput {
            epoch: 4,
            pool: 90,
            contributions: alloc::vec![contrib(1, 20), contrib(2, 10)],
            ..Default::default()
        });
        assert_eq!(a, b, "aggregation + sort make the record canonical");
        assert_eq!(a.share_of(&prov(1)), 60); // 20/30 of 90
        assert_eq!(a.share_of(&prov(2)), 30); // 10/30 of 90
    }

    #[test]
    fn dust_from_integer_division_stays_unallocated() {
        // Pool 10, three providers 1 byte each → each floor(10/3)=3, Σ=9, dust 1 not minted.
        let rec = compute(&RewardInput {
            epoch: 5,
            pool: 10,
            contributions: alloc::vec![contrib(1, 1), contrib(2, 1), contrib(3, 1)],
            ..Default::default()
        });
        let total: u64 = rec.shares.iter().map(|s| s.amount).sum();
        assert_eq!(
            total, 9,
            "dust (10-9) is left in the pool, never over-minted"
        );
    }

    fn ent(c: u8, spent: u64, remaining: u64) -> Entitlement {
        Entitlement {
            consumer: prov(c),
            spent,
            remaining,
        }
    }

    #[test]
    fn the_record_describes_the_whole_epoch_not_just_payouts() {
        // The record is the DURABLE attested summary and settling is committee-gated, so a node that
        // never settles must still read its OWN figures straight off the records chain.
        let rec = compute(&RewardInput {
            epoch: 3,
            pool: 100,
            contributions: alloc::vec![
                Contribution {
                    provider: prov(1),
                    bytes: 60,
                    cumulative_bytes: 660
                },
                Contribution {
                    provider: prov(2),
                    bytes: 40,
                    cumulative_bytes: 40
                },
            ],
            entitlements: alloc::vec![ent(8, 70, 30), ent(9, 30, 0)],
        });
        // Payout, this epoch's bytes, and the running total all present.
        assert_eq!(rec.share_of(&prov(1)), 60);
        assert_eq!(rec.bytes_of(&prov(1)), 60);
        assert_eq!(rec.cumulative_bytes_of(&prov(1)), Some(660));
        // Consumer view: spend + resulting balance.
        assert_eq!(rec.spent_by(&prov(8)), 70);
        assert_eq!(rec.remaining_for(&prov(8)), Some(30));
        assert_eq!(rec.remaining_for(&prov(9)), Some(0)); // present, exhausted
                                                          // ABSENT is not zero: None lets a caller walk back to the account's most recent row instead of
                                                          // mistaking "didn't act this epoch" for "has nothing".
        assert_eq!(rec.remaining_for(&prov(1)), None);
        assert_eq!(rec.cumulative_bytes_of(&prov(9)), None);
    }

    #[test]
    fn records_are_canonical_so_they_hash_identically() {
        // Records are SIGNED and compared BY HASH across the committee, so a differing field order would
        // split an otherwise-agreeing quorum. Same epoch, shuffled input + duplicates => one record.
        let a = compute(&RewardInput {
            epoch: 4,
            pool: 10,
            contributions: alloc::vec![
                Contribution {
                    provider: prov(1),
                    bytes: 4,
                    cumulative_bytes: 9
                },
                Contribution {
                    provider: prov(1),
                    bytes: 6,
                    cumulative_bytes: 9
                },
            ],
            entitlements: alloc::vec![ent(9, 30, 5), ent(8, 40, 2), ent(8, 30, 2)],
        });
        let b = compute(&RewardInput {
            epoch: 4,
            pool: 10,
            contributions: alloc::vec![Contribution {
                provider: prov(1),
                bytes: 10,
                cumulative_bytes: 9
            }],
            entitlements: alloc::vec![ent(8, 70, 2), ent(9, 30, 5)],
        });
        assert_eq!(a, b, "input order / duplicates must not change the record");
        assert_eq!(
            postcard::to_allocvec(&a).unwrap(),
            postcard::to_allocvec(&b).unwrap(),
            "and must not change its BYTES (the thing the committee signs)"
        );
        // Deltas SUM across duplicates; state does NOT (summing a cumulative would double it).
        assert_eq!(a.bytes_of(&prov(1)), 10);
        assert_eq!(a.cumulative_bytes_of(&prov(1)), Some(9));
        assert_eq!(a.spent_by(&prov(8)), 70);
        assert_eq!(a.remaining_for(&prov(8)), Some(2));
    }

    #[test]
    fn run_reward_roundtrips() {
        let input = postcard::to_allocvec(&RewardInput {
            epoch: 6,
            pool: 100,
            contributions: alloc::vec![contrib(1, 75), contrib(2, 25)],
            ..Default::default()
        })
        .unwrap();
        let out = run_reward(&input).unwrap();
        let rec: RewardRecord = postcard::from_bytes(&out).unwrap();
        assert_eq!(rec.share_of(&prov(1)), 75);
    }
}
