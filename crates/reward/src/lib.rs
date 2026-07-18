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

/// DEFAULT bootstrap issuance rate, in TOKENS PER DAY — **0, i.e. issuance is OFF at genesis.**
///
/// Deliberately inert until two unresolved problems are settled, because this is the one knob that
/// CREATES money and a wrong default is a live faucet:
///
/// 1. **Cold start is circular.** Issuance requires contribution; contribution counts only PAID-entitlement
///    serving (`settle_epoch_from_cheques` skips a consumer whose `subs.allocate` returns 0); entitlement is
///    bought with tokens. With every balance at 0 nothing can start, so a nonzero default would mint
///    nothing anyway — while looking like it works.
/// 2. **A lone contributor captures the whole subsidy.** Shares are a RATIO, so the only contributor in an
///    epoch takes 100% of `paid + issued`. Anyone holding a single token can pay 1, serve itself, and take
///    the full per-epoch top-up, repeatedly, up to the lifetime cap. Capping the *bytes* numerator (which
///    the existing self-dealing test does) does not cap the *issued* share riding the same ratio.
///
/// Escaping (1) means accepting unpaid contribution, which makes (2) worse — they are one problem, and it
/// needs either a genesis allocation or sybil-resistant proof of useful contribution (PDP/K5). Until then
/// the MECHANISM ships complete and tested but switched off; governance turns it on via
/// [`ISSUANCE_PER_DAY_CONFIG_KEY`] once the policy is decided.
///
/// The unit is a rate in TIME, not per epoch — the `subscription::DEFAULT_WINDOW` lesson: a per-epoch
/// figure silently changes meaning when the epoch period is retuned. For reference, 1024 tokens/day
/// subsidises ~1 GiB/day at the default 1 MiB/token price.
pub const DEFAULT_ISSUANCE_TOKENS_PER_DAY: u64 = 0;

/// Governed config key for the bootstrap issuance rate, in TOKENS PER DAY (a rate in time, see above).
pub const ISSUANCE_PER_DAY_CONFIG_KEY: &str = "economy:issuance_tokens_per_day";

/// DEFAULT lifetime ceiling on cumulative FRESH issuance, in tokens — the supply cap for minted supply.
///
/// Absolute, so it needs no period conversion. At the default rate this is ~2.7 years of uninterrupted
/// bootstrap, and far less in practice: issuance tapers to zero on its own as paid demand fills the
/// target, so the cap is a backstop against a network that never develops paid demand, not the plan.
pub const DEFAULT_ISSUANCE_TOTAL_CAP: u64 = 1_000_000;

/// Governed config key for the lifetime issuance ceiling, in TOKENS.
pub const ISSUANCE_TOTAL_CAP_CONFIG_KEY: &str = "economy:issuance_total_cap";

/// The governed issuance schedule resolved for ONE epoch, carried in the input so the record stays a pure
/// function of it — the same rule `entitlements` follows, and it matters more here: issuance is the only
/// operation that CREATES tokens, so it is the last thing that may be node-local and unreproducible.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct IssuanceParams {
    /// Bootstrap target for this epoch's DISTRIBUTABLE pool, in tokens. Issuance tops the paid pool up to
    /// this and no further — which is the whole taper: the gap shrinks as paid demand grows.
    pub target_per_epoch: u64,
    /// Lifetime ceiling on cumulative fresh issuance, in tokens.
    pub total_cap: u64,
}

/// Fresh issuance for ONE epoch: top the paid pool up toward the governed target, bounded by the lifetime
/// cap, and only when there is contribution to reward.
///
/// **The taper is structural, not a schedule.** `target − paid` shrinks as paid demand grows and reaches
/// zero once demand meets the target — the design's "bootstrap curve tapers as paid demand grows, steady
/// state is fee-recycled" with no clock to tune and no cliff to mistime. A network that develops real
/// demand stops minting because it no longer needs to, not because a timer said so.
///
/// **`has_contribution` gates it**, because shares are a RATIO of contribution: with none, every share is
/// zero and the entire pool falls to dust. Issuing into that would mint supply on an IDLE network with
/// nobody earning it — inflation for nothing, and the dust would accumulate claimable-by-no-one. No
/// contribution, no issuance.
pub fn issuance_for(
    paid_pool: u64,
    cumulative_issued: u64,
    has_contribution: bool,
    params: &IssuanceParams,
) -> u64 {
    if !has_contribution {
        return 0;
    }
    let gap = params.target_per_epoch.saturating_sub(paid_pool);
    let headroom = params.total_cap.saturating_sub(cumulative_issued);
    if gap < headroom {
        gap
    } else {
        headroom
    }
}

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
    /// The PAID pool: Σ consumers' paid egress this epoch. Fresh issuance is added ON TOP inside
    /// `compute`; this field stays what consumers actually paid, so the two are never conflated.
    pub pool: u64,
    pub contributions: Vec<Contribution>,
    #[serde(default)]
    pub entitlements: Vec<Entitlement>,
    /// Cumulative fresh issuance BEFORE this epoch — running state read off the records chain and carried
    /// in, so `compute` can enforce the lifetime cap deterministically instead of trusting the node.
    #[serde(default)]
    pub cumulative_issued: u64,
    /// The governed issuance schedule for this epoch.
    #[serde(default)]
    pub issuance: IssuanceParams,
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
    /// The DISTRIBUTABLE pool this epoch divided (the input's PAID `pool` plus any fresh `issued`).
    /// Carried so the record is
    /// self-describing — the shares' denominator is otherwise invisible — and so any node can report the
    /// pool without settling: what is left after this epoch is `pool − Σ shares` (the dust), so both the
    /// distributed and the residual figures come from this one field plus the shares.
    #[serde(default)]
    pub pool: u64,
    pub shares: Vec<Share>,
    #[serde(default)]
    pub entitlements: Vec<Entitlement>,
    /// FRESH tokens issued into this epoch's pool (0 in steady state). Carried so the record is
    /// self-describing about the thing that most needs to be auditable: `pool − issued` is what consumers
    /// actually paid, so any node can see how much of a reward was demand and how much was subsidy.
    #[serde(default)]
    pub issued: u64,
    /// Cumulative fresh issuance INCLUDING this epoch — the resulting STATE, which the next epoch's input
    /// carries back in to enforce the cap. State, so duplicate inputs take MAX and never sum (the same
    /// rule as `Share::cumulative_bytes`; summing a cumulative would double it).
    #[serde(default)]
    pub cumulative_issued: u64,
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

    /// What remained UNALLOCATED after this epoch's distribution — the integer dust `compute` could not
    /// divide (`pool − Σ shares`). This is the running pool's value at this settle, so a node that never
    /// settles can still report the pool from the durable record.
    pub fn pool_remaining(&self) -> u64 {
        let allocated: u64 = self.shares.iter().map(|s| s.amount).sum();
        self.pool.saturating_sub(allocated)
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
    // Fresh issuance tops the PAID pool up toward the governed bootstrap target; the shares below divide
    // the resulting DISTRIBUTABLE pool. Computed inside this pure function (not handed in as an inflated
    // `pool`) precisely because it creates money: a verifier re-derives the same input from committed
    // chains and re-runs this, so an over-mint cannot survive verification.
    let issued = issuance_for(
        input.pool,
        input.cumulative_issued,
        total > 0,
        &input.issuance,
    );
    let distributable = input.pool.saturating_add(issued);
    let shares = by_provider
        .into_iter() // BTreeMap iterates in sorted key order → canonical output
        .map(|(provider, (bytes, cumulative_bytes))| {
            let amount = if total == 0 {
                0
            } else {
                ((distributable as u128) * bytes / total) as u64
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
        // The DISTRIBUTABLE total (paid + issued) — the shares' actual denominator, which is what this
        // field has always meant.
        pool: distributable,
        shares,
        entitlements,
        issued,
        cumulative_issued: input.cumulative_issued.saturating_add(issued),
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
    fn the_record_carries_the_pool_so_any_node_can_report_it() {
        // The pool is the shares' denominator and the dashboard's headline number; without it in the
        // record, a node that never settles (committee-gated) reports 0 for it.
        let rec = compute(&RewardInput {
            epoch: 5,
            pool: 100,
            contributions: alloc::vec![contrib(1, 30), contrib(2, 30)],
            ..Default::default()
        });
        assert_eq!(rec.pool, 100);
        // 100 split evenly over 60 bytes = 50 each, nothing left.
        assert_eq!(rec.pool_remaining(), 0);
        // Integer dust stays in the pool: 100 over 3 equal providers pays 33 each, 1 remains.
        let dusty = compute(&RewardInput {
            epoch: 6,
            pool: 100,
            contributions: alloc::vec![contrib(1, 10), contrib(2, 10), contrib(3, 10)],
            ..Default::default()
        });
        assert_eq!(
            dusty.pool_remaining(),
            1,
            "pool − Σ shares = the undividable dust"
        );
        // A zero-contribution epoch distributes nothing, so the whole pool carries.
        let idle = compute(&RewardInput {
            epoch: 7,
            pool: 42,
            contributions: alloc::vec![],
            ..Default::default()
        });
        assert_eq!(idle.pool_remaining(), 42);
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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

    /// Issuance TAPERS as paid demand grows — the design's bootstrap curve, with no clock. It fills the
    /// gap to the target, shrinks as paid demand rises, and is exactly zero once demand meets the target
    /// (steady state = pure fee recycling, the pre-issuance model unchanged).
    #[test]
    fn issuance_tapers_to_zero_as_paid_demand_reaches_the_target() {
        let p = IssuanceParams {
            target_per_epoch: 100,
            total_cap: 1_000_000,
        };
        assert_eq!(
            issuance_for(0, 0, true, &p),
            100,
            "no demand → full subsidy"
        );
        assert_eq!(
            issuance_for(40, 0, true, &p),
            60,
            "partial demand → partial subsidy"
        );
        assert_eq!(issuance_for(99, 0, true, &p), 1, "nearly there → a sliver");
        assert_eq!(
            issuance_for(100, 0, true, &p),
            0,
            "demand met the target → NO issuance"
        );
        assert_eq!(
            issuance_for(500, 0, true, &p),
            0,
            "demand past the target → still none, never negative"
        );
    }

    /// The lifetime cap is a hard ceiling on FRESH supply, and it binds even mid-gap.
    #[test]
    fn issuance_is_bounded_by_the_lifetime_cap() {
        let p = IssuanceParams {
            target_per_epoch: 100,
            total_cap: 250,
        };
        assert_eq!(
            issuance_for(0, 200, true, &p),
            50,
            "only the headroom left under the cap"
        );
        assert_eq!(
            issuance_for(0, 250, true, &p),
            0,
            "cap reached → issuance stops forever"
        );
        assert_eq!(
            issuance_for(0, 9_999, true, &p),
            0,
            "over the cap → saturating, never wraps"
        );
    }

    /// NO CONTRIBUTION, NO ISSUANCE. Shares are a ratio of contribution, so with none every share is zero
    /// and the whole pool becomes dust — minting into that would inflate supply on an idle network with
    /// nobody earning it, and the dust would sit claimable-by-no-one.
    #[test]
    fn an_idle_network_issues_nothing() {
        let p = IssuanceParams {
            target_per_epoch: 100,
            total_cap: 1_000_000,
        };
        assert_eq!(
            issuance_for(0, 0, false, &p),
            0,
            "no contribution → no mint"
        );

        let rec = compute(&RewardInput {
            epoch: 7,
            pool: 0,
            contributions: alloc::vec![],
            entitlements: alloc::vec![],
            cumulative_issued: 0,
            issuance: p,
        });
        assert_eq!(rec.issued, 0, "idle epoch mints nothing");
        assert_eq!(rec.pool, 0, "and distributes nothing");
        assert_eq!(rec.cumulative_issued, 0, "supply is untouched");
    }

    /// End-to-end: a bootstrap epoch with real contribution issues the subsidy, distributes paid+issued by
    /// the SAME contribution ratio, stays aggregate-bounded, and advances the cumulative supply state.
    #[test]
    fn a_bootstrap_epoch_issues_distributes_and_advances_supply() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        let rec = compute(&RewardInput {
            epoch: 3,
            pool: 40, // consumers paid 40
            contributions: alloc::vec![
                Contribution {
                    provider: a,
                    bytes: 300,
                    cumulative_bytes: 300
                },
                Contribution {
                    provider: b,
                    bytes: 100,
                    cumulative_bytes: 100
                },
            ],
            entitlements: alloc::vec![],
            cumulative_issued: 10,
            issuance: IssuanceParams {
                target_per_epoch: 100,
                total_cap: 1_000_000,
            },
        });
        assert_eq!(rec.issued, 60, "topped 40 paid up to the 100 target");
        assert_eq!(rec.pool, 100, "distributable = paid + issued");
        assert_eq!(
            rec.cumulative_issued, 70,
            "supply state advanced by exactly what was issued"
        );
        // 3:1 contribution ratio over the distributable pool.
        assert_eq!(rec.share_of(&a), 75);
        assert_eq!(rec.share_of(&b), 25);
        let total_shares: u64 = rec.shares.iter().map(|s| s.amount).sum();
        assert!(
            total_shares <= rec.pool,
            "aggregate-bounded by the DISTRIBUTABLE pool"
        );
        // And the record stays self-describing about what was demand vs subsidy.
        assert_eq!(
            rec.pool - rec.issued,
            40,
            "paid-in is recoverable from the record"
        );
    }

    /// Issuance must not break the canonical-hash property P8 established: same input, any order, one
    /// byte-identical record. (Issuance is a pure function of scalar input, so it cannot reorder — this
    /// pins that it also does not vary.)
    #[test]
    fn issuance_keeps_records_canonical_and_reproducible() {
        let mk = |contribs: Vec<Contribution>| {
            compute(&RewardInput {
                epoch: 5,
                pool: 10,
                contributions: contribs,
                entitlements: alloc::vec![],
                cumulative_issued: 3,
                issuance: IssuanceParams {
                    target_per_epoch: 50,
                    total_cap: 1_000,
                },
            })
        };
        let a = [9u8; 32];
        let b = [4u8; 32];
        let one = mk(alloc::vec![
            Contribution {
                provider: a,
                bytes: 10,
                cumulative_bytes: 10
            },
            Contribution {
                provider: b,
                bytes: 20,
                cumulative_bytes: 20
            },
        ]);
        let two = mk(alloc::vec![
            Contribution {
                provider: b,
                bytes: 20,
                cumulative_bytes: 20
            },
            Contribution {
                provider: a,
                bytes: 10,
                cumulative_bytes: 10
            },
        ]);
        assert_eq!(
            one, two,
            "shuffled input → identical record, issuance included"
        );
        assert_eq!(one.issued, 40);
        assert_eq!(one.cumulative_issued, 43);
    }
}
