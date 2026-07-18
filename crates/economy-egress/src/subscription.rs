//! Subscriptions — the windowed egress ENTITLEMENT a payment buys (`ECONOMY_PROGRAMS_DESIGN.md` P6;
//! `ECONOMIC_LAYER_DESIGN.md §10.1`). This is economy-egress's policy: token moves the value, this
//! decides what that value entitles you to have served.
//!
//! **The model.** Paying `t` tokens at epoch `e` buys `t × bytes_per_token` egress BYTES that expire at
//! `e + window` — a subscription. Serving a consumer within its unexpired entitlement is REWARDABLE
//! (the provider earns from the pool); serving past it is unrewarded subsidy. Unused bytes are LOST at
//! expiry: **use-it-or-lose-it, no escrow, no refund** — the tokens were already folded into the epoch
//! pool by the `Pay` write and priced into everyone's pool-average, so refunding them would mean
//! clawing back reward already paid out to providers. The subscription is a *claim on serving*, not a
//! deposit.
//!
//! **Why `bytes_per_token` is governed.** It is the egress PRICE — the one knob that says what a token
//! buys. Pool-average already sets what a provider *earns* per byte (pool ÷ rewardable bytes, derived);
//! this sets what a consumer may *spend* a token on. Governance tunes it without a code change.
//!
//! **Determinism.** Every node folds the identical committed inputs (paid cumulatives + epoch) in the
//! same order, so every node's ledger — and therefore the reward record — is bit-identical. FIFO
//! (oldest grant first) is what makes "use it before it expires" both natural and deterministic.

use core::time::Duration;

use alloc::collections::{BTreeMap, VecDeque};

/// Base units in one whole token — mirrors `zeph_token::ONE_TOKEN` (canonical); see the note there and
/// the drift test in `noded`.
pub const ONE_TOKEN: u64 = 100_000_000;

/// DEFAULT egress price: bytes of REWARDABLE SERVING that one whole token funds — 1 TiB. Amounts
/// elsewhere are BASE UNITS; `purchase` scales by [`ONE_TOKEN`].
///
/// **This is not a metered purchase of bytes.** A paid consumer's actual consumption is UNLIMITED; serving
/// past its entitlement still happens, it is merely unrewarded subsidy (see
/// `serving_beyond_a_consumers_quota_is_unrewarded_subsidy`). The entitlement exists solely so reward
/// distribution is FAIR — it caps how much provider reward one payer's money can fund, which is what stops
/// a consumer's payment from being counted many times over. Nobody ever buys a single byte, so the
/// sub-byte rounding in `purchase` is immaterial: at this price one base unit funds ~11 KB.
///
/// **Entitlement is NOT convertible to tokens, so its size cannot threaten the supply.** Entitlements are
/// pass-through in the record (never paid out); the only payout is `distributable × bytes / Σ bytes` — a
/// ratio of a FIXED pool. However large an allowance is, the amount distributed is still just the seed
/// plus what was paid; the allowance only changes how that fixed amount is SPLIT. (An earlier revision of
/// this comment claimed a 1 TiB tier at 1 MiB/token was "worth 1,048,576 tokens" and breached the supply
/// cap. That was wrong — it multiplied a ratio cap by a purchase price to invent a token figure that has
/// no meaning in distribution.)
///
/// What the price DOES govern is how generous the free tier is relative to BUYING: at 1 MiB/token, 1 TiB
/// of entitlement would cost over a million tokens, so nobody would ever pay for what they get free. At
/// 1 TiB/token the tier is worth one token's purchase, so paying more buys a meaningfully larger share.
/// That is an INCENTIVE question, not a solvency one. Governed (see
/// [`BYTES_PER_TOKEN_CONFIG_KEY`]) — this is only the genesis default.
pub const DEFAULT_BYTES_PER_TOKEN: u64 = 1 << 40; // 1 TiB per token

/// Governed config key for the egress price (`SetConfig` → every node reads the identical value).
pub const BYTES_PER_TOKEN_CONFIG_KEY: &str = "economy:bytes_per_token";

/// DEFAULT subscription window, as a DURATION — 30 days.
///
/// Deliberately NOT an epoch count. A window is a promise to the payer ("30 days of egress"), so it must
/// survive a change to the epoch period: this was originally `DEFAULT_WINDOW_EPOCHS = 86_400`, derived as
/// 30 days *at a 30s epoch*, which would have silently become 300 days the moment the epoch was retuned
/// to 5 minutes. The node converts to epochs via `epoch::epochs_in` at its own layer, where the period is
/// known. Governed via [`WINDOW_SECS_CONFIG_KEY`] — also in TIME, for the same reason.
pub const DEFAULT_WINDOW: Duration = Duration::from_secs(30 * 24 * 3600);

/// Governed config key for the subscription window, in SECONDS (a duration, not an epoch count).
pub const WINDOW_SECS_CONFIG_KEY: &str = "economy:subscription_window_secs";

/// SEEDING-PHASE PAID-TIER SUBSIDY: egress BYTES every account is granted per window WITHOUT paying —
/// i.e. everyone is put on the PAID TIER for free while the network bootstraps. 1 TiB.
///
/// **Do not confuse this with the FREE TIER.** They are different mechanisms with opposite lifetimes:
/// - The **free tier** is the reciprocity grant (`reciprocity:grant`, `noded::cheque`) — tit-for-tat
///   admission to fetch, not a token and not an entitlement. It is PERMANENT and is not touched here.
/// - **This** is a subsidy that hands out PAID-tier status without payment. It is a PHASE: in normal
///   operation the allowance must be PAID for, and governance ends the subsidy by setting this to 0.
///
/// Switching this off does NOT remove the free tier; it only stops giving paid-tier status away. Tests
/// asserting paid-only entitlement semantics disable it deliberately — they describe the end state.
///
/// **Known, accepted exposure while the SUBSIDY is on** (it ends with the subsidy, not with the free
/// tier, which stays forever). It breaks the `self_dealing_nets_at_most_what_was_paid`
/// invariant: rewardable serving is normally capped at what a consumer PAID, so self-dealing is zero-sum,
/// but a free allowance lets an account manufacture up to this many rewardable bytes having paid nothing.
/// Because shares are a ratio of a SHARED pool, that dilutes honest providers' share of real payments,
/// and identities are free so it scales with sybils. This is accepted as bounded, temporary
/// seeding-phase cost — total minting is separately capped — and is the main reason the phase must end.
///
/// **This is what makes the economy able to start at all.** Rewardable serving requires the consumer to
/// hold entitlement, entitlement was previously only bought with tokens, and tokens are only earned by
/// rewardable serving — a closed loop that could never begin from all-zero balances. Granting the tier by
/// default breaks it: serving is rewardable from genesis, so contribution exists, so the seed can be
/// earned and distributed.
///
/// It renews per window rather than being a one-time faucet (it is granted lazily and expires like any
/// purchased grant), and it is BOUNDED per account per window, which is what stops a self-dealer from
/// manufacturing unlimited rewardable bytes to dominate the contribution ratio.
pub const SEEDING_PAID_TIER_BYTES: u64 = 1 << 40; // 1 TiB

/// Governed config key for the default-tier allowance in BYTES. Setting it to 0 turns the default tier
/// OFF (payment then becomes the only route to entitlement, restoring the pre-default behaviour).
pub const SEEDING_PAID_TIER_CONFIG_KEY: &str = "economy:seeding_paid_tier_bytes";

/// One purchased entitlement: `remaining` egress bytes, unusable once `expires_at` is reached.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    /// First epoch at which this grant is dead (purchase epoch + window).
    pub expires_at: u64,
    /// Egress bytes still claimable from this grant.
    pub remaining: u64,
}

/// Per-consumer windowed egress entitlements — a FIFO of grants, oldest first.
#[derive(Clone, Debug, Default)]
pub struct SubscriptionLedger {
    grants: BTreeMap<[u8; 32], VecDeque<Grant>>,
    /// GOVERNED default-tier allowance in bytes (0 = the default tier is off).
    seeding_paid_tier: u64,
    /// Window in EPOCHS that a lazily-granted default tier lives for, mirroring a purchased grant.
    seeding_window: u64,
    /// Per account: the first epoch at which it may receive its NEXT default-tier grant.
    ///
    /// Eligibility must be tracked explicitly rather than inferred from "has no live grants": `allocate`
    /// drops a consumer's entry once its grants are exhausted, so an emptiness check would re-grant the
    /// allowance immediately on the very next call — an unlimited faucet, defeating the bound that stops a
    /// self-dealer manufacturing rewardable bytes. This is what makes it ONE allowance per window.
    seeding_next: BTreeMap<[u8; 32], u64>,
}

impl SubscriptionLedger {
    pub fn new() -> Self {
        Self {
            grants: BTreeMap::new(),
            seeding_paid_tier: SEEDING_PAID_TIER_BYTES,
            seeding_window: 0, // set by the node from the governed window; 0 → fall back at grant time
            seeding_next: BTreeMap::new(),
        }
    }

    /// Apply the GOVERNED default tier: the per-window allowance every account holds without paying, and
    /// the window it lives for. `bytes = 0` turns the default tier off.
    pub fn set_seeding_paid_tier(&mut self, bytes: u64, window_epochs: u64) {
        self.seeding_paid_tier = bytes;
        self.seeding_window = window_epochs;
    }

    /// The default-tier allowance currently in force (observability + tests).
    pub fn seeding_paid_tier(&self) -> u64 {
        self.seeding_paid_tier
    }

    /// Buy an entitlement: `tokens` paid at `epoch` → `tokens × bytes_per_token` egress bytes expiring
    /// `window` epochs later. A zero-value purchase is a no-op (no empty grant to walk later).
    pub fn purchase(
        &mut self,
        consumer: [u8; 32],
        tokens: u64,
        bytes_per_token: u64,
        epoch: u64,
        window: u64,
    ) {
        // `tokens` is in BASE UNITS while `bytes_per_token` prices a WHOLE token, so scale down by
        // ONE_TOKEN. Done in u128 so the intermediate cannot overflow, and floor-divided: a payment that
        // buys a fractional byte buys none. Rounding DOWN is the safe direction — the ledger never grants
        // entitlement that was not fully paid for.
        let bytes =
            ((tokens as u128).saturating_mul(bytes_per_token as u128) / ONE_TOKEN as u128) as u64;
        if bytes == 0 {
            return;
        }
        self.grants.entry(consumer).or_default().push_back(Grant {
            expires_at: epoch.saturating_add(window),
            remaining: bytes,
        });
    }

    /// Drop `consumer`'s expired grants — their unused bytes are LOST (use-it-or-lose-it). Grants are
    /// pushed with non-decreasing `expires_at` (window is constant per epoch and epochs only advance),
    /// so expiry is a prefix of the FIFO.
    fn expire(&mut self, consumer: &[u8; 32], epoch: u64) {
        if let Some(q) = self.grants.get_mut(consumer) {
            while q.front().is_some_and(|g| g.expires_at <= epoch) {
                q.pop_front();
            }
            if q.is_empty() {
                self.grants.remove(consumer);
            }
        }
    }

    /// Consume up to `bytes` of `consumer`'s unexpired entitlement, OLDEST GRANT FIRST, and return how
    /// much was actually allocated (≤ `bytes`). The shortfall is subsidy — served but unrewarded.
    pub fn allocate(&mut self, consumer: &[u8; 32], bytes: u64, epoch: u64) -> u64 {
        self.expire(consumer, epoch);
        // DEFAULT TIER: an account with no live entitlement is granted its allowance here, lazily, rather
        // than being refused. This is the single change that lets the economy start — without it,
        // entitlement needs tokens and tokens need entitlement, so nothing can ever be earned from an
        // all-zero genesis. Granted on demand (never for accounts that never transact) and expiring like
        // any purchased grant, so it RENEWS per window instead of being a one-time faucet.
        let eligible = epoch >= self.seeding_next.get(consumer).copied().unwrap_or(0);
        if self.seeding_paid_tier > 0 && eligible {
            let window = if self.seeding_window == 0 {
                1
            } else {
                self.seeding_window
            };
            self.grants.entry(*consumer).or_default().push_back(Grant {
                remaining: self.seeding_paid_tier,
                expires_at: epoch.saturating_add(window),
            });
            // One allowance per window: eligibility is what gates the next grant, NOT whether the last one
            // was spent. Exhausting it early buys nothing.
            self.seeding_next
                .insert(*consumer, epoch.saturating_add(window));
        }
        let Some(q) = self.grants.get_mut(consumer) else {
            return 0; // default tier off and nothing purchased → not rewardable, as before
        };
        let mut want = bytes;
        let mut used = 0u64;
        while want > 0 {
            let Some(front) = q.front_mut() else { break };
            let take = front.remaining.min(want);
            front.remaining -= take;
            used += take;
            want -= take;
            if front.remaining == 0 {
                q.pop_front();
            }
        }
        if q.is_empty() {
            self.grants.remove(consumer);
        }
        used
    }

    /// Every consumer holding unexpired entitlement, as `(consumer, remaining)` — the post-settle
    /// snapshot the epoch record carries so a subscriber can read its balance off the durable chain
    /// without settling (and without needing the historical price to replay it). Sorted (BTreeMap order)
    /// so the record stays canonical.
    pub fn balances(&self, epoch: u64) -> alloc::vec::Vec<([u8; 32], u64)> {
        self.grants
            .iter()
            .filter_map(|(c, q)| {
                let rem = q
                    .iter()
                    .filter(|g| g.expires_at > epoch)
                    .map(|g| g.remaining)
                    .fold(0u64, |a, b| a.saturating_add(b));
                (rem > 0).then_some((*c, rem))
            })
            .collect()
    }

    /// `consumer`'s unexpired remaining entitlement — the dashboard "egress left on your subscription".
    pub fn available(&self, consumer: &[u8; 32], epoch: u64) -> u64 {
        self.grants
            .get(consumer)
            .map(|q| {
                q.iter()
                    .filter(|g| g.expires_at > epoch)
                    .map(|g| g.remaining)
                    .fold(0u64, |a, b| a.saturating_add(b))
            })
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;

    fn consumer(n: u8) -> [u8; 32] {
        [n; 32]
    }

    #[test]
    fn a_purchase_buys_tokens_times_price_in_bytes() {
        let mut l = SubscriptionLedger::new();
        l.purchase(consumer(1), 3 * ONE_TOKEN, 1000, 0, 100); // 3 tokens × 1000 B/token = 3000 B
        assert_eq!(l.available(&consumer(1), 0), 3000);
        // A zero purchase creates nothing.
        l.purchase(consumer(2), 0, 1000, 0, 100);
        assert_eq!(l.available(&consumer(2), 0), 0);
    }

    #[test]
    fn allocation_caps_at_the_entitlement_and_the_rest_is_subsidy() {
        let mut l = SubscriptionLedger::new();
        // Default tier OFF: this test is about the PURCHASED entitlement cap, so the free allowance would
        // mask exactly the boundary under test. The default tier has its own test below.
        l.set_seeding_paid_tier(0, 0);
        l.purchase(consumer(1), ONE_TOKEN, 1000, 0, 100);
        // Serving 400 within entitlement → all rewardable.
        assert_eq!(l.allocate(&consumer(1), 400, 1), 400);
        assert_eq!(l.available(&consumer(1), 1), 600);
        // Serving 900 more → only the remaining 600 is rewardable; 300 is unrewarded subsidy.
        assert_eq!(l.allocate(&consumer(1), 900, 1), 600);
        assert_eq!(l.available(&consumer(1), 1), 0);
        // Exhausted → nothing more is rewardable.
        assert_eq!(l.allocate(&consumer(1), 100, 1), 0);
        // An unknown consumer has no entitlement (never paid → serving it is pure subsidy).
        assert_eq!(l.allocate(&consumer(9), 500, 1), 0);
    }

    #[test]
    fn unused_bytes_expire_and_are_never_refunded() {
        let mut l = SubscriptionLedger::new();
        l.set_seeding_paid_tier(0, 0); // isolate purchased-grant expiry from the renewing default tier
        l.purchase(consumer(1), ONE_TOKEN, 1000, 0, 10); // expires at epoch 10
        assert_eq!(l.available(&consumer(1), 9), 1000);
        // At expiry the entitlement is dead — use-it-or-lose-it (no escrow, no refund).
        assert_eq!(l.available(&consumer(1), 10), 0);
        assert_eq!(l.allocate(&consumer(1), 500, 10), 0);
        // A fresh purchase after expiry stands on its own.
        l.purchase(consumer(1), 2 * ONE_TOKEN, 1000, 10, 10);
        assert_eq!(l.allocate(&consumer(1), 2500, 11), 2000);
    }

    /// THE deadlock break. Before the default tier, an account that had never paid got zero rewardable
    /// serving — and since tokens are only earned BY rewardable serving, an all-zero genesis could never
    /// produce a first token. Everyone being on the paid tier by default means serving is rewardable from
    /// genesis, so contribution exists and the seed can actually be earned.
    #[test]
    fn every_account_is_on_the_paid_tier_by_default_so_the_economy_can_start() {
        let mut l = SubscriptionLedger::new();
        l.set_seeding_paid_tier(1000, 10);
        // A consumer that has NEVER paid anything is nonetheless served rewardably.
        assert_eq!(
            l.allocate(&consumer(7), 400, 0),
            400,
            "never paid, still rewardable — this is what lets the first token be earned"
        );
        // BOUNDED: the allowance caps what one account can make rewardable, which is what stops a
        // self-dealer manufacturing unlimited bytes to dominate the contribution ratio.
        assert_eq!(
            l.allocate(&consumer(7), 5_000, 0),
            600,
            "capped at the allowance"
        );
        assert_eq!(
            l.allocate(&consumer(7), 5_000, 0),
            0,
            "exhausted for this window"
        );
        // It RENEWS: after the window expires, the account is on the tier again (not a one-time faucet).
        assert_eq!(
            l.allocate(&consumer(7), 400, 10),
            400,
            "renewed in the next window"
        );
    }

    /// Governance can turn the default tier off, restoring payment as the only route to entitlement.
    #[test]
    fn governance_can_switch_the_default_tier_off() {
        let mut l = SubscriptionLedger::new();
        l.set_seeding_paid_tier(0, 0);
        assert_eq!(
            l.allocate(&consumer(3), 500, 0),
            0,
            "off → never paid, never rewardable"
        );
    }

    #[test]
    fn grants_are_consumed_oldest_first_so_they_are_used_before_expiring() {
        let mut l = SubscriptionLedger::new();
        // Default tier OFF: this test asserts PURCHASED-grant behaviour, which the free allowance masks.
        l.set_seeding_paid_tier(0, 0);
        l.purchase(consumer(1), ONE_TOKEN, 100, 0, 10); // expires at 10
        l.purchase(consumer(1), ONE_TOKEN, 100, 5, 10); // expires at 15
                                                        // Spending 150 drains the OLD grant (100) then dips into the new one (50).
        assert_eq!(l.allocate(&consumer(1), 150, 6), 150);
        assert_eq!(l.available(&consumer(1), 6), 50);
        // Only the newer grant survives past epoch 10 — the older one is already spent, not lost.
        assert_eq!(l.available(&consumer(1), 12), 50);
        assert_eq!(l.available(&consumer(1), 15), 0); // newer one expires too
    }

    #[test]
    fn the_ledger_is_deterministic_across_identical_folds() {
        // Two nodes folding the same committed inputs in the same order agree exactly — this is what
        // makes the reward record bit-identical network-wide.
        let build = || {
            let mut l = SubscriptionLedger::new();
            // Default tier OFF: this test asserts PURCHASED-grant behaviour, which the free allowance masks.
            l.set_seeding_paid_tier(0, 0);
            l.purchase(consumer(1), 5 * ONE_TOKEN, 1000, 0, 100);
            l.purchase(consumer(2), 2 * ONE_TOKEN, 1000, 1, 100);
            l.allocate(&consumer(1), 3000, 2);
            l.allocate(&consumer(2), 5000, 2);
            (l.available(&consumer(1), 2), l.available(&consumer(2), 2))
        };
        assert_eq!(build(), build());
        assert_eq!(build(), (2000, 0)); // c1: 5000−3000 left; c2: 2000 fully spent (3000 was subsidy)
    }
}
