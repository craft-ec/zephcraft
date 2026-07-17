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

/// DEFAULT egress price: bytes of rewardable serving that ONE token buys. Governed (see
/// [`BYTES_PER_TOKEN_CONFIG_KEY`]) — this is only the genesis default.
pub const DEFAULT_BYTES_PER_TOKEN: u64 = 1 << 20; // 1 MiB per token

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
}

impl SubscriptionLedger {
    pub fn new() -> Self {
        Self {
            grants: BTreeMap::new(),
        }
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
        let bytes = tokens.saturating_mul(bytes_per_token);
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
        let Some(q) = self.grants.get_mut(consumer) else {
            return 0;
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
        l.purchase(consumer(1), 3, 1000, 0, 100); // 3 tokens × 1000 B/token = 3000 B
        assert_eq!(l.available(&consumer(1), 0), 3000);
        // A zero purchase creates nothing.
        l.purchase(consumer(2), 0, 1000, 0, 100);
        assert_eq!(l.available(&consumer(2), 0), 0);
    }

    #[test]
    fn allocation_caps_at_the_entitlement_and_the_rest_is_subsidy() {
        let mut l = SubscriptionLedger::new();
        l.purchase(consumer(1), 1, 1000, 0, 100);
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
        l.purchase(consumer(1), 1, 1000, 0, 10); // expires at epoch 10
        assert_eq!(l.available(&consumer(1), 9), 1000);
        // At expiry the entitlement is dead — use-it-or-lose-it (no escrow, no refund).
        assert_eq!(l.available(&consumer(1), 10), 0);
        assert_eq!(l.allocate(&consumer(1), 500, 10), 0);
        // A fresh purchase after expiry stands on its own.
        l.purchase(consumer(1), 2, 1000, 10, 10);
        assert_eq!(l.allocate(&consumer(1), 2500, 11), 2000);
    }

    #[test]
    fn grants_are_consumed_oldest_first_so_they_are_used_before_expiring() {
        let mut l = SubscriptionLedger::new();
        l.purchase(consumer(1), 1, 100, 0, 10); // expires at 10
        l.purchase(consumer(1), 1, 100, 5, 10); // expires at 15
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
            l.purchase(consumer(1), 5, 1000, 0, 100);
            l.purchase(consumer(2), 2, 1000, 1, 100);
            l.allocate(&consumer(1), 3000, 2);
            l.allocate(&consumer(2), 5000, 2);
            (l.available(&consumer(1), 2), l.available(&consumer(2), 2))
        };
        assert_eq!(build(), build());
        assert_eq!(build(), (2000, 0)); // c1: 5000−3000 left; c2: 2000 fully spent (3000 was subsidy)
    }
}
