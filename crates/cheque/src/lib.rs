//! Serving cheques — SWAP-style signed cumulative bandwidth tallies (`ECONOMIC_LAYER_DESIGN.md` §7;
//! §11 step 2). The measurement + payment substrate: a CONSUMER signs a running total of bytes it has
//! received from a PROVIDER; the provider accumulates these (one per consumer, monotonic), and the sum
//! is BOTH the provider's earned payment (bytes × price, applied at settlement) and its
//! serving-contribution MEASUREMENT. The cheque is the one artifact playing three roles — payment
//! instrument, fair-exchange proof, and measurement evidence.
//!
//! **Anti-forge:** the consumer's ed25519 signature covers `(server, consumer, cumulative, timestamp)`,
//! so a provider can't inflate a cheque and a cheque can't be replayed onto a different pair.
//! **Settlement-capped:** a consumer's single paid quota is allocated to its cheques FIRST-COME by
//! timestamp ([`allocate_quota`]) — the total PAID is capped at the quota (= what it paid), so a
//! self-dealing pair is zero-sum with no per-pair cap; the rest is subsidy. **Monotonic:**
//! a stale (lower) cheque is refused, so the latest supersedes all prior — O(1) storage per counterparty,
//! no per-transfer history (the SWAP insight). **Anti-FARMING** (self-serving to fake earnings) is an
//! economic-layer concern — pay-for-egress makes self-dealing zero-sum, and the metric counts paid egress
//! from distinct payers (§8) — NOT this mechanism's job: this just records signed tallies faithfully.
//!
//! This crate is the pure, offline core (types + sign/verify + accumulation + the measurement). The
//! transport hook that emits/collects cheques on the piece-serving path, and on-ledger settlement, ride
//! on top in later phases.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use zeph_core::NodeId;
use zeph_crypto::NodeIdentity;

/// Domain tag separating a serving-cheque signature from every other ed25519 use in the system.
const CHEQUE_DOMAIN: &[u8] = b"craftec/serving-cheque/1";

/// The bytes a CONSUMER signs to acknowledge a cumulative — domain-tagged, covering
/// `(server, consumer, cumulative, timestamp)` so a cheque binds to exactly one pair and can't be
/// replayed or re-timestamped after signing.
fn cheque_bytes(
    server: &[u8; 32],
    consumer: &[u8; 32],
    cumulative: u64,
    timestamp: u64,
) -> Vec<u8> {
    let mut b = Vec::with_capacity(CHEQUE_DOMAIN.len() + 80);
    b.extend_from_slice(CHEQUE_DOMAIN);
    b.extend_from_slice(server);
    b.extend_from_slice(consumer);
    b.extend_from_slice(&cumulative.to_le_bytes());
    b.extend_from_slice(&timestamp.to_le_bytes());
    b
}

/// A SWAP-style signed cumulative tally: the CONSUMER acknowledges having received `cumulative_bytes`
/// total from `server`. Each new cheque for a `(server, consumer)` pair carries a strictly higher
/// cumulative and supersedes the prior — the provider keeps only the latest.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServingCheque {
    /// The provider being paid (earns for serving) — an ed25519 pubkey.
    pub server: [u8; 32],
    /// The consumer who signs (owes) — the recipient of the served bytes, an ed25519 pubkey.
    pub consumer: [u8; 32],
    /// Cumulative bytes `server` has served `consumer` (monotonic; supersedes all prior cheques).
    pub cumulative_bytes: u64,
    /// The consumer's issue time (its wall clock) — the SETTLEMENT allocation key: a consumer's single
    /// paid quota is allocated to its cheques in timestamp order (first-come paid, the rest subsidy).
    /// Gaming it can't inflate the total (the reward is capped at the quota = what was paid) — it only
    /// reorders which provider gets the paid portion vs. subsidy, so it needs to be roughly monotonic
    /// for fairness, not trustworthy for safety.
    pub timestamp: u64,
    /// The consumer's ed25519 signature over `(server, consumer, cumulative_bytes, timestamp)`.
    pub consumer_sig: Vec<u8>,
}

impl ServingCheque {
    /// The CONSUMER signs a cheque acknowledging `cumulative_bytes` received from `server` as of
    /// `timestamp` (the consumer's issue time).
    pub fn sign(
        consumer_identity: &NodeIdentity,
        server: [u8; 32],
        cumulative_bytes: u64,
        timestamp: u64,
    ) -> Self {
        let consumer = consumer_identity.node_id().0;
        let consumer_sig = consumer_identity
            .sign(&cheque_bytes(
                &server,
                &consumer,
                cumulative_bytes,
                timestamp,
            ))
            .to_vec();
        Self {
            server,
            consumer,
            cumulative_bytes,
            timestamp,
            consumer_sig,
        }
    }

    /// Whether `consumer_sig` is a valid signature by `consumer` over this cheque — the check a provider
    /// runs before crediting it. `false` for a forged, tampered, or wrong-signer cheque.
    pub fn verify(&self) -> bool {
        let Ok(sig) = <[u8; 64]>::try_from(self.consumer_sig.as_slice()) else {
            return false;
        };
        NodeIdentity::verify(
            &NodeId(self.consumer),
            &cheque_bytes(
                &self.server,
                &self.consumer,
                self.cumulative_bytes,
                self.timestamp,
            ),
            &sig,
        )
    }
}

/// The CONSUMER's side — tracks the cumulative it has acknowledged to each provider, issuing a fresh
/// (higher) cheque as it receives more bytes. Per-segment interleaved payment (§7) calls `issue` once
/// per served chunk; the cheque returned is handed back to the provider inline with the transfer.
#[derive(Default)]
pub struct ChequeIssuer {
    /// `server → cumulative bytes acknowledged so far`.
    issued: HashMap<[u8; 32], u64>,
}

impl ChequeIssuer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Acknowledge `additional` more bytes received from `server` and issue the updated signed cheque
    /// (the new cumulative = prior + additional) stamped `timestamp`. Monotonic by construction.
    pub fn issue(
        &mut self,
        identity: &NodeIdentity,
        server: [u8; 32],
        additional: u64,
        timestamp: u64,
    ) -> ServingCheque {
        let cumulative = self.issued.entry(server).or_default();
        *cumulative = cumulative.saturating_add(additional);
        ServingCheque::sign(identity, server, *cumulative, timestamp)
    }

    /// The cumulative acknowledged to `server` so far.
    pub fn owed_to(&self, server: &[u8; 32]) -> u64 {
        self.issued.get(server).copied().unwrap_or(0)
    }
}

/// The PROVIDER's side — accumulates the latest cheque from each consumer (monotonic). The sum of
/// cumulatives is the provider's cheque-proven serving MEASUREMENT (and payment basis).
pub struct ChequeBook {
    me: [u8; 32],
    /// `consumer → the latest (highest-cumulative) valid cheque from them`.
    received: HashMap<[u8; 32], ServingCheque>,
}

impl ChequeBook {
    pub fn new(me: [u8; 32]) -> Self {
        Self {
            me,
            received: HashMap::new(),
        }
    }

    /// Rebuild from persisted cheques (re-validating each against `me`), for reload.
    pub fn load(me: [u8; 32], cheques: Vec<ServingCheque>) -> Self {
        let mut book = Self::new(me);
        for c in cheques {
            book.record(c);
        }
        book
    }

    /// Record `cheque` iff it names ME as the server, its consumer signature verifies, and its cumulative
    /// is STRICTLY higher than any prior cheque from that consumer (monotonic). Returns whether accepted
    /// (a stale / forged / mis-addressed cheque is refused).
    pub fn record(&mut self, cheque: ServingCheque) -> bool {
        if cheque.server != self.me || !cheque.verify() {
            return false;
        }
        match self.received.get(&cheque.consumer) {
            Some(prev) if cheque.cumulative_bytes <= prev.cumulative_bytes => false,
            _ => {
                self.received.insert(cheque.consumer, cheque);
                true
            }
        }
    }

    /// Total cheque-proven bytes served (sum of the latest cumulative per consumer) — the serving
    /// contribution MEASUREMENT + payment basis.
    pub fn total_earned(&self) -> u64 {
        self.received.values().map(|c| c.cumulative_bytes).sum()
    }

    /// The latest cheque from `consumer` (for settlement / audit).
    pub fn latest_from(&self, consumer: &[u8; 32]) -> Option<&ServingCheque> {
        self.received.get(consumer)
    }

    /// The recorded cheques, for persistence (`load` rebuilds from these).
    pub fn cheques(&self) -> Vec<ServingCheque> {
        self.received.values().cloned().collect()
    }
}

/// Settlement allocation (§7/§8): given a CONSUMER's cheques (the latest cumulative per provider) and
/// its paid `quota`, split each provider's owed cumulative into `(paid, subsidy)`. The quota is
/// allocated FIRST-COME by cheque timestamp until exhausted; everything beyond is subsidy. This caps the
/// consumer's total PAID (rewarded) egress at its quota = what it paid — so self-dealing is zero-sum and
/// no per-pair cap is needed. Returns `server → (paid_bytes, subsidy_bytes)`.
///
/// Pure + DETERMINISTIC (ties broken by server id), so every node — and a verifier re-running it —
/// allocates identically. The ledger calls this at settlement (step 4), paying each provider its `paid`
/// portion from the consumer's tokens and its `subsidy` portion from the pool.
pub fn allocate_quota(cheques: &[ServingCheque], quota: u64) -> HashMap<[u8; 32], (u64, u64)> {
    let mut ordered: Vec<&ServingCheque> = cheques.iter().collect();
    ordered.sort_by(|a, b| (a.timestamp, a.server).cmp(&(b.timestamp, b.server)));
    let mut remaining = quota;
    let mut out = HashMap::new();
    for c in ordered {
        let paid = remaining.min(c.cumulative_bytes);
        remaining -= paid;
        out.insert(c.server, (paid, c.cumulative_bytes - paid));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id() -> NodeIdentity {
        NodeIdentity::generate()
    }

    #[test]
    fn sign_and_verify_roundtrip_and_tamper_rejects() {
        let consumer = id();
        let server = id().node_id().0;
        let c = ServingCheque::sign(&consumer, server, 1000, 100);
        assert!(c.verify(), "a well-formed cheque verifies");
        assert_eq!(c.consumer, consumer.node_id().0);

        // tamper the cumulative → sig no longer matches
        let mut t = c.clone();
        t.cumulative_bytes = 9999;
        assert!(!t.verify(), "a bumped cumulative invalidates the signature");
        // tamper the server → replay onto a different provider fails
        let mut t2 = c.clone();
        t2.server = id().node_id().0;
        assert!(!t2.verify(), "re-pointing to a different server fails");
        // tamper the timestamp → sig no longer matches (the timestamp is signed)
        let mut t_ts = c.clone();
        t_ts.timestamp = 999;
        assert!(
            !t_ts.verify(),
            "a changed timestamp invalidates the signature"
        );
        // a garbage signature fails
        let mut t3 = c.clone();
        t3.consumer_sig = vec![0u8; 64];
        assert!(!t3.verify());
    }

    #[test]
    fn a_cheque_signed_by_a_non_consumer_is_rejected() {
        // Claim `consumer` but sign with an impostor's key → verify against `consumer` fails.
        let consumer = id().node_id().0;
        let impostor = id();
        let server = id().node_id().0;
        let mut forged = ServingCheque::sign(&impostor, server, 500, 100);
        forged.consumer = consumer; // claim the real consumer, but the sig is the impostor's
        assert!(
            !forged.verify(),
            "a cheque signed by a non-consumer is refused"
        );
    }

    #[test]
    fn provider_accumulates_monotonically_and_measures_earnings() {
        let me = id();
        let server = me.node_id().0;
        let mut book = ChequeBook::new(server);
        let alice = id();
        let mut issuer = ChequeIssuer::new();

        // Alice acknowledges 100, then 250 more (cumulative 350).
        assert!(book.record(issuer.issue(&alice, server, 100, 100)));
        assert_eq!(book.total_earned(), 100);
        assert!(book.record(issuer.issue(&alice, server, 250, 200)));
        assert_eq!(book.total_earned(), 350);
        assert_eq!(issuer.owed_to(&server), 350);

        // A stale cheque (lower cumulative than the latest) is refused.
        let stale = ServingCheque::sign(&alice, server, 200, 300);
        assert!(!book.record(stale), "a stale (lower) cheque is refused");
        assert_eq!(
            book.total_earned(),
            350,
            "earnings unchanged by the stale cheque"
        );
        // Re-recording the exact latest is also refused (not strictly higher).
        let same = ServingCheque::sign(&alice, server, 350, 400);
        assert!(!book.record(same));
    }

    #[test]
    fn a_cheque_for_a_different_server_is_refused() {
        let me = id().node_id().0;
        let other_server = id().node_id().0;
        let alice = id();
        let mut book = ChequeBook::new(me);
        // Alice's cheque names a DIFFERENT server → this book refuses it (not addressed to me).
        let c = ServingCheque::sign(&alice, other_server, 100, 100);
        assert!(
            !book.record(c),
            "a cheque for another server is not credited to me"
        );
        assert_eq!(book.total_earned(), 0);
    }

    #[test]
    fn earnings_sum_across_distinct_consumers() {
        let server = id().node_id().0;
        let mut book = ChequeBook::new(server);
        let (alice, bob) = (id(), bob_and_issue(server));
        book.record(ServingCheque::sign(&alice, server, 400, 100));
        book.record(bob.1);
        assert_eq!(book.total_earned(), 400 + 700);
        assert_eq!(book.cheques().len(), 2);
    }

    fn bob_and_issue(server: [u8; 32]) -> (NodeIdentity, ServingCheque) {
        let bob = id();
        let c = ServingCheque::sign(&bob, server, 700, 100);
        (bob, c)
    }

    #[test]
    fn load_reconstructs_the_book() {
        let server = id().node_id().0;
        let mut book = ChequeBook::new(server);
        let alice = id();
        book.record(ServingCheque::sign(&alice, server, 123, 100));
        let reloaded = ChequeBook::load(server, book.cheques());
        assert_eq!(reloaded.total_earned(), 123);
        assert!(reloaded.latest_from(&alice.node_id().0).is_some());
    }

    #[test]
    fn quota_allocation_caps_paid_at_quota_by_timestamp() {
        let alice = id();
        let (p1, p2, p3) = (id().node_id().0, id().node_id().0, id().node_id().0);
        // Alice owes three providers 100 each, timestamped 10 / 20 / 30 (given out of order to prove sort).
        let cheques = vec![
            ServingCheque::sign(&alice, p1, 100, 10),
            ServingCheque::sign(&alice, p3, 100, 30),
            ServingCheque::sign(&alice, p2, 100, 20),
        ];
        // Quota 250: first-come by timestamp — p1 (100), p2 (100), then p3 straddles (50 paid, 50 subsidy).
        let alloc = allocate_quota(&cheques, 250);
        assert_eq!(alloc[&p1], (100, 0));
        assert_eq!(alloc[&p2], (100, 0));
        assert_eq!(
            alloc[&p3],
            (50, 50),
            "the provider straddling the quota boundary is split paid/subsidy"
        );
        let total_paid: u64 = alloc.values().map(|(p, _)| *p).sum();
        assert_eq!(
            total_paid, 250,
            "total PAID never exceeds the quota — self-dealing is zero-sum"
        );

        // A quota of 0 → everything is subsidy (a free user pays nothing → all cost-reimbursed).
        let free = allocate_quota(&cheques, 0);
        assert_eq!(free[&p1], (0, 100));
        assert_eq!(free.values().map(|(p, _)| *p).sum::<u64>(), 0);
    }
}
