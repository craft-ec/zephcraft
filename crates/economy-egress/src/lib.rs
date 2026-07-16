//! `zeph-economy-egress` — the ECONOMY-EGRESS protocol program: the *policy/record* authority for paid
//! egress (`ECONOMY_PROGRAMS_DESIGN.md`). It is the canonical program behind the `economy-egress` anchor,
//! replacing the old `reward` program and absorbing its valuation (design §5).
//!
//! **The split (P5).** [`zeph_token`] is the VALUE authority: it owns the account chain, folds every op,
//! and holds all balances + credit dedup. This crate owns what token deliberately doesn't know — what a
//! served byte is *worth*: the share of the epoch pool each provider is owed, and (P6) subscriptions. It
//! is **not** an account-chain transition: it never folds a balance and holds no per-account state. It is
//! a pure, STATELESS valuation of a node-built input, so a verifier re-running this wasm reproduces the
//! node's native computation exactly.
//!
//! **Why the reward *dedup* is not here.** Deduping a credit is value safety — it protects the balance —
//! so `claimed_epochs` lives in [`zeph_token::TokenState`], folded together with the credit in ONE write
//! on the provider's own chain (single-writer ⇒ atomic). This crate says what is *owed*; token says what
//! is *paid*, exactly once. That division is what removes any need for a cross-program transaction (§3).
//!
//! `#![no_std]` so the identical crate compiles for `apps/economy-egress-wasm` and native noded.

#![no_std]

extern crate alloc;

// The valuation vocabulary + math, re-exported so `zeph_economy_egress::{RewardRecord, compute, …}` is
// the single import for the economy program's callers (node settlement, the wasm wrapper, tests).
pub use zeph_reward::{compute, Contribution, RewardInput, RewardRecord, Share};

/// The WHOLE program body: decode a node-built [`RewardInput`], compute the contribution-ratio shares,
/// and return the encoded [`RewardRecord`] to commit (`None` = malformed input → commit nothing).
///
/// Stateless — a pure function of its input (no `state`), so the record is re-derivable by any node or
/// verifier from the same committed inputs. This is the epoch-close valuation the settlement loop runs
/// natively and the committee attests; P6 adds subscription policy alongside it.
pub fn run_program(input: &[u8]) -> Option<alloc::vec::Vec<u8>> {
    zeph_reward::run_reward(input)
}

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;
    use alloc::vec;

    fn provider(n: u8) -> [u8; 32] {
        [n; 32]
    }

    #[test]
    fn run_program_is_the_reward_valuation() {
        // The economy program IS the egress valuation: equal contribution → equal share of the pool.
        let input = RewardInput {
            epoch: 1,
            pool: 100,
            contributions: vec![
                Contribution {
                    provider: provider(1),
                    bytes: 50,
                },
                Contribution {
                    provider: provider(2),
                    bytes: 50,
                },
            ],
        };
        let encoded = postcard::to_allocvec(&input).unwrap();
        let out = run_program(&encoded).expect("valid input computes a record");
        let record: RewardRecord = postcard::from_bytes(&out).unwrap();
        assert_eq!(record.epoch, 1);
        assert_eq!(record.share_of(&provider(1)), 50);
        assert_eq!(record.share_of(&provider(2)), 50);
        // Reproducible: the same input re-runs to the identical record (verifier re-execution).
        assert_eq!(run_program(&encoded).unwrap(), out);
    }

    #[test]
    fn malformed_input_commits_nothing() {
        assert!(run_program(&[0xff, 0xff, 0xff]).is_none());
    }

    #[test]
    fn native_compute_matches_the_program_body() {
        // The native path (`compute`) and the wasm program body agree by construction — same crate.
        let input = RewardInput {
            epoch: 7,
            pool: 90,
            contributions: vec![Contribution {
                provider: provider(3),
                bytes: 30,
            }],
        };
        let native = compute(&input);
        let via_program: RewardRecord =
            postcard::from_bytes(&run_program(&postcard::to_allocvec(&input).unwrap()).unwrap())
                .unwrap();
        assert_eq!(native, via_program);
    }
}
