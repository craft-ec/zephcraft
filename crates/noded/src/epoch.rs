//! THE epoch — the single definition of the network's rotation/close period.
//!
//! Everything that rotates or closes on an epoch derives it from here: the ordering COMMITTEE
//! (`epoch_committee`), the registry's WRITER election (`headreg`), and the economy's SETTLEMENT close
//! (`settlement_service`). They must agree: a settlement epoch is ordered and attested by *that epoch's*
//! committee, so if the two periods drift, "epoch E's committee" stops being well-defined.
//!
//! Before 2026-07-17 this constant was copy-pasted into three modules plus one bare `/ 30_000` literal in
//! `control.rs`, held in sync only by comments ("MUST match epoch_committee::EPOCH_MILLIS"). That made
//! the period effectively untunable — the knob existed four times and silently desynced the settlement
//! close from the committee that orders it if you missed one. One definition, one knob.
//!
//! **Tuning note.** Nothing needs a fast close: settlement is an accounting close, not a latency path
//! (cheques accumulate continuously and `RewardClaim` is on-demand, never an automated per-node loop), so
//! a longer epoch divides ALL per-epoch work linearly. Durations that must outlive a period change —
//! e.g. the subscription window — are expressed in TIME here and converted to epochs, never stored as a
//! raw epoch COUNT, so re-tuning cannot silently re-scale them.

use std::time::Duration;

/// The epoch period. One knob for committee rotation, registry writer election, and settlement close.
///
/// 5 MINUTES [2026-07-17, was 30s]. Nothing needs a fast close: cheques accumulate continuously and a
/// `RewardClaim` is on-demand (never an automated per-node loop), so the close is an accounting boundary,
/// not a latency path — and every per-epoch cost divides linearly by this. Knock-on effects are all
/// derived, not hardcoded: the subscription window is a DURATION (`epochs_in`), and `CLAIM_WINDOW_EPOCHS`
/// (8) now spans 40min rather than 4min — a more forgiving claim deadline, which is the right direction.
/// Committee + registry-writer terms lengthen to 5min in step, which is the point of the single knob:
/// they cannot drift apart and leave "epoch E's committee" ill-defined.
pub const EPOCH_MILLIS: u64 = 300_000;

/// The epoch index at `now_millis` — the identical derivation every subsystem (and every node) uses, so
/// all nodes agree on the current epoch from their HLC-synced clocks alone (no coordination).
pub fn epoch_at(now_millis: u64) -> u64 {
    now_millis / EPOCH_MILLIS
}

/// How many epochs span `d` — for expressing a policy window as a DURATION and deriving the epoch count,
/// so changing [`EPOCH_MILLIS`] re-derives it instead of silently rescaling the window. Rounds up, and is
/// never 0 (a window shorter than one epoch still lasts one).
pub const fn epochs_in(d: Duration) -> u64 {
    let ms = d.as_millis() as u64;
    let n = ms.div_ceil(EPOCH_MILLIS);
    if n == 0 {
        1
    } else {
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_index_is_the_shared_derivation() {
        assert_eq!(epoch_at(0), 0);
        assert_eq!(epoch_at(EPOCH_MILLIS - 1), 0);
        assert_eq!(epoch_at(EPOCH_MILLIS), 1);
        assert_eq!(epoch_at(EPOCH_MILLIS * 7 + 5), 7);
    }

    #[test]
    fn a_window_is_expressed_in_time_and_survives_retuning() {
        // 30 days is 30 days regardless of the period — the POINT of deriving instead of hardcoding a
        // count (a raw 86_400 would silently mean 300 days at a 5-minute epoch).
        let thirty_days = Duration::from_secs(30 * 24 * 3600);
        assert_eq!(
            epochs_in(thirty_days),
            thirty_days.as_millis() as u64 / EPOCH_MILLIS
        );
        // Sub-epoch windows still last one epoch (never 0 → never instantly expired).
        assert_eq!(epochs_in(Duration::from_millis(1)), 1);
        assert_eq!(epochs_in(Duration::ZERO), 1);
        // Rounds up: a window that overhangs a boundary keeps the whole final epoch.
        assert_eq!(epochs_in(Duration::from_millis(EPOCH_MILLIS + 1)), 2);
    }
}
