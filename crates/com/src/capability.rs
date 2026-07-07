//! Per-program **capability grant** — the policy that decides which host functions a
//! program may import (`COMPUTE_EXECUTION_DESIGN.md` §5).
//!
//! Enforcement is at **link time**: the runtime binds *only* the granted imports, so a
//! program importing a capability it wasn't granted **fails to instantiate**
//! (`unknown import`) and cannot escape its grant. The **native default is the
//! [`CapabilityGrant::deterministic`] profile** — the safe (agreeable) floor: a program is
//! consensus-agreeable unless explicitly granted more (fail safe). This is a
//! `MINIMAL_KERNEL` anchor: mechanism binds the surface, the grant is the policy.
//!
//! The full [`Capability`] surface (§4) is bound in the transition runtime: the
//! deterministic caps (`Input`/`Caller`/`State`/`Commit`/`Crypto`/`Sql`/`Obj`/`Clock`, the
//! last being the *consensus* clock — `ctx.now`) plus the non-deterministic `WallClock`
//! (real per-node wall-time, app profile only). `Random` is a reserved variant with NO bound
//! host fn (kernel primitive K2, deferred), so no profile — not even `full` — grants it.

use std::collections::HashSet;

/// The unified host-function surface (`COMPUTE_EXECUTION_DESIGN.md` §4). Each variant tags
/// one capability; the grant decides which are bound at link time. The ✅ deterministic
/// subset is safe for consensus-critical programs; `WallClock` is host-varying and belongs
/// to the app profile only. `Random` is reserved (no bound host fn yet) and is granted by no
/// profile.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Capability {
    /// `input` — read the request/args. Deterministic.
    Input,
    /// `caller` — who invoked. Deterministic.
    Caller,
    /// `state` — read the prior account-blob state. Deterministic.
    State,
    /// `commit` — write the new account-blob state. Deterministic.
    Commit,
    /// `ed25519_verify` (+ hash) — pure crypto. Deterministic.
    Crypto,
    /// `sql_query` / `sql_execute` — CraftSQL structured state. Deterministic.
    Sql,
    /// `obj_get` / `obj_put` — content-addressed object store. Deterministic.
    Obj,
    /// `clock` — consensus time (§6), never raw wall-clock. Deterministic.
    Clock,
    /// `wall_clock` — true wall-time. Non-deterministic; app profile only.
    WallClock,
    /// reserved — binding is kernel primitive K2 (deferred): it interacts with
    /// verification/replay, since a random app can't be re-verified unless the randomness
    /// is request-seeded. NOT bound by any profile (including `full`) until then.
    #[allow(dead_code)]
    Random,
}

/// The set of [`Capability`]s a program is granted. A capability not in the set is **not
/// bound**, so a program importing it fails to instantiate (`COMPUTE_EXECUTION_DESIGN.md`
/// §5). The default the runtime hands out is [`CapabilityGrant::deterministic`] — the safe
/// floor.
#[derive(Clone)]
pub struct CapabilityGrant {
    caps: HashSet<Capability>,
}

impl CapabilityGrant {
    /// The **deterministic profile** — the ✅ subset only (§5). For consensus-critical
    /// programs (registry, governance, config, agreed program-accounts): it cannot observe
    /// anything host-varying, so every node computes the identical result. This is the
    /// native default. `Clock` IS granted here (Phase 4): it returns the CONSENSUS
    /// timestamp `ctx.now` (the writer's HLC value, already agreed by the single-writer/
    /// replica substrate), so every node reads the same "now" and the result stays
    /// reproducible — exactly §6's block-time model. Real per-node wall-time is the separate
    /// `WallClock` capability (`wall_clock` host fn), which is app-profile only.
    pub fn deterministic() -> Self {
        Self {
            caps: [
                Capability::Input,
                Capability::Caller,
                Capability::State,
                Capability::Commit,
                Capability::Crypto,
                Capability::Sql,
                Capability::Obj,
                Capability::Clock,
            ]
            .into_iter()
            .collect(),
        }
    }

    /// The **app (full) profile** — the deterministic subset **plus** `wall_clock` (§5). For
    /// userspace apps that are not consensus-critical and may be non-deterministic.
    /// `WallClock` binds the `wall_clock` host fn (real per-node wall-time). `Random` is
    /// deliberately NOT granted: it has no bound host fn (K2, deferred), and the advertised
    /// grant must match the actually-bound host fns — otherwise a full-profile app importing
    /// `random` would fail to instantiate (`unknown import`).
    pub fn full() -> Self {
        let mut g = Self::deterministic();
        g.caps.insert(Capability::WallClock);
        g
    }

    /// Whether `cap` is granted (and so should be bound at link time).
    pub fn allows(&self, cap: Capability) -> bool {
        self.caps.contains(&cap)
    }

    /// A copy of this grant with `cap` removed — for restriction / tests.
    pub fn without(&self, cap: Capability) -> Self {
        let mut caps = self.caps.clone();
        caps.remove(&cap);
        Self { caps }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_profile_is_the_deterministic_subset() {
        let g = CapabilityGrant::deterministic();
        for cap in [
            Capability::Input,
            Capability::Caller,
            Capability::State,
            Capability::Commit,
            Capability::Crypto,
            Capability::Sql,
            Capability::Obj,
            Capability::Clock,
        ] {
            assert!(g.allows(cap), "{cap:?} is deterministic → granted");
        }
        // Clock (the CONSENSUS clock, ctx.now) IS deterministic — reproducible across nodes.
        assert!(
            g.allows(Capability::Clock),
            "consensus clock is granted by the deterministic profile"
        );
        // Real per-node wall-time (`wall_clock`) is host-varying → NOT deterministic.
        assert!(!g.allows(Capability::WallClock), "wall-clock is non-det");
        assert!(!g.allows(Capability::Random), "random is non-det");
    }

    #[test]
    fn full_profile_adds_wall_clock_but_not_random() {
        let g = CapabilityGrant::full();
        assert!(g.allows(Capability::WallClock));
        // `Random` has no bound host fn (K2, deferred), so the advertised grant must NOT
        // include it — else a full-profile app importing `random` fails to instantiate.
        assert!(
            !g.allows(Capability::Random),
            "random is not bound → not granted"
        );
        assert!(g.allows(Capability::Commit), "full ⊇ deterministic");
    }

    #[test]
    fn without_removes_exactly_one_capability() {
        let g = CapabilityGrant::deterministic().without(Capability::Commit);
        assert!(!g.allows(Capability::Commit));
        assert!(g.allows(Capability::State), "only Commit removed");
    }
}
