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
//! The full [`Capability`] surface (§4) is declared here up front; Phase 1 only *binds*
//! `Input`/`State`/`Commit`/`Crypto` in the transition runtime — the remaining
//! deterministic caps (`Caller`/`Sql`/`Obj`/`Clock`) and the non-deterministic ones
//! (`WallClock`/`Random`) are declared now and bound in later phases.

use std::collections::HashSet;

/// The unified host-function surface (`COMPUTE_EXECUTION_DESIGN.md` §4). Each variant tags
/// one capability; the grant decides which are bound at link time. The ✅ deterministic
/// subset is safe for consensus-critical programs; `WallClock`/`Random` are host-varying
/// and belong to the app profile only.
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
    /// `random` — true RNG. Non-deterministic; app profile only.
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
    /// native default. `Clock` (the future *consensus* clock, §6) is deliberately NOT here
    /// yet: the only clock host fn today reads each node's per-node HLC (`now_millis`),
    /// which is host-varying → it binds under `WallClock`, not the deterministic profile.
    /// The consensus clock is Phase 4.
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
            ]
            .into_iter()
            .collect(),
        }
    }

    /// The **app (full) profile** — the deterministic subset **plus** `wall_clock`/`random`
    /// (§5). For userspace apps that are not consensus-critical and may be
    /// non-deterministic. (`clock`/`now_millis` binds here under `WallClock` until the
    /// consensus clock lands in Phase 4; `Random` has no host fn bound yet.)
    pub fn full() -> Self {
        let mut g = Self::deterministic();
        g.caps.insert(Capability::WallClock);
        g.caps.insert(Capability::Random);
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
        ] {
            assert!(g.allows(cap), "{cap:?} is deterministic → granted");
        }
        // The per-node clock host fn binds under WallClock (host-varying); the deterministic
        // profile must NOT grant it (the consensus clock is Phase 4).
        assert!(
            !g.allows(Capability::Clock),
            "consensus clock not bound yet"
        );
        assert!(!g.allows(Capability::WallClock), "wall-clock is non-det");
        assert!(!g.allows(Capability::Random), "random is non-det");
    }

    #[test]
    fn full_profile_adds_wall_clock_and_random() {
        let g = CapabilityGrant::full();
        assert!(g.allows(Capability::WallClock));
        assert!(g.allows(Capability::Random));
        assert!(g.allows(Capability::Commit), "full ⊇ deterministic");
    }

    #[test]
    fn without_removes_exactly_one_capability() {
        let g = CapabilityGrant::deterministic().without(Capability::Commit);
        assert!(!g.allows(Capability::Commit));
        assert!(g.allows(Capability::State), "only Commit removed");
    }
}
