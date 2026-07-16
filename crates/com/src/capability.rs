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
//! (real per-node wall-time) and `Random` (OS CSPRNG bytes) — both app profile only — and
//! `Verify` (the verification-orchestration host fn; app profile + the
//! [`CapabilityGrant::verifier`] re-run grant, where it is bound INERT).

use std::collections::HashSet;

/// The unified host-function surface (`COMPUTE_EXECUTION_DESIGN.md` §4). Each variant tags
/// one capability; the grant decides which are bound at link time. The ✅ deterministic
/// subset is safe for consensus-critical programs; `WallClock` and `Random` are host-varying
/// / non-reproducible and belong to the app profile only.
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
    /// `random` — OS CSPRNG bytes (kernel primitive K2). Non-reproducible, so it binds ONLY in the
    /// app (`full`) profile — never the deterministic or verifier profiles. A program that reads
    /// `random` therefore cannot be a consensus program and cannot be verified (a re-run would
    /// differ), exactly like `wall_clock` — the app accepts that tradeoff by importing it.
    Random,
    /// `verify` — a PRODUCER program's orchestration call into the **verification** primitive
    /// (consistency: "get k independent nodes to confirm `f(inputs) = claimed_output`"). Bound in
    /// the app (`full`) profile and in the [`verifier`](CapabilityGrant::verifier) re-run grant,
    /// NOT the deterministic profile (protocol programs don't orchestrate verification). During a
    /// verifier's re-run it binds **inert** (`TransitionCtx::verify_mode`) so a single-module
    /// program instantiates without recursing (`VERIFICATION_DESIGN §9`). Distinct from attestation
    /// (authority) — see `VERIFICATION_ATTESTATION_MODEL.md`.
    Verify,
    /// `attest` — a program's orchestration call into the **attestation** primitive (authority:
    /// "does my chosen quorum authorize this statement?"). Like [`Verify`](Capability::Verify): app
    /// (`full`) profile + the [`verifier`](CapabilityGrant::verifier) re-run grant (bound inert on a
    /// re-run — attestation is non-deterministic, so a verifiable pure `f` never calls it), NOT the
    /// deterministic profile. Distinct from verification (consistency).
    Attest,
    /// `sequence` — a program's orchestration call into the **ordering sequencer** (uniqueness:
    /// "commit this write at `(account, nonce)`, serialized through my quorum"). Like
    /// [`Attest`](Capability::Attest) it is app (`full`) profile + the
    /// [`verifier`](CapabilityGrant::verifier) re-run grant (bound inert on a re-run — sequencing is
    /// non-deterministic, so a verifiable pure `f` never calls it), NOT the deterministic profile.
    /// Distinct from attestation: attestation AUTHORIZES a statement (a read); the sequencer ORDERS a
    /// write (append-at-nonce), the mechanism the token ledger's account-chains ride on
    /// (`ECONOMIC_LAYER_DESIGN.md` §4).
    Sequence,
    /// `pre_grant` — a program's runtime-mediated **proxy re-encryption delegation** (sharing,
    /// kernel primitive K3). The backend derives THIS identity's PRE key and returns the *blind*
    /// re-encryption fragments delegating to a recipient (Umbral `generate_kfrags`); the app never
    /// sees the secret — the runtime mediates (`ENCRYPTION_DESIGN §13`). Like
    /// [`Random`](Capability::Random) it is non-deterministic (`generate_kfrags` draws OS randomness),
    /// so it is app (`full`) profile only; and like [`Attest`](Capability::Attest) it is bound in the
    /// [`verifier`](CapabilityGrant::verifier) re-run grant (INERT via `verify_mode`) so a
    /// single-module app whose pure `f` shares a module with a `share()` that imports `pre_grant`
    /// still links — a re-run never delegates. NOT the deterministic profile. The *grant record* is
    /// an owner-DB row (existing `sql`), and the *re-encryption transform* is pure WASM — neither
    /// needs a host fn (`ENCRYPTION_DESIGN §9b`); only the key-touching `generate_kfrags` does.
    Pre,
    /// `invoke_program` — **cross-program invocation (CPI)**: synchronously call another program's
    /// interface method and read back its committed output (`ECONOMY_PROGRAMS_DESIGN.md §4`). Unlike
    /// `verify`/`attest`/`sequence` (non-deterministic orchestration), CPI is **DETERMINISTIC** — the
    /// callee runs under the deterministic subset in its OWN reserved namespace (read-only), so a
    /// verifier re-execution reproduces the whole call tree. It therefore belongs in the deterministic
    /// profile: a consensus/economy program (e.g. the token/economy fold) can call `token.balance_of`
    /// and stay reproducible. It is a CALCULATION primitive (returns a value), never a value move — the
    /// single-writer model makes cross-program WRITES unnecessary (§3). One level only: a callee's ctx
    /// carries no invoke backend, so it cannot recurse.
    InvokeProgram,
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
                // CPI is deterministic (callee forced deterministic) → part of the consensus floor, so
                // an economy/consensus program can read another program (e.g. token.balance_of) and
                // still reproduce on a verifier re-run.
                Capability::InvokeProgram,
            ]
            .into_iter()
            .collect(),
        }
    }

    /// The **app (full) profile** — the deterministic subset **plus** `wall_clock`, `random`, and
    /// `verify`/`attest` (§5). For userspace apps that are not consensus-critical and may be
    /// non-deterministic. `WallClock` binds `wall_clock` (real per-node wall-time); `Random` binds
    /// `random` (OS CSPRNG bytes); `Verify`/`Attest` bind the cross-node orchestration host fns.
    pub fn full() -> Self {
        let mut g = Self::deterministic();
        g.caps.insert(Capability::WallClock);
        g.caps.insert(Capability::Random);
        g.caps.insert(Capability::Verify);
        g.caps.insert(Capability::Attest);
        g.caps.insert(Capability::Sequence);
        g.caps.insert(Capability::Pre);
        g
    }

    /// The **verifier re-run grant** — the deterministic subset **plus** `Verify` bound INERT. A
    /// verifier re-runs the pure `f` under this grant with [`TransitionCtx::verify_mode`] set, so a
    /// single-module program (its pure `f` sharing a module with orchestration that imports
    /// `verify`) still instantiates — the `verify` import resolves — but every `verify` call is a
    /// no-op, preventing recursion (`VERIFICATION_DESIGN §9`). It stays otherwise deterministic (no
    /// `wall_clock`), so the re-run is reproducible.
    pub fn verifier() -> Self {
        let mut g = Self::deterministic();
        g.caps.insert(Capability::Verify);
        g.caps.insert(Capability::Attest);
        g.caps.insert(Capability::Sequence);
        g.caps.insert(Capability::Pre);
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
            Capability::InvokeProgram,
        ] {
            assert!(g.allows(cap), "{cap:?} is deterministic → granted");
        }
        // CPI is deterministic (callee forced deterministic) → in the consensus floor.
        assert!(
            g.allows(Capability::InvokeProgram),
            "invoke_program (deterministic CPI) is granted by the deterministic profile"
        );
        // Clock (the CONSENSUS clock, ctx.now) IS deterministic — reproducible across nodes.
        assert!(
            g.allows(Capability::Clock),
            "consensus clock is granted by the deterministic profile"
        );
        // Real per-node wall-time (`wall_clock`) is host-varying → NOT deterministic.
        assert!(!g.allows(Capability::WallClock), "wall-clock is non-det");
        assert!(!g.allows(Capability::Random), "random is non-det");
        // `verify`/`attest` are producer orchestration (network), not a consensus-program surface.
        assert!(
            !g.allows(Capability::Verify),
            "verify orchestration is not in the deterministic profile"
        );
        assert!(
            !g.allows(Capability::Attest),
            "attest orchestration is not in the deterministic profile"
        );
        // `pre_grant` is key-touching + non-deterministic (generate_kfrags) → not a consensus surface.
        assert!(
            !g.allows(Capability::Pre),
            "pre_grant (sharing delegation) is not in the deterministic profile"
        );
        assert!(
            !g.allows(Capability::Sequence),
            "sequence (ordering) orchestration is not in the deterministic profile"
        );
    }

    #[test]
    fn full_profile_adds_wall_clock_random_and_verify() {
        let g = CapabilityGrant::full();
        assert!(g.allows(Capability::WallClock));
        // `Random` (K2) is bound in the app profile — matched by the actual `random` host fn.
        assert!(
            g.allows(Capability::Random),
            "full grants random (app-profile only)"
        );
        assert!(
            g.allows(Capability::Verify),
            "full grants verify orchestration"
        );
        assert!(
            g.allows(Capability::Attest),
            "full grants attest orchestration"
        );
        // `Pre` (K3 sharing delegation) is bound in the app profile — matched by the `pre_grant` host fn.
        assert!(
            g.allows(Capability::Pre),
            "full grants pre_grant (app-profile only)"
        );
        assert!(
            g.allows(Capability::Sequence),
            "full grants sequence (ordering) orchestration"
        );
        assert!(g.allows(Capability::Commit), "full ⊇ deterministic");
    }

    #[test]
    fn verifier_grant_is_deterministic_plus_inert_verify() {
        let g = CapabilityGrant::verifier();
        assert!(
            g.allows(Capability::Verify),
            "the re-run grant binds verify (inert via verify_mode) so a verify-importing module links"
        );
        assert!(
            g.allows(Capability::Attest),
            "the re-run grant also binds attest (inert), so an attest-importing module links"
        );
        assert!(
            g.allows(Capability::Pre),
            "the re-run grant also binds pre_grant (inert), so a share-importing module links"
        );
        assert!(
            g.allows(Capability::Sequence),
            "the re-run grant also binds sequence (inert), so a sequence-importing module links"
        );
        assert!(g.allows(Capability::Commit), "verifier ⊇ deterministic");
        // The re-run must stay reproducible: no host-varying wall-clock.
        assert!(
            !g.allows(Capability::WallClock),
            "the verifier re-run stays deterministic — no wall-clock"
        );
        assert!(!g.allows(Capability::Random));
    }

    #[test]
    fn without_removes_exactly_one_capability() {
        let g = CapabilityGrant::deterministic().without(Capability::Commit);
        assert!(!g.allows(Capability::Commit));
        assert!(g.allows(Capability::State), "only Commit removed");
    }
}
