//! Verification — automated cross-node **consistency** (`VERIFICATION_DESIGN.md`, re-cut
//! 2026-07-12). Answers *"is this the correct output of this deterministic program?"* by having
//! **any** node re-run the program on the exact same inputs and compare. It is consistency ONLY —
//! not authority (that is [`crate::gov`]-style quorum **attestation**, `ATTESTATION_DESIGN.md`),
//! not durability. Verifiers are interchangeable; the app's threshold `k` (how many independent
//! re-runs must agree) is the one policy knob.
//!
//! Built so far: **P1** the offline core — the [`Verdict`] a verifier signs and the
//! [`verify_locally`] re-run that produces it; **P2** the `verify` capability + host ABI (the
//! producer's orchestration call, inert on a re-run). The distribution layer (an open request board
//! with cooldown-rotated verifiers collecting `k` verdicts) rides on top in later phases; the board
//! stays "dumb" precisely because every verdict is a self-contained, signature-checkable statement.
//!
//! **Determinism boundary (what makes it sound):** the re-run uses the
//! [`CapabilityGrant::verifier`] grant (the deterministic subset — no wall-clock, no RNG — plus
//! `verify` bound INERT) and the consensus `now` carried in the request, so an honest re-run on
//! *any* node is bit-identical. A program can only be verified if everything its **pure `f`** reads
//! is an explicit input — `prev_state`, `request`, `now` — never host-varying time, and its `f`
//! never calls `verify` (orchestration does that, and orchestration is not re-run).

use serde::{Deserialize, Serialize};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;

use crate::capability::CapabilityGrant;
use crate::transition::{TransitionCtx, TransitionRuntime};

/// Domain tag separating a verdict signature from every other ed25519 use.
const VERDICT_DOMAIN: &[u8] = b"craftec/verify/verdict/1";

/// The exact deterministic run to be verified. Everything the program may read is an **explicit**
/// field here — `program_cid` + `func` (which code), `prev_state` + `request` (its inputs), and the
/// consensus `now` — so re-running it is reproducible. `now` is CARRIED, never read from host
/// wall-time; a program that wants a timestamp reads the consensus `clock` (= `ctx.now`), so the
/// same `now` reproduces the same output.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VerifyRequest {
    pub program_cid: [u8; 32],
    pub func: String,
    pub prev_state: Vec<u8>,
    pub request: Vec<u8>,
    pub now: u64,
    pub claimed_output: Vec<u8>,
}

impl VerifyRequest {
    /// A collision-resistant id binding the exact `(program_cid, func, prev_state, request, now)`
    /// being re-run. Verdicts anchor to this, so verdicts over different inputs never merge.
    pub fn request_hash(&self) -> [u8; 32] {
        let mut b = Vec::new();
        b.extend_from_slice(&self.program_cid);
        b.extend_from_slice(&(self.func.len() as u32).to_be_bytes());
        b.extend_from_slice(self.func.as_bytes());
        b.extend_from_slice(&(self.prev_state.len() as u32).to_be_bytes());
        b.extend_from_slice(&self.prev_state);
        b.extend_from_slice(&(self.request.len() as u32).to_be_bytes());
        b.extend_from_slice(&self.request);
        b.extend_from_slice(&self.now.to_be_bytes());
        Cid::of(&b).0
    }

    /// BLAKE3 of the claimed output. Verdicts carry the hash (compact on the board); two different
    /// claimed outputs for the same request hash distinctly, so their verdicts can't be pooled.
    pub fn output_hash(&self) -> [u8; 32] {
        Cid::of(&self.claimed_output).0
    }
}

/// One verifier's signed statement: *"I re-ran this exact program run and it did / did not
/// reproduce the claimed output."* The unit an open board collects: a claim is accepted once `k`
/// **distinct** verifiers signed `agree = true` over the same `(request_hash, output_hash)`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Verdict {
    /// The verifier's node id (ed25519 pubkey) — who re-ran it.
    pub verifier: [u8; 32],
    /// Binds the exact run ([`VerifyRequest::request_hash`]).
    pub request_hash: [u8; 32],
    /// Binds the claimed output ([`VerifyRequest::output_hash`]).
    pub output_hash: [u8; 32],
    /// Did the re-run reproduce the claimed output?
    pub agree: bool,
    /// ed25519 over [`verdict_signing_bytes`].
    pub signature: Vec<u8>,
}

/// The bytes a verifier signs — domain-tagged so a verdict can never be replayed as another message.
fn verdict_signing_bytes(
    verifier: &[u8; 32],
    request_hash: &[u8; 32],
    output_hash: &[u8; 32],
    agree: bool,
) -> Vec<u8> {
    let mut b = Vec::with_capacity(VERDICT_DOMAIN.len() + 32 * 3 + 1);
    b.extend_from_slice(VERDICT_DOMAIN);
    b.extend_from_slice(verifier);
    b.extend_from_slice(request_hash);
    b.extend_from_slice(output_hash);
    b.push(agree as u8);
    b
}

impl Verdict {
    /// Sign a verdict with the verifier's identity (`verifier` = the signer's node id).
    pub fn sign(
        identity: &NodeIdentity,
        request_hash: [u8; 32],
        output_hash: [u8; 32],
        agree: bool,
    ) -> Self {
        let verifier = identity.node_id().0;
        let sig = identity.sign(&verdict_signing_bytes(
            &verifier,
            &request_hash,
            &output_hash,
            agree,
        ));
        Self {
            verifier,
            request_hash,
            output_hash,
            agree,
            signature: sig.to_vec(),
        }
    }

    /// Check the verdict is authentically signed by the verifier it claims. This is an authenticity
    /// check only — it does NOT re-run the program (that is what *produced* the verdict). A
    /// collector re-checks this on every verdict before counting it toward `k`.
    pub fn verify_sig(&self) -> bool {
        let Ok(sig) = <[u8; 64]>::try_from(self.signature.as_slice()) else {
            return false;
        };
        let msg = verdict_signing_bytes(
            &self.verifier,
            &self.request_hash,
            &self.output_hash,
            self.agree,
        );
        NodeIdentity::verify(&NodeId(self.verifier), &msg, &sig)
    }
}

/// Re-run `wasm` deterministically on `req` and produce a **signed** [`Verdict`] on whether it
/// reproduces `req.claimed_output`. The re-run uses [`CapabilityGrant::verifier`] (the fail-safe
/// deterministic subset plus `verify` bound inert) and `req.now`, so an honest re-run on any node
/// yields the identical
/// output — the property that makes verification sound. A program that **traps, exceeds fuel, or
/// won't instantiate** counts as NOT reproducing the claim (`agree = false`): a bad producer can't
/// hide behind a crashing re-run. If `wasm` doesn't hash to `req.program_cid`, it's the wrong
/// program → `agree = false`.
///
/// **No self-verification** (`VERIFICATION_DESIGN §3`): a producer re-running its own output is a
/// rubber stamp, so `identity` MUST be a different node than the producer. This function can't know
/// the producer; the board/scheduler (later phases) enforces distinctness.
pub async fn verify_locally(
    runtime: &TransitionRuntime,
    identity: &NodeIdentity,
    req: &VerifyRequest,
    wasm: &[u8],
    fuel: u64,
) -> Verdict {
    let agree = if Cid::of(wasm).0 != req.program_cid {
        false // wrong program bytes for the claimed cid — nothing to verify
    } else {
        // Re-run under the VERIFIER grant + verify_mode: deterministic caps (reproducible) plus
        // `verify` bound INERT, so a single-module program (pure `f` alongside orchestration that
        // imports `verify`) still instantiates and re-runs without recursing.
        let ctx =
            TransitionCtx::deterministic(req.prev_state.clone(), req.request.clone(), req.now)
                .in_verify_mode();
        let grant = CapabilityGrant::verifier();
        matches!(
            runtime.run_program(wasm, &req.func, ctx, fuel, &grant).await,
            Ok(out) if out == req.claimed_output
        )
    };
    Verdict::sign(identity, req.request_hash(), req.output_hash(), agree)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Deterministic fixture: reads input byte, commits [input[0] * 2]. Imports only input + commit
    // (both in the deterministic profile), so it re-runs identically on every node.
    const DOUBLE_WAT: &[u8] = br#"(module
      (import "craftcom" "input"  (func $input  (param i32 i32) (result i32)))
      (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "run")
        (drop (call $input (i32.const 0) (i32.const 64)))
        (i32.store8 (i32.const 100) (i32.mul (i32.load8_u (i32.const 0)) (i32.const 2)))
        (drop (call $commit (i32.const 100) (i32.const 1)))))"#;

    // Always traps — stands in for a program that crashes on re-run.
    const TRAP_WAT: &[u8] = br#"(module (func (export "run") (unreachable)))"#;

    const FUEL: u64 = 10_000_000;

    fn req_for(wasm: &[u8], input: &[u8], claimed_output: &[u8]) -> VerifyRequest {
        VerifyRequest {
            program_cid: Cid::of(wasm).0,
            func: "run".to_string(),
            prev_state: vec![],
            request: input.to_vec(),
            now: 0,
            claimed_output: claimed_output.to_vec(),
        }
    }

    #[tokio::test]
    async fn honest_output_gets_an_agree_verdict_that_verifies() {
        let rt = TransitionRuntime::new().unwrap();
        let id = NodeIdentity::generate();
        // double(21) = 42
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let v = verify_locally(&rt, &id, &req, DOUBLE_WAT, FUEL).await;
        assert!(v.agree, "the honest claimed output re-runs to itself");
        assert!(v.verify_sig(), "the verdict is authentically signed");
        assert_eq!(v.verifier, id.node_id().0);
        assert_eq!(v.request_hash, req.request_hash());
        assert_eq!(v.output_hash, req.output_hash());
    }

    #[tokio::test]
    async fn a_wrong_claimed_output_gets_a_disagree_verdict() {
        let rt = TransitionRuntime::new().unwrap();
        let id = NodeIdentity::generate();
        // producer claims double(21) = 43 (a lie); the re-run yields 42
        let req = req_for(DOUBLE_WAT, &[21], &[43]);
        let v = verify_locally(&rt, &id, &req, DOUBLE_WAT, FUEL).await;
        assert!(!v.agree, "a forged output does not reproduce → disagree");
        assert!(v.verify_sig());
    }

    #[tokio::test]
    async fn a_trapping_program_cannot_reproduce_the_claim() {
        let rt = TransitionRuntime::new().unwrap();
        let id = NodeIdentity::generate();
        let req = req_for(TRAP_WAT, &[], &[1]); // any claimed output
        let v = verify_locally(&rt, &id, &req, TRAP_WAT, FUEL).await;
        assert!(
            !v.agree,
            "a program that traps on re-run cannot be verified"
        );
    }

    #[tokio::test]
    async fn wrong_wasm_for_the_claimed_cid_disagrees() {
        let rt = TransitionRuntime::new().unwrap();
        let id = NodeIdentity::generate();
        // req claims DOUBLE_WAT's cid, but we hand the verifier TRAP_WAT — mismatch → disagree,
        // without even running it.
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let v = verify_locally(&rt, &id, &req, TRAP_WAT, FUEL).await;
        assert!(!v.agree, "the wasm doesn't hash to the claimed program_cid");
    }

    #[tokio::test]
    async fn two_nodes_reach_the_same_agreement_deterministically() {
        // The core soundness property: independent verifiers re-run the same request and reach the
        // SAME agree bit (over the same request/output hashes), signing under their own keys.
        let rt = TransitionRuntime::new().unwrap();
        let a = NodeIdentity::generate();
        let b = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[10], &[20]);
        let va = verify_locally(&rt, &a, &req, DOUBLE_WAT, FUEL).await;
        let vb = verify_locally(&rt, &b, &req, DOUBLE_WAT, FUEL).await;
        assert!(va.agree && vb.agree, "both independent re-runs agree");
        assert_eq!(va.request_hash, vb.request_hash);
        assert_eq!(va.output_hash, vb.output_hash);
        assert_ne!(
            va.verifier, vb.verifier,
            "distinct verifiers (no self-verify)"
        );
        assert!(va.verify_sig() && vb.verify_sig());
    }

    #[test]
    fn a_tampered_verdict_fails_its_signature_check() {
        let a = NodeIdentity::generate();
        let mut v = Verdict::sign(&a, [1u8; 32], [2u8; 32], true);
        assert!(v.verify_sig());
        // flip the verdict from agree→disagree without re-signing: the signature no longer matches.
        v.agree = false;
        assert!(!v.verify_sig(), "flipping the verdict breaks the signature");
    }

    // ---- P2: the `verify` capability + host ABI (verify-mode inert; link-time gate) ----

    // A split-module program: a PURE `f` (verifiable — never calls verify) alongside an
    // `orchestrate` export that DOES call verify (producer-only, not re-run). The module imports
    // `verify`, so it only instantiates where that capability is granted.
    const SPLIT_WAT: &[u8] = br#"(module
      (import "craftcom" "input"  (func $input  (param i32 i32) (result i32)))
      (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
      (import "craftcom" "verify" (func $verify (param i32 i32 i32 i32 i32 i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "f")
        (drop (call $input (i32.const 0) (i32.const 64)))
        (i32.store8 (i32.const 100) (i32.mul (i32.load8_u (i32.const 0)) (i32.const 2)))
        (drop (call $commit (i32.const 100) (i32.const 1))))
      (func (export "orchestrate")
        (i32.store8 (i32.const 0)
          (call $verify (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0)))
        (drop (call $commit (i32.const 0) (i32.const 1)))))"#;

    #[tokio::test]
    async fn a_verify_importing_program_is_gated_by_the_capability() {
        let rt = TransitionRuntime::new().unwrap();
        // deterministic grant does NOT bind `verify` → the import can't resolve → won't instantiate.
        let det = TransitionCtx::deterministic(vec![], vec![21], 0);
        assert!(
            rt.run_program(SPLIT_WAT, "f", det, FUEL, &CapabilityGrant::deterministic())
                .await
                .is_err(),
            "a program importing `verify` fails to instantiate without the Verify capability"
        );
        // the verifier grant binds it (inert) → the same module instantiates and its pure f runs.
        let ver = TransitionCtx::deterministic(vec![], vec![21], 0).in_verify_mode();
        assert_eq!(
            rt.run_program(SPLIT_WAT, "f", ver, FUEL, &CapabilityGrant::verifier())
                .await
                .unwrap(),
            vec![42],
            "the pure f re-runs correctly under the verifier grant"
        );
    }

    #[tokio::test]
    async fn a_verify_importing_programs_pure_f_still_verifies() {
        // THE P2 GUARANTEE: even though the module imports `verify`, its pure `f` (which never
        // calls verify) verifies — the verifier grant resolves the import inert, so no recursion
        // and f's output is reproducible.
        let rt = TransitionRuntime::new().unwrap();
        let id = NodeIdentity::generate();
        let req = req_for(SPLIT_WAT, &[21], &[42]); // note: func defaults to "run"
        let req = VerifyRequest {
            func: "f".to_string(),
            ..req
        };
        let v = verify_locally(&rt, &id, &req, SPLIT_WAT, FUEL).await;
        assert!(
            v.agree,
            "the verify-importing module's pure f is verifiable"
        );
    }

    #[tokio::test]
    async fn verify_is_inert_on_a_re_run_but_reports_unavailable_to_a_producer() {
        let rt = TransitionRuntime::new().unwrap();
        // Producer mode (full profile, no board backend wired): verify() -> -1, stored as byte 0xFF.
        let producer = TransitionCtx::deterministic(vec![], vec![], 0);
        assert_eq!(
            rt.run_program(
                SPLIT_WAT,
                "orchestrate",
                producer,
                FUEL,
                &CapabilityGrant::full()
            )
            .await
            .unwrap(),
            vec![0xFF],
            "producer path reports UNAVAILABLE (-1) until the board is wired (P3)"
        );
        // Verifier re-run (verify_mode): verify() -> 2 (INERT), the recursion guard.
        let rerun = TransitionCtx::deterministic(vec![], vec![], 0).in_verify_mode();
        assert_eq!(
            rt.run_program(
                SPLIT_WAT,
                "orchestrate",
                rerun,
                FUEL,
                &CapabilityGrant::verifier()
            )
            .await
            .unwrap(),
            vec![2],
            "verify is INERT on a re-run — no recursion into nested verification"
        );
    }
}
