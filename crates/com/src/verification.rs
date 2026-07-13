//! Verification — automated cross-node **consistency** (`VERIFICATION_DESIGN.md`, re-cut
//! 2026-07-12). Answers *"is this the correct output of this deterministic program?"* by having
//! **any** node re-run the program on the exact same inputs and compare. It is consistency ONLY —
//! not authority (that is [`crate::gov`]-style quorum **attestation**, `ATTESTATION_DESIGN.md`),
//! not durability. Verifiers are interchangeable; the app's threshold `k` (how many independent
//! re-runs must agree) is the one policy knob.
//!
//! Built so far: **P1** the offline core — the [`Verdict`] a verifier signs and the
//! [`verify_locally`] re-run that produces it; **P2** the `verify` capability + host ABI (the
//! producer's orchestration call, inert on a re-run); **P3** the open request [`Board`] — a dumb,
//! append-only store of posted requests + verdicts, with the collect-to-`k` read semantics. The
//! board stays "dumb" precisely because every verdict is a self-contained, signature-checkable
//! statement, so correctness is paid back by readers ([`Board::satisfied`]) and a gossiped/merged
//! board is safe. **P4** the [`Verifier`] cooldown scheduler (rendezvous-picked, cooldown-gated
//! grabbing that forces `k` distinct verifiers + disrupts collusion) and [`Board::collected`] (the
//! ≥`k` verdict certificate). **P5a** the producer helper [`produce`] + a first consumer (a shared
//! counter) proven end-to-end over the in-memory board. **P5b-1** the board as a wire-serializable
//! **CRDT** ([`BoardSnapshot`] + [`Board::merge`]): nodes converge by exchanging snapshots (a union
//! of self-contained signed entries), so distribution needs no coordinator. What remains (P5b-2) is
//! the noded transport wiring: gossip the snapshot over the network + a verifier loop + the producer's
//! async wait.
//!
//! **Determinism boundary (what makes it sound):** the re-run uses the
//! [`CapabilityGrant::verifier`] grant (the deterministic subset — no wall-clock, no RNG — plus
//! `verify` bound INERT) and the consensus `now` carried in the request, so an honest re-run on
//! *any* node is bit-identical. A program can only be verified if everything its **pure `f`** reads
//! is an explicit input — `prev_state`, `request`, `now` — never host-varying time, and its `f`
//! never calls `verify` (orchestration does that, and orchestration is not re-run).

use std::collections::HashMap;

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

/// **Producer side** of verification: run the pure `func` on `(prev_state, request)` at consensus
/// time `now`, and package a [`VerifyRequest`] for its output — the claim `k` independent nodes will
/// confirm. It runs `func` exactly as a verifier will re-run it ([`CapabilityGrant::verifier`] +
/// verify-mode), so `claimed_output` is byte-identical to what verifiers reproduce. The producer
/// then posts this (as a [`PostedRequest`] with its policy) to the [`Board`] and waits for
/// [`Board::collected`]. Errors only if the program traps / won't run.
pub async fn produce(
    runtime: &TransitionRuntime,
    wasm: &[u8],
    func: &str,
    prev_state: &[u8],
    request: &[u8],
    now: u64,
    fuel: u64,
) -> anyhow::Result<VerifyRequest> {
    let ctx =
        TransitionCtx::deterministic(prev_state.to_vec(), request.to_vec(), now).in_verify_mode();
    let claimed_output = runtime
        .run_program(wasm, func, ctx, fuel, &CapabilityGrant::verifier())
        .await?;
    Ok(VerifyRequest {
        program_cid: Cid::of(wasm).0, // a content-addressed program's cid IS its wasm hash
        func: func.to_string(),
        prev_state: prev_state.to_vec(),
        request: request.to_vec(),
        now,
        claimed_output,
    })
}

/// Which nodes may verify a request (`VERIFICATION_DESIGN §4/§5`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum VerifierSet {
    /// Any node — the open board (the default). Cooldown-rotated grabbing (P4) spreads the load.
    Open,
    /// Only these nodes — a pre-agreed set. Lower latency (no open-board wait), at the cost of a
    /// fixed verifier pool.
    Whitelist(Vec<[u8; 32]>),
}

impl VerifierSet {
    /// Whether `node` is eligible to verify under this set.
    pub fn allows(&self, node: &[u8; 32]) -> bool {
        match self {
            VerifierSet::Open => true,
            VerifierSet::Whitelist(set) => set.contains(node),
        }
    }
}

/// The app's declared verification policy: how many DISTINCT agreeing verdicts are required (`k`)
/// and which verifiers are eligible. `k = 1, set = Open` is the baseline `verify`; a larger `k`
/// raises the bar to `k` independent colluders.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VerifyPolicy {
    pub k: u32,
    pub set: VerifierSet,
}

/// A request as it sits on the board: the run to verify, the policy, and WHO posted it. The
/// producer is recorded so it is excluded from its own verdict count — a producer cannot verify
/// itself (`VERIFICATION_DESIGN §3`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostedRequest {
    pub producer: [u8; 32],
    pub req: VerifyRequest,
    pub policy: VerifyPolicy,
}

/// A serializable snapshot of a [`Board`] — the gossip payload peers exchange. Because every entry
/// is a self-contained signed statement and the board is append-only + dedup'd, merging two
/// snapshots is a plain UNION (order-independent, idempotent), so the board is a **CRDT**: nodes
/// converge to the same board by exchanging snapshots, with no coordinator.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoardSnapshot {
    pub requests: Vec<PostedRequest>,
    pub verdicts: Vec<Verdict>,
}

/// The open verification board (`VERIFICATION_DESIGN §5`) — a **dumb**, append-only, dedup'd store
/// of posted requests and the verdicts on them. It holds NO invariant of its own: `post_*` accept
/// any well-formed entry, and ALL correctness (valid signatures, the threshold `k`, the verifier
/// set, no self-verification) is paid back by READERS in [`Board::satisfied`]. Keeping the board
/// dumb is what lets it be freely gossiped and merged — every verdict is a self-contained, signed
/// statement, so a node can trust what it collected without trusting the board.
///
/// This is the **local semantics**, fully testable offline. Making one logical board across nodes
/// (gossip/anti-entropy over the transport) is the integration layer; because the board is an
/// append-only dedup'd map, that merge is a union — order-independent and idempotent.
#[derive(Default)]
pub struct Board {
    /// `request_hash` → the posted request.
    requests: HashMap<[u8; 32], PostedRequest>,
    /// `request_hash` → (`verifier` → its verdict). The inner map dedups by verifier, so a verifier
    /// counts at most once no matter how many times its verdict is gossiped.
    verdicts: HashMap<[u8; 32], HashMap<[u8; 32], Verdict>>,
}

impl Board {
    pub fn new() -> Self {
        Self::default()
    }

    /// Post a request. Idempotent by `request_hash` — redundant posts (e.g. re-gossiped) collapse.
    pub fn post_request(&mut self, posted: PostedRequest) {
        self.requests
            .entry(posted.req.request_hash())
            .or_insert(posted);
    }

    /// Post a verdict (append; dedup by `(request_hash, verifier)`). The board is **dumb** — it does
    /// NOT check the signature here; [`Board::satisfied`] verifies on read. Redundancy is a feature:
    /// verdicts from many verifiers for the same request are all kept.
    pub fn post_verdict(&mut self, v: Verdict) {
        self.verdicts
            .entry(v.request_hash)
            .or_default()
            .insert(v.verifier, v);
    }

    /// The requests `node` may grab to verify now: it is eligible (open / whitelisted), it did NOT
    /// produce the request (no self-verification), it has not already verified it, and the request
    /// is not already satisfied. Cooldown-rotated ordering among these is P4.
    pub fn grabbable_by(&self, node: &[u8; 32]) -> Vec<&PostedRequest> {
        self.requests
            .values()
            .filter(|p| {
                p.producer != *node
                    && p.policy.set.allows(node)
                    && !self
                        .verdicts
                        .get(&p.req.request_hash())
                        .is_some_and(|m| m.contains_key(node))
                    && !self.satisfied(p)
            })
            .collect()
    }

    /// Whether `posted` has reached its policy: `k` DISTINCT verifiers whose verdicts are each
    /// (a) authentically signed, (b) over this exact `(request_hash, output_hash)`, (c) agreeing,
    /// (d) from an eligible verifier, and (e) NOT the producer. This is where the board's dumbness
    /// is repaid — every correctness check happens on read, so a gossiped/merged board is safe.
    pub fn satisfied(&self, posted: &PostedRequest) -> bool {
        self.valid_agreements(posted) >= posted.policy.k as usize
    }

    /// Count DISTINCT verifiers with a valid, agreeing verdict for `posted` (the check behind
    /// [`Board::satisfied`]).
    pub fn valid_agreements(&self, posted: &PostedRequest) -> usize {
        self.valid_verdicts(posted).len()
    }

    /// The verification **certificate** once `posted` is satisfied: the ≥`k` valid, agreeing,
    /// distinct-verifier verdicts a producer collects as proof the claim was independently verified.
    /// `None` until the policy is met. (In the wired system the producer waits for gossip to deliver
    /// these; here it is a pure read over the current board.)
    pub fn collected(&self, posted: &PostedRequest) -> Option<Vec<Verdict>> {
        let vs = self.valid_verdicts(posted);
        (vs.len() >= posted.policy.k as usize).then(|| vs.into_iter().cloned().collect())
    }

    /// The verdicts that COUNT for `posted`: authentically signed, over this exact
    /// `(request_hash, output_hash)`, agreeing, from an eligible non-producer verifier. Inner-map
    /// keys are verifiers, so these are already one-per-distinct-verifier.
    fn valid_verdicts(&self, posted: &PostedRequest) -> Vec<&Verdict> {
        let rh = posted.req.request_hash();
        let oh = posted.req.output_hash();
        let Some(vs) = self.verdicts.get(&rh) else {
            return Vec::new();
        };
        vs.values()
            .filter(|v| {
                v.agree
                    && v.request_hash == rh
                    && v.output_hash == oh
                    && v.verifier != posted.producer
                    && posted.policy.set.allows(&v.verifier)
                    && v.verify_sig()
            })
            .collect()
    }

    /// Snapshot the board for gossip — every request + verdict it currently holds.
    pub fn snapshot(&self) -> BoardSnapshot {
        BoardSnapshot {
            requests: self.requests.values().cloned().collect(),
            verdicts: self
                .verdicts
                .values()
                .flat_map(|m| m.values().cloned())
                .collect(),
        }
    }

    /// Merge a peer's snapshot — a **CRDT union**: add every request + verdict not already held
    /// (via the same idempotent [`Board::post_request`] / [`Board::post_verdict`]). Idempotent and
    /// commutative, so repeated/crossed gossip converges. Safe against a malicious snapshot: the
    /// board stays dumb, so a bad entry can only add something a reader will independently re-check
    /// ([`Board::satisfied`]) — it can't corrupt an existing entry or fabricate a certificate.
    pub fn merge(&mut self, snap: BoardSnapshot) {
        for r in snap.requests {
            self.post_request(r);
        }
        for v in snap.verdicts {
            self.post_verdict(v);
        }
    }
}

/// A rendezvous score `blake3(node ‖ request_hash)` — lets each verifier deterministically prefer a
/// *different* request (load spread), and keeps the choice unpredictable to a producer (it is keyed
/// on the verifier's own id, which the producer can't control).
fn rendezvous(node: &[u8; 32], request_hash: &[u8; 32]) -> [u8; 32] {
    let mut b = Vec::with_capacity(64);
    b.extend_from_slice(node);
    b.extend_from_slice(request_hash);
    Cid::of(&b).0
}

/// A verifier node's **cooldown scheduler** (`VERIFICATION_DESIGN §5`). After posting a verdict a
/// node holds a `cooldown` before grabbing another. That single mechanism does three jobs: it
/// **spreads load** across the fleet, it forces **`k` DISTINCT verifiers** (diversity — one node
/// can't rush to satisfy a request alone), and it **disrupts collusion** — a producer cannot steer
/// its job to a chosen colluder, because each node independently picks its *own* next job (keyed on
/// its own id) and no one assigns work.
pub struct Verifier {
    node: [u8; 32],
    cooldown_ms: u64,
    /// When this node last posted a verdict (ms since some fixed epoch). `None` = never → ready.
    last_verified_ms: Option<u64>,
}

impl Verifier {
    pub fn new(node: [u8; 32], cooldown_ms: u64) -> Self {
        Self {
            node,
            cooldown_ms,
            last_verified_ms: None,
        }
    }

    /// Off cooldown at `now_ms`?
    pub fn ready(&self, now_ms: u64) -> bool {
        match self.last_verified_ms {
            None => true,
            Some(t) => now_ms >= t.saturating_add(self.cooldown_ms),
        }
    }

    /// The request this node should verify next at `now_ms`: `None` if on cooldown or nothing is
    /// grabbable. Among grabbable requests it picks the one minimising [`rendezvous`], so different
    /// nodes prefer different requests and the producer can't predict which node grabs its job.
    pub fn select<'a>(&self, board: &'a Board, now_ms: u64) -> Option<&'a PostedRequest> {
        if !self.ready(now_ms) {
            return None;
        }
        board
            .grabbable_by(&self.node)
            .into_iter()
            .min_by_key(|p| rendezvous(&self.node, &p.req.request_hash()))
    }

    /// Record that this node just posted a verdict at `now_ms` — starts its cooldown.
    pub fn mark_verified(&mut self, now_ms: u64) {
        self.last_verified_ms = Some(now_ms);
    }
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

    // ---- P3: the open request board ----

    fn agree_verdict(id: &NodeIdentity, req: &VerifyRequest) -> Verdict {
        Verdict::sign(id, req.request_hash(), req.output_hash(), true)
    }

    fn open_posted(producer: [u8; 32], req: VerifyRequest, k: u32) -> PostedRequest {
        PostedRequest {
            producer,
            req,
            policy: VerifyPolicy {
                k,
                set: VerifierSet::Open,
            },
        }
    }

    #[test]
    fn board_collects_k_distinct_agreeing_verdicts() {
        let producer = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let posted = open_posted(producer.node_id().0, req.clone(), 2);
        let mut b = Board::new();
        b.post_request(posted.clone());
        assert!(!b.satisfied(&posted), "no verdicts yet");

        let v1 = NodeIdentity::generate();
        b.post_verdict(agree_verdict(&v1, &req));
        assert_eq!(b.valid_agreements(&posted), 1);
        assert!(!b.satisfied(&posted), "1 of 2");

        let v2 = NodeIdentity::generate();
        b.post_verdict(agree_verdict(&v2, &req));
        assert!(
            b.satisfied(&posted),
            "2 distinct agreeing verdicts meet k=2"
        );
    }

    #[test]
    fn board_dedups_a_verifier_and_ignores_disagreement() {
        let producer = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let posted = open_posted(producer.node_id().0, req.clone(), 2);
        let mut b = Board::new();
        b.post_request(posted.clone());

        let v1 = NodeIdentity::generate();
        b.post_verdict(agree_verdict(&v1, &req));
        b.post_verdict(agree_verdict(&v1, &req)); // same verifier again
        assert_eq!(b.valid_agreements(&posted), 1, "one verifier counts once");

        // a disagreeing verdict doesn't count toward the agree-threshold
        let v2 = NodeIdentity::generate();
        b.post_verdict(Verdict::sign(
            &v2,
            req.request_hash(),
            req.output_hash(),
            false,
        ));
        assert_eq!(b.valid_agreements(&posted), 1);
    }

    #[test]
    fn board_rejects_self_verification_and_invalid_verdicts_on_read() {
        let producer = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let posted = open_posted(producer.node_id().0, req.clone(), 1);
        let mut b = Board::new();
        b.post_request(posted.clone());

        // no self-verification: the producer's own verdict doesn't count
        b.post_verdict(agree_verdict(&producer, &req));
        assert_eq!(
            b.valid_agreements(&posted),
            0,
            "producer cannot verify itself"
        );

        // a tampered verdict (sig no longer matches its fields) is ignored on read
        let v = NodeIdentity::generate();
        let mut forged = agree_verdict(&v, &req);
        forged.output_hash = [0xEE; 32];
        b.post_verdict(forged);
        assert_eq!(b.valid_agreements(&posted), 0, "invalid verdict ignored");

        // an honest third-party verdict satisfies k=1
        let w = NodeIdentity::generate();
        b.post_verdict(agree_verdict(&w, &req));
        assert!(b.satisfied(&posted));
    }

    #[test]
    fn whitelist_policy_only_counts_listed_verifiers() {
        let producer = NodeIdentity::generate();
        let allowed = NodeIdentity::generate();
        let outsider = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let posted = PostedRequest {
            producer: producer.node_id().0,
            req: req.clone(),
            policy: VerifyPolicy {
                k: 1,
                set: VerifierSet::Whitelist(vec![allowed.node_id().0]),
            },
        };
        let mut b = Board::new();
        b.post_request(posted.clone());

        b.post_verdict(agree_verdict(&outsider, &req));
        assert_eq!(
            b.valid_agreements(&posted),
            0,
            "a non-whitelisted verifier doesn't count"
        );
        b.post_verdict(agree_verdict(&allowed, &req));
        assert!(b.satisfied(&posted), "a whitelisted verifier does");
    }

    #[test]
    fn grabbable_excludes_producer_already_verified_and_satisfied() {
        let producer = NodeIdentity::generate();
        let verifier = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let posted = open_posted(producer.node_id().0, req.clone(), 1);
        let mut b = Board::new();
        b.post_request(posted);

        assert!(
            b.grabbable_by(&producer.node_id().0).is_empty(),
            "no self-verification — producer can't grab its own request"
        );
        assert_eq!(
            b.grabbable_by(&verifier.node_id().0).len(),
            1,
            "a fresh eligible verifier can grab it"
        );

        b.post_verdict(agree_verdict(&verifier, &req));
        assert!(
            b.grabbable_by(&verifier.node_id().0).is_empty(),
            "not grabbable again by a verifier who already verified it"
        );
        let other = NodeIdentity::generate();
        assert!(
            b.grabbable_by(&other.node_id().0).is_empty(),
            "a satisfied request isn't grabbable"
        );
    }

    #[test]
    fn post_request_is_idempotent() {
        let producer = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let posted = open_posted(producer.node_id().0, req, 1);
        let mut b = Board::new();
        b.post_request(posted.clone());
        b.post_request(posted); // re-gossiped
        let some_node = NodeIdentity::generate().node_id().0;
        assert_eq!(
            b.grabbable_by(&some_node).len(),
            1,
            "a re-posted request collapses to one entry"
        );
    }

    // ---- P4: cooldown-rotated verifier selection + collection certificate ----

    #[test]
    fn verifier_respects_cooldown_and_readiness() {
        let producer = NodeIdentity::generate();
        let node = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let mut board = Board::new();
        board.post_request(open_posted(producer.node_id().0, req.clone(), 1));

        let mut v = Verifier::new(node.node_id().0, 1000);
        assert!(v.ready(0), "a fresh verifier is ready");
        assert!(
            v.select(&board, 0).is_some(),
            "and can grab a pending request"
        );

        v.mark_verified(500);
        assert!(!v.ready(1000), "on cooldown 500..1500");
        assert!(
            v.select(&board, 1000).is_none(),
            "no grab while cooling down"
        );
        assert!(v.ready(1500), "ready again at last+cooldown");
    }

    #[test]
    fn a_single_verifier_cannot_satisfy_a_k_of_3_policy() {
        // distinctness (dedup) + cooldown: one node can post at most one COUNTING verdict, so it can
        // never meet k=3 alone — verification needs k DISTINCT verifiers.
        let producer = NodeIdentity::generate();
        let node = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let posted = open_posted(producer.node_id().0, req.clone(), 3);
        let mut board = Board::new();
        board.post_request(posted.clone());

        let mut v = Verifier::new(node.node_id().0, 1000);
        let mut now = 0u64;
        for _ in 0..50 {
            if v.select(&board, now).is_some() {
                board.post_verdict(agree_verdict(&node, &req));
                v.mark_verified(now);
            }
            now += 1000;
        }
        assert!(
            !board.satisfied(&posted),
            "one distinct verifier can't meet k=3"
        );
        assert_eq!(
            board.valid_agreements(&posted),
            1,
            "its repeats dedup to one"
        );
        assert!(board.collected(&posted).is_none(), "no certificate");
    }

    #[test]
    fn cooldown_scheduler_converges_with_k_distinct_verifiers() {
        let producer = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let k = 3u32;
        let posted = open_posted(producer.node_id().0, req.clone(), k);
        let mut board = Board::new();
        board.post_request(posted.clone());

        let ids: Vec<NodeIdentity> = (0..5).map(|_| NodeIdentity::generate()).collect();
        let mut verifiers: Vec<Verifier> = ids
            .iter()
            .map(|id| Verifier::new(id.node_id().0, 1000))
            .collect();

        let mut now = 0u64;
        let mut participants: std::collections::HashSet<[u8; 32]> =
            std::collections::HashSet::new();
        for _ in 0..100 {
            if board.satisfied(&posted) {
                break;
            }
            for (i, v) in verifiers.iter_mut().enumerate() {
                // a node re-runs + agrees when it grabs (re-run correctness is tested in P1);
                // `.is_some()` drops the board borrow before we post.
                if v.select(&board, now).is_some() {
                    board.post_verdict(agree_verdict(&ids[i], &req));
                    v.mark_verified(now);
                    participants.insert(ids[i].node_id().0);
                }
            }
            now += 200;
        }
        assert!(
            board.satisfied(&posted),
            "the scheduler converges to k verdicts"
        );
        assert!(
            participants.len() >= k as usize,
            "k distinct nodes participated — cooldown/dedup forced diversity"
        );
        let cert = board
            .collected(&posted)
            .expect("a certificate once satisfied");
        assert!(cert.len() >= k as usize && cert.iter().all(|v| v.agree && v.verify_sig()));
    }

    #[test]
    fn collected_yields_the_certificate_only_when_satisfied() {
        let producer = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let posted = open_posted(producer.node_id().0, req.clone(), 2);
        let mut board = Board::new();
        board.post_request(posted.clone());

        board.post_verdict(agree_verdict(&NodeIdentity::generate(), &req));
        assert!(
            board.collected(&posted).is_none(),
            "1 of 2 — no certificate yet"
        );
        board.post_verdict(agree_verdict(&NodeIdentity::generate(), &req));
        let cert = board.collected(&posted).expect("2 of 2 → certificate");
        assert_eq!(cert.len(), 2);
    }

    // ---- P5a: first consumer (a shared counter) — end-to-end over the in-memory board ----

    // A consistency-critical shared counter: pure `f(state, input) = state + input` (1-byte). It
    // reads only explicit inputs (state + input) and never calls verify → verifiable. This is the
    // kind of state (a counter/quota/balance) where a producer wants k nodes to confirm the
    // transition before committing.
    const COUNTER_WAT: &[u8] = br#"(module
      (import "craftcom" "state"  (func $state  (param i32 i32) (result i32)))
      (import "craftcom" "input"  (func $input  (param i32 i32) (result i32)))
      (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "f")
        (drop (call $state (i32.const 0) (i32.const 1)))
        (drop (call $input (i32.const 1) (i32.const 1)))
        (i32.store8 (i32.const 2)
          (i32.add (i32.load8_u (i32.const 0)) (i32.load8_u (i32.const 1))))
        (drop (call $commit (i32.const 2) (i32.const 1)))))"#;

    #[tokio::test]
    async fn shared_counter_verifies_k_of_3_end_to_end() {
        let rt = TransitionRuntime::new().unwrap();
        let producer = NodeIdentity::generate();
        let (prev, inc, now) = (&[5u8][..], &[3u8][..], 42u64);

        // Producer computes the canonical output + packages the claim.
        let req = produce(&rt, COUNTER_WAT, "f", prev, inc, now, FUEL)
            .await
            .unwrap();
        assert_eq!(req.claimed_output, vec![8], "5 + 3 = 8");

        // Posts it with a k=3 open policy.
        let posted = PostedRequest {
            producer: producer.node_id().0,
            req: req.clone(),
            policy: VerifyPolicy {
                k: 3,
                set: VerifierSet::Open,
            },
        };
        let mut board = Board::new();
        board.post_request(posted.clone());

        // Cooldown-scheduled verifiers grab, RE-RUN the counter, and post real verdicts.
        let ids: Vec<NodeIdentity> = (0..5).map(|_| NodeIdentity::generate()).collect();
        let mut verifiers: Vec<Verifier> = ids
            .iter()
            .map(|id| Verifier::new(id.node_id().0, 1000))
            .collect();
        let mut t = 0u64;
        for _ in 0..50 {
            if board.satisfied(&posted) {
                break;
            }
            let acting: Vec<usize> = verifiers
                .iter()
                .enumerate()
                .filter(|(_, v)| v.select(&board, t).is_some())
                .map(|(i, _)| i)
                .collect();
            for i in acting {
                let verdict = verify_locally(&rt, &ids[i], &req, COUNTER_WAT, FUEL).await;
                assert!(verdict.agree, "an honest verifier reproduces 8");
                board.post_verdict(verdict);
                verifiers[i].mark_verified(t);
            }
            t += 200;
        }
        let cert = board
            .collected(&posted)
            .expect("k=3 independent confirmations");
        assert!(cert.len() >= 3 && cert.iter().all(|v| v.agree && v.verify_sig()));
    }

    #[tokio::test]
    async fn a_forged_counter_transition_never_collects_a_certificate() {
        let rt = TransitionRuntime::new().unwrap();
        let producer = NodeIdentity::generate();

        // Producer LIES: claims 5 + 3 = 9. The verifier re-runs f and gets 8 → disagree → no cert.
        let mut req = produce(&rt, COUNTER_WAT, "f", &[5u8], &[3u8], 0, FUEL)
            .await
            .unwrap();
        req.claimed_output = vec![9];
        let posted = PostedRequest {
            producer: producer.node_id().0,
            req: req.clone(),
            policy: VerifyPolicy {
                k: 1,
                set: VerifierSet::Open,
            },
        };
        let mut board = Board::new();
        board.post_request(posted.clone());
        // even many verifiers can't confirm a false claim
        for _ in 0..3 {
            board.post_verdict(
                verify_locally(&rt, &NodeIdentity::generate(), &req, COUNTER_WAT, FUEL).await,
            );
        }
        assert_eq!(
            board.valid_agreements(&posted),
            0,
            "no one reproduces the forged output"
        );
        assert!(
            board.collected(&posted).is_none(),
            "a forged transition never collects a certificate"
        );
    }

    // ---- P5b-1: the board as a wire-serializable CRDT (snapshot + merge) ----

    #[test]
    fn board_snapshot_round_trips_via_postcard() {
        let producer = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let posted = open_posted(producer.node_id().0, req.clone(), 1);
        let mut b = Board::new();
        b.post_request(posted.clone());
        b.post_verdict(agree_verdict(&NodeIdentity::generate(), &req));

        let bytes = postcard::to_allocvec(&b.snapshot()).unwrap();
        let snap: BoardSnapshot = postcard::from_bytes(&bytes).unwrap();
        let mut b2 = Board::new();
        b2.merge(snap);
        assert!(
            b2.satisfied(&posted),
            "a decoded snapshot reconstructs the board"
        );
    }

    #[test]
    fn merging_snapshots_is_an_idempotent_union() {
        let producer = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let posted = open_posted(producer.node_id().0, req.clone(), 2);

        // node A saw v1's verdict, node B saw v2's — each holds the request + one verdict
        let mut a = Board::new();
        a.post_request(posted.clone());
        a.post_verdict(agree_verdict(&NodeIdentity::generate(), &req));
        let mut b = Board::new();
        b.post_request(posted.clone());
        b.post_verdict(agree_verdict(&NodeIdentity::generate(), &req));

        assert!(
            !a.satisfied(&posted) && !b.satisfied(&posted),
            "1 of 2 each"
        );
        a.merge(b.snapshot());
        assert!(
            a.satisfied(&posted),
            "the union has both verdicts → k=2 met"
        );
        let before = a.valid_agreements(&posted);
        a.merge(b.snapshot());
        assert_eq!(
            a.valid_agreements(&posted),
            before,
            "merging again changes nothing (idempotent)"
        );
    }

    #[test]
    fn merge_is_commutative() {
        let producer = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let posted = open_posted(producer.node_id().0, req.clone(), 2);

        let mut a = Board::new();
        a.post_request(posted.clone());
        a.post_verdict(agree_verdict(&NodeIdentity::generate(), &req));
        let mut b = Board::new();
        b.post_request(posted.clone());
        b.post_verdict(agree_verdict(&NodeIdentity::generate(), &req));

        let mut ab = Board::new();
        ab.merge(a.snapshot());
        ab.merge(b.snapshot());
        let mut ba = Board::new();
        ba.merge(b.snapshot());
        ba.merge(a.snapshot());
        assert!(
            ab.satisfied(&posted) && ba.satisfied(&posted),
            "either merge order reaches k=2"
        );
    }

    #[test]
    fn a_gossiped_snapshot_cannot_fabricate_a_certificate() {
        let producer = NodeIdentity::generate();
        let req = req_for(DOUBLE_WAT, &[21], &[42]);
        let posted = open_posted(producer.node_id().0, req.clone(), 1);
        let mut b = Board::new();
        b.post_request(posted.clone());

        // a malicious peer gossips a forged (invalid-sig) verdict AND a producer self-verdict
        let mut forged = agree_verdict(&NodeIdentity::generate(), &req);
        forged.output_hash = [0xEE; 32]; // breaks the signature
        let snap = BoardSnapshot {
            requests: vec![],
            verdicts: vec![forged, agree_verdict(&producer, &req)],
        };
        b.merge(snap);
        assert_eq!(
            b.valid_agreements(&posted),
            0,
            "readers re-check — forged and self verdicts don't count"
        );
        assert!(b.collected(&posted).is_none());
    }
}
