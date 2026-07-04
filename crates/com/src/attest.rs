//! Attestation — the program-owned authority core (docs/ATTESTATION_DESIGN.md;
//! foundation §41 Attestation Flow + §56 Attestation Path).
//!
//! Phase 1: the attested EXECUTION core. An agent runs a DETERMINISTIC program on a
//! request under a restricted ABI (read `input`, declare `output` via `commit` —
//! nothing else: no clock, no rng, no sql/obj side effects), so the output is a pure
//! function of `(program, prev_root, request)`. The agent signs an [`Attestation`]
//! over the transition. A quorum of ≥k agents attesting the SAME output authorizes
//! it — k independent Ed25519 signatures, no DKG/MPC (foundation §41 internal mode).
//!
//! This is §56 made concrete: load WASM → run on a pinned snapshot → sign
//! `hash(program ‖ prev ‖ request ‖ output)` → collect k distinct valid signatures.
//! (§56 signs `hash(event_id ‖ decision ‖ snapshot_cid)`; our tuple is a superset:
//! `request_hash`=event, `output_root`=decision, `prev_root`=snapshot_cid.)
//!
//! Phases 2–3 (PDA accounts + the agent-set/broadcast wire) build on this; the
//! coordination wire (`ATTEST_BROADCAST`, foundation §1042) is deliberately later.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use wasmtime::{Caller, Config, Engine, Extern, Linker, Memory, Module, Store};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;

/// Import module the restricted attested ABI is bound under (shares the `craftcom`
/// namespace with the capability runtime, but exposes only `input` + `commit`).
const ATTEST_MODULE: &str = "craftcom";

/// Domain tag mixed into the signed bytes, so an attestation signature can never be
/// replayed as any other Craftec signature.
const ATTEST_DOMAIN: &[u8] = b"craftec/attest/1";

/// One agent's attestation of a deterministic state transition (foundation §41/§56).
/// Signed over `ATTEST_DOMAIN ‖ program_cid ‖ prev_root ‖ request_hash ‖ output_root`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attestation {
    /// CID of the WASM program the agent ran.
    pub program_cid: [u8; 32],
    /// Prior state root the transition builds on (the pinned snapshot; zero = genesis).
    pub prev_root: [u8; 32],
    /// Hash of the request the program was run on.
    pub request_hash: [u8; 32],
    /// Hash of the program's committed output (the "decision" in §56 terms).
    pub output_root: [u8; 32],
    /// The attesting agent's NodeId (public key).
    pub agent: [u8; 32],
    /// 64-byte Ed25519 signature, held as a `Vec` for postcard (serde arrays cap at 32).
    pub signature: Vec<u8>,
}

/// A deterministic WASM runner for attested execution. Restricted ABI: the program
/// reads `input` and declares its `output` via `commit` — and nothing else — so every
/// honest agent computes the identical output. Its own sync (no async, no threads),
/// fuel-metered `Engine`, isolated from the capability [`crate::Runtime`].
pub struct AttestedRuntime {
    engine: Engine,
}

/// Per-run context: the request bytes in, the committed output out.
struct AttestCtx {
    input: Vec<u8>,
    output: Vec<u8>,
}

impl AttestedRuntime {
    pub fn new() -> anyhow::Result<Self> {
        let mut cfg = Config::new();
        cfg.consume_fuel(true); // deterministic bound; sync (no async_support)
        Ok(Self {
            engine: Engine::new(&cfg)?,
        })
    }

    /// Run `func` deterministically on `request`, returning the committed output.
    /// Fuel-metered; a runaway program traps (`Err`). Pure: identical `request` →
    /// identical output on every agent.
    pub fn run(
        &self,
        wasm: &[u8],
        func: &str,
        request: &[u8],
        fuel: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let module = Module::new(&self.engine, wasm)?;
        let mut store = Store::new(
            &self.engine,
            AttestCtx {
                input: request.to_vec(),
                output: Vec::new(),
            },
        );
        store.set_fuel(fuel)?;
        let mut linker = Linker::new(&self.engine);
        bind_deterministic(&mut linker)?;
        let instance = linker.instantiate(&mut store, &module)?;
        let f = instance.get_typed_func::<(), ()>(&mut store, func)?;
        f.call(&mut store, ())?;
        Ok(store.into_data().output)
    }
}

impl Default for AttestedRuntime {
    fn default() -> Self {
        Self::new().expect("attested runtime")
    }
}

/// The bytes an agent signs to attest a transition.
fn signing_bytes(
    program_cid: &[u8; 32],
    prev_root: &[u8; 32],
    request_hash: &[u8; 32],
    output_root: &[u8; 32],
) -> Vec<u8> {
    let mut b = Vec::with_capacity(ATTEST_DOMAIN.len() + 128);
    b.extend_from_slice(ATTEST_DOMAIN);
    b.extend_from_slice(program_cid);
    b.extend_from_slice(prev_root);
    b.extend_from_slice(request_hash);
    b.extend_from_slice(output_root);
    b
}

/// Run the deterministic program on `request` and sign the resulting transition —
/// one agent's attestation (foundation §41 step: "loads agent, validates, signs with
/// its own Ed25519 key"). Returns the attestation and the raw output bytes.
#[allow(clippy::too_many_arguments)]
pub fn attest_run(
    identity: &NodeIdentity,
    rt: &AttestedRuntime,
    wasm: &[u8],
    func: &str,
    program_cid: [u8; 32],
    prev_root: [u8; 32],
    request: &[u8],
    fuel: u64,
) -> anyhow::Result<(Attestation, Vec<u8>)> {
    let output = rt.run(wasm, func, request, fuel)?;
    let request_hash = Cid::of(request).0;
    let output_root = Cid::of(&output).0;
    let msg = signing_bytes(&program_cid, &prev_root, &request_hash, &output_root);
    let signature = identity.sign(&msg).to_vec();
    Ok((
        Attestation {
            program_cid,
            prev_root,
            request_hash,
            output_root,
            agent: identity.node_id().0,
            signature,
        },
        output,
    ))
}

/// Verify one attestation's signature (does NOT check agent-set membership — that's
/// [`verify_quorum`]'s job).
pub fn verify(att: &Attestation) -> bool {
    let Ok(sig) = <[u8; 64]>::try_from(att.signature.as_slice()) else {
        return false;
    };
    let msg = signing_bytes(
        &att.program_cid,
        &att.prev_root,
        &att.request_hash,
        &att.output_root,
    );
    NodeIdentity::verify(&NodeId(att.agent), &msg, &sig)
}

/// Verify a k-of-n quorum: ≥`k` DISTINCT agents from `agent_set`, each a valid
/// attestation over the SAME expected transition `(program_cid, prev_root,
/// request_hash)`, agreeing on one `output_root`. Returns the agreed `output_root`
/// iff a quorum exists — a disagreeing or out-of-set or forged attestation is not
/// counted, and a liar that attests a different output merely splits its own vote.
pub fn verify_quorum(
    atts: &[Attestation],
    program_cid: &[u8; 32],
    prev_root: &[u8; 32],
    request_hash: &[u8; 32],
    agent_set: &[[u8; 32]],
    k: usize,
) -> Option<[u8; 32]> {
    let mut by_output: HashMap<[u8; 32], HashSet<[u8; 32]>> = HashMap::new();
    for a in atts {
        if &a.program_cid != program_cid
            || &a.prev_root != prev_root
            || &a.request_hash != request_hash
        {
            continue;
        }
        if !agent_set.contains(&a.agent) || !verify(a) {
            continue;
        }
        by_output.entry(a.output_root).or_default().insert(a.agent);
    }
    by_output
        .into_iter()
        .find(|(_, agents)| agents.len() >= k)
        .map(|(out, _)| out)
}

// ---- phase 2: program-derived accounts + attested head authority ----

/// Domain tag for deriving a program-derived account identity.
const PDA_DOMAIN: &[u8] = b"craftec/pda/v1";

/// Derive a **program-derived account (PDA)** identity from a program (+ seed). No
/// private key exists for it; its state is authorized ONLY by an attestation quorum
/// over `program_cid` (ATTESTATION_DESIGN §3). Deterministic + collision-resistant, so
/// anyone can compute the account for a program and no one can sign for it directly.
pub fn pda(program_cid: &[u8; 32], seed: &[u8]) -> NodeId {
    let mut b = Vec::with_capacity(PDA_DOMAIN.len() + 32 + seed.len());
    b.extend_from_slice(PDA_DOMAIN);
    b.extend_from_slice(program_cid);
    b.extend_from_slice(seed);
    NodeId(Cid::of(&b).0)
}

/// An authorized advance of a PDA account's head: the account's program, run on
/// `(prev_root, request)`, deterministically produced `new_root`, attested by a
/// quorum. This is the *attested authority type* for a `KIND_ROOT`/`KIND_APP` head
/// (foundation §62 A2) — it replaces the single owner signature for a PDA account.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttestedCommit {
    pub program_cid: [u8; 32],
    pub seed: Vec<u8>,
    pub prev_root: [u8; 32],
    pub request: Vec<u8>,
    pub new_root: [u8; 32],
    pub attestations: Vec<Attestation>,
}

impl AttestedCommit {
    /// The account this commit advances (unverified \u2014 call [`verify_commit`]).
    pub fn account(&self) -> NodeId {
        pda(&self.program_cid, &self.seed)
    }
}

/// A verified head advance: which account moves, and from/to which root.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PdaAdvance {
    pub account: NodeId,
    pub prev_root: [u8; 32],
    pub new_root: [u8; 32],
}

/// Verify that an [`AttestedCommit`] legitimately advances its PDA account's head.
/// Returns the account + transition IFF a k-of-n quorum of `agent_set` attested that
/// the account's program, run on `(prev_root, request)`, produced EXACTLY `new_root`.
/// Rejects a sub-quorum, forged/out-of-set attestations, or a `new_root` the quorum
/// never attested (so a coordinator can't staple a false result onto real signatures).
pub fn verify_commit(
    commit: &AttestedCommit,
    agent_set: &[[u8; 32]],
    k: usize,
) -> Option<PdaAdvance> {
    let request_hash = Cid::of(&commit.request).0;
    let agreed = verify_quorum(
        &commit.attestations,
        &commit.program_cid,
        &commit.prev_root,
        &request_hash,
        agent_set,
        k,
    )?;
    // The quorum must have attested EXACTLY the claimed new head.
    if agreed != commit.new_root {
        return None;
    }
    Some(PdaAdvance {
        account: pda(&commit.program_cid, &commit.seed),
        prev_root: commit.prev_root,
        new_root: commit.new_root,
    })
}

/// The agent's exported linear memory, if present.
fn det_memory(caller: &mut Caller<'_, AttestCtx>) -> Option<Memory> {
    match caller.get_export("memory") {
        Some(Extern::Memory(m)) => Some(m),
        _ => None,
    }
}

fn det_read(caller: &Caller<'_, AttestCtx>, mem: &Memory, ptr: i32, len: i32) -> Option<Vec<u8>> {
    if ptr < 0 || len < 0 {
        return None;
    }
    let data = mem.data(caller);
    let (s, e) = (ptr as usize, ptr as usize + len as usize);
    data.get(s..e).map(|b| b.to_vec())
}

fn det_write(
    caller: &mut Caller<'_, AttestCtx>,
    mem: &Memory,
    out: i32,
    cap: i32,
    data: &[u8],
) -> i32 {
    if out < 0 || cap < 0 || data.len() > cap as usize {
        return -1;
    }
    let m = mem.data_mut(caller);
    let (s, e) = (out as usize, out as usize + data.len());
    match m.get_mut(s..e) {
        Some(dst) => {
            dst.copy_from_slice(data);
            data.len() as i32
        }
        None => -1,
    }
}

/// Bind ONLY the deterministic ABI — `input` (read the request) and `commit` (declare
/// the output). No clock, no rng, no sql/obj: the run is a pure function of its input.
fn bind_deterministic(linker: &mut Linker<AttestCtx>) -> anyhow::Result<()> {
    linker.func_wrap(
        ATTEST_MODULE,
        "input",
        |mut caller: Caller<'_, AttestCtx>, out: i32, cap: i32| -> i32 {
            let input = caller.data().input.clone();
            let Some(mem) = det_memory(&mut caller) else {
                return -1;
            };
            det_write(&mut caller, &mem, out, cap, &input)
        },
    )?;
    linker.func_wrap(
        ATTEST_MODULE,
        "commit",
        |mut caller: Caller<'_, AttestCtx>, ptr: i32, len: i32| -> i32 {
            let Some(mem) = det_memory(&mut caller) else {
                return -1;
            };
            let Some(bytes) = det_read(&caller, &mem, ptr, len) else {
                return -1;
            };
            let n = bytes.len() as i32;
            caller.data_mut().output = bytes;
            n
        },
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DEFAULT_FUEL;

    // A deterministic program: read the first input byte `b`, commit `[b, b*2]`.
    const DOUBLE_WAT: &[u8] = br#"(module
      (import "craftcom" "input"  (func $input  (param i32 i32) (result i32)))
      (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "run")
        (drop (call $input (i32.const 0) (i32.const 64)))
        (i32.store8 (i32.const 100) (i32.load8_u (i32.const 0)))
        (i32.store8 (i32.const 101) (i32.mul (i32.load8_u (i32.const 0)) (i32.const 2)))
        (drop (call $commit (i32.const 100) (i32.const 2)))))"#;

    fn agents(n: usize) -> Vec<NodeIdentity> {
        (0..n).map(|_| NodeIdentity::generate()).collect()
    }
    fn set(ids: &[NodeIdentity]) -> Vec<[u8; 32]> {
        ids.iter().map(|a| a.node_id().0).collect()
    }

    #[test]
    fn deterministic_run_is_pure() {
        let rt = AttestedRuntime::new().unwrap();
        let a = rt.run(DOUBLE_WAT, "run", &[21], DEFAULT_FUEL).unwrap();
        let b = rt.run(DOUBLE_WAT, "run", &[21], DEFAULT_FUEL).unwrap();
        assert_eq!(a, vec![21, 42]);
        assert_eq!(a, b, "same input → same output");
    }

    #[test]
    fn runaway_program_traps_on_fuel() {
        let rt = AttestedRuntime::new().unwrap();
        let spin = br#"(module (func (export "run") (loop (br 0))))"#;
        assert!(rt.run(spin, "run", &[], 100_000).is_err());
    }

    #[test]
    fn quorum_of_agreeing_agents_attests_the_transition() {
        let rt = AttestedRuntime::new().unwrap();
        let program_cid = Cid::of(DOUBLE_WAT).0;
        let prev = [0u8; 32];
        let req = vec![10u8];
        let ids = agents(3);
        let n = set(&ids);
        let atts: Vec<Attestation> = ids
            .iter()
            .map(|id| {
                attest_run(
                    id,
                    &rt,
                    DOUBLE_WAT,
                    "run",
                    program_cid,
                    prev,
                    &req,
                    DEFAULT_FUEL,
                )
                .unwrap()
                .0
            })
            .collect();
        let req_hash = Cid::of(&req).0;
        // three agents independently reach the SAME output → k=2 quorum agrees.
        assert_eq!(
            verify_quorum(&atts, &program_cid, &prev, &req_hash, &n, 2),
            Some(Cid::of(&[10u8, 20]).0),
        );
    }

    #[test]
    fn disagreeing_agent_splits_its_own_vote() {
        let rt = AttestedRuntime::new().unwrap();
        let program_cid = Cid::of(DOUBLE_WAT).0;
        let prev = [0u8; 32];
        let req = vec![10u8];
        let req_hash = Cid::of(&req).0;
        let honest = agents(2);
        let liar = NodeIdentity::generate();
        let mut n = set(&honest);
        n.push(liar.node_id().0);

        let mut atts: Vec<Attestation> = honest
            .iter()
            .map(|id| {
                attest_run(
                    id,
                    &rt,
                    DOUBLE_WAT,
                    "run",
                    program_cid,
                    prev,
                    &req,
                    DEFAULT_FUEL,
                )
                .unwrap()
                .0
            })
            .collect();
        // liar validly signs a DIFFERENT (wrong) output for the same transition.
        let wrong = Cid::of(&[99u8, 99]).0;
        let msg = signing_bytes(&program_cid, &prev, &req_hash, &wrong);
        atts.push(Attestation {
            program_cid,
            prev_root: prev,
            request_hash: req_hash,
            output_root: wrong,
            agent: liar.node_id().0,
            signature: liar.sign(&msg).to_vec(),
        });

        // k=2: the honest output has 2 → wins; the liar's output has only 1.
        assert_eq!(
            verify_quorum(&atts, &program_cid, &prev, &req_hash, &n, 2),
            Some(Cid::of(&[10u8, 20]).0),
        );
        // k=3: no single output reaches 3 → the liar prevented a quorum, can't forge one.
        assert_eq!(
            verify_quorum(&atts, &program_cid, &prev, &req_hash, &n, 3),
            None
        );
    }

    #[test]
    fn k_minus_one_is_insufficient() {
        let rt = AttestedRuntime::new().unwrap();
        let program_cid = Cid::of(DOUBLE_WAT).0;
        let prev = [0u8; 32];
        let req = vec![7u8];
        let req_hash = Cid::of(&req).0;
        let id = NodeIdentity::generate();
        let n = vec![id.node_id().0];
        let att = attest_run(
            &id,
            &rt,
            DOUBLE_WAT,
            "run",
            program_cid,
            prev,
            &req,
            DEFAULT_FUEL,
        )
        .unwrap()
        .0;
        assert_eq!(
            verify_quorum(&[att], &program_cid, &prev, &req_hash, &n, 2),
            None,
            "one attestation cannot meet k=2",
        );
    }

    #[test]
    fn out_of_set_and_forged_attestations_rejected() {
        let rt = AttestedRuntime::new().unwrap();
        let program_cid = Cid::of(DOUBLE_WAT).0;
        let prev = [0u8; 32];
        let req = vec![5u8];
        let req_hash = Cid::of(&req).0;
        let insider = NodeIdentity::generate();
        let outsider = NodeIdentity::generate();
        let n = vec![insider.node_id().0]; // outsider deliberately NOT in the set
        let a_in = attest_run(
            &insider,
            &rt,
            DOUBLE_WAT,
            "run",
            program_cid,
            prev,
            &req,
            DEFAULT_FUEL,
        )
        .unwrap()
        .0;
        let a_out = attest_run(
            &outsider,
            &rt,
            DOUBLE_WAT,
            "run",
            program_cid,
            prev,
            &req,
            DEFAULT_FUEL,
        )
        .unwrap()
        .0;
        // both agree, but only the insider counts → k=2 is not met.
        assert_eq!(
            verify_quorum(
                &[a_in.clone(), a_out],
                &program_cid,
                &prev,
                &req_hash,
                &n,
                2
            ),
            None,
        );
        // a tampered signature fails verification outright.
        let mut bad = a_in;
        bad.signature[0] ^= 0xFF;
        assert!(!verify(&bad));
    }

    // ---- phase 2: PDA accounts + attested head authority ----

    fn commit_for(
        rt: &AttestedRuntime,
        program_cid: [u8; 32],
        seed: &[u8],
        prev: [u8; 32],
        request: &[u8],
        signers: &[NodeIdentity],
    ) -> AttestedCommit {
        let attestations: Vec<Attestation> = signers
            .iter()
            .map(|id| {
                attest_run(
                    id,
                    rt,
                    DOUBLE_WAT,
                    "run",
                    program_cid,
                    prev,
                    request,
                    DEFAULT_FUEL,
                )
                .unwrap()
                .0
            })
            .collect();
        let new_root = attestations[0].output_root;
        AttestedCommit {
            program_cid,
            seed: seed.to_vec(),
            prev_root: prev,
            request: request.to_vec(),
            new_root,
            attestations,
        }
    }

    #[test]
    fn pda_is_deterministic_program_and_seed_bound() {
        let p1 = Cid::of(b"prog-a").0;
        let p2 = Cid::of(b"prog-b").0;
        assert_eq!(pda(&p1, b"s"), pda(&p1, b"s"), "stable");
        assert_ne!(pda(&p1, b"s"), pda(&p2, b"s"), "program-bound");
        assert_ne!(pda(&p1, b"s"), pda(&p1, b"t"), "seed-bound");
    }

    #[test]
    fn attested_commit_advances_the_pda_account() {
        let rt = AttestedRuntime::new().unwrap();
        let program_cid = Cid::of(DOUBLE_WAT).0;
        let ids = agents(3);
        let n = set(&ids);
        let commit = commit_for(&rt, program_cid, b"registry", [0u8; 32], &[10u8], &ids);
        let adv = verify_commit(&commit, &n, 2).expect("a k=2 quorum advances the account");
        assert_eq!(adv.account, pda(&program_cid, b"registry"));
        assert_eq!(adv.prev_root, [0u8; 32]);
        assert_eq!(adv.new_root, Cid::of(&[10u8, 20]).0);
        assert_eq!(adv.account, commit.account());
    }

    #[test]
    fn sub_quorum_commit_does_not_advance() {
        let rt = AttestedRuntime::new().unwrap();
        let program_cid = Cid::of(DOUBLE_WAT).0;
        let ids = agents(3);
        let n = set(&ids);
        let commit = commit_for(&rt, program_cid, b"registry", [0u8; 32], &[10u8], &ids);
        assert!(
            verify_commit(&commit, &n, 4).is_none(),
            "3 attestations cannot meet k=4"
        );
    }

    #[test]
    fn stapled_false_new_root_is_rejected() {
        // A coordinator keeps real signatures but claims a new_root the quorum never
        // attested — must be rejected (the quorum agreed on a different output).
        let rt = AttestedRuntime::new().unwrap();
        let program_cid = Cid::of(DOUBLE_WAT).0;
        let ids = agents(3);
        let n = set(&ids);
        let mut commit = commit_for(&rt, program_cid, b"registry", [0u8; 32], &[10u8], &ids);
        commit.new_root = Cid::of(b"a-lie").0;
        assert!(verify_commit(&commit, &n, 2).is_none());
    }

    #[test]
    fn out_of_set_agents_cannot_advance() {
        let rt = AttestedRuntime::new().unwrap();
        let program_cid = Cid::of(DOUBLE_WAT).0;
        let signers = agents(3);
        let commit = commit_for(&rt, program_cid, b"registry", [0u8; 32], &[10u8], &signers);
        let strangers = set(&agents(3)); // a totally different set
        assert!(
            verify_commit(&commit, &strangers, 2).is_none(),
            "attestations from outside the agent set do not count"
        );
    }
}
