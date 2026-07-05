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

/// Per-run context: the prior state + request bytes in, the committed output out.
struct AttestCtx {
    prev_state: Vec<u8>,
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
        self.run_transition(wasm, func, &[], request, fuel)
    }

    /// Run a state-transition program: the prior state is exposed via the `state` host
    /// function, the request via `input`. The output is what the program commits.
    pub fn run_transition(
        &self,
        wasm: &[u8],
        func: &str,
        prev_state: &[u8],
        request: &[u8],
        fuel: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let module = Module::new(&self.engine, wasm)?;
        let mut store = Store::new(
            &self.engine,
            AttestCtx {
                prev_state: prev_state.to_vec(),
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
    let att = attest_transition(identity, program_cid, prev_root, request, &output);
    Ok((att, output))
}

/// Sign an attestation over a deterministic transition computed by ANY means — used
/// for **native network-owned programs** (e.g. the registry, §4) whose code is the
/// node's own and thus already identical on every agent, so it needn't run through the
/// WASM sandbox. The quorum check is unchanged: k agents must still agree on the same
/// `output`, so a wrong output is outvoted exactly as with [`attest_run`]. The caller
/// guarantees the transition is deterministic.
pub fn attest_transition(
    identity: &NodeIdentity,
    program_cid: [u8; 32],
    prev_root: [u8; 32],
    request: &[u8],
    output: &[u8],
) -> Attestation {
    let request_hash = Cid::of(request).0;
    let output_root = Cid::of(output).0;
    let msg = signing_bytes(&program_cid, &prev_root, &request_hash, &output_root);
    Attestation {
        program_cid,
        prev_root,
        request_hash,
        output_root,
        agent: identity.node_id().0,
        signature: identity.sign(&msg).to_vec(),
    }
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

// ---- phase 3a: deterministic epoch-rotating committee ----

/// The attesting committee for an epoch: the deterministic sortition of the eligible
/// pool. Everyone derives the identical committee from `(eligible, epoch)`, so it is
/// verifiable without a stored whitelist (ATTESTATION_DESIGN §5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Committee {
    pub epoch: u64,
    /// Members in canonical (sorted) order.
    pub members: Vec<[u8; 32]>,
    /// Quorum threshold (clamped to the committee size).
    pub k: usize,
}

/// Deterministically select the epoch's committee: from the `eligible` pool (already
/// quality-gated upstream by reputation/uptime), take the `n` nodes with the smallest
/// `BLAKE3(epoch ‖ node)` — a per-epoch sortition. Deterministic (all nodes compute
/// the same set) and rotating (a different draw each epoch). `k` is clamped to size.
pub fn select_committee(eligible: &[[u8; 32]], epoch: u64, n: usize, k: usize) -> Committee {
    let mut scored: Vec<([u8; 32], [u8; 32])> = eligible
        .iter()
        .map(|node| {
            let mut buf = [0u8; 40];
            buf[..8].copy_from_slice(&epoch.to_be_bytes());
            buf[8..].copy_from_slice(node);
            (Cid::of(&buf).0, *node)
        })
        .collect();
    // Total order by (score, node) so the draw is unambiguous; take the n smallest.
    scored.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut members: Vec<[u8; 32]> = scored.into_iter().take(n).map(|(_, node)| node).collect();
    members.sort(); // canonical form for membership checks
    let k = k.min(members.len());
    Committee { epoch, members, k }
}

impl Committee {
    /// Is `node` on this committee?
    pub fn is_member(&self, node: &[u8; 32]) -> bool {
        self.members.binary_search(node).is_ok()
    }

    /// Verify an attested commit against THIS committee — its members are the agent
    /// set and `k` is the quorum. Returns the authorized advance iff a committee
    /// quorum agrees.
    pub fn verify_commit(&self, commit: &AttestedCommit) -> Option<PdaAdvance> {
        verify_commit(commit, &self.members, self.k)
    }
}

// ---- phase 3c: HLC-window epochs + the verifiable committee chain ----

/// Domain tag for committee-checkpoint endorsements.
const COMMITTEE_DOMAIN: &[u8] = b"craftec/committee/1";

/// The epoch number for an HLC-millis timestamp, given the epoch length. Deterministic
/// — every node maps a time to the same epoch, so they select the same committee.
pub fn epoch_of(hlc_millis: u64, epoch_millis: u64) -> u64 {
    if epoch_millis == 0 {
        return 0;
    }
    hlc_millis / epoch_millis
}

/// Canonical hash of a committee (epoch ‖ k ‖ sorted members) — the chain link.
pub fn committee_hash(c: &Committee) -> [u8; 32] {
    let mut b = Vec::with_capacity(16 + 32 * c.members.len());
    b.extend_from_slice(&c.epoch.to_be_bytes());
    b.extend_from_slice(&(c.k as u64).to_be_bytes());
    for m in &c.members {
        b.extend_from_slice(m);
    }
    Cid::of(&b).0
}

/// One outgoing-committee member's endorsement of the next committee.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Endorsement {
    pub agent: [u8; 32],
    pub signature: Vec<u8>,
}

/// A hand-off in the committee chain: the committee for its `committee.epoch`, linked
/// to the previous committee by `prev_hash`, endorsed by a quorum of the PRECEDING
/// committee. Readers follow the chain from a trusted genesis anchor; each committee
/// is only accepted if the one before it vouched for it (ATTESTATION_DESIGN §5).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitteeCheckpoint {
    pub committee: Committee,
    pub prev_hash: [u8; 32],
    pub endorsements: Vec<Endorsement>,
}

fn checkpoint_bytes(committee: &Committee, prev_hash: &[u8; 32]) -> Vec<u8> {
    let mut b = Vec::with_capacity(COMMITTEE_DOMAIN.len() + 64);
    b.extend_from_slice(COMMITTEE_DOMAIN);
    b.extend_from_slice(&committee_hash(committee));
    b.extend_from_slice(prev_hash);
    b
}

/// A member of the outgoing committee endorses the `next` committee (chained to
/// `prev_hash`). Collect ≥k of these from the outgoing committee to form a checkpoint.
pub fn endorse_checkpoint(
    identity: &NodeIdentity,
    next: &Committee,
    prev_hash: [u8; 32],
) -> Endorsement {
    let msg = checkpoint_bytes(next, &prev_hash);
    Endorsement {
        agent: identity.node_id().0,
        signature: identity.sign(&msg).to_vec(),
    }
}

/// Verify a committee chain from a trusted `genesis` committee (the bootstrap anchor).
/// Each checkpoint must link to the previous committee (`prev_hash`) and be endorsed
/// by ≥k members of that PRECEDING committee. Returns the current committee iff the
/// whole chain verifies — so who is legitimately in charge at any epoch is provable
/// from genesis, with no stored whitelist.
pub fn verify_chain(genesis: &Committee, checkpoints: &[CommitteeCheckpoint]) -> Option<Committee> {
    let mut current = genesis.clone();
    for cp in checkpoints {
        if cp.prev_hash != committee_hash(&current) {
            return None; // broken link
        }
        let msg = checkpoint_bytes(&cp.committee, &cp.prev_hash);
        let mut endorsers: HashSet<[u8; 32]> = HashSet::new();
        for e in &cp.endorsements {
            if !current.is_member(&e.agent) {
                continue;
            }
            let Ok(sig) = <[u8; 64]>::try_from(e.signature.as_slice()) else {
                continue;
            };
            if NodeIdentity::verify(&NodeId(e.agent), &msg, &sig) {
                endorsers.insert(e.agent);
            }
        }
        if endorsers.len() < current.k {
            return None; // the outgoing committee did not reach quorum on the hand-off
        }
        current = cp.committee.clone();
    }
    Some(current)
}

/// The committee chain as **durable, program-owned state** — the sequence of epoch
/// committees, each endorsed by the previous. Stored ONCE as content (erasure-coded)
/// and fetched on demand, NOT full-replicated to every node like a blockchain. That is
/// the design's advantage over a chain: content-addressed durable storage + read the
/// epoch you need, instead of every node holding the entire history.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitteeChain {
    pub genesis: Committee,
    pub checkpoints: Vec<CommitteeCheckpoint>,
}

impl CommitteeChain {
    pub fn new(genesis: Committee) -> Self {
        Self {
            genesis,
            checkpoints: Vec::new(),
        }
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes).ok()
    }
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).unwrap_or_default()
    }
    /// Content root of the whole chain (its cid when stored durably).
    pub fn root(&self) -> [u8; 32] {
        Cid::of(&self.encode()).0
    }

    /// Append the next epoch's checkpoint (endorsed by the current committee). Returns
    /// false if it doesn't validly extend the chain.
    pub fn append(&mut self, cp: CommitteeCheckpoint) -> bool {
        let Some(current) = self.current() else {
            return false;
        };
        if cp.prev_hash != committee_hash(&current) || cp.committee.epoch <= current.epoch {
            return false;
        }
        if endorsers_of(&cp, &current) < current.k {
            return false;
        }
        self.checkpoints.push(cp);
        true
    }

    /// The current (latest) committee — the whole chain verified from genesis.
    pub fn current(&self) -> Option<Committee> {
        verify_chain(&self.genesis, &self.checkpoints)
    }

    /// The committee governing `epoch` — the latest committee whose epoch ≤ `epoch`,
    /// verifying every checkpoint up to it. This is what a reader fetches to check an
    /// attestation made during that epoch.
    pub fn committee_at(&self, epoch: u64) -> Option<Committee> {
        if epoch < self.genesis.epoch {
            return None;
        }
        let mut current = self.genesis.clone();
        let mut prev_hash = committee_hash(&current);
        let mut best = Some(current.clone());
        for cp in &self.checkpoints {
            if cp.committee.epoch > epoch {
                break;
            }
            if cp.prev_hash != prev_hash || endorsers_of(cp, &current) < current.k {
                return None;
            }
            current = cp.committee.clone();
            prev_hash = committee_hash(&current);
            best = Some(current.clone());
        }
        best
    }
}

/// Count DISTINCT valid endorsements of `cp` by members of the `by` committee.
fn endorsers_of(cp: &CommitteeCheckpoint, by: &Committee) -> usize {
    let msg = checkpoint_bytes(&cp.committee, &cp.prev_hash);
    let mut set = HashSet::new();
    for e in &cp.endorsements {
        if !by.is_member(&e.agent) {
            continue;
        }
        if let Ok(sig) = <[u8; 64]>::try_from(e.signature.as_slice()) {
            if NodeIdentity::verify(&NodeId(e.agent), &msg, &sig) {
                set.insert(e.agent);
            }
        }
    }
    set.len()
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
    linker.func_wrap(
        ATTEST_MODULE,
        "state",
        |mut caller: Caller<'_, AttestCtx>, out: i32, cap: i32| -> i32 {
            let ps = caller.data().prev_state.clone();
            let Some(mem) = det_memory(&mut caller) else {
                return -1;
            };
            det_write(&mut caller, &mem, out, cap, &ps)
        },
    )?;
    // Deterministic ed25519 verification — the one crypto primitive an attested program
    // needs (e.g. the registry program checking an owner's signed submission). Reads a
    // 32-byte pubkey, `msg_len` message bytes, and a 64-byte signature from guest memory;
    // returns 1 if valid, else 0. Verification is deterministic, so it's safe here.
    linker.func_wrap(
        ATTEST_MODULE,
        "ed25519_verify",
        |mut caller: Caller<'_, AttestCtx>, pk: i32, msg: i32, msg_len: i32, sig: i32| -> i32 {
            let Some(mem) = det_memory(&mut caller) else {
                return 0;
            };
            let (Some(pk), Some(m), Some(s)) = (
                det_read(&caller, &mem, pk, 32),
                det_read(&caller, &mem, msg, msg_len),
                det_read(&caller, &mem, sig, 64),
            ) else {
                return 0;
            };
            let (Ok(pk), Ok(s)) = (
                <[u8; 32]>::try_from(pk.as_slice()),
                <[u8; 64]>::try_from(s.as_slice()),
            ) else {
                return 0;
            };
            i32::from(NodeIdentity::verify(&NodeId(pk), &m, &s))
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

    // Reads input = pubkey(32) | msg(4) | sig(64); commits [ed25519_verify result].
    const VERIFY_WAT: &[u8] = br#"(module
      (import "craftcom" "input"  (func $input  (param i32 i32) (result i32)))
      (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
      (import "craftcom" "ed25519_verify" (func $verify (param i32 i32 i32 i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "run")
        (drop (call $input (i32.const 0) (i32.const 200)))
        (i32.store8 (i32.const 300)
          (call $verify (i32.const 0) (i32.const 32) (i32.const 4) (i32.const 36)))
        (drop (call $commit (i32.const 300) (i32.const 1)))))"#;

    #[test]
    fn ed25519_verify_host_function() {
        let rt = AttestedRuntime::new().unwrap();
        let id = NodeIdentity::generate();
        let msg = [1u8, 2, 3, 4];
        let sig = id.sign(&msg);
        let mut input = Vec::new();
        input.extend_from_slice(&id.node_id().0);
        input.extend_from_slice(&msg);
        input.extend_from_slice(&sig);
        assert_eq!(
            rt.run(VERIFY_WAT, "run", &input, DEFAULT_FUEL).unwrap(),
            vec![1],
            "a valid signature verifies inside the sandbox"
        );
        let mut bad = input.clone();
        bad[36] ^= 0xFF; // corrupt the signature
        assert_eq!(
            rt.run(VERIFY_WAT, "run", &bad, DEFAULT_FUEL).unwrap(),
            vec![0],
            "a tampered signature is rejected"
        );
    }

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

    // ---- phase 3a: deterministic rotating committee ----

    #[test]
    fn committee_is_deterministic_and_order_independent() {
        let pool: Vec<[u8; 32]> = (0u8..12).map(|i| [i; 32]).collect();
        let c1 = select_committee(&pool, 7, 5, 3);
        // a shuffled pool must yield the identical committee
        let mut shuffled = pool.clone();
        shuffled.reverse();
        let c2 = select_committee(&shuffled, 7, 5, 3);
        assert_eq!(
            c1, c2,
            "committee is a deterministic function of (pool, epoch)"
        );
        assert_eq!(c1.members.len(), 5);
        assert_eq!(c1.k, 3);
    }

    #[test]
    fn committee_rotates_across_epochs() {
        let pool: Vec<[u8; 32]> = (0u8..40).map(|i| [i; 32]).collect();
        let a = select_committee(&pool, 1, 7, 4);
        let b = select_committee(&pool, 2, 7, 4);
        assert_ne!(a.members, b.members, "a new epoch redraws the committee");
    }

    #[test]
    fn k_and_n_clamp_to_a_small_pool() {
        let pool: Vec<[u8; 32]> = (0u8..3).map(|i| [i; 32]).collect();
        let c = select_committee(&pool, 1, 10, 8);
        assert_eq!(c.members.len(), 3, "n clamps to the pool size");
        assert_eq!(c.k, 3, "k clamps to the committee size");
    }

    #[test]
    fn commit_verifies_against_its_epoch_committee() {
        let rt = AttestedRuntime::new().unwrap();
        let program_cid = Cid::of(DOUBLE_WAT).0;
        let pool = agents(9);
        let eligible: Vec<[u8; 32]> = pool.iter().map(|a| a.node_id().0).collect();
        let committee = select_committee(&eligible, 42, 5, 3);
        // exactly the selected members attest.
        let signers: Vec<&NodeIdentity> = pool
            .iter()
            .filter(|id| committee.is_member(&id.node_id().0))
            .collect();
        assert_eq!(signers.len(), 5);
        let attestations: Vec<Attestation> = signers
            .iter()
            .map(|id| {
                attest_run(
                    id,
                    &rt,
                    DOUBLE_WAT,
                    "run",
                    program_cid,
                    [0u8; 32],
                    &[10u8],
                    DEFAULT_FUEL,
                )
                .unwrap()
                .0
            })
            .collect();
        let commit = AttestedCommit {
            program_cid,
            seed: b"registry".to_vec(),
            prev_root: [0u8; 32],
            request: vec![10u8],
            new_root: attestations[0].output_root,
            attestations,
        };
        let adv = committee
            .verify_commit(&commit)
            .expect("a committee quorum advances the account");
        assert_eq!(adv.new_root, Cid::of(&[10u8, 20]).0);
    }

    #[test]
    fn attestations_from_non_members_do_not_advance() {
        let rt = AttestedRuntime::new().unwrap();
        let program_cid = Cid::of(DOUBLE_WAT).0;
        let pool = agents(9);
        let eligible: Vec<[u8; 32]> = pool.iter().map(|a| a.node_id().0).collect();
        let committee = select_committee(&eligible, 42, 5, 3);
        // the NON-members attest instead — they are outside the committee.
        let outsiders: Vec<&NodeIdentity> = pool
            .iter()
            .filter(|id| !committee.is_member(&id.node_id().0))
            .collect();
        let attestations: Vec<Attestation> = outsiders
            .iter()
            .map(|id| {
                attest_run(
                    id,
                    &rt,
                    DOUBLE_WAT,
                    "run",
                    program_cid,
                    [0u8; 32],
                    &[10u8],
                    DEFAULT_FUEL,
                )
                .unwrap()
                .0
            })
            .collect();
        let commit = AttestedCommit {
            program_cid,
            seed: b"registry".to_vec(),
            prev_root: [0u8; 32],
            request: vec![10u8],
            new_root: attestations[0].output_root,
            attestations,
        };
        assert!(
            committee.verify_commit(&commit).is_none(),
            "non-committee attestations carry no authority"
        );
    }

    // ---- phase 3c: epochs + committee chain ----

    fn committee_of(ids: &[NodeIdentity], epoch: u64, k: usize) -> Committee {
        let mut members: Vec<[u8; 32]> = ids.iter().map(|i| i.node_id().0).collect();
        members.sort();
        Committee { epoch, members, k }
    }

    fn checkpoint(
        outgoing: &[NodeIdentity],
        next: &Committee,
        prev_hash: [u8; 32],
        endorsers: usize,
    ) -> CommitteeCheckpoint {
        let endorsements = outgoing
            .iter()
            .take(endorsers)
            .map(|id| endorse_checkpoint(id, next, prev_hash))
            .collect();
        CommitteeCheckpoint {
            committee: next.clone(),
            prev_hash,
            endorsements,
        }
    }

    #[test]
    fn epoch_advances_with_time() {
        assert_eq!(epoch_of(0, 1000), 0);
        assert_eq!(epoch_of(999, 1000), 0);
        assert_eq!(epoch_of(1000, 1000), 1);
        assert_eq!(epoch_of(2500, 1000), 2);
        assert_eq!(epoch_of(5, 0), 0, "zero-length epoch is guarded");
    }

    #[test]
    fn committee_chain_verifies_from_genesis() {
        let g = agents(3);
        let genesis = committee_of(&g, 0, 2);
        let c1_ids = agents(3);
        let c1 = committee_of(&c1_ids, 1, 2);
        let cp1 = checkpoint(&g, &c1, committee_hash(&genesis), 2);
        let c2 = committee_of(&agents(3), 2, 2);
        let cp2 = checkpoint(&c1_ids, &c2, committee_hash(&c1), 2);
        let current = verify_chain(&genesis, &[cp1, cp2]).expect("a well-formed chain verifies");
        assert_eq!(current, c2, "the chain resolves to the latest committee");
    }

    #[test]
    fn chain_rejects_sub_quorum_handoff() {
        let g = agents(3);
        let genesis = committee_of(&g, 0, 2);
        let c1 = committee_of(&agents(3), 1, 2);
        // only 1 endorsement, but genesis.k = 2
        let cp1 = checkpoint(&g, &c1, committee_hash(&genesis), 1);
        assert!(verify_chain(&genesis, &[cp1]).is_none());
    }

    #[test]
    fn chain_rejects_broken_link() {
        let g = agents(3);
        let genesis = committee_of(&g, 0, 2);
        let c1 = committee_of(&agents(3), 1, 2);
        // prev_hash points at nothing valid
        let cp1 = checkpoint(&g, &c1, [0xEE; 32], 2);
        assert!(verify_chain(&genesis, &[cp1]).is_none());
    }

    #[test]
    fn chain_rejects_endorsements_from_outside_the_prev_committee() {
        let g = agents(3);
        let genesis = committee_of(&g, 0, 2);
        let c1 = committee_of(&agents(3), 1, 2);
        // outsiders (not in genesis) endorse the hand-off
        let outsiders = agents(3);
        let cp1 = checkpoint(&outsiders, &c1, committee_hash(&genesis), 3);
        assert!(
            verify_chain(&genesis, &[cp1]).is_none(),
            "only the preceding committee can endorse the next"
        );
    }

    // ---- phase 4f: durable committee chain ----

    #[test]
    fn committee_chain_resolves_the_epoch_committee() {
        let g = agents(3);
        let genesis = committee_of(&g, 0, 2);
        let c1_ids = agents(3);
        let c1 = committee_of(&c1_ids, 1, 2);
        let c2 = committee_of(&agents(3), 2, 2);
        let mut chain = CommitteeChain::new(genesis.clone());
        assert!(chain.append(checkpoint(&g, &c1, committee_hash(&genesis), 2)));
        assert!(chain.append(checkpoint(&c1_ids, &c2, committee_hash(&c1), 2)));
        assert_eq!(chain.current(), Some(c2.clone()));
        assert_eq!(chain.committee_at(0), Some(genesis));
        assert_eq!(chain.committee_at(1), Some(c1));
        assert_eq!(chain.committee_at(2), Some(c2.clone()));
        assert_eq!(
            chain.committee_at(9),
            Some(c2),
            "the latest committee governs future epochs"
        );
    }

    #[test]
    fn committee_chain_encode_roundtrips() {
        let g = agents(3);
        let genesis = committee_of(&g, 0, 2);
        let c1 = committee_of(&agents(3), 1, 2);
        let mut chain = CommitteeChain::new(genesis.clone());
        chain.append(checkpoint(&g, &c1, committee_hash(&genesis), 2));
        assert_eq!(CommitteeChain::decode(&chain.encode()).unwrap(), chain);
    }

    #[test]
    fn committee_chain_rejects_a_sub_quorum_handoff() {
        let g = agents(3);
        let genesis = committee_of(&g, 0, 2);
        let c1 = committee_of(&agents(3), 1, 2);
        let mut chain = CommitteeChain::new(genesis.clone());
        // only 1 endorsement, but genesis.k = 2 → append rejected, chain unchanged.
        assert!(!chain.append(checkpoint(&g, &c1, committee_hash(&genesis), 1)));
        assert!(chain.checkpoints.is_empty());
        assert_eq!(chain.current(), Some(genesis));
    }
}
