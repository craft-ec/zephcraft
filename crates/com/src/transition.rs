//! The deterministic state-transition runtime — computes `(prev_state, request) →
//! new_state` (foundation §56).
//!
//! A DETERMINISTIC program runner under a restricted ABI: the program reads `input`,
//! optionally reads the prior `state`, and declares its `output` via `commit` — and
//! nothing else (no clock, no rng, no sql/obj side effects), so the output is a pure
//! function of `(program, prev_state, input)`. Every node that runs the same program on
//! the same input computes the identical `new_state` — no attestation, no committee; the
//! determinism itself is what makes the result reproducible.
//!
//! Its own sync engine (no async, no threads), fuel-metered, isolated from the
//! capability [`crate::Runtime`]. Reused as the execution core behind program accounts.
//! Also exposes [`pda`] — deriving a program-derived account identity — used by the
//! registry and generic-account paths.

use wasmtime::{Caller, Config, Engine, Extern, Linker, Memory, Module, Store};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;

/// Import module the restricted deterministic ABI is bound under (shares the `craftcom`
/// namespace with the capability runtime, but exposes only `input` + `commit` + `state`).
const TRANSITION_HOST_MODULE: &str = "craftcom";

/// A deterministic WASM runner. Restricted ABI: the program reads `input` and declares
/// its `output` via `commit` — and nothing else — so every honest node computes the
/// identical output. Its own sync engine (no async, no threads), fuel-metered `Engine`,
/// isolated from the capability [`crate::Runtime`].
pub struct TransitionRuntime {
    engine: Engine,
}

/// Per-run context: the prior state + request bytes in, the committed output out.
struct TransitionCtx {
    prev_state: Vec<u8>,
    input: Vec<u8>,
    output: Vec<u8>,
}

impl TransitionRuntime {
    pub fn new() -> anyhow::Result<Self> {
        let mut cfg = Config::new();
        cfg.consume_fuel(true); // deterministic bound; sync (no async_support)
        Ok(Self {
            engine: Engine::new(&cfg)?,
        })
    }

    /// Run `func` deterministically on `request`, returning the committed output.
    /// Fuel-metered; a runaway program traps (`Err`). Pure: identical `request` →
    /// identical output on every node.
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
            TransitionCtx {
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

impl Default for TransitionRuntime {
    fn default() -> Self {
        Self::new().expect("transition runtime")
    }
}

/// Domain tag for deriving a program-derived account identity.
const PDA_DOMAIN: &[u8] = b"craftec/pda/v1";

/// Derive a **program-derived account (PDA)** identity from a program (+ seed). No
/// private key exists for it; its state is authorized by the program's own deterministic
/// logic (the transition validates each request). Deterministic + collision-resistant, so
/// anyone can compute the account for a program and no one can sign for it directly.
pub fn pda(program_cid: &[u8; 32], seed: &[u8]) -> NodeId {
    let mut b = Vec::with_capacity(PDA_DOMAIN.len() + 32 + seed.len());
    b.extend_from_slice(PDA_DOMAIN);
    b.extend_from_slice(program_cid);
    b.extend_from_slice(seed);
    NodeId(Cid::of(&b).0)
}

/// The agent's exported linear memory, if present.
fn det_memory(caller: &mut Caller<'_, TransitionCtx>) -> Option<Memory> {
    match caller.get_export("memory") {
        Some(Extern::Memory(m)) => Some(m),
        _ => None,
    }
}

fn det_read(
    caller: &Caller<'_, TransitionCtx>,
    mem: &Memory,
    ptr: i32,
    len: i32,
) -> Option<Vec<u8>> {
    if ptr < 0 || len < 0 {
        return None;
    }
    let data = mem.data(caller);
    let (s, e) = (ptr as usize, ptr as usize + len as usize);
    data.get(s..e).map(|b| b.to_vec())
}

fn det_write(
    caller: &mut Caller<'_, TransitionCtx>,
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
fn bind_deterministic(linker: &mut Linker<TransitionCtx>) -> anyhow::Result<()> {
    linker.func_wrap(
        TRANSITION_HOST_MODULE,
        "input",
        |mut caller: Caller<'_, TransitionCtx>, out: i32, cap: i32| -> i32 {
            let input = caller.data().input.clone();
            let Some(mem) = det_memory(&mut caller) else {
                return -1;
            };
            det_write(&mut caller, &mem, out, cap, &input)
        },
    )?;
    linker.func_wrap(
        TRANSITION_HOST_MODULE,
        "commit",
        |mut caller: Caller<'_, TransitionCtx>, ptr: i32, len: i32| -> i32 {
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
        TRANSITION_HOST_MODULE,
        "state",
        |mut caller: Caller<'_, TransitionCtx>, out: i32, cap: i32| -> i32 {
            let ps = caller.data().prev_state.clone();
            let Some(mem) = det_memory(&mut caller) else {
                return -1;
            };
            det_write(&mut caller, &mem, out, cap, &ps)
        },
    )?;
    // Deterministic ed25519 verification — the one crypto primitive a transition program
    // needs (e.g. the registry program checking an owner's signed submission). Reads a
    // 32-byte pubkey, `msg_len` message bytes, and a 64-byte signature from guest memory;
    // returns 1 if valid, else 0. Verification is deterministic, so it's safe here.
    linker.func_wrap(
        TRANSITION_HOST_MODULE,
        "ed25519_verify",
        |mut caller: Caller<'_, TransitionCtx>, pk: i32, msg: i32, msg_len: i32, sig: i32| -> i32 {
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
        let rt = TransitionRuntime::new().unwrap();
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

    #[test]
    fn deterministic_run_is_pure() {
        let rt = TransitionRuntime::new().unwrap();
        let a = rt.run(DOUBLE_WAT, "run", &[21], DEFAULT_FUEL).unwrap();
        let b = rt.run(DOUBLE_WAT, "run", &[21], DEFAULT_FUEL).unwrap();
        assert_eq!(a, vec![21, 42]);
        assert_eq!(a, b, "same input → same output");
    }

    #[test]
    fn runaway_program_traps_on_fuel() {
        let rt = TransitionRuntime::new().unwrap();
        let spin = br#"(module (func (export "run") (loop (br 0))))"#;
        assert!(rt.run(spin, "run", &[], 100_000).is_err());
    }
}
