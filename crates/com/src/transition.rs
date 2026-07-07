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
//! A fuel-metered engine that is the node's ONE WASM runtime (there is no separate
//! capability runtime — that type was removed; capabilities are now a per-program grant
//! over this same runtime). Reused as the execution core behind program accounts.
//! Also exposes [`pda`] — deriving a program-derived account identity — used by the
//! registry and generic-account paths.

use std::sync::Arc;

use wasmtime::{Caller, Config, Engine, Extern, Linker, Memory, Module, Store};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;

use crate::{AppBackend, Capability, CapabilityGrant};

/// Import module the restricted deterministic ABI is bound under (shares the `craftcom`
/// namespace with the capability runtime, but exposes only `input` + `commit` + `state`).
const TRANSITION_HOST_MODULE: &str = "craftcom";

/// A deterministic WASM runner. Restricted ABI: the program reads `input` and declares
/// its `output` via `commit` — and nothing else — so every honest node computes the
/// identical output. Async, fuel-metered `Engine` (async so this unified runtime can await
/// sql/obj I/O in later phases). It is the node's SINGLE WASM runtime; a program's surface
/// is decided by its [`CapabilityGrant`], not by a separate runtime type.
pub struct TransitionRuntime {
    engine: Engine,
}

/// Per-run context for the unified runtime: the prior state + request bytes in, the
/// committed output out, plus the `caller`/`app_ns`/`backend` the granted host functions
/// read. Under the deterministic grant only `prev_state`/`input`/`output` are touched
/// (`backend` is `None`); under a `full` grant the sql/obj/clock/caller functions read the
/// remaining fields. Build one with [`TransitionCtx::deterministic`] (backend-less) or
/// [`TransitionCtx::new`] (backend-backed).
pub struct TransitionCtx {
    prev_state: Vec<u8>,
    input: Vec<u8>,
    output: Vec<u8>,
    /// The invoking identity (exposed via the `caller` host fn under `Capability::Caller`).
    caller: [u8; 32],
    /// The app namespace sql/obj writes are confined to (the structural gate).
    app_ns: String,
    /// The CONSENSUS timestamp (millis) the `clock` host fn returns — the writer's HLC value,
    /// agreed by the single-writer/replica substrate (§6). Deterministic/reproducible: every
    /// node runs the transition with the SAME `now`, so `clock` reads identically everywhere.
    /// Distinct from real per-node wall-time, which is the app-only `wall_clock` (reads
    /// `backend.now_millis()`).
    pub now: u64,
    /// The substrate sql/obj/wall_clock act on. `None` under the deterministic grant (protocol
    /// programs don't import those functions); a called sql/obj/wall_clock fn with `None`
    /// returns its failure path rather than panicking.
    backend: Option<Arc<dyn AppBackend>>,
}

impl TransitionCtx {
    /// A backend-backed context — for apps/tests supplying a real (or mock) [`AppBackend`],
    /// the invoking `caller`, the `app_ns` sql/obj writes are confined to, and the consensus
    /// `now` (the timestamp the `clock` host fn returns).
    pub fn new(
        prev_state: Vec<u8>,
        input: Vec<u8>,
        caller: [u8; 32],
        app_ns: String,
        now: u64,
        backend: Option<Arc<dyn AppBackend>>,
    ) -> Self {
        Self {
            prev_state,
            input,
            output: Vec::new(),
            caller,
            app_ns,
            now,
            backend,
        }
    }

    /// The deterministic context: prior state + request + the consensus `now`, no backend,
    /// default caller/app_ns. This is what [`TransitionRuntime::run_transition`] builds, so
    /// protocol programs run with a reproducible clock and no host-varying surface.
    pub fn deterministic(prev_state: Vec<u8>, input: Vec<u8>, now: u64) -> Self {
        Self::new(prev_state, input, [0u8; 32], String::new(), now, None)
    }
}

impl TransitionRuntime {
    pub fn new() -> anyhow::Result<Self> {
        let mut cfg = Config::new();
        cfg.consume_fuel(true); // deterministic bound
        cfg.async_support(true); // await sql/obj I/O in later phases
        Ok(Self {
            engine: Engine::new(&cfg)?,
        })
    }

    /// Run `func` deterministically on `request`, returning the committed output.
    /// Fuel-metered; a runaway program traps (`Err`). Pure: identical `request` →
    /// identical output on every node.
    pub async fn run(
        &self,
        wasm: &[u8],
        func: &str,
        request: &[u8],
        fuel: u64,
    ) -> anyhow::Result<Vec<u8>> {
        self.run_transition(
            wasm,
            func,
            &[],
            request,
            fuel,
            &CapabilityGrant::deterministic(),
        )
        .await
    }

    /// Run a state-transition program deterministically: the prior state is exposed via the
    /// `state` host function, the request via `input`. The output is what the program
    /// commits. Convenience over [`run_program`](Self::run_program) that builds a
    /// backend-less [`TransitionCtx::deterministic`] — protocol programs (registry,
    /// governance, account paths) run through here, unchanged.
    pub async fn run_transition(
        &self,
        wasm: &[u8],
        func: &str,
        prev_state: &[u8],
        request: &[u8],
        fuel: u64,
        grant: &CapabilityGrant,
    ) -> anyhow::Result<Vec<u8>> {
        // `now = 0` here: the convenience path has no substrate clock. The account-store
        // substrate threads the node's real consensus HLC by building its own ctx (setting
        // `now = clock.now().millis()`) and calling `run_program` directly.
        let ctx = TransitionCtx::deterministic(prev_state.to_vec(), request.to_vec(), 0);
        self.run_program(wasm, func, ctx, fuel, grant).await
    }

    /// The unified run path: instantiate `wasm` under `grant` (binding ONLY the granted host
    /// functions — a non-granted import fails to link) against a full [`TransitionCtx`], run
    /// `func`, and return the committed output. A `ctx.backend` of `Some` lets the granted
    /// sql/obj/clock functions reach the substrate; `None` leaves them to return their
    /// failure path. Fuel-metered.
    pub async fn run_program(
        &self,
        wasm: &[u8],
        func: &str,
        ctx: TransitionCtx,
        fuel: u64,
        grant: &CapabilityGrant,
    ) -> anyhow::Result<Vec<u8>> {
        let module = Module::new(&self.engine, wasm)?;
        let mut store = Store::new(&self.engine, ctx);
        store.set_fuel(fuel)?;
        let mut linker = Linker::new(&self.engine);
        bind_granted(&mut linker, grant)?;
        let instance = linker.instantiate_async(&mut store, &module).await?;
        let f = instance.get_typed_func::<(), ()>(&mut store, func)?;
        f.call_async(&mut store, ()).await?;
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

fn det_read_str(
    caller: &Caller<'_, TransitionCtx>,
    mem: &Memory,
    ptr: i32,
    len: i32,
) -> Option<String> {
    det_read(caller, mem, ptr, len).and_then(|b| String::from_utf8(b).ok())
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

/// Bind the host ABI per the capability `grant` — a function is bound **only if** its
/// capability is granted, so a program importing a non-granted capability fails to
/// instantiate (`unknown import`; `COMPUTE_EXECUTION_DESIGN.md` §5). This is the unified
/// surface: `input` (Input), `state` (State), `commit` (Commit), `ed25519_verify` (Crypto),
/// `caller` (Caller), `sql_execute`/`sql_query` (Sql), `obj_put`/`obj_get` (Obj), `clock`
/// (Clock — the CONSENSUS timestamp `ctx.now`, reproducible → IN the deterministic profile),
/// and `wall_clock` (WallClock — real per-node wall-time, host-varying → app profile only).
/// These wasm-facing host-fn names are the full [`Capability`] surface bound over this one
/// runtime (there is no separate capability runtime type). The deterministic
/// grant binds the ✅ subset (backend-less, `clock` included); a `full` grant additionally
/// binds `wall_clock`. The sql/obj functions read `ctx.backend` and return their failure path
/// (never panic) when it is `None`; `clock` reads `ctx.now` and needs no backend.
fn bind_granted(linker: &mut Linker<TransitionCtx>, grant: &CapabilityGrant) -> anyhow::Result<()> {
    if grant.allows(Capability::Input) {
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "input",
            |mut caller: Caller<'_, TransitionCtx>, (out, cap): (i32, i32)| {
                Box::new(async move {
                    let input = caller.data().input.clone();
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1i32;
                    };
                    det_write(&mut caller, &mem, out, cap, &input)
                })
            },
        )?;
    }
    if grant.allows(Capability::Commit) {
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "commit",
            |mut caller: Caller<'_, TransitionCtx>, (ptr, len): (i32, i32)| {
                Box::new(async move {
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1i32;
                    };
                    let Some(bytes) = det_read(&caller, &mem, ptr, len) else {
                        return -1;
                    };
                    let n = bytes.len() as i32;
                    caller.data_mut().output = bytes;
                    n
                })
            },
        )?;
    }
    if grant.allows(Capability::State) {
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "state",
            |mut caller: Caller<'_, TransitionCtx>, (out, cap): (i32, i32)| {
                Box::new(async move {
                    let ps = caller.data().prev_state.clone();
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1i32;
                    };
                    det_write(&mut caller, &mem, out, cap, &ps)
                })
            },
        )?;
    }
    if grant.allows(Capability::Crypto) {
        // Deterministic ed25519 verification — the one crypto primitive a transition
        // program needs (e.g. the registry program checking an owner's signed submission).
        // Reads a 32-byte pubkey, `msg_len` message bytes, and a 64-byte signature from
        // guest memory; returns 1 if valid, else 0. Verification is deterministic, so it's
        // safe here.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "ed25519_verify",
            |mut caller: Caller<'_, TransitionCtx>,
             (pk, msg, msg_len, sig): (i32, i32, i32, i32)| {
                Box::new(async move {
                    let Some(mem) = det_memory(&mut caller) else {
                        return 0i32;
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
                })
            },
        )?;
    }
    if grant.allows(Capability::Caller) {
        // `caller(out, cap) -> i32` — writes the 32-byte invoking NodeId. Reads `ctx.caller`
        // directly (no backend needed). Mirrors lib.rs `caller`.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "caller",
            |mut caller: Caller<'_, TransitionCtx>, (out, cap): (i32, i32)| {
                Box::new(async move {
                    let id = caller.data().caller;
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1i32;
                    };
                    det_write(&mut caller, &mem, out, cap, &id)
                })
            },
        )?;
    }
    if grant.allows(Capability::Sql) {
        // `sql_execute(ptr, len) -> i64` — write SQL to the app's OWN namespace (the ctx
        // `app_ns`, never an agent argument → structural gate). Mirrors lib.rs `sql_execute`.
        // Backend-None → -1 (the failure path), never a panic.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "sql_execute",
            |mut caller: Caller<'_, TransitionCtx>, (ptr, len): (i32, i32)| {
                Box::new(async move {
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1i64;
                    };
                    let Some(sql) = det_read_str(&caller, &mem, ptr, len) else {
                        return -1;
                    };
                    let ns = caller.data().app_ns.clone(); // gate: ctx namespace, not agent
                    let Some(backend) = caller.data().backend.clone() else {
                        return -1; // no substrate (protocol program) → failure path
                    };
                    match backend.sql_execute(&ns, &sql).await {
                        Ok(n) => n as i64,
                        Err(_) => -1,
                    }
                })
            },
        )?;
        // `sql_query(owner_ptr, owner_len, sql_ptr, sql_len, out, cap) -> i32` — read the app
        // namespace of `owner` (own if `owner_len==0`); SAME app_ns only. Mirrors lib.rs.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "sql_query",
            |mut caller: Caller<'_, TransitionCtx>,
             (owner_ptr, owner_len, sql_ptr, sql_len, out, cap): (
                i32,
                i32,
                i32,
                i32,
                i32,
                i32,
            )| {
                Box::new(async move {
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1i32;
                    };
                    let owner = if owner_len == 0 {
                        None
                    } else {
                        match det_read(&caller, &mem, owner_ptr, owner_len) {
                            Some(b) if b.len() == 32 => {
                                let mut a = [0u8; 32];
                                a.copy_from_slice(&b);
                                Some(a)
                            }
                            _ => return -1,
                        }
                    };
                    let Some(sql) = det_read_str(&caller, &mem, sql_ptr, sql_len) else {
                        return -1;
                    };
                    let ns = caller.data().app_ns.clone(); // gate: same app_ns only
                    let Some(backend) = caller.data().backend.clone() else {
                        return -1;
                    };
                    match backend.sql_query(owner, &ns, &sql).await {
                        Ok(res) => det_write(&mut caller, &mem, out, cap, res.as_bytes()),
                        Err(_) => -1,
                    }
                })
            },
        )?;
    }
    if grant.allows(Capability::Obj) {
        // `obj_put(ptr, len, out, cap) -> i32` — store bytes; writes the 32-byte CID.
        // Mirrors lib.rs `obj_put`. Backend-None → -1.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "obj_put",
            |mut caller: Caller<'_, TransitionCtx>, (ptr, len, out, cap): (i32, i32, i32, i32)| {
                Box::new(async move {
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1i32;
                    };
                    let Some(data) = det_read(&caller, &mem, ptr, len) else {
                        return -1;
                    };
                    let Some(backend) = caller.data().backend.clone() else {
                        return -1;
                    };
                    match backend.obj_put(&data).await {
                        Ok(cid) => det_write(&mut caller, &mem, out, cap, &cid),
                        Err(_) => -1,
                    }
                })
            },
        )?;
        // `obj_get(cid_ptr, out, cap) -> i32` — fetch by CID; content length written.
        // Mirrors lib.rs `obj_get`. Backend-None → -1.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "obj_get",
            |mut caller: Caller<'_, TransitionCtx>, (cid_ptr, out, cap): (i32, i32, i32)| {
                Box::new(async move {
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1i32;
                    };
                    let cid = match det_read(&caller, &mem, cid_ptr, 32) {
                        Some(b) if b.len() == 32 => {
                            let mut a = [0u8; 32];
                            a.copy_from_slice(&b);
                            a
                        }
                        _ => return -1,
                    };
                    let Some(backend) = caller.data().backend.clone() else {
                        return -1;
                    };
                    match backend.obj_get(cid).await {
                        Ok(data) => det_write(&mut caller, &mem, out, cap, &data),
                        Err(_) => -1,
                    }
                })
            },
        )?;
    }
    if grant.allows(Capability::Clock) {
        // `clock() -> i64` — the CONSENSUS timestamp (`ctx.now`): the writer's HLC value,
        // already agreed by the single-writer/replica substrate (§6). Reproducible — every
        // node runs the transition with the SAME `now`, so this reads identically everywhere,
        // which is why it belongs to the deterministic profile. NOT the machine wall-clock
        // (that is `wall_clock` below). Reads `ctx.now` directly; no backend needed.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "clock",
            |caller: Caller<'_, TransitionCtx>, (): ()| {
                Box::new(async move { caller.data().now as i64 })
            },
        )?;
    }
    if grant.allows(Capability::WallClock) {
        // `wall_clock() -> i64` — real per-node wall-time (`backend.now_millis()`, the HLC's
        // wall reading). Host-varying (each node's own clock), so it binds ONLY under the app
        // (full) profile, never the deterministic one. Backend-None → 0 (apps always have a
        // backend; a backend-less run simply reports 0). Distinct craftcom import from
        // `clock` above.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "wall_clock",
            |caller: Caller<'_, TransitionCtx>, (): ()| {
                Box::new(async move {
                    match caller.data().backend.as_ref() {
                        Some(backend) => backend.now_millis() as i64,
                        None => 0,
                    }
                })
            },
        )?;
    }
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

    #[tokio::test]
    async fn ed25519_verify_host_function() {
        let rt = TransitionRuntime::new().unwrap();
        let id = NodeIdentity::generate();
        let msg = [1u8, 2, 3, 4];
        let sig = id.sign(&msg);
        let mut input = Vec::new();
        input.extend_from_slice(&id.node_id().0);
        input.extend_from_slice(&msg);
        input.extend_from_slice(&sig);
        assert_eq!(
            rt.run(VERIFY_WAT, "run", &input, DEFAULT_FUEL)
                .await
                .unwrap(),
            vec![1],
            "a valid signature verifies inside the sandbox"
        );
        let mut bad = input.clone();
        bad[36] ^= 0xFF; // corrupt the signature
        assert_eq!(
            rt.run(VERIFY_WAT, "run", &bad, DEFAULT_FUEL).await.unwrap(),
            vec![0],
            "a tampered signature is rejected"
        );
    }

    #[tokio::test]
    async fn deterministic_run_is_pure() {
        let rt = TransitionRuntime::new().unwrap();
        let a = rt
            .run(DOUBLE_WAT, "run", &[21], DEFAULT_FUEL)
            .await
            .unwrap();
        let b = rt
            .run(DOUBLE_WAT, "run", &[21], DEFAULT_FUEL)
            .await
            .unwrap();
        assert_eq!(a, vec![21, 42]);
        assert_eq!(a, b, "same input → same output");
    }

    #[tokio::test]
    async fn runaway_program_traps_on_fuel() {
        let rt = TransitionRuntime::new().unwrap();
        let spin = br#"(module (func (export "run") (loop (br 0))))"#;
        assert!(rt.run(spin, "run", &[], 100_000).await.is_err());
    }

    // The DOUBLE_WAT program imports only `input` + `commit` — both in the deterministic
    // grant → it instantiates and runs, proving the grant binds today's surface.
    #[tokio::test]
    async fn deterministic_grant_binds_input_and_commit() {
        let rt = TransitionRuntime::new().unwrap();
        let out = rt
            .run_transition(
                DOUBLE_WAT,
                "run",
                &[],
                &[21],
                DEFAULT_FUEL,
                &CapabilityGrant::deterministic(),
            )
            .await
            .expect("input+commit are granted");
        assert_eq!(out, vec![21, 42]);
    }

    // THE GATE: with `commit` removed from the grant, the host fn is NOT bound, so the same
    // program can't resolve its `commit` import and fails to instantiate (link-time
    // gating). A non-granted import cannot be escaped.
    #[tokio::test]
    async fn a_non_granted_import_fails_to_instantiate() {
        let rt = TransitionRuntime::new().unwrap();
        let err = rt
            .run_transition(
                DOUBLE_WAT,
                "run",
                &[],
                &[21],
                DEFAULT_FUEL,
                &CapabilityGrant::deterministic().without(Capability::Commit),
            )
            .await
            .expect_err("commit is not bound → unknown import");
        let msg = format!("{err:#}").to_lowercase();
        assert!(
            msg.contains("commit") || msg.contains("import") || msg.contains("unknown"),
            "instantiation failed on the missing `commit` import: {msg}"
        );
    }

    // ---- phase 2b: grant-gated capability host functions ----

    // A minimal backend for the capability gate tests — records nothing, returns success.
    struct MockBackend;
    #[async_trait::async_trait]
    impl AppBackend for MockBackend {
        async fn sql_execute(&self, _ns: &str, _sql: &str) -> anyhow::Result<u64> {
            Ok(0)
        }
        async fn sql_query(
            &self,
            _o: Option<[u8; 32]>,
            _ns: &str,
            _sql: &str,
        ) -> anyhow::Result<String> {
            Ok(String::new())
        }
        async fn obj_put(&self, _d: &[u8]) -> anyhow::Result<[u8; 32]> {
            Ok([7u8; 32])
        }
        async fn obj_get(&self, _c: [u8; 32]) -> anyhow::Result<Vec<u8>> {
            Ok(Vec::new())
        }
        fn now_millis(&self) -> u64 {
            0
        }
    }

    // Imports `obj_put` (Obj capability) and stores 4 bytes.
    const OBJ_PUT_WAT: &[u8] = br#"(module
      (import "craftcom" "obj_put" (func $put (param i32 i32 i32 i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "run")
        (drop (call $put (i32.const 0) (i32.const 4) (i32.const 64) (i32.const 32)))))"#;

    // THE GATE (new capability): the same `obj_put`-importing program instantiates under a
    // grant containing Obj (with a backend in the ctx) and FAILS to instantiate under a
    // grant WITHOUT Obj — proving a newly-ported host fn is bound only when granted.
    #[tokio::test]
    async fn obj_capability_gates_obj_put() {
        let rt = TransitionRuntime::new().unwrap();
        let backend: Arc<dyn AppBackend> = Arc::new(MockBackend);

        // Obj granted (deterministic profile includes Obj) → binds → runs.
        let ctx = TransitionCtx::new(
            Vec::new(),
            Vec::new(),
            [0u8; 32],
            "feed".into(),
            0,
            Some(backend.clone()),
        );
        rt.run_program(
            OBJ_PUT_WAT,
            "run",
            ctx,
            DEFAULT_FUEL,
            &CapabilityGrant::deterministic(),
        )
        .await
        .expect("Obj granted → obj_put binds and the program runs");

        // Obj removed → `obj_put` is not bound → unresolved import → fails to instantiate.
        let ctx = TransitionCtx::new(
            Vec::new(),
            Vec::new(),
            [0u8; 32],
            "feed".into(),
            0,
            Some(backend),
        );
        let err = rt
            .run_program(
                OBJ_PUT_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::deterministic().without(Capability::Obj),
            )
            .await
            .expect_err("no Obj grant → obj_put unbound → unknown import");
        let msg = format!("{err:#}").to_lowercase();
        assert!(
            msg.contains("obj_put") || msg.contains("import") || msg.contains("unknown"),
            "instantiation failed on the missing `obj_put` import: {msg}"
        );
    }

    // Backend-None safety: obj_put is granted but the ctx has no backend (a protocol-style
    // run). The host fn must take its failure path (return -1), NOT panic. The program
    // returns nothing but completes cleanly.
    #[tokio::test]
    async fn obj_put_with_no_backend_does_not_panic() {
        let rt = TransitionRuntime::new().unwrap();
        let ctx = TransitionCtx::deterministic(Vec::new(), Vec::new(), 0);
        rt.run_program(
            OBJ_PUT_WAT,
            "run",
            ctx,
            DEFAULT_FUEL,
            &CapabilityGrant::deterministic(),
        )
        .await
        .expect("obj_put with backend=None returns its failure path, no panic");
    }

    // A minimal backend whose `now_millis` returns a fixed real-time reading, for the
    // wall_clock tests. sql/obj are no-ops.
    struct ClockBackend(u64);
    #[async_trait::async_trait]
    impl AppBackend for ClockBackend {
        async fn sql_execute(&self, _n: &str, _s: &str) -> anyhow::Result<u64> {
            Ok(0)
        }
        async fn sql_query(
            &self,
            _o: Option<[u8; 32]>,
            _n: &str,
            _s: &str,
        ) -> anyhow::Result<String> {
            Ok(String::new())
        }
        async fn obj_put(&self, _d: &[u8]) -> anyhow::Result<[u8; 32]> {
            Ok([0u8; 32])
        }
        async fn obj_get(&self, _c: [u8; 32]) -> anyhow::Result<Vec<u8>> {
            Ok(Vec::new())
        }
        fn now_millis(&self) -> u64 {
            self.0
        }
    }

    // ---- phase 4: the consensus clock ----

    // `clock` returns the CONSENSUS timestamp `ctx.now` (deterministic/reproducible) and is
    // in the deterministic profile. A program committing the clock value under a chosen
    // `ctx.now` must observe exactly that value — proving `clock` reads ctx.now, not any
    // machine time. It needs no backend.
    #[tokio::test]
    async fn clock_returns_ctx_now_under_deterministic() {
        // clock() -> i64; store all 8 little-endian bytes and commit them.
        const CLOCK_WAT: &[u8] = br#"(module
          (import "craftcom" "clock" (func $clock (result i64)))
          (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
          (memory (export "memory") 1)
          (func (export "run")
            (i64.store (i32.const 0) (call $clock))
            (drop (call $commit (i32.const 0) (i32.const 8)))))"#;

        let rt = TransitionRuntime::new().unwrap();
        // A chosen consensus timestamp; no backend at all (clock reads ctx.now directly).
        let ctx = TransitionCtx::deterministic(Vec::new(), Vec::new(), 12345);
        let out = rt
            .run_program(
                CLOCK_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::deterministic(),
            )
            .await
            .expect("deterministic profile grants the consensus clock");
        let got = u64::from_le_bytes(out.as_slice().try_into().expect("8-byte clock value"));
        assert_eq!(got, 12345, "clock returns ctx.now deterministically");
    }

    // `wall_clock` (real per-node time) is APP-ONLY: a program importing it instantiates under
    // `full` (grants WallClock, reads the backend's now_millis) but FAILS to instantiate under
    // the deterministic profile (WallClock ungranted → unbound import).
    #[tokio::test]
    async fn wall_clock_is_app_only() {
        // wall_clock() -> i64; store its low byte and commit it.
        const WALL_WAT: &[u8] = br#"(module
          (import "craftcom" "wall_clock" (func $wall (result i64)))
          (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
          (memory (export "memory") 1)
          (func (export "run")
            (i32.store8 (i32.const 0) (i32.wrap_i64 (call $wall)))
            (drop (call $commit (i32.const 0) (i32.const 1)))))"#;

        let rt = TransitionRuntime::new().unwrap();
        let backend: Arc<dyn AppBackend> = Arc::new(ClockBackend(42));

        // Under `full` (grants WallClock) → wall_clock binds, returns the backend's real time.
        let ctx = TransitionCtx::new(
            Vec::new(),
            Vec::new(),
            [0u8; 32],
            String::new(),
            0,
            Some(backend.clone()),
        );
        let out = rt
            .run_program(WALL_WAT, "run", ctx, DEFAULT_FUEL, &CapabilityGrant::full())
            .await
            .expect("full grants WallClock → wall_clock binds");
        assert_eq!(
            out,
            vec![42],
            "wall_clock returned the backend's now_millis"
        );

        // Under the deterministic grant (no WallClock) → wall_clock is unbound → fails to link.
        let ctx = TransitionCtx::new(
            Vec::new(),
            Vec::new(),
            [0u8; 32],
            String::new(),
            0,
            Some(backend),
        );
        assert!(
            rt.run_program(
                WALL_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::deterministic(),
            )
            .await
            .is_err(),
            "deterministic profile must NOT bind the real per-node wall_clock"
        );
    }
}
