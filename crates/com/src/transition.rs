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

use crate::{
    AppBackend, AttestBackend, Capability, CapabilityGrant, InvokeProgramBackend, SequenceBackend,
    SequencedWrite, VerifyBackend, VerifyRequest,
};

/// Import module the restricted deterministic ABI is bound under (shares the `craftcom`
/// namespace with the capability runtime, but exposes only `input` + `commit` + `state`).
const TRANSITION_HOST_MODULE: &str = "craftcom";

/// Max bytes a single `random` call fills — bounds the host-side allocation so a caller can't
/// request a huge buffer (a DoS). Ample for keys/nonces/seeds; an app needing more loops.
const MAX_RANDOM: i32 = 1 << 16;

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
    /// True when THIS run is itself a verifier re-run (under [`CapabilityGrant::verifier`]). The
    /// `verify` host fn is then bound INERT (a no-op), so a re-run can't recurse into nested
    /// verification (`VERIFICATION_DESIGN §9`). Set via [`Self::in_verify_mode`].
    verify_mode: bool,
    /// The content cid of the WASM being run — named in a [`VerifyRequest`] so verifiers fetch +
    /// re-run the same program. Set by the invoker via [`Self::with_program`]; default `[0;32]`.
    program_cid: [u8; 32],
    /// The program's authenticated OWNER, resolved by the invoking node from the (owner-signed)
    /// registry — the identity whose declared quorum `attest` consults. `None` when the program was
    /// invoked by raw cid or remotely (no authenticated owner) → `attest` reports UNAVAILABLE, so an
    /// owner is NEVER caller-supplied (which would let an invoker self-authorize). Set via
    /// [`Self::with_program_owner`].
    program_owner: Option<[u8; 32]>,
    /// The node service the `verify` host fn drives (post a request + await the certificate).
    /// `None` on the deterministic/protocol path and during a verifier re-run — so `verify` reports
    /// UNAVAILABLE there. Set by the invoker via [`Self::with_verify_backend`].
    verify_backend: Option<Arc<dyn VerifyBackend>>,
    /// The node service the `attest` host fn drives (solicit the quorum's sign-offs + await
    /// authorization). `None` → `attest` reports UNAVAILABLE. Set via [`Self::with_attest_backend`].
    attest_backend: Option<Arc<dyn AttestBackend>>,
    /// The node service the `sequence` host fn drives (submit a write to the account's quorum + await
    /// the commit). `None` → `sequence` reports UNAVAILABLE. Set via [`Self::with_sequence_backend`].
    sequence_backend: Option<Arc<dyn SequenceBackend>>,
    /// The node service the `invoke_program` host fn drives — cross-program invocation (CPI), a
    /// deterministic calculation. `None` → `invoke_program` reports UNAVAILABLE, which is also how
    /// recursion is bounded: a CALLEE's ctx carries no invoke backend, so it cannot nest a CPI. Set via
    /// [`Self::with_invoke_backend`].
    invoke_backend: Option<Arc<dyn InvokeProgramBackend>>,
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
            verify_mode: false,
            program_cid: [0u8; 32],
            program_owner: None,
            verify_backend: None,
            attest_backend: None,
            sequence_backend: None,
            invoke_backend: None,
        }
    }

    /// The deterministic context: prior state + request + the consensus `now`, no backend,
    /// default caller/app_ns. This is what [`TransitionRuntime::run_transition`] builds, so
    /// protocol programs run with a reproducible clock and no host-varying surface.
    pub fn deterministic(prev_state: Vec<u8>, input: Vec<u8>, now: u64) -> Self {
        Self::new(prev_state, input, [0u8; 32], String::new(), now, None)
    }

    /// Mark this context as a verifier re-run, so the `verify` host fn binds INERT (the recursion
    /// guard — see [`Self::verify_mode`]). Used by `verify_locally` when re-running under
    /// [`CapabilityGrant::verifier`].
    pub fn in_verify_mode(mut self) -> Self {
        self.verify_mode = true;
        self
    }

    /// Set the program's authenticated owner (the registry-resolved publisher). MUST come from the
    /// invoking node's own registry resolution, never from a caller-supplied field.
    pub fn with_program_owner(mut self, owner: Option<[u8; 32]>) -> Self {
        self.program_owner = owner;
        self
    }

    /// Name the program being run (its content cid), so a `verify` call can name it in the
    /// [`VerifyRequest`] verifiers fetch + re-run.
    pub fn with_program(mut self, program_cid: [u8; 32]) -> Self {
        self.program_cid = program_cid;
        self
    }

    /// Inject the [`VerifyBackend`] the `verify` host fn drives (post + await). Absent → `verify`
    /// reports UNAVAILABLE.
    pub fn with_verify_backend(mut self, backend: Option<Arc<dyn VerifyBackend>>) -> Self {
        self.verify_backend = backend;
        self
    }

    /// Inject the [`AttestBackend`] the `attest` host fn drives (solicit + await). Absent → `attest`
    /// reports UNAVAILABLE.
    pub fn with_attest_backend(mut self, backend: Option<Arc<dyn AttestBackend>>) -> Self {
        self.attest_backend = backend;
        self
    }

    /// Inject the [`SequenceBackend`] the `sequence` host fn drives (submit + await the commit).
    /// Absent → `sequence` reports UNAVAILABLE.
    pub fn with_sequence_backend(mut self, backend: Option<Arc<dyn SequenceBackend>>) -> Self {
        self.sequence_backend = backend;
        self
    }

    /// Inject the [`InvokeProgramBackend`] the `invoke_program` host fn drives (CPI — a deterministic
    /// cross-program read). Absent → `invoke_program` reports UNAVAILABLE (and a callee is given `None`,
    /// which bounds recursion to one level).
    pub fn with_invoke_backend(mut self, backend: Option<Arc<dyn InvokeProgramBackend>>) -> Self {
        self.invoke_backend = backend;
        self
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
        // `obj_get_range(cid_ptr, offset, len, out, cap) -> i32` — RANGE/partial read of a FILE by its
        // manifest cid: writes the bytes in `[offset, offset+len)`, fetching only the covering segments
        // (streaming/seek over large files). Negative offset/len or no backend → -1; else the byte
        // length written (or -1 if it exceeds `cap`). Never panics.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "obj_get_range",
            |mut caller: Caller<'_, TransitionCtx>,
             (cid_ptr, offset, len, out, cap): (i32, i64, i64, i32, i32)| {
                Box::new(async move {
                    if offset < 0 || len < 0 {
                        return -1i32;
                    }
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1;
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
                    match backend.obj_get_range(cid, offset as u64, len as u64).await {
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
    if grant.allows(Capability::Random) {
        // `random(out_ptr, len) -> i32` — fill `len` bytes at `out_ptr` with cryptographically
        // secure random from the node's OS CSPRNG. Non-reproducible, so it binds ONLY under the app
        // (full) profile — never the deterministic or verifier profiles — so a consensus/verified
        // program can't import it (its output must be a pure function of its inputs). Returns the
        // number of bytes written, or `-1` on a negative/oversized `len` or an out-of-bounds
        // destination. Never panics.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "random",
            |mut caller: Caller<'_, TransitionCtx>, (out, len): (i32, i32)| {
                Box::new(async move {
                    if !(0..=MAX_RANDOM).contains(&len) {
                        return -1i32;
                    }
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1;
                    };
                    let mut buf = vec![0u8; len as usize];
                    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut buf);
                    det_write(&mut caller, &mem, out, len, &buf)
                })
            },
        )?;
    }
    if grant.allows(Capability::Verify) {
        // `verify(func_ptr, func_len, in_ptr, in_len, claim_ptr, claim_len) -> i32` — a PRODUCER
        // program's orchestration call into the VERIFICATION primitive (consistency): "get k
        // independent nodes to confirm `f(inputs) = claimed_output`." Consistency, not authority
        // (that is attestation). Return codes:
        //   `2`  INERT — this run is itself a verifier re-run (`ctx.verify_mode`): verify is a
        //        no-op, the recursion guard that lets a single-module program re-run its pure `f`
        //        without triggering nested verification (`VERIFICATION_DESIGN §9`).
        //   `-1` UNAVAILABLE — no verifier backend wired yet (P2) or a malformed call. The board +
        //        cooldown-rotated collection to threshold `k` land in P3/P4; until then the
        //        producer path reports unavailable rather than pretending to verify.
        // (`1` verified / `0` rejected are reserved for the wired board.) Never panics.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "verify",
            |mut caller: Caller<'_, TransitionCtx>,
             (fp, fl, ip, il, cp, cl): (i32, i32, i32, i32, i32, i32)| {
                Box::new(async move {
                    // Recursion guard FIRST: a verifier's re-run must never trigger verification.
                    if caller.data().verify_mode {
                        return 2i32;
                    }
                    // Read the ABI args (function name + inputs + claimed output); a malformed call
                    // is a clean -1, never a trap.
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1i32;
                    };
                    let (Some(func), Some(request), Some(claimed_output)) = (
                        det_read_str(&caller, &mem, fp, fl),
                        det_read(&caller, &mem, ip, il),
                        det_read(&caller, &mem, cp, cl),
                    ) else {
                        return -1;
                    };
                    // Producer path: post the request to the board + await its certificate.
                    let Some(vb) = caller.data().verify_backend.clone() else {
                        return -1; // no verifier backend wired → UNAVAILABLE
                    };
                    let req = VerifyRequest {
                        program_cid: caller.data().program_cid,
                        func,
                        prev_state: caller.data().prev_state.clone(),
                        request,
                        now: caller.data().now,
                        claimed_output,
                    };
                    i32::from(vb.verify(req).await) // 1 = verified (k agreed), 0 = rejected/timeout
                })
            },
        )?;
    }
    if grant.allows(Capability::Attest) {
        // `attest(statement_ptr, statement_len) -> i32` — a program's orchestration call into the
        // ATTESTATION primitive (authority): "does my declared quorum authorize this statement?"
        // `2` INERT on a verifier re-run (`ctx.verify_mode`) — attestation is non-deterministic, so a
        // re-run must not re-trigger it; `-1` UNAVAILABLE (no backend / malformed); else
        // `1` authorized / `0` rejected from the quorum-solicitation backend. Never panics.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "attest",
            |mut caller: Caller<'_, TransitionCtx>, (sp, sl): (i32, i32)| {
                Box::new(async move {
                    if caller.data().verify_mode {
                        return 2i32;
                    }
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1i32;
                    };
                    let Some(statement) = det_read(&caller, &mem, sp, sl) else {
                        return -1;
                    };
                    let Some(ab) = caller.data().attest_backend.clone() else {
                        return -1; // no attest backend wired → UNAVAILABLE
                    };
                    let Some(owner) = caller.data().program_owner else {
                        return -1; // no authenticated program owner (raw-cid / remote) → UNAVAILABLE
                    };
                    let program_cid = caller.data().program_cid;
                    // The owner is the registry-authenticated program owner, so the quorum consulted
                    // is the OWNER's, never the caller's — an invoker cannot self-authorize.
                    i32::from(ab.attest(owner, program_cid, statement).await) // 1 authorized / 0 rejected
                })
            },
        )?;
    }
    if grant.allows(Capability::Sequence) {
        // `sequence(account_ptr, nonce, payload_ptr, payload_len, owner_sig_ptr) -> i32` — a program's
        // orchestration call into the ORDERING SEQUENCER (uniqueness): "commit this PRE-AUTHORED write
        // at (account, nonce), serialized through my quorum." The write is authored by the account
        // owner (the 64-byte `owner_sig` at `owner_sig_ptr`, over `(account, nonce, payload)`) — the
        // app passes the owner-signed write; the backend verifies that authenticity before ordering.
        // `2` INERT on a verifier re-run (`ctx.verify_mode`) — sequencing is non-deterministic, so a
        // re-run must not re-order; `-1` UNAVAILABLE (no backend / no authenticated owner / malformed /
        // negative nonce); else `1` committed / `0` rejected (not owner-authentic, nonce not next, or
        // the quorum did not authorize). Never panics.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "sequence",
            |mut caller: Caller<'_, TransitionCtx>,
             (ap, nonce, pp, pl, sp): (i32, i64, i32, i32, i32)| {
                Box::new(async move {
                    if caller.data().verify_mode {
                        return 2i32;
                    }
                    if nonce < 0 {
                        return -1i32; // a nonce is a u64 slot index; a negative arg is malformed
                    }
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1i32;
                    };
                    let (Some(account_bytes), Some(payload), Some(owner_sig)) = (
                        det_read(&caller, &mem, ap, 32),
                        det_read(&caller, &mem, pp, pl),
                        det_read(&caller, &mem, sp, 64), // the account owner's 64-byte authorization
                    ) else {
                        return -1;
                    };
                    let Ok(account) = <[u8; 32]>::try_from(account_bytes.as_slice()) else {
                        return -1;
                    };
                    let Some(sb) = caller.data().sequence_backend.clone() else {
                        return -1; // no sequence backend wired → UNAVAILABLE
                    };
                    let Some(owner) = caller.data().program_owner else {
                        return -1; // no authenticated program owner (raw-cid / remote) → UNAVAILABLE
                    };
                    let program_cid = caller.data().program_cid;
                    let write = SequencedWrite {
                        account,
                        nonce: nonce as u64,
                        payload,
                        owner_sig,
                    };
                    // `owner` is the registry-authenticated program owner, so the quorum that ORDERS the
                    // write is the OWNER's; the WRITE itself is authorized by the account's `owner_sig`.
                    i32::from(sb.sequence(owner, program_cid, write).await) // 1 committed / 0 rejected
                })
            },
        )?;
    }
    if grant.allows(Capability::Pre) {
        // `pre_grant(recipient_ptr, recipient_len, threshold, shares, out_ptr, out_cap) -> i32` — a
        // program's runtime-mediated PROXY RE-ENCRYPTION delegation (K3 sharing). The backend derives
        // THIS identity's PRE key and returns the *blind* re-encryption fragments delegating
        // decryption to `recipient_pk` (Umbral generate_kfrags, `threshold`-of-`shares`); the app
        // never sees the secret (`ENCRYPTION_DESIGN §13`). It uses the running identity's OWN key —
        // you can only ever delegate your own data, so there is no self-authorize risk and (unlike
        // `attest`) no registry-owner check is needed. Writes the serialized `Vec<ReKeyFrag>`
        // (postcard) to `(out, cap)` and returns its length. Return codes:
        //   `2`  INERT on a verifier re-run (`ctx.verify_mode`) — delegation is non-deterministic, so
        //        a re-run must never mint fragments; the recursion/repro guard that lets a
        //        single-module program (pure `f` + a `share()`) still link.
        //   `-1` UNAVAILABLE — no backend / backend has not wired sharing (`Ok(None)`) / a malformed
        //        call (bad recipient length, threshold∉[1,shares]) / the output buffer is too small.
        // The app then stores the fragments in its OWN grants table (via `sql_execute`) and
        // distributes them; the re-encryption transform itself is pure WASM (no host fn). Never panics.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "pre_grant",
            |mut caller: Caller<'_, TransitionCtx>,
             (rp, rl, threshold, shares, out, cap): (i32, i32, i32, i32, i32, i32)| {
                Box::new(async move {
                    // A verifier's re-run must never mint delegation fragments (non-deterministic).
                    if caller.data().verify_mode {
                        return 2i32;
                    }
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1i32;
                    };
                    // Recipient PRE public key: raw serialized bytes (a compressed curve point, not a
                    // 32-byte NodeId). The backend validates the encoding; here just bound the length so
                    // a bogus (rp, rl) is a clean -1, never an over-read. A compressed key is ~33 bytes.
                    let Some(recipient) = det_read(&caller, &mem, rp, rl) else {
                        return -1;
                    };
                    if recipient.is_empty() || recipient.len() > 64 {
                        return -1;
                    }
                    // Threshold must be a valid m-of-n (1 ≤ m ≤ n): a clean -1, never a trap.
                    if threshold < 1 || shares < 1 || threshold > shares {
                        return -1;
                    }
                    let Some(backend) = caller.data().backend.clone() else {
                        return -1; // no substrate → UNAVAILABLE
                    };
                    match backend
                        .pre_rekey(recipient, threshold as u32, shares as u32)
                        .await
                    {
                        Ok(Some(bytes)) => det_write(&mut caller, &mem, out, cap, &bytes),
                        _ => -1, // Ok(None) (unwired) or Err → UNAVAILABLE
                    }
                })
            },
        )?;
    }

    if grant.allows(Capability::InvokeProgram) {
        // `invoke_program(name_ptr, name_len, func_ptr, func_len, in_ptr, in_len, out_ptr, out_cap) -> i32`
        // — CROSS-PROGRAM INVOCATION (CPI): resolve `name` (a canonical anchor name) → run `func(input)`
        // under the deterministic subset in the callee's OWN reserved namespace (read-only) → write its
        // committed output to `out` (≤ `out_cap`). Returns bytes written, or `-1` (bad args / no backend /
        // callee rejected / out buffer too small). CPI is DETERMINISTIC (the callee is forced deterministic),
        // so — unlike verify/attest/sequence — it is NOT inert on a verifier re-run: it re-runs and
        // reproduces. No backend → `-1` (which also bounds recursion: a callee is given no invoke backend).
        // Never panics.
        linker.func_wrap_async(
            TRANSITION_HOST_MODULE,
            "invoke_program",
            |mut caller: Caller<'_, TransitionCtx>,
             (np, nl, fp, fl, ip, il, out, cap): (i32, i32, i32, i32, i32, i32, i32, i32)| {
                Box::new(async move {
                    let Some(mem) = det_memory(&mut caller) else {
                        return -1i32;
                    };
                    let (Some(name), Some(func), Some(input)) = (
                        det_read_str(&caller, &mem, np, nl),
                        det_read_str(&caller, &mem, fp, fl),
                        det_read(&caller, &mem, ip, il),
                    ) else {
                        return -1;
                    };
                    let Some(backend) = caller.data().invoke_backend.clone() else {
                        return -1; // no invoke backend wired (a callee has none → one level only)
                    };
                    match backend.invoke_program(&name, &func, input).await {
                        Some(output) => det_write(&mut caller, &mem, out, cap, &output),
                        None => -1, // resolution/exec failure or a rejected (empty) commit
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

    // Producer-path verify() test: a mock backend returns a fixed verdict, and a program that calls
    // verify() and commits the result byte proves the host fn maps it to 1 / 0 / -1.
    struct MockVerify(bool);
    #[async_trait::async_trait]
    impl VerifyBackend for MockVerify {
        async fn verify(&self, _req: VerifyRequest) -> bool {
            self.0
        }
    }

    const VERIFY_ORCH_WAT: &[u8] = br#"(module
      (import "craftcom" "verify" (func $verify (param i32 i32 i32 i32 i32 i32) (result i32)))
      (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "run")
        (i32.store8 (i32.const 0)
          (call $verify (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0)))
        (drop (call $commit (i32.const 0) (i32.const 1)))))"#;

    #[tokio::test]
    async fn verify_producer_path_returns_the_backend_verdict() {
        let rt = TransitionRuntime::new().unwrap();
        // verified → 1
        let ctx = TransitionCtx::deterministic(vec![], vec![], 0)
            .with_program([7u8; 32])
            .with_verify_backend(Some(Arc::new(MockVerify(true))));
        assert_eq!(
            rt.run_program(
                VERIFY_ORCH_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::full()
            )
            .await
            .unwrap(),
            vec![1],
            "backend says verified → 1"
        );
        // rejected → 0
        let ctx = TransitionCtx::deterministic(vec![], vec![], 0)
            .with_verify_backend(Some(Arc::new(MockVerify(false))));
        assert_eq!(
            rt.run_program(
                VERIFY_ORCH_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::full()
            )
            .await
            .unwrap(),
            vec![0],
            "backend says rejected → 0"
        );
        // no backend → -1 (stored as 0xFF)
        let ctx = TransitionCtx::deterministic(vec![], vec![], 0);
        assert_eq!(
            rt.run_program(
                VERIFY_ORCH_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::full()
            )
            .await
            .unwrap(),
            vec![0xFF],
            "no verify backend → UNAVAILABLE (-1)"
        );
    }

    // Producer-path attest() test — mirrors verify(): a mock quorum backend returns a fixed
    // authorization, and a program calling attest() + committing the result proves 1 / 0 / -1 / 2.
    struct MockAttest(bool);
    #[async_trait::async_trait]
    impl AttestBackend for MockAttest {
        async fn attest(
            &self,
            _owner: [u8; 32],
            _program_cid: [u8; 32],
            _statement: Vec<u8>,
        ) -> bool {
            self.0
        }
    }

    const ATTEST_ORCH_WAT: &[u8] = br#"(module
      (import "craftcom" "attest" (func $attest (param i32 i32) (result i32)))
      (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "run")
        (i32.store8 (i32.const 0) (call $attest (i32.const 0) (i32.const 0)))
        (drop (call $commit (i32.const 0) (i32.const 1)))))"#;

    #[tokio::test]
    async fn attest_producer_path_returns_the_backend_authorization() {
        let rt = TransitionRuntime::new().unwrap();
        // authorized → 1 (an authenticated program owner is present)
        let ctx = TransitionCtx::deterministic(vec![], vec![], 0)
            .with_program_owner(Some([9u8; 32]))
            .with_attest_backend(Some(Arc::new(MockAttest(true))));
        assert_eq!(
            rt.run_program(
                ATTEST_ORCH_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::full()
            )
            .await
            .unwrap(),
            vec![1],
            "quorum authorized → 1"
        );
        // rejected → 0
        let ctx = TransitionCtx::deterministic(vec![], vec![], 0)
            .with_program_owner(Some([9u8; 32]))
            .with_attest_backend(Some(Arc::new(MockAttest(false))));
        assert_eq!(
            rt.run_program(
                ATTEST_ORCH_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::full()
            )
            .await
            .unwrap(),
            vec![0],
            "quorum rejected → 0"
        );
        // no backend → -1 (0xFF)
        let ctx =
            TransitionCtx::deterministic(vec![], vec![], 0).with_program_owner(Some([9u8; 32]));
        assert_eq!(
            rt.run_program(
                ATTEST_ORCH_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::full()
            )
            .await
            .unwrap(),
            vec![0xFF],
            "no attest backend → UNAVAILABLE (-1)"
        );
        // backend present but NO authenticated owner → -1: an invoker can't self-authorize by
        // supplying an owner; without a registry-resolved owner, attest is UNAVAILABLE.
        let ctx = TransitionCtx::deterministic(vec![], vec![], 0)
            .with_attest_backend(Some(Arc::new(MockAttest(true))));
        assert_eq!(
            rt.run_program(
                ATTEST_ORCH_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::full()
            )
            .await
            .unwrap(),
            vec![0xFF],
            "backend but no authenticated owner → UNAVAILABLE (-1)"
        );
        // inert on a verifier re-run → 2 (attestation is non-deterministic; a re-run must not trigger it)
        let ctx = TransitionCtx::deterministic(vec![], vec![], 0)
            .in_verify_mode()
            .with_attest_backend(Some(Arc::new(MockAttest(true))));
        assert_eq!(
            rt.run_program(
                ATTEST_ORCH_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::verifier()
            )
            .await
            .unwrap(),
            vec![2],
            "attest is INERT on a re-run"
        );
    }

    // Producer-path sequence() test — mirrors attest(): a mock sequencer backend returns a fixed
    // commit outcome, and a program calling sequence() + committing the result proves 1 / 0 / -1 / 2.
    struct MockSequence(bool);
    #[async_trait::async_trait]
    impl SequenceBackend for MockSequence {
        async fn sequence(
            &self,
            _owner: [u8; 32],
            _program_cid: [u8; 32],
            _write: SequencedWrite,
        ) -> bool {
            self.0
        }
    }

    // account_ptr=0 (32 zero bytes), nonce=0, payload=(0,0), owner_sig_ptr=0 (64 zero bytes); store the
    // i32 result at offset 0 and commit that 1 byte. The mock ignores the write, so zeros are fine.
    const SEQUENCE_ORCH_WAT: &[u8] = br#"(module
      (import "craftcom" "sequence" (func $sequence (param i32 i64 i32 i32 i32) (result i32)))
      (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "run")
        (i32.store8 (i32.const 0) (call $sequence (i32.const 0) (i64.const 0) (i32.const 0) (i32.const 0) (i32.const 0)))
        (drop (call $commit (i32.const 0) (i32.const 1)))))"#;

    #[tokio::test]
    async fn sequence_producer_path_returns_the_commit_outcome() {
        let rt = TransitionRuntime::new().unwrap();
        // committed → 1 (an authenticated program owner is present)
        let ctx = TransitionCtx::deterministic(vec![], vec![], 0)
            .with_program_owner(Some([9u8; 32]))
            .with_sequence_backend(Some(Arc::new(MockSequence(true))));
        assert_eq!(
            rt.run_program(
                SEQUENCE_ORCH_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::full()
            )
            .await
            .unwrap(),
            vec![1],
            "quorum committed → 1"
        );
        // rejected → 0
        let ctx = TransitionCtx::deterministic(vec![], vec![], 0)
            .with_program_owner(Some([9u8; 32]))
            .with_sequence_backend(Some(Arc::new(MockSequence(false))));
        assert_eq!(
            rt.run_program(
                SEQUENCE_ORCH_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::full()
            )
            .await
            .unwrap(),
            vec![0],
            "quorum rejected → 0"
        );
        // no backend → -1 (0xFF)
        let ctx =
            TransitionCtx::deterministic(vec![], vec![], 0).with_program_owner(Some([9u8; 32]));
        assert_eq!(
            rt.run_program(
                SEQUENCE_ORCH_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::full()
            )
            .await
            .unwrap(),
            vec![0xFF],
            "no sequence backend → UNAVAILABLE (-1)"
        );
        // backend present but NO authenticated owner → -1: an invoker can't self-order.
        let ctx = TransitionCtx::deterministic(vec![], vec![], 0)
            .with_sequence_backend(Some(Arc::new(MockSequence(true))));
        assert_eq!(
            rt.run_program(
                SEQUENCE_ORCH_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::full()
            )
            .await
            .unwrap(),
            vec![0xFF],
            "backend but no authenticated owner → UNAVAILABLE (-1)"
        );
        // inert on a verifier re-run → 2 (sequencing is non-deterministic; a re-run must not re-order)
        let ctx = TransitionCtx::deterministic(vec![], vec![], 0)
            .in_verify_mode()
            .with_sequence_backend(Some(Arc::new(MockSequence(true))));
        assert_eq!(
            rt.run_program(
                SEQUENCE_ORCH_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::verifier()
            )
            .await
            .unwrap(),
            vec![2],
            "sequence is INERT on a re-run"
        );
    }

    // Producer-path pre_grant() test (K3 sharing): a mock backend holds the OWNER's PRE key and
    // returns the blind re-encryption fragments delegating to the recipient (a REAL cipher::grant) —
    // the runtime-mediated ReKeyGen the app never does itself. A program reads the recipient's pubkey
    // from `input`, calls pre_grant, and commits the serialized fragments. The test then proves those
    // fragments are USABLE end-to-end: a proxy re-encrypts a real sealed object and only the recipient
    // (at ≥ threshold) recovers the plaintext — the owner key never left the backend.
    struct MockPre {
        owner: zeph_cipher::EncKeypair,
    }
    #[async_trait::async_trait]
    impl AppBackend for MockPre {
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
            Ok([0u8; 32])
        }
        async fn obj_get(&self, _c: [u8; 32]) -> anyhow::Result<Vec<u8>> {
            Ok(Vec::new())
        }
        fn now_millis(&self) -> u64 {
            0
        }
        async fn pre_rekey(
            &self,
            recipient_pk: Vec<u8>,
            threshold: u32,
            shares: u32,
        ) -> anyhow::Result<Option<Vec<u8>>> {
            let recipient = zeph_cipher::EncPublicKey::from_bytes(&recipient_pk)?;
            let kfrags =
                zeph_cipher::grant(&self.owner, &recipient, threshold as usize, shares as usize);
            Ok(Some(postcard::to_allocvec(&kfrags)?))
        }
    }

    // Reads the 33-byte recipient PRE pubkey (compressed curve point) from `input` (offset 0) then
    // delegates 2-of-3, writing the serialized fragments to offset 64 and committing exactly the
    // returned length.
    const PRE_GRANT_WAT: &[u8] = br#"(module
      (import "craftcom" "input"     (func $input     (param i32 i32) (result i32)))
      (import "craftcom" "pre_grant" (func $pre_grant (param i32 i32 i32 i32 i32 i32) (result i32)))
      (import "craftcom" "commit"    (func $commit    (param i32 i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "run")
        (local $n i32)
        (drop (call $input (i32.const 0) (i32.const 33)))
        (local.set $n
          (call $pre_grant (i32.const 0) (i32.const 33) (i32.const 2) (i32.const 3) (i32.const 64) (i32.const 8000)))
        (drop (call $commit (i32.const 64) (local.get $n)))))"#;

    #[tokio::test]
    async fn pre_grant_produces_usable_delegation_fragments() {
        let rt = TransitionRuntime::new().unwrap();
        let owner = zeph_cipher::EncKeypair::from_identity_seed(&[1u8; 32]);
        let recipient = zeph_cipher::EncKeypair::from_identity_seed(&[2u8; 32]);
        let owner_pk = owner.public();
        let recipient_pk = recipient.public();

        // The program receives the recipient's pubkey as input and asks the runtime to delegate. The
        // backend (own identity) holds the owner key; the app only ever sees the resulting fragments.
        let backend: Arc<dyn AppBackend> = Arc::new(MockPre {
            owner: zeph_cipher::EncKeypair::from_identity_seed(&[1u8; 32]),
        });
        let ctx = TransitionCtx::new(
            Vec::new(),
            recipient_pk.to_bytes(),
            [0u8; 32],
            "share".into(),
            0,
            Some(backend),
        );
        let committed = rt
            .run_program(
                PRE_GRANT_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::full(),
            )
            .await
            .unwrap();

        // The committed bytes deserialize to real re-encryption fragments (2-of-3).
        let kfrags: Vec<zeph_cipher::ReKeyFrag> =
            postcard::from_bytes(&committed).expect("pre_grant returns serialized kfrags");
        assert_eq!(kfrags.len(), 3, "3 shares delegated");

        // END-TO-END: a proxy re-encrypts a real sealed object with 2 fragments; the recipient recovers
        // it. This is the whole point — the fragments the app got from the host fn actually work, and
        // the owner secret never entered the runtime or the WASM.
        let secret = b"granted via the runtime, never touching the owner key";
        let obj = zeph_cipher::encrypt(&owner_pk, secret);
        let cfrags: Vec<_> = kfrags
            .iter()
            .take(2)
            .map(|kf| zeph_cipher::reencrypt(&owner_pk, &recipient_pk, &obj, kf).unwrap())
            .collect();
        assert_eq!(
            zeph_cipher::decrypt_granted(&recipient, &owner_pk, &obj, &cfrags).unwrap(),
            secret,
            "the recipient decrypts using fragments the app obtained from pre_grant"
        );
        // Below threshold (1 < 2) fails — the delegation really is 2-of-3.
        assert!(
            zeph_cipher::decrypt_granted(&recipient, &owner_pk, &obj, &cfrags[..1]).is_err(),
            "one fragment is below the 2-of-3 threshold → no decrypt"
        );
    }

    // THE GATE: pre_grant is bound ONLY under a grant containing Pre. The deterministic profile omits
    // it, so a pre_grant-importing program fails to instantiate (link-time capability gating).
    #[tokio::test]
    async fn pre_capability_gates_pre_grant() {
        let rt = TransitionRuntime::new().unwrap();
        let backend: Arc<dyn AppBackend> = Arc::new(MockPre {
            owner: zeph_cipher::EncKeypair::from_identity_seed(&[1u8; 32]),
        });
        let ctx = TransitionCtx::new(
            Vec::new(),
            zeph_cipher::EncKeypair::from_identity_seed(&[2u8; 32])
                .public()
                .to_bytes(),
            [0u8; 32],
            "share".into(),
            0,
            Some(backend),
        );
        // Deterministic profile (no Pre) → the `pre_grant` import can't resolve → instantiate fails.
        let err = rt
            .run_program(
                PRE_GRANT_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::deterministic(),
            )
            .await;
        assert!(
            err.is_err(),
            "pre_grant is not bound without the Pre capability → fails to instantiate"
        );
    }

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

    // Fills 16 bytes via `random` and commits them — the producer path for the RNG host fn.
    const RANDOM_WAT: &[u8] = br#"(module
      (import "craftcom" "random" (func $random (param i32 i32) (result i32)))
      (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "run")
        (drop (call $random (i32.const 0) (i32.const 16)))
        (drop (call $commit (i32.const 0) (i32.const 16)))))"#;

    #[tokio::test]
    async fn random_fills_bytes_under_full_and_is_denied_under_deterministic() {
        let rt = TransitionRuntime::new().unwrap();
        // full profile: `random` is bound → 16 fresh bytes, and two runs differ (not a fixed buffer).
        let run = || async {
            rt.run_program(
                RANDOM_WAT,
                "run",
                TransitionCtx::deterministic(vec![], vec![], 0),
                DEFAULT_FUEL,
                &CapabilityGrant::full(),
            )
            .await
            .unwrap()
        };
        let a = run().await;
        let b = run().await;
        assert_eq!(a.len(), 16, "random wrote the requested 16 bytes");
        assert_ne!(
            a, b,
            "two draws differ (astronomically unlikely to collide)"
        );

        // deterministic profile: `random` is NOT bound → the module fails to instantiate. This is the
        // safety property — a consensus/verified program can never observe randomness.
        assert!(
            rt.run_program(
                RANDOM_WAT,
                "run",
                TransitionCtx::deterministic(vec![], vec![], 0),
                DEFAULT_FUEL,
                &CapabilityGrant::deterministic(),
            )
            .await
            .is_err(),
            "deterministic profile must NOT bind random"
        );
    }

    // A CPI caller: invoke_program("token","ping", <empty>) into out[64..], commit the returned bytes
    // (0 if the call errored — `select`-guarded so a negative length never reaches `commit`).
    const INVOKE_WAT: &[u8] = br#"(module
      (import "craftcom" "invoke_program" (func $cpi (param i32 i32 i32 i32 i32 i32 i32 i32) (result i32)))
      (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
      (memory (export "memory") 1)
      (data (i32.const 0) "token")
      (data (i32.const 16) "ping")
      (func (export "run") (local $n i32)
        (local.set $n (call $cpi
          (i32.const 0) (i32.const 5) (i32.const 16) (i32.const 4)
          (i32.const 32) (i32.const 0) (i32.const 64) (i32.const 64)))
        (drop (call $commit (i32.const 64)
          (select (local.get $n) (i32.const 0) (i32.ge_s (local.get $n) (i32.const 0)))))))"#;

    struct MockInvoke(Vec<u8>);
    #[async_trait::async_trait]
    impl InvokeProgramBackend for MockInvoke {
        async fn invoke_program(
            &self,
            _name: &str,
            _func: &str,
            _input: Vec<u8>,
        ) -> Option<Vec<u8>> {
            Some(self.0.clone())
        }
    }

    #[tokio::test]
    async fn invoke_program_returns_the_callee_output_and_reproduces() {
        let rt = TransitionRuntime::new().unwrap();
        // Backend present → the callee's output flows back through the CPI host fn and is committed.
        let ctx = TransitionCtx::deterministic(vec![], vec![], 0)
            .with_invoke_backend(Some(Arc::new(MockInvoke(b"pong".to_vec()))));
        let out = rt
            .run_program(
                INVOKE_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::deterministic(),
            )
            .await
            .unwrap();
        assert_eq!(out, b"pong", "CPI output returned to the caller");
        // CPI is deterministic → a verifier re-run with the same inputs reproduces the same output.
        let ctx2 = TransitionCtx::deterministic(vec![], vec![], 0)
            .with_invoke_backend(Some(Arc::new(MockInvoke(b"pong".to_vec()))));
        let out2 = rt
            .run_program(
                INVOKE_WAT,
                "run",
                ctx2,
                DEFAULT_FUEL,
                &CapabilityGrant::deterministic(),
            )
            .await
            .unwrap();
        assert_eq!(out, out2, "CPI is deterministic → reproduces");
    }

    #[tokio::test]
    async fn invoke_program_without_backend_is_unavailable() {
        let rt = TransitionRuntime::new().unwrap();
        // No invoke backend (also a callee's situation → recursion bounded to one level) → cpi returns
        // -1, nothing committed.
        let ctx = TransitionCtx::deterministic(vec![], vec![], 0);
        let out = rt
            .run_program(
                INVOKE_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::deterministic(),
            )
            .await
            .unwrap();
        assert!(
            out.is_empty(),
            "no invoke backend → UNAVAILABLE, empty commit"
        );
    }

    #[tokio::test]
    async fn invoke_program_denied_without_the_capability() {
        let rt = TransitionRuntime::new().unwrap();
        // A grant lacking InvokeProgram must not bind the import → the module fails to instantiate.
        let ctx = TransitionCtx::deterministic(vec![], vec![], 0)
            .with_invoke_backend(Some(Arc::new(MockInvoke(b"pong".to_vec()))));
        assert!(
            rt.run_program(
                INVOKE_WAT,
                "run",
                ctx,
                DEFAULT_FUEL,
                &CapabilityGrant::deterministic().without(Capability::InvokeProgram),
            )
            .await
            .is_err(),
            "invoke_program is unbound without the capability → unknown import"
        );
    }
}
