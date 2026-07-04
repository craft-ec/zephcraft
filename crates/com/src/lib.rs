//! CraftCOM — sandboxed WASM compute over the Craftec substrate (foundation
//! Part G §38–41; docs/CRAFTCOM_DESIGN.md). **Mechanism, not policy:** the runtime
//! runs untrusted WASM agents with metered, capability-gated access to CraftSQL +
//! CraftOBJ. Aggregation and consensus are the app's job, never the runtime's.
//!
//! Phase 1: fuel-metered runtime core. Phase 2: host functions + the STRUCTURAL
//! capability gate. Phase 3: async execution + [`CraftBackend`] wiring the host
//! functions to real CraftSQL/CraftOBJ (see [`craft`]).
//!
//! The gate is STRUCTURAL — the host functions expose no namespace parameter, so an
//! agent can only ever write its OWN app namespace and read across other
//! participants' SAME app namespace. It can't name a personal DB or a neighbor app,
//! so there's nothing to "escape" — confinement is by construction.
//!
//! ## Guest ABI (import module `craftcom`)
//! The guest exports its linear memory as `memory`. Strings/bytes are passed as
//! `(ptr, len)`; results are written into a guest-provided `(out_ptr, out_cap)` and
//! the actual length is returned (`-1` on error / insufficient capacity).
//! - `clock() -> i64` — HLC millis.
//! - `caller(out, cap) -> i32` — writes the 32-byte invoking NodeId.
//! - `sql_execute(sql_ptr, sql_len) -> i64` — write SQL to the app's OWN namespace.
//! - `sql_query(owner_ptr, owner_len, sql_ptr, sql_len, out, cap) -> i32` — read the
//!   app namespace of `owner` (own if `owner_len==0`); result-JSON length written.
//! - `obj_put(ptr, len, out, cap) -> i32` — store bytes; writes the 32-byte CID.
//! - `obj_get(cid_ptr, out, cap) -> i32` — fetch by CID; content length written.

use std::sync::Arc;

use async_trait::async_trait;
use wasmtime::{Caller, Config, Engine, Extern, Linker, Memory, Module, Store};

mod attest;
mod craft;
mod invoke;
pub use attest::{
    attest_run, pda, select_committee, verify, verify_commit, verify_quorum, Attestation,
    AttestedCommit, AttestedRuntime, Committee, PdaAdvance,
};
pub use craft::CraftBackend;
pub use invoke::{invoke_remote, serve_invocations, InvokeRequest, InvokeService, INVOKE_ALPN};

/// Default fuel budget per invocation — roughly proportional to executed WASM
/// instructions (foundation §38). A runaway loop exhausts this and traps.
pub const DEFAULT_FUEL: u64 = 10_000_000;

/// Import module the host functions are bound under.
const HOST_MODULE: &str = "craftcom";

/// The substrate an agent's host functions act on. **Identity-bound:** an
/// implementation is constructed for ONE user (the node's own identity), so
/// `sql_execute` always writes THAT user's `(own, app_ns)` — the agent never picks
/// the writer. `sql_query` may name another participant (`owner`) but only within
/// the same `app_ns`. This is where the capability gate is enforced concretely.
#[async_trait]
pub trait AppBackend: Send + Sync {
    /// Write SQL against the OWN app namespace `(own_identity, app_ns)`.
    async fn sql_execute(&self, app_ns: &str, sql: &str) -> anyhow::Result<u64>;
    /// Read the app namespace of `owner` (own if `None`) — SAME `app_ns` only.
    async fn sql_query(
        &self,
        owner: Option<[u8; 32]>,
        app_ns: &str,
        sql: &str,
    ) -> anyhow::Result<String>;
    /// Store bytes as an app object; returns its CID.
    async fn obj_put(&self, data: &[u8]) -> anyhow::Result<[u8; 32]>;
    /// Fetch an app object by CID.
    async fn obj_get(&self, cid: [u8; 32]) -> anyhow::Result<Vec<u8>>;
    /// Current HLC time in millis (sync — no IO).
    fn now_millis(&self) -> u64;
}

/// Per-invocation host context in the Wasmtime `Store`: who invoked the agent, the
/// app namespace it is confined to, the substrate it acts on, and the invocation's
/// input bytes.
pub struct HostCtx {
    /// The verified NodeId that invoked this agent (exposed via `caller`).
    pub caller: [u8; 32],
    /// The app namespace this invocation is confined to (the gate).
    pub app_ns: String,
    /// The identity-bound substrate the host functions call.
    pub backend: Arc<dyn AppBackend>,
    /// Opaque input bytes for this invocation (exposed via `input`) — e.g. a post
    /// body, or the participant list for an aggregation.
    pub input: Vec<u8>,
}

impl HostCtx {
    /// An inert context (no capabilities) — for running modules that import no host
    /// functions (the runtime-core path).
    pub fn inert() -> Self {
        Self {
            caller: [0u8; 32],
            app_ns: String::new(),
            backend: Arc::new(Noop),
            input: Vec::new(),
        }
    }
}

/// Outcome of running an agent function: its return value + fuel actually burned.
#[derive(Debug, Clone, Copy)]
pub struct Outcome {
    pub value: i64,
    pub fuel_used: u64,
}

/// The CraftCOM runtime. One `Engine` is shared (it caches compiled code); every
/// invocation gets its OWN `Store` with a fresh fuel budget + host context, so
/// agents never share state and a trap in one cannot affect another.
pub struct Runtime {
    engine: Engine,
}

impl Runtime {
    /// Build a runtime with fuel metering + async execution enabled (host functions
    /// await CraftSQL/CraftOBJ).
    pub fn new() -> anyhow::Result<Self> {
        let mut cfg = Config::new();
        cfg.consume_fuel(true);
        cfg.async_support(true);
        cfg.wasm_backtrace(true);
        Ok(Self {
            engine: Engine::new(&cfg)?,
        })
    }

    /// Invoke `func` (no args → `i64`) with capability-gated host functions, under a
    /// fresh `fuel` budget. A runaway agent exhausts fuel and traps (`Err`).
    pub async fn invoke(
        &self,
        wasm: &[u8],
        func: &str,
        ctx: HostCtx,
        fuel: u64,
    ) -> anyhow::Result<Outcome> {
        let module = Module::new(&self.engine, wasm)?;
        let mut store = Store::new(&self.engine, ctx);
        store.set_fuel(fuel)?;
        let mut linker = Linker::new(&self.engine);
        bind_host_functions(&mut linker)?;
        let instance = linker.instantiate_async(&mut store, &module).await?;
        let f = instance.get_typed_func::<(), i64>(&mut store, func)?;
        let value = f.call_async(&mut store, ()).await?;
        let remaining = store.get_fuel().unwrap_or(0);
        Ok(Outcome {
            value,
            fuel_used: fuel.saturating_sub(remaining),
        })
    }

    /// Run a module with NO host capabilities (runtime-core path).
    pub async fn run_i64(&self, wasm: &[u8], func: &str, fuel: u64) -> anyhow::Result<Outcome> {
        self.invoke(wasm, func, HostCtx::inert(), fuel).await
    }
}

/// Bind the `craftcom` host functions onto the linker. Every function reads the app
/// namespace from the store CONTEXT — never from an agent argument — which is what
/// makes the namespace gate structural.
fn bind_host_functions(linker: &mut Linker<HostCtx>) -> anyhow::Result<()> {
    linker.func_wrap_async(
        HOST_MODULE,
        "clock",
        |caller: Caller<'_, HostCtx>, (): ()| {
            Box::new(async move { caller.data().backend.now_millis() as i64 })
        },
    )?;

    linker.func_wrap_async(
        HOST_MODULE,
        "caller",
        |mut caller: Caller<'_, HostCtx>, (out, cap): (i32, i32)| {
            Box::new(async move {
                let id = caller.data().caller;
                let Some(mem) = memory(&mut caller) else {
                    return -1i32;
                };
                write_out(&mut caller, &mem, out, cap, &id)
            })
        },
    )?;

    linker.func_wrap_async(
        HOST_MODULE,
        "input",
        |mut caller: Caller<'_, HostCtx>, (out, cap): (i32, i32)| {
            Box::new(async move {
                let input = caller.data().input.clone();
                let Some(mem) = memory(&mut caller) else {
                    return -1i32;
                };
                write_out(&mut caller, &mem, out, cap, &input)
            })
        },
    )?;

    linker.func_wrap_async(
        HOST_MODULE,
        "sql_execute",
        |mut caller: Caller<'_, HostCtx>, (ptr, len): (i32, i32)| {
            Box::new(async move {
                let Some(mem) = memory(&mut caller) else {
                    return -1i64;
                };
                let Some(sql) = read_str(&caller, &mem, ptr, len) else {
                    return -1;
                };
                let ns = caller.data().app_ns.clone(); // gate: ctx namespace, not agent
                let backend = caller.data().backend.clone();
                match backend.sql_execute(&ns, &sql).await {
                    Ok(n) => n as i64,
                    Err(_) => -1,
                }
            })
        },
    )?;

    linker.func_wrap_async(
        HOST_MODULE,
        "sql_query",
        |mut caller: Caller<'_, HostCtx>,
         (owner_ptr, owner_len, sql_ptr, sql_len, out, cap): (i32, i32, i32, i32, i32, i32)| {
            Box::new(async move {
                let Some(mem) = memory(&mut caller) else {
                    return -1i32;
                };
                let owner = if owner_len == 0 {
                    None
                } else {
                    match read_bytes(&caller, &mem, owner_ptr, owner_len) {
                        Some(b) if b.len() == 32 => {
                            let mut a = [0u8; 32];
                            a.copy_from_slice(&b);
                            Some(a)
                        }
                        _ => return -1,
                    }
                };
                let Some(sql) = read_str(&caller, &mem, sql_ptr, sql_len) else {
                    return -1;
                };
                let ns = caller.data().app_ns.clone(); // gate: same app_ns only
                let backend = caller.data().backend.clone();
                match backend.sql_query(owner, &ns, &sql).await {
                    Ok(res) => write_out(&mut caller, &mem, out, cap, res.as_bytes()),
                    Err(_) => -1,
                }
            })
        },
    )?;

    linker.func_wrap_async(
        HOST_MODULE,
        "obj_put",
        |mut caller: Caller<'_, HostCtx>, (ptr, len, out, cap): (i32, i32, i32, i32)| {
            Box::new(async move {
                let Some(mem) = memory(&mut caller) else {
                    return -1i32;
                };
                let Some(data) = read_bytes(&caller, &mem, ptr, len) else {
                    return -1;
                };
                let backend = caller.data().backend.clone();
                match backend.obj_put(&data).await {
                    Ok(cid) => write_out(&mut caller, &mem, out, cap, &cid),
                    Err(_) => -1,
                }
            })
        },
    )?;

    linker.func_wrap_async(
        HOST_MODULE,
        "obj_get",
        |mut caller: Caller<'_, HostCtx>, (cid_ptr, out, cap): (i32, i32, i32)| {
            Box::new(async move {
                let Some(mem) = memory(&mut caller) else {
                    return -1i32;
                };
                let cid = match read_bytes(&caller, &mem, cid_ptr, 32) {
                    Some(b) if b.len() == 32 => {
                        let mut a = [0u8; 32];
                        a.copy_from_slice(&b);
                        a
                    }
                    _ => return -1,
                };
                let backend = caller.data().backend.clone();
                match backend.obj_get(cid).await {
                    Ok(data) => write_out(&mut caller, &mem, out, cap, &data),
                    Err(_) => -1,
                }
            })
        },
    )?;

    Ok(())
}

/// The agent's exported linear memory, if present.
fn memory(caller: &mut Caller<'_, HostCtx>) -> Option<Memory> {
    match caller.get_export("memory") {
        Some(Extern::Memory(m)) => Some(m),
        _ => None,
    }
}

/// Copy `len` bytes out of guest memory at `ptr` (bounds-checked).
fn read_bytes(caller: &Caller<'_, HostCtx>, mem: &Memory, ptr: i32, len: i32) -> Option<Vec<u8>> {
    if ptr < 0 || len < 0 {
        return None;
    }
    let data = mem.data(caller);
    let (s, e) = (ptr as usize, ptr as usize + len as usize);
    data.get(s..e).map(|b| b.to_vec())
}

/// Read a UTF-8 string out of guest memory.
fn read_str(caller: &Caller<'_, HostCtx>, mem: &Memory, ptr: i32, len: i32) -> Option<String> {
    read_bytes(caller, mem, ptr, len).and_then(|b| String::from_utf8(b).ok())
}

/// Write `data` into the guest's `(out, cap)` buffer; returns bytes written, or
/// `-1` if it doesn't fit or the region is out of bounds.
fn write_out(
    caller: &mut Caller<'_, HostCtx>,
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

/// A no-capability backend (for `HostCtx::inert`).
struct Noop;
#[async_trait]
impl AppBackend for Noop {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // ---- phase 1: runtime core ----

    #[tokio::test]
    async fn runs_a_module_and_returns_a_value() {
        let rt = Runtime::new().unwrap();
        let wasm = br#"(module (func (export "answer") (result i64) i64.const 42))"#;
        let out = rt.run_i64(wasm, "answer", DEFAULT_FUEL).await.unwrap();
        assert_eq!(out.value, 42);
        assert!(out.fuel_used > 0);
    }

    #[tokio::test]
    async fn runaway_loop_traps_on_fuel_exhaustion() {
        let rt = Runtime::new().unwrap();
        let wasm = br#"(module (func (export "spin") (result i64) (loop (br 0)) i64.const 0))"#;
        let err = rt.run_i64(wasm, "spin", 100_000).await.unwrap_err();
        let msg = format!("{err:#}").to_lowercase();
        assert!(msg.contains("fuel") || msg.contains("trap") || msg.contains("interrupt"));
    }

    // ---- phase 2: host functions + capability gate ----

    type QueryLog = Vec<(Option<[u8; 32]>, String, String)>; // (owner, app_ns, sql)

    #[derive(Default)]
    struct MockBackend {
        executes: Mutex<Vec<(String, String)>>, // (app_ns, sql)
        queries: Mutex<QueryLog>,
        clock: u64,
    }
    #[async_trait]
    impl AppBackend for MockBackend {
        async fn sql_execute(&self, ns: &str, sql: &str) -> anyhow::Result<u64> {
            self.executes.lock().unwrap().push((ns.into(), sql.into()));
            Ok(3)
        }
        async fn sql_query(
            &self,
            o: Option<[u8; 32]>,
            ns: &str,
            sql: &str,
        ) -> anyhow::Result<String> {
            self.queries
                .lock()
                .unwrap()
                .push((o, ns.into(), sql.into()));
            Ok("[]".into())
        }
        async fn obj_put(&self, _d: &[u8]) -> anyhow::Result<[u8; 32]> {
            Ok([9u8; 32])
        }
        async fn obj_get(&self, _c: [u8; 32]) -> anyhow::Result<Vec<u8>> {
            Ok(vec![1, 2, 3])
        }
        fn now_millis(&self) -> u64 {
            self.clock
        }
    }

    fn ctx(backend: Arc<dyn AppBackend>, caller: [u8; 32], app_ns: &str) -> HostCtx {
        HostCtx {
            caller,
            app_ns: app_ns.into(),
            backend,
            input: Vec::new(),
        }
    }

    #[tokio::test]
    async fn clock_host_function_returns_backend_time() {
        let rt = Runtime::new().unwrap();
        let backend = Arc::new(MockBackend {
            clock: 999,
            ..Default::default()
        });
        let wasm = br#"(module
            (import "craftcom" "clock" (func $clock (result i64)))
            (func (export "run") (result i64) (call $clock)))"#;
        let out = rt
            .invoke(wasm, "run", ctx(backend, [0; 32], "feed"), DEFAULT_FUEL)
            .await
            .unwrap();
        assert_eq!(out.value, 999);
    }

    #[tokio::test]
    async fn caller_host_function_exposes_the_invoking_identity() {
        let rt = Runtime::new().unwrap();
        let backend = Arc::new(MockBackend::default());
        let wasm = br#"(module
            (import "craftcom" "caller" (func $caller (param i32 i32) (result i32)))
            (memory (export "memory") 1)
            (func (export "run") (result i64)
                (drop (call $caller (i32.const 0) (i32.const 32)))
                (i64.load8_u (i32.const 0))))"#;
        let out = rt
            .invoke(wasm, "run", ctx(backend, [0xAB; 32], "feed"), DEFAULT_FUEL)
            .await
            .unwrap();
        assert_eq!(out.value, 0xAB);
    }

    #[tokio::test]
    async fn sql_execute_is_confined_to_the_context_app_namespace() {
        let rt = Runtime::new().unwrap();
        let backend = Arc::new(MockBackend::default());
        let wasm = br#"(module
            (import "craftcom" "sql_execute" (func $exec (param i32 i32) (result i64)))
            (memory (export "memory") 1)
            (data (i32.const 0) "hello")
            (func (export "run") (result i64) (call $exec (i32.const 0) (i32.const 5))))"#;
        let out = rt
            .invoke(
                wasm,
                "run",
                ctx(backend.clone(), [0; 32], "feed"),
                DEFAULT_FUEL,
            )
            .await
            .unwrap();
        assert_eq!(out.value, 3);
        let recorded = backend.executes.lock().unwrap();
        assert_eq!(
            recorded.as_slice(),
            &[("feed".to_string(), "hello".to_string())],
            "the write landed in the CONTEXT's app_ns — agent cannot pick another"
        );
    }

    #[tokio::test]
    async fn sql_query_reads_another_participant_but_same_app_namespace() {
        let rt = Runtime::new().unwrap();
        let backend = Arc::new(MockBackend::default());
        let wasm = br#"(module
            (import "craftcom" "sql_query"
                (func $q (param i32 i32 i32 i32 i32 i32) (result i32)))
            (memory (export "memory") 1)
            (data (i32.const 0) "\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02")
            (data (i32.const 32) "SELECT")
            (func (export "run") (result i64)
                (i64.extend_i32_s
                    (call $q (i32.const 0) (i32.const 32) (i32.const 32) (i32.const 6)
                            (i32.const 64) (i32.const 256)))))"#;
        let out = rt
            .invoke(
                wasm,
                "run",
                ctx(backend.clone(), [0; 32], "feed"),
                DEFAULT_FUEL,
            )
            .await
            .unwrap();
        assert_eq!(out.value, 2);
        let recorded = backend.queries.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        let (owner, ns, sql) = &recorded[0];
        assert_eq!(*owner, Some([0x02; 32]));
        assert_eq!(ns, "feed");
        assert_eq!(sql, "SELECT");
    }
}
