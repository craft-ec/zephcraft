//! CraftCOM — sandboxed WASM compute over the Craftec substrate (foundation
//! Part G §38–41; docs/CRAFTCOM_DESIGN.md). **Mechanism, not policy:** the runtime
//! runs untrusted WASM agents with metered, capability-gated access to CraftSQL +
//! CraftOBJ. Aggregation and consensus are the app's job, never the runtime's.
//!
//! Phase 1 (this file): the runtime CORE — a Wasmtime engine configured for
//! deterministic, fuel-metered, sandboxed execution. Load a module, run a
//! function, get a result; a runaway loop exhausts fuel and traps. Host functions,
//! the per-user app-namespace capability gate, and invocation land in later phases.

use wasmtime::{Config, Engine, Linker, Module, Store};

/// Default fuel budget per invocation — roughly proportional to executed WASM
/// instructions (foundation §38). A runaway loop exhausts this and traps.
pub const DEFAULT_FUEL: u64 = 10_000_000;

/// Outcome of running an agent function: its return value + fuel actually burned.
#[derive(Debug, Clone, Copy)]
pub struct Outcome {
    pub value: i64,
    pub fuel_used: u64,
}

/// Per-invocation host context stored in the Wasmtime `Store`. Phase 1 carries
/// nothing; later phases hold the caller identity + app-namespace capabilities
/// that host functions read.
struct HostCtx;

/// The CraftCOM runtime. One `Engine` is shared (it caches compiled code); every
/// invocation gets its OWN `Store` with a fresh fuel budget, so agents never share
/// state and a trap in one cannot affect another.
pub struct Runtime {
    engine: Engine,
}

impl Runtime {
    /// Build a runtime with fuel metering enabled. Fails only if the host platform
    /// can't initialize the Wasmtime engine.
    pub fn new() -> anyhow::Result<Self> {
        let mut cfg = Config::new();
        cfg.consume_fuel(true); // metered execution — bounds runaway agents
        cfg.wasm_backtrace(true);
        let engine = Engine::new(&cfg)?;
        Ok(Self { engine })
    }

    /// Run the exported `func` (no args → `i64`) under a fresh `fuel` budget.
    /// Returns the value + fuel consumed; a runaway agent exhausts fuel and traps
    /// (returned as `Err`), never hangs the node.
    pub fn run_i64(&self, wasm: &[u8], func: &str, fuel: u64) -> anyhow::Result<Outcome> {
        let module = Module::new(&self.engine, wasm)?;
        let mut store = Store::new(&self.engine, HostCtx);
        store.set_fuel(fuel)?;
        // Phase 1: no host functions yet — an empty linker.
        let linker = Linker::new(&self.engine);
        let instance = linker.instantiate(&mut store, &module)?;
        let f = instance.get_typed_func::<(), i64>(&mut store, func)?;
        let value = f.call(&mut store, ())?;
        let remaining = store.get_fuel().unwrap_or(0);
        Ok(Outcome {
            value,
            fuel_used: fuel.saturating_sub(remaining),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A trivial agent runs deterministically and returns its value; fuel is burned.
    #[test]
    fn runs_a_module_and_returns_a_value() {
        let rt = Runtime::new().unwrap();
        let wasm = br#"(module (func (export "answer") (result i64) i64.const 42))"#;
        let out = rt.run_i64(wasm, "answer", DEFAULT_FUEL).unwrap();
        assert_eq!(out.value, 42);
        assert!(out.fuel_used > 0, "executing a function burns fuel");
    }

    /// A runaway loop exhausts its fuel budget and TRAPS — it cannot hang the node.
    #[test]
    fn runaway_loop_traps_on_fuel_exhaustion() {
        let rt = Runtime::new().unwrap();
        let wasm = br#"(module (func (export "spin") (result i64)
            (loop (br 0))
            i64.const 0))"#;
        let err = rt.run_i64(wasm, "spin", 100_000).unwrap_err();
        let msg = format!("{err:#}").to_lowercase();
        assert!(
            msg.contains("fuel") || msg.contains("trap") || msg.contains("interrupt"),
            "expected a fuel/trap error, got: {msg}"
        );
    }

    /// Two invocations of the same module are isolated — separate fresh stores.
    #[test]
    fn invocations_are_isolated() {
        let rt = Runtime::new().unwrap();
        let wasm = br#"(module (func (export "answer") (result i64) i64.const 7))"#;
        let a = rt.run_i64(wasm, "answer", DEFAULT_FUEL).unwrap();
        let b = rt.run_i64(wasm, "answer", DEFAULT_FUEL).unwrap();
        assert_eq!(a.value, 7);
        assert_eq!(b.value, 7);
        assert_eq!(
            a.fuel_used, b.fuel_used,
            "identical runs burn identical fuel"
        );
    }
}
