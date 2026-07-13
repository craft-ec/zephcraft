//! CraftCOM — sandboxed WASM compute over the Craftec substrate (foundation
//! Part G §38–41; docs/CRAFTCOM_DESIGN.md). **Mechanism, not policy:** the runtime
//! runs untrusted WASM agents with metered, capability-gated access to CraftSQL +
//! CraftOBJ. Aggregation and consensus are the app's job, never the runtime's.
//!
//! There is ONE runtime — the unified [`TransitionRuntime`] (see [`transition`]) — with a
//! per-program [`CapabilityGrant`]: consensus-critical programs run under the deterministic
//! subset, userspace apps under `full`. The host ABI (`input`/`state`/`commit`/`caller`/
//! `sql_*`/`obj_*`/`clock`/`wall_clock`/`ed25519_verify`) is bound per grant at link time, so a
//! program importing a non-granted capability fails to instantiate (`COMPUTE_EXECUTION_DESIGN.md`).
//!
//! The namespace gate is STRUCTURAL — the sql/obj host functions expose no namespace
//! parameter, so an agent can only ever write its OWN app namespace and read across other
//! participants' SAME app namespace. It can't name a personal DB or a neighbor app, so
//! there's nothing to "escape" — confinement is by construction.
//!
//! ## Guest ABI (import module `craftcom`)
//! The guest exports its linear memory as `memory`. Strings/bytes are passed as
//! `(ptr, len)`; an app declares its output via `commit(ptr, len)` and exports `run()`
//! (no result). Read helpers write into a guest-provided `(out_ptr, out_cap)` and return
//! the actual length (`-1` on error / insufficient capacity).
//! - `clock() -> i64` — CONSENSUS time millis (`ctx.now`, reproducible; deterministic profile).
//! - `wall_clock() -> i64` — real per-node wall-time millis (app profile only).
//! - `caller(out, cap) -> i32` — writes the 32-byte invoking NodeId.
//! - `input(out, cap) -> i32` — writes the invocation input bytes.
//! - `commit(ptr, len) -> i32` — declare the invocation output bytes.
//! - `sql_execute(sql_ptr, sql_len) -> i64` — write SQL to the app's OWN namespace.
//! - `sql_query(owner_ptr, owner_len, sql_ptr, sql_len, out, cap) -> i32` — read the
//!   app namespace of `owner` (own if `owner_len==0`); result-JSON length written.
//! - `obj_put(ptr, len, out, cap) -> i32` — store bytes; writes the 32-byte CID.
//! - `obj_get(cid_ptr, out, cap) -> i32` — fetch by CID; content length written.

use async_trait::async_trait;

mod attestation;
mod capability;
mod craft;
mod gov;
mod invoke;
mod registry;
mod transition;
mod verification;
pub use attestation::{
    AttestAction, AttestProposal, Attestation, MemberSignature, Quorum, QuorumChain,
};
pub use capability::{Capability, CapabilityGrant};
pub use craft::CraftBackend;
pub use gov::{
    GovAction, GovSignature, GovernanceApproval, GovernanceChain, GovernanceProposal, GovernanceSet,
};
pub use invoke::{invoke_remote, serve_invocations, InvokeRequest, InvokeService, INVOKE_ALPN};
pub use registry::{
    registry_program_cid, ConfigRegistryState, HeadEntry, HeadSubmission, NativeProgram,
    ProgramRegistryState, RegistryProgram, RegistryState,
};
pub use transition::{pda, TransitionCtx, TransitionRuntime};
pub use verification::{
    produce, verify_locally, Board, BoardSnapshot, PostedRequest, Verdict, Verifier, VerifierSet,
    VerifyPolicy, VerifyRequest,
};

/// Default fuel budget per invocation — roughly proportional to executed WASM
/// instructions (foundation §38). A runaway loop exhausts this and traps.
pub const DEFAULT_FUEL: u64 = 10_000_000;

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

/// The node service the `verify` host fn calls to run one verification round: post `req` to the
/// board (as this node's producer) and await its policy certificate, or time out. Injected into a
/// program run the same way [`AppBackend`] is (identity-bound, per-node). Returns true iff `k`
/// independent verifiers confirmed the claim — the `verify` host fn maps that to `1` (verified) /
/// `0` (rejected). Keeps consistency (verification) out of the deterministic runtime: it is an
/// app-profile orchestration call, never part of a re-run pure `f`.
#[async_trait]
pub trait VerifyBackend: Send + Sync {
    async fn verify(&self, req: VerifyRequest) -> bool;
}

/// The node service the `attest` host fn calls to run one attestation round: solicit sign-offs from
/// the program's declared quorum over `statement`, and return whether `k`-of-n authorized it (or a
/// timeout). Injected the same way [`AppBackend`]/[`VerifyBackend`] are. Attestation is AUTHORITY —
/// "do the parties I chose approve this?" — distinct from verification (consistency); the `attest`
/// host fn maps the result to `1` (authorized) / `0` (rejected). App-profile orchestration, never
/// part of a re-run pure `f`.
#[async_trait]
pub trait AttestBackend: Send + Sync {
    async fn attest(&self, program_cid: [u8; 32], statement: Vec<u8>) -> bool;
}
