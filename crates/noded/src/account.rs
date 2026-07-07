//! Generic program accounts — the substrate behind the program registry, usable by ANY
//! program. An account is `pda(program_cid, seed)`; its state is advanced by running the
//! program's WASM on `(prev_state, request)`.
//!
//! **The program itself is the writer.** Its deterministic execution IS the write authority —
//! the program validates the request and decides the new state. There is no keyholder, no
//! committee, no attestation: a single writer per account by construction (one advance at a
//! time; whatever authority a write needs is enforced *inside* the program, e.g. an
//! owner-signed request). The program registry is one instance of this substrate; a counter /
//! tally / any shared-state program is another. Aligns with `MINIMAL_KERNEL_DESIGN`: the
//! kernel provides the account mechanism (derive + single-writer + persist + publish); each
//! use is a program on top.
//!
//! State model (first cut): a current-state blob per account under
//! `<data_dir>/accounts/<account>.state`, published as durable content (erasure-coded, so it
//! survives node loss). Local resolve reads the node's own copy. SQL-backed account state and
//! non-DHT cross-node resolution are the next layers.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use zeph_com::{pda, CapabilityGrant, TransitionRuntime, DEFAULT_FUEL};
use zeph_core::Cid;
use zeph_obj::{ConsumeMode, ObjEngine};

/// The outcome of advancing an account.
pub struct AdvanceResult {
    pub account: [u8; 32],
    pub new_root: [u8; 32],
}

/// The node's generic program-account store. Each account = `pda(program_cid, seed)`, a
/// single-writer state advanced PURELY by running the program — no committee, no attestation.
pub struct ProgramAccountStore {
    obj: Arc<ObjEngine>,
    runtime: TransitionRuntime,
    dir: PathBuf,
}

impl ProgramAccountStore {
    pub fn open(obj: Arc<ObjEngine>, data_dir: &Path) -> Self {
        let dir = data_dir.join("accounts");
        let _ = std::fs::create_dir_all(&dir);
        Self {
            obj,
            runtime: TransitionRuntime::new().expect("program runtime"),
            dir,
        }
    }

    fn state_path(&self, account: [u8; 32]) -> PathBuf {
        self.dir.join(format!("{}.state", hex::encode(account)))
    }
    fn load_state(&self, account: [u8; 32]) -> Vec<u8> {
        std::fs::read(self.state_path(account)).unwrap_or_default()
    }

    /// Fetch a program's WASM bytes by cid (following a File manifest to its content).
    async fn fetch_program(&self, cid: [u8; 32]) -> Option<Vec<u8>> {
        let raw = self.obj.get(Cid(cid), ConsumeMode::Drop).await.ok()?;
        match zeph_obj::Manifest::decode(&raw) {
            Some(zeph_obj::Manifest::File { content, .. }) => {
                self.obj.get(Cid(content), ConsumeMode::Drop).await.ok()
            }
            _ => Some(raw),
        }
    }

    /// Run the program on `(prev, request)`. `None` = the program rejected the request
    /// (empty output) or its wasm is unavailable.
    async fn run(&self, program_cid: [u8; 32], prev: &[u8], request: &[u8]) -> Option<Vec<u8>> {
        let wasm = self.fetch_program(program_cid).await?;
        // Protocol program-accounts are consensus-critical → the deterministic profile
        // (the safe default): every node computes the identical new state.
        let out = self
            .runtime
            .run_transition(
                &wasm,
                "run",
                prev,
                request,
                DEFAULT_FUEL,
                &CapabilityGrant::deterministic(),
            )
            .ok()?;
        (!out.is_empty()).then_some(out)
    }

    /// Advance an account by running its program — the program's execution IS the write.
    /// The account ADDRESS is `pda(program_id, seed)` (a STABLE namespace that survives
    /// code upgrades), while the EXECUTING WASM is `code_cid` — so governance can swap the
    /// program behind an account without moving it. Persists the new state locally +
    /// publishes it as durable content. Single-writer: one advance at a time per account.
    pub async fn advance(
        &self,
        program_id: [u8; 32],
        code_cid: [u8; 32],
        seed: &[u8],
        request: &[u8],
    ) -> anyhow::Result<AdvanceResult> {
        let account = pda(&program_id, seed).0;
        let prev = self.load_state(account);
        let new_state = self
            .run(code_cid, &prev, request)
            .await
            .ok_or_else(|| anyhow::anyhow!("program rejected the request"))?;
        std::fs::write(self.state_path(account), &new_state)?;
        let _ = self.obj.publish_system(&new_state).await; // durable content (survives node loss)
        Ok(AdvanceResult {
            account,
            new_root: Cid::of(&new_state).0,
        })
    }

    /// The current state of `pda(program_cid, seed)` (local copy).
    pub async fn resolve(&self, program_cid: [u8; 32], seed: &[u8]) -> Vec<u8> {
        self.load_state(pda(&program_cid, seed).0)
    }

    /// Adopt `bytes` DIRECTLY as the state of `pda(program_id, seed)` — write it as the
    /// account's state file and publish it as durable content WITHOUT running a program.
    /// Used to adopt a registry state handed off from the previous epoch's writer (the state
    /// was already validated by the program when it was originally advanced). Mirrors
    /// `advance`'s persist+publish tail.
    pub async fn put_state(
        &self,
        program_id: [u8; 32],
        seed: &[u8],
        bytes: &[u8],
    ) -> anyhow::Result<()> {
        let account = pda(&program_id, seed).0;
        std::fs::write(self.state_path(account), bytes)?;
        let _ = self.obj.publish_system(bytes).await; // durable content (survives node loss)
        Ok(())
    }
}
