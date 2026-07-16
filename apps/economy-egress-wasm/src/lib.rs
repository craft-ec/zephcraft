//! The ECONOMY-EGRESS program as a governance-swappable WASM PROTOCOL PROGRAM — the canonical program
//! pinned behind the K1 `economy-egress` anchor (`ECONOMY_PROGRAMS_DESIGN.md §5`; replaces the pre-split
//! `reward`/reward.wasm, absorbing its valuation).
//!
//! A thin, STATELESS wrapper over the shared `zeph_economy_egress` crate: read the node-built
//! `RewardInput` via `input`, compute the egress valuation (contribution-ratio shares), and `commit` the
//! encoded `RewardRecord`. Stateless (no `state`) — it is a pure function of its input, so a verifier
//! re-running it reproduces the node's native computation exactly. Balances live in the separate `token`
//! program; this one only says what each provider is OWED.

#![no_std]

extern crate alloc;

use alloc::vec;
use alloc::vec::Vec;

#[global_allocator]
static ALLOC: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;

#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

#[link(wasm_import_module = "craftcom")]
extern "C" {
    fn input(out: *mut u8, cap: i32) -> i32;
    fn commit(ptr: *const u8, len: i32) -> i32;
}

fn read_host(f: unsafe extern "C" fn(*mut u8, i32) -> i32) -> Vec<u8> {
    let mut buf = vec![0u8; 1 << 20]; // 1 MiB cap
    let n = unsafe { f(buf.as_mut_ptr(), buf.len() as i32) };
    if n < 0 {
        return Vec::new();
    }
    buf.truncate(n as usize);
    buf
}

#[no_mangle]
pub extern "C" fn run() {
    let req = read_host(input);
    if let Some(out) = zeph_economy_egress::run_program(&req) {
        unsafe {
            commit(out.as_ptr(), out.len() as i32);
        }
    }
    // else: malformed input → commit nothing
}
