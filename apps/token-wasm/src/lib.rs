//! The TOKEN program as a governance-upgradeable WASM PROTOCOL PROGRAM — the canonical program pinned
//! behind the K1 `token` anchor, and the program of every account chain
//! (`ECONOMY_PROGRAMS_DESIGN.md §5`; replaces the pre-split `token-ledger`/ledger.wasm).
//!
//! A thin wrapper over `zeph_token` (the SAME crate the node folds natively): read the account's prior
//! `TokenState` via `state`, the node-built `LedgerInput` via `input`, run the pure transition, and
//! `commit` the new state. An empty commit = a rejected write. Because the wasm and the node share
//! `zeph_token`, a verifier re-running this program reproduces the node's fold exactly.

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
    fn state(out: *mut u8, cap: i32) -> i32;
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
    let prev = read_host(state);
    let req = read_host(input);
    if let Some(out) = zeph_token::run_transition(&prev, &req) {
        unsafe {
            commit(out.as_ptr(), out.len() as i32);
        }
    }
    // else: reject → commit nothing (empty output = a rejected write)
}
