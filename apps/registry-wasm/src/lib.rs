//! The app-name registry, as a governance-upgradeable WASM program. Runs under the
//! CraftCOM deterministic attested ABI: reads the prior `RegistryState` via `state`, the
//! signed `HeadSubmission` via `input`, verifies the owner signature with the
//! `ed25519_verify` host function, upserts, and `commit`s the new state. The types +
//! encodings mirror `zeph-com`'s registry exactly (postcard), so its output decodes as a
//! `RegistryState` on the node.
#![no_std]

extern crate alloc;

use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use serde::{Deserialize, Serialize};

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
    fn ed25519_verify(pk: *const u8, msg: *const u8, msg_len: i32, sig: *const u8) -> i32;
}

const HEAD_DOMAIN: &[u8] = b"craftec/head/1";

#[derive(Serialize, Deserialize)]
struct HeadSubmission {
    owner: [u8; 32],
    name: String,
    cid: [u8; 32],
    version: u64,
    signature: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone)]
struct HeadEntry {
    owner: [u8; 32],
    name: String,
    cid: [u8; 32],
    version: u64,
}

#[derive(Serialize, Deserialize, Default)]
struct RegistryState {
    entries: Vec<HeadEntry>,
}

fn head_signing_bytes(owner: &[u8; 32], name: &str, cid: &[u8; 32], version: u64) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(HEAD_DOMAIN);
    b.extend_from_slice(owner);
    b.extend_from_slice(&(name.len() as u32).to_be_bytes());
    b.extend_from_slice(name.as_bytes());
    b.extend_from_slice(cid);
    b.extend_from_slice(&version.to_be_bytes());
    b
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

/// The registry transition. On any validation failure it commits nothing (empty output),
/// which the node treats as a rejected submission.
#[no_mangle]
pub extern "C" fn run() {
    let prev = read_host(state);
    let req = read_host(input);

    let mut st: RegistryState = if prev.is_empty() {
        RegistryState::default()
    } else {
        match postcard::from_bytes(&prev) {
            Ok(s) => s,
            Err(_) => return,
        }
    };
    let sub: HeadSubmission = match postcard::from_bytes(&req) {
        Ok(s) => s,
        Err(_) => return,
    };

    if sub.signature.len() != 64 {
        return;
    }
    let msg = head_signing_bytes(&sub.owner, &sub.name, &sub.cid, sub.version);
    let ok = unsafe {
        ed25519_verify(
            sub.owner.as_ptr(),
            msg.as_ptr(),
            msg.len() as i32,
            sub.signature.as_ptr(),
        )
    };
    if ok != 1 {
        return; // bad owner signature
    }

    let entry = HeadEntry {
        owner: sub.owner,
        name: sub.name.clone(),
        cid: sub.cid,
        version: sub.version,
    };
    match st.entries.binary_search_by(|e| {
        e.owner
            .cmp(&entry.owner)
            .then_with(|| e.name.as_str().cmp(entry.name.as_str()))
    }) {
        Ok(i) => {
            if sub.version <= st.entries[i].version {
                return; // stale version
            }
            st.entries[i] = entry;
        }
        Err(i) => st.entries.insert(i, entry),
    }

    let out = postcard::to_allocvec(&st).unwrap_or_default();
    unsafe {
        commit(out.as_ptr(), out.len() as i32);
    }
}
