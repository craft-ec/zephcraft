//! A minimal user program for the generic attested-account demo: a committee-attested
//! counter. State = a u64 (little-endian). Request = an optional u64 amount (default 1).
//! Each advance runs under the committee, so the count moves forward with no keyholder.
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

fn read(f: unsafe extern "C" fn(*mut u8, i32) -> i32) -> Vec<u8> {
    let mut buf = vec![0u8; 64];
    let n = unsafe { f(buf.as_mut_ptr(), buf.len() as i32) };
    if n < 0 {
        return Vec::new();
    }
    buf.truncate(n as usize);
    buf
}

fn u64le(b: &[u8], default: u64) -> u64 {
    if b.len() >= 8 {
        u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
    } else {
        default
    }
}

#[no_mangle]
pub extern "C" fn run() {
    let cur = u64le(&read(state), 0);
    let amount = u64le(&read(input), 1);
    let next = cur.wrapping_add(amount);
    let out = next.to_le_bytes();
    unsafe {
        commit(out.as_ptr(), out.len() as i32);
    }
}
