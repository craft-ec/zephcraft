//! A minimal CraftCOM capability app proving the compute layer end-to-end: it reads an
//! invocation message via `input`, appends it to a `guestbook` table via `sql_execute`,
//! reads back the row count via `sql_query`, and declares that count text as its output
//! via `commit`. Dependency-free (`no_std`, no alloc, fixed stack buffers) so the wasm is
//! tiny and cannot panic on odd input.
//!
//! Host ABI mirrors `crates/com/src/transition.rs` `bind_granted` exactly (module
//! `"craftcom"`); the (ptr,len) read / (out,cap) write conventions match the working WATs
//! in `crates/com/tests/craft_backend.rs` and `feed.rs`.
#![no_std]

#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

#[link(wasm_import_module = "craftcom")]
extern "C" {
    // input(out, cap) -> i32 : bytes written (the invocation message), or -1.
    fn input(out: *mut u8, cap: i32) -> i32;
    // sql_execute(ptr, len) -> i64 : rows-affected, or -1. Writes to the app's OWN ns.
    fn sql_execute(ptr: *const u8, len: i32) -> i64;
    // sql_query(owner_ptr, owner_len, sql_ptr, sql_len, out, cap) -> i32 : bytes written,
    // or -1. owner_len == 0 => read our OWN namespace (owner_ptr ignored).
    fn sql_query(
        owner_ptr: *const u8,
        owner_len: i32,
        sql_ptr: *const u8,
        sql_len: i32,
        out: *mut u8,
        cap: i32,
    ) -> i32;
    // commit(ptr, len) -> i32 : declares [ptr, ptr+len) as the app's output bytes.
    fn commit(ptr: *const u8, len: i32) -> i32;
}

/// Cap on the accepted message length (defensive; anything longer is truncated).
const MAX_MSG: usize = 512;

#[no_mangle]
pub extern "C" fn run() {
    // 1. Read the invocation input (the guestbook message) into a fixed buffer.
    let mut msg = [0u8; MAX_MSG];
    let n = unsafe { input(msg.as_mut_ptr(), MAX_MSG as i32) };
    let msg_len = if n < 0 {
        0
    } else {
        (n as usize).min(MAX_MSG)
    };

    // 2. Build the SQL in a fixed buffer: create-if-needed, then insert the message.
    //    execute_batch on the host runs both statements. Single quotes are escaped by
    //    doubling ('' ); control bytes (< 0x20) are dropped so the literal stays clean.
    const PREFIX: &[u8] =
        b"CREATE TABLE IF NOT EXISTS guestbook(id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT);INSERT INTO guestbook(msg) VALUES('";
    const SUFFIX: &[u8] = b"');";
    // Worst case: every message byte becomes two ('' ) -> 2 * MAX_MSG, plus fixed parts.
    let mut sql = [0u8; 256 + 2 * MAX_MSG];
    let mut pos = 0usize;

    for &b in PREFIX {
        sql[pos] = b;
        pos += 1;
    }
    let escape_room = sql.len() - SUFFIX.len() - 1; // leave space for the closing suffix
    for i in 0..msg_len {
        let c = msg[i];
        if c < 0x20 {
            continue; // drop control bytes / NULs
        }
        if c == b'\'' {
            if pos + 2 > escape_room {
                break;
            }
            sql[pos] = b'\'';
            pos += 1;
            sql[pos] = b'\'';
            pos += 1;
        } else {
            if pos + 1 > escape_room {
                break;
            }
            sql[pos] = c;
            pos += 1;
        }
    }
    for &b in SUFFIX {
        sql[pos] = b;
        pos += 1;
    }
    // Ignore the result: -1 on failure is handled by the count read below still running.
    let _ = unsafe { sql_execute(sql.as_ptr(), pos as i32) };

    // 3. Query the current row count from our OWN namespace.
    const QUERY: &[u8] = b"SELECT COUNT(*) FROM guestbook";
    let mut out = [0u8; 256];
    let qn = unsafe {
        sql_query(
            core::ptr::null(), // owner_ptr unused when owner_len == 0
            0,                 // own namespace
            QUERY.as_ptr(),
            QUERY.len() as i32,
            out.as_mut_ptr(),
            out.len() as i32,
        )
    };
    let out_len = if qn < 0 {
        0
    } else {
        (qn as usize).min(out.len())
    };

    // 4. Commit the returned count text as the app's output bytes.
    unsafe {
        commit(out.as_ptr(), out_len as i32);
    }
}
