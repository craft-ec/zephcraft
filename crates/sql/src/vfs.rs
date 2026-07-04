//! SQLite VFS backed by CraftOBJ content-addressed pages (foundation §33).
//!
//! `xRead`/`xWrite` map byte offsets onto `Pager` pages; `xSync` commits the
//! buffered pages to objects and produces a new immutable ROOT CID, stored in a
//! shared map (the in-memory stand-in for the signed `KIND_ROOT` head). The
//! rollback journal is served from an in-RAM handle (never persisted — commits
//! are atomic via the root CID, so a hot journal never needs recovery).

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlite_vfs::{DatabaseHandle, LockKind, OpenKind, OpenOptions, Vfs, WalDisabled};
use zeph_core::Cid;

use crate::{ObjectStore, Pager, PAGE_SIZE};

/// db name → current root CID (in-memory here; the real head is a signed
/// `KIND_ROOT` record published via routing).
pub type Roots = Arc<Mutex<HashMap<String, [u8; 32]>>>;
/// Per-db lazy-read fetchers: a reader registers one so the sync VFS can pull
/// page contents on demand instead of syncing the whole DB up front.
pub type Fetchers = Arc<Mutex<HashMap<String, crate::fetch::Fetcher>>>;
/// Per-db page cipher: (DEK, serialized wrapped-DEK). Registered by `open_as` for
/// a PRIVATE db; the VFS applies it to the pager (encrypt pages) — the VFS itself
/// stays crypto-agnostic. The wrapped-DEK is written into the root on commit.
pub type Ciphers = Arc<Mutex<HashMap<String, (zeph_cipher::Dek, Vec<u8>)>>>;

/// A SQLite VFS whose database files are CraftOBJ page objects.
pub struct CraftVfs {
    dir: PathBuf,
    roots: Roots,
    fetchers: Fetchers,
    ciphers: Ciphers,
}

impl CraftVfs {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            roots: Arc::new(Mutex::new(HashMap::new())),
            fetchers: Arc::new(Mutex::new(HashMap::new())),
            ciphers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Handle to the shared root map — lets callers read the committed root CID
    /// for a db (and, later, publish it as `KIND_ROOT`).
    pub fn roots(&self) -> Roots {
        self.roots.clone()
    }

    /// Handle to the per-db lazy-read fetcher map.
    pub fn fetchers(&self) -> Fetchers {
        self.fetchers.clone()
    }

    /// Handle to the per-db page-cipher map (private dbs).
    pub fn ciphers(&self) -> Ciphers {
        self.ciphers.clone()
    }

    /// Current committed root CID for `name`, if any.
    pub fn root(&self, name: &str) -> Option<Cid> {
        self.roots.lock().expect("roots").get(name).map(|b| Cid(*b))
    }
}

pub struct CraftHandle {
    inner: Inner,
    lock: LockKind,
}

// The `Db` variant is the common, long-lived case (the open database); boxing it
// would add indirection to page-heavy operations for no real memory win.
#[allow(clippy::large_enum_variant)]
enum Inner {
    /// The main database — persisted as CraftOBJ pages under a root CID.
    Db {
        pager: Pager,
        roots: Roots,
        name: String,
    },
    /// Journal/temp files — ephemeral, in RAM.
    Mem { data: Vec<u8> },
}

impl Vfs for CraftVfs {
    type Handle = CraftHandle;

    fn open(&self, db: &str, opts: OpenOptions) -> io::Result<CraftHandle> {
        if opts.kind == OpenKind::MainDb {
            let store = ObjectStore::open(&self.dir)?;
            let roots = self.roots.clone();
            let mut pager = match roots.lock().expect("roots").get(db).copied() {
                Some(root) => Pager::open(store, Cid(root)).map_err(to_io)?,
                None => Pager::create(store),
            };
            // Lazy reader: pull page contents on demand via the registered fetcher.
            if let Some(f) = self.fetchers.lock().expect("fetchers").get(db) {
                pager.set_remote(f.clone());
            }
            // Private db: apply the page cipher + wrapped-DEK registered by open_as.
            if let Some((dek, wrapped)) = self.ciphers.lock().expect("ciphers").get(db) {
                pager.set_cipher(dek.clone());
                pager.set_wrapped_dek(wrapped.clone());
            }
            Ok(CraftHandle {
                inner: Inner::Db {
                    pager,
                    roots,
                    name: db.to_string(),
                },
                lock: LockKind::None,
            })
        } else {
            Ok(CraftHandle {
                inner: Inner::Mem { data: Vec::new() },
                lock: LockKind::None,
            })
        }
    }

    fn delete(&self, db: &str) -> io::Result<()> {
        self.roots.lock().expect("roots").remove(db);
        Ok(())
    }

    fn exists(&self, db: &str) -> io::Result<bool> {
        Ok(self.roots.lock().expect("roots").contains_key(db))
    }

    fn temporary_name(&self) -> String {
        "craft-temp".to_string()
    }

    fn random(&self, buffer: &mut [i8]) {
        for (i, b) in buffer.iter_mut().enumerate() {
            *b = (i as i8).wrapping_mul(31) ^ 0x5a;
        }
    }

    fn sleep(&self, duration: Duration) -> Duration {
        duration
    }
}

impl DatabaseHandle for CraftHandle {
    type WalIndex = WalDisabled;

    fn size(&self) -> io::Result<u64> {
        Ok(match &self.inner {
            Inner::Db { pager, .. } => pager.page_count() as u64 * PAGE_SIZE as u64,
            Inner::Mem { data } => data.len() as u64,
        })
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        match &mut self.inner {
            Inner::Db { pager, .. } => pager.read_at(buf, offset),
            Inner::Mem { data } => {
                let off = offset as usize;
                for (i, b) in buf.iter_mut().enumerate() {
                    *b = data.get(off + i).copied().unwrap_or(0);
                }
            }
        }
        Ok(())
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        match &mut self.inner {
            Inner::Db { pager, .. } => pager.write_at(buf, offset),
            Inner::Mem { data } => {
                let end = offset as usize + buf.len();
                if data.len() < end {
                    data.resize(end, 0);
                }
                data[offset as usize..end].copy_from_slice(buf);
            }
        }
        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> io::Result<()> {
        if let Inner::Db { pager, roots, name } = &mut self.inner {
            let root = pager.commit().map_err(to_io)?;
            roots.lock().expect("roots").insert(name.clone(), root.0);
        }
        Ok(())
    }

    fn set_len(&mut self, size: u64) -> io::Result<()> {
        match &mut self.inner {
            Inner::Db { pager, .. } => {
                pager.set_page_count((size as usize).div_ceil(PAGE_SIZE) as u32)
            }
            Inner::Mem { data } => data.resize(size as usize, 0),
        }
        Ok(())
    }

    fn lock(&mut self, lock: LockKind) -> io::Result<bool> {
        self.lock = lock;
        Ok(true)
    }

    fn unlock(&mut self, lock: LockKind) -> io::Result<bool> {
        self.lock = lock;
        Ok(true)
    }

    fn reserved(&mut self) -> io::Result<bool> {
        Ok(false)
    }

    fn current_lock(&self) -> io::Result<LockKind> {
        Ok(self.lock)
    }

    fn wal_index(&self, _readonly: bool) -> io::Result<WalDisabled> {
        Ok(WalDisabled)
    }
}

fn to_io(e: crate::SqlError) -> io::Error {
    io::Error::other(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn sql_persists_to_pages_and_reopens_from_root() {
        let dir = tempdir().unwrap();
        let vfs = CraftVfs::new(dir.path());
        let roots = vfs.roots();
        sqlite_vfs::register("craft-test", vfs, false).unwrap();

        // Write a real table over the CraftOBJ-backed VFS.
        {
            let conn = rusqlite::Connection::open_with_flags_and_vfs(
                "acct.db",
                rusqlite::OpenFlags::default(),
                "craft-test",
            )
            .unwrap();
            conn.execute_batch(
                "PRAGMA page_size=16384; PRAGMA synchronous=FULL;
                 CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
                 INSERT INTO users(id,name) VALUES (1,'alice'),(2,'bob'),(3,'carol');",
            )
            .unwrap();
        }

        // A root CID was committed (the KIND_ROOT head).
        let root = roots.lock().unwrap().get("acct.db").copied();
        assert!(root.is_some(), "commit produced a root CID");

        // A FRESH connection to the same db loads from that root and reads back.
        let conn2 = rusqlite::Connection::open_with_flags_and_vfs(
            "acct.db",
            rusqlite::OpenFlags::default(),
            "craft-test",
        )
        .unwrap();
        let name: String = conn2
            .query_row("SELECT name FROM users WHERE id=2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            name, "bob",
            "SQL round-trips through content-addressed pages"
        );
        let count: i64 = conn2
            .query_row("SELECT count(*) FROM users", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 3);
    }
}
