use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use zeph_core::Cid;

use crate::{ObjectStore, Result, SqlError};

/// SQLite page size (foundation §33: PRAGMA page_size = 16384).
pub const PAGE_SIZE: usize = 16 * 1024;

/// The root object: the full page map + page count. Its CID is the DB ROOT —
/// the mutable head `KIND_ROOT` publishes. Serialized with postcard.
#[derive(Serialize, Deserialize)]
struct RootIndex {
    page_count: u32,
    /// page number → object CID
    pages: BTreeMap<u32, [u8; 32]>,
}

/// The CID-VFS storage core: maps page numbers to content-addressed objects,
/// buffers writes (like SQLite's xWrite), and commits to an immutable root CID
/// (xSync). Unchanged pages keep their CID across commits (dedup).
pub struct Pager {
    store: ObjectStore,
    pages: BTreeMap<u32, [u8; 32]>,
    dirty: HashMap<u32, Vec<u8>>,
    page_count: u32,
}

impl Pager {
    /// A fresh, empty database.
    pub fn create(store: ObjectStore) -> Self {
        Self {
            store,
            pages: BTreeMap::new(),
            dirty: HashMap::new(),
            page_count: 0,
        }
    }

    /// Open the snapshot identified by `root` (a prior commit's CID).
    pub fn open(store: ObjectStore, root: Cid) -> Result<Self> {
        let bytes = store
            .get(&root)
            .ok_or_else(|| SqlError::RootNotFound(root.to_hex()))?;
        let idx: RootIndex =
            postcard::from_bytes(&bytes).map_err(|e| SqlError::CorruptIndex(e.to_string()))?;
        Ok(Self {
            store,
            pages: idx.pages,
            dirty: HashMap::new(),
            page_count: idx.page_count,
        })
    }

    pub fn page_count(&self) -> u32 {
        self.page_count
    }

    /// Read page `n` (always PAGE_SIZE bytes): newest buffered write, else the
    /// committed object, else a zero page (SQLite tolerates short/zero reads).
    pub fn read_page(&self, n: u32) -> Vec<u8> {
        if let Some(d) = self.dirty.get(&n) {
            return pad(d);
        }
        if let Some(cid) = self.pages.get(&n) {
            if let Some(data) = self.store.get(&Cid(*cid)) {
                return pad(&data);
            }
        }
        vec![0u8; PAGE_SIZE]
    }

    /// Buffer a write to page `n` (flushed on commit — SQLite's xWrite).
    pub fn write_page(&mut self, n: u32, data: Vec<u8>) {
        self.dirty.insert(n, data);
        if n + 1 > self.page_count {
            self.page_count = n + 1;
        }
    }

    /// Truncate to `pages` pages (SQLite xTruncate).
    pub fn set_page_count(&mut self, pages: u32) {
        self.page_count = pages;
        self.pages.retain(|&n, _| n < pages);
        self.dirty.retain(|&n, _| n < pages);
    }

    /// Byte-addressable read (spans/sub-divides pages) — for the VFS's
    /// `read_exact_at`. Fills `buf` from the pages covering `[offset, +len)`.
    pub fn read_at(&self, buf: &mut [u8], offset: u64) {
        let mut done = 0;
        while done < buf.len() {
            let pos = offset as usize + done;
            let page = (pos / PAGE_SIZE) as u32;
            let within = pos % PAGE_SIZE;
            let pdata = self.read_page(page);
            let n = (PAGE_SIZE - within).min(buf.len() - done);
            buf[done..done + n].copy_from_slice(&pdata[within..within + n]);
            done += n;
        }
    }

    /// Byte-addressable write (read-modify-write of covering pages) — for the
    /// VFS's `write_all_at`. Buffered until commit.
    pub fn write_at(&mut self, buf: &[u8], offset: u64) {
        let mut done = 0;
        while done < buf.len() {
            let pos = offset as usize + done;
            let page = (pos / PAGE_SIZE) as u32;
            let within = pos % PAGE_SIZE;
            let mut pdata = self.read_page(page);
            let n = (PAGE_SIZE - within).min(buf.len() - done);
            pdata[within..within + n].copy_from_slice(&buf[done..done + n]);
            self.write_page(page, pdata);
            done += n;
        }
    }

    /// Flush buffered pages to objects, build a new root index, return its CID —
    /// SQLite's xSync commit trigger.
    pub fn commit(&mut self) -> Result<Cid> {
        for (n, data) in self.dirty.drain() {
            let cid = self.store.put(&data)?;
            self.pages.insert(n, cid.0);
        }
        let idx = RootIndex {
            page_count: self.page_count,
            pages: self.pages.clone(),
        };
        let blob = postcard::to_allocvec(&idx).map_err(|e| SqlError::Serde(e.to_string()))?;
        Ok(self.store.put(&blob)?)
    }
}

fn pad(data: &[u8]) -> Vec<u8> {
    let mut v = data.to_vec();
    v.resize(PAGE_SIZE, 0);
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn page(fill: u8) -> Vec<u8> {
        vec![fill; PAGE_SIZE]
    }

    #[test]
    fn commit_reopen_versions_and_dedup() {
        let dir = tempdir().unwrap();
        let mut p = Pager::create(ObjectStore::open(dir.path()).unwrap());
        p.write_page(0, page(1));
        p.write_page(1, page(2));
        let root1 = p.commit().unwrap();
        assert_eq!(p.page_count(), 2);

        // change ONLY page 1, commit → a new root
        p.write_page(1, page(9));
        let root2 = p.commit().unwrap();
        assert_ne!(root1, root2, "a change yields a new root CID");

        // reopen the OLD snapshot → original page 1 (immutable history)
        let old = Pager::open(ObjectStore::open(dir.path()).unwrap(), root1).unwrap();
        assert_eq!(old.read_page(1), page(2), "old root is immutable");
        assert_eq!(old.read_page(0), page(1));

        // reopen the NEW snapshot → changed page 1, unchanged page 0
        let new = Pager::open(ObjectStore::open(dir.path()).unwrap(), root2).unwrap();
        assert_eq!(new.read_page(1), page(9));
        assert_eq!(new.read_page(0), page(1));

        // dedup: page 0 is the SAME object CID in both snapshots (not re-stored)
        assert_eq!(
            old.pages[&0], new.pages[&0],
            "unchanged page shared across roots"
        );
        assert_ne!(old.pages[&1], new.pages[&1], "changed page has a new CID");
    }

    #[test]
    fn unknown_root_errors_and_unwritten_page_is_zero() {
        let dir = tempdir().unwrap();
        assert!(
            Pager::open(ObjectStore::open(dir.path()).unwrap(), Cid::of(b"nope")).is_err(),
            "opening an unknown root fails loud"
        );
        let p = Pager::create(ObjectStore::open(dir.path()).unwrap());
        assert_eq!(
            p.read_page(5),
            vec![0u8; PAGE_SIZE],
            "unwritten page reads as zeros"
        );
    }
}
