use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use zeph_core::Cid;

use crate::{ObjectStore, Result, SqlError};

/// SQLite page size (foundation §33: PRAGMA page_size = 16384).
pub const PAGE_SIZE: usize = 16 * 1024;

/// Tree fanout — 256 children per node, keyed by one byte of the page number.
const FANOUT: u64 = 256;

/// A tree node: sparse `child_index (0..256) → CID`. At depth 0 (leaf) the CIDs
/// are page objects; above, they are child-node objects. Postcard-serialized.
pub(crate) type NodeMap = BTreeMap<u8, [u8; 32]>;

/// The root object — a tiny header pointing at the index tree. Its CID is the DB
/// ROOT (the `KIND_ROOT` head). A commit rewrites only the tree nodes on the
/// path to changed pages, so unchanged subtrees keep their CIDs (dedup), and the
/// index cost is O(changed · depth), not O(total pages).
#[derive(Serialize, Deserialize)]
pub(crate) struct RootIndex {
    pub page_count: u32,
    /// Tree depth (0 = empty DB; else levels of nodes above the page objects).
    pub depth: u8,
    /// CID of the top node (meaningless if depth == 0).
    pub root_cid: [u8; 32],
}

pub(crate) fn decode_root(bytes: &[u8]) -> Result<RootIndex> {
    postcard::from_bytes(bytes).map_err(|e| SqlError::CorruptIndex(e.to_string()))
}

pub(crate) fn decode_node(bytes: &[u8]) -> Result<NodeMap> {
    postcard::from_bytes(bytes).map_err(|e| SqlError::CorruptIndex(e.to_string()))
}

/// Depth needed to hold `page_count` pages (1 level per 256×).
fn depth_for(page_count: u32) -> u8 {
    if page_count == 0 {
        return 0;
    }
    let mut d = 1u8;
    let mut cap = FANOUT;
    while cap < page_count as u64 {
        cap *= FANOUT;
        d += 1;
    }
    d
}

/// The CID-VFS storage core: page objects addressed by a fanout-256 radix tree
/// over page numbers. Reads use an in-memory flat map (fast); commits rewrite
/// only the dirty path through the tree (incremental).
pub struct Pager {
    store: ObjectStore,
    /// page# → page object CID (flat, for O(1) reads).
    pages: BTreeMap<u32, [u8; 32]>,
    /// buffered writes (flushed on commit — SQLite's xWrite).
    dirty: HashMap<u32, Vec<u8>>,
    /// page numbers whose index entry changed since the last commit.
    dirty_index: BTreeSet<u32>,
    /// (level, node_key) → committed node CID — lets a commit reuse unchanged
    /// subtrees instead of rebuilding them.
    node_cids: HashMap<(u8, u32), [u8; 32]>,
    page_count: u32,
    depth: u8,
}

impl Pager {
    pub fn create(store: ObjectStore) -> Self {
        Self {
            store,
            pages: BTreeMap::new(),
            dirty: HashMap::new(),
            dirty_index: BTreeSet::new(),
            node_cids: HashMap::new(),
            page_count: 0,
            depth: 0,
        }
    }

    /// Open the snapshot at `root`, materializing the page map + node-CID cache.
    pub fn open(store: ObjectStore, root: Cid) -> Result<Self> {
        let bytes = store
            .get(&root)
            .ok_or_else(|| SqlError::RootNotFound(root.to_hex()))?;
        let ri = decode_root(&bytes)?;
        let mut pages = BTreeMap::new();
        let mut node_cids = HashMap::new();
        if ri.depth > 0 {
            load_subtree(
                &store,
                ri.depth - 1,
                0,
                Cid(ri.root_cid),
                &mut pages,
                &mut node_cids,
            )?;
        }
        Ok(Self {
            store,
            pages,
            dirty: HashMap::new(),
            dirty_index: BTreeSet::new(),
            node_cids,
            page_count: ri.page_count,
            depth: ri.depth,
        })
    }

    pub fn page_count(&self) -> u32 {
        self.page_count
    }

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

    pub fn write_page(&mut self, n: u32, data: Vec<u8>) {
        self.dirty.insert(n, data);
        self.dirty_index.insert(n);
        if n + 1 > self.page_count {
            self.page_count = n + 1;
        }
    }

    pub fn set_page_count(&mut self, pages: u32) {
        let drop: Vec<u32> = self.pages.range(pages..).map(|(k, _)| *k).collect();
        for p in drop {
            self.pages.remove(&p);
            self.dirty_index.insert(p);
        }
        self.dirty.retain(|&n, _| n < pages);
        self.page_count = pages;
    }

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

    /// Flush buffered pages, rewrite the dirty tree path, return the new root
    /// CID — SQLite's xSync commit trigger. Incremental: only nodes on the path
    /// to a changed page are re-stored.
    pub fn commit(&mut self) -> Result<Cid> {
        for (n, data) in std::mem::take(&mut self.dirty) {
            let cid = self.store.put(&data)?;
            self.pages.insert(n, cid.0);
        }
        let new_depth = depth_for(self.page_count);
        let changed: BTreeSet<u32> = if new_depth != self.depth {
            // Crossing a 256^d boundary restructures node keys → rebuild all.
            self.depth = new_depth;
            self.node_cids.clear();
            self.pages.keys().copied().collect()
        } else {
            std::mem::take(&mut self.dirty_index)
        };
        self.dirty_index.clear();
        self.rebuild(changed)?;

        let root_cid = if self.depth == 0 {
            [0u8; 32]
        } else {
            *self
                .node_cids
                .get(&(self.depth - 1, 0))
                .unwrap_or(&[0u8; 32])
        };
        let ri = RootIndex {
            page_count: self.page_count,
            depth: self.depth,
            root_cid,
        };
        let blob = postcard::to_allocvec(&ri).map_err(|e| SqlError::Serde(e.to_string()))?;
        Ok(self.store.put(&blob)?)
    }

    fn rebuild(&mut self, changed: BTreeSet<u32>) -> Result<()> {
        if self.depth == 0 {
            self.node_cids.clear();
            return Ok(());
        }
        let mut dirty_keys: BTreeSet<u32> = changed
            .iter()
            .map(|p| (*p as u64 / FANOUT) as u32)
            .collect();
        for level in 0..self.depth {
            let mut next = BTreeSet::new();
            for &nk in &dirty_keys {
                let mut children: NodeMap = BTreeMap::new();
                for i in 0u64..FANOUT {
                    let child_key = (nk as u64 * FANOUT + i) as u32;
                    let cid = if level == 0 {
                        self.pages.get(&child_key).copied()
                    } else {
                        self.node_cids.get(&(level - 1, child_key)).copied()
                    };
                    if let Some(c) = cid {
                        children.insert(i as u8, c);
                    }
                }
                if children.is_empty() {
                    self.node_cids.remove(&(level, nk));
                } else {
                    let blob = postcard::to_allocvec(&children)
                        .map_err(|e| SqlError::Serde(e.to_string()))?;
                    self.node_cids.insert((level, nk), self.store.put(&blob)?.0);
                }
                next.insert((nk as u64 / FANOUT) as u32);
            }
            dirty_keys = next;
        }
        Ok(())
    }
}

fn load_subtree(
    store: &ObjectStore,
    level: u8,
    node_key: u32,
    node_cid: Cid,
    pages: &mut BTreeMap<u32, [u8; 32]>,
    node_cids: &mut HashMap<(u8, u32), [u8; 32]>,
) -> Result<()> {
    node_cids.insert((level, node_key), node_cid.0);
    let bytes = store
        .get(&node_cid)
        .ok_or_else(|| SqlError::CorruptIndex(format!("missing node {}", node_cid.to_hex())))?;
    for (idx, child) in decode_node(&bytes)? {
        let child_key = (node_key as u64 * FANOUT + idx as u64) as u32;
        if level == 0 {
            pages.insert(child_key, child);
        } else {
            load_subtree(store, level - 1, child_key, Cid(child), pages, node_cids)?;
        }
    }
    Ok(())
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
    fn store_at(dir: &std::path::Path) -> ObjectStore {
        ObjectStore::open(dir).unwrap()
    }

    #[test]
    fn commit_reopen_versions_and_dedup() {
        let dir = tempdir().unwrap();
        let mut p = Pager::create(store_at(dir.path()));
        p.write_page(0, page(1));
        p.write_page(1, page(2));
        let root1 = p.commit().unwrap();
        p.write_page(1, page(9));
        let root2 = p.commit().unwrap();
        assert_ne!(root1, root2);

        let old = Pager::open(store_at(dir.path()), root1).unwrap();
        assert_eq!(old.read_page(1), page(2));
        assert_eq!(old.read_page(0), page(1));
        let new = Pager::open(store_at(dir.path()), root2).unwrap();
        assert_eq!(new.read_page(1), page(9));
        assert_eq!(old.pages[&0], new.pages[&0], "unchanged page shared");
        assert_ne!(old.pages[&1], new.pages[&1], "changed page differs");
    }

    #[test]
    fn unknown_root_errors_and_unwritten_page_is_zero() {
        let dir = tempdir().unwrap();
        assert!(Pager::open(store_at(dir.path()), Cid::of(b"nope")).is_err());
        let p = Pager::create(store_at(dir.path()));
        assert_eq!(p.read_page(5), vec![0u8; PAGE_SIZE]);
    }

    #[test]
    fn tree_index_scales_and_stays_incremental() {
        let dir = tempdir().unwrap();
        let mut p = Pager::create(store_at(dir.path()));
        // 300 pages forces a depth-2 tree (>256).
        for i in 0..300u32 {
            p.write_page(i, page((i % 251) as u8));
        }
        let root1 = p.commit().unwrap();
        assert_eq!(p.page_count(), 300);

        // Reopen from the tree root → every page reads back.
        let p1 = Pager::open(store_at(dir.path()), root1).unwrap();
        for i in 0..300u32 {
            assert_eq!(p1.read_page(i), page((i % 251) as u8), "page {i}");
        }

        // Change ONE page deep in the tree, commit → new root.
        let mut p2 = Pager::open(store_at(dir.path()), root1).unwrap();
        p2.write_page(150, page(200));
        let root2 = p2.commit().unwrap();
        assert_ne!(root1, root2);

        // Old snapshot intact; new snapshot has the change; the OTHER leaf's node
        // is shared (incremental — only the path to page 150 was rewritten).
        let old = Pager::open(store_at(dir.path()), root1).unwrap();
        let new = Pager::open(store_at(dir.path()), root2).unwrap();
        assert_eq!(old.read_page(150), page(150));
        assert_eq!(new.read_page(150), page(200));
        assert_eq!(old.pages[&0], new.pages[&0], "untouched page shared");
        assert_ne!(old.pages[&150], new.pages[&150]);
        // Leaf chunk 1 (pages 256..300) untouched → its whole subtree is reused;
        // only the leaf holding page 150 and the root were rewritten (incremental).
        assert_eq!(
            old.node_cids[&(0, 1)],
            new.node_cids[&(0, 1)],
            "untouched subtree shared"
        );
        assert_ne!(
            old.node_cids[&(0, 0)],
            new.node_cids[&(0, 0)],
            "touched leaf rewritten"
        );
        assert_ne!(
            old.node_cids[&(1, 0)],
            new.node_cids[&(1, 0)],
            "root rewritten"
        );
    }
}
