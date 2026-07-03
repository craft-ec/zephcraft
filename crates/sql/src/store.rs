use std::fs;
use std::io::Write;
use std::path::PathBuf;

use zeph_core::Cid;

/// Content-addressed blob store for CraftSQL page objects: `put(bytes) → Cid`,
/// `get(cid) → bytes`. Sharded 256-ways by hex prefix; idempotent (same bytes →
/// same path, written once). Kept SEPARATE from the erasure-coded content store
/// so DB pages aren't swept into the demand-driven lifecycle (CRAFTOBJ_DESIGN
/// "Provider granularity" — DB pages are a distinct object class).
pub struct ObjectStore {
    root: PathBuf,
}

impl ObjectStore {
    pub fn open(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = dir.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn path(&self, cid: &Cid) -> PathBuf {
        let hex = cid.to_hex();
        self.root.join(&hex[0..2]).join(&hex)
    }

    /// Store `data`, returning its CID. Idempotent — identical bytes dedup.
    pub fn put(&self, data: &[u8]) -> std::io::Result<Cid> {
        let cid = Cid::of(data);
        let path = self.path(&cid);
        if path.exists() {
            return Ok(cid);
        }
        let dir = path.parent().expect("cid path has a shard parent");
        fs::create_dir_all(dir)?;
        let tmp = dir.join(format!(".{}.tmp", cid.to_hex()));
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(data)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        Ok(cid)
    }

    /// Fetch the object for `cid` (self-verifying: its path is its hash).
    pub fn get(&self, cid: &Cid) -> Option<Vec<u8>> {
        fs::read(self.path(cid)).ok()
    }

    pub fn has(&self, cid: &Cid) -> bool {
        self.path(cid).exists()
    }

    /// Every object CID currently held — for compaction (GC of superseded pages).
    pub fn list(&self) -> std::io::Result<Vec<Cid>> {
        let mut out = Vec::new();
        for shard in fs::read_dir(&self.root)? {
            let shard = shard?.path();
            if !shard.is_dir() {
                continue; // e.g. the `<key>.gens` sidecar files at the root
            }
            for entry in fs::read_dir(&shard)? {
                let name = entry?.file_name();
                if let Some(cid) = name.to_str().and_then(cid_from_hex) {
                    out.push(cid);
                }
            }
        }
        Ok(out)
    }

    /// Remove an object (idempotent — missing is fine). Used by compaction to
    /// reclaim page versions no longer reachable from the current root.
    pub fn delete(&self, cid: &Cid) -> std::io::Result<()> {
        match fs::remove_file(self.path(cid)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

fn cid_from_hex(s: &str) -> Option<Cid> {
    if s.len() != 64 {
        return None; // skips `.tmp` files and non-object names
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(Cid(out))
}
