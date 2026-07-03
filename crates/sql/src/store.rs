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
}
