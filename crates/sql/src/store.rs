use std::fs;
use std::io::Write;
use std::path::PathBuf;

use zeph_core::Cid;

/// Content-addressed blob store for CraftSQL page objects: `put(bytes) → Cid`,
/// `get(cid) → bytes`. Sharded 256-ways by hex prefix; idempotent (same bytes →
/// same path, written once). Kept SEPARATE from the erasure-coded content store
/// so DB pages aren't swept into the demand-driven lifecycle (CRAFTOBJ_DESIGN
/// "Provider granularity" — DB pages are a distinct object class).
///
/// PER-DB ISOLATION: each CraftSQL database gets its OWN `root` (`store_dir/<key>/`)
/// so that its lifecycle — in particular compaction's GC, which deletes objects no
/// longer reachable from the DB's root — can NEVER touch another DB's objects. All
/// DBs used to share one flat `root`, but the store is content-addressed *and*
/// same-schema DBs (e.g. the 256 registry shards) dedup their identical initial
/// pages into one object; a per-DB GC over that shared store deleted pages other
/// DBs still referenced, corrupting them ("file is not a database"). Isolation
/// removes that whole class of bug (writes/GC are physically scoped to one DB).
///
/// `fallback` is a READ-ONLY legacy root: nodes that wrote pages under the old flat
/// layout keep resolving after upgrade because `get`/`has` fall through to it. New
/// writes and all deletes stay in the per-DB `root`, so the fallback is never
/// mutated — legacy objects linger harmlessly and are never GC'd across DBs.
pub struct ObjectStore {
    root: PathBuf,
    fallback: Option<PathBuf>,
}

impl ObjectStore {
    pub fn open(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = dir.into();
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            fallback: None,
        })
    }

    /// Open a per-DB store rooted at `dir`, with reads falling back to the legacy
    /// flat `fallback` root (never written or deleted). See the struct docs.
    pub fn open_with_fallback(
        dir: impl Into<PathBuf>,
        fallback: impl Into<PathBuf>,
    ) -> std::io::Result<Self> {
        let root = dir.into();
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            fallback: Some(fallback.into()),
        })
    }

    /// Open a per-DB store at `base/<component>/` — VALIDATING that `component` (the DB key) is a
    /// single safe path component first, so a caller-chosen namespace can't traverse out of `base`
    /// via `/`, `\`, `..`, etc. This is the mechanism-level guard: every per-DB store open (the VFS
    /// and `open_db_store`) goes through here, and it is the only place that turns a caller-supplied
    /// name into a `create_dir_all` root. Reads fall back to the legacy flat `fallback` root.
    pub fn open_db_component(
        base: impl Into<PathBuf>,
        component: &str,
        fallback: impl Into<PathBuf>,
    ) -> std::io::Result<Self> {
        if !is_safe_component(component) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unsafe db namespace/key: {component:?}"),
            ));
        }
        Self::open_with_fallback(base.into().join(component), fallback)
    }

    fn path(&self, cid: &Cid) -> PathBuf {
        let hex = cid.to_hex();
        self.root.join(&hex[0..2]).join(&hex)
    }

    fn fallback_path(&self, cid: &Cid) -> Option<PathBuf> {
        let hex = cid.to_hex();
        self.fallback.as_ref().map(|f| f.join(&hex[0..2]).join(hex))
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

    /// Fetch the object for `cid` (self-verifying: its path is its hash). Reads the
    /// per-DB root first, then the read-only legacy fallback root (see struct docs).
    pub fn get(&self, cid: &Cid) -> Option<Vec<u8>> {
        if let Ok(b) = fs::read(self.path(cid)) {
            return Some(b);
        }
        self.fallback_path(cid).and_then(|p| fs::read(p).ok())
    }

    pub fn has(&self, cid: &Cid) -> bool {
        self.path(cid).exists() || self.fallback_path(cid).map(|p| p.exists()).unwrap_or(false)
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

/// True iff `s` is a single safe path component: non-empty, no `/` or `\` separators, and not `.`
/// or `..` or an absolute/prefix component. A DB key becomes a directory (`store_dir/<key>/`), so
/// anything else would let a caller-chosen namespace escape the store dir (path traversal). Rejects
/// `\` explicitly too, so a name that traverses on Windows can't slip through a unix build.
pub(crate) fn is_safe_component(s: &str) -> bool {
    use std::path::Component;
    if s.is_empty() || s.contains('/') || s.contains('\\') {
        return false;
    }
    let mut comps = std::path::Path::new(s).components();
    matches!(
        (comps.next(), comps.next()),
        (Some(Component::Normal(c)), None) if c == std::ffi::OsStr::new(s)
    )
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
