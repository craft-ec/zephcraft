//! Persistent content-addressed store (foundation §27, §31–32) with pinning.
//!
//! Replaces the M1-exit in-memory demo store. Two ways to hold a CID:
//!  - **Pieces** — coded pieces for a generation (normal storage node role),
//!    subject to eviction under disk pressure.
//!  - **Pin** — the whole decoded content (~1×), exempt from eviction, serving
//!    coded pieces by encoding on demand (decision 2026-07-03). Uploaders pin
//!    by default; consumers may pin after fetch.
//!
//! A **tombstone set** (deletion, decision 2026-07-03) blocks a CID from being
//! (re-)stored so repair/distribution can't resurrect deleted content — the
//! lifecycle consults it from day one.
//!
//! On-disk layout (atomic temp→fsync→rename writes; 256-way sharding by CID):
//! ```text
//! <root>/cid/<hex[0:2]>/<hex>/meta            postcard Generation
//!                            /content         whole content (pinned only)
//!                            /pieces/<pid_hex> coded piece bytes
//! <root>/tombstones/<hex>                      deletion marker
//! ```

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use zeph_core::Cid;
use zeph_erasure::{encode, CodedPiece};

/// Per-CID generation metadata (what's needed to interpret/serve its pieces).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Generation {
    pub k: u32,
    pub piece_len: u64,
    pub total_len: u64,
    /// postcard-encoded zeph_erasure::vtags::VTags.
    pub vtags: Vec<u8>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct StoreStats {
    pub cids: usize,
    pub pieces: usize,
    pub pinned: usize,
    pub bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization: {0}")]
    Serde(String),
    #[error("cid {0} is tombstoned (deleted); refusing to store")]
    Tombstoned(String),
    #[error("no generation metadata for cid {0}")]
    NoGeneration(String),
    #[error("erasure: {0}")]
    Erasure(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;

struct CidState {
    gen: Generation,
    piece_ids: HashSet<[u8; 32]>,
    pinned: bool,
    /// System object (a CraftSQL page generation): managed by the DB layer, not
    /// the user — excluded from user pin/unpin/delete/want and from eviction.
    system: bool,
    /// Bookkeeping ABOUT this store, so it must never be counted as part of what the store holds.
    ///
    /// The holdings manifest is what this exists for. A manifest that listed itself would change the set
    /// it describes every time it published — manufacturing the very churn it reports, forever. Measured
    /// on the live fleet before this flag: +3-4 cids/min/node on an IDLE fleet, versus a provably flat
    /// store with manifests off.
    not_holdings: bool,
    has_content: bool,
    last_access: u64,
}

impl CidState {
    fn bytes(&self) -> u64 {
        let piece_bytes = self.piece_ids.len() as u64 * self.gen.piece_len;
        let content_bytes = if self.has_content {
            self.gen.total_len
        } else {
            0
        };
        piece_bytes + content_bytes
    }
}

pub struct Store {
    root: PathBuf,
    index: Mutex<HashMap<Cid, CidState>>,
    tombstones: Mutex<HashSet<Cid>>,
    /// WANT interest markers — CIDs this node wants kept alive (may not hold).
    wanted: Mutex<HashSet<Cid>>,
    /// Content whose erasure set has been successfully pushed to peers — publish
    /// stops re-pushing it (idempotent re-announce), so re-publishes don't grow pieces.
    distributed: Mutex<HashSet<Cid>>,
    /// Eviction cooldown — CIDs recently evicted (cid → unix secs). While in
    /// cooldown the lifecycle won't refill; the record is purged after the TTL.
    evicted: Mutex<HashMap<Cid, u64>>,
}

impl Store {
    /// Open (or create) a store rooted at `root`, rebuilding the in-memory
    /// index from disk.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join("cid"))?;
        fs::create_dir_all(root.join("tombstones"))?;
        fs::create_dir_all(root.join("wanted"))?;
        fs::create_dir_all(root.join("distributed"))?;
        fs::create_dir_all(root.join("evicted"))?;
        let store = Self {
            root,
            index: Mutex::new(HashMap::new()),
            tombstones: Mutex::new(HashSet::new()),
            wanted: Mutex::new(HashSet::new()),
            distributed: Mutex::new(HashSet::new()),
            evicted: Mutex::new(HashMap::new()),
        };
        store.reload()?;
        Ok(store)
    }

    fn reload(&self) -> Result<()> {
        let mut index = self.index.lock().expect("index");
        let mut tombstones = self.tombstones.lock().expect("tombstones");
        index.clear();
        tombstones.clear();

        let ts_dir = self.root.join("tombstones");
        if ts_dir.is_dir() {
            for entry in fs::read_dir(&ts_dir)? {
                if let Some(cid) = parse_cid(&entry?.file_name().to_string_lossy()) {
                    tombstones.insert(cid);
                }
            }
        }
        let mut wanted = self.wanted.lock().expect("wanted");
        wanted.clear();
        let w_dir = self.root.join("wanted");
        if w_dir.is_dir() {
            for entry in fs::read_dir(&w_dir)? {
                if let Some(cid) = parse_cid(&entry?.file_name().to_string_lossy()) {
                    wanted.insert(cid);
                }
            }
        }
        drop(wanted);
        let mut distributed = self.distributed.lock().expect("distributed");
        distributed.clear();
        let d_dir = self.root.join("distributed");
        if d_dir.is_dir() {
            for entry in fs::read_dir(&d_dir)? {
                if let Some(cid) = parse_cid(&entry?.file_name().to_string_lossy()) {
                    distributed.insert(cid);
                }
            }
        }
        drop(distributed);
        let mut evicted = self.evicted.lock().expect("evicted");
        evicted.clear();
        let e_dir = self.root.join("evicted");
        if e_dir.is_dir() {
            for entry in fs::read_dir(&e_dir)?.flatten() {
                if let Some(cid) = parse_cid(&entry.file_name().to_string_lossy()) {
                    let ts = fs::read_to_string(entry.path())
                        .ok()
                        .and_then(|s| s.trim().parse::<u64>().ok())
                        .unwrap_or(0);
                    evicted.insert(cid, ts);
                }
            }
        }
        drop(evicted);

        let cid_dir = self.root.join("cid");
        for shard in fs::read_dir(&cid_dir)? {
            let shard = shard?.path();
            if !shard.is_dir() {
                continue;
            }
            for entry in fs::read_dir(&shard)? {
                let dir = entry?.path();
                let Some(cid) = dir
                    .file_name()
                    .and_then(|n| parse_cid(&n.to_string_lossy()))
                else {
                    continue;
                };
                let Ok(meta_bytes) = fs::read(dir.join("meta")) else {
                    continue;
                };
                let Ok(gen) = postcard::from_bytes::<Generation>(&meta_bytes) else {
                    continue;
                };
                let mut piece_ids = HashSet::new();
                if let Ok(pieces) = fs::read_dir(dir.join("pieces")) {
                    for p in pieces.flatten() {
                        if let Some(pid) = parse_pid(&p.file_name().to_string_lossy()) {
                            piece_ids.insert(pid);
                        }
                    }
                }
                let has_content = dir.join("content").exists();
                let pinned = dir.join("pinned").exists();
                let system = dir.join("system").exists();
                let not_holdings = dir.join("not-holdings").exists();
                index.insert(
                    cid,
                    CidState {
                        gen,
                        piece_ids,
                        pinned,
                        system,
                        not_holdings,
                        has_content,
                        last_access: now(),
                    },
                );
            }
        }
        Ok(())
    }

    fn cid_dir(&self, cid: &Cid) -> PathBuf {
        let hex = cid.to_hex();
        self.root.join("cid").join(&hex[0..2]).join(&hex)
    }

    /// Record generation metadata for a CID (idempotent). Required before
    /// pieces or a pin can be stored.
    pub fn put_generation(&self, cid: Cid, gen: Generation) -> Result<()> {
        self.guard_tombstone(&cid)?;
        let dir = self.cid_dir(&cid);
        fs::create_dir_all(dir.join("pieces"))?;
        write_atomic(
            &dir.join("meta"),
            &postcard::to_allocvec(&gen).map_err(|e| StoreError::Serde(e.to_string()))?,
        )?;
        let mut index = self.index.lock().expect("index");
        index.entry(cid).or_insert_with(|| CidState {
            gen,
            piece_ids: HashSet::new(),
            pinned: false,
            system: false,
            not_holdings: false,
            has_content: false,
            last_access: now(),
        });
        Ok(())
    }

    /// Store one coded piece for a CID (its generation must already be set).
    pub fn put_piece(&self, cid: Cid, piece: &CodedPiece) -> Result<[u8; 32]> {
        self.guard_tombstone(&cid)?;
        let pid = piece.piece_id();
        let dir = self.cid_dir(&cid);
        let mut bytes = Vec::with_capacity(4 + piece.coding_vector.len() + piece.data.len());
        bytes.extend_from_slice(&(piece.coding_vector.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&piece.coding_vector);
        bytes.extend_from_slice(&piece.data);
        write_atomic(&dir.join("pieces").join(hex::encode(pid)), &bytes)?;
        let mut index = self.index.lock().expect("index");
        if let Some(state) = index.get_mut(&cid) {
            state.piece_ids.insert(pid);
            state.last_access = now();
        }
        Ok(pid)
    }

    /// Remove one specific coded piece. Used by Distribution's MOVE: the piece
    /// is deleted locally ONLY after the receiver has acked storing it, so a
    /// copy always exists somewhere — a move never loses data. Returns whether
    /// the piece was present.
    pub fn remove_piece(&self, cid: &Cid, pid: &[u8; 32]) -> Result<bool> {
        let path = self.cid_dir(cid).join("pieces").join(hex::encode(pid));
        let existed = path.exists();
        let _ = fs::remove_file(&path);
        let mut index = self.index.lock().expect("index");
        if let Some(state) = index.get_mut(cid) {
            state.piece_ids.remove(pid);
            state.last_access = now();
        }
        Ok(existed)
    }

    /// Store the whole decoded content for a CID. `pinned=true` exempts it
    /// from eviction (an explicit pin); `pinned=false` is a transient
    /// seed-mode consumer that holds the content but may be evicted.
    pub fn put_content(&self, cid: Cid, content: &[u8], pinned: bool) -> Result<()> {
        self.guard_tombstone(&cid)?;
        // A generation (meta file) MUST exist first: reload keys off `meta`, so
        // content written without it orphans on disk and is lost on reopen.
        // Fail loud rather than silently drop the data.
        if !self.index.lock().expect("index").contains_key(&cid) {
            return Err(StoreError::NoGeneration(cid.to_hex()));
        }
        let dir = self.cid_dir(&cid);
        fs::create_dir_all(dir.join("pieces"))?;
        write_atomic(&dir.join("content"), content)?;
        if pinned {
            write_atomic(&dir.join("pinned"), b"")?;
        }
        let mut index = self.index.lock().expect("index");
        if let Some(state) = index.get_mut(&cid) {
            state.pinned = state.pinned || pinned;
            state.has_content = true;
            state.last_access = now();
        }
        Ok(())
    }

    /// Pin a CID: store the whole decoded content, exempt from eviction.
    pub fn pin(&self, cid: Cid, content: &[u8]) -> Result<()> {
        self.put_content(cid, content, true)
    }

    /// Revert a pin to normal (evictable) lifecycle. Keeps the content file
    /// until eviction; just clears the exemption.
    pub fn unpin(&self, cid: &Cid) -> Result<()> {
        let _ = fs::remove_file(self.cid_dir(cid).join("pinned"));
        if let Some(state) = self.index.lock().expect("index").get_mut(cid) {
            state.pinned = false;
        }
        Ok(())
    }

    pub fn is_pinned(&self, cid: &Cid) -> bool {
        self.index
            .lock()
            .expect("index")
            .get(cid)
            .is_some_and(|s| s.pinned)
    }

    /// Mark a CID as a system object (a CraftSQL generation) — DB-managed,
    /// exempt from user lifecycle commands and from eviction.
    pub fn mark_system(&self, cid: &Cid) -> Result<()> {
        write_atomic(&self.cid_dir(cid).join("system"), b"")?;
        if let Some(state) = self.index.lock().expect("index").get_mut(cid) {
            state.system = true;
        }
        Ok(())
    }

    /// Release a system object back to the normal lifecycle (compaction dropping
    /// a superseded generation). Removes the exemption so it can fade/evict.
    pub fn unmark_system(&self, cid: &Cid) -> Result<()> {
        let _ = fs::remove_file(self.cid_dir(cid).join("system"));
        if let Some(state) = self.index.lock().expect("index").get_mut(cid) {
            state.system = false;
        }
        Ok(())
    }

    /// Mark a cid as bookkeeping ABOUT the store — never part of the holdings it reports.
    ///
    /// NB the marker file is `not-holdings`, NOT `meta`: `meta` is already the generation metadata, and
    /// writing an empty marker there erases it, dropping the object from the index on the next reopen.
    ///
    /// Persisted as a marker file so it survives a restart — otherwise a restarted node would re-adopt
    /// its own old manifests as holdings and republish them as if they were data.
    pub fn mark_not_holdings(&self, cid: &Cid) -> Result<()> {
        write_atomic(&self.cid_dir(cid).join("not-holdings"), b"")?;
        if let Some(state) = self.index.lock().expect("index").get_mut(cid) {
            state.not_holdings = true;
        }
        Ok(())
    }

    /// The real CONTENT this node holds — everything except bookkeeping ABOUT this store.
    ///
    /// This is the set that both matters and must not include itself: it is what we report as holdings,
    /// and what the fleet is expected to keep durable. Bookkeeping is neither. It is local-only by
    /// construction (one copy, ours), so scanning it finds it "at risk" against the durability floor and
    /// distributes it — which puts it in every peer's store, changes THEIR holdings, and makes them
    /// republish. That is the same loop as counting it in our own holdings, entered from the other end.
    ///
    /// `cids()` still reports everything physically held; only this VIEW excludes the self-description.
    pub fn content_cids(&self) -> Vec<Cid> {
        self.index
            .lock()
            .expect("index")
            .iter()
            .filter(|(_, s)| !s.not_holdings)
            .map(|(c, _)| *c)
            .collect()
    }

    pub fn is_system(&self, cid: &Cid) -> bool {
        self.index
            .lock()
            .expect("index")
            .get(cid)
            .is_some_and(|s| s.system)
    }

    /// Whether we hold the whole content for a CID (pin or seed cache) —
    /// cheap index check, no disk read.
    pub fn has_content(&self, cid: &Cid) -> bool {
        self.index
            .lock()
            .expect("index")
            .get(cid)
            .is_some_and(|s| s.has_content)
    }

    pub fn generation(&self, cid: &Cid) -> Option<Generation> {
        self.index
            .lock()
            .expect("index")
            .get(cid)
            .map(|s| s.gen.clone())
    }

    /// Whole content for a pinned/decoded CID, if held.
    pub fn content(&self, cid: &Cid) -> Option<Vec<u8>> {
        {
            let mut index = self.index.lock().expect("index");
            let state = index.get_mut(cid)?;
            if !state.has_content {
                return None;
            }
            state.last_access = now();
        }
        fs::read(self.cid_dir(cid).join("content")).ok()
    }

    pub fn piece_count(&self, cid: &Cid) -> usize {
        self.index
            .lock()
            .expect("index")
            .get(cid)
            .map(|s| s.piece_ids.len())
            .unwrap_or(0)
    }

    /// Serve coded pieces for a CID, excluding `exclude` piece_ids, up to
    /// `max`. Returns held pieces first; if the CID has content (pinned or
    /// decoded), tops up by ENCODING fresh pieces on demand — so a pinner
    /// never runs dry and has no rare-piece problem.
    pub fn serve_pieces(
        &self,
        cid: &Cid,
        exclude: &HashSet<[u8; 32]>,
        max: usize,
    ) -> Result<Vec<CodedPiece>> {
        let (gen, held_ids, has_content) = {
            let mut index = self.index.lock().expect("index");
            let Some(state) = index.get_mut(cid) else {
                return Ok(Vec::new());
            };
            state.last_access = now();
            (
                state.gen.clone(),
                state.piece_ids.clone(),
                state.has_content,
            )
        };

        let mut out = Vec::new();
        for pid in held_ids.iter().filter(|p| !exclude.contains(*p)) {
            if out.len() >= max {
                return Ok(out);
            }
            if let Some(piece) = self.read_piece(cid, pid) {
                out.push(piece);
            }
        }
        if out.len() < max && has_content {
            if let Some(content) = self.content(cid) {
                let sources = split_sources(&content, gen.k as usize, gen.piece_len as usize);
                let mut rng = rand::rngs::OsRng;
                while out.len() < max {
                    let piece = encode(&sources, &mut rng)
                        .map_err(|e| StoreError::Erasure(format!("{e:?}")))?;
                    if !exclude.contains(&piece.piece_id()) {
                        out.push(piece);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Mint ONE fresh independent coded piece from the WHOLE content — a new
    /// random full-rank combination of all k sources. Unlike [`serve_pieces`],
    /// this NEVER returns a stored piece: every call is a new independent
    /// combination, so a content holder can always add rank and restore a cid
    /// that has fallen below k. Returns None if the whole content is absent.
    ///
    /// The distinction matters for repair: `serve_pieces` returns STORED pieces
    /// first (topping up with fresh encodes only after), so a content holder
    /// that has ALSO ingested coded pieces would re-serve the same stored piece
    /// on every call — pushing duplicates that add no independent rank and
    /// leaving a below-k cid stuck while its inflated piece count masks it as
    /// recovered. Minting straight from content sidesteps that entirely.
    pub fn mint_from_content(&self, cid: &Cid) -> Option<CodedPiece> {
        let gen = self.generation(cid)?;
        let content = self.content(cid)?;
        let sources = split_sources(&content, gen.k as usize, gen.piece_len as usize);
        let mut rng = rand::rngs::OsRng;
        encode(&sources, &mut rng).ok()
    }

    fn read_piece(&self, cid: &Cid, pid: &[u8; 32]) -> Option<CodedPiece> {
        let bytes = fs::read(self.cid_dir(cid).join("pieces").join(hex::encode(pid))).ok()?;
        if bytes.len() < 4 {
            return None;
        }
        let vlen = u32::from_be_bytes(bytes[0..4].try_into().ok()?) as usize;
        if bytes.len() < 4 + vlen {
            return None;
        }
        Some(CodedPiece {
            coding_vector: bytes[4..4 + vlen].to_vec(),
            data: bytes[4 + vlen..].to_vec(),
        })
    }

    /// Delete a CID: remove all its data and tombstone it so repair/
    /// distribution/ingest can't resurrect it (decision 2026-07-03).
    pub fn tombstone(&self, cid: Cid) -> Result<()> {
        write_atomic(&self.root.join("tombstones").join(cid.to_hex()), b"")?;
        self.tombstones.lock().expect("tombstones").insert(cid);
        self.index.lock().expect("index").remove(&cid);
        let _ = fs::remove_dir_all(self.cid_dir(&cid));
        Ok(())
    }

    pub fn is_tombstoned(&self, cid: &Cid) -> bool {
        self.tombstones.lock().expect("tombstones").contains(cid)
    }
    /// CIDs this node has locally banned (tombstoned).
    pub fn tombstoned_cids(&self) -> Vec<Cid> {
        self.tombstones
            .lock()
            .expect("tombstones")
            .iter()
            .copied()
            .collect()
    }
    /// Lift a local ban — remove the tombstone so this node may host the CID
    /// again (operator reversing their own refusal; data must be re-fetched).
    pub fn untombstone(&self, cid: &Cid) -> Result<()> {
        let _ = fs::remove_file(self.root.join("tombstones").join(cid.to_hex()));
        self.tombstones.lock().expect("tombstones").remove(cid);
        Ok(())
    }

    /// Drop this node's local copy (content + pieces + generation + pin/want)
    /// WITHOUT tombstoning — the CID can be re-fetched or re-published later. This
    /// is soft delete ("remove my file"); `tombstone` is the ban ("never host").
    pub fn forget(&self, cid: &Cid) -> Result<()> {
        self.index.lock().expect("index").remove(cid);
        self.wanted.lock().expect("wanted").remove(cid);
        self.distributed.lock().expect("distributed").remove(cid);
        let _ = fs::remove_file(self.root.join("wanted").join(cid.to_hex()));
        let _ = fs::remove_file(self.root.join("distributed").join(cid.to_hex()));
        let _ = fs::remove_dir_all(self.cid_dir(cid));
        Ok(())
    }

    /// Mark a CID as WANTed (keep-alive intent; independent of holding it).
    pub fn set_want(&self, cid: Cid) -> Result<()> {
        write_atomic(&self.root.join("wanted").join(cid.to_hex()), b"")?;
        self.wanted.lock().expect("wanted").insert(cid);
        Ok(())
    }
    pub fn unset_want(&self, cid: &Cid) -> Result<()> {
        let _ = fs::remove_file(self.root.join("wanted").join(cid.to_hex()));
        self.wanted.lock().expect("wanted").remove(cid);
        Ok(())
    }
    pub fn is_wanted(&self, cid: &Cid) -> bool {
        self.wanted.lock().expect("wanted").contains(cid)
    }

    /// Mark a CID's erasure set as successfully distributed to peers (publish stops
    /// re-pushing it). Persistent so it survives restart.
    pub fn set_distributed(&self, cid: Cid) -> Result<()> {
        write_atomic(&self.root.join("distributed").join(cid.to_hex()), b"")?;
        self.distributed.lock().expect("distributed").insert(cid);
        Ok(())
    }
    pub fn is_distributed(&self, cid: &Cid) -> bool {
        self.distributed.lock().expect("distributed").contains(cid)
    }
    pub fn wanted_cids(&self) -> Vec<Cid> {
        self.wanted
            .lock()
            .expect("wanted")
            .iter()
            .copied()
            .collect()
    }

    /// Is `cid` within its eviction cooldown (evicted less than `ttl` ago)?
    /// While true, the lifecycle must not refill it (anti-thrash).
    pub fn is_in_cooldown(&self, cid: &Cid, ttl: Duration) -> bool {
        self.evicted
            .lock()
            .expect("evicted")
            .get(cid)
            .is_some_and(|ts| now_secs().saturating_sub(*ts) < ttl.as_secs())
    }
    /// Record an eviction (starts the cooldown).
    fn record_eviction(&self, cid: Cid) {
        let ts = now_secs();
        let _ = write_atomic(
            &self.root.join("evicted").join(cid.to_hex()),
            ts.to_string().as_bytes(),
        );
        self.evicted.lock().expect("evicted").insert(cid, ts);
    }
    /// Clear a cooldown (manual want/pin override — the operator wants it back).
    pub fn clear_cooldown(&self, cid: &Cid) {
        let _ = fs::remove_file(self.root.join("evicted").join(cid.to_hex()));
        self.evicted.lock().expect("evicted").remove(cid);
    }
    /// Purge cooldown records older than `ttl` (forgotten → re-acquirable).
    pub fn purge_cooldown(&self, ttl: Duration) {
        let now = now_secs();
        let mut e = self.evicted.lock().expect("evicted");
        let expired: Vec<Cid> = e
            .iter()
            .filter(|(_, ts)| now.saturating_sub(**ts) >= ttl.as_secs())
            .map(|(c, _)| *c)
            .collect();
        for cid in expired {
            e.remove(&cid);
            let _ = fs::remove_file(self.root.join("evicted").join(cid.to_hex()));
        }
    }

    fn guard_tombstone(&self, cid: &Cid) -> Result<()> {
        if self.is_tombstoned(cid) {
            return Err(StoreError::Tombstoned(cid.to_hex()));
        }
        Ok(())
    }

    /// Evict unpinned CIDs (oldest-accessed first) until total held bytes are
    /// at or below `target_bytes`. Pins are never evicted. Returns bytes freed.
    pub fn evict_to(&self, target_bytes: u64) -> Result<u64> {
        let victims: Vec<Cid> = {
            let index = self.index.lock().expect("index");
            let mut total: u64 = index.values().map(|s| s.bytes()).sum();
            if total <= target_bytes {
                return Ok(0);
            }
            let mut candidates: Vec<(u64, u64, Cid)> = index
                .iter()
                .filter(|(_, s)| !s.pinned && !s.system)
                .map(|(cid, s)| (s.last_access, s.bytes(), *cid))
                .collect();
            candidates.sort_by_key(|(access, _, _)| *access); // oldest first
            let mut chosen = Vec::new();
            for (_, bytes, cid) in candidates {
                if total <= target_bytes {
                    break;
                }
                total = total.saturating_sub(bytes);
                chosen.push(cid);
            }
            chosen
        };
        let mut freed = 0u64;
        for cid in &victims {
            if let Some(state) = self.index.lock().expect("index").remove(cid) {
                freed += state.bytes();
            }
            let _ = fs::remove_dir_all(self.cid_dir(cid));
            self.record_eviction(*cid); // start the cooldown (anti-thrash)
        }
        Ok(freed)
    }

    pub fn stats(&self) -> StoreStats {
        let index = self.index.lock().expect("index");
        StoreStats {
            cids: index.len(),
            pieces: index.values().map(|s| s.piece_ids.len()).sum(),
            pinned: index.values().filter(|s| s.pinned).count(),
            bytes: index.values().map(|s| s.bytes()).sum(),
        }
    }

    pub fn cids(&self) -> Vec<Cid> {
        self.index.lock().expect("index").keys().copied().collect()
    }
}

/// Split whole content into k padded sources (matches the publish encoder).
fn split_sources(content: &[u8], k: usize, piece_len: usize) -> Vec<Vec<u8>> {
    let mut sources: Vec<Vec<u8>> = content
        .chunks(piece_len.max(1))
        .map(|c| c.to_vec())
        .collect();
    while sources.len() < k {
        sources.push(vec![0u8; piece_len]);
    }
    for s in &mut sources {
        s.resize(piece_len, 0);
    }
    sources
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().expect("path has parent");
    fs::create_dir_all(dir)?;
    let tmp = dir.join(format!(
        ".{}.tmp",
        path.file_name().expect("file name").to_string_lossy()
    ));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, path)?;
    Ok(())
}

fn parse_cid(name: &str) -> Option<Cid> {
    let bytes: [u8; 32] = hex::decode(name).ok()?.try_into().ok()?;
    Some(Cid(bytes))
}

fn parse_pid(name: &str) -> Option<[u8; 32]> {
    hex::decode(name).ok()?.try_into().ok()
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
