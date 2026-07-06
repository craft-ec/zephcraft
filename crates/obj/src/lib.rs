//! CraftOBJ engine: publish, fetch, and serve coded pieces — tying store +
//! routing + erasure into network behaviors (CRAFTOBJ_DESIGN v2.0).
//!
//! M2.3a scope: the store-and-retrieve core.
//!  - `publish`: encode → store locally (pin by default) → push n pieces
//!    across ≥K distinct peers → announce providers. Reports durable only
//!    at ≥K distinct peer acks (the durability rule).
//!  - `get`: resolve providers via routing (no manual peer) → fetch pieces
//!    (exclude-list) → vtag-verify each → progressive decode → verify whole
//!    content → apply consume mode (seed/drop/ephemeral).
//!  - `serve`: ingest pushed pieces (VERIFY vtags before storing → pollution
//!    never enters the store) and answer piece requests from the store.
//!
//! Distribution, HealthScan, repair, and deletion are later obj sub-items.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::rngs::OsRng;
use zeph_core::{Cid, NodeId};
use zeph_erasure::{encode, recode, target_pieces, vtags, CodedPiece, Decoder};
use zeph_routing::ContentRouting;
use zeph_store::{Generation, Store};
use zeph_transport::{Connection, PeerAddr, Transport};
use zeph_wire as wire;

mod encrypted;
mod manifest;
pub use encrypted::{EncryptedEnvelope, PlainFile, Recipient, ENVELOPE_MAGIC};
pub use manifest::{Entry, Manifest};

/// ALPN for piece exchange.
pub const ALPN: &[u8] = b"/craftec/piece/1";

/// Re-announce a provider record this long after its last announce (foundation §462:
/// 22h re-announce inside a 48h DHT TTL). Per-cid scheduling — NOT every cycle.
const REPUBLISH_MS: u64 = 22 * 3600 * 1000;
/// On ingest, re-announce our (growing) piece count at most this often per cid — keeps provider
/// records tracking real holdings so the health scan's summed `effective` doesn't undercount and
/// churn repairs, without an announce-per-piece flood.
const INGEST_ANNOUNCE_DEBOUNCE_MS: u64 = 2000;
/// How long the health scan caches the alive-peer set used to exclude DEAD holders (whose
/// provider records linger until TTL). Bounds liveness lookups to one per this window.
const ALIVE_CACHE: Duration = Duration::from_secs(10);
/// Timeout for a liveness probe (a connect attempt) when no membership source is wired.
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
/// Cids processed per chunk in the health-scan sweep + re-announce refresh. Between chunks the
/// loop sleeps `ObjConfig::pace_delay`, bounding in-flight DHT ops and trickling the load
/// instead of an O(N) burst — so both scale to thousands of cids without storming the overlay.
const PACE_CHUNK: usize = 5;
const MAX_FRAME: usize = wire::MAX_MESSAGE_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumeMode {
    /// Become a transient provider after decoding (default; = Scaling).
    Seed,
    /// Discard content after decoding; hold nothing long-term.
    Drop,
    /// Pure client — same as Drop at this layer (serve-nothing-during-fetch
    /// is a fetch-side detail deferred; here Drop and Ephemeral both hold
    /// nothing after decode).
    Ephemeral,
}

#[derive(Debug, Clone)]
pub struct ObjConfig {
    /// Generation size (single-generation demo path; K=8 default).
    pub k: usize,
    /// Distinct-peer threshold for `durable` (foundation ≥K rule).
    pub durability_threshold: usize,
    /// Storage this node offers to the network (bytes) — announced as
    /// capacity and (later) the eviction ceiling.
    pub capacity_bytes: u64,
    /// HealthScan availability-probe timeout: a holder that doesn't answer
    /// within this is treated as gone (not counted toward availability).
    pub probe_timeout: Duration,
    /// Scaling: pulls (served piece requests) for a CID within one scan cycle
    /// above which it is "hot" and a provider recruits another (bandwidth).
    pub scale_threshold: u32,
    /// Degradation: pulls per cycle BELOW which a surplus CID sheds toward the
    /// floor. Must be < scale_threshold (hysteresis band → no scale/shed flap).
    pub degrade_threshold: u32,
    /// Fade grace window: content last FETCHED within this counts as
    /// demand-alive (kept repaired). Beyond it — and unpinned, unwanted — it is
    /// left to fade. Configurable; default 1 day.
    pub fade_grace: Duration,
    /// Eviction cooldown: an evicted CID won't be refilled for this long
    /// (anti-thrash), then the record is purged. Default 30 days.
    pub eviction_cooldown: Duration,
    /// Pacing delay between chunks of PACE_CHUNK cids in the health-scan sweep and the
    /// re-announce refresh — spaces DHT ops out so reaching steady state is a slow crawl,
    /// not a burst. Default 1s; tests set it ~0. See PACE_CHUNK.
    pub pace_delay: Duration,
}

impl Default for ObjConfig {
    fn default() -> Self {
        Self {
            k: 8,
            durability_threshold: 8,
            capacity_bytes: 10 * 1024 * 1024 * 1024, // 10 GiB default
            probe_timeout: Duration::from_secs(2),
            scale_threshold: 20,
            degrade_threshold: 5,
            fade_grace: Duration::from_secs(24 * 60 * 60),
            eviction_cooldown: Duration::from_secs(30 * 24 * 60 * 60),
            pace_delay: Duration::from_secs(1),
        }
    }
}

/// One HealthScan pass' outcome (for the dashboard / status).
#[derive(Debug, Clone, Default)]
pub struct HealthReport {
    pub scanned: usize,
    /// CIDs whose VERIFIED availability is below the durability floor.
    pub at_risk: usize,
    /// Pieces this node minted + pushed this pass (repair actions taken).
    pub repaired: usize,
    /// Surplus pieces shed this pass (degradation actions taken).
    pub degraded: usize,
    /// At-risk CIDs left to FADE this pass (nothing wants them — no repair).
    pub fading: usize,
    /// Retained copies dropped this pass because they are now durable on >= k peers.
    pub offloaded: usize,
    /// CIDs currently ABOVE the durability band (shedding cold surplus toward the floor).
    pub surplus: usize,
}

/// One Distribution pass' outcome.
#[derive(Debug, Clone, Default)]
pub struct DistributeReport {
    pub scanned: usize,
    /// Pieces MOVED to less-full peers this pass.
    pub moved: usize,
}

/// One Scaling pass' outcome.
#[derive(Debug, Clone, Default)]
pub struct ScaleReport {
    /// CIDs found "hot" (pull rate over threshold) this pass.
    pub hot: usize,
    /// New providers recruited (pieces created for bandwidth headroom).
    pub scaled: usize,
}

/// Rendezvous epoch length — rotates which holder repairs a CID over time so
/// the same node isn't always elected (BLAKE3(node ‖ cid ‖ epoch)).
const HEALTH_EPOCH_MS: u64 = 30_000;

/// Per-piece push timeout — a slow/stalled peer mustn't hang a publish.
const PUSH_TIMEOUT: Duration = Duration::from_secs(10);

/// Outcome of publishing a file (content + its manifest).
#[derive(Debug, Clone)]
/// Result of publishing a PRIVATE file (ENCRYPTION_DESIGN.md phase 2). The
/// shareable id is `envelope_cid`; resolving it (with the owner's key) decrypts.
pub struct PrivatePublish {
    pub envelope_cid: Cid,
    pub ciphertext_cid: Cid,
    pub size: u64,
    pub durable: bool,
}

/// A decrypted private file.
pub struct PlainFileOut {
    pub name: String,
    pub mime: String,
    pub content: Vec<u8>,
}

pub struct FilePublish {
    /// The manifest CID — share this; fetching it restores the file by name.
    pub manifest_cid: Cid,
    /// The raw-content CID (BLAKE3 of the bytes; dedups across names).
    pub content_cid: Cid,
    pub size: u64,
    pub durable: bool,
    pub pinned: bool,
}

#[derive(Debug, Clone)]
pub struct PublishReport {
    pub cid: Cid,
    pub pieces_pushed: usize,
    pub distinct_peers: usize,
    /// True iff the content reached ≥ durability_threshold distinct peers.
    pub durable: bool,
    pub pinned: bool,
}

/// Source of live candidate peers to place pieces on. Production backs this with SWIM
/// membership (real-time, in-network liveness); tests back it with an in-memory double.
/// Replaces the old `ContentRouting::nodes()` census lookup.
#[async_trait::async_trait]
pub trait PeerSource: Send + Sync {
    /// Live peers (id + dialable addr). May include self; callers filter it out.
    async fn peers(&self) -> Vec<(NodeId, PeerAddr)>;
}

/// Per-cid diagnostic snapshot from the last health scan (for the dashboard). The verdict is
/// derived separately from the at-risk / fading sets; this carries the raw numbers.
#[derive(Clone, Debug)]
pub struct CidHealth {
    /// Wall-clock ms of the last scan (HLC), 0 if never scanned.
    pub last_scan_ms: u64,
    /// Effective coded pieces on the network the scan counted (sum over LIVE providers incl self).
    pub effective: u32,
    /// Durability floor n = target_pieces(k).
    pub floor: u32,
    /// Distinct LIVE peer providers seen.
    pub live_providers: u32,
    /// What the scan CONCLUDED about this cid (e.g. "below band — under-replicated").
    pub decision: String,
    /// What the scan DID as a result (e.g. "repaired — minted + pushed 1 piece", "none — healthy").
    pub action: String,
}

pub struct ObjEngine {
    transport: Arc<Transport>,
    store: Arc<Store>,
    routing: Arc<dyn ContentRouting>,
    peer_source: Arc<dyn PeerSource>,
    config: ObjConfig,
    /// Observed download demand: pulls (served piece requests) per CID since
    /// the last Scaling pass. This is ACTUAL fetch traffic, not the WANT
    /// interest signal — Scaling responds to real downloads.
    demand: Mutex<HashMap<[u8; 32], u32>>,
    /// Monotonic time of the last real FETCH served per CID (serve-only — NOT
    /// bumped by lifecycle writes, unlike the store's LRU last_access). Fade's
    /// demand-recency signal.
    last_served: Mutex<HashMap<[u8; 32], Instant>>,
    /// The node event bus (foundation §52), set post-construction. Producers:
    /// health_scan → RepairNeeded, enforce_quota → DiskWatermarkHit. Optional so
    /// tests construct an engine without one.
    events: std::sync::OnceLock<zeph_events::EventBus>,
    /// The owner's encryption keypair (PRE), set post-construction. Needed for
    /// publish_private / get_private; None if this node never handles private data.
    enc: std::sync::OnceLock<zeph_cipher::EncKeypair>,
    /// Snapshot from the last health scan: retained content not yet durable —
    /// (cid, coded pieces on OTHER nodes, floor = target n). Drives the dashboard's
    /// "pending distribution" view.
    pending: Mutex<Vec<([u8; 32], u32, u32)>>,
    /// Per-cid last-announce time (ms) — drives TTL-aware re-announce scheduling so a record
    /// is refreshed ~every 22h, not every cycle.
    announced_at: Mutex<HashMap<[u8; 32], u64>>,
    /// Persistent GLOBAL durability sets, aggregated across per-chunk scans: cids currently
    /// at-risk / left-to-fade. Each health_scan_chunk updates the chunk's cids in place, so the
    /// dashboard reads accurate global counts without any one job scanning the whole set.
    at_risk_ids: Mutex<HashSet<[u8; 32]>>,
    fading_ids: Mutex<HashSet<[u8; 32]>>,
    /// Cids currently SHEDDING cold surplus back toward the floor (Schmitt state, mirrors at_risk_ids).
    surplus_ids: Mutex<HashSet<[u8; 32]>>,
    /// Set by the node: a channel to fire demand-driven scaling. When a served pull crosses
    /// scale_threshold the serve path sends the cid here, and a worker recruits another provider
    /// immediately — scaling reacts to access, not to any scan/distribute cadence.
    scale_trigger: std::sync::OnceLock<tokio::sync::mpsc::UnboundedSender<Cid>>,
    /// Liveness source (set by the node = membership; a test source in tests). The health scan
    /// filters provider records by this so a DIED holder — whose record lingers until TTL — stops
    /// inflating a cid's effective count, and repair fires. None → no filtering (legacy).
    liveness: Mutex<Option<Arc<dyn PeerSource>>>,
    alive_cache: Mutex<Option<(Instant, HashSet<NodeId>)>>,
    /// Per-node liveness probe cache (used only when no membership source is wired — e.g. tests).
    node_liveness: Mutex<HashMap<NodeId, (Instant, bool)>>,
    /// Per-cid health snapshot from the last scan (dashboard diagnostics).
    cid_health: Mutex<HashMap<[u8; 32], CidHealth>>,
}

impl ObjEngine {
    /// Construct with an explicit [`PeerSource`] — production passes a membership-backed one
    /// so candidate peers come from live SWIM state, not the tracker.
    pub fn with_peer_source(
        transport: Arc<Transport>,
        store: Arc<Store>,
        routing: Arc<dyn ContentRouting>,
        peer_source: Arc<dyn PeerSource>,
        config: ObjConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            transport,
            store,
            routing,
            peer_source,
            config,
            demand: Mutex::new(HashMap::new()),
            last_served: Mutex::new(HashMap::new()),
            events: std::sync::OnceLock::new(),
            enc: std::sync::OnceLock::new(),
            pending: Mutex::new(Vec::new()),
            announced_at: Mutex::new(HashMap::new()),
            at_risk_ids: Mutex::new(HashSet::new()),
            fading_ids: Mutex::new(HashSet::new()),
            surplus_ids: Mutex::new(HashSet::new()),
            scale_trigger: std::sync::OnceLock::new(),
            liveness: Mutex::new(None),
            alive_cache: Mutex::new(None),
            node_liveness: Mutex::new(HashMap::new()),
            cid_health: Mutex::new(HashMap::new()),
        })
    }

    /// Attach the node event bus so lifecycle producers can publish (§52).
    pub fn set_events(&self, bus: zeph_events::EventBus) {
        let _ = self.events.set(bus);
    }

    /// Publish an event if a bus is attached (no-op otherwise).
    fn emit(&self, event: zeph_events::Event) {
        if let Some(bus) = self.events.get() {
            bus.publish(event);
        }
    }

    /// Attach the owner's encryption keypair (enables private publish/get).
    pub fn set_enc_keypair(&self, kp: zeph_cipher::EncKeypair) {
        let _ = self.enc.set(kp);
    }

    /// Publish a PRIVATE file: encrypt {name,mime,data} under the owner's key,
    /// store the ciphertext (erasure-coded like anything else), and publish a
    /// small envelope pointing at it. The network sees only ciphertext + envelope.
    pub async fn publish_private(
        &self,
        name: &str,
        mime: &str,
        data: &[u8],
        pin: bool,
    ) -> anyhow::Result<PrivatePublish> {
        let enc = self
            .enc
            .get()
            .ok_or_else(|| anyhow::anyhow!("no encryption keypair set"))?;
        let plain = encrypted::PlainFile {
            name: name.to_string(),
            mime: mime.to_string(),
            content: data.to_vec(),
        };
        let sealed = zeph_cipher::encrypt(&enc.public(), &plain.encode());
        let ct = self.publish(&sealed.ciphertext, pin).await?;
        let envelope = encrypted::EncryptedEnvelope {
            capsule: sealed.capsule,
            ciphertext_cid: ct.cid.0,
            owner: self.transport.node_id().0,
            recipients: Vec::new(),
        };
        let er = self.publish(&envelope.encode(), pin).await?;
        Ok(PrivatePublish {
            envelope_cid: er.cid,
            ciphertext_cid: ct.cid,
            size: data.len() as u64,
            durable: ct.durable,
        })
    }

    /// Resolve + decrypt a private object by its envelope CID (needs our key).
    pub async fn get_private(&self, envelope_cid: Cid) -> anyhow::Result<PlainFileOut> {
        let enc = self
            .enc
            .get()
            .ok_or_else(|| anyhow::anyhow!("no encryption keypair set"))?;
        let ebytes = self.get(envelope_cid, ConsumeMode::Drop).await?;
        let envelope = encrypted::EncryptedEnvelope::decode(&ebytes)
            .ok_or_else(|| anyhow::anyhow!("not an encrypted envelope"))?;
        let ct = self
            .get(Cid(envelope.ciphertext_cid), ConsumeMode::Drop)
            .await?;
        let sealed = zeph_cipher::SealedObject {
            capsule: envelope.capsule,
            ciphertext: ct,
        };
        let plaintext = zeph_cipher::decrypt_self(enc, &sealed)?;
        let pf = encrypted::PlainFile::decode(&plaintext)
            .ok_or_else(|| anyhow::anyhow!("corrupt plaintext"))?;
        Ok(PlainFileOut {
            name: pf.name,
            mime: pf.mime,
            content: pf.content,
        })
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    /// The effective engine configuration (for the Settings view).
    pub fn config(&self) -> &ObjConfig {
        &self.config
    }

    /// Content is "alive" — worth maintaining/spreading — iff it is pinned
    /// (locally or by a provider), wanted (locally or network-wide), or fetched
    /// within the fade grace window. Otherwise it fades (Repair + Distribution
    /// both skip it).
    /// Fade decision — should this content keep being repaired, or be left to fade? True if
    /// it is pinned, a system generation (CraftSQL), locally wanted, provider-pinned, or
    /// recently served. Only if NONE of those cheap local checks hold does it consult the
    /// network — a per-cid `is_wanted` DHT lookup, checked LAST so it runs only for otherwise
    /// cold content (never enumerates; foundation §290 HAVE/WANT).
    async fn is_alive(&self, cid: &Cid, provider_pinned: bool) -> bool {
        self.store.is_pinned(cid)
            || self.store.is_system(cid)
            || self.store.is_wanted(cid)
            || provider_pinned
            || self
                .last_served
                .lock()
                .expect("last_served")
                .get(&cid.0)
                .is_some_and(|t| t.elapsed() < self.config.fade_grace)
            || self.routing.is_wanted(*cid).await.unwrap_or(false)
    }

    /// Current wall-clock time (unix millis) from the HLC — for metadata
    /// envelopes (`published_at`).
    fn now_millis(&self) -> u64 {
        self.transport.clock().now().millis()
    }

    #[doc(hidden)]
    pub fn served_recently(&self, cid: &Cid) -> bool {
        self.last_served
            .lock()
            .expect("last_served")
            .contains_key(&cid.0)
    }

    /// Served-pull count for a CID in the current demand window (drives
    /// Scaling; exposed for the dashboard and tests).
    pub fn served_pulls(&self, cid: &Cid) -> u32 {
        self.demand
            .lock()
            .expect("demand")
            .get(&cid.0)
            .copied()
            .unwrap_or(0)
    }

    fn split_sources(&self, data: &[u8], piece_len: usize) -> Vec<Vec<u8>> {
        let mut sources: Vec<Vec<u8>> = data.chunks(piece_len.max(1)).map(|c| c.to_vec()).collect();
        while sources.len() < self.config.k {
            sources.push(vec![0u8; piece_len]);
        }
        for s in &mut sources {
            s.resize(piece_len, 0);
        }
        sources
    }

    /// Publish content: encode, store locally (pin by default), spread to
    /// distinct peers, announce providers. `durable` = reached ≥K peers.
    pub async fn publish(&self, data: &[u8], pin: bool) -> anyhow::Result<PublishReport> {
        self.publish_impl(data, pin, false).await
    }

    async fn publish_impl(
        &self,
        data: &[u8],
        pin: bool,
        system: bool,
    ) -> anyhow::Result<PublishReport> {
        anyhow::ensure!(!data.is_empty(), "refusing to publish empty content");
        let cid = Cid::of(data);
        let k = self.config.k;
        let piece_len = data.len().div_ceil(k).max(1);
        let sources = self.split_sources(data, piece_len);

        let mut rng = OsRng;
        let tags = vtags::generate(&sources, &mut rng)?;
        let vtags_blob = postcard::to_allocvec(&tags)?;
        let gen = Generation {
            k: k as u32,
            piece_len: piece_len as u64,
            total_len: data.len() as u64,
            vtags: vtags_blob,
        };

        self.store.put_generation(cid, gen.clone())?;
        // Mark BEFORE pushing so each push carries the system flag to holders.
        if system {
            self.store.mark_system(&cid)?;
        }
        if pin {
            self.store.pin(cid, data)?;
        }

        // Already distributed: a previous publish's erasure set reached the network, so a
        // re-publish must NOT re-push — that re-erasure-codes and mints FRESH random coded
        // pieces every time, growing the cluster's piece count without bound. Just refresh
        // the announce and return.
        if self.store.is_distributed(&cid) {
            if pin {
                let _ = self.routing.announce(cid, 0, true).await;
            }
            return Ok(PublishReport {
                cid,
                pieces_pushed: 0,
                distinct_peers: 0,
                durable: true,
                pinned: pin,
            });
        }

        // Candidate storage peers from the node registry (exclude self).
        let me = self.transport.node_id();
        let candidates: Vec<PeerAddr> = self
            .peer_source
            .peers()
            .await
            .into_iter()
            .filter(|(id, _)| *id != me)
            .map(|(_, addr)| addr)
            .collect();

        let n = target_pieces(k);
        let mut distinct: HashSet<_> = HashSet::new();
        let mut pushed = 0usize;
        if !candidates.is_empty() {
            // Encode all pieces up front (encode needs &mut rng), then push them
            // CONCURRENTLY with a per-push timeout. Sequential pushes turned
            // publish into n round-trips — crippling over a relay link and
            // ruinous for folders (many objects). One round-trip now, and a
            // stalled peer times out instead of hanging the whole publish.
            let jobs: Vec<(PeerAddr, CodedPiece)> = (0..n)
                .map(|i| {
                    Ok((
                        candidates[i % candidates.len()].clone(),
                        encode(&sources, &mut rng)?,
                    ))
                })
                .collect::<anyhow::Result<_>>()?;
            let gen_ref = &gen;
            let results = futures::future::join_all(jobs.iter().map(|(peer, piece)| async move {
                match tokio::time::timeout(PUSH_TIMEOUT, self.push_piece(peer, cid, gen_ref, piece))
                    .await
                {
                    Ok(Ok(())) => Some(peer.node_id()),
                    _ => None,
                }
            }))
            .await;
            for id in results.into_iter().flatten() {
                distinct.insert(id);
                pushed += 1;
            }
        }

        if pin {
            let _ = self.routing.announce(cid, 0, true).await;
        }

        // DURABILITY GATE + COMPLETION. The publisher RETAINS its own copy of everything
        // it publishes (kept alive by its class — system marker / pin / want), so the first
        // distribution is never left with zero copies even into a no-peer window. When the
        // erasure set has REACHED the network — all n coded pieces pushed, or as many
        // distinct peers as this cluster can offer — mark it DISTRIBUTED so re-publishes
        // stop re-pushing (no unbounded piece growth). From there the health scan maintains
        // it as stable content, and the publisher offloads its own copy once the full
        // erasure set is durable on peers.
        if !self.store.has_content(&cid) {
            self.store.put_content(cid, data, false)?;
        }
        if !system && !pin {
            self.store.set_want(cid)?;
        }
        let target = self.config.durability_threshold.min(candidates.len());
        if !candidates.is_empty() && (pushed >= n || distinct.len() >= target) {
            let _ = self.store.set_distributed(cid);
        }
        let durable = distinct.len() >= self.config.durability_threshold;
        if !durable {
            tracing::warn!(
                cid = %cid.to_hex(),
                distinct_peers = distinct.len(),
                threshold = self.config.durability_threshold,
                "publish not yet durable — retained locally; health scan will redistribute"
            );
        }

        Ok(PublishReport {
            cid,
            pieces_pushed: pushed,
            distinct_peers: distinct.len(),
            durable,
            pinned: pin,
        })
    }

    /// Publish a FILE: store its bytes as content, then a File manifest naming
    /// them (size + mime). Returns the manifest CID — what you share; fetching
    /// it restores the file by name. The content CID stays BLAKE3(bytes) so
    /// identical bytes dedup regardless of filename.
    pub async fn publish_file(
        &self,
        name: &str,
        mime: &str,
        data: &[u8],
        pin: bool,
    ) -> anyhow::Result<FilePublish> {
        let cr = self.publish(data, pin).await?;
        let m = Manifest::File {
            name: name.to_string(),
            size: data.len() as u64,
            mime: mime.to_string(),
            content: cr.cid.0,
        };
        let mr = self.publish(&m.encode(), pin).await?;
        // Attach an editable metadata envelope (published_at = now) to the
        // MANIFEST cid — the named thing — not the raw content.
        let _ = self
            .routing
            .announce_meta(mr.cid, self.now_millis(), None)
            .await;
        Ok(FilePublish {
            manifest_cid: mr.cid,
            content_cid: cr.cid,
            size: data.len() as u64,
            durable: cr.durable,
            pinned: pin,
        })
    }

    /// Publish a DIRECTORY manifest from already-published child entries (each
    /// `Entry.cid` is a child manifest CID). Returns the dir manifest CID.
    pub async fn publish_dir(
        &self,
        name: &str,
        entries: Vec<Entry>,
        pin: bool,
    ) -> anyhow::Result<Cid> {
        let m = Manifest::Dir {
            name: name.to_string(),
            entries,
        };
        let cid = self.publish(&m.encode(), pin).await?.cid;
        let _ = self
            .routing
            .announce_meta(cid, self.now_millis(), None)
            .await;
        Ok(cid)
    }

    /// Fetch and decode a manifest object by CID.
    pub async fn fetch_manifest(&self, cid: Cid) -> anyhow::Result<Manifest> {
        let bytes = self.get(cid, ConsumeMode::Drop).await?;
        Manifest::decode(&bytes).ok_or_else(|| anyhow::anyhow!("{cid} is not a manifest"))
    }

    /// Fetch a file by its manifest CID → (name, mime, bytes).
    pub async fn fetch_file(&self, manifest_cid: Cid) -> anyhow::Result<(String, String, Vec<u8>)> {
        match self.fetch_manifest(manifest_cid).await? {
            Manifest::File {
                name,
                mime,
                content,
                ..
            } => {
                let bytes = self.get(Cid(content), ConsumeMode::Seed).await?;
                Ok((name, mime, bytes))
            }
            Manifest::Dir { name, .. } => {
                anyhow::bail!("'{name}' is a folder, not a file")
            }
        }
    }

    /// Set (edit) this node's metadata envelope comment for `cid`. Preserves
    /// the original `published_at` if this node already has an envelope.
    pub async fn set_meta(&self, cid: Cid, comment: Option<String>) -> anyhow::Result<()> {
        let me = self.transport.node_id();
        let published_at = self
            .routing
            .metas(cid)
            .await
            .unwrap_or_default()
            .into_iter()
            .find(|m| m.publisher == me)
            .map(|m| m.published_at)
            .unwrap_or_else(|| self.now_millis());
        self.routing
            .announce_meta(cid, published_at, comment)
            .await?;
        Ok(())
    }

    /// Delete this node's metadata envelope for `cid` (signed withdrawal).
    pub async fn del_meta(&self, cid: Cid) -> anyhow::Result<()> {
        self.routing.withdraw_meta(cid).await?;
        Ok(())
    }

    async fn push_piece(
        &self,
        peer: &PeerAddr,
        cid: Cid,
        gen: &Generation,
        piece: &CodedPiece,
    ) -> anyhow::Result<()> {
        let msg = wire::Message::PiecePush(wire::PiecePush {
            cid: cid.0,
            k: gen.k,
            piece_len: gen.piece_len,
            total_len: gen.total_len,
            vtags: gen.vtags.clone(),
            piece: wire::WirePiece {
                coding_vector: piece.coding_vector.clone(),
                data: piece.data.clone(),
            },
            // Propagate the system class with every push (publish, repair,
            // distribute) so each holder treats it as DB data locally.
            system: self.store.is_system(&cid),
        });
        match self.request(peer, &msg).await? {
            wire::Message::PiecePushAck(ack) if ack.ok => Ok(()),
            wire::Message::PiecePushAck(ack) => anyhow::bail!("push rejected: {}", ack.reason),
            _ => anyhow::bail!("unexpected push reply"),
        }
    }

    async fn request(&self, peer: &PeerAddr, msg: &wire::Message) -> anyhow::Result<wire::Message> {
        let conn = self.transport.connect(peer, ALPN).await?;
        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(&wire::encode(msg, self.transport.clock().now().0))
            .await?;
        send.finish()?;
        let bytes =
            tokio::time::timeout(Duration::from_secs(30), recv.read_to_end(MAX_FRAME)).await??;
        let frame = wire::decode(&bytes)?;
        conn.close(0u32.into(), b"done");
        Ok(frame.message)
    }

    /// Fetch content by CID alone: resolve providers, fetch + verify + decode.
    pub async fn get(&self, cid: Cid, mode: ConsumeMode) -> anyhow::Result<Vec<u8>> {
        // Local shortcut: we already hold the whole content.
        if let Some(content) = self.store.content(&cid) {
            if cid.verifies(&content) {
                return Ok(content);
            }
        }

        let mut decoder: Option<Decoder> = None;
        let mut tags: Option<vtags::VTags> = None;
        let mut total_len = 0u64;
        let mut exclude: HashSet<[u8; 32]> = HashSet::new();

        // Reconstruct from LOCAL pieces first: a node holding >=k of its own
        // coded pieces decodes with NO network fetch — instant, and robust even
        // when the network is momentarily depleted. Only the deficit is fetched
        // below. (This is why `pin` on a node that already holds enough pieces
        // completes immediately instead of re-fetching.)
        if let Some(gen) = self.store.generation(&cid) {
            if let Ok(t) = postcard::from_bytes::<vtags::VTags>(&gen.vtags) {
                let local = self
                    .store
                    .serve_pieces(&cid, &HashSet::new(), gen.k as usize * 3)
                    .unwrap_or_default();
                if !local.is_empty() {
                    let mut d = Decoder::new(gen.k as usize, gen.piece_len as usize);
                    for p in &local {
                        if vtags::verify(&t, p) && exclude.insert(p.piece_id()) {
                            let _ = d.add_piece(p);
                        }
                    }
                    total_len = gen.total_len;
                    tags = Some(t);
                    decoder = Some(d);
                }
            }
        }

        // Fetch the deficit from the network — skipped entirely if local pieces
        // already decoded the content.
        let done = |d: &Option<Decoder>| d.as_ref().is_some_and(|d| d.is_complete());
        let mut providers = if done(&decoder) {
            Vec::new()
        } else {
            self.routing.resolve(cid).await.unwrap_or_default()
        };
        anyhow::ensure!(
            !providers.is_empty() || done(&decoder),
            "no providers for {cid} and local pieces insufficient to reconstruct"
        );
        {
            use rand::seq::SliceRandom;
            providers.shuffle(&mut OsRng);
        }

        // Each round, pull ONE piece from up to FANOUT providers CONCURRENTLY
        // and take pieces as they arrive — the fastest sources contribute first
        // (RLNC: any k independent pieces from any mix of providers decode).
        const FANOUT: usize = 16;
        'rounds: for _round in 0..64 {
            if decoder.as_ref().is_some_and(|d| d.is_complete()) {
                break;
            }
            let excl: Vec<[u8; 32]> = exclude.iter().copied().collect();
            let futs = providers.iter().take(FANOUT).map(|p| {
                let msg = wire::Message::PieceRequest(wire::PieceRequest {
                    cid: cid.0,
                    exclude: excl.clone(),
                    max_pieces: 1,
                });
                async move { self.request(&p.addr, &msg).await }
            });
            let results = futures::future::join_all(futs).await;

            let mut progressed = false;
            for res in results {
                let Ok(wire::Message::PieceResponse(resp)) = res else {
                    continue;
                };
                if !resp.found || resp.pieces.is_empty() {
                    continue;
                }
                if tags.is_none() {
                    tags = Some(postcard::from_bytes(&resp.vtags)?);
                    decoder = Some(Decoder::new(resp.k as usize, resp.piece_len as usize));
                    total_len = resp.total_len;
                }
                let tags_ref = tags.as_ref().expect("set");
                let decoder_ref = decoder.as_mut().expect("set");
                for wp in resp.pieces {
                    let piece = CodedPiece {
                        coding_vector: wp.coding_vector,
                        data: wp.data,
                    };
                    anyhow::ensure!(
                        vtags::verify(tags_ref, &piece),
                        "piece failed vtag verification — refusing polluted data"
                    );
                    if exclude.insert(piece.piece_id()) {
                        decoder_ref.add_piece(&piece)?;
                        progressed = true;
                    }
                }
                if decoder.as_ref().is_some_and(|d| d.is_complete()) {
                    break 'rounds;
                }
            }
            if !progressed {
                break;
            }
        }

        let decoder = decoder.ok_or_else(|| anyhow::anyhow!("no pieces received for {cid}"))?;
        anyhow::ensure!(decoder.is_complete(), "insufficient pieces to decode {cid}");
        let mut bytes: Vec<u8> = decoder.decode()?.into_iter().flatten().collect();
        bytes.truncate(total_len as usize);
        anyhow::ensure!(cid.verifies(&bytes), "decoded content does not match {cid}");

        // Consume mode: seed = become a transient provider.
        if mode == ConsumeMode::Seed {
            if let Some(t) = &tags {
                let gen = Generation {
                    k: decoder_k(t),
                    piece_len: t.piece_len,
                    total_len,
                    vtags: postcard::to_allocvec(t)?,
                };
                let _ = self.store.put_generation(cid, gen);
                let _ = self.store.put_content(cid, &bytes, false);
                let _ = self.routing.announce(cid, 0, false).await;
            }
        }
        Ok(bytes)
    }

    /// Pin a CID: ensure we hold the whole content (fetch if needed), store
    /// it eviction-exempt, and announce as a pinned provider.
    pub async fn pin(&self, cid: Cid) -> anyhow::Result<()> {
        guard_not_system(&self.store, &cid)?;
        let content = match self.store.content(&cid) {
            Some(c) if cid.verifies(&c) => c,
            _ => self.get(cid, ConsumeMode::Drop).await?,
        };
        self.store.pin(cid, &content)?;
        self.store.clear_cooldown(&cid); // manual pin overrides eviction cooldown
        let _ = self.routing.announce(cid, 0, true).await;
        Ok(())
    }

    /// Unpin a CID: revert to the normal (evictable) lifecycle.
    pub async fn unpin(&self, cid: Cid) -> anyhow::Result<()> {
        guard_not_system(&self.store, &cid)?;
        self.store.unpin(&cid)?;
        Ok(())
    }

    /// Reconstruct an object from LOCAL pieces only (no network) — returns the
    /// whole content if we hold it whole OR hold ≥k of its coded pieces. Lets us
    /// name a curated cid we host only as pieces (e.g. a wanted manifest) without
    /// a network round-trip.
    pub fn decode_local(&self, cid: &Cid) -> Option<Vec<u8>> {
        if let Some(c) = self.store.content(cid) {
            if cid.verifies(&c) {
                return Some(c);
            }
        }
        let gen = self.store.generation(cid)?;
        let t = postcard::from_bytes::<vtags::VTags>(&gen.vtags).ok()?;
        let local = self
            .store
            .serve_pieces(cid, &HashSet::new(), gen.k as usize * 3)
            .unwrap_or_default();
        if local.is_empty() {
            return None;
        }
        let mut d = Decoder::new(gen.k as usize, gen.piece_len as usize);
        let mut seen = HashSet::new();
        for p in &local {
            if vtags::verify(&t, p) && seen.insert(p.piece_id()) {
                let _ = d.add_piece(p);
            }
        }
        if !d.is_complete() {
            return None;
        }
        let mut bytes: Vec<u8> = d.decode().ok()?.into_iter().flatten().collect();
        bytes.truncate(gen.total_len as usize);
        cid.verifies(&bytes).then_some(bytes)
    }

    /// Objects this manifest/envelope directly references — the content (File),
    /// the ciphertext (private envelope), or the child entries (Dir) — reconstructed
    /// from local pieces if needed. Empty for raw content or objects we don't have.
    /// Lets callers treat a file/folder as its whole reachable chain.
    pub fn referenced_objects(&self, cid: &Cid) -> Vec<Cid> {
        match self.decode_local(cid) {
            Some(bytes) => chain_children(&bytes),
            None => Vec::new(),
        }
    }

    /// Pin a whole file/folder: pin the manifest/envelope AND cascade to every
    /// object it references (content, ciphertext, folder children, recursively),
    /// so a pin keeps the ENTIRE thing alive — not just the top object, which
    /// would leave the content evictable and the file broken. Returns the count.
    pub async fn pin_chain(&self, cid: Cid) -> anyhow::Result<usize> {
        let mut n = 0;
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![cid];
        while let Some(c) = stack.pop() {
            if !seen.insert(c.0) {
                continue;
            }
            // pin() fetches + stores the content, so after it the object is local
            // and we can decode it to discover the next links in the chain.
            if self.pin(c).await.is_ok() {
                n += 1;
            }
            if let Some(bytes) = self.store.content(&c) {
                stack.extend(chain_children(&bytes));
            }
        }
        Ok(n)
    }

    /// Unpin a whole file/folder — the cascade counterpart to `pin_chain`.
    pub async fn unpin_chain(&self, cid: Cid) -> anyhow::Result<usize> {
        let mut n = 0;
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![cid];
        while let Some(c) = stack.pop() {
            if !seen.insert(c.0) {
                continue;
            }
            // Decode the chain BEFORE unpinning (unpin keeps the object, but read
            // links while the object is still definitely held).
            if let Some(bytes) = self.store.content(&c) {
                stack.extend(chain_children(&bytes));
            }
            let _ = self.unpin(c).await;
            n += 1;
        }
        Ok(n)
    }

    /// Forget a whole file/folder locally — cascade `forget_local` over the chain
    /// so deleting a file drops its content/ciphertext too (no orphaned objects).
    pub async fn forget_chain(&self, cid: Cid) -> anyhow::Result<usize> {
        let mut n = 0;
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![cid];
        while let Some(c) = stack.pop() {
            if !seen.insert(c.0) {
                continue;
            }
            // Read the chain BEFORE forgetting — forget removes the bytes.
            if let Some(bytes) = self.store.content(&c) {
                stack.extend(chain_children(&bytes));
            }
            let _ = self.forget_local(c).await;
            n += 1;
        }
        Ok(n)
    }

    /// WANT a whole file/folder — cascade `want` over the chain so the keep-alive
    /// intent covers the content, not just the manifest (else the content could
    /// fade while the manifest stays). Cascades over the part of the chain we hold.
    pub async fn want_chain(&self, cid: Cid) -> anyhow::Result<usize> {
        let mut n = 0;
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![cid];
        while let Some(c) = stack.pop() {
            if !seen.insert(c.0) {
                continue;
            }
            if let Some(bytes) = self.store.content(&c) {
                stack.extend(chain_children(&bytes));
            }
            let _ = self.want(c).await;
            n += 1;
        }
        Ok(n)
    }

    /// UNWANT a whole file/folder — the cascade counterpart to `want_chain`.
    pub async fn unwant_chain(&self, cid: Cid) -> anyhow::Result<usize> {
        let mut n = 0;
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![cid];
        while let Some(c) = stack.pop() {
            if !seen.insert(c.0) {
                continue;
            }
            if let Some(bytes) = self.store.content(&c) {
                stack.extend(chain_children(&bytes));
            }
            let _ = self.unwant(c).await;
            n += 1;
        }
        Ok(n)
    }

    /// BAN a whole file/folder — cascade the tombstone over the chain so a banned
    /// file refuses to host BOTH the manifest and its content (else the content
    /// stays hostable). Decodes the chain BEFORE tombstoning removes the bytes.
    pub async fn ban_chain(&self, cid: Cid) -> anyhow::Result<usize> {
        let mut n = 0;
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![cid];
        while let Some(c) = stack.pop() {
            if !seen.insert(c.0) {
                continue;
            }
            if let Some(bytes) = self.store.content(&c) {
                stack.extend(chain_children(&bytes));
            }
            let _ = self.delete_local(c).await; // tombstone
            n += 1;
        }
        Ok(n)
    }

    /// UNBAN a whole file/folder. The ban removed the local bytes, so we can't
    /// decode the chain locally — re-fetch each object (the ban was LOCAL, so the
    /// network still serves it) to rediscover and un-tombstone the whole chain.
    pub async fn unban_chain(&self, cid: Cid) -> anyhow::Result<usize> {
        let mut n = 0;
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![cid];
        while let Some(c) = stack.pop() {
            if !seen.insert(c.0) {
                continue;
            }
            let _ = self.undelete(c).await; // lift the tombstone first…
            n += 1;
            // …then re-fetch (now permitted) to rediscover the chain's next links.
            if let Ok(bytes) = self.get(c, ConsumeMode::Drop).await {
                stack.extend(chain_children(&bytes));
            }
        }
        Ok(n)
    }

    /// Publish a CraftSQL page generation as a SYSTEM object. It rides the FULL
    /// normal lifecycle — erasure-coded, distributed, repaired, scaled, and
    /// **degraded** (surplus above the floor sheds when demand is cold) — but is
    /// NOT pinned (no whole-content copy on the owner). Instead a WANT keeps it
    /// alive network-wide so it never fades; the `system` marker excludes it from
    /// user commands + local eviction. Returns its CID.
    pub async fn publish_system(&self, data: &[u8]) -> anyhow::Result<Cid> {
        let report = self.publish_impl(data, false, true).await?;
        Ok(report.cid)
    }

    /// Release a system object back to the normal lifecycle (compaction dropping
    /// a superseded generation) — clear the marker locally AND tell current
    /// holders to do the same so the generation fades network-wide. Idempotent
    /// and re-sendable: re-calling it reaches holders that were offline before
    /// (churn), so `reannounce` re-runs it until no providers remain. Returns the
    /// number of holders still providing the (now-releasing) generation.
    pub async fn release_system(&self, cid: Cid) -> anyhow::Result<usize> {
        self.store.unmark_system(&cid)?;
        let providers = self.routing.resolve(cid).await.unwrap_or_default();
        let me = self.transport.node_id();
        let mut remaining = 0;
        for p in providers {
            if p.node_id == me {
                continue;
            }
            remaining += 1;
            let msg = wire::Message::ReleaseSystem(wire::ReleaseSystem { cid: cid.0 });
            let _ = self.request(&p.addr, &msg).await;
        }
        Ok(remaining)
    }

    /// WANT a CID: signal keep-alive interest to the network without holding
    /// it (the demand-independent survival signal; gates Fade). Local intent is
    /// persisted so it survives restart and is re-announced.
    pub async fn want(&self, cid: Cid) -> anyhow::Result<()> {
        guard_not_system(&self.store, &cid)?;
        self.store.set_want(cid)?;
        self.store.clear_cooldown(&cid); // manual want overrides eviction cooldown
        let _ = self.routing.announce_want(cid).await;
        Ok(())
    }

    /// Withdraw WANT for a CID.
    pub async fn unwant(&self, cid: Cid) -> anyhow::Result<()> {
        self.store.unset_want(&cid)?;
        let _ = self.routing.withdraw_want(cid).await;
        Ok(())
    }

    /// Lift a local ban: remove the tombstone so this node may host the CID
    /// again (operator reverses their own delete; content is re-fetched on
    /// demand).
    pub async fn undelete(&self, cid: Cid) -> anyhow::Result<()> {
        self.store.untombstone(&cid)?;
        Ok(())
    }

    /// Delete a CID from this node: tombstone it (blocks resurrection) and
    /// withdraw the provider record. (Signed network-wide DELETE propagation
    /// is a later obj sub-item.)
    pub async fn delete_local(&self, cid: Cid) -> anyhow::Result<()> {
        guard_not_system(&self.store, &cid)?;
        self.store.tombstone(cid)?;
        let _ = self.routing.withdraw(cid).await;
        Ok(())
    }

    /// Soft-forget this node's local copy: drop content + pieces (no tombstone, so
    /// it's re-fetchable/re-publishable) + stop advertising it. Used by the
    /// file-manager `delete` (vs `delete_local`/tombstone = ban).
    pub async fn forget_local(&self, cid: Cid) -> anyhow::Result<()> {
        guard_not_system(&self.store, &cid)?;
        self.store.forget(&cid)?;
        let _ = self.routing.withdraw(cid).await;
        Ok(())
    }

    /// Re-announce provider records for ALL content this node holds — pins,
    /// seed-cached content, AND plain coded pieces. Called on startup and
    /// periodically so held content stays discoverable across restart,
    /// churn, and tracker restart (foundation §6: re-announce before the
    /// provider-record TTL). Without this, a restarted node's holdings —
    /// pinned or not — silently become unreachable once their records
    /// expire, even though the bytes are on disk. Returns the count announced.
    pub async fn reannounce_providers(&self) -> usize {
        // TTL-aware PER-CID scheduling: re-announce a record only when it is DUE (past the
        // republish window for its DHT TTL), not every cycle. Steady-state → near-zero
        // re-announces; startup/restart (empty schedule) → refresh everything once.
        let now = self.now_millis();
        let records: Vec<(Cid, u32, bool)> = {
            let sched = self.announced_at.lock().expect("announced_at");
            self.store
                .cids()
                .into_iter()
                .filter_map(|cid| {
                    let due = sched
                        .get(&cid.0)
                        .is_none_or(|t| now.saturating_sub(*t) >= REPUBLISH_MS);
                    if !due {
                        return None;
                    }
                    let count = self.store.piece_count(&cid) as u32;
                    let pinned = self.store.is_pinned(&cid);
                    (count > 0 || self.store.has_content(&cid)).then_some((cid, count, pinned))
                })
                .collect()
        };
        // Trickle the due announces in small chunks of PACE_CHUNK with a delay between — the
        // startup/restart refresh (when everything is due) is otherwise an O(N) put burst.
        // Scales to thousands of cids; steady-state is near-zero due each cycle anyway.
        let mut announced = 0usize;
        for chunk in records.chunks(PACE_CHUNK) {
            let results = futures::future::join_all(chunk.iter().map(|(cid, count, pinned)| {
                let (cid, count, pinned) = (*cid, *count, *pinned);
                async move { self.routing.announce(cid, count, pinned).await.is_ok() }
            }))
            .await;
            {
                let mut sched = self.announced_at.lock().expect("announced_at");
                for ((cid, _, _), ok) in chunk.iter().zip(&results) {
                    if *ok {
                        sched.insert(cid.0, now);
                        announced += 1;
                    }
                }
            }
            tokio::time::sleep(self.config.pace_delay).await;
        }
        // WANT interest (the node's own wants — few); re-announce each cycle (cheap).
        futures::future::join_all(
            self.store
                .wanted_cids()
                .into_iter()
                .map(|cid| async move { self.routing.announce_want(cid).await }),
        )
        .await;
        announced
    }

    /// Enforce the storage quota: if used bytes exceed capacity, evict LRU
    /// non-pinned content down to 90% (each eviction starts a cooldown so it
    /// isn't immediately refilled), then purge expired cooldown records.
    pub async fn enforce_quota(&self) {
        let cap = self.config.capacity_bytes;
        let used = self.store.stats().bytes;
        if cap > 0 && used > cap {
            self.emit(zeph_events::Event::DiskWatermarkHit { used, cap });
            let freed = self.store.evict_to(cap * 9 / 10).unwrap_or(0);
            if freed > 0 {
                tracing::info!(freed, "evicted under disk pressure");
            }
        }
        self.store.purge_cooldown(self.config.eviction_cooldown);
    }

    /// One HealthScan pass — the bidirectional control loop's *upward* half
    /// (Repair). For each held CID: measure VERIFIED availability (HAVE) via
    /// live probes to each provider — provider records are only candidates, so
    /// dead holders (which never answer) drop out of the count. Compare HAVE to
    /// the durability floor n = target_pieces(k). The floor is maintained even
    /// under a pin (pin ≠ spread); pinners repair+distribute but never degrade.
    /// Below floor ⇒ data-at-risk: if this node is rendezvous-elected among the
    /// live capable holders, it mints one fresh piece and pushes it to a peer
    /// that isn't yet a holder (HAVE ↑ toward the floor). Degradation (HAVE ↓)
    /// and WANT-gated fade are the loop's later halves.
    /// Wire the liveness source (membership in production; a test source in tests). Providers
    /// not in this set are treated as gone by the health scan.
    pub fn set_liveness(&self, src: Arc<dyn PeerSource>) {
        *self.liveness.lock().expect("liveness") = Some(src);
    }

    /// Currently-alive peer node-ids (cached ~10s). None if no liveness source is wired (then the
    /// health scan does not filter — legacy behaviour).
    async fn alive_peers(&self) -> Option<HashSet<NodeId>> {
        let src = self.liveness.lock().expect("liveness").clone()?;
        {
            let c = self.alive_cache.lock().expect("alive_cache");
            if let Some((t, set)) = c.as_ref() {
                if t.elapsed() < ALIVE_CACHE {
                    return Some(set.clone());
                }
            }
        }
        let set: HashSet<NodeId> = src.peers().await.into_iter().map(|(id, _)| id).collect();
        *self.alive_cache.lock().expect("alive_cache") = Some((Instant::now(), set.clone()));
        Some(set)
    }

    /// Liveness fallback when no membership source is wired: probe the holder with a short
    /// connect (cached ~10s). A killed node whose transport is closed fails to connect → dead.
    async fn probe_alive(&self, id: NodeId, addr: &PeerAddr) -> bool {
        {
            let c = self.node_liveness.lock().expect("node_liveness");
            if let Some((t, alive)) = c.get(&id) {
                if t.elapsed() < ALIVE_CACHE {
                    return *alive;
                }
            }
        }
        let alive = tokio::time::timeout(PROBE_TIMEOUT, self.transport.connect(addr, ALPN))
            .await
            .map(|r| r.is_ok())
            .unwrap_or(false);
        self.node_liveness
            .lock()
            .expect("node_liveness")
            .insert(id, (Instant::now(), alive));
        alive
    }

    /// Scan ONE small chunk of cids — a coordinator-managed unit. Callers submit many of these,
    /// paced, so no single job sweeps the whole set or holds a slot while it sleeps. Verdicts
    /// roll into the engine's persistent at-risk / fading sets, so `report.at_risk`/`fading` are
    /// the accurate GLOBAL counts aggregated across every chunk, and `scanned` is the total held.
    pub async fn health_scan_chunk(&self, cids: &[Cid]) -> HealthReport {
        let mut report = HealthReport::default();
        let epoch = self.transport.clock().now().0 / HEALTH_EPOCH_MS;
        let me = self.transport.node_id();
        let mut chunk_at_risk: HashSet<[u8; 32]> = HashSet::new();
        let mut chunk_fading: HashSet<[u8; 32]> = HashSet::new();
        let mut chunk_surplus: HashSet<[u8; 32]> = HashSet::new();
        let resolved = futures::future::join_all(cids.iter().map(|cid| {
            let cid = *cid;
            async move { (cid, self.routing.resolve(cid).await.unwrap_or_default()) }
        }))
        .await;
        let alive_set = self.alive_peers().await;

        for (cid, providers) in resolved {
            if self.store.is_tombstoned(&cid) {
                continue;
            }
            let Some(gen) = self.store.generation(&cid) else {
                continue;
            };
            report.scanned += 1;
            let floor = target_pieces(gen.k as usize);

            // Availability from live provider records (no per-cid probe). Record holders
            // (addr + count) to both elect a repairer and target one.
            let mut have = 0usize;
            let mut capable: Vec<NodeId> = Vec::new();
            let mut live: Vec<(NodeId, PeerAddr, u32, bool)> = Vec::new();
            let mut seen_self = false;
            if providers.iter().any(|p| p.node_id == me) {
                seen_self = true;
                let c = self.store.piece_count(&cid);
                if c > 0 || self.store.has_content(&cid) {
                    have += c;
                    if c >= 2 || (self.store.has_content(&cid) && self.store.is_pinned(&cid)) {
                        capable.push(me);
                    }
                }
            }
            // Read provider RECORDS directly — NO per-cid network probe. `resolve` already
            // returns only live (non-expired) provider records with their piece counts, so
            // probing each turned a single pass into 186 x providers x 2s timeouts. A repair
            // PUSH verifies a holder's reachability at the moment it matters, so records are
            // the right, fast signal for a periodic maintenance scan.
            for p in &providers {
                if p.node_id == me {
                    continue;
                }
                // Skip holders no longer alive: a dead holder's record lingers until TTL but its
                // pieces are gone, so counting it would suppress a needed repair. Use the wired
                // liveness source (membership) if present, else fall back to a cached probe.
                let is_alive = match &alive_set {
                    Some(set) => set.contains(&p.node_id),
                    None => self.probe_alive(p.node_id, &p.addr).await,
                };
                if !is_alive {
                    continue;
                }
                have += p.piece_count as usize;
                if p.piece_count >= 2 || p.pinned {
                    capable.push(p.node_id);
                }
                live.push((p.node_id, p.addr.clone(), p.piece_count, p.pinned));
            }
            // Include self if we hold but aren't (yet) in the provider records.
            if !seen_self {
                let c = self.store.piece_count(&cid);
                if c > 0 || self.store.has_content(&cid) {
                    have += c;
                    if c >= 2 || (self.store.has_content(&cid) && self.store.is_pinned(&cid)) {
                        capable.push(me);
                    }
                }
            }

            // OFFLOAD (tail of the durability gate): a copy we retained for durability —
            // we hold the whole content, but it is NOT user-pinned or user-wanted — may be
            // dropped ONLY once the network holds the FULL baseline erasure set on OTHER
            // nodes: >= `floor` (= target_pieces(k) = n) coded pieces across live peers.
            // Releasing merely at k distinct holders is zero-margin — each could hold a
            // single piece, and losing one node would drop below the decode threshold.
            // Requiring the full erasure set means peers can lose n - k pieces and still
            // reconstruct (and repair among themselves) before we ever drop out.
            let have_others: usize = live.iter().map(|(_, _, c, _)| *c as usize).sum();
            if self.store.has_content(&cid)
                && !self.store.is_pinned(&cid)
                && !self.store.is_wanted(&cid)
                && have_others >= floor
            {
                let _ = self.store.forget(&cid);
                report.offloaded += 1;
                self.record_health(
                    &cid,
                    have,
                    floor,
                    live.len(),
                    "durable on peers — full erasure set present",
                    "offloaded — dropped our retained copy",
                );
                continue;
            }

            // The distributed floor is maintained REGARDLESS of pins (a pin is
            // not a substitute for spread): `have` is the coded-piece count, and
            // a pinner participates in repair as a mint source below.
            let effective = have;
            // Hysteresis deadband around the floor so measurement wobble (repair's 1-piece step,
            // ~2s record lag, liveness jitter) doesn't make repair and shed fight at the exact
            // floor. Repair below the band, shed above it, hold steady inside it.
            let delta = (floor / 8).max(2);
            let high = floor + delta;
            let low = floor.saturating_sub(delta);
            // SURPLUS side (Schmitt): once COLD surplus rises above the band it sheds all the
            // way back to the FLOOR (band centre) — not just to the band top — symmetric with
            // repair, so it rests with ±Δ of margin. Warm surplus is kept for serving bandwidth.
            let cold = self.served_pulls(&cid) < self.config.degrade_threshold;
            let shedding =
                cold && (effective > high || (self.is_surplus(&cid) && effective > floor));
            if shedding {
                chunk_surplus.insert(cid.0);
                let mut action = "none — not the elected shedder";
                let mut shedders: Vec<NodeId> = live
                    .iter()
                    .filter(|(_, _, c, pinned)| *c > 2 && !*pinned)
                    .map(|(id, _, _, _)| *id)
                    .collect();
                if self.store.piece_count(&cid) > 2 && !self.store.is_pinned(&cid) {
                    shedders.push(me);
                }
                let winner = shedders
                    .iter()
                    .max_by_key(|id| rendezvous_score(id, &cid, epoch));
                if winner == Some(&me) && self.shed_one(&cid) {
                    report.degraded += 1;
                    action = "degraded — shed 1 piece toward the floor";
                }
                self.record_health(
                    &cid,
                    have,
                    floor,
                    live.len(),
                    "surplus — shedding toward floor (cold)",
                    action,
                );
                continue;
            }
            if effective > high {
                self.record_health(
                    &cid,
                    have,
                    floor,
                    live.len(),
                    "surplus above band — demand warm",
                    "none — kept for serving bandwidth",
                );
                continue;
            }
            // Schmitt hysteresis: a cid that WENT at-risk keeps repairing until it climbs back
            // to the floor (band CENTRE), so it rests with ±Δ of margin on each side; a cid not
            // under repair is left alone anywhere in the band. This is what stops the ±1
            // repair/shed flap — small wobble around the floor no longer flips the decision.
            let recovering = self.is_at_risk(&cid) && effective < floor;
            if effective >= low && !recovering {
                self.record_health(
                    &cid,
                    have,
                    floor,
                    live.len(),
                    "within durability band (floor ± Δ)",
                    "none — stable",
                );
                continue; // inside the deadband — nothing to do
            }

            // FADE: content nothing wants — no pin, no want, no live demand — is
            // NOT repaired; churn erodes it (passive death). Fail-safe: a later
            // WANT/pin resumes repair (the pieces are still there). Holding alone
            // is no longer implicit want.
            let alive = self
                .is_alive(&cid, providers.iter().any(|p| p.pinned))
                .await;
            if !alive {
                chunk_fading.insert(cid.0);
                report.fading += 1;
                self.record_health(
                    &cid,
                    have,
                    floor,
                    live.len(),
                    "below band but unwanted & undemanded",
                    "none — left to fade",
                );
                continue;
            }
            chunk_at_risk.insert(cid.0);
            report.at_risk += 1;

            // Only a capable holder can repair; rendezvous-elect exactly one so
            // holders don't all repair at once (thundering herd).
            // A DURABILITY-RETAINED copy (we hold the content but it is NOT user-pinned)
            // is a PASSIVE backup — it must NOT drive repair. Otherwise every retained
            // system generation makes us mint + push fresh coded pieces each pass, and the
            // cluster accumulates pieces without bound. Only pinned content (or a real
            // piece-holder) repairs; the retained copy just waits, safe.
            let can_i = self.store.piece_count(&cid) >= 2
                || (self.store.has_content(&cid) && self.store.is_pinned(&cid));
            if !can_i {
                self.record_health(
                    &cid,
                    have,
                    floor,
                    live.len(),
                    "below band — under-replicated",
                    "none — we can't repair (retained/passive copy)",
                );
                continue;
            }
            let winner = capable
                .iter()
                .max_by_key(|id| rendezvous_score(id, &cid, epoch));
            if winner != Some(&me) {
                self.record_health(
                    &cid,
                    have,
                    floor,
                    live.len(),
                    "below band — under-replicated",
                    "none — another holder is the elected repairer",
                );
                continue;
            }
            // Repair: mint one fresh piece and push it to the live holder that
            // most needs it (fewest pieces). Falls back to a non-holder peer if
            // no other live holder exists (sole survivor recruiting a new one).
            self.emit(zeph_events::Event::RepairNeeded(cid));
            if self.repair_one(&cid, &gen, &live).await {
                report.repaired += 1;
                self.record_health(
                    &cid,
                    have,
                    floor,
                    live.len(),
                    "below band — under-replicated",
                    "repaired — minted + pushed 1 piece",
                );
            } else {
                self.record_health(
                    &cid,
                    have,
                    floor,
                    live.len(),
                    "below band — under-replicated",
                    "repair failed — no reachable target",
                );
            }
        }
        // Roll this chunk's verdicts into the persistent GLOBAL sets — add cids that came out
        // at-risk / fading, clear chunk cids that are now healthy — then report the global
        // counts + the total held set, so the dashboard aggregates correctly across chunks.
        {
            let mut ar = self.at_risk_ids.lock().expect("at_risk_ids");
            let mut fd = self.fading_ids.lock().expect("fading_ids");
            let mut su = self.surplus_ids.lock().expect("surplus_ids");
            for cid in cids {
                if chunk_at_risk.contains(&cid.0) {
                    ar.insert(cid.0);
                } else {
                    ar.remove(&cid.0);
                }
                if chunk_fading.contains(&cid.0) {
                    fd.insert(cid.0);
                } else {
                    fd.remove(&cid.0);
                }
                if chunk_surplus.contains(&cid.0) {
                    su.insert(cid.0);
                } else {
                    su.remove(&cid.0);
                }
            }
            report.at_risk = ar.len();
            report.fading = fd.len();
            report.surplus = su.len();
        }
        report.scanned = self.store.cids().len();
        report
    }

    /// Scan the ENTIRE held set as a single chunk. Convenience for tests + one-shot callers; the
    /// node's periodic path submits per-chunk `health_scan_chunk` jobs (see noded) instead, so no
    /// job ever sweeps the whole set at once.
    pub async fn health_scan(&self) -> HealthReport {
        self.health_scan_chunk(&self.store.cids()).await
    }

    /// Is this cid currently believed at-risk (per its last scan)? Drives the scheduler's
    /// adaptive re-check backoff — at-risk cids stay hot, healthy cids back off.
    pub fn is_at_risk(&self, cid: &Cid) -> bool {
        self.at_risk_ids
            .lock()
            .expect("at_risk_ids")
            .contains(&cid.0)
    }

    /// Is this cid currently being left to fade (unwanted, not repaired)?
    pub fn is_fading(&self, cid: &Cid) -> bool {
        self.fading_ids.lock().expect("fading_ids").contains(&cid.0)
    }

    /// Is this cid currently shedding cold surplus back toward the floor?
    pub fn is_surplus(&self, cid: &Cid) -> bool {
        self.surplus_ids
            .lock()
            .expect("surplus_ids")
            .contains(&cid.0)
    }

    /// Is this cid actively CONVERGING toward the floor and so worth re-scanning frequently?
    /// True while REPAIRING (below floor) or SHEDDING cold surplus (above floor + demand cold);
    /// false once stable (at the floor, warm surplus kept for bandwidth, or fading). Drives the
    /// scheduler's backoff so shedding keeps pace with repair instead of drifting out to the cap.
    pub fn converging(&self, cid: &Cid) -> bool {
        // Repairing back up toward the floor, or shedding cold surplus back down to it (Schmitt).
        if self.is_at_risk(cid) || self.is_surplus(cid) {
            return true;
        }
        match self.cid_health(cid) {
            Some(h) if h.floor > 0 && h.effective > h.floor + (h.floor / 8).max(2) => {
                self.served_pulls(cid) < self.config.degrade_threshold // above band — shed if cold
            }
            _ => false,
        }
    }

    /// The last-scan health snapshot for a cid, if scanned.
    pub fn cid_health(&self, cid: &Cid) -> Option<CidHealth> {
        self.cid_health
            .lock()
            .expect("cid_health")
            .get(&cid.0)
            .cloned()
    }

    /// Record the scan's decision + action for a cid (dashboard diagnostics).
    fn record_health(
        &self,
        cid: &Cid,
        eff: usize,
        floor: usize,
        live: usize,
        decision: &str,
        action: &str,
    ) {
        self.cid_health.lock().expect("cid_health").insert(
            cid.0,
            CidHealth {
                last_scan_ms: self.transport.clock().now().millis(),
                effective: eff as u32,
                floor: floor as u32,
                live_providers: live as u32,
                decision: decision.to_string(),
                action: action.to_string(),
            },
        );
    }

    /// Recompute the "pending distribution" snapshot CHEAPLY — from provider RECORDS
    /// (their claimed piece_count via `resolve`, no per-peer probe), so it stays fresh even
    /// when the verified health scan is slow. For each copy we retain (whole content, not
    /// user-pinned) whose OTHER-node pieces are below the erasure floor, record its
    /// progress. This is what the publisher is still holding + spreading.
    pub async fn distribute_pending(&self) {
        let me = self.transport.node_id();
        let candidates: Vec<PeerAddr> = self
            .peer_source
            .peers()
            .await
            .into_iter()
            .filter(|(id, _)| *id != me)
            .map(|(_, addr)| addr)
            .collect();
        // "Complete" = reached as many distinct peers as the durability target OR the whole
        // cluster can offer — then never pushed again.
        let target = self
            .config
            .durability_threshold
            .min(candidates.len().max(1));
        let mut out: Vec<([u8; 32], u32, u32)> = Vec::new();
        for cid in self.store.cids() {
            // Everything we hold whole (files + db/app generations) that is not yet complete.
            if self.store.is_tombstoned(&cid)
                || !self.store.has_content(&cid)
                || self.store.is_distributed(&cid)
            {
                continue;
            }
            let Some(gen) = self.store.generation(&cid) else {
                continue;
            };
            let floor = target_pieces(gen.k as usize);
            let have_others: usize = self
                .routing
                .resolve(cid)
                .await
                .unwrap_or_default()
                .iter()
                .filter(|p| p.node_id != me)
                .map(|p| p.piece_count as usize)
                .sum();
            if have_others >= floor {
                let _ = self.store.set_distributed(cid);
                continue;
            }
            if candidates.is_empty() {
                out.push((cid.0, have_others as u32, floor as u32));
                continue;
            }
            // Complete the distribution: mint the deficit from our retained content and push
            // it toward the floor. This is the tail of PUBLISH (make sure the erasure is
            // pushed), NOT ongoing repair — once it reaches the target it is marked complete
            // and never pushed again, so it cannot grow the cluster without bound. Pushes
            // that fail land nothing (no growth); pushes that succeed converge to the floor.
            let deficit = (floor - have_others).min(floor);
            let pieces = self
                .store
                .serve_pieces(&cid, &HashSet::new(), deficit)
                .unwrap_or_default();
            let mut distinct = HashSet::new();
            for (i, piece) in pieces.iter().enumerate() {
                let peer = &candidates[i % candidates.len()];
                if tokio::time::timeout(PUSH_TIMEOUT, self.push_piece(peer, cid, &gen, piece))
                    .await
                    .map(|r| r.is_ok())
                    .unwrap_or(false)
                {
                    distinct.insert(peer.node_id());
                }
            }
            if distinct.len() >= target {
                let _ = self.store.set_distributed(cid);
            } else {
                out.push((cid.0, (have_others + distinct.len()) as u32, floor as u32));
            }
        }
        out.sort_by_key(|(_, have, _)| *have); // least-durable first
        *self.pending.lock().expect("pending") = out;
    }

    /// The last "pending distribution" snapshot: (cid, pieces on OTHER nodes, floor).
    pub fn pending_durability(&self) -> Vec<([u8; 32], u32, u32)> {
        self.pending.lock().expect("pending").clone()
    }

    /// One Distribution pass — the spin-up / spread behavior. For each CID
    /// this node is over-concentrated on (holds > 2 coded pieces), find the
    /// least-full LIVE peer and MOVE one piece to it: push, then delete our
    /// copy only after the receiver acks. Unlike Repair, this creates no new
    /// pieces (total availability is conserved) — it spreads existing
    /// redundancy across more distinct nodes (better fault geometry) and is how
    /// a freshly-joined empty node gets populated. Moves only when it strictly
    /// improves balance (target has ≥2 fewer pieces), so it converges and never
    /// oscillates; a node always retains ≥2 pieces (stays repair-eligible).
    pub async fn distribute(&self) -> DistributeReport {
        let mut report = DistributeReport::default();
        let me = self.transport.node_id();
        // Live peers ONCE (was re-fetched per cid), and pre-resolve providers for every
        // candidate CONCURRENTLY (was a per-cid resolve + a probe of every node for every
        // cid — O(cids x nodes) round-trips that made a pass take minutes).
        let peers: Vec<(NodeId, PeerAddr)> = self
            .peer_source
            .peers()
            .await
            .into_iter()
            .filter(|(id, _)| *id != me)
            .collect();
        let candidates: Vec<Cid> = self
            .store
            .cids()
            .into_iter()
            .filter(|c| !self.store.is_tombstoned(c) && self.store.piece_count(c) > 2)
            .collect();
        let resolved =
            futures::future::join_all(candidates.into_iter().map(|cid| async move {
                (cid, self.routing.resolve(cid).await.unwrap_or_default())
            }))
            .await;

        for (cid, providers) in resolved {
            let my_pieces = self.store.piece_count(&cid);
            if my_pieces <= 2 {
                continue;
            }
            let Some(gen) = self.store.generation(&cid) else {
                continue;
            };
            // FADE-gate: don't spread content nothing wants — let it stay cold so
            // churn/eviction reclaim it (no bandwidth on dead content).
            if !self
                .is_alive(&cid, providers.iter().any(|p| p.pinned))
                .await
            {
                continue;
            }
            report.scanned += 1;

            // Balance this CID WITHIN the pass. Seed a local belief of each peer's piece count
            // from the (possibly stale) provider records, then move pieces to the least-full
            // peer, updating the belief as we go — so we CONVERGE in a single pass regardless
            // of record freshness. Records only seed the starting estimate; the belief tracks
            // our own moves, so stale records no longer pile every piece on the first holder.
            let mut belief: HashMap<NodeId, u32> = peers
                .iter()
                .map(|(id, _)| {
                    let c = providers
                        .iter()
                        .find(|p| p.node_id == *id)
                        .map_or(0, |p| p.piece_count);
                    (*id, c)
                })
                .collect();
            let mut mine = my_pieces;
            loop {
                let Some((tid, taddr, tcount)) = peers
                    .iter()
                    .map(|(id, addr)| (*id, addr, *belief.get(id).unwrap_or(&0)))
                    .min_by_key(|(_, _, c)| *c)
                else {
                    break;
                };
                // Stop when a move would no longer strictly reduce imbalance (no ping-pong);
                // keep >=2 pieces locally so we stay repair-eligible.
                if tcount as usize + 1 >= mine || mine <= 2 {
                    break;
                }
                // MOVE one stored piece: push it, delete locally on ack, credit the belief.
                let Ok(held) = self.store.serve_pieces(&cid, &HashSet::new(), 1) else {
                    break;
                };
                let Some(piece) = held.into_iter().next() else {
                    break;
                };
                let pid = piece.piece_id();
                if self.push_piece(taddr, cid, &gen, &piece).await.is_ok() {
                    let _ = self.store.remove_piece(&cid, &pid);
                    *belief.entry(tid).or_insert(0) += 1;
                    mine -= 1;
                    report.moved += 1;
                } else {
                    break;
                }
            }
        }
        report
    }

    /// One Scaling pass — the demand-driven UPWARD behavior. Demand here is
    /// ACTUAL download traffic: the count of piece-pull requests this node
    /// served for a CID since the last pass (NOT the WANT interest signal). A
    /// CID whose pull rate exceeds `scale_threshold` is "hot"; if it hasn't
    /// already reached the provider ceiling, this loaded provider recruits ONE
    /// more (mints a fresh piece and pushes it to a live non-holder) so future
    /// downloads have another source — bandwidth headroom above the durability
    /// floor. Bounded to one recruit per hot CID per cycle (periodic principle);
    /// when demand fades, Degradation sheds the extra providers.
    /// Wire the demand-driven scale trigger (the node passes the sender; a worker drains it).
    pub fn set_scale_trigger(&self, tx: tokio::sync::mpsc::UnboundedSender<Cid>) {
        let _ = self.scale_trigger.set(tx);
    }

    /// Periodic backstop + demand-window reset: snapshot & clear the pull window, recruit for
    /// anything still hot. The INSTANT path is demand-driven (scale_one, fired on the serve path
    /// the moment a pull crosses scale_threshold) — this just resets the window and mops up any
    /// residue, so it is no longer the primary scaling trigger and never gates on a scan.
    pub async fn scale(&self) -> ScaleReport {
        let mut report = ScaleReport::default();
        let demand: HashMap<[u8; 32], u32> = {
            let mut d = self.demand.lock().expect("demand");
            std::mem::take(&mut *d)
        };
        for (cid_bytes, pulls) in demand {
            if pulls < self.config.scale_threshold {
                continue;
            }
            report.hot += 1;
            if self.scale_one(Cid(cid_bytes)).await {
                report.scaled += 1;
            }
        }
        report
    }

    /// Recruit ONE additional provider for a hot CID (more replicas → more serving bandwidth), up
    /// to the intrinsic ceiling: a CID of n pieces spreads across at most n/2 providers (each
    /// holds ≥2), so bigger content naturally gets more providers. Demand-driven — fired the
    /// instant a pull crosses scale_threshold, with no scan/distribute cadence gating it.
    pub async fn scale_one(&self, cid: Cid) -> bool {
        let me = self.transport.node_id();
        if self.store.is_tombstoned(&cid) {
            return false;
        }
        // Must be able to mint a fresh piece to hand out.
        if !(self.store.piece_count(&cid) >= 2 || self.store.has_content(&cid)) {
            return false;
        }
        let Some(gen) = self.store.generation(&cid) else {
            return false;
        };
        let providers = self.routing.resolve(cid).await.unwrap_or_default();
        let max_providers = (target_pieces(gen.k as usize) / 2).max(1);
        if providers.len() >= max_providers {
            return false;
        }
        let provider_ids: HashSet<NodeId> = providers.iter().map(|p| p.node_id).collect();
        let Some(piece) = self.mint_piece(&cid, &gen) else {
            return false;
        };
        // Recruit one new provider: a live peer not already holding the CID.
        for (id, addr) in self.peer_source.peers().await {
            if id == me || provider_ids.contains(&id) {
                continue;
            }
            if self.push_piece(&addr, cid, &gen, &piece).await.is_ok() {
                return true;
            }
        }
        false
    }

    /// Drop one stored coded piece (Degradation MOVE-less shed — reduces
    /// surplus toward the floor). Never touches pinned whole content.
    fn shed_one(&self, cid: &Cid) -> bool {
        let Ok(held) = self.store.serve_pieces(cid, &HashSet::new(), 1) else {
            return false;
        };
        let Some(piece) = held.into_iter().next() else {
            return false;
        };
        self.store
            .remove_piece(cid, &piece.piece_id())
            .unwrap_or(false)
    }

    /// Mint one fresh coded piece from local holdings: recode held pieces (a
    /// new independent combination), or — if we hold the whole content (pin) —
    /// encode a fresh piece from sources (serve_pieces does this on demand).
    fn mint_piece(&self, cid: &Cid, gen: &Generation) -> Option<CodedPiece> {
        let held = self
            .store
            .serve_pieces(cid, &HashSet::new(), gen.k as usize)
            .ok()?;
        if held.is_empty() {
            return None;
        }
        if self.store.has_content(cid) {
            held.into_iter().next()
        } else {
            let mut rng = OsRng;
            recode(&held, &mut rng).ok()
        }
    }

    /// Push one freshly-minted piece to raise availability toward the floor.
    /// Primary target: the live holder that most needs it (fewest pieces) — a
    /// 1-piece holder becomes repair-eligible at 2. Fallback (no other live
    /// holder — sole survivor): recruit a fresh non-holder peer from the node
    /// registry. Dead peers never answer probes, so a piece is never wasted on
    /// a vanished node.
    async fn repair_one(
        &self,
        cid: &Cid,
        gen: &Generation,
        live: &[(NodeId, PeerAddr, u32, bool)],
    ) -> bool {
        let Some(piece) = self.mint_piece(cid, gen) else {
            return false;
        };
        let me = self.transport.node_id();
        // Fewest-piece live holder other than self.
        if let Some((_, addr, _, _)) = live
            .iter()
            .filter(|(id, _, _, _)| *id != me)
            .min_by_key(|(_, _, c, _)| *c)
        {
            return self.push_piece(addr, *cid, gen, &piece).await.is_ok();
        }
        // Sole survivor: recruit a brand-new holder from the node registry.
        let holder_ids: HashSet<NodeId> = live.iter().map(|(id, _, _, _)| *id).collect();
        for (id, addr) in self.peer_source.peers().await {
            if id == me || holder_ids.contains(&id) {
                continue;
            }
            if self.push_piece(&addr, *cid, gen, &piece).await.is_ok() {
                return true;
            }
        }
        false
    }

    /// Serve the piece ALPN: ingest pushes (vtag-verify → store → announce)
    /// and answer requests from the store.
    pub async fn serve(self: Arc<Self>, mut conns: tokio::sync::mpsc::Receiver<Connection>) {
        while let Some(conn) = conns.recv().await {
            let engine = self.clone();
            tokio::spawn(async move {
                while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    let Ok(bytes) = recv.read_to_end(MAX_FRAME).await else {
                        return;
                    };
                    let Ok(frame) = wire::decode(&bytes) else {
                        return;
                    };
                    let reply = engine.handle(frame.message).await;
                    let _ = send
                        .write_all(&wire::encode(&reply, engine.transport.clock().now().0))
                        .await;
                    let _ = send.finish();
                }
            });
        }
    }

    async fn handle(&self, msg: wire::Message) -> wire::Message {
        match msg {
            wire::Message::PiecePush(push) => wire::Message::PiecePushAck(self.ingest(push).await),
            wire::Message::ReleaseSystem(r) => {
                // A CraftSQL generation was superseded by compaction: drop the
                // system marker so it returns to the normal lifecycle and fades.
                let _ = self.store.unmark_system(&Cid(r.cid));
                wire::Message::PiecePushAck(wire::PiecePushAck {
                    ok: true,
                    reason: String::new(),
                })
            }
            wire::Message::PieceRequest(req) => {
                let cid = Cid(req.cid);
                let exclude: HashSet<[u8; 32]> = req.exclude.into_iter().collect();
                let gen = self.store.generation(&cid);
                let pieces = self
                    .store
                    .serve_pieces(&cid, &exclude, req.max_pieces as usize)
                    .unwrap_or_default();
                // Record a real fetch: demand (drives Scaling) + last-served
                // recency (drives Fade — serve-only, not lifecycle writes).
                if !pieces.is_empty() {
                    // Count the pull; the MOMENT it crosses scale_threshold, fire a demand-driven
                    // scale for this cid (and reset its window) — replication reacts to access
                    // instantly, independent of any scan/distribute cadence.
                    // Consume the count for an INSTANT trigger only when a trigger is wired;
                    // otherwise let it accumulate for the periodic scale() backstop (tests).
                    let fire = {
                        let mut d = self.demand.lock().expect("demand");
                        let n = d.entry(cid.0).or_insert(0);
                        *n += 1;
                        if self.scale_trigger.get().is_some() && *n >= self.config.scale_threshold {
                            *n = 0;
                            true
                        } else {
                            false
                        }
                    };
                    if fire {
                        if let Some(tx) = self.scale_trigger.get() {
                            let _ = tx.send(cid);
                        }
                    }
                    self.last_served
                        .lock()
                        .expect("last_served")
                        .insert(cid.0, Instant::now());
                }
                let (k, piece_len, total_len, vtags) = gen
                    .map(|g| (g.k, g.piece_len, g.total_len, g.vtags))
                    .unwrap_or((0, 0, 0, Vec::new()));
                wire::Message::PieceResponse(wire::PieceResponse {
                    found: !pieces.is_empty(),
                    k,
                    piece_len,
                    total_len,
                    vtags,
                    pieces: pieces
                        .into_iter()
                        .map(|p| wire::WirePiece {
                            coding_vector: p.coding_vector,
                            data: p.data,
                        })
                        .collect(),
                })
            }
            wire::Message::AvailabilityProbe(probe) => {
                let cid = Cid(probe.cid);
                let piece_count = self.store.piece_count(&cid) as u32;
                let pinned = self.store.is_pinned(&cid);
                let has = piece_count > 0 || self.store.has_content(&cid);
                wire::Message::AvailabilityAck(wire::AvailabilityAck {
                    has,
                    piece_count,
                    pinned,
                })
            }
            _ => wire::Message::PiecePushAck(wire::PiecePushAck {
                ok: false,
                reason: "unexpected".into(),
            }),
        }
    }

    async fn ingest(&self, push: wire::PiecePush) -> wire::PiecePushAck {
        let reject = |reason: &str| wire::PiecePushAck {
            ok: false,
            reason: reason.to_string(),
        };
        let cid = Cid(push.cid);
        if self.store.is_tombstoned(&cid) {
            return reject("tombstoned");
        }
        if self
            .store
            .is_in_cooldown(&cid, self.config.eviction_cooldown)
        {
            return reject("in-cooldown");
        }
        let Ok(tags): Result<vtags::VTags, _> = postcard::from_bytes(&push.vtags) else {
            return reject("vtags-malformed");
        };
        let piece = CodedPiece {
            coding_vector: push.piece.coding_vector,
            data: push.piece.data,
        };
        // VERIFY AT INGEST — pollution never enters the store.
        if !vtags::verify(&tags, &piece) {
            return reject("vtag-invalid");
        }
        let gen = Generation {
            k: push.k,
            piece_len: push.piece_len,
            total_len: push.total_len,
            vtags: push.vtags,
        };
        let was_empty = self.store.piece_count(&cid) == 0;
        if self.store.put_generation(cid, gen).is_err()
            || self.store.put_piece(cid, &piece).is_err()
        {
            return reject("store-error");
        }
        // A CraftSQL system piece: mark it locally so this holder keeps it alive
        // (never fades) + exempt from user commands + eviction — no WANT record.
        if push.system {
            let _ = self.store.mark_system(&cid);
        }
        // Announce our provider record with the CURRENT piece count — immediately on the first
        // piece (become a provider) and thereafter DEBOUNCED per-cid. The health scan's
        // `effective` is the SUM of providers' record counts vs the floor; a holder that only ever
        // announced count=1 undercounts its real holdings, so `effective` stays below the floor
        // forever → perpetual repair that keeps MINTING pieces. Announcing the real count as it
        // grows lets `effective` reach the floor and repair converge; debounce avoids a flood.
        let now = self.now_millis();
        let due = was_empty || {
            let sched = self.announced_at.lock().expect("announced_at");
            sched
                .get(&cid.0)
                .is_none_or(|t| now.saturating_sub(*t) >= INGEST_ANNOUNCE_DEBOUNCE_MS)
        };
        if due {
            let count = self.store.piece_count(&cid) as u32;
            let pinned = self.store.is_pinned(&cid);
            if self.routing.announce(cid, count, pinned).await.is_ok() {
                self.announced_at
                    .lock()
                    .expect("announced_at")
                    .insert(cid.0, now);
            }
        }
        wire::PiecePushAck {
            ok: true,
            reason: String::new(),
        }
    }
}

/// Refuse a user lifecycle operation on a CraftSQL system object (DB generation).
fn guard_not_system(store: &Store, cid: &Cid) -> anyhow::Result<()> {
    anyhow::ensure!(
        !store.is_system(cid),
        "cid {} is a CraftSQL system object (DB-managed; not user-controllable)",
        cid.to_hex()
    );
    Ok(())
}

/// Objects a manifest/envelope directly links to: content (File), ciphertext
/// (envelope), or child entries (Dir). Empty if `bytes` is raw content. The basis
/// for cascading pin/unpin/forget over a whole file/folder chain.
fn chain_children(bytes: &[u8]) -> Vec<Cid> {
    if let Some(env) = EncryptedEnvelope::decode(bytes) {
        return vec![Cid(env.ciphertext_cid)];
    }
    match Manifest::decode(bytes) {
        Some(Manifest::File { content, .. }) => vec![Cid(content)],
        Some(Manifest::Dir { entries, .. }) => entries.iter().map(|e| Cid(e.cid)).collect(),
        None => Vec::new(),
    }
}

fn decoder_k(tags: &vtags::VTags) -> u32 {
    tags.k
}

/// Durability floor n for a generation of size k (re-exported for callers).
pub fn floor_for_k(k: usize) -> usize {
    target_pieces(k)
}

/// Rendezvous score for repair election: BLAKE3(node_id ‖ cid ‖ epoch).
/// Highest score among capable holders wins → exactly one repairer per epoch.
fn rendezvous_score(id: &NodeId, cid: &Cid, epoch: u64) -> [u8; 32] {
    let mut buf = Vec::with_capacity(72);
    buf.extend_from_slice(&id.0);
    buf.extend_from_slice(&cid.0);
    buf.extend_from_slice(&epoch.to_le_bytes());
    Cid::of(&buf).0
}
