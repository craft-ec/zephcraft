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

mod manifest;
pub use manifest::{Entry, Manifest};

/// ALPN for piece exchange.
pub const ALPN: &[u8] = b"/craftec/piece/1";

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

pub struct ObjEngine {
    transport: Arc<Transport>,
    store: Arc<Store>,
    routing: Arc<dyn ContentRouting>,
    config: ObjConfig,
    /// Observed download demand: pulls (served piece requests) per CID since
    /// the last Scaling pass. This is ACTUAL fetch traffic, not the WANT
    /// interest signal — Scaling responds to real downloads.
    demand: Mutex<HashMap<[u8; 32], u32>>,
    /// Monotonic time of the last real FETCH served per CID (serve-only — NOT
    /// bumped by lifecycle writes, unlike the store's LRU last_access). Fade's
    /// demand-recency signal.
    last_served: Mutex<HashMap<[u8; 32], Instant>>,
}

impl ObjEngine {
    pub fn new(
        transport: Arc<Transport>,
        store: Arc<Store>,
        routing: Arc<dyn ContentRouting>,
        config: ObjConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            transport,
            store,
            routing,
            config,
            demand: Mutex::new(HashMap::new()),
            last_served: Mutex::new(HashMap::new()),
        })
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    /// Content is "alive" — worth maintaining/spreading — iff it is pinned
    /// (locally or by a provider), wanted (locally or network-wide), or fetched
    /// within the fade grace window. Otherwise it fades (Repair + Distribution
    /// both skip it).
    fn is_alive(&self, cid: &Cid, wanted: &HashSet<[u8; 32]>, provider_pinned: bool) -> bool {
        self.store.is_pinned(cid)
            || self.store.is_wanted(cid)
            || wanted.contains(&cid.0)
            || provider_pinned
            || self
                .last_served
                .lock()
                .expect("last_served")
                .get(&cid.0)
                .is_some_and(|t| t.elapsed() < self.config.fade_grace)
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
        if pin {
            self.store.pin(cid, data)?;
        }

        // Candidate storage peers from the node registry (exclude self).
        let me = self.transport.node_id();
        let candidates: Vec<PeerAddr> = self
            .routing
            .nodes()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|(id, _)| *id != me)
            .filter_map(|(_, np)| np.addr.parse().ok())
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

        Ok(PublishReport {
            cid,
            pieces_pushed: pushed,
            distinct_peers: distinct.len(),
            durable: distinct.len() >= self.config.durability_threshold,
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

    /// Publish a CraftSQL page generation as a SYSTEM object — erasure-coded +
    /// distributed + repaired like content, but marked DB-managed so it's exempt
    /// from user pin/unpin/delete/want and from eviction. Returns its CID.
    pub async fn publish_system(&self, data: &[u8]) -> anyhow::Result<Cid> {
        let report = self.publish(data, true).await?;
        self.store.mark_system(&report.cid)?;
        Ok(report.cid)
    }

    /// Release a system object back to the normal lifecycle (compaction dropping
    /// a superseded generation) — clears the DB exemption + pin so it can fade.
    pub async fn release_system(&self, cid: Cid) -> anyhow::Result<()> {
        self.store.unmark_system(&cid)?;
        self.store.unpin(&cid)?;
        Ok(())
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

    /// Re-announce provider records for ALL content this node holds — pins,
    /// seed-cached content, AND plain coded pieces. Called on startup and
    /// periodically so held content stays discoverable across restart,
    /// churn, and tracker restart (foundation §6: re-announce before the
    /// provider-record TTL). Without this, a restarted node's holdings —
    /// pinned or not — silently become unreachable once their records
    /// expire, even though the bytes are on disk. Returns the count announced.
    pub async fn reannounce_providers(&self) -> usize {
        let mut announced = 0;
        for cid in self.store.cids() {
            let count = self.store.piece_count(&cid) as u32;
            let pinned = self.store.is_pinned(&cid);
            // A provider is any node that can serve the CID: it holds pieces,
            // OR the whole content (pin or seed cache → serves by encoding).
            if count == 0 && !self.store.has_content(&cid) {
                continue;
            }
            if self.routing.announce(cid, count, pinned).await.is_ok() {
                announced += 1;
            }
        }
        // Re-announce WANT interest (keep-alive intent survives TTL/restart).
        for cid in self.store.wanted_cids() {
            let _ = self.routing.announce_want(cid).await;
        }
        announced
    }

    /// Enforce the storage quota: if used bytes exceed capacity, evict LRU
    /// non-pinned content down to 90% (each eviction starts a cooldown so it
    /// isn't immediately refilled), then purge expired cooldown records.
    pub async fn enforce_quota(&self) {
        let cap = self.config.capacity_bytes;
        if cap > 0 && self.store.stats().bytes > cap {
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
    pub async fn health_scan(&self) -> HealthReport {
        let mut report = HealthReport::default();
        let epoch = self.transport.clock().now().0 / HEALTH_EPOCH_MS;
        let me = self.transport.node_id();
        // The set of CIDs the network WANTs — content outside it (and unpinned,
        // undemanded) is left to fade (FADE: absence of repair).
        let wanted: HashSet<[u8; 32]> = self
            .routing
            .wanted_cids()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|c| c.0)
            .collect();

        for cid in self.store.cids() {
            if self.store.is_tombstoned(&cid) {
                continue;
            }
            let Some(gen) = self.store.generation(&cid) else {
                continue;
            };
            report.scanned += 1;
            let floor = target_pieces(gen.k as usize);
            let providers = self.routing.resolve(cid).await.unwrap_or_default();

            // VERIFIED availability: probe each provider live; unreachable
            // holders simply don't answer and so don't count. Record live
            // holders (addr + count) to both elect a repairer and target one.
            let mut have = 0usize;
            let mut capable: Vec<NodeId> = Vec::new();
            let mut live: Vec<(NodeId, PeerAddr, u32, bool)> = Vec::new();
            let mut seen_self = false;
            for p in &providers {
                if p.node_id == me {
                    seen_self = true;
                    let c = self.store.piece_count(&cid);
                    if c > 0 || self.store.has_content(&cid) {
                        have += c;
                        if c >= 2 || self.store.has_content(&cid) {
                            capable.push(me);
                        }
                    }
                    continue;
                }
                let Some(ack) = self.probe_availability(&p.addr, cid).await else {
                    continue;
                };
                if !ack.has {
                    continue;
                }
                have += ack.piece_count as usize;
                if ack.piece_count >= 2 || ack.pinned {
                    capable.push(p.node_id);
                }
                live.push((p.node_id, p.addr.clone(), ack.piece_count, ack.pinned));
            }
            // Include self if we hold but aren't (yet) in the provider records.
            if !seen_self {
                let c = self.store.piece_count(&cid);
                if c > 0 || self.store.has_content(&cid) {
                    have += c;
                    if c >= 2 || self.store.has_content(&cid) {
                        capable.push(me);
                    }
                }
            }

            // The distributed floor is maintained REGARDLESS of pins (a pin is
            // not a substitute for spread): `have` is the coded-piece count, and
            // a pinner participates in repair as a mint source below.
            let effective = have;
            if effective > floor {
                // Surplus above the floor. If download demand has faded, DEGRADE:
                // shed one piece toward the floor. Rendezvous-elected (one
                // shedder per cycle) — the deliberately slow, conservative
                // direction, in contrast to parallel un-elected Scaling.
                if self.served_pulls(&cid) < self.config.degrade_threshold {
                    // Pinners NEVER degrade — they only create + distribute to
                    // prevent loss. Shedders are non-pinner holders with surplus.
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
                    }
                }
                continue;
            }
            if effective >= floor {
                continue; // exactly at the floor — nothing to do
            }

            // FADE: content nothing wants — no pin, no want, no live demand — is
            // NOT repaired; churn erodes it (passive death). Fail-safe: a later
            // WANT/pin resumes repair (the pieces are still there). Holding alone
            // is no longer implicit want.
            let alive = self.is_alive(&cid, &wanted, providers.iter().any(|p| p.pinned));
            if !alive {
                report.fading += 1;
                continue;
            }
            report.at_risk += 1;

            // Only a capable holder can repair; rendezvous-elect exactly one so
            // holders don't all repair at once (thundering herd).
            let can_i = self.store.piece_count(&cid) >= 2 || self.store.has_content(&cid);
            if !can_i {
                continue;
            }
            let winner = capable
                .iter()
                .max_by_key(|id| rendezvous_score(id, &cid, epoch));
            if winner != Some(&me) {
                continue;
            }
            // Repair: mint one fresh piece and push it to the live holder that
            // most needs it (fewest pieces). Falls back to a non-holder peer if
            // no other live holder exists (sole survivor recruiting a new one).
            if self.repair_one(&cid, &gen, &live).await {
                report.repaired += 1;
            }
        }
        report
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
        let wanted: HashSet<[u8; 32]> = self
            .routing
            .wanted_cids()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|c| c.0)
            .collect();
        for cid in self.store.cids() {
            if self.store.is_tombstoned(&cid) {
                continue;
            }
            let my_pieces = self.store.piece_count(&cid);
            if my_pieces <= 2 {
                continue;
            }
            let Some(gen) = self.store.generation(&cid) else {
                continue;
            };
            // FADE-gate: don't spread content nothing wants — let it stay cold
            // so churn/eviction reclaim it (no bandwidth on dead content).
            let provider_pinned = self
                .routing
                .resolve(cid)
                .await
                .unwrap_or_default()
                .iter()
                .any(|p| p.pinned);
            if !self.is_alive(&cid, &wanted, provider_pinned) {
                continue;
            }
            report.scanned += 1;

            // Least-full live peer (empty peers report piece_count 0).
            let mut best: Option<(PeerAddr, u32)> = None;
            for (id, np) in self.routing.nodes().await.unwrap_or_default() {
                if id == me {
                    continue;
                }
                let Ok(addr) = np.addr.parse::<PeerAddr>() else {
                    continue;
                };
                let Some(ack) = self.probe_availability(&addr, cid).await else {
                    continue;
                };
                if best.as_ref().is_none_or(|(_, c)| ack.piece_count < *c) {
                    best = Some((addr, ack.piece_count));
                }
            }
            let Some((addr, tcount)) = best else {
                continue;
            };
            // Move only if it strictly reduces imbalance (no ping-pong).
            if tcount as usize + 1 >= my_pieces {
                continue;
            }

            // MOVE one stored piece: push it, delete locally on ack.
            let Ok(held) = self.store.serve_pieces(&cid, &HashSet::new(), 1) else {
                continue;
            };
            let Some(piece) = held.into_iter().next() else {
                continue;
            };
            let pid = piece.piece_id();
            if self.push_piece(&addr, cid, &gen, &piece).await.is_ok() {
                let _ = self.store.remove_piece(&cid, &pid);
                report.moved += 1;
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
    pub async fn scale(&self) -> ScaleReport {
        let mut report = ScaleReport::default();
        let me = self.transport.node_id();
        // Snapshot + reset the demand window.
        let demand: HashMap<[u8; 32], u32> = {
            let mut d = self.demand.lock().expect("demand");
            std::mem::take(&mut *d)
        };
        for (cid_bytes, pulls) in demand {
            if pulls < self.config.scale_threshold {
                continue;
            }
            let cid = Cid(cid_bytes);
            if self.store.is_tombstoned(&cid) {
                continue;
            }
            // Must be able to mint a fresh piece to hand out.
            if !(self.store.piece_count(&cid) >= 2 || self.store.has_content(&cid)) {
                continue;
            }
            let Some(gen) = self.store.generation(&cid) else {
                continue;
            };
            report.hot += 1;

            let providers = self.routing.resolve(cid).await.unwrap_or_default();
            // Intrinsic ceiling (self-correcting, no network-size input): each
            // provider holds ≥2 pieces (the repair-recode minimum), so a CID of
            // n pieces spreads across at most n/2 providers. Bigger content has
            // more pieces → naturally more providers (bandwidth).
            let max_providers = (target_pieces(gen.k as usize) / 2).max(1);
            if providers.len() >= max_providers {
                continue;
            }
            let provider_ids: HashSet<NodeId> = providers.iter().map(|p| p.node_id).collect();
            let Some(piece) = self.mint_piece(&cid, &gen) else {
                continue;
            };
            // Recruit one new provider: a live peer not already holding the CID.
            for (id, np) in self.routing.nodes().await.unwrap_or_default() {
                if id == me || provider_ids.contains(&id) {
                    continue;
                }
                if let Ok(addr) = np.addr.parse::<PeerAddr>() {
                    if self.push_piece(&addr, cid, &gen, &piece).await.is_ok() {
                        report.scaled += 1;
                        break;
                    }
                }
            }
        }
        report
    }

    /// Live availability probe: "do you hold `cid`?" — no piece transfer.
    /// Short-timeout so a dead holder resolves to "unavailable" quickly rather
    /// than stalling the scan.
    async fn probe_availability(&self, peer: &PeerAddr, cid: Cid) -> Option<wire::AvailabilityAck> {
        let msg = wire::Message::AvailabilityProbe(wire::AvailabilityProbe { cid: cid.0 });
        match tokio::time::timeout(self.config.probe_timeout, self.request(peer, &msg)).await {
            Ok(Ok(wire::Message::AvailabilityAck(ack))) => Some(ack),
            _ => None,
        }
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
        for (id, np) in self.routing.nodes().await.unwrap_or_default() {
            if id == me || holder_ids.contains(&id) {
                continue;
            }
            if let Ok(addr) = np.addr.parse::<PeerAddr>() {
                if self.push_piece(&addr, *cid, gen, &piece).await.is_ok() {
                    return true;
                }
            }
        }
        false
    }

    /// Announce a relay this node operates into the relay registry (§26).
    pub async fn announce_relay(&self, relay_url: String) -> anyhow::Result<()> {
        self.routing
            .announce_relay_registry(relay_url)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Announce this node into the tracker's node registry (map/census),
    /// reporting real bytes stored + offered capacity.
    pub async fn announce_node(&self) -> anyhow::Result<()> {
        let used = self.store.stats().bytes;
        self.routing
            .announce_node_registry(used, self.config.capacity_bytes)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
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
                    *self
                        .demand
                        .lock()
                        .expect("demand")
                        .entry(cid.0)
                        .or_insert(0) += 1;
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
        // Announce as provider on first piece for this CID.
        if was_empty {
            let count = self.store.piece_count(&cid) as u32;
            let _ = self.routing.announce(cid, count, false).await;
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
