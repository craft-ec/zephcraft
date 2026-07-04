//! Tracker registry: the three announce-based tables (providers, nodes,
//! relays) with TTL expiry. Server-side state for the tracker service.
//!
//! Records are signed (verified on ingest AND re-verified by consumers).
//! Newer records (higher hlc_ts) replace older ones for the same key. TTL
//! expiry — not explicit deletion — is the primary liveness mechanism, per
//! foundation §6 (records expire; nodes re-announce).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Serialize;
use zeph_wire::SignedRecord;

use crate::records::{
    self, KIND_APP, KIND_MANIFEST, KIND_META, KIND_NODE, KIND_PROVIDER, KIND_RELAY, KIND_ROOT,
    KIND_WANT,
};

/// Display snapshot of the registries (for the tracker dashboard / map).
#[derive(Debug, Clone, Serialize)]
pub struct RegistrySnapshot {
    pub provider_cids: usize,
    pub provider_records: usize,
    pub nodes: Vec<NodeEntry>,
    pub relays: Vec<RelayEntry>,
    pub top_cids: Vec<CidEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeEntry {
    pub id: String,
    pub addr: String,
    pub version: String,
    pub age_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RelayEntry {
    pub id: String,
    pub relay_url: String,
    pub age_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CidEntry {
    pub cid: String,
    pub providers: usize,
    pub pinned: usize,
}

/// Privacy-safe aggregate stats for the PUBLIC landing page — counts only,
/// never addresses or identities.
#[derive(Debug, Clone, Serialize)]
pub struct PublicStats {
    pub nodes: usize,
    pub content_cids: usize,
    pub provider_records: usize,
    pub relays: usize,
    /// Sum of advisory piece counts across all provider records.
    pub pieces_tracked: u64,
    /// Total bytes stored across all nodes (used).
    pub storage_used_bytes: u64,
    /// Total storage offered across all nodes (capacity/volume).
    pub storage_capacity_bytes: u64,
    /// capacity - used (floored at 0).
    pub storage_available_bytes: u64,
}

pub struct RegistryConfig {
    pub provider_ttl: Duration,
    pub node_ttl: Duration,
    pub relay_ttl: Duration,
    /// Curated allowlist of node_ids permitted to announce relays (v1 trust;
    /// migrates to the §48 governance whitelist). Empty = accept any relay
    /// announce (dev/test).
    pub relay_allowlist: Vec<[u8; 32]>,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            provider_ttl: Duration::from_secs(48 * 3600),
            node_ttl: Duration::from_secs(3600),
            relay_ttl: Duration::from_secs(3600),
            relay_allowlist: Vec::new(),
        }
    }
}

struct Entry {
    record: SignedRecord,
    at: Instant,
}

#[derive(Default)]
struct Tables {
    /// cid -> node_id -> entry
    providers: HashMap<[u8; 32], HashMap<[u8; 32], Entry>>,
    /// node_id -> entry
    nodes: HashMap<[u8; 32], Entry>,
    /// node_id -> entry
    relays: HashMap<[u8; 32], Entry>,
    /// cid -> node_id -> entry (WANT interest signals)
    wants: HashMap<[u8; 32], HashMap<[u8; 32], Entry>>,
    /// cid -> node_id -> entry (editable metadata envelopes)
    metas: HashMap<[u8; 32], HashMap<[u8; 32], Entry>>,
    /// (node_id, namespace) -> entry (single-writer DB root pointers, CAS).
    /// NOT TTL-expired: a DB head must not vanish because the owner went quiet.
    roots: HashMap<([u8; 32], String), Entry>,
    /// (node_id, namespace) -> entry (DB durability manifest pointers). Like
    /// roots: NOT TTL-expired, single-writer, highest seq wins (monotonic).
    manifests: HashMap<([u8; 32], String), Entry>,
    /// (node_id, app_name) -> entry (CraftCOM app heads; highest version wins).
    apps: HashMap<([u8; 32], String), Entry>,
}

pub struct Registry {
    cfg: RegistryConfig,
    tables: Mutex<Tables>,
}

impl Registry {
    pub fn new(cfg: RegistryConfig) -> Self {
        Self {
            cfg,
            tables: Mutex::new(Tables::default()),
        }
    }

    /// Ingest an announce. Rejects bad signatures, disallowed relays, and
    /// stale updates (older hlc_ts than the record already held).
    pub fn announce(&self, record: SignedRecord) -> Result<(), &'static str> {
        if !records::verify(&record) {
            return Err("bad-signature");
        }
        let mut tables = self.tables.lock().expect("registry lock");
        match record.kind {
            KIND_PROVIDER => {
                let Some(p) = records::provider(&record) else {
                    return Err("bad-provider-payload");
                };
                let by_node = tables.providers.entry(p.cid).or_default();
                if superseded(by_node.get(&record.node_id), &record) {
                    return Err("stale");
                }
                by_node.insert(record.node_id, Entry::now(record));
            }
            KIND_WANT => {
                let Some(w) = records::want(&record) else {
                    return Err("bad-want-payload");
                };
                let by_node = tables.wants.entry(w.cid).or_default();
                if superseded(by_node.get(&record.node_id), &record) {
                    return Err("stale");
                }
                by_node.insert(record.node_id, Entry::now(record));
            }
            KIND_META => {
                let Some(m) = records::meta(&record) else {
                    return Err("bad-meta-payload");
                };
                let by_node = tables.metas.entry(m.cid).or_default();
                if superseded(by_node.get(&record.node_id), &record) {
                    return Err("stale");
                }
                by_node.insert(record.node_id, Entry::now(record));
            }
            KIND_ROOT => {
                let Some(r) = records::root(&record) else {
                    return Err("bad-root-payload");
                };
                let key = (record.node_id, r.namespace.clone());
                match tables.roots.get(&key) {
                    None => {
                        // No current record: first write, OR the owner restoring a
                        // head the tracker lost on restart (re-announce). Accept —
                        // the record is signed by the owner. (Rollback-replay after
                        // loss is a separate hardening item.)
                    }
                    Some(cur) => {
                        let Some(curp) = records::root(&cur.record) else {
                            return Err("root-corrupt");
                        };
                        // Idempotent re-announce of the current head → refresh, no CAS.
                        if r.root_cid == curp.root_cid && r.seq == curp.seq {
                            // accept as-is
                        } else {
                            // CAS: must expect the current root AND advance the seq.
                            if r.prev_cid != curp.root_cid {
                                return Err("root-conflict");
                            }
                            if r.seq <= curp.seq {
                                return Err("root-stale");
                            }
                        }
                    }
                }
                tables.roots.insert(key, Entry::now(record));
            }
            KIND_APP => {
                let Some(a) = records::app(&record) else {
                    return Err("bad-app-payload");
                };
                let key = (record.node_id, a.name.clone());
                if let Some(cur) = tables.apps.get(&key) {
                    if let Some(curp) = records::app(&cur.record) {
                        // Monotonic: only a strictly newer version replaces the head
                        // (idempotent re-announce of the current version is fine).
                        if a.version < curp.version {
                            return Err("app-stale");
                        }
                    }
                }
                tables.apps.insert(key, Entry::now(record));
            }
            KIND_MANIFEST => {
                let Some(m) = records::manifest(&record) else {
                    return Err("bad-manifest-payload");
                };
                let key = (record.node_id, m.namespace.clone());
                if let Some(cur) = tables.manifests.get(&key) {
                    if let Some(curm) = records::manifest(&cur.record) {
                        if m.seq <= curm.seq {
                            return Err("manifest-stale");
                        }
                    }
                }
                tables.manifests.insert(key, Entry::now(record));
            }
            KIND_NODE => {
                if records::node(&record).is_none() {
                    return Err("bad-node-payload");
                }
                if superseded(tables.nodes.get(&record.node_id), &record) {
                    return Err("stale");
                }
                tables.nodes.insert(record.node_id, Entry::now(record));
            }
            KIND_RELAY => {
                if records::relay(&record).is_none() {
                    return Err("bad-relay-payload");
                }
                if !self.cfg.relay_allowlist.is_empty()
                    && !self.cfg.relay_allowlist.contains(&record.node_id)
                {
                    return Err("relay-not-allowlisted");
                }
                if superseded(tables.relays.get(&record.node_id), &record) {
                    return Err("stale");
                }
                tables.relays.insert(record.node_id, Entry::now(record));
            }
            _ => return Err("unknown-kind"),
        }
        Ok(())
    }

    /// Withdraw a record (graceful provider/relay departure). Best-effort;
    /// TTL expiry is the backstop.
    pub fn withdraw(&self, record: SignedRecord) -> Result<(), &'static str> {
        if !records::verify(&record) {
            return Err("bad-signature");
        }
        let mut tables = self.tables.lock().expect("registry lock");
        match record.kind {
            KIND_PROVIDER => {
                if let Some(p) = records::provider(&record) {
                    if let Some(by_node) = tables.providers.get_mut(&p.cid) {
                        by_node.remove(&record.node_id);
                    }
                }
            }
            KIND_WANT => {
                if let Some(w) = records::want(&record) {
                    if let Some(by_node) = tables.wants.get_mut(&w.cid) {
                        by_node.remove(&record.node_id);
                    }
                }
            }
            KIND_META => {
                if let Some(m) = records::meta(&record) {
                    if let Some(by_node) = tables.metas.get_mut(&m.cid) {
                        by_node.remove(&record.node_id);
                    }
                }
            }
            KIND_ROOT => {
                if let Some(r) = records::root(&record) {
                    tables.roots.remove(&(record.node_id, r.namespace));
                }
            }
            KIND_MANIFEST => {
                if let Some(m) = records::manifest(&record) {
                    tables.manifests.remove(&(record.node_id, m.namespace));
                }
            }
            KIND_NODE => {
                tables.nodes.remove(&record.node_id);
            }
            KIND_RELAY => {
                tables.relays.remove(&record.node_id);
            }
            _ => return Err("unknown-kind"),
        }
        Ok(())
    }

    pub fn providers(&self, cid: &[u8; 32], max: usize) -> Vec<SignedRecord> {
        let mut tables = self.tables.lock().expect("registry lock");
        self.expire(&mut tables);
        tables
            .providers
            .get(cid)
            .map(|by_node| {
                by_node
                    .values()
                    .take(max)
                    .map(|e| e.record.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Every provider record across all CIDs (capped) — for content
    /// enumeration by nodes that want to show the network's content list.
    pub fn all_providers(&self, max: usize) -> Vec<SignedRecord> {
        let mut tables = self.tables.lock().expect("registry lock");
        self.expire(&mut tables);
        tables
            .providers
            .values()
            .flat_map(|by_node| by_node.values())
            .take(max)
            .map(|e| e.record.clone())
            .collect()
    }

    pub fn all_wants(&self, max: usize) -> Vec<SignedRecord> {
        let mut tables = self.tables.lock().expect("registry lock");
        self.expire(&mut tables);
        tables
            .wants
            .values()
            .flat_map(|by_node| by_node.values())
            .take(max)
            .map(|e| e.record.clone())
            .collect()
    }

    pub fn all_metas(&self, max: usize) -> Vec<SignedRecord> {
        let mut tables = self.tables.lock().expect("registry lock");
        self.expire(&mut tables);
        tables
            .metas
            .values()
            .flat_map(|by_node| by_node.values())
            .take(max)
            .map(|e| e.record.clone())
            .collect()
    }

    /// All root pointers owned by `owner` (usually one per namespace). No TTL
    /// expiry — a DB head persists until superseded or withdrawn.
    pub fn roots_for(&self, owner: &[u8; 32], max: usize) -> Vec<SignedRecord> {
        let tables = self.tables.lock().expect("registry lock");
        tables
            .roots
            .iter()
            .filter(|((nid, _), _)| nid == owner)
            .take(max)
            .map(|(_, e)| e.record.clone())
            .collect()
    }

    pub fn apps_for(&self, owner: &[u8; 32], max: usize) -> Vec<SignedRecord> {
        let tables = self.tables.lock().expect("registry lock");
        tables
            .apps
            .iter()
            .filter(|((nid, _), _)| nid == owner)
            .take(max)
            .map(|(_, e)| e.record.clone())
            .collect()
    }

    pub fn manifests_for(&self, owner: &[u8; 32], max: usize) -> Vec<SignedRecord> {
        let tables = self.tables.lock().expect("registry lock");
        tables
            .manifests
            .iter()
            .filter(|((nid, _), _)| nid == owner)
            .take(max)
            .map(|(_, e)| e.record.clone())
            .collect()
    }

    pub fn nodes(&self, max: usize) -> Vec<SignedRecord> {
        let mut tables = self.tables.lock().expect("registry lock");
        self.expire(&mut tables);
        tables
            .nodes
            .values()
            .take(max)
            .map(|e| e.record.clone())
            .collect()
    }

    pub fn relays(&self, max: usize) -> Vec<SignedRecord> {
        let mut tables = self.tables.lock().expect("registry lock");
        self.expire(&mut tables);
        tables
            .relays
            .values()
            .take(max)
            .map(|e| e.record.clone())
            .collect()
    }

    pub fn counts(&self) -> (usize, usize, usize) {
        let mut tables = self.tables.lock().expect("registry lock");
        self.expire(&mut tables);
        let providers = tables.providers.values().map(|m| m.len()).sum();
        (providers, tables.nodes.len(), tables.relays.len())
    }

    /// A display snapshot of all three registries (expires stale entries
    /// first). Top CIDs are ordered by provider count.
    pub fn snapshot(&self, top_n: usize) -> RegistrySnapshot {
        let mut tables = self.tables.lock().expect("registry lock");
        self.expire(&mut tables);

        let mut nodes: Vec<NodeEntry> = tables
            .nodes
            .iter()
            .filter_map(|(id, e)| {
                let n = records::node(&e.record)?;
                Some(NodeEntry {
                    id: hex::encode(id),
                    addr: n.addr,
                    version: n.version,
                    age_secs: e.at.elapsed().as_secs(),
                })
            })
            .collect();
        nodes.sort_by_key(|n| n.age_secs);

        let mut relays: Vec<RelayEntry> = tables
            .relays
            .iter()
            .filter_map(|(id, e)| {
                let r = records::relay(&e.record)?;
                Some(RelayEntry {
                    id: hex::encode(id),
                    relay_url: r.relay_url,
                    age_secs: e.at.elapsed().as_secs(),
                })
            })
            .collect();
        relays.sort_by_key(|r| r.age_secs);

        let provider_records: usize = tables.providers.values().map(|m| m.len()).sum();
        let mut top_cids: Vec<CidEntry> = tables
            .providers
            .iter()
            .map(|(cid, by_node)| {
                let pinned = by_node
                    .values()
                    .filter(|e| records::provider(&e.record).is_some_and(|p| p.pinned))
                    .count();
                CidEntry {
                    cid: hex::encode(cid),
                    providers: by_node.len(),
                    pinned,
                }
            })
            .collect();
        top_cids.sort_by(|a, b| b.providers.cmp(&a.providers));
        top_cids.truncate(top_n);

        RegistrySnapshot {
            provider_cids: tables.providers.len(),
            provider_records,
            nodes,
            relays,
            top_cids,
        }
    }

    /// Aggregate, privacy-safe counts for public display (no addresses).
    pub fn public_stats(&self) -> PublicStats {
        let mut tables = self.tables.lock().expect("registry lock");
        self.expire(&mut tables);
        let provider_records: usize = tables.providers.values().map(|m| m.len()).sum();
        let pieces_tracked: u64 = tables
            .providers
            .values()
            .flat_map(|m| m.values())
            .filter_map(|e| records::provider(&e.record))
            .map(|p| p.piece_count as u64)
            .sum();
        let (mut used, mut capacity) = (0u64, 0u64);
        for e in tables.nodes.values() {
            if let Some(n) = records::node(&e.record) {
                used = used.saturating_add(n.used_bytes);
                capacity = capacity.saturating_add(n.capacity_bytes);
            }
        }
        PublicStats {
            nodes: tables.nodes.len(),
            content_cids: tables.providers.len(),
            provider_records,
            relays: tables.relays.len(),
            pieces_tracked,
            storage_used_bytes: used,
            storage_capacity_bytes: capacity,
            storage_available_bytes: capacity.saturating_sub(used),
        }
    }

    fn expire(&self, tables: &mut Tables) {
        let (pt, nt, rt) = (self.cfg.provider_ttl, self.cfg.node_ttl, self.cfg.relay_ttl);
        for by_node in tables.providers.values_mut() {
            by_node.retain(|_, e| e.at.elapsed() < pt);
        }
        tables.providers.retain(|_, m| !m.is_empty());
        for by_node in tables.wants.values_mut() {
            by_node.retain(|_, e| e.at.elapsed() < pt);
        }
        tables.wants.retain(|_, m| !m.is_empty());
        for by_node in tables.metas.values_mut() {
            by_node.retain(|_, e| e.at.elapsed() < pt);
        }
        tables.metas.retain(|_, m| !m.is_empty());
        tables.nodes.retain(|_, e| e.at.elapsed() < nt);
        tables.relays.retain(|_, e| e.at.elapsed() < rt);
    }
}

impl Entry {
    fn now(record: SignedRecord) -> Self {
        Self {
            record,
            at: Instant::now(),
        }
    }
}

fn superseded(existing: Option<&Entry>, incoming: &SignedRecord) -> bool {
    existing.is_some_and(|e| e.record.hlc_ts > incoming.hlc_ts)
}
