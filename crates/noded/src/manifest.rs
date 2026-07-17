//! `HoldingsManifest` — a node's SIGNED claim about what it holds (`docs/DURABILITY_DESIGN.md` P1).
//!
//! **Why this exists.** Durability is checked today by polling every cid on a timer: O(N_cids) per
//! interval, forever, detecting loss in 15min–2h. Measured on the live fleet, that is the largest traffic
//! source on an idle node (26,193 inbound DHT streams — ~252/sec — 104s after a restart, at 2890 cids),
//! because every cid resolve is an iterative DHT lookup and every cid is due at boot. The period is a
//! constant factor on the wrong axis: at 1M cids even a 2-hour period is ~139 resolves/sec/node.
//!
//! Pieces are not lost per-cid — they are lost **per-node**, and SWIM already reports death in SECONDS.
//! So account per node: each node asserts its holdings ONCE, and repair becomes a reaction to an event
//! rather than a sweep of the inventory. Work then scales with CHURN, not with how much is stored.
//!
//! **The split that makes it scale.** The DHT head (`peer → manifest_cid@version`) is the cheap signal:
//! the manifest is content-addressed, so a changed cid IS a changed holdings set — anti-entropy is an
//! O(1) cid comparison with no member data moved. The full set lives in obj and is fetched ONLY on an
//! event (a death, or a mismatched head). At this size the content-address serves as the Merkle root; a
//! real tree + diffs is a later concern (a 1M-cid manifest is ~32 MB and must not be republished whole —
//! see the design's "manifest size" gap).
//!
//! **Trust.** The manifest is a signed CLAIM, not proof: a node can assert holdings it lacks. K8's
//! `AvailabilityProbe` verifies before repair acts on it, and PDP sampling (K5) is what would make the
//! claim itself trustworthy. Recorded here so no caller mistakes a signature for possession.

use std::collections::HashSet;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;
use zeph_obj::{ConsumeMode, ObjEngine};
use zeph_routing::ContentRouting;

/// Canonical (sorted) form — the invariant `verify` enforces and every reader depends on.
fn sorted(set: &HashSet<[u8; 32]>) -> Vec<[u8; 32]> {
    let mut v: Vec<[u8; 32]> = set.iter().copied().collect();
    v.sort_unstable();
    v
}

/// The DHT app-record name a node publishes its holdings head under. One per node: the record's publisher
/// IS the subject, so a name collision across nodes is impossible.
const HOLDINGS_NAME: &str = "craftec/holdings/1";

/// How many versions between full snapshots.
///
/// Bounds a reader with no baseline: it follows `prev` at most this many hops to reach a snapshot it can
/// apply forward from. Lower = cheaper cold start, more frequent big publishes; higher = the reverse.
const SNAPSHOT_EVERY: u64 = 16;

/// What a manifest version says about the holdings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Body {
    /// The COMPLETE set, sorted. Published every [`SNAPSHOT_EVERY`] versions so a reader with no baseline
    /// (a fresh node, or one that fell behind) always has a bounded path to a usable state.
    Snapshot(Vec<[u8; 32]>),
    /// What changed since `version - 1`, whose manifest is at `prev`.
    ///
    /// This is the point of the whole structure: the PUBLISHER knows exactly what it added and removed, so
    /// making every reader re-fetch the entire set to re-derive that change is pure waste — ~32 MB at 1M
    /// cids, to learn that one cid moved. A reader holding the previous version applies an O(Δ) update.
    Diff {
        added: Vec<[u8; 32]>,
        removed: Vec<[u8; 32]>,
        /// The previous version's manifest cid — the chain a cold reader walks back to a snapshot.
        prev: [u8; 32],
    },
}

/// A node's signed holdings claim, as of `version`.
///
/// Both bodies are SORTED — canonical, so the same claim always serializes to the same bytes and therefore
/// the same content-address. That is what lets a head comparison stand in for a set comparison.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HoldingsManifest {
    /// The claiming node. Checked against the record's publisher on read, so one node cannot publish a
    /// manifest "for" another.
    pub node: [u8; 32],
    /// Monotonic, bumped on every publish. The DHT head carries it, so a stale record can never displace
    /// a newer one (`announce_app` keeps the max version).
    pub version: u64,
    /// Snapshot or diff — see [`Body`].
    pub body: Body,
    /// Signature by `node` over `(node, version, body)` — see [`signing_bytes`].
    pub sig: Vec<u8>,
}

/// The exact bytes a manifest signature covers. Separate from `postcard(manifest)` because the signature
/// cannot cover itself; sorted bodies make this deterministic.
///
/// The DIFF is signed too, not just the resulting set: a reader applies the diff without ever seeing the
/// whole set, so the diff IS the claim from its point of view. An unsigned diff would let anyone rewrite
/// what a node said it dropped — i.e. manufacture or suppress repair.
fn signing_bytes(node: &[u8; 32], version: u64, body: &Body) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(node);
    buf.extend_from_slice(&version.to_le_bytes());
    match body {
        Body::Snapshot(cids) => {
            buf.push(0);
            for c in cids {
                buf.extend_from_slice(c);
            }
        }
        Body::Diff {
            added,
            removed,
            prev,
        } => {
            buf.push(1);
            buf.extend_from_slice(prev);
            buf.push(2);
            for c in added {
                buf.extend_from_slice(c);
            }
            buf.push(3);
            for c in removed {
                buf.extend_from_slice(c);
            }
        }
    }
    buf
}

impl HoldingsManifest {
    /// Verify the claim is internally sound: signed by the node it names, and canonical.
    ///
    /// The sort check is not pedantry — an unsorted manifest would content-address differently while
    /// asserting identical holdings, which silently breaks the head-comparison-as-set-comparison property
    /// the whole design rests on.
    ///
    /// Unused until P2 (death-driven repair) reads peers' manifests; P1 only PUBLISHES. Kept + tested now
    /// so the read path is verified before anything depends on it.
    #[allow(dead_code)]
    pub fn verify(&self) -> bool {
        let canonical = |v: &Vec<[u8; 32]>| v.windows(2).all(|w| w[0] < w[1]);
        match &self.body {
            Body::Snapshot(cids) => {
                if !canonical(cids) {
                    return false;
                }
            }
            Body::Diff { added, removed, .. } => {
                if !canonical(added) || !canonical(removed) {
                    return false;
                }
                // A cid cannot be both gained and dropped in one version — that is not a state any real
                // change produces, and accepting it would make the applied result order-dependent.
                if added.iter().any(|a| removed.binary_search(a).is_ok()) {
                    return false;
                }
            }
        }
        let Ok(sig) = <[u8; 64]>::try_from(self.sig.as_slice()) else {
            return false;
        };
        NodeIdentity::verify(
            &NodeId(self.node),
            &signing_bytes(&self.node, self.version, &self.body),
            &sig,
        )
    }
}

/// Our last publish: the version, the exact set it asserted, and its manifest cid (the `prev` link the
/// next diff points at).
struct Published {
    version: u64,
    cids: HashSet<[u8; 32]>,
    cid: [u8; 32],
}

/// Publishes this node's holdings manifest and fetches peers'.
pub struct ManifestStore {
    identity: Arc<NodeIdentity>,
    obj: Arc<ObjEngine>,
    routing: Arc<dyn ContentRouting>,
    /// Our own last published `(version, set, manifest_cid)`.
    ///
    /// Retaining OUR set is not the O(N) sin the reader side avoids: it is our own store, which we hold
    /// anyway, and it is what lets us publish a DIFF instead of making every reader re-derive the change
    /// from a full set. Publishing on a timer regardless of change is the habit this design exists to
    /// remove, so an unchanged set publishes nothing at all.
    last: tokio::sync::Mutex<Option<Published>>,
}

impl ManifestStore {
    pub fn new(
        identity: Arc<NodeIdentity>,
        obj: Arc<ObjEngine>,
        routing: Arc<dyn ContentRouting>,
    ) -> Self {
        Self {
            identity,
            obj,
            routing,
            last: tokio::sync::Mutex::new(None),
        }
    }

    /// Publish `cids` as this node's holdings, if they changed. Returns the new version, or `None` when
    /// nothing changed (the common case — steady state must be silent).
    ///
    /// Publishes a DIFF against the last version, or a full [`Body::Snapshot`] on the first publish and
    /// every [`SNAPSHOT_EVERY`] versions thereafter — the periodic snapshot is what bounds a cold reader's
    /// walk back down the `prev` chain.
    pub async fn publish(&self, cids: Vec<Cid>) -> Option<u64> {
        let now: HashSet<[u8; 32]> = cids.into_iter().map(|c| c.0).collect();
        let mut last = self.last.lock().await;

        let (version, body) = match last.as_ref() {
            None => (1, Body::Snapshot(sorted(&now))),
            Some(p) => {
                let added: HashSet<[u8; 32]> = now.difference(&p.cids).copied().collect();
                let removed: HashSet<[u8; 32]> = p.cids.difference(&now).copied().collect();
                if added.is_empty() && removed.is_empty() {
                    return None; // unchanged → say nothing. Steady state MUST be silent.
                }
                let version = p.version + 1;
                if version % SNAPSHOT_EVERY == 0 {
                    (version, Body::Snapshot(sorted(&now)))
                } else {
                    (
                        version,
                        Body::Diff {
                            added: sorted(&added),
                            removed: sorted(&removed),
                            prev: p.cid,
                        },
                    )
                }
            }
        };

        let node = self.identity.node_id().0;
        let sig = self
            .identity
            .sign(&signing_bytes(&node, version, &body))
            .to_vec();
        let manifest = HoldingsManifest {
            node,
            version,
            body,
            sig,
        };
        let bytes = postcard::to_allocvec(&manifest).ok()?;
        let cid = self.obj.publish_system(&bytes).await.ok()?;
        // The head is the cheap signal: content-addressed, so a changed cid IS a changed claim.
        self.routing
            .announce_app(HOLDINGS_NAME, cid, version)
            .await
            .ok()?;
        *last = Some(Published {
            version,
            cids: now,
            cid: cid.0,
        });
        Some(version)
    }

    /// The head a peer currently announces: `(manifest_cid, version)`. O(1) — one DHT record, no set data.
    /// This is the anti-entropy primitive: an unchanged cid means unchanged holdings, so a peer that has
    /// lost nothing costs nothing to check.
    ///
    /// Consumed by P3 (manifest anti-entropy); P1 only publishes.
    #[allow(dead_code)]
    pub async fn peer_head(&self, peer: [u8; 32]) -> Option<([u8; 32], u64)> {
        let rec = self
            .routing
            .resolve_app(NodeId(peer), HOLDINGS_NAME)
            .await
            .ok()??;
        Some((rec.wasm_cid.0, rec.version))
    }

    /// Fetch and VERIFY a peer's full holdings manifest. O(set) — call it on an EVENT (a death, or a head
    /// that changed), never on a timer.
    ///
    /// Rejects a manifest whose `node` is not the peer we asked about: the record's publisher is the only
    /// authority over its own holdings, so a peer cannot speak for another (which would otherwise let one
    /// node fabricate a death-repair workload for the whole fleet).
    ///
    /// Fetch + verify one specific manifest by cid.
    ///
    /// Rejects a manifest whose `node` is not the peer we asked about: the record's publisher is the only
    /// authority over its own holdings, so a peer cannot speak for another (which would otherwise let one
    /// node fabricate a repair workload for the whole fleet).
    async fn fetch_at(&self, peer: [u8; 32], cid: [u8; 32]) -> Option<HoldingsManifest> {
        let bytes = self
            .obj
            .get_following_manifest(Cid(cid), ConsumeMode::Drop)
            .await
            .ok()?;
        let manifest: HoldingsManifest = postcard::from_bytes(&bytes).ok()?;
        if manifest.node != peer || !manifest.verify() {
            return None;
        }
        Some(manifest)
    }

    /// A peer's holdings CHANGE, resolved against the version a reader already has.
    ///
    /// This is where the diff pays off. A reader holding `known_version` gets an O(Δ) answer: one small
    /// fetch naming exactly what moved. Only a reader with NO usable baseline pays for a set, and even
    /// then the walk back down the `prev` chain is bounded by [`SNAPSHOT_EVERY`].
    ///
    /// Returns `(version, added, removed)` — relative to `known_version` when it can be, or relative to
    /// nothing (i.e. `added` = the full set) when the reader must re-baseline.
    pub async fn changes_since(
        &self,
        peer: [u8; 32],
        known: Option<(u64, [u8; 32])>,
    ) -> Option<(u64, Vec<[u8; 32]>, Vec<[u8; 32]>)> {
        let (head_cid, head_version) = self.peer_head(peer).await?;
        if let Some((kv, kc)) = known {
            if kc == head_cid {
                return Some((kv, Vec::new(), Vec::new())); // unchanged — the O(1) path
            }
        }
        let head = self.fetch_at(peer, head_cid).await?;

        // FAST PATH: the head is exactly one diff past what we know. One small object, no set.
        if let Body::Diff {
            added,
            removed,
            prev,
        } = &head.body
        {
            if let Some((kv, kc)) = known {
                if head.version == kv + 1 && *prev == kc {
                    return Some((head.version, added.clone(), removed.clone()));
                }
            }
        }

        // COLD PATH: no baseline, or we fell behind by more than one version. Walk back to a snapshot and
        // apply forward. Bounded by SNAPSHOT_EVERY — the reason snapshots exist at all.
        let mut chain = vec![head];
        loop {
            let Body::Diff { prev, .. } = &chain.last()?.body else {
                break; // reached a snapshot
            };
            let prev = *prev;
            if chain.len() as u64 > SNAPSHOT_EVERY * 2 {
                return None; // chain longer than it should ever be — refuse rather than walk forever
            }
            chain.push(self.fetch_at(peer, prev).await?);
        }
        // chain is head..snapshot; fold forward from the snapshot.
        let mut set: HashSet<[u8; 32]> = match &chain.last()?.body {
            Body::Snapshot(cids) => cids.iter().copied().collect(),
            Body::Diff { .. } => return None, // unreachable: the loop only exits on a snapshot
        };
        for m in chain.iter().rev().skip(1) {
            if let Body::Diff { added, removed, .. } = &m.body {
                for c in removed {
                    set.remove(c);
                }
                for c in added {
                    set.insert(*c);
                }
            }
        }
        // A re-baselining reader has nothing to diff against: report the whole set as `added`, and let the
        // caller reconcile against its own index. `removed` is empty because we cannot know what it dropped
        // before we were watching — and inventing losses would manufacture repair work.
        Some((head_version, sorted(&set), Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot_for(id: &NodeIdentity, version: u64, cids: &[[u8; 32]]) -> HoldingsManifest {
        let node = id.node_id().0;
        let body = Body::Snapshot(cids.to_vec());
        let sig = id.sign(&signing_bytes(&node, version, &body)).to_vec();
        HoldingsManifest {
            node,
            version,
            body,
            sig,
        }
    }

    fn diff_for(
        id: &NodeIdentity,
        version: u64,
        added: &[[u8; 32]],
        removed: &[[u8; 32]],
    ) -> HoldingsManifest {
        let node = id.node_id().0;
        let body = Body::Diff {
            added: added.to_vec(),
            removed: removed.to_vec(),
            prev: [9u8; 32],
        };
        let sig = id.sign(&signing_bytes(&node, version, &body)).to_vec();
        HoldingsManifest {
            node,
            version,
            body,
            sig,
        }
    }

    #[test]
    fn a_signed_manifest_verifies_and_a_tampered_one_does_not() {
        let id = NodeIdentity::generate();
        let cids = [[1u8; 32], [2u8; 32], [3u8; 32]];
        let m = snapshot_for(&id, 1, &cids);
        assert!(m.verify());

        // Adding a cid to someone's claim must not verify — otherwise a peer could inflate another node's
        // holdings and, at P2, manufacture repair work for the whole fleet.
        let mut forged = m.clone();
        if let Body::Snapshot(v) = &mut forged.body {
            v.push([4u8; 32]);
        }
        assert!(!forged.verify());

        // Nor may the version be replayed onto a different set.
        let mut rolled = m.clone();
        rolled.version = 99;
        assert!(!rolled.verify());

        // Nor may another identity's signature be pasted in.
        let other = NodeIdentity::generate();
        let mut swapped = m.clone();
        swapped.sig = snapshot_for(&other, 1, &cids).sig;
        assert!(!swapped.verify());
    }

    #[test]
    fn an_uncanonical_manifest_is_rejected() {
        // The head-comparison-as-set-comparison property REQUIRES canonical bytes: the same holdings must
        // always content-address identically, or anti-entropy sees phantom differences forever.
        let id = NodeIdentity::generate();
        let unsorted = [[2u8; 32], [1u8; 32]];
        assert!(
            !snapshot_for(&id, 1, &unsorted).verify(),
            "unsorted cids must not verify"
        );
        let duped = [[1u8; 32], [1u8; 32]];
        assert!(
            !snapshot_for(&id, 1, &duped).verify(),
            "duplicate cids must not verify"
        );
        let sorted = [[1u8; 32], [2u8; 32]];
        assert!(snapshot_for(&id, 1, &sorted).verify());
    }

    #[test]
    fn a_diff_is_signed_so_it_cannot_be_rewritten() {
        // The diff IS the claim from a reader's point of view — a reader applies it without ever seeing the
        // full set. An unsigned or malleable diff would let anyone rewrite what a node said it dropped, i.e.
        // manufacture repair work or suppress it. Both are worse than the bandwidth this saves.
        let id = NodeIdentity::generate();
        let m = diff_for(&id, 2, &[[5u8; 32]], &[[3u8; 32]]);
        assert!(m.verify());

        // Rewriting what it DROPPED must not verify (suppressing a loss = silent data loss).
        let mut suppressed = m.clone();
        if let Body::Diff { removed, .. } = &mut suppressed.body {
            removed.clear();
        }
        assert!(!suppressed.verify());

        // Inventing a loss must not verify (manufactured repair work for the whole fleet).
        let mut invented = m.clone();
        if let Body::Diff { removed, .. } = &mut invented.body {
            *removed = vec![[1u8; 32], [3u8; 32]];
        }
        assert!(!invented.verify());

        // Repointing `prev` must not verify — the chain a cold reader walks is part of the claim.
        let mut repointed = m.clone();
        if let Body::Diff { prev, .. } = &mut repointed.body {
            *prev = [0xAAu8; 32];
        }
        assert!(!repointed.verify());
    }

    #[test]
    fn a_diff_cannot_both_add_and_remove_the_same_cid() {
        // Not a state any real change produces, and accepting it would make the applied result depend on
        // whether the reader applies `added` or `removed` first — i.e. two readers could disagree about
        // whether a cid still exists.
        let id = NodeIdentity::generate();
        let c = [4u8; 32];
        assert!(!diff_for(&id, 2, &[c], &[c]).verify());
    }

    #[test]
    fn identical_holdings_serialize_identically() {
        // Two nodes holding the same set must produce byte-identical cid lists, so a head cid can stand in
        // for the set. (The manifests differ by `node`/`sig`, but the SET encoding must not.)
        let a = NodeIdentity::generate();
        let b = NodeIdentity::generate();
        let cids = [[7u8; 32], [9u8; 32]];
        let ma = snapshot_for(&a, 1, &cids);
        let mb = snapshot_for(&b, 1, &cids);
        assert_eq!(
            postcard::to_allocvec(&ma.body).unwrap(),
            postcard::to_allocvec(&mb.body).unwrap()
        );
    }

    #[test]
    fn the_intersection_is_what_partitions_repair_work() {
        // P2's core: on a death each survivor considers only `S_dead ∩ S_own` — computed LOCALLY, no
        // network. Differing sets are not a coordination problem; they ARE the partition.
        let dead: std::collections::BTreeSet<[u8; 32]> =
            [[1u8; 32], [2u8; 32], [3u8; 32], [4u8; 32]]
                .into_iter()
                .collect();
        let mine: std::collections::BTreeSet<[u8; 32]> =
            [[3u8; 32], [4u8; 32], [5u8; 32]].into_iter().collect();
        let shared: Vec<_> = dead.intersection(&mine).copied().collect();
        assert_eq!(shared, vec![[3u8; 32], [4u8; 32]]);
        // Cids the dead node held that I do not hold are NOT my concern — someone else holds them, or they
        // are already lost (the design's "last-holder loss" gap, which repair cannot see).
        assert!(!shared.contains(&[1u8; 32]));
    }
}
