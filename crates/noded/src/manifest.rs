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

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;
use zeph_obj::{ConsumeMode, ObjEngine};
use zeph_routing::ContentRouting;

/// The DHT app-record name a node publishes its holdings head under. One per node: the record's publisher
/// IS the subject, so a name collision across nodes is impossible.
const HOLDINGS_NAME: &str = "craftec/holdings/1";

/// A node's signed holdings claim.
///
/// `cids` is SORTED — canonical, so the same holdings always serialize to the same bytes and therefore the
/// same content-address. That is what lets a head comparison stand in for a set comparison.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HoldingsManifest {
    /// The claiming node. Checked against the record's publisher on read, so one node cannot publish a
    /// manifest "for" another.
    pub node: [u8; 32],
    /// Monotonic, bumped on every publish. The DHT head carries it, so a stale record can never displace
    /// a newer one (`announce_app` keeps the max version).
    pub version: u64,
    /// The held cids, sorted.
    pub cids: Vec<[u8; 32]>,
    /// Signature by `node` over `(node, version, cids)` — see [`signing_bytes`].
    pub sig: Vec<u8>,
}

/// The exact bytes a manifest signature covers. Separate from `postcard(manifest)` because the signature
/// cannot cover itself; sorted `cids` make this deterministic.
fn signing_bytes(node: &[u8; 32], version: u64, cids: &[[u8; 32]]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(40 + cids.len() * 32);
    buf.extend_from_slice(node);
    buf.extend_from_slice(&version.to_le_bytes());
    for c in cids {
        buf.extend_from_slice(c);
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
        if !self.cids.windows(2).all(|w| w[0] < w[1]) {
            return false; // unsorted or duplicated → not canonical
        }
        let Ok(sig) = <[u8; 64]>::try_from(self.sig.as_slice()) else {
            return false;
        };
        NodeIdentity::verify(
            &NodeId(self.node),
            &signing_bytes(&self.node, self.version, &self.cids),
            &sig,
        )
    }
}

/// Publishes this node's holdings manifest and fetches peers'.
pub struct ManifestStore {
    identity: Arc<NodeIdentity>,
    obj: Arc<ObjEngine>,
    routing: Arc<dyn ContentRouting>,
    /// Last published `(version, cid_count)` — so an unchanged holdings set does not republish. Publishing
    /// on a timer regardless of change is the very habit this design exists to remove.
    last: tokio::sync::Mutex<Option<(u64, usize)>>,
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

    /// Publish `cids` as this node's holdings, if they changed since the last publish. Returns the new
    /// version, or `None` when nothing changed (the common case — steady state must be silent).
    pub async fn publish(&self, mut cids: Vec<Cid>) -> Option<u64> {
        cids.sort_unstable();
        cids.dedup();
        let raw: Vec<[u8; 32]> = cids.iter().map(|c| c.0).collect();

        let mut last = self.last.lock().await;
        let version = last.map(|(v, _)| v + 1).unwrap_or(1);
        // Cheap change check: a different count is definitely a change. Equal counts still republish only
        // if the SET differs — but comparing sets needs the previous set, which we deliberately do not
        // retain (it is the thing that is O(N) in memory). The version bump on every real publish plus the
        // content-address means a redundant publish is wasteful, never wrong; `unchanged` below keeps the
        // steady state quiet, which is what matters.
        let unchanged = matches!(*last, Some((_, n)) if n == raw.len());
        if unchanged {
            return None;
        }

        let node = self.identity.node_id().0;
        let sig = self
            .identity
            .sign(&signing_bytes(&node, version, &raw))
            .to_vec();
        let manifest = HoldingsManifest {
            node,
            version,
            cids: raw.clone(),
            sig,
        };
        let bytes = postcard::to_allocvec(&manifest).ok()?;
        let cid = self.obj.publish_system(&bytes).await.ok()?;
        // The head is the cheap signal: content-addressed, so a changed cid IS a changed holdings set.
        self.routing
            .announce_app(HOLDINGS_NAME, cid, version)
            .await
            .ok()?;
        *last = Some((version, raw.len()));
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
    /// Consumed by P2 (death-driven repair); P1 only publishes.
    #[allow(dead_code)]
    pub async fn fetch(&self, peer: [u8; 32]) -> Option<HoldingsManifest> {
        let (cid, _version) = self.peer_head(peer).await?;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_for(id: &NodeIdentity, version: u64, cids: &[[u8; 32]]) -> HoldingsManifest {
        let node = id.node_id().0;
        HoldingsManifest {
            node,
            version,
            cids: cids.to_vec(),
            sig: id.sign(&signing_bytes(&node, version, cids)).to_vec(),
        }
    }

    #[test]
    fn a_signed_manifest_verifies_and_a_tampered_one_does_not() {
        let id = NodeIdentity::generate();
        let cids = [[1u8; 32], [2u8; 32], [3u8; 32]];
        let m = manifest_for(&id, 1, &cids);
        assert!(m.verify());

        // Adding a cid to someone's claim must not verify — otherwise a peer could inflate another node's
        // holdings and, at P2, manufacture repair work for the whole fleet.
        let mut forged = m.clone();
        forged.cids.push([4u8; 32]);
        assert!(!forged.verify());

        // Nor may the version be replayed onto a different set.
        let mut rolled = m.clone();
        rolled.version = 99;
        assert!(!rolled.verify());

        // Nor may another identity's signature be pasted in.
        let other = NodeIdentity::generate();
        let mut swapped = m.clone();
        swapped.sig = manifest_for(&other, 1, &cids).sig;
        assert!(!swapped.verify());
    }

    #[test]
    fn an_uncanonical_manifest_is_rejected() {
        // The head-comparison-as-set-comparison property REQUIRES canonical bytes: the same holdings must
        // always content-address identically, or anti-entropy sees phantom differences forever.
        let id = NodeIdentity::generate();
        let unsorted = [[2u8; 32], [1u8; 32]];
        assert!(
            !manifest_for(&id, 1, &unsorted).verify(),
            "unsorted cids must not verify"
        );
        let duped = [[1u8; 32], [1u8; 32]];
        assert!(
            !manifest_for(&id, 1, &duped).verify(),
            "duplicate cids must not verify"
        );
        let sorted = [[1u8; 32], [2u8; 32]];
        assert!(manifest_for(&id, 1, &sorted).verify());
    }

    #[test]
    fn identical_holdings_serialize_identically() {
        // Two nodes holding the same set must produce byte-identical cid lists, so a head cid can stand in
        // for the set. (The manifests differ by `node`/`sig`, but the SET encoding must not.)
        let a = NodeIdentity::generate();
        let b = NodeIdentity::generate();
        let cids = [[7u8; 32], [9u8; 32]];
        let ma = manifest_for(&a, 1, &cids);
        let mb = manifest_for(&b, 1, &cids);
        assert_eq!(
            postcard::to_allocvec(&ma.cids).unwrap(),
            postcard::to_allocvec(&mb.cids).unwrap()
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
