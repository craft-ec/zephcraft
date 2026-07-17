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
//! O(1) cid comparison with no member data moved. On a CHANGE the reader fetches a [`Body::Diff`] naming
//! only what moved, never the set: the publisher already knows what it added and removed, so making every
//! reader re-derive that from a ~32 MB set would be pure waste. The content-address IS the Merkle root
//! here — the head cid already commits to the whole body, so a separate tree would only duplicate it.
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
/// What a reader learned about a peer's holdings — and, crucially, HOW MUCH it learned.
///
/// The two variants are different claims and must not be collapsed into one shape: a `Delta` with an empty
/// `removed` says "it dropped nothing", while a `Reset` says "I cannot tell you what it dropped, here is
/// the truth instead". Returning the latter as the former is a silent lie that leaves the reader believing
/// a peer still holds a cid it dropped — and a phantom holder means repair never fires for that cid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Changes {
    /// Exactly what moved since the reader's baseline.
    Delta {
        version: u64,
        head: [u8; 32],
        added: Vec<[u8; 32]>,
        removed: Vec<[u8; 32]>,
    },
    /// The reader's baseline is unreachable (first sight, or it fell further behind than the chain goes),
    /// so this is the peer's WHOLE current set. The caller must REPLACE what it believed about this peer
    /// rather than merge into it: one of our cids absent from `set` is one this peer no longer holds.
    Reset {
        version: u64,
        head: [u8; 32],
        set: Vec<[u8; 32]>,
    },
}

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
    /// Where the version high-water mark is persisted. See [`ManifestStore::resume_version`].
    version_file: std::path::PathBuf,
}

/// Read the persisted version high-water mark. Unreadable/corrupt/absent all mean 0 — publishing from 1 is
/// exactly the old behaviour, so a bad read degrades to the old bug rather than to a node that cannot
/// publish at all.
fn resume_version(path: &std::path::Path) -> u64 {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

fn save_version(path: &std::path::Path, version: u64) -> std::io::Result<()> {
    std::fs::write(path, version.to_string())
}

/// Walk `prev` back from `head` until we reach EITHER the reader's own baseline or a snapshot, returning
/// the chain (head-first) and which of the two we hit.
///
/// Reaching the baseline is what keeps a reader that merely fell BEHIND out of the re-baseline path: every
/// intermediate diff is on this chain, so its removals are fully computable. Treating "behind" as "no
/// baseline" would silently drop them — and a peer we still believe holds a cid it dropped is a phantom
/// holder that no later diff ever corrects, because the peer computes its diffs against its own current
/// set and will never mention that cid again.
///
/// `fetch` is a parameter so the decision is testable without a network — the walk is the part of this
/// protocol with a subtle correctness property, so it must not be the part that only production exercises.
async fn walk_chain<F, Fut>(
    head: HoldingsManifest,
    known_cid: Option<[u8; 32]>,
    mut fetch: F,
) -> Option<(Vec<HoldingsManifest>, bool)>
where
    F: FnMut([u8; 32]) -> Fut,
    Fut: std::future::Future<Output = Option<HoldingsManifest>>,
{
    let mut chain = vec![head];
    loop {
        let Body::Diff { prev, .. } = &chain.last()?.body else {
            return Some((chain, false)); // reached a snapshot: the reader must re-baseline
        };
        let prev = *prev;
        if Some(prev) == known_cid {
            return Some((chain, true)); // we already know the state at `prev`; no need to fetch it
        }
        if chain.len() as u64 > SNAPSHOT_EVERY * 2 {
            return None; // chain longer than it should ever be — refuse rather than walk forever
        }
        chain.push(fetch(prev).await?);
    }
}

/// Fold a chain of diffs (head-first, all `Body::Diff`) into ONE net delta against the reader's baseline.
///
/// Applying each diff in publish order lets a cid that moved twice settle on its final side; `verify`
/// guarantees no single diff claims a cid on both sides, so the result is unambiguous.
fn net_delta(chain: &[HoldingsManifest]) -> (Vec<[u8; 32]>, Vec<[u8; 32]>) {
    let (mut added, mut removed) = (HashSet::new(), HashSet::new());
    for m in chain.iter().rev() {
        if let Body::Diff {
            added: a,
            removed: r,
            ..
        } = &m.body
        {
            for c in r {
                added.remove(c);
                removed.insert(*c);
            }
            for c in a {
                removed.remove(c);
                added.insert(*c);
            }
        }
    }
    (sorted(&added), sorted(&removed))
}

/// Fold a chain whose LAST entry is a snapshot (head-first) into the full set it describes.
fn fold_set(chain: &[HoldingsManifest]) -> Option<Vec<[u8; 32]>> {
    let mut set: HashSet<[u8; 32]> = match &chain.last()?.body {
        Body::Snapshot(cids) => cids.iter().copied().collect(),
        Body::Diff { .. } => return None, // caller only folds a chain that bottomed out at a snapshot
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
    Some(sorted(&set))
}

impl ManifestStore {
    pub fn new(
        identity: Arc<NodeIdentity>,
        obj: Arc<ObjEngine>,
        routing: Arc<dyn ContentRouting>,
        data_dir: &std::path::Path,
    ) -> Self {
        Self {
            identity,
            obj,
            routing,
            last: tokio::sync::Mutex::new(None),
            version_file: data_dir.join("holdings_version"),
        }
    }

    /// The version this process must publish ABOVE — the high-water mark of every previous process.
    ///
    /// The version is not a private counter: `announce_app` uses it as the DHT record's `seq`, and the
    /// record store rejects `seq <= existing`. A restart that resets to 1 therefore meets its OWN
    /// pre-restart record and every republish is refused until the count climbs back past it — the node's
    /// holdings head frozen at a stale manifest, every change it makes invisible, for up to the record TTL
    /// (1h). A long absence self-heals when the record expires; a quick restart does not, which is the
    /// common case, and a home node restarting daily hits it every morning.
    ///
    /// Unreadable or corrupt is treated as 0: publishing from 1 is exactly today's behaviour, so a bad
    /// read degrades to the old bug rather than to a node that cannot publish at all.
    fn resume_version(&self) -> u64 {
        resume_version(&self.version_file)
    }

    /// Record the high-water mark. Best-effort: a failed write costs us the old restart bug next boot, not
    /// this publish, so it must not fail the publish.
    fn save_version(&self, version: u64) {
        if let Err(e) = save_version(&self.version_file, version) {
            tracing::warn!(error = %e, "could not persist holdings version — a restart may stall republish");
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
            // First publish of THIS process. Resume above every previous process's version (see
            // `resume_version`) and send a SNAPSHOT: we deliberately do not persist the last SET — that is
            // the ~32 MB write this whole design exists to avoid — so we have no baseline to diff against.
            // Readers whose baseline is now unreachable take `Changes::Reset` and re-baseline, which costs
            // one full-set fetch per peer per restart. That is the correct trade at any sane restart rate.
            None => (self.resume_version() + 1, Body::Snapshot(sorted(&now))),
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
        let cid = self.obj.publish_local(&bytes).await.ok()?;
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
        self.save_version(version);
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
    /// This is where the diff pays off. A reader holding `known` gets an O(Δ) answer: one small fetch
    /// naming exactly what moved. Only a reader whose baseline is unreachable pays for a set, and even then
    /// the walk back down the `prev` chain is bounded by [`SNAPSHOT_EVERY`].
    pub async fn changes_since(
        &self,
        peer: [u8; 32],
        known: Option<(u64, [u8; 32])>,
    ) -> Option<Changes> {
        let (head_cid, head_version) = self.peer_head(peer).await?;
        if let Some((kv, kc)) = known {
            if kc == head_cid {
                // Unchanged — the O(1) steady state. Nothing fetched.
                return Some(Changes::Delta {
                    version: kv,
                    head: head_cid,
                    added: Vec::new(),
                    removed: Vec::new(),
                });
            }
        }
        let known_cid = known.map(|(_, kc)| kc);
        let head = self.fetch_at(peer, head_cid).await?;
        let (chain, reached_baseline) =
            walk_chain(head, known_cid, |cid| self.fetch_at(peer, cid)).await?;

        if reached_baseline {
            let (added, removed) = net_delta(&chain);
            return Some(Changes::Delta {
                version: head_version,
                head: head_cid,
                added,
                removed,
            });
        }

        // No usable baseline: fold the snapshot forward and hand back the WHOLE set for the caller to
        // reconcile against. `Reset` rather than `Delta{removed: []}` on purpose — those two are not the
        // same claim, and conflating them is exactly how a dropped cid becomes invisible forever.
        Some(Changes::Reset {
            version: head_version,
            head: head_cid,
            set: fold_set(&chain)?,
        })
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

    fn diff_for_prev(
        id: &NodeIdentity,
        version: u64,
        added: &[[u8; 32]],
        removed: &[[u8; 32]],
        prev: [u8; 32],
    ) -> HoldingsManifest {
        let mut m = diff_for(id, version, added, removed);
        if let Body::Diff { prev: p, .. } = &mut m.body {
            *p = prev;
        }
        m.sig = id
            .sign(&signing_bytes(&m.node, m.version, &m.body))
            .to_vec();
        m
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

    // Cids are opaque to the walk — it only compares them and fetches by them — so tests name them
    // directly rather than round-tripping through obj to content-address a manifest.
    const C1: [u8; 32] = [0xC1; 32];
    const C2: [u8; 32] = [0xC2; 32];

    /// The walk's entire dependency on the network, faked: cid -> manifest.
    fn served(
        entries: &[([u8; 32], HoldingsManifest)],
    ) -> std::collections::HashMap<[u8; 32], HoldingsManifest> {
        entries.iter().cloned().collect()
    }

    #[test]
    fn the_version_survives_a_restart() {
        // The version is not a private counter: announce_app uses it as the DHT record's seq, and the
        // record store rejects seq <= existing. A restart that resets to 1 meets its OWN pre-restart
        // record (seq=N) and every republish is REFUSED until it climbs back past N — holdings head frozen
        // at a stale manifest for up to the 1h record TTL. A home node restarting daily hits this every
        // morning, which is the fleet this is for.
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("holdings_version");

        assert_eq!(
            resume_version(&f),
            0,
            "no file yet: publish from 1, as before"
        );
        save_version(&f, 7).unwrap();
        assert_eq!(
            resume_version(&f),
            7,
            "a 'restart' must resume above the old high-water mark"
        );

        // Corrupt/truncated must not wedge publishing — degrade to the old behaviour, never to silence.
        std::fs::write(&f, "not-a-number").unwrap();
        assert_eq!(resume_version(&f), 0);
    }

    #[tokio::test]
    async fn a_reader_that_fell_behind_reaches_its_baseline_instead_of_re_baselining() {
        // THE bug this structure invites, and the one the fast path hides: a reader ONE version behind gets
        // a true `removed`, so everything looks correct — while a reader TWO behind silently re-baselines
        // and loses every removal in the gap. Falling behind is ordinary, not exotic: publish and
        // anti-entropy are independent 60s timers, and a single missed tick is enough.
        let id = NodeIdentity::generate();
        let v1 = snapshot_for(&id, 1, &[[1u8; 32], [2u8; 32], [3u8; 32]]);
        let v2 = diff_for_prev(&id, 2, &[[4u8; 32]], &[[1u8; 32]], C1);
        let v3 = diff_for_prev(&id, 3, &[[5u8; 32]], &[[2u8; 32]], C2);
        let store = served(&[(C1, v1), (C2, v2)]);

        // Two versions behind: baseline C1 is reachable, so this is a DELTA, not a reset.
        let (chain, reached) = walk_chain(v3, Some(C1), |c| {
            let store = &store;
            async move { store.get(&c).cloned() }
        })
        .await
        .unwrap();
        assert!(
            reached,
            "the baseline is on the chain — the walk must stop there"
        );
        assert_eq!(
            chain.len(),
            2,
            "it must NOT re-fetch the baseline it already has"
        );
        let (added, removed) = net_delta(&chain);
        assert_eq!(added, vec![[4u8; 32], [5u8; 32]]);
        assert_eq!(
            removed,
            vec![[1u8; 32], [2u8; 32]],
            "both gap removals must survive; dropping them is a phantom holder forever"
        );
    }

    #[tokio::test]
    async fn a_reader_with_no_reachable_baseline_re_baselines() {
        // The other side of the same decision: a reader whose baseline is NOT on the chain (first sight, or
        // it fell further behind than the chain reaches) gets the whole set, and the caller replaces.
        let id = NodeIdentity::generate();
        let v1 = snapshot_for(&id, 1, &[[1u8; 32], [2u8; 32]]);
        let v2 = diff_for_prev(&id, 2, &[[3u8; 32]], &[[1u8; 32]], C1);
        let store = served(&[(C1, v1)]);

        for baseline in [None, Some([0xFEu8; 32])] {
            let (chain, reached) = walk_chain(v2.clone(), baseline, |c| {
                let store = &store;
                async move { store.get(&c).cloned() }
            })
            .await
            .unwrap();
            assert!(!reached);
            assert_eq!(fold_set(&chain), Some(vec![[2u8; 32], [3u8; 32]]));
        }
    }

    #[tokio::test]
    async fn a_walk_refuses_a_chain_that_never_bottoms_out() {
        // A peer serving an endless (or cyclic) prev chain must cost us a bounded walk, not a hang.
        let id = NodeIdentity::generate();
        let endless = diff_for_prev(&id, 2, &[[1u8; 32]], &[], C1);
        let reply = endless.clone();
        let out = walk_chain(endless, Some([0xEEu8; 32]), |_| {
            let reply = reply.clone();
            async move { Some(reply) } // never a snapshot, never the baseline
        })
        .await;
        assert_eq!(out, None, "bounded refusal, not an unbounded walk");
    }

    #[test]
    fn a_cid_that_moves_twice_settles_on_its_final_side() {
        // Folding a gap must not report a cid as both added and removed — the caller applies these to an
        // index, and a contradictory pair would leave the result dependent on which side it applied first.
        let id = NodeIdentity::generate();
        let (x, y) = ([7u8; 32], [8u8; 32]);
        // Publish order is v2 then v3, so the chain (head-first) is [v3, v2]:
        //   x: removed at v2, re-added at v3  => net ADDED
        //   y: added at v2, removed at v3     => net REMOVED
        let chain = vec![diff_for(&id, 3, &[x], &[y]), diff_for(&id, 2, &[y], &[x])];
        let (added, removed) = net_delta(&chain);
        assert_eq!(added, vec![x]);
        assert_eq!(removed, vec![y]);
    }

    #[test]
    fn folding_a_snapshot_chain_reconstructs_the_current_set() {
        let id = NodeIdentity::generate();
        let (a, b, c, d) = ([1u8; 32], [2u8; 32], [3u8; 32], [4u8; 32]);
        // head-first: v3 = Diff{+d,-b} <- v2 = Diff{-a} <- v1 = Snapshot{a,b,c}
        let chain = vec![
            diff_for(&id, 3, &[d], &[b]),
            diff_for(&id, 2, &[], &[a]),
            snapshot_for(&id, 1, &[a, b, c]),
        ];
        assert_eq!(fold_set(&chain), Some(vec![c, d]));
    }

    #[test]
    fn folding_a_chain_that_never_reaches_a_snapshot_yields_nothing() {
        // Rather than hand back a set folded from an unknown starting point, which would be a fabrication.
        let id = NodeIdentity::generate();
        assert_eq!(fold_set(&[diff_for(&id, 2, &[[9u8; 32]], &[])]), None);
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
