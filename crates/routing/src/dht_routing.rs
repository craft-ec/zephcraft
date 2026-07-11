//! `DhtRouting` — the `ContentRouting` implementation backed by the Kademlia DHT
//! (`zeph-dht`) instead of the central tracker. Each routing record maps to a DHT key/value:
//!
//! - **provider / want / meta** — keyed by the CID (namespaced per kind), one signed record
//!   per publisher, all coexisting (many providers per CID). `seq` is a wall-clock stamp so
//!   re-announces (and 22h republishes) always advance and refresh; a withdraw stores an
//!   empty tombstone that supersedes and is skipped on read.
//! - **app** — an owner-keyed head, **highest-version wins** (no strict CAS; a DHT has no
//!   single authority — foundation §62). The key embeds the owner and reads filter to the
//!   owner's own signature, so only the owner can advance their head.
//!
//! Census + enumeration are NOT DHT-native: a DHT can't list all keys or all nodes, so the
//! trait carries no such methods. Fade uses per-CID want lookups (`is_wanted`) instead.

use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use zeph_core::{Cid, NodeId};
use zeph_dht::DhtNode;

use crate::records::{
    AppPayload, MetaPayload, ProviderPayload, WantPayload, KIND_APP, KIND_META, KIND_PROVIDER,
    KIND_WANT,
};
use crate::{AppRecord, ContentRouting, MetaRecord, Result};

/// Content routing over the DHT.
pub struct DhtRouting {
    dht: Arc<DhtNode>,
    self_addr: String,
    /// Monotonic freshness counter for re-announceable records (provider/want/meta): tracks
    /// wall-clock ms but always advances, so a same-millisecond re-announce or withdraw is
    /// never rejected as an equal seq.
    seq: AtomicU64,
}

impl DhtRouting {
    pub fn new(dht: Arc<DhtNode>) -> Self {
        let self_addr = dht.contact().addr.to_string();
        Self {
            dht,
            self_addr,
            seq: AtomicU64::new(now_millis()),
        }
    }

    /// Next freshness seq: monotonic and roughly wall-clock, so re-announces/withdraws in the
    /// same millisecond still strictly advance.
    fn next_seq(&self) -> u64 {
        let now = now_millis();
        let mut cur = self.seq.load(Ordering::SeqCst);
        loop {
            let next = (cur + 1).max(now);
            match self
                .seq
                .compare_exchange_weak(cur, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return next,
                Err(actual) => cur = actual,
            }
        }
    }

    /// DHT key for a per-CID record kind (provider/want/meta) — namespaced so kinds under
    /// the same CID never collide.
    fn cid_key(kind: u8, cid: &Cid) -> [u8; 32] {
        let mut b = Vec::with_capacity(1 + 32);
        b.push(kind);
        b.extend_from_slice(&cid.0);
        Cid::of(&b).0
    }

    /// DHT key for an owner-keyed head (app): kind ‖ owner ‖ name.
    fn owned_key(kind: u8, owner: &[u8; 32], name: &str) -> [u8; 32] {
        let mut b = Vec::with_capacity(1 + 32 + name.len());
        b.push(kind);
        b.extend_from_slice(owner);
        b.extend_from_slice(name.as_bytes());
        Cid::of(&b).0
    }

    fn me(&self) -> [u8; 32] {
        self.dht.contact().id.0
    }

    async fn put(&self, key: [u8; 32], value: Vec<u8>, seq: u64) {
        self.dht.put(key, seq, value).await;
    }
}

#[async_trait]
impl ContentRouting for DhtRouting {
    // ---- provider records --------------------------------------------------------

    async fn announce(&self, cid: Cid, piece_count: u32, pinned: bool) -> Result<()> {
        let payload = ProviderPayload {
            cid: cid.0,
            piece_count,
            addr: self.self_addr.clone(),
            pinned,
        };
        let value = postcard::to_allocvec(&payload).expect("provider payload serializes");
        self.put(Self::cid_key(KIND_PROVIDER, &cid), value, self.next_seq())
            .await;
        Ok(())
    }

    async fn resolve(&self, cid: Cid) -> Result<Vec<crate::ProviderRecord>> {
        let recs = self.dht.get(Self::cid_key(KIND_PROVIDER, &cid)).await;
        Ok(recs
            .into_iter()
            .filter_map(|r| {
                let p: ProviderPayload = postcard::from_bytes(&r.value).ok()?;
                let addr = p.addr.parse().ok()?;
                Some(crate::ProviderRecord {
                    node_id: NodeId(r.publisher),
                    addr,
                    piece_count: p.piece_count,
                    pinned: p.pinned,
                })
            })
            .collect())
    }

    async fn withdraw(&self, cid: Cid) -> Result<()> {
        // Tombstone: an empty value at a fresh seq supersedes our provider record; readers
        // fail to decode it and skip us. TTL reclaims it.
        self.put(
            Self::cid_key(KIND_PROVIDER, &cid),
            Vec::new(),
            self.next_seq(),
        )
        .await;
        Ok(())
    }

    // ---- want signals ------------------------------------------------------------

    async fn announce_want(&self, cid: Cid) -> Result<()> {
        let value = postcard::to_allocvec(&WantPayload { cid: cid.0 }).expect("want serializes");
        self.put(Self::cid_key(KIND_WANT, &cid), value, self.next_seq())
            .await;
        Ok(())
    }

    async fn withdraw_want(&self, cid: Cid) -> Result<()> {
        self.put(Self::cid_key(KIND_WANT, &cid), Vec::new(), self.next_seq())
            .await;
        Ok(())
    }

    async fn is_wanted(&self, cid: Cid) -> Result<bool> {
        let recs = self.dht.get(Self::cid_key(KIND_WANT, &cid)).await;
        // A live want is a decodable payload; a withdrawn one is an empty tombstone.
        Ok(recs
            .iter()
            .any(|r| postcard::from_bytes::<WantPayload>(&r.value).is_ok()))
    }

    // ---- editable metadata -------------------------------------------------------

    async fn announce_meta(
        &self,
        cid: Cid,
        published_at: u64,
        comment: Option<String>,
    ) -> Result<()> {
        let payload = MetaPayload {
            cid: cid.0,
            published_at,
            comment,
        };
        let value = postcard::to_allocvec(&payload).expect("meta serializes");
        self.put(Self::cid_key(KIND_META, &cid), value, self.next_seq())
            .await;
        Ok(())
    }

    async fn withdraw_meta(&self, cid: Cid) -> Result<()> {
        self.put(Self::cid_key(KIND_META, &cid), Vec::new(), self.next_seq())
            .await;
        Ok(())
    }

    async fn metas(&self, cid: Cid) -> Result<Vec<MetaRecord>> {
        let recs = self.dht.get(Self::cid_key(KIND_META, &cid)).await;
        Ok(recs
            .into_iter()
            .filter_map(|r| {
                let p: MetaPayload = postcard::from_bytes(&r.value).ok()?;
                Some(MetaRecord {
                    publisher: NodeId(r.publisher),
                    published_at: p.published_at,
                    comment: p.comment,
                })
            })
            .collect())
    }

    // ---- owner-keyed app head (highest-version-wins) -----------------------------

    async fn announce_app(&self, name: &str, wasm_cid: Cid, version: u64) -> Result<()> {
        let payload = AppPayload {
            name: name.to_string(),
            wasm_cid: wasm_cid.0,
            version,
        };
        let value = postcard::to_allocvec(&payload).expect("app serializes");
        self.put(Self::owned_key(KIND_APP, &self.me(), name), value, version)
            .await;
        Ok(())
    }

    async fn resolve_app(&self, publisher: NodeId, name: &str) -> Result<Option<AppRecord>> {
        let recs = self
            .dht
            .get(Self::owned_key(KIND_APP, &publisher.0, name))
            .await;
        Ok(recs
            .into_iter()
            .filter(|r| r.publisher == publisher.0)
            .filter_map(|r| postcard::from_bytes::<AppPayload>(&r.value).ok())
            .max_by_key(|p| p.version)
            .map(|p| AppRecord {
                publisher,
                name: p.name,
                wasm_cid: Cid(p.wasm_cid),
                version: p.version,
            }))
    }
}

/// Wall-clock milliseconds since the epoch.
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use zeph_crypto::NodeIdentity;
    use zeph_dht::Contact;
    use zeph_transport::{Reach, Transport};

    async fn node() -> (Arc<DhtNode>, Contact) {
        let id = Arc::new(NodeIdentity::generate());
        let transport = Arc::new(
            Transport::bind(
                id.secret_key_bytes(),
                Reach::LocalOnly,
                vec![zeph_transport::MUX_ALPN.to_vec()],
                0,
            )
            .await
            .expect("bind"),
        );
        let n = DhtNode::new(id, transport.clone(), 3_600_000);
        let (tx, rx) = mpsc::channel(64);
        let t = transport.clone();
        tokio::spawn(async move { t.serve(vec![(zeph_transport::tag::DHT, tx)]).await });
        n.clone().serve(rx);
        let c = n.contact();
        (n, c)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn providers_and_heads_route_over_the_dht() {
        let (_seed, seed_c) = node().await;
        let (a_n, _) = node().await;
        a_n.bootstrap(vec![seed_c.clone()]).await;
        let (b_n, _) = node().await;
        b_n.bootstrap(vec![seed_c.clone()]).await;
        let a = DhtRouting::new(a_n.clone());
        let b = DhtRouting::new(b_n.clone());
        let owner = a_n.contact().id;

        // A announces itself as a provider; B resolves and finds A (dialable, with counts).
        let cid = Cid([7u8; 32]);
        a.announce(cid, 5, true).await.unwrap();
        let providers = b.resolve(cid).await.unwrap();
        assert_eq!(providers.len(), 1, "one provider");
        assert_eq!(providers[0].node_id, owner);
        assert_eq!(providers[0].piece_count, 5);
        assert!(providers[0].pinned);

        // Owner-keyed app head: highest-version-wins across two publishes.
        a.announce_app("chat", Cid([1u8; 32]), 1).await.unwrap();
        a.announce_app("chat", Cid([2u8; 32]), 2).await.unwrap();
        let app = b.resolve_app(owner, "chat").await.unwrap().expect("app");
        assert_eq!(app.version, 2);
        assert_eq!(app.wasm_cid, Cid([2u8; 32]));

        // Withdraw removes A as a provider.
        a.withdraw(cid).await.unwrap();
        assert!(
            b.resolve(cid).await.unwrap().is_empty(),
            "withdrawn provider is gone"
        );

        // A second, independent provider coexists under the same CID.
        let cid2 = Cid([9u8; 32]);
        a.announce(cid2, 2, false).await.unwrap();
        b.announce(cid2, 3, false).await.unwrap();
        let both = a.resolve(cid2).await.unwrap();
        assert_eq!(both.len(), 2, "two providers coexist under one cid");

        // Per-cid want signal (Fade's replacement for wanted_cids enumeration).
        let wcid = Cid([11u8; 32]);
        assert!(!b.is_wanted(wcid).await.unwrap(), "nothing wants it yet");
        a.announce_want(wcid).await.unwrap();
        assert!(
            b.is_wanted(wcid).await.unwrap(),
            "want visible network-wide"
        );
        a.withdraw_want(wcid).await.unwrap();
        assert!(!b.is_wanted(wcid).await.unwrap(), "withdrawn want is gone");
    }
}
