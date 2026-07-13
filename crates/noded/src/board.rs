//! The verification **board service** (`VERIFICATION_DESIGN §5`, phases P5b-2a/2b) — wires
//! `zeph_com`'s [`Board`] CRDT into the node. It (a) **gossips** [`BoardSnapshot`]s to `census()`
//! peers (fire-and-forget, like the membership epidemic push), (b) **merges** inbound snapshots (the
//! board is a union CRDT, so merge is order-independent + idempotent), (c) runs a background
//! **verifier loop** that grabs pending requests via the cooldown [`Verifier`] scheduler, re-runs
//! the program deterministically ([`verify_locally`]), and posts its signed verdict back, and (d)
//! implements [`VerifyBackend`] — the **producer** side of a `verify` host-fn call: post a request,
//! gossip it, and poll [`zeph_com::Board::collected`] until the certificate arrives or it times out.
//!
//! Distribution is **additive**: `tag::BOARD` is an appended mux tag, so a node without this handler
//! simply drops the stream — board gossip is mixed-version-safe (a staggered roll, not simultaneous).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, RwLock};
use zeph_com::{
    verify_locally, Board, BoardSnapshot, PostedRequest, TransitionRuntime, Verifier, VerifierSet,
    VerifyBackend, VerifyPolicy, VerifyRequest, DEFAULT_FUEL,
};
use zeph_core::{hlc::Clock, Cid, NodeId};
use zeph_crypto::NodeIdentity;
use zeph_membership::Membership;
use zeph_obj::{ConsumeMode, ObjEngine};
use zeph_transport::{tag, PeerAddr, TaggedStream, Transport};

/// Cap on a gossiped snapshot frame (matches the other muxed services).
const MAX_FRAME: usize = 1 << 20;
/// A verifier's cooldown between grabbing jobs — spreads load + forces `k` distinct verifiers (§5).
const COOLDOWN_MS: u64 = 3_000;
/// Board gossip cadence (fire-and-forget epidemic push).
const GOSSIP_SECS: u64 = 5;
/// How often this node looks for a pending request to verify.
const VERIFY_SECS: u64 = 2;
/// How many census peers a gossip round pushes to.
const FANOUT: usize = 3;
/// How long a producer's `verify` call waits for the certificate before giving up (returns 0).
const VERIFY_TIMEOUT_SECS: u64 = 30;
/// How often the producer polls the board for its certificate while waiting.
const POLL_MS: u64 = 500;

/// The node-side verification board: the `com` [`Board`] CRDT + this node's verifier scheduler,
/// gossiped over `tag::BOARD` and driven by background loops.
pub struct BoardService {
    identity: Arc<NodeIdentity>,
    transport: Arc<Transport>,
    obj: Arc<ObjEngine>,
    clock: Arc<Clock>,
    runtime: TransitionRuntime,
    board: RwLock<Board>,
    /// This node's cooldown scheduler (which pending request it verifies next).
    scheduler: RwLock<Verifier>,
    /// The peer set to gossip to; injected after construction (mirrors governance).
    membership: RwLock<Option<Arc<Membership>>>,
}

impl BoardService {
    pub fn new(
        identity: Arc<NodeIdentity>,
        transport: Arc<Transport>,
        obj: Arc<ObjEngine>,
    ) -> Arc<Self> {
        let clock = transport.clock();
        let node = identity.node_id().0;
        Arc::new(Self {
            identity,
            transport,
            obj,
            clock,
            runtime: TransitionRuntime::new().expect("board verifier runtime"),
            board: RwLock::new(Board::new()),
            scheduler: RwLock::new(Verifier::new(node, COOLDOWN_MS)),
            membership: RwLock::new(None),
        })
    }

    /// Inject the membership handle whose `census()` supplies gossip targets.
    pub async fn set_membership(&self, m: Arc<Membership>) {
        *self.membership.write().await = Some(m);
    }

    /// A local producer posts a request (driven by the `verify` host fn via [`VerifyBackend`]):
    /// merge it in and gossip so verifiers pick it up.
    pub async fn post_request(&self, posted: PostedRequest) {
        self.board.write().await.post_request(posted);
        self.gossip_once().await;
    }

    /// Snapshot the current board (for a producer to poll `collected`, and for tests).
    #[allow(dead_code)]
    pub async fn snapshot(&self) -> BoardSnapshot {
        self.board.read().await.snapshot()
    }

    /// Serve inbound `tag::BOARD` streams (a peer's gossiped snapshot) and spawn the gossip +
    /// verifier loops. Mirrors the registry serve shape (per-stream spawn, read-to-end, decode).
    pub async fn serve(self: Arc<Self>, mut streams: mpsc::Receiver<TaggedStream>) {
        self.clone().spawn_loops();
        while let Some(TaggedStream {
            mut send, mut recv, ..
        }) = streams.recv().await
        {
            let this = self.clone();
            tokio::spawn(async move {
                let Ok(bytes) = recv.read_to_end(MAX_FRAME).await else {
                    return;
                };
                if let Ok(snap) = postcard::from_bytes::<BoardSnapshot>(&bytes) {
                    this.board.write().await.merge(snap);
                }
                // Fire-and-forget: ack with an empty frame so the pushing client's read completes.
                let _ = send.write_all(&[]).await;
                let _ = send.finish();
            });
        }
    }

    fn spawn_loops(self: Arc<Self>) {
        let g = self.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_secs(GOSSIP_SECS));
            loop {
                iv.tick().await;
                g.gossip_once().await;
            }
        });
        let v = self;
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_secs(VERIFY_SECS));
            loop {
                iv.tick().await;
                v.verify_once().await;
            }
        });
    }

    /// Push the current snapshot to a fanout of census peers (fire-and-forget, per peer).
    async fn gossip_once(&self) {
        let peers = self.fanout_peers().await;
        if peers.is_empty() {
            return;
        }
        let snap = self.board.read().await.snapshot();
        if snap.requests.is_empty() && snap.verdicts.is_empty() {
            return;
        }
        let Ok(bytes) = postcard::to_allocvec(&snap) else {
            return;
        };
        let bytes = Arc::new(bytes);
        for (_id, addr) in peers {
            let (t, b) = (self.transport.clone(), bytes.clone());
            tokio::spawn(async move {
                let _ = t.request_tagged(&addr, tag::BOARD, &b, MAX_FRAME).await;
            });
        }
    }

    /// Census peers to gossip to (self excluded), truncated to [`FANOUT`].
    async fn fanout_peers(&self) -> Vec<(NodeId, PeerAddr)> {
        let me = self.identity.node_id();
        let Some(m) = self.membership.read().await.clone() else {
            return Vec::new();
        };
        let mut peers: Vec<(NodeId, PeerAddr)> = m
            .census()
            .await
            .into_iter()
            .filter(|(id, _)| *id != me)
            .collect();
        peers.truncate(FANOUT);
        peers
    }

    /// One verifier step: if off cooldown, grab a pending request, re-run its program, and post a
    /// signed verdict + gossip. A missing program (wasm not fetchable) is skipped, not failed.
    pub async fn verify_once(&self) {
        let now_ms = self.clock.now().millis();
        // Pick a job under read locks, then drop the borrows before the (async) re-run.
        let job = {
            let board = self.board.read().await;
            let sched = self.scheduler.read().await;
            sched.select(&board, now_ms).cloned()
        };
        let Some(posted) = job else {
            return;
        };
        let Some(wasm) = self.fetch_program(posted.req.program_cid).await else {
            return;
        };
        let verdict = verify_locally(
            &self.runtime,
            &self.identity,
            &posted.req,
            &wasm,
            DEFAULT_FUEL,
        )
        .await;
        self.board.write().await.post_verdict(verdict);
        self.scheduler.write().await.mark_verified(now_ms);
        self.gossip_once().await;
    }

    /// Fetch a program's wasm by cid (following a `File` manifest to its content), per `account.rs`.
    async fn fetch_program(&self, cid: [u8; 32]) -> Option<Vec<u8>> {
        let raw = self.obj.get(Cid(cid), ConsumeMode::Drop).await.ok()?;
        match zeph_obj::Manifest::decode(&raw) {
            Some(zeph_obj::Manifest::File { content, .. }) => {
                self.obj.get(Cid(content), ConsumeMode::Drop).await.ok()
            }
            _ => Some(raw),
        }
    }
}

#[async_trait::async_trait]
impl VerifyBackend for BoardService {
    /// The producer side of a `verify` host-fn call: post `req` (as this node) with a baseline
    /// `k = 1, Open` policy, gossip it, then poll for the certificate until it collects or times out.
    /// Returns true iff an independent verifier confirmed the claim.
    async fn verify(&self, req: VerifyRequest) -> bool {
        let posted = PostedRequest {
            producer: self.identity.node_id().0,
            req,
            policy: VerifyPolicy {
                k: 1,
                set: VerifierSet::Open,
            },
        };
        self.post_request(posted.clone()).await; // merge locally + gossip so a verifier picks it up
        let poll = async {
            loop {
                if self.board.read().await.collected(&posted).is_some() {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(POLL_MS)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(VERIFY_TIMEOUT_SECS), poll)
            .await
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeph_com::{produce, Verdict, VerifierSet, VerifyPolicy};
    use zeph_obj::{ObjConfig, PeerSource};
    use zeph_routing::{ContentRouting, MetaRecord, ProviderRecord};
    use zeph_store::Store;
    use zeph_transport::Reach;

    // A consistency-critical shared counter: pure f = state + input (1 byte). No verify call → verifiable.
    const COUNTER_WAT: &[u8] = br#"(module
      (import "craftcom" "state"  (func $state  (param i32 i32) (result i32)))
      (import "craftcom" "input"  (func $input  (param i32 i32) (result i32)))
      (import "craftcom" "commit" (func $commit (param i32 i32) (result i32)))
      (memory (export "memory") 1)
      (func (export "f")
        (drop (call $state (i32.const 0) (i32.const 1)))
        (drop (call $input (i32.const 1) (i32.const 1)))
        (i32.store8 (i32.const 2)
          (i32.add (i32.load8_u (i32.const 0)) (i32.load8_u (i32.const 1))))
        (drop (call $commit (i32.const 2) (i32.const 1)))))"#;

    struct NullRouting;
    #[async_trait::async_trait]
    impl ContentRouting for NullRouting {
        async fn announce(&self, _: Cid, _: u32, _: bool) -> zeph_routing::Result<()> {
            Ok(())
        }
        async fn resolve(&self, _: Cid) -> zeph_routing::Result<Vec<ProviderRecord>> {
            Ok(vec![])
        }
        async fn withdraw(&self, _: Cid) -> zeph_routing::Result<()> {
            Ok(())
        }
        async fn announce_want(&self, _: Cid) -> zeph_routing::Result<()> {
            Ok(())
        }
        async fn withdraw_want(&self, _: Cid) -> zeph_routing::Result<()> {
            Ok(())
        }
        async fn is_wanted(&self, _: Cid) -> zeph_routing::Result<bool> {
            Ok(false)
        }
        async fn announce_meta(
            &self,
            _: Cid,
            _: u64,
            _: Option<String>,
        ) -> zeph_routing::Result<()> {
            Ok(())
        }
        async fn withdraw_meta(&self, _: Cid) -> zeph_routing::Result<()> {
            Ok(())
        }
        async fn metas(&self, _: Cid) -> zeph_routing::Result<Vec<MetaRecord>> {
            Ok(vec![])
        }
    }
    struct NullPeers;
    #[async_trait::async_trait]
    impl PeerSource for NullPeers {
        async fn peers(&self) -> Vec<(NodeId, PeerAddr)> {
            vec![]
        }
    }

    // The node-side verifier loop end-to-end: a program published to obj is fetched, RE-RUN, and a
    // signed verdict posted — the real integration of fetch_program + verify_locally + the scheduler.
    #[tokio::test]
    async fn verify_once_re_runs_a_published_program_and_posts_a_verdict() {
        let dir = tempfile::tempdir().unwrap();
        let identity = Arc::new(NodeIdentity::generate());
        let transport = Arc::new(
            Transport::bind(
                identity.secret_key_bytes(),
                Reach::LocalOnly,
                vec![zeph_transport::MUX_ALPN.to_vec()],
                0,
            )
            .await
            .unwrap(),
        );
        let store = Arc::new(Store::open(dir.path()).unwrap());
        let engine = ObjEngine::with_peer_source(
            transport.clone(),
            store,
            Arc::new(NullRouting),
            Arc::new(NullPeers),
            ObjConfig::default(),
        );
        let service = BoardService::new(identity.clone(), transport.clone(), engine.clone());

        // Publish the counter program so the verifier loop can fetch it by cid.
        engine.publish_system(COUNTER_WAT).await.unwrap();

        // A DIFFERENT node produced the claim (this node may only verify others' work).
        let rt = TransitionRuntime::new().unwrap();
        let req = produce(&rt, COUNTER_WAT, "f", &[5u8], &[3u8], 0, DEFAULT_FUEL)
            .await
            .unwrap();
        assert_eq!(req.claimed_output, vec![8], "5 + 3 = 8");
        let producer = NodeIdentity::generate().node_id().0;
        let posted = PostedRequest {
            producer,
            req,
            policy: VerifyPolicy {
                k: 1,
                set: VerifierSet::Open,
            },
        };
        service.post_request(posted.clone()).await;

        service.verify_once().await;
        assert!(
            service.board.read().await.satisfied(&posted),
            "the node fetched, re-ran the program, and posted a valid verdict → k=1 met"
        );
    }

    // The producer side (VerifyBackend::verify): posting a request and awaiting the certificate.
    // A single node can't verify its own request (no self-verification), so we pre-inject another
    // node's verdict — then verify() collects it and returns true without waiting out the timeout.
    #[tokio::test]
    async fn verify_backend_returns_true_once_another_node_confirms() {
        let dir = tempfile::tempdir().unwrap();
        let identity = Arc::new(NodeIdentity::generate());
        let transport = Arc::new(
            Transport::bind(
                identity.secret_key_bytes(),
                Reach::LocalOnly,
                vec![zeph_transport::MUX_ALPN.to_vec()],
                0,
            )
            .await
            .unwrap(),
        );
        let store = Arc::new(Store::open(dir.path()).unwrap());
        let engine = ObjEngine::with_peer_source(
            transport.clone(),
            store,
            Arc::new(NullRouting),
            Arc::new(NullPeers),
            ObjConfig::default(),
        );
        let service = BoardService::new(identity.clone(), transport, engine);

        let rt = TransitionRuntime::new().unwrap();
        let req = produce(&rt, COUNTER_WAT, "f", &[5u8], &[3u8], 0, DEFAULT_FUEL)
            .await
            .unwrap();

        // Another node (B) has already verified this exact request — seed its agree verdict. verify()
        // builds the same PostedRequest (producer = this node), so B's verdict counts (B != producer).
        let b = NodeIdentity::generate();
        service.board.write().await.post_verdict(Verdict::sign(
            &b,
            req.request_hash(),
            req.output_hash(),
            true,
        ));

        assert!(
            service.verify(req).await,
            "verify() collects the independent verdict and returns verified"
        );
    }
}
