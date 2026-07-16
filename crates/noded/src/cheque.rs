//! The serving-cheque service — the node-side transport hook for SWAP-style egress cheques
//! (`ECONOMIC_LAYER_DESIGN.md` §7; §11 step 2). It bridges obj's fetch metering ([`ByteMeter`]) to the
//! [`zeph_cheque`] core:
//! - **Consumer side:** obj fires [`on_bytes_received`](ByteMeter::on_bytes_received) for each verified
//!   piece this node fetches from a provider. We accumulate per-provider against a CREDIT BAND, and when
//!   it crosses, issue a cumulative [`ServingCheque`] and push it fire-and-forget over `tag::CHEQUE`.
//!   This is the DECOUPLED model — the piece hot-path is untouched; cheques ride their own tag.
//! - **Provider side:** [`serve`](ChequeService::serve) records inbound cheques into a [`ChequeBook`],
//!   whose `total_earned()` is the node's cheque-proven serving MEASUREMENT (surfaced in step 3).
//!
//! Settlement (allocating a consumer's paid quota across its cheques, `zeph_cheque::allocate_quota`) is
//! the ledger's job (step 4); this service only accumulates + exchanges the signed tallies.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, RwLock};
use zeph_cheque::{ChequeBook, ChequeIssuer, ServingCheque};
use zeph_core::hlc::Clock;
use zeph_core::NodeId;
use zeph_crypto::NodeIdentity;
use zeph_membership::Membership;
use zeph_obj::ByteMeter;
use zeph_transport::{tag, TaggedStream, Transport};

/// Bytes fetched from one provider before issuing + pushing a cumulative cheque (the SWAP credit band).
/// Bounds the provider's un-cheque'd credit exposure and the push rate (one push per ~band per provider).
const CREDIT_BAND: u64 = 4 * 1024 * 1024;
/// Max cheque frame (a `ServingCheque` is small — a few fixed fields + a 64-byte sig).
const MAX_CHEQUE_FRAME: usize = 4096;
/// Cold-start reciprocity grant (§8): a node with zero contribution may still fetch this much for free
/// before the admission gate requires reciprocity (having served others) or payment. A governed policy
/// value later — native default for the MVP.
const COLD_START_GRANT: u64 = 1 << 30; // 1 GiB

pub struct ChequeService {
    identity: Arc<NodeIdentity>,
    clock: Arc<Clock>,
    transport: Arc<Transport>,
    membership: RwLock<Option<Arc<Membership>>>,
    /// Consumer side — the cumulative acknowledged to each provider.
    issuer: Mutex<ChequeIssuer>,
    /// Provider side — the latest cheque from each consumer; `total_earned` is the measurement.
    book: Mutex<ChequeBook>,
    /// Per-provider bytes accumulated since the last cheque (the credit band).
    pending: Mutex<HashMap<[u8; 32], u64>>,
    /// Total bytes this node has fetched from providers (the CONSUMED side of reciprocity). Reciprocity
    /// headroom = `total_earned − consumed`; the admission gate reads it synchronously.
    consumed: AtomicU64,
    /// Cheques to push (drained by [`run_pusher`](ChequeService::run_pusher)); `on_bytes_received` must
    /// not block, so it enqueues here rather than doing the network push inline.
    push_tx: mpsc::Sender<ServingCheque>,
}

impl ChequeService {
    /// Build the service; returns it plus the push receiver to hand to [`run_pusher`].
    pub fn new(
        identity: Arc<NodeIdentity>,
        clock: Arc<Clock>,
        transport: Arc<Transport>,
    ) -> (Arc<Self>, mpsc::Receiver<ServingCheque>) {
        let me = identity.node_id().0;
        let (push_tx, push_rx) = mpsc::channel(256);
        let svc = Arc::new(Self {
            identity,
            clock,
            transport,
            membership: RwLock::new(None),
            issuer: Mutex::new(ChequeIssuer::new()),
            book: Mutex::new(ChequeBook::new(me)),
            pending: Mutex::new(HashMap::new()),
            consumed: AtomicU64::new(0),
            push_tx,
        });
        (svc, push_rx)
    }

    /// The free-tier admission decision (§8 tit-for-tat): may this node fetch `bytes` right now? Yes while
    /// its CONSUMED total stays within what it has EARNED (cheque-proven bytes served to others) plus the
    /// cold-start grant. Beyond that a fetch must be paid for (a cheque) or wait until the node contributes
    /// more. Synchronous (atomics + the book lock), so the obj admission gate can call it inline.
    pub fn reciprocity_admits(&self, bytes: u64) -> bool {
        let consumed = self.consumed.load(Ordering::Relaxed);
        let budget = self.total_earned().saturating_add(COLD_START_GRANT);
        consumed.saturating_add(bytes) <= budget
    }

    /// Inject the membership handle used to resolve a provider's address for the push.
    pub async fn set_membership(&self, m: Arc<Membership>) {
        *self.membership.write().await = Some(m);
    }

    /// The node's total cheque-proven bytes served (sum of the latest cumulative per consumer) — the
    /// serving-contribution MEASUREMENT. Recorded now (this phase); step 3 (P3) reads it into the
    /// participation metric, so it has no in-binary caller yet.
    #[allow(dead_code)]
    pub fn total_earned(&self) -> u64 {
        self.book.lock().expect("cheque book lock").total_earned()
    }

    /// The PROOF behind this node's serving measurement — the latest counterparty-signed cheque per
    /// consumer (each names this node as `server`). The settlement loop attaches this to its announcement
    /// so other nodes can VERIFY the claimed served bytes rather than trust them (anti-farming).
    pub fn serving_proof(&self) -> Vec<ServingCheque> {
        self.book.lock().expect("cheque book lock").cheques()
    }

    /// MERGE `cheques` into the book (recovery). `record` keeps the highest cumulative per consumer, so
    /// this never downgrades a fresher cheque — used on startup to rebuild the book from this node's own
    /// durable settlement-report proof after a restart / total data loss.
    pub fn load_cheques(&self, cheques: Vec<ServingCheque>) {
        let mut book = self.book.lock().expect("cheque book lock");
        for c in cheques {
            book.record(c);
        }
    }

    /// Drain the push queue: resolve each cheque's provider address and push it fire-and-forget on
    /// `tag::CHEQUE` (reply ignored, like `tag::BOARD`). No-op targets (unknown addr) are dropped.
    pub async fn run_pusher(self: Arc<Self>, mut rx: mpsc::Receiver<ServingCheque>) {
        while let Some(cheque) = rx.recv().await {
            let addr = {
                let guard = self.membership.read().await;
                let Some(m) = guard.as_ref() else { continue };
                m.member_addr(NodeId(cheque.server)).await
            };
            let Some(addr) = addr else { continue };
            if let Ok(bytes) = postcard::to_allocvec(&cheque) {
                let _ = self
                    .transport
                    .request_tagged(&addr, tag::CHEQUE, &bytes, MAX_CHEQUE_FRAME)
                    .await;
            }
        }
    }

    /// Serve inbound cheque pushes (`tag::CHEQUE`): record each valid cheque naming this node as server.
    /// Fire-and-forget — the sender ignores the reply, so we just close the stream after recording.
    pub async fn serve(self: Arc<Self>, mut streams: mpsc::Receiver<TaggedStream>) {
        while let Some(TaggedStream {
            mut send, mut recv, ..
        }) = streams.recv().await
        {
            let me = self.clone();
            tokio::spawn(async move {
                if let Ok(bytes) = recv.read_to_end(MAX_CHEQUE_FRAME).await {
                    if let Ok(cheque) = postcard::from_bytes::<ServingCheque>(&bytes) {
                        me.book.lock().expect("cheque book lock").record(cheque);
                    }
                }
                let _ = send.finish();
            });
        }
    }
}

impl ByteMeter for ChequeService {
    /// obj fires this inline for each verified piece fetched from `provider`. Accumulate against the
    /// credit band; when it crosses, issue a cumulative cheque and ENQUEUE the push (non-blocking — no
    /// IO here). Below the band we just accumulate (no cheque yet), bounding the push rate.
    fn on_bytes_received(&self, provider: NodeId, bytes: u64) {
        // Track the CONSUMED side of reciprocity (every byte fetched from a provider).
        self.consumed.fetch_add(bytes, Ordering::Relaxed);
        let crossed = {
            let mut pending = self.pending.lock().expect("cheque pending lock");
            let acc = pending.entry(provider.0).or_default();
            *acc += bytes;
            if *acc >= CREDIT_BAND {
                Some(std::mem::take(acc))
            } else {
                None
            }
        };
        if let Some(additional) = crossed {
            let ts = self.clock.now().millis();
            let cheque = self.issuer.lock().expect("cheque issuer lock").issue(
                &self.identity,
                provider.0,
                additional,
                ts,
            );
            // Non-blocking: if the push queue is full, drop — the next band re-issues a fresh cumulative.
            let _ = self.push_tx.try_send(cheque);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeph_transport::{Reach, MUX_ALPN};

    async fn service() -> (Arc<ChequeService>, mpsc::Receiver<ServingCheque>) {
        let identity = Arc::new(NodeIdentity::generate());
        let clock = Arc::new(Clock::new());
        let transport = Arc::new(
            Transport::bind(
                identity.secret_key_bytes(),
                Reach::LocalOnly,
                vec![MUX_ALPN.to_vec()],
                0,
            )
            .await
            .unwrap(),
        );
        ChequeService::new(identity, clock, transport)
    }

    #[tokio::test]
    async fn credit_band_batches_into_a_cumulative_cheque() {
        let (svc, mut push_rx) = service().await;
        let provider = NodeId([5u8; 32]);

        // Below the band → no cheque enqueued yet.
        svc.on_bytes_received(provider, CREDIT_BAND - 1);
        assert!(
            push_rx.try_recv().is_err(),
            "no cheque below the credit band"
        );

        // Crossing the band → one cumulative cheque for everything accumulated.
        svc.on_bytes_received(provider, 10);
        let cheque = push_rx
            .try_recv()
            .expect("a cheque is enqueued at the band");
        assert_eq!(cheque.server, provider.0);
        assert_eq!(
            cheque.cumulative_bytes,
            CREDIT_BAND + 9,
            "the cheque is CUMULATIVE across the whole band"
        );
        assert!(cheque.verify(), "the issued cheque is validly signed");

        // A second band → the cumulative grows (monotonic).
        svc.on_bytes_received(provider, CREDIT_BAND);
        let c2 = push_rx.try_recv().expect("second cheque");
        assert_eq!(c2.cumulative_bytes, 2 * CREDIT_BAND + 9);
    }

    #[tokio::test]
    async fn provider_side_records_inbound_cheques_as_earnings() {
        let (svc, _rx) = service().await;
        let me = svc.identity.node_id().0;
        let consumer = NodeIdentity::generate();
        // A consumer's cheque naming ME as server is recorded → measured as earnings.
        let cheque = ServingCheque::sign(&consumer, me, 5000, 1);
        assert!(svc.book.lock().unwrap().record(cheque));
        assert_eq!(svc.total_earned(), 5000);
    }

    #[tokio::test]
    async fn reciprocity_grants_free_headroom_then_requires_earning() {
        let (svc, _rx) = service().await;
        let gb = 1u64 << 30;
        // A fresh node's cold-start grant covers up to 1 GiB of free fetching.
        assert!(svc.reciprocity_admits(gb));
        assert!(!svc.reciprocity_admits(gb + 1), "over the grant → declined");
        // Consuming the grant exhausts the free headroom.
        svc.on_bytes_received(NodeId([7u8; 32]), gb);
        assert!(
            !svc.reciprocity_admits(1),
            "grant spent → must earn or pay to fetch more"
        );
        // Earning (serving a consumer) reopens headroom by exactly what was served.
        let me = svc.identity.node_id().0;
        let consumer = NodeIdentity::generate();
        svc.book
            .lock()
            .unwrap()
            .record(ServingCheque::sign(&consumer, me, 5000, 1));
        assert!(svc.reciprocity_admits(5000));
        assert!(!svc.reciprocity_admits(5001));
    }
}
