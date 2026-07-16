//! The sequence store — per-account ORDERED-write logs, quorum-serialized (`ECONOMIC_LAYER_DESIGN.md`
//! §4; §11 P3, **model 1: leaderless gossip**). It is the node-side [`SequenceBackend`], the ordering
//! mechanism the token ledger sits on. One [`AccountSequence`] per `(owner, program, account)`; each
//! committed write is a k-of-n [`SequencedCommit`] over `(account, nonce, payload)`, so at most one
//! write ever commits per nonce.
//!
//! **Reuses the program's DECLARED QUORUM:** a program declares one quorum (via
//! [`AttestStore::bootstrap`]) for BOTH authority (`attest`) and ordering (here) — one program, one
//! quorum, resolved by [`AttestStore::current_quorum`]. The sequencer additionally REQUIRES that
//! quorum be intersection-sized (`2k>n`) — the safety precondition against equivocation.
//!
//! **GLOBAL like attestation:** committed sequences publish as durable content + a per-(owner,
//! program, account) DHT head and are pulled cross-node (the same anti-entropy [`AttestStore`] uses),
//! so ANY node reads the ordered log. On adoption a node NEVER accepts a sequence that diverges from
//! its committed prefix (a fork) — it adopts only a strictly-longer one that EXTENDS what it holds.
//!
//! **Signature COLLECTION (P4b-2, leaderless solicit-RPC):** [`sequence`](SequenceBackend::sequence)
//! COLLECTS the quorum's k ordering-signatures — this node signs locally (if a member) and solicits the
//! rest on `tag::SIGN_SOLICIT`; each member auto-signs a valid, non-equivocating proposal
//! ([`serve`](SequenceStore::serve) → `sign_write_locally`, guarded by a PERSISTENT signed-set for
//! cross-restart non-equivocation) — then `submit`s. Any node can be the collector (no leader).
//! [`submit`](SequenceStore::submit) remains the path for a pre-gathered k-of-n commit.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use zeph_com::{
    AccountSequence, MemberSignature, SequenceBackend, SequencedCommit, SequencedWrite,
};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;
use zeph_membership::Membership;
use zeph_obj::{ConsumeMode, ObjEngine};
use zeph_routing::ContentRouting;
use zeph_transport::{tag, TaggedStream, Transport};

// Referenced by this module's intra-doc links and constructed by the tests (via `super::*`); the
// live quorum type is now the `QuorumSource` trait (an `AttestStore` is one of its impls).
#[allow(unused_imports)]
use crate::attest::AttestStore;
use crate::quorum_source::QuorumSource;

/// Min interval between cross-node anti-entropy syncs of the SAME sequence [2026-07-17].
///
/// `sequence_of` used to call [`SequenceStore::sync`] on EVERY read, and `sync` fans a DHT head lookup
/// out to EVERY census peer — so ONE read cost O(census) DHT lookups (each verifying signatures). The
/// economy's read patterns multiplied that badly: `LedgerService::balance` resolves a debit PER `Claim`
/// op while folding, so a chain with `c` claims cost O(c × census) lookups per balance read, and the
/// dashboard reads balance on every snapshot. Profiling an IDLE 5-node fleet found ~half a core burned
/// in Ed25519 verification down this path.
///
/// The log is APPEND-ONLY and every caller re-runs on its own timer, so a read at most this stale is
/// harmless — a commit missed now is picked up on the next window. Kept SHORT deliberately: a `Claim`
/// needs its debit visible, and this bounds how long a just-committed transfer can stay unseen (that
/// race already exists via network delay; this must not widen it materially).
const SYNC_DEBOUNCE: Duration = Duration::from_secs(2);

/// How long a collector waits for one member's sign-solicitation reply before moving on.
const SOLICIT_TIMEOUT: Duration = Duration::from_secs(5);
/// Max solicitation request / reply frame (a `SequencedWrite` / a `MemberSignature`).
const MAX_SOLICIT_FRAME: usize = 64 * 1024;

/// A sign-solicitation: a collector asks a quorum member to add its ORDERING signature to this
/// pre-authored write. The member resolves `(owner, program_cid)`'s quorum, checks it is a member and
/// the write is owner-authentic + non-equivocating, and returns its [`MemberSignature`] (or refuses).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SignSolicit {
    owner: [u8; 32],
    program_cid: [u8; 32],
    write: SequencedWrite,
}

/// Persistent non-equivocation key: `(owner, program, account, nonce)`.
type SignedKey = ([u8; 32], [u8; 32], [u8; 32], u64);

/// Reserved DHT app-name for a committed account-sequence head, keyed by owner + program + account so
/// each account's log resolves independently. The leading control char keeps it out of app-name space.
fn sequence_head_name(owner: &[u8; 32], program_cid: &[u8; 32], account: &[u8; 32]) -> String {
    format!(
        "\u{1}sequence-{}-{}-{}",
        hex::encode(owner),
        hex::encode(program_cid),
        hex::encode(account)
    )
}

/// Map key: `(owner, program_cid, account)`.
type SeqKey = ([u8; 32], [u8; 32], [u8; 32]);

/// Per-(owner, program, account) committed ordered-write logs, published + pulled cross-node.
pub struct SequenceStore {
    identity: Arc<NodeIdentity>,
    sequences: RwLock<HashMap<SeqKey, AccountSequence>>,
    dir: PathBuf,
    obj: Arc<ObjEngine>,
    routing: Arc<dyn ContentRouting>,
    membership: RwLock<Option<Arc<Membership>>>,
    /// The shared quorum SOURCE (`quorum_for`): a user program's declared attestation quorum, or an
    /// anchored program's computed epoch committee — routed by `AnchorAwareQuorumSource` (§10.5).
    quorums: Arc<dyn QuorumSource>,
    /// For soliciting ORDERING signatures from remote quorum members (the collector path).
    transport: Arc<Transport>,
    /// This member's PERSISTENT signed-set — `(owner, program, account, nonce) → hash(write)` it has
    /// signed — so it never equivocates (signs two different writes at one nonce), even across restart.
    signed: RwLock<HashMap<SignedKey, [u8; 32]>>,
    /// Last cross-node anti-entropy sync per sequence — the READ path debounce (see [`SYNC_DEBOUNCE`]).
    last_sync: RwLock<HashMap<SeqKey, Instant>>,
}

impl SequenceStore {
    /// Open the store, loading any persisted `<data_dir>/sequence/<owner>_<program>_<account>.seq`.
    pub fn open(
        identity: Arc<NodeIdentity>,
        data_dir: &Path,
        obj: Arc<ObjEngine>,
        routing: Arc<dyn ContentRouting>,
        quorums: Arc<dyn QuorumSource>,
        transport: Arc<Transport>,
    ) -> Arc<Self> {
        let dir = data_dir.join("sequence");
        let _ = std::fs::create_dir_all(&dir);
        let mut sequences = HashMap::new();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for e in entries.flatten() {
                let path = e.path();
                if path.extension().and_then(|x| x.to_str()) != Some("seq") {
                    continue;
                }
                let Some(key) = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(parse_key)
                else {
                    continue;
                };
                if let Some(seq) = std::fs::read(&path)
                    .ok()
                    .and_then(|b| AccountSequence::decode(&b))
                    .filter(|s| s.account == key.2)
                {
                    sequences.insert(key, seq);
                }
            }
        }
        // The persistent signed-set (cross-restart non-equivocation) — a postcard map, empty if absent.
        let signed = std::fs::read(dir.join("signed.set"))
            .ok()
            .and_then(|b| postcard::from_bytes(&b).ok())
            .unwrap_or_default();
        Arc::new(Self {
            identity,
            sequences: RwLock::new(sequences),
            dir,
            obj,
            routing,
            membership: RwLock::new(None),
            quorums,
            transport,
            signed: RwLock::new(signed),
            last_sync: RwLock::new(HashMap::new()),
        })
    }

    /// Inject the membership handle whose `census()` supplies pull targets.
    pub async fn set_membership(&self, m: Arc<Membership>) {
        *self.membership.write().await = Some(m);
    }

    fn me(&self) -> [u8; 32] {
        self.identity.node_id().0
    }

    fn seq_path(&self, k: &SeqKey) -> PathBuf {
        self.dir.join(format!(
            "{}_{}_{}.seq",
            hex::encode(k.0),
            hex::encode(k.1),
            hex::encode(k.2)
        ))
    }

    fn persist(&self, k: &SeqKey, seq: &AccountSequence) {
        let _ = std::fs::write(self.seq_path(k), seq.encode());
    }

    /// Append a pre-collected k-of-n commit to `(owner, program, commit.write.account)`'s sequence —
    /// the write path. Accepts iff the program's quorum is bootstrapped + intersection-sized, the
    /// commit targets the next nonce, and the k-of-n signatures verify (all enforced by
    /// [`AccountSequence::append`]). Persists + publishes. Returns whether it committed.
    pub async fn submit(
        &self,
        owner: [u8; 32],
        program_cid: [u8; 32],
        commit: SequencedCommit,
    ) -> bool {
        let Some(quorum) = self.quorums.quorum_for(&owner, &program_cid).await else {
            return false; // no declared quorum for this program
        };
        let account = commit.write.account;
        let key = (owner, program_cid, account);
        // See the latest committed order before appending, so the nonce check is against the live
        // length — a write that lost the race for its nonce is then cleanly refused.
        self.sync(&key).await;
        {
            let mut seqs = self.sequences.write().await;
            let mut seq = seqs
                .get(&key)
                .cloned()
                .unwrap_or_else(|| AccountSequence::new(account));
            if !seq.append(commit, &quorum) {
                return false; // wrong nonce / not intersection-sized / sub-threshold — refused
            }
            self.persist(&key, &seq);
            seqs.insert(key, seq);
        }
        self.publish(&key).await;
        true
    }

    /// The ordered log for `(owner, program, account)` — syncing from peers first, so any node reads
    /// the same committed order. The sync is DEBOUNCED ([`SYNC_DEBOUNCE`]); the commit path calls
    /// [`sync`](Self::sync) directly because its nonce check must see the live length.
    pub async fn sequence_of(
        &self,
        owner: [u8; 32],
        program_cid: [u8; 32],
        account: [u8; 32],
    ) -> Option<AccountSequence> {
        let key = (owner, program_cid, account);
        self.sync_debounced(&key).await;
        self.sequences.read().await.get(&key).cloned()
    }

    /// Anti-entropy for the READ path: at most one [`sync`](Self::sync) per sequence per
    /// [`SYNC_DEBOUNCE`]. `sync` fans a DHT head lookup to EVERY census peer, so an undebounced read
    /// costs O(census) lookups — and the economy's readers call this per participant, per census node,
    /// and per `Claim` op while folding a balance, which multiplied it into the idle-CPU burn this
    /// debounce removes. Check-and-set under one write lock so concurrent readers can't stampede.
    async fn sync_debounced(&self, key: &SeqKey) {
        {
            let mut last = self.last_sync.write().await;
            match last.get(key) {
                Some(t) if t.elapsed() < SYNC_DEBOUNCE => return, // synced recently → read local
                _ => last.insert(*key, Instant::now()),
            };
        }
        self.sync(key).await;
    }

    /// Publish an account's committed sequence: durable content + a per-(owner,program,account) DHT
    /// head (version = length + 1, strictly increasing). Mirrors [`AttestStore`]'s publish.
    async fn publish(&self, key: &SeqKey) {
        let Some((bytes, len)) = self
            .sequences
            .read()
            .await
            .get(key)
            .map(|s| (s.encode(), s.next_nonce()))
        else {
            return;
        };
        if let Ok(cid) = self.obj.publish_system(&bytes).await {
            let _ = self
                .routing
                .announce_app(&sequence_head_name(&key.0, &key.1, &key.2), cid, len + 1)
                .await;
        }
    }

    async fn fetch_if_newer(
        &self,
        key: &SeqKey,
        from: [u8; 32],
        local_len: u64,
    ) -> Option<AccountSequence> {
        let rec = self
            .routing
            .resolve_app(NodeId(from), &sequence_head_name(&key.0, &key.1, &key.2))
            .await
            .ok()??;
        if rec.version <= local_len + 1 {
            return None;
        }
        let bytes = self
            .obj
            .get_following_manifest(rec.wasm_cid, ConsumeMode::Drop)
            .await
            .ok()?;
        AccountSequence::decode(&bytes)
    }

    /// Pull an account's sequence from census peers and adopt the longest one that (a) verifies under
    /// the program's quorum and (b) does NOT diverge from our committed prefix. A fork is impossible
    /// under an intersection-sized quorum with honest non-equivocation, but we still refuse a divergent
    /// sequence (safety over liveness). No-op when membership is unset (tests → local-only).
    async fn sync(&self, key: &SeqKey) {
        let Some(quorum) = self.quorums.quorum_for(&key.0, &key.1).await else {
            return;
        };
        if !quorum.is_intersection_sized() {
            return; // a non-intersection-sized quorum can equivocate; never sequence under it
        }
        let targets: Vec<[u8; 32]> = {
            let guard = self.membership.read().await;
            let Some(m) = guard.as_ref() else {
                return;
            };
            m.census()
                .await
                .into_iter()
                .map(|(n, _)| n.0)
                .filter(|id| *id != self.me())
                .collect()
        };
        for peer in targets {
            let local_len = self
                .sequences
                .read()
                .await
                .get(key)
                .map_or(0, |s| s.next_nonce());
            let Some(fetched) = self.fetch_if_newer(key, peer, local_len).await else {
                continue;
            };
            if fetched.account != key.2 || !fetched.verify(&quorum) {
                continue; // must be for this account and fully quorum-authorized
            }
            let mut seqs = self.sequences.write().await;
            let cur = seqs.get(key);
            let cur_len = cur.map_or(0, |s| s.next_nonce());
            // Non-equivocation: the fetched sequence must EXTEND ours — identical commits on the
            // shared prefix. A divergence at any committed nonce is a fork; refuse it.
            if cur.is_some_and(|c| !extends(c, &fetched)) {
                continue;
            }
            if fetched.next_nonce() > cur_len {
                self.persist(key, &fetched);
                seqs.insert(*key, fetched);
            }
        }
    }

    /// The MEMBER's auto-sign: add THIS node's ORDERING signature to `write` iff the node is a member of
    /// `(owner, program_cid)`'s intersection-sized quorum, the write is owner-authentic, and signing does
    /// not equivocate (the persistent [`signed`](Self::signed) set holds no DIFFERENT write at this
    /// `(account, nonce)`; idempotent for the identical write). This is the structural non-equivocation
    /// invariant, made crash-safe by persistence — no signing policy can override it.
    async fn sign_write_locally(
        &self,
        owner: [u8; 32],
        program_cid: [u8; 32],
        write: &SequencedWrite,
    ) -> Option<MemberSignature> {
        let quorum = self.quorums.quorum_for(&owner, &program_cid).await?;
        if !quorum.is_member(&self.me())
            || !quorum.is_intersection_sized()
            || !write.owner_authentic()
        {
            return None;
        }
        let key = (owner, program_cid, write.account, write.nonce);
        let h = Cid::of(&write.signing_bytes()).0;
        let snapshot = {
            let mut signed = self.signed.write().await;
            match signed.get(&key) {
                Some(prev) if *prev != h => return None, // equivocation — hard refusal
                _ => {
                    signed.insert(key, h);
                }
            }
            signed.clone()
        };
        self.persist_signed(&snapshot);
        Some(write.sign(&self.identity))
    }

    /// Solicit ONE remote quorum member's ordering signature over `solicit.write` (a mux request/reply on
    /// `tag::SIGN_SOLICIT`). `None` on unreachable / timeout / refusal.
    async fn solicit_member(
        &self,
        member: [u8; 32],
        solicit: &SignSolicit,
    ) -> Option<MemberSignature> {
        let addr = {
            let m = self.membership.read().await;
            m.as_ref()?.member_addr(NodeId(member)).await?
        };
        let bytes = postcard::to_allocvec(solicit).ok()?;
        let resp = tokio::time::timeout(
            SOLICIT_TIMEOUT,
            self.transport
                .request_tagged(&addr, tag::SIGN_SOLICIT, &bytes, MAX_SOLICIT_FRAME),
        )
        .await
        .ok()?
        .ok()?;
        match resp.split_first() {
            Some((0x01, sig)) => postcard::from_bytes::<MemberSignature>(sig).ok(),
            _ => None, // 0x00 refusal / empty
        }
    }

    /// Gather `quorum.threshold` ORDERING signatures for `write` (self signs locally; remote members are
    /// solicited on `tag::SIGN_SOLICIT`), then `submit` the assembled commit. Leaderless — any node can
    /// be the collector. Returns whether it committed (false if the threshold couldn't be reached).
    async fn collect(
        &self,
        owner: [u8; 32],
        program_cid: [u8; 32],
        write: SequencedWrite,
        quorum: &zeph_com::Quorum,
    ) -> bool {
        let solicit = SignSolicit {
            owner,
            program_cid,
            write: write.clone(),
        };
        let mut sigs: Vec<MemberSignature> = Vec::new();
        for member in &quorum.members {
            if sigs.len() >= quorum.threshold {
                break;
            }
            let sig = if *member == self.me() {
                self.sign_write_locally(owner, program_cid, &write).await
            } else {
                self.solicit_member(*member, &solicit).await
            };
            if let Some(s) = sig {
                if !sigs.iter().any(|x| x.member == s.member) {
                    sigs.push(s);
                }
            }
        }
        if sigs.len() < quorum.threshold {
            return false; // members offline / refused → couldn't reach threshold
        }
        self.submit(
            owner,
            program_cid,
            SequencedCommit {
                write,
                signatures: sigs,
            },
        )
        .await
    }

    /// Serve inbound sign-solicitations (`tag::SIGN_SOLICIT`): auto-sign each valid, non-equivocating
    /// proposal (via [`sign_write_locally`](Self::sign_write_locally)) and reply `0x01 + sig`, else `0x00`
    /// to refuse. The soliciting caller is the QUIC-authenticated `remote` (no extra auth needed — the
    /// member signs based on the quorum + owner-authenticity, not on who asked).
    pub async fn serve(self: Arc<Self>, mut streams: mpsc::Receiver<TaggedStream>) {
        while let Some(TaggedStream {
            mut send, mut recv, ..
        }) = streams.recv().await
        {
            let me = self.clone();
            tokio::spawn(async move {
                let Ok(bytes) = recv.read_to_end(MAX_SOLICIT_FRAME).await else {
                    return;
                };
                let mut resp = vec![0x00u8]; // default: refuse
                if let Ok(s) = postcard::from_bytes::<SignSolicit>(&bytes) {
                    if let Some(sig) = me
                        .sign_write_locally(s.owner, s.program_cid, &s.write)
                        .await
                    {
                        if let Ok(sb) = postcard::to_allocvec(&sig) {
                            resp = Vec::with_capacity(1 + sb.len());
                            resp.push(0x01);
                            resp.extend_from_slice(&sb);
                        }
                    }
                }
                let _ = send.write_all(&resp).await;
                let _ = send.finish();
            });
        }
    }

    fn persist_signed(&self, signed: &HashMap<SignedKey, [u8; 32]>) {
        if let Ok(bytes) = postcard::to_allocvec(signed) {
            let _ = std::fs::write(self.dir.join("signed.set"), bytes);
        }
    }
}

/// Whether `longer` extends `base` — identical commits on `base`'s prefix (no fork).
fn extends(base: &AccountSequence, longer: &AccountSequence) -> bool {
    longer.commits.len() >= base.commits.len()
        && base
            .commits
            .iter()
            .zip(&longer.commits)
            .all(|(a, b)| a == b)
}

fn parse_key(stem: &str) -> Option<SeqKey> {
    let mut it = stem.split('_');
    let o = parse32(it.next()?)?;
    let p = parse32(it.next()?)?;
    let a = parse32(it.next()?)?;
    Some((o, p, a))
}
fn parse32(hex: &str) -> Option<[u8; 32]> {
    hex::decode(hex)
        .ok()
        .and_then(|b| <[u8; 32]>::try_from(b.as_slice()).ok())
}

#[async_trait::async_trait]
impl SequenceBackend for SequenceStore {
    /// The `sequence` host fn's write path: order a PRE-AUTHORED `write` under `owner`'s quorum by
    /// COLLECTING the quorum's k ordering-signatures — this node signs locally (if a member) and solicits
    /// the rest on `tag::SIGN_SOLICIT` — then submits (P4b-2, leaderless: any node collects).
    /// Owner-authenticity + intersection sizing are checked here; per-member non-equivocation is enforced
    /// in `sign_write_locally`. Returns false if the threshold can't be reached (members offline/refused).
    async fn sequence(
        &self,
        owner: [u8; 32],
        program_cid: [u8; 32],
        write: SequencedWrite,
    ) -> bool {
        let Some(quorum) = self.quorums.quorum_for(&owner, &program_cid).await else {
            return false;
        };
        if !quorum.is_intersection_sized() || !write.owner_authentic() {
            return false;
        }
        self.collect(owner, program_cid, write, &quorum).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeph_core::Cid;
    use zeph_obj::{ObjConfig, PeerSource};
    use zeph_routing::{MetaRecord, ProviderRecord};
    use zeph_store::Store;
    use zeph_transport::{PeerAddr, Reach, Transport};

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

    /// A node with a shared attest+sequence store pair (membership unset → local-only, like the
    /// attest tests). Returns the node identity so tests can make it a quorum member.
    async fn open_stores() -> (
        Arc<NodeIdentity>,
        Arc<AttestStore>,
        Arc<SequenceStore>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let node = Arc::new(NodeIdentity::generate());
        let transport = Arc::new(
            Transport::bind(
                node.secret_key_bytes(),
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
        let attest = AttestStore::open(
            node.clone(),
            dir.path(),
            engine.clone(),
            Arc::new(NullRouting),
        );
        let seq = SequenceStore::open(
            node.clone(),
            dir.path(),
            engine,
            Arc::new(NullRouting),
            attest.clone(),
            transport,
        );
        (node, attest, seq, dir)
    }

    /// Author an owner-authentic write as `owner` (account = `owner`'s pubkey).
    fn write(owner: &NodeIdentity, nonce: u64, payload: &[u8]) -> SequencedWrite {
        SequencedWrite::author(owner, nonce, payload.to_vec())
    }
    fn commit(w: &SequencedWrite, signers: &[&NodeIdentity]) -> SequencedCommit {
        SequencedCommit {
            write: w.clone(),
            signatures: signers.iter().map(|s| w.sign(s)).collect(),
        }
    }

    #[tokio::test]
    async fn self_quorum_sequences_a_write_and_reads_it_back() {
        let (node, attest, seq, _dir) = open_stores().await;
        let owner = node.node_id().0;
        let program = [9u8; 32];
        let account = owner; // sequence() authors for the node's OWN account
                             // A 1-of-1 quorum whose sole member is THIS node → sequence() auto-commits.
        attest.bootstrap(program, vec![owner], 1).await;

        assert!(
            seq.sequence(
                owner,
                program,
                SequencedWrite::author(&node, 0, b"pay alice".to_vec())
            )
            .await,
            "self-quorum commits the write at nonce 0"
        );
        // Wrong next nonce is refused (nonce 0 is filled; next is 1).
        assert!(
            !seq.sequence(
                owner,
                program,
                SequencedWrite::author(&node, 0, b"pay bob".to_vec())
            )
            .await,
            "a second write at nonce 0 is refused"
        );
        assert!(
            seq.sequence(
                owner,
                program,
                SequencedWrite::author(&node, 1, b"pay bob".to_vec())
            )
            .await,
            "nonce 1 commits"
        );

        let log = seq.sequence_of(owner, program, account).await.unwrap();
        assert_eq!(log.payload_at(0), Some(&b"pay alice"[..]));
        assert_eq!(log.payload_at(1), Some(&b"pay bob"[..]));
        assert_eq!(log.next_nonce(), 2);
    }

    #[tokio::test]
    async fn a_pre_authored_write_for_another_account_is_ordered() {
        // The ABI unblocks the general case: a write authored by SOMEONE ELSE (their owner_sig), ordered
        // by this node as the sole quorum member — account != the node.
        let (node, attest, seq, _dir) = open_stores().await;
        let owner = node.node_id().0;
        let program = [11u8; 32];
        let alice = NodeIdentity::generate();
        let alice_acct = alice.node_id().0;
        // A 1-of-1 quorum whose member is THIS node (the orderer), NOT alice.
        attest.bootstrap(program, vec![owner], 1).await;

        let w = SequencedWrite::author(&alice, 0, b"alice's write".to_vec());
        assert!(
            seq.sequence(owner, program, w).await,
            "the node orders alice's pre-authored write"
        );
        let log = seq.sequence_of(owner, program, alice_acct).await.unwrap();
        assert_eq!(log.payload_at(0), Some(&b"alice's write"[..]));

        // A write claiming alice's account but signed by an IMPOSTOR is refused (not owner-authentic).
        let impostor = NodeIdentity::generate();
        let mut spoof = SequencedWrite::author(&impostor, 1, b"forged".to_vec());
        spoof.account = alice_acct;
        assert!(
            !seq.sequence(owner, program, spoof).await,
            "a spoofed write (wrong signer) is refused"
        );
    }

    #[tokio::test]
    async fn member_auto_signs_and_refuses_equivocation_across_restart() {
        // The member auto-sign (P4b-2): a quorum member signs a valid proposal, is idempotent for the
        // identical write, and REFUSES a conflicting write at the same (account, nonce) — even after a
        // restart, via the persistent signed-set. This is the structural non-equivocation invariant.
        let dir = tempfile::tempdir().unwrap();
        let node = Arc::new(NodeIdentity::generate());
        let owner = node.node_id().0;
        let program = [12u8; 32];
        let alice = NodeIdentity::generate();
        let a = SequencedWrite::author(&alice, 0, b"A".to_vec());
        let b = SequencedWrite::author(&alice, 0, b"B".to_vec()); // same (account, nonce), different

        {
            let (attest, seq) = build_stores(node.clone(), dir.path()).await;
            let (m2, m3) = (NodeIdentity::generate(), NodeIdentity::generate());
            // A 2-of-3 quorum this node is a member of (intersection-sized).
            attest
                .bootstrap(program, vec![owner, m2.node_id().0, m3.node_id().0], 2)
                .await;
            assert!(
                seq.sign_write_locally(owner, program, &a).await.is_some(),
                "member signs A"
            );
            assert!(
                seq.sign_write_locally(owner, program, &a).await.is_some(),
                "idempotent re-sign of A"
            );
            assert!(
                seq.sign_write_locally(owner, program, &b).await.is_none(),
                "refuses conflicting B at the same nonce"
            );
        }
        // Reopen — the quorum reloads from disk AND the persistent signed-set still refuses B.
        let (_attest, seq) = build_stores(node, dir.path()).await;
        assert!(
            seq.sign_write_locally(owner, program, &b).await.is_none(),
            "still refuses B after a restart (persistent non-equivocation)"
        );
    }

    #[tokio::test]
    async fn submit_k_of_n_orders_and_refuses_forks_and_gaps() {
        let (node, attest, seq, _dir) = open_stores().await;
        let owner = node.node_id().0;
        let program = [2u8; 32];
        let acct = NodeIdentity::generate();
        let account = acct.node_id().0;
        let (a, b, c) = (
            NodeIdentity::generate(),
            NodeIdentity::generate(),
            NodeIdentity::generate(),
        );
        // 2-of-3 is intersection-sized (2k=4 > 3).
        attest
            .bootstrap(
                program,
                vec![a.node_id().0, b.node_id().0, c.node_id().0],
                2,
            )
            .await;

        // nonce 0 commits with 2 sigs.
        assert!(
            seq.submit(owner, program, commit(&write(&acct, 0, b"n0"), &[&a, &b]))
                .await
        );
        // a conflicting write at nonce 0 (a fork) is refused — the slot is filled.
        assert!(
            !seq.submit(owner, program, commit(&write(&acct, 0, b"fork"), &[&b, &c]))
                .await,
            "fork at a committed nonce refused"
        );
        // skipping to nonce 2 is refused (must be sequential).
        assert!(
            !seq.submit(owner, program, commit(&write(&acct, 2, b"gap"), &[&a, &b]))
                .await,
            "non-sequential nonce refused"
        );
        // a sub-threshold (1 sig) commit is refused.
        assert!(
            !seq.submit(owner, program, commit(&write(&acct, 1, b"n1"), &[&a]))
                .await,
            "1-of-3 sub-threshold refused"
        );
        // nonce 1 commits with 2 sigs.
        assert!(
            seq.submit(owner, program, commit(&write(&acct, 1, b"n1"), &[&b, &c]))
                .await
        );

        let log = seq.sequence_of(owner, program, account).await.unwrap();
        assert_eq!(log.payload_at(0), Some(&b"n0"[..]));
        assert_eq!(log.payload_at(1), Some(&b"n1"[..]));
    }

    #[tokio::test]
    async fn a_non_intersection_sized_quorum_cannot_sequence() {
        let (node, attest, seq, _dir) = open_stores().await;
        let owner = node.node_id().0;
        let program = [3u8; 32];
        let acct = NodeIdentity::generate();
        let (a, b, c, d) = (
            NodeIdentity::generate(),
            NodeIdentity::generate(),
            NodeIdentity::generate(),
            NodeIdentity::generate(),
        );
        // 2-of-4 is NOT intersection-sized (2k=4 == 4) → the sequencer refuses to order under it.
        attest
            .bootstrap(
                program,
                vec![a.node_id().0, b.node_id().0, c.node_id().0, d.node_id().0],
                2,
            )
            .await;
        assert!(
            !seq.submit(owner, program, commit(&write(&acct, 0, b"x"), &[&a, &b]))
                .await,
            "a valid 2-of-4 signature set is still refused — the quorum can equivocate"
        );
    }

    #[tokio::test]
    async fn persisted_sequence_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let node = Arc::new(NodeIdentity::generate());
        let owner = node.node_id().0;
        let program = [4u8; 32];
        let account = owner; // sequence() authors for the node's OWN account

        {
            let (attest, seq) = build_stores(node.clone(), dir.path()).await;
            attest.bootstrap(program, vec![owner], 1).await;
            assert!(
                seq.sequence(
                    owner,
                    program,
                    SequencedWrite::author(&node, 0, b"durable".to_vec())
                )
                .await
            );
        }
        // Reopen from disk — the committed sequence reloads.
        let (_attest, seq) = build_stores(node, dir.path()).await;
        let log = seq.sequence_of(owner, program, account).await.unwrap();
        assert_eq!(log.payload_at(0), Some(&b"durable"[..]));
    }

    async fn build_stores(
        node: Arc<NodeIdentity>,
        dir: &Path,
    ) -> (Arc<AttestStore>, Arc<SequenceStore>) {
        let transport = Arc::new(
            Transport::bind(
                node.secret_key_bytes(),
                Reach::LocalOnly,
                vec![zeph_transport::MUX_ALPN.to_vec()],
                0,
            )
            .await
            .unwrap(),
        );
        let store = Arc::new(Store::open(dir).unwrap());
        let engine = ObjEngine::with_peer_source(
            transport.clone(),
            store,
            Arc::new(NullRouting),
            Arc::new(NullPeers),
            ObjConfig::default(),
        );
        let attest = AttestStore::open(node.clone(), dir, engine.clone(), Arc::new(NullRouting));
        let seq = SequenceStore::open(
            node,
            dir,
            engine,
            Arc::new(NullRouting),
            attest.clone(),
            transport,
        );
        (attest, seq)
    }
}
