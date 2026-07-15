//! ENCRYPTION phase 2 GATE: a private file published over an in-process network
//! is stored as ciphertext only; the OWNER reads it back byte-identical by
//! envelope CID; a DIFFERENT identity fetches the same objects but cannot decrypt.

use std::sync::Arc;

use zeph_cipher::EncKeypair;
use zeph_crypto::NodeIdentity;
use zeph_obj::{ConsumeMode, ObjConfig, ObjEngine};
use zeph_store::Store;
use zeph_testkit::{MemNet, MemRouting};
use zeph_transport::{Reach, Transport};

/// Replaces the in-process tracker: one shared in-memory network view.
fn start_tracker() -> MemNet {
    MemNet::new()
}

async fn node(tracker: &MemNet, dir: &std::path::Path) -> (Arc<ObjEngine>, Arc<MemRouting>) {
    node_cfg(tracker, dir, ObjConfig::default()).await
}

async fn node_cfg(
    tracker: &MemNet,
    dir: &std::path::Path,
    cfg: ObjConfig,
) -> (Arc<ObjEngine>, Arc<MemRouting>) {
    let id = Arc::new(NodeIdentity::generate());
    let t = Arc::new(
        Transport::bind(
            id.secret_key_bytes(),
            Reach::LocalOnly,
            vec![zeph_transport::MUX_ALPN.to_vec()],
            0,
        )
        .await
        .unwrap(),
    );
    let store = Arc::new(Store::open(dir).unwrap());
    let routing = tracker.routing(id, t.addr());
    let engine = ObjEngine::with_peer_source(
        t.clone(),
        store,
        routing.clone(),
        Arc::new(tracker.peers()),
        cfg,
    );
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let st = t.clone();
    tokio::spawn(async move { st.serve(vec![(zeph_transport::tag::PIECE, tx)]).await });
    let se = engine.clone();
    tokio::spawn(async move { se.serve(rx).await });
    (engine, routing)
}

#[tokio::test]
async fn private_object_hides_content_and_only_owner_reads() {
    let tracker = start_tracker();
    let dirs: Vec<tempfile::TempDir> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();

    // Storage nodes to hold distributed pieces.
    for dir in dirs.iter().take(4) {
        let (_e, r) = node(&tracker, dir.path()).await;
        r.announce_node(0, 0).await.unwrap();
    }

    // Publisher with identity A.
    let (owner, _ro) = node(&tracker, dirs[4].path()).await;
    owner.set_enc_keypair(EncKeypair::from_identity_seed(&[1u8; 32]));
    let secret = b"top secret private file contents";
    let pp = owner
        .publish_private("secret.txt", "text/plain", secret, true)
        .await
        .unwrap();

    // 1. Owner reads it back byte-identical (name + content).
    let out = owner.get_private(pp.envelope_cid).await.unwrap();
    assert_eq!(out.content, secret);
    assert_eq!(out.name, "secret.txt");
    assert_eq!(out.mime, "text/plain");

    // 2. The network holds only CIPHERTEXT — the plaintext never appears in any stored ciphertext
    //    SEGMENT (chunk-then-encrypt: each segment is its own sealed object).
    let ebytes = owner.get(pp.envelope_cid, ConsumeMode::Drop).await.unwrap();
    let env = zeph_obj::EncryptedEnvelope::decode(&ebytes).unwrap();
    assert!(
        !env.segments.is_empty(),
        "a private file has ≥1 sealed segment"
    );
    for seg in &env.segments {
        let ct = owner
            .get(zeph_core::Cid(seg.cid), ConsumeMode::Drop)
            .await
            .unwrap();
        assert!(
            !ct.windows(secret.len()).any(|w| w == secret),
            "plaintext must not appear in a ciphertext segment"
        );
    }

    // 3. A DIFFERENT identity fetches the same objects but cannot decrypt.
    let (reader, _rr) = node(&tracker, dirs[5].path()).await;
    reader.set_enc_keypair(EncKeypair::from_identity_seed(&[2u8; 32]));
    assert!(
        reader.get_private(pp.envelope_cid).await.is_err(),
        "a different identity must not be able to decrypt"
    );
}

/// Private files are chunk-then-encrypt: a large private file splits into independently-sealed
/// segments → it streams (range reads), dedups identical blocks within the file, hides everything,
/// and only the owner decrypts.
#[tokio::test]
async fn large_private_file_segments_streams_and_dedups() {
    let tracker = start_tracker();
    let dirs: Vec<tempfile::TempDir> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    let cfg = || ObjConfig {
        file_segment_bytes: 40 * 1024,
        file_k: 8,
        ..ObjConfig::default()
    };
    let (owner, _o) = node_cfg(&tracker, dirs[0].path(), cfg()).await;
    owner.set_enc_keypair(EncKeypair::from_identity_seed(&[1u8; 32]));

    // A 40 KB block, then the SAME block again, then a distinct half-block → 3 segments where the
    // first two are identical plaintext (should dedup to one sealed cid under the file's DEK).
    let block: Vec<u8> = (0..40 * 1024u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 11) as u8)
        .collect();
    let mut data = block.clone();
    data.extend_from_slice(&block);
    data.extend_from_slice(&block[..20 * 1024]);

    let pp = owner
        .publish_private("big.bin", "application/octet-stream", &data, true)
        .await
        .unwrap();

    // 3 sealed segments; the two identical plaintext blocks dedup to ONE ciphertext cid (block-level
    // dedup WITHIN the file — deterministic per-segment seal under one DEK).
    let env = zeph_obj::EncryptedEnvelope::decode(
        &owner.get(pp.envelope_cid, ConsumeMode::Drop).await.unwrap(),
    )
    .unwrap();
    assert_eq!(env.segments.len(), 3, "three sealed segments");
    assert_eq!(
        env.segments.iter().map(|s| s.len).sum::<u64>(),
        data.len() as u64,
        "segment plaintext lengths sum to the file size"
    );
    assert_eq!(
        env.segments[0].cid, env.segments[1].cid,
        "identical plaintext segments dedup to one sealed cid (within-file block dedup)"
    );
    assert_ne!(
        env.segments[0].cid, env.segments[2].cid,
        "the distinct tail differs"
    );

    // Owner reads the whole file back byte-identical (name + content).
    let out = owner.get_private(pp.envelope_cid).await.unwrap();
    assert_eq!(out.content, data, "whole private file round-trips");
    assert_eq!(out.name, "big.bin");

    // RANGE read: a window spanning the seg0→seg1 boundary decrypts correctly (streaming/seek over
    // sealed segments) — fetching + decrypting only the covering segments.
    let off = 40 * 1024 - 100;
    let got = owner
        .get_private_range(pp.envelope_cid, off as u64, 500)
        .await
        .unwrap();
    assert_eq!(
        got,
        data[off..off + 500],
        "private range read == the plaintext slice"
    );

    // A DIFFERENT identity fetches the same objects but cannot decrypt.
    let (reader, _r) = node_cfg(&tracker, dirs[1].path(), cfg()).await;
    reader.set_enc_keypair(EncKeypair::from_identity_seed(&[2u8; 32]));
    assert!(
        reader.get_private(pp.envelope_cid).await.is_err(),
        "a different identity must not be able to decrypt"
    );
}
