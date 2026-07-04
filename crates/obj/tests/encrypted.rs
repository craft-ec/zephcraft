//! ENCRYPTION phase 2 GATE: a private file published over an in-process network
//! is stored as ciphertext only; the OWNER reads it back byte-identical by
//! envelope CID; a DIFFERENT identity fetches the same objects but cannot decrypt.

use std::sync::Arc;

use zeph_cipher::EncKeypair;
use zeph_crypto::NodeIdentity;
use zeph_obj::{ConsumeMode, ObjConfig, ObjEngine};
use zeph_routing::{Registry, RegistryConfig, TrackerRouting};
use zeph_store::Store;
use zeph_transport::{Reach, Transport};

async fn transport(alpns: Vec<Vec<u8>>) -> Arc<Transport> {
    let id = NodeIdentity::generate();
    Arc::new(
        Transport::bind(id.secret_key_bytes(), Reach::LocalOnly, alpns, 0)
            .await
            .unwrap(),
    )
}

async fn start_tracker() -> Arc<Transport> {
    let t = transport(vec![zeph_routing::ALPN.to_vec()]).await;
    let registry = Arc::new(Registry::new(RegistryConfig::default()));
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let st = t.clone();
    tokio::spawn(async move { st.serve(vec![(zeph_routing::ALPN.to_vec(), tx)]).await });
    let rt = t.clone();
    tokio::spawn(async move { zeph_routing::serve(registry, rt, rx).await });
    t
}

async fn node(tracker: &Transport, dir: &std::path::Path) -> (Arc<ObjEngine>, Arc<TrackerRouting>) {
    let id = Arc::new(NodeIdentity::generate());
    let t = Arc::new(
        Transport::bind(
            id.secret_key_bytes(),
            Reach::LocalOnly,
            vec![zeph_obj::ALPN.to_vec()],
            0,
        )
        .await
        .unwrap(),
    );
    let store = Arc::new(Store::open(dir).unwrap());
    let routing = Arc::new(TrackerRouting::new(
        t.clone(),
        id,
        vec![tracker.addr()],
        "test".into(),
    ));
    let engine = ObjEngine::new(t.clone(), store, routing.clone(), ObjConfig::default());
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let st = t.clone();
    tokio::spawn(async move { st.serve(vec![(zeph_obj::ALPN.to_vec(), tx)]).await });
    let se = engine.clone();
    tokio::spawn(async move { se.serve(rx).await });
    (engine, routing)
}

#[tokio::test]
async fn private_object_hides_content_and_only_owner_reads() {
    let tracker = start_tracker().await;
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

    // 2. The network holds only CIPHERTEXT — the plaintext never appears in the
    //    stored ciphertext object.
    let ct = owner
        .get(pp.ciphertext_cid, ConsumeMode::Drop)
        .await
        .unwrap();
    assert!(
        !ct.windows(secret.len()).any(|w| w == secret),
        "plaintext must not appear in the ciphertext object"
    );

    // 3. A DIFFERENT identity fetches the same objects but cannot decrypt.
    let (reader, _rr) = node(&tracker, dirs[5].path()).await;
    reader.set_enc_keypair(EncKeypair::from_identity_seed(&[2u8; 32]));
    assert!(
        reader.get_private(pp.envelope_cid).await.is_err(),
        "a different identity must not be able to decrypt"
    );
}
