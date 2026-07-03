//! M2.1 GATE: a tracker holds all three registries; a client announces a
//! provider record and another client resolves it; node + relay registries
//! work; signatures are enforced (forged records rejected).

use std::sync::Arc;

use zeph_core::Cid;
use zeph_crypto::NodeIdentity;
use zeph_routing::{ContentRouting, Registry, RegistryConfig, TrackerRouting};
use zeph_transport::{Reach, Transport};

async fn node(alpns: Vec<Vec<u8>>) -> (Arc<Transport>, Arc<NodeIdentity>) {
    let id = Arc::new(NodeIdentity::generate());
    let t = Arc::new(
        Transport::bind(id.secret_key_bytes(), Reach::LocalOnly, alpns, 0)
            .await
            .unwrap(),
    );
    (t, id)
}

async fn start_tracker() -> Arc<Transport> {
    let (transport, _) = node(vec![zeph_routing::ALPN.to_vec()]).await;
    let registry = Arc::new(Registry::new(RegistryConfig::default()));
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let serve_t = transport.clone();
    tokio::spawn(async move { serve_t.serve(vec![(zeph_routing::ALPN.to_vec(), tx)]).await });
    let reg_t = transport.clone();
    tokio::spawn(async move { zeph_routing::serve(registry, reg_t, rx).await });
    transport
}

fn routing(t: Arc<Transport>, id: Arc<NodeIdentity>, tracker: &Transport) -> TrackerRouting {
    TrackerRouting::new(t, id, vec![tracker.addr()], "test".into())
}

#[tokio::test]
async fn tracker_three_registries_and_signature_enforcement() {
    let tracker = start_tracker().await;

    // Provider announces it holds a CID; a separate client resolves it.
    let (pt, pid) = node(vec![]).await;
    let provider_id = pid.node_id();
    let provider = routing(pt, pid, &tracker);
    let cid = Cid::of(b"hello content routing");
    provider.announce(cid, 3, true).await.unwrap();
    provider.announce_node(0, 0).await.unwrap();
    provider
        .announce_relay("https://relay1.example".into())
        .await
        .unwrap();

    let (ct, cid2) = node(vec![]).await;
    let client = routing(ct, cid2, &tracker);

    let found = client.resolve(cid).await.unwrap();
    assert_eq!(found.len(), 1, "provider resolved");
    assert_eq!(found[0].node_id, provider_id);
    assert_eq!(found[0].piece_count, 3);
    assert!(found[0].pinned);

    // A different CID resolves to nothing.
    assert!(client.resolve(Cid::of(b"other")).await.unwrap().is_empty());

    // Node + relay registries.
    let nodes = client.nodes().await.unwrap();
    assert!(
        nodes.iter().any(|(id, _)| *id == provider_id),
        "node registry"
    );
    let relays = client.relays().await.unwrap();
    assert_eq!(relays.len(), 1);
    assert_eq!(relays[0].relay_url, "https://relay1.example");
}

#[tokio::test]
async fn tracker_rejects_forged_records() {
    // A record whose signature doesn't match its node_id must be refused by
    // the registry (verify() at ingest) and never surface.
    let registry = Registry::new(RegistryConfig::default());
    let real = NodeIdentity::generate();
    let payload = zeph_routing::NodePayload {
        addr: "x".into(),
        version: "v".into(),
        used_bytes: 0,
        capacity_bytes: 0,
    };
    let mut rec = zeph_routing::records::sign(&real, zeph_routing::records::KIND_NODE, &payload, 1);
    // Tamper: flip the claimed identity, keep the (now-wrong) signature.
    rec.node_id = NodeIdentity::generate().node_id().0;
    assert_eq!(registry.announce(rec), Err("bad-signature"));
    assert!(registry.nodes(10).is_empty());
}
