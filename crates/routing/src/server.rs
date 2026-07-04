//! Tracker server: serves the tracker ALPN, applying announces/queries to a
//! `Registry`. Also the shared request helper used by the client.

use std::sync::Arc;
use std::time::Duration;

use zeph_transport::{Connection, PeerAddr, Transport};
use zeph_wire as wire;

use crate::registry::Registry;

const MAX_FRAME: usize = 4 * 1024 * 1024;

/// One request frame → one reply frame over a bi-stream. Used by clients to
/// talk to trackers (announce/resolve/withdraw).
pub async fn request(
    transport: &Transport,
    tracker: &PeerAddr,
    msg: &wire::Message,
) -> anyhow::Result<wire::Message> {
    let conn = transport.connect(tracker, crate::ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(&wire::encode(msg, transport.clock().now().0))
        .await?;
    send.finish()?;
    let bytes =
        tokio::time::timeout(Duration::from_secs(15), recv.read_to_end(MAX_FRAME)).await??;
    let frame = wire::decode(&bytes)?;
    conn.close(0u32.into(), b"done");
    Ok(frame.message)
}

/// Serve tracker connections handed over by the transport's ALPN dispatcher.
pub async fn serve(
    registry: Arc<Registry>,
    transport: Arc<Transport>,
    mut conns: tokio::sync::mpsc::Receiver<Connection>,
) {
    while let Some(conn) = conns.recv().await {
        let registry = registry.clone();
        let clock = transport.clock();
        tokio::spawn(async move {
            while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                let Ok(bytes) = recv.read_to_end(MAX_FRAME).await else {
                    return;
                };
                let Ok(frame) = wire::decode(&bytes) else {
                    return;
                };
                let reply = handle(&registry, frame.message);
                let _ = send.write_all(&wire::encode(&reply, clock.now().0)).await;
                let _ = send.finish();
            }
        });
    }
}

fn handle(registry: &Registry, msg: wire::Message) -> wire::Message {
    match msg {
        wire::Message::TrackerAnnounce(record) => {
            let ack = match registry.announce(record) {
                Ok(()) => wire::TrackerAck {
                    ok: true,
                    reason: String::new(),
                },
                Err(reason) => wire::TrackerAck {
                    ok: false,
                    reason: reason.to_string(),
                },
            };
            wire::Message::TrackerAnnounceAck(ack)
        }
        wire::Message::TrackerWithdraw(record) => {
            let ack = match registry.withdraw(record) {
                Ok(()) => wire::TrackerAck {
                    ok: true,
                    reason: String::new(),
                },
                Err(reason) => wire::TrackerAck {
                    ok: false,
                    reason: reason.to_string(),
                },
            };
            wire::Message::TrackerAnnounceAck(ack)
        }
        wire::Message::TrackerResolve(q) => {
            let records = match q.query_kind {
                1 => registry.providers(&q.cid, q.max as usize),
                2 => registry.nodes(q.max as usize),
                3 => registry.relays(q.max as usize),
                4 => registry.all_providers(q.max as usize),
                5 => registry.all_wants(q.max as usize),
                6 => registry.all_metas(q.max as usize),
                7 => registry.roots_for(&q.cid, q.max as usize),
                8 => registry.manifests_for(&q.cid, q.max as usize),
                9 => registry.apps_for(&q.cid, q.max as usize),
                _ => Vec::new(),
            };
            wire::Message::TrackerResolveReply(wire::TrackerResolveReply { records })
        }
        other => {
            tracing::warn!(tag = other.type_tag(), "unexpected on tracker alpn");
            wire::Message::TrackerAnnounceAck(wire::TrackerAck {
                ok: false,
                reason: "unexpected-message".into(),
            })
        }
    }
}
