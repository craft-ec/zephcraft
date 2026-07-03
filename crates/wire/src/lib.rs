//! Wire protocol: framed, postcard-serialized messages (foundation §23, §62).
//!
//! Frame layout (v1, all integers big-endian, 17-byte header):
//!
//! ```text
//! [ type_tag: u32 | version: u8 (=1) | hlc_ts: u64 | payload_len: u32 | payload ]
//! ```
//!
//! The payload is the postcard encoding of the message body for `type_tag`.
//! Clock-skew policy (§62.1): ordinary messages are warn-and-accept; only
//! attestation/SIGNED_WRITE paths use strict rejection.

use serde::{Deserialize, Serialize};

pub const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 17;
/// Maximum total message size (foundation §23).
pub const MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;
/// Strict skew threshold (foundation §42): applied only on strict paths.
pub const MAX_SKEW_MS: u64 = zeph_core::hlc::MAX_SKEW_MS;

/// Message type tags (foundation §23 table).
pub mod tag {
    pub const PING: u32 = 0x0001;
    pub const PONG: u32 = 0x0002;
    // Membership (0x0100 block, foundation §23):
    pub const JOIN: u32 = 0x0100;
    pub const FORWARD_JOIN: u32 = 0x0101;
    pub const NEIGHBOR: u32 = 0x0102;
    pub const NEIGHBOR_REPLY: u32 = 0x0103;
    pub const DISCONNECT: u32 = 0x0104;
    pub const SHUFFLE: u32 = 0x0105;
    pub const SHUFFLE_REPLY: u32 = 0x0106;
    // Piece exchange (CRAFTOBJ_DESIGN §Wire Protocol Additions):
    pub const PIECE_REQUEST: u32 = 0x0010;
    pub const PIECE_RESPONSE: u32 = 0x0011;
    pub const PIECE_PUSH: u32 = 0x0012;
    pub const PIECE_PUSH_ACK: u32 = 0x0013;
    // Tracker registries (content providers / nodes / relays):
    pub const TRACKER_ANNOUNCE: u32 = 0x0200;
    pub const TRACKER_ANNOUNCE_ACK: u32 = 0x0201;
    pub const TRACKER_RESOLVE: u32 = 0x0202;
    pub const TRACKER_RESOLVE_REPLY: u32 = 0x0203;
    pub const TRACKER_WITHDRAW: u32 = 0x0204;
    // HealthScan live availability probe (CRAFTOBJ_DESIGN §HealthScan):
    pub const AVAILABILITY_PROBE: u32 = 0x0041;
    pub const AVAILABILITY_ACK: u32 = 0x0042;
}

/// A signed registry record. The typed payload (provider/node/relay) is
/// opaque bytes here — the routing layer defines and verifies it. Signature
/// covers `kind ‖ node_id ‖ payload ‖ hlc_ts` (routing computes the bytes),
/// so a record cannot be forged for another identity and survives relaying
/// beyond the QUIC session that carried it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedRecord {
    /// 1 = content provider, 2 = node, 3 = relay.
    pub kind: u8,
    pub node_id: [u8; 32],
    pub payload: Vec<u8>,
    pub hlc_ts: u64,
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrackerAck {
    pub ok: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrackerResolve {
    /// 1 = providers for `cid`, 2 = all nodes, 3 = all relays.
    pub query_kind: u8,
    pub cid: [u8; 32],
    pub max: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrackerResolveReply {
    /// Matching signed records — the client RE-VERIFIES each (never trusts
    /// the tracker to have checked).
    pub records: Vec<SignedRecord>,
}

/// A shareable peer address in text form (`<node_id_hex>@<addr>[,...]`,
/// including relay URLs). String-typed so wire stays independent of the
/// transport's address types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInfo {
    pub addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ping {
    pub nonce: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pong {
    pub nonce: [u8; 32],
}

/// One coded piece on the wire (self-describing; CRAFTOBJ_DESIGN v2.0).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WirePiece {
    pub coding_vector: Vec<u8>,
    pub data: Vec<u8>,
}

/// Push one coded piece to a storage peer. Carries the generation shape and
/// the vtags blob so the receiver can verify AT INGEST before accepting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PiecePush {
    pub cid: [u8; 32],
    pub k: u32,
    pub piece_len: u64,
    pub total_len: u64,
    /// postcard-encoded zeph_erasure::vtags::VTags.
    pub vtags: Vec<u8>,
    pub piece: WirePiece,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PiecePushAck {
    pub ok: bool,
    /// "vtag-invalid", "quota", ... when !ok.
    pub reason: String,
}

/// Request up to `max_pieces` pieces for `cid`, excluding piece_ids the
/// requester already holds (exclude-list fetching).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PieceRequest {
    pub cid: [u8; 32],
    pub exclude: Vec<[u8; 32]>,
    pub max_pieces: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PieceResponse {
    pub found: bool,
    pub k: u32,
    pub piece_len: u64,
    pub total_len: u64,
    /// postcard-encoded VTags (empty when !found).
    pub vtags: Vec<u8>,
    pub pieces: Vec<WirePiece>,
}

/// Membership messages (HyParView-style, foundation §3 as amended §62.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Join {
    pub origin: PeerInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardJoin {
    pub origin: PeerInfo,
    pub ttl: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Neighbor {
    pub origin: PeerInfo,
    /// High priority: requester's active view is empty — must accept.
    pub high_priority: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NeighborReply {
    pub accepted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shuffle {
    pub origin: PeerInfo,
    pub sample: Vec<PeerInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShuffleReply {
    pub sample: Vec<PeerInfo>,
}

/// HealthScan live probe: "do you currently hold `cid`?" — cheap, transfers
/// no pieces. The reply carries the VERIFIED holding (provider records are
/// candidates; this is availability truth).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvailabilityProbe {
    pub cid: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvailabilityAck {
    /// True iff this node can currently serve the CID (holds pieces or content).
    pub has: bool,
    /// Coded pieces held (a pinner reports the durability floor — full).
    pub piece_count: u32,
    pub pinned: bool,
}

/// All wire messages. The enum discriminant is NOT serialized — the frame's
/// `type_tag` selects the payload type explicitly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Ping(Ping),
    Pong(Pong),
    Join(Join),
    ForwardJoin(ForwardJoin),
    Neighbor(Neighbor),
    NeighborReply(NeighborReply),
    Disconnect,
    Shuffle(Shuffle),
    ShuffleReply(ShuffleReply),
    PiecePush(PiecePush),
    PiecePushAck(PiecePushAck),
    PieceRequest(PieceRequest),
    PieceResponse(PieceResponse),
    TrackerAnnounce(SignedRecord),
    TrackerAnnounceAck(TrackerAck),
    TrackerResolve(TrackerResolve),
    TrackerResolveReply(TrackerResolveReply),
    TrackerWithdraw(SignedRecord),
    AvailabilityProbe(AvailabilityProbe),
    AvailabilityAck(AvailabilityAck),
}

impl Message {
    pub fn type_tag(&self) -> u32 {
        match self {
            Message::Ping(_) => tag::PING,
            Message::Pong(_) => tag::PONG,
            Message::Join(_) => tag::JOIN,
            Message::ForwardJoin(_) => tag::FORWARD_JOIN,
            Message::Neighbor(_) => tag::NEIGHBOR,
            Message::NeighborReply(_) => tag::NEIGHBOR_REPLY,
            Message::Disconnect => tag::DISCONNECT,
            Message::Shuffle(_) => tag::SHUFFLE,
            Message::ShuffleReply(_) => tag::SHUFFLE_REPLY,
            Message::PiecePush(_) => tag::PIECE_PUSH,
            Message::PiecePushAck(_) => tag::PIECE_PUSH_ACK,
            Message::PieceRequest(_) => tag::PIECE_REQUEST,
            Message::PieceResponse(_) => tag::PIECE_RESPONSE,
            Message::TrackerAnnounce(_) => tag::TRACKER_ANNOUNCE,
            Message::TrackerAnnounceAck(_) => tag::TRACKER_ANNOUNCE_ACK,
            Message::TrackerResolve(_) => tag::TRACKER_RESOLVE,
            Message::TrackerResolveReply(_) => tag::TRACKER_RESOLVE_REPLY,
            Message::TrackerWithdraw(_) => tag::TRACKER_WITHDRAW,
            Message::AvailabilityProbe(_) => tag::AVAILABILITY_PROBE,
            Message::AvailabilityAck(_) => tag::AVAILABILITY_ACK,
        }
    }
}

/// A decoded frame: header fields plus the parsed message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub version: u8,
    pub hlc_ts: u64,
    pub message: Message,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireError {
    #[error("frame too short: {0} bytes (need at least {HEADER_LEN})")]
    TooShort(usize),
    #[error("unsupported frame version {0}")]
    BadVersion(u8),
    #[error("declared payload length {declared} does not match actual {actual}")]
    LengthMismatch { declared: usize, actual: usize },
    #[error("message size {0} exceeds maximum {MAX_MESSAGE_SIZE}")]
    TooLarge(usize),
    #[error("unknown message type tag {0:#06x}")]
    UnknownType(u32),
    #[error("malformed payload for tag {tag:#06x}: {reason}")]
    Malformed { tag: u32, reason: String },
}

/// Encode a message into a v1 frame.
pub fn encode(message: &Message, hlc_ts: u64) -> Vec<u8> {
    let payload = match message {
        Message::Ping(p) => postcard::to_allocvec(p),
        Message::Pong(p) => postcard::to_allocvec(p),
        Message::Join(p) => postcard::to_allocvec(p),
        Message::ForwardJoin(p) => postcard::to_allocvec(p),
        Message::Neighbor(p) => postcard::to_allocvec(p),
        Message::NeighborReply(p) => postcard::to_allocvec(p),
        Message::Disconnect => Ok(Vec::new()),
        Message::Shuffle(p) => postcard::to_allocvec(p),
        Message::ShuffleReply(p) => postcard::to_allocvec(p),
        Message::PiecePush(p) => postcard::to_allocvec(p),
        Message::PiecePushAck(p) => postcard::to_allocvec(p),
        Message::PieceRequest(p) => postcard::to_allocvec(p),
        Message::PieceResponse(p) => postcard::to_allocvec(p),
        Message::TrackerAnnounce(p) => postcard::to_allocvec(p),
        Message::TrackerAnnounceAck(p) => postcard::to_allocvec(p),
        Message::TrackerResolve(p) => postcard::to_allocvec(p),
        Message::TrackerResolveReply(p) => postcard::to_allocvec(p),
        Message::TrackerWithdraw(p) => postcard::to_allocvec(p),
        Message::AvailabilityProbe(p) => postcard::to_allocvec(p),
        Message::AvailabilityAck(p) => postcard::to_allocvec(p),
    }
    .expect("postcard serialization of wire messages cannot fail");
    debug_assert!(HEADER_LEN + payload.len() <= MAX_MESSAGE_SIZE);

    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
    frame.extend_from_slice(&message.type_tag().to_be_bytes());
    frame.push(VERSION);
    frame.extend_from_slice(&hlc_ts.to_be_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    frame
}

/// Decode a v1 frame. Rejects truncation, bad version, length mismatch,
/// oversize, unknown tags, and malformed payloads.
pub fn decode(bytes: &[u8]) -> Result<Frame, WireError> {
    if bytes.len() > MAX_MESSAGE_SIZE {
        return Err(WireError::TooLarge(bytes.len()));
    }
    if bytes.len() < HEADER_LEN {
        return Err(WireError::TooShort(bytes.len()));
    }
    let type_tag = u32::from_be_bytes(bytes[0..4].try_into().expect("4 bytes"));
    let version = bytes[4];
    if version != VERSION {
        return Err(WireError::BadVersion(version));
    }
    let hlc_ts = u64::from_be_bytes(bytes[5..13].try_into().expect("8 bytes"));
    let declared = u32::from_be_bytes(bytes[13..17].try_into().expect("4 bytes")) as usize;
    let payload = &bytes[HEADER_LEN..];
    if declared != payload.len() {
        return Err(WireError::LengthMismatch {
            declared,
            actual: payload.len(),
        });
    }

    let malformed = |e: postcard::Error| WireError::Malformed {
        tag: type_tag,
        reason: e.to_string(),
    };
    let message = match type_tag {
        tag::PING => Message::Ping(postcard::from_bytes(payload).map_err(malformed)?),
        tag::PONG => Message::Pong(postcard::from_bytes(payload).map_err(malformed)?),
        tag::JOIN => Message::Join(postcard::from_bytes(payload).map_err(malformed)?),
        tag::FORWARD_JOIN => {
            Message::ForwardJoin(postcard::from_bytes(payload).map_err(malformed)?)
        }
        tag::NEIGHBOR => Message::Neighbor(postcard::from_bytes(payload).map_err(malformed)?),
        tag::NEIGHBOR_REPLY => {
            Message::NeighborReply(postcard::from_bytes(payload).map_err(malformed)?)
        }
        tag::DISCONNECT => Message::Disconnect,
        tag::SHUFFLE => Message::Shuffle(postcard::from_bytes(payload).map_err(malformed)?),
        tag::SHUFFLE_REPLY => {
            Message::ShuffleReply(postcard::from_bytes(payload).map_err(malformed)?)
        }
        tag::PIECE_PUSH => Message::PiecePush(postcard::from_bytes(payload).map_err(malformed)?),
        tag::PIECE_PUSH_ACK => {
            Message::PiecePushAck(postcard::from_bytes(payload).map_err(malformed)?)
        }
        tag::PIECE_REQUEST => {
            Message::PieceRequest(postcard::from_bytes(payload).map_err(malformed)?)
        }
        tag::PIECE_RESPONSE => {
            Message::PieceResponse(postcard::from_bytes(payload).map_err(malformed)?)
        }
        tag::TRACKER_ANNOUNCE => {
            Message::TrackerAnnounce(postcard::from_bytes(payload).map_err(malformed)?)
        }
        tag::TRACKER_ANNOUNCE_ACK => {
            Message::TrackerAnnounceAck(postcard::from_bytes(payload).map_err(malformed)?)
        }
        tag::TRACKER_RESOLVE => {
            Message::TrackerResolve(postcard::from_bytes(payload).map_err(malformed)?)
        }
        tag::TRACKER_RESOLVE_REPLY => {
            Message::TrackerResolveReply(postcard::from_bytes(payload).map_err(malformed)?)
        }
        tag::TRACKER_WITHDRAW => {
            Message::TrackerWithdraw(postcard::from_bytes(payload).map_err(malformed)?)
        }
        tag::AVAILABILITY_PROBE => {
            Message::AvailabilityProbe(postcard::from_bytes(payload).map_err(malformed)?)
        }
        tag::AVAILABILITY_ACK => {
            Message::AvailabilityAck(postcard::from_bytes(payload).map_err(malformed)?)
        }
        other => return Err(WireError::UnknownType(other)),
    };
    Ok(Frame {
        version,
        hlc_ts,
        message,
    })
}

/// Skew verdict per the §62.1 policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Skew {
    /// Within tolerance.
    Ok,
    /// Beyond tolerance on an ordinary path: accept, clamp HLC merge, log.
    WarnAccept { skew_ms: u64 },
    /// Beyond tolerance on a strict path (attestation, SIGNED_WRITE): reject.
    Reject { skew_ms: u64 },
}

/// Evaluate sender clock skew. `strict` is true only for attestation and
/// SIGNED_WRITE paths (foundation §62.1).
pub fn check_skew(local_hlc: u64, remote_hlc: u64, strict: bool) -> Skew {
    let local_ms = local_hlc >> 16;
    let remote_ms = remote_hlc >> 16;
    let skew_ms = local_ms.abs_diff(remote_ms);
    if skew_ms <= MAX_SKEW_MS {
        Skew::Ok
    } else if strict {
        Skew::Reject { skew_ms }
    } else {
        Skew::WarnAccept { skew_ms }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ping() -> Message {
        Message::Ping(Ping { nonce: [7u8; 32] })
    }

    #[test]
    fn roundtrip_ping_pong() {
        for msg in [ping(), Message::Pong(Pong { nonce: [9u8; 32] })] {
            let ts = zeph_core::hlc::Clock::new().now().0;
            let bytes = encode(&msg, ts);
            let frame = decode(&bytes).unwrap();
            assert_eq!(frame.message, msg);
            assert_eq!(frame.hlc_ts, ts);
            assert_eq!(frame.version, VERSION);
        }
    }

    #[test]
    fn header_layout_is_exactly_17_bytes_big_endian() {
        let bytes = encode(&ping(), 0x0000_1122_3344_5566);
        assert_eq!(&bytes[0..4], &0x0000_0001u32.to_be_bytes()); // PING tag
        assert_eq!(bytes[4], 1); // version
        assert_eq!(&bytes[5..13], &0x0000_1122_3344_5566u64.to_be_bytes());
        let declared = u32::from_be_bytes(bytes[13..17].try_into().unwrap()) as usize;
        assert_eq!(declared, bytes.len() - HEADER_LEN);
    }

    #[test]
    fn rejects_truncated_frames() {
        let bytes = encode(&ping(), 1);
        for cut in [0, 1, HEADER_LEN - 1, bytes.len() - 1] {
            assert!(decode(&bytes[..cut]).is_err(), "cut at {cut} must fail");
        }
    }

    #[test]
    fn rejects_bad_version_and_unknown_tag() {
        let mut bytes = encode(&ping(), 1);
        bytes[4] = 9;
        assert_eq!(decode(&bytes), Err(WireError::BadVersion(9)));

        let mut bytes = encode(&ping(), 1);
        bytes[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_be_bytes());
        assert_eq!(decode(&bytes), Err(WireError::UnknownType(0xDEAD_BEEF)));
    }

    #[test]
    fn rejects_length_mismatch_and_garbage_payload() {
        let mut bytes = encode(&ping(), 1);
        let wrong = (bytes.len() - HEADER_LEN + 5) as u32;
        bytes[13..17].copy_from_slice(&wrong.to_be_bytes());
        assert!(matches!(
            decode(&bytes),
            Err(WireError::LengthMismatch { .. })
        ));

        let mut bytes = encode(&ping(), 1);
        bytes.truncate(HEADER_LEN + 3); // partial payload
        bytes[13..17].copy_from_slice(&3u32.to_be_bytes());
        assert!(matches!(decode(&bytes), Err(WireError::Malformed { .. })));
    }

    #[test]
    fn rejects_oversize() {
        let huge = vec![0u8; MAX_MESSAGE_SIZE + 1];
        assert_eq!(
            decode(&huge),
            Err(WireError::TooLarge(MAX_MESSAGE_SIZE + 1))
        );
    }

    #[test]
    fn roundtrip_membership_messages() {
        let info = |s: &str| PeerInfo { addr: s.into() };
        let msgs = [
            Message::Join(Join {
                origin: info("aa@1.2.3.4:9"),
            }),
            Message::ForwardJoin(ForwardJoin {
                origin: info("bb@[::1]:2"),
                ttl: 6,
            }),
            Message::Neighbor(Neighbor {
                origin: info("cc@9.9.9.9:1"),
                high_priority: true,
            }),
            Message::NeighborReply(NeighborReply { accepted: false }),
            Message::Disconnect,
            Message::Shuffle(Shuffle {
                origin: info("dd@8.8.8.8:1"),
                sample: vec![info("ee@7.7.7.7:2"), info("ff@6.6.6.6:3")],
            }),
            Message::ShuffleReply(ShuffleReply {
                sample: vec![info("gg@5.5.5.5:4")],
            }),
        ];
        for msg in msgs {
            let bytes = encode(&msg, 42);
            let frame = decode(&bytes).unwrap();
            assert_eq!(frame.message, msg);
        }
    }

    #[test]
    fn roundtrip_piece_messages() {
        let piece = WirePiece {
            coding_vector: vec![1, 2, 3, 4],
            data: vec![9u8; 64],
        };
        let msgs = [
            Message::PiecePush(PiecePush {
                cid: [7; 32],
                k: 8,
                piece_len: 64,
                total_len: 500,
                vtags: vec![1, 2, 3],
                piece: piece.clone(),
            }),
            Message::PiecePushAck(PiecePushAck {
                ok: false,
                reason: "vtag-invalid".into(),
            }),
            Message::PieceRequest(PieceRequest {
                cid: [7; 32],
                exclude: vec![[1; 32]],
                max_pieces: 2,
            }),
            Message::PieceResponse(PieceResponse {
                found: true,
                k: 8,
                piece_len: 64,
                total_len: 500,
                vtags: vec![1, 2, 3],
                pieces: vec![piece],
            }),
        ];
        for msg in msgs {
            let bytes = encode(&msg, 5);
            assert_eq!(decode(&bytes).unwrap().message, msg);
        }
    }

    #[test]
    fn skew_policy_warn_vs_strict() {
        let base = 1_000_000u64 << 16;
        let skewed = (1_000_000u64 + 700) << 16; // 700ms ahead
        assert_eq!(check_skew(base, base + (200 << 16), false), Skew::Ok);
        assert!(matches!(
            check_skew(base, skewed, false),
            Skew::WarnAccept { skew_ms: 700 }
        ));
        assert!(matches!(
            check_skew(base, skewed, true),
            Skew::Reject { skew_ms: 700 }
        ));
    }
}
