//! Durable page generations (foundation §33). A commit's new pages are packed
//! into one immutable, self-verifying blob and published like a file — erasure-
//! coded k=8/n=32 and repaired by HealthScan — so the DB survives loss of the
//! owner + page-holders. Recovery decodes the generations back into pages.

use serde::{Deserialize, Serialize};
use zeph_core::Cid;

use crate::{Result, SqlError};

/// One generation: the (CID, bytes) of each page in a commit's batch.
#[derive(Serialize, Deserialize)]
struct GenBlob {
    pages: Vec<([u8; 32], Vec<u8>)>,
}

/// Pack a batch of pages into a generation blob (the unit that gets erasure-
/// coded and distributed).
pub(crate) fn pack(pages: &[(Cid, Vec<u8>)]) -> Result<Vec<u8>> {
    let g = GenBlob {
        pages: pages.iter().map(|(c, d)| (c.0, d.clone())).collect(),
    };
    postcard::to_allocvec(&g).map_err(|e| SqlError::Serde(e.to_string()))
}

/// Unpack a generation blob back into its pages, verifying each page's CID
/// (self-authenticating — corrupt or spoofed pieces can't reconstruct).
pub(crate) fn unpack(blob: &[u8]) -> Result<Vec<(Cid, Vec<u8>)>> {
    let g: GenBlob =
        postcard::from_bytes(blob).map_err(|e| SqlError::CorruptIndex(e.to_string()))?;
    let mut out = Vec::with_capacity(g.pages.len());
    for (cid, data) in g.pages {
        if Cid::of(&data) != Cid(cid) {
            return Err(SqlError::CorruptIndex(format!(
                "generation page hash mismatch {}",
                Cid(cid).to_hex()
            )));
        }
        out.push((Cid(cid), data));
    }
    Ok(out)
}

/// Durable, self-healing blob storage — a generation goes in erasure-coded
/// (k=8/n=32) + distributed + repaired, and comes back reconstructed from any k
/// pieces. Abstracts the obj engine so CraftSQL is testable without a network.
#[async_trait::async_trait]
pub trait DurableStore: Send + Sync {
    /// Publish a generation blob durably; returns its content CID.
    async fn put_generation(&self, blob: Vec<u8>) -> Result<Cid>;
    /// Reconstruct a generation blob by CID (None if unrecoverable).
    async fn get_generation(&self, cid: Cid) -> Result<Option<Vec<u8>>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(fill: u8, n: usize) -> Vec<u8> {
        vec![fill; n]
    }

    #[test]
    fn pack_unpack_roundtrips_and_verifies() {
        let pages: Vec<(Cid, Vec<u8>)> = (0..8u8)
            .map(|i| {
                let d = page(i, 2048);
                (Cid::of(&d), d)
            })
            .collect();
        let blob = pack(&pages).unwrap();
        let back = unpack(&blob).unwrap();
        assert_eq!(back, pages, "generation round-trips");

        // Tamper with a page's bytes but keep its (now-wrong) CID → rejected.
        let mut bad = GenBlob {
            pages: pages.iter().map(|(c, d)| (c.0, d.clone())).collect(),
        };
        bad.pages[3].1[0] ^= 0xff;
        let blob = postcard::to_allocvec(&bad).unwrap();
        assert!(unpack(&blob).is_err(), "corrupt page is rejected");
    }
}
