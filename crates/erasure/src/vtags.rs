//! Null-space verification tags (decision R1, foundation §62.1).
//!
//! For source rows rᵢ = (eᵢ ‖ sᵢ), every valid coded piece (c ‖ d) lies in
//! their span. A null vector v = (v_c ‖ v_d) satisfies rᵢ·v = 0 for all i,
//! which by the augmented structure means v_c[i] = sᵢ·v_d. We derive v_d
//! from a 32-byte seed via BLAKE3 XOF, so a tag stores only (seed, v_c) —
//! ~(32 + k) bytes instead of a full-length vector — and any node verifies
//! any piece (original or recoded) with one derived stream + two dot
//! products per tag. L = 8 tags → forgery probability 2⁻⁶⁴ for corruption
//! crafted independently of the tags.
//!
//! SECURITY CAVEAT (recorded for R1 follow-up): tags are public, so the
//! 2⁻⁶⁴ bound holds against random/blind corruption (bit rot, transmission
//! faults, non-adaptive garbage) — the overwhelmingly common case — but an
//! ADAPTIVE attacker who solves the published linear constraints can craft
//! pieces that pass vtags while lying outside the row space. Defense in
//! depth already in the architecture: PDP coefficient cross-checks by
//! challengers holding unpredictable pieces (M3), and whole-content BLAKE3
//! verification at decode with parole/ban on mismatch. A computationally
//! sound public scheme (pairing-based homomorphic signatures) is the
//! documented upgrade path if adaptive pollution is observed in practice.

use serde::{Deserialize, Serialize};

use crate::{gf, CodedPiece, ErasureError};

/// Number of independent tags (foundation: L=8 → 2⁻⁶⁴).
pub const L: usize = 8;

/// Verification scheme identifiers. Present from day one so a
/// computationally-sound scheme (e.g. pairing-based homomorphic
/// signatures) can replace null-space tags without breaking the blob or
/// piece formats: old content keeps verifying under its recorded scheme.
pub const SCHEME_NULL_SPACE_V1: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VTag {
    /// Seed deriving the data part v_d (length = piece_len) via BLAKE3 XOF.
    pub seed: [u8; 32],
    /// Coefficient part v_c: v_c[i] = sᵢ · v_d, one byte per source piece.
    pub coeff_part: Vec<u8>,
}

/// The vtags blob for one generation/segment — published as a CraftOBJ
/// object; its own CID is its integrity proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VTags {
    /// Verification scheme (SCHEME_NULL_SPACE_V1 today).
    pub scheme: u8,
    pub k: u32,
    pub piece_len: u64,
    pub tags: Vec<VTag>,
}

fn derive_v_d(seed: &[u8; 32], piece_len: usize) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"zeph-vtag-v1");
    hasher.update(seed);
    let mut out = vec![0u8; piece_len];
    hasher.finalize_xof().fill(&mut out);
    out
}

/// Publisher side: generate L tags for a generation of source pieces.
pub fn generate(sources: &[Vec<u8>], rng: &mut impl rand::Rng) -> Result<VTags, ErasureError> {
    let Some(first) = sources.first() else {
        return Err(ErasureError::BadSources);
    };
    let piece_len = first.len();
    if piece_len == 0 || sources.iter().any(|s| s.len() != piece_len) {
        return Err(ErasureError::BadSources);
    }
    let tags = (0..L)
        .map(|_| {
            let mut seed = [0u8; 32];
            rng.fill_bytes(&mut seed);
            let v_d = derive_v_d(&seed, piece_len);
            let coeff_part = sources.iter().map(|s| gf::dot(s, &v_d)).collect();
            VTag { seed, coeff_part }
        })
        .collect();
    Ok(VTags {
        scheme: SCHEME_NULL_SPACE_V1,
        k: sources.len() as u32,
        piece_len: piece_len as u64,
        tags,
    })
}

/// Any-node verification: piece is a valid combination of the sources iff
/// dot(c, v_c) ⊕ dot(d, v_d) == 0 for every tag.
pub fn verify(vtags: &VTags, piece: &CodedPiece) -> bool {
    if vtags.scheme != SCHEME_NULL_SPACE_V1 {
        return false; // unknown scheme: refuse rather than falsely accept
    }
    if piece.coding_vector.len() != vtags.k as usize || piece.data.len() != vtags.piece_len as usize
    {
        return false;
    }
    vtags.tags.iter().all(|tag| {
        let v_d = derive_v_d(&tag.seed, vtags.piece_len as usize);
        gf::dot(&piece.coding_vector, &tag.coeff_part) ^ gf::dot(&piece.data, &v_d) == 0
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{encode_n, recode};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    fn setup(seed: u64, k: usize, len: usize) -> (Vec<Vec<u8>>, VTags, StdRng) {
        let mut rng = StdRng::seed_from_u64(seed);
        let sources: Vec<Vec<u8>> = (0..k)
            .map(|_| (0..len).map(|_| rng.gen()).collect())
            .collect();
        let vtags = generate(&sources, &mut rng).unwrap();
        (sources, vtags, rng)
    }

    /// GATE: every original piece verifies; recoded pieces (and recodes of
    /// recodes) verify with the SAME tags — no key handoff for repair.
    #[test]
    fn originals_and_recoded_pieces_verify() {
        let (sources, vtags, mut rng) = setup(21, 16, 700);
        let pieces = encode_n(&sources, 24, &mut rng).unwrap();
        for piece in &pieces {
            assert!(verify(&vtags, piece), "original piece must verify");
        }
        let repair1 = recode(&pieces[..2], &mut rng).unwrap();
        let repair2 = recode(&[repair1.clone(), pieces[5].clone()], &mut rng).unwrap();
        assert!(verify(&vtags, &repair1), "recoded piece must verify");
        assert!(verify(&vtags, &repair2), "recode-of-recode must verify");
    }

    /// GATE: corrupted pieces fail — flipped data byte, flipped coefficient,
    /// wholesale garbage, and wrong shapes.
    #[test]
    fn corrupted_pieces_fail() {
        let (sources, vtags, mut rng) = setup(23, 8, 300);
        let piece = crate::encode(&sources, &mut rng).unwrap();
        assert!(verify(&vtags, &piece), "sanity: honest piece verifies");

        let mut bad_data = piece.clone();
        bad_data.data[137] ^= 0x5A;
        assert!(!verify(&vtags, &bad_data), "flipped data byte must fail");

        let mut bad_vec = piece.clone();
        bad_vec.coding_vector[3] ^= 0x01;
        assert!(!verify(&vtags, &bad_vec), "flipped coefficient must fail");

        let garbage = CodedPiece {
            coding_vector: vec![7u8; 8],
            data: vec![42u8; 300],
        };
        assert!(!verify(&vtags, &garbage), "garbage must fail");

        let wrong_shape = CodedPiece {
            coding_vector: vec![1u8; 9],
            data: vec![0u8; 300],
        };
        assert!(!verify(&vtags, &wrong_shape), "wrong shape must fail");
    }

    #[test]
    fn unknown_scheme_is_refused() {
        let (sources, mut vtags, mut rng) = setup(25, 8, 128);
        let piece = crate::encode(&sources, &mut rng).unwrap();
        assert!(verify(&vtags, &piece));
        vtags.scheme = 99;
        assert!(!verify(&vtags, &piece), "unknown scheme must not verify");
    }

    #[test]
    fn blob_is_small() {
        // (seed 32B + coeff_part kB) × L tags + header — independent of
        // piece_len, so the published blob stays tiny even for 256KiB pieces.
        let (_, vtags, _) = setup(24, 32, 4096);
        let approx = vtags
            .tags
            .iter()
            .map(|t| 32 + t.coeff_part.len())
            .sum::<usize>()
            + 16;
        assert!(approx < 1024, "vtags blob stays under 1 KiB, got {approx}");
    }
}
