//! RLNC erasure coding over GF(2⁸) with null-space verification tags.
//!
//! Spec: foundation §4/§28 (+ §62.1 vtags amendment), CRAFTOBJ_DESIGN v2.0.
//! Pieces are random linear combinations of K equal-length source pieces;
//! any K linearly independent pieces reconstruct the source (progressive
//! Gaussian elimination), and any holder of ≥2 pieces can mint fresh valid
//! pieces by recombination — no decode needed (the churn-repair property).
//!
//! Deviations recorded in the tracker: scalar table-driven GF math (explicit
//! SIMD deferred to a perf pass); see `vtags` for the adaptive-attacker
//! caveat and its defense-in-depth.

pub mod gf;
pub mod vtags;

use serde::{Deserialize, Serialize};

/// Redundancy target shared by encoder and health scanner (foundation §28):
/// `n = k × ceil(2.0 + 16/k)`. K=32→96, K=16→48, K=8→32.
pub fn target_pieces(k: usize) -> usize {
    assert!(k > 0);
    k * (2 + 16usize.div_ceil(k))
}

/// One coded piece: a coding vector over the K sources plus combined data.
/// Self-describing headers (cid, segment ids) wrap this at the obj layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodedPiece {
    pub coding_vector: Vec<u8>,
    pub data: Vec<u8>,
}

impl CodedPiece {
    /// piece_id = BLAKE3(coding_vector ‖ data) — coefficients alone do not
    /// identify a piece (foundation §4).
    pub fn piece_id(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.coding_vector);
        hasher.update(&self.data);
        *hasher.finalize().as_bytes()
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ErasureError {
    #[error("source pieces must be non-empty and of equal length")]
    BadSources,
    #[error("piece shape mismatch: expected k={expected_k}, len={expected_len}")]
    Shape {
        expected_k: usize,
        expected_len: usize,
    },
    #[error("need at least one piece to recode")]
    NothingToRecode,
    #[error("decoder is not complete (rank {rank} of {k})")]
    Incomplete { rank: usize, k: usize },
}

fn check_sources(sources: &[Vec<u8>]) -> Result<usize, ErasureError> {
    let Some(first) = sources.first() else {
        return Err(ErasureError::BadSources);
    };
    if first.is_empty() || sources.iter().any(|s| s.len() != first.len()) {
        return Err(ErasureError::BadSources);
    }
    Ok(first.len())
}

/// Encode one coded piece: a random linear combination of the sources.
pub fn encode(sources: &[Vec<u8>], rng: &mut impl rand::Rng) -> Result<CodedPiece, ErasureError> {
    let piece_len = check_sources(sources)?;
    let k = sources.len();
    // Random coding vector; all-zero is useless, resample (p = 256^-k).
    let mut coding_vector = vec![0u8; k];
    loop {
        rng.fill_bytes(&mut coding_vector);
        if coding_vector.iter().any(|&c| c != 0) {
            break;
        }
    }
    let mut data = vec![0u8; piece_len];
    for (i, source) in sources.iter().enumerate() {
        gf::axpy(&mut data, source, coding_vector[i]);
    }
    Ok(CodedPiece {
        coding_vector,
        data,
    })
}

/// Encode `n` coded pieces (publish path: n = target_pieces(k)).
pub fn encode_n(
    sources: &[Vec<u8>],
    n: usize,
    rng: &mut impl rand::Rng,
) -> Result<Vec<CodedPiece>, ErasureError> {
    (0..n).map(|_| encode(sources, rng)).collect()
}

/// Recode: mint a fresh piece from held pieces WITHOUT decoding — the RLNC
/// repair property. The result is a random combination of the inputs.
pub fn recode(held: &[CodedPiece], rng: &mut impl rand::Rng) -> Result<CodedPiece, ErasureError> {
    let Some(first) = held.first() else {
        return Err(ErasureError::NothingToRecode);
    };
    let k = first.coding_vector.len();
    let piece_len = first.data.len();
    if held
        .iter()
        .any(|p| p.coding_vector.len() != k || p.data.len() != piece_len)
    {
        return Err(ErasureError::Shape {
            expected_k: k,
            expected_len: piece_len,
        });
    }
    let mut coding_vector = vec![0u8; k];
    let mut data = vec![0u8; piece_len];
    loop {
        for piece in held {
            let c = rand::Rng::gen::<u8>(rng);
            gf::axpy(&mut coding_vector, &piece.coding_vector, c);
            gf::axpy(&mut data, &piece.data, c);
        }
        if coding_vector.iter().any(|&c| c != 0) {
            break;
        }
        // Degenerate combination (astronomically rare): restart.
        coding_vector.fill(0);
        data.fill(0);
    }
    Ok(CodedPiece {
        coding_vector,
        data,
    })
}

/// Progressive Gaussian elimination decoder: feed pieces as they arrive;
/// linearly dependent pieces are rejected cheaply; at rank K the sources
/// fall out of the reduced rows directly.
pub struct Decoder {
    k: usize,
    piece_len: usize,
    /// Rows in reduced form; `pivot[r]` is the pivot column of row r.
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    pivots: Vec<usize>,
}

impl Decoder {
    pub fn new(k: usize, piece_len: usize) -> Self {
        Self {
            k,
            piece_len,
            rows: Vec::with_capacity(k),
            pivots: Vec::with_capacity(k),
        }
    }

    pub fn rank(&self) -> usize {
        self.rows.len()
    }

    pub fn is_complete(&self) -> bool {
        self.rank() == self.k
    }

    /// Returns true if the piece was linearly independent (rank increased).
    pub fn add_piece(&mut self, piece: &CodedPiece) -> Result<bool, ErasureError> {
        if piece.coding_vector.len() != self.k || piece.data.len() != self.piece_len {
            return Err(ErasureError::Shape {
                expected_k: self.k,
                expected_len: self.piece_len,
            });
        }
        if self.is_complete() {
            return Ok(false);
        }
        let mut vector = piece.coding_vector.clone();
        let mut data = piece.data.clone();

        // Reduce by existing pivots.
        for (row, &pivot) in self.rows.iter().zip(&self.pivots) {
            let c = vector[pivot];
            if c != 0 {
                gf::axpy(&mut vector, &row.0, c);
                gf::axpy(&mut data, &row.1, c);
            }
        }
        let Some(pivot) = vector.iter().position(|&c| c != 0) else {
            return Ok(false); // dependent
        };
        // Normalize the pivot to 1.
        let inv = gf::inv(vector[pivot]);
        gf::scale(&mut vector, inv);
        gf::scale(&mut data, inv);
        // Eliminate this pivot from existing rows (keep matrix reduced).
        for (row, _) in self.rows.iter_mut().zip(&self.pivots) {
            let c = row.0[pivot];
            if c != 0 {
                let (rv, rd) = row;
                gf::axpy(rv, &vector, c);
                gf::axpy(rd, &data, c);
            }
        }
        self.rows.push((vector, data));
        self.pivots.push(pivot);
        Ok(true)
    }

    /// Reconstruct the K source pieces (rows ordered by pivot column).
    pub fn decode(self) -> Result<Vec<Vec<u8>>, ErasureError> {
        if !self.is_complete() {
            return Err(ErasureError::Incomplete {
                rank: self.rank(),
                k: self.k,
            });
        }
        let mut ordered: Vec<(usize, Vec<u8>)> = self
            .pivots
            .into_iter()
            .zip(self.rows.into_iter().map(|(_, data)| data))
            .collect();
        ordered.sort_by_key(|(pivot, _)| *pivot);
        Ok(ordered.into_iter().map(|(_, data)| data).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    fn random_sources(rng: &mut StdRng, k: usize, len: usize) -> Vec<Vec<u8>> {
        (0..k)
            .map(|_| (0..len).map(|_| rng.gen()).collect())
            .collect()
    }

    /// GATE: any K linearly independent pieces decode back to the source —
    /// across k values, with shuffled subsets of an n-piece spread.
    #[test]
    fn any_k_independent_pieces_decode_to_source() {
        for (seed, k) in [(1u64, 8usize), (2, 16), (3, 32)] {
            let mut rng = StdRng::seed_from_u64(seed);
            let sources = random_sources(&mut rng, k, 1024);
            let n = target_pieces(k);
            let mut pieces = encode_n(&sources, n, &mut rng).unwrap();

            use rand::seq::SliceRandom;
            pieces.shuffle(&mut rng);
            // Simulate churn: throw away all but K + a couple.
            pieces.truncate(k + 2);

            let mut decoder = Decoder::new(k, 1024);
            for piece in &pieces {
                if decoder.is_complete() {
                    break;
                }
                decoder.add_piece(piece).unwrap();
            }
            assert!(decoder.is_complete(), "k={k}: rank {} < k", decoder.rank());
            assert_eq!(decoder.decode().unwrap(), sources, "k={k}");
        }
    }

    /// GATE: recoded pieces (including recodes-of-recodes) are first-class —
    /// they mix with originals and still decode to the source.
    #[test]
    fn recoded_pieces_decode_with_originals() {
        let mut rng = StdRng::seed_from_u64(7);
        let k = 16;
        let sources = random_sources(&mut rng, k, 512);
        let originals = encode_n(&sources, k + 4, &mut rng).unwrap();

        // A repair node holding only 2 pieces mints new ones; chain twice.
        let gen1: Vec<CodedPiece> = (0..8)
            .map(|_| recode(&originals[..2], &mut rng).unwrap())
            .collect();
        let gen2: Vec<CodedPiece> = (0..8).map(|_| recode(&gen1, &mut rng).unwrap()).collect();

        let mut decoder = Decoder::new(k, 512);
        for piece in gen2.iter().chain(gen1.iter()).chain(originals.iter()) {
            if decoder.is_complete() {
                break;
            }
            decoder.add_piece(piece).unwrap();
        }
        assert!(decoder.is_complete());
        assert_eq!(decoder.decode().unwrap(), sources);
    }

    #[test]
    fn dependent_pieces_are_rejected() {
        let mut rng = StdRng::seed_from_u64(9);
        let sources = random_sources(&mut rng, 8, 64);
        let piece = encode(&sources, &mut rng).unwrap();
        let mut decoder = Decoder::new(8, 64);
        assert!(decoder.add_piece(&piece).unwrap());
        assert!(
            !decoder.add_piece(&piece).unwrap(),
            "duplicate must not raise rank"
        );
        // A scaled copy is also dependent.
        let mut scaled = piece.clone();
        gf::scale(&mut scaled.coding_vector, 3);
        gf::scale(&mut scaled.data, 3);
        assert!(!decoder.add_piece(&scaled).unwrap());
    }

    #[test]
    fn incomplete_decode_errors() {
        let mut rng = StdRng::seed_from_u64(11);
        let sources = random_sources(&mut rng, 8, 64);
        let mut decoder = Decoder::new(8, 64);
        decoder
            .add_piece(&encode(&sources, &mut rng).unwrap())
            .unwrap();
        assert!(matches!(
            decoder.decode(),
            Err(ErasureError::Incomplete { rank: 1, k: 8 })
        ));
    }

    #[test]
    fn target_pieces_matches_foundation_table() {
        assert_eq!(target_pieces(32), 96);
        assert_eq!(target_pieces(16), 48);
        assert_eq!(target_pieces(8), 32);
    }
}
