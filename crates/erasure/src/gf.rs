//! GF(2⁸) arithmetic with compile-time log/exp tables (polynomial 0x11D).
//!
//! Bulk row operations use a per-coefficient 256-byte multiplication table,
//! which the compiler auto-vectorizes acceptably; explicit SIMD shuffle
//! kernels are a recorded perf deviation for a later pass.

const POLY: u16 = 0x11D;

const TABLES: ([u8; 512], [u8; 256]) = {
    let mut exp = [0u8; 512];
    let mut log = [0u8; 256];
    let mut x: u16 = 1;
    let mut i = 0;
    while i < 255 {
        exp[i] = x as u8;
        log[x as usize] = i as u8;
        x <<= 1;
        if x & 0x100 != 0 {
            x ^= POLY;
        }
        i += 1;
    }
    // Duplicate so exp[a + b] never needs a mod for a, b < 255.
    let mut j = 255;
    while j < 512 {
        exp[j] = exp[j - 255];
        j += 1;
    }
    (exp, log)
};

const EXP: [u8; 512] = TABLES.0;
const LOG: [u8; 256] = TABLES.1;

/// Addition/subtraction in GF(2⁸) is XOR.
#[inline]
pub fn add(a: u8, b: u8) -> u8 {
    a ^ b
}

#[inline]
pub fn mul(a: u8, b: u8) -> u8 {
    if a == 0 || b == 0 {
        0
    } else {
        EXP[LOG[a as usize] as usize + LOG[b as usize] as usize]
    }
}

/// Multiplicative inverse. Panics on 0 (no inverse exists).
#[inline]
pub fn inv(a: u8) -> u8 {
    assert!(a != 0, "0 has no inverse in GF(2^8)");
    EXP[255 - LOG[a as usize] as usize]
}

/// 256-entry multiplication table for a fixed coefficient.
#[inline]
fn mul_table(c: u8) -> [u8; 256] {
    let mut table = [0u8; 256];
    if c == 0 {
        return table;
    }
    let log_c = LOG[c as usize] as usize;
    let mut x = 1usize;
    while x < 256 {
        table[x] = EXP[log_c + LOG[x] as usize];
        x += 1;
    }
    table
}

/// dst ^= c * src (the RLNC row operation). No-op when c == 0.
pub fn axpy(dst: &mut [u8], src: &[u8], c: u8) {
    debug_assert_eq!(dst.len(), src.len());
    if c == 0 {
        return;
    }
    if c == 1 {
        for (d, s) in dst.iter_mut().zip(src) {
            *d ^= s;
        }
        return;
    }
    let table = mul_table(c);
    for (d, s) in dst.iter_mut().zip(src) {
        *d ^= table[*s as usize];
    }
}

/// dst *= c in place.
pub fn scale(dst: &mut [u8], c: u8) {
    if c == 1 {
        return;
    }
    let table = mul_table(c);
    for d in dst.iter_mut() {
        *d = table[*d as usize];
    }
}

/// Dot product Σ aᵢ·bᵢ over GF(2⁸).
pub fn dot(a: &[u8], b: &[u8]) -> u8 {
    debug_assert_eq!(a.len(), b.len());
    let mut acc = 0u8;
    for (&x, &y) in a.iter().zip(b) {
        acc ^= mul(x, y);
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_axioms_hold() {
        for a in 1..=255u8 {
            assert_eq!(mul(a, inv(a)), 1, "a * a^-1 == 1 for a={a}");
            assert_eq!(mul(a, 1), a);
            assert_eq!(mul(a, 0), 0);
        }
        // Distributivity fuzz over the whole field cross-section.
        for a in (0..=255u8).step_by(7) {
            for b in (0..=255u8).step_by(11) {
                for c in (0..=255u8).step_by(13) {
                    assert_eq!(mul(a, add(b, c)), add(mul(a, b), mul(a, c)));
                }
            }
        }
    }

    #[test]
    fn axpy_matches_scalar_mul() {
        let src: Vec<u8> = (0..=255).collect();
        for c in [0u8, 1, 2, 87, 255] {
            let mut dst = vec![0u8; 256];
            axpy(&mut dst, &src, c);
            for (i, &d) in dst.iter().enumerate() {
                assert_eq!(d, mul(src[i], c));
            }
        }
    }
}
