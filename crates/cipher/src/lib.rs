//! `zeph-cipher` — content encryption for ZephCraft (confidentiality layer).
//!
//! Hybrid scheme (see `docs/ENCRYPTION_DESIGN.md`):
//! - **Bulk**: content sealed with a random per-object **DEK** under
//!   XChaCha20-Poly1305 (proven AEAD). `CID = BLAKE3(ciphertext)` (caller-computed)
//!   — the network only ever sees ciphertext.
//! - **Capsule**: the DEK is encapsulated under the owner's key as a **PRE-native
//!   capsule** (Umbral). Self-open needs the owner's PRE secret key. Sharing (proxy
//!   re-encryption) is additive later — no format change.
//! - **Crypto-shred**: destroy the capsule → the random DEK is unrecoverable →
//!   ciphertext decrypts to nothing, forever.
//!
//! Self-contained: no network, no filesystem.

use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde::{Deserialize, Serialize};
use umbral_pre::{
    decrypt_original, decrypt_reencrypted, encrypt as umbral_encrypt, generate_kfrags,
    reencrypt as umbral_reencrypt, Capsule, CapsuleFrag, KeyFrag, PublicKey, SecretKey,
    SecretKeyFactory, Signer,
};

pub const DEK_LEN: usize = 32;
pub const XNONCE_LEN: usize = 24;

#[derive(Debug, thiserror::Error)]
pub enum CipherError {
    #[error("decrypt failed (wrong key, tampered ciphertext, or shredded)")]
    Decrypt,
    #[error("malformed input: {0}")]
    Malformed(&'static str),
}

pub type Result<T> = std::result::Result<T, CipherError>;

// ─────────────────────────── DEK + bulk (XChaCha20-Poly1305) ───────────────────

/// A random 32-byte data-encryption key (one per object). Zeroized on drop.
#[derive(Clone)]
pub struct Dek([u8; DEK_LEN]);

impl Dek {
    pub fn generate() -> Self {
        let mut k = [0u8; DEK_LEN];
        rand::RngCore::fill_bytes(&mut OsRng, &mut k);
        Dek(k)
    }
    pub fn from_bytes(b: [u8; DEK_LEN]) -> Self {
        Dek(b)
    }
    pub fn as_bytes(&self) -> &[u8; DEK_LEN] {
        &self.0
    }
}

impl Drop for Dek {
    fn drop(&mut self) {
        self.0.iter_mut().for_each(|b| *b = 0);
    }
}

/// Seal `plaintext` under `dek`. Returns `nonce(24) || ciphertext+tag`; the AEAD
/// tag authenticates it (`open` fails closed on any tamper).
pub fn seal(dek: &Dek, plaintext: &[u8]) -> Vec<u8> {
    let cipher = XChaCha20Poly1305::new((&dek.0).into());
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let ct = cipher
        .encrypt(&nonce, plaintext)
        .expect("xchacha20poly1305 encrypt infallible for valid key");
    let mut out = Vec::with_capacity(XNONCE_LEN + ct.len());
    out.extend_from_slice(nonce.as_slice());
    out.extend_from_slice(&ct);
    out
}

/// Seal `plaintext` under `dek` with a DETERMINISTIC nonce (a keyed BLAKE3 of the
/// plaintext). Same `(dek, plaintext)` → same ciphertext → same CID, so
/// content-addressed dedup / structural sharing is preserved (needed for CraftSQL
/// pages). Leaks equality of identical plaintexts under the same key — acceptable
/// for a sole-owner DB. Output format matches `seal` (`nonce || ct`), so `open`
/// decrypts either.
pub fn seal_deterministic(dek: &Dek, plaintext: &[u8]) -> Vec<u8> {
    let mut h = blake3::Hasher::new_keyed(&dek.0);
    h.update(b"craftec/det-nonce/v1");
    h.update(plaintext);
    let mut nonce = [0u8; XNONCE_LEN];
    h.finalize_xof().fill(&mut nonce);
    let cipher = XChaCha20Poly1305::new((&dek.0).into());
    let ct = cipher
        .encrypt(XNonce::from_slice(&nonce), plaintext)
        .expect("xchacha20poly1305 encrypt infallible for valid key");
    let mut out = Vec::with_capacity(XNONCE_LEN + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    out
}

/// Open a `seal`ed blob under `dek`.
pub fn open(dek: &Dek, sealed: &[u8]) -> Result<Vec<u8>> {
    if sealed.len() < XNONCE_LEN {
        return Err(CipherError::Malformed("sealed blob shorter than nonce"));
    }
    let (nonce, ct) = sealed.split_at(XNONCE_LEN);
    let cipher = XChaCha20Poly1305::new((&dek.0).into());
    cipher
        .decrypt(XNonce::from_slice(nonce), ct)
        .map_err(|_| CipherError::Decrypt)
}

// ─────────────────────────── Encryption keypair (PRE-native) ───────────────────

/// The owner's encryption keypair (Umbral / PRE). Distinct from the Ed25519
/// signing identity; both derive from the one identity seed.
pub struct EncKeypair {
    sk: SecretKey,
}

impl EncKeypair {
    /// A fresh random keypair (tests / ephemeral).
    pub fn generate() -> Self {
        Self {
            sk: SecretKey::random(),
        }
    }

    /// Deterministically derive the identity's encryption keypair from its 32-byte
    /// signing seed (domain-separated). Same identity → same encryption key.
    pub fn from_identity_seed(seed: &[u8; 32]) -> Self {
        let n = SecretKeyFactory::seed_size();
        let mut buf = vec![0u8; n];
        let mut h = blake3::Hasher::new_derive_key("craftec/enc/pre/v1");
        h.update(seed);
        h.finalize_xof().fill(&mut buf);
        let factory =
            SecretKeyFactory::from_secure_randomness(&buf).expect("derived seed size matches");
        Self {
            sk: factory.make_key(b"craftec/enc/identity/v1"),
        }
    }

    pub fn public(&self) -> EncPublicKey {
        EncPublicKey(self.sk.public_key())
    }
}

/// An owner's PRE public key — capsules are encapsulated under it.
#[derive(Clone)]
pub struct EncPublicKey(PublicKey);

impl EncPublicKey {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_compressed_bytes().to_vec()
    }
    pub fn from_bytes(b: &[u8]) -> Result<Self> {
        PublicKey::try_from_compressed_bytes(b)
            .map(EncPublicKey)
            .map_err(|_| CipherError::Malformed("public key"))
    }
}

// ─────────────────────────── Capsule + sealed object ───────────────────────────

/// A DEK encapsulated under an owner's PRE public key. Serializable for the
/// on-wire envelope. Destroying this = crypto-shred (the random DEK is gone).
#[derive(Clone, Serialize, Deserialize)]
pub struct DekCapsule {
    capsule: Capsule, // Umbral capsule (serde-serializable)
    enc_dek: Vec<u8>, // Umbral DEM ciphertext of the DEK bytes
}

/// Encapsulate a DEK under `pk` (owner as sole recipient).
pub fn encapsulate(pk: &EncPublicKey, dek: &Dek) -> DekCapsule {
    let (capsule, enc_dek) =
        umbral_encrypt(&pk.0, dek.as_bytes()).expect("umbral encrypt of 32B DEK");
    DekCapsule {
        capsule,
        enc_dek: enc_dek.to_vec(),
    }
}

/// Open a capsule with the owner's keypair → the DEK. Fails if shredded/tampered
/// or wrong key.
pub fn open_capsule(kp: &EncKeypair, cap: &DekCapsule) -> Result<Dek> {
    let dek_bytes =
        decrypt_original(&kp.sk, &cap.capsule, &cap.enc_dek).map_err(|_| CipherError::Decrypt)?;
    let arr: [u8; DEK_LEN] = dek_bytes[..]
        .try_into()
        .map_err(|_| CipherError::Malformed("dek length"))?;
    Ok(Dek::from_bytes(arr))
}

/// A fully sealed object: the encapsulated key + the AEAD ciphertext. The network
/// stores/addresses `ciphertext` (`CID = BLAKE3(ciphertext)`); only a key holder
/// recovers the plaintext.
#[derive(Clone, Serialize, Deserialize)]
pub struct SealedObject {
    pub capsule: DekCapsule,
    pub ciphertext: Vec<u8>,
}

/// Encrypt `plaintext` for `pk`: random DEK → seal bulk → encapsulate DEK.
pub fn encrypt(pk: &EncPublicKey, plaintext: &[u8]) -> SealedObject {
    let dek = Dek::generate();
    SealedObject {
        ciphertext: seal(&dek, plaintext),
        capsule: encapsulate(pk, &dek),
    }
}

/// Decrypt a sealed object with the owner's keypair.
pub fn decrypt_self(kp: &EncKeypair, obj: &SealedObject) -> Result<Vec<u8>> {
    let dek = open_capsule(kp, &obj.capsule)?;
    open(&dek, &obj.ciphertext)
}

// ─────────────────────────── Sharing — proxy re-encryption (K3) ─────────────────
//
// Umbral is a THRESHOLD PRE. To grant a recipient, the owner issues M-of-N re-encryption key
// fragments (`ReKeyFrag`) to proxy nodes; each proxy transforms the owner's capsule into a
// `ReCapsuleFrag` WITHOUT learning the DEK; the recipient collects `threshold` cfrags and recovers
// the DEK with its OWN key. Additive — no change to `SealedObject`/`DekCapsule`. The threshold
// secret-sharing is intrinsic to Umbral (no separate K4 primitive needed). Revoke = stop
// re-encrypting / rotate the object's DEK.

/// A re-encryption key fragment (one of an M-of-N grant). Serializable — it travels to a proxy
/// node. Carries no plaintext and no DEK; useless without the recipient's secret key.
#[derive(Clone, Serialize, Deserialize)]
pub struct ReKeyFrag(KeyFrag);

/// A re-encrypted capsule fragment produced by one proxy. Serializable — it travels to the
/// recipient, who needs `threshold` of them.
#[derive(Clone, Serialize, Deserialize)]
pub struct ReCapsuleFrag(CapsuleFrag);

/// Owner GRANTS access to `recipient_pk`: generate `shares` re-encryption key fragments, any
/// `threshold` of which let the recipient recover the DEK. The owner's PRE key both delegates and
/// signs the fragments (so proxies/recipients can verify the grant's origin).
pub fn grant(
    owner: &EncKeypair,
    recipient_pk: &EncPublicKey,
    threshold: usize,
    shares: usize,
) -> Vec<ReKeyFrag> {
    let signer = Signer::new(owner.sk.clone());
    generate_kfrags(
        &owner.sk,
        &recipient_pk.0,
        &signer,
        threshold,
        shares,
        true, // sign delegating key → verifiable against the owner
        true, // sign receiving key → verifiable against the recipient
    )
    .iter()
    .map(|vkf| ReKeyFrag(vkf.clone().unverify()))
    .collect()
}

/// A PROXY re-encrypts `obj`'s capsule with one `kfrag` → a cfrag. Verifies the kfrag originated
/// from `owner_pk` for `recipient_pk` first; learns no plaintext.
pub fn reencrypt(
    owner_pk: &EncPublicKey,
    recipient_pk: &EncPublicKey,
    obj: &SealedObject,
    kfrag: &ReKeyFrag,
) -> Result<ReCapsuleFrag> {
    let verified = kfrag
        .0
        .clone()
        .verify(&owner_pk.0, Some(&owner_pk.0), Some(&recipient_pk.0))
        .map_err(|_| CipherError::Malformed("kfrag verify"))?;
    let vcf = umbral_reencrypt(&obj.capsule.capsule, verified);
    Ok(ReCapsuleFrag(vcf.unverify()))
}

/// The RECIPIENT combines `threshold` cfrags with its own key → the DEK → plaintext. Fails if
/// fewer than `threshold` valid cfrags, if a cfrag is forged, or if this isn't the granted recipient.
pub fn decrypt_granted(
    recipient: &EncKeypair,
    owner_pk: &EncPublicKey,
    obj: &SealedObject,
    cfrags: &[ReCapsuleFrag],
) -> Result<Vec<u8>> {
    let recipient_pk = recipient.public();
    let mut verified = Vec::with_capacity(cfrags.len());
    for cf in cfrags {
        let v = cf
            .0
            .clone()
            .verify(
                &obj.capsule.capsule,
                &owner_pk.0,     // verifying key (the grant's signer)
                &owner_pk.0,     // delegating key
                &recipient_pk.0, // receiving key
            )
            .map_err(|_| CipherError::Decrypt)?;
        verified.push(v);
    }
    let dek_bytes =
        decrypt_reencrypted(&recipient.sk, &owner_pk.0, &obj.capsule.capsule, verified, &obj.capsule.enc_dek)
            .map_err(|_| CipherError::Decrypt)?;
    let arr: [u8; DEK_LEN] = dek_bytes[..]
        .try_into()
        .map_err(|_| CipherError::Malformed("dek length"))?;
    open(&Dek::from_bytes(arr), &obj.ciphertext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bulk_seal_open_roundtrips() {
        let dek = Dek::generate();
        let msg = b"the drive is private";
        let sealed = seal(&dek, msg);
        assert_ne!(&sealed[XNONCE_LEN..], msg);
        assert_eq!(open(&dek, &sealed).unwrap(), msg);
        assert!(matches!(
            open(&Dek::generate(), &sealed),
            Err(CipherError::Decrypt)
        ));
    }

    #[test]
    fn deterministic_seal_is_stable_and_decrypts() {
        let dek = Dek::generate();
        let page = b"a craftsql page of rows";
        let a = seal_deterministic(&dek, page);
        let b = seal_deterministic(&dek, page);
        assert_eq!(
            a, b,
            "same (key,plaintext) → same ciphertext (structural sharing)"
        );
        assert_eq!(open(&dek, &a).unwrap(), page);
        // Different plaintext → different ciphertext (no nonce reuse).
        assert_ne!(seal_deterministic(&dek, b"other page"), a);
    }

    #[test]
    fn bulk_tamper_fails_closed() {
        let dek = Dek::generate();
        let mut sealed = seal(&dek, b"secret");
        let last = sealed.len() - 1;
        sealed[last] ^= 0xff;
        assert!(matches!(open(&dek, &sealed), Err(CipherError::Decrypt)));
    }

    #[test]
    fn identity_seed_derivation_is_deterministic() {
        let seed = [7u8; 32];
        let a = EncKeypair::from_identity_seed(&seed).public().to_bytes();
        let b = EncKeypair::from_identity_seed(&seed).public().to_bytes();
        assert_eq!(a, b, "same seed → same encryption key");
        let c = EncKeypair::from_identity_seed(&[8u8; 32])
            .public()
            .to_bytes();
        assert_ne!(a, c, "different seed → different key");
    }

    #[test]
    fn encrypt_decrypt_roundtrips_and_is_sole_owner() {
        let owner = EncKeypair::from_identity_seed(&[1u8; 32]);
        let msg = b"private file contents";
        let obj = encrypt(&owner.public(), msg);
        // Network sees only ciphertext.
        assert_ne!(obj.ciphertext, msg);
        // Owner reads it back.
        assert_eq!(decrypt_self(&owner, &obj).unwrap(), msg);
        // A different identity CANNOT (sole-owner CID / no shared key).
        let other = EncKeypair::from_identity_seed(&[2u8; 32]);
        assert!(matches!(
            decrypt_self(&other, &obj),
            Err(CipherError::Decrypt)
        ));
    }

    #[test]
    fn grant_lets_a_recipient_decrypt_via_threshold_reencryption() {
        let owner = EncKeypair::from_identity_seed(&[1u8; 32]);
        let bob = EncKeypair::from_identity_seed(&[2u8; 32]);
        let msg = b"shared with bob, proxies never see it";
        let obj = encrypt(&owner.public(), msg);

        // Owner grants 2-of-3 to Bob (M-of-N kfrags — the threshold is intrinsic to Umbral).
        let kfrags = grant(&owner, &bob.public(), 2, 3);
        assert_eq!(kfrags.len(), 3);

        // Two proxies re-encrypt (each learns no plaintext).
        let cfrags: Vec<ReCapsuleFrag> = kfrags
            .iter()
            .take(2)
            .map(|kf| reencrypt(&owner.public(), &bob.public(), &obj, kf).unwrap())
            .collect();

        // Bob recovers the plaintext with his own key + the threshold of cfrags.
        assert_eq!(
            decrypt_granted(&bob, &owner.public(), &obj, &cfrags).unwrap(),
            msg
        );

        // Below threshold (1 of 2 needed) → fails.
        assert!(decrypt_granted(&bob, &owner.public(), &obj, &cfrags[..1]).is_err());

        // A non-recipient can't decrypt even holding the cfrags (they were made for Bob's key).
        let carol = EncKeypair::from_identity_seed(&[9u8; 32]);
        assert!(decrypt_granted(&carol, &owner.public(), &obj, &cfrags).is_err());

        // The owner still self-decrypts (grant is additive, not a handover).
        assert_eq!(decrypt_self(&owner, &obj).unwrap(), msg);
    }

    #[test]
    fn kfrags_and_cfrags_roundtrip_over_the_wire() {
        let owner = EncKeypair::from_identity_seed(&[4u8; 32]);
        let bob = EncKeypair::from_identity_seed(&[5u8; 32]);
        let obj = encrypt(&owner.public(), b"wire-serialized grant");

        // A kfrag serializes (owner → proxy).
        let kfrag = &grant(&owner, &bob.public(), 2, 2)[0];
        let kf_bytes = postcard::to_allocvec(kfrag).unwrap();
        let kfrag2: ReKeyFrag = postcard::from_bytes(&kf_bytes).unwrap();

        // A cfrag serializes (proxy → recipient).
        let cfrag = reencrypt(&owner.public(), &bob.public(), &obj, &kfrag2).unwrap();
        let cf_bytes = postcard::to_allocvec(&cfrag).unwrap();
        let _cfrag2: ReCapsuleFrag = postcard::from_bytes(&cf_bytes).unwrap();
        assert!(!kf_bytes.is_empty() && !cf_bytes.is_empty());
    }

    #[test]
    fn crypto_shred_makes_it_unrecoverable() {
        let owner = EncKeypair::from_identity_seed(&[3u8; 32]);
        let mut shredded = encrypt(&owner.public(), b"delete me for real");
        assert!(
            decrypt_self(&owner, &shredded).is_ok(),
            "readable before shred"
        );
        // Shred = destroy the wrapped DEK; the ciphertext persists but is useless.
        shredded.capsule.enc_dek.iter_mut().for_each(|b| *b = 0);
        assert!(
            decrypt_self(&owner, &shredded).is_err(),
            "wrapped DEK destroyed → unrecoverable"
        );
    }
}
