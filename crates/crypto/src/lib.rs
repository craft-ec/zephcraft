//! Node identity: Ed25519 keypair and on-disk keystore.
//!
//! Spec: foundation §7 (primitives), §11 (first-run initialization),
//! §17 (node identity). The NodeId IS the Ed25519 public key (iroh
//! convention — no hashing). The signing key is zeroized on drop
//! (ed25519-dalek `zeroize` feature); loaded key buffers are zeroized
//! after use.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use zeph_core::NodeId;
use zeroize::Zeroize;

/// File names inside the keystore directory (foundation §11).
const KEY_FILE: &str = "node.key"; // raw 32-byte Ed25519 private key, mode 0600
const PUB_FILE: &str = "node.pub"; // raw 32-byte Ed25519 public key
const ID_FILE: &str = "node_id"; // hex NodeId, for humans and scripts

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("keystore io: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid key material in {0}: expected exactly 32 bytes")]
    InvalidKey(PathBuf),
    #[error("keystore corruption: stored public key does not match key derived from private key")]
    PubkeyMismatch,
    #[error("insecure permissions on {0}: private key must be 0600")]
    InsecurePermissions(PathBuf),
}

pub type Result<T> = std::result::Result<T, CryptoError>;

/// A node's Ed25519 identity. Sign latency ~7µs, verify ~2.3µs (foundation §17).
pub struct NodeIdentity {
    signing: SigningKey,
}

impl NodeIdentity {
    /// Generate a fresh identity from the OS CSPRNG.
    pub fn generate() -> Self {
        Self {
            signing: SigningKey::generate(&mut rand::rngs::OsRng),
        }
    }

    /// The NodeId is the Ed25519 public key, verbatim.
    pub fn node_id(&self) -> NodeId {
        NodeId(self.signing.verifying_key().to_bytes())
    }

    /// Sign a message; returns the 64-byte Ed25519 signature.
    pub fn sign(&self, msg: &[u8]) -> [u8; 64] {
        self.signing.sign(msg).to_bytes()
    }

    /// Raw private key bytes, for handing the SAME identity to the transport
    /// (iroh secret key) so NodeId == transport EndpointId. Sensitive: caller
    /// must zeroize its copy as soon as the consumer has taken ownership.
    pub fn secret_key_bytes(&self) -> [u8; 32] {
        self.signing.to_bytes()
    }

    /// Verify a signature against a NodeId (public key). Uses `verify_strict`
    /// to reject malleable/mixed-order signatures.
    pub fn verify(node_id: &NodeId, msg: &[u8], sig: &[u8; 64]) -> bool {
        let Ok(vk) = VerifyingKey::from_bytes(&node_id.0) else {
            return false;
        };
        vk.verify_strict(msg, &Signature::from_bytes(sig)).is_ok()
    }

    fn from_private_bytes(bytes: &[u8; 32]) -> Self {
        Self {
            signing: SigningKey::from_bytes(bytes),
        }
    }
}

/// On-disk keystore holding one node identity (foundation §11, §17).
///
/// Layout: `<dir>/node.key` (0600), `<dir>/node.pub`, `<dir>/node_id` (hex).
/// Writes are atomic (temp file + fsync + rename). `node.key` is written
/// LAST so a crash mid-init leaves no private key behind a missing pub —
/// load treats a present key as authoritative and re-derives/verifies.
pub struct Keystore {
    dir: PathBuf,
}

impl Keystore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// First run: generate + persist a new identity. Subsequent runs: load
    /// and verify the existing one. Idempotent (foundation §11).
    pub fn init_or_load(&self) -> Result<NodeIdentity> {
        if self.dir.join(KEY_FILE).exists() {
            self.load()
        } else {
            let identity = NodeIdentity::generate();
            self.save(&identity)?;
            Ok(identity)
        }
    }

    /// Load the identity: enforce 0600 on the private key, re-derive the
    /// public key, and compare against the stored one — mismatch indicates
    /// corruption (foundation §17 step 2).
    pub fn load(&self) -> Result<NodeIdentity> {
        let key_path = self.dir.join(KEY_FILE);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&key_path)?.permissions().mode();
            if mode & 0o077 != 0 {
                return Err(CryptoError::InsecurePermissions(key_path));
            }
        }

        let mut key_bytes = read_exact_32(&key_path)?;
        let identity = NodeIdentity::from_private_bytes(&key_bytes);
        key_bytes.zeroize();

        let stored_pub = read_exact_32(&self.dir.join(PUB_FILE))?;
        if stored_pub != identity.node_id().0 {
            return Err(CryptoError::PubkeyMismatch);
        }

        Ok(identity)
    }

    fn save(&self, identity: &NodeIdentity) -> Result<()> {
        fs::create_dir_all(&self.dir)?;
        let node_id = identity.node_id();

        write_atomic(&self.dir.join(PUB_FILE), &node_id.0, 0o644)?;
        write_atomic(&self.dir.join(ID_FILE), node_id.to_hex().as_bytes(), 0o644)?;

        let mut key_bytes = identity.signing.to_bytes();
        let result = write_atomic(&self.dir.join(KEY_FILE), &key_bytes, 0o600);
        key_bytes.zeroize();
        result
    }
}

fn read_exact_32(path: &Path) -> Result<[u8; 32]> {
    let bytes = fs::read(path)?;
    let arr: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| CryptoError::InvalidKey(path.to_path_buf()))?;
    Ok(arr)
}

/// Atomic write: temp file in the same directory, restrictive permissions
/// set before the payload is written, fsync, then rename over the target.
fn write_atomic(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let dir = path.parent().expect("keystore paths always have a parent");
    let tmp = dir.join(format!(
        ".{}.tmp",
        path.file_name().expect("file name").to_string_lossy()
    ));

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    let _ = mode;

    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_verify_roundtrip() {
        let id = NodeIdentity::generate();
        let sig = id.sign(b"hello zeph");
        assert!(NodeIdentity::verify(&id.node_id(), b"hello zeph", &sig));
    }

    #[test]
    fn verify_rejects_tampered_message_and_wrong_key() {
        let id = NodeIdentity::generate();
        let sig = id.sign(b"hello zeph");
        assert!(!NodeIdentity::verify(&id.node_id(), b"hello zeph!", &sig));
        let other = NodeIdentity::generate();
        assert!(!NodeIdentity::verify(&other.node_id(), b"hello zeph", &sig));
    }

    #[test]
    fn first_run_creates_second_run_reuses_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = Keystore::new(dir.path().join("keys"));
        let first = store.init_or_load().unwrap();
        let second = store.init_or_load().unwrap();
        assert_eq!(first.node_id(), second.node_id());
        // A signature from run 1 verifies against run 2's identity.
        let sig = first.sign(b"persistent identity");
        assert!(NodeIdentity::verify(
            &second.node_id(),
            b"persistent identity",
            &sig
        ));
    }

    #[cfg(unix)]
    #[test]
    fn private_key_file_is_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let store = Keystore::new(dir.path());
        store.init_or_load().unwrap();
        let mode = fs::metadata(dir.path().join(KEY_FILE))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn load_rejects_insecure_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let store = Keystore::new(dir.path());
        store.init_or_load().unwrap();
        let key_path = dir.path().join(KEY_FILE);
        fs::set_permissions(&key_path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            store.load(),
            Err(CryptoError::InsecurePermissions(_))
        ));
    }

    #[test]
    fn load_detects_pubkey_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let store = Keystore::new(dir.path());
        store.init_or_load().unwrap();
        fs::write(dir.path().join(PUB_FILE), [0u8; 32]).unwrap();
        assert!(matches!(store.load(), Err(CryptoError::PubkeyMismatch)));
    }

    #[test]
    fn node_id_file_matches_hex() {
        let dir = tempfile::tempdir().unwrap();
        let store = Keystore::new(dir.path());
        let id = store.init_or_load().unwrap();
        let written = fs::read_to_string(dir.path().join(ID_FILE)).unwrap();
        assert_eq!(written, id.node_id().to_hex());
    }
}
