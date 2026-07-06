//! Signed records and the local record store.
//!
//! The DHT is a generic *signed* key-value store. A [`StoredRecord`] binds an opaque `value`
//! (whatever the `ContentRouting` mapping puts there — a provider record, a want, a head
//! pointer) to a DHT `key` and a `publisher`, signed by that publisher's Ed25519 key. Every
//! node verifies the signature on store AND on return, so no node can forge or tamper with
//! another's record. Per `(key, publisher)` the **highest `seq` wins** (republishes advance
//! freshness; owner-keyed heads advance version) — but records from *different* publishers
//! under the same key **coexist**, which is exactly the "many small provider records per
//! CID" model (foundation §CraftOBJ).

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use zeph_core::NodeId;
use zeph_crypto::NodeIdentity;

/// A signed key-value record living on the K nodes closest to `key`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredRecord {
    pub key: [u8; 32],
    pub publisher: [u8; 32],
    pub seq: u64,
    pub value: Vec<u8>,
    pub sig: Vec<u8>,
}

impl StoredRecord {
    fn signing_bytes(key: &[u8; 32], publisher: &[u8; 32], seq: u64, value: &[u8]) -> Vec<u8> {
        let mut b = Vec::with_capacity(32 + 32 + 8 + value.len());
        b.extend_from_slice(key);
        b.extend_from_slice(publisher);
        b.extend_from_slice(&seq.to_be_bytes());
        b.extend_from_slice(value);
        b
    }

    /// Build + sign a record under `identity` (the publisher).
    pub fn sign(identity: &NodeIdentity, key: [u8; 32], seq: u64, value: Vec<u8>) -> Self {
        let publisher = identity.node_id().0;
        let sig = identity
            .sign(&Self::signing_bytes(&key, &publisher, seq, &value))
            .to_vec();
        Self {
            key,
            publisher,
            seq,
            value,
            sig,
        }
    }

    /// True iff the signature is valid for the claimed publisher over its own fields.
    pub fn verify(&self) -> bool {
        let Ok(sig): std::result::Result<[u8; 64], _> = self.sig.as_slice().try_into() else {
            return false;
        };
        NodeIdentity::verify(
            &NodeId(self.publisher),
            &Self::signing_bytes(&self.key, &self.publisher, self.seq, &self.value),
            &sig,
        )
    }
}

/// A record with its expiry (wall-clock millis).
type Expiring = (StoredRecord, u64);
/// Records under one key, one per publisher.
type PerKey = HashMap<[u8; 32], Expiring>;

/// Local store of records this node is responsible for (it is among the K closest to their
/// keys) plus its own published records. TTL-expired; time is supplied by the caller (the
/// node's clock) so it stays testable.
pub struct RecordStore {
    inner: Mutex<HashMap<[u8; 32], PerKey>>,
    ttl_millis: u64,
}

impl RecordStore {
    pub fn new(ttl_millis: u64) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            ttl_millis,
        }
    }

    /// Store a record. Rejects a bad signature or a stale/equal seq for the same
    /// `(key, publisher)`. Returns true iff it was accepted (new or advancing).
    pub fn put(&self, rec: StoredRecord, now_millis: u64) -> bool {
        if !rec.verify() {
            return false;
        }
        let mut inner = self.inner.lock().expect("record store");
        let per_key = inner.entry(rec.key).or_default();
        if let Some((existing, _)) = per_key.get(&rec.publisher) {
            if existing.seq >= rec.seq {
                return false; // stale or replayed
            }
        }
        let expires = now_millis.saturating_add(self.ttl_millis);
        per_key.insert(rec.publisher, (rec, expires));
        true
    }

    /// All non-expired records under `key` (one per publisher).
    pub fn get(&self, key: &[u8; 32], now_millis: u64) -> Vec<StoredRecord> {
        let inner = self.inner.lock().expect("record store");
        inner
            .get(key)
            .map(|per_key| {
                per_key
                    .values()
                    .filter(|(_, exp)| *exp > now_millis)
                    .map(|(r, _)| r.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Drop expired records; returns how many were removed. Called periodically.
    pub fn expire(&self, now_millis: u64) -> usize {
        let mut inner = self.inner.lock().expect("record store");
        let mut removed = 0;
        inner.retain(|_, per_key| {
            let before = per_key.len();
            per_key.retain(|_, (_, exp)| *exp > now_millis);
            removed += before - per_key.len();
            !per_key.is_empty()
        });
        removed
    }

    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("record store")
            .values()
            .map(|m| m.len())
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot every stored record (with its expiry) to `path`, written atomically via a temp
    /// file + rename so a crash mid-write can't corrupt the live snapshot. Called periodically
    /// and on shutdown so a node restart comes back with its store intact instead of empty.
    pub fn save(&self, path: &std::path::Path) -> std::io::Result<usize> {
        let records: Vec<PersistedRecord> = {
            let inner = self.inner.lock().expect("record store");
            inner
                .values()
                .flat_map(|per_key| per_key.values())
                .map(|(rec, exp)| PersistedRecord {
                    rec: rec.clone(),
                    expires: *exp,
                })
                .collect()
        };
        let bytes = postcard::to_allocvec(&records)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, path)?;
        Ok(records.len())
    }

    /// Populate this store from a snapshot at `path`, DROPPING already-expired records and
    /// RE-VERIFYING every signature — a tampered or truncated on-disk file can inject nothing.
    /// A missing or corrupt file leaves the store unchanged. Returns how many records loaded.
    pub fn load_from(&self, path: &std::path::Path, now_millis: u64) -> usize {
        let Ok(bytes) = std::fs::read(path) else {
            return 0; // no snapshot yet
        };
        let Ok(records) = postcard::from_bytes::<Vec<PersistedRecord>>(&bytes) else {
            return 0; // corrupt — start clean rather than trusting garbage
        };
        let mut inner = self.inner.lock().expect("record store");
        let mut loaded = 0;
        for PersistedRecord { rec, expires } in records {
            if expires <= now_millis {
                continue; // already expired — do not resurrect
            }
            if !rec.verify() {
                continue; // bad signature — reject
            }
            let per_key = inner.entry(rec.key).or_default();
            match per_key.get(&rec.publisher) {
                Some((existing, _)) if existing.seq >= rec.seq => {} // keep the fresher copy
                _ => {
                    per_key.insert(rec.publisher, (rec, expires));
                    loaded += 1;
                }
            }
        }
        loaded
    }
}

/// A record plus its expiry (wall-clock millis), the on-disk persistence unit.
#[derive(Serialize, Deserialize)]
struct PersistedRecord {
    rec: StoredRecord,
    expires: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> NodeIdentity {
        NodeIdentity::generate()
    }

    #[test]
    fn signed_record_verifies_and_detects_tampering() {
        let id = identity();
        let rec = StoredRecord::sign(&id, [1u8; 32], 1, b"hello".to_vec());
        assert!(rec.verify());
        let mut tampered = rec.clone();
        tampered.value = b"world".to_vec();
        assert!(!tampered.verify(), "tampered value must fail verification");
        let mut wrong_key = rec.clone();
        wrong_key.publisher = [9u8; 32];
        assert!(
            !wrong_key.verify(),
            "wrong publisher must fail verification"
        );
    }

    #[test]
    fn put_rejects_forgery_and_stale_seq() {
        let store = RecordStore::new(1000);
        let id = identity();
        let key = [2u8; 32];
        assert!(store.put(StoredRecord::sign(&id, key, 5, b"v5".to_vec()), 0));
        // stale seq for the same publisher → rejected
        assert!(!store.put(StoredRecord::sign(&id, key, 4, b"v4".to_vec()), 0));
        // equal seq → rejected (idempotent, no advance)
        assert!(!store.put(StoredRecord::sign(&id, key, 5, b"v5b".to_vec()), 0));
        // advancing seq → accepted
        assert!(store.put(StoredRecord::sign(&id, key, 6, b"v6".to_vec()), 0));
        // a forged record (bad sig) → rejected
        let mut forged = StoredRecord::sign(&id, key, 99, b"x".to_vec());
        forged.sig = vec![0u8; 64];
        assert!(!store.put(forged, 0));
        assert_eq!(store.get(&key, 0).len(), 1);
        assert_eq!(store.get(&key, 0)[0].seq, 6);
    }

    #[test]
    fn many_publishers_coexist_under_one_key() {
        let store = RecordStore::new(1000);
        let key = [3u8; 32];
        for _ in 0..4 {
            let id = identity();
            assert!(store.put(StoredRecord::sign(&id, key, 1, b"here".to_vec()), 0));
        }
        assert_eq!(store.get(&key, 0).len(), 4, "one record per provider");
    }

    #[test]
    fn persist_roundtrips_dropping_expired_and_tampered() {
        let path =
            std::env::temp_dir().join(format!("zeph_dht_persist_{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&path);

        // Save a live record (expires at 1000).
        let store = RecordStore::new(1000);
        let id = identity();
        let key = [7u8; 32];
        store.put(StoredRecord::sign(&id, key, 3, b"live".to_vec()), 0);
        assert_eq!(store.save(&path).unwrap(), 1);

        // Load before expiry → survives, seq + value intact.
        let fresh = RecordStore::new(1000);
        assert_eq!(fresh.load_from(&path, 500), 1);
        let got = fresh.get(&key, 500);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].seq, 3);
        assert_eq!(got[0].value, b"live");

        // Load after expiry → dropped (persistence honours the stored TTL, no resurrection).
        let stale = RecordStore::new(1000);
        assert_eq!(stale.load_from(&path, 1500), 0);

        // A tampered on-disk snapshot injects nothing (signature re-verified on load).
        let mut forged = StoredRecord::sign(&id, [9u8; 32], 1, b"ok".to_vec());
        forged.value = b"tampered".to_vec(); // breaks the signature
        let snapshot = vec![PersistedRecord {
            rec: forged,
            expires: 10_000,
        }];
        std::fs::write(&path, postcard::to_allocvec(&snapshot).unwrap()).unwrap();
        let victim = RecordStore::new(1000);
        assert_eq!(victim.load_from(&path, 0), 0, "tampered record rejected");

        // Corrupt/missing file → empty, not a panic.
        std::fs::write(&path, b"not postcard").unwrap();
        assert_eq!(RecordStore::new(1000).load_from(&path, 0), 0);
        std::fs::remove_file(&path).ok();
        assert_eq!(RecordStore::new(1000).load_from(&path, 0), 0);
    }

    #[test]
    fn records_expire_after_ttl() {
        let store = RecordStore::new(100);
        let id = identity();
        let key = [4u8; 32];
        store.put(StoredRecord::sign(&id, key, 1, b"v".to_vec()), 0);
        assert_eq!(store.get(&key, 50).len(), 1);
        assert_eq!(store.get(&key, 150).len(), 0, "expired");
        assert_eq!(store.expire(150), 1);
        assert!(store.is_empty());
    }
}
