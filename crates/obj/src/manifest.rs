//! Content manifests — names, sizes, folders. A manifest is the immutable,
//! content-addressed description of a file or directory (the git-tree / IPFS
//! UnixFS / BitTorrent-`.torrent` analog): it wraps raw content CIDs with human
//! metadata. Sharing a manifest CID conveys the name/size/structure PLUS the
//! content CIDs — while the CID itself stays `BLAKE3(bytes)`, so identical
//! bytes still dedup regardless of name. CraftVFS (later) sits on top of this.

use serde::{Deserialize, Serialize};

/// Magic prefix distinguishing a manifest object from raw content bytes, so
/// `get` can tell "restore this file/tree by name" from "hand back raw bytes".
pub const MANIFEST_MAGIC: &[u8] = b"ZMANIFS1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Manifest {
    /// A single file: its bytes live at `content` (a CraftOBJ object).
    File {
        name: String,
        size: u64,
        mime: String,
        content: [u8; 32],
    },
    /// A directory: `entries` point to child MANIFEST CIDs (files or subdirs).
    Dir { name: String, entries: Vec<Entry> },
}

/// One directory entry — points to a child manifest (itself a File or Dir).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub name: String,
    pub size: u64,
    pub is_dir: bool,
    /// CID of the child manifest.
    pub cid: [u8; 32],
}

impl Manifest {
    pub fn name(&self) -> &str {
        match self {
            Manifest::File { name, .. } | Manifest::Dir { name, .. } => name,
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Manifest::File { size, .. } => *size,
            Manifest::Dir { entries, .. } => entries.iter().map(|e| e.size).sum(),
        }
    }

    pub fn is_dir(&self) -> bool {
        matches!(self, Manifest::Dir { .. })
    }

    /// Serialize with the magic prefix — the bytes that get published as an
    /// object (the manifest CID is `BLAKE3` of these).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = MANIFEST_MAGIC.to_vec();
        out.extend_from_slice(&postcard::to_allocvec(self).expect("manifest serializes"));
        out
    }

    /// Decode from fetched bytes iff they carry the magic prefix (else the CID
    /// pointed at raw content, not a manifest).
    pub fn decode(bytes: &[u8]) -> Option<Manifest> {
        let rest = bytes.strip_prefix(MANIFEST_MAGIC)?;
        postcard::from_bytes(rest).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_manifest_round_trips_and_is_detectable() {
        let m = Manifest::File {
            name: "photo.jpg".into(),
            size: 2048,
            mime: "image/jpeg".into(),
            content: [7u8; 32],
        };
        let bytes = m.encode();
        assert_eq!(Manifest::decode(&bytes), Some(m.clone()));
        assert_eq!(m.name(), "photo.jpg");
        assert_eq!(m.size(), 2048);
        // Raw content (no magic) is not mistaken for a manifest.
        assert!(Manifest::decode(b"just some file bytes").is_none());
        assert!(Manifest::decode(&[0u8; 64]).is_none());
    }

    #[test]
    fn dir_size_sums_entries() {
        let d = Manifest::Dir {
            name: "album".into(),
            entries: vec![
                Entry {
                    name: "a".into(),
                    size: 10,
                    is_dir: false,
                    cid: [1; 32],
                },
                Entry {
                    name: "b".into(),
                    size: 20,
                    is_dir: false,
                    cid: [2; 32],
                },
            ],
        };
        assert_eq!(d.size(), 30);
        assert!(d.is_dir());
    }
}
