//! Sync→async page-fetch bridge for lazy reads.
//!
//! The SQLite VFS reads pages *synchronously*; fetching a missing page from the
//! network is *async*. A `Fetcher` bridges the two: the sync read hands a CID to
//! a background async task over an unbounded channel and blocks on a std channel
//! for the bytes. This lets a reader open a DB by syncing only its (tiny) index
//! and pull page contents on demand — not the whole database up front.

use std::sync::mpsc;

use tokio::sync::mpsc as tmpsc;
use zeph_core::Cid;

/// A request: fetch this CID, reply on the std channel.
pub type FetchRequest = (Cid, mpsc::Sender<Option<Vec<u8>>>);

/// Handle used by the synchronous VFS to fetch a page's bytes on demand.
#[derive(Clone)]
pub struct Fetcher {
    tx: tmpsc::UnboundedSender<FetchRequest>,
}

impl Fetcher {
    pub fn new(tx: tmpsc::UnboundedSender<FetchRequest>) -> Self {
        Self { tx }
    }

    /// Fetch `cid`, blocking the calling (sync) thread until the background task
    /// returns the bytes. `None` if the request or fetch fails. Requires the
    /// background task to run on another thread (multi-threaded runtime).
    pub fn fetch(&self, cid: Cid) -> Option<Vec<u8>> {
        let (rtx, rrx) = mpsc::channel();
        self.tx.send((cid, rtx)).ok()?;
        rrx.recv().ok().flatten()
    }
}
