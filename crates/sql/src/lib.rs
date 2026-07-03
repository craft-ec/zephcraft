//! CraftSQL storage core — the CID-VFS foundation (foundation §33).
//!
//! A SQLite database becomes 16 KB pages stored as content-addressed CraftOBJ
//! objects. This crate is the storage substance the SQLite VFS sits on: an
//! `ObjectStore` (put/get blobs by CID) and a `Pager` that maps page numbers to
//! object CIDs, buffers writes, and commits to a single immutable ROOT CID — the
//! mutable head `KIND_ROOT` publishes. Reopening from a root CID yields a
//! consistent snapshot (pages immutable; unchanged pages deduplicated).
//!
//! Unit 2 wires SQLite's VFS (xRead/xWrite/xSync) as a thin adapter over `Pager`.

mod db;
mod gen;
mod net;
mod pager;
mod store;
mod vfs;

pub use db::{CraftDb, CraftSql, PageSource, RootStore, RoutingRootStore};
pub use gen::DurableStore;
pub use net::{serve_pages, ObjDurable, TransportPageSource, ALPN as PAGE_ALPN};
pub use pager::{Pager, PAGE_SIZE};
pub use store::ObjectStore;
pub use vfs::{CraftHandle, CraftVfs, Roots};

#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization: {0}")]
    Serde(String),
    #[error("root object {0} not found")]
    RootNotFound(String),
    #[error("corrupt page index: {0}")]
    CorruptIndex(String),
    #[error("sqlite: {0}")]
    Sqlite(String),
    #[error("write conflict — the DB root moved under you (retry)")]
    Conflict,
    #[error("database opened read-only")]
    ReadOnly,
}

pub type Result<T> = std::result::Result<T, SqlError>;
