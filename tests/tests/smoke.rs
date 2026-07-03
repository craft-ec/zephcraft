//! Workspace smoke test — proves the crate graph wires together.

use zeph_core::Cid;

#[test]
fn content_addressing_smoke() {
    let data = b"store bytes. retrieve bytes. keep them alive.";
    let cid = Cid::of(data);
    assert!(cid.verifies(data));
    assert_eq!(cid.to_hex().len(), 64);
}
