# ZephCraft

**The Craftec node — a self-healing, content-addressed storage network.**

> Spare disk on every device, pooled into one network that replicates only what matters.

ZephCraft lets you publish files and folders by name, retrieve them anywhere by content ID, and lets the network keep alive what's actually wanted — erasure-coded, self-repairing through churn, with no central server and no manual pinning.

## What it is

- **Content-addressed** — `Cid = BLAKE3(bytes)`; identical bytes dedup regardless of name.
- **Erasure-coded (RLNC over GF(2⁸))** — any *k* of *n* coded pieces reconstruct; pieces are fungible; repair mints fresh ones.
- **Self-healing lifecycle** — Repair, Distribution, Scaling, Degradation, Fade, Eviction: replication tracks *real demand*, not manual pins.
- **Signed everything** — Ed25519 identities; content providers, metadata envelopes (`KIND_META`), and single-writer DB roots (`KIND_ROOT`) are all signed records.
- **Manifests** — files and folders published and restored *by name*; editable, dedup-safe metadata (BitTorrent-envelope style).

Transport is [iroh](https://iroh.computer) (QUIC, NAT traversal, relay fallback); hashing is BLAKE3; wire format is postcard. Pollution is rejected at ingest via public null-space verification tags.

## Status

**Early.** A working storage core — proven live on a real network and under simulated churn (see the DST harness) — with the distributed database layer (CraftSQL, single-writer-per-identity over CraftOBJ) as the next milestone. Not yet at scale.

## Design docs

- [`docs/craftec_technical_foundation.md`](docs/craftec_technical_foundation.md) — the source of truth.
- [`docs/CRAFTOBJ_DESIGN.md`](docs/CRAFTOBJ_DESIGN.md) — the storage layer design + decisions.
- [`docs/ZEPH_POSITIONING.md`](docs/ZEPH_POSITIONING.md) — what it is, and what it isn't (vs BitTorrent / cloud / blockchain).

## Build & test

```sh
cargo build --workspace
cargo test  --workspace
```

## Run a node

```sh
cargo run -p zeph-noded -- --data-dir ~/.zeph        # start the daemon
zeph --data-dir ~/.zeph publish <file>               # publish → prints a CID
zeph --data-dir ~/.zeph get <cid> -o <path>          # fetch it back, by name
zeph --data-dir ~/.zeph status                        # live node status
```

The daemon serves a local dashboard on `127.0.0.1` (token-authed).

## Layout

```
crates/     core · crypto · wire · erasure · transport ·
            membership · routing · store · obj · noded
apps/       tracker — the discovery tracker
webui/      the node dashboard (embedded in the daemon)
website/    the zeph.craft.ec landing page
deploy/     systemd unit + deployment notes
tests/      DST churn harness + multi-node integration
docs/       design docs (the foundation is the source of truth)
```

## License

MIT
