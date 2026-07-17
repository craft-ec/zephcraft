//! M2.2 GATE: persistent CAS with pin semantics — survives reopen, pins are
//! eviction-exempt and serve pieces on demand, tombstones block resurrection.

use std::collections::HashSet;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use zeph_core::Cid;
use zeph_erasure::{encode_n, vtags, Decoder};
use zeph_store::{Generation, Store};

const K: usize = 8;
const PIECE_LEN: usize = 4096;

fn generation(content: &[u8], rng: &mut StdRng) -> (Generation, Vec<Vec<u8>>) {
    let sources: Vec<Vec<u8>> = content
        .chunks(PIECE_LEN)
        .map(|c| {
            let mut v = c.to_vec();
            v.resize(PIECE_LEN, 0);
            v
        })
        .collect();
    let tags = vtags::generate(&sources, rng).unwrap();
    (
        Generation {
            k: K as u32,
            piece_len: PIECE_LEN as u64,
            total_len: content.len() as u64,
            vtags: postcard::to_allocvec(&tags).unwrap(),
        },
        sources,
    )
}

#[test]
fn pieces_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(1);
    let content: Vec<u8> = (0..K * PIECE_LEN).map(|_| rng.gen()).collect();
    let cid = Cid::of(&content);
    let (gen, sources) = generation(&content, &mut rng);

    {
        let store = Store::open(dir.path()).unwrap();
        store.put_generation(cid, gen.clone()).unwrap();
        for piece in encode_n(&sources, 10, &mut rng).unwrap() {
            store.put_piece(cid, &piece).unwrap();
        }
        assert_eq!(store.piece_count(&cid), 10);
    }
    // Reopen: index rebuilt from disk.
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.piece_count(&cid), 10);
    assert_eq!(store.generation(&cid), Some(gen));

    // Served pieces decode back to the content.
    let served = store.serve_pieces(&cid, &HashSet::new(), K).unwrap();
    let mut decoder = Decoder::new(K, PIECE_LEN);
    for p in &served {
        decoder.add_piece(p).unwrap();
    }
    let decoded: Vec<u8> = decoder.decode().unwrap().into_iter().flatten().collect();
    assert_eq!(&decoded[..content.len()], &content[..]);
}

#[test]
fn pin_survives_reopen_and_serves_pieces_on_demand() {
    let dir = tempfile::tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(2);
    let content: Vec<u8> = (0..K * PIECE_LEN).map(|_| rng.gen()).collect();
    let cid = Cid::of(&content);
    let (gen, _) = generation(&content, &mut rng);

    {
        let store = Store::open(dir.path()).unwrap();
        store.put_generation(cid, gen).unwrap();
        store.pin(cid, &content).unwrap(); // no pieces stored, only content
        assert!(store.is_pinned(&cid));
    }
    let store = Store::open(dir.path()).unwrap();
    assert!(store.is_pinned(&cid), "pin survives reopen");
    assert_eq!(store.content(&cid).unwrap(), content);

    // A pinner with ZERO stored pieces still serves K fresh pieces that decode.
    assert_eq!(store.piece_count(&cid), 0);
    let served = store.serve_pieces(&cid, &HashSet::new(), K).unwrap();
    assert_eq!(served.len(), K);
    let mut decoder = Decoder::new(K, PIECE_LEN);
    for p in &served {
        decoder.add_piece(p).unwrap();
    }
    let decoded: Vec<u8> = decoder.decode().unwrap().into_iter().flatten().collect();
    assert_eq!(&decoded[..content.len()], &content[..]);
}

#[test]
fn mint_from_content_is_independent_even_with_stored_pieces() {
    // Repair correctness (regression guard): a content holder that has ALSO
    // ingested coded pieces must still mint FRESH INDEPENDENT pieces from the
    // whole content. serve_pieces returns STORED pieces first, so minting via
    // it would re-serve the same stored piece every call — repair would push
    // duplicates that add no rank, leaving a below-k cid stuck while the
    // inflated count masks it as recovered. mint_from_content must sidestep
    // that: K minted pieces alone must span the k-dim space and decode.
    let dir = tempfile::tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(7);
    let content: Vec<u8> = (0..K * PIECE_LEN).map(|_| rng.gen()).collect();
    let cid = Cid::of(&content);
    let (gen, sources) = generation(&content, &mut rng);

    let store = Store::open(dir.path()).unwrap();
    store.put_generation(cid, gen).unwrap();
    store.pin(cid, &content).unwrap(); // whole content
                                       // Also ingest a few coded pieces — the exact case the old
                                       // serve_pieces path mishandled (it re-served a stored piece
                                       // instead of a fresh encode).
    for piece in encode_n(&sources, 3, &mut rng).unwrap() {
        store.put_piece(cid, &piece).unwrap();
    }
    assert_eq!(store.piece_count(&cid), 3);

    // Mint K pieces straight from content.
    let minted: Vec<_> = (0..K)
        .map(|_| store.mint_from_content(&cid).expect("content present"))
        .collect();
    // Not collapsed onto one repeated stored piece: coding vectors are distinct.
    let distinct: HashSet<Vec<u8>> = minted.iter().map(|p| p.coding_vector.clone()).collect();
    assert_eq!(
        distinct.len(),
        K,
        "minted pieces must be distinct independent combinations, not a repeated stored piece"
    );
    // Strong proof of full rank: the K minted pieces alone decode to content.
    let mut decoder = Decoder::new(K, PIECE_LEN);
    for p in &minted {
        decoder.add_piece(p).unwrap();
    }
    let decoded: Vec<u8> = decoder.decode().unwrap().into_iter().flatten().collect();
    assert_eq!(
        &decoded[..content.len()],
        &content[..],
        "K minted pieces span the k-dim space (independent, full rank)"
    );
}

#[test]
fn eviction_skips_pins() {
    let dir = tempfile::tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(3);
    let store = Store::open(dir.path()).unwrap();

    let mut pinned_cid = None;
    for i in 0..5 {
        let content: Vec<u8> = (0..K * PIECE_LEN).map(|_| rng.gen()).collect();
        let cid = Cid::of(&content);
        let (gen, sources) = generation(&content, &mut rng);
        store.put_generation(cid, gen).unwrap();
        for piece in encode_n(&sources, 4, &mut rng).unwrap() {
            store.put_piece(cid, &piece).unwrap();
        }
        if i == 2 {
            store.pin(cid, &content).unwrap();
            pinned_cid = Some(cid);
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }

    // Evict aggressively to near-zero; only the pin must remain.
    store.evict_to(0).unwrap();
    let cids = store.cids();
    assert_eq!(cids.len(), 1, "everything but the pin evicted");
    assert_eq!(cids[0], pinned_cid.unwrap());
    assert!(store.is_pinned(&cids[0]));
}

#[test]
fn system_objects_survive_eviction_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(7);

    let mut sys_cid = None;
    {
        let store = Store::open(dir.path()).unwrap();
        for i in 0..4 {
            let content: Vec<u8> = (0..K * PIECE_LEN).map(|_| rng.gen()).collect();
            let cid = Cid::of(&content);
            let (gen, sources) = generation(&content, &mut rng);
            store.put_generation(cid, gen).unwrap();
            for piece in encode_n(&sources, 4, &mut rng).unwrap() {
                store.put_piece(cid, &piece).unwrap();
            }
            if i == 1 {
                // A CraftSQL generation: pinned + marked system.
                store.pin(cid, &content).unwrap();
                store.mark_system(&cid).unwrap();
                sys_cid = Some(cid);
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let sys = sys_cid.unwrap();
        assert!(store.is_system(&sys));

        // Eviction never touches a system object.
        store.evict_to(0).unwrap();
        assert!(store.is_system(&sys), "system object survives eviction");
    }

    let sys = sys_cid.unwrap();
    // The system marker persists across reopen.
    let store = Store::open(dir.path()).unwrap();
    assert!(store.is_system(&sys), "system marker survives reopen");

    // Releasing it (compaction) returns it to the normal lifecycle → evictable.
    store.unmark_system(&sys).unwrap();
    store.unpin(&sys).unwrap();
    store.evict_to(0).unwrap();
    assert!(
        !store.cids().contains(&sys),
        "released system object is now evictable"
    );
}

#[test]
fn tombstone_blocks_resurrection() {
    let dir = tempfile::tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(4);
    let content: Vec<u8> = (0..K * PIECE_LEN).map(|_| rng.gen()).collect();
    let cid = Cid::of(&content);
    let (gen, sources) = generation(&content, &mut rng);

    let store = Store::open(dir.path()).unwrap();
    store.put_generation(cid, gen.clone()).unwrap();
    store
        .put_piece(cid, &encode_n(&sources, 1, &mut rng).unwrap()[0])
        .unwrap();

    store.tombstone(cid).unwrap();
    assert!(store.is_tombstoned(&cid));
    assert_eq!(store.piece_count(&cid), 0, "data removed on tombstone");
    // Repair/distribution trying to re-add is refused.
    assert!(store.put_generation(cid, gen).is_err(), "re-store refused");
    assert!(store
        .put_piece(cid, &encode_n(&sources, 1, &mut rng).unwrap()[0])
        .is_err());

    // Tombstone persists across reopen.
    drop(store);
    let store = Store::open(dir.path()).unwrap();
    assert!(store.is_tombstoned(&cid), "tombstone survives reopen");
}

/// SMALL-OBJECT PATH AT VOLUME (CraftSQL prerequisite): CraftSQL puts/gets many
/// small objects (16 KB pages) at high rate. Prove the store handles a large
/// population correctly — every object reads back byte-identical in-session AND
/// survives a reopen (reload rebuilds the whole index from 256-way shards).
#[test]
fn small_objects_at_volume_persist_and_reload() {
    let dir = tempfile::tempdir().unwrap();
    let n = 500usize;
    let page = |i: usize| -> Vec<u8> {
        let mut v: Vec<u8> = (0..16 * 1024usize)
            .map(|b| b.wrapping_mul(31).wrapping_add(i) as u8)
            .collect();
        v[0..8].copy_from_slice(&(i as u64).to_le_bytes()); // guarantee uniqueness
        v
    };
    let gen_for = |data: &[u8]| Generation {
        k: 8,
        piece_len: 2048,
        total_len: data.len() as u64,
        vtags: Vec::new(),
    };

    let t0 = std::time::Instant::now();
    {
        let store = Store::open(dir.path()).unwrap();
        for i in 0..n {
            let data = page(i);
            let cid = Cid::of(&data);
            store.put_generation(cid, gen_for(&data)).unwrap();
            store.pin(cid, &data).unwrap();
        }
        for i in 0..n {
            let data = page(i);
            assert_eq!(
                store.content(&Cid::of(&data)).as_deref(),
                Some(data.as_slice())
            );
        }
        assert_eq!(store.stats().cids, n);
    }
    let write_read = t0.elapsed();

    // Reopen — reload must recover EVERY object from disk.
    let t1 = std::time::Instant::now();
    let store = Store::open(dir.path()).unwrap();
    let reload = t1.elapsed();
    assert_eq!(store.stats().cids, n, "all {n} objects reloaded");
    for i in 0..n {
        let data = page(i);
        let cid = Cid::of(&data);
        assert_eq!(
            store.content(&cid).as_deref(),
            Some(data.as_slice()),
            "survives reload"
        );
        assert!(store.is_pinned(&cid));
    }
    eprintln!("small-object volume: {n} x 16KB — write+read {write_read:?}, reload {reload:?}");
}

/// HARDENING: content written without a generation would orphan on reload —
/// now it fails loud instead of silently losing data.
#[test]
fn put_content_without_generation_fails_loud() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let data = b"orphan me".to_vec();
    let cid = Cid::of(&data);
    let err = store.put_content(cid, &data, true);
    assert!(
        err.is_err(),
        "put_content without put_generation must error, not orphan"
    );
}

#[test]
fn bookkeeping_objects_are_never_reported_as_holdings() {
    // The holdings manifest is stored like anything else, so without this flag it lands in the very set it
    // describes: publishing a manifest changes the holdings, which publishes another manifest, forever.
    // Measured on the live fleet before the flag existed: +3-4 cids/min/node on an IDLE fleet, against a
    // provably flat store once manifests were off. `cids()` keeps reporting everything we physically hold;
    // only the holdings VIEW excludes the bookkeeping about itself.
    let dir = tempfile::tempdir().unwrap();
    let mut rng = StdRng::seed_from_u64(7);
    let data: Vec<u8> = (0..K * PIECE_LEN).map(|_| rng.gen()).collect();
    let manifest: Vec<u8> = (0..K * PIECE_LEN).map(|_| rng.gen()).collect();
    let (data_cid, manifest_cid) = (Cid::of(&data), Cid::of(&manifest));

    {
        let store = Store::open(dir.path()).unwrap();
        for (cid, bytes) in [(data_cid, &data), (manifest_cid, &manifest)] {
            let (gen, sources) = generation(bytes, &mut rng);
            store.put_generation(cid, gen).unwrap();
            for piece in encode_n(&sources, 2, &mut rng).unwrap() {
                store.put_piece(cid, &piece).unwrap();
            }
        }
        store.mark_not_holdings(&manifest_cid).unwrap();

        assert_eq!(store.content_cids(), vec![data_cid]);
        assert_eq!(store.cids().len(), 2, "we still physically hold both");
    }

    // The marker MUST survive a restart. In memory only, a restarted node re-adopts its own old manifests
    // as holdings and republishes them as data — reopening the loop it was meant to close.
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.content_cids(), vec![data_cid]);
}
