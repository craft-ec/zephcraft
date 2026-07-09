//! Transfer Plane acceptance harness (docs/TRANSFER_PLANE_V2.md, "Acceptance
//! harness") — the offline reproduction of the production pathology, built
//! from REAL components (no testkit doubles). Nothing deploys without passing
//! these bars; a FAILING run against current code is the recorded baseline.
//!
//! Heavy (in-process clusters of 8 and 20 real nodes) — run explicitly:
//!
//! ```text
//! cargo test -p zeph-tests --test transfer_plane -- --ignored --test-threads=1
//! ```

use std::time::{Duration, Instant};

use rand::prelude::*;
use zeph_core::Cid;
use zeph_tests::TestNode;

/// Distinct ~4KB payloads (index stamped in so contents can never collide).
fn payloads(n: usize) -> Vec<Vec<u8>> {
    let mut rng = rand::thread_rng();
    (0..n)
        .map(|i| {
            let mut data = vec![0u8; 4096];
            rng.fill_bytes(&mut data);
            data[..8].copy_from_slice(&(i as u64).to_le_bytes());
            data
        })
        .collect()
}

/// Spawn `n` nodes bootstrapping from `seed` — concurrently, like a real
/// (re)join wave.
async fn spawn_wave(seed: &zeph_dht::Contact, n: usize) -> Vec<TestNode> {
    futures::future::join_all((0..n).map(|_| TestNode::spawn(std::slice::from_ref(seed))))
        .await
        .into_iter()
        .map(|r| r.expect("spawn node"))
        .collect()
}

/// Wait for the publisher's initial distribution to settle: every published
/// cid marked distributed AND the pending-durability snapshot empty. Returns
/// (settled, elapsed, undistributed_left, pending_left).
async fn wait_for_distribution(
    publisher: &TestNode,
    cids: &[Cid],
    budget: Duration,
) -> (bool, Duration, usize, usize) {
    let start = Instant::now();
    loop {
        let undistributed = cids
            .iter()
            .filter(|c| !publisher.engine.store().is_distributed(c))
            .count();
        let pending = publisher.engine.pending_durability().len();
        if undistributed == 0 && pending == 0 {
            return (true, start.elapsed(), 0, 0);
        }
        if start.elapsed() >= budget {
            return (false, start.elapsed(), undistributed, pending);
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Nearest-rank percentile over a sorted slice.
fn percentile(sorted: &[u128], p: f64) -> u128 {
    assert!(!sorted.is_empty());
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn dump_cluster(nodes: &[TestNode]) {
    for (i, n) in nodes.iter().enumerate() {
        println!(
            "node{i} ({}): held_cids={} jobs={:?}",
            n.node_id.to_hex(),
            n.engine.store().cids().len(),
            n.jobs.stats()
        );
        for r in n.jobs.recent_jobs() {
            println!("  recent job: {} ok={} {}ms", r.key, r.ok, r.ms);
        }
    }
}

/// Scenario A — steady state: 8 nodes, 200 objects published and converged.
/// Bar: per-cid health scan p50 < 250ms, p99 < 1s (in-process; live LAN adds
/// ~1ms RTT).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "heavy: real 8-node in-process cluster — run explicitly (see module doc)"]
async fn scenario_a_steady_state() {
    let mut nodes = vec![TestNode::spawn(&[]).await.expect("seed node")];
    let seed = nodes[0].contact.clone();
    nodes.extend(spawn_wave(&seed, 7).await);
    println!(
        "scenario A: 8 nodes up (seed {})",
        nodes[0].node_id.to_hex()
    );

    // Publish 200 small distinct objects from node 0.
    let data = payloads(200);
    let mut cids = Vec::with_capacity(data.len());
    for d in &data {
        cids.push(
            nodes[0]
                .engine
                .publish(d, false)
                .await
                .expect("publish")
                .cid,
        );
    }
    println!("scenario A: published {} objects from node 0", cids.len());

    let (settled, took, undist, pending) =
        wait_for_distribution(&nodes[0], &cids, Duration::from_secs(120)).await;
    println!(
        "scenario A: distribution settled={settled} after {took:?} \
         (undistributed={undist}, pending_durability={pending})"
    );

    // Measure: health_scan_chunk wall-clock for 50 random held cids on 3 nodes.
    let mut samples: Vec<(usize, String, u128)> = Vec::new();
    for &ni in &[1usize, 4, 7] {
        let node = &nodes[ni];
        let mut held: Vec<Cid> = node.engine.store().cids();
        assert!(
            !held.is_empty(),
            "node {ni} holds nothing to scan — distribution never reached it"
        );
        {
            let mut rng = rand::thread_rng();
            held.shuffle(&mut rng);
        }
        held.truncate(50);
        for cid in held {
            let t0 = Instant::now();
            let _ = node.engine.health_scan_chunk(&[cid]).await;
            samples.push((ni, cid.to_hex(), t0.elapsed().as_millis()));
        }
    }

    let mut ms: Vec<u128> = samples.iter().map(|s| s.2).collect();
    ms.sort_unstable();
    let (p50, p90, p99) = (
        percentile(&ms, 50.0),
        percentile(&ms, 90.0),
        percentile(&ms, 99.0),
    );
    let max = *ms.last().expect("samples");
    println!(
        "scenario A: scan latency over {} samples: p50={p50}ms p90={p90}ms \
         p99={p99}ms max={max}ms",
        ms.len()
    );

    let pass = p50 < 250 && p99 < 1000;
    if !pass {
        println!("--- scenario A FAILURE diagnostics ---");
        println!("full latency distribution (ms, sorted): {ms:?}");
        println!("per-sample (node, cid, ms):");
        for (ni, cid, m) in &samples {
            println!("  node{ni} {cid} {m}ms");
        }
        dump_cluster(&nodes);
    }
    for n in &mut nodes {
        n.shutdown().await;
    }
    assert!(
        pass,
        "scenario A bar failed: p50={p50}ms (bar < 250ms), p99={p99}ms (bar < 1000ms), \
         max={max}ms over {} samples",
        ms.len()
    );
}

/// Scenario B — mass rejoin: 5 nodes with 100 published objects, then 15 more
/// nodes join at once. Bars: every node's census reaches 20 within 30s; every
/// node's job queue is eventually (< 180s) below depth 10 and STAYS there; no
/// completed job observed above 10s wall-clock.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "heavy: real 20-node in-process cluster — run explicitly (see module doc)"]
async fn scenario_b_mass_rejoin() {
    let mut nodes = vec![TestNode::spawn(&[]).await.expect("seed node")];
    let seed = nodes[0].contact.clone();
    nodes.extend(spawn_wave(&seed, 4).await);
    println!(
        "scenario B: 5 nodes up (seed {})",
        nodes[0].node_id.to_hex()
    );

    let data = payloads(100);
    let mut cids = Vec::with_capacity(data.len());
    for d in &data {
        cids.push(
            nodes[0]
                .engine
                .publish(d, false)
                .await
                .expect("publish")
                .cid,
        );
    }
    let (settled, took, undist, pending) =
        wait_for_distribution(&nodes[0], &cids, Duration::from_secs(120)).await;
    println!(
        "scenario B: initial distribution settled={settled} after {took:?} \
         (undistributed={undist}, pending_durability={pending})"
    );

    // Mass rejoin: 15 more nodes in one wave.
    let t_join = Instant::now();
    nodes.extend(spawn_wave(&seed, 15).await);
    println!(
        "scenario B: 15-node join wave spawned in {:?}",
        t_join.elapsed()
    );

    // Bar 1: census reaches 20 on EVERY node within 30s of the wave.
    let mut census_full_at: Option<Duration> = None;
    let mut last_census: Vec<usize> = vec![0; nodes.len()];
    while t_join.elapsed() < Duration::from_secs(30) {
        for (i, n) in nodes.iter().enumerate() {
            last_census[i] = n.membership.census().await.len();
        }
        if last_census.iter().all(|&c| c >= 20) {
            census_full_at = Some(t_join.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    println!(
        "scenario B: census-20 within 30s: {census_full_at:?} (counts at 30s: {last_census:?})"
    );

    // Bar 2: monitor every node's coordinator for the FULL 180s window (no
    // early exit — the rejoin storm takes ~a minute to form; an instant green
    // poll must not mask it). "Eventually drained" = the final 3 polls (15s)
    // all show queue_depth < 10 on every node.
    let mut census_full_late = census_full_at;
    let mut depth_timeline: Vec<(u64, u64, usize)> = Vec::new(); // (t_secs, max_depth, node)
    let mut worst_depths: Vec<u64> = vec![0; nodes.len()];
    let mut all_below_history: Vec<bool> = Vec::new();
    let mut max_job: (u64, String, usize) = (0, String::new(), 0); // (ms, key, node)
    let mut slow_jobs: std::collections::BTreeMap<(usize, String, u64), bool> =
        std::collections::BTreeMap::new();
    let drain_start = Instant::now();
    loop {
        let t = drain_start.elapsed();
        let mut all_below = true;
        let mut poll_max = (0u64, 0usize);
        for (i, n) in nodes.iter().enumerate() {
            let s = n.jobs.stats();
            worst_depths[i] = worst_depths[i].max(s.queue_depth);
            if s.queue_depth >= 10 {
                all_below = false;
            }
            if s.queue_depth > poll_max.0 {
                poll_max = (s.queue_depth, i);
            }
            for r in n.jobs.recent_jobs() {
                if r.ms > max_job.0 {
                    max_job = (r.ms, r.key.clone(), i);
                }
                if r.ms > 10_000 {
                    slow_jobs.insert((i, r.key.clone(), r.ms), r.ok);
                }
            }
        }
        all_below_history.push(all_below);
        depth_timeline.push((t.as_secs(), poll_max.0, poll_max.1));
        if census_full_late.is_none() {
            for (i, n) in nodes.iter().enumerate() {
                last_census[i] = n.membership.census().await.len();
            }
            if last_census.iter().all(|&c| c >= 20) {
                census_full_late = Some(t_join.elapsed());
            }
        }
        if t >= Duration::from_secs(180) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    let drained = all_below_history.len() >= 3
        && all_below_history[all_below_history.len() - 3..]
            .iter()
            .all(|&b| b);

    let mut violations: Vec<String> = Vec::new();
    if census_full_at.is_none() {
        violations.push(format!(
            "census did not reach 20 on every node within 30s \
             (reached at {census_full_late:?}; latest counts {last_census:?})"
        ));
    }
    if !drained {
        violations.push(format!(
            "queues not drained: final polls still had nodes at queue_depth >= 10 \
             (worst depths per node: {worst_depths:?})"
        ));
    }
    if max_job.0 > 10_000 {
        violations.push(format!(
            "job wall-clock bar broken: max completed job {}ms (key={}, node{}) — bar 10000ms; \
             all observed >10s jobs: {:?}",
            max_job.0,
            max_job.1,
            max_job.2,
            slow_jobs.keys().collect::<Vec<_>>()
        ));
    }
    println!(
        "scenario B: census20_at={census_full_late:?} drained={drained} \
         max_job={}ms ({} on node{})",
        max_job.0, max_job.1, max_job.2
    );
    println!(
        "scenario B: cluster max queue_depth timeline (t_secs, depth, node): {depth_timeline:?}"
    );
    if !violations.is_empty() {
        println!("--- scenario B FAILURE diagnostics ---");
        dump_cluster(&nodes);
    }
    for n in &mut nodes {
        n.shutdown().await;
    }
    assert!(
        violations.is_empty(),
        "scenario B bars failed:\n{}",
        violations.join("\n")
    );
}
