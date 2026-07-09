# Transfer Plane v2 — structural design

Status: DESIGN (2026-07-09). Supersedes the v1 transfer behaviors incrementally
patched during the 20-node scaling work. Directed by the operator's design
decisions recorded in `.claude/feature-progress.md` (Phase 5); this document is
the buildable spec. Governing principle: **work is queued, negotiated, and
budgeted — never fanned out on impulse.**

## Why a rebuild, not more patches

The v1 transfer plane let every subsystem (scan, repair, distribution,
replication, announce) independently contact any peer at any moment, with
failure signaled only by multi-second timeouts. Every scaling incident of
2026-07-09 traces to that shape:

- conn-per-request → handshake storms → OOM kill-loops (fixed by pooling,
  which then serialized servers per peer — the scan convoy);
- unpaced fan-out at boot/rejoin → dial storms → false-dead cascades;
- timeout-as-failure-signal → job slots wasted for seconds per busy peer;
- N holders all scanning the same cid → N× duplicated lookups hogging the
  cluster (scan traffic *grows* as healing succeeds).

Patching each symptom moved the bottleneck. The v2 shape removes the class.

## The five structural elements

### 1. One multiplexed connection per peer
Single QUIC connection per peer; every protocol is a stream carrying a 1-byte
protocol tag (replaces per-ALPN connections). Server side handles streams with
bounded concurrent pipelining (no per-peer serialization, no unbounded
parallelism). ~19 connections per node at 20 nodes, versus ~190.

### 2. Bounded active set (choke model)
A node performs transfer WORK (pushes, pulls, replication) with at most K
peers at a time (default K=4, BitTorrent-style). All other known peers are
candidates in a queue — cheap addresses, zero live cost. The active set
rotates: a peer leaves the set when its queued work drains or it misbehaves
(busy/slow), the next candidate enters. Liveness probes and census gossip are
NOT budgeted (tiny, fixed-rate, correctness-critical).

### 3. Offer/grant admission (negotiate before bytes)
New wire exchange on the transfer path:
- `Offer { class, cid, items, bytes }` — sender proposes a batch.
- `Grant { accept, retry_after_ms }` — receiver grants 0..=items from its gauge
  state (critical=0, high=1, else up to 4 per offer).
On partial/zero grant: **redirect** the remainder to the next candidate when
the target is fungible (coded pieces — any candidate serves), **requeue with
backoff** when it is fixed (registry shard replicas — election-bound). A busy
answer costs one RTT, not a timeout. Wire change → version-consistent roll.

### 4. Elected healthscan (scanner = actor)
Rendezvous-elect ONE scanner per cid per epoch over the last-known capable
holder set (ids+counts recorded from the previous scan's provider records) ∪
self. Only the winner scans (one DHT resolve per cid per interval,
cluster-wide); having won, it repairs/degrades directly. Non-winners just
reschedule locally. Divergent views → occasional benign double-scan; dead
winners age out via membership-filtered records; a fresh holder bootstraps
with one unconditional scan. Kills the N×-holders scan multiplication AND the
"repair waits for the winner to coincidentally scan" lag. Composes with
provider-aware backoff (already shipped).

### 5. Fair scheduling across work classes
The coordinator keeps priorities for URGENCY (Repair preempts Eviction) but
adds class fairness so no class HOGS the queue: per-class in-flight caps
(e.g. pushstate ≤ 2, scans ≤ 4 of 8 slots) instead of pure priority order.
Boot keeps the sequential phases + clamp (shipped); steady state gets fairness.

## Scale elements (designed upfront — the connectivity substrate itself)

The registry was sharded for thousands of nodes while the substrate under it
(membership, census-coupled election, reactive sweeps) stayed O(N) per node
and O(N^2) aggregate. The DHT is the stack's only log-N structure; these
elements move everything else onto it or into O(delta):

- **S1 — Digest membership.** Shuffles exchange a member-set DIGEST (bucketed
  root hash + count); full entries flow only for differing buckets. Census
  stays convergent; steady-state gossip cost drops from O(N)/round to O(delta)
  (~zero when stable). Ceiling moves from ~hundreds of nodes to ~10k+.
- **S2 — DHT-native registry placement.** Shard replicas = the K DHT-closest
  node ids to BLAKE3(rtype||shard); writer = closest live. Placement inherits
  Kademlia's log-N scaling and needs NO census. Removes the census-coupled
  election — the current design's real ceiling — and the readiness gate
  becomes DHT-table-based. (Largest migration; build after v2 core.)
- **S3 — Lazy-only convergence.** Joins/leaves trigger NOTHING. All placement
  correction happens through each item's elected, paced scan (plus on-demand
  repair on resolve-miss). Churn of any size produces bounded work by
  construction; the census-change sweeps (migrate/distribute storms) are
  deleted, not gated.
- **S4 — Version beacons on gossip.** Governance/config sequence numbers ride
  the shuffle; content is fetched only on delta. Kills the last O(N) polling
  loop (governance tick).
- **S5 — The invariant (enforced in review).** Per-node background work =
  O(held + active_set) — never O(census), never O(cluster), never reactive to
  membership events. Code iterating the member set or the full cid set outside
  a paced queue is a defect.

Not reinvented: the Kademlia DHT (log-N routing backbone), BLAKE3 content
addressing, RLNC erasure — the gap was everything that bypassed them with
global views and impulse fan-out.

## Acceptance harness (built FIRST — nothing deploys without passing it)

`tests/transfer_plane.rs` (zeph-testkit): spawns a full in-process cluster
(LocalOnly transport, real engines/DHT/registry/coordinator — no mocks), runs
the exact workload that broke production, and asserts the operator's bars:

- **Scenario A — steady state**: 8 nodes, 200 objects published and converged.
  Bar: scan p50 < 250ms, p99 < 1s (in-process; live LAN adds ~1ms RTT).
- **Scenario B — mass rejoin**: publish on 5 nodes, start 15 more, converge.
  Bar: census reaches 20 < 30s; at-risk drains to 0; NO job > 10s wall-clock;
  queue drains monotonically (no plateau > 60s).
- **Scenario C — capped receiver**: one node with a tiny simulated budget
  (gauge forced high). Bar: cluster converges around it; the capped node sheds
  via grants (zero timeouts consumed by busy peers).

Instrumented: per-job wall-clock distribution, per-class slot occupancy,
connection count, dial attempts — printed on failure so a regression names
its bottleneck.

## Build order (each step passes the harness before the next)

1. Harness with Scenario A+B against CURRENT code — records the baseline and
   reproduces the pathology offline (validates the harness itself).
2. Element 5 (class fairness) + Element 4 (elected scan) — no wire change.
3. Elements 1+3 (mux + offer/grant) — ONE wire migration, version-consistent
   fleet roll.
4. Element 2 (choke set) — sender-side, no wire change.
5. Single production deploy + the original stress measurements (writer spread,
   remote resolve latency, reshard under load).
