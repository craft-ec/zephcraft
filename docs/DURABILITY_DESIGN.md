# Durability — manifest accounting + death-driven repair

**Status:** design, unbuilt. Supersedes the periodic health scan as the primary durability mechanism.
**Date:** 2026-07-17.
**Origin:** a live bandwidth investigation (see §1) that measured the current scan's real cost.

---

## 0. The one-line claim

Durability today is checked by **polling every cid on a timer**. That is O(N_cids) forever, it detects
loss in 15min–2h, and it is the largest traffic source on an idle fleet. Pieces are not lost per-cid —
they are lost **per-node**, and the network already learns of node death **in seconds**. Accounting per
NODE instead of per CID makes the work proportional to **churn** rather than to **inventory**, and makes
detection *faster* at the same time. The current design is on the wrong axis, not merely mistuned.

---

## 1. Why — the measured problem

Not inferred. From the per-tag transport counters, on a live node **104 seconds after restart**:

```
dht:    26,193 inbound streams   (~252/sec)     store_cids: 2890
piece:   6,219                   (~60/sec)      scan_queue: 2890
member:     79 | ping: 61 | registry: 8
```

The health scan resolves **every held cid** (`routing.resolve(cid)` per cid — an *iterative* DHT lookup
fanning out to several peers). Every cid is due at boot, so a fleet-wide restart has every node resolving
its entire store at once: **the fleet DDoSing its own DHT**. Two fixes have landed as stopgaps — the
re-check floor (30s → 15min) and dripping the first pass over a full re-check interval so it runs at the
steady-state rate — but both are constant factors on an O(N) design:

| Store | @15 min | @2 h |
|---|---|---|
| 3,000 | 3.3/s | 0.4/s |
| 100,000 | 111/s | 14/s |
| **1,000,000** | **1,111/s** | **139/s** |

At 1M cids even a 2-hour period is ~139 resolves/sec **per node**, each fanning out ~8 peers ≈ 1.1k DHT
ops/sec — the storm rebuilt from scratch. Reaching ~3/s would need a **~4-day** period, which is not a
durability backstop. **Sampling does not rescue this**: sampling is polling with a smaller constant — same
axis, same O(N) coverage debt, just paid slower.

**The deeper cost:** polling is *also slower*. SWIM detects death in **seconds**; the scan detects it in
**15min–2h**. The current design is simultaneously more expensive AND less durable. Polling only felt safe.

---

## 2. The structural move — account per NODE, not per CID

> **Each node publishes a signed MANIFEST of what it holds** (a Merkle root over its cid set, plus the set
> or a diff). N_nodes assertions replace N_cids probes.

1,000 nodes × 1 manifest, versus 1,000,000 cid probes. Work then scales with **churn** (what actually
creates repair need) instead of **inventory** (which creates none).

This is the same pattern the codebase already uses twice and should not invent a third time:
- **membership** — gossip a digest; reconcile only on mismatch (O(1) steady-state).
- **registry** — `PushState`; the writer pushes, replicas do not poll.

---

## 3. The layers

| Layer | Catches | Cost |
|---|---|---|
| **Death-driven repair** — SWIM `Dead` → read the dead node's last manifest → repair its cids | node death (the common case) | O(dead node's set), only on an event |
| **Manifest anti-entropy** — compare signed roots, drill into diffs | loss the holder KNOWS about (eviction, deliberate drops) + missed death events. **NOT** unaware loss — see below | O(1) compare, O(diff) reconcile |
| **Availability probe (K8)** — ask a holder for a cid | loss the holder does NOT know about (bytes gone, index intact) | O(probed cids) |
| **PDP sampling** (K5) | a node **lying** — claiming a manifest it cannot back | O(k) challenges, independent of N |
| **Repair-on-read** | hot data | free |
| **Erasure margin (n/k)** | buys time so repair can be lazy + batched | free |

They cover each other's blind spots, and none is sufficient alone:
- death-driven alone misses **silent loss** (node alive, data gone);
- manifests alone **trust the holder**;
- PDP alone is too expensive to run exhaustively.

**PDP is what makes a manifest trustworthy.** A signed claim + random spot-checks is a far stronger
primitive than either half. K5 is already in the tracker; this design is its first real consumer.

---

## 4. Distributing repair without coordination

Node **X** dies holding set `S_x`. Every survivor holds a *different* set. Who repairs what?

**Step 1 — local intersection.** Each survivor **Y** fetches X's manifest **once** (one read, not one per
cid) and computes `S_x ∩ S_y` locally — no network. Y only considers cids it **already holds pieces for**.
The differing sets are not a coordination problem; they are what **partitions the work for free**.

**Step 2 — rendezvous election.** For each shared cid:

```
repairer(c) = argmax_HRW( hash(c, node) )   over the surviving holders of c
```

Every survivor computes the **same** winner from the **same** inputs, so there are no messages, no leader,
and no consensus. Hashing on the cid spreads X's set uniformly across everyone who overlapped it, in
proportion to overlap. Both primitives already exist: `headreg` uses blake3 rendezvous for shard→writer,
and the health scan already elects a repairer from verified holders. This reuses them on an **event**
rather than a **timer**.

**Cost:** O(|S_x|) fleet-wide — *what the dead node held* — spread across its overlapping peers, and only
when a node actually dies. An idle fleet costs **zero**.

### 4.1 Failover — election without it just moves the SPOF

If the winner is slow, dead, or lying, the cid waits **silently**. So the election is **ranked**: if the
piece count has not recovered within a deadline, rank-2 assumes the duty, then rank-3. The deadline must
exceed a realistic repair time, or a slow repairer causes a duplicate storm.

### 4.2 Budget + priority — correlated failure is the dangerous case

A rack/AZ loss (or the 19-node freeze in the tracker) kills many nodes at once, so death-driven repair
stampedes **precisely when the fleet is weakest**. Therefore:
- **bound** the repair rate (a budget, like the job-class caps already in the scheduler);
- **order by ACTUAL redundancy** — cids at k+1 before k+3. Repairing in discovery order loses the urgent
  ones while the fleet is busy with the safe ones.

### 4.3 Hysteresis — do not repair a flap

A flapping node (see the relay-churn note) triggers repair → returns → the work was waste, and it repeats.
Arm repair **only on converged `Dead`**, never on `Suspect`. SWIM's Suspect state exists for exactly this.

---

## 5. The convergence hazard (the one real correctness risk)

Everything above rests on "every node computes the same answer". That holds only while the **census is
converged**. The tracker already records this hazard on the settlement path: *"the participant SET is the
converged census, so a momentary census difference can differ the record until it converges."* Same input,
same failure mode: if A sees X as Dead and B does not yet, their candidate sets differ, the election picks
different winners, and either two nodes repair (waste) or each assumes the other will (**loss**).

**The asymmetry decides the design:** a duplicate repair costs bandwidth; a missed repair costs **data**.
So:

1. **Repair MUST be idempotent** — regenerating an existing piece is a no-op. That makes duplicates *safe*,
   which is what permits the next rule.
2. **When views disagree, ACT.** Never skip on the assumption that someone else will.
3. **Hysteresis first** — converged `Dead` only; most divergence then never materialises.
4. **Probe before repairing** — manifests are *snapshots*; a node may have gained or lost since publishing.
   K8's `AvailabilityProbe` is ground truth. Probe-then-repair makes a stale manifest cost one cheap probe
   instead of a pointless regenerate-and-distribute.

**Layering:** manifest = the candidate list (cheap, may be stale) · probe = the truth (verify before
working) · census = the shared basis for agreeing *without talking*.

---

## 6. Honest gaps

- **Last-holder loss.** A cid in `S_x` that **no** survivor holds appears in nobody's intersection — it is
  invisible to death-driven repair. It is already-lost data (X was the last holder), so preventing it is
  the placement/erasure policy's job, not repair's. Stated explicitly because the design otherwise quietly
  assumes coverage it does not have. Manifest anti-entropy is the backstop that would *surface* it.
- **Lying nodes** are only caught by PDP sampling (K5, unbuilt). Until then a manifest is trusted.
- **Unaware loss** (bytes gone, index intact) is invisible to manifests *by construction* — the node reports
  what it believes, and it believes wrongly. Only asking for the bytes settles it (K8 probe / K5 PDP). This
  is why P3 cannot retire the probe, and it is the strongest argument for prioritising K5.
- **Reverse index cost — RESOLVED [2026-07-17, during P4].** The first cut cached every watched peer's full
  set (O(N_nodes × N_cids)) — the scan's O(N) mistake moved into memory. Fixed by the observation that a node
  can only repair cids **it holds**: the index therefore keys on OUR cids and stores only the intersection
  (`HolderIndex: our_cid → {peers}`), bounded by OUR store × replication regardless of fleet inventory. A
  peer's holdings we do not share are not ours to remember. Bonus: a death is now answered ENTIRELY from the
  index — `{c : holders[c] ∋ dead}` — so it costs no manifest fetch and no DHT lookup at the moment the fleet
  can least afford either.
- **Manifest size — RESOLVED [2026-07-17].** The manifest is now `Body::Snapshot | Body::Diff{added,
  removed, prev}`: the publisher emits a DIFF against the previous version (it already knows what it
  added/removed — making every reader re-fetch the set to re-derive that was the waste), with a full
  snapshot every `SNAPSHOT_EVERY` versions to bound a cold reader's walk back down the `prev` chain.
  `changes_since(peer, known)` gives readers an O(Δ) answer — one small object naming exactly what moved —
  and only a reader with NO usable baseline pays for a set. NOTE: no Merkle field was needed; the manifest
  is content-addressed, so the head cid already IS the root, and an extra tree would only duplicate it —
  readers never hold a peer's full set to verify a root against anyway.
  The DIFF is SIGNED (not just the resulting set): a reader applies it without ever seeing the whole set, so
  from its point of view the diff IS the claim. An unsigned diff would let anyone suppress a reported loss
  (silent data loss) or invent one (manufactured fleet-wide repair).
- **Sampling is not a fix.** Recorded here because it is the intuitive answer and it is wrong: same axis,
  smaller constant.

---

## 7. Phases

1. **P1 — manifests.** Publish a signed holdings manifest (Merkle root + diff) per node; read a peer's.
   No behaviour change yet — purely additive.
2. **P2 — death-driven repair.** SWIM converged `Dead` → intersect → HRW elect → probe → repair. Ranked
   failover. Runs ALONGSIDE the periodic scan, which becomes the backstop with a long period.
3. **P3 — manifest anti-entropy.** Watch peers' heads (O(1) each); a changed head means changed holdings →
   fetch → diff → the cids they no longer hold are potential loss → elect → repair. Replaces the scan's
   `resolve(cid)`-per-cid **provider lookup** (holders now come from manifests, locally).
   **CORRECTION [2026-07-17, during implementation].** An earlier draft said this retires the periodic scan.
   It does not, and shipping that would have been a durability REGRESSION. `store.cids()` enumerates the
   INDEX, not the bytes: a node whose disk failed while its index survived publishes an UNCHANGED manifest —
   it does not know it lost anything. A manifest is a claim about what a node *believes* it holds, so
   anti-entropy can only catch loss the holder is AWARE of (eviction, deliberate drops). Unaware loss is
   caught only by asking for the bytes: K8's `AvailabilityProbe` today, PDP (K5) properly. **The scan's
   probe must therefore survive P3** — only its per-cid DHT resolve goes. Retiring the probe requires P5.
4. **P4 — budget + priority.** Repair rate cap; order by actual redundancy. Needed before any large fleet.
5. **P5 — PDP sampling (K5).** Makes manifests trustworthy rather than trusted — and is what finally allows
   the periodic probe to be replaced by sampling rather than merely made cheaper. Until P5 lands, a
   probe of some form MUST keep running: manifests + death events cover honest, self-known loss only.

**Do not skip P4 before scale**: an unbudgeted death-driven repair on a correlated failure is a worse
outage than the polling it replaces.

---

## 8. What this changes, numerically

| | today (polling) | this design |
|---|---|---|
| Steady-state cost | O(N_cids) per interval, forever | **zero** (no events, no work) |
| Cost on a node death | none (until the timer notices) | O(dead node's set), once |
| Detection latency | 15 min – 2 h | **seconds** (SWIM) |
| Scaling limit | ~10⁴ cids before the period becomes absurd | churn-bound, not inventory-bound |
| Silent-loss coverage | yes (that is what the scan is for) | manifest anti-entropy |
| Liar coverage | AvailabilityProbe (per scan) | PDP sampling (K5) |
