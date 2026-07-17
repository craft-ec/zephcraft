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
| **Death-driven repair** — SWIM `Dead` → intersect the local index → rendezvous-elect → repair | node death (the common case) | election O(1) local; **execution O(elected) DHT resolves** — see note | 
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

**Measured on the live fleet [2026-07-17], and a correction to the cost column.** Killing a node holding
~3600 cids: SWIM converged to `Dead` in 25s, the per-cid rendezvous election partitioned the set across the
three survivors (1214 / 1259 / 1242 elected, sum 3715 vs ~3600 candidates ≈ 3% overlap — the safe kind:
two survivors both elect, one mints, the other's `repair_cid` finds it already at floor and no-ops), 281
pieces re-minted, last-holder cases first. So the coordination-free election is validated (on an easy,
high-overlap topology — see §6). BUT the *execution* took **2h36m**. The election is O(1) local; the
execution is O(elected) DHT resolves (`repair_cid` re-resolves providers per cid, the probe-before-repair
safety), and at ~3.8s/resolve that is hours for a large node. Two fixes this forced:
- **`on_death` is now SPAWNED, not awaited inline.** It ran on the census-watcher thread, so the watcher was
  blind to any further death for the whole 2h36m — proven by the victim's rejoin not registering until the
  instant repair finished. A detector must not be held hostage by its executor, least of all under the
  correlated failure this whole layer is for.
- **`repair_our_share` now logs START + progress + DONE**, not only DONE. A multi-hour grind that speaks only
  at the end is indistinguishable from a hang or a no-op — which is why two earlier live tests read as
  failures while repair was in fact running correctly.

**Open — execution cost.** O(elected) DHT resolves per death is the real remaining cost, and most are
no-ops (1242 elected, 87 minted). A pre-filter — skip the resolve when the local index still shows enough
holders — would cut it ~14x, but the index tracks holder NODES not piece COUNTS, so it cannot compute the
piece floor directly; closing that needs per-holder counts in the index or a cheaper availability signal.
Deferred, and noted so the "cheap local" framing is not trusted at scale.

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

## 5. Liveness is not durability — the diurnal oscillator

**This is the gap that decides whether the design works for home nodes, and today it does not.**

Everything in §4 arms repair on converged `Dead`. That is correct when death is permanent. It is wrong when
the fleet is home machines that go to sleep at midnight and come back at 08:00 — because *at the moment of
departure, "asleep" and "dead" are indistinguishable*. Only waiting tells them apart, and repair does not
wait.

**The one number doing two jobs.** Repair and shed are both driven by

    effective = own_pieces + Σ (piece_count of LIVE providers)

That single quantity is being asked to answer two different questions: *do the bytes still exist*
(DURABILITY) and *can I reach k of them right now* (AVAILABILITY). On an always-on fleet those are the same
number, which is why this never surfaced. For a node that is present 12h a day they diverge every single
night, and both mechanisms treat the divergence as real: a sleeping node's bytes are counted as LOST, and a
waking node's bytes are counted as SURPLUS.

**The closed loop.** Neither half is wrong on its own; together they oscillate forever.

| phase | `effective` | mechanism | action |
|---|---|---|---|
| night — X sleeps | `floor − X` | death-driven repair (§4) | MINT, in bulk, within seconds |
| day — X wakes | `floor + X` | cold-surplus shed | SHED, 1 piece per scan |

The day's shed destroys exactly the redundancy the night's repair built, because while X is awake its pieces
make the cid *look* over-replicated. Then X sleeps and it is a deficit again. Nothing converges to a stable
set of pieces; it converges to a stable **churn**.

It is not marginal at real parameters. The Schmitt band is `delta = max(floor/8, 2)`, so at k=32 (floor ≈ 96)
delta ≈ 12, while a holder on a ~5-way spread carries ≈ 19 pieces. Any node holding more than delta sustains
the cycle — i.e. every ordinary holder.

**The asymmetry is structural, not a tuning bug.** Absence is PUSHED: SWIM tells the whole fleet in ~25s and
repair fires off the local index with no scan. Presence is PULLED: nobody is notified that a node came back;
its provider records simply reappear in the DHT, and the surplus is only noticed when some holder next scans
that cid — a bounded but lazy ~1–2h (`recheck_max`), and then only ONE piece per scan. The system reacts to
a home node vanishing in seconds and to its return in hours.

**At this fleet's scale it merely oscillates. At the target scale it RATCHETS.** Because shed is rate-limited
to 1 piece/scan and repair mints in bulk, the surplus never fully sheds before nightfall — the leftover is
what absorbs X's next absence — so on a small fleet it damps to a steady ~7 pieces/cid/night of mint+shed
rather than growing. That is luck, not design.

And it is luck that RUNS OUT, because it depends on the one thing this design exists to remove. `shed_one` is
called from exactly one place: inside `health_scan_chunk`. **Every shed rides the periodic health scan** —
the O(N_cids)/interval sweep measured in §1 at ~252 DHT streams/sec/node, which does not hold at 1M cids and
which P5 replaces with PDP SAMPLING. Sampling visits cids statistically, not exhaustively; a given cid's
surplus is then essentially never noticed. So at the target scale:

| | trigger | cost | fires? |
|---|---|---|---|
| MINT | SWIM death (event) | O(churn) | promptly, in bulk, at any scale |
| SHED | periodic scan (sweep) | O(N_cids) | never, at scale |

Every night every home node's absence mints; nothing ever sheds. Storage grows monotonically, forever.

**Correction — the MINT path is not itself wrong; it is floor-gated.** `repair_cid` recounts live providers
into `have = own + Σ live piece_counts` and returns early on `have >= floor`. So a departure only mints when
it genuinely drops the cid below the durability floor. It fires on an ordinary absence for a narrower reason
than "mint is broken": the standing margin is the Schmitt band `delta ≈ 12` (at k=32), while one holder on a
~5-way spread carries `≈ 19` pieces — so a single absence really does cross the floor. And what pins the
margin that thin is the SHED, which trims `effective` down toward the floor *while counting the soon-absent
node's pieces*. So the fault is two-sided: the margin is provisioned smaller than one node's share, and the
shed actively removes what margin remains. The mint is the one part behaving correctly.

**The general error, which is worth more than the specific bug:** repair was made event-driven while its
INVERSE was left sweep-driven. That pair cannot be stable at any scale where the sweep is the thing being
retired — the fast side always wins, and the slow side's cleanup is exactly the O(N_cids) cost this design
was built to delete. A mechanism that creates work on an event must have its cleanup driven by an event too,
or it must not create the work at all.

**Why a grace period is NOT the fix.** The obvious patch — wait before treating `Dead` as lost — only
addresses the night half. The day half still trims X's pieces as cold surplus, so the fleet keeps paying,
and the grace window buys the reduced durability of an under-replicated cid for nothing. A timer cannot fix
a mis-measurement.

**The fix has three parts, ordered by how load-bearing they are.**

**(1) Unify repair and shed into ONE queued job type, per cid — not a per-event sweep [BUILT 2026-07-18].**
This is the structural correction under everything else. Death repair used to run as one giant per-death
task looping ~1200 cids, holding a budget permit across the whole sweep; spawning it (so it stopped blocking
the census watcher) only gave the sweep its own thread — the sweep itself is still the wrong unit. The right
unit is the health scan's: a single `PieceJob { cid, kind }` submitted to the shared scheduler, one per cid,
drained at a bounded worker count with per-cid dedup and priority. A death then ENQUEUES `|S_x ∩ ours|` jobs
and returns immediately; the scheduler does the rest. Multiple deaths add jobs to the same queue instead of
spawning competing sweeps. Repair and shed are the *same* job with opposite `kind`, because they are the
same operation — "move `effective` toward the band" — in opposite directions, and must not grow two
divergent execution paths.

**(2) The queue latency IS the epoch offset — no moving average needed.** A job re-checks current state at
EXECUTION time (`repair_cid` already re-resolves and no-ops on `have >= floor`; shed will mirror it). So a
repair job enqueued when X leaves, drained after a deliberate debounce, simply no-ops if X (or an equivalent
holder) is back by then. Transient churn self-cancels through queue delay + execution-time re-check — the
"absolute net over an epoch" idea, realised as a property of the queue rather than a statistic anyone
computes. Urgency sets the delay: below-k / last-holder jobs run at the front with ~no debounce (real loss
cannot wait); margin-restoration jobs (above-k, below-band) carry a debounce long enough that a normal
sleep/wake offsets before they fire. A true moving average was rejected earlier for a timescale reason — the
window that hides a night also delays real-loss detection by a night — and the per-job debounce sidesteps it
by keying the delay on URGENCY, not on a fixed time.

**(3) Placement across the availability dimension is the primary lever; accounting split + provisioning are
the backup.** The deepest fix needs a mixed fleet to tune, so it is sequenced last:
- **Phase-diverse placement.** "One region sleeps as another wakes" only helps a given cid if THAT cid's
  pieces span the anti-correlated regions. Today placement is `peer_source.peers()` — timezone-blind, it
  spreads onto whoever is reachable at publish, i.e. onto the awake cohort that then sleeps together. Spread
  each cid's `n` pieces across the diurnal-phase dimension (a failure-domain-style spread) and a roughly
  constant fraction of every cid's holders is always awake, so instantaneous `effective` never crosses the
  floor and the EXISTING gate handles absence with nothing added. Needs peers' phase, which is observable
  (uptime pattern), not declared.
- **Accounting split + `k/p` provisioning.** Count pieces that EXIST for durability (only that mints), pieces
  REACHABLE for availability, and hold `k/p` for the expected online fraction `p` so a normal night never
  crosses the line. This is the static version of the same idea; placement is the adaptive one.

Together these make the returning node a non-event — **it never lost anything** — and, crucially, with no
unnecessary mint there is no surplus, so the scan-driven shed is never needed to clean up after it. The
scaling problem is not solved by cheaper cleanup; it is dissolved by not making the mess.

**What (3) needs that we do not have.** Distinguishing "offline but holding" from "gone forever" requires
knowing a holder still HAS the bytes while we cannot reach it — a signed manifest is a claim, worth even less
from a node that is not answering. Same trust boundary PDP (K5) closes. So the honest sequencing is: build
(1) and (2) now (they are correct at any `p` and testable via ordinary surplus/deficit), measure the online
fraction on a real mixed fleet, then tune (3).

**Status.** (1) and (2) BUILT and rolled 2026-07-18. (3) OPEN — not exercised by the current fleet (4
always-on Hetzner nodes, where durability and availability are identical by construction), so its tuning is
unmeasured. The A-G harness cannot see the diurnal case either: `scenario_h_mass_death` kills permanently. A
diurnal scenario (stop/restore on offset phases, assert net mint/shed churn ≈ 0) is the test that would, and
is the natural home for validating (3) once there is a fleet with real phase diversity.

---

## 6. The convergence hazard (the one real correctness risk)

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

## 7. Honest gaps

- **Last-holder loss.** A cid in `S_x` that **no** survivor holds appears in nobody's intersection — it is
  invisible to death-driven repair. It is already-lost data (X was the last holder), so preventing it is
  the placement/erasure policy's job, not repair's. Stated explicitly because the design otherwise quietly
  assumes coverage it does not have. Manifest anti-entropy is the backstop that would *surface* it.
- **A restarting node's republish. FIXED [2026-07-17].** `ManifestStore.last` was in-memory, so a restart
  reset the manifest version to 1 — but `announce_app` uses that version as the DHT record's `seq` and
  `RecordStore::put` rejects `existing.seq >= rec.seq`, so a restarted node met its OWN pre-restart record
  and every republish was refused until the count climbed back past it. Holdings head frozen at a stale
  manifest, every change invisible, for up to the 1h record TTL. A long absence self-healed (the record
  expires); a quick restart did not, and a home node restarting daily hit it every morning. Now resumed from
  a persisted high-water mark. The last SET is deliberately NOT persisted — that is the ~32 MB write this
  design exists to avoid — so a process's first publish is a `Snapshot` and readers re-baseline via
  `Changes::Reset`, costing one full-set fetch per peer per restart.
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

  **The trap this structure sets, and the rule that defuses it.** "It dropped nothing" and "I cannot tell
  you what it dropped" are DIFFERENT claims, and the diff shape invites collapsing them into one
  (`removed: []`). The first cut of this did exactly that: any reader more than ONE version behind took the
  re-baseline path, whose empty `removed` the caller read as "nothing dropped" — losing every removal in the
  gap, permanently, because a peer diffs against its own current set and never mentions a dropped cid again.
  A phantom holder is worse than a missing one: repair elects around it and never fires. So `changes_since`
  returns `Delta | Reset` as distinct TYPES, a reader that merely fell behind folds the gap into a true
  delta (its baseline is on the chain — the removals are all computable), and a `Reset` REPLACES rather than
  merges (absence IS the removal). The set-comparing predecessor could not have this bug; anything that
  trades a full compare for a delta must carry this rule with it.

---

## 8. Phases

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

## 9. What this changes, numerically

| | today (polling) | this design |
|---|---|---|
| Steady-state cost | O(N_cids) per interval, forever | **zero** (no events, no work) |
| Cost on a node death | none (until the timer notices) | O(dead node's set), once |
| Detection latency | 15 min – 2 h | **seconds** (SWIM) |
| Scaling limit | ~10⁴ cids before the period becomes absurd | churn-bound, not inventory-bound |
| Silent-loss coverage | yes (that is what the scan is for) | manifest anti-entropy |
| Liar coverage | AvailabilityProbe (per scan) | PDP sampling (K5) |
