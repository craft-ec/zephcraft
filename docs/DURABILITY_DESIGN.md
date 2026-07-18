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

## 5. Steady-state churn — the reconcile window, and why diurnal is not special

**The concern that motivated this section, stated correctly.** Repair arms on converged `Dead` (§4). A
node going to sleep looks, at the instant it leaves, exactly like a node that died — only waiting tells
them apart. On a fleet of home machines that raised an obvious worry: does every sleep trigger a repair,
and every wake a shed, so the fleet thrashes mint-against-shed forever?

**The answer, on a GLOBAL fleet, is no — and the reason is that there is no synchronized "night".** The
network runs in every timezone at once. At any instant some regions are going dark and others are waking,
continuously, so the fleet's online population is roughly CONSTANT and the stream of departures and arrivals
is steady and balanced, not a burst. That is precisely the `O(churn)` case §4 is built for. There is no
fleet-wide midnight to survive; there is just churn, arriving at a steady rate, which the machinery below
consolidates and the floor gate mostly ignores. An earlier draft of this section framed the problem as a
single node leaving at midnight and returning at 08:00 — a *synchronized* diurnal event. That framing was
wrong: it imported a single-node, single-timezone picture into a system whose defining property is that all
timezones run it together.

**Why the floor gate makes ordinary churn cheap.** Repair and shed both read
`effective = own_pieces + Σ (piece_count of LIVE providers)`, and `repair_cid` returns early on
`have >= floor`. So a provider going offline only mints if it genuinely drops the cid BELOW the durability
floor. When a cid's providers are spread across timezones, a roughly constant fraction is always awake, so
`effective` stays above the floor as individual providers cycle in and out — the departures and arrivals
substitute for each other, and repair never fires. The mint path is correct as written; it does the right
thing precisely by doing nothing when live redundancy is adequate.

**What we BUILT, and why it is the general mechanism (not a diurnal special case).**

**(1) One per-cid reconcile job, not per-direction, not a per-event sweep [2026-07-18].** A cid whose
provider set changes enqueues a single `reconcile:{cid}` job that reads the probe-verified net `have` across
ALL its providers ONCE and moves toward the band: mint below the floor, shed one above it, nothing inside.
Repair and shed are the same operation — "move `effective` toward the band" — in opposite directions, so
they are one job (`reconcile_cid`) with one dedup key, delegating to the two reviewed executors
(`repair_cid`, `shed_cid`). This replaced death-repair's old giant per-death sweep (one task grinding ~1200
cids, measured at 2h36m live) with per-cid jobs on the shared scheduler, and it means a death and a return
of the same cid COALESCE into one net evaluation instead of a repair and a shed that each separately no-op.

**(2) The trigger is a MONITORING WINDOW, not a per-event reflex [2026-07-18].** Reconcile must not fire on
each provider change — under steady churn that would be constant work, even though most fires net to a
no-op. Instead every change accrues a per-cid signed delta (`+1` a holder appeared, `-1` a holder
dropped/died) into a `RECONCILE_WINDOW` (30s). At the window's close, only cids whose net is NON-ZERO enqueue
one reconcile; a departure offset by an arrival in the same window nets to 0, is pruned, and fires NOTHING —
not even a no-op. This is the "normalize over a period" the design turns on: over any window the fleet's
balanced churn largely cancels per cid, and only genuine net movement triggers work. `reconcile_cid` still
computes the true net from probe-verified pieces at execution; the accrued sign is just a cheap gate on
whether to look at all. The window bounds how long a genuine loss waits (≤30s, covered by erasure margin)
against how much churn it swallows; a mass death, a node's whole manifest delta, and a quick flap all
collapse into one net.

Live-validated (all four nodes, 2026-07-18): steady state fires zero reconcile-windows; a kill accrues
`-1` across ~1200 elected cids and consolidates them into a SINGLE window batch; a restore within ~90s
leaves most of those jobs to find the node back at the floor and no-op; the fleet settles with no ongoing
mint/shed thrash.

**Accounting split + `k/p` provisioning — DE-SCOPED.** An earlier version of this section made a
durability-vs-availability accounting split ("count pieces that EXIST separately from pieces REACHABLE") a
required third part. It is not load-bearing. That split only matters if a normal amount of sleeping
routinely drops cids below the floor — but under continuous, balanced, global churn with an adequate erasure
set `n`, live redundancy stays above the floor on its own, and the existing floor gate handles the rest. The
split is a tool to reach for only if measurement on a real mixed fleet shows live redundancy dipping below
the floor in ordinary operation. It is not a milestone this design is waiting on.

**The one real residual: per-cid PLACEMENT diversity.** The fleet aggregate normalizing does not by itself
guarantee that a *given* cid's providers span timezones. Today placement is `peer_source.peers()` — whoever
is reachable at publish time — which skews toward the publisher's currently-awake cohort, and that cohort
correlates with timezone. So a cid whose `n` pieces all land inside one timezone's active window has
providers that then sleep together: THAT cid can dip below the floor on a correlated schedule while the rest
of the fleet is fine. Global participation makes this rare (different cids cluster in different zones, so the
aggregate stays balanced), but the publish-time-biased placement does not eliminate it. The fix, if
measurement ever shows it mattering, is a placement nicety — spread each cid's `n` pieces across the
availability (timezone/phase) dimension the way one would spread across failure domains, using peers'
observable uptime phase — not a timer and not an accounting change. This is the honest remaining item, and
it is narrow.

**Status.** (1) and (2) BUILT, rolled, and live-validated 2026-07-18 — correct at any online fraction and
exercised by ordinary kill/restore. The accounting split is de-scoped (not needed under global continuous
churn + adequate `n`). Per-cid placement diversity is the only open item, and it is a measurement-gated
placement refinement, not a correctness gap. The current 4-node always-on Hetzner fleet cannot exercise
timezone diversity at all, so nothing here about placement is measured — it is reasoned from the global
operating premise; a real multi-timezone fleet is what would confirm it.

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
