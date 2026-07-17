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

**The general error, which is worth more than the specific bug:** repair was made event-driven while its
INVERSE was left sweep-driven. That pair cannot be stable at any scale where the sweep is the thing being
retired — the fast side always wins, and the slow side's cleanup is exactly the O(N_cids) cost this design
was built to delete. A mechanism that creates work on an event must have its cleanup driven by an event too,
or it must not create the work at all.

**Why a grace period is NOT the fix.** The obvious patch — wait before treating `Dead` as lost — only
addresses the night half. The day half still trims X's pieces as cold surplus, so the fleet keeps paying,
and the grace window buys the reduced durability of an under-replicated cid for nothing. A timer cannot fix
a mis-measurement.

**The fix is to stop conflating the two.** An absent-but-not-dead holder's pieces still EXIST; they are
merely unreachable. So:

- **Durability accounting** counts pieces that exist — including those on a known holder that is currently
  offline. Below-floor here means bytes are actually gone, and only that should MINT.
- **Availability accounting** counts pieces reachable now. Below-k here means "cannot serve right now", and
  the answer is to serve from elsewhere or wait for the holder — NOT to manufacture new pieces.
- **Provision for the expected online fraction.** If a `p` fraction of holders is typically online, the
  steady-state spread must put `k/p` pieces out so that a normal night never crosses the availability line.
  Then absence is a NO-OP, there is nothing to shed in the morning, and the oscillator never starts.
- Shedding must judge surplus against pieces that EXIST, so a sleeping holder's pieces are not re-minted at
  night and its waking ones are not trimmed by day.

This makes the returning node a non-event, which is the correct outcome: **it never lost anything.** Note
what that buys beyond efficiency: with no unnecessary mint there is no surplus, so the un-scalable
scan-driven shed is never needed to clean up after it. The scaling problem is not solved by making the
cleanup cheaper — it is dissolved by not making the mess.

**If minting on absence is ever kept, the shed MUST become event-driven too** — symmetric to `on_death`. A
return is a membership event exactly as a death is, and the holder index already knows which cids the
returning node holds, so `on_return(X)` costs O(|S_x ∩ ours|) with no sweep and no DHT resolve, just like
§4. Any design that mints on an event and sheds on a sweep is the ratchet above wearing a different hat.

**What this needs that we do not have.** Distinguishing "offline but holding" from "gone forever" requires
knowing a holder still HAS the bytes while we cannot reach it. A signed holdings manifest is a claim, not
proof, and a claim from a node that is not answering is worth even less. This is the same trust boundary PDP
(K5) exists to close, and it is why the honest sequencing is: measure the online fraction on a real mixed
fleet FIRST, then set the provisioning target, and only then consider a durability-vs-availability split in
the accounting.

**Status: OPEN. Not exercised by the current fleet** (4 always-on Hetzner nodes, where the two numbers are
identical by construction), so nothing here is measured — it is derived from the mechanisms above. The A-G
harness cannot see it either: `scenario_h_mass_death` kills nodes permanently. A diurnal scenario (kill,
wait past the scan backoff, restore, assert no net mint/shed churn) is the test that would.

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
- **A restarting node cannot republish its holdings for up to the record TTL (1h). CONCRETE BUG, unfixed.**
  `ManifestStore.last` is in-memory, so a restart resets the manifest version to 1. `announce_app` uses that
  version as the DHT record's `seq`, and `RecordStore::put` rejects `existing.seq >= rec.seq` — so a node
  that restarts meets its OWN pre-restart record (seq=N) and every republish is rejected until its version
  climbs back past N. Its holdings head stays frozen at the pre-restart manifest, and any change it makes in
  that window is invisible. A long absence self-heals (the record TTLs out after 1h and `seq=1` is accepted
  again); a quick restart does not, which is the common case. The version is a chain identity, not just a
  counter, so the fix is to PERSIST it rather than to special-case the seq. Note the diff chain itself
  survives a rewind correctly — a peer whose baseline is unreachable gets `Changes::Reset` and re-baselines
  — so this is purely the DHT seq rule, not a manifest-format problem.
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
