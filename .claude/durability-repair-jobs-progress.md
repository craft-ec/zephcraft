# Feature: Queued per-cid repair/shed jobs (durability §5 parts 1+2)

Source of truth for this multi-phase work. Re-read before each phase.

## Goal
Replace death-driven repair's one-giant-sweep-per-death with a **single queued job type, one
per cid**, on the shared scheduler the health scan uses — and add the **event-driven shed** as
the same job with opposite `kind`. Queue latency + execution-time re-check = the epoch offset
(transient churn self-cancels). Design: `docs/DURABILITY_DESIGN.md` §5 (1)+(2).

Part (3) — phase-diverse placement + accounting split — is OUT OF SCOPE here (needs a mixed
fleet to tune).

## Why (the bug this closes)
- Spawning `on_death` stopped it blocking the census watcher, but each death is still one task
  looping ~1200 cids holding a permit across the whole sweep. Two deaths = two competing sweeps.
- Shed only runs inside `health_scan_chunk` — no event trigger, rides the O(N_cids) scan that
  P5 retires. Event-driven shed is its mirror.

## Phases
- [x] P1 — Mapped scheduler API. KEY FACTS: `submit(key, prio, max_attempts, factory)->bool`,
      dedup by key string; NO delay param (delay = external DueQueue like the scan); scan-repair
      already submits `repair:{cid}` at Priority::Repair via EngineWork::Repair; JobCoordinator is
      NODED-level (obj reaches it only via EngineWork). Repair class was uncapped.
- [x] P3 — STAGE A DONE: death/anti-entropy repair ENQUEUES per-cid jobs. Added
      `ObjEngine::request_repair(cid)` (front door: sends EngineWork::Repair if wired, inline
      repair_cid if not) — same `repair:{cid}` key so death+scan COALESCE. `repair_our_share`
      elects+sorts (fewest-holders-first, preserved via FIFO submit order) then enqueues; giant
      sweep + budget semaphore DELETED. Repair concurrency now an explicit `set_class_cap(Repair,2)`
      preserving the old budget value. 74 tests, clippy clean. Not yet gated/rolled.
- [ ] P2/urgency — the debounce/urgency tiers are STAGE C (below); Stage A submits all at
      Priority::Repair immediately (no offset yet, but correct + coalescing).
- [x] P4 — STAGE B DONE: event-driven SHED. `EngineWork::Shed(cid)` (Eviction priority) +
      `ObjEngine::request_shed` front door + `ObjEngine::shed_cid` executor (mirror of repair_cid:
      resolve providers, compute `have`, surplus check, rendezvous-elect ONE shedder, shed OWN
      pieces down to `floor/holders` fair-share, never below floor). Trigger: `apply_delta`/
      `apply_reset` now return `(lost, gained)`; `check_peer` routes `gained` → `request_shed`
      (the surplus signal it used to discard). first_sight still shed-exempt. Drainers updated in
      main.rs AND tests/src/lib.rs. 74 tests, clippy clean. Not yet gated/rolled.
      OFFSET (part of §5.2): shed_cid re-checks `have > floor+delta` at execution, so a shed queued
      when a holder returned no-ops if the holder left again before it drained. Partial offset via
      re-check; full night-length debounce still needs part 3.
- [x] P5-safety — REVIEW-CAUGHT DATA LOSS in shed_cid, fixed (commit 97649f8). It summed raw
      stale-HIGH provider counts (shed never re-announces → records stale ~22h) with no probe →
      could destroy real pieces below the floor. Fix: probe-verify (unverifiable=0), shed ONE
      piece/invocation (bounds the epoch-boundary concurrent-shedder race to 1 piece each),
      re-announce lower count. Also fixed `shed:` misclassified as Other not Eviction. The offset
      is the probe-verified re-check: `have <= floor+delta` → no-op.
- [ ] P5-remaining — full night-length offset (debounce keyed on urgency) = §5 part 3, needs
      mixed fleet. NOT built.
- [ ] P6 — Keep the periodic scan as backstop (still submits the same job type) — do NOT retire
      it (needs PDP/K5), just make it one more producer of the same queue
- [ ] P7 — Gate + roll + live test (kill/restore, assert queue drains, no competing sweeps)

## Decisions / notes
- 2026-07-18: mint is floor-gated and CORRECT; fault is margin+shed (see §5 correction).
- Offset = queue property, not a moving average (rejected: timescale tension). Debounce keyed on
  URGENCY.
- Repair+shed MUST be one job type / one execution path (opposite directions of "move effective
  toward the band"), not two.


## 2026-07-18 — UNIFIED into reconcile (user clarification: offset per cid ACROSS providers)
The separate repair:{cid}/shed:{cid} keys meant a departure and an arrival for the same cid did
NOT coalesce (per-cid-per-provider). Fixed: ONE reconcile:{cid} key for both directions.
- verified_have(cid): probe-verified net across ALL providers (unverifiable=0), one pass.
- reconcile_cid: have<floor→repair_cid; cold surplus>band→shed_cid; in band→NOTHING (the offset).
- EngineWork::Shed→Reconcile; request_repair/request_shed→request_reconcile; JobClass reconcile:→Repair.
- on_death + anti-entropy(lost∪gained) both enqueue reconcile:{cid} → death offset by return coalesces.
- Delegates to the reviewed repair_cid/shed_cid → dispatcher, not a new destructive path.
- Test: reconcile_nets_to_noop_when_redundancy_is_in_band. Commit 8522c23. NOT yet gated/rolled.
