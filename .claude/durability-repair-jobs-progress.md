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
- [ ] P4 — Event-driven SHED: the `added`/surplus signal `check_peer` currently discards enqueues
      shed jobs; `shed_one` reachable outside the scan
- [ ] P5 — Execution-time re-check is the offset: confirm repair job no-ops on `have >= floor`
      and shed job no-ops on `have <= band`; add tests proving transient churn self-cancels
- [ ] P6 — Keep the periodic scan as backstop (still submits the same job type) — do NOT retire
      it (needs PDP/K5), just make it one more producer of the same queue
- [ ] P7 — Gate + roll + live test (kill/restore, assert queue drains, no competing sweeps)

## Decisions / notes
- 2026-07-18: mint is floor-gated and CORRECT; fault is margin+shed (see §5 correction).
- Offset = queue property, not a moving average (rejected: timescale tension). Debounce keyed on
  URGENCY.
- Repair+shed MUST be one job type / one execution path (opposite directions of "move effective
  toward the band"), not two.
