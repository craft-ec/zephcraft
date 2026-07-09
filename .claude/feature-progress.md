# SCALE CONVERGENCE: CONN POOL + JOB COORDINATOR EXTENSION + RESOURCE MANAGER (2026-07-09, in progress)
Root cause chain proven by the capped 20-node redo + single/5-node rejoin experiments:
conn-per-request architecture → under concurrency handshakes stack (each holds MBs of QUIC state)
→ RSS balloons (zeph5: flat 240MB alone; 965MB with 4 co-rejoiners, −800MB freed in ONE 5s sample
when attempts aborted = pending-conn state, not data) → OOM cap kills → deaths re-trigger dials →
thrash. Churn↔death correlation: zeph9 3432 churn lines/8min → 3 kills; zeph6 2190 → 3; zeph7 766 → 2.
Same root as Mac flapping + noq PTO wedges. DHT already has the right pattern (conn_for cache).
User also directed: (a) resource manager to supplement the job coordinator, (b) extend coordinator
to cover ALL node jobs (today only distribute ×2 + healthscan go through it; repair runs INSIDE the
scan job at HealthScan priority — the Repair tier is unused!).

- [x] Phase 1: per-peer connection pool in Transport — DONE. Pool keyed (peer, ALPN) with
      close_reason validity + stable_id-checked evict + connect_fresh + evict_peer; cleared on
      rebind/close. All 6 request paths converted (per-request conn.close removed); DHT's private
      conns cache DELETED (delegates to the pool; attempt-1 = connect_fresh). Review found 2 real
      issues, both fixed: (1) external tokio::timeout wrapping push_piece dropped the future
      before its internal evict ran → stuck-but-open conn pooled forever; fix = timeout param
      runs INSIDE request(), evicts on timeout too (data-plane contract; pings still tolerate
      timeouts); (2) membership oneway branch swallowed the delivery-read error → now fails the
      request and evicts. headreg 3s-drain site documented as self-healing. Gates: clippy 0,
      164/164 tests. NOTE for reviewers: never wrap pooled-conn requests in external timeouts.
- [x] Phase 2: coordinator extension — DONE (commit 17723c8). Audit found: only distribute×2 +
      healthscan went through the coordinator; repair ran INSIDE scan jobs (Repair tier unused);
      publish distribution = raw spawn per publish; distribute_pending = inline loop; headreg
      replicate = spawn per write. All routed: EngineWork trigger → Encoding publish:{cid} /
      Repair repair:{cid} jobs; distribute_pending deduped Distribution job; pushstate:{shard}
      full-state-at-run-time + per-shard dirty counter (review fix: mid-push write was dropped);
      repair_cid re-checks floor + Fade gate at exec time (review fix: TOCTOU minted surplus).
      Stays direct (deliberate): membership probe/shuffle, gov tick, migrate/reshard rounds.
- [x] Phase 3: resource manager — DONE pending review. sched::ResourceGauge (budget from own
      cgroup memory.max, RSS sampler 5s): >85% only Repair dispatches, >95% nothing + inbound
      sheds (obj ingest + headreg PushState answer "busy"; senders' next pass retries). deferred
      + mem_load_pct in JobStats. Gauge off when no cgroup limit / non-Linux (Mac). Gated
      dispatch re-checks on 500ms tick. Test: gauge_gates_routine_work_but_not_repair.
- [ ] Phase 4 (acceptance): deploy fleet, rerun 20-node rejoin — PASS = census 20 converges, no
      OOM kills, churn lines near-zero, deploys fast. THEN the original stress measurements
      (writer spread, held-DB counts, remote resolve latency, reshard 8→9 under load).

# ISOLATION WATCHDOG: ENDPOINT REBIND (2026-07-09, commit 29f9ce1) — DEPLOYED to all 5 nodes
Fleet roll (binary b9f74279, watchdog string verified in binary on both server + Mac): staggered
restart zeph..zeph4, Mac binary swap + launchd bounce (transient bootstrap IO-error-5, retry OK).
Post-roll: Mac 4/4 active, census eligible=5, shards=256. Census-overview UI (e183de4) shipped in
the same roll (dashboard is include_str-embedded). Gotcha for next deploy: the release binary is
`target/release/zeph` (NOT zeph-noded — [[bin]] name); an install of the wrong path no-ops silently.
Review verdict: design sound; 1 CRITICAL found+fixed — close()/rebind() race (SIGTERM during a
wedge-recovery rebind could install a fresh open endpoint AFTER close() returned, orphaned forever);
fix = re-check `closed` before installing, close the just-built endpoint and bail. Reviewer caveat
(accepted, below threshold): dht/main cache transport.addr() once at startup — only matters on
port=0 nodes, and the Mac (the only port-0 node) is relay-dialed so its usable addr survives rebinds.
Incident (during the 19-node stress test + box freeze): the Mac's long-lived iroh endpoint WEDGED —
after the all-peers outage every recovery dial to known-alive seeds died in 3s for 10+ min while ICMP
on the same path was clean; `noq` errors `MultipathNotNegotiated` + `PTO expired while unset` (all ~5
conns died in the SAME millisecond = local/uplink path event, e.g. hotspot NAT churn). Process restart
reconnected in 15-20s, three times. Membership-level recovery can't fix it (dials go THROUGH the
wedged endpoint). FIX: (1) transport — endpoint behind RwLock + saved BindCfg; `rebind()` closes old
FIRST (frees fixed port), rebuilds identical (identity/port/relays/ALPNs), 10×500ms retries; `serve()`
re-attaches via epoch counter, exits only on `close()`; removed dead `endpoint()` accessor. (2)
membership — `wedge_rebind` (default 120s) + `isolated_since_ms`; when active view empty AND bootstrap
seeds exist AND isolation outlasts the window → transport.rebind() + re-arm seed recovery; full window
between attempts. Solo nodes (no seeds) never rebind. Also fixed pre-existing broken wire test
(Shuffle/ShuffleReply `members` field missing in roundtrip initializers). Gates: fmt/build/clippy(0)/
workspace tests green (transport 5/5 incl rebind roundtrip; membership 4/4 incl watchdog test;
healthscan 15/15 on rerun — earlier fail was parallel-load flake). Docs: ZEPHCRAFT.md §3.4+§4.1.
Memory: zeph-iroh-endpoint-wedge. NEXT: commit, then fleet roll (4 Hetzner + Mac) together with the
census-UI commit e183de4 (still undeployed). Edge accepted: a seed node with no peers of its own never
arms the watchdog (nothing to dial; wedge only ever observed on churn-prone uplinks).

# BACKGROUND-LOOP AUDIT + COMMENT-HYGIENE SWEEP (2026-07-09, commits f7e2a28 + d420794) — DONE
Follow-up to the churn incident: audited ALL 13 periodic loops for unconditional per-tick network
work. 12 clean (TTL-gated / change-gated / local-only / event-drained / bounded+cached / by-design
liveness / steady-state-empty). ONE offender: `distribute()` — an unconditional O(held) concurrent
DHT-lookup sweep every 30s (hundreds of lookups/tick on a loaded node). FIX (f7e2a28): census-gated
via the migrate_round pattern — fires once the census digest is stable 2 ticks after a change (never
during a join storm) + a ~10min heartbeat; scale()/enforce_quota() stay per-tick (no-ops when idle).
COMMENT SWEEP (d420794): purged every verifier-flagged stale comment — headreg module/field docs
(deleted shard_seed fn, WASM-validator prose → SQL/native reality), registry_net blob-era seed
formula, dead REGISTRY_SEED const REMOVED (com), sql KIND_ROOT/tracker/CAS prose, obj publish
`durable` overclaim, dht Phase-1/2 framing + tracker census claim, membership tracker-registry
docstring, noded routing_dht comment / CLI "via tracker" / "Poll the tracker" / committee mentions,
account.rs as-built note, gov.rs committee analogy. No behavior change except the distribute gate.
Deployed to all 5 nodes. Tier-0 comment debt: CLOSED.

# PEER-FLAPPING ROOT CAUSE: SELF-INFLICTED CONNECTION CHURN (2026-07-09, commit a846723) — FIXED + MEASURED
User reported consistent peer disconnect/reconnect on the Mac and (correctly, again) rejected my
packet-loss theory. CONTROLLED TEST proved it: ICMP to Hetzner = 0% loss/≤380ms WITH the node running,
while zeph pings on the same path timed out at 3s (2,860/2,915 failures = timeout; 18× "server refused
to accept a new connection" = connection pressure). Cause: our own QUIC-handshake churn — the
GOVERNANCE TICK did resolve_app (DHT lookup) + 1-2 obj.get(Drop) content fetches for EVERY census peer
EVERY 5s (Drop retains nothing → refetch forever) + unconditional publish/announce per tick; plus
fresh-connection-per-ping probes. Hetzner LAN hides it; the Mac's 260ms RTT amplifies handshakes ~100×
(the canary, not the cause — 3rd incident of this class after member-sync 10s and DHT per-op conns).
FIX: fetch_if_newer (version-gated: fetch only if announced version > local seq+1 → steady-state ticks
do ZERO content fetches), publish_if_due (announce on seq change + 10min heartbeat), tick 5s→30s,
DRAIN_TICKS 6→18 (~180s, matches slower propagation), membership ping retry-once before a failure
counts. MEASURED on the fleet (12-min window): Mac unreachable 31-64 → **3** (−95%), mark-dead → **0**
(the user's symptom eliminated), node1→Mac 23 → **1**, governance intact (seq 6).
Memory: zeph-connection-churn-flapping (the ICMP-vs-app-ping diagnostic + gate-per-tick-loops rule).

# DOCS CONSOLIDATION + PUBLIC SURFACES (2026-07-09) — DONE
One consolidated design doc + website + docs-site, all shipped:
- **`docs/ZEPHCRAFT.md`** (commit 65fcdd6): THE single reconciled design & state document (16 parts,
  ~430 lines dense) consolidating all ~20 design docs against code. Produced by a 9-domain parallel
  extraction workflow (~1M tokens read over docs+crates), synthesized, then ADVERSARIALLY VERIFIED by
  3 independent reviewer agents (numbers/mechanisms/status lenses) — verdicts "very hard to refute";
  all 7 findings fixed (job-priority order Repair>Encoding>Distribution>HealthScan>Eviction;
  NativeProgram exists-but-uncalled; readiness gate bounded-20s-not-absolute; TRACKER_* tags are
  fossils not "carried over DHT"; app-path clock = local time; delete propagation REJECTED-not-
  deferred; 17-crate inventory incl. cipher/events/sched). Part 14 = per-doc supersession map;
  maintenance rule: fix ZEPHCRAFT.md in the same change that lands code.
- **Public stats endpoint** (commit 5895253) + LIVE CUTOVER: api.zeph.craft.ec/stats was the RETIRED
  tracker serving all zeros (nodes stopped announcing; source deleted) — the website's "live network"
  section showed a dead network. Node now serves token-free CORS-open GET /stats on
  --public-stats-port (census-based node count + local store/DHT counts, tracker-compatible schema).
  Deployed: zombie zeph-tracker stopped+disabled, node1 runs --public-stats-port 9947 (Traefik yaml
  untouched), all 5 nodes rolled. LIVE: nodes 5, cids ~905, pieces ~4k, providers ~5.4k — real numbers.
- **Website zeph.craft.ec** (same commit, DEPLOYED via vercel): stats copy no longer credits the
  tracker; new STACK section (the full vertical, all live); nav/footer → docs.craft.ec/zeph; erasure
  floor corrected to as-built (4× survives 75% piece loss); lede/meta mention databases+compute.
- **docs-site docs.craft.ec** (docs-site repo commit 7309bd2, deployed): new /zeph section — index,
  architecture, storage, database, compute, registry-governance, run-a-node, faq (honest not-built
  list); root index reframed (Craftec = infrastructure + apps). Builds clean (29 pages).
- Mac launchd note: `launchctl bootstrap` can fail transiently ("Input/output error 5") right after
  bootout — wait ~5s and retry; ALWAYS check `launchctl list` after (the silent-fail lesson).

# Feature: Kademlia DHT for content routing

Replace tracker-based **content routing** with a Kademlia DHT behind the existing
`ContentRouting` trait. Per foundation §62 + user direction:

- **DHT = all content routing**: provider records (cid→holders), want records, and
  owner-keyed heads (DB root / app / manifest / meta) as **highest-seq-wins signed
  records** (no strict CAS — a DHT has no single authority).
- **Tracker, slimmed = node/relay census + DHT bootstrap** only.
- **No global content enumeration** — `content()` is DROPPED entirely. The dashboard's
  "serving N cids" already counts OUR OWN held pieces (local), not network enumeration.
  Node census stays (DHT routing table + tracker).
- **Fade** uses per-cid want lookups, not global `wanted_cids()` enumeration.

Kademlia params (foundation §3): 256 k-buckets, k=20, α=3, XOR distance on 32-byte keys,
provider records keep `addr` inline (dialable), TTL 48h / republish 22h. Reuse the existing
`SignedRecord` + `records::sign/verify` verbatim. New crate `zeph-dht`, ALPN `/craftec/dht/1`.

## Phases

- [x] **P1 — Overlay core.** DONE. `zeph-dht` crate: k-bucket table (table.rs), DHT protocol
      (proto.rs, own ALPN `/craftec/dht/1`, postcard), `DhtNode` (node.rs) with serve +
      iterative α=3 lookup + bootstrap. 9 tests green incl. a live 5-node overlay test
      (bootstrap + lookup locates a peer known only via the seed). clippy 0.
- [x] **P2 — Record store.** DONE. `StoredRecord` (generic signed key-value envelope,
      Ed25519, verified on store + return, highest-seq-per-publisher, many publishers coexist)
      + `RecordStore` (TTL, expire). `Store`/`StoreAck`/`FindValue`/`Value` messages; node
      `put` (sign → lookup K-closest → Store) + `get` (iterative FIND_VALUE, verify, merge).
      14 tests incl. cross-overlay PUT/GET (node 1 publishes, node 4 fetches). Republish is
      routing-layer policy (re-put every 22h), wired in P3/P4. clippy 0.
- [x] **P3 — `DhtRouting` impl of `ContentRouting`.** DONE (crates/routing/src/dht_routing.rs).
      provider/want/meta keyed by CID (namespaced per kind), many-coexist, monotonic-seq
      re-announce, empty-tombstone withdraw. root/app/manifest owner-keyed, highest-seq/
      version-wins, reads filtered to the owner's signature. census/enumeration return empty
      (tracker serves them in the composite). Test: providers announce/resolve/withdraw +
      coexist, head highest-seq-wins — all over a live 3-node overlay. Routing suite green,
      clippy 0.
## RETIRE THE TRACKER (re-planned 2026-07-05)

Decision: retire the tracker service AND `TrackerRouting` entirely. `ContentRouting` becomes
pure-content, `DhtRouting` its ONLY impl. `CompositeRouting` deleted (nothing to compose).
- **Content** (provider/want/meta/root/app/manifest) → DHT.
- **Census / liveness** → SWIM membership (real-time, in-network; NOT the governance chain).
- **Bootstrap** → seed peer addresses in config.
- **Relays** → relay URLs in config (already mostly there); drop the dynamic relay registry.
- **Fade** → per-cid `is_wanted(cid)` replaces `wanted_cids()` enumeration.
- **content()** → gone (dashboard is local).

- [x] **P4a — Composite.** (superseded — CompositeRouting will be DELETED, not used.)
- [x] **P4b — Trim the trait + membership census.** ContentRouting → content-only
      (drop nodes/relays/announce_node_registry/announce_relay_registry/content/wanted_cids;
      add `is_wanted(cid)`). DhtRouting: add is_wanted. Rewire census callers (obj candidate
      peers, dashboard) to membership. Fade → per-cid is_wanted. Delete CompositeRouting.
      DONE — confirmed by reconciliation 2026-07-08 (trait is 16 content-only methods, is_wanted
      required, DhtRouting sole impl, CompositeRouting deleted).
- [x] **P4c — Wire DhtRouting into noded + seed bootstrap.** DONE (flag-gated). routing_dht +
      dht_seeds config (OFF by default); DhtNode construct/serve, DHT ALPN, bootstrap from
      seeds, routing=DhtRouting, MembershipPeers (peers.rs) as the PeerSource. Republish rides
      the re-announce loop; hourly expire. VERIFIED on the Mac: flag-off identical; flag-on the
      overlay bootstraps + publish/get/health-scan work over the DHT. Reverted Mac to flag-off.
      (noq PoisonError on abrupt-shutdown is a pre-existing dependency issue, not P4c.)
- [x] **P5a-c — Migrate the cluster to the DHT.** DONE. 5-node cluster resolves + repairs
      entirely over the DHT; no tracker in the routing path. Stability hardening done (unified
      job manager, hysteresis band, record-store persistence). Tracker still CONSTRUCTED as a
      fallback + all tracker code still present.

## RETIRE THE TRACKER — code deletion (2026-07-06)

Surface map (agent): NO CompositeRouting, NO content() (both already gone). Two impls only:
TrackerRouting (delete) + DhtRouting (keep). Trait census methods `nodes/relays/
announce_node_registry/announce_relay_registry/wanted_cids` are REQUIRED (no default);
`is_wanted` has an enumerate-default that DhtRouting already overrides. Census callers:
obj RoutingPeerSource.nodes (→ MembershipPeers, already the DHT-path source), noded seed loop
(dead on DHT), sql net.rs owner_addr (needs membership+resolve), ObjEngine announce_node/relay
(drop). ~13 test files build TrackerRouting as shared routing + peer census.

- [x] **P5d-1 — Test double.** DONE (commit test:...). MemNet/MemRouting/MemPeers in zeph-testkit; 13 harnesses migrated; tracker.rs deleted; healthscan 15/0, com 55/0.
- [~] **P5d-1 (orig text) — Test double.** MemRouting (shared in-mem ContentRouting) gated
      `#[cfg(any(test, feature="test-support"))]` in zeph-routing; MemPeers (shared PeerSource)
      same in zeph-obj. Migrate the ~13 harnesses off TrackerRouting → MemRouting + MemPeers
      (ObjEngine::with_peer_source). DELETE routing/tests/tracker.rs (the only real tracker test).
      Exit: all suites green with zero TrackerRouting refs in tests.
- [x] **P5d-2 — Production rewiring.** DONE (b82c6b8). DhtRouting+MembershipPeers unconditional; owner_addr→PeerSource (remote-fetch fix); tracker construction/seed/announce removed. obj: MembershipPeers unconditional, delete
      RoutingPeerSource. noded: DhtRouting unconditional, remove tracker construction + seed loop
      + announce_node/relay calls. sql net.rs owner_addr → membership snapshot + resolve fallback.
      Exit: build green, cluster redeploy stays healthy.
- [x] **P5d-3/4/5 — DONE (aa1da52).** Deleted TrackerRouting+server.rs+registry.rs+apps/tracker; trait trimmed to 16 content-only methods (is_wanted required); DhtRouting sole impl; dead record kinds + noded --tracker/trackers removed. (dead after -1/-2).
- [x] **Restart overlay gate (e25ed94).** First scan waits for the Kademlia routing table to
      settle (not just membership) — flattened core-restart at_risk transient 182→30 peak, 7x
      less false repair. FEATURE COMPLETE: tracker fully retired, cluster DHT-only.

## Notes / decisions
- Provider records carry `addr` inline → resolve returns dialable providers (no separate
  NodeId→addr discovery needed for providers).
- DHT routing-table contacts carry `PeerAddr` → dialable during lookups.
- Heads: highest seq/version wins; single-writer-per-identity makes same-seq races rare.
- 22h republish (foundation), NOT the old 6s reannounce.


---

# Feature: Open owner-signed registry + verification substrate (updated 2026-07-07)

Two SEPARATE tracks, settled through a long design pass. **Design docs are the source of truth:**
`docs/VERIFICATION_DESIGN.md` (new), `docs/ATTESTATION_DESIGN.md` (revision banner 2026-07-07),
`docs/REGISTRY_DESIGN.md` §2.1 (patched 2026-07-07). Memory: `zeph-attested-registry-notes`.

## Settled facts (do NOT re-litigate)
- **No incident.** app-registry v2 (char-limit) is LIVE — deploying a >32-char name is rejected;
  governance is durable, the v2 SetProgram is intact. The earlier "revert" was my misread of a
  program-registry version field. Verify behaviour empirically (deploy test) before diagnosing.
- **Attestation is CONSISTENCY-only** — not authority (owner signature), not arbitration
  (governance), not durability (erasure-coded storage).
- **Open registries do NOT use attestation.** app / DB-root / manifest / meta are all owner-signed
  CRDTs (partition-by-owner, last-writer-wins per key) — they converge by construction, nothing to
  verify. Attestation is only for consistency-critical state (shared counter/quota/balance).

## Terminology convention (2026-07-07) — everything is a "program", drop "app"
Applied as a TARGETED rename sweep AFTER the attestation removal (they touch the same files; do not
run concurrently). Not a blind `s/app/program/` (that mauls `append`/`apply`/`happen`).
- **Everything the network runs is a PROGRAM** (WASM). "app" is retired.
- **Protocol Program Registry** — governance-controlled: which WASM is canonical for each protocol
  program / anchor. Old `program_registry()` (gov.rs) → `protocol_program_registry()`.
- **User Program Registry** — owner-deployed `(owner, name) → cid`, owner-signed CRDT. Old
  app-registry / `AppRegistry` / `appreg.rs`.
- **Runtime namespaces:** `protocol_program.<ns>` / `user_program.<ns>` (replacing `app.<ns>`).
- Identifier renames: `AppRegistry`→`UserProgramRegistry`, `appreg.rs`→`user_program_registry.rs`,
  `program_registry()`→`protocol_program_registry()`, `KIND_APP`→`KIND_USER_PROGRAM`,
  `announce_app`/`resolve_app`→`announce_program`/`resolve_program`, "deploy a … app"→"deploy a
  program", webui "user apps"→"user programs". Docs to sweep: CRAFTCOM, REGISTRY, VERIFICATION,
  MINIMAL_KERNEL, ATTESTATION, CLAUDE.md, webui.

## Directive (2026-07-07) — remove attestation entirely, build the anchor, rework the app
Per user: **TOTALLY REMOVE** the k-of-n committee attestation — it needs a ground-up rework as the
open-board verification substrate (`VERIFICATION_DESIGN.md`), a **replacement not an evolution**.
Rework the app-registry to the open model behind the anchor; the new validation WASM redeploys via
governance later. Keep the binary MINIMAL (`MINIMAL_KERNEL_DESIGN.md`); set up the WASM-integration
**anchor**.

**KEEP / REMOVE boundary:**
- **KEEP:** the deterministic WASM program runtime (run a program on `(prev,request)→new_state`) —
  reframed as the **anchor runtime**; the registry program *logic* (`RegistryState`/validation);
  `pda`/`registry_program_cid`/`REGISTRY_SEED`/`HeadSubmission`.
- **REMOVE:** `select_committee`, `verify_quorum`, `attest_transition`, `AttestRequest`/
  `request_attestation`, `AttestedCommit`, `CommitteeChain`, `AttestService` committee-orchestration,
  `ATTEST_ALPN` + handler, attested accounts (`noded/account.rs`), `noded/committee.rs`,
  `control.rs` `api_attestation` + committee status, appreg `coord`/`committee_status`/`mode`/
  `set_coordinator`, the attestation tests.

### Phase 1 — Rip out the attestation subsystem (keep the WASM runtime)
- [x] **Drop attestation from the app-registry WRITE path.** DONE 2026-07-07 — `register()` runs the
      program locally (no committee), `try_committee` + dead imports removed, char-limit still fires,
      build/clippy clean.
- [x] **Remove the com attestation machinery** — `attest.rs` committee/quorum/chain + `coordinate.rs`
      committee orchestration; **split out and keep** the WASM runtime + `NativeProgram` +
      `run_transition`.
- [x] **Remove the noded wiring** — `noded/committee.rs`, `noded/account.rs` (attested accounts),
      the `ATTEST_ALPN` handler (`main.rs`), `control.rs` `api_attestation`, the webui committee panel.
- [x] **Remove/rework the attestation tests** (`com/tests/coordinate.rs`, `registry_live.rs`).
- [x] **Clean appreg vestiges** (`coord`/`committee_status`/`mode`/`set_coordinator`) — the
      membership handle moves to the sync path (phase 3).
- *Gate:* MET 2026-07-07 — 5 files deleted (coordinate/account/committee + 2 tests), attest.rs split
  (runtime + pda kept, NativeProgram relocated to registry.rs). `cargo build/clippy --workspace` clean,
  27 zeph-com tests pass, char-limit `rejects_an_overlong_name` passes, 0 residual attestation symbols.
  REMAINING: webui still shows the dead `/api/attestation` panel (5 refs) — folded into the rename sweep.

### Phase 2 — The anchor (minimal kernel, the WASM-integration point)
- [ ] **Generalize `run_program` into a named ANCHOR:** the kernel resolves the anchor's program cid
      via the governance program registry and runs it (fuel-bounded) with a **native-default**
      fallback. One generic primitive (`MINIMAL_KERNEL_DESIGN §3, §6–7`) — sane default + anti-brick
      + per-epoch decision cache.
- *Gate:* an anchor resolves to its native default at genesis; a governance `SetProgram` swaps a
  WASM program; a missing/failed/fuel-exhausted program falls back to the default (never bricks).

### Phase 3 — Rework the app-registry as the first anchor consumer (open CRDT)
- [ ] **Open-registry MECHANISM in the kernel:** owner-signed rows (carry the sig), anti-entropy
      UNION-merge / LWW-per-`(owner,name)`, resolve LOCALLY. Drop `announce_app`/`resolve_app` (owner
      pointer); coalesce-to-latest; per-row storage (not the O(N) blob) at scale.
- [ ] **Validation via the ANCHOR** (governance program, native default). The real validation WASM
      (char-limit) redeploys via governance later — "the new app".
- *Gate:* resolve an app AND a DB root with the owner node OFFLINE; validation runs via the anchor.
- Extends to DB root / manifest / meta — same substrate.

## Program-account substrate — the fresh design (2026-07-07, user-confirmed)

REVAMP: not registry-specific. Build the GENERIC substrate; the registry is one consumer.
`account = pda(program_cid, seed)` → a single-writer account. **THE PROGRAM IS THE WRITER** —
its deterministic execution IS the write authority (validates the request, decides new state).
NO owner key, NO committee, NO attestation, NO gossip. Durability = CraftSQL/CraftOBJ (content is
erasure-coded — the DB *is* the durable layer, so no replication). Multi-account by seed (as many
as you want, any purpose). Writes to SQL + object. Reads direct (derive address → read).
Aligns with MINIMAL_KERNEL: kernel = the account mechanism; each use = a program on top.

The build is a SUBTRACTION from the deleted `account.rs` (recovered from 634ee25^) — strip the
committee, leaving a pure program-executed writer.

- [x] **Step 1 — the substrate + RPC/CLI.** DONE 2026-07-07. `crates/noded/src/account.rs` = `ProgramAccountStore`
      (WRITTEN): `open(obj,data_dir)`, `advance(program_cid,seed,request)` (run program → persist →
      publish durable content), `resolve`. No identity/routing/committee. Wiring (mod + construct +
      `program-advance`/`program-resolve` RPC/CLI) in progress. Gate MET: `program-advance`/`program-resolve` RPC+CLI wired (control.rs, main.rs); build + clippy clean, 0 warnings; char-limit test passes. appreg untouched.
- [x] **Step 2 — registry as consumer.** DONE 2026-07-07. Migrate the registry to `store.advance(REGISTRY namespace,
      seed, submission)`. NOTE the account address must derive from a STABLE program-namespace id,
      not the governance-upgradeable cid (else an upgrade orphans the state) — resolve the executing
      program separately from the address. State moved to accounts/<pda>.state (fresh on redeploy). `appreg`→`programreg`,
      `AppRegistry`→`ProgramRegistry`; thin store consumer; store `advance(program_id, code_cid, ...)`
      splits stable address from executing code; deploy path fully off-DHT (version via
      `current_version`, announce dropped). Invoke cross-node keeps a KIND_APP fallback until 4b.
      Build/clippy/27 com tests clean.
- [ ] **Step 3 — SQL-backed account state** (CraftSQL DB per account, `SELECT` resolve) — replaces
      the state blob; the query surface + per-row scaling.
- [x] **Step 4 (4b) — non-DHT cross-node resolution. DONE 2026-07-07.**** Governance/config
      `registry_writer` (default None → self-writer). One authoritative writer holds the global
      registry account; non-writers forward Submit + query Resolve over a new REGISTRY_ALPN
      (/craftec/registry/1), mirroring the removed committee ALPN request/serve pattern. Closes the offline-owner gap. registry_net.rs (ALPN + client), programreg serve/
      writer-dispatch, main.rs wiring. Build/clippy/27 com tests clean. FOLLOW-UPS: (1) resolve has
      no cache — queries the writer each time; (2) current_version is still LOCAL, so a NON-writer
      RE-deploy computes a stale version (first deploys fine); make it cross-node or deploy on the
      writer. Original note: — how a reader gets another account's latest root
      without an owner-announced DHT pointer (the one genuinely open piece). Today: local resolve +
      durable publish; cross-node deferred.
- [x] **Step 4 (4c) — DETERMINISTIC PER-EPOCH WRITER ELECTION (rotating writer). DONE 2026-07-07.**
      Replaced the fixed `registry_writer` config with a computed rotation: `writer(epoch)` = the
      eligible member (self + membership.active) with the smallest `blake3(epoch_le ‖ node_id)`;
      `epoch = clock.now().millis() / EPOCH_MILLIS` (30s). `is_writer`/`writer_addr`/`current_writer`
      are computed, not configured. HANDOFF: on becoming a NEW epoch's writer, `ensure_current()`
      fetches the previous writer's full state via `RegistryReq::GetState`→`RegistryResp::State`
      (new `ProgramAccountStore::put_state` adopts it) before advancing/resolving. Removed the
      `registry_writer` config field + `writer` struct field; clock passed into `open`. Election in
      `programreg::elect`/`current_writer`; handoff in `programreg::ensure_current` (called at top of
      `advance_local`/`resolve_local`). Build/clippy/com tests clean.
      EDGE CASES (accepted, not over-engineered — also inline code comments): (a) clock-skew races at
      epoch boundaries can briefly yield two writers → a write may be lost in that window; (b) if the
      previous writer is unreachable at handoff, keep local/last-known state (best-effort); (c) the
      FULL state is transferred each rotation — fine while small; later hand off the cid + fetch lazily.

## Track B — Verification substrate (the attestation REWORK, deferred)
Fresh ground-up per `VERIFICATION_DESIGN.md`: open request board + cooldown-rotated verifiers;
`verify` (k=1) / `attestation` (k-of-n / whitelist / open); pure-function boundary; no
self-verification. **Nothing of the removed committee code is reused except the WASM runtime.**
Build only when a consistency-critical app needs it. Deferred layers: Sybil-weighting, credit economy.

## Non-issues (do NOT re-open)
- Committee-chain re-genesis: **MOOT** — the committee is being removed.
- Persisting `AttestedCommit` / verify-against-chain: N/A — removed.
- A "durable DHT head record": **rejected** — heads live in the open registry, not a pinned DHT record.

## Registry follow-ups (2026-07-07) — the distributed registry is BUILT + committed
Full design as-built: `docs/REGISTRY_DESIGN.md §0`. Substrate + sharded rotating-writer registry +
cross-node resolution shipped (zephcraft commits: substrate, rework, rotating writer, sharding).
- [x] Non-writer re-deploy version — DONE (current_version routes to the shard writer).
- [x] Boundary-race — grace window (2s) shipped; deterministic boundary while skew < grace.
- [x] **Read-caching** — DONE (commit 3e2683a). `ResolveCache` TTL's (RESOLVE_CACHE_TTL_MS=3s) the
      resolved `(rtype,owner,name)→(cid,version)` for NON-replica reads (a replica reads authoritative
      local state); `register()` invalidates the key (read-your-writes). Extracted w/ injected clock,
      unit-tested (TTL, key-isolation, invalidate). Takes a hot shard's writer tens→thousands of readers.
- [ ] **Live cluster test** — redeploy the new binary; deploy an app on node A; resolve it from node
      B with A OFFLINE; confirm it resolves via the shard's rotating writer (no DHT). The real proof.
- [ ] **SQL-backed per-shard state** — today each shard's state is a postcard blob; move to a
      CraftSQL DB per shard (SELECT resolve, page-level durability, per-row scale).
- [~] **Dynamic re-sharding** — the one hard bit: changing the shard count on a live network without
      dropping keys, via power-of-two split/merge (bits→bits±1). Phased:
      - [x] **B1 routing foundation** (commit 6c316a8) — SHARD_COUNT const → runtime `shard_bits` field +
            `shard_count()`; `shard_of(owner,name,bits)` routes to the LOW `bits` of the key hash
            (bits=8 == old %256, behavior-preserving, NO cutover). Low-bit routing makes split LOCAL:
            shard s's keys go only to children s and s|(1<<bits). Live count in RegistryStatus + dashboard.
            Unit-tested prefix-stability invariant. All ShardKey sites still fixed at shard_bits=8.
      - [x] **B2 cluster agreement on `bits`** — DONE. `shard_bits` is now a GOVERNED value, agreed
            cluster-wide via the governance chain (minimal-kernel: policy in governance). Built on the
            pre-existing inert `GovAction::SetConfig{key,value}` stub: added `ConfigRegistryState`
            (com/registry.rs, mirrors ProgramRegistryState — i64, monotonic-version upsert),
            `GovernanceChain::config_registry()` fold (gov.rs), `GovernanceChainStore::resolve_config()`
            (governance.rs), and a `set_config` arm to `parse_gov_action` (control.rs `gov-propose`).
            `HeadRegistry::shard_bits` is now an async governance read (fallback DEFAULT_SHARD_BITS=8,
            clamped to [1, MAX_SHARD_BITS=12] so a bad value can't blow up the O(2^bits) loops); read
            ONCE per op and threaded into `shard_of`. Transition window: the key-routed wire requests
            (Submit/Resolve/CurrentVersion) now CARRY the submitter's `bits`, and the writer routes with
            the SUBMITTER's bits (not its own), so a `shard_bits` change in flight can't split-route a
            key. Behavior-preserving at bits=8 (governance unset → default 8 → identical routing). Unit
            tests: config-registry upsert/resolve/stale + chain SetConfig fold. WIRE CHANGE → roll ALL
            nodes version-consistent before deploy. NOTE: at a FIXED bits this is fully correct; it does
            NOT yet migrate state on a bits change — that's B3 (state doesn't follow until then).
      - [x] **B3/B4 ONLINE RESHARD — BUILT (2026-07-08).** (Superseded the brief wipe-and-restart
            close-out from commit b58f9c9: the user first said "wipe is fine" — I over-narrowed to the
            cutover question — then clarified they DO want live online resharding. Built it.) A live
            cluster now changes `shard_bits` via governance with NO wipe; keys migrate. Three tested
            batches (one commit):
            - **A — addressing:** `ShardKey` carries the shard-count GENERATION (`bits`); `shard_seed`
              folds it, so `(rtype,8,5)` and `(rtype,9,5)` are DISTINCT accounts (a reshard reads the old
              generation and writes the new without clobbering). Election (`replicas`) deliberately
              ignores `bits`, so a shard number keeps a stable replica set across generations (parent `s`
              and child-0 `s` share replicas → migration locality). `GetState`/`PushState` wire carry
              `bits`. Behavior-preserving at fixed bits.
            - **B — split/merge:** `reshard_round` (new anti-entropy job in the 10s serve loop, gated on
              a persisted per-node generation marker `GEN_MARKER_SEED` so it's a no-op while the count is
              stable) re-buckets every head this node holds at the OLD generation into the NEW
              generation's accounts (pure `rebucket_entries` + `RegistryState::merge_entries` in com) and
              pushes to the new owners. Merge-forward (old generation left intact → both resolve during
              the window), idempotent, at-least-once (marker saved after a full pass). Handles
              grow/shrink/multi-step uniformly (re-routes each key at the target count).
            - **C — transition reads:** `resolve_entry` refactored to a per-generation `resolve_at_bits`;
              on a miss at the current generation it reads through to the ADJACENT generation (bits±1), so
              a resolve survives the in-flight migration window.
            Tests: `shard_seed_is_distinct_per_generation`, `rebucket_routes_every_entry_and_splits_parent_into_two_children`
            (+ the earlier routing/clamp tests). Build+clippy+com(35)+workspace green. WIRE CHANGE (bits on
            GetState/PushState) → roll all nodes version-consistent.
            KNOWN WINDOW (documented, accepted pre-prod): a write landing on the OLD generation AFTER a node
            has migrated isn't swept forward again — visible only to old-count readers until its writer
            moves to the new count (bounded by governance-propagation seconds; softened by the read-through).
            NOT YET (future, if needed): continuous re-bucketing until the old generation quiesces; old-gen
            account GC after a reshard settles; a live-cluster grow-then-shrink integration test on hardware.
      - [x] **B3/B4 PROVEN ON THE LIVE CLUSTER (2026-07-08).** Deployed all 5 nodes (4 Hetzner + Mac) on
            the reshard binary + added `gov-propose --set-config key=value` CLI (commit ffabafe). Ran the
            grow: `gov-propose --set-config shard_bits=9` on the Mac governor → governance propagated to
            all 4 Hetzner nodes (gov seq 0→1) → each node's reshard_round split 8→9 → node1 shard_count
            256→512 cluster-wide (~50s) → the pre-registered `reshardtest` head (cid 0623371b) STILL
            resolved cross-node from node2/3/4 AND appeared in entries_global at bits=9 (i.e. physically
            re-bucketed into its gen-9 account, not just read-through). No wipe, no downtime for the key.
            NOTE the binary upgrade itself performed the one-time seed-format cutover (old no-bits accounts
            orphaned → the pre-existing 49 heads went unresolvable; expected, user pre-approved wipe). Fresh
            deploys on the new binary migrate cleanly.
      - [x] **SHRINK (merge) ALSO PROVEN LIVE (2026-07-08).** Grow-then-shrink now fully validated.
            Deployed `mergetest` at bits=9 (exists ONLY in a gen-9 account), then `set-config
            shard_bits=8`: governance propagated to all 4 Hetzner nodes in ~10s (the seq 1→2 transition —
            fast, since the announce-version fix lets it supersede), shard_count 512→256, and `mergetest`
            (a) resolved cross-node AND (b) appeared in entries_global at bits=8 — proving it was physically
            MERGED from its gen-9 account down into a gen-8 account, not merely read-through. `reshardtest`
            regression-clean. So both directions work: split (parent→2 children) and merge (2 children→parent).
      - [x] **GOVERNANCE PROPAGATION BUG found + fixed during the live test (commit b14461d).** The live
            test EXPOSED a deterministic (NOT network) bug: `governance publish()` announced the chain head
            at `seq.max(1)`, flooring both genesis (seq 0) and the first change (seq 1) to DHT record
            version 1. The DHT record store rejects an equal seq (record.rs: `existing.seq >= rec.seq`), so
            the seq-1 record never superseded the genesis record → peers forever resolved genesis → the
            FIRST governance change (0→1) never propagated. (I initially misattributed this to the Mac's
            relay flakiness; user correctly rejected that — it was code. See memory
            attribute-failures-to-code-not-environment.) Fix: announce at `seq + 1` (monotonic, never 0).
            After redeploying the fixed binary to the Mac (sole seq>0 publisher), all 4 Hetzner nodes
            adopted seq 1 within ~50s and the reshard fired. Higher transitions (1→2, …) were never
            affected; only the 0→1 step.
- [ ] **Fuller boundary hardening** — replace the grace heuristic with a short writer lease if
      clock-skew guarantees prove insufficient in practice.
- [ ] **rows()/summary()** are now per-node partial views (only shards this node writes) — a proper
      network-wide snapshot would query across shard writers (UI concern, low priority).

## Cluster test PASSED (2026-07-07) — writer-offline gap CLOSED
Live 5-node cluster (4 Hetzner + Mac governor), all on the new binary. Validated end-to-end:
- [x] **Cross-node resolve** — deploy on node1, resolve from node2/3/4 (baseline). Works.
- [x] **Offline-owner resolve** — stop node1 (owner+writer), resolve node1's program from node2/3/4 → all return the correct cid. THE GAP IS CLOSED. (First cluster run returned "not found"; fixed by K-successor replication + resolve fallback.)
- [x] **Replication confirmed** — a deploy's state lands on K=3 nodes (verified via accounts/ state files).
What made it work (all committed): native default (fresh net self-starts) + type-in-seed + K-successor replication (writer rotates among a stable K, push-on-write, merge-on-takeover) + resolve robustness (3s request timeout + self→writer→replica fallback).
### Findings from the live run (real, worth remembering)
- **Heterogeneous binaries break the registry**: the Mac node, left on the OLD binary, stayed in membership, got elected registry writer for a shard, and its incompatible ALPN + missing state made ALL resolves for that shard fail. Fix = keep the cluster on one binary (updated the Mac, kept its governor identity). Rollout lesson: registry participants must be version-consistent.
- **node4 transiently "not found"** right after the kill = membership convergence lag; resolved once SWIM dropped node1 (the fallback then reaches a live replica). Expected.
### Still open (NOT the registry's job / minor)
- [ ] **Content durability with < 8 nodes**: the 16KB program WASM was below the 8-peer erasure floor on a 4-node cluster, so it lived only on the owner → `invoke` (which fetches content) can't run it offline. Registry resolve is fine (that's why we test with the resolve-only CLI). A real network (≥8 nodes) replicates content durably. Separate from the pointer work.
- [ ] Mac node is one commit behind (d4be8de vs 84d17d6) — ALPN-compatible, functional; update when convenient.
- [ ] Read-caching still deferred (resolve now reads locally when self is a replica — partial).

## Compute execution — unified runtime (2026-07-07, DESIGN pinned)
Design doc written: `docs/COMPUTE_EXECUTION_DESIGN.md`. Settled (with the user) that the two runtimes
(transition `AttestedRuntime` + capability `Runtime`) are an ACCIDENTAL split, not two program
classes — the registry (a protocol program) legitimately wants CraftSQL, and SQL is deterministic,
so the real boundary is `clock`/randomness, not `sql`/`obj`. TARGET: ONE runtime + per-program
capability grant; consensus programs get the deterministic subset (no wall-clock/random); apps get
the full set. Industry-standard (WASI/wasmtime, CosmWasm, Substrate, EVM — one VM, determinism by
denying non-determinism, consensus/block clock). Phased migration in the doc §11:
- [x] **Phase 0 (cosmetic, do first):** rename `AttestedRuntime`→`TransitionRuntime` (attest.rs→transition.rs, AttestCtx, ATTEST_MODULE); scrub stale "committee/attested" comments in registry.rs + apps/registry-wasm; DELETE orphaned `apps/counter-wasm` demo. No behavior change. DONE — confirmed by reconciliation 2026-07-08.
- [x] Phase 1: unified host surface + link-time capability binding (default = deterministic profile). DONE — confirmed by reconciliation 2026-07-08.
- [x] Phase 2: absorb capability runtime's sql/obj/clock/caller as grantable; reconcile guest ABI. DONE — confirmed by reconciliation 2026-07-08.
- [x] Phase 3: re-point `zeph invoke` (read) + substrate `advance` (write) onto one runtime; retire capability Runtime. DONE — confirmed by reconciliation 2026-07-08.
- [x] Phase 4: consensus clock (time from request HLC for deterministic profile). DONE — confirmed by reconciliation 2026-07-08.
Note: the counter deploy timeout I hit was ALSO fixed (request timeout 3s->8s, commit 73eda29) but
the Hetzner cluster still runs the pre-that binary; redeploy when convenient.

### Phase 2 DONE (2026-07-07) — runtime merged into one grant-gated async runtime
- 2a (d7dc10a): transition runtime -> async + TransitionCtx extended (caller/app_ns/backend). Behavior-preserving.
- 2b (bdeb2e9): ported sql/obj/caller/clock as GRANT-GATED host fns; clock=WallClock (per-node HLC, non-det), deterministic() dropped Clock; backend=Option (None->-1, no panic); run_program(ctx) core + run_transition convenience; ABI = run()->()+commit (2c satisfied). Capability Runtime + invoke.rs UNTOUCHED.
- The transition runtime is now THE unified runtime (10 host fns, grant-gated). 38 com tests pass incl. new capability gate tests.
- [x] Phase 3 DONE (181bae7): migrate `zeph invoke` (InvokeService) + substrate onto the unified runtime; DELETE the capability Runtime + its bind_host_functions; invoke reads committed output (not i64). This is where the old runtime finally goes.
- [x] Phase 4: consensus clock (deterministic Clock from request HLC) + gate WallClock to full profile. DONE — confirmed by reconciliation 2026-07-08.

### Phase 3 DONE — ONE runtime. Capability Runtime deleted (-418 net lines). invoke returns committed bytes (run()->()+commit); 3 integration tests migrated to the new ABI + pass. Only Phase 4 (consensus clock) remains.

### COMPUTE_EXECUTION DESIGN COMPLETE (2026-07-07) — phases 0-4 all built + committed
One WASM runtime; per-program capability grant; deterministic subset (clock=consensus/ctx.now, wall_clock=app-only); capability Runtime deleted; invoke returns committed bytes. Commits: 1e9a9ba(0) 76dabef(1) d7dc10a(2a) bdeb2e9(2b) 181bae7(3) 43d2ebf(4). All com tests green. Remaining future work (separate): verifier re-run reproducibility (persist now in request), a real capability-app demo, and the invoke ABI is now committed-bytes (any old app must use commit).

### Deploy/write speed FIXED (2026-07-07): 10s -> ~40ms, all 5 nodes updated
Root cause (found by INSTRUMENTING, after several wrong registry guesses): CraftOBJ publish awaited join_all of every piece-push, so any publish (deploy wasm, registry state, CraftSQL page-commit) blocked on the slowest peer — the Mac on a hotspot relay stalled it ~10s. Chain of fixes (all committed + deployed to all 5 nodes):
- 4a90ca5 fire-and-forget replica pushes; 7a548e0 PUSH_TIMEOUT 10s->3s + async registry publish; 8ee918c deploy wasm publish backgrounded; 9fef5df exclude slow (rtt>150ms) peers from writer/replica election; b749c49 fire-and-forget apps_add (the app-index CraftSQL write = the real 2.4s bottleneck).
- fcc07e7 THE ROOT FIX: CraftOBJ publish retains locally SYNC then SPAWNS distribution + returns the cid immediately (cid is BLAKE3, retain is local, distribution is only for durability = async). push_piece/request became free fns; 14 tests adapted with bounded polls. Now EVERY write is fast, not just deploy-path callers.
Note: the per-caller fire-and-forget spawns (deploy wasm, account.rs publish, apps_add) are now redundant given fcc07e7 but harmless; could be simplified later. Also: DEPLOY_TIMING instrumentation was removed. Pre-existing noq QUIC teardown SIGABRT under parallel obj tests -> use --test-threads=1 (already the project rule).

### DB roots + manifests on the registry substrate — DONE + PROVEN (2026-07-07), all 5 nodes
Decision settled: foundation §62 A3 (DB roots off the registry) SUPERSEDED by REGISTRY_DESIGN §2.1 — its objection assumed the ATTESTED registry's quorum bottleneck, which is void now the registry is a sharded rotating-writer CRDT (per-owner keys never contend). Commit 081a272.
Phase 1 (57574f0): CraftSQL root+manifest heads publish/resolve through the HeadRegistry (RT_DBROOT/RT_MANIFEST) instead of the DHT KIND_ROOT/KIND_MANIFEST path. programreg register/current_version/resolve_entry take an rtype; resolve surfaces (cid, version); resolve(owner,name) kept as an RT_PROGRAM shim. New registry_heads.rs: RegistryRootStore/RegistryManifestStore impl zeph_sql's RootStore/ManifestStore over the registry (stale-version->Conflict; single-writer LWW-by-seq, prev ignored — the DHT backend already ignored prev). main.rs builds the registry before CraftSQL, drops the redundant reannounce_heads.
Rename (72508cd): ProgramRegistry->HeadRegistry, programreg.rs->headreg.rs (holds programs+roots+manifests now).
PROOF on the live cluster: a fresh guestbook2 DB counted 1,2,3 across invokes, then RESTARTED node1 (clears the in-memory root cache) and the next invoke returned 4 — the DB reopened by resolving its root through the registry, not the DHT.
- [x] REMAINING — phase 2 (cleanup, not wired to anything live): delete the now-dead DHT publish_root/resolve_root/withdraw_root/publish_manifest/resolve_manifest (ContentRouting trait + dht_routing impl), RootRecord/ManifestRecord/RootPayload/ManifestPayload/KIND_ROOT/KIND_MANIFEST, RoutingRootStore/RoutingManifestStore in sql, CraftSql::reannounce_heads; adapt testkit mock + obj/tests/healthscan.rs (DHT root-CAS test). DONE — confirmed by reconciliation 2026-07-08 (dead root/manifest DHT funcs, records, payloads, KIND consts, and Routing*Store all removed; only historical doc mentions remain).

## SHARD-PAGE ERASURE DURABILITY RESTORED (2026-07-09, commit 2942cf3)
Dropping ObjDurable from the shard engine (during the SQL-registry build) was a durability REGRESSION
(the old blob registry erasure-coded every state via publish_system) AND a band-aid over the real bug:
the shard-DB namespace `reg/<rtype>/<bits>/<shard>` contained SLASHES, so CraftSQL's per-DB durability
sidecar path `store_dir/<owner16>_<ns>.gens` became a NESTED path whose parent dirs don't exist →
save_manifest failed 'No such file' → the durability sweep failed → CraftDb::write propagated it (`?`)
and failed the write. ObjDurable-off masked it (sweep returns early). User DBs use slash-free namespaces,
so never hit it. FIX: ns_of slash-free (`reg_<rtype>_<bits>_<shard>`) → flat `.gens` sidecar → re-added
with_durable(ObjDurable). Shard pages now get default erasure durability (k=8/n=32 changed-page coding +
distribute + repair) on top of K-replica row-push. Proven single-node: deploy succeeds w/ durability,
`.gens` sidecars written, resolve works. Namespace change = a cutover (wipe, accepted).
FOLLOW-UP BUG found while verifying erasure (commit 1a55f00): `shard_db` created a DB + published a
root on ANY access including READS, so read paths (resolve/sql_state/serve handlers) created empty DBs
+ roots for every accessed shard; the held-index backfill then counted those roots and snowballed
`held` toward all 2^bits shards each restart (observed ~768 DBs, writer_shards≈full) — DEFEATING the
O(held) loops. Fix: `shard_db_existing` opens only if a root exists (no create); all READS use it,
only sql_upsert (a real write) creates + held_add. CLUSTER (wiped fresh, all 5 rolled): 0 DBs before
any deploy, 1 DB per replica after one deploy (not ~768), resolve works, `.gens` written (erasure
active). O(held) is now genuinely effective + erasure durability restored. DONE.

## REGISTRY READINESS GATE — post-restart resolve/register transient (2026-07-09, commit 402f26d)
MEASURED first (user's instinct confirmed): in steady state deploy→resolve is instantly consistent from
every node; the "not found" transient occurs ONLY in the post-restart convergence window — a freshly
(re)started node's census is still growing, so its writer election differs from the settled cluster →
it routes resolves to the wrong node (miss) or lands registers on the wrong writer. FIX (mirrors the
health-scan restart gate the user pointed to): a one-way `ready` latch flips once the census member
count has been UNCHANGED for READY_STABLE_SECS(10) (bounded by READY_MAX_SECS(90)); register/resolve/
current_version `wait_ready()` first (bounded READY_WAIT_SECS(20); a no-op once ready). PROVEN LIVE:
restart node2 → immediate resolve WAITED 16s then returned the CORRECT cid (was: instant wrong
"not found"); once converged, resolve is instant again (0.022s, no steady-state regression). eligible=5.

## O(shards)→O(held) REGISTRY LOOPS (2026-07-09, commits f4db195 + 7e3e247)
Lifted the last scaling ceiling: status/migrate_round/sweep_generation/gc_generation/rows/summary/
local_head_rows scanned all 2^bits shards (is_writer/is_replica per shard) → O(2^bits). Now a
PERSISTENT held-shards index (the (rtype,bits,shard) set this node actually has a DB for) drives
them → O(held). Empty shards have no state, so skipping them is correct. Persisted IMMEDIATELY on
first-write-per-shard (once per shard, not per write — hot path unaffected) + on GC removal, under
HELD_MARKER_SEED; lazily loaded once. `writer_of()` hoists eligible() out of the loops.
`backfill_held_if_needed()` (7e3e247): one-time O(2^bits) probe of shard-ROOT pointers on first boot
after the upgrade (or a fresh node) so existing shard DBs from the prior binary aren't dropped from
the dashboard — uses shard_roots.resolve (account read, no DB creation), persists so it never repeats.
PROVEN single-node: status head-count correct, survives restart (held loaded from disk), reshard 8→9
sweeps held shards to gen-9 + status still counts them. CLUSTER DEPLOY + VERIFIED (all 5 rolled): the
backfill repopulated held from existing shard DBs — `sqltest` (deployed pre-upgrade) is back in the
global entries AND resolves from all 4 nodes; status program_heads correct. DONE.

## SQL-BACKED REGISTRY (2026-07-08, building) — docs/SQL_REGISTRY_DESIGN.md
Replace the per-shard `RegistryState` postcard blob with a per-shard CraftSQL DB, so registry
write/resolve/replicate/durability scale O(1)/O(changed) not O(rows-in-shard). Motivated by the
target topology (thousands of nodes, ~80% NAT readers, ~20% writer backbone) where blob
write-amplification + whole-shard replication flood the scarce writer tier. Decisions settled this
session: granularity = **DB-per-shard** (Option 1 — preserves the sharding/election/reshard model;
fine at scale where each writer holds bounded substantial DBs; only wasteful at tiny scale, accepted);
validation = **native** (drop the governed-WASM validator — mechanism not policy, memory
[[registry-native-validation-not-wasm-hook]]); recursion broken by a **blob-backed RootStore** (shard-DB
root cid stored in the ProgramAccountStore account, pages in CraftOBJ). Full design in the doc.
- [x] **P1+P2+P3 — DONE (commit 376daab), done coherently in one pass since storage + replication +
      reshard are tightly coupled.** shard_root.rs (blob-backed RootStore breaking the recursion); a
      dedicated CraftSql engine (ns `reg/<rtype>/<bits>/<shard>`) + per-shard DB cache; register =
      version-guarded upsert, resolve = indexed SELECT, current_version/status/rows/entries = SELECT,
      GetState = SELECT*-as-RegistryState (wire DTO unchanged), PushState = row upsert, ensure_current
      takeover = GetState→upsert. Validation NATIVE (sig + name char-limit). Row-level replication: a
      write pushes a 1-row RegistryState (scale win). No ObjDurable on the shard engine (durability =
      K-replica row-push; write path never blocks on/fails from sync erasure — this fixed a single-node
      deploy failure the sync sweep caused). Blob persistence + shard_seed + WASM advance path removed.
      PROVEN single-node: deploy, v1→v2 upsert, resolve, restart persistence, online reshard 8→9 (rows
      swept gen-8→gen-9, regshards 12→19). Wire unchanged (no version-consistency break).
- [x] **P4 — cluster deploy + live re-test. DONE + PROVEN.** All 5 nodes rolled to the SQL binary
      (cutover = wipe, old blobs ignored; program_heads 0 fresh). Cross-node: deploy `sqltest` on node1
      → resolves from all 4 nodes. Offline-owner: node2 served `sqltest` with node1 DOWN → the row was
      replicated via row-push. Cluster reshard 9→8 (seq 6, shard_count 512→256 in ~20s) → `sqltest`
      resolves from all nodes after the SQL sweep (rows moved gen-9→gen-8 shard DBs cross-node). Cluster
      rests at bits=8/256, gov seq 6. FEATURE COMPLETE.
      FOLLOW-UPS (not blocking): erasure-durability for shard pages as a best-effort background layer;
      the user-DB app-index (`apps`) durability warns on single-isolated nodes (pre-existing, unrelated).

## RESHARD ROBUSTNESS — drain + GC (2026-07-08, commit 4abf6a5, PROVEN LIVE)
Closed the two deferred reshard gaps. `reshard_round` no longer does a single merge-forward pass;
after a generation change it DRAINS the old generation: keeps re-sweeping old→current for
`DRAIN_TICKS` (6 ≈ 60s, >> the ~20s governance-propagation window) so a write that lands on the old
generation from a straggler still on the old count is carried forward (closes the "late write" gap),
THEN GC's the old generation via a new `ProgramAccountStore::clear` (deletes the local account state
files) so old generations don't accumulate on disk (closes the "GC" gap). Drain state is in-memory
`(old_gen, ticks)`; a restart mid-drain just leaves the old gen un-GC'd (harmless — reads resolve at
the current generation). `sweep_generation` extracted from the old inline body (idempotent LWW merge).
PROVEN LIVE on the 5-node cluster (deployed all 5): a reshard 8→9 with a file-set diff of node1's
`accounts/` showed 4 pre-existing gen-8 account files DELETED after the drain window + 1 new gen-9
added (63→60 files), while `mergetest` resolved throughout. Count now trends DOWN across reshards
(64→63→60) instead of accumulating. Registry holds few non-empty accounts (2 programs), so the
magnitude is small; the mechanism is confirmed.

## GOVERNANCE PROPAGATION HARDENED — census-based tick (2026-07-08, commit 7679b68)
Root-cause fix behind the seq 0→1 propagation bug (whose proximate symptom was the announce-version
floor, commit b14461d). `GovernanceChainStore::tick()` pulled peer chains only from `snapshot().active`
— the bounded (~5), per-node-divergent HyParView active view — so a governor absent from a node's
active view was never pulled and its change never reached that node. Same active-view limitation class
already fixed for registry election. Fix: pull from `census()` (the converged, union-merged member set)
∪ the current governors (the SOURCE of every change; a flaky/relay-only governor can drop out of the
census at the TTL edge, so include the ids explicitly — `fetch` resolves a peer head via the DHT, no
direct peering needed). PROVEN LIVE: after rolling all 5 nodes, a `set-config shard_bits=9` (seq 2→3)
propagated to all 4 Hetzner nodes + resharded 256→512 in ~20s. O(targets) fetches/tick (fine at
10s–100s nodes; digest/sampling is the scale follow-up). Cluster rests at bits=9.

## CONVERGED MEMBERSHIP + registry election fix + dynamic-sharding groundwork (2026-07-08)
Root cause (19-node live scaling test): registry `eligible()` elects over the size-5 HyParView ACTIVE view (partial + per-node-divergent) -> caps at ~6 writers + INCONSISTENT shard->writer assignment above ~6 nodes (split-brain, not a throughput cap). Fix = elect over a CONVERGED member set. See docs/STATE_AND_ROADMAP.md §5 + memory zeph-registry-active-view-election-cap.
- [ ] Phase 1 — converged membership: add a `members` map (node_id -> {addr,last_heard}) to the membership crate; anti-entropy it via a new `MemberSync` gossip round (union + max last_heard); each node re-asserts self each round; `census()` = members alive within TTL. Deaths propagate by aging out (no SWIM suspect/incarnation yet — acceptable; slower death detection). NOTE: full-map gossip is O(N) — fine for 10s-100s of nodes, needs digest/SWIM-piggyback for 1M (future).
- [ ] Phase 2 — election over census: headreg `eligible()` uses `membership.census()` not `snapshot().active` (writer + replica election both). DROP the rtt-exclusion (local rtt breaks election consistency; slow-writer handled by resolve fallback + the tail fixes; a converged health signal is future). Verify on cluster: eligible grows to full N.
- [ ] Phase 3 (groundwork) — dynamic sharding: make SHARD_COUNT a governed/converged value (needs K1 config registry) so all nodes agree; design consistent-hashing split/merge + rebalance. Full auto-resharding = later.

### Phase 1+2 LANDED + PROVEN (2026-07-08, commit 50f34ea)
19-node re-run: eligible 6 -> 19 (census-based election spans the cluster), writer_shards 41 -> 15 (shards spread across all 19, not ~6), active view stayed 5 (census is decoupled). Election consistent across nodes (both agreed). Base cluster healed post-teardown (eligible back to 5, resolve returns correct cid — NO data loss).
### NEW GAP EXPOSED -> Phase 2.5 (state migration on re-election)
Making the election correctly span N re-elects shards to new writers on membership growth, but the registry does NOT migrate state to the new replica set — so existing heads were orphaned on the old holders (still durable in CraftOBJ, but not routed-to) and transiently unresolvable while the cluster was grown. Healed on teardown (election reverted). NOT data loss; a routing/migration gap.
- [ ] Phase 2.5: state migration on replica-set change — re-push / anti-entropy state to the current replica set when membership changes (not only on write), OR reconstruct-from-durable-CraftOBJ on takeover, OR broaden resolve-miss to query old holders. Prerequisite met (consistent election); this completes elastic membership. NOW ahead of Phase 3 (dynamic sharding).

### Phase 2.5 DONE + PROVEN (2026-07-08, commits a0d83f5 + 769cf93)
State migration on membership change. First attempt (a0d83f5) fired migration on EVERY census change -> during a join storm the census changes every gossip tick -> the 768-scan+push stormed and STALLED convergence (19->12). Fix (769cf93): debounce -> migrate once after the census is unchanged for MIGRATE_STABLE_TICKS (3 ticks, ~30s). LESSON (memory attribute-failures-to-code-not-environment): I wrongly blamed box load; the variable was my code. Re-run at 19 nodes: eligible converged to 18 (no storm), and node1/guestbook2 resolved to the SAME correct cid (807888d6) from BOTH an old node and a new node -> state followed the election, consistent. Elastic membership now works end-to-end: grow -> consistent census election + state migration -> resolves work.

### Relay-peer (Mac governor) stability saga — 2026-07-08
Long investigation into the Mac (phone hotspot, relay, ~600ms rtt) misbehaving. Findings + fixes:
- REAL regression #1 (fixed b5cc1f4): converged-membership added a SEPARATE member-sync connection every 10s that congested the fragile relay. Fix: fold member gossip into the existing 30s shuffle (no new connection).
- Confounder: the hotspot is genuinely flaky (~70% probe success). CONTROLLED test — full pre-membership cluster (f0554a8 everywhere) fluctuated IDENTICALLY (71%). So probe-% instability is the NETWORK, not code. Lesson: probe-% is the wrong metric.
- REAL bug #2 (fixed 1288873): the right metric is RECONNECT-vs-STUCK. Reconnection test proved the node STUCK at eligible=1 (t0=t1=t2=1) — dropped out and never climbed back. Cause: membership.start got only cfg.peers (EMPTY when a node configures only dht_seeds), so recover_isolated had NO seed to dial. Fix: seed membership bootstrap from dht_seeds + gentle rate-limited (15s, one seed) recovery ALONGSIDE fill_active (an earlier attempt ddf6c03 skipped fill_active -> itself caused stuck; reverted). Post-fix: t0=5 (converges), recovery fires (~15s spacing, not a storm), eligible climbs 1->2 (reconnects).
- My-error commits: ddf6c03 (skipped fill_active, reverted 5d095e2). Chased several wrong hypotheses (re-bootstrap storm [no-op], registry write-path [gated, still unstable]) before the controlled test + the reconnect-vs-stuck reframe pinned it.

### ROOT CAUSE of Mac "no active connection" regression — fill_active (fixed c3f99c9)
Found by DIFFING old-vs-new functions (user correctly rejected my live tests: full-pre-membership was window-confounded; old-Mac-vs-new-Hetzner is mixed-version = invalid). add_active/mark_dead UNCHANGED. The regression is my fill_active self-heal (2a99780): f0554a8 loops ALL passive candidates and DROPS a failed promotion (self-cleans passive of unreachable/stale addrs -> promotions keep finding reachable peers). Mine CAPPED at active_size attempts + RE-QUEUED failures -> on a passive polluted with dead addrs (accumulated via shuffle), a random few attempts hit only dead entries -> active view never fills -> ZERO active connections, can't refill, and re-queue prevents self-cleaning. Reverted to draining; recover_isolated's seed dial covers the full-isolation case that motivated the self-heal. This is the REAL regression; the member-sync fold (b5cc1f4) + empty-bootstrap (1288873) were also real but secondary.
