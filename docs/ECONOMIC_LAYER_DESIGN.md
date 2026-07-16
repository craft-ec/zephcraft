# Economic Layer & Participation Metrics — design

Status: DESIGN + PARTIAL BUILD (updated 2026-07-16). Captures the 2026-07-15/16 economic-layer
decisions. **§10 is now RESOLVED:** the distribution policy is **reward ∝ paid demand**, and
token issuance/genesis, quorum sizing `(n,k)`, and the free-tier parameters ship as **governed
parameters** (native default fair-launch), not baked constants. Mechanism is **built**: the
ordering sequencer (§4) and the serving-cheque + measurement substrate (§7) are live (§11 steps
1–2). The **token ledger — a canonical *protocol program*, not a user app** (§11 step 4), published
against the Layer-0/Layer-1 interface standard (§5) — is the active next build. Free tier = **global
tit-for-tat reciprocity**, not a credit token (§8); balances are **self-custodial account-chains**,
not PDAs (§3).

Governing principle (per `MINIMAL_KERNEL_DESIGN.md`): the node **measures and
meters** (mechanism, in the binary); the **economy** — the participation metric,
the token, the distribution rule — is **governed WASM** (policy). The node never
knows what a token is worth or how it is minted.

Reconciles / supersedes: `VISION.md` "no token / USDC-escrow" go-to-market (→ a
**native** token, §9) and its CraftSEC per-write MPC model (→ the verification +
attestation split of the 2026-07-12 re-cut, §4/§9). PDP [K5] deferral unchanged.

---

## 0. Goal — why an economic layer at all

To attract users and providers, the network must **reward contribution**: providers
won't donate disk/bandwidth without earning, and consumers won't trust the network
without a stake in its correctness. That is the storage-market bootstrap (Filecoin,
Sia, Storj, Arweave all rest on it).

**Decision made 2026-07-15: the network mints its OWN token on its OWN ledger** — a
native financial chain, not a client of an external chain. This supersedes the
`VISION.md` framing of "no token, USDC via multichain escrows"; external-chain I/O
(USDC gateways) may still exist later as an *on/off ramp*, but the internal unit of
account and settlement is native.

---

## 1. The organizing inversion — participation metrics come FIRST

Distribution (who gets minted tokens, and how many) must be **tied to contribution**.
Therefore the primitive to design first is **not the token** and **not the
distribution rule** — it is the **participation metric**: a per-participant
contribution score.

```
        measurements ──▶ participation metric ──▶ distribution ──▶ token mint
   (PDP, bandwidth,       (contribution score,      (UNDECIDED —      (native
    verification,          weighted/decayed/          the reward        ledger)
    attestation)           sybil-normalized)          function)
```

PDP, bandwidth, verification, and attestation are **not** the economy — they are the
**measurement instruments** that feed the participation metric. Get the measurements
right (verifiable, hard to forge, cheap to collect) and the metric + distribution are
policy choices layered on top. This doc specifies the measurement substrate (§2), the
accounting pipeline that runs it (§6), and the token/ordering machinery (§3–§5, §7); the
metric formula and the distribution rule are deferred (§10).

---

## 2. The measurements (contribution signals)

Each signal is a *verifiable* contribution. The load-bearing insight: **bandwidth /
serving is self-verifying; storage-at-rest is the one thing that needs PDP.** Because
everything is content-addressed (`cid = BLAKE3(bytes)`), a served piece is proven by
the recipient the instant they receive it. Storage *over time* — proving you *kept*
data without being asked to produce it — is the hard case, and the only one gated on
crypto.

| Signal | Measures | How verified | Availability |
|---|---|---|---|
| **Serving / egress** | upload bandwidth (pieces served) | recipient-signed **cheques/receipts** + content-address hash-check (§7) | buildable now |
| **Repair contribution** | durability work (recode + redistribute) | repaired pieces hash/vtag-verify | buildable now |
| **Relay / connectivity** | NAT-traversal / forwarding for peers | measurable forwarded traffic — paid per-hop, byte-count-acknowledged (§7) | buildable now |
| **Verification participation** [K6] | compute-consistency work | signed verdicts on the Board | **LIVE** |
| **Attestation participation** [K7] | authority / quorum sign-off work | signed quorum approvals | **LIVE** |
| **Storage-at-rest** [K5 PDP] | data *held* over time | homomorphic storage proof | **GATED** — interim: availability probes [K8] + owner-pays-pin |

Notes:
- **Anti-farming falls out of pay-for-egress** (detailed in §7). If consumers *pay* for
  downloads, self-requesting to farm serving-receipts = paying yourself = zero-sum. No
  PDP needed to close the loophole.
- **Storage is indirectly incentivized** by serve-to-earn: you must *hold* content to
  *serve* it, and serving pays — so demand does the work PDP would. The residual gap is
  **cold storage** (data kept but never fetched), which serve-to-earn does not reward.
  Interim cover: **owner-pays-to-pin** (owner stakes a keep-alive fee; providers earn it
  and must pass K8 availability probes + hold reputation). PDP [K5] hardens this later,
  swapping into the *same* measurement slot — no redesign.
- **PDP is cryptographer-gated** (see the K5 memory / `STATE_AND_ROADMAP`): the sound
  per-holder proof needs an asymmetric binary-field homomorphic signature (lattice-LHS)
  that survives erasure **recoding**; the vtags approach is unsound (forgeable). This is
  a real dependency, not a scope choice — hence the economy is designed to launch
  *without* it, on the self-verifying signals above.

---

## 3. The native token & ledger — architecture

North-star-consistent: **no global-lockstep chain, no BFT-committee-per-block.**

- **Balances = verification-validated account-chains.** Each identity's balance is a
  **single-writer registry head** (we already have owner-signed, versioned,
  single-writer-per-identity state). A transfer is a **signed debit** on the sender's
  chain. Validity ("do they have the balance?") is confirmed by **verification [K6]** —
  k nodes re-execute the sender's chain and agree. This is the VISION's *"validity by
  re-execution, not a committee."* The **recipient's** credit lands by either **claim** (recipient
  sweeps the debit onto its own chain) or **fold** (its balance = claimed-ins − debits-out,
  referencing the sender's ordered debit) — the one thing the self-chain model must specify that a
  program-owned account (PDA) would get for free (atomic dual-side write); pinned at step 4.
- **One balance per account: liquid tokens** (transferable, earnable, withdrawable). The free
  tier is **not** a second balance — it is a **reciprocity position** (§8) *derived* from the
  account's own serving vs. consumption (`total_earned − consumed`), not a stored "credit" token.
  A non-transferable, consume-only, expiring balance is a quota wearing a token's clothes; we don't
  mint one.
- **Balances are self-custodial account-chains, not program-owned accounts (not PDAs).** A balance
  is the fold of the owner's *own* single-writer chain, re-executed for validity — not a
  program-derived account the ledger owns (contrast Solana, where your SPL balance sits in a
  Token-Program-owned account). The user signs their own writes; the token *program* only
  **constrains** them to valid transitions (verification rejects anything else — see §5). What a
  PDA buys Solana (program-controlled writes, ordering, atomicity) is unbundled here into the
  **sequencer** (non-equivocation/ordering) + **verification** (validity) + the recipient-credit
  rule. Genuinely *shared* state — the subsidy pool, issuance counter, epoch clock — is the lone
  exception: it lives on a **governance-owned chain** (the one PDA-analog), touched at epoch
  cadence, not per transfer.
- **Ordering / uniqueness = attestation sequencer [K7]** (§4) — the one thing
  verification cannot provide.
- **Reward = bounded pool-average distribution (§10.1).** A provider submits its signed cheques /
  measurement evidence → the **reward-valuation program** (a *separate* governed program the ledger
  calls via program-to-program invoke) distributes the epoch's payment pool at a uniform per-byte
  rate → verification [K6] re-runs the deterministic distribution to confirm each provider's share →
  tokens move. Mostly **redistribution** of consumers' payments (aggregate-bounded by paid-in); fresh
  issuance only tops up the pool during bootstrap. Cheques are single-use (monotonic-cumulative →
  can't be re-rewarded).
- **Issuance & genesis = a governed policy program (§10.3), default fair-launch.** No premine
  in the default: tokens are earned by contribution from genesis, a bootstrap-issuance curve
  tapers as paid demand grows, and steady state is fee-recycled. The schedule, supply-cap-vs-
  inflation, and any genesis split are governance parameters — not baked into the binary.
- **Custody / treasury / multisig = attestation quorums [K7]** — "attestation for
  custody, not validity," exactly the split the VISION calls for.

Every piece composes from substrate already live: registry (account-chains) +
verification (validity) + attestation (ordering & custody) + the receipt mechanism
(earning). **No cryptographer needed for this layer; no external chain.**

---

## 4. Ordering = an attestation sequencer (extends the deferred K7 auto-signing)

**Why verification alone cannot prevent a double-spend.** Verification checks
*consistency* (`f(x) = y?`), not *uniqueness*. A double-spend is two txs from the same
balance at nonce N (→Alice, →Bob); **each is individually valid**, so each passes
verification in isolation. Choosing which is canonical is an **agreement** problem, and
agreement cannot come from local re-execution (the consensus boundary). So ordering
needs a coordination point.

**We avoid forks at *commit*, not by detection + slashing.** Route each account's
writes through its **attestation quorum**, which enforces per-version uniqueness: it
accepts the first version-N tx and **rejects the second**. The fork never happens —
nothing to detect, nothing to slash. Clean split of the two primitives:

- **Verification [K6] = validity** (well-formed, sufficient balance).
- **Attestation [K7] = uniqueness / ordering** (which tx is *the* canonical version-N).

This is **not a new substrate** — it is the **deferred automated attestation** (K7's
"member policy-program auto-signing", Package A shipped manual cosign; the auto-sign
half is on hold) **extended into a sequencer** with three properties plain attestation
lacks:

1. **Stateful non-equivocation** — a member remembers what it attested at
   `(account, nonce)` and refuses a conflicting second signature (first-writer-wins).
   *This is a SAFETY invariant, enforced structurally by the binary* (see §5), not left
   to the policy program.
2. **Quorum-intersection sizing** — any two quorums must share an honest member, else
   →Alice and →Bob each gather a *disjoint* k and both commit. Forces `k > (n+f)/2`
   (classic 2f+1 of 3f+1), not an arbitrary k-of-n. **Decided (§10.4):** `(n,k)` is a
   ledger-program parameter, default **n=4, k=3** (f=1), `2k>n` enforced structurally.
3. **Per-account (or per-shard) scoping** — the quorum attests one account's nonce
   sequence, so ordering load shards with the registry. **Decided (§10.5):** the quorum is
   a **rotating epoch committee** — each epoch its n members are deterministically selected
   from live membership (rendezvous rank over `(shard, epoch)`), not a fixed declared set;
   commits bind to their epoch's committee and the next committee continues from the account's
   durable committed nonce head.

**Tradeoff (the finality decision, §10.4):** this puts an attestation round on every
token transaction's path — latency + a liveness dependency on the quorum. That is the
tax for "money," and where the real BFT engineering lives.

---

## 5. Binary vs program — the seam

The token *logic* is a **program** (the first *canonical* app); the binary provides only
mechanism. Same split already used for governance, the registry, and attestation.

| Piece | Where | Why |
|---|---|---|
| **Token ledger** — balances, transfer, mint/reward math, supply schedule, fee/burn, staking | **Program** (WASM financial-chain app) | Pure policy; validity by verification [K6]. Governance-upgradeable without a binary roll. |
| **Discretionary attestation policy** — "should this tx be attested at all" | **Program** (the deferred K7 auto-sign) | Swappable policy. |
| **Reward valuation** — pool-average distribution (§10.1); quorum **sizing** `(n,k)` | **Program** (a *separate* reward program the ledger invokes) | Economic knobs, governed (default n=4/k=3, §10.4). |
| **Program-to-program invoke** (`invoke_program` host fn) | **Binary** (extends the runtime) | Lets the ledger call the reward-valuation program behind its anchor; **deterministic-callee only** (deterministic-capability subset — no wall-clock/random — so verification's re-execution reproduces it). |
| **Sequencer quorum membership** — which live nodes form the epoch committee | **Binary** (rotating epoch committee, §10.5) | Deterministic per-epoch selection from membership (node's view + rendezvous rank) — agreement machinery, not a program knob. |
| **Attestation rounds + auto-sign host hook** | **Binary** (extends K7) | Agreement machinery. |
| **Non-equivocation invariant** — never sign two conflicting statements at one `(account, nonce)` | **Binary, structural** | SAFETY, not policy (like the namespace gate). A buggy/malicious policy can only *decline* to sign — never cause a double-spend. |
| **Per-account nonce/attestation state store** | **Binary** (registry / CraftSQL) | The sequencer's memory. |
| **Serving-receipt emit/collect + metering→charge seam** | **Binary** (transport/runtime hooks) | A program can't observe raw piece-serving or fuel — only the node can. |
| **Verification, registry account-chains, propagation** | **Binary** (shipped) | Mechanism, live. |
| **Canonical-token-program pin (its cid)** | **Config** (trust root) | So auto-rewards/fees have a referent — like governance pins genesis and attestation pins the owner. |

**Dividing principle:** the node only knows how to run a program, re-execute it
(verify), gather quorum sign-offs (attest), store account state, and **report facts** (a
receipt, a fuel count). All economics are in the program. The only things that *must* be
binary are the facts a program cannot observe for itself (serving receipts, fuel
metering) and the non-equivocation safety invariant. The binary owns the *invariant*;
the program owns *discretion*.

**The anchor that makes it swappable = K1.** The "canonical-token-program pin" is a **K1
anchor** (the anchor dispatcher + config registry), and the governed numeric knobs (fee φ,
allowance, `(n,k)`, issuance schedule) are **K1 config** values. K1's config half is live; its
**anchor-dispatcher half is deferred** *"until a genuinely governed-WASM protocol program
exists"* — and the token ledger (§11 step 4) is precisely that first program, so building it is
what requires K1's deferred half. K1's own litmus governs the split above: **hard invariants go
native, genuine swappable policy goes governed-WASM behind an anchor** — a governed hook on a
safety invariant (e.g. non-equivocation) would be kernel bloat, the exact mistake K1's history
warned against.

### Protocol standards — publish the interface, not the implementation

Because the token is a **governed program behind a K1 anchor**, callers must depend on a **stable,
versioned interface**, never the implementation — that is what lets governance swap the program
without breaking wallets and apps (the ERC-20 lesson: a stable interface is what makes an ecosystem).
So the token ships with a published **interface standard**, layered:

- **Layer 0 — core fungible interface** (every token/asset, incl. user-issued): `transfer`,
  `balance(owner) → tokens`, `total_supply`, `metadata`; reserved `approve`/`transfer_from`
  (delegation) + `burn`. It is **message-schema + transition-semantics**, not function signatures —
  a versioned set of postcard messages the invoke runtime dispatches, with the invariants that give
  it teeth (nonce strictly monotonic via the sequencer; amounts are integer base units).
- **Layer 1 — protocol privileges** (the native token / canonical programs **only**): mint-from-
  contribution (`claim`) and egress `settle`. These are **gated to the canonical cid by the trust
  root** — a user asset *can't* adopt them (verification rejects a non-canonical program touching
  protocol receipts/pool, §5/§8) and has nothing to plug into semantically (the binary's meters feed
  the *native* economy). A user-issued asset implements **Layer 0 only**; the native token is the
  **reference implementation** of Layer 0 + Layer 1.

Discovery/versioning rides existing plumbing: the **anchor registry** maps a canonical name → current
program cid + `interface_version`; callers resolve the anchor, never the implementation. This is what
"the token is a **protocol program**, not a user app" means concretely — network-level machinery
(closer to governance than to the tracker), decomposable into separate programs (reward /
ledger-transfer / settlement) behind separate anchors.

**Process — defer.** A formal BIP/EIP-style change process (a "ZIP") is *not* needed yet: design docs
+ governance suffice while the contributor set is small, and governance + K1 anchors already give
**binding** ratification + atomic deployment (a proposal names a program cid; ratifying it *is* the
anchor swap — tighter than a purely-social BIP). Formalise it when *development* decentralises. Name
the fungible standard e.g. **CTS-1** (Craftec Token Standard); reference impl = the native token.

---

## 6. Managing the inversion — the accounting pipeline

The inversion runs on one pattern that keeps it decentralized: **each participant
self-accounts, claims per epoch, and the network verifies the claim** — nobody tracks
everybody. Accounting is O(1) per participant, not O(N) global.

```
measurement → attribution → accumulation → claim → verify → score → distribute → mint
   (hook)      (signed to    (earner's own   (epoch  (K6 re-   (metric  (reward    (sequencer-
               an identity)   single-writer   close)  run +     WASM)    WASM)      ordered)
                              chain)                  no reuse)
```

| Stage | How it's managed | Where |
|---|---|---|
| **Attribution** | every measurement is signed evidence bound to the earner (a serving cheque signed by the recipient naming the server; a verdict signed by the participant) | binary emits |
| **Accumulation** | the earner collects its *own* evidence in its *own* single-writer account-chain over an **epoch** — no central ledger of everyone's work | registry / CraftSQL |
| **Claim** | at epoch close the earner submits an aggregate ("epoch E, I served X, here is the evidence") | ledger tx |
| **Verify** | the reward function re-runs deterministically over the evidence (K6); each cheque is single-use (a spent-set rejects reuse) | K6 + program |
| **Score** | the participation metric normalizes + weights the heterogeneous signals into one number | metric WASM |
| **Distribute + mint** | epoch issuance (bounded by the schedule) split by relative score; mint txs ordered by the sequencer | ledger WASM + sequencer |

**Why it scales:** the network never tracks everyone centrally — each participant
self-reports (backed by unforgeable signed evidence) and the network verifies by
re-execution. The same single-writer-plus-verification pattern the whole substrate uses.

**Kept honest by:** single-use cheques (no double-counting a served byte), pay-for-egress
zero-sum (§7), stake-per-identity (sybils cost stake), sequencer-ordered mint (the epoch
mint can't be double-submitted).

**Two management levers that make it tractable:**
1. **Shadow mode — measure before you pay.** You cannot set good metric weights a priori.
   Build the measurement + accumulation substrate first, run the metric with **no
   minting**, watch the real contribution distribution, then set the distribution weights
   *from data*. Measure → calibrate → activate. This is the biggest reason to do the
   inversion at all: it turns distribution into a data-driven decision, not a guess.
   Cold-start during shadow: a committed **retroactive credit** ("we're measuring now;
   genesis contributors are credited when minting activates") + a small bootstrap
   allocation to seed the first providers.
2. **Governed weights — tuned, not frozen.** The metric formula, signal weights, and
   issuance schedule are governed WASM parameters (changed by governance/attestation, not
   a binary roll). The inversion is a living dial.

Open (→ §10): the **epoch model** (window length + claim cadence) and the shadow→active
cutover.

### Data capture & storage — where the pipeline's data lives

**No new storage engine.** The ledger is an **app**, so it captures and stores exactly like
any CraftCOM app — via the existing host fns into per-identity **CraftSQL** (`app.ledger`
namespace) + durable **CraftOBJ** + **registry** heads. The self-account pattern above
dictates the layout: **each participant stores its OWN accounting; the network stores only
the authoritative heads others must read** — distributed by construction, no central store.

**Capture path.** Signed evidence is produced by the *binary* hooks (the serving/relay
transport hook + the verification/attestation quorum — §5), handed to the ledger *program*,
which persists it with `sql_execute` / `obj_put`. Binary reports facts; program stores them.

| Data | Where | How it's bounded |
|---|---|---|
| **Own evidence** (cheques received, verdicts) | owner's CraftSQL (`app.ledger`) | a cheque is a *cumulative* tally → one row **per active counterparty**, not per transfer → O(channels) |
| **Balance + chain head** (authoritative, global-read) | **registry** head (root + balance + nonce) | O(1) per account; the sequencer/verifiers read it |
| **Chain history** (tx log) | owner's CraftSQL, durable on **CraftOBJ** (erasure) | fetchable via the head **even if the owner is offline** |
| **In-flight channel / cheque** | local until settlement | only the *latest* cheque kept |
| **Spent-receipt / nonce set** (anti-reuse) | ledger state (CraftSQL) | **epoch-scoped** + expiry → active window only |
| **Global state** (subsidy pool, issuance, epoch clock) | governance-owned chain, **sequenced** | the one non-single-writer piece |

**Money-specific management** (the honest challenges the substrate already answers):
- **Bounded volume, not O(all transfers).** Cumulative cheques (one per counterparty) +
  prune settled evidence once its claim is minted → durable footprint is O(accounts +
  active channels).
- **Bounded re-execution.** Verifiers don't replay from genesis — periodic **checkpoints**
  (an attested balance state-root per epoch) start re-execution from the last checkpoint;
  older chain is pruned/archived to cold OBJ.
- **Money can't fade.** Ledger state (head + checkpoints) is **pinned / high-durability** on
  CraftOBJ (durability gate + health-scan/repair), *not* the fade-if-unfetched path.
- **Read-availability without the owner.** Balances publish as registry heads and the chain
  is erasure-distributed, so any node can read + re-execute an account's state regardless of
  the owner's liveness — no stall.

**Net:** reuses CraftSQL + registry + CraftOBJ *unchanged*; the only economic-specific
additions are **checkpointing, epoch-scoped pruning, and high-durability pinning** of ledger
state — all policy/parameters, not new mechanism.

---

## 7. Egress payment — SWAP-style cheques

Egress is a **metered, consumer-funded service** (a *transfer*, not issuance) — and the
hardest mechanism in the layer, because it is the P2P **fair-exchange** problem (neither
side wants to pay/serve first). The answer is *incremental, content-verified* exchange.

- **Per-segment interleaved exchange.** Files are already segmented (each 8MiB segment its
  own cid). For each segment: the consumer sends a signed **cheque** (a cumulative running
  tally of tokens owed) → the provider serves the next segment → the consumer verifies it
  against its cid → repeat. If either side stops, it loses **at most one segment**.
  Content-addressing does half the work: the consumer *knows* it got the right bytes
  before paying for the next.
- **Off-ledger cheques, on-ledger settlement at a threshold.** A ledger tx per segment is
  absurd (an attestation round per 8MiB). So the per-segment cheque is signed
  *off-ledger*; the provider keeps the latest and **settles on-ledger only when the
  running balance crosses a threshold** — one tx amortized over the whole transfer. The
  consumer's egress funds are **escrowed** so the provider knows the cheque is backed and
  can settle unilaterally with the latest cheque.
- **One artifact, three roles.** The signed cheque *is* the serving receipt *is* the
  measurement evidence (§6). Pay-for-egress and the receipt mechanism are the same thing.
- **Prior art:** Swarm's SWAP (bilateral bandwidth accounting; settle with signed cheques
  when debt crosses a band). A known-workable shape, not speculation.
- **Bilateral credit band = tit-for-tat, hardened.** Peers serving each other on credit
  within a tolerance band (nets to zero between reciprocal peers) is exactly BitTorrent's
  tit-for-tat — but promoted from a *soft, ephemeral, per-swarm choke heuristic* to
  **persistent, cross-content, cryptographically-signed accounting with a monetary
  fallback**: reciprocal peers barter bytes with zero settlement overhead (tit-for-tat's
  efficiency), and asymmetric relationships **settle in real tokens** when the imbalance
  crosses the band. This closes tit-for-tat's three classic holes — seeders now *earn*
  (serve-to-earn) instead of pure altruism, free-riding is capped by the band + forced
  settlement (no gaming the optimistic-unchoke slot), and credit is *global and
  persistent*, not forgotten per swarm. Promoted to **global** per-account reciprocity, this
  band *is* the **free tier (§8)**: staying net-balanced is free, a deficit settles in tokens.
  The only pool subsidy is the cold-start grant, not standing free serving.

**Anti-farming:** a cheque draws *real escrowed consumer tokens*, so it can't conjure
value — it only moves value that already exists. A self-serving sybil drains its own
escrow into itself (net zero, net negative after fees), and the metric counts *paid
egress from distinct paying consumers*, so self-payment earns zero metric credit.

**Reuses:** segments (payment chunks + per-chunk verification), the mux stream (carries
cheques inline), the ledger + sequencer (escrow + settlement), receipts (= the cheques).

**Settlement across many providers (decided §10.8).** A swarm fetch collects cheques from
*many* providers against **one** prepaid per-consumer egress balance — not a channel per
provider. At epoch close, settlement runs `allocate_quota` over the consumer's cheque set to
split it into (paid, subsidy), total paid capped at the escrowed balance. Because that cap is
*global* across providers, the set must be **complete** — a consumer that hid cheques to make
each provider look "first-come" by timestamp could otherwise double-allocate its quota.
Completeness is enforced by **reconciliation, not trust**: every provider independently holds
and submits its own cheque, and the ledger takes the **monotonic max per (provider, consumer)
pair** — so a cheque the consumer omits is supplied by the provider, and one the consumer
inflates is bounded by its own signature. A provider left out of the consumer's set simply
**settles unilaterally** from the escrow. Net: the consumer gains nothing by hiding cheques and
is incentivised to submit the full set to release its escrow cleanly.

Open (→ §10.9): escrow/channel lifecycle (top-up, close, reclaim on timeout, disputes);
cheque granularity; the credit-band size.

### Relay bandwidth — the same cheques, one hop at a time

Endpoint egress (above) is anchored by content-addressing — the consumer hash-checks what
it received, so the receipt is self-verifying. **Relay bandwidth breaks that anchor:** a
relay forwards opaque, often end-to-end-encrypted bytes it cannot see or hash. So relay
payment needs a *different anchor*, but **not a different mechanism.**

- **Reduce multi-hop to per-hop bilateral exchange.** A relay accounts bandwidth only with
  its two immediate neighbors, using the same signed cheques + credit band as §7. It
  **pays its downstream** (for bytes it pulls through) and **charges its upstream** (for
  delivering them), keeping a small **margin** for the forwarding work. Cost cascades back
  to the originator (the consumer), who ultimately funds the whole path. The hard 3-party
  fair-exchange dissolves into a chain of 2-party exchanges (Swarm's forwarding model).
- **The anchor is a byte-count acknowledgment, not a content hash.** The served neighbor
  signs "received N bytes from relay R" — that ack *is* R's receipt, and R cannot claim
  payment without it (**no delivery → no ack → no pay**). For a *content* transfer the
  endpoint's content-verified receipt still anchors the tail of the chain: if any hop
  corrupts, the consumer's hash-check fails, it signs nothing, and receipts stop cascading
  back — so the faulty hop (where the receipt chain breaks) earns nothing.
- **Two flavors, one mechanism.** (a) *Connectivity relay* (iroh DERP-style NAT traversal
  — the live fleet's public role): carries E2E-encrypted traffic, paid per byte by the
  peer that needs it, acknowledged by byte-count receipts, can't see content (privacy
  preserved). (b) *Overlay forwarding* (Swarm-style multi-hop retrieval): the cascade above
  with per-hop margins. Our transfer plane is mostly direct once peers are discovered, so
  (a) is the near-term case; (b) is the general form.

**Anti-gaming (same shape as egress):** a relay can't inflate bytes beyond what the
downstream *signs* for; non-delivery earns nothing (can't fake work it didn't do);
self-/wash-relay between sybils draws real escrow → **zero-sum** (net-negative after
margins/fees), and the metric counts **paid** relay bytes from distinct paying parties, so
relaying to yourself earns no metric credit — identical to paid-egress (§2).

**Who pays:** the peer that needs the path. A NAT'd consumer fetching content pays the
provider (egress) *and* each relay hop (carriage) in one cascading settlement — a relay is
just another chargeable hop.

**Metered separately from egress (decided 2026-07-15).** Relay carriage is its own metered
category, **not** bundled into the download bill. Rationale: (1) *Cost-causation fairness,
in **both** tiers* — you pay for the resources your delivery actually consumes. In the
**paid** tier a non-NAT user's bill is egress-only, strictly less than a NAT user's (egress
+ relay carriage) — bundling (a flat bandwidth price, or relay averaged across users) would
make direct users overpay and subsidize NAT users. In the **free** tier separating also
stops relay users from draining the common subsidy pool (§8). This is *cost-causation, not a
penalty*: each user pays their **true** delivery cost (like cloud egress) — and it
incentivizes cheaper direct connections while the higher relay price attracts more relay
supply (self-correcting). (2) *Congestion pricing*
— relay is scarce (few public relays) and is the *fallback*; making its cost visible
pressures clients to establish (cheaper) direct connections and reserves relay capacity for
those who truly need it. **Mechanism vs policy:** metering relay separately does not force
charging for it — it *enables* an independent pricing choice. Because NAT is usually not the
user's choice (CGNAT / mobile / firewalls), the free tier should include a **capped relay
allowance** (access-fairness for restricted networks) rather than excluding relay outright;
beyond the cap, relay draws tokens. The allowance cap is a policy knob (§10).

---

## 8. Cold-start & the free tier — tit-for-tat reciprocity (not a subsidised token)

**Revised 2026-07-16.** A new account has zero tokens (demand-side cold-start), and we want a
*permanent* free tier for adoption. The key realisation: **reciprocity is the base for *everyone*,
not a free-only tier.** The SWAP credit band (§7) nets reciprocal byte-exchange to zero for *any*
account — paid or not — so **tokens (or a grant) only ever settle the *deficit*: the net imbalance
between what you served and what you consumed.** Your reciprocity position is `total_earned −
consumed` (bytes you served *anyone*, minus bytes you fetched from *anyone*, globally):
- **Net-positive (served ≥ consumed) → free, for everyone.** Consumption is balanced by service;
  cheques net out; the network pays nothing. Genuinely free because it is *reciprocal*, not
  subsidised — there is no "free allowance," just reciprocity.
- **Net-deficit (consumed > served) → settle the deficit.** *This* is the only place paid and free
  diverge (below).

**There is no "credit balance."** A non-transferable, consume-only, expiring balance is a *quota
wearing a token's clothes* — strip transferability, persistence, and tradeability and what's left is
a reciprocity limit, not a currency. So: **one token balance + a reciprocity position derived from
accounting we already collect** — `total_earned` (serving side) minus issued cheques (consuming
side). Nothing new to store; nothing about the free tier in the token standard.

**Paid vs free = how your *deficit* settles (not two consumption systems).** The reciprocal part of
everyone's usage is free and un-gated (cheques net to zero). Only the deficit is settled, and *that*
is where the two diverge — because it is backed by different things:

| | reciprocal part (≤ your contribution) | deficit — paid (has tokens) | deficit — free (no tokens) |
|---|---|---|---|
| settles via | nothing — cheques net to zero | **tokens**, against escrow | bounded **cold-start grant**, else throttle |
| backed by | your own service | escrowed tokens (real value locked) | a reciprocity promise (nothing locked) |
| check timing | none (free for all) | **retroactive** at settlement (`allocate_quota`) | **real-time** at the admission gate |
| exposure if abused | none | none — escrow covers it | bounded wasted bandwidth → *why* it must gate live |

Escrow buys the paid deficit its laziness (locked value → reconcile after the fact); the free deficit
has nothing locked, so it **must gate up front**. *Pre-funded → check late; un-funded → check live.*
The admission gate therefore fires only on an **unbacked deficit** — a paid user consumes
reciprocity-first and hits *no* gate even when they run a deficit (escrow backs it), so there is **no
consumption-time free/paid decision** for them; the deficit resolves at the retroactive timestamp
settlement of §7/§10.8.

**Subsidy shrinks to cold-start only.** The one case reciprocity can't cover is a brand-new account
that has served nothing yet — a read-only newcomer needs a small **starting grant** of reciprocity
headroom to begin. That bootstrap (identity-gated, small, one-time-ish) is the *only* thing the pool
funds for the free tier — not a standing allowance draining it forever.

**Downstream details (pin at step 4):**
- **Reciprocity offset applies *before* tokens.** At settlement, your `total_earned` first reduces
  what you owe; **then** `allocate_quota` settles only the *remaining* deficit in tokens by timestamp.
  As built, `allocate_quota` doesn't net against serving — so settlement needs a reciprocity-offset
  step in front of it.
- **Global is the position; bilateral is a fast-path.** The raw SWAP cheque nets *bilaterally*
  (per-pair). The authoritative reciprocity position is **global** (`total_earned − consumed` across
  everyone — serve anyone, consume from anyone); bilateral netting between mutual peers is a trustless
  settlement optimisation on top.
- **No double-count (reciprocity first, surplus rewarded).** Your serving first offsets your *own*
  consumption (reciprocity, no tokens); only the **surplus** (served beyond consumed) earns a token
  reward (mint). The offsetting bytes are not also rewarded — the same contribution is never counted
  in two denominations.

**Free vs paid — the product boundary (what actually differs).** The free tier is *not* "paid,
subsidised" — it is a deliberately **bounded, consume-only, reciprocity-gated** slice. The limits *are*
the product boundary, and the reason anyone pays:

| Axis | Free (reciprocity) | Paid (tokens) |
|---|---|---|
| **Scale** | headroom = your reciprocity balance (`served − consumed`) + a small cold-start grant | buy any volume |
| **Reliability** | reciprocity-gated in real time; throttles at the deficit band | escrow-backed → always admitted, settled retroactively |
| **Durability / publish** | consume-only — *read* the shared pool | owner-pays-pin: publish + persist your own data, run a service |
| **Value** | nothing tradeable — reciprocity is a *position*, not an asset you hold | liquid — transferable, earnable (serve-to-earn), withdrawable |

The reliability gap is **not** provider discrimination (the cheque is tier-blind, §7) — it is at
*admission*: a free fetch is gated in real time on your reciprocity position, a paid fetch is
escrow-backed and never gated. **What drives conversion:** you pay the moment you need *scale*
(consume beyond what you contribute), *reliability* (can't be throttled at the deficit band),
*durability* (publish/persist your own data — the north-star product; free is consume-only and can
host nothing), or *to earn/transact* (reciprocity can't be transferred, sold, or withdrawn — only
tokens can). One line: **free lets you *use* the network; paid lets you *build on* it.** And the free
tier is **self-funding by construction** — reciprocal users serve their own keep, so only the
cold-start grant is pool-funded.

**Where the tier rules are enforced (not in the cheque).** The serving-cheque is a **meter, not
an enforcer** — it only records "C received N bytes from P," tier-blind by design (so a provider
can't cherry-pick paid fetchers, which would starve the free tier). Enforcement lives *downstream*,
at the points where value changes hands or a resource is committed:

| Rule enforced | Where | Timing |
|---|---|---|
| non-transferability of credit | ledger program transition (a `transfer` rejects a credit source) | authoritative (re-run by verification) |
| paid/subsidy split + allowance cap | ledger **settlement** (`allocate_quota` + tokens-vs-credit burn; over-spend not honored) | authoritative, retroactive |
| real-time tier decision (draw tokens / draw credit / throttle) | consumer-side **fetch-admission gate** at `get`-initiation | real-time |
| reliability (free is pool-gated) | same admission gate (allowance + pool health) | real-time |
| durability (consume-only vs owner-pays-pin) | a **separate pin / publish-admission gate** on the distribute path | real-time |

Two are retroactive-but-authoritative (settlement, re-checked by verification [K6]); the rest are
real-time gates. Durability is enforced on the **store** path — a different code path from serving
entirely.

This is the §5 seam applied to tiers, and it sharpens *"the ledger is the policy"*: distinguish
**ledger state** (economic data — balances, pool, epoch) from the **ledger program** (the
transition function — *that* is the policy: tiers, split, non-transferability, mint). The program
is not a consensus engine; it *rides on* three mechanism primitives it does not contain — the
sequencer (ordering), verification (validity, by re-running it), attestation (custody). The
**binary** provides the enforcement *points* (the meter, the admission gate, the pin gate, the
account-state store, and the non-equivocation invariant); the **program** provides the *rules*;
nothing economic lives in the binary. The mechanism that makes the ledger program *the* swappable
policy behind a stable anchor — and turns the governed numeric knobs (fee φ, allowance, `(n,k)`,
issuance schedule) into config values — is **K1 (the anchor dispatcher + config registry)**: its
config half is live, and its deferred anchor-dispatcher half is exactly what the token-ledger app
(§11 step 4, the first genuinely governed-WASM program) requires. The litmus is K1's own rule:
**hard invariants go native, genuine swappable policy goes governed-WASM behind an anchor** — so
non-equivocation + the meter are native, while tier / allowance / reward are program + config.

**Farming resistance — the threat model.** *Under the tit-for-tat model above, most free-tier farming
is intrinsically defended: you cannot consume free without contributing equal service, so a pure
leecher hits the deficit band and must pay — there is no standing subsidy to mint from. The threats
below now chiefly guard the two remaining pool-funded surfaces — the **cold-start grant** and the
**paid-overflow subsidy** (§10.1).* Three *distinct* threats, easily conflated:

**(A) Producer-side self-dealing** — an attacker owns both a free-consumer *and* the provider
that gets paid, converting free credit into liquid tokens. **Primary defense: the *protocol*,
not the consumer, picks the producer.** A free-tier fetch is served by protocol-selected
holders (erasure pieces pulled from rendezvous/DHT-placed nodes), so the attacker cannot
ensure *its* node is the one paid — the credit is spent, but a (probably honest) provider
receives it. This reverts the worst case from *value-minting* back to **bounded wasted
bandwidth** (the pool paid an honest provider to serve a sybil). **Caveat — holding
monopolization:** if the attacker makes its node the *sole* holder of the fetched content
(self-published junk, or pieces concentrated on its own nodes), "random among holders" picks
it with probability 1. So this defense requires **protocol-enforced piece placement**
(holdings can't be monopolized) and/or weighting free-tier serving-reward by
**independent-holder count / organic demand** (no reward for serving content only you fetch).
With those, (A) is effectively closed.

**(B) Bandwidth inflation via a custom program** — a modified client claims more served/
relayed bytes than it moved. **Defense: every credited byte must be signed for by the
counterparty** (the recipient's cheque; the downstream's byte-count ack). A custom program
cannot forge the counterparty's signature, so it cannot inflate *alone* — inflation requires
a *colluding* counterparty, which collapses back into (A) and is defended the same way.
Crucially this needs **no binary trust** — we verify the signed cheques + content-addressing
+ re-execution (K6), not the client. (Which is why binary attestation is the wrong tool —
see below.)

**(C) Sybil free consumers** (one actor, many free accounts) — a *separate* problem. Given
(A), their consumption pays *honest* providers, so the damage is **bounded wasted subsidized
bandwidth**, not value extraction — a *cost-budget* problem, not a security hole. Capped by
the **identity gate** (stake / invite / PoP) + the per-identity allowance + the pool-health
allowance sizing (§8 funding). The gate's job here is to bound the free tier's *cost*, not to
prevent value-minting — (A)'s producer randomization does that.

**The simplest primary defense (leaning 2026-07-15): profit only from *paid* demand.**
Make provider **profit/reward come *only* from paid demand** (paid egress + paid pinning +
paid relay), and **cost-reimburse free-tier serving from the pool at *no margin*.** Then the
farm collapses by construction: self-serving *paid* content pays yourself (**zero-sum**);
self-serving *free* content only reimburses your bandwidth cost (**break-even, no profit**).
Nothing to extract, either way. This is the market pricing real demand directly — you can't
fake paying yourself — instead of a contribution oracle approximating it. With this,
**producer randomization + organic-demand weighting (§10.2) become optional defense-in-depth,
not load-bearing** (producer randomization still useful for load-balancing/availability).

**Two conditions keep this airtight (both required):**
- **Reward = direct payment *revenue*, NOT a share of a contribution *pool*.** If rewards
  were a pool split proportional to measured contribution/volume, a provider could pay its
  *own* consumer account → its "contribution" rises → its pool-share redirects tokens from
  honest participants (the self-payment is zero-sum on the payment but *buys a bigger slice*).
  Direct-revenue removes the pool-to-steal: inflating your volume earns only what you paid
  yourself → strictly zero-sum. **Corollary:** drop "epoch issuance split by contribution
  score" (that split *was* the farmable part), and keep reward **linear in revenue** — no
  volume tiers / reputation multipliers / superlinear bonuses (any superlinearity re-creates
  the self-inflation incentive). Bootstrap issuance is a taper (flat/decaying or matched to
  *verified independent* revenue), **never volume-proportional**.
- **Metered / quota-bounded, never *truly* unlimited.** The zero-sum property needs payment
  to track usage. A flat "unlimited" plan makes the *marginal* fetch free → self-fetching
  costs nothing → farmable. Offer generous quotas / fair-use caps ("soft unlimited"), like
  every real "unlimited" ISP/cloud. Also set free-tier reimbursement *at* cost, not above
  (margin = small residual farm).
  - **SUPERSEDED (2026-07-16) — see §10.1 (pool-average) + §8 top (tit-for-tat).** The earlier
    "metered-reward with subsidized overflow" framing is gone: there is **NO cost-reimbursed overflow
    band**. Reward is the **contribution-ratio** distribution of the payment pool (§10.1: `pool ×
    (its serving / total serving)`, a uniform rate), and **settlement is claim-based** (each provider
    claims its verified epoch share). Consumption beyond paid + reciprocal is **throttled** (the
    admission gate), not subsidized. The consumer-facing tiers are the **reciprocity** model (§8 top):
    reciprocal = free (netted), a deficit settles in tokens or throttles. The only pool-funded subsidy
    is the small **cold-start grant** (below) — separate from reward distribution. Farm-safety comes
    from aggregate-boundedness (pool = payments), non-inflatable attested serving, Sybil-neutral
    proportionality, and uniform pricing (§10.1) — not from an overflow band.

**Net (fallback framing if reward is *not* restricted to paid demand):** producer
randomization + protocol-enforced placement prevent *value-minting*; counterparty-signed
cheques prevent *inflation*; the identity gate + dynamic allowance bound the *cost* of sybil
consumption. Non-transferability still forces any farm through a real, detectable round-trip.
**Residuals (§10):** the holding-monopolization gap in (A), and — if free-serving routes
through the metric for extra safety — the delayed provider reward for free traffic.

**Why not gate on binary attestation (code checksum)?** A tempting idea — "only count
traffic from the genuine binary" — but it fails on two independent counts. (1) *A software
checksum is a claim, not a proof*: nothing binds the sent hash to the actually-running code;
secure remote attestation needs a hardware TEE (SGX/SEV/TPM), which **excludes most devices**
(fatal for "every device contributes" reach) *and* is repeatedly broken (SGX side-channels
→ forged attestations). (2) *Even perfect attestation doesn't stop the farm*: it runs on
**genuine binaries doing genuine work** — an attacker runs many honest nodes and self-deals
*real* traffic; attestation proves the code is honest, it cannot prove the *demand is
organic*. The hole is **sybil identity + self-dealing, orthogonal to code integrity.** And it
misaligns with the substrate's "verify the *output*, don't trust the node" principle
(content-addressing, signed cheques, re-execution) — binary honesty is deliberately *not*
load-bearing. The levers stay **producer-randomization** (anti-value-mint, §8-A),
**counterparty-signed cheques** (anti-inflation, §8-B), and the **identity gate** (bounds
cost, §8-C) — not the binary. (Binary attestation is at most niche defense-in-depth for a
*privileged role* — e.g., TEE-attested quorum members — accepting the hardware cost for a
small set.)

**Symmetry:** this demand-side subsidy mirrors the supply-side one (the §6 retroactive /
genesis provider reward). Both taper as the real economy grows; both are governed policy,
not baked into the binary.

**Funding — how paid tokens offset the free quota.** The subsidy pool is fed by a
**recycled protocol fee-skim**: a small fraction **φ** of every *paid* settlement
(egress / relay / storage) is diverted into the pool rather than burned — so **paid usage
directly funds free usage** (freemium: paying customers carry the free ones), and the
free-tier budget *scales with paid adoption*. Pool **inflow** = fee-skim **+ a tapering
issuance top-up** during bootstrap (when payers are few, issuance funds the free tier and
tapers to zero as fee inflow grows — the §8 taper). Pool **outflow** = the real tokens paid
to providers/relays when free users redeem credit. **Self-balancing:** the per-identity free
allowance is a *function of pool health* (fee-inflow rate / balance / active free
identities), so the free quota can never over-draw what paid activity has funded — it
throttles when the pool thins, loosens when paid volume is strong. The pool sustains roughly
`F/C` free users per paying user (F = fees/payer/epoch, C = credit-cost/free-user), so φ and
the allowance are the dials that set the free:paid ratio. Per user, consumption is
**credit-first then tokens**, and a paying user **net-funds** the pool (fees exceed the base
credit drawn). This resolves the fee question toward **recycle-to-pool, not burn** (§10.3).

Open (→ §10): the fee rate **φ** + the issuance-taper schedule; the **allowance function**
(how it tracks pool health) + refresh cadence; and the **identity / sybil gate** for
claiming it (refundable stake / invite-referral / proof-of-personhood) — the crux of
keeping a *standing* subsidy from being drained.

---

## 9. Reconciliation with prior docs

- **`VISION.md` "no token / USDC-escrow" GTM** → superseded by the **native token**
  decision (§0). External-chain gateways remain possible as on/off ramps, not the
  internal unit of account.
- **`VISION.md` CraftSEC per-write MPC** ("every financial write co-signed by threshold
  nodes") → realized by the **verification + attestation** split (validity by
  re-execution [K6]; ordering & custody by the attestation sequencer [K7]), per the
  2026-07-12 re-cut. Not a separate MPC layer — it *is* attestation.
- **`VISION.md` "tit-for-tat, no tokens"** → generalized: the reciprocal barter is kept
  (the credit band, §7) for its zero-overhead efficiency, but backed by **monetary
  settlement + serve-to-earn**, superseding the "no tokens" stance (per §0). Fixes
  tit-for-tat's seeder-starvation and free-riding that a token-free scheme cannot.
- **PDP [K5] deferral** — unchanged; still cryptographer-gated. The economy launches on
  self-verifying signals (§2) and treats PDP as a later hardening of the cold-storage
  measurement.
- **`MINIMAL_KERNEL_DESIGN.md`** — this design keeps the kernel minimal: no "token"
  concept baked in; the economy is governed WASM behind a config-pinned anchor.

---

## 10. Decisions — RESOLVED 2026-07-15 (economic models locked; numeric parameters governed-at-launch)

The economic *models* below are decided. Where a decision is a **number** (a rate, a cadence,
a committee size), it ships as a **governed parameter** with the default noted — mechanism-first
per the minimal-kernel principle (native default at genesis, governance-swappable without a
binary roll). Committing to a *shape* rather than a magic constant is itself the resolution of
#3/#4.

**Economics / policy — DECIDED:**
1. **Distribution policy — DECIDED: reward ∝ paid demand, as a BOUNDED POOL-AVERAGE (revised
   2026-07-16).** Payments pool per epoch; each consumer's rewardable basis is capped at their paid
   quota (`min(used, paid-quota)`); the pool is distributed to providers at a **uniform per-byte
   rate** (`pool ÷ total rewardable-served`), so a provider earns the *average* rate for bytes
   served **regardless of which consumer it was assigned**. This is the fair compensation given the
   protocol (not the consumer) picks the producer — a provider can't choose its consumer, so it
   shouldn't bear that consumer's rate. *This supersedes the earlier "direct-revenue-per-consumer,
   linear" form.* It stays farm-safe because it is (a) **aggregate-bounded by payments**
   (redistribution, not fresh issuance — can't extract more than was paid in), (b) distributed by
   **non-inflatable attested serving** (counterparty-signed cheques + producer-randomization → can't
   fake or monopolize your share), and (c) **Sybil-neutral** (proportional split). **Guardrail: the
   per-GB price must be uniform / floor-bounded** — else a self-dealer pays a trivially-low rate for
   a large quota and extracts the pool *average* (a farm); under uniform pricing, self-dealing nets
   zero. It is mostly **redistribution** (consumers' tokens → providers at the average rate); fresh
   issuance only tops up the pool during **bootstrap** (tapering, identity-gated). Free/reciprocal
   serving earns reciprocity credit (§8), *not* pool share. Cold storage = **owner-pays-pin**;
   consensus work = **fee-funded**. The rate self-balances (more providers → lower unit rate →
   market-clearing). **NO overflow subsidy (corrected 2026-07-16):** the pool is fully distributed by
   contribution ratio — there is no cost-reimbursed "overflow band"; consumption beyond paid +
   reciprocal is **throttled** (the admission gate), not subsidized (the only pool-funded item is the
   small cold-start grant, §8, which is separate from reward distribution). **Settlement is
   CLAIM-based:** the verified per-epoch shares form an **epoch reward RECORD**, and each provider
   **claims** its share onto its own chain (`RewardClaim{epoch}`, single-use — the transfer→claim
   pattern with the record as the "debit"), so there is no node-side fan-out of writes. **Pay-into-pool,
   not escrow (revised 2026-07-16):** a consumer pays its metered egress into the pool with a
   *self-authored* `Pay` debit — NOT an escrow lock that settlement later draws from. This removes the
   cross-account settlement-authority problem entirely (no keyless committee reaching into user escrow):
   *both* sides are self-authored — consumer `Pay` in, provider `RewardClaim` out. The provider serves
   before it's paid, but that guarantee is already covered by the SWAP-cheque interleaving (§7) + the
   admission gate (an unfunded consumer is throttled), so no lock is needed. **Cross-epoch rolling — a
   running pool with a claim window:** the pool is a *running* balance (`Σ payments − Σ claims`, never
   reset), split into **`unallocated`** (payments not yet assigned — new pay-ins + dust + expired
   forfeits, and the *only* thing a record distributes) and **`owed`** (shares a published record
   assigned but not yet claimed — reserved, so they can't be re-distributed). Each epoch's record moves
   `unallocated → owed` by contribution ratio; a `RewardClaim` moves `owed → the provider's balance`.
   Integer **dust** stays `unallocated` (folds into next epoch's record automatically); **unclaimed
   shares** stay claimable for a governed **N epochs**, after which the record *expires* and its `owed`
   reverts to `unallocated` (forfeit) — which also bounds record storage to the last N epochs.
   Conservation is total: every paid token is claimed, `owed` (within window), or `unallocated`
   (rolling); `unallocated + owed ≥ 0` always. The pool state (two counters + last-N records) lives on
   the governance-owned epoch chain (§6), touched once per epoch, not per fetch. *This is the spine.*
2. **Participation-metric formula — DECIDED: dissolved.** Paid demand *is* the metric; no
   rich multi-signal contribution oracle (with sybil-normalization + organic-demand weighting)
   is built. Organic-demand weighting is retained only as **optional defense-in-depth** (§8-A),
   off by default — reachable only if the pure paid-demand model proves too narrow (then: how
   much to reward non-paid signals — repair, verification, attestation — beyond their own fee
   streams). Not on the critical path.
3. **Token economics — DECIDED: genesis + issuance are a GOVERNED PARAMETER; native default =
   fair-launch.** No premine in the default: every token is earned by contribution from genesis;
   a bootstrap-issuance curve tapers as paid demand grows; steady state is **fee-recycled**
   (fees **recycle-to-pool, not burn** — funds the free tier §8 + the shadow-mode retroactive
   reward §6). The exact issuance schedule, supply-cap-vs-perpetual-inflation, and any genesis
   split are set by the governed issuance-policy program at launch (default fair-launch), not
   baked into the binary — so the number is a governance decision, not a release.
4. **Finality — DECIDED: (n,k) is a ledger-program parameter; default n=4, k=3 (f=1).** A
   write is settled once k of n commit — fork-impossible at commit (§4); one quorum round-trip
   = finality. `2k>n` is enforced structurally; governance raises (n,k) as stakes/fleet grow.
5. **Sequencer quorum selection — DECIDED: rotating epoch committee.** An account's (or shard's)
   ordering quorum is NOT a fixed declared set: each epoch the n members are **deterministically
   selected from live membership** (rendezvous rank over `(shard, epoch)`, §58 ranking) — every
   node computes the same committee with no election messages, and it rotates each epoch. Commits
   bind to their epoch's committee; verification checks signers ∈ that epoch's committee; the next
   committee continues from the account's durable committed nonce head (cross-epoch hand-off).
   Size/threshold come from #4 (governed); **membership becomes a binary mechanism** (it moves
   out of the §5 "program knob" row). *Cost: this is the heaviest remaining build — an epoch
   clock, a deterministic committee function, and boundary hand-off of in-flight sequences —
   chosen over the declared-set MVP for maximal decentralization. Build it once the ledger
   mechanism is otherwise proven; a genesis committee is the degenerate 1-epoch case to bootstrap.*

**Free tier / egress — models decided (§7–§8); numeric parameters GOVERNED-at-launch:**
6. **Free-tier funding — DECIDED (revised 2026-07-16):** the free tier is **tit-for-tat** (§8), so
   it is *self-funding* — reciprocal users serve their own keep. The pool funds only the **cold-start
   grant** for brand-new accounts (plus the paid-overflow subsidy). Governed parameters: the
   **cold-start grant size + refresh**, the fee-skim **φ** that refills the pool, and the
   **pool-health limit** on overflow subsidy — *not* a standing free allowance.
7. **Free-tier farming defenses — DECIDED (revised 2026-07-16):** tit-for-tat makes free-tier farming
   **largely intrinsic** — you can't consume free without contributing equal service (§8), so a pure
   leecher hits the deficit band and must pay. The residual defenses guard only the **cold-start
   grant** + paid-overflow subsidy: the **identity gate** (stake / invite / PoP) bounds cold-start
   cost; counterparty-signed cheques (§8-B — built) prevent inflation; protocol-picked producer +
   enforced placement (§8-A) stay as defense-in-depth for the overflow subsidy. Gate mechanism + grant
   size are governed at launch.
8. **Swarm-fetch payment — DECIDED:** a single prepaid **per-consumer egress balance** settled
   across all providers by `allocate_quota` (built, step 2 P1) — NOT a channel-open per provider.
9. **Escrow / relay — DECIDED:** relay is metered **separately** from egress (§7). The cheque
   **credit-band**, the **free-tier relay cap**, and the **per-hop margin** are governed
   parameters (defaults at launch); reclaim-on-timeout + the dispute lifecycle are deferred to
   the ledger build.

**Accounting — models decided; cadences GOVERNED-at-launch:**
10. **Epoch model — DECIDED shape:** fixed-window epochs; per-epoch claim; shadow→active cutover
    after a governed number of shadow epochs. Window length is governed (default set at launch
    to the settlement cadence).
11. **Storage — DECIDED shape:** per-epoch checkpoint; spent-set/evidence pruned on a governed
    window; ledger state pinned at the full erasure set (the durability gate). Exact cadences
    governed.

**Crypto — DEFERRED (unchanged):**
12. **PDP soundness** — the lattice-LHS milestone (needs a cryptographer). Gates ONLY the direct
    cold-storage-at-rest reward; the rest of the economy ships without it.

---

## 11. Sequencing — status (updated 2026-07-15, §10 resolved)

1. **Sequencer** (finish K7 auto-signing → non-equivocating, intersection-sized, per-account
   ordering mechanism). **DONE** (P1–P4b-2). Currently binds to a program's declared quorum;
   §10.5 upgrades selection to a rotating epoch committee (folded into step 4).
2. **Serving-cheque transport hook + measurement collection** — the measurement + egress
   substrate (§6/§7). **DONE** (P1 cheque core + P2 transport hook). Providers record cheques →
   `total_earned`; `allocate_quota` settles a consumer's paid quota across providers. Surfacing
   the measurement folds into the ledger (step 4), not a standalone metric.
3. ~~Participation metric (governed WASM, shadow mode)~~ — **DISSOLVED (§10.2):** paid demand
   *is* the metric; no separate contribution-oracle app. The shadow→active accounting
   (§6/§10.10) lives inside the ledger's settlement, not a standalone scorer. Organic-demand
   weighting stays available as optional defense-in-depth (§8-A), off by default.
4. **Token ledger app** — the active next build, now **UNBLOCKED** (§10 resolved). Two balances
   (tokens + credit), transfer, mint from measurement-justified receipts, egress settlement via
   `allocate_quota`, free-tier credit redemption — on verification [K6] + the sequencer. Ships
   mechanism-first with governed policy: issuance/genesis (§10.3, default fair-launch), quorum
   `(n,k)` (§10.4, default 4/3), fee φ + allowance (§10.6). Its heaviest sub-part is the
   **rotating epoch committee** for sequencer selection (§10.5) — epoch clock + deterministic
   committee fn + cross-epoch sequence hand-off; a genesis committee bootstraps it.
5. **PDP [K5]** later — hardens the cold-storage measurement; **deferred on §10.12
   (cryptographer).** Gates only the direct cold-storage-at-rest reward.

Mechanism (steps 1–2) is complete. With §10 resolved, **step 4 (the token-ledger app) is the
active next build**; step 3 dissolved into it; step 5 stays deferred.
