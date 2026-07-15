# Economic Layer & Participation Metrics — design

Status: DESIGN + PARTIAL BUILD (updated 2026-07-15). Captures the 2026-07-15 economic-layer
decisions. **§10 is now RESOLVED:** the distribution policy is **reward ∝ paid demand**, and
token issuance/genesis, quorum sizing `(n,k)`, and the free-tier parameters ship as **governed
parameters** (native default fair-launch), not baked constants. Mechanism is **built**: the
ordering sequencer (§4) and the serving-cheque + measurement substrate (§7) are live (§11 steps
1–2). The **token-ledger app** (§11 step 4) is the active next build.

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
  re-execution, not a committee."*
- **Two balance types per account** — liquid **tokens** (transferable, earnable,
  withdrawable) and non-transferable consume-only **credit** (the free tier, §8).
  Consumption draws credit-first, then tokens.
- **Ordering / uniqueness = attestation sequencer [K7]** (§4) — the one thing
  verification cannot provide.
- **Mint = measurement-justified issuance.** A provider submits its signed receipts /
  measurement evidence → a deterministic reward function (the distribution policy §10.1,
  run by the accounting pipeline §6) → verification [K6] re-runs it to confirm the mint
  amount → tokens minted. Receipts / evidence are single-use (can't be re-minted).
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
| **Reward valuation** — measurement → token amount; quorum **sizing** `(n,k)` | **Program** | Economic knobs, governed (default n=4/k=3, §10.4). |
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
  persistent*, not forgotten per swarm. It is a *settlement optimization*, **distinct
  from the free tier (§8), which is a real subsidy.**

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

## 8. Cold-start & the free tier — non-transferable consume-only credit

**Decided 2026-07-15.** A new account has zero tokens (demand-side cold-start), and we
want a *permanent* free tier for adoption — not just a one-time faucet. Decision: a
**recurring, non-transferable, consume-only credit allowance per identity, subsidized by
the general network** (freemium — paid users and token holders fund the free users).

**Two balances per account** (see §3):

| Balance | Properties |
|---|---|
| **tokens** | liquid, transferable, earnable, withdrawable — the real unit of value |
| **credit** | non-transferable, consume-only, per-identity allowance, network-funded, refreshed per epoch |

Consumption draws **credit-first, then tokens**.

**Credit is a network-honored voucher — the provider is *always* paid in real tokens.**
From the consumer's token balance (paid tier) or from the **subsidy pool** (free-tier
credit); the spent credit is then burned. So providers happily serve free-tier traffic
(they still get real tokens), and the free tier's cost is *socialized* rather than dumped
on whoever happens to serve a free user.

**Free vs paid — the product boundary (what actually differs).** The free tier is *not* "paid,
subsidised" — it is a deliberately **bounded, consume-only, pool-gated** slice. The limits *are*
the product boundary, and the reason anyone pays:

| Axis | Free (credit) | Paid (tokens) |
|---|---|---|
| **Scale** | small allowance per identity/epoch, expires | buy any volume |
| **Reliability** | pool-subsidised → throttled / best-effort under pool pressure | self-funded → always admitted, never pool-gated |
| **Durability / publish** | consume-only — *read* the shared pool | owner-pays-pin: publish + persist your own data, run a service |
| **Value** | non-transferable, expiring voucher | liquid — transferable, earnable (serve-to-earn), withdrawable |

The reliability gap is **not** provider discrimination (the cheque is tier-blind, §7) — it is at
*admission*: a free fetch is gated on allowance + pool health, a paid fetch is not. **What drives
conversion:** you pay the moment you need *scale* (outgrow the allowance), *reliability* (can't be
throttled), *durability* (publish/persist your own data — the north-star product; free is
consume-only and can host nothing), or *to earn/transact* (free credit can't be earned,
transferred, or withdrawn). One line: **free lets you *use* the network; paid lets you *build on*
it.** The tier is kept thin on purpose — enough to hook, not to substitute — and sized to pool
health so it can't be drained; that thinness is what makes the freemium self-funding (paid funds
free).

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

**Farming resistance — the threat model.** Three *distinct* threats, easily conflated:

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
  - **Resolved shape — metered-reward with subsidized overflow (2026-07-15).** *Reward =
    metered paid-usage revenue, **capped per-consumer at what they paid**. Overflow beyond the
    paid quota is subsidized: **cost-reimbursed (break-even, unrewarded) and
    best-effort/throttled/pool-bounded**.* This gives the flat/"unlimited" consumer UX while
    keeping the farm closed on all sides:
    - **Metered (paid) band → reward** (profit), capped at the consumer's payment → a
      self-dealer earns at most what it paid for the quota = **strictly zero-sum**. This is
      *direct revenue, not a pool-split* (satisfies the condition above automatically).
    - **Overflow band → cost-reimbursed** (provider made whole so it *will* serve, but no
      profit) → break-even self-dealing gains nothing = **no farm**.
    - **Overflow is aggregate-bounded** (throttled + pool-health-limited via the §8
      self-balancing allowance) → even zero-profit unlimited fetching can't drain the pool
      unboundedly. "Unlimited" = best-effort continuation, not unbounded full-speed resource.
    Net: zero-sum-safe reward (self-inflation impossible), bounded subsidy cost, flat consumer
    UX. Producer randomization is then *not* needed for farm-safety here (it stays useful for
    load-balancing). The *free* tier is the overflow band with a zero paid-quota — same
    mechanics, quota-bounded by the identity gate + allowance. See §10.1/§10.2 — this simplifies the metric toward "reward ∝ paid
demand," with cold storage via owner-pays-pin, consensus work fee-funded, bootstrap
subsidized (the three gaps paid-egress alone doesn't cover).

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
1. **Distribution policy — DECIDED: reward ∝ paid demand.** Metered-reward with subsidized
   overflow. Reward is direct **revenue** (not a contribution-pool split) and **linear in
   revenue** (no volume/reputation superlinearity → self-inflation-proof); **metered /
   quota-bounded**, never flat-unlimited (a flat-unlimited marginal self-fetch is free →
   farmable). Reward is capped per-consumer at paid usage; overflow is cost-reimbursed +
   pool-bounded (the free tier is this with a zero paid-quota). Cold storage = **owner-pays-pin**;
   consensus work = **fee-funded**; bootstrap = **subsidized**. The market prices real demand
   directly and is auto-farm-resistant (§8). *This is the spine.*
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
6. **Free-tier funding — DECIDED shape:** recycled fee-skim φ + tapering bootstrap issuance,
   the allowance a self-balancing function of pool health. The **fee rate φ**, the
   **issuance-taper schedule**, and the **allowance function** are governance parameters set at
   launch (not baked).
7. **Free-tier farming defenses — DECIDED:** primary = **protocol-picked producer + enforced
   piece placement** (§8-A) so a self-dealer can't guarantee its node is the paid one;
   anti-inflation = counterparty-signed cheques (§8-B — built, step 2). Cost bound = the
   **identity gate** (stake / invite / PoP) + allowance size + refresh cadence + pool-health
   sizing (§8-C): the gate mechanism is chosen at launch (governed), allowance refreshes per
   epoch. Optional extra safety (route free-serving through the sybil-normalized metric §10.2)
   stays off unless needed.
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
