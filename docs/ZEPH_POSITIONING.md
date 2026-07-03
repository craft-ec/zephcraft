# ZephCraft — Landing Page Positioning

**Captured 2026-07-03.** The thesis and messaging for `zeph.craft.ec`. This is
the *why*, distilled from the storage-design discussions. The page is built
after M1 exit (done) — likely alongside/after M2 so it can lead with a real
demo. Frontend-design treatment when built.

---

## The one-sentence thesis

**The world's shared storage grid — every device contributes spare disk into
one content-addressed pool that replicates only what matters.**

The internet already has the hardware: billions of devices with idle storage.
ZephCraft turns that into a single elastic storage utility — you draw from it,
you contribute to it, and the network keeps alive exactly what's needed, no
more.

## The mental model: a grid, not a backup

You don't each own a generator sized to your peak load — you draw from a grid
sized to *aggregate* demand. Storage should work the same way. Today every
service over-provisions its own silo; ZephCraft pools spare capacity so the
network stores things once, sized to what data actually needs, and balances
the exchange with reciprocity (give disk, use disk).

More devices join → **more capacity, not more redundancy.** That linear scaling
is the property both the cloud and blockchains lack.

## The core claim: replicate only what matters

Redundancy in ZephCraft is *engineered to need*, from three independent knobs:

- **Erasure coding sets the floor** — ~3× redundancy (RLNC, n=96 for K=32)
  survives losing two-thirds of holders. Naive full-copy replication needs
  ~18–30 copies for the same durability under churn. Durability tax: 3×, not 20×.
- **Demand scaling** adds providers only where there's live traffic and sheds
  them when demand fades. Temporary, bandwidth-driven.
- **Pins** are the intentional "this matters" anchor — content someone chooses
  to guarantee.

Plus **global dedup** (same bytes = one CID = stored once) and **the fade**
(content nobody wants and nobody pins is allowed to disappear — a feature, not
a bug: the network never pays to keep garbage alive).

Net: `replication ≈ durability floor + live demand + intentional pins`.

## The contrasts (the page's spine)

| | Their redundancy | The problem |
|---|---|---|
| **BitTorrent** | Accidental — a side-effect of who's online (popular = 1000× copies, unpopular = 1× then 0) | Simultaneously **wasteful** (whole-file copies, no erasure, no dedup) AND **fragile** (no floor, no repair — dies when the last seed leaves) |
| **Cloud (S3, etc.)** | Hidden in someone's data center you rent | You don't own it, you pay forever, one company can revoke it |
| **Blockchain** | Total — every node stores everything | O(N) waste; more nodes ≠ more capacity |
| **ZephCraft** | Engineered to need — floor + demand + pins | Leaner *and* more durable at once; distribution, not replication |

The sharp line for the page: **BitTorrent's redundancy is an accident of
participation; ZephCraft's is engineered to what the data needs.**

## Why it's possible now (the mechanisms, briefly)

- **Content addressing (BLAKE3 CIDs)** → dedup + integrity for free.
- **RLNC erasure coding** → cheap durability + repair *without decoding* (any
  2-piece holder mints fresh pieces) → survives churn, no rare-piece problem.
- **Protocol-driven lifecycle** → demand-proportional, self-sizing.
- **Pinning** → intentional anchors, BitTorrent's "seed forever" done right.
- **Single-writer, no consensus** → scales linearly, not O(N).

## Audience & the page's single job

- **Primary**: technically-literate builders and node operators — people who
  can run `zeph` and grasp "shared storage grid." Their action: run a node /
  read the design.
- **The page's one job**: make "the internet as one shared storage grid,
  replicating only what matters" *click* in 30 seconds, then hand off to either
  "run a node" (quickstart) or "how it works" (the design docs / this thesis).
- Eventually hosts the **live network map** (MU.4) — the single most persuasive
  artifact a volunteer network has (see Helium coverage maps, Bitcoin node
  maps): your dot appears when you join.

## Headline candidates (for the design pass to choose/refine)

- "The internet's shared storage grid."
- "Store once. Everywhere. Only what matters."
- "Spare disk on every device, pooled into one network."
- "Not the cloud. Not BitTorrent. A storage grid."

## Honest guardrails — do NOT overclaim

The page must not claim what the code hasn't proven. As of 2026-07-03:

- **Proven (M1)**: coded pieces move and verify end-to-end across the internet
  (Mac ↔ Hetzner via our own relay), byte-identical restore, per-piece + whole-
  content verification. This is the *data plane*, a transfer proof.
- **NOT yet proven**: the self-sizing behaviors — demand scaling, self-healing
  repair, the fade — are M2, and "what matters survives, the rest fades under
  real churn" is validated by the M2 DST harness. Until then the thesis is a
  sound *argument*, not a *measurement*.
- Therefore: the launch page ships when M2 makes the claims demonstrable. Pre-M2
  framing (if any) says "building" not "proven." No "11 nines," no fake metrics,
  no claiming durability the network can't yet show.

## Links the page draws from

- Thesis mechanics: `docs/ZEPHCRAFT_NETWORK.md`, `docs/CRAFTOBJ_DESIGN.md` v2.0
- Foundation: `docs/craftec_technical_foundation.md` v3.7
- The comparison research: 0G / competitive notes (session history)
