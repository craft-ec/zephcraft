# The ZephCraft Network — How It All Fits Together

**v1.0 — July 2026.** Consolidated network architecture: how identity, transport,
membership, routing, storage, relays, the dashboard/map, and the open-network
model compose. Derives from `craftec_technical_foundation.md` v3.7 and
`CRAFTOBJ_DESIGN.md` v2.0; where those give the *what*, this doc gives the
*how it hangs together*. Written after the 2026-07-03 design Q&A session.

---

## One paragraph

ZephCraft is a decentralized storage network with **no privileged roles**: a
node is an Ed25519 keypair running the `zeph` daemon, connectivity is
any-to-any QUIC (iroh), membership is tiny per-node partial views, content is
found by push-announced provider records (tracker now, Kademlia DHT later),
data survives churn through RLNC self-repair that needs no coordinator, and
every "global" facility — trackers, relays, bootstrap, governance — is a
service *anyone can run*, so the network is not bound to its founders.

## The layer stack

```
map / public site        reads tracker registries (union by node_id)
dashboard  (per node)    127.0.0.1 web UI + `zeph status` CLI      [live: MU.1-2]
────────────────────────────────────────────────────────────────
CraftOBJ storage         publish / distribute / health / repair    [M2]
content routing          ContentRouting trait: tracker → iroh DHT  [M2 → M3]
membership               partial views (HyParView-style)           [M1.5]
wire                     postcard frames, HLC, type tags           [M1.4]
────────────────────────────────────────────────────────────────
transport (iroh 1.0)     any-to-any QUIC, relays, hole-punch       [live: M1.2]
relay mesh               iroh-relay on every public node           [M1.8: static list]
identity                 Ed25519 keypair = NodeId = QUIC key       [live: M1.1]
```

Two things are commonly misread in this picture:

1. **Membership and the tracker are siblings, not a hierarchy.** Both ride
   directly on iroh. Membership decides which ~8 connections stay *warm* for
   gossip and failure detection; the tracker is a *well-known destination*
   every node dials directly. Partial views bound gossip, never reachability.
2. **iroh makes the network fully connected in potential.** Anyone who can
   name a node (id + addr, or via a relay) can dial it. Every global behavior
   — announces, fetches, repair pushes — is a direct dial to a named node.

## Scale properties

| Facility | Per-node state | Mechanism | Practical ceiling |
|---|---|---|---|
| Membership | O(log N): ~8 active + ~50 passive peers | partial views, epidemic join | millions (removed SWIM's full-table wall, §62.1) |
| Content lookup | O(log N) routing entries (≤5,120) | Kademlia point lookup, 1–3 hops | millions (foundation §3) |
| Relay | ~0 (a URL list) | most pairs hole-punch direct; relay = broker + ~25% fallback | 1M+ conns/relay server (§26) |
| Storage | what you volunteer | pieces spread by push/distribution | grows WITH node count (anti-blockchain) |

## The three flows

**Join**: read config → dial bootstrap peers (list of independent operators,
§18) → membership join propagates through views → announce to configured
trackers (signed, TTL) → dialable by anyone, visible on the map.

**Publish → whole-network availability**: encode segment into n RLNC pieces
(n = k·ceil(2 + 16/k)) → push ~2 pieces/peer to ≥K distinct peers — publish
reports durable ONLY at ≥K acks → holders announce provider records →
Distribution equalizes (>2 pieces? pass excess to have-nots) → anyone,
anywhere resolves the CID and pulls pieces directly. Content moves by
content-addressed PULL, never by flooding membership links.

**Repair (no coordinator, no publisher needed)**: each holder of ≥2 pieces
health-scans its CIDs (batched live AVAILABILITY_PROBE, PDP-discounted counts,
§62.1) → deficit found → rendezvous hash BLAKE3(node‖cid‖epoch) elects top-N
probe-confirmed holders → each recodes 1 fresh piece locally (RLNC: no decode,
no fetch) → vtag-verified, distributed. Data dies only if churn outruns this
loop (validated by the M2 DST harness).

## Trackers, the map, and the census caveat

A tracker is a **dumb, anyone-can-run bulletin board** (open-tracker model):
signed announces in, TTL expiry = liveness, queries out. Nodes multi-announce
to every tracker in config; map surfaces union registries by node_id — more
community trackers = more redundancy, not fragmentation. Signed announces
prevent identity spoofing.

**Fundamental caveat**: Kademlia cannot enumerate, so no protocol lists "all
nodes." The map is a *voluntary census* — `private = true` nodes work fully
but stay invisible (same property as Bitcoin node maps). Coverage stays high
because visibility is how nodes are found and (tracker era) how they receive
storage work.

## Relay mesh

Public-IP nodes run `iroh-relay` alongside `zeph` (§26): relays broker
hole-punches and carry fallback traffic for the ~25% behind symmetric NAT.
v1 (M1.8): static relay list in config, our Hetzner relay first, n0's public
relays demoted to last-resort bootstrap fallback. Target: relay capability
advertised + discovered like storage capability — the relay mesh grows exactly
as the storage mesh does. Needs per-relay: hostname + TLS (ACME automatic).

## The open-network ladder (founder-proofing)

The protocol has **no seat only we can hold** — keypair identity, local
admission (quotas/reciprocity), kernel-level self-healing, MIT license. The
remaining founder-dependencies are operational/social, each with a removal
step (tracked as M-OPEN):

1. **See it**: open-source code + specs (recommended at M2 — working network
   first impression). Spec matters as much as code: enables re-implementation.
2. **Join without us**: bootstrap = list of independently-operated seeds;
   fully user-editable config.
3. **Look up without us**: iroh Kademlia DHT behind ContentRouting (M3) —
   lookup collectively owned, no tracker to shut down.
4. **Traverse NAT without us**: community relay mesh (above).
5. **Upgrade without us**: network-agent whitelist governance Phase 1 (our
   key) → Phase 2 (k-of-n independent maintainers) — the transition is itself
   a signed governance action, no binary change (§48).

If the founders vanish today, running nodes keep storing, serving, healing
indefinitely; what's lost is only upgrade coordination — which the ladder
distributes.

## Live today vs. target (2026-07-03)

| | Today | Target |
|---|---|---|
| Nodes | Hetzner (24/7 systemd) + Mac ad hoc | anyone, any OS, one binary |
| Connectivity | real-internet QUIC, NAT traversal proven | same + our relay mesh |
| Membership | static peer list in config | partial views (M1.5) |
| Wire | ad-hoc ping payload | postcard frames + HLC (M1.4) |
| Storage | none yet | full CraftOBJ lifecycle (M2) |
| Lookup | none yet (peers by config) | tracker (M2) → DHT (M3) |
| UI | dashboard + `zeph status` (live) | + storage view, node map (MU.3–4) |
| Openness | private repo, local commits | M-OPEN ladder |

Progress and gates: `.claude/features/zephcraft.md`. Build discipline:
strictly sequential, walking-skeleton order (root `CLAUDE.md`).
