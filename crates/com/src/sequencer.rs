//! Ordering sequencer — per-account, non-equivocating, quorum-serialized writes
//! (`ECONOMIC_LAYER_DESIGN.md` §4/§5; §11 step 1). The one thing verification [K6] cannot provide:
//! **uniqueness**. Verification checks *consistency* ("is `f(x)=y`?"); a double-spend is two
//! individually-valid writes at the same account nonce, and choosing which is canonical is
//! *agreement*, which local re-execution cannot give. This substrate serializes writes to each
//! account's nonce sequence through a k-of-n quorum, so at most one write ever commits per
//! `(account, nonce)` — a fork is impossible at commit, not detected-and-slashed after.
//!
//! It is the attestation substrate ([`crate::attestation`]) SPECIALIZED for ordering, not a new
//! primitive: it reuses [`Quorum`] (members + threshold), [`Quorum::count_signers`] (the one k-of-n
//! crypto count), and [`MemberSignature`]. Two properties distinguish it from plain attestation:
//!   1. **Per-account nonces** — a [`SequencedWrite`] targets `(account, nonce)`, and an
//!      [`AccountSequence`] is the strict sequential fold of commits for one account. (Attestation
//!      sequences the quorum's OWN config seq; here the quorum is a stable per-shard config and the
//!      nonce belongs to the account.)
//!   2. **Quorum-intersection sizing** — the quorum must be [`Quorum::is_intersection_sized`]
//!      (`2k>n`), so two conflicting writes cannot each gather a *disjoint* `k`.
//!
//! **Non-equivocation is a STRUCTURAL invariant, not a policy** (§5): a [`SequencerMember`] signs at
//! most ONE write per `(account, nonce)` — idempotent for the identical write, a hard refusal for a
//! different one — regardless of any signing policy above it. The binary owns this invariant; a
//! (later) auto-sign policy program owns only the discretion to sign or decline. Together with
//! intersection sizing, this is what makes the fork impossible: a shared honest member is in every
//! quorum and refuses the second conflicting write, so only one can reach `k`.
//!
//! This module (P1) is the pure, offline core — the types, the fold, the intersection check, and the
//! member's non-equivocation. The write host fn, the node serialization service, and cross-node
//! propagation ride on top in later phases.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use zeph_core::{Cid, NodeId};
use zeph_crypto::NodeIdentity;

use crate::attestation::{MemberSignature, Quorum};

/// Domain tag separating a quorum-member ORDERING signature from every other ed25519 use (attestation
/// member sign-offs, owner genesis, governance). A sequencer member sig can never be replayed elsewhere.
const SEQUENCER_DOMAIN: &[u8] = b"craftec/sequencer/1";

/// Domain tag for the ACCOUNT OWNER's authorization signature over a write. Distinct from
/// `SEQUENCER_DOMAIN` so an owner authorization can never be replayed as a quorum-member ordering sig.
const OWNER_DOMAIN: &[u8] = b"craftec/sequencer-owner/1";

/// One write to an account's nonce slot — the unit the quorum orders. `payload` is opaque (e.g. a
/// token-ledger transaction); the sequencer only orders it, never interprets it. `owner_sig` is the
/// ACCOUNT owner's signature authorizing this write (the account IS an ed25519 pubkey) — the
/// **owner-authenticity gate**: only the owner's writes enter the account's sequence, so a third
/// party cannot front-run / grief the account's nonces (`ECONOMIC_LAYER_DESIGN.md` §4; like Ethereum's
/// signed-by-sender rule). App-specific validity (e.g. sufficient balance) is NOT checked here — that
/// is the program's transition fn, enforced by verification. (A program-derived account with no
/// backing key needs a different, future authorization path — `owner_authentic` will reject it.)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SequencedWrite {
    /// The account whose nonce sequence this write extends — an ed25519 public key.
    pub account: [u8; 32],
    /// The slot this write claims — must equal the account's current length (the next free nonce).
    pub nonce: u64,
    /// The opaque write payload (the sequencer orders it; the app interprets it).
    pub payload: Vec<u8>,
    /// The account owner's ed25519 signature over `(account, nonce, payload)` — the authorization that
    /// this write is really from the account. Checked by [`owner_authentic`](Self::owner_authentic).
    pub owner_sig: Vec<u8>,
}

/// The bytes the ACCOUNT OWNER signs to authorize a write — domain-tagged, covering
/// `(account, nonce, payload)` but NOT `owner_sig` itself (which would be circular).
fn authored_bytes(account: &[u8; 32], nonce: u64, payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(OWNER_DOMAIN.len() + 40 + payload.len());
    b.extend_from_slice(OWNER_DOMAIN);
    b.extend_from_slice(account);
    b.extend_from_slice(&nonce.to_le_bytes());
    b.extend_from_slice(payload);
    b
}

impl SequencedWrite {
    /// Author a write AS the account owner: `account` = the signer's identity, `owner_sig` = the
    /// owner's signature over `(account, nonce, payload)`. Only the holder of the account key can
    /// produce this — the owner-authenticity gate. (The token-ledger flow: the user signs their own
    /// transfer client-side; the app hands the authored write to the sequencer.)
    pub fn author(account_identity: &NodeIdentity, nonce: u64, payload: Vec<u8>) -> Self {
        let account = account_identity.node_id().0;
        let owner_sig = account_identity
            .sign(&authored_bytes(&account, nonce, &payload))
            .to_vec();
        Self {
            account,
            nonce,
            payload,
            owner_sig,
        }
    }

    /// Whether `owner_sig` is a valid signature by `account` (the pubkey) over this write — the
    /// owner-authenticity check the sequencer runs before ordering. `false` for an unauthenticated or
    /// forged write, or an `account` that is not a real pubkey (a program-derived address, which needs
    /// a different — future — authorization path).
    pub fn owner_authentic(&self) -> bool {
        let Ok(sig) = <[u8; 64]>::try_from(self.owner_sig.as_slice()) else {
            return false;
        };
        NodeIdentity::verify(
            &NodeId(self.account),
            &authored_bytes(&self.account, self.nonce, &self.payload),
            &sig,
        )
    }

    /// The bytes a quorum member signs to ORDER this write — domain-tagged, covering the whole authored
    /// write (including `owner_sig`), so a member orders exactly this authenticated write at this slot.
    pub fn signing_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(SEQUENCER_DOMAIN.len() + 40 + self.payload.len());
        b.extend_from_slice(SEQUENCER_DOMAIN);
        b.extend_from_slice(&postcard::to_allocvec(self).unwrap_or_default());
        b
    }

    /// A quorum member signs this write to order it. Non-equivocation is enforced by [`SequencerMember`],
    /// not here — this is the raw signature (ed25519 is deterministic, so it is idempotent per member).
    pub fn sign(&self, identity: &NodeIdentity) -> MemberSignature {
        MemberSignature {
            member: identity.node_id().0,
            signature: identity.sign(&self.signing_bytes()).to_vec(),
        }
    }
}

/// A [`SequencedWrite`] plus the collected member signatures — the committed, k-of-n-authorized unit.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SequencedCommit {
    pub write: SequencedWrite,
    pub signatures: Vec<MemberSignature>,
}

impl SequencedCommit {
    /// Authorized against `q` iff the write is **owner-authentic** (signed by the account), the quorum
    /// is **intersection-sized**, AND ≥ `q.threshold` distinct members signed this exact write. The
    /// owner-authenticity gate keeps a third party from ordering writes into an account it doesn't own;
    /// intersection sizing stops two conflicting writes from each gathering a disjoint `k`.
    pub fn authorizes(&self, q: &Quorum) -> bool {
        self.write.owner_authentic()
            && q.is_intersection_sized()
            && q.count_signers(&self.write.signing_bytes(), &self.signatures) >= q.threshold
    }
}

/// The strict, sequential fold of committed writes for ONE account — the account's ordered write
/// log. `commits[i].write.nonce == i`, so nonce == position: exactly one write occupies each slot,
/// and a conflicting write at an already-filled slot is simply not the next nonce and is refused.
/// This is where "one writer per nonce" holds structurally, the same way
/// [`crate::attestation::QuorumChain`] enforces one attestation per config seq.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccountSequence {
    pub account: [u8; 32],
    pub commits: Vec<SequencedCommit>,
}

impl AccountSequence {
    pub fn new(account: [u8; 32]) -> Self {
        Self {
            account,
            commits: Vec::new(),
        }
    }

    /// The next free nonce = the current length.
    pub fn next_nonce(&self) -> u64 {
        self.commits.len() as u64
    }

    /// Append `commit` iff it validly extends this account's sequence under `q`: it targets THIS
    /// account, claims exactly the next nonce, and is quorum-authorized. Returns success. A commit
    /// for a filled slot fails (its nonce != next), so a fork can't overwrite a committed write.
    pub fn append(&mut self, commit: SequencedCommit, q: &Quorum) -> bool {
        if commit.write.account == self.account
            && commit.write.nonce == self.next_nonce()
            && commit.authorizes(q)
        {
            self.commits.push(commit);
            true
        } else {
            false
        }
    }

    /// Fold the whole sequence under `q`: `true` iff EVERY commit is well-formed (right account,
    /// sequential nonce, quorum-authorized). A tampered or gap-y sequence rejects wholesale, so a
    /// forged commit can't be adopted mid-chain.
    pub fn verify(&self, q: &Quorum) -> bool {
        self.commits.iter().enumerate().all(|(i, c)| {
            c.write.account == self.account && c.write.nonce == i as u64 && c.authorizes(q)
        })
    }

    /// The committed payload at `nonce`, if any (the ordered read the app consumes).
    pub fn payload_at(&self, nonce: u64) -> Option<&[u8]> {
        self.commits
            .get(nonce as usize)
            .map(|c| c.write.payload.as_slice())
    }

    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).unwrap_or_default()
    }
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes).ok()
    }
    /// Content id of the encoded sequence (for durable publish / cross-node adoption).
    pub fn root(&self) -> [u8; 32] {
        Cid::of(&self.encode()).0
    }
}

/// A quorum member's SIGNING side, carrying the **non-equivocation invariant** structurally: it
/// signs at most one write per `(account, nonce)`. This is the safety kernel — a member that has
/// signed write A at `(acct, n)` will (a) re-issue the SAME signature for the identical write
/// (idempotent — safe to retry) and (b) REFUSE any different write at `(acct, n)` (the hard
/// invariant). No signing policy can override this; a later auto-sign policy program decides only
/// *whether* to offer a write to [`sign`](SequencerMember::sign), never whether the invariant holds.
///
/// (Phase 1 keeps the signed-set in memory; the persistent, crash-safe version is a later phase. The
/// invariant and its tests live here so the kernel is nailed down before it is wired to the node.)
pub struct SequencerMember {
    identity: NodeIdentity,
    /// `(account, nonce)` → the write this member already committed to at that slot.
    signed: HashMap<([u8; 32], u64), SequencedWrite>,
}

impl SequencerMember {
    pub fn new(identity: NodeIdentity) -> Self {
        Self {
            identity,
            signed: HashMap::new(),
        }
    }

    pub fn node_id(&self) -> [u8; 32] {
        self.identity.node_id().0
    }

    /// Sign `write`, honoring non-equivocation. `Some(sig)` if this member has not signed a
    /// DIFFERENT write at `(account, nonce)` (idempotent for the identical write); `None` — a hard
    /// refusal — if it already signed a conflicting one. This is the structural invariant.
    pub fn sign(&mut self, write: &SequencedWrite) -> Option<MemberSignature> {
        if !write.owner_authentic() {
            return None; // not authorized by the account owner — never order it
        }
        let slot = (write.account, write.nonce);
        match self.signed.get(&slot) {
            Some(prev) if prev != write => None, // equivocation — hard refusal
            _ => {
                self.signed.insert(slot, write.clone());
                Some(write.sign(&self.identity))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members(n: usize) -> Vec<NodeIdentity> {
        (0..n).map(|_| NodeIdentity::generate()).collect()
    }
    fn quorum(ids: &[NodeIdentity], k: usize) -> Quorum {
        Quorum::genesis(ids.iter().map(|i| i.node_id().0).collect(), k)
    }
    fn account() -> NodeIdentity {
        NodeIdentity::generate()
    }
    /// Author an owner-authentic write as `owner` (the account = `owner`'s pubkey).
    fn write(owner: &NodeIdentity, nonce: u64, payload: &[u8]) -> SequencedWrite {
        SequencedWrite::author(owner, nonce, payload.to_vec())
    }
    /// A commit where `signers` each sign `w` directly (no non-equivocation tracking).
    fn commit(w: &SequencedWrite, signers: &[&NodeIdentity]) -> SequencedCommit {
        SequencedCommit {
            write: w.clone(),
            signatures: signers.iter().map(|s| w.sign(s)).collect(),
        }
    }

    #[test]
    fn intersection_sizing_is_2k_gt_n() {
        let ids = members(3);
        assert!(
            !quorum(&ids, 1).is_intersection_sized(),
            "1-of-3: 2<3, two disjoint 1-quorums exist"
        );
        assert!(
            quorum(&ids, 2).is_intersection_sized(),
            "2-of-3: 4>3, any two 2-subsets overlap"
        );
        let ids4 = members(4);
        assert!(
            !quorum(&ids4, 2).is_intersection_sized(),
            "2-of-4: 4==4, {{0,1}} and {{2,3}} are disjoint — NOT intersection-sized"
        );
        assert!(quorum(&ids4, 3).is_intersection_sized(), "3-of-4: 6>4");
    }

    #[test]
    fn byzantine_tolerance_matches_sizing() {
        // f = 2k − n − 1 equivocating (double-signing) members tolerated.
        assert_eq!(
            quorum(&members(3), 2).byzantine_tolerance(),
            0,
            "2-of-3 → f=0"
        );
        assert_eq!(
            quorum(&members(4), 3).byzantine_tolerance(),
            1,
            "3-of-4 → f=1"
        );
        assert_eq!(
            quorum(&members(7), 5).byzantine_tolerance(),
            2,
            "5-of-7 (2f+1 of 3f+1, f=2)"
        );

        let acct = account();
        let a = write(&acct, 0, b"alice");
        let b = write(&acct, 0, b"bob");

        // n=3, k=2 (f=0): a single Byzantine double-signer forces a fork — honest m0 signs A, honest
        // m1 signs B, Byzantine m2 signs BOTH, so both commits reach k=2. Two nodes could each adopt
        // one — exactly the equivocation intersection sizing is meant to stop, and 2-of-3 can't.
        let n3 = members(3);
        let q3 = quorum(&n3, 2);
        let fork_a = commit(&a, &[&n3[0], &n3[2]]); // honest m0 + Byzantine m2
        let fork_b = commit(&b, &[&n3[1], &n3[2]]); // honest m1 + Byzantine m2
        assert!(
            fork_a.authorizes(&q3) && fork_b.authorizes(&q3),
            "2-of-3 tolerates 0 Byzantine: one double-signer forks it"
        );

        // n=4, k=3 (f=1): the SAME single Byzantine cannot force a fork. To reach k=3, A needs 2
        // honest signers; only 1 honest remains for B, so with the Byzantine, B tops out at 2 < 3.
        let n4 = members(4);
        let q4 = quorum(&n4, 3);
        let ok_a = commit(&a, &[&n4[0], &n4[1], &n4[3]]); // 2 honest + Byzantine m3
        let best_b = commit(&b, &[&n4[2], &n4[3]]); // last honest + Byzantine m3 = 2 sigs
        assert!(ok_a.authorizes(&q4), "A commits with 3 signers");
        assert!(
            !best_b.authorizes(&q4),
            "3-of-4 tolerates 1 Byzantine: the second write cannot reach k=3 → no fork"
        );
    }

    #[test]
    fn commit_requires_intersection_sized_quorum() {
        let ids = members(3);
        let acct = account();
        let w = write(&acct, 0, b"pay alice");
        // 1-of-3 is NOT intersection-sized → a commit is refused even with a valid signature.
        let non_iso = quorum(&ids, 1);
        assert!(
            !commit(&w, &[&ids[0]]).authorizes(&non_iso),
            "a non-intersection-sized quorum cannot authorize a sequenced write"
        );
        // 2-of-3 IS intersection-sized → 2 signers authorize; 1 does not.
        let iso = quorum(&ids, 2);
        assert!(commit(&w, &[&ids[0], &ids[1]]).authorizes(&iso));
        assert!(!commit(&w, &[&ids[0]]).authorizes(&iso), "1 signer < k=2");
    }

    #[test]
    fn member_refuses_to_equivocate_but_is_idempotent() {
        let mut m = SequencerMember::new(NodeIdentity::generate());
        let acct = account();
        let a = write(&acct, 0, b"pay alice");
        let b = write(&acct, 0, b"pay bob"); // SAME (account, nonce), different payload

        let sig_a = m.sign(&a).expect("first sign at (acct,0) allowed");
        // idempotent: signing the identical write again returns the same signature.
        let sig_a2 = m.sign(&a).expect("idempotent re-sign allowed");
        assert_eq!(
            sig_a, sig_a2,
            "ed25519 is deterministic → identical signature"
        );
        // equivocation: a DIFFERENT write at the same slot is hard-refused.
        assert!(
            m.sign(&b).is_none(),
            "member must refuse a conflicting same-nonce write"
        );
        // a different nonce is fine.
        assert!(m.sign(&write(&acct, 1, b"pay bob")).is_some());
    }

    #[test]
    fn two_conflicting_writes_cannot_both_commit() {
        // n=3, k=2 (intersection-sized). Two conflicting writes race for (acct, 0). With honest
        // members (each refuses to double-sign), only one can reach k=2 → the fork is impossible.
        let ids = members(3);
        let q = quorum(&ids, 2);
        let acct = account();
        let a = write(&acct, 0, b"pay alice");
        let b = write(&acct, 0, b"pay bob");
        let mut m: Vec<SequencerMember> = ids.into_iter().map(SequencerMember::new).collect();

        // Write A gathers m0, m1.
        let a_sigs: Vec<MemberSignature> =
            [0usize, 1].iter().filter_map(|&i| m[i].sign(&a)).collect();
        let commit_a = SequencedCommit {
            write: a.clone(),
            signatures: a_sigs,
        };
        assert!(commit_a.authorizes(&q), "A reaches k=2");

        // Write B now tries ALL three members. m0,m1 already signed A → refuse; only m2 can sign.
        let b_sigs: Vec<MemberSignature> = [0usize, 1, 2]
            .iter()
            .filter_map(|&i| m[i].sign(&b))
            .collect();
        assert_eq!(
            b_sigs.len(),
            1,
            "only the one member that had not signed A can sign B"
        );
        let commit_b = SequencedCommit {
            write: b,
            signatures: b_sigs,
        };
        assert!(
            !commit_b.authorizes(&q),
            "B cannot reach k=2 → the fork is impossible at commit"
        );
    }

    #[test]
    fn account_sequence_enforces_sequential_nonce_and_binding() {
        let ids = members(3);
        let q = quorum(&ids, 2);
        let acct = account();
        let mut seq = AccountSequence::new(acct.node_id().0);

        // nonce 0 appends.
        assert!(seq.append(commit(&write(&acct, 0, b"n0"), &[&ids[0], &ids[1]]), &q));
        // a second write claiming nonce 0 (a fork) is refused — the slot is filled.
        assert!(!seq.append(commit(&write(&acct, 0, b"fork"), &[&ids[1], &ids[2]]), &q));
        // skipping to nonce 2 is refused (must be sequential).
        assert!(!seq.append(commit(&write(&acct, 2, b"gap"), &[&ids[0], &ids[1]]), &q));
        // nonce 1 appends.
        assert!(seq.append(commit(&write(&acct, 1, b"n1"), &[&ids[1], &ids[2]]), &q));
        assert_eq!(seq.next_nonce(), 2);

        // a commit for a DIFFERENT account is refused.
        let other = account();
        assert!(!seq.append(
            commit(&write(&other, 2, b"wrong-acct"), &[&ids[0], &ids[1]]),
            &q
        ));

        assert_eq!(seq.payload_at(0), Some(&b"n0"[..]));
        assert_eq!(seq.payload_at(1), Some(&b"n1"[..]));
        assert_eq!(seq.payload_at(2), None);
        assert!(seq.verify(&q));
    }

    #[test]
    fn tampered_sequence_fails_verify() {
        let ids = members(3);
        let q = quorum(&ids, 2);
        let acct = account();
        let mut seq = AccountSequence::new(acct.node_id().0);
        assert!(seq.append(commit(&write(&acct, 0, b"real"), &[&ids[0], &ids[1]]), &q));
        assert!(seq.verify(&q));

        // flip a signature byte → the k-of-n count drops below threshold → verify rejects.
        let mut bad = seq.clone();
        bad.commits[0].signatures[0].signature[0] ^= 0xFF;
        assert!(!bad.verify(&q), "a tampered commit fails the fold");

        // rewrite a payload without re-signing → signatures no longer match signing_bytes → rejects.
        let mut bad2 = seq.clone();
        bad2.commits[0].write.payload = b"forged".to_vec();
        assert!(
            !bad2.verify(&q),
            "a mutated payload invalidates the signatures"
        );
    }

    #[test]
    fn encode_decode_roundtrips() {
        let ids = members(3);
        let q = quorum(&ids, 2);
        let acct = account();
        let mut seq = AccountSequence::new(acct.node_id().0);
        seq.append(commit(&write(&acct, 0, b"x"), &[&ids[0], &ids[1]]), &q);
        let back = AccountSequence::decode(&seq.encode()).unwrap();
        assert_eq!(back, seq);
        assert!(back.verify(&q));
        assert_eq!(back.root(), seq.root());
    }

    #[test]
    fn an_unauthenticated_write_is_never_ordered() {
        let ids = members(3);
        let q = quorum(&ids, 2);
        let acct = account();
        // A write whose owner_sig is garbage is NOT owner-authentic.
        let mut forged = write(&acct, 0, b"steal");
        forged.owner_sig = vec![0u8; 64];
        assert!(
            !forged.owner_authentic(),
            "a bad owner_sig fails authenticity"
        );
        // Even a full k-of-n of member signatures does not authorize it — the gate refuses it.
        let c = commit(&forged, &[&ids[0], &ids[1]]);
        assert!(
            !c.authorizes(&q),
            "the owner-authenticity gate refuses an unauthenticated write"
        );
        // A member also refuses to sign it.
        let mut m = SequencerMember::new(NodeIdentity::generate());
        assert!(
            m.sign(&forged).is_none(),
            "a member never orders an unauthenticated write"
        );
        // A write signed by a DIFFERENT key than the account (an impostor claiming `acct`) is refused.
        let impostor = account();
        let mut spoof = SequencedWrite::author(&impostor, 0, b"spoof".to_vec());
        spoof.account = acct.node_id().0; // claim acct, but the sig is the impostor's
        assert!(
            !spoof.owner_authentic(),
            "a write signed by a non-owner is refused"
        );
    }
}
