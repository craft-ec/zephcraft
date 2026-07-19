//! The ECONOMIC STORE — the network's economic state, held in CraftSQL rather than inlined in every
//! epoch record.
//!
//! **Why a database.** The state is O(accounts): per-payer watermarks, per-pair served watermarks,
//! per-account subsidy eligibility, and the outstanding claim set. Carrying that inside `RewardRecord`
//! put the whole thing into EVERY epoch record. Here it is indexed and queryable, commits are
//! O(changed) (unchanged CraftSQL pages are content-addressed and deduplicated), and the record keeps
//! only a 32-byte commitment.
//!
//! **Why the record commits to a ROW HASH and not the DB's root CID.** Measured, not assumed
//! (`zeph_sql::db::tests::two_independent_instances_agree_on_the_root_cid`): two independent instances
//! given identical SQL commit the same root, but the SAME rows written in a DIFFERENT ORDER commit a
//! DIFFERENT root — the root commits to the WRITE HISTORY. A node that restarts and replays epochs
//! writes a different sequence than one that ran continuously, so their roots would diverge while
//! holding identical state. That is a false divergence, precisely what verification must never produce.
//! [`EconomicSnapshot::state_hash`] hashes CONTENT in key order and has no such dependence.
//!
//! So the three mechanisms keep their proper jobs: the REGISTRY holds the DB head (owner-signed
//! durability, its normal role for DB roots), VERIFICATION compares the record's canonical hash, and
//! ATTESTATION finalises the epoch. Nothing new is invented here — the state simply moved out of the
//! record, and the commitment moved in.

use anyhow::Result;
use zeph_reward::EconomicSnapshot;
use zeph_sql::CraftDb;

/// The namespace the economic DB lives under.
pub const ECON_NAMESPACE: &str = "econ";

/// Schema. `INTEGER` is i64 in SQLite, which bounds every amount below `i64::MAX` — far above the
/// token cap (1e14 base units) and any realistic byte watermark, but stated so it is a known bound
/// rather than a latent surprise.
const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS econ_singleton(k TEXT PRIMARY KEY, v INTEGER NOT NULL);\
CREATE TABLE IF NOT EXISTS econ_paid_wm(account BLOB PRIMARY KEY, v INTEGER NOT NULL);\
CREATE TABLE IF NOT EXISTS econ_served_wm(provider BLOB NOT NULL, consumer BLOB NOT NULL, v INTEGER NOT NULL, PRIMARY KEY(provider, consumer));\
CREATE TABLE IF NOT EXISTS econ_seeding(account BLOB PRIMARY KEY, next_epoch INTEGER NOT NULL);\
CREATE TABLE IF NOT EXISTS econ_claims(epoch INTEGER NOT NULL, provider BLOB NOT NULL, PRIMARY KEY(epoch, provider));";



/// Write the whole position. A full rewrite rather than a diff: it is obviously correct, and it is not
/// the cost it appears to be — CraftSQL pages are content-addressed, so pages whose bytes did not
/// change deduplicate and the commit stays O(changed) anyway.
pub async fn persist(db: &mut CraftDb, snap: &EconomicSnapshot) -> Result<()> {
    let mut sql = String::from(SCHEMA);
    sql.push_str("DELETE FROM econ_singleton;DELETE FROM econ_paid_wm;DELETE FROM econ_served_wm;DELETE FROM econ_seeding;DELETE FROM econ_claims;");
    sql.push_str(&format!(
        "INSERT INTO econ_singleton VALUES ('pool',{}),('minted',{});",
        snap.pool as i64, snap.minted as i64
    ));
    for (k, v) in &snap.paid_watermarks {
        sql.push_str(&format!(
            "INSERT INTO econ_paid_wm VALUES (x'{}',{});",
            hex::encode(k), *v as i64
        ));
    }
    for ((p, c), v) in &snap.served_watermarks {
        sql.push_str(&format!(
            "INSERT INTO econ_served_wm VALUES (x'{}',x'{}',{});",
            hex::encode(p), hex::encode(c), *v as i64
        ));
    }
    for (k, v) in &snap.seeding_next {
        sql.push_str(&format!(
            "INSERT INTO econ_seeding VALUES (x'{}',{});",
            hex::encode(k), *v as i64
        ));
    }
    for (e, p) in &snap.claimed {
        sql.push_str(&format!(
            "INSERT INTO econ_claims VALUES ({},x'{}');",
            *e as i64, hex::encode(p)
        ));
    }
    db.write(&sql).await?;
    Ok(())
}

/// Read the whole position back, in KEY ORDER.
///
/// The `ORDER BY` clauses are load-bearing, not tidiness: [`EconomicSnapshot::state_hash`] hashes rows
/// in sequence, so a different iteration order is a different commitment. SQLite compares BLOBs
/// bytewise, which matches Rust's ordering on `[u8; 32]`, so the order here is the same order `compute`
/// sorts into.
pub fn load(db: &CraftDb) -> Result<EconomicSnapshot> {
    let conn = db.conn();
    let mut snap = EconomicSnapshot::default();
    let mut single = conn.prepare("SELECT k,v FROM econ_singleton")?;
    for row in single.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))? {
        let (k, v) = row?;
        match k.as_str() {
            "pool" => snap.pool = v as u64,
            "minted" => snap.minted = v as u64,
            _ => {}
        }
    }
    let mut paid = conn.prepare("SELECT account,v FROM econ_paid_wm ORDER BY account")?;
    for row in paid.query_map([], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?)))? {
        let (a, v) = row?;
        if let Ok(a) = <[u8; 32]>::try_from(a.as_slice()) {
            snap.paid_watermarks.push((a, v as u64));
        }
    }
    let mut served =
        conn.prepare("SELECT provider,consumer,v FROM econ_served_wm ORDER BY provider,consumer")?;
    for row in served.query_map([], |r| {
        Ok((
            r.get::<_, Vec<u8>>(0)?,
            r.get::<_, Vec<u8>>(1)?,
            r.get::<_, i64>(2)?,
        ))
    })? {
        let (p, c, v) = row?;
        if let (Ok(p), Ok(c)) = (
            <[u8; 32]>::try_from(p.as_slice()),
            <[u8; 32]>::try_from(c.as_slice()),
        ) {
            snap.served_watermarks.push(((p, c), v as u64));
        }
    }
    let mut seed = conn.prepare("SELECT account,next_epoch FROM econ_seeding ORDER BY account")?;
    for row in seed.query_map([], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?)))? {
        let (a, v) = row?;
        if let Ok(a) = <[u8; 32]>::try_from(a.as_slice()) {
            snap.seeding_next.push((a, v as u64));
        }
    }
    let mut claims = conn.prepare("SELECT epoch,provider FROM econ_claims ORDER BY epoch,provider")?;
    for row in claims.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)))? {
        let (e, p) = row?;
        if let Ok(p) = <[u8; 32]>::try_from(p.as_slice()) {
            snap.claimed.push((e as u64, p));
        }
    }
    Ok(snap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use zeph_core::{Cid, NodeId};
    use zeph_sql::{CraftSql, Result as SqlResult, RootStore};

    /// Minimal in-memory head store — the registry's role (owner-signed head) is not what is under test.
    struct Heads(Mutex<HashMap<String, (Cid, u64)>>);

    #[async_trait::async_trait]
    impl RootStore for Heads {
        async fn resolve(&self, _o: NodeId, ns: &str) -> SqlResult<Option<(Cid, u64)>> {
            Ok(self.0.lock().unwrap().get(ns).copied())
        }
        async fn publish(
            &self,
            ns: &str,
            root: Cid,
            _prev: Option<Cid>,
            seq: u64,
        ) -> SqlResult<()> {
            self.0.lock().unwrap().insert(ns.to_string(), (root, seq));
            Ok(())
        }
    }

    fn sample() -> EconomicSnapshot {
        // Deliberately built in NON-sorted order: `load` must return it in key order regardless, since
        // the hash is over row sequence.
        EconomicSnapshot {
            pool: 12_345,
            minted: 67_890,
            paid_watermarks: vec![([9u8; 32], 500), ([1u8; 32], 100)],
            served_watermarks: vec![(([2u8; 32], [8u8; 32]), 42), (([2u8; 32], [3u8; 32]), 7)],
            seeding_next: vec![([5u8; 32], 900), ([4u8; 32], 11)],
            claimed: vec![(9, [7u8; 32]), (2, [6u8; 32])],
        }
    }

    /// THE round-trip: state written to CraftSQL and read back must produce the SAME canonical hash —
    /// otherwise the record's commitment could never be checked against the stored state, and the whole
    /// split (state in SQL, commitment in the record) does not hold together.
    #[tokio::test]
    async fn a_round_trip_through_sql_preserves_the_canonical_hash() {
        let dir = tempfile::tempdir().unwrap();
        let heads: Arc<dyn RootStore> = Arc::new(Heads(Mutex::new(HashMap::new())));
        let owner = NodeId([3u8; 32]);
        let sql = CraftSql::register(dir.path(), heads, owner).unwrap();
        let mut db = sql.open(ECON_NAMESPACE).await.unwrap();

        let mut original = sample();
        // Sort as `compute` does, so the hash is taken over canonical order.
        original.paid_watermarks.sort_unstable();
        original.served_watermarks.sort_unstable();
        original.seeding_next.sort_unstable();
        original.claimed.sort_unstable();
        let expected = original.state_hash();

        persist(&mut db, &original).await.unwrap();
        let loaded = load(&db).unwrap();

        assert_eq!(loaded.pool, original.pool, "pool survives");
        assert_eq!(loaded.minted, original.minted, "supply survives");
        assert_eq!(
            loaded.state_hash(),
            expected,
            "the canonical hash must survive the round trip — the ORDER BY clauses in `load` are what \
             make this true, and without it the record's commitment could never be checked"
        );

        // And persisting from UNSORTED input still yields the same stored order, because `load` orders by
        // key rather than trusting insertion order.
        let mut db2 = sql.open("econ2").await.unwrap();
        persist(&mut db2, &sample()).await.unwrap();
        assert_eq!(
            load(&db2).unwrap().state_hash(),
            expected,
            "row order comes from the keys, not from how they were written"
        );
    }
}
