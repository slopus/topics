//! Phase-8B fault catalog — boundary: **reclaim** (file B).
//!
//! Two strategies, one test fn each, driven through the Phase-8A harness
//! ([`FakeDisk`] pending/durable + `crash()`, `Engine::with_data_dir_fs`, the
//! crash-point discipline, and the model-oracle contract from
//! `tests/crash_oracle.rs`):
//!
//! - **F-EVICT-FLOOR-CRASH** (crash-point, critical): crash after an
//!   EvictWatermark (cap/TTL) fsync but before segment reclaim. Oracle: the
//!   involuntary `evict_floor` recovers and never regresses; an involuntary cap
//!   tombstone still fires for a below-floor cursor after restart.
//!
//! - **F-CLOCK-FORWARD-TTL** (crash-point, medium): the wall clock jumps far
//!   forward across a restart (TTL/evict floors + the idempotency window read
//!   ms). Oracle: a forward jump may expire *more* (allowed) but must not
//!   resurrect deleted data, regress floors, or double-apply; the
//!   tombstone/evict floors are preserved and the idempotency window only
//!   shrinks.
//!
//! Both are bounded crash-point sweeps with small fixed workloads, capped points
//! and fixed seeds, so the file runs in well under a minute.
//!
//! ```text
//! cargo test --features test-fs --test fault_reclaim_b
//! ```

#![cfg(feature = "test-fs")]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use streams::storage::{File, Fs, OpenOpts};
use streams::types::{
    BoxConfig, BoxType, DeleteRequest, DiffRequest, RecordIn, WriteRequest,
};

// ===========================================================================
// Plumbing (mirrors tests/crash_oracle.rs — reused, not reinvented)
// ===========================================================================

const DATA_DIR: &str = "/data";
const T0: i64 = 1_700_000_000_000;

fn cfg() -> ServerConfig {
    ServerConfig {
        data_dir: Some(DATA_DIR.to_string()),
        ..Default::default()
    }
}

/// A fresh test clock at the canonical epoch start.
fn clock_at(now_ms: i64) -> SharedClock {
    Arc::new(TestClock::new(now_ms))
}

/// Build a durable engine whose WAL + snapshots live on `disk`, reading time
/// through `clk` (so a recovery can resume on a *jumped* clock — F-CLOCK-FORWARD).
fn open_engine_clk(disk: &FakeDisk, clk: SharedClock) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clk, disk.arc()).expect("engine opens through FakeDisk")
}

/// Build an engine on a clock pinned at [`T0`].
fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    open_engine_clk(disk, clock_at(T0))
}

/// Make every WAL file's NAME durable (the create+dir-fsync production does at
/// WAL open — modelled explicitly so the file survives a crash).
fn sync_wal_dir(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

/// One record as read back through the diff path — the fields the durability
/// contract must preserve byte-for-byte.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Rec {
    data: String,
    tag: Option<String>,
    node: Option<String>,
}

/// A flat dump of a recovered box: head / earliest / count, the live records by
/// seq, and any tombstone reason a from-0 read surfaces. `None` if absent.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BoxDump {
    head: u64,
    earliest: u64,
    count: u64,
    records: BTreeMap<u64, Rec>,
    tombstone_reason: Option<String>,
}

/// Read the full recovered state of `name` through the public API (state + a
/// paginated diff over every record from seq 0). The `box_state` + `diff` reads
/// both run `enforce_retention` at the engine's *current* clock, so this is also
/// what re-derives a cap/TTL floor lazily after a WAL-only recovery.
fn dump_box(engine: &Engine, name: &str) -> Option<BoxDump> {
    let st = engine.box_state(name, false).ok()?;
    let mut records = BTreeMap::new();
    let mut tombstone_reason = None;
    let mut from = 0u64;
    loop {
        let d = engine
            .diff(
                name,
                DiffRequest {
                    from_seq: from,
                    limit: 1000,
                    node: None,
                    include_tags: true,
                    include_meta: true,
                    wait_ms: 0,
                },
            )
            .ok()?;
        if let Some(tomb) = &d.tombstone {
            tombstone_reason = Some(format!("{:?}", tomb.reason).to_lowercase());
        }
        for r in &d.records {
            let data = r
                .data
                .get("v")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| r.data.to_string());
            records.insert(
                r.seq,
                Rec {
                    data,
                    tag: r.tag.clone(),
                    node: r.node.clone(),
                },
            );
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    Some(BoxDump {
        head: st.head_seq,
        earliest: st.earliest_seq,
        count: st.count,
        records,
        tombstone_reason,
    })
}

/// Append one record, returning its assigned seq. `durable=true` blocks on the
/// group fsync (acked ⇒ must-survive).
fn append(engine: &Engine, name: &str, data: &str, tag: Option<&str>) -> Option<u64> {
    let req = WriteRequest {
        records: vec![RecordIn {
            data: json!({ "v": data }),
            tag: tag.map(|s| s.to_string()),
            node: None,
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: Some(true),
        config: None,
        disable_backpressure: true,
    };
    engine.write(name, req, true).ok().map(|r| r.last_seq)
}

/// Create (or replace) a box with an explicit durable + cap_records + ttl_ms.
fn put_box(engine: &Engine, name: &str, durable: bool, cap: u64, ttl_ms: u64) -> bool {
    let cfg = BoxConfig {
        r#type: BoxType::Log,
        durable,
        cap_records: cap,
        ttl_ms,
        ..Default::default()
    };
    engine.put_box(name, cfg).is_ok()
}

// ===========================================================================
// CrashAfter: fire FakeDisk.crash() after the Nth call of a chosen op class.
// Copied from tests/crash_oracle.rs — the harness-level "power loss after the
// Nth FS mutating call" injector.
// ===========================================================================

#[derive(Clone)]
struct CrashAfter {
    disk: FakeDisk,
    op: FaultOp,
    at: u64,
    seen: Arc<AtomicU64>,
    tripped: Arc<std::sync::atomic::AtomicBool>,
}

impl CrashAfter {
    fn new(disk: FakeDisk, op: FaultOp, at: u64) -> Self {
        CrashAfter {
            disk,
            op,
            at,
            seen: Arc::new(AtomicU64::new(0)),
            tripped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }

    fn tick(&self, op: FaultOp) {
        if op != self.op {
            return;
        }
        let idx = self.seen.fetch_add(1, AtomicOrdering::SeqCst);
        if idx == self.at
            && self
                .tripped
                .compare_exchange(false, true, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
                .is_ok()
        {
            self.disk.crash(TornDamage::None);
        }
    }
}

struct CrashAfterFile {
    inner: Box<dyn File>,
    owner: CrashAfter,
}

impl File for CrashAfterFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        // Trip AFTER the write reaches the (pending) image, so a crash at index N
        // keeps the first N writes' pending bytes available for fsync but drops
        // anything not yet fsynced — exactly the power-loss-after-Nth-write model.
        let r = self.inner.write_at(offset, buf);
        self.owner.tick(FaultOp::WriteAt);
        r
    }
    fn set_len(&mut self, len: u64) -> std::io::Result<()> {
        let r = self.inner.set_len(len);
        self.owner.tick(FaultOp::SetLen);
        r
    }
    fn sync_data(&self) -> std::io::Result<()> {
        let r = self.inner.sync_data();
        self.owner.tick(FaultOp::SyncData);
        r
    }
    fn sync_all(&self) -> std::io::Result<()> {
        let r = self.inner.sync_all();
        self.owner.tick(FaultOp::SyncAll);
        r
    }
    fn metadata_len(&self) -> std::io::Result<u64> {
        self.inner.metadata_len()
    }
    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> std::io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for CrashAfter {
    fn open(&self, path: &std::path::Path, opts: OpenOpts) -> std::io::Result<Box<dyn File>> {
        let inner = self.disk.open(path, opts)?;
        Ok(Box::new(CrashAfterFile {
            inner,
            owner: self.clone(),
        }))
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        let r = self.disk.rename(from, to);
        self.tick(FaultOp::Rename);
        r
    }
    fn remove_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.disk.remove_file(path)
    }
    fn read_dir(&self, dir: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
        self.disk.read_dir(dir)
    }
    fn sync_dir(&self, dir: &std::path::Path) -> std::io::Result<()> {
        let r = self.disk.sync_dir(dir);
        self.tick(FaultOp::SyncDir);
        r
    }
    fn create_dir_all(&self, dir: &std::path::Path) -> std::io::Result<()> {
        self.disk.create_dir_all(dir)
    }
    fn exists(&self, path: &std::path::Path) -> bool {
        self.disk.exists(path)
    }
    fn metadata_len(&self, path: &std::path::Path) -> std::io::Result<u64> {
        self.disk.metadata_len(path)
    }
}

// ===========================================================================
// F-EVICT-FLOOR-CRASH  (crash-point, reclaim, critical)
//
//   fault:  crash after an EvictWatermark (cap/TTL) fsync but before segment
//           reclaim.
//   inject: harness crash-point sweep — FakeDisk.crash() after each FS mutating
//           (write_at) call of a durable cap-box workload (the eviction is
//           driven by appending past `cap_records`, which advances the
//           involuntary evict_floor; the segment reclaim that would follow is
//           what the crash interrupts).
//   oracle: evict_floor recovers and never regresses; an involuntary cap
//           tombstone STILL fires for a below-floor cursor after restart
//           (involuntary loss is never silent — matches the capped-box
//           assertion in crash_oracle::cap_evict_floor_survives_crash_with_*).
//
// NOTE on the engine model: the engine does not write `EvictWatermark` WAL
// frames on the hot path — the involuntary cap floor is *durably re-derivable*
// from the (durable) `cap_records` BoxConfig: a WAL-only recovery rebuilds the
// records, and the first `enforce_retention` (run by any state/diff read)
// recomputes `evict_floor = head - cap_records` and re-materializes the
// tombstone. So "crash after the watermark, before reclaim" is exercised here as
// "crash at every FS point of the over-cap workload, then recover and confirm
// the floor + tombstone come back" — the floor is never lost and never regresses.
// ===========================================================================

/// The over-cap durable workload: a cap=3 box with 6 appends (so the newest 3
/// survive and seqs 1..=3 fall below the involuntary evict floor → tombstone).
fn evict_workload(engine: &Engine) {
    put_box(engine, "cap", true, 3, 0);
    for i in 1..=6u64 {
        append(engine, "cap", &i.to_string(), None);
    }
}

/// Assert the cap recovery contract on a recovered `cap` box: only the newest 3
/// survive, head is 6, earliest is 4, and a from-0 read tombstones with reason
/// `cap` (the involuntary floor is explicit, never silent), and the floor never
/// regressed below 3.
fn assert_cap_recovered(dump: &BoxDump) {
    assert_eq!(dump.head, 6, "head recovered to the model");
    assert_eq!(
        dump.records.keys().copied().collect::<Vec<_>>(),
        vec![4, 5, 6],
        "only the newest cap=3 survive (evict_floor==3 re-derived, never regressed)"
    );
    assert_eq!(dump.count, 3, "retained count matches the cap");
    assert!(
        dump.earliest >= 4,
        "earliest_seq never regresses below the evict floor (got {})",
        dump.earliest
    );
    assert_eq!(
        dump.tombstone_reason.as_deref(),
        Some("cap"),
        "an involuntary cap tombstone STILL fires for a below-floor cursor after restart"
    );
    // Records are genuine (no fabrication / torn frame misread as a record).
    assert_eq!(dump.records[&4].data, "4");
    assert_eq!(dump.records[&5].data, "5");
    assert_eq!(dump.records[&6].data, "6");
}

#[test]
fn f_evict_floor_crash() {
    // 1) Probe M: how many write_at calls the over-cap workload issues (run with
    //    at = u64::MAX ⇒ never fires; just count) so the sweep covers every FS
    //    boundary of the small workload.
    let probe_disk = FakeDisk::new();
    let probe = FaultFs::new(probe_disk.arc(), FaultOp::WriteAt, FaultKind::Eio, u64::MAX, true);
    {
        let engine = Engine::with_data_dir_fs(cfg(), clock_at(T0), probe.arc()).expect("probe");
        evict_workload(&engine);
        drop(engine);
    }
    let total_writes = probe.calls_seen();
    assert!(total_writes >= 4, "workload issues several write_at calls (M={total_writes})");

    // Cap the sweep so it stays fast (each durable append blocks on a real group
    // fsync). Crash AFTER every write_at index in 0..=cap, recover, assert the
    // floor + tombstone contract at the crash points where the whole over-cap
    // workload became durable.
    let cap_points = total_writes.min(16);
    let mut fully_recovered = 0usize;
    for crash_point in 0..=cap_points {
        let disk = FakeDisk::with_seed(0xE71C_F100 ^ crash_point);
        let trip = CrashAfter::new(disk.clone(), FaultOp::WriteAt, crash_point);
        {
            // Some appends past the crash point land on the (now-frozen) device and
            // are dropped; only fsync-returned appends are acked-durable. We do NOT
            // mirror a separate model — instead we assert the strong contract only
            // at the crash points where ALL six appends were durable (head==6),
            // which is exactly the "crash after the watermark" case the strategy
            // targets; at earlier crash points we assert the always-true invariant.
            let engine = Engine::with_data_dir_fs(cfg(), clock_at(T0), trip.arc())
                .expect("sweep engine opens");
            evict_workload(&engine);
            drop(engine);
        }
        disk.reset_power();

        // Recover through the crashed image (fresh clock at T0 — no clock jump
        // here; that is F-CLOCK-FORWARD-TTL). The first read re-derives the floor.
        let engine = open_engine(&disk);
        if let Some(dump) = dump_box(&engine, "cap") {
            // ALWAYS-TRUE invariant at every crash point: survivors are a dense
            // window with NO record below the evict floor surfacing silently, and
            // no fabricated record. Whenever a cap floor advanced (more than `cap`
            // records are durable, i.e. head > 3) a from-0 read MUST tombstone.
            let survivors: Vec<u64> = dump.records.keys().copied().collect();
            assert!(
                dump.count <= 3 || survivors.is_empty(),
                "cap=3 box never retains more than 3 records (crash_point {crash_point}, got {})",
                dump.count
            );
            if dump.head > 3 && !survivors.is_empty() {
                assert_eq!(
                    dump.tombstone_reason.as_deref(),
                    Some("cap"),
                    "cap floor advanced (head {}) ⇒ involuntary tombstone after recovery \
                     (crash_point {crash_point})",
                    dump.head
                );
            }
            // STRONG contract at the targeted crash point: the whole over-cap
            // workload was durable (head==6) ⇒ the exact cap window + floor +
            // tombstone all recovered.
            if dump.head == 6 {
                assert_cap_recovered(&dump);
                fully_recovered += 1;

                // The floor never regresses across a SECOND recovery (idempotent).
                drop(engine);
                let engine2 = open_engine(&disk);
                let dump2 = dump_box(&engine2, "cap").expect("cap present on recovery #2");
                assert_eq!(dump2, dump, "recovery idempotent at crash_point {crash_point}");
                assert_cap_recovered(&dump2);
            }
        }
    }

    assert!(
        fully_recovered >= 1,
        "at least one crash point had the whole over-cap workload durable so the \
         evict_floor + tombstone recovery was exercised (got {fully_recovered})"
    );
}

// ===========================================================================
// F-CLOCK-FORWARD-TTL  (crash-point, reclaim, medium)
//
//   fault:  the wall clock jumps FAR FORWARD across a restart; TTL/evict floors
//           and the idempotency window read ms.
//   inject: TestClock jumped far forward on the second boot (recovery clock).
//   oracle: a forward jump may expire MORE (allowed) but must NOT resurrect
//           deleted data, regress floors, or double-apply; tombstone/evict
//           floors preserved; the idempotency window only shrinks.
//
// Workload: a durable TTL box. Records 1..=3 written at T0; a VOLUNTARY delete
// of the seq-1 prefix (silent, no tombstone); records 4..=5 written at T0. We
// then crash with everything acked-durable, and reopen on a clock jumped far
// past the TTL so the first post-recovery read TTL-expires the survivors. The
// contract: the deleted prefix stays gone (no resurrection), the delete stays
// SILENT where it dominates, the involuntary TTL floor only advances (monotone),
// and head/seq never regress.
// ===========================================================================

#[test]
fn f_clock_forward_ttl() {
    // TTL chosen large enough that nothing expires at T0 but everything expires
    // after the forward jump.
    const TTL_MS: u64 = 60_000;
    const JUMP_MS: i64 = 10 * 24 * 60 * 60 * 1000; // +10 days, far past any TTL.

    // 1) Build the durable TTL workload on a clock pinned at T0, crash with the
    //    whole tail durable, so every acked write is on the platter.
    let disk = FakeDisk::new();
    {
        let engine = open_engine_clk(&disk, clock_at(T0));
        assert!(put_box(&engine, "ttl", true, 0, TTL_MS));
        assert_eq!(append(&engine, "ttl", "r1", Some("a")), Some(1));
        assert_eq!(append(&engine, "ttl", "r2", Some("a")), Some(2));
        assert_eq!(append(&engine, "ttl", "r3", Some("b")), Some(3));
        // Voluntary delete of the seq<2 prefix: silent (advances delete_floor, NOT
        // evict_floor) — must stay silent and never resurrect after the jump.
        assert!(engine
            .delete(
                "ttl",
                DeleteRequest { before_seq: Some(2), match_: None },
            )
            .is_ok());
        assert_eq!(append(&engine, "ttl", "r4", None), Some(4));
        assert_eq!(append(&engine, "ttl", "r5", None), Some(5));
        sync_wal_dir(&disk);
        // Power loss with the whole durable tail acked.
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // --- Baseline recovery on the SAME clock (no jump): nothing TTL-expires; the
    //     deleted prefix (seq 1) stays gone and SILENT; 2..=5 are live. ---------
    {
        let engine = open_engine_clk(&disk, clock_at(T0));
        let base = dump_box(&engine, "ttl").expect("ttl box recovers");
        assert_eq!(base.head, 5, "head recovered, never regresses");
        assert_eq!(
            base.records.keys().copied().collect::<Vec<_>>(),
            vec![2, 3, 4, 5],
            "voluntary-deleted seq 1 stays gone; 2..=5 live (no TTL expiry at T0)"
        );
        assert_eq!(
            base.tombstone_reason, None,
            "the voluntary prefix delete stays SILENT (no tombstone) — delete is not eviction"
        );
        assert_eq!(base.earliest, 2, "earliest at the delete floor + 1");
        drop(engine);
    }

    // --- Forward-jumped recovery: reopen on a clock 10 days past T0. The first
    //     read runs enforce_retention at the jumped now, TTL-expiring every
    //     surviving record. A forward jump may expire MORE (allowed). -----------
    let jumped: SharedClock = {
        let c = TestClock::new(T0);
        c.set(T0 + JUMP_MS);
        Arc::new(c)
    };
    // sanity: the jumped clock really is past every record's TTL.
    assert!(jumped.now_ms() - T0 > TTL_MS as i64, "jump exceeds TTL");

    let engine = open_engine_clk(&disk, jumped.clone());
    let after = dump_box(&engine, "ttl").expect("ttl box recovers under a jumped clock");

    // (a) NO RESURRECTION: the voluntary-deleted seq 1 (and anything below the
    //     delete floor) never comes back, regardless of the clock jump.
    assert!(
        !after.records.contains_key(&1),
        "voluntary-deleted seq 1 must NOT resurrect after a forward clock jump: {:?}",
        after.records.keys().collect::<Vec<_>>()
    );

    // (b) HEAD/SEQ MONOTONE: head never regresses (it is a logical counter, not a
    //     wall-clock value), and never fabricates a future seq.
    assert_eq!(after.head, 5, "head is logical; a forward jump never regresses or inflates it");

    // (c) FLOORS ONLY ADVANCE: every surviving record is below the (now far
    //     larger) TTL horizon ⇒ all 2..=5 expire. The earliest_seq advances to
    //     head+1 (all expired) and never regresses below the baseline earliest.
    assert!(
        after.earliest >= 2,
        "earliest_seq never regresses below the pre-jump floor (got {})",
        after.earliest
    );
    assert!(
        after.records.is_empty(),
        "a far-forward jump may expire MORE — all retained records TTL-expire (got {:?})",
        after.records.keys().collect::<Vec<_>>()
    );

    // (d) INVOLUNTARY LOSS STAYS EXPLICIT: the records lost to TTL are an
    //     involuntary eviction, so a from-0 read that crosses the expiry floor
    //     surfaces a tombstone with a ttl/cap/mixed reason — never silent, never
    //     a fabricated record. (Records 2..=5 expired by TTL; the seq-1 prefix was
    //     a voluntary delete, so the reason for the involuntary part is ttl.)
    assert!(
        after.tombstone_reason.is_some(),
        "TTL expiry is involuntary ⇒ a below-floor read tombstones (never silent)"
    );
    let reason = after.tombstone_reason.as_deref().unwrap();
    assert!(
        reason == "ttl" || reason == "mixed" || reason == "cap",
        "involuntary tombstone reason is ttl/mixed/cap, got {reason:?}"
    );

    // (e) NO DOUBLE-APPLY / IDEMPOTENT under the jump: a SECOND recovery on the
    //     same jumped clock converges byte-identically (the floors are already at
    //     the horizon; re-running expires nothing new).
    drop(engine);
    let engine2 = open_engine_clk(&disk, jumped.clone());
    let after2 = dump_box(&engine2, "ttl").expect("ttl box recovers on jumped recovery #2");
    assert_eq!(
        after2, after,
        "recovery under a forward clock jump is idempotent (no double-apply)"
    );

    // (f) A post-recovery append continues at exactly head+1 (the forward jump
    //     never punched a gap nor reused a seq) and is itself immediately TTL-aged
    //     out (it was written 10 days ago in clock terms? no — it is written at
    //     `now`, so it is fresh): the new record is live, proving the floor only
    //     advanced for the OLD records, never globally poisoned the box.
    let new_seq = append(&engine2, "ttl", "r6", None).expect("post-recovery append succeeds");
    assert_eq!(new_seq, 6, "a post-recovery append continues at head+1 — no gap, no seq reuse");
    let final_dump = dump_box(&engine2, "ttl").expect("ttl box after the fresh append");
    assert!(
        final_dump.records.contains_key(&6),
        "the fresh post-jump append (written at `now`) is live — TTL aged only the OLD records"
    );
    assert_eq!(final_dump.records[&6].data, "r6");
    assert_eq!(final_dump.head, 6, "head advanced to the fresh append");
}
