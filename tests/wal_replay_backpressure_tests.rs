//! Tests that WAL replay completes when the replayed data exceeds
//! `slatedb::config::Settings::max_unflushed_bytes` — i.e. the configuration
//! that production uses to keep `shard.close()` drain time bounded.
//!
//! Reproduces the post-OOMKill scenario: pod is killed with unflushed WAL data,
//! restart re-opens the shard and must replay that WAL. Replay's backpressure
//! loop in `slatedb::Db::maybe_apply_backpressure` waits on the memtable_flusher
//! whenever in-memory imm memtables exceed `max_unflushed_bytes`. The test
//! verifies that replay still terminates (not deadlocks) when the replayed
//! data is many multiples of `max_unflushed_bytes`.

mod test_helpers;

use silo::gubernator::MockGubernatorClient;
use silo::job_store_shard::JobStoreShard;
use silo::settings::{Backend, DatabaseConfig, WalConfig};
use silo::shard_range::ShardRange;
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};
use std::time::Duration;
use test_helpers::*;

/// Read the current value of slatedb's `slatedb.db.backpressure_count`
/// counter from the recorder. Returns 0 if the metric isn't registered yet.
fn backpressure_count(recorder: &DefaultMetricsRecorder) -> u64 {
    let snap = recorder.snapshot();
    snap.by_name("slatedb.db.backpressure_count")
        .first()
        .map(|m| match m.value {
            MetricValue::Counter(v) => v,
            _ => 0,
        })
        .unwrap_or(0)
}

/// Build slatedb settings tuned to force WAL replay to cross the
/// `max_unflushed_bytes` threshold many times during replay.
fn replay_stress_slatedb_settings() -> slatedb::config::Settings {
    slatedb::config::Settings {
        // Fast WAL flush so writes durably land in WAL SSTs before we drop.
        flush_interval: Some(Duration::from_millis(10)),
        // Pathologically small unflushed cap. `WalReplayIterator`
        // accumulates ~`l0_sst_size_bytes` per memtable, so a single
        // replayed memtable (256 KiB) is already 1/2 the cap and the
        // first frozen imm immediately trips backpressure on the next
        // iteration. This forces the replay loop through repeated
        // `maybe_apply_backpressure` waits.
        max_unflushed_bytes: 512 * 1024,
        // 256 KiB target memtable size so we generate many replayed
        // memtables (rather than one big one that finishes in a single
        // iteration without exercising backpressure between iterations).
        l0_sst_size_bytes: 256 * 1024,
        // Default l0_flush_parallelism is 4; with max_unflushed_bytes
        // = 512 KiB and l0_sst_size_bytes = 256 KiB the cap allows ~2
        // in-flight imms, so the flusher must continually drain to
        // keep replay progressing.
        ..Default::default()
    }
}

#[silo::test]
async fn replay_completes_when_replayed_data_exceeds_max_unflushed_bytes() {
    // Generous outer timeout: if replay deadlocks on backpressure this
    // test should fail by timing out, not run forever. 60s is plenty for
    // a few MiB of fs-backed replay; prod symptom would manifest as a
    // hang well past this.
    with_timeout!(60_000, {
        let data_dir = tempfile::tempdir().unwrap();
        let wal_dir = tempfile::tempdir().unwrap();

        let cfg = DatabaseConfig {
            name: "replay-stress".to_string(),
            backend: Backend::Fs,
            path: data_dir.path().to_string_lossy().to_string(),
            wal: Some(WalConfig {
                backend: Backend::Fs,
                path: wal_dir.path().to_string_lossy().to_string(),
            }),
            // We never call close() on the first shard (simulating crash),
            // so this flag is irrelevant for the first shard.
            apply_wal_on_close: false,
            slatedb: Some(replay_stress_slatedb_settings()),
            memory_cache: None,
        };

        let rate_limiter = MockGubernatorClient::new_arc();
        let shard = JobStoreShard::open(&cfg, rate_limiter, None, ShardRange::full())
            .await
            .expect("open shard 1");

        // Write enough data that on-disk WAL data is many multiples of
        // max_unflushed_bytes (512 KiB). 2000 jobs * ~8 KiB payload + key/index
        // overhead per job is well over 16 MiB. With l0_sst_size_bytes = 256
        // KiB this forces replay to produce many memtables and triggers
        // `slatedb::db: ... applying backpressure` warnings repeatedly during
        // replay (verified empirically — the WARN appears 8+ times for this
        // volume). If you tune the volume down, backpressure may not fire and
        // the test no longer exercises the path it claims to.
        let payload_body = "x".repeat(8192);
        let mut expected_job_ids = Vec::with_capacity(2000);
        for i in 0..2000 {
            let payload = msgpack_payload(&serde_json::json!({
                "i": i,
                "data": payload_body,
            }));
            let id = shard
                .enqueue(
                    "-",
                    None,
                    10u8,
                    now_ms(),
                    None,
                    payload,
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue");
            expected_job_ids.push(id);
        }

        let counters_before = shard.get_counters().await.expect("get_counters");
        assert_eq!(counters_before.total_jobs, 2000);

        // Give the WAL a moment to persist any in-flight buffer flushes.
        // (enqueue() awaits durability per WriteOptions::default(), but
        // background L0 flushing is async — we explicitly do NOT want to
        // wait long enough for everything to migrate to L0, since the
        // point of the test is to leave WAL work for replay.)
        tokio::time::sleep(Duration::from_millis(50)).await;

        // "Crash": drop the shard *without* calling close(). This skips
        // the explicit memtable->SST flush in JobStoreShard::close(),
        // leaving entries in the local WAL that the next open() must replay.
        drop(shard);

        // Yield so any background tasks tied to the dropped shard can
        // unwind cleanly before we open a fresh Db on the same paths.
        // (slatedb's open path fences zombie writers, so this is belt &
        // suspenders, not strictly necessary.)
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Reopen with the same paths and the same low max_unflushed_bytes.
        // Wrap in an inner timeout so a deadlock surfaces as a clear
        // assertion message rather than the outer with_timeout! panic.
        let rate_limiter2 = MockGubernatorClient::new_arc();
        let reopened = tokio::time::timeout(
            Duration::from_secs(45),
            JobStoreShard::open(&cfg, rate_limiter2, None, ShardRange::full()),
        )
        .await
        .expect(
            "WAL replay deadlocked: open() did not return within 45s with \
             max_unflushed_bytes=512KiB and ~16MiB+ of WAL data to replay",
        )
        .expect("reopen shard");

        // Replay must reconstruct every job. If backpressure caused replay
        // to silently truncate or skip entries, this would be < 2000.
        let counters_after = reopened.get_counters().await.expect("get_counters");
        assert_eq!(
            counters_after.total_jobs, 2000,
            "all enqueued jobs should be recovered after WAL replay"
        );
        assert_eq!(counters_after.completed_jobs, 0);

        // Spot-check a handful of individual jobs by ID to confirm the
        // payload-bearing rows (not just counters) survived replay.
        for id in expected_job_ids.iter().take(5).chain(expected_job_ids.iter().rev().take(5)) {
            let status = reopened
                .get_job_status("-", id)
                .await
                .expect("get_job_status")
                .unwrap_or_else(|| panic!("job {id} missing after replay"));
            assert_eq!(status.kind, silo::job::JobStatusKind::Scheduled);
        }

        // Cleanly close so the test doesn't leak background tasks.
        reopened.close().await.expect("close reopened shard");
    });
}

/// Demonstrates the value of the post-replay flush in
/// `JobStoreShard::open_with_resolved_store`. After a crash with unflushed
/// WAL, replay reconstructs imm memtables from WAL SSTs. Without the
/// post-replay flush, the residual imm memtables can still be near
/// `max_unflushed_bytes` when `open()` returns, and the very first
/// post-open writes immediately trip `maybe_apply_backpressure`. The
/// post-open flush eliminates that residue, so the first writes after
/// open run on a clean memtable.
#[silo::test]
async fn post_open_writes_dont_hit_backpressure_after_crash_replay() {
    with_timeout!(60_000, {
        let data_dir = tempfile::tempdir().unwrap();
        let wal_dir = tempfile::tempdir().unwrap();

        let cfg = DatabaseConfig {
            name: "post-open-writes".to_string(),
            backend: Backend::Fs,
            path: data_dir.path().to_string_lossy().to_string(),
            wal: Some(WalConfig {
                backend: Backend::Fs,
                path: wal_dir.path().to_string_lossy().to_string(),
            }),
            apply_wal_on_close: false,
            slatedb: Some(replay_stress_slatedb_settings()),
            memory_cache: None,
        };

        // Phase 1: open + write enough to leave WAL data for replay + crash.
        let rate_limiter = MockGubernatorClient::new_arc();
        let shard = JobStoreShard::open(&cfg, rate_limiter, None, ShardRange::full())
            .await
            .expect("open shard 1");

        let payload_body = "x".repeat(8192);
        for i in 0..2000 {
            let payload = msgpack_payload(&serde_json::json!({
                "i": i,
                "data": payload_body,
            }));
            shard
                .enqueue(
                    "-",
                    None,
                    10u8,
                    now_ms(),
                    None,
                    payload,
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue");
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(shard);
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Phase 2: reopen. With the post-replay flush in
        // JobStoreShard::open_with_resolved_store, by the time this returns
        // the replayed memtable has been drained to L0 SSTs.
        let rate_limiter2 = MockGubernatorClient::new_arc();
        let reopened = JobStoreShard::open(&cfg, rate_limiter2, None, ShardRange::full())
            .await
            .expect("reopen shard");

        // Snapshot the backpressure counter immediately after open returns.
        // Replay-time backpressure events are included in this baseline —
        // the post-open writes below must NOT add to it.
        let recorder = reopened.slatedb_metrics_recorder().clone();
        let bp_after_open = backpressure_count(&recorder);

        // Phase 3: issue a tight burst of post-open writes. With the
        // post-replay flush in place, total in-memory unflushed bytes
        // start near zero and the burst fits comfortably under
        // max_unflushed_bytes (512 KiB) for any individual moment after
        // each successful flush_interval. Without the post-replay flush,
        // these writes would be racing against the flusher still draining
        // ~max_unflushed_bytes worth of replay residue and would
        // immediately trip backpressure.
        //
        // We deliberately keep the burst SMALL (50 jobs * 1 KiB ~= 50
        // KiB total memtable bytes) so that the burst itself cannot
        // exceed max_unflushed_bytes on its own — meaning any
        // backpressure observed must be from leftover replay state.
        let small_payload = "y".repeat(1024);
        for i in 0..50 {
            let payload = msgpack_payload(&serde_json::json!({
                "post_open": i,
                "data": small_payload,
            }));
            reopened
                .enqueue(
                    "-",
                    None,
                    10u8,
                    now_ms(),
                    None,
                    payload,
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("post-open enqueue");
        }

        let bp_after_writes = backpressure_count(&recorder);
        let bp_during_writes = bp_after_writes - bp_after_open;

        assert_eq!(
            bp_during_writes, 0,
            "post-open writes should not trigger backpressure when the \
             replayed memtable has been drained at open time \
             (saw {bp_during_writes} backpressure events; \
             before-writes baseline was {bp_after_open}, \
             after-writes was {bp_after_writes})",
        );

        // Sanity: the writes actually landed.
        let counters = reopened.get_counters().await.expect("get_counters");
        assert_eq!(counters.total_jobs, 2000 + 50);

        reopened.close().await.expect("close reopened shard");
    });
}
