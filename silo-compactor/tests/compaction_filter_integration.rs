//! End-to-end test that exercises the standalone compactor with
//! [`silo_compactor::compaction_filter::CompletedJobCompactionFilter`]
//! attached. The test writes a mix of old terminal, recent terminal, and
//! non-terminal job rows to an in-memory SlateDB; submits a full compaction
//! against a separately-run [`slatedb::Compactor`] with the filter
//! supplier; polls the manifest until all L0 SSTs have been consumed; then
//! opens a fresh [`slatedb::DbReader`] and verifies that only the
//! expired-completed rows were purged.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use silo::codec::encode_job_status;
use silo::job::{JobStatus, JobStatusKind};
use silo::job_store_shard::counter_merge_operator;
use silo::keys::{
    attempt_key, idx_metadata_key, idx_status_time_key, job_cancelled_key, job_info_key,
    job_status_key,
};
use silo_compactor::compaction_filter::CompletedJobCompactionFilterSupplier;
use slatedb::admin::AdminBuilder;
use slatedb::compactor::{CompactionSpec, SourceId};
use slatedb::config::{CompactorOptions, FlushOptions, FlushType};
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::path::Path;
use slatedb::{CompactorBuilder, Db, DbReaderBuilder};
use ulid::Ulid;

fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Compactor tuned for integration tests: short poll interval so submitted
/// compactions start within tens of milliseconds rather than the default
/// 5-second cadence, and an artificially-high `min_compaction_sources` so
/// the size-tiered scheduler never auto-triggers while we are submitting
/// manual specs via Admin.
///
/// Without the short poll the filter's `cutoff_ms = now - retention` drifts
/// past our "recent" rows while the compactor sleeps; without the disabled
/// auto-scheduler it produces compactions with conflicting destinations
/// that cause our manual submits to fail validation.
fn test_compactor_options() -> CompactorOptions {
    let mut scheduler_options = std::collections::HashMap::new();
    scheduler_options.insert("min_compaction_sources".to_string(), "1000".to_string());
    scheduler_options.insert("max_compaction_sources".to_string(), "1000".to_string());
    CompactorOptions {
        poll_interval: Duration::from_millis(50),
        scheduler_options,
        ..CompactorOptions::default()
    }
}

/// Write the six-prefix "family" of rows for a given job: JOB_INFO,
/// JOB_STATUS, IDX_STATUS_TIME, IDX_METADATA, ATTEMPT, JOB_CANCELLED.
/// Mirrors the key set the filter inspects.
async fn write_job_family(db: &Db, tenant: &str, job_id: &str, kind: JobStatusKind, ts_ms: i64) {
    let status = JobStatus::new(kind, ts_ms, None, None);
    db.put(job_status_key(tenant, job_id), encode_job_status(&status))
        .await
        .unwrap();
    db.put(job_info_key(tenant, job_id), b"info-payload")
        .await
        .unwrap();
    db.put(
        idx_status_time_key(tenant, kind.as_str(), ts_ms, job_id),
        b"",
    )
    .await
    .unwrap();
    db.put(
        idx_metadata_key(tenant, "env", "prod", job_id),
        b"metadata-value",
    )
    .await
    .unwrap();
    db.put(attempt_key(tenant, job_id, 1), b"attempt-payload")
        .await
        .unwrap();
    db.put(job_cancelled_key(tenant, job_id), b"cancelled-payload")
        .await
        .unwrap();
}

/// Returns the list of family-key labels that are currently *missing* for
/// a job. Used to build actionable assertion messages.
async fn missing_family_keys(
    reader: &slatedb::DbReader,
    tenant: &str,
    job_id: &str,
) -> Vec<&'static str> {
    let mut missing = Vec::new();
    if reader
        .get(job_status_key(tenant, job_id).as_slice())
        .await
        .unwrap()
        .is_none()
    {
        missing.push("JOB_STATUS");
    }
    if reader
        .get(job_info_key(tenant, job_id).as_slice())
        .await
        .unwrap()
        .is_none()
    {
        missing.push("JOB_INFO");
    }
    if reader
        .get(idx_metadata_key(tenant, "env", "prod", job_id).as_slice())
        .await
        .unwrap()
        .is_none()
    {
        missing.push("IDX_METADATA");
    }
    if reader
        .get(attempt_key(tenant, job_id, 1).as_slice())
        .await
        .unwrap()
        .is_none()
    {
        missing.push("ATTEMPT");
    }
    if reader
        .get(job_cancelled_key(tenant, job_id).as_slice())
        .await
        .unwrap()
        .is_none()
    {
        missing.push("JOB_CANCELLED");
    }
    missing
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drops_old_completed_jobs_on_compaction() {
    // Retention is 5s; we age old rows to 10s in the past so they are firmly
    // beyond the cutoff by the time compaction runs.
    let retention = Duration::from_secs(5);
    let now_ms = now_epoch_ms();
    let old_ms = now_ms - 10_000;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db_path = Path::from("/testdb");

    // ── Write phase ─────────────────────────────────────────────────────
    {
        let db = Db::builder(db_path.clone(), Arc::clone(&store))
            .with_merge_operator(counter_merge_operator())
            .build()
            .await
            .unwrap();

        // Expired terminal jobs — the filter should drop every row.
        write_job_family(
            &db,
            "tenant-a",
            "job-old-ok",
            JobStatusKind::Succeeded,
            old_ms,
        )
        .await;
        write_job_family(
            &db,
            "tenant-a",
            "job-old-fail",
            JobStatusKind::Failed,
            old_ms,
        )
        .await;
        write_job_family(
            &db,
            "tenant-b",
            "job-old-cancel",
            JobStatusKind::Cancelled,
            old_ms,
        )
        .await;

        // Recent terminal — rows must be kept until they age past retention.
        write_job_family(
            &db,
            "tenant-a",
            "job-new-ok",
            JobStatusKind::Succeeded,
            now_ms,
        )
        .await;

        // Old non-terminal — job is still active so nothing should be dropped.
        write_job_family(
            &db,
            "tenant-a",
            "job-old-run",
            JobStatusKind::Running,
            old_ms,
        )
        .await;

        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        db.close().await.unwrap();
    }

    // ── Compact phase ───────────────────────────────────────────────────
    let admin = AdminBuilder::new(db_path.clone(), Arc::clone(&store)).build();

    let state = admin.read_compactor_state_view().await.unwrap();
    let l0_ids: Vec<Ulid> = state
        .manifest()
        .l0
        .iter()
        .map(|s| s.sst.id.unwrap_compacted_id())
        .collect();
    assert!(
        !l0_ids.is_empty(),
        "expected at least one L0 SST after flush, got zero"
    );

    // Sanity check: before compaction, every row we wrote is readable.
    {
        let reader = DbReaderBuilder::new(db_path.clone(), Arc::clone(&store))
            .with_merge_operator(counter_merge_operator())
            .build()
            .await
            .unwrap();
        for (tenant, job_id) in [
            ("tenant-a", "job-old-ok"),
            ("tenant-a", "job-old-fail"),
            ("tenant-b", "job-old-cancel"),
            ("tenant-a", "job-new-ok"),
            ("tenant-a", "job-old-run"),
        ] {
            let missing = missing_family_keys(&reader, tenant, job_id).await;
            assert!(
                missing.is_empty(),
                "pre-compaction: {tenant}/{job_id} missing keys: {:?}",
                missing,
            );
        }
    }

    let supplier = Arc::new(CompletedJobCompactionFilterSupplier::new(
        retention,
        db_path.clone(),
        Arc::clone(&store),
    ));

    let compactor = CompactorBuilder::new(db_path.clone(), Arc::clone(&store))
        .with_options(test_compactor_options())
        .with_merge_operator(counter_merge_operator())
        .with_compaction_filter_supplier(supplier)
        .build();

    let run_task = tokio::spawn({
        let compactor = compactor.clone();
        async move { compactor.run().await }
    });
    // Give the compactor a moment to load state before we submit.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let sources: Vec<SourceId> = l0_ids.iter().map(|&id| SourceId::SstView(id)).collect();
    // Compaction destination must be `>= highest_existing_sr.id + 1` for
    // L0-only specs; `compacted` is empty here so 0 is valid.
    submit_and_wait(
        &admin,
        CompactionSpec::new(sources, 0),
        Duration::from_secs(30),
    )
    .await;

    compactor.stop().await.unwrap();
    let _ = run_task.await;

    // ── Verify phase ────────────────────────────────────────────────────
    let reader = DbReaderBuilder::new(db_path.clone(), Arc::clone(&store))
        .with_merge_operator(counter_merge_operator())
        .build()
        .await
        .unwrap();

    // Dest was the only sorted run after compaction → `is_dest_last_run =
    // true` → the filter chose `Drop`, so expired rows are gone.
    for (tenant, job_id) in [
        ("tenant-a", "job-old-ok"),
        ("tenant-a", "job-old-fail"),
        ("tenant-b", "job-old-cancel"),
    ] {
        let missing = missing_family_keys(&reader, tenant, job_id).await;
        assert_eq!(
            missing.len(),
            5,
            "expired {tenant}/{job_id} should be fully dropped; still present: {:?}",
            ([
                "JOB_STATUS",
                "JOB_INFO",
                "IDX_METADATA",
                "ATTEMPT",
                "JOB_CANCELLED"
            ])
            .iter()
            .filter(|k| !missing.contains(k))
            .collect::<Vec<_>>(),
        );
    }

    // Recent terminal + old non-terminal jobs must keep every row.
    for (tenant, job_id, reason) in [
        ("tenant-a", "job-new-ok", "recent terminal"),
        ("tenant-a", "job-old-run", "old non-terminal"),
    ] {
        let missing = missing_family_keys(&reader, tenant, job_id).await;
        assert!(
            missing.is_empty(),
            "{reason} {tenant}/{job_id} should be fully preserved; missing: {:?}",
            missing,
        );
    }

    // IDX_STATUS_TIME keys are self-describing: expired terminal keys gone,
    // non-terminal keys with identical timestamp preserved.
    assert_eq!(
        reader
            .get(idx_status_time_key("tenant-a", "Succeeded", old_ms, "job-old-ok").as_slice())
            .await
            .unwrap(),
        None,
    );
    assert!(
        reader
            .get(idx_status_time_key("tenant-a", "Running", old_ms, "job-old-run").as_slice())
            .await
            .unwrap()
            .is_some(),
        "non-terminal idx_status_time rows must not be dropped even when old"
    );
    assert!(
        reader
            .get(idx_status_time_key("tenant-a", "Succeeded", now_ms, "job-new-ok").as_slice())
            .await
            .unwrap()
            .is_some(),
        "recent terminal idx_status_time rows must not be dropped"
    );
}

// Integration coverage for the tombstone path is intentionally not included
// here. Exercising `is_dest_last_run = false` requires orchestrating at
// least two sorted runs AND a subsequent SR-only compaction whose
// destination sits above the lowest-id SR. In practice slatedb's
// Admin-submit + out-of-band-Compactor pair routinely loses state between
// sequential submissions (the Admin.submit_compaction store version lags
// the Compactor's updates), causing back-to-back L0 submissions with the
// needed destinations to be rejected as `InvalidCompaction`. The tombstone
// branch of the filter is covered at the unit-test level in
// `silo_compactor::compaction_filter::tests::{drop_decision_uses_tombstone_when_not_last_run,
// tombstones_old_completed_status_when_not_last_run}` which directly
// verifies the decision against a synthesized filter with
// `is_dest_last_run = false`.

/// Submit a compaction spec and poll the manifest until every source listed
/// in the spec has been consumed (L0 SSTs removed from `l0`, sorted runs
/// removed from `compacted`).
async fn submit_and_wait(admin: &slatedb::admin::Admin, spec: CompactionSpec, timeout: Duration) {
    let expected_l0: Vec<Ulid> = spec
        .sources()
        .iter()
        .filter_map(|s| match s {
            SourceId::SstView(id) => Some(*id),
            _ => None,
        })
        .collect();
    let expected_srs: Vec<u32> = spec
        .sources()
        .iter()
        .filter_map(|s| match s {
            SourceId::SortedRun(id) => Some(*id),
            _ => None,
        })
        .collect();

    admin.submit_compaction(spec).await.unwrap();

    let deadline = Instant::now() + timeout;
    loop {
        let state = admin.read_compactor_state_view().await.unwrap();
        let current_l0: HashSet<Ulid> = state
            .manifest()
            .l0
            .iter()
            .map(|s| s.sst.id.unwrap_compacted_id())
            .collect();
        let current_srs: HashSet<u32> = state.manifest().compacted.iter().map(|sr| sr.id).collect();
        let l0_done = expected_l0.iter().all(|id| !current_l0.contains(id));
        let srs_done = expected_srs.iter().all(|id| !current_srs.contains(id));
        if l0_done && srs_done {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "submit_and_wait: timed out. l0 remaining={}/{}, srs remaining={}/{}",
                expected_l0
                    .iter()
                    .filter(|id| current_l0.contains(id))
                    .count(),
                expected_l0.len(),
                expected_srs
                    .iter()
                    .filter(|id| current_srs.contains(id))
                    .count(),
                expected_srs.len(),
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
