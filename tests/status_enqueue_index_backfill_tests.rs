//! The version-2 listing-index backfill: the sweep that writes the
//! status-segmented enqueue-time index (`IDX_STATUS_ENQUEUE_TIME`) beside
//! the enqueue-time index, removes stale and orphaned entries, and records
//! version 2 in the completion marker.

mod test_helpers;

use std::sync::Arc;
use std::time::Duration;

use silo::gubernator::MockGubernatorClient;
use silo::instrumented_db::InstrumentedDb;
use silo::job::{JobStatus, ListingSegment};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::{
    ENQUEUE_TIME_INDEX_VERSION, JobStoreShard, STATUS_ENQUEUE_TIME_INDEX_VERSION,
};
use silo::keys::{
    StatusEnqueueTimeIndexValue, end_bound, enqueue_time_index_backfill_complete_key,
    enqueue_time_index_backfill_progress_key, idx_enqueue_time_key, idx_enqueue_time_tenant_prefix,
    idx_status_enqueue_time_key, idx_status_enqueue_time_tenant_prefix, job_info_key,
    job_status_key, parse_enqueue_time_index_key, parse_status_enqueue_time_index_key,
};
use silo::query::ShardQueryEngine;
use silo::settings::{Backend, DatabaseConfig, EnqueueTimeIndexBackfillConfig};
use silo::shard_range::ShardRange;
use test_helpers::*;

const TENANT: &str = "-";
const PRIORITY: u8 = 10;

fn sweep_config(enabled: bool, status_index: bool) -> EnqueueTimeIndexBackfillConfig {
    EnqueueTimeIndexBackfillConfig {
        enabled,
        status_index,
        metadata_index: false,
        batch_size: 4,
        pause_ms: 0,
    }
}

/// Open (or reopen) a full-range shard at `path` with the given backfill settings.
async fn open_shard_at(
    path: &std::path::Path,
    terminal_expire_s: Option<u64>,
    backfill: EnqueueTimeIndexBackfillConfig,
) -> Arc<JobStoreShard> {
    open_shard_with_range(path, ShardRange::full(), terminal_expire_s, backfill).await
}

/// Open (or reopen) the shard at `path` owning `range`, as a split child does
/// over its parent's data.
async fn open_shard_with_range(
    path: &std::path::Path,
    range: ShardRange,
    terminal_expire_s: Option<u64>,
    backfill: EnqueueTimeIndexBackfillConfig,
) -> Arc<JobStoreShard> {
    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: path.to_string_lossy().to_string(),
        slatedb: Some(fast_flush_slatedb_settings()),
        completed_job_expire_s: terminal_expire_s,
        terminal_job_expire_s: terminal_expire_s,
        enqueue_time_index_backfill: backfill,
        ..Default::default()
    };
    JobStoreShard::open(&cfg, MockGubernatorClient::new_arc(), None, range)
        .await
        .expect("open shard")
}

async fn enqueue_many(shard: &JobStoreShard, prefix: &str, count: usize) -> Vec<String> {
    enqueue_many_for(shard, TENANT, prefix, count).await
}

async fn enqueue_many_for(
    shard: &JobStoreShard,
    tenant: &str,
    prefix: &str,
    count: usize,
) -> Vec<String> {
    let mut ids = Vec::with_capacity(count);
    for i in 0..count {
        let id = format!("{prefix}-{i:04}");
        shard
            .enqueue(
                tenant,
                Some(id.clone()),
                PRIORITY,
                now_ms() + i as i64,
                None,
                msgpack_payload(&serde_json::json!({"i": i})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
        ids.push(id);
    }
    ids
}

/// Remove both listing-index entries for `ids`, simulating jobs created
/// before the indexes existed.
async fn strip_entries(shard: &JobStoreShard, ids: &[String]) {
    for id in ids {
        let job = shard
            .get_job(TENANT, id)
            .await
            .expect("get_job")
            .expect("job exists");
        let status = stored_status(shard, id).await;
        let db = shard.db();
        db.delete(idx_enqueue_time_key(TENANT, job.enqueue_time_ms(), id))
            .await
            .expect("delete enqueue-time entry");
        db.delete(idx_status_enqueue_time_key(
            TENANT,
            ListingSegment::from_status(&status),
            job.priority(),
            job.enqueue_time_ms(),
            id,
        ))
        .await
        .expect("delete status-segmented entry");
    }
    shard.db().flush().await.expect("flush");
}

async fn stored_status(shard: &JobStoreShard, id: &str) -> JobStatus {
    shard
        .get_job_status(TENANT, id)
        .await
        .expect("get_job_status")
        .expect("status exists")
}

/// `(job_id, expire_ts)` of the tenant's enqueue-time entries, in key order.
async fn enqueue_time_entries(db: &InstrumentedDb) -> Vec<(String, Option<i64>)> {
    enqueue_time_entries_for(db, TENANT).await
}

async fn enqueue_time_entries_for(db: &InstrumentedDb, tenant: &str) -> Vec<(String, Option<i64>)> {
    let start = idx_enqueue_time_tenant_prefix(tenant);
    let end = end_bound(&start);
    let mut iter = db.scan::<Vec<u8>, _>(start..end).await.expect("scan");
    let mut out = Vec::new();
    while let Some(kv) = iter.next().await.expect("next") {
        let parsed = parse_enqueue_time_index_key(&kv.key).expect("enqueue-time key");
        out.push((parsed.job_id, kv.expire_ts));
    }
    out
}

/// One decoded status-segmented entry.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    segment: ListingSegment,
    job_id: String,
    next_attempt_starts_after_ms: Option<i64>,
    expire_ts: Option<i64>,
}

/// The tenant's status-segmented entries, in key order.
async fn status_entries(db: &InstrumentedDb) -> Vec<Entry> {
    status_entries_for(db, TENANT).await
}

async fn status_entries_for(db: &InstrumentedDb, tenant: &str) -> Vec<Entry> {
    let start = idx_status_enqueue_time_tenant_prefix(tenant);
    let end = end_bound(&start);
    let mut iter = db.scan::<Vec<u8>, _>(start..end).await.expect("scan");
    let mut out = Vec::new();
    while let Some(kv) = iter.next().await.expect("next") {
        let key = parse_status_enqueue_time_index_key(&kv.key).expect("status-segmented key");
        let value = StatusEnqueueTimeIndexValue::decode(&kv.value).expect("entry value");
        out.push(Entry {
            segment: key.segment,
            job_id: key.job_id,
            next_attempt_starts_after_ms: value.next_attempt_starts_after_ms,
            expire_ts: kv.expire_ts,
        });
    }
    out
}

fn sorted(mut ids: Vec<String>) -> Vec<String> {
    ids.sort();
    ids
}

/// Every job in the tenant has exactly one entry in each index, in the
/// segment of its stored status, with its status's next-attempt time and
/// `JOB_INFO`'s TTL; and no entry points at a missing job.
async fn assert_indexes_match_jobs(shard: &JobStoreShard) {
    let db = shard.db();
    let expected_ids = sorted(shard.scan_jobs(TENANT, None).await.expect("scan_jobs"));

    let enqueue = enqueue_time_entries(db).await;
    let mut enqueue_ids: Vec<String> = enqueue.iter().map(|(id, _)| id.clone()).collect();
    enqueue_ids.sort();
    assert_eq!(enqueue_ids, expected_ids, "enqueue-time entries vs jobs");

    let status = status_entries(db).await;
    let mut status_ids: Vec<String> = status.iter().map(|e| e.job_id.clone()).collect();
    status_ids.sort();
    assert_eq!(status_ids, expected_ids, "status-segmented entries vs jobs");

    for entry in &status {
        let stored = stored_status(shard, &entry.job_id).await;
        assert_eq!(
            entry.segment,
            ListingSegment::from_status(&stored),
            "{}: segment",
            entry.job_id
        );
        assert_eq!(
            entry.next_attempt_starts_after_ms,
            StatusEnqueueTimeIndexValue::for_status(&stored).next_attempt_starts_after_ms,
            "{}: value",
            entry.job_id
        );
        let info_ttl = db
            .get_key_value(job_info_key(TENANT, &entry.job_id))
            .await
            .expect("get_key_value")
            .expect("JOB_INFO present")
            .expire_ts;
        assert_eq!(entry.expire_ts, info_ttl, "{}: TTL", entry.job_id);
    }
}

async fn marker_version(db: &InstrumentedDb) -> Option<Vec<u8>> {
    db.get(&enqueue_time_index_backfill_complete_key())
        .await
        .expect("get marker")
        .map(|v| v.to_vec())
}

#[silo::test]
async fn enabled_sweep_with_status_index_reaches_version_2_in_one_run() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 10).await;
    strip_entries(&shard, &ids).await;
    assert_eq!(shard.listing_index_version(), 0);
    assert!(enqueue_time_entries(shard.db()).await.is_empty());
    assert!(status_entries(shard.db()).await.is_empty());
    shard.close().await.expect("close");

    let shard = open_shard_at(tmp.path(), None, sweep_config(true, true)).await;
    poll_until(
        || async { shard.listing_index_version() },
        |v| *v == STATUS_ENQUEUE_TIME_INDEX_VERSION,
        10_000,
    )
    .await;
    assert_eq!(
        shard.listing_index_version(),
        STATUS_ENQUEUE_TIME_INDEX_VERSION
    );
    assert!(shard.status_enqueue_time_index_complete());
    assert_eq!(
        marker_version(shard.db()).await,
        Some(vec![STATUS_ENQUEUE_TIME_INDEX_VERSION]),
        "the marker records version 2"
    );
    assert_eq!(
        sorted(
            enqueue_time_entries(shard.db())
                .await
                .into_iter()
                .map(|(id, _)| id)
                .collect()
        ),
        ids
    );
    assert_indexes_match_jobs(&shard).await;

    // The query engine serves a status listing from the new index once the
    // marker reads version 2.
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let query = format!(
        "SELECT id FROM jobs WHERE tenant = '{TENANT}' AND status_kind = 'Waiting' ORDER BY enqueue_time_ms DESC, id ASC"
    );
    let explain = engine
        .sql(&format!("EXPLAIN {query}"))
        .await
        .expect("explain")
        .collect()
        .await
        .expect("collect");
    let explain = datafusion::arrow::util::pretty::pretty_format_batches(&explain)
        .expect("format")
        .to_string();
    assert!(explain.contains("path=StatusEnqueueTimeIndex"), "{explain}");
    let listed = engine
        .sql(&query)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    let listed: Vec<String> = listed
        .iter()
        .flat_map(|b| {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .expect("string column");
            (0..b.num_rows())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(sorted(listed), ids);
}

/// Plant a status-segmented entry for `job_id` in `segment` at the standard
/// priority and `enqueue_time_ms`, regardless of the job's stored status.
async fn plant_status_entry(
    db: &InstrumentedDb,
    segment: ListingSegment,
    enqueue_time_ms: i64,
    job_id: &str,
) -> Vec<u8> {
    plant_status_entry_for(db, TENANT, segment, enqueue_time_ms, job_id).await
}

async fn plant_status_entry_for(
    db: &InstrumentedDb,
    tenant: &str,
    segment: ListingSegment,
    enqueue_time_ms: i64,
    job_id: &str,
) -> Vec<u8> {
    let key = idx_status_enqueue_time_key(tenant, segment, PRIORITY, enqueue_time_ms, job_id);
    db.put(
        &key,
        StatusEnqueueTimeIndexValue {
            next_attempt_starts_after_ms: None,
        }
        .encode(),
    )
    .await
    .expect("plant entry");
    key
}

async fn write_progress(db: &InstrumentedDb, json: &str) {
    db.put(enqueue_time_index_backfill_progress_key(), json.as_bytes())
        .await
        .expect("write checkpoint");
}

async fn run_sweep(
    shard: &JobStoreShard,
    target: u8,
) -> silo::job_store_shard::ListingIndexBackfillResult {
    let result = shard
        .backfill_listing_index(target, 4, Duration::ZERO)
        .await
        .expect("backfill");
    assert!(result.complete, "{result:?}");
    assert!(!result.cancelled, "{result:?}");
    assert_eq!(shard.listing_index_version(), target);
    result
}

#[silo::test]
async fn empty_marker_loads_as_version_1_and_the_flag_gates_the_version_2_sweep() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), Some(60), Default::default()).await;
    let ids = enqueue_many(&shard, "job", 6).await;
    // One terminal job whose enqueue-time entry deliberately carries no TTL,
    // so a rewrite by the sweep would be visible.
    let tasks = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    let done_id = tasks[0].attempt().job_id().to_string();
    shard
        .report_attempt_outcome(
            tasks[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report");
    let done_job = shard
        .get_job(TENANT, &done_id)
        .await
        .expect("get_job")
        .expect("exists");
    let done_key = idx_enqueue_time_key(TENANT, done_job.enqueue_time_ms(), &done_id);
    shard
        .db()
        .put(&done_key, [])
        .await
        .expect("re-put without TTL");
    // Only the status-segmented entries are missing, as on a shard that
    // sits at version 1.
    for entry in status_entries(shard.db()).await {
        let job = shard
            .get_job(TENANT, &entry.job_id)
            .await
            .expect("get_job")
            .expect("exists");
        shard
            .db()
            .delete(idx_status_enqueue_time_key(
                TENANT,
                entry.segment,
                PRIORITY,
                job.enqueue_time_ms(),
                &entry.job_id,
            ))
            .await
            .expect("delete");
    }
    shard
        .db()
        .put(enqueue_time_index_backfill_complete_key(), [])
        .await
        .expect("empty marker");
    shard.db().flush().await.expect("flush");
    shard.close().await.expect("close");

    let shard = open_shard_at(tmp.path(), Some(60), sweep_config(true, false)).await;
    assert_eq!(shard.listing_index_version(), ENQUEUE_TIME_INDEX_VERSION);
    assert!(shard.enqueue_time_index_complete());
    assert!(!shard.status_enqueue_time_index_complete());
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(
        shard.listing_index_version(),
        ENQUEUE_TIME_INDEX_VERSION,
        "without status_index the sweep must not run"
    );
    assert!(status_entries(shard.db()).await.is_empty());
    shard.close().await.expect("close");

    let shard = open_shard_at(tmp.path(), Some(60), sweep_config(true, true)).await;
    poll_until(
        || async { shard.listing_index_version() },
        |v| *v == STATUS_ENQUEUE_TIME_INDEX_VERSION,
        10_000,
    )
    .await;
    assert_eq!(
        shard.listing_index_version(),
        STATUS_ENQUEUE_TIME_INDEX_VERSION
    );
    let enqueue = enqueue_time_entries(shard.db()).await;
    assert_eq!(
        sorted(enqueue.iter().map(|(id, _)| id.clone()).collect()),
        ids,
        "enqueue-time entries are neither duplicated nor dropped"
    );
    assert_eq!(
        shard
            .db()
            .get_key_value(&done_key)
            .await
            .expect("get")
            .expect("present")
            .expire_ts,
        None,
        "an existing enqueue-time entry is not rewritten"
    );
    assert_indexes_match_jobs(&shard).await;
}

fn versionless_checkpoint(after_id: &str) -> String {
    format!(
        r#"{{"last_key":"{}","rows_scanned":8,"rows_written":8}}"#,
        hex::encode(job_info_key(TENANT, after_id))
    )
}

#[silo::test]
async fn versionless_checkpoint_is_honoured_by_a_version_1_sweep() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 10).await;
    strip_entries(&shard, &ids).await;
    write_progress(shard.db(), &versionless_checkpoint(&ids[7])).await;

    run_sweep(&shard, ENQUEUE_TIME_INDEX_VERSION).await;

    assert_eq!(
        sorted(
            enqueue_time_entries(shard.db())
                .await
                .into_iter()
                .map(|(id, _)| id)
                .collect()
        ),
        ids[8..].to_vec(),
        "a version-1 sweep resumes after the versionless checkpoint"
    );
}

#[silo::test]
async fn versionless_checkpoint_is_discarded_by_a_version_2_sweep() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 10).await;
    strip_entries(&shard, &ids).await;
    write_progress(shard.db(), &versionless_checkpoint(&ids[7])).await;
    shard.close().await.expect("close");

    let shard = open_shard_at(tmp.path(), None, sweep_config(true, true)).await;
    poll_until(
        || async { shard.listing_index_version() },
        |v| *v == STATUS_ENQUEUE_TIME_INDEX_VERSION,
        10_000,
    )
    .await;
    assert_eq!(
        shard.listing_index_version(),
        STATUS_ENQUEUE_TIME_INDEX_VERSION
    );
    assert_indexes_match_jobs(&shard).await;
    assert_eq!(
        sorted(
            enqueue_time_entries(shard.db())
                .await
                .into_iter()
                .map(|(id, _)| id)
                .collect()
        ),
        ids,
        "the sweep restarts from the range start"
    );
}

#[silo::test]
async fn sweep_resumes_a_version_2_checkpoint_after_restart_in_the_jobs_pass() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 200).await;
    strip_entries(&shard, &ids).await;
    shard.close().await.expect("close");

    let shard = open_shard_at(
        tmp.path(),
        None,
        EnqueueTimeIndexBackfillConfig {
            enabled: true,
            status_index: true,
            metadata_index: false,
            batch_size: 1,
            pause_ms: 20,
        },
    )
    .await;
    let indexed_before_close = poll_until(
        || async { status_entries(shard.db()).await.len() },
        |n| *n >= 5,
        10_000,
    )
    .await;
    shard.close().await.expect("close");

    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    assert_eq!(shard.listing_index_version(), 0, "sweep was interrupted");
    let checkpoint = shard
        .db()
        .get(&enqueue_time_index_backfill_progress_key())
        .await
        .expect("get")
        .expect("checkpoint persisted");
    assert!(
        String::from_utf8_lossy(&checkpoint).contains(r#""version":2"#),
        "checkpoint carries its target version: {}",
        String::from_utf8_lossy(&checkpoint)
    );
    let resumed = status_entries(shard.db()).await.len();
    assert!(
        resumed >= indexed_before_close && resumed < 200,
        "{resumed} entries should survive the close and leave the sweep partial"
    );

    let result = shard
        .backfill_listing_index(STATUS_ENQUEUE_TIME_INDEX_VERSION, 50, Duration::ZERO)
        .await
        .expect("backfill");
    assert!(result.complete);
    assert!(
        result.rows_scanned >= 200,
        "counts accumulate across the resume: {result:?}"
    );
    assert_eq!(
        shard.listing_index_version(),
        STATUS_ENQUEUE_TIME_INDEX_VERSION
    );
    assert_indexes_match_jobs(&shard).await;
}

#[silo::test]
async fn sweep_resumes_a_checkpoint_inside_the_orphan_pass() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 3).await;
    let db = shard.db();
    // Orphans in both families for jobs that do not exist. The status
    // orphans share a segment, priority, and enqueue time so they sort by
    // id; the enqueue-time family sorts before the status family.
    db.put(idx_enqueue_time_key(TENANT, 5_000, "orphan-e1"), [])
        .await
        .expect("plant");
    db.put(idx_enqueue_time_key(TENANT, 4_000, "orphan-e2"), [])
        .await
        .expect("plant");
    let mut status_keys = Vec::new();
    for id in ["orphan-a", "orphan-b", "orphan-c", "orphan-d"] {
        status_keys.push(plant_status_entry(db, ListingSegment::Failed, 7_000, id).await);
    }
    write_progress(
        db,
        &format!(
            r#"{{"version":2,"pass":"Orphans","last_key":"{}","rows_scanned":3,"rows_written":3}}"#,
            hex::encode(&status_keys[1])
        ),
    )
    .await;

    run_sweep(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;

    let remaining_status: Vec<String> = status_entries(db)
        .await
        .into_iter()
        .map(|e| e.job_id)
        .filter(|id| id.starts_with("orphan"))
        .collect();
    assert_eq!(
        remaining_status,
        vec!["orphan-a".to_string(), "orphan-b".to_string()],
        "orphans after the checkpoint are removed, those before it are not revisited"
    );
    let remaining_enqueue: Vec<String> = enqueue_time_entries(db)
        .await
        .into_iter()
        .map(|(id, _)| id)
        .filter(|id| id.starts_with("orphan"))
        .collect();
    assert_eq!(
        remaining_enqueue,
        vec!["orphan-e1".to_string(), "orphan-e2".to_string()],
        "the enqueue-time family sits before the checkpoint"
    );
    for id in &ids {
        assert!(
            status_entries(db).await.iter().any(|e| &e.job_id == id),
            "{id} keeps its entry"
        );
    }

    // A full sweep from the start removes the rest.
    shard
        .set_listing_index_version(0)
        .await
        .expect("clear marker");
    run_sweep(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    assert_indexes_match_jobs(&shard).await;
}

#[silo::test]
async fn stale_entry_in_another_segment_is_removed() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 3).await;
    let job = shard
        .get_job(TENANT, &ids[0])
        .await
        .expect("get_job")
        .expect("exists");
    plant_status_entry(
        shard.db(),
        ListingSegment::Failed,
        job.enqueue_time_ms(),
        &ids[0],
    )
    .await;
    plant_status_entry(
        shard.db(),
        ListingSegment::Running,
        job.enqueue_time_ms(),
        &ids[0],
    )
    .await;
    assert_eq!(status_entries(shard.db()).await.len(), 5);

    run_sweep(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;

    let entries: Vec<Entry> = status_entries(shard.db())
        .await
        .into_iter()
        .filter(|e| e.job_id == ids[0])
        .collect();
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(entries[0].segment, ListingSegment::ScheduledFirst);
    assert_indexes_match_jobs(&shard).await;
}

#[silo::test]
async fn orphaned_entries_in_both_families_are_removed() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    enqueue_many(&shard, "job", 3).await;
    let db = shard.db();
    db.put(idx_enqueue_time_key(TENANT, 5_000, "gone"), [])
        .await
        .expect("plant");
    plant_status_entry(db, ListingSegment::Succeeded, 5_000, "gone").await;
    plant_status_entry(db, ListingSegment::Cancelled, 6_000, "also-gone").await;

    run_sweep(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;

    assert!(
        !enqueue_time_entries(db)
            .await
            .iter()
            .any(|(id, _)| id == "gone")
    );
    assert!(
        !status_entries(db)
            .await
            .iter()
            .any(|e| e.job_id == "gone" || e.job_id == "also-gone")
    );
    assert_indexes_match_jobs(&shard).await;
}

#[silo::test]
async fn job_without_a_status_record_gets_only_an_enqueue_time_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 2).await;
    strip_entries(&shard, &ids).await;
    let job = shard
        .get_job(TENANT, &ids[0])
        .await
        .expect("get_job")
        .expect("exists");
    // A stale entry the sweep must still remove even without a status.
    plant_status_entry(
        shard.db(),
        ListingSegment::Running,
        job.enqueue_time_ms(),
        &ids[0],
    )
    .await;
    shard
        .db()
        .delete(job_status_key(TENANT, &ids[0]))
        .await
        .expect("drop status");

    run_sweep(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;

    assert_eq!(
        sorted(
            enqueue_time_entries(shard.db())
                .await
                .into_iter()
                .map(|(id, _)| id)
                .collect()
        ),
        ids
    );
    let status: Vec<String> = status_entries(shard.db())
        .await
        .into_iter()
        .map(|e| e.job_id)
        .collect();
    assert_eq!(status, vec![ids[1].clone()]);
}

#[silo::test]
async fn terminal_job_entry_carries_the_job_ttl() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), Some(60), Default::default()).await;
    let ids = enqueue_many(&shard, "job", 2).await;
    let tasks = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    let done_id = tasks[0].attempt().job_id().to_string();
    shard
        .report_attempt_outcome(
            tasks[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report");
    strip_entries(&shard, &ids).await;

    run_sweep(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;

    let done: Vec<Entry> = status_entries(shard.db())
        .await
        .into_iter()
        .filter(|e| e.job_id == done_id)
        .collect();
    assert_eq!(done.len(), 1, "{done:?}");
    assert_eq!(done[0].segment, ListingSegment::Succeeded);
    assert!(done[0].expire_ts.is_some(), "{done:?}");
    assert_indexes_match_jobs(&shard).await;
}

#[silo::test]
async fn jobs_deleted_or_transitioned_mid_sweep_end_in_the_right_place() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 60).await;
    strip_entries(&shard, &ids).await;
    let tasks = shard
        .dequeue("worker", "default", 60)
        .await
        .expect("dequeue")
        .tasks;
    let victim = ids[40].clone();
    let victim_task = tasks
        .iter()
        .find(|t| t.attempt().job_id() == victim)
        .expect("victim dequeued");
    shard
        .report_attempt_outcome(
            victim_task.attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report victim");
    let late = tasks
        .iter()
        .find(|t| t.attempt().job_id() == ids[45])
        .expect("late dequeued");

    let sweeper = Arc::clone(&shard);
    let sweep = tokio::spawn(async move {
        sweeper
            .backfill_listing_index(
                STATUS_ENQUEUE_TIME_INDEX_VERSION,
                1,
                Duration::from_millis(5),
            )
            .await
            .expect("backfill")
    });
    tokio::time::sleep(Duration::from_millis(60)).await;
    shard.delete_job(TENANT, &victim).await.expect("delete_job");
    shard
        .report_attempt_outcome(
            late.attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report late");
    let result = sweep.await.expect("join");

    assert!(result.complete);
    assert!(
        !status_entries(shard.db())
            .await
            .iter()
            .any(|e| e.job_id == victim)
    );
    let late_entries: Vec<Entry> = status_entries(shard.db())
        .await
        .into_iter()
        .filter(|e| e.job_id == ids[45])
        .collect();
    assert_eq!(late_entries.len(), 1, "{late_entries:?}");
    assert_eq!(late_entries[0].segment, ListingSegment::Succeeded);
    assert_indexes_match_jobs(&shard).await;
}

#[silo::test]
async fn disabled_sweep_keeps_version_0_and_an_empty_marker_reads_version_1() {
    let query = "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '-'";
    let plan_line = |explain: String| {
        explain
            .lines()
            .find(|l| l.contains("SiloExecutionPlan:"))
            .unwrap_or_else(|| panic!("no SiloExecutionPlan line in EXPLAIN:\n{explain}"))
            .trim()
            .to_string()
    };

    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    enqueue_many(&shard, "job", 3).await;
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert_eq!(shard.listing_index_version(), 0);
    assert!(!shard.enqueue_time_index_complete());
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let line = plan_line(engine.explain(query).await.expect("explain"));
    assert!(
        !line.contains("path=EnqueueTimeIndex"),
        "version 0 must not take the enqueue-time path: {line}"
    );
    drop(engine);

    shard
        .db()
        .put(enqueue_time_index_backfill_complete_key(), [])
        .await
        .expect("empty marker");
    shard.db().flush().await.expect("flush");
    shard.close().await.expect("close");

    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    assert_eq!(shard.listing_index_version(), ENQUEUE_TIME_INDEX_VERSION);
    assert!(shard.enqueue_time_index_complete());
    assert!(!shard.status_enqueue_time_index_complete());
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let line = plan_line(engine.explain(query).await.expect("explain"));
    assert!(
        line.contains("path=EnqueueTimeIndex"),
        "version 1 takes the enqueue-time path: {line}"
    );
}

#[silo::test]
async fn entry_in_the_right_segment_with_a_stale_value_or_ttl_is_rewritten_once() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), Some(60), Default::default()).await;
    let ids = enqueue_many(&shard, "job", 2).await;
    let tasks = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    shard
        .report_attempt_outcome(
            tasks[0].attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report");
    strip_entries(&shard, &ids).await;

    // Plant each job's entry in its own segment, but with an empty value and
    // no TTL: the scheduled job's entry lacks its next-attempt time and the
    // succeeded job's entry lacks the job record's TTL.
    for id in &ids {
        let job = shard
            .get_job(TENANT, id)
            .await
            .expect("get_job")
            .expect("exists");
        let stored = stored_status(&shard, id).await;
        plant_status_entry(
            shard.db(),
            ListingSegment::from_status(&stored),
            job.enqueue_time_ms(),
            id,
        )
        .await;
    }
    assert_eq!(status_entries(shard.db()).await.len(), 2);

    let first = run_sweep(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    assert!(first.rows_written > 0, "{first:?}");
    assert_indexes_match_jobs(&shard).await;

    shard
        .set_listing_index_version(0)
        .await
        .expect("clear marker");
    let second = run_sweep(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    assert_eq!(second.rows_written, 0, "{second:?}");
    assert_indexes_match_jobs(&shard).await;
}

/// Tenant hashes: zzz sits inside `[, 8000...)`, aaa outside it.
const LEFT_CHILD_END: &str = "8000000000000000";

/// Plant one orphan in each listing-index family for `tenant`: entries
/// whose job has no `JOB_INFO` row.
async fn plant_orphans_for(db: &InstrumentedDb, tenant: &str, suffix: &str) {
    db.put(
        idx_enqueue_time_key(tenant, 5_000, &format!("orphan-e-{suffix}")),
        [],
    )
    .await
    .expect("plant");
    plant_status_entry_for(
        db,
        tenant,
        ListingSegment::Failed,
        7_000,
        &format!("orphan-s-{suffix}"),
    )
    .await;
}

async fn orphan_ids_for(db: &InstrumentedDb, tenant: &str) -> Vec<String> {
    let mut ids: Vec<String> = enqueue_time_entries_for(db, tenant)
        .await
        .into_iter()
        .map(|(id, _)| id)
        .chain(
            status_entries_for(db, tenant)
                .await
                .into_iter()
                .map(|e| e.job_id),
        )
        .filter(|id| id.starts_with("orphan"))
        .collect();
    ids.sort();
    ids
}

/// A split child sweeps only the keys inside its range: orphans of a tenant
/// it owns are removed, and orphans of a tenant outside its range are left
/// for the post-split cleanup.
#[silo::test]
async fn split_child_sweep_removes_in_range_orphans_and_leaves_the_rest() {
    let tmp = tempfile::tempdir().unwrap();
    let parent = open_shard_at(tmp.path(), None, Default::default()).await;
    let inside = enqueue_many_for(&parent, "zzz", "z", 3).await;
    let outside = enqueue_many_for(&parent, "aaa", "a", 3).await;
    plant_orphans_for(parent.db(), "zzz", "in").await;
    plant_orphans_for(parent.db(), "aaa", "out").await;
    parent.close().await.expect("close");

    let child = open_shard_with_range(
        tmp.path(),
        ShardRange::new("", LEFT_CHILD_END),
        None,
        Default::default(),
    )
    .await;
    run_sweep(&child, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;

    let db = child.db();
    assert!(orphan_ids_for(db, "zzz").await.is_empty());
    assert_eq!(
        orphan_ids_for(db, "aaa").await,
        vec!["orphan-e-out".to_string(), "orphan-s-out".to_string()],
        "out-of-range orphans are left for the post-split cleanup"
    );
    for id in &inside {
        assert!(
            status_entries_for(db, "zzz")
                .await
                .iter()
                .any(|e| &e.job_id == id)
        );
    }
    for id in &outside {
        assert!(
            status_entries_for(db, "aaa")
                .await
                .iter()
                .any(|e| &e.job_id == id)
        );
    }
}

/// A child cloned from a parent whose checkpoint sits inside the Orphans
/// pass resumes that pass: orphans after the checkpoint are removed, those
/// before it are not revisited, and the child reaches version 2.
#[silo::test]
async fn split_child_resumes_the_parents_orphan_pass_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let parent = open_shard_at(tmp.path(), None, Default::default()).await;
    let inside = enqueue_many_for(&parent, "zzz", "z", 3).await;
    let db = parent.db();
    db.put(idx_enqueue_time_key("zzz", 5_000, "orphan-e1"), [])
        .await
        .expect("plant");
    let mut status_keys = Vec::new();
    for id in ["orphan-a", "orphan-b", "orphan-c", "orphan-d"] {
        status_keys
            .push(plant_status_entry_for(db, "zzz", ListingSegment::Failed, 7_000, id).await);
    }
    write_progress(
        db,
        &format!(
            r#"{{"version":2,"pass":"Orphans","last_key":"{}","rows_scanned":3,"rows_written":3}}"#,
            hex::encode(&status_keys[1])
        ),
    )
    .await;
    parent.close().await.expect("close");

    let child = open_shard_with_range(
        tmp.path(),
        ShardRange::new("", LEFT_CHILD_END),
        None,
        Default::default(),
    )
    .await;
    assert_eq!(child.listing_index_version(), 0);
    run_sweep(&child, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;

    let db = child.db();
    assert_eq!(
        orphan_ids_for(db, "zzz").await,
        vec![
            "orphan-a".to_string(),
            "orphan-b".to_string(),
            "orphan-e1".to_string()
        ],
        "orphans after the checkpoint are removed, those before it are not revisited"
    );
    for id in &inside {
        assert!(
            status_entries_for(db, "zzz")
                .await
                .iter()
                .any(|e| &e.job_id == id)
        );
    }
}
