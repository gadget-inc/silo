//! The version-3 listing-index backfill: the sweep that writes each job's
//! enqueue time into its metadata index entries (`IDX_METADATA`), removes
//! metadata entries whose job is gone, and records version 3 in the
//! completion marker; and the write paths that keep every new entry
//! carrying the enqueue time.

mod test_helpers;

use std::sync::Arc;
use std::time::Duration;

use silo::gubernator::MockGubernatorClient;
use silo::instrumented_db::InstrumentedDb;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::{
    ENQUEUE_TIME_INDEX_VERSION, JobStoreShard, METADATA_ENQUEUE_TIME_INDEX_VERSION,
    STATUS_ENQUEUE_TIME_INDEX_VERSION,
};
use silo::keys::{
    MetadataIndexValue, end_bound, enqueue_time_index_backfill_complete_key,
    enqueue_time_index_backfill_progress_key, idx_metadata_key, idx_metadata_tenant_prefix,
    job_info_key, parse_metadata_index_key,
};
use silo::query::ShardQueryEngine;
use silo::settings::{Backend, DatabaseConfig, EnqueueTimeIndexBackfillConfig};
use silo::shard_range::ShardRange;
use test_helpers::*;

const TENANT: &str = "-";
const PRIORITY: u8 = 10;

fn sweep_config(enabled: bool, metadata_index: bool) -> EnqueueTimeIndexBackfillConfig {
    EnqueueTimeIndexBackfillConfig {
        enabled,
        status_index: true,
        metadata_index,
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
    JobStoreShard::open(
        &cfg,
        MockGubernatorClient::new_arc(),
        None,
        ShardRange::full(),
    )
    .await
    .expect("open shard")
}

/// The metadata every job in these tests carries: an `env` shared by all
/// jobs and a `tag` unique to each.
fn metadata_for(i: usize) -> Vec<(String, String)> {
    vec![
        ("env".to_string(), "prod".to_string()),
        ("tag".to_string(), format!("t{i}")),
    ]
}

async fn enqueue_many(shard: &JobStoreShard, prefix: &str, count: usize) -> Vec<String> {
    let mut ids = Vec::with_capacity(count);
    for i in 0..count {
        let id = format!("{prefix}-{i:04}");
        shard
            .enqueue(
                TENANT,
                Some(id.clone()),
                PRIORITY,
                now_ms() + i as i64,
                None,
                msgpack_payload(&serde_json::json!({"i": i})),
                vec![],
                Some(metadata_for(i)),
                "default",
            )
            .await
            .expect("enqueue");
        ids.push(id);
    }
    ids
}

/// One decoded metadata index entry.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    key: String,
    value: String,
    job_id: String,
    enqueue_time_ms: Option<i64>,
    expire_ts: Option<i64>,
}

/// The tenant's metadata index entries, in key order.
async fn metadata_entries(db: &InstrumentedDb) -> Vec<Entry> {
    let start = idx_metadata_tenant_prefix(TENANT);
    let end = end_bound(&start);
    let mut iter = db.scan::<Vec<u8>, _>(start..end).await.expect("scan");
    let mut out = Vec::new();
    while let Some(kv) = iter.next().await.expect("next") {
        let key = parse_metadata_index_key(&kv.key).expect("metadata key");
        let value = MetadataIndexValue::decode(&kv.value).expect("entry value");
        out.push(Entry {
            key: key.key,
            value: key.value,
            job_id: key.job_id,
            enqueue_time_ms: value.enqueue_time_ms,
            expire_ts: kv.expire_ts,
        });
    }
    out
}

/// Rewrite every metadata entry of `ids` with an empty value and no TTL,
/// as a binary that predates version 3 wrote them.
async fn strip_values(shard: &JobStoreShard, ids: &[String]) {
    for id in ids {
        let job = shard
            .get_job(TENANT, id)
            .await
            .expect("get_job")
            .expect("job exists");
        for (k, v) in job.metadata() {
            shard
                .db()
                .put(idx_metadata_key(TENANT, &k, &v, id), [])
                .await
                .expect("strip value");
        }
    }
    shard.db().flush().await.expect("flush");
}

/// Every metadata entry in the tenant belongs to an existing job, carries
/// that job's enqueue time, and carries `JOB_INFO`'s TTL; and every job's
/// metadata pair has an entry.
async fn assert_entries_match_jobs(shard: &JobStoreShard) {
    let db = shard.db();
    let mut ids = shard.scan_jobs(TENANT, None).await.expect("scan_jobs");
    ids.sort();
    let entries = metadata_entries(db).await;
    let mut expected = Vec::new();
    for id in &ids {
        let job = shard
            .get_job(TENANT, id)
            .await
            .expect("get_job")
            .expect("exists");
        let info_ttl = db
            .get_key_value(job_info_key(TENANT, id))
            .await
            .expect("get_key_value")
            .expect("JOB_INFO present")
            .expire_ts;
        for (k, v) in job.metadata() {
            expected.push(Entry {
                key: k,
                value: v,
                job_id: id.clone(),
                enqueue_time_ms: Some(job.enqueue_time_ms()),
                expire_ts: info_ttl,
            });
        }
    }
    expected.sort_by(|a, b| (&a.key, &a.value, &a.job_id).cmp(&(&b.key, &b.value, &b.job_id)));
    assert_eq!(entries, expected);
}

async fn marker_version(db: &InstrumentedDb) -> Option<Vec<u8>> {
    db.get(&enqueue_time_index_backfill_complete_key())
        .await
        .expect("get marker")
        .map(|v| v.to_vec())
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

async fn write_progress(db: &InstrumentedDb, json: &str) {
    db.put(enqueue_time_index_backfill_progress_key(), json.as_bytes())
        .await
        .expect("write checkpoint");
}

async fn explain(engine: &ShardQueryEngine, query: &str) -> String {
    let batches = engine
        .sql(&format!("EXPLAIN {query}"))
        .await
        .expect("explain")
        .collect()
        .await
        .expect("collect");
    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .expect("format")
        .to_string()
}

async fn listed_ids(engine: &ShardQueryEngine, query: &str) -> Vec<String> {
    let batches = engine
        .sql(query)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    let mut ids: Vec<String> = batches
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
    ids.sort();
    ids
}

#[test]
fn the_metadata_index_flag_targets_version_3_and_implies_the_status_index() {
    assert_eq!(
        sweep_config(true, false).target_version(),
        STATUS_ENQUEUE_TIME_INDEX_VERSION
    );
    assert_eq!(
        sweep_config(true, true).target_version(),
        METADATA_ENQUEUE_TIME_INDEX_VERSION
    );
    let metadata_only = EnqueueTimeIndexBackfillConfig {
        enabled: true,
        status_index: false,
        metadata_index: true,
        ..Default::default()
    };
    assert_eq!(
        metadata_only.target_version(),
        METADATA_ENQUEUE_TIME_INDEX_VERSION
    );
    assert_eq!(
        EnqueueTimeIndexBackfillConfig::default().target_version(),
        ENQUEUE_TIME_INDEX_VERSION
    );
}

#[silo::test]
async fn enabled_sweep_with_metadata_index_reaches_version_3_in_one_run() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 10).await;
    strip_values(&shard, &ids).await;
    shard
        .db()
        .put(idx_metadata_key(TENANT, "env", "prod", "gone"), [])
        .await
        .expect("plant orphan");
    assert_eq!(shard.listing_index_version(), 0);
    assert!(
        metadata_entries(shard.db())
            .await
            .iter()
            .all(|e| e.enqueue_time_ms.is_none())
    );
    shard.close().await.expect("close");

    let shard = open_shard_at(tmp.path(), None, sweep_config(true, true)).await;
    poll_until(
        || async { shard.listing_index_version() },
        |v| *v == METADATA_ENQUEUE_TIME_INDEX_VERSION,
        10_000,
    )
    .await;
    assert!(shard.metadata_enqueue_time_index_complete());
    assert!(shard.status_enqueue_time_index_complete());
    assert_eq!(
        marker_version(shard.db()).await,
        Some(vec![METADATA_ENQUEUE_TIME_INDEX_VERSION]),
        "the marker records version 3"
    );
    assert_entries_match_jobs(&shard).await;

    // The query engine serves a metadata listing of ids and enqueue times
    // from the index once the marker reads version 3.
    let engine = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("engine");
    let query = format!(
        "SELECT id, enqueue_time_ms FROM jobs WHERE tenant = '{TENANT}' AND array_contains(element_at(metadata, 'env'), 'prod')"
    );
    let plan = explain(&engine, &query).await;
    assert!(plan.contains("path=MetadataIndex"), "{plan}");
    assert_eq!(listed_ids(&engine, &query).await, ids);
}

#[silo::test]
async fn version_3_sweep_rewrites_only_entries_missing_the_enqueue_time() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 8).await;
    strip_values(&shard, &ids[..3]).await;
    shard
        .set_listing_index_version(STATUS_ENQUEUE_TIME_INDEX_VERSION)
        .await
        .expect("set marker");

    let result = run_sweep(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;
    assert_eq!(
        result.rows_scanned,
        8 + 8 * 2 + 8 * 2,
        "the jobs pass, both listing families, and the metadata family"
    );
    assert_eq!(
        result.rows_written, 3,
        "only the jobs with stripped entries are rewritten"
    );
    assert_entries_match_jobs(&shard).await;

    let again = run_sweep(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;
    assert_eq!(again.rows_written, 0);
    assert_eq!(again.rows_scanned, 0, "a completed target runs no pass");
}

#[silo::test]
async fn the_status_index_flag_alone_leaves_metadata_values_untouched() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 4).await;
    strip_values(&shard, &ids).await;

    run_sweep(&shard, STATUS_ENQUEUE_TIME_INDEX_VERSION).await;
    assert!(!shard.metadata_enqueue_time_index_complete());
    assert!(
        metadata_entries(shard.db())
            .await
            .iter()
            .all(|e| e.enqueue_time_ms.is_none()),
        "a version-2 sweep writes no metadata values"
    );

    run_sweep(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;
    assert_entries_match_jobs(&shard).await;
}

#[silo::test]
async fn terminal_job_entry_carries_the_enqueue_time_and_the_job_ttl() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), Some(60), Default::default()).await;
    let ids = enqueue_many(&shard, "job", 3).await;
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
    assert!(ids.contains(&done_id));
    // Re-put the finished job's entries with no value and no TTL.
    strip_values(&shard, std::slice::from_ref(&done_id)).await;
    let before: Vec<Entry> = metadata_entries(shard.db())
        .await
        .into_iter()
        .filter(|e| e.job_id == done_id)
        .collect();
    assert_eq!(before.len(), 2);
    assert!(before.iter().all(|e| e.expire_ts.is_none()));

    run_sweep(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;

    let info_ttl = shard
        .db()
        .get_key_value(job_info_key(TENANT, &done_id))
        .await
        .expect("get_key_value")
        .expect("JOB_INFO present")
        .expire_ts;
    assert!(info_ttl.is_some(), "the finished job carries a TTL");
    for entry in metadata_entries(shard.db())
        .await
        .into_iter()
        .filter(|e| e.job_id == done_id)
    {
        assert_eq!(entry.expire_ts, info_ttl, "{entry:?}");
        assert!(entry.enqueue_time_ms.is_some(), "{entry:?}");
    }
    assert_entries_match_jobs(&shard).await;
}

#[silo::test]
async fn sweep_resumes_a_checkpoint_inside_the_metadata_orphan_pass() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 3).await;
    let db = shard.db();
    // Orphans under one key and value, so they sort by id.
    let mut orphan_keys = Vec::new();
    for id in ["orphan-a", "orphan-b", "orphan-c", "orphan-d"] {
        let key = idx_metadata_key(TENANT, "env", "prod", id);
        db.put(&key, MetadataIndexValue::new(1_000).encode())
            .await
            .expect("plant");
        orphan_keys.push(key);
    }
    write_progress(
        db,
        &format!(
            r#"{{"version":3,"pass":"MetadataOrphans","last_key":"{}","rows_scanned":9,"rows_written":0}}"#,
            hex::encode(&orphan_keys[1])
        ),
    )
    .await;

    run_sweep(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;

    let remaining: Vec<String> = metadata_entries(db)
        .await
        .into_iter()
        .map(|e| e.job_id)
        .filter(|id| id.starts_with("orphan"))
        .collect();
    assert_eq!(
        remaining,
        vec!["orphan-a".to_string(), "orphan-b".to_string()],
        "orphans after the checkpoint are removed, those before it are not revisited"
    );
    for id in &ids {
        assert!(
            metadata_entries(db).await.iter().any(|e| &e.job_id == id),
            "{id} keeps its entries"
        );
    }

    // A full sweep from the start removes the rest.
    shard
        .set_listing_index_version(0)
        .await
        .expect("clear marker");
    run_sweep(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;
    assert_entries_match_jobs(&shard).await;
}

#[silo::test]
async fn a_version_2_checkpoint_is_discarded_by_a_version_3_sweep() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 4).await;
    strip_values(&shard, &ids).await;
    // A version-2 checkpoint past every job: honoured, the jobs pass would
    // skip them all and leave their values empty.
    write_progress(
        shard.db(),
        &format!(
            r#"{{"version":2,"pass":"Jobs","last_key":"{}","rows_scanned":4,"rows_written":4}}"#,
            hex::encode(job_info_key(TENANT, &ids[3]))
        ),
    )
    .await;

    run_sweep(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;
    assert_entries_match_jobs(&shard).await;
    assert!(
        shard
            .db()
            .get(&enqueue_time_index_backfill_progress_key())
            .await
            .expect("get")
            .is_none(),
        "the checkpoint is cleared with completion"
    );
}

#[silo::test]
async fn jobs_deleted_mid_sweep_leave_no_metadata_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let ids = enqueue_many(&shard, "job", 6).await;
    strip_values(&shard, &ids).await;
    // Cancel then delete a job while its entries are still stripped; the
    // sweep must neither resurrect its entries nor leave orphans.
    shard.cancel_job(TENANT, &ids[2]).await.expect("cancel");
    shard.delete_job(TENANT, &ids[2]).await.expect("delete");
    assert!(
        !metadata_entries(shard.db())
            .await
            .iter()
            .any(|e| e.job_id == ids[2])
    );

    run_sweep(&shard, METADATA_ENQUEUE_TIME_INDEX_VERSION).await;
    assert_entries_match_jobs(&shard).await;
    assert!(
        !metadata_entries(shard.db())
            .await
            .iter()
            .any(|e| e.job_id == ids[2])
    );
}

#[silo::test]
async fn disabled_sweep_keeps_the_marker_below_version_3() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    enqueue_many(&shard, "job", 3).await;
    shard
        .set_listing_index_version(STATUS_ENQUEUE_TIME_INDEX_VERSION)
        .await
        .expect("set marker");
    shard.close().await.expect("close");

    let shard = open_shard_at(tmp.path(), None, sweep_config(false, true)).await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(
        shard.listing_index_version(),
        STATUS_ENQUEUE_TIME_INDEX_VERSION
    );
    assert!(!shard.metadata_enqueue_time_index_complete());
}

// ===== Write paths =====

/// The entries `job_id` has, keyed by `(key, value)`.
async fn entries_of(db: &InstrumentedDb, job_id: &str) -> Vec<Entry> {
    metadata_entries(db)
        .await
        .into_iter()
        .filter(|e| e.job_id == job_id)
        .collect()
}

#[silo::test]
async fn enqueue_writes_the_enqueue_time_into_every_metadata_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), None, Default::default()).await;
    let start_at = now_ms() + 5_000;
    shard
        .enqueue(
            TENANT,
            Some("job".to_string()),
            PRIORITY,
            start_at,
            None,
            msgpack_payload(&serde_json::json!({})),
            vec![],
            Some(metadata_for(7)),
            "default",
        )
        .await
        .expect("enqueue");
    let entries = entries_of(shard.db(), "job").await;
    assert_eq!(entries.len(), 2);
    for entry in &entries {
        assert_eq!(entry.enqueue_time_ms, Some(start_at), "{entry:?}");
        assert_eq!(entry.expire_ts, None, "{entry:?}");
    }
}

#[silo::test]
async fn terminal_transition_and_restart_keep_the_enqueue_time_in_the_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), Some(60), Default::default()).await;
    let ids = enqueue_many(&shard, "job", 1).await;
    let id = &ids[0];
    let job = shard
        .get_job(TENANT, id)
        .await
        .expect("get_job")
        .expect("exists");

    shard.cancel_job(TENANT, id).await.expect("cancel");
    let cancelled = entries_of(shard.db(), id).await;
    assert_eq!(cancelled.len(), 2);
    for entry in &cancelled {
        assert_eq!(
            entry.enqueue_time_ms,
            Some(job.enqueue_time_ms()),
            "{entry:?}"
        );
        assert!(entry.expire_ts.is_some(), "{entry:?}");
    }

    shard.restart_job(TENANT, id).await.expect("restart");
    let revived = entries_of(shard.db(), id).await;
    assert_eq!(revived.len(), 2);
    for entry in &revived {
        assert_eq!(
            entry.enqueue_time_ms,
            Some(job.enqueue_time_ms()),
            "{entry:?}"
        );
        assert_eq!(entry.expire_ts, None, "{entry:?}");
    }
}

#[silo::test]
async fn import_writes_the_effective_enqueue_time_into_the_entries() {
    use silo::job_store_shard::import::ImportJobParams;

    let tmp = tempfile::tempdir().unwrap();
    let shard = open_shard_at(tmp.path(), Some(60), Default::default()).await;
    let enqueue_time_ms = 1_700_000_000_000;
    let params = ImportJobParams {
        id: "imported".to_string(),
        priority: PRIORITY,
        enqueue_time_ms,
        start_at_ms: 0,
        retry_policy: None,
        payload: msgpack_payload(&serde_json::json!({})),
        limits: vec![],
        metadata: Some(metadata_for(1)),
        task_group: "default".to_string(),
        attempts: vec![],
    };
    shard
        .import_jobs(TENANT, vec![params])
        .await
        .expect("import");
    let entries = entries_of(shard.db(), "imported").await;
    assert_eq!(entries.len(), 2);
    for entry in &entries {
        assert_eq!(entry.enqueue_time_ms, Some(enqueue_time_ms), "{entry:?}");
    }
}
