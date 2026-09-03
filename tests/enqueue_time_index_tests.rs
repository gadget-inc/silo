//! Lifecycle maintenance of the enqueue-time index (`IDX_ENQUEUE_TIME`):
//! written once when a job is created, deleted with the job, and carrying the
//! same row TTL as the job's other rows through every terminal transition.

mod test_helpers;

use silo::instrumented_db::InstrumentedDb;
use silo::job::JobStatusKind;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::job_store_shard::import::{ImportJobParams, ImportedAttempt, ImportedAttemptStatus};
use silo::keys::{
    attempt_key, end_bound, idx_enqueue_time_key, idx_enqueue_time_tenant_prefix, idx_metadata_key,
    job_cancelled_key, job_info_key, job_status_key, parse_enqueue_time_index_key,
};
use silo::retry::RetryPolicy;
use silo::shard_range::ShardRange;
use test_helpers::*;

/// The row TTL on `key`, panicking if the row is absent.
async fn expire_ts_of(db: &InstrumentedDb, key: &[u8]) -> Option<i64> {
    db.get_key_value(key)
        .await
        .expect("get_key_value")
        .unwrap_or_else(|| panic!("row {key:?} should be present"))
        .expire_ts
}

/// The job's index entry key, derived from its stored `JOB_INFO`.
async fn entry_key(shard: &JobStoreShard, tenant: &str, id: &str) -> Vec<u8> {
    let job = shard
        .get_job(tenant, id)
        .await
        .expect("get_job")
        .expect("job exists");
    idx_enqueue_time_key(tenant, job.enqueue_time_ms(), id)
}

/// Assert the entry carries exactly the TTL that `JOB_INFO` and `JOB_STATUS`
/// carry, and return that TTL.
async fn assert_entry_ttl_matches_job(
    shard: &JobStoreShard,
    tenant: &str,
    id: &str,
) -> Option<i64> {
    let db = shard.db();
    let info_ttl = expire_ts_of(db, &job_info_key(tenant, id)).await;
    let status_ttl = expire_ts_of(db, &job_status_key(tenant, id)).await;
    let entry_ttl = expire_ts_of(db, &entry_key(shard, tenant, id).await).await;
    assert_eq!(info_ttl, status_ttl, "JOB_INFO and JOB_STATUS TTLs differ");
    assert_eq!(entry_ttl, info_ttl, "index entry TTL differs from JOB_INFO");
    entry_ttl
}

/// A tenant's index entries in key order: `(enqueue_time_ms, job_id)`.
async fn index_entries(db: &InstrumentedDb, tenant: &str) -> Vec<(i64, String)> {
    let start = idx_enqueue_time_tenant_prefix(tenant);
    let end = end_bound(&start);
    let mut iter = db.scan::<Vec<u8>, _>(start..end).await.expect("scan");
    let mut entries = Vec::new();
    while let Some(kv) = iter.next().await.expect("next") {
        let parsed = parse_enqueue_time_index_key(&kv.key).expect("enqueue-time index key");
        entries.push((parsed.enqueue_time_ms(), parsed.job_id));
    }
    entries
}

async fn enqueue_at(shard: &JobStoreShard, tenant: &str, id: &str, start_at_ms: i64) {
    let payload = msgpack_payload(&serde_json::json!({"id": id}));
    shard
        .enqueue(
            tenant,
            Some(id.to_string()),
            10u8,
            start_at_ms,
            None,
            payload,
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
}

fn import_base(id: &str) -> ImportJobParams {
    ImportJobParams {
        id: id.to_string(),
        priority: 50,
        enqueue_time_ms: 1_700_000_000_000,
        start_at_ms: 0,
        retry_policy: None,
        payload: msgpack_payload(&serde_json::json!({"imported": true})),
        limits: vec![],
        metadata: None,
        task_group: "default".to_string(),
        attempts: vec![],
    }
}

fn import_succeeded_attempt(finished_at_ms: i64) -> ImportedAttempt {
    ImportedAttempt {
        status: ImportedAttemptStatus::Succeeded { result: vec![1] },
        started_at_ms: finished_at_ms - 1000,
        finished_at_ms,
    }
}

/// Dequeue the single pending task on `default` and report `outcome` for it.
async fn run_next_attempt(shard: &JobStoreShard, outcome: AttemptOutcome) {
    let tasks = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1, "expected exactly one task to dequeue");
    let task_id = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(&task_id, outcome)
        .await
        .expect("report_attempt_outcome");
}

#[silo::test]
async fn enqueue_writes_one_entry_per_job_ordered_newest_first() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_at(&shard, "-", "older", now - 10_000).await;
    enqueue_at(&shard, "-", "newer", now + 10_000).await;
    enqueue_at(&shard, "-", "immediate", 0).await;
    enqueue_at(&shard, "other", "elsewhere", now).await;

    assert_eq!(
        index_entries(shard.db(), "-").await,
        vec![
            (now + 10_000, "newer".to_string()),
            (now - 10_000, "older".to_string()),
            (0, "immediate".to_string()),
        ]
    );
    assert_eq!(
        index_entries(shard.db(), "other").await,
        vec![(now, "elsewhere".to_string())]
    );
}

#[silo::test]
async fn first_import_writes_entry_keyed_by_enqueue_time() {
    let (_tmp, shard) = open_temp_shard().await;
    let scheduled = import_base("imported-scheduled");
    let mut terminal = import_base("imported-terminal");
    terminal.enqueue_time_ms = 1_600_000_000_000;
    terminal.attempts = vec![import_succeeded_attempt(now_ms() - 1_000)];

    let results = shard
        .import_jobs("-", vec![scheduled, terminal])
        .await
        .expect("import_jobs");
    assert!(results.iter().all(|r| r.success), "{results:?}");

    assert_eq!(
        index_entries(shard.db(), "-").await,
        vec![
            (1_700_000_000_000, "imported-scheduled".to_string()),
            (1_600_000_000_000, "imported-terminal".to_string()),
        ]
    );
}

#[silo::test]
async fn delete_job_removes_entry() {
    let (_tmp, shard) = open_temp_shard().await;
    enqueue_at(&shard, "-", "doomed", now_ms()).await;
    enqueue_at(&shard, "-", "survivor", now_ms() - 1).await;
    run_next_attempt(&shard, AttemptOutcome::Success { result: vec![] }).await;
    assert_eq!(
        shard
            .get_job_status("-", "doomed")
            .await
            .expect("status")
            .expect("exists")
            .kind,
        JobStatusKind::Succeeded
    );

    shard.delete_job("-", "doomed").await.expect("delete_job");

    let ids: Vec<String> = index_entries(shard.db(), "-")
        .await
        .into_iter()
        .map(|(_, id)| id)
        .collect();
    assert_eq!(ids, vec!["survivor".to_string()]);
}

fn import_failed_attempt(finished_at_ms: i64) -> ImportedAttempt {
    ImportedAttempt {
        status: ImportedAttemptStatus::Failed {
            error_code: "ERR".to_string(),
            error: vec![4],
        },
        started_at_ms: finished_at_ms - 1000,
        finished_at_ms,
    }
}

fn generous_retry_policy() -> RetryPolicy {
    RetryPolicy {
        retry_count: 5,
        initial_interval_ms: 100,
        max_interval_ms: 10_000,
        randomize_interval: false,
        backoff_factor: 2.0,
    }
}

#[silo::test]
async fn terminal_attempt_outcome_tags_entry_with_job_ttl() {
    let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;
    enqueue_at(&shard, "-", "job", now_ms()).await;
    assert_eq!(assert_entry_ttl_matches_job(&shard, "-", "job").await, None);

    run_next_attempt(&shard, AttemptOutcome::Success { result: vec![] }).await;

    assert!(
        assert_entry_ttl_matches_job(&shard, "-", "job")
            .await
            .is_some(),
        "terminal job rows should carry a TTL"
    );
}

#[silo::test]
async fn cancel_tags_entry_with_job_ttl() {
    let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;
    enqueue_at(&shard, "-", "job", now_ms() + 60_000).await;

    shard.cancel_job("-", "job").await.expect("cancel_job");

    assert!(
        assert_entry_ttl_matches_job(&shard, "-", "job")
            .await
            .is_some(),
        "cancelled job rows should carry a TTL"
    );
}

#[silo::test]
async fn terminal_reimport_tags_entry_with_job_ttl() {
    let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;
    shard
        .import_jobs("-", vec![import_base("job")])
        .await
        .expect("initial import");
    assert_eq!(assert_entry_ttl_matches_job(&shard, "-", "job").await, None);

    let mut terminal = import_base("job");
    terminal.attempts = vec![import_succeeded_attempt(now_ms() - 1_000)];
    let results = shard
        .import_jobs("-", vec![terminal])
        .await
        .expect("reimport");
    assert_eq!(results[0].status, JobStatusKind::Succeeded, "{results:?}");

    assert!(
        assert_entry_ttl_matches_job(&shard, "-", "job")
            .await
            .is_some(),
        "terminal reimport rows should carry a TTL"
    );
}

#[silo::test]
async fn reimport_landing_scheduled_clears_entry_ttl() {
    let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;
    let failed_at_ms = now_ms() - 5_000;
    let mut failed = import_base("job");
    failed.attempts = vec![import_failed_attempt(failed_at_ms)];
    let results = shard
        .import_jobs("-", vec![failed])
        .await
        .expect("failed import");
    assert_eq!(results[0].status, JobStatusKind::Failed, "{results:?}");
    assert!(
        assert_entry_ttl_matches_job(&shard, "-", "job")
            .await
            .is_some(),
        "failed job rows should carry a TTL"
    );

    let mut retried = import_base("job");
    retried.retry_policy = Some(generous_retry_policy());
    retried.attempts = vec![
        import_failed_attempt(failed_at_ms),
        import_failed_attempt(failed_at_ms + 1_000),
    ];
    let results = shard
        .import_jobs("-", vec![retried])
        .await
        .expect("reimport");
    assert_eq!(results[0].status, JobStatusKind::Scheduled, "{results:?}");

    assert_eq!(
        assert_entry_ttl_matches_job(&shard, "-", "job").await,
        None,
        "a job reimported back to Scheduled must carry no TTL"
    );
}

#[silo::test]
async fn restart_revives_every_job_row_without_ttl() {
    let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;
    let payload = msgpack_payload(&serde_json::json!({}));
    shard
        .enqueue(
            "-",
            Some("job".to_string()),
            10u8,
            now_ms(),
            None,
            payload,
            vec![],
            Some(vec![("env".to_string(), "prod".to_string())]),
            "default",
        )
        .await
        .expect("enqueue");
    run_next_attempt(
        &shard,
        AttemptOutcome::Error {
            error_code: "E".to_string(),
            error: vec![],
        },
    )
    .await;
    let db = shard.db();
    let tagged_rows = [
        job_info_key("-", "job"),
        idx_metadata_key("-", "env", "prod", "job"),
        attempt_key("-", "job", 1),
        entry_key(&shard, "-", "job").await,
    ];
    for key in &tagged_rows {
        assert!(
            expire_ts_of(db, key).await.is_some(),
            "failed job row {key:?} should carry a TTL before restart"
        );
    }

    shard.restart_job("-", "job").await.expect("restart_job");

    for key in &tagged_rows {
        assert_eq!(
            expire_ts_of(db, key).await,
            None,
            "restarted job row {key:?} must carry no TTL"
        );
    }
    assert_eq!(assert_entry_ttl_matches_job(&shard, "-", "job").await, None);
}

#[silo::test]
async fn restart_of_cancelled_job_removes_cancellation_row() {
    let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;
    enqueue_at(&shard, "-", "job", now_ms() + 60_000).await;
    shard.cancel_job("-", "job").await.expect("cancel_job");
    assert!(
        shard
            .is_job_cancelled("-", "job")
            .await
            .expect("is_job_cancelled"),
        "job should be cancelled before restart"
    );

    shard.restart_job("-", "job").await.expect("restart_job");

    assert!(
        shard
            .db()
            .get(&job_cancelled_key("-", "job"))
            .await
            .expect("get")
            .is_none(),
        "cancellation row must be absent after restart"
    );
    assert_eq!(assert_entry_ttl_matches_job(&shard, "-", "job").await, None);
}

#[silo::test]
async fn post_split_cleanup_removes_entries_outside_range() {
    let (_tmp, shard) = open_temp_shard().await;
    // Tenant hashes: zzz (6d85...) is inside [, 8000...), aaa (ae01...) is outside.
    enqueue_at(&shard, "aaa", "a1", now_ms()).await;
    enqueue_at(&shard, "aaa", "a2", now_ms()).await;
    enqueue_at(&shard, "zzz", "z1", now_ms()).await;
    shard.db().flush().await.expect("flush");

    let left_range = ShardRange::new("", "8000000000000000");
    let result = shard
        .after_split_cleanup_defunct_data(&left_range, 10)
        .await
        .expect("cleanup");
    assert!(result.complete);

    assert!(index_entries(shard.db(), "aaa").await.is_empty());
    assert_eq!(index_entries(shard.db(), "zzz").await.len(), 1);
}

#[silo::test]
async fn reimport_leaves_entry_key_unchanged() {
    let (_tmp, shard) = open_temp_shard().await;
    shard
        .import_jobs("-", vec![import_base("job")])
        .await
        .expect("initial import");

    let mut reimport = import_base("job");
    reimport.enqueue_time_ms = 1_800_000_000_000;
    reimport.attempts = vec![import_succeeded_attempt(now_ms() - 1_000)];
    let results = shard
        .import_jobs("-", vec![reimport])
        .await
        .expect("reimport");
    assert!(results[0].success, "{results:?}");

    assert_eq!(
        index_entries(shard.db(), "-").await,
        vec![(1_700_000_000_000, "job".to_string())]
    );
}
