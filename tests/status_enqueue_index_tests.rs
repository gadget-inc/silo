//! Lifecycle maintenance of the status-segmented enqueue-time index
//! (`IDX_STATUS_ENQUEUE_TIME`): one entry per live job, in the segment of its
//! stored status, carrying the job's row TTL through every terminal
//! transition, and removed with the job.

mod test_helpers;

use silo::codec::decode_task;
use silo::instrumented_db::InstrumentedDb;
use silo::job::{
    GubernatorAlgorithm, GubernatorRateLimit, JobStatus, JobStatusKind, Limit, ListingSegment,
    RateLimitRetryPolicy,
};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::job_store_shard::import::{ImportJobParams, ImportedAttempt, ImportedAttemptStatus};
use silo::keys::{
    StatusEnqueueTimeIndexValue, end_bound, idx_status_enqueue_time_key,
    idx_status_enqueue_time_tenant_prefix, job_info_key, job_status_key,
    parse_status_enqueue_time_index_key, parse_task_key, tasks_prefix,
};
use silo::retry::RetryPolicy;
use silo::shard_range::ShardRange;
use silo::task::Task;
use test_helpers::*;

const PRIORITY: u8 = 20;

fn status(kind: JobStatusKind, current_attempt: Option<u32>) -> JobStatus {
    JobStatus::new(kind, 1_000, None, current_attempt)
}

/// One decoded index entry.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    segment: ListingSegment,
    priority: u8,
    enqueue_time_ms: i64,
    job_id: String,
    next_attempt_starts_after_ms: Option<i64>,
}

/// A tenant's entries in key order.
async fn entries(db: &InstrumentedDb, tenant: &str) -> Vec<Entry> {
    let start = idx_status_enqueue_time_tenant_prefix(tenant);
    let end = end_bound(&start);
    let mut iter = db.scan::<Vec<u8>, _>(start..end).await.expect("scan");
    let mut out = Vec::new();
    while let Some(kv) = iter.next().await.expect("next") {
        let key = parse_status_enqueue_time_index_key(&kv.key).expect("status-segmented key");
        let value = StatusEnqueueTimeIndexValue::decode(&kv.value).expect("entry value");
        out.push(Entry {
            segment: key.segment,
            priority: key.priority,
            enqueue_time_ms: key.enqueue_time_ms(),
            job_id: key.job_id,
            next_attempt_starts_after_ms: value.next_attempt_starts_after_ms,
        });
    }
    out
}

/// Assert the tenant has exactly one entry, for `job_id`, in `segment`, and
/// return it.
async fn assert_single_entry(
    shard: &JobStoreShard,
    tenant: &str,
    job_id: &str,
    segment: ListingSegment,
) -> Entry {
    let all = entries(shard.db(), tenant).await;
    assert_eq!(all.len(), 1, "expected one entry for {job_id}, got {all:?}");
    let entry = all.into_iter().next().expect("one entry");
    assert_eq!(entry.job_id, job_id);
    assert_eq!(entry.segment, segment, "segment of {job_id}: {entry:?}");
    assert_eq!(entry.priority, PRIORITY);
    entry
}

async fn stored_status(shard: &JobStoreShard, tenant: &str, id: &str) -> JobStatus {
    shard
        .get_job_status(tenant, id)
        .await
        .expect("get_job_status")
        .expect("status exists")
}

fn generous_retry_policy() -> RetryPolicy {
    RetryPolicy {
        retry_count: 5,
        initial_interval_ms: 60_000,
        max_interval_ms: 60_000,
        randomize_interval: false,
        backoff_factor: 1.0,
    }
}

async fn enqueue_with(
    shard: &JobStoreShard,
    tenant: &str,
    id: &str,
    start_at_ms: i64,
    retry_policy: Option<RetryPolicy>,
) {
    shard
        .enqueue(
            tenant,
            Some(id.to_string()),
            PRIORITY,
            start_at_ms,
            retry_policy,
            msgpack_payload(&serde_json::json!({"id": id})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
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
async fn enqueue_writes_one_first_attempt_entry_carrying_the_start_time() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    enqueue_with(&shard, "-", "future", now + 60_000, None).await;
    enqueue_with(&shard, "-", "immediate", 0, None).await;
    enqueue_with(&shard, "-", "past", -5, None).await;
    enqueue_with(&shard, "other", "elsewhere", now, None).await;

    let all = entries(shard.db(), "-").await;
    assert_eq!(
        all.iter()
            .map(|e| (e.segment, e.enqueue_time_ms, e.job_id.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (ListingSegment::ScheduledFirst, now + 60_000, "future"),
            (ListingSegment::ScheduledFirst, 0, "immediate"),
            (ListingSegment::ScheduledFirst, -5, "past"),
        ]
    );
    for entry in &all {
        let status = stored_status(&shard, "-", &entry.job_id).await;
        assert_eq!(
            entry.next_attempt_starts_after_ms, status.next_attempt_starts_after_ms,
            "{entry:?} must carry the status record's next-attempt time"
        );
        assert!(entry.next_attempt_starts_after_ms.is_some());
    }
    assert_eq!(entries(shard.db(), "other").await.len(), 1);
}

#[silo::test]
async fn every_transition_moves_the_single_entry_to_the_new_segment() {
    let (_tmp, shard) = open_temp_shard().await;
    let start = now_ms() - 1_000;
    enqueue_with(&shard, "-", "job", start, Some(generous_retry_policy())).await;
    assert_single_entry(&shard, "-", "job", ListingSegment::ScheduledFirst).await;

    let tasks = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1);
    let running = assert_single_entry(&shard, "-", "job", ListingSegment::Running).await;
    assert_eq!(running.next_attempt_starts_after_ms, None);

    shard
        .report_attempt_outcome(
            tasks[0].attempt().task_id(),
            AttemptOutcome::Error {
                error_code: "E".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report error");
    let retry = assert_single_entry(&shard, "-", "job", ListingSegment::ScheduledRetry).await;
    let retry_status = stored_status(&shard, "-", "job").await;
    assert_eq!(retry_status.current_attempt, Some(2));
    assert_eq!(
        retry.next_attempt_starts_after_ms,
        retry_status.next_attempt_starts_after_ms
    );
    assert!(retry.next_attempt_starts_after_ms.expect("retry time") > now_ms());

    shard.expedite_job("-", "job").await.expect("expedite");
    let expedited = assert_single_entry(&shard, "-", "job", ListingSegment::ScheduledRetry).await;
    let expedited_status = stored_status(&shard, "-", "job").await;
    assert_eq!(
        expedited.next_attempt_starts_after_ms,
        expedited_status.next_attempt_starts_after_ms
    );
    assert!(
        expedited
            .next_attempt_starts_after_ms
            .expect("expedited time")
            <= now_ms()
    );
    assert_ne!(
        expedited.next_attempt_starts_after_ms, retry.next_attempt_starts_after_ms,
        "expedite must update the entry's next-attempt time"
    );

    shard.cancel_job("-", "job").await.expect("cancel");
    let cancelled = assert_single_entry(&shard, "-", "job", ListingSegment::Cancelled).await;
    assert_eq!(cancelled.next_attempt_starts_after_ms, None);

    shard.restart_job("-", "job").await.expect("restart");
    assert_single_entry(&shard, "-", "job", ListingSegment::ScheduledRetry).await;

    let leased = shard
        .lease_task("-", "job", "worker")
        .await
        .expect("lease_task");
    assert_single_entry(&shard, "-", "job", ListingSegment::Running).await;

    shard
        .report_attempt_outcome(
            leased.attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report success");
    let done = assert_single_entry(&shard, "-", "job", ListingSegment::Succeeded).await;
    assert_eq!(done.next_attempt_starts_after_ms, None);
    assert_eq!(done.enqueue_time_ms, start);

    enqueue_with(&shard, "-", "fails", start, None).await;
    run_next_attempt(
        &shard,
        AttemptOutcome::Error {
            error_code: "E".to_string(),
            error: vec![],
        },
    )
    .await;
    let failed: Vec<Entry> = entries(shard.db(), "-")
        .await
        .into_iter()
        .filter(|e| e.job_id == "fails")
        .collect();
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(failed[0].segment, ListingSegment::Failed);
    assert_eq!(failed[0].next_attempt_starts_after_ms, None);

    shard.delete_job("-", "job").await.expect("delete");
    shard.delete_job("-", "fails").await.expect("delete");
    assert!(entries(shard.db(), "-").await.is_empty());
}

/// The row TTL on `key`, panicking if the row is absent.
async fn expire_ts_of(db: &InstrumentedDb, key: &[u8]) -> Option<i64> {
    db.get_key_value(key)
        .await
        .expect("get_key_value")
        .unwrap_or_else(|| panic!("row {key:?} should be present"))
        .expire_ts
}

/// The single entry's row TTL, asserting it equals `JOB_INFO`'s.
async fn single_entry_ttl(shard: &JobStoreShard, tenant: &str, id: &str) -> Option<i64> {
    let status = stored_status(shard, tenant, id).await;
    let entry = assert_single_entry(shard, tenant, id, ListingSegment::from_status(&status)).await;
    let key = idx_status_enqueue_time_key(
        tenant,
        entry.segment,
        entry.priority,
        entry.enqueue_time_ms,
        id,
    );
    let entry_ttl = expire_ts_of(shard.db(), &key).await;
    let info_ttl = expire_ts_of(shard.db(), &job_info_key(tenant, id)).await;
    assert_eq!(
        entry_ttl, info_ttl,
        "entry TTL differs from JOB_INFO for {id}"
    );
    entry_ttl
}

#[silo::test]
async fn terminal_entry_carries_the_job_ttl_and_restart_clears_it() {
    let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;
    enqueue_with(&shard, "-", "job", now_ms(), None).await;
    assert_eq!(single_entry_ttl(&shard, "-", "job").await, None);

    run_next_attempt(
        &shard,
        AttemptOutcome::Error {
            error_code: "E".to_string(),
            error: vec![],
        },
    )
    .await;
    assert_single_entry(&shard, "-", "job", ListingSegment::Failed).await;
    assert!(
        single_entry_ttl(&shard, "-", "job").await.is_some(),
        "a failed job's entry must carry the retention TTL"
    );

    shard.restart_job("-", "job").await.expect("restart");
    assert_single_entry(&shard, "-", "job", ListingSegment::ScheduledRetry).await;
    assert_eq!(single_entry_ttl(&shard, "-", "job").await, None);
}

fn import_base(id: &str) -> ImportJobParams {
    ImportJobParams {
        id: id.to_string(),
        priority: PRIORITY,
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

#[silo::test]
async fn import_writes_the_entry_and_reimport_moves_it() {
    let (_tmp, shard) = open_temp_shard().await;
    let results = shard
        .import_jobs("-", vec![import_base("job")])
        .await
        .expect("import");
    assert!(results[0].success, "{results:?}");
    let imported = assert_single_entry(&shard, "-", "job", ListingSegment::ScheduledFirst).await;
    assert_eq!(imported.enqueue_time_ms, 1_700_000_000_000);
    assert_eq!(
        imported.next_attempt_starts_after_ms,
        stored_status(&shard, "-", "job")
            .await
            .next_attempt_starts_after_ms
    );

    let mut terminal = import_base("job");
    terminal.attempts = vec![ImportedAttempt {
        status: ImportedAttemptStatus::Succeeded { result: vec![1] },
        started_at_ms: now_ms() - 2_000,
        finished_at_ms: now_ms() - 1_000,
    }];
    let results = shard
        .import_jobs("-", vec![terminal])
        .await
        .expect("reimport");
    assert_eq!(results[0].status, JobStatusKind::Succeeded, "{results:?}");
    let reimported = assert_single_entry(&shard, "-", "job", ListingSegment::Succeeded).await;
    assert_eq!(reimported.enqueue_time_ms, 1_700_000_000_000);
    assert_eq!(reimported.next_attempt_starts_after_ms, None);
}

#[silo::test]
async fn delete_without_a_status_record_removes_every_candidate_entry() {
    let (_tmp, shard) = open_temp_shard().await;
    enqueue_with(&shard, "-", "job", now_ms(), None).await;
    run_next_attempt(&shard, AttemptOutcome::Success { result: vec![] }).await;
    assert_single_entry(&shard, "-", "job", ListingSegment::Succeeded).await;

    shard
        .db()
        .delete(job_status_key("-", "job"))
        .await
        .expect("drop status record");
    assert!(
        shard
            .get_job_status("-", "job")
            .await
            .expect("status")
            .is_none(),
        "status record must be gone before delete_job"
    );

    shard.delete_job("-", "job").await.expect("delete_job");

    assert!(entries(shard.db(), "-").await.is_empty());
    assert!(shard.get_job("-", "job").await.expect("get_job").is_none());
}

#[silo::test]
async fn post_split_cleanup_removes_entries_outside_range() {
    let (_tmp, shard) = open_temp_shard().await;
    // Tenant hashes: zzz (6d85...) is inside [, 8000...), aaa (ae01...) is outside.
    enqueue_with(&shard, "aaa", "a1", now_ms(), None).await;
    enqueue_with(&shard, "aaa", "a2", now_ms(), None).await;
    enqueue_with(&shard, "zzz", "z1", now_ms(), None).await;
    shard.db().flush().await.expect("flush");

    let left_range = ShardRange::new("", "8000000000000000");
    let result = shard
        .after_split_cleanup_defunct_data(&left_range, 10)
        .await
        .expect("cleanup");
    assert!(result.complete);

    assert!(entries(shard.db(), "aaa").await.is_empty());
    assert_eq!(entries(shard.db(), "zzz").await.len(), 1);
}

/// The start time of the first `CheckRateLimit` task on the shard.
async fn find_check_rate_limit_start_ms(db: &InstrumentedDb) -> Option<u64> {
    let start = tasks_prefix();
    let end = end_bound(&start);
    let mut iter = db.scan::<Vec<u8>, _>(start..end).await.expect("scan tasks");
    while let Some(kv) = iter.next().await.expect("iter") {
        let Some(parsed) = parse_task_key(&kv.key) else {
            continue;
        };
        if matches!(decode_task(&kv.value), Ok(Task::CheckRateLimit { .. })) {
            return Some(parsed.start_time_ms);
        }
    }
    None
}

/// A rate limiter that always reports over-limit, so every `CheckRateLimit`
/// task is rescheduled with backoff.
struct AlwaysOverLimit;

#[async_trait::async_trait]
impl silo::gubernator::RateLimitClient for AlwaysOverLimit {
    async fn check_rate_limit(
        &self,
        _name: &str,
        _unique_key: &str,
        _hits: i64,
        limit: i64,
        duration_ms: i64,
        _algorithm: silo::pb::gubernator::Algorithm,
        _behavior: i32,
    ) -> Result<silo::gubernator::RateLimitResult, silo::gubernator::GubernatorError> {
        Ok(silo::gubernator::RateLimitResult {
            under_limit: false,
            limit,
            remaining: 0,
            reset_time_ms: now_ms() + duration_ms,
            error: None,
        })
    }

    async fn health_check(&self) -> Result<(String, i32), silo::gubernator::GubernatorError> {
        Ok(("ok".to_string(), 1))
    }
}

/// An over-limit rate check reschedules the attempt's `CheckRateLimit` task
/// after the backoff and retargets the Scheduled status to that start time.
/// The entry's value must follow.
#[silo::test]
async fn scheduled_task_retarget_updates_the_entry_value() {
    let (_tmp, shard) =
        open_temp_shard_with_rate_limiter(std::sync::Arc::new(AlwaysOverLimit)).await;
    let now = now_ms();
    let rate_limit = GubernatorRateLimit {
        name: "api".to_string(),
        unique_key: "retarget-key".to_string(),
        limit: 1,
        duration_ms: 60_000,
        hits: 1,
        algorithm: GubernatorAlgorithm::TokenBucket,
        behavior: 0,
        retry_policy: RateLimitRetryPolicy {
            initial_backoff_ms: 5_000,
            max_backoff_ms: 5_000,
            backoff_multiplier: 1.0,
            max_retries: 10,
        },
    };
    shard
        .enqueue(
            "-",
            Some("limited".to_string()),
            PRIORITY,
            now,
            None,
            msgpack_payload(&serde_json::json!({})),
            vec![Limit::RateLimit(rate_limit)],
            None,
            "default",
        )
        .await
        .expect("enqueue limited");
    let before = assert_single_entry(&shard, "-", "limited", ListingSegment::ScheduledFirst).await;
    assert_eq!(before.next_attempt_starts_after_ms, Some(now));

    // The dequeue scan processes the CheckRateLimit task, finds it over
    // limit, and parks a retry after the backoff.
    let tasks = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert!(
        tasks.is_empty(),
        "an over-limit job must not lease: {tasks:?}"
    );

    let retry_start_ms = poll_until(
        || async {
            find_check_rate_limit_start_ms(shard.db())
                .await
                .filter(|&start| start as i64 != now)
        },
        |found| found.is_some(),
        5_000,
    )
    .await
    .expect("a rescheduled CheckRateLimit task after the backoff");
    assert!(retry_start_ms as i64 >= now + 5_000);

    let after = stored_status(&shard, "-", "limited").await;
    assert_eq!(after.kind, JobStatusKind::Scheduled);
    assert_eq!(
        after.next_attempt_starts_after_ms,
        Some(retry_start_ms as i64),
        "status must be retargeted to the retry's start time"
    );

    let entry = assert_single_entry(&shard, "-", "limited", ListingSegment::ScheduledFirst).await;
    assert_eq!(
        entry.next_attempt_starts_after_ms,
        Some(retry_start_ms as i64)
    );
}

#[test]
fn segment_derives_from_status_kind_and_attempt() {
    let cases = [
        (
            JobStatusKind::Scheduled,
            None,
            ListingSegment::ScheduledFirst,
        ),
        (
            JobStatusKind::Scheduled,
            Some(1),
            ListingSegment::ScheduledFirst,
        ),
        (
            JobStatusKind::Scheduled,
            Some(2),
            ListingSegment::ScheduledRetry,
        ),
        (
            JobStatusKind::Scheduled,
            Some(7),
            ListingSegment::ScheduledRetry,
        ),
        (JobStatusKind::Running, None, ListingSegment::Running),
        (JobStatusKind::Running, Some(3), ListingSegment::Running),
        (JobStatusKind::Succeeded, None, ListingSegment::Succeeded),
        (JobStatusKind::Failed, None, ListingSegment::Failed),
        (JobStatusKind::Cancelled, None, ListingSegment::Cancelled),
        (JobStatusKind::Cancelled, Some(2), ListingSegment::Cancelled),
    ];
    for (kind, attempt, want) in cases {
        assert_eq!(
            ListingSegment::from_status(&status(kind, attempt)),
            want,
            "{kind:?} with current_attempt {attempt:?}"
        );
    }
}

#[test]
fn segment_discriminants_roundtrip_in_declaration_order() {
    for (i, segment) in ListingSegment::ALL.into_iter().enumerate() {
        assert_eq!(segment.as_u8() as usize, i);
        assert_eq!(ListingSegment::from_u8(segment.as_u8()), Some(segment));
    }
    assert_eq!(ListingSegment::from_u8(6), None);
}

#[silo::test]
async fn succeeded_entry_carries_the_job_ttl() {
    let (_tmp, shard) = open_temp_shard_with_terminal_expire_s(60).await;
    enqueue_with(&shard, "-", "job", now_ms(), None).await;
    run_next_attempt(&shard, AttemptOutcome::Success { result: vec![] }).await;

    assert_single_entry(&shard, "-", "job", ListingSegment::Succeeded).await;
    assert!(
        single_entry_ttl(&shard, "-", "job").await.is_some(),
        "a succeeded job's entry must carry the retention TTL"
    );
}
