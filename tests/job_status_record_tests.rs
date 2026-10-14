//! The stored `JOB_STATUS` record carries the job's enqueue time and priority
//! through every write: stamped at enqueue and import, carried across each
//! transition, and filled in from `JOB_INFO` when the previous record lacks
//! them.

mod test_helpers;

use silo::codec::encode_job_status;
use silo::job::{JobStatus, JobStatusKind};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::job_store_shard::import::{ImportJobParams, ImportedAttempt, ImportedAttemptStatus};
use silo::keys::job_status_key;
use silo::retry::RetryPolicy;
use test_helpers::*;

const PRIORITY: u8 = 37;

async fn enqueue_at(shard: &JobStoreShard, id: &str, start_at_ms: i64) {
    shard
        .enqueue(
            "-",
            Some(id.to_string()),
            PRIORITY,
            start_at_ms,
            None,
            msgpack_payload(&serde_json::json!({"id": id})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
}

async fn stored_status(shard: &JobStoreShard, id: &str) -> JobStatus {
    shard
        .get_job_status("-", id)
        .await
        .expect("get_job_status")
        .expect("status exists")
}

/// Assert the stored status has `kind` and carries the job's enqueue time
/// and priority.
async fn assert_status_carries_job_fields(
    shard: &JobStoreShard,
    id: &str,
    kind: JobStatusKind,
    enqueue_time_ms: i64,
    priority: u8,
) {
    let status = stored_status(shard, id).await;
    assert_eq!(status.kind, kind, "{id}: status kind");
    assert_eq!(
        status.enqueue_time_ms,
        Some(enqueue_time_ms),
        "{id}: enqueue_time_ms after {kind:?}"
    );
    assert_eq!(
        status.priority,
        Some(priority),
        "{id}: priority after {kind:?}"
    );
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

#[silo::test]
async fn every_transition_keeps_enqueue_time_and_priority_on_the_status_record() {
    let (_tmp, shard) = open_temp_shard().await;
    let start = now_ms() - 1_000;
    shard
        .enqueue(
            "-",
            Some("job".to_string()),
            PRIORITY,
            start,
            Some(RetryPolicy {
                retry_count: 5,
                initial_interval_ms: 60_000,
                max_interval_ms: 60_000,
                randomize_interval: false,
                backoff_factor: 1.0,
            }),
            msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
    let check = |kind| assert_status_carries_job_fields(&shard, "job", kind, start, PRIORITY);

    let tasks = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1);
    check(JobStatusKind::Running).await;

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
    check(JobStatusKind::Scheduled).await;
    assert_eq!(stored_status(&shard, "job").await.current_attempt, Some(2));

    shard.expedite_job("-", "job").await.expect("expedite");
    check(JobStatusKind::Scheduled).await;

    shard.cancel_job("-", "job").await.expect("cancel");
    check(JobStatusKind::Cancelled).await;

    shard.restart_job("-", "job").await.expect("restart");
    check(JobStatusKind::Scheduled).await;

    let leased = shard
        .lease_task("-", "job", "worker")
        .await
        .expect("lease_task");
    check(JobStatusKind::Running).await;

    shard
        .report_attempt_outcome(
            leased.attempt().task_id(),
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report success");
    check(JobStatusKind::Succeeded).await;
}

#[silo::test]
async fn import_and_reimport_stamp_the_job_fields_on_the_status_record() {
    let (_tmp, shard) = open_temp_shard().await;
    shard
        .import_jobs("-", vec![import_base("job")])
        .await
        .expect("import");
    assert_status_carries_job_fields(
        &shard,
        "job",
        JobStatusKind::Scheduled,
        1_700_000_000_000,
        50,
    )
    .await;

    // Reimport never changes the stored enqueue time or priority.
    let mut terminal = import_base("job");
    terminal.enqueue_time_ms = 1_800_000_000_000;
    terminal.priority = 10;
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
    assert_status_carries_job_fields(
        &shard,
        "job",
        JobStatusKind::Succeeded,
        1_700_000_000_000,
        50,
    )
    .await;
}

#[silo::test]
async fn status_record_without_job_fields_gains_them_on_its_next_transition() {
    let (_tmp, shard) = open_temp_shard().await;
    let start = now_ms() + 60_000;
    enqueue_at(&shard, "job", start).await;

    // Replace the stored record with one shaped like a record written
    // without the job fields.
    let stored = stored_status(&shard, "job").await;
    let without_job_fields = JobStatus::new(
        stored.kind,
        stored.changed_at_ms,
        stored.next_attempt_starts_after_ms,
        stored.current_attempt,
    );
    shard
        .db()
        .put(
            job_status_key("-", "job"),
            encode_job_status(&without_job_fields),
        )
        .await
        .expect("put status without job fields");
    let reread = stored_status(&shard, "job").await;
    assert_eq!((reread.enqueue_time_ms, reread.priority), (None, None));

    shard.cancel_job("-", "job").await.expect("cancel");

    assert_status_carries_job_fields(&shard, "job", JobStatusKind::Cancelled, start, PRIORITY)
        .await;
}

#[silo::test]
async fn enqueue_stamps_enqueue_time_and_priority_on_the_status_record() {
    let (_tmp, shard) = open_temp_shard().await;
    let start = now_ms() + 60_000;
    enqueue_at(&shard, "job", start).await;

    assert_status_carries_job_fields(&shard, "job", JobStatusKind::Scheduled, start, PRIORITY)
        .await;
}
