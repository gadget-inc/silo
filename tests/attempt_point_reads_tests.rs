//! Tests for how a job's attempt rows are read: by key for attempts `1..=n`
//! when the job's status record gives `n`, and by a scan of the job's attempt
//! prefix only when it does not or `n` is above the point-read limit.

mod test_helpers;

use std::time::Duration;

use silo::codec::{encode_attempt, encode_job_status};
use silo::job::{JobStatus, JobStatusKind};
use silo::job_attempt::{AttemptOutcome, AttemptStatus, JobAttempt};
use silo::job_store_shard::import::{ImportJobParams, ImportedAttempt, ImportedAttemptStatus};
use silo::job_store_shard::{ATTEMPT_POINT_READ_LIMIT, JobStoreShard};
use silo::keys::{attempt_key, job_status_key};
use silo::metrics::Metrics;
use silo::retry::RetryPolicy;
use slatedb::WriteBatch;
use test_helpers::*;

const TENANT: &str = "-";

fn retry_policy(retry_count: u32) -> RetryPolicy {
    RetryPolicy {
        retry_count,
        initial_interval_ms: 1,
        max_interval_ms: 1,
        randomize_interval: false,
        backoff_factor: 1.0,
    }
}

fn failed_attempt(finished_at_ms: i64) -> ImportedAttempt {
    ImportedAttempt {
        status: ImportedAttemptStatus::Failed {
            error_code: "ERR".to_string(),
            error: vec![1, 2, 3],
        },
        started_at_ms: finished_at_ms - 1_000,
        finished_at_ms,
    }
}

fn succeeded_attempt(finished_at_ms: i64) -> ImportedAttempt {
    ImportedAttempt {
        status: ImportedAttemptStatus::Succeeded {
            result: vec![4, 5, 6],
        },
        started_at_ms: finished_at_ms - 1_000,
        finished_at_ms,
    }
}

/// Import a job that has already failed `failed` attempts and is due to run
/// attempt `failed + 1` now.
async fn import_with_failed_attempts(shard: &JobStoreShard, job_id: &str, failed: usize) {
    let params = ImportJobParams {
        id: job_id.to_string(),
        priority: 50,
        enqueue_time_ms: 1_700_000_000_000,
        start_at_ms: 0,
        retry_policy: Some(retry_policy(failed as u32 + 5)),
        payload: msgpack_payload(&serde_json::json!({"imported": true})),
        limits: vec![],
        metadata: None,
        task_group: "default".to_string(),
        attempts: (0..failed)
            .map(|i| failed_attempt(1_700_000_001_000 + i as i64))
            .collect(),
    };
    let results = shard
        .import_jobs(TENANT, vec![params])
        .await
        .expect("import");
    assert!(results[0].success, "import failed: {:?}", results[0].error);
}

/// Dequeue the one pending task in the shard and return its task id.
async fn dequeue_one(shard: &JobStoreShard) -> String {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let tasks = shard
            .dequeue("worker", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        if let Some(task) = tasks.first() {
            return task.attempt().task_id().to_string();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no task became available"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn status(shard: &JobStoreShard, job_id: &str) -> JobStatus {
    shard
        .get_job_status(TENANT, job_id)
        .await
        .expect("get status")
        .expect("status present")
}

fn attempt_numbers(attempts: &[silo::job_attempt::JobAttemptView]) -> Vec<u32> {
    attempts.iter().map(|a| a.attempt_number()).collect()
}

async fn attempt_expire_ts(shard: &JobStoreShard, job_id: &str, attempt: u32) -> Option<i64> {
    shard
        .db()
        .get_key_value(&attempt_key(TENANT, job_id, attempt))
        .await
        .expect("get attempt row")
        .unwrap_or_else(|| panic!("attempt {attempt} row present"))
        .expire_ts
}

async fn attempt_row_exists(shard: &JobStoreShard, job_id: &str, attempt: u32) -> bool {
    shard
        .db()
        .get(&attempt_key(TENANT, job_id, attempt))
        .await
        .expect("get attempt row")
        .is_some()
}

async fn delete_attempt_row(shard: &JobStoreShard, job_id: &str, attempt: u32) {
    let mut batch = WriteBatch::new();
    batch.delete(attempt_key(TENANT, job_id, attempt));
    shard.db().write(batch).await.expect("delete attempt row");
}

fn slatedb_scan_requests(shard: &JobStoreShard, metrics: &Metrics) -> f64 {
    metrics.update_slatedb_stats("test", shard.slatedb_metrics_recorder());
    metric_value_or_zero(
        &gather_metrics_text(metrics),
        &["silo_slatedb_scan_requests_total", "shard=\"test\""],
    )
}

/// Wait until SlateDB's scan request count for the shard holds still across
/// a short pause, so a scan started by an earlier write has finished.
async fn wait_for_scans_to_settle(shard: &JobStoreShard, metrics: &Metrics) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let before = slatedb_scan_requests(shard, metrics);
        tokio::time::sleep(Duration::from_millis(25)).await;
        if slatedb_scan_requests(shard, metrics) == before {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "background scans never paused"
        );
    }
}

/// How many times the shard read a job's attempt rows from `source`
/// (`keys` or `scan`).
fn attempt_reads(metrics: &Metrics, source: &str) -> f64 {
    metric_value_or_zero(
        &gather_metrics_text(metrics),
        &[
            "silo_job_attempt_reads_total",
            "shard=\"test\"",
            &format!("source=\"{source}\""),
        ],
    )
}

/// The fewest SlateDB scans opened across one call of `read`, sampled five
/// times. The count covers the whole shard, so a background scan can add to
/// a sample; a read that scans shows in all of them.
async fn fewest_scans<F, Fut>(shard: &JobStoreShard, metrics: &Metrics, mut read: F) -> f64
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut fewest = f64::MAX;
    for _ in 0..5 {
        wait_for_scans_to_settle(shard, metrics).await;
        let before = slatedb_scan_requests(shard, metrics);
        read().await;
        fewest = fewest.min(slatedb_scan_requests(shard, metrics) - before);
    }
    fewest
}

// ---------------------------------------------------------------------------
// The attempt number a status record carries
// ---------------------------------------------------------------------------

#[silo::test]
fn a_scheduled_status_bounds_attempt_rows_below_its_pending_attempt() {
    assert_eq!(JobStatus::scheduled(0, 0, 1).highest_attempt(), Some(0));
    assert_eq!(JobStatus::scheduled(0, 0, 4).highest_attempt(), Some(3));
}

#[silo::test]
fn a_status_with_no_attempt_number_does_not_bound_attempt_rows() {
    assert_eq!(JobStatus::running(0).highest_attempt(), None);
    assert_eq!(JobStatus::succeeded(0).highest_attempt(), None);
    assert_eq!(JobStatus::failed(0).highest_attempt(), None);
    assert_eq!(JobStatus::cancelled(0).highest_attempt(), None);
}

#[silo::test]
fn a_recorded_latest_attempt_bounds_attempt_rows() {
    assert_eq!(
        JobStatus::running(0)
            .with_latest_attempt(7)
            .highest_attempt(),
        Some(7)
    );
    assert_eq!(
        JobStatus::succeeded(0)
            .with_latest_attempt(1)
            .highest_attempt(),
        Some(1)
    );
}

#[silo::test]
fn the_latest_attempt_survives_an_encode_and_decode() {
    let encoded = encode_job_status(&JobStatus::failed(5).with_latest_attempt(9));
    let decoded = silo::codec::decode_job_status_owned(&encoded).expect("decode");
    assert_eq!(decoded.latest_attempt, Some(9));

    let encoded = encode_job_status(&JobStatus::failed(5));
    let decoded = silo::codec::decode_job_status_owned(&encoded).expect("decode");
    assert_eq!(decoded.latest_attempt, None);
}

#[silo::test]
async fn a_running_job_records_the_attempt_it_is_running() {
    let (_tmp, shard) = open_temp_shard().await;
    import_with_failed_attempts(&shard, "job", 2).await;

    assert_eq!(status(&shard, "job").await.highest_attempt(), Some(2));
    dequeue_one(&shard).await;

    let running = status(&shard, "job").await;
    assert_eq!(running.kind, JobStatusKind::Running);
    assert_eq!(running.latest_attempt, Some(3));
}

#[silo::test]
async fn a_job_leased_by_id_records_the_attempt_it_is_running() {
    let (_tmp, shard) = open_temp_shard().await;
    import_with_failed_attempts(&shard, "job", 1).await;

    shard
        .lease_task(TENANT, "job", "worker")
        .await
        .expect("lease task");

    let running = status(&shard, "job").await;
    assert_eq!(running.kind, JobStatusKind::Running);
    assert_eq!(running.latest_attempt, Some(2));
}

#[silo::test]
async fn a_finished_job_records_its_last_attempt() {
    let (_tmp, shard) = open_temp_shard().await;

    import_with_failed_attempts(&shard, "succeeds", 2).await;
    let task_id = dequeue_one(&shard).await;
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![1] })
        .await
        .expect("report success");
    let succeeded = status(&shard, "succeeds").await;
    assert_eq!(succeeded.kind, JobStatusKind::Succeeded);
    assert_eq!(succeeded.latest_attempt, Some(3));

    let (_tmp, shard) = open_temp_shard().await;
    let job_id = shard
        .enqueue(
            TENANT,
            None,
            10u8,
            now_ms(),
            None,
            msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
    let task_id = dequeue_one(&shard).await;
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"boom".to_vec(),
            },
        )
        .await
        .expect("report failure");
    let failed = status(&shard, &job_id).await;
    assert_eq!(failed.kind, JobStatusKind::Failed);
    assert_eq!(failed.latest_attempt, Some(1));

    let (_tmp, shard) = open_temp_shard().await;
    import_with_failed_attempts(&shard, "cancelled", 1).await;
    let task_id = dequeue_one(&shard).await;
    shard.cancel_job(TENANT, "cancelled").await.expect("cancel");
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Cancelled)
        .await
        .expect("report cancelled");
    let cancelled = status(&shard, "cancelled").await;
    assert_eq!(cancelled.kind, JobStatusKind::Cancelled);
    assert_eq!(cancelled.latest_attempt, Some(2));
}

#[silo::test]
async fn an_imported_terminal_job_records_its_last_attempt() {
    let (_tmp, shard) = open_temp_shard().await;
    let params = ImportJobParams {
        id: "job".to_string(),
        priority: 50,
        enqueue_time_ms: 1_700_000_000_000,
        start_at_ms: 0,
        retry_policy: None,
        payload: msgpack_payload(&serde_json::json!({})),
        limits: vec![],
        metadata: None,
        task_group: "default".to_string(),
        attempts: vec![
            failed_attempt(1_700_000_001_000),
            failed_attempt(1_700_000_002_000),
            succeeded_attempt(1_700_000_003_000),
        ],
    };
    shard
        .import_jobs(TENANT, vec![params])
        .await
        .expect("import");

    let succeeded = status(&shard, "job").await;
    assert_eq!(succeeded.kind, JobStatusKind::Succeeded);
    assert_eq!(succeeded.latest_attempt, Some(3));
}

/// A job cancelled before its pending attempt ran keeps that attempt's
/// number, which bounds its rows one below it.
#[silo::test]
async fn a_job_cancelled_while_scheduled_bounds_rows_below_its_pending_attempt() {
    let (_tmp, shard) = open_temp_shard().await;
    import_with_failed_attempts(&shard, "job", 2).await;

    shard.cancel_job(TENANT, "job").await.expect("cancel");

    let cancelled = status(&shard, "job").await;
    assert_eq!(cancelled.kind, JobStatusKind::Cancelled);
    assert_eq!(cancelled.highest_attempt(), Some(2));
    assert_eq!(
        attempt_numbers(
            &shard
                .get_job_attempts(TENANT, "job")
                .await
                .expect("attempts")
        ),
        [1, 2]
    );
}

// ---------------------------------------------------------------------------
// Reading attempts
// ---------------------------------------------------------------------------

#[silo::test]
async fn a_running_job_with_three_attempts_reads_them_by_key_in_order() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    import_with_failed_attempts(&shard, "job", 2).await;
    dequeue_one(&shard).await;

    let attempts = shard
        .get_job_attempts(TENANT, "job")
        .await
        .expect("attempts");
    assert_eq!(attempt_numbers(&attempts), [1, 2, 3]);
    assert!(matches!(attempts[0].state(), AttemptStatus::Failed { .. }));
    assert!(matches!(attempts[2].state(), AttemptStatus::Running));

    let keys_before = attempt_reads(&metrics, "keys");
    let scans = fewest_scans(&shard, &metrics, || async {
        let attempts = shard
            .get_job_attempts(TENANT, "job")
            .await
            .expect("attempts");
        assert_eq!(attempt_numbers(&attempts), [1, 2, 3]);
    })
    .await;

    assert_eq!(scans, 0.0, "reading three attempts opened a scan");
    assert_eq!(attempt_reads(&metrics, "scan"), 0.0);
    assert_eq!(attempt_reads(&metrics, "keys") - keys_before, 5.0);
}

#[silo::test]
async fn a_job_between_attempts_reads_the_attempts_it_has_run() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    import_with_failed_attempts(&shard, "job", 2).await;

    let scans = fewest_scans(&shard, &metrics, || async {
        let attempts = shard
            .get_job_attempts(TENANT, "job")
            .await
            .expect("attempts");
        assert_eq!(attempt_numbers(&attempts), [1, 2]);
    })
    .await;

    assert_eq!(scans, 0.0);
    assert_eq!(attempt_reads(&metrics, "scan"), 0.0);
}

#[silo::test]
async fn a_job_that_has_not_run_reads_no_attempt_rows() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    import_with_failed_attempts(&shard, "job", 0).await;

    let scans = fewest_scans(&shard, &metrics, || async {
        let attempts = shard
            .get_job_attempts(TENANT, "job")
            .await
            .expect("attempts");
        assert!(attempts.is_empty());
    })
    .await;

    assert_eq!(scans, 0.0);
    assert_eq!(attempt_reads(&metrics, "scan"), 0.0);
}

#[silo::test]
async fn the_latest_attempt_of_a_finished_job_is_read_by_key() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    import_with_failed_attempts(&shard, "job", 2).await;
    let task_id = dequeue_one(&shard).await;
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![9] })
        .await
        .expect("report success");

    let scans = fewest_scans(&shard, &metrics, || async {
        let latest = shard
            .get_latest_job_attempt(TENANT, "job")
            .await
            .expect("latest")
            .expect("an attempt");
        assert_eq!(latest.attempt_number(), 3);
        assert!(matches!(latest.state(), AttemptStatus::Succeeded { .. }));
    })
    .await;

    assert_eq!(scans, 0.0, "reading the latest attempt opened a scan");
    assert_eq!(attempt_reads(&metrics, "scan"), 0.0);
}

/// The readers that take a status the caller already holds read the same
/// rows as the ones that look the status up.
#[silo::test]
async fn readers_given_a_status_read_the_rows_it_bounds() {
    let (_tmp, shard) = open_temp_shard().await;
    import_with_failed_attempts(&shard, "job", 2).await;
    dequeue_one(&shard).await;
    let running = status(&shard, "job").await;

    let attempts = shard
        .get_job_attempts_for_status(TENANT, "job", &running)
        .await
        .expect("attempts");
    assert_eq!(attempt_numbers(&attempts), [1, 2, 3]);

    let latest = shard
        .get_latest_job_attempt_for_status(TENANT, "job", &running)
        .await
        .expect("latest")
        .expect("an attempt");
    assert_eq!(latest.attempt_number(), 3);
}

#[silo::test]
async fn an_absent_attempt_row_is_left_out_of_a_read() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    import_with_failed_attempts(&shard, "job", 2).await;
    dequeue_one(&shard).await;
    delete_attempt_row(&shard, "job", 2).await;

    let attempts = shard
        .get_job_attempts(TENANT, "job")
        .await
        .expect("attempts");

    assert_eq!(attempt_numbers(&attempts), [1, 3]);
    assert_eq!(attempt_reads(&metrics, "scan"), 0.0);
}

/// When the row for the highest attempt is absent, the latest attempt is
/// the highest one that does have a row, and finding it counts as one read
/// by key.
#[silo::test]
async fn the_latest_attempt_is_the_highest_row_present() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    import_with_failed_attempts(&shard, "job", 3).await;
    delete_attempt_row(&shard, "job", 3).await;
    let keys_before = attempt_reads(&metrics, "keys");
    let scans_before = attempt_reads(&metrics, "scan");

    let latest = shard
        .get_latest_job_attempt(TENANT, "job")
        .await
        .expect("latest")
        .expect("an attempt");

    assert_eq!(latest.attempt_number(), 2);
    assert_eq!(attempt_reads(&metrics, "keys") - keys_before, 1.0);
    assert_eq!(attempt_reads(&metrics, "scan") - scans_before, 0.0);
}

/// Above the point-read limit, a latest-attempt read whose highest row is
/// absent finds the rows below it with a scan, and counts as one read by
/// scan and none by key.
#[silo::test]
async fn a_latest_attempt_read_that_scans_counts_as_one_scan() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let count = ATTEMPT_POINT_READ_LIMIT + 8;
    import_with_failed_attempts(&shard, "job", count as usize).await;
    delete_attempt_row(&shard, "job", count).await;
    let keys_before = attempt_reads(&metrics, "keys");
    let scans_before = attempt_reads(&metrics, "scan");

    let latest = shard
        .get_latest_job_attempt(TENANT, "job")
        .await
        .expect("latest")
        .expect("an attempt");

    assert_eq!(latest.attempt_number(), count - 1);
    assert_eq!(attempt_reads(&metrics, "scan") - scans_before, 1.0);
    assert_eq!(attempt_reads(&metrics, "keys") - keys_before, 0.0);
}

/// A latest-attempt read that finds the highest row counts as one read by
/// key, however many attempts the job has.
#[silo::test]
async fn a_latest_attempt_read_above_the_point_read_limit_counts_as_one_key_read() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let count = ATTEMPT_POINT_READ_LIMIT + 8;
    import_with_failed_attempts(&shard, "job", count as usize).await;
    let keys_before = attempt_reads(&metrics, "keys");
    let scans_before = attempt_reads(&metrics, "scan");

    let latest = shard
        .get_latest_job_attempt(TENANT, "job")
        .await
        .expect("latest")
        .expect("an attempt");

    assert_eq!(latest.attempt_number(), count);
    assert_eq!(attempt_reads(&metrics, "keys") - keys_before, 1.0);
    assert_eq!(attempt_reads(&metrics, "scan") - scans_before, 0.0);
}

#[silo::test]
async fn a_job_above_the_point_read_limit_returns_every_attempt() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let count = ATTEMPT_POINT_READ_LIMIT as usize + 8;
    import_with_failed_attempts(&shard, "job", count).await;

    let attempts = shard
        .get_job_attempts(TENANT, "job")
        .await
        .expect("attempts");

    let expected: Vec<u32> = (1..=count as u32).collect();
    assert_eq!(attempt_numbers(&attempts), expected);
    assert_eq!(attempt_reads(&metrics, "scan"), 1.0);
    assert_eq!(attempt_reads(&metrics, "keys"), 0.0);
}

#[silo::test]
async fn a_job_at_the_point_read_limit_is_read_by_key() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let count = ATTEMPT_POINT_READ_LIMIT as usize;
    import_with_failed_attempts(&shard, "job", count).await;

    let attempts = shard
        .get_job_attempts(TENANT, "job")
        .await
        .expect("attempts");

    let expected: Vec<u32> = (1..=count as u32).collect();
    assert_eq!(attempt_numbers(&attempts), expected);
    assert_eq!(attempt_reads(&metrics, "scan"), 0.0);
    assert_eq!(attempt_reads(&metrics, "keys"), 1.0);
}

/// A status record with no attempt number gives no bound, so the reader
/// scans the job's attempt prefix and still returns every attempt.
#[silo::test]
async fn a_status_with_no_attempt_number_is_read_by_scan() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    import_with_failed_attempts(&shard, "job", 2).await;
    dequeue_one(&shard).await;

    let mut unbounded = JobStatus::running(now_ms());
    let stored = status(&shard, "job").await;
    unbounded.enqueue_time_ms = stored.enqueue_time_ms;
    unbounded.priority = stored.priority;
    let mut batch = WriteBatch::new();
    batch.put(job_status_key(TENANT, "job"), encode_job_status(&unbounded));
    shard.db().write(batch).await.expect("write status");

    let attempts = shard
        .get_job_attempts(TENANT, "job")
        .await
        .expect("attempts");
    assert_eq!(attempt_numbers(&attempts), [1, 2, 3]);
    let latest = shard
        .get_latest_job_attempt(TENANT, "job")
        .await
        .expect("latest")
        .expect("an attempt");
    assert_eq!(latest.attempt_number(), 3);

    assert_eq!(attempt_reads(&metrics, "scan"), 2.0);
    assert_eq!(attempt_reads(&metrics, "keys"), 0.0);
}

/// A job with no status record is read by scan, which returns whatever
/// attempt rows remain under its prefix.
#[silo::test]
async fn a_job_with_no_status_record_is_read_by_scan() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    import_with_failed_attempts(&shard, "job", 2).await;
    let mut batch = WriteBatch::new();
    batch.delete(job_status_key(TENANT, "job"));
    shard.db().write(batch).await.expect("delete status");

    let attempts = shard
        .get_job_attempts(TENANT, "job")
        .await
        .expect("attempts");

    assert_eq!(attempt_numbers(&attempts), [1, 2]);
    assert_eq!(attempt_reads(&metrics, "scan"), 1.0);
}

// ---------------------------------------------------------------------------
// Re-writing attempts when a job reaches or leaves a terminal status
// ---------------------------------------------------------------------------

async fn open_expiring_shard_with_metrics()
-> (tempfile::TempDir, std::sync::Arc<JobStoreShard>, Metrics) {
    open_temp_shard_with_metrics_and_config(|cfg| {
        cfg.completed_job_expire_s = Some(60);
        cfg.terminal_job_expire_s = Some(60);
    })
    .await
}

/// A terminal report on a job's third attempt gives all three attempt rows
/// the terminal TTL and opens no scan. The scan count covers the whole
/// shard, so it is taken as the fewest across several jobs.
#[silo::test]
async fn a_terminal_report_on_the_third_attempt_expires_every_attempt_without_a_scan() {
    let (_tmp, shard, metrics) = open_expiring_shard_with_metrics().await;

    let mut fewest = f64::MAX;
    for i in 0..5 {
        let job_id = format!("job-{i}");
        import_with_failed_attempts(&shard, &job_id, 2).await;
        let task_id = dequeue_one(&shard).await;
        for attempt in 1..=3 {
            assert_eq!(attempt_expire_ts(&shard, &job_id, attempt).await, None);
        }

        wait_for_scans_to_settle(&shard, &metrics).await;
        let before = slatedb_scan_requests(&shard, &metrics);
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![1] })
            .await
            .expect("report success");
        fewest = fewest.min(slatedb_scan_requests(&shard, &metrics) - before);

        for attempt in 1..=3 {
            assert!(
                attempt_expire_ts(&shard, &job_id, attempt).await.is_some(),
                "attempt {attempt} of {job_id} carries no TTL"
            );
        }
        let latest = shard
            .get_latest_job_attempt(TENANT, &job_id)
            .await
            .expect("latest")
            .expect("an attempt");
        assert!(matches!(latest.state(), AttemptStatus::Succeeded { .. }));
    }

    assert_eq!(fewest, 0.0, "every terminal report opened a scan");
    assert_eq!(attempt_reads(&metrics, "scan"), 0.0);
}

/// A terminal report on a job's only attempt writes that attempt's row
/// itself and reads no other attempt row.
#[silo::test]
async fn a_terminal_report_on_the_first_attempt_reads_no_attempt_rows() {
    let (_tmp, shard, metrics) = open_expiring_shard_with_metrics().await;
    import_with_failed_attempts(&shard, "job", 0).await;
    let task_id = dequeue_one(&shard).await;

    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![1] })
        .await
        .expect("report success");

    assert!(attempt_expire_ts(&shard, "job", 1).await.is_some());
    assert_eq!(attempt_reads(&metrics, "scan"), 0.0);
    assert_eq!(attempt_reads(&metrics, "keys"), 0.0);
}

#[silo::test]
async fn a_terminal_report_skips_an_absent_attempt_row() {
    let (_tmp, shard, metrics) = open_expiring_shard_with_metrics().await;
    import_with_failed_attempts(&shard, "job", 2).await;
    let task_id = dequeue_one(&shard).await;
    delete_attempt_row(&shard, "job", 2).await;

    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![1] })
        .await
        .expect("report success");

    assert!(attempt_expire_ts(&shard, "job", 1).await.is_some());
    assert!(attempt_expire_ts(&shard, "job", 3).await.is_some());
    assert!(
        !attempt_row_exists(&shard, "job", 2).await,
        "the absent attempt row was written"
    );
    assert_eq!(attempt_reads(&metrics, "scan"), 0.0);
}

#[silo::test]
async fn cancelling_a_scheduled_job_expires_its_attempts_by_key() {
    let (_tmp, shard, metrics) = open_expiring_shard_with_metrics().await;
    import_with_failed_attempts(&shard, "job", 2).await;

    shard.cancel_job(TENANT, "job").await.expect("cancel");

    assert!(attempt_expire_ts(&shard, "job", 1).await.is_some());
    assert!(attempt_expire_ts(&shard, "job", 2).await.is_some());
    assert_eq!(attempt_reads(&metrics, "scan"), 0.0);
}

/// Restarting a failed job clears the TTL on its attempt rows and numbers
/// the next attempt after the last one, without scanning for either.
#[silo::test]
async fn restarting_a_failed_job_revives_its_attempts_by_key() {
    let (_tmp, shard, metrics) = open_expiring_shard_with_metrics().await;
    let job_id = shard
        .enqueue(
            TENANT,
            None,
            10u8,
            now_ms(),
            Some(retry_policy(1)),
            msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
    for _ in 0..2 {
        let task_id = dequeue_one(&shard).await;
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: b"boom".to_vec(),
                },
            )
            .await
            .expect("report failure");
    }
    assert_eq!(status(&shard, &job_id).await.kind, JobStatusKind::Failed);
    assert!(attempt_expire_ts(&shard, &job_id, 1).await.is_some());
    assert!(attempt_expire_ts(&shard, &job_id, 2).await.is_some());
    let scans_before = attempt_reads(&metrics, "scan");

    shard.restart_job(TENANT, &job_id).await.expect("restart");

    assert_eq!(attempt_expire_ts(&shard, &job_id, 1).await, None);
    assert_eq!(attempt_expire_ts(&shard, &job_id, 2).await, None);
    let restarted = status(&shard, &job_id).await;
    assert_eq!(restarted.kind, JobStatusKind::Scheduled);
    assert_eq!(restarted.current_attempt, Some(3));
    assert_eq!(attempt_reads(&metrics, "scan") - scans_before, 0.0);
}

/// A failed job whose status record carries no attempt number is restarted
/// by scanning for its attempts, and its next attempt still follows the
/// last one.
#[silo::test]
async fn restarting_a_job_with_no_attempt_number_scans_for_its_attempts() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let job_id = shard
        .enqueue(
            TENANT,
            None,
            10u8,
            now_ms(),
            None,
            msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
    let task_id = dequeue_one(&shard).await;
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"boom".to_vec(),
            },
        )
        .await
        .expect("report failure");
    let mut stored = status(&shard, &job_id).await;
    stored.latest_attempt = None;
    let mut batch = WriteBatch::new();
    batch.put(job_status_key(TENANT, &job_id), encode_job_status(&stored));
    shard.db().write(batch).await.expect("write status");

    shard.restart_job(TENANT, &job_id).await.expect("restart");

    let restarted = status(&shard, &job_id).await;
    assert_eq!(restarted.current_attempt, Some(2));
    assert!(attempt_reads(&metrics, "scan") >= 1.0);
}

// ---------------------------------------------------------------------------
// The attempt bound from one status record to the next
// ---------------------------------------------------------------------------

/// How many status writes carried a lower attempt bound than the record
/// they replaced.
fn attempt_bound_regressions(metrics: &Metrics) -> f64 {
    metric_value_or_zero(
        &gather_metrics_text(metrics),
        &["silo_job_attempt_bound_regressions_total", "shard=\"test\""],
    )
}

async fn enqueue_job(
    shard: &JobStoreShard,
    start_at_ms: i64,
    retry_policy: Option<RetryPolicy>,
) -> String {
    shard
        .enqueue(
            TENANT,
            None,
            10u8,
            start_at_ms,
            retry_policy,
            msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue")
}

async fn report_failure(shard: &JobStoreShard, task_id: &str) {
    shard
        .report_attempt_outcome(
            task_id,
            AttemptOutcome::Error {
                error_code: "TEST".to_string(),
                error: b"boom".to_vec(),
            },
        )
        .await
        .expect("report failure");
}

async fn report_success(shard: &JobStoreShard, task_id: &str) {
    shard
        .report_attempt_outcome(task_id, AttemptOutcome::Success { result: vec![1] })
        .await
        .expect("report success");
}

/// A job with attempt rows 1, 3 and 4, a stored bound of 4, and a pending
/// task for attempt 2: the state a task for an attempt below the job's
/// highest leaves behind when it is dispatched.
async fn job_with_a_pending_attempt_below_its_rows(shard: &JobStoreShard, job_id: &str) {
    import_with_failed_attempts(shard, job_id, 1).await;
    let mut stored = status(shard, job_id).await;
    assert_eq!(stored.current_attempt, Some(2));
    stored.latest_attempt = Some(4);

    let mut batch = WriteBatch::new();
    for attempt_number in [3, 4] {
        let attempt = JobAttempt {
            job_id: job_id.to_string(),
            attempt_number,
            relative_attempt_number: attempt_number,
            task_id: format!("task-{attempt_number}"),
            started_at_ms: 1_700_000_002_000,
            status: AttemptStatus::Failed {
                finished_at_ms: 1_700_000_003_000,
                error_code: "ERR".to_string(),
                error: vec![1, 2, 3],
            },
        };
        batch.put(
            attempt_key(TENANT, job_id, attempt_number),
            encode_attempt(&attempt),
        );
    }
    batch.put(job_status_key(TENANT, job_id), encode_job_status(&stored));
    shard.db().write(batch).await.expect("write rows");
}

#[silo::test]
async fn a_status_write_below_the_stored_attempt_bound_keeps_the_stored_bound() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    job_with_a_pending_attempt_below_its_rows(&shard, "job").await;
    assert_eq!(attempt_bound_regressions(&metrics), 0.0);

    dequeue_one(&shard).await;

    let running = status(&shard, "job").await;
    assert_eq!(running.kind, JobStatusKind::Running);
    assert_eq!(running.highest_attempt(), Some(4));
    assert_eq!(attempt_bound_regressions(&metrics), 1.0);
}

#[silo::test]
async fn attempts_above_a_dispatched_attempt_are_still_read() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    job_with_a_pending_attempt_below_its_rows(&shard, "job").await;
    dequeue_one(&shard).await;

    let attempts = shard
        .get_job_attempts(TENANT, "job")
        .await
        .expect("attempts");

    assert_eq!(attempt_numbers(&attempts), [1, 2, 3, 4]);
    assert_eq!(attempt_reads(&metrics, "scan"), 0.0);
}

#[silo::test]
async fn a_job_that_retries_and_succeeds_never_lowers_its_attempt_bound() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let job_id = enqueue_job(&shard, now_ms(), Some(retry_policy(1))).await;

    let task_id = dequeue_one(&shard).await;
    report_failure(&shard, &task_id).await;
    assert_eq!(status(&shard, &job_id).await.kind, JobStatusKind::Scheduled);
    let task_id = dequeue_one(&shard).await;
    report_success(&shard, &task_id).await;

    let succeeded = status(&shard, &job_id).await;
    assert_eq!(succeeded.kind, JobStatusKind::Succeeded);
    assert_eq!(succeeded.highest_attempt(), Some(2));
    assert_eq!(attempt_bound_regressions(&metrics), 0.0);
}

#[silo::test]
async fn a_job_that_fails_and_is_restarted_never_lowers_its_attempt_bound() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let job_id = enqueue_job(&shard, now_ms(), None).await;

    let task_id = dequeue_one(&shard).await;
    report_failure(&shard, &task_id).await;
    assert_eq!(status(&shard, &job_id).await.kind, JobStatusKind::Failed);
    shard.restart_job(TENANT, &job_id).await.expect("restart");
    let task_id = dequeue_one(&shard).await;
    report_success(&shard, &task_id).await;

    let succeeded = status(&shard, &job_id).await;
    assert_eq!(succeeded.kind, JobStatusKind::Succeeded);
    assert_eq!(succeeded.highest_attempt(), Some(2));
    assert_eq!(attempt_bound_regressions(&metrics), 0.0);
}

/// Expediting a job, cancelling it between attempts, restarting it and
/// leasing it by id each write a status record whose bound is the stored
/// one or the next attempt's.
#[silo::test]
async fn expedite_cancel_restart_and_lease_by_id_never_lower_the_attempt_bound() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let far_future_ms = now_ms() + 3_600_000;
    let job_id = enqueue_job(&shard, far_future_ms, Some(retry_policy(3))).await;

    shard.expedite_job(TENANT, &job_id).await.expect("expedite");
    assert_eq!(status(&shard, &job_id).await.highest_attempt(), Some(0));
    let task_id = dequeue_one(&shard).await;
    report_failure(&shard, &task_id).await;
    shard.cancel_job(TENANT, &job_id).await.expect("cancel");
    assert_eq!(status(&shard, &job_id).await.highest_attempt(), Some(1));
    shard.restart_job(TENANT, &job_id).await.expect("restart");
    assert_eq!(status(&shard, &job_id).await.highest_attempt(), Some(1));
    let leased = shard
        .lease_task(TENANT, &job_id, "worker")
        .await
        .expect("lease by id");
    assert_eq!(status(&shard, &job_id).await.highest_attempt(), Some(2));
    report_success(&shard, leased.attempt().task_id()).await;

    assert_eq!(status(&shard, &job_id).await.highest_attempt(), Some(2));
    assert_eq!(attempt_bound_regressions(&metrics), 0.0);
}
