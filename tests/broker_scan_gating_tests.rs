//! Tests for broker scan gating: an empty poll never triggers a scan on its
//! own. The broker scans after a write that may have readied a task, when
//! the earliest future-dated row it stopped at becomes due, or when its last
//! scan stopped with ready rows remaining.

mod test_helpers;

use std::time::{Duration, Instant};

use silo::job::{ConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::retry::RetryPolicy;
use test_helpers::*;

fn read_scans_total(metrics: &silo::metrics::Metrics) -> f64 {
    metric_value_or_zero(
        &gather_metrics_text(metrics),
        &["silo_broker_scans_total", "shard=\"test\""],
    )
}

async fn enqueue_at(shard: &JobStoreShard, job_id: &str, start_at_ms: i64, limits: Vec<Limit>) {
    shard
        .enqueue(
            "-",
            Some(job_id.to_string()),
            10u8,
            start_at_ms,
            None,
            msgpack_payload(&serde_json::json!({"j": job_id})),
            limits,
            None,
            "default",
        )
        .await
        .expect("enqueue");
}

/// Poll `dequeue` for `task_group` until it returns something, failing after
/// `timeout`. Returns the leased task ids and how long the wait took.
async fn lease_within(
    shard: &JobStoreShard,
    task_group: &str,
    timeout: Duration,
) -> (Vec<String>, Duration) {
    let started = Instant::now();
    loop {
        let batch = shard
            .dequeue("worker", task_group, 10)
            .await
            .expect("dequeue")
            .tasks;
        if !batch.is_empty() {
            let ids = batch
                .iter()
                .map(|t| t.attempt().task_id().to_string())
                .collect();
            return (ids, started.elapsed());
        }
        assert!(
            started.elapsed() < timeout,
            "no task leased within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Wait until the broker's last scan stopped at `next_ready_at_ms`:
/// `i64::MAX` once the passes a write-path wakeup triggers have read the
/// whole range, or the start time of a future-dated row.
async fn settle_until(shard: &JobStoreShard, task_group: &str, next_ready_at_ms: i64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while shard.broker_next_ready_at_ms(task_group) != Some(next_ready_at_ms) {
        assert!(
            Instant::now() < deadline,
            "broker never recorded next-ready {next_ready_at_ms}, got {:?}",
            shard.broker_next_ready_at_ms(task_group)
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn settle(shard: &JobStoreShard, task_group: &str) {
    settle_until(shard, task_group, i64::MAX).await;
}

/// Empty polls on an idle shard do not scan. The only scans during a burst
/// of polls are the scanner's own periodic fallback passes, which back off
/// to one every two seconds; without gating each poll would scan.
#[silo::test]
async fn idle_polls_do_not_scan() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    enqueue_at(&shard, "warm", now_ms(), vec![]).await;
    let (ids, _) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(ids.len(), 1);
    settle(&shard, "default").await;

    let before = read_scans_total(&metrics);
    let polls = 40;
    for _ in 0..polls {
        let result = shard
            .dequeue("worker", "default", 10)
            .await
            .expect("dequeue");
        assert!(result.tasks.is_empty());
    }
    let after = read_scans_total(&metrics);

    assert!(
        after - before <= 4.0,
        "{polls} empty polls caused {} scans",
        after - before
    );
}

/// A task enqueued for the future is not scanned for by polls that arrive
/// before it is due, and is leased once it is.
#[silo::test]
async fn future_task_is_scanned_for_only_once_due() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let due_ms = now_ms() + 1_000;
    enqueue_at(&shard, "later", due_ms, vec![]).await;

    // The first poll creates the broker; its scan stops at the future row.
    let result = shard
        .dequeue("worker", "default", 10)
        .await
        .expect("dequeue");
    assert!(result.tasks.is_empty());
    settle_until(&shard, "default", due_ms).await;

    let before = read_scans_total(&metrics);
    let polls = 12;
    for _ in 0..polls {
        let result = shard
            .dequeue("worker", "default", 10)
            .await
            .expect("dequeue");
        assert!(result.tasks.is_empty(), "leased before due");
    }
    let after = read_scans_total(&metrics);
    assert!(
        after - before <= 4.0,
        "{polls} polls before the due time caused {} scans",
        after - before
    );

    let (ids, _) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(ids.len(), 1);
    assert!(now_ms() >= due_ms, "leased before its start time");
}

/// Enqueueing a task due sooner than the row the last scan stopped at lowers
/// the broker's recorded next-ready time, so it is leased on time.
#[silo::test]
async fn sooner_enqueue_lowers_the_recorded_next_ready_time() {
    let (_tmp, shard) = open_temp_shard().await;
    let far_ms = now_ms() + 5_000;
    enqueue_at(&shard, "far", far_ms, vec![]).await;
    let result = shard
        .dequeue("worker", "default", 10)
        .await
        .expect("dequeue");
    assert!(result.tasks.is_empty());
    settle_until(&shard, "default", far_ms).await;

    let soon_ms = now_ms() + 1_000;
    enqueue_at(&shard, "soon", soon_ms, vec![]).await;
    assert_eq!(shard.broker_next_ready_at_ms("default"), Some(soon_ms));

    let (ids, _) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(ids.len(), 1);
    let leased_at = now_ms();
    assert!(leased_at >= soon_ms, "leased before its start time");
    assert!(
        leased_at < soon_ms + 500,
        "leased {}ms after it was due",
        leased_at - soon_ms
    );
    assert_eq!(
        shard.broker_next_ready_at_ms("default"),
        Some(far_ms),
        "the far row is the next thing to wait for"
    );
}

/// A retry scheduled to run now by an attempt failure wakes the broker.
#[silo::test]
async fn retry_due_now_wakes_the_broker() {
    let (_tmp, shard) = open_temp_shard().await;
    shard
        .enqueue(
            "-",
            Some("retrying".to_string()),
            10u8,
            now_ms(),
            Some(RetryPolicy {
                retry_count: 1,
                initial_interval_ms: 0,
                max_interval_ms: 0,
                randomize_interval: false,
                backoff_factor: 1.0,
            }),
            msgpack_payload(&serde_json::json!({"j": 1})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
    let (ids, _) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(ids.len(), 1);
    settle(&shard, "default").await;

    shard
        .report_attempt_outcome(
            &ids[0],
            AttemptOutcome::Error {
                error_code: "boom".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report outcome");

    let (retry_ids, waited) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(retry_ids.len(), 1);
    assert_ne!(retry_ids[0], ids[0]);
    assert!(
        waited < Duration::from_millis(1_000),
        "retry waited {waited:?} for the periodic fallback scan"
    );
}

/// Restarting a failed job wakes the broker for the new attempt.
#[silo::test]
async fn restart_wakes_the_broker() {
    let (_tmp, shard) = open_temp_shard().await;
    enqueue_at(&shard, "restarted", now_ms(), vec![]).await;
    let (ids, _) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(ids.len(), 1);
    shard
        .report_attempt_outcome(
            &ids[0],
            AttemptOutcome::Error {
                error_code: "boom".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report outcome");

    settle(&shard, "default").await;

    shard
        .restart_job("-", "restarted")
        .await
        .expect("restart_job");

    let (restart_ids, waited) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(restart_ids.len(), 1);
    assert_ne!(restart_ids[0], ids[0]);
    assert!(
        waited < Duration::from_millis(1_000),
        "restarted attempt waited {waited:?} for the periodic fallback scan"
    );
}

/// A follow-up row written by dequeue itself (the RunAttempt a granted
/// RequestTicket continues into) is picked up by the same dequeue call.
#[silo::test]
async fn chain_followup_written_by_dequeue_is_leased_promptly() {
    let (_tmp, shard) = open_temp_shard().await;
    let due_ms = now_ms() + 300;
    enqueue_at(
        &shard,
        "ticketed",
        due_ms,
        vec![Limit::Concurrency(ConcurrencyLimit {
            key: "q".to_string(),
            max_concurrency: 1,
        })],
    )
    .await;
    let result = shard
        .dequeue("worker", "default", 10)
        .await
        .expect("dequeue");
    assert!(result.tasks.is_empty());
    settle_until(&shard, "default", due_ms).await;

    let (ids, _) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(ids.len(), 1);
    let leased_at = now_ms();
    assert!(leased_at >= due_ms);
    assert!(
        leased_at < due_ms + 1_000,
        "follow-up leased {}ms after the ticket was due",
        leased_at - due_ms
    );
}
