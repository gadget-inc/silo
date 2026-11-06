//! Tests for the broker scan floor: each scan starts at the start time below
//! which every task row has already been dealt with, so the tombstones of
//! dequeued tasks at the head of the range are not walked again. Every path
//! that writes a task row lowers the floor to the row's start time, and a
//! periodic full rescan bounds the cost of one that did not.

mod test_helpers;

use std::time::{Duration, Instant};

use silo::job::{ConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::retry::RetryPolicy;
use test_helpers::*;

fn read_full_rescans_total(metrics: &silo::metrics::Metrics) -> f64 {
    metric_value_or_zero(
        &gather_metrics_text(metrics),
        &["silo_broker_full_rescans_total", "task_group=\"default\""],
    )
}

fn read_full_rescan_inserted_total(metrics: &silo::metrics::Metrics) -> f64 {
    metric_value_or_zero(
        &gather_metrics_text(metrics),
        &[
            "silo_broker_full_rescan_inserted_total",
            "task_group=\"default\"",
        ],
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

/// Wait until the broker's last scan read to the end of its range.
async fn settle(shard: &JobStoreShard, task_group: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while shard.broker_next_ready_at_ms(task_group) != Some(i64::MAX) {
        assert!(
            Instant::now() < deadline,
            "broker never read to the end of its range, next-ready is {:?}",
            shard.broker_next_ready_at_ms(task_group)
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The floor of a task group's broker, which must exist and be set.
fn scan_floor_ms(shard: &JobStoreShard, task_group: &str) -> i64 {
    shard
        .broker_scan_floor_ms(task_group)
        .expect("broker exists")
        .expect("floor is set once a scan has read the range")
}

/// Dequeuing every enqueued task moves the scan floor past all of their
/// rows, so the next scan starts after the dead head of the range.
#[silo::test]
async fn dequeued_rows_move_the_scan_floor_past_them() {
    let (_tmp, shard) = open_temp_shard().await;
    let count = 200;
    let mut last_start_ms = 0;
    for i in 0..count {
        last_start_ms = now_ms();
        enqueue_at(&shard, &format!("job-{i}"), last_start_ms, vec![]).await;
    }

    let ids = dequeue_task_ids_until(&shard, "worker", "default", count).await;
    assert_eq!(ids.len(), count);
    assert_eq!(count_task_keys(shard.db()).await, 0);
    settle(&shard, "default").await;

    let floor_ms = scan_floor_ms(&shard, "default");
    assert!(
        floor_ms >= last_start_ms,
        "floor {floor_ms} sits below the last dequeued row at {last_start_ms}"
    );
}

/// A task enqueued with a start time below the floor is leased promptly:
/// the enqueue lowers the floor to the row's start time.
#[silo::test]
async fn enqueue_below_the_floor_is_leased() {
    let (_tmp, shard) = open_temp_shard().await;
    enqueue_at(&shard, "head", now_ms(), vec![]).await;
    let (ids, _) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(ids.len(), 1);
    settle(&shard, "default").await;

    let early_ms = scan_floor_ms(&shard, "default") - 60_000;
    enqueue_at(&shard, "early", early_ms, vec![]).await;
    assert!(
        scan_floor_ms(&shard, "default") <= early_ms,
        "the enqueue lowers the floor to the row's start time"
    );

    let (ids, waited) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(ids.len(), 1);
    assert!(
        waited < Duration::from_millis(1_000),
        "the early row waited {waited:?} for a full rescan"
    );
}

/// A retry re-queued by a failed attempt is leased promptly: the retry's
/// task row is noted to the broker like any other write.
#[silo::test]
async fn retry_after_a_failed_attempt_is_leased() {
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
        "the retry waited {waited:?} for a full rescan"
    );
}

/// A chain the grant scanner resumes keeps the job's original start time,
/// which sorts below the floor by the time the slot frees up. The grant
/// lowers the floor, so the resumed attempt is leased promptly.
#[silo::test]
async fn grant_scanner_resume_below_the_floor_is_leased() {
    let (_tmp, shard) = open_temp_shard().await;
    let limit = Limit::Concurrency(ConcurrencyLimit {
        key: "q".to_string(),
        max_concurrency: 1,
    });
    enqueue_at(&shard, "holder", now_ms(), vec![limit.clone()]).await;
    let (holder_ids, _) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(holder_ids.len(), 1);

    let waiting_start_ms = now_ms();
    enqueue_at(&shard, "waiting", waiting_start_ms, vec![limit]).await;
    // Polls while the slot is held find nothing and leave the floor above
    // the waiting job's start time.
    for _ in 0..3 {
        let result = shard
            .dequeue("worker", "default", 10)
            .await
            .expect("dequeue");
        assert!(result.tasks.is_empty(), "leased while the slot is held");
    }
    settle(&shard, "default").await;
    assert!(
        scan_floor_ms(&shard, "default") > waiting_start_ms,
        "the floor has moved past the waiting job's start time"
    );

    shard
        .report_attempt_outcome(&holder_ids[0], AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report outcome");

    let (ids, waited) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(ids.len(), 1);
    assert_ne!(ids[0], holder_ids[0]);
    assert!(
        waited < Duration::from_millis(2_000),
        "the granted attempt waited {waited:?} for a full rescan"
    );
}

/// An immediate enqueue (start time 0) tells the broker the time its row is
/// keyed at, so the floor stays there instead of dropping to the head of
/// the range.
#[silo::test]
async fn immediate_enqueue_keeps_the_floor_at_the_time_written() {
    let (_tmp, shard) = open_temp_shard().await;
    enqueue_at(&shard, "head", now_ms(), vec![]).await;
    let (ids, _) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(ids.len(), 1);
    settle(&shard, "default").await;

    // The row is keyed at the current time, at or above the floor, so the
    // note has nothing to lower; a raw start time of 0 would drop it to 0.
    let floor_before_ms = scan_floor_ms(&shard, "default");
    enqueue_at(&shard, "immediate", 0, vec![]).await;
    let floor_ms = scan_floor_ms(&shard, "default");
    assert!(
        floor_ms >= floor_before_ms,
        "floor dropped from {floor_before_ms} to {floor_ms} for an immediate enqueue"
    );

    let (ids, _) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(ids.len(), 1);
}

/// The periodic full rescan runs on schedule and, with every writer noting
/// its rows, counts nothing as found below the floor, even while rows keep
/// arriving.
#[silo::test]
async fn full_rescan_runs_on_schedule_and_buffers_nothing() {
    let (_tmp, shard, metrics) =
        open_temp_shard_with_metrics_and_config(|cfg| cfg.broker_full_rescan_interval_ms = 200)
            .await;
    enqueue_at(&shard, "head", now_ms(), vec![]).await;
    let (ids, _) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(ids.len(), 1);
    settle(&shard, "default").await;

    let rescans = poll_until(
        || async { read_full_rescans_total(&metrics) },
        |rescans| *rescans >= 1.0,
        10_000,
    )
    .await;
    assert!(rescans >= 1.0);

    // Keep writing through the next rescans: the rows a rescan happens to
    // buffer were noted by their writers, so none counts as found below the
    // floor.
    for i in 0..8 {
        enqueue_at(&shard, &format!("steady-{i}"), 0, vec![]).await;
        let (ids, _) = lease_within(&shard, "default", Duration::from_secs(5)).await;
        assert_eq!(ids.len(), 1);
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    let rescans_after = poll_until(
        || async { read_full_rescans_total(&metrics) },
        |after| *after >= rescans + 1.0,
        10_000,
    )
    .await;
    assert!(rescans_after > rescans);
    assert_eq!(
        read_full_rescan_inserted_total(&metrics),
        0.0,
        "a full rescan counted a noted row as found below the floor"
    );
}

/// A task row written behind the broker's back below the floor is found by
/// the next full rescan and counted, since no writer noted it.
#[silo::test]
async fn full_rescan_counts_a_row_written_behind_the_brokers_back() {
    let (_tmp, shard, metrics) =
        open_temp_shard_with_metrics_and_config(|cfg| cfg.broker_full_rescan_interval_ms = 200)
            .await;
    enqueue_at(&shard, "head", now_ms(), vec![]).await;
    let (ids, _) = lease_within(&shard, "default", Duration::from_secs(5)).await;
    assert_eq!(ids.len(), 1);
    settle(&shard, "default").await;

    let hidden_start_ms = scan_floor_ms(&shard, "default") - 10_000;
    let hidden = silo::task::Task::RunAttempt {
        id: "hidden-task".to_string(),
        tenant: "-".to_string(),
        job_id: "hidden".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        held_queues: vec![],
        task_group: "default".to_string(),
    };
    let key = silo::keys::task_key("default", hidden_start_ms, 10, "hidden", 1, hidden_start_ms);
    let mut batch = slatedb::WriteBatch::new();
    batch.put(&key, &silo::codec::encode_task(&hidden));
    shard.db().write(batch).await.expect("write hidden row");

    let found = poll_until(
        || async { read_full_rescan_inserted_total(&metrics) },
        |found| *found >= 1.0,
        10_000,
    )
    .await;
    assert_eq!(found, 1.0, "the rescan counts the hidden row once");
    assert_eq!(
        shard.broker_buffer_len(),
        1,
        "the rescan buffered the hidden row"
    );
}
