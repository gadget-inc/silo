//! Order in which the grant scanner serves the waiters of a concurrency
//! queue: priority-ordered request rows by priority then start time,
//! start-time-ordered rows by start time, and how the two coexist.

mod test_helpers;

use std::sync::Arc;

use silo::codec::encode_concurrency_action;
use silo::concurrency::GrantScannerConfig;
use silo::job::{ConcurrencyLimit, JobStatusKind, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::keys::{
    ParsedConcurrencyRequestKey, concurrency_request_key, concurrency_request_key_start_time_first,
    concurrency_request_prefix, end_bound, parse_concurrency_request_key,
};
use silo::task::ConcurrencyAction;
use slatedb::WriteBatch;

use test_helpers::*;

const TENANT: &str = "-";
const TASK_GROUP: &str = "default";

/// Open a shard that writes request rows in the given order, with the
/// background grant scanner stopped so `process_concurrency_grants` is the
/// only thing granting.
async fn open_shard(priority_ordered_requests: bool) -> (tempfile::TempDir, Arc<JobStoreShard>) {
    let (tmp, shard, _metrics) = open_temp_shard_with_grant_scanner_config(GrantScannerConfig {
        priority_ordered_requests,
        ..GrantScannerConfig::default()
    })
    .await;
    shard.stop_grant_scanner();
    (tmp, shard)
}

fn limit(queue: &str) -> Limit {
    Limit::Concurrency(ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: 1,
    })
}

async fn enqueue(shard: &JobStoreShard, queue: &str, priority: u8, start_at_ms: i64) -> String {
    shard
        .enqueue(
            TENANT,
            None,
            priority,
            start_at_ms,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"p": priority})),
            vec![limit(queue)],
            None,
            TASK_GROUP,
        )
        .await
        .expect("enqueue")
}

/// Enqueue a job that takes the queue's only slot and lease it. Returns the
/// leased task id.
async fn take_slot(shard: &JobStoreShard, queue: &str) -> String {
    enqueue(shard, queue, 10, now_ms()).await;
    let tasks = shard
        .dequeue("w", TASK_GROUP, 1)
        .await
        .expect("dequeue holder")
        .tasks;
    assert_eq!(tasks.len(), 1, "the first job should take the slot");
    tasks[0].attempt().task_id().to_string()
}

async fn succeed(shard: &JobStoreShard, task_id: &str) {
    shard
        .report_attempt_outcome(task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report success");
}

/// Run one scanner pass granting at most one waiter, then lease what it
/// granted. Returns the granted job's id and its task id.
async fn grant_one(shard: &JobStoreShard, queue: &str) -> (String, String) {
    shard.process_concurrency_grants(TENANT, queue, 1).await;
    let tasks = shard
        .dequeue("w", TASK_GROUP, 1)
        .await
        .expect("dequeue granted")
        .tasks;
    assert_eq!(tasks.len(), 1, "one waiter should have been granted");
    (
        tasks[0].job().id().to_string(),
        tasks[0].attempt().task_id().to_string(),
    )
}

async fn request_rows(
    shard: &JobStoreShard,
    queue: &str,
) -> Vec<(Vec<u8>, ParsedConcurrencyRequestKey, bytes::Bytes)> {
    let start = concurrency_request_prefix(TENANT, queue);
    let end = end_bound(&start);
    let mut iter = shard
        .db()
        .scan::<Vec<u8>, _>(start..end)
        .await
        .expect("scan requests");
    let mut rows = Vec::new();
    while let Some(kv) = iter.next().await.expect("iterate requests") {
        let parsed = parse_concurrency_request_key(&kv.key).expect("parse request key");
        rows.push((kv.key.to_vec(), parsed, kv.value));
    }
    rows
}

/// Re-key `job_id`'s request row as a start-time-ordered row, leaving its
/// value and the requester counter as they are.
async fn rekey_start_time_first(shard: &JobStoreShard, queue: &str, job_id: &str) {
    let rows = request_rows(shard, queue).await;
    let (key, parsed, value) = rows
        .into_iter()
        .find(|(_, parsed, _)| parsed.job_id == job_id)
        .expect("job has a request row");
    let rekeyed = concurrency_request_key_start_time_first(
        TENANT,
        queue,
        parsed.start_time_ms as i64,
        parsed.priority,
        &parsed.job_id,
        parsed.attempt_number,
        &parsed.suffix,
    );
    let mut batch = WriteBatch::new();
    batch.delete(&key);
    batch.put(&rekeyed, &value);
    shard.db().write(batch).await.expect("rekey request row");
}

/// Write a request row for a job that starts an hour from now.
async fn write_future_request_row(shard: &JobStoreShard, queue: &str, key: Vec<u8>, job_id: &str) {
    let parsed = parse_concurrency_request_key(&key).expect("parse request key");
    let action = ConcurrencyAction::EnqueueTask {
        start_time_ms: parsed.start_time_ms as i64,
        priority: parsed.priority,
        job_id: job_id.to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        task_group: TASK_GROUP.to_string(),
        limit_index: 0,
        held_queues: Vec::new(),
        task_id: format!("{job_id}-task"),
        limits: vec![limit(queue)],
    };
    let mut batch = WriteBatch::new();
    batch.put(&key, &encode_concurrency_action(&action));
    shard.db().write(batch).await.expect("write request row");
}

#[silo::test]
async fn higher_priority_waiter_is_granted_before_an_earlier_lower_priority_one() {
    let (_tmp, shard) = open_shard(true).await;
    let queue = "priority-first-q";
    let now = now_ms();
    let holder = take_slot(&shard, queue).await;

    let low = enqueue(&shard, queue, 50, now - 1000).await;
    let high = enqueue(&shard, queue, 5, now).await;

    succeed(&shard, &holder).await;
    let (first, first_task) = grant_one(&shard, queue).await;
    assert_eq!(
        first, high,
        "priority 5 runs ahead of an earlier priority 50"
    );

    succeed(&shard, &first_task).await;
    let (second, _) = grant_one(&shard, queue).await;
    assert_eq!(second, low);
}

#[silo::test]
async fn equal_priority_waiters_are_granted_in_start_time_order() {
    let (_tmp, shard) = open_shard(true).await;
    let queue = "same-priority-q";
    let now = now_ms();
    let holder = take_slot(&shard, queue).await;

    let later = enqueue(&shard, queue, 20, now).await;
    let earlier = enqueue(&shard, queue, 20, now - 1000).await;

    succeed(&shard, &holder).await;
    let (first, first_task) = grant_one(&shard, queue).await;
    assert_eq!(first, earlier);

    succeed(&shard, &first_task).await;
    let (second, _) = grant_one(&shard, queue).await;
    assert_eq!(second, later);
}

#[silo::test]
async fn start_time_ordered_shard_writes_band_zero_rows_and_grants_by_start_time() {
    let (_tmp, shard) = open_shard(false).await;
    let queue = "start-time-first-q";
    let now = now_ms();
    let holder = take_slot(&shard, queue).await;

    let low = enqueue(&shard, queue, 50, now - 1000).await;
    let high = enqueue(&shard, queue, 5, now).await;

    let rows = request_rows(&shard, queue).await;
    assert_eq!(rows.len(), 2);
    for (key, parsed, _) in &rows {
        assert_eq!(parsed.band, 0, "start-time-ordered rows are in band 0");
        assert_eq!(
            key,
            &concurrency_request_key_start_time_first(
                TENANT,
                queue,
                parsed.start_time_ms as i64,
                parsed.priority,
                &parsed.job_id,
                parsed.attempt_number,
                &parsed.suffix,
            )
        );
    }
    assert_eq!(rows[0].1.job_id, low, "rows sort by start time");

    succeed(&shard, &holder).await;
    let (first, first_task) = grant_one(&shard, queue).await;
    assert_eq!(
        first, low,
        "the earlier waiter runs first whatever its priority"
    );

    succeed(&shard, &first_task).await;
    let (second, _) = grant_one(&shard, queue).await;
    assert_eq!(second, high);
}

#[silo::test]
async fn future_dated_rows_are_skipped_without_blocking_lower_bands() {
    let (_tmp, shard) = open_shard(true).await;
    let queue = "future-rows-q";
    let now = now_ms();
    let future = now + 60 * 60 * 1000;
    let holder = take_slot(&shard, queue).await;

    // A future-dated row in band 0 and one in band 3, both ahead of the
    // ready waiter's band.
    write_future_request_row(
        &shard,
        queue,
        concurrency_request_key_start_time_first(TENANT, queue, future, 50, "future-a", 1, "a"),
        "future-a",
    )
    .await;
    write_future_request_row(
        &shard,
        queue,
        concurrency_request_key(TENANT, queue, future, 3, "future-b", 1, "b"),
        "future-b",
    )
    .await;
    let ready = enqueue(&shard, queue, 5, now).await;
    let ready_low = enqueue(&shard, queue, 255, now).await;

    let order: Vec<String> = request_rows(&shard, queue)
        .await
        .into_iter()
        .map(|(_, parsed, _)| parsed.job_id)
        .collect();
    assert_eq!(
        order,
        vec![
            "future-a".to_string(),
            "future-b".to_string(),
            ready.clone(),
            ready_low.clone()
        ]
    );

    succeed(&shard, &holder).await;
    let (first, first_task) = grant_one(&shard, queue).await;
    assert_eq!(first, ready);

    // The last band is reached past future-dated rows too.
    succeed(&shard, &first_task).await;
    let (second, second_task) = grant_one(&shard, queue).await;
    assert_eq!(second, ready_low);

    // With only future-dated rows left a pass grants nothing and keeps them.
    succeed(&shard, &second_task).await;
    let granted = shard.process_concurrency_grants(TENANT, queue, 4).await;
    assert!(granted.is_empty());
    let remaining: Vec<String> = request_rows(&shard, queue)
        .await
        .into_iter()
        .map(|(_, parsed, _)| parsed.job_id)
        .collect();
    assert_eq!(
        remaining,
        vec!["future-a".to_string(), "future-b".to_string()]
    );
}

#[silo::test]
async fn start_time_ordered_rows_are_granted_ahead_of_priority_ordered_rows() {
    let (_tmp, shard) = open_shard(true).await;
    let queue = "mixed-order-q";
    let now = now_ms();
    let holder = take_slot(&shard, queue).await;

    let start_time_ordered = enqueue(&shard, queue, 50, now).await;
    rekey_start_time_first(&shard, queue, &start_time_ordered).await;
    let priority_ordered = enqueue(&shard, queue, 5, now - 1000).await;

    succeed(&shard, &holder).await;
    let (first, first_task) = grant_one(&shard, queue).await;
    assert_eq!(first, start_time_ordered);

    succeed(&shard, &first_task).await;
    let (second, _) = grant_one(&shard, queue).await;
    assert_eq!(second, priority_ordered);
    assert_eq!(
        shard
            .get_concurrency_requester_count(TENANT, queue)
            .await
            .expect("requester count"),
        0
    );
}

#[silo::test]
async fn cancel_deletes_a_waiting_job_request_row_in_either_order() {
    let (_tmp, shard) = open_shard(true).await;
    let queue = "cancel-q";
    let now = now_ms();
    let _holder = take_slot(&shard, queue).await;

    let start_time_ordered = enqueue(&shard, queue, 50, now).await;
    rekey_start_time_first(&shard, queue, &start_time_ordered).await;
    let priority_ordered = enqueue(&shard, queue, 50, now).await;
    assert_eq!(request_rows(&shard, queue).await.len(), 2);

    for (job_id, rows_left) in [(&start_time_ordered, 1), (&priority_ordered, 0)] {
        shard.cancel_job(TENANT, job_id).await.expect("cancel");
        let status = shard
            .get_job_status(TENANT, job_id)
            .await
            .expect("status")
            .expect("job exists");
        assert_eq!(status.kind, JobStatusKind::Cancelled);
        assert_eq!(request_rows(&shard, queue).await.len(), rows_left);
        assert_eq!(
            shard
                .get_concurrency_requester_count(TENANT, queue)
                .await
                .expect("requester count"),
            rows_left as i64
        );
    }
}
