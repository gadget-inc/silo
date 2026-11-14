mod test_helpers;

use std::sync::Arc;

use silo::codec::{encode_concurrency_action, encode_job_status};
use silo::job::{ConcurrencyLimit, JobStatus, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::keys::{
    concurrency_holders_queue_prefix, concurrency_request_key, concurrency_request_prefix,
    concurrency_requester_counter_key, job_status_key,
};
use silo::query::ShardQueryEngine;
use silo::task::ConcurrencyAction;
use test_helpers::*;

#[silo::test]
async fn requester_counter_tracks_enqueue_and_grant() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "counter-q".to_string();

    // First job takes the single slot (holder, no request)
    shard
        .enqueue(
            "-",
            Some("job-1".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 1})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue1");
    let tasks = shard.dequeue("w", "default", 1).await.expect("deq1").tasks;
    assert_eq!(tasks.len(), 1);
    let t1_id = tasks[0].attempt().task_id().to_string();

    // Counter should be 0 (no requesters yet)
    let count = shard
        .get_concurrency_requester_count("-", &queue)
        .await
        .expect("get counter");
    assert_eq!(count, 0, "no requesters after first job gets holder");

    // Second job should become a requester (queue full)
    shard
        .enqueue(
            "-",
            Some("job-2".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 2})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue2");

    let count = shard
        .get_concurrency_requester_count("-", &queue)
        .await
        .expect("get counter");
    assert_eq!(count, 1, "one requester after second enqueue");

    // Third job should also become a requester
    shard
        .enqueue(
            "-",
            Some("job-3".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": 3})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue3");

    let count = shard
        .get_concurrency_requester_count("-", &queue)
        .await
        .expect("get counter");
    assert_eq!(count, 2, "two requesters after third enqueue");

    // Complete first task; this should trigger grant and decrement counter
    shard
        .report_attempt_outcome(&t1_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report");

    // Poll until counter decrements (grant happens async)
    let final_count = poll_until(
        || async {
            shard
                .get_concurrency_requester_count("-", &queue)
                .await
                .unwrap_or(99)
        },
        |c| *c == 1,
        5000,
    )
    .await;
    assert_eq!(final_count, 1, "counter should decrement after grant");
}

#[silo::test]
async fn requester_counter_tracks_cancel() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "cancel-counter-q".to_string();

    // First job takes the slot
    shard
        .enqueue(
            "-",
            Some("holder-job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue holder");
    let tasks = shard.dequeue("w", "default", 1).await.expect("deq").tasks;
    assert_eq!(tasks.len(), 1);

    // Second job becomes a requester
    let waiter_id = shard
        .enqueue(
            "-",
            Some("waiter-job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue waiter");

    let count = shard
        .get_concurrency_requester_count("-", &queue)
        .await
        .expect("get counter");
    assert_eq!(count, 1, "one requester before cancel");

    // Cancel the waiting job
    shard
        .cancel_job("-", &waiter_id)
        .await
        .expect("cancel waiter");

    let count = shard
        .get_concurrency_requester_count("-", &queue)
        .await
        .expect("get counter after cancel");
    assert_eq!(count, 0, "counter should be 0 after cancelling requester");
}

#[silo::test]
async fn query_count_requesters_uses_counter() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "query-counter-q".to_string();

    // First job takes slot
    shard
        .enqueue(
            "-",
            Some("holder-job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue holder");
    shard.dequeue("w", "default", 1).await.expect("deq");

    // Enqueue 5 waiters
    for i in 0..5 {
        shard
            .enqueue(
                "-",
                Some(format!("waiter-{}", i)),
                10,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue waiter");
    }

    // Query using COUNT(*) with entry_type = 'requester'
    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let batches = sql
        .sql("SELECT COUNT(*) FROM queues WHERE tenant = '-' AND queue_name = 'query-counter-q' AND entry_type = 'requester'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    assert!(!batches.is_empty());
    let count_col = batches[0].column(0);
    let count_arr = count_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("count column");
    assert_eq!(count_arr.value(0), 5, "COUNT(*) should return 5 requesters");

    // Also verify holder count query still works (not optimized, goes through scan)
    let batches = sql
        .sql("SELECT COUNT(*) FROM queues WHERE tenant = '-' AND queue_name = 'query-counter-q' AND entry_type = 'holder'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    let count_col = batches[0].column(0);
    let count_arr = count_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("count column");
    assert_eq!(count_arr.value(0), 1, "COUNT(*) should return 1 holder");

    // Verify total count (no entry_type filter) still works correctly
    let batches = sql
        .sql("SELECT COUNT(*) FROM queues WHERE tenant = '-' AND queue_name = 'query-counter-q'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    let count_col = batches[0].column(0);
    let count_arr = count_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("count column");
    assert_eq!(
        count_arr.value(0),
        6,
        "COUNT(*) without entry_type should return all entries"
    );
}

/// Helper: write a fake concurrency request key and bump the requester counter.
/// The request references a job that does NOT exist in the DB, so the grant scanner
/// will treat it as stale (missing job status) and clean it up.
async fn inject_fake_request(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    queue: &str,
    fake_job_id: &str,
    attempt: u32,
    start_time_ms: i64,
) {
    let db = shard.db();
    let req_key = concurrency_request_key(
        tenant,
        queue,
        start_time_ms,
        10,
        fake_job_id,
        attempt,
        "fake0001",
    );
    let action = ConcurrencyAction::EnqueueTask {
        start_time_ms,
        priority: 10,
        job_id: fake_job_id.to_string(),
        attempt_number: attempt,
        relative_attempt_number: attempt,
        task_group: "default".to_string(),
        limit_index: 0,
        held_queues: Vec::new(),
        task_id: format!("{}:{}:fake", fake_job_id, attempt),
        limits: Vec::new(),
    };
    let action_val = encode_concurrency_action(&action);
    db.put(&req_key, &action_val)
        .await
        .expect("put fake request");

    // Bump the counter to reflect the injected request
    let counter_key = concurrency_requester_counter_key(tenant, queue);
    db.merge(counter_key, 1i64.to_le_bytes())
        .await
        .expect("merge counter");
}

/// The grant scanner should detect and clean up request keys whose job status
/// is missing (deleted / never existed) and decrement the requester counter.
#[silo::test]
async fn requester_counter_decrements_on_stale_request_missing_status() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "stale-missing-q".to_string();
    let tenant = "-";

    // Job A takes the single slot
    shard
        .enqueue(
            tenant,
            Some("holder-job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue holder");
    let tasks = shard.dequeue("w", "default", 1).await.expect("deq").tasks;
    assert_eq!(tasks.len(), 1);
    let holder_task_id = tasks[0].attempt().task_id().to_string();

    // Inject a fake request for a job that doesn't exist
    inject_fake_request(&shard, tenant, &queue, "ghost-job", 1, now).await;

    let count = shard
        .get_concurrency_requester_count(tenant, &queue)
        .await
        .expect("counter");
    assert_eq!(count, 1, "counter should reflect the injected fake request");

    // Complete the holder to trigger the grant scanner
    shard
        .report_attempt_outcome(&holder_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report");

    // The grant scanner should find the fake request, see no job status, delete it,
    // and decrement the counter.
    let final_count = poll_until(
        || async {
            shard
                .get_concurrency_requester_count(tenant, &queue)
                .await
                .unwrap_or(99)
        },
        |c| *c == 0,
        5000,
    )
    .await;
    assert_eq!(
        final_count, 0,
        "counter should be 0 after stale request cleanup"
    );

    // The stale request key should have been deleted
    let remaining_requests = count_concurrency_requests(shard.db()).await;
    assert_eq!(remaining_requests, 0, "stale request key should be deleted");
}

/// The grant scanner should detect and clean up request keys for jobs whose
/// status has changed away from Scheduled (e.g. to Failed), and decrement the
/// requester counter.
#[silo::test]
async fn requester_counter_decrements_on_stale_request_wrong_status() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "stale-status-q".to_string();
    let tenant = "-";

    // Job A takes the single slot
    shard
        .enqueue(
            tenant,
            Some("holder-a".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue holder A");
    let tasks = shard.dequeue("w", "default", 1).await.expect("deq A").tasks;
    assert_eq!(tasks.len(), 1);
    let holder_a_task = tasks[0].attempt().task_id().to_string();

    // Job B enqueues and becomes a real requester (queue full)
    shard
        .enqueue(
            tenant,
            Some("job-b".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue B");

    assert_eq!(
        shard
            .get_concurrency_requester_count(tenant, &queue)
            .await
            .expect("counter"),
        1,
        "one real requester"
    );

    // Cancel job B — this cleans up its request key and decrements the counter
    shard.cancel_job(tenant, "job-b").await.expect("cancel B");

    assert_eq!(
        shard
            .get_concurrency_requester_count(tenant, &queue)
            .await
            .expect("counter"),
        0,
        "counter 0 after cancel"
    );

    // Now inject a fake request that references a job whose status is not Scheduled.
    // We use "job-b" which we just cancelled — its status is now Cancelled.
    inject_fake_request(&shard, tenant, &queue, "job-b", 99, now).await;

    assert_eq!(
        shard
            .get_concurrency_requester_count(tenant, &queue)
            .await
            .expect("counter"),
        1,
        "counter reflects injected stale request"
    );

    // Complete holder A to trigger the grant scanner
    shard
        .report_attempt_outcome(&holder_a_task, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report A");

    // The grant scanner should find the stale request for job-b (status=Cancelled,
    // attempt mismatch), delete it, and decrement the counter.
    let final_count = poll_until(
        || async {
            shard
                .get_concurrency_requester_count(tenant, &queue)
                .await
                .unwrap_or(99)
        },
        |c| *c == 0,
        5000,
    )
    .await;
    assert_eq!(
        final_count, 0,
        "counter should be 0 after stale request cleanup"
    );

    let remaining_requests = count_concurrency_requests(shard.db()).await;
    assert_eq!(remaining_requests, 0, "stale request key should be deleted");
}

/// When multiple stale requests exist, the grant scanner should clean them all up
/// and decrement the counter by the correct total amount.
#[silo::test]
async fn requester_counter_decrements_for_multiple_stale_requests() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "stale-multi-q".to_string();
    let tenant = "-";

    // Job A takes the single slot
    shard
        .enqueue(
            tenant,
            Some("holder-job".to_string()),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.clone(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue holder");
    let tasks = shard.dequeue("w", "default", 1).await.expect("deq").tasks;
    assert_eq!(tasks.len(), 1);
    let holder_task_id = tasks[0].attempt().task_id().to_string();

    // Inject 3 fake requests for non-existent jobs
    for i in 0..3 {
        inject_fake_request(&shard, tenant, &queue, &format!("ghost-{i}"), 1, now).await;
    }

    assert_eq!(
        shard
            .get_concurrency_requester_count(tenant, &queue)
            .await
            .expect("counter"),
        3,
        "counter should reflect 3 injected fake requests"
    );

    // Complete the holder to trigger grant scanner
    shard
        .report_attempt_outcome(&holder_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report");

    // All stale requests should be cleaned up
    let final_count = poll_until(
        || async {
            shard
                .get_concurrency_requester_count(tenant, &queue)
                .await
                .unwrap_or(99)
        },
        |c| *c == 0,
        5000,
    )
    .await;
    assert_eq!(
        final_count, 0,
        "counter should be 0 after all stale requests cleaned"
    );

    let remaining_requests = count_concurrency_requests(shard.db()).await;
    assert_eq!(
        remaining_requests, 0,
        "all stale request keys should be deleted"
    );
}

/// Fill a single-slot queue with one holder and park `waiters` requests
/// behind it. Returns the holder's task id.
async fn park_requests(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    queue: &str,
    waiters: usize,
) -> String {
    let now = now_ms();
    let limit = || {
        vec![Limit::Concurrency(ConcurrencyLimit {
            key: queue.to_string(),
            max_concurrency: 1,
        })]
    };
    shard
        .enqueue(
            tenant,
            Some(format!("{queue}-holder")),
            10,
            now,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            limit(),
            None,
            "default",
        )
        .await
        .expect("enqueue holder");
    let tasks = shard.dequeue("w", "default", 1).await.expect("deq").tasks;
    assert_eq!(tasks.len(), 1);
    let holder_task_id = tasks[0].attempt().task_id().to_string();

    for i in 0..waiters {
        shard
            .enqueue(
                tenant,
                Some(format!("{queue}-waiter-{i}")),
                10,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                limit(),
                None,
                "default",
            )
            .await
            .expect("enqueue waiter");
    }
    holder_task_id
}

async fn drift_requester_counter(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    queue: &str,
    delta: i64,
) {
    shard
        .db()
        .merge(
            concurrency_requester_counter_key(tenant, queue),
            delta.to_le_bytes(),
        )
        .await
        .expect("drift counter");
}

async fn requester_count(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    queue: &str,
) -> i64 {
    shard
        .get_concurrency_requester_count(tenant, queue)
        .await
        .expect("get counter")
}

#[silo::test]
async fn recount_corrects_negative_requester_counter() {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "recount-negative-q";
    park_requests(&shard, "-", queue, 3).await;
    assert_eq!(requester_count(&shard, "-", queue).await, 3);

    drift_requester_counter(&shard, "-", queue, -5).await;
    assert_eq!(requester_count(&shard, "-", queue).await, -2);

    let delta = shard
        .recount_requester_counter("-", queue)
        .await
        .expect("recount");
    assert_eq!(delta, 5, "delta restores the live row count");
    assert_eq!(requester_count(&shard, "-", queue).await, 3);
}

#[silo::test]
async fn recount_corrects_too_high_requester_counter() {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "recount-high-q";
    park_requests(&shard, "-", queue, 2).await;

    drift_requester_counter(&shard, "-", queue, 4).await;
    assert_eq!(requester_count(&shard, "-", queue).await, 6);

    let delta = shard
        .recount_requester_counter("-", queue)
        .await
        .expect("recount");
    assert_eq!(delta, -4);
    assert_eq!(requester_count(&shard, "-", queue).await, 2);
}

#[silo::test]
async fn recount_leaves_correct_requester_counter_alone() {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "recount-correct-q";
    park_requests(&shard, "-", queue, 2).await;

    let delta = shard
        .recount_requester_counter("-", queue)
        .await
        .expect("recount");
    assert_eq!(delta, 0, "a correct counter receives no merge");
    assert_eq!(requester_count(&shard, "-", queue).await, 2);
}

#[silo::test]
async fn recount_zeroes_counter_row_with_no_request_rows() {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "recount-empty-q";
    // A counter row with no request rows behind it.
    drift_requester_counter(&shard, "-", queue, 7).await;
    assert_eq!(requester_count(&shard, "-", queue).await, 7);

    let delta = shard
        .recount_requester_counter("-", queue)
        .await
        .expect("recount");
    assert_eq!(delta, -7);
    assert_eq!(requester_count(&shard, "-", queue).await, 0);
}

/// A request enqueued between the recount's snapshot and its commit is not
/// lost: the recount merges a delta computed at the snapshot, and the
/// concurrent enqueue's own `+1` merge lands on top of it.
#[silo::test]
async fn recount_keeps_request_enqueued_after_snapshot() {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "recount-concurrent-q";
    let now = now_ms();
    park_requests(&shard, "-", queue, 2).await;
    drift_requester_counter(&shard, "-", queue, -3).await;
    assert_eq!(requester_count(&shard, "-", queue).await, -1);

    let delta = shard
        .recount_requester_counter_for_test(
            "-",
            queue,
            inject_fake_request(&shard, "-", queue, "late-job", 1, now),
        )
        .await
        .expect("recount");
    assert_eq!(delta, 3, "delta is computed at the snapshot: 2 rows - (-1)");

    let rows = count_concurrency_requests(shard.db()).await;
    assert_eq!(rows, 3, "two parked waiters plus the late request");
    assert_eq!(
        requester_count(&shard, "-", queue).await,
        3,
        "counter equals the final row count"
    );
}

/// A second recount of the same queue started while one is between its
/// snapshot and its commit waits for the first, then finds nothing to
/// correct, so the drift is corrected once rather than twice.
#[silo::test]
async fn overlapping_recounts_apply_the_correction_once() {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "recount-overlap-q";
    park_requests(&shard, "-", queue, 2).await;
    drift_requester_counter(&shard, "-", queue, -3).await;
    assert_eq!(requester_count(&shard, "-", queue).await, -1);

    let mut second_handle: Option<tokio::task::JoinHandle<i64>> = None;
    let first_delta = shard
        .recount_requester_counter_for_test("-", queue, async {
            let shard = Arc::clone(&shard);
            second_handle = Some(tokio::spawn(async move {
                shard
                    .recount_requester_counter("-", "recount-overlap-q")
                    .await
                    .expect("second recount")
            }));
            // Give the second recount time to run; it must wait for the
            // first to commit instead.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        })
        .await
        .expect("first recount");
    let second_delta = second_handle
        .expect("hook ran")
        .await
        .expect("join second recount");

    assert_eq!(first_delta, 3);
    assert_eq!(
        second_delta, 0,
        "the second recount saw the corrected counter"
    );
    assert_eq!(requester_count(&shard, "-", queue).await, 2);
}

/// `queue_counts` hides a counter at or below zero. One reconcile pass
/// restores the true requester count for the queue.
#[silo::test]
async fn queue_counts_reports_true_requesters_after_reconcile() {
    let (_tmp, shard) = open_temp_shard().await;
    let queue = "recount-queue-counts-q";
    park_requests(&shard, "-", queue, 4).await;
    drift_requester_counter(&shard, "-", queue, -9).await;
    assert_eq!(requester_count(&shard, "-", queue).await, -5);

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");

    assert_eq!(
        queue_counts_requesters(&sql, queue).await,
        0,
        "a negative counter reads as zero before the recount"
    );

    let summary = shard
        .reconcile_counters(&silo::shard_range::ShardRange::full())
        .await;
    assert_eq!(summary.failed, 0);
    assert_eq!(summary.requester_counters_corrected, 1);

    assert_eq!(queue_counts_requesters(&sql, queue).await, 4);
}

/// The `requesters` column of `queue_counts` for one queue of tenant `-`,
/// or 0 when the table has no row for it.
async fn queue_counts_requesters(sql: &ShardQueryEngine, queue: &str) -> i64 {
    let batches = sql
        .sql(&format!(
            "SELECT requesters FROM queue_counts WHERE tenant = '-' AND queue_name = '{queue}'"
        ))
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    batches
        .iter()
        .filter(|b| b.num_rows() > 0)
        .map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .expect("requesters column")
                .value(0)
        })
        .next()
        .unwrap_or(0)
}

fn concurrency_limits(keys: &[&str]) -> Vec<Limit> {
    keys.iter()
        .map(|key| {
            Limit::Concurrency(ConcurrencyLimit {
                key: key.to_string(),
                max_concurrency: 1,
            })
        })
        .collect()
}

/// Write a request row for `job_id`'s attempt 1 on `queue` carrying
/// `task_id` as the chain task id and `limits` as the chain, and bump the
/// queue's requester counter for it. Returns the row's key.
async fn inject_request_row(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    queue: &str,
    job_id: &str,
    task_id: &str,
    start_time_ms: i64,
    limits: &[Limit],
) -> Vec<u8> {
    let action = ConcurrencyAction::EnqueueTask {
        start_time_ms,
        priority: 10,
        job_id: job_id.to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        task_group: "default".to_string(),
        limit_index: 0,
        held_queues: Vec::new(),
        task_id: task_id.to_string(),
        limits: limits.to_vec(),
    };
    let key = concurrency_request_key(tenant, queue, start_time_ms, 10, job_id, 1, "seed0001");
    let db = shard.db();
    db.put(&key, &encode_concurrency_action(&action))
        .await
        .expect("put request row");
    db.merge(
        concurrency_requester_counter_key(tenant, queue),
        1i64.to_le_bytes(),
    )
    .await
    .expect("merge counter");
    key
}

async fn write_scheduled_status(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    job_id: &str,
    changed_at_ms: i64,
    start_time_ms: i64,
) {
    shard
        .db()
        .put(
            &job_status_key(tenant, job_id),
            &encode_job_status(&JobStatus::scheduled(changed_at_ms, start_time_ms, 1)),
        )
        .await
        .expect("put status");
}

async fn queue_request_rows(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    queue: &str,
) -> usize {
    count_with_binary_prefix(shard.db(), &concurrency_request_prefix(tenant, queue)).await
}

async fn queue_holder_rows(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    queue: &str,
) -> usize {
    count_with_binary_prefix(shard.db(), &concurrency_holders_queue_prefix(tenant, queue)).await
}

/// Install a scan hook that runs `f` once, in the window after the scanner
/// has read its batch of request rows and before it acts on them.
fn install_scan_hook<F, Fut>(shard: &Arc<silo::job_store_shard::JobStoreShard>, f: F)
where
    F: Fn(Arc<silo::job_store_shard::JobStoreShard>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let weak = Arc::downgrade(shard);
    let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let f = Arc::new(f);
    shard.set_grant_scan_hook_for_test(Some(Arc::new(move || {
        let weak = weak.clone();
        let fired = Arc::clone(&fired);
        let f = Arc::clone(&f);
        Box::pin(async move {
            if fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            if let Some(shard) = weak.upgrade() {
                f(shard).await;
            }
        })
    })));
}

/// A request row the scanner read from its snapshot and whose job status was
/// written after the pass began is re-read before it is reserved. When the
/// row is gone, the candidate is skipped: no holder, no chain resume and no
/// counter change, because the path that deleted the row already decremented
/// the counter.
#[silo::test]
async fn scanner_skips_candidate_whose_row_vanished_after_pass_began() {
    let (_tmp, shard) = open_temp_shard().await;
    shard.stop_grant_scanner();
    let tenant = "-";
    let queue = "vanished-q";
    let limits = concurrency_limits(&[queue]);
    let start = now_ms() - 1_000;

    let request_key =
        inject_request_row(&shard, tenant, queue, "job-v", "task-old", start, &limits).await;
    write_scheduled_status(&shard, tenant, "job-v", start, start).await;
    assert_eq!(requester_count(&shard, tenant, queue).await, 1);

    install_scan_hook(&shard, move |shard| {
        let request_key = request_key.clone();
        async move {
            // What a cancel does: delete the row and decrement for it.
            shard.db().delete(&request_key).await.expect("delete row");
            shard
                .db()
                .merge(
                    concurrency_requester_counter_key("-", "vanished-q"),
                    (-1i64).to_le_bytes(),
                )
                .await
                .expect("merge counter");
            // What a re-enqueue under the same id does: a fresh Scheduled
            // status on attempt 1, written after the pass began.
            let later = now_ms() + 60_000;
            write_scheduled_status(&shard, "-", "job-v", later, later).await;
        }
    });

    let granted = shard.process_concurrency_grants(tenant, queue, 1).await;
    shard.set_grant_scan_hook_for_test(None);
    assert!(
        granted.is_empty(),
        "the vanished request must not be granted"
    );

    assert_eq!(queue_holder_rows(&shard, tenant, queue).await, 0);
    assert_eq!(shard.concurrency_holder_count(tenant, queue), 0);
    assert_eq!(queue_request_rows(&shard, tenant, queue).await, 0);
    assert_eq!(
        requester_count(&shard, tenant, queue).await,
        0,
        "the counter is not decremented a second time"
    );
    assert_single_live_terminal_task_row_per_attempt(shard.db()).await;
}

/// A candidate whose status changed after the pass began but whose request
/// row is still present is granted normally.
#[silo::test]
async fn scanner_grants_candidate_whose_status_changed_but_row_remains() {
    let (_tmp, shard) = open_temp_shard().await;
    shard.stop_grant_scanner();
    let tenant = "-";
    let queue = "status-changed-q";
    let limits = concurrency_limits(&[queue]);
    let start = now_ms() - 1_000;

    inject_request_row(&shard, tenant, queue, "job-s", "task-s", start, &limits).await;
    write_scheduled_status(&shard, tenant, "job-s", start, start).await;

    install_scan_hook(&shard, |shard| async move {
        let later = now_ms() + 60_000;
        write_scheduled_status(&shard, "-", "job-s", later, later).await;
    });

    let granted = shard.process_concurrency_grants(tenant, queue, 1).await;
    shard.set_grant_scan_hook_for_test(None);
    assert_eq!(granted, vec!["default".to_string()]);

    assert_eq!(queue_holder_rows(&shard, tenant, queue).await, 1);
    assert_eq!(shard.concurrency_holder_count(tenant, queue), 1);
    assert_eq!(queue_request_rows(&shard, tenant, queue).await, 0);
    assert_eq!(requester_count(&shard, tenant, queue).await, 0);
}

/// A candidate whose status predates the pass is granted.
#[silo::test]
async fn scanner_grants_candidate_whose_status_predates_pass() {
    let (_tmp, shard) = open_temp_shard().await;
    shard.stop_grant_scanner();
    let tenant = "-";
    let queue = "status-old-q";

    let holder_task_id = park_requests(&shard, tenant, queue, 1).await;
    assert_eq!(requester_count(&shard, tenant, queue).await, 1);
    shard
        .report_attempt_outcome(&holder_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("finish holder");

    let granted = shard.process_concurrency_grants(tenant, queue, 1).await;
    assert_eq!(granted, vec!["default".to_string()]);

    assert_eq!(queue_holder_rows(&shard, tenant, queue).await, 1);
    assert_eq!(queue_request_rows(&shard, tenant, queue).await, 0);
    assert_eq!(requester_count(&shard, tenant, queue).await, 0);
}

/// A job parked on a saturated queue is cancelled, deleted and enqueued again
/// under the same id, with its queues in the other order, while a pass holds
/// its old request row. After the pass the job holds no slot from the old
/// chain, nothing from the old chain is parked downstream, and the queue's
/// requester counter equals its live rows.
#[silo::test]
async fn replaced_job_holds_no_slot_from_its_old_chain() {
    let (_tmp, shard) = open_temp_shard().await;
    shard.stop_grant_scanner();
    let tenant = "-";
    let platform = "inc-platform";
    let user = "inc-user";
    let now = now_ms();

    let enqueue = |id: &str, limits: Vec<Limit>| {
        let id = id.to_string();
        let shard = Arc::clone(&shard);
        async move {
            shard
                .enqueue(
                    "-",
                    Some(id),
                    10,
                    now,
                    None,
                    test_helpers::msgpack_payload(&serde_json::json!({})),
                    limits,
                    None,
                    "default",
                )
                .await
                .expect("enqueue")
        }
    };

    // Saturate both single-slot queues, dequeuing each holder as it lands.
    enqueue("user-holder", concurrency_limits(&[user])).await;
    let tasks = shard.dequeue("w", "default", 1).await.expect("deq").tasks;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].attempt().job_id(), "user-holder");
    enqueue("platform-holder", concurrency_limits(&[platform])).await;
    let tasks = shard.dequeue("w", "default", 1).await.expect("deq").tasks;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].attempt().job_id(), "platform-holder");
    let platform_task = tasks[0].attempt().task_id().to_string();

    // The job parks on the platform queue, first hop of its chain.
    enqueue("job-b", concurrency_limits(&[platform, user])).await;
    assert_eq!(queue_request_rows(&shard, tenant, platform).await, 1);
    assert_eq!(requester_count(&shard, tenant, platform).await, 1);

    // The platform slot frees; the scanner is stopped, so nothing is granted yet.
    shard
        .report_attempt_outcome(&platform_task, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("finish platform holder");
    assert_eq!(shard.concurrency_holder_count(tenant, platform), 0);

    // While the pass holds the old row: cancel, delete, enqueue again under
    // the same id with the queues reordered. The user queue is full, so the
    // new chain parks there.
    install_scan_hook(&shard, |shard| async move {
        shard.cancel_job("-", "job-b").await.expect("cancel");
        shard.delete_job("-", "job-b").await.expect("delete");
        shard
            .enqueue(
                "-",
                Some("job-b".to_string()),
                10,
                now_ms(),
                None,
                test_helpers::msgpack_payload(&serde_json::json!({})),
                concurrency_limits(&["inc-user", "inc-platform"]),
                None,
                "default",
            )
            .await
            .expect("re-enqueue");
    });

    let granted = shard.process_concurrency_grants(tenant, platform, 1).await;
    shard.set_grant_scan_hook_for_test(None);
    assert!(
        granted.is_empty(),
        "the old chain's row must not be granted"
    );

    assert_eq!(
        queue_holder_rows(&shard, tenant, platform).await,
        0,
        "no holder from the old chain"
    );
    assert_eq!(shard.concurrency_holder_count(tenant, platform), 0);
    assert_eq!(queue_request_rows(&shard, tenant, platform).await, 0);
    assert_eq!(
        requester_count(&shard, tenant, platform).await,
        0,
        "platform counter equals its live rows"
    );
    assert_eq!(
        queue_request_rows(&shard, tenant, user).await,
        1,
        "only the new chain is parked on the user queue"
    );
    assert_eq!(requester_count(&shard, tenant, user).await, 1);
    assert_single_live_terminal_task_row_per_attempt(shard.db()).await;
}

/// A waiting job is cancelled after a scanner pass has read its request row
/// and before the pass looks at the job's status. The cancel deletes the row
/// and decrements the counter for it; the pass then finds a status that no
/// longer wants the request, and must not decrement for a row that is gone.
#[silo::test]
async fn scanner_leaves_counter_alone_for_request_cancelled_after_pass_began() {
    let (_tmp, shard) = open_temp_shard().await;
    shard.stop_grant_scanner();
    let tenant = "-";
    let queue = "cancel-mid-pass-q";
    let holder_task_id = park_requests(&shard, tenant, queue, 2).await;
    assert_eq!(requester_count(&shard, tenant, queue).await, 2);

    // Free the slot so the pass scans the requests instead of stopping at
    // the at-capacity check.
    shard
        .report_attempt_outcome(&holder_task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("finish holder");

    install_scan_hook(&shard, |shard| async move {
        shard
            .cancel_job("-", "cancel-mid-pass-q-waiter-0")
            .await
            .expect("cancel waiter");
    });

    let granted = shard.process_concurrency_grants(tenant, queue, 2).await;
    shard.set_grant_scan_hook_for_test(None);
    assert_eq!(
        granted,
        vec!["default".to_string()],
        "the waiter that was not cancelled takes the free slot"
    );

    assert_eq!(queue_request_rows(&shard, tenant, queue).await, 0);
    assert_eq!(
        requester_count(&shard, tenant, queue).await,
        0,
        "the cancelled request is decremented for once, by the cancel"
    );
}

/// How a cancel started inside a scanner pass stood when the pass moved on.
enum CancelInPass {
    Finished(Result<(), silo::job_store_shard::JobStoreShardError>),
    Waiting(tokio::task::JoinHandle<Result<(), silo::job_store_shard::JobStoreShardError>>),
}

/// A chain resumer that starts a cancel of `job_id` the first time the
/// scanner resumes a chain, and gives the cancel a head start before the
/// grant goes on. By then the pass has checked the status of every
/// candidate, so the cancel arrives between a candidate's status check and
/// the commit of its grant.
struct CancelDuringPass {
    inner: Arc<dyn silo::concurrency::LimitChainResumer>,
    shard: std::sync::Weak<silo::job_store_shard::JobStoreShard>,
    job_id: &'static str,
    cancel: std::sync::Mutex<Option<CancelInPass>>,
}

#[async_trait::async_trait]
impl silo::concurrency::LimitChainResumer for CancelDuringPass {
    async fn resume_chain(
        &self,
        batch: &mut slatedb::WriteBatch,
        scheduled_refreshes: &mut silo::job_store_shard::ScheduledRefreshes,
        params: silo::concurrency::ResumeChainParams,
    ) -> Result<Vec<(String, String)>, silo::concurrency::ConcurrencyError> {
        let already_started = self.cancel.lock().unwrap().is_some();
        if !already_started && let Some(shard) = self.shard.upgrade() {
            let job_id = self.job_id;
            let mut handle = tokio::spawn(async move { shard.cancel_job("-", job_id).await });
            let head_start = std::time::Duration::from_millis(250);
            let state = match tokio::time::timeout(head_start, &mut handle).await {
                Ok(joined) => CancelInPass::Finished(joined.expect("cancel task")),
                Err(_) => CancelInPass::Waiting(handle),
            };
            *self.cancel.lock().unwrap() = Some(state);
        }
        self.inner
            .resume_chain(batch, scheduled_refreshes, params)
            .await
    }

    fn tasks_written(&self, written: &[(String, i64)]) {
        self.inner.tasks_written(written)
    }

    fn expect_tasks(&self, written: &[(String, i64)]) {
        self.inner.expect_tasks(written)
    }

    fn refresh_index_rows_committed(&self, task_groups: &[String]) {
        self.inner.refresh_index_rows_committed(task_groups)
    }
}

async fn enqueue_on_two_slot_queue(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    queue: &str,
    job_id: &str,
) {
    shard
        .enqueue(
            tenant,
            Some(job_id.to_string()),
            10,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![Limit::Concurrency(ConcurrencyLimit {
                key: queue.to_string(),
                max_concurrency: 2,
            })],
            None,
            "default",
        )
        .await
        .expect("enqueue");
}

/// A cancel arrives after a scanner pass has checked its job's status and
/// before the pass commits the grant. The pass grants the request and
/// decrements for it; the cancel then finds the granted task and takes it
/// and its holder back. The request row is decremented for once, and the
/// cancelled job keeps no slot.
#[silo::test]
async fn cancel_arriving_mid_pass_undoes_the_grant_and_decrements_once() {
    let (_tmp, shard) = open_temp_shard().await;
    shard.stop_grant_scanner();
    let tenant = "-";
    let queue = "cancel-before-commit-q";

    for holder in ["holder-0", "holder-1"] {
        enqueue_on_two_slot_queue(&shard, tenant, queue, holder).await;
    }
    let holder_task_ids = dequeue_task_ids_until(&shard, "w", "default", 2).await;
    for waiter in ["waiter-0", "waiter-1"] {
        enqueue_on_two_slot_queue(&shard, tenant, queue, waiter).await;
    }
    assert_eq!(requester_count(&shard, tenant, queue).await, 2);
    for task_id in &holder_task_ids {
        shard
            .report_attempt_outcome(task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("finish holder");
    }

    let granted = run_pass_with_cancel_mid_pass(&shard, queue, 2, "waiter-1").await;
    assert_eq!(
        granted.len(),
        2,
        "both waiters passed their status check before the cancel arrived"
    );

    assert_eq!(queue_request_rows(&shard, tenant, queue).await, 0);
    assert_eq!(
        requester_count(&shard, tenant, queue).await,
        0,
        "each request row is decremented for once"
    );
    let status = shard
        .get_job_status(tenant, "waiter-1")
        .await
        .expect("get status")
        .expect("status exists");
    assert_eq!(status.kind, silo::job::JobStatusKind::Cancelled);
    assert_eq!(
        queue_holder_rows(&shard, tenant, queue).await,
        1,
        "the cancelled job's holder row is gone"
    );
    assert_eq!(shard.concurrency_holder_count(tenant, queue), 1);
    assert_eq!(
        shard.request_row_lock_count_for_test(),
        0,
        "no lock entry outlives the pass and the cancel"
    );
}

/// Run one scanner pass over `queue` for up to `count` grants, start a
/// cancel of `job_id` once the pass has checked its candidates' statuses,
/// and wait for the cancel to succeed. Returns the pass's grants.
async fn run_pass_with_cancel_mid_pass(
    shard: &Arc<silo::job_store_shard::JobStoreShard>,
    queue: &str,
    count: u32,
    job_id: &'static str,
) -> Vec<String> {
    let resumer = Arc::new(CancelDuringPass {
        inner: shard
            .take_chain_resumer_for_test()
            .expect("resumer installed at open"),
        shard: Arc::downgrade(shard),
        job_id,
        cancel: std::sync::Mutex::new(None),
    });
    shard.set_chain_resumer_for_test(resumer.clone());

    let granted = shard.process_concurrency_grants("-", queue, count).await;

    let cancel = resumer
        .cancel
        .lock()
        .unwrap()
        .take()
        .expect("cancel started");
    let cancelled = match cancel {
        CancelInPass::Finished(result) => result,
        CancelInPass::Waiting(handle) => handle.await.expect("cancel task"),
    };
    cancelled.expect("cancel succeeds");
    shard.set_chain_resumer_for_test(Arc::clone(&resumer.inner));
    granted
}

fn one_slot(queue: &str) -> Limit {
    Limit::Concurrency(ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency: 1,
    })
}

fn floating_one_slot(queue: &str) -> Limit {
    Limit::FloatingConcurrency(silo::job::FloatingConcurrencyLimit {
        key: queue.to_string(),
        default_max_concurrency: 1,
        refresh_interval_ms: 60_000,
        metadata: vec![],
    })
}

async fn enqueue_with_limits(
    shard: &silo::job_store_shard::JobStoreShard,
    job_id: &str,
    limits: Vec<Limit>,
    task_group: &str,
) {
    shard
        .enqueue(
            "-",
            Some(job_id.to_string()),
            10,
            now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            limits,
            None,
            task_group,
        )
        .await
        .expect("enqueue");
}

/// Park the job `waiter` behind a holder on the one-slot `queue`, then
/// finish the holder with the grant scanner stopped, so the next scanner
/// pass over `queue` grants `waiter`. `waiter`'s limits are `queue`
/// followed by `rest`.
async fn park_waiter_and_free_its_slot(
    shard: &silo::job_store_shard::JobStoreShard,
    queue: &str,
    waiter: &str,
    rest: Vec<Limit>,
) {
    enqueue_with_limits(
        shard,
        &format!("{queue}-holder"),
        vec![one_slot(queue)],
        "default",
    )
    .await;
    let holder_task_ids = dequeue_task_ids_until(shard, "w", "default", 1).await;
    let mut limits = vec![one_slot(queue)];
    limits.extend(rest);
    enqueue_with_limits(shard, waiter, limits, "default").await;
    assert_eq!(requester_count(shard, "-", queue).await, 1);
    shard
        .report_attempt_outcome(
            &holder_task_ids[0],
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("finish holder");
}

/// `queue` has no request row, a requester counter of zero, and `holders`
/// holders both durably and in memory.
async fn assert_queue_settled(
    shard: &silo::job_store_shard::JobStoreShard,
    queue: &str,
    holders: usize,
) {
    assert_eq!(
        queue_request_rows(shard, "-", queue).await,
        0,
        "{queue}: no request row is left"
    );
    assert_eq!(
        requester_count(shard, "-", queue).await,
        0,
        "{queue}: each request row is decremented for once"
    );
    assert_eq!(
        queue_holder_rows(shard, "-", queue).await,
        holders,
        "{queue}: durable holders"
    );
    assert_eq!(
        shard.concurrency_holder_count("-", queue),
        holders,
        "{queue}: in-memory holders"
    );
}

/// `job_id` is cancelled, its task group has no task row left, and no
/// request-row lock entry outlives the pass and the cancel.
async fn assert_cancelled_with_nothing_pending(
    shard: &silo::job_store_shard::JobStoreShard,
    job_id: &str,
) {
    let status = shard
        .get_job_status("-", job_id)
        .await
        .expect("get status")
        .expect("status exists");
    assert_eq!(status.kind, silo::job::JobStatusKind::Cancelled);
    let tasks = shard.peek_tasks("default", 50).await.expect("peek tasks");
    assert!(
        tasks.is_empty(),
        "the cancelled job's chain left a task row: {tasks:?}"
    );
    assert_eq!(shard.request_row_lock_count_for_test(), 0);
}

/// The job's second queue is full, so the pass's grant on the first queue
/// parks a new request at the second while holding the first. A cancel that
/// arrived mid-pass removes that request, decrements the second queue's
/// counter for it, and releases the first queue's holder.
#[silo::test]
async fn cancel_arriving_mid_pass_removes_the_request_parked_at_the_next_queue() {
    let (_tmp, shard) = open_temp_shard().await;
    shard.stop_grant_scanner();
    let first = "cancel-chain-first-q";
    let second = "cancel-chain-second-q";
    enqueue_with_limits(
        &shard,
        "second-queue-blocker",
        vec![one_slot(second)],
        "blockers",
    )
    .await;
    park_waiter_and_free_its_slot(&shard, first, "waiter", vec![one_slot(second)]).await;

    let granted = run_pass_with_cancel_mid_pass(&shard, first, 1, "waiter").await;
    assert_eq!(granted.len(), 1, "the pass grants the first queue");

    assert_queue_settled(&shard, first, 0).await;
    assert_queue_settled(&shard, second, 1).await;
    assert_cancelled_with_nothing_pending(&shard, "waiter").await;
}

/// The job's second limit is a floating queue with a free slot, so the
/// pass's grant on the first queue takes the floating slot too and writes
/// the run task. A cancel that arrived mid-pass removes the task and
/// releases both holders.
#[silo::test]
async fn cancel_arriving_mid_pass_releases_the_floating_slot_granted_with_the_first_queue() {
    let (_tmp, shard) = open_temp_shard().await;
    shard.stop_grant_scanner();
    let first = "cancel-floating-room-first-q";
    let floating = "cancel-floating-room-floating-q";
    park_waiter_and_free_its_slot(&shard, first, "waiter", vec![floating_one_slot(floating)]).await;

    let granted = run_pass_with_cancel_mid_pass(&shard, first, 1, "waiter").await;
    assert_eq!(granted.len(), 1, "the pass grants the first queue");

    assert_queue_settled(&shard, first, 0).await;
    assert_queue_settled(&shard, floating, 0).await;
    assert_cancelled_with_nothing_pending(&shard, "waiter").await;
}

/// The job's second limit is a floating queue that is full, so the pass's
/// grant on the first queue parks a request at the floating queue. A cancel
/// that arrived mid-pass removes it and leaves the floating queue's counter,
/// which the enqueue path's waiter probe reads, at zero.
#[silo::test]
async fn cancel_arriving_mid_pass_removes_the_request_parked_at_a_floating_queue() {
    let (_tmp, shard) = open_temp_shard().await;
    shard.stop_grant_scanner();
    let first = "cancel-floating-full-first-q";
    let floating = "cancel-floating-full-floating-q";
    enqueue_with_limits(
        &shard,
        "floating-queue-blocker",
        vec![floating_one_slot(floating)],
        "blockers",
    )
    .await;
    park_waiter_and_free_its_slot(&shard, first, "waiter", vec![floating_one_slot(floating)]).await;

    let granted = run_pass_with_cancel_mid_pass(&shard, first, 1, "waiter").await;
    assert_eq!(granted.len(), 1, "the pass grants the first queue");

    assert_queue_settled(&shard, first, 0).await;
    assert_queue_settled(&shard, floating, 1).await;
    assert_cancelled_with_nothing_pending(&shard, "waiter").await;
}

/// The job's second limit is a rate limit, so the pass's grant writes a
/// rate-limit check task that carries the queue's holder. A cancel that
/// arrived mid-pass removes the task and releases the holder.
#[silo::test]
async fn cancel_arriving_mid_pass_removes_the_rate_limit_check_and_its_holder() {
    let (_tmp, shard) = open_temp_shard().await;
    shard.stop_grant_scanner();
    let queue = "cancel-rate-limit-q";
    let rate_limit = Limit::RateLimit(silo::job::GubernatorRateLimit {
        name: "cancel-mid-pass".to_string(),
        unique_key: "cancel-mid-pass-rate-limit".to_string(),
        limit: 100,
        duration_ms: 60_000,
        hits: 1,
        algorithm: silo::job::GubernatorAlgorithm::TokenBucket,
        behavior: 0,
        retry_policy: silo::job::RateLimitRetryPolicy::default(),
    });
    park_waiter_and_free_its_slot(&shard, queue, "waiter", vec![rate_limit]).await;

    let granted = run_pass_with_cancel_mid_pass(&shard, queue, 1, "waiter").await;
    assert_eq!(granted.len(), 1, "the pass grants the queue");

    assert_queue_settled(&shard, queue, 0).await;
    assert_cancelled_with_nothing_pending(&shard, "waiter").await;
}
