mod test_helpers;

use std::sync::Arc;

use silo::codec::encode_concurrency_action;
use silo::job::{ConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::keys::concurrency_request_key;
use silo::keys::concurrency_requester_counter_key;
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
