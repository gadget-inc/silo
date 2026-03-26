//! Tests for task scanner pickup of tasks created "behind the cursor".
//!
//! These tests verify that all code paths that create task keys result in
//! those tasks eventually being picked up by the TaskBroker scanner and
//! delivered via dequeue. This is important because:
//!
//! - The scanner always starts from the beginning of the task group key range.
//! - If we ever optimise the scanner to use a cursor (resume scanning from
//!   where it left off), we must ensure tasks inserted behind the cursor are
//!   still picked up.
//!
//! Each test exercises a different code path that can create a task key that
//! sorts BEFORE tasks already in the broker buffer.

mod test_helpers;

use silo::job::ConcurrencyLimit;
use silo::job::Limit;
use silo::job_attempt::AttemptOutcome;

use test_helpers::*;

const TIMEOUT_MS: u64 = 30000;

// ---------------------------------------------------------------------------
// 1. Enqueue with start_at_ms=0 sorts before all other tasks
// ---------------------------------------------------------------------------

/// Tasks enqueued with start_at_ms=0 use time=0 in their key, placing them
/// before any task with a real timestamp. After the buffer is already populated
/// with tasks at now_ms, a new start_at_ms=0 task must still be picked up
/// eventually (the scanner rescans from the beginning of the key range).
#[silo::test]
async fn enqueue_start_at_zero_picked_up_after_buffer_populated() {
    with_timeout!(TIMEOUT_MS, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let now = now_ms();

        // Enqueue several tasks with start_at_ms=now to populate the buffer
        for i in 0..5 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("job-now-{}", i)),
                    50,
                    now,
                    None,
                    payload.clone(),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue now");
        }

        // Dequeue one to prove the buffer is working
        let first = shard
            .dequeue("w", "default", 1)
            .await
            .expect("first dequeue");
        assert_eq!(first.tasks.len(), 1, "should dequeue from initial batch");

        // Now enqueue a task with start_at_ms=0 — this sorts BEFORE the buffered tasks
        shard
            .enqueue(
                "t1",
                Some("job-zero".to_string()),
                50,
                0,
                None,
                payload.clone(),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue zero");

        // Drain all remaining tasks — the zero-time task must appear eventually
        let mut all_ids: Vec<String> = Vec::new();
        loop {
            let batch = shard
                .dequeue("w", "default", 10)
                .await
                .expect("drain dequeue");
            if batch.tasks.is_empty() {
                break;
            }
            for task in &batch.tasks {
                all_ids.push(task.job().id().to_string());
            }
        }

        assert!(
            all_ids.contains(&"job-zero".to_string()),
            "start_at_ms=0 task must be picked up; got jobs: {:?}",
            all_ids
        );
        // Should have gotten all 5 remaining tasks (5 enqueued - 1 dequeued + 1 zero = 5)
        assert_eq!(
            all_ids.len(),
            5,
            "all tasks must be drained; got: {:?}",
            all_ids
        );
    });
}

// ---------------------------------------------------------------------------
// 2. Expedite moves a future task to now
// ---------------------------------------------------------------------------

/// Expediting a far-future scheduled job creates a new task key at now_ms,
/// which sorts before the old future-scheduled key. The scanner must pick up
/// the new key even if it already has buffered entries with later timestamps.
#[silo::test]
async fn expedited_task_picked_up_by_scanner() {
    with_timeout!(TIMEOUT_MS, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let now = now_ms();
        let far_future = now + 86_400_000; // 1 day ahead

        // Enqueue a job scheduled for far future
        shard
            .enqueue(
                "t1",
                Some("future-job".to_string()),
                50,
                far_future,
                None,
                payload.clone(),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue future");

        // Enqueue some immediate tasks to populate the buffer
        for i in 0..3 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("immediate-{}", i)),
                    50,
                    now,
                    None,
                    payload.clone(),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue immediate");
        }

        // Dequeue immediate tasks — future job should NOT appear (it's in the future)
        let batch1 = shard
            .dequeue("w", "default", 10)
            .await
            .expect("dequeue batch 1");
        let batch1_ids: Vec<&str> = batch1.tasks.iter().map(|t| t.job().id()).collect();
        assert!(
            !batch1_ids.contains(&"future-job"),
            "future job should not be dequeued before expedite"
        );

        // Complete those tasks
        for task in &batch1.tasks {
            shard
                .report_attempt_outcome(
                    task.attempt().task_id(),
                    AttemptOutcome::Success { result: vec![] },
                )
                .await
                .expect("report outcome");
        }

        // Expedite the future job — moves it to now
        shard
            .expedite_job("t1", "future-job")
            .await
            .expect("expedite");

        // The expedited task should now be dequeueable
        let mut all_ids: Vec<String> = Vec::new();
        loop {
            let batch = shard
                .dequeue("w", "default", 10)
                .await
                .expect("dequeue after expedite");
            if batch.tasks.is_empty() {
                break;
            }
            for task in &batch.tasks {
                all_ids.push(task.job().id().to_string());
            }
        }
        assert!(
            all_ids.contains(&"future-job".to_string()),
            "expedited task must be picked up; got: {:?}",
            all_ids
        );
    });
}

// ---------------------------------------------------------------------------
// 3. Restart creates a task at now_ms
// ---------------------------------------------------------------------------

/// Restarting a failed job creates a new task. The scanner must pick it up
/// even if the buffer already contains tasks from an earlier scan.
#[silo::test]
async fn restarted_job_task_picked_up_by_scanner() {
    with_timeout!(TIMEOUT_MS, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let now = now_ms();

        // Enqueue and process a job to Failed state
        shard
            .enqueue(
                "t1",
                Some("restart-me".to_string()),
                50,
                now,
                None,
                payload.clone(),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        let tasks = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);

        shard
            .report_attempt_outcome(
                tasks[0].attempt().task_id(),
                AttemptOutcome::Error {
                    error_code: "ERR".to_string(),
                    error: vec![],
                },
            )
            .await
            .expect("report error");

        // Enqueue more tasks so the buffer has entries
        for i in 0..3 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("filler-{}", i)),
                    50,
                    now,
                    None,
                    payload.clone(),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue filler");
        }

        // Restart the failed job — creates a new task
        shard
            .restart_job("t1", "restart-me")
            .await
            .expect("restart");

        // Dequeue all — the restarted job should appear
        let result = shard
            .dequeue("w", "default", 10)
            .await
            .expect("dequeue after restart");

        let job_ids: Vec<&str> = result.tasks.iter().map(|t| t.job().id()).collect();
        assert!(
            job_ids.contains(&"restart-me"),
            "restarted job must be picked up; got jobs: {:?}",
            job_ids
        );
    });
}

// ---------------------------------------------------------------------------
// 4. Concurrency grant creates a task with the original start_time_ms
// ---------------------------------------------------------------------------

/// When a concurrency slot opens and a queued request is granted, a RunAttempt
/// task is created with the original start_time_ms from when the job was first
/// enqueued. This time could be well in the past. The scanner must pick it up.
#[silo::test]
async fn concurrency_grant_task_picked_up_by_scanner() {
    with_timeout!(TIMEOUT_MS, {
        let (_tmp, shard) = open_temp_shard_with_reconcile_interval_ms(50).await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let now = now_ms();

        let limit = Limit::Concurrency(ConcurrencyLimit {
            key: "test-queue".to_string(),
            max_concurrency: 1,
        });

        // Enqueue job A with concurrency limit — it gets the slot
        shard
            .enqueue(
                "t1",
                Some("holder".to_string()),
                50,
                now,
                None,
                payload.clone(),
                vec![limit.clone()],
                None,
                "default",
            )
            .await
            .expect("enqueue holder");

        // Dequeue job A — it becomes the holder
        let holder_tasks = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue holder");
        assert_eq!(holder_tasks.tasks.len(), 1);
        let holder_task_id = holder_tasks.tasks[0].attempt().task_id().to_string();

        // Enqueue job B with same concurrency limit — it gets queued (RequestTicket)
        shard
            .enqueue(
                "t1",
                Some("waiter".to_string()),
                50,
                now,
                None,
                payload.clone(),
                vec![limit.clone()],
                None,
                "default",
            )
            .await
            .expect("enqueue waiter");

        // Enqueue some tasks in a different group to populate other buffers
        for i in 0..3 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("other-{}", i)),
                    50,
                    now,
                    None,
                    payload.clone(),
                    vec![],
                    None,
                    "other-group",
                )
                .await
                .expect("enqueue other");
        }

        // Complete holder — this frees the concurrency slot
        shard
            .report_attempt_outcome(&holder_task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("complete holder");

        // Wait for the concurrency grant scanner to process the pending request
        // and create a RunAttempt task for the waiter
        let result = poll_until(
            || async {
                shard
                    .dequeue("w", "default", 1)
                    .await
                    .expect("dequeue waiter")
            },
            |r| !r.tasks.is_empty(),
            5000,
        )
        .await;

        assert_eq!(
            result.tasks.len(),
            1,
            "waiter should be dequeued after concurrency grant"
        );
        assert_eq!(result.tasks[0].job().id(), "waiter");
    });
}

// ---------------------------------------------------------------------------
// 5. Interleaved enqueue and dequeue across task groups
// ---------------------------------------------------------------------------

/// Tasks enqueued into a task group that already has buffered entries should be
/// picked up even when they sort before existing buffer entries.
#[silo::test]
async fn interleaved_enqueue_dequeue_picks_up_all_tasks() {
    with_timeout!(TIMEOUT_MS, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let now = now_ms();

        // Phase 1: enqueue batch at now
        for i in 0..5 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("batch1-{}", i)),
                    50,
                    now,
                    None,
                    payload.clone(),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue batch 1");
        }

        // Dequeue 2 to partially drain the buffer
        let first = shard
            .dequeue("w", "default", 2)
            .await
            .expect("dequeue first");
        assert_eq!(first.tasks.len(), 2);

        // Phase 2: enqueue more at now+1 (sorts after remaining batch1 entries)
        for i in 0..3 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("batch2-{}", i)),
                    50,
                    now + 1,
                    None,
                    payload.clone(),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue batch 2");
        }

        // Phase 3: enqueue at start_at_ms=0 — sorts BEFORE everything
        shard
            .enqueue(
                "t1",
                Some("zero-time".to_string()),
                50,
                0,
                None,
                payload.clone(),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue zero");

        // Dequeue all remaining — should get all 7 remaining tasks
        // (5 original - 2 dequeued + 3 batch2 + 1 zero = 7)
        let mut all_ids: Vec<String> = Vec::new();
        for task in &first.tasks {
            all_ids.push(task.job().id().to_string());
        }

        // Drain everything
        loop {
            let batch = shard
                .dequeue("w", "default", 10)
                .await
                .expect("drain dequeue");
            if batch.tasks.is_empty() {
                break;
            }
            for task in &batch.tasks {
                all_ids.push(task.job().id().to_string());
            }
        }

        // We should have all 9 jobs
        assert_eq!(
            all_ids.len(),
            9,
            "expected 9 total tasks, got {}: {:?}",
            all_ids.len(),
            all_ids
        );
        assert!(
            all_ids.contains(&"zero-time".to_string()),
            "zero-time task must be dequeued; got: {:?}",
            all_ids
        );
    });
}

// ---------------------------------------------------------------------------
// 6. Higher priority task enqueued after lower priority tasks
// ---------------------------------------------------------------------------

/// A task with higher priority (lower number) enqueued after the buffer already
/// contains lower-priority tasks. Within the same start_time_ms, the
/// higher-priority task key sorts first and must be picked up eventually.
#[silo::test]
async fn higher_priority_enqueued_later_picked_up() {
    with_timeout!(TIMEOUT_MS, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let now = now_ms();

        // Enqueue low-priority tasks (priority=100) first
        for i in 0..5 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("low-pri-{}", i)),
                    100,
                    now,
                    None,
                    payload.clone(),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue low priority");
        }

        // Dequeue one to prove scanner is active
        let first = shard
            .dequeue("w", "default", 1)
            .await
            .expect("first dequeue");
        assert_eq!(first.tasks.len(), 1);

        // Now enqueue a HIGH priority task (priority=1) — sorts before the low-pri tasks
        shard
            .enqueue(
                "t1",
                Some("high-pri".to_string()),
                1,
                now,
                None,
                payload.clone(),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue high priority");

        // Drain all remaining — the high-pri task must appear
        let mut all_ids: Vec<String> = Vec::new();
        loop {
            let batch = shard
                .dequeue("w", "default", 10)
                .await
                .expect("drain dequeue");
            if batch.tasks.is_empty() {
                break;
            }
            for task in &batch.tasks {
                all_ids.push(task.job().id().to_string());
            }
        }

        assert!(
            all_ids.contains(&"high-pri".to_string()),
            "high priority task must be picked up; got: {:?}",
            all_ids
        );
        // 5 original - 1 dequeued + 1 high-pri = 5
        assert_eq!(
            all_ids.len(),
            5,
            "all tasks must be drained; got: {:?}",
            all_ids
        );
    });
}

// ---------------------------------------------------------------------------
// 7. Retry after failure creates a task at a future time
// ---------------------------------------------------------------------------

/// When a job fails and has a retry policy, a new task is created at the retry
/// backoff time. That task should be picked up once its time arrives.
#[silo::test]
async fn retry_task_picked_up_after_backoff() {
    with_timeout!(TIMEOUT_MS, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let now = now_ms();

        // Enqueue with a retry policy (1 retry, very short initial backoff)
        let retry = silo::retry::RetryPolicy {
            retry_count: 1,
            initial_interval_ms: 1, // 1ms backoff so the retry is immediately ready
            max_interval_ms: 10,
            randomize_interval: false,
            backoff_factor: 1.0,
        };

        shard
            .enqueue(
                "t1",
                Some("retry-job".to_string()),
                50,
                now,
                Some(retry),
                payload.clone(),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue retry job");

        // Dequeue and fail the first attempt
        let tasks = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue attempt 1");
        assert_eq!(tasks.tasks.len(), 1);
        assert_eq!(tasks.tasks[0].attempt().attempt_number(), 1);

        shard
            .report_attempt_outcome(
                tasks.tasks[0].attempt().task_id(),
                AttemptOutcome::Error {
                    error_code: "FAIL".to_string(),
                    error: vec![],
                },
            )
            .await
            .expect("fail attempt 1");

        // Small sleep to let the retry backoff elapse (1ms)
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // The retry task should now be dequeueable
        let retry_result = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue retry");
        assert_eq!(retry_result.tasks.len(), 1, "retry task should be dequeued");
        assert_eq!(retry_result.tasks[0].job().id(), "retry-job");
        assert_eq!(
            retry_result.tasks[0].attempt().attempt_number(),
            2,
            "should be attempt 2"
        );
    });
}

// ---------------------------------------------------------------------------
// 8. Multiple task groups: task in group A does not block group B
// ---------------------------------------------------------------------------

/// Each task group has its own scanner. Creating a task in group A should not
/// affect task delivery in group B, and vice versa. Tasks created in either
/// group after the other group's buffer is populated must still be delivered.
#[silo::test]
async fn tasks_across_groups_are_independently_scanned() {
    with_timeout!(TIMEOUT_MS, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let now = now_ms();

        // Populate group A
        for i in 0..3 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("group-a-{}", i)),
                    50,
                    now,
                    None,
                    payload.clone(),
                    vec![],
                    None,
                    "group-a",
                )
                .await
                .expect("enqueue group-a");
        }

        // Populate group B
        for i in 0..3 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("group-b-{}", i)),
                    50,
                    now,
                    None,
                    payload.clone(),
                    vec![],
                    None,
                    "group-b",
                )
                .await
                .expect("enqueue group-b");
        }

        // Drain group A first
        let a_result = shard
            .dequeue("w", "group-a", 10)
            .await
            .expect("dequeue group-a");
        assert_eq!(a_result.tasks.len(), 3);

        // Group B should still have all its tasks
        let b_result = shard
            .dequeue("w", "group-b", 10)
            .await
            .expect("dequeue group-b");
        assert_eq!(b_result.tasks.len(), 3);

        // Now add new tasks to group A — they should be picked up
        shard
            .enqueue(
                "t1",
                Some("group-a-late".to_string()),
                50,
                now,
                None,
                payload.clone(),
                vec![],
                None,
                "group-a",
            )
            .await
            .expect("enqueue group-a late");

        let a_late = shard
            .dequeue("w", "group-a", 1)
            .await
            .expect("dequeue group-a late");
        assert_eq!(a_late.tasks.len(), 1);
        assert_eq!(a_late.tasks[0].job().id(), "group-a-late");
    });
}

// ---------------------------------------------------------------------------
// 9. Bulk enqueue then drain verifies no tasks are lost
// ---------------------------------------------------------------------------

/// Enqueue many tasks in a burst, then drain them all. No tasks should be
/// lost even if the scanner needs multiple passes to buffer them all.
#[silo::test]
async fn bulk_enqueue_drain_no_tasks_lost() {
    with_timeout!(TIMEOUT_MS, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let now = now_ms();
        let total = 50;

        for i in 0..total {
            shard
                .enqueue(
                    "t1",
                    Some(format!("bulk-{:04}", i)),
                    50,
                    now + (i as i64), // stagger slightly for unique keys
                    None,
                    payload.clone(),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue bulk");
        }

        let mut dequeued_ids: Vec<String> = Vec::new();
        loop {
            let batch = shard
                .dequeue("w", "default", 20)
                .await
                .expect("drain dequeue");
            if batch.tasks.is_empty() {
                break;
            }
            for task in &batch.tasks {
                dequeued_ids.push(task.job().id().to_string());
            }
        }

        assert_eq!(
            dequeued_ids.len(),
            total,
            "expected {} tasks, got {}: missing tasks were lost by the scanner",
            total,
            dequeued_ids.len()
        );
    });
}
