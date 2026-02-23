mod test_helpers;

use silo::job::{ConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use std::sync::Arc;
use std::time::Duration;

use test_helpers::*;

/// Poll dequeue until we get the expected number of tasks, or timeout.
/// Needed because the grant scanner runs asynchronously after releases.
async fn poll_dequeue(
    shard: &Arc<JobStoreShard>,
    worker: &str,
    group: &str,
    expected: usize,
    timeout_ms: u64,
) -> Vec<silo::task::LeasedTask> {
    let start = std::time::Instant::now();
    let mut all_tasks = Vec::new();
    loop {
        let remaining = expected - all_tasks.len();
        if remaining == 0 {
            return all_tasks;
        }
        let result = shard
            .dequeue(worker, group, remaining)
            .await
            .expect("dequeue");
        all_tasks.extend(result.tasks);
        if all_tasks.len() >= expected {
            return all_tasks;
        }
        if start.elapsed() > Duration::from_millis(timeout_ms) {
            return all_tasks;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Test that concurrent report_attempt_outcome calls on the same concurrency queue
/// don't race and double-grant the same pending request.
///
/// The bug (fixed): release_and_grant_next used WriteBatch (no OCC), so two concurrent
/// calls both scanned SlateDB, found the same first pending request, and both granted it.
/// The fix decouples release from grant via a background scanner that serializes all grants.
///
/// This test:
/// 1. Enqueues N jobs with concurrency limit K (where N > K)
/// 2. Dequeues K tasks (filling the concurrency slots)
/// 3. Enqueues more jobs to create pending requests
/// 4. Concurrently reports all K tasks as complete (via tokio::spawn)
/// 5. Verifies that K new tasks were granted (not fewer due to the race)
#[silo::test]
async fn concurrent_release_and_grant_does_not_double_grant() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "race-q".to_string();
    let limit = 5u32;
    let total_jobs = 20usize;

    // Enqueue all jobs with the same concurrency limit
    let mut job_ids = Vec::new();
    for i in 0..total_jobs {
        let job_id = shard
            .enqueue(
                "-",
                None,
                50u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"job": i})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: limit,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue");
        job_ids.push(job_id);
    }

    // Dequeue: first K jobs get granted immediately and become leasable tasks
    let tasks = shard
        .dequeue("w", "default", limit as usize)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(
        tasks.len(),
        limit as usize,
        "should dequeue exactly {limit} tasks (concurrency limit)"
    );

    // Collect task IDs for the leased tasks
    let task_ids: Vec<String> = tasks
        .iter()
        .map(|t| t.attempt().task_id().to_string())
        .collect();

    // There should be pending concurrency requests for the remaining jobs
    let pending_requests = count_concurrency_requests(shard.db()).await;
    assert!(
        pending_requests > 0,
        "should have pending concurrency requests, got {pending_requests}"
    );

    // Now concurrently report all K tasks as complete.
    // Each report_attempt_outcome releases a holder and signals the grant scanner.
    // The scanner serializes all grants, preventing the double-grant race.
    let shard = Arc::clone(&shard);
    let mut handles = Vec::new();
    for task_id in &task_ids {
        let shard = Arc::clone(&shard);
        let task_id = task_id.clone();
        handles.push(tokio::spawn(async move {
            shard
                .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
                .await
                .expect("report outcome");
        }));
    }

    // Wait for all concurrent completions to finish
    for handle in handles {
        handle.await.expect("join");
    }

    // After K concurrent releases, the grant scanner should grant K new tasks.
    // Poll because grants happen asynchronously in the background scanner.
    let new_tasks = poll_dequeue(&shard, "w", "default", limit as usize, 5000).await;

    assert_eq!(
        new_tasks.len(),
        limit as usize,
        "Expected {limit} new tasks after {limit} concurrent releases, but got {}. \
         This indicates grants were lost — the grant scanner did not process all releases.",
        new_tasks.len()
    );

    // Verify the new tasks are for different jobs (no double-grant)
    let new_job_ids: std::collections::HashSet<String> =
        new_tasks.iter().map(|t| t.job().id().to_string()).collect();
    assert_eq!(
        new_job_ids.len(),
        limit as usize,
        "New tasks should be for {limit} distinct jobs, but got {} distinct jobs. \
         This indicates the same request was granted multiple times.",
        new_job_ids.len()
    );

    // Also verify the holder count matches expected
    let holders = count_concurrency_holders(shard.db()).await;
    assert_eq!(
        holders, limit as usize,
        "Holder count should be {limit} after release+grant cycle, got {holders}"
    );
}

/// Same test but with limit=1 to catch the serial case
#[silo::test]
async fn concurrent_release_and_grant_mutex_queue() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let queue = "mutex-race-q".to_string();

    // Enqueue 3 jobs with limit=1
    for i in 0..3 {
        shard
            .enqueue(
                "-",
                None,
                50u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"job": i})),
                vec![Limit::Concurrency(ConcurrencyLimit {
                    key: queue.clone(),
                    max_concurrency: 1,
                })],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    // Dequeue: first job gets the slot
    let tasks = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].attempt().task_id().to_string();

    // Complete it — grant scanner should grant the second job
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report");

    // Poll for the second job (grant happens asynchronously)
    let shard = Arc::clone(&shard);
    let tasks2 = poll_dequeue(&shard, "w", "default", 1, 5000).await;
    assert_eq!(
        tasks2.len(),
        1,
        "second job should be granted after first completes"
    );
    let task_id2 = tasks2[0].attempt().task_id().to_string();

    // Complete second — should grant third
    shard
        .report_attempt_outcome(&task_id2, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report 2");

    let tasks3 = poll_dequeue(&shard, "w", "default", 1, 5000).await;
    assert_eq!(
        tasks3.len(),
        1,
        "third job should be granted after second completes"
    );
}
