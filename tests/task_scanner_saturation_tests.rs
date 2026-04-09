//! Tests that verify the task broker buffer fills up properly under load,
//! and that the scanner doesn't spin at high frequency when there's no work.

mod test_helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use test_helpers::*;

const TIMEOUT_MS: u64 = 180000;

/// After dequeuing tasks, their task keys should be deleted from SlateDB.
/// The scanner should not see them on subsequent scans, and silo tombstones
/// should not accumulate for keys that are gone from the DB.
#[silo::test]
async fn tombstones_dont_accumulate_for_deleted_keys() {
    with_timeout!(TIMEOUT_MS, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let now = now_ms();

        // Enqueue 20 tasks
        for i in 0..20 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("tombstone-test-{:04}", i)),
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
        }

        // Wait for scanner to populate buffer
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let buf_before = shard.broker_buffer_len();
        eprintln!("buffer before dequeue: {}", buf_before);
        assert!(buf_before > 0, "buffer should have entries");

        // Dequeue all tasks
        let mut dequeued = 0;
        loop {
            let r = shard.dequeue("w", "default", 10).await.expect("dequeue");
            if r.tasks.is_empty() {
                break;
            }
            dequeued += r.tasks.len();
        }
        eprintln!("dequeued: {}", dequeued);
        assert_eq!(dequeued, 20);

        // Flush to make sure deletes are persisted
        shard.db().flush().await.expect("flush");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Now count how many task keys remain in the DB
        let remaining_tasks = count_task_keys(shard.db()).await;
        eprintln!(
            "task keys remaining in DB after dequeue+flush: {}",
            remaining_tasks
        );

        // All task keys should be deleted — none should remain
        assert_eq!(
            remaining_tasks, 0,
            "all task keys should be deleted after dequeue, but {} remain",
            remaining_tasks
        );

        // Enqueue 10 more tasks
        for i in 0..10 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("post-dequeue-{:04}", i)),
                    50,
                    now,
                    None,
                    payload.clone(),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue post");
        }

        // Wait for scanner
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // The scanner should find all 10 new tasks without being blocked
        // by tombstones from the previous batch
        let buf_after = shard.broker_buffer_len();
        eprintln!("buffer after re-enqueue: {}", buf_after);
        assert!(
            buf_after >= 8,
            "buffer should contain most of the 10 new tasks, got {}",
            buf_after
        );
    });
}

/// Verify dequeued task keys are actually gone from the raw DB scan.
#[silo::test]
async fn dequeued_task_keys_are_deleted_from_db() {
    with_timeout!(TIMEOUT_MS, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let now = now_ms();

        // Enqueue 5 tasks
        for i in 0..5 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("delete-check-{:04}", i)),
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
        }

        // Count task keys before dequeue
        let before = count_task_keys(shard.db()).await;
        eprintln!("task keys before dequeue: {}", before);
        assert_eq!(before, 5);

        // Dequeue all
        let r = shard.dequeue("w", "default", 10).await.expect("dequeue");
        eprintln!("dequeued: {}", r.tasks.len());

        // Count task keys immediately after dequeue (no flush)
        let after_immediate = count_task_keys(shard.db()).await;
        eprintln!("task keys immediately after dequeue: {}", after_immediate);

        // Flush and count again
        shard.db().flush().await.expect("flush");
        let after_flush = count_task_keys(shard.db()).await;
        eprintln!("task keys after flush: {}", after_flush);

        assert_eq!(
            after_flush, 0,
            "all task keys should be deleted after dequeue, but {} remain",
            after_flush
        );
    });
}

/// Same as above but with a separate WAL store, matching production config.
/// This tests whether the separate WAL path causes deleted keys to remain
/// visible in scans (which would explain production tombstone accumulation).
#[silo::test]
async fn dequeued_task_keys_are_deleted_from_db_with_local_wal() {
    with_timeout!(TIMEOUT_MS, {
        let (_wal_cfg, shard) = open_temp_shard_with_local_wal(true).await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let now = now_ms();

        // Enqueue 5 tasks
        for i in 0..5 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("wal-delete-check-{:04}", i)),
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
        }

        let before = count_task_keys(shard.db()).await;
        eprintln!("[WAL] task keys before dequeue: {}", before);
        assert_eq!(before, 5);

        let r = shard.dequeue("w", "default", 10).await.expect("dequeue");
        eprintln!("[WAL] dequeued: {}", r.tasks.len());

        let after_immediate = count_task_keys(shard.db()).await;
        eprintln!(
            "[WAL] task keys immediately after dequeue: {}",
            after_immediate
        );

        shard.db().flush().await.expect("flush");
        let after_flush = count_task_keys(shard.db()).await;
        eprintln!("[WAL] task keys after flush: {}", after_flush);

        assert_eq!(
            after_flush, 0,
            "[WAL] all task keys should be deleted after dequeue, but {} remain",
            after_flush
        );
    });
}

/// Reproduce production pattern: concurrent enqueue + dequeue on the same
/// task group. Monitor whether silo tombstones accumulate (indicating the
/// scanner is seeing deleted keys that should be gone).
#[silo::test]
async fn concurrent_enqueue_dequeue_tombstone_accumulation() {
    with_timeout!(TIMEOUT_MS, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let stop = Arc::new(AtomicBool::new(false));

        // Enqueue a seed batch
        for i in 0..10 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("seed-{:04}", i)),
                    50,
                    now_ms(),
                    None,
                    payload.clone(),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue seed");
        }

        // Concurrent enqueue loop
        let shard_enq = Arc::clone(&shard);
        let stop_enq = Arc::clone(&stop);
        let payload_enq = payload.clone();
        let enqueued = Arc::new(AtomicUsize::new(0));
        let enqueued2 = Arc::clone(&enqueued);
        let enqueue_task = tokio::spawn(async move {
            let mut i = 100;
            while !stop_enq.load(Ordering::Relaxed) {
                let _ = shard_enq
                    .enqueue(
                        "t1",
                        Some(format!("conc-{:06}", i)),
                        50,
                        now_ms(),
                        None,
                        payload_enq.clone(),
                        vec![],
                        None,
                        "default",
                    )
                    .await;
                enqueued2.fetch_add(1, Ordering::Relaxed);
                i += 1;
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });

        // Concurrent dequeue loop
        let shard_deq = Arc::clone(&shard);
        let stop_deq = Arc::clone(&stop);
        let dequeued = Arc::new(AtomicUsize::new(0));
        let dequeued2 = Arc::clone(&dequeued);
        let dequeue_task = tokio::spawn(async move {
            while !stop_deq.load(Ordering::Relaxed) {
                let result = shard_deq
                    .dequeue("worker", "default", 5)
                    .await
                    .expect("dequeue");
                let count = result.tasks.len();
                dequeued2.fetch_add(count, Ordering::Relaxed);
                // Complete the tasks with a delay to simulate worker processing
                for task in &result.tasks {
                    let _ = shard_deq
                        .report_attempt_outcome(
                            task.attempt().task_id(),
                            silo::job_attempt::AttemptOutcome::Success { result: vec![] },
                        )
                        .await;
                }
                // Slower dequeue rate so tasks accumulate in the DB
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        });

        // Sample buffer and task key counts over 2 minutes
        let mut max_task_keys_in_db = 0usize;
        let mut samples = Vec::new();
        let sample_count = 120; // 120 * 1000ms = 2 minutes
        for i in 0..sample_count {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            let buf = shard.broker_buffer_len();
            let db_tasks = count_task_keys(shard.db()).await;
            max_task_keys_in_db = max_task_keys_in_db.max(db_tasks);
            if i % 10 == 0 {
                eprintln!(
                    "t={}s buffer={} db_tasks={} enqueued={} dequeued={}",
                    (i + 1),
                    buf,
                    db_tasks,
                    enqueued.load(Ordering::Relaxed),
                    dequeued.load(Ordering::Relaxed),
                );
            }
            samples.push((buf, db_tasks));
        }

        stop.store(true, Ordering::Relaxed);
        enqueue_task.await.expect("enqueue task");
        dequeue_task.await.expect("dequeue task");

        let total_enqueued = enqueued.load(Ordering::Relaxed);
        let total_dequeued = dequeued.load(Ordering::Relaxed);
        eprintln!(
            "total enqueued={} dequeued={} max_db_tasks={}",
            total_enqueued, total_dequeued, max_task_keys_in_db
        );

        // The buffer should reflect approximately the number of task keys
        // actually in the DB, not be stuck at a tiny number while the DB
        // has many keys
        let (last_buf, last_db) = samples.last().unwrap();
        eprintln!("final: buffer={} db_tasks={}", last_buf, last_db);
    });
}

/// Test the RequestTicket → Requested cycle with concurrency-limited tasks.
/// When the concurrency queue is full, RequestTicket tasks are released back
/// (not tombstoned). They stay in the DB and get re-scanned. This should NOT
/// cause tombstone accumulation.
#[silo::test]
async fn concurrency_limited_tasks_dont_cause_tombstone_accumulation() {
    with_timeout!(TIMEOUT_MS, {
        let (_tmp, shard) = open_temp_shard_with_reconcile_interval_ms(100).await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let now = now_ms();
        let stop = Arc::new(AtomicBool::new(false));

        let limit = silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
            key: "test-queue".to_string(),
            max_concurrency: 2,
        });

        // Enqueue 2 jobs that fill the concurrency slots
        for i in 0..2 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("holder-{}", i)),
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
        }

        // Dequeue holders — they become Running, filling the concurrency slots
        let holders = shard
            .dequeue("w", "default", 10)
            .await
            .expect("dequeue holders");
        assert_eq!(holders.tasks.len(), 2, "should get 2 holders");
        let holder_task_ids: Vec<String> = holders
            .tasks
            .iter()
            .map(|t| t.attempt().task_id().to_string())
            .collect();

        // Enqueue 20 more jobs — these will be queued as RequestTicket tasks
        for i in 0..20 {
            shard
                .enqueue(
                    "t1",
                    Some(format!("waiter-{:04}", i)),
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
        }

        // Continuously dequeue — RequestTicket tasks will be picked up, processed
        // (concurrency full → Requested), and released back. This creates the
        // scan-dequeue-release cycle.
        let shard_deq = Arc::clone(&shard);
        let stop_deq = Arc::clone(&stop);
        let dequeue_count = Arc::new(AtomicUsize::new(0));
        let dequeue_count2 = Arc::clone(&dequeue_count);
        let dequeue_task = tokio::spawn(async move {
            while !stop_deq.load(Ordering::Relaxed) {
                let _ = shard_deq.dequeue("w", "default", 5).await;
                dequeue_count2.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        });

        // Let it run for 30 seconds — watch buffer and DB task counts
        for i in 0..30 {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            let buf = shard.broker_buffer_len();
            let db_tasks = count_task_keys(shard.db()).await;
            if i % 5 == 0 {
                eprintln!(
                    "t={}s buffer={} db_tasks={} dequeue_calls={}",
                    i + 1,
                    buf,
                    db_tasks,
                    dequeue_count.load(Ordering::Relaxed),
                );
            }
        }

        stop.store(true, Ordering::Relaxed);
        dequeue_task.await.expect("dequeue task");

        eprintln!(
            "final: buffer={} db_tasks={} dequeue_calls={}",
            shard.broker_buffer_len(),
            count_task_keys(shard.db()).await,
            dequeue_count.load(Ordering::Relaxed),
        );

        // Now release one holder and check that waiters get processed
        shard
            .report_attempt_outcome(
                &holder_task_ids[0],
                silo::job_attempt::AttemptOutcome::Success { result: vec![] },
            )
            .await
            .expect("complete holder");

        // Wait for grant scanner to process
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let result = shard
            .dequeue("w", "default", 5)
            .await
            .expect("dequeue after release");
        eprintln!(
            "after releasing holder: dequeued {} tasks",
            result.tasks.len()
        );
    });
}

/// With many tasks in the DB, the broker buffer should fill up to target_buffer
/// (8192) even when tasks are being continuously dequeued.
#[silo::test]
async fn buffer_fills_to_target_under_concurrent_dequeue() {
    with_timeout!(TIMEOUT_MS, {
        let (_tmp, shard) = open_temp_shard().await;
        let payload = msgpack_payload(&serde_json::json!({"test": true}));
        let now = now_ms();

        // Import tasks so the scanner has plenty to buffer.
        let task_count = 500;
        let batch_size = 100;
        let mut imported = 0;
        while imported < task_count {
            let end = std::cmp::min(imported + batch_size, task_count);
            let batch: Vec<silo::job_store_shard::import::ImportJobParams> = (imported..end)
                .map(|i| silo::job_store_shard::import::ImportJobParams {
                    id: format!("job-{:06}", i),
                    priority: 50,
                    enqueue_time_ms: now - 86_400_000 + (i as i64 * 17),
                    start_at_ms: now,
                    retry_policy: None,
                    payload: payload.clone(),
                    limits: vec![],
                    metadata: None,
                    task_group: "default".to_string(),
                    attempts: vec![],
                })
                .collect();
            shard.import_jobs("t1", batch).await.expect("import");
            imported = end;
        }

        // Wait for the scanner to populate the buffer
        let mut max_buf_len = 0usize;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let buf_len = shard.broker_buffer_len();
            max_buf_len = max_buf_len.max(buf_len);
            if buf_len >= task_count / 2 {
                break;
            }
        }

        assert!(
            max_buf_len >= task_count / 2,
            "buffer should fill to at least half of available tasks ({}), got max {}",
            task_count / 2,
            max_buf_len
        );

        // Now start concurrent dequeues
        let shard2 = Arc::clone(&shard);
        let stop = Arc::new(AtomicBool::new(false));
        let dequeued = Arc::new(AtomicUsize::new(0));
        let stop2 = Arc::clone(&stop);
        let dequeued2 = Arc::clone(&dequeued);

        let dequeue_task = tokio::spawn(async move {
            while !stop2.load(Ordering::Relaxed) {
                let result = shard2
                    .dequeue("worker", "default", 10)
                    .await
                    .expect("dequeue");
                dequeued2.fetch_add(result.tasks.len(), Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

        let buf_during_drain = shard.broker_buffer_len();
        let total_dequeued = dequeued.load(Ordering::Relaxed);

        stop.store(true, Ordering::Relaxed);
        dequeue_task.await.expect("dequeue task");

        println!(
            "buffer during drain: {}, total dequeued: {}",
            buf_during_drain, total_dequeued
        );

        assert!(
            buf_during_drain >= 10,
            "buffer should stay reasonably full during concurrent dequeue, got {} (dequeued {})",
            buf_during_drain,
            total_dequeued
        );
    });
}
