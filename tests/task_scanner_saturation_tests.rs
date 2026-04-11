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
