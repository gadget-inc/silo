//! Profile the task scanner hot path using real shard dequeue operations.
//!
//! Creates a shard with a large number of pending tasks and measures sustained
//! dequeue throughput, which exercises the full TaskBroker scanner pipeline.
//!
//! Run with:
//!   cargo bench --bench task_scanner_profile
//!
//! Profile with:
//!   samply record cargo bench --bench task_scanner_profile

#[path = "bench_helpers.rs"]
mod bench_helpers;

use bench_helpers::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Number of tasks to enqueue for sustained dequeue benchmarks.
const TASK_COUNT: usize = 50_000;
/// Task group used for benchmark tasks.
const TASK_GROUP: &str = "scanner-bench";
/// Tenant used for benchmark tasks.
const TENANT: &str = "scanner-bench-tenant";

/// Enqueue a large number of ready tasks into a shard for benchmarking.
async fn enqueue_many_tasks(shard: &Arc<silo::job_store_shard::JobStoreShard>, count: usize) {
    let now = now_ms();
    let batch_size = 500;
    let mut enqueued = 0;
    let start = Instant::now();

    while enqueued < count {
        let batch_end = std::cmp::min(enqueued + batch_size, count);
        let mut batch = Vec::with_capacity(batch_end - enqueued);
        for idx in enqueued..batch_end {
            let job_id = format!("scan-bench-{:08}", idx);
            batch.push(silo::job_store_shard::import::ImportJobParams {
                id: job_id,
                priority: 50,
                enqueue_time_ms: now - 86_400_000 + (idx as i64 * 17),
                start_at_ms: now, // ready immediately
                retry_policy: None,
                payload: rmp_serde::to_vec(&serde_json::json!({"bench": "scanner"})).unwrap(),
                limits: vec![],
                metadata: None,
                task_group: TASK_GROUP.to_string(),
                attempts: vec![], // no attempts = Waiting status, creates a task entry
            });
        }

        shard
            .import_jobs(TENANT, batch)
            .await
            .expect("import scan bench jobs");

        enqueued = batch_end;
        if enqueued % 10_000 == 0 {
            let rate = enqueued as f64 / start.elapsed().as_secs_f64();
            println!(
                "  Enqueued {}/{} tasks ({:.0} jobs/sec)",
                enqueued, count, rate
            );
        }
    }

    // Flush to SSTs so the scan path is realistic
    shard
        .db()
        .flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .expect("flush memtable");

    println!(
        "  Enqueued {} tasks in {:.1}s",
        count,
        start.elapsed().as_secs_f64()
    );
}

/// Sustained dequeue: drain tasks as fast as possible, measuring throughput.
/// Returns (total_dequeued, elapsed, per_batch_durations).
async fn sustained_dequeue(
    shard: &Arc<silo::job_store_shard::JobStoreShard>,
    batch_size: usize,
    max_tasks: usize,
) -> (usize, Duration, Vec<Duration>) {
    let start = Instant::now();
    let mut total = 0;
    let mut durations = Vec::new();

    while total < max_tasks {
        let t = Instant::now();
        let result = shard
            .dequeue("bench-worker", TASK_GROUP, batch_size)
            .await
            .expect("dequeue");
        let elapsed = t.elapsed();

        if result.tasks.is_empty() {
            // Give scanner a moment to refill, then try once more
            tokio::time::sleep(Duration::from_millis(100)).await;
            let result = shard
                .dequeue("bench-worker", TASK_GROUP, batch_size)
                .await
                .expect("dequeue retry");
            if result.tasks.is_empty() {
                break;
            }
            total += result.tasks.len();
            durations.push(elapsed + Duration::from_millis(100));
        } else {
            total += result.tasks.len();
            durations.push(elapsed);
        }
    }

    (total, start.elapsed(), durations)
}

fn print_stats(label: &str, durations: &mut [Duration]) {
    if durations.is_empty() {
        println!("  {:<40} (no data)", label);
        return;
    }
    durations.sort();
    let p50 = durations[durations.len() / 2];
    let p99 = durations[(durations.len() as f64 * 0.99).ceil() as usize - 1];
    let mean: Duration = durations.iter().sum::<Duration>() / durations.len() as u32;
    let min = durations[0];
    let max = durations[durations.len() - 1];
    println!(
        "  {:<40} p50={:<10} p99={:<10} mean={:<10} [min={:<10} max={}]",
        format!("{}:", label),
        format_duration(p50),
        format_duration(p99),
        format_duration(mean),
        format_duration(min),
        format_duration(max),
    );
}

#[tokio::main]
async fn main() {
    println!("\n========================================");
    println!("Task Scanner Profile Benchmark");
    println!("========================================\n");

    let metadata = ensure_golden_shard().await;

    println!("Cloning golden shard and adding {} tasks...", TASK_COUNT);
    let (_guard, shard) = clone_golden_shard("scanner-profile", &metadata).await;
    enqueue_many_tasks(&shard, TASK_COUNT).await;

    // Let the scanner populate the buffer initially
    println!("\n  Waiting for scanner to populate buffer...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- Sustained dequeue throughput ---
    println!("\n--- Sustained dequeue throughput (batch_size=32) ---");
    {
        let (total, elapsed, mut durations) = sustained_dequeue(&shard, 32, TASK_COUNT).await;
        let rate = total as f64 / elapsed.as_secs_f64();
        println!(
            "  Dequeued {} tasks in {:.1}s = {:.0} tasks/sec",
            total,
            elapsed.as_secs_f64(),
            rate,
        );
        print_stats("dequeue_batch(32)", &mut durations);
    }

    shard.close().await.expect("close");

    // Run again with a fresh clone for a second batch size
    let (_guard2, shard2) = clone_golden_shard("scanner-profile-2", &metadata).await;
    enqueue_many_tasks(&shard2, TASK_COUNT).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("\n--- Sustained dequeue throughput (batch_size=1) ---");
    {
        // Only drain 2000 with batch_size=1 to keep it reasonable
        let (total, elapsed, mut durations) = sustained_dequeue(&shard2, 1, 2000).await;
        let rate = total as f64 / elapsed.as_secs_f64();
        println!(
            "  Dequeued {} tasks in {:.1}s = {:.0} tasks/sec",
            total,
            elapsed.as_secs_f64(),
            rate,
        );
        print_stats("dequeue_batch(1)", &mut durations);
    }

    shard2.close().await.expect("close");
    println!("\nDone.");
}
