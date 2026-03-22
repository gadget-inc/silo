//! Benchmark to measure scan performance with accumulated tombstones.
//!
//! This creates realistic churn (enqueue+dequeue+complete cycles) to generate
//! tombstones in SSTs, then measures how expensive lease reaping and task
//! broker-style scans become.
//!
//! Run with: cargo bench --bench scan_tombstone_profile

#[path = "bench_helpers.rs"]
mod bench_helpers;

use bench_helpers::*;
use silo::job_attempt::AttemptOutcome;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Number of full enqueue→dequeue→complete cycles to generate tombstones.
/// Each cycle creates and deletes task keys, lease keys, etc.
const CHURN_ROUNDS: usize = 100;
/// Jobs per churn round.
const JOBS_PER_ROUND: usize = 500;
/// Tenant to use for churn.
const CHURN_TENANT: &str = "bench_churn";

const SCAN_ITERS: usize = 50;

async fn run_churn_cycle(shard: &Arc<silo::job_store_shard::JobStoreShard>, round: usize) {
    let now = now_ms();
    let worker_id = format!("churn-worker-{}", round);

    // Enqueue jobs
    for i in 0..JOBS_PER_ROUND {
        let job_id = format!("churn-r{:04}-j{:04}", round, i);
        shard
            .enqueue(
                CHURN_TENANT,
                Some(job_id),
                50,
                now,
                None,
                vec![1, 2, 3],
                vec![],
                None,
                "",
            )
            .await
            .expect("enqueue");
    }

    // Dequeue all
    let mut task_ids = Vec::with_capacity(JOBS_PER_ROUND);
    let mut remaining = JOBS_PER_ROUND;
    while remaining > 0 {
        let batch = std::cmp::min(remaining, 50);
        let result = shard.dequeue(&worker_id, "", batch).await.expect("dequeue");
        if result.tasks.is_empty() {
            // Scanner may need time to populate buffer
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        for task in &result.tasks {
            task_ids.push(task.attempt().task_id().to_string());
        }
        remaining = remaining.saturating_sub(result.tasks.len());
    }

    // Complete all jobs
    for task_id in &task_ids {
        let _ = shard
            .report_attempt_outcome(task_id, AttemptOutcome::Success { result: vec![] })
            .await;
    }
}

async fn measure_lease_reap(
    shard: &Arc<silo::job_store_shard::JobStoreShard>,
    iters: usize,
) -> BenchResult {
    let mut durations = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        let _ = shard.reap_expired_leases("-").await;
        durations.push(start.elapsed());
    }
    durations.sort();
    BenchResult {
        label: "reap_expired_leases".to_string(),
        durations,
    }
}

async fn measure_dequeue_scan(
    shard: &Arc<silo::job_store_shard::JobStoreShard>,
    iters: usize,
) -> BenchResult {
    let worker_id = "scan-bench-worker";
    let mut durations = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        // Dequeue with 0 results expected exercises the task broker scan path
        let _ = shard.dequeue(worker_id, "", 10).await;
        durations.push(start.elapsed());
    }
    durations.sort();
    BenchResult {
        label: "dequeue (scan path)".to_string(),
        durations,
    }
}

async fn measure_reconcile(
    shard: &Arc<silo::job_store_shard::JobStoreShard>,
    iters: usize,
) -> BenchResult {
    let mut durations = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        shard.reconcile_pending_concurrency_requests_once().await;
        durations.push(start.elapsed());
    }
    durations.sort();
    BenchResult {
        label: "reconcile_concurrency".to_string(),
        durations,
    }
}

#[tokio::main]
async fn main() {
    let metadata = ensure_golden_shard().await;
    let (guard, shard) = clone_golden_shard("scan-tombstone", &metadata).await;

    // --- Baseline: measure scans before churn ---
    println!("=== Baseline (no churn) ===");
    // Warm up the shard scanner
    tokio::time::sleep(Duration::from_millis(200)).await;

    measure_lease_reap(&shard, SCAN_ITERS).await.print();
    measure_dequeue_scan(&shard, SCAN_ITERS).await.print();
    measure_reconcile(&shard, SCAN_ITERS).await.print();

    // --- Generate tombstones via churn ---
    println!(
        "\n=== Generating tombstones ({} rounds × {} jobs) ===",
        CHURN_ROUNDS, JOBS_PER_ROUND
    );
    let churn_start = Instant::now();
    for round in 0..CHURN_ROUNDS {
        run_churn_cycle(&shard, round).await;
        if (round + 1) % 5 == 0 {
            println!(
                "  round {}/{} done ({:.1}s elapsed)",
                round + 1,
                CHURN_ROUNDS,
                churn_start.elapsed().as_secs_f64()
            );
        }
    }
    println!(
        "  churn complete: {} total jobs churned in {:.1}s",
        CHURN_ROUNDS * JOBS_PER_ROUND,
        churn_start.elapsed().as_secs_f64()
    );

    // Wait for memtable to flush so tombstones end up in SSTs
    println!("\n  waiting for memtable flush...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- After churn: measure scans again ---
    println!("\n=== After churn (tombstones in SSTs) ===");
    measure_lease_reap(&shard, SCAN_ITERS).await.print();
    measure_dequeue_scan(&shard, SCAN_ITERS).await.print();
    measure_reconcile(&shard, SCAN_ITERS).await.print();

    // --- After full compaction: measure scans again ---
    println!("\n=== Triggering full compaction... ===");
    shard
        .submit_full_compaction()
        .await
        .expect("submit compaction");

    // Wait for compaction to complete
    println!("  waiting for compaction to finish...");
    for i in 0..60 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _stats = shard.slatedb_stats();
        // Check if compaction reduced sorted runs to 1 or fewer
        // We can't easily check sorted run count, so just wait a reasonable time
        if i >= 5 {
            // Try a scan to see if it's faster
            let start = Instant::now();
            let _ = shard.reap_expired_leases("-").await;
            let d = start.elapsed();
            if d < Duration::from_millis(5) || i >= 15 {
                println!(
                    "  compaction appears done after {}s (reap took {:.1}ms)",
                    i + 1,
                    d.as_secs_f64() * 1000.0
                );
                break;
            }
        }
    }

    println!("\n=== After full compaction ===");
    measure_lease_reap(&shard, SCAN_ITERS).await.print();
    measure_dequeue_scan(&shard, SCAN_ITERS).await.print();
    measure_reconcile(&shard, SCAN_ITERS).await.print();

    shard.close().await.expect("close");
    drop(guard);
}
