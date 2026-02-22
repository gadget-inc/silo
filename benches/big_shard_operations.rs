//! Benchmarks for common shard operations against a large (~500K job) dataset.
//!
//! Each benchmark group clones the golden shard (or opens it read-only),
//! runs warmup + measured iterations, and prints timing results.

#[path = "bench_helpers.rs"]
mod bench_helpers;

use bench_helpers::*;
use silo::job::ConcurrencyLimit;
use silo::job::Limit;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::import::ImportJobParams;
use std::sync::Arc;
use std::time::Instant;

const WARMUP_ITERS: usize = 3;
const MEASURED_ITERS: usize = 100;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Run warmup + measured iterations of an async operation that returns nothing.
async fn bench_op<F, Fut>(label: &str, warmup: usize, measured: usize, mut f: F) -> BenchResult
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    for _ in 0..warmup {
        f().await;
    }

    let mut durations = Vec::with_capacity(measured);
    for _ in 0..measured {
        let start = Instant::now();
        f().await;
        durations.push(start.elapsed());
    }

    durations.sort();
    BenchResult {
        label: label.to_string(),
        durations,
    }
}

// ---------------------------------------------------------------------------
// Benchmark groups
// ---------------------------------------------------------------------------

async fn bench_get_job(metadata: &GoldenShardMetadata) {
    println!("--- get_job (readonly) ---");
    let shard = open_golden_shard_readonly(metadata).await;

    let (tenant, job_id) = &metadata.succeeded_jobs[0];
    let r = bench_op("get_job", WARMUP_ITERS, MEASURED_ITERS, || {
        let shard = Arc::clone(&shard);
        let tenant = tenant.clone();
        let job_id = job_id.clone();
        async move {
            shard.get_job(&tenant, &job_id).await.expect("get_job");
        }
    })
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

async fn bench_get_jobs_batch(metadata: &GoldenShardMetadata) {
    println!("--- get_jobs_batch (readonly) ---");
    let shard = open_golden_shard_readonly(metadata).await;

    let tenant = &metadata.succeeded_jobs[0].0;
    let ids: Vec<String> = metadata.succeeded_jobs[..10]
        .iter()
        .map(|(_, id)| id.clone())
        .collect();

    let r = bench_op("get_jobs_batch(10)", WARMUP_ITERS, MEASURED_ITERS, || {
        let shard = Arc::clone(&shard);
        let tenant = tenant.clone();
        let ids = ids.clone();
        async move {
            shard
                .get_jobs_batch(&tenant, &ids)
                .await
                .expect("get_jobs_batch");
        }
    })
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

async fn bench_enqueue_no_limit(metadata: &GoldenShardMetadata) {
    println!("--- enqueue (no limits) ---");
    let (_guard, shard) = clone_golden_shard("enqueue-no-limit", metadata).await;
    let now = now_ms();

    let counter = std::sync::atomic::AtomicUsize::new(0);
    let r = bench_op("enqueue_no_limit", WARMUP_ITERS, MEASURED_ITERS, || {
        let shard = Arc::clone(&shard);
        let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        async move {
            let job_id = format!("bench-enqueue-{:06}", i);
            shard
                .enqueue(
                    "bench_enqueue",
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
    })
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

async fn bench_enqueue_empty_concurrency(metadata: &GoldenShardMetadata) {
    println!("--- enqueue (empty concurrency queue) ---");
    let (_guard, shard) = clone_golden_shard("enqueue-empty-conc", metadata).await;
    let now = now_ms();

    let limit = Limit::Concurrency(ConcurrencyLimit {
        key: "bench-empty-queue".to_string(),
        max_concurrency: 100,
    });

    let counter = std::sync::atomic::AtomicUsize::new(0);
    let r = bench_op(
        "enqueue_empty_concurrency",
        WARMUP_ITERS,
        MEASURED_ITERS,
        || {
            let shard = Arc::clone(&shard);
            let limit = limit.clone();
            let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                let job_id = format!("bench-enqueue-conc-{:06}", i);
                shard
                    .enqueue(
                        "bench_enqueue",
                        Some(job_id),
                        50,
                        now,
                        None,
                        vec![1, 2, 3],
                        vec![limit],
                        None,
                        "",
                    )
                    .await
                    .expect("enqueue");
            }
        },
    )
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

async fn bench_enqueue_heavy_concurrency(metadata: &GoldenShardMetadata) {
    println!("--- enqueue (heavy concurrency queue: 5 holders + 20K waiters) ---");
    let (_guard, shard) = clone_golden_shard("enqueue-heavy-conc", metadata).await;
    let now = now_ms();

    let limit = Limit::Concurrency(ConcurrencyLimit {
        key: DEEP_QUEUE_KEY.to_string(),
        max_concurrency: DEEP_QUEUE_HOLDERS as u32,
    });

    let counter = std::sync::atomic::AtomicUsize::new(0);
    let r = bench_op(
        "enqueue_heavy_concurrency",
        WARMUP_ITERS,
        MEASURED_ITERS,
        || {
            let shard = Arc::clone(&shard);
            let limit = limit.clone();
            let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                let job_id = format!("bench-enqueue-heavy-{:06}", i);
                shard
                    .enqueue(
                        BENCH_DEEP_QUEUE_TENANT,
                        Some(job_id),
                        50,
                        now,
                        None,
                        vec![1, 2, 3],
                        vec![limit],
                        None,
                        "",
                    )
                    .await
                    .expect("enqueue");
            }
        },
    )
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

async fn bench_dequeue_no_limit(metadata: &GoldenShardMetadata) {
    println!("--- dequeue (no limits, from ~35K Waiting jobs) ---");
    let (_guard, shard) = clone_golden_shard("dequeue-no-limit", metadata).await;

    let r = bench_op("dequeue_no_limit", WARMUP_ITERS, MEASURED_ITERS, || {
        let shard = Arc::clone(&shard);
        async move {
            shard.dequeue("bench-worker", "", 1).await.expect("dequeue");
        }
    })
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

async fn bench_dequeue_heavy_concurrency(metadata: &GoldenShardMetadata) {
    println!("--- dequeue (heavy concurrency: release holder -> grant waiter) ---");
    let (_guard, shard) = clone_golden_shard("dequeue-heavy-conc", metadata).await;

    // Report outcome on one holder to free a slot, then dequeue processes the grant
    let holder_task_id = &metadata.deep_queue_holder_task_ids[0];
    shard
        .report_attempt_outcome(
            holder_task_id,
            AttemptOutcome::Success { result: Vec::new() },
        )
        .await
        .expect("report outcome on holder");

    let r = bench_op(
        "dequeue_heavy_concurrency",
        0, // no warmup — each dequeue consumes a waiter grant
        1, // single measured iteration
        || {
            let shard = Arc::clone(&shard);
            async move {
                shard
                    .dequeue("bench-worker-heavy", "", 1)
                    .await
                    .expect("dequeue");
            }
        },
    )
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

async fn bench_cancel_no_limit(metadata: &GoldenShardMetadata) {
    println!("--- cancel_job (no limits) ---");
    let (_guard, shard) = clone_golden_shard("cancel-no-limit", metadata).await;

    let total_needed = WARMUP_ITERS + MEASURED_ITERS;
    let targets: Vec<(String, String)> = metadata.waiting_jobs[..total_needed].to_vec();
    let counter = std::sync::atomic::AtomicUsize::new(0);

    let r = bench_op("cancel_no_limit", WARMUP_ITERS, MEASURED_ITERS, || {
        let shard = Arc::clone(&shard);
        let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tenant, job_id) = targets[i].clone();
        async move {
            shard
                .cancel_job(&tenant, &job_id)
                .await
                .expect("cancel_job");
        }
    })
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

async fn bench_cancel_with_concurrency(metadata: &GoldenShardMetadata) {
    println!("--- cancel_job (concurrency waiter) ---");
    let (_guard, shard) = clone_golden_shard("cancel-conc", metadata).await;

    let total_needed = WARMUP_ITERS + MEASURED_ITERS;
    let waiter_ids: Vec<String> = metadata.deep_queue_waiter_ids[..total_needed].to_vec();
    let counter = std::sync::atomic::AtomicUsize::new(0);

    let r = bench_op(
        "cancel_with_concurrency",
        WARMUP_ITERS,
        MEASURED_ITERS,
        || {
            let shard = Arc::clone(&shard);
            let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let job_id = waiter_ids[i].clone();
            async move {
                shard
                    .cancel_job(BENCH_DEEP_QUEUE_TENANT, &job_id)
                    .await
                    .expect("cancel_job");
            }
        },
    )
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

async fn bench_expedite_scheduled(metadata: &GoldenShardMetadata) {
    println!("--- expedite_job (far-future scheduled) ---");
    let (_guard, shard) = clone_golden_shard("expedite", metadata).await;

    let total_needed = WARMUP_ITERS + MEASURED_ITERS;
    let targets: Vec<String> = metadata.expedite_job_ids[..total_needed].to_vec();
    let counter = std::sync::atomic::AtomicUsize::new(0);

    let r = bench_op("expedite_scheduled", WARMUP_ITERS, MEASURED_ITERS, || {
        let shard = Arc::clone(&shard);
        let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let job_id = targets[i].clone();
        async move {
            shard
                .expedite_job(BENCH_EXPEDITE_TENANT, &job_id)
                .await
                .expect("expedite_job");
        }
    })
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

async fn bench_import_jobs(metadata: &GoldenShardMetadata) {
    println!("--- import_jobs (batch of 50) ---");
    let (_guard, shard) = clone_golden_shard("import", metadata).await;
    let now = now_ms();

    let counter = std::sync::atomic::AtomicUsize::new(0);
    let r = bench_op("import_jobs(50)", WARMUP_ITERS, MEASURED_ITERS, || {
        let shard = Arc::clone(&shard);
        let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        async move {
            let batch: Vec<ImportJobParams> = (0..50)
                .map(|j| {
                    let idx = i * 50 + j;
                    let job_id = format!("bench-import-{:08}", idx);
                    build_import_params(idx, &job_id, now)
                })
                .collect();
            shard
                .import_jobs("bench_import", batch)
                .await
                .expect("import_jobs");
        }
    })
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

async fn bench_heartbeat(metadata: &GoldenShardMetadata) {
    println!("--- heartbeat_task ---");
    let (_guard, shard) = clone_golden_shard("heartbeat", metadata).await;

    // Setup: enqueue + dequeue 5 jobs to get running tasks
    let (worker_id, task_ids) = enqueue_and_dequeue_jobs(&shard, "bench_heartbeat", 5).await;

    // Heartbeat is non-destructive, so we cycle through the same tasks
    let counter = std::sync::atomic::AtomicUsize::new(0);
    let r = bench_op("heartbeat_task", WARMUP_ITERS, MEASURED_ITERS, || {
        let shard = Arc::clone(&shard);
        let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let task_id = task_ids[i % task_ids.len()].clone();
        let worker_id = worker_id.clone();
        async move {
            shard
                .heartbeat_task(&worker_id, &task_id)
                .await
                .expect("heartbeat_task");
        }
    })
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

async fn bench_report_outcome(metadata: &GoldenShardMetadata) {
    println!("--- report_attempt_outcome ---");
    let (_guard, shard) = clone_golden_shard("report-outcome", metadata).await;

    let total_needed = WARMUP_ITERS + MEASURED_ITERS;
    // Setup: enqueue + dequeue enough jobs
    let (_worker_id, task_ids) =
        enqueue_and_dequeue_jobs(&shard, "bench_outcome", total_needed).await;

    let counter = std::sync::atomic::AtomicUsize::new(0);
    let r = bench_op(
        "report_outcome(success)",
        WARMUP_ITERS,
        MEASURED_ITERS,
        || {
            let shard = Arc::clone(&shard);
            let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let task_id = task_ids[i].clone();
            async move {
                shard
                    .report_attempt_outcome(
                        &task_id,
                        AttemptOutcome::Success { result: Vec::new() },
                    )
                    .await
                    .expect("report_attempt_outcome");
            }
        },
    )
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

async fn bench_restart_failed(metadata: &GoldenShardMetadata) {
    println!("--- restart_job (failed) ---");
    let (_guard, shard) = clone_golden_shard("restart", metadata).await;

    let total_needed = WARMUP_ITERS + MEASURED_ITERS;
    let targets: Vec<(String, String)> = metadata.failed_jobs[..total_needed].to_vec();
    let counter = std::sync::atomic::AtomicUsize::new(0);

    let r = bench_op("restart_failed", WARMUP_ITERS, MEASURED_ITERS, || {
        let shard = Arc::clone(&shard);
        let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tenant, job_id) = targets[i].clone();
        async move {
            shard
                .restart_job(&tenant, &job_id)
                .await
                .expect("restart_job");
        }
    })
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

async fn bench_delete_terminal(metadata: &GoldenShardMetadata) {
    println!("--- delete_job (succeeded/terminal) ---");
    let (_guard, shard) = clone_golden_shard("delete", metadata).await;

    let total_needed = WARMUP_ITERS + MEASURED_ITERS;
    let targets: Vec<(String, String)> = metadata.succeeded_jobs[..total_needed].to_vec();
    let counter = std::sync::atomic::AtomicUsize::new(0);

    let r = bench_op("delete_terminal", WARMUP_ITERS, MEASURED_ITERS, || {
        let shard = Arc::clone(&shard);
        let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tenant, job_id) = targets[i].clone();
        async move {
            shard
                .delete_job(&tenant, &job_id)
                .await
                .expect("delete_job");
        }
    })
    .await;
    r.print();

    shard.close().await.expect("close");
    println!();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    println!("\n========================================");
    println!("Big Shard Operations Benchmark");
    println!("========================================\n");

    let metadata = ensure_golden_shard().await;

    println!(
        "Golden shard: {} total jobs, checkpoint={}\n",
        metadata.total_jobs, metadata.checkpoint_id
    );

    // Read-only benchmarks (no clone needed)
    bench_get_job(&metadata).await;
    bench_get_jobs_batch(&metadata).await;

    // Mutation benchmarks (each clones the golden shard)
    bench_enqueue_no_limit(&metadata).await;
    bench_enqueue_empty_concurrency(&metadata).await;
    bench_enqueue_heavy_concurrency(&metadata).await;
    bench_dequeue_no_limit(&metadata).await;
    bench_dequeue_heavy_concurrency(&metadata).await;
    bench_cancel_no_limit(&metadata).await;
    bench_cancel_with_concurrency(&metadata).await;
    bench_expedite_scheduled(&metadata).await;
    bench_import_jobs(&metadata).await;
    bench_heartbeat(&metadata).await;
    bench_report_outcome(&metadata).await;
    bench_restart_failed(&metadata).await;
    bench_delete_terminal(&metadata).await;

    println!("Done.");
}
