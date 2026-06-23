//! Noisy-neighbor grant-scanner fairness benchmark.
//!
//! Two tenants live on the SAME shard (same `JobStoreShard`, same grant scanner
//! and task broker registry):
//!
//!   * Tenant A ("the noisy neighbor") enqueues a large number of jobs spread
//!     across many concurrency queues. Every queue has a tight static
//!     `Concurrency(max=1)` limit plus a wide `FloatingConcurrency(max=360)`
//!     limit, so each queue keeps a deep backlog of pending concurrency
//!     *requesters* that the grant scanner must repeatedly walk as holders are
//!     released and re-granted.
//!
//!   * Tenant B ("the victim") enqueues far fewer jobs, with the same shape of
//!     limits.
//!
//! We measure tenant B's end-to-end completion throughput (jobs/sec) both in
//! isolation and while tenant A is hammering the shared grant scanner. If the
//! scanner is fair — bounded per-invocation scan budget and per-queue parallel
//! fan-out — tenant B's throughput should barely move when A is present. If the
//! scanner processes queues serially and walks unbounded backlogs, tenant A
//! starves B and the "noisy" throughput collapses.
//!
//! The runtime is intentionally multi-threaded (`worker_threads >= 2`) so the
//! background grant-scanner task can run on a different worker than the drain
//! loops, matching production.

use silo::gubernator::NullGubernatorClient;
use silo::job::{ConcurrencyLimit, FloatingConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::settings::{Backend, DatabaseConfig};
use silo::shard_range::ShardRange;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---- Workload knobs (override via env for quick tuning) ----

/// Number of distinct concurrency queues tenant A spreads its jobs over.
const A_QUEUES: usize = 24;
/// Jobs per tenant-A queue. Deep so each queue carries a long requester backlog.
const A_DEPTH: usize = 160;
/// Number of distinct concurrency queues tenant B spreads its jobs over.
const B_QUEUES: usize = 8;
/// Jobs per tenant-B queue. Far fewer than tenant A overall.
const B_DEPTH: usize = 60;

/// Worker (consumer) count per tenant.
const A_WORKERS: usize = 8;
const B_WORKERS: usize = 8;
/// Dequeue batch size per claim.
const BATCH: usize = 8;

/// Static (tight) concurrency limit — only one holder per queue at a time.
const TIGHT_MAX: u32 = 1;
/// Floating (wide) concurrency limit — effectively never binding; it exists to
/// double the number of requester rows the scanner must walk per job.
const FLOAT_MAX: u32 = 360;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

async fn open_temp_shard(flush_interval_ms: u64) -> (tempfile::TempDir, Arc<JobStoreShard>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = DatabaseConfig {
        name: "noisy-neighbor-shard".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
        slatedb: Some(slatedb::config::Settings {
            flush_interval: Some(Duration::from_millis(flush_interval_ms)),
            ..Default::default()
        }),
        ..Default::default()
    };
    let shard = JobStoreShard::open(&cfg, NullGubernatorClient::new(), None, ShardRange::full())
        .await
        .expect("open shard");
    (tmp, shard)
}

/// Two limits per job: a tight static concurrency queue (max 1) and a wide
/// floating concurrency queue (max 360). Both are tenant-scoped via the key.
fn job_limits(tight_key: &str, float_key: &str) -> Vec<Limit> {
    vec![
        Limit::Concurrency(ConcurrencyLimit {
            key: tight_key.to_string(),
            max_concurrency: TIGHT_MAX,
        }),
        Limit::FloatingConcurrency(FloatingConcurrencyLimit {
            key: float_key.to_string(),
            default_max_concurrency: FLOAT_MAX,
            // Long refresh interval: we don't want worker-driven refreshes to
            // perturb the measurement.
            refresh_interval_ms: 600_000,
            metadata: vec![],
        }),
    ]
}

/// Enqueue `num_queues * depth` jobs for `tenant` under task group `group`,
/// fanning the enqueues out across producers for fast setup. Returns the total
/// number of jobs enqueued.
async fn enqueue_tenant(
    shard: &Arc<JobStoreShard>,
    tenant: &'static str,
    group: &'static str,
    num_queues: usize,
    depth: usize,
    now: i64,
) -> usize {
    let mut handles = Vec::new();
    for q in 0..num_queues {
        let shard = Arc::clone(shard);
        let handle = tokio::spawn(async move {
            let tight = format!("{tenant}-tight-{q}");
            let float = format!("{tenant}-float-{q}");
            for i in 0..depth {
                let payload = rmp_serde::to_vec(&serde_json::json!({"q": q, "i": i}))
                    .expect("serialize payload");
                shard
                    .enqueue(
                        tenant,
                        None,
                        50,
                        now,
                        None,
                        payload,
                        job_limits(&tight, &float),
                        None,
                        group,
                    )
                    .await
                    .expect("enqueue");
            }
        });
        handles.push(handle);
    }
    for h in handles {
        h.await.expect("enqueue producer");
    }
    num_queues * depth
}

/// Spawn `num_workers` drain workers for a task group. Each worker dequeues,
/// reports success (releasing the concurrency holder so the next requester can
/// be granted), and bumps `counter`. Workers run until `stop` is set.
fn spawn_drain_workers(
    shard: &Arc<JobStoreShard>,
    group: &'static str,
    num_workers: usize,
    batch: usize,
    counter: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    (0..num_workers)
        .map(|w| {
            let shard = Arc::clone(shard);
            let counter = Arc::clone(&counter);
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let worker_id = format!("{group}-w{w}");
                while !stop.load(Ordering::Relaxed) {
                    let result = shard.dequeue(&worker_id, group, batch).await.expect("dequeue");
                    if result.tasks.is_empty() {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                        continue;
                    }
                    for t in &result.tasks {
                        let tid = t.attempt().task_id().to_string();
                        shard
                            .report_attempt_outcome(&tid, AttemptOutcome::Success { result: Vec::new() })
                            .await
                            .expect("report");
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect()
}

/// Drive tenant B's workers until all `b_total` jobs complete, returning the
/// elapsed wall-clock time. Assumes any noisy-neighbor load is already running.
async fn measure_drain(
    shard: &Arc<JobStoreShard>,
    group: &'static str,
    num_workers: usize,
    batch: usize,
    b_total: usize,
) -> Duration {
    let counter = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let handles = spawn_drain_workers(shard, group, num_workers, batch, Arc::clone(&counter), Arc::clone(&stop));

    while counter.load(Ordering::Relaxed) < b_total {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let elapsed = start.elapsed();

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }
    elapsed
}

/// Tenant B draining a shard all to itself — the baseline.
async fn scenario_isolated(flush_ms: u64, b_queues: usize, b_depth: usize) -> f64 {
    let (_tmp, shard) = open_temp_shard(flush_ms).await;
    let now = now_ms();
    let b_total = enqueue_tenant(&shard, "tenant-b", "group-b", b_queues, b_depth, now).await;

    let elapsed = measure_drain(&shard, "group-b", B_WORKERS, BATCH, b_total).await;
    shard.close().await.expect("close");
    b_total as f64 / elapsed.as_secs_f64()
}

/// Tenant B draining while tenant A hammers the shared grant scanner.
async fn scenario_noisy(
    flush_ms: u64,
    a_queues: usize,
    a_depth: usize,
    b_queues: usize,
    b_depth: usize,
) -> f64 {
    let (_tmp, shard) = open_temp_shard(flush_ms).await;
    let now = now_ms();

    let a_total = enqueue_tenant(&shard, "tenant-a", "group-a", a_queues, a_depth, now).await;
    let b_total = enqueue_tenant(&shard, "tenant-b", "group-b", b_queues, b_depth, now).await;
    println!("  (tenant A backlog: {a_total} jobs across {a_queues} queues; tenant B: {b_total} jobs)");

    // Start tenant A's churn and let it ramp before timing B, so the scanner is
    // already under sustained load when B begins draining.
    let a_counter = Arc::new(AtomicUsize::new(0));
    let a_stop = Arc::new(AtomicBool::new(false));
    let a_handles = spawn_drain_workers(
        &shard,
        "group-a",
        A_WORKERS,
        BATCH,
        Arc::clone(&a_counter),
        Arc::clone(&a_stop),
    );
    tokio::time::sleep(Duration::from_millis(200)).await;

    let elapsed = measure_drain(&shard, "group-b", B_WORKERS, BATCH, b_total).await;

    // Stop the noisy neighbor.
    a_stop.store(true, Ordering::Relaxed);
    for h in a_handles {
        let _ = h.await;
    }
    let a_done = a_counter.load(Ordering::Relaxed);
    println!("  (tenant A completed {a_done} jobs while B drained)");

    shard.close().await.expect("close");
    b_total as f64 / elapsed.as_secs_f64()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let flush_ms: u64 = env_usize("NN_FLUSH_MS", 1) as u64;
    let a_queues = env_usize("NN_A_QUEUES", A_QUEUES);
    let a_depth = env_usize("NN_A_DEPTH", A_DEPTH);
    let b_queues = env_usize("NN_B_QUEUES", B_QUEUES);
    let b_depth = env_usize("NN_B_DEPTH", B_DEPTH);

    println!("\n=================================================");
    println!("Noisy-Neighbor Grant-Scanner Throughput Benchmark");
    println!("=================================================\n");
    println!(
        "runtime: multi_thread (4 workers) | tight max={TIGHT_MAX}, floating max={FLOAT_MAX}\n"
    );

    println!("--- Scenario 1: tenant B alone (baseline) ---");
    let isolated = scenario_isolated(flush_ms, b_queues, b_depth).await;
    println!("  tenant B throughput: {isolated:.0} jobs/sec\n");

    println!("--- Scenario 2: tenant B + noisy tenant A on same shard ---");
    let noisy = scenario_noisy(flush_ms, a_queues, a_depth, b_queues, b_depth).await;
    println!("  tenant B throughput: {noisy:.0} jobs/sec\n");

    let retained = if isolated > 0.0 {
        100.0 * noisy / isolated
    } else {
        0.0
    };
    println!("=================================================");
    println!("tenant B retains {retained:.1}% of baseline throughput under noisy neighbor");
    println!("(higher = scanner is fair; A is not hogging it)");
    println!("=================================================\n");
}
