//! A/B benchmark for the terminal-job expiration feature.
//!
//! Measures `report_attempt_outcome` throughput on the hot termination path
//! both with and without `completed_job_expire_s` enabled, so we can quantify
//! the cost of the extra reads/writes (`JOB_INFO`, `IDX_METADATA`, attempt
//! scan, and re-puts with TTL). The bench drives Succeeded outcomes, so only
//! `completed_job_expire_s` is exercised.
//!
//! Run with: `cargo bench --bench job_termination`.

use silo::gubernator::NullGubernatorClient;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::settings::{Backend, DatabaseConfig, SEVEN_DAYS_S};
use silo::shard_range::ShardRange;
use std::sync::Arc;
use std::time::{Duration, Instant};

async fn open_temp_shard(
    flush_interval_ms: u64,
    completed_job_expire_s: Option<u64>,
) -> (tempfile::TempDir, Arc<JobStoreShard>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = DatabaseConfig {
        name: "bench-shard".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
        slatedb: Some(slatedb::config::Settings {
            flush_interval: Some(Duration::from_millis(flush_interval_ms)),
            ..Default::default()
        }),
        completed_job_expire_s,
        ..Default::default()
    };
    let shard = JobStoreShard::open(&cfg, NullGubernatorClient::new(), None, ShardRange::full())
        .await
        .expect("open shard");
    (tmp, shard)
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn make_metadata(n: usize) -> Vec<(String, String)> {
    (0..n).map(|i| (format!("k{i}"), format!("v{i}"))).collect()
}

/// Seed `total_jobs` jobs (with `metadata_n` metadata pairs each), then
/// dequeue and successfully complete them across `num_consumers` workers.
/// Returns jobs/sec for the dequeue+terminate phase only (the seed phase is
/// excluded from the timer).
async fn measure_termination_throughput(
    flush_ms: u64,
    completed_job_expire_s: Option<u64>,
    num_consumers: usize,
    total_jobs: usize,
    batch: usize,
    metadata_n: usize,
) -> f64 {
    let (_tmp, shard) = open_temp_shard(flush_ms, completed_job_expire_s).await;
    let now_ms = now_ms();

    // Seed
    let metadata = make_metadata(metadata_n);
    for i in 0..total_jobs {
        let payload = rmp_serde::to_vec(&serde_json::json!({"i": i})).expect("serialize payload");
        shard
            .enqueue(
                "-",
                None,
                50,
                now_ms,
                None,
                payload,
                vec![],
                Some(metadata.clone()),
                "",
            )
            .await
            .expect("enqueue");
    }

    let start = Instant::now();

    let processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut handles = vec![];
    for consumer_id in 0..num_consumers {
        let shard = Arc::clone(&shard);
        let worker_id = format!("worker-{consumer_id}");
        let processed = Arc::clone(&processed);
        let handle = tokio::spawn(async move {
            loop {
                if processed.load(std::sync::atomic::Ordering::Relaxed) >= total_jobs {
                    break;
                }

                let result = shard.dequeue(&worker_id, "", batch).await.expect("dequeue");
                if result.tasks.is_empty() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }

                for t in &result.tasks {
                    let tid = t.attempt().task_id().to_string();
                    shard
                        .report_attempt_outcome(
                            &tid,
                            AttemptOutcome::Success { result: Vec::new() },
                        )
                        .await
                        .expect("report");
                }
                processed.fetch_add(result.tasks.len(), std::sync::atomic::Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.expect("consumer task");
    }

    let elapsed = start.elapsed();
    shard.close().await.expect("close");

    total_jobs as f64 / elapsed.as_secs_f64()
}

async fn ab_row(label: &str, num_consumers: usize, total_jobs: usize, metadata_n: usize) {
    let baseline =
        measure_termination_throughput(1, None, num_consumers, total_jobs, 32, metadata_n).await;
    let with_ttl = measure_termination_throughput(
        1,
        Some(SEVEN_DAYS_S),
        num_consumers,
        total_jobs,
        32,
        metadata_n,
    )
    .await;
    let overhead_pct = (baseline - with_ttl) / baseline * 100.0;
    println!(
        "  {label:<48} baseline {baseline:>7.0} jobs/sec | TTL on {with_ttl:>7.0} jobs/sec | overhead {overhead_pct:>5.1}%"
    );
}

#[tokio::main]
async fn main() {
    println!("\n========================================");
    println!("Job Termination A/B Benchmark");
    println!("(completed_job_expire_s None vs Some(7d))");
    println!("========================================\n");

    println!("--- Single consumer, 1000 jobs ---");
    ab_row("0 metadata pairs", 1, 1000, 0).await;
    ab_row("3 metadata pairs", 1, 1000, 3).await;
    ab_row("10 metadata pairs", 1, 1000, 10).await;

    println!("\n--- 4 consumers, 2000 jobs ---");
    ab_row("0 metadata pairs", 4, 2000, 0).await;
    ab_row("3 metadata pairs", 4, 2000, 3).await;
    ab_row("10 metadata pairs", 4, 2000, 10).await;

    println!("\n--- 8 consumers, 4000 jobs ---");
    ab_row("3 metadata pairs", 8, 4000, 3).await;
}
