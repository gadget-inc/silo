use serde_json::json;
use silo::job_store_shard::JobStoreShard;
use silo::settings::{Backend, DatabaseConfig};
use std::sync::Arc;
use std::time::Instant;

async fn open_temp_shard(
    flush_interval_ms: Option<u64>,
) -> (tempfile::TempDir, Arc<JobStoreShard>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = DatabaseConfig {
        name: "bench-shard".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
        flush_interval_ms,
    };
    let shard = JobStoreShard::open(&cfg).await.expect("open shard");
    (tmp, Arc::new(shard))
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

async fn measure_enqueue_throughput(
    flush_ms: u64,
    num_producers: usize,
    jobs_per_producer: usize,
) -> f64 {
    let (_tmp, shard) = open_temp_shard(Some(flush_ms)).await;
    let now_ms = now_ms();

    let start = Instant::now();

    // Spawn parallel producers
    let mut handles = vec![];
    for producer_id in 0..num_producers {
        let shard = Arc::clone(&shard);
        let handle = tokio::spawn(async move {
            for i in 0..jobs_per_producer {
                let payload = json!({"producer": producer_id, "i": i});
                shard
                    .enqueue("-", None, 50, now_ms, None, payload, vec![])
                    .await
                    .expect("enqueue");
            }
        });
        handles.push(handle);
    }

    // Wait for all producers to complete
    for handle in handles {
        handle.await.expect("producer task");
    }

    // Close to ensure durable flush
    shard.close().await.expect("close");

    let elapsed = start.elapsed();
    let total_jobs = (num_producers * jobs_per_producer) as f64;

    total_jobs / elapsed.as_secs_f64()
}

async fn measure_dequeue_throughput(
    flush_ms: u64,
    num_consumers: usize,
    total_jobs: usize,
    batch: usize,
) -> f64 {
    let (_tmp, shard) = open_temp_shard(Some(flush_ms)).await;
    let now_ms = now_ms();

    // Seed tasks
    for i in 0..total_jobs {
        let payload = json!({"i": i});
        shard
            .enqueue("-", None, 50, now_ms, None, payload, vec![])
            .await
            .expect("enqueue");
    }

    let start = Instant::now();

    // Spawn parallel consumers
    let mut handles = vec![];
    let processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    for consumer_id in 0..num_consumers {
        let shard = Arc::clone(&shard);
        let worker_id = format!("worker-{}", consumer_id);
        let processed = Arc::clone(&processed);
        let handle = tokio::spawn(async move {
            loop {
                if processed.load(std::sync::atomic::Ordering::Relaxed) >= total_jobs {
                    break;
                }

                let tasks = shard
                    .dequeue("-", &worker_id, batch)
                    .await
                    .expect("dequeue");
                if tasks.is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    continue;
                }

                // Report success to release leases
                for t in &tasks {
                    let tid = t.attempt().task_id().to_string();
                    shard
                        .report_attempt_outcome(
                            "-",
                            &tid,
                            silo::job_attempt::AttemptOutcome::Success { result: Vec::new() },
                        )
                        .await
                        .expect("report");
                }
                processed.fetch_add(tasks.len(), std::sync::atomic::Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    // Wait for all consumers to complete
    for handle in handles {
        handle.await.expect("consumer task");
    }

    let elapsed = start.elapsed();
    shard.close().await.expect("close");

    total_jobs as f64 / elapsed.as_secs_f64()
}

#[tokio::main]
async fn main() {
    println!("\n========================================");
    println!("Job Store Shard Throughput Benchmark");
    println!("========================================\n");

    println!("Testing with 50ms flush interval\n");

    println!("--- Enqueue Throughput ---");
    let enq_1p = measure_enqueue_throughput(50, 1, 50).await;
    println!("  1 producer  x 50  jobs: {:.0} jobs/sec", enq_1p);

    let enq_4p = measure_enqueue_throughput(50, 4, 50).await;
    println!(
        "  4 producers x 50  jobs: {:.0} jobs/sec ({}x speedup)",
        enq_4p,
        enq_4p / enq_1p
    );

    let enq_8p = measure_enqueue_throughput(50, 8, 25).await;
    println!(
        "  8 producers x 25  jobs: {:.0} jobs/sec ({}x speedup)\n",
        enq_8p,
        enq_8p / enq_1p
    );

    println!("--- Dequeue Throughput ---");
    let deq_1c = measure_dequeue_throughput(50, 1, 100, 32).await;
    println!(
        "  1 consumer  x 100 jobs (batch 32): {:.0} jobs/sec",
        deq_1c
    );

    let deq_4c = measure_dequeue_throughput(50, 4, 100, 32).await;
    println!(
        "  4 consumers x 100 jobs (batch 32): {:.0} jobs/sec ({}x speedup)",
        deq_4c,
        deq_4c / deq_1c
    );

    let deq_8c = measure_dequeue_throughput(50, 8, 200, 32).await;
    println!(
        "  8 consumers x 200 jobs (batch 32): {:.0} jobs/sec ({}x speedup)\n",
        deq_8c,
        deq_8c / deq_1c
    );
}
