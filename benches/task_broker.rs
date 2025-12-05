//! Benchmark for TaskBroker inflight set operations
//!
//! This benchmark isolates the performance of the claim/ack/requeue operations
//! to measure the impact of lock-free vs mutex-based inflight tracking.
//!
//! Run with: cargo bench --bench task_broker --features bench

use silo::concurrency::ConcurrencyManager;
use silo::job_store_shard::Task;
use silo::settings::{Backend, DatabaseConfig};
use silo::task_broker::TaskBroker;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

async fn setup_broker() -> (tempfile::TempDir, Arc<TaskBroker>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = DatabaseConfig {
        name: "bench-broker".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
        flush_interval_ms: Some(100),
        wal: None,
    };

    // Open the database directly
    let object_store =
        silo::storage::resolve_object_store(&cfg.backend, &cfg.path).expect("object store");
    let db = slatedb::DbBuilder::new(cfg.path.as_str(), object_store)
        .build()
        .await
        .expect("open db");
    let db = Arc::new(db);

    let concurrency = Arc::new(ConcurrencyManager::new());
    let broker = TaskBroker::new(db, concurrency);
    (tmp, broker)
}

/// Measure claim_ready + ack_durable cycle throughput with multiple threads
async fn measure_claim_ack_throughput(num_threads: usize, ops_per_thread: usize) -> f64 {
    let (_tmp, broker) = setup_broker().await;
    let broker = Arc::clone(&broker);

    // Pre-populate the buffer with fake tasks
    let total_ops = num_threads * ops_per_thread;
    for i in 0..total_ops * 2 {
        let key = format!("tasks/{:020}/{:03}/job-{}/1", 1000 + i, 50, i);
        let task = Task::RunAttempt {
            id: format!("task-{}", i),
            job_id: format!("job-{}", i),
            attempt_number: 1,
            held_queues: vec![],
        };
        broker.insert_for_test(&key, task);
    }

    let start = Instant::now();
    let claimed_count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];
    for _ in 0..num_threads {
        let broker = Arc::clone(&broker);
        let claimed_count = Arc::clone(&claimed_count);
        let handle = tokio::spawn(async move {
            let mut local_claimed = 0;
            while local_claimed < ops_per_thread {
                let tasks = broker.claim_ready(16);
                if tasks.is_empty() {
                    break;
                }
                let keys: Vec<String> = tasks.iter().map(|t| t.key.clone()).collect();
                broker.ack_durable(&keys);
                local_claimed += tasks.len();
            }
            claimed_count.fetch_add(local_claimed, Ordering::Relaxed);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("task");
    }

    let elapsed = start.elapsed();
    let total = claimed_count.load(Ordering::Relaxed);
    total as f64 / elapsed.as_secs_f64()
}

/// Measure claim + requeue cycle (simulating failed durable writes)
async fn measure_claim_requeue_throughput(num_threads: usize, ops_per_thread: usize) -> f64 {
    let (_tmp, broker) = setup_broker().await;
    let broker = Arc::clone(&broker);

    // Pre-populate with tasks
    let total_ops = num_threads * ops_per_thread;
    for i in 0..total_ops {
        let key = format!("tasks/{:020}/{:03}/job-{}/1", 1000 + i, 50, i);
        let task = Task::RunAttempt {
            id: format!("task-{}", i),
            job_id: format!("job-{}", i),
            attempt_number: 1,
            held_queues: vec![],
        };
        broker.insert_for_test(&key, task);
    }

    let start = Instant::now();
    let ops_count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];
    for _ in 0..num_threads {
        let broker = Arc::clone(&broker);
        let ops_count = Arc::clone(&ops_count);
        let handle = tokio::spawn(async move {
            let mut local_ops = 0;
            while local_ops < ops_per_thread {
                let tasks = broker.claim_ready(8);
                if tasks.is_empty() {
                    tokio::task::yield_now().await;
                    continue;
                }
                // Requeue them (simulating failed durable write)
                broker.requeue(tasks.clone());
                local_ops += tasks.len();
            }
            ops_count.fetch_add(local_ops, Ordering::Relaxed);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("task");
    }

    let elapsed = start.elapsed();
    let total = ops_count.load(Ordering::Relaxed);
    total as f64 / elapsed.as_secs_f64()
}

/// Measure high contention scenario: many threads competing for few tasks
async fn measure_contention_throughput(num_threads: usize) -> f64 {
    let (_tmp, broker) = setup_broker().await;
    let broker = Arc::clone(&broker);

    // Small number of tasks = high contention
    for i in 0..100 {
        let key = format!("tasks/{:020}/{:03}/job-{}/1", 1000 + i, 50, i);
        let task = Task::RunAttempt {
            id: format!("task-{}", i),
            job_id: format!("job-{}", i),
            attempt_number: 1,
            held_queues: vec![],
        };
        broker.insert_for_test(&key, task);
    }

    let total_ops = 10000;
    let ops_per_thread = total_ops / num_threads;

    let start = Instant::now();
    let ops_count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];
    for _ in 0..num_threads {
        let broker = Arc::clone(&broker);
        let ops_count = Arc::clone(&ops_count);
        let handle = tokio::spawn(async move {
            let mut local_ops = 0;
            while local_ops < ops_per_thread {
                let tasks = broker.claim_ready(1);
                if !tasks.is_empty() {
                    let keys: Vec<String> = tasks.iter().map(|t| t.key.clone()).collect();
                    broker.ack_durable(&keys);
                    local_ops += 1;
                    // Immediately add the task back for more contention
                    for t in tasks {
                        broker.insert_for_test(&t.key, t.task.clone());
                    }
                }
            }
            ops_count.fetch_add(local_ops, Ordering::Relaxed);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("task");
    }

    let elapsed = start.elapsed();
    let total = ops_count.load(Ordering::Relaxed);
    total as f64 / elapsed.as_secs_f64()
}

/// Measure inflight_len() call overhead
fn measure_inflight_len_throughput(broker: &TaskBroker, iterations: usize) -> f64 {
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = broker.inflight_len();
    }
    let elapsed = start.elapsed();
    iterations as f64 / elapsed.as_secs_f64()
}

#[tokio::main]
async fn main() {
    println!("\n========================================");
    println!("TaskBroker Inflight Set Benchmark");
    println!("========================================\n");

    println!("--- Claim/Ack Throughput (varying threads) ---");
    let ops = 5000;

    let t1 = measure_claim_ack_throughput(1, ops).await;
    println!("  1 thread  x {} ops: {:>10.0} ops/sec", ops, t1);

    let t2 = measure_claim_ack_throughput(2, ops).await;
    println!(
        "  2 threads x {} ops: {:>10.0} ops/sec ({:.2}x)",
        ops,
        t2,
        t2 / t1
    );

    let t4 = measure_claim_ack_throughput(4, ops).await;
    println!(
        "  4 threads x {} ops: {:>10.0} ops/sec ({:.2}x)",
        ops,
        t4,
        t4 / t1
    );

    let t8 = measure_claim_ack_throughput(8, ops).await;
    println!(
        "  8 threads x {} ops: {:>10.0} ops/sec ({:.2}x)\n",
        ops,
        t8,
        t8 / t1
    );

    println!("--- Claim/Requeue Throughput (simulating retries) ---");
    let r1 = measure_claim_requeue_throughput(1, 2000).await;
    println!("  1 thread:  {:>10.0} ops/sec", r1);

    let r4 = measure_claim_requeue_throughput(4, 2000).await;
    println!("  4 threads: {:>10.0} ops/sec ({:.2}x)\n", r4, r4 / r1);

    println!("--- High Contention Scenario ---");
    let c2 = measure_contention_throughput(2).await;
    println!("  2 threads:  {:>10.0} ops/sec", c2);

    let c4 = measure_contention_throughput(4).await;
    println!("  4 threads:  {:>10.0} ops/sec ({:.2}x)", c4, c4 / c2);

    let c8 = measure_contention_throughput(8).await;
    println!("  8 threads:  {:>10.0} ops/sec ({:.2}x)", c8, c8 / c2);

    let c16 = measure_contention_throughput(16).await;
    println!("  16 threads: {:>10.0} ops/sec ({:.2}x)\n", c16, c16 / c2);

    println!("--- inflight_len() Overhead ---");
    let (_tmp, broker) = setup_broker().await;
    // Add some items to inflight
    for i in 0..1000 {
        let key = format!("tasks/{:020}/{:03}/job-{}/1", 1000 + i, 50, i);
        let task = Task::RunAttempt {
            id: format!("task-{}", i),
            job_id: format!("job-{}", i),
            attempt_number: 1,
            held_queues: vec![],
        };
        broker.insert_for_test(&key, task);
    }
    // Claim them to populate inflight
    let _ = broker.claim_ready(1000);

    let len_throughput = measure_inflight_len_throughput(&broker, 100_000);
    println!(
        "  1000 items in inflight: {:>10.0} calls/sec\n",
        len_throughput
    );
}
