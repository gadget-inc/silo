//! Benchmark tool for measuring Silo task throughput against a remote server.
//!
//! This tool connects to a running Silo node via gRPC and simulates concurrent workers
//! polling and executing tasks. It reports the rate of task execution in real-time.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use silo::pb::report_outcome_request::Outcome;
use silo::pb::silo_client::SiloClient;
use silo::pb::{
    ConcurrencyLimit, EnqueueRequest, JsonValueBytes, LeaseTasksRequest, Limit, ReportOutcomeRequest,
};
use tonic::transport::Channel;

#[derive(Parser, Debug)]
#[command(name = "silo-bench")]
#[command(about = "Benchmark tool for measuring Silo task throughput")]
struct Args {
    /// Address of the Silo server (e.g., http://localhost:50051)
    #[arg(long, short = 'a')]
    address: String,

    /// Number of concurrent workers
    #[arg(long, short = 'w', default_value = "8")]
    workers: u32,

    /// Shard name to target (use "all" to target shards 0 through num-shards-1)
    #[arg(long, short = 's', default_value = "0")]
    shard: String,

    /// Number of shards when using shard=all
    #[arg(long, default_value = "8")]
    num_shards: u32,

    /// Duration to run the benchmark, in seconds
    #[arg(long, short = 'd', default_value = "30")]
    duration_secs: u64,

    /// Maximum tasks to request per poll
    #[arg(long, default_value = "4")]
    max_tasks_per_poll: u32,

    /// Enable enqueuing tasks during the benchmark
    #[arg(long)]
    enqueue: bool,

    /// Number of concurrent enqueuers (only used with --enqueue)
    #[arg(long, default_value = "4")]
    enqueuers: u32,

    /// Concurrency limit key for enqueued tasks
    #[arg(long, default_value = "bench-queue")]
    concurrency_key: String,

    /// Max concurrency for enqueued tasks
    #[arg(long, default_value = "100")]
    max_concurrency: u32,

    /// Report interval in seconds
    #[arg(long, default_value = "1")]
    report_interval_secs: u64,

    /// Optional tenant for multi-tenant mode
    #[arg(long)]
    tenant: Option<String>,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn connect(address: &str) -> anyhow::Result<SiloClient<Channel>> {
    let channel = Channel::from_shared(address.to_string())?
        .connect()
        .await?;
    Ok(SiloClient::new(channel))
}

/// Worker task that polls for tasks and reports success outcomes
async fn worker_loop(
    client: SiloClient<Channel>,
    worker_id: String,
    shards: Vec<String>,
    max_tasks: u32,
    running: Arc<AtomicBool>,
    completed_count: Arc<AtomicU64>,
    poll_count: Arc<AtomicU64>,
    empty_poll_count: Arc<AtomicU64>,
    tenant: Option<String>,
) {
    let mut client = client;
    let mut shard_idx = 0usize;

    while running.load(Ordering::Relaxed) {
        // Round-robin across shards
        let shard = &shards[shard_idx % shards.len()];
        shard_idx = shard_idx.wrapping_add(1);

        let request = LeaseTasksRequest {
            shard: shard.clone(),
            worker_id: worker_id.clone(),
            max_tasks,
            tenant: tenant.clone(),
        };

        poll_count.fetch_add(1, Ordering::Relaxed);

        match client.lease_tasks(request).await {
            Ok(response) => {
                let tasks = response.into_inner().tasks;
                if tasks.is_empty() {
                    empty_poll_count.fetch_add(1, Ordering::Relaxed);
                    // Small sleep when no tasks to avoid tight polling
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                }

                for task in tasks {
                    // Report success immediately (simulating instant task execution)
                    let outcome_request = ReportOutcomeRequest {
                        shard: shard.clone(),
                        task_id: task.id.clone(),
                        tenant: tenant.clone(),
                        outcome: Some(Outcome::Success(JsonValueBytes {
                            data: b"{}".to_vec(),
                        })),
                    };

                    match client.report_outcome(outcome_request).await {
                        Ok(_) => {
                            completed_count.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            eprintln!("Worker {} failed to report outcome: {}", worker_id, e);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Worker {} poll failed: {}", worker_id, e);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

/// Enqueuer task that continuously enqueues tasks
async fn enqueuer_loop(
    client: SiloClient<Channel>,
    enqueuer_id: String,
    shards: Vec<String>,
    concurrency_key: String,
    max_concurrency: u32,
    running: Arc<AtomicBool>,
    enqueued_count: Arc<AtomicU64>,
    tenant: Option<String>,
) {
    let mut client = client;
    let mut shard_idx = 0usize;
    let mut job_counter = 0u64;

    while running.load(Ordering::Relaxed) {
        let shard = &shards[shard_idx % shards.len()];
        shard_idx = shard_idx.wrapping_add(1);

        let payload = serde_json::json!({
            "enqueuer": enqueuer_id,
            "job": job_counter,
            "timestamp": now_ms(),
        });

        let request = EnqueueRequest {
            shard: shard.clone(),
            id: String::new(), // Let server generate ID
            priority: (job_counter % 100) as u32,
            start_at_ms: now_ms(),
            retry_policy: None,
            payload: Some(JsonValueBytes {
                data: serde_json::to_vec(&payload).unwrap(),
            }),
            limits: vec![Limit {
                limit: Some(silo::pb::limit::Limit::Concurrency(ConcurrencyLimit {
                    key: concurrency_key.clone(),
                    max_concurrency,
                })),
            }],
            tenant: tenant.clone(),
            metadata: Default::default(),
        };

        match client.enqueue(request).await {
            Ok(_) => {
                enqueued_count.fetch_add(1, Ordering::Relaxed);
                job_counter = job_counter.wrapping_add(1);
            }
            Err(e) => {
                eprintln!("Enqueuer {} failed to enqueue: {}", enqueuer_id, e);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        // Yield occasionally to let other tasks run
        if job_counter % 50 == 0 {
            tokio::task::yield_now().await;
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    println!("Silo Benchmark Tool");
    println!("==================");
    println!("Server: {}", args.address);
    println!("Workers: {}", args.workers);
    println!("Duration: {}s", args.duration_secs);
    if args.enqueue {
        println!("Enqueuers: {}", args.enqueuers);
        println!("Concurrency key: {}", args.concurrency_key);
        println!("Max concurrency: {}", args.max_concurrency);
    }
    println!();

    // Determine which shards to target
    let shards: Vec<String> = if args.shard == "all" {
        (0..args.num_shards).map(|i| i.to_string()).collect()
    } else {
        vec![args.shard.clone()]
    };
    println!("Targeting shards: {:?}", shards);

    // Test connection
    println!("Connecting to server...");
    let test_client = connect(&args.address).await?;
    drop(test_client);
    println!("Connection successful!");
    println!();

    // Shared counters
    let running = Arc::new(AtomicBool::new(true));
    let completed_count = Arc::new(AtomicU64::new(0));
    let poll_count = Arc::new(AtomicU64::new(0));
    let empty_poll_count = Arc::new(AtomicU64::new(0));
    let enqueued_count = Arc::new(AtomicU64::new(0));

    // Spawn worker tasks
    let mut handles = Vec::new();
    for i in 0..args.workers {
        let client = connect(&args.address).await?;
        let worker_id = format!("bench-worker-{}", i);
        let shards = shards.clone();
        let running = running.clone();
        let completed = completed_count.clone();
        let polls = poll_count.clone();
        let empty_polls = empty_poll_count.clone();
        let tenant = args.tenant.clone();

        handles.push(tokio::spawn(worker_loop(
            client,
            worker_id,
            shards,
            args.max_tasks_per_poll,
            running,
            completed,
            polls,
            empty_polls,
            tenant,
        )));
    }

    // Spawn enqueuer tasks if enabled
    if args.enqueue {
        for i in 0..args.enqueuers {
            let client = connect(&args.address).await?;
            let enqueuer_id = format!("bench-enqueuer-{}", i);
            let shards = shards.clone();
            let running = running.clone();
            let enqueued = enqueued_count.clone();
            let concurrency_key = args.concurrency_key.clone();
            let max_concurrency = args.max_concurrency;
            let tenant = args.tenant.clone();

            handles.push(tokio::spawn(enqueuer_loop(
                client,
                enqueuer_id,
                shards,
                concurrency_key,
                max_concurrency,
                running,
                enqueued,
                tenant,
            )));
        }
    }

    // Reporting loop
    let start = Instant::now();
    let report_interval = Duration::from_secs(args.report_interval_secs);
    let mut last_completed = 0u64;
    let mut last_enqueued = 0u64;
    let mut last_report = Instant::now();

    println!("Starting benchmark...\n");
    println!(
        "{:>8} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "Elapsed", "Completed", "Rate/s", "Enqueued", "Enq Rate/s", "Empty Polls"
    );
    println!("{}", "-".repeat(72));

    while start.elapsed() < Duration::from_secs(args.duration_secs) {
        tokio::time::sleep(report_interval).await;

        let elapsed = start.elapsed();
        let current_completed = completed_count.load(Ordering::Relaxed);
        let current_enqueued = enqueued_count.load(Ordering::Relaxed);
        let current_empty_polls = empty_poll_count.load(Ordering::Relaxed);

        let interval_secs = last_report.elapsed().as_secs_f64();
        let completed_delta = current_completed - last_completed;
        let enqueued_delta = current_enqueued - last_enqueued;
        let rate = completed_delta as f64 / interval_secs;
        let enqueue_rate = enqueued_delta as f64 / interval_secs;

        println!(
            "{:>7.1}s {:>12} {:>12.1} {:>12} {:>12.1} {:>12}",
            elapsed.as_secs_f64(),
            current_completed,
            rate,
            current_enqueued,
            enqueue_rate,
            current_empty_polls
        );

        last_completed = current_completed;
        last_enqueued = current_enqueued;
        last_report = Instant::now();
    }

    // Stop workers
    println!("\nStopping workers...");
    running.store(false, Ordering::Relaxed);

    // Wait for all workers to finish
    for handle in handles {
        let _ = handle.await;
    }

    // Final statistics
    let total_elapsed = start.elapsed();
    let total_completed = completed_count.load(Ordering::Relaxed);
    let total_polls = poll_count.load(Ordering::Relaxed);
    let total_empty_polls = empty_poll_count.load(Ordering::Relaxed);
    let total_enqueued = enqueued_count.load(Ordering::Relaxed);

    println!("\n==================");
    println!("Final Results");
    println!("==================");
    println!("Total time: {:.2}s", total_elapsed.as_secs_f64());
    println!("Total completed: {}", total_completed);
    println!(
        "Average rate: {:.1} tasks/sec",
        total_completed as f64 / total_elapsed.as_secs_f64()
    );
    println!("Total polls: {}", total_polls);
    println!(
        "Empty polls: {} ({:.1}%)",
        total_empty_polls,
        if total_polls > 0 {
            total_empty_polls as f64 / total_polls as f64 * 100.0
        } else {
            0.0
        }
    );
    if args.enqueue {
        println!("Total enqueued: {}", total_enqueued);
        println!(
            "Average enqueue rate: {:.1} tasks/sec",
            total_enqueued as f64 / total_elapsed.as_secs_f64()
        );
    }

    Ok(())
}

