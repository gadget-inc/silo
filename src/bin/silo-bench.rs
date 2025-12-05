//! Benchmark tool for measuring Silo task throughput against a remote server.
//!
//! This tool connects to a running Silo node via gRPC, discovers cluster topology,
//! and simulates concurrent workers polling and executing tasks. It reports the
//! rate of task execution in real-time.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use rand::Rng;
use silo::pb::report_outcome_request::Outcome;
use silo::pb::silo_client::SiloClient;
use silo::pb::{
    ConcurrencyLimit, EnqueueRequest, GetClusterInfoRequest, JsonValueBytes, LeaseTasksRequest,
    Limit, ReportOutcomeRequest,
};
use tonic::transport::Channel;

#[derive(Parser, Debug)]
#[command(name = "silo-bench")]
#[command(about = "Benchmark tool for measuring Silo task throughput")]
struct Args {
    /// Address of the Silo server (e.g., http://localhost:50051)
    #[arg(long, short = 'a', default_value = "http://localhost:50051")]
    address: String,

    /// Number of concurrent workers polling for tasks
    #[arg(long, short = 'w', default_value = "8")]
    workers: u32,

    /// Number of concurrent enqueuers pushing jobs
    #[arg(long, short = 'e', default_value = "4")]
    enqueuers: u32,

    /// Duration to run the benchmark, in seconds
    #[arg(long, short = 'd', default_value = "30")]
    duration_secs: u64,

    /// Maximum tasks to request per poll
    #[arg(long, default_value = "4")]
    max_tasks_per_poll: u32,

    /// Concurrency limit key for enqueued tasks
    #[arg(long, default_value = "bench-queue")]
    concurrency_key: String,

    /// Max concurrency for enqueued tasks
    #[arg(long, default_value = "100")]
    max_concurrency: u32,

    /// Report interval in seconds
    #[arg(long, default_value = "1")]
    report_interval_secs: u64,

    /// Tenant ID for multi-tenant mode (required if server has tenancy enabled)
    #[arg(long, default_value = "-")]
    tenant: String,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Ensure an address has the http:// scheme prefix
fn ensure_http_scheme(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    }
}

async fn connect(address: &str) -> anyhow::Result<SiloClient<Channel>> {
    let url = ensure_http_scheme(address);
    let channel = Channel::from_shared(url)?.connect().await?;
    Ok(SiloClient::new(channel))
}

/// Discover cluster topology by calling GetClusterInfo
async fn discover_cluster(address: &str) -> anyhow::Result<ClusterInfo> {
    let mut client = connect(address).await?;
    let response = client
        .get_cluster_info(GetClusterInfoRequest {})
        .await?
        .into_inner();

    let num_shards = response.num_shards;
    let shard_owners: Vec<ShardOwnerInfo> = response
        .shard_owners
        .into_iter()
        .map(|owner| ShardOwnerInfo {
            shard_id: owner.shard_id,
            grpc_addr: owner.grpc_addr,
            node_id: owner.node_id,
        })
        .collect();

    Ok(ClusterInfo {
        num_shards,
        shard_owners,
        this_node_id: response.this_node_id,
        this_grpc_addr: response.this_grpc_addr,
    })
}

#[derive(Debug, Clone)]
struct ShardOwnerInfo {
    shard_id: u32,
    grpc_addr: String,
    #[allow(dead_code)]
    node_id: String,
}

#[derive(Debug, Clone)]
struct ClusterInfo {
    num_shards: u32,
    shard_owners: Vec<ShardOwnerInfo>,
    this_node_id: String,
    this_grpc_addr: String,
}

impl ClusterInfo {
    /// Get a random shard ID
    fn random_shard(&self) -> u32 {
        rand::thread_rng().gen_range(0..self.num_shards)
    }

    /// Get the address of the server owning a specific shard
    fn address_for_shard(&self, shard_id: u32) -> Option<&str> {
        self.shard_owners
            .iter()
            .find(|owner| owner.shard_id == shard_id)
            .map(|owner| owner.grpc_addr.as_str())
    }

    /// Get all unique server addresses in the cluster
    fn all_addresses(&self) -> Vec<String> {
        let mut addrs: Vec<String> = self
            .shard_owners
            .iter()
            .map(|owner| owner.grpc_addr.clone())
            .collect();
        addrs.sort();
        addrs.dedup();
        addrs
    }
}

/// Worker task that polls for tasks from random shards and reports success outcomes
async fn worker_loop(
    cluster_info: Arc<ClusterInfo>,
    worker_id: String,
    max_tasks: u32,
    running: Arc<AtomicBool>,
    completed_count: Arc<AtomicU64>,
    poll_count: Arc<AtomicU64>,
    empty_poll_count: Arc<AtomicU64>,
    tenant: Option<String>,
) {
    // Create connections to all servers in the cluster
    let mut connections: std::collections::HashMap<String, SiloClient<Channel>> =
        std::collections::HashMap::new();

    for addr in cluster_info.all_addresses() {
        match connect(&addr).await {
            Ok(client) => {
                connections.insert(addr, client);
            }
            Err(e) => {
                eprintln!("Worker {} failed to connect to {}: {}", worker_id, addr, e);
            }
        }
    }

    if connections.is_empty() {
        eprintln!("Worker {} has no active connections, exiting", worker_id);
        return;
    }

    while running.load(Ordering::Relaxed) {
        // Pick a random shard
        let shard = cluster_info.random_shard();

        // Find the server owning this shard
        let addr = match cluster_info.address_for_shard(shard) {
            Some(addr) => addr.to_string(),
            None => {
                // Fallback: use any available connection
                connections.keys().next().unwrap().clone()
            }
        };

        let client = match connections.get_mut(&addr) {
            Some(c) => c,
            None => {
                // Try to reconnect
                match connect(&addr).await {
                    Ok(c) => {
                        connections.insert(addr.clone(), c);
                        connections.get_mut(&addr).unwrap()
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                }
            }
        };

        let request = LeaseTasksRequest {
            shard: Some(shard),
            worker_id: worker_id.clone(),
            max_tasks,
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
                    let task_shard = task.shard;
                    let task_addr = cluster_info
                        .address_for_shard(task_shard)
                        .map(|a| a.to_string())
                        .unwrap_or_else(|| addr.clone());

                    // Determine which address to use for reporting - prefer task_addr if we have a connection
                    let report_addr = if connections.contains_key(&task_addr) {
                        task_addr
                    } else {
                        addr.clone()
                    };

                    let outcome_request = ReportOutcomeRequest {
                        shard: task_shard,
                        task_id: task.id.clone(),
                        tenant: tenant.clone(),
                        outcome: Some(Outcome::Success(JsonValueBytes {
                            data: b"{}".to_vec(),
                        })),
                    };

                    if let Some(report_client) = connections.get_mut(&report_addr) {
                        match report_client.report_outcome(outcome_request).await {
                            Ok(_) => {
                                completed_count.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(e) => {
                                eprintln!("Worker {} failed to report outcome: {}", worker_id, e);
                            }
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

/// Enqueuer task that continuously enqueues tasks to random shards
async fn enqueuer_loop(
    cluster_info: Arc<ClusterInfo>,
    enqueuer_id: String,
    concurrency_key: String,
    max_concurrency: u32,
    running: Arc<AtomicBool>,
    enqueued_count: Arc<AtomicU64>,
    tenant: Option<String>,
) {
    // Create connections to all servers in the cluster
    let mut connections: std::collections::HashMap<String, SiloClient<Channel>> =
        std::collections::HashMap::new();

    for addr in cluster_info.all_addresses() {
        match connect(&addr).await {
            Ok(client) => {
                connections.insert(addr, client);
            }
            Err(e) => {
                eprintln!(
                    "Enqueuer {} failed to connect to {}: {}",
                    enqueuer_id, addr, e
                );
            }
        }
    }

    if connections.is_empty() {
        eprintln!(
            "Enqueuer {} has no active connections, exiting",
            enqueuer_id
        );
        return;
    }

    let mut job_counter = 0u64;

    while running.load(Ordering::Relaxed) {
        // Pick a random shard
        let shard = cluster_info.random_shard();

        // Find the server owning this shard
        let addr = match cluster_info.address_for_shard(shard) {
            Some(addr) => addr.to_string(),
            None => {
                // Fallback: use any available connection
                connections.keys().next().unwrap().clone()
            }
        };

        let client = match connections.get_mut(&addr) {
            Some(c) => c,
            None => {
                // Try to reconnect
                match connect(&addr).await {
                    Ok(c) => {
                        connections.insert(addr.clone(), c);
                        connections.get_mut(&addr).unwrap()
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                }
            }
        };

        let payload = serde_json::json!({
            "enqueuer": enqueuer_id,
            "job": job_counter,
            "timestamp": now_ms(),
        });

        let request = EnqueueRequest {
            shard,
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
    println!("===================");
    println!("Initial server: {}", args.address);
    println!("Workers: {}", args.workers);
    println!("Enqueuers: {}", args.enqueuers);
    println!("Duration: {}s", args.duration_secs);
    println!("Tenant: {}", args.tenant);
    println!("Concurrency key: {}", args.concurrency_key);
    println!("Max concurrency: {}", args.max_concurrency);
    println!();

    // Discover cluster topology
    println!("Discovering cluster topology...");
    let cluster_info = discover_cluster(&args.address).await?;
    println!(
        "Connected to node: {} ({})",
        cluster_info.this_node_id, cluster_info.this_grpc_addr
    );
    println!("Cluster has {} shards", cluster_info.num_shards);
    println!("Shard owners:");
    for owner in &cluster_info.shard_owners {
        println!(
            "  Shard {} -> {} ({})",
            owner.shard_id, owner.grpc_addr, owner.node_id
        );
    }
    println!("Unique servers: {:?}", cluster_info.all_addresses());
    println!();

    let cluster_info = Arc::new(cluster_info);

    // Shared counters
    let running = Arc::new(AtomicBool::new(true));
    let completed_count = Arc::new(AtomicU64::new(0));
    let poll_count = Arc::new(AtomicU64::new(0));
    let empty_poll_count = Arc::new(AtomicU64::new(0));
    let enqueued_count = Arc::new(AtomicU64::new(0));

    // Spawn worker tasks
    let mut handles = Vec::new();
    for i in 0..args.workers {
        let cluster = cluster_info.clone();
        let worker_id = format!("bench-worker-{}", i);
        let running = running.clone();
        let completed = completed_count.clone();
        let polls = poll_count.clone();
        let empty_polls = empty_poll_count.clone();
        let tenant = Some(args.tenant.clone());

        handles.push(tokio::spawn(worker_loop(
            cluster,
            worker_id,
            args.max_tasks_per_poll,
            running,
            completed,
            polls,
            empty_polls,
            tenant,
        )));
    }

    // Spawn enqueuer tasks
    for i in 0..args.enqueuers {
        let cluster = cluster_info.clone();
        let enqueuer_id = format!("bench-enqueuer-{}", i);
        let running = running.clone();
        let enqueued = enqueued_count.clone();
        let concurrency_key = args.concurrency_key.clone();
        let max_concurrency = args.max_concurrency;
        let tenant = Some(args.tenant.clone());

        handles.push(tokio::spawn(enqueuer_loop(
            cluster,
            enqueuer_id,
            concurrency_key,
            max_concurrency,
            running,
            enqueued,
            tenant,
        )));
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

    println!("\n===================");
    println!("Final Results");
    println!("===================");
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
    println!("Total enqueued: {}", total_enqueued);
    println!(
        "Average enqueue rate: {:.1} tasks/sec",
        total_enqueued as f64 / total_elapsed.as_secs_f64()
    );

    Ok(())
}
