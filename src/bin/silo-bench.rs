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
    ConcurrencyLimit, EnqueueRequest, GetClusterInfoRequest, LeaseTasksRequest, Limit,
    MsgpackBytes, ReportOutcomeRequest,
};
use silo::settings::LogFormat;
use silo::trace;
use tonic::transport::Channel;
use tracing::{error, info, warn};

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

    /// Tenant ID prefix for multi-tenant mode. When tenant_count > 1, tenants are
    /// named "{tenant_prefix}-0", "{tenant_prefix}-1", etc.
    #[arg(long, default_value = "bench")]
    tenant_prefix: String,

    /// Number of tenants to distribute jobs across
    #[arg(long, short = 't', default_value = "4")]
    tenant_count: u32,

    /// Imbalance factor for tenant job distribution. When set to 1.0 (default),
    /// jobs are distributed evenly. When > 1.0, creates an imbalanced distribution
    /// where the first tenant receives `imbalance_factor` times more jobs than
    /// the last tenant (with intermediate tenants receiving proportionally
    /// distributed amounts).
    #[arg(long, default_value = "1.0")]
    imbalance_factor: f64,

    /// Enable structured JSON logging
    #[arg(long)]
    structured_logging: bool,
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
        rand::rng().random_range(0..self.num_shards)
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

/// Handles weighted tenant selection for distributing jobs across tenants.
///
/// With imbalance_factor = 1.0, all tenants receive equal weight (uniform distribution).
/// With imbalance_factor > 1.0, the first tenant gets `imbalance_factor` times more
/// jobs than the last tenant, with intermediate tenants distributed linearly.
#[derive(Debug, Clone)]
struct TenantSelector {
    /// List of tenant IDs
    tenants: Vec<String>,
    /// Cumulative weights for weighted random selection (normalized to 0.0-1.0)
    cumulative_weights: Vec<f64>,
}

impl TenantSelector {
    /// Create a new TenantSelector with the given prefix, count, and imbalance factor.
    ///
    /// - `prefix`: The tenant ID prefix (e.g., "bench" produces "bench-0", "bench-1", etc.)
    /// - `count`: Number of tenants
    /// - `imbalance_factor`: How much more the first tenant receives vs the last (1.0 = uniform)
    fn new(prefix: &str, count: u32, imbalance_factor: f64) -> Self {
        let count = count.max(1);
        let tenants: Vec<String> = (0..count).map(|i| format!("{}-{}", prefix, i)).collect();

        // Calculate weights for each tenant
        // With imbalance_factor = F:
        //   - Tenant 0 (first) gets weight F
        //   - Tenant n-1 (last) gets weight 1.0
        //   - Linear interpolation between
        let weights: Vec<f64> = if count == 1 {
            vec![1.0]
        } else {
            (0..count)
                .map(|i| {
                    // Linear interpolation from imbalance_factor (at i=0) to 1.0 (at i=count-1)
                    let t = i as f64 / (count - 1) as f64;
                    imbalance_factor * (1.0 - t) + 1.0 * t
                })
                .collect()
        };

        // Calculate cumulative weights (normalized)
        let total: f64 = weights.iter().sum();
        let mut cumulative = 0.0;
        let cumulative_weights: Vec<f64> = weights
            .iter()
            .map(|w| {
                cumulative += w / total;
                cumulative
            })
            .collect();

        TenantSelector {
            tenants,
            cumulative_weights,
        }
    }

    /// Select a random tenant according to the weighted distribution
    fn select(&self) -> &str {
        let r: f64 = rand::rng().random();
        for (i, &threshold) in self.cumulative_weights.iter().enumerate() {
            if r < threshold {
                return &self.tenants[i];
            }
        }
        // Fallback to last tenant (shouldn't happen due to cumulative weights reaching 1.0)
        self.tenants.last().map(|s| s.as_str()).unwrap_or("-")
    }

    /// Get all tenant IDs
    fn all_tenants(&self) -> &[String] {
        &self.tenants
    }

    /// Get the weight percentages for each tenant (for logging/display)
    fn weight_percentages(&self) -> Vec<(String, f64)> {
        let mut prev = 0.0;
        self.tenants
            .iter()
            .zip(self.cumulative_weights.iter())
            .map(|(tenant, &cum)| {
                let pct = (cum - prev) * 100.0;
                prev = cum;
                (tenant.clone(), pct)
            })
            .collect()
    }
}

/// Worker task that polls for tasks from random shards and reports success outcomes.
/// Workers lease tasks from all tenants and report outcomes without specifying a tenant
/// (the server determines the tenant from the task_id).
async fn worker_loop(
    cluster_info: Arc<ClusterInfo>,
    worker_id: String,
    max_tasks: u32,
    running: Arc<AtomicBool>,
    completed_count: Arc<AtomicU64>,
    poll_count: Arc<AtomicU64>,
    empty_poll_count: Arc<AtomicU64>,
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
                error!(worker_id = %worker_id, address = %addr, error = %e, "Worker failed to connect");
            }
        }
    }

    if connections.is_empty() {
        error!(worker_id = %worker_id, "Worker has no active connections, exiting");
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
            task_group: "default".to_string(),
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
                        tenant: None, // Server determines tenant from task_id
                        outcome: Some(Outcome::Success(MsgpackBytes {
                            data: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                        })),
                    };

                    if let Some(report_client) = connections.get_mut(&report_addr) {
                        match report_client.report_outcome(outcome_request).await {
                            Ok(_) => {
                                completed_count.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(e) => {
                                warn!(worker_id = %worker_id, error = %e, "Worker failed to report outcome");
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(worker_id = %worker_id, error = %e, "Worker poll failed");
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
    tenant_selector: Arc<TenantSelector>,
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
                error!(enqueuer_id = %enqueuer_id, address = %addr, error = %e, "Enqueuer failed to connect");
            }
        }
    }

    if connections.is_empty() {
        error!(enqueuer_id = %enqueuer_id, "Enqueuer has no active connections, exiting");
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

        // Select a tenant according to the weighted distribution
        let tenant = tenant_selector.select().to_string();

        let payload = serde_json::json!({
            "enqueuer": enqueuer_id,
            "job": job_counter,
            "tenant": tenant,
            "timestamp": now_ms(),
        });

        let request = EnqueueRequest {
            shard,
            id: String::new(), // Let server generate ID
            priority: (job_counter % 100) as u32,
            start_at_ms: now_ms(),
            retry_policy: None,
            payload: Some(MsgpackBytes {
                data: rmp_serde::to_vec(&payload).unwrap(),
            }),
            limits: vec![Limit {
                limit: Some(silo::pb::limit::Limit::Concurrency(ConcurrencyLimit {
                    key: concurrency_key.clone(),
                    max_concurrency,
                })),
            }],
            tenant: Some(tenant),
            metadata: Default::default(),
            task_group: "default".to_string(),
        };

        match client.enqueue(request).await {
            Ok(_) => {
                enqueued_count.fetch_add(1, Ordering::Relaxed);
                job_counter = job_counter.wrapping_add(1);
            }
            Err(e) => {
                warn!(enqueuer_id = %enqueuer_id, error = %e, "Enqueuer failed to enqueue");
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

    // Initialize tracing with structured logging if requested
    let log_format = if args.structured_logging {
        LogFormat::Json
    } else {
        LogFormat::Text
    };
    trace::init(log_format)?;

    info!("Silo Benchmark Tool");
    info!(
        initial_server = %args.address,
        workers = args.workers,
        enqueuers = args.enqueuers,
        duration_secs = args.duration_secs,
        tenant_prefix = %args.tenant_prefix,
        tenant_count = args.tenant_count,
        imbalance_factor = args.imbalance_factor,
        concurrency_key = %args.concurrency_key,
        max_concurrency = args.max_concurrency,
        "Benchmark configuration"
    );

    // Create tenant selector for weighted distribution
    let tenant_selector = Arc::new(TenantSelector::new(
        &args.tenant_prefix,
        args.tenant_count,
        args.imbalance_factor,
    ));

    // Log tenant distribution
    info!(
        tenants = ?tenant_selector.all_tenants(),
        "Configured tenants"
    );
    if args.imbalance_factor != 1.0 {
        for (tenant, pct) in tenant_selector.weight_percentages() {
            info!(tenant = %tenant, weight_pct = pct, "Tenant weight");
        }
    }

    // Discover cluster topology
    info!("Discovering cluster topology...");
    let cluster_info = discover_cluster(&args.address).await?;
    info!(
        node_id = %cluster_info.this_node_id,
        grpc_addr = %cluster_info.this_grpc_addr,
        "Connected to node"
    );
    info!(num_shards = cluster_info.num_shards, "Cluster shard count");
    for owner in &cluster_info.shard_owners {
        info!(
            shard_id = owner.shard_id,
            grpc_addr = %owner.grpc_addr,
            node_id = %owner.node_id,
            "Shard owner"
        );
    }
    info!(servers = ?cluster_info.all_addresses(), "Unique servers");

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

        handles.push(tokio::spawn(worker_loop(
            cluster,
            worker_id,
            args.max_tasks_per_poll,
            running,
            completed,
            polls,
            empty_polls,
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
        let selector = tenant_selector.clone();

        handles.push(tokio::spawn(enqueuer_loop(
            cluster,
            enqueuer_id,
            concurrency_key,
            max_concurrency,
            running,
            enqueued,
            selector,
        )));
    }

    // Reporting loop
    let start = Instant::now();
    let report_interval = Duration::from_secs(args.report_interval_secs);
    let mut last_completed = 0u64;
    let mut last_enqueued = 0u64;
    let mut last_report = Instant::now();

    info!("Starting benchmark");
    if !args.structured_logging {
        println!(
            "{:>8} {:>12} {:>12} {:>12} {:>12} {:>12}",
            "Elapsed", "Completed", "Rate/s", "Enqueued", "Enq Rate/s", "Empty Polls"
        );
        println!("{}", "-".repeat(72));
    }

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

        info!(
            elapsed_secs = elapsed.as_secs_f64(),
            completed = current_completed,
            completed_rate_per_sec = rate,
            enqueued = current_enqueued,
            enqueue_rate_per_sec = enqueue_rate,
            empty_polls = current_empty_polls,
            "Benchmark progress"
        );

        if !args.structured_logging {
            println!(
                "{:>7.1}s {:>12} {:>12.1} {:>12} {:>12.1} {:>12}",
                elapsed.as_secs_f64(),
                current_completed,
                rate,
                current_enqueued,
                enqueue_rate,
                current_empty_polls
            );
        }

        last_completed = current_completed;
        last_enqueued = current_enqueued;
        last_report = Instant::now();
    }

    // Stop workers
    info!("Stopping workers...");
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

    let avg_rate = total_completed as f64 / total_elapsed.as_secs_f64();
    let empty_poll_pct = if total_polls > 0 {
        total_empty_polls as f64 / total_polls as f64 * 100.0
    } else {
        0.0
    };
    let avg_enqueue_rate = total_enqueued as f64 / total_elapsed.as_secs_f64();

    info!(
        total_time_secs = total_elapsed.as_secs_f64(),
        total_completed = total_completed,
        average_rate_per_sec = avg_rate,
        total_polls = total_polls,
        empty_polls = total_empty_polls,
        empty_poll_percentage = empty_poll_pct,
        total_enqueued = total_enqueued,
        average_enqueue_rate_per_sec = avg_enqueue_rate,
        "Final benchmark results"
    );

    if !args.structured_logging {
        println!("\n===================");
        println!("Final Results");
        println!("===================");
        println!("Total time: {:.2}s", total_elapsed.as_secs_f64());
        println!("Total completed: {}", total_completed);
        println!("Average rate: {:.1} tasks/sec", avg_rate);
        println!("Total polls: {}", total_polls);
        println!(
            "Empty polls: {} ({:.1}%)",
            total_empty_polls, empty_poll_pct
        );
        println!("Total enqueued: {}", total_enqueued);
        println!("Average enqueue rate: {:.1} tasks/sec", avg_enqueue_rate);
    }

    trace::shutdown();
    Ok(())
}
