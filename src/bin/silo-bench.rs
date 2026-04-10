//! Benchmark tool for measuring Silo task throughput against a remote server.
//!
//! This tool connects to a running Silo node via gRPC, discovers cluster topology,
//! and simulates concurrent workers polling and executing tasks. It reports the
//! rate of task execution in real-time.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use rand::Rng;
use silo::pb::report_outcome_request::Outcome;
use silo::pb::{
    CompactShardRequest, ConcurrencyLimit, EnqueueRequest, GetShardStorageInfoRequest,
    GetShardStorageInfoResponse, LeaseTasksRequest, Limit, ReportOutcomeRequest, SerializedBytes,
    serialized_bytes,
};
use silo::routing_client::RoutingClient;
use silo::settings::LogFormat;
use silo::trace;
use tracing::{error, info, warn};

#[derive(clap::ValueEnum, Clone, Debug, Default, PartialEq)]
enum BenchMode {
    /// Throughput-only benchmark (default). Measures enqueue/lease rates without compaction.
    #[default]
    Throughput,
    /// Compaction benchmark. Interleaves compaction triggers and measures latency impact.
    Compaction,
}

/// The strategy the benchmark uses to trigger compaction calls, NOT the compaction algorithm
/// used by the underlying SlateDB instance.
#[derive(clap::ValueEnum, Clone, Debug, Default)]
enum CompactionStrategy {
    /// Trigger compaction every `compaction-interval-secs` seconds.
    #[default]
    Periodic,
    /// Warmup, then trigger a single compaction and observe.
    OneShot,
    /// Warmup, then repeatedly trigger compaction with gaps between cycles.
    Repeated,
}

#[derive(Parser, Debug)]
#[command(name = "silo-bench")]
#[command(about = "Benchmark tool for measuring Silo task throughput")]
struct Args {
    /// Address of the Silo server (e.g., http://localhost:7450)
    #[arg(long, short = 'a', default_value = "http://localhost:7450")]
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

    // --- Compaction mode args ---
    /// Benchmark mode
    #[arg(long, value_enum, default_value = "throughput")]
    mode: BenchMode,

    /// Compaction trigger strategy (compaction mode only)
    #[arg(long, value_enum, default_value = "periodic")]
    compaction_strategy: CompactionStrategy,

    /// Seconds between compaction triggers (periodic strategy)
    #[arg(long, default_value = "15")]
    compaction_interval_secs: u64,

    /// Warmup seconds before first compaction (one-shot/repeated strategies)
    #[arg(long, default_value = "10")]
    warmup_secs: u64,

    /// Milliseconds between polls when waiting for compaction to finish
    #[arg(long, default_value = "500")]
    compaction_poll_interval_ms: u64,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
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

// --- Latency tracking for compaction mode ---

/// Per-request latency statistics computed from a set of samples.
#[derive(Debug, Clone)]
struct LatencyStats {
    count: usize,
    p50: Duration,
    p99: Duration,
    max: Duration,
}

impl LatencyStats {
    fn zero() -> Self {
        Self {
            count: 0,
            p50: Duration::ZERO,
            p99: Duration::ZERO,
            max: Duration::ZERO,
        }
    }
}

impl std::fmt::Display for LatencyStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "n={} p50={:.1}ms p99={:.1}ms max={:.1}ms",
            self.count,
            self.p50.as_secs_f64() * 1000.0,
            self.p99.as_secs_f64() * 1000.0,
            self.max.as_secs_f64() * 1000.0,
        )
    }
}

/// Thread-safe latency collector. Workers push durations; the reporter drains and computes stats.
struct LatencyTracker {
    samples: std::sync::Mutex<Vec<Duration>>,
}

impl LatencyTracker {
    fn new() -> Self {
        Self {
            samples: std::sync::Mutex::new(Vec::with_capacity(4096)),
        }
    }

    /// Record a single request latency.
    fn record(&self, duration: Duration) {
        self.samples.lock().unwrap().push(duration);
    }

    /// Drain all accumulated samples and compute percentile stats.
    fn drain_and_compute(&self) -> Option<LatencyStats> {
        let mut samples = {
            let mut guard = self.samples.lock().unwrap();
            std::mem::take(&mut *guard)
        };
        if samples.is_empty() {
            return None;
        }
        samples.sort_unstable();
        let count = samples.len();
        let p50 = samples[count / 2];
        let p99_idx = (count as f64 * 0.99).ceil() as usize;
        let p99 = samples[p99_idx.min(count - 1)];
        let max = samples[count - 1];
        Some(LatencyStats {
            count,
            p50,
            p99,
            max,
        })
    }
}

/// Compaction state constants for the AtomicU8.
const COMPACTION_IDLE: u8 = 0;
const COMPACTION_IN_PROGRESS: u8 = 1;

/// Maximum number of retries for wrong-shard routing errors per request.
const MAX_ROUTING_RETRIES: u32 = 3;

/// Pre-serialized empty success payload, shared across all outcome reports.
fn make_success_outcome_request(
    shard: String,
    task_id: String,
    tenant_id: Option<String>,
) -> ReportOutcomeRequest {
    ReportOutcomeRequest {
        shard,
        task_id,
        outcome: Some(Outcome::Success(SerializedBytes {
            encoding: Some(serialized_bytes::Encoding::Msgpack(
                rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
            )),
        })),
        tenant_id,
    }
}

/// Worker task that polls for tasks from random shards and reports success outcomes.
/// Workers lease tasks from all tenants and report outcomes without specifying a tenant
/// (the server determines the tenant from the task_id).
async fn worker_loop(
    client: Arc<RoutingClient>,
    worker_id: String,
    max_tasks: u32,
    running: Arc<AtomicBool>,
    completed_count: Arc<AtomicU64>,
    poll_count: Arc<AtomicU64>,
    empty_poll_count: Arc<AtomicU64>,
) {
    while running.load(Ordering::Relaxed) {
        let (mut silo_client, shard) = match client.client_for_random_shard() {
            Ok(pair) => pair,
            Err(e) => {
                warn!(worker_id = %worker_id, error = %e, "Worker failed to get client");
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };

        let request = LeaseTasksRequest {
            shard: Some(shard.clone()),
            worker_id: worker_id.clone(),
            max_tasks,
            task_group: "default".to_string(),
        };

        poll_count.fetch_add(1, Ordering::Relaxed);

        let result = silo_client.lease_tasks(request).await;
        match result {
            Ok(response) => {
                let tasks = response.into_inner().tasks;
                if tasks.is_empty() {
                    empty_poll_count.fetch_add(1, Ordering::Relaxed);
                    // Small sleep when no tasks to avoid tight polling
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                }

                for task in tasks {
                    let task_shard = task.shard.clone();
                    let task_tenant = task.tenant_id.clone();

                    let mut report_client = match client.client_for_shard(&task_shard) {
                        Ok(c) => c,
                        Err(_) => silo_client.clone(),
                    };

                    let outcome_request = make_success_outcome_request(
                        task_shard.clone(),
                        task.id.clone(),
                        task_tenant.clone(),
                    );

                    match report_client.report_outcome(outcome_request).await {
                        Ok(_) => {
                            completed_count.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            // Try handling routing error for outcome reporting
                            if client.handle_routing_error(&e, &task_shard).await.is_some() {
                                // Retry once with updated routing
                                if let Ok(mut retry_client) = client.client_for_shard(&task_shard) {
                                    let retry_req = make_success_outcome_request(
                                        task_shard,
                                        task.id.clone(),
                                        task_tenant,
                                    );
                                    match retry_client.report_outcome(retry_req).await {
                                        Ok(_) => {
                                            completed_count.fetch_add(1, Ordering::Relaxed);
                                        }
                                        Err(e) => {
                                            warn!(worker_id = %worker_id, error = %e, "Worker failed to report outcome after retry");
                                        }
                                    }
                                }
                            } else {
                                warn!(worker_id = %worker_id, error = %e, "Worker failed to report outcome");
                            }
                        }
                    }
                }
            }
            Err(e) => {
                // Handle routing errors for lease_tasks
                if let Some(needs_backoff) = client.handle_routing_error(&e, &shard).await {
                    if needs_backoff {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    continue;
                }
                warn!(worker_id = %worker_id, error = %e, "Worker poll failed");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

/// Enqueuer task that continuously enqueues tasks to the correct shards.
async fn enqueuer_loop(
    client: Arc<RoutingClient>,
    enqueuer_id: String,
    concurrency_key: String,
    max_concurrency: u32,
    running: Arc<AtomicBool>,
    enqueued_count: Arc<AtomicU64>,
    tenant_selector: Arc<TenantSelector>,
) {
    let mut job_counter = 0u64;

    while running.load(Ordering::Relaxed) {
        let tenant = tenant_selector.select().to_string();

        // Serialize payload once outside the retry loop
        let payload = serde_json::json!({
            "enqueuer": enqueuer_id,
            "job": job_counter,
            "tenant": tenant,
            "timestamp": now_ms(),
        });
        let payload_bytes = rmp_serde::to_vec(&payload).unwrap();
        let priority = (job_counter % 100) as u32;
        let start_at_ms = now_ms();

        let mut retries = 0u32;
        loop {
            let (mut silo_client, shard) = match client.client_for_tenant(&tenant) {
                Ok(pair) => pair,
                Err(e) => {
                    warn!(enqueuer_id = %enqueuer_id, tenant = %tenant, error = %e, "No shard for tenant");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    break;
                }
            };

            let request = EnqueueRequest {
                shard: shard.clone(),
                id: String::new(),
                priority,
                start_at_ms,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(payload_bytes.clone())),
                }),
                limits: vec![Limit {
                    limit: Some(silo::pb::limit::Limit::Concurrency(ConcurrencyLimit {
                        key: concurrency_key.clone(),
                        max_concurrency,
                    })),
                }],
                tenant: Some(tenant.clone()),
                metadata: Default::default(),
                task_group: "default".to_string(),
            };

            match silo_client.enqueue(request).await {
                Ok(_) => {
                    enqueued_count.fetch_add(1, Ordering::Relaxed);
                    job_counter = job_counter.wrapping_add(1);
                    break;
                }
                Err(e) => {
                    if retries < MAX_ROUTING_RETRIES
                        && let Some(needs_backoff) = client.handle_routing_error(&e, &shard).await
                    {
                        retries += 1;
                        if needs_backoff {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                        continue; // retry with updated routing
                    }
                    warn!(enqueuer_id = %enqueuer_id, error = %e, "Enqueuer failed to enqueue");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    break;
                }
            }
        }

        // Yield occasionally to let other tasks run
        if job_counter % 50 == 0 {
            tokio::task::yield_now().await;
        }
    }
}

// --- Compaction mode: latency-tracking worker and enqueuer loops ---

/// Worker loop for compaction mode. Targets a specific shard and records per-request latency.
#[allow(clippy::too_many_arguments)]
async fn compaction_worker_loop(
    client: Arc<RoutingClient>,
    worker_id: String,
    target_shard: String,
    max_tasks: u32,
    running: Arc<AtomicBool>,
    completed_count: Arc<AtomicU64>,
    poll_count: Arc<AtomicU64>,
    empty_poll_count: Arc<AtomicU64>,
    lease_latency: Arc<LatencyTracker>,
    lease_latency_idle: Arc<LatencyTracker>,
    lease_latency_compacting: Arc<LatencyTracker>,
    compaction_state: Arc<AtomicU8>,
) {
    while running.load(Ordering::Relaxed) {
        let mut silo_client = match client.client_for_shard(&target_shard) {
            Ok(c) => c,
            Err(e) => {
                warn!(worker_id = %worker_id, error = %e, "Worker failed to get client");
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };

        let request = LeaseTasksRequest {
            shard: Some(target_shard.clone()),
            worker_id: worker_id.clone(),
            max_tasks,
            task_group: "default".to_string(),
        };

        poll_count.fetch_add(1, Ordering::Relaxed);

        let start = Instant::now();
        let result = silo_client.lease_tasks(request).await;
        let elapsed = start.elapsed();

        lease_latency.record(elapsed);
        if compaction_state.load(Ordering::Relaxed) == COMPACTION_IN_PROGRESS {
            lease_latency_compacting.record(elapsed);
        } else {
            lease_latency_idle.record(elapsed);
        }

        match result {
            Ok(response) => {
                let tasks = response.into_inner().tasks;
                if tasks.is_empty() {
                    empty_poll_count.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                }

                for task in tasks {
                    let task_shard = task.shard.clone();
                    let task_tenant = task.tenant_id.clone();

                    let mut report_client = match client.client_for_shard(&task_shard) {
                        Ok(c) => c,
                        Err(_) => silo_client.clone(),
                    };

                    let outcome_request = make_success_outcome_request(
                        task_shard.clone(),
                        task.id.clone(),
                        task_tenant.clone(),
                    );

                    match report_client.report_outcome(outcome_request).await {
                        Ok(_) => {
                            completed_count.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            if client.handle_routing_error(&e, &task_shard).await.is_some() {
                                if let Ok(mut retry_client) = client.client_for_shard(&task_shard) {
                                    let retry_req = make_success_outcome_request(
                                        task_shard,
                                        task.id.clone(),
                                        task_tenant,
                                    );
                                    match retry_client.report_outcome(retry_req).await {
                                        Ok(_) => {
                                            completed_count.fetch_add(1, Ordering::Relaxed);
                                        }
                                        Err(e) => {
                                            warn!(worker_id = %worker_id, error = %e, "Worker failed to report outcome after retry");
                                        }
                                    }
                                }
                            } else {
                                warn!(worker_id = %worker_id, error = %e, "Worker failed to report outcome");
                            }
                        }
                    }
                }
            }
            Err(e) => {
                if let Some(needs_backoff) = client.handle_routing_error(&e, &target_shard).await {
                    if needs_backoff {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    continue;
                }
                warn!(worker_id = %worker_id, error = %e, "Worker poll failed");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

/// Enqueuer loop for compaction mode. Records per-request latency.
#[allow(clippy::too_many_arguments)]
async fn compaction_enqueuer_loop(
    client: Arc<RoutingClient>,
    enqueuer_id: String,
    concurrency_key: String,
    max_concurrency: u32,
    running: Arc<AtomicBool>,
    enqueued_count: Arc<AtomicU64>,
    tenant_selector: Arc<TenantSelector>,
    enqueue_latency: Arc<LatencyTracker>,
    enqueue_latency_idle: Arc<LatencyTracker>,
    enqueue_latency_compacting: Arc<LatencyTracker>,
    compaction_state: Arc<AtomicU8>,
) {
    let mut job_counter = 0u64;

    while running.load(Ordering::Relaxed) {
        let tenant = tenant_selector.select().to_string();

        let payload = serde_json::json!({
            "enqueuer": enqueuer_id,
            "job": job_counter,
            "tenant": tenant,
            "timestamp": now_ms(),
        });
        let payload_bytes = rmp_serde::to_vec(&payload).unwrap();
        let priority = (job_counter % 100) as u32;
        let start_at_ms = now_ms();

        let mut retries = 0u32;
        loop {
            let (mut silo_client, shard) = match client.client_for_tenant(&tenant) {
                Ok(pair) => pair,
                Err(e) => {
                    warn!(enqueuer_id = %enqueuer_id, tenant = %tenant, error = %e, "No shard for tenant");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    break;
                }
            };

            let request = EnqueueRequest {
                shard: shard.clone(),
                id: String::new(),
                priority,
                start_at_ms,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(payload_bytes.clone())),
                }),
                limits: vec![Limit {
                    limit: Some(silo::pb::limit::Limit::Concurrency(ConcurrencyLimit {
                        key: concurrency_key.clone(),
                        max_concurrency,
                    })),
                }],
                tenant: Some(tenant.clone()),
                metadata: Default::default(),
                task_group: "default".to_string(),
            };

            let start = Instant::now();
            match silo_client.enqueue(request).await {
                Ok(_) => {
                    let elapsed = start.elapsed();
                    enqueue_latency.record(elapsed);
                    if compaction_state.load(Ordering::Relaxed) == COMPACTION_IN_PROGRESS {
                        enqueue_latency_compacting.record(elapsed);
                    } else {
                        enqueue_latency_idle.record(elapsed);
                    }
                    enqueued_count.fetch_add(1, Ordering::Relaxed);
                    job_counter = job_counter.wrapping_add(1);
                    break;
                }
                Err(e) => {
                    if retries < MAX_ROUTING_RETRIES
                        && let Some(needs_backoff) = client.handle_routing_error(&e, &shard).await
                    {
                        retries += 1;
                        if needs_backoff {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                        continue;
                    }
                    warn!(enqueuer_id = %enqueuer_id, error = %e, "Enqueuer failed to enqueue");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    break;
                }
            }
        }

        if job_counter % 50 == 0 {
            tokio::task::yield_now().await;
        }
    }
}

/// Fetch storage info for a shard via gRPC.
async fn get_storage_info(
    client: &RoutingClient,
    shard_id: &str,
) -> Option<GetShardStorageInfoResponse> {
    let mut silo_client = client.client_for_shard(shard_id).ok()?;
    silo_client
        .get_shard_storage_info(GetShardStorageInfoRequest {
            shard: shard_id.to_string(),
        })
        .await
        .ok()
        .map(|r| r.into_inner())
}

/// Poll until compaction appears complete (sorted_run_count <= 1) or timeout.
async fn poll_until_compaction_done(
    client: &RoutingClient,
    shard_id: &str,
    poll_interval: Duration,
    running: &AtomicBool,
) {
    let timeout = Instant::now() + Duration::from_secs(120);
    loop {
        if !running.load(Ordering::Relaxed) || Instant::now() > timeout {
            break;
        }
        tokio::time::sleep(poll_interval).await;
        if let Some(info) = get_storage_info(client, shard_id).await {
            info!(
                shard_id = %shard_id,
                l0_sst_count = info.l0_sst_count,
                sorted_run_count = info.sorted_run_count,
                "Polling compaction progress"
            );
            if info.sorted_run_count <= 1 {
                info!(shard_id = %shard_id, "Compaction complete");
                break;
            }
        }
    }
}

/// Compaction loop that triggers compaction according to the chosen strategy.
#[allow(clippy::too_many_arguments)]
async fn compaction_loop(
    client: Arc<RoutingClient>,
    target_shard: String,
    strategy: CompactionStrategy,
    interval_secs: u64,
    warmup_secs: u64,
    poll_interval: Duration,
    running: Arc<AtomicBool>,
    compaction_state: Arc<AtomicU8>,
) {
    let trigger_compaction = |client: &Arc<RoutingClient>, shard: &str| {
        let client = client.clone();
        let shard = shard.to_string();
        async move {
            let mut silo_client = match client.client_for_shard(&shard) {
                Ok(c) => c,
                Err(e) => {
                    error!(error = %e, "Failed to get client for compaction");
                    return;
                }
            };
            match silo_client
                .compact_shard(CompactShardRequest {
                    shard: shard.clone(),
                })
                .await
            {
                Ok(resp) => {
                    info!(
                        shard_id = %shard,
                        status = %resp.into_inner().status,
                        "Compaction triggered"
                    );
                }
                Err(e) => {
                    error!(shard_id = %shard, error = %e, "Failed to trigger compaction");
                }
            }
        }
    };

    match strategy {
        CompactionStrategy::Periodic => {
            let interval = Duration::from_secs(interval_secs);
            while running.load(Ordering::Relaxed) {
                tokio::time::sleep(interval).await;
                if !running.load(Ordering::Relaxed) {
                    break;
                }
                compaction_state.store(COMPACTION_IN_PROGRESS, Ordering::Relaxed);
                info!("Triggering periodic compaction");
                trigger_compaction(&client, &target_shard).await;
                poll_until_compaction_done(&client, &target_shard, poll_interval, &running).await;
                compaction_state.store(COMPACTION_IDLE, Ordering::Relaxed);
                info!("Periodic compaction cycle complete");
            }
        }
        CompactionStrategy::OneShot => {
            info!(
                warmup_secs = warmup_secs,
                "Warming up before one-shot compaction"
            );
            tokio::time::sleep(Duration::from_secs(warmup_secs)).await;
            if !running.load(Ordering::Relaxed) {
                return;
            }
            compaction_state.store(COMPACTION_IN_PROGRESS, Ordering::Relaxed);
            info!("Triggering one-shot compaction");
            trigger_compaction(&client, &target_shard).await;
            poll_until_compaction_done(&client, &target_shard, poll_interval, &running).await;
            compaction_state.store(COMPACTION_IDLE, Ordering::Relaxed);
            info!("One-shot compaction complete, continuing benchmark");
        }
        CompactionStrategy::Repeated => {
            info!(
                warmup_secs = warmup_secs,
                "Warming up before repeated compaction"
            );
            tokio::time::sleep(Duration::from_secs(warmup_secs)).await;
            while running.load(Ordering::Relaxed) {
                compaction_state.store(COMPACTION_IN_PROGRESS, Ordering::Relaxed);
                info!("Triggering repeated compaction cycle");
                trigger_compaction(&client, &target_shard).await;
                poll_until_compaction_done(&client, &target_shard, poll_interval, &running).await;
                compaction_state.store(COMPACTION_IDLE, Ordering::Relaxed);
                info!("Compaction cycle complete, resting 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

// --- Mode entry points ---

/// Format a Duration as a human-readable millisecond string.
fn fmt_ms(d: Duration) -> String {
    format!("{:.1}ms", d.as_secs_f64() * 1000.0)
}

/// Run the compaction benchmark mode.
async fn run_compaction_bench(
    args: &Args,
    client: Arc<RoutingClient>,
    tenant_selector: Arc<TenantSelector>,
) -> anyhow::Result<()> {
    // Resolve the target shard from the first tenant
    let first_tenant = &tenant_selector.all_tenants()[0];
    let topo = client.topology();
    let target_owner = topo
        .shard_for_tenant(first_tenant)
        .ok_or_else(|| anyhow::anyhow!("no shard found for tenant '{}'", first_tenant))?;
    let target_shard = target_owner.shard_id.clone();
    info!(
        tenant = %first_tenant,
        shard_id = %target_shard,
        grpc_addr = %target_owner.grpc_addr,
        "Compaction benchmark targeting shard"
    );

    // Log initial storage state
    if let Some(info) = get_storage_info(&client, &target_shard).await {
        info!(
            l0_sst_count = info.l0_sst_count,
            sorted_run_count = info.sorted_run_count,
            total_l0_size = info.total_l0_size,
            total_sorted_run_size = info.total_sorted_run_size,
            "Initial storage state"
        );
    }

    // Shared state
    let running = Arc::new(AtomicBool::new(true));
    let compaction_state = Arc::new(AtomicU8::new(COMPACTION_IDLE));
    let completed_count = Arc::new(AtomicU64::new(0));
    let poll_count = Arc::new(AtomicU64::new(0));
    let empty_poll_count = Arc::new(AtomicU64::new(0));
    let enqueued_count = Arc::new(AtomicU64::new(0));

    // Latency trackers: idle/compacting split (for final comparison summary)
    let enqueue_latency_idle = Arc::new(LatencyTracker::new());
    let enqueue_latency_compacting = Arc::new(LatencyTracker::new());
    let lease_latency_idle = Arc::new(LatencyTracker::new());
    let lease_latency_compacting = Arc::new(LatencyTracker::new());

    // Per-interval trackers (drained each reporting cycle)
    let interval_enqueue_latency = Arc::new(LatencyTracker::new());
    let interval_lease_latency = Arc::new(LatencyTracker::new());

    let mut handles = Vec::new();

    // Spawn workers targeting the compaction shard
    for i in 0..args.workers {
        let client = client.clone();
        let worker_id = format!("bench-worker-{}", i);
        handles.push(tokio::spawn(compaction_worker_loop(
            client,
            worker_id,
            target_shard.clone(),
            args.max_tasks_per_poll,
            running.clone(),
            completed_count.clone(),
            poll_count.clone(),
            empty_poll_count.clone(),
            interval_lease_latency.clone(),
            lease_latency_idle.clone(),
            lease_latency_compacting.clone(),
            compaction_state.clone(),
        )));
    }

    // Spawn enqueuers
    for i in 0..args.enqueuers {
        let client = client.clone();
        let enqueuer_id = format!("bench-enqueuer-{}", i);
        handles.push(tokio::spawn(compaction_enqueuer_loop(
            client,
            enqueuer_id,
            args.concurrency_key.clone(),
            args.max_concurrency,
            running.clone(),
            enqueued_count.clone(),
            tenant_selector.clone(),
            interval_enqueue_latency.clone(),
            enqueue_latency_idle.clone(),
            enqueue_latency_compacting.clone(),
            compaction_state.clone(),
        )));
    }

    // Spawn compaction loop
    let poll_interval = Duration::from_millis(args.compaction_poll_interval_ms);
    handles.push(tokio::spawn(compaction_loop(
        client.clone(),
        target_shard.clone(),
        args.compaction_strategy.clone(),
        args.compaction_interval_secs,
        args.warmup_secs,
        poll_interval,
        running.clone(),
        compaction_state.clone(),
    )));

    // Reporting loop
    let start = Instant::now();
    let report_interval = Duration::from_secs(args.report_interval_secs);
    let mut last_completed = 0u64;
    let mut last_enqueued = 0u64;
    let mut last_report = Instant::now();

    info!("Starting compaction benchmark");
    if !args.structured_logging {
        println!(
            "{:>8} {:>8} {:>10} {:>10} {:>8} {:>10} {:>10} {:>9}",
            "Elapsed", "Enq/s", "Enq p50", "Enq p99", "Cmp/s", "Lease p50", "Lease p99", "State"
        );
        println!("{}", "-".repeat(82));
    }

    while start.elapsed() < Duration::from_secs(args.duration_secs) {
        tokio::time::sleep(report_interval).await;

        let elapsed = start.elapsed();
        let current_completed = completed_count.load(Ordering::Relaxed);
        let current_enqueued = enqueued_count.load(Ordering::Relaxed);
        let state = compaction_state.load(Ordering::Relaxed);
        let state_str = if state == COMPACTION_IN_PROGRESS {
            "COMPACT"
        } else {
            "idle"
        };

        let interval_secs = last_report.elapsed().as_secs_f64();
        let completed_delta = current_completed - last_completed;
        let enqueued_delta = current_enqueued - last_enqueued;
        let complete_rate = completed_delta as f64 / interval_secs;
        let enqueue_rate = enqueued_delta as f64 / interval_secs;

        let enq_stats = interval_enqueue_latency
            .drain_and_compute()
            .unwrap_or(LatencyStats::zero());
        let lease_stats = interval_lease_latency
            .drain_and_compute()
            .unwrap_or(LatencyStats::zero());

        // Also record into the overall trackers
        // (the per-interval trackers are what workers write to; we accumulate overall
        // stats via the idle/compacting split trackers which workers also write to)

        // Fetch storage info for structured logging
        let storage_info = get_storage_info(&client, &target_shard).await;

        info!(
            elapsed_secs = elapsed.as_secs_f64(),
            enqueue_rate_per_sec = enqueue_rate,
            enqueue_p50_us = enq_stats.p50.as_micros(),
            enqueue_p99_us = enq_stats.p99.as_micros(),
            complete_rate_per_sec = complete_rate,
            lease_p50_us = lease_stats.p50.as_micros(),
            lease_p99_us = lease_stats.p99.as_micros(),
            compaction_state = state_str,
            l0_sst_count = storage_info.as_ref().map(|i| i.l0_sst_count).unwrap_or(0),
            sorted_run_count = storage_info
                .as_ref()
                .map(|i| i.sorted_run_count)
                .unwrap_or(0),
            "Compaction benchmark progress"
        );

        if !args.structured_logging {
            println!(
                "{:>7.1}s {:>8.1} {:>10} {:>10} {:>8.1} {:>10} {:>10} {:>9}",
                elapsed.as_secs_f64(),
                enqueue_rate,
                fmt_ms(enq_stats.p50),
                fmt_ms(enq_stats.p99),
                complete_rate,
                fmt_ms(lease_stats.p50),
                fmt_ms(lease_stats.p99),
                state_str,
            );
        }

        last_completed = current_completed;
        last_enqueued = current_enqueued;
        last_report = Instant::now();
    }

    // Shutdown
    info!("Stopping workers...");
    running.store(false, Ordering::Relaxed);
    for handle in handles {
        let _ = handle.await;
    }

    // Final storage state
    let final_storage = get_storage_info(&client, &target_shard).await;
    if let Some(info) = &final_storage {
        info!(
            l0_sst_count = info.l0_sst_count,
            sorted_run_count = info.sorted_run_count,
            total_l0_size = info.total_l0_size,
            total_sorted_run_size = info.total_sorted_run_size,
            "Final storage state"
        );
    }

    // Final latency summary
    let total_elapsed = start.elapsed();
    let total_completed = completed_count.load(Ordering::Relaxed);
    let total_enqueued = enqueued_count.load(Ordering::Relaxed);

    let idle_enq = enqueue_latency_idle
        .drain_and_compute()
        .unwrap_or(LatencyStats::zero());
    let compact_enq = enqueue_latency_compacting
        .drain_and_compute()
        .unwrap_or(LatencyStats::zero());
    let idle_lease = lease_latency_idle
        .drain_and_compute()
        .unwrap_or(LatencyStats::zero());
    let compact_lease = lease_latency_compacting
        .drain_and_compute()
        .unwrap_or(LatencyStats::zero());

    info!(
        total_time_secs = total_elapsed.as_secs_f64(),
        total_enqueued = total_enqueued,
        total_completed = total_completed,
        enqueue_idle = %idle_enq,
        enqueue_compacting = %compact_enq,
        lease_idle = %idle_lease,
        lease_compacting = %compact_lease,
        "Compaction benchmark final results"
    );

    if !args.structured_logging {
        println!("\n============================");
        println!("Compaction Benchmark Results");
        println!("============================");
        println!("Total time:      {:.2}s", total_elapsed.as_secs_f64());
        println!("Total enqueued:  {}", total_enqueued);
        println!("Total completed: {}", total_completed);
        println!();
        println!("Enqueue latency (write):");
        println!("  Idle:       {}", idle_enq);
        println!("  Compacting: {}", compact_enq);
        if compact_enq.count > 0 && idle_enq.count > 0 && idle_enq.p99 > Duration::ZERO {
            let p99_increase =
                compact_enq.p99.as_secs_f64() / idle_enq.p99.as_secs_f64() * 100.0 - 100.0;
            println!("  p99 delta:  {:+.1}%", p99_increase);
        }
        println!();
        println!("Lease latency (read):");
        println!("  Idle:       {}", idle_lease);
        println!("  Compacting: {}", compact_lease);
        if compact_lease.count > 0 && idle_lease.count > 0 && idle_lease.p99 > Duration::ZERO {
            let p99_increase =
                compact_lease.p99.as_secs_f64() / idle_lease.p99.as_secs_f64() * 100.0 - 100.0;
            println!("  p99 delta:  {:+.1}%", p99_increase);
        }
    }

    Ok(())
}

/// Run the original throughput-only benchmark mode.
async fn run_throughput_bench(
    args: &Args,
    client: Arc<RoutingClient>,
    tenant_selector: Arc<TenantSelector>,
) -> anyhow::Result<()> {
    // Shared counters
    let running = Arc::new(AtomicBool::new(true));
    let completed_count = Arc::new(AtomicU64::new(0));
    let poll_count = Arc::new(AtomicU64::new(0));
    let empty_poll_count = Arc::new(AtomicU64::new(0));
    let enqueued_count = Arc::new(AtomicU64::new(0));

    // Spawn worker tasks
    let mut handles = Vec::new();
    for i in 0..args.workers {
        let client = client.clone();
        let worker_id = format!("bench-worker-{}", i);
        let running = running.clone();
        let completed = completed_count.clone();
        let polls = poll_count.clone();
        let empty_polls = empty_poll_count.clone();

        handles.push(tokio::spawn(worker_loop(
            client,
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
        let client = client.clone();
        let enqueuer_id = format!("bench-enqueuer-{}", i);
        let running = running.clone();
        let enqueued = enqueued_count.clone();
        let concurrency_key = args.concurrency_key.clone();
        let max_concurrency = args.max_concurrency;
        let selector = tenant_selector.clone();

        handles.push(tokio::spawn(enqueuer_loop(
            client,
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

    Ok(())
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
        mode = ?args.mode,
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

    if args.mode == BenchMode::Compaction {
        info!(
            compaction_strategy = ?args.compaction_strategy,
            compaction_interval_secs = args.compaction_interval_secs,
            warmup_secs = args.warmup_secs,
            compaction_poll_interval_ms = args.compaction_poll_interval_ms,
            "Compaction configuration"
        );
    }

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

    // Discover cluster topology and create shared client
    info!("Discovering cluster topology...");
    let client = RoutingClient::discover(&args.address).await?;

    let topo = client.topology();
    info!(
        node_id = %topo.this_node_id,
        grpc_addr = %topo.this_grpc_addr,
        "Connected to node"
    );
    info!(num_shards = topo.num_shards, "Cluster shard count");
    for owner in &topo.shard_owners {
        let range_display = format!(
            "[{}, {})",
            if owner.range_start.is_empty() {
                "-∞"
            } else {
                &owner.range_start
            },
            if owner.range_end.is_empty() {
                "+∞"
            } else {
                &owner.range_end
            }
        );
        info!(
            shard_id = owner.shard_id,
            grpc_addr = %owner.grpc_addr,
            node_id = %owner.node_id,
            range = %range_display,
            "Shard owner"
        );
    }
    info!(servers = ?client.all_addresses(), "Unique servers");

    // Log tenant-to-shard mapping
    for tenant in tenant_selector.all_tenants() {
        match topo.shard_for_tenant(tenant) {
            Some(owner) => {
                info!(
                    tenant = %tenant,
                    shard_id = %owner.shard_id,
                    grpc_addr = %owner.grpc_addr,
                    "Tenant routing"
                );
            }
            None => {
                error!(tenant = %tenant, "No shard found for tenant - enqueue will fail!");
            }
        }
    }

    let result = match args.mode {
        BenchMode::Throughput => run_throughput_bench(&args, client, tenant_selector).await,
        BenchMode::Compaction => run_compaction_bench(&args, client, tenant_selector).await,
    };

    trace::shutdown();
    result
}
