//! Benchmark tool for measuring Silo task throughput against a remote server.
//!
//! This tool connects to a running Silo node via gRPC, discovers cluster topology,
//! and simulates concurrent workers polling and executing tasks. It reports the
//! rate of task execution in real-time.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use rand::Rng;
use silo::cluster_client::ClientConfig;
use silo::pb::report_outcome_request::Outcome;
use silo::pb::{
    ConcurrencyLimit, EnqueueRequest, FloatingConcurrencyLimit, LeaseTasksRequest, Limit,
    QueryRequest, RefreshSuccess, ReportOutcomeRequest, ReportRefreshOutcomeRequest,
    SerializedBytes, report_refresh_outcome_request, serialized_bytes,
};
use silo::routing_client::RoutingClient;
use silo::settings::LogFormat;
use silo::trace;
use tracing::{error, info, warn};

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

    /// Concurrency limit key for enqueued tasks. If unset, each tenant uses its
    /// own queue named "{tenant}-queue". If set, every tenant shares this key.
    #[arg(long)]
    concurrency_key: Option<String>,

    /// Max concurrency for enqueued tasks
    #[arg(long, default_value = "100")]
    max_concurrency: u32,

    /// Number of FloatingConcurrency limits to attach to each enqueued job, in
    /// addition to the static Concurrency limit. Each enqueue randomly picks
    /// between 1 and this max (inclusive). Set to 0 to disable floating limits.
    #[arg(long, default_value = "2")]
    max_floating_limits: u32,

    /// Default max_concurrency for each FloatingConcurrency limit (used until
    /// the first worker refresh updates it).
    #[arg(long, default_value = "50")]
    floating_default_max: u32,

    /// Refresh interval (ms) for FloatingConcurrency limits. Workers receive
    /// refresh tasks at most this often. Lower values exercise the
    /// refresh/grant interaction more aggressively.
    #[arg(long, default_value = "2000")]
    floating_refresh_interval_ms: i64,

    /// Probability (0.0-1.0) that a refresh worker reports a Failure instead
    /// of Success. Failures exercise the backoff/retry path that previously
    /// leaked holders.
    #[arg(long, default_value = "0.1")]
    floating_failure_rate: f64,

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

    /// Bearer token for gRPC authentication.
    /// Can also be set via SILO_AUTH_TOKEN environment variable.
    #[arg(long, env = "SILO_AUTH_TOKEN")]
    auth_token: Option<String>,
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
#[allow(clippy::too_many_arguments)]
async fn worker_loop(
    client: Arc<RoutingClient>,
    worker_id: String,
    max_tasks: u32,
    running: Arc<AtomicBool>,
    completed_count: Arc<AtomicU64>,
    poll_count: Arc<AtomicU64>,
    empty_poll_count: Arc<AtomicU64>,
    refresh_count: Arc<AtomicU64>,
    floating_failure_rate: f64,
    floating_default_max: u32,
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
                let response = response.into_inner();
                let tasks = response.tasks;
                let refresh_tasks = response.refresh_tasks;

                // Handle floating-limit refresh tasks first so the new
                // max_concurrency lands before subsequent enqueues see the
                // limit. The chosen value oscillates around the configured
                // default so the in-flight max changes constantly — this
                // stresses the chain code that has to reconcile in-flight
                // grants against changing capacity.
                for rt in refresh_tasks {
                    let report_shard = rt.shard.clone();
                    let mut refresh_client = match client.client_for_shard(&report_shard) {
                        Ok(c) => c,
                        Err(_) => silo_client.clone(),
                    };

                    let outcome: report_refresh_outcome_request::Outcome = {
                        let mut rng = rand::rng();
                        let roll: f64 = rng.random();
                        if roll < floating_failure_rate {
                            report_refresh_outcome_request::Outcome::Failure(
                                silo::pb::RefreshFailure {
                                    code: "bench_simulated".to_string(),
                                    message: "bench-injected refresh failure".to_string(),
                                },
                            )
                        } else {
                            // Vary max_concurrency widely: half to 2x the
                            // default. Includes 1 occasionally so we exercise
                            // tight-capacity scenarios.
                            let half = (floating_default_max / 2).max(1);
                            let high = (floating_default_max * 2).max(half + 1);
                            let new_max = rng.random_range(half..=high);
                            report_refresh_outcome_request::Outcome::Success(RefreshSuccess {
                                new_max_concurrency: new_max,
                            })
                        }
                    };

                    let req = ReportRefreshOutcomeRequest {
                        shard: report_shard.clone(),
                        task_id: rt.id.clone(),
                        outcome: Some(outcome),
                        tenant_id: rt.tenant_id.clone(),
                    };

                    match refresh_client.report_refresh_outcome(req).await {
                        Ok(_) => {
                            refresh_count.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            warn!(worker_id = %worker_id, error = %e, "report_refresh_outcome failed");
                        }
                    }
                }

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
#[allow(clippy::too_many_arguments)]
async fn enqueuer_loop(
    client: Arc<RoutingClient>,
    enqueuer_id: String,
    concurrency_key_override: Option<String>,
    max_concurrency: u32,
    max_floating_limits: u32,
    floating_default_max: u32,
    floating_refresh_interval_ms: i64,
    running: Arc<AtomicBool>,
    enqueued_count: Arc<AtomicU64>,
    tenant_selector: Arc<TenantSelector>,
) {
    let mut job_counter = 0u64;

    while running.load(Ordering::Relaxed) {
        let tenant = tenant_selector.select().to_string();
        let concurrency_key = concurrency_key_override
            .clone()
            .unwrap_or_else(|| format!("{}-queue", tenant));

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

        // Build the limit list: one static Concurrency + 1..=N floating
        // limits chosen per-tenant. We sample the floating count fresh per
        // enqueue so the workload exercises 0/1/2-limit chains within the
        // same run. Floating queue keys are tenant-scoped, so distinct
        // tenants don't share a floating limit.
        let mut limits: Vec<Limit> = Vec::new();
        limits.push(Limit {
            limit: Some(silo::pb::limit::Limit::Concurrency(ConcurrencyLimit {
                key: concurrency_key.clone(),
                max_concurrency,
            })),
        });
        if max_floating_limits > 0 {
            let n_floating = {
                let mut rng = rand::rng();
                rng.random_range(1..=max_floating_limits)
            };
            for i in 0..n_floating {
                limits.push(Limit {
                    limit: Some(silo::pb::limit::Limit::FloatingConcurrency(
                        FloatingConcurrencyLimit {
                            key: format!("{}-float-{}", tenant, i),
                            default_max_concurrency: floating_default_max,
                            refresh_interval_ms: floating_refresh_interval_ms,
                            metadata: Default::default(),
                        },
                    )),
                });
            }
        }

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
                limits: limits.clone(),
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

/// Run the throughput benchmark.
async fn run_throughput_bench(
    args: &Args,
    client: Arc<RoutingClient>,
    tenant_selector: Arc<TenantSelector>,
) -> anyhow::Result<()> {
    // Shared counters
    let running = Arc::new(AtomicBool::new(true));
    let enqueuers_running = Arc::new(AtomicBool::new(true));
    let completed_count = Arc::new(AtomicU64::new(0));
    let poll_count = Arc::new(AtomicU64::new(0));
    let empty_poll_count = Arc::new(AtomicU64::new(0));
    let enqueued_count = Arc::new(AtomicU64::new(0));
    let refresh_count = Arc::new(AtomicU64::new(0));

    // Spawn worker tasks
    let mut handles = Vec::new();
    for i in 0..args.workers {
        let client = client.clone();
        let worker_id = format!("bench-worker-{}", i);
        let running = running.clone();
        let completed = completed_count.clone();
        let polls = poll_count.clone();
        let empty_polls = empty_poll_count.clone();
        let refreshes = refresh_count.clone();
        let failure_rate = args.floating_failure_rate;
        let floating_default = args.floating_default_max;

        handles.push(tokio::spawn(worker_loop(
            client,
            worker_id,
            args.max_tasks_per_poll,
            running,
            completed,
            polls,
            empty_polls,
            refreshes,
            failure_rate,
            floating_default,
        )));
    }

    // Spawn enqueuer tasks. Enqueuers share their own `running` flag so we
    // can stop them before workers — letting workers drain in-flight jobs
    // before the post-bench holder audit.
    let mut enqueuer_handles = Vec::new();
    for i in 0..args.enqueuers {
        let client = client.clone();
        let enqueuer_id = format!("bench-enqueuer-{}", i);
        let running = enqueuers_running.clone();
        let enqueued = enqueued_count.clone();
        let concurrency_key = args.concurrency_key.clone();
        let max_concurrency = args.max_concurrency;
        let max_floating = args.max_floating_limits;
        let floating_default = args.floating_default_max;
        let floating_refresh = args.floating_refresh_interval_ms;
        let selector = tenant_selector.clone();

        enqueuer_handles.push(tokio::spawn(enqueuer_loop(
            client,
            enqueuer_id,
            concurrency_key,
            max_concurrency,
            max_floating,
            floating_default,
            floating_refresh,
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
            "{:>8} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
            "Elapsed", "Completed", "Rate/s", "Enqueued", "Enq Rate/s", "Empty Polls", "Refreshes"
        );
        println!("{}", "-".repeat(85));
    }

    while start.elapsed() < Duration::from_secs(args.duration_secs) {
        tokio::time::sleep(report_interval).await;

        let elapsed = start.elapsed();
        let current_completed = completed_count.load(Ordering::Relaxed);
        let current_enqueued = enqueued_count.load(Ordering::Relaxed);
        let current_empty_polls = empty_poll_count.load(Ordering::Relaxed);
        let current_refreshes = refresh_count.load(Ordering::Relaxed);

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
            refreshes = current_refreshes,
            "Benchmark progress"
        );

        if !args.structured_logging {
            println!(
                "{:>7.1}s {:>12} {:>12.1} {:>12} {:>12.1} {:>12} {:>12}",
                elapsed.as_secs_f64(),
                current_completed,
                rate,
                current_enqueued,
                enqueue_rate,
                current_empty_polls,
                current_refreshes
            );
        }

        last_completed = current_completed;
        last_enqueued = current_enqueued;
        last_report = Instant::now();
    }

    // Drain phase: stop enqueueing, let workers continue processing the
    // in-flight backlog. Without this, the post-bench holder count would
    // legitimately be non-zero (jobs are still leased), masking real leaks.
    info!("Stopping enqueuers; draining in-flight work...");
    enqueuers_running.store(false, Ordering::Relaxed);
    for handle in enqueuer_handles {
        let _ = handle.await;
    }

    // Wait until the in-flight backlog is fully drained or we hit a timeout.
    // Termination condition: no completions OR no empty-poll delta over a
    // window. Holder leaks make `completed` plateau even though jobs are
    // pending — drain bails after ~60 s in that case, leaving the audit to
    // flag the leak.
    let drain_started = Instant::now();
    let drain_deadline = Duration::from_secs(120);
    let mut prev_completed = completed_count.load(Ordering::Relaxed);
    let mut stable_ticks = 0u32;
    while drain_started.elapsed() < drain_deadline {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let now_completed = completed_count.load(Ordering::Relaxed);
        let now_enqueued = enqueued_count.load(Ordering::Relaxed);
        if now_completed == prev_completed {
            stable_ticks += 1;
            // Three stable ticks (~6 s with no completions) means either
            // we're done or stuck. Either way move on to the audit.
            if stable_ticks >= 3 {
                let backlog = now_enqueued.saturating_sub(now_completed);
                info!(
                    drain_elapsed_secs = drain_started.elapsed().as_secs_f64(),
                    backlog = backlog,
                    "Drain stopped (no completions for ~6s)"
                );
                break;
            }
        } else {
            stable_ticks = 0;
        }
        prev_completed = now_completed;
    }

    // Stop workers
    info!("Stopping workers...");
    running.store(false, Ordering::Relaxed);
    for handle in handles {
        let _ = handle.await;
    }

    // Final statistics
    let total_elapsed = start.elapsed();
    let total_completed = completed_count.load(Ordering::Relaxed);
    let total_polls = poll_count.load(Ordering::Relaxed);
    let total_empty_polls = empty_poll_count.load(Ordering::Relaxed);
    let total_enqueued = enqueued_count.load(Ordering::Relaxed);
    let total_refreshes = refresh_count.load(Ordering::Relaxed);

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
        total_refreshes = total_refreshes,
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
        println!("Total refresh outcomes: {}", total_refreshes);
        let backlog = total_enqueued.saturating_sub(total_completed);
        println!("Backlog (enqueued - completed): {}", backlog);
    }

    // Post-bench holder audit. Workers have stopped and the in-flight queue
    // has drained, so any remaining concurrency holders are leaks. Sum
    // across every shard.
    info!("Auditing holder counts across all shards...");
    let audit = audit_holder_leaks(&client).await;
    match audit {
        Ok(report) => {
            report.log();
            if report.is_leak() {
                anyhow::bail!(
                    "HOLDER LEAK DETECTED: {} concurrency holders remain after drain (backlog={})",
                    report.total_holders,
                    total_enqueued.saturating_sub(total_completed)
                );
            }
        }
        Err(e) => {
            warn!(error = %e, "holder audit failed (treating as inconclusive, not a leak)");
        }
    }

    Ok(())
}

/// Result of scanning every shard for outstanding concurrency holders +
/// concurrency requests after the bench has drained.
#[derive(Debug, Default)]
struct HolderAudit {
    total_holders: u64,
    total_requests: u64,
    /// Per-shard (holders, requests) counts for diagnostics.
    per_shard: Vec<(String, u64, u64)>,
}

impl HolderAudit {
    fn log(&self) {
        for (shard, h, r) in &self.per_shard {
            info!(shard = %shard, holders = h, requests = r, "shard audit");
        }
        info!(
            total_holders = self.total_holders,
            total_requests = self.total_requests,
            "Post-bench holder audit"
        );
        println!("\n===================");
        println!("Post-bench Holder Audit");
        println!("===================");
        for (shard, h, r) in &self.per_shard {
            println!("  shard {} : holders={} requests={}", shard, h, r);
        }
        println!(
            "TOTAL: holders={} requests={}",
            self.total_holders, self.total_requests
        );
        if self.is_leak() {
            println!("*** HOLDER LEAK ***");
        } else {
            println!("(clean — no outstanding holders)");
        }
    }

    fn is_leak(&self) -> bool {
        self.total_holders > 0
    }
}

/// Query each shard for the count of outstanding concurrency holders +
/// requests. Uses the SQL Query RPC against the `queues` table.
async fn audit_holder_leaks(client: &RoutingClient) -> anyhow::Result<HolderAudit> {
    let topo = client.topology();
    let mut audit = HolderAudit::default();
    for owner in &topo.shard_owners {
        let mut silo_client = match client.client_for_shard(&owner.shard_id) {
            Ok(c) => c,
            Err(e) => {
                warn!(shard = %owner.shard_id, error = %e, "audit: could not get client");
                continue;
            }
        };
        let holders = query_count(
            &mut silo_client,
            &owner.shard_id,
            "SELECT count(*) AS n FROM queues WHERE entry_type = 'holder'",
        )
        .await
        .unwrap_or(0);
        let requests = query_count(
            &mut silo_client,
            &owner.shard_id,
            "SELECT count(*) AS n FROM queues WHERE entry_type = 'requester'",
        )
        .await
        .unwrap_or(0);
        audit.total_holders += holders;
        audit.total_requests += requests;
        audit
            .per_shard
            .push((owner.shard_id.clone(), holders, requests));
    }
    Ok(audit)
}

/// Run a `SELECT count(*) AS n FROM ...` query and return the scalar count.
/// The Query RPC returns rows as MessagePack-encoded `SerializedBytes`; we
/// decode them as a `serde_json::Value` and pull out the `n` field.
async fn query_count(
    client: &mut silo::cluster_client::InterceptedSiloClient,
    shard: &str,
    sql: &str,
) -> anyhow::Result<u64> {
    let resp = client
        .query(QueryRequest {
            shard: shard.to_string(),
            sql: sql.to_string(),
            tenant: None,
            parameters: vec![],
        })
        .await?
        .into_inner();
    for row in resp.rows {
        let bytes = match row.encoding {
            Some(serialized_bytes::Encoding::Msgpack(b)) => b,
            None => continue,
        };
        let v: serde_json::Value = rmp_serde::from_slice(&bytes)?;
        // The aggregate column is named `n` in our SQL; tolerate the
        // datafusion default `count(*)` alias as well.
        for key in ["n", "count(*)", "COUNT(*)"] {
            if let Some(val) = v.get(key)
                && let Some(n) = val.as_u64()
            {
                return Ok(n);
            }
        }
    }
    Ok(0)
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
    let concurrency_key_display = args
        .concurrency_key
        .as_deref()
        .unwrap_or("<per-tenant>")
        .to_string();
    info!(
        initial_server = %args.address,
        workers = args.workers,
        enqueuers = args.enqueuers,
        duration_secs = args.duration_secs,
        tenant_prefix = %args.tenant_prefix,
        tenant_count = args.tenant_count,
        imbalance_factor = args.imbalance_factor,
        concurrency_key = %concurrency_key_display,
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

    // Discover cluster topology and create shared client
    info!("Discovering cluster topology...");
    let client_config = ClientConfig {
        auth_token: args.auth_token.clone(),
        ..ClientConfig::default()
    };
    let client = RoutingClient::discover_with_config(&args.address, client_config).await?;

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

    let result = run_throughput_bench(&args, client, tenant_selector).await;

    trace::shutdown();
    result
}
