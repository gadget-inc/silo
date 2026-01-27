//! Chaos scenario: Comprehensive fault injection with invariant verification.
//!
//! This scenario combines multiple fault modes with randomized parameters to
//! maximize state space exploration. It uses the InvariantTracker to continuously
//! verify system correctness throughout execution.
//!
//! Fault modes:
//! - Random message loss (5-15%)
//! - Variable message latency (5-100ms)
//! - Network partitions between workers and server
//! - Random worker crashes (via partition + timeout)
//! - Mix of concurrency-limited and unlimited jobs
//! - Random job cancellations
//! - Random job expedites (for scheduled/retrying jobs)
//! - Random job restarts (for failed/cancelled jobs)
//! - Variable number of workers (2-6)
//!
//! Invariants verified (from Alloy spec):
//! - queueLimitEnforced: At most max_concurrency tasks per limit key
//! - oneLeasePerJob: A job never has two active leases
//! - noLeasesForTerminal: Terminal jobs have no active leases
//! - validTransitions: Only valid status transitions occur
//! - No duplicate completions

use crate::helpers::{
    ConcurrencyLimit, EnqueueRequest, HashMap, InvariantTracker, LeaseTasksRequest, Limit,
    SerializedBytes, ReportOutcomeRequest, RetryPolicy, Task, create_turmoil_client, get_seed, limit, serialized_bytes,
    report_outcome_request, run_scenario_impl, setup_server, turmoil, verify_server_invariants,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use silo::cluster_client::ClientConfig;
use silo::pb::{CancelJobRequest, ExpediteJobRequest, RestartJobRequest};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

/// Concurrency limit configurations for chaos testing
const CHAOS_LIMITS: &[(&str, u32)] = &[
    ("chaos-mutex", 1),     // Strict serialization
    ("chaos-pair", 2),      // Small concurrency
    ("chaos-batch", 4),     // Medium concurrency
    ("chaos-unlimited", 0), // No limit (0 means unlimited)
];

/// Chaos configuration derived from seed
struct ChaosConfig {
    fail_rate: f64,
    max_latency_ms: u64,
    num_workers: u32,
    num_jobs: u32,
    // Number of cancellation operations to attempt
    num_cancels: u32,
    // Number of expedite operations to attempt
    num_expedites: u32,
    // Number of restart operations to attempt
    num_restarts: u32,
    // Number of partitions to inject
    num_partitions: u32,
    // Range of partition duration (min, max) ms
    partition_duration_range: (u64, u64),
    // Range of worker batch sizes (min, max)
    worker_batch_range: (u32, u32),
}

impl ChaosConfig {
    fn from_seed(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);

        // Message fail rate varies from 5-15%. The client has hard timeouts on
        // connection attempts, so even high fail rates won't cause indefinite hangs.
        let fail_rate = 0.05 + 0.10 * (rng.random_range(0..100) as f64 / 100.0);
        let max_latency_ms = rng.random_range(5..100);
        let num_workers = rng.random_range(2..=5);
        // Job count kept moderate to complete within 180s simulation budget.
        // The goal is testing fault handling, not throughput.
        let num_jobs = rng.random_range(10..=20);

        // Operation counts - keep moderate to leave time for core work
        let num_cancels = rng.random_range(1..=4);
        let num_expedites = rng.random_range(1..=3);
        let num_restarts = rng.random_range(1..=2);
        let num_partitions = rng.random_range(1..=3);

        // Partition duration range
        let partition_min = rng.random_range(300..800);
        let partition_max = rng.random_range(partition_min..2500);

        // Worker batch size range. Minimum of 2 ensures reasonable throughput
        // even with network issues causing reconnection delays.
        let batch_min = rng.random_range(2..=3);
        let batch_max = rng.random_range(batch_min..=6);

        Self {
            fail_rate,
            max_latency_ms,
            num_workers,
            num_jobs,
            num_cancels,
            num_expedites,
            num_restarts,
            num_partitions,
            partition_duration_range: (partition_min, partition_max),
            worker_batch_range: (batch_min, batch_max),
        }
    }
}

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("chaos", seed, 180, |sim| {
        let config = ChaosConfig::from_seed(seed);

        tracing::info!(
            fail_rate = config.fail_rate,
            max_latency_ms = config.max_latency_ms,
            num_workers = config.num_workers,
            num_jobs = config.num_jobs,
            num_cancels = config.num_cancels,
            num_expedites = config.num_expedites,
            num_restarts = config.num_restarts,
            num_partitions = config.num_partitions,
            partition_duration = ?config.partition_duration_range,
            worker_batch = ?config.worker_batch_range,
            "chaos_config"
        );

        // Capture config values for closures
        let fail_rate = config.fail_rate;
        let max_latency_ms = config.max_latency_ms;
        let num_workers = config.num_workers;
        let num_jobs = config.num_jobs;
        let num_cancels = config.num_cancels;
        let num_expedites = config.num_expedites;
        let num_restarts = config.num_restarts;
        let num_partitions = config.num_partitions;
        let partition_duration_range = config.partition_duration_range;
        let worker_batch_range = config.worker_batch_range;

        sim.set_fail_rate(fail_rate);
        sim.set_max_message_latency(Duration::from_millis(max_latency_ms));

        // Client configuration optimized for DST with short timeouts.
        let client_config = ClientConfig::for_dst();

        // Shared invariant tracker. State tracking is done via server-side DST events,
        // which are emitted synchronously by the server and collected in a thread-local
        // event bus. This eliminates race conditions from client-side tracking.
        let tracker = Arc::new(InvariantTracker::new());

        // Register concurrency limits
        for (key, max_conc) in CHAOS_LIMITS {
            if *max_conc > 0 {
                tracker.concurrency.register_limit(key, *max_conc);
            }
        }

        // Tracking state
        let total_enqueued = Arc::new(AtomicU32::new(0));
        let total_attempted = Arc::new(AtomicU32::new(0));
        let total_completed = Arc::new(AtomicU32::new(0));
        let scenario_done = Arc::new(AtomicBool::new(false));

        sim.host("server", || async move { setup_server(9910).await });

        // Producer: Enqueues jobs with mixed configurations
        let producer_enqueued = Arc::clone(&total_enqueued);
        let producer_attempted = Arc::clone(&total_attempted);
        let producer_seed = seed;
        let producer_num_jobs = num_jobs;
        let producer_config = client_config.clone();
        sim.client("producer", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let mut rng = StdRng::seed_from_u64(producer_seed.wrapping_add(1));

            let mut client = create_turmoil_client("http://server:9910", &producer_config).await?;
            let mut consecutive_failures = 0u32;

            for i in 0..producer_num_jobs {
                // Randomly select a limit configuration (including "unlimited")
                let (limit_key, max_conc) = CHAOS_LIMITS[rng.random_range(0..CHAOS_LIMITS.len())];
                let job_id = format!("{}-{}", limit_key, i);
                let priority = rng.random_range(1..100);

                // Random delay between enqueues
                let delay_ms = rng.random_range(0..50);
                if delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }

                // Build limits vec - empty for unlimited
                let limits = if max_conc > 0 {
                    vec![Limit {
                        limit: Some(limit::Limit::Concurrency(ConcurrencyLimit {
                            key: limit_key.into(),
                            max_concurrency: max_conc,
                        })),
                    }]
                } else {
                    vec![]
                };

                // Some jobs get retry policies
                let retry_policy = if rng.random_ratio(30, 100) {
                    Some(RetryPolicy {
                        retry_count: rng.random_range(1..=3),
                        initial_interval_ms: 100,
                        max_interval_ms: 1000,
                        randomize_interval: true,
                        backoff_factor: 1.5,
                    })
                } else {
                    None
                };

                tracing::trace!(
                    job_id = %job_id,
                    limit_key = limit_key,
                    max_conc = max_conc,
                    has_retry = retry_policy.is_some(),
                    "enqueue"
                );

                producer_attempted.fetch_add(1, Ordering::SeqCst);
                match client
                    .enqueue(tonic::Request::new(EnqueueRequest {
                        shard: 0,
                        id: job_id.clone(),
                        priority,
                        start_at_ms: 0,
                        retry_policy: retry_policy.clone(),
                        payload: Some(SerializedBytes {
                            encoding: Some(serialized_bytes::Encoding::Msgpack(rmp_serde::to_vec(&serde_json::json!({
                                "chaos": true,
                                "limit": limit_key,
                                "idx": i
                            }))
                            .unwrap())),
                        }),
                        limits: limits.clone(),
                        tenant: None,
                        metadata: HashMap::new(),
                        task_group: "default".to_string(),
                    }))
                    .await
                {
                    Ok(_) => {
                        producer_enqueued.fetch_add(1, Ordering::SeqCst);
                        consecutive_failures = 0;
                    }
                    Err(e) => {
                        tracing::trace!(job_id = %job_id, error = %e, "enqueue_failed");
                        consecutive_failures += 1;

                        // Reconnect after failures - the HTTP/2 connection may be
                        // corrupted due to packet loss breaking stream semantics.
                        if consecutive_failures >= 1 {
                            tracing::trace!("reconnecting after failure");
                            if let Ok(new_client) =
                                create_turmoil_client("http://server:9910", &producer_config).await
                            {
                                client = new_client;
                                consecutive_failures = 0;
                            }
                        }
                    }
                }
            }

            tracing::trace!(
                enqueued = producer_enqueued.load(Ordering::SeqCst),
                "producer_done"
            );
            Ok(())
        });

        // Canceller: Randomly cancels some jobs after they're enqueued
        let canceller_seed = seed;
        let canceller_config = client_config.clone();
        let canceller_num_cancels = num_cancels;
        sim.client("canceller", async move {
            // Wait for some jobs to be enqueued
            tokio::time::sleep(Duration::from_millis(500)).await;
            let mut rng = StdRng::seed_from_u64(canceller_seed.wrapping_add(99));

            let mut client = create_turmoil_client("http://server:9910", &canceller_config).await?;
            let mut consecutive_failures = 0u32;

            for _ in 0..canceller_num_cancels {
                let limit_idx = rng.random_range(0..CHAOS_LIMITS.len());
                let job_idx = rng.random_range(0..20);
                let job_id = format!("{}-{}", CHAOS_LIMITS[limit_idx].0, job_idx);

                tokio::time::sleep(Duration::from_millis(rng.random_range(100..500))).await;

                match client
                    .cancel_job(tonic::Request::new(CancelJobRequest {
                        shard: 0,
                        id: job_id.clone(),
                        tenant: None,
                    }))
                    .await
                {
                    Ok(_) => {
                        tracing::trace!(job_id = %job_id, "cancelled");
                        consecutive_failures = 0;
                    }
                    Err(e) => {
                        // Cancellation can fail if job doesn't exist or is already terminal
                        tracing::trace!(job_id = %job_id, error = %e, "cancel_failed");
                        consecutive_failures += 1;
                        if consecutive_failures >= 2 {
                            if let Ok(new_client) =
                                create_turmoil_client("http://server:9910", &canceller_config).await
                            {
                                client = new_client;
                                consecutive_failures = 0;
                            }
                        }
                    }
                }
            }

            tracing::trace!("canceller_done");
            Ok(())
        });

        // Expediter: Randomly expedites scheduled/retrying jobs
        let expediter_seed = seed;
        let expediter_config = client_config.clone();
        let expediter_num_expedites = num_expedites;
        sim.client("expediter", async move {
            // Wait for some jobs to be in various states
            tokio::time::sleep(Duration::from_millis(2000)).await;
            let mut rng = StdRng::seed_from_u64(expediter_seed.wrapping_add(77));

            let mut client = create_turmoil_client("http://server:9910", &expediter_config).await?;
            let mut consecutive_failures = 0u32;

            for i in 0..expediter_num_expedites {
                // Random delay between expedites
                tokio::time::sleep(Duration::from_millis(rng.random_range(500..2000))).await;

                let limit_idx = rng.random_range(0..CHAOS_LIMITS.len());
                let job_idx = rng.random_range(0..30);
                let job_id = format!("{}-{}", CHAOS_LIMITS[limit_idx].0, job_idx);

                match client
                    .expedite_job(tonic::Request::new(ExpediteJobRequest {
                        shard: 0,
                        id: job_id.clone(),
                        tenant: None,
                    }))
                    .await
                {
                    Ok(_) => {
                        tracing::trace!(job_id = %job_id, expedite_num = i, "expedited");
                        consecutive_failures = 0;
                    }
                    Err(e) => {
                        // Expedite can fail if job doesn't exist, is running, terminal, cancelled, etc.
                        tracing::trace!(job_id = %job_id, error = %e, "expedite_failed");
                        consecutive_failures += 1;
                        if consecutive_failures >= 2 {
                            if let Ok(new_client) =
                                create_turmoil_client("http://server:9910", &expediter_config).await
                            {
                                client = new_client;
                                consecutive_failures = 0;
                            }
                        }
                    }
                }
            }

            tracing::trace!("expediter_done");
            Ok(())
        });

        // Restarter: Randomly restarts failed/cancelled jobs
        let restarter_seed = seed;
        let restarter_config = client_config.clone();
        let restarter_num_restarts = num_restarts;
        sim.client("restarter", async move {
            // Wait for some jobs to potentially fail or be cancelled
            tokio::time::sleep(Duration::from_millis(5000)).await;
            let mut rng = StdRng::seed_from_u64(restarter_seed.wrapping_add(88));

            let mut client = create_turmoil_client("http://server:9910", &restarter_config).await?;
            let mut consecutive_failures = 0u32;

            for i in 0..restarter_num_restarts {
                // Random delay between restarts
                tokio::time::sleep(Duration::from_millis(rng.random_range(1000..3000))).await;

                let limit_idx = rng.random_range(0..CHAOS_LIMITS.len());
                let job_idx = rng.random_range(0..30);
                let job_id = format!("{}-{}", CHAOS_LIMITS[limit_idx].0, job_idx);

                match client
                    .restart_job(tonic::Request::new(RestartJobRequest {
                        shard: 0,
                        id: job_id.clone(),
                        tenant: None,
                    }))
                    .await
                {
                    Ok(_) => {
                        tracing::trace!(job_id = %job_id, restart_num = i, "restarted");
                        // Note: We don't track this in the job tracker since it might
                        // conflict with other state tracking. The server-side verification
                        // will catch any actual invariant violations.
                        consecutive_failures = 0;
                    }
                    Err(e) => {
                        // Restart can fail if job doesn't exist, isn't failed/cancelled, etc.
                        tracing::trace!(job_id = %job_id, error = %e, "restart_failed");
                        consecutive_failures += 1;
                        if consecutive_failures >= 2 {
                            if let Ok(new_client) =
                                create_turmoil_client("http://server:9910", &restarter_config).await
                            {
                                client = new_client;
                                consecutive_failures = 0;
                            }
                        }
                    }
                }
            }

            tracing::trace!("restarter_done");
            Ok(())
        });

        // Spawn workers
        for worker_num in 0..num_workers {
            let worker_seed = seed.wrapping_add(100 + worker_num as u64);
            let worker_id = format!("chaos-worker-{}", worker_num);
            let worker_completed = Arc::clone(&total_completed);
            let worker_done_flag = Arc::clone(&scenario_done);
            let worker_config = client_config.clone();
            let worker_batch_range = worker_batch_range;

            let client_name: &'static str =
                Box::leak(format!("worker{}", worker_num).into_boxed_str());
            sim.client(client_name, async move {
                // Stagger worker start times
                let start_delay = 100 + (worker_num as u64 * 30);
                tokio::time::sleep(Duration::from_millis(start_delay)).await;

                let mut rng = StdRng::seed_from_u64(worker_seed);

                let mut client =
                    create_turmoil_client("http://server:9910", &worker_config).await?;

                let mut completed = 0u32;
                let mut failed = 0u32;
                let mut processing: Vec<Task> = Vec::new();
                let mut consecutive_failures = 0u32;

                // 50 rounds with batch size 2+ should handle 20 jobs with network issues
                for round in 0..50 {
                    if worker_done_flag.load(Ordering::SeqCst) {
                        break;
                    }

                    // Random batch size from configured range
                    let max_tasks = rng.random_range(worker_batch_range.0..=worker_batch_range.1);

                    let lease_result = client
                        .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                            shard: Some(0),
                            worker_id: worker_id.clone(),
                            max_tasks,
                            task_group: "default".to_string(),
                        }))
                        .await;

                    match lease_result {
                        Ok(resp) => {
                            consecutive_failures = 0;
                            let tasks = resp.into_inner().tasks;

                            for task in tasks {
                                // Extract limit key from job_id for logging
                                let limit_key = task
                                    .job_id
                                    .split('-')
                                    .take(2)
                                    .collect::<Vec<_>>()
                                    .join("-");

                                tracing::trace!(
                                    worker = %worker_id,
                                    job_id = %task.job_id,
                                    task_id = %task.id,
                                    limit_key = %limit_key,
                                    round = round,
                                    "lease"
                                );
                                processing.push(task);
                            }
                        }
                        Err(e) => {
                            tracing::trace!(
                                worker = %worker_id,
                                error = %e,
                                round = round,
                                "lease_failed"
                            );
                            consecutive_failures += 1;

                            // Reconnect after failures - the HTTP/2 connection may be
                            // corrupted due to packet loss breaking stream semantics.
                            if consecutive_failures >= 1 {
                                tracing::trace!(worker = %worker_id, "reconnecting after lease failure");
                                if let Ok(new_client) =
                                    create_turmoil_client("http://server:9910", &worker_config).await
                                {
                                    client = new_client;
                                    consecutive_failures = 0;
                                }
                            }
                        }
                    }

                    // Process tasks
                    if !processing.is_empty() {
                        let num_to_complete = rng.random_range(1..=processing.len());
                        let mut indices_to_remove: HashSet<usize> = HashSet::new();

                        for i in 0..num_to_complete {
                            if i >= processing.len() {
                                break;
                            }

                            let task = &processing[i];

                            // Random processing time
                            let process_time = rng.random_range(5..100);
                            tokio::time::sleep(Duration::from_millis(process_time)).await;

                            // 10% chance of failure
                            let should_fail = rng.random_ratio(10, 100);

                            let outcome = if should_fail {
                                report_outcome_request::Outcome::Failure(silo::pb::Failure {
                                    code: "chaos_failure".into(),
                                    data: None,
                                })
                            } else {
                                report_outcome_request::Outcome::Success(SerializedBytes {
                                    encoding: Some(serialized_bytes::Encoding::Msgpack(rmp_serde::to_vec(&serde_json::json!("chaos_done"))
                                        .unwrap())),
                                })
                            };

                            match client
                                .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                    shard: 0,
                                    task_id: task.id.clone(),
                                    outcome: Some(outcome),
                                }))
                                .await
                            {
                                Ok(_) => {
                                    if should_fail {
                                        tracing::trace!(
                                            worker = %worker_id,
                                            job_id = %task.job_id,
                                            "report_failure"
                                        );
                                        failed += 1;
                                    } else {
                                        tracing::trace!(
                                            worker = %worker_id,
                                            job_id = %task.job_id,
                                            "complete"
                                        );
                                        completed += 1;
                                        worker_completed.fetch_add(1, Ordering::SeqCst);
                                    }
                                    indices_to_remove.insert(i);
                                }
                                Err(e) => {
                                    tracing::trace!(
                                        worker = %worker_id,
                                        job_id = %task.job_id,
                                        error = %e,
                                        "report_outcome_failed"
                                    );
                                }
                            }
                        }

                        // Remove completed tasks
                        let mut indices: Vec<_> = indices_to_remove.into_iter().collect();
                        indices.sort_by(|a, b| b.cmp(a));
                        for idx in indices {
                            processing.remove(idx);
                        }
                    }

                    // Random delay between rounds
                    let delay = rng.random_range(10..150);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }

                // Finish remaining tasks
                for task in &processing {
                    let process_time = rng.random_range(5..30);
                    tokio::time::sleep(Duration::from_millis(process_time)).await;

                    match client
                        .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                            shard: 0,
                            task_id: task.id.clone(),
                            outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                                encoding: Some(serialized_bytes::Encoding::Msgpack(rmp_serde::to_vec(&serde_json::json!("chaos_done")).unwrap())),
                            })),
                        }))
                        .await
                    {
                        Ok(_) => {
                            completed += 1;
                            worker_completed.fetch_add(1, Ordering::SeqCst);

                            tracing::trace!(
                                worker = %worker_id,
                                job_id = %task.job_id,
                                "complete_final"
                            );
                        }
                        Err(e) => {
                            tracing::trace!(
                                worker = %worker_id,
                                job_id = %task.job_id,
                                error = %e,
                                "report_outcome_failed_final"
                            );
                        }
                    }
                }

                tracing::trace!(
                    worker = %worker_id,
                    completed = completed,
                    failed = failed,
                    "worker_done"
                );

                Ok(())
            });
        }

        // Partition injector: Randomly partitions workers from server
        let partition_seed = seed;
        let partition_num_workers = num_workers;
        let partition_count = num_partitions;
        let partition_dur_range = partition_duration_range;
        sim.client("partitioner", async move {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let mut rng = StdRng::seed_from_u64(partition_seed.wrapping_add(200));

            for p in 0..partition_count {
                // Random delay between partitions
                tokio::time::sleep(Duration::from_millis(rng.random_range(2000..5000))).await;

                // Pick a random worker to partition
                let worker_to_partition = rng.random_range(0..partition_num_workers);
                let worker_name = format!("worker{}", worker_to_partition);

                let sim_time = turmoil::sim_elapsed().map(|d| d.as_millis()).unwrap_or(0);
                tracing::trace!(
                    worker = %worker_name,
                    partition = p,
                    sim_time_ms = sim_time,
                    "partition_start"
                );

                turmoil::partition(worker_name.as_str(), "server");

                // Partition duration from configured range
                let partition_duration =
                    rng.random_range(partition_dur_range.0..=partition_dur_range.1);
                tokio::time::sleep(Duration::from_millis(partition_duration)).await;

                turmoil::repair(worker_name.as_str(), "server");

                let sim_time = turmoil::sim_elapsed().map(|d| d.as_millis()).unwrap_or(0);
                tracing::trace!(
                    worker = %worker_name,
                    partition = p,
                    sim_time_ms = sim_time,
                    duration_ms = partition_duration,
                    "partition_end"
                );
            }

            tracing::trace!("partitioner_done");
            Ok(())
        });

        // Invariant verifier: Periodically checks system invariants
        // This verifier consumes server-side DST events to track state accurately
        let verifier_tracker = Arc::clone(&tracker);
        let verifier_completed = Arc::clone(&total_completed);
        let verifier_enqueued = Arc::clone(&total_enqueued);
        let verifier_attempted = Arc::clone(&total_attempted);
        let verifier_done_flag = Arc::clone(&scenario_done);
        let verifier_config = client_config.clone();
        sim.client("verifier", async move {
            // Wait for work to start
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Create client for server-side checks
            let mut client = match create_turmoil_client("http://server:9910", &verifier_config).await {
                Ok(c) => Some(c),
                Err(_) => None,
            };

            // Periodically verify invariants. Fewer checks because each check
            // involves RPC calls that can be delayed by network issues.
            for check in 0..8 {
                tokio::time::sleep(Duration::from_millis(500)).await;

                // Process DST events from server-side instrumentation
                verifier_tracker.process_dst_events();

                // Run client-side invariant checks (now based on server events)
                verifier_tracker.verify_all();

                // Run server-side invariant checks if we have a client
                if let Some(ref mut c) = client {
                    match verify_server_invariants(c, 0).await {
                        Ok(result) => {
                            if !result.violations.is_empty() {
                                tracing::warn!(
                                    violations = ?result.violations,
                                    "server_invariant_violations"
                                );
                            }
                            tracing::trace!(
                                check = check,
                                running = result.running_job_count,
                                terminal = result.terminal_job_count,
                                holders = ?result.holder_counts_by_queue,
                                "server_state"
                            );
                        }
                        Err(e) => {
                            tracing::trace!(error = %e, check = check, "server_check_failed");
                            // Try to reconnect
                            if let Ok(new_client) =
                                create_turmoil_client("http://server:9910", &verifier_config).await
                            {
                                client = Some(new_client);
                            }
                        }
                    }
                }

                let enqueued = verifier_enqueued.load(Ordering::SeqCst);
                let completed = verifier_completed.load(Ordering::SeqCst);

                tracing::trace!(
                    check = check,
                    enqueued = enqueued,
                    completed = completed,
                    "invariant_check_passed"
                );
            }

            // Wait for work to complete. The checks above take real time due to
            // RPC delays under network chaos, so keep this short to stay within
            // the 180s simulation budget.
            tokio::time::sleep(Duration::from_secs(40)).await;
            verifier_done_flag.store(true, Ordering::SeqCst);

            // Process final DST events
            verifier_tracker.process_dst_events();

            // Final invariant verification
            verifier_tracker.verify_all();
            verifier_tracker.jobs.verify_no_terminal_leases();

            // Verify state machine transitions were valid
            let transition_violations = verifier_tracker.jobs.verify_all_transitions();
            if !transition_violations.is_empty() {
                tracing::warn!(
                    violations = transition_violations.len(),
                    "transition_violations_found"
                );
                for v in &transition_violations {
                    tracing::warn!(violation = %v, "transition_violation");
                }
                // Fail on transition violations - these are now based on server events
                panic!(
                    "INVARIANT VIOLATION: {} invalid state transitions detected",
                    transition_violations.len()
                );
            }

            let attempted = verifier_attempted.load(Ordering::SeqCst);
            let enqueued = verifier_enqueued.load(Ordering::SeqCst);
            let completed = verifier_completed.load(Ordering::SeqCst);
            let terminal = verifier_tracker.jobs.terminal_count();

            tracing::info!(
                attempted = attempted,
                enqueued = enqueued,
                completed = completed,
                terminal = terminal,
                transition_violations = transition_violations.len(),
                "final_verification"
            );

            // Verify some progress was made. In chaos scenarios with high message loss,
            // we can't expect high completion rates - the important thing is that the
            // system doesn't deadlock and makes some progress.
            //
            // If the enqueue success rate was very low (< 50%), network conditions were
            // too extreme to expect meaningful progress. In this case, just verify
            // invariants hold without asserting on completion count.
            let enqueue_success_rate = if attempted > 0 {
                enqueued as f64 / attempted as f64
            } else {
                1.0
            };

            if enqueue_success_rate >= 0.5 {
                // Network conditions were reasonable, expect some progress
                let expected_min = (enqueued as f64 * 0.1) as u32; // Only expect 10% completion
                assert!(
                    completed >= expected_min || enqueued == 0,
                    "Insufficient progress: only {}/{} jobs completed (expected at least {})",
                    completed,
                    enqueued,
                    expected_min
                );
            } else {
                tracing::warn!(
                    enqueue_success_rate = enqueue_success_rate,
                    "Skipping progress assertion due to extreme network conditions"
                );
            }

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}
