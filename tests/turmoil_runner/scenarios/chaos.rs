//! Chaos scenario: Comprehensive fault injection with invariant verification.
//!
//! This scenario combines multiple fault modes with randomized parameters to
//! maximize state space exploration. It uses the InvariantTracker to continuously
//! verify system correctness throughout execution.
//!
//! Fault modes:
//! - Random message loss (5-20%)
//! - Variable message latency (5-100ms)
//! - Network partitions between workers and server
//! - Random worker crashes (via partition + timeout)
//! - Mix of concurrency-limited and unlimited jobs
//! - Random job cancellations
//! - Variable number of workers (2-6)
//!
//! Invariants verified (from Alloy spec):
//! - queueLimitEnforced: At most max_concurrency tasks per limit key
//! - oneLeasePerJob: A job never has two active leases
//! - noLeasesForTerminal: Terminal jobs have no active leases
//! - No duplicate completions

use crate::helpers::{
    ConcurrencyLimit, EnqueueRequest, GetJobRequest, HashMap, InvariantTracker, JobStatus,
    LeaseTasksRequest, Limit, MsgpackBytes, ReportOutcomeRequest, RetryPolicy, Task, get_seed,
    limit, report_outcome_request, run_scenario_impl, setup_server, turmoil, turmoil_connector,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use silo::pb::silo_client::SiloClient;
use silo::pb::CancelJobRequest;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Endpoint;

/// Connect to the server with retries.
/// In chaos mode, the initial connection can fail due to message loss.
async fn connect_with_retry(
    endpoint: &str,
    max_retries: u32,
) -> turmoil::Result<SiloClient<tonic::transport::Channel>> {
    let mut last_error: Option<String> = None;
    for attempt in 0..max_retries {
        match Endpoint::new(endpoint.to_string())
            .map_err(|e| e.to_string())?
            .connect_with_connector(turmoil_connector())
            .await
        {
            Ok(ch) => return Ok(SiloClient::new(ch)),
            Err(e) => {
                tracing::trace!(
                    attempt = attempt,
                    error = %e,
                    "connection attempt failed, retrying"
                );
                last_error = Some(e.to_string());
                // Exponential backoff
                tokio::time::sleep(Duration::from_millis(50 * (1 << attempt.min(4)))).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| "no connection attempts made".to_string()).into())
}

/// Concurrency limit configurations for chaos testing
const CHAOS_LIMITS: &[(&str, u32)] = &[
    ("chaos-mutex", 1),     // Strict serialization
    ("chaos-pair", 2),      // Small concurrency
    ("chaos-batch", 4),     // Medium concurrency
    ("chaos-unlimited", 0), // No limit (0 means unlimited)
];

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("chaos", seed, 180, |sim| {
        let mut config_rng = StdRng::seed_from_u64(seed);

        // Randomize network conditions based on seed
        let fail_rate = 0.05 + 0.15 * (config_rng.random_range(0..100) as f64 / 100.0);
        let max_latency_ms = config_rng.random_range(5..100);
        let num_workers = config_rng.random_range(2..=6);
        let num_jobs = config_rng.random_range(15..=40);

        tracing::info!(
            fail_rate = fail_rate,
            max_latency_ms = max_latency_ms,
            num_workers = num_workers,
            num_jobs = num_jobs,
            "chaos_config"
        );

        sim.set_fail_rate(fail_rate);
        sim.set_max_message_latency(Duration::from_millis(max_latency_ms));

        // Shared invariant tracker
        let tracker = Arc::new(InvariantTracker::new());

        // Register concurrency limits
        for (key, max_conc) in CHAOS_LIMITS {
            if *max_conc > 0 {
                tracker.concurrency.register_limit(key, *max_conc);
            }
        }

        // Tracking state
        let total_enqueued = Arc::new(AtomicU32::new(0));
        let total_completed = Arc::new(AtomicU32::new(0));
        let scenario_done = Arc::new(AtomicBool::new(false));

        sim.host("server", || async move { setup_server(9910).await });

        // Producer: Enqueues jobs with mixed configurations
        let producer_tracker = Arc::clone(&tracker);
        let producer_enqueued = Arc::clone(&total_enqueued);
        let producer_seed = seed;
        let producer_num_jobs = num_jobs;
        sim.client("producer", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let mut rng = StdRng::seed_from_u64(producer_seed.wrapping_add(1));

            let mut client = connect_with_retry("http://server:9910", 10).await?;

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

                match client
                    .enqueue(tonic::Request::new(EnqueueRequest {
                        shard: 0,
                        id: job_id.clone(),
                        priority,
                        start_at_ms: 0,
                        retry_policy,
                        payload: Some(MsgpackBytes {
                            data: rmp_serde::to_vec(&serde_json::json!({
                                "chaos": true,
                                "limit": limit_key,
                                "idx": i
                            }))
                            .unwrap(),
                        }),
                        limits,
                        tenant: None,
                        metadata: HashMap::new(),
                        task_group: "default".to_string(),
                    }))
                    .await
                {
                    Ok(_) => {
                        producer_tracker.jobs.job_enqueued(&job_id);
                        producer_enqueued.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => {
                        tracing::trace!(job_id = %job_id, error = %e, "enqueue_failed");
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
        let canceller_tracker = Arc::clone(&tracker);
        sim.client("canceller", async move {
            // Wait for some jobs to be enqueued
            tokio::time::sleep(Duration::from_millis(500)).await;
            let mut rng = StdRng::seed_from_u64(canceller_seed.wrapping_add(99));

            let mut client = connect_with_retry("http://server:9910", 10).await?;

            // Cancel a few random jobs
            let num_cancels = rng.random_range(2..=5);
            for _ in 0..num_cancels {
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
                        canceller_tracker.jobs.job_cancelled(&job_id);
                    }
                    Err(e) => {
                        // Cancellation can fail if job doesn't exist or is already terminal
                        tracing::trace!(job_id = %job_id, error = %e, "cancel_failed");
                    }
                }
            }

            tracing::trace!("canceller_done");
            Ok(())
        });

        // Spawn workers
        for worker_num in 0..num_workers {
            let worker_seed = seed.wrapping_add(100 + worker_num as u64);
            let worker_id = format!("chaos-worker-{}", worker_num);
            let worker_tracker = Arc::clone(&tracker);
            let worker_completed = Arc::clone(&total_completed);
            let worker_done_flag = Arc::clone(&scenario_done);

            let client_name: &'static str =
                Box::leak(format!("worker{}", worker_num).into_boxed_str());
            sim.client(client_name, async move {
                // Stagger worker start times
                let start_delay = 100 + (worker_num as u64 * 30);
                tokio::time::sleep(Duration::from_millis(start_delay)).await;

                let mut rng = StdRng::seed_from_u64(worker_seed);

                let mut client = connect_with_retry("http://server:9910", 10).await?;

                let mut completed = 0u32;
                let mut failed = 0u32;
                let mut processing: Vec<Task> = Vec::new();

                for round in 0..80 {
                    if worker_done_flag.load(Ordering::SeqCst) {
                        break;
                    }

                    // Random batch size
                    let max_tasks = rng.random_range(1..=5);

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
                            let tasks = resp.into_inner().tasks;

                            for task in tasks {
                                // Extract limit key from job_id
                                let limit_key = task
                                    .job_id
                                    .split('-')
                                    .take(2)
                                    .collect::<Vec<_>>()
                                    .join("-");

                                // Track lease in job state tracker
                                worker_tracker.jobs.task_leased(&task.job_id, &task.id);

                                // Track in concurrency tracker if this is a limited job
                                let is_limited = CHAOS_LIMITS
                                    .iter()
                                    .any(|(k, c)| *k == limit_key && *c > 0);
                                if is_limited {
                                    worker_tracker.concurrency.acquire(
                                        &limit_key,
                                        &task.id,
                                        &task.job_id,
                                    );
                                }

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
                                    data: vec![],
                                })
                            } else {
                                report_outcome_request::Outcome::Success(MsgpackBytes {
                                    data: rmp_serde::to_vec(&serde_json::json!("chaos_done"))
                                        .unwrap(),
                                })
                            };

                            // Release from tracker BEFORE report_outcome because the server
                            // will release and potentially grant to another worker immediately
                            let limit_key = task
                                .job_id
                                .split('-')
                                .take(2)
                                .collect::<Vec<_>>()
                                .join("-");
                            worker_tracker.jobs.task_released(&task.job_id, &task.id);
                            let is_limited = CHAOS_LIMITS
                                .iter()
                                .any(|(k, c)| *k == limit_key && *c > 0);
                            if is_limited {
                                worker_tracker.concurrency.release(
                                    &limit_key,
                                    &task.id,
                                    &task.job_id,
                                );
                            }

                            match client
                                .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                    shard: 0,
                                    tenant: None,
                                    task_id: task.id.clone(),
                                    outcome: Some(outcome),
                                }))
                                .await
                            {
                                Ok(_) => {
                                    // Releases already done above

                                    if should_fail {
                                        tracing::trace!(
                                            worker = %worker_id,
                                            job_id = %task.job_id,
                                            "report_failure"
                                        );
                                        worker_tracker.jobs.job_retrying(&task.job_id);
                                        failed += 1;
                                    } else {
                                        // Check for duplicate completion
                                        let was_new =
                                            worker_tracker.jobs.job_completed(&task.job_id);
                                        assert!(
                                            was_new,
                                            "INVARIANT VIOLATION: Job '{}' completed twice!",
                                            task.job_id
                                        );

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
                                    // Re-acquire since we pre-released but the report failed
                                    worker_tracker.jobs.task_leased(&task.job_id, &task.id);
                                    if is_limited {
                                        worker_tracker.concurrency.acquire(
                                            &limit_key,
                                            &task.id,
                                            &task.job_id,
                                        );
                                    }
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

                    // Release from tracker BEFORE report_outcome
                    let limit_key = task
                        .job_id
                        .split('-')
                        .take(2)
                        .collect::<Vec<_>>()
                        .join("-");
                    worker_tracker.jobs.task_released(&task.job_id, &task.id);
                    let is_limited =
                        CHAOS_LIMITS.iter().any(|(k, c)| *k == limit_key && *c > 0);
                    if is_limited {
                        worker_tracker
                            .concurrency
                            .release(&limit_key, &task.id, &task.job_id);
                    }

                    match client
                        .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                            shard: 0,
                            tenant: None,
                            task_id: task.id.clone(),
                            outcome: Some(report_outcome_request::Outcome::Success(MsgpackBytes {
                                data: rmp_serde::to_vec(&serde_json::json!("chaos_done")).unwrap(),
                            })),
                        }))
                        .await
                    {
                        Ok(_) => {
                            // Releases already done above

                            let was_new = worker_tracker.jobs.job_completed(&task.job_id);
                            if was_new {
                                completed += 1;
                                worker_completed.fetch_add(1, Ordering::SeqCst);
                            }

                            tracing::trace!(
                                worker = %worker_id,
                                job_id = %task.job_id,
                                "complete_final"
                            );
                        }
                        Err(e) => {
                            // Re-acquire since we pre-released but the report failed
                            worker_tracker.jobs.task_leased(&task.job_id, &task.id);
                            if is_limited {
                                worker_tracker.concurrency.acquire(
                                    &limit_key,
                                    &task.id,
                                    &task.job_id,
                                );
                            }
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
        sim.client("partitioner", async move {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let mut rng = StdRng::seed_from_u64(partition_seed.wrapping_add(200));

            // Inject a few partitions during the scenario
            let num_partitions = rng.random_range(1..=3);
            for p in 0..num_partitions {
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

                // Partition duration
                let partition_duration = rng.random_range(500..2000);
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
        let verifier_tracker = Arc::clone(&tracker);
        let verifier_completed = Arc::clone(&total_completed);
        let verifier_enqueued = Arc::clone(&total_enqueued);
        let verifier_done_flag = Arc::clone(&scenario_done);
        sim.client("verifier", async move {
            // Wait for work to start
            tokio::time::sleep(Duration::from_millis(500)).await;

            let mut client = connect_with_retry("http://server:9910", 10).await?;

            // Periodically verify invariants
            for check in 0..20 {
                tokio::time::sleep(Duration::from_millis(500)).await;

                // Run all invariant checks
                verifier_tracker.verify_all();

                let enqueued = verifier_enqueued.load(Ordering::SeqCst);
                let completed = verifier_completed.load(Ordering::SeqCst);

                tracing::trace!(
                    check = check,
                    enqueued = enqueued,
                    completed = completed,
                    "invariant_check_passed"
                );
            }

            // Final verification after all work should be done
            tokio::time::sleep(Duration::from_secs(120)).await;
            verifier_done_flag.store(true, Ordering::SeqCst);

            // Final invariant verification
            verifier_tracker.verify_all();
            verifier_tracker.jobs.verify_no_terminal_leases();

            let enqueued = verifier_enqueued.load(Ordering::SeqCst);
            let completed = verifier_completed.load(Ordering::SeqCst);
            let terminal = verifier_tracker.jobs.terminal_count();

            tracing::info!(
                enqueued = enqueued,
                completed = completed,
                terminal = terminal,
                "final_verification"
            );

            // Query server to verify job states
            let mut server_terminal = 0u32;
            let mut server_running = 0u32;
            let mut server_scheduled = 0u32;

            for (limit_key, _) in CHAOS_LIMITS {
                for i in 0..50 {
                    let job_id = format!("{}-{}", limit_key, i);
                    match client
                        .get_job(tonic::Request::new(GetJobRequest {
                            shard: 0,
                            id: job_id.clone(),
                            tenant: None,
                            include_attempts: false,
                        }))
                        .await
                    {
                        Ok(resp) => {
                            let job = resp.into_inner();
                            match job.status() {
                                JobStatus::Succeeded
                                | JobStatus::Failed
                                | JobStatus::Cancelled => {
                                    server_terminal += 1;
                                }
                                JobStatus::Running => {
                                    server_running += 1;
                                    tracing::trace!(job_id = %job_id, "job_still_running");
                                }
                                JobStatus::Scheduled => {
                                    server_scheduled += 1;
                                    tracing::trace!(job_id = %job_id, "job_still_scheduled");
                                }
                            }
                        }
                        Err(_) => {
                            // Job may not exist if enqueue failed
                        }
                    }
                }
            }

            tracing::info!(
                server_terminal = server_terminal,
                server_running = server_running,
                server_scheduled = server_scheduled,
                "server_job_states"
            );

            // Verify reasonable progress was made
            let expected_min = (enqueued as f64 * 0.3) as u32;
            assert!(
                completed >= expected_min || enqueued == 0,
                "Insufficient progress: only {}/{} jobs completed (expected at least {})",
                completed,
                enqueued,
                expected_min
            );

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}
