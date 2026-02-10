//! Latency invariants scenario: High message delays with invariant-focused verification.
//!
//! This scenario tests system correctness under network latency without message loss.
//! Unlike the chaos scenario which focuses on reconnection resilience, this scenario
//! assumes reliable delivery but with significant delays, focusing on invariant
//! violations that might emerge from timing-dependent races.
//!
//! Configuration:
//! - No message loss (fail_rate = 0)
//! - Variable high latency (50-500ms)
//! - Multiple concurrency queues with different limits
//! - Multiple workers competing for limited concurrency slots
//! - Frequent invariant checks
//!
//! Invariants verified (from Alloy spec):
//! - queueLimitEnforced: At most max_concurrency tasks per limit key
//! - oneLeasePerJob: A job never has two active leases
//! - noLeasesForTerminal: Terminal jobs have no active leases
//! - validTransitions: Only valid status transitions occur
//! - noDoubleLease: A task is never leased twice

use crate::helpers::{
    ClientConfig, ConcurrencyLimit, EnqueueRequest, HashMap, InvariantTracker, LeaseTasksRequest,
    Limit, ReportOutcomeRequest, RetryPolicy, SerializedBytes, TEST_SHARD_ID, Task,
    connect_to_server, get_seed, limit, report_outcome_request, run_scenario_impl,
    serialized_bytes, setup_server,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

/// Concurrency limit configurations - mix of strict and permissive limits
/// to stress concurrency tracking under latency conditions.
const CONCURRENCY_LIMITS: &[(&str, u32)] = &[
    ("latency-serial", 1),   // Strict serialization - most sensitive to races
    ("latency-pair", 2),     // Small concurrency window
    ("latency-quad", 4),     // Medium concurrency
    ("latency-octet", 8),    // Larger concurrency
    ("latency-unlimited", 0), // No limit (0 means unlimited)
];

/// Configuration for the latency invariants scenario
struct LatencyConfig {
    /// Minimum message latency in ms
    min_latency_ms: u64,
    /// Maximum message latency in ms
    max_latency_ms: u64,
    /// Number of worker clients
    num_workers: u32,
    /// Number of producer clients
    num_producers: u32,
    /// Total jobs to enqueue across all producers
    num_jobs: u32,
    /// How often to check invariants (in ms)
    invariant_check_interval_ms: u64,
    /// Number of invariant checks to perform
    num_invariant_checks: u32,
    /// Batch size range for workers (min, max)
    worker_batch_range: (u32, u32),
    /// Percentage of jobs that should fail (0-100)
    failure_rate_percent: u32,
    /// Percentage of jobs with retry policies (0-100)
    retry_rate_percent: u32,
}

impl LatencyConfig {
    fn from_seed(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);

        // High latency (50-300ms) to stress test timing-dependent code paths
        // and verify invariants hold even with significant network delays
        let min_latency_ms = rng.random_range(50..150);
        let max_latency_ms = rng.random_range(150..300);

        // Multiple workers to create contention on limited concurrency slots
        let num_workers = rng.random_range(4..=8);

        // Multiple producers to create interleaved enqueues
        let num_producers = rng.random_range(2..=4);

        // Higher job count to stress test the system
        let num_jobs = rng.random_range(80..=150);

        // Frequent invariant checks to catch timing violations
        let invariant_check_interval_ms = rng.random_range(200..400);
        let num_invariant_checks = rng.random_range(15..=25);

        // Worker batch sizes - larger batches can stress concurrency limits more
        let batch_min = rng.random_range(2..=4);
        let batch_max = rng.random_range(batch_min..=8);

        // Moderate failure rate to exercise retry logic
        let failure_rate_percent = rng.random_range(5..=15);

        // Higher retry rate to create more state transitions
        let retry_rate_percent = rng.random_range(40..=60);

        Self {
            min_latency_ms,
            max_latency_ms,
            num_workers,
            num_producers,
            num_jobs,
            invariant_check_interval_ms,
            num_invariant_checks,
            worker_batch_range: (batch_min, batch_max),
            failure_rate_percent,
            retry_rate_percent,
        }
    }
}

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("high_latency", seed, 180, |sim| {
        let config = LatencyConfig::from_seed(seed);

        tracing::info!(
            min_latency_ms = config.min_latency_ms,
            max_latency_ms = config.max_latency_ms,
            num_workers = config.num_workers,
            num_producers = config.num_producers,
            num_jobs = config.num_jobs,
            invariant_check_interval_ms = config.invariant_check_interval_ms,
            num_invariant_checks = config.num_invariant_checks,
            worker_batch = ?config.worker_batch_range,
            failure_rate = config.failure_rate_percent,
            retry_rate = config.retry_rate_percent,
            "high_latency_config"
        );

        // Capture config values for closures
        let max_latency_ms = config.max_latency_ms;
        let num_workers = config.num_workers;
        let num_producers = config.num_producers;
        let num_jobs = config.num_jobs;
        let invariant_check_interval_ms = config.invariant_check_interval_ms;
        let _num_invariant_checks = config.num_invariant_checks;
        let worker_batch_range = config.worker_batch_range;
        let failure_rate_percent = config.failure_rate_percent;
        let retry_rate_percent = config.retry_rate_percent;

        // No message loss - this scenario focuses on latency-induced races
        sim.set_fail_rate(0.0);
        sim.set_max_message_latency(Duration::from_millis(max_latency_ms));

        // Use standard DST client configuration
        let client_config = ClientConfig::for_dst();

        // Shared invariant tracker
        let tracker = Arc::new(InvariantTracker::new());

        // Register all concurrency limits
        for (key, max_conc) in CONCURRENCY_LIMITS {
            if *max_conc > 0 {
                tracker.concurrency.register_limit(key, *max_conc);
            }
        }

        // Tracking state
        let total_enqueued = Arc::new(AtomicU32::new(0));
        let total_completed = Arc::new(AtomicU32::new(0));
        let total_failed = Arc::new(AtomicU32::new(0));
        let scenario_done = Arc::new(AtomicBool::new(false));
        let enqueueing_done = Arc::new(AtomicBool::new(false));
        let producers_done = Arc::new(AtomicU32::new(0));

        sim.host("server", || async move { setup_server(9910).await });

        // Spawn multiple producers to create interleaved enqueue patterns
        let jobs_per_producer = num_jobs / num_producers;
        for producer_num in 0..num_producers {
            let producer_enqueued = Arc::clone(&total_enqueued);
            let producer_seed = seed.wrapping_add(producer_num as u64);
            let producer_jobs = if producer_num == num_producers - 1 {
                // Last producer takes any remainder
                num_jobs - (jobs_per_producer * (num_producers - 1))
            } else {
                jobs_per_producer
            };
            let producer_retry_rate = retry_rate_percent;
            let producer_done_counter = Arc::clone(&producers_done);
            let producer_enqueueing_done = Arc::clone(&enqueueing_done);
            let producer_num_producers = num_producers;

            let client_name: &'static str =
                Box::leak(format!("producer{}", producer_num).into_boxed_str());
            sim.client(client_name, async move {
                let mut rng = StdRng::seed_from_u64(producer_seed);

                // Stagger producer start times
                let start_delay = 50 + (producer_num as u64 * 100);
                tokio::time::sleep(Duration::from_millis(start_delay)).await;

                let mut client = connect_to_server("http://server:9910").await?;

                for i in 0..producer_jobs {
                    // Select a concurrency limit configuration
                    let (limit_key, max_conc) =
                        CONCURRENCY_LIMITS[rng.random_range(0..CONCURRENCY_LIMITS.len())];
                    let job_id = format!("p{}-{}-{}", producer_num, limit_key, i);
                    let priority = rng.random_range(1..100);

                    // Build limits - empty for unlimited
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

                    // Retry policy for some jobs
                    let retry_policy = if rng.random_ratio(producer_retry_rate, 100) {
                        Some(RetryPolicy {
                            retry_count: rng.random_range(1..=3),
                            initial_interval_ms: 50,
                            max_interval_ms: 500,
                            randomize_interval: true,
                            backoff_factor: 1.5,
                        })
                    } else {
                        None
                    };

                    tracing::trace!(
                        producer = producer_num,
                        job_id = %job_id,
                        limit_key = limit_key,
                        max_conc = max_conc,
                        has_retry = retry_policy.is_some(),
                        "enqueue"
                    );

                    match client
                        .enqueue(tonic::Request::new(EnqueueRequest {
                            shard: TEST_SHARD_ID.to_string(),
                            id: job_id.clone(),
                            priority,
                            start_at_ms: 0,
                            retry_policy,
                            payload: Some(SerializedBytes {
                                encoding: Some(serialized_bytes::Encoding::Msgpack(
                                    rmp_serde::to_vec(&serde_json::json!({
                                        "scenario": "high_latency",
                                        "limit": limit_key,
                                        "producer": producer_num,
                                        "idx": i
                                    }))
                                    .unwrap(),
                                )),
                            }),
                            limits,
                            tenant: None,
                            metadata: HashMap::new(),
                            task_group: "default".to_string(),
                        }))
                        .await
                    {
                        Ok(_) => {
                            producer_enqueued.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(e) => {
                            // With no message loss, failures should be rare
                            tracing::warn!(job_id = %job_id, error = %e, "enqueue_failed_unexpectedly");
                        }
                    }

                    // Small delay between enqueues to spread load
                    let delay_ms = rng.random_range(10..50);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }

                tracing::trace!(
                    producer = producer_num,
                    enqueued = producer_enqueued.load(Ordering::SeqCst),
                    "producer_done"
                );

                // Signal that this producer is done
                let done_count = producer_done_counter.fetch_add(1, Ordering::SeqCst) + 1;
                if done_count >= producer_num_producers {
                    producer_enqueueing_done.store(true, Ordering::SeqCst);
                    tracing::trace!("all_producers_done");
                }

                Ok(())
            });
        }

        // Spawn workers that compete for concurrency-limited jobs
        for worker_num in 0..num_workers {
            let worker_seed = seed.wrapping_add(100 + worker_num as u64);
            let worker_id = format!("latency-worker-{}", worker_num);
            let worker_completed = Arc::clone(&total_completed);
            let worker_failed = Arc::clone(&total_failed);
            let worker_done_flag = Arc::clone(&scenario_done);
            let _worker_config = client_config.clone();
            let worker_batch_range = worker_batch_range;
            let worker_failure_rate = failure_rate_percent;

            let client_name: &'static str =
                Box::leak(format!("worker{}", worker_num).into_boxed_str());
            sim.client(client_name, async move {
                // Stagger worker start times
                let start_delay = 200 + (worker_num as u64 * 50);
                tokio::time::sleep(Duration::from_millis(start_delay)).await;

                let mut rng = StdRng::seed_from_u64(worker_seed);
                let mut client = connect_to_server("http://server:9910").await?;

                let mut completed = 0u32;
                let mut failed = 0u32;
                let mut processing: Vec<Task> = Vec::new();
                let mut round = 0u32;

                // Keep working until the verifier signals convergence.
                // Under high latency with serial concurrency limits, jobs can take
                // many rounds to process, so a fixed round limit is insufficient.
                while !worker_done_flag.load(Ordering::SeqCst) {
                    round += 1;

                    // Variable batch size
                    let max_tasks = rng.random_range(worker_batch_range.0..=worker_batch_range.1);

                    match client
                        .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                            shard: Some(TEST_SHARD_ID.to_string()),
                            worker_id: worker_id.clone(),
                            max_tasks,
                            task_group: "default".to_string(),
                        }))
                        .await
                    {
                        Ok(resp) => {
                            let tasks = resp.into_inner().tasks;
                            for task in tasks {
                                let limit_key = extract_limit_key(&task.job_id);
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
                            // With no message loss, failures should be rare
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

                            // Simulate processing time
                            let process_time = rng.random_range(10..100);
                            tokio::time::sleep(Duration::from_millis(process_time)).await;

                            // Configurable failure rate
                            let should_fail = rng.random_ratio(worker_failure_rate, 100);

                            let outcome = if should_fail {
                                report_outcome_request::Outcome::Failure(silo::pb::Failure {
                                    code: "latency_test_failure".into(),
                                    data: None,
                                })
                            } else {
                                report_outcome_request::Outcome::Success(SerializedBytes {
                                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                                        rmp_serde::to_vec(&serde_json::json!("latency_done"))
                                            .unwrap(),
                                    )),
                                })
                            };

                            match client
                                .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                    shard: TEST_SHARD_ID.to_string(),
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
                                        worker_failed.fetch_add(1, Ordering::SeqCst);
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

                    // Short delay between rounds
                    let delay = rng.random_range(20..80);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }

                // Finish remaining tasks
                for task in &processing {
                    let process_time = rng.random_range(5..30);
                    tokio::time::sleep(Duration::from_millis(process_time)).await;

                    match client
                        .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                            shard: TEST_SHARD_ID.to_string(),
                            task_id: task.id.clone(),
                            outcome: Some(report_outcome_request::Outcome::Success(
                                SerializedBytes {
                                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                                        rmp_serde::to_vec(&serde_json::json!("latency_done"))
                                            .unwrap(),
                                    )),
                                },
                            )),
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

        // Invariant verifier: Frequently checks system invariants using DST events.
        // This is the core of this scenario - more frequent checks than chaos scenario.
        let verifier_tracker = Arc::clone(&tracker);
        let verifier_completed = Arc::clone(&total_completed);
        let verifier_failed = Arc::clone(&total_failed);
        let verifier_enqueued = Arc::clone(&total_enqueued);
        let verifier_done_flag = Arc::clone(&scenario_done);
        let verifier_enqueueing_done = Arc::clone(&enqueueing_done);
        sim.client("verifier", async move {
            // Wait for work to start
            tokio::time::sleep(Duration::from_millis(300)).await;

            let mut check = 0u32;

            // Phase 1: Wait for all jobs to reach terminal state.
            // Use the terminal event counter as a convergence heuristic (includes pending events).
            loop {
                tokio::time::sleep(Duration::from_millis(invariant_check_interval_ms)).await;

                let enqueued = verifier_enqueued.load(Ordering::SeqCst);
                let terminal = silo::dst_events::terminal_event_count();
                let enqueueing_complete = verifier_enqueueing_done.load(Ordering::SeqCst);

                tracing::trace!(
                    check = check,
                    enqueued = enqueued,
                    terminal = terminal,
                    enqueueing_complete = enqueueing_complete,
                    "convergence_check"
                );

                check += 1;

                // Exit when all jobs have been enqueued and all have reached terminal state
                if enqueueing_complete && enqueued > 0 && terminal >= enqueued as usize {
                    tracing::info!(
                        enqueued = enqueued,
                        terminal = terminal,
                        checks = check,
                        "all_jobs_terminal"
                    );
                    break;
                }

                // Safety limit to prevent infinite loops
                if check > 500 {
                    tracing::warn!(
                        enqueued = enqueued,
                        terminal = terminal,
                        enqueueing_complete = enqueueing_complete,
                        "reached check limit, proceeding to final verification"
                    );
                    break;
                }
            }

            // Wait a bit longer to allow any in-flight writes to complete before stopping workers.
            // The terminal_event_count() includes pending events, but we validate against confirmed
            // events. Under high latency, there may be writes in-flight that haven't been confirmed yet.
            // Give them time to complete.
            tokio::time::sleep(Duration::from_millis(invariant_check_interval_ms * 2)).await;

            // Signal workers to stop
            verifier_done_flag.store(true, Ordering::SeqCst);

            // Process all confirmed DST events and validate invariants
            verifier_tracker.process_and_validate();
            verifier_tracker.verify_all();
            verifier_tracker.jobs.verify_no_terminal_leases();

            // Verify all state machine transitions were valid
            let transition_violations = verifier_tracker.jobs.verify_all_transitions();
            if !transition_violations.is_empty() {
                tracing::error!(
                    violations = transition_violations.len(),
                    "transition_violations_found"
                );
                for v in &transition_violations {
                    tracing::error!(violation = %v, "transition_violation");
                }
                panic!(
                    "INVARIANT VIOLATION: {} invalid state transitions detected",
                    transition_violations.len()
                );
            }

            // Verify concurrency limits were never exceeded
            verifier_tracker.concurrency.verify_no_duplicate_job_leases();

            let enqueued = verifier_enqueued.load(Ordering::SeqCst);
            let completed = verifier_completed.load(Ordering::SeqCst);
            let failed = verifier_failed.load(Ordering::SeqCst);
            let terminal = verifier_tracker.jobs.terminal_count();

            tracing::info!(
                enqueued = enqueued,
                completed = completed,
                failed = failed,
                terminal = terminal,
                checks = check,
                "final_verification"
            );

            // With no message loss, ALL enqueued jobs should reach terminal state
            if enqueued > 0 && terminal < enqueued as usize {
                let non_terminal = verifier_tracker.jobs.get_non_terminal_jobs();
                tracing::error!(
                    terminal = terminal,
                    enqueued = enqueued,
                    missing = non_terminal.len(),
                    "not_all_jobs_terminal"
                );
                for (job_id, status) in &non_terminal {
                    tracing::error!(job_id = %job_id, status = %status, "non_terminal_job");
                }
                panic!(
                    "Not all jobs reached terminal state: {}/{} jobs are terminal. \
                     With no message loss, all jobs should complete.\n\
                     Non-terminal jobs: {:?}",
                    terminal,
                    enqueued,
                    non_terminal
                );
            }

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}

/// Extract the limit key from a job_id formatted as "p{producer}-{limit_key}-{idx}"
fn extract_limit_key(job_id: &str) -> String {
    // Skip "p{N}-" prefix, take until the last "-{idx}"
    let parts: Vec<&str> = job_id.split('-').collect();
    if parts.len() >= 3 {
        // Join all parts except first (producer) and last (idx)
        parts[1..parts.len() - 1].join("-")
    } else {
        job_id.to_string()
    }
}
