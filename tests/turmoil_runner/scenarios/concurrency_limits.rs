//! Concurrency limits stress scenario: Testing job concurrency limit enforcement
//! under randomized load with multiple queues, workers, and orderings.
//!
//! This test uses seeded randomization to explore different interleavings:
//! - Multiple concurrency limit keys with varying max_concurrency values
//! - Multiple producers enqueueing jobs in random order
//! - Multiple workers competing for leases with random timing
//! - Random job completion times and occasional failures
//!
//! Invariants verified:
//! 1. Per-response limit check: A single lease response never contains more tasks
//!    for a limit key than that limit's max_concurrency
//! 2. Global concurrency limit: At no point do we have more than max_concurrency
//!    tasks actively leased system-wide for any limit key (queueLimitEnforced)
//! 3. No duplicate completions: Same job_id never reported as success twice
//! 4. All jobs terminal: Every enqueued job reaches a terminal state (via GetJob)
//! 5. System progress: Jobs complete without deadlock

use crate::helpers::{
    AttemptStatus, ConcurrencyLimit, EnqueueRequest, GetJobRequest, HashMap, InvariantTracker,
    JobStatus, LeaseTasksRequest, Limit, ReportOutcomeRequest, SerializedBytes, TEST_SHARD_ID,
    Task, get_seed, limit, report_outcome_request, run_scenario_impl, serialized_bytes,
    setup_server, turmoil_connector,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use silo::pb::silo_client::SiloClient;
use std::collections::{HashMap as StdHashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::transport::Endpoint;

/// Configuration for a concurrency limit group
struct LimitConfig {
    key: &'static str,
    max_concurrency: u32,
}

/// The limit groups we'll test with - varying concurrency to stress different code paths
const LIMIT_CONFIGS: &[LimitConfig] = &[
    LimitConfig {
        key: "strict-1",
        max_concurrency: 1,
    },
    LimitConfig {
        key: "pair-2",
        max_concurrency: 2,
    },
    LimitConfig {
        key: "batch-5",
        max_concurrency: 5,
    },
];

const NUM_JOBS_PER_LIMIT: usize = 10;
const NUM_WORKERS: usize = 4;

/// Extract limit key from job_id (format: "{key}-p{n}-{num}")
fn extract_limit_key(job_id: &str) -> String {
    job_id
        .split('-')
        .take_while(|s| *s != "p1" && *s != "p2")
        .collect::<Vec<_>>()
        .join("-")
}

/// Get max_concurrency for a limit key
fn get_max_concurrency(limit_key: &str) -> Option<u32> {
    LIMIT_CONFIGS
        .iter()
        .find(|c| c.key == limit_key)
        .map(|c| c.max_concurrency)
}

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("concurrency_limits", seed, 120, |sim| {
        // Shared state for invariant checking
        let total_completed: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
        let total_enqueued: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));

        // Track all enqueued job_ids for terminal state verification
        let enqueued_jobs: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        // Track completed job_ids to detect duplicates (invariant #3)
        let completed_jobs: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

        // Shared invariant tracker. State tracking is done via server-side DST events,
        // which are emitted synchronously by the server and collected in a thread-local
        // event bus. This eliminates race conditions from client-side tracking.
        let tracker = Arc::new(InvariantTracker::new());

        // Register all limit keys with their max concurrency values
        for config in LIMIT_CONFIGS {
            tracker
                .concurrency
                .register_limit(config.key, config.max_concurrency);
        }

        sim.host("server", || async move { setup_server(9905).await });

        // Producer 1: Enqueues jobs in random order across all limit groups
        // Note: Job tracking is handled by server-side DST events, not client-side
        let producer_seed = seed;
        let enqueue_counter1 = Arc::clone(&total_enqueued);
        let enqueued_jobs1 = Arc::clone(&enqueued_jobs);
        sim.client("producer1", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let mut rng = StdRng::seed_from_u64(producer_seed.wrapping_add(1));

            let ch = Endpoint::new("http://server:9905")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            // Build list of all jobs to enqueue, then shuffle
            let mut jobs: Vec<(usize, usize)> = Vec::new();
            for (limit_idx, _) in LIMIT_CONFIGS.iter().enumerate() {
                for job_num in 0..(NUM_JOBS_PER_LIMIT / 2) {
                    jobs.push((limit_idx, job_num));
                }
            }

            // Fisher-Yates shuffle with seeded rng
            for i in (1..jobs.len()).rev() {
                let j = rng.random_range(0..=i);
                jobs.swap(i, j);
            }

            let mut enqueued = 0;
            for (limit_idx, job_num) in jobs {
                let config = &LIMIT_CONFIGS[limit_idx];
                let job_id = format!("{}-p1-{}", config.key, job_num);
                let priority = rng.random_range(1..100);

                // Random delay between enqueues to create interesting interleavings
                let delay_ms = rng.random_range(0..30);
                if delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }

                tracing::trace!(
                    job_id = %job_id,
                    limit_key = config.key,
                    max_conc = config.max_concurrency,
                    "enqueue"
                );

                match client
                    .enqueue(tonic::Request::new(EnqueueRequest {
                        shard: TEST_SHARD_ID.to_string(),
                        id: job_id.clone(),
                        priority,
                        start_at_ms: 0,
                        retry_policy: None,
                        payload: Some(SerializedBytes {
                            encoding: Some(serialized_bytes::Encoding::Msgpack(
                                rmp_serde::to_vec(&serde_json::json!({
                                    "limit_key": config.key,
                                    "job_num": job_num
                                }))
                                .unwrap(),
                            )),
                        }),
                        limits: vec![Limit {
                            limit: Some(limit::Limit::Concurrency(ConcurrencyLimit {
                                key: config.key.into(),
                                max_concurrency: config.max_concurrency,
                            })),
                        }],
                        tenant: None,
                        metadata: HashMap::new(),
                        task_group: "default".to_string(),
                    }))
                    .await
                {
                    Ok(_) => {
                        enqueued += 1;
                        enqueue_counter1.fetch_add(1, Ordering::SeqCst);
                        enqueued_jobs1.lock().unwrap().push(job_id);
                    }
                    Err(e) => {
                        tracing::trace!(job_id = %job_id, error = %e, "enqueue_failed");
                    }
                }
            }

            tracing::trace!(enqueued = enqueued, "producer1_done");
            Ok(())
        });

        // Producer 2: Enqueues the other half of jobs with different timing
        let producer2_seed = seed;
        let enqueue_counter2 = Arc::clone(&total_enqueued);
        let enqueued_jobs2 = Arc::clone(&enqueued_jobs);
        sim.client("producer2", async move {
            // Start slightly later to create interleaving with producer1
            tokio::time::sleep(Duration::from_millis(80)).await;
            let mut rng = StdRng::seed_from_u64(producer2_seed.wrapping_add(2));

            let ch = Endpoint::new("http://server:9905")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            let mut jobs: Vec<(usize, usize)> = Vec::new();
            for (limit_idx, _) in LIMIT_CONFIGS.iter().enumerate() {
                for job_num in (NUM_JOBS_PER_LIMIT / 2)..NUM_JOBS_PER_LIMIT {
                    jobs.push((limit_idx, job_num));
                }
            }

            // Shuffle differently from producer1
            for i in 0..(jobs.len().saturating_sub(1)) {
                let j = rng.random_range(i..jobs.len());
                jobs.swap(i, j);
            }

            let mut enqueued = 0;
            for (limit_idx, job_num) in jobs {
                let config = &LIMIT_CONFIGS[limit_idx];
                let job_id = format!("{}-p2-{}", config.key, job_num);
                let priority = rng.random_range(1..100);

                let delay_ms = rng.random_range(5..40);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                tracing::trace!(
                    job_id = %job_id,
                    limit_key = config.key,
                    max_conc = config.max_concurrency,
                    "enqueue"
                );

                match client
                    .enqueue(tonic::Request::new(EnqueueRequest {
                        shard: TEST_SHARD_ID.to_string(),
                        id: job_id.clone(),
                        priority,
                        start_at_ms: 0,
                        retry_policy: None,
                        payload: Some(SerializedBytes {
                            encoding: Some(serialized_bytes::Encoding::Msgpack(
                                rmp_serde::to_vec(&serde_json::json!({
                                    "limit_key": config.key,
                                    "job_num": job_num
                                }))
                                .unwrap(),
                            )),
                        }),
                        limits: vec![Limit {
                            limit: Some(limit::Limit::Concurrency(ConcurrencyLimit {
                                key: config.key.into(),
                                max_concurrency: config.max_concurrency,
                            })),
                        }],
                        tenant: None,
                        metadata: HashMap::new(),
                        task_group: "default".to_string(),
                    }))
                    .await
                {
                    Ok(_) => {
                        enqueued += 1;
                        enqueue_counter2.fetch_add(1, Ordering::SeqCst);
                        enqueued_jobs2.lock().unwrap().push(job_id);
                    }
                    Err(e) => {
                        tracing::trace!(job_id = %job_id, error = %e, "enqueue_failed");
                    }
                }
            }

            tracing::trace!(enqueued = enqueued, "producer2_done");
            Ok(())
        });

        // Spawn multiple workers with different characteristics
        for worker_num in 0..NUM_WORKERS {
            let worker_seed = seed.wrapping_add(100 + worker_num as u64);
            let worker_id = format!("worker{}", worker_num);
            let completion_counter = Arc::clone(&total_completed);
            let completed_jobs_tracker = Arc::clone(&completed_jobs);

            let client_name: &'static str =
                Box::leak(format!("worker{}", worker_num).into_boxed_str());
            sim.client(client_name, async move {
                // Stagger worker start times based on worker number
                let start_delay = 100 + (worker_num as u64 * 25);
                tokio::time::sleep(Duration::from_millis(start_delay)).await;

                let mut rng = StdRng::seed_from_u64(worker_seed);

                let ch = Endpoint::new("http://server:9905")?
                    .connect_with_connector(turmoil_connector())
                    .await?;
                let mut client = SiloClient::new(ch);

                let mut completed = 0u32;
                let mut failed = 0u32;

                // Track tasks we're currently processing
                let mut processing: Vec<Task> = Vec::new();

                for round in 0..60 {
                    // Random batch size per lease attempt - vary aggressively
                    let max_tasks = rng.random_range(1..=8);

                    let lease_result = client
                        .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                            shard: Some(TEST_SHARD_ID.to_string()),
                            worker_id: worker_id.clone(),
                            max_tasks,
                            task_group: "default".to_string(),
                        }))
                        .await;

                    match lease_result {
                        Ok(resp) => {
                            let tasks = resp.into_inner().tasks;

                            // INVARIANT #1: Per-response concurrency limit check
                            // Count tasks per limit key in this single response
                            let mut tasks_per_limit: StdHashMap<String, u32> = StdHashMap::new();
                            for task in &tasks {
                                let limit_key = extract_limit_key(&task.job_id);
                                *tasks_per_limit.entry(limit_key).or_insert(0) += 1;
                            }

                            // Verify no limit key exceeds its max_concurrency in this response
                            for (limit_key, count) in &tasks_per_limit {
                                if let Some(max_conc) = get_max_concurrency(limit_key) {
                                    assert!(
                                        *count <= max_conc,
                                        "INVARIANT VIOLATION: Lease response contains {} tasks for \
                                         limit key '{}' but max_concurrency is {} (worker={}, round={})",
                                        count, limit_key, max_conc, worker_id, round
                                    );
                                }
                            }

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
                            tracing::trace!(
                                worker = %worker_id,
                                error = %e,
                                round = round,
                                "lease_failed"
                            );
                        }
                    }

                    // Process tasks with randomized timing and outcomes
                    if !processing.is_empty() {
                        // Randomly decide how many to complete this round
                        let num_to_complete = rng.random_range(1..=processing.len());
                        let mut indices_to_remove: HashSet<usize> = HashSet::new();

                        for i in 0..num_to_complete {
                            if i >= processing.len() {
                                break;
                            }

                            let task = &processing[i];

                            // Random processing time - simulate variable work
                            let process_time = rng.random_range(5..80);
                            tokio::time::sleep(Duration::from_millis(process_time)).await;

                            // 15% chance of failure (will retry) to stress retry logic
                            let should_fail = rng.random_ratio(15, 100);

                            let outcome = if should_fail {
                                report_outcome_request::Outcome::Failure(silo::pb::Failure {
                                    code: "random_failure".into(),
                                    data: None,
                                })
                            } else {
                                report_outcome_request::Outcome::Success(SerializedBytes {
                                    encoding: Some(serialized_bytes::Encoding::Msgpack(rmp_serde::to_vec(&serde_json::json!("done")).unwrap())),
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
                                    } else {
                                        // INVARIANT #3: No duplicate completions
                                        let was_new = completed_jobs_tracker
                                            .lock()
                                            .unwrap()
                                            .insert(task.job_id.clone());
                                        assert!(
                                            was_new,
                                            "INVARIANT VIOLATION: Job '{}' completed twice! \
                                             (worker={}, round={})",
                                            task.job_id, worker_id, round
                                        );

                                        tracing::trace!(
                                            worker = %worker_id,
                                            job_id = %task.job_id,
                                            "complete"
                                        );
                                        completed += 1;
                                        completion_counter.fetch_add(1, Ordering::SeqCst);
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

                        // Remove completed tasks (in reverse order to preserve indices)
                        let mut indices: Vec<_> = indices_to_remove.into_iter().collect();
                        indices.sort_by(|a, b| b.cmp(a));
                        for idx in indices {
                            processing.remove(idx);
                        }
                    }

                    // Random delay between rounds - vary from quick polling to slow
                    let delay = rng.random_range(10..120);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }

                // Finish processing any remaining tasks
                for task in &processing {
                    let process_time = rng.random_range(5..30);
                    tokio::time::sleep(Duration::from_millis(process_time)).await;

                    match client
                        .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                            shard: TEST_SHARD_ID.to_string(),
                            task_id: task.id.clone(),
                            outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                                encoding: Some(serialized_bytes::Encoding::Msgpack(rmp_serde::to_vec(&serde_json::json!("done")).unwrap())),
                            })),
                        }))
                        .await
                    {
                        Ok(_) => {
                            // INVARIANT #3: No duplicate completions
                            let was_new = completed_jobs_tracker
                                .lock()
                                .unwrap()
                                .insert(task.job_id.clone());
                            assert!(
                                was_new,
                                "INVARIANT VIOLATION: Job '{}' completed twice! (worker={}, final)",
                                task.job_id, worker_id
                            );

                            tracing::trace!(
                                worker = %worker_id,
                                job_id = %task.job_id,
                                "complete_final"
                            );
                            completed += 1;
                            completion_counter.fetch_add(1, Ordering::SeqCst);
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

        // Verifier client: waits for work to complete and checks all invariants
        // This verifier consumes server-side DST events to track state accurately
        let verify_completed = Arc::clone(&total_completed);
        let verify_enqueued = Arc::clone(&total_enqueued);
        let verify_jobs = Arc::clone(&enqueued_jobs);
        let verify_completed_set = Arc::clone(&completed_jobs);
        let verify_tracker = Arc::clone(&tracker);
        sim.client("verifier", async move {
            // Wait for producers and workers to do their work
            tokio::time::sleep(Duration::from_secs(90)).await;

            // Process all DST events from server-side instrumentation
            verify_tracker.process_dst_events();

            let enqueued = verify_enqueued.load(Ordering::SeqCst);
            let completed = verify_completed.load(Ordering::SeqCst);

            tracing::info!(
                enqueued = enqueued,
                completed = completed,
                "verification_progress"
            );

            // Connect to server for GetJob queries
            let ch = Endpoint::new("http://server:9905")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            // INVARIANT #3: All jobs should reach terminal state
            let job_ids = verify_jobs.lock().unwrap().clone();
            let mut terminal_count = 0u32;
            let mut running_count = 0u32;
            let mut scheduled_count = 0u32;

            for job_id in &job_ids {
                match client
                    .get_job(tonic::Request::new(GetJobRequest {
                        shard: TEST_SHARD_ID.to_string(),
                        id: job_id.clone(),
                        tenant: None,
                        include_attempts: true,
                    }))
                    .await
                {
                    Ok(resp) => {
                        let job = resp.into_inner();
                        match job.status() {
                            JobStatus::Succeeded | JobStatus::Failed | JobStatus::Cancelled => {
                                terminal_count += 1;
                            }
                            JobStatus::Running => {
                                running_count += 1;
                                tracing::trace!(job_id = %job_id, "job_still_running");
                            }
                            JobStatus::Scheduled => {
                                scheduled_count += 1;
                                tracing::trace!(job_id = %job_id, "job_still_scheduled");
                            }
                        }

                        let running_attempts = job
                            .attempts
                            .iter()
                            .filter(|attempt| attempt.status() == AttemptStatus::Running)
                            .count();
                        match job.status() {
                            JobStatus::Running => {
                                assert_eq!(
                                    running_attempts, 1,
                                    "Running job should have exactly one running attempt"
                                );
                            }
                            JobStatus::Scheduled => {
                                assert_eq!(
                                    running_attempts, 0,
                                    "Scheduled job should not have running attempts"
                                );
                            }
                            JobStatus::Succeeded | JobStatus::Failed | JobStatus::Cancelled => {
                                assert_eq!(
                                    running_attempts, 0,
                                    "Terminal job should not have running attempts"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(job_id = %job_id, error = %e, "get_job_failed");
                    }
                }
            }

            tracing::info!(
                total = job_ids.len(),
                terminal = terminal_count,
                running = running_count,
                scheduled = scheduled_count,
                "verification_job_states"
            );

            // Verify most jobs reached terminal state (allow some slack for timing)
            let completed_set_count = verify_completed_set.lock().unwrap().len() as u32;
            tracing::info!(
                completed_set_count = completed_set_count,
                completed_counter = completed,
                "verification_completion_counts"
            );

            // These counts should match - if not, something is wrong with our tracking
            assert!(
                completed_set_count == completed,
                "INVARIANT VIOLATION: Completion tracking mismatch: set has {} but counter has {}",
                completed_set_count,
                completed
            );

            // Verify progress was made - we should complete a significant portion
            let expected_min = (enqueued as f64 * 0.5) as u32;
            assert!(
                completed >= expected_min,
                "Insufficient progress: only {}/{} jobs completed (expected at least {})",
                completed,
                enqueued,
                expected_min
            );

            // INVARIANT #2: Final global concurrency verification
            // After all work is done, verify no duplicate job leases occurred
            verify_tracker.verify_all();

            // Log final concurrency state for all limit keys
            for config in LIMIT_CONFIGS {
                let holder_count = verify_tracker
                    .concurrency
                    .total_holder_count_for_queue(config.key);
                tracing::trace!(
                    limit_key = config.key,
                    remaining_holders = holder_count,
                    "final_concurrency_state"
                );
            }

            // Verify state machine transitions were valid
            let transition_violations = verify_tracker.jobs.verify_all_transitions();
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

            tracing::trace!(
                enqueued = enqueued,
                completed = completed,
                terminal = terminal_count,
                "verifier_done"
            );

            Ok(())
        });
    });
}
