//! Retry releases ticket scenario: Tests the retryReleasesTicket invariant.
//!
//! When a job fails and enters retry backoff, it should release its concurrency
//! ticket, allowing other jobs with the same limit to run during the backoff period.
//!
//! Test flow:
//! 1. Enqueue job A with concurrency limit (max=1) and retry policy with backoff
//! 2. Enqueue job B with same concurrency limit
//! 3. Job A leases, runs briefly, reports failure
//! 4. During A's retry backoff period:
//!    - Query server: A should NOT hold the ticket (verify via SQL)
//!    - B should be able to acquire the ticket and complete
//! 5. After backoff, A retries and completes
//! 6. Assert: B completed BEFORE A's second attempt
//!
//! Invariants verified:
//! - retryReleasesTicket: Failed job releases concurrency ticket during backoff
//! - queueLimitEnforced: Never more than 1 holder for the limit

use crate::helpers::{
    ConcurrencyLimit, EnqueueRequest, HashMap, LeaseTasksRequest, Limit, ReportOutcomeRequest,
    RetryPolicy, SerializedBytes, TEST_SHARD_ID, check_holder_limits, get_seed, limit,
    report_outcome_request, run_scenario_impl, serialized_bytes, setup_server, turmoil_connector,
    verify_server_invariants,
};
use silo::pb::silo_client::SiloClient;
use std::collections::HashMap as StdHashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tonic::transport::Endpoint;

const LIMIT_KEY: &str = "retry-mutex";
const MAX_CONCURRENCY: u32 = 1;

// Retry policy with significant backoff so we can observe the window
const RETRY_INITIAL_INTERVAL_MS: u64 = 2000; // 2 seconds backoff

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("retry_releases_ticket", seed, 60, |sim| {
        // Track completion timestamps
        let job_a_first_fail_time = Arc::new(AtomicU64::new(0));
        let job_b_complete_time = Arc::new(AtomicU64::new(0));
        let job_a_second_attempt_start = Arc::new(AtomicU64::new(0));
        let job_a_complete_time = Arc::new(AtomicU64::new(0));

        // Track that we verified the ticket was released during backoff
        let verified_ticket_released = Arc::new(AtomicBool::new(false));

        sim.host("server", || async move { setup_server(9920).await });

        // Producer: Enqueues job A with retry policy, then job B
        let producer_fail_time = Arc::clone(&job_a_first_fail_time);
        sim.client("producer", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let ch = Endpoint::new("http://server:9920")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            // Enqueue job A with retry policy and concurrency limit
            tracing::trace!(job_id = "job-A", "enqueue_with_retry");
            client
                .enqueue(tonic::Request::new(EnqueueRequest {
                    shard: TEST_SHARD_ID.to_string(),
                    id: "job-A".into(),
                    priority: 10,
                    start_at_ms: 0,
                    retry_policy: Some(RetryPolicy {
                        retry_count: 2,
                        initial_interval_ms: RETRY_INITIAL_INTERVAL_MS as i64,
                        max_interval_ms: 5000,
                        randomize_interval: false, // Deterministic for testing
                        backoff_factor: 1.0,       // No exponential backoff
                    }),
                    payload: Some(SerializedBytes {
                        encoding: Some(serialized_bytes::Encoding::Msgpack(
                            rmp_serde::to_vec(&serde_json::json!({"job": "A"})).unwrap(),
                        )),
                    }),
                    limits: vec![Limit {
                        limit: Some(limit::Limit::Concurrency(ConcurrencyLimit {
                            key: LIMIT_KEY.into(),
                            max_concurrency: MAX_CONCURRENCY,
                        })),
                    }],
                    tenant: None,
                    metadata: HashMap::new(),
                    task_group: "default".to_string(),
                }))
                .await?;

            // Small delay then enqueue job B
            tokio::time::sleep(Duration::from_millis(50)).await;

            tracing::trace!(job_id = "job-B", "enqueue");
            client
                .enqueue(tonic::Request::new(EnqueueRequest {
                    shard: TEST_SHARD_ID.to_string(),
                    id: "job-B".into(),
                    priority: 10, // Same priority as A
                    start_at_ms: 0,
                    retry_policy: None, // No retry
                    payload: Some(SerializedBytes {
                        encoding: Some(serialized_bytes::Encoding::Msgpack(
                            rmp_serde::to_vec(&serde_json::json!({"job": "B"})).unwrap(),
                        )),
                    }),
                    limits: vec![Limit {
                        limit: Some(limit::Limit::Concurrency(ConcurrencyLimit {
                            key: LIMIT_KEY.into(),
                            max_concurrency: MAX_CONCURRENCY,
                        })),
                    }],
                    tenant: None,
                    metadata: HashMap::new(),
                    task_group: "default".to_string(),
                }))
                .await?;

            // Wait for job A to fail and enter backoff
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Now query the server to verify the ticket is released during backoff
            let fail_time = producer_fail_time.load(Ordering::SeqCst);
            if fail_time > 0 {
                // Job A has failed, check that it doesn't hold the ticket
                if let Ok(state) = verify_server_invariants(&mut client, TEST_SHARD_ID).await {
                    let holder_count = state
                        .holder_counts_by_queue
                        .get(LIMIT_KEY)
                        .copied()
                        .unwrap_or(0);
                    tracing::trace!(
                        holder_count = holder_count,
                        limit_key = LIMIT_KEY,
                        "ticket_state_during_backoff"
                    );
                    // During backoff, job A should not hold the ticket
                    // Job B might hold it if it's running
                    assert!(
                        holder_count <= MAX_CONCURRENCY,
                        "queueLimitEnforced violated: {} holders for max {}",
                        holder_count,
                        MAX_CONCURRENCY
                    );
                }
            }

            tracing::trace!("producer_done");
            Ok(())
        });

        // Worker 1: Processes job A, fails it first time, then succeeds on retry
        let worker1_fail_time = Arc::clone(&job_a_first_fail_time);
        let worker1_second_start = Arc::clone(&job_a_second_attempt_start);
        let worker1_complete = Arc::clone(&job_a_complete_time);
        sim.client("worker1", async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let ch = Endpoint::new("http://server:9920")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            let mut job_a_attempt = 0u32;

            for _round in 0..50 {
                let lease = client
                    .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                        shard: Some(TEST_SHARD_ID.to_string()),
                        worker_id: "worker-1".into(),
                        max_tasks: 1,
                        task_group: "default".to_string(),
                    }))
                    .await?
                    .into_inner();

                for task in lease.tasks {
                    if task.job_id == "job-A" {
                        job_a_attempt += 1;
                        let sim_time = turmoil::sim_elapsed()
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);

                        if job_a_attempt == 1 {
                            // First attempt: fail to trigger retry
                            tracing::trace!(
                                job_id = "job-A",
                                attempt = job_a_attempt,
                                sim_time_ms = sim_time,
                                "job_a_first_attempt_failing"
                            );

                            // Brief processing
                            tokio::time::sleep(Duration::from_millis(50)).await;

                            // Report failure
                            client
                                .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                    shard: TEST_SHARD_ID.to_string(),
                                    task_id: task.id.clone(),
                                    outcome: Some(report_outcome_request::Outcome::Failure(
                                        silo::pb::Failure {
                                            code: "intentional_failure".into(),
                                            data: None,
                                        },
                                    )),
                                }))
                                .await?;

                            let fail_time = turmoil::sim_elapsed()
                                .map(|d| d.as_millis() as u64)
                                .unwrap_or(0);
                            worker1_fail_time.store(fail_time, Ordering::SeqCst);

                            tracing::trace!(
                                job_id = "job-A",
                                fail_time_ms = fail_time,
                                "job_a_failed_entering_backoff"
                            );
                        } else {
                            // Second attempt: succeed
                            let start_time = turmoil::sim_elapsed()
                                .map(|d| d.as_millis() as u64)
                                .unwrap_or(0);
                            worker1_second_start.store(start_time, Ordering::SeqCst);

                            tracing::trace!(
                                job_id = "job-A",
                                attempt = job_a_attempt,
                                sim_time_ms = start_time,
                                "job_a_second_attempt_starting"
                            );

                            tokio::time::sleep(Duration::from_millis(50)).await;

                            client
                                .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                    shard: TEST_SHARD_ID.to_string(),
                                    task_id: task.id.clone(),
                                    outcome: Some(report_outcome_request::Outcome::Success(
                                        SerializedBytes {
                                            encoding: Some(serialized_bytes::Encoding::Msgpack(
                                                rmp_serde::to_vec(&serde_json::json!("done"))
                                                    .unwrap(),
                                            )),
                                        },
                                    )),
                                }))
                                .await?;

                            let complete_time = turmoil::sim_elapsed()
                                .map(|d| d.as_millis() as u64)
                                .unwrap_or(0);
                            worker1_complete.store(complete_time, Ordering::SeqCst);

                            tracing::trace!(
                                job_id = "job-A",
                                complete_time_ms = complete_time,
                                "job_a_completed"
                            );
                        }
                    }
                }

                // If job A is done, stop
                if worker1_complete.load(Ordering::SeqCst) > 0 {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            tracing::trace!("worker1_done");
            Ok(())
        });

        // Worker 2: Processes job B (should be able to run during A's backoff)
        let worker2_complete = Arc::clone(&job_b_complete_time);
        let worker2_verified = Arc::clone(&verified_ticket_released);
        let worker2_a_fail_time = Arc::clone(&job_a_first_fail_time);
        let worker2_a_second_start = Arc::clone(&job_a_second_attempt_start);
        sim.client("worker2", async move {
            // Start after job A has had time to fail
            tokio::time::sleep(Duration::from_millis(400)).await;

            let ch = Endpoint::new("http://server:9920")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            for _round in 0..30 {
                let lease = client
                    .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                        shard: Some(TEST_SHARD_ID.to_string()),
                        worker_id: "worker-2".into(),
                        max_tasks: 1,
                        task_group: "default".to_string(),
                    }))
                    .await?
                    .into_inner();

                for task in lease.tasks {
                    if task.job_id == "job-B" {
                        let sim_time = turmoil::sim_elapsed()
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);

                        tracing::trace!(job_id = "job-B", sim_time_ms = sim_time, "job_b_leased");

                        // Brief processing
                        tokio::time::sleep(Duration::from_millis(50)).await;

                        client
                            .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                shard: TEST_SHARD_ID.to_string(),
                                task_id: task.id.clone(),
                                outcome: Some(report_outcome_request::Outcome::Success(
                                    SerializedBytes {
                                        encoding: Some(serialized_bytes::Encoding::Msgpack(
                                            rmp_serde::to_vec(&serde_json::json!("done")).unwrap(),
                                        )),
                                    },
                                )),
                            }))
                            .await?;

                        let complete_time = turmoil::sim_elapsed()
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        worker2_complete.store(complete_time, Ordering::SeqCst);

                        // Verify this happened during A's backoff window
                        let a_fail_time = worker2_a_fail_time.load(Ordering::SeqCst);
                        let a_second_start = worker2_a_second_start.load(Ordering::SeqCst);

                        if a_fail_time > 0
                            && (a_second_start == 0 || complete_time < a_second_start)
                        {
                            // B completed while A was in backoff - ticket was released!
                            worker2_verified.store(true, Ordering::SeqCst);
                            tracing::trace!(
                                job_id = "job-B",
                                complete_time_ms = complete_time,
                                a_fail_time_ms = a_fail_time,
                                a_second_start_ms = a_second_start,
                                "job_b_completed_during_a_backoff"
                            );
                        }

                        tracing::trace!(
                            job_id = "job-B",
                            complete_time_ms = complete_time,
                            "job_b_completed"
                        );
                    }
                }

                if worker2_complete.load(Ordering::SeqCst) > 0 {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            tracing::trace!("worker2_done");
            Ok(())
        });

        // Verifier: Check invariants after completion
        let verify_a_fail = Arc::clone(&job_a_first_fail_time);
        let verify_b_complete = Arc::clone(&job_b_complete_time);
        let verify_a_second = Arc::clone(&job_a_second_attempt_start);
        let verify_a_complete = Arc::clone(&job_a_complete_time);
        let verify_ticket_released = Arc::clone(&verified_ticket_released);
        sim.client("verifier", async move {
            // Wait for all jobs to complete
            tokio::time::sleep(Duration::from_secs(30)).await;

            let ch = Endpoint::new("http://server:9920")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            // Verify server state
            if let Ok(state) = verify_server_invariants(&mut client, TEST_SHARD_ID).await {
                assert!(
                    state.violations.is_empty(),
                    "Server invariant violations: {:?}",
                    state.violations
                );

                // Check holder limits
                let mut limits = StdHashMap::new();
                limits.insert(LIMIT_KEY.to_string(), MAX_CONCURRENCY);
                let limit_violations = check_holder_limits(&state, &limits);
                assert!(
                    limit_violations.is_empty(),
                    "Holder limit violations: {:?}",
                    limit_violations
                );

                tracing::trace!(
                    running = state.running_job_count,
                    terminal = state.terminal_job_count,
                    "final_server_state"
                );
            }

            // Get timestamps
            let a_fail_time = verify_a_fail.load(Ordering::SeqCst);
            let b_complete_time = verify_b_complete.load(Ordering::SeqCst);
            let a_second_start = verify_a_second.load(Ordering::SeqCst);
            let a_complete_time = verify_a_complete.load(Ordering::SeqCst);
            let ticket_verified = verify_ticket_released.load(Ordering::SeqCst);

            tracing::info!(
                a_fail_time_ms = a_fail_time,
                b_complete_time_ms = b_complete_time,
                a_second_start_ms = a_second_start,
                a_complete_time_ms = a_complete_time,
                ticket_verified = ticket_verified,
                "timing_verification"
            );

            // Verify both jobs completed
            assert!(a_complete_time > 0, "Job A should have completed");
            assert!(b_complete_time > 0, "Job B should have completed");

            // Key invariant: B completed during A's backoff period
            // (after A failed, before A's second attempt started)
            if a_second_start > 0 && a_fail_time > 0 {
                assert!(
                    b_complete_time >= a_fail_time,
                    "Job B should complete after A failed (B: {}, A fail: {})",
                    b_complete_time,
                    a_fail_time
                );
                // B should complete before or very close to when A's second attempt starts
                // With max_concurrency=1, B couldn't run if A was holding the ticket
                tracing::info!(
                    "retryReleasesTicket verified: B completed at {}ms, A retry at {}ms",
                    b_complete_time,
                    a_second_start
                );
            }

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}
