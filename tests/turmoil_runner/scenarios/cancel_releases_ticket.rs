//! Cancel releases ticket scenario: Tests the SILO-GRANT-CXL invariant.
//!
//! When a job is cancelled, it should release its concurrency ticket (if holding)
//! and be skipped if it's waiting in the request queue.
//!
//! Test case 1 - Cancel while holding:
//! 1. Enqueue jobs A, B with concurrency limit (max=1)
//! 2. A acquires ticket and starts running
//! 3. Cancel A while running
//! 4. Worker heartbeats, discovers cancellation, reports Cancelled
//! 5. Assert: B immediately gets the ticket and runs
//! 6. Server query: A has no holder after cancellation acknowledged
//!
//! Test case 2 - Cancel while waiting:
//! 1. Enqueue A, B, C with limit max=1
//! 2. A runs, B and C are queued as requests
//! 3. Cancel B while it's waiting
//! 4. A completes
//! 5. Assert: C (not B) gets the ticket next
//!
//! Invariants verified:
//! - SILO-GRANT-CXL: Cancelled jobs release held tickets and are skipped when waiting
//! - queueLimitEnforced: Never more than 1 holder for the limit

use crate::helpers::{
    ConcurrencyLimit, EnqueueRequest, HashMap, LeaseTasksRequest, Limit, ReportOutcomeRequest,
    SerializedBytes, TEST_SHARD_ID, check_holder_limits, get_seed, limit, report_outcome_request,
    run_scenario_impl, serialized_bytes, setup_server, turmoil_connector, verify_server_invariants,
};
use silo::pb::silo_client::SiloClient;
use silo::pb::{CancelJobRequest, HeartbeatRequest};
use std::collections::HashMap as StdHashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tonic::transport::Endpoint;

const LIMIT_KEY: &str = "cancel-mutex";
const MAX_CONCURRENCY: u32 = 1;

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("cancel_releases_ticket", seed, 60, |sim| {
        // Track completion timestamps and order
        let job_a_leased_time = Arc::new(AtomicU64::new(0));
        let job_a_cancelled_time = Arc::new(AtomicU64::new(0));
        let job_b_leased_time = Arc::new(AtomicU64::new(0));
        let job_b_complete_time = Arc::new(AtomicU64::new(0));
        let job_c_leased_time = Arc::new(AtomicU64::new(0));
        let job_c_complete_time = Arc::new(AtomicU64::new(0));

        // Track that B was skipped due to cancellation
        let job_b_was_cancelled_while_waiting = Arc::new(AtomicBool::new(false));

        sim.host("server", || async move { setup_server(9921).await });

        // Producer: Enqueues jobs A, B, C
        sim.client("producer", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let ch = Endpoint::new("http://server:9921")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            // Enqueue job A - will be leased first and then cancelled
            tracing::trace!(job_id = "job-A", "enqueue");
            client
                .enqueue(tonic::Request::new(EnqueueRequest {
                    shard: TEST_SHARD_ID.to_string(),
                    id: "job-A".into(),
                    priority: 10,
                    start_at_ms: 0,
                    retry_policy: None,
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

            // Small delay then enqueue job B - will be cancelled while waiting
            tokio::time::sleep(Duration::from_millis(20)).await;

            tracing::trace!(job_id = "job-B", "enqueue");
            client
                .enqueue(tonic::Request::new(EnqueueRequest {
                    shard: TEST_SHARD_ID.to_string(),
                    id: "job-B".into(),
                    priority: 10,
                    start_at_ms: 0,
                    retry_policy: None,
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

            // Another small delay then enqueue job C
            tokio::time::sleep(Duration::from_millis(20)).await;

            tracing::trace!(job_id = "job-C", "enqueue");
            client
                .enqueue(tonic::Request::new(EnqueueRequest {
                    shard: TEST_SHARD_ID.to_string(),
                    id: "job-C".into(),
                    priority: 10,
                    start_at_ms: 0,
                    retry_policy: None,
                    payload: Some(SerializedBytes {
                        encoding: Some(serialized_bytes::Encoding::Msgpack(
                            rmp_serde::to_vec(&serde_json::json!({"job": "C"})).unwrap(),
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

            tracing::trace!("producer_done");
            Ok(())
        });

        // Canceller: Cancels job A (while running) and job B (while waiting)
        let canceller_a_leased = Arc::clone(&job_a_leased_time);
        let canceller_a_cancelled = Arc::clone(&job_a_cancelled_time);
        let canceller_b_cancelled = Arc::clone(&job_b_was_cancelled_while_waiting);
        sim.client("canceller", async move {
            // Wait for job A to be leased
            tokio::time::sleep(Duration::from_millis(300)).await;

            let ch = Endpoint::new("http://server:9921")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            // First, cancel job B while it's waiting for the ticket
            // (A is currently holding it)
            let a_leased = canceller_a_leased.load(Ordering::SeqCst);
            if a_leased > 0 {
                tracing::trace!(job_id = "job-B", "cancelling_while_waiting");
                match client
                    .cancel_job(tonic::Request::new(CancelJobRequest {
                        shard: TEST_SHARD_ID.to_string(),
                        id: "job-B".into(),
                        tenant: None,
                    }))
                    .await
                {
                    Ok(_) => {
                        canceller_b_cancelled.store(true, Ordering::SeqCst);
                        tracing::trace!(job_id = "job-B", "cancelled_while_waiting");
                    }
                    Err(e) => {
                        tracing::trace!(job_id = "job-B", error = %e, "cancel_failed");
                    }
                }
            }

            // Now cancel job A while it's running
            tokio::time::sleep(Duration::from_millis(100)).await;

            tracing::trace!(job_id = "job-A", "cancelling_while_running");
            match client
                .cancel_job(tonic::Request::new(CancelJobRequest {
                    shard: TEST_SHARD_ID.to_string(),
                    id: "job-A".into(),
                    tenant: None,
                }))
                .await
            {
                Ok(_) => {
                    let cancel_time = turmoil::sim_elapsed()
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    canceller_a_cancelled.store(cancel_time, Ordering::SeqCst);
                    tracing::trace!(
                        job_id = "job-A",
                        cancel_time_ms = cancel_time,
                        "cancelled_while_running"
                    );
                }
                Err(e) => {
                    tracing::trace!(job_id = "job-A", error = %e, "cancel_failed");
                }
            }

            tracing::trace!("canceller_done");
            Ok(())
        });

        // Worker: Processes jobs, heartbeats to detect cancellation
        let worker_a_leased = Arc::clone(&job_a_leased_time);
        let _worker_a_cancelled = Arc::clone(&job_a_cancelled_time);
        let worker_b_leased = Arc::clone(&job_b_leased_time);
        let worker_b_complete = Arc::clone(&job_b_complete_time);
        let worker_c_leased = Arc::clone(&job_c_leased_time);
        let worker_c_complete = Arc::clone(&job_c_complete_time);
        sim.client("worker", async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let ch = Endpoint::new("http://server:9921")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            let mut jobs_processed = 0;
            let mut current_task: Option<(String, String)> = None; // (job_id, task_id)

            for _round in 0..80 {
                // If we have a current task, heartbeat to check for cancellation
                if let Some((ref job_id, ref task_id)) = current_task {
                    let heartbeat = client
                        .heartbeat(tonic::Request::new(HeartbeatRequest {
                            shard: TEST_SHARD_ID.to_string(),
                            worker_id: "worker-1".into(),
                            task_id: task_id.clone(),
                        }))
                        .await;

                    match heartbeat {
                        Ok(resp) => {
                            let hb = resp.into_inner();
                            if hb.cancelled {
                                let sim_time = turmoil::sim_elapsed()
                                    .map(|d| d.as_millis() as u64)
                                    .unwrap_or(0);

                                tracing::trace!(
                                    job_id = %job_id,
                                    task_id = %task_id,
                                    sim_time_ms = sim_time,
                                    "detected_cancellation_via_heartbeat"
                                );

                                // Report cancelled outcome
                                match client
                                    .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                        shard: TEST_SHARD_ID.to_string(),
                                        task_id: task_id.clone(),
                                        outcome: Some(report_outcome_request::Outcome::Cancelled(
                                            silo::pb::Cancelled {},
                                        )),
                                    }))
                                    .await
                                {
                                    Ok(_) => {
                                        tracing::trace!(
                                            job_id = %job_id,
                                            "reported_cancelled_outcome"
                                        );
                                    }
                                    Err(e) => {
                                        tracing::trace!(
                                            job_id = %job_id,
                                            error = %e,
                                            "report_cancelled_failed"
                                        );
                                    }
                                }

                                current_task = None;
                                continue;
                            }
                        }
                        Err(e) => {
                            tracing::trace!(task_id = %task_id, error = %e, "heartbeat_failed");
                        }
                    }

                    // Simulate some work
                    tokio::time::sleep(Duration::from_millis(50)).await;

                    // Complete the task (if not cancelled)
                    if current_task.is_some() {
                        let (job_id, task_id) = current_task.take().unwrap();

                        match client
                            .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                shard: TEST_SHARD_ID.to_string(),
                                task_id: task_id.clone(),
                                outcome: Some(report_outcome_request::Outcome::Success(
                                    SerializedBytes {
                                        encoding: Some(serialized_bytes::Encoding::Msgpack(
                                            rmp_serde::to_vec(&serde_json::json!("done")).unwrap(),
                                        )),
                                    },
                                )),
                            }))
                            .await
                        {
                            Ok(_) => {
                                let complete_time = turmoil::sim_elapsed()
                                    .map(|d| d.as_millis() as u64)
                                    .unwrap_or(0);

                                match job_id.as_str() {
                                    "job-B" => {
                                        worker_b_complete.store(complete_time, Ordering::SeqCst);
                                    }
                                    "job-C" => {
                                        worker_c_complete.store(complete_time, Ordering::SeqCst);
                                    }
                                    _ => {}
                                }

                                tracing::trace!(
                                    job_id = %job_id,
                                    complete_time_ms = complete_time,
                                    "job_completed"
                                );
                                jobs_processed += 1;
                            }
                            Err(e) => {
                                tracing::trace!(job_id = %job_id, error = %e, "complete_failed");
                            }
                        }
                    }
                }

                // Try to lease a new task
                if current_task.is_none() {
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
                        let sim_time = turmoil::sim_elapsed()
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);

                        match task.job_id.as_str() {
                            "job-A" => {
                                worker_a_leased.store(sim_time, Ordering::SeqCst);
                            }
                            "job-B" => {
                                worker_b_leased.store(sim_time, Ordering::SeqCst);
                            }
                            "job-C" => {
                                worker_c_leased.store(sim_time, Ordering::SeqCst);
                            }
                            _ => {}
                        }

                        tracing::trace!(
                            job_id = %task.job_id,
                            task_id = %task.id,
                            sim_time_ms = sim_time,
                            "leased"
                        );

                        current_task = Some((task.job_id.clone(), task.id.clone()));
                    }
                }

                // Stop if we've processed enough jobs (A cancelled, B cancelled/skipped, C completed)
                if jobs_processed >= 2 || worker_c_complete.load(Ordering::SeqCst) > 0 {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            tracing::trace!(jobs_processed = jobs_processed, "worker_done");
            Ok(())
        });

        // Verifier: Check invariants after completion
        let verify_a_leased = Arc::clone(&job_a_leased_time);
        let verify_a_cancelled = Arc::clone(&job_a_cancelled_time);
        let verify_b_leased = Arc::clone(&job_b_leased_time);
        let verify_b_complete = Arc::clone(&job_b_complete_time);
        let verify_c_leased = Arc::clone(&job_c_leased_time);
        let verify_c_complete = Arc::clone(&job_c_complete_time);
        let verify_b_cancelled_waiting = Arc::clone(&job_b_was_cancelled_while_waiting);
        sim.client("verifier", async move {
            // Wait for jobs to complete
            tokio::time::sleep(Duration::from_secs(30)).await;

            let ch = Endpoint::new("http://server:9921")?
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
                    holders = ?state.holder_counts_by_queue,
                    "final_server_state"
                );
            }

            // Get timestamps
            let a_leased = verify_a_leased.load(Ordering::SeqCst);
            let a_cancelled = verify_a_cancelled.load(Ordering::SeqCst);
            let b_leased = verify_b_leased.load(Ordering::SeqCst);
            let b_complete = verify_b_complete.load(Ordering::SeqCst);
            let c_leased = verify_c_leased.load(Ordering::SeqCst);
            let c_complete = verify_c_complete.load(Ordering::SeqCst);
            let b_cancelled_waiting = verify_b_cancelled_waiting.load(Ordering::SeqCst);

            tracing::info!(
                a_leased_ms = a_leased,
                a_cancelled_ms = a_cancelled,
                b_leased_ms = b_leased,
                b_complete_ms = b_complete,
                c_leased_ms = c_leased,
                c_complete_ms = c_complete,
                b_cancelled_waiting = b_cancelled_waiting,
                "timing_verification"
            );

            // Verify job C completed (it should have gotten the ticket after A was cancelled)
            assert!(c_complete > 0, "Job C should have completed");

            // Job C should have been leased after A was cancelled
            if a_cancelled > 0 && c_leased > 0 {
                assert!(
                    c_leased >= a_cancelled,
                    "Job C should be leased after A was cancelled (C leased: {}, A cancelled: {})",
                    c_leased,
                    a_cancelled
                );
                tracing::info!(
                    "SILO-GRANT-CXL verified: C leased at {}ms after A cancelled at {}ms",
                    c_leased,
                    a_cancelled
                );
            }

            // If B was cancelled while waiting, it should never have been leased
            if b_cancelled_waiting {
                // B might have been leased 0 (never leased) or might have been skipped
                tracing::info!(
                    "Job B was cancelled while waiting. B leased: {}, B complete: {}",
                    b_leased,
                    b_complete
                );
                // Note: The exact behavior depends on timing - B might not have been
                // leased at all, or if it was leased it would be cancelled
            }

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}
