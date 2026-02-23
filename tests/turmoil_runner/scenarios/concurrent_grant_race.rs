//! Concurrent grant race scenario: Testing that release_and_grant_next correctly
//! handles concurrent calls without double-granting the same pending request.
//!
//! This scenario is designed to maximize the probability of concurrent
//! release_and_grant_next calls for the same concurrency queue, exposing a race
//! condition where two concurrent calls both scan SlateDB, find the same pending
//! request, and both grant it — resulting in one wasted release and a duplicate
//! ConcurrencyTicketGranted event.
//!
//! Setup:
//! - Single concurrency queue with limit=5
//! - 40 jobs (so 5 running + 35 pending at peak)
//! - 8 workers completing tasks as fast as possible (no processing delay)
//! - Workers lease 1 task at a time to maximize concurrent report_outcome calls
//!
//! The existing noDoubleLease invariant in GlobalConcurrencyTracker.acquire() catches
//! the bug: if two ConcurrencyTicketGranted events fire for the same task_id, the
//! second acquire() panics because the task is already in the holders set.

use crate::helpers::{
    AttemptStatus, ConcurrencyLimit, EnqueueRequest, GetJobRequest, HashMap, InvariantTracker,
    JobStatus, LeaseTasksRequest, Limit, ReportOutcomeRequest, SerializedBytes,
    SiloClient, TEST_SHARD_ID, connect_to_server, get_seed, limit, report_outcome_request,
    run_scenario_impl, serialized_bytes, setup_server, turmoil_connector,
};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::transport::Endpoint;

const LIMIT_KEY: &str = "race-limit";
const MAX_CONCURRENCY: u32 = 5;
const NUM_JOBS: usize = 40;
const NUM_WORKERS: usize = 8;

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("concurrent_grant_race", seed, 120, |sim| {
        let total_completed: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
        let total_enqueued: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
        let enqueued_jobs: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let completed_jobs: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

        let tracker = Arc::new(InvariantTracker::new());
        tracker
            .concurrency
            .register_limit(LIMIT_KEY, MAX_CONCURRENCY);

        sim.host("server", || async move { setup_server(9906).await });

        // Single producer: enqueue all jobs rapidly with the same concurrency queue
        let enqueue_counter = Arc::clone(&total_enqueued);
        let enqueued_jobs_ref = Arc::clone(&enqueued_jobs);
        sim.client("producer", async move {
            let mut client = connect_to_server("http://server:9906").await?;

            for job_num in 0..NUM_JOBS {
                let job_id = format!("{}-{}", LIMIT_KEY, job_num);

                tracing::trace!(job_id = %job_id, "enqueue");

                match client
                    .enqueue(tonic::Request::new(EnqueueRequest {
                        shard: TEST_SHARD_ID.to_string(),
                        id: job_id.clone(),
                        priority: 50,
                        start_at_ms: 0,
                        retry_policy: None,
                        payload: Some(SerializedBytes {
                            encoding: Some(serialized_bytes::Encoding::Msgpack(
                                rmp_serde::to_vec(&serde_json::json!({
                                    "job_num": job_num
                                }))
                                .unwrap(),
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
                    .await
                {
                    Ok(_) => {
                        enqueue_counter.fetch_add(1, Ordering::SeqCst);
                        enqueued_jobs_ref.lock().unwrap().push(job_id);
                    }
                    Err(e) => {
                        tracing::trace!(job_id = %job_id, error = %e, "enqueue_failed");
                    }
                }
            }

            tracing::trace!("producer_done");
            Ok(())
        });

        // Spawn many workers that complete tasks as fast as possible.
        // Each worker leases 1 task at a time and reports success immediately,
        // maximizing the chance of concurrent report_outcome calls hitting the server.
        for worker_num in 0..NUM_WORKERS {
            let worker_id = format!("worker{}", worker_num);
            let completion_counter = Arc::clone(&total_completed);
            let completed_jobs_tracker = Arc::clone(&completed_jobs);

            let client_name: &'static str =
                Box::leak(format!("worker{}", worker_num).into_boxed_str());
            sim.client(client_name, async move {
                // Stagger worker starts so the server has time to be ready
                let start_delay = 200 + (worker_num as u64 * 25);
                tokio::time::sleep(Duration::from_millis(start_delay)).await;

                let mut client = connect_to_server("http://server:9906").await?;

                let mut completed = 0u32;

                // Keep leasing and completing until we've done enough rounds
                for round in 0..80 {
                    // Lease exactly 1 task to maximize concurrency of report_outcome calls
                    let lease_result = client
                        .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                            shard: Some(TEST_SHARD_ID.to_string()),
                            worker_id: worker_id.clone(),
                            max_tasks: 1,
                            task_group: "default".to_string(),
                        }))
                        .await;

                    match lease_result {
                        Ok(resp) => {
                            let tasks = resp.into_inner().tasks;

                            for task in tasks {
                                tracing::trace!(
                                    worker = %worker_id,
                                    job_id = %task.job_id,
                                    task_id = %task.id,
                                    round = round,
                                    "leased"
                                );

                                // Complete immediately — no processing delay.
                                // This maximizes concurrent report_outcome calls.
                                match client
                                    .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                        shard: TEST_SHARD_ID.to_string(),
                                        task_id: task.id.clone(),
                                        outcome: Some(
                                            report_outcome_request::Outcome::Success(
                                                SerializedBytes {
                                                    encoding: Some(
                                                        serialized_bytes::Encoding::Msgpack(
                                                            rmp_serde::to_vec(
                                                                &serde_json::json!("done"),
                                                            )
                                                            .unwrap(),
                                                        ),
                                                    ),
                                                },
                                            ),
                                        ),
                                    }))
                                    .await
                                {
                                    Ok(_) => {
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

                    // Minimal delay between rounds — just enough to yield
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }

                tracing::trace!(
                    worker = %worker_id,
                    completed = completed,
                    "worker_done"
                );

                Ok(())
            });
        }

        // Verifier: waits for work to complete and checks all invariants
        let verify_completed = Arc::clone(&total_completed);
        let verify_enqueued = Arc::clone(&total_enqueued);
        let verify_jobs = Arc::clone(&enqueued_jobs);
        let verify_completed_set = Arc::clone(&completed_jobs);
        let verify_tracker = Arc::clone(&tracker);
        sim.client("verifier", async move {
            // Wait for producers and workers to finish
            tokio::time::sleep(Duration::from_secs(90)).await;

            // Process all confirmed DST events and validate invariants.
            // This is where noDoubleLease violations will be caught: if two
            // ConcurrencyTicketGranted events fire for the same task_id,
            // acquire() panics.
            verify_tracker.process_and_validate();

            let enqueued = verify_enqueued.load(Ordering::SeqCst);
            let completed = verify_completed.load(Ordering::SeqCst);

            tracing::info!(
                enqueued = enqueued,
                completed = completed,
                "verification_progress"
            );

            // Verify all jobs reached terminal state
            let ch = Endpoint::new("http://server:9906")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

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

                        // Verify attempt invariants
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
                            _ => {
                                assert_eq!(
                                    running_attempts, 0,
                                    "Non-running job should not have running attempts"
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

            let completed_set_count = verify_completed_set.lock().unwrap().len() as u32;
            assert!(
                completed_set_count == completed,
                "Completion tracking mismatch: set has {} but counter has {}",
                completed_set_count,
                completed
            );

            // All jobs should complete — no failures in this scenario
            let expected_min = (enqueued as f64 * 0.8) as u32;
            assert!(
                completed >= expected_min,
                "Insufficient progress: only {}/{} jobs completed (expected at least {})",
                completed,
                enqueued,
                expected_min
            );

            // Verify all concurrency invariants
            verify_tracker.verify_all();

            // Verify state machine transitions were valid
            let transition_violations = verify_tracker.jobs.verify_all_transitions();
            if !transition_violations.is_empty() {
                for v in &transition_violations {
                    tracing::warn!(violation = %v, "transition_violation");
                }
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
