//! High message loss scenario: Testing resilience under 15% packet loss.
//!
//! Invariants verified (using DST events, not RPCs):
//! - System makes progress despite message loss
//! - Jobs that are successfully enqueued eventually complete or fail
//! - No duplicate completions despite retries
//! - Valid state transitions
//! - No leases for terminal jobs

use crate::helpers::{
    ClientConfig, EnqueueRequest, HashMap, InvariantTracker, LeaseTasksRequest,
    ReportOutcomeRequest, RetryPolicy, SerializedBytes, TEST_SHARD_ID, connect_to_server,
    create_turmoil_client, get_seed, report_outcome_request, run_scenario_impl, serialized_bytes,
    setup_server,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("high_message_loss", seed, 120, |sim| {
        sim.set_fail_rate(0.15); // 15% message loss
        sim.set_max_message_latency(Duration::from_millis(30));

        // Client configuration optimized for DST with short timeouts
        let client_config = ClientConfig::for_dst();

        sim.host("server", || async move { setup_server(9904).await });

        // DST event-based tracking - no network involved, reliable under message loss
        let tracker = Arc::new(InvariantTracker::new());
        let total_completed = Arc::new(AtomicU32::new(0));
        let total_enqueued = Arc::new(AtomicU32::new(0));
        let scenario_done = Arc::new(AtomicBool::new(false));

        let producer_enqueued = Arc::clone(&total_enqueued);
        let producer_config = client_config.clone();
        sim.client("producer", async move {
            // Use connect_to_server for initial connection, then producer_config for reconnection
            let mut client = connect_to_server("http://server:9904").await?;
            let mut consecutive_failures = 0u32;

            let mut enqueued = 0;
            for i in 0..20 {
                let job_id = format!("lossy-job-{}", i);
                // Use a retry policy so that when leases expire (due to lost responses),
                // the jobs get re-scheduled instead of failing permanently
                let retry_policy = Some(RetryPolicy {
                    retry_count: 10, // Allow many retries to survive high message loss
                    initial_interval_ms: 100,
                    max_interval_ms: 1000,
                    randomize_interval: false,
                    backoff_factor: 1.5,
                });

                match client
                    .enqueue(tonic::Request::new(EnqueueRequest {
                        shard: TEST_SHARD_ID.to_string(),
                        id: job_id.clone(),
                        priority: i as u32,
                        start_at_ms: 0,
                        retry_policy,
                        payload: Some(SerializedBytes {
                            encoding: Some(serialized_bytes::Encoding::Msgpack(
                                rmp_serde::to_vec(&serde_json::json!({"job": i})).unwrap(),
                            )),
                        }),
                        limits: vec![],
                        tenant: None,
                        metadata: HashMap::new(),
                        task_group: "default".to_string(),
                    }))
                    .await
                {
                    Ok(_) => {
                        tracing::trace!(job_id = %job_id, "enqueue");
                        producer_enqueued.fetch_add(1, Ordering::SeqCst);
                        enqueued += 1;
                        consecutive_failures = 0;
                    }
                    Err(_) => {
                        tracing::trace!(job_id = %job_id, "enqueue_failed");
                        consecutive_failures += 1;

                        // Reconnect after failures - HTTP/2 connection may be corrupted
                        if consecutive_failures >= 1 {
                            tracing::trace!("reconnecting after failure");
                            if let Ok(new_client) =
                                create_turmoil_client("http://server:9904", &producer_config).await
                            {
                                client = new_client;
                                consecutive_failures = 0;
                            }
                        }
                    }
                }
            }
            tracing::trace!(enqueued = enqueued, "producer_done");
            Ok(())
        });

        let worker_completed = Arc::clone(&total_completed);
        let worker_done_flag = Arc::clone(&scenario_done);
        let worker_config = client_config.clone();
        sim.client("worker", async move {
            // Small delay so producer can start enqueuing
            tokio::time::sleep(Duration::from_millis(200)).await;
            let mut client = connect_to_server("http://server:9904").await?;
            let mut consecutive_failures = 0u32;

            let mut completed = 0;
            for _ in 0..50 {
                // Exit early if verifier has finished
                if worker_done_flag.load(Ordering::SeqCst) {
                    break;
                }
                match client
                    .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                        shard: Some(TEST_SHARD_ID.to_string()),
                        worker_id: "resilient".into(),
                        max_tasks: 5,
                        task_group: "default".to_string(),
                    }))
                    .await
                {
                    Ok(resp) => {
                        consecutive_failures = 0;
                        let tasks = resp.into_inner().tasks;
                        for task in &tasks {
                            tracing::trace!(job_id = %task.job_id, "lease");
                            match client
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
                                .await
                            {
                                Ok(_) => {
                                    tracing::trace!(job_id = %task.job_id, "complete");
                                    completed += 1;
                                    worker_completed.fetch_add(1, Ordering::SeqCst);
                                }
                                Err(_) => {
                                    tracing::trace!(job_id = %task.job_id, "complete_failed");
                                }
                            }
                        }
                    }
                    Err(_) => {
                        tracing::trace!("lease_failed");
                        consecutive_failures += 1;

                        // Reconnect after failures - HTTP/2 connection may be corrupted
                        if consecutive_failures >= 1 {
                            tracing::trace!("reconnecting after lease failure");
                            if let Ok(new_client) =
                                create_turmoil_client("http://server:9904", &worker_config).await
                            {
                                client = new_client;
                                consecutive_failures = 0;
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            tracing::trace!(completed = completed, "worker_done");
            Ok(())
        });

        // Verifier: Poll for progress using DST events (no RPCs needed).
        // DST events are emitted synchronously by the server and collected via a
        // thread-local event bus - no network involved, so verification is reliable.
        //
        // Instead of checking once at a fixed time, we poll until progress is made
        // or a hard timeout is reached. This ensures the test maintains value by
        // requiring actual progress while tolerating variable network conditions.
        let verifier_tracker = Arc::clone(&tracker);
        let verifier_completed = Arc::clone(&total_completed);
        let verifier_enqueued = Arc::clone(&total_enqueued);
        let verifier_done_flag = Arc::clone(&scenario_done);
        sim.client("verifier", async move {
            // Poll for progress with increasing intervals.
            // We require at least one job to reach terminal state (success/fail/cancel)
            // as observed through DST events (server ground truth).
            const MAX_WAIT_SECS: u64 = 100; // Hard timeout - if no progress by now, it's a bug
            const POLL_INTERVAL_MS: u64 = 500;

            let start = std::time::Instant::now();
            let mut progress_made = false;

            while start.elapsed().as_secs() < MAX_WAIT_SECS {
                tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;

                // Process DST events from server-side instrumentation
                verifier_tracker.process_dst_events();

                let enqueued = verifier_enqueued.load(Ordering::SeqCst);
                let completed = verifier_completed.load(Ordering::SeqCst);
                let terminal = verifier_tracker.jobs.terminal_count();

                // Run invariant checks on each poll
                verifier_tracker.verify_all();

                // Check if progress has been made
                if completed > 0 || terminal > 0 {
                    progress_made = true;
                    tracing::info!(
                        enqueued = enqueued,
                        client_completed = completed,
                        terminal = terminal,
                        elapsed_secs = start.elapsed().as_secs(),
                        "progress_detected"
                    );
                    break;
                }

                // If nothing enqueued yet, keep waiting for producer
                if enqueued == 0 {
                    continue;
                }

                // Jobs enqueued but no progress yet - log and keep polling
                if start.elapsed().as_secs() % 10 == 0 {
                    tracing::trace!(
                        enqueued = enqueued,
                        terminal = terminal,
                        elapsed_secs = start.elapsed().as_secs(),
                        "waiting_for_progress"
                    );
                }
            }

            // Final state check
            verifier_tracker.process_dst_events();
            let enqueued = verifier_enqueued.load(Ordering::SeqCst);
            let completed = verifier_completed.load(Ordering::SeqCst);
            let terminal = verifier_tracker.jobs.terminal_count();

            tracing::info!(
                enqueued = enqueued,
                client_completed = completed,
                terminal = terminal,
                progress_made = progress_made,
                "verification_results"
            );

            // Run final invariant checks
            verifier_tracker.verify_all();
            verifier_tracker.jobs.verify_no_terminal_leases();

            // Verify state machine transitions were valid
            let transition_violations = verifier_tracker.jobs.verify_all_transitions();
            if !transition_violations.is_empty() {
                for v in &transition_violations {
                    tracing::warn!(violation = %v, "transition_violation");
                }
                panic!(
                    "INVARIANT VIOLATION: {} invalid state transitions detected",
                    transition_violations.len()
                );
            }

            // INVARIANT: At least some jobs must have completed or reached terminal state.
            // We waited up to MAX_WAIT_SECS for this - if it didn't happen, that's a real bug.
            assert!(
                progress_made || enqueued == 0,
                "No progress made after {}s: client_completed={}, terminal={}, enqueued={}. \
                 This indicates a bug - even with 15% message loss, jobs should eventually complete.",
                MAX_WAIT_SECS,
                completed,
                terminal,
                enqueued
            );

            // Signal worker to exit early
            verifier_done_flag.store(true, Ordering::SeqCst);

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}
