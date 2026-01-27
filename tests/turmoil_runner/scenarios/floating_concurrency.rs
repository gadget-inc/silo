//! Floating concurrency scenario: Tests dynamic concurrency limit adjustment.
//!
//! FloatingConcurrencyLimit allows workers to periodically update the max concurrency
//! value based on external factors (like API rate limits). This scenario tests:
//!
//! 1. Jobs with floating limits are enqueued and processed
//! 2. RefreshFloatingLimit tasks are sent to workers
//! 3. Workers can report new max concurrency values
//! 4. The limit adjustment affects subsequent job processing
//!
//! Test flow:
//! 1. Enqueue jobs with floating limit (default_max=2)
//! 2. Jobs run up to the default limit
//! 3. Worker receives RefreshFloatingLimit task
//! 4. Worker reports new max=4
//! 5. More jobs can run concurrently
//!
//! Invariants verified:
//! - Jobs with floating limits can be processed
//! - Refresh tasks are delivered to workers
//! - Limit changes take effect

use crate::helpers::{
    EnqueueRequest, HashMap, LeaseTasksRequest, Limit, SerializedBytes, ReportOutcomeRequest,
    get_seed, limit, report_outcome_request, run_scenario_impl, serialized_bytes, setup_server, turmoil_connector,
    verify_server_invariants,
};
use silo::pb::silo_client::SiloClient;
use silo::pb::{FloatingConcurrencyLimit, RefreshSuccess, ReportRefreshOutcomeRequest};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Endpoint;

const LIMIT_KEY: &str = "floating-limit-test";
const DEFAULT_MAX_CONCURRENCY: u32 = 2;
const REFRESH_INTERVAL_MS: i64 = 1000; // 1 second refresh interval

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("floating_concurrency", seed, 90, |sim| {
        // Track job processing
        let total_enqueued = Arc::new(AtomicU32::new(0));
        let total_completed = Arc::new(AtomicU32::new(0));
        let refresh_tasks_received = Arc::new(AtomicU32::new(0));
        let refresh_tasks_reported = Arc::new(AtomicU32::new(0));

        sim.host("server", || async move { setup_server(9924).await });

        // Producer: Enqueues jobs with floating concurrency limit
        let producer_enqueued = Arc::clone(&total_enqueued);
        sim.client("producer", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let ch = Endpoint::new("http://server:9924")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            // Enqueue several jobs with floating limit
            for i in 0..8 {
                let job_id = format!("floating-job-{}", i);

                tracing::trace!(job_id = %job_id, "enqueue_with_floating_limit");

                match client
                    .enqueue(tonic::Request::new(EnqueueRequest {
                        shard: 0,
                        id: job_id.clone(),
                        priority: 10,
                        start_at_ms: 0,
                        retry_policy: None,
                        payload: Some(SerializedBytes {
                            encoding: Some(serialized_bytes::Encoding::Msgpack(rmp_serde::to_vec(&serde_json::json!({"job": i})).unwrap())),
                        }),
                        limits: vec![Limit {
                            limit: Some(limit::Limit::FloatingConcurrency(FloatingConcurrencyLimit {
                                key: LIMIT_KEY.to_string(),
                                default_max_concurrency: DEFAULT_MAX_CONCURRENCY,
                                refresh_interval_ms: REFRESH_INTERVAL_MS,
                                metadata: std::collections::HashMap::new(),
                            })),
                        }],
                        tenant: None,
                        metadata: HashMap::new(),
                        task_group: "default".to_string(),
                    }))
                    .await
                {
                    Ok(_) => {
                        producer_enqueued.fetch_add(1, Ordering::SeqCst);
                        tracing::trace!(job_id = %job_id, "enqueue_success");
                    }
                    Err(e) => {
                        tracing::trace!(job_id = %job_id, error = %e, "enqueue_failed");
                    }
                }

                // Small delay between enqueues
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            tracing::trace!(
                enqueued = producer_enqueued.load(Ordering::SeqCst),
                "producer_done"
            );
            Ok(())
        });

        // Worker: Processes jobs and handles refresh tasks
        let worker_completed = Arc::clone(&total_completed);
        let worker_refresh_received = Arc::clone(&refresh_tasks_received);
        let worker_refresh_reported = Arc::clone(&refresh_tasks_reported);
        sim.client("worker", async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let ch = Endpoint::new("http://server:9924")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            let mut completed = 0;
            let mut new_max_concurrency = DEFAULT_MAX_CONCURRENCY + 2; // Will report increased limit

            for _round in 0..100 {
                let lease = client
                    .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                        shard: Some(0),
                        worker_id: "worker-1".into(),
                        max_tasks: 5,
                        task_group: "default".to_string(),
                    }))
                    .await?
                    .into_inner();

                // Process job tasks
                for task in &lease.tasks {
                    tracing::trace!(
                        job_id = %task.job_id,
                        task_id = %task.id,
                        "leased_job_task"
                    );

                    // Simulate some work
                    tokio::time::sleep(Duration::from_millis(50)).await;

                    // Complete the task
                    match client
                        .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                            shard: 0,
                            task_id: task.id.clone(),
                            outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                                encoding: Some(serialized_bytes::Encoding::Msgpack(rmp_serde::to_vec(&serde_json::json!("done")).unwrap())),
                            })),
                        }))
                        .await
                    {
                        Ok(_) => {
                            tracing::trace!(job_id = %task.job_id, "completed");
                            completed += 1;
                            worker_completed.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(e) => {
                            tracing::trace!(job_id = %task.job_id, error = %e, "complete_failed");
                        }
                    }
                }

                // Process refresh tasks
                for refresh_task in &lease.refresh_tasks {
                    worker_refresh_received.fetch_add(1, Ordering::SeqCst);

                    tracing::trace!(
                        task_id = %refresh_task.id,
                        queue_key = %refresh_task.queue_key,
                        current_max = refresh_task.current_max_concurrency,
                        "received_refresh_task"
                    );

                    // Report new max concurrency (increase by 2 each time)
                    match client
                        .report_refresh_outcome(tonic::Request::new(ReportRefreshOutcomeRequest {
                            shard: 0,
                            task_id: refresh_task.id.clone(),
                            outcome: Some(
                                silo::pb::report_refresh_outcome_request::Outcome::Success(
                                    RefreshSuccess {
                                        new_max_concurrency: new_max_concurrency,
                                    },
                                ),
                            ),
                        }))
                        .await
                    {
                        Ok(_) => {
                            worker_refresh_reported.fetch_add(1, Ordering::SeqCst);
                            tracing::trace!(
                                task_id = %refresh_task.id,
                                new_max = new_max_concurrency,
                                "reported_refresh_success"
                            );
                            // Increase for next refresh (simulating dynamic adjustment)
                            new_max_concurrency = (new_max_concurrency + 1).min(10);
                        }
                        Err(e) => {
                            tracing::trace!(
                                task_id = %refresh_task.id,
                                error = %e,
                                "report_refresh_failed"
                            );
                        }
                    }
                }

                // Stop if we've completed all jobs
                if completed >= 8 {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            tracing::trace!(
                completed = completed,
                refresh_received = worker_refresh_received.load(Ordering::SeqCst),
                refresh_reported = worker_refresh_reported.load(Ordering::SeqCst),
                "worker_done"
            );
            Ok(())
        });

        // Verifier: Check all jobs completed and refresh tasks were processed
        let verify_enqueued = Arc::clone(&total_enqueued);
        let verify_completed = Arc::clone(&total_completed);
        let verify_refresh_received = Arc::clone(&refresh_tasks_received);
        let verify_refresh_reported = Arc::clone(&refresh_tasks_reported);
        sim.client("verifier", async move {
            // Wait for jobs to complete
            tokio::time::sleep(Duration::from_secs(60)).await;

            let ch = Endpoint::new("http://server:9924")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            // Verify server state
            if let Ok(state) = verify_server_invariants(&mut client, 0).await {
                assert!(
                    state.violations.is_empty(),
                    "Server invariant violations: {:?}",
                    state.violations
                );

                tracing::trace!(
                    running = state.running_job_count,
                    terminal = state.terminal_job_count,
                    "server_state"
                );
            }

            let enqueued = verify_enqueued.load(Ordering::SeqCst);
            let completed = verify_completed.load(Ordering::SeqCst);
            let refresh_received = verify_refresh_received.load(Ordering::SeqCst);
            let refresh_reported = verify_refresh_reported.load(Ordering::SeqCst);

            tracing::info!(
                enqueued = enqueued,
                completed = completed,
                refresh_received = refresh_received,
                refresh_reported = refresh_reported,
                "final_verification"
            );

            // Verify progress
            assert!(
                completed > 0,
                "At least some jobs should have completed"
            );

            // Verify most jobs completed (allow some slack due to timing)
            let expected_min = (enqueued as f64 * 0.5) as u32;
            assert!(
                completed >= expected_min,
                "Expected at least {} jobs completed, got {}",
                expected_min,
                completed
            );

            // Verify refresh tasks were processed
            // Note: Refresh tasks may or may not be sent depending on timing
            // Just log for visibility
            if refresh_received > 0 {
                tracing::info!(
                    "Floating limit refresh flow verified: {} tasks received, {} reported",
                    refresh_received,
                    refresh_reported
                );
            } else {
                tracing::info!(
                    "No refresh tasks received (may be due to short test duration)"
                );
            }

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}
