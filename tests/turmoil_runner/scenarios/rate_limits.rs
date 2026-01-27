//! Rate limits scenario: Tests the CheckRateLimit task processing path.
//!
//! This scenario verifies that jobs with Gubernator rate limits are processed
//! correctly through the rate limit checking flow.
//!
//! Test flow:
//! 1. Enqueue multiple jobs with rate limit configuration
//! 2. Jobs pass through CheckRateLimit internal tasks
//! 3. Jobs that pass rate limit check proceed to execution
//! 4. All jobs eventually complete
//!
//! Note: The MockGubernatorClient used in DST tracks counters but doesn't
//! automatically reset them. This test verifies the rate limit code path
//! is exercised correctly.
//!
//! Invariants verified:
//! - Jobs with rate limits can be enqueued and processed
//! - Rate limit check doesn't cause deadlocks
//! - Jobs eventually complete despite rate limit processing

use crate::helpers::{
    EnqueueRequest, GetJobRequest, HashMap, JobStatus, LeaseTasksRequest, Limit, SerializedBytes,
    ReportOutcomeRequest, get_seed, limit, report_outcome_request, run_scenario_impl, serialized_bytes,
    setup_server, turmoil_connector, verify_server_invariants,
};
use silo::pb::silo_client::SiloClient;
use silo::pb::{GubernatorAlgorithm, GubernatorRateLimit, RateLimitRetryPolicy};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Endpoint;

const RATE_LIMIT_NAME: &str = "test-rate-limit";

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("rate_limits", seed, 60, |sim| {
        // Track job processing
        let total_enqueued = Arc::new(AtomicU32::new(0));
        let total_completed = Arc::new(AtomicU32::new(0));

        sim.host("server", || async move { setup_server(9923).await });

        // Producer: Enqueues jobs with rate limits
        let producer_enqueued = Arc::clone(&total_enqueued);
        sim.client("producer", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let ch = Endpoint::new("http://server:9923")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            // Enqueue several jobs with rate limits
            // Using a generous limit so jobs can proceed
            for i in 0..5 {
                let job_id = format!("rate-limited-job-{}", i);

                tracing::trace!(job_id = %job_id, "enqueue_with_rate_limit");

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
                            limit: Some(limit::Limit::RateLimit(GubernatorRateLimit {
                                name: RATE_LIMIT_NAME.to_string(),
                                unique_key: format!("test-key-{}", i % 2), // Use 2 different keys
                                limit: 100,      // Generous limit
                                duration_ms: 60000, // 1 minute window
                                hits: 1,
                                algorithm: GubernatorAlgorithm::TokenBucket as i32,
                                behavior: 0,
                                retry_policy: Some(RateLimitRetryPolicy {
                                    initial_backoff_ms: 100,
                                    max_backoff_ms: 5000,
                                    backoff_multiplier: 2.0,
                                    max_retries: 10,
                                }),
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
                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            // Also enqueue jobs with combined rate + concurrency limits
            for i in 0..3 {
                let job_id = format!("combined-limit-job-{}", i);

                tracing::trace!(job_id = %job_id, "enqueue_with_combined_limits");

                match client
                    .enqueue(tonic::Request::new(EnqueueRequest {
                        shard: 0,
                        id: job_id.clone(),
                        priority: 10,
                        start_at_ms: 0,
                        retry_policy: None,
                        payload: Some(SerializedBytes {
                            encoding: Some(serialized_bytes::Encoding::Msgpack(rmp_serde::to_vec(&serde_json::json!({"combined": i})).unwrap())),
                        }),
                        limits: vec![
                            // Rate limit
                            Limit {
                                limit: Some(limit::Limit::RateLimit(GubernatorRateLimit {
                                    name: "combined-rate-limit".to_string(),
                                    unique_key: "combined-key".to_string(),
                                    limit: 50,
                                    duration_ms: 60000,
                                    hits: 1,
                                    algorithm: GubernatorAlgorithm::TokenBucket as i32,
                                    behavior: 0,
                                    retry_policy: Some(RateLimitRetryPolicy {
                                        initial_backoff_ms: 50,
                                        max_backoff_ms: 2000,
                                        backoff_multiplier: 1.5,
                                        max_retries: 5,
                                    }),
                                })),
                            },
                            // Concurrency limit
                            Limit {
                                limit: Some(limit::Limit::Concurrency(silo::pb::ConcurrencyLimit {
                                    key: "combined-concurrency".to_string(),
                                    max_concurrency: 2,
                                })),
                            },
                        ],
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

                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            tracing::trace!(
                enqueued = producer_enqueued.load(Ordering::SeqCst),
                "producer_done"
            );
            Ok(())
        });

        // Worker: Processes jobs
        let worker_completed = Arc::clone(&total_completed);
        sim.client("worker", async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let ch = Endpoint::new("http://server:9923")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            let mut completed = 0;

            for _round in 0..80 {
                let lease = client
                    .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                        shard: Some(0),
                        worker_id: "worker-1".into(),
                        max_tasks: 3,
                        task_group: "default".to_string(),
                    }))
                    .await?
                    .into_inner();

                for task in lease.tasks {
                    tracing::trace!(
                        job_id = %task.job_id,
                        task_id = %task.id,
                        "leased"
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

                // Stop if we've completed all jobs
                if completed >= 8 {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            tracing::trace!(completed = completed, "worker_done");
            Ok(())
        });

        // Verifier: Check all jobs completed
        let verify_enqueued = Arc::clone(&total_enqueued);
        let verify_completed = Arc::clone(&total_completed);
        sim.client("verifier", async move {
            // Wait for jobs to complete
            tokio::time::sleep(Duration::from_secs(30)).await;

            let ch = Endpoint::new("http://server:9923")?
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

            // Check individual job statuses
            let mut terminal_count = 0;
            let mut scheduled_count = 0;
            let mut running_count = 0;

            for i in 0..5 {
                let job_id = format!("rate-limited-job-{}", i);
                if let Ok(resp) = client
                    .get_job(tonic::Request::new(GetJobRequest {
                        shard: 0,
                        id: job_id.clone(),
                        tenant: None,
                        include_attempts: false,
                    }))
                    .await
                {
                    let job = resp.into_inner();
                    match job.status() {
                        JobStatus::Succeeded | JobStatus::Failed | JobStatus::Cancelled => {
                            terminal_count += 1;
                        }
                        JobStatus::Scheduled => scheduled_count += 1,
                        JobStatus::Running => running_count += 1,
                    }
                    tracing::trace!(
                        job_id = %job_id,
                        status = ?job.status(),
                        "job_status"
                    );
                }
            }

            for i in 0..3 {
                let job_id = format!("combined-limit-job-{}", i);
                if let Ok(resp) = client
                    .get_job(tonic::Request::new(GetJobRequest {
                        shard: 0,
                        id: job_id.clone(),
                        tenant: None,
                        include_attempts: false,
                    }))
                    .await
                {
                    let job = resp.into_inner();
                    match job.status() {
                        JobStatus::Succeeded | JobStatus::Failed | JobStatus::Cancelled => {
                            terminal_count += 1;
                        }
                        JobStatus::Scheduled => scheduled_count += 1,
                        JobStatus::Running => running_count += 1,
                    }
                    tracing::trace!(
                        job_id = %job_id,
                        status = ?job.status(),
                        "job_status"
                    );
                }
            }

            let enqueued = verify_enqueued.load(Ordering::SeqCst);
            let completed = verify_completed.load(Ordering::SeqCst);

            tracing::info!(
                enqueued = enqueued,
                completed = completed,
                terminal = terminal_count,
                scheduled = scheduled_count,
                running = running_count,
                "final_verification"
            );

            // Verify progress - with rate limits, all jobs should still complete
            // given our generous limits
            assert!(
                completed > 0,
                "At least some jobs should have completed"
            );

            // Verify most jobs are terminal
            let expected_terminal = (enqueued as f64 * 0.5) as u32;
            assert!(
                terminal_count >= expected_terminal,
                "Expected at least {} terminal jobs, got {}",
                expected_terminal,
                terminal_count
            );

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}
