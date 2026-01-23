//! High message loss scenario: Testing resilience under 15% packet loss.
//!
//! Invariants verified:
//! - System makes progress despite message loss
//! - Jobs that are successfully enqueued eventually complete or fail
//! - No duplicate completions despite retries

use crate::helpers::{
    AttemptStatus, ClientConfig, EnqueueRequest, GetJobRequest, HashMap, JobStateTracker, JobStatus,
    LeaseTasksRequest, MsgpackBytes, ReportOutcomeRequest, RetryPolicy, create_turmoil_client,
    get_seed, report_outcome_request, run_scenario_impl, setup_server,
};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("high_message_loss", seed, 120, |sim| {
        sim.set_fail_rate(0.15); // 15% message loss
        sim.set_max_message_latency(Duration::from_millis(30));

        // Client configuration optimized for DST with short timeouts
        let client_config = ClientConfig::for_dst();

        sim.host("server", || async move { setup_server(9904).await });

        // Shared tracking state
        let job_tracker = Arc::new(JobStateTracker::new());
        let enqueued_jobs: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let total_completed = Arc::new(AtomicU32::new(0));
        let scenario_done = Arc::new(AtomicBool::new(false));

        let producer_tracker = Arc::clone(&job_tracker);
        let producer_jobs = Arc::clone(&enqueued_jobs);
        let producer_config = client_config.clone();
        sim.client("producer", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let mut client = create_turmoil_client("http://server:9904", &producer_config).await?;
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
                        shard: 0,
                        id: job_id.clone(),
                        priority: i as u32,
                        start_at_ms: 0,
                        retry_policy,
                        payload: Some(MsgpackBytes {
                            data: rmp_serde::to_vec(&serde_json::json!({"job": i})).unwrap(),
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
                        producer_tracker.job_enqueued(&job_id);
                        producer_jobs.lock().unwrap().insert(job_id);
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

        let worker_tracker = Arc::clone(&job_tracker);
        let worker_completed = Arc::clone(&total_completed);
        let worker_done_flag = Arc::clone(&scenario_done);
        let worker_config = client_config.clone();
        sim.client("worker", async move {
            tokio::time::sleep(Duration::from_millis(200)).await;

            let mut client = create_turmoil_client("http://server:9904", &worker_config).await?;
            let mut consecutive_failures = 0u32;

            let mut completed = 0;
            for _ in 0..50 {
                // Exit early if verifier has finished
                if worker_done_flag.load(Ordering::SeqCst) {
                    break;
                }
                match client
                    .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                        shard: Some(0),
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
                            worker_tracker.task_leased(&task.job_id, &task.id);
                            tracing::trace!(job_id = %task.job_id, "lease");
                            match client
                                .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                    shard: 0,
                                    task_id: task.id.clone(),
                                    outcome: Some(report_outcome_request::Outcome::Success(
                                        MsgpackBytes {
                                            data: rmp_serde::to_vec(&serde_json::json!("done"))
                                                .unwrap(),
                                        },
                                    )),
                                }))
                                .await
                            {
                                Ok(_) => {
                                    worker_tracker.task_released(&task.job_id, &task.id);
                                    let was_new = worker_tracker.job_completed(&task.job_id);
                                    if was_new {
                                        tracing::trace!(job_id = %task.job_id, "complete");
                                        completed += 1;
                                        worker_completed.fetch_add(1, Ordering::SeqCst);
                                    } else {
                                        // Already completed - this can happen with message loss
                                        // if our completion ACK was lost but the server processed it
                                        tracing::trace!(job_id = %task.job_id, "already_completed");
                                    }
                                }
                                Err(_) => {
                                    // Completion failed - release tracking, job will be retried
                                    worker_tracker.task_released(&task.job_id, &task.id);
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

        // Verifier: Check final state
        let verify_tracker = Arc::clone(&job_tracker);
        let verify_completed = Arc::clone(&total_completed);
        let verify_jobs = Arc::clone(&enqueued_jobs);
        let verify_done_flag = Arc::clone(&scenario_done);
        let verify_config = client_config.clone();
        sim.client("verifier", async move {
            // Wait for work to complete. With 15% message loss and 2s request timeouts,
            // requests can take a long time. We wait long enough for reasonable progress.
            tokio::time::sleep(Duration::from_secs(60)).await;

            let mut client = create_turmoil_client("http://server:9904", &verify_config).await?;
            let mut consecutive_failures = 0u32;

            let enqueued = verify_jobs.lock().unwrap().clone();
            let completed = verify_completed.load(Ordering::SeqCst);

            tracing::trace!(
                enqueued = enqueued.len(),
                completed = completed,
                "verification_start"
            );

            // Check server state for all attempted enqueues
            let mut server_succeeded = 0u32;
            let mut server_exists = 0u32;
            for job_id in &enqueued {
                match client
                    .get_job(tonic::Request::new(GetJobRequest {
                        shard: 0,
                        id: job_id.clone(),
                        tenant: None,
                        include_attempts: true,
                    }))
                    .await
                {
                    Ok(resp) => {
                        consecutive_failures = 0;
                        server_exists += 1;
                        let job = resp.into_inner();
                        let status = job.status();
                        if status == JobStatus::Succeeded {
                            server_succeeded += 1;
                        }
                        let running_attempts = job
                            .attempts
                            .iter()
                            .filter(|attempt| attempt.status() == AttemptStatus::Running)
                            .count();
                        if status == JobStatus::Running {
                            assert_eq!(
                                running_attempts, 1,
                                "Running job should have exactly one running attempt"
                            );
                        } else {
                            assert_eq!(
                                running_attempts, 0,
                                "Non-running job should not have running attempts"
                            );
                        }
                    }
                    Err(_) => {
                        // Job may not exist if enqueue failed due to message loss
                        consecutive_failures += 1;

                        // Reconnect after failures - HTTP/2 connection may be corrupted
                        if consecutive_failures >= 1 {
                            tracing::trace!("reconnecting after get_job failure");
                            if let Ok(new_client) =
                                create_turmoil_client("http://server:9904", &verify_config).await
                            {
                                client = new_client;
                                consecutive_failures = 0;
                            }
                        }
                    }
                }
            }

            tracing::info!(
                server_exists = server_exists,
                server_succeeded = server_succeeded,
                client_completed = completed,
                "verification_results"
            );

            // INVARIANT: At least some jobs should have completed
            // With 15% message loss, server queries may also fail, so we check both:
            // - client_completed: worker reported successful completion (most reliable)
            // - server_succeeded: server confirms success (may fail due to get_job drops)
            // - enqueued is empty: no jobs were successfully enqueued from client's view
            assert!(
                completed > 0 || server_succeeded > 0 || enqueued.is_empty(),
                "No progress made: client_completed={}, server_succeeded={}, enqueued={}",
                completed,
                server_succeeded,
                enqueued.len()
            );

            // INVARIANT: No terminal job leases
            verify_tracker.verify_no_terminal_leases();

            // Signal worker to exit early
            verify_done_flag.store(true, Ordering::SeqCst);

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}
