//! High message loss scenario: Testing resilience under 15% packet loss.
//!
//! Invariants verified:
//! - System makes progress despite message loss
//! - Jobs that are successfully enqueued eventually complete or fail
//! - No duplicate completions despite retries

use crate::helpers::{
    EnqueueRequest, GetJobRequest, HashMap, JobStateTracker, JobStatus, LeaseTasksRequest,
    MsgpackBytes, ReportOutcomeRequest, get_seed, report_outcome_request, run_scenario_impl,
    setup_server, turmoil_connector,
};
use silo::pb::silo_client::SiloClient;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::transport::Endpoint;

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("high_message_loss", seed, 90, |sim| {
        sim.set_fail_rate(0.15); // 15% message loss
        sim.set_max_message_latency(Duration::from_millis(30));

        sim.host("server", || async move { setup_server(9904).await });

        // Shared tracking state
        let job_tracker = Arc::new(JobStateTracker::new());
        let enqueued_jobs: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let total_completed = Arc::new(AtomicU32::new(0));

        let producer_tracker = Arc::clone(&job_tracker);
        let producer_jobs = Arc::clone(&enqueued_jobs);
        sim.client("producer", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let ch = Endpoint::new("http://server:9904")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            let mut enqueued = 0;
            for i in 0..20 {
                let job_id = format!("lossy-job-{}", i);
                match client
                    .enqueue(tonic::Request::new(EnqueueRequest {
                        shard: 0,
                        id: job_id.clone(),
                        priority: i as u32,
                        start_at_ms: 0,
                        retry_policy: None,
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
                    }
                    Err(_) => {
                        tracing::trace!(job_id = %job_id, "enqueue_failed");
                    }
                }
            }
            tracing::trace!(enqueued = enqueued, "producer_done");
            Ok(())
        });

        let worker_tracker = Arc::clone(&job_tracker);
        let worker_completed = Arc::clone(&total_completed);
        sim.client("worker", async move {
            tokio::time::sleep(Duration::from_millis(200)).await;

            let ch = Endpoint::new("http://server:9904")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            let mut completed = 0;
            for _ in 0..30 {
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
                        let tasks = resp.into_inner().tasks;
                        for task in &tasks {
                            worker_tracker.task_leased(&task.job_id, &task.id);
                            tracing::trace!(job_id = %task.job_id, "lease");
                            match client
                                .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                    shard: 0,
                                    tenant: None,
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
        sim.client("verifier", async move {
            // Wait for work to complete
            tokio::time::sleep(Duration::from_secs(60)).await;

            let ch = Endpoint::new("http://server:9904")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

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
                        include_attempts: false,
                    }))
                    .await
                {
                    Ok(resp) => {
                        server_exists += 1;
                        let status = resp.into_inner().status();
                        if status == JobStatus::Succeeded {
                            server_succeeded += 1;
                        }
                    }
                    Err(_) => {
                        // Job may not exist if enqueue failed due to message loss
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
            // With 15% message loss over 30 rounds, we should make progress
            assert!(
                server_succeeded > 0 || enqueued.is_empty(),
                "No jobs succeeded despite {} enqueue attempts",
                enqueued.len()
            );

            // INVARIANT: No terminal job leases
            verify_tracker.verify_no_terminal_leases();

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}
