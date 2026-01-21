//! Multiple workers scenario: Multiple workers competing for tasks.
//!
//! Invariants verified:
//! - All enqueued jobs are processed exactly once (no missed, no duplicates)
//! - No job is processed by two workers simultaneously
//! - All jobs reach terminal state

use crate::helpers::{
    AttemptStatus, EnqueueRequest, GetJobRequest, HashMap, JobStateTracker, JobStatus,
    LeaseTasksRequest, MsgpackBytes, ReportOutcomeRequest, get_seed, report_outcome_request,
    run_scenario_impl, setup_server, turmoil_connector,
};
use silo::pb::silo_client::SiloClient;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::transport::Endpoint;

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("multiple_workers", seed, 60, |sim| {
        sim.host("server", || async move { setup_server(9902).await });

        // Shared state for invariant checking
        let job_tracker = Arc::new(JobStateTracker::new());
        let total_completed = Arc::new(AtomicU32::new(0));
        let enqueued_jobs: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        let producer_tracker = Arc::clone(&job_tracker);
        let producer_jobs = Arc::clone(&enqueued_jobs);

        // Enqueue jobs from producer
        sim.client("producer", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let ch = Endpoint::new("http://server:9902")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            for i in 0..10 {
                let job_id = format!("job-{}", i);
                tracing::trace!(job_id = %job_id, "enqueue");
                client
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
                    .await?;

                producer_tracker.job_enqueued(&job_id);
                producer_jobs.lock().unwrap().push(job_id);
            }
            tracing::trace!("producer_done");
            Ok(())
        });

        // Worker 1
        let w1_tracker = Arc::clone(&job_tracker);
        let w1_completed = Arc::clone(&total_completed);
        sim.client("worker1", async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let ch = Endpoint::new("http://server:9902")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            let mut completed = 0;
            for _ in 0..10 {
                let lease = client
                    .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                        shard: Some(0),
                        worker_id: "w1".into(),
                        max_tasks: 2,
                        task_group: "default".to_string(),
                    }))
                    .await?
                    .into_inner();

                for task in &lease.tasks {
                    // Track lease acquisition
                    w1_tracker.task_leased(&task.job_id, &task.id);
                    assert_eq!(
                        task.attempt_number, 1,
                        "Unexpected retry attempt in multiple_workers scenario"
                    );

                    tracing::trace!(worker = "worker1", job_id = %task.job_id, "lease");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    client
                        .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                            shard: 0,
                            tenant: None,
                            task_id: task.id.clone(),
                            outcome: Some(report_outcome_request::Outcome::Success(MsgpackBytes {
                                data: rmp_serde::to_vec(&serde_json::json!("ok")).unwrap(),
                            })),
                        }))
                        .await?;

                    // Track completion
                    w1_tracker.task_released(&task.job_id, &task.id);
                    let was_new = w1_tracker.job_completed(&task.job_id);
                    assert!(
                        was_new,
                        "INVARIANT VIOLATION: Job '{}' completed twice!",
                        task.job_id
                    );

                    tracing::trace!(worker = "worker1", job_id = %task.job_id, "complete");
                    completed += 1;
                    w1_completed.fetch_add(1, Ordering::SeqCst);
                }

                if lease.tasks.is_empty() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            tracing::trace!(worker = "worker1", completed = completed, "worker_done");
            Ok(())
        });

        // Worker 2
        let w2_tracker = Arc::clone(&job_tracker);
        let w2_completed = Arc::clone(&total_completed);
        sim.client("worker2", async move {
            tokio::time::sleep(Duration::from_millis(120)).await;

            let ch = Endpoint::new("http://server:9902")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            let mut completed = 0;
            for _ in 0..10 {
                let lease = client
                    .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                        shard: Some(0),
                        worker_id: "w2".into(),
                        max_tasks: 2,
                        task_group: "default".to_string(),
                    }))
                    .await?
                    .into_inner();

                for task in &lease.tasks {
                    // Track lease acquisition
                    w2_tracker.task_leased(&task.job_id, &task.id);
                    assert_eq!(
                        task.attempt_number, 1,
                        "Unexpected retry attempt in multiple_workers scenario"
                    );

                    tracing::trace!(worker = "worker2", job_id = %task.job_id, "lease");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    client
                        .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                            shard: 0,
                            tenant: None,
                            task_id: task.id.clone(),
                            outcome: Some(report_outcome_request::Outcome::Success(MsgpackBytes {
                                data: rmp_serde::to_vec(&serde_json::json!("ok")).unwrap(),
                            })),
                        }))
                        .await?;

                    // Track completion
                    w2_tracker.task_released(&task.job_id, &task.id);
                    let was_new = w2_tracker.job_completed(&task.job_id);
                    assert!(
                        was_new,
                        "INVARIANT VIOLATION: Job '{}' completed twice!",
                        task.job_id
                    );

                    tracing::trace!(worker = "worker2", job_id = %task.job_id, "complete");
                    completed += 1;
                    w2_completed.fetch_add(1, Ordering::SeqCst);
                }

                if lease.tasks.is_empty() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            tracing::trace!(worker = "worker2", completed = completed, "worker_done");
            Ok(())
        });

        // Verifier: Check all jobs completed exactly once
        let verify_tracker = Arc::clone(&job_tracker);
        let verify_completed = Arc::clone(&total_completed);
        let verify_jobs = Arc::clone(&enqueued_jobs);
        sim.client("verifier", async move {
            // Wait for workers to finish
            tokio::time::sleep(Duration::from_secs(30)).await;

            let ch = Endpoint::new("http://server:9902")?
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

            // INVARIANT: All enqueued jobs should be completed
            assert_eq!(
                completed as usize,
                enqueued.len(),
                "Not all jobs completed: {} enqueued, {} completed",
                enqueued.len(),
                completed
            );

            // INVARIANT: All jobs should be in Succeeded state
            let mut succeeded = 0u32;
            for job_id in &enqueued {
                let job_resp = client
                    .get_job(tonic::Request::new(GetJobRequest {
                        shard: 0,
                        id: job_id.clone(),
                        tenant: None,
                        include_attempts: true,
                    }))
                    .await?
                    .into_inner();

                let status = job_resp.status();
                if status == JobStatus::Succeeded {
                    let running_attempts = job_resp
                        .attempts
                        .iter()
                        .filter(|attempt| attempt.status() == AttemptStatus::Running)
                        .count();
                    succeeded += 1;

                    assert_eq!(
                        running_attempts, 0,
                        "Terminal job should not have running attempts"
                    );
                    assert_eq!(
                        job_resp.attempts.len(),
                        1,
                        "Expected exactly one attempt for completed job"
                    );
                    assert_eq!(
                        job_resp.attempts[0].status(),
                        AttemptStatus::Succeeded,
                        "Attempt should be marked Succeeded"
                    );
                } else {
                    tracing::error!(job_id = %job_id, status = ?status, "job_not_succeeded");
                }
            }

            assert_eq!(
                succeeded as usize,
                enqueued.len(),
                "Not all jobs succeeded: {} succeeded out of {}",
                succeeded,
                enqueued.len()
            );

            // INVARIANT: No terminal job leases (noLeasesForTerminal)
            verify_tracker.verify_no_terminal_leases();

            tracing::trace!(
                succeeded = succeeded,
                "verifier_done"
            );

            Ok(())
        });
    });
}
