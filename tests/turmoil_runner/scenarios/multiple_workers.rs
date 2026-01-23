//! Multiple workers scenario: Multiple workers competing for tasks.
//!
//! Invariants verified:
//! - All enqueued jobs are processed exactly once (no missed, no duplicates)
//! - No job is processed by two workers simultaneously
//! - All jobs reach terminal state

use crate::helpers::{
    AttemptStatus, EnqueueRequest, GetJobRequest, HashMap, InvariantTracker, JobStatus,
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

        // Shared state for invariant checking.
        // State tracking is done via server-side DST events, which are emitted
        // synchronously by the server and collected in a thread-local event bus.
        // This eliminates race conditions from client-side tracking.
        let tracker = Arc::new(InvariantTracker::new());
        let total_completed = Arc::new(AtomicU32::new(0));
        let enqueued_jobs: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

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

                producer_jobs.lock().unwrap().push(job_id);
            }
            tracing::trace!("producer_done");
            Ok(())
        });

        // Worker 1
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
        // This verifier consumes server-side DST events to track state accurately
        let verify_tracker = Arc::clone(&tracker);
        let verify_completed = Arc::clone(&total_completed);
        let verify_jobs = Arc::clone(&enqueued_jobs);
        sim.client("verifier", async move {
            // Wait for workers to finish
            tokio::time::sleep(Duration::from_secs(30)).await;

            // Process all DST events from server-side instrumentation
            verify_tracker.process_dst_events();

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
            verify_tracker.verify_all();

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
                succeeded = succeeded,
                "verifier_done"
            );

            Ok(())
        });
    });
}
