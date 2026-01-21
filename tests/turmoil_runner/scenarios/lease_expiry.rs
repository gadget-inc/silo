//! Lease expiry scenario: Worker crash and lease recovery by another worker.
//!
//! Invariants verified:
//! - Lease expiry releases the job for re-execution
//! - Second worker successfully acquires the expired lease
//! - Job completes successfully after recovery
//! - Attempt count increases after lease expiry (first attempt failed/timed out)

use crate::helpers::{
    AttemptStatus, EnqueueRequest, GetJobRequest, HashMap, JobStateTracker, JobStatus,
    LeaseTasksRequest, MsgpackBytes, ReportOutcomeRequest, RetryPolicy, get_seed,
    report_outcome_request, run_scenario_impl, setup_server, turmoil, turmoil_connector,
};
use silo::pb::silo_client::SiloClient;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use tonic::transport::Endpoint;

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("lease_expiry", seed, 60, |sim| {
        sim.host("server", || async move { setup_server(9903).await });

        let worker2_got_task = StdArc::new(AtomicBool::new(false));
        let worker2_got_task_clone = worker2_got_task.clone();

        // Track job state for invariant verification
        let job_tracker = StdArc::new(JobStateTracker::new());
        let job_tracker_w1 = job_tracker.clone();
        let job_tracker_w2 = job_tracker.clone();
        let job_tracker_verify = job_tracker.clone();

        // Track attempt numbers seen by each worker
        let w1_attempt = StdArc::new(AtomicU32::new(0));
        let w2_attempt = StdArc::new(AtomicU32::new(0));
        let w1_attempt_clone = w1_attempt.clone();
        let w2_attempt_clone = w2_attempt.clone();

        // Worker 1 - leases and crashes (partition)
        sim.client("worker1", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let ch = Endpoint::new("http://server:9903")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            // Enqueue with retry policy
            tracing::trace!(job_id = "lease-test", "enqueue");
            client
                .enqueue(tonic::Request::new(EnqueueRequest {
                    shard: 0,
                    id: "lease-test".into(),
                    priority: 1,
                    start_at_ms: 0,
                    retry_policy: Some(RetryPolicy {
                        retry_count: 3,
                        initial_interval_ms: 0,
                        max_interval_ms: 0,
                        randomize_interval: false,
                        backoff_factor: 1.0,
                    }),
                    payload: Some(MsgpackBytes {
                        data: rmp_serde::to_vec(&serde_json::json!({"test": "lease"})).unwrap(),
                    }),
                    limits: vec![],
                    tenant: None,
                    metadata: HashMap::new(),
                    task_group: "default".to_string(),
                }))
                .await?;

            // Track enqueue
            job_tracker_w1.job_enqueued("lease-test");

            // Lease
            let lease = client
                .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                    shard: Some(0),
                    worker_id: "w1".into(),
                    max_tasks: 1,
                    task_group: "default".to_string(),
                }))
                .await?
                .into_inner();

            tracing::trace!(worker = "worker1", count = lease.tasks.len(), "lease");

            assert!(
                !lease.tasks.is_empty(),
                "Worker1 should lease the initial task"
            );
            let leased_task_info: Option<(String, String)> = if !lease.tasks.is_empty() {
                let task = &lease.tasks[0];
                // Track lease and attempt number
                job_tracker_w1.task_leased(&task.job_id, &task.id);
                assert_eq!(task.attempt_number, 1, "First lease should be attempt #1");
                w1_attempt_clone.store(task.attempt_number, Ordering::SeqCst);
                tracing::trace!(
                    worker = "worker1",
                    job_id = %task.job_id,
                    attempt = task.attempt_number,
                    "lease_acquired"
                );
                Some((task.job_id.clone(), task.id.clone()))
            } else {
                None
            };

            // Release from tracker BEFORE crashing. The server's lease reaper will
            // release the expired lease server-side, so we simulate that here.
            if let Some((job_id, task_id)) = &leased_task_info {
                job_tracker_w1.task_released(job_id, task_id);
            }

            // Simulate crash
            turmoil::partition("worker1", "server");
            let sim_time = turmoil::sim_elapsed().map(|d| d.as_millis()).unwrap_or(0);
            tracing::trace!(worker = "worker1", sim_time_ms = sim_time, "partition");

            // Wait past lease expiry (10s default)
            tokio::time::sleep(Duration::from_millis(15000)).await;

            let sim_time = turmoil::sim_elapsed().map(|d| d.as_millis()).unwrap_or(0);
            tracing::trace!(worker = "worker1", sim_time_ms = sim_time, "after_expiry");

            // Repair for cleanup but don't complete
            turmoil::repair("worker1", "server");

            Ok(())
        });

        // Worker 2 - should pick up expired lease
        sim.client("worker2", async move {
            tokio::time::sleep(Duration::from_millis(25000)).await;

            let ch = Endpoint::new("http://server:9903")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            let sim_time = turmoil::sim_elapsed().map(|d| d.as_millis()).unwrap_or(0);
            tracing::trace!(worker = "worker2", sim_time_ms = sim_time, "start");

            for attempt in 0..20 {
                match client
                    .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                        shard: Some(0),
                        worker_id: "w2".into(),
                        max_tasks: 1,
                        task_group: "default".to_string(),
                    }))
                    .await
                {
                    Ok(resp) => {
                        let tasks = resp.into_inner().tasks;
                        if !tasks.is_empty() {
                            let task = &tasks[0];

                            // Track lease and attempt number
                            job_tracker_w2.task_leased(&task.job_id, &task.id);
                            assert!(
                                task.attempt_number >= 2,
                                "Retry lease should have attempt number >= 2"
                            );
                            w2_attempt_clone.store(task.attempt_number, Ordering::SeqCst);

                            tracing::trace!(
                                worker = "worker2",
                                attempt = attempt,
                                job_id = %task.job_id,
                                task_attempt = task.attempt_number,
                                "lease"
                            );
                            worker2_got_task_clone.store(true, Ordering::SeqCst);

                            // Release from tracker BEFORE report_outcome (server releases immediately)
                            job_tracker_w2.task_released(&task.job_id, &task.id);

                            client
                                .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                    shard: 0,
                                    tenant: None,
                                    task_id: task.id.clone(),
                                    outcome: Some(report_outcome_request::Outcome::Success(
                                        MsgpackBytes {
                                            data: rmp_serde::to_vec(&serde_json::json!(
                                                "recovered"
                                            ))
                                            .unwrap(),
                                        },
                                    )),
                                }))
                                .await?;

                            // Track completion (release already done above)
                            job_tracker_w2.job_completed(&task.job_id);

                            tracing::trace!(worker = "worker2", job_id = %task.job_id, "complete");
                            break;
                        }
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            let got_task = worker2_got_task_clone.load(Ordering::SeqCst);
            tracing::trace!(worker = "worker2", got_task = got_task, "done");

            // This is the key assertion - if mad-turmoil's time simulation works,
            // the lease should have expired and worker2 should get the task
            assert!(
                got_task,
                "Worker2 should have gotten the task after lease expiry"
            );

            Ok(())
        });

        // Verifier: Check final job state
        sim.client("verifier", async move {
            // Wait for everything to complete
            tokio::time::sleep(Duration::from_secs(40)).await;

            let ch = Endpoint::new("http://server:9903")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            // Verify job reached terminal state
            let job_resp = client
                .get_job(tonic::Request::new(GetJobRequest {
                    shard: 0,
                    id: "lease-test".into(),
                    tenant: None,
                    include_attempts: true,
                }))
                .await?
                .into_inner();

            let status = job_resp.status();
            tracing::trace!(
                job_id = "lease-test",
                status = ?status,
                attempt_count = job_resp.attempts.len(),
                "final_job_state"
            );
            let running_attempts = job_resp
                .attempts
                .iter()
                .filter(|attempt| attempt.status() == AttemptStatus::Running)
                .count();
            let succeeded_attempts = job_resp
                .attempts
                .iter()
                .filter(|attempt| attempt.status() == AttemptStatus::Succeeded)
                .count();
            let crashed_attempts = job_resp
                .attempts
                .iter()
                .filter(|attempt| {
                    attempt.status() == AttemptStatus::Failed
                        && attempt.error_code.as_deref() == Some("WORKER_CRASHED")
                })
                .count();

            // INVARIANT: Job should be in terminal state (Succeeded)
            assert!(
                status == JobStatus::Succeeded,
                "Job should be Succeeded after recovery, got {:?}",
                status
            );
            assert_eq!(
                running_attempts, 0,
                "Terminal job should not have running attempts"
            );
            assert_eq!(
                succeeded_attempts, 1,
                "Expected exactly one successful attempt"
            );
            assert!(
                crashed_attempts >= 1,
                "Expected a failed attempt due to lease expiry (WORKER_CRASHED)"
            );
            assert!(
                job_resp.attempts.len() >= 2,
                "Expected at least two attempts after lease expiry"
            );

            let mut seen_attempt_numbers = std::collections::HashSet::new();
            for attempt in &job_resp.attempts {
                assert!(
                    seen_attempt_numbers.insert(attempt.attempt_number),
                    "Duplicate attempt number {} in job attempts",
                    attempt.attempt_number
                );
            }

            // INVARIANT: There should be multiple attempts (first expired, second succeeded)
            // Note: The reaper creates a new attempt when the lease expires
            let w1_attempt_num = w1_attempt.load(Ordering::SeqCst);
            let w2_attempt_num = w2_attempt.load(Ordering::SeqCst);

            tracing::trace!(
                w1_attempt = w1_attempt_num,
                w2_attempt = w2_attempt_num,
                "attempt_numbers"
            );

            // Worker 2's attempt should be after worker 1's (could be same if retry policy
            // creates a new task, or different if lease expiry creates retry)
            // The key invariant is that worker2 got a task and completed it successfully
            assert!(
                w1_attempt_num > 0,
                "Worker1 should have recorded an attempt number"
            );
            assert!(
                w2_attempt_num > w1_attempt_num,
                "Worker2 attempt number should be greater than worker1's"
            );

            // INVARIANT: Job tracker should show the job as completed
            assert!(
                job_tracker_verify.is_completed("lease-test"),
                "Job should be marked as completed in tracker"
            );

            // INVARIANT: No terminal job leases (noLeasesForTerminal)
            job_tracker_verify.verify_no_terminal_leases();

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}
