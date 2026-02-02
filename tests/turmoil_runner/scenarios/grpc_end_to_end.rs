//! gRPC end-to-end scenario: Basic enqueue, lease, and complete flow.

use crate::helpers::{
    AttemptStatus, EnqueueRequest, GetJobRequest, HashMap, JobStatus, LeaseTasksRequest,
    ReportOutcomeRequest, SerializedBytes, TEST_SHARD_ID, connect_to_server, get_seed,
    report_outcome_request, run_scenario_impl, serialized_bytes, setup_server,
    verify_server_invariants,
};

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("grpc_end_to_end", seed, 30, |sim| {
        sim.host("server", || async move { setup_server(9900).await });

        sim.client("client", async move {
            // Connect to server, waiting for it to be ready
            let mut client = connect_to_server("http://server:9900").await?;
            tracing::trace!("client_start");

            // Verify initial server state (no jobs)
            let initial_state = verify_server_invariants(&mut client, TEST_SHARD_ID).await;
            tracing::trace!(result = ?initial_state, "initial_server_state");

            // Enqueue
            tracing::trace!(job_id = "test-job", "enqueue");
            client
                .enqueue(tonic::Request::new(EnqueueRequest {
                    shard: TEST_SHARD_ID.to_string(),
                    id: "test-job".into(),
                    priority: 1,
                    start_at_ms: 0,
                    retry_policy: None,
                    payload: Some(SerializedBytes {
                        encoding: Some(serialized_bytes::Encoding::Msgpack(
                            rmp_serde::to_vec(&serde_json::json!({"test": true})).unwrap(),
                        )),
                    }),
                    limits: vec![],
                    tenant: None,
                    metadata: HashMap::new(),
                    task_group: "default".to_string(),
                }))
                .await?;
            tracing::trace!(job_id = "test-job", "enqueue_done");

            // Verify server state after enqueue (should have no running jobs, no terminal)
            if let Ok(state) = verify_server_invariants(&mut client, TEST_SHARD_ID).await {
                tracing::trace!(
                    running = state.running_job_count,
                    terminal = state.terminal_job_count,
                    "server_state_after_enqueue"
                );
                assert!(
                    state.violations.is_empty(),
                    "Server invariant violations after enqueue: {:?}",
                    state.violations
                );
            }

            // Lease
            let lease = client
                .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                    shard: Some(TEST_SHARD_ID.to_string()),
                    worker_id: "worker-1".into(),
                    max_tasks: 1,
                    task_group: "default".to_string(),
                }))
                .await?
                .into_inner();

            tracing::trace!(count = lease.tasks.len(), "lease");

            if !lease.tasks.is_empty() {
                let task = &lease.tasks[0];
                tracing::trace!(
                    job_id = %task.job_id,
                    attempt = task.attempt_number,
                    "lease_task"
                );

                // INVARIANT: Lease implies job is Running with one running attempt
                let job_resp = client
                    .get_job(tonic::Request::new(GetJobRequest {
                        shard: TEST_SHARD_ID.to_string(),
                        id: task.job_id.clone(),
                        tenant: None,
                        include_attempts: true,
                    }))
                    .await?
                    .into_inner();
                let running_attempts = job_resp
                    .attempts
                    .iter()
                    .filter(|attempt| attempt.status() == AttemptStatus::Running)
                    .count();
                assert_eq!(
                    job_resp.status(),
                    JobStatus::Running,
                    "Leased job should be Running"
                );
                assert_eq!(
                    running_attempts, 1,
                    "Running job should have exactly one running attempt"
                );

                // Complete
                client
                    .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                        shard: TEST_SHARD_ID.to_string(),
                        task_id: task.id.clone(),
                        outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                            encoding: Some(serialized_bytes::Encoding::Msgpack(
                                rmp_serde::to_vec(&serde_json::json!({"result": "ok"})).unwrap(),
                            )),
                        })),
                    }))
                    .await?;
                tracing::trace!(job_id = %task.job_id, "complete");

                // INVARIANT: Completed job is terminal with no running attempts
                let job_resp = client
                    .get_job(tonic::Request::new(GetJobRequest {
                        shard: TEST_SHARD_ID.to_string(),
                        id: task.job_id.clone(),
                        tenant: None,
                        include_attempts: true,
                    }))
                    .await?
                    .into_inner();
                let running_attempts = job_resp
                    .attempts
                    .iter()
                    .filter(|attempt| attempt.status() == AttemptStatus::Running)
                    .count();
                assert_eq!(
                    job_resp.status(),
                    JobStatus::Succeeded,
                    "Completed job should be Succeeded"
                );
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

                // Verify server state after completion
                if let Ok(state) = verify_server_invariants(&mut client, TEST_SHARD_ID).await {
                    tracing::trace!(
                        running = state.running_job_count,
                        terminal = state.terminal_job_count,
                        holders = ?state.holder_counts_by_queue,
                        "server_state_after_complete"
                    );
                    assert!(
                        state.violations.is_empty(),
                        "Server invariant violations after complete: {:?}",
                        state.violations
                    );
                    assert_eq!(
                        state.running_job_count, 0,
                        "No jobs should be running after completion"
                    );
                    assert_eq!(
                        state.terminal_job_count, 1,
                        "One job should be terminal after completion"
                    );
                }
            }

            tracing::trace!("client_done");
            Ok(())
        });
    });
}
