//! Expedite concurrency scenario: Tests expedite behavior with concurrency limits.
//!
//! When a job is expedited, it should be prioritized for the next available
//! concurrency slot, even if other jobs have higher priority numbers.
//!
//! Test flow:
//! 1. Enqueue jobs A (priority 10), B (priority 20), C (priority 30) with limit max=1
//!    Note: Lower priority number = higher priority in Silo
//! 2. A runs first (highest priority), B and C are waiting for tickets
//! 3. Expedite C to start_at=now (even though C has lowest priority)
//! 4. A completes
//! 5. Assert: C runs next (due to expedite moving it to front of queue)
//! 6. C completes, then B runs
//!
//! Also tests:
//! - expeditePreservesStatus: Job stays Scheduled after expedite
//! - expediteRejectsCancelled: Cannot expedite cancelled job
//!
//! Invariants verified:
//! - queueLimitEnforced: Never more than 1 holder for the limit
//! - Expedited job gets priority over waiting jobs

use crate::helpers::{
    ConcurrencyLimit, EnqueueRequest, GetJobRequest, HashMap, JobStatus, LeaseTasksRequest, Limit,
    MsgpackBytes, ReportOutcomeRequest, check_holder_limits, get_seed, limit,
    report_outcome_request, run_scenario_impl, setup_server, turmoil_connector,
    verify_server_invariants,
};
use silo::pb::silo_client::SiloClient;
use silo::pb::{CancelJobRequest, ExpediteJobRequest};
use std::collections::HashMap as StdHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::transport::Endpoint;

const LIMIT_KEY: &str = "expedite-mutex";
const MAX_CONCURRENCY: u32 = 1;

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("expedite_concurrency", seed, 60, |sim| {
        // Track completion order
        let completion_order: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let lease_order: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        // Track timestamps
        let job_a_complete_time = Arc::new(AtomicU64::new(0));
        let job_c_expedited_time = Arc::new(AtomicU64::new(0));
        let expedite_rejected_cancelled = Arc::new(AtomicU64::new(0));

        sim.host("server", || async move { setup_server(9922).await });

        // Producer: Enqueues jobs A, B, C, D with different priorities
        sim.client("producer", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let ch = Endpoint::new("http://server:9922")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            // Enqueue job A - highest priority (lowest number)
            tracing::trace!(job_id = "job-A", priority = 10, "enqueue");
            client
                .enqueue(tonic::Request::new(EnqueueRequest {
                    shard: 0,
                    id: "job-A".into(),
                    priority: 10, // Highest priority
                    start_at_ms: 0,
                    retry_policy: None,
                    payload: Some(MsgpackBytes {
                        data: rmp_serde::to_vec(&serde_json::json!({"job": "A"})).unwrap(),
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
                .await?;

            tokio::time::sleep(Duration::from_millis(10)).await;

            // Enqueue job B - medium priority
            tracing::trace!(job_id = "job-B", priority = 20, "enqueue");
            client
                .enqueue(tonic::Request::new(EnqueueRequest {
                    shard: 0,
                    id: "job-B".into(),
                    priority: 20,
                    start_at_ms: 0,
                    retry_policy: None,
                    payload: Some(MsgpackBytes {
                        data: rmp_serde::to_vec(&serde_json::json!({"job": "B"})).unwrap(),
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
                .await?;

            tokio::time::sleep(Duration::from_millis(10)).await;

            // Enqueue job C - lowest priority (will be expedited)
            tracing::trace!(job_id = "job-C", priority = 30, "enqueue");
            client
                .enqueue(tonic::Request::new(EnqueueRequest {
                    shard: 0,
                    id: "job-C".into(),
                    priority: 30, // Lowest priority
                    start_at_ms: 0,
                    retry_policy: None,
                    payload: Some(MsgpackBytes {
                        data: rmp_serde::to_vec(&serde_json::json!({"job": "C"})).unwrap(),
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
                .await?;

            tokio::time::sleep(Duration::from_millis(10)).await;

            // Enqueue job D - will be cancelled to test expediteRejectsCancelled
            tracing::trace!(job_id = "job-D", priority = 40, "enqueue");
            client
                .enqueue(tonic::Request::new(EnqueueRequest {
                    shard: 0,
                    id: "job-D".into(),
                    priority: 40,
                    start_at_ms: 0,
                    retry_policy: None,
                    payload: Some(MsgpackBytes {
                        data: rmp_serde::to_vec(&serde_json::json!({"job": "D"})).unwrap(),
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
                .await?;

            tracing::trace!("producer_done");
            Ok(())
        });

        // Expediter: Expedites job C after A starts running
        let expediter_a_complete = Arc::clone(&job_a_complete_time);
        let expediter_c_expedited = Arc::clone(&job_c_expedited_time);
        let expediter_rejected = Arc::clone(&expedite_rejected_cancelled);
        sim.client("expediter", async move {
            // Wait for job A to be leased and running
            tokio::time::sleep(Duration::from_millis(300)).await;

            let ch = Endpoint::new("http://server:9922")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            // First, cancel job D
            tracing::trace!(job_id = "job-D", "cancelling");
            let _ = client
                .cancel_job(tonic::Request::new(CancelJobRequest {
                    shard: 0,
                    id: "job-D".into(),
                    tenant: None,
                }))
                .await;

            // Try to expedite cancelled job D - should fail
            tracing::trace!(job_id = "job-D", "expediting_cancelled_job");
            match client
                .expedite_job(tonic::Request::new(ExpediteJobRequest {
                    shard: 0,
                    id: "job-D".into(),
                    tenant: None,
                }))
                .await
            {
                Ok(_) => {
                    tracing::trace!(job_id = "job-D", "expedite_succeeded_unexpectedly");
                }
                Err(e) => {
                    expediter_rejected.store(1, Ordering::SeqCst);
                    tracing::trace!(
                        job_id = "job-D",
                        error = %e,
                        "expedite_rejected_cancelled_as_expected"
                    );
                }
            }

            // Verify job C is still Scheduled before expedite
            let job_c_before = client
                .get_job(tonic::Request::new(GetJobRequest {
                    shard: 0,
                    id: "job-C".into(),
                    tenant: None,
                    include_attempts: false,
                }))
                .await;

            if let Ok(resp) = job_c_before {
                let job = resp.into_inner();
                tracing::trace!(
                    job_id = "job-C",
                    status = ?job.status(),
                    "status_before_expedite"
                );
            }

            // Now expedite job C (which has lowest priority but we want it to run next)
            // Do this before A completes so C is waiting when expedited
            if expediter_a_complete.load(Ordering::SeqCst) == 0 {
                tracing::trace!(job_id = "job-C", "expediting");
                match client
                    .expedite_job(tonic::Request::new(ExpediteJobRequest {
                        shard: 0,
                        id: "job-C".into(),
                        tenant: None,
                    }))
                    .await
                {
                    Ok(_) => {
                        let expedite_time = turmoil::sim_elapsed()
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        expediter_c_expedited.store(expedite_time, Ordering::SeqCst);
                        tracing::trace!(
                            job_id = "job-C",
                            expedite_time_ms = expedite_time,
                            "expedited"
                        );
                    }
                    Err(e) => {
                        tracing::trace!(job_id = "job-C", error = %e, "expedite_failed");
                    }
                }
            }

            // Verify job C is still Scheduled after expedite (expeditePreservesStatus)
            let job_c_after = client
                .get_job(tonic::Request::new(GetJobRequest {
                    shard: 0,
                    id: "job-C".into(),
                    tenant: None,
                    include_attempts: false,
                }))
                .await;

            if let Ok(resp) = job_c_after {
                let job = resp.into_inner();
                tracing::trace!(
                    job_id = "job-C",
                    status = ?job.status(),
                    "status_after_expedite"
                );
                // Job should still be Scheduled (not Running yet since A is still running)
                if job.status() != JobStatus::Running {
                    assert_eq!(
                        job.status(),
                        JobStatus::Scheduled,
                        "expeditePreservesStatus: Job C should still be Scheduled after expedite"
                    );
                }
            }

            tracing::trace!("expediter_done");
            Ok(())
        });

        // Worker: Processes jobs in order
        let worker_completion_order = Arc::clone(&completion_order);
        let worker_lease_order = Arc::clone(&lease_order);
        let worker_a_complete = Arc::clone(&job_a_complete_time);
        sim.client("worker", async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let ch = Endpoint::new("http://server:9922")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            let mut jobs_completed = 0;

            for _round in 0..60 {
                let lease = client
                    .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                        shard: Some(0),
                        worker_id: "worker-1".into(),
                        max_tasks: 1,
                        task_group: "default".to_string(),
                    }))
                    .await?
                    .into_inner();

                for task in lease.tasks {
                    let sim_time = turmoil::sim_elapsed()
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);

                    // Record lease order
                    {
                        let mut order = worker_lease_order.lock().unwrap();
                        order.push(task.job_id.clone());
                    }

                    tracing::trace!(
                        job_id = %task.job_id,
                        task_id = %task.id,
                        sim_time_ms = sim_time,
                        "leased"
                    );

                    // Simulate some work
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    // Complete the task
                    match client
                        .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                            shard: 0,
                            task_id: task.id.clone(),
                            outcome: Some(report_outcome_request::Outcome::Success(MsgpackBytes {
                                data: rmp_serde::to_vec(&serde_json::json!("done")).unwrap(),
                            })),
                        }))
                        .await
                    {
                        Ok(_) => {
                            let complete_time = turmoil::sim_elapsed()
                                .map(|d| d.as_millis() as u64)
                                .unwrap_or(0);

                            if task.job_id == "job-A" {
                                worker_a_complete.store(complete_time, Ordering::SeqCst);
                            }

                            // Record completion order
                            {
                                let mut order = worker_completion_order.lock().unwrap();
                                order.push(task.job_id.clone());
                            }

                            tracing::trace!(
                                job_id = %task.job_id,
                                complete_time_ms = complete_time,
                                "completed"
                            );
                            jobs_completed += 1;
                        }
                        Err(e) => {
                            tracing::trace!(
                                job_id = %task.job_id,
                                error = %e,
                                "complete_failed"
                            );
                        }
                    }
                }

                // Stop if we've completed A, B, C (D was cancelled)
                if jobs_completed >= 3 {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            tracing::trace!(jobs_completed = jobs_completed, "worker_done");
            Ok(())
        });

        // Verifier: Check completion order and invariants
        let verify_completion_order = Arc::clone(&completion_order);
        let verify_lease_order = Arc::clone(&lease_order);
        let verify_c_expedited = Arc::clone(&job_c_expedited_time);
        let verify_rejected = Arc::clone(&expedite_rejected_cancelled);
        sim.client("verifier", async move {
            // Wait for jobs to complete
            tokio::time::sleep(Duration::from_secs(30)).await;

            let ch = Endpoint::new("http://server:9922")?
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

                // Check holder limits
                let mut limits = StdHashMap::new();
                limits.insert(LIMIT_KEY.to_string(), MAX_CONCURRENCY);
                let limit_violations = check_holder_limits(&state, &limits);
                assert!(
                    limit_violations.is_empty(),
                    "Holder limit violations: {:?}",
                    limit_violations
                );

                tracing::trace!(
                    running = state.running_job_count,
                    terminal = state.terminal_job_count,
                    "final_server_state"
                );
            }

            // Get completion and lease order
            let completion = verify_completion_order.lock().unwrap().clone();
            let leases = verify_lease_order.lock().unwrap().clone();
            let c_expedited = verify_c_expedited.load(Ordering::SeqCst);
            let rejected_cancelled = verify_rejected.load(Ordering::SeqCst);

            tracing::info!(
                completion_order = ?completion,
                lease_order = ?leases,
                c_expedited_ms = c_expedited,
                rejected_cancelled = rejected_cancelled,
                "order_verification"
            );

            // Verify expediteRejectsCancelled
            assert!(
                rejected_cancelled > 0,
                "expediteRejectsCancelled: Expediting cancelled job D should have been rejected"
            );

            // Verify job A completed first (highest priority)
            assert!(
                !completion.is_empty(),
                "At least one job should have completed"
            );
            assert_eq!(
                completion[0], "job-A",
                "Job A should complete first (highest priority)"
            );

            // If C was expedited before A completed, C should run before B
            if c_expedited > 0 && completion.len() >= 3 {
                // Check that C appears before B in lease order (after A)
                let a_pos = leases.iter().position(|j| j == "job-A");
                let b_pos = leases.iter().position(|j| j == "job-B");
                let c_pos = leases.iter().position(|j| j == "job-C");

                if let (Some(a), Some(b), Some(c)) = (a_pos, b_pos, c_pos) {
                    tracing::info!(
                        a_lease_pos = a,
                        b_lease_pos = b,
                        c_lease_pos = c,
                        "lease_positions"
                    );

                    // A should be first
                    assert_eq!(a, 0, "Job A should be leased first");

                    // After expedite, C should be leased before B
                    assert!(
                        c < b,
                        "Expedited job C (pos {}) should be leased before B (pos {})",
                        c,
                        b
                    );
                    tracing::info!(
                        "Expedite priority verified: C leased at position {}, B at position {}",
                        c,
                        b
                    );
                }
            }

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}
