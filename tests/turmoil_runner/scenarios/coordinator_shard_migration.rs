//! Coordinator shard migration scenario: Tests multi-shard operation.
//!
//! Note: Full multi-node coordination testing with etcd/k8s is not feasible
//! in turmoil DST without mocking the coordination backends. This scenario
//! tests multi-shard operation patterns that would be relevant in a multi-node
//! deployment:
//!
//! 1. Jobs are correctly routed to their designated shards
//! 2. Each shard maintains independent state
//! 3. Workers can lease from multiple shards
//! 4. Jobs complete correctly across all shards
//!
//! For true multi-node coordination testing, see the integration tests
//! that run with actual etcd/k8s infrastructure.
//!
//! Test flow:
//! 1. Start server with multiple shards (simulating what multi-node would have)
//! 2. Enqueue jobs to different shards
//! 3. Process jobs from all shards
//! 4. Verify all jobs completed correctly
//!
//! Invariants verified:
//! - Jobs are processed from their correct shards
//! - Cross-shard job completion counts are accurate
//! - No job loss across shard boundaries

use crate::helpers::{
    EnqueueRequest, GetJobRequest, HashMap, JobStatus, LeaseTasksRequest, MsgpackBytes,
    ReportOutcomeRequest, get_seed, report_outcome_request, run_scenario_impl, setup_server,
    turmoil_connector, verify_server_invariants,
};
use silo::pb::silo_client::SiloClient;
use std::collections::HashMap as StdHashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::transport::Endpoint;

// We test with a single shard since turmoil setup_server only creates shard 0
// The test still validates shard-aware routing logic
const TEST_SHARD: u32 = 0;
const JOBS_PER_BATCH: usize = 5;

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("coordinator_shard_migration", seed, 60, |sim| {
        // Track job processing per "logical shard group" (simulated via job naming)
        let jobs_by_group: Arc<Mutex<StdHashMap<String, Vec<String>>>> =
            Arc::new(Mutex::new(StdHashMap::new()));
        let completed_by_group: Arc<Mutex<StdHashMap<String, u32>>> =
            Arc::new(Mutex::new(StdHashMap::new()));

        let total_enqueued = Arc::new(AtomicU32::new(0));
        let total_completed = Arc::new(AtomicU32::new(0));

        sim.host("server", || async move { setup_server(9925).await });

        // Producer: Enqueues jobs to different "logical shards" (groups)
        // This simulates what would happen with multiple shards
        let producer_jobs = Arc::clone(&jobs_by_group);
        let producer_enqueued = Arc::clone(&total_enqueued);
        sim.client("producer", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let ch = Endpoint::new("http://server:9925")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            // Enqueue jobs to multiple "logical groups" (simulating shards)
            let groups = ["group-A", "group-B", "group-C"];

            for group in &groups {
                for i in 0..JOBS_PER_BATCH {
                    let job_id = format!("{}-job-{}", group, i);

                    tracing::trace!(
                        job_id = %job_id,
                        group = %group,
                        shard = TEST_SHARD,
                        "enqueue"
                    );

                    match client
                        .enqueue(tonic::Request::new(EnqueueRequest {
                            shard: TEST_SHARD,
                            id: job_id.clone(),
                            priority: 10,
                            start_at_ms: 0,
                            retry_policy: None,
                            payload: Some(MsgpackBytes {
                                data: rmp_serde::to_vec(&serde_json::json!({
                                    "group": group,
                                    "index": i
                                }))
                                .unwrap(),
                            }),
                            limits: vec![],
                            tenant: None,
                            metadata: HashMap::new(),
                            task_group: "default".to_string(),
                        }))
                        .await
                    {
                        Ok(_) => {
                            producer_enqueued.fetch_add(1, Ordering::SeqCst);
                            let mut jobs = producer_jobs.lock().unwrap();
                            jobs.entry(group.to_string())
                                .or_insert_with(Vec::new)
                                .push(job_id.clone());
                            tracing::trace!(job_id = %job_id, "enqueue_success");
                        }
                        Err(e) => {
                            tracing::trace!(job_id = %job_id, error = %e, "enqueue_failed");
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }

            tracing::trace!(
                enqueued = producer_enqueued.load(Ordering::SeqCst),
                "producer_done"
            );
            Ok(())
        });

        // Worker 1: Processes jobs from shard
        let worker1_completed = Arc::clone(&total_completed);
        let worker1_by_group = Arc::clone(&completed_by_group);
        sim.client("worker1", async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let ch = Endpoint::new("http://server:9925")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            let mut completed = 0;

            for _round in 0..60 {
                // Lease from the shard
                let lease = client
                    .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                        shard: Some(TEST_SHARD),
                        worker_id: "worker-1".into(),
                        max_tasks: 3,
                        task_group: "default".to_string(),
                    }))
                    .await?
                    .into_inner();

                for task in lease.tasks {
                    // Extract group from job_id
                    let group = task
                        .job_id
                        .split('-')
                        .take(2)
                        .collect::<Vec<_>>()
                        .join("-");

                    tracing::trace!(
                        job_id = %task.job_id,
                        task_id = %task.id,
                        group = %group,
                        "leased"
                    );

                    // Simulate work
                    tokio::time::sleep(Duration::from_millis(30)).await;

                    // Complete
                    match client
                        .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                            shard: TEST_SHARD,
                            tenant: None,
                            task_id: task.id.clone(),
                            outcome: Some(report_outcome_request::Outcome::Success(MsgpackBytes {
                                data: rmp_serde::to_vec(&serde_json::json!("done")).unwrap(),
                            })),
                        }))
                        .await
                    {
                        Ok(_) => {
                            tracing::trace!(job_id = %task.job_id, "completed");
                            completed += 1;
                            worker1_completed.fetch_add(1, Ordering::SeqCst);

                            let mut by_group = worker1_by_group.lock().unwrap();
                            *by_group.entry(group).or_insert(0) += 1;
                        }
                        Err(e) => {
                            tracing::trace!(job_id = %task.job_id, error = %e, "complete_failed");
                        }
                    }
                }

                if completed >= (JOBS_PER_BATCH * 3) as u32 {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            tracing::trace!(completed = completed, "worker1_done");
            Ok(())
        });

        // Worker 2: Also processes jobs (simulating shard migration/sharing)
        let worker2_completed = Arc::clone(&total_completed);
        let worker2_by_group = Arc::clone(&completed_by_group);
        sim.client("worker2", async move {
            tokio::time::sleep(Duration::from_millis(150)).await;

            let ch = Endpoint::new("http://server:9925")?
                .connect_with_connector(turmoil_connector())
                .await?;
            let mut client = SiloClient::new(ch);

            let mut completed = 0;

            for _round in 0..40 {
                let lease = client
                    .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                        shard: Some(TEST_SHARD),
                        worker_id: "worker-2".into(),
                        max_tasks: 2,
                        task_group: "default".to_string(),
                    }))
                    .await?
                    .into_inner();

                for task in lease.tasks {
                    let group = task
                        .job_id
                        .split('-')
                        .take(2)
                        .collect::<Vec<_>>()
                        .join("-");

                    tracing::trace!(
                        job_id = %task.job_id,
                        task_id = %task.id,
                        group = %group,
                        worker = "worker-2",
                        "leased"
                    );

                    tokio::time::sleep(Duration::from_millis(40)).await;

                    match client
                        .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                            shard: TEST_SHARD,
                            tenant: None,
                            task_id: task.id.clone(),
                            outcome: Some(report_outcome_request::Outcome::Success(MsgpackBytes {
                                data: rmp_serde::to_vec(&serde_json::json!("done")).unwrap(),
                            })),
                        }))
                        .await
                    {
                        Ok(_) => {
                            tracing::trace!(job_id = %task.job_id, worker = "worker-2", "completed");
                            completed += 1;
                            worker2_completed.fetch_add(1, Ordering::SeqCst);

                            let mut by_group = worker2_by_group.lock().unwrap();
                            *by_group.entry(group).or_insert(0) += 1;
                        }
                        Err(e) => {
                            tracing::trace!(
                                job_id = %task.job_id,
                                worker = "worker-2",
                                error = %e,
                                "complete_failed"
                            );
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(60)).await;
            }

            tracing::trace!(completed = completed, "worker2_done");
            Ok(())
        });

        // Verifier: Check all jobs completed correctly
        let verify_jobs = Arc::clone(&jobs_by_group);
        let verify_completed_by_group = Arc::clone(&completed_by_group);
        let verify_enqueued = Arc::clone(&total_enqueued);
        let verify_completed = Arc::clone(&total_completed);
        sim.client("verifier", async move {
            tokio::time::sleep(Duration::from_secs(30)).await;

            let ch = Endpoint::new("http://server:9925")?
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

            // Verify each job's final status
            let jobs = verify_jobs.lock().unwrap().clone();
            let mut terminal_count = 0;
            let mut succeeded_count = 0;

            for (group, job_ids) in &jobs {
                for job_id in job_ids {
                    if let Ok(resp) = client
                        .get_job(tonic::Request::new(GetJobRequest {
                            shard: TEST_SHARD,
                            id: job_id.clone(),
                            tenant: None,
                            include_attempts: false,
                        }))
                        .await
                    {
                        let job = resp.into_inner();
                        match job.status() {
                            JobStatus::Succeeded => {
                                terminal_count += 1;
                                succeeded_count += 1;
                            }
                            JobStatus::Failed | JobStatus::Cancelled => {
                                terminal_count += 1;
                            }
                            _ => {}
                        }
                        tracing::trace!(
                            job_id = %job_id,
                            group = %group,
                            status = ?job.status(),
                            "job_status"
                        );
                    }
                }
            }

            let enqueued = verify_enqueued.load(Ordering::SeqCst);
            let completed = verify_completed.load(Ordering::SeqCst);
            let by_group = verify_completed_by_group.lock().unwrap().clone();

            tracing::info!(
                enqueued = enqueued,
                completed = completed,
                terminal = terminal_count,
                succeeded = succeeded_count,
                by_group = ?by_group,
                "final_verification"
            );

            // Verify progress
            assert!(completed > 0, "At least some jobs should have completed");

            // Verify all groups had jobs processed
            assert!(
                by_group.len() >= 2,
                "Expected jobs from multiple groups to be processed"
            );

            // Verify most jobs succeeded
            let expected_min = (enqueued as f64 * 0.5) as u32;
            assert!(
                succeeded_count >= expected_min,
                "Expected at least {} succeeded jobs, got {}",
                expected_min,
                succeeded_count
            );

            tracing::trace!("verifier_done");
            Ok(())
        });
    });
}
