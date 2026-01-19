//! Lease expiry scenario: Worker crash and lease recovery by another worker.

use crate::helpers::{
    EnqueueRequest, HashMap, LeaseTasksRequest, MsgpackBytes, ReportOutcomeRequest, RetryPolicy,
    get_seed, report_outcome_request, run_scenario_impl, setup_server, turmoil, turmoil_connector,
};
use silo::pb::silo_client::SiloClient;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tonic::transport::Endpoint;

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("lease_expiry", seed, 60, |sim| {
        sim.host("server", || async move { setup_server(9903).await });

        let worker2_got_task = StdArc::new(AtomicBool::new(false));
        let worker2_got_task_clone = worker2_got_task.clone();

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
                            tracing::trace!(
                                worker = "worker2",
                                attempt = attempt,
                                job_id = %tasks[0].job_id,
                                "lease"
                            );
                            worker2_got_task_clone.store(true, Ordering::SeqCst);

                            client
                                .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                                    shard: 0,
                                    tenant: None,
                                    task_id: tasks[0].id.clone(),
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
                            tracing::trace!(worker = "worker2", job_id = %tasks[0].job_id, "complete");
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
    });
}
