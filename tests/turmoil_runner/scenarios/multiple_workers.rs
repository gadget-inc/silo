//! Multiple workers scenario: Multiple workers competing for tasks.

use crate::helpers::{
    EnqueueRequest, HashMap, LeaseTasksRequest, MsgpackBytes, ReportOutcomeRequest, get_seed,
    report_outcome_request, run_scenario_impl, setup_server, turmoil_connector,
};
use silo::pb::silo_client::SiloClient;
use std::time::Duration;
use tonic::transport::Endpoint;

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("multiple_workers", seed, 60, |sim| {
        sim.host("server", || async move { setup_server(9902).await });

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
                        id: job_id,
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
            }
            tracing::trace!("producer_done");
            Ok(())
        });

        // Worker 1
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
                }

                if lease.tasks.is_empty() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            tracing::trace!(worker = "worker1", completed = completed, "worker_done");
            Ok(())
        });

        // Worker 2
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
                }

                if lease.tasks.is_empty() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            tracing::trace!(worker = "worker2", completed = completed, "worker_done");
            Ok(())
        });
    });
}
