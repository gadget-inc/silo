//! High message loss scenario: Testing resilience under 15% packet loss.

use crate::helpers::{
    EnqueueRequest, HashMap, LeaseTasksRequest, MsgpackBytes, ReportOutcomeRequest, get_seed,
    report_outcome_request, run_scenario_impl, setup_server, turmoil_connector,
};
use silo::pb::silo_client::SiloClient;
use std::time::Duration;
use tonic::transport::Endpoint;

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("high_message_loss", seed, 90, |sim| {
        sim.set_fail_rate(0.15); // 15% message loss
        sim.set_max_message_latency(Duration::from_millis(30));

        sim.host("server", || async move { setup_server(9904).await });

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
                                    tracing::trace!(job_id = %task.job_id, "complete");
                                    completed += 1;
                                }
                                Err(_) => {
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
    });
}
