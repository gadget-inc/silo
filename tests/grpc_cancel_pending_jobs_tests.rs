mod grpc_integration_helpers;

use grpc_integration_helpers::{
    TEST_SHARD_ID, create_test_factory, setup_test_server, shutdown_server,
};
use silo::pb::*;
use silo::settings::AppConfig;

fn test_config() -> anyhow::Result<AppConfig> {
    let mut config = AppConfig::load(None)?;
    config.tenancy.enabled = true;
    Ok(config)
}

fn enqueue(id: &str, tenant: &str) -> EnqueueRequest {
    EnqueueRequest {
        shard: TEST_SHARD_ID.into(),
        id: id.into(),
        tenant: Some(tenant.into()),
        task_group: "default".into(),
        ..Default::default()
    }
}

fn cancel(ids: &[&str], tenant: &str) -> CancelPendingJobsRequest {
    CancelPendingJobsRequest {
        shard: TEST_SHARD_ID.into(),
        ids: ids.iter().map(|id| (*id).into()).collect(),
        tenant: Some(tenant.into()),
    }
}

fn lease(id: &str, tenant: &str) -> LeaseTaskRequest {
    LeaseTaskRequest {
        shard: TEST_SHARD_ID.into(),
        id: id.into(),
        tenant: Some(tenant.into()),
        worker_id: "worker".into(),
    }
}

#[silo::test(flavor = "multi_thread")]
async fn batch_cancel_returns_ordered_results_and_preserves_running_jobs() -> anyhow::Result<()> {
    let (factory, _tmp) = create_test_factory().await?;
    let (mut client, shutdown_tx, server, _) = setup_test_server(factory, test_config()?).await?;
    for id in ["pending", "running", "completed", "cancelled"] {
        client.enqueue(enqueue(id, "tenant-a")).await?;
    }
    client.enqueue(enqueue("pending", "tenant-b")).await?;
    let running = client
        .lease_task(lease("running", "tenant-a"))
        .await?
        .into_inner()
        .task
        .unwrap();
    let completed = client
        .lease_task(lease("completed", "tenant-a"))
        .await?
        .into_inner()
        .task
        .unwrap();
    client
        .report_outcome(ReportOutcomeRequest {
            shard: TEST_SHARD_ID.into(),
            task_id: completed.id,
            tenant_id: Some("tenant-a".into()),
            outcome: Some(report_outcome_request::Outcome::Success(
                SerializedBytes::default(),
            )),
        })
        .await?;
    client
        .cancel_job(CancelJobRequest {
            shard: TEST_SHARD_ID.into(),
            id: "cancelled".into(),
            tenant: Some("tenant-a".into()),
        })
        .await?;

    let ids = ["pending", "missing", "running", "completed", "cancelled"];
    let results = client
        .cancel_pending_jobs(cancel(&ids, "tenant-a"))
        .await?
        .into_inner()
        .results;
    assert_eq!(
        results
            .iter()
            .map(|result| result.id.as_str())
            .collect::<Vec<_>>(),
        ids
    );
    assert_eq!(
        results
            .iter()
            .map(|result| result.status())
            .collect::<Vec<_>>(),
        vec![
            CancelPendingJobStatus::Cancelled,
            CancelPendingJobStatus::NotFound,
            CancelPendingJobStatus::Running,
            CancelPendingJobStatus::Terminal,
            CancelPendingJobStatus::AlreadyCancelled,
        ]
    );
    assert!(results.iter().all(|result| result.error.is_none()));
    assert_eq!(
        client
            .cancel_pending_jobs(cancel(&["pending"], "tenant-a"))
            .await?
            .into_inner()
            .results[0]
            .status(),
        CancelPendingJobStatus::AlreadyCancelled
    );

    let heartbeat = client
        .heartbeat(HeartbeatRequest {
            shard: TEST_SHARD_ID.into(),
            worker_id: "worker".into(),
            task_id: running.id,
            tenant_id: Some("tenant-a".into()),
        })
        .await?
        .into_inner();
    assert!(
        !heartbeat.cancelled,
        "pending-only cancellation must never signal running workers"
    );
    assert!(
        client
            .lease_task(lease("pending", "tenant-a"))
            .await
            .is_err()
    );
    assert!(
        client
            .lease_task(lease("pending", "tenant-b"))
            .await?
            .into_inner()
            .task
            .is_some()
    );
    shutdown_server(shutdown_tx, server).await
}

#[silo::test(flavor = "multi_thread")]
async fn batch_cancel_validates_the_whole_request_before_mutating() -> anyhow::Result<()> {
    let (factory, _tmp) = create_test_factory().await?;
    let (mut client, shutdown_tx, server, _) = setup_test_server(factory, test_config()?).await?;
    client.enqueue(enqueue("pending", "tenant")).await?;
    let mut oversized = cancel(&["pending"], "tenant");
    oversized.ids.extend((0..1000).map(|i| format!("job-{i}")));
    for request in [
        cancel(&[], "tenant"),
        cancel(&["pending", ""], "tenant"),
        cancel(&["pending", "pending"], "tenant"),
        oversized,
    ] {
        assert_eq!(
            client
                .cancel_pending_jobs(request)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    assert!(
        client
            .lease_task(lease("pending", "tenant"))
            .await?
            .into_inner()
            .task
            .is_some()
    );
    shutdown_server(shutdown_tx, server).await
}

#[silo::test(flavor = "multi_thread")]
async fn batch_cancel_racing_worker_dequeue_never_dispatches_cancelled_jobs() -> anyhow::Result<()>
{
    let (factory, _tmp) = create_test_factory().await?;
    let (mut client, shutdown_tx, server, _) = setup_test_server(factory, test_config()?).await?;
    let mut worker = client.clone();
    for i in 0..10 {
        let task_group = format!("worker-race-{i}");
        let ids: Vec<String> = (0..30).map(|j| format!("worker-race-{i}-{j}")).collect();
        for id in &ids {
            let mut request = enqueue(id, "tenant");
            request.task_group = task_group.clone();
            client.enqueue(request).await?;
        }
        let request = CancelPendingJobsRequest {
            shard: TEST_SHARD_ID.into(),
            ids,
            tenant: Some("tenant".into()),
        };
        let (cancelled, leased) = tokio::join!(
            client.cancel_pending_jobs(request),
            worker.lease_tasks(LeaseTasksRequest {
                shard: Some(TEST_SHARD_ID.into()),
                worker_id: "worker".into(),
                max_tasks: 30,
                task_group,
            }),
        );
        let results = cancelled?.into_inner().results;
        for task in leased?.into_inner().tasks {
            assert_eq!(
                results
                    .iter()
                    .find(|result| result.id == task.job_id)
                    .unwrap()
                    .status(),
                CancelPendingJobStatus::Running,
                "a production worker must never receive a job cancelled by the pending-only RPC"
            );
            assert!(
                !worker
                    .heartbeat(HeartbeatRequest {
                        shard: TEST_SHARD_ID.into(),
                        worker_id: "worker".into(),
                        task_id: task.id,
                        tenant_id: Some("tenant".into()),
                    })
                    .await?
                    .into_inner()
                    .cancelled
            );
        }
    }
    shutdown_server(shutdown_tx, server).await
}

#[silo::test(flavor = "multi_thread")]
async fn batch_cancel_racing_a_lease_never_cancels_a_running_attempt() -> anyhow::Result<()> {
    let (factory, _tmp) = create_test_factory().await?;
    let (mut client, shutdown_tx, server, _) = setup_test_server(factory, test_config()?).await?;
    let mut worker = client.clone();
    for i in 0..20 {
        let id = format!("racing-{i}");
        client.enqueue(enqueue(&id, "tenant")).await?;
        let (cancelled, leased) = tokio::join!(
            client.cancel_pending_jobs(cancel(&[&id], "tenant")),
            worker.lease_task(lease(&id, "tenant")),
        );
        let result = cancelled?.into_inner().results.remove(0);
        match result.status() {
            CancelPendingJobStatus::Cancelled => assert!(leased.is_err()),
            CancelPendingJobStatus::Running => {
                let task = leased?.into_inner().task.unwrap();
                let heartbeat = worker
                    .heartbeat(HeartbeatRequest {
                        shard: TEST_SHARD_ID.into(),
                        worker_id: "worker".into(),
                        task_id: task.id,
                        tenant_id: Some("tenant".into()),
                    })
                    .await?
                    .into_inner();
                assert!(!heartbeat.cancelled);
            }
            other => panic!("unexpected cancellation result: {other:?}"),
        }
    }
    shutdown_server(shutdown_tx, server).await
}
