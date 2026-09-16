mod grpc_integration_helpers;
mod test_helpers;

use std::sync::Arc;

use grpc_integration_helpers::{setup_multi_shard_server, shutdown_server};
use silo::job_store_shard::JobStoreShard;
use silo::pb::silo_client::SiloClient;
use silo::pb::*;
use silo::settings::AppConfig;

/// A tenant whose hash lands in `shard`'s range, taken from a small pool.
fn tenant_on(shard: &JobStoreShard) -> &'static str {
    ["aaa", "bbb", "lll", "mmm", "nnn", "yyy"]
        .into_iter()
        .find(|t| shard.get_range().contains_tenant(t))
        .expect("one of the pool tenants hashes into the shard")
}

async fn enqueue_floating(shard: &JobStoreShard, tenant: &str, queue: &str, tag: u32) {
    shard
        .enqueue(
            tenant,
            Some(format!("{queue}-job-{tag}")),
            10u8,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"j": tag})),
            vec![silo::job::Limit::FloatingConcurrency(
                silo::job::FloatingConcurrencyLimit {
                    key: queue.to_string(),
                    default_max_concurrency: 1,
                    refresh_interval_ms: 100,
                    metadata: vec![],
                },
            )],
            None,
            "default",
        )
        .await
        .expect("enqueue floating job");
}

/// A lease request whose job budget is met by one shard still returns a
/// pending refresh from another local shard for the same task group.
#[silo::test(flavor = "multi_thread")]
async fn lease_tasks_drains_refreshes_from_shards_it_did_not_dequeue() -> anyhow::Result<()> {
    let (shutdown_tx, server, addr, factory, _tmp) =
        setup_multi_shard_server(2, AppConfig::load(None).unwrap()).await?;
    let shards: Vec<Arc<JobStoreShard>> = factory.instances().values().cloned().collect();
    assert_eq!(shards.len(), 2);
    let (job_shard, refresh_shard) = (&shards[0], &shards[1]);
    for shard in &shards {
        shard.stop_grant_scanner();
    }

    // One plain due job on the first shard fills a budget of one.
    job_shard
        .enqueue(
            tenant_on(job_shard),
            Some("budget-job".to_string()),
            10u8,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await?;

    // On the second shard the holder is leased first, so its only pending
    // work under `default` is the refresh the waiter's enqueue scheduled.
    let tenant = tenant_on(refresh_shard);
    let queue = "fl-fan-out-q";
    enqueue_floating(refresh_shard, tenant, queue, 1).await;
    let holder = refresh_shard.dequeue("setup", "default", 10).await?;
    assert_eq!(holder.tasks.len(), 1);
    assert!(holder.refresh_tasks.is_empty());
    enqueue_floating(refresh_shard, tenant, queue, 2).await;

    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
    let mut client = SiloClient::new(channel);
    let resp = client
        .lease_tasks(LeaseTasksRequest {
            shard: None,
            worker_id: "w1".to_string(),
            max_tasks: 1,
            task_group: "default".to_string(),
        })
        .await?
        .into_inner();

    assert_eq!(resp.tasks.len(), 1, "the job budget is filled");
    assert_eq!(resp.tasks[0].shard, job_shard.name());
    assert_eq!(
        resp.refresh_tasks.len(),
        1,
        "the other shard's pending refresh rides along"
    );
    assert_eq!(resp.refresh_tasks[0].queue_key, queue);
    assert_eq!(resp.refresh_tasks[0].shard, refresh_shard.name());

    shutdown_server(shutdown_tx, server).await?;
    Ok(())
}
