mod grpc_integration_helpers;

use std::net::SocketAddr;
use std::sync::Arc;

use grpc_integration_helpers::shutdown_server;
use silo::coordination::NoneCoordinator;
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::pb::silo_client::SiloClient;
use silo::pb::*;
use silo::server::run_server;
use silo::settings::{AppConfig, Backend, DatabaseTemplate};
use tokio::net::TcpListener;

/// Helper to set up a multi-shard test server using the production code path.
async fn setup_multi_shard_server(
    initial_shard_count: u32,
    config: AppConfig,
) -> anyhow::Result<(
    SiloClient<tonic::transport::Channel>,
    tokio::sync::broadcast::Sender<()>,
    tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    SocketAddr,
    Arc<ShardFactory>,
    tempfile::TempDir,
)> {
    let tmp = tempfile::tempdir()?;
    let template = DatabaseTemplate {
        backend: Backend::Fs,
        path: tmp.path().join("%shard%").to_string_lossy().to_string(),
        wal: None,
        apply_wal_on_close: true,
        slatedb: None,
    };
    let rate_limiter = MockGubernatorClient::new_arc();
    let factory = Arc::new(ShardFactory::new(template, rate_limiter, None));

    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    let coordinator = Arc::new(
        NoneCoordinator::new(
            "test-node",
            format!("http://{}", addr),
            initial_shard_count,
            factory.clone(),
            Vec::new(),
        )
        .await
        .unwrap(),
    );

    let server = tokio::spawn(run_server(
        listener,
        factory.clone(),
        coordinator,
        config,
        None,
        shutdown_rx,
    ));

    let endpoint = format!("http://{}", addr);
    let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
    let client = SiloClient::new(channel);

    Ok((client, shutdown_tx, server, addr, factory, tmp))
}

/// This test reproduces the silo-bench routing bug: when the bench tool discovers
/// cluster topology, it ignores the shard range information and picks a random shard
/// for each tenant. With multiple shards, the tenant is likely not in that shard's
/// range, resulting in FailedPrecondition errors like:
///   "tenant 'bench-0' is not within shard X range [4, 8); refresh topology and retry"
///
/// The test sets up 8 shards and then attempts to enqueue tenants "bench-0" through
/// "bench-3" using the WRONG routing approach (random shard selection), demonstrating
/// that most tenant+shard combinations fail.
///
/// After fixing the bench tool to use the range information from GetClusterInfo,
/// this test verifies that routing tenants to the correct shard always succeeds.
#[silo::test(flavor = "multi_thread")]
async fn bench_tenant_routing_with_shard_ranges() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let mut cfg = AppConfig::load(None).unwrap();
        cfg.tenancy.enabled = true;

        let (mut client, shutdown_tx, server, _addr, _factory, _tmp) =
            setup_multi_shard_server(8, cfg).await?;

        // Discover cluster topology (same as silo-bench does)
        let cluster_info = client
            .get_cluster_info(GetClusterInfoRequest {})
            .await?
            .into_inner();
        assert_eq!(cluster_info.shard_owners.len(), 8, "should have 8 shards");

        // Verify that range information is present in the response
        for owner in &cluster_info.shard_owners {
            // At least one of range_start or range_end should be non-empty
            // (first shard has empty start, last has empty end, middle shards have both)
            assert!(
                !owner.range_start.is_empty() || !owner.range_end.is_empty(),
                "shard {} should have range information, got range_start='{}' range_end='{}'",
                owner.shard_id,
                owner.range_start,
                owner.range_end,
            );
        }

        // The bench tenants: "bench-0", "bench-1", "bench-2", "bench-3"
        let tenants = vec!["bench-0", "bench-1", "bench-2", "bench-3"];

        // For each tenant, find the shard that owns it by checking ranges
        // This is what silo-bench SHOULD do (but didn't before the fix)
        for tenant in &tenants {
            let owning_shard = cluster_info
                .shard_owners
                .iter()
                .find(|owner| {
                    let after_start =
                        owner.range_start.is_empty() || *tenant >= owner.range_start.as_str();
                    let before_end =
                        owner.range_end.is_empty() || *tenant < owner.range_end.as_str();
                    after_start && before_end
                })
                .unwrap_or_else(|| {
                    panic!("tenant '{}' should be owned by some shard", tenant)
                });

            // Enqueue to the correct shard -- this should succeed
            let result = client
                .enqueue(EnqueueRequest {
                    shard: owning_shard.shard_id.clone(),
                    id: format!("correct-routing-{}", tenant),
                    priority: 5,
                    start_at_ms: 0,
                    retry_policy: None,
                    payload: Some(SerializedBytes {
                        encoding: Some(serialized_bytes::Encoding::Msgpack(
                            rmp_serde::to_vec(&serde_json::json!({"tenant": tenant})).unwrap(),
                        )),
                    }),
                    limits: vec![],
                    tenant: Some(tenant.to_string()),
                    metadata: std::collections::HashMap::new(),
                    task_group: "default".to_string(),
                })
                .await;

            assert!(
                result.is_ok(),
                "enqueue to correct shard should succeed for tenant '{}' on shard '{}' (range [{}, {})): {:?}",
                tenant,
                owning_shard.shard_id,
                owning_shard.range_start,
                owning_shard.range_end,
                result.err()
            );
        }

        // Now demonstrate the bug: if we pick the WRONG shard for a tenant,
        // the server rejects it with FailedPrecondition
        let first_shard = &cluster_info.shard_owners[0];
        // Find a tenant NOT in the first shard's range
        let wrong_tenant = tenants.iter().find(|tenant| {
            let after_start =
                first_shard.range_start.is_empty() || **tenant >= first_shard.range_start.as_str();
            let before_end =
                first_shard.range_end.is_empty() || **tenant < first_shard.range_end.as_str();
            !(after_start && before_end)
        });

        if let Some(wrong_tenant) = wrong_tenant {
            let result = client
                .enqueue(EnqueueRequest {
                    shard: first_shard.shard_id.clone(),
                    id: format!("wrong-routing-{}", wrong_tenant),
                    priority: 5,
                    start_at_ms: 0,
                    retry_policy: None,
                    payload: Some(SerializedBytes {
                        encoding: Some(serialized_bytes::Encoding::Msgpack(
                            rmp_serde::to_vec(&serde_json::json!({"tenant": wrong_tenant}))
                                .unwrap(),
                        )),
                    }),
                    limits: vec![],
                    tenant: Some(wrong_tenant.to_string()),
                    metadata: std::collections::HashMap::new(),
                    task_group: "default".to_string(),
                })
                .await;

            assert!(
                result.is_err(),
                "enqueue to WRONG shard should fail for tenant '{}' on shard '{}' (range [{}, {}))",
                wrong_tenant,
                first_shard.shard_id,
                first_shard.range_start,
                first_shard.range_end,
            );
            let status = result.unwrap_err();
            assert_eq!(
                status.code(),
                tonic::Code::FailedPrecondition,
                "should get FailedPrecondition, got: {}",
                status
            );
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}
