mod grpc_integration_helpers;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::join_all;
use grpc_integration_helpers::{setup_multi_shard_server, shutdown_server};
use silo::pb::silo_admin_server::{SiloAdmin, SiloAdminServer};
use silo::pb::silo_server::{Silo, SiloServer};
use silo::pb::*;
use silo::routing_client::{ErrorAction, RoutingClient};
use silo::settings::AppConfig;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct CountingClusterInfoService {
    grpc_addr: String,
    request_count: Arc<AtomicUsize>,
    response_delay: Duration,
}

impl CountingClusterInfoService {
    fn unimplemented<T>(method: &'static str) -> Result<Response<T>, Status> {
        Err(Status::unimplemented(method))
    }
}

#[tonic::async_trait]
impl Silo for CountingClusterInfoService {
    type QueryArrowStream = futures::stream::Empty<Result<ArrowIpcMessage, Status>>;

    async fn get_cluster_info(
        &self,
        _request: Request<GetClusterInfoRequest>,
    ) -> Result<Response<GetClusterInfoResponse>, Status> {
        self.request_count.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.response_delay).await;

        Ok(Response::new(GetClusterInfoResponse {
            num_shards: 1,
            shard_owners: vec![ShardOwner {
                shard_id: "counting-shard".to_string(),
                grpc_addr: self.grpc_addr.clone(),
                node_id: "counting-node".to_string(),
                range_start: String::new(),
                range_end: String::new(),
                placement_ring: None,
            }],
            this_node_id: "counting-node".to_string(),
            this_grpc_addr: self.grpc_addr.clone(),
            members: vec![],
        }))
    }

    async fn enqueue(
        &self,
        _request: Request<EnqueueRequest>,
    ) -> Result<Response<EnqueueResponse>, Status> {
        Self::unimplemented("enqueue")
    }

    async fn get_job(
        &self,
        _request: Request<GetJobRequest>,
    ) -> Result<Response<GetJobResponse>, Status> {
        Self::unimplemented("get_job")
    }

    async fn get_job_result(
        &self,
        _request: Request<GetJobResultRequest>,
    ) -> Result<Response<GetJobResultResponse>, Status> {
        Self::unimplemented("get_job_result")
    }

    async fn delete_job(
        &self,
        _request: Request<DeleteJobRequest>,
    ) -> Result<Response<DeleteJobResponse>, Status> {
        Self::unimplemented("delete_job")
    }

    async fn cancel_job(
        &self,
        _request: Request<CancelJobRequest>,
    ) -> Result<Response<CancelJobResponse>, Status> {
        Self::unimplemented("cancel_job")
    }

    async fn restart_job(
        &self,
        _request: Request<RestartJobRequest>,
    ) -> Result<Response<RestartJobResponse>, Status> {
        Self::unimplemented("restart_job")
    }

    async fn expedite_job(
        &self,
        _request: Request<ExpediteJobRequest>,
    ) -> Result<Response<ExpediteJobResponse>, Status> {
        Self::unimplemented("expedite_job")
    }

    async fn lease_task(
        &self,
        _request: Request<LeaseTaskRequest>,
    ) -> Result<Response<LeaseTaskResponse>, Status> {
        Self::unimplemented("lease_task")
    }

    async fn lease_tasks(
        &self,
        _request: Request<LeaseTasksRequest>,
    ) -> Result<Response<LeaseTasksResponse>, Status> {
        Self::unimplemented("lease_tasks")
    }

    async fn report_outcome(
        &self,
        _request: Request<ReportOutcomeRequest>,
    ) -> Result<Response<ReportOutcomeResponse>, Status> {
        Self::unimplemented("report_outcome")
    }

    async fn report_refresh_outcome(
        &self,
        _request: Request<ReportRefreshOutcomeRequest>,
    ) -> Result<Response<ReportRefreshOutcomeResponse>, Status> {
        Self::unimplemented("report_refresh_outcome")
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        Self::unimplemented("heartbeat")
    }

    async fn query(
        &self,
        _request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        Self::unimplemented("query")
    }

    async fn query_arrow(
        &self,
        _request: Request<QueryArrowRequest>,
    ) -> Result<Response<Self::QueryArrowStream>, Status> {
        Err(Status::unimplemented("query_arrow"))
    }
}

#[tonic::async_trait]
impl SiloAdmin for CountingClusterInfoService {
    async fn get_node_info(
        &self,
        _request: Request<GetNodeInfoRequest>,
    ) -> Result<Response<GetNodeInfoResponse>, Status> {
        Self::unimplemented("get_node_info")
    }

    async fn cpu_profile(
        &self,
        _request: Request<CpuProfileRequest>,
    ) -> Result<Response<CpuProfileResponse>, Status> {
        Self::unimplemented("cpu_profile")
    }

    async fn request_split(
        &self,
        _request: Request<RequestSplitRequest>,
    ) -> Result<Response<RequestSplitResponse>, Status> {
        Self::unimplemented("request_split")
    }

    async fn get_split_status(
        &self,
        _request: Request<GetSplitStatusRequest>,
    ) -> Result<Response<GetSplitStatusResponse>, Status> {
        Self::unimplemented("get_split_status")
    }

    async fn configure_shard(
        &self,
        _request: Request<ConfigureShardRequest>,
    ) -> Result<Response<ConfigureShardResponse>, Status> {
        Self::unimplemented("configure_shard")
    }

    async fn import_jobs(
        &self,
        _request: Request<ImportJobsRequest>,
    ) -> Result<Response<ImportJobsResponse>, Status> {
        Self::unimplemented("import_jobs")
    }

    async fn reset_shards(
        &self,
        _request: Request<ResetShardsRequest>,
    ) -> Result<Response<ResetShardsResponse>, Status> {
        Self::unimplemented("reset_shards")
    }

    async fn force_release_shard(
        &self,
        _request: Request<ForceReleaseShardRequest>,
    ) -> Result<Response<ForceReleaseShardResponse>, Status> {
        Self::unimplemented("force_release_shard")
    }

    async fn compact_shard(
        &self,
        _request: Request<CompactShardRequest>,
    ) -> Result<Response<CompactShardResponse>, Status> {
        Self::unimplemented("compact_shard")
    }

    async fn read_lsm_state(
        &self,
        _request: Request<ReadLsmStateRequest>,
    ) -> Result<Response<ReadLsmStateResponse>, Status> {
        Self::unimplemented("read_lsm_state")
    }
}

async fn setup_counting_cluster_info_server(
    response_delay: Duration,
) -> anyhow::Result<(
    tokio::sync::broadcast::Sender<()>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    SocketAddr,
    Arc<AtomicUsize>,
)> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let addr = listener.local_addr()?;
    let request_count = Arc::new(AtomicUsize::new(0));
    let service = CountingClusterInfoService {
        grpc_addr: format!("http://{}", addr),
        request_count: request_count.clone(),
        response_delay,
    };
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    let server = tokio::spawn(async move {
        let incoming = async_stream::stream! {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => yield Ok::<_, std::io::Error>(stream),
                    Err(err) => {
                        yield Err(err);
                        break;
                    }
                }
            }
        };

        Server::builder()
            .add_service(SiloServer::new(service.clone()))
            .add_service(SiloAdminServer::new(service))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.recv().await;
            })
            .await
            .map_err(anyhow::Error::from)
    });

    Ok((shutdown_tx, server, addr, request_count))
}

async fn shutdown_counting_server(
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    server: tokio::task::JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let _ = shutdown_tx.send(());
    match server.await {
        Ok(inner) => inner,
        Err(err) => Err(anyhow::anyhow!(err)),
    }
}

/// Test that classify_error correctly identifies NOT_FOUND with redirect metadata.
#[silo::test(flavor = "multi_thread")]
async fn classify_error_not_found_with_redirect() {
    let mut status = tonic::Status::not_found("shard not found");
    status.metadata_mut().insert(
        "x-silo-shard-owner-addr",
        "http://other-node:7450".parse().unwrap(),
    );

    let action = silo::routing_client::classify_error(&status, "shard-123");
    match action {
        ErrorAction::Redirect { shard_id, new_addr } => {
            assert_eq!(shard_id, "shard-123");
            assert_eq!(new_addr, "http://other-node:7450");
        }
        other => panic!("expected Redirect, got {:?}", other),
    }
}

/// Test that classify_error correctly identifies NOT_FOUND without redirect as RefreshTopology.
#[silo::test(flavor = "multi_thread")]
async fn classify_error_not_found_without_redirect() {
    let status = tonic::Status::not_found("shard not found");
    let action = silo::routing_client::classify_error(&status, "shard-123");
    assert!(
        matches!(action, ErrorAction::RefreshTopology),
        "expected RefreshTopology, got {:?}",
        action
    );
}

/// Test that classify_error correctly identifies FailedPrecondition as RefreshTopology.
#[silo::test(flavor = "multi_thread")]
async fn classify_error_failed_precondition() {
    let status = tonic::Status::failed_precondition(
        "tenant 'bench-0' is not within shard X range [4, 8); refresh topology and retry",
    );
    let action = silo::routing_client::classify_error(&status, "shard-456");
    assert!(
        matches!(action, ErrorAction::RefreshTopology),
        "expected RefreshTopology, got {:?}",
        action
    );
}

/// Test that classify_error returns RetryWithBackoff for UNAVAILABLE errors.
#[silo::test(flavor = "multi_thread")]
async fn classify_error_unavailable() {
    let status = tonic::Status::unavailable("shard temporarily unavailable");
    let action = silo::routing_client::classify_error(&status, "shard-456");
    assert!(
        matches!(action, ErrorAction::RetryWithBackoff),
        "expected RetryWithBackoff, got {:?}",
        action
    );
}

/// Test that classify_error returns NotRoutingError for other error types.
#[silo::test(flavor = "multi_thread")]
async fn classify_error_other_errors() {
    let status = tonic::Status::internal("database error");
    let action = silo::routing_client::classify_error(&status, "shard-789");
    assert!(
        matches!(action, ErrorAction::NotRoutingError),
        "expected NotRoutingError, got {:?}",
        action
    );
}

/// Test that RoutingClient discovers topology and can route tenants to correct shards.
#[silo::test(flavor = "multi_thread")]
async fn routing_client_discovers_topology() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let mut cfg = AppConfig::load(None).unwrap();
        cfg.tenancy.enabled = true;

        let (shutdown_tx, server, addr, _factory, _tmp) = setup_multi_shard_server(4, cfg).await?;

        let client = RoutingClient::discover(&format!("http://{}", addr)).await?;
        let topo = client.topology();

        assert_eq!(topo.shard_owners.len(), 4, "should discover 4 shards");

        // Every tenant should map to exactly one shard
        for tenant in &["bench-0", "bench-1", "bench-2", "bench-3"] {
            let owner = topo.shard_for_tenant(tenant);
            assert!(
                owner.is_some(),
                "tenant '{}' should have an owning shard",
                tenant
            );
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that RoutingClient can successfully enqueue to the correct shard via tenant routing.
#[silo::test(flavor = "multi_thread")]
async fn routing_client_enqueue_correct_shard() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let mut cfg = AppConfig::load(None).unwrap();
        cfg.tenancy.enabled = true;

        let (shutdown_tx, server, addr, _factory, _tmp) = setup_multi_shard_server(4, cfg).await?;

        let client = RoutingClient::discover(&format!("http://{}", addr)).await?;

        // Enqueue to correct shard for each tenant - should all succeed
        for tenant in &["bench-0", "bench-1", "bench-2", "bench-3"] {
            let (mut silo_client, shard) = client.client_for_tenant(tenant)?;

            let result = silo_client
                .enqueue(EnqueueRequest {
                    shard: shard.clone(),
                    id: format!("test-{}", tenant),
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
                "enqueue should succeed for tenant '{}' on shard '{}': {:?}",
                tenant,
                shard,
                result.err()
            );
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that sending to the wrong shard produces a FailedPrecondition error,
/// and that handle_routing_error triggers a topology refresh that allows
/// subsequent requests to succeed.
#[silo::test(flavor = "multi_thread")]
async fn routing_client_handles_failed_precondition_with_refresh() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let mut cfg = AppConfig::load(None).unwrap();
        cfg.tenancy.enabled = true;

        let (shutdown_tx, server, addr, _factory, _tmp) = setup_multi_shard_server(8, cfg).await?;

        let client = RoutingClient::discover(&format!("http://{}", addr)).await?;
        let topo = client.topology();

        // Find a tenant that is NOT in the first shard's range
        let first_shard = &topo.shard_owners[0];
        let tenants = ["bench-0", "bench-1", "bench-2", "bench-3"];
        let wrong_tenant = tenants.iter().find(|tenant| {
            topo.shard_for_tenant(tenant)
                .map(|owner| owner.shard_id != first_shard.shard_id)
                .unwrap_or(true)
        });

        let wrong_tenant = match wrong_tenant {
            Some(t) => *t,
            None => {
                // All tenants map to the first shard (unlikely with 8 shards)
                shutdown_server(shutdown_tx, server).await?;
                return Ok(());
            }
        };

        // Send to the wrong shard deliberately
        let (mut silo_client, _correct_shard) = client.client_for_tenant(wrong_tenant)?;

        let result = silo_client
            .enqueue(EnqueueRequest {
                shard: first_shard.shard_id.clone(), // WRONG shard for this tenant
                id: "wrong-shard-test".to_string(),
                priority: 5,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({"test": true})).unwrap(),
                    )),
                }),
                limits: vec![],
                tenant: Some(wrong_tenant.to_string()),
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            })
            .await;

        // Should fail with FailedPrecondition
        assert!(result.is_err(), "enqueue to wrong shard should fail");
        let err = result.unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::FailedPrecondition,
            "should get FailedPrecondition, got: {}",
            err
        );

        // handle_routing_error should trigger a topology refresh and return Some
        let handled = client
            .handle_routing_error(&err, &first_shard.shard_id)
            .await;
        assert!(
            handled.is_some(),
            "handle_routing_error should succeed for FailedPrecondition"
        );

        // After refresh, routing to the correct shard via client_for_tenant should work
        let (mut silo_client, shard) = client.client_for_tenant(wrong_tenant)?;
        let result = silo_client
            .enqueue(EnqueueRequest {
                shard: shard.clone(),
                id: "after-refresh-test".to_string(),
                priority: 5,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(SerializedBytes {
                    encoding: Some(serialized_bytes::Encoding::Msgpack(
                        rmp_serde::to_vec(&serde_json::json!({"test": true})).unwrap(),
                    )),
                }),
                limits: vec![],
                tenant: Some(wrong_tenant.to_string()),
                metadata: std::collections::HashMap::new(),
                task_group: "default".to_string(),
            })
            .await;

        assert!(
            result.is_ok(),
            "enqueue should succeed after topology refresh: {:?}",
            result.err()
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that refresh_topology picks up the current cluster state.
#[silo::test(flavor = "multi_thread")]
async fn routing_client_refresh_topology() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let mut cfg = AppConfig::load(None).unwrap();
        cfg.tenancy.enabled = true;

        let (shutdown_tx, server, addr, _factory, _tmp) = setup_multi_shard_server(4, cfg).await?;

        let client = RoutingClient::discover(&format!("http://{}", addr)).await?;

        // Initial topology should have 4 shards
        let topo = client.topology();
        assert_eq!(topo.shard_owners.len(), 4);

        // Refresh topology - should still have 4 shards (unchanged cluster)
        let refreshed = client.refresh_topology().await;
        assert!(refreshed, "refresh_topology should succeed");

        let topo = client.topology();
        assert_eq!(
            topo.shard_owners.len(),
            4,
            "should still have 4 shards after refresh"
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that handle_routing_error correctly processes a redirect by updating the
/// shard-to-address override. Even though the redirect target won't exist in this
/// single-node test, the topology update should be visible.
#[silo::test(flavor = "multi_thread")]
async fn routing_client_handles_redirect_metadata() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let mut cfg = AppConfig::load(None).unwrap();
        cfg.tenancy.enabled = true;

        let (shutdown_tx, server, addr, _factory, _tmp) = setup_multi_shard_server(4, cfg).await?;

        let client = RoutingClient::discover(&format!("http://{}", addr)).await?;
        let topo = client.topology();
        let first_shard_id = topo.shard_owners[0].shard_id.clone();
        let original_addr = topo.address_for_shard(&first_shard_id).unwrap().to_string();

        // Simulate a NOT_FOUND with redirect metadata (as if the server told us the shard moved)
        let mut status = tonic::Status::not_found("shard not found");
        let redirect_addr = format!("http://127.0.0.1:{}", 19999);
        status
            .metadata_mut()
            .insert("x-silo-shard-owner-addr", redirect_addr.parse().unwrap());

        let handled = client.handle_routing_error(&status, &first_shard_id).await;
        assert!(handled.is_some(), "redirect metadata should be handled");

        let topo = client.topology();
        let new_addr = topo.address_for_shard(&first_shard_id).unwrap().to_string();
        assert_eq!(
            new_addr, redirect_addr,
            "shard address should be updated to redirect target"
        );
        assert_ne!(
            new_addr, original_addr,
            "address should differ from original"
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that redirect metadata teaches the client a new discovery address so a
/// later topology refresh can recover even after the original discovery node is gone.
#[silo::test(flavor = "multi_thread")]
async fn routing_client_refreshes_via_redirect_discovery_address() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let mut cfg_a = AppConfig::load(None).unwrap();
        cfg_a.tenancy.enabled = true;

        let mut cfg_b = AppConfig::load(None).unwrap();
        cfg_b.tenancy.enabled = true;

        let (shutdown_tx_a, server_a, addr_a, _factory_a, _tmp_a) =
            setup_multi_shard_server(1, cfg_a).await?;
        let client = RoutingClient::discover(&format!("http://{}", addr_a)).await?;
        let shard_id = client.topology().shard_owners[0].shard_id.clone();

        let (shutdown_tx_b, server_b, addr_b, _factory_b, _tmp_b) =
            setup_multi_shard_server(1, cfg_b).await?;

        let redirect_addr = format!("http://{}", addr_b);
        let mut status = tonic::Status::not_found("shard moved");
        status
            .metadata_mut()
            .insert("x-silo-shard-owner-addr", redirect_addr.parse().unwrap());

        let handled = client.handle_routing_error(&status, &shard_id).await;
        assert!(handled.is_some(), "redirect metadata should be handled");

        shutdown_server(shutdown_tx_a, server_a).await?;

        let refreshed = client.refresh_topology().await;
        assert!(
            refreshed,
            "refresh_topology should succeed via the redirect-learned address"
        );

        let topo = client.topology();
        assert_eq!(
            topo.this_grpc_addr, redirect_addr,
            "refresh should use the redirect-learned discovery address"
        );

        shutdown_server(shutdown_tx_b, server_b).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that concurrent refresh callers share one GetClusterInfo refresh RPC.
#[silo::test(flavor = "multi_thread")]
async fn routing_client_coalesces_concurrent_refreshes() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let (shutdown_tx, server, addr, request_count) =
            setup_counting_cluster_info_server(Duration::from_millis(150)).await?;

        let client = RoutingClient::discover(&format!("http://{}", addr)).await?;
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            1,
            "discovery should issue one GetClusterInfo RPC"
        );

        let caller_count = 8;
        let barrier = Arc::new(tokio::sync::Barrier::new(caller_count + 1));
        let refreshes = (0..caller_count)
            .map(|_| {
                let client = client.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    client.refresh_topology().await
                })
            })
            .collect::<Vec<_>>();

        barrier.wait().await;

        for result in join_all(refreshes).await {
            assert!(
                result.expect("refresh task should join"),
                "refresh_topology should succeed"
            );
        }

        assert_eq!(
            request_count.load(Ordering::SeqCst),
            2,
            "concurrent refreshes should share a single GetClusterInfo RPC"
        );

        shutdown_counting_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that the RoutingClient connection pool shares connections across
/// multiple client_for_tenant calls to the same shard.
#[silo::test(flavor = "multi_thread")]
async fn routing_client_connection_pool_reuse() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let mut cfg = AppConfig::load(None).unwrap();
        cfg.tenancy.enabled = true;

        let (shutdown_tx, server, addr, _factory, _tmp) = setup_multi_shard_server(4, cfg).await?;

        let client = RoutingClient::discover(&format!("http://{}", addr)).await?;

        // Call client_for_tenant many times - all should succeed and reuse connections
        for _ in 0..20 {
            let (_silo_client, _shard) = client.client_for_tenant("bench-0")?;
        }

        // The connection pool should have exactly the number of unique addresses
        // (1 server in this test)
        let all_addrs = client.all_addresses();
        assert_eq!(all_addrs.len(), 1, "should have exactly 1 unique address");

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that invalidate_connection removes a cached connection, forcing a fresh
/// one to be created on the next request.
#[silo::test(flavor = "multi_thread")]
async fn routing_client_invalidate_connection() -> anyhow::Result<()> {
    tokio::time::timeout(std::time::Duration::from_millis(15000), async {
        let mut cfg = AppConfig::load(None).unwrap();
        cfg.tenancy.enabled = true;

        let (shutdown_tx, server, addr, _factory, _tmp) = setup_multi_shard_server(4, cfg).await?;

        let server_addr = format!("http://{}", addr);
        let client = RoutingClient::discover(&server_addr).await?;

        // Ensure a connection exists by making a request
        let (mut silo_client, _shard) = client.client_for_tenant("bench-0")?;
        let topo = silo_client
            .get_cluster_info(silo::pb::GetClusterInfoRequest {})
            .await;
        assert!(topo.is_ok(), "initial request should succeed");

        // Invalidate the connection
        client.invalidate_connection(&server_addr);

        // Next request should still work — a fresh lazy connection is created
        let (mut silo_client, _shard) = client.client_for_tenant("bench-0")?;
        let topo = silo_client
            .get_cluster_info(silo::pb::GetClusterInfoRequest {})
            .await;
        assert!(
            topo.is_ok(),
            "request after invalidation should succeed: {:?}",
            topo.err()
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}
