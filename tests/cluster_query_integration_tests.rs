//! Cluster query integration tests with etcd coordinator
//!
//! These tests start multiple nodes with etcd coordination and verify
//! cross-cluster queries work correctly.

mod test_helpers;

use datafusion::arrow::array::{Int64Array, StringArray};
use silo::cluster_query::ClusterQueryEngine;
use silo::coordination::Coordinator;
use silo::coordination::etcd::EtcdCoordinator;
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::pb::silo_client::SiloClient;
use silo::pb::{EnqueueRequest, SerializedBytes, serialized_bytes};
use silo::server::run_server;
use silo::settings::{Backend, DatabaseTemplate};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Channel;
use uuid::Uuid;

const NUM_SHARDS: u32 = 16;

/// Generate a unique cluster prefix to avoid test interference
fn unique_prefix() -> String {
    format!("test-cluster-query-{}", Uuid::new_v4())
}

/// Create a test shard factory with memory backend.
/// Uses a unique prefix to avoid path collisions between parallel tests.
fn make_test_factory(prefix: &str, node_id: &str) -> Arc<ShardFactory> {
    let tmpdir =
        std::env::temp_dir().join(format!("silo-cluster-query-test-{}-{}", prefix, node_id));
    Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Memory,
            path: tmpdir.join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
            slatedb: None,
        },
        MockGubernatorClient::new_arc(),
        None,
    ))
}

/// Get a gRPC client for a node
async fn get_client(addr: &str) -> SiloClient<Channel> {
    let channel = Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .expect("failed to connect to node");
    SiloClient::new(channel)
}

/// Enqueue a job via gRPC to a specific shard.
/// Retries on "shard not ready" errors which can happen during shard acquisition.
async fn enqueue_job(
    client: &mut SiloClient<Channel>,
    shard: &str,
    job_id: &str,
    tenant: Option<&str>,
) {
    let max_retries = 10;
    let mut last_error = None;

    for attempt in 0..max_retries {
        let request = EnqueueRequest {
            shard: shard.to_string(),
            id: job_id.to_string(),
            priority: 5,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(
                    rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                )),
            }),
            limits: vec![],
            tenant: tenant.map(|s| s.to_string()),
            metadata: HashMap::new(),
            task_group: "default".to_string(),
        };

        match client.enqueue(request).await {
            Ok(_) => return,
            Err(e) => {
                let msg = e.message();
                // Retry on transient "shard not ready" errors during acquisition
                if msg.contains("shard not ready") || msg.contains("acquisition in progress") {
                    last_error = Some(e);
                    tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                    continue;
                }
                panic!("failed to enqueue job: {}", e);
            }
        }
    }

    panic!(
        "failed to enqueue job after {} retries: {:?}",
        max_retries, last_error
    );
}

/// Helper struct to manage routing to correct nodes based on shard ownership
struct ClusterClients {
    client1: SiloClient<Channel>,
    client2: SiloClient<Channel>,
    node1_shards: Vec<String>,
    node2_shards: Vec<String>,
}

impl ClusterClients {
    async fn new(addr1: &str, addr2: &str, coordinator: &EtcdCoordinator) -> Self {
        let client1 = get_client(addr1).await;
        let client2 = get_client(addr2).await;

        // Get shard owner map to know which node owns which shards
        let owner_map = coordinator
            .get_shard_owner_map()
            .await
            .expect("get owner map");

        let mut node1_shards = Vec::new();
        let mut node2_shards = Vec::new();

        for (shard_id, addr) in &owner_map.shard_to_addr {
            if addr.contains(&addr1.replace("127.0.0.1:", "")) || addr == addr1 {
                node1_shards.push(shard_id.to_string());
            } else {
                node2_shards.push(shard_id.to_string());
            }
        }

        node1_shards.sort();
        node2_shards.sort();

        Self {
            client1,
            client2,
            node1_shards,
            node2_shards,
        }
    }

    /// Get the client for the node that owns the given shard
    fn client_for_shard(&mut self, shard: &str) -> &mut SiloClient<Channel> {
        if self.node1_shards.contains(&shard.to_string()) {
            &mut self.client1
        } else {
            &mut self.client2
        }
    }

    /// Get a shard owned by node 1
    fn shard_on_node1(&self) -> String {
        self.node1_shards
            .first()
            .expect("node1 should own at least one shard")
            .clone()
    }

    /// Get a shard owned by node 2
    fn shard_on_node2(&self) -> String {
        self.node2_shards
            .first()
            .expect("node2 should own at least one shard")
            .clone()
    }
}

#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_two_nodes_basic_query() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load config");

    let factory1 = make_test_factory(&prefix, "n1");
    let factory2 = make_test_factory(&prefix, "n2");

    // Bind listeners first to get available ports
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap().to_string();
    let addr2 = listener2.local_addr().unwrap().to_string();

    // Start node 1 with the actual address
    let (coordinator1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        &addr1,
        NUM_SHARDS,
        10,
        factory1.clone(),
        Vec::new(),
    )
    .await
    .expect("start coordinator 1");
    let coordinator1 = Arc::new(coordinator1);

    // Start node 2 with the actual address
    let (coordinator2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n2",
        &addr2,
        NUM_SHARDS,
        10,
        factory2.clone(),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");
    let coordinator2 = Arc::new(coordinator2);

    // Wait for cluster to converge
    assert!(
        coordinator1.wait_converged(Duration::from_secs(30)).await,
        "cluster did not converge"
    );

    // Start gRPC servers with the already-bound listeners

    let (shutdown_tx1, shutdown_rx1) = tokio::sync::broadcast::channel(1);
    let (shutdown_tx2, shutdown_rx2) = tokio::sync::broadcast::channel(1);

    let server1 = tokio::spawn(run_server(
        listener1,
        factory1.clone(),
        coordinator1.clone(),
        cfg.clone(),
        None, // metrics
        shutdown_rx1,
    ));

    let server2 = tokio::spawn(run_server(
        listener2,
        factory2.clone(),
        coordinator2.clone(),
        cfg.clone(),
        None, // metrics
        shutdown_rx2,
    ));

    // Give servers time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Set up cluster clients with shard routing
    let mut clients = ClusterClients::new(&addr1, &addr2, &coordinator1).await;

    // Get shards owned by each node
    let shard1 = clients.shard_on_node1();
    let shard2 = clients.shard_on_node2();

    // Enqueue jobs to shards owned by the correct nodes
    enqueue_job(clients.client_for_shard(&shard1), &shard1, "job-001", None).await;
    enqueue_job(clients.client_for_shard(&shard1), &shard1, "job-002", None).await;
    enqueue_job(clients.client_for_shard(&shard2), &shard2, "job-003", None).await;
    enqueue_job(clients.client_for_shard(&shard2), &shard2, "job-004", None).await;

    // Create cluster query engine on node 1
    let query_engine = ClusterQueryEngine::new(factory1.clone(), Some(coordinator1.clone()))
        .await
        .expect("failed to create query engine");

    // Test basic count query
    let df = query_engine
        .sql("SELECT COUNT(*) as cnt FROM jobs")
        .await
        .expect("query failed");
    let batches = df.collect().await.expect("collect failed");

    let total_count: i64 = batches
        .iter()
        .map(|b| {
            b.column_by_name("cnt")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        })
        .sum();

    assert_eq!(total_count, 4, "should have 4 jobs total");

    // Cleanup
    let _ = shutdown_tx1.send(());
    let _ = shutdown_tx2.send(());
    coordinator1.shutdown().await.ok();
    coordinator2.shutdown().await.ok();
    server1.abort();
    server2.abort();
    h1.abort();
    h2.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_two_nodes_status_filter_query() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load config");

    let factory1 = make_test_factory(&prefix, "n1");
    let factory2 = make_test_factory(&prefix, "n2");

    // Bind listeners first to get available ports
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap().to_string();
    let addr2 = listener2.local_addr().unwrap().to_string();

    // Start nodes with the actual addresses
    let (coordinator1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        &addr1,
        NUM_SHARDS,
        10,
        factory1.clone(),
        Vec::new(),
    )
    .await
    .expect("start coordinator 1");
    let coordinator1 = Arc::new(coordinator1);

    let (coordinator2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n2",
        &addr2,
        NUM_SHARDS,
        10,
        factory2.clone(),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");
    let coordinator2 = Arc::new(coordinator2);

    assert!(coordinator1.wait_converged(Duration::from_secs(30)).await);

    // Start gRPC servers with the already-bound listeners

    let (shutdown_tx1, shutdown_rx1) = tokio::sync::broadcast::channel(1);
    let (shutdown_tx2, shutdown_rx2) = tokio::sync::broadcast::channel(1);

    let server1 = tokio::spawn(run_server(
        listener1,
        factory1.clone(),
        coordinator1.clone(),
        cfg.clone(),
        None, // metrics
        shutdown_rx1,
    ));

    let server2 = tokio::spawn(run_server(
        listener2,
        factory2.clone(),
        coordinator2.clone(),
        cfg.clone(),
        None, // metrics
        shutdown_rx2,
    ));

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Set up cluster clients with shard routing
    let mut clients = ClusterClients::new(&addr1, &addr2, &coordinator1).await;

    // Enqueue some jobs, alternating between available shards
    let all_shards: Vec<String> = clients
        .node1_shards
        .iter()
        .chain(clients.node2_shards.iter())
        .cloned()
        .collect();
    for i in 0..5u32 {
        let shard = &all_shards[i as usize % all_shards.len()];
        let client = clients.client_for_shard(shard);
        enqueue_job(client, shard, &format!("job-{:03}", i), None).await;
    }

    // Create cluster query engine
    let query_engine = ClusterQueryEngine::new(factory1.clone(), Some(coordinator1.clone()))
        .await
        .expect("failed to create query engine");

    // Test status filter - jobs enqueued with start_at_ms=0 are Waiting (start_time <= now)
    let df = query_engine
        .sql("SELECT id, status_kind, enqueue_time_ms, priority FROM jobs WHERE status_kind = 'Waiting' ORDER BY enqueue_time_ms ASC LIMIT 100")
        .await
        .expect("query failed");
    let batches = df.collect().await.expect("collect failed");

    let mut count = 0;
    for batch in &batches {
        count += batch.num_rows();
        // Verify all returned rows have Waiting status
        if let Some(status_col) = batch
            .column_by_name("status_kind")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        {
            for i in 0..batch.num_rows() {
                assert_eq!(status_col.value(i), "Waiting");
            }
        }
    }

    assert_eq!(count, 5, "should have 5 waiting jobs");

    // Cleanup
    let _ = shutdown_tx1.send(());
    let _ = shutdown_tx2.send(());
    coordinator1.shutdown().await.ok();
    coordinator2.shutdown().await.ok();
    server1.abort();
    server2.abort();
    h1.abort();
    h2.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_two_nodes_count_across_shards() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load config");

    let factory1 = make_test_factory(&prefix, "n1");
    let factory2 = make_test_factory(&prefix, "n2");

    // Bind listeners first to get available ports
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap().to_string();
    let addr2 = listener2.local_addr().unwrap().to_string();

    // Start nodes with the actual addresses
    let (coordinator1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        &addr1,
        NUM_SHARDS,
        10,
        factory1.clone(),
        Vec::new(),
    )
    .await
    .expect("start coordinator 1");
    let coordinator1 = Arc::new(coordinator1);

    let (coordinator2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n2",
        &addr2,
        NUM_SHARDS,
        10,
        factory2.clone(),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");
    let coordinator2 = Arc::new(coordinator2);

    assert!(coordinator1.wait_converged(Duration::from_secs(30)).await);

    // Start gRPC servers with the already-bound listeners

    let (shutdown_tx1, shutdown_rx1) = tokio::sync::broadcast::channel(1);
    let (shutdown_tx2, shutdown_rx2) = tokio::sync::broadcast::channel(1);

    let server1 = tokio::spawn(run_server(
        listener1,
        factory1.clone(),
        coordinator1.clone(),
        cfg.clone(),
        None, // metrics
        shutdown_rx1,
    ));

    let server2 = tokio::spawn(run_server(
        listener2,
        factory2.clone(),
        coordinator2.clone(),
        cfg.clone(),
        None, // metrics
        shutdown_rx2,
    ));

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Set up cluster clients with shard routing
    let mut clients = ClusterClients::new(&addr1, &addr2, &coordinator1).await;

    // Enqueue jobs to different shards, routing to the correct node
    let all_shards: Vec<String> = clients
        .node1_shards
        .iter()
        .chain(clients.node2_shards.iter())
        .cloned()
        .collect();
    for i in 0..20u32 {
        let shard = &all_shards[i as usize % all_shards.len()];
        let client = clients.client_for_shard(shard);
        enqueue_job(client, shard, &format!("job-{:03}", i), None).await;
    }

    // Create cluster query engine
    let query_engine = ClusterQueryEngine::new(factory1.clone(), Some(coordinator1.clone()))
        .await
        .expect("failed to create query engine");

    // Test COUNT aggregation across shards
    let df = query_engine
        .sql("SELECT COUNT(*) as cnt FROM jobs")
        .await
        .expect("query failed");
    let batches = df.collect().await.expect("collect failed");

    let total: i64 = batches
        .iter()
        .map(|b| {
            b.column_by_name("cnt")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        })
        .sum();

    assert_eq!(total, 20, "should have 20 jobs total across all shards");

    // Cleanup
    let _ = shutdown_tx1.send(());
    let _ = shutdown_tx2.send(());
    coordinator1.shutdown().await.ok();
    coordinator2.shutdown().await.ok();
    server1.abort();
    server2.abort();
    h1.abort();
    h2.abort();
}

#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_two_nodes_queues_table() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load config");

    let factory1 = make_test_factory(&prefix, "n1");
    let factory2 = make_test_factory(&prefix, "n2");

    // Bind listeners first to get available ports
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap().to_string();
    let addr2 = listener2.local_addr().unwrap().to_string();

    // Start nodes with the actual addresses
    let (coordinator1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        &addr1,
        NUM_SHARDS,
        10,
        factory1.clone(),
        Vec::new(),
    )
    .await
    .expect("start coordinator 1");
    let coordinator1 = Arc::new(coordinator1);

    let (coordinator2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n2",
        &addr2,
        NUM_SHARDS,
        10,
        factory2.clone(),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");
    let coordinator2 = Arc::new(coordinator2);

    assert!(coordinator1.wait_converged(Duration::from_secs(30)).await);

    // Start gRPC servers with the already-bound listeners

    let (shutdown_tx1, shutdown_rx1) = tokio::sync::broadcast::channel(1);
    let (shutdown_tx2, shutdown_rx2) = tokio::sync::broadcast::channel(1);

    let server1 = tokio::spawn(run_server(
        listener1,
        factory1.clone(),
        coordinator1.clone(),
        cfg.clone(),
        None, // metrics
        shutdown_rx1,
    ));

    let server2 = tokio::spawn(run_server(
        listener2,
        factory2.clone(),
        coordinator2.clone(),
        cfg.clone(),
        None, // metrics
        shutdown_rx2,
    ));

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create cluster query engine
    let query_engine = ClusterQueryEngine::new(factory1.clone(), Some(coordinator1.clone()))
        .await
        .expect("failed to create query engine");

    // Test queues table query - used by queues page
    let df = query_engine
        .sql("SELECT queue_name, entry_type, COUNT(*) as cnt FROM queues GROUP BY queue_name, entry_type")
        .await
        .expect("query failed");
    let batches = df.collect().await.expect("collect failed");

    // Should succeed even with no queues (just verify we got results without error)
    let _ = batches;

    // Cleanup
    let _ = shutdown_tx1.send(());
    let _ = shutdown_tx2.send(());
    coordinator1.shutdown().await.ok();
    coordinator2.shutdown().await.ok();
    server1.abort();
    server2.abort();
    h1.abort();
    h2.abort();
}

/// Test that projection works correctly when querying remote shards.
/// This ensures that when we SELECT specific columns, the projection is
/// properly applied to results from both local and remote shards.
#[silo::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_two_nodes_projection_remote_shards() {
    let prefix = unique_prefix();
    let cfg = silo::settings::AppConfig::load(None).expect("load config");

    let factory1 = make_test_factory(&prefix, "n1");
    let factory2 = make_test_factory(&prefix, "n2");

    // Bind listeners first to get available ports
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap().to_string();
    let addr2 = listener2.local_addr().unwrap().to_string();

    // Start nodes with the actual addresses
    let (coordinator1, h1) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n1",
        &addr1,
        NUM_SHARDS,
        10,
        factory1.clone(),
        Vec::new(),
    )
    .await
    .expect("start coordinator 1");
    let coordinator1 = Arc::new(coordinator1);

    let (coordinator2, h2) = EtcdCoordinator::start(
        &cfg.coordination.etcd_endpoints,
        &prefix,
        "n2",
        &addr2,
        NUM_SHARDS,
        10,
        factory2.clone(),
        Vec::new(),
    )
    .await
    .expect("start coordinator 2");
    let coordinator2 = Arc::new(coordinator2);

    assert!(coordinator1.wait_converged(Duration::from_secs(30)).await);

    // Start gRPC servers with the already-bound listeners

    let (shutdown_tx1, shutdown_rx1) = tokio::sync::broadcast::channel(1);
    let (shutdown_tx2, shutdown_rx2) = tokio::sync::broadcast::channel(1);

    let server1 = tokio::spawn(run_server(
        listener1,
        factory1.clone(),
        coordinator1.clone(),
        cfg.clone(),
        None, // metrics
        shutdown_rx1,
    ));

    let server2 = tokio::spawn(run_server(
        listener2,
        factory2.clone(),
        coordinator2.clone(),
        cfg.clone(),
        None, // metrics
        shutdown_rx2,
    ));

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Set up cluster clients with shard routing
    let mut clients = ClusterClients::new(&addr1, &addr2, &coordinator1).await;

    // Enqueue jobs to different shards, ensuring some go to each node
    let all_shards: Vec<String> = clients
        .node1_shards
        .iter()
        .chain(clients.node2_shards.iter())
        .cloned()
        .collect();
    for i in 0..10u32 {
        let shard = &all_shards[i as usize % all_shards.len()];
        let client = clients.client_for_shard(shard);
        enqueue_job(client, shard, &format!("proj-job-{:03}", i), None).await;
    }

    // Create cluster query engine on node 1
    let query_engine = ClusterQueryEngine::new(factory1.clone(), Some(coordinator1.clone()))
        .await
        .expect("failed to create query engine");

    // Test projection with specific columns (not SELECT *)
    // This is the pattern used by the webui and catches schema mismatches
    // between local and remote shards
    let df = query_engine
        .sql("SELECT shard_id, id, status_kind, enqueue_time_ms FROM jobs ORDER BY id")
        .await
        .expect("projection query failed");
    let batches = df.collect().await.expect("collect failed");

    // Verify we got results
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10, "should have 10 jobs from projection query");

    // Verify the schema has exactly the columns we requested
    for batch in &batches {
        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 4, "should have exactly 4 columns");
        assert_eq!(schema.field(0).name(), "shard_id");
        assert_eq!(schema.field(1).name(), "id");
        assert_eq!(schema.field(2).name(), "status_kind");
        assert_eq!(schema.field(3).name(), "enqueue_time_ms");

        // Verify column types are correct (this catches the bug where
        // projection indices were applied to wrong columns)
        let shard_col = batch
            .column_by_name("shard_id")
            .expect("shard_id column missing")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("shard_id should be String (UUID)");
        let id_col = batch
            .column_by_name("id")
            .expect("id column missing")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id should be String");
        let status_col = batch
            .column_by_name("status_kind")
            .expect("status_kind column missing")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("status_kind should be String");
        let time_col = batch
            .column_by_name("enqueue_time_ms")
            .expect("enqueue_time_ms column missing")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("enqueue_time_ms should be Int64");

        // Verify actual data values make sense
        for i in 0..batch.num_rows() {
            assert!(
                !shard_col.value(i).is_empty(),
                "shard_id should be a non-empty UUID string"
            );
            assert!(
                id_col.value(i).starts_with("proj-job-"),
                "id should start with proj-job-"
            );
            assert_eq!(
                status_col.value(i),
                "Waiting",
                "status should be Waiting (jobs enqueued with start_at_ms=0)"
            );
            // Just verify enqueue_time_ms is accessible (timestamp value depends on test env)
            let _ = time_col.value(i);
        }
    }

    // Also test a projection without shard_id to ensure non-first-column projections work
    let df = query_engine
        .sql("SELECT id, priority FROM jobs ORDER BY id LIMIT 5")
        .await
        .expect("second projection query failed");
    let batches = df.collect().await.expect("collect failed");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 5,
        "should have 5 jobs from limited projection query"
    );

    for batch in &batches {
        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 2, "should have exactly 2 columns");
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "priority");
    }

    // Cleanup
    let _ = shutdown_tx1.send(());
    let _ = shutdown_tx2.send(());
    coordinator1.shutdown().await.ok();
    coordinator2.shutdown().await.ok();
    server1.abort();
    server2.abort();
    h1.abort();
    h2.abort();
}
