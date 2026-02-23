//! Unit tests for ClusterClient

mod grpc_integration_helpers;
mod test_helpers;

use silo::cluster_client::{ClientConfig, ClusterClient, ClusterNodeInfo, NodeInfo, ShardNodeInfo};
use silo::coordination::NoneCoordinator;
use silo::coordination::split::SplitCleanupStatus;
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::{
    AppConfig, Backend, DatabaseTemplate, GubernatorSettings, LoggingConfig, WebUiConfig,
};
use silo::shard_range::{ShardId, ShardMap, ShardRange};
use std::sync::Arc;
use std::time::Duration;

/// Create a test factory with memory backend and a ShardMap
fn make_test_factory_with_shards(num_shards: u32) -> (Arc<ShardFactory>, ShardMap) {
    let tmpdir = tempfile::tempdir().unwrap();
    let factory = Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Memory,
            path: tmpdir.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
            concurrency_reconcile_interval_ms: 5000,
            slatedb: None,
        },
        MockGubernatorClient::new_arc(),
        None,
    ));
    let shard_map = ShardMap::create_initial(num_shards).expect("create shard map");
    (factory, shard_map)
}

#[silo::test]
async fn cluster_client_new_without_coordinator() {
    let (factory, shard_map) = make_test_factory_with_shards(2);
    let client = ClusterClient::new(factory.clone(), None);

    // Without coordinator, owns_shard should return false for unopened shards
    let shard0_id = shard_map.shards()[0].id;
    let shard1_id = shard_map.shards()[1].id;
    assert!(!client.owns_shard(&shard0_id));
    assert!(!client.owns_shard(&shard1_id));
}

#[silo::test]
async fn cluster_client_owns_shard_after_opening() {
    let (factory, shard_map) = make_test_factory_with_shards(3);
    let shard0_id = shard_map.shards()[0].id;
    let shard1_id = shard_map.shards()[1].id;
    let shard2_id = shard_map.shards()[2].id;
    factory
        .open(&shard0_id, &ShardRange::full())
        .await
        .expect("open shard 0");
    factory
        .open(&shard1_id, &ShardRange::full())
        .await
        .expect("open shard 1");

    let client = ClusterClient::new(factory.clone(), None);

    assert!(client.owns_shard(&shard0_id));
    assert!(client.owns_shard(&shard1_id));
    assert!(!client.owns_shard(&shard2_id));
}

#[silo::test]
async fn cluster_client_query_local_shard() {
    let (factory, shard_map) = make_test_factory_with_shards(1);
    let shard_id = shard_map.shards()[0].id;
    factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");

    // Enqueue a job
    let shard = factory.get(&shard_id).unwrap();
    shard
        .enqueue(
            "test-tenant",
            Some("job-001".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"test": "data"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue job");

    let client = ClusterClient::new(factory.clone(), None);

    // Query the local shard
    let result = client
        .query_shard(&shard_id, "SELECT id FROM jobs")
        .await
        .expect("query should succeed");

    assert_eq!(result.shard_id, shard_id);
    assert_eq!(result.row_count, 1);
    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0].name, "id");
}

#[silo::test]
async fn cluster_client_query_local_shard_empty_results() {
    let (factory, shard_map) = make_test_factory_with_shards(1);
    let shard_id = shard_map.shards()[0].id;
    factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");

    let client = ClusterClient::new(factory.clone(), None);

    // Query the local shard with no jobs
    let result = client
        .query_shard(&shard_id, "SELECT id FROM jobs")
        .await
        .expect("query should succeed");

    assert_eq!(result.shard_id, shard_id);
    assert_eq!(result.row_count, 0);
    assert_eq!(result.rows.len(), 0);
    // Schema should still be present even with no rows
    assert_eq!(result.columns.len(), 1);
}

#[silo::test]
async fn cluster_client_query_all_local_shards() {
    let (factory, shard_map) = make_test_factory_with_shards(2);
    let shard0_id = shard_map.shards()[0].id;
    let shard1_id = shard_map.shards()[1].id;
    factory
        .open(&shard0_id, &ShardRange::full())
        .await
        .expect("open shard 0");
    factory
        .open(&shard1_id, &ShardRange::full())
        .await
        .expect("open shard 1");

    // Enqueue jobs to each shard
    let shard0 = factory.get(&shard0_id).unwrap();
    shard0
        .enqueue(
            "test-tenant",
            Some("job-on-shard-0".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue job on shard 0");

    let shard1 = factory.get(&shard1_id).unwrap();
    shard1
        .enqueue(
            "test-tenant",
            Some("job-on-shard-1".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue job on shard 1");

    let client = ClusterClient::new(factory.clone(), None);

    // Query all shards (should run in parallel)
    let results = client
        .query_all_shards("SELECT id FROM jobs")
        .await
        .expect("query all should succeed");

    assert_eq!(results.len(), 2, "should have results from 2 shards");

    let total_rows: i32 = results.iter().map(|r| r.row_count).sum();
    assert_eq!(total_rows, 2, "should have 2 jobs total");

    // Verify we got results from different shards
    let shard_ids: Vec<silo::shard_range::ShardId> = results.iter().map(|r| r.shard_id).collect();
    assert!(shard_ids.contains(&shard0_id));
    assert!(shard_ids.contains(&shard1_id));
}

#[silo::test]
async fn cluster_client_query_shard_not_found_without_coordinator() {
    let (factory, shard_map) = make_test_factory_with_shards(2);
    let shard0_id = shard_map.shards()[0].id;
    let shard1_id = shard_map.shards()[1].id; // We won't open this one
    factory
        .open(&shard0_id, &ShardRange::full())
        .await
        .expect("open shard 0");

    let client = ClusterClient::new(factory.clone(), None);

    // Without coordinator, querying a shard we don't own should return NoCoordinator error
    let result = client.query_shard(&shard1_id, "SELECT id FROM jobs").await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, silo::cluster_client::ClusterClientError::NoCoordinator),
        "expected NoCoordinator error, got {:?}",
        err
    );
}

#[silo::test]
async fn cluster_client_get_job_local() {
    let (factory, shard_map) = make_test_factory_with_shards(1);
    let shard_id = shard_map.shards()[0].id;
    factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");

    let shard = factory.get(&shard_id).unwrap();
    shard
        .enqueue(
            "test-tenant",
            Some("job-001".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"foo": "bar"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue job");

    let client = ClusterClient::new(factory.clone(), None);

    // Get the job
    let response = client
        .get_job(&shard_id, "test-tenant", "job-001", false)
        .await
        .expect("get job should succeed");

    assert_eq!(response.id, "job-001");
    assert_eq!(response.priority, 5);
    assert!(response.payload.is_some());
}

#[silo::test]
async fn cluster_client_get_job_not_found() {
    let (factory, shard_map) = make_test_factory_with_shards(1);
    let shard_id = shard_map.shards()[0].id;
    factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");

    let client = ClusterClient::new(factory.clone(), None);

    // Get non-existent job
    let result = client
        .get_job(&shard_id, "test-tenant", "non-existent", false)
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, silo::cluster_client::ClusterClientError::JobNotFound),
        "expected JobNotFound error, got {:?}",
        err
    );
}

#[silo::test]
async fn cluster_client_cancel_job_local() {
    let (factory, shard_map) = make_test_factory_with_shards(1);
    let shard_id = shard_map.shards()[0].id;
    factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");

    let shard = factory.get(&shard_id).unwrap();
    shard
        .enqueue(
            "test-tenant",
            Some("job-to-cancel".to_string()),
            5,
            test_helpers::now_ms() + 10000, // Future so it's still pending
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue job");

    let client = ClusterClient::new(factory.clone(), None);

    // Cancel the job
    client
        .cancel_job(&shard_id, "test-tenant", "job-to-cancel")
        .await
        .expect("cancel should succeed");

    // Verify the job was cancelled by checking it's gone from scheduled
    let result = client
        .query_shard(
            &shard_id,
            "SELECT id FROM jobs WHERE status_kind = 'Scheduled'",
        )
        .await
        .expect("query should succeed");

    assert_eq!(result.row_count, 0, "job should no longer be scheduled");
}

#[silo::test]
async fn cluster_client_json_serialization_preserves_data() {
    let (factory, shard_map) = make_test_factory_with_shards(1);
    let shard_id = shard_map.shards()[0].id;
    factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");

    // Enqueue jobs with various JSON payload types
    let shard = factory.get(&shard_id).unwrap();

    // Complex nested JSON
    shard
        .enqueue(
            "test-tenant",
            Some("job-complex".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({
                "string": "hello",
                "number": 42,
                "float": 3.14,
                "bool": true,
                "null": null,
                "array": [1, 2, 3],
                "nested": {"key": "value"}
            })),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue complex job");

    let client = ClusterClient::new(factory.clone(), None);

    // Query and verify the payload column is properly serialized
    let result = client
        .query_shard(&shard_id, "SELECT id, payload FROM jobs")
        .await
        .expect("query should succeed");

    assert_eq!(result.row_count, 1);

    // Parse the JSON row to verify it's valid
    let row_data = match &result.rows[0].encoding {
        Some(silo::pb::serialized_bytes::Encoding::Msgpack(data)) => data,
        None => panic!("expected msgpack encoding"),
    };
    let row_json: serde_json::Value =
        rmp_serde::from_slice(row_data).expect("row should be valid MessagePack");

    assert_eq!(row_json["id"], "job-complex");

    // The payload should be a valid JSON string
    let payload_str = row_json["payload"]
        .as_str()
        .expect("payload should be string");
    let payload: serde_json::Value =
        serde_json::from_str(payload_str).expect("payload should be valid JSON");

    assert_eq!(payload["string"], "hello");
    assert_eq!(payload["number"], 42);
    assert_eq!(payload["bool"], true);
    assert!(payload["null"].is_null());
}

#[silo::test]
async fn cluster_client_get_shard_owner_map_without_coordinator() {
    let (factory, _shard_map) = make_test_factory_with_shards(1);
    let client = ClusterClient::new(factory.clone(), None);

    let result = client.get_shard_owner_map().await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, silo::cluster_client::ClusterClientError::NoCoordinator),
        "expected NoCoordinator error, got {:?}",
        err
    );
}

#[silo::test]
async fn cluster_client_query_local_shard_sql_error() {
    let (factory, shard_map) = make_test_factory_with_shards(1);
    let shard_id = shard_map.shards()[0].id;
    factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");

    let client = ClusterClient::new(factory.clone(), None);

    // Query with invalid SQL should fail
    let result = client
        .query_shard(&shard_id, "SELECT FROM WHERE INVALID")
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(
            err,
            silo::cluster_client::ClusterClientError::QueryFailed(_)
        ),
        "expected QueryFailed error, got {:?}",
        err
    );
}

#[silo::test]
async fn cluster_client_query_local_shard_invalid_column() {
    let (factory, shard_map) = make_test_factory_with_shards(1);
    let shard_id = shard_map.shards()[0].id;
    factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");

    let client = ClusterClient::new(factory.clone(), None);

    // Query with non-existent column should fail
    let result = client
        .query_shard(&shard_id, "SELECT nonexistent_column FROM jobs")
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(
            err,
            silo::cluster_client::ClusterClientError::QueryFailed(_)
        ),
        "expected QueryFailed error, got {:?}",
        err
    );
}

#[silo::test]
async fn cluster_client_cancel_nonexistent_job() {
    let (factory, shard_map) = make_test_factory_with_shards(1);
    let shard_id = shard_map.shards()[0].id;
    factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");

    let client = ClusterClient::new(factory.clone(), None);

    // Cancel a job that doesn't exist - should return an error
    let result = client
        .cancel_job(&shard_id, "test-tenant", "nonexistent-job")
        .await;

    // cancel_job returns error for non-existent jobs
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(
            err,
            silo::cluster_client::ClusterClientError::QueryFailed(_)
        ),
        "expected QueryFailed error for non-existent job, got {:?}",
        err
    );
}

#[silo::test]
async fn cluster_client_cancel_job_on_remote_shard_without_coordinator() {
    let (factory, shard_map) = make_test_factory_with_shards(2);
    let shard0_id = shard_map.shards()[0].id;
    let unopened_shard_id = shard_map.shards()[1].id; // NOT opened
    factory
        .open(&shard0_id, &ShardRange::full())
        .await
        .expect("open shard 0");

    let client = ClusterClient::new(factory.clone(), None);

    // Try to cancel job on shard we don't own (without coordinator)
    let result = client
        .cancel_job(&unopened_shard_id, "test-tenant", "some-job")
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, silo::cluster_client::ClusterClientError::NoCoordinator),
        "expected NoCoordinator error, got {:?}",
        err
    );
}

#[silo::test]
async fn cluster_client_get_job_on_remote_shard_without_coordinator() {
    let (factory, shard_map) = make_test_factory_with_shards(2);
    let shard0_id = shard_map.shards()[0].id;
    let unopened_shard_id = shard_map.shards()[1].id; // NOT opened
    factory
        .open(&shard0_id, &ShardRange::full())
        .await
        .expect("open shard 0");

    let client = ClusterClient::new(factory.clone(), None);

    // Try to get job on shard we don't own (without coordinator)
    let result = client
        .get_job(&unopened_shard_id, "test-tenant", "some-job", false)
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, silo::cluster_client::ClusterClientError::NoCoordinator),
        "expected NoCoordinator error, got {:?}",
        err
    );
}

#[silo::test]
async fn cluster_client_query_all_local_shards_with_mixed_results() {
    let (factory, shard_map) = make_test_factory_with_shards(3);
    let shard0_id = shard_map.shards()[0].id;
    let shard1_id = shard_map.shards()[1].id;
    let shard2_id = shard_map.shards()[2].id;
    factory
        .open(&shard0_id, &ShardRange::full())
        .await
        .expect("open shard 0");
    factory
        .open(&shard1_id, &ShardRange::full())
        .await
        .expect("open shard 1");
    factory
        .open(&shard2_id, &ShardRange::full())
        .await
        .expect("open shard 2");

    // Only add jobs to shards 0 and 2
    let shard0 = factory.get(&shard0_id).unwrap();
    shard0
        .enqueue(
            "test-tenant",
            Some("job-0".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue on shard 0");

    let shard2 = factory.get(&shard2_id).unwrap();
    shard2
        .enqueue(
            "test-tenant",
            Some("job-2".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue on shard 2");

    // Shard 1 has no jobs (empty result)

    let client = ClusterClient::new(factory.clone(), None);

    let results = client
        .query_all_shards("SELECT id FROM jobs")
        .await
        .expect("query all should succeed");

    // All 3 shards should return results (even empty ones)
    assert_eq!(results.len(), 3, "should have results from all 3 shards");

    // Total should be 2 jobs
    let total_rows: i32 = results.iter().map(|r| r.row_count).sum();
    assert_eq!(total_rows, 2, "should have 2 jobs total");
}

#[silo::test]
fn test_client_config_default() {
    let config = ClientConfig::default();
    assert_eq!(config.connect_timeout, Duration::from_secs(5));
    assert_eq!(config.request_timeout, Duration::from_secs(30));
    assert_eq!(config.keepalive_interval, Duration::from_secs(10));
    assert_eq!(config.keepalive_timeout, Duration::from_secs(5));
    assert_eq!(config.max_retries, 3);
    assert_eq!(config.retry_backoff_base, Duration::from_millis(50));
}

#[silo::test]
fn test_client_config_unreliable_network() {
    let config = ClientConfig::for_unreliable_network();
    assert_eq!(config.request_timeout, Duration::from_secs(10));
    assert_eq!(config.keepalive_interval, Duration::from_secs(5));
    assert_eq!(config.keepalive_timeout, Duration::from_secs(3));
    assert_eq!(config.max_retries, 10);
}

#[silo::test]
fn test_client_config_dst() {
    let config = ClientConfig::for_dst();
    assert_eq!(config.connect_timeout, Duration::from_secs(2));
    assert_eq!(config.request_timeout, Duration::from_secs(2));
    assert_eq!(config.keepalive_interval, Duration::from_secs(1));
    assert_eq!(config.keepalive_timeout, Duration::from_secs(1));
}

#[silo::test]
fn test_backoff_for_attempt() {
    let config = ClientConfig::default();
    let base = Duration::from_millis(50);

    // attempt 0: base * 2^0 = base * 1
    assert_eq!(config.backoff_for_attempt(0), base * 1);
    // attempt 1: base * 2^1 = base * 2
    assert_eq!(config.backoff_for_attempt(1), base * 2);
    // attempt 2: base * 2^2 = base * 4
    assert_eq!(config.backoff_for_attempt(2), base * 4);
    // attempt 3: base * 2^3 = base * 8
    assert_eq!(config.backoff_for_attempt(3), base * 8);
    // attempt 4: base * 2^4 = base * 16 (cap)
    assert_eq!(config.backoff_for_attempt(4), base * 16);
    // attempt 5+: capped at 2^4 = 16
    assert_eq!(config.backoff_for_attempt(5), base * 16);
    assert_eq!(config.backoff_for_attempt(100), base * 16);
}

// === ClusterNodeInfo in-memory tests ===

#[silo::test]
fn cluster_node_info_get_shard_info() {
    let shard_id_1 = ShardId::new();
    let shard_id_2 = ShardId::new();
    let unknown_id = ShardId::new();

    let info = ClusterNodeInfo {
        nodes: vec![NodeInfo {
            node_id: "node-a".to_string(),
            shards: vec![
                ShardNodeInfo {
                    shard_id: shard_id_1,
                    total_jobs: 100,
                    completed_jobs: 50,
                    cleanup_status: SplitCleanupStatus::CompactionDone,
                    created_at_ms: 1000,
                    cleanup_completed_at_ms: 0,
                },
                ShardNodeInfo {
                    shard_id: shard_id_2,
                    total_jobs: 200,
                    completed_jobs: 75,
                    cleanup_status: SplitCleanupStatus::CompactionDone,
                    created_at_ms: 2000,
                    cleanup_completed_at_ms: 0,
                },
            ],
        }],
    };

    // Found cases
    let found = info.get_shard_info(&shard_id_1);
    assert!(found.is_some());
    assert_eq!(found.unwrap().total_jobs, 100);

    let found2 = info.get_shard_info(&shard_id_2);
    assert!(found2.is_some());
    assert_eq!(found2.unwrap().total_jobs, 200);

    // Not found
    assert!(info.get_shard_info(&unknown_id).is_none());
}

#[silo::test]
fn cluster_node_info_shard_info_map() {
    let shard_id_1 = ShardId::new();
    let shard_id_2 = ShardId::new();
    let shard_id_3 = ShardId::new();

    let info = ClusterNodeInfo {
        nodes: vec![
            NodeInfo {
                node_id: "node-a".to_string(),
                shards: vec![ShardNodeInfo {
                    shard_id: shard_id_1,
                    total_jobs: 10,
                    completed_jobs: 5,
                    cleanup_status: SplitCleanupStatus::CompactionDone,
                    created_at_ms: 1000,
                    cleanup_completed_at_ms: 0,
                }],
            },
            NodeInfo {
                node_id: "node-b".to_string(),
                shards: vec![
                    ShardNodeInfo {
                        shard_id: shard_id_2,
                        total_jobs: 20,
                        completed_jobs: 10,
                        cleanup_status: SplitCleanupStatus::CompactionDone,
                        created_at_ms: 2000,
                        cleanup_completed_at_ms: 0,
                    },
                    ShardNodeInfo {
                        shard_id: shard_id_3,
                        total_jobs: 30,
                        completed_jobs: 15,
                        cleanup_status: SplitCleanupStatus::CompactionDone,
                        created_at_ms: 3000,
                        cleanup_completed_at_ms: 0,
                    },
                ],
            },
        ],
    };

    let map = info.shard_info_map();
    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&shard_id_1).unwrap().total_jobs, 10);
    assert_eq!(map.get(&shard_id_2).unwrap().total_jobs, 20);
    assert_eq!(map.get(&shard_id_3).unwrap().total_jobs, 30);
}

// === Helper for creating a test AppConfig ===

fn make_test_app_config(tmp: &tempfile::TempDir) -> AppConfig {
    AppConfig {
        server: Default::default(),
        coordination: Default::default(),
        tenancy: silo::settings::TenancyConfig { enabled: true },
        gubernator: GubernatorSettings::default(),
        webui: WebUiConfig::default(),
        logging: LoggingConfig::default(),
        metrics: silo::settings::MetricsConfig::default(),
        database: DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
            concurrency_reconcile_interval_ms: 5000,
            slatedb: None,
        },
    }
}

// === Remote gRPC tests ===

#[silo::test(flavor = "multi_thread")]
async fn cluster_client_remote_query_shard() {
    let (server_factory, _tmp) = grpc_integration_helpers::create_test_factory()
        .await
        .unwrap();
    let server_tmp = tempfile::tempdir().unwrap();
    let config = make_test_app_config(&server_tmp);
    let (_, shutdown_tx, server_task, server_addr) =
        grpc_integration_helpers::setup_test_server(server_factory.clone(), config)
            .await
            .unwrap();

    let shard_id = ShardId::parse(grpc_integration_helpers::TEST_SHARD_ID).unwrap();

    // Enqueue a job on the server
    let shard = server_factory.get(&shard_id).unwrap();
    shard
        .enqueue(
            "test-tenant",
            Some("remote-query-job".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"remote": true})),
            vec![],
            None,
            "default",
        )
        .await
        .unwrap();

    // Create a coordinator that knows the shard lives at the server's address
    let coordinator = Arc::new(
        NoneCoordinator::from_factory(
            "test-node",
            format!("http://{}", server_addr),
            server_factory.clone(),
        )
        .await,
    );

    // Create a cluster client with an empty local factory but the coordinator
    // that knows shards belong to the server
    let empty_factory = Arc::new(ShardFactory::new_noop());
    let client = ClusterClient::new(empty_factory, Some(coordinator));

    // Query should go remote since our factory doesn't have the shard
    let result = client
        .query_shard(&shard_id, "SELECT id FROM jobs")
        .await
        .expect("remote query should succeed");

    assert_eq!(result.shard_id, shard_id);
    assert_eq!(result.row_count, 1);

    grpc_integration_helpers::shutdown_server(shutdown_tx, server_task)
        .await
        .unwrap();
}

#[silo::test(flavor = "multi_thread")]
async fn cluster_client_remote_get_job() {
    let (server_factory, _tmp) = grpc_integration_helpers::create_test_factory()
        .await
        .unwrap();
    let server_tmp = tempfile::tempdir().unwrap();
    let config = make_test_app_config(&server_tmp);
    let (_, shutdown_tx, server_task, server_addr) =
        grpc_integration_helpers::setup_test_server(server_factory.clone(), config)
            .await
            .unwrap();

    let shard_id = ShardId::parse(grpc_integration_helpers::TEST_SHARD_ID).unwrap();

    // Enqueue a job on the server
    let shard = server_factory.get(&shard_id).unwrap();
    shard
        .enqueue(
            "test-tenant",
            Some("remote-get-job".to_string()),
            7,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"data": "hello"})),
            vec![],
            None,
            "default",
        )
        .await
        .unwrap();

    let coordinator = Arc::new(
        NoneCoordinator::from_factory(
            "test-node",
            format!("http://{}", server_addr),
            server_factory.clone(),
        )
        .await,
    );
    let empty_factory = Arc::new(ShardFactory::new_noop());
    let client = ClusterClient::new(empty_factory, Some(coordinator));

    let response = client
        .get_job(&shard_id, "test-tenant", "remote-get-job", false)
        .await
        .expect("remote get_job should succeed");

    assert_eq!(response.id, "remote-get-job");
    assert_eq!(response.priority, 7);

    grpc_integration_helpers::shutdown_server(shutdown_tx, server_task)
        .await
        .unwrap();
}

#[silo::test(flavor = "multi_thread")]
async fn cluster_client_remote_get_job_not_found() {
    let (server_factory, _tmp) = grpc_integration_helpers::create_test_factory()
        .await
        .unwrap();
    let server_tmp = tempfile::tempdir().unwrap();
    let config = make_test_app_config(&server_tmp);
    let (_, shutdown_tx, server_task, server_addr) =
        grpc_integration_helpers::setup_test_server(server_factory.clone(), config)
            .await
            .unwrap();

    let shard_id = ShardId::parse(grpc_integration_helpers::TEST_SHARD_ID).unwrap();

    let coordinator = Arc::new(
        NoneCoordinator::from_factory(
            "test-node",
            format!("http://{}", server_addr),
            server_factory.clone(),
        )
        .await,
    );
    let empty_factory = Arc::new(ShardFactory::new_noop());
    let client = ClusterClient::new(empty_factory, Some(coordinator));

    let result = client
        .get_job(&shard_id, "test-tenant", "nonexistent", false)
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, silo::cluster_client::ClusterClientError::JobNotFound),
        "expected JobNotFound, got {:?}",
        err
    );

    grpc_integration_helpers::shutdown_server(shutdown_tx, server_task)
        .await
        .unwrap();
}

#[silo::test(flavor = "multi_thread")]
async fn cluster_client_remote_cancel_job() {
    let (server_factory, _tmp) = grpc_integration_helpers::create_test_factory()
        .await
        .unwrap();
    let server_tmp = tempfile::tempdir().unwrap();
    let config = make_test_app_config(&server_tmp);
    let (_, shutdown_tx, server_task, server_addr) =
        grpc_integration_helpers::setup_test_server(server_factory.clone(), config)
            .await
            .unwrap();

    let shard_id = ShardId::parse(grpc_integration_helpers::TEST_SHARD_ID).unwrap();

    // Enqueue a future job so it can be cancelled
    let shard = server_factory.get(&shard_id).unwrap();
    shard
        .enqueue(
            "test-tenant",
            Some("remote-cancel-job".to_string()),
            5,
            test_helpers::now_ms() + 60_000,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .unwrap();

    let coordinator = Arc::new(
        NoneCoordinator::from_factory(
            "test-node",
            format!("http://{}", server_addr),
            server_factory.clone(),
        )
        .await,
    );
    let empty_factory = Arc::new(ShardFactory::new_noop());
    let client = ClusterClient::new(empty_factory, Some(coordinator));

    client
        .cancel_job(&shard_id, "test-tenant", "remote-cancel-job")
        .await
        .expect("remote cancel should succeed");

    grpc_integration_helpers::shutdown_server(shutdown_tx, server_task)
        .await
        .unwrap();
}

// === get_all_node_info tests ===

#[silo::test(flavor = "multi_thread")]
async fn cluster_client_get_all_node_info_local() {
    let (server_factory, _tmp) = grpc_integration_helpers::create_test_factory()
        .await
        .unwrap();
    let shard_id = ShardId::parse(grpc_integration_helpers::TEST_SHARD_ID).unwrap();

    let coordinator = Arc::new(
        NoneCoordinator::from_factory("test-node", "http://127.0.0.1:0", server_factory.clone())
            .await,
    );

    let client = ClusterClient::new(server_factory.clone(), Some(coordinator));

    let cluster_info = client
        .get_all_node_info()
        .await
        .expect("get_all_node_info should succeed");

    assert_eq!(cluster_info.nodes.len(), 1);
    assert_eq!(cluster_info.nodes[0].node_id, "test-node");
    assert_eq!(cluster_info.nodes[0].shards.len(), 1);
    assert_eq!(cluster_info.nodes[0].shards[0].shard_id, shard_id);
}

#[silo::test]
async fn cluster_client_get_all_node_info_no_coordinator() {
    let factory = Arc::new(ShardFactory::new_noop());
    let client = ClusterClient::new(factory, None);

    let result = client.get_all_node_info().await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, silo::cluster_client::ClusterClientError::NoCoordinator),
        "expected NoCoordinator, got {:?}",
        err
    );
}

// === Connection caching tests ===

#[silo::test(flavor = "multi_thread")]
async fn cluster_client_connection_caching() {
    let (server_factory, _tmp) = grpc_integration_helpers::create_test_factory()
        .await
        .unwrap();
    let server_tmp = tempfile::tempdir().unwrap();
    let config = make_test_app_config(&server_tmp);
    let (_, shutdown_tx, server_task, server_addr) =
        grpc_integration_helpers::setup_test_server(server_factory.clone(), config)
            .await
            .unwrap();

    let shard_id = ShardId::parse(grpc_integration_helpers::TEST_SHARD_ID).unwrap();

    let coordinator = Arc::new(
        NoneCoordinator::from_factory(
            "test-node",
            format!("http://{}", server_addr),
            server_factory.clone(),
        )
        .await,
    );
    let empty_factory = Arc::new(ShardFactory::new_noop());
    let client = ClusterClient::new(empty_factory, Some(coordinator));

    // First query - creates connection
    let result1 = client
        .query_shard(&shard_id, "SELECT id FROM jobs")
        .await
        .expect("first query should succeed");
    assert_eq!(result1.row_count, 0);

    // Second query - should reuse cached connection
    let result2 = client
        .query_shard(&shard_id, "SELECT id FROM jobs")
        .await
        .expect("second query should succeed");
    assert_eq!(result2.row_count, 0);

    // Invalidate connection
    client.invalidate_connection(&format!("http://{}", server_addr));

    // Third query - should reconnect
    let result3 = client
        .query_shard(&shard_id, "SELECT id FROM jobs")
        .await
        .expect("third query after invalidation should succeed");
    assert_eq!(result3.row_count, 0);

    grpc_integration_helpers::shutdown_server(shutdown_tx, server_task)
        .await
        .unwrap();
}

// === create_silo_client connection failure ===

#[silo::test(flavor = "multi_thread")]
async fn create_silo_client_connection_failure() {
    let config = ClientConfig {
        connect_timeout: Duration::from_millis(100),
        request_timeout: Duration::from_millis(100),
        keepalive_interval: Duration::from_secs(1),
        keepalive_timeout: Duration::from_secs(1),
        max_retries: 1,
        retry_backoff_base: Duration::from_millis(10),
    };

    // Try to connect to a port that nothing is listening on
    let result = silo::cluster_client::create_silo_client("http://127.0.0.1:1", &config).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(
            err,
            silo::cluster_client::ClusterClientError::ConnectionFailed(_, _)
        ),
        "expected ConnectionFailed, got {:?}",
        err
    );
}
