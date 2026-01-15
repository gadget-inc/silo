//! Unit tests for ClusterClient

mod test_helpers;

use silo::cluster_client::ClusterClient;
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::{Backend, DatabaseTemplate};
use std::sync::Arc;

/// Create a test factory with memory backend
fn make_test_factory() -> Arc<ShardFactory> {
    let tmpdir = tempfile::tempdir().unwrap();
    Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Memory,
            path: tmpdir.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        },
        MockGubernatorClient::new_arc(),
    ))
}

#[silo::test]
async fn cluster_client_new_without_coordinator() {
    let factory = make_test_factory();
    let client = ClusterClient::new(factory.clone(), None);

    // Without coordinator, owns_shard should return false for unopened shards
    assert!(!client.owns_shard(0));
    assert!(!client.owns_shard(1));
}

#[silo::test]
async fn cluster_client_owns_shard_after_opening() {
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    let client = ClusterClient::new(factory.clone(), None);

    assert!(client.owns_shard(0));
    assert!(client.owns_shard(1));
    assert!(!client.owns_shard(2));
}

#[silo::test]
async fn cluster_client_query_local_shard() {
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard");

    // Enqueue a job
    let shard = factory.get("0").unwrap();
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
        )
        .await
        .expect("enqueue job");

    let client = ClusterClient::new(factory.clone(), None);

    // Query the local shard
    let result = client
        .query_shard(0, "SELECT id FROM jobs")
        .await
        .expect("query should succeed");

    assert_eq!(result.shard_id, 0);
    assert_eq!(result.row_count, 1);
    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0].name, "id");
}

#[silo::test]
async fn cluster_client_query_local_shard_empty_results() {
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard");

    let client = ClusterClient::new(factory.clone(), None);

    // Query the local shard with no jobs
    let result = client
        .query_shard(0, "SELECT id FROM jobs")
        .await
        .expect("query should succeed");

    assert_eq!(result.shard_id, 0);
    assert_eq!(result.row_count, 0);
    assert_eq!(result.rows.len(), 0);
    // Schema should still be present even with no rows
    assert_eq!(result.columns.len(), 1);
}

#[silo::test]
async fn cluster_client_query_all_local_shards() {
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");

    // Enqueue jobs to each shard
    let shard0 = factory.get("0").unwrap();
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
        )
        .await
        .expect("enqueue job on shard 0");

    let shard1 = factory.get("1").unwrap();
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
    let shard_ids: Vec<u32> = results.iter().map(|r| r.shard_id).collect();
    assert!(shard_ids.contains(&0));
    assert!(shard_ids.contains(&1));
}

#[silo::test]
async fn cluster_client_query_shard_not_found_without_coordinator() {
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard 0");

    let client = ClusterClient::new(factory.clone(), None);

    // Without coordinator, querying a shard we don't own should return NoCoordinator error
    let result = client.query_shard(999, "SELECT id FROM jobs").await;

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
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard");

    let shard = factory.get("0").unwrap();
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
        )
        .await
        .expect("enqueue job");

    let client = ClusterClient::new(factory.clone(), None);

    // Get the job
    let response = client
        .get_job(0, "test-tenant", "job-001", false)
        .await
        .expect("get job should succeed");

    assert_eq!(response.id, "job-001");
    assert_eq!(response.priority, 5);
    assert!(response.payload.is_some());
}

#[silo::test]
async fn cluster_client_get_job_not_found() {
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard");

    let client = ClusterClient::new(factory.clone(), None);

    // Get non-existent job
    let result = client.get_job(0, "test-tenant", "non-existent", false).await;

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
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard");

    let shard = factory.get("0").unwrap();
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
        )
        .await
        .expect("enqueue job");

    let client = ClusterClient::new(factory.clone(), None);

    // Cancel the job
    client
        .cancel_job(0, "test-tenant", "job-to-cancel")
        .await
        .expect("cancel should succeed");

    // Verify the job was cancelled by checking it's gone from scheduled
    let result = client
        .query_shard(0, "SELECT id FROM jobs WHERE status_kind = 'Scheduled'")
        .await
        .expect("query should succeed");

    assert_eq!(result.row_count, 0, "job should no longer be scheduled");
}

#[silo::test]
async fn cluster_client_json_serialization_preserves_data() {
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard");

    // Enqueue jobs with various JSON payload types
    let shard = factory.get("0").unwrap();

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
        )
        .await
        .expect("enqueue complex job");

    let client = ClusterClient::new(factory.clone(), None);

    // Query and verify the payload column is properly serialized
    let result = client
        .query_shard(0, "SELECT id, payload FROM jobs")
        .await
        .expect("query should succeed");

    assert_eq!(result.row_count, 1);

    // Parse the JSON row to verify it's valid
    let row_json: serde_json::Value =
        rmp_serde::from_slice(&result.rows[0].data).expect("row should be valid MessagePack");

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
    let factory = make_test_factory();
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
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard");

    let client = ClusterClient::new(factory.clone(), None);

    // Query with invalid SQL should fail
    let result = client.query_shard(0, "SELECT FROM WHERE INVALID").await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, silo::cluster_client::ClusterClientError::QueryFailed(_)),
        "expected QueryFailed error, got {:?}",
        err
    );
}

#[silo::test]
async fn cluster_client_query_local_shard_invalid_column() {
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard");

    let client = ClusterClient::new(factory.clone(), None);

    // Query with non-existent column should fail
    let result = client
        .query_shard(0, "SELECT nonexistent_column FROM jobs")
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, silo::cluster_client::ClusterClientError::QueryFailed(_)),
        "expected QueryFailed error, got {:?}",
        err
    );
}

#[silo::test]
async fn cluster_client_cancel_nonexistent_job() {
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard");

    let client = ClusterClient::new(factory.clone(), None);

    // Cancel a job that doesn't exist - should return an error
    let result = client.cancel_job(0, "test-tenant", "nonexistent-job").await;

    // cancel_job returns error for non-existent jobs
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, silo::cluster_client::ClusterClientError::QueryFailed(_)),
        "expected QueryFailed error for non-existent job, got {:?}",
        err
    );
}

#[silo::test]
async fn cluster_client_cancel_job_on_remote_shard_without_coordinator() {
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard 0");
    // Note: Shard 999 is NOT opened

    let client = ClusterClient::new(factory.clone(), None);

    // Try to cancel job on shard we don't own (without coordinator)
    let result = client
        .cancel_job(999, "test-tenant", "some-job")
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
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard 0");
    // Note: Shard 999 is NOT opened

    let client = ClusterClient::new(factory.clone(), None);

    // Try to get job on shard we don't own (without coordinator)
    let result = client
        .get_job(999, "test-tenant", "some-job", false)
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
    let factory = make_test_factory();
    factory.open(0).await.expect("open shard 0");
    factory.open(1).await.expect("open shard 1");
    factory.open(2).await.expect("open shard 2");

    // Only add jobs to shards 0 and 2
    let shard0 = factory.get("0").unwrap();
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
        )
        .await
        .expect("enqueue on shard 0");

    let shard2 = factory.get("2").unwrap();
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
