mod grpc_integration_helpers;
mod test_helpers;

use grpc_integration_helpers::{create_test_factory, setup_test_server, shutdown_server};
use silo::pb::silo_admin_client::SiloAdminClient;
use silo::pb::*;
use silo::settings::AppConfig;
use test_helpers::{msgpack_payload, open_temp_shard};

/// Test that submit_full_compaction succeeds on a shard with data.
/// Enqueues jobs, flushes, then triggers compaction. Verifies data is still readable after.
#[silo::test]
async fn submit_full_compaction_with_data() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue some jobs to create data in the LSM tree
    for i in 0..10 {
        let payload = msgpack_payload(&serde_json::json!({"value": i}));
        shard
            .enqueue(
                "tenant-a",
                Some(format!("job-{}", i)),
                0,
                0,
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue should succeed");
    }

    // Flush to ensure data is in SSTs
    shard.db().flush().await.unwrap();

    // Submit full compaction - should succeed
    shard
        .submit_full_compaction()
        .await
        .expect("submit_full_compaction should succeed");

    // Verify data is still accessible after compaction submission
    let job = shard
        .get_job("tenant-a", "job-0")
        .await
        .expect("get_job should succeed");
    assert!(job.is_some(), "job should still exist after compaction");
}

/// Test that submit_full_compaction succeeds on an empty shard (no-op).
#[silo::test]
async fn submit_full_compaction_empty_shard() {
    let (_tmp, shard) = open_temp_shard().await;

    // Flush to ensure memtable is empty
    shard.db().flush().await.unwrap();

    // Submit full compaction on empty shard - should succeed (no-op)
    shard
        .submit_full_compaction()
        .await
        .expect("submit_full_compaction on empty shard should succeed");
}

/// Test the CompactShard gRPC endpoint succeeds for a valid shard.
#[silo::test(flavor = "multi_thread")]
async fn grpc_compact_shard_succeeds() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (_client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
        let mut admin_client = SiloAdminClient::new(channel);

        let resp = admin_client
            .compact_shard(CompactShardRequest {
                shard: grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            })
            .await?
            .into_inner();

        assert!(
            resp.status.contains("full compaction submitted"),
            "status should indicate compaction was submitted, got: {}",
            resp.status
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that CompactShard returns an error for an invalid shard ID.
#[silo::test(flavor = "multi_thread")]
async fn grpc_compact_shard_invalid_id() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (_client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
        let mut admin_client = SiloAdminClient::new(channel);

        let result = admin_client
            .compact_shard(CompactShardRequest {
                shard: "not-a-valid-uuid".to_string(),
            })
            .await;

        assert!(result.is_err(), "invalid shard ID should return an error");
        let status = result.unwrap_err();
        assert_eq!(
            status.code(),
            tonic::Code::InvalidArgument,
            "invalid shard ID should return InvalidArgument"
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test that CompactShard returns NotFound for a shard not owned by this node.
#[silo::test(flavor = "multi_thread")]
async fn grpc_compact_shard_not_found() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let (_client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
        let mut admin_client = SiloAdminClient::new(channel);

        // Use a valid UUID that doesn't correspond to any shard on this node
        let result = admin_client
            .compact_shard(CompactShardRequest {
                shard: "11111111-1111-1111-1111-111111111111".to_string(),
            })
            .await;

        assert!(result.is_err(), "unknown shard should return an error");
        let status = result.unwrap_err();
        assert_eq!(
            status.code(),
            tonic::Code::NotFound,
            "unknown shard should return NotFound"
        );

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

/// Test CompactShard on a shard with data and tombstones from cancelled+deleted jobs.
/// This simulates the production scenario where completed jobs leave tombstones.
#[silo::test(flavor = "multi_thread")]
async fn grpc_compact_shard_with_tombstones() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(10000), async {
        let (factory, _tmp) = create_test_factory().await?;
        let shard_id =
            silo::shard_range::ShardId::parse(grpc_integration_helpers::TEST_SHARD_ID).unwrap();
        let shard = factory.get(&shard_id).expect("shard should exist");

        // Enqueue, cancel, then delete jobs to create tombstones
        for i in 0..5 {
            let payload = msgpack_payload(&serde_json::json!({"data": i}));
            shard
                .enqueue(
                    "tenant-a",
                    Some(format!("tombstone-job-{}", i)),
                    0,
                    0,
                    None,
                    payload,
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue should succeed");
        }

        // Cancel then delete jobs to create tombstones
        for i in 0..5 {
            shard
                .cancel_job("tenant-a", &format!("tombstone-job-{}", i))
                .await
                .expect("cancel should succeed");
            shard
                .delete_job("tenant-a", &format!("tombstone-job-{}", i))
                .await
                .expect("delete should succeed");
        }

        // Flush to ensure tombstones are in SSTs
        shard.db().flush().await.unwrap();

        // Now compact via gRPC
        let (_client, shutdown_tx, server, addr) =
            setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
        let mut admin_client = SiloAdminClient::new(channel);

        let resp = admin_client
            .compact_shard(CompactShardRequest {
                shard: grpc_integration_helpers::TEST_SHARD_ID.to_string(),
            })
            .await?
            .into_inner();

        assert!(
            resp.status.contains("full compaction submitted"),
            "status should indicate compaction was submitted, got: {}",
            resp.status
        );

        // Verify deleted jobs are gone
        for i in 0..5 {
            let job = shard
                .get_job("tenant-a", &format!("tombstone-job-{}", i))
                .await
                .expect("get_job should succeed");
            assert!(job.is_none(), "deleted job should not exist");
        }

        shutdown_server(shutdown_tx, server).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}
