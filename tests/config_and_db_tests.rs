use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::job_store_shard::JobStoreShard;
use silo::settings::{
    expand_env_vars_for_test, AppConfig, Backend, DatabaseConfig, DatabaseTemplate,
    GubernatorSettings, LoggingConfig, WalConfig, WebUiConfig,
};
use silo::storage::resolve_object_store;

#[silo::test]
async fn open_fs_db_from_config() {
    let tmp = tempfile::tempdir().unwrap();

    let cfg = AppConfig {
        server: Default::default(),
        coordination: Default::default(),
        tenancy: silo::settings::TenancyConfig { enabled: false },
        gubernator: GubernatorSettings::default(),
        webui: WebUiConfig::default(),
        logging: LoggingConfig::default(),
        metrics: silo::settings::MetricsConfig::default(),
        database: DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
            apply_wal_on_close: true,
        },
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let factory = ShardFactory::new(cfg.database.clone(), rate_limiter);
    let shard = factory.open(0).await.expect("open shard");

    shard.db().put(b"k", b"v").await.expect("put");
    shard.db().flush().await.expect("flush");
    let got = shard.db().get(b"k").await.expect("get");
    assert_eq!(got.unwrap(), slatedb::bytes::Bytes::from_static(b"v"));
}

// =============================================================================
// WAL Flush Before Close Tests
// =============================================================================

/// Helper to check if a directory exists and has files in it
async fn dir_has_files(path: &std::path::Path) -> bool {
    if !path.exists() {
        return false;
    }
    match tokio::fs::read_dir(path).await {
        Ok(mut entries) => entries.next_entry().await.ok().flatten().is_some(),
        Err(_) => false,
    }
}

#[silo::test]
async fn shard_with_local_wal_has_wal_close_config() {
    // When opening a shard with local WAL storage, the shard should have a WAL close config
    let data_dir = tempfile::tempdir().unwrap();
    let wal_dir = tempfile::tempdir().unwrap();

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: data_dir.path().to_string_lossy().to_string(),
        flush_interval_ms: Some(10),
        wal: Some(WalConfig {
            backend: Backend::Fs,
            path: wal_dir.path().to_string_lossy().to_string(),
        }),
        apply_wal_on_close: true,
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let shard = JobStoreShard::open_with_rate_limiter(&cfg, rate_limiter)
        .await
        .expect("open shard");

    // Verify WAL close config is present
    let wal_config = shard.wal_close_config();
    assert!(wal_config.is_some(), "shard should have WAL close config");
    let wal_config = wal_config.unwrap();
    assert!(wal_config.flush_on_close, "flush_on_close should be true");
    assert!(
        wal_config
            .path
            .contains(wal_dir.path().to_string_lossy().as_ref()),
        "WAL path should match configured path"
    );
}

#[silo::test]
async fn shard_without_local_wal_has_no_wal_close_config() {
    // When opening a shard without WAL config, there should be no WAL close config
    let data_dir = tempfile::tempdir().unwrap();

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: data_dir.path().to_string_lossy().to_string(),
        flush_interval_ms: Some(10),
        wal: None,
        apply_wal_on_close: true,
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let shard = JobStoreShard::open_with_rate_limiter(&cfg, rate_limiter)
        .await
        .expect("open shard");

    assert!(
        shard.wal_close_config().is_none(),
        "shard should not have WAL close config"
    );
}

#[silo::test]
async fn close_with_flush_wal_removes_local_wal_directory() {
    // When closing a shard with apply_wal_on_close=true, the local WAL directory should be removed
    let data_dir = tempfile::tempdir().unwrap();
    let wal_dir = tempfile::tempdir().unwrap();
    let wal_path = wal_dir.path().to_path_buf();

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: data_dir.path().to_string_lossy().to_string(),
        flush_interval_ms: Some(10),
        wal: Some(WalConfig {
            backend: Backend::Fs,
            path: wal_dir.path().to_string_lossy().to_string(),
        }),
        apply_wal_on_close: true,
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let shard = JobStoreShard::open_with_rate_limiter(&cfg, rate_limiter)
        .await
        .expect("open shard");

    // Write some data to ensure WAL has content
    shard
        .db()
        .put(b"test_key", b"test_value")
        .await
        .expect("put");

    // After writing, the WAL directory should have files
    assert!(
        dir_has_files(&wal_path).await,
        "WAL directory should have files after write"
    );

    // Close the shard - this should flush to SST and remove WAL
    shard.close().await.expect("close shard");

    // WAL directory should be removed
    assert!(
        !wal_path.exists(),
        "WAL directory should be removed after close"
    );
}

#[silo::test]
async fn close_without_flush_wal_preserves_local_wal_directory() {
    // When closing a shard with apply_wal_on_close=false, the local WAL directory should remain
    let data_dir = tempfile::tempdir().unwrap();
    let wal_dir = tempfile::tempdir().unwrap();
    let wal_path = wal_dir.path().to_path_buf();

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: data_dir.path().to_string_lossy().to_string(),
        flush_interval_ms: Some(10),
        wal: Some(WalConfig {
            backend: Backend::Fs,
            path: wal_dir.path().to_string_lossy().to_string(),
        }),
        apply_wal_on_close: false, // Disable apply on close
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let shard = JobStoreShard::open_with_rate_limiter(&cfg, rate_limiter)
        .await
        .expect("open shard");

    // Write some data
    shard
        .db()
        .put(b"test_key", b"test_value")
        .await
        .expect("put");

    // Close the shard without apply on close
    shard.close().await.expect("close shard");

    // WAL directory should still exist (though its contents may vary)
    // Note: we can't guarantee files exist because SlateDB may still clean up on close
    // but the directory itself won't be explicitly deleted by our code
    assert!(
        wal_path.exists(),
        "WAL directory should still exist after close without flush"
    );
}

#[silo::test]
async fn data_persists_after_close_with_flush_and_reopen() {
    // Data written before close should be accessible after reopening from a fresh state
    // This simulates the shard being reopened on a different node
    let data_dir = tempfile::tempdir().unwrap();
    let wal_dir = tempfile::tempdir().unwrap();
    let data_path = data_dir.path().to_path_buf();

    // First: Open shard, write data, and close with flush
    {
        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: data_dir.path().to_string_lossy().to_string(),
            flush_interval_ms: Some(10),
            wal: Some(WalConfig {
                backend: Backend::Fs,
                path: wal_dir.path().to_string_lossy().to_string(),
            }),
            apply_wal_on_close: true,
        };

        let rate_limiter = MockGubernatorClient::new_arc();
        let shard = JobStoreShard::open_with_rate_limiter(&cfg, rate_limiter)
            .await
            .expect("open shard");

        // Write multiple keys
        shard.db().put(b"key1", b"value1").await.expect("put key1");
        shard.db().put(b"key2", b"value2").await.expect("put key2");
        shard.db().put(b"key3", b"value3").await.expect("put key3");

        // Close with flush - this flushes to SST and removes WAL
        shard.close().await.expect("close shard");
    }

    // Second: Reopen the shard WITHOUT the local WAL directory
    // (simulating opening on a new node that only has access to object storage)
    {
        // Create a new WAL directory (simulating a new node)
        let new_wal_dir = tempfile::tempdir().unwrap();

        let cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: data_path.to_string_lossy().to_string(),
            flush_interval_ms: Some(10),
            wal: Some(WalConfig {
                backend: Backend::Fs,
                path: new_wal_dir.path().to_string_lossy().to_string(),
            }),
            apply_wal_on_close: true,
        };

        let rate_limiter = MockGubernatorClient::new_arc();
        let shard = JobStoreShard::open_with_rate_limiter(&cfg, rate_limiter)
            .await
            .expect("reopen shard");

        // Verify all data is accessible
        let v1 = shard.db().get(b"key1").await.expect("get key1");
        let v2 = shard.db().get(b"key2").await.expect("get key2");
        let v3 = shard.db().get(b"key3").await.expect("get key3");

        assert_eq!(v1, Some(slatedb::bytes::Bytes::from_static(b"value1")));
        assert_eq!(v2, Some(slatedb::bytes::Bytes::from_static(b"value2")));
        assert_eq!(v3, Some(slatedb::bytes::Bytes::from_static(b"value3")));
    }
}

#[silo::test]
async fn factory_close_all_flushes_all_shards_wal() {
    // Test that close_all flushes WAL for all shards
    let data_dir = tempfile::tempdir().unwrap();
    let wal_dir = tempfile::tempdir().unwrap();

    let template = DatabaseTemplate {
        backend: Backend::Fs,
        path: data_dir
            .path()
            .join("%shard%")
            .to_string_lossy()
            .to_string(),
        wal: Some(WalConfig {
            backend: Backend::Fs,
            path: wal_dir.path().join("%shard%").to_string_lossy().to_string(),
        }),
        apply_wal_on_close: true,
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let factory = ShardFactory::new(template, rate_limiter);

    // Open multiple shards
    let shard0 = factory.open(0).await.expect("open shard 0");
    let shard1 = factory.open(1).await.expect("open shard 1");

    // Write to both
    shard0.db().put(b"key0", b"value0").await.expect("put 0");
    shard1.db().put(b"key1", b"value1").await.expect("put 1");

    // Both WAL directories should exist
    let wal_path_0 = wal_dir.path().join("0");
    let wal_path_1 = wal_dir.path().join("1");
    assert!(wal_path_0.exists(), "shard 0 WAL should exist");
    assert!(wal_path_1.exists(), "shard 1 WAL should exist");

    // Close all
    factory.close_all().await.expect("close all shards");

    // Both WAL directories should be removed
    assert!(!wal_path_0.exists(), "shard 0 WAL should be removed");
    assert!(!wal_path_1.exists(), "shard 1 WAL should be removed");
}

#[silo::test]
async fn wal_config_is_local_storage_detection() {
    // Test that is_local_storage() correctly identifies local storage backends
    let fs_wal = WalConfig {
        backend: Backend::Fs,
        path: "/tmp/wal".to_string(),
    };
    assert!(fs_wal.is_local_storage(), "Fs backend should be local");

    let memory_wal = WalConfig {
        backend: Backend::Memory,
        path: "memory-wal".to_string(),
    };
    assert!(
        !memory_wal.is_local_storage(),
        "Memory backend should not be local"
    );

    let s3_wal = WalConfig {
        backend: Backend::S3,
        path: "s3://bucket/wal".to_string(),
    };
    assert!(!s3_wal.is_local_storage(), "S3 backend should not be local");

    let url_wal = WalConfig {
        backend: Backend::Url,
        path: "gs://bucket/wal".to_string(),
    };
    assert!(
        !url_wal.is_local_storage(),
        "Url backend should not be local"
    );
}

// =============================================================================
// Environment Variable Expansion Tests
// =============================================================================

#[test]
fn expand_env_vars_no_vars() {
    let input = "hello world";
    assert_eq!(expand_env_vars_for_test(input), "hello world");
}

#[test]
fn expand_env_vars_simple_var() {
    std::env::set_var("TEST_SILO_VAR", "test_value");
    let input = "prefix-${TEST_SILO_VAR}-suffix";
    assert_eq!(expand_env_vars_for_test(input), "prefix-test_value-suffix");
    std::env::remove_var("TEST_SILO_VAR");
}

#[test]
fn expand_env_vars_missing_var() {
    std::env::remove_var("TEST_SILO_MISSING");
    let input = "prefix-${TEST_SILO_MISSING}-suffix";
    assert_eq!(expand_env_vars_for_test(input), "prefix--suffix");
}

#[test]
fn expand_env_vars_default_value_used() {
    std::env::remove_var("TEST_SILO_DEFAULT");
    let input = "prefix-${TEST_SILO_DEFAULT:-fallback}-suffix";
    assert_eq!(expand_env_vars_for_test(input), "prefix-fallback-suffix");
}

#[test]
fn expand_env_vars_default_value_not_used() {
    std::env::set_var("TEST_SILO_DEFAULT2", "actual_value");
    let input = "prefix-${TEST_SILO_DEFAULT2:-fallback}-suffix";
    assert_eq!(
        expand_env_vars_for_test(input),
        "prefix-actual_value-suffix"
    );
    std::env::remove_var("TEST_SILO_DEFAULT2");
}

#[test]
fn expand_env_vars_multiple_vars() {
    std::env::set_var("TEST_SILO_A", "aaa");
    std::env::set_var("TEST_SILO_B", "bbb");
    let input = "${TEST_SILO_A}:${TEST_SILO_B}";
    assert_eq!(expand_env_vars_for_test(input), "aaa:bbb");
    std::env::remove_var("TEST_SILO_A");
    std::env::remove_var("TEST_SILO_B");
}

#[test]
fn expand_env_vars_unclosed_brace() {
    let input = "prefix-${UNCLOSED-suffix";
    // Malformed syntax is kept as-is
    assert_eq!(expand_env_vars_for_test(input), "prefix-${UNCLOSED-suffix");
}

#[test]
fn expand_env_vars_dollar_without_brace() {
    let input = "cost is $50";
    assert_eq!(expand_env_vars_for_test(input), "cost is $50");
}

#[test]
fn expand_env_vars_real_k8s_example() {
    std::env::set_var("POD_IP", "10.0.0.5");
    let input = "${POD_IP}:50051";
    assert_eq!(expand_env_vars_for_test(input), "10.0.0.5:50051");
    std::env::remove_var("POD_IP");
}

// =============================================================================
// Storage Tests
// =============================================================================

#[test]
fn resolve_object_store_fs_creates_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("new_subdir");

    // Directory doesn't exist yet
    assert!(!path.exists());

    let result = resolve_object_store(&Backend::Fs, &path.to_string_lossy());
    assert!(result.is_ok());

    // Directory should now exist
    assert!(path.exists());
}

#[test]
fn resolve_object_store_fs_canonicalizes_path() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();

    let result = resolve_object_store(&Backend::Fs, &path).unwrap();

    // Canonical path should not contain ".." or "." and should be absolute
    assert!(result.canonical_path.starts_with('/'));
    assert!(!result.canonical_path.contains(".."));
}

#[test]
fn resolve_object_store_memory_returns_same_path() {
    let result = resolve_object_store(&Backend::Memory, "my-test-path").unwrap();

    // Memory backend should return the path as-is
    assert_eq!(result.canonical_path, "my-test-path");
}
