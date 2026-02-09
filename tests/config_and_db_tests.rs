use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::job_store_shard::JobStoreShard;
use silo::settings::{
    AppConfig, Backend, DatabaseConfig, DatabaseTemplate, GubernatorSettings, LoggingConfig,
    WalConfig, WebUiConfig, expand_env_vars_for_test,
};
use silo::shard_range::{ShardMap, ShardRange};
use silo::storage::resolve_object_store;
use std::time::Duration;

/// Create SlateDB settings with a fast flush interval for tests
fn fast_flush_slatedb_settings() -> slatedb::config::Settings {
    slatedb::config::Settings {
        flush_interval: Some(Duration::from_millis(10)),
        ..Default::default()
    }
}

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
            slatedb: None,
        },
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let factory = ShardFactory::new(cfg.database.clone(), rate_limiter, None);
    let shard_map = ShardMap::create_initial(1).expect("create shard map");
    let shard_id = shard_map.shards()[0].id;
    let shard = factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");

    shard.db().put(b"k", b"v").await.expect("put");
    shard.db().flush().await.expect("flush");
    let got = shard.db().get(b"k").await.expect("get");
    assert_eq!(got.unwrap(), slatedb::bytes::Bytes::from_static(b"v"));
}

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
        wal: Some(WalConfig {
            backend: Backend::Fs,
            path: wal_dir.path().to_string_lossy().to_string(),
        }),
        apply_wal_on_close: true,
        slatedb: Some(fast_flush_slatedb_settings()),
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let shard = JobStoreShard::open(&cfg, rate_limiter, None, ShardRange::full())
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
        wal: None,
        apply_wal_on_close: true,
        slatedb: Some(fast_flush_slatedb_settings()),
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let shard = JobStoreShard::open(&cfg, rate_limiter, None, ShardRange::full())
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
        wal: Some(WalConfig {
            backend: Backend::Fs,
            path: wal_dir.path().to_string_lossy().to_string(),
        }),
        apply_wal_on_close: true,
        slatedb: Some(fast_flush_slatedb_settings()),
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let shard = JobStoreShard::open(&cfg, rate_limiter, None, ShardRange::full())
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
        wal: Some(WalConfig {
            backend: Backend::Fs,
            path: wal_dir.path().to_string_lossy().to_string(),
        }),
        apply_wal_on_close: false, // Disable apply on close
        slatedb: Some(fast_flush_slatedb_settings()),
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let shard = JobStoreShard::open(&cfg, rate_limiter, None, ShardRange::full())
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
            wal: Some(WalConfig {
                backend: Backend::Fs,
                path: wal_dir.path().to_string_lossy().to_string(),
            }),
            apply_wal_on_close: true,
            slatedb: Some(fast_flush_slatedb_settings()),
        };

        let rate_limiter = MockGubernatorClient::new_arc();
        let shard = JobStoreShard::open(&cfg, rate_limiter, None, ShardRange::full())
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
            wal: Some(WalConfig {
                backend: Backend::Fs,
                path: new_wal_dir.path().to_string_lossy().to_string(),
            }),
            apply_wal_on_close: true,
            slatedb: Some(fast_flush_slatedb_settings()),
        };

        let rate_limiter = MockGubernatorClient::new_arc();
        let shard = JobStoreShard::open(&cfg, rate_limiter, None, ShardRange::full())
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
        slatedb: None,
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let factory = ShardFactory::new(template, rate_limiter, None);

    // Create ShardMap and open multiple shards
    let shard_map = ShardMap::create_initial(2).expect("create shard map");
    let shard0_id = shard_map.shards()[0].id;
    let shard1_id = shard_map.shards()[1].id;
    let shard0 = factory
        .open(&shard0_id, &ShardRange::full())
        .await
        .expect("open shard 0");
    let shard1 = factory
        .open(&shard1_id, &ShardRange::full())
        .await
        .expect("open shard 1");

    // Write to both
    shard0.db().put(b"key0", b"value0").await.expect("put 0");
    shard1.db().put(b"key1", b"value1").await.expect("put 1");

    // Both WAL directories should exist (using UUID paths now)
    let wal_path_0 = wal_dir.path().join(shard0_id.to_string());
    let wal_path_1 = wal_dir.path().join(shard1_id.to_string());
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

#[silo::test]
fn expand_env_vars_no_vars() {
    let input = "hello world";
    assert_eq!(expand_env_vars_for_test(input), "hello world");
}

#[silo::test]
fn expand_env_vars_simple_var() {
    // SAFETY: Test runs single-threaded and uses a unique env var name
    unsafe { std::env::set_var("TEST_SILO_VAR", "test_value") };
    let input = "prefix-${TEST_SILO_VAR}-suffix";
    assert_eq!(expand_env_vars_for_test(input), "prefix-test_value-suffix");
    unsafe { std::env::remove_var("TEST_SILO_VAR") };
}

#[silo::test]
fn expand_env_vars_missing_var() {
    // SAFETY: Test runs single-threaded and uses a unique env var name
    unsafe { std::env::remove_var("TEST_SILO_MISSING") };
    let input = "prefix-${TEST_SILO_MISSING}-suffix";
    assert_eq!(expand_env_vars_for_test(input), "prefix--suffix");
}

#[silo::test]
fn expand_env_vars_default_value_used() {
    // SAFETY: Test runs single-threaded and uses a unique env var name
    unsafe { std::env::remove_var("TEST_SILO_DEFAULT") };
    let input = "prefix-${TEST_SILO_DEFAULT:-fallback}-suffix";
    assert_eq!(expand_env_vars_for_test(input), "prefix-fallback-suffix");
}

#[silo::test]
fn expand_env_vars_default_value_not_used() {
    // SAFETY: Test runs single-threaded and uses a unique env var name
    unsafe { std::env::set_var("TEST_SILO_DEFAULT2", "actual_value") };
    let input = "prefix-${TEST_SILO_DEFAULT2:-fallback}-suffix";
    assert_eq!(
        expand_env_vars_for_test(input),
        "prefix-actual_value-suffix"
    );
    unsafe { std::env::remove_var("TEST_SILO_DEFAULT2") };
}

#[silo::test]
fn expand_env_vars_multiple_vars() {
    // SAFETY: Test runs single-threaded and uses unique env var names
    unsafe {
        std::env::set_var("TEST_SILO_A", "aaa");
        std::env::set_var("TEST_SILO_B", "bbb");
    }
    let input = "${TEST_SILO_A}:${TEST_SILO_B}";
    assert_eq!(expand_env_vars_for_test(input), "aaa:bbb");
    unsafe {
        std::env::remove_var("TEST_SILO_A");
        std::env::remove_var("TEST_SILO_B");
    }
}

#[silo::test]
fn expand_env_vars_unclosed_brace() {
    let input = "prefix-${UNCLOSED-suffix";
    // Malformed syntax is kept as-is
    assert_eq!(expand_env_vars_for_test(input), "prefix-${UNCLOSED-suffix");
}

#[silo::test]
fn expand_env_vars_dollar_without_brace() {
    let input = "cost is $50";
    assert_eq!(expand_env_vars_for_test(input), "cost is $50");
}

#[silo::test]
fn expand_env_vars_real_k8s_example() {
    // SAFETY: Test runs single-threaded and uses a unique env var name
    unsafe { std::env::set_var("POD_IP", "10.0.0.5") };
    let input = "${POD_IP}:7450";
    assert_eq!(expand_env_vars_for_test(input), "10.0.0.5:7450");
    unsafe { std::env::remove_var("POD_IP") };
}

#[silo::test]
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

#[silo::test]
fn resolve_object_store_fs_canonicalizes_path() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().to_string_lossy().to_string();

    let result = resolve_object_store(&Backend::Fs, &path).unwrap();

    // Root path should not contain ".." or "." and should be absolute
    // (canonical_path is empty for LocalFileSystem since the root is set in the prefix)
    assert!(result.root_path.starts_with('/'));
    assert!(!result.root_path.contains(".."));
}

#[silo::test]
fn resolve_object_store_memory_returns_same_path() {
    let result = resolve_object_store(&Backend::Memory, "my-test-path").unwrap();

    // Memory backend should return the path as-is
    assert_eq!(result.canonical_path, "my-test-path");
}

#[silo::test]
fn parse_toml_with_full_slatedb_settings() {
    // Test that TOML config with full [database.slatedb] section parses correctly
    // Note: SlateDB's Settings requires all fields when deserializing via TOML/serde
    // because it doesn't use #[serde(default)] on individual fields
    let toml_str = r#"
[database]
backend = "fs"
path = "/tmp/silo-%shard%"

[database.slatedb]
flush_interval = "50ms"
manifest_poll_interval = "1s"
manifest_update_timeout = "300s"
min_filter_keys = 1000
filter_bits_per_key = 10
l0_sst_size_bytes = 33554432
l0_max_ssts = 4
max_unflushed_bytes = 536870912

[database.slatedb.compactor_options]
poll_interval = "5s"
manifest_update_timeout = "300s"
max_sst_size = 268435456
max_concurrent_compactions = 4

[database.slatedb.object_store_cache_options]
max_cache_size_bytes = 17179869184
part_size_bytes = 4194304
cache_puts = false
scan_interval = "3600s"
"#;

    let cfg: AppConfig = toml::from_str(toml_str).expect("parse TOML");

    // Verify the slatedb settings were parsed
    let slatedb_settings = cfg
        .database
        .slatedb
        .expect("slatedb settings should be present");
    assert_eq!(
        slatedb_settings.flush_interval,
        Some(std::time::Duration::from_millis(50))
    );
    assert_eq!(slatedb_settings.l0_sst_size_bytes, 33554432);
    assert_eq!(slatedb_settings.l0_max_ssts, 4);
}

#[silo::test]
fn parse_toml_without_slatedb_settings() {
    // Test that TOML config without [database.slatedb] section still works
    let toml_str = r#"
[database]
backend = "fs"
path = "/tmp/silo-%shard%"
"#;

    let cfg: AppConfig = toml::from_str(toml_str).expect("parse TOML");

    // slatedb should be None when not specified
    assert!(cfg.database.slatedb.is_none());
}

#[silo::test]
fn parse_toml_with_partial_slatedb_settings_merges_with_defaults() {
    // Regression test: when only some slatedb settings are specified,
    // unspecified fields should use slatedb defaults instead of failing.
    // This matches the behavior users expect from slatedb's own config loaders.
    let toml_str = r#"
[database]
backend = "fs"
path = "/tmp/silo-%shard%"

[database.slatedb]
# Only specify flush_interval - other fields should use defaults
flush_interval = "1ms"

[database.slatedb.object_store_cache_options]
# Only specify cache options we care about
root_folder = "/var/cache"
cache_puts = true
"#;

    let cfg: AppConfig = toml::from_str(toml_str).expect("parse TOML with partial slatedb config");

    let slatedb_settings = cfg
        .database
        .slatedb
        .expect("slatedb settings should be present");

    // Verify user-specified values are used
    assert_eq!(
        slatedb_settings.flush_interval,
        Some(std::time::Duration::from_millis(1)),
        "flush_interval should use user-specified value"
    );
    assert_eq!(
        slatedb_settings
            .object_store_cache_options
            .root_folder
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        Some("/var/cache".to_string()),
        "root_folder should use user-specified value"
    );
    assert!(
        slatedb_settings.object_store_cache_options.cache_puts,
        "cache_puts should use user-specified value"
    );

    // Verify unspecified fields use slatedb defaults
    let defaults = slatedb::config::Settings::default();
    assert_eq!(
        slatedb_settings.manifest_poll_interval, defaults.manifest_poll_interval,
        "manifest_poll_interval should use default"
    );
    assert_eq!(
        slatedb_settings.l0_sst_size_bytes, defaults.l0_sst_size_bytes,
        "l0_sst_size_bytes should use default"
    );
    assert_eq!(
        slatedb_settings.l0_max_ssts, defaults.l0_max_ssts,
        "l0_max_ssts should use default"
    );
    assert_eq!(
        slatedb_settings.filter_bits_per_key, defaults.filter_bits_per_key,
        "filter_bits_per_key should use default"
    );
    assert_eq!(
        slatedb_settings.object_store_cache_options.part_size_bytes,
        defaults.object_store_cache_options.part_size_bytes,
        "part_size_bytes should use default"
    );
}

#[silo::test]
async fn factory_passes_slatedb_settings_to_shards() {
    // Test that ShardFactory passes slatedb settings from template to opened shards
    let tmp = tempfile::tempdir().unwrap();

    let template = DatabaseTemplate {
        backend: Backend::Fs,
        path: tmp.path().join("%shard%").to_string_lossy().to_string(),
        wal: None,
        apply_wal_on_close: true,
        slatedb: Some(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(25)),
            l0_sst_size_bytes: 16777216, // 16MB
            ..Default::default()
        }),
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let factory = ShardFactory::new(template, rate_limiter, None);

    let shard_map = ShardMap::create_initial(1).expect("create shard map");
    let shard_id = shard_map.shards()[0].id;
    let shard = factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");

    // Verify the shard works with the custom settings
    shard.db().put(b"key", b"value").await.expect("put");
    shard.db().flush().await.expect("flush");
    let result = shard.db().get(b"key").await.expect("get");
    assert_eq!(result.unwrap().as_ref(), b"value");
}
