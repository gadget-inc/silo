use silo::settings::{AppConfig, Backend, DatabaseConfig};
use silo::factory::ShardFactory;

#[tokio::test]
async fn open_fs_db_from_config() {
    let tmp = tempfile::tempdir().unwrap();

    let cfg = AppConfig {
        databases: vec![DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: tmp.path().to_string_lossy().to_string(),
        }],
    };

    let mut factory = ShardFactory::new();
    let shard = factory.open(&cfg.databases[0]).await.expect("open shard");

    shard.db().put(b"k", b"v").await.expect("put");
    shard.db().flush().await.expect("flush");
    let got = shard.db().get(b"k").await.expect("get");
    assert_eq!(got.unwrap(), slatedb::bytes::Bytes::from_static(b"v"));
}


