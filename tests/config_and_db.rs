use silo::settings::{AppConfig, Backend, DatabaseConfig};
use silo::storage::DbFactory;

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

    let mut factory = DbFactory::new();
    let db = factory.open(&cfg.databases[0]).await.expect("open db");

    db.put(b"k", b"v").await.expect("put");
    db.flush().await.expect("flush");
    let got = db.get(b"k").await.expect("get");
    assert_eq!(got.unwrap(), slatedb::bytes::Bytes::from_static(b"v"));
}


