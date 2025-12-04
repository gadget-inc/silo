use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::{AppConfig, Backend, DatabaseTemplate, GubernatorSettings, WebUiConfig};

#[silo::test]
async fn open_fs_db_from_config() {
    let tmp = tempfile::tempdir().unwrap();

    let cfg = AppConfig {
        server: Default::default(),
        coordination: Default::default(),
        tenancy: silo::settings::TenancyConfig { enabled: false },
        gubernator: GubernatorSettings::default(),
        webui: WebUiConfig::default(),
        database: DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
            wal: None,
        },
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let mut factory = ShardFactory::new(cfg.database.clone(), rate_limiter);
    let shard = factory.open(0).await.expect("open shard");

    shard.db().put(b"k", b"v").await.expect("put");
    shard.db().flush().await.expect("flush");
    let got = shard.db().get(b"k").await.expect("get");
    assert_eq!(got.unwrap(), slatedb::bytes::Bytes::from_static(b"v"));
}
