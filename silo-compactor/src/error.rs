use crate::storage::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum CompactorError {
    #[error("config error: {0}")]
    Config(String),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("shard map loader error: {0}")]
    ShardLoader(String),
    #[error("pod discovery error: {0}")]
    PodDiscovery(String),
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),
    #[error("etcd error: {0}")]
    Etcd(String),
    #[error("slatedb error: {0}")]
    Slatedb(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
