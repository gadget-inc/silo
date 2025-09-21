use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    pub database: DatabaseTemplate,
    /// Optional Raft cluster section (OpenRaft-based). Minimal fields we need.
    #[serde(default)]
    pub raft: Option<RaftConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseTemplate {
    pub backend: Backend,
    /// May contain "%shard%" placeholder that will be replaced with the shard number
    pub path: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ServerConfig {
    #[serde(default = "default_grpc_addr")]
    pub grpc_addr: String, // e.g. 127.0.0.1:50051
    #[serde(default)]
    pub single_node: bool,
}

fn default_grpc_addr() -> String {
    "127.0.0.1:50051".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub name: String,
    pub backend: Backend,
    pub path: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    Fs,
    S3,
    Memory,
    Url,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct RaftConfig {
    pub node_id: u64,
    pub listen_address: String, // host:port for raft rpc
    pub db_root_dir: String,
    pub log_dir: String,
    #[serde(default)]
    pub initial_cluster: Vec<RaftNode>,
    /// OpenRaft configuration; serde default allows omitting to use OpenRaft defaults.
    #[serde(default)]
    pub openraft: openraft::Config,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct RaftNode {
    pub id: u64,
    pub address: String,
}

impl AppConfig {
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let default = Self {
            server: ServerConfig {
                grpc_addr: default_grpc_addr(),
                single_node: false,
            },
            database: DatabaseTemplate {
                backend: Backend::Fs,
                path: "/tmp/silo-%shard%".to_string(),
            },
            raft: None,
        };

        match path {
            Some(p) => {
                let data = fs::read_to_string(p)?;
                let cfg: Self = toml::from_str(&data)?;
                Ok(cfg)
            }
            None => Ok(default),
        }
    }
}
