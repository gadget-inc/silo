use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub coordination: CoordinationConfig,
    pub database: DatabaseTemplate,
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
}

#[derive(Debug, Deserialize, Clone)]
pub struct CoordinationConfig {
    /// List of etcd endpoints, e.g. ["http://127.0.0.1:2379"]
    #[serde(default = "default_etcd_endpoints")]
    pub etcd_endpoints: Vec<String>,
}

impl Default for CoordinationConfig {
    fn default() -> Self {
        Self {
            etcd_endpoints: default_etcd_endpoints(),
        }
    }
}

fn default_etcd_endpoints() -> Vec<String> {
    vec!["http://127.0.0.1:2379".to_string()]
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

impl AppConfig {
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let default = Self {
            server: ServerConfig {
                grpc_addr: default_grpc_addr(),
            },
            coordination: CoordinationConfig {
                etcd_endpoints: default_etcd_endpoints(),
            },
            database: DatabaseTemplate {
                backend: Backend::Fs,
                path: "/tmp/silo-%shard%".to_string(),
            },
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
