use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
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
