use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub coordination: CoordinationConfig,
    #[serde(default)]
    pub tenancy: TenancyConfig,
    #[serde(default)]
    pub gubernator: GubernatorSettings,
    #[serde(default)]
    pub webui: WebUiConfig,
    pub database: DatabaseTemplate,
}

/// Settings for Gubernator rate limiting service
#[derive(Debug, Deserialize, Clone, Default)]
pub struct GubernatorSettings {
    /// Gubernator server address (e.g., "http://localhost:1051")
    /// If not set, rate limiting will be disabled and rate limit tasks will fail.
    pub address: Option<String>,
    /// Maximum time to wait for coalescing before sending a batch (default: 5ms)
    #[serde(default = "default_coalesce_interval_ms")]
    pub coalesce_interval_ms: u64,
    /// Maximum number of requests to batch together (default: 100)
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// Connection timeout in milliseconds (default: 5000ms)
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Request timeout in milliseconds (default: 10000ms)
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

fn default_coalesce_interval_ms() -> u64 {
    5
}

fn default_max_batch_size() -> usize {
    100
}

fn default_connect_timeout_ms() -> u64 {
    5000
}

fn default_request_timeout_ms() -> u64 {
    10000
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
pub struct WebUiConfig {
    /// Enable web UI server
    #[serde(default = "default_webui_enabled")]
    pub enabled: bool,
    /// Web UI listen address (e.g., "127.0.0.1:50052")
    #[serde(default = "default_webui_addr")]
    pub addr: String,
}

impl Default for WebUiConfig {
    fn default() -> Self {
        Self {
            enabled: default_webui_enabled(),
            addr: default_webui_addr(),
        }
    }
}

fn default_webui_enabled() -> bool {
    true
}

fn default_webui_addr() -> String {
    "127.0.0.1:50052".to_string()
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

#[derive(Debug, Deserialize, Clone, Default)]
pub struct TenancyConfig {
    #[serde(default)]
    pub enabled: bool,
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
    /// Optional flush interval for SlateDB. If None, uses SlateDB's default.
    #[serde(default)]
    pub flush_interval_ms: Option<u64>,
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
            tenancy: TenancyConfig { enabled: false },
            gubernator: GubernatorSettings::default(),
            webui: WebUiConfig::default(),
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

impl GubernatorSettings {
    /// Convert settings to a GubernatorConfig if an address is configured
    pub fn to_config(&self) -> Option<crate::gubernator::GubernatorConfig> {
        self.address.as_ref().map(|addr| crate::gubernator::GubernatorConfig {
            address: addr.clone(),
            coalesce_interval_ms: self.coalesce_interval_ms,
            max_batch_size: self.max_batch_size,
            connect_timeout_ms: self.connect_timeout_ms,
            request_timeout_ms: self.request_timeout_ms,
        })
    }
}
