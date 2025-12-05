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

fn default_apply_wal_on_close() -> bool {
    true
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseTemplate {
    pub backend: Backend,
    /// May contain "%shard%" placeholder that will be replaced with the shard number
    pub path: String,
    /// Optional separate WAL storage configuration.
    /// If not set, WAL uses the same backend/path as the main store.
    #[serde(default)]
    pub wal: Option<WalConfig>,
    /// Whether to flush WAL data to object storage (SSTs) before closing shards.
    /// When true and a separate local WAL is configured, closing a shard will:
    /// 1. Flush all memtable data to SSTs in object storage
    /// 2. Close the database
    /// 3. Delete the local WAL directory
    /// This ensures durability and allows shards to be reopened elsewhere.
    /// Defaults to true.
    #[serde(default = "default_apply_wal_on_close")]
    pub apply_wal_on_close: bool,
}

/// Configuration for a separate WAL object store
#[derive(Debug, Deserialize, Clone)]
pub struct WalConfig {
    pub backend: Backend,
    /// May contain "%shard%" placeholder that will be replaced with the shard number
    pub path: String,
}

impl WalConfig {
    /// Returns true if the WAL is stored on local filesystem (as opposed to object storage).
    /// Local WAL storage requires special handling on close to ensure durability.
    pub fn is_local_storage(&self) -> bool {
        matches!(self.backend, Backend::Fs)
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ServerConfig {
    #[serde(default = "default_grpc_addr")]
    pub grpc_addr: String, // e.g. 127.0.0.1:50051
    /// Enable development mode features like ResetShards RPC.
    /// WARNING: This allows destructive operations and should never be enabled in production.
    #[serde(default)]
    pub dev_mode: bool,
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
    "127.0.0.1:8080".to_string()
}

/// Coordination backend type
#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CoordinationBackend {
    /// No coordination - single node mode for local development
    #[default]
    None,
    /// etcd-based distributed coordination
    Etcd,
    /// Kubernetes Lease-based coordination
    K8s,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CoordinationConfig {
    /// Which coordination backend to use
    #[serde(default)]
    pub backend: CoordinationBackend,

    /// Cluster prefix for namespacing coordination keys/leases
    #[serde(default = "default_cluster_prefix")]
    pub cluster_prefix: String,

    /// Lease TTL in seconds (applies to etcd and k8s backends)
    #[serde(default = "default_lease_ttl_secs")]
    pub lease_ttl_secs: i64,

    /// Number of shards in the cluster (default: 8)
    #[serde(default = "default_num_shards")]
    pub num_shards: u32,

    // ---- etcd-specific settings ----
    /// List of etcd endpoints, e.g. ["http://127.0.0.1:2379"]
    #[serde(default = "default_etcd_endpoints")]
    pub etcd_endpoints: Vec<String>,

    // ---- k8s-specific settings ----
    /// Kubernetes namespace for Lease objects (default: "default")
    #[serde(default = "default_k8s_namespace")]
    pub k8s_namespace: String,
}

impl Default for CoordinationConfig {
    fn default() -> Self {
        Self {
            backend: CoordinationBackend::default(),
            cluster_prefix: default_cluster_prefix(),
            lease_ttl_secs: default_lease_ttl_secs(),
            num_shards: default_num_shards(),
            etcd_endpoints: default_etcd_endpoints(),
            k8s_namespace: default_k8s_namespace(),
        }
    }
}

fn default_num_shards() -> u32 {
    8
}

fn default_cluster_prefix() -> String {
    "silo".to_string()
}

fn default_lease_ttl_secs() -> i64 {
    10
}

fn default_k8s_namespace() -> String {
    "default".to_string()
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
    /// Optional separate WAL storage configuration.
    /// If not set, WAL uses the same backend/path as the main store.
    #[serde(default)]
    pub wal: Option<WalConfig>,
    /// Whether to flush WAL data to object storage (SSTs) before closing the shard.
    /// When true and a separate local WAL is configured, closing the shard will:
    /// 1. Flush all memtable data to SSTs in object storage
    /// 2. Close the database
    /// 3. Delete the local WAL directory
    /// This ensures durability and allows the shard to be reopened elsewhere.
    /// Defaults to true when WAL is configured.
    #[serde(default = "default_apply_wal_on_close")]
    pub apply_wal_on_close: bool,
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
                dev_mode: false,
            },
            coordination: CoordinationConfig::default(),
            tenancy: TenancyConfig { enabled: false },
            gubernator: GubernatorSettings::default(),
            webui: WebUiConfig::default(),
            database: DatabaseTemplate {
                backend: Backend::Fs,
                path: "/tmp/silo-%shard%".to_string(),
                wal: None,
                apply_wal_on_close: true,
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
        self.address
            .as_ref()
            .map(|addr| crate::gubernator::GubernatorConfig {
                address: addr.clone(),
                coalesce_interval_ms: self.coalesce_interval_ms,
                max_batch_size: self.max_batch_size,
                connect_timeout_ms: self.connect_timeout_ms,
                request_timeout_ms: self.request_timeout_ms,
            })
    }
}
