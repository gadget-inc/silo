use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Serialize, Clone)]
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
    #[serde(default)]
    pub logging: LoggingConfig,
    pub database: DatabaseTemplate,
}

/// Logging configuration
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct LoggingConfig {
    /// Log output format: "text" (default) or "json"
    #[serde(default)]
    pub format: LogFormat,
}

/// Log output format
#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable text format (default)
    #[default]
    Text,
    /// Structured JSON format
    Json,
}

/// Settings for Gubernator rate limiting service
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
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

#[derive(Debug, Deserialize, Serialize, Clone)]
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
#[derive(Debug, Deserialize, Serialize, Clone)]
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

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct ServerConfig {
    #[serde(default = "default_grpc_addr")]
    pub grpc_addr: String, // e.g. 127.0.0.1:50051
    /// Enable development mode features like ResetShards RPC.
    /// WARNING: This allows destructive operations and should never be enabled in production.
    #[serde(default)]
    pub dev_mode: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
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
#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq, Eq)]
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

#[derive(Debug, Deserialize, Serialize, Clone)]
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

    /// The gRPC address that other nodes should use to connect to this node.
    /// If not set, falls back to server.grpc_addr.
    ///
    /// This is useful when the bind address (e.g., 0.0.0.0:50051) differs from
    /// the routable address (e.g., pod-ip:50051 or service-name:50051).
    ///
    /// In Kubernetes, this should typically be set to the pod IP or a headless
    /// service DNS name like "$(POD_NAME).my-service.$(NAMESPACE).svc.cluster.local:50051".
    #[serde(default)]
    pub advertised_grpc_addr: Option<String>,

    // ---- etcd-specific settings ----
    /// List of etcd endpoints, e.g. ["http://127.0.0.1:2379"]
    #[serde(default = "default_etcd_endpoints")]
    pub etcd_endpoints: Vec<String>,

    // ---- k8s-specific settings ----
    /// Kubernetes namespace for Lease objects (default: "default")
    #[serde(default = "default_k8s_namespace")]
    pub k8s_namespace: String,

    // ---- Placement engine settings ----
    /// Configuration for the load-aware placement engine
    #[serde(default)]
    pub placement: PlacementConfig,
}

impl Default for CoordinationConfig {
    fn default() -> Self {
        Self {
            backend: CoordinationBackend::default(),
            cluster_prefix: default_cluster_prefix(),
            lease_ttl_secs: default_lease_ttl_secs(),
            num_shards: default_num_shards(),
            advertised_grpc_addr: None,
            etcd_endpoints: default_etcd_endpoints(),
            k8s_namespace: default_k8s_namespace(),
            placement: PlacementConfig::default(),
        }
    }
}

/// Configuration for the load-aware shard placement engine.
///
/// The placement engine monitors load across all nodes and moves shards
/// between nodes to balance the load. It is conservative by default to
/// avoid constant shard movement (flapping).
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PlacementConfig {
    /// Enable the load-aware placement engine (default: false)
    /// When disabled, only rendezvous hashing is used for shard placement.
    #[serde(default)]
    pub enabled: bool,

    /// Minimum ratio of max node load to average load to trigger rebalancing.
    /// A value of 2.0 means the busiest node must have 2x the average load
    /// before we consider moving shards. (default: 2.0)
    #[serde(default = "default_load_imbalance_threshold")]
    pub load_imbalance_threshold: f64,

    /// Number of consecutive intervals the load must be imbalanced before acting.
    /// This provides hysteresis to avoid reacting to transient spikes. (default: 3)
    #[serde(default = "default_stability_intervals")]
    pub stability_intervals: u32,

    /// Minimum time between migrations in seconds.
    /// This cooldown prevents rapid successive migrations. (default: 60)
    #[serde(default = "default_migration_cooldown_secs")]
    pub migration_cooldown_secs: u32,

    /// Maximum number of migrations allowed per hour.
    /// Acts as a circuit breaker to prevent runaway rebalancing. (default: 10)
    #[serde(default = "default_max_migrations_per_hour")]
    pub max_migrations_per_hour: u32,

    /// How often to collect load metrics from nodes in seconds. (default: 5)
    #[serde(default = "default_load_collection_interval_secs")]
    pub load_collection_interval_secs: u32,

    /// TTL for placement overrides in seconds.
    /// Overrides expire if not refreshed, allowing the system to self-heal
    /// if the placement engine leader fails. (default: 300 = 5 minutes)
    #[serde(default = "default_override_ttl_secs")]
    pub override_ttl_secs: u32,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            load_imbalance_threshold: default_load_imbalance_threshold(),
            stability_intervals: default_stability_intervals(),
            migration_cooldown_secs: default_migration_cooldown_secs(),
            max_migrations_per_hour: default_max_migrations_per_hour(),
            load_collection_interval_secs: default_load_collection_interval_secs(),
            override_ttl_secs: default_override_ttl_secs(),
        }
    }
}

fn default_load_imbalance_threshold() -> f64 {
    2.0
}

fn default_stability_intervals() -> u32 {
    3
}

fn default_migration_cooldown_secs() -> u32 {
    60
}

fn default_max_migrations_per_hour() -> u32 {
    10
}

fn default_load_collection_interval_secs() -> u32 {
    5
}

fn default_override_ttl_secs() -> u32 {
    300 // 5 minutes
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

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
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

#[derive(Debug, Deserialize, Serialize, Clone)]
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

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    Fs,
    S3,
    Gcs,
    Memory,
    Url,
}

/// Expand environment variables in a string.
///
/// Supports two syntaxes:
/// - `${VAR}` - expands to the value of VAR, or empty string if not set
/// - `${VAR:-default}` - expands to the value of VAR, or "default" if not set
///
/// This is useful for Kubernetes deployments where you need to inject
/// values like pod IP via the Downward API.
fn expand_env_vars(input: &str) -> Cow<'_, str> {
    use std::env;

    // Quick check: if no `${` exists, return as-is
    if !input.contains("${") {
        return Cow::Borrowed(input);
    }

    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'

            // Find the closing '}'
            let mut var_content = String::new();
            let mut found_close = false;
            for ch in chars.by_ref() {
                if ch == '}' {
                    found_close = true;
                    break;
                }
                var_content.push(ch);
            }

            if found_close {
                // Check for default value syntax: VAR:-default
                let (var_name, default_value) = if let Some(pos) = var_content.find(":-") {
                    (&var_content[..pos], Some(&var_content[pos + 2..]))
                } else {
                    (var_content.as_str(), None)
                };

                match env::var(var_name) {
                    Ok(val) => result.push_str(&val),
                    Err(_) => {
                        if let Some(default) = default_value {
                            result.push_str(default);
                        }
                        // If no default and var not set, expand to empty string
                    }
                }
            } else {
                // Malformed: no closing brace, output as-is
                result.push('$');
                result.push('{');
                result.push_str(&var_content);
            }
        } else {
            result.push(c);
        }
    }

    Cow::Owned(result)
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
            logging: LoggingConfig::default(),
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
                // Expand environment variables in the config file before parsing.
                // This allows using ${VAR} or ${VAR:-default} syntax in config values.
                let expanded = expand_env_vars(&data);
                let cfg: Self = toml::from_str(&expanded)?;
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

/// Expand environment variables in a string (public for testing).
///
/// See [`expand_env_vars`] for details.
#[doc(hidden)]
pub fn expand_env_vars_for_test(input: &str) -> String {
    expand_env_vars(input).into_owned()
}
