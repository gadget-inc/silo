use serde::{Deserialize, Deserializer, Serialize};
use std::borrow::Cow;
use std::fs;
use std::path::Path;
use std::time::Duration;

/// Custom deserializer for slatedb::config::Settings that merges user-provided
/// values with defaults. This is necessary because slatedb's Settings struct
/// doesn't use #[serde(default)] on its fields, so serde requires all non-Option
/// fields to be present when any field is specified.
fn deserialize_slatedb_settings<'de, D>(
    deserializer: D,
) -> Result<Option<slatedb::config::Settings>, D::Error>
where
    D: Deserializer<'de>,
{
    // Deserialize as an optional TOML table
    let user_config: Option<toml::Table> = Option::deserialize(deserializer)?;

    match user_config {
        None => Ok(None),
        Some(user_table) => {
            // Serialize defaults to TOML
            let defaults = slatedb::config::Settings::default();
            let default_str = toml::to_string(&defaults).map_err(serde::de::Error::custom)?;
            let mut merged: toml::Table =
                toml::from_str(&default_str).map_err(serde::de::Error::custom)?;

            // Deep merge user config into defaults
            deep_merge_toml(&mut merged, user_table);

            // Deserialize merged config back to Settings
            let settings: slatedb::config::Settings =
                merged.try_into().map_err(serde::de::Error::custom)?;

            Ok(Some(settings))
        }
    }
}

/// Recursively merge overlay table into base table. Values in overlay take precedence.
fn deep_merge_toml(base: &mut toml::Table, overlay: toml::Table) {
    for (key, value) in overlay {
        match (base.get_mut(&key), value) {
            // Both are tables: recursively merge
            (Some(toml::Value::Table(base_table)), toml::Value::Table(overlay_table)) => {
                deep_merge_toml(base_table, overlay_table);
            }
            // Otherwise: overlay value replaces base
            (_, value) => {
                base.insert(key, value);
            }
        }
    }
}

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
    #[serde(default)]
    pub metrics: MetricsConfig,
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

/// Prometheus metrics configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MetricsConfig {
    /// Enable Prometheus metrics endpoint
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,
    /// Metrics listen address (e.g., "127.0.0.1:9090")
    #[serde(default = "default_metrics_addr")]
    pub addr: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            addr: default_metrics_addr(),
        }
    }
}

fn default_metrics_enabled() -> bool {
    true
}

fn default_metrics_addr() -> String {
    "127.0.0.1:9090".to_string()
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

pub const DEFAULT_CONCURRENCY_RECONCILE_INTERVAL_MS: u64 = 5000;

fn default_concurrency_reconcile_interval_ms() -> u64 {
    DEFAULT_CONCURRENCY_RECONCILE_INTERVAL_MS
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
    ///
    /// This ensures durability and allows shards to be reopened elsewhere.
    /// Defaults to true.
    #[serde(default = "default_apply_wal_on_close")]
    pub apply_wal_on_close: bool,
    /// Interval in milliseconds for periodic concurrency request reconciliation.
    /// This background task scans pending requests and re-triggers grant processing
    /// to self-heal from missed in-memory notifications.
    #[serde(default = "default_concurrency_reconcile_interval_ms")]
    pub concurrency_reconcile_interval_ms: u64,
    /// Optional SlateDB-specific settings for tuning database performance.
    /// If not specified, SlateDB defaults are used. When partially specified,
    /// unspecified fields use SlateDB defaults.
    /// See <https://docs.rs/slatedb/latest/slatedb/config/struct.Settings.html> for all options.
    #[serde(default, deserialize_with = "deserialize_slatedb_settings")]
    pub slatedb: Option<slatedb::config::Settings>,
    /// Optional in-memory cache size configuration for SlateDB.
    /// Controls the sizes of the block cache (data blocks) and meta cache
    /// (SST indexes and bloom filters). If not specified, SlateDB defaults are used
    /// (512 MB for block cache, 128 MB for meta cache).
    #[serde(default)]
    pub memory_cache: Option<MemoryCacheConfig>,
    /// Settings that control how / whether compaction runs for this silo
    /// instance. Used by the compaction A/B test harness.
    #[serde(default)]
    pub compaction: CompactionConfig,
}

/// Compaction-related tuning for a silo instance. All fields default to
/// "compact as SlateDB does out of the box" — change these for the harness.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct CompactionConfig {
    /// Disable the in-process SlateDB compactor for this silo instance.
    /// When true, the opened SlateDB will NOT run compaction; an external
    /// process (e.g. `silo-compactor`) is expected to compact the store.
    ///
    /// Implemented by forcing `compactor_options = None` on the merged
    /// SlateDB settings before opening the database.
    #[serde(default)]
    pub disable: bool,
    /// Compaction scheduler selection and tuning. Today the only variant
    /// available in SlateDB is `size_tiered`; the enum is structured so future
    /// schedulers (e.g. leveled) can be added without a config breaking change.
    #[serde(default)]
    pub scheduler: CompactionSchedulerConfig,
    /// Optional SlateDB compaction filter to install.
    #[serde(default)]
    pub filter: CompactionFilterConfig,
    /// When true, also emit slatedb compaction stats to structured logs on a
    /// periodic interval. Prometheus metrics are unaffected.
    #[serde(default)]
    pub log_stats: bool,
}

/// Compaction scheduler configuration. Tagged enum — new variants can be
/// added without breaking existing configs.
///
/// Example TOML:
/// ```toml
/// [database.compaction.scheduler]
/// kind = "size_tiered"
/// min_compaction_sources = 4
/// max_compaction_sources = 8
/// include_size_threshold = 4.0
/// ```
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CompactionSchedulerConfig {
    /// Size-tiered compaction (the only scheduler implemented in slatedb 0.12).
    SizeTiered {
        #[serde(default = "default_stcs_min_sources")]
        min_compaction_sources: usize,
        #[serde(default = "default_stcs_max_sources")]
        max_compaction_sources: usize,
        #[serde(default = "default_stcs_include_size_threshold")]
        include_size_threshold: f32,
    },
}

impl Default for CompactionSchedulerConfig {
    fn default() -> Self {
        Self::SizeTiered {
            min_compaction_sources: default_stcs_min_sources(),
            max_compaction_sources: default_stcs_max_sources(),
            include_size_threshold: default_stcs_include_size_threshold(),
        }
    }
}

fn default_stcs_min_sources() -> usize {
    4
}
fn default_stcs_max_sources() -> usize {
    8
}
fn default_stcs_include_size_threshold() -> f32 {
    4.0
}

/// Compaction filter selection for the A/B test harness.
#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CompactionFilterConfig {
    /// No compaction filter installed (default).
    #[default]
    None,
    /// Keep all entries but count what was seen and log a summary at the end
    /// of each compaction job. Used to validate the filter plumbing without
    /// changing compaction semantics.
    NoopCounting,
}

/// Configuration for SlateDB's in-memory caches.
///
/// SlateDB maintains two separate in-memory caches:
/// - **block_cache**: Caches data blocks read from SST files. Larger values reduce
///   I/O for point lookups and scans over recently-accessed data.
/// - **meta_cache**: Caches SST index blocks and bloom filters. Larger values avoid
///   re-reading and re-parsing SST indexes on each scan, which can be a significant
///   CPU cost for scan-heavy workloads.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MemoryCacheConfig {
    /// Size of the block cache in bytes. Defaults to 536870912 (512 MB).
    pub block_cache_bytes: Option<u64>,
    /// Size of the meta cache (SST indexes and bloom filters) in bytes.
    /// Defaults to 134217728 (128 MB).
    pub meta_cache_bytes: Option<u64>,
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_grpc_addr")]
    pub grpc_addr: String, // e.g. 127.0.0.1:7450
    /// Enable development mode features like ResetShards RPC.
    /// WARNING: This allows destructive operations and should never be enabled in production.
    #[serde(default)]
    pub dev_mode: bool,
    /// Maximum allowed execution time for a single SQL statement, in milliseconds.
    /// Defaults to 5000ms (5s). Set to 0 to disable statement timeout.
    #[serde(default = "default_statement_timeout_ms")]
    pub statement_timeout_ms: Option<u64>,
    /// Shared secret for gRPC authentication. When set, all incoming gRPC
    /// requests must include this token as a Bearer token in the `authorization`
    /// metadata header. When unset (default), authentication is disabled.
    /// Supports environment variable expansion, e.g. `${SILO_AUTH_TOKEN}`.
    #[serde(default)]
    pub auth_token: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            grpc_addr: default_grpc_addr(),
            dev_mode: false,
            statement_timeout_ms: default_statement_timeout_ms(),
            auth_token: None,
        }
    }
}

impl ServerConfig {
    pub fn statement_timeout(&self) -> Option<Duration> {
        self.statement_timeout_ms.and_then(|timeout_ms| {
            if timeout_ms == 0 {
                None
            } else {
                Some(Duration::from_millis(timeout_ms))
            }
        })
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WebUiConfig {
    /// Enable web UI server
    #[serde(default = "default_webui_enabled")]
    pub enabled: bool,
    /// Web UI listen address (e.g., "127.0.0.1:8080")
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

    /// Initial number of shards to create when bootstrapping a new cluster.
    /// This is only used during cluster initialization - once the cluster is
    /// created, the shard count is stored in the coordination backend and
    /// this config value is ignored. Default: 8.
    #[serde(default = "default_initial_shard_count")]
    pub initial_shard_count: u32,

    /// The gRPC address that other nodes should use to connect to this node.
    /// If not set, falls back to server.grpc_addr.
    ///
    /// This is useful when the bind address (e.g., 0.0.0.0:7450) differs from
    /// the routable address (e.g., pod-ip:7450 or service-name:7450).
    ///
    /// In Kubernetes, this should typically be set to the pod IP or a headless
    /// service DNS name like "$(POD_NAME).my-service.$(NAMESPACE).svc.cluster.local:7450".
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

    /// Placement rings this node participates in.
    /// Empty list means node only participates in the default ring.
    /// To participate in both default and named rings, explicitly include "default".
    #[serde(default)]
    pub placement_rings: Vec<String>,

    /// Stable node identity for this silo instance.
    /// If not set, a random UUID is generated on each startup.
    ///
    /// For Kubernetes StatefulSet deployments with permanent shard leases,
    /// set this to the pod name (e.g., `node_id = "${POD_NAME}"`) so that
    /// a restarted pod re-acquires its own leases automatically.
    #[serde(default)]
    pub node_id: Option<String>,
}

impl Default for CoordinationConfig {
    fn default() -> Self {
        Self {
            backend: CoordinationBackend::default(),
            cluster_prefix: default_cluster_prefix(),
            lease_ttl_secs: default_lease_ttl_secs(),
            initial_shard_count: default_initial_shard_count(),
            advertised_grpc_addr: None,
            etcd_endpoints: default_etcd_endpoints(),
            k8s_namespace: default_k8s_namespace(),
            placement_rings: Vec::new(),
            node_id: None,
        }
    }
}

fn default_initial_shard_count() -> u32 {
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
    "127.0.0.1:7450".to_string()
}

fn default_statement_timeout_ms() -> Option<u64> {
    Some(5_000)
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DatabaseConfig {
    pub name: String,
    pub backend: Backend,
    pub path: String,
    /// Optional separate WAL storage configuration.
    /// If not set, WAL uses the same backend/path as the main store.
    #[serde(default)]
    pub wal: Option<WalConfig>,
    /// Whether to flush WAL data to object storage (SSTs) before closing the shard.
    /// When true and a separate local WAL is configured, closing the shard will:
    /// 1. Flush all memtable data to SSTs in object storage
    /// 2. Close the database
    /// 3. Delete the local WAL directory
    ///
    /// This ensures durability and allows the shard to be reopened elsewhere.
    /// Defaults to true when WAL is configured.
    #[serde(default = "default_apply_wal_on_close")]
    pub apply_wal_on_close: bool,
    /// Optional SlateDB-specific settings for tuning database performance.
    /// If not specified, SlateDB defaults are used. When partially specified,
    /// unspecified fields use SlateDB defaults.
    /// See <https://docs.rs/slatedb/latest/slatedb/config/struct.Settings.html> for all options.
    #[serde(default, deserialize_with = "deserialize_slatedb_settings")]
    pub slatedb: Option<slatedb::config::Settings>,
    /// Optional in-memory cache size configuration for SlateDB.
    /// Controls the sizes of the block cache (data blocks) and meta cache
    /// (SST indexes and bloom filters). If not specified, SlateDB defaults are used
    /// (512 MB for block cache, 128 MB for meta cache).
    #[serde(default)]
    pub memory_cache: Option<MemoryCacheConfig>,
    /// Compaction-related settings — see [`CompactionConfig`].
    #[serde(default)]
    pub compaction: CompactionConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    Fs,
    S3,
    Gcs,
    Memory,
    Url,
    /// Turmoil simulated filesystem for deterministic simulation testing.
    /// Only available when the `dst` feature is enabled.
    #[cfg(feature = "dst")]
    TurmoilFs,
}

impl Backend {
    /// Returns true for backends that store data on a local filesystem.
    pub fn is_local_fs(&self) -> bool {
        match self {
            Backend::Fs => true,
            #[cfg(feature = "dst")]
            Backend::TurmoilFs => true,
            _ => false,
        }
    }
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
                statement_timeout_ms: default_statement_timeout_ms(),
                auth_token: None,
            },
            coordination: CoordinationConfig::default(),
            tenancy: TenancyConfig { enabled: false },
            gubernator: GubernatorSettings::default(),
            webui: WebUiConfig::default(),
            logging: LoggingConfig::default(),
            metrics: MetricsConfig::default(),
            database: DatabaseTemplate {
                backend: Backend::Fs,
                path: "/tmp/silo-%shard%".to_string(),
                wal: None,
                apply_wal_on_close: true,
                concurrency_reconcile_interval_ms: default_concurrency_reconcile_interval_ms(),
                slatedb: None,
                memory_cache: None,
                compaction: CompactionConfig::default(),
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
