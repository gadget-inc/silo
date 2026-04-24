use std::borrow::Cow;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Deserializer};

use crate::compaction_filter::DEFAULT_RETENTION_SECS;
use crate::storage::Backend;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub storage: StorageConfig,
    pub shard_discovery: ShardDiscoveryConfig,
    #[serde(default)]
    pub coordinator: CoordinatorConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default, deserialize_with = "deserialize_compactor_options")]
    pub compactor_options: Option<slatedb::config::CompactorOptions>,
    #[serde(default)]
    pub compaction_filter: CompactionFilterConfig,
}

/// Compaction filter selection. Defaults to [`CompactionFilterConfig::None`]
/// so existing deployments keep current behavior after the upgrade.
#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CompactionFilterConfig {
    #[default]
    None,
    /// Delete terminal (Succeeded/Failed/Cancelled) job data older than
    /// `retention_secs`. Defaults to 7 days.
    CompletedJobs {
        #[serde(default = "default_completed_jobs_retention_secs")]
        retention_secs: u64,
    },
}

fn default_completed_jobs_retention_secs() -> u64 {
    DEFAULT_RETENTION_SECS
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageConfig {
    pub backend: Backend,
    /// Path with `%shard%` placeholder, replaced by the shard UUID at runtime.
    pub path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ShardDiscoveryConfig {
    pub backend: ShardDiscoveryBackend,
    pub cluster_prefix: String,
    #[serde(default)]
    pub etcd_endpoints: Vec<String>,
    #[serde(default = "default_k8s_namespace")]
    pub k8s_namespace: String,
    #[serde(default)]
    pub placement_ring: Option<String>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShardDiscoveryBackend {
    Etcd,
    K8sConfigmap,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CoordinatorConfig {
    #[serde(default)]
    pub mode: CoordinatorMode,
    #[serde(default)]
    pub self_pod_name: Option<String>,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default = "default_pod_selector")]
    pub pod_label_selector: String,
    #[serde(default = "default_max_shards_per_pod")]
    pub max_shards_per_pod: usize,
    #[serde(
        default = "default_rebalance_interval",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub rebalance_interval: Duration,
    #[serde(
        default = "default_worker_backoff",
        deserialize_with = "duration_serde::deserialize"
    )]
    pub worker_restart_backoff: Duration,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            mode: CoordinatorMode::default(),
            self_pod_name: None,
            namespace: None,
            pod_label_selector: default_pod_selector(),
            max_shards_per_pod: default_max_shards_per_pod(),
            rebalance_interval: default_rebalance_interval(),
            worker_restart_backoff: default_worker_backoff(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CoordinatorMode {
    #[default]
    K8sDeployment,
    Single,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct LoggingConfig {
    #[serde(default)]
    pub format: LogFormat,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

fn default_k8s_namespace() -> String {
    "default".into()
}
fn default_pod_selector() -> String {
    "app=silo-compactor".into()
}
fn default_max_shards_per_pod() -> usize {
    12
}
fn default_rebalance_interval() -> Duration {
    Duration::from_secs(30)
}
fn default_worker_backoff() -> Duration {
    Duration::from_secs(5)
}

impl AppConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        let expanded = expand_env_vars(&raw);
        let cfg: Self = toml::from_str(&expanded)?;
        Ok(cfg)
    }

    /// Best-effort resolution of self pod name. Order: explicit config →
    /// `POD_NAME` env var → system hostname.
    pub fn resolve_self_pod_name(&self) -> anyhow::Result<String> {
        if let Some(n) = &self.coordinator.self_pod_name
            && !n.is_empty()
        {
            return Ok(n.clone());
        }
        if let Ok(n) = std::env::var("POD_NAME")
            && !n.is_empty()
        {
            return Ok(n);
        }
        let h = hostname::get()?.to_string_lossy().into_owned();
        Ok(h)
    }
}

/// Deep-merge user TOML overlay onto `slatedb::config::CompactorOptions::default()`.
/// Mirrors `silo::settings::deserialize_slatedb_settings` so partial overrides
/// of a few fields don't require specifying the rest.
fn deserialize_compactor_options<'de, D>(
    d: D,
) -> Result<Option<slatedb::config::CompactorOptions>, D::Error>
where
    D: Deserializer<'de>,
{
    let user: Option<toml::Table> = Option::deserialize(d)?;
    let Some(user_table) = user else {
        return Ok(None);
    };
    let defaults = slatedb::config::CompactorOptions::default();
    let default_str = toml::to_string(&defaults).map_err(serde::de::Error::custom)?;
    let mut merged: toml::Table = toml::from_str(&default_str).map_err(serde::de::Error::custom)?;
    deep_merge_toml(&mut merged, user_table);
    let opts: slatedb::config::CompactorOptions =
        merged.try_into().map_err(serde::de::Error::custom)?;
    Ok(Some(opts))
}

fn deep_merge_toml(base: &mut toml::Table, overlay: toml::Table) {
    for (key, value) in overlay {
        match (base.get_mut(&key), value) {
            (Some(toml::Value::Table(base_table)), toml::Value::Table(overlay_table)) => {
                deep_merge_toml(base_table, overlay_table);
            }
            (_, value) => {
                base.insert(key, value);
            }
        }
    }
}

/// Expand `${VAR}` and `${VAR:-default}` references in a config string. Mirrors
/// `silo::settings::expand_env_vars` so the same Downward API patterns work.
fn expand_env_vars(input: &str) -> Cow<'_, str> {
    use std::env;

    if !input.contains("${") {
        return Cow::Borrowed(input);
    }

    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next();
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
                    }
                }
            } else {
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

mod duration_serde {
    use serde::{Deserialize, Deserializer};
    use std::time::Duration;

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let s = String::deserialize(d)?;
        humantime::parse_duration(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config() {
        let toml_str = r#"
            [storage]
            backend = "fs"
            path = "/tmp/silo/%shard%"

            [shard_discovery]
            backend = "etcd"
            cluster_prefix = "silo-dev"
            etcd_endpoints = ["http://127.0.0.1:2379"]
        "#;
        let cfg: AppConfig = toml::from_str(toml_str).expect("parse");
        assert_eq!(cfg.coordinator.max_shards_per_pod, 12);
        assert_eq!(cfg.coordinator.rebalance_interval, Duration::from_secs(30));
        assert_eq!(cfg.coordinator.mode, CoordinatorMode::K8sDeployment);
        assert!(cfg.compactor_options.is_none());
        assert_eq!(cfg.compaction_filter, CompactionFilterConfig::None);
    }

    #[test]
    fn parses_completed_jobs_filter_with_retention() {
        let toml_str = r#"
            [storage]
            backend = "memory"
            path = "memory://x/%shard%"

            [shard_discovery]
            backend = "etcd"
            cluster_prefix = "silo-dev"

            [compaction_filter]
            kind = "completed_jobs"
            retention_secs = 900
        "#;
        let cfg: AppConfig = toml::from_str(toml_str).expect("parse");
        assert_eq!(
            cfg.compaction_filter,
            CompactionFilterConfig::CompletedJobs {
                retention_secs: 900
            }
        );
    }

    #[test]
    fn completed_jobs_filter_defaults_to_seven_days() {
        let toml_str = r#"
            [storage]
            backend = "memory"
            path = "memory://x/%shard%"

            [shard_discovery]
            backend = "etcd"
            cluster_prefix = "silo-dev"

            [compaction_filter]
            kind = "completed_jobs"
        "#;
        let cfg: AppConfig = toml::from_str(toml_str).expect("parse");
        assert_eq!(
            cfg.compaction_filter,
            CompactionFilterConfig::CompletedJobs {
                retention_secs: DEFAULT_RETENTION_SECS,
            }
        );
    }

    #[test]
    fn parses_compactor_options_partial_override() {
        let toml_str = r#"
            [storage]
            backend = "memory"
            path = "memory://x/%shard%"

            [shard_discovery]
            backend = "etcd"
            cluster_prefix = "silo-dev"

            [compactor_options]
            poll_interval = "10s"
        "#;
        let cfg: AppConfig = toml::from_str(toml_str).expect("parse");
        let opts = cfg.compactor_options.expect("present");
        assert_eq!(opts.poll_interval, Duration::from_secs(10));
        assert_eq!(
            opts.max_concurrent_compactions,
            slatedb::config::CompactorOptions::default().max_concurrent_compactions
        );
    }

    #[test]
    fn env_var_expansion() {
        unsafe {
            std::env::set_var("SC_TEST_HOST", "127.0.0.1");
        }
        let s = "endpoint = \"http://${SC_TEST_HOST}:2379\"\n";
        let out = expand_env_vars(s);
        assert!(out.contains("127.0.0.1"));
    }

    #[test]
    fn env_var_default_fallback() {
        let s = "x = \"${SC_TEST_NEVER_SET:-fallback}\"\n";
        let out = expand_env_vars(s);
        assert!(out.contains("fallback"));
    }

    #[test]
    fn missing_required_field_errors() {
        let toml_str = r#"
            [storage]
            backend = "fs"
            # missing path

            [shard_discovery]
            backend = "etcd"
            cluster_prefix = "silo"
        "#;
        let r: Result<AppConfig, _> = toml::from_str(toml_str);
        assert!(r.is_err());
    }
}
