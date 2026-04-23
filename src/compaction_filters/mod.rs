//! Compaction configuration glue: applies silo-level [`CompactionConfig`]
//! onto slatedb [`slatedb::config::Settings`] and builds the optional
//! [`CompactionFilterSupplier`] installed on the slatedb `DbBuilder`.
//!
//! This module is the seam the A/B harness drives — it is intentionally thin
//! so new filter variants (e.g. a future db-reading filter) or new
//! `CompactionSchedulerConfig` variants can be wired with a single match arm
//! each.

use std::sync::Arc;
use std::time::Duration;

use object_store::path::Path;
use slatedb::compactor::CompactionSchedulerSupplier;
use slatedb::{CompactionFilterSupplier, config::CompactorOptions};

use crate::settings::{
    CompactionConfig, CompactionFilterConfig, CompactionSchedulerConfig,
};

pub mod completed_jobs;
pub mod noop_counting;

/// Mutate `settings` in place to reflect the silo-level compaction config:
///
/// - If `compaction.disable` is true, set `compactor_options = None` so the
///   opened slatedb will not run its in-process compactor. External
///   compaction processes (e.g. `silo-compactor`) are expected to compact
///   the store in that configuration.
/// - Otherwise, ensure `compactor_options` is `Some` and overlay the
///   configured scheduler's tuning parameters onto
///   `CompactorOptions::scheduler_options`.
pub fn apply_compaction_config(
    settings: &mut slatedb::config::Settings,
    compaction: &CompactionConfig,
    _shard_name: &str,
) {
    if compaction.disable {
        settings.compactor_options = None;
        return;
    }

    let mut opts = settings
        .compactor_options
        .take()
        .unwrap_or_else(CompactorOptions::default);
    apply_scheduler_config(&mut opts, &compaction.scheduler);
    settings.compactor_options = Some(opts);
}

fn apply_scheduler_config(opts: &mut CompactorOptions, scheduler: &CompactionSchedulerConfig) {
    match scheduler {
        CompactionSchedulerConfig::SizeTiered {
            min_compaction_sources,
            max_compaction_sources,
            include_size_threshold,
        } => {
            opts.scheduler_options.insert(
                "min_compaction_sources".to_string(),
                min_compaction_sources.to_string(),
            );
            opts.scheduler_options.insert(
                "max_compaction_sources".to_string(),
                max_compaction_sources.to_string(),
            );
            opts.scheduler_options.insert(
                "include_size_threshold".to_string(),
                include_size_threshold.to_string(),
            );
        }
        CompactionSchedulerConfig::Leveled {
            level0_file_num_compaction_trigger,
            max_bytes_for_level_base,
            max_bytes_for_level_multiplier,
            num_levels,
        } => {
            opts.scheduler_options.insert(
                "level0_file_num_compaction_trigger".to_string(),
                level0_file_num_compaction_trigger.to_string(),
            );
            opts.scheduler_options.insert(
                "max_bytes_for_level_base".to_string(),
                max_bytes_for_level_base.to_string(),
            );
            opts.scheduler_options.insert(
                "max_bytes_for_level_multiplier".to_string(),
                max_bytes_for_level_multiplier.to_string(),
            );
            opts.scheduler_options.insert(
                "num_levels".to_string(),
                num_levels.to_string(),
            );
        }
    }
}

/// Build the optional scheduler supplier. Returns `Some` only for
/// scheduler variants that need a custom supplier (currently `Leveled`).
/// `SizeTiered` uses the default scheduler built into slatedb.
pub fn build_scheduler_supplier(
    compaction: &CompactionConfig,
) -> Option<Arc<dyn CompactionSchedulerSupplier>> {
    match &compaction.scheduler {
        CompactionSchedulerConfig::SizeTiered { .. } => None,
        CompactionSchedulerConfig::Leveled { .. } => Some(Arc::new(
            slatedb::compactor::LeveledCompactionSchedulerSupplier::new(),
        )),
    }
}

/// Object-store context needed by filter variants that read from the DB
/// (e.g. [`CompactionFilterConfig::CompletedJobs`]).
pub struct SupplierContext {
    pub db_path: Path,
    pub store: Arc<dyn object_store::ObjectStore>,
}

/// Build the optional compaction filter supplier from silo config.
///
/// `ctx` is required for variants that need to open a `DbReader` for point
/// reads (currently `CompletedJobs`). Pass `None` when you know the config
/// variant doesn't need it (tests, `None`/`NoopCounting`).
pub async fn build_supplier(
    compaction: &CompactionConfig,
    shard_name: &str,
    ctx: Option<SupplierContext>,
) -> anyhow::Result<Option<Arc<dyn CompactionFilterSupplier>>> {
    match &compaction.filter {
        CompactionFilterConfig::None => Ok(None),
        CompactionFilterConfig::NoopCounting => Ok(Some(Arc::new(
            noop_counting::NoopCountingSupplier::new(shard_name.to_string()),
        ))),
        CompactionFilterConfig::CompletedJobs { retention_secs } => {
            let ctx = ctx.ok_or_else(|| {
                anyhow::anyhow!(
                    "CompletedJobs filter requires a SupplierContext (db_path + object store)"
                )
            })?;
            let retention = Duration::from_secs(*retention_secs);
            Ok(Some(Arc::new(
                completed_jobs::CompletedJobCompactionFilterSupplier::new(
                    retention,
                    Some((ctx.db_path, ctx.store)),
                ),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disable_forces_none() {
        let mut settings = slatedb::config::Settings::default();
        assert!(settings.compactor_options.is_some());
        let cfg = CompactionConfig {
            disable: true,
            ..Default::default()
        };
        apply_compaction_config(&mut settings, &cfg, "shard");
        assert!(settings.compactor_options.is_none());
    }

    #[test]
    fn size_tiered_tuning_populates_scheduler_options() {
        let mut settings = slatedb::config::Settings::default();
        let cfg = CompactionConfig {
            scheduler: CompactionSchedulerConfig::SizeTiered {
                min_compaction_sources: 2,
                max_compaction_sources: 9,
                include_size_threshold: 3.5,
            },
            ..Default::default()
        };
        apply_compaction_config(&mut settings, &cfg, "shard");
        let opts = settings.compactor_options.expect("still some");
        assert_eq!(
            opts.scheduler_options.get("min_compaction_sources"),
            Some(&"2".to_string())
        );
        assert_eq!(
            opts.scheduler_options.get("max_compaction_sources"),
            Some(&"9".to_string())
        );
        assert_eq!(
            opts.scheduler_options.get("include_size_threshold"),
            Some(&"3.5".to_string())
        );
    }

    #[tokio::test]
    async fn build_supplier_none_by_default() {
        let cfg = CompactionConfig::default();
        assert!(build_supplier(&cfg, "shard", None).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn build_supplier_noop_counting() {
        let cfg = CompactionConfig {
            filter: CompactionFilterConfig::NoopCounting,
            ..Default::default()
        };
        assert!(build_supplier(&cfg, "shard", None)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn build_supplier_completed_jobs_requires_context() {
        let cfg = CompactionConfig {
            filter: CompactionFilterConfig::CompletedJobs {
                retention_secs: 3600,
            },
            ..Default::default()
        };
        assert!(build_supplier(&cfg, "shard", None).await.is_err());
    }
}
