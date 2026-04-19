//! Compaction configuration glue: applies silo-level [`CompactionConfig`]
//! onto slatedb [`slatedb::config::Settings`] and builds the optional
//! [`CompactionFilterSupplier`] installed on the slatedb `DbBuilder`.
//!
//! This module is the seam the A/B harness drives — it is intentionally thin
//! so new filter variants (e.g. a future db-reading filter) or new
//! `CompactionSchedulerConfig` variants can be wired with a single match arm
//! each.

use std::sync::Arc;

use slatedb::{CompactionFilterSupplier, config::CompactorOptions};

use crate::settings::{
    CompactionConfig, CompactionFilterConfig, CompactionSchedulerConfig,
};

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
            // slatedb's SizeTieredCompactionScheduler reads these keys from
            // `CompactorOptions.scheduler_options` — see the
            // `From<&HashMap<String,String>> for SizeTieredCompactionSchedulerOptions`
            // impl in slatedb/src/config.rs. Using the string map keeps us on
            // the public extension point and leaves room for future schedulers
            // without pulling a scheduler-specific type into this API.
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
    }
}

/// Build the optional compaction filter supplier from silo config. Returns
/// `None` for [`CompactionFilterConfig::None`] (the default) so the DbBuilder
/// is not touched.
pub fn build_supplier(
    compaction: &CompactionConfig,
    shard_name: &str,
) -> Option<Arc<dyn CompactionFilterSupplier>> {
    match compaction.filter {
        CompactionFilterConfig::None => None,
        CompactionFilterConfig::NoopCounting => Some(Arc::new(
            noop_counting::NoopCountingSupplier::new(shard_name.to_string()),
        )),
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

    #[test]
    fn build_supplier_none_by_default() {
        let cfg = CompactionConfig::default();
        assert!(build_supplier(&cfg, "shard").is_none());
    }

    #[test]
    fn build_supplier_noop_counting() {
        let cfg = CompactionConfig {
            filter: CompactionFilterConfig::NoopCounting,
            ..Default::default()
        };
        assert!(build_supplier(&cfg, "shard").is_some());
    }
}
