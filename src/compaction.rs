//! Custom compaction scheduler for Silo.
//!
//! Extends SlateDB's size-tiered compaction (STC) with a **space amplification trigger**:
//! when the ratio of non-bottom-run data to the bottom run exceeds a threshold, a full
//! compaction is forced. This is critical for Silo's high-churn workload where task keys
//! are created and deleted within minutes, leaving tombstones that the default STC doesn't
//! clean up fast enough.
//!
//! Inspired by RocksDB's Universal Compaction space amplification trigger.

use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};

use slatedb::compactor::{
    CompactionRequest, CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec,
    CompactorStateView, SourceId,
};
use slatedb::config::CompactorOptions;
use tracing::debug;

/// Default configuration for the Silo compaction scheduler.
const DEFAULT_MIN_COMPACTION_SOURCES: usize = 2;
const DEFAULT_MAX_COMPACTION_SOURCES: usize = 16;
const DEFAULT_INCLUDE_SIZE_THRESHOLD: f32 = 4.0;
const DEFAULT_MAX_SPACE_AMPLIFICATION_PERCENT: u64 = 25;

/// Extract the sorted run ID from a SourceId, or None if it's an SST.
fn source_sr_id(s: &SourceId) -> Option<u32> {
    match s {
        SourceId::SortedRun(id) => Some(*id),
        SourceId::Sst(_) => None,
    }
}

/// Configurable options for the Silo compaction scheduler.
#[derive(Clone, Copy, Debug)]
pub struct SiloCompactionSchedulerOptions {
    pub min_compaction_sources: usize,
    pub max_compaction_sources: usize,
    pub include_size_threshold: f32,
    pub max_space_amplification_percent: u64,
}

impl Default for SiloCompactionSchedulerOptions {
    fn default() -> Self {
        Self {
            min_compaction_sources: DEFAULT_MIN_COMPACTION_SOURCES,
            max_compaction_sources: DEFAULT_MAX_COMPACTION_SOURCES,
            include_size_threshold: DEFAULT_INCLUDE_SIZE_THRESHOLD,
            max_space_amplification_percent: DEFAULT_MAX_SPACE_AMPLIFICATION_PERCENT,
        }
    }
}

impl From<&HashMap<String, String>> for SiloCompactionSchedulerOptions {
    fn from(opts: &HashMap<String, String>) -> Self {
        let mut result = Self::default();
        if let Some(v) = opts.get("min_compaction_sources")
            && let Ok(n) = v.parse()
        {
            result.min_compaction_sources = n;
        }
        if let Some(v) = opts.get("max_compaction_sources")
            && let Ok(n) = v.parse()
        {
            result.max_compaction_sources = n;
        }
        if let Some(v) = opts.get("include_size_threshold")
            && let Ok(n) = v.parse()
        {
            result.include_size_threshold = n;
        }
        if let Some(v) = opts.get("max_space_amplification_percent")
            && let Ok(n) = v.parse()
        {
            result.max_space_amplification_percent = n;
        }
        result
    }
}

/// Compute space amplification as a percentage from sorted run sizes.
///
/// Takes L0 sizes and sorted run sizes (in order from highest ID to lowest ID).
/// Space amp = `(non_bottom_size / bottom_size) * 100`.
/// Returns 0.0 if there are no sorted runs or the bottom run is empty.
pub fn compute_space_amplification_percent_from_sizes(
    l0_total_size: u64,
    sorted_run_sizes: &[u64],
) -> f64 {
    if sorted_run_sizes.is_empty() {
        return 0.0;
    }

    // Bottom run is the last one (lowest ID)
    let bottom_size = *sorted_run_sizes.last().unwrap_or(&0);
    if bottom_size == 0 {
        return 0.0;
    }

    // Non-bottom = all L0 SSTs + all sorted runs except the bottom
    let non_bottom_sr_size: u64 = if sorted_run_sizes.len() > 1 {
        sorted_run_sizes[..sorted_run_sizes.len() - 1].iter().sum()
    } else {
        0
    };

    ((l0_total_size + non_bottom_sr_size) as f64 / bottom_size as f64) * 100.0
}

// ---------- Internal helpers (reimplemented from STC since they are pub(crate)) ----------

#[derive(Clone, Copy, Debug)]
struct CompactionSource {
    source: SourceId,
    size: u64,
}

impl CompactionSource {
    fn sr_id(&self) -> u32 {
        source_sr_id(&self.source).expect("expected SortedRun source")
    }
}

/// Checks that a candidate compaction does not conflict with active compactions.
struct ConflictChecker {
    sources_used: HashSet<SourceId>,
}

impl ConflictChecker {
    fn new<'a>(compactions: impl Iterator<Item = &'a CompactionSpec>) -> Self {
        let mut checker = Self {
            sources_used: HashSet::new(),
        };
        for compaction in compactions {
            checker.add_compaction(compaction);
        }
        checker
    }

    fn check_compaction(&self, sources: &VecDeque<CompactionSource>, dst: u32) -> bool {
        for source in sources.iter() {
            if self.sources_used.contains(&source.source) {
                return false;
            }
        }
        let dst = SourceId::SortedRun(dst);
        if self.sources_used.contains(&dst) {
            return false;
        }
        true
    }

    fn check_sources(&self, sources: &[SourceId], dst: u32) -> bool {
        for source in sources.iter() {
            if self.sources_used.contains(source) {
                return false;
            }
        }
        let dst = SourceId::SortedRun(dst);
        if self.sources_used.contains(&dst) {
            return false;
        }
        true
    }

    fn add_compaction(&mut self, compaction: &CompactionSpec) {
        for source in compaction.sources().iter() {
            self.sources_used.insert(*source);
        }
        self.sources_used
            .insert(SourceId::SortedRun(compaction.destination()));
    }
}

/// Avoids creating sorted runs that would immediately need further compaction.
struct BackpressureChecker {
    include_size_threshold: f32,
    max_compaction_sources: usize,
    longest_compactable_runs_by_sr: HashMap<u32, VecDeque<CompactionSource>>,
}

impl BackpressureChecker {
    fn new(
        include_size_threshold: f32,
        max_compaction_sources: usize,
        srs: &[CompactionSource],
    ) -> Self {
        let mut longest_compactable_runs_by_sr = HashMap::new();
        for (i, sr) in srs.iter().enumerate() {
            let sr_id = sr.sr_id();
            let compactable_run =
                SiloCompactionScheduler::build_compactable_run(include_size_threshold, srs, i);
            longest_compactable_runs_by_sr.insert(sr_id, compactable_run);
        }
        Self {
            include_size_threshold,
            max_compaction_sources,
            longest_compactable_runs_by_sr,
        }
    }

    fn check_compaction(
        &self,
        sources: &VecDeque<CompactionSource>,
        next_sr: Option<&CompactionSource>,
    ) -> bool {
        let estimated_result_size: u64 = sources.iter().map(|src| src.size).sum();
        if let Some(next_sr) = next_sr
            && next_sr.size <= ((estimated_result_size as f32) * self.include_size_threshold) as u64
            && self
                .longest_compactable_runs_by_sr
                .get(&next_sr.sr_id())
                .map(|r| r.len())
                .unwrap_or(0)
                >= self.max_compaction_sources
        {
            return false;
        }
        true
    }
}

struct CompactionChecker {
    conflict_checker: ConflictChecker,
    backpressure_checker: BackpressureChecker,
}

impl CompactionChecker {
    fn new(conflict_checker: ConflictChecker, backpressure_checker: BackpressureChecker) -> Self {
        Self {
            conflict_checker,
            backpressure_checker,
        }
    }

    fn check_compaction(
        &self,
        sources: &VecDeque<CompactionSource>,
        dst: u32,
        next_sr: Option<&CompactionSource>,
    ) -> bool {
        if !self.conflict_checker.check_compaction(sources, dst) {
            return false;
        }
        if !self.backpressure_checker.check_compaction(sources, next_sr) {
            return false;
        }
        true
    }
}

// ---------- The scheduler ----------

/// Silo's custom compaction scheduler.
///
/// Priority order in `propose()`:
/// 1. **Space amplification check**: if non-bottom / bottom > threshold, force full compaction.
/// 2. **L0 compaction**: if L0 count >= `min_compaction_sources`, compact to a new SR.
/// 3. **SR compaction**: walk sorted runs for similarly-sized consecutive groups.
struct SiloCompactionScheduler {
    options: SiloCompactionSchedulerOptions,
    max_concurrent_compactions: usize,
}

impl SiloCompactionScheduler {
    fn new(options: SiloCompactionSchedulerOptions, max_concurrent_compactions: usize) -> Self {
        Self {
            options,
            max_concurrent_compactions,
        }
    }

    fn compaction_sources(
        &self,
        state: &CompactorStateView,
    ) -> (Vec<CompactionSource>, Vec<CompactionSource>) {
        let manifest = state.manifest();
        (
            manifest
                .l0
                .iter()
                .map(|l0| CompactionSource {
                    source: SourceId::Sst(l0.id.unwrap_compacted_id()),
                    size: l0.info.index_offset + l0.info.index_len,
                })
                .collect(),
            manifest
                .compacted
                .iter()
                .map(|sr| CompactionSource {
                    source: SourceId::SortedRun(sr.id),
                    size: sr.estimate_size(),
                })
                .collect(),
        )
    }

    /// Compute space amplification from the compactor state view.
    fn space_amplification_percent(&self, state: &CompactorStateView) -> f64 {
        let manifest = state.manifest();
        let l0_size: u64 = manifest
            .l0
            .iter()
            .map(|sst| sst.info.index_offset + sst.info.index_len)
            .sum();
        let sr_sizes: Vec<u64> = manifest
            .compacted
            .iter()
            .map(|sr| sr.estimate_size())
            .collect();
        compute_space_amplification_percent_from_sizes(l0_size, &sr_sizes)
    }

    /// Try to propose a full compaction if space amplification exceeds the threshold.
    fn propose_space_amp_compaction(
        &self,
        state: &CompactorStateView,
        conflict_checker: &ConflictChecker,
    ) -> Option<CompactionSpec> {
        let space_amp = self.space_amplification_percent(state);
        if space_amp <= self.options.max_space_amplification_percent as f64 {
            return None;
        }

        debug!(
            space_amp_percent = space_amp,
            threshold = self.options.max_space_amplification_percent,
            "space amplification exceeds threshold, proposing full compaction"
        );

        let manifest = state.manifest();

        // Build a full compaction: all L0s + all SRs -> lowest SR
        let sources: Vec<SourceId> = manifest
            .l0
            .iter()
            .map(|sst| SourceId::Sst(sst.id.unwrap_compacted_id()))
            .chain(
                manifest
                    .compacted
                    .iter()
                    .map(|sr| SourceId::SortedRun(sr.id)),
            )
            .collect();

        if sources.is_empty() {
            return None;
        }

        let destination = manifest.compacted.iter().map(|sr| sr.id).min().unwrap_or(0);

        // Check for conflicts with active compactions
        if !conflict_checker.check_sources(&sources, destination) {
            return None;
        }

        Some(CompactionSpec::new(sources, destination))
    }

    fn pick_next_compaction(
        &self,
        l0: &[CompactionSource],
        srs: &[CompactionSource],
        checker: &CompactionChecker,
    ) -> Option<CompactionSpec> {
        // Compact L0s if we have enough
        let l0_candidates: VecDeque<_> = l0.iter().copied().collect();
        if let Some(mut l0_candidates) = self.clamp_min(l0_candidates) {
            l0_candidates = self.clamp_max(l0_candidates);
            let dst = srs.first().map_or(0, |sr| sr.sr_id() + 1);
            let next_sr = srs.first();
            if checker.check_compaction(&l0_candidates, dst, next_sr) {
                return Some(self.create_compaction(l0_candidates, dst));
            }
        }

        // Try to compact sorted runs
        for i in 0..srs.len() {
            let compactable_run = Self::build_compactable_run_checked(
                self.options.include_size_threshold,
                srs,
                i,
                checker,
            );
            let compactable_run = self.clamp_min(compactable_run);
            if let Some(mut compactable_run) = compactable_run {
                compactable_run = self.clamp_max(compactable_run);
                let dst = compactable_run
                    .back()
                    .expect("expected non-empty compactable run")
                    .sr_id();
                return Some(self.create_compaction(compactable_run, dst));
            }
        }
        None
    }

    fn clamp_min(&self, sources: VecDeque<CompactionSource>) -> Option<VecDeque<CompactionSource>> {
        if sources.len() < self.options.min_compaction_sources {
            return None;
        }
        Some(sources)
    }

    fn clamp_max(&self, mut sources: VecDeque<CompactionSource>) -> VecDeque<CompactionSource> {
        while sources.len() > self.options.max_compaction_sources {
            sources.pop_front();
        }
        sources
    }

    fn create_compaction(&self, sources: VecDeque<CompactionSource>, dst: u32) -> CompactionSpec {
        let sources: Vec<SourceId> = sources.iter().map(|src| src.source).collect();
        CompactionSpec::new(sources, dst)
    }

    /// Build a compactable run of similarly-sized sorted runs starting at `start_idx`.
    /// This variant does NOT check against a CompactionChecker (used by BackpressureChecker).
    fn build_compactable_run(
        size_threshold: f32,
        sources: &[CompactionSource],
        start_idx: usize,
    ) -> VecDeque<CompactionSource> {
        let mut compactable_runs = VecDeque::new();
        let mut maybe_min_sz = None;
        for source in sources.iter().skip(start_idx) {
            if let Some(min_sz) = maybe_min_sz {
                if source.size > ((min_sz as f32) * size_threshold) as u64 {
                    break;
                }
                maybe_min_sz = Some(min(min_sz, source.size));
            } else {
                maybe_min_sz = Some(source.size);
            }
            compactable_runs.push_back(*source);
        }
        compactable_runs
    }

    /// Build a compactable run with checker validation.
    fn build_compactable_run_checked(
        size_threshold: f32,
        sources: &[CompactionSource],
        start_idx: usize,
        checker: &CompactionChecker,
    ) -> VecDeque<CompactionSource> {
        let mut compactable_runs = VecDeque::new();
        let mut maybe_min_sz = None;
        for (i, source) in sources.iter().enumerate().skip(start_idx) {
            if let Some(min_sz) = maybe_min_sz {
                if source.size > ((min_sz as f32) * size_threshold) as u64 {
                    break;
                }
                maybe_min_sz = Some(min(min_sz, source.size));
            } else {
                maybe_min_sz = Some(source.size);
            }
            compactable_runs.push_back(*source);
            let dst = source.sr_id();
            let next_sr = sources.get(i + 1);
            if !checker.check_compaction(&compactable_runs, dst, next_sr) {
                compactable_runs.pop_back();
                break;
            }
        }
        compactable_runs
    }
}

impl CompactionScheduler for SiloCompactionScheduler {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        let mut compactions = Vec::new();
        let (l0, srs) = self.compaction_sources(state);
        let active_compactions = state
            .compactions()
            .into_iter()
            .flat_map(|c| c.recent_compactions())
            .filter(|c| c.active())
            .collect::<Vec<_>>();
        let conflict_checker = ConflictChecker::new(active_compactions.iter().map(|j| j.spec()));

        // Priority 1: Space amplification trigger
        if let Some(full_compaction) = self.propose_space_amp_compaction(state, &conflict_checker) {
            return vec![full_compaction];
        }

        // Priority 2 & 3: Normal L0 and SR compactions
        let backpressure_checker = BackpressureChecker::new(
            self.options.include_size_threshold,
            self.options.max_compaction_sources,
            &srs,
        );
        let mut checker = CompactionChecker::new(conflict_checker, backpressure_checker);

        while active_compactions.len() + compactions.len() < self.max_concurrent_compactions {
            let Some(compaction) = self.pick_next_compaction(&l0, &srs, &checker) else {
                break;
            };
            checker.conflict_checker.add_compaction(&compaction);
            compactions.push(compaction);
        }

        compactions
    }

    fn validate(
        &self,
        state: &CompactorStateView,
        compaction: &CompactionSpec,
    ) -> Result<(), slatedb::Error> {
        // Logical order: [L0 (newest → oldest), then SRs (highest id → 0)]
        let sources_logical_order: Vec<SourceId> = state
            .manifest()
            .l0
            .iter()
            .map(|sst| SourceId::Sst(sst.id.unwrap_compacted_id()))
            .chain(
                state
                    .manifest()
                    .compacted
                    .iter()
                    .map(|sr| SourceId::SortedRun(sr.id)),
            )
            .collect();

        // Sources must be consecutive in the logical order
        if !sources_logical_order
            .windows(compaction.sources().len())
            .any(|w| w == compaction.sources().as_slice())
        {
            return Err(slatedb::Error::invalid(
                "non-consecutive compaction sources".to_string(),
            ));
        }

        let has_sr = compaction
            .sources()
            .iter()
            .any(|s| matches!(s, SourceId::SortedRun(_)));

        if has_sr {
            // Must merge into the lowest-id SR among sources
            let min_sr = compaction
                .sources()
                .iter()
                .filter_map(source_sr_id)
                .min()
                .expect("at least one SR in sources");
            if compaction.destination() != min_sr {
                return Err(slatedb::Error::invalid(
                    "destination not the lowest-id SR among sources".to_string(),
                ));
            }
        }

        Ok(())
    }

    fn generate(
        &self,
        state: &CompactorStateView,
        request: &CompactionRequest,
    ) -> Result<Vec<CompactionSpec>, slatedb::Error> {
        match request {
            CompactionRequest::Spec(spec) => Ok(vec![spec.clone()]),
            CompactionRequest::Full => {
                let manifest = state.manifest();
                let sources = manifest
                    .l0
                    .iter()
                    .map(|sst| SourceId::Sst(sst.id.unwrap_compacted_id()))
                    .chain(
                        manifest
                            .compacted
                            .iter()
                            .map(|sr| SourceId::SortedRun(sr.id)),
                    )
                    .collect::<Vec<_>>();
                if sources.is_empty() {
                    return Err(slatedb::Error::invalid(
                        "cannot compact empty database".to_string(),
                    ));
                }
                let destination = manifest.compacted.iter().map(|sr| sr.id).min().unwrap_or(0);
                Ok(vec![CompactionSpec::new(sources, destination)])
            }
        }
    }
}

/// Supplier that creates `SiloCompactionScheduler` instances.
pub struct SiloCompactionSchedulerSupplier;

impl CompactionSchedulerSupplier for SiloCompactionSchedulerSupplier {
    fn compaction_scheduler(
        &self,
        compactor_options: &CompactorOptions,
    ) -> Box<dyn CompactionScheduler + Send + Sync> {
        let scheduler_options =
            SiloCompactionSchedulerOptions::from(&compactor_options.scheduler_options);
        Box::new(SiloCompactionScheduler::new(
            scheduler_options,
            compactor_options.max_concurrent_compactions,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_options_from_empty_hashmap() {
        let opts = SiloCompactionSchedulerOptions::from(&HashMap::new());
        assert_eq!(opts.min_compaction_sources, DEFAULT_MIN_COMPACTION_SOURCES);
        assert_eq!(opts.max_compaction_sources, DEFAULT_MAX_COMPACTION_SOURCES);
        assert_eq!(
            opts.max_space_amplification_percent,
            DEFAULT_MAX_SPACE_AMPLIFICATION_PERCENT
        );
    }

    #[test]
    fn test_options_from_hashmap() {
        let mut map = HashMap::new();
        map.insert("min_compaction_sources".to_string(), "3".to_string());
        map.insert(
            "max_space_amplification_percent".to_string(),
            "50".to_string(),
        );
        let opts = SiloCompactionSchedulerOptions::from(&map);
        assert_eq!(opts.min_compaction_sources, 3);
        assert_eq!(opts.max_space_amplification_percent, 50);
        assert_eq!(opts.max_compaction_sources, DEFAULT_MAX_COMPACTION_SOURCES);
    }

    #[test]
    fn test_options_invalid_values_use_defaults() {
        let mut map = HashMap::new();
        map.insert(
            "min_compaction_sources".to_string(),
            "not_a_number".to_string(),
        );
        let opts = SiloCompactionSchedulerOptions::from(&map);
        assert_eq!(opts.min_compaction_sources, DEFAULT_MIN_COMPACTION_SOURCES);
    }

    #[test]
    fn test_space_amp_no_sorted_runs() {
        let result = compute_space_amplification_percent_from_sizes(1000, &[]);
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_space_amp_single_sorted_run() {
        // 1000 bytes L0, 10000 bytes bottom run = 10% amp
        let result = compute_space_amplification_percent_from_sizes(1000, &[10000]);
        assert!((result - 10.0).abs() < 0.01);
    }

    #[test]
    fn test_space_amp_multiple_sorted_runs() {
        // L0=500, SR0=5000, SR1(bottom)=10000
        // non-bottom = 500 + 5000 = 5500, bottom = 10000
        // amp = 55%
        let result = compute_space_amplification_percent_from_sizes(500, &[5000, 10000]);
        assert!((result - 55.0).abs() < 0.01);
    }

    #[test]
    fn test_space_amp_zero_bottom() {
        let result = compute_space_amplification_percent_from_sizes(1000, &[0]);
        assert_eq!(result, 0.0);
    }
}
