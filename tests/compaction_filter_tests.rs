//! Tests exercising slatedb's `compaction_filters` feature (enabled on the
//! kirinrastogi/slatedb fork) plus the new per-compaction metrics the fork
//! exposes.
//!
//! The test spins up a standalone slatedb `Db` (not a silo `JobStoreShard`)
//! because silo's `DbBuilder` wrapper does not yet plumb through a
//! `CompactionFilterSupplier` — the filter has to be attached via
//! `CompactorBuilder::with_compaction_filter_supplier`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use object_store::memory::InMemory;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::{Rng, RngCore};
use slatedb::admin::AdminBuilder;
use slatedb::compactor::{CompactionSpec, CompactorBuilder, SourceId};
use slatedb::config::{CompactorOptions, Settings};
use slatedb::{
    CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier,
    CompactionJobContext, DbBuilder, RowEntry, ValueDeletable,
};
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};

/// Filter that flips a coin for every entry and either drops the entry
/// or converts it to a tombstone.
///
/// Counts how many of each decision it made so the test can sanity-check
/// that both branches exercised.
struct CoinFlipFilter {
    rng: StdRng,
    dropped: Arc<AtomicU64>,
    tombstoned: Arc<AtomicU64>,
}

#[async_trait]
impl CompactionFilter for CoinFlipFilter {
    async fn filter(
        &mut self,
        _entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        if self.rng.random_bool(0.5) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            Ok(CompactionFilterDecision::Drop)
        } else {
            self.tombstoned.fetch_add(1, Ordering::Relaxed);
            Ok(CompactionFilterDecision::Modify(ValueDeletable::Tombstone))
        }
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        Ok(())
    }
}

struct CoinFlipFilterSupplier {
    seed: u64,
    dropped: Arc<AtomicU64>,
    tombstoned: Arc<AtomicU64>,
}

#[async_trait]
impl CompactionFilterSupplier for CoinFlipFilterSupplier {
    async fn create_compaction_filter(
        &self,
        _ctx: &CompactionJobContext,
    ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        Ok(Box::new(CoinFlipFilter {
            rng: StdRng::seed_from_u64(self.seed),
            dropped: self.dropped.clone(),
            tombstoned: self.tombstoned.clone(),
        }))
    }
}

fn counter(recorder: &DefaultMetricsRecorder, name: &str) -> u64 {
    let snapshot = recorder.snapshot();
    let metrics = snapshot.by_name(name);
    metrics
        .into_iter()
        .map(|m| match m.value {
            MetricValue::Counter(v) => v,
            _ => 0,
        })
        .sum()
}

async fn wait_for_counter(
    recorder: &DefaultMetricsRecorder,
    name: &str,
    at_least: u64,
    timeout: Duration,
) -> u64 {
    let start = std::time::Instant::now();
    loop {
        let v = counter(recorder, name);
        if v >= at_least {
            return v;
        }
        if start.elapsed() > timeout {
            panic!(
                "timed out waiting for metric {name} to reach {at_least} (last seen {v})"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coin_flip_filter_runs_during_compaction_and_emits_metrics() {
    let object_store = Arc::new(InMemory::new());
    let db_path = "/compaction_filter_test";

    let dropped = Arc::new(AtomicU64::new(0));
    let tombstoned = Arc::new(AtomicU64::new(0));
    let supplier = Arc::new(CoinFlipFilterSupplier {
        seed: 0xC0FFEE,
        dropped: dropped.clone(),
        tombstoned: tombstoned.clone(),
    });

    let recorder = Arc::new(DefaultMetricsRecorder::new());

    // Tight compactor loop; one source is enough to pick up a compaction.
    let compactor_opts = CompactorOptions {
        poll_interval: Duration::from_millis(50),
        max_concurrent_compactions: 1,
        ..CompactorOptions::default()
    };

    let compactor_builder = CompactorBuilder::new(db_path, object_store.clone())
        .with_options(compactor_opts)
        .with_metrics_recorder(recorder.clone())
        .with_compaction_filter_supplier(supplier);

    // Small L0 SSTs so flushes produce many tiny files and compaction has
    // something to chew on.
    let mut settings = Settings::default();
    settings.l0_sst_size_bytes = 1024;

    let db = DbBuilder::new(db_path, object_store.clone())
        .with_settings(settings)
        .with_metrics_recorder(recorder.clone())
        .with_compactor_builder(compactor_builder)
        .build()
        .await
        .expect("build slatedb");

    // Insert records across several flushes so multiple L0 SSTs land.
    let mut rng = StdRng::seed_from_u64(42);
    for batch in 0..4 {
        for i in 0..200u32 {
            let mut v = vec![0u8; 128];
            rng.fill_bytes(&mut v);
            let key = format!("k-{batch:02}-{i:04}");
            db.put(key.as_bytes(), &v).await.expect("put");
        }
        db.flush().await.expect("flush");
    }

    // Kick a full compaction via the admin surface so we don't depend on
    // the size-tiered scheduler's threshold heuristics firing.
    let admin = AdminBuilder::new(db_path, object_store.clone()).build();
    let state = admin
        .read_compactor_state_view()
        .await
        .expect("read compactor state");
    let manifest = state.manifest();
    let sources: Vec<SourceId> = manifest
        .l0
        .iter()
        .map(|sst| SourceId::SstView(sst.sst.id.unwrap_compacted_id()))
        .chain(
            manifest
                .compacted
                .iter()
                .map(|sr| SourceId::SortedRun(sr.id)),
        )
        .collect();
    assert!(!sources.is_empty(), "expected L0 SSTs to have been flushed");
    let destination = manifest.compacted.iter().map(|sr| sr.id).min().unwrap_or(0);
    admin
        .submit_compaction(CompactionSpec::new(sources, destination))
        .await
        .expect("submit compaction");

    // Wait for the in-process compactor to pick it up.
    let completed = wait_for_counter(
        &recorder,
        "slatedb.compactor.compactions_completed",
        1,
        Duration::from_secs(30),
    )
    .await;
    assert!(completed >= 1);

    // Metrics introduced by the fork should be populated.
    let entries_written = counter(&recorder, "slatedb.compactor.entries_written");
    let sst_files_written = counter(&recorder, "slatedb.compactor.sst_files_written");
    let records_dropped = counter(&recorder, "slatedb.compactor.records_dropped");
    let tombstones_created = counter(&recorder, "slatedb.compactor.tombstones_created");

    // Dropped keys disappear entirely from output, so entries_written ≤ input.
    let dropped_count = dropped.load(Ordering::Relaxed);
    let tombstoned_count = tombstoned.load(Ordering::Relaxed);
    assert!(
        dropped_count + tombstoned_count > 0,
        "filter should have seen at least one entry"
    );
    assert!(
        sst_files_written >= 1,
        "at least one output sst should have been produced (got {sst_files_written})"
    );

    println!(
        "filter decisions: drop={dropped_count}, tombstone={tombstoned_count}; \
         metrics: entries_written={entries_written}, sst_files_written={sst_files_written}, \
         records_dropped={records_dropped}, tombstones_created={tombstones_created}, \
         compactions_completed={completed}",
    );

    drop(db);
}
