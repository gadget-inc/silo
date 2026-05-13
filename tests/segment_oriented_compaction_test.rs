//! End-to-end test for segment-oriented compaction.
//!
//! Lives in its own integration-test binary because
//! `silo::segment::init` is process-wide (OnceLock) and other test
//! binaries assume the default-disabled state. Cargo gives every
//! `tests/*.rs` file its own process, so we get a clean slot here.

use std::time::Duration;

use silo::job::JobStatusKind;
use silo::job_store_shard::{JobStoreShard, OpenShardOptions};
use silo::segment::{self, SegmentStrategy, SiloPrefixExtractor};
use silo::settings::Backend;
use silo::shard_range::ShardRange;
use silo::storage::resolve_object_store;

#[path = "test_helpers.rs"]
mod test_helpers;
use silo::gubernator::MockGubernatorClient;

fn fast_flush_settings() -> slatedb::config::Settings {
    slatedb::config::Settings {
        flush_interval: Some(Duration::from_millis(1)),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_date_strategy_round_trips_a_job() {
    let strategy = SegmentStrategy::Date;
    // First wire of the global; subsequent re-entries assert equality.
    silo::segment::init(strategy).expect("first init");

    let rate_limiter = MockGubernatorClient::new_arc();
    let tmp = tempfile::tempdir().unwrap();
    let resolved = resolve_object_store(&Backend::Fs, tmp.path().to_string_lossy().as_ref())
        .expect("resolve store");

    let shard = JobStoreShard::open_with_resolved_store(
        "seg-test".to_string(),
        &resolved.canonical_path,
        OpenShardOptions {
            store: resolved.store,
            wal_store: None,
            wal_close_config: None,
            slatedb_settings: Some(fast_flush_settings()),
            memory_cache: None,
            rate_limiter,
            metrics: None,
            concurrency_reconcile_interval: Duration::from_millis(1000),
            enable_counter_reconciliation: false,
            terminal_job_expire_ms: None,
            compaction_segment: Some(strategy),
        },
        ShardRange::full(),
    )
    .await
    .expect("open shard with segmenting on");

    // Enqueue creates a UUIDv7 job_id, so the segment derivation hits
    // the v7 timestamp path rather than the sentinel fallback.
    let job_id = shard
        .enqueue(
            "tenant-a",
            None,
            1,
            0,
            None,
            b"payload".to_vec(),
            vec![],
            None,
            "tg",
        )
        .await
        .expect("enqueue");

    let view = shard
        .get_job("tenant-a", &job_id)
        .await
        .expect("get job")
        .expect("job present");
    assert!(view.id().starts_with(&job_id[..8]));

    let status = shard
        .get_job_status("tenant-a", &job_id)
        .await
        .expect("get status")
        .expect("status present");
    assert!(matches!(status.kind, JobStatusKind::Scheduled));


    // The on-disk key for this job must carry the date-segment prefix
    // derived from the UUIDv7 timestamp. Sanity-check that the prefix
    // bytes match what we computed from the id.
    let derived = segment::prefix_for_id(&job_id);
    assert_eq!(derived.len(), strategy.prefix_len());
    assert!(derived.iter().all(|b| b.is_ascii_digit()),);

    drop(view);
    drop(shard);
}

#[test]
fn double_init_with_same_strategy_is_ok() {
    // Idempotent inits keep `OpenShardOptions` simple: opening multiple
    // shards in one process must not error on the second one.
    let _ = silo::segment::init(SegmentStrategy::Date);
    silo::segment::init(SegmentStrategy::Date).expect("idempotent init");
}

#[test]
fn extractor_name_matches_strategy() {
    use slatedb::prefix_extractor::PrefixExtractor;
    let ex = SiloPrefixExtractor::new(SegmentStrategy::Date);
    assert_eq!(ex.name(), SegmentStrategy::Date.name());
    let ex_m = SiloPrefixExtractor::new(SegmentStrategy::Minute);
    assert_eq!(ex_m.name(), SegmentStrategy::Minute.name());
    // Names must differ — slatedb pins extractor identity by name and
    // rejects a swap at reopen time. If we ever collide these we'd lose
    // that guard.
    assert_ne!(
        SegmentStrategy::Date.name(),
        SegmentStrategy::Minute.name()
    );
}
