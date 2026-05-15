//! Tests for eager hydration of the concurrency holders cache at shard startup.
//!
//! `JobStoreShard::new` scans every concurrency holder record in durable storage
//! into `ConcurrencyCounts` so that the first `try_reserve` and grant scanner
//! tick see accurate capacity. Queues with zero holders are skipped — the lazy
//! `ensure_hydrated` path remains the fallback for queues first observed after
//! startup (see `omittedQueuesAreSafe` in specs/job_shard.als).

mod test_helpers;

use silo::codec::encode_holder;
use silo::gubernator::MockGubernatorClient;
use silo::keys::concurrency_holder_key;
use silo::settings::{Backend, DatabaseConfig};
use silo::shard_range::{ShardRange, hash_tenant};
use silo::task::HolderRecord;

use test_helpers::*;

fn fresh_holder() -> Vec<u8> {
    encode_holder(&HolderRecord {
        granted_at_ms: now_ms(),
    })
}

fn fs_config(path: &std::path::Path) -> DatabaseConfig {
    DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: path.to_string_lossy().to_string(),
        slatedb: Some(fast_flush_slatedb_settings()),
        // These tests exercise the eager startup-hydration path, so opt in
        // explicitly here regardless of the global default.
        hydrate_all_at_startup: true,
        ..Default::default()
    }
}

/// Eager hydration: holders that exist in durable storage are loaded into the
/// in-memory cache before `open` returns, across multiple (tenant, queue)
/// pairs.
#[silo::test]
async fn eager_hydration_loads_existing_holders_at_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = fs_config(tmp.path());
    let rate_limiter = MockGubernatorClient::new_arc();

    // Phase 1: open a shard and plant holders for two queues under one tenant
    // and a third queue under a different tenant.
    let shard1 = silo::job_store_shard::JobStoreShard::open(
        &cfg,
        rate_limiter.clone(),
        None,
        ShardRange::full(),
    )
    .await
    .expect("open shard1");

    let holder = fresh_holder();
    for (tenant, queue, task) in [
        ("tenant-a", "queue-1", "task-1"),
        ("tenant-a", "queue-1", "task-2"),
        ("tenant-a", "queue-2", "task-3"),
        ("tenant-b", "queue-1", "task-4"),
    ] {
        shard1
            .db()
            .put(&concurrency_holder_key(tenant, queue, task), &holder)
            .await
            .expect("plant holder");
    }
    shard1.db().flush().await.expect("flush");
    shard1.close().await.expect("close");

    // Phase 2: reopen — eager hydration must populate the cache from the
    // holders we just planted.
    let shard2 =
        silo::job_store_shard::JobStoreShard::open(&cfg, rate_limiter, None, ShardRange::full())
            .await
            .expect("open shard2");

    assert_eq!(shard2.concurrency_holder_count("tenant-a", "queue-1"), 2);
    assert_eq!(shard2.concurrency_holder_count("tenant-a", "queue-2"), 1);
    assert_eq!(shard2.concurrency_holder_count("tenant-b", "queue-1"), 1);
    // Queue never seen before stays at zero — and (per the omittedQueuesAreSafe
    // invariant) lazy hydration would still find zero if it ran.
    assert_eq!(shard2.concurrency_holder_count("tenant-a", "never-seen"), 0);

    shard2.close().await.expect("close");
}

/// A freshly opened shard with no holders has an empty cache — zero-holder
/// queues are skipped, including the case where there are no queues at all.
/// Uses `fs_config` (eager mode enabled) so this actually exercises the
/// `hydrate_all` path on an empty shard.
#[silo::test]
async fn eager_hydration_is_a_noop_when_no_holders_exist() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = fs_config(tmp.path());
    let rate_limiter = MockGubernatorClient::new_arc();

    let shard =
        silo::job_store_shard::JobStoreShard::open(&cfg, rate_limiter, None, ShardRange::full())
            .await
            .expect("open shard");

    assert_eq!(shard.concurrency_holder_count("any-tenant", "any-queue"), 0);

    shard.close().await.expect("close");
}

/// After a shard split, both child shards see the same holder records in
/// durable storage but each must only hydrate the tenants in its own range.
#[silo::test]
async fn eager_hydration_filters_by_shard_range() {
    // Find two tenant IDs whose hashes land in different halves of the hash
    // space so we can build a range that contains one and excludes the other.
    let (lo_tenant, hi_tenant) = {
        let mut lo: Option<String> = None;
        let mut hi: Option<String> = None;
        for i in 0..512u32 {
            let name = format!("tenant-{i}");
            let h = hash_tenant(&name);
            if h.as_str() < "8" && lo.is_none() {
                lo = Some(name);
            } else if h.as_str() >= "8" && hi.is_none() {
                hi = Some(name);
            }
            if lo.is_some() && hi.is_some() {
                break;
            }
        }
        (lo.expect("low-hash tenant"), hi.expect("high-hash tenant"))
    };

    let tmp = tempfile::tempdir().unwrap();
    let cfg = fs_config(tmp.path());
    let rate_limiter = MockGubernatorClient::new_arc();

    // Plant holders for both tenants under the full range.
    let shard1 = silo::job_store_shard::JobStoreShard::open(
        &cfg,
        rate_limiter.clone(),
        None,
        ShardRange::full(),
    )
    .await
    .expect("open shard1");

    let holder = fresh_holder();
    shard1
        .db()
        .put(&concurrency_holder_key(&lo_tenant, "q", "t-lo"), &holder)
        .await
        .expect("plant lo holder");
    shard1
        .db()
        .put(&concurrency_holder_key(&hi_tenant, "q", "t-hi"), &holder)
        .await
        .expect("plant hi holder");
    shard1.db().flush().await.expect("flush");
    shard1.close().await.expect("close shard1");

    // Reopen with a range that only contains the low-hash tenant. The high-hash
    // tenant's holder must be filtered out during startup hydration.
    let half_range = ShardRange::new("", "8");
    assert!(half_range.contains_tenant(&lo_tenant));
    assert!(!half_range.contains_tenant(&hi_tenant));

    let shard2 = silo::job_store_shard::JobStoreShard::open(&cfg, rate_limiter, None, half_range)
        .await
        .expect("open shard2");

    assert_eq!(shard2.concurrency_holder_count(&lo_tenant, "q"), 1);
    assert_eq!(
        shard2.concurrency_holder_count(&hi_tenant, "q"),
        0,
        "out-of-range holder must not appear in the cache"
    );

    shard2.close().await.expect("close shard2");
}
