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

    // Phase 2: reopen with eager hydration — `hydrate_all` must populate the
    // cache from the holders we just planted.
    let shard2 = open_shard_at_path_eager(tmp.path(), ShardRange::full()).await;

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

    let shard2 = open_shard_at_path_eager(tmp.path(), half_range).await;

    assert_eq!(shard2.concurrency_holder_count(&lo_tenant, "q"), 1);
    assert_eq!(
        shard2.concurrency_holder_count(&hi_tenant, "q"),
        0,
        "out-of-range holder must not appear in the cache"
    );

    shard2.close().await.expect("close shard2");
}

// ---------------------------------------------------------------------------
// Bounded-time startup hydration with deferred ticket granting
// ---------------------------------------------------------------------------

/// With `startup_hydration_timeout = None` (the default), `open` only returns
/// after hydration is fully complete: both gates are true before any caller
/// can interact with the shard.
#[silo::test]
async fn default_open_returns_with_both_gates_flipped() {
    let (_tmp, shard) = open_temp_shard().await;
    assert!(
        shard.is_accepting_enqueues(),
        "default behavior: enqueues open as soon as open() returns"
    );
    assert!(
        shard.is_grants_enabled(),
        "default behavior: grants enabled as soon as open() returns"
    );
}

/// With a small `startup_hydration_timeout`, `open` returns immediately and
/// both gates begin closed. The timer flips `accepting_enqueues` first; once
/// hydration completes (essentially instantly on a small DB), `grants_enabled`
/// also flips.
#[silo::test]
async fn timed_startup_flips_gates_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    // Use a long-enough timer that hydration can race past it on a small DB,
    // but short enough that the test doesn't dawdle.
    let shard = open_temp_shard_at_path_with_hydration_timeout(
        tmp.path(),
        std::time::Duration::from_millis(200),
    )
    .await;

    // On a fresh empty DB, hydration is essentially instant. Both gates may
    // flip within microseconds. Poll briefly for the terminal state.
    for _ in 0..50 {
        if shard.is_accepting_enqueues() && shard.is_grants_enabled() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        shard.is_accepting_enqueues(),
        "timed startup should flip accepting_enqueues"
    );
    assert!(
        shard.is_grants_enabled(),
        "timed startup should flip grants_enabled after hydration"
    );

    shard.close().await.expect("close");
}

/// Jobs enqueued while the shard is in the "accepting enqueues but grants
/// disabled" window must be drained by the grant scanner once hydration
/// finishes. We engineer that window by planting enough holders on an
/// unrelated queue to slow startup hydration past a tiny timer, then enqueue
/// concurrency-limited jobs on a separate queue, and finally wait for the
/// backlog to become holders.
#[silo::test]
async fn backlog_enqueued_during_hydration_is_drained_after() {
    use silo::job::{ConcurrencyLimit, Limit};

    let tmp = tempfile::tempdir().unwrap();
    let cfg = fs_config(tmp.path());
    let rate_limiter = MockGubernatorClient::new_arc();

    // Phase 1: plant a large holder set on `warmup-q` so that the eager scan
    // in phase 2 takes meaningful wall time. This is what makes the
    // "accepting=true, grants=false" window observable. Use batched writes
    // so the setup itself is fast.
    let shard1 = silo::job_store_shard::JobStoreShard::open(
        &cfg,
        rate_limiter.clone(),
        None,
        ShardRange::full(),
    )
    .await
    .expect("open shard1");
    let holder = fresh_holder();
    let mut batch = slatedb::WriteBatch::new();
    // 50 000 holders puts the hydration scan well into the hundreds of
    // milliseconds even on a fast SSD, comfortably above the 2 ms timer.
    for i in 0..50_000u32 {
        let task = format!("warmup-{i:05}");
        batch.put(
            &concurrency_holder_key("warmup-tenant", "warmup-q", &task),
            &holder,
        );
    }
    shard1.db().write(batch).await.expect("plant warmup batch");
    shard1.db().flush().await.expect("flush");
    shard1.close().await.expect("close shard1");

    // The test target: separate queue/tenant so the backlog is independent
    // of the warmup-tenant holders we planted to slow hydration.
    let test_tenant = "-";
    let test_queue = "drain-test-q";

    // Phase 2: reopen with a short startup timer. We retry the race a few
    // times — if hydration wins on a particular attempt we fall through and
    // try again.
    let mut succeeded = false;
    let mut last_failure: Option<String> = None;
    for _attempt in 0..5 {
        let shard = open_temp_shard_at_path_with_hydration_timeout(
            tmp.path(),
            std::time::Duration::from_millis(2),
        )
        .await;

        // Wait briefly for the gate window. Up to ~100ms is plenty when the
        // hydration scan has 5k holder reads ahead of it.
        let mut window_observed = false;
        let window_deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
        while std::time::Instant::now() < window_deadline {
            if shard.is_accepting_enqueues() && !shard.is_grants_enabled() {
                window_observed = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        if !window_observed {
            last_failure = Some("never observed the open window".into());
            shard.close().await.ok();
            continue;
        }

        // Land three concurrency-limited enqueues inside the window. With
        // `max_concurrency: 3` all of them eventually become holders.
        let mut job_ids = Vec::with_capacity(3);
        let mut enqueue_failed = false;
        for i in 0..3 {
            match shard
                .enqueue(
                    test_tenant,
                    None,
                    10u8,
                    now_ms(),
                    None,
                    test_helpers::msgpack_payload(&serde_json::json!({"backlog": i})),
                    vec![Limit::Concurrency(ConcurrencyLimit {
                        key: test_queue.to_string(),
                        max_concurrency: 3,
                    })],
                    None,
                    "default",
                )
                .await
            {
                Ok(id) => job_ids.push(id),
                Err(e) => {
                    last_failure = Some(format!("enqueue failed mid-window: {e}"));
                    enqueue_failed = true;
                    break;
                }
            }
        }
        if enqueue_failed {
            shard.close().await.ok();
            continue;
        }

        // While grants are still disabled, no holder may exist for our test
        // queue — every enqueue must have fallen through to the
        // request-queued branch.
        assert_eq!(
            shard.concurrency_holder_count(test_tenant, test_queue),
            0,
            "no holder for test queue while grants_enabled is false"
        );
        // Pending TicketRequests for the test tenant exist on durable
        // storage: there should be at least 3, none of which are ours' yet.
        let requests_during = count_concurrency_requests(shard.db()).await;
        assert!(
            requests_during >= 3,
            "expected at least 3 pending requests during the window, got {requests_during}"
        );

        // Wait for hydration to complete.
        let hydration_deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while std::time::Instant::now() < hydration_deadline {
            if shard.is_grants_enabled() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            shard.is_grants_enabled(),
            "startup hydration must finish within 30s"
        );

        // After hydration the grant scanner is woken; the three pending
        // requests on test_queue should become holders. Poll until all
        // three are granted.
        let drain_deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut drained = 0;
        while std::time::Instant::now() < drain_deadline {
            drained = shard.concurrency_holder_count(test_tenant, test_queue);
            if drained == 3 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(
            drained, 3,
            "grant scanner must drain all three backlogged requests into holders"
        );

        // Durable state: TicketRequest rows for these jobs must have been
        // consumed by the grant. (The 5000 warmup holders are still in
        // durable storage; they're for a different tenant/queue.)
        let test_tenant_holders =
            count_concurrency_holders_for_tenant(shard.db(), test_tenant).await;
        assert_eq!(
            test_tenant_holders, 3,
            "three durable TicketHolder rows must exist for the test tenant after drain"
        );

        shard.close().await.expect("close");
        succeeded = true;
        break;
    }

    assert!(
        succeeded,
        "failed to land a window-and-drain run across 5 attempts; last failure: {last_failure:?}"
    );
}

/// Before any gate flips, enqueue returns `ShardHydrating`. To exercise this
/// deterministically we plant enough holders that the eager scan can't beat
/// the synchronous code that issues the enqueue right after `open` returns.
#[silo::test]
async fn enqueue_before_gates_flip_returns_shard_hydrating() {
    use silo::job_store_shard::JobStoreShardError;

    let tmp = tempfile::tempdir().unwrap();
    let cfg = fs_config(tmp.path());
    let rate_limiter = MockGubernatorClient::new_arc();

    // Phase 1: plant a large set of holder records so that the eager scan in
    // phase 2 has enough work to lose the race against an immediate enqueue.
    let shard1 = silo::job_store_shard::JobStoreShard::open(
        &cfg,
        rate_limiter.clone(),
        None,
        ShardRange::full(),
    )
    .await
    .expect("open shard1");
    let holder = fresh_holder();
    let mut batch = slatedb::WriteBatch::new();
    for i in 0..2000u32 {
        let task = format!("task-{i:05}");
        batch.put(&concurrency_holder_key("tenant", "queue", &task), &holder);
    }
    shard1.db().write(batch).await.expect("plant batch");
    shard1.db().flush().await.expect("flush");
    shard1.close().await.expect("close shard1");

    // Phase 2: reopen with a long timer so the hydration task is the only
    // path that can flip the gates. We then enqueue immediately; if any
    // gate has already flipped, retry the race up to a few times.
    let mut observed_hydrating = false;
    for _attempt in 0..5 {
        let shard = open_temp_shard_at_path_with_hydration_timeout(
            tmp.path(),
            std::time::Duration::from_secs(60),
        )
        .await;

        if shard.is_accepting_enqueues() {
            // Lost the race on this attempt — hydration finished before we
            // could observe the closed gate. Try again.
            shard.close().await.expect("close");
            continue;
        }

        let err = shard
            .enqueue(
                "-",
                None,
                10u8,
                now_ms(),
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"hydrating": true})),
                vec![],
                None,
                "default",
            )
            .await
            .expect_err("enqueue should reject while gates are closed");
        assert!(
            matches!(err, JobStoreShardError::ShardHydrating),
            "expected ShardHydrating, got {err:?}"
        );
        observed_hydrating = true;
        shard.close().await.expect("close");
        break;
    }
    assert!(
        observed_hydrating,
        "expected to observe the closed gate at least once across 5 attempts; the hydration task is winning the race too consistently"
    );
}
