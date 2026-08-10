//! Tests for ShardFactory: close, reset, clone, and template path validation.

mod test_helpers;

use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::settings::{Backend, DatabaseTemplate, WalConfig};
use silo::shard_range::{ShardId, ShardRange};
use std::sync::Arc;

/// Helper to create a filesystem-backed factory with a tempdir
fn make_fs_factory(tmp: &tempfile::TempDir) -> Arc<ShardFactory> {
    let template = DatabaseTemplate {
        backend: Backend::Fs,
        path: tmp.path().join("%shard%").to_string_lossy().to_string(),
        ..Default::default()
    };
    Arc::new(ShardFactory::new(
        template,
        MockGubernatorClient::new_arc(),
        None,
    ))
}

/// Helper to create a Memory-backed factory rooted at a unique shared root.
///
/// All shards opened through this factory share one in-memory store, so
/// multi-shard clone/open round-trips exercise the same shared-root path
/// semantics as a real object store (GCS/S3).
fn make_memory_factory() -> Arc<ShardFactory> {
    let template = DatabaseTemplate {
        backend: Backend::Memory,
        path: format!("{}/%shard%", ShardId::new()),
        ..Default::default()
    };
    Arc::new(ShardFactory::new(
        template,
        MockGubernatorClient::new_arc(),
        None,
    ))
}

/// Helper to create a Memory-backed factory with a separate Memory WAL store.
///
/// The data store is rooted at a shared root (so parent and children resolve
/// under one store); each shard gets its own WAL store keyed by the per-shard
/// WAL path, mirroring a real object-store deployment with split WAL.
fn make_memory_factory_with_wal() -> Arc<ShardFactory> {
    let root = ShardId::new();
    let template = DatabaseTemplate {
        backend: Backend::Memory,
        path: format!("{root}/data/%shard%"),
        wal: Some(WalConfig {
            backend: Backend::Memory,
            path: format!("{root}/wal/%shard%"),
        }),
        ..Default::default()
    };
    Arc::new(ShardFactory::new(
        template,
        MockGubernatorClient::new_arc(),
        None,
    ))
}

/// Helper to create a filesystem-backed factory with separate WAL dir
fn make_fs_factory_with_wal(
    data_tmp: &tempfile::TempDir,
    wal_tmp: &tempfile::TempDir,
) -> Arc<ShardFactory> {
    let template = DatabaseTemplate {
        backend: Backend::Fs,
        path: data_tmp
            .path()
            .join("%shard%")
            .to_string_lossy()
            .to_string(),
        wal: Some(WalConfig {
            backend: Backend::Fs,
            path: wal_tmp.path().join("%shard%").to_string_lossy().to_string(),
        }),
        ..Default::default()
    };
    Arc::new(ShardFactory::new(
        template,
        MockGubernatorClient::new_arc(),
        None,
    ))
}

// --- close tests ---

#[silo::test]
async fn close_shard_not_found() {
    let factory = ShardFactory::new_noop();
    let shard_id = ShardId::new();

    // Closing a shard that doesn't exist should succeed silently
    let result = factory.close(&shard_id).await;
    assert!(result.is_ok());
}

#[silo::test]
async fn close_shard_after_open() {
    let tmp = tempfile::tempdir().unwrap();
    let factory = make_fs_factory(&tmp);
    let shard_id = ShardId::new();

    factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");
    assert!(factory.owns_shard(&shard_id));

    factory.close(&shard_id).await.expect("close shard");
    assert!(
        !factory.owns_shard(&shard_id),
        "shard should no longer be owned after close"
    );
    assert!(
        factory.get(&shard_id).is_none(),
        "get should return None after close"
    );
}

// --- reset tests ---

#[silo::test]
async fn reset_shard_after_enqueue() {
    let tmp = tempfile::tempdir().unwrap();
    let factory = make_fs_factory(&tmp);
    let shard_id = ShardId::new();

    let shard = factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");

    // Enqueue a job
    shard
        .enqueue(
            "test-tenant",
            Some("job-001".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"key": "value"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue job");

    // Verify job exists
    let counters = shard.get_counters().await.expect("get counters");
    assert_eq!(counters.total_jobs, 1);

    // Reset the shard
    let new_shard = factory
        .reset(&shard_id, &ShardRange::full())
        .await
        .expect("reset shard");

    // Verify the shard is fresh (no jobs)
    let new_counters = new_shard
        .get_counters()
        .await
        .expect("get counters after reset");
    assert_eq!(new_counters.total_jobs, 0);
    assert!(factory.owns_shard(&shard_id));
}

#[silo::test]
async fn reset_shard_not_previously_opened() {
    let tmp = tempfile::tempdir().unwrap();
    let factory = make_fs_factory(&tmp);
    let shard_id = ShardId::new();

    // Reset on a shard that was never opened should still work
    let shard = factory
        .reset(&shard_id, &ShardRange::full())
        .await
        .expect("reset un-opened shard");

    let counters = shard.get_counters().await.expect("get counters");
    assert_eq!(counters.total_jobs, 0);
    assert!(factory.owns_shard(&shard_id));
}

#[silo::test]
async fn reset_with_wal_config() {
    let data_tmp = tempfile::tempdir().unwrap();
    let wal_tmp = tempfile::tempdir().unwrap();
    let factory = make_fs_factory_with_wal(&data_tmp, &wal_tmp);
    let shard_id = ShardId::new();

    let shard = factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard with WAL");

    // Enqueue a job
    shard
        .enqueue(
            "test-tenant",
            Some("wal-job".to_string()),
            3,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue job with WAL");

    // Reset should delete WAL data too
    let new_shard = factory
        .reset(&shard_id, &ShardRange::full())
        .await
        .expect("reset shard with WAL");

    let counters = new_shard
        .get_counters()
        .await
        .expect("get counters after reset");
    assert_eq!(counters.total_jobs, 0);
}

// --- clone_closed_shard tests ---

#[silo::test]
async fn clone_closed_shard_creates_copies() {
    let tmp = tempfile::tempdir().unwrap();
    let factory = make_fs_factory(&tmp);
    let parent_id = ShardId::new();
    let left_child_id = ShardId::new();
    let right_child_id = ShardId::new();

    let parent = factory
        .open(&parent_id, &ShardRange::full())
        .await
        .expect("open parent");

    // Enqueue a job to the parent
    parent
        .enqueue(
            "test-tenant",
            Some("cloned-job".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"cloned": true})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue to parent");

    // Close parent (clone_closed_shard expects it to be closed)
    parent.close().await.expect("close parent");

    // Clone the shard to both children
    factory
        .clone_closed_shard(&parent_id, &left_child_id, &right_child_id)
        .await
        .expect("clone closed shard");

    // Open the left child and verify it has the same data
    let left_child = factory
        .open(&left_child_id, &ShardRange::full())
        .await
        .expect("open left child");

    let left_counters = left_child
        .get_counters()
        .await
        .expect("get left child counters");
    assert_eq!(
        left_counters.total_jobs, 1,
        "left child should have the parent's job"
    );

    // Verify we can read the specific job from the left child
    let job = left_child
        .get_job("test-tenant", "cloned-job")
        .await
        .expect("get job from left child");
    assert!(job.is_some(), "left child should contain the job");

    // Open the right child and verify it also has the data
    let right_child = factory
        .open(&right_child_id, &ShardRange::full())
        .await
        .expect("open right child");

    let right_counters = right_child
        .get_counters()
        .await
        .expect("get right child counters");
    assert_eq!(
        right_counters.total_jobs, 1,
        "right child should have the parent's job"
    );
}

/// When the parent shard has a separate WAL configured, cloning should propagate
/// WAL stores to the children so they can open without hitting
/// "wal store reconfiguration unsupported".
#[silo::test]
async fn clone_closed_shard_with_split_wal() {
    let data_tmp = tempfile::tempdir().unwrap();
    let wal_tmp = tempfile::tempdir().unwrap();
    let factory = make_fs_factory_with_wal(&data_tmp, &wal_tmp);
    let parent_id = ShardId::new();
    let left_child_id = ShardId::new();
    let right_child_id = ShardId::new();

    let parent = factory
        .open(&parent_id, &ShardRange::full())
        .await
        .expect("open parent");

    // Write data so the shard has WAL activity
    parent
        .enqueue(
            "test-tenant",
            Some("wal-test-job".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"wal": true})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue to parent");

    // Close parent (clone_closed_shard expects it to be closed)
    parent.close().await.expect("close parent");

    // Clone the shard — this should succeed even with split WAL
    factory
        .clone_closed_shard(&parent_id, &left_child_id, &right_child_id)
        .await
        .expect("clone closed shard with WAL");

    // Open the children — this is where "wal store reconfiguration unsupported"
    // would occur if the clone didn't properly configure the WAL store
    let left_child = factory
        .open(&left_child_id, &ShardRange::full())
        .await
        .expect("open left child with WAL");

    let left_counters = left_child
        .get_counters()
        .await
        .expect("get left child counters");
    assert_eq!(
        left_counters.total_jobs, 1,
        "left child should have the parent's job"
    );

    let right_child = factory
        .open(&right_child_id, &ShardRange::full())
        .await
        .expect("open right child with WAL");

    let right_counters = right_child
        .get_counters()
        .await
        .expect("get right child counters");
    assert_eq!(
        right_counters.total_jobs, 1,
        "right child should have the parent's job"
    );
}

// --- object-store clone round-trip tests ---

/// Regression guard for the object-store split data-loss signature.
///
/// On an object-store backend, parent and children must resolve under one
/// shared storage root so a clone lands exactly where `open` later reads. When
/// each shard instead gets its own store root, the children open empty and the
/// shard's data is silently lost. This drives the clone round-trip on the
/// shared-root Memory backend and asserts each child holds the parent's full
/// job set.
#[silo::test]
async fn clone_closed_shard_object_store_preserves_jobs() {
    let factory = make_memory_factory();
    let parent_id = ShardId::new();
    let left_child_id = ShardId::new();
    let right_child_id = ShardId::new();

    let parent = factory
        .open(&parent_id, &ShardRange::full())
        .await
        .expect("open parent");

    parent
        .enqueue(
            "test-tenant",
            Some("cloned-job".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"cloned": true})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue to parent");

    let parent_counters = parent.get_counters().await.expect("get parent counters");
    assert_eq!(parent_counters.total_jobs, 1);

    // Close parent (clone_closed_shard expects it to be closed)
    parent.close().await.expect("close parent");

    factory
        .clone_closed_shard(&parent_id, &left_child_id, &right_child_id)
        .await
        .expect("clone closed shard");

    // A fresh clone is a full byte-for-byte copy of the parent, so each child
    // holds the parent's whole job set immediately after the split (cleanup
    // trims each child to its range later).
    let left_child = factory
        .open(&left_child_id, &ShardRange::full())
        .await
        .expect("open left child");
    assert_eq!(
        left_child
            .get_counters()
            .await
            .expect("get left child counters")
            .total_jobs,
        1,
        "left child should have the parent's job"
    );
    assert!(
        left_child
            .get_job("test-tenant", "cloned-job")
            .await
            .expect("get job from left child")
            .is_some(),
        "left child should contain the parent's job"
    );

    let right_child = factory
        .open(&right_child_id, &ShardRange::full())
        .await
        .expect("open right child");
    assert_eq!(
        right_child
            .get_counters()
            .await
            .expect("get right child counters")
            .total_jobs,
        1,
        "right child should have the parent's job"
    );
}

/// The object-store clone round-trip must also work with a separate WAL store:
/// cloning propagates each child's WAL store so the children open without
/// "wal store reconfiguration unsupported", and each holds the parent's jobs.
#[silo::test]
async fn clone_closed_shard_object_store_with_split_wal() {
    let factory = make_memory_factory_with_wal();
    let parent_id = ShardId::new();
    let left_child_id = ShardId::new();
    let right_child_id = ShardId::new();

    let parent = factory
        .open(&parent_id, &ShardRange::full())
        .await
        .expect("open parent");
    parent
        .enqueue(
            "test-tenant",
            Some("wal-job".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"wal": true})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue to parent");
    parent.close().await.expect("close parent");

    factory
        .clone_closed_shard(&parent_id, &left_child_id, &right_child_id)
        .await
        .expect("clone closed shard with WAL on object store");

    // Opening the children is where "wal store reconfiguration unsupported"
    // would surface if the clone did not configure each child's WAL store.
    let left_child = factory
        .open(&left_child_id, &ShardRange::full())
        .await
        .expect("open left child with WAL");
    assert_eq!(
        left_child
            .get_counters()
            .await
            .expect("left counters")
            .total_jobs,
        1,
        "left child should hold the parent's job"
    );

    let right_child = factory
        .open(&right_child_id, &ShardRange::full())
        .await
        .expect("open right child with WAL");
    assert_eq!(
        right_child
            .get_counters()
            .await
            .expect("right counters")
            .total_jobs,
        1,
        "right child should hold the parent's job"
    );
}

/// `delete_shard_data` on an object-store backend must scope deletion to
/// exactly one shard's prefix. Two shards share a single store root, so
/// deleting one (via `reset`) must not touch a sibling whose prefix differs
/// only by shard name.
#[silo::test]
async fn delete_object_store_shard_data_leaves_sibling_intact() {
    let factory = make_memory_factory();
    let shard_a = ShardId::new();
    let shard_b = ShardId::new();

    let a = factory
        .open(&shard_a, &ShardRange::full())
        .await
        .expect("open shard a");
    let b = factory
        .open(&shard_b, &ShardRange::full())
        .await
        .expect("open shard b");

    for (shard, job_id) in [(&a, "job-a"), (&b, "job-b")] {
        shard
            .enqueue(
                "test-tenant",
                Some(job_id.to_string()),
                5,
                test_helpers::now_ms(),
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"k": "v"})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue job");
    }

    // reset closes shard A, deletes its data via delete_shard_data, then reopens.
    let a_reset = factory
        .reset(&shard_a, &ShardRange::full())
        .await
        .expect("reset shard a");
    assert_eq!(
        a_reset.get_counters().await.expect("a counters").total_jobs,
        0,
        "reset shard A should be empty after its data is deleted"
    );

    // Sibling B shares the store root but its prefix differs by shard name, so
    // deleting A must not have removed B's data.
    assert_eq!(
        b.get_counters().await.expect("b counters").total_jobs,
        1,
        "sibling shard B should keep its job after shard A's data is deleted"
    );
    assert!(
        b.get_job("test-tenant", "job-b")
            .await
            .expect("get job from b")
            .is_some(),
        "sibling shard B should still contain its job"
    );
}

// --- post-clone child verification tests ---

/// A child that opens empty/short of the parent's job count is the
/// object-store split data-loss signature. The pre-commit verification must
/// detect it: a child whose whole-shard `total_jobs` differs from the parent's
/// pre-close count fails verification so the split can abort before the
/// shard-map commit.
#[silo::test]
async fn verify_cloned_children_detects_short_child() {
    let factory = make_memory_factory();
    let parent_id = ShardId::new();

    let parent = factory
        .open(&parent_id, &ShardRange::full())
        .await
        .expect("open parent");
    parent
        .enqueue(
            "test-tenant",
            Some("only-job".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"k": "v"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue to parent");
    let parent_total = parent
        .get_counters()
        .await
        .expect("parent counters")
        .total_jobs;
    assert_eq!(parent_total, 1);
    parent.close().await.expect("close parent");

    // A never-cloned child opens as an empty database -- total_jobs 0 -- the
    // empty-child symptom of a misplaced clone. It must not match a parent
    // that held one job.
    let empty_child = ShardId::new();
    let result = factory
        .verify_and_initialize_cloned_children(&parent_id, &[empty_child])
        .await;
    assert!(
        result.is_err(),
        "an empty child must fail verification against a non-empty parent"
    );
}

/// A successful clone -- each child a full copy of the parent -- must pass
/// verification (no false positives), and the raw verification opens must not
/// register the children in the factory's instance map. If they did, the
/// post-commit `open(child)` would return the stale pre-commit handle.
#[silo::test]
async fn verify_cloned_children_accepts_full_clone_without_caching() {
    let factory = make_memory_factory();
    let parent_id = ShardId::new();
    let left_child_id = ShardId::new();
    let right_child_id = ShardId::new();

    let parent = factory
        .open(&parent_id, &ShardRange::full())
        .await
        .expect("open parent");
    parent
        .enqueue(
            "test-tenant",
            Some("only-job".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"k": "v"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue to parent");
    let parent_total = parent
        .get_counters()
        .await
        .expect("parent counters")
        .total_jobs;
    parent.close().await.expect("close parent");

    factory
        .clone_closed_shard(&parent_id, &left_child_id, &right_child_id)
        .await
        .expect("clone closed shard");

    factory
        .verify_and_initialize_cloned_children(&parent_id, &[left_child_id, right_child_id])
        .await
        .expect("full clones should pass verification");

    // The raw verification open must leave no factory instance behind.
    assert!(
        !factory.owns_shard(&left_child_id),
        "verification must not cache the left child in the instances map"
    );
    assert!(
        !factory.owns_shard(&right_child_id),
        "verification must not cache the right child in the instances map"
    );
    assert!(factory.get(&left_child_id).is_none());
    assert!(factory.get(&right_child_id).is_none());

    // The post-clone `open` returns a child that actually holds the parent's
    // data (not a stale pre-commit handle).
    let left_child = factory
        .open(&left_child_id, &ShardRange::full())
        .await
        .expect("open left child after verification");
    assert_eq!(
        left_child
            .get_counters()
            .await
            .expect("left counters")
            .total_jobs,
        parent_total,
    );
}

/// When verification fails, the split aborts before touching the parent: the
/// parent's data is untouched and the parent reopens with its full job set.
/// (In the split state machine this maps to `PreCommitParentClosed`, which
/// abandons the split and recovers the parent.)
#[silo::test]
async fn parent_reopens_intact_after_verification_mismatch() {
    let factory = make_memory_factory();
    let parent_id = ShardId::new();
    let left_child_id = ShardId::new();
    let right_child_id = ShardId::new();

    let parent = factory
        .open(&parent_id, &ShardRange::full())
        .await
        .expect("open parent");
    parent
        .enqueue(
            "test-tenant",
            Some("survivor-job".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"k": "v"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue to parent");
    parent.close().await.expect("close parent");

    factory
        .clone_closed_shard(&parent_id, &left_child_id, &right_child_id)
        .await
        .expect("clone closed shard");

    // Force a mismatch by verifying a child that was never cloned: it opens
    // empty and cannot match the parent's job count.
    let result = factory
        .verify_and_initialize_cloned_children(&parent_id, &[ShardId::new()])
        .await;
    assert!(
        result.is_err(),
        "a never-cloned child must fail verification"
    );

    // Recover the parent the way the split state machine does on a pre-commit
    // abort: evict the stale closed factory entry, then reopen. The parent's
    // data is untouched by the failed child verification, so it reopens intact.
    factory
        .close(&parent_id)
        .await
        .expect("evict stale parent entry");
    let reopened = factory
        .open(&parent_id, &ShardRange::full())
        .await
        .expect("reopen parent after failed verification");
    assert_eq!(
        reopened
            .get_counters()
            .await
            .expect("parent counters")
            .total_jobs,
        1,
        "parent should retain its data after a failed split verification"
    );
    assert!(
        reopened
            .get_job("test-tenant", "survivor-job")
            .await
            .expect("get job from reopened parent")
            .is_some(),
        "parent should still contain its job after a failed split verification"
    );
}

// --- object-store layout tests ---

/// Pins the absolute object keys for URL-style templates to the deployed
/// double-nested layout, so shard resolution changes can never silently move
/// existing shards' data.
///
/// slatedb roots a URL-resolved store at the URL's path component (a
/// `PrefixStore`), so absolute keys are `<root-path>/<db-path>/…`. For a
/// `gs://bucket/silo/%shard%` template every existing shard's objects live at
/// `silo/<shard>/silo/<shard>/…`, which requires the store to resolve at the
/// shared root `gs://bucket/silo` with database path `<shard>/silo/<shard>`.
/// A bare-shard-name database path (keys `silo/<shard>/<shard>/…`) or a
/// per-shard store root (keys `silo/<shard>/silo/<shard>/…` but with parent
/// and children on distinct roots) both fail this pin.
#[silo::test]
fn url_template_resolves_shared_root_and_double_nested_db_path() {
    let (root, db_path) = ShardFactory::object_store_layout(
        &Backend::Gcs,
        "gs://test-bucket/silo/%shard%",
        "0a1b2c3d-0000-0000-0000-000000000000",
    )
    .expect("layout for gs template");
    assert_eq!(root, "gs://test-bucket/silo");
    assert_eq!(
        db_path,
        "0a1b2c3d-0000-0000-0000-000000000000/silo/0a1b2c3d-0000-0000-0000-000000000000"
    );

    // {shard} placeholder form resolves identically.
    let (root, db_path) =
        ShardFactory::object_store_layout(&Backend::Url, "s3://test-bucket/prefix/{shard}", "abc")
            .expect("layout for s3 template");
    assert_eq!(root, "s3://test-bucket/prefix");
    assert_eq!(db_path, "abc/prefix/abc");
}

/// URL-style templates must put the shard placeholder at a `/` boundary:
/// off-boundary placeholders would resolve to different absolute keys than
/// the per-shard resolution the deployed data was written under, silently
/// moving every shard's data. Memory templates stay unrestricted — they are
/// test sentinels with no deployed data.
#[silo::test]
fn url_template_with_non_boundary_placeholder_is_rejected() {
    let result =
        ShardFactory::object_store_layout(&Backend::Gcs, "gs://test-bucket/silo-%shard%", "abc");
    assert!(
        result.is_err(),
        "off-boundary placeholder must be rejected for URL-style backends"
    );

    let (root, db_path) =
        ShardFactory::object_store_layout(&Backend::Memory, "mem-0/shard-%shard%", "abc")
            .expect("memory templates stay unrestricted");
    assert_eq!(root, "mem-0/shard-");
    assert_eq!(db_path, "abc/mem-0/shard-abc");
}

/// A template without a shard placeholder (test sentinels, the noop factory)
/// degenerates to the whole path as the shared root and the store-path
/// component as the database path — the same keys the template produces today.
#[silo::test]
fn url_template_without_placeholder_uses_store_path_component() {
    let (root, db_path) = ShardFactory::object_store_layout(
        &Backend::Gcs,
        "gs://test-bucket/silo/fixed",
        "ignored-shard",
    )
    .expect("layout for placeholder-free gs template");
    assert_eq!(root, "gs://test-bucket/silo/fixed");
    assert_eq!(db_path, "silo/fixed");

    let (root, db_path) =
        ShardFactory::object_store_layout(&Backend::Memory, "some-sentinel", "ignored-shard")
            .expect("layout for placeholder-free memory template");
    assert_eq!(root, "some-sentinel");
    assert_eq!(db_path, "some-sentinel");
}

/// The Memory registry shares stores per root, so each noop factory needs a
/// unique template root — otherwise every noop factory in the process would
/// read and write one shared store.
#[silo::test]
fn noop_factories_use_unique_template_roots() {
    let a = ShardFactory::new_noop();
    let b = ShardFactory::new_noop();
    assert_ne!(
        a.template().path,
        b.template().path,
        "each noop factory must get its own template root"
    );
}

/// Memory templates use the same layout arithmetic as URL backends — the
/// store-path component is the raw expanded path — so in-memory tests exercise
/// the production key nesting.
#[silo::test]
fn memory_template_layout_mirrors_url_arithmetic() {
    let (root, db_path) =
        ShardFactory::object_store_layout(&Backend::Memory, "mem-root/data/%shard%", "abc")
            .expect("layout for memory template");
    assert_eq!(root, "mem-root/data");
    assert_eq!(db_path, "abc/mem-root/data/abc");
}

/// End-to-end pin of the Memory-backend key layout: objects written through a
/// factory-opened shard land in the shared per-root store under the
/// layout-preserving database path `<shard>/<root>/<shard>/…`, and are visible
/// to a later independent resolution of the same root.
#[silo::test]
async fn memory_shard_objects_land_under_layout_preserving_keys() {
    use futures::TryStreamExt;

    let root = format!("layout-root-{}", ShardId::new());
    let factory = Arc::new(ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Memory,
            path: format!("{root}/%shard%"),
            ..Default::default()
        },
        MockGubernatorClient::new_arc(),
        None,
    ));
    let shard_id = ShardId::new();

    let shard = factory
        .open(&shard_id, &ShardRange::full())
        .await
        .expect("open shard");
    shard
        .enqueue(
            "test-tenant",
            Some("layout-job".to_string()),
            5,
            test_helpers::now_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"k": "v"})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");
    factory.close(&shard_id).await.expect("close shard");

    // An independent resolution of the same root sees the shard's objects —
    // and every key sits under the double-nested layout-preserving prefix.
    let resolved =
        silo::storage::resolve_object_store(&Backend::Memory, &root).expect("resolve shared root");
    let objects: Vec<_> = resolved
        .store
        .list(None)
        .try_collect()
        .await
        .expect("list store");
    assert!(
        !objects.is_empty(),
        "the shard's objects must be visible under the shared root"
    );
    let expected_prefix = format!("{shard_id}/{root}/{shard_id}/");
    for obj in &objects {
        assert!(
            obj.location.as_ref().starts_with(&expected_prefix),
            "object key {} must start with layout-preserving prefix {}",
            obj.location,
            expected_prefix
        );
    }
}

// --- template path validation tests ---

#[silo::test]
async fn open_invalid_template_no_placeholder() {
    let factory = ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: "/tmp/no-placeholder-here".to_string(),
            ..Default::default()
        },
        MockGubernatorClient::new_arc(),
        None,
    );

    let shard_id = ShardId::new();
    let result = factory.open(&shard_id, &ShardRange::full()).await;
    assert!(result.is_err(), "should fail without placeholder");
    let err = result.err().unwrap();
    let err_msg = format!("{}", err);
    assert!(
        err_msg.contains("shard placeholder"),
        "error should mention shard placeholder, got: {}",
        err_msg
    );
}

#[silo::test]
async fn open_invalid_template_bad_boundary() {
    let factory = ShardFactory::new(
        DatabaseTemplate {
            backend: Backend::Fs,
            path: "/tmp/prefix%shard%".to_string(),
            ..Default::default()
        },
        MockGubernatorClient::new_arc(),
        None,
    );

    let shard_id = ShardId::new();
    let result = factory.open(&shard_id, &ShardRange::full()).await;
    assert!(result.is_err(), "should fail with non-boundary placeholder");
    let err = result.err().unwrap();
    let err_msg = format!("{}", err);
    assert!(
        err_msg.contains("preceded by '/'"),
        "error should mention path boundary, got: {}",
        err_msg
    );
}

// --- instances and owns_shard tests ---

#[silo::test]
async fn factory_instances_and_ownership() {
    let tmp = tempfile::tempdir().unwrap();
    let factory = make_fs_factory(&tmp);
    let shard1 = ShardId::new();
    let shard2 = ShardId::new();

    assert!(factory.instances().is_empty());
    assert!(!factory.owns_shard(&shard1));

    factory
        .open(&shard1, &ShardRange::full())
        .await
        .expect("open shard1");
    factory
        .open(&shard2, &ShardRange::full())
        .await
        .expect("open shard2");

    assert_eq!(factory.instances().len(), 2);
    assert!(factory.owns_shard(&shard1));
    assert!(factory.owns_shard(&shard2));

    factory.close(&shard1).await.expect("close shard1");
    assert_eq!(factory.instances().len(), 1);
    assert!(!factory.owns_shard(&shard1));
    assert!(factory.owns_shard(&shard2));
}

// --- close_all test ---

#[silo::test]
async fn close_all_shards() {
    let tmp = tempfile::tempdir().unwrap();
    let factory = make_fs_factory(&tmp);
    let shard1 = ShardId::new();
    let shard2 = ShardId::new();

    factory
        .open(&shard1, &ShardRange::full())
        .await
        .expect("open shard1");
    factory
        .open(&shard2, &ShardRange::full())
        .await
        .expect("open shard2");

    assert_eq!(factory.instances().len(), 2);

    factory.close_all().await.expect("close all");

    // After close_all, instances still has entries but they are closed
    // owns_shard checks initialization status
    // The shards are closed but the entries remain in the DashMap
    // Let's verify get returns something (the OnceCell is still initialized)
    // Actually close_all doesn't remove from instances, only close() does
    // So get() will still return the closed shard Arc
    // The key test is that close_all doesn't error
}
