//! Tests for `JobStoreShard::reconcile_counters`.
//!
//! The reconciler is the sole owner of counter correctness in steady state —
//! the standalone compactor drops terminal job rows but cannot decrement
//! counters (no writable Db handle), so this background task closes the loop
//! by re-deriving counter truth from `JOB_INFO`/`JOB_STATUS` and writing a
//! signed merge operand to correct any drift.

mod test_helpers;

use silo::job_attempt::AttemptOutcome;
use silo::keys::{
    shard_completed_jobs_counter_key, shard_total_jobs_counter_key, tenant_status_counter_key,
};
use silo::shard_range::ShardRange;
use test_helpers::*;

/// Inject a signed delta directly against a counter key, bypassing the normal
/// transactional bookkeeping. Used to seed drift for tests.
async fn drift_counter(shard: &silo::job_store_shard::JobStoreShard, key: &[u8], delta: i64) {
    shard
        .db()
        .merge(key, delta.to_le_bytes())
        .await
        .expect("seed drift via merge");
}

async fn enqueue_one(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    suffix: &str,
) -> String {
    shard
        .enqueue(
            tenant,
            None,
            10u8,
            now_ms(),
            None,
            msgpack_payload(&serde_json::json!({"reconcile": suffix})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue")
}

async fn dequeue_and_succeed(shard: &silo::job_store_shard::JobStoreShard) {
    let tasks = shard
        .dequeue("w", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1, "expected one task");
    let task_id = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report success");
}

#[silo::test]
async fn reconcile_corrects_total_jobs_drift() {
    let (_tmp, shard) = open_temp_shard().await;

    enqueue_one(&shard, "-", "a").await;
    enqueue_one(&shard, "-", "b").await;

    // Inject +5 drift into total_jobs.
    drift_counter(&shard, &shard_total_jobs_counter_key(), 5).await;

    let pre = shard.get_counters().await.expect("get_counters");
    assert_eq!(pre.total_jobs, 7, "drifted total should be 2 + 5");

    let summary = shard.reconcile_counters(&ShardRange::full()).await;
    assert_eq!(summary.failed, 0, "no failures expected");
    assert!(summary.corrected >= 1, "should issue at least one correction");

    let post = shard.get_counters().await.expect("get_counters");
    assert_eq!(post.total_jobs, 2, "reconcile should restore truth");
    assert_eq!(post.completed_jobs, 0);
}

#[silo::test]
async fn reconcile_corrects_completed_jobs_drift() {
    let (_tmp, shard) = open_temp_shard().await;

    enqueue_one(&shard, "-", "a").await;
    enqueue_one(&shard, "-", "b").await;
    dequeue_and_succeed(&shard).await;
    dequeue_and_succeed(&shard).await;

    // Inject -3 drift into completed_jobs (under-report).
    drift_counter(&shard, &shard_completed_jobs_counter_key(), -3).await;

    let pre = shard.get_counters().await.expect("get_counters");
    assert_eq!(pre.completed_jobs, -1, "drifted completed should be 2 - 3");

    let summary = shard.reconcile_counters(&ShardRange::full()).await;
    assert_eq!(summary.failed, 0);

    let post = shard.get_counters().await.expect("get_counters");
    assert_eq!(post.total_jobs, 2);
    assert_eq!(post.completed_jobs, 2, "reconcile should restore truth");
}

#[silo::test]
async fn reconcile_is_idempotent() {
    let (_tmp, shard) = open_temp_shard().await;

    enqueue_one(&shard, "-", "a").await;
    dequeue_and_succeed(&shard).await;

    let first = shard.reconcile_counters(&ShardRange::full()).await;
    assert_eq!(first.failed, 0);

    let second = shard.reconcile_counters(&ShardRange::full()).await;
    assert_eq!(second.failed, 0);
    assert_eq!(
        second.corrected, 0,
        "second pass should issue zero corrections when truth already matches"
    );

    let counters = shard.get_counters().await.expect("get_counters");
    assert_eq!(counters.total_jobs, 1);
    assert_eq!(counters.completed_jobs, 1);
}

#[silo::test]
async fn reconcile_zeroes_zombie_tenant_status_counter() {
    let (_tmp, shard) = open_temp_shard().await;

    // Seed a tenant_status_counter for a tenant/kind with no matching JOB_STATUS.
    drift_counter(
        &shard,
        &tenant_status_counter_key("ghost-tenant", "Succeeded"),
        7,
    )
    .await;

    let summary = shard.reconcile_counters(&ShardRange::full()).await;
    assert_eq!(summary.failed, 0);

    let entries = shard
        .scan_tenant_status_counters(None)
        .await
        .expect("scan tenant_status");
    assert!(
        !entries
            .iter()
            .any(|(t, k, _)| t == "ghost-tenant" && k == "Succeeded"),
        "zombie counter should have been driven to 0 (filtered out by scan): {entries:?}"
    );
}

#[silo::test]
async fn reconcile_corrects_tenant_status_counter_drift() {
    let (_tmp, shard) = open_temp_shard().await;

    enqueue_one(&shard, "-", "a").await;
    dequeue_and_succeed(&shard).await;

    // Drift the per-tenant Succeeded counter upward.
    drift_counter(&shard, &tenant_status_counter_key("-", "Succeeded"), 4).await;

    let summary = shard.reconcile_counters(&ShardRange::full()).await;
    assert_eq!(summary.failed, 0);
    assert!(summary.corrected >= 1);

    let entries = shard
        .scan_tenant_status_counters(None)
        .await
        .expect("scan tenant_status");
    let succeeded = entries
        .iter()
        .find(|(t, k, _)| t == "-" && k == "Succeeded")
        .map(|(_, _, c)| *c);
    assert_eq!(
        succeeded,
        Some(1),
        "tenant_status Succeeded should be exactly 1 after reconcile, got entries {entries:?}"
    );
}

#[silo::test]
async fn reconcile_summary_reports_scanned_jobs() {
    let (_tmp, shard) = open_temp_shard().await;

    enqueue_one(&shard, "-", "a").await;
    enqueue_one(&shard, "-", "b").await;
    enqueue_one(&shard, "-", "c").await;

    let summary = shard.reconcile_counters(&ShardRange::full()).await;
    assert_eq!(summary.failed, 0);
    assert_eq!(summary.scanned_jobs, 3, "should have scanned 3 JOB_STATUS rows");
}
