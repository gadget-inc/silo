mod test_helpers;

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray, UInt32Array};
use datafusion::arrow::record_batch::RecordBatch;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::keys::{
    ParsedStatusTimeIndexKey, end_bound, idx_status_time_prefix, parse_status_time_index_key,
};
use silo::query::ShardQueryEngine;
use silo::retry::RetryPolicy;
use slatedb::DbIterator;
use test_helpers::*;

// Helper to extract string column values from batches
fn extract_string_column(batches: &[RecordBatch], col_idx: usize) -> Vec<String> {
    let mut result = Vec::new();
    for batch in batches {
        let col = batch.column(col_idx);
        let sa = col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column");
        for i in 0..sa.len() {
            if !sa.is_null(i) {
                result.push(sa.value(i).to_string());
            }
        }
    }
    result
}

// Helper to run SQL query and collect results
async fn query_ids(sql: &ShardQueryEngine, query: &str) -> Vec<String> {
    let batches = sql
        .sql(query)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    extract_string_column(&batches, 0)
}

// Helper to enqueue a simple job
async fn enqueue_job(shard: &JobStoreShard, id: &str, priority: u8, start_at_ms: i64) -> String {
    shard
        .enqueue(
            "-",
            Some(id.to_string()),
            priority,
            start_at_ms,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue")
}

// Helper to enqueue a job for a specific tenant
async fn enqueue_job_for_tenant(
    shard: &JobStoreShard,
    tenant: &str,
    id: &str,
    priority: u8,
    start_at_ms: i64,
) -> String {
    shard
        .enqueue(
            tenant,
            Some(id.to_string()),
            priority,
            start_at_ms,
            None,
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue")
}

/// Collect all status/time index entries (0x03) for a given tenant and status.
async fn collect_index_entries(
    shard: &JobStoreShard,
    tenant: &str,
    status: &str,
) -> Vec<ParsedStatusTimeIndexKey> {
    let start = idx_status_time_prefix(tenant, status);
    let end = end_bound(&start);
    let mut iter: DbIterator = shard.db().scan::<Vec<u8>, _>(start..end).await.unwrap();
    let mut out = Vec::new();
    loop {
        let maybe = iter.next().await.unwrap();
        let Some(kv) = maybe else { break };
        if let Some(parsed) = parse_status_time_index_key(&kv.key) {
            out.push(parsed);
        }
    }
    out
}

/// Collect ALL status/time index entries (0x03) across all tenants and statuses.
async fn collect_all_index_entries(shard: &JobStoreShard) -> Vec<ParsedStatusTimeIndexKey> {
    let start = vec![0x03u8];
    let end = end_bound(&start);
    let mut iter: DbIterator = shard.db().scan::<Vec<u8>, _>(start..end).await.unwrap();
    let mut out = Vec::new();
    loop {
        let maybe = iter.next().await.unwrap();
        let Some(kv) = maybe else { break };
        if let Some(parsed) = parse_status_time_index_key(&kv.key) {
            out.push(parsed);
        }
    }
    out
}

// ===== SQL-level query tests =====

#[silo::test]
async fn sql_waiting_status_display() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue with past start time (should be Waiting)
    enqueue_job(&shard, "past-job", 10, now - 5000).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let batches = sql
        .sql("SELECT id, status_kind, current_attempt FROM jobs WHERE tenant = '-' AND id = 'past-job'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    let ids = extract_string_column(&batches, 0);
    assert_eq!(ids, vec!["past-job"]);

    let statuses = extract_string_column(&batches, 1);
    assert_eq!(statuses, vec!["Waiting"]);

    // Freshly enqueued job should have current_attempt = 1 (1-indexed)
    let attempt_col = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("current_attempt column");
    assert_eq!(attempt_col.value(0), 1);
}

#[silo::test]
async fn sql_future_scheduled_status_display() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let future_start = now + 60_000;

    // Enqueue with future start time (should be Scheduled)
    enqueue_job(&shard, "future-job", 10, future_start).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let batches = sql
        .sql("SELECT id, status_kind, current_attempt, next_attempt_starts_after_ms FROM jobs WHERE tenant = '-' AND id = 'future-job'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    let ids = extract_string_column(&batches, 0);
    assert_eq!(ids, vec!["future-job"]);

    let statuses = extract_string_column(&batches, 1);
    assert_eq!(statuses, vec!["Scheduled"]);

    // Freshly enqueued job should have current_attempt = 1 (1-indexed)
    let attempt_col = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("current_attempt column");
    assert_eq!(attempt_col.value(0), 1);

    // next_attempt_starts_after_ms should be the future start time
    let next_attempt_col = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("next_attempt_starts_after_ms column");
    assert_eq!(next_attempt_col.value(0), future_start);
}

#[silo::test]
async fn sql_filter_waiting() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Waiting: start_time <= now
    enqueue_job(&shard, "w1", 10, now - 1000).await;
    enqueue_job(&shard, "w2", 10, now).await;

    // Future-scheduled: start_time > now
    enqueue_job(&shard, "f1", 10, now + 60_000).await;

    // Running: dequeue one of the waiting jobs
    shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue")
        .tasks;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Waiting' ORDER BY id",
    )
    .await;

    // Only non-dequeued waiting jobs (w2 remains, w1 was dequeued and is now Running)
    assert_eq!(got, vec!["w2"]);
}

#[silo::test]
async fn sql_filter_scheduled_future_only() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Waiting jobs (start_time <= now)
    enqueue_job(&shard, "w1", 10, now - 1000).await;
    enqueue_job(&shard, "w2", 10, now).await;

    // Future-scheduled jobs
    enqueue_job(&shard, "f1", 10, now + 60_000).await;
    enqueue_job(&shard, "f2", 10, now + 120_000).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Scheduled' ORDER BY id",
    )
    .await;

    // Only future-scheduled jobs
    assert_eq!(got, vec!["f1", "f2"]);
}

#[silo::test]
async fn sql_filter_waiting_cross_tenant() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Waiting jobs across tenants
    enqueue_job_for_tenant(&shard, "tenant_a", "a1", 10, now - 1000).await;
    enqueue_job_for_tenant(&shard, "tenant_b", "b1", 10, now).await;

    // Future-scheduled
    enqueue_job_for_tenant(&shard, "tenant_a", "a2", 10, now + 60_000).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");

    // Cross-tenant waiting query (no tenant filter)
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE status_kind = 'Waiting' ORDER BY id",
    )
    .await;

    assert_eq!(got, vec!["a1", "b1"]);
}

#[silo::test]
async fn sql_count_waiting() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // 3 waiting jobs
    enqueue_job(&shard, "w1", 10, now - 2000).await;
    enqueue_job(&shard, "w2", 10, now - 1000).await;
    enqueue_job(&shard, "w3", 10, now).await;

    // 1 future-scheduled
    enqueue_job(&shard, "f1", 10, now + 60_000).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");
    let batches = sql
        .sql("SELECT COUNT(*) FROM jobs WHERE tenant = '-' AND status_kind = 'Waiting'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    assert!(!batches.is_empty());
    let count_col = batches[0].column(0);
    let count_arr = count_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column");
    assert_eq!(count_arr.value(0), 3);
}

#[silo::test]
async fn scheduled_start_index_lifecycle() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue with future start time
    enqueue_job(&shard, "lifecycle", 10, now + 60_000).await;

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");

    // Should be future-scheduled
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Scheduled'",
    )
    .await;
    assert_eq!(got, vec!["lifecycle"]);

    // Should NOT be Waiting
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Waiting'",
    )
    .await;
    assert!(got.is_empty());

    // Expedite the job to run now
    shard
        .expedite_job("-", "lifecycle")
        .await
        .expect("expedite");

    // After expedite, should be Waiting (start_time = now)
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Waiting'",
    )
    .await;
    assert_eq!(got, vec!["lifecycle"]);

    // Should no longer be in future-scheduled
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Scheduled'",
    )
    .await;
    assert!(got.is_empty());

    // Cancel the waiting/scheduled job (status transitions to Cancelled immediately for Scheduled jobs)
    shard.cancel_job("-", "lifecycle").await.expect("cancel");

    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Cancelled'",
    )
    .await;
    assert_eq!(got, vec!["lifecycle"]);

    // Should no longer appear as Waiting or Scheduled
    let got = query_ids(
        &sql,
        "SELECT id FROM jobs WHERE tenant = '-' AND status_kind = 'Waiting'",
    )
    .await;
    assert!(got.is_empty());
}

// ===== Storage-level index verification tests =====
//
// These tests directly scan the 0x03 status/time index prefix to verify
// that exactly the right entries exist at each step. This ensures the
// surgical single-delete approach correctly maintains the index.

#[silo::test]
async fn index_entry_uses_start_time_for_scheduled() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let future_start = now + 60_000;

    enqueue_job(&shard, "j1", 10, future_start).await;

    let entries = collect_index_entries(&shard, "-", "Scheduled").await;
    assert_eq!(entries.len(), 1, "expected exactly 1 Scheduled index entry");
    assert_eq!(entries[0].job_id, "j1");
    // The index timestamp should be the inverted start_at_ms, not changed_at_ms
    assert_eq!(
        entries[0].inverted_timestamp,
        u64::MAX - future_start as u64,
        "index should be keyed by start_at_ms for Scheduled jobs"
    );
}

#[silo::test]
async fn index_entry_uses_start_time_for_waiting() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let past_start = now - 5000;

    enqueue_job(&shard, "j1", 10, past_start).await;

    let entries = collect_index_entries(&shard, "-", "Scheduled").await;
    assert_eq!(entries.len(), 1, "expected exactly 1 Scheduled index entry");
    assert_eq!(entries[0].job_id, "j1");
    // Even for past start times, the index is keyed by start_at_ms
    assert_eq!(
        entries[0].inverted_timestamp,
        u64::MAX - past_start as u64,
        "index should be keyed by start_at_ms even for past start times"
    );
}

#[silo::test]
async fn index_dequeue_removes_scheduled_adds_running() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "j1", 10, now - 1000).await;

    // Before dequeue: 1 Scheduled entry
    let scheduled = collect_index_entries(&shard, "-", "Scheduled").await;
    assert_eq!(scheduled.len(), 1);

    // Dequeue
    let result = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue");
    assert_eq!(result.tasks.len(), 1);

    // After dequeue: Scheduled entry removed, Running entry added
    let scheduled = collect_index_entries(&shard, "-", "Scheduled").await;
    assert_eq!(
        scheduled.len(),
        0,
        "Scheduled index entry should be removed after dequeue"
    );

    let running = collect_index_entries(&shard, "-", "Running").await;
    assert_eq!(
        running.len(),
        1,
        "Running index entry should be added after dequeue"
    );
    assert_eq!(running[0].job_id, "j1");
}

#[silo::test]
async fn index_cancel_scheduled_removes_scheduled_adds_cancelled() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "j1", 10, now + 60_000).await;

    // Before cancel: 1 Scheduled entry
    let scheduled = collect_index_entries(&shard, "-", "Scheduled").await;
    assert_eq!(scheduled.len(), 1);

    shard.cancel_job("-", "j1").await.expect("cancel");

    // After cancel: Scheduled entry removed, Cancelled entry added
    let scheduled = collect_index_entries(&shard, "-", "Scheduled").await;
    assert_eq!(
        scheduled.len(),
        0,
        "Scheduled index entry should be removed after cancel"
    );

    let cancelled = collect_index_entries(&shard, "-", "Cancelled").await;
    assert_eq!(
        cancelled.len(),
        1,
        "Cancelled index entry should be added after cancel"
    );
    assert_eq!(cancelled[0].job_id, "j1");
}

#[silo::test]
async fn index_cancel_waiting_removes_scheduled_adds_cancelled() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue with past start time (Waiting = Scheduled + start_time <= now)
    enqueue_job(&shard, "j1", 10, now - 5000).await;

    let scheduled = collect_index_entries(&shard, "-", "Scheduled").await;
    assert_eq!(
        scheduled.len(),
        1,
        "waiting job has a Scheduled index entry"
    );

    shard.cancel_job("-", "j1").await.expect("cancel");

    let scheduled = collect_index_entries(&shard, "-", "Scheduled").await;
    assert_eq!(
        scheduled.len(),
        0,
        "Scheduled index entry should be removed after cancelling a waiting job"
    );

    let cancelled = collect_index_entries(&shard, "-", "Cancelled").await;
    assert_eq!(cancelled.len(), 1);
    assert_eq!(cancelled[0].job_id, "j1");
}

#[silo::test]
async fn index_expedite_replaces_scheduled_entry() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();
    let future_start = now + 60_000;

    enqueue_job(&shard, "j1", 10, future_start).await;

    // Before expedite: 1 Scheduled entry keyed at future_start
    let entries = collect_index_entries(&shard, "-", "Scheduled").await;
    assert_eq!(entries.len(), 1);
    let old_inverted = entries[0].inverted_timestamp;
    assert_eq!(old_inverted, u64::MAX - future_start as u64);

    shard.expedite_job("-", "j1").await.expect("expedite");

    // After expedite: still 1 Scheduled entry, but now keyed at ~now
    let entries = collect_index_entries(&shard, "-", "Scheduled").await;
    assert_eq!(
        entries.len(),
        1,
        "should have exactly 1 Scheduled index entry after expedite (old one removed, new one added)"
    );
    assert_eq!(entries[0].job_id, "j1");
    // The new entry should have a larger inverted timestamp (closer to now = larger inverted value)
    assert!(
        entries[0].inverted_timestamp > old_inverted,
        "expedited entry should have a larger inverted timestamp (earlier real time)"
    );
}

#[silo::test]
async fn index_no_orphaned_entries_through_expedite() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "j1", 10, now + 60_000).await;

    // Total index entries should be exactly 1
    let all = collect_all_index_entries(&shard).await;
    assert_eq!(all.len(), 1, "1 entry after enqueue");

    shard.expedite_job("-", "j1").await.expect("expedite");

    // Still exactly 1 total index entry (old deleted, new added)
    let all = collect_all_index_entries(&shard).await;
    assert_eq!(
        all.len(),
        1,
        "should still be exactly 1 index entry after expedite, no orphans"
    );
}

#[silo::test]
async fn index_restart_replaces_terminal_with_scheduled() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "j1", 10, now - 1000).await;

    // Dequeue to get Running
    let result = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue");
    let task_id = result.tasks[0].attempt().task_id().to_string();

    // Fail the job
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Error {
                error_code: "ERR".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report error");

    // Job should now be Failed (no retries configured)
    let failed = collect_index_entries(&shard, "-", "Failed").await;
    assert_eq!(failed.len(), 1, "should have 1 Failed entry");
    assert_eq!(failed[0].job_id, "j1");

    // No Running or Scheduled entries should remain
    let running = collect_index_entries(&shard, "-", "Running").await;
    assert_eq!(running.len(), 0);
    let scheduled = collect_index_entries(&shard, "-", "Scheduled").await;
    assert_eq!(scheduled.len(), 0);

    // Restart the job
    shard.restart_job("-", "j1").await.expect("restart");

    // Failed entry should be gone, new Scheduled entry should exist
    let failed = collect_index_entries(&shard, "-", "Failed").await;
    assert_eq!(
        failed.len(),
        0,
        "Failed index entry should be removed after restart"
    );

    let scheduled = collect_index_entries(&shard, "-", "Scheduled").await;
    assert_eq!(
        scheduled.len(),
        1,
        "Scheduled index entry should be added after restart"
    );
    assert_eq!(scheduled[0].job_id, "j1");
}

#[silo::test]
async fn index_total_count_stable_through_lifecycle() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue 2 jobs
    enqueue_job(&shard, "j1", 10, now - 1000).await;
    enqueue_job(&shard, "j2", 10, now + 60_000).await;

    let all = collect_all_index_entries(&shard).await;
    assert_eq!(all.len(), 2, "2 entries after enqueue");

    // Dequeue j1 (Scheduled -> Running)
    let result = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue");
    assert_eq!(result.tasks.len(), 1);

    let all = collect_all_index_entries(&shard).await;
    assert_eq!(
        all.len(),
        2,
        "still 2 entries after dequeue (1 Running + 1 Scheduled)"
    );

    // Expedite j2 (Scheduled -> Scheduled at new time)
    shard.expedite_job("-", "j2").await.expect("expedite");

    let all = collect_all_index_entries(&shard).await;
    assert_eq!(all.len(), 2, "still 2 entries after expedite");

    // Cancel j2 (Scheduled -> Cancelled)
    shard.cancel_job("-", "j2").await.expect("cancel");

    let all = collect_all_index_entries(&shard).await;
    assert_eq!(
        all.len(),
        2,
        "still 2 entries after cancel (1 Running + 1 Cancelled)"
    );

    // Complete j1 successfully
    let task_id = result.tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report success");

    let all = collect_all_index_entries(&shard).await;
    assert_eq!(
        all.len(),
        2,
        "still 2 entries after success (1 Succeeded + 1 Cancelled)"
    );

    // Verify final state
    let succeeded = collect_index_entries(&shard, "-", "Succeeded").await;
    assert_eq!(succeeded.len(), 1);
    assert_eq!(succeeded[0].job_id, "j1");

    let cancelled = collect_index_entries(&shard, "-", "Cancelled").await;
    assert_eq!(cancelled.len(), 1);
    assert_eq!(cancelled[0].job_id, "j2");
}

#[silo::test]
async fn index_multiple_jobs_independent() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue 3 jobs with different start times
    enqueue_job(&shard, "past", 10, now - 5000).await;
    enqueue_job(&shard, "present", 10, now).await;
    enqueue_job(&shard, "future", 10, now + 60_000).await;

    let entries = collect_index_entries(&shard, "-", "Scheduled").await;
    assert_eq!(entries.len(), 3, "all 3 jobs should have Scheduled entries");

    // Cancel only the past job
    shard.cancel_job("-", "past").await.expect("cancel");

    let scheduled = collect_index_entries(&shard, "-", "Scheduled").await;
    assert_eq!(
        scheduled.len(),
        2,
        "only 2 Scheduled entries after cancelling 1"
    );
    let ids: Vec<&str> = scheduled.iter().map(|e| e.job_id.as_str()).collect();
    // Index is sorted by inverted timestamp, so future (smallest inverted) comes first
    assert!(ids.contains(&"present"), "present should still be in index");
    assert!(ids.contains(&"future"), "future should still be in index");
    assert!(
        !ids.contains(&"past"),
        "past should be removed from Scheduled index"
    );

    let cancelled = collect_index_entries(&shard, "-", "Cancelled").await;
    assert_eq!(cancelled.len(), 1);
    assert_eq!(cancelled[0].job_id, "past");

    // Total entries should be exactly 3
    let all = collect_all_index_entries(&shard).await;
    assert_eq!(all.len(), 3);
}

#[silo::test]
async fn sql_current_attempt_differentiates_new_vs_retried() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    // Enqueue a fresh job with future start time (0 attempts, future-scheduled)
    enqueue_job(&shard, "fresh", 10, now + 60_000).await;

    // Enqueue a job with a retry policy, then fail it so it gets rescheduled
    let retry_policy = RetryPolicy {
        retry_count: 3,
        initial_interval_ms: 60_000, // long backoff so it stays Scheduled
        max_interval_ms: 120_000,
        randomize_interval: false,
        backoff_factor: 1.0,
    };
    shard
        .enqueue(
            "-",
            Some("retried".to_string()),
            10,
            now,
            Some(retry_policy),
            test_helpers::msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            "default",
        )
        .await
        .expect("enqueue");

    // Dequeue and fail the retried job so it gets rescheduled with attempt 1
    let tasks = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &task_id,
            AttemptOutcome::Error {
                error_code: "ERR".to_string(),
                error: vec![],
            },
        )
        .await
        .expect("report error");

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");

    // Query both scheduled jobs
    let batches = sql
        .sql("SELECT id, status_kind, current_attempt, next_attempt_starts_after_ms FROM jobs WHERE tenant = '-' AND status_kind = 'Scheduled' ORDER BY id")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    let ids = extract_string_column(&batches, 0);
    assert_eq!(ids, vec!["fresh", "retried"]);

    let attempt_col = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("current_attempt column");

    // Fresh job: attempt 1 (1-indexed, first attempt)
    assert_eq!(attempt_col.value(0), 1, "fresh job should have attempt 1");
    // Retried job: attempt 2 (failed once, now on second attempt)
    assert_eq!(attempt_col.value(1), 2, "retried job should have attempt 2");

    // Both should have next_attempt_starts_after_ms set
    let next_col = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("next_attempt_starts_after_ms column");
    assert!(
        !next_col.is_null(0),
        "fresh job should have next_attempt_starts_after_ms"
    );
    assert!(
        !next_col.is_null(1),
        "retried job should have next_attempt_starts_after_ms"
    );

    // The retried job's next attempt should be in the future (backoff from failure)
    assert!(
        next_col.value(1) > now,
        "retried job next_attempt_starts_after_ms should be in the future"
    );
}

#[silo::test]
async fn sql_current_attempt_null_for_running_and_terminal() {
    let (_tmp, shard) = open_temp_shard().await;
    let now = now_ms();

    enqueue_job(&shard, "j1", 5, now).await;
    enqueue_job(&shard, "j2", 10, now).await;

    // Dequeue j1 to make it Running
    let tasks = shard
        .dequeue("worker", "default", 1)
        .await
        .expect("dequeue")
        .tasks;
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].attempt().task_id().to_string();

    // Complete j1 to make it Succeeded
    shard
        .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report success");

    let sql = ShardQueryEngine::new(Arc::clone(&shard), "jobs").expect("new ShardQueryEngine");

    // Succeeded job: current_attempt and next_attempt_starts_after_ms should be null
    let batches = sql
        .sql("SELECT id, current_attempt, next_attempt_starts_after_ms FROM jobs WHERE tenant = '-' AND id = 'j1'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    let attempt_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("current_attempt column");
    assert!(
        attempt_col.is_null(0),
        "current_attempt should be null for terminal jobs"
    );

    let next_col = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("next_attempt_starts_after_ms column");
    assert!(
        next_col.is_null(0),
        "next_attempt_starts_after_ms should be null for terminal jobs"
    );

    // Waiting job: current_attempt should be 1 (1-indexed, first attempt)
    let batches = sql
        .sql("SELECT id, current_attempt FROM jobs WHERE tenant = '-' AND id = 'j2'")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");

    let attempt_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("current_attempt column");
    assert_eq!(attempt_col.value(0), 1);
}
