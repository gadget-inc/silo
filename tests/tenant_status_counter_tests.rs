mod test_helpers;

use silo::job::JobStatusKind;
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::import::{ImportJobParams, ImportedAttempt, ImportedAttemptStatus};
use silo::retry::RetryPolicy;
use silo::shard_range::ShardRange;

use test_helpers::*;

/// Helper to get tenant status counters as a map of status_kind -> count
async fn get_status_counts(
    shard: &std::sync::Arc<silo::job_store_shard::JobStoreShard>,
    tenant: &str,
) -> std::collections::HashMap<String, i64> {
    let entries = shard
        .scan_tenant_status_counters()
        .await
        .expect("scan_tenant_status_counters");
    entries
        .into_iter()
        .filter(|(t, _, _)| t == tenant)
        .map(|(_, status, count)| (status, count))
        .collect()
}

#[silo::test]
async fn counter_increments_on_enqueue() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue 3 jobs
    for i in 0..3 {
        shard
            .enqueue(
                "t1",
                None,
                10u8,
                now_ms(),
                None,
                msgpack_payload(&serde_json::json!({"i": i})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");
    }

    let counts = get_status_counts(&shard, "t1").await;
    assert_eq!(counts.get("Scheduled").copied().unwrap_or(0), 3);
}

#[silo::test]
async fn counter_transitions_through_lifecycle() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        // Enqueue
        let job_id = shard
            .enqueue(
                "t1",
                None,
                10u8,
                now_ms(),
                None,
                msgpack_payload(&serde_json::json!({"test": "lifecycle"})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        let counts = get_status_counts(&shard, "t1").await;
        assert_eq!(counts.get("Scheduled").copied().unwrap_or(0), 1);

        // Dequeue → Running
        let tasks = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);

        let counts = get_status_counts(&shard, "t1").await;
        assert_eq!(
            counts.get("Scheduled").copied().unwrap_or(0),
            0,
            "Scheduled should be 0 after dequeue"
        );
        assert_eq!(
            counts.get("Running").copied().unwrap_or(0),
            1,
            "Running should be 1 after dequeue"
        );

        // Report success → Succeeded
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report success");

        let counts = get_status_counts(&shard, "t1").await;
        assert_eq!(counts.get("Running").copied().unwrap_or(0), 0);
        assert_eq!(counts.get("Succeeded").copied().unwrap_or(0), 1);

        // Verify job status
        let status = shard
            .get_job_status("t1", &job_id)
            .await
            .expect("get status")
            .expect("exists");
        assert_eq!(status.kind, JobStatusKind::Succeeded);
    });
}

#[silo::test]
async fn counter_handles_cancel() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let job_id = shard
            .enqueue(
                "t1",
                None,
                10u8,
                now_ms(),
                None,
                msgpack_payload(&serde_json::json!({"test": "cancel"})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        let counts = get_status_counts(&shard, "t1").await;
        assert_eq!(counts.get("Scheduled").copied().unwrap_or(0), 1);

        // Cancel
        shard.cancel_job("t1", &job_id).await.expect("cancel");

        let counts = get_status_counts(&shard, "t1").await;
        assert_eq!(
            counts.get("Scheduled").copied().unwrap_or(0),
            0,
            "Scheduled should be 0 after cancel"
        );
        assert_eq!(
            counts.get("Cancelled").copied().unwrap_or(0),
            1,
            "Cancelled should be 1 after cancel"
        );
    });
}

#[silo::test]
async fn counter_handles_restart() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let job_id = shard
            .enqueue(
                "t1",
                None,
                10u8,
                now_ms(),
                None,
                msgpack_payload(&serde_json::json!({"test": "restart"})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Cancel the job
        shard.cancel_job("t1", &job_id).await.expect("cancel");

        let counts = get_status_counts(&shard, "t1").await;
        assert_eq!(counts.get("Cancelled").copied().unwrap_or(0), 1);

        // Restart
        shard.restart_job("t1", &job_id).await.expect("restart");

        let counts = get_status_counts(&shard, "t1").await;
        assert_eq!(
            counts.get("Cancelled").copied().unwrap_or(0),
            0,
            "Cancelled should be 0 after restart"
        );
        assert_eq!(
            counts.get("Scheduled").copied().unwrap_or(0),
            1,
            "Scheduled should be 1 after restart"
        );
    });
}

#[silo::test]
async fn counter_handles_retry() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        // Enqueue with retry policy
        shard
            .enqueue(
                "t1",
                None,
                10u8,
                now_ms(),
                Some(RetryPolicy {
                    retry_count: 2,
                    initial_interval_ms: 0,
                    max_interval_ms: 0,
                    randomize_interval: false,
                    backoff_factor: 1.0,
                }),
                msgpack_payload(&serde_json::json!({"test": "retry"})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Dequeue
        let tasks = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);

        let counts = get_status_counts(&shard, "t1").await;
        assert_eq!(counts.get("Running").copied().unwrap_or(0), 1);

        // Report error (will retry)
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(
                &task_id,
                AttemptOutcome::Error {
                    error_code: "TEST".to_string(),
                    error: vec![],
                },
            )
            .await
            .expect("report error");

        let counts = get_status_counts(&shard, "t1").await;
        assert_eq!(
            counts.get("Running").copied().unwrap_or(0),
            0,
            "Running should be 0 after retry"
        );
        assert_eq!(
            counts.get("Scheduled").copied().unwrap_or(0),
            1,
            "Scheduled should be 1 after retry"
        );
    });
}

#[silo::test]
async fn counter_handles_delete() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;

        let job_id = shard
            .enqueue(
                "t1",
                None,
                10u8,
                now_ms(),
                None,
                msgpack_payload(&serde_json::json!({"test": "delete"})),
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue");

        // Dequeue + succeed to make it terminal
        let tasks = shard
            .dequeue("w", "default", 1)
            .await
            .expect("dequeue")
            .tasks;
        let task_id = tasks[0].attempt().task_id().to_string();
        shard
            .report_attempt_outcome(&task_id, AttemptOutcome::Success { result: vec![] })
            .await
            .expect("report success");

        let counts = get_status_counts(&shard, "t1").await;
        assert_eq!(counts.get("Succeeded").copied().unwrap_or(0), 1);

        // Delete
        shard.delete_job("t1", &job_id).await.expect("delete");

        let counts = get_status_counts(&shard, "t1").await;
        assert_eq!(
            counts.get("Succeeded").copied().unwrap_or(0),
            0,
            "Succeeded should be 0 after delete"
        );
    });
}

#[silo::test]
async fn counter_handles_import() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let now = now_ms();

        let results = shard
            .import_jobs(
                "t1",
                vec![
                    ImportJobParams {
                        id: "imp-1".to_string(),
                        priority: 10,
                        enqueue_time_ms: now,
                        start_at_ms: now,
                        retry_policy: None,
                        payload: msgpack_payload(&serde_json::json!({"test": "import1"})),
                        limits: vec![],
                        metadata: None,
                        task_group: "default".to_string(),
                        attempts: vec![ImportedAttempt {
                            status: ImportedAttemptStatus::Succeeded { result: vec![] },
                            started_at_ms: now - 100,
                            finished_at_ms: now,
                        }],
                    },
                    ImportJobParams {
                        id: "imp-2".to_string(),
                        priority: 10,
                        enqueue_time_ms: now,
                        start_at_ms: now,
                        retry_policy: None,
                        payload: msgpack_payload(&serde_json::json!({"test": "import2"})),
                        limits: vec![],
                        metadata: None,
                        task_group: "default".to_string(),
                        attempts: vec![ImportedAttempt {
                            status: ImportedAttemptStatus::Failed {
                                error_code: "ERR".to_string(),
                                error: vec![],
                            },
                            started_at_ms: now - 100,
                            finished_at_ms: now,
                        }],
                    },
                ],
            )
            .await
            .expect("import");

        assert!(results.iter().all(|r| r.success));

        let counts = get_status_counts(&shard, "t1").await;
        assert_eq!(counts.get("Succeeded").copied().unwrap_or(0), 1);
        assert_eq!(counts.get("Failed").copied().unwrap_or(0), 1);
    });
}

#[silo::test]
async fn counter_handles_reimport() {
    with_timeout!(20000, {
        let (_tmp, shard) = open_temp_shard().await;
        let now = now_ms();

        // Import as Failed first (Succeeded cannot be reimported)
        let results = shard
            .import_jobs(
                "t1",
                vec![ImportJobParams {
                    id: "reimp-1".to_string(),
                    priority: 10,
                    enqueue_time_ms: now,
                    start_at_ms: now,
                    retry_policy: None,
                    payload: msgpack_payload(&serde_json::json!({"test": "reimport"})),
                    limits: vec![],
                    metadata: None,
                    task_group: "default".to_string(),
                    attempts: vec![ImportedAttempt {
                        status: ImportedAttemptStatus::Failed {
                            error_code: "ERR".to_string(),
                            error: vec![],
                        },
                        started_at_ms: now - 100,
                        finished_at_ms: now,
                    }],
                }],
            )
            .await
            .expect("import");
        assert!(results[0].success);

        let counts = get_status_counts(&shard, "t1").await;
        assert_eq!(counts.get("Failed").copied().unwrap_or(0), 1);

        // Reimport as Succeeded (should transition from Failed to Succeeded)
        // The first attempt must exactly match the existing one
        let results = shard
            .import_jobs(
                "t1",
                vec![ImportJobParams {
                    id: "reimp-1".to_string(),
                    priority: 10,
                    enqueue_time_ms: now,
                    start_at_ms: now,
                    retry_policy: None,
                    payload: msgpack_payload(&serde_json::json!({"test": "reimport"})),
                    limits: vec![],
                    metadata: None,
                    task_group: "default".to_string(),
                    attempts: vec![
                        // Must match existing attempt exactly
                        ImportedAttempt {
                            status: ImportedAttemptStatus::Failed {
                                error_code: "ERR".to_string(),
                                error: vec![],
                            },
                            started_at_ms: now - 100,
                            finished_at_ms: now,
                        },
                        // New attempt
                        ImportedAttempt {
                            status: ImportedAttemptStatus::Succeeded { result: vec![] },
                            started_at_ms: now + 1,
                            finished_at_ms: now + 2,
                        },
                    ],
                }],
            )
            .await
            .expect("reimport");
        assert!(results[0].success);

        let counts = get_status_counts(&shard, "t1").await;
        assert_eq!(
            counts.get("Failed").copied().unwrap_or(0),
            0,
            "Failed should be 0 after reimport to Succeeded"
        );
        assert_eq!(
            counts.get("Succeeded").copied().unwrap_or(0),
            1,
            "Succeeded should be 1 after reimport"
        );
    });
}

#[silo::test]
async fn counter_scan_filters_by_shard_range() {
    with_timeout!(20000, {
        // Open shard with full range
        let (_tmp, shard) = open_temp_shard().await;

        // Enqueue jobs for different tenants
        for tenant in &["alpha", "bravo"] {
            shard
                .enqueue(
                    tenant,
                    None,
                    10u8,
                    now_ms(),
                    None,
                    msgpack_payload(&serde_json::json!({"tenant": tenant})),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue");
        }

        // With full range, both tenants should be visible
        let entries = shard.scan_tenant_status_counters().await.expect("scan");
        assert_eq!(entries.len(), 2, "full range should see both tenants");

        // Create a range that includes "alpha" but not "bravo" by hashing
        let alpha_hash = silo::shard_range::hash_tenant("alpha");
        let bravo_hash = silo::shard_range::hash_tenant("bravo");

        // Pick the smaller hash as the one we want in range, exclude the other
        let (included_tenant, _excluded_tenant, range) = if alpha_hash < bravo_hash {
            // alpha < bravo: use range [alpha_hash, bravo_hash) which includes alpha
            (
                "alpha",
                "bravo",
                ShardRange::new(alpha_hash.clone(), bravo_hash.clone()),
            )
        } else {
            // bravo < alpha: use range [bravo_hash, alpha_hash) which includes bravo
            (
                "bravo",
                "alpha",
                ShardRange::new(bravo_hash.clone(), alpha_hash.clone()),
            )
        };

        let (_tmp2, restricted_shard) = open_temp_shard_with_range(range).await;

        // Enqueue into restricted shard
        for tenant in &["alpha", "bravo"] {
            restricted_shard
                .enqueue(
                    tenant,
                    None,
                    10u8,
                    now_ms(),
                    None,
                    msgpack_payload(&serde_json::json!({"tenant": tenant})),
                    vec![],
                    None,
                    "default",
                )
                .await
                .expect("enqueue");
        }

        // Only the in-range tenant should appear in the scan
        let entries = restricted_shard
            .scan_tenant_status_counters()
            .await
            .expect("scan restricted");
        assert!(
            entries.iter().all(|(t, _, _)| t == included_tenant),
            "only {} should be in range, got: {:?}",
            included_tenant,
            entries
        );
        assert_eq!(
            entries.len(),
            1,
            "restricted range should only see one tenant"
        );
    });
}
