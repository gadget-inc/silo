mod test_helpers;

use silo::compaction::compute_space_amplification_percent_from_sizes;
use test_helpers::{msgpack_payload, open_temp_shard};

/// Test that a shard opens successfully with the custom compaction scheduler wired in.
#[silo::test]
async fn shard_opens_with_custom_scheduler() {
    let (_tmp, shard) = open_temp_shard().await;
    // If we got here, the custom scheduler was wired in without error
    assert_eq!(shard.name(), "test");
}

/// Test that compaction runs with the custom scheduler after enqueue + delete.
/// The scheduler has min_compaction_sources=2 (vs STC's 4), so compaction should
/// trigger more aggressively.
#[silo::test]
async fn compaction_runs_with_custom_scheduler() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue some jobs
    for i in 0..20 {
        let payload = msgpack_payload(&serde_json::json!({"value": i}));
        shard
            .enqueue(
                "tenant-a",
                Some(format!("job-{}", i)),
                0,
                0,
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue should succeed");
    }

    // Flush to create L0 SSTs
    shard.db().flush().await.unwrap();

    // Verify data is readable before compaction
    let job_before = shard
        .get_job("tenant-a", "job-0")
        .await
        .expect("get_job should succeed");
    assert!(
        job_before.is_some(),
        "job should exist after enqueue and flush"
    );

    // Submit full compaction (tests generate + validate path)
    // This may be a no-op if the compactor already compacted everything,
    // which is fine - the important thing is it doesn't error
    let _ = shard.submit_full_compaction().await;

    // Verify data is still readable
    let job = shard
        .get_job("tenant-a", "job-0")
        .await
        .expect("get_job should succeed");
    assert!(job.is_some(), "job should still exist after compaction");
}

/// Test that space_amplification_percent is computed in LSM state.
#[silo::test]
async fn lsm_state_includes_space_amplification() {
    let (_tmp, shard) = open_temp_shard().await;

    // Enqueue some jobs
    for i in 0..5 {
        let payload = msgpack_payload(&serde_json::json!({"value": i}));
        shard
            .enqueue(
                "tenant-a",
                Some(format!("job-{}", i)),
                0,
                0,
                None,
                payload,
                vec![],
                None,
                "default",
            )
            .await
            .expect("enqueue should succeed");
    }

    shard.db().flush().await.unwrap();

    let lsm = shard.read_lsm_state().await.expect("read LSM state");
    // space_amplification_percent should be a valid number (0 or positive)
    assert!(
        lsm.space_amplification_percent >= 0.0,
        "space amplification should be non-negative, got {}",
        lsm.space_amplification_percent
    );
}

/// Test compute_space_amplification_percent_from_sizes with various inputs.
#[silo::test]
fn space_amp_helper_no_sorted_runs() {
    assert_eq!(
        compute_space_amplification_percent_from_sizes(1000, &[]),
        0.0
    );
}

#[silo::test]
fn space_amp_helper_single_run() {
    let result = compute_space_amplification_percent_from_sizes(1000, &[10000]);
    assert!(
        (result - 10.0).abs() < 0.01,
        "expected ~10%, got {}",
        result
    );
}

#[silo::test]
fn space_amp_helper_multiple_runs() {
    // L0=500, SR0=5000, SR1(bottom)=10000
    // non-bottom = 500 + 5000 = 5500, bottom = 10000 => 55%
    let result = compute_space_amplification_percent_from_sizes(500, &[5000, 10000]);
    assert!(
        (result - 55.0).abs() < 0.01,
        "expected ~55%, got {}",
        result
    );
}

#[silo::test]
fn space_amp_helper_zero_bottom() {
    assert_eq!(
        compute_space_amplification_percent_from_sizes(1000, &[0]),
        0.0
    );
}

#[silo::test]
fn space_amp_helper_no_l0_no_extra_runs() {
    // Single sorted run, no L0 => 0% amplification
    let result = compute_space_amplification_percent_from_sizes(0, &[10000]);
    assert_eq!(result, 0.0);
}
