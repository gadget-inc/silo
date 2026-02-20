use silo::keys::{
    attempt_key, attempt_prefix, concurrency_holder_key, concurrency_request_job_prefix,
    concurrency_request_key, end_bound, floating_limit_state_key, idx_metadata_key,
    idx_status_time_key, idx_status_time_prefix, job_cancelled_key, job_info_key, job_info_prefix,
    job_status_key, leased_task_key, parse_attempt_key, parse_concurrency_holder_key,
    parse_concurrency_request_key, parse_floating_limit_key, parse_job_cancelled_key,
    parse_job_info_key, parse_job_status_key, parse_lease_key, parse_metadata_index_key,
    parse_status_time_index_key, parse_task_key, task_group_prefix, task_key,
};

#[test]
fn test_job_info_key_roundtrip() {
    let key = job_info_key("tenant1", "job123");
    let parsed = parse_job_info_key(&key).unwrap();
    assert_eq!(parsed.tenant, "tenant1");
    assert_eq!(parsed.job_id, "job123");
}

#[test]
fn test_task_key_roundtrip() {
    let key = task_key("group1", 1000, 10, "job123", 1);
    let parsed = parse_task_key(&key).unwrap();
    assert_eq!(parsed.task_group, "group1");
    assert_eq!(parsed.start_time_ms, 1000);
    assert_eq!(parsed.priority, 10);
    assert_eq!(parsed.job_id, "job123");
    assert_eq!(parsed.attempt, 1);
}

#[test]
fn test_task_key_ordering() {
    // Tasks should sort by task_group, then time, then priority
    let key1 = task_key("group1", 1000, 10, "job1", 1);
    let key2 = task_key("group1", 1001, 10, "job2", 1);
    let key3 = task_key("group1", 1000, 20, "job3", 1);
    let key4 = task_key("group2", 500, 5, "job4", 1);

    // group1 < group2
    assert!(key1 < key4);
    // Same group, earlier time comes first
    assert!(key1 < key2);
    // Same group and time, lower priority comes first
    assert!(key1 < key3);
}

#[test]
fn test_status_time_index_ordering() {
    // Newer timestamps should sort FIRST (inverted)
    let key1 = idx_status_time_key("t", "Running", 1000, "job1");
    let key2 = idx_status_time_key("t", "Running", 2000, "job2");

    // key2 has higher timestamp, but should sort BEFORE key1 (newest first)
    assert!(key2 < key1);
}

#[test]
fn test_concurrency_request_key_roundtrip() {
    let key = concurrency_request_key("tenant1", "queue1", 5000, 25, "job42", 3, "a1b2c3d4");
    let parsed = parse_concurrency_request_key(&key).unwrap();
    assert_eq!(parsed.tenant, "tenant1");
    assert_eq!(parsed.queue, "queue1");
    assert_eq!(parsed.start_time_ms, 5000);
    assert_eq!(parsed.priority, 25);
    assert_eq!(parsed.job_id, "job42");
    assert_eq!(parsed.attempt_number, 3);
    assert_eq!(parsed.suffix, "a1b2c3d4");
    assert_eq!(parsed.request_id(), "job42:3:a1b2c3d4");
}

#[test]
fn test_concurrency_holder_key_roundtrip() {
    let key = concurrency_holder_key("tenant1", "queue1", "task123");
    let parsed = parse_concurrency_holder_key(&key).unwrap();
    assert_eq!(parsed.tenant, "tenant1");
    assert_eq!(parsed.queue, "queue1");
    assert_eq!(parsed.task_id, "task123");
}

#[test]
fn test_prefix_scanning() {
    // Jobs with same tenant should share a prefix
    let key1 = job_info_key("tenant1", "job1");
    let key2 = job_info_key("tenant1", "job2");
    let prefix = job_info_prefix("tenant1");

    assert!(key1.starts_with(&prefix));
    assert!(key2.starts_with(&prefix));

    // Different tenant should NOT match
    let key3 = job_info_key("tenant2", "job1");
    assert!(!key3.starts_with(&prefix));
}

#[test]
fn test_task_group_prefix_scanning() {
    let key1 = task_key("mygroup", 1000, 10, "job1", 1);
    let key2 = task_key("mygroup", 2000, 20, "job2", 2);
    let prefix = task_group_prefix("mygroup");

    assert!(key1.starts_with(&prefix));
    assert!(key2.starts_with(&prefix));

    let key3 = task_key("othergroup", 1000, 10, "job3", 1);
    assert!(!key3.starts_with(&prefix));
}

#[test]
fn test_special_characters_in_tenant() {
    // Tenants with special characters should work
    let key = job_info_key("tenant/with/slashes", "job123");
    let parsed = parse_job_info_key(&key).unwrap();
    assert_eq!(parsed.tenant, "tenant/with/slashes");
    assert_eq!(parsed.job_id, "job123");
}

#[test]
fn test_metadata_index_roundtrip() {
    let key = idx_metadata_key("tenant1", "env", "production", "job123");
    let parsed = parse_metadata_index_key(&key).unwrap();
    assert_eq!(parsed.tenant, "tenant1");
    assert_eq!(parsed.key, "env");
    assert_eq!(parsed.value, "production");
    assert_eq!(parsed.job_id, "job123");
}

#[test]
fn test_status_time_index_roundtrip() {
    let key = idx_status_time_key("tenant1", "Running", 1234567890, "job123");
    let parsed = parse_status_time_index_key(&key).unwrap();
    assert_eq!(parsed.tenant, "tenant1");
    assert_eq!(parsed.status, "Running");
    assert_eq!(parsed.changed_at_ms(), 1234567890);
    assert_eq!(parsed.job_id, "job123");
}

#[test]
fn test_status_time_prefix_scanning() {
    let key1 = idx_status_time_key("tenant1", "Running", 1000, "job1");
    let key2 = idx_status_time_key("tenant1", "Running", 2000, "job2");
    let prefix = idx_status_time_prefix("tenant1", "Running");

    assert!(
        key1.starts_with(&prefix),
        "key1 should start with prefix\nkey1: {:?}\nprefix: {:?}",
        key1,
        prefix
    );
    assert!(
        key2.starts_with(&prefix),
        "key2 should start with prefix\nkey2: {:?}\nprefix: {:?}",
        key2,
        prefix
    );

    // Different status should NOT match
    let key3 = idx_status_time_key("tenant1", "Scheduled", 1000, "job3");
    assert!(
        !key3.starts_with(&prefix),
        "key3 (Scheduled) should NOT start with Running prefix"
    );
}

#[test]
fn test_status_time_prefix_scanning_hyphen_tenant() {
    // Test the exact parameters from the cluster_query_multi_shard_filter_pushdown test
    // which uses tenant "-" and status "Scheduled"
    let key1 = idx_status_time_key("-", "Scheduled", 1000, "s1_scheduled");
    let key2 = idx_status_time_key("-", "Scheduled", 2000, "another_job");
    let prefix = idx_status_time_prefix("-", "Scheduled");

    assert!(
        key1.starts_with(&prefix),
        "key1 should start with prefix\nkey1: {:?}\nprefix: {:?}",
        key1,
        prefix
    );
    assert!(
        key2.starts_with(&prefix),
        "key2 should start with prefix\nkey2: {:?}\nprefix: {:?}",
        key2,
        prefix
    );

    // Different tenant should NOT match
    let key3 = idx_status_time_key("other", "Scheduled", 1000, "job3");
    assert!(
        !key3.starts_with(&prefix),
        "key3 (other tenant) should NOT start with '-' prefix"
    );

    // Different status should NOT match
    let key4 = idx_status_time_key("-", "Running", 1000, "job4");
    assert!(
        !key4.starts_with(&prefix),
        "key4 (Running) should NOT start with Scheduled prefix"
    );
}

#[test]
fn test_job_status_key_roundtrip() {
    let key = job_status_key("tenant1", "job123");
    let parsed = parse_job_status_key(&key).unwrap();
    assert_eq!(parsed.tenant, "tenant1");
    assert_eq!(parsed.job_id, "job123");
}

#[test]
fn test_job_status_key_different_prefix_than_job_info() {
    // Ensure job_status and job_info keys have different prefixes
    let info_key = job_info_key("tenant1", "job123");
    let status_key = job_status_key("tenant1", "job123");

    // They should have different first bytes (prefix)
    assert_ne!(info_key[0], status_key[0]);

    // parse_job_info_key should NOT parse a job_status_key
    assert!(parse_job_info_key(&status_key).is_none());

    // parse_job_status_key should NOT parse a job_info_key
    assert!(parse_job_status_key(&info_key).is_none());
}

#[test]
fn test_attempt_key_roundtrip() {
    let key = attempt_key("tenant1", "job123", 3);
    let parsed = parse_attempt_key(&key).unwrap();
    assert_eq!(parsed.tenant, "tenant1");
    assert_eq!(parsed.job_id, "job123");
    assert_eq!(parsed.attempt, 3);
}

#[test]
fn test_attempt_key_ordering() {
    // Attempts for the same job should sort by attempt number
    let key1 = attempt_key("tenant1", "job1", 1);
    let key2 = attempt_key("tenant1", "job1", 2);
    let key3 = attempt_key("tenant1", "job1", 10);

    assert!(key1 < key2);
    assert!(key2 < key3);
}

#[test]
fn test_attempt_prefix_scanning() {
    let key1 = attempt_key("tenant1", "job1", 1);
    let key2 = attempt_key("tenant1", "job1", 2);
    let prefix = attempt_prefix("tenant1", "job1");

    assert!(key1.starts_with(&prefix));
    assert!(key2.starts_with(&prefix));

    // Different job should NOT match
    let key3 = attempt_key("tenant1", "job2", 1);
    assert!(!key3.starts_with(&prefix));
}

#[test]
fn test_lease_key_roundtrip() {
    let key = leased_task_key("task-uuid-12345");
    let parsed = parse_lease_key(&key).unwrap();
    assert_eq!(parsed.task_id, "task-uuid-12345");
}

#[test]
fn test_job_cancelled_key_roundtrip() {
    let key = job_cancelled_key("tenant1", "job123");
    let parsed = parse_job_cancelled_key(&key).unwrap();
    assert_eq!(parsed.tenant, "tenant1");
    assert_eq!(parsed.job_id, "job123");
}

#[test]
fn test_floating_limit_key_roundtrip() {
    let key = floating_limit_state_key("tenant1", "queue:myqueue");
    let parsed = parse_floating_limit_key(&key).unwrap();
    assert_eq!(parsed.tenant, "tenant1");
    assert_eq!(parsed.queue_key, "queue:myqueue");
}

#[test]
fn test_end_bound_function() {
    // Test basic case: incrementing last byte
    let prefix = vec![0x01, 0x02, 0x03];
    let end = end_bound(&prefix);
    assert_eq!(end, vec![0x01, 0x02, 0x04]); // Incremented last byte

    // Test with 0xFF at end: should carry over
    let prefix2 = vec![0x01, 0x02, 0xFF];
    let end2 = end_bound(&prefix2);
    assert_eq!(end2, vec![0x01, 0x03]); // Carried over

    // Test with multiple 0xFFs: should carry over multiple times
    let prefix3 = vec![0x01, 0xFF, 0xFF];
    let end3 = end_bound(&prefix3);
    assert_eq!(end3, vec![0x02]); // Carried over twice

    // Test all 0xFFs: fallback to appending 0xFF
    let prefix4 = vec![0xFF, 0xFF, 0xFF];
    let end4 = end_bound(&prefix4);
    assert_eq!(end4, vec![0xFF, 0xFF, 0xFF, 0xFF]); // Fallback

    // Verify that keys with prefix are strictly less than end bound
    let key1 = vec![0x01, 0x02, 0x03, 0x00];
    let key2 = vec![0x01, 0x02, 0x03, 0xFF, 0xFF, 0xFF];
    let end_for_prefix = end_bound(&vec![0x01, 0x02, 0x03]);
    assert!(
        key1 < end_for_prefix,
        "key with prefix should be < end_bound"
    );
    assert!(
        key2 < end_for_prefix,
        "key with prefix + 0xFFs should be < end_bound"
    );
}

#[test]
fn test_empty_strings() {
    // Empty tenant should still work
    let key = job_info_key("", "job123");
    let parsed = parse_job_info_key(&key).unwrap();
    assert_eq!(parsed.tenant, "");
    assert_eq!(parsed.job_id, "job123");

    // Empty job_id should still work
    let key2 = job_info_key("tenant1", "");
    let parsed2 = parse_job_info_key(&key2).unwrap();
    assert_eq!(parsed2.tenant, "tenant1");
    assert_eq!(parsed2.job_id, "");
}

#[test]
fn test_special_characters_comprehensive() {
    // Test various special characters that might cause issues
    let special_chars = [
        "tenant/with/slashes",
        "tenant%with%percents",
        "tenant with spaces",
        "tenant\twith\ttabs",
        "tenant\0with\0nulls", // Embedded nulls - storekey should handle these
        "tenant:with:colons",
        "租户名", // Unicode characters
    ];

    for tenant in special_chars {
        let key = job_info_key(tenant, "job123");
        let parsed = parse_job_info_key(&key).unwrap();
        assert_eq!(parsed.tenant, tenant, "Failed for tenant: {:?}", tenant);
        assert_eq!(parsed.job_id, "job123");
    }
}

#[test]
fn test_concurrency_request_ordering() {
    // Requests should sort by start_time, then priority, then job_id, then attempt, then suffix
    let key1 = concurrency_request_key("t", "q", 1000, 10, "job1", 1, "aaaa");
    let key2 = concurrency_request_key("t", "q", 2000, 10, "job2", 1, "bbbb");
    let key3 = concurrency_request_key("t", "q", 1000, 20, "job3", 1, "cccc");

    // Earlier time comes first
    assert!(key1 < key2);
    // Same time, lower priority comes first
    assert!(key1 < key3);
}

#[test]
fn test_concurrency_request_job_prefix_scanning() {
    // Keys for the same job should share a job prefix
    let key1 = concurrency_request_key("t", "q", 1000, 10, "job1", 1, "aaaa0000");
    let key2 = concurrency_request_key("t", "q", 1000, 10, "job1", 1, "bbbb1111");
    let prefix = concurrency_request_job_prefix("t", "q", 1000, 10, "job1", 1);

    assert!(
        key1.starts_with(&prefix),
        "key1 should start with job prefix"
    );
    assert!(
        key2.starts_with(&prefix),
        "key2 should start with job prefix"
    );

    // Different job should NOT match
    let key3 = concurrency_request_key("t", "q", 1000, 10, "job2", 1, "cccc2222");
    assert!(
        !key3.starts_with(&prefix),
        "key3 (different job) should NOT start with job1 prefix"
    );

    // Different attempt should NOT match
    let key4 = concurrency_request_key("t", "q", 1000, 10, "job1", 2, "dddd3333");
    assert!(
        !key4.starts_with(&prefix),
        "key4 (different attempt) should NOT start with attempt 1 prefix"
    );
}

#[test]
fn test_parse_wrong_prefix_returns_none() {
    let job_key = job_info_key("tenant", "job");

    // All other parsers should return None for a job_info key
    assert!(parse_job_status_key(&job_key).is_none());
    assert!(parse_status_time_index_key(&job_key).is_none());
    assert!(parse_metadata_index_key(&job_key).is_none());
    assert!(parse_task_key(&job_key).is_none());
    assert!(parse_lease_key(&job_key).is_none());
    assert!(parse_attempt_key(&job_key).is_none());
    assert!(parse_concurrency_request_key(&job_key).is_none());
    assert!(parse_concurrency_holder_key(&job_key).is_none());
    assert!(parse_job_cancelled_key(&job_key).is_none());
    assert!(parse_floating_limit_key(&job_key).is_none());
}

#[test]
fn test_parse_empty_slice_returns_none() {
    let empty: &[u8] = &[];

    assert!(parse_job_info_key(empty).is_none());
    assert!(parse_job_status_key(empty).is_none());
    assert!(parse_task_key(empty).is_none());
    assert!(parse_attempt_key(empty).is_none());
    assert!(parse_lease_key(empty).is_none());
    assert!(parse_job_cancelled_key(empty).is_none());
    assert!(parse_floating_limit_key(empty).is_none());
}

#[test]
fn test_large_numeric_values() {
    // Test with large timestamp values near u64::MAX
    let large_time: i64 = i64::MAX;
    let key = task_key("group", large_time, 255, "job", u32::MAX);
    let parsed = parse_task_key(&key).unwrap();
    assert_eq!(parsed.start_time_ms, large_time as u64);
    assert_eq!(parsed.priority, 255);
    assert_eq!(parsed.attempt, u32::MAX);
}
