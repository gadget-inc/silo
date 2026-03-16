mod test_helpers;

use silo::keys::{standalone_concurrency_holder_key, standalone_concurrency_request_prefix};
use test_helpers::*;

#[silo::test]
async fn standalone_acquire_under_capacity_grants_immediately() {
    let (_tmp, shard) = open_temp_shard().await;
    let range = shard.get_range();

    let result = shard
        .standalone_acquire_ticket(&range, "-", "q1", "p1", 2, 0, vec![1, 2, 3])
        .await
        .expect("acquire");

    assert!(result.acquired, "should be acquired when under capacity");
    assert!(result.promoted.is_empty(), "no others to promote");

    // Verify holder exists in DB
    let holder = shard
        .db()
        .get(&standalone_concurrency_holder_key("-", "q1", "p1"))
        .await
        .expect("get holder");
    assert!(holder.is_some(), "holder record should exist in DB");
}

#[silo::test]
async fn standalone_acquire_at_capacity_queues_request() {
    let (_tmp, shard) = open_temp_shard().await;
    let range = shard.get_range();

    // Fill capacity
    let r1 = shard
        .standalone_acquire_ticket(&range, "-", "q1", "p1", 1, 0, vec![])
        .await
        .expect("acquire p1");
    assert!(r1.acquired);

    // Second participant should be queued
    let r2 = shard
        .standalone_acquire_ticket(&range, "-", "q1", "p2", 1, 0, vec![])
        .await
        .expect("acquire p2");
    assert!(!r2.acquired, "should NOT be acquired when at capacity");
}

#[silo::test]
async fn standalone_release_promotes_waiter() {
    let (_tmp, shard) = open_temp_shard().await;
    let range = shard.get_range();

    // Fill capacity
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "p1", 1, 0, vec![])
        .await
        .expect("acquire p1");

    // Queue a waiter
    let r2 = shard
        .standalone_acquire_ticket(&range, "-", "q1", "p2", 1, 0, vec![4, 5, 6])
        .await
        .expect("acquire p2");
    assert!(!r2.acquired);

    // Release p1 - should promote p2
    let release = shard
        .standalone_release_ticket(&range, "-", "q1", "p1", 1)
        .await
        .expect("release p1");

    assert!(release.was_held, "p1 was a holder");
    assert_eq!(release.promoted.len(), 1, "should promote one waiter");
    assert_eq!(release.promoted[0].participant_id, "p2");
    assert_eq!(release.promoted[0].metadata, vec![4, 5, 6]);
}

#[silo::test]
async fn standalone_acquire_idempotent() {
    let (_tmp, shard) = open_temp_shard().await;
    let range = shard.get_range();

    let r1 = shard
        .standalone_acquire_ticket(&range, "-", "q1", "p1", 1, 0, vec![])
        .await
        .expect("acquire p1 first");
    assert!(r1.acquired);

    // Re-acquire same participant - should return acquired=true
    let r2 = shard
        .standalone_acquire_ticket(&range, "-", "q1", "p1", 1, 0, vec![])
        .await
        .expect("acquire p1 second");
    assert!(r2.acquired, "idempotent re-acquire should succeed");
}

#[silo::test]
async fn standalone_priority_ordering() {
    let (_tmp, shard) = open_temp_shard().await;
    let range = shard.get_range();

    // Fill capacity
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "holder", 1, 0, vec![])
        .await
        .expect("acquire holder");

    // Queue low priority first
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "low", 1, 10, vec![])
        .await
        .expect("acquire low");

    // Queue high priority second
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "high", 1, 1, vec![])
        .await
        .expect("acquire high");

    // Release holder - should promote high priority first
    let release = shard
        .standalone_release_ticket(&range, "-", "q1", "holder", 1)
        .await
        .expect("release holder");

    assert_eq!(release.promoted.len(), 1);
    assert_eq!(
        release.promoted[0].participant_id, "high",
        "higher priority (lower value) should be promoted first"
    );
}

#[silo::test]
async fn standalone_queue_type_enforcement_via_enqueue() {
    let (_tmp, shard) = open_temp_shard().await;
    let range = shard.get_range();

    // Use queue as standalone
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "p1", 1, 0, vec![])
        .await
        .expect("standalone acquire");

    // Try to use same queue via job system by enqueuing a job with concurrency limit
    let result = shard
        .enqueue(
            "-",
            None,
            10u8,
            silo::job_store_shard::now_epoch_ms(),
            None,
            test_helpers::msgpack_payload(&serde_json::json!({"k": "v"})),
            vec![silo::job::Limit::Concurrency(silo::job::ConcurrencyLimit {
                key: "q1".to_string(),
                max_concurrency: 1,
            })],
            None,
            "default",
        )
        .await;

    assert!(
        result.is_err(),
        "job enqueue on standalone queue should fail"
    );
}

#[silo::test]
async fn standalone_release_nonexistent_returns_not_held() {
    let (_tmp, shard) = open_temp_shard().await;
    let range = shard.get_range();

    let result = shard
        .standalone_release_ticket(&range, "-", "q1", "nobody", 1)
        .await
        .expect("release nonexistent");

    assert!(
        !result.was_held,
        "should not report was_held for nonexistent"
    );
    assert!(result.promoted.is_empty());
}

#[silo::test]
async fn standalone_release_requester() {
    let (_tmp, shard) = open_temp_shard().await;
    let range = shard.get_range();

    // Fill capacity
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "holder", 1, 0, vec![])
        .await
        .expect("acquire holder");

    // Queue a requester
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "waiter", 1, 0, vec![])
        .await
        .expect("acquire waiter");

    // Release the requester (not the holder)
    let result = shard
        .standalone_release_ticket(&range, "-", "q1", "waiter", 1)
        .await
        .expect("release waiter");

    assert!(!result.was_held, "waiter was not a holder");
    assert!(result.promoted.is_empty(), "no promotions needed");

    // Verify request is gone
    let prefix = standalone_concurrency_request_prefix("-", "q1");
    let end = silo::keys::end_bound(&prefix);
    let mut iter = shard
        .db()
        .scan::<Vec<u8>, _>(prefix..end)
        .await
        .expect("scan");
    let next = iter.next().await.expect("next");
    assert!(next.is_none(), "request should be deleted");
}

#[silo::test]
async fn standalone_multiple_promotions() {
    let (_tmp, shard) = open_temp_shard().await;
    let range = shard.get_range();

    // Fill all 3 slots
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "p1", 3, 0, vec![])
        .await
        .expect("acquire p1");
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "p2", 3, 0, vec![])
        .await
        .expect("acquire p2");
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "p3", 3, 0, vec![])
        .await
        .expect("acquire p3");

    assert_eq!(
        shard.concurrency_holder_count("-", "q1"),
        3,
        "all 3 should be holders"
    );

    // Queue 2 more waiters
    let r4 = shard
        .standalone_acquire_ticket(&range, "-", "q1", "p4", 3, 0, vec![])
        .await
        .expect("acquire p4");
    assert!(!r4.acquired);

    let r5 = shard
        .standalone_acquire_ticket(&range, "-", "q1", "p5", 3, 0, vec![])
        .await
        .expect("acquire p5");
    assert!(!r5.acquired);

    // Release 2 holders - should promote both waiters
    shard
        .standalone_release_ticket(&range, "-", "q1", "p1", 3)
        .await
        .expect("release p1");

    shard
        .standalone_release_ticket(&range, "-", "q1", "p2", 3)
        .await
        .expect("release p2");

    assert_eq!(
        shard.concurrency_holder_count("-", "q1"),
        3,
        "should still have 3 holders after promotions"
    );
}

#[silo::test]
async fn standalone_force_release_promotes_next() {
    let (_tmp, shard) = open_temp_shard().await;
    let range = shard.get_range();

    shard
        .standalone_acquire_ticket(&range, "-", "q1", "p1", 1, 0, vec![])
        .await
        .expect("acquire p1");
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "p2", 1, 0, vec![7, 8])
        .await
        .expect("acquire p2");

    let result = shard
        .standalone_force_release_ticket(&range, "-", "q1", "p1", 1)
        .await
        .expect("force release p1");

    assert!(result.was_held);
    assert_eq!(result.promoted.len(), 1);
    assert_eq!(result.promoted[0].participant_id, "p2");
}

#[silo::test]
async fn standalone_force_admit_all() {
    let (_tmp, shard) = open_temp_shard().await;
    let range = shard.get_range();

    // Fill capacity
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "holder", 1, 0, vec![])
        .await
        .expect("acquire holder");

    // Queue 2 waiters
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "w1", 1, 0, vec![1])
        .await
        .expect("acquire w1");
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "w2", 1, 0, vec![2])
        .await
        .expect("acquire w2");

    // Force admit all
    let admitted = shard
        .standalone_force_admit_requesters(&range, "-", "q1")
        .await
        .expect("force admit");

    assert_eq!(admitted.len(), 2, "should admit both waiters");

    assert_eq!(
        shard.concurrency_holder_count("-", "q1"),
        3,
        "original holder + 2 admitted"
    );
}

#[silo::test]
async fn standalone_list_holders_basic() {
    let (_tmp, shard) = open_temp_shard().await;
    let range = shard.get_range();

    shard
        .standalone_acquire_ticket(&range, "-", "q1", "p1", 5, 3, vec![10, 20])
        .await
        .expect("acquire p1");
    shard
        .standalone_acquire_ticket(&range, "-", "q1", "p2", 5, 5, vec![30, 40])
        .await
        .expect("acquire p2");

    let result = shard
        .standalone_list_holders("-", "q1", 10, &[])
        .await
        .expect("list holders");

    assert_eq!(result.holders.len(), 2);
    assert!(result.next_cursor.is_empty(), "should be exhausted");

    // Verify fields
    let p1 = result
        .holders
        .iter()
        .find(|h| h.participant_id == "p1")
        .expect("find p1");
    assert_eq!(p1.tenant, "-");
    assert_eq!(p1.queue, "q1");
    assert_eq!(p1.metadata, vec![10, 20]);
    assert_eq!(p1.priority, 3);
    assert!(p1.granted_at_ms > 0);
}
