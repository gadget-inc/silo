//! Unit tests for ConcurrencyCounts in-memory reservation system.
//!
//! These tests verify the atomic reservation and rollback mechanisms that prevent
//! TOCTOU (Time-Of-Check-Time-Of-Use) races in concurrency slot management.
//!
//! Note: These tests use try_reserve_sync which tests the in-memory reservation
//! logic without requiring a database. The full try_reserve method adds lazy
//! hydration from storage before calling this same internal logic.

use silo::concurrency::ConcurrencyCounts;

#[silo::test]
fn try_reserve_success() {
    let counts = ConcurrencyCounts::new();

    // Should succeed when under limit
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task1", 2, "job1"));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    // Should succeed for second slot
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task2", 2, "job1"));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 2);
}

#[silo::test]
fn try_reserve_at_capacity() {
    let counts = ConcurrencyCounts::new();

    // Fill to capacity
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task1", 1, "job1"));

    // Should fail when at capacity
    assert!(!counts.try_reserve_sync("tenant1", "queue1", "task2", 1, "job2"));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);
}

#[silo::test]
fn try_reserve_idempotent() {
    let counts = ConcurrencyCounts::new();

    // Reserve same task twice (HashSet semantics)
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task1", 2, "job1"));
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task1", 2, "job1"));

    // Should still only count as one holder
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);
}

#[silo::test]
fn try_reserve_different_queues() {
    let counts = ConcurrencyCounts::new();

    // Different queues are independent
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task1", 1, "job1"));
    assert!(counts.try_reserve_sync("tenant1", "queue2", "task2", 1, "job2"));

    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);
    assert_eq!(counts.holder_count("tenant1", "queue2"), 1);
}

#[silo::test]
fn try_reserve_different_tenants() {
    let counts = ConcurrencyCounts::new();

    // Different tenants are independent
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task1", 1, "job1"));
    assert!(counts.try_reserve_sync("tenant2", "queue1", "task2", 1, "job2"));

    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);
    assert_eq!(counts.holder_count("tenant2", "queue1"), 1);
}

#[silo::test]
fn release_reservation() {
    let counts = ConcurrencyCounts::new();

    // Reserve and then release
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task1", 1, "job1"));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    counts.release_reservation("tenant1", "queue1", "task1");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 0);

    // Should be able to reserve again
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task2", 1, "job2"));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);
}

#[silo::test]
fn release_reservation_nonexistent() {
    let counts = ConcurrencyCounts::new();

    // Releasing a non-existent reservation should not panic
    counts.release_reservation("tenant1", "queue1", "task1");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 0);
}

#[silo::test]
fn atomic_release_and_reserve() {
    let counts = ConcurrencyCounts::new();

    // Setup: reserve task1
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task1", 1, "job1"));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    // Atomic swap: release task1, reserve task2
    counts.atomic_release_and_reserve("tenant1", "queue1", "task1", "task2", "job2");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    // task1 should be gone, task2 should be there
    // We can verify by trying to reserve - should fail since task2 is there
    assert!(!counts.try_reserve_sync("tenant1", "queue1", "task3", 1, "job3"));
}

#[silo::test]
fn atomic_release() {
    let counts = ConcurrencyCounts::new();

    // Setup: reserve task1
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task1", 1, "job1"));

    // Atomic release without grant
    counts.atomic_release("tenant1", "queue1", "task1");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 0);

    // Should be able to reserve again
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task2", 1, "job2"));
}

#[silo::test]
fn atomic_release_nonexistent() {
    let counts = ConcurrencyCounts::new();

    // Releasing a non-existent task should not panic
    counts.atomic_release("tenant1", "queue1", "task1");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 0);
}

#[silo::test]
fn rollback_release_and_reserve() {
    let counts = ConcurrencyCounts::new();

    // Setup: reserve task1
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task1", 1, "job1"));

    // Do atomic release and reserve
    counts.atomic_release_and_reserve("tenant1", "queue1", "task1", "task2", "job2");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    // Rollback: should restore task1 and remove task2
    counts.rollback_release_and_reserve("tenant1", "queue1", "task1", "task2");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    // Verify task1 is back by checking we're still at capacity
    assert!(!counts.try_reserve_sync("tenant1", "queue1", "task3", 1, "job3"));
}

#[silo::test]
fn rollback_release() {
    let counts = ConcurrencyCounts::new();

    // Setup: reserve task1
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task1", 1, "job1"));

    // Release it
    counts.atomic_release("tenant1", "queue1", "task1");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 0);

    // Rollback: should restore task1
    counts.rollback_release("tenant1", "queue1", "task1");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    // Should be at capacity again
    assert!(!counts.try_reserve_sync("tenant1", "queue1", "task2", 1, "job2"));
}

#[silo::test]
fn toctou_prevention_scenario() {
    // This test demonstrates the fix for the TOCTOU race.
    // The key insight is that try_reserve atomically checks AND reserves.
    let counts = ConcurrencyCounts::new();

    // Simulate two concurrent operations trying to get the last slot
    // With try_reserve, only one can succeed

    // Thread 1: try to reserve
    let success1 = counts.try_reserve_sync("tenant1", "queue1", "task1", 1, "job1");

    // Thread 2: try to reserve (simulating concurrent access)
    let success2 = counts.try_reserve_sync("tenant1", "queue1", "task2", 1, "job2");

    // Only one should succeed
    assert!(success1);
    assert!(!success2);
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);
}

#[silo::test]
fn rollback_after_failed_db_write_scenario() {
    // Simulates the rollback flow when a DB write fails after try_reserve
    let counts = ConcurrencyCounts::new();

    // Reserve a slot (simulating successful try_reserve)
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task1", 1, "job1"));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    // Simulate DB write failure - need to rollback
    counts.release_reservation("tenant1", "queue1", "task1");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 0);

    // The slot should now be available for another attempt
    assert!(counts.try_reserve_sync("tenant1", "queue1", "task2", 1, "job2"));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);
}

#[silo::test]
fn concurrent_reserve_limit_enforcement() {
    // Tests that try_reserve correctly enforces limits with various max_concurrency values
    let counts = ConcurrencyCounts::new();

    // With limit=3, should be able to reserve 3 tasks
    assert!(counts.try_reserve_sync("t", "q", "task1", 3, "job1"));
    assert!(counts.try_reserve_sync("t", "q", "task2", 3, "job2"));
    assert!(counts.try_reserve_sync("t", "q", "task3", 3, "job3"));

    // 4th should fail
    assert!(!counts.try_reserve_sync("t", "q", "task4", 3, "job4"));

    assert_eq!(counts.holder_count("t", "q"), 3);
}

#[silo::test]
fn release_and_reserve_maintains_count() {
    // Verifies that atomic_release_and_reserve doesn't change the count
    let counts = ConcurrencyCounts::new();

    // Setup: fill to capacity
    assert!(counts.try_reserve_sync("t", "q", "task1", 2, "job1"));
    assert!(counts.try_reserve_sync("t", "q", "task2", 2, "job2"));
    assert_eq!(counts.holder_count("t", "q"), 2);

    // Atomic swap shouldn't change count
    counts.atomic_release_and_reserve("t", "q", "task1", "task3", "job3");
    assert_eq!(counts.holder_count("t", "q"), 2);

    // Still at capacity
    assert!(!counts.try_reserve_sync("t", "q", "task4", 2, "job4"));
}

#[silo::test]
fn multiple_rollbacks_in_sequence() {
    // Tests the rollback flow for multiple release-and-grant operations
    let counts = ConcurrencyCounts::new();

    // Setup initial state
    assert!(counts.try_reserve_sync("t", "q1", "task1", 1, "job1"));
    assert!(counts.try_reserve_sync("t", "q2", "task2", 1, "job2"));

    // Perform operations on both queues
    counts.atomic_release_and_reserve("t", "q1", "task1", "task3", "job3");
    counts.atomic_release("t", "q2", "task2");

    assert_eq!(counts.holder_count("t", "q1"), 1);
    assert_eq!(counts.holder_count("t", "q2"), 0);

    // Rollback both (simulating DB write failure)
    counts.rollback_release_and_reserve("t", "q1", "task1", "task3");
    counts.rollback_release("t", "q2", "task2");

    // Should be back to original state
    assert_eq!(counts.holder_count("t", "q1"), 1);
    assert_eq!(counts.holder_count("t", "q2"), 1);
}

/// Test the retry-scheduling scenario where both an immediate grant and a release
/// happen in the same operation, requiring rollback of both on failure.
///
/// Scenario (with max_concurrency=2):
/// 1. Job A is running (holds task1, 1/2 slots)
/// 2. Job A fails and schedules retry task2
/// 3. Retry task2 gets immediate grant (2/2 slots now)
/// 4. A's original slot (task1) is released via release_and_reserve to grant to waiting task3
/// 5. If DB write fails, we need to rollback:
///    - task2's immediate grant (from retry scheduling)
///    - task1's release and task3's grant (from release_and_reserve)
///
/// Without proper rollback, we'd leak slot(s) in memory:
/// - If we only rollback release_and_reserve: task2's slot leaks (count shows 2, should be 1)
/// - If we only rollback task2's grant: task1 is released but shouldn't be
#[silo::test]
fn retry_scheduling_with_immediate_grant_rollback() {
    let counts = ConcurrencyCounts::new();

    // Initial state: Job A running with task1 (1/2 slots used)
    assert!(counts.try_reserve_sync("t", "q", "task1", 2, "jobA"));
    assert_eq!(counts.holder_count("t", "q"), 1);

    // There's also a waiting job B that will get granted when A releases
    // (simulated by task3 which will be granted via release_and_reserve)

    // Step 1: Retry scheduling - task2 gets immediate grant (now 2/2)
    // This simulates what happens in lease.rs when enqueue_limit_task_at_index
    // is called for retry scheduling with available capacity
    assert!(counts.try_reserve_sync("t", "q", "task2", 2, "jobA"));
    assert_eq!(counts.holder_count("t", "q"), 2);

    // Step 2: Release A's slot and grant to waiting B (task3)
    // This simulates release_and_grant_next in lease.rs
    counts.atomic_release_and_reserve("t", "q", "task1", "task3", "jobB");
    assert_eq!(counts.holder_count("t", "q"), 2); // task2 + task3

    // Now simulate DB write failure - we need to rollback BOTH operations

    // Rollback the retry's immediate grant (task2)
    counts.release_reservation("t", "q", "task2");

    // Rollback the release-and-reserve (restore task1, remove task3)
    counts.rollback_release_and_reserve("t", "q", "task1", "task3");

    // Should be back to original state: only task1 holding
    assert_eq!(counts.holder_count("t", "q"), 1);

    // Verify task1 is the holder (can't add another at limit=1)
    assert!(!counts.try_reserve_sync("t", "q", "task_check", 1, "jobX"));
}

/// Test that WITHOUT proper rollback of retry immediate grants, we leak slots.
/// This demonstrates why the rollback is necessary.
#[silo::test]
fn retry_immediate_grant_leak_without_rollback() {
    let counts = ConcurrencyCounts::new();

    // Initial state: Job A running with task1 (1/2 slots used)
    assert!(counts.try_reserve_sync("t", "q", "task1", 2, "jobA"));
    assert_eq!(counts.holder_count("t", "q"), 1);

    // Retry scheduling - task2 gets immediate grant (now 2/2)
    assert!(counts.try_reserve_sync("t", "q", "task2", 2, "jobA"));
    assert_eq!(counts.holder_count("t", "q"), 2);

    // Release A's slot and grant to waiting B (task3)
    counts.atomic_release_and_reserve("t", "q", "task1", "task3", "jobB");
    assert_eq!(counts.holder_count("t", "q"), 2); // task2 + task3

    // Simulate INCORRECT rollback - only rollback release_and_reserve, forget task2
    // This is what would happen if we ignored the grants from enqueue_limit_task_at_index
    counts.rollback_release_and_reserve("t", "q", "task1", "task3");

    // BUG: count is 2 but should be 1! task2's slot leaked
    assert_eq!(
        counts.holder_count("t", "q"),
        2,
        "This demonstrates the leak: count is 2 when it should be 1 after rollback"
    );

    // We can't acquire more slots even though task1 should be the only holder
    // and max=2 should allow one more
    assert!(
        !counts.try_reserve_sync("t", "q", "new_task", 2, "jobX"),
        "Leaked slot prevents new reservations"
    );
}
