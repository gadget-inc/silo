//! Unit tests for ConcurrencyCounts in-memory reservation system.
//!
//! These tests verify the atomic reservation and rollback mechanisms that prevent
//! TOCTOU (Time-Of-Check-Time-Of-Use) races in concurrency slot management.

use silo::concurrency::ConcurrencyCounts;

#[test]
fn try_reserve_success() {
    let counts = ConcurrencyCounts::new();

    // Should succeed when under limit
    assert!(counts.try_reserve("tenant1", "queue1", "task1", 2));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    // Should succeed for second slot
    assert!(counts.try_reserve("tenant1", "queue1", "task2", 2));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 2);
}

#[test]
fn try_reserve_at_capacity() {
    let counts = ConcurrencyCounts::new();

    // Fill to capacity
    assert!(counts.try_reserve("tenant1", "queue1", "task1", 1));

    // Should fail when at capacity
    assert!(!counts.try_reserve("tenant1", "queue1", "task2", 1));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);
}

#[test]
fn try_reserve_idempotent() {
    let counts = ConcurrencyCounts::new();

    // Reserve same task twice (HashSet semantics)
    assert!(counts.try_reserve("tenant1", "queue1", "task1", 2));
    assert!(counts.try_reserve("tenant1", "queue1", "task1", 2));

    // Should still only count as one holder
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);
}

#[test]
fn try_reserve_different_queues() {
    let counts = ConcurrencyCounts::new();

    // Different queues are independent
    assert!(counts.try_reserve("tenant1", "queue1", "task1", 1));
    assert!(counts.try_reserve("tenant1", "queue2", "task2", 1));

    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);
    assert_eq!(counts.holder_count("tenant1", "queue2"), 1);
}

#[test]
fn try_reserve_different_tenants() {
    let counts = ConcurrencyCounts::new();

    // Different tenants are independent
    assert!(counts.try_reserve("tenant1", "queue1", "task1", 1));
    assert!(counts.try_reserve("tenant2", "queue1", "task2", 1));

    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);
    assert_eq!(counts.holder_count("tenant2", "queue1"), 1);
}

#[test]
fn release_reservation() {
    let counts = ConcurrencyCounts::new();

    // Reserve and then release
    assert!(counts.try_reserve("tenant1", "queue1", "task1", 1));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    counts.release_reservation("tenant1", "queue1", "task1");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 0);

    // Should be able to reserve again
    assert!(counts.try_reserve("tenant1", "queue1", "task2", 1));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);
}

#[test]
fn release_reservation_nonexistent() {
    let counts = ConcurrencyCounts::new();

    // Releasing a non-existent reservation should not panic
    counts.release_reservation("tenant1", "queue1", "task1");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 0);
}

#[test]
fn atomic_release_and_reserve() {
    let counts = ConcurrencyCounts::new();

    // Setup: reserve task1
    assert!(counts.try_reserve("tenant1", "queue1", "task1", 1));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    // Atomic swap: release task1, reserve task2
    counts.atomic_release_and_reserve("tenant1", "queue1", "task1", "task2");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    // task1 should be gone, task2 should be there
    // We can verify by trying to reserve - should fail since task2 is there
    assert!(!counts.try_reserve("tenant1", "queue1", "task3", 1));
}

#[test]
fn atomic_release() {
    let counts = ConcurrencyCounts::new();

    // Setup: reserve task1
    assert!(counts.try_reserve("tenant1", "queue1", "task1", 1));

    // Atomic release without grant
    counts.atomic_release("tenant1", "queue1", "task1");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 0);

    // Should be able to reserve again
    assert!(counts.try_reserve("tenant1", "queue1", "task2", 1));
}

#[test]
fn atomic_release_nonexistent() {
    let counts = ConcurrencyCounts::new();

    // Releasing a non-existent task should not panic
    counts.atomic_release("tenant1", "queue1", "task1");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 0);
}

#[test]
fn rollback_release_and_reserve() {
    let counts = ConcurrencyCounts::new();

    // Setup: reserve task1
    assert!(counts.try_reserve("tenant1", "queue1", "task1", 1));

    // Do atomic release and reserve
    counts.atomic_release_and_reserve("tenant1", "queue1", "task1", "task2");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    // Rollback: should restore task1 and remove task2
    counts.rollback_release_and_reserve("tenant1", "queue1", "task1", "task2");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    // Verify task1 is back by checking we're still at capacity
    assert!(!counts.try_reserve("tenant1", "queue1", "task3", 1));
}

#[test]
fn rollback_release() {
    let counts = ConcurrencyCounts::new();

    // Setup: reserve task1
    assert!(counts.try_reserve("tenant1", "queue1", "task1", 1));

    // Release it
    counts.atomic_release("tenant1", "queue1", "task1");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 0);

    // Rollback: should restore task1
    counts.rollback_release("tenant1", "queue1", "task1");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    // Should be at capacity again
    assert!(!counts.try_reserve("tenant1", "queue1", "task2", 1));
}

#[test]
fn toctou_prevention_scenario() {
    // This test demonstrates the fix for the TOCTOU race.
    // The key insight is that try_reserve atomically checks AND reserves.
    let counts = ConcurrencyCounts::new();

    // Simulate two concurrent operations trying to get the last slot
    // With try_reserve, only one can succeed

    // Thread 1: try to reserve
    let success1 = counts.try_reserve("tenant1", "queue1", "task1", 1);

    // Thread 2: try to reserve (simulating concurrent access)
    let success2 = counts.try_reserve("tenant1", "queue1", "task2", 1);

    // Only one should succeed
    assert!(success1);
    assert!(!success2);
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);
}

#[test]
fn rollback_after_failed_db_write_scenario() {
    // Simulates the rollback flow when a DB write fails after try_reserve
    let counts = ConcurrencyCounts::new();

    // Reserve a slot (simulating successful try_reserve)
    assert!(counts.try_reserve("tenant1", "queue1", "task1", 1));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);

    // Simulate DB write failure - need to rollback
    counts.release_reservation("tenant1", "queue1", "task1");
    assert_eq!(counts.holder_count("tenant1", "queue1"), 0);

    // The slot should now be available for another attempt
    assert!(counts.try_reserve("tenant1", "queue1", "task2", 1));
    assert_eq!(counts.holder_count("tenant1", "queue1"), 1);
}

#[test]
fn concurrent_reserve_limit_enforcement() {
    // Tests that try_reserve correctly enforces limits with various max_concurrency values
    let counts = ConcurrencyCounts::new();

    // With limit=3, should be able to reserve 3 tasks
    assert!(counts.try_reserve("t", "q", "task1", 3));
    assert!(counts.try_reserve("t", "q", "task2", 3));
    assert!(counts.try_reserve("t", "q", "task3", 3));

    // 4th should fail
    assert!(!counts.try_reserve("t", "q", "task4", 3));

    assert_eq!(counts.holder_count("t", "q"), 3);
}

#[test]
fn release_and_reserve_maintains_count() {
    // Verifies that atomic_release_and_reserve doesn't change the count
    let counts = ConcurrencyCounts::new();

    // Setup: fill to capacity
    assert!(counts.try_reserve("t", "q", "task1", 2));
    assert!(counts.try_reserve("t", "q", "task2", 2));
    assert_eq!(counts.holder_count("t", "q"), 2);

    // Atomic swap shouldn't change count
    counts.atomic_release_and_reserve("t", "q", "task1", "task3");
    assert_eq!(counts.holder_count("t", "q"), 2);

    // Still at capacity
    assert!(!counts.try_reserve("t", "q", "task4", 2));
}

#[test]
fn multiple_rollbacks_in_sequence() {
    // Tests the rollback flow for multiple release-and-grant operations
    let counts = ConcurrencyCounts::new();

    // Setup initial state
    assert!(counts.try_reserve("t", "q1", "task1", 1));
    assert!(counts.try_reserve("t", "q2", "task2", 1));

    // Perform operations on both queues
    counts.atomic_release_and_reserve("t", "q1", "task1", "task3");
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
