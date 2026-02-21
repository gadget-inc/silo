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
