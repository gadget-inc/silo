//! Tests for the in-memory lease index: which leases it reports due, and how
//! writes, renewals and releases move an entry.

use silo::lease_index::{LeaseIndex, ScannedLease};

fn due_ids(index: &LeaseIndex, now_ms: i64) -> Vec<String> {
    index
        .due(now_ms)
        .into_iter()
        .map(|lease| lease.task_id)
        .collect()
}

#[test]
fn an_empty_index_has_nothing_due() {
    let index = LeaseIndex::default();
    assert!(index.due(i64::MAX).is_empty());
    assert_eq!(index.next_expiry_ms(), None);
    assert_eq!(index.len(), 0);
}

#[test]
fn a_lease_is_due_from_its_expiry_onward() {
    let index = LeaseIndex::default();
    index.expect_written("a", 1_000);

    assert!(due_ids(&index, 999).is_empty());
    assert_eq!(due_ids(&index, 1_000), ["a"]);
    assert_eq!(due_ids(&index, 5_000), ["a"]);
    assert_eq!(index.next_expiry_ms(), Some(1_000));
}

#[test]
fn due_leases_come_back_earliest_first_with_their_expiry() {
    let index = LeaseIndex::default();
    index.expect_written("late", 300);
    index.expect_written("early", 100);
    index.expect_written("middle", 200);
    index.expect_written("future", 900);

    let due = index.due(300);
    let pairs: Vec<(&str, i64)> = due
        .iter()
        .map(|lease| (lease.task_id.as_str(), lease.expiry_ms))
        .collect();
    assert_eq!(pairs, [("early", 100), ("middle", 200), ("late", 300)]);
    assert_eq!(index.len(), 4, "reading due leases removes nothing");
}

#[test]
fn an_expected_write_never_raises_an_existing_entry() {
    let index = LeaseIndex::default();
    index.expect_written("a", 1_000);
    index.expect_written("a", 2_000);
    assert_eq!(index.next_expiry_ms(), Some(1_000));

    index.expect_written("a", 500);
    assert_eq!(index.next_expiry_ms(), Some(500));
    assert_eq!(index.len(), 1);
}

#[test]
fn a_confirmed_write_sets_the_entry_in_either_direction() {
    let index = LeaseIndex::default();
    index.expect_written("a", 1_000);

    assert!(index.confirm_written("a", 2_000), "the entry existed");
    assert!(due_ids(&index, 1_999).is_empty());
    assert_eq!(due_ids(&index, 2_000), ["a"]);

    assert!(index.confirm_written("a", 700));
    assert_eq!(due_ids(&index, 700), ["a"]);
    assert_eq!(index.len(), 1);
}

#[test]
fn confirming_an_unknown_lease_adds_it_and_says_it_was_untracked() {
    let index = LeaseIndex::default();
    assert!(!index.confirm_written("a", 1_000));
    assert_eq!(due_ids(&index, 1_000), ["a"]);
}

#[test]
fn a_release_removes_the_entry_it_names() {
    let index = LeaseIndex::default();
    index.expect_written("a", 1_000);
    index.expect_written("b", 1_000);

    index.release("a", 1_000);

    assert_eq!(due_ids(&index, 1_000), ["b"]);
    assert_eq!(index.len(), 1);
}

/// A release names the expiry of the lease row it deleted. An entry that has
/// moved since belongs to a newer write of the same task id and stays.
#[test]
fn a_release_leaves_an_entry_whose_expiry_has_moved() {
    let index = LeaseIndex::default();
    index.expect_written("a", 1_000);
    index.confirm_written("a", 2_000);

    index.release("a", 1_000);

    assert_eq!(due_ids(&index, 2_000), ["a"]);
}

#[test]
fn releasing_an_unknown_lease_is_a_no_op() {
    let index = LeaseIndex::default();
    index.release("a", 1_000);
    assert_eq!(index.len(), 0);
}

#[test]
fn a_full_scan_adds_a_lease_the_index_has_not_heard_of() {
    let index = LeaseIndex::default();
    let scan = index.begin_full_scan();

    assert_eq!(scan.lookup("a"), ScannedLease::Untracked);
    assert!(scan.seed("a", 1_000));

    assert_eq!(scan.lookup("a"), ScannedLease::Tracked { expiry_ms: 1_000 });
    assert_eq!(due_ids(&index, 1_000), ["a"]);
}

/// The scan's snapshot can trail a renewal, and a release names the expiry
/// its writer recorded, so the scan leaves an entry it finds where it is.
#[test]
fn a_full_scan_leaves_an_indexed_lease_at_its_expiry() {
    let index = LeaseIndex::default();
    index.expect_written("a", 2_000);
    let scan = index.begin_full_scan();

    assert_eq!(scan.lookup("a"), ScannedLease::Tracked { expiry_ms: 2_000 });
    assert!(!scan.seed("a", 1_000));

    assert_eq!(index.next_expiry_ms(), Some(2_000));
    index.release("a", 2_000);
    assert_eq!(index.len(), 0);
}

/// A row the scan read before its lease was released is gone from the
/// database, so the scan adds no entry for it.
#[test]
fn a_full_scan_does_not_add_a_lease_released_since_it_began() {
    let index = LeaseIndex::default();
    index.expect_written("tracked", 1_000);
    let scan = index.begin_full_scan();

    index.release("tracked", 1_000);
    index.release("never tracked", 1_000);

    for task_id in ["tracked", "never tracked"] {
        assert_eq!(scan.lookup(task_id), ScannedLease::Released, "{task_id}");
        assert!(!scan.seed(task_id, 1_000), "{task_id}");
    }
    assert_eq!(index.len(), 0);
}

/// A release that leaves the entry in place, because the entry belongs to a
/// newer write of the task id, leaves the lease tracked.
#[test]
fn a_lease_written_again_after_its_release_is_tracked_by_a_full_scan() {
    let index = LeaseIndex::default();
    let scan = index.begin_full_scan();
    index.expect_written("a", 1_000);
    index.release("a", 1_000);
    index.expect_written("a", 3_000);
    index.release("a", 1_000);

    assert_eq!(scan.lookup("a"), ScannedLease::Tracked { expiry_ms: 3_000 });
    assert!(!scan.seed("a", 1_000));
    assert_eq!(index.next_expiry_ms(), Some(3_000));
}

/// A pass that deletes a lease row drops the entry it saw for the lease. A
/// renewal that lands between that sighting and the drop moves the entry,
/// which then stays for the row the renewal wrote.
#[test]
fn a_release_at_the_expiry_a_full_scan_saw_leaves_a_lease_renewed_since() {
    let index = LeaseIndex::default();
    index.expect_written("renewed", 1_000);
    index.expect_written("idle", 1_000);
    let scan = index.begin_full_scan();
    let ScannedLease::Tracked { expiry_ms: renewed } = scan.lookup("renewed") else {
        panic!("the index holds the lease");
    };
    let ScannedLease::Tracked { expiry_ms: idle } = scan.lookup("idle") else {
        panic!("the index holds the lease");
    };

    index.confirm_written("renewed", 9_000);
    index.release("renewed", renewed);
    index.release("idle", idle);

    assert_eq!(due_ids(&index, 9_000), ["renewed"]);
}

#[test]
fn a_release_before_a_full_scan_began_does_not_hide_the_lease_from_it() {
    let index = LeaseIndex::default();
    index.release("before any scan", 1_000);
    {
        let earlier = index.begin_full_scan();
        index.release("during the earlier scan", 1_000);
        assert_eq!(
            earlier.lookup("during the earlier scan"),
            ScannedLease::Released
        );
    }

    let scan = index.begin_full_scan();

    assert!(scan.seed("before any scan", 1_000));
    assert!(scan.seed("during the earlier scan", 1_000));
    assert_eq!(index.len(), 2);
}

#[test]
fn releases_stay_recorded_until_the_last_overlapping_full_scan_ends() {
    let index = LeaseIndex::default();
    let first = index.begin_full_scan();
    let second = index.begin_full_scan();
    index.release("a", 1_000);

    drop(first);

    assert_eq!(second.lookup("a"), ScannedLease::Released);
    assert!(!second.seed("a", 1_000));
}

#[test]
fn a_full_scan_is_due_before_the_first_and_again_after_the_interval() {
    let index = LeaseIndex::default();
    assert!(
        index.full_scan_due(0, 60_000),
        "nothing has seeded the index"
    );
    assert!(!index.seeded());

    index.record_full_scan(10_000);

    assert!(index.seeded());
    assert!(!index.full_scan_due(10_000, 60_000));
    assert!(!index.full_scan_due(69_999, 60_000));
    assert!(index.full_scan_due(70_000, 60_000));
}
