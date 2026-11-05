//! Tests for the staggered periodic full-compaction schedule: the pure slot
//! assignment that spreads shards across a period, and the tracker that
//! decides which owned shards are due on a tick.

use std::collections::BTreeMap;
use std::time::Duration;

use silo::compaction_schedule::{CompactionSchedule, DueShard, SlotPosition, SubmissionTracker};
use silo::shard_range::ShardId;
use uuid::Uuid;

/// One hour spread across 60 one-minute slots, the production shape.
fn hourly_schedule() -> CompactionSchedule {
    CompactionSchedule::new(3600, 60).expect("3600 s period divides into 60 s slots")
}

/// A fixed, reproducible set of shard ids so spread assertions are stable.
fn fixed_shards(count: u64) -> Vec<ShardId> {
    (0..count as u128)
        .map(|i| {
            ShardId::from_uuid(Uuid::from_u128(
                0x5eed_0000_0000_0000_0000_0000_0000_0000 + i * 7919,
            ))
        })
        .collect()
}

#[test]
fn every_shard_maps_to_exactly_one_slot_within_range() {
    let schedule = hourly_schedule();
    assert_eq!(schedule.slot_count(), 60);
    for shard in fixed_shards(200) {
        let slot = schedule.slot_for(shard);
        assert!(
            slot < schedule.slot_count(),
            "shard {shard}: slot {slot} is outside 0..{}",
            schedule.slot_count()
        );
    }
}

#[test]
fn slot_assignment_is_stable_across_calls_and_schedules() {
    let schedule = hourly_schedule();
    let other = hourly_schedule();
    for shard in fixed_shards(50) {
        let first = schedule.slot_for(shard);
        assert_eq!(
            first,
            schedule.slot_for(shard),
            "shard {shard} changed slot between calls"
        );
        assert_eq!(
            first,
            other.slot_for(shard),
            "shard {shard} changed slot between schedule instances"
        );
    }
}

#[test]
fn fixed_shards_spread_across_slots_instead_of_collapsing() {
    let schedule = hourly_schedule();
    let shards = fixed_shards(34);
    let mut per_slot: BTreeMap<u64, usize> = BTreeMap::new();
    for shard in &shards {
        *per_slot.entry(schedule.slot_for(*shard)).or_default() += 1;
    }
    let busiest = per_slot.values().copied().max().unwrap_or(0);
    assert!(
        per_slot.len() >= 24,
        "34 shards landed on only {} of 60 slots: {per_slot:?}",
        per_slot.len()
    );
    assert!(
        busiest <= 3,
        "a single slot received {busiest} of 34 shards: {per_slot:?}"
    );
}

/// A one-shard schedule where the shard's slot is known, so tick times can be
/// chosen relative to it.
fn shard_and_slot_start(schedule: &CompactionSchedule) -> (ShardId, u64) {
    let shard = fixed_shards(1)[0];
    (shard, schedule.slot_for(shard) * schedule.slot_s())
}

fn due_ids(due: &[DueShard]) -> Vec<ShardId> {
    due.iter().map(|d| d.shard).collect()
}

#[test]
fn current_slot_follows_wall_clock_within_the_period() {
    let schedule = hourly_schedule();
    let period_start = 3600 * 1234;
    let at = |offset: u64| schedule.position(period_start + offset);
    assert_eq!(
        at(0),
        Some(SlotPosition {
            period: 1234,
            slot: 0
        })
    );
    assert_eq!(
        at(59),
        Some(SlotPosition {
            period: 1234,
            slot: 0
        })
    );
    assert_eq!(
        at(60),
        Some(SlotPosition {
            period: 1234,
            slot: 1
        })
    );
    assert_eq!(
        at(3599),
        Some(SlotPosition {
            period: 1234,
            slot: 59
        })
    );
    assert_eq!(
        at(3600),
        Some(SlotPosition {
            period: 1235,
            slot: 0
        })
    );
}

#[test]
fn no_slot_is_active_in_the_tail_of_a_non_dividing_period() {
    let schedule = CompactionSchedule::new(100, 30).expect("non-zero period and slot");
    assert_eq!(schedule.slot_count(), 3);
    assert_eq!(schedule.tail_s(), 10);
    assert_eq!(
        schedule.position(89),
        Some(SlotPosition { period: 0, slot: 2 })
    );
    assert_eq!(
        schedule.position(90),
        None,
        "second 90 of a 100 s period with 30 s slots is tail"
    );
    assert_eq!(schedule.position(99), None);
    assert_eq!(
        schedule.position(100),
        Some(SlotPosition { period: 1, slot: 0 })
    );
}

#[test]
fn a_slot_longer_than_the_period_is_one_slot_per_period() {
    let schedule = CompactionSchedule::new(100, 300).expect("non-zero period and slot");
    assert_eq!(schedule.slot_count(), 1);
    assert_eq!(schedule.tail_s(), 0);
    assert_eq!(
        schedule.slot_s(),
        100,
        "the slot is shortened to the period so a ticker firing once per slot sees every period"
    );
    assert_eq!(
        schedule.delay_until_slot_midpoint(Duration::from_secs(100 * 7)),
        Duration::from_secs(50),
        "ticks align to the period, not the configured slot"
    );
    for shard in fixed_shards(10) {
        assert_eq!(schedule.slot_for(shard), 0);
    }
    assert_eq!(
        schedule.position(250),
        Some(SlotPosition { period: 2, slot: 0 })
    );
}

#[test]
fn shard_is_submitted_once_per_period_even_when_ticks_repeat_in_its_slot() {
    let schedule = hourly_schedule();
    let (shard, slot_start) = shard_and_slot_start(&schedule);
    let mut tracker = SubmissionTracker::default();

    let before = tracker.due(&schedule, slot_start.saturating_sub(1), [shard]);
    assert!(
        before.is_empty(),
        "not due a second before its slot: {before:?}"
    );

    let first = tracker.due(&schedule, slot_start, [shard]);
    assert_eq!(due_ids(&first), vec![shard]);
    assert_eq!(first[0].slot, schedule.slot_for(shard));

    for offset in [1, 30, 59] {
        let again = tracker.due(&schedule, slot_start + offset, [shard]);
        assert!(
            again.is_empty(),
            "tick at +{offset}s in the same slot re-submitted: {again:?}"
        );
    }

    let after = tracker.due(&schedule, slot_start + 60, [shard]);
    assert!(
        after.is_empty(),
        "not due once its slot has passed: {after:?}"
    );

    let next_period = tracker.due(&schedule, slot_start + 3600, [shard]);
    assert_eq!(
        due_ids(&next_period),
        vec![shard],
        "due again in the next period"
    );
}

#[test]
fn a_shard_that_opens_mid_slot_is_submitted_once() {
    let schedule = hourly_schedule();
    let (shard, slot_start) = shard_and_slot_start(&schedule);
    let mut tracker = SubmissionTracker::default();

    let unowned = tracker.due(&schedule, slot_start, []);
    assert!(unowned.is_empty());

    let opened = tracker.due(&schedule, slot_start + 20, [shard]);
    assert_eq!(due_ids(&opened), vec![shard]);

    let again = tracker.due(&schedule, slot_start + 40, [shard]);
    assert!(
        again.is_empty(),
        "re-submitted after opening mid-slot: {again:?}"
    );
}

#[test]
fn only_shards_in_the_current_slot_are_due() {
    let schedule = hourly_schedule();
    let shards = fixed_shards(34);
    let mut tracker = SubmissionTracker::default();
    let mut submitted: Vec<ShardId> = Vec::new();
    for slot in 0..schedule.slot_count() {
        let due = tracker.due(&schedule, slot * schedule.slot_s(), shards.iter().copied());
        for d in &due {
            assert_eq!(d.slot, slot);
            assert_eq!(
                schedule.slot_for(d.shard),
                slot,
                "shard {} is due outside its slot",
                d.shard
            );
        }
        submitted.extend(due_ids(&due));
    }
    let mut expected = shards.clone();
    expected.sort();
    submitted.sort();
    assert_eq!(
        submitted, expected,
        "every shard is submitted exactly once over a full period"
    );
}

#[test]
fn first_tick_is_delayed_to_the_next_slot_midpoint() {
    let schedule = hourly_schedule();
    let delay = |now_ms: u64| {
        schedule
            .delay_until_slot_midpoint(Duration::from_millis(now_ms))
            .as_millis() as u64
    };
    assert_eq!(
        delay(0),
        30_000,
        "at a slot boundary the midpoint is half a slot away"
    );
    assert_eq!(delay(29_999), 1);
    assert_eq!(delay(30_000), 0, "exactly at the midpoint");
    assert_eq!(
        delay(30_001),
        59_999,
        "just past the midpoint waits for the next one"
    );
    assert_eq!(delay(60_000 * 1234 + 45_000), 45_000);
}
