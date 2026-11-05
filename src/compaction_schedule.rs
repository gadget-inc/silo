//! Staggered scheduling of periodic full compactions.
//!
//! A full compaction rewrites every SST of a shard, so the shard's block
//! cache, meta cache and local object-store cache are all cold afterwards.
//! Submitting every shard at once would make a whole node cold at once, and
//! nodes started by the same rolling deploy would all do so inside the same
//! few minutes. This module spreads the submissions out instead: the period
//! is cut into fixed slots, every shard is pinned to one slot by a hash of its
//! id, and the current slot is derived from wall-clock time. A shard is
//! therefore submitted at the same point in every period whichever node owns
//! it and whenever that node started, and one shard goes cold at a time per
//! node.

use std::collections::HashMap;
use std::time::Duration;

use thiserror::Error;

use crate::shard_range::ShardId;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CompactionScheduleError {
    #[error("periodic_full_compaction_s must be at least 1 second")]
    ZeroPeriod,
    #[error("periodic_full_compaction_slot_s must be at least 1 second")]
    ZeroSlot,
}

/// How a compaction period is divided into slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionSchedule {
    period_s: u64,
    slot_s: u64,
}

impl CompactionSchedule {
    /// A `period_s` second period divided into `slot_s` second slots.
    ///
    /// A slot length that does not divide the period leaves a quiet tail of
    /// `period_s % slot_s` seconds at the end of every period in which no
    /// shard is due. A slot longer than the period is shortened to the
    /// period, giving a single slot that spans it, so a ticker firing once
    /// per slot still sees every period.
    pub fn new(period_s: u64, slot_s: u64) -> Result<Self, CompactionScheduleError> {
        if period_s == 0 {
            return Err(CompactionScheduleError::ZeroPeriod);
        }
        if slot_s == 0 {
            return Err(CompactionScheduleError::ZeroSlot);
        }
        Ok(Self {
            period_s,
            slot_s: slot_s.min(period_s),
        })
    }

    pub fn period_s(&self) -> u64 {
        self.period_s
    }

    /// Slot length in seconds, never longer than the period.
    pub fn slot_s(&self) -> u64 {
        self.slot_s
    }

    /// Number of slots in a period, at least one.
    pub fn slot_count(&self) -> u64 {
        self.period_s / self.slot_s
    }

    /// Seconds at the end of each period in which no slot is active.
    pub fn tail_s(&self) -> u64 {
        self.period_s
            .saturating_sub(self.slot_count() * self.slot_s)
    }

    /// The slot a shard is due in, in every period.
    ///
    /// The slot is `xxh64(shard uuid bytes) % slot_count`. Shard ids are
    /// unique within a cluster, so the id alone spreads a cluster's shards
    /// across the period; the cluster prefix is the same for every shard a
    /// node owns and would add nothing to the spread, so it is left out. The
    /// hash is the one tenant routing uses, so the assignment depends only on
    /// the shard id and the slot count.
    pub fn slot_for(&self, shard: ShardId) -> u64 {
        xxhash_rust::xxh64::xxh64(shard.as_uuid().as_bytes(), 0) % self.slot_count()
    }

    /// How long after `now` (time since the Unix epoch) the next midpoint of
    /// a slot falls. A ticker started there and firing once per slot stays
    /// clear of slot boundaries, so timer jitter cannot land a tick in the
    /// wrong slot.
    pub fn delay_until_slot_midpoint(&self, now: Duration) -> Duration {
        let slot_ms = u128::from(self.slot_s) * 1000;
        let into_slot = now.as_millis() % slot_ms;
        let midpoint = slot_ms / 2;
        let delay_ms = (midpoint + slot_ms - into_slot) % slot_ms;
        Duration::from_millis(u64::try_from(delay_ms).unwrap_or(u64::MAX))
    }

    /// The period and slot that `now_unix_s` seconds since the Unix epoch
    /// falls in, or `None` in the tail of a period that the slots do not
    /// cover. Periods are counted from the epoch, so every node computes the
    /// same position from the same wall-clock time.
    pub fn position(&self, now_unix_s: u64) -> Option<SlotPosition> {
        let slot = (now_unix_s % self.period_s) / self.slot_s;
        (slot < self.slot_count()).then_some(SlotPosition {
            period: now_unix_s / self.period_s,
            slot,
        })
    }
}

/// Where in the schedule a point in time falls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotPosition {
    /// Periods elapsed since the Unix epoch.
    pub period: u64,
    /// Slot within that period, below the schedule's slot count.
    pub slot: u64,
}

/// A shard the tracker selected for submission on a tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DueShard {
    pub shard: ShardId,
    pub period: u64,
    pub slot: u64,
}

/// Remembers the last period each shard was submitted in, so a shard is
/// submitted at most once per period on this node however many ticks land
/// in its slot and whenever it opens within that slot.
#[derive(Debug, Default)]
pub struct SubmissionTracker {
    last_period: HashMap<ShardId, u64>,
}

impl SubmissionTracker {
    /// The owned shards to submit on a tick at `now_unix_s`: those whose slot
    /// is the current one and that have not been submitted in the current
    /// period. Each returned shard is recorded as submitted for the period,
    /// and entries from earlier periods are dropped.
    pub fn due(
        &mut self,
        schedule: &CompactionSchedule,
        now_unix_s: u64,
        owned: impl IntoIterator<Item = ShardId>,
    ) -> Vec<DueShard> {
        let Some(SlotPosition { period, slot }) = schedule.position(now_unix_s) else {
            return Vec::new();
        };
        self.last_period.retain(|_, last| *last >= period);
        let mut due = Vec::new();
        for shard in owned {
            if schedule.slot_for(shard) != slot {
                continue;
            }
            if self.last_period.get(&shard) == Some(&period) {
                continue;
            }
            self.last_period.insert(shard, period);
            due.push(DueShard {
                shard,
                period,
                slot,
            });
        }
        due
    }
}
