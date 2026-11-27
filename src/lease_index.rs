//! In-memory index of a shard's leases, ordered by expiry.
//!
//! The durable lease row is the authority on whether a lease has expired. An
//! entry only says when the reaper should read that row: an entry that is
//! early or left behind costs one point read, while a lease with no entry
//! waits for the reaper's periodic scan of the whole lease range. Writers
//! therefore add or lower an entry before their write commits
//! ([`LeaseIndex::expect_written`]), and raise or remove one only after it
//! has ([`LeaseIndex::confirm_written`], [`LeaseIndex::release`]).
//!
//! A scan of the whole lease range reads rows from a snapshot, so a row it
//! yields may have been deleted since. While a [`FullScan`] is open the
//! index remembers which leases were released, and the scan adds no entry
//! for those.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// A lease whose indexed expiry has passed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueLease {
    pub task_id: String,
    /// The expiry the index held when the lease was reported due.
    pub expiry_ms: i64,
}

/// What the index knows of a lease whose row a full scan read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScannedLease {
    /// The index holds an entry for the lease.
    Tracked,
    /// The lease was released after the scan began and the index holds no
    /// entry for it, so the row the scan read has been deleted.
    Released,
    /// The index has not heard of the lease.
    Untracked,
}

#[derive(Debug, Default)]
pub struct LeaseIndex {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    expiry_by_task: HashMap<Arc<str>, i64>,
    by_expiry: BTreeSet<(i64, Arc<str>)>,
    last_full_scan_ms: Option<i64>,
    /// How many [`FullScan`]s are open.
    open_scans: usize,
    /// Leases released while a full scan was open.
    released_during_scan: HashSet<Arc<str>>,
}

impl Inner {
    fn set(&mut self, task_id: &str, expiry_ms: i64) -> bool {
        if let Some((task, current)) = self.expiry_by_task.get_key_value(task_id) {
            let (task, current) = (Arc::clone(task), *current);
            if current != expiry_ms {
                self.by_expiry.remove(&(current, Arc::clone(&task)));
                self.by_expiry.insert((expiry_ms, Arc::clone(&task)));
                self.expiry_by_task.insert(task, expiry_ms);
            }
            return true;
        }
        let task: Arc<str> = Arc::from(task_id);
        self.by_expiry.insert((expiry_ms, Arc::clone(&task)));
        self.expiry_by_task.insert(task, expiry_ms);
        false
    }

    /// Drop the entry for `task_id` if `matches` its expiry, and remember
    /// the release for any full scan that is open.
    fn remove(&mut self, task_id: &str, matches: impl FnOnce(i64) -> bool) {
        let entry = self
            .expiry_by_task
            .get_key_value(task_id)
            .map(|(task, expiry_ms)| (Arc::clone(task), *expiry_ms));
        if self.open_scans > 0 {
            let task = entry
                .as_ref()
                .map_or_else(|| Arc::from(task_id), |(task, _)| Arc::clone(task));
            self.released_during_scan.insert(task);
        }
        let Some((task, expiry_ms)) = entry else {
            return;
        };
        if matches(expiry_ms) {
            self.by_expiry.remove(&(expiry_ms, Arc::clone(&task)));
            self.expiry_by_task.remove(&task);
        }
    }

    fn scanned(&self, task_id: &str) -> ScannedLease {
        if self.expiry_by_task.contains_key(task_id) {
            ScannedLease::Tracked
        } else if self.released_during_scan.contains(task_id) {
            ScannedLease::Released
        } else {
            ScannedLease::Untracked
        }
    }
}

/// A scan of the whole lease range in progress. While one is open the index
/// records every release, so the scan can tell a lease it has not heard of
/// from one whose row was deleted after the scan's snapshot. Open it before
/// the scan's iterator.
#[derive(Debug)]
pub struct FullScan<'a> {
    index: &'a LeaseIndex,
}

impl FullScan<'_> {
    /// What the index knows of a lease whose row the scan read.
    pub fn lookup(&self, task_id: &str) -> ScannedLease {
        self.index.lock().scanned(task_id)
    }

    /// Add a live lease row the scan read. A lease the index already holds
    /// keeps the entry its writer gave it, and a lease released since the
    /// scan began gets none. Returns whether an entry was added.
    pub fn seed(&self, task_id: &str, expiry_ms: i64) -> bool {
        let mut inner = self.index.lock();
        if inner.scanned(task_id) != ScannedLease::Untracked {
            return false;
        }
        inner.set(task_id, expiry_ms);
        true
    }
}

impl Drop for FullScan<'_> {
    fn drop(&mut self) {
        let mut inner = self.index.lock();
        inner.open_scans = inner.open_scans.saturating_sub(1);
        if inner.open_scans == 0 {
            inner.released_during_scan = HashSet::new();
        }
    }
}

impl LeaseIndex {
    /// The state is plain data that every method leaves consistent, so a
    /// lock poisoned by a panicking holder is still safe to read.
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Note a lease row about to be written with `expiry_ms`. Call before the
    /// write commits. An entry that already expires earlier is kept, so the
    /// reaper never looks later than a row that may still be durable needs.
    pub fn expect_written(&self, task_id: &str, expiry_ms: i64) {
        let mut inner = self.lock();
        let earliest = inner
            .expiry_by_task
            .get(task_id)
            .map_or(expiry_ms, |current| expiry_ms.min(*current));
        inner.set(task_id, earliest);
    }

    /// Record `expiry_ms` as the expiry of a durable lease row: a write or
    /// renewal that has committed, or a row just read. Returns whether the
    /// index already held the lease.
    pub fn confirm_written(&self, task_id: &str, expiry_ms: i64) -> bool {
        self.lock().set(task_id, expiry_ms)
    }

    /// Drop the entry for a lease row that was deleted, or found absent,
    /// while the index held `expiry_ms` for it. An entry holding any other
    /// expiry belongs to a newer write of the same task id and stays.
    pub fn release(&self, task_id: &str, expiry_ms: i64) {
        self.lock()
            .remove(task_id, |indexed_ms| indexed_ms == expiry_ms);
    }

    /// Drop the entry for a lease row that was deleted, whatever expiry the
    /// index holds for it.
    pub fn forget(&self, task_id: &str) {
        self.lock().remove(task_id, |_| true);
    }

    /// Open a scan of the whole lease range. Call before opening the scan's
    /// iterator, so every release the iterator's snapshot can miss is
    /// recorded.
    pub fn begin_full_scan(&self) -> FullScan<'_> {
        self.lock().open_scans += 1;
        FullScan { index: self }
    }

    /// Leases whose indexed expiry is at or before `now_ms`, earliest first.
    pub fn due(&self, now_ms: i64) -> Vec<DueLease> {
        self.lock()
            .by_expiry
            .iter()
            .take_while(|(expiry_ms, _)| *expiry_ms <= now_ms)
            .map(|(expiry_ms, task)| DueLease {
                task_id: task.to_string(),
                expiry_ms: *expiry_ms,
            })
            .collect()
    }

    /// The earliest indexed expiry, if the index holds any lease.
    pub fn next_expiry_ms(&self) -> Option<i64> {
        self.lock()
            .by_expiry
            .first()
            .map(|(expiry_ms, _)| *expiry_ms)
    }

    pub fn len(&self) -> usize {
        self.lock().expiry_by_task.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether a scan of the whole lease range has ever fed the index.
    pub fn seeded(&self) -> bool {
        self.lock().last_full_scan_ms.is_some()
    }

    /// Whether the whole lease range should be scanned at `now_ms`: always
    /// until a first scan has seeded the index, then once per `interval_ms`.
    pub fn full_scan_due(&self, now_ms: i64, interval_ms: i64) -> bool {
        let last_full_scan_ms = self.lock().last_full_scan_ms;
        last_full_scan_ms.is_none_or(|last_ms| now_ms.saturating_sub(last_ms) >= interval_ms)
    }

    /// Record that a scan of the whole lease range started at `now_ms`.
    pub fn record_full_scan(&self, now_ms: i64) {
        self.lock().last_full_scan_ms = Some(now_ms);
    }
}
