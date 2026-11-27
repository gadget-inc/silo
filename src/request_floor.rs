//! Where a read of a concurrency queue's request rows starts.
//!
//! Granting or cancelling a request deletes its row, and SlateDB keeps the
//! delete as a tombstone until a full compaction. A scan that opens at the
//! queue's prefix steps over every tombstone ahead of the first live row.
//! [`RequestFloors`] holds, per queue, a floor: a key below which every
//! request row is dead apart from the rows whose keys it has noted. A
//! [`RequestRowCursor`] reads the noted keys below the floor one by one and
//! opens its scan at the floor.
//!
//! Everything rests on one invariant:
//!
//! > A live request row whose key sorts below its queue's floor has its key
//! > noted. A request row whose write has not settled has its key noted,
//! > wherever it sorts.
//!
//! A write settles when its batch is confirmed committed, or
//! [`RequestFloorConfig::abandon_grace`] after its [`RequestRowWrite`] was
//! dropped without a confirmation. Until then a pass cannot tell a row that
//! does not exist from one its snapshot is too old to show, so the key stays
//! noted and no pass may move the floor past it unseen.
//!
//! A pass moves the floor only over what it proved dead: keys its scan
//! returned and it then deleted, and the gaps between the keys its scan
//! returned, which held no row at the scan's snapshot. A row written into
//! such a gap was not settled when the pass began, so its key is still
//! noted. The floor stops at the first row the pass read and left in place.
//!
//! A scan that opened at the prefix and ran to the end of the queue's rows,
//! with every row it returned dead, proved the whole queue dead at its
//! snapshot, and by the same argument any key is then a sound floor. A pass
//! that dropped the floor for a rebuild puts it back. A pass that began
//! with no floor and read no row sets one in the last band at the time it
//! began, above every row with an earlier start time. The first row written
//! close under that floor by start time, in any band, lowers it to its key.
//!
//! The floor is dropped every [`RequestFloorConfig::rebuild_interval`], and
//! the pass that drops it reads from the prefix. A row written without a
//! [`RequestRowWrite`] therefore waits at most one interval.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use slatedb::bytes::Bytes;

use crate::instrumented_db::{InstrumentedDb, InstrumentedDbIterator};
use crate::keys::{
    concurrency_request_band_start, concurrency_request_key_order, concurrency_request_order_start,
    concurrency_request_prefix, end_bound,
};
use crate::metrics::{Metrics, RequestNotedReadOutcome};

/// Settings of a shard's [`RequestFloors`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestFloorConfig {
    /// Whether floors are kept at all. When false every read starts at the
    /// queue's prefix and writes are not tracked.
    pub enabled: bool,
    /// A write that lands below the floor, in the floor's band and at most
    /// this much start time under it, lowers the floor to its key. A write
    /// further below, or in another band, leaves the floor and is read by
    /// key. A write in another band counts as [`OTHER_BAND_LAG_MS`] below,
    /// so that value or more lowers the floor for every write below it.
    /// Under a floor set by a pass that found the whole queue dead, a write
    /// in any band counts by its start time.
    pub min_lag_ms: u64,
    /// Most keys a queue keeps noted below its floor. A write that would
    /// exceed it lowers the floor to its key instead.
    pub noted_max_keys: usize,
    /// How long a queue's floor is used before a pass drops it and reads
    /// from the prefix. Zero drops it on every pass. That pass is the only
    /// one that finds a row whose write committed more than
    /// [`Self::abandon_grace`] after its [`RequestRowWrite`] was dropped,
    /// so an interval too long to elapse leaves such a row unread.
    pub rebuild_interval: Duration,
    /// How long a write whose [`RequestRowWrite`] was dropped unconfirmed
    /// is still treated as possibly committing.
    pub abandon_grace: Duration,
}

/// What the state knows of one request row's write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Note {
    /// The batch holding the row has not been confirmed committed.
    InFlight,
    /// The batch committed. `seq` orders the confirmation against the
    /// starts of passes.
    Committed { seq: u64 },
    /// The write's tracker was dropped unconfirmed at `at`.
    Abandoned { at: Instant },
}

impl Note {
    /// Whether the write settled before a pass that began at `seq` and
    /// `started`: a row it wrote, if one exists, is visible to that pass.
    fn settled_before(self, seq: u64, started: Instant, grace: Duration) -> bool {
        match self {
            Note::InFlight => false,
            Note::Committed { seq: confirmed } => confirmed < seq,
            Note::Abandoned { at } => at.checked_add(grace).is_some_and(|t| t <= started),
        }
    }

    /// Whether the write has settled as of `now`.
    fn settled_at(self, now: Instant, grace: Duration) -> bool {
        match self {
            Note::InFlight => false,
            Note::Committed { .. } => true,
            Note::Abandoned { at } => at.checked_add(grace).is_some_and(|t| t <= now),
        }
    }
}

#[derive(Debug)]
struct QueueFloor {
    /// Every request row of the queue below this key is dead, apart from
    /// rows whose keys are in `noted`. `None` reads from the prefix.
    floor: Option<Vec<u8>>,
    /// Bumped whenever `floor` changes. A pass records a floor only if the
    /// version is the one it started with.
    floor_version: u64,
    /// When the pass that first set the current floor began.
    floor_since: Instant,
    noted: BTreeMap<Vec<u8>, Note>,
    /// Orders pass starts against write confirmations.
    seq: u64,
    /// Passes in flight: the `seq` each began at, and when.
    passes: BTreeMap<u64, Instant>,
}

impl QueueFloor {
    fn new(now: Instant) -> Self {
        Self {
            floor: None,
            floor_version: 0,
            floor_since: now,
            noted: BTreeMap::new(),
            seq: 0,
            passes: BTreeMap::new(),
        }
    }

    fn below_floor(&self, key: &[u8]) -> bool {
        self.floor.as_deref().is_some_and(|floor| key < floor)
    }

    fn noted_below_floor(&self) -> impl Iterator<Item = &Vec<u8>> {
        let end = match self.floor.as_deref() {
            Some(floor) => Bound::Excluded(floor),
            None => Bound::Excluded(&[][..]),
        };
        self.noted
            .range::<[u8], _>((Bound::Unbounded, end))
            .map(|(key, _)| key)
    }

    /// Holds nothing a later read needs.
    fn is_empty(&self) -> bool {
        self.floor.is_none() && self.noted.is_empty() && self.passes.is_empty()
    }

    /// Drop the notes outside the floor that no pass still needs: those
    /// settled before the oldest pass in flight began, or before `now` when
    /// none is. Such a row is visible to every pass that could move the
    /// floor, and a pass moves the floor past a row it sees only by
    /// deleting it.
    fn prune_outside_floor(&mut self, now: Instant, grace: Duration) {
        let oldest = self
            .passes
            .iter()
            .next()
            .map(|(seq, started)| (*seq, *started));
        let floor = self.floor.as_deref();
        self.noted.retain(|key, note| {
            if floor.is_some_and(|floor| key.as_slice() < floor) {
                return true;
            }
            let settled = match oldest {
                Some((seq, started)) => note.settled_before(seq, started, grace),
                None => note.settled_at(now, grace),
            };
            !settled
        });
    }
}

#[derive(Debug, Default)]
struct Inner {
    /// Keyed by the queue's request prefix, so iteration is in key order.
    queues: BTreeMap<Vec<u8>, QueueFloor>,
    /// Noted keys across every queue.
    noted_total: usize,
}

/// The state one grant scanner pass took from its queue when it began.
#[derive(Debug)]
pub struct PassStart {
    seq: u64,
    started: Instant,
    floor: Option<Vec<u8>>,
    floor_version: u64,
    noted_below: Vec<Vec<u8>>,
    rebuild: Option<Rebuild>,
}

impl PassStart {
    /// The floor the pass reads from. `None` reads from the prefix.
    pub fn floor(&self) -> Option<&[u8]> {
        self.floor.as_deref()
    }

    /// The noted keys below the floor, ascending.
    pub fn noted_below(&self) -> &[Vec<u8>] {
        &self.noted_below
    }

    /// Whether the pass dropped its queue's floor to read from the prefix.
    pub fn is_rebuild(&self) -> bool {
        self.rebuild.is_some()
    }
}

/// The floor a rebuilding pass dropped, and the keys noted below it then.
#[derive(Debug)]
struct Rebuild {
    floor: Vec<u8>,
    noted: BTreeSet<Vec<u8>>,
}

/// The request floors of one shard's concurrency queues.
pub struct RequestFloors {
    shard: String,
    config: RequestFloorConfig,
    metrics: Option<Metrics>,
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for RequestFloors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestFloors")
            .field("shard", &self.shard)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl RequestFloors {
    pub fn new(
        shard: impl Into<String>,
        config: RequestFloorConfig,
        metrics: Option<Metrics>,
    ) -> Self {
        Self {
            shard: shard.into(),
            config,
            metrics,
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn config(&self) -> &RequestFloorConfig {
        &self.config
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Note a request row about to be written under `request_key`, whose
    /// first `prefix_len` bytes are its queue's request prefix. Call it
    /// before the batch holding the row commits, keep the returned tracker
    /// alive across the commit, and [`RequestRowWrite::confirm`] it once the
    /// commit has succeeded.
    pub fn note_write(
        self: &Arc<Self>,
        prefix_len: usize,
        request_key: Vec<u8>,
    ) -> RequestRowWrite {
        if !self.config.enabled || request_key.len() < prefix_len {
            return RequestRowWrite::default();
        }
        let now = Instant::now();
        let mut overflowed = false;
        {
            let mut guard = self.lock();
            let Inner {
                queues,
                noted_total,
            } = &mut *guard;
            let state = queues
                .entry(request_key[..prefix_len].to_vec())
                .or_insert_with(|| QueueFloor::new(now));
            if state
                .noted
                .insert(request_key.clone(), Note::InFlight)
                .is_none()
            {
                *noted_total += 1;
            }
            if let Some(floor) = state.floor.as_deref()
                && request_key.as_slice() < floor
            {
                let close =
                    lag_below_floor_ms(floor, &request_key, prefix_len) <= self.config.min_lag_ms;
                let max_keys = self.config.noted_max_keys;
                let full = !close
                    && state.noted.len() > max_keys
                    && state.noted_below_floor().nth(max_keys).is_some();
                if close || full {
                    state.floor = Some(request_key.clone());
                    state.floor_version += 1;
                    overflowed = full;
                }
            }
        }
        if overflowed && let Some(ref m) = self.metrics {
            m.record_concurrency_request_noted_overflow(&self.shard);
        }
        RequestRowWrite {
            inner: Some(TrackedWrite {
                floors: Arc::clone(self),
                prefix_len,
                key: request_key,
                confirmed: AtomicBool::new(false),
            }),
        }
    }

    fn confirm_write(&self, prefix_len: usize, key: &[u8]) {
        let mut guard = self.lock();
        let Inner {
            queues,
            noted_total,
        } = &mut *guard;
        let queue = &key[..prefix_len.min(key.len())];
        let Some(state) = queues.get_mut(queue) else {
            return;
        };
        if !state.noted.contains_key(key) {
            return;
        }
        if state.passes.is_empty() && !state.below_floor(key) {
            // Every later pass sees the row and reads it from the scan.
            state.noted.remove(key);
            *noted_total = noted_total.saturating_sub(1);
            if state.is_empty() {
                queues.remove(queue);
            }
        } else {
            state.seq += 1;
            let seq = state.seq;
            if let Some(note) = state.noted.get_mut(key) {
                *note = Note::Committed { seq };
            }
        }
    }

    fn abandon_write(&self, prefix_len: usize, key: &[u8]) {
        let abandoned = {
            let mut guard = self.lock();
            let queue = &key[..prefix_len.min(key.len())];
            match guard
                .queues
                .get_mut(queue)
                .and_then(|state| state.noted.get_mut(key))
            {
                Some(note) => {
                    *note = Note::Abandoned { at: Instant::now() };
                    true
                }
                None => false,
            }
        };
        if abandoned && let Some(ref m) = self.metrics {
            m.record_concurrency_request_write_abandoned(&self.shard);
        }
    }

    /// Begin a pass over the queue whose request prefix is `queue`. Every
    /// read of the pass must follow this call, so that a write confirmed
    /// before it is visible to them.
    pub fn begin_pass(&self, queue: &[u8], now: Instant) -> PassStart {
        let mut guard = self.lock();
        let state = guard
            .queues
            .entry(queue.to_vec())
            .or_insert_with(|| QueueFloor::new(now));
        let mut rebuild = None;
        if state.floor.is_some()
            && now.saturating_duration_since(state.floor_since) >= self.config.rebuild_interval
        {
            let noted = state.noted_below_floor().cloned().collect();
            if let Some(floor) = state.floor.take() {
                rebuild = Some(Rebuild { floor, noted });
            }
            state.floor_version += 1;
        }
        state.seq += 1;
        let seq = state.seq;
        state.passes.insert(seq, now);
        PassStart {
            seq,
            started: now,
            floor: state.floor.clone(),
            floor_version: state.floor_version,
            noted_below: state.noted_below_floor().cloned().collect(),
            rebuild,
        }
    }

    /// A pass read the noted key `key` and found no row. Returns whether
    /// the key was retired: it is when the key's write settled before the
    /// pass began, so the row was deleted or never written. Otherwise the
    /// key stays noted for a later pass.
    pub fn noted_row_missing(&self, queue: &[u8], key: &[u8], pass: &PassStart) -> bool {
        let mut guard = self.lock();
        let Inner {
            queues,
            noted_total,
        } = &mut *guard;
        let Some(state) = queues.get_mut(queue) else {
            return true;
        };
        match state.noted.get(key) {
            None => true,
            Some(note)
                if note.settled_before(pass.seq, pass.started, self.config.abandon_grace) =>
            {
                state.noted.remove(key);
                *noted_total = noted_total.saturating_sub(1);
                true
            }
            Some(_) => false,
        }
    }

    /// Whether `key` is noted in `queue`.
    pub fn is_noted(&self, queue: &[u8], key: &[u8]) -> bool {
        self.lock()
            .queues
            .get(queue)
            .is_some_and(|state| state.noted.contains_key(key))
    }

    /// End `pass`. `consumed` are the request keys the pass proved dead: it
    /// committed their deletes, or read them back as gone. `proven_floor`
    /// is the key up to which the pass's scan, opened at the floor it began
    /// with, found nothing but keys in `consumed`.
    pub fn finish_pass<'k>(
        &self,
        queue: &[u8],
        pass: &PassStart,
        consumed: impl IntoIterator<Item = &'k [u8]>,
        proven_floor: Option<Vec<u8>>,
        now: Instant,
    ) {
        let noted_keys = {
            let mut guard = self.lock();
            let Inner {
                queues,
                noted_total,
            } = &mut *guard;
            let Some(state) = queues.get_mut(queue) else {
                return;
            };
            let noted_before = state.noted.len();
            state.passes.remove(&pass.seq);
            for key in consumed {
                state.noted.remove(key);
            }
            if let Some(mut floor) = proven_floor
                && state.floor_version == pass.floor_version
            {
                // Keep the keys noted below the floor within their limit.
                if state.noted.len() > self.config.noted_max_keys
                    && let Some(cap) = state.noted.keys().nth(self.config.noted_max_keys)
                    && cap.as_slice() < floor.as_slice()
                {
                    floor = cap.clone();
                }
                if state
                    .floor
                    .as_deref()
                    .is_none_or(|current| current < floor.as_slice())
                {
                    if state.floor.is_none() {
                        state.floor_since = pass.started;
                    }
                    state.floor = Some(floor);
                    state.floor_version += 1;
                }
            }
            state.prune_outside_floor(now, self.config.abandon_grace);
            *noted_total = noted_total
                .saturating_add(state.noted.len())
                .saturating_sub(noted_before);
            if state.is_empty() {
                queues.remove(queue);
            }
            *noted_total
        };
        if let Some(ref m) = self.metrics {
            m.set_concurrency_request_noted_keys(&self.shard, noted_keys);
        }
    }

    /// Drop what idle queues have no use for: notes outside the floor whose
    /// writes have settled, and the whole state of a queue with no pass in
    /// flight, no unsettled write, and a floor old enough that its next
    /// pass would drop it.
    pub fn sweep_idle(&self, now: Instant) {
        let noted_keys = {
            let mut guard = self.lock();
            let Inner {
                queues,
                noted_total,
            } = &mut *guard;
            let grace = self.config.abandon_grace;
            let rebuild_interval = self.config.rebuild_interval;
            queues.retain(|_, state| {
                if !state.passes.is_empty() {
                    return true;
                }
                let noted_before = state.noted.len();
                state.prune_outside_floor(now, grace);
                let floor_expired = state.floor.is_none()
                    || now.saturating_duration_since(state.floor_since) >= rebuild_interval;
                let unsettled = state
                    .noted
                    .values()
                    .any(|note| !note.settled_at(now, grace));
                let keep = unsettled || !floor_expired;
                let noted_after = if keep { state.noted.len() } else { 0 };
                *noted_total = noted_total
                    .saturating_add(noted_after)
                    .saturating_sub(noted_before);
                keep
            });
            *noted_total
        };
        if let Some(ref m) = self.metrics {
            m.set_concurrency_request_noted_keys(&self.shard, noted_keys);
        }
    }

    /// The floor of `queue` and the keys noted below it, for a read that
    /// records nothing.
    fn snapshot(&self, queue: &[u8]) -> (Option<Vec<u8>>, Vec<Vec<u8>>) {
        let guard = self.lock();
        match guard.queues.get(queue) {
            Some(state) => (
                state.floor.clone(),
                state.noted_below_floor().cloned().collect(),
            ),
            None => (None, Vec::new()),
        }
    }

    /// The floor of the queue whose request prefix is `queue`.
    pub fn floor(&self, queue: &[u8]) -> Option<Vec<u8>> {
        self.lock()
            .queues
            .get(queue)
            .and_then(|state| state.floor.clone())
    }

    /// Every key noted in `queue`, ascending.
    pub fn noted_keys(&self, queue: &[u8]) -> Vec<Vec<u8>> {
        self.lock()
            .queues
            .get(queue)
            .map(|state| state.noted.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// How many queues hold state.
    pub fn queue_count(&self) -> usize {
        self.lock().queues.len()
    }

    /// A cursor for one grant scanner pass over `(tenant, queue)`. The pass
    /// begins here and ends when the cursor is dropped.
    pub fn grant_cursor<'a>(
        &'a self,
        db: &'a InstrumentedDb,
        tenant: &'a str,
        queue: &'a str,
        observer: Option<RequestReadObserver>,
    ) -> RequestRowCursor<'a> {
        let prefix = concurrency_request_prefix(tenant, queue);
        let pass = self
            .config
            .enabled
            .then(|| self.begin_pass(&prefix, Instant::now()));
        let (open_at, noted) = match &pass {
            Some(pass) => (
                pass.floor.clone().unwrap_or_else(|| prefix.clone()),
                pass.noted_below.iter().cloned().collect(),
            ),
            None => (prefix.clone(), VecDeque::new()),
        };
        let began_ms = crate::job_store_shard::now_epoch_ms();
        if let (Some(m), Some(pass)) = (&self.metrics, &pass) {
            let lag_ms = pass
                .floor
                .as_deref()
                .and_then(|floor| concurrency_request_key_order(floor, prefix.len()))
                .map(|(_, start_ms)| {
                    u64::try_from(began_ms)
                        .unwrap_or(0)
                        .saturating_sub(start_ms)
                })
                .unwrap_or(0);
            m.record_concurrency_request_floor_lag(&self.shard, lag_ms);
        }
        let mut cursor = RequestRowCursor::new(
            self,
            db,
            tenant,
            queue,
            prefix,
            RequestReader::Grants,
            pass,
            open_at,
            noted,
            observer,
        );
        cursor.began_ms = began_ms;
        cursor
    }

    /// A cursor that reads `(tenant, queue)`'s rows in the order a grant
    /// pass would and records nothing.
    pub fn peek_cursor<'a>(
        &'a self,
        db: &'a InstrumentedDb,
        tenant: &'a str,
        queue: &'a str,
        observer: Option<RequestReadObserver>,
    ) -> RequestRowCursor<'a> {
        let prefix = concurrency_request_prefix(tenant, queue);
        let (floor, noted) = if self.config.enabled {
            self.snapshot(&prefix)
        } else {
            (None, Vec::new())
        };
        let open_at = floor.unwrap_or_else(|| prefix.clone());
        RequestRowCursor::new(
            self,
            db,
            tenant,
            queue,
            prefix,
            RequestReader::Peek,
            None,
            open_at,
            noted.into(),
            observer,
        )
    }
}

/// The lag of a write below a floor that lies in another band: more than
/// any two start times of one band can differ by.
pub const OTHER_BAND_LAG_MS: u64 = 1 << 56;

/// How far `key` starts below `floor`, in a queue whose request prefix is
/// `prefix_len` bytes long. [`OTHER_BAND_LAG_MS`] when they are in different
/// bands or either has no ordering field. A whole-queue floor lies above
/// every band, so a key's lag below it is by start time whatever its band.
fn lag_below_floor_ms(floor: &[u8], key: &[u8], prefix_len: usize) -> u64 {
    match (
        concurrency_request_key_order(floor, prefix_len),
        concurrency_request_key_order(key, prefix_len),
    ) {
        (Some((floor_band, floor_ms)), Some((band, start_ms)))
            if floor_band == band || is_whole_queue_floor(floor, prefix_len) =>
        {
            floor_ms.saturating_sub(start_ms)
        }
        _ => OTHER_BAND_LAG_MS,
    }
}

/// The floor a pass sets when it began with none and found the whole queue
/// dead: the last band at `began_ms`, above every row with an earlier start
/// time. It is an ordering field alone, which no request row's key is.
fn whole_queue_floor(queue: &[u8], began_ms: i64) -> Vec<u8> {
    concurrency_request_order_start(queue, u8::MAX, began_ms)
}

/// Whether `floor` came from [`whole_queue_floor`].
fn is_whole_queue_floor(floor: &[u8], prefix_len: usize) -> bool {
    prefix_len.checked_add(8) == Some(floor.len())
}

/// The smallest key that sorts after `key`.
fn successor(key: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(key.len() + 1);
    next.extend_from_slice(key);
    next.push(0x00);
    next
}

/// One request row's write, from the moment its key is built until the
/// batch holding it is known to have committed. See
/// [`RequestFloors::note_write`].
///
/// Dropping it unconfirmed tells the state the write was abandoned: the
/// batch failed, was never submitted, or its future was cancelled.
#[derive(Default)]
#[must_use = "a request row's write must be confirmed after its batch commits"]
pub struct RequestRowWrite {
    inner: Option<TrackedWrite>,
}

struct TrackedWrite {
    floors: Arc<RequestFloors>,
    prefix_len: usize,
    key: Vec<u8>,
    confirmed: AtomicBool,
}

impl RequestRowWrite {
    /// The batch holding the row has committed and the row is visible to
    /// reads that start from now on.
    pub fn confirm(&self) {
        if let Some(write) = &self.inner
            && !write.confirmed.swap(true, Ordering::AcqRel)
        {
            write.floors.confirm_write(write.prefix_len, &write.key);
        }
    }

    /// Whether the write is tracked. It is not when floors are disabled.
    pub fn is_tracked(&self) -> bool {
        self.inner.is_some()
    }
}

impl Drop for RequestRowWrite {
    fn drop(&mut self) {
        if let Some(write) = &self.inner
            && !write.confirmed.load(Ordering::Acquire)
        {
            write.floors.abandon_write(write.prefix_len, &write.key);
        }
    }
}

impl std::fmt::Debug for RequestRowWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            Some(write) => f
                .debug_struct("RequestRowWrite")
                .field("key", &write.key)
                .field("confirmed", &write.confirmed.load(Ordering::Acquire))
                .finish(),
            None => f.write_str("RequestRowWrite(untracked)"),
        }
    }
}

/// Which read a [`RequestRowCursor`] served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestReader {
    /// A grant scanner pass.
    Grants,
    /// A look at the queue's first waiter.
    Peek,
}

/// What became of a request row a pass read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowFate {
    /// The pass granted the request and deleted its row.
    Granted,
    /// The row was deleted without a grant: by the pass as stale, or by
    /// another writer before the pass read it back.
    Deleted,
}

/// One move of a cursor's range scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadStep {
    /// The scan opened at this key.
    Open(Vec<u8>),
    /// The scan skipped ahead to this key.
    Seek(Vec<u8>),
    /// The scan returned the row under this key.
    Key(Bytes),
    /// The scan reached the end of the queue's request rows.
    End,
}

/// A key interval a scan advanced across: from where it was positioned to
/// the next key it returned, or to the end of the queue's rows (`None`).
pub type SteppedInterval = (Bound<Vec<u8>>, Option<Vec<u8>>);

/// What one cursor read, reported to a [`RequestReadObserver`] when the
/// cursor is dropped.
#[derive(Debug, Clone)]
pub struct RequestReadTrace {
    pub tenant: String,
    pub queue: String,
    pub reader: RequestReader,
    /// Range scans opened.
    pub scans_opened: u32,
    /// Seeks of an open scan.
    pub seeks: u32,
    /// Noted keys read one by one.
    pub point_reads: u32,
    /// The scan's moves, in order.
    pub steps: Vec<ReadStep>,
}

impl RequestReadTrace {
    /// The key intervals the scan advanced across, each from where it was
    /// positioned to the next key it returned (`None`: the end of the
    /// queue's rows). The scan stepped over every tombstone inside them.
    pub fn stepped_intervals(&self) -> Vec<SteppedInterval> {
        let mut intervals = Vec::new();
        let mut from: Option<Bound<Vec<u8>>> = None;
        for step in &self.steps {
            match step {
                ReadStep::Open(key) | ReadStep::Seek(key) => {
                    from = Some(Bound::Included(key.clone()));
                }
                ReadStep::Key(key) => {
                    if let Some(from) = from.take() {
                        intervals.push((from, Some(key.to_vec())));
                    }
                    from = Some(Bound::Excluded(key.to_vec()));
                }
                ReadStep::End => {
                    if let Some(from) = from.take() {
                        intervals.push((from, None));
                    }
                }
            }
        }
        intervals
    }

    /// How many of `dead` (keys of deleted rows) lie inside the intervals
    /// the scan advanced across.
    pub fn dead_rows_walked(&self, dead: &BTreeSet<Vec<u8>>) -> usize {
        self.stepped_intervals()
            .into_iter()
            .map(|(from, to)| {
                let to = match to {
                    Some(key) => Bound::Excluded(key),
                    None => Bound::Unbounded,
                };
                let empty = match (&from, &to) {
                    (Bound::Included(a) | Bound::Excluded(a), Bound::Excluded(b)) => a >= b,
                    _ => false,
                };
                if empty {
                    0
                } else {
                    dead.range((from, to)).count()
                }
            })
            .sum()
    }
}

/// Receives the trace of every cursor a shard's reads open.
pub type RequestReadObserver = Arc<dyn Fn(RequestReadTrace) + Send + Sync>;

/// A request row a cursor returned.
#[derive(Debug, Clone)]
pub struct RequestRow {
    pub key: Bytes,
    pub value: Bytes,
}

/// One step of a [`RequestRowCursor`]. Each costs one key of the reader's
/// budget.
#[derive(Debug)]
pub enum RequestRead {
    Row(RequestRow),
    /// A noted key was read and no row exists under it.
    Missing,
    /// No request row is left to read.
    End,
}

/// Reads one queue's request rows in key order: the noted keys below the
/// queue's floor one by one, then a range scan opened at the floor.
///
/// A cursor from [`RequestFloors::grant_cursor`] is one grant scanner pass.
/// The pass tells it which rows it proved dead ([`Self::consumed`]), and
/// when the cursor is dropped it moves the floor over the leading run of
/// those.
pub struct RequestRowCursor<'a> {
    floors: &'a RequestFloors,
    db: &'a InstrumentedDb,
    tenant: &'a str,
    queue: &'a str,
    prefix: Vec<u8>,
    reader: RequestReader,
    pass: Option<PassStart>,
    /// Noted keys below the floor still to read, ascending.
    noted: VecDeque<Vec<u8>>,
    /// Noted keys read whose row exists.
    noted_found: Vec<Vec<u8>>,
    iter: Option<InstrumentedDbIterator>,
    /// When the pass began, in epoch milliseconds. Zero for a cursor that
    /// is no pass.
    began_ms: i64,
    /// Where the range scan opens.
    open_at: Vec<u8>,
    /// The scan opens above the floor, so what it reads proves nothing
    /// about the keys just above the floor.
    opens_past_floor: bool,
    ended: bool,
    scans_opened: u32,
    seeks: u32,
    point_reads: u32,
    steps: Vec<ReadStep>,
    consumed: BTreeMap<Vec<u8>, RowFate>,
    /// Rows a rebuilding pass found below the dropped floor that were not
    /// noted.
    unnoted_rows: u64,
    observer: Option<RequestReadObserver>,
}

impl<'a> RequestRowCursor<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        floors: &'a RequestFloors,
        db: &'a InstrumentedDb,
        tenant: &'a str,
        queue: &'a str,
        prefix: Vec<u8>,
        reader: RequestReader,
        pass: Option<PassStart>,
        open_at: Vec<u8>,
        noted: VecDeque<Vec<u8>>,
        observer: Option<RequestReadObserver>,
    ) -> Self {
        Self {
            floors,
            db,
            tenant,
            queue,
            prefix,
            reader,
            pass,
            noted,
            noted_found: Vec::new(),
            iter: None,
            began_ms: 0,
            open_at,
            opens_past_floor: false,
            ended: false,
            scans_opened: 0,
            seeks: 0,
            point_reads: 0,
            steps: Vec::new(),
            consumed: BTreeMap::new(),
            unnoted_rows: 0,
            observer,
        }
    }

    /// Read the next key. An error leaves the cursor where it was; the
    /// reader should stop.
    pub async fn next(&mut self) -> Result<RequestRead, slatedb::Error> {
        if self.ended {
            return Ok(RequestRead::End);
        }
        if let Some(key) = self.noted.pop_front() {
            self.point_reads += 1;
            return match self.db.get(&key).await? {
                Some(value) => {
                    self.noted_found.push(key.clone());
                    Ok(RequestRead::Row(RequestRow {
                        key: Bytes::from(key),
                        value,
                    }))
                }
                None => {
                    if let Some(pass) = &self.pass {
                        let retired = self.floors.noted_row_missing(&self.prefix, &key, pass);
                        if let Some(ref m) = self.floors.metrics {
                            m.record_concurrency_request_noted_reads(
                                &self.floors.shard,
                                if retired {
                                    RequestNotedReadOutcome::Missing
                                } else {
                                    RequestNotedReadOutcome::Pending
                                },
                                1,
                            );
                        }
                    }
                    Ok(RequestRead::Missing)
                }
            };
        }
        if self.iter.is_none() {
            let end = end_bound(&self.prefix);
            let iter = self
                .db
                .scan_with_options::<Vec<u8>, _>(self.open_at.clone()..end, &crate::scan_options())
                .await?;
            self.scans_opened += 1;
            self.steps.push(ReadStep::Open(self.open_at.clone()));
            self.iter = Some(iter);
        }
        let Some(iter) = self.iter.as_mut() else {
            return Ok(RequestRead::End);
        };
        match iter.next().await? {
            Some(kv) => {
                self.steps.push(ReadStep::Key(kv.key.clone()));
                if let Some(rebuild) = self.pass.as_ref().and_then(|pass| pass.rebuild.as_ref())
                    && kv.key.as_ref() < rebuild.floor.as_slice()
                    && !rebuild.noted.contains(kv.key.as_ref())
                    && !self.floors.is_noted(&self.prefix, &kv.key)
                {
                    self.unnoted_rows += 1;
                    tracing::warn!(
                        tenant = %self.tenant,
                        queue = %self.queue,
                        "request row below its queue's floor was not noted"
                    );
                }
                Ok(RequestRead::Row(RequestRow {
                    key: kv.key,
                    value: kv.value,
                }))
            }
            None => {
                self.steps.push(ReadStep::End);
                self.ended = true;
                Ok(RequestRead::End)
            }
        }
    }

    /// Leave `band`: nothing more of it is read. Returns whether a later
    /// band may hold rows.
    pub async fn skip_band(&mut self, band: u8) -> Result<bool, slatedb::Error> {
        let Some(next_band) = band.checked_add(1) else {
            self.ended = true;
            return Ok(false);
        };
        let next_start = concurrency_request_band_start(self.tenant, self.queue, next_band);
        while self
            .noted
            .front()
            .is_some_and(|key| key.as_slice() < next_start.as_slice())
        {
            self.noted.pop_front();
        }
        match self.iter.as_mut() {
            Some(iter) => {
                iter.seek(&next_start).await?;
                self.seeks += 1;
                self.steps.push(ReadStep::Seek(next_start));
            }
            None => {
                if next_start > self.open_at {
                    self.open_at = next_start;
                    self.opens_past_floor = true;
                }
            }
        }
        Ok(true)
    }

    /// The row under `key`, which this cursor returned, is dead: the delete
    /// the reader wrote for it has committed, or the reader read it back as
    /// gone.
    pub fn consumed(&mut self, key: Vec<u8>, fate: RowFate) {
        self.consumed.insert(key, fate);
    }

    /// The key up to which `pass`'s scan found nothing but rows now dead.
    /// `None` when the scan proved nothing from the floor up.
    ///
    /// It is the first key the scan returned that was not consumed, or just
    /// past the last key it returned when every one was. A scan that also
    /// ran to the end of the queue's rows from the prefix proved the whole
    /// queue dead, so a higher key is as sound: the floor `pass` dropped
    /// for a rebuild, or, for a pass that began with no floor and read no
    /// row, a [`whole_queue_floor`].
    fn proven_floor(&self, pass: &PassStart) -> Option<Vec<u8>> {
        if self.opens_past_floor {
            return None;
        }
        let mut last: Option<&Bytes> = None;
        let mut reached_end = false;
        for step in &self.steps {
            match step {
                ReadStep::Open(_) => {}
                ReadStep::Key(key) => {
                    if !self.consumed.contains_key(key.as_ref()) {
                        return Some(key.to_vec());
                    }
                    last = Some(key);
                }
                ReadStep::Seek(_) => break,
                ReadStep::End => {
                    reached_end = true;
                    break;
                }
            }
        }
        let past_last = last.map(|key| successor(key));
        if !reached_end || pass.floor.is_some() {
            return past_last;
        }
        // The scan ran from the prefix to the end: the whole queue is dead.
        let over_dead_queue = match &pass.rebuild {
            Some(rebuild) => Some(rebuild.floor.clone()),
            None if past_last.is_none() => Some(whole_queue_floor(&self.prefix, self.began_ms)),
            None => None,
        };
        past_last.max(over_dead_queue)
    }
}

impl Drop for RequestRowCursor<'_> {
    fn drop(&mut self) {
        if let Some(pass) = self.pass.take() {
            let proven_floor = self.proven_floor(&pass);
            self.floors.finish_pass(
                &self.prefix,
                &pass,
                self.consumed.keys().map(Vec::as_slice),
                proven_floor,
                Instant::now(),
            );
            if let Some(ref m) = self.floors.metrics {
                let (mut granted, mut deleted, mut left) = (0u64, 0u64, 0u64);
                for key in &self.noted_found {
                    match self.consumed.get(key) {
                        Some(RowFate::Granted) => granted += 1,
                        Some(RowFate::Deleted) => deleted += 1,
                        None => left += 1,
                    }
                }
                let shard = &self.floors.shard;
                for (outcome, count) in [
                    (RequestNotedReadOutcome::Granted, granted),
                    (RequestNotedReadOutcome::Deleted, deleted),
                    (RequestNotedReadOutcome::Left, left),
                ] {
                    m.record_concurrency_request_noted_reads(shard, outcome, count);
                }
                m.record_concurrency_request_floor_rebuild_rows(shard, self.unnoted_rows);
            }
        }
        if let Some(observer) = self.observer.take() {
            observer(RequestReadTrace {
                tenant: self.tenant.to_string(),
                queue: self.queue.to_string(),
                reader: self.reader,
                scans_opened: self.scans_opened,
                seeks: self.seeks,
                point_reads: self.point_reads,
                steps: std::mem::take(&mut self.steps),
            });
        }
    }
}
