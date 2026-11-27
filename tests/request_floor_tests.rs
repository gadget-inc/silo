//! `RequestFloors` and `RequestRowCursor` on their own: the rules for
//! noting writes and moving a queue's floor, and a randomized run of
//! interleaved writers and passes against a real database that checks the
//! floor's invariant after every step.

mod test_helpers;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use silo::instrumented_db::InstrumentedDb;
use silo::keys::{
    concurrency_request_key, concurrency_request_key_order,
    concurrency_request_key_start_time_first, concurrency_request_prefix, end_bound,
    parse_concurrency_request_key,
};
use silo::request_floor::{
    OTHER_BAND_LAG_MS, PassStart, RequestFloorConfig, RequestFloors, RequestRead, RequestRowCursor,
    RequestRowWrite, RowFate,
};
use slatedb::WriteBatch;
use slatedb::config::WriteOptions;

use test_helpers::*;

const TENANT: &str = "-";
const QUEUE: &str = "q";

fn config() -> RequestFloorConfig {
    RequestFloorConfig {
        enabled: true,
        min_lag_ms: 1_000,
        noted_max_keys: 4096,
        rebuild_interval: Duration::from_secs(3600),
        abandon_grace: Duration::from_secs(10),
    }
}

fn floors(config: RequestFloorConfig) -> Arc<RequestFloors> {
    Arc::new(RequestFloors::new("test", config, None))
}

fn prefix() -> Vec<u8> {
    concurrency_request_prefix(TENANT, QUEUE)
}

/// A priority-ordered request key: band `priority`, start `start_ms`.
fn key(priority: u8, start_ms: i64, job: &str) -> Vec<u8> {
    concurrency_request_key(TENANT, QUEUE, start_ms, priority, job, 1, "aaaaaaaa")
}

fn note(floors: &Arc<RequestFloors>, key: &[u8]) -> RequestRowWrite {
    floors.note_write(prefix().len(), key.to_vec())
}

/// Run a pass that consumed `consumed` and proved the queue dead up to
/// `proven_floor`.
fn pass(floors: &RequestFloors, consumed: &[&[u8]], proven_floor: Option<&[u8]>) {
    let now = Instant::now();
    let start = floors.begin_pass(&prefix(), now);
    finish(floors, &start, consumed, proven_floor);
}

fn finish(
    floors: &RequestFloors,
    start: &PassStart,
    consumed: &[&[u8]],
    proven_floor: Option<&[u8]>,
) {
    floors.finish_pass(
        &prefix(),
        start,
        consumed.iter().copied(),
        proven_floor.map(<[u8]>::to_vec),
        Instant::now(),
    );
}

#[silo::test]
fn request_key_order_reads_band_and_start_time() {
    let prefix_len = prefix().len();
    for (priority, start_ms) in [(0u8, 0i64), (0, 1_700_000_000_000), (7, 42), (255, 1 << 40)] {
        let priority_ordered = key(priority, start_ms, "job");
        let parsed = parse_concurrency_request_key(&priority_ordered).unwrap();
        assert_eq!(
            concurrency_request_key_order(&priority_ordered, prefix_len),
            Some((parsed.band, parsed.start_time_ms))
        );
        assert_eq!(parsed.band, priority);

        let start_time_ordered = concurrency_request_key_start_time_first(
            TENANT, QUEUE, start_ms, priority, "job", 1, "aaaaaaaa",
        );
        assert_eq!(
            concurrency_request_key_order(&start_time_ordered, prefix_len),
            Some((0, start_ms as u64))
        );
    }
    assert_eq!(concurrency_request_key_order(&prefix(), prefix_len), None);
}

#[silo::test]
fn a_write_above_the_floor_is_noted_until_it_is_confirmed() {
    let floors = floors(config());
    let floor = key(0, 10_000, "floor");
    pass(&floors, &[], Some(&floor));
    assert_eq!(floors.floor(&prefix()), Some(floor.clone()));

    let above = key(0, 20_000, "above");
    let write = note(&floors, &above);
    assert!(write.is_tracked());
    assert_eq!(floors.noted_keys(&prefix()), vec![above.clone()]);
    assert_eq!(
        floors.floor(&prefix()),
        Some(floor),
        "the floor does not move"
    );

    write.confirm();
    assert!(
        floors.noted_keys(&prefix()).is_empty(),
        "with no pass in flight every later pass sees the row in its scan"
    );
}

#[silo::test]
fn a_write_confirmed_during_a_pass_stays_noted_past_that_pass() {
    let floors = floors(config());
    let racing = floors.begin_pass(&prefix(), Instant::now());

    // The row commits after the pass began, so the pass's scan may not
    // show it, and the pass may consume a later key.
    let row = key(0, 5_000, "row");
    let write = note(&floors, &row);
    write.confirm();
    assert_eq!(floors.noted_keys(&prefix()), vec![row.clone()]);

    let later = key(0, 9_000, "later");
    finish(&floors, &racing, &[&later], Some(&key(0, 9_001, "next")));
    assert!(floors.floor(&prefix()).unwrap() > row);
    assert_eq!(
        floors.noted_keys(&prefix()),
        vec![row.clone()],
        "the floor passed the row unseen, so its key stays noted"
    );

    // The next pass reads the key and finds the row.
    let next = floors.begin_pass(&prefix(), Instant::now());
    assert_eq!(next.noted_below(), std::slice::from_ref(&row));
    finish(&floors, &next, &[&row], None);
    assert!(floors.noted_keys(&prefix()).is_empty());
}

#[silo::test]
fn a_write_just_below_the_floor_lowers_it() {
    let floors = floors(config());
    let floor = key(3, 10_000, "floor");
    pass(&floors, &[], Some(&floor));

    let close = key(3, 9_500, "close");
    let write = note(&floors, &close);
    assert_eq!(floors.floor(&prefix()), Some(close.clone()));
    assert_eq!(
        floors.noted_keys(&prefix()),
        vec![close.clone()],
        "the key is noted as well while its write is in flight"
    );
    write.confirm();
    assert!(floors.noted_keys(&prefix()).is_empty());
}

#[silo::test]
fn a_write_far_below_the_floor_or_in_another_band_is_read_by_key() {
    let floors = floors(config());
    let floor = key(3, 100_000, "floor");
    pass(&floors, &[], Some(&floor));

    let far = key(3, 50_000, "far");
    let other_band = key(1, 99_999, "band");
    let far_write = note(&floors, &far);
    let band_write = note(&floors, &other_band);
    far_write.confirm();
    band_write.confirm();

    assert_eq!(floors.floor(&prefix()), Some(floor));
    let next = floors.begin_pass(&prefix(), Instant::now());
    assert_eq!(
        next.noted_below(),
        [other_band.clone(), far.clone()],
        "both are read by key, in key order"
    );
    finish(&floors, &next, &[], None);
    assert_eq!(
        floors.noted_keys(&prefix()),
        vec![other_band, far],
        "a row read and left in place stays noted"
    );
}

#[silo::test]
fn the_maximum_lag_lowers_the_floor_for_every_write_below_it() {
    let floors = floors(RequestFloorConfig {
        min_lag_ms: OTHER_BAND_LAG_MS,
        ..config()
    });
    pass(&floors, &[], Some(&key(3, 100_000, "floor")));

    let far = key(3, 5, "far");
    note(&floors, &far).confirm();
    assert_eq!(floors.floor(&prefix()), Some(far.clone()));

    let other_band = key(1, 99_999, "band");
    note(&floors, &other_band).confirm();
    assert_eq!(floors.floor(&prefix()), Some(other_band));
}

#[silo::test]
fn a_write_past_the_noted_limit_lowers_the_floor() {
    let floors = floors(RequestFloorConfig {
        noted_max_keys: 2,
        ..config()
    });
    let floor = key(0, 1_000_000, "floor");
    pass(&floors, &[], Some(&floor));

    let first = key(0, 300_000, "a");
    let second = key(0, 200_000, "b");
    note(&floors, &first).confirm();
    note(&floors, &second).confirm();
    assert_eq!(floors.floor(&prefix()), Some(floor));

    let third = key(0, 250_000, "c");
    note(&floors, &third).confirm();
    assert_eq!(
        floors.floor(&prefix()),
        Some(third),
        "the third key would be one too many below the floor"
    );
    let next = floors.begin_pass(&prefix(), Instant::now());
    assert_eq!(next.noted_below(), [second]);
    finish(&floors, &next, &[], None);
}

#[silo::test]
fn a_pass_does_not_raise_the_floor_over_more_noted_keys_than_the_limit() {
    let floors = floors(RequestFloorConfig {
        noted_max_keys: 2,
        ..config()
    });
    let racing = floors.begin_pass(&prefix(), Instant::now());
    let keys: Vec<Vec<u8>> = (0..4)
        .map(|i| key(0, 1_000 + i, &format!("job{i}")))
        .collect();
    let writes: Vec<RequestRowWrite> = keys.iter().map(|key| note(&floors, key)).collect();
    finish(&floors, &racing, &[], Some(&key(0, 900_000, "beyond")));
    assert_eq!(
        floors.floor(&prefix()),
        Some(keys[2].clone()),
        "the floor stops where two noted keys lie below it"
    );
    drop(writes);
}

#[silo::test]
fn a_missing_row_is_retired_only_once_its_write_has_settled() {
    let grace = Duration::from_millis(50);
    let floors = floors(RequestFloorConfig {
        abandon_grace: grace,
        ..config()
    });
    pass(&floors, &[], Some(&key(0, 1_000_000, "floor")));

    // In flight: the row may still commit.
    let row = key(0, 5, "row");
    let write = note(&floors, &row);
    let first = floors.begin_pass(&prefix(), Instant::now());
    assert!(!floors.noted_row_missing(&prefix(), &row, &first));

    // Abandoned while the pass runs: not settled before the pass began.
    drop(write);
    assert!(!floors.noted_row_missing(&prefix(), &row, &first));
    finish(&floors, &first, &[], None);

    // A pass that begins inside the grace period still keeps the key.
    let second = floors.begin_pass(&prefix(), Instant::now());
    assert!(!floors.noted_row_missing(&prefix(), &row, &second));
    finish(&floors, &second, &[], None);
    assert_eq!(floors.noted_keys(&prefix()), vec![row.clone()]);

    // A pass that begins after it retires the key.
    let third = floors.begin_pass(&prefix(), Instant::now() + grace * 2);
    assert_eq!(third.noted_below(), std::slice::from_ref(&row));
    assert!(floors.noted_row_missing(&prefix(), &row, &third));
    finish(&floors, &third, &[], None);
    assert!(floors.noted_keys(&prefix()).is_empty());
}

#[silo::test]
fn a_committed_row_that_is_missing_was_deleted() {
    let floors = floors(config());
    pass(&floors, &[], Some(&key(0, 1_000_000, "floor")));
    let row = key(0, 5, "row");
    note(&floors, &row).confirm();

    let next = floors.begin_pass(&prefix(), Instant::now());
    assert!(floors.noted_row_missing(&prefix(), &row, &next));
    finish(&floors, &next, &[], None);
    assert!(floors.noted_keys(&prefix()).is_empty());
}

#[silo::test]
fn a_pass_that_began_under_another_floor_records_none() {
    let floors = floors(config());
    let floor = key(0, 100_000, "floor");
    pass(&floors, &[], Some(&floor));

    let racing = floors.begin_pass(&prefix(), Instant::now());
    let lowered = key(0, 99_900, "lowered");
    let write = note(&floors, &lowered);
    assert_eq!(floors.floor(&prefix()), Some(lowered.clone()));

    finish(&floors, &racing, &[], Some(&key(0, 200_000, "beyond")));
    assert_eq!(
        floors.floor(&prefix()),
        Some(lowered),
        "the pass read from a floor that a writer has since lowered"
    );
    drop(write);
}

#[silo::test]
fn the_floor_never_moves_down_at_the_end_of_a_pass() {
    let floors = floors(config());
    let floor = key(0, 100_000, "floor");
    pass(&floors, &[], Some(&floor));
    pass(&floors, &[], Some(&key(0, 50_000, "lower")));
    assert_eq!(floors.floor(&prefix()), Some(floor));
}

#[silo::test]
fn a_pass_past_the_rebuild_interval_drops_the_floor() {
    let interval = Duration::from_secs(60);
    let floors = floors(RequestFloorConfig {
        rebuild_interval: interval,
        ..config()
    });
    let t0 = Instant::now();
    let first = floors.begin_pass(&prefix(), t0);
    let floor = key(0, 100_000, "floor");
    finish(&floors, &first, &[], Some(&floor));

    let within = floors.begin_pass(&prefix(), t0 + interval / 2);
    assert!(!within.is_rebuild());
    assert_eq!(within.floor(), Some(floor.as_slice()));

    // A pass in flight across the rebuild records nothing: the floor it
    // would restore is the one being checked.
    let rebuilding = floors.begin_pass(&prefix(), t0 + interval);
    assert!(rebuilding.is_rebuild());
    assert_eq!(rebuilding.floor(), None);
    finish(&floors, &within, &[], Some(&key(0, 200_000, "beyond")));
    assert_eq!(floors.floor(&prefix()), None);

    finish(&floors, &rebuilding, &[], Some(&floor));
    assert_eq!(floors.floor(&prefix()), Some(floor));
    let after = floors.begin_pass(&prefix(), t0 + interval + interval / 2);
    assert!(!after.is_rebuild(), "the interval restarts at the rebuild");
    finish(&floors, &after, &[], None);
}

#[silo::test]
fn a_zero_rebuild_interval_reads_every_pass_from_the_prefix() {
    let floors = floors(RequestFloorConfig {
        rebuild_interval: Duration::ZERO,
        ..config()
    });
    pass(&floors, &[], Some(&key(0, 100_000, "floor")));
    let next = floors.begin_pass(&prefix(), Instant::now());
    assert!(next.is_rebuild());
    assert_eq!(next.floor(), None);
    finish(&floors, &next, &[], None);
}

#[silo::test]
fn idle_queues_are_swept_and_unsettled_writes_are_kept() {
    let interval = Duration::from_secs(60);
    let grace = Duration::from_millis(20);
    let floors = floors(RequestFloorConfig {
        rebuild_interval: interval,
        abandon_grace: grace,
        ..config()
    });
    let t0 = Instant::now();
    let first = floors.begin_pass(&prefix(), t0);
    finish(&floors, &first, &[], Some(&key(0, 100_000, "floor")));
    assert_eq!(floors.queue_count(), 1);

    floors.sweep_idle(t0 + interval / 2);
    assert_eq!(floors.queue_count(), 1, "the floor is still in use");

    let write = note(&floors, &key(0, 500_000, "inflight"));
    floors.sweep_idle(t0 + interval * 2);
    assert_eq!(floors.queue_count(), 1, "a write in flight keeps the state");

    drop(write);
    floors.sweep_idle(Instant::now() + interval * 2);
    assert_eq!(
        floors.queue_count(),
        0,
        "an abandoned write past its grace period holds nothing"
    );
}

#[silo::test]
fn a_queue_with_nothing_to_remember_holds_no_state() {
    let floors = floors(config());
    pass(&floors, &[], None);
    assert_eq!(floors.queue_count(), 0);

    note(&floors, &key(0, 5, "row")).confirm();
    assert_eq!(floors.queue_count(), 0);
}

#[silo::test]
fn disabled_floors_track_nothing() {
    let floors = floors(RequestFloorConfig {
        enabled: false,
        ..config()
    });
    let write = note(&floors, &key(0, 5, "row"));
    assert!(!write.is_tracked());
    write.confirm();
    assert_eq!(floors.queue_count(), 0);
}

// ---------------------------------------------------------------------------
// Randomized run against a real database
// ---------------------------------------------------------------------------

async fn put(db: &InstrumentedDb, key: &[u8]) {
    let mut batch = WriteBatch::new();
    batch.put(key, b"row");
    write(db, batch).await;
}

async fn delete(db: &InstrumentedDb, key: &[u8]) {
    let mut batch = WriteBatch::new();
    batch.delete(key);
    write(db, batch).await;
}

async fn write(db: &InstrumentedDb, batch: WriteBatch) {
    db.write_with_options(
        batch,
        &WriteOptions {
            await_durable: false,
            ..Default::default()
        },
    )
    .await
    .expect("write");
}

async fn prefix_scan(db: &InstrumentedDb) -> Vec<Vec<u8>> {
    let start = prefix();
    let end = end_bound(&start);
    let mut iter = db.scan::<Vec<u8>, _>(start..end).await.expect("scan");
    let mut keys = Vec::new();
    while let Some(kv) = iter.next().await.expect("iterate") {
        keys.push(kv.key.to_vec());
    }
    keys
}

/// A pass in flight in the randomized run.
struct RunningPass<'a> {
    cursor: RequestRowCursor<'a>,
    /// Rows committed and confirmed before the pass began.
    visible_at_begin: BTreeSet<Vec<u8>>,
    returned: Vec<Vec<u8>>,
    skipped_a_band: bool,
    ended: bool,
}

/// What the randomized run knows to be true of the database.
#[derive(Default)]
struct Model {
    /// Rows that exist.
    live: BTreeSet<Vec<u8>>,
    /// Writes noted and neither committed nor abandoned.
    in_flight: BTreeMap<Vec<u8>, RequestRowWrite>,
    /// Writes whose tracker was dropped and whose row may still be applied.
    late: BTreeSet<Vec<u8>>,
}

impl Model {
    /// The invariant the floor rests on: a live row below the floor is
    /// noted, and so is every row whose write has not settled.
    fn check(&self, floors: &RequestFloors, step: &str) {
        let noted: BTreeSet<Vec<u8>> = floors.noted_keys(&prefix()).into_iter().collect();
        if let Some(floor) = floors.floor(&prefix()) {
            for key in self.live.range(..floor.clone()) {
                assert!(
                    noted.contains(key),
                    "after {step}: a live row below the floor is not noted"
                );
            }
        }
        for key in self.in_flight.keys().chain(self.late.iter()) {
            assert!(
                noted.contains(key),
                "after {step}: a row whose write has not settled is not noted"
            );
        }
    }
}

fn random_key(rng: &mut StdRng, serial: &mut u32) -> Vec<u8> {
    *serial += 1;
    let start_ms = 1_000_000 + rng.random_range(0..24) * 250;
    let priority = if rng.random_bool(0.3) { 2 } else { 6 };
    let job = format!("job{serial}");
    let suffix = format!("{:08x}", rng.random::<u32>());
    if rng.random_bool(0.2) {
        concurrency_request_key_start_time_first(
            TENANT, QUEUE, start_ms, priority, &job, 1, &suffix,
        )
    } else {
        concurrency_request_key(TENANT, QUEUE, start_ms, priority, &job, 1, &suffix)
    }
}

/// Interleave writers and passes at random. `late_writes` lets a write be
/// applied after its tracker was dropped, as SlateDB does for a batch whose
/// submitting future is cancelled; the grace period then has to cover it.
async fn randomized_run(seed: u64, config: RequestFloorConfig, late_writes: bool, steps: usize) {
    let (_tmp, shard) = open_temp_shard().await;
    shard.stop_grant_scanner();
    let db = shard.db();
    let floors = floors(config);
    let mut rng = StdRng::seed_from_u64(seed);
    let mut model = Model::default();
    let mut passes: Vec<RunningPass<'_>> = Vec::new();
    let mut serial = 0u32;

    for step in 0..steps {
        let step_name = match rng.random_range(0..100) {
            0..25 => {
                let key = random_key(&mut rng, &mut serial);
                let write = note(&floors, &key);
                model.in_flight.insert(key, write);
                "note"
            }
            25..45 => {
                if let Some(key) = pick(&mut rng, model.in_flight.keys()) {
                    let write = model.in_flight.remove(&key).unwrap();
                    put(db, &key).await;
                    write.confirm();
                    model.live.insert(key);
                }
                "commit"
            }
            45..50 => {
                if let Some(key) = pick(&mut rng, model.in_flight.keys()) {
                    drop(model.in_flight.remove(&key));
                    if late_writes && rng.random_bool(0.5) {
                        model.late.insert(key);
                    }
                }
                "abandon"
            }
            50..53 => {
                if let Some(key) = pick(&mut rng, model.late.iter()) {
                    model.late.remove(&key);
                    put(db, &key).await;
                    model.live.insert(key);
                }
                "late apply"
            }
            53..62 => {
                if passes.len() < 3 {
                    let visible_at_begin = model.live.clone();
                    passes.push(RunningPass {
                        cursor: floors.grant_cursor(db, TENANT, QUEUE, None),
                        visible_at_begin,
                        returned: Vec::new(),
                        skipped_a_band: false,
                        ended: false,
                    });
                }
                "begin pass"
            }
            62..92 => {
                if !passes.is_empty() {
                    let index = rng.random_range(0..passes.len());
                    step_pass(&mut rng, db, &mut model, &mut passes[index]).await;
                }
                "step pass"
            }
            92..98 => {
                if !passes.is_empty() {
                    let index = rng.random_range(0..passes.len());
                    finish_running_pass(&model, passes.swap_remove(index));
                }
                "finish pass"
            }
            _ => {
                floors.sweep_idle(Instant::now());
                "sweep"
            }
        };
        model.check(&floors, &format!("step {step} ({step_name}), seed {seed}"));
    }

    // Settle everything, then every live row must be reachable, in the
    // order a scan from the prefix returns them.
    for pass in passes.drain(..) {
        finish_running_pass(&model, pass);
    }
    for (key, write) in std::mem::take(&mut model.in_flight) {
        put(db, &key).await;
        write.confirm();
        model.live.insert(key);
    }
    for key in std::mem::take(&mut model.late) {
        put(db, &key).await;
        model.live.insert(key);
    }
    model.check(&floors, &format!("settling, seed {seed}"));

    let mut cursor = floors.grant_cursor(db, TENANT, QUEUE, None);
    let mut read = Vec::new();
    loop {
        match cursor.next().await.expect("read") {
            RequestRead::Row(row) => read.push(row.key.to_vec()),
            RequestRead::Missing => {}
            RequestRead::End => break,
        }
    }
    drop(cursor);
    assert_eq!(
        read,
        prefix_scan(db).await,
        "seed {seed}: a pass reads every live row, in key order"
    );
    assert_eq!(read, model.live.iter().cloned().collect::<Vec<_>>());
}

fn pick<'k>(rng: &mut StdRng, keys: impl Iterator<Item = &'k Vec<u8>>) -> Option<Vec<u8>> {
    let keys: Vec<&Vec<u8>> = keys.collect();
    if keys.is_empty() {
        None
    } else {
        Some(keys[rng.random_range(0..keys.len())].clone())
    }
}

/// Read one key with `pass` and deal with it as a grant scanner pass might:
/// consume the row, leave it, or treat it as future-dated and leave its
/// band.
async fn step_pass(
    rng: &mut StdRng,
    db: &InstrumentedDb,
    model: &mut Model,
    pass: &mut RunningPass<'_>,
) {
    if pass.ended {
        return;
    }
    let row = match pass.cursor.next().await.expect("read") {
        RequestRead::Row(row) => row,
        RequestRead::Missing => return,
        RequestRead::End => {
            pass.ended = true;
            return;
        }
    };
    let key = row.key.to_vec();
    if let Some(last) = pass.returned.last() {
        assert!(last < &key, "a pass returns rows in key order");
    }
    pass.returned.push(key.clone());
    let fate = rng.random_range(0..10);
    if fate < 6 {
        // Consume: delete the row if it is still there, as the scanner
        // does after re-reading it, and report it dead either way.
        if model.live.remove(&key) {
            delete(db, &key).await;
            pass.cursor.consumed(key.clone(), RowFate::Granted);
        } else {
            pass.cursor.consumed(key.clone(), RowFate::Deleted);
        }
    }
    // Leave the row's band after either outcome. The scanner does so only
    // for a future-dated row it left; the floor must hold for both.
    if fate == 5 || fate == 9 {
        let (band, _) =
            concurrency_request_key_order(&key, prefix().len()).expect("request key order");
        pass.skipped_a_band = true;
        if !pass.cursor.skip_band(band).await.expect("skip band") {
            pass.ended = true;
        }
    }
}

/// End `pass`. One that read to the end without leaving a band returned
/// every row that was visible when it began and still exists.
fn finish_running_pass(model: &Model, pass: RunningPass<'_>) {
    if pass.ended && !pass.skipped_a_band {
        let returned: BTreeSet<&Vec<u8>> = pass.returned.iter().collect();
        for key in pass.visible_at_begin.intersection(&model.live) {
            assert!(
                returned.contains(key),
                "a pass that read to the end missed a row it could see"
            );
        }
    }
    drop(pass.cursor);
}

#[silo::test]
async fn random_writers_and_passes_keep_every_live_row_reachable() {
    for seed in 0..60 {
        randomized_run(seed, config(), false, 800).await;
    }
}

#[silo::test]
async fn random_writers_and_passes_with_a_small_noted_limit() {
    for seed in 100..140 {
        randomized_run(
            seed,
            RequestFloorConfig {
                noted_max_keys: 3,
                ..config()
            },
            false,
            800,
        )
        .await;
    }
}

#[silo::test]
async fn random_writers_and_passes_with_keyed_reads_off() {
    for seed in 200..230 {
        randomized_run(
            seed,
            RequestFloorConfig {
                min_lag_ms: u64::MAX,
                ..config()
            },
            false,
            800,
        )
        .await;
    }
}

#[silo::test]
async fn random_writers_and_passes_with_writes_applied_after_their_tracker_dropped() {
    for seed in 300..340 {
        randomized_run(seed, config(), true, 800).await;
    }
}

#[silo::test]
async fn random_writers_and_passes_that_rebuild_on_every_pass() {
    for seed in 400..420 {
        randomized_run(
            seed,
            RequestFloorConfig {
                rebuild_interval: Duration::ZERO,
                ..config()
            },
            false,
            800,
        )
        .await;
    }
}

#[silo::test]
async fn random_writers_and_passes_with_abandoned_writes_settling_at_once() {
    for seed in 500..530 {
        randomized_run(
            seed,
            RequestFloorConfig {
                abandon_grace: Duration::ZERO,
                ..config()
            },
            false,
            800,
        )
        .await;
    }
}
