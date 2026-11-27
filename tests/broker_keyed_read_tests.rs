//! Tests for reading old task rows by key: a task row whose start time is far
//! behind the current time (a job granted a concurrency slot after waiting
//! keeps its original one) is point-read by the broker's next scan and leaves
//! the scan floor where it was, so the scan does not step over every row
//! written since that start time.
//!
//! Each test counts what the broker read. A test that only checked what ended
//! up buffered would pass whichever way the row was found.

mod test_helpers;

use std::time::{Duration, Instant};

use silo::job::{ConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::metrics::Metrics;
use test_helpers::*;

const HOUR_MS: i64 = 3_600_000;

/// A lag under which no row is old enough to be read by key, so every note
/// lowers the scan floor.
const LOWER_THE_FLOOR: u64 = u64::MAX;

/// A full rescan interval no test run reaches. A full rescan reads the whole
/// range again, which would add to the reads these tests count.
const NO_FULL_RESCAN_MS: u64 = 24 * HOUR_MS as u64;

async fn open_shard(
    keyed_read_min_lag_ms: u64,
) -> (tempfile::TempDir, std::sync::Arc<JobStoreShard>, Metrics) {
    open_temp_shard_with_metrics_and_config(|cfg| {
        cfg.broker_keyed_read_min_lag_ms = keyed_read_min_lag_ms;
        cfg.broker_full_rescan_interval_ms = NO_FULL_RESCAN_MS;
    })
    .await
}

fn queue_of_one() -> Vec<Limit> {
    vec![Limit::Concurrency(ConcurrencyLimit {
        key: "q".to_string(),
        max_concurrency: 1,
    })]
}

async fn enqueue_at(shard: &JobStoreShard, job_id: &str, start_at_ms: i64, limits: Vec<Limit>) {
    shard
        .enqueue(
            "-",
            Some(job_id.to_string()),
            10u8,
            start_at_ms,
            None,
            msgpack_payload(&serde_json::json!({"j": job_id})),
            limits,
            None,
            "default",
        )
        .await
        .expect("enqueue");
}

/// Dequeue up to `max` tasks, returning `(job_id, task_id)` for each.
async fn dequeue(shard: &JobStoreShard, max: usize) -> Vec<(String, String)> {
    shard
        .dequeue("worker", "default", max)
        .await
        .expect("dequeue")
        .tasks
        .iter()
        .map(|t| (t.job().id().to_string(), t.attempt().task_id().to_string()))
        .collect()
}

async fn succeed(shard: &JobStoreShard, task_id: &str) {
    shard
        .report_attempt_outcome(task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report outcome");
}

/// Wait until the broker's buffer holds `len` tasks.
async fn buffered(shard: &JobStoreShard, len: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while shard.broker_buffer_len() != len {
        assert!(
            Instant::now() < deadline,
            "broker buffer holds {} tasks, expected {len}",
            shard.broker_buffer_len()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Wait until the broker's last scan read to the end of its range.
async fn settle(shard: &JobStoreShard) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while shard.broker_next_ready_at_ms("default") != Some(i64::MAX) {
        assert!(
            Instant::now() < deadline,
            "broker never read to the end of its range, next-ready is {:?}",
            shard.broker_next_ready_at_ms("default")
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Wait until the broker's scan floor is above `start_time_ms`.
async fn floor_above(shard: &JobStoreShard, start_time_ms: i64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while shard.broker_scan_floor_ms("default").flatten() <= Some(start_time_ms) {
        assert!(
            Instant::now() < deadline,
            "scan floor stayed at {:?}, at or below {start_time_ms}",
            shard.broker_scan_floor_ms("default")
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn scan_floor_ms(shard: &JobStoreShard) -> i64 {
    shard
        .broker_scan_floor_ms("default")
        .expect("broker exists")
        .expect("floor is set once a scan has read the range")
}

/// What the broker has read so far, from its counters.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Reads {
    /// Rows buffered by a point read of a noted key.
    keyed_inserted: f64,
    /// Rows buffered by a range scan.
    range_inserted: f64,
    /// Rows a range scan read that were already dealt with: buffered, in
    /// flight or under an ack tombstone.
    reread: f64,
}

impl Reads {
    fn of(metrics: &Metrics) -> Self {
        let text = gather_metrics_text(metrics);
        let value = |name: &str, outcome: &str| {
            metric_value_or_zero(
                &text,
                &[
                    name,
                    "task_group=\"default\"",
                    &format!("outcome=\"{outcome}\""),
                ],
            )
        };
        let range = |outcome: &str| value("silo_broker_scan_tasks_read_total", outcome);
        Self {
            keyed_inserted: value("silo_broker_keyed_reads_total", "inserted"),
            range_inserted: range("inserted"),
            reread: range("skipped_already_buffered")
                + range("skipped_inflight")
                + range("skipped_tombstone"),
        }
    }

    fn since(self, earlier: Self) -> Self {
        Self {
            keyed_inserted: self.keyed_inserted - earlier.keyed_inserted,
            range_inserted: self.range_inserted - earlier.range_inserted,
            reread: self.reread - earlier.reread,
        }
    }
}

/// What a broker did when a job that had waited an hour for its concurrency
/// slot was granted it, while a newer task sat in the buffer.
struct GrantOfAnOldJob {
    reads: Reads,
    floor_before_ms: i64,
    floor_after_ms: i64,
    /// Job ids in the order a dequeue returned them once the grant landed.
    claim_order: Vec<String>,
}

async fn grant_an_old_job(keyed_read_min_lag_ms: u64) -> GrantOfAnOldJob {
    let (_tmp, shard, metrics) = open_shard(keyed_read_min_lag_ms).await;

    enqueue_at(&shard, "holder", now_ms(), queue_of_one()).await;
    buffered(&shard, 1).await;
    let held = dequeue(&shard, 1).await;
    assert_eq!(held.len(), 1);

    let bystander_start_ms = now_ms();
    enqueue_at(&shard, "bystander", bystander_start_ms, vec![]).await;
    buffered(&shard, 1).await;
    // Parks on the held queue, keeping its hour-old start time.
    enqueue_at(&shard, "waited", now_ms() - HOUR_MS, queue_of_one()).await;
    // A scan reads the rows that share its floor's millisecond again, so
    // wait for the floor to move past the bystander before counting.
    floor_above(&shard, bystander_start_ms).await;
    settle(&shard).await;
    let floor_before_ms = scan_floor_ms(&shard);
    let before = Reads::of(&metrics);

    // Freeing the slot grants it to the waiting job, whose task row is
    // written at its original start time.
    succeed(&shard, &held[0].1).await;
    buffered(&shard, 2).await;
    settle(&shard).await;
    let reads = Reads::of(&metrics).since(before);
    let floor_after_ms = scan_floor_ms(&shard);

    let claim_order = dequeue(&shard, 10)
        .await
        .into_iter()
        .map(|(job_id, _)| job_id)
        .collect();
    GrantOfAnOldJob {
        reads,
        floor_before_ms,
        floor_after_ms,
        claim_order,
    }
}

/// A grant of a job that waited leaves the scan floor where it was: the
/// granted row is read by its key, the scan that buffers it reads nothing it
/// had already dealt with, and the row is still claimed ahead of newer ones.
#[silo::test]
async fn grant_of_a_job_that_waited_leaves_the_scan_floor_where_it_was() {
    let grant = grant_an_old_job(silo::settings::DEFAULT_BROKER_KEYED_READ_MIN_LAG_MS).await;
    assert_eq!(
        grant.reads,
        Reads {
            keyed_inserted: 1.0,
            range_inserted: 0.0,
            reread: 0.0,
        }
    );
    assert!(
        grant.floor_after_ms >= grant.floor_before_ms,
        "the grant moved the floor from {} back to {}",
        grant.floor_before_ms,
        grant.floor_after_ms
    );
    assert_eq!(grant.claim_order, ["waited", "bystander"]);
}

/// The same grant, noted by lowering the floor to the job's start time,
/// makes the scan read the buffered task again. This is what the test above
/// would see if a path still lowered the floor for an old row.
#[silo::test]
async fn grant_of_a_job_that_waited_rereads_buffered_tasks_when_it_lowers_the_floor() {
    let grant = grant_an_old_job(LOWER_THE_FLOOR).await;
    assert_eq!(grant.reads.keyed_inserted, 0.0);
    assert_eq!(grant.reads.range_inserted, 1.0);
    assert!(
        grant.reads.reread >= 1.0,
        "a scan from the old start time reads the buffered task again"
    );
    assert_eq!(grant.claim_order, ["waited", "bystander"]);
}

/// A task enqueued with a start time far behind the current time is leased
/// promptly without lowering the floor.
#[silo::test]
async fn enqueue_far_behind_is_read_by_key() {
    let (_tmp, shard, metrics) =
        open_shard(silo::settings::DEFAULT_BROKER_KEYED_READ_MIN_LAG_MS).await;
    enqueue_at(&shard, "head", now_ms(), vec![]).await;
    buffered(&shard, 1).await;
    assert_eq!(dequeue(&shard, 1).await.len(), 1);
    settle(&shard).await;
    let floor_before_ms = scan_floor_ms(&shard);
    let before = Reads::of(&metrics);

    enqueue_at(&shard, "early", floor_before_ms - HOUR_MS, vec![]).await;
    assert!(
        scan_floor_ms(&shard) >= floor_before_ms,
        "the enqueue lowered the floor"
    );
    buffered(&shard, 1).await;
    assert_eq!(
        Reads::of(&metrics).since(before),
        Reads {
            keyed_inserted: 1.0,
            range_inserted: 0.0,
            reread: 0.0,
        }
    );
    assert_eq!(dequeue(&shard, 1).await[0].0, "early");
}

const BACKLOG: usize = 30;
const LIVE_PER_GRANT: usize = 5;

/// Drain a concurrency queue of `BACKLOG` hour-old jobs one grant at a time
/// while `LIVE_PER_GRANT` tasks arrive at the current time per grant and
/// stay buffered. Returns, per grant, how many rows the range scans read
/// that they had already dealt with, and the total rows buffered by key.
async fn drain_an_old_backlog(keyed_read_min_lag_ms: u64) -> (Vec<f64>, f64) {
    let (_tmp, shard, metrics) = open_shard(keyed_read_min_lag_ms).await;
    let backlog_start_ms = now_ms() - HOUR_MS;
    for i in 0..BACKLOG {
        enqueue_at(
            &shard,
            &format!("backlog-{i:02}"),
            backlog_start_ms + i as i64,
            queue_of_one(),
        )
        .await;
    }

    let mut reread_per_grant = Vec::with_capacity(BACKLOG);
    let mut before = Reads::of(&metrics);
    for i in 0..BACKLOG {
        for j in 0..LIVE_PER_GRANT {
            enqueue_at(&shard, &format!("live-{i:02}-{j}"), now_ms(), vec![]).await;
        }
        // Every live task so far, and the backlog job that holds the slot.
        buffered(&shard, LIVE_PER_GRANT * (i + 1) + 1).await;
        settle(&shard).await;
        let reads = Reads::of(&metrics);
        reread_per_grant.push(reads.since(before).reread);
        before = reads;

        // A job granted after waiting is claimed ahead of the newer tasks.
        let claimed = dequeue(&shard, 1).await;
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].0, format!("backlog-{i:02}"));
        succeed(&shard, &claimed[0].1).await;
    }
    (reread_per_grant, Reads::of(&metrics).keyed_inserted)
}

/// Draining an old backlog while newer tasks pile up in the buffer costs
/// the same per grant however many tasks have piled up: each granted job's
/// row is read by its key, and the range scans do not go back over the
/// buffered tasks.
#[silo::test]
async fn draining_an_old_backlog_does_not_reread_what_is_buffered() {
    let (reread_per_grant, keyed_inserted) =
        drain_an_old_backlog(silo::settings::DEFAULT_BROKER_KEYED_READ_MIN_LAG_MS).await;
    // The first backlog job takes the free slot as it is enqueued, and the
    // broker's first scan, which reads the whole range, buffers its row.
    assert_eq!(keyed_inserted, (BACKLOG - 1) as f64);

    // The tasks that arrive with a grant are recent, so each lowers the
    // floor to itself and its scan can read the ones that arrived just
    // before it again. That is bounded by the burst, whatever the buffer
    // holds: with 150 tasks buffered the last grant costs what the first did.
    let burst_bound = (LIVE_PER_GRANT * LIVE_PER_GRANT) as f64;
    let worst = reread_per_grant.iter().copied().fold(0.0, f64::max);
    assert!(
        worst <= burst_bound,
        "a grant re-read more than its own burst of arrivals: {reread_per_grant:?}"
    );
}

/// The same drain with every grant lowering the floor re-reads the whole
/// buffer per grant, so the cost per grant grows with the buffer. This is
/// the measurement the test above would fail on.
#[silo::test]
async fn draining_an_old_backlog_rereads_the_buffer_when_grants_lower_the_floor() {
    let (reread_per_grant, keyed_inserted) = drain_an_old_backlog(LOWER_THE_FLOOR).await;
    assert_eq!(keyed_inserted, 0.0);

    let (early, late) = reread_per_grant.split_at(BACKLOG / 2);
    assert!(
        late.iter().sum::<f64>() > early.iter().sum::<f64>() + LIVE_PER_GRANT as f64,
        "rows re-read per grant did not grow with the buffer: {reread_per_grant:?}"
    );
    let last = reread_per_grant[BACKLOG - 1];
    assert!(
        last >= (LIVE_PER_GRANT * (BACKLOG - 1)) as f64,
        "the last grant re-read {last} rows with {} buffered",
        LIVE_PER_GRANT * BACKLOG
    );
}
