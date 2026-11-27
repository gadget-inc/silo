//! The grant scanner's request floor: a pass reads a concurrency queue's
//! request rows starting past the rows earlier passes granted or deleted,
//! and reads rows written below that point by key.
//!
//! A test that claims a cost counts what the pass's reads touched: scans
//! opened, keys read one by one, and the deleted rows that lie inside the
//! key intervals its scan stepped across ("dead rows walked"). SlateDB
//! keeps a deleted row as a tombstone until compaction and a scan steps over
//! every tombstone in its way, so dead rows walked is the work a pass does
//! that grants nothing.
//!
//! Every test runs with start-time-ordered and with priority-ordered request
//! rows.

mod test_helpers;

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use silo::concurrency::GrantScannerConfig;
use silo::job::{ConcurrencyLimit, FloatingConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::keys::{
    concurrency_request_key, concurrency_request_key_start_time_first, concurrency_request_prefix,
    concurrency_requester_counter_key, end_bound, parse_concurrency_request_key,
};
use silo::request_floor::{RequestReadTrace, RequestReader};
use slatedb::WriteBatch;

use test_helpers::*;

const TENANT: &str = "-";
const TASK_GROUP: &str = "default";

/// Runs `$case(priority_ordered)` once per request row order.
macro_rules! both_orders {
    ($($case:ident),+ $(,)?) => {
        $(
            mod $case {
                #[silo::test]
                async fn start_time_ordered() {
                    crate::$case(false).await;
                }

                #[silo::test]
                async fn priority_ordered() {
                    crate::$case(true).await;
                }
            }
        )+
    };
}

/// A scanner config for tests that count what a pass reads: the floor is
/// never dropped for a rebuild, so no pass reads from the prefix on a timer.
fn config(priority_ordered_requests: bool) -> GrantScannerConfig {
    GrantScannerConfig {
        priority_ordered_requests,
        request_floor_rebuild_interval_ms: u64::MAX,
        ..GrantScannerConfig::default()
    }
}

struct Harness {
    _tmp: tempfile::TempDir,
    shard: Arc<JobStoreShard>,
    metrics: silo::metrics::Metrics,
    traces: Arc<Mutex<Vec<RequestReadTrace>>>,
}

/// One `process_concurrency_grants` call.
struct Pass {
    /// Task groups of the grants, in grant order.
    granted: Vec<String>,
    /// What the pass read. `None` when the pass returned at its capacity
    /// precheck without reading request rows.
    trace: Option<RequestReadTrace>,
}

impl Pass {
    fn trace(&self) -> &RequestReadTrace {
        self.trace.as_ref().expect("the pass read request rows")
    }
}

impl Harness {
    /// Open a shard with the background grant scanner stopped, so the
    /// passes a test runs are the only ones.
    async fn open(config: GrantScannerConfig) -> Self {
        // These tests commit tens of thousands of times, each waiting for
        // the next flush.
        let (tmp, shard, metrics) = open_temp_shard_with_grant_scanner_config_and_slatedb(
            config,
            slatedb::config::Settings {
                flush_interval: Some(Duration::from_millis(1)),
                ..Default::default()
            },
        )
        .await;
        shard.stop_grant_scanner();
        let traces: Arc<Mutex<Vec<RequestReadTrace>>> = Arc::default();
        let sink = Arc::clone(&traces);
        shard.set_request_read_observer_for_test(Some(Arc::new(move |trace| {
            sink.lock().unwrap().push(trace);
        })));
        Self {
            _tmp: tmp,
            shard,
            metrics,
            traces,
        }
    }

    async fn enqueue(&self, priority: u8, start_at_ms: i64, limits: Vec<Limit>) -> String {
        self.enqueue_in(TASK_GROUP, priority, start_at_ms, limits)
            .await
    }

    async fn enqueue_in(
        &self,
        task_group: &str,
        priority: u8,
        start_at_ms: i64,
        limits: Vec<Limit>,
    ) -> String {
        self.shard
            .enqueue(
                TENANT,
                None,
                priority,
                start_at_ms,
                None,
                msgpack_payload(&serde_json::json!({"p": priority})),
                limits,
                None,
                task_group,
            )
            .await
            .expect("enqueue")
    }

    /// Lease the one runnable task of `task_group` and report it
    /// successful, which releases every slot its job holds. Returns the
    /// job's id.
    async fn complete_one_in(&self, task_group: &str) -> String {
        // A floating limit's refresh task can take a dequeue's one slot;
        // ask again until the job's own task comes back.
        let mut tasks = Vec::new();
        for _ in 0..20 {
            tasks = self
                .shard
                .dequeue("w", task_group, 1)
                .await
                .expect("dequeue")
                .tasks;
            if !tasks.is_empty() {
                break;
            }
        }
        assert_eq!(
            tasks.len(),
            1,
            "one task of {task_group} should be runnable"
        );
        let job_id = tasks[0].job().id().to_string();
        self.shard
            .report_attempt_outcome(
                tasks[0].attempt().task_id(),
                AttemptOutcome::Success { result: vec![] },
            )
            .await
            .expect("report success");
        job_id
    }

    async fn complete_one(&self) -> String {
        self.complete_one_in(TASK_GROUP).await
    }

    /// Run one scanner pass over `queue` and return what it granted and
    /// read.
    async fn pass(&self, queue: &str, count: u32) -> Pass {
        self.traces.lock().unwrap().clear();
        let granted = self
            .shard
            .process_concurrency_grants(TENANT, queue, count)
            .await;
        let mut traces: Vec<RequestReadTrace> = self
            .traces
            .lock()
            .unwrap()
            .drain(..)
            .filter(|trace| trace.reader == RequestReader::Grants && trace.queue == queue)
            .collect();
        assert!(traces.len() <= 1, "one pass opens at most one cursor");
        Pass {
            granted,
            trace: traces.pop(),
        }
    }

    /// Complete `count` running jobs of `task_group`.
    async fn complete_in(&self, task_group: &str, count: usize) {
        for _ in 0..count {
            self.complete_one_in(task_group).await;
        }
    }

    /// Read the task group of `queue`'s head waiter as the floating refresh
    /// scheduler does, and return it with what the read touched.
    async fn peek(&self, queue: &str) -> (Option<String>, RequestReadTrace) {
        self.traces.lock().unwrap().clear();
        let task_group = self
            .shard
            .peek_head_waiting_request_task_group_for_test(TENANT, queue)
            .await
            .expect("peek head waiter");
        let mut traces: Vec<RequestReadTrace> = self
            .traces
            .lock()
            .unwrap()
            .drain(..)
            .filter(|trace| trace.reader == RequestReader::Peek)
            .collect();
        assert_eq!(traces.len(), 1, "a peek opens one cursor");
        (task_group, traces.remove(0))
    }

    /// Move `job_id`'s request row in `queue` to a key with the given start
    /// time and suffix, writing straight to the database: the requester
    /// counter and the request floors hear nothing of it. Returns the old
    /// and new keys and the row's value.
    async fn rekey(
        &self,
        queue: &str,
        job_id: &str,
        priority_ordered: bool,
        start_time_ms: i64,
        suffix: &str,
        put_new_row: bool,
    ) -> (Vec<u8>, Vec<u8>, bytes::Bytes) {
        let old_key = self.request_key_of(queue, job_id).await;
        let value = self
            .shard
            .db()
            .get(&old_key)
            .await
            .expect("read request row")
            .expect("request row exists");
        let parsed = parse_concurrency_request_key(&old_key).expect("parse request key");
        let build = if priority_ordered {
            concurrency_request_key
        } else {
            concurrency_request_key_start_time_first
        };
        let new_key = build(
            TENANT,
            queue,
            start_time_ms,
            parsed.priority,
            &parsed.job_id,
            parsed.attempt_number,
            suffix,
        );
        let mut batch = WriteBatch::new();
        batch.delete(&old_key);
        if put_new_row {
            batch.put(&new_key, &value);
        }
        self.shard
            .db()
            .write(batch)
            .await
            .expect("rekey request row");
        (old_key, new_key, value)
    }

    /// The live request rows of `queue`, in key order.
    async fn request_keys(&self, queue: &str) -> Vec<Vec<u8>> {
        let start = concurrency_request_prefix(TENANT, queue);
        let end = end_bound(&start);
        let mut iter = self
            .shard
            .db()
            .scan::<Vec<u8>, _>(start..end)
            .await
            .expect("scan requests");
        let mut keys = Vec::new();
        while let Some(kv) = iter.next().await.expect("iterate requests") {
            keys.push(kv.key.to_vec());
        }
        keys
    }

    /// The live request row of `job_id` in `queue`.
    async fn request_key_of(&self, queue: &str, job_id: &str) -> Vec<u8> {
        self.request_keys(queue)
            .await
            .into_iter()
            .find(|key| {
                parse_concurrency_request_key(key)
                    .expect("parse request key")
                    .job_id
                    == job_id
            })
            .expect("job has a request row")
    }

    fn floor(&self, queue: &str) -> Option<Vec<u8>> {
        self.shard
            .request_floors_for_test()
            .floor(&concurrency_request_prefix(TENANT, queue))
    }

    fn noted(&self, queue: &str) -> Vec<Vec<u8>> {
        self.shard
            .request_floors_for_test()
            .noted_keys(&concurrency_request_prefix(TENANT, queue))
    }

    async fn requester_count(&self, queue: &str) -> i64 {
        match self
            .shard
            .db()
            .get(concurrency_requester_counter_key(TENANT, queue))
            .await
            .expect("read requester counter")
        {
            Some(raw) => i64::from_le_bytes(raw.as_ref().try_into().expect("8-byte counter")),
            None => 0,
        }
    }

    fn metric(&self, name: &str, labels: &[&str]) -> f64 {
        let body = gather_metrics_text(&self.metrics);
        let mut substrings = vec![name];
        substrings.extend_from_slice(labels);
        metric_value_or_zero(&body, &substrings)
    }
}

fn fixed(queue: &str, max_concurrency: u32) -> Limit {
    Limit::Concurrency(ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency,
    })
}

fn floating(queue: &str, max_concurrency: u32) -> Limit {
    Limit::FloatingConcurrency(FloatingConcurrencyLimit {
        key: queue.to_string(),
        default_max_concurrency: max_concurrency,
        refresh_interval_ms: 3_600_000,
        metadata: vec![],
    })
}

/// Enqueue `waiters` jobs behind a floating limit of one slot, starting one
/// millisecond apart from `base`, then have a refresh raise the limit above
/// their number, so that passes can grant them without anyone releasing a
/// slot: the shape of a backlog draining through a limit that was just
/// raised. Returns the waiters' request keys in key order.
async fn backlog_behind_a_raised_limit(
    harness: &Harness,
    queue: &str,
    priority: u8,
    base: i64,
    waiters: usize,
) -> Vec<Vec<u8>> {
    let limit = || {
        vec![Limit::FloatingConcurrency(FloatingConcurrencyLimit {
            key: queue.to_string(),
            default_max_concurrency: 1,
            refresh_interval_ms: 1,
            metadata: vec![],
        })]
    };
    harness.enqueue(priority, base, limit()).await;
    // Let the limit's state go stale, so the first waiter schedules a
    // refresh.
    tokio::time::sleep(Duration::from_millis(20)).await;
    for i in 0..waiters {
        harness
            .enqueue(priority, base + 1 + i as i64, limit())
            .await;
    }
    let keys = harness.request_keys(queue).await;
    assert_eq!(keys.len(), waiters);
    let leased =
        dequeue_refresh_tasks_until(&harness.shard, "w", TASK_GROUP, 1, Duration::from_secs(5))
            .await;
    harness
        .shard
        .report_refresh_success(&leased.refresh_tasks[0].task_id, 2 * waiters as u32 + 100)
        .await
        .expect("raise the floating limit");
    keys
}

/// Tracks which request rows of a queue have been deleted, to count how
/// many of them a pass's scan stepped across.
#[derive(Default)]
struct DeadRows {
    seen: BTreeSet<Vec<u8>>,
    dead: BTreeSet<Vec<u8>>,
}

impl DeadRows {
    /// Record the queue's live rows. A row seen before and gone now is
    /// dead.
    async fn observe(&mut self, harness: &Harness, queue: &str) {
        let live: BTreeSet<Vec<u8>> = harness.request_keys(queue).await.into_iter().collect();
        for key in self.seen.difference(&live) {
            self.dead.insert(key.clone());
        }
        self.seen.extend(live);
    }

    fn walked_by(&self, pass: &Pass) -> usize {
        pass.trace
            .as_ref()
            .map_or(0, |trace| trace.dead_rows_walked(&self.dead))
    }
}

/// Enqueue `waiters` jobs behind a queue of one slot and grant `grants` of
/// them, one per pass, each after the running job finished. Returns how
/// many dead rows each pass walked, and the keys of the granted rows.
async fn drain_single_queue(
    harness: &Harness,
    queue: &str,
    waiters: usize,
    grants: usize,
) -> (Vec<usize>, BTreeSet<Vec<u8>>) {
    let base = now_ms() - 3_600_000;
    harness.enqueue(10, base, vec![fixed(queue, 1)]).await;
    for i in 0..waiters {
        harness
            .enqueue(10, base + 1 + i as i64, vec![fixed(queue, 1)])
            .await;
    }
    // Grants come out in key order, so the dead rows after `n` grants are
    // the first `n` of the keys the waiters started with.
    let keys = harness.request_keys(queue).await;
    assert_eq!(keys.len(), waiters);

    let mut walked = Vec::with_capacity(grants);
    let mut dead = BTreeSet::new();
    for key in keys.iter().take(grants) {
        harness.complete_one().await;
        let pass = harness.pass(queue, 1).await;
        assert_eq!(pass.granted.len(), 1, "each pass grants the next waiter");
        assert!(pass.trace().scans_opened <= 1);
        walked.push(pass.trace().dead_rows_walked(&dead));
        dead.insert(key.clone());
    }
    assert_eq!(harness.request_keys(queue).await, keys[grants..]);
    (walked, dead)
}

/// One backlog drained through a queue of one slot. With the floor, a pass
/// walks no dead rows however many grants came before it, and neither does
/// a look at the head waiter. With the floor off, pass `n` walks the `n`
/// rows granted before it.
async fn a_single_backlog_drains_without_walking_dead_rows(priority_ordered: bool) {
    let harness = Harness::open(config(priority_ordered)).await;
    let (walked, dead) = drain_single_queue(&harness, "drain", 320, 300).await;
    assert_eq!(
        walked.iter().max(),
        Some(&0),
        "no pass steps over an earlier grant's row"
    );
    let (task_group, trace) = harness.peek("drain").await;
    assert_eq!(task_group.as_deref(), Some(TASK_GROUP));
    assert_eq!(trace.scans_opened, 1);
    assert_eq!(trace.dead_rows_walked(&dead), 0);

    let harness = Harness::open(GrantScannerConfig {
        request_floor: false,
        ..config(priority_ordered)
    })
    .await;
    let (walked, dead) = drain_single_queue(&harness, "drain", 320, 300).await;
    assert_eq!(
        walked,
        (0..300).collect::<Vec<usize>>(),
        "read from the prefix, pass n walks the n rows granted before it"
    );
    let (_, trace) = harness.peek("drain").await;
    assert_eq!(trace.dead_rows_walked(&dead), 300);
}

/// Ten thousand jobs behind a limit of one, run to completion one grant
/// per pass once the limit is raised: no pass walks a dead row, and a look
/// at the head waiter after ten thousand grants walks none either.
async fn ten_thousand_grants_walk_no_dead_rows(priority_ordered: bool) {
    const WAITERS: usize = 10_000;
    let harness = Harness::open(config(priority_ordered)).await;
    let queue = "ten-thousand";
    let base = now_ms() - 86_400_000;
    let keys = backlog_behind_a_raised_limit(&harness, queue, 10, base, WAITERS + 1).await;

    let mut dead = BTreeSet::new();
    for key in &keys[..WAITERS] {
        let pass = harness.pass(queue, 1).await;
        assert_eq!(pass.granted.len(), 1);
        assert_eq!(pass.trace().scans_opened, 1);
        assert_eq!(pass.trace().steps.len(), 2, "one open and one key read");
        assert_eq!(pass.trace().dead_rows_walked(&dead), 0);
        dead.insert(key.clone());
    }

    let (task_group, trace) = harness.peek(queue).await;
    assert_eq!(task_group.as_deref(), Some(TASK_GROUP));
    assert_eq!(trace.scans_opened, 1);
    assert_eq!(trace.steps.len(), 2);
    assert_eq!(trace.dead_rows_walked(&dead), 0);

    // A waiter far below the floor is the head of the queue, and the look
    // finds it by its key.
    harness
        .enqueue_in("old", 10, base - 3_600_000, vec![floating(queue, 1)])
        .await;
    let (task_group, trace) = harness.peek(queue).await;
    assert_eq!(task_group.as_deref(), Some("old"));
    assert_eq!(trace.point_reads, 1);
    assert_eq!(trace.scans_opened, 0);

    assert_eq!(harness.pass(queue, 2).await.granted, ["old", TASK_GROUP]);
    assert!(harness.request_keys(queue).await.is_empty());
}

/// A row written in a higher band than the one being drained is granted
/// from a read by key, and neither that pass nor the next walks the low
/// band's granted rows. (Start-time-ordered rows have one band; the same
/// row then sorts below the drained rows by its older start time.)
async fn an_arrival_below_the_floor_does_not_walk_the_drained_rows(priority_ordered: bool) {
    const DRAINED: usize = 5_000;
    let harness = Harness::open(config(priority_ordered)).await;
    let queue = "bands";
    let base = now_ms() - 86_400_000;
    let keys = backlog_behind_a_raised_limit(&harness, queue, 50, base, DRAINED + 100).await;
    for _ in 0..DRAINED / 50 {
        assert_eq!(harness.pass(queue, 50).await.granted.len(), 50);
    }
    let dead: BTreeSet<Vec<u8>> = keys[..DRAINED].iter().cloned().collect();
    assert_eq!(harness.request_keys(queue).await, keys[DRAINED..]);

    harness
        .enqueue_in("urgent", 1, base - 10_000, vec![floating(queue, 1)])
        .await;
    let pass = harness.pass(queue, 1).await;
    assert_eq!(pass.granted, ["urgent"]);
    assert_eq!(pass.trace().point_reads, 1);
    assert_eq!(
        pass.trace().scans_opened,
        0,
        "the row was read by key alone"
    );

    let pass = harness.pass(queue, 1).await;
    assert_eq!(pass.granted, [TASK_GROUP]);
    assert_eq!(pass.trace().point_reads, 0);
    assert_eq!(pass.trace().scans_opened, 1);
    assert_eq!(pass.trace().dead_rows_walked(&dead), 0);

    // The same arrival with the floor off walks every drained row.
    let harness = Harness::open(GrantScannerConfig {
        request_floor: false,
        ..config(priority_ordered)
    })
    .await;
    let keys = backlog_behind_a_raised_limit(&harness, queue, 50, base, 600).await;
    for _ in 0..10 {
        assert_eq!(harness.pass(queue, 50).await.granted.len(), 50);
    }
    let dead: BTreeSet<Vec<u8>> = keys[..500].iter().cloned().collect();
    harness
        .enqueue_in("urgent", 1, base - 10_000, vec![floating(queue, 1)])
        .await;
    assert_eq!(harness.pass(queue, 1).await.granted, ["urgent"]);
    let pass = harness.pass(queue, 1).await;
    assert_eq!(pass.granted, [TASK_GROUP]);
    assert_eq!(pass.trace().dead_rows_walked(&dead), 500);
}

/// Each round one row arrives ahead of a deep backlog and is granted with
/// one row of the backlog. Returns the dead rows each pass walked.
async fn shallow_front_rounds(harness: &Harness, queue: &str, rounds: usize) -> Vec<usize> {
    let base = now_ms() - 3_600_000;
    for _ in 0..2 {
        harness.enqueue(50, base, vec![fixed(queue, 2)]).await;
    }
    for i in 0..rounds + 10 {
        harness
            .enqueue(50, base + 1 + i as i64, vec![fixed(queue, 2)])
            .await;
    }
    let mut dead = DeadRows::default();
    let mut walked = Vec::with_capacity(rounds);
    let mut running = vec![TASK_GROUP, TASK_GROUP];
    for round in 0..rounds {
        harness
            .enqueue_in(
                "front",
                1,
                base - 100_000 + round as i64,
                vec![fixed(queue, 2)],
            )
            .await;
        dead.observe(harness, queue).await;
        for task_group in running.drain(..) {
            harness.complete_one_in(task_group).await;
        }
        let pass = harness.pass(queue, 2).await;
        assert_eq!(pass.granted, ["front", TASK_GROUP], "round {round}");
        walked.push(dead.walked_by(&pass));
        dead.observe(harness, queue).await;
        running = vec!["front", TASK_GROUP];
    }
    walked
}

/// A front that empties on every pass over a backlog that drains slowly:
/// the dead rows a pass walks stay bounded as grants accumulate, and grow
/// with them when the floor is off.
async fn a_shallow_front_over_a_deep_backlog_stays_cheap(priority_ordered: bool) {
    let harness = Harness::open(config(priority_ordered)).await;
    let walked = shallow_front_rounds(&harness, "shallow", 200).await;
    assert_eq!(walked.iter().max(), Some(&0));

    let harness = Harness::open(GrantScannerConfig {
        request_floor: false,
        ..config(priority_ordered)
    })
    .await;
    let walked = shallow_front_rounds(&harness, "shallow", 60).await;
    assert_eq!(
        walked.last(),
        Some(&(2 * 59)),
        "read from the prefix, a pass walks both rows of every earlier round"
    );
}

/// Three upstream queues feed one downstream floating limit: two hold
/// backlogs an hour apart in age and the third takes jobs at the current
/// time. Each job holds its upstream slot while it waits downstream, so
/// rows reach the downstream queue at start times hours apart. Returns the
/// dead rows each downstream pass walked.
async fn drain_three_backlogs(harness: &Harness, per_queue: usize) -> Vec<usize> {
    let downstream = "downstream";
    let now = now_ms();
    let sources = [
        ("a", "up-a", now - 7_200_000),
        ("b", "up-b", now - 3_600_000),
        ("c", "up-c", now),
    ];
    for (task_group, upstream, start) in sources {
        for i in 0..per_queue {
            harness
                .enqueue_in(
                    task_group,
                    10,
                    start + i as i64,
                    vec![fixed(upstream, 1), floating(downstream, 1)],
                )
                .await;
        }
    }

    let mut dead = DeadRows::default();
    dead.observe(harness, downstream).await;
    let mut walked = Vec::new();
    // The first job enqueued took its upstream slot and the downstream one.
    let mut running = Some("a".to_string());
    let mut completed = 0;
    while let Some(task_group) = running.take() {
        harness.complete_one_in(&task_group).await;
        completed += 1;
        // The downstream pass runs before the upstream ones, as it does
        // when the scanner serves both queues in one iteration: the job
        // that is about to get its upstream slot is not yet waiting
        // downstream.
        let pass = harness.pass(downstream, 1).await;
        if pass.trace.is_some() {
            walked.push(dead.walked_by(&pass));
        }
        running = pass.granted.first().cloned();
        for (_, upstream, _) in sources {
            let upstream_pass = harness.pass(upstream, 1).await;
            // With nobody waiting downstream, the job that takes the freed
            // upstream slot takes the downstream slot with it.
            if running.is_none() {
                running = upstream_pass.granted.first().cloned();
            }
        }
        dead.observe(harness, downstream).await;
    }
    assert_eq!(completed, 3 * per_queue);
    assert!(harness.request_keys(downstream).await.is_empty());
    walked
}

/// Backlogs of different ages feeding one queue: rows that arrive hours
/// below the floor are read by key, so a downstream pass walks no dead
/// rows. With keyed reads off (the lag at its maximum) every such arrival
/// lowers the floor and the walk grows with the grants.
async fn backlogs_of_different_ages_feeding_one_queue_stay_cheap(priority_ordered: bool) {
    const PER_QUEUE: usize = 60;
    let harness = Harness::open(config(priority_ordered)).await;
    let walked = drain_three_backlogs(&harness, PER_QUEUE).await;
    assert_eq!(walked.iter().max(), Some(&0));
    assert!(
        harness.metric(
            "silo_concurrency_request_noted_reads_total",
            &["outcome=\"granted\""]
        ) >= (PER_QUEUE - 1) as f64,
        "the older backlog's rows were granted from reads by key"
    );

    let harness = Harness::open(GrantScannerConfig {
        request_floor_min_lag_ms: u64::MAX,
        ..config(priority_ordered)
    })
    .await;
    let walked = drain_three_backlogs(&harness, PER_QUEUE).await;
    assert!(
        walked.iter().max().copied().unwrap_or(0) >= PER_QUEUE / 2,
        "with keyed reads off the walk grows with the grants: {walked:?}"
    );
}

/// A row read by key is granted in its key order: before the rows at the
/// floor of its band, after the rows of a higher band.
async fn a_noted_row_is_granted_in_key_order(priority_ordered: bool) {
    let harness = Harness::open(config(priority_ordered)).await;
    let queue = "order";
    let base = now_ms() - 3_600_000;
    for _ in 0..3 {
        harness.enqueue(50, base, vec![fixed(queue, 3)]).await;
    }
    for (i, task_group) in ["w1", "w2", "w3", "w4"].into_iter().enumerate() {
        harness
            .enqueue_in(task_group, 50, base + 1 + i as i64, vec![fixed(queue, 3)])
            .await;
    }
    harness.complete_one().await;
    assert_eq!(harness.pass(queue, 1).await.granted, ["w1"]);
    let floor = harness.floor(queue).expect("the pass set a floor");

    // Same band as the waiters, far older: below the floor.
    let old = harness
        .enqueue_in("old", 50, base - 50_000, vec![fixed(queue, 3)])
        .await;
    // A higher band when rows are priority-ordered, a later start time
    // otherwise.
    let urgent = harness
        .enqueue_in("urgent", 10, base + 100, vec![fixed(queue, 3)])
        .await;
    assert!(harness.request_key_of(queue, &old).await < floor);
    assert_eq!(
        harness.request_key_of(queue, &urgent).await < floor,
        priority_ordered
    );

    harness.complete_in(TASK_GROUP, 2).await;
    harness.complete_one_in("w1").await;
    let pass = harness.pass(queue, 3).await;
    if priority_ordered {
        assert_eq!(pass.granted, ["urgent", "old", "w2"]);
        assert_eq!(pass.trace().point_reads, 2);
    } else {
        assert_eq!(pass.granted, ["old", "w2", "w3"]);
        assert_eq!(pass.trace().point_reads, 1);
    }
}

/// A row committed while a pass is running, at a key below one the pass
/// then consumes, is not in the pass's snapshot. The floor moves past it,
/// and the next pass reads it by key.
async fn a_row_committed_during_a_pass_is_granted_by_the_next(priority_ordered: bool) {
    let harness = Harness::open(config(priority_ordered)).await;
    let queue = "race";
    let base = now_ms() - 3_600_000;
    harness.enqueue(10, base, vec![fixed(queue, 1)]).await;
    harness
        .enqueue_in("first", 10, base + 10, vec![fixed(queue, 1)])
        .await;
    harness.complete_one().await;

    let shard = Arc::clone(&harness.shard);
    let late_job: Arc<Mutex<Option<String>>> = Arc::default();
    let late_job_slot = Arc::clone(&late_job);
    harness
        .shard
        .set_grant_scan_hook_for_test(Some(Arc::new(move || {
            let shard = Arc::clone(&shard);
            let late_job_slot = Arc::clone(&late_job_slot);
            Box::pin(async move {
                if late_job_slot.lock().unwrap().is_some() {
                    return;
                }
                let job_id = shard
                    .enqueue(
                        TENANT,
                        None,
                        10,
                        base + 5,
                        None,
                        msgpack_payload(&serde_json::json!({})),
                        vec![fixed("race", 1)],
                        None,
                        "late",
                    )
                    .await
                    .expect("enqueue during the pass");
                *late_job_slot.lock().unwrap() = Some(job_id);
            })
        })));
    let pass = harness.pass(queue, 1).await;
    harness.shard.set_grant_scan_hook_for_test(None);
    assert_eq!(pass.granted, ["first"]);

    let late_job = late_job.lock().unwrap().clone().expect("the hook enqueued");
    let late_key = harness.request_key_of(queue, &late_job).await;
    assert!(
        harness.floor(queue).expect("the pass set a floor") > late_key,
        "the floor moved past the row the pass could not see"
    );
    assert_eq!(harness.noted(queue), vec![late_key]);

    harness.complete_one_in("first").await;
    let pass = harness.pass(queue, 1).await;
    assert_eq!(pass.granted, ["late"]);
    assert_eq!(pass.trace().point_reads, 1);
    assert_eq!(pass.trace().scans_opened, 0);
    assert!(harness.noted(queue).is_empty());
}

/// A noted key whose row does not exist yet is read on every pass while its
/// write may still commit. A write that commits later is granted from that
/// read; one that never commits is dropped after its grace period and not
/// read again.
async fn a_noted_key_waits_for_its_write_to_settle(priority_ordered: bool) {
    let grace_ms = 200;
    let harness = Harness::open(GrantScannerConfig {
        request_write_abandon_grace_ms: grace_ms,
        ..config(priority_ordered)
    })
    .await;
    let queue = "pending";
    let base = now_ms() - 3_600_000;
    harness.enqueue(10, base, vec![fixed(queue, 1)]).await;
    for i in 0..5 {
        harness
            .enqueue(10, base + 1 + i, vec![fixed(queue, 1)])
            .await;
    }
    harness.complete_one().await;
    assert_eq!(harness.pass(queue, 1).await.granted, [TASK_GROUP]);

    // Take a real job's row out of the database and stand in for the
    // writers of two rows below the floor: one will commit, one never does.
    let job = harness
        .enqueue_in("slow", 10, base - 50_000, vec![fixed(queue, 1)])
        .await;
    let (written_key, slow_key, value) = harness
        .rekey(
            queue,
            &job,
            priority_ordered,
            base - 50_000,
            "slowslow",
            false,
        )
        .await;
    let never_key = {
        let mut key = slow_key.clone();
        let len = key.len();
        key[len - 9..len - 1].copy_from_slice(b"neverwrt");
        key
    };
    assert!(never_key < slow_key);
    let floors = harness.shard.request_floors_for_test();
    let prefix_len = concurrency_request_prefix(TENANT, queue).len();
    let slow_write = floors.note_write(prefix_len, slow_key.clone());
    let never_write = floors.note_write(prefix_len, never_key.clone());

    // No row exists under any of the three keys. The one whose write was
    // confirmed is retired; the two in flight are kept.
    harness.complete_one().await;
    let pass = harness.pass(queue, 1).await;
    assert_eq!(pass.granted, [TASK_GROUP]);
    assert_eq!(pass.trace().point_reads, 3);
    assert_eq!(
        harness.noted(queue),
        vec![never_key.clone(), slow_key.clone()]
    );
    assert!(!harness.noted(queue).contains(&written_key));

    // The slow write commits; the other is abandoned.
    harness
        .shard
        .db()
        .put(&slow_key, &value)
        .await
        .expect("write the slow row");
    slow_write.confirm();
    drop(never_write);
    harness.complete_one().await;
    let pass = harness.pass(queue, 1).await;
    assert_eq!(pass.granted, ["slow"]);
    assert_eq!(pass.trace().point_reads, 2);
    assert_eq!(
        harness.noted(queue),
        vec![never_key.clone()],
        "an abandoned write may still commit within its grace period"
    );

    tokio::time::sleep(Duration::from_millis(grace_ms + 100)).await;
    harness.complete_one_in("slow").await;
    let pass = harness.pass(queue, 1).await;
    assert_eq!(pass.granted, [TASK_GROUP]);
    assert_eq!(pass.trace().point_reads, 1);
    assert!(harness.noted(queue).is_empty());

    harness.complete_one().await;
    let pass = harness.pass(queue, 1).await;
    assert_eq!(pass.granted, [TASK_GROUP]);
    assert_eq!(
        pass.trace().point_reads,
        0,
        "a retired key is not read again"
    );

    let shard_label = ["shard=\"test\""];
    let reads = |outcome: &str| {
        harness.metric(
            "silo_concurrency_request_noted_reads_total",
            &[shard_label[0], &format!("outcome=\"{outcome}\"")],
        )
    };
    assert_eq!(reads("granted"), 1.0);
    assert_eq!(reads("missing"), 2.0);
    assert_eq!(reads("pending"), 3.0);
    assert_eq!(
        harness.metric("silo_concurrency_request_writes_abandoned_total", &[]),
        1.0
    );
}

/// A write that would put more keys below the floor than the limit lowers
/// the floor to its key, and every row is still granted in key order.
async fn a_write_past_the_noted_limit_lowers_the_floor(priority_ordered: bool) {
    let harness = Harness::open(GrantScannerConfig {
        request_noted_max_keys: 2,
        ..config(priority_ordered)
    })
    .await;
    let queue = "overflow";
    let base = now_ms() - 3_600_000;
    harness.enqueue(50, base, vec![fixed(queue, 1)]).await;
    for i in 0..2 {
        harness
            .enqueue(50, base + 1 + i, vec![fixed(queue, 1)])
            .await;
    }
    harness.complete_one().await;
    assert_eq!(harness.pass(queue, 1).await.granted, [TASK_GROUP]);
    let floor = harness.floor(queue).expect("the pass set a floor");

    harness
        .enqueue_in("o1", 50, base - 50_000, vec![fixed(queue, 1)])
        .await;
    harness
        .enqueue_in("o2", 50, base - 40_000, vec![fixed(queue, 1)])
        .await;
    assert_eq!(harness.floor(queue), Some(floor));
    let third = harness
        .enqueue_in("o3", 50, base - 30_000, vec![fixed(queue, 1)])
        .await;
    assert_eq!(
        harness.floor(queue),
        Some(harness.request_key_of(queue, &third).await)
    );
    assert_eq!(
        harness.metric("silo_concurrency_request_noted_overflows_total", &[]),
        1.0
    );

    let mut granted = Vec::new();
    let mut running = TASK_GROUP.to_string();
    for _ in 0..4 {
        harness.complete_one_in(&running).await;
        let pass = harness.pass(queue, 1).await;
        assert_eq!(pass.granted.len(), 1);
        running = pass.granted[0].clone();
        granted.push(running.clone());
    }
    assert_eq!(granted, ["o1", "o2", "o3", TASK_GROUP]);
}

/// A pass that runs out of capacity leaves the floor at the first row it
/// could not grant, and that row is granted when a holder releases.
async fn the_floor_stops_at_the_first_row_capacity_left(priority_ordered: bool) {
    let harness = Harness::open(config(priority_ordered)).await;
    let queue = "capacity";
    let base = now_ms() - 3_600_000;
    for _ in 0..3 {
        harness.enqueue(10, base, vec![fixed(queue, 3)]).await;
    }
    for (i, task_group) in ["w1", "w2", "w3", "w4", "w5"].into_iter().enumerate() {
        harness
            .enqueue_in(task_group, 10, base + 1 + i as i64, vec![fixed(queue, 3)])
            .await;
    }
    let keys = harness.request_keys(queue).await;

    harness.complete_in(TASK_GROUP, 2).await;
    let pass = harness.pass(queue, 5).await;
    assert_eq!(pass.granted, ["w1", "w2"]);
    assert_eq!(
        harness.floor(queue),
        Some(keys[2].clone()),
        "the floor is the first row read and left in place"
    );

    harness.complete_one().await;
    let pass = harness.pass(queue, 5).await;
    assert_eq!(pass.granted, ["w3"]);
    assert_eq!(harness.floor(queue), Some(keys[3].clone()));
}

/// A pass that stops at its requested count leaves the floor just past the
/// last row it granted, and the next pass grants the row after it.
async fn the_floor_stops_where_the_requested_count_ran_out(priority_ordered: bool) {
    let harness = Harness::open(config(priority_ordered)).await;
    let queue = "count";
    let base = now_ms() - 3_600_000;
    for _ in 0..5 {
        harness.enqueue(10, base, vec![fixed(queue, 5)]).await;
    }
    for (i, task_group) in ["w1", "w2", "w3", "w4", "w5"].into_iter().enumerate() {
        harness
            .enqueue_in(task_group, 10, base + 1 + i as i64, vec![fixed(queue, 5)])
            .await;
    }
    let keys = harness.request_keys(queue).await;

    harness.complete_in(TASK_GROUP, 5).await;
    let pass = harness.pass(queue, 2).await;
    assert_eq!(pass.granted, ["w1", "w2"]);
    let floor = harness.floor(queue).expect("the pass set a floor");
    assert!(floor > keys[1] && floor <= keys[2]);

    let pass = harness.pass(queue, 5).await;
    assert_eq!(pass.granted, ["w3", "w4", "w5"]);
    assert_eq!(
        pass.trace()
            .dead_rows_walked(&keys[..2].iter().cloned().collect()),
        0
    );
}

/// A front of rows whose next limit is saturated is left in place, and a
/// pass that spends its key budget on them stops there. The floor stays in
/// front of them, and they are granted once the next limit's line clears.
async fn the_floor_stays_in_front_of_rows_a_pass_skips(priority_ordered: bool) {
    let harness = Harness::open(GrantScannerConfig {
        // A budget of four keys per pass.
        batch_size: 1,
        next_hop_skip_min_backlog: 1,
        ..config(priority_ordered)
    })
    .await;
    let (queue, hop) = ("front", "hop");
    let base = now_ms() - 3_600_000;
    // The next limit is held, with one job waiting in line for it.
    harness
        .enqueue_in("hop", 10, base, vec![fixed(hop, 1)])
        .await;
    harness
        .enqueue_in("hop", 10, base, vec![fixed(hop, 1)])
        .await;

    harness.enqueue(10, base, vec![fixed(queue, 1)]).await;
    for (i, task_group) in ["s1", "s2", "s3", "s4", "s5", "s6"].into_iter().enumerate() {
        harness
            .enqueue_in(
                task_group,
                10,
                base + 1 + i as i64,
                vec![fixed(queue, 1), fixed(hop, 1)],
            )
            .await;
    }
    let keys = harness.request_keys(queue).await;
    harness.complete_one().await;

    for _ in 0..2 {
        let pass = harness.pass(queue, 1).await;
        assert!(
            pass.granted.is_empty(),
            "every row read waits on the next limit"
        );
        assert_eq!(
            pass.trace().steps.len(),
            1 + 4,
            "one open and the budget's four keys"
        );
        assert_eq!(harness.floor(queue), Some(keys[0].clone()));
    }
    assert_eq!(harness.request_keys(queue).await, keys);

    // The next limit's line clears: its holder finishes and its waiter
    // takes the slot.
    harness.complete_one_in("hop").await;
    assert_eq!(harness.pass(hop, 1).await.granted, ["hop"]);
    let pass = harness.pass(queue, 1).await;
    assert_eq!(pass.granted, ["s1"]);
    let floor = harness.floor(queue).expect("floor");
    assert!(floor > keys[0] && floor <= keys[1]);
}

/// A future-dated row is never consumed, so the floor stays in front of it.
/// The rows behind it are still granted.
async fn a_future_dated_row_holds_the_floor_and_blocks_nothing(priority_ordered: bool) {
    let harness = Harness::open(config(priority_ordered)).await;
    let queue = "future";
    let base = now_ms() - 3_600_000;
    harness.enqueue(50, base, vec![fixed(queue, 1)]).await;
    let future_job = harness
        .enqueue_in("future", 10, base, vec![fixed(queue, 1)])
        .await;
    let (_, future_key, _) = harness
        .rekey(
            queue,
            &future_job,
            priority_ordered,
            now_ms() + 3_600_000,
            "futurerw",
            true,
        )
        .await;
    for i in 0..3 {
        harness
            .enqueue(50, base + 1 + i, vec![fixed(queue, 1)])
            .await;
    }

    for _ in 0..3 {
        harness.complete_one().await;
        let pass = harness.pass(queue, 1).await;
        assert_eq!(pass.granted, [TASK_GROUP]);
        if priority_ordered {
            // The future row leads its band, ahead of the waiters' band.
            assert_eq!(pass.trace().seeks, 1);
            assert_eq!(harness.floor(queue), Some(future_key.clone()));
        }
    }
    harness.complete_one().await;
    let pass = harness.pass(queue, 1).await;
    assert!(pass.granted.is_empty());
    assert_eq!(harness.request_keys(queue).await, vec![future_key.clone()]);
    assert!(harness.floor(queue).expect("floor") <= future_key);
}

/// A noted row whose job is cancelled is deleted by the cancel. The next
/// pass reads its key, finds nothing and retires it; the requester counter
/// is decremented once, by the cancel.
async fn a_cancelled_noted_row_is_retired_on_its_next_read(priority_ordered: bool) {
    let harness = Harness::open(config(priority_ordered)).await;
    let queue = "cancel";
    let base = now_ms() - 3_600_000;
    harness.enqueue(10, base, vec![fixed(queue, 1)]).await;
    for i in 0..3 {
        harness
            .enqueue(10, base + 1 + i, vec![fixed(queue, 1)])
            .await;
    }
    harness.complete_one().await;
    assert_eq!(harness.pass(queue, 1).await.granted, [TASK_GROUP]);

    let job = harness
        .enqueue_in("cancelled", 10, base - 50_000, vec![fixed(queue, 1)])
        .await;
    let key = harness.request_key_of(queue, &job).await;
    assert_eq!(harness.noted(queue), vec![key]);
    assert_eq!(harness.requester_count(queue).await, 3);
    harness
        .shard
        .cancel_job(TENANT, &job)
        .await
        .expect("cancel");
    assert_eq!(harness.requester_count(queue).await, 2);

    harness.complete_one().await;
    let pass = harness.pass(queue, 1).await;
    assert_eq!(pass.granted, [TASK_GROUP]);
    assert_eq!(pass.trace().point_reads, 1);
    assert!(harness.noted(queue).is_empty());
    assert_eq!(harness.requester_count(queue).await, 1);

    harness.complete_one().await;
    let pass = harness.pass(queue, 1).await;
    assert_eq!(pass.granted, [TASK_GROUP]);
    assert_eq!(pass.trace().point_reads, 0);
    assert_eq!(harness.requester_count(queue).await, 0);
}

/// A row put below the floor without telling it is not read until the
/// floor is dropped. The pass that drops it reads from the prefix, grants
/// the row and counts it. A rebuild over a floor that hid nothing counts
/// nothing.
async fn a_rebuild_finds_a_row_the_floor_was_not_told_of(priority_ordered: bool) {
    let rebuild_interval_ms = 2_000;
    let harness = Harness::open(GrantScannerConfig {
        request_floor_rebuild_interval_ms: rebuild_interval_ms,
        ..config(priority_ordered)
    })
    .await;
    let queue = "rebuild";
    let base = now_ms() - 3_600_000;
    harness.enqueue(10, base, vec![fixed(queue, 1)]).await;
    for i in 0..4 {
        harness
            .enqueue(10, base + 1 + i, vec![fixed(queue, 1)])
            .await;
    }
    harness.complete_one().await;
    assert_eq!(harness.pass(queue, 1).await.granted, [TASK_GROUP]);
    let floor = harness.floor(queue).expect("the pass set a floor");

    let hidden = harness
        .enqueue_in("hidden", 10, base + 100, vec![fixed(queue, 1)])
        .await;
    let (_, hidden_key, _) = harness
        .rekey(
            queue,
            &hidden,
            priority_ordered,
            base - 50_000,
            "hiddenrw",
            true,
        )
        .await;
    assert!(hidden_key < floor);
    assert!(harness.noted(queue).is_empty());
    let rebuild_rows = || harness.metric("silo_concurrency_request_floor_rebuild_rows_total", &[]);

    harness.complete_one().await;
    let pass = harness.pass(queue, 1).await;
    assert_eq!(pass.granted, [TASK_GROUP], "the hidden row is not read");
    assert_eq!(rebuild_rows(), 0.0);

    tokio::time::sleep(Duration::from_millis(rebuild_interval_ms + 100)).await;
    harness.complete_one().await;
    let pass = harness.pass(queue, 1).await;
    assert_eq!(pass.granted, ["hidden"]);
    assert_eq!(rebuild_rows(), 1.0);

    tokio::time::sleep(Duration::from_millis(rebuild_interval_ms + 100)).await;
    harness.complete_one_in("hidden").await;
    let pass = harness.pass(queue, 1).await;
    assert_eq!(pass.granted, [TASK_GROUP]);
    assert_eq!(
        rebuild_rows(),
        1.0,
        "a floor that hid nothing counts nothing"
    );
}

/// Run a queue through grants, an arrival far below the floor, an arrival
/// in a higher band and a cancel. Returns the task groups in grant order.
async fn scripted_grants(harness: &Harness) -> Vec<String> {
    let queue = "script";
    let base = now_ms() - 3_600_000;
    harness
        .enqueue_in("h", 50, base, vec![fixed(queue, 1)])
        .await;
    for i in 0..6 {
        harness
            .enqueue_in(&format!("w{i}"), 50, base + 1 + i, vec![fixed(queue, 1)])
            .await;
    }
    let mut granted = Vec::new();
    let mut running = "h".to_string();
    for round in 0..9 {
        match round {
            2 => {
                harness
                    .enqueue_in("old", 50, base - 50_000, vec![fixed(queue, 1)])
                    .await;
            }
            3 => {
                harness
                    .enqueue_in("urgent", 5, base + 500, vec![fixed(queue, 1)])
                    .await;
                let cancelled = harness
                    .enqueue_in("cancelled", 50, base - 40_000, vec![fixed(queue, 1)])
                    .await;
                harness
                    .shard
                    .cancel_job(TENANT, &cancelled)
                    .await
                    .expect("cancel");
            }
            _ => {}
        }
        harness.complete_one_in(&running).await;
        let pass = harness.pass(queue, 1).await;
        let Some(task_group) = pass.granted.first() else {
            break;
        };
        running = task_group.clone();
        granted.push(running.clone());
    }
    granted
}

/// With the floor off, passes read from the prefix, nothing is tracked,
/// and the same rows are granted in the same order.
async fn the_floor_off_grants_the_same_rows_in_the_same_order(priority_ordered: bool) {
    let with_floor = Harness::open(config(priority_ordered)).await;
    let granted_with_floor = scripted_grants(&with_floor).await;
    assert!(with_floor.floor("script").is_some());

    let without_floor = Harness::open(GrantScannerConfig {
        request_floor: false,
        ..config(priority_ordered)
    })
    .await;
    let granted_without_floor = scripted_grants(&without_floor).await;
    assert_eq!(without_floor.floor("script"), None);
    assert_eq!(
        without_floor.shard.request_floors_for_test().queue_count(),
        0
    );

    assert_eq!(granted_with_floor, granted_without_floor);
    let expected: Vec<&str> = if priority_ordered {
        vec!["w0", "w1", "old", "urgent", "w2", "w3", "w4", "w5"]
    } else {
        vec!["w0", "w1", "old", "w2", "w3", "w4", "w5", "urgent"]
    };
    assert_eq!(granted_with_floor, expected);
}

/// The floor lives in memory. A shard opened again starts without one,
/// reads from the prefix, and grants the rows that were below the old floor
/// in key order with the rest.
async fn a_reopened_shard_grants_the_same_rows_in_the_same_order(priority_ordered: bool) {
    use silo::gubernator::MockGubernatorClient;
    use silo::settings::{Backend, DatabaseConfig};
    use silo::shard_range::ShardRange;

    let tmp = tempfile::tempdir().unwrap();
    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
        slatedb: Some(fast_flush_slatedb_settings()),
        concurrency_request_priority_order: priority_ordered,
        ..Default::default()
    };
    let queue = "reopen";
    let base = now_ms() - 3_600_000;
    let limits = || vec![fixed(queue, 1)];
    let enqueue = |shard: Arc<JobStoreShard>, task_group: &'static str, start: i64| async move {
        shard
            .enqueue(
                TENANT,
                None,
                10,
                start,
                None,
                msgpack_payload(&serde_json::json!({})),
                limits(),
                None,
                task_group,
            )
            .await
            .expect("enqueue")
    };
    let complete = |shard: Arc<JobStoreShard>, task_group: String| async move {
        let tasks = shard
            .dequeue("w", &task_group, 1)
            .await
            .expect("dequeue")
            .tasks;
        assert_eq!(tasks.len(), 1);
        shard
            .report_attempt_outcome(
                tasks[0].attempt().task_id(),
                AttemptOutcome::Success { result: vec![] },
            )
            .await
            .expect("report success");
    };

    let shard = JobStoreShard::open(
        &cfg,
        MockGubernatorClient::new_arc(),
        None,
        ShardRange::full(),
    )
    .await
    .expect("open shard");
    shard.stop_grant_scanner();
    enqueue(Arc::clone(&shard), "h", base).await;
    for (i, task_group) in ["w1", "w2", "w3"].into_iter().enumerate() {
        enqueue(Arc::clone(&shard), task_group, base + 1 + i as i64).await;
    }
    complete(Arc::clone(&shard), "h".to_string()).await;
    assert_eq!(
        shard.process_concurrency_grants(TENANT, queue, 1).await,
        ["w1"]
    );
    // Below the floor: read by key for as long as this shard stays open.
    enqueue(Arc::clone(&shard), "old", base - 50_000).await;
    let prefix = concurrency_request_prefix(TENANT, queue);
    assert_eq!(shard.request_floors_for_test().noted_keys(&prefix).len(), 1);
    shard.close().await.expect("close shard");
    drop(shard);

    let shard = JobStoreShard::open(
        &cfg,
        MockGubernatorClient::new_arc(),
        None,
        ShardRange::full(),
    )
    .await
    .expect("reopen shard");
    shard.stop_grant_scanner();
    assert!(
        shard
            .request_floors_for_test()
            .noted_keys(&prefix)
            .is_empty(),
        "nothing of the old shard's floor state is left"
    );
    let mut granted = Vec::new();
    let mut running = "w1".to_string();
    for _ in 0..3 {
        complete(Arc::clone(&shard), running.clone()).await;
        let pass = shard.process_concurrency_grants(TENANT, queue, 1).await;
        assert_eq!(pass.len(), 1);
        running = pass[0].clone();
        granted.push(running.clone());
    }
    assert_eq!(granted, ["old", "w2", "w3"]);
    shard.close().await.expect("close shard");
}

both_orders!(
    a_single_backlog_drains_without_walking_dead_rows,
    ten_thousand_grants_walk_no_dead_rows,
    an_arrival_below_the_floor_does_not_walk_the_drained_rows,
    a_shallow_front_over_a_deep_backlog_stays_cheap,
    backlogs_of_different_ages_feeding_one_queue_stay_cheap,
    a_noted_row_is_granted_in_key_order,
    a_row_committed_during_a_pass_is_granted_by_the_next,
    a_noted_key_waits_for_its_write_to_settle,
    a_write_past_the_noted_limit_lowers_the_floor,
    the_floor_stops_at_the_first_row_capacity_left,
    the_floor_stops_where_the_requested_count_ran_out,
    the_floor_stays_in_front_of_rows_a_pass_skips,
    a_future_dated_row_holds_the_floor_and_blocks_nothing,
    a_cancelled_noted_row_is_retired_on_its_next_read,
    a_rebuild_finds_a_row_the_floor_was_not_told_of,
    the_floor_off_grants_the_same_rows_in_the_same_order,
    a_reopened_shard_grants_the_same_rows_in_the_same_order,
);
