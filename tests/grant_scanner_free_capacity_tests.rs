//! A concurrency queue with free capacity and waiting requesters converges
//! to full occupancy through the grant scanner alone, without the periodic
//! requester sweep.
//!
//! Every shard here opens with a one-hour reconcile interval, so the sweep
//! never seeds the scanner during a test. Most tests stop the background
//! scanner and run its drain iterations by hand, which makes the number of
//! iterations a queue needs an exact, assertable quantity.

mod test_helpers;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use silo::codec::encode_concurrency_action;
use silo::concurrency::GrantScannerConfig;
use silo::job::{ConcurrencyLimit, FloatingConcurrencyLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::job_store_shard::JobStoreShard;
use silo::keys::{concurrency_request_key, concurrency_requester_counter_key};
use silo::task::ConcurrencyAction;
use slatedb::WriteBatch;

use test_helpers::*;

const TENANT: &str = "-";
const TASK_GROUP: &str = "default";
const NO_SWEEP_INTERVAL_MS: u64 = 60 * 60 * 1000;
const REFRESH_INTERVAL_MS: i64 = 100;
const WAIT: Duration = Duration::from_secs(30);
const PRIORITY: u8 = 10;

/// Scanner sizes small enough that a modest backlog spans several passes:
/// a pass scans 4 candidates at a time, walks at most 16 request keys, and
/// commits every 4 grants. Request rows are priority-ordered.
const BATCH_SIZE: usize = 4;
const WALK_BUDGET: usize = BATCH_SIZE * 4;
const COMMIT_CHUNK: usize = 4;

fn small_scanner_config() -> GrantScannerConfig {
    GrantScannerConfig {
        batch_size: BATCH_SIZE,
        commit_chunk_size: COMMIT_CHUNK,
        priority_ordered_requests: true,
        ..GrantScannerConfig::default()
    }
}

fn fixed_limit(queue: &str, max_concurrency: u32) -> Limit {
    Limit::Concurrency(ConcurrencyLimit {
        key: queue.to_string(),
        max_concurrency,
    })
}

fn floating_limit(queue: &str, default_max_concurrency: u32) -> Limit {
    Limit::FloatingConcurrency(FloatingConcurrencyLimit {
        key: queue.to_string(),
        default_max_concurrency,
        refresh_interval_ms: REFRESH_INTERVAL_MS,
        metadata: vec![],
    })
}

async fn enqueue(shard: &JobStoreShard, id: &str, limits: Vec<Limit>, start_at_ms: i64) -> String {
    enqueue_with_priority(shard, id, PRIORITY, limits, start_at_ms).await
}

async fn enqueue_with_priority(
    shard: &JobStoreShard,
    id: &str,
    priority: u8,
    limits: Vec<Limit>,
    start_at_ms: i64,
) -> String {
    shard
        .enqueue(
            TENANT,
            Some(id.to_string()),
            priority,
            start_at_ms,
            None,
            msgpack_payload(&serde_json::json!({"k": "v"})),
            limits,
            None,
            TASK_GROUP,
        )
        .await
        .expect("enqueue")
}

async fn succeed(shard: &JobStoreShard, task_id: &str) {
    shard
        .report_attempt_outcome(task_id, AttemptOutcome::Success { result: vec![] })
        .await
        .expect("report success");
}

async fn requester_count(shard: &JobStoreShard, queue: &str) -> i64 {
    shard
        .get_concurrency_requester_count(TENANT, queue)
        .await
        .expect("requester count")
}

fn holders(shard: &JobStoreShard, queue: &str) -> usize {
    shard.concurrency_holder_count(TENANT, queue)
}

/// A shard with small scanner sizes, no periodic sweep, and the background
/// scanner stopped: grants happen only in `run_scanner_iteration`.
async fn open_shard_with_scanner_stopped() -> (tempfile::TempDir, Arc<JobStoreShard>) {
    let (tmp, shard) = open_temp_shard_with_scanner_config_and_reconcile_interval_ms(
        small_scanner_config(),
        NO_SWEEP_INTERVAL_MS,
    )
    .await;
    shard.stop_grant_scanner();
    (tmp, shard)
}

/// Run one drain iteration as the background scanner does: take every hot
/// entry plus a bounded cold batch, then run one pass per queue. Returns the
/// work the iteration took as `(queue, requested count)` pairs.
async fn run_scanner_iteration(shard: &JobStoreShard) -> Vec<(String, u32)> {
    let work = shard.take_pending_grant_batch_for_test();
    let mut taken = Vec::with_capacity(work.len());
    for (tenant, queue, count) in work {
        shard
            .process_concurrency_grants(&tenant, &queue, count)
            .await;
        taken.push((queue, count));
    }
    taken
}

/// Run drain iterations until one finds no work. Returns how many iterations
/// had work. Panics past `limit`, which no test here should approach.
async fn run_scanner_until_idle(shard: &JobStoreShard, limit: usize) -> usize {
    let mut iterations = 0;
    while !run_scanner_iteration(shard).await.is_empty() {
        iterations += 1;
        assert!(
            iterations <= limit,
            "scanner still has work after {limit} iterations"
        );
    }
    iterations
}

/// Lease every ready run task and the first `refresh_count` refresh tasks.
/// Returns the leased run tasks as `(job_id, task_id)` and the refresh task
/// ids.
async fn lease_ready(
    shard: &JobStoreShard,
    run_count: usize,
    refresh_count: usize,
) -> (Vec<(String, String)>, Vec<String>) {
    let mut runs = Vec::new();
    let mut refreshes = Vec::new();
    let deadline = std::time::Instant::now() + WAIT;
    while runs.len() < run_count || refreshes.len() < refresh_count {
        let batch = shard
            .dequeue("worker", TASK_GROUP, 50)
            .await
            .expect("dequeue");
        runs.extend(batch.tasks.iter().map(|task| {
            (
                task.job().id().to_string(),
                task.attempt().task_id().to_string(),
            )
        }));
        refreshes.extend(batch.refresh_tasks.iter().map(|task| task.task_id.clone()));
        assert!(
            std::time::Instant::now() < deadline,
            "timed out leasing {run_count} run tasks and {refresh_count} refresh tasks; got {} and {}",
            runs.len(),
            refreshes.len()
        );
        if runs.len() < run_count || refreshes.len() < refresh_count {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    (runs, refreshes)
}

/// Make the floating queue's refresh interval elapsed, keeping every other
/// field of its state row.
async fn backdate_last_refreshed(shard: &JobStoreShard, queue: &str) {
    let key = silo::keys::floating_limit_state_key(TENANT, queue);
    let raw = shard
        .db()
        .get(&key)
        .await
        .expect("get floating state")
        .expect("floating state exists");
    let mut state = silo::codec::decode_floating_limit_state(raw)
        .expect("decode floating state")
        .to_owned();
    state.last_refreshed_at_ms = 0;
    shard
        .db()
        .put(&key, &silo::codec::encode_floating_limit_state(&state))
        .await
        .expect("put floating state");
    shard.db().flush().await.expect("flush floating state");
}

/// A kind of request row the scanner deletes without granting.
#[derive(Clone, Copy)]
enum StaleRow {
    /// A well-formed request for a job that does not exist.
    MissingJob,
    /// A request whose value does not decode, so the pass learns nothing
    /// about the queue from it, its capacity included.
    Undecodable,
}

/// Write `count` request rows of `kind` on `queue`, with their requester
/// counter increments. They sort ahead of any row written at `before_ms` or
/// later, and a scanner pass deletes each one it reaches.
async fn write_stale_requests(
    shard: &JobStoreShard,
    queue: &str,
    kind: StaleRow,
    count: usize,
    before_ms: i64,
) {
    let mut batch = WriteBatch::new();
    for i in 0..count {
        let start_time_ms = before_ms - 10_000 + i as i64;
        let job_id = format!("missing-job-{i:04}");
        let key = concurrency_request_key(TENANT, queue, start_time_ms, PRIORITY, &job_id, 1, "a");
        match kind {
            StaleRow::MissingJob => {
                let action = ConcurrencyAction::EnqueueTask {
                    start_time_ms,
                    priority: PRIORITY,
                    job_id: job_id.clone(),
                    attempt_number: 1,
                    relative_attempt_number: 1,
                    task_group: TASK_GROUP.to_string(),
                    limit_index: 0,
                    held_queues: Vec::new(),
                    task_id: format!("missing-task-{i:04}"),
                    limits: vec![fixed_limit(queue, 4)],
                };
                batch.put(key, encode_concurrency_action(&action));
            }
            StaleRow::Undecodable => batch.put(key, b"not a concurrency action"),
        }
    }
    batch.merge(
        concurrency_requester_counter_key(TENANT, queue),
        (count as i64).to_le_bytes(),
    );
    shard.db().write(batch).await.expect("write stale requests");
}

/// Write a request row, and its requester counter increment, for a job that
/// starts an hour from now: a waiter no scanner pass grants. With it in
/// place every arrival at `queue` joins the waiters instead of taking a free
/// slot.
async fn write_future_waiter(shard: &JobStoreShard, queue: &str, max_concurrency: u32) {
    let start_time_ms = now_ms() + 60 * 60 * 1000;
    let action = ConcurrencyAction::EnqueueTask {
        start_time_ms,
        priority: PRIORITY,
        job_id: "future-job".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        task_group: TASK_GROUP.to_string(),
        limit_index: 0,
        held_queues: Vec::new(),
        task_id: "future-task".to_string(),
        limits: vec![fixed_limit(queue, max_concurrency)],
    };
    let mut batch = WriteBatch::new();
    batch.put(
        concurrency_request_key(TENANT, queue, start_time_ms, PRIORITY, "future-job", 1, "a"),
        encode_concurrency_action(&action),
    );
    batch.merge(
        concurrency_requester_counter_key(TENANT, queue),
        1i64.to_le_bytes(),
    );
    shard.db().write(batch).await.expect("write future waiter");
}

/// A floating limit's max drops while its holders are busy, a backlog forms
/// behind the low max, and the max rises again by more than one pass can
/// grant. Occupancy follows the max back up within a bounded number of
/// scanner iterations, the slots go to the waiters in priority order, and
/// the scanner goes idle once the queue is full.
#[silo::test]
async fn floating_queue_refills_after_its_max_drops_and_rises() {
    const INITIAL_MAX: u32 = 6;
    const LOW_MAX: u32 = 2;
    const HIGH_MAX: u32 = 40;
    const BACKLOG: usize = 60;
    const URGENT_PRIORITY: u8 = 5;
    const BULK_PRIORITY: u8 = 20;

    let (_tmp, shard) = open_shard_with_scanner_stopped().await;
    let queue = "fl-drop-rise";
    let now = now_ms();

    for i in 0..INITIAL_MAX as usize {
        enqueue(
            &shard,
            &format!("first-{i:03}"),
            vec![floating_limit(queue, INITIAL_MAX)],
            now,
        )
        .await;
    }
    // The backlog alternates bulk and urgent jobs, bulk first.
    for i in 0..BACKLOG {
        let (name, priority) = if i % 2 == 0 {
            ("bulk", BULK_PRIORITY)
        } else {
            ("urgent", URGENT_PRIORITY)
        };
        enqueue_with_priority(
            &shard,
            &format!("{name}-{i:03}"),
            priority,
            vec![floating_limit(queue, INITIAL_MAX)],
            now,
        )
        .await;
    }
    assert_eq!(holders(&shard, queue), INITIAL_MAX as usize);
    assert_eq!(requester_count(&shard, queue).await, BACKLOG as i64);

    // The max drops to LOW_MAX while INITIAL_MAX holders are running.
    let (running, refreshes) = lease_ready(&shard, INITIAL_MAX as usize, 1).await;
    shard
        .report_refresh_success(&refreshes[0], LOW_MAX)
        .await
        .expect("report the lowered max");
    for (_, task_id) in &running {
        succeed(&shard, task_id).await;
    }
    run_scanner_until_idle(&shard, 10).await;
    assert_eq!(
        holders(&shard, queue),
        LOW_MAX as usize,
        "the queue holds the lowered max once the earlier holders finish"
    );
    let backlog_at_low_max = BACKLOG - LOW_MAX as usize;
    assert_eq!(
        requester_count(&shard, queue).await,
        backlog_at_low_max as i64
    );

    // The max rises to HIGH_MAX. The saturated queue's next pass schedules
    // the refresh; a worker reports the raised max.
    backdate_last_refreshed(&shard, queue).await;
    shard.process_concurrency_grants(TENANT, queue, 1).await;
    let (mut granted, refreshes) = lease_ready(&shard, LOW_MAX as usize, 1).await;
    shard
        .report_refresh_success(&refreshes[0], HIGH_MAX)
        .await
        .expect("report the raised max");

    let free_after_raise = (HIGH_MAX - LOW_MAX) as usize;
    assert!(
        free_after_raise > WALK_BUDGET,
        "the raise must exceed what one pass can grant"
    );
    // One pass for the raise itself, then one commit chunk per iteration.
    let iteration_bound = 1 + (free_after_raise - WALK_BUDGET).div_ceil(COMMIT_CHUNK) + 2;
    let iterations = run_scanner_until_idle(&shard, iteration_bound).await;

    assert_eq!(
        holders(&shard, queue),
        HIGH_MAX as usize,
        "occupancy reaches the raised max after {iterations} iterations"
    );
    assert_eq!(
        requester_count(&shard, queue).await,
        (backlog_at_low_max - free_after_raise) as i64,
        "the jobs still waiting are the ones the raised max has no room for"
    );
    assert_eq!(
        shard.pending_grant_count_for_test(TENANT, queue),
        0,
        "a full queue leaves no grant demand queued"
    );

    // HIGH_MAX backlog jobs hold slots: every urgent one, and bulk ones for
    // the rest.
    let (more_granted, _) = lease_ready(&shard, free_after_raise, 0).await;
    granted.extend(more_granted);
    let urgent_granted = granted
        .iter()
        .filter(|(job_id, _)| job_id.starts_with("urgent-"))
        .count();
    let bulk_granted = granted
        .iter()
        .filter(|(job_id, _)| job_id.starts_with("bulk-"))
        .count();
    assert_eq!(
        urgent_granted,
        BACKLOG / 2,
        "every urgent waiter is granted"
    );
    assert_eq!(bulk_granted, HIGH_MAX as usize - BACKLOG / 2);
}

/// Jobs gated by a small fixed queue and then a floating limit hold the
/// fixed queue's slots while they wait on the floating limit behind a
/// backlog. When the floating max rises, the running scanner grants the
/// whole backlog, the parked jobs start, and the fixed queue turns over
/// until every job behind it has run.
#[silo::test]
async fn jobs_parked_on_a_floating_limit_start_and_their_first_queue_turns_over() {
    const USER_MAX: u32 = 4;
    const USER_JOBS: usize = 10;
    const LOW_MAX: u32 = 2;
    const HIGH_MAX: u32 = 64;
    const FLOATING_ONLY_BACKLOG: usize = 30;

    let (_tmp, shard) = open_temp_shard_with_scanner_config_and_reconcile_interval_ms(
        small_scanner_config(),
        NO_SWEEP_INTERVAL_MS,
    )
    .await;
    let user_queue = "user-queue";
    let floating_queue = "fl-platform";
    let now = now_ms();

    // The floating limit fills, and a backlog of floating-only jobs forms.
    for i in 0..(LOW_MAX as usize + FLOATING_ONLY_BACKLOG) {
        enqueue(
            &shard,
            &format!("floating-only-{i:03}"),
            vec![floating_limit(floating_queue, LOW_MAX)],
            now - 2_000,
        )
        .await;
    }
    // Two-limit jobs arrive behind that backlog. USER_MAX of them take the
    // user queue's slots and park on the floating limit; the rest wait on the
    // user queue.
    let mut user_jobs: HashSet<String> = HashSet::new();
    for i in 0..USER_JOBS {
        let id = enqueue(
            &shard,
            &format!("user-{i:03}"),
            vec![
                fixed_limit(user_queue, USER_MAX),
                floating_limit(floating_queue, LOW_MAX),
            ],
            now - 1_000,
        )
        .await;
        user_jobs.insert(id);
    }
    assert_eq!(holders(&shard, user_queue), USER_MAX as usize);
    assert_eq!(
        requester_count(&shard, user_queue).await,
        (USER_JOBS - USER_MAX as usize) as i64
    );
    assert_eq!(holders(&shard, floating_queue), LOW_MAX as usize);
    let parked_on_floating = FLOATING_ONLY_BACKLOG + USER_MAX as usize;
    assert_eq!(
        requester_count(&shard, floating_queue).await,
        parked_on_floating as i64
    );
    assert!(
        parked_on_floating > WALK_BUDGET,
        "the backlog must exceed what one pass can grant"
    );

    // The floating max rises with nothing completing.
    let (running, refreshes) = lease_ready(&shard, LOW_MAX as usize, 1).await;
    shard
        .report_refresh_success(&refreshes[0], HIGH_MAX)
        .await
        .expect("report the raised max");

    let expected_floating_holders = LOW_MAX as usize + parked_on_floating;
    let floating_holders = poll_until(
        || async { holders(&shard, floating_queue) },
        |&count| count == expected_floating_holders,
        WAIT.as_millis() as u64,
    )
    .await;
    assert_eq!(
        floating_holders, expected_floating_holders,
        "every job waiting on the floating limit is granted once its max has room for all of them"
    );
    assert_eq!(requester_count(&shard, floating_queue).await, 0);
    assert_eq!(
        holders(&shard, user_queue),
        USER_MAX as usize,
        "the parked jobs still hold their user queue slots while they run"
    );

    // Workers run everything. Each finished user job frees a user queue
    // slot for a job waiting behind it.
    let mut finished_user_jobs: HashSet<String> = HashSet::new();
    let mut to_finish = running;
    let deadline = std::time::Instant::now() + WAIT;
    while finished_user_jobs.len() < USER_JOBS {
        for (job_id, task_id) in to_finish.drain(..) {
            succeed(&shard, &task_id).await;
            if user_jobs.contains(&job_id) {
                finished_user_jobs.insert(job_id);
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out with {} of {USER_JOBS} user queue jobs finished",
            finished_user_jobs.len()
        );
        let batch = shard
            .dequeue("worker", TASK_GROUP, 50)
            .await
            .expect("dequeue");
        to_finish.extend(batch.tasks.iter().map(|task| {
            (
                task.job().id().to_string(),
                task.attempt().task_id().to_string(),
            )
        }));
        if to_finish.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    assert_eq!(finished_user_jobs, user_jobs);
    assert_eq!(requester_count(&shard, user_queue).await, 0);
}

/// A release nudges the scanner for one grant, and the pass spends its whole
/// walk budget deleting request rows of jobs that no longer exist. The freed
/// slot still goes to the first live waiter on a following iteration.
#[silo::test]
async fn slot_is_filled_after_a_pass_spends_its_walk_budget_on_stale_requests() {
    assert_slot_is_filled_behind_a_stale_front("stale-front", StaleRow::MissingJob).await;
}

/// The same release, with a front of request rows that do not decode: the
/// first pass deletes its walk budget of them without ever learning the
/// queue's capacity, and the freed slot is still filled.
#[silo::test]
async fn slot_is_filled_after_a_pass_spends_its_walk_budget_on_undecodable_requests() {
    assert_slot_is_filled_behind_a_stale_front("undecodable-front", StaleRow::Undecodable).await;
}

async fn assert_slot_is_filled_behind_a_stale_front(queue: &str, kind: StaleRow) {
    const MAX: u32 = 4;
    const STALE: usize = WALK_BUDGET + 4;
    const WAITERS: usize = 3;

    let (_tmp, shard) = open_shard_with_scanner_stopped().await;
    let now = now_ms();

    for i in 0..MAX as usize {
        enqueue(
            &shard,
            &format!("holder-{i}"),
            vec![fixed_limit(queue, MAX)],
            now,
        )
        .await;
    }
    write_stale_requests(&shard, queue, kind, STALE, now).await;
    for i in 0..WAITERS {
        enqueue(
            &shard,
            &format!("waiter-{i}"),
            vec![fixed_limit(queue, MAX)],
            now,
        )
        .await;
    }
    assert_eq!(holders(&shard, queue), MAX as usize);
    assert_eq!(
        requester_count(&shard, queue).await,
        (STALE + WAITERS) as i64
    );

    let (running, _) = lease_ready(&shard, MAX as usize, 0).await;
    succeed(&shard, &running[0].1).await;
    assert_eq!(holders(&shard, queue), MAX as usize - 1);
    assert_eq!(shard.hot_pending_grant_count_for_test(TENANT, queue), 1);

    // First iteration: the release's pass walks WALK_BUDGET stale rows and
    // grants nothing.
    let first = run_scanner_iteration(&shard).await;
    assert_eq!(first, vec![(queue.to_string(), 1)]);
    assert_eq!(holders(&shard, queue), MAX as usize - 1);
    assert_eq!(
        requester_count(&shard, queue).await,
        (STALE - WALK_BUDGET + WAITERS) as i64,
        "the first pass deletes exactly its walk budget of stale rows"
    );

    let more_iterations = run_scanner_until_idle(&shard, 3).await;
    assert_eq!(
        holders(&shard, queue),
        MAX as usize,
        "the freed slot is filled after {more_iterations} more iterations"
    );
    assert_eq!(
        requester_count(&shard, queue).await,
        (WAITERS - 1) as i64,
        "one live waiter was granted and every stale row is gone"
    );
}

/// One queue has a deep backlog and far more free capacity than a pass can
/// fill, and several small queues release slots while it catches up. Every
/// small queue's waiter is granted in the first iteration after its release,
/// and the deep queue's share of each of those iterations is one commit
/// chunk.
#[silo::test]
async fn small_queues_are_served_every_iteration_while_a_deep_queue_catches_up() {
    const DEEP_MAX: u32 = 200;
    const DEEP_BACKLOG: usize = 200;
    const SMALL_QUEUES: usize = 5;
    const SMALL_WAITERS: usize = 2;

    let (_tmp, shard) = open_shard_with_scanner_stopped().await;
    let deep_queue = "deep";
    let now = now_ms();

    // Each small queue: one running holder and SMALL_WAITERS waiters.
    let small_queues: Vec<String> = (0..SMALL_QUEUES).map(|i| format!("small-{i}")).collect();
    for queue in &small_queues {
        for j in 0..=SMALL_WAITERS {
            enqueue(
                &shard,
                &format!("{queue}-job-{j}"),
                vec![fixed_limit(queue, 1)],
                now,
            )
            .await;
        }
        assert_eq!(holders(&shard, queue), 1);
        assert_eq!(requester_count(&shard, queue).await, SMALL_WAITERS as i64);
    }
    let (running, _) = lease_ready(&shard, SMALL_QUEUES, 0).await;
    let mut small_holder_task: std::collections::HashMap<String, String> = running
        .into_iter()
        .map(|(job_id, task_id)| {
            let queue = job_id
                .split("-job-")
                .next()
                .expect("small queue job id")
                .to_string();
            (queue, task_id)
        })
        .collect();

    // The deep queue: no holders, DEEP_BACKLOG ready waiters, and no grant
    // demand queued for it. Each arrival joins the waiters behind the future
    // waiter and nudges the scanner; discarding those nudges leaves the queue
    // with free capacity nothing has asked the scanner to fill.
    write_future_waiter(&shard, deep_queue, DEEP_MAX).await;
    for i in 0..DEEP_BACKLOG {
        enqueue(
            &shard,
            &format!("deep-job-{i:03}"),
            vec![fixed_limit(deep_queue, DEEP_MAX)],
            now,
        )
        .await;
    }
    let discarded = shard.take_pending_grant_batch_for_test();
    assert!(discarded.iter().all(|(_, queue, _)| queue == deep_queue));
    assert_eq!(holders(&shard, deep_queue), 0);
    assert_eq!(
        requester_count(&shard, deep_queue).await,
        DEEP_BACKLOG as i64 + 1
    );

    // A single nudge is all the deep queue gets.
    shard.seed_hot_pending_grant_for_test(TENANT, deep_queue, 1);
    let first = run_scanner_iteration(&shard).await;
    assert_eq!(first, vec![(deep_queue.to_string(), 1)]);
    assert_eq!(holders(&shard, deep_queue), 1);

    // While the deep queue catches up, release one small-queue slot before
    // each iteration.
    let mut iterations = 1;
    for round in 0..SMALL_WAITERS {
        for queue in &small_queues {
            let deep_before = holders(&shard, deep_queue);
            assert!(
                deep_before < DEEP_MAX as usize,
                "the deep queue is still catching up"
            );
            let waiting_before = requester_count(&shard, queue).await;

            let task_id = small_holder_task.remove(queue).expect("small queue holder");
            succeed(&shard, &task_id).await;

            let taken = run_scanner_iteration(&shard).await;
            iterations += 1;

            assert!(
                taken.contains(&(queue.clone(), 1)),
                "round {round}: the iteration after {queue}'s release serves it; took {taken:?}"
            );
            assert_eq!(
                holders(&shard, queue),
                1,
                "{queue}'s freed slot is refilled"
            );
            assert_eq!(requester_count(&shard, queue).await, waiting_before - 1);

            let deep_work: Vec<u32> = taken
                .iter()
                .filter(|(taken_queue, _)| taken_queue == deep_queue)
                .map(|(_, count)| *count)
                .collect();
            assert_eq!(
                deep_work,
                vec![COMMIT_CHUNK as u32],
                "the deep queue's share of the iteration is one commit chunk"
            );
            assert_eq!(holders(&shard, deep_queue), deep_before + COMMIT_CHUNK);

            // The granted waiter becomes the queue's next holder.
            let (leased, _) = lease_ready(&shard, 1, 0).await;
            for (job_id, task_id) in leased {
                let leased_queue = job_id
                    .split("-job-")
                    .next()
                    .expect("small queue job id")
                    .to_string();
                small_holder_task.insert(leased_queue, task_id);
            }
        }
    }

    // Left alone, the deep queue reaches its max one chunk per iteration.
    let remaining = DEEP_MAX as usize - holders(&shard, deep_queue);
    let bound = remaining.div_ceil(COMMIT_CHUNK) + 2;
    iterations += run_scanner_until_idle(&shard, bound).await;
    assert_eq!(
        holders(&shard, deep_queue),
        DEEP_MAX as usize,
        "the deep queue is full after {iterations} iterations"
    );
    assert_eq!(requester_count(&shard, deep_queue).await, 1);
}
