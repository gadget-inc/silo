//! Reproductions for double materialization of terminal task rows.
//!
//! A job's limit chain must keep at most one live terminal task row
//! (`RunAttempt` or `CheckRateLimit`) per `(job_id, attempt)`. The task key's
//! trailing `epoch_ms` is a write-time disambiguator, not identity, so any two
//! writers that stamp different epochs coexist and the same attempt dispatches
//! twice. Each test here arranges one candidate mechanism directly and asserts
//! the invariant via `assert_single_live_terminal_task_row_per_attempt`;
//! `#[ignore]`d tests are red reproductions of mechanisms the grant /
//! chain-resume / request-row path does not yet prevent.

mod test_helpers;

use silo::codec::encode_task;
use silo::concurrency::ResumeChainParams;
use silo::job::{ConcurrencyLimit, GubernatorRateLimit, Limit};
use silo::job_attempt::AttemptOutcome;
use silo::task::{ConcurrencyAction, Task};
use slatedb::WriteBatch;

use test_helpers::*;

const QUEUE: &str = "dup-q";
const TASK_GROUP: &str = "default";

fn one_concurrency_limit(max_concurrency: u32) -> Vec<Limit> {
    vec![Limit::Concurrency(ConcurrencyLimit {
        key: QUEUE.to_string(),
        max_concurrency,
    })]
}

/// Enqueue a job whose chain parks without a live terminal row: the start time
/// is far in the future, so the walk writes a future `RequestTicket` and the
/// job sits in `Scheduled` at attempt 1. Returns the job id.
async fn enqueue_parked_job(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    limits: Vec<Limit>,
) -> String {
    shard
        .enqueue(
            tenant,
            None,
            10,
            now_ms() + 3_600_000,
            None,
            msgpack_payload(&serde_json::json!({})),
            limits,
            None,
            TASK_GROUP,
        )
        .await
        .expect("enqueue parked job")
}

/// Hand-write a deferred concurrency request row for `job_id`'s attempt 1,
/// carrying `task_id` as the chain task id — the durable state
/// `append_request_edits` produces. `suffix` stands in for the random key
/// suffix, so repeated calls create additional rows for the same attempt
/// instead of colliding.
async fn write_request_row(
    shard: &silo::job_store_shard::JobStoreShard,
    tenant: &str,
    job_id: &str,
    task_id: &str,
    start_time_ms: i64,
    suffix: &str,
    limits: &[Limit],
) {
    let action = ConcurrencyAction::EnqueueTask {
        start_time_ms,
        priority: 10,
        job_id: job_id.to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        task_group: TASK_GROUP.to_string(),
        limit_index: 0,
        held_queues: Vec::new(),
        task_id: task_id.to_string(),
        limits: limits.to_vec(),
    };
    let key =
        silo::keys::concurrency_request_key(tenant, QUEUE, start_time_ms, 10, job_id, 1, suffix);
    let mut batch = WriteBatch::new();
    batch.put(&key, &silo::codec::encode_concurrency_action(&action));
    shard.db().write(batch).await.expect("write request row");
    shard.db().flush().await.expect("flush request row");
}

/// Count live RunAttempt task rows for a job across the task keyspace.
async fn count_live_run_attempt_rows(
    shard: &silo::job_store_shard::JobStoreShard,
    job_id: &str,
) -> usize {
    let prefix = silo::keys::task_group_prefix(TASK_GROUP);
    let end = silo::keys::end_bound(&prefix);
    let mut iter = shard
        .db()
        .scan::<Vec<u8>, _>(prefix..end)
        .await
        .expect("scan tasks");
    let mut count = 0;
    while let Some(kv) = iter.next().await.expect("iterate tasks") {
        if let Ok(Task::RunAttempt { job_id: jid, .. }) = silo::codec::decode_task(&kv.value)
            && jid == job_id
        {
            count += 1;
        }
    }
    count
}

/// The invariant helper itself must detect a hand-planted duplicate: two live
/// RunAttempt rows for one `(job_id, attempt)` at different epochs.
#[silo::test]
async fn invariant_helper_reports_hand_planted_duplicate_terminal_rows() {
    let (_tmp, shard) = open_temp_shard().await;
    let start = now_ms() - 1_000;

    let task = Task::RunAttempt {
        id: "planted-task".to_string(),
        tenant: "-".to_string(),
        job_id: "planted-job".to_string(),
        attempt_number: 1,
        relative_attempt_number: 1,
        held_queues: vec![],
        task_group: TASK_GROUP.to_string(),
    };
    let mut batch = WriteBatch::new();
    for epoch in [start, start + 1] {
        batch.put(
            &silo::keys::task_key(TASK_GROUP, start, 10, "planted-job", 1, epoch),
            &encode_task(&task),
        );
    }
    shard.db().write(batch).await.expect("write duplicates");

    let violations = duplicate_live_terminal_task_rows(shard.db()).await;
    assert_eq!(
        violations.len(),
        1,
        "expected exactly one violation group, got {violations:?}"
    );
    assert!(
        violations[0].contains("planted-job") && violations[0].contains("2 live terminal"),
        "violation should name the job and the row count: {violations:?}"
    );
}

/// R1 — two request rows for one `(job_id, attempt)` (the state the random
/// request-key suffix permits) granted by two separate scanner invocations.
/// Each invocation stamps its own epoch on the resumed chain's terminal write,
/// so the second grant materializes a second live RunAttempt row.
#[silo::test]
#[ignore = "red reproduction: duplicate request rows granted across scanner invocations \
            materialize two terminal rows for one attempt (random request-key suffix + \
            blind terminal write + per-chunk epoch stamping)"]
async fn duplicate_request_rows_granted_across_invocations_keep_single_terminal_row() {
    let (_tmp, shard) = open_temp_shard().await;
    shard.stop_grant_scanner();
    let tenant = "dup-cross-invocation";
    let limits = one_concurrency_limit(2);

    let job_id = enqueue_parked_job(&shard, tenant, limits.clone()).await;
    let ready_at = now_ms() - 1_000;
    write_request_row(
        &shard,
        tenant,
        &job_id,
        "dup-chain-task",
        ready_at,
        "aaaa0001",
        &limits,
    )
    .await;
    write_request_row(
        &shard,
        tenant,
        &job_id,
        "dup-chain-task",
        ready_at,
        "bbbb0002",
        &limits,
    )
    .await;

    let first = shard.process_concurrency_grants(tenant, QUEUE, 1).await;
    assert_eq!(first.len(), 1, "first invocation must grant one request");
    // A later invocation stamps a fresh chunk epoch.
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    let second = shard.process_concurrency_grants(tenant, QUEUE, 1).await;
    assert_eq!(
        second.len(),
        1,
        "second invocation must grant the duplicate"
    );

    assert_single_live_terminal_task_row_per_attempt(shard.db()).await;
}

/// R2 — the same duplicate request rows granted within ONE scanner commit
/// chunk share the chunk's epoch, so both terminal writes land at the same
/// task key and collapse to a single live row. Pins the same-chunk collision
/// as regression coverage.
#[silo::test]
async fn duplicate_request_rows_granted_in_one_chunk_keep_single_terminal_row() {
    let (_tmp, shard) = open_temp_shard().await;
    shard.stop_grant_scanner();
    let tenant = "dup-same-chunk";
    let limits = one_concurrency_limit(2);

    let job_id = enqueue_parked_job(&shard, tenant, limits.clone()).await;
    let ready_at = now_ms() - 1_000;
    write_request_row(
        &shard,
        tenant,
        &job_id,
        "dup-chain-task",
        ready_at,
        "aaaa0001",
        &limits,
    )
    .await;
    write_request_row(
        &shard,
        tenant,
        &job_id,
        "dup-chain-task",
        ready_at,
        "bbbb0002",
        &limits,
    )
    .await;

    let granted = shard.process_concurrency_grants(tenant, QUEUE, 2).await;
    assert_eq!(
        granted.len(),
        2,
        "both duplicate rows must be granted in one pass (one commit chunk)"
    );

    assert_single_live_terminal_task_row_per_attempt(shard.db()).await;
    assert_eq!(
        count_live_run_attempt_rows(&shard, &job_id).await,
        1,
        "same-chunk grants share one epoch, so the terminal writes collide at one key"
    );
}

/// R3 — a RequestTicket replayed after its at-capacity conversion committed:
/// the durable request row from the conversion AND the replayed ticket both
/// continue the same chain. The scanner grants the row while the job is still
/// `Scheduled`; the replayed ticket grants again at dequeue; both terminal
/// rows dispatch to the worker — one attempt delivered twice, the second over
/// the first's live lease.
#[silo::test]
#[ignore = "red reproduction: a replayed at-capacity RequestTicket conversion leaves both a \
            request row and a re-processable ticket for one attempt; the scanner and the \
            ticket each materialize a terminal row and both dispatch (source=stored)"]
async fn request_ticket_replayed_after_landed_conversion_delivers_attempt_once() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    shard.stop_grant_scanner();
    let tenant = "dup-ticket-replay";
    let limits = one_concurrency_limit(2);

    let job_id = enqueue_parked_job(&shard, tenant, limits.clone()).await;

    // The chain's ticket sits future-dated; read its task id — the conversion
    // and the replay both carry it.
    let (ticket_key, ticket_bytes) = first_task_kv(shard.db()).await.expect("ticket present");
    let Ok(Task::RequestTicket {
        task_id, priority, ..
    }) = silo::codec::decode_task(&ticket_bytes)
    else {
        panic!("expected the parked job's RequestTicket");
    };

    // Durable state of the landed conversion: a request row for the attempt,
    // with the future ticket replaced by the replayed ready-dated copy (the
    // broker re-buffered the ticket whose conversion commit landed).
    let ready_at = now_ms() - 1_000;
    write_request_row(
        &shard, tenant, &job_id, &task_id, ready_at, "aaaa0001", &limits,
    )
    .await;
    let replayed = Task::RequestTicket {
        queue: QUEUE.to_string(),
        start_time_ms: ready_at,
        priority,
        tenant: tenant.to_string(),
        job_id: job_id.clone(),
        attempt_number: 1,
        relative_attempt_number: 1,
        task_id: task_id.clone(),
        task_group: TASK_GROUP.to_string(),
        limit_index: 0,
        held_queues: vec![],
        limits: limits.clone(),
    };
    let replayed_key = silo::keys::task_key(TASK_GROUP, ready_at, priority, &job_id, 1, ready_at);
    let mut batch = WriteBatch::new();
    batch.put(&replayed_key, &encode_task(&replayed));
    batch.delete(&ticket_key);
    shard
        .db()
        .write(batch)
        .await
        .expect("write replayed ticket");
    shard.db().flush().await.expect("flush replayed ticket");

    // The scanner grants the conversion's request row: the chain's terminal
    // RunAttempt row exists, undispatched, and the job is still Scheduled.
    let granted = shard.process_concurrency_grants(tenant, QUEUE, 1).await;
    assert_eq!(
        granted.len(),
        1,
        "the landed conversion's request row must validate and grant"
    );
    assert_eq!(count_live_run_attempt_rows(&shard, &job_id).await, 1);

    // A worker polls: the replayed ticket grants the chain AGAIN and writes a
    // second terminal row; both rows dispatch. A single execution total means
    // exactly one delivery and no lease overwrite.
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    shard
        .force_buffer_tasks_for_test(vec![replayed_key])
        .await
        .expect("buffer replayed ticket");
    let mut deliveries = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let out = shard
            .dequeue("worker-replay", TASK_GROUP, 4)
            .await
            .expect("dequeue")
            .tasks;
        deliveries.extend(out.iter().map(|t| t.attempt().task_id().to_string()));
        if count_live_run_attempt_rows(&shard, &job_id).await == 0 && !deliveries.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    assert_single_live_terminal_task_row_per_attempt(shard.db()).await;
    assert_eq!(
        deliveries.len(),
        1,
        "one attempt must be delivered exactly once, got deliveries {deliveries:?}"
    );
    let body = gather_metrics_text(&metrics);
    let overwrites = metric_value_or_zero(
        &body,
        &["silo_task_lease_overwrites_total", "task_group=\"default\""],
    );
    assert_eq!(overwrites, 0.0, "no lease overwrite may fire");
}

/// R4 — two chain resumes for one attempt in two separate write batches (the
/// shape a scanner grant racing a dequeue-driven resume produces): each
/// writer's durable read cannot see the other's uncommitted batch, and each
/// stamps its own `now_ms` epoch, so both terminal writes survive commit.
#[silo::test]
#[ignore = "red reproduction: two chain resumes for one attempt against separate write \
            batches each pass their own durable read and stamp distinct epochs — both \
            terminal rows commit"]
async fn concurrent_chain_resumes_in_two_batches_keep_single_terminal_row() {
    let (_tmp, shard) = open_temp_shard().await;
    shard.stop_grant_scanner();
    let tenant = "dup-two-batch";
    let limits = one_concurrency_limit(2);

    let job_id = enqueue_parked_job(&shard, tenant, limits.clone()).await;
    let resumer = shard
        .take_chain_resumer_for_test()
        .expect("chain resumer installed at shard open");

    let start_at = now_ms() - 1_000;
    let resume_params = |epoch_now: i64| ResumeChainParams {
        tenant: tenant.to_string(),
        task_id: "dup-chain-task".to_string(),
        job_id: job_id.clone(),
        attempt_number: 1,
        relative_attempt_number: 1,
        // Past the final limit: the resumer writes the terminal RunAttempt.
        limit_index: 1,
        priority: 10,
        start_at_ms: start_at,
        held_queues: vec![QUEUE.to_string()],
        task_group: TASK_GROUP.to_string(),
        limits: limits.clone(),
        now_ms: epoch_now,
        read_cache: None,
    };

    let epoch_base = now_ms();
    for offset in [0, 1] {
        let mut batch = WriteBatch::new();
        let grants = resumer
            .resume_chain(&mut batch, resume_params(epoch_base + offset))
            .await
            .expect("resume chain");
        assert!(
            grants.is_empty(),
            "terminal resume makes no further reservations"
        );
        shard.db().write(batch).await.expect("commit resume batch");
    }

    assert_single_live_terminal_task_row_per_attempt(shard.db()).await;
}

/// R5 — a replayed CheckRateLimit continuation: two live CheckRateLimit rows
/// for one attempt (the durable state a replay produces) each re-enter the
/// chain and each writes a RunAttempt at its own key. Both dispatch to the
/// same worker — the second over the first's still-live lease — so one
/// attempt is delivered twice.
#[silo::test]
#[ignore = "red reproduction: a replayed CheckRateLimit continuation re-enters the chain \
            with the same task id and materializes a second RunAttempt; both dispatch, \
            the second over the first's live lease (source=stored)"]
async fn replayed_check_rate_limit_continuation_delivers_attempt_once() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    shard.stop_grant_scanner();
    let tenant = "dup-crl-replay";
    let limits = vec![Limit::RateLimit(GubernatorRateLimit::new(
        "dup-rl",
        "dup-rl-key",
        100,
        60_000,
    ))];

    // Enqueue with a past start so the chain writes a ready CheckRateLimit row.
    let start_a = now_ms() - 2_000;
    let job_id = shard
        .enqueue(
            tenant,
            None,
            10,
            start_a,
            None,
            msgpack_payload(&serde_json::json!({})),
            limits,
            None,
            TASK_GROUP,
        )
        .await
        .expect("enqueue rate-limited job");

    let (_crl_key, crl_bytes) = first_task_kv(shard.db()).await.expect("check row present");
    let Ok(Task::CheckRateLimit { .. }) = silo::codec::decode_task(&crl_bytes) else {
        panic!("expected the job's CheckRateLimit row");
    };

    // The replay's durable state: a second live CheckRateLimit row for the
    // same attempt at a different key (later start keeps the two rows' chain
    // continuations at distinct task keys regardless of epoch collisions).
    let start_b = start_a + 1_000;
    let replay_key = silo::keys::task_key(TASK_GROUP, start_b, 10, &job_id, 1, start_b);
    let mut batch = WriteBatch::new();
    batch.put(&replay_key, &crl_bytes);
    shard.db().write(batch).await.expect("write replayed row");
    shard.db().flush().await.expect("flush replayed row");

    // One dequeue loop processes both continuations and the RunAttempts they
    // write. A single execution total means one delivery and no lease
    // overwrite.
    let mut deliveries = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let out = shard
            .dequeue("worker-crl", TASK_GROUP, 4)
            .await
            .expect("dequeue")
            .tasks;
        deliveries.extend(out.iter().map(|t| t.attempt().task_id().to_string()));
        if count_task_keys(shard.db()).await == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    assert_single_live_terminal_task_row_per_attempt(shard.db()).await;
    assert_eq!(
        deliveries.len(),
        1,
        "one attempt must be delivered exactly once, got deliveries {deliveries:?}"
    );
    let body = gather_metrics_text(&metrics);
    let overwrites = metric_value_or_zero(
        &body,
        &["silo_task_lease_overwrites_total", "task_group=\"default\""],
    );
    assert_eq!(overwrites, 0.0, "no lease overwrite may fire");
}

/// R6 — redispatch over another worker's still-live lease (the
/// production-confirmed cancelled-dequeue shape): the row is the SAME
/// materialization delivered again, so it must deliver (lost-response
/// recovery), count a stored overwrite, and leave no duplicate terminal row.
#[silo::test]
async fn redispatch_over_other_workers_live_lease_delivers_without_duplicate_row() {
    let (_tmp, shard, metrics) = open_temp_shard_with_metrics().await;
    let tenant = "dup-redispatch";
    let now = now_ms();

    shard
        .enqueue(
            tenant,
            None,
            10,
            now,
            None,
            msgpack_payload(&serde_json::json!({})),
            vec![],
            None,
            TASK_GROUP,
        )
        .await
        .expect("enqueue");

    let (_key, task_bytes) = first_task_kv(shard.db()).await.expect("task present");
    let Ok(Task::RunAttempt {
        id: task_id,
        job_id,
        ..
    }) = silo::codec::decode_task(&task_bytes)
    else {
        panic!("expected RunAttempt task");
    };

    // The still-live lease a dropped-mid-commit dispatch to worker A leaves
    // behind (its response never reached A).
    let lease = silo::task::LeaseRecord {
        worker_id: "worker-a".to_string(),
        task: Task::RunAttempt {
            id: task_id.clone(),
            tenant: tenant.to_string(),
            job_id: job_id.clone(),
            attempt_number: 1,
            relative_attempt_number: 1,
            held_queues: vec![],
            task_group: TASK_GROUP.to_string(),
        },
        expiry_ms: now + silo::task::DEFAULT_LEASE_MS,
        started_at_ms: now,
    };
    let mut batch = WriteBatch::new();
    batch.put(
        &silo::keys::leased_task_key(&task_id),
        &silo::codec::encode_lease(&lease),
    );
    shard.db().write(batch).await.expect("write lease");
    shard.db().flush().await.expect("flush lease");

    let result = shard
        .dequeue("worker-b", TASK_GROUP, 1)
        .await
        .expect("dequeue");
    assert_eq!(result.tasks.len(), 1, "the redispatch must deliver");

    let body = gather_metrics_text(&metrics);
    let stored = metric_value_or_zero(
        &body,
        &[
            "silo_task_lease_overwrites_total",
            "task_group=\"default\"",
            "source=\"stored\"",
        ],
    );
    assert_eq!(stored, 1.0, "the redispatch counts one stored overwrite");
    assert_single_live_terminal_task_row_per_attempt(shard.db()).await;
    assert_eq!(
        count_live_run_attempt_rows(&shard, &job_id).await,
        0,
        "the redispatch consumed the single materialization; no duplicate row exists"
    );

    // Reporting the outcome as worker-b completes the attempt normally.
    let delivered_task_id = result.tasks[0].attempt().task_id().to_string();
    shard
        .report_attempt_outcome(
            &delivered_task_id,
            AttemptOutcome::Success { result: vec![] },
        )
        .await
        .expect("report outcome");
}
