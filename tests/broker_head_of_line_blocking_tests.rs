//! Regression test for broker head-of-line blocking by parked RequestTickets.
//!
//! Production incident (2026-06): one tenant enqueued ~29k future-scheduled jobs
//! gated by a user queue with max_concurrency=1. One job won the only slot; the
//! rest became `Task::RequestTicket` entries parked at the front of the task
//! group's key range. An at-capacity ticket is left in place by
//! `handle_request_ticket` (ack_release) to be retried on a later scan, so the
//! tickets never leave the DB while the slot is held. The broker scan always
//! restarts from the front of the group's key range and claims in key order,
//! so with more unconsumable tickets than a scan batch (4096) the broker never
//! makes progress past them — starving every other tenant's tasks in the same
//! task group on the same shard, even tasks that already hold all their
//! concurrency slots and are ready to run.
//!
//! This test reproduces the wedge with two tenants on one shard and asserts
//! the desired behavior: tenant B's runnable jobs must be dequeued even while
//! tenant A's parked tickets clog the front of the task group.

mod test_helpers;

use std::sync::Arc;
use std::time::Duration;

use silo::job::{ConcurrencyLimit, FloatingConcurrencyLimit, Limit};
use test_helpers::*;

const TASK_GROUP: &str = "shared-actions";
const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

/// Must exceed the broker's scan_batch (4096) so a single scan pass can never
/// read past tenant A's parked tickets.
const TENANT_A_JOBS: usize = 4_300;
const TENANT_B_JOBS: usize = 3;

/// How far in the future tenant A's jobs are scheduled. Enqueueing all of
/// TENANT_A_JOBS must finish within this window so every job takes the
/// future-scheduled path and writes a parked `Task::RequestTicket` (an
/// immediate-start enqueue writes a deferred request record instead, which
/// does not occupy the task-group key range).
const START_DELAY_MS: i64 = 15_000;

/// Long enough that no RefreshFloatingLimit task fires during the test.
const REFRESH_INTERVAL_MS: i64 = 3_600_000;

fn limits(queue: &str, max_concurrency: u32, floating_queue: &str) -> Vec<Limit> {
    vec![
        // The tight fixed limit must precede the floating limit — silo walks
        // limits in client order (see walk_limit_chain), matching production.
        Limit::Concurrency(ConcurrencyLimit {
            key: queue.to_string(),
            max_concurrency,
        }),
        Limit::FloatingConcurrency(FloatingConcurrencyLimit {
            key: floating_queue.to_string(),
            default_max_concurrency: 360,
            refresh_interval_ms: REFRESH_INTERVAL_MS,
            metadata: vec![],
        }),
    ]
}

#[silo::test]
async fn tenant_b_is_not_starved_by_tenant_a_parked_request_tickets() {
    with_timeout!(150_000, {
        let (_tmp, shard) = open_temp_shard().await;

        // Both tenants must land on the same shard — the wedge is per
        // (shard, task_group). The temp shard owns the full tenant range.
        let range = shard.get_range();
        assert!(
            range.contains_tenant(TENANT_A) && range.contains_tenant(TENANT_B),
            "test setup: both tenants must map to this shard"
        );

        let payload = msgpack_payload(&serde_json::json!({"k": "v"}));
        let start_a = now_ms() + START_DELAY_MS;

        // Tenant A: future-scheduled jobs gated by a queue with
        // max_concurrency=1. Each enqueue writes a parked RequestTicket task
        // keyed at start_a in the shared task group. Enqueue concurrently so
        // commits share SlateDB flush windows.
        let mut enqueued = 0usize;
        while enqueued < TENANT_A_JOBS {
            let chunk = (TENANT_A_JOBS - enqueued).min(512);
            let mut join_set = tokio::task::JoinSet::new();
            for _ in 0..chunk {
                let shard = Arc::clone(&shard);
                let payload = payload.clone();
                join_set.spawn(async move {
                    shard
                        .enqueue(
                            TENANT_A,
                            None,
                            10u8,
                            start_a,
                            None,
                            payload,
                            limits("queue-a", 1, "surge-a"),
                            None,
                            TASK_GROUP,
                        )
                        .await
                });
            }
            while let Some(res) = join_set.join_next().await {
                res.expect("enqueue task panicked").expect("enqueue A");
            }
            enqueued += chunk;
        }
        assert!(
            now_ms() < start_a,
            "test setup too slow: tenant A enqueues overran the future start \
             time, so some jobs missed the RequestTicket path; increase \
             START_DELAY_MS"
        );

        // Every tenant A job must be a parked RequestTicket in the task group.
        let parked =
            count_with_binary_prefix(shard.db(), &silo::keys::task_group_prefix(TASK_GROUP)).await;
        assert_eq!(
            parked, TENANT_A_JOBS,
            "expected one parked RequestTicket per tenant A job"
        );

        // Wait until tenant A's tickets become ready (scannable).
        let wait = (start_a - now_ms()).max(0) as u64 + 200;
        tokio::time::sleep(Duration::from_millis(wait)).await;

        // Tenant B: immediate jobs on its own queue (max_concurrency=3). All
        // three are granted their fixed + floating slots at enqueue time; the
        // resulting RunAttempt tasks land in the same task group *behind*
        // tenant A's tickets (later start time). They are fully runnable —
        // only the broker stands between them and a worker.
        for i in 0..TENANT_B_JOBS {
            shard
                .enqueue(
                    TENANT_B,
                    Some(format!("b-{i}")),
                    10u8,
                    0, // immediate
                    None,
                    payload.clone(),
                    limits("queue-b", TENANT_B_JOBS as u32, "surge-b"),
                    None,
                    TASK_GROUP,
                )
                .await
                .expect("enqueue B");
        }
        assert_eq!(
            count_concurrency_holders_for_tenant(shard.db(), TENANT_B).await,
            TENANT_B_JOBS * 2,
            "tenant B jobs should hold both their fixed and floating slots \
             immediately after enqueue"
        );

        // Drive dequeues like production workers. The first granted tenant A
        // ticket takes queue-a's only slot and its RunAttempt is leased (we
        // never complete it, so the slot is held for the rest of the test and
        // all remaining tenant A tickets stay at capacity, recycling through
        // the broker scan window). Tenant B's three RunAttempts must still be
        // dequeued.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut a_leased = 0usize;
        let mut b_leased = 0usize;
        while std::time::Instant::now() < deadline {
            let result = shard
                .dequeue("worker-1", TASK_GROUP, 10)
                .await
                .expect("dequeue");
            for task in &result.tasks {
                if task.job().id().starts_with("b-") {
                    b_leased += 1;
                } else {
                    a_leased += 1;
                }
            }
            if b_leased >= TENANT_B_JOBS && a_leased >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(
            b_leased, TENANT_B_JOBS,
            "tenant B's granted, runnable jobs were starved behind tenant A's \
             {} parked at-capacity RequestTickets: the broker scan restarts \
             from the front of the task group every pass and the tickets are \
             never consumed (handle_request_ticket leaves at-capacity tickets \
             in place), so tasks keyed behind them are never claimed",
            TENANT_A_JOBS
        );
        assert_eq!(
            a_leased, 1,
            "exactly one tenant A job should run (queue-a max_concurrency=1, \
             never completed)"
        );
    });
}
