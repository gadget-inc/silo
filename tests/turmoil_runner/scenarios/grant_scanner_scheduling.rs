//! Grant scanner scheduling scenario: hot-lane nudges must be serviced
//! promptly even while the periodic reconcile sweep continuously re-seeds a
//! large cold-lane backlog of flood queues.
//!
//! Shape (the silo-staging starvation incident in miniature): 120 cap-1
//! "flood" queues each hold their single slot forever (their first job's
//! RunAttempt is routed to a task group no worker leases) with a second job
//! deferred behind it, so every requester counter stays non-zero and every
//! reconcile tick re-discovers all 120 queues. One "small" cap-1 queue has a
//! holder a worker actually completes; the release fires a hot-lane nudge.
//!
//! Invariants verified:
//! 1. Hot-lane latency: after the small queue's holder completes, the
//!    deferred waiter is granted and LEASED within a bounded sim window —
//!    it must not wait behind the flood queues' sweep-discovered entries.
//!    (Pre-fix, the release nudge shared one FIFO drain cycle with the
//!    entire sweep backlog and could wait a full cycle.)
//! 2. Liveness: the waiter's grant consumes its request row (observed via
//!    the lease itself).
//!
//! All client timing is fixed (no rng) so the trace is trivially
//! deterministic; turmoil's seeded scheduler provides the interleavings.

use crate::helpers::{
    ConcurrencyLimit, EnqueueRequest, HashMap, LeaseTasksRequest, Limit, ReportOutcomeRequest,
    SerializedBytes, TEST_SHARD_ID, connect_to_server, get_seed, limit, report_outcome_request,
    run_scenario_impl, serialized_bytes, setup_server,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const NUM_FLOOD_QUEUES: usize = 120;
/// Sim-time bound between completing the small queue's holder and leasing
/// the granted waiter. Generous vs the expected sub-second hot-lane latency,
/// tight vs any regression back to sweep-cycle latency (the flood alone
/// keeps the sweep re-seeding 120 entries every 5s tick).
const MAX_GRANT_TO_LEASE_SECS: u64 = 15;

fn msgpack_payload() -> Option<SerializedBytes> {
    Some(SerializedBytes {
        encoding: Some(serialized_bytes::Encoding::Msgpack(
            rmp_serde::to_vec(&serde_json::json!("payload")).unwrap(),
        )),
    })
}

fn enqueue_request(job_id: &str, queue: &str, task_group: &str) -> EnqueueRequest {
    EnqueueRequest {
        shard: TEST_SHARD_ID.to_string(),
        id: job_id.to_string(),
        priority: 50,
        start_at_ms: 0,
        retry_policy: None,
        payload: msgpack_payload(),
        limits: vec![Limit {
            limit: Some(limit::Limit::Concurrency(ConcurrencyLimit {
                key: queue.into(),
                max_concurrency: 1,
            })),
        }],
        tenant: None,
        metadata: HashMap::new(),
        task_group: task_group.to_string(),
    }
}

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("grant_scanner_scheduling", seed, 120, |sim| {
        // Set by the producer once the entire flood is enqueued; the worker
        // starts its latency measurement only after this (plus one reconcile
        // tick), so the sweep backlog is fully in place. Shared in-process
        // state is deterministic under turmoil's single-threaded sim.
        let flood_done = Arc::new(AtomicBool::new(false));

        sim.host("server", || async move { setup_server(9926).await });

        // Producer: the small queue's holder+waiter FIRST (so the worker can
        // lease the holder early — each simulated enqueue round-trip costs
        // ~100ms, so the 240-job flood takes ~25 sim-seconds), then the
        // flood: 120 cap-1 queues whose holders keep their slot forever
        // (their RunAttempts land in the unleased "flood" group) with one
        // deferred waiter each, keeping every requester counter non-zero.
        let producer_flood_done = Arc::clone(&flood_done);
        sim.client("producer", async move {
            let mut client = connect_to_server("http://server:9926").await?;

            for job_id in ["small-holder", "small-waiter"] {
                client
                    .enqueue(tonic::Request::new(enqueue_request(
                        job_id, "small-q", "small",
                    )))
                    .await?;
                tracing::trace!(job_id = %job_id, "small_enqueued");
            }

            for q in 0..NUM_FLOOD_QUEUES {
                let queue = format!("flood-{q:03}");
                for suffix in ["holder", "waiter"] {
                    let job_id = format!("{queue}-{suffix}");
                    client
                        .enqueue(tonic::Request::new(enqueue_request(
                            &job_id, &queue, "flood",
                        )))
                        .await?;
                    tracing::trace!(job_id = %job_id, "flood_enqueued");
                }
            }

            producer_flood_done.store(true, Ordering::SeqCst);
            tracing::trace!("producer_done");
            Ok(())
        });

        // Worker: leases only the "small" group. It waits for the flood to
        // be fully staged (plus one reconcile tick so the sweep has
        // re-seeded the cold lane with all 120 flood queues), THEN leases
        // the small holder, completes it, and measures how long the freed
        // slot takes to reach the waiter as a leasable RunAttempt. Leasing
        // only after staging keeps the holder's lease far from expiry (the
        // flood takes ~25 sim-seconds to enqueue). Measurement-critical
        // calls panic on error rather than `?`: a client Err merely ends
        // the sim as DST_RESULT:error, which does NOT fail the test outside
        // fuzz mode — a panic does.
        let worker_flood_done = Arc::clone(&flood_done);
        sim.client("worker", async move {
            while !worker_flood_done.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            tokio::time::sleep(Duration::from_secs(6)).await;

            let mut client = connect_to_server("http://server:9926").await?;

            let mut holder_task_id: Option<String> = None;
            for _ in 0..40 {
                let tasks = client
                    .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                        shard: Some(TEST_SHARD_ID.to_string()),
                        worker_id: "worker".to_string(),
                        max_tasks: 4,
                        task_group: "small".to_string(),
                    }))
                    .await
                    .expect("lease small-holder")
                    .into_inner()
                    .tasks;
                for task in tasks {
                    tracing::trace!(job_id = %task.job_id, "leased");
                    if task.job_id == "small-holder" {
                        holder_task_id = Some(task.id.clone());
                    }
                }
                if holder_task_id.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            let holder_task_id = holder_task_id.expect("small-holder never leased");
            tracing::trace!("small_holder_leased");

            // Complete the holder: the release fires a hot-lane nudge for
            // small-q while the flood keeps the cold lane re-seeded.
            client
                .report_outcome(tonic::Request::new(ReportOutcomeRequest {
                    shard: TEST_SHARD_ID.to_string(),
                    task_id: holder_task_id,
                    outcome: Some(report_outcome_request::Outcome::Success(SerializedBytes {
                        encoding: Some(serialized_bytes::Encoding::Msgpack(
                            rmp_serde::to_vec(&serde_json::json!("done")).unwrap(),
                        )),
                    })),
                    tenant_id: None,
                }))
                .await
                .expect("report small-holder outcome");
            let released_at = tokio::time::Instant::now();
            tracing::trace!("small_holder_completed");

            // The waiter must be granted and leasable within the bound.
            let mut waiter_leased = false;
            while released_at.elapsed() < Duration::from_secs(MAX_GRANT_TO_LEASE_SECS) {
                let tasks = client
                    .lease_tasks(tonic::Request::new(LeaseTasksRequest {
                        shard: Some(TEST_SHARD_ID.to_string()),
                        worker_id: "worker".to_string(),
                        max_tasks: 4,
                        task_group: "small".to_string(),
                    }))
                    .await
                    .expect("lease small-waiter")
                    .into_inner()
                    .tasks;
                if tasks.iter().any(|t| t.job_id == "small-waiter") {
                    waiter_leased = true;
                    // Sim time is deterministic, so this duration is too —
                    // safe to trace as part of the determinism oracle.
                    tracing::trace!(
                        elapsed_ms = released_at.elapsed().as_millis() as u64,
                        "small_waiter_leased"
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }

            assert!(
                waiter_leased,
                "INVARIANT VIOLATION: small-q's freed slot was not granted+leased \
                 within {MAX_GRANT_TO_LEASE_SECS}s of release — the hot-lane nudge \
                 starved behind the flood queues' sweep backlog"
            );

            tracing::trace!("worker_done");
            Ok(())
        });
    });
}
