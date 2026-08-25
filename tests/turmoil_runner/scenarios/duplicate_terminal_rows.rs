//! Duplicate terminal task rows scenario: the server-side invariant check must
//! report a shard holding two live terminal task rows for one
//! `(job_id, attempt)`.
//!
//! `verify_server_invariants` swallows per-query errors, so a scenario seeded
//! with a hand-planted duplicate is what proves the singleLiveTerminalRow
//! query actually runs: an unreported duplicate here means the query silently
//! broke, not that the shard is healthy.

use crate::helpers::{
    TEST_SHARD_ID, connect_to_server, get_seed, run_scenario_impl, setup_server_with_seeder,
    verify_server_invariants,
};

pub fn run() {
    let seed = get_seed();
    run_scenario_impl("duplicate_terminal_rows", seed, 30, |sim| {
        sim.host("server", || async move {
            setup_server_with_seeder(9930, |shard| async move {
                // Two live RunAttempt rows for one (job_id, attempt): the same
                // task at two write-time epochs, exactly what a double
                // materialization in the grant path produces. Start time is in
                // the past relative to simulated time zero, so both rows are
                // live; no worker polls in this scenario, so they stay live.
                let task = silo::task::Task::RunAttempt {
                    id: "seeded-duplicate-task".to_string(),
                    tenant: "-".to_string(),
                    job_id: "seeded-duplicate-job".to_string(),
                    attempt_number: 1,
                    relative_attempt_number: 1,
                    held_queues: vec![],
                    task_group: "default".to_string(),
                };
                let mut batch = slatedb::WriteBatch::new();
                for epoch in [1_000, 1_001] {
                    batch.put(
                        &silo::keys::task_key(
                            "default",
                            1_000,
                            10,
                            "seeded-duplicate-job",
                            1,
                            epoch,
                        ),
                        &silo::codec::encode_task(&task),
                    );
                }
                shard.db().write(batch).await.map_err(|e| e.to_string())?;
                shard.db().flush().await.map_err(|e| e.to_string())?;
                Ok(())
            })
            .await
        });

        sim.client("client", async move {
            let mut client = connect_to_server("http://server:9930").await?;
            tracing::trace!("client_start");

            let state = verify_server_invariants(&mut client, TEST_SHARD_ID)
                .await
                .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
            tracing::trace!(violations = ?state.violations, "server_state");

            let reported = state.violations.iter().any(|v| {
                v.contains("singleLiveTerminalRow")
                    && v.contains("seeded-duplicate-job")
                    && v.contains("has 2 live terminal task rows")
            });
            assert!(
                reported,
                "the seeded duplicate terminal rows must be reported as a \
                 singleLiveTerminalRow violation counting exactly 2 rows; got \
                 violations: {:?}",
                state.violations
            );

            tracing::trace!("client_done");
            Ok(())
        });
    });
}
