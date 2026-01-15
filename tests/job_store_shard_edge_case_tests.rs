mod test_helpers;

use silo::job_attempt::{AttemptOutcome};

use test_helpers::*;

#[silo::test]
async fn tenant_allows_same_job_id_independent() {
    with_timeout!(2_000, {
        let (_tmp, shard) = open_temp_shard().await;

        let now = now_ms();

        // Enqueue the same explicit job id under two different tenants
        let shared_id = "shared-id".to_string();

        let id_a = shard
            .enqueue(
                "tenantA",
                Some(shared_id.clone()),
                10u8,
                now,
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"tenant": "A"})),
                vec![],
                None,
            )
            .await
            .expect("enqueue A");
        assert_eq!(id_a, shared_id);

        let id_b = shard
            .enqueue(
                "tenantB",
                Some(shared_id.clone()),
                10u8,
                now + 1, // avoid any potential key collisions in task queue
                None,
                test_helpers::msgpack_payload(&serde_json::json!({"tenant": "B"})),
                vec![],
                None,
            )
            .await
            .expect("enqueue B");
        assert_eq!(id_b, shared_id);

        // Both jobs must exist independently
        assert!(shard
            .get_job("tenantA", &shared_id)
            .await
            .unwrap()
            .is_some());
        assert!(shard
            .get_job("tenantB", &shared_id)
            .await
            .unwrap()
            .is_some());

        // Complete tenantA's job first so it can be deleted
        let tasks_a = shard.dequeue("wA", 1).await.expect("deq A").tasks;
        assert_eq!(tasks_a.len(), 1);
        shard
            .report_attempt_outcome(
                "tenantA",
                tasks_a[0].attempt().task_id(),
                AttemptOutcome::Success { result: vec![] },
            )
            .await
            .expect("complete A");

        // Deleting tenantA's job must not affect tenantB's job
        shard
            .delete_job("tenantA", &shared_id)
            .await
            .expect("delete A");
        assert!(shard
            .get_job("tenantA", &shared_id)
            .await
            .unwrap()
            .is_none());
        assert!(shard
            .get_job("tenantB", &shared_id)
            .await
            .unwrap()
            .is_some());
    });
}
