//! Bridge from the grant scanner to the shard's limit-chain walker.
//!
//! The grant scanner lives inside `ConcurrencyManager` and cannot directly
//! reference `JobStoreShard` (the shard owns the manager). This module defines
//! `ShardChainResumer`, an implementation of [`LimitChainResumer`] that holds a
//! `Weak<JobStoreShard>` and forwards `resume_chain` calls into
//! `JobStoreShard::enqueue_limit_task_at_index`, the single canonical chain
//! walker.
//!
//! Installed via `ConcurrencyManager::set_chain_resumer` once during shard
//! startup; the weak reference is upgraded per call, returning
//! `ConcurrencyError::ShardShuttingDown` if the shard has been dropped.

use std::sync::{Arc, Weak};

use async_trait::async_trait;
use slatedb::WriteBatch;

use crate::concurrency::{ConcurrencyError, LimitChainResumer, ResumeChainParams};
use crate::job_store_shard::helpers::DbWriteBatcher;
use crate::job_store_shard::{
    JobStoreShard, JobStoreShardError, LimitTaskParams, LimitTaskWriteResult,
};

pub(crate) struct ShardChainResumer {
    shard: Weak<JobStoreShard>,
}

impl ShardChainResumer {
    pub(crate) fn install(shard: &Arc<JobStoreShard>) -> Arc<dyn LimitChainResumer> {
        Arc::new(Self {
            shard: Arc::downgrade(shard),
        })
    }
}

#[async_trait]
impl LimitChainResumer for ShardChainResumer {
    async fn resume_chain(
        &self,
        batch: &mut WriteBatch,
        params: ResumeChainParams,
    ) -> Result<Vec<(String, String)>, ConcurrencyError> {
        let shard = self
            .shard
            .upgrade()
            .ok_or(ConcurrencyError::ShardShuttingDown)?;

        // The full limits list rides on the deferred request/ticket value, so
        // we never have to round-trip JobInfo here. `now_ms` is threaded
        // through from the scanner so the `task_key(now_ms, …)` we write
        // here can't disagree with the `granted_at_ms` the scanner already
        // baked into this batch.
        let now_ms = params.now_ms;

        // The resumed task_key keeps the job's ORIGINAL `start_at_ms` as its
        // broker-ordering start time — a job that has waited for its slot must
        // not be shoved to the back of the broker's start-time-ordered scan, or
        // the capped scan may never reach it (it holds concurrency slots that
        // then never release; see the granted-RunAttempt back-of-queue leak,
        // the production back-of-queue wedge incident).
        //
        // The tombstone dodge moves to the trailing `epoch_ms` instead. The
        // broker tombstones any task_key it ack-deleted, and an interim
        // RunAttempt/CheckRateLimit an earlier chain step wrote at
        // `task_key(start_at_ms, …, epoch=earlier)` may still be observed by an
        // in-flight LSM scan even though it's deleted; re-emitting at the same
        // full key would be suppressed and strand the holders this resumer just
        // granted. Stamping `now_ms` as the epoch makes the resumed key unique
        // versus that interim (whose epoch was set at an earlier enqueue/grant,
        // so strictly less than `now_ms`) without perturbing ordering. Mirrors
        // `handle_request_ticket` (dequeue.rs) — see
        // `project_broker_tombstone_chain_continuation`.
        let task_key_epoch_ms = now_ms;
        let mut writer = DbWriteBatcher::new(&shard.db, batch);
        let result = shard
            .enqueue_limit_task_at_index(
                &mut writer,
                LimitTaskParams {
                    tenant: &params.tenant,
                    task_id: &params.task_id,
                    job_id: &params.job_id,
                    attempt_number: params.attempt_number,
                    relative_attempt_number: params.relative_attempt_number,
                    limit_index: params.limit_index as usize,
                    limits: &params.limits,
                    priority: params.priority,
                    scheduled_at_ms: params.start_at_ms,
                    task_key_epoch_ms,
                    now_ms,
                    held_queues: params.held_queues.clone(),
                    task_group: &params.task_group,
                    skip_try_reserve: false,
                },
            )
            .await
            .map_err(into_concurrency_error)?;

        if let Err(e) = retarget_if_concrete_task(&shard, &mut writer, &params, &result).await {
            for (queue, task_id) in &result.grants {
                shard
                    .concurrency
                    .rollback_grant(&params.tenant, queue, task_id);
            }
            return Err(into_concurrency_error(e));
        }

        Ok(result.grants)
    }
}

async fn retarget_if_concrete_task(
    shard: &JobStoreShard,
    writer: &mut DbWriteBatcher<'_>,
    params: &ResumeChainParams,
    result: &LimitTaskWriteResult,
) -> Result<(), JobStoreShardError> {
    let Some(task_key_start_ms) = result.pending_task_key_start_ms else {
        return Ok(());
    };
    if task_key_start_ms == params.start_at_ms {
        return Ok(());
    }
    shard
        .retarget_scheduled_task_key(
            writer,
            &params.tenant,
            &params.job_id,
            params.attempt_number,
            task_key_start_ms,
        )
        .await
}

fn into_concurrency_error(e: JobStoreShardError) -> ConcurrencyError {
    match e {
        JobStoreShardError::Slate(arc) => ConcurrencyError::Slate(arc),
        JobStoreShardError::Codec(s) => ConcurrencyError::Encoding(s),
        other => ConcurrencyError::ChainResume(other.to_string()),
    }
}
