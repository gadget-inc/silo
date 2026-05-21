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
use crate::job_store_shard::{JobStoreShard, JobStoreShardError, LimitTaskParams};

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
        // we never have to round-trip JobInfo here.
        let now_ms = crate::job_store_shard::now_epoch_ms();

        // Land the resumed chain's task_key at `now_ms`, not the original
        // enqueue time. The broker tombstones any task_key it has ack-deleted,
        // and an LSM scan can momentarily observe the interim RunAttempt that
        // earlier chain steps wrote at `task_key(params.start_at_ms, ...)` —
        // even though that interim is deleted in the same batch. Once a worker
        // claims and ack-deletes that key, every later write at the same
        // task_key is silently suppressed by the tombstone, which strands the
        // holders this resumer just granted. Mirrors the precedent in
        // `handle_request_ticket` (dequeue.rs) — see
        // `project_broker_tombstone_chain_continuation`.
        let mut writer = DbWriteBatcher::new(&shard.db, batch);
        shard
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
                    start_at_ms: now_ms,
                    now_ms,
                    held_queues: params.held_queues.clone(),
                    task_group: &params.task_group,
                    skip_try_reserve: false,
                },
            )
            .await
            .map_err(into_concurrency_error)
    }
}

fn into_concurrency_error(e: JobStoreShardError) -> ConcurrencyError {
    match e {
        JobStoreShardError::Slate(arc) => ConcurrencyError::Slate(arc),
        JobStoreShardError::Codec(s) => ConcurrencyError::Encoding(s),
        other => ConcurrencyError::ChainResume(other.to_string()),
    }
}
