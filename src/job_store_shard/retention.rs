use std::sync::Arc;
use std::time::Duration;

use slatedb::IsolationLevel;
use slatedb::config::WriteOptions;

use crate::job::{JobStatus, JobView};
use crate::job_store_shard::counters::encode_counter;
use crate::job_store_shard::helpers::{TxnWriter, WriteBatcher, retry_on_txn_conflict};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{
    attempt_prefix, end_bound, idx_metadata_key, idx_status_time_key, job_cancelled_key,
    job_info_key, job_status_key, status_index_timestamp, task_key, tenant_status_counter_key,
};
use crate::task::Task;
use crate::task_broker::BrokerTask;
use tracing::{debug, warn};

pub(crate) const TERMINAL_CLEANUP_TASK_PRIORITY: u8 = 0;
pub(crate) const TERMINAL_CLEANUP_TASK_ATTEMPT: u32 = 0;
const TERMINAL_CLEANUP_BATCH_SIZE: usize = 64;
const TERMINAL_CLEANUP_IDLE_SLEEP_MS: u64 = 25;

impl JobStoreShard {
    pub(crate) fn effective_terminal_retention_s(
        &self,
        requested: Option<i64>,
    ) -> Result<i64, JobStoreShardError> {
        let retention_s = requested.unwrap_or(self.default_terminal_retention_s);
        Self::validate_terminal_retention_s(retention_s)?;
        Ok(retention_s)
    }

    pub(crate) fn validate_terminal_retention_s(
        retention_s: i64,
    ) -> Result<(), JobStoreShardError> {
        if retention_s < 0 {
            return Err(JobStoreShardError::InvalidArgument(
                "terminal_retention_s must be >= 0".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn terminal_cleanup_due_ms(
        terminal_status_changed_at_ms: i64,
        retention_s: i64,
    ) -> Result<i64, JobStoreShardError> {
        let retention_ms = retention_s.checked_mul(1000).ok_or_else(|| {
            JobStoreShardError::InvalidArgument("terminal_retention_s is too large".to_string())
        })?;
        terminal_status_changed_at_ms
            .checked_add(retention_ms)
            .ok_or_else(|| {
                JobStoreShardError::InvalidArgument(
                    "terminal_retention_s produces an invalid due timestamp".to_string(),
                )
            })
    }

    pub(crate) fn terminal_cleanup_task_key(
        task_group: &str,
        job_id: &str,
        terminal_status_changed_at_ms: i64,
        retention_s: i64,
    ) -> Result<Vec<u8>, JobStoreShardError> {
        let due_ms = Self::terminal_cleanup_due_ms(terminal_status_changed_at_ms, retention_s)?;
        Ok(task_key(
            task_group,
            due_ms,
            TERMINAL_CLEANUP_TASK_PRIORITY,
            job_id,
            TERMINAL_CLEANUP_TASK_ATTEMPT,
        ))
    }

    pub(crate) fn schedule_terminal_cleanup_task<W: WriteBatcher>(
        &self,
        writer: &mut W,
        tenant: &str,
        job_view: &JobView,
        job_status: &JobStatus,
    ) -> Result<Option<Vec<u8>>, JobStoreShardError> {
        if !job_status.is_terminal() {
            return Ok(None);
        }

        let Some(retention_s) = job_view.terminal_retention_s() else {
            return Ok(None);
        };
        Self::validate_terminal_retention_s(retention_s)?;

        let task = Task::DeleteTerminalJob {
            tenant: tenant.to_string(),
            job_id: job_view.id().to_string(),
            terminal_status_changed_at_ms: job_status.changed_at_ms,
            task_group: job_view.task_group().to_string(),
        };
        let key = Self::terminal_cleanup_task_key(
            job_view.task_group(),
            job_view.id(),
            job_status.changed_at_ms,
            retention_s,
        )?;
        writer.put(&key, crate::codec::encode_task(&task))?;
        Ok(Some(key))
    }

    pub(crate) fn clear_terminal_cleanup_task<W: WriteBatcher>(
        &self,
        writer: &mut W,
        job_view: &JobView,
        job_status: &JobStatus,
    ) -> Result<Option<Vec<u8>>, JobStoreShardError> {
        let Some(retention_s) = job_view.terminal_retention_s() else {
            return Ok(None);
        };
        let key = Self::terminal_cleanup_task_key(
            job_view.task_group(),
            job_view.id(),
            job_status.changed_at_ms,
            retention_s,
        )?;
        writer.delete(&key)?;
        Ok(Some(key))
    }

    pub(crate) async fn delete_job_records_in_txn(
        &self,
        txn: &slatedb::DbTransaction,
        tenant: &str,
        job_id: &str,
        job_view: &JobView,
        status: &JobStatus,
        cleanup_task_key: Option<&[u8]>,
    ) -> Result<(), JobStoreShardError> {
        let mut writer = TxnWriter(txn);

        if let Some(task_key) = cleanup_task_key {
            writer.delete(task_key)?;
        }

        let status_index_key = idx_status_time_key(
            tenant,
            status.kind.as_str(),
            status_index_timestamp(status),
            job_id,
        );
        writer.delete(&status_index_key)?;
        writer.merge(
            tenant_status_counter_key(tenant, status.kind.as_str()),
            encode_counter(-1),
        )?;

        for (mk, mv) in job_view.metadata() {
            let metadata_key = idx_metadata_key(tenant, &mk, &mv, job_id);
            writer.delete(&metadata_key)?;
        }

        let attempts_start = attempt_prefix(tenant, job_id);
        let attempts_end = end_bound(&attempts_start);
        let mut iter = txn.scan::<Vec<u8>, _>(attempts_start..attempts_end).await?;
        while let Some(kv) = iter.next().await? {
            writer.delete(&kv.key)?;
        }

        writer.delete(job_info_key(tenant, job_id))?;
        writer.delete(job_status_key(tenant, job_id))?;
        writer.delete(job_cancelled_key(tenant, job_id))?;

        self.decrement_total_jobs_counter(&mut writer)?;
        if status.is_terminal() {
            self.decrement_completed_jobs_counter(&mut writer)?;
        }

        Ok(())
    }

    pub(crate) fn spawn_terminal_cleanup_task(self: &Arc<Self>) {
        let shard = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        let shard_name = self.name.clone();

        tokio::spawn(async move {
            loop {
                if cancellation.is_cancelled() {
                    debug!(
                        shard = %shard_name,
                        "stopping terminal cleanup processor (shard closing)"
                    );
                    break;
                }

                let claimed = shard
                    .broker
                    .claim_cleanup_ready_or_nudge(TERMINAL_CLEANUP_BATCH_SIZE)
                    .await;
                if claimed.is_empty() {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => {
                            debug!(
                                shard = %shard_name,
                                "stopping terminal cleanup processor (shard closing)"
                            );
                            break;
                        }
                        _ = tokio::time::sleep(Duration::from_millis(TERMINAL_CLEANUP_IDLE_SLEEP_MS)) => {}
                    }
                    continue;
                }

                for entry in claimed {
                    let key = entry.key.clone();
                    if let Err(error) = shard.process_delete_terminal_task(&entry).await {
                        warn!(
                            shard = %shard_name,
                            error = %error,
                            "terminal cleanup task processing failed, requeueing"
                        );
                        shard.broker.requeue_cleanup(vec![entry]);
                        continue;
                    }

                    shard
                        .broker
                        .ack_durable(std::slice::from_ref(&key), std::slice::from_ref(&key));
                    shard.broker.evict_keys(std::slice::from_ref(&key));
                }
            }
        });
    }

    async fn process_delete_terminal_task(
        &self,
        entry: &BrokerTask,
    ) -> Result<(), JobStoreShardError> {
        retry_on_txn_conflict("delete_terminal_task", || {
            self.process_delete_terminal_task_txn(entry)
        })
        .await
    }

    async fn process_delete_terminal_task_txn(
        &self,
        entry: &BrokerTask,
    ) -> Result<(), JobStoreShardError> {
        let delete_task = entry.decoded.as_delete_terminal_job().ok_or_else(|| {
            JobStoreShardError::Codec("expected DeleteTerminalJob task".to_string())
        })?;
        let tenant = delete_task.tenant().unwrap_or_default();
        let job_id = delete_task.job_id().unwrap_or_default();
        let terminal_status_changed_at_ms = delete_task.terminal_status_changed_at_ms();

        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        let status_key = job_status_key(tenant, job_id);
        let info_key = job_info_key(tenant, job_id);
        let maybe_status_raw = txn.get(&status_key).await?;
        let maybe_job_raw = txn.get(&info_key).await?;

        match (maybe_status_raw, maybe_job_raw) {
            (Some(status_raw), Some(job_raw)) => {
                let status = crate::job_store_shard::helpers::decode_job_status_owned(&status_raw)?;
                let job_view = JobView::new(job_raw)?;

                if status.is_terminal() && status.changed_at_ms == terminal_status_changed_at_ms {
                    self.delete_job_records_in_txn(
                        &txn,
                        tenant,
                        job_id,
                        &job_view,
                        &status,
                        Some(&entry.key),
                    )
                    .await?;
                } else {
                    txn.delete(&entry.key)?;
                }
            }
            _ => {
                txn.delete(&entry.key)?;
            }
        }

        txn.commit_with_options(&WriteOptions {
            await_durable: true,
        })
        .await?;
        Ok(())
    }
}
