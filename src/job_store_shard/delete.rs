//! Job deletion operations.

use slatedb::IsolationLevel;

use crate::job::{JobStatusKind, JobView};
use crate::job_store_shard::counters::encode_counter;
use crate::job_store_shard::helpers::{
    DbWriteBatcher, TxnWriter, WriteBatcher, decode_job_status_owned, retry_on_txn_conflict,
};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{
    idx_metadata_key, idx_status_time_key, job_cancelled_key, job_info_key, job_status_key,
    status_index_timestamp, tenant_status_counter_key,
};

/// Outcome of one transactional scheduled-delete attempt.
enum ScheduledDeleteRoute {
    /// The job was Scheduled and has been deleted.
    Deleted,
    /// The job no longer exists (idempotent success).
    Missing,
    /// The job's in-transaction status was not Scheduled; the caller
    /// re-routes on a fresh status read.
    NotScheduled,
}

/// Outcome of the non-transactional settled-status delete path.
enum SettledDeleteOutcome {
    /// The job was terminal or missing and has been deleted (or was already gone).
    Done,
    /// The status read showed Scheduled; the caller re-routes to the
    /// transactional scheduled branch.
    WasScheduled,
}

impl JobStoreShard {
    /// Delete a job by id.
    ///
    /// Scheduled and terminal (Succeeded/Failed/Cancelled) jobs are deletable;
    /// deleting a missing job is an idempotent success. Running jobs are
    /// refused — cancel them or let them finish first.
    ///
    /// A Scheduled job's footprint goes beyond its job rows (pending task,
    /// possibly concurrency holders/requests, broker-buffer entry), so the
    /// scheduled branch runs inside a serializable-snapshot transaction that
    /// re-reads the status as its first operation and acts on that value — a
    /// concurrent dequeue can otherwise lease the task between the status
    /// check and the deletes, leaving a live lease pointing at deleted rows.
    #[tracing::instrument(skip_all, fields(shard = %self.name))]
    pub async fn delete_job(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        // The pre-transaction status read only routes; each branch authorizes
        // off its own re-read. The loop handles a status that flips between
        // the routing read and the branch's authorizing read (e.g. a
        // Scheduled job dequeued to Running, or a Cancelled job restarted
        // back to Scheduled).
        const MAX_ROUTE_ATTEMPTS: usize = 3;
        for _ in 0..MAX_ROUTE_ATTEMPTS {
            let status = self.get_job_status(tenant, id).await?;
            if status
                .as_ref()
                .is_some_and(|s| s.kind == JobStatusKind::Scheduled)
            {
                let route = retry_on_txn_conflict("delete_scheduled_job", || {
                    self.delete_scheduled_job_inner(tenant, id)
                })
                .await?;
                match route {
                    ScheduledDeleteRoute::Deleted | ScheduledDeleteRoute::Missing => {
                        return Ok(());
                    }
                    ScheduledDeleteRoute::NotScheduled => continue,
                }
            }
            match self.delete_settled_job(tenant, id).await? {
                SettledDeleteOutcome::Done => return Ok(()),
                SettledDeleteOutcome::WasScheduled => continue,
            }
        }
        Err(JobStoreShardError::TransactionConflict(
            "delete_job".to_string(),
        ))
    }

    /// Delete a Scheduled job inside a serializable-snapshot transaction,
    /// performing the same cleanup a cancel-then-delete pair produces:
    /// pending task removal, concurrency holder/request release, requester
    /// counter decrement, index cleanup, and counter transitions — without an
    /// intermediate observable Cancelled state.
    async fn delete_scheduled_job_inner(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<ScheduledDeleteRoute, JobStoreShardError> {
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        // Authorizing read: act only on the in-transaction status. A
        // transition that committed before this transaction began produces no
        // read-set conflict, so the outer routing read must never authorize.
        let status_key = job_status_key(tenant, id);
        let Some(status_raw) = txn.get(&status_key).await? else {
            return Ok(ScheduledDeleteRoute::Missing);
        };
        let status = decode_job_status_owned(&status_raw)?;
        if status.kind != JobStatusKind::Scheduled {
            return Ok(ScheduledDeleteRoute::NotScheduled);
        }

        // Job info supplies task-key reconstruction fields, metadata indexes,
        // and gauge metadata.
        let job_info_key_bytes = job_info_key(tenant, id);
        let job_view = match txn.get(&job_info_key_bytes).await? {
            Some(raw) => Some(JobView::new(raw)?),
            None => None,
        };

        // Pending task, concurrency holders, and request records — shared
        // with the cancel path so the two cleanups cannot silently diverge.
        // Returns the deleted task key for post-commit broker-buffer eviction
        // and the holder pairs to release post-commit.
        let (deleted_task_key, holders_to_release) = match job_view {
            Some(ref view) => {
                self.remove_scheduled_job_execution_state(&txn, tenant, id, &status, view)
                    .await?
            }
            None => (None, Vec::new()),
        };

        // Index cleanup. Scheduled rows key the status-time index on the
        // next-attempt timestamp, not changed_at_ms.
        txn.delete(idx_status_time_key(
            tenant,
            status.kind.as_str(),
            status_index_timestamp(&status),
            id,
        ))?;
        if let Some(ref view) = job_view {
            for (mk, mv) in view.metadata().into_iter() {
                txn.delete(idx_metadata_key(tenant, &mk, &mv, id))?;
            }
        }
        txn.delete(&job_info_key_bytes)?;
        txn.delete(&status_key)?;
        txn.delete(job_cancelled_key(tenant, id))?;

        // Attempt rows from prior attempts (retry case) carry no TTL until
        // the job reaches a terminal status, which a deleted Scheduled job
        // never does — sweep them here or they survive forever and resurface
        // under a re-enqueued job with the same id.
        let attempt_start = crate::keys::attempt_prefix(tenant, id);
        let attempt_end = crate::keys::end_bound(&attempt_start);
        let mut attempt_keys: Vec<Vec<u8>> = Vec::new();
        let mut iter = txn.scan::<Vec<u8>, _>(attempt_start..attempt_end).await?;
        while let Some(kv) = iter.next().await? {
            attempt_keys.push(kv.key.to_vec());
        }
        drop(iter);
        for key in &attempt_keys {
            txn.delete(key)?;
        }

        // Counter bookkeeping: total-jobs and the (Scheduled) tenant status
        // counter. No completed-jobs decrement — a Scheduled job was never
        // counted completed. Merges via TxnWriter are excluded from conflict
        // detection.
        let mut writer = TxnWriter(&txn);
        self.decrement_total_jobs_counter(&mut writer)?;
        writer.merge(
            tenant_status_counter_key(tenant, status.kind.as_str()),
            encode_counter(-1),
        )?;

        // In-memory queue gauge: decrement exactly once per deleted job,
        // rolled back if this commit attempt fails so a conflict retry
        // doesn't drift the gauge.
        let gauge_info = job_view
            .as_ref()
            .map(|view| (view.task_group().to_string(), view.metadata()));
        if let Some((task_group, metadata)) = gauge_info.as_ref() {
            self.apply_background_action_queue_counter_delta(
                tenant,
                task_group,
                JobStatusKind::Scheduled,
                metadata,
                -1,
            );
        }

        if let Err(e) = txn.commit().await {
            if let Some((task_group, metadata)) = gauge_info.as_ref() {
                self.apply_background_action_queue_counter_delta(
                    tenant,
                    task_group,
                    JobStatusKind::Scheduled,
                    metadata,
                    1,
                );
            }
            return Err(e.into());
        }

        // ---- Post-commit: update in-memory state ----

        // Evict the deleted task from the broker buffer
        if let Some(ref key) = deleted_task_key {
            self.brokers.evict_keys(std::slice::from_ref(key));
        }

        // Release concurrency holders from in-memory counts and signal the
        // grant scanner so freed slots become grantable.
        for (task_id, queue) in &holders_to_release {
            self.concurrency
                .counts()
                .atomic_release(tenant, queue, task_id);
            self.concurrency.request_grant(tenant, queue);
        }

        Ok(ScheduledDeleteRoute::Deleted)
    }

    /// Delete a terminal-status or missing job. Terminal jobs have no
    /// dequeuable task, so a plain write batch suffices.
    async fn delete_settled_job(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<SettledDeleteOutcome, JobStoreShardError> {
        use slatedb::WriteBatch;

        let status = self.get_job_status(tenant, id).await?;
        if let Some(ref status) = status {
            match status.kind {
                JobStatusKind::Scheduled => return Ok(SettledDeleteOutcome::WasScheduled),
                JobStatusKind::Running => {
                    return Err(JobStoreShardError::JobInProgress(
                        id.to_string(),
                        status.kind,
                    ));
                }
                _ => {}
            }
        }

        // Check if job even exists (status.is_none() means job doesn't exist)
        // If job doesn't exist, just return Ok (idempotent delete)
        let job_info_key_bytes = job_info_key(tenant, id);
        if status.is_none() && self.db.get(&job_info_key_bytes).await?.is_none() {
            return Ok(SettledDeleteOutcome::Done);
        }

        let job_status_key_bytes = job_status_key(tenant, id);
        let mut batch = WriteBatch::new();
        // Clean up secondary index entries if present
        if let Some(ref status) = status {
            let timek = idx_status_time_key(tenant, status.kind.as_str(), status.changed_at_ms, id);
            batch.delete(&timek);
        }
        // Clean up metadata index entries (load job info to enumerate metadata)
        let job_metric_info = if let Some(raw) = self.db.get(&job_info_key_bytes).await? {
            let view = JobView::new(raw)?;
            for (mk, mv) in view.metadata().into_iter() {
                let mkey = idx_metadata_key(tenant, &mk, &mv, id);
                batch.delete(&mkey);
            }
            Some((view.task_group().to_string(), view.metadata()))
        } else {
            None
        };
        batch.delete(&job_info_key_bytes);
        batch.delete(&job_status_key_bytes);
        // Also delete cancellation record if present
        batch.delete(job_cancelled_key(tenant, id));

        // Update counters in the same batch
        let mut writer = DbWriteBatcher::new(&self.db, &mut batch);
        self.decrement_total_jobs_counter(&mut writer)?;
        if let Some(ref status) = status {
            // Job was in terminal state (we already checked above)
            self.decrement_completed_jobs_counter(&mut writer)?;
            // Decrement tenant status counter
            let counter_key = tenant_status_counter_key(tenant, status.kind.as_str());
            writer.merge(&counter_key, encode_counter(-1))?;
        }

        if let (Some(status), Some((task_group, metadata))) =
            (status.as_ref(), job_metric_info.as_ref())
        {
            self.apply_background_action_queue_counter_transition(
                tenant,
                task_group,
                Some(status.kind),
                JobStatusKind::Succeeded,
                metadata,
            );
        }

        if let Err(e) = self.db.write(batch).await {
            if let (Some(status), Some((task_group, metadata))) =
                (status.as_ref(), job_metric_info.as_ref())
            {
                self.rollback_background_action_queue_counter_transition(
                    tenant,
                    task_group,
                    Some(status.kind),
                    JobStatusKind::Succeeded,
                    metadata,
                );
            }
            return Err(e.into());
        }

        Ok(SettledDeleteOutcome::Done)
    }
}
