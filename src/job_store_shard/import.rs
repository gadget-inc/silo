//! Job import operations.
//!
//! Allows importing jobs from other systems with historical attempt records.
//! Unlike enqueue, import accepts completed attempts and lets Silo take ownership going forward.

use slatedb::config::WriteOptions;
use slatedb::{IsolationLevel, WriteBatch};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::codec::{decode_attempt, decode_task, encode_attempt, encode_job_info};
use crate::dst_events::{self, DstEvent};
use crate::fb::silo::fb;
use crate::job::{JobInfo, JobStatus, JobStatusKind, JobView, Limit};
use crate::job_attempt::{AttemptStatus, JobAttempt};
use crate::job_store_shard::helpers::{
    TxnWriter, decode_job_status_owned, now_epoch_ms, retry_on_txn_conflict,
};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError, LimitTaskParams};
use crate::keys::{
    attempt_key, attempt_prefix, concurrency_holder_key, end_bound, idx_metadata_key,
    job_cancelled_key, job_info_key, job_status_key, task_key,
};
use crate::retry::{RetryPolicy, retries_exhausted};
use crate::task::Task;

/// Error returned when a job cannot be reimported.
#[derive(Debug, Clone)]
pub struct JobNotReimportableError {
    pub job_id: String,
    pub status: JobStatusKind,
    pub reason: String,
}

impl std::fmt::Display for JobNotReimportableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot reimport job {}: {} (status: {:?})",
            self.job_id, self.reason, self.status
        )
    }
}

impl std::error::Error for JobNotReimportableError {}

/// Imported attempt from another system. All attempts must be terminal.
#[derive(Debug, Clone)]
pub struct ImportedAttempt {
    pub status: ImportedAttemptStatus,
    pub started_at_ms: i64,
    pub finished_at_ms: i64,
}

/// Status of an imported attempt.
#[derive(Debug, Clone)]
pub enum ImportedAttemptStatus {
    Succeeded { result: Vec<u8> },
    Failed { error_code: String, error: Vec<u8> },
    Cancelled,
}

/// Result of importing a single job.
#[derive(Debug)]
pub struct ImportJobResult {
    pub job_id: String,
    pub success: bool,
    pub error: Option<String>,
    pub status: JobStatusKind,
}

/// Parameters for importing a single job.
#[derive(Debug, Clone)]
pub struct ImportJobParams {
    pub id: String,
    pub priority: u8,
    pub enqueue_time_ms: i64,
    pub start_at_ms: i64,
    pub retry_policy: Option<RetryPolicy>,
    pub payload: Vec<u8>,
    pub limits: Vec<Limit>,
    pub metadata: Option<Vec<(String, String)>>,
    pub task_group: String,
    pub attempts: Vec<ImportedAttempt>,
}

impl JobStoreShard {
    /// Import a batch of jobs, returning per-job results.
    /// Each job is imported independently; failures don't affect other jobs in the batch.
    pub async fn import_jobs(
        &self,
        tenant: &str,
        jobs: Vec<ImportJobParams>,
    ) -> Result<Vec<ImportJobResult>, JobStoreShardError> {
        let mut results = Vec::with_capacity(jobs.len());
        for params in jobs {
            let job_id = params.id.clone();
            match self.import_job(tenant, params).await {
                Ok(status) => results.push(ImportJobResult {
                    job_id,
                    success: true,
                    error: None,
                    status,
                }),
                Err(e) => results.push(ImportJobResult {
                    job_id,
                    success: false,
                    error: Some(e.to_string()),
                    status: JobStatusKind::Failed,
                }),
            }
        }
        Ok(results)
    }

    /// Import a single job with historical attempts.
    /// Uses a transaction for dedup (caller-provided IDs).
    async fn import_job(
        &self,
        tenant: &str,
        params: ImportJobParams,
    ) -> Result<JobStatusKind, JobStoreShardError> {
        // Validate ID
        if params.id.is_empty() {
            return Err(JobStoreShardError::InvalidArgument(
                "import job id is required".into(),
            ));
        }

        retry_on_txn_conflict("import_job", || self.import_job_txn(tenant, &params)).await
    }

    /// Inner transaction-based import for a single job.
    async fn import_job_txn(
        &self,
        tenant: &str,
        params: &ImportJobParams,
    ) -> Result<JobStatusKind, JobStoreShardError> {
        let now_ms = now_epoch_ms();
        let job_id = &params.id;
        let effective_enqueue_time_ms = if params.enqueue_time_ms <= 0 {
            now_ms
        } else {
            params.enqueue_time_ms
        };
        let effective_start_at_ms = if params.start_at_ms <= 0 {
            now_ms
        } else {
            params.start_at_ms
        };

        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        // [SILO-IMP-1] Check for duplicate - if exists, reimport instead
        let info_key = job_info_key(tenant, job_id);
        if let Some(existing_job_raw) = txn.get(&info_key).await? {
            let existing_job = JobView::new(existing_job_raw)?;
            return self
                .reimport_job_txn(tenant, params, txn, existing_job, now_ms)
                .await;
        }

        // Validate attempts for new import (reimport has its own validation)
        if let Err(msg) = validate_import_attempts(&params.attempts) {
            return Err(JobStoreShardError::InvalidArgument(msg));
        }

        // Write JobInfo
        let job = JobInfo {
            id: job_id.to_string(),
            priority: params.priority,
            enqueue_time_ms: effective_enqueue_time_ms,
            payload: params.payload.clone(),
            retry_policy: params.retry_policy.clone(),
            metadata: params.metadata.clone().unwrap_or_default(),
            limits: params.limits.clone(),
            task_group: params.task_group.clone(),
        };
        let job_value = encode_job_info(&job);
        txn.put(&info_key, &job_value)?;

        // Write metadata secondary index
        for (mk, mv) in &job.metadata {
            let mkey = idx_metadata_key(tenant, mk, mv, job_id);
            txn.put(&mkey, [])?;
        }

        // [SILO-IMP-2] Write imported attempt records (existing statuses unchanged - vacuously true for new import)
        // [SILO-IMP-3] All new attempts are terminal (validated by validate_import_attempts)
        let num_attempts = params.attempts.len() as u32;
        for (i, imported) in params.attempts.iter().enumerate() {
            let attempt_number = (i as u32) + 1;
            let task_id = Uuid::new_v4().to_string();

            let status = match &imported.status {
                ImportedAttemptStatus::Succeeded { result } => AttemptStatus::Succeeded {
                    finished_at_ms: imported.finished_at_ms,
                    result: result.clone(),
                },
                ImportedAttemptStatus::Failed { error_code, error } => AttemptStatus::Failed {
                    finished_at_ms: imported.finished_at_ms,
                    error_code: error_code.clone(),
                    error: error.clone(),
                },
                ImportedAttemptStatus::Cancelled => AttemptStatus::Cancelled {
                    finished_at_ms: imported.finished_at_ms,
                },
            };

            let attempt_record = JobAttempt {
                job_id: job_id.to_string(),
                attempt_number,
                relative_attempt_number: attempt_number,
                task_id,
                started_at_ms: imported.started_at_ms,
                status,
            };
            let attempt_value = encode_attempt(&attempt_record);
            let akey = attempt_key(tenant, job_id, attempt_number);
            txn.put(&akey, &attempt_value)?;
        }

        let (job_status, status_kind, is_terminal) =
            determine_import_status(params, num_attempts, now_ms, effective_start_at_ms);

        // Write status + index
        let mut writer = TxnWriter(&txn);
        Self::write_job_status_with_index(&mut writer, tenant, job_id, job_status)?;

        // [SILO-IMP-5] Terminal status set by determine_import_status (Succeeded/Failed/Cancelled)
        // [SILO-IMP-6] Scheduled status set by determine_import_status when retries remain
        // For non-terminal imports, create a task for the next attempt
        // [SILO-IMP-7] Task created in DB queue for non-terminal import
        // [SILO-IMP-CONC-1] Queue has capacity -> try_reserve succeeds in enqueue_limit_task_at_index
        // [SILO-IMP-CONC-2] Holder + task in DB queue when concurrency granted
        // [SILO-IMP-CONC-3] Queue at capacity -> try_reserve fails in enqueue_limit_task_at_index
        // [SILO-IMP-CONC-4] Request created (no task in DB) when concurrency queued
        let mut grants = Vec::new();
        if !is_terminal {
            let next_attempt = num_attempts + 1;
            let task_id = Uuid::new_v4().to_string();

            grants = self
                .enqueue_limit_task_at_index(
                    &mut writer,
                    LimitTaskParams {
                        tenant,
                        task_id: &task_id,
                        job_id,
                        attempt_number: next_attempt,
                        relative_attempt_number: next_attempt,
                        limit_index: 0,
                        limits: &params.limits,
                        priority: params.priority,
                        start_at_ms: effective_start_at_ms,
                        now_ms,
                        held_queues: Vec::new(),
                        task_group: &params.task_group,
                        skip_try_reserve: false,
                    },
                )
                .await?;
        }

        // Include counters in the transaction (unmark_write excludes them from conflict detection)
        self.increment_total_jobs_counter(&mut writer)?;
        if is_terminal {
            self.increment_completed_jobs_counter(&mut writer)?;
        }

        let write_op = if !is_terminal {
            let op = dst_events::next_write_op();
            dst_events::emit_pending(
                DstEvent::JobEnqueued {
                    tenant: tenant.to_string(),
                    job_id: job_id.to_string(),
                },
                op,
            );
            Some(op)
        } else {
            None
        };

        if let Err(e) = txn
            .commit_with_options(&WriteOptions {
                await_durable: true,
            })
            .await
        {
            if let Some(op) = write_op {
                dst_events::cancel_write(op);
            }
            self.rollback_grants(tenant, &grants);
            return Err(e.into());
        }
        if let Some(op) = write_op {
            dst_events::confirm_write(op);
        }

        // For non-terminal, finish enqueue (flush + broker wakeup)
        if !is_terminal {
            self.finish_enqueue(job_id, effective_start_at_ms, &grants)
                .await?;
        }

        debug!(job_id = %job_id, status = ?status_kind, "imported job");

        Ok(status_kind)
    }

    /// Reimport an existing job with updated attempt data.
    ///
    /// This is called when `import_job_txn` discovers the job already exists.
    /// It validates preconditions, writes new attempts, cleans up old scheduling state,
    /// and creates new scheduling state as needed.
    async fn reimport_job_txn(
        &self,
        tenant: &str,
        params: &ImportJobParams,
        txn: slatedb::DbTransaction,
        existing_job: JobView,
        now_ms: i64,
    ) -> Result<JobStatusKind, JobStoreShardError> {
        let job_id = &params.id;
        let effective_start_at_ms = if params.start_at_ms <= 0 {
            now_ms
        } else {
            params.start_at_ms
        };

        // === Validate preconditions ===

        // [SILO-REIMP-1] Pre: job exists (confirmed by caller finding job_info)

        // Read current status
        let status_key = job_status_key(tenant, job_id);
        let status_raw = txn
            .get(&status_key)
            .await?
            .ok_or_else(|| JobStoreShardError::JobNotFound(job_id.to_string()))?;
        let old_status = decode_job_status_owned(&status_raw)?;

        // [SILO-REIMP-2] Pre: not Running (no active lease)
        if old_status.kind == JobStatusKind::Running {
            return Err(JobStoreShardError::JobNotReimportable(
                JobNotReimportableError {
                    job_id: job_id.to_string(),
                    status: old_status.kind,
                    reason: "job is currently running".to_string(),
                },
            ));
        }

        // [SILO-REIMP-4] Pre: not Succeeded (truly terminal)
        if old_status.kind == JobStatusKind::Succeeded {
            return Err(JobStoreShardError::JobNotReimportable(
                JobNotReimportableError {
                    job_id: job_id.to_string(),
                    status: old_status.kind,
                    reason: "job already succeeded".to_string(),
                },
            ));
        }

        // [SILO-REIMP-3] Pre: scan existing attempts, verify all terminal
        let attempt_start = attempt_prefix(tenant, job_id);
        let attempt_end = end_bound(&attempt_start);
        let mut iter = txn.scan::<Vec<u8>, _>(attempt_start..attempt_end).await?;
        let mut existing_attempts = Vec::new();
        while let Some(kv) = iter.next().await? {
            let decoded = decode_attempt(kv.value.clone())?;
            existing_attempts.push(decoded);
        }

        for (i, existing) in existing_attempts.iter().enumerate() {
            let a = existing.fb();
            if a.status_kind() == fb::AttemptStatusKind::Running {
                return Err(JobStoreShardError::JobNotReimportable(
                    JobNotReimportableError {
                        job_id: job_id.to_string(),
                        status: old_status.kind,
                        reason: format!("existing attempt {} is still running", i + 1),
                    },
                ));
            }
        }

        let existing_count = existing_attempts.len();

        // === Validate attempt consistency ===

        // Must have at least as many attempts as existing
        if params.attempts.len() < existing_count {
            return Err(JobStoreShardError::JobNotReimportable(
                JobNotReimportableError {
                    job_id: job_id.to_string(),
                    status: old_status.kind,
                    reason: format!(
                        "reimport has {} attempts but job has {} existing attempts",
                        params.attempts.len(),
                        existing_count
                    ),
                },
            ));
        }

        // Verify existing attempts match the provided ones
        for (i, existing) in existing_attempts.iter().enumerate() {
            let a = existing.fb();
            let imported = &params.attempts[i];

            // Validate status type + timestamps match
            let mismatch = match (a.status_kind(), &imported.status) {
                (fb::AttemptStatusKind::Succeeded, ImportedAttemptStatus::Succeeded { result }) => {
                    a.finished_at_ms().unwrap_or(0) != imported.finished_at_ms
                        || a.result().map(|v| v.bytes()).unwrap_or_default() != result.as_slice()
                        || a.started_at_ms() != imported.started_at_ms
                }
                (
                    fb::AttemptStatusKind::Failed,
                    ImportedAttemptStatus::Failed { error_code, .. },
                ) => {
                    a.finished_at_ms().unwrap_or(0) != imported.finished_at_ms
                        || a.error_code().unwrap_or_default() != error_code.as_str()
                        || a.started_at_ms() != imported.started_at_ms
                }
                (fb::AttemptStatusKind::Cancelled, ImportedAttemptStatus::Cancelled) => {
                    a.finished_at_ms().unwrap_or(0) != imported.finished_at_ms
                        || a.started_at_ms() != imported.started_at_ms
                }
                _ => true, // Status type mismatch
            };

            if mismatch {
                return Err(JobStoreShardError::JobNotReimportable(
                    JobNotReimportableError {
                        job_id: job_id.to_string(),
                        status: old_status.kind,
                        reason: format!("existing attempt {} does not match reimport data", i + 1),
                    },
                ));
            }
        }

        // Must have at least one new attempt
        if params.attempts.len() <= existing_count {
            return Err(JobStoreShardError::JobNotReimportable(
                JobNotReimportableError {
                    job_id: job_id.to_string(),
                    status: old_status.kind,
                    reason: "reimport must include at least one new attempt".to_string(),
                },
            ));
        }

        // Validate new attempts (the suffix after existing ones)
        if let Err(msg) = validate_import_attempts(&params.attempts[existing_count..]) {
            return Err(JobStoreShardError::InvalidArgument(msg));
        }

        // === Write only new attempt records ===
        // [SILO-IMP-2] existing attempt statuses unchanged (we don't touch them)
        // [SILO-IMP-3] new attempts are terminal (validated above)
        let total_attempts = params.attempts.len() as u32;
        for i in existing_count..params.attempts.len() {
            let imported = &params.attempts[i];
            let attempt_number = (i as u32) + 1;
            let task_id = Uuid::new_v4().to_string();

            let status = match &imported.status {
                ImportedAttemptStatus::Succeeded { result } => AttemptStatus::Succeeded {
                    finished_at_ms: imported.finished_at_ms,
                    result: result.clone(),
                },
                ImportedAttemptStatus::Failed { error_code, error } => AttemptStatus::Failed {
                    finished_at_ms: imported.finished_at_ms,
                    error_code: error_code.clone(),
                    error: error.clone(),
                },
                ImportedAttemptStatus::Cancelled => AttemptStatus::Cancelled {
                    finished_at_ms: imported.finished_at_ms,
                },
            };

            let attempt_record = JobAttempt {
                job_id: job_id.to_string(),
                attempt_number,
                relative_attempt_number: attempt_number,
                task_id,
                started_at_ms: imported.started_at_ms,
                status,
            };
            let attempt_value = encode_attempt(&attempt_record);
            let akey = attempt_key(tenant, job_id, attempt_number);
            txn.put(&akey, &attempt_value)?;
        }

        // === Determine new status ===
        let (new_job_status, status_kind, is_terminal) =
            determine_import_status(params, total_attempts, now_ms, effective_start_at_ms);

        // Persist the updated retry policy so future retry decisions (e.g. in
        // report_attempt_outcome) stay consistent with this reimport.
        let updated_job = JobInfo {
            id: existing_job.id().to_string(),
            priority: existing_job.priority(),
            enqueue_time_ms: existing_job.enqueue_time_ms(),
            payload: existing_job.payload_bytes().to_vec(),
            retry_policy: params.retry_policy.clone(),
            metadata: existing_job.metadata(),
            limits: existing_job.limits(),
            task_group: existing_job.task_group().to_string(),
        };
        let updated_job_value = encode_job_info(&updated_job);
        txn.put(job_info_key(tenant, job_id), &updated_job_value)?;

        // === Clean up old scheduling state ===

        // [SILO-REIMP-5] Remove DB queue tasks for this job.
        // Only Scheduled jobs have tasks/concurrency state to clean up.
        // Failed and Cancelled jobs are already terminal with no pending tasks.
        let task_group = existing_job.task_group().to_string();
        let priority = existing_job.priority();
        let stored_limits = existing_job.limits();
        let mut matched_task_keys: Vec<Vec<u8>> = Vec::new();
        let mut released_holders: Vec<(String, Vec<String>)> = Vec::new();

        if old_status.kind == JobStatusKind::Scheduled {
            // O(1) task key reconstruction from status fields (same pattern as cancel/expedite/lease)
            let attempt_number = old_status.current_attempt.unwrap_or(1);
            let start_time_ms = old_status.next_attempt_starts_after_ms.unwrap_or(0);
            let computed_key =
                task_key(&task_group, start_time_ms, priority, job_id, attempt_number);
            let task_found = match txn.get(&computed_key).await? {
                Some(raw) => Some((computed_key, raw)),
                None => {
                    // Fallback: try with time=0 (immediate scheduling case)
                    let zero_key = task_key(&task_group, 0, priority, job_id, attempt_number);
                    txn.get(&zero_key).await?.map(|raw| (zero_key, raw))
                }
            };

            if let Some((found_key, task_raw)) = task_found {
                let decoded = decode_task(&task_raw)
                    .map_err(|e| JobStoreShardError::Codec(format!("reimport task decode: {e}")))?;

                txn.delete(&found_key)?;
                matched_task_keys.push(found_key);

                match decoded {
                    Task::RunAttempt {
                        id: tid,
                        held_queues,
                        ..
                    }
                    | Task::CheckRateLimit {
                        task_id: tid,
                        held_queues,
                        ..
                    } => {
                        // Delete concurrency holders for each held queue
                        for queue in &held_queues {
                            let holder_key = concurrency_holder_key(tenant, queue, &tid);
                            txn.delete(&holder_key)?;
                        }
                        if !held_queues.is_empty() {
                            released_holders.push((tid, held_queues));
                        }
                    }
                    Task::RequestTicket { .. } => {
                        // FutureRequestTaskWritten case: deleting the task is sufficient.
                    }
                    _ => {}
                }
            } else {
                // No task found - TicketRequested case (request record exists but no task
                // in DB queue). Delete concurrency requests for this job.
                self.delete_concurrency_requests_for_job(&txn, tenant, job_id, &stored_limits)
                    .await?;
            }
        }

        // === Update status, scheduling state, and counters ===
        let mut writer = TxnWriter(&txn);

        self.set_job_status_with_index(&mut writer, tenant, job_id, new_job_status)
            .await?;

        // Clean up cancelled key: cancel_job writes a separate cancelled key that blocks
        // lease creation. Always remove it on reimport so it doesn't block future dequeue
        // or leave stale state on terminal jobs.
        if old_status.kind == JobStatusKind::Cancelled {
            txn.delete(job_cancelled_key(tenant, job_id))?;
        }

        // Create new scheduling state if non-terminal
        let mut grants = Vec::new();
        if !is_terminal {
            let next_attempt = total_attempts + 1;
            let new_task_id = Uuid::new_v4().to_string();

            // [SILO-REIMP-8] status = Scheduled
            // [SILO-REIMP-9] new task in DB queue, old tasks for j removed
            // [SILO-REIMP-CONC-1/2] concurrency granted path
            // [SILO-REIMP-CONC-3/4] concurrency queued path
            grants = self
                .enqueue_limit_task_at_index(
                    &mut writer,
                    LimitTaskParams {
                        tenant,
                        task_id: &new_task_id,
                        job_id,
                        attempt_number: next_attempt,
                        relative_attempt_number: next_attempt,
                        limit_index: 0,
                        limits: &stored_limits,
                        priority,
                        start_at_ms: effective_start_at_ms,
                        now_ms,
                        held_queues: Vec::new(),
                        task_group: &task_group,
                    },
                )
                .await?;
        }
        // [SILO-REIMP-7] If terminal: status is terminal, no new tasks

        // Update counters
        let was_terminal = old_status.is_terminal();
        if !was_terminal && is_terminal {
            self.increment_completed_jobs_counter(&mut writer)?;
        }
        if was_terminal && !is_terminal {
            self.decrement_completed_jobs_counter(&mut writer)?;
        }
        // total_jobs counter unchanged (job already counted)

        // === DST events, commit ===
        let write_op = if !is_terminal {
            let op = dst_events::next_write_op();
            dst_events::emit_pending(
                DstEvent::JobEnqueued {
                    tenant: tenant.to_string(),
                    job_id: job_id.to_string(),
                },
                op,
            );
            Some(op)
        } else {
            None
        };

        if let Err(e) = txn
            .commit_with_options(&WriteOptions {
                await_durable: true,
            })
            .await
        {
            if let Some(op) = write_op {
                dst_events::cancel_write(op);
            }
            // Rollback new grants
            self.rollback_grants(tenant, &grants);
            return Err(e.into());
        }
        if let Some(op) = write_op {
            dst_events::confirm_write(op);
        }

        // [SILO-REIMP-6] Remove buffered tasks for this job.
        // Must happen after commit so the scanner cannot re-buffer old tasks from DB.
        self.broker.evict_keys(&matched_task_keys);

        // Release in-memory holders and grant next requests post-commit.
        // This mirrors cancel/lease behavior: DB task cleanup commits first, then
        // release-and-grant runs as a follow-up durable write.
        for (finished_task_id, held_queues) in &released_holders {
            let mut grant_batch = WriteBatch::new();
            let release_rollbacks = self
                .concurrency
                .release_and_grant_next(
                    &self.db,
                    &mut grant_batch,
                    tenant,
                    held_queues,
                    finished_task_id,
                    now_ms,
                )
                .await
                .map_err(|e| JobStoreShardError::Codec(e.to_string()))?;

            if let Err(e) = self
                .db
                .write_with_options(
                    grant_batch,
                    &WriteOptions {
                        await_durable: true,
                    },
                )
                .await
            {
                self.concurrency.rollback_release_grants(&release_rollbacks);
                warn!(error = %e, job_id = %job_id, "reimport: grant-next write failed, rolled back in-memory state");
            } else {
                self.broker.wakeup();
            }
        }

        // For non-terminal, finish enqueue (flush + broker wakeup)
        if !is_terminal {
            self.finish_enqueue(job_id, effective_start_at_ms, &grants)
                .await?;
        }

        debug!(job_id = %job_id, status = ?status_kind, "reimported job");

        Ok(status_kind)
    }
}

/// Validate that all imported attempts are terminal and properly ordered.
fn validate_import_attempts(attempts: &[ImportedAttempt]) -> Result<(), String> {
    for (i, attempt) in attempts.iter().enumerate() {
        // All attempts must be terminal
        match &attempt.status {
            ImportedAttemptStatus::Succeeded { .. }
            | ImportedAttemptStatus::Failed { .. }
            | ImportedAttemptStatus::Cancelled => {}
        }

        // All attempts except the last must be Failed
        if i < attempts.len() - 1 && !matches!(attempt.status, ImportedAttemptStatus::Failed { .. })
        {
            return Err(format!(
                "import attempt {} must be Failed (only the last attempt may be non-Failed)",
                i + 1
            ));
        }
    }
    Ok(())
}

/// Determine the status for an imported job based on its attempts and retry policy.
fn determine_import_status(
    params: &ImportJobParams,
    num_attempts: u32,
    now_ms: i64,
    effective_start_at_ms: i64,
) -> (JobStatus, JobStatusKind, bool) {
    if num_attempts == 0 {
        return (
            JobStatus::scheduled(now_ms, effective_start_at_ms, 1),
            JobStatusKind::Scheduled,
            false,
        );
    }

    let last_attempt = &params.attempts[params.attempts.len() - 1];

    match &last_attempt.status {
        ImportedAttemptStatus::Succeeded { .. } => (
            JobStatus::succeeded(last_attempt.finished_at_ms),
            JobStatusKind::Succeeded,
            true,
        ),
        ImportedAttemptStatus::Cancelled => (
            JobStatus::cancelled(last_attempt.finished_at_ms),
            JobStatusKind::Cancelled,
            true,
        ),
        ImportedAttemptStatus::Failed { .. } => {
            let failed_count = params
                .attempts
                .iter()
                .filter(|a| matches!(a.status, ImportedAttemptStatus::Failed { .. }))
                .count() as u32;

            let exhausted = match params.retry_policy {
                Some(ref policy) => retries_exhausted(failed_count, policy),
                None => true, // No retry policy -> always exhausted
            };

            if exhausted {
                (
                    JobStatus::failed(last_attempt.finished_at_ms),
                    JobStatusKind::Failed,
                    true,
                )
            } else {
                let next_attempt = num_attempts + 1;
                (
                    JobStatus::scheduled(now_ms, effective_start_at_ms, next_attempt),
                    JobStatusKind::Scheduled,
                    false,
                )
            }
        }
    }
}
