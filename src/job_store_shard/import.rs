//! Job import operations.
//!
//! Allows importing jobs from other systems with historical attempt records.
//! Unlike enqueue, import accepts completed attempts and lets Silo take ownership going forward.

use slatedb::ErrorKind as SlateErrorKind;
use slatedb::IsolationLevel;
use slatedb::config::WriteOptions;
use tracing::debug;
use uuid::Uuid;

use crate::codec::{encode_attempt, encode_job_info};
use crate::dst_events::{self, DstEvent};
use crate::job::{JobInfo, JobStatus, JobStatusKind, Limit};
use crate::job_attempt::{AttemptStatus, JobAttempt};
use crate::job_store_shard::helpers::{TxnWriter, now_epoch_ms};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError, LimitTaskParams};
use crate::keys::{attempt_key, idx_metadata_key, job_info_key};
use crate::retry::{RetryPolicy, retries_exhausted};

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
        // Validate
        if params.id.is_empty() {
            return Err(JobStoreShardError::InvalidArgument(
                "import job id is required".into(),
            ));
        }
        if let Err(msg) = validate_import_attempts(&params.attempts) {
            return Err(JobStoreShardError::InvalidArgument(msg));
        }

        const MAX_RETRIES: usize = 5;

        for attempt in 0..MAX_RETRIES {
            match self.import_job_txn(tenant, &params).await {
                Ok(status) => return Ok(status),
                Err(JobStoreShardError::Slate(ref e))
                    if e.kind() == SlateErrorKind::Transaction =>
                {
                    if attempt + 1 < MAX_RETRIES {
                        let delay_ms = 10 * (1 << attempt);
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        debug!(
                            operation = "import_job",
                            attempt = attempt + 1,
                            "transaction conflict, retrying"
                        );
                        continue;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Err(JobStoreShardError::TransactionConflict(
            "import_job".to_string(),
        ))
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

        // Check for duplicate
        let info_key = job_info_key(tenant, job_id);
        if txn.get(&info_key).await?.is_some() {
            return Err(JobStoreShardError::JobAlreadyExists(job_id.to_string()));
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
        let job_value = encode_job_info(&job)?;
        txn.put(&info_key, &job_value)?;

        // Write metadata secondary index
        for (mk, mv) in &job.metadata {
            let mkey = idx_metadata_key(tenant, mk, mv, job_id);
            txn.put(&mkey, [])?;
        }

        // Write imported attempt records
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
            let attempt_value = encode_attempt(&attempt_record)?;
            let akey = attempt_key(tenant, job_id, attempt_number);
            txn.put(&akey, &attempt_value)?;
        }

        let (job_status, status_kind, is_terminal) =
            determine_import_status(params, num_attempts, now_ms, effective_start_at_ms);

        // Write status + index
        let mut writer = TxnWriter(&txn);
        Self::write_job_status_with_index(&mut writer, tenant, job_id, job_status)?;

        // For non-terminal imports, create a task for the next attempt
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
