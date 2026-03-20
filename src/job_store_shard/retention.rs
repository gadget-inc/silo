use std::sync::Arc;

use slatedb::IsolationLevel;
use slatedb::config::WriteOptions;

use crate::job::JobView;
use crate::job_store_shard::counters::encode_counter;
use crate::job_store_shard::helpers::{TxnWriter, WriteBatcher, retry_on_txn_conflict};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{
    attempt_prefix, end_bound, idx_metadata_key, idx_status_time_all_prefix, idx_status_time_key,
    job_cancelled_key, job_info_key, job_status_key, parse_status_time_index_key,
    status_index_timestamp, tenant_status_counter_key,
};
use tracing::{debug, info, warn};

impl JobStoreShard {
    /// Resolve the effective terminal retention in milliseconds for a job.
    ///
    /// If the job specifies `terminal_retention_ms`, that value is used. Otherwise
    /// the shard-level default is used.
    pub(crate) fn effective_terminal_retention_ms(
        &self,
        requested: Option<i64>,
    ) -> Result<i64, JobStoreShardError> {
        let retention_ms = requested.unwrap_or(self.default_terminal_retention_ms);
        if retention_ms < 0 {
            return Err(JobStoreShardError::InvalidArgument(
                "terminal_retention_ms must be >= 0".to_string(),
            ));
        }
        Ok(retention_ms)
    }

    pub(crate) async fn delete_job_records_in_txn(
        &self,
        txn: &slatedb::DbTransaction,
        tenant: &str,
        job_id: &str,
        job_view: &JobView,
        status: &crate::job::JobStatus,
    ) -> Result<(), JobStoreShardError> {
        let mut writer = TxnWriter(txn);

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

    /// Spawn a periodic background scanner that finds and deletes expired terminal jobs.
    pub(crate) fn spawn_retention_scanner(self: &Arc<Self>) {
        let shard = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        let shard_name = self.name.clone();
        let scan_interval = self.retention_scan_interval;

        if scan_interval.is_zero() {
            debug!(
                shard = %shard_name,
                "retention scanner disabled (scan interval is zero)"
            );
            return;
        }

        tokio::spawn(async move {
            // Sleep first to avoid expensive scan during shard acquisition.
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    debug!(shard = %shard_name, "retention scanner cancelled during initial delay");
                    return;
                }
                _ = tokio::time::sleep(scan_interval) => {}
            }

            let mut interval = tokio::time::interval(scan_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        debug!(
                            shard = %shard_name,
                            "stopping retention scanner (shard closing)"
                        );
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = shard.run_retention_scan().await {
                            warn!(
                                shard = %shard_name,
                                error = %e,
                                "retention scan failed"
                            );
                        }
                    }
                }
            }
        });
    }

    /// Run a single retention scan pass over the status/time index.
    ///
    /// Scans all entries in the `0x03` (idx_status_time) prefix. Non-terminal
    /// statuses are skipped via key-only parsing. For terminal entries, the job
    /// info is read to determine the effective retention, and expired jobs are
    /// deleted.
    async fn run_retention_scan(&self) -> Result<(), JobStoreShardError> {
        let now_ms = crate::job_store_shard::now_epoch_ms();
        let prefix = idx_status_time_all_prefix();
        let end = end_bound(&prefix);
        let mut iter = self.db.scan::<Vec<u8>, _>(prefix..end).await?;

        let mut deleted_count: u64 = 0;
        let mut scanned_count: u64 = 0;

        while let Some(kv) = iter.next().await? {
            scanned_count += 1;

            // Check for cancellation periodically
            if scanned_count % 256 == 0 && self.cancellation.is_cancelled() {
                debug!(
                    shard = %self.name,
                    scanned = scanned_count,
                    deleted = deleted_count,
                    "retention scan interrupted by shard close"
                );
                return Ok(());
            }

            let Some(parsed) = parse_status_time_index_key(&kv.key) else {
                continue;
            };

            // Skip non-terminal statuses — key-only check, no point lookups
            let is_terminal = parsed.status == "Failed"
                || parsed.status == "Succeeded"
                || parsed.status == "Cancelled";
            if !is_terminal {
                continue;
            }

            let changed_at_ms = parsed.changed_at_ms();
            let tenant = &parsed.tenant;
            let job_id = &parsed.job_id;

            // Read job info to get the per-job retention setting
            let info_key = job_info_key(tenant, job_id);
            let Some(job_raw) = self.db.get(&info_key).await? else {
                // Job info missing but index entry exists — stale index entry.
                // This can happen if a job was deleted but the index wasn't cleaned up.
                continue;
            };
            let job_view = match JobView::new(job_raw) {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        shard = %self.name,
                        tenant = %tenant,
                        job_id = %job_id,
                        error = %e,
                        "corrupt job info during retention scan, skipping"
                    );
                    continue;
                }
            };
            let retention_ms =
                match self.effective_terminal_retention_ms(job_view.terminal_retention_ms()) {
                    Ok(retention_ms) => retention_ms,
                    Err(e) => {
                        warn!(
                            shard = %self.name,
                            tenant = %tenant,
                            job_id = %job_id,
                            error = %e,
                            "invalid terminal retention on stored job info, skipping"
                        );
                        continue;
                    }
                };

            // Check if retention has elapsed
            let expiry_ms = changed_at_ms.saturating_add(retention_ms);
            if now_ms < expiry_ms {
                continue;
            }

            // Delete the expired job
            if let Err(e) = self.delete_expired_job(tenant, job_id, changed_at_ms).await {
                warn!(
                    shard = %self.name,
                    tenant = %tenant,
                    job_id = %job_id,
                    error = %e,
                    "failed to delete expired job"
                );
                continue;
            }

            deleted_count += 1;
        }

        if let Some(metrics) = &self.metrics {
            metrics.record_retention_scan(&self.name, deleted_count);
        }

        if deleted_count > 0 {
            info!(
                shard = %self.name,
                scanned = scanned_count,
                deleted = deleted_count,
                "retention scan completed"
            );
        } else {
            debug!(
                shard = %self.name,
                scanned = scanned_count,
                "retention scan completed (no expired jobs)"
            );
        }

        Ok(())
    }

    /// Delete a single expired terminal job within a transaction.
    ///
    /// Re-reads the job status within the transaction to verify it is still
    /// terminal and still at the same `changed_at_ms` (guards against
    /// concurrent restart).
    async fn delete_expired_job(
        &self,
        tenant: &str,
        job_id: &str,
        expected_changed_at_ms: i64,
    ) -> Result<(), JobStoreShardError> {
        retry_on_txn_conflict("delete_expired_job", || async {
            let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

            let status_key = job_status_key(tenant, job_id);
            let info_key = job_info_key(tenant, job_id);
            let maybe_status_raw = txn.get(&status_key).await?;
            let maybe_job_raw = txn.get(&info_key).await?;

            match (maybe_status_raw, maybe_job_raw) {
                (Some(status_raw), Some(job_raw)) => {
                    let status =
                        crate::job_store_shard::helpers::decode_job_status_owned(&status_raw)?;
                    let job_view = JobView::new(job_raw)?;

                    // Verify still terminal and unchanged (guards against concurrent restart)
                    if status.is_terminal() && status.changed_at_ms == expected_changed_at_ms {
                        self.delete_job_records_in_txn(&txn, tenant, job_id, &job_view, &status)
                            .await?;
                    }
                    // If status changed (e.g. restarted), silently skip — the job is no longer expired
                }
                _ => {
                    // Job already deleted or partially missing — nothing to do
                }
            }

            txn.commit_with_options(&WriteOptions {
                await_durable: true,
            })
            .await?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use slatedb::config::{DurabilityLevel, ReadOptions};

    use super::*;
    use crate::gubernator::MockGubernatorClient;
    use crate::job_attempt::AttemptOutcome;
    use crate::keys::{job_info_key, job_status_key};
    use crate::settings::{Backend, DEFAULT_TERMINAL_RETENTION, DatabaseConfig};
    use crate::shard_range::ShardRange;

    #[tokio::test]
    async fn delete_expired_job_waits_for_remote_durability() {
        let data_dir = tempfile::tempdir().unwrap();
        let data_path = data_dir.path().to_string_lossy().to_string();
        let job_id = "retention-delete-durable";

        let initial_cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: data_path.clone(),
            wal: None,
            apply_wal_on_close: true,
            default_terminal_retention: DEFAULT_TERMINAL_RETENTION,
            retention_scan_interval: Duration::from_secs(86400),
            slatedb: Some(slatedb::config::Settings {
                flush_interval: Some(Duration::from_millis(10)),
                ..Default::default()
            }),
            memory_cache: None,
        };
        let initial_shard = JobStoreShard::open(
            &initial_cfg,
            MockGubernatorClient::new_arc(),
            None,
            ShardRange::full(),
        )
        .await
        .expect("open initial shard");

        initial_shard
            .enqueue(
                "-",
                Some(job_id.to_string()),
                50,
                0,
                None,
                vec![1],
                vec![],
                None,
                Some(0),
                "default",
            )
            .await
            .expect("enqueue");

        let task = initial_shard
            .dequeue("worker-1", "default", 1)
            .await
            .expect("dequeue")
            .tasks
            .pop()
            .expect("task");
        initial_shard
            .report_attempt_outcome(
                task.attempt().task_id(),
                AttemptOutcome::Success { result: vec![9] },
            )
            .await
            .expect("report outcome");
        let terminal_status = initial_shard
            .get_job_status("-", job_id)
            .await
            .expect("get status")
            .expect("job exists");

        initial_shard.close().await.expect("close initial shard");

        let delete_cfg = DatabaseConfig {
            name: "test".to_string(),
            backend: Backend::Fs,
            path: data_path,
            wal: None,
            apply_wal_on_close: true,
            default_terminal_retention: DEFAULT_TERMINAL_RETENTION,
            retention_scan_interval: Duration::from_secs(86400),
            slatedb: Some(slatedb::config::Settings {
                flush_interval: Some(Duration::from_secs(1)),
                ..Default::default()
            }),
            memory_cache: None,
        };
        let shard = JobStoreShard::open(
            &delete_cfg,
            MockGubernatorClient::new_arc(),
            None,
            ShardRange::full(),
        )
        .await
        .expect("open delete shard");

        let remote_read = ReadOptions::new().with_durability_filter(DurabilityLevel::Remote);
        assert!(
            shard
                .db
                .get_with_options(&job_info_key("-", job_id), &remote_read)
                .await
                .expect("remote get before delete")
                .is_some(),
            "terminal job should already be visible at the remote durability boundary"
        );

        shard
            .delete_expired_job("-", job_id, terminal_status.changed_at_ms)
            .await
            .expect("delete expired job");

        assert!(
            shard
                .db
                .get_with_options(&job_info_key("-", job_id), &remote_read)
                .await
                .expect("remote get job info after delete")
                .is_none(),
            "delete_expired_job should not return before the delete is durable"
        );
        assert!(
            shard
                .db
                .get_with_options(&job_status_key("-", job_id), &remote_read)
                .await
                .expect("remote get job status after delete")
                .is_none()
        );
    }
}
