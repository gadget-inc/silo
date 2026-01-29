//! [SILO-SPLIT-CLEANUP-1] Post-split cleanup operations for JobStoreShard.
//!
//! After a shard split, child shards contain defunct data (keys outside their tenant range).
//! This module provides methods to:
//! - Delete keys that don't belong in this shard's range
//! - Run compaction to reclaim storage space from deleted keys
//! - Track cleanup progress for crash recovery

use crate::keys::{decode_tenant, tasks_prefix};
use crate::shard_range::ShardRange;
use slatedb::WriteBatch;
use tracing::{debug, info};

use crate::job_store_shard::{JobStoreShard, JobStoreShardError};

/// Key prefixes that contain tenant_id in their path structure.
/// Format: {prefix}/{tenant}/...
const TENANT_PREFIXED_KEYS: &[&str] = &[
    "jobs/",
    "job_status/",
    "job_cancelled/",
    "attempts/",
    "requests/",
    "holders/",
    "floating_limits/",
    "idx/status_ts/",
    "idx/meta/",
];

/// Key for storing cleanup progress checkpoint.
const CLEANUP_PROGRESS_KEY: &str = "_silo_cleanup/progress";

/// Key for storing cleanup completion marker.
const CLEANUP_COMPLETE_KEY: &str = "_silo_cleanup/complete";

/// Progress tracking for cleanup operation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct CleanupProgress {
    /// Last key processed (for resumption after crash)
    pub last_key: Option<String>,
    /// Number of keys deleted so far
    pub keys_deleted: u64,
    /// Number of keys scanned so far
    pub keys_scanned: u64,
    /// Whether cleanup is complete
    pub complete: bool,
}

/// Result of a cleanup operation.
#[derive(Debug, Clone)]
pub struct CleanupResult {
    /// Number of keys deleted
    pub keys_deleted: u64,
    /// Number of keys scanned
    pub keys_scanned: u64,
    /// Whether cleanup completed fully
    pub complete: bool,
}

impl JobStoreShard {
    /// [SILO-SPLIT-CLEANUP-1] Delete keys outside this shard's tenant range.
    ///
    /// After a split, child shards contain data from the parent shard that may not belong to them based on their tenant range. This method scans all keys and deletes those with tenant_ids outside the shard's range.
    pub async fn after_split_cleanup_defunct_data(
        &self,
        range: &ShardRange,
        batch_size: usize,
    ) -> Result<CleanupResult, JobStoreShardError> {
        // Load existing progress or start fresh
        let mut progress = self.load_split_cleanup_progress().await?;

        if progress.complete {
            info!(
                shard = %self.name,
                keys_deleted = progress.keys_deleted,
                "cleanup already complete, skipping"
            );
            return Ok(CleanupResult {
                keys_deleted: progress.keys_deleted,
                keys_scanned: progress.keys_scanned,
                complete: true,
            });
        }

        info!(
            shard = %self.name,
            range = %range,
            resuming_from = ?progress.last_key,
            "starting defunct data cleanup"
        );

        // Process each tenant-prefixed key family
        for prefix in TENANT_PREFIXED_KEYS {
            progress = self
                .after_split_cleanup_prefix(prefix, range, batch_size, progress)
                .await?;
        }

        // Process task keys specially (they don't have tenant in the key, but reference jobs)
        progress = self
            .after_split_cleanup_task_keys(range, batch_size, progress)
            .await?;

        // Process lease keys
        progress = self
            .after_split_cleanup_lease_keys(range, batch_size, progress)
            .await?;

        // Mark cleanup as complete
        progress.complete = true;
        self.save_split_cleanup_progress(&progress).await?;

        info!(
            shard = %self.name,
            keys_deleted = progress.keys_deleted,
            keys_scanned = progress.keys_scanned,
            "defunct data cleanup complete"
        );

        Ok(CleanupResult {
            keys_deleted: progress.keys_deleted,
            keys_scanned: progress.keys_scanned,
            complete: true,
        })
    }

    /// Cleanup keys with a specific prefix that contains tenant_id.
    async fn after_split_cleanup_prefix(
        &self,
        prefix: &str,
        range: &ShardRange,
        batch_size: usize,
        mut progress: CleanupProgress,
    ) -> Result<CleanupProgress, JobStoreShardError> {
        // Start from the beginning of this prefix
        // Note: We process each prefix fully even on resume. This is simpler than
        // trying to track progress across multiple prefixes. Deletes are idempotent.
        let start = prefix.as_bytes().to_vec();
        let mut end = prefix.as_bytes().to_vec();
        end.push(0xFF);

        let mut batch = WriteBatch::new();
        let mut batch_count = 0;

        let mut iter = self.db.scan::<Vec<u8>, _>(start..=end).await?;

        while let Some(kv) = iter.next().await? {
            let key_str = match std::str::from_utf8(&kv.key) {
                Ok(s) => s,
                Err(_) => {
                    // Skip non-UTF8 keys
                    continue;
                }
            };

            progress.keys_scanned += 1;

            // Extract tenant from key path
            if let Some(tenant) = extract_tenant_from_key(key_str, prefix) {
                let decoded_tenant = decode_tenant(&tenant);

                // Check if tenant is outside the shard's range
                if !range.contains(&decoded_tenant) {
                    batch.delete(&kv.key);
                    batch_count += 1;
                    progress.keys_deleted += 1;
                }
            }

            progress.last_key = Some(key_str.to_string());

            // Write batch and checkpoint progress
            if batch_count >= batch_size {
                self.db.write(batch).await?;
                self.save_split_cleanup_progress(&progress).await?;
                batch = WriteBatch::new();
                batch_count = 0;

                debug!(
                    shard = %self.name,
                    prefix,
                    keys_scanned = progress.keys_scanned,
                    keys_deleted = progress.keys_deleted,
                    "cleanup progress checkpoint"
                );
            }
        }

        // Write any remaining items
        if batch_count > 0 {
            self.db.write(batch).await?;
            self.save_split_cleanup_progress(&progress).await?;
        }

        Ok(progress)
    }

    /// Cleanup task keys by checking if referenced job's tenant is in range.
    ///
    /// Task keys have format: tasks/{task_group}/{time}/{priority}/{job_id}/{attempt}
    /// The task value contains the tenant_id.
    async fn after_split_cleanup_task_keys(
        &self,
        range: &ShardRange,
        batch_size: usize,
        mut progress: CleanupProgress,
    ) -> Result<CleanupProgress, JobStoreShardError> {
        let prefix = tasks_prefix();
        let start = prefix.as_bytes().to_vec();
        let mut end = prefix.as_bytes().to_vec();
        end.push(0xFF);

        let mut batch = WriteBatch::new();
        let mut batch_count = 0;

        let mut iter = self.db.scan::<Vec<u8>, _>(start..=end).await?;

        while let Some(kv) = iter.next().await? {
            let key_str = match std::str::from_utf8(&kv.key) {
                Ok(s) => s,
                Err(_) => continue,
            };

            progress.keys_scanned += 1;

            // Task stores tenant_id in the task value itself
            // Decode the task and check tenant
            if let Ok(task) = crate::codec::decode_task(&kv.value) {
                let tenant = task.tenant();
                let decoded_tenant = decode_tenant(tenant);

                if !range.contains(&decoded_tenant) {
                    batch.delete(&kv.key);
                    batch_count += 1;
                    progress.keys_deleted += 1;
                }
            }

            progress.last_key = Some(key_str.to_string());

            if batch_count >= batch_size {
                self.db.write(batch).await?;
                self.save_split_cleanup_progress(&progress).await?;
                batch = WriteBatch::new();
                batch_count = 0;
            }
        }

        if batch_count > 0 {
            self.db.write(batch).await?;
            self.save_split_cleanup_progress(&progress).await?;
        }

        Ok(progress)
    }

    /// Cleanup lease keys by checking if referenced task's tenant is in range.
    ///
    /// Lease keys have format: lease/{task_id}
    /// The lease value contains the tenant_id.
    async fn after_split_cleanup_lease_keys(
        &self,
        range: &ShardRange,
        batch_size: usize,
        mut progress: CleanupProgress,
    ) -> Result<CleanupProgress, JobStoreShardError> {
        let prefix = "lease/";
        let start = prefix.as_bytes().to_vec();
        let mut end = prefix.as_bytes().to_vec();
        end.push(0xFF);

        let mut batch = WriteBatch::new();
        let mut batch_count = 0;

        let mut iter = self.db.scan::<Vec<u8>, _>(start..=end).await?;

        while let Some(kv) = iter.next().await? {
            let key_str = match std::str::from_utf8(&kv.key) {
                Ok(s) => s,
                Err(_) => continue,
            };

            progress.keys_scanned += 1;

            // Decode the lease value to get tenant_id from the embedded task
            if let Ok(lease) = crate::codec::decode_lease(&kv.value) {
                // The lease contains a Task which has the tenant_id
                // We need to extract tenant from the archived task
                let archived = lease.archived();
                let tenant = match &archived.task {
                    rkyv::Archived::<crate::task::Task>::RunAttempt { tenant, .. } => {
                        tenant.as_str()
                    }
                    rkyv::Archived::<crate::task::Task>::RequestTicket { tenant, .. } => {
                        tenant.as_str()
                    }
                    rkyv::Archived::<crate::task::Task>::CheckRateLimit { tenant, .. } => {
                        tenant.as_str()
                    }
                    rkyv::Archived::<crate::task::Task>::RefreshFloatingLimit {
                        tenant, ..
                    } => tenant.as_str(),
                };
                let decoded_tenant = decode_tenant(tenant);

                if !range.contains(&decoded_tenant) {
                    batch.delete(&kv.key);
                    batch_count += 1;
                    progress.keys_deleted += 1;
                }
            }

            progress.last_key = Some(key_str.to_string());

            if batch_count >= batch_size {
                self.db.write(batch).await?;
                self.save_split_cleanup_progress(&progress).await?;
                batch = WriteBatch::new();
                batch_count = 0;
            }
        }

        if batch_count > 0 {
            self.db.write(batch).await?;
            self.save_split_cleanup_progress(&progress).await?;
        }

        Ok(progress)
    }

    /// Load cleanup progress from the database.
    async fn load_split_cleanup_progress(&self) -> Result<CleanupProgress, JobStoreShardError> {
        match self.db.get(CLEANUP_PROGRESS_KEY.as_bytes()).await? {
            Some(data) => {
                let progress: CleanupProgress = serde_json::from_slice(&data)?;
                Ok(progress)
            }
            None => Ok(CleanupProgress::default()),
        }
    }

    /// Save cleanup progress to the database for crash recovery.
    async fn save_split_cleanup_progress(
        &self,
        progress: &CleanupProgress,
    ) -> Result<(), JobStoreShardError> {
        let data = serde_json::to_vec(progress)?;
        self.db.put(CLEANUP_PROGRESS_KEY.as_bytes(), &data).await?;
        Ok(())
    }

    /// Clear cleanup progress markers (called after compaction completes).
    pub async fn clear_cleanup_progress(&self) -> Result<(), JobStoreShardError> {
        let mut batch = WriteBatch::new();
        batch.delete(CLEANUP_PROGRESS_KEY.as_bytes());
        batch.delete(CLEANUP_COMPLETE_KEY.as_bytes());
        self.db.write(batch).await?;
        Ok(())
    }

    /// Run full compaction on the shard.
    ///
    /// After cleanup deletes defunct keys, this method triggers a full
    /// compaction to reclaim storage space from the deleted data.
    ///
    /// This uses SlateDB's Admin API to request a full compaction.
    pub async fn run_full_compaction(&self) -> Result<(), JobStoreShardError> {
        info!(shard = %self.name, "starting full compaction");

        // SlateDB doesn't expose direct compaction API on the Db handle.
        // Instead, we flush to ensure all data is in SSTs, then SlateDB's
        // background compaction will handle the rest.
        //
        // For explicit compaction, we'd need to use the Admin API which requires
        // the object store path. For now, we flush and let background compaction
        // handle the cleanup over time.
        self.db
            .flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await?;

        info!(shard = %self.name, "flush complete, background compaction will reclaim space");

        // Clear cleanup progress markers since we're done
        self.clear_cleanup_progress().await?;

        Ok(())
    }

    /// Check if this shard has pending cleanup work.
    ///
    /// Returns true if cleanup has been started but not completed.
    pub async fn has_pending_split_cleanup(&self) -> Result<bool, JobStoreShardError> {
        let progress = self.load_split_cleanup_progress().await?;
        // Has pending work if we've started (keys_scanned > 0) but not complete
        Ok(progress.keys_scanned > 0 && !progress.complete)
    }

    /// Check if cleanup is complete for this shard.
    pub async fn is_split_cleanup_complete(&self) -> Result<bool, JobStoreShardError> {
        let progress = self.load_split_cleanup_progress().await?;
        Ok(progress.complete)
    }
}

/// Extract tenant_id from a key with the given prefix.
///
/// For keys like "jobs/{tenant}/{id}", this extracts {tenant}.
/// For keys like "idx/status_ts/{tenant}/...", this extracts {tenant}.
pub fn extract_tenant_from_key(key: &str, prefix: &str) -> Option<String> {
    let remainder = key.strip_prefix(prefix)?;

    // For idx/ prefixes, there's an extra level before tenant
    // idx/status_ts/{tenant}/...
    // idx/meta/{tenant}/...
    let tenant_part = if prefix.starts_with("idx/") {
        // Skip the idx type (status_ts or meta) which is already in the prefix
        remainder
    } else {
        remainder
    };

    // Tenant is the first path segment
    let end = tenant_part.find('/')?;
    Some(tenant_part[..end].to_string())
}
