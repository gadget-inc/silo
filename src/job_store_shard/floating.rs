//! Floating concurrency limit operations.

use slatedb::WriteBatch;
use uuid::Uuid;

use crate::codec::{
    decode_floating_limit_state, decode_lease, encode_floating_limit_state, encode_task,
    DecodedFloatingLimitState,
};
use crate::job::{FloatingConcurrencyLimit, FloatingLimitState};
use crate::job_store_shard::helpers::now_epoch_ms;
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{floating_limit_state_key, leased_task_key};
use crate::task::Task;

impl JobStoreShard {
    /// Get or create the floating limit state for a given queue key.
    /// Returns a zero-copy decoded view. For the rare "just created" case,
    /// we encode then decode to return the same type (extra decode is fine for cold path).
    pub(crate) async fn get_or_create_floating_limit_state(
        &self,
        batch: &mut WriteBatch,
        tenant: &str,
        fl: &FloatingConcurrencyLimit,
    ) -> Result<DecodedFloatingLimitState, JobStoreShardError> {
        let state_key = floating_limit_state_key(tenant, &fl.key);

        if let Some(raw) = self.db.get(state_key.as_bytes()).await? {
            // Hot path: state exists, return zero-copy decoded view
            return Ok(decode_floating_limit_state(&raw)?);
        }

        // Cold path: first time seeing this queue key, create and write new state
        let state = FloatingLimitState {
            current_max_concurrency: fl.default_max_concurrency,
            last_refreshed_at_ms: 0,
            refresh_task_scheduled: false,
            refresh_interval_ms: fl.refresh_interval_ms,
            default_max_concurrency: fl.default_max_concurrency,
            retry_count: 0,
            next_retry_at_ms: None,
            metadata: fl.metadata.clone(),
        };

        let state_bytes = encode_floating_limit_state(&state)?;
        batch.put(state_key.as_bytes(), &state_bytes);

        // Decode what we just encoded so we return the same type
        Ok(decode_floating_limit_state(&state_bytes)?)
    }

    /// Check if a floating limit refresh is needed and schedule it if so.
    /// This method is called during enqueue and dequeue operations to lazily trigger refreshes.
    pub(crate) fn maybe_schedule_floating_limit_refresh(
        &self,
        batch: &mut WriteBatch,
        tenant: &str,
        fl: &FloatingConcurrencyLimit,
        state: &DecodedFloatingLimitState,
        now_ms: i64,
    ) -> Result<(), JobStoreShardError> {
        let archived = state.archived();

        // Check if refresh is already scheduled
        if archived.refresh_task_scheduled {
            return Ok(());
        }

        // Check if we need to refresh based on interval
        let next_refresh_due = archived.last_refreshed_at_ms + archived.refresh_interval_ms;
        let should_refresh = now_ms >= next_refresh_due;

        // Also check if we're in backoff from a failed refresh
        let in_backoff = archived
            .next_retry_at_ms
            .as_ref()
            .map(|&t| now_ms < t)
            .unwrap_or(false);

        if !should_refresh || in_backoff {
            return Ok(());
        }

        // Schedule a refresh task
        let task_id = Uuid::new_v4().to_string();
        let refresh_task = Task::RefreshFloatingLimit {
            task_id: task_id.clone(),
            tenant: tenant.to_string(),
            queue_key: fl.key.clone(),
            current_max_concurrency: archived.current_max_concurrency,
            last_refreshed_at_ms: archived.last_refreshed_at_ms,
            metadata: archived
                .metadata
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
                .collect(),
        };

        // Use a synthetic task key based on the queue key to avoid collisions
        let task_value = encode_task(&refresh_task)?;
        let task_key_str = format!(
            "tasks/{:020}/{:02}/floating_refresh/{}/{}",
            now_ms,
            0, // highest priority for refresh tasks
            fl.key,
            task_id
        );
        batch.put(task_key_str.as_bytes(), &task_value);

        // Update state to mark refresh as scheduled - construct new state directly
        let new_state = FloatingLimitState {
            refresh_task_scheduled: true,
            current_max_concurrency: archived.current_max_concurrency,
            last_refreshed_at_ms: archived.last_refreshed_at_ms,
            refresh_interval_ms: archived.refresh_interval_ms,
            default_max_concurrency: archived.default_max_concurrency,
            retry_count: archived.retry_count,
            next_retry_at_ms: archived.next_retry_at_ms.as_ref().copied(),
            metadata: archived
                .metadata
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
                .collect(),
        };
        let state_key = floating_limit_state_key(tenant, &fl.key);
        let state_value = encode_floating_limit_state(&new_state)?;
        batch.put(state_key.as_bytes(), &state_value);

        tracing::debug!(
            queue_key = %fl.key,
            current_max = archived.current_max_concurrency,
            last_refreshed = archived.last_refreshed_at_ms,
            "scheduled floating limit refresh task"
        );

        Ok(())
    }

    /// Report a successful floating limit refresh from a worker.
    /// Updates the floating limit state with the new max concurrency value.
    pub async fn report_refresh_success(
        &self,
        tenant: &str,
        task_id: &str,
        new_max_concurrency: u32,
    ) -> Result<(), JobStoreShardError> {
        // Load the lease to get the queue key
        let lease_key = leased_task_key(task_id);
        let maybe_raw = self.db.get(lease_key.as_bytes()).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(JobStoreShardError::LeaseNotFound(task_id.to_string()));
        };

        let decoded = decode_lease(&value_bytes)?;
        let queue_key = match decoded.to_task() {
            Task::RefreshFloatingLimit { queue_key, .. } => queue_key,
            _ => {
                return Err(JobStoreShardError::Rkyv(
                    "task is not a RefreshFloatingLimit".to_string(),
                ))
            }
        };

        let now_ms = now_epoch_ms();
        let state_key = floating_limit_state_key(tenant, &queue_key);

        // Load state and construct new state with updates (zero-copy read from archived)
        let maybe_state = self.db.get(state_key.as_bytes()).await?;
        let Some(raw) = maybe_state else {
            return Err(JobStoreShardError::Rkyv(format!(
                "floating limit state not found for queue {}",
                queue_key
            )));
        };
        let decoded = decode_floating_limit_state(&raw)?;
        let archived = decoded.archived();

        // Construct new state directly - avoids intermediate owned allocation
        let new_state = FloatingLimitState {
            current_max_concurrency: new_max_concurrency,
            last_refreshed_at_ms: now_ms,
            refresh_task_scheduled: false,
            retry_count: 0,
            next_retry_at_ms: None,
            // Preserve unchanged fields from archived view
            refresh_interval_ms: archived.refresh_interval_ms,
            default_max_concurrency: archived.default_max_concurrency,
            metadata: archived
                .metadata
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
                .collect(),
        };

        let mut batch = WriteBatch::new();
        let state_value = encode_floating_limit_state(&new_state)?;
        batch.put(state_key.as_bytes(), &state_value);
        batch.delete(lease_key.as_bytes());

        self.db.write(batch).await?;
        self.db.flush().await?;

        tracing::debug!(
            queue_key = %queue_key,
            new_max_concurrency = new_max_concurrency,
            "floating limit refresh succeeded"
        );

        Ok(())
    }

    /// Report a failed floating limit refresh from a worker.
    /// Schedules a retry with exponential backoff.
    pub async fn report_refresh_failure(
        &self,
        tenant: &str,
        task_id: &str,
        error_code: &str,
        error_message: &str,
    ) -> Result<(), JobStoreShardError> {
        // Load the lease to get the task details
        let lease_key = leased_task_key(task_id);
        let maybe_raw = self.db.get(lease_key.as_bytes()).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(JobStoreShardError::LeaseNotFound(task_id.to_string()));
        };

        let decoded = decode_lease(&value_bytes)?;
        let (queue_key, current_max_concurrency, last_refreshed_at_ms, metadata) =
            match decoded.to_task() {
                Task::RefreshFloatingLimit {
                    queue_key,
                    current_max_concurrency,
                    last_refreshed_at_ms,
                    metadata,
                    ..
                } => (
                    queue_key,
                    current_max_concurrency,
                    last_refreshed_at_ms,
                    metadata,
                ),
                _ => {
                    return Err(JobStoreShardError::Rkyv(
                        "task is not a RefreshFloatingLimit".to_string(),
                    ))
                }
            };

        let now_ms = now_epoch_ms();
        let state_key = floating_limit_state_key(tenant, &queue_key);

        // Load state (zero-copy read from archived)
        let maybe_state = self.db.get(state_key.as_bytes()).await?;
        let Some(raw) = maybe_state else {
            return Err(JobStoreShardError::Rkyv(format!(
                "floating limit state not found for queue {}",
                queue_key
            )));
        };
        let decoded = decode_floating_limit_state(&raw)?;
        let archived = decoded.archived();

        // Calculate exponential backoff using archived retry_count
        const INITIAL_BACKOFF_MS: i64 = 1000; // 1 second
        const MAX_BACKOFF_MS: i64 = 60_000; // 1 minute
        const BACKOFF_MULTIPLIER: f64 = 2.0;

        let new_retry_count = archived.retry_count + 1;
        let backoff_ms = ((INITIAL_BACKOFF_MS as f64)
            * BACKOFF_MULTIPLIER.powi(archived.retry_count as i32))
        .round() as i64;
        let capped_backoff_ms = backoff_ms.min(MAX_BACKOFF_MS);
        let next_retry_at = now_ms + capped_backoff_ms;

        // Schedule a new refresh task
        let new_task_id = Uuid::new_v4().to_string();
        let refresh_task = Task::RefreshFloatingLimit {
            task_id: new_task_id.clone(),
            tenant: tenant.to_string(),
            queue_key: queue_key.clone(),
            current_max_concurrency,
            last_refreshed_at_ms,
            metadata,
        };

        let task_value = encode_task(&refresh_task)?;
        let task_key_str = format!(
            "tasks/{:020}/{:02}/floating_refresh/{}/{}",
            next_retry_at,
            0, // highest priority
            queue_key,
            new_task_id
        );

        // Construct new state directly - avoids intermediate owned allocation
        let new_state = FloatingLimitState {
            retry_count: new_retry_count,
            next_retry_at_ms: Some(next_retry_at),
            refresh_task_scheduled: true,
            // Preserve unchanged fields from archived view
            current_max_concurrency: archived.current_max_concurrency,
            last_refreshed_at_ms: archived.last_refreshed_at_ms,
            refresh_interval_ms: archived.refresh_interval_ms,
            default_max_concurrency: archived.default_max_concurrency,
            metadata: archived
                .metadata
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
                .collect(),
        };

        let mut batch = WriteBatch::new();
        let state_value = encode_floating_limit_state(&new_state)?;
        batch.put(state_key.as_bytes(), &state_value);
        batch.put(task_key_str.as_bytes(), &task_value);
        batch.delete(lease_key.as_bytes());

        self.db.write(batch).await?;
        self.db.flush().await?;

        tracing::warn!(
            queue_key = %queue_key,
            error_code = %error_code,
            error_message = %error_message,
            retry_count = new_retry_count,
            next_retry_at_ms = next_retry_at,
            "floating limit refresh failed, scheduled retry"
        );

        Ok(())
    }
}
