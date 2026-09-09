//! Floating concurrency limit operations.

use slatedb::WriteBatch;
use slatedb::config::{PutOptions, Ttl, WriteOptions};
use uuid::Uuid;

use crate::codec::{
    DecodedFloatingLimitState, decode_concurrency_action, decode_floating_limit_state,
    decode_lease, decode_task_validated, encode_floating_limit_state, encode_lease,
    encode_refresh_index_row,
};
use crate::job::{FloatingConcurrencyLimit, FloatingLimitState, JobView};
use crate::job_store_shard::helpers::{DbWriteBatcher, WriteBatcher, now_epoch_ms};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError, ScheduledRefreshes};
use crate::keys::{
    concurrency_request_prefix, end_bound, floating_limit_state_key, job_info_key, leased_task_key,
    parse_refresh_task_key, refresh_task_group_prefix, refresh_task_key, refresh_tasks_prefix,
};
use crate::task::{DEFAULT_LEASE_MS, LeaseRecord, LeasedRefreshTask, Task};

/// Row TTL on refresh index rows. Longer than any stale window, so it only
/// cleans up a row left under a task group no worker polls again; SlateDB
/// applies it at flush and compaction, so it is storage cleanup, not a
/// read-side guarantee.
pub const REFRESH_INDEX_ROW_TTL_MS: i64 = 24 * 60 * 60 * 1000;

/// Most refresh index rows one drain leases. The bound counts leased rows,
/// not scanned rows, so backed-off retries sorting earlier in the group's
/// range never hide a claimable row.
pub const REFRESH_DRAIN_MAX_LEASED: usize = 16;

/// `base_ms` doubled `stale_reset_count` times, capped at `max_ms`, with
/// the shift saturating instead of overflowing. A cap below the base is
/// treated as the base: the cap bounds growth, it never shrinks the window.
fn stale_window_ms(base_ms: i64, stale_reset_count: u32, max_ms: i64) -> i64 {
    let widened = if stale_reset_count >= i64::BITS - 1 {
        i64::MAX
    } else {
        base_ms.saturating_mul(1i64 << stale_reset_count)
    };
    widened.min(max_ms).max(base_ms)
}

impl JobStoreShard {
    /// Mark `task_group` as possibly holding a refresh index row. Called
    /// only after the put's batch is durable: a mark made before the row is
    /// visible could be cleared by a drain that scans the still-empty range,
    /// leaving the committed row unmarked. A drain that runs between the
    /// commit and the mark misses the row once; the next drain sees it.
    fn mark_refresh_pending(&self, task_group: &str) {
        let mut groups = self
            .refresh_pending_groups
            .lock()
            .expect("refresh pending groups lock");
        *groups.entry(task_group.to_string()).or_insert(0) += 1;
    }

    /// Mark every group in `task_groups` pending, after the batch that wrote
    /// their index rows is durable.
    pub(crate) fn mark_refresh_pending_groups<'a>(
        &self,
        task_groups: impl IntoIterator<Item = &'a str>,
    ) {
        for task_group in task_groups {
            self.mark_refresh_pending(task_group);
        }
    }

    /// Whether `task_group` may hold a refresh index row.
    fn refresh_group_pending(&self, task_group: &str) -> bool {
        self.refresh_pending_groups
            .lock()
            .expect("refresh pending groups lock")
            .contains_key(task_group)
    }

    /// Warm the pending-group gate from the durable refresh index at shard
    /// open. Every group with a row is marked; a drain drops a group once it
    /// finds the range empty.
    pub(crate) async fn warm_refresh_pending_groups(&self) -> Result<(), JobStoreShardError> {
        let start = refresh_tasks_prefix();
        let end = end_bound(&start);
        let mut iter = self
            .db
            .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options_uncached())
            .await?;
        while let Some(kv) = iter.next().await? {
            if let Some(parsed) = parse_refresh_task_key(&kv.key) {
                self.mark_refresh_pending(&parsed.task_group);
            }
        }
        Ok(())
    }

    /// Test-only: write a refresh index row for `task` (a
    /// `RefreshFloatingLimit`) directly, marking its group pending as the
    /// scheduler would. Lets tests seed more pending refreshes than live
    /// floating queues would produce.
    #[doc(hidden)]
    pub async fn put_refresh_index_row_for_test(
        &self,
        task: &Task,
        not_before_ms: Option<i64>,
    ) -> Result<(), JobStoreShardError> {
        let Task::RefreshFloatingLimit {
            tenant,
            queue_key,
            task_group,
            ..
        } = task
        else {
            return Err(JobStoreShardError::InvalidArgument(
                "refresh index rows hold RefreshFloatingLimit tasks".to_string(),
            ));
        };
        let mut batch = WriteBatch::new();
        batch.put_with_options(
            refresh_task_key(task_group, tenant, queue_key),
            encode_refresh_index_row(task, not_before_ms),
            &PutOptions {
                ttl: Ttl::ExpireAt(now_epoch_ms() + REFRESH_INDEX_ROW_TTL_MS),
            },
        );
        self.db.write(batch).await?;
        self.mark_refresh_pending(task_group);
        Ok(())
    }

    /// Lease every claimable refresh index row under `task_group`, up to
    /// `REFRESH_DRAIN_MAX_LEASED`, in one durable batch. Rows for tenants
    /// outside the shard range and unreadable rows are deleted; rows whose
    /// `not_before_ms` is in the future stay for a later drain. Index keys
    /// never reach the task broker, so nothing here is acked or tombstoned.
    /// Drains on one shard are serialized from scan to commit, so two
    /// workers polling the same group cannot both lease one row; the
    /// pending-group check runs first so idle groups never wait on the lock.
    pub async fn drain_pending_refreshes(
        &self,
        worker_id: &str,
        task_group: &str,
    ) -> Result<Vec<LeasedRefreshTask>, JobStoreShardError> {
        if !self.refresh_group_pending(task_group) {
            return Ok(Vec::new());
        }
        let _drain = self.refresh_drain_lock.lock().await;
        // Re-read under the lock: the drain that just released it may have
        // emptied the range and cleared the group.
        let generation = {
            let groups = self
                .refresh_pending_groups
                .lock()
                .expect("refresh pending groups lock");
            groups.get(task_group).copied()
        };
        let Some(generation) = generation else {
            return Ok(Vec::new());
        };

        let now_ms = now_epoch_ms();
        let expiry_ms = now_ms + DEFAULT_LEASE_MS;
        let shard_range = self.get_range();
        let start = refresh_task_group_prefix(task_group);
        let end = end_bound(&start);
        let mut iter = self
            .db
            .scan_with_options::<Vec<u8>, _>(start..end, &crate::scan_options())
            .await?;

        let mut batch = WriteBatch::new();
        let mut leased = Vec::new();
        let mut range_empty = true;
        while let Some(kv) = iter.next().await? {
            range_empty = false;
            let Some(parsed) = parse_refresh_task_key(&kv.key) else {
                continue;
            };
            if !shard_range.contains_tenant(&parsed.tenant) {
                tracing::debug!(
                    tenant = %parsed.tenant,
                    queue_key = %parsed.queue_key,
                    range = %shard_range,
                    "dropping defunct refresh index row (tenant outside shard range)"
                );
                batch.delete(&kv.key);
                continue;
            }
            let decoded = match decode_task_validated(kv.value) {
                Ok(decoded) => decoded,
                Err(e) => {
                    // The state row's stale window reschedules a readable
                    // replacement; keeping the row would pin the group
                    // pending until its TTL.
                    tracing::warn!(
                        tenant = %parsed.tenant,
                        queue_key = %parsed.queue_key,
                        error = %e,
                        "refresh index row is unreadable; dropping it"
                    );
                    batch.delete(&kv.key);
                    continue;
                }
            };
            if decoded.not_before_ms().is_some_and(|t| now_ms < t) {
                continue;
            }
            if leased.len() >= REFRESH_DRAIN_MAX_LEASED {
                break;
            }
            let Some(rfl) = decoded.as_refresh_floating_limit() else {
                continue;
            };
            let task_id = rfl.task_id().unwrap_or_default().to_string();
            let record = LeaseRecord {
                worker_id: worker_id.to_string(),
                task: decoded.to_task()?,
                expiry_ms,
                started_at_ms: 0,
            };
            batch.put(leased_task_key(&task_id), encode_lease(&record));
            batch.delete(&kv.key);
            leased.push(LeasedRefreshTask {
                task_id,
                tenant_id: parsed.tenant,
                queue_key: parsed.queue_key,
                current_max_concurrency: rfl.current_max_concurrency(),
                last_refreshed_at_ms: rfl.last_refreshed_at_ms(),
                metadata: crate::codec::fb_kv_pairs_to_owned(rfl.metadata()),
                task_group: rfl.task_group().unwrap_or_default().to_string(),
            });
        }

        if range_empty {
            // A put committed and marked since the scan began bumped the
            // generation and keeps the group marked. This relies on marks
            // being made only after their row is durable (see
            // `mark_refresh_pending`).
            let mut groups = self
                .refresh_pending_groups
                .lock()
                .expect("refresh pending groups lock");
            if groups.get(task_group) == Some(&generation) {
                groups.remove(task_group);
            }
        }

        if !batch.is_empty() {
            self.db
                .write_with_options(
                    batch,
                    &WriteOptions {
                        await_durable: true,
                        ..Default::default()
                    },
                )
                .await?;
        }
        Ok(leased)
    }

    /// The stale window for a state row: the shard's base window doubled per
    /// consecutive stale reset the row records, capped at the shard's max.
    /// Shifts saturate, so a large count reads as the cap.
    fn floating_limit_stale_window_ms(&self, state: &DecodedFloatingLimitState) -> i64 {
        stale_window_ms(
            self.floating_refresh_stale_ms,
            state.stale_reset_count(),
            self.floating_refresh_stale_max_ms,
        )
    }

    /// True when the state's outstanding-refresh flag is set but the refresh
    /// was scheduled longer ago than the row's stale window, or carries no
    /// stamp at all. Such a refresh is treated as lost: no index row, lease,
    /// or in-memory suppression may pin a tenant's cap forever.
    fn floating_limit_refresh_stale(&self, state: &DecodedFloatingLimitState, now_ms: i64) -> bool {
        state.refresh_task_scheduled()
            && state
                .refresh_scheduled_at_ms()
                .is_none_or(|at| now_ms - at > self.floating_limit_stale_window_ms(state))
    }

    /// True when a refresh task is scheduled and still trusted to complete.
    fn floating_limit_refresh_outstanding(
        &self,
        state: &DecodedFloatingLimitState,
        now_ms: i64,
    ) -> bool {
        state.refresh_task_scheduled() && !self.floating_limit_refresh_stale(state, now_ms)
    }

    pub(crate) fn floating_limit_refresh_ready(
        &self,
        state: &DecodedFloatingLimitState,
        now_ms: i64,
    ) -> bool {
        if self.floating_limit_refresh_outstanding(state, now_ms) {
            return false;
        }

        let next_refresh_due = state.last_refreshed_at_ms() + state.refresh_interval_ms();
        if now_ms < next_refresh_due {
            return false;
        }

        let in_backoff = state
            .next_retry_at_ms()
            .map(|t| now_ms < t)
            .unwrap_or(false);

        !in_backoff
    }

    pub(crate) async fn has_waiting_concurrency_requests(
        &self,
        tenant: &str,
        queue_key: &str,
    ) -> Result<bool, JobStoreShardError> {
        let start = concurrency_request_prefix(tenant, queue_key);
        let end = end_bound(&start);
        let mut iter = self.db.scan::<Vec<u8>, _>(start..end).await?;
        Ok(iter.next().await?.is_some())
    }

    /// Task group of the head waiting request on `(tenant, queue_key)`, the
    /// group a scanner-originated refresh task is written under. A stored
    /// request record may carry an empty group; the job's info row supplies
    /// it then, as the scanner's own resume path does. `None` when there is
    /// no waiter, the record or job info is unreadable, or the group is still
    /// empty: a task under an empty group lands in a range no broker serves.
    pub(crate) async fn peek_head_waiting_request_task_group(
        &self,
        tenant: &str,
        queue_key: &str,
    ) -> Result<Option<String>, JobStoreShardError> {
        let start = concurrency_request_prefix(tenant, queue_key);
        let end = end_bound(&start);
        let mut iter = self.db.scan::<Vec<u8>, _>(start..end).await?;
        let Some(kv) = iter.next().await? else {
            return Ok(None);
        };
        let decoded = match decode_concurrency_action(kv.value) {
            Ok(decoded) => decoded,
            Err(e) => {
                tracing::warn!(
                    tenant = %tenant,
                    queue_key = %queue_key,
                    error = %e,
                    "head waiting request is unreadable; not scheduling a floating limit refresh"
                );
                return Ok(None);
            }
        };
        let Some(request) = decoded.fb().variant_as_enqueue_task() else {
            return Ok(None);
        };
        let task_group = request.task_group().unwrap_or_default();
        if !task_group.is_empty() {
            return Ok(Some(task_group.to_string()));
        }
        let job_id = request.job_id().unwrap_or_default();
        let Some(raw) = self.db.get(&job_info_key(tenant, job_id)).await? else {
            return Ok(None);
        };
        let job = match JobView::new(raw) {
            Ok(job) => job,
            Err(e) => {
                tracing::warn!(
                    tenant = %tenant,
                    queue_key = %queue_key,
                    job_id = %job_id,
                    error = %e,
                    "head waiter's job info is unreadable; not scheduling a floating limit refresh"
                );
                return Ok(None);
            }
        };
        Ok(Some(job.task_group().to_string()).filter(|g| !g.is_empty()))
    }

    /// Schedule a refresh for a floating queue the grant scanner found at
    /// capacity with pending grant demand. Readiness is judged here with the
    /// shard's stale threshold; the task group comes from the head waiter,
    /// whose presence is what establishes the backlog. The index row and
    /// state are committed in their own batch; the next dequeue for the
    /// group drains the row. Returns whether a row was written.
    pub(crate) async fn schedule_floating_refresh_from_scanner(
        &self,
        tenant: &str,
        queue_key: &str,
        state: &DecodedFloatingLimitState,
    ) -> Result<bool, JobStoreShardError> {
        let now_ms = now_epoch_ms();
        if !self.floating_limit_refresh_ready(state, now_ms) {
            return Ok(false);
        }
        // The scanner's row is a snapshot from its precheck. A refresh outcome
        // committed since then must not be overwritten from that snapshot, so
        // readiness and the write use a fresh read of the row.
        let state_key = floating_limit_state_key(tenant, queue_key);
        let Some(raw) = self.db.get(&state_key).await? else {
            return Ok(false);
        };
        let state = &decode_floating_limit_state(raw)?;
        if !self.floating_limit_refresh_ready(state, now_ms) {
            return Ok(false);
        }
        let Some(task_group) = self
            .peek_head_waiting_request_task_group(tenant, queue_key)
            .await?
        else {
            return Ok(false);
        };
        let mut batch = WriteBatch::new();
        let mut writer = DbWriteBatcher::new(&self.db, &mut batch);
        let written = self.maybe_schedule_floating_limit_refresh(
            &mut writer,
            tenant,
            queue_key,
            state,
            now_ms,
            &task_group,
            true,
        )?;
        if !written {
            return Ok(false);
        }
        self.db.write(batch).await?;
        self.mark_refresh_pending(&task_group);
        Ok(true)
    }

    /// Schedule a refresh for a RequestTicket that just landed as a waiter on
    /// a floating queue. The ticket's own request row sits in the uncommitted
    /// batch, invisible to the durable waiter probe, so the ticket supplies
    /// the waiter signal directly. `scheduled_refreshes` is the batch's
    /// record: a queue already scheduled in this batch is skipped, and a
    /// written row is recorded for the batch owner to mark once durable.
    /// Returns whether a row was written.
    pub(crate) async fn schedule_floating_refresh_for_ticket<W: WriteBatcher>(
        &self,
        writer: &mut W,
        scheduled_refreshes: &mut ScheduledRefreshes,
        tenant: &str,
        fl: &FloatingConcurrencyLimit,
        now_ms: i64,
        task_group: &str,
    ) -> Result<bool, JobStoreShardError> {
        if scheduled_refreshes.contains(tenant, &fl.key) {
            return Ok(false);
        }
        let state = self
            .get_or_create_floating_limit_state(writer, tenant, fl)
            .await?;
        let written = self.maybe_schedule_floating_limit_refresh(
            writer, tenant, &fl.key, &state, now_ms, task_group, true,
        )?;
        if written {
            scheduled_refreshes.record(tenant, &fl.key, task_group);
        }
        Ok(written)
    }

    /// Get or create the floating limit state for a given queue key.
    /// Returns a zero-copy decoded view. For the rare "just created" case,
    /// we encode then decode to return the same type (extra decode is fine for cold path).
    pub(crate) async fn get_or_create_floating_limit_state<W: WriteBatcher>(
        &self,
        writer: &mut W,
        tenant: &str,
        fl: &FloatingConcurrencyLimit,
    ) -> Result<DecodedFloatingLimitState, JobStoreShardError> {
        let state_key = floating_limit_state_key(tenant, &fl.key);

        if let Some(raw) = writer.get(&state_key).await? {
            // Hot path: state exists, return zero-copy decoded view
            return Ok(decode_floating_limit_state(raw)?);
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
            refresh_scheduled_at_ms: None,
            stale_reset_count: 0,
        };

        let state_bytes = encode_floating_limit_state(&state);
        writer.put(&state_key, &state_bytes)?;

        // Decode what we just encoded so we return the same type
        Ok(decode_floating_limit_state(state_bytes)?)
    }

    /// Check if a floating limit refresh is needed and schedule it if so.
    /// Returns whether a refresh index row was written.
    ///
    /// Three call sites lazily trigger refreshes, each supplying its own
    /// waiter signal and task group: the enqueue path (a job that just
    /// became a waiter, or a durable-waiter probe), the RequestTicket
    /// handler at dequeue (a scheduled-start ticket that just landed as a
    /// waiter), and the grant scanner's at-capacity precheck via
    /// `schedule_floating_refresh_from_scanner` (the head waiter's group).
    ///
    /// The row is keyed by `(task_group, tenant, queue_key)`, so writing
    /// over a stale flag replaces any unclaimed row for the queue in place.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn maybe_schedule_floating_limit_refresh<W: WriteBatcher>(
        &self,
        writer: &mut W,
        tenant: &str,
        queue_key: &str,
        state: &DecodedFloatingLimitState,
        now_ms: i64,
        task_group: &str,
        has_waiters: bool,
    ) -> Result<bool, JobStoreShardError> {
        if self.floating_limit_refresh_outstanding(state, now_ms) {
            return Ok(false);
        }

        // Check if we need to refresh based on interval
        let next_refresh_due = state.last_refreshed_at_ms() + state.refresh_interval_ms();
        let should_refresh = now_ms >= next_refresh_due;

        // Also check if we're in backoff from a failed refresh
        let in_backoff = state
            .next_retry_at_ms()
            .map(|t| now_ms < t)
            .unwrap_or(false);

        if !should_refresh || in_backoff || !has_waiters {
            return Ok(false);
        }

        // Reaching here with the flag still set means the outstanding refresh
        // aged out; this write replaces it and widens the next stale window.
        // A first schedule after a clean outcome keeps the count the outcome
        // wrote, so a healthy refresh cycle never widens the window.
        let mut stale_reset_count = state.stale_reset_count();
        if state.refresh_task_scheduled() {
            let age_ms = state.refresh_scheduled_at_ms().map(|at| now_ms - at);
            stale_reset_count = stale_reset_count.saturating_add(1);
            let next_stale_window_ms = stale_window_ms(
                self.floating_refresh_stale_ms,
                stale_reset_count,
                self.floating_refresh_stale_max_ms,
            );
            tracing::warn!(
                tenant = %tenant,
                queue_key = %queue_key,
                age_ms = ?age_ms,
                stale_resets = stale_reset_count,
                next_stale_window_ms,
                "floating limit refresh flag is stale; scheduling a replacement refresh"
            );
            if let Some(m) = &self.metrics {
                m.record_floating_limit_refresh_reset(&self.name, "stale_scheduled");
            }
        }

        // Schedule a refresh task
        let task_id = Uuid::new_v4().to_string();
        let refresh_task = Task::RefreshFloatingLimit {
            task_id: task_id.clone(),
            tenant: tenant.to_string(),
            queue_key: queue_key.to_string(),
            current_max_concurrency: state.current_max_concurrency(),
            last_refreshed_at_ms: state.last_refreshed_at_ms(),
            metadata: state.metadata(),
            task_group: task_group.to_string(),
        };

        writer.put_with_expire(
            refresh_task_key(task_group, tenant, queue_key),
            encode_refresh_index_row(&refresh_task, None),
            now_ms + REFRESH_INDEX_ROW_TTL_MS,
        )?;

        // Update state to mark refresh as scheduled
        let new_state = FloatingLimitState {
            refresh_task_scheduled: true,
            refresh_scheduled_at_ms: Some(now_ms),
            stale_reset_count,
            ..state.to_owned()
        };
        let state_key = floating_limit_state_key(tenant, queue_key);
        let state_value = encode_floating_limit_state(&new_state);
        writer.put(&state_key, &state_value)?;

        tracing::debug!(
            queue_key = %queue_key,
            current_max = state.current_max_concurrency(),
            last_refreshed = state.last_refreshed_at_ms(),
            "scheduled floating limit refresh task"
        );

        Ok(true)
    }

    /// Report a successful floating limit refresh from a worker.
    /// Updates the floating limit state with the new max concurrency value.
    pub async fn report_refresh_success(
        &self,
        task_id: &str,
        new_max_concurrency: u32,
    ) -> Result<(), JobStoreShardError> {
        // Load the lease to get the queue key and tenant
        let lease_key = leased_task_key(task_id);
        let maybe_raw = self.db.get(&lease_key).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(JobStoreShardError::LeaseNotFound(task_id.to_string()));
        };

        let decoded = decode_lease(value_bytes)?;
        let tenant = decoded.tenant();
        let (_, queue_key) = decoded.refresh_floating_limit_info().ok_or_else(|| {
            JobStoreShardError::Codec("task is not a RefreshFloatingLimit".to_string())
        })?;

        let now_ms = now_epoch_ms();
        let state_key = floating_limit_state_key(tenant, queue_key);

        // Load state and construct new state with updates
        let maybe_state = self.db.get(&state_key).await?;
        let Some(raw) = maybe_state else {
            return Err(JobStoreShardError::Codec(format!(
                "floating limit state not found for queue {}",
                queue_key
            )));
        };
        let decoded_state = decode_floating_limit_state(raw)?;
        let old_max_concurrency = decoded_state.current_max_concurrency();

        let new_state = FloatingLimitState {
            current_max_concurrency: new_max_concurrency,
            last_refreshed_at_ms: now_ms,
            refresh_task_scheduled: false,
            refresh_scheduled_at_ms: None,
            retry_count: 0,
            next_retry_at_ms: None,
            stale_reset_count: 0,
            ..decoded_state.to_owned()
        };

        let mut batch = WriteBatch::new();
        let state_value = encode_floating_limit_state(&new_state);
        batch.put(&state_key, &state_value);
        batch.delete(&lease_key);

        self.db.write(batch).await?;

        // Update the in-memory limit cache
        self.concurrency.cache_queue_limit(
            tenant,
            queue_key,
            new_max_concurrency,
            crate::concurrency::ConcurrencyLimitType::Floating,
        );

        // Nudge the grant scanner by the capacity delta so requests waiting on
        // a raised floating cap (including the 0->N bootstrap) are granted
        // immediately rather than waiting for the periodic reconcile scan.
        // `process_grants` re-validates real capacity, so passing the delta is a
        // safe upper bound on how many pending requests to consider.
        let delta = new_max_concurrency.saturating_sub(old_max_concurrency);
        if delta > 0 {
            self.concurrency
                .request_grant_count(tenant, queue_key, delta);
        }

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
        task_id: &str,
        error_code: &str,
        error_message: &str,
    ) -> Result<(), JobStoreShardError> {
        // Load the lease to get the task details and tenant
        let lease_key = leased_task_key(task_id);
        let maybe_raw = self.db.get(&lease_key).await?;
        let Some(value_bytes) = maybe_raw else {
            return Err(JobStoreShardError::LeaseNotFound(task_id.to_string()));
        };

        let decoded = decode_lease(value_bytes)?;
        let tenant = decoded.tenant().to_string();
        let fb_lr = decoded.fb();
        let rfl = fb_lr.task_as_refresh_floating_limit().ok_or_else(|| {
            JobStoreShardError::Codec("task is not a RefreshFloatingLimit".to_string())
        })?;
        let queue_key = rfl.queue_key().unwrap_or_default().to_string();
        let current_max_concurrency = rfl.current_max_concurrency();
        let last_refreshed_at_ms = rfl.last_refreshed_at_ms();
        let metadata = crate::codec::fb_kv_pairs_to_owned(rfl.metadata());
        let task_group = rfl.task_group().unwrap_or_default().to_string();

        let now_ms = now_epoch_ms();
        let state_key = floating_limit_state_key(&tenant, &queue_key);

        // Load state
        let maybe_state = self.db.get(&state_key).await?;
        let Some(raw) = maybe_state else {
            return Err(JobStoreShardError::Codec(format!(
                "floating limit state not found for queue {}",
                queue_key
            )));
        };
        let decoded_state = decode_floating_limit_state(raw)?;

        // Calculate exponential backoff
        const INITIAL_BACKOFF_MS: i64 = 1000; // 1 second
        const MAX_BACKOFF_MS: i64 = 60_000; // 1 minute
        const BACKOFF_MULTIPLIER: f64 = 2.0;

        let new_retry_count = decoded_state.retry_count() + 1;
        let backoff_ms = ((INITIAL_BACKOFF_MS as f64)
            * BACKOFF_MULTIPLIER.powi(decoded_state.retry_count() as i32))
        .round() as i64;
        let capped_backoff_ms = backoff_ms.min(MAX_BACKOFF_MS);
        let next_retry_at = now_ms + capped_backoff_ms;

        let has_waiters = self
            .has_waiting_concurrency_requests(&tenant, &queue_key)
            .await?;

        // The retry row is not claimable before `next_retry_at`, which can
        // sit up to the backoff cap in the future; stamping that (not now)
        // keeps the retry from reading as stale before it is even claimable.
        let new_state = FloatingLimitState {
            retry_count: new_retry_count,
            next_retry_at_ms: Some(next_retry_at),
            refresh_task_scheduled: has_waiters,
            refresh_scheduled_at_ms: has_waiters.then_some(next_retry_at),
            stale_reset_count: 0,
            ..decoded_state.to_owned()
        };

        let mut batch = WriteBatch::new();
        let state_value = encode_floating_limit_state(&new_state);
        batch.put(&state_key, &state_value);
        if has_waiters {
            let refresh_task = Task::RefreshFloatingLimit {
                task_id: Uuid::new_v4().to_string(),
                tenant: tenant.clone(),
                queue_key: queue_key.clone(),
                current_max_concurrency,
                last_refreshed_at_ms,
                metadata,
                task_group: task_group.clone(),
            };
            batch.put_with_options(
                refresh_task_key(&task_group, &tenant, &queue_key),
                encode_refresh_index_row(&refresh_task, Some(next_retry_at)),
                &PutOptions {
                    ttl: Ttl::ExpireAt(now_ms + REFRESH_INDEX_ROW_TTL_MS),
                },
            );
        }
        batch.delete(&lease_key);

        self.db.write(batch).await?;

        if has_waiters {
            self.mark_refresh_pending(&task_group);
            tracing::warn!(
                queue_key = %queue_key,
                error_code = %error_code,
                error_message = %error_message,
                retry_count = new_retry_count,
                next_retry_at_ms = next_retry_at,
                "floating limit refresh failed, scheduled retry"
            );
        } else {
            tracing::warn!(
                queue_key = %queue_key,
                error_code = %error_code,
                error_message = %error_message,
                retry_count = new_retry_count,
                next_retry_at_ms = next_retry_at,
                "floating limit refresh failed, no waiters; skipping retry"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::stale_window_ms;

    #[test]
    fn stale_window_doubles_per_reset_up_to_the_cap() {
        let cases = [
            (60_000, 0, 3_600_000, 60_000),
            (60_000, 1, 3_600_000, 120_000),
            (60_000, 5, 3_600_000, 1_920_000),
            (60_000, 6, 3_600_000, 3_600_000),
            (100, 2, 300, 300),
        ];
        for (base, count, max, want) in cases {
            assert_eq!(
                stale_window_ms(base, count, max),
                want,
                "stale_window_ms({base}, {count}, {max})"
            );
        }
    }

    #[test]
    fn stale_window_saturates_instead_of_overflowing() {
        assert_eq!(stale_window_ms(60_000, 62, i64::MAX), i64::MAX);
        assert_eq!(stale_window_ms(60_000, 63, i64::MAX), i64::MAX);
        assert_eq!(stale_window_ms(1, u32::MAX, i64::MAX), i64::MAX);
        assert_eq!(stale_window_ms(i64::MAX, 1, i64::MAX), i64::MAX);
    }

    #[test]
    fn stale_window_never_shrinks_below_the_base() {
        assert_eq!(stale_window_ms(60_000, 0, 1_000), 60_000);
        assert_eq!(stale_window_ms(60_000, 3, 1_000), 60_000);
        assert_eq!(stale_window_ms(0, 5, 3_600_000), 0);
    }
}
