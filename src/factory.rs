use dashmap::DashMap;
use futures::TryStreamExt;
use slatedb::object_store::path::Path as ObjectPath;
use slatedb::object_store::{ObjectStore, ObjectStoreExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::OnceCell;
use url::Url;

use crate::concurrency::GrantScannerConfig;
use crate::gubernator::RateLimitClient;
use crate::job_store_shard::{JobStoreShard, JobStoreShardError, OpenShardOptions};
use crate::metrics::Metrics;
use crate::settings::DatabaseTemplate;
use crate::shard_range::{ShardId, ShardRange};
use crate::storage::resolve_object_store;

/// A shard entry that supports atomic initialization and is either open or closing.
/// The OnceCell holds the shard once it is opened; the lifecycle mutex is what
/// lets only one caller open or close it at a time.
struct ShardEntry {
    cell: OnceCell<Arc<JobStoreShard>>,
    /// Set once a close has begun on this entry. A closing entry is never
    /// served again: it leaves the map when its close succeeds.
    closing: AtomicBool,
    /// Serializes open-initialization and close attempts for this shard id, so
    /// a close waits for an in-flight initialization and then closes the shard
    /// it produced, and concurrent closes run one after another.
    lifecycle: Arc<tokio::sync::Mutex<()>>,
}

impl ShardEntry {
    fn new() -> Self {
        Self {
            cell: OnceCell::new(),
            closing: AtomicBool::new(false),
            lifecycle: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    fn get(&self) -> Option<Arc<JobStoreShard>> {
        self.cell.get().cloned()
    }

    fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    fn mark_closing(&self) {
        self.closing.store(true, Ordering::Release);
    }

    /// The shard, when this entry is open. `None` while uninitialized or
    /// closing, and `None` for a shard closed directly on the shard object
    /// (the split cloning phase does this), whose entry is never marked.
    fn serving(&self) -> Option<Arc<JobStoreShard>> {
        if self.is_closing() {
            return None;
        }
        self.get().filter(|shard| !shard.is_closing())
    }

    async fn get_or_try_init<F, Fut>(&self, f: F) -> Result<Arc<JobStoreShard>, JobStoreShardError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Arc<JobStoreShard>, JobStoreShardError>>,
    {
        self.cell.get_or_try_init(f).await.map(Arc::clone)
    }
}

/// Factory for opening and holding `Shard` instances by ShardId.
///
/// Uses interior mutability (DashMap) so it can be shared across tasks
/// and shards can be opened/closed dynamically as ownership changes.
/// A per-shard lifecycle mutex serializes opens and closes of the same shard
/// while different shards proceed in parallel.
/// Default timeout for shard close operations (30 seconds).
/// SlateDB's internal retrying_object_store retries indefinitely on transient errors,
/// so we need a timeout to prevent close from hanging forever if the object store is
/// unreachable or the filesystem is read-only.
const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default timeout for reopening a shard after its close completed (30 seconds).
/// `open` has no timeout of its own and SlateDB retries indefinitely, so callers
/// that must not block on an unreachable object store bound the open with this.
const DEFAULT_REOPEN_TIMEOUT: Duration = Duration::from_secs(30);

pub struct ShardFactory {
    instances: DashMap<ShardId, Arc<ShardEntry>>,
    template: DatabaseTemplate,
    rate_limiter: Arc<dyn RateLimitClient>,
    metrics: Option<Metrics>,
    close_timeout: Duration,
    reopen_timeout: Duration,
    /// Test-only clone fault: when armed, the next `clone_closed_shard` skips
    /// cloning this child so it comes up empty. Debug builds only; inert
    /// unless a test arms it.
    #[cfg(debug_assertions)]
    clone_skip_child: std::sync::Mutex<Option<ShardId>>,
    /// Test-only initialization fault: when armed, the pre-commit cleanup
    /// metadata initialization for this child fails. Debug builds only;
    /// inert unless a test arms it.
    #[cfg(debug_assertions)]
    init_fail_child: std::sync::Mutex<Option<ShardId>>,
    /// Test-only close fault: the number of upcoming `close` attempts that
    /// fail for each shard. Debug builds only; inert unless a test arms it.
    #[cfg(debug_assertions)]
    close_failures: std::sync::Mutex<HashMap<ShardId, u32>>,
}

impl ShardFactory {
    pub fn new(
        template: DatabaseTemplate,
        rate_limiter: Arc<dyn RateLimitClient>,
        metrics: Option<Metrics>,
    ) -> Self {
        Self {
            instances: DashMap::new(),
            template,
            rate_limiter,
            metrics,
            close_timeout: DEFAULT_CLOSE_TIMEOUT,
            reopen_timeout: DEFAULT_REOPEN_TIMEOUT,
            #[cfg(debug_assertions)]
            clone_skip_child: std::sync::Mutex::new(None),
            #[cfg(debug_assertions)]
            init_fail_child: std::sync::Mutex::new(None),
            #[cfg(debug_assertions)]
            close_failures: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Create a no-op factory for testing splitter logic without real shards.
    ///
    /// This factory cannot actually open shards; it's only useful for tests that
    /// need a factory reference but don't call `open()` or `clone_closed_shard()`.
    #[doc(hidden)]
    pub fn new_noop() -> Self {
        use crate::gubernator::NullGubernatorClient;

        Self {
            instances: DashMap::new(),
            template: DatabaseTemplate {
                // The root is unique per factory so the shared Memory store
                // (the default backend) does not bleed across noop factories.
                path: format!("noop-{}/%shard%", ShardId::new()),
                apply_wal_on_close: false,
                ..Default::default()
            },
            rate_limiter: NullGubernatorClient::new(),
            metrics: None,
            close_timeout: DEFAULT_CLOSE_TIMEOUT,
            reopen_timeout: DEFAULT_REOPEN_TIMEOUT,
            #[cfg(debug_assertions)]
            clone_skip_child: std::sync::Mutex::new(None),
            #[cfg(debug_assertions)]
            init_fail_child: std::sync::Mutex::new(None),
            #[cfg(debug_assertions)]
            close_failures: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Arm the test-only clone fault: the next `clone_closed_shard` call skips
    /// cloning `child_id`, so that child opens empty. Lets tests drive the
    /// pre-commit verification abort path, which a correct clone cannot reach
    /// through the public interface. `pub` because integration tests compile
    /// as a separate crate.
    #[doc(hidden)]
    #[cfg(debug_assertions)]
    pub fn inject_empty_child_clone(&self, child_id: ShardId) {
        *self
            .clone_skip_child
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(child_id);
    }

    /// Disarm and return the injected clone fault, if any. Always `None` in
    /// release builds.
    fn take_injected_empty_child(&self) -> Option<ShardId> {
        #[cfg(debug_assertions)]
        {
            self.clone_skip_child
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
        }
        #[cfg(not(debug_assertions))]
        {
            None
        }
    }

    /// Arm the test-only initialization fault: the pre-commit cleanup
    /// metadata initialization for `child_id` fails, letting tests drive the
    /// init-failure abort path, which a healthy shard cannot reach through
    /// the public interface. `pub` because integration tests compile as a
    /// separate crate.
    #[doc(hidden)]
    #[cfg(debug_assertions)]
    pub fn inject_child_init_failure(&self, child_id: ShardId) {
        *self
            .init_fail_child
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(child_id);
    }

    /// Disarm and report whether the initialization fault was armed for this
    /// child. Always `false` in release builds.
    fn take_injected_init_failure(&self, child_id: &ShardId) -> bool {
        #[cfg(debug_assertions)]
        {
            let mut armed = self
                .init_fail_child
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if armed.as_ref() == Some(child_id) {
                armed.take();
                return true;
            }
            false
        }
        #[cfg(not(debug_assertions))]
        {
            let _ = child_id;
            false
        }
    }

    /// Arm the test-only close fault: the next `attempts` close attempts on
    /// `shard_id` fail after the entry is marked closing, without touching
    /// the shard. Lets tests drive consecutive close failures and close
    /// failures on an already-closed database, neither of which a storage
    /// fault can produce. `pub` because integration tests compile as a
    /// separate crate.
    #[doc(hidden)]
    #[cfg(debug_assertions)]
    pub fn inject_close_failures(&self, shard_id: ShardId, attempts: u32) {
        let mut armed = self
            .close_failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if attempts == 0 {
            armed.remove(&shard_id);
        } else {
            armed.insert(shard_id, attempts);
        }
    }

    /// Consume one injected close failure for this shard, if any is armed.
    /// Always `false` in release builds.
    fn take_injected_close_failure(&self, shard_id: &ShardId) -> bool {
        #[cfg(debug_assertions)]
        {
            let mut armed = self
                .close_failures
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(remaining) = armed.get_mut(shard_id) else {
                return false;
            };
            *remaining = remaining.saturating_sub(1);
            if *remaining == 0 {
                armed.remove(shard_id);
            }
            true
        }
        #[cfg(not(debug_assertions))]
        {
            let _ = shard_id;
            false
        }
    }

    /// Set the timeout for shard close operations.
    /// SlateDB retries indefinitely on transient errors, so this timeout prevents
    /// close from hanging forever. Useful for tests that inject storage failures.
    pub fn set_close_timeout(&mut self, timeout: Duration) {
        self.close_timeout = timeout;
    }

    /// Set the timeout callers apply when reopening a shard whose close completed.
    pub fn set_reopen_timeout(&mut self, timeout: Duration) {
        self.reopen_timeout = timeout;
    }

    /// The timeout callers apply when reopening a shard whose close completed.
    pub fn reopen_timeout(&self) -> Duration {
        self.reopen_timeout
    }

    /// Get an open shard by its ID. A shard whose close has begun is never returned.
    pub fn get(&self, shard_id: &ShardId) -> Option<Arc<JobStoreShard>> {
        self.instances
            .get(shard_id)
            .and_then(|entry| entry.serving())
    }

    /// Open a shard using the shared database template.
    ///
    /// The shard's UUID is used to construct the storage path. The `range` parameter
    /// specifies the tenant keyspace this shard is responsible for - this is immutable
    /// after opening.
    ///
    /// Initialization is atomic per shard: if two callers try to open the same shard
    /// concurrently, only one will actually open the database and the other will wait
    /// and receive the same instance.
    ///
    /// Returns `ClosePending` when a close has begun on this shard and has not completed.
    ///
    /// **Note on path resolution:**
    /// For `Backend::Fs`, we resolve the object store at the storage root level (not
    /// the shard-specific path) and pass the shard name to DbBuilder. This is required
    /// for cloned databases to work correctly - they store relative paths to parent SST
    /// files that must resolve correctly from the storage root.
    pub async fn open(
        &self,
        shard_id: &ShardId,
        range: &ShardRange,
    ) -> Result<Arc<JobStoreShard>, JobStoreShardError> {
        let shard_id = *shard_id;

        // Wall-clock timer for this open call. Compared against the per-init
        // timer below to expose time spent waiting on a concurrent opener or
        // closer (lifecycle lock contention) vs. time spent actually opening.
        let call_started = std::time::Instant::now();

        // Get or create the entry for this shard, then take its lifecycle lock so
        // only one caller opens the database and no close runs meanwhile. The
        // closing check runs under the map's per-key lock, the same lock `close` marks
        // under, so a second database handle is never opened on a shard path
        // whose close is pending.
        let (entry, _lifecycle) = loop {
            let entry = {
                let entry = self
                    .instances
                    .entry(shard_id)
                    .or_insert_with(|| Arc::new(ShardEntry::new()));
                if entry.is_closing() {
                    return Err(JobStoreShardError::ClosePending(shard_id));
                }
                Arc::clone(entry.value())
            };
            let lifecycle = Arc::clone(&entry.lifecycle).lock_owned().await;
            // A close that ran while this call waited for the lifecycle lock
            // either removed the entry (start over on a fresh one) or left it
            // closing (refused by the check above on the next pass).
            if self.is_current_entry(&shard_id, &entry) && !entry.is_closing() {
                break (entry, lifecycle);
            }
        };

        // A caller that waited for the lifecycle lock behind another opener finds
        // the cell filled and gets that instance.
        let name = shard_id.to_string();
        let range = range.clone();
        let template = &self.template;
        let rate_limiter = Arc::clone(&self.rate_limiter);
        let metrics = self.metrics.clone();

        let result = entry
            .get_or_try_init(|| async {
                // Timer for the actual open work (only the caller that finds the
                // cell empty runs this closure).
                let init_started = std::time::Instant::now();
                // For Backend::Fs, we need to open at the storage root level so that
                // cloned databases can correctly resolve their relative parent SST paths.
                // Extract the root from the template and use shard name as the db path.
                let (resolved, db_path) =
                    Self::resolve_at_root(&template.backend, &template.path, &name)?;

                // Configure separate WAL object store if specified
                let (wal_store, wal_close_config) = if let Some(wal_template) = &template.wal {
                    let wal_path = wal_template
                        .path
                        .replace("%shard%", &name)
                        .replace("{shard}", &name);
                    let wal_resolved = resolve_object_store(&wal_template.backend, &wal_path)?;

                    // Only set up WAL cleanup for local (Fs) storage backends
                    let close_config = if wal_template.is_local_storage() {
                        Some(crate::job_store_shard::WalCloseConfig {
                            path: wal_resolved.root_path,
                            flush_on_close: template.apply_wal_on_close,
                        })
                    } else {
                        None
                    };
                    (Some(wal_resolved.store), close_config)
                } else {
                    (None, None)
                };

                let shard_arc = JobStoreShard::open_with_resolved_store(
                    name.clone(),
                    &db_path,
                    OpenShardOptions {
                        store: resolved.store,
                        wal_store,
                        wal_close_config,
                        slatedb_settings: template.slatedb.clone(),
                        memory_cache: template.memory_cache.clone(),
                        rate_limiter,
                        metrics,
                        concurrency_reconcile_interval: Duration::from_millis(
                            template.concurrency_reconcile_interval_ms.max(1),
                        ),
                        counter_reconciliation_seconds: template.counter_reconciliation_seconds,
                        hydrate_all_at_startup: template.hydrate_all_at_startup,
                        grant_scanner: GrantScannerConfig {
                            batch_size: template.grant_scanner_batch_size,
                            buffer_size: template.grant_scanner_buffer_size,
                            concurrency: template.grant_scanner_concurrency,
                            cold_batch_size: template.grant_scanner_cold_batch_size,
                            next_hop_skip_min_backlog: template
                                .grant_scanner_next_hop_skip_min_backlog,
                            live_headroom_fraction: template.grant_scanner_live_headroom_fraction,
                            commit_chunk_size: template.grant_scanner_commit_chunk_size,
                        },
                        concurrency_reconcile_scan_slice: template.concurrency_reconcile_scan_slice,
                        holder_drift_scan_slice: template.holder_drift_scan_slice,
                        completed_job_expire_s: template.completed_job_expire_s,
                        terminal_job_expire_s: template.terminal_job_expire_s,
                        count_from_status_counters: template.count_from_status_counters,
                        floating_refresh_stale_ms: template.floating_refresh_stale_ms,
                        floating_refresh_stale_max_ms: template.floating_refresh_stale_max_ms,
                        broker_tombstone_revive_after_generations: template
                            .broker_tombstone_revive_after_generations,
                    },
                    range.clone(),
                )
                .await?;

                tracing::info!(
                    shard_id = %shard_id,
                    range = %range,
                    init_ms = init_started.elapsed().as_millis() as u64,
                    "opened shard"
                );
                Ok(shard_arc)
            })
            .await;

        tracing::debug!(
            shard_id = %shard_id,
            total_ms = call_started.elapsed().as_millis() as u64,
            "factory: open returned"
        );

        result
    }

    /// Validate that the template path has the shard placeholder at a directory boundary.
    ///
    /// The placeholder (`%shard%` or `{shard}`) must be preceded by `/` (or be at the start)
    /// so that the shard ID forms a complete directory name, not a suffix of another name.
    /// This is required for clone/split operations and for consistent path handling.
    ///
    /// Returns the position of the placeholder if valid.
    fn validate_template_path(template_path: &str) -> Result<usize, JobStoreShardError> {
        let placeholder_pos = template_path
            .find("%shard%")
            .or_else(|| template_path.find("{shard}"));

        let pos = placeholder_pos.ok_or_else(|| {
            JobStoreShardError::Codec(format!(
                "database template path must contain a shard placeholder (%shard% or {{shard}}), got: {}",
                template_path
            ))
        })?;

        // Validate that the placeholder is at a path boundary (preceded by / or at start of path)
        if pos > 0 {
            let char_before = template_path.chars().nth(pos - 1);
            if char_before != Some('/') {
                return Err(JobStoreShardError::Codec(format!(
                    "shard placeholder in database template path must be preceded by '/' for correct path handling. \
                     Got: '{}'. Change to something like '/data/%shard%' where the shard ID is a directory name.",
                    template_path
                )));
            }
        }

        Ok(pos)
    }

    /// Resolve the object store at the storage root level.
    ///
    /// For Backend::Fs, this extracts the root path before the placeholder and
    /// returns the shard name as the db_path. For object-store backends, the
    /// store resolves once at the shared template root with the
    /// layout-preserving database path from [`Self::object_store_layout`], so
    /// parent and children of a split share one store while every shard's
    /// absolute object keys match what deployed binaries read and write.
    ///
    /// The returned `ResolvedStore.root_path` combined with `db_path` gives the
    /// full path where shard data is stored.
    fn resolve_at_root(
        backend: &crate::settings::Backend,
        template_path: &str,
        shard_name: &str,
    ) -> Result<(crate::storage::ResolvedStore, String), JobStoreShardError> {
        if backend.is_local_fs() {
            // Local filesystem backends: resolve at the storage root so that
            // cloned databases can correctly resolve their relative parent SST paths.
            let pos = Self::validate_template_path(template_path)?;

            let root = &template_path[..pos];
            let root_trimmed = root.trim_end_matches('/');
            let root_path = if root_trimmed.is_empty() {
                "/"
            } else {
                root_trimmed
            };

            let resolved = resolve_object_store(backend, root_path)?;
            Ok((resolved, shard_name.to_string()))
        } else {
            let (root, db_path) = Self::object_store_layout(backend, template_path, shard_name)?;
            let resolved = resolve_object_store(backend, &root)?;
            Ok((resolved, db_path))
        }
    }

    /// Compute the shared store root and layout-preserving database path for a
    /// shard on an object-store backend (Memory, S3, GCS, URL).
    ///
    /// The root is the template prefix before the first `%shard%`/`{shard}`
    /// placeholder (trailing slashes trimmed), or the whole path when no
    /// placeholder exists. Resolving every shard's store at this one root makes
    /// clone destinations equal open paths: a split's children land in the same
    /// store the parent resolved, exactly where each child's own `open` reads.
    ///
    /// The database path is the expanded template relative to the root,
    /// followed by the store-path component of the expanded template (the URL's
    /// path for URL-style backends, the raw expanded path for Memory). slatedb
    /// roots URL-resolved stores at the URL's path component, so for a
    /// `gs://bucket/silo/%shard%` template this puts absolute keys at
    /// `silo/<shard>/silo/<shard>/…` — byte-for-byte the keys deployed shards
    /// already use, so no data moves. Placeholder-free templates degenerate to
    /// database path = store-path component, preserving their keys too.
    pub fn object_store_layout(
        backend: &crate::settings::Backend,
        template_path: &str,
        shard_name: &str,
    ) -> Result<(String, String), JobStoreShardError> {
        let placeholder_pos = template_path
            .find("%shard%")
            .or_else(|| template_path.find("{shard}"));
        // For URL-style backends, a placeholder that is not at a `/` boundary
        // would make the shared-root arithmetic produce different absolute
        // keys than resolving the full expanded URL, silently moving every
        // shard's data. Fail loudly instead. Memory templates stay
        // unrestricted: they are test sentinels with no deployed data.
        if let Some(pos) = placeholder_pos
            && matches!(
                backend,
                crate::settings::Backend::S3
                    | crate::settings::Backend::Gcs
                    | crate::settings::Backend::Url
            )
            && pos > 0
            && template_path.chars().nth(pos - 1) != Some('/')
        {
            return Err(JobStoreShardError::Codec(format!(
                "shard placeholder in object-store database template path must be preceded by '/' \
                 so shards resolve under a shared root with unchanged keys. \
                 Got: '{}'. Change to something like 'gs://bucket/silo/%shard%'.",
                template_path
            )));
        }
        let root = match placeholder_pos {
            Some(pos) => template_path[..pos].trim_end_matches('/'),
            None => template_path,
        };
        let expanded = template_path
            .replace("%shard%", shard_name)
            .replace("{shard}", shard_name);
        let store_path = match backend {
            crate::settings::Backend::S3
            | crate::settings::Backend::Gcs
            | crate::settings::Backend::Url => {
                let url = Url::parse(&expanded).map_err(|e| {
                    JobStoreShardError::Codec(format!(
                        "failed to parse object store URL '{}': {}. Expected format: gs://bucket/path or s3://bucket/path",
                        expanded, e
                    ))
                })?;
                let url_path = url.path();
                url_path.strip_prefix('/').unwrap_or(url_path).to_string()
            }
            _ => expanded.clone(),
        };
        let relative = expanded[root.len()..].trim_start_matches('/');
        let db_path = if relative.is_empty() {
            store_path
        } else {
            format!("{relative}/{store_path}")
        };
        Ok((root.to_string(), db_path))
    }

    /// Resolve the WAL object store for a shard, if split WAL storage is configured.
    fn resolve_wal_store(
        &self,
        shard_name: &str,
    ) -> Result<Option<Arc<dyn ObjectStore>>, JobStoreShardError> {
        if let Some(wal_template) = &self.template.wal {
            let wal_path = wal_template
                .path
                .replace("%shard%", shard_name)
                .replace("{shard}", shard_name);
            let wal_resolved = resolve_object_store(&wal_template.backend, &wal_path)?;
            Ok(Some(wal_resolved.store))
        } else {
            Ok(None)
        }
    }

    /// Get the filesystem path where a shard's data is stored.
    /// This is only valid for local filesystem backends (Fs, TurmoilFs).
    fn get_shard_data_path(
        &self,
        shard_name: &str,
    ) -> Result<std::path::PathBuf, JobStoreShardError> {
        let (resolved, db_path) =
            Self::resolve_at_root(&self.template.backend, &self.template.path, shard_name)?;
        // For Fs backends, root_path is the canonical filesystem root and db_path is the shard name
        // For other backends, root_path might be a URL path component
        Ok(std::path::Path::new(&resolved.root_path).join(&db_path))
    }

    /// Close a specific shard and remove it from the factory.
    ///
    /// The entry is marked closing before `shard.close()` runs, which takes the shard out of service for good: `close()` tears down the shard's runtime irreversibly, so a shard whose close has begun must never be served again. If the close fails or times out, the entry stays in the closing state and the error is returned; calling `close` again makes another attempt on the same shard object. The entry is removed once an attempt reports the shard closed.
    ///
    /// SlateDB marks a database closed when its close begins, before it flushes. An attempt that follows one that timed out inside the database close therefore reports "already closed" at once and counts as complete: data acknowledged before that point is in the WAL, and the next writer to open the path fences this handle.
    ///
    /// The whole call runs under the close timeout, waiting for the entry included, because SlateDB's internal retrying_object_store retries indefinitely on transient errors: both a close and the `open` a close waits behind would otherwise hang forever if the object store is unreachable.
    pub async fn close(&self, shard_id: &ShardId) -> Result<(), JobStoreShardError> {
        // Wall-clock timer for the whole close call, including the timeout-guarded
        // wait, so slow closes can be attributed without a profiler.
        let call_started = std::time::Instant::now();

        // Marking under the map's per-key lock orders this against `open`'s
        // closing check, which runs under the same lock.
        let entry = self.instances.get(shard_id).map(|entry| {
            entry.mark_closing();
            Arc::clone(entry.value())
        });
        let Some(entry) = entry else {
            tracing::trace!(shard_id = %shard_id, "factory.close: shard not found in instances");
            return Ok(());
        };

        // Waits out an in-flight `open` initialization or an earlier `close`
        // attempt on this entry. `open` has no timeout of its own, so the wait
        // shares this call's deadline; the entry stays closing, and the next
        // attempt closes whatever the open produces.
        let deadline = tokio::time::Instant::now() + self.close_timeout;
        let Ok(_lifecycle) = tokio::time::timeout_at(deadline, entry.lifecycle.lock()).await else {
            tracing::error!(
                shard_id = %shard_id,
                timeout_secs = self.close_timeout.as_secs(),
                "factory.close: timed out waiting for an in-flight open or close of this shard, close pending"
            );
            return Err(self.close_timed_out());
        };
        if !self.is_current_entry(shard_id, &entry) {
            tracing::trace!(shard_id = %shard_id, "factory.close: an earlier close removed the entry");
            return Ok(());
        }

        if let Some(shard) = entry.get() {
            self.close_shard_by(deadline, shard_id, &shard).await?;
        } else {
            tracing::trace!(shard_id = %shard_id, "factory.close: shard not initialized");
        }
        self.instances
            .remove_if(shard_id, |_, current| Arc::ptr_eq(current, &entry));

        tracing::debug!(
            shard_id = %shard_id,
            total_ms = call_started.elapsed().as_millis() as u64,
            "factory: close returned"
        );

        Ok(())
    }

    /// Whether `entry` is the entry the map holds for this shard id.
    fn is_current_entry(&self, shard_id: &ShardId, entry: &Arc<ShardEntry>) -> bool {
        self.instances
            .get(shard_id)
            .is_some_and(|current| Arc::ptr_eq(current.value(), entry))
    }

    fn close_timed_out(&self) -> JobStoreShardError {
        JobStoreShardError::Codec(format!(
            "shard close timed out after {}s",
            self.close_timeout.as_secs()
        ))
    }

    /// Run one `shard.close()` attempt that must finish by `deadline`. A
    /// database that reports itself already closed counts as a completed close.
    async fn close_shard_by(
        &self,
        deadline: tokio::time::Instant,
        shard_id: &ShardId,
        shard: &JobStoreShard,
    ) -> Result<(), JobStoreShardError> {
        if self.take_injected_close_failure(shard_id) {
            tracing::warn!(shard_id = %shard_id, "test fault injection: failing shard close");
            return Err(JobStoreShardError::Codec(format!(
                "injected close failure for shard {shard_id}"
            )));
        }

        tracing::trace!(shard_id = %shard_id, "factory.close: calling shard.close()");
        let close_started = std::time::Instant::now();
        match tokio::time::timeout_at(deadline, shard.close()).await {
            Ok(Ok(())) => {
                tracing::info!(
                    shard_id = %shard_id,
                    close_ms = close_started.elapsed().as_millis() as u64,
                    "closed shard"
                );
                Ok(())
            }
            // The database marks itself closed when its close begins, so this is
            // what an attempt reports after an earlier one timed out inside it.
            Ok(Err(JobStoreShardError::Slate(ref slate_err)))
                if matches!(slate_err.kind(), slatedb::ErrorKind::Closed(_)) =>
            {
                tracing::info!(shard_id = %shard_id, "factory.close: shard already closed, treating as success");
                Ok(())
            }
            Ok(Err(e)) => {
                tracing::error!(shard_id = %shard_id, error = %e, "factory.close: shard.close() failed, close pending");
                Err(e)
            }
            Err(_elapsed) => {
                tracing::error!(
                    shard_id = %shard_id,
                    timeout_secs = self.close_timeout.as_secs(),
                    "factory.close: shard.close() timed out (object store may be unreachable), close pending"
                );
                Err(self.close_timed_out())
            }
        }
    }

    /// Reset a specific shard: close it, delete all data, and reopen fresh.
    /// A shard whose close is pending has that close retried first.
    /// This is intended for testing/development only.
    pub async fn reset(
        &self,
        shard_id: &ShardId,
        range: &ShardRange,
    ) -> Result<Arc<JobStoreShard>, JobStoreShardError> {
        let name = shard_id.to_string();

        // 1. Close the shard if it exists. No data is deleted until the close
        // has completed: a close that is still pending keeps its database handle.
        let close_was_pending = self.is_closing(shard_id);
        if let Err(e) = self.close(shard_id).await {
            if close_was_pending {
                return Err(JobStoreShardError::ClosePending(*shard_id));
            }
            return Err(e);
        }

        // 2. Delete the data using the appropriate method for the backend
        self.delete_shard_data(&name).await?;

        // Delete WAL directory if configured separately
        if let Some(wal_cfg) = &self.template.wal {
            self.delete_wal_data(&name, wal_cfg).await?;
        }

        // 3. Reopen the shard fresh
        tracing::info!(shard_id = %shard_id, "reopening shard after reset");
        self.open(shard_id, range).await
    }

    /// Delete all data for a shard from storage.
    /// Uses the same path resolution as opening to ensure we delete the correct directory.
    async fn delete_shard_data(&self, shard_name: &str) -> Result<(), JobStoreShardError> {
        if self.template.backend.is_local_fs() {
            let data_path = self.get_shard_data_path(shard_name)?;
            let path_str = data_path.to_string_lossy();

            if let Err(e) = tokio::fs::remove_dir_all(&data_path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(shard_name = %shard_name, path = %path_str, error = %e, "failed to delete shard data directory");
                }
            } else {
                tracing::info!(shard_name = %shard_name, path = %path_str, "deleted shard data directory");
            }
        } else {
            // For object store backends, use the object store API to delete all objects
            let (resolved, db_path) =
                Self::resolve_at_root(&self.template.backend, &self.template.path, shard_name)?;

            // List and delete all objects under the shard's path
            let prefix = ObjectPath::from(db_path.as_str());
            let objects: Vec<_> = resolved
                .store
                .list(Some(&prefix))
                .try_collect()
                .await
                .map_err(|e| {
                    JobStoreShardError::Codec(format!("failed to list objects for deletion: {}", e))
                })?;

            let count = objects.len();
            for obj in objects {
                if let Err(e) = resolved.store.delete(&obj.location).await {
                    tracing::warn!(
                        shard_name = %shard_name,
                        path = %obj.location,
                        error = %e,
                        "failed to delete object"
                    );
                }
            }
            tracing::info!(shard_name = %shard_name, objects_deleted = count, "deleted shard data from object store");
        }

        Ok(())
    }

    /// Delete WAL data for a shard.
    async fn delete_wal_data(
        &self,
        shard_name: &str,
        wal_cfg: &crate::settings::WalConfig,
    ) -> Result<(), JobStoreShardError> {
        let wal_path = wal_cfg
            .path
            .replace("%shard%", shard_name)
            .replace("{shard}", shard_name);

        if wal_cfg.is_local_storage() {
            // Local WAL - use filesystem deletion
            if let Err(e) = tokio::fs::remove_dir_all(&wal_path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(shard_name = %shard_name, path = %wal_path, error = %e, "failed to delete shard WAL directory");
                }
            } else {
                tracing::debug!(shard_name = %shard_name, path = %wal_path, "deleted shard WAL directory");
            }
        } else {
            // Object store WAL - use object store API
            let resolved = resolve_object_store(&wal_cfg.backend, &wal_path)?;
            let prefix = ObjectPath::from(resolved.canonical_path.as_str());
            let objects: Vec<_> = resolved
                .store
                .list(Some(&prefix))
                .try_collect()
                .await
                .map_err(|e| {
                    JobStoreShardError::Codec(format!(
                        "failed to list WAL objects for deletion: {}",
                        e
                    ))
                })?;

            for obj in objects {
                if let Err(e) = resolved.store.delete(&obj.location).await {
                    tracing::warn!(
                        shard_name = %shard_name,
                        path = %obj.location,
                        error = %e,
                        "failed to delete WAL object"
                    );
                }
            }
        }

        Ok(())
    }

    /// Check if this factory owns a shard by its ID.
    /// Returns true only if the shard entry exists, has been initialized, and is open.
    pub fn owns_shard(&self, shard_id: &ShardId) -> bool {
        self.get(shard_id).is_some()
    }

    /// Whether a close has begun on this shard and has not completed.
    pub fn is_closing(&self, shard_id: &ShardId) -> bool {
        self.instances
            .get(shard_id)
            .is_some_and(|entry| entry.is_closing())
    }

    /// Get a snapshot of all currently open instances.
    pub fn instances(&self) -> HashMap<ShardId, Arc<JobStoreShard>> {
        self.instances
            .iter()
            .filter_map(|entry| entry.value().serving().map(|shard| (*entry.key(), shard)))
            .collect()
    }

    /// Close all shards gracefully, open or closing, each through [`Self::close`]:
    /// under the close timeout, removed on success, left closing on failure.
    /// Returns all errors if any shards fail to close.
    pub async fn close_all(&self) -> Result<(), CloseAllError> {
        let mut errors: Vec<(ShardId, JobStoreShardError)> = Vec::new();
        // Collect and sort shard IDs for deterministic shutdown order
        let mut shard_ids: Vec<ShardId> = self.instances.iter().map(|e| *e.key()).collect();
        shard_ids.sort_unstable();
        for shard_id in shard_ids {
            if let Err(e) = self.close(&shard_id).await {
                errors.push((shard_id, e));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(CloseAllError { errors })
        }
    }

    /// Clone a closed shard's database to create child shards for splitting.
    ///
    /// This is used during shard splits after the parent shard has been fully closed.
    /// It opens a raw SlateDB database at the parent's path (without WAL, broker, or
    /// any silo-level processing), creates a single checkpoint, clones to both children,
    /// then closes the raw database.
    ///
    /// By operating on a fully closed parent, we guarantee that no in-flight writes
    /// can land after the checkpoint, ensuring children get a consistent snapshot.
    pub async fn clone_closed_shard(
        &self,
        parent_id: &ShardId,
        left_child_id: &ShardId,
        right_child_id: &ShardId,
    ) -> Result<(), ShardFactoryError> {
        let parent_name = parent_id.to_string();
        let left_child_name = left_child_id.to_string();
        let right_child_name = right_child_id.to_string();

        // Resolve paths relative to storage root
        let (parent_resolved, parent_db_path) =
            Self::resolve_at_root(&self.template.backend, &self.template.path, &parent_name)?;
        let (_, left_child_db_path) = Self::resolve_at_root(
            &self.template.backend,
            &self.template.path,
            &left_child_name,
        )?;
        let (_, right_child_db_path) = Self::resolve_at_root(
            &self.template.backend,
            &self.template.path,
            &right_child_name,
        )?;

        // Resolve separate WAL object stores if configured. Each shard (parent and
        // children) gets its own WAL store so that the clone's wal_object_store_uri
        // is correctly recorded in the manifest.
        let parent_wal_store = self.resolve_wal_store(&parent_name)?;
        let left_child_wal_store = self.resolve_wal_store(&left_child_name)?;
        let right_child_wal_store = self.resolve_wal_store(&right_child_name)?;

        // Open a raw SlateDB database at the parent's path. We need the WAL store
        // configured so slatedb can validate the wal_object_store_uri in the manifest.
        // We still need the merge operator because the parent may contain counter merge
        // entries that haven't been fully compacted yet.
        let mut parent_db_builder =
            slatedb::DbBuilder::new(parent_db_path.as_str(), Arc::clone(&parent_resolved.store))
                .with_merge_operator(crate::job_store_shard::counter_merge_operator());
        if let Some(wal) = &parent_wal_store {
            parent_db_builder = parent_db_builder.with_wal_object_store(Arc::clone(wal));
        }
        let db = parent_db_builder.build().await.map_err(|e| {
            ShardFactoryError::CloneError(format!("failed to reopen parent DB for cloning: {}", e))
        })?;

        // Flush to ensure all data is in object storage before checkpointing
        db.flush().await.map_err(|e| {
            ShardFactoryError::CloneError(format!("failed to flush before checkpoint: {}", e))
        })?;

        // Write a sentinel key to force WAL advancement. This ensures
        // replay_after_wal_id advances to next_wal_sst_id - 1, so clones
        // don't inherit a WAL gap that references SSTs on the parent's
        // (now gone) local WAL storage.
        let sentinel_key = format!(
            "clone_sentinel_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        );
        db.put(sentinel_key.as_bytes(), b"").await.map_err(|e| {
            ShardFactoryError::CloneError(format!("failed to write clone sentinel: {}", e))
        })?;
        db.delete(sentinel_key.as_bytes()).await.map_err(|e| {
            ShardFactoryError::CloneError(format!("failed to delete clone sentinel: {}", e))
        })?;

        // Create a single checkpoint shared by both children. Use no lifetime so
        // the checkpoint persists until the children have fully compacted away their
        // dependency on the parent's SSTs.
        // CheckpointScope::All calls flush_wals() + flush_memtables() internally,
        // which flushes the sentinel to L0 and advances replay_after_wal_id past
        // all WAL SSTs. This ensures clones don't inherit a WAL gap.
        let checkpoint_options = slatedb::config::CheckpointOptions {
            lifetime: None,
            ..Default::default()
        };
        let checkpoint = db
            .create_checkpoint(slatedb::config::CheckpointScope::All, &checkpoint_options)
            .await
            .map_err(|e| {
                ShardFactoryError::CloneError(format!("failed to create checkpoint: {}", e))
            })?;

        tracing::info!(
            parent_shard_id = %parent_id,
            left_child_id = %left_child_id,
            right_child_id = %right_child_id,
            checkpoint_id = ?checkpoint.id,
            "created checkpoint for shard cloning (from closed parent)"
        );

        let injected_empty_child = self.take_injected_empty_child();

        // Clone for left child
        if injected_empty_child == Some(*left_child_id) {
            tracing::warn!(child_id = %left_child_id, "test fault injection: skipping left child clone");
        } else {
            let mut left_admin_builder = slatedb::admin::Admin::builder(
                left_child_db_path.as_str(),
                Arc::clone(&parent_resolved.store),
            );
            if let Some(wal) = &left_child_wal_store {
                left_admin_builder = left_admin_builder.with_wal_object_store(Arc::clone(wal));
            }
            let left_admin = left_admin_builder.build();

            left_admin
                .create_clone_builder(parent_db_path.as_str(), Some(checkpoint.id))
                .build()
                .await
                .map_err(|e| {
                    ShardFactoryError::CloneError(format!(
                        "failed to clone left child database: {} (parent={}, child={})",
                        e, parent_db_path, left_child_db_path
                    ))
                })?;
        }

        // Clone for right child
        if injected_empty_child == Some(*right_child_id) {
            tracing::warn!(child_id = %right_child_id, "test fault injection: skipping right child clone");
        } else {
            let mut right_admin_builder = slatedb::admin::Admin::builder(
                right_child_db_path.as_str(),
                Arc::clone(&parent_resolved.store),
            );
            if let Some(wal) = &right_child_wal_store {
                right_admin_builder = right_admin_builder.with_wal_object_store(Arc::clone(wal));
            }
            let right_admin = right_admin_builder.build();

            right_admin
                .create_clone_builder(parent_db_path.as_str(), Some(checkpoint.id))
                .build()
                .await
                .map_err(|e| {
                    ShardFactoryError::CloneError(format!(
                        "failed to clone right child database: {} (parent={}, child={})",
                        e, parent_db_path, right_child_db_path
                    ))
                })?;
        }

        // Close the raw database
        db.close().await.map_err(|e| {
            ShardFactoryError::CloneError(format!(
                "failed to close raw parent DB after cloning: {}",
                e
            ))
        })?;

        tracing::info!(
            parent_shard_id = %parent_id,
            left_child_id = %left_child_id,
            right_child_id = %right_child_id,
            parent_db_path = %parent_db_path,
            left_child_db_path = %left_child_db_path,
            right_child_db_path = %right_child_db_path,
            "cloned closed shard database to both children"
        );

        Ok(())
    }

    /// Verify that each freshly cloned child holds the closed parent's full
    /// job set.
    ///
    /// A fresh SlateDB clone is a byte-for-byte copy of the closed parent, so
    /// immediately after `clone_closed_shard` -- before post-commit cleanup
    /// trims each child to its range -- every child's whole-shard `total_jobs`
    /// must equal the parent's. Both counts are read from raw post-close
    /// opens of the same storage, so the comparison is exact by construction
    /// (a count taken from the live parent could drift: background cleanup
    /// decrements counters until close completes). An empty or mis-placed
    /// child reads `0` (or fails to open at all, aborting via the open
    /// error), letting the split abort before the shard-map commit point.
    ///
    /// This is a manifest-landed tripwire keyed on the empty-child signature,
    /// not a full row-integrity audit: a clone that landed the counter key but
    /// corrupted rows would still pass.
    ///
    /// After a child's count check passes, its cleanup metadata is initialized
    /// in the same raw open (see
    /// [`JobStoreShard::initialize_split_cleanup_metadata`]): every acquirer
    /// of the committed child, on any node, then sees `CleanupPending` rather
    /// than the absent-or-inherited state a byte-for-byte clone carries. The
    /// per-child ordering (count check before initialize write) keeps the two
    /// failure classes distinguishable. The parent is only counted, never
    /// initialized -- resetting it to `CleanupPending` would make a
    /// subsequently aborted split recover a parent that spuriously scans its
    /// whole keyspace.
    pub async fn verify_and_initialize_cloned_children(
        &self,
        parent_id: &ShardId,
        child_ids: &[ShardId],
    ) -> Result<(), ShardFactoryError> {
        let parent_total_jobs = self.read_closed_shard_total_jobs(parent_id).await?;
        for child_id in child_ids {
            self.verify_and_initialize_cloned_child(child_id, parent_id, parent_total_jobs)
                .await?;
        }
        Ok(())
    }

    /// Verify one freshly cloned child against the parent's job count, then
    /// initialize its cleanup metadata -- a single raw open per child, count
    /// check first.
    async fn verify_and_initialize_cloned_child(
        &self,
        child_id: &ShardId,
        parent_id: &ShardId,
        parent_total_jobs: i64,
    ) -> Result<(), ShardFactoryError> {
        self.with_closed_shard_raw(child_id, |shard| async move {
            let child_total_jobs = shard
                .get_counters()
                .await
                .map_err(ShardFactoryError::ShardError)?
                .total_jobs;
            if child_total_jobs != parent_total_jobs {
                return Err(ShardFactoryError::ChildVerification(format!(
                    "cloned child {child_id} holds {child_total_jobs} jobs but parent {parent_id} \
                     held {parent_total_jobs}; aborting split before commit to avoid data loss"
                )));
            }

            if self.take_injected_init_failure(child_id) {
                return Err(ShardFactoryError::ChildInitialization(format!(
                    "injected cleanup-metadata initialization failure for child {child_id}"
                )));
            }

            shard
                .initialize_split_cleanup_metadata()
                .await
                .map_err(|e| {
                    ShardFactoryError::ChildInitialization(format!(
                        "failed to initialize cleanup metadata for cloned child {child_id}: {e}"
                    ))
                })
        })
        .await
    }

    /// Open a closed shard raw, run `f` against it, and always close the
    /// handle -- JobStoreShard has no Drop, so a leaked handle strands the
    /// shard's SlateDB instance and its spawned background tasks (grant
    /// scanner, reconcilers). The operation's error is surfaced before any
    /// close error.
    async fn with_closed_shard_raw<T, F, Fut>(
        &self,
        shard_id: &ShardId,
        f: F,
    ) -> Result<T, ShardFactoryError>
    where
        F: FnOnce(Arc<JobStoreShard>) -> Fut,
        Fut: std::future::Future<Output = Result<T, ShardFactoryError>>,
    {
        let shard = self.open_closed_shard_raw(shard_id).await?;
        let result = f(Arc::clone(&shard)).await;
        let close_result = shard.close().await;
        let value = result?;
        close_result?;
        Ok(value)
    }

    /// Read a closed shard's whole-shard `total_jobs` via the raw
    /// non-registering open.
    ///
    /// Hydration is disabled and `get_counters` reads single merged counter
    /// keys, so the check is O(1) even on a multi-million-job shard.
    async fn read_closed_shard_total_jobs(
        &self,
        shard_id: &ShardId,
    ) -> Result<i64, ShardFactoryError> {
        self.with_closed_shard_raw(shard_id, |shard| async move {
            Ok(shard.get_counters().await?.total_jobs)
        })
        .await
    }

    /// Read the split-cleanup metadata stored in a closed shard's database via
    /// the raw non-registering open. `pub` because integration tests compile
    /// as a separate crate and need to observe child metadata without
    /// registering the child in the factory.
    #[doc(hidden)]
    pub async fn read_closed_shard_cleanup_metadata(
        &self,
        shard_id: &ShardId,
    ) -> Result<ClosedShardCleanupMetadata, ShardFactoryError> {
        self.with_closed_shard_raw(shard_id, |shard| async move {
            Self::collect_cleanup_metadata(&shard).await
        })
        .await
    }

    async fn collect_cleanup_metadata(
        shard: &JobStoreShard,
    ) -> Result<ClosedShardCleanupMetadata, ShardFactoryError> {
        let status = shard.get_cleanup_status_raw().await?;
        let progress_key_present = shard
            .db()
            .get(&crate::keys::cleanup_progress_key())
            .await
            .map_err(JobStoreShardError::from)?
            .is_some();
        let complete_marker_present = shard
            .db()
            .get(&crate::keys::cleanup_complete_key())
            .await
            .map_err(JobStoreShardError::from)?
            .is_some();
        let cleanup_completed_at_ms = shard.get_cleanup_completed_at_ms().await?;
        Ok(ClosedShardCleanupMetadata {
            status,
            progress_key_present,
            complete_marker_present,
            cleanup_completed_at_ms,
        })
    }

    /// Open a closed shard's database with the raw `open_with_resolved_store`
    /// primitive.
    ///
    /// Uses the raw open -- NOT `open` -- so the shard is never registered in the
    /// `instances` map: `open` caches the instance in the per-shard OnceCell, and
    /// the post-commit `open(child)` would then return this stale pre-commit
    /// handle instead of opening the committed child. The caller MUST close the
    /// returned handle -- JobStoreShard has no Drop.
    async fn open_closed_shard_raw(
        &self,
        shard_id: &ShardId,
    ) -> Result<Arc<JobStoreShard>, ShardFactoryError> {
        let name = shard_id.to_string();
        let (resolved, db_path) =
            Self::resolve_at_root(&self.template.backend, &self.template.path, &name)?;
        let wal_store = self.resolve_wal_store(&name)?;

        let shard = JobStoreShard::open_with_resolved_store(
            name.clone(),
            &db_path,
            OpenShardOptions {
                store: resolved.store,
                wal_store,
                wal_close_config: None,
                slatedb_settings: self.template.slatedb.clone(),
                memory_cache: self.template.memory_cache.clone(),
                rate_limiter: Arc::clone(&self.rate_limiter),
                metrics: self.metrics.clone(),
                concurrency_reconcile_interval: Duration::from_millis(
                    self.template.concurrency_reconcile_interval_ms.max(1),
                ),
                counter_reconciliation_seconds: None,
                hydrate_all_at_startup: false,
                grant_scanner: GrantScannerConfig {
                    batch_size: self.template.grant_scanner_batch_size,
                    buffer_size: self.template.grant_scanner_buffer_size,
                    concurrency: self.template.grant_scanner_concurrency,
                    cold_batch_size: self.template.grant_scanner_cold_batch_size,
                    next_hop_skip_min_backlog: self
                        .template
                        .grant_scanner_next_hop_skip_min_backlog,
                    live_headroom_fraction: self.template.grant_scanner_live_headroom_fraction,
                    commit_chunk_size: self.template.grant_scanner_commit_chunk_size,
                },
                concurrency_reconcile_scan_slice: self.template.concurrency_reconcile_scan_slice,
                holder_drift_scan_slice: self.template.holder_drift_scan_slice,
                completed_job_expire_s: self.template.completed_job_expire_s,
                terminal_job_expire_s: self.template.terminal_job_expire_s,
                count_from_status_counters: self.template.count_from_status_counters,
                floating_refresh_stale_ms: self.template.floating_refresh_stale_ms,
                floating_refresh_stale_max_ms: self.template.floating_refresh_stale_max_ms,
                broker_tombstone_revive_after_generations: self
                    .template
                    .broker_tombstone_revive_after_generations,
            },
            ShardRange::full(),
        )
        .await?;

        Ok(shard)
    }

    /// Get the database template used by this factory.
    pub fn template(&self) -> &DatabaseTemplate {
        &self.template
    }
}

/// Split-cleanup metadata read from a closed shard's database. Field-level
/// key presence lets tests assert a full metadata reset without holding a
/// live handle to the shard.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedShardCleanupMetadata {
    /// Raw cleanup status: `None` when no status key is present.
    pub status: Option<crate::coordination::SplitCleanupStatus>,
    /// Whether the cleanup progress key is present.
    pub progress_key_present: bool,
    /// Whether the legacy cleanup complete-marker key is present.
    pub complete_marker_present: bool,
    /// Cleanup completion timestamp, if recorded.
    pub cleanup_completed_at_ms: Option<i64>,
}

#[derive(Debug, Error)]
pub struct CloseAllError {
    pub errors: Vec<(ShardId, JobStoreShardError)>,
}

impl std::fmt::Display for CloseAllError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} shard(s) failed to close", self.errors.len())
    }
}

/// Errors that can occur during shard factory operations.
#[derive(Debug, Error)]
pub enum ShardFactoryError {
    #[error("clone error: {0}")]
    CloneError(String),

    #[error("child verification error: {0}")]
    ChildVerification(String),

    #[error("child initialization error: {0}")]
    ChildInitialization(String),

    #[error("storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),

    #[error("shard error: {0}")]
    ShardError(#[from] JobStoreShardError),
}
