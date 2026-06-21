//! Job store shard - a single shard of the distributed job storage system.
mod cancel;
mod cleanup;
pub(crate) mod counters;
mod dequeue;
mod drop_tenant_holders;
mod enqueue;
mod expedite;
mod floating;
pub(crate) mod helpers;
pub(crate) mod holder_release_guard;
pub mod import;
mod lease;
mod lease_task;
pub(crate) mod limit_chain;
mod rate_limit;
mod restart;
mod scan;

pub use cleanup::{CleanupProgress, CleanupResult};
pub use drop_tenant_holders::DropTenantStats;

pub use counters::{
    JobStatusTruth, ReconcileSummary, ShardCounters, TenantStatusCounterScanRange,
    counter_merge_operator,
};
pub use expedite::JobNotExpediteableError;
pub use import::JobNotReimportableError;
pub use lease_task::JobNotLeaseableError;
pub use restart::JobNotRestartableError;

pub(crate) use enqueue::{LimitTaskParams, LimitTaskWriteResult};
use helpers::DbWriteBatcher;
use helpers::WriteBatcher;
pub use helpers::now_epoch_ms;

use slatedb_common::metrics::DefaultMetricsRecorder;
use std::sync::Arc;
#[cfg(feature = "server")]
use std::sync::OnceLock;
use std::time::Duration;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

use crate::instrumented_db::InstrumentedDb;

use crate::codec::CodecError;
use crate::concurrency::ConcurrencyManager;
use crate::gubernator::RateLimitClient;
use crate::job::{JobStatus, JobStatusKind, JobView};
use crate::job_attempt::JobAttemptView;
use crate::keys::{attempt_key, job_info_key, job_status_key};
use crate::metrics::Metrics;
#[cfg(feature = "server")]
use crate::query::ShardQueryEngine;
use crate::settings::DatabaseConfig;
use crate::shard_range::ShardRange;
use crate::storage::resolve_object_store;
use crate::task::{LeasedRefreshTask, LeasedTask};
use crate::task_broker::TaskBrokerRegistry;
use dashmap::DashMap;

/// Configuration for WAL cleanup during shard close
#[derive(Debug, Clone)]
pub struct WalCloseConfig {
    /// Path to the local WAL directory to clean up
    pub path: String,
    /// Whether to flush memtable to SST before closing
    pub flush_on_close: bool,
}

/// Options for opening a shard with a pre-resolved object store.
pub struct OpenShardOptions {
    /// Pre-resolved object store at the storage root level
    pub store: Arc<dyn slatedb::object_store::ObjectStore>,
    /// Optional separate object store for WAL
    pub wal_store: Option<Arc<dyn slatedb::object_store::ObjectStore>>,
    /// Optional WAL cleanup configuration for local storage
    pub wal_close_config: Option<WalCloseConfig>,
    /// Optional SlateDB settings for tuning database performance.
    /// The `merge_operator` field will be overridden with Silo's counter merge operator.
    pub slatedb_settings: Option<slatedb::config::Settings>,
    /// Optional in-memory cache size configuration.
    pub memory_cache: Option<crate::settings::MemoryCacheConfig>,
    /// Rate limiter client for this shard
    pub rate_limiter: Arc<dyn RateLimitClient>,
    /// Optional metrics collector
    pub metrics: Option<Metrics>,
    /// Interval for periodic concurrency request reconciliation.
    pub concurrency_reconcile_interval: Duration,
    /// Interval in seconds for the per-shard counter reconciliation
    /// background task. The reconciler exists to correct counter drift caused
    /// by the standalone compactor dropping terminal job rows; deployments
    /// that run the standalone compactor set this to enable it. `None`
    /// (the default) disables the task entirely.
    pub counter_reconciliation_seconds: Option<u64>,
    /// When true, scan every concurrency holder into the in-memory cache
    /// before opening the shard for ticket granting, and treat
    /// `ensure_hydrated` misses thereafter as empty queues. When false, no
    /// startup scan runs and the singleflighted per-queue `ensure_hydrated`
    /// path hydrates each queue lazily on first access.
    ///
    /// Populated from `DatabaseConfig::hydrate_all_at_startup` /
    /// `DatabaseTemplate::hydrate_all_at_startup`; tests set it explicitly.
    pub hydrate_all_at_startup: bool,
    /// Max grants the background grant scanner commits per `process_grants`
    /// pass. Populated from `grant_scanner_batch_size` in the database config.
    pub grant_scanner_batch_size: usize,
    /// Max in-flight status lookups the grant scanner buffers per pass.
    /// Populated from `grant_scanner_buffer_size` in the database config.
    pub grant_scanner_buffer_size: usize,
    /// Max per-queue `process_grants` passes the grant scanner runs concurrently.
    /// Populated from `grant_scanner_concurrency` in the database config.
    pub grant_scanner_concurrency: usize,
    /// When set, jobs that reach Succeeded have their associated KV records
    /// re-put with a SlateDB row TTL of this many seconds. `None` disables
    /// the feature for successful jobs.
    pub completed_job_expire_s: Option<u64>,
    /// When set, jobs that reach any non-success terminal status (Failed,
    /// Cancelled, …) have their associated KV records re-put with a SlateDB
    /// row TTL of this many seconds. `None` disables the feature for those
    /// jobs.
    pub terminal_job_expire_s: Option<u64>,
}

/// Compute the row TTL (`expire_ts`, epoch ms) for a job that reached the
/// given terminal status, given the two configured expiration windows.
/// Returns `None` for non-terminal kinds (`Scheduled`, `Running`) — those
/// jobs are still alive and must never be tagged for expiry.
///
/// `seconds: u64` is operator-supplied; saturating arithmetic keeps the
/// `u64 → i64` cast meaningful even if someone passes an absurd value
/// (would otherwise wrap to a negative `expire_ts`, which SlateDB
/// interprets as already-expired).
pub(crate) fn compute_terminal_expire_ts(
    kind: JobStatusKind,
    now_ms: i64,
    completed_job_expire_s: Option<u64>,
    terminal_job_expire_s: Option<u64>,
) -> Option<i64> {
    let seconds = match kind {
        JobStatusKind::Succeeded => completed_job_expire_s?,
        JobStatusKind::Failed | JobStatusKind::Cancelled => terminal_job_expire_s?,
        JobStatusKind::Scheduled | JobStatusKind::Running => return None,
    };
    let ms_i64 = i64::try_from(seconds.saturating_mul(1000)).unwrap_or(i64::MAX);
    Some(now_ms.saturating_add(ms_i64))
}

fn expand_slatedb_settings_for_shard(
    mut settings: slatedb::config::Settings,
    shard_name: &str,
) -> slatedb::config::Settings {
    expand_shard_placeholder_in_path(
        &mut settings.object_store_cache_options.root_folder,
        shard_name,
    );
    settings
}

fn expand_shard_placeholder_in_path(path: &mut Option<std::path::PathBuf>, shard_name: &str) {
    let Some(path) = path.as_mut() else {
        return;
    };
    let Some(path_str) = path.to_str() else {
        return;
    };
    if !path_str.contains("%shard%") && !path_str.contains("{shard}") {
        return;
    }

    *path = std::path::PathBuf::from(
        path_str
            .replace("%shard%", shard_name)
            .replace("{shard}", shard_name),
    );
}

/// Result of a dequeue operation - contains both job tasks and floating limit refresh tasks
#[derive(Debug, Default)]
pub struct DequeueResult {
    pub tasks: Vec<LeasedTask>,
    pub refresh_tasks: Vec<LeasedRefreshTask>,
}

/// Represents a single shard of the system. Owns the SlateDB instance.
pub struct JobStoreShard {
    pub(crate) name: String,
    pub(crate) db: Arc<InstrumentedDb>,
    pub(crate) brokers: Arc<TaskBrokerRegistry>,
    pub(crate) concurrency: Arc<ConcurrencyManager>,
    #[cfg(feature = "server")]
    query_engine: OnceLock<ShardQueryEngine>,
    pub(crate) rate_limiter: Arc<dyn RateLimitClient>,
    /// Optional WAL close configuration - present when using local WAL storage
    wal_close_config: Option<WalCloseConfig>,
    /// Optional metrics for recording shard-level stats
    pub(crate) metrics: Option<Metrics>,
    /// Interval for periodic concurrency request reconciliation.
    concurrency_reconcile_interval: Duration,
    /// Cancellation token for background tasks like cleanup.
    /// Signaled when the shard is closing.
    cancellation: CancellationToken,
    /// Object store for the shard's SlateDB instance (needed for Admin API).
    store: Arc<dyn slatedb::object_store::ObjectStore>,
    /// Database path relative to the object store root (needed for Admin API).
    db_path: String,
    /// The shard's tenant range, immutable after opening.
    range: ShardRange,
    /// Metrics recorder for SlateDB, passed to DbBuilder and used for stats collection.
    slatedb_metrics_recorder: Arc<DefaultMetricsRecorder>,
    /// Sparse in-memory counters for background action queue metrics.
    /// Keyed by `(tenant, task_group, status_bucket, queue_kind, queue)`.
    pub(crate) background_action_queue_counts:
        DashMap<(String, String, String, String, String), i64>,
    /// TTL (seconds) applied to Succeeded jobs' associated records. `None`
    /// disables the feature for successful jobs.
    pub(crate) completed_job_expire_s: Option<u64>,
    /// TTL (seconds) applied to non-success terminal jobs' associated records
    /// (Failed, Cancelled, …). `None` disables the feature for those jobs.
    pub(crate) terminal_job_expire_s: Option<u64>,
}

#[derive(Debug, Error)]
pub enum JobStoreShardError {
    #[error(transparent)]
    Storage(#[from] crate::storage::StorageError),
    #[error("{0}")]
    Slate(Arc<slatedb::Error>),
    #[error("json serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("codec error: {0}")]
    Codec(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("lease not found for task id {0}")]
    LeaseNotFound(String),
    #[error("lease owner mismatch for task id {task_id}: expected {expected}, got {got}")]
    LeaseOwnerMismatch {
        task_id: String,
        expected: String,
        got: String,
    },
    #[error("job already exists with id {0}")]
    JobAlreadyExists(String),
    #[error("job not found with id {0}")]
    JobNotFound(String),
    #[error("cannot delete job {0}: job is currently running or has pending requests")]
    JobInProgress(String),
    #[error("job already cancelled with id {0}")]
    JobAlreadyCancelled(String),
    #[error("cannot cancel job {0}: job is already in terminal state {1:?}")]
    JobAlreadyTerminal(String, JobStatusKind),
    #[error("cannot restart job: {0}")]
    JobNotRestartable(#[from] JobNotRestartableError),
    #[error("cannot reimport job: {0}")]
    JobNotReimportable(#[from] JobNotReimportableError),
    #[error("cannot expedite job: {0}")]
    JobNotExpediteable(#[from] JobNotExpediteableError),
    #[error("cannot lease job: {0}")]
    JobNotLeaseable(#[from] JobNotLeaseableError),
    #[error("transaction conflict during {0}, exceeded max retries")]
    TransactionConflict(String),
}

/// Information about the LSM tree state of a shard's SlateDB instance.
#[derive(Debug, Clone)]
pub struct LsmState {
    /// L0 SSTs (unflushed/uncompacted).
    pub l0_ssts: Vec<LsmSstInfo>,
    /// Sorted runs (compacted).
    pub sorted_runs: Vec<LsmSortedRunInfo>,
    /// Total estimated size of all L0 SSTs in bytes.
    pub total_l0_size: u64,
    /// Total estimated size of all sorted runs in bytes.
    pub total_sorted_run_size: u64,
}

/// Information about a single SST file.
#[derive(Debug, Clone)]
pub struct LsmSstInfo {
    /// SST identifier (debug format of SsTableId).
    pub id: String,
    /// Estimated size in bytes.
    pub estimated_size: u64,
}

/// Information about a sorted run.
#[derive(Debug, Clone)]
pub struct LsmSortedRunInfo {
    /// Sorted run identifier.
    pub id: u32,
    /// Number of SSTs in this sorted run.
    pub sst_count: usize,
    /// Estimated total size in bytes.
    pub estimated_size: u64,
}

impl From<LsmState> for crate::pb::GetShardStorageInfoResponse {
    fn from(lsm: LsmState) -> Self {
        Self {
            l0_sst_count: lsm.l0_ssts.len() as u64,
            total_l0_size: lsm.total_l0_size,
            sorted_run_count: lsm.sorted_runs.len() as u64,
            total_sorted_run_size: lsm.total_sorted_run_size,
            sorted_runs: lsm
                .sorted_runs
                .iter()
                .map(|sr| crate::pb::SortedRunInfo {
                    id: sr.id,
                    sst_count: sr.sst_count as u64,
                    estimated_size: sr.estimated_size,
                })
                .collect(),
        }
    }
}

impl From<CodecError> for JobStoreShardError {
    fn from(e: CodecError) -> Self {
        JobStoreShardError::Codec(e.to_string())
    }
}

impl From<slatedb::Error> for JobStoreShardError {
    fn from(e: slatedb::Error) -> Self {
        JobStoreShardError::Slate(Arc::new(e))
    }
}

impl From<Arc<slatedb::Error>> for JobStoreShardError {
    fn from(e: Arc<slatedb::Error>) -> Self {
        JobStoreShardError::Slate(e)
    }
}

impl From<crate::concurrency::ConcurrencyError> for JobStoreShardError {
    fn from(e: crate::concurrency::ConcurrencyError) -> Self {
        match e {
            crate::concurrency::ConcurrencyError::Slate(e) => JobStoreShardError::Slate(e),
            crate::concurrency::ConcurrencyError::Encoding(s) => JobStoreShardError::Codec(s),
            crate::concurrency::ConcurrencyError::ShardShuttingDown => {
                JobStoreShardError::Codec("shard shutting down".to_string())
            }
            crate::concurrency::ConcurrencyError::ChainResume(s) => JobStoreShardError::Codec(s),
        }
    }
}

impl JobStoreShard {
    /// Open a shard with a rate limit client, range, and optional metrics.
    ///
    /// The `range` parameter specifies the tenant keyspace this shard is responsible for.
    /// This is immutable after opening - when shards split, new child shards are created
    /// with new identities and ranges.
    pub async fn open(
        cfg: &DatabaseConfig,
        rate_limiter: Arc<dyn RateLimitClient>,
        metrics: Option<Metrics>,
        range: ShardRange,
    ) -> Result<Arc<Self>, JobStoreShardError> {
        let resolved = resolve_object_store(&cfg.backend, &cfg.path)?;

        // Configure separate WAL object store if specified, and track for cleanup on close
        let (wal_store, wal_close_config) = if let Some(wal_cfg) = &cfg.wal {
            let wal_resolved = resolve_object_store(&wal_cfg.backend, &wal_cfg.path)?;

            // Only set up WAL cleanup for local (Fs) storage backends
            let close_config = if wal_cfg.is_local_storage() {
                Some(WalCloseConfig {
                    path: wal_resolved.root_path,
                    flush_on_close: cfg.apply_wal_on_close,
                })
            } else {
                None
            };
            (Some(wal_resolved.store), close_config)
        } else {
            (None, None)
        };

        // Use SlateDB settings if configured
        let slatedb_settings = cfg.slatedb.clone();

        Self::open_with_resolved_store(
            cfg.name.clone(),
            &resolved.canonical_path,
            OpenShardOptions {
                store: resolved.store,
                wal_store,
                wal_close_config,
                slatedb_settings,
                memory_cache: cfg.memory_cache.clone(),
                rate_limiter,
                metrics,
                concurrency_reconcile_interval: Duration::from_millis(
                    crate::settings::DEFAULT_CONCURRENCY_RECONCILE_INTERVAL_MS.max(1),
                ),
                counter_reconciliation_seconds: cfg.counter_reconciliation_seconds,
                hydrate_all_at_startup: cfg.hydrate_all_at_startup,
                grant_scanner_batch_size: cfg.grant_scanner_batch_size,
                grant_scanner_buffer_size: cfg.grant_scanner_buffer_size,
                grant_scanner_concurrency: cfg.grant_scanner_concurrency,
                completed_job_expire_s: cfg.completed_job_expire_s,
                terminal_job_expire_s: cfg.terminal_job_expire_s,
            },
            range,
        )
        .await
    }

    /// Open a shard using a pre-resolved object store and relative path.
    ///
    /// This is the unified method for opening shards - it works for both normal shards
    /// and cloned shards because it operates at the storage root level.
    ///
    /// **Why root-level opening is required:**
    /// SlateDB clones store relative paths to parent SST files (e.g., `parent-uuid/compacted/file.sst`).
    /// If we opened with a LocalFileSystem rooted at the child path, these would resolve incorrectly
    /// as `child-uuid/parent-uuid/compacted/...` instead of `parent-uuid/compacted/...`.
    ///
    /// # Arguments
    /// * `name` - The shard's name (typically its UUID)
    /// * `db_path` - Path to the database relative to the object store root
    /// * `options` - Storage and configuration options for the shard
    /// * `range` - The tenant keyspace range for this shard
    pub async fn open_with_resolved_store(
        name: String,
        db_path: &str,
        options: OpenShardOptions,
        range: ShardRange,
    ) -> Result<Arc<Self>, JobStoreShardError> {
        let OpenShardOptions {
            store,
            wal_store,
            wal_close_config,
            slatedb_settings,
            memory_cache,
            rate_limiter,
            metrics,
            concurrency_reconcile_interval,
            counter_reconciliation_seconds,
            hydrate_all_at_startup,
            grant_scanner_batch_size,
            grant_scanner_buffer_size,
            grant_scanner_concurrency,
            completed_job_expire_s,
            terminal_job_expire_s,
        } = options;

        // Wall-clock timer for the whole open, used to emit per-phase debug
        // timings so slow shard opens can be attributed without a profiler.
        let open_started = std::time::Instant::now();

        let slatedb_metrics_recorder = Arc::new(DefaultMetricsRecorder::new());
        let mut db_builder = slatedb::DbBuilder::new(db_path, store.clone())
            .with_merge_operator(counters::counter_merge_operator())
            .with_metrics_recorder(slatedb_metrics_recorder.clone());

        // Configure separate WAL object store if provided
        if let Some(wal) = wal_store {
            db_builder = db_builder.with_wal_object_store(wal);
        }

        // Apply custom SlateDB settings if specified
        // Note: The merge_operator field in settings is ignored because we already
        // set it above via with_merge_operator() for counter support
        if let Some(settings) = slatedb_settings {
            db_builder =
                db_builder.with_settings(expand_slatedb_settings_for_shard(settings, &name));
        }

        // Apply custom in-memory cache sizes if specified
        if let Some(cache_cfg) = memory_cache {
            use slatedb::db_cache::{
                DEFAULT_BLOCK_CACHE_CAPACITY, DEFAULT_META_CACHE_CAPACITY, SplitCache,
                foyer::{FoyerCache, FoyerCacheOptions},
            };

            let block_cache = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                max_capacity: cache_cfg
                    .block_cache_bytes
                    .unwrap_or(DEFAULT_BLOCK_CACHE_CAPACITY),
                ..Default::default()
            }));
            let meta_cache = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                max_capacity: cache_cfg
                    .meta_cache_bytes
                    .unwrap_or(DEFAULT_META_CACHE_CAPACITY),
                ..Default::default()
            }));

            let cache = SplitCache::new()
                .with_block_cache(Some(block_cache))
                .with_meta_cache(Some(meta_cache))
                .build();

            db_builder = db_builder.with_db_cache(Arc::new(cache));
        }

        let shard_span = info_span!("shard", shard = %name);
        let db_build_started = std::time::Instant::now();
        let db = db_builder.build().instrument(shard_span.clone()).await?;
        tracing::debug!(
            shard = %name,
            elapsed_ms = db_build_started.elapsed().as_millis() as u64,
            "shard open: slatedb build"
        );
        let db = InstrumentedDb::new(Arc::new(db), shard_span);
        let concurrency = Arc::new(ConcurrencyManager::new(
            name.clone(),
            metrics.clone(),
            hydrate_all_at_startup,
            grant_scanner_batch_size,
            grant_scanner_buffer_size,
            grant_scanner_concurrency,
        ));

        // Eagerly hydrate the in-memory holders cache from durable storage so
        // that try_reserve and the grant scanner observe accurate capacity
        // from the first operation. Queues with zero holders are skipped —
        // when eager mode is enabled, `ensure_hydrated` treats such misses as
        // empty hydrated queues (see omittedQueuesAreSafe in specs/job_shard.als).
        //
        // Controlled by `hydrate_all_at_startup`. When false, the
        // singleflighted JIT `ensure_hydrated` path covers each queue on
        // first access.
        let hydrate_started = std::time::Instant::now();
        if hydrate_all_at_startup {
            concurrency.counts().hydrate_all(&db, &range).await?;
        }
        tracing::debug!(
            shard = %name,
            hydrated = hydrate_all_at_startup,
            elapsed_ms = hydrate_started.elapsed().as_millis() as u64,
            "shard open: hydrate"
        );

        let brokers = TaskBrokerRegistry::new(
            Arc::clone(&db),
            name.clone(),
            metrics.clone(),
            range.clone(),
        );

        let shard = Arc::new(Self {
            name,
            db,
            brokers,
            concurrency,
            #[cfg(feature = "server")]
            query_engine: OnceLock::new(),
            rate_limiter,
            wal_close_config,
            metrics,
            concurrency_reconcile_interval,
            cancellation: CancellationToken::new(),
            store,
            db_path: db_path.to_string(),
            range: range.clone(),
            slatedb_metrics_recorder,
            background_action_queue_counts: DashMap::new(),
            completed_job_expire_s,
            terminal_job_expire_s,
        });

        // Install the chain resumer before starting the grant scanner so the
        // scanner's first wake-up has a working callback for resuming limit
        // chains after a deferred grant. Uses a Weak<JobStoreShard> internally
        // so this trait object does not form a strong reference cycle.
        shard
            .concurrency
            .set_chain_resumer(limit_chain::ShardChainResumer::install(&shard));

        // Rebuild the background-action queue counter gauges in the background
        // rather than on the open critical path. The rebuild scans the entire
        // JOB_STATUS prefix (one row per job on the shard) and does a point read
        // of JOB_INFO per scheduled/running job, so it is O(jobs) and can take a
        // very long time on large shards (tens of millions of jobs). Its only
        // consumer is the Prometheus background-action queue gauge
        // (`snapshot_background_action_queue_counters`), so blocking the shard
        // from serving on it is unnecessary — the gauge simply reports an empty
        // shard until the spawned rebuild completes.
        shard.spawn_background_action_queue_counter_rebuild(range.clone());

        // Start the grant scanner after both ConcurrencyManager and TaskBrokerRegistry are ready,
        // and after the chain resumer is installed. It takes the instrumented db so its writes
        // are tagged with the shard span too.
        shard.concurrency.start_grant_scanner(
            Arc::clone(&shard.db),
            Arc::clone(&shard.brokers),
            range.clone(),
        );

        // Periodically reconcile pending concurrency requests to self-heal from
        // missed in-memory notifications or transient scanner failures.
        shard.spawn_concurrency_reconcile_task(range.clone());

        // Reactive holder reconciler — wakes on `reconcile_notify` from a
        // `PendingGrantGuard` / `PendingHolderReleaseGuard` Drop in dequeue
        // and immediately reconciles. Sibling to the periodic task above; no
        // jitter and no interval, so a dropped dequeue future un-wedges its
        // queue within milliseconds rather than waiting up to one reconcile
        // interval. The periodic task still runs `reconcile_pending_holders`
        // on its tick as a safety net against missed notifications.
        shard.spawn_holder_reconcile_task(range.clone());

        // Watch slatedb's status channel and emit prometheus metrics on each
        // change (durable_seq advances, manifest revisions). No-op when
        // metrics are disabled.
        shard.spawn_db_status_watcher();

        // Periodically reconcile job counters from JOB_INFO/JOB_STATUS truth.
        // Only enabled when the deployment runs the standalone compactor, which
        // drops terminal job rows without a writable Db handle and therefore
        // cannot decrement counters at drop time.
        if let Some(interval_seconds) = counter_reconciliation_seconds {
            shard.spawn_counter_reconcile_task(range, interval_seconds);
        }

        // Set the shard creation timestamp if this is the first time opening
        let created_at_started = std::time::Instant::now();
        shard.set_created_at_ms_if_unset().await?;
        tracing::debug!(
            shard = %shard.name,
            elapsed_ms = created_at_started.elapsed().as_millis() as u64,
            "shard open: set created_at"
        );

        tracing::debug!(
            shard = %shard.name,
            total_ms = open_started.elapsed().as_millis() as u64,
            "shard open: complete"
        );

        Ok(shard)
    }

    /// Get the query engine for this shard, lazily initializing it on first access.
    ///
    /// This is the low-level query engine for single-shard queries, typically used
    /// by gRPC handlers. For cluster-wide queries, use `ClusterQueryEngine`.
    #[cfg(feature = "server")]
    pub fn query_engine(self: &Arc<Self>) -> &ShardQueryEngine {
        self.query_engine.get_or_init(|| {
            ShardQueryEngine::new(Arc::clone(self), "jobs").expect("Failed to create query engine")
        })
    }

    /// Close the underlying SlateDB instance gracefully.
    ///
    /// When a separate local WAL is configured with `apply_wal_on_close: true`:
    /// 1. Flushes all memtable data to SSTs in object storage
    /// 2. Closes the database
    /// 3. Deletes the local WAL directory
    ///
    /// This ensures all data is durably stored in object storage before closing,
    /// allowing the shard to be safely reopened elsewhere (e.g., on a different node).
    pub async fn close(&self) -> Result<(), JobStoreShardError> {
        // Wall-clock timer for the whole close, used to emit per-phase debug
        // timings so slow shard closes can be attributed without a profiler.
        let close_started = std::time::Instant::now();

        self.cancellation.cancel();
        self.brokers.stop();
        self.concurrency.stop_grant_scanner();

        // If we have a local WAL with flush_on_close enabled, flush memtable to SSTs first
        if let Some(ref wal_config) = self.wal_close_config
            && wal_config.flush_on_close
        {
            tracing::info!(
                shard = %self.name,
                wal_path = %wal_config.path,
                "flushing memtable to object storage before close"
            );

            // Flush memtable to SST to ensure all data is in object storage
            // This converts all in-memory data (and WAL data) to SSTs
            let flush_started = std::time::Instant::now();
            self.db
                .flush_with_options(slatedb::config::FlushOptions {
                    flush_type: slatedb::config::FlushType::MemTable,
                })
                .await
                .map_err(JobStoreShardError::from)?;

            tracing::debug!(
                shard = %self.name,
                elapsed_ms = flush_started.elapsed().as_millis() as u64,
                "shard close: flush memtable"
            );
        }

        // Close the database
        tracing::trace!(shard = %self.name, "shard.close: calling db.close()");
        let db_close_started = std::time::Instant::now();
        self.db.close().await.map_err(JobStoreShardError::from)?;
        tracing::debug!(
            shard = %self.name,
            elapsed_ms = db_close_started.elapsed().as_millis() as u64,
            "shard close: db close"
        );

        // After closing, clean up the local WAL directory if configured
        if let Some(ref wal_config) = self.wal_close_config
            && wal_config.flush_on_close
        {
            tracing::info!(
                shard = %self.name,
                wal_path = %wal_config.path,
                "removing local WAL directory after successful close"
            );

            let wal_remove_started = std::time::Instant::now();
            if let Err(e) = tokio::fs::remove_dir_all(&wal_config.path).await {
                // Log but don't fail if WAL directory removal fails
                // The data is already durably in object storage
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        shard = %self.name,
                        wal_path = %wal_config.path,
                        error = %e,
                        "failed to remove local WAL directory (data is still safe in object storage)"
                    );
                }
            } else {
                tracing::debug!(
                    shard = %self.name,
                    wal_path = %wal_config.path,
                    elapsed_ms = wal_remove_started.elapsed().as_millis() as u64,
                    "shard close: remove local WAL directory"
                );
            }
        }

        tracing::debug!(
            shard = %self.name,
            total_ms = close_started.elapsed().as_millis() as u64,
            "shard close: complete"
        );

        Ok(())
    }

    /// Returns the WAL close configuration, if any.
    /// This is primarily used for testing to verify WAL cleanup behavior.
    pub fn wal_close_config(&self) -> Option<&WalCloseConfig> {
        self.wal_close_config.as_ref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Spawn a background task that mirrors slatedb's `Db::subscribe` watch
    /// channel into prometheus gauges and a manifest-revision counter. Exits
    /// when the shard's cancellation token is cancelled (during `close()`)
    /// or when the underlying `Db` is dropped (sender side closes).
    ///
    /// No-op when metrics are disabled — avoids spawning an idle task per
    /// shard in test/bench configurations that opt out of metrics.
    fn spawn_db_status_watcher(self: &Arc<Self>) {
        let Some(metrics) = self.metrics.clone() else {
            return;
        };
        let shard_name = self.name.clone();
        let cancellation = self.cancellation.clone();
        let mut rx = self.db.inner().subscribe();

        tokio::spawn(async move {
            // Emit initial values so gauges aren't empty until the first
            // change. The first watch read also seeds the manifest-revision
            // dedup key without bumping the counter.
            let initial = rx.borrow_and_update().clone();
            let mut prev_manifest_id = initial.current_manifest.id();
            metrics.record_db_status(&shard_name, &initial, false);

            loop {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        tracing::debug!(
                            shard = %shard_name,
                            "stopping slatedb status watcher (shard closing)"
                        );
                        break;
                    }
                    changed = rx.changed() => {
                        if changed.is_err() {
                            // Sender dropped — the Db was dropped without a
                            // close(). Treat the same as cancellation.
                            tracing::debug!(
                                shard = %shard_name,
                                "stopping slatedb status watcher (db dropped)"
                            );
                            break;
                        }
                        let status = rx.borrow_and_update().clone();
                        let manifest_changed = status.current_manifest.id() != prev_manifest_id;
                        prev_manifest_id = status.current_manifest.id();
                        metrics.record_db_status(&shard_name, &status, manifest_changed);
                    }
                }
            }
        });
    }

    /// Reactive holder reconciler — drains the per-task_id reconciliation
    /// queue immediately on a `reconcile_notify` wake from a dropped
    /// `PendingGrantGuard` / `PendingHolderReleaseGuard`. No jitter, no
    /// periodic tick: a wedged queue (durable=0, in_mem=1 after the dequeue
    /// future was cancelled around its commit) un-wedges as soon as Drop
    /// runs. The periodic task (`spawn_concurrency_reconcile_task`) still
    /// calls `reconcile_pending_holders` on its tick as a safety net.
    ///
    /// If a pass re-queues anything (transient hydrate / `db.get` failure),
    /// we sleep `RECONCILE_RETRY_BACKOFF` then poke `reconcile_notify` so
    /// the next iteration retries promptly — without this, the re-queued
    /// tuple would sit idle until the next unrelated Drop or the 5 s
    /// periodic tick. The sleep also bounds the retry rate so a sustained
    /// failure (slatedb temporarily unavailable, etc.) doesn't tight-loop.
    /// Spawn a one-shot background task that rebuilds the background-action
    /// queue counter gauges from durable storage.
    ///
    /// This is kept off the open critical path because the rebuild scans the
    /// whole JOB_STATUS prefix and point-reads JOB_INFO per scheduled/running
    /// job — O(jobs), which is prohibitively slow on large shards. The gauge it
    /// populates is metrics-only, so the shard can serve immediately while the
    /// rebuild runs. A failure is logged and dropped; the periodic counter
    /// reconciler (when enabled) and live transition deltas keep the gauge
    /// converging afterwards.
    fn spawn_background_action_queue_counter_rebuild(self: &Arc<Self>, range: ShardRange) {
        let shard = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        let shard_name = self.name.clone();

        tokio::spawn(async move {
            let started = std::time::Instant::now();
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    tracing::debug!(
                        shard = %shard_name,
                        "skipping background action queue counter rebuild (shard closing)"
                    );
                }
                result = shard.rebuild_background_action_queue_counters(&range) => {
                    match result {
                        Ok(()) => tracing::debug!(
                            shard = %shard_name,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "shard open: rebuild bg action counters (background)"
                        ),
                        Err(e) => tracing::warn!(
                            shard = %shard_name,
                            error = %e,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "background action queue counter rebuild failed; gauge will converge via reconcile and live deltas"
                        ),
                    }
                }
            }
        });
    }

    fn spawn_holder_reconcile_task(self: &Arc<Self>, range: ShardRange) {
        /// Delay between reactive-reconciler retries when a pass re-queued
        /// at least one tuple. 100 ms = ~10 retries/sec ceiling under
        /// sustained failure, ~100 ms recovery on a transient one.
        const RECONCILE_RETRY_BACKOFF: Duration = Duration::from_millis(100);

        let shard = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        let shard_name = self.name.clone();
        let reconcile_notify = self.concurrency.reconcile_notify();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = reconcile_notify.notified() => {
                        let requeued = shard
                            .concurrency
                            .reconcile_pending_holders(&shard.db, &range)
                            .await;
                        if requeued {
                            // Bound retry rate; cancellation must still
                            // short-circuit the sleep so shard close is
                            // prompt.
                            tokio::select! {
                                biased;
                                _ = tokio::time::sleep(RECONCILE_RETRY_BACKOFF) => {
                                    reconcile_notify.notify_one();
                                }
                                _ = cancellation.cancelled() => {
                                    tracing::debug!(
                                        shard = %shard_name,
                                        "stopping reactive holder reconciler during retry backoff (shard closing)"
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    _ = cancellation.cancelled() => {
                        tracing::debug!(
                            shard = %shard_name,
                            "stopping reactive holder reconciler (shard closing)"
                        );
                        break;
                    }
                }
            }
        });
    }

    fn spawn_concurrency_reconcile_task(self: &Arc<Self>, range: ShardRange) {
        let shard = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        let shard_name = self.name.clone();
        let reconcile_interval = self.concurrency_reconcile_interval;

        tokio::spawn(async move {
            // Deterministic jitter based on shard name so that many shards
            // opened simultaneously in one process don't all scan the
            // CONCURRENCY_REQUESTS prefix at the same instant.
            let interval_ms = reconcile_interval.as_millis() as u64;
            let jitter_ms = if interval_ms > 0 {
                let mut h: u64 = 1469598103934665603;
                for b in shard_name.as_bytes() {
                    h ^= *b as u64;
                    h = h.wrapping_mul(1099511628211);
                }
                h % interval_ms
            } else {
                0
            };

            tokio::select! {
                biased;
                _ = tokio::time::sleep(Duration::from_millis(jitter_ms)) => {}
                _ = cancellation.cancelled() => {
                    tracing::debug!(
                        shard = %shard_name,
                        "stopping periodic concurrency reconciliation before first tick (shard closing)"
                    );
                    return;
                }
            }

            let mut interval = tokio::time::interval(reconcile_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    _ = interval.tick() => {
                        // Holders first: the request scan can grant new slots,
                        // and we want the in-memory holder set consistent
                        // before that grant decision runs. This is the safety
                        // net for any missed `reconcile_notify` wake; the
                        // reactive `spawn_holder_reconcile_task` is the
                        // primary low-latency path. The periodic tick
                        // already rate-limits, so we drop the re-queue
                        // signal from the reactive path here.
                        let _ = shard
                            .concurrency
                            .reconcile_pending_holders(&shard.db, &range)
                            .await;
                        shard
                            .concurrency
                            .reconcile_pending_requests(shard.db.as_ref(), &range)
                            .await;
                        // After draining, snapshot drift + pending-queue size
                        // for prometheus. Cheap per tick (one ranged scan per
                        // hydrated queue); the per-tick cadence keeps the
                        // total cost bounded.
                        shard
                            .concurrency
                            .report_holder_drift(&shard.db, &range)
                            .await;
                    }
                    _ = cancellation.cancelled() => {
                        tracing::debug!(
                            shard = %shard_name,
                            "stopping periodic concurrency reconciliation (shard closing)"
                        );
                        break;
                    }
                }
            }
        });
    }

    /// Spawn a background task that periodically calls `reconcile_counters`.
    ///
    /// Unlike the concurrency reconciler (small scan, fast cadence), this task
    /// scans all `JOB_INFO` + `JOB_STATUS` rows in the shard and so runs at
    /// the deployment-configured cadence (typically hourly). To avoid all
    /// shards spiking object-store reads at the same wall-clock minute on
    /// process boot, we sleep a shard-deterministic jitter `[0, interval)`
    /// before the first tick. The jitter is derived from the shard name's
    /// hash so that deterministic simulation tests remain reproducible.
    fn spawn_counter_reconcile_task(self: &Arc<Self>, range: ShardRange, interval_seconds: u64) {
        let shard = Arc::clone(self);
        let cancellation = self.cancellation.clone();
        let shard_name = self.name.clone();
        let reconcile_interval =
            Duration::from_millis(interval_seconds.saturating_mul(1000).max(1));

        tokio::spawn(async move {
            // Deterministic jitter based on shard name so we don't stampede
            // object storage when many shards open simultaneously.
            let interval_ms = reconcile_interval.as_millis() as u64;
            let jitter_ms = if interval_ms > 0 {
                let mut h: u64 = 1469598103934665603;
                for b in shard_name.as_bytes() {
                    h ^= *b as u64;
                    h = h.wrapping_mul(1099511628211);
                }
                h % interval_ms
            } else {
                0
            };

            tokio::select! {
                biased;
                _ = tokio::time::sleep(Duration::from_millis(jitter_ms)) => {}
                _ = cancellation.cancelled() => {
                    tracing::debug!(
                        shard = %shard_name,
                        "stopping periodic counter reconciliation before first tick (shard closing)"
                    );
                    return;
                }
            }

            let mut interval = tokio::time::interval(reconcile_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    _ = interval.tick() => {
                        let summary = shard.reconcile_counters(&range).await;
                        if summary.failed > 0 {
                            tracing::warn!(
                                shard = %shard_name,
                                scanned_jobs = summary.scanned_jobs,
                                corrected = summary.corrected,
                                failed = summary.failed,
                                "counter reconcile pass had failures; counters may remain stale until next pass"
                            );
                        } else {
                            tracing::debug!(
                                shard = %shard_name,
                                scanned_jobs = summary.scanned_jobs,
                                corrected = summary.corrected,
                                "counter reconcile pass complete"
                            );
                        }
                    }
                    _ = cancellation.cancelled() => {
                        tracing::debug!(
                            shard = %shard_name,
                            "stopping periodic counter reconciliation (shard closing)"
                        );
                        break;
                    }
                }
            }
        });
    }

    pub fn db(&self) -> &InstrumentedDb {
        &self.db
    }

    /// Resolve the row TTL (`expire_ts`, epoch ms) for a job that reached the
    /// given terminal status, or `None` if no TTL is configured.
    pub(crate) fn terminal_expire_ts(&self, kind: JobStatusKind, now_ms: i64) -> Option<i64> {
        compute_terminal_expire_ts(
            kind,
            now_ms,
            self.completed_job_expire_s,
            self.terminal_job_expire_s,
        )
    }

    /// Snapshot the in-memory concurrency limit cache for use by the query system.
    pub fn snapshot_queue_limits(&self) -> Vec<crate::concurrency::CachedQueueLimit> {
        self.concurrency.snapshot_queue_limits()
    }

    /// Get the total number of tasks in the broker buffer across all task groups.
    pub fn broker_buffer_len(&self) -> usize {
        self.brokers.buffer_len()
    }

    /// Number of tasks currently held in the broker's in-flight set for a task
    /// group (claimed by a worker but not yet durably acked). Exposed for
    /// observability and for tests that assert the dequeue drop-guard releases
    /// claimed keys when its future is cancelled.
    pub fn broker_inflight_len(&self, task_group: &str) -> usize {
        self.brokers.group_inflight_len(task_group)
    }

    /// Snapshot the sizes of the in-memory concurrency holders cache.
    pub fn concurrency_cache_stats(&self) -> crate::concurrency::ConcurrencyCacheStats {
        self.concurrency.cache_stats()
    }

    /// Holder count tracked in the in-memory concurrency cache for a specific
    /// (tenant, queue). Useful for tests and ad-hoc debugging.
    pub fn concurrency_holder_count(&self, tenant: &str, queue: &str) -> usize {
        self.concurrency.counts().holder_count(tenant, queue)
    }

    /// Whether a specific `task_id` is currently recorded as an in-memory
    /// holder for `(tenant, queue)`. Test-only accessor used by the
    /// four-quadrant reconciler test.
    pub fn concurrency_contains_holder(&self, tenant: &str, queue: &str, task_id: &str) -> bool {
        self.concurrency
            .counts()
            .contains_holder(tenant, queue, task_id)
    }

    /// Test-only: enqueue a `(tenant, queue, task_id)` for reconciliation.
    /// Mirrors what `PendingHolderReleaseGuard::drop` does on cancellation,
    /// so the four-quadrant test can drive the reconciler deterministically
    /// without orchestrating a real dequeue future cancellation.
    pub fn request_concurrency_reconciliation(
        &self,
        tenant: String,
        queue: String,
        task_id: String,
    ) {
        self.concurrency
            .request_reconciliation(tenant, queue, task_id);
    }

    /// Test-only: insert an in-memory holder directly, used to set up the
    /// "in_mem present, durable absent" ghost quadrant.
    pub fn concurrency_insert_holder(&self, tenant: &str, queue: &str, task_id: &str) {
        self.concurrency
            .counts()
            .insert_holder(tenant, queue, task_id);
    }

    /// Test-only: peek the current pending-grant count accumulated for
    /// `(tenant, queue)`. Used by the reconciler regression test to assert
    /// that each ghost release fired its own `request_grant`.
    pub fn pending_grant_count_for_test(&self, tenant: &str, queue: &str) -> u32 {
        self.concurrency.pending_grant_count_for_test(tenant, queue)
    }

    /// Test-only: drain pending reconciliations and apply them now. Returns
    /// whether the pass had to re-queue anything (transient DB error
    /// during hydrate or `db.get`). The production path goes through
    /// `spawn_holder_reconcile_task` woken via `reconcile_notify`; this
    /// synchronous entry point lets tests assert post-state without a
    /// sleep race.
    pub async fn reconcile_pending_holders_for_test(&self) -> bool {
        let range = self.get_range();
        self.concurrency
            .reconcile_pending_holders(&self.db, &range)
            .await
    }

    /// Test-only: run one self-healing drift pass. Walks hydrated queues,
    /// compares the in-memory holder set to durable rows, and enqueues any
    /// ghosts (in-memory holders with no durable row) for reconciliation.
    /// Callers typically follow with `reconcile_pending_holders_for_test` to
    /// drive the actual release.
    pub async fn report_holder_drift_for_test(&self) {
        let range = self.get_range();
        self.concurrency.report_holder_drift(&self.db, &range).await
    }
    /// Get the SlateDB metrics registry for this shard.
    /// Use this to collect storage-level statistics for observability.
    pub fn slatedb_metrics_recorder(&self) -> &Arc<DefaultMetricsRecorder> {
        &self.slatedb_metrics_recorder
    }

    /// Read LSM tree state from the SlateDB manifest.
    ///
    /// Returns information about L0 SSTs and sorted runs that helps operators
    /// understand whether compaction is needed.
    pub async fn read_lsm_state(&self) -> Result<LsmState, JobStoreShardError> {
        use slatedb::admin::AdminBuilder;

        let admin = AdminBuilder::new(self.db_path.as_str(), self.store.clone()).build();

        let state = admin.read_compactor_state_view().await.map_err(|e| {
            JobStoreShardError::Codec(format!("failed to read compactor state: {e}"))
        })?;

        let manifest = state.manifest();

        let l0_ssts: Vec<LsmSstInfo> = manifest
            .l0()
            .iter()
            .map(|sst| LsmSstInfo {
                id: format!("{:?}", sst.id),
                estimated_size: sst.estimate_size(),
            })
            .collect();

        let sorted_runs: Vec<LsmSortedRunInfo> = manifest
            .compacted()
            .iter()
            .map(|sr| LsmSortedRunInfo {
                id: sr.id,
                sst_count: sr.sst_views.len(),
                estimated_size: sr.estimate_size(),
            })
            .collect();

        let total_l0_size: u64 = l0_ssts.iter().map(|s| s.estimated_size).sum();
        let total_sorted_run_size: u64 = sorted_runs.iter().map(|s| s.estimated_size).sum();

        Ok(LsmState {
            l0_ssts,
            sorted_runs,
            total_l0_size,
            total_sorted_run_size,
        })
    }

    /// Get the shard's tenant range.
    ///
    /// The range is immutable after the shard is opened. When shards split,
    /// new child shards are created with new identities and ranges.
    pub fn get_range(&self) -> ShardRange {
        self.range.clone()
    }

    /// Run one pass of pending concurrency-request reconciliation.
    ///
    /// Intended for diagnostics/benchmarking of the periodic reconciler path.
    pub async fn reconcile_pending_concurrency_requests_once(&self) {
        let range = self.get_range();
        self.concurrency
            .reconcile_pending_requests(self.db.as_ref(), &range)
            .await;
    }

    /// Stop the background grant scanner without closing the shard.
    ///
    /// Intended for benchmarking — prevents the background scanner from racing
    /// with manual `process_concurrency_grants` calls.
    pub fn stop_grant_scanner(&self) {
        self.concurrency.stop_grant_scanner();
    }

    /// Directly process pending concurrency grants for a single queue.
    ///
    /// Bypasses the background grant scanner and processes `count` grants synchronously.
    /// Intended for benchmarking the grant processing hot path.
    pub async fn process_concurrency_grants(
        &self,
        tenant: &str,
        queue: &str,
        count: u32,
    ) -> Vec<String> {
        let range = self.get_range();
        self.concurrency
            .process_grants(&self.db, &range, tenant, queue, count)
            .await
    }

    /// Test-only: drop the installed chain resumer so the next
    /// `process_concurrency_grants` exercises the scanner's
    /// release-and-bail branch. Returns the removed resumer (caller may
    /// drop it or re-install via `set_chain_resumer`).
    pub fn take_chain_resumer_for_test(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::concurrency::LimitChainResumer>> {
        self.concurrency.take_chain_resumer_for_test()
    }

    /// Test-only: release the in-memory reservation for a (tenant, queue,
    /// task_id) tuple. Symmetric with `rollback_grant`'s rollback path,
    /// exposed publicly so tests that fabricate orphan request scenarios
    /// can clear the in-memory holder created by the natural enqueue path.
    pub fn rollback_concurrency_grant_for_test(&self, tenant: &str, queue: &str, task_id: &str) {
        self.concurrency.rollback_grant(tenant, queue, task_id);
    }

    /// Test-only: inject the broker buffer entries for the given task keys,
    /// reading the persisted task bytes from the DB. Lets tests deterministically
    /// drive `dequeue` against future-scheduled or pre-cancel-staged tasks
    /// without waiting for the background broker scanner.
    pub async fn force_buffer_tasks_for_test(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<usize, JobStoreShardError> {
        use crate::codec::decode_task_validated;
        use crate::task_broker::BrokerTask;
        let mut tasks = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(raw) = self.db.get(&key).await? else {
                continue;
            };
            let decoded = decode_task_validated(raw)
                .map_err(|e| JobStoreShardError::Codec(format!("force_buffer decode: {e}")))?;
            tasks.push(BrokerTask { key, decoded });
        }
        let count = tasks.len();
        self.brokers.requeue(tasks);
        Ok(count)
    }

    /// Test-only: reserve an in-memory holder for (tenant, queue, task_id).
    /// Bypasses DB hydration and capacity checks so tests can fabricate the
    /// "in-memory and on-disk holder exist for a fake chain" state needed to
    /// drive cancel/reimport paths through their defensive arms.
    pub async fn force_reserve_concurrency_for_test(
        &self,
        tenant: &str,
        queue: &str,
        task_id: &str,
        limit: usize,
    ) -> bool {
        self.concurrency
            .counts()
            .try_reserve(
                &self.db,
                &self.get_range(),
                tenant,
                queue,
                task_id,
                limit,
                "force-reserve-test",
            )
            .await
            .unwrap_or(false)
    }

    /// Fetch a job by id as a zero-copy archived view.
    pub async fn get_job(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<JobView>, JobStoreShardError> {
        let key = job_info_key(tenant, id);
        match self.db.get(&key).await? {
            Some(raw) => Ok(Some(JobView::new(raw)?)),
            None => Ok(None),
        }
    }

    /// Fetch multiple jobs by id concurrently. Returns a map of job_id -> JobView.
    /// Jobs that don't exist will not be present in the result map.
    pub async fn get_jobs_batch(
        &self,
        tenant: &str,
        ids: &[String],
    ) -> Result<std::collections::HashMap<String, JobView>, JobStoreShardError> {
        use futures::StreamExt;
        use std::collections::HashMap;

        // Bound concurrency to avoid thundering-herd on the object-store I/O
        // layer (local-disk: spawn_blocking thread explosion; GCS: connection
        // pool saturation).  64 in-flight gets gives ~3x wall-clock speedup on
        // warm block-cache while keeping cold-start I/O manageable.
        const CONCURRENCY: usize = 64;

        // Pre-build (id, key) pairs with owned data so the per-future async
        // blocks are 'static and work cleanly with buffer_unordered.
        let keyed: Vec<(String, Vec<u8>)> = ids
            .iter()
            .map(|id| (id.clone(), job_info_key(tenant, id)))
            .collect();
        let db = Arc::clone(&self.db);

        let results: Vec<_> = futures::stream::iter(keyed.into_iter().map(|(id, key)| {
            let db = Arc::clone(&db);
            async move {
                let maybe_raw = db.get(&key).await?;
                Ok::<_, JobStoreShardError>((id, maybe_raw))
            }
        }))
        .buffer_unordered(CONCURRENCY)
        .collect()
        .await;

        let mut map = HashMap::with_capacity(ids.len());
        for item in results {
            let (id, maybe_raw) = item?;
            if let Some(raw) = maybe_raw {
                map.insert(id, JobView::new(raw)?);
            }
        }
        Ok(map)
    }

    /// Fetch a job status by id as a zero-copy archived view.
    pub async fn get_job_status(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<JobStatus>, JobStoreShardError> {
        let key = job_status_key(tenant, id);
        let maybe_raw = self.db.get(&key).await?;
        let Some(raw) = maybe_raw else {
            return Ok(None);
        };

        Ok(Some(helpers::decode_job_status_owned(&raw)?))
    }

    /// Fetch multiple job statuses by id concurrently. Returns a map of job_id -> JobStatus.
    /// Jobs that don't exist will not be present in the result map.
    pub async fn get_jobs_status_batch(
        &self,
        tenant: &str,
        ids: &[String],
    ) -> Result<std::collections::HashMap<String, JobStatus>, JobStoreShardError> {
        use futures::StreamExt;
        use std::collections::HashMap;

        const CONCURRENCY: usize = 64;

        let keyed: Vec<(String, Vec<u8>)> = ids
            .iter()
            .map(|id| (id.clone(), job_status_key(tenant, id)))
            .collect();
        let db = Arc::clone(&self.db);

        let results: Vec<_> = futures::stream::iter(keyed.into_iter().map(|(id, key)| {
            let db = Arc::clone(&db);
            async move {
                let maybe_raw = db.get(&key).await?;
                Ok::<_, JobStoreShardError>((id, maybe_raw))
            }
        }))
        .buffer_unordered(CONCURRENCY)
        .collect()
        .await;

        let mut map = HashMap::with_capacity(ids.len());
        for item in results {
            let (id, maybe_raw) = item?;
            if let Some(raw) = maybe_raw {
                map.insert(id, helpers::decode_job_status_owned(&raw)?);
            }
        }
        Ok(map)
    }

    /// Fetch a job attempt by job id and attempt number.
    pub async fn get_job_attempt(
        &self,
        tenant: &str,
        job_id: &str,
        attempt_number: u32,
    ) -> Result<Option<JobAttemptView>, JobStoreShardError> {
        let key = attempt_key(tenant, job_id, attempt_number);
        match self.db.get(&key).await? {
            Some(raw) => Ok(Some(JobAttemptView::new(raw)?)),
            None => Ok(None),
        }
    }

    /// Fetch all attempts for a job, ordered by attempt number (ascending).
    /// Returns an empty vector if the job has no attempts.
    pub async fn get_job_attempts(
        &self,
        tenant: &str,
        job_id: &str,
    ) -> Result<Vec<JobAttemptView>, JobStoreShardError> {
        let start = crate::keys::attempt_prefix(tenant, job_id);
        let end = crate::keys::end_bound(&start);

        let mut iter = self.db.scan::<Vec<u8>, _>(start..end).await?;
        let mut attempts = Vec::new();

        while let Some(kv) = iter.next().await? {
            let view = JobAttemptView::new(kv.value.clone())?;
            attempts.push(view);
        }

        Ok(attempts)
    }

    /// Get the latest (highest numbered) attempt for a job, if any.
    /// This is typically the attempt that contains the final result for completed jobs.
    pub async fn get_latest_job_attempt(
        &self,
        tenant: &str,
        job_id: &str,
    ) -> Result<Option<JobAttemptView>, JobStoreShardError> {
        let attempts = self.get_job_attempts(tenant, job_id).await?;
        Ok(attempts.into_iter().last())
    }

    /// Delete a job by id.
    ///
    /// Returns an error if the job is currently running (has active leases/holders) or has pending tasks/requests. Jobs must finish or permanently fail before deletion.
    pub async fn delete_job(&self, tenant: &str, id: &str) -> Result<(), JobStoreShardError> {
        use crate::keys::idx_metadata_key;
        use slatedb::WriteBatch;

        // Check if job is running or has pending state
        let status = self.get_job_status(tenant, id).await?;
        if let Some(ref status) = status
            && !status.is_terminal()
        {
            return Err(JobStoreShardError::JobInProgress(id.to_string()));
        }

        // Check if job even exists (status.is_none() means job doesn't exist)
        // If job doesn't exist, just return Ok (idempotent delete)
        let job_info_key_bytes = job_info_key(tenant, id);
        if status.is_none() && self.db.get(&job_info_key_bytes).await?.is_none() {
            return Ok(());
        }

        let job_status_key_bytes = job_status_key(tenant, id);
        let mut batch = WriteBatch::new();
        // Clean up secondary index entries if present
        if let Some(ref status) = status {
            let timek = crate::keys::idx_status_time_key(
                tenant,
                status.kind.as_str(),
                status.changed_at_ms,
                id,
            );
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
        batch.delete(crate::keys::job_cancelled_key(tenant, id));

        // Update counters in the same batch
        let mut writer = DbWriteBatcher::new(&self.db, &mut batch);
        self.decrement_total_jobs_counter(&mut writer)?;
        if let Some(ref status) = status {
            // Job was in terminal state (we already checked it's terminal above)
            self.decrement_completed_jobs_counter(&mut writer)?;
            // Decrement tenant status counter
            let counter_key = crate::keys::tenant_status_counter_key(tenant, status.kind.as_str());
            writer.merge(
                &counter_key,
                crate::job_store_shard::counters::encode_counter(-1),
            )?;
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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{JobStatusKind, compute_terminal_expire_ts, expand_slatedb_settings_for_shard};

    #[test]
    fn terminal_expire_ts_succeeded_uses_completed_expire_setting() {
        let ts = compute_terminal_expire_ts(JobStatusKind::Succeeded, 1_000, Some(60), Some(30));
        assert_eq!(ts, Some(1_000 + 60_000));
    }

    #[test]
    fn terminal_expire_ts_failed_uses_terminal_expire_setting() {
        let ts = compute_terminal_expire_ts(JobStatusKind::Failed, 1_000, Some(60), Some(30));
        assert_eq!(ts, Some(1_000 + 30_000));
    }

    #[test]
    fn terminal_expire_ts_cancelled_uses_terminal_expire_setting() {
        let ts = compute_terminal_expire_ts(JobStatusKind::Cancelled, 1_000, Some(60), Some(30));
        assert_eq!(ts, Some(1_000 + 30_000));
    }

    #[test]
    fn terminal_expire_ts_returns_none_for_scheduled_even_when_both_settings_set() {
        // Live jobs (Scheduled/Running) must never carry a TTL: they're still
        // reachable and tagging them would let SlateDB drop the row out from
        // under an active worker.
        let ts = compute_terminal_expire_ts(JobStatusKind::Scheduled, 1_000, Some(60), Some(30));
        assert_eq!(ts, None);
    }

    #[test]
    fn terminal_expire_ts_returns_none_for_running_even_when_both_settings_set() {
        let ts = compute_terminal_expire_ts(JobStatusKind::Running, 1_000, Some(60), Some(30));
        assert_eq!(ts, None);
    }

    #[test]
    fn terminal_expire_ts_returns_none_when_succeeded_setting_unset() {
        let ts = compute_terminal_expire_ts(JobStatusKind::Succeeded, 1_000, None, Some(30));
        assert_eq!(ts, None);
    }

    #[test]
    fn terminal_expire_ts_returns_none_when_terminal_setting_unset() {
        let ts = compute_terminal_expire_ts(JobStatusKind::Failed, 1_000, Some(60), None);
        assert_eq!(ts, None);
    }

    #[test]
    fn terminal_expire_ts_saturates_on_overflowing_seconds() {
        let huge = u64::MAX;
        let ts = compute_terminal_expire_ts(JobStatusKind::Failed, 1_000, None, Some(huge));
        // `u64::MAX * 1000` overflows; the cast saturates to `i64::MAX`.
        // Then `1_000 + i64::MAX` saturates again at `i64::MAX`.
        assert_eq!(ts, Some(i64::MAX));
    }

    #[test]
    fn terminal_expire_ts_saturates_when_now_ms_is_near_max() {
        // Sane seconds; `now_ms` near i64::MAX. Add must saturate, not wrap negative.
        let ts = compute_terminal_expire_ts(JobStatusKind::Failed, i64::MAX - 5, None, Some(60));
        assert_eq!(ts, Some(i64::MAX));
    }

    #[test]
    fn expands_shard_placeholder_in_slatedb_cache_root_folder() {
        let settings = slatedb::config::Settings {
            object_store_cache_options: slatedb::config::ObjectStoreCacheOptions {
                root_folder: Some("/var/silo-cache/%shard%".into()),
                ..Default::default()
            },
            ..Default::default()
        };

        let settings = expand_slatedb_settings_for_shard(settings, "shard-123");

        assert_eq!(
            settings.object_store_cache_options.root_folder.as_deref(),
            Some(std::path::Path::new("/var/silo-cache/shard-123"))
        );
    }

    #[test]
    fn expands_braced_shard_placeholder_in_slatedb_cache_root_folder() {
        let settings = slatedb::config::Settings {
            object_store_cache_options: slatedb::config::ObjectStoreCacheOptions {
                root_folder: Some("/var/silo-cache/{shard}".into()),
                ..Default::default()
            },
            ..Default::default()
        };

        let settings = expand_slatedb_settings_for_shard(settings, "shard-123");

        assert_eq!(
            settings.object_store_cache_options.root_folder.as_deref(),
            Some(std::path::Path::new("/var/silo-cache/shard-123"))
        );
    }

    #[test]
    fn leaves_slatedb_cache_root_folder_without_placeholder_unchanged() {
        let settings = slatedb::config::Settings {
            object_store_cache_options: slatedb::config::ObjectStoreCacheOptions {
                root_folder: Some("/var/silo-cache".into()),
                ..Default::default()
            },
            ..Default::default()
        };

        let settings = expand_slatedb_settings_for_shard(settings, "shard-123");

        assert_eq!(
            settings.object_store_cache_options.root_folder.as_deref(),
            Some(std::path::Path::new("/var/silo-cache"))
        );
    }
}
