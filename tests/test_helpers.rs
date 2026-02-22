#![allow(dead_code)]

use silo::gubernator::{MockGubernatorClient, RateLimitClient};
use silo::job_store_shard::{JobStoreShard, OpenShardOptions};
use silo::settings::{Backend, DatabaseConfig};
use silo::shard_range::ShardRange;
use silo::storage::resolve_object_store;
use slatedb::{Db, DbIterator};
use std::sync::Arc;
use std::time::Duration;

/// Create SlateDB settings with a fast flush interval for tests
pub fn fast_flush_slatedb_settings() -> slatedb::config::Settings {
    slatedb::config::Settings {
        flush_interval: Some(Duration::from_millis(10)),
        ..Default::default()
    }
}

// Helper: enforce a tight timeout for async tests likely to hang
#[macro_export]
macro_rules! with_timeout {
    ($ms:expr, $body:block) => {{
        tokio::time::timeout(std::time::Duration::from_millis($ms), async move { $body })
            .await
            .expect("test timed out")
    }};
}

pub fn parse_time_from_task_key(key: &str) -> Option<u64> {
    // Legacy: Accept both legacy and tenant-aware formats:
    // - tasks/{task_group}/{:020}/{:02}/{job_id}/{attempt}
    // - t/<tenant>/tasks/{task_group}/{:020}/{:02}/{job_id}/{attempt}
    if let Some(pos) = key.find("tasks/") {
        let after = &key[pos + "tasks/".len()..];
        // Skip the task_group part, then get the timestamp
        let parts: Vec<&str> = after.split('/').collect();
        if parts.len() >= 2 {
            // parts[0] = task_group, parts[1] = timestamp
            return parts[1].parse::<u64>().ok();
        }
    }
    None
}

/// Parse start time from a binary-encoded task key using storekey.
pub fn parse_time_from_binary_task_key(key: &[u8]) -> Option<u64> {
    let parsed = silo::keys::parse_task_key(key)?;
    Some(parsed.start_time_ms)
}

pub async fn open_temp_shard() -> (tempfile::TempDir, std::sync::Arc<JobStoreShard>) {
    let rate_limiter = MockGubernatorClient::new_arc();
    open_temp_shard_with_rate_limiter(rate_limiter).await
}

/// Open a temp shard with a custom periodic concurrency reconciliation interval.
pub async fn open_temp_shard_with_reconcile_interval_ms(
    interval_ms: u64,
) -> (tempfile::TempDir, std::sync::Arc<JobStoreShard>) {
    let rate_limiter = MockGubernatorClient::new_arc();
    let tmp = tempfile::tempdir().unwrap();
    let resolved = resolve_object_store(&Backend::Fs, tmp.path().to_string_lossy().as_ref())
        .expect("resolve fs object store");
    let shard = JobStoreShard::open_with_resolved_store(
        "test".to_string(),
        &resolved.canonical_path,
        OpenShardOptions {
            store: resolved.store,
            wal_store: None,
            wal_close_config: None,
            slatedb_settings: Some(fast_flush_slatedb_settings()),
            rate_limiter,
            metrics: None,
            concurrency_reconcile_interval: Duration::from_millis(interval_ms.max(1)),
        },
        ShardRange::full(),
    )
    .await
    .expect("open shard");
    (tmp, shard)
}

/// Open a temp shard with a specific range (useful for testing split-aware behavior)
pub async fn open_temp_shard_with_range(
    range: ShardRange,
) -> (tempfile::TempDir, std::sync::Arc<JobStoreShard>) {
    let rate_limiter = MockGubernatorClient::new_arc();
    let tmp = tempfile::tempdir().unwrap();
    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
        wal: None,
        apply_wal_on_close: true,
        slatedb: Some(fast_flush_slatedb_settings()),
    };
    let shard = JobStoreShard::open(&cfg, rate_limiter, None, range)
        .await
        .expect("open shard");
    (tmp, shard)
}

/// Open a temp shard with a custom rate limiter (useful for testing rate limit behavior)
pub async fn open_temp_shard_with_rate_limiter(
    rate_limiter: Arc<dyn RateLimitClient>,
) -> (tempfile::TempDir, std::sync::Arc<JobStoreShard>) {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs,
        path: tmp.path().to_string_lossy().to_string(),
        wal: None,
        apply_wal_on_close: true,
        // Use fast flush interval for tests to speed them up
        slatedb: Some(fast_flush_slatedb_settings()),
    };
    let shard = JobStoreShard::open(&cfg, rate_limiter, None, ShardRange::full())
        .await
        .expect("open shard");
    (tmp, shard)
}

/// Configuration for opening a shard with separate WAL storage
pub struct WalShardConfig {
    pub data_dir: tempfile::TempDir,
    pub wal_dir: tempfile::TempDir,
    pub apply_wal_on_close: bool,
}

/// Open a temp shard with a separate local WAL directory (for testing WAL flush behavior)
pub async fn open_temp_shard_with_local_wal(
    flush_on_close: bool,
) -> (WalShardConfig, std::sync::Arc<JobStoreShard>) {
    let rate_limiter = MockGubernatorClient::new_arc();
    open_temp_shard_with_local_wal_and_rate_limiter(rate_limiter, flush_on_close).await
}

/// Open a temp shard with separate local WAL directory and custom rate limiter
pub async fn open_temp_shard_with_local_wal_and_rate_limiter(
    rate_limiter: Arc<dyn RateLimitClient>,
    flush_on_close: bool,
) -> (WalShardConfig, std::sync::Arc<JobStoreShard>) {
    use silo::settings::WalConfig;

    let data_dir = tempfile::tempdir().unwrap();
    let wal_dir = tempfile::tempdir().unwrap();

    let cfg = DatabaseConfig {
        name: "test".to_string(),
        backend: Backend::Fs, // Main data backend - simulates object storage (could be s3 in prod)
        path: data_dir.path().to_string_lossy().to_string(),
        wal: Some(WalConfig {
            backend: Backend::Fs, // Local WAL
            path: wal_dir.path().to_string_lossy().to_string(),
        }),
        apply_wal_on_close: flush_on_close,
        slatedb: Some(fast_flush_slatedb_settings()),
    };

    let shard = JobStoreShard::open(&cfg, rate_limiter, None, ShardRange::full())
        .await
        .expect("open shard with local WAL");

    let config = WalShardConfig {
        data_dir,
        wal_dir,
        apply_wal_on_close: flush_on_close,
    };

    (config, shard)
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub async fn first_kv_with_prefix(db: &Db, prefix: &str) -> Option<(String, bytes::Bytes)> {
    let start: Vec<u8> = prefix.as_bytes().to_vec();
    let mut end: Vec<u8> = prefix.to_string().into_bytes();
    end.push(0xFF);
    let mut iter: DbIterator = db.scan::<Vec<u8>, _>(start..=end).await.ok()?;
    let first = iter.next().await.ok()?;
    first.map(|kv| (String::from_utf8_lossy(&kv.key).to_string(), kv.value))
}

/// Get first key/value with a binary prefix (for storekey-encoded keys).
pub async fn first_kv_with_binary_prefix(
    db: &Db,
    prefix: &[u8],
) -> Option<(Vec<u8>, bytes::Bytes)> {
    let start = prefix.to_vec();
    let end = silo::keys::end_bound(&start);
    let mut iter: DbIterator = db.scan::<Vec<u8>, _>(start..end).await.ok()?;
    let first = iter.next().await.ok()?;
    first.map(|kv| (kv.key.to_vec(), kv.value))
}

/// Get first task key/value (0x05 prefix).
pub async fn first_task_kv(db: &Db) -> Option<(Vec<u8>, bytes::Bytes)> {
    first_kv_with_binary_prefix(db, &silo::keys::tasks_prefix()).await
}

/// Get first lease key/value (0x06 prefix).
pub async fn first_lease_kv(db: &Db) -> Option<(Vec<u8>, bytes::Bytes)> {
    first_kv_with_binary_prefix(db, &silo::keys::leases_prefix()).await
}

/// Get first concurrency request key/value (0x08 prefix).
pub async fn first_concurrency_request_kv(db: &Db) -> Option<(Vec<u8>, bytes::Bytes)> {
    first_kv_with_binary_prefix(db, &silo::keys::concurrency_requests_prefix()).await
}

pub async fn count_tasks_before(db: &Db, cutoff_ms: i64) -> usize {
    let start: Vec<u8> = b"tasks/".to_vec();
    let mut end: Vec<u8> = b"tasks/".to_vec();
    end.push(0xFF);
    let mut iter: DbIterator = db.scan::<Vec<u8>, _>(start..=end).await.unwrap();
    let mut count = 0usize;
    loop {
        let maybe = iter.next().await.unwrap();
        let Some(kv) = maybe else { break };
        let key_str = String::from_utf8_lossy(&kv.key).to_string();
        if let Some(ts) = parse_time_from_task_key(&key_str) {
            if (ts as i64) < cutoff_ms {
                count += 1;
            }
        }
    }
    count
}

pub async fn count_with_prefix(db: &Db, prefix: &str) -> usize {
    let start: Vec<u8> = prefix.as_bytes().to_vec();
    let mut end: Vec<u8> = prefix.to_string().into_bytes();
    end.push(0xFF);
    let mut iter: DbIterator = db.scan::<Vec<u8>, _>(start..=end).await.unwrap();
    let mut count = 0usize;
    loop {
        let maybe = iter.next().await.unwrap();
        if maybe.is_none() {
            break;
        }
        count += 1;
    }
    count
}

/// Count keys with a binary prefix (for use with storekey-encoded keys).
pub async fn count_with_binary_prefix(db: &Db, prefix: &[u8]) -> usize {
    let start = prefix.to_vec();
    let end = silo::keys::end_bound(&start);
    let mut iter: DbIterator = db.scan::<Vec<u8>, _>(start..end).await.unwrap();
    let mut count = 0usize;
    loop {
        let maybe = iter.next().await.unwrap();
        if maybe.is_none() {
            break;
        }
        count += 1;
    }
    count
}

/// Count job info keys (0x01 prefix).
pub async fn count_job_info_keys(db: &Db) -> usize {
    count_with_binary_prefix(db, &silo::keys::jobs_prefix()).await
}

/// Count job info keys for a specific tenant.
pub async fn count_job_info_keys_for_tenant(db: &Db, tenant: &str) -> usize {
    count_with_binary_prefix(db, &silo::keys::job_info_prefix(tenant)).await
}

/// Count job status keys (0x02 prefix).
pub async fn count_job_status_keys(db: &Db) -> usize {
    count_with_binary_prefix(db, &[0x02]).await
}

/// Count status/time index keys (0x03 prefix).
pub async fn count_status_time_index_keys(db: &Db) -> usize {
    count_with_binary_prefix(db, &[0x03]).await
}

/// Count task keys (0x05 prefix).
pub async fn count_task_keys(db: &Db) -> usize {
    count_with_binary_prefix(db, &silo::keys::tasks_prefix()).await
}

/// Count lease keys (0x06 prefix).
pub async fn count_lease_keys(db: &Db) -> usize {
    count_with_binary_prefix(db, &silo::keys::leases_prefix()).await
}

/// Count concurrency request keys (0x08 prefix).
pub async fn count_concurrency_requests(db: &Db) -> usize {
    count_with_binary_prefix(db, &silo::keys::concurrency_requests_prefix()).await
}

/// Count concurrency holder keys (0x09 prefix).
pub async fn count_concurrency_holders(db: &Db) -> usize {
    count_with_binary_prefix(db, &silo::keys::concurrency_holders_prefix()).await
}

/// Count concurrency holder keys for a specific tenant.
pub async fn count_concurrency_holders_for_tenant(db: &Db, tenant: &str) -> usize {
    count_with_binary_prefix(db, &silo::keys::concurrency_holders_tenant_prefix(tenant)).await
}

/// Encode a JSON value as MessagePack bytes for use as a job payload.
/// This is a helper for tests to create MessagePack-encoded payloads.
pub fn msgpack_payload(value: &serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec(value).expect("failed to encode payload as messagepack")
}

/// Poll an async function until the predicate returns true, or timeout.
/// Returns the last value produced by `f`.
pub async fn poll_until<F, Fut, T, P>(mut f: F, predicate: P, timeout_ms: u64) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = T>,
    P: Fn(&T) -> bool,
{
    let start = std::time::Instant::now();
    loop {
        let value = f().await;
        if predicate(&value) {
            return value;
        }
        if start.elapsed() > Duration::from_millis(timeout_ms) {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
