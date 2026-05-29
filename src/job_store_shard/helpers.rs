//! Helper functions shared across job_store_shard submodules.
use slatedb::WriteBatch;
use slatedb::bytes::Bytes;
use slatedb::config::{PutOptions, Ttl};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::codec::encode_task;
use crate::instrumented_db::{InstrumentedDb, InstrumentedDbIterator, InstrumentedDbTransaction};
use crate::job::JobStatus;
use crate::job_store_shard::JobStoreShardError;
use crate::keys::{end_bound, task_key};
use crate::task::Task;

/// A trait that abstracts over SlateDB's two ways of writing in groups: `WriteBatch` and `DbTransaction`.
/// `WriteBatch` doesn't track transaction conflicts and is a bit faster, but doesn't support read-modify-write operations.
/// `DbTransaction` tracks transaction conflicts and is a bit slower, but supports read-modify-write operations, and may require retries on conflict.
///
/// We use this trait to allow functions to be generic over both targets, avoiding code duplication between batch-based and transaction-based code paths.
///
/// For batch operations, reads come from the underlying `Db` while writes go to the `WriteBatch`.
/// For transaction operations, both reads and writes go through the `DbTransaction`.
///
pub(crate) trait WriteBatcher {
    /// Put a key-value pair.
    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<(), slatedb::Error>;

    /// Put a key-value pair with a SlateDB row TTL set to `expire_ts`
    /// (epoch milliseconds). Used by the terminal-job expiration path so the
    /// row is dropped during compaction once it ages past `expire_ts`.
    ///
    /// No default impl — every implementor must thread the TTL through to
    /// the underlying writer. A silent fallback that drops the TTL would
    /// produce data that never expires without any compile-time signal, so
    /// the trait forces every writer to decide explicitly.
    fn put_with_expire<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
        expire_ts: i64,
    ) -> Result<(), slatedb::Error>;

    /// Delete a key.
    fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), slatedb::Error>;

    /// Merge a value into a key (used for counter operations).
    ///
    /// For batches, this simply adds a merge to the batch.
    /// For transactions, this merges and then calls `unmark_write` to exclude the key
    /// from conflict detection, since counter keys are global shard-level keys that would
    /// otherwise cause excessive transaction conflicts under concurrent load.
    fn merge<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<(), slatedb::Error>;

    /// Get a value by key.
    ///
    /// For transactions, this reads from the transaction snapshot and tracks
    /// the read key in the SSI read set (so a concurrent writer to the same
    /// key trips a read-write conflict at commit).
    /// For batches, this reads from the underlying database.
    fn get(
        &self,
        key: &[u8],
    ) -> impl std::future::Future<Output = Result<Option<Bytes>, slatedb::Error>> + Send;

    /// Scan all keys with the given prefix.
    ///
    /// For transactions, this reads from the transaction snapshot and tracks
    /// the scanned range in the SSI read set (so a concurrent writer that
    /// inserts a key into the range trips a phantom-read conflict at commit).
    /// For batches, this reads from the underlying database.
    fn scan_prefix(
        &self,
        prefix: &[u8],
    ) -> impl std::future::Future<Output = Result<InstrumentedDbIterator, slatedb::Error>> + Send;
}

/// Wrapper around `&mut WriteBatch` that implements `WriteBatcher`
///
/// This combines a database reference for reads with a write batch for writes, allowing batch-based code paths to use the same trait as transaction-based ones.
pub(crate) struct DbWriteBatcher<'a> {
    pub db: &'a InstrumentedDb,
    pub batch: &'a mut WriteBatch,
}

impl<'a> DbWriteBatcher<'a> {
    pub fn new(db: &'a InstrumentedDb, batch: &'a mut WriteBatch) -> Self {
        Self { db, batch }
    }
}

impl WriteBatcher for DbWriteBatcher<'_> {
    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<(), slatedb::Error> {
        self.batch.put(key, value);
        Ok(())
    }

    fn put_with_expire<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
        expire_ts: i64,
    ) -> Result<(), slatedb::Error> {
        self.batch.put_with_options(
            key,
            value,
            &PutOptions {
                ttl: Ttl::ExpireAt(expire_ts),
            },
        );
        Ok(())
    }

    fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), slatedb::Error> {
        self.batch.delete(key);
        Ok(())
    }

    fn merge<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<(), slatedb::Error> {
        self.batch.merge(key, value);
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, slatedb::Error> {
        self.db.get(key).await
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<InstrumentedDbIterator, slatedb::Error> {
        let end = end_bound(prefix);
        self.db.scan::<Vec<u8>, _>(prefix.to_vec()..end).await
    }
}

/// Wrapper around `&InstrumentedDbTransaction` that implements `WriteBatcher`.
///
/// This wrapper is needed because transaction methods take `&self` (interior mutability)
/// while `WriteBatch` methods take `&mut self`. The wrapper allows us to have a uniform
/// `&mut self` interface in the `WriteBatcher` trait.
pub(crate) struct TxnWriter<'a>(pub &'a InstrumentedDbTransaction);

impl WriteBatcher for TxnWriter<'_> {
    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<(), slatedb::Error> {
        self.0.put(key, value)
    }

    fn put_with_expire<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
        expire_ts: i64,
    ) -> Result<(), slatedb::Error> {
        self.0.put_with_options(
            key,
            value,
            &PutOptions {
                ttl: Ttl::ExpireAt(expire_ts),
            },
        )
    }

    fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), slatedb::Error> {
        self.0.delete(key)
    }

    fn merge<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<(), slatedb::Error> {
        let key_ref = key.as_ref();
        self.0.merge(key_ref, value)?;
        self.0.unmark_write([key_ref])?;
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, slatedb::Error> {
        self.0.get(key).await
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<InstrumentedDbIterator, slatedb::Error> {
        let end = end_bound(prefix);
        self.0.scan::<Vec<u8>, _>(prefix.to_vec()..end).await
    }
}

/// Put `value` at `key` through the transaction, applying a SlateDB row TTL
/// expiring at `expire_ts` (epoch ms) when `expire_ts` is `Some`, and a plain
/// put otherwise.
///
/// Centralizes the terminal-vs-non-terminal put branch used across the import
/// and cancel paths, where every record written in the txn must pick up the
/// row TTL iff the job lands in a terminal status.
pub(crate) fn put_with_optional_expire(
    txn: &InstrumentedDbTransaction,
    key: impl AsRef<[u8]>,
    value: impl AsRef<[u8]>,
    expire_ts: Option<i64>,
) -> Result<(), slatedb::Error> {
    match expire_ts {
        Some(ts) => txn.put_with_options(
            key,
            value,
            &PutOptions {
                ttl: Ttl::ExpireAt(ts),
            },
        ),
        None => txn.put(key, value),
    }
}

/// Get current epoch time in milliseconds.
///
/// Uses `std::time::SystemTime::now()` which returns real wall-clock time in production.
/// For deterministic simulation testing with turmoil, use the `mad-turmoil` crate which
/// overrides `clock_gettime` at the libc level to return turmoil's simulated time.
pub fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Encode and write a task to a batch or transaction at the standard task key location.
pub(crate) fn put_task<W: WriteBatcher>(
    writer: &mut W,
    task_group: &str,
    time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt: u32,
    task: &Task,
) -> Result<(), JobStoreShardError> {
    let task_value = encode_task(task);
    writer.put(
        task_key(task_group, time_ms, priority, job_id, attempt),
        &task_value,
    )?;
    Ok(())
}

/// Retry an operation that may fail with a SlateDB transaction conflict.
///
/// Encapsulates the MAX_RETRIES=5 loop with exponential backoff and
/// `TransactionConflict` error return on exhaustion.
///
/// Also retries on SlateDB `InvalidClockTick` errors, which occur under heavy
/// concurrent write load due to a TOCTOU race in SlateDB's MonotonicClock: one
/// thread reads the wall-clock, gets preempted, another thread commits a later
/// tick, then the first thread's earlier tick is rejected. These are transient
/// and resolve on retry once the clock advances.
pub(crate) async fn retry_on_txn_conflict<T, F, Fut>(
    operation_name: &str,
    mut f: F,
) -> Result<T, JobStoreShardError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, JobStoreShardError>>,
{
    const MAX_RETRIES: usize = 5;

    for attempt in 0..MAX_RETRIES {
        match f().await {
            Ok(val) => return Ok(val),
            Err(JobStoreShardError::Slate(ref e))
                if e.kind() == slatedb::ErrorKind::Transaction || is_clock_tick_error(e) =>
            {
                if attempt + 1 < MAX_RETRIES {
                    let delay_ms = 10 * (1 << attempt); // 10ms, 20ms, 40ms, 80ms
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    tracing::debug!(
                        operation = %operation_name,
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
        operation_name.to_string(),
    ))
}

/// Check if a SlateDB error is an InvalidClockTick error.
///
/// SlateDB maps InvalidClockTick to ErrorKind::Invalid (a broad category), so we
/// check the error message to identify this specific transient error.
fn is_clock_tick_error(e: &slatedb::Error) -> bool {
    e.kind() == slatedb::ErrorKind::Invalid && e.to_string().contains("invalid clock tick")
}

/// Load a `JobView` for the given tenant/job from a transaction or DB reader.
///
/// Returns `JobNotFound` if the job info key doesn't exist.
pub(crate) async fn load_job_view(
    reader: &impl WriteBatcher,
    tenant: &str,
    id: &str,
) -> Result<crate::job::JobView, JobStoreShardError> {
    let job_info_key = crate::keys::job_info_key(tenant, id);
    let maybe_job_raw = reader.get(&job_info_key).await?;
    let Some(job_raw) = maybe_job_raw else {
        return Err(JobStoreShardError::JobNotFound(id.to_string()));
    };
    crate::job::JobView::new(job_raw)
}

/// Decode a `JobStatus` from raw bytes into an owned value.
pub(crate) fn decode_job_status_owned(raw: &[u8]) -> Result<JobStatus, JobStoreShardError> {
    Ok(crate::codec::decode_job_status_owned(raw)?)
}
