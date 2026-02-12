//! Helper functions shared across job_store_shard submodules.
use rkyv::Deserialize as RkyvDeserialize;
use slatedb::bytes::Bytes;
use slatedb::{Db, DbTransaction, WriteBatch};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::codec::{decode_job_status, encode_task};
use crate::job::JobStatus;
use crate::job_store_shard::JobStoreShardError;
use crate::keys::task_key;
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
    /// For transactions, this reads from the transaction snapshot.
    /// For batches, this reads from the underlying database.
    fn get(
        &self,
        key: &[u8],
    ) -> impl std::future::Future<Output = Result<Option<Bytes>, slatedb::Error>> + Send;
}

/// Wrapper around `&mut WriteBatch` that implements `WriteBatcher`
///
/// This combines a database reference for reads with a write batch for writes, allowing batch-based code paths to use the same trait as transaction-based ones.
pub(crate) struct DbWriteBatcher<'a> {
    pub db: &'a Db,
    pub batch: &'a mut WriteBatch,
}

impl<'a> DbWriteBatcher<'a> {
    pub fn new(db: &'a Db, batch: &'a mut WriteBatch) -> Self {
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
}

/// Wrapper around `&DbTransaction` that implements `WriteBatcher`.
///
/// This wrapper is needed because `DbTransaction` methods take `&self` (interior mutability)
/// while `WriteBatch` methods take `&mut self`. The wrapper allows us to have a uniform
/// `&mut self` interface in the `WriteBatcher` trait.
pub(crate) struct TxnWriter<'a>(pub &'a DbTransaction);

impl WriteBatcher for TxnWriter<'_> {
    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<(), slatedb::Error> {
        self.0.put(key, value)
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
    let task_value = encode_task(task)?;
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
                if e.kind() == slatedb::ErrorKind::Transaction =>
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

/// Decode a `JobStatus` from raw rkyv bytes into an owned value.
pub(crate) fn decode_job_status_owned(raw: &[u8]) -> Result<JobStatus, JobStoreShardError> {
    let decoded = decode_job_status(raw)?;
    let mut des = rkyv::Infallible;
    Ok(RkyvDeserialize::deserialize(decoded.archived(), &mut des)
        .unwrap_or_else(|_| unreachable!("infallible deserialization for JobStatus")))
}
