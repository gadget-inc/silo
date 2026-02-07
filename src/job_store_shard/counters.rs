//! Shard-level job counters for high-performance job counting.
//!
//! These counters track:
//! - `total_jobs`: Count of all jobs that exist in the shard (not deleted)
//! - `completed_jobs`: Count of jobs that have reached a terminal state
//!   (Succeeded, Failed, or Cancelled)
//!
//! Counters use SlateDB's MergeOperator to avoid read-modify-write cycles. Instead of reading
//! the current value, modifying it, and writing it back, we write a delta (+1 or -1) that gets
//! merged at read time.
//!
//! ## Why counter operations are non-transactional
//!
//! Counter modifications are done as separate non-transactional writes rather than being included
//! in the main operation's transaction. This is because:
//!
//! 1. SlateDB's SerializableSnapshot transactions track all keys that are read or written
//! 2. Counter keys are global shard-level keys (single key per counter)
//! 3. If counters were in transactions, ALL concurrent operations would conflict on these keys
//! 4. This would cause excessive transaction retries under load (e.g., 20 concurrent enqueues)
//!
//! The counters don't need transactional guarantees because:
//! - They're monotonic operations (increment/decrement)
//! - They happen after the main transaction commits successfully
//! - Eventual consistency is acceptable for counter values
//!
//! TODO: Once SlateDB supports excluding specific keys from conflict detection
//! (https://github.com/slatedb/slatedb/issues/1254), we can move counter operations back
//! inside transactions for better atomicity guarantees.

use std::sync::Arc;

use slatedb::bytes::Bytes;
use slatedb::{MergeOperator, MergeOperatorError, WriteBatch};

use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{shard_completed_jobs_counter_key, shard_total_jobs_counter_key};

/// Shard job counters returned by get_counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShardCounters {
    /// Total number of jobs in the shard (not deleted).
    pub total_jobs: i64,
    /// Number of jobs in terminal states (Succeeded, Failed, Cancelled).
    pub completed_jobs: i64,
}

impl ShardCounters {
    /// Calculate the number of open (non-terminal) jobs.
    pub fn open_jobs(&self) -> i64 {
        self.total_jobs.saturating_sub(self.completed_jobs)
    }
}

/// MergeOperator for counter keys that sums i64 deltas.
///
/// This operator treats both the existing value and operands as little-endian i64s
/// and sums them together. This allows counters to be incremented/decremented
/// without reading the current value first.
#[derive(Debug)]
pub struct CounterMergeOperator;

impl MergeOperator for CounterMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let existing = existing_value.map(|v| decode_counter(&v)).unwrap_or(0);
        let delta = decode_counter(&operand);
        let new_value = existing.saturating_add(delta);
        Ok(Bytes::copy_from_slice(&encode_counter(new_value)))
    }

    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        let mut total = existing_value.map(|v| decode_counter(&v)).unwrap_or(0);

        for operand in operands {
            let delta = decode_counter(operand);
            total = total.saturating_add(delta);
        }

        Ok(Bytes::copy_from_slice(&encode_counter(total)))
    }
}

/// Create an Arc'd CounterMergeOperator for use with DbBuilder.
pub fn counter_merge_operator() -> Arc<CounterMergeOperator> {
    Arc::new(CounterMergeOperator)
}

/// Helper to encode an i64 counter value as bytes.
pub(crate) fn encode_counter(value: i64) -> [u8; 8] {
    value.to_le_bytes()
}

/// Helper to decode an i64 counter value from bytes.
pub(crate) fn decode_counter(bytes: &[u8]) -> i64 {
    if bytes.len() >= 8 {
        let arr: [u8; 8] = bytes[..8].try_into().unwrap_or([0; 8]);
        i64::from_le_bytes(arr)
    } else {
        0
    }
}

impl JobStoreShard {
    /// Get the current job counters for this shard.
    pub async fn get_counters(&self) -> Result<ShardCounters, JobStoreShardError> {
        let total_key = shard_total_jobs_counter_key();
        let completed_key = shard_completed_jobs_counter_key();

        let total_jobs = match self.db.get(&total_key).await? {
            Some(bytes) => decode_counter(&bytes),
            None => 0,
        };

        let completed_jobs = match self.db.get(&completed_key).await? {
            Some(bytes) => decode_counter(&bytes),
            None => 0,
        };

        Ok(ShardCounters {
            total_jobs,
            completed_jobs,
        })
    }

    // These add counter merges to an existing WriteBatch, avoiding the overhead
    // of a separate database write. Use these when the caller already has a
    // WriteBatch that will be written atomically.
    //
    // IMPORTANT: Do NOT use these inside DbTransaction paths — counter keys are
    // global shard-level keys and will cause transaction conflicts under
    // concurrent load. Use the standalone (non-batch) variants below instead.

    /// Increment the total jobs counter within an existing WriteBatch.
    pub(crate) fn increment_total_jobs_counter_batch(&self, batch: &mut WriteBatch) {
        let key = shard_total_jobs_counter_key();
        batch.merge(&key, encode_counter(1));
    }

    /// Decrement the total jobs counter within an existing WriteBatch.
    pub(crate) fn decrement_total_jobs_counter_batch(&self, batch: &mut WriteBatch) {
        let key = shard_total_jobs_counter_key();
        batch.merge(&key, encode_counter(-1));
    }

    /// Increment the completed jobs counter within an existing WriteBatch.
    pub(crate) fn increment_completed_jobs_counter_batch(&self, batch: &mut WriteBatch) {
        let key = shard_completed_jobs_counter_key();
        batch.merge(&key, encode_counter(1));
    }

    /// Decrement the completed jobs counter within an existing WriteBatch.
    pub(crate) fn decrement_completed_jobs_counter_batch(&self, batch: &mut WriteBatch) {
        let key = shard_completed_jobs_counter_key();
        batch.merge(&key, encode_counter(-1));
    }

    // These create their own WriteBatch and write directly to the database.
    // Use these after transaction commits where counters cannot be included
    // in the transaction due to conflict concerns.
    //
    // TODO(slatedb#1254): Once SlateDB supports excluding keys from conflict detection,
    // consider moving these back to transactional operations for better atomicity.

    /// Increment the total jobs counter as a standalone write.
    /// Use after transaction-based operations where the counter can't be in the transaction.
    pub(crate) async fn increment_total_jobs_counter(&self) -> Result<(), JobStoreShardError> {
        let key = shard_total_jobs_counter_key();
        let mut batch = WriteBatch::new();
        batch.merge(&key, encode_counter(1));
        self.db.write(batch).await?;
        Ok(())
    }

    /// Increment the completed jobs counter as a standalone write.
    pub(crate) async fn increment_completed_jobs_counter(&self) -> Result<(), JobStoreShardError> {
        let key = shard_completed_jobs_counter_key();
        let mut batch = WriteBatch::new();
        batch.merge(&key, encode_counter(1));
        self.db.write(batch).await?;
        Ok(())
    }

    /// Decrement the completed jobs counter as a standalone write.
    pub(crate) async fn decrement_completed_jobs_counter(&self) -> Result<(), JobStoreShardError> {
        let key = shard_completed_jobs_counter_key();
        let mut batch = WriteBatch::new();
        batch.merge(&key, encode_counter(-1));
        self.db.write(batch).await?;
        Ok(())
    }
}
