//! Shard-level job counters for high-performance job counting.
//!
//! These counters track:
//! - `total_jobs`: Count of all jobs that exist in the shard (not deleted)
//! - `completed_jobs`: Count of jobs that have reached a terminal state
//!   (Succeeded, Failed, or Cancelled)
//!
//! Counters use SlateDB's MergeOperator to avoid read-modify-write cycles. Instead of reading the current value, modifying it, and writing it back, we write a delta (+1 or -1) that gets merged at read time.

use std::sync::Arc;

use slatedb::bytes::Bytes;
use slatedb::{DbTransaction, MergeOperator, MergeOperatorError, WriteBatch};

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

    /// Increment the total jobs counter in a write batch using merge.
    /// Call this when a new job is enqueued.
    pub(crate) fn increment_total_jobs_counter(&self, batch: &mut WriteBatch) {
        let key = shard_total_jobs_counter_key();
        batch.merge(&key, encode_counter(1));
    }

    /// Decrement the total jobs counter in a write batch using merge.
    /// Call this when a job is deleted.
    pub(crate) fn decrement_total_jobs_counter(&self, batch: &mut WriteBatch) {
        let key = shard_total_jobs_counter_key();
        batch.merge(&key, encode_counter(-1));
    }

    /// Increment the completed jobs counter in a write batch using merge.
    /// Call this when a job transitions to a terminal state (Succeeded, Failed, Cancelled).
    pub(crate) fn increment_completed_jobs_counter(&self, batch: &mut WriteBatch) {
        let key = shard_completed_jobs_counter_key();
        batch.merge(&key, encode_counter(1));
    }

    /// Decrement the completed jobs counter in a write batch using merge.
    /// Call this when a terminal job is restarted or deleted.
    pub(crate) fn decrement_completed_jobs_counter(&self, batch: &mut WriteBatch) {
        let key = shard_completed_jobs_counter_key();
        batch.merge(&key, encode_counter(-1));
    }

    /// Increment the completed jobs counter within a transaction using merge.
    /// Call this when a scheduled job is cancelled (immediately becomes terminal).
    pub(crate) fn increment_completed_jobs_counter_txn(
        &self,
        txn: &DbTransaction,
    ) -> Result<(), JobStoreShardError> {
        let key = shard_completed_jobs_counter_key();
        txn.merge(&key, encode_counter(1))?;
        Ok(())
    }

    /// Decrement the completed jobs counter within a transaction using merge.
    /// Call this when a terminal job is restarted.
    pub(crate) fn decrement_completed_jobs_counter_txn(
        &self,
        txn: &DbTransaction,
    ) -> Result<(), JobStoreShardError> {
        let key = shard_completed_jobs_counter_key();
        txn.merge(&key, encode_counter(-1))?;
        Ok(())
    }
}
