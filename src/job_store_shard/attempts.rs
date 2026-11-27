//! Reading a job's attempt rows.
//!
//! A job's attempt rows live at `attempt_key(tenant, job_id, n)` for attempt
//! numbers counted up from 1. When the job's status record bounds the highest
//! number ([`JobStatus::highest_attempt`]), the rows are read by key, one
//! point read per attempt. A scan of the job's attempt prefix is kept for a
//! job whose status record gives no bound and for a job with more attempts
//! than [`ATTEMPT_POINT_READ_LIMIT`].

use futures::{StreamExt, TryStreamExt};
use slatedb::bytes::Bytes;

use crate::job::JobStatus;
use crate::job_attempt::JobAttemptView;
use crate::job_store_shard::helpers::WriteBatcher;
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{attempt_key, attempt_prefix, end_bound};

/// The most attempts a job can have and still be read by key. Above it, one
/// scan of the job's attempt prefix replaces that many point reads.
pub const ATTEMPT_POINT_READ_LIMIT: u32 = 32;

/// The most attempt rows of one job read by key at once.
const ATTEMPT_READ_CONCURRENCY: usize = 8;

/// Where a read of a job's attempt rows got them from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptReadSource {
    /// Point reads of the job's attempt keys.
    Keys,
    /// A scan of the job's attempt prefix.
    Scan,
}

impl AttemptReadSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Keys => "keys",
            Self::Scan => "scan",
        }
    }
}

/// The highest attempt number to read by key, when the rows are to be read
/// by key: `highest_attempt` is known and within the point-read limit.
fn point_read_bound(highest_attempt: Option<u32>) -> Option<u32> {
    highest_attempt.filter(|highest| *highest <= ATTEMPT_POINT_READ_LIMIT)
}

impl JobStoreShard {
    fn record_attempt_read(&self, source: AttemptReadSource) {
        if let Some(ref m) = self.metrics {
            m.record_job_attempt_read(&self.name, source);
        }
    }

    /// Fetch all attempts for a job, ordered by attempt number (ascending).
    /// Returns an empty vector if the job has no attempts.
    pub async fn get_job_attempts(
        &self,
        tenant: &str,
        job_id: &str,
    ) -> Result<Vec<JobAttemptView>, JobStoreShardError> {
        let status = self.get_job_status(tenant, job_id).await?;
        let highest_attempt = status.as_ref().and_then(JobStatus::highest_attempt);
        self.read_attempt_views(tenant, job_id, highest_attempt)
            .await
    }

    /// [`Self::get_job_attempts`] for a caller that already holds the job's
    /// status record.
    pub async fn get_job_attempts_for_status(
        &self,
        tenant: &str,
        job_id: &str,
        status: &JobStatus,
    ) -> Result<Vec<JobAttemptView>, JobStoreShardError> {
        self.read_attempt_views(tenant, job_id, status.highest_attempt())
            .await
    }

    /// Get the latest (highest numbered) attempt for a job, if any.
    /// This is typically the attempt that contains the final result for completed jobs.
    pub async fn get_latest_job_attempt(
        &self,
        tenant: &str,
        job_id: &str,
    ) -> Result<Option<JobAttemptView>, JobStoreShardError> {
        let status = self.get_job_status(tenant, job_id).await?;
        let highest_attempt = status.as_ref().and_then(JobStatus::highest_attempt);
        self.read_latest_attempt_view(tenant, job_id, highest_attempt)
            .await
    }

    /// [`Self::get_latest_job_attempt`] for a caller that already holds the
    /// job's status record.
    pub async fn get_latest_job_attempt_for_status(
        &self,
        tenant: &str,
        job_id: &str,
        status: &JobStatus,
    ) -> Result<Option<JobAttemptView>, JobStoreShardError> {
        self.read_latest_attempt_view(tenant, job_id, status.highest_attempt())
            .await
    }

    /// The job's attempts in ascending attempt order: by key for attempts
    /// `1..=highest_attempt` when that bound is known and within the
    /// point-read limit, by a scan of the job's attempt prefix otherwise. An
    /// attempt number with no row is left out.
    async fn read_attempt_views(
        &self,
        tenant: &str,
        job_id: &str,
        highest_attempt: Option<u32>,
    ) -> Result<Vec<JobAttemptView>, JobStoreShardError> {
        let Some(highest) = point_read_bound(highest_attempt) else {
            self.record_attempt_read(AttemptReadSource::Scan);
            let start = attempt_prefix(tenant, job_id);
            let end = end_bound(&start);
            let mut iter = self.db.scan::<Vec<u8>, _>(start..end).await?;
            let mut attempts = Vec::new();
            while let Some(kv) = iter.next().await? {
                attempts.push(JobAttemptView::new(kv.value.clone())?);
            }
            return Ok(attempts);
        };
        if highest == 0 {
            return Ok(Vec::new());
        }

        self.record_attempt_read(AttemptReadSource::Keys);
        let rows: Vec<Option<Bytes>> = futures::stream::iter((1..=highest).map(|attempt| {
            let key = attempt_key(tenant, job_id, attempt);
            async move { self.db.get(&key).await }
        }))
        .buffered(ATTEMPT_READ_CONCURRENCY)
        .try_collect()
        .await?;
        rows.into_iter()
            .flatten()
            .map(JobAttemptView::new)
            .collect()
    }

    /// The job's highest-numbered attempt: one read of the key for
    /// `highest_attempt` when that bound is known, falling back to the rows
    /// below it when that row is absent.
    async fn read_latest_attempt_view(
        &self,
        tenant: &str,
        job_id: &str,
        highest_attempt: Option<u32>,
    ) -> Result<Option<JobAttemptView>, JobStoreShardError> {
        let Some(highest) = highest_attempt else {
            return Ok(self.read_attempt_views(tenant, job_id, None).await?.pop());
        };
        if highest == 0 {
            return Ok(None);
        }
        self.record_attempt_read(AttemptReadSource::Keys);
        if let Some(view) = self.get_job_attempt(tenant, job_id, highest).await? {
            return Ok(Some(view));
        }
        Ok(self
            .read_attempt_views(tenant, job_id, Some(highest - 1))
            .await?
            .pop())
    }

    /// The `(key, value)` of each of the job's attempt rows, in ascending
    /// attempt order, read through `writer`: by key for attempts
    /// `1..=highest_attempt` when that bound is known and within the
    /// point-read limit, by a scan of the job's attempt prefix otherwise. An
    /// attempt number with no row is left out.
    ///
    /// Through a `TxnWriter` every key read, present or absent, joins the
    /// transaction's read set, as the scanned range does on the scan path.
    pub(crate) async fn read_attempt_rows<W: WriteBatcher>(
        &self,
        writer: &W,
        tenant: &str,
        job_id: &str,
        highest_attempt: Option<u32>,
    ) -> Result<Vec<(Vec<u8>, Bytes)>, JobStoreShardError> {
        let mut rows = Vec::new();
        let Some(highest) = point_read_bound(highest_attempt) else {
            self.record_attempt_read(AttemptReadSource::Scan);
            let mut iter = writer.scan_prefix(&attempt_prefix(tenant, job_id)).await?;
            while let Some(kv) = iter.next().await? {
                rows.push((kv.key.to_vec(), kv.value));
            }
            return Ok(rows);
        };
        if highest == 0 {
            return Ok(rows);
        }

        self.record_attempt_read(AttemptReadSource::Keys);
        for attempt in 1..=highest {
            let key = attempt_key(tenant, job_id, attempt);
            if let Some(value) = writer.get(&key).await? {
                rows.push((key, value));
            }
        }
        Ok(rows)
    }
}
