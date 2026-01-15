//! Shard load statistics for the placement engine.
//!
//! This module tracks real-time load metrics for each shard, including:
//! - Enqueue and dequeue rates
//! - Current pending and running job counts
//!
//! All counters use atomic operations for lock-free, zero-allocation tracking.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Statistics tracker for a single shard.
///
/// Uses atomic counters for lock-free, zero-allocation tracking.
/// All operations are thread-safe and non-blocking.
pub struct ShardStats {
    /// Total number of jobs enqueued since stats were created/reset
    enqueue_count: AtomicU64,
    /// Total number of tasks dequeued since stats were created/reset
    dequeue_count: AtomicU64,

    /// Enqueue count at last rate calculation
    last_enqueue_count: AtomicU64,
    /// Dequeue count at last rate calculation
    last_dequeue_count: AtomicU64,
    /// Timestamp of last rate calculation (epoch millis)
    last_rate_calc_ms: AtomicI64,

    /// Current number of pending (scheduled but not running) jobs
    pending_jobs: AtomicU64,
    /// Current number of running jobs (tasks currently leased to workers)
    running_jobs: AtomicU64,
}

impl Default for ShardStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardStats {
    /// Create a new stats tracker.
    pub fn new() -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        Self {
            enqueue_count: AtomicU64::new(0),
            dequeue_count: AtomicU64::new(0),
            last_enqueue_count: AtomicU64::new(0),
            last_dequeue_count: AtomicU64::new(0),
            last_rate_calc_ms: AtomicI64::new(now_ms),
            pending_jobs: AtomicU64::new(0),
            running_jobs: AtomicU64::new(0),
        }
    }

    /// Record a job enqueue.
    #[inline]
    pub fn record_enqueue(&self) {
        self.enqueue_count.fetch_add(1, Ordering::Relaxed);
        self.pending_jobs.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a task dequeue (job started running).
    #[inline]
    pub fn record_dequeue(&self) {
        self.dequeue_count.fetch_add(1, Ordering::Relaxed);
        // Move from pending to running
        self.pending_jobs.fetch_sub(1, Ordering::Relaxed);
        self.running_jobs.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a job completion (success, failure, or cancellation).
    #[inline]
    pub fn record_job_completed(&self) {
        self.running_jobs.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record when a job that was running goes back to pending (e.g., lease expired).
    #[inline]
    pub fn record_job_requeued(&self) {
        self.running_jobs.fetch_sub(1, Ordering::Relaxed);
        self.pending_jobs.fetch_add(1, Ordering::Relaxed);
    }

    /// Record when a pending job is deleted or cancelled without running.
    #[inline]
    pub fn record_pending_job_removed(&self) {
        // Saturating sub to avoid underflow
        let current = self.pending_jobs.load(Ordering::Relaxed);
        if current > 0 {
            self.pending_jobs.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Get the current snapshot of stats for reporting.
    pub fn snapshot(&self) -> ShardStatsSnapshot {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let enqueue_count = self.enqueue_count.load(Ordering::Relaxed);
        let dequeue_count = self.dequeue_count.load(Ordering::Relaxed);
        let last_enqueue = self.last_enqueue_count.load(Ordering::Relaxed);
        let last_dequeue = self.last_dequeue_count.load(Ordering::Relaxed);
        let last_calc_ms = self.last_rate_calc_ms.load(Ordering::Relaxed);

        // Calculate time delta in seconds
        let delta_ms = (now_ms - last_calc_ms).max(1) as f64;
        let delta_secs = delta_ms / 1000.0;

        // Calculate rates
        let enqueue_delta = enqueue_count.saturating_sub(last_enqueue);
        let dequeue_delta = dequeue_count.saturating_sub(last_dequeue);
        let enqueue_rate = enqueue_delta as f64 / delta_secs;
        let dequeue_rate = dequeue_delta as f64 / delta_secs;

        // Update last values for next calculation
        self.last_enqueue_count.store(enqueue_count, Ordering::Relaxed);
        self.last_dequeue_count.store(dequeue_count, Ordering::Relaxed);
        self.last_rate_calc_ms.store(now_ms, Ordering::Relaxed);

        ShardStatsSnapshot {
            enqueue_rate_per_sec: enqueue_rate,
            dequeue_rate_per_sec: dequeue_rate,
            pending_jobs: self.pending_jobs.load(Ordering::Relaxed),
            running_jobs: self.running_jobs.load(Ordering::Relaxed),
            total_enqueued: enqueue_count,
            total_dequeued: dequeue_count,
        }
    }

    /// Get pending job count.
    pub fn pending_jobs(&self) -> u64 {
        self.pending_jobs.load(Ordering::Relaxed)
    }

    /// Get running job count.
    pub fn running_jobs(&self) -> u64 {
        self.running_jobs.load(Ordering::Relaxed)
    }

    /// Set the pending jobs count (used during hydration).
    pub fn set_pending_jobs(&self, count: u64) {
        self.pending_jobs.store(count, Ordering::Relaxed);
    }

    /// Set the running jobs count (used during hydration).
    pub fn set_running_jobs(&self, count: u64) {
        self.running_jobs.store(count, Ordering::Relaxed);
    }
}

/// A point-in-time snapshot of shard statistics.
#[derive(Debug, Clone)]
pub struct ShardStatsSnapshot {
    /// Jobs enqueued per second (recent rate)
    pub enqueue_rate_per_sec: f64,
    /// Tasks dequeued per second (recent rate)
    pub dequeue_rate_per_sec: f64,
    /// Current number of pending jobs
    pub pending_jobs: u64,
    /// Current number of running jobs
    pub running_jobs: u64,
    /// Total jobs enqueued since start
    pub total_enqueued: u64,
    /// Total tasks dequeued since start
    pub total_dequeued: u64,
}

impl ShardStatsSnapshot {
    /// Calculate a composite load score for placement decisions.
    ///
    /// The score combines multiple factors:
    /// - Throughput (enqueue + dequeue rate) - indicates activity level
    /// - Queue depth (pending jobs) - indicates backlog
    /// - Running jobs - indicates current utilization
    ///
    /// Higher scores indicate busier shards.
    pub fn load_score(&self) -> f64 {
        // Weight factors (can be tuned)
        const THROUGHPUT_WEIGHT: f64 = 1.0;
        const PENDING_WEIGHT: f64 = 0.5;
        const RUNNING_WEIGHT: f64 = 0.3;

        let throughput = self.enqueue_rate_per_sec + self.dequeue_rate_per_sec;

        throughput * THROUGHPUT_WEIGHT
            + self.pending_jobs as f64 * PENDING_WEIGHT
            + self.running_jobs as f64 * RUNNING_WEIGHT
    }
}

/// Load report for a single shard, ready for transmission.
#[derive(Debug, Clone)]
pub struct ShardLoadReport {
    pub shard_id: u32,
    pub enqueue_rate_per_sec: f64,
    pub dequeue_rate_per_sec: f64,
    pub pending_jobs: u64,
    pub running_jobs: u64,
    pub load_score: f64,
}

impl ShardLoadReport {
    /// Create a load report from a stats snapshot.
    pub fn from_snapshot(shard_id: u32, snapshot: ShardStatsSnapshot) -> Self {
        Self {
            shard_id,
            enqueue_rate_per_sec: snapshot.enqueue_rate_per_sec,
            dequeue_rate_per_sec: snapshot.dequeue_rate_per_sec,
            pending_jobs: snapshot.pending_jobs,
            running_jobs: snapshot.running_jobs,
            load_score: snapshot.load_score(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_tracking() {
        let stats = ShardStats::new();

        // Enqueue some jobs
        stats.record_enqueue();
        stats.record_enqueue();
        stats.record_enqueue();
        assert_eq!(stats.pending_jobs(), 3);
        assert_eq!(stats.running_jobs(), 0);

        // Dequeue one
        stats.record_dequeue();
        assert_eq!(stats.pending_jobs(), 2);
        assert_eq!(stats.running_jobs(), 1);

        // Complete one
        stats.record_job_completed();
        assert_eq!(stats.pending_jobs(), 2);
        assert_eq!(stats.running_jobs(), 0);
    }

    #[test]
    fn test_snapshot() {
        let stats = ShardStats::new();

        // Enqueue and dequeue
        for _ in 0..10 {
            stats.record_enqueue();
        }
        for _ in 0..5 {
            stats.record_dequeue();
        }
        for _ in 0..2 {
            stats.record_job_completed();
        }

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.pending_jobs, 5);
        assert_eq!(snapshot.running_jobs, 3);
        assert_eq!(snapshot.total_enqueued, 10);
        assert_eq!(snapshot.total_dequeued, 5);
    }

    #[test]
    fn test_load_score() {
        let snapshot = ShardStatsSnapshot {
            enqueue_rate_per_sec: 100.0,
            dequeue_rate_per_sec: 50.0,
            pending_jobs: 200,
            running_jobs: 10,
            total_enqueued: 1000,
            total_dequeued: 500,
        };

        let score = snapshot.load_score();
        // throughput: 150 * 1.0 = 150
        // pending: 200 * 0.5 = 100
        // running: 10 * 0.3 = 3
        // total: 253
        assert!((score - 253.0).abs() < 0.001);
    }
}
