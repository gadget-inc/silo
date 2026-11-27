//! Runs the lease reaper across a node's open shards.
//!
//! Each tick starts a reap pass on every shard that has lease work due. The
//! passes run concurrently up to a bound, so one shard's slow pass does not
//! delay its neighbours, and a shard whose previous pass is still running is
//! left alone until it finishes.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Instant;

use futures::FutureExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::warn;

use crate::job_store_shard::JobStoreShard;
use crate::metrics::Metrics;
use crate::shard_range::ShardId;

/// The most shards whose leases are reaped at once.
pub const MAX_CONCURRENT_SHARD_REAPS: usize = 4;

/// Runs keyed background work with at most `max_concurrent` pieces running
/// at once and at most one piece queued or running per key.
pub struct KeyedTaskPool<K> {
    in_flight: HashSet<K>,
    tasks: JoinSet<K>,
    permits: Arc<Semaphore>,
}

impl<K> KeyedTaskPool<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            in_flight: HashSet::new(),
            tasks: JoinSet::new(),
            permits: Arc::new(Semaphore::new(max_concurrent.max(1))),
        }
    }

    /// Start `work` for `key` unless work for that key is still queued or
    /// running. Returns whether it was started.
    pub fn spawn<F>(&mut self, key: K, work: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if !self.in_flight.insert(key.clone()) {
            return false;
        }
        let permits = Arc::clone(&self.permits);
        self.tasks.spawn(async move {
            // The semaphore is never closed; were it, the work is skipped
            // and the key freed.
            if let Ok(_permit) = permits.acquire_owned().await {
                // A panic in the work must still free its key.
                let _ = AssertUnwindSafe(work).catch_unwind().await;
            }
            key
        });
        true
    }

    /// Wait for one piece of work to finish and free its key. Returns `None`
    /// at once when nothing is queued or running.
    pub async fn join_next(&mut self) -> Option<K> {
        loop {
            match self.tasks.join_next().await? {
                Ok(key) => {
                    self.in_flight.remove(&key);
                    return Some(key);
                }
                // The task was aborted, which only dropping the pool does.
                Err(_) => continue,
            }
        }
    }

    /// Wait for all queued and running work to finish.
    pub async fn drain(&mut self) {
        while self.join_next().await.is_some() {}
    }
}

/// Reaps expired leases on a node's open shards.
pub struct LeaseReaper {
    metrics: Option<Metrics>,
    pool: KeyedTaskPool<ShardId>,
    /// Shards the last tick saw open, whose tracked-lease gauges are live.
    gauged: HashSet<ShardId>,
}

impl LeaseReaper {
    pub fn new(metrics: Option<Metrics>) -> Self {
        Self {
            metrics,
            pool: KeyedTaskPool::new(MAX_CONCURRENT_SHARD_REAPS),
            gauged: HashSet::new(),
        }
    }

    /// Start a reap pass, as of `now_ms`, on every open shard that has lease
    /// work due and no pass still queued or running.
    pub fn tick(&mut self, shards: &HashMap<ShardId, Arc<JobStoreShard>>, now_ms: i64) {
        if let Some(ref m) = self.metrics {
            for closed in self.gauged.iter().filter(|id| !shards.contains_key(id)) {
                m.clear_lease_reaper_tracked_leases(&closed.to_string());
            }
        }
        self.gauged = shards.keys().copied().collect();

        for (shard_id, shard) in shards {
            if let Some(ref m) = self.metrics {
                m.set_lease_reaper_tracked_leases(
                    &shard_id.to_string(),
                    shard.tracked_lease_count(),
                );
            }
            if !shard.lease_reap_is_due(now_ms) {
                continue;
            }
            self.pool.spawn(
                *shard_id,
                reap_shard(*shard_id, Arc::clone(shard), self.metrics.clone(), now_ms),
            );
        }
    }

    /// Wait for one in-flight reap pass to finish. Returns `None` at once
    /// when none is queued or running.
    pub async fn join_next(&mut self) -> Option<ShardId> {
        self.pool.join_next().await
    }

    /// Wait for every in-flight reap pass to finish.
    pub async fn drain(&mut self) {
        self.pool.drain().await;
    }
}

async fn reap_shard(
    shard_id: ShardId,
    shard: Arc<JobStoreShard>,
    metrics: Option<Metrics>,
    now_ms: i64,
) {
    let started = Instant::now();
    let result = shard.reap_due_leases_at(now_ms).await;
    let shard_label = shard_id.to_string();
    match result {
        Ok(pass) => {
            if let Some(ref m) = metrics {
                m.record_lease_reaper_pass(&shard_label, started.elapsed().as_secs_f64(), &pass);
            }
        }
        Err(e) => {
            if let Some(ref m) = metrics {
                m.record_lease_reaper_error(&shard_label);
            }
            warn!(shard = %shard_label, error = %e, "lease reaper failed");
        }
    }
}
