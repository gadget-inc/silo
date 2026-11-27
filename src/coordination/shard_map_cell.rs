//! This node's copy of the cluster shard map, readable without a lock.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use arc_swap::ArcSwap;

use crate::shard_range::ShardMap;

/// This node's copy of the cluster shard map.
///
/// Every tenant-scoped RPC reads the shard map to check that the tenant falls
/// inside the addressed shard's range, so a read must never wait. [`load`]
/// returns an `Arc` to the current map without taking a lock or copying it,
/// and it returns immediately even while a write is in progress. Writers build
/// a new map and swap it in; a snapshot a reader already holds never changes.
///
/// This replaces a `tokio::sync::Mutex<ShardMap>`. Tokio's mutex is fair: a
/// release hands the lock to the oldest waiter, and nobody else can take it
/// until that waiter's task is polled. With thousands of tasks queued on the
/// runtime, each handoff waited behind the whole run queue, so the lock
/// changed hands a few times a second while RPCs arrived for it dozens of
/// times a second, and tenant-scoped RPCs stopped completing.
///
/// Writes are serialized by a separate mutex, so an [`update`] can't build on
/// a map that a concurrent [`store`] is replacing and then overwrite it. That
/// mutex is a `std` one, held only to copy, modify and swap the map, and never
/// across an `.await`.
///
/// [`load`]: ShardMapCell::load
/// [`store`]: ShardMapCell::store
/// [`update`]: ShardMapCell::update
pub struct ShardMapCell {
    current: ArcSwap<ShardMap>,
    writer: Mutex<()>,
}

impl ShardMapCell {
    pub fn new(shard_map: ShardMap) -> Self {
        Self {
            current: ArcSwap::from_pointee(shard_map),
            writer: Mutex::new(()),
        }
    }

    /// The current shard map. Never blocks.
    pub fn load(&self) -> Arc<ShardMap> {
        self.current.load_full()
    }

    /// Replace the shard map, e.g. with one just read from the backend.
    pub fn store(&self, shard_map: ShardMap) {
        let _writer = self.lock_writer();
        self.current.store(Arc::new(shard_map));
    }

    /// Apply `modify` to a copy of the current shard map and swap the copy in
    /// if it returns `Ok`. On `Err` the shard map is left as it was.
    pub fn update<T, E>(&self, modify: impl FnOnce(&mut ShardMap) -> Result<T, E>) -> Result<T, E> {
        let _writer = self.lock_writer();
        let mut next = ShardMap::clone(&self.current.load());
        let result = modify(&mut next)?;
        self.current.store(Arc::new(next));
        Ok(result)
    }

    fn lock_writer(&self) -> MutexGuard<'_, ()> {
        // The mutex guards no data, and a writer that panics does so before it
        // swaps anything in, so a poisoned lock leaves nothing half-written.
        self.writer.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
