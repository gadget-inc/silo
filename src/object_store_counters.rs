//! [`CountingObjectStore`] — a thin wrapper around any [`ObjectStore`] that
//! atomically tallies the object-store API calls made through it, plus bytes
//! uploaded via `put_opts`.
//!
//! Used by the compaction A/B test harness to estimate cloud storage cost
//! per compaction run. SlateDB's own Prometheus exporter counts *bytes
//! compacted* (output SST size) but not per-op call counts, which is the
//! cost signal most cloud providers bill on. Wrapping the store at the
//! silo-compactor seam lets us attribute every request made during a
//! compaction run to the compactor.
//!
//! Only the methods without default trait impls are overridden — the
//! higher-level helpers (`put`, `get`, `head`, `put_multipart`, etc.)
//! delegate through these, so counts stay canonical.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use slatedb::object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result as OsResult, path::Path as ObjectPath,
};

/// Atomic counters for one [`CountingObjectStore`]. Cheap to read — each
/// field is a single relaxed `load`.
#[derive(Debug, Default)]
pub struct ObjectStoreCounters {
    pub put_opts: AtomicU64,
    pub put_multipart: AtomicU64,
    pub get_opts: AtomicU64,
    pub delete: AtomicU64,
    pub list: AtomicU64,
    pub list_with_delimiter: AtomicU64,
    pub copy: AtomicU64,
    pub copy_if_not_exists: AtomicU64,
    pub put_bytes: AtomicU64,
}

impl ObjectStoreCounters {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Snapshot the current counter values in a single pass. Counters are
    /// monotonic, so there's no risk of inconsistent reads here — this just
    /// provides a plain data struct for the metrics pump.
    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            put_opts: self.put_opts.load(Ordering::Relaxed),
            put_multipart: self.put_multipart.load(Ordering::Relaxed),
            get_opts: self.get_opts.load(Ordering::Relaxed),
            delete: self.delete.load(Ordering::Relaxed),
            list: self.list.load(Ordering::Relaxed),
            list_with_delimiter: self.list_with_delimiter.load(Ordering::Relaxed),
            copy: self.copy.load(Ordering::Relaxed),
            copy_if_not_exists: self.copy_if_not_exists.load(Ordering::Relaxed),
            put_bytes: self.put_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CounterSnapshot {
    pub put_opts: u64,
    pub put_multipart: u64,
    pub get_opts: u64,
    pub delete: u64,
    pub list: u64,
    pub list_with_delimiter: u64,
    pub copy: u64,
    pub copy_if_not_exists: u64,
    pub put_bytes: u64,
}

impl CounterSnapshot {
    /// Total API calls (not bytes) across every counted op. Matches the
    /// "object store request count" dimension that cloud providers bill.
    pub fn total_ops(&self) -> u64 {
        self.put_opts
            + self.put_multipart
            + self.get_opts
            + self.delete
            + self.list
            + self.list_with_delimiter
            + self.copy
            + self.copy_if_not_exists
    }
}

pub struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<ObjectStoreCounters>,
}

impl CountingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, counters: Arc<ObjectStoreCounters>) -> Self {
        Self { inner, counters }
    }

    pub fn counters(&self) -> &Arc<ObjectStoreCounters> {
        &self.counters
    }
}

impl fmt::Debug for CountingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountingObjectStore")
            .field("inner", &self.inner)
            .finish()
    }
}

impl fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.counters.put_opts.fetch_add(1, Ordering::Relaxed);
        self.counters
            .put_bytes
            .fetch_add(payload.content_length() as u64, Ordering::Relaxed);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.counters.put_multipart.fetch_add(1, Ordering::Relaxed);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> OsResult<GetResult> {
        self.counters.get_opts.fetch_add(1, Ordering::Relaxed);
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &ObjectPath, range: Range<u64>) -> OsResult<Bytes> {
        // The default impl would call `get_opts` (counted). We override to
        // also count the call at this layer so range-specific stats stay
        // accurate — but we avoid double-counting by NOT delegating through
        // self.get_opts; call the inner store directly after bookkeeping.
        self.counters.get_opts.fetch_add(1, Ordering::Relaxed);
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &ObjectPath) -> OsResult<ObjectMeta> {
        // head() defaults to a get_opts() with head=true; count as a get.
        self.counters.get_opts.fetch_add(1, Ordering::Relaxed);
        self.inner.head(location).await
    }

    async fn delete(&self, location: &ObjectPath) -> OsResult<()> {
        self.counters.delete.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.counters.list.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> OsResult<ListResult> {
        self.counters
            .list_with_delimiter
            .fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.counters.copy.fetch_add(1, Ordering::Relaxed);
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        self.counters
            .copy_if_not_exists
            .fetch_add(1, Ordering::Relaxed);
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    #[tokio::test]
    async fn counts_basic_ops() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let counters = ObjectStoreCounters::new();
        let store = CountingObjectStore::new(inner, counters.clone());
        let path = ObjectPath::from("a/b");

        store.put(&path, PutPayload::from_static(b"hello")).await.unwrap();
        let _ = store.get(&path).await.unwrap();
        store.head(&path).await.unwrap();
        let _list: Vec<_> = futures::StreamExt::collect::<Vec<_>>(store.list(None)).await;
        store.delete(&path).await.unwrap();

        let snap = counters.snapshot();
        assert_eq!(snap.put_opts, 1, "put should fan through put_opts");
        assert_eq!(snap.put_bytes, 5);
        // get() calls get_opts; head() counts as get_opts as well.
        assert_eq!(snap.get_opts, 2);
        assert_eq!(snap.list, 1);
        assert_eq!(snap.delete, 1);
        assert!(snap.total_ops() >= 5);
    }
}
