//! [`InstrumentedDb`] — `slatedb::Db` wrapper that attaches a per-shard tracing
//! span to every operation it performs.
//!
//! `slatedb` logs through the `log` crate, which `tracing-subscriber` bridges
//! into tracing events. Bridged events inherit whatever span is active on the
//! current task when they fire. This wrapper makes sure that any silo code
//! awaiting a slatedb operation does so under the shard span, so log lines
//! like the backpressure warnings from `slatedb::db` are tagged with the
//! shard.
//!
//! Shard-scoped slatedb operations must go through `InstrumentedDb`,
//! [`InstrumentedDbIterator`], or [`InstrumentedDbTransaction`]. The
//! `inner()` accessor is an escape hatch reserved for non-shard-scoped work
//! (factory operations on closed parents, status watchers, metrics readers).
//!
//! `InstrumentedDb` deliberately does not implement `Deref<Target = Db>`. If a
//! method you need isn't here, add a wrapper rather than reaching for
//! `inner()` — Deref is exactly what caused the past coverage gaps.
use std::ops::RangeBounds;
use std::sync::Arc;

use slatedb::bytes::Bytes;
use slatedb::config::{FlushOptions, ScanOptions, WriteOptions};
use slatedb::{Db, DbIterator, DbTransaction, IsolationLevel, KeyValue, WriteBatch, WriteHandle};
use tracing::{Instrument, Span};

pub struct InstrumentedDb {
    inner: Arc<Db>,
    span: Span,
}

impl InstrumentedDb {
    pub fn new(inner: Arc<Db>, span: Span) -> Arc<Self> {
        Arc::new(Self { inner, span })
    }

    /// Escape hatch for non-shard-scoped consumers (factory ops, status
    /// watchers, metrics readers). Do not use for shard-scoped reads/writes —
    /// add a wrapped method instead.
    pub fn inner(&self) -> &Arc<Db> {
        &self.inner
    }

    pub async fn write(&self, batch: WriteBatch) -> Result<WriteHandle, slatedb::Error> {
        self.inner.write(batch).instrument(self.span.clone()).await
    }

    pub async fn write_with_options(
        &self,
        batch: WriteBatch,
        options: &WriteOptions,
    ) -> Result<WriteHandle, slatedb::Error> {
        self.inner
            .write_with_options(batch, options)
            .instrument(self.span.clone())
            .await
    }

    pub async fn put<K, V>(&self, key: K, value: V) -> Result<WriteHandle, slatedb::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.inner
            .put(key, value)
            .instrument(self.span.clone())
            .await
    }

    pub async fn merge<K, V>(&self, key: K, value: V) -> Result<WriteHandle, slatedb::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.inner
            .merge(key, value)
            .instrument(self.span.clone())
            .await
    }

    pub async fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<WriteHandle, slatedb::Error> {
        self.inner.delete(key).instrument(self.span.clone()).await
    }

    pub async fn close(&self) -> Result<(), slatedb::Error> {
        self.inner.close().instrument(self.span.clone()).await
    }

    pub async fn flush(&self) -> Result<(), slatedb::Error> {
        self.inner.flush().instrument(self.span.clone()).await
    }

    pub async fn flush_with_options(&self, options: FlushOptions) -> Result<(), slatedb::Error> {
        self.inner
            .flush_with_options(options)
            .instrument(self.span.clone())
            .await
    }

    pub async fn get<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
    ) -> Result<Option<Bytes>, slatedb::Error> {
        self.inner.get(key).instrument(self.span.clone()).await
    }

    pub async fn scan<K, T>(&self, range: T) -> Result<InstrumentedDbIterator, slatedb::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        let inner = self.inner.scan(range).instrument(self.span.clone()).await?;
        Ok(InstrumentedDbIterator {
            inner,
            span: self.span.clone(),
        })
    }

    pub async fn scan_with_options<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<InstrumentedDbIterator, slatedb::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        let inner = self
            .inner
            .scan_with_options(range, options)
            .instrument(self.span.clone())
            .await?;
        Ok(InstrumentedDbIterator {
            inner,
            span: self.span.clone(),
        })
    }

    pub async fn begin(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<InstrumentedDbTransaction, slatedb::Error> {
        let inner = self
            .inner
            .begin(isolation_level)
            .instrument(self.span.clone())
            .await?;
        Ok(InstrumentedDbTransaction {
            inner,
            span: self.span.clone(),
        })
    }
}

/// Wraps a [`slatedb::DbIterator`] so that `.next()` runs under the shard span.
pub struct InstrumentedDbIterator {
    inner: DbIterator,
    span: Span,
}

impl InstrumentedDbIterator {
    pub async fn next(&mut self) -> Result<Option<KeyValue>, slatedb::Error> {
        self.inner.next().instrument(self.span.clone()).await
    }
}

/// Wraps a [`slatedb::DbTransaction`] so that every method runs under the shard
/// span. Sync methods (`put`/`delete`/`merge`/`unmark_write`) enter the span
/// for the duration of the call so any synchronous `log!` emissions inherit it.
pub struct InstrumentedDbTransaction {
    inner: DbTransaction,
    span: Span,
}

impl InstrumentedDbTransaction {
    pub async fn get<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
    ) -> Result<Option<Bytes>, slatedb::Error> {
        self.inner.get(key).instrument(self.span.clone()).await
    }

    pub async fn scan<K, T>(&self, range: T) -> Result<InstrumentedDbIterator, slatedb::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        let inner = self.inner.scan(range).instrument(self.span.clone()).await?;
        Ok(InstrumentedDbIterator {
            inner,
            span: self.span.clone(),
        })
    }

    pub fn put<K, V>(&self, key: K, value: V) -> Result<(), slatedb::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let _g = self.span.enter();
        self.inner.put(key, value)
    }

    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), slatedb::Error> {
        let _g = self.span.enter();
        self.inner.delete(key)
    }

    pub fn merge<K, V>(&self, key: K, value: V) -> Result<(), slatedb::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let _g = self.span.enter();
        self.inner.merge(key, value)
    }

    pub fn unmark_write<K, I>(&self, keys: I) -> Result<(), slatedb::Error>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = K>,
    {
        let _g = self.span.enter();
        self.inner.unmark_write(keys)
    }

    pub async fn commit(self) -> Result<Option<WriteHandle>, slatedb::Error> {
        self.inner.commit().instrument(self.span).await
    }

    pub async fn commit_with_options(
        self,
        options: &WriteOptions,
    ) -> Result<Option<WriteHandle>, slatedb::Error> {
        self.inner
            .commit_with_options(options)
            .instrument(self.span)
            .await
    }
}
