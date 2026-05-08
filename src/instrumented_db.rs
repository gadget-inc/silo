//! [`InstrumentedDb`] — `slatedb::Db` wrapper that attaches a per-shard tracing
//! span to every write operation it performs.
//!
//! `slatedb` logs through the `log` crate, which `tracing-subscriber` bridges
//! into tracing events. Bridged events inherit whatever span is active on the
//! current task when they fire. This wrapper makes sure that any silo code
//! awaiting `db.write` / `db.put` / `db.merge` / `db.delete` does so under the
//! shard span, so log lines like the backpressure warnings from
//! `slatedb::db` are tagged with the shard.
//!
//! The wrapper derefs to `&Db`, so reads and other passthrough calls keep
//! working without modification (they don't typically log).
use std::ops::Deref;
use std::sync::Arc;

use slatedb::bytes::Bytes;
use slatedb::config::WriteOptions;
use slatedb::{Db, WriteBatch, WriteHandle};
use tracing::{Instrument, Span};

pub struct InstrumentedDb {
    inner: Arc<Db>,
    span: Span,
}

impl InstrumentedDb {
    pub fn new(inner: Arc<Db>, span: Span) -> Arc<Self> {
        Arc::new(Self { inner, span })
    }

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

    /// Return the value associated with `key` if present. Reads don't usually
    /// log, but we still pin the span so any future trace/debug events bridged
    /// from slatedb's read path inherit shard context.
    pub async fn get<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
    ) -> Result<Option<Bytes>, slatedb::Error> {
        self.inner.get(key).instrument(self.span.clone()).await
    }
}

impl Deref for InstrumentedDb {
    type Target = Db;

    fn deref(&self) -> &Db {
        &self.inner
    }
}
