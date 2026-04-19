//! No-op counting compaction filter: keeps every entry but tallies what it
//! saw, then logs a structured summary in `on_compaction_end`. Exists to
//! validate the compaction filter plumbing without changing compaction
//! semantics — future business-logic filters should follow the same shape.

use async_trait::async_trait;
use slatedb::{
    CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier,
    CompactionJobContext, RowEntry, ValueDeletable,
};

pub struct NoopCountingSupplier {
    shard_name: String,
}

impl NoopCountingSupplier {
    pub fn new(shard_name: String) -> Self {
        Self { shard_name }
    }
}

#[async_trait]
impl CompactionFilterSupplier for NoopCountingSupplier {
    async fn create_compaction_filter(
        &self,
        context: &CompactionJobContext,
    ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        Ok(Box::new(NoopCountingFilter {
            shard_name: self.shard_name.clone(),
            destination: context.destination,
            is_dest_last_run: context.is_dest_last_run,
            entries_seen: 0,
            value_entries: 0,
            tombstone_entries: 0,
            merge_entries: 0,
            bytes_seen: 0,
        }))
    }
}

pub struct NoopCountingFilter {
    shard_name: String,
    destination: u32,
    is_dest_last_run: bool,
    entries_seen: u64,
    value_entries: u64,
    tombstone_entries: u64,
    merge_entries: u64,
    bytes_seen: u64,
}

#[async_trait]
impl CompactionFilter for NoopCountingFilter {
    async fn filter(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        self.entries_seen += 1;
        self.bytes_seen += entry.key.len() as u64;
        match &entry.value {
            ValueDeletable::Value(bytes) => {
                self.value_entries += 1;
                self.bytes_seen += bytes.len() as u64;
            }
            ValueDeletable::Tombstone => {
                self.tombstone_entries += 1;
            }
            ValueDeletable::Merge(bytes) => {
                self.merge_entries += 1;
                self.bytes_seen += bytes.len() as u64;
            }
        }
        Ok(CompactionFilterDecision::Keep)
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        tracing::info!(
            shard = %self.shard_name,
            destination = self.destination,
            is_dest_last_run = self.is_dest_last_run,
            entries_seen = self.entries_seen,
            value_entries = self.value_entries,
            tombstone_entries = self.tombstone_entries,
            merge_entries = self.merge_entries,
            bytes_seen = self.bytes_seen,
            "noop_counting compaction filter summary",
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn row(key: &[u8], value: ValueDeletable) -> RowEntry {
        RowEntry {
            key: Bytes::copy_from_slice(key),
            value,
            seq: 0,
            create_ts: None,
            expire_ts: None,
        }
    }

    #[tokio::test]
    async fn counts_each_variant_and_keeps_everything() {
        let supplier = NoopCountingSupplier::new("shard".into());
        let ctx = CompactionJobContext {
            destination: 7,
            is_dest_last_run: true,
            compaction_clock_tick: 0,
            retention_min_seq: None,
        };
        let mut filter = supplier.create_compaction_filter(&ctx).await.unwrap();

        for (value, _label) in [
            (ValueDeletable::Value(Bytes::from_static(b"v1")), "value"),
            (ValueDeletable::Tombstone, "tombstone"),
            (ValueDeletable::Merge(Bytes::from_static(b"m")), "merge"),
        ] {
            let decision = filter.filter(&row(b"k", value)).await.unwrap();
            assert_eq!(decision, CompactionFilterDecision::Keep);
        }

        filter.on_compaction_end().await.unwrap();
    }
}
