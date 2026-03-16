//! Standalone concurrency ticket operations for JobStoreShard.
//!
//! These methods implement a distributed semaphore where silo stores state and does
//! atomic promotions. The caller (e.g. Gadget via Temporal) handles notification/signaling
//! of promoted participants.

use slatedb::WriteBatch;
use slatedb::config::WriteOptions;

use crate::codec::{
    decode_standalone_holder, decode_standalone_request, encode_standalone_holder,
    encode_standalone_request,
};
use crate::job_store_shard::counters::encode_counter;
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::keys::{
    end_bound, parse_standalone_concurrency_holder_key, parse_standalone_concurrency_request_key,
    standalone_concurrency_holder_key, standalone_concurrency_holders_queue_prefix,
    standalone_concurrency_request_key, standalone_concurrency_request_prefix,
    standalone_concurrency_requester_counter_key,
};
use crate::shard_range::ShardRange;
use crate::task::{StandaloneHolderRecord, StandaloneRequestRecord};

/// A participant that was promoted from request queue to holder.
#[derive(Debug, Clone)]
pub struct PromotedParticipant {
    pub participant_id: String,
    pub metadata: Vec<u8>,
}

/// Result of a standalone acquire operation.
#[derive(Debug)]
pub struct StandaloneAcquireResult {
    /// Whether this participant was promoted to holder.
    pub acquired: bool,
    /// Other participants promoted during this operation.
    pub promoted: Vec<PromotedParticipant>,
}

/// Result of a standalone release operation.
#[derive(Debug)]
pub struct StandaloneReleaseResult {
    /// Whether the participant was a holder.
    pub was_held: bool,
    /// Participants promoted after the release.
    pub promoted: Vec<PromotedParticipant>,
}

/// A holder entry for listing.
#[derive(Debug, Clone)]
pub struct StandaloneHolderEntry {
    pub tenant: String,
    pub queue: String,
    pub participant_id: String,
    pub metadata: Vec<u8>,
    pub granted_at_ms: i64,
    pub priority: u8,
}

/// Result of listing standalone holders.
#[derive(Debug)]
pub struct StandaloneListHoldersResult {
    pub holders: Vec<StandaloneHolderEntry>,
    /// Empty when exhausted.
    pub next_cursor: Vec<u8>,
}

impl JobStoreShard {
    /// Acquire a standalone concurrency ticket.
    ///
    /// 1. Idempotency: if participant is already a holder, return acquired=true
    /// 2. Queue type check: verify no job-system holders exist
    /// 3. Add to request queue
    /// 4. Promote up to available slots
    /// 5. Return acquired + list of OTHER promoted participants
    #[allow(clippy::too_many_arguments)]
    pub async fn standalone_acquire_ticket(
        &self,
        range: &ShardRange,
        tenant: &str,
        queue: &str,
        participant_id: &str,
        max_concurrency: u32,
        priority: u8,
        metadata: Vec<u8>,
    ) -> Result<StandaloneAcquireResult, JobStoreShardError> {
        let db = &self.db;
        let counts = self.concurrency.counts();
        let limit = max_concurrency as usize;

        // Ensure hydrated
        counts.ensure_hydrated(db, range, tenant, queue).await?;

        // 1. Idempotency check: already a holder?
        let holder_key = standalone_concurrency_holder_key(tenant, queue, participant_id);
        if let Some(_existing) = db.get(&holder_key).await? {
            return Ok(StandaloneAcquireResult {
                acquired: true,
                promoted: vec![],
            });
        }

        // 2. Queue type check happens inside try_reserve_standalone

        // 3. Write request record
        let now_ms = crate::job_store_shard::now_epoch_ms() as u64;
        let request_key =
            standalone_concurrency_request_key(tenant, queue, priority, now_ms, participant_id);
        let request_record = StandaloneRequestRecord {
            metadata: metadata.clone(),
        };
        let request_value = encode_standalone_request(&request_record);

        let mut batch = WriteBatch::new();
        batch.put(&request_key, &request_value);
        batch.merge(
            standalone_concurrency_requester_counter_key(tenant, queue),
            encode_counter(1),
        );
        db.write(batch).await?;

        // 4. Promote: scan requests and promote up to available_slots
        let promoted = self
            .promote_standalone_requests(range, tenant, queue, limit, None)
            .await?;

        // 5. Check if self was promoted
        let self_promoted = promoted.iter().any(|p| p.participant_id == participant_id);

        // Filter self from promoted list (caller only wants OTHER promoted participants)
        let other_promoted: Vec<PromotedParticipant> = promoted
            .into_iter()
            .filter(|p| p.participant_id != participant_id)
            .collect();

        Ok(StandaloneAcquireResult {
            acquired: self_promoted,
            promoted: other_promoted,
        })
    }

    /// Release a standalone concurrency ticket.
    ///
    /// Removes the participant from holders or requesters.
    /// Promotes the next highest-priority requester if capacity opens.
    pub async fn standalone_release_ticket(
        &self,
        range: &ShardRange,
        tenant: &str,
        queue: &str,
        participant_id: &str,
        max_concurrency: u32,
    ) -> Result<StandaloneReleaseResult, JobStoreShardError> {
        let db = &self.db;
        let counts = self.concurrency.counts();
        let limit = max_concurrency as usize;

        counts.ensure_hydrated(db, range, tenant, queue).await?;

        // Check if holder
        let holder_key = standalone_concurrency_holder_key(tenant, queue, participant_id);
        let was_held = if let Some(_holder_bytes) = db.get(&holder_key).await? {
            // Delete holder from DB
            db.delete(&holder_key).await?;
            // Release in-memory
            counts.atomic_release(tenant, queue, participant_id);
            counts.maybe_clear_queue_type(tenant, queue);
            true
        } else {
            // Check if requester - scan for this participant in request queue
            let deleted = self
                .delete_standalone_request(tenant, queue, participant_id)
                .await?;
            if !deleted {
                return Ok(StandaloneReleaseResult {
                    was_held: false,
                    promoted: vec![],
                });
            }
            false
        };

        // Promote next waiters
        let promoted = if was_held {
            self.promote_standalone_requests(range, tenant, queue, limit, None)
                .await?
        } else {
            vec![]
        };

        Ok(StandaloneReleaseResult { was_held, promoted })
    }

    /// Force-release a standalone concurrency ticket (admin/break-glass).
    /// Same as release.
    pub async fn standalone_force_release_ticket(
        &self,
        range: &ShardRange,
        tenant: &str,
        queue: &str,
        participant_id: &str,
        max_concurrency: u32,
    ) -> Result<StandaloneReleaseResult, JobStoreShardError> {
        self.standalone_release_ticket(range, tenant, queue, participant_id, max_concurrency)
            .await
    }

    /// Force-admit all pending requesters regardless of max_concurrency (break-glass).
    pub async fn standalone_force_admit_requesters(
        &self,
        range: &ShardRange,
        tenant: &str,
        queue: &str,
    ) -> Result<Vec<PromotedParticipant>, JobStoreShardError> {
        let counts = self.concurrency.counts();
        counts
            .ensure_hydrated(&self.db, range, tenant, queue)
            .await?;

        // Promote all with unlimited capacity
        self.promote_standalone_requests(range, tenant, queue, usize::MAX, None)
            .await
    }

    /// List standalone concurrency holders with cursor-based pagination.
    pub async fn standalone_list_holders(
        &self,
        tenant: &str,
        queue: &str,
        page_size: u32,
        cursor: &[u8],
    ) -> Result<StandaloneListHoldersResult, JobStoreShardError> {
        let db = &self.db;

        // Determine scan prefix
        let prefix = if queue.is_empty() {
            if tenant.is_empty() {
                crate::keys::standalone_concurrency_holders_prefix()
            } else {
                crate::keys::standalone_concurrency_holders_tenant_prefix(tenant)
            }
        } else {
            standalone_concurrency_holders_queue_prefix(tenant, queue)
        };

        let start = if cursor.is_empty() {
            prefix.clone()
        } else {
            // Start after the cursor key
            let mut s = cursor.to_vec();
            // Append a byte to make it exclusive (scan past cursor)
            s.push(0x00);
            s
        };
        let end = end_bound(&prefix);

        let mut iter = db.scan::<Vec<u8>, _>(start..end).await?;
        let mut holders = Vec::new();
        let mut last_key: Option<Vec<u8>> = None;
        let limit = if page_size == 0 {
            100
        } else {
            page_size as usize
        };

        loop {
            if holders.len() >= limit {
                break;
            }
            let maybe = iter.next().await?;
            let Some(kv) = maybe else { break };

            let Some(parsed) = parse_standalone_concurrency_holder_key(&kv.key) else {
                continue;
            };

            match decode_standalone_holder(&kv.value) {
                Ok(record) => {
                    last_key = Some(kv.key.to_vec());
                    holders.push(StandaloneHolderEntry {
                        tenant: parsed.tenant,
                        queue: parsed.queue,
                        participant_id: parsed.participant_id,
                        metadata: record.metadata,
                        granted_at_ms: record.granted_at_ms,
                        priority: record.priority,
                    });
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to decode standalone holder record");
                    continue;
                }
            }
        }

        // Determine next_cursor
        let next_cursor = if holders.len() >= limit {
            last_key.unwrap_or_default()
        } else {
            vec![] // Exhausted
        };

        Ok(StandaloneListHoldersResult {
            holders,
            next_cursor,
        })
    }

    /// Promote pending standalone requests up to available capacity.
    /// Returns the list of promoted participants.
    async fn promote_standalone_requests(
        &self,
        range: &ShardRange,
        tenant: &str,
        queue: &str,
        max_concurrency: usize,
        _scan_limit: Option<usize>,
    ) -> Result<Vec<PromotedParticipant>, JobStoreShardError> {
        let db = &self.db;
        let counts = self.concurrency.counts();

        let start = standalone_concurrency_request_prefix(tenant, queue);
        let end = end_bound(&start);
        let mut iter = db.scan::<Vec<u8>, _>(start..end).await?;

        let mut promoted = Vec::new();
        let now_ms = crate::job_store_shard::now_epoch_ms();

        loop {
            let maybe = iter.next().await?;
            let Some(kv) = maybe else { break };

            let Some(parsed) = parse_standalone_concurrency_request_key(&kv.key) else {
                continue;
            };

            // Try to reserve a slot
            let reserved = counts
                .try_reserve_standalone(
                    db,
                    range,
                    tenant,
                    queue,
                    &parsed.participant_id,
                    max_concurrency,
                )
                .await;

            let reserved = match reserved {
                Ok(true) => true,
                Ok(false) => break, // At capacity, stop scanning
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        participant = %parsed.participant_id,
                        "standalone promotion: reserve failed"
                    );
                    break;
                }
            };

            if !reserved {
                break;
            }

            // Decode request to get metadata
            let metadata = match decode_standalone_request(&kv.value) {
                Ok(req) => req.metadata,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to decode standalone request, skipping");
                    counts.release_reservation(tenant, queue, &parsed.participant_id);
                    continue;
                }
            };

            // Write holder + delete request atomically
            let holder_record = StandaloneHolderRecord {
                granted_at_ms: now_ms,
                metadata: metadata.clone(),
                priority: parsed.priority,
            };
            let holder_value = encode_standalone_holder(&holder_record);

            let mut batch = WriteBatch::new();
            batch.put(
                standalone_concurrency_holder_key(tenant, queue, &parsed.participant_id),
                &holder_value,
            );
            batch.delete(&kv.key);
            batch.merge(
                standalone_concurrency_requester_counter_key(tenant, queue),
                encode_counter(-1),
            );

            if let Err(e) = db
                .write_with_options(
                    batch,
                    &WriteOptions {
                        await_durable: true,
                    },
                )
                .await
            {
                // Rollback in-memory reservation
                counts.release_reservation(tenant, queue, &parsed.participant_id);
                tracing::warn!(
                    error = %e,
                    participant = %parsed.participant_id,
                    "standalone promotion: write failed, rolled back"
                );
                break;
            }

            promoted.push(PromotedParticipant {
                participant_id: parsed.participant_id,
                metadata,
            });
        }

        Ok(promoted)
    }

    /// Delete a standalone request for a specific participant.
    /// Returns true if the request was found and deleted.
    async fn delete_standalone_request(
        &self,
        tenant: &str,
        queue: &str,
        participant_id: &str,
    ) -> Result<bool, JobStoreShardError> {
        let db = &self.db;
        let start = standalone_concurrency_request_prefix(tenant, queue);
        let end = end_bound(&start);
        let mut iter = db.scan::<Vec<u8>, _>(start..end).await?;

        loop {
            let maybe = iter.next().await?;
            let Some(kv) = maybe else { break };

            let Some(parsed) = parse_standalone_concurrency_request_key(&kv.key) else {
                continue;
            };

            if parsed.participant_id == participant_id {
                let mut batch = WriteBatch::new();
                batch.delete(&kv.key);
                batch.merge(
                    standalone_concurrency_requester_counter_key(tenant, queue),
                    encode_counter(-1),
                );
                db.write(batch).await?;
                return Ok(true);
            }
        }

        Ok(false)
    }
}
