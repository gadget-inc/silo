use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::factory::ShardFactory;
use crate::shard_range::{ShardId, ShardMap, SplitInProgress};

use crate::coordination::{CoordinationError, ShardOwnerMap};

/// [SILO-COORD-INV-8] Status of post-split cleanup for a shard.
///
/// After a split, child shards contain defunct data (keys outside their new range).
/// This status tracks the progression through cleanup phases:
/// CleanupPending -> CleanupRunning -> CleanupDone -> CompactionDone
///
/// Status only progresses forward through this sequence - it cannot regress
/// to an earlier state. This ensures cleanup work is not repeated and the
/// state machine progresses monotonically.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SplitCleanupStatus {
    /// Initial state for new shards or shards that don't need cleanup
    #[default]
    CompactionDone,
    /// Just created from split, cleanup has not started yet
    CleanupPending,
    /// Cleanup is in progress (deleting defunct data)
    CleanupRunning,
    /// Cleanup complete, ready for compaction
    CleanupDone,
}

impl SplitCleanupStatus {
    /// Returns true if cleanup is still pending or in progress
    pub fn needs_work(&self) -> bool {
        matches!(
            self,
            Self::CleanupPending | Self::CleanupRunning | Self::CleanupDone
        )
    }

    /// Returns the ordinal value for ordering comparisons.
    /// Higher values represent later states in the cleanup progression.
    fn ordinal(&self) -> u8 {
        match self {
            Self::CleanupPending => 0,
            Self::CleanupRunning => 1,
            Self::CleanupDone => 2,
            Self::CompactionDone => 3,
        }
    }

    /// [SILO-COORD-INV-8] Check if transitioning from current to new_status is valid.
    ///
    /// Status can only progress forward: Pending -> Running -> Done -> CompactionDone.
    /// Returns true if the transition is valid (same state or forward progress).
    pub fn can_transition_to(&self, new_status: Self) -> bool {
        new_status.ordinal() >= self.ordinal()
    }
}

impl fmt::Display for SplitCleanupStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SplitCleanupStatus::CleanupPending => write!(f, "CleanupPending"),
            SplitCleanupStatus::CleanupRunning => write!(f, "CleanupRunning"),
            SplitCleanupStatus::CleanupDone => write!(f, "CleanupDone"),
            SplitCleanupStatus::CompactionDone => write!(f, "CompactionDone"),
        }
    }
}

impl std::str::FromStr for SplitCleanupStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "CleanupPending" => Ok(SplitCleanupStatus::CleanupPending),
            "CleanupRunning" => Ok(SplitCleanupStatus::CleanupRunning),
            "CleanupDone" => Ok(SplitCleanupStatus::CleanupDone),
            "CompactionDone" => Ok(SplitCleanupStatus::CompactionDone),
            _ => Err(format!("unknown cleanup status: {}", s)),
        }
    }
}

/// Phases of a shard split operation.
///
/// A split operation divides one parent shard into two child shards at a
/// specified split point. The phases ensure traffic is paused, data is cloned,
/// and the shard map is atomically updated.
///
/// The shard map update is the "point of no return" - once children exist in the
/// shard map, the split is committed. If we crash before the shard map update,
/// the split is abandoned and can be retried (potentially leaving orphaned clone
/// databases that will be garbage collected later).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SplitPhase {
    /// Split has been requested but not started
    SplitRequested,
    /// Traffic to parent shard is being paused (returning retryable errors)
    SplitPausing,
    /// SlateDB clone operation and shard map update in progress.
    /// This phase clones both child databases, then atomically updates the shard map.
    /// The shard map update is the commit point.
    SplitCloning,
    /// Split is complete, children are active
    SplitComplete,
}

impl SplitPhase {
    /// Returns true if traffic to the parent shard should be paused
    /// (returning retryable errors to clients)
    pub fn traffic_paused(&self) -> bool {
        matches!(self, SplitPhase::SplitPausing | SplitPhase::SplitCloning)
    }
}

impl fmt::Display for SplitPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SplitPhase::SplitRequested => write!(f, "SplitRequested"),
            SplitPhase::SplitPausing => write!(f, "SplitPausing"),
            SplitPhase::SplitCloning => write!(f, "SplitCloning"),
            SplitPhase::SplitComplete => write!(f, "SplitComplete"),
        }
    }
}

/// Backend-specific operations needed for shard splitting.
///
/// This trait abstracts over the storage operations that differ between coordination backends (etcd, k8s). The splitter uses this trait to perform the actual persistence operations while managing the overall split state machine.
#[async_trait]
pub trait SplitStorageBackend: Send + Sync {
    /// Load a split-in-progress record from storage.
    async fn load_split(
        &self,
        parent_shard_id: &ShardId,
    ) -> Result<Option<SplitInProgress>, CoordinationError>;

    /// Store a split-in-progress record to storage.
    async fn store_split(&self, split: &SplitInProgress) -> Result<(), CoordinationError>;

    /// Delete a split-in-progress record from storage.
    async fn delete_split(&self, parent_shard_id: &ShardId) -> Result<(), CoordinationError>;

    /// Update the shard map atomically after a split.
    ///
    /// This should:
    /// 1. Read the current shard map from storage
    /// 2. Apply the split (using ShardMap::split_shard)
    /// 3. Write the updated shard map atomically (CAS)
    /// 4. Update the local shard map cache
    async fn update_shard_map_for_split(
        &self,
        split: &SplitInProgress,
    ) -> Result<(), CoordinationError>;

    /// Reload the shard map from storage into the local cache.
    async fn reload_shard_map(&self) -> Result<(), CoordinationError>;

    /// List all split records (for recovery scanning).
    async fn list_all_splits(&self) -> Result<Vec<SplitInProgress>, CoordinationError>;
}

/// Context for split operations, containing references to shared coordinator state.
///
/// This is passed to the splitter to provide access to:
/// - The node's identity and owned shards
/// - The shard map
/// - The shard factory for cloning/opening shards
/// - Shard owner computation
pub struct ShardSplitContext {
    /// This node's ID
    pub node_id: String,
    /// The shard map (protected by mutex)
    pub shard_map: Arc<Mutex<ShardMap>>,
    /// Set of shards owned by this node (protected by mutex)
    pub owned: Arc<Mutex<HashSet<ShardId>>>,
    /// Factory for shard operations (clone, open, close)
    pub factory: Arc<ShardFactory>,
}

impl ShardSplitContext {
    /// Create a new context from coordinator base components.
    pub fn new(
        node_id: String,
        shard_map: Arc<Mutex<ShardMap>>,
        owned: Arc<Mutex<HashSet<ShardId>>>,
        factory: Arc<ShardFactory>,
    ) -> Self {
        Self {
            node_id,
            shard_map,
            owned,
            factory,
        }
    }

    /// Check if this node owns the specified shard.
    pub async fn owns_shard(&self, shard_id: &ShardId) -> bool {
        self.owned.lock().await.contains(shard_id)
    }
}

use crate::coordination::Coordinator;

/// Internal error type that distinguishes whether a split error occurred
/// before or after the commit point (shard map update).
enum SplitExecutionError {
    /// Error occurred before the shard map was updated. The split can be
    /// safely abandoned and the parent shard restored to service.
    PreCommit(CoordinationError),
    /// Error occurred after the shard map was updated. The split is committed
    /// and cannot be rolled back.
    PostCommit(CoordinationError),
}

/// splitter for shard split operations.
///
/// This struct encapsulates all the shared split logic, using the coordinator's `SplitStorageBackend` implementation for backend-specific storage operations.
pub struct ShardSplitter {
    /// The coordinator (which implements SplitStorageBackend)
    coordinator: Arc<dyn Coordinator>,
    /// Shared context with coordinator state (owned, constructed from coordinator)
    ctx: ShardSplitContext,
}

impl ShardSplitter {
    /// Create a new shard split splitter from a coordinator.
    pub fn new(coordinator: Arc<dyn Coordinator>) -> Self {
        let base = coordinator.base();
        let ctx = ShardSplitContext::new(
            coordinator.node_id().to_string(),
            Arc::clone(&base.shard_map),
            Arc::clone(&base.owned),
            Arc::clone(&base.factory),
        );

        Self { coordinator, ctx }
    }

    /// Request a shard split.
    ///
    /// Validates that this node owns the shard, the split point is valid,
    /// and no split is already in progress. Then creates and stores the
    /// split record.
    pub async fn request_split(
        &self,
        shard_id: ShardId,
        split_point: String,
    ) -> Result<SplitInProgress, CoordinationError> {
        // Verify this node owns the shard
        if !self.ctx.owns_shard(&shard_id).await {
            return Err(CoordinationError::NotShardOwner(shard_id));
        }

        // [SILO-COORD-INV-7] Verify the shard exists in the shard map and validate split point.
        // Split operations must reference an existing parent shard - we cannot split
        // a shard that doesn't exist in the authoritative shard map.
        {
            let shard_map = self.ctx.shard_map.lock().await;
            let shard_info = shard_map
                .get_shard(&shard_id)
                .ok_or(CoordinationError::ShardNotFound(shard_id))?;

            if !shard_info.range.contains(&split_point) {
                return Err(CoordinationError::ShardMapError(
                    crate::shard_range::ShardMapError::InvalidSplitPoint(format!(
                        "split point '{}' is not within shard range {}",
                        split_point, shard_info.range
                    )),
                ));
            }
        }

        // Check if a split is already in progress
        if self.coordinator.load_split(&shard_id).await?.is_some() {
            return Err(CoordinationError::SplitAlreadyInProgress(shard_id));
        }

        // Create and store the split record
        let split = SplitInProgress::new(shard_id, split_point, self.ctx.node_id.clone());
        self.coordinator.store_split(&split).await?;

        info!(
            shard_id = %shard_id,
            split_point = %split.split_point,
            left_child = %split.left_child_id,
            right_child = %split.right_child_id,
            "split requested"
        );

        Ok(split)
    }

    /// Get the current split status for a shard.
    pub async fn get_split_status(
        &self,
        parent_shard_id: ShardId,
    ) -> Result<Option<SplitInProgress>, CoordinationError> {
        self.coordinator.load_split(&parent_shard_id).await
    }

    /// Check if a shard is currently paused for split.
    pub async fn is_shard_paused(&self, shard_id: ShardId) -> bool {
        match self.coordinator.load_split(&shard_id).await {
            Ok(Some(split)) => split.phase.traffic_paused(),
            _ => false,
        }
    }

    /// Advance the split to the next phase.
    ///
    /// This is primarily for testing to control the split state machine step by step.
    /// Note: This only advances the phase marker, it does NOT perform the actual work
    /// (cloning, shard map update, etc.). Use `execute_split` for full split execution.
    pub async fn advance_split_phase(
        &self,
        parent_shard_id: ShardId,
    ) -> Result<(), CoordinationError> {
        let mut split = self
            .coordinator
            .load_split(&parent_shard_id)
            .await?
            .ok_or(CoordinationError::NoSplitInProgress(parent_shard_id))?;

        // Verify ownership (not needed for SplitComplete which is just cleanup)
        if split.phase != SplitPhase::SplitComplete && !self.ctx.owns_shard(&parent_shard_id).await
        {
            return Err(CoordinationError::NotShardOwner(parent_shard_id));
        }

        split.advance_phase();
        self.coordinator.store_split(&split).await?;

        debug!(
            parent_shard_id = %parent_shard_id,
            phase = %split.phase,
            "split phase advanced"
        );

        Ok(())
    }

    /// Execute a split operation through all phases to completion.
    ///
    /// This method drives the split through the state machine:
    /// SplitRequested -> SplitPausing -> SplitCloning -> SplitComplete
    ///
    /// The shard map update (inside SplitCloning) is the "point of no return".
    /// If an error occurs before the shard map update, the split is abandoned:
    /// the split record is deleted so the parent shard resumes normal operation.
    /// Orphaned clone databases may be left behind but don't affect correctness.
    ///
    /// The `get_shard_owner_map` closure is used to compute ownership after the shard map
    /// is updated, since this requires backend-specific member information.
    pub async fn execute_split<F, Fut>(
        &self,
        parent_shard_id: ShardId,
        get_shard_owner_map: F,
    ) -> Result<(), CoordinationError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<ShardOwnerMap, CoordinationError>>,
    {
        match self
            .execute_split_inner(parent_shard_id, get_shard_owner_map)
            .await
        {
            Ok(()) => Ok(()),
            Err(SplitExecutionError::PreCommit(e)) => {
                // [SILO-COORD-INV-15] The split failed before the commit point
                // (shard map update). Abandon the split by deleting the record
                // so the parent shard resumes normal operation.
                warn!(
                    parent_shard_id = %parent_shard_id,
                    error = %e,
                    "split failed before commit, abandoning split to restore parent shard"
                );
                if let Err(abandon_err) = self.abandon_split(&parent_shard_id).await {
                    warn!(
                        parent_shard_id = %parent_shard_id,
                        error = %abandon_err,
                        "failed to abandon split record during error recovery"
                    );
                }
                Err(e)
            }
            Err(SplitExecutionError::PostCommit(e)) => {
                // The split failed after the commit point. The shard map has
                // already been updated, so the split is committed. Return the
                // error but don't try to undo the split.
                Err(e)
            }
        }
    }

    /// Inner implementation of split execution that distinguishes pre-commit
    /// and post-commit errors.
    async fn execute_split_inner<F, Fut>(
        &self,
        parent_shard_id: ShardId,
        get_shard_owner_map: F,
    ) -> Result<(), SplitExecutionError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<ShardOwnerMap, CoordinationError>>,
    {
        loop {
            // Load current split state
            let mut split = self
                .coordinator
                .load_split(&parent_shard_id)
                .await
                .map_err(SplitExecutionError::PreCommit)?
                .ok_or(SplitExecutionError::PreCommit(
                    CoordinationError::NoSplitInProgress(parent_shard_id),
                ))?;

            // Verify we own the shard (not needed for SplitComplete which is just cleanup)
            if split.phase != SplitPhase::SplitComplete
                && !self.ctx.owns_shard(&parent_shard_id).await
            {
                return Err(SplitExecutionError::PreCommit(
                    CoordinationError::NotShardOwner(parent_shard_id),
                ));
            }

            match split.phase {
                SplitPhase::SplitRequested => {
                    // Advance to SplitPausing
                    split.advance_phase();
                    self.coordinator
                        .store_split(&split)
                        .await
                        .map_err(SplitExecutionError::PreCommit)?;
                    info!(
                        parent_shard_id = %parent_shard_id,
                        phase = %split.phase,
                        "split advanced to pausing phase"
                    );
                }
                SplitPhase::SplitPausing => {
                    // Traffic is now paused. Advance to SplitCloning.
                    split.advance_phase();
                    self.coordinator
                        .store_split(&split)
                        .await
                        .map_err(SplitExecutionError::PreCommit)?;
                    info!(
                        parent_shard_id = %parent_shard_id,
                        phase = %split.phase,
                        "split advanced to cloning phase"
                    );
                }
                SplitPhase::SplitCloning => {
                    // [SILO-COORD-INV-5] Traffic is paused before we reach this phase.
                    // The state machine ensures we pass through SplitPausing before
                    // SplitCloning, and traffic_paused() returns true for both phases.

                    // Fully close the parent shard before cloning. This guarantees
                    // that all processing has stopped (broker, background tasks, in-flight
                    // dequeues) and all data is flushed. By reopening just the raw SlateDB
                    // database for the checkpoint, we ensure the snapshotter is the only
                    // thing working on the shard -- no writes can land after the checkpoint.
                    let parent_shard = self.ctx.factory.get(&parent_shard_id).ok_or_else(|| {
                        SplitExecutionError::PreCommit(CoordinationError::BackendError(format!(
                            "parent shard {} not open in factory",
                            parent_shard_id
                        )))
                    })?;
                    parent_shard.close().await.map_err(|e| {
                        SplitExecutionError::PreCommit(CoordinationError::BackendError(format!(
                            "failed to close parent shard before cloning: {}",
                            e
                        )))
                    })?;
                    info!(
                        parent_shard_id = %parent_shard_id,
                        "closed parent shard before cloning"
                    );

                    // Clone the closed parent's database for both children. This reopens
                    // just the raw SlateDB database, creates a checkpoint, clones to both
                    // children, then closes the raw database.
                    info!(
                        parent_shard_id = %parent_shard_id,
                        left_child = %split.left_child_id,
                        right_child = %split.right_child_id,
                        "cloning database for split"
                    );

                    self.ctx
                        .factory
                        .clone_closed_shard(
                            &parent_shard_id,
                            &split.left_child_id,
                            &split.right_child_id,
                        )
                        .await
                        .map_err(|e| {
                            SplitExecutionError::PreCommit(CoordinationError::BackendError(
                                format!("failed to clone children: {}", e),
                            ))
                        })?;

                    info!(
                        parent_shard_id = %parent_shard_id,
                        left_child = %split.left_child_id,
                        right_child = %split.right_child_id,
                        "cloning complete, updating shard map"
                    );

                    // Pre-add BOTH children to owned set BEFORE updating the shard map.
                    // This prevents a race condition where the shard map watch fires and
                    // shard guards try to acquire children before we've opened them.
                    // We'll remove any children we don't actually own after computing ownership.
                    let both_children = [split.left_child_id, split.right_child_id];
                    {
                        let mut owned = self.ctx.owned.lock().await;
                        for child_id in &both_children {
                            owned.insert(*child_id);
                        }
                    }

                    // Update the shard map atomically - THIS IS THE COMMIT POINT
                    // After this succeeds, the split is committed and children exist.
                    if let Err(e) = self.coordinator.update_shard_map_for_split(&split).await {
                        // Shard map update failed - this is still pre-commit.
                        // Remove the pre-added children from owned set before abandoning.
                        {
                            let mut owned = self.ctx.owned.lock().await;
                            for child_id in &both_children {
                                owned.remove(child_id);
                            }
                        }
                        return Err(SplitExecutionError::PreCommit(e));
                    }

                    // --- PAST THE COMMIT POINT ---
                    // From here on, all errors are post-commit.

                    // Reload the shard map to update local cache
                    self.coordinator
                        .reload_shard_map()
                        .await
                        .map_err(SplitExecutionError::PostCommit)?;

                    // Now compute which children we actually own based on the updated shard map
                    let owner_map = get_shard_owner_map()
                        .await
                        .map_err(SplitExecutionError::PostCommit)?;
                    let children_we_own: Vec<ShardId> = both_children
                        .into_iter()
                        .filter(|child_id| {
                            owner_map
                                .shard_to_node
                                .get(child_id)
                                .map(|owner| *owner == self.ctx.node_id)
                                .unwrap_or(false)
                        })
                        .collect();

                    // Close the parent shard before opening children.
                    // The parent is no longer valid after being split - its data now lives
                    // in the two child shards. Closing it releases database resources and
                    // ensures no stale references remain.
                    self.ctx
                        .factory
                        .close(&parent_shard_id)
                        .await
                        .map_err(|e| {
                            SplitExecutionError::PostCommit(CoordinationError::BackendError(
                                format!("failed to close parent shard {}: {}", parent_shard_id, e),
                            ))
                        })?;
                    info!(
                        parent_shard_id = %parent_shard_id,
                        "closed parent shard after split"
                    );

                    // Update ownership: remove parent and children we don't own
                    {
                        let mut owned = self.ctx.owned.lock().await;
                        owned.remove(&parent_shard_id);
                        // Remove children we don't own (they were pre-added to prevent race)
                        for child_id in &both_children {
                            if !children_we_own.contains(child_id) {
                                owned.remove(child_id);
                            }
                        }
                    }

                    // Open the child shards we own and set their cleanup status
                    for child_id in children_we_own {
                        // Look up the child's range from the reloaded shard map
                        let range = {
                            let map = self.ctx.shard_map.lock().await;
                            let info =
                                map.get_shard(&child_id)
                                    .ok_or(SplitExecutionError::PostCommit(
                                        CoordinationError::ShardNotFound(child_id),
                                    ))?;
                            info.range.clone()
                        };
                        self.ctx
                            .factory
                            .open(&child_id, &range)
                            .await
                            .map_err(|e| {
                                SplitExecutionError::PostCommit(CoordinationError::BackendError(
                                    format!("failed to open child shard {}: {}", child_id, e),
                                ))
                            })?;

                        if let Some(shard) = self.ctx.factory.get(&child_id) {
                            shard
                                .set_cleanup_status(SplitCleanupStatus::CleanupPending)
                                .await
                                .map_err(|e| {
                                    SplitExecutionError::PostCommit(
                                        CoordinationError::BackendError(format!(
                                            "failed to set cleanup status for child shard {}: {}",
                                            child_id, e
                                        )),
                                    )
                                })?;
                        }

                        info!(
                            child_shard_id = %child_id,
                            range = %range,
                            "opened child shard after split"
                        );
                    }

                    // Advance to SplitComplete
                    split.advance_phase();
                    self.coordinator
                        .store_split(&split)
                        .await
                        .map_err(SplitExecutionError::PostCommit)?;
                    info!(
                        parent_shard_id = %parent_shard_id,
                        phase = %split.phase,
                        "shard map updated, split complete"
                    );
                }
                SplitPhase::SplitComplete => {
                    // [SILO-COORD-INV-13] Clean up: delete the split record.
                    // The split is already committed (children exist in shard map).
                    // Deleting the split record just removes the tracking metadata.
                    // The committed state (children in shard map) is preserved.
                    self.coordinator
                        .delete_split(&parent_shard_id)
                        .await
                        .map_err(SplitExecutionError::PostCommit)?;
                    info!(
                        parent_shard_id = %parent_shard_id,
                        "split record cleaned up"
                    );
                    return Ok(());
                }
            }
        }
    }

    /// Abandon a split by deleting its record.
    ///
    /// This is used when a split fails before the commit point to restore the
    /// parent shard to normal operation. Orphaned clone databases may be left
    /// behind but don't affect correctness and will be garbage collected.
    async fn abandon_split(&self, parent_shard_id: &ShardId) -> Result<(), CoordinationError> {
        info!(
            parent_shard_id = %parent_shard_id,
            "abandoning split, deleting split record to restore parent shard"
        );
        self.coordinator.delete_split(parent_shard_id).await
    }

    /// Recover from stale split operations after a node restart.
    ///
    /// [SILO-COORD-INV-11] All incomplete splits are abandoned on crash.
    /// The shard map update is the commit point - if children don't exist in the
    /// shard map, the split never completed and is safe to abandon. Orphaned
    /// clone databases may exist but don't affect correctness.
    ///
    /// This preserves the parent shard when a crash occurs before the commit point,
    /// ensuring no data is lost during failed splits.
    pub async fn recover_stale_splits(&self) -> Result<(), CoordinationError> {
        let splits = self.coordinator.list_all_splits().await?;

        for split in splits {
            // Only process splits initiated by this node
            if split.initiator_node_id != self.ctx.node_id {
                continue;
            }

            // Check if children exist in the shard map (split committed)
            let children_exist = {
                let shard_map = self.ctx.shard_map.lock().await;
                shard_map.get_shard(&split.left_child_id).is_some()
            };

            if children_exist {
                // Split committed (shard map was updated). Just clean up the record.
                info!(
                    parent_shard_id = %split.parent_shard_id,
                    phase = %split.phase,
                    "cleaning up completed split record"
                );
            } else {
                // Split did not commit. Abandon it.
                // Any orphaned clone databases will be garbage collected later.
                info!(
                    parent_shard_id = %split.parent_shard_id,
                    phase = %split.phase,
                    "abandoning incomplete split"
                );
            }

            self.coordinator
                .delete_split(&split.parent_shard_id)
                .await?;
        }

        Ok(())
    }
}
