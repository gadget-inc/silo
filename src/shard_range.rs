//! Hash-based shard coordination types.
//!
//! This module defines the core data structures for hash-based sharding,
//! where tenant IDs are hashed (XXH64) into a uniform keyspace and then
//! assigned to shards via lexicographical ranges over the hex-encoded hash.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

use crate::coordination::SplitPhase;

/// Hash a tenant ID to a 16-character hex string using XXH64 (seed 0).
///
/// This ensures uniform distribution across the shard keyspace regardless
/// of the structure of tenant ID strings (e.g. shared prefixes like "env-").
pub fn hash_tenant(tenant_id: &str) -> String {
    let hash = xxhash_rust::xxh64::xxh64(tenant_id.as_bytes(), 0);
    format!("{:016x}", hash)
}

/// A unique identifier for a shard.
///
/// Shards have immutable identities that persist across restarts and
/// are used for storage paths and coordination keys.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardId(Uuid);

impl ShardId {
    /// Create a new random shard ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create a shard ID from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Parse a shard ID from a string.
    pub fn parse(s: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(s)?))
    }

    /// Get the underlying UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for ShardId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ShardId({})", self.0)
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for ShardId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<ShardId> for Uuid {
    fn from(id: ShardId) -> Self {
        id.0
    }
}

/// A range in the hash keyspace owned by a shard.
///
/// Ranges are defined as `[start, end)` where:
/// - `start` is inclusive (empty string means unbounded start, i.e., from the beginning)
/// - `end` is exclusive (empty string means unbounded end, i.e., to infinity)
///
/// Boundaries are 16-character hex-encoded u64 hash values.
/// Use `contains_tenant` (which hashes the tenant ID first) for tenant lookups.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRange {
    /// Inclusive start of the range. Empty string means unbounded (from beginning).
    pub start: String,
    /// Exclusive end of the range. Empty string means unbounded (to infinity).
    pub end: String,
}

impl ShardRange {
    /// Create a new shard range.
    pub fn new(start: impl Into<String>, end: impl Into<String>) -> Self {
        Self {
            start: start.into(),
            end: end.into(),
        }
    }

    /// Create a range covering the entire keyspace.
    pub fn full() -> Self {
        Self {
            start: String::new(),
            end: String::new(),
        }
    }

    /// Check if this range contains the given tenant ID.
    ///
    /// Uses lexicographical (string) comparison.
    pub fn contains(&self, tenant_id: &str) -> bool {
        // Check start bound (inclusive)
        let after_start = self.start.is_empty() || tenant_id >= self.start.as_str();

        // Check end bound (exclusive)
        let before_end = self.end.is_empty() || tenant_id < self.end.as_str();

        after_start && before_end
    }

    /// Check if this range contains the hash of the given tenant ID.
    ///
    /// Hashes the tenant ID with XXH64 and checks if the result falls
    /// within this range's bounds.
    pub fn contains_tenant(&self, tenant_id: &str) -> bool {
        self.contains(&hash_tenant(tenant_id))
    }

    /// Check if this range is unbounded at the start.
    pub fn is_start_unbounded(&self) -> bool {
        self.start.is_empty()
    }

    /// Check if this range is unbounded at the end.
    pub fn is_end_unbounded(&self) -> bool {
        self.end.is_empty()
    }

    /// Check if this range covers the entire keyspace.
    pub fn is_full(&self) -> bool {
        self.start.is_empty() && self.end.is_empty()
    }

    /// [SILO-COORD-INV-2] Split this range at the given point into two contiguous ranges.
    ///
    /// Returns `(left, right)` where:
    /// - `left` = [self.start, split_point)
    /// - `right` = [split_point, self.end)
    ///
    /// # Errors
    /// Returns an error if the split point is not strictly within the range.
    pub fn split(&self, split_point: &str) -> Result<(ShardRange, ShardRange), ShardMapError> {
        // Validate that split_point is within this range
        if !self.contains(split_point) {
            return Err(ShardMapError::InvalidSplitPoint(format!(
                "split point '{}' is not within range {}",
                split_point, self
            )));
        }

        // split_point must be strictly after start (can't split at the start)
        if !self.start.is_empty() && split_point <= self.start.as_str() {
            return Err(ShardMapError::InvalidSplitPoint(format!(
                "split point '{}' must be strictly after range start '{}'",
                split_point, self.start
            )));
        }

        // split_point must be strictly before end (can't split at the end)
        if !self.end.is_empty() && split_point >= self.end.as_str() {
            return Err(ShardMapError::InvalidSplitPoint(format!(
                "split point '{}' must be strictly before range end '{}'",
                split_point, self.end
            )));
        }

        let left = ShardRange {
            start: self.start.clone(),
            end: split_point.to_string(),
        };
        let right = ShardRange {
            start: split_point.to_string(),
            end: self.end.clone(),
        };

        // [SILO-COORD-INV-3] Verify children are contiguous
        debug_assert_eq!(left.end, right.start);

        Ok((left, right))
    }

    /// Compute the numeric midpoint of this hash-space range for auto-split.
    ///
    /// Interprets the range boundaries as 16-character hex-encoded u64 values
    /// and returns the midpoint as a u64.
    /// Returns None if the range is too small to split.
    pub fn midpoint(&self) -> Option<u64> {
        let start_val: u64 = if self.start.is_empty() {
            0
        } else {
            u64::from_str_radix(&self.start, 16).ok()?
        };

        let end_val: u64 = if self.end.is_empty() {
            u64::MAX
        } else {
            u64::from_str_radix(&self.end, 16).ok()?
        };

        if end_val <= start_val + 1 {
            return None;
        }

        Some(start_val + (end_val - start_val) / 2)
    }
}

impl fmt::Display for ShardRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let start = if self.start.is_empty() {
            "-∞"
        } else {
            &self.start
        };
        let end = if self.end.is_empty() {
            "+∞"
        } else {
            &self.end
        };
        write!(f, "[{}, {})", start, end)
    }
}

/// Metadata about a shard, including its identity and range.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardInfo {
    /// Unique identifier for this shard.
    pub id: ShardId,
    /// The tenant ID range owned by this shard.
    pub range: ShardRange,
    /// Timestamp when this shard was created (milliseconds since epoch).
    pub created_at_ms: i64,
    /// If this shard was created by splitting another shard, the parent's ID.
    pub parent_shard_id: Option<ShardId>,
    /// The placement ring this shard belongs to.
    /// None means the shard is on the default ring.
    /// Only nodes that participate in this ring will be considered for ownership.
    #[serde(default)]
    pub placement_ring: Option<String>,
}

impl ShardInfo {
    /// Create a new shard info.
    pub fn new(id: ShardId, range: ShardRange) -> Self {
        Self {
            id,
            range,
            created_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            parent_shard_id: None,
            placement_ring: None,
        }
    }

    /// Create a new shard info with a specific creation time.
    pub fn with_created_at(id: ShardId, range: ShardRange, created_at_ms: i64) -> Self {
        Self {
            id,
            range,
            created_at_ms,
            parent_shard_id: None,
            placement_ring: None,
        }
    }

    /// Create a new shard info that was split from a parent.
    /// Note: The cleanup status is stored in the shard's database, not in the shard map.
    pub fn from_split(id: ShardId, range: ShardRange, parent_id: ShardId) -> Self {
        Self {
            id,
            range,
            created_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            parent_shard_id: Some(parent_id),
            placement_ring: None,
        }
    }

    /// Get the placement ring for this shard.
    pub fn placement_ring(&self) -> Option<&str> {
        self.placement_ring.as_deref()
    }

    /// Set the placement ring for this shard.
    pub fn set_placement_ring(&mut self, ring: Option<String>) {
        self.placement_ring = ring;
    }

    /// Check if this shard's range contains the given tenant ID.
    ///
    /// Hashes the tenant ID before checking the range.
    pub fn contains(&self, tenant_id: &str) -> bool {
        self.range.contains_tenant(tenant_id)
    }
}

/// Tracks an in-progress shard split operation.
///
/// The split creates two child shards from one parent, dividing the keyspace
/// at a specified split point. This struct persists in the coordination backend
/// to enable crash recovery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitInProgress {
    /// The parent shard being split
    pub parent_shard_id: ShardId,
    /// The tenant ID where the keyspace is split.
    /// Left child: [parent_start, split_point)
    /// Right child: [split_point, parent_end)
    pub split_point: String,
    /// The left child shard (tenant IDs < split_point)
    pub left_child_id: ShardId,
    /// The right child shard (tenant IDs >= split_point)
    pub right_child_id: ShardId,
    /// Current phase of the split operation
    pub phase: SplitPhase,
    /// Timestamp when the split was requested (milliseconds since epoch)
    pub requested_at_ms: i64,
    /// Node ID that initiated the split (for crash recovery)
    pub initiator_node_id: String,
}

impl SplitInProgress {
    /// Create a new split operation in the SplitRequested phase.
    pub fn new(parent_shard_id: ShardId, split_point: String, initiator_node_id: String) -> Self {
        Self {
            parent_shard_id,
            split_point,
            left_child_id: ShardId::new(),
            right_child_id: ShardId::new(),
            phase: SplitPhase::SplitRequested,
            requested_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            initiator_node_id,
        }
    }

    /// [SILO-COORD-INV-9] Advance to the next phase.
    ///
    /// Split phases only progress forward: SplitRequested -> SplitPausing ->
    /// SplitCloning -> SplitComplete. There is no way to regress to an earlier
    /// phase. SplitComplete is terminal.
    pub fn advance_phase(&mut self) {
        self.phase = match self.phase {
            SplitPhase::SplitRequested => SplitPhase::SplitPausing,
            SplitPhase::SplitPausing => SplitPhase::SplitCloning,
            SplitPhase::SplitCloning => SplitPhase::SplitComplete,
            SplitPhase::SplitComplete => SplitPhase::SplitComplete, // Terminal
        };
    }

    /// Check if the split is complete.
    pub fn is_complete(&self) -> bool {
        self.phase == SplitPhase::SplitComplete
    }
}

/// A map of all shards in the cluster and their ranges.
///
/// The shard map is the authoritative source for:
/// - Which shards exist in the cluster
/// - What range of tenant IDs each shard owns
///
/// Shards are stored sorted by their range start for efficient binary search.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardMap {
    /// All shards, sorted by range.start for binary search.
    shards: Vec<ShardInfo>,
    /// Version number for change detection. Incremented on each modification.
    pub version: u64,
}

impl ShardMap {
    /// Create a new empty shard map.
    pub fn new() -> Self {
        Self {
            shards: Vec::new(),
            version: 0,
        }
    }

    /// Create an initial shard map with the given number of shards.
    ///
    /// Divides the u64 hash keyspace evenly among shards.
    /// Tenant IDs are hashed with XXH64 before lookup, so the
    /// boundaries are 16-character hex strings in hash space.
    pub fn create_initial(shard_count: u32) -> Result<Self, ShardMapError> {
        if shard_count == 0 {
            return Err(ShardMapError::InvalidShardCount(
                "shard count must be positive".to_string(),
            ));
        }

        let mut shards = Vec::with_capacity(shard_count as usize);

        if shard_count == 1 {
            // Single shard covers entire keyspace
            shards.push(ShardInfo::new(ShardId::new(), ShardRange::full()));
        } else {
            // Divide the u64 hash keyspace into equal ranges
            let boundaries = compute_range_boundaries(shard_count);

            for i in 0..shard_count as usize {
                let start = if i == 0 {
                    String::new() // Unbounded start
                } else {
                    format_hash_boundary(boundaries[i - 1])
                };

                let end = if i == shard_count as usize - 1 {
                    String::new() // Unbounded end
                } else {
                    format_hash_boundary(boundaries[i])
                };

                shards.push(ShardInfo::new(ShardId::new(), ShardRange::new(start, end)));
            }
        }

        let mut map = Self { shards, version: 1 };
        map.sort_shards();

        // Validate the created map
        map.validate()?;

        Ok(map)
    }

    /// Get all shards in the map.
    pub fn shards(&self) -> &[ShardInfo] {
        &self.shards
    }

    /// Get the number of shards.
    pub fn len(&self) -> usize {
        self.shards.len()
    }

    /// Check if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    /// Find the shard that owns the given tenant ID.
    ///
    /// Hashes the tenant ID with XXH64 and uses binary search for efficient lookup.
    /// Returns None if the map is empty or the tenant ID doesn't fall in any range
    /// (which should not happen in a valid map).
    pub fn shard_for_tenant(&self, tenant_id: &str) -> Option<&ShardInfo> {
        if self.shards.is_empty() {
            return None;
        }

        let hashed = hash_tenant(tenant_id);
        let hashed_str = hashed.as_str();

        // Binary search to find the shard whose range contains this tenant's hash
        let idx = self.shards.partition_point(|shard| {
            shard.range.start.is_empty() || shard.range.start.as_str() <= hashed_str
        });

        if idx == 0 {
            if self.shards[0].range.contains(hashed_str) {
                return Some(&self.shards[0]);
            }
            return None;
        }

        let candidate = &self.shards[idx - 1];
        if candidate.range.contains(hashed_str) {
            Some(candidate)
        } else {
            None
        }
    }

    /// Find a shard by its ID.
    pub fn get_shard(&self, id: &ShardId) -> Option<&ShardInfo> {
        self.shards.iter().find(|s| &s.id == id)
    }

    /// Find a shard by its ID and return a mutable reference.
    pub fn get_shard_mut(&mut self, id: &ShardId) -> Option<&mut ShardInfo> {
        self.shards.iter_mut().find(|s| &s.id == id)
    }

    /// Get all shard IDs.
    pub fn shard_ids(&self) -> Vec<ShardId> {
        self.shards.iter().map(|s| s.id).collect()
    }

    /// Validate that the shard map is consistent.
    ///
    /// Checks:
    /// - No overlapping ranges
    /// - No gaps in coverage (all tenant IDs map to a shard)
    /// - Ranges are properly ordered
    pub fn validate(&self) -> Result<(), ShardMapError> {
        if self.shards.is_empty() {
            return Err(ShardMapError::EmptyMap);
        }

        // Check first shard has unbounded start
        if !self.shards[0].range.is_start_unbounded() {
            return Err(ShardMapError::GapInCoverage(format!(
                "first shard must have unbounded start, got {:?}",
                self.shards[0].range.start
            )));
        }

        // Check last shard has unbounded end
        if !self.shards[self.shards.len() - 1].range.is_end_unbounded() {
            return Err(ShardMapError::GapInCoverage(format!(
                "last shard must have unbounded end, got {:?}",
                self.shards[self.shards.len() - 1].range.end
            )));
        }

        // Check adjacent ranges are contiguous (no gaps or overlaps)
        for i in 0..self.shards.len() - 1 {
            let current = &self.shards[i];
            let next = &self.shards[i + 1];

            // Current's end must equal next's start
            if current.range.end != next.range.start {
                return Err(ShardMapError::GapInCoverage(format!(
                    "gap or overlap between shards {} and {}: {} vs {}",
                    current.id, next.id, current.range.end, next.range.start
                )));
            }
        }

        // Check for duplicate shard IDs
        let mut seen_ids = std::collections::HashSet::new();
        for shard in &self.shards {
            if !seen_ids.insert(shard.id) {
                return Err(ShardMapError::DuplicateShardId(shard.id));
            }
        }

        Ok(())
    }

    /// Sort shards by their range start.
    fn sort_shards(&mut self) {
        self.shards
            .sort_by(|a, b| a.range.start.cmp(&b.range.start));
    }

    /// Add a shard to the map. Used during deserialization or testing.
    pub fn add_shard(&mut self, shard: ShardInfo) {
        self.shards.push(shard);
        self.sort_shards();
        self.version += 1;
    }

    /// Create a shard map from a list of shards.
    pub fn from_shards(shards: Vec<ShardInfo>) -> Self {
        let mut map = Self { shards, version: 1 };
        map.sort_shards();
        map
    }

    /// [SILO-COORD-INV-12] Atomically split a shard into two children.
    ///
    /// This removes the parent shard and replaces it with two child shards
    /// whose ranges are [parent_start, split_point) and [split_point, parent_end).
    ///
    /// Once children are added to the shard map, they persist indefinitely - there
    /// is no operation to delete a shard from the map. This ensures that committed
    /// splits cannot be undone and the children remain available.
    ///
    /// The children are created with parent_shard_id set to the parent's ID.
    /// Note: Cleanup status is stored in each shard's database, not in the shard map.
    ///
    /// # Returns
    /// Returns `(left_child_info, right_child_info)` on success.
    ///
    /// # Errors
    /// - `ShardNotFound` if the parent shard doesn't exist
    /// - `InvalidSplitPoint` if the split point is not valid for the parent's range
    pub fn split_shard(
        &mut self,
        parent_id: &ShardId,
        split_point: &str,
        left_child_id: ShardId,
        right_child_id: ShardId,
    ) -> Result<(ShardInfo, ShardInfo), ShardMapError> {
        // Find the parent shard
        let parent_idx = self
            .shards
            .iter()
            .position(|s| &s.id == parent_id)
            .ok_or(ShardMapError::ShardNotFound(*parent_id))?;

        let parent = &self.shards[parent_idx];

        // [SILO-COORD-INV-2] Compute child ranges
        let (left_range, right_range) = parent.range.split(split_point)?;

        // Create child shards
        let left_child = ShardInfo::from_split(left_child_id, left_range, *parent_id);
        let right_child = ShardInfo::from_split(right_child_id, right_range, *parent_id);

        // Remove parent and add children
        self.shards.remove(parent_idx);
        self.shards.push(left_child.clone());
        self.shards.push(right_child.clone());

        // Re-sort and bump version
        self.sort_shards();
        self.version += 1;

        // Validate the resulting map
        self.validate()?;

        Ok((left_child, right_child))
    }
}

impl Default for ShardMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur when working with shard maps.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ShardMapError {
    #[error("shard map is empty")]
    EmptyMap,

    #[error("invalid shard count: {0}")]
    InvalidShardCount(String),

    #[error("gap in keyspace coverage: {0}")]
    GapInCoverage(String),

    #[error("overlapping shard ranges: {0}")]
    OverlappingRanges(String),

    #[error("duplicate shard ID: {0}")]
    DuplicateShardId(ShardId),

    #[error("shard not found: {0}")]
    ShardNotFound(ShardId),

    #[error("invalid split point: {0}")]
    InvalidSplitPoint(String),

    #[error("split already in progress for shard: {0}")]
    SplitAlreadyInProgress(ShardId),
}

/// Format a u64 hash-space value as a 16-character hex string for use in shard ranges.
pub fn format_hash_boundary(val: u64) -> String {
    format!("{:016x}", val)
}

/// Compute evenly-spaced range boundaries for the given shard count.
///
/// Returns `shard_count - 1` boundary values that divide the u64 hash
/// keyspace into equal-sized partitions.
fn compute_range_boundaries(shard_count: u32) -> Vec<u64> {
    if shard_count <= 1 {
        return Vec::new();
    }

    let mut boundaries = Vec::with_capacity((shard_count - 1) as usize);
    let space: u128 = 1u128 << 64; // 2^64

    for i in 1..shard_count {
        let boundary = (i as u128 * space / shard_count as u128) as u64;
        boundaries.push(boundary);
    }

    boundaries
}
