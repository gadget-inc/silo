//! Range-based shard coordination types.
//!
//! This module defines the core data structures for range-based sharding,
//! where the tenant_id keyspace is split into lexicographical ranges,
//! each owned by a shard with a unique UUID identity.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

use crate::coordination::SplitPhase;

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

/// A lexicographical range of tenant IDs owned by a shard.
///
/// Ranges are defined as `[start, end)` where:
/// - `start` is inclusive (empty string means unbounded start, i.e., from the beginning)
/// - `end` is exclusive (empty string means unbounded end, i.e., to infinity)
///
/// Tenant IDs are compared lexicographically (byte-by-byte string comparison).
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
        // [SILO-RANGE-1] Validate that split_point is within this range
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

    /// Compute the lexicographic midpoint of this range for auto-split.
    ///
    /// Returns None if the range cannot be split (e.g., single character range).
    pub fn midpoint(&self) -> Option<String> {
        // For unbounded ranges, we need to pick reasonable bounds
        let effective_start = if self.start.is_empty() {
            "0".to_string()
        } else {
            self.start.clone()
        };

        let effective_end = if self.end.is_empty() {
            // Use a reasonably high value (all 'z's)
            "zzzzzzzz".to_string()
        } else {
            self.end.clone()
        };

        // Simple midpoint: take first char of start, compute midpoint with first char of end
        // This is a simplified approach; a production system might use more sophisticated logic
        if effective_start >= effective_end {
            return None;
        }

        // Binary search through the string space
        let start_bytes = effective_start.as_bytes();
        let end_bytes = effective_end.as_bytes();

        // Find the first differing position
        let min_len = start_bytes.len().min(end_bytes.len());
        let mut diff_pos = 0;
        while diff_pos < min_len && start_bytes[diff_pos] == end_bytes[diff_pos] {
            diff_pos += 1;
        }

        if diff_pos == min_len {
            // One is a prefix of the other
            if start_bytes.len() < end_bytes.len() {
                // Start is shorter, we can split by extending start
                let mut mid = effective_start.clone();
                // Add a character that's midway through ASCII printable range
                mid.push('M');
                if mid.as_str() > effective_start.as_str() && mid.as_str() < effective_end.as_str()
                {
                    return Some(mid);
                }
            }
            return None;
        }

        // Compute midpoint at the differing position
        let start_char = start_bytes[diff_pos];
        let end_char = end_bytes[diff_pos];

        if end_char <= start_char + 1 {
            // Too close to split at this level, extend
            let mut mid = String::from_utf8_lossy(&start_bytes[..=diff_pos]).to_string();
            mid.push('~'); // High ASCII value
            if mid.as_str() > effective_start.as_str() && mid.as_str() < effective_end.as_str() {
                return Some(mid);
            }
            return None;
        }

        let mid_char = start_char + (end_char - start_char) / 2;
        let mut result = String::from_utf8_lossy(&start_bytes[..diff_pos]).to_string();
        result.push(mid_char as char);

        // Validate the result
        if result.as_str() > effective_start.as_str() && result.as_str() < effective_end.as_str() {
            Some(result)
        } else {
            None
        }
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
        }
    }

    /// Create a new shard info with a specific creation time.
    pub fn with_created_at(id: ShardId, range: ShardRange, created_at_ms: i64) -> Self {
        Self {
            id,
            range,
            created_at_ms,
            parent_shard_id: None,
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
        }
    }

    /// Check if this shard's range contains the given tenant ID.
    pub fn contains(&self, tenant_id: &str) -> bool {
        self.range.contains(tenant_id)
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

    /// Advance to the next phase.
    pub fn advance_phase(&mut self) {
        self.phase = match self.phase {
            SplitPhase::SplitRequested => SplitPhase::SplitPausing,
            SplitPhase::SplitPausing => SplitPhase::SplitCloning,
            SplitPhase::SplitCloning => SplitPhase::SplitUpdatingMap,
            SplitPhase::SplitUpdatingMap => SplitPhase::SplitComplete,
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
    /// Divides the keyspace evenly among shards using hex prefixes.
    /// For example, with 16 shards, each shard owns 1/16th of the keyspace
    /// based on the first hex digit of the tenant ID hash.
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
            // Divide keyspace into equal ranges
            // We use a simple approach: divide into ranges based on first character
            // For better distribution, we create ranges using hex boundaries
            let boundaries = compute_range_boundaries(shard_count);

            for i in 0..shard_count as usize {
                let start = if i == 0 {
                    String::new() // Unbounded start
                } else {
                    boundaries[i - 1].clone()
                };

                let end = if i == shard_count as usize - 1 {
                    String::new() // Unbounded end
                } else {
                    boundaries[i].clone()
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
    /// Uses binary search for efficient lookup.
    /// Returns None if the map is empty or the tenant ID doesn't fall in any range
    /// (which should not happen in a valid map).
    pub fn shard_for_tenant(&self, tenant_id: &str) -> Option<&ShardInfo> {
        if self.shards.is_empty() {
            return None;
        }

        // Binary search to find the shard whose range contains this tenant
        // We search for the rightmost shard where start <= tenant_id
        let idx = self.shards.partition_point(|shard| {
            // For unbounded start (empty string), it's always <= tenant_id
            shard.range.start.is_empty() || shard.range.start.as_str() <= tenant_id
        });

        // partition_point returns the index where we'd insert, so we need idx - 1
        if idx == 0 {
            // tenant_id is before all shards - check first shard
            // This can happen if first shard has unbounded start
            if self.shards[0].range.contains(tenant_id) {
                return Some(&self.shards[0]);
            }
            return None;
        }

        let candidate = &self.shards[idx - 1];
        if candidate.range.contains(tenant_id) {
            Some(candidate)
        } else {
            None
        }
    }

    /// Find a shard by its ID.
    pub fn get_shard(&self, id: &ShardId) -> Option<&ShardInfo> {
        self.shards.iter().find(|s| &s.id == id)
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

    /// [SILO-SPLIT-MAP-1] Atomically split a shard into two children.
    ///
    /// This removes the parent shard and replaces it with two child shards
    /// whose ranges are [parent_start, split_point) and [split_point, parent_end).
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

/// Compute evenly-spaced range boundaries for the given shard count.
///
/// Returns `shard_count - 1` boundary strings that divide the keyspace.
/// Uses a simple hex-based scheme for predictable distribution.
fn compute_range_boundaries(shard_count: u32) -> Vec<String> {
    if shard_count <= 1 {
        return Vec::new();
    }

    let mut boundaries = Vec::with_capacity((shard_count - 1) as usize);

    // We'll use a simple scheme based on hex characters
    // For up to 16 shards, we use single hex digits
    // For more, we use multi-character boundaries
    if shard_count <= 16 {
        // Use hex digits 0-f to create boundaries
        let hex_chars: Vec<char> = "0123456789abcdef".chars().collect();
        let step = 16 / shard_count;
        for i in 1..shard_count {
            let idx = (i * step) as usize;
            if idx < hex_chars.len() {
                boundaries.push(hex_chars[idx].to_string());
            }
        }
    } else if shard_count <= 256 {
        // Use two hex digits
        for i in 1..shard_count {
            let boundary = (i * 256 / shard_count) as u8;
            boundaries.push(format!("{:02x}", boundary));
        }
    } else {
        // Use four hex digits for larger shard counts
        for i in 1..shard_count {
            let boundary = (i as u64 * 65536 / shard_count as u64) as u16;
            boundaries.push(format!("{:04x}", boundary));
        }
    }

    boundaries
}
