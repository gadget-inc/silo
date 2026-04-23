use std::fmt;

use serde::Deserialize;
use uuid::Uuid;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(transparent)]
pub struct ShardId(pub Uuid);

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ShardId({})", self.0)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ShardInfo {
    pub id: ShardId,
    #[serde(default)]
    pub placement_ring: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ShardMap {
    pub shards: Vec<ShardInfo>,
    #[serde(default)]
    pub version: u64,
}

impl ShardMap {
    pub fn shard_ids(&self, ring_filter: Option<&str>) -> Vec<ShardId> {
        self.shards
            .iter()
            .filter(|s| match ring_filter {
                None => true,
                Some(r) => s.placement_ring.as_deref() == Some(r),
            })
            .map(|s| s.id)
            .collect()
    }
}
