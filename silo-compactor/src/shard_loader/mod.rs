use async_trait::async_trait;

use crate::error::CompactorError;
use crate::shard_map::ShardMap;

pub mod etcd;
pub mod k8s;

#[async_trait]
pub trait ShardMapLoader: Send + Sync {
    async fn load(&self) -> Result<ShardMap, CompactorError>;
}
