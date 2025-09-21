use std::collections::HashMap;

use crate::settings::DatabaseConfig;
use crate::shard::Shard;
use crate::shard::ShardError;

/// Factory for opening and holding `Shard` instances by name.
pub struct ShardFactory {
    instances: HashMap<String, Shard>,
}

impl ShardFactory {
    pub fn new() -> Self {
        Self { instances: HashMap::new() }
    }

    pub fn get(&self, name: &str) -> Option<&Shard> {
        self.instances.get(name)
    }

    pub async fn open(&mut self, cfg: &DatabaseConfig) -> Result<&Shard, ShardError> {
        let shard = Shard::open(cfg).await?;
        self.instances.insert(cfg.name.clone(), shard);
        Ok(self.instances.get(&cfg.name).expect("inserted"))
    }
}


