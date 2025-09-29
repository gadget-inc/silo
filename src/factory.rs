use std::collections::HashMap;

use crate::job_store_shard::JobStoreShard;
use crate::job_store_shard::JobStoreShardError;
use crate::settings::{DatabaseConfig, DatabaseTemplate};
use thiserror::Error;

/// Factory for opening and holding `Shard` instances by name.
pub struct ShardFactory {
    instances: HashMap<String, JobStoreShard>,
    template: DatabaseTemplate,
}

impl ShardFactory {
    pub fn new(template: DatabaseTemplate) -> Self {
        Self {
            instances: HashMap::new(),
            template,
        }
    }

    pub fn get(&self, name: &str) -> Option<&JobStoreShard> {
        self.instances.get(name)
    }

    /// Open a shard using the shared database template.
    pub async fn open(
        &mut self,
        shard_number: usize,
    ) -> Result<&JobStoreShard, JobStoreShardError> {
        let name = shard_number.to_string();
        let path = self
            .template
            .path
            .replace("%shard%", &name)
            .replace("{shard}", &name);
        let cfg = DatabaseConfig {
            name: name.clone(),
            backend: self.template.backend.clone(),
            path,
            flush_interval_ms: None, // Use SlateDB's default in production
        };
        let shard = JobStoreShard::open(&cfg).await?;
        self.instances.insert(cfg.name.clone(), shard);
        Ok(self.instances.get(&cfg.name).expect("inserted"))
    }

    pub fn instances(&self) -> &HashMap<String, JobStoreShard> {
        &self.instances
    }

    /// Close all shards gracefully. Returns all errors if any shards fail to close.
    pub async fn close_all(&self) -> Result<(), CloseAllError> {
        let mut errors: Vec<(String, JobStoreShardError)> = Vec::new();
        for (name, shard) in self.instances.iter() {
            if let Err(e) = shard.close().await {
                errors.push((name.clone(), e));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(CloseAllError { errors })
        }
    }
}

#[derive(Debug, Error)]
pub struct CloseAllError {
    pub errors: Vec<(String, JobStoreShardError)>,
}

impl std::fmt::Display for CloseAllError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} shard(s) failed to close", self.errors.len())
    }
}
