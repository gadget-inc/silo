use std::collections::HashMap;
use std::sync::Arc;

use slatedb::{Db, Error as SlateError};
use slatedb::object_store::ObjectStore;
use thiserror::Error;

use crate::settings::{Backend, DatabaseConfig};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("slatedb error: {0}")]
    Slate(#[from] SlateError),
    #[error("unsupported backend: {0:?}")]
    UnsupportedBackend(Backend),
    #[error("invalid object store url: {0}")]
    InvalidUrl(String),
}

/// Factory for opening and holding SlateDB instances.
pub struct DbFactory {
    instances: HashMap<String, Db>,
}

impl DbFactory {
    pub fn new() -> Self {
        Self {
            instances: HashMap::new(),
        }
    }

    pub fn get(&self, name: &str) -> Option<&Db> {
        self.instances.get(name)
    }

    pub async fn open(&mut self, cfg: &DatabaseConfig) -> Result<&Db, StorageError> {
        let object_store = resolve_object_store(&cfg.backend, &cfg.path)?;
        let db = Db::open(cfg.path.as_str(), object_store).await?;
        self.instances.insert(cfg.name.clone(), db);
        Ok(self.instances.get(&cfg.name).expect("inserted"))
    }
}

fn resolve_object_store(backend: &Backend, path: &str) -> Result<Arc<dyn ObjectStore>, StorageError> {
    match backend {
        Backend::Fs => {
            // Use object_store's LocalFileSystem scoped to the provided directory
            let fs = object_store::local::LocalFileSystem::new_with_prefix(path)
                .map_err(|e| StorageError::InvalidUrl(format!("{}", e)))?;
            Ok(Arc::new(fs))
        }
        Backend::Memory => {
            Ok(Arc::new(object_store::memory::InMemory::new()))
        }
        Backend::S3 | Backend::Url => {
            // Interpret path as a URL understood by SlateDB's resolver, e.g. s3://bucket/prefix
            let store = Db::resolve_object_store(path)?;
            Ok(store)
        }
    }
}


