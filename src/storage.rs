use std::sync::Arc;

use slatedb::object_store::ObjectStore;
use slatedb::{Db, Error as SlateError};
use std::fs;
use std::path::Path;
use thiserror::Error;

use crate::settings::Backend;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("slatedb error: {0}")]
    Slate(#[from] SlateError),
    #[error("invalid object store url: {0}")]
    InvalidUrl(String),
}

pub(crate) fn resolve_object_store(
    backend: &Backend,
    path: &str,
) -> Result<Arc<dyn ObjectStore>, StorageError> {
    match backend {
        Backend::Fs => {
            // Ensure the directory exists before creating the LocalFileSystem root
            let root = Path::new(path);
            if !root.exists() {
                fs::create_dir_all(root).map_err(|e| {
                    StorageError::InvalidUrl(format!("failed to create fs root {}: {}", path, e))
                })?;
            }
            // Use slatedb's re-exported object_store to ensure trait compatibility
            let fs = slatedb::object_store::local::LocalFileSystem::new_with_prefix(path)
                .map_err(|e| StorageError::InvalidUrl(format!("{}", e)))?;
            Ok(Arc::new(fs))
        }
        Backend::Memory => Ok(Arc::new(slatedb::object_store::memory::InMemory::new())),
        Backend::S3 | Backend::Url => {
            // Interpret path as a URL understood by SlateDB's resolver, e.g. s3://bucket/prefix
            let store = Db::resolve_object_store(path)?;
            Ok(store)
        }
    }
}
