#![allow(dead_code)]

use anyhow::Context;
use etcd_client::{Client, ConnectOptions};

/// Legacy compatibility: Connect to etcd without starting a coordinator.
/// Used for tests that need raw etcd access.
pub struct EtcdConnection {
    client: Client,
}

impl EtcdConnection {
    pub async fn connect(cfg: &silo::settings::CoordinationConfig) -> anyhow::Result<Self> {
        let endpoints = if cfg.etcd_endpoints.is_empty() {
            vec!["http://127.0.0.1:2379".to_string()]
        } else {
            cfg.etcd_endpoints.clone()
        };
        let opts = ConnectOptions::default();
        let client = Client::connect(endpoints, Some(opts))
            .await
            .context("failed to connect to etcd")?;
        Ok(Self { client })
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }
}
