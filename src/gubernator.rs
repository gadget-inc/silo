//! Gubernator rate limit client with request coalescing.
//!
//! This module provides a client for the Gubernator rate limiting service.
//! It supports request coalescing to batch multiple rate limit checks into
//! a single RPC call for efficiency.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error};

use crate::pb::gubernator::{
    Algorithm, GetRateLimitsReq, RateLimitReq, RateLimitResp, Status, v1_client::V1Client,
};

/// Error types for Gubernator client operations
#[derive(Debug, Error, Clone)]
pub enum GubernatorError {
    #[error("connection error: {0}")]
    Connection(String),

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("rate limit check failed: {0}")]
    RateLimitError(String),

    #[error("request cancelled")]
    Cancelled,

    #[error("client not connected")]
    NotConnected,

    #[error("coalescer channel closed")]
    ChannelClosed,

    #[error("rate limiting not configured - no gubernator address provided")]
    NotConfigured,
}

impl From<tonic::transport::Error> for GubernatorError {
    fn from(e: tonic::transport::Error) -> Self {
        GubernatorError::Connection(e.to_string())
    }
}

impl From<tonic::Status> for GubernatorError {
    fn from(e: tonic::Status) -> Self {
        GubernatorError::Rpc(e.to_string())
    }
}

/// Result of a rate limit check
#[derive(Debug, Clone)]
pub struct RateLimitResult {
    /// Whether the request is under the limit
    pub under_limit: bool,
    /// The configured limit
    pub limit: i64,
    /// Remaining requests in the current window
    pub remaining: i64,
    /// Unix timestamp (ms) when the limit resets
    pub reset_time_ms: i64,
    /// Error message if any
    pub error: Option<String>,
}

impl From<RateLimitResp> for RateLimitResult {
    fn from(resp: RateLimitResp) -> Self {
        Self {
            under_limit: resp.status == Status::UnderLimit as i32,
            limit: resp.limit,
            remaining: resp.remaining,
            reset_time_ms: resp.reset_time,
            error: if resp.error.is_empty() {
                None
            } else {
                Some(resp.error)
            },
        }
    }
}

/// Trait for rate limit clients (real, mock, or null)
#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait RateLimitClient: Send + Sync {
    /// Check a rate limit
    async fn check_rate_limit(
        &self,
        name: &str,
        unique_key: &str,
        hits: i64,
        limit: i64,
        duration_ms: i64,
        algorithm: Algorithm,
        behavior: i32,
    ) -> Result<RateLimitResult, GubernatorError>;

    /// Perform a health check
    async fn health_check(&self) -> Result<(String, i32), GubernatorError>;
}

/// A null client that always returns an error - used when gubernator is not configured
pub struct NullGubernatorClient;

impl NullGubernatorClient {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl Default for NullGubernatorClient {
    fn default() -> Self {
        Self
    }
}

#[async_trait]
impl RateLimitClient for NullGubernatorClient {
    async fn check_rate_limit(
        &self,
        _name: &str,
        _unique_key: &str,
        _hits: i64,
        _limit: i64,
        _duration_ms: i64,
        _algorithm: Algorithm,
        _behavior: i32,
    ) -> Result<RateLimitResult, GubernatorError> {
        Err(GubernatorError::NotConfigured)
    }

    async fn health_check(&self) -> Result<(String, i32), GubernatorError> {
        Err(GubernatorError::NotConfigured)
    }
}

/// A pending rate limit request waiting to be coalesced
struct PendingRequest {
    req: RateLimitReq,
    tx: oneshot::Sender<Result<RateLimitResult, GubernatorError>>,
}

/// Configuration for the Gubernator client
#[derive(Debug, Clone)]
pub struct GubernatorConfig {
    /// Gubernator server address (e.g., "http://localhost:1051")
    pub address: String,
    /// Maximum time to wait for coalescing before sending a batch
    pub coalesce_interval_ms: u64,
    /// Maximum number of requests to batch together
    pub max_batch_size: usize,
    /// Connection timeout
    pub connect_timeout_ms: u64,
    /// Request timeout
    pub request_timeout_ms: u64,
}

impl Default for GubernatorConfig {
    fn default() -> Self {
        Self {
            address: "http://localhost:1051".to_string(),
            coalesce_interval_ms: 5,
            max_batch_size: 100,
            connect_timeout_ms: 5000,
            request_timeout_ms: 10000,
        }
    }
}

/// Gubernator rate limit client with request coalescing
pub struct GubernatorClient {
    config: GubernatorConfig,
    client: RwLock<Option<V1Client<Channel>>>,
    coalesce_tx: mpsc::Sender<PendingRequest>,
    _coalesce_handle: tokio::task::JoinHandle<()>,
}

impl GubernatorClient {
    /// Create a new Gubernator client
    pub fn new(config: GubernatorConfig) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<PendingRequest>(1000);

        let client = Arc::new(Self {
            config: config.clone(),
            client: RwLock::new(None),
            coalesce_tx: tx,
            _coalesce_handle: tokio::spawn(async {}), // Placeholder, will be replaced
        });

        // Start the coalescing loop
        let client_clone = Arc::clone(&client);
        let handle = tokio::spawn(Self::coalesce_loop(client_clone, rx, config));

        // Replace the placeholder handle
        // Note: This is a bit awkward but necessary for the self-referential setup
        // In practice, the handle is just for cleanup and the loop runs independently
        drop(handle);

        client
    }

    /// Connect to the Gubernator server
    pub async fn connect(&self) -> Result<(), GubernatorError> {
        let endpoint = Endpoint::from_shared(self.config.address.clone())
            .map_err(|e| GubernatorError::Connection(e.to_string()))?
            .connect_timeout(Duration::from_millis(self.config.connect_timeout_ms))
            .timeout(Duration::from_millis(self.config.request_timeout_ms));

        let channel = endpoint.connect().await?;
        let grpc_client = V1Client::new(channel);

        let mut client = self.client.write().await;
        *client = Some(grpc_client);

        debug!(address = %self.config.address, "connected to Gubernator");
        Ok(())
    }

    /// Check if the client is connected
    pub async fn is_connected(&self) -> bool {
        self.client.read().await.is_some()
    }

    /// Check multiple rate limits in a single batch (bypasses coalescing)
    pub async fn check_rate_limits_batch(
        &self,
        requests: Vec<RateLimitReq>,
    ) -> Result<Vec<RateLimitResult>, GubernatorError> {
        let client = self.client.read().await;
        let Some(mut client) = client.clone() else {
            return Err(GubernatorError::NotConnected);
        };

        let resp = client
            .get_rate_limits(GetRateLimitsReq { requests })
            .await?;

        Ok(resp
            .into_inner()
            .responses
            .into_iter()
            .map(RateLimitResult::from)
            .collect())
    }

    /// The coalescing loop that batches requests together
    async fn coalesce_loop(
        client: Arc<Self>,
        mut rx: mpsc::Receiver<PendingRequest>,
        config: GubernatorConfig,
    ) {
        let mut pending: Vec<PendingRequest> = Vec::with_capacity(config.max_batch_size);
        let coalesce_duration = Duration::from_millis(config.coalesce_interval_ms);

        loop {
            // Wait for the first request or check if channel is closed
            let first = match tokio::time::timeout(Duration::from_secs(60), rx.recv()).await {
                Ok(Some(req)) => req,
                Ok(None) => {
                    debug!("coalesce channel closed, exiting loop");
                    break;
                }
                Err(_) => continue, // Timeout, just loop again
            };

            pending.push(first);

            // Coalesce additional requests within the window
            let deadline = tokio::time::Instant::now() + coalesce_duration;
            loop {
                if pending.len() >= config.max_batch_size {
                    break;
                }

                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(req)) => pending.push(req),
                    Ok(None) => break, // Channel closed
                    Err(_) => break,   // Timeout reached
                }
            }

            // Send the batch
            if !pending.is_empty() {
                let batch: Vec<PendingRequest> = std::mem::take(&mut pending);
                let client_clone = Arc::clone(&client);

                // Spawn a task to handle this batch so we don't block the loop
                tokio::spawn(async move {
                    Self::process_batch(client_clone, batch).await;
                });
            }
        }
    }

    /// Process a batch of coalesced requests
    async fn process_batch(client: Arc<Self>, batch: Vec<PendingRequest>) {
        let requests: Vec<RateLimitReq> = batch.iter().map(|p| p.req.clone()).collect();
        let request_count = requests.len();

        debug!(count = request_count, "processing coalesced batch");

        // Get the GRPC client
        let grpc_client = {
            let guard = client.client.read().await;
            guard.clone()
        };

        let Some(mut grpc_client) = grpc_client else {
            // Not connected, fail all requests
            for pending in batch {
                let _ = pending.tx.send(Err(GubernatorError::NotConnected));
            }
            return;
        };

        // Make the RPC call
        match grpc_client
            .get_rate_limits(GetRateLimitsReq { requests })
            .await
        {
            Ok(resp) => {
                let responses = resp.into_inner().responses;

                // Match responses to requests
                for (pending, resp) in batch.into_iter().zip(responses.into_iter()) {
                    let result = RateLimitResult::from(resp);
                    let _ = pending.tx.send(Ok(result));
                }
            }
            Err(e) => {
                error!(error = %e, "batch rate limit check failed");
                let err = GubernatorError::Rpc(e.to_string());

                // Fail all requests in the batch
                for pending in batch {
                    let _ = pending
                        .tx
                        .send(Err(GubernatorError::RateLimitError(err.to_string())));
                }
            }
        }
    }
}

#[async_trait]
impl RateLimitClient for GubernatorClient {
    async fn check_rate_limit(
        &self,
        name: &str,
        unique_key: &str,
        hits: i64,
        limit: i64,
        duration_ms: i64,
        algorithm: Algorithm,
        behavior: i32,
    ) -> Result<RateLimitResult, GubernatorError> {
        let req = RateLimitReq {
            name: name.to_string(),
            unique_key: unique_key.to_string(),
            hits,
            limit,
            duration: duration_ms,
            algorithm: algorithm as i32,
            behavior,
            burst: 0,
            metadata: HashMap::new(),
            created_at: 0,
        };

        let (tx, rx) = oneshot::channel();
        self.coalesce_tx
            .send(PendingRequest { req, tx })
            .await
            .map_err(|_| GubernatorError::ChannelClosed)?;

        rx.await.map_err(|_| GubernatorError::Cancelled)?
    }

    async fn health_check(&self) -> Result<(String, i32), GubernatorError> {
        let client = self.client.read().await;
        let Some(mut client) = client.clone() else {
            return Err(GubernatorError::NotConnected);
        };

        let resp = client
            .health_check(crate::pb::gubernator::HealthCheckReq {})
            .await?;
        let inner = resp.into_inner();
        Ok((inner.status, inner.peer_count))
    }
}

/// Mock Gubernator client that simulates rate limiting behavior - useful for testing
pub struct MockGubernatorClient {
    /// Map of rate limit keys to their current counts
    counters: Mutex<HashMap<String, AtomicI64>>,
}

impl Default for MockGubernatorClient {
    fn default() -> Self {
        Self::new()
    }
}

impl MockGubernatorClient {
    pub fn new() -> Self {
        Self {
            counters: Mutex::new(HashMap::new()),
        }
    }

    /// Create a new mock client wrapped in Arc
    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Reset all counters
    pub async fn reset(&self) {
        let mut counters = self.counters.lock().await;
        counters.clear();
    }

    /// Reset a specific counter
    pub async fn reset_key(&self, name: &str, unique_key: &str) {
        let key = format!("{}:{}", name, unique_key);
        let mut counters = self.counters.lock().await;
        counters.remove(&key);
    }
}

#[async_trait]
impl RateLimitClient for MockGubernatorClient {
    async fn check_rate_limit(
        &self,
        name: &str,
        unique_key: &str,
        hits: i64,
        limit: i64,
        duration_ms: i64,
        _algorithm: Algorithm,
        _behavior: i32,
    ) -> Result<RateLimitResult, GubernatorError> {
        let key = format!("{}:{}", name, unique_key);
        let mut counters = self.counters.lock().await;

        let counter = counters.entry(key).or_insert_with(|| AtomicI64::new(0));

        let current = counter.fetch_add(hits, Ordering::SeqCst);
        let new_value = current + hits;
        let remaining = (limit - new_value).max(0);
        let under_limit = new_value <= limit;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        Ok(RateLimitResult {
            under_limit,
            limit,
            remaining,
            reset_time_ms: now_ms + duration_ms,
            error: None,
        })
    }

    async fn health_check(&self) -> Result<(String, i32), GubernatorError> {
        Ok(("healthy".to_string(), 1))
    }
}
