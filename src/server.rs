use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::factory::{CloseAllError, ShardFactory};
use crate::job_attempt::AttemptOutcome;
use crate::job_store_shard::{JobStoreShard, JobStoreShardError, DEFAULT_LEASE_MS};
use crate::pb::silo_server::{Silo, SiloServer};
use crate::pb::*;
use crate::settings::AppConfig;

fn map_err(e: JobStoreShardError) -> Status {
    match e {
        JobStoreShardError::JobNotFound(_) => Status::not_found("job not found"),
        other => Status::internal(other.to_string()),
    }
}

/// gRPC service implementation backed by a `ShardFactory`.
#[derive(Clone)]
pub struct SiloService {
    factory: Arc<ShardFactory>,
    cfg: AppConfig,
}

impl SiloService {
    pub fn new(factory: Arc<ShardFactory>, cfg: AppConfig) -> Self {
        Self { factory, cfg }
    }

    #[allow(clippy::result_large_err)]
    fn shard(&self, name: &str) -> Result<Arc<JobStoreShard>, Status> {
        self.factory
            .get(name)
            .ok_or_else(|| Status::not_found("shard not found"))
    }

    #[allow(clippy::result_large_err)]
    fn validate_tenant(&self, tenant: Option<&str>) -> Result<String, Status> {
        let enabled = self.cfg.tenancy.enabled;
        let present = tenant.and_then(|t| if t.is_empty() { None } else { Some(t) });
        match (enabled, present) {
            (true, Some(t)) => {
                if t.chars().count() > 64 {
                    return Err(Status::invalid_argument("invalid tenant id"));
                }
                Ok(t.to_string())
            }
            (true, None) => Err(Status::invalid_argument("tenant id required")),
            (false, Some(_)) => Err(Status::invalid_argument("tenant id not accepted")),
            (false, None) => Ok("-".to_string()),
        }
    }
}

#[tonic::async_trait]
impl Silo for SiloService {
    async fn enqueue(
        &self,
        req: Request<EnqueueRequest>,
    ) -> Result<Response<EnqueueResponse>, Status> {
        let r = req.into_inner();
        let shard = self.shard(&r.shard)?;
        let tenant = self.validate_tenant(r.tenant.as_deref())?;
        let payload_bytes = r
            .payload
            .as_ref()
            .map(|p| p.data.clone())
            .unwrap_or_default();
        let payload = serde_json::from_slice::<serde_json::Value>(&payload_bytes)
            .unwrap_or(serde_json::Value::Null);
        let retry = r.retry_policy.map(|rp| crate::retry::RetryPolicy {
            retry_count: rp.retry_count,
            initial_interval_ms: rp.initial_interval_ms,
            max_interval_ms: rp.max_interval_ms,
            randomize_interval: rp.randomize_interval,
            backoff_factor: rp.backoff_factor,
        });
        let limits = r
            .concurrency_limits
            .into_iter()
            .map(|l| crate::job::ConcurrencyLimit {
                key: l.key,
                max_concurrency: l.max_concurrency,
            })
            .collect();
        // Validate metadata constraints: <=16 entries, key < 64 chars, value < u16::MAX
        if r.metadata.len() > 16 {
            return Err(Status::invalid_argument(
                "metadata has too many entries (max 16)",
            ));
        }
        for (k, v) in &r.metadata {
            if k.chars().count() >= 64 {
                return Err(Status::invalid_argument(
                    "metadata key too long (must be < 64 chars)",
                ));
            }
            if v.len() as u128 >= (u16::MAX as u128) {
                return Err(Status::invalid_argument(
                    "metadata value too long (must be < u16::MAX bytes)",
                ));
            }
        }
        let metadata: Option<Vec<(String, String)>> = if r.metadata.is_empty() {
            None
        } else {
            Some(r.metadata.into_iter().collect())
        };
        let id = shard
            .enqueue_with_metadata(
                &tenant,
                if r.id.is_empty() { None } else { Some(r.id) },
                r.priority as u8,
                r.start_at_ms,
                retry,
                payload,
                limits,
                metadata,
            )
            .await
            .map_err(map_err)?;
        Ok(Response::new(EnqueueResponse { id }))
    }

    async fn get_job(
        &self,
        req: Request<GetJobRequest>,
    ) -> Result<Response<GetJobResponse>, Status> {
        let r = req.into_inner();
        let shard = self.shard(&r.shard)?;
        let tenant = self.validate_tenant(r.tenant.as_deref())?;
        let Some(view) = shard.get_job(&tenant, &r.id).await.map_err(map_err)? else {
            return Err(Status::not_found("job not found"));
        };
        let retry = view.retry_policy();
        let retry_policy = retry.map(|p| RetryPolicy {
            retry_count: p.retry_count,
            initial_interval_ms: p.initial_interval_ms,
            max_interval_ms: p.max_interval_ms,
            randomize_interval: p.randomize_interval,
            backoff_factor: p.backoff_factor,
        });
        let resp = GetJobResponse {
            id: view.id().to_string(),
            priority: view.priority() as u32,
            enqueue_time_ms: view.enqueue_time_ms(),
            payload: Some(JsonValueBytes {
                data: view.payload_bytes().to_vec(),
            }),
            retry_policy,
            concurrency_limits: view
                .concurrency_limits()
                .into_iter()
                .map(|l| ConcurrencyLimit {
                    key: l.key,
                    max_concurrency: l.max_concurrency,
                })
                .collect(),
            metadata: view.metadata().into_iter().collect(),
        };
        Ok(Response::new(resp))
    }

    async fn delete_job(
        &self,
        req: Request<DeleteJobRequest>,
    ) -> Result<Response<DeleteJobResponse>, Status> {
        let r = req.into_inner();
        let shard = self.shard(&r.shard)?;
        let tenant = self.validate_tenant(r.tenant.as_deref())?;
        shard.delete_job(&tenant, &r.id).await.map_err(map_err)?;
        Ok(Response::new(DeleteJobResponse {}))
    }

    async fn lease_tasks(
        &self,
        req: Request<LeaseTasksRequest>,
    ) -> Result<Response<LeaseTasksResponse>, Status> {
        let r = req.into_inner();
        let shard = self.shard(&r.shard)?;
        let tenant = self.validate_tenant(r.tenant.as_deref())?;
        let tasks = shard
            .dequeue(&tenant, &r.worker_id, r.max_tasks as usize)
            .await
            .map_err(map_err)?;
        let mut out = Vec::with_capacity(tasks.len());
        for lt in tasks {
            let job = lt.job();
            let attempt = lt.attempt();
            out.push(Task {
                id: attempt.task_id().to_string(),
                job_id: job.id().to_string(),
                attempt_number: attempt.attempt_number(),
                lease_ms: DEFAULT_LEASE_MS,
                payload: Some(JsonValueBytes {
                    data: job.payload_bytes().to_vec(),
                }),
                priority: job.priority() as u32,
            });
        }
        Ok(Response::new(LeaseTasksResponse { tasks: out }))
    }

    async fn report_outcome(
        &self,
        req: Request<ReportOutcomeRequest>,
    ) -> Result<Response<ReportOutcomeResponse>, Status> {
        let r = req.into_inner();
        let shard = self.shard(&r.shard)?;
        let tenant = self.validate_tenant(r.tenant.as_deref())?;
        let outcome = match r
            .outcome
            .ok_or_else(|| Status::invalid_argument("missing outcome"))?
        {
            report_outcome_request::Outcome::Success(s) => {
                AttemptOutcome::Success { result: s.data }
            }
            report_outcome_request::Outcome::Failure(f) => AttemptOutcome::Error {
                error_code: f.code,
                error: f.data,
            },
        };
        shard
            .report_attempt_outcome(&tenant, &r.task_id, outcome)
            .await
            .map_err(map_err)?;
        Ok(Response::new(ReportOutcomeResponse {}))
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let r = req.into_inner();
        let shard = self.shard(&r.shard)?;
        shard
            .heartbeat_task(&r.worker_id, &r.task_id)
            .await
            .map_err(map_err)?;
        Ok(Response::new(HeartbeatResponse {}))
    }

    async fn query(&self, req: Request<QueryRequest>) -> Result<Response<QueryResponse>, Status> {
        let r = req.into_inner();
        let shard = self.shard(&r.shard)?;
        let _tenant = self.validate_tenant(r.tenant.as_deref())?;

        // Get the cached query engine for this shard
        let query_engine = shard.query_engine();

        // Execute query
        let dataframe = query_engine
            .sql(&r.sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("SQL error: {}", e)))?;

        // Get schema before consuming dataframe
        let schema = Arc::new(dataframe.schema().as_arrow().clone());

        // Collect results
        let batches = dataframe
            .collect()
            .await
            .map_err(|e| Status::internal(format!("Query execution failed: {}", e)))?;

        // Use schema from dataframe or first batch
        let schema = if let Some(batch) = batches.first() {
            batch.schema()
        } else {
            schema
        };

        let columns: Vec<ColumnInfo> = schema
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().to_string(),
                data_type: format!("{:?}", f.data_type()),
            })
            .collect();

        // Convert batches to JSON rows
        let mut rows = Vec::new();
        for batch in batches {
            // Convert each batch to JSON using Arrow's built-in JSON writer
            let mut buf = Vec::new();
            let mut writer = datafusion::arrow::json::ArrayWriter::new(&mut buf);
            writer
                .write(&batch)
                .map_err(|e| Status::internal(format!("Failed to serialize results: {}", e)))?;
            writer
                .finish()
                .map_err(|e| Status::internal(format!("Failed to finish serialization: {}", e)))?;

            // Parse the JSON array into individual row objects
            let json_array: Vec<serde_json::Value> = serde_json::from_slice(&buf)
                .map_err(|e| Status::internal(format!("Failed to parse JSON: {}", e)))?;

            for row_value in json_array {
                let row_bytes = serde_json::to_vec(&row_value)
                    .map_err(|e| Status::internal(format!("Failed to serialize row: {}", e)))?;
                rows.push(JsonValueBytes { data: row_bytes });
            }
        }

        let row_count = rows.len() as i32;

        Ok(Response::new(QueryResponse {
            columns,
            rows,
            row_count,
        }))
    }
}

/// Run the gRPC server and a periodic reaper task together until shutdown.
pub async fn run_grpc_with_reaper(
    listener: TcpListener,
    factory: Arc<ShardFactory>,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Load app config to pass tenancy flag; use defaults when not provided here
    let cfg = crate::settings::AppConfig::load(None)
        .map_err(Box::<dyn std::error::Error + Send + Sync>::from)?;
    let svc = SiloService::new(factory.clone(), cfg);
    let server = SiloServer::new(svc);

    // Periodic reaper that iterates all shards every second
    let (tick_tx, mut tick_rx) = broadcast::channel::<()>(1);
    let reaper_factory = factory.clone();
    let reaper: JoinHandle<()> = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Iterate shards and reap
                    for shard in reaper_factory.instances().values() {
                        let _ = shard.reap_expired_leases().await;
                    }
                }
                _ = tick_rx.recv() => {
                    break;
                }
            }
        }
    });

    let local_addr = listener.local_addr()?;
    // Log after successful bind (listener provided by main)
    tracing::info!(addr = %local_addr, "server started and listening");

    let incoming = TcpListenerStream::new(listener);

    // Serve with graceful shutdown
    let serve = tonic::transport::Server::builder()
        .add_service(server)
        .serve_with_incoming_shutdown(incoming, async move {
            let _ = shutdown.recv().await;
            info!("graceful shutdown signal received");
            let _ = tick_tx.send(());
        });

    serve.await?;
    info!("all connections drained, shutting down services");
    // After server has stopped accepting connections, close all shards
    match factory.close_all().await {
        Ok(()) => info!("closed all shards"),
        Err(CloseAllError { errors }) => {
            for (name, err) in errors {
                tracing::error!(shard = %name, error = %err, "failed to close shard");
            }
        }
    }
    reaper.await.ok();
    Ok(())
}

/// Generic variant that accepts any incoming stream of IOs (e.g., Turmoil TcpListener) and runs the server
/// with a periodic reaper. Use this in simulations to inject custom accept loops.
pub async fn run_grpc_with_reaper_incoming<S, IO>(
    incoming: S,
    factory: Arc<ShardFactory>,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: tokio_stream::Stream<Item = Result<IO, std::io::Error>> + Send + 'static,
    IO: tokio::io::AsyncRead
        + tokio::io::AsyncWrite
        + Unpin
        + Send
        + 'static
        + tonic::transport::server::Connected,
{
    let cfg = crate::settings::AppConfig::load(None)
        .map_err(Box::<dyn std::error::Error + Send + Sync>::from)?;
    let svc = SiloService::new(factory.clone(), cfg);
    let server = SiloServer::new(svc);

    // Periodic reaper that iterates all shards every second
    let (tick_tx, mut tick_rx) = broadcast::channel::<()>(1);
    let reaper_factory = factory.clone();
    let reaper: JoinHandle<()> = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    for shard in reaper_factory.instances().values() {
                        let _ = shard.reap_expired_leases().await;
                    }
                }
                _ = tick_rx.recv() => { break; }
            }
        }
    });

    let serve = tonic::transport::Server::builder()
        .add_service(server)
        .serve_with_incoming_shutdown(incoming, async move {
            let _ = shutdown.recv().await;
            info!("graceful shutdown signal received");
            let _ = tick_tx.send(());
        });

    serve.await?;
    info!("all connections drained, shutting down services");
    match factory.close_all().await {
        Ok(()) => info!("closed all shards"),
        Err(CloseAllError { errors }) => {
            for (name, err) in errors {
                tracing::error!(shard = %name, error = %err, "failed to close shard");
            }
        }
    }
    reaper.await.ok();
    Ok(())
}
