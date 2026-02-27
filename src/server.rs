use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use rand::seq::SliceRandom;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use tonic_health::server::health_reporter;
use tonic_reflection::server::Builder as ReflectionBuilder;
use tracing::info;

use crate::arrow_ipc::batch_to_ipc;
use datafusion::common::{ParamValues, ScalarValue};

/// File descriptor set for gRPC reflection
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("silo_descriptor");

use crate::coordination::{Coordinator, ShardSplitter};
use crate::factory::ShardFactory;
use crate::job::{GubernatorAlgorithm, GubernatorRateLimit, JobStatusKind, RateLimitRetryPolicy};
use crate::job_attempt::{AttemptOutcome, AttemptStatus as JobAttemptStatus};
use crate::job_store_shard::{JobStoreShard, JobStoreShardError};
use crate::metrics::Metrics;
use crate::pb::silo_server::{Silo, SiloServer};
use crate::pb::*;
use crate::settings::AppConfig;
use crate::task::DEFAULT_LEASE_MS;

/// Convert a job::JobStatusKind to a proto JobStatus enum value
fn job_status_kind_to_proto(kind: JobStatusKind) -> JobStatus {
    match kind {
        JobStatusKind::Scheduled => JobStatus::Scheduled,
        JobStatusKind::Running => JobStatus::Running,
        JobStatusKind::Succeeded => JobStatus::Succeeded,
        JobStatusKind::Failed => JobStatus::Failed,
        JobStatusKind::Cancelled => JobStatus::Cancelled,
    }
}

/// gRPC metadata key for shard owner address on redirect
pub const SHARD_OWNER_ADDR_METADATA_KEY: &str = "x-silo-shard-owner-addr";
/// gRPC metadata key for shard owner node ID on redirect  
pub const SHARD_OWNER_NODE_METADATA_KEY: &str = "x-silo-shard-owner-node";

/// Convert a proto Limit to a job::Limit
fn proto_limit_to_job_limit(proto: Limit) -> Option<crate::job::Limit> {
    match proto.limit? {
        limit::Limit::Concurrency(c) => Some(crate::job::Limit::Concurrency(
            crate::job::ConcurrencyLimit {
                key: c.key,
                max_concurrency: c.max_concurrency,
            },
        )),
        limit::Limit::RateLimit(r) => {
            let algorithm = match r.algorithm {
                0 => GubernatorAlgorithm::TokenBucket,
                1 => GubernatorAlgorithm::LeakyBucket,
                _ => GubernatorAlgorithm::TokenBucket,
            };
            let retry_policy = r
                .retry_policy
                .map(|rp| RateLimitRetryPolicy {
                    initial_backoff_ms: rp.initial_backoff_ms,
                    max_backoff_ms: rp.max_backoff_ms,
                    backoff_multiplier: rp.backoff_multiplier,
                    max_retries: rp.max_retries,
                })
                .unwrap_or_default();

            Some(crate::job::Limit::RateLimit(GubernatorRateLimit {
                name: r.name,
                unique_key: r.unique_key,
                limit: r.limit,
                duration_ms: r.duration_ms,
                hits: r.hits,
                algorithm,
                behavior: r.behavior,
                retry_policy,
            }))
        }
        limit::Limit::FloatingConcurrency(f) => Some(crate::job::Limit::FloatingConcurrency(
            crate::job::FloatingConcurrencyLimit {
                key: f.key,
                default_max_concurrency: f.default_max_concurrency,
                refresh_interval_ms: f.refresh_interval_ms,
                metadata: f.metadata.into_iter().collect(),
            },
        )),
    }
}

/// Convert a job::Limit to a proto Limit
pub fn job_limit_to_proto_limit(job_limit: crate::job::Limit) -> Limit {
    match job_limit {
        crate::job::Limit::Concurrency(c) => Limit {
            limit: Some(limit::Limit::Concurrency(ConcurrencyLimit {
                key: c.key,
                max_concurrency: c.max_concurrency,
            })),
        },
        crate::job::Limit::RateLimit(r) => Limit {
            limit: Some(limit::Limit::RateLimit(crate::pb::GubernatorRateLimit {
                name: r.name,
                unique_key: r.unique_key,
                limit: r.limit,
                duration_ms: r.duration_ms,
                hits: r.hits,
                algorithm: r.algorithm.as_u8() as i32,
                behavior: r.behavior,
                retry_policy: Some(crate::pb::RateLimitRetryPolicy {
                    initial_backoff_ms: r.retry_policy.initial_backoff_ms,
                    max_backoff_ms: r.retry_policy.max_backoff_ms,
                    backoff_multiplier: r.retry_policy.backoff_multiplier,
                    max_retries: r.retry_policy.max_retries,
                }),
            })),
        },
        crate::job::Limit::FloatingConcurrency(f) => Limit {
            limit: Some(limit::Limit::FloatingConcurrency(
                crate::pb::FloatingConcurrencyLimit {
                    key: f.key,
                    default_max_concurrency: f.default_max_concurrency,
                    refresh_interval_ms: f.refresh_interval_ms,
                    metadata: f.metadata.into_iter().collect(),
                },
            )),
        },
    }
}

/// Convert a JobAttemptView to a proto JobAttempt
pub fn job_attempt_view_to_proto(
    attempt: &crate::job_attempt::JobAttemptView,
) -> crate::pb::JobAttempt {
    let state = attempt.state();
    let (status, finished_at_ms, result, error_code, error_data) = match state {
        JobAttemptStatus::Running => (AttemptStatus::Running, None, None, None, None),
        JobAttemptStatus::Succeeded {
            finished_at_ms,
            result,
        } => (
            AttemptStatus::Succeeded,
            Some(finished_at_ms),
            Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(result)),
            }),
            None,
            None,
        ),
        JobAttemptStatus::Failed {
            finished_at_ms,
            error_code,
            error,
        } => (
            AttemptStatus::Failed,
            Some(finished_at_ms),
            None,
            Some(error_code),
            Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(error)),
            }),
        ),
        JobAttemptStatus::Cancelled { finished_at_ms } => (
            AttemptStatus::Cancelled,
            Some(finished_at_ms),
            None,
            None,
            None,
        ),
    };

    crate::pb::JobAttempt {
        job_id: attempt.job_id().to_string(),
        attempt_number: attempt.attempt_number(),
        task_id: attempt.task_id().to_string(),
        status: status.into(),
        started_at_ms: attempt.started_at_ms(),
        finished_at_ms,
        result,
        error_code,
        error_data,
    }
}

/// Extract the result from a list of proto JobAttempts.
/// Returns the result from the last succeeded attempt, if any.
pub fn result_from_proto_attempts(attempts: &[crate::pb::JobAttempt]) -> Option<SerializedBytes> {
    attempts
        .iter()
        .rev()
        .find(|a| a.status == i32::from(AttemptStatus::Succeeded))
        .and_then(|a| a.result.clone())
}

/// Convert a `LeasedTask` into a proto `Task` message.
fn leased_task_to_proto(lt: &crate::task::LeasedTask, shard_id: &str) -> Task {
    let job = lt.job();
    let attempt = lt.attempt();
    let task_group = job.task_group().to_string();
    let attempt_number = attempt.attempt_number();
    let relative_attempt_number = attempt.relative_attempt_number();
    let tenant_str = lt.tenant_id().to_string();

    // Determine if this is the last attempt based on retry policy
    let max_attempts = job.retry_policy().map(|p| p.retry_count + 1).unwrap_or(1);
    let is_last_attempt = relative_attempt_number >= max_attempts;

    let tenant_id = if tenant_str == "-" {
        None
    } else {
        Some(tenant_str)
    };

    Task {
        id: attempt.task_id().to_string(),
        job_id: job.id().to_string(),
        attempt_number,
        lease_ms: DEFAULT_LEASE_MS,
        payload: Some(SerializedBytes {
            encoding: Some(serialized_bytes::Encoding::Msgpack(
                job.payload_bytes().to_vec(),
            )),
        }),
        priority: job.priority() as u32,
        shard: shard_id.to_string(),
        task_group,
        tenant_id,
        is_last_attempt,
        metadata: job.metadata().into_iter().collect(),
        limits: job
            .limits()
            .into_iter()
            .map(job_limit_to_proto_limit)
            .collect(),
        relative_attempt_number,
    }
}

fn map_err(e: JobStoreShardError) -> Status {
    match e {
        JobStoreShardError::JobNotFound(_) => Status::not_found("job not found"),
        JobStoreShardError::JobAlreadyCancelled(_) => {
            Status::failed_precondition("job is already cancelled")
        }
        JobStoreShardError::JobAlreadyTerminal(_, _) => {
            Status::failed_precondition("job is already in terminal state")
        }
        JobStoreShardError::JobNotRestartable(ref e) => Status::failed_precondition(e.to_string()),
        JobStoreShardError::JobNotExpediteable(ref e) => Status::failed_precondition(e.to_string()),
        JobStoreShardError::JobNotLeaseable(ref e) => Status::failed_precondition(e.to_string()),
        JobStoreShardError::InvalidArgument(ref msg) => Status::invalid_argument(msg.clone()),
        other => Status::internal(other.to_string()),
    }
}

/// Extract msgpack payload bytes from a proto SerializedBytes field.
fn extract_payload_bytes(payload: &Option<SerializedBytes>) -> Vec<u8> {
    payload
        .as_ref()
        .and_then(|p| {
            p.encoding
                .as_ref()
                .map(|serialized_bytes::Encoding::Msgpack(data)| data.clone())
        })
        .unwrap_or_default()
}

/// Convert a proto RetryPolicy to a domain RetryPolicy.
fn proto_retry_to_domain(rp: crate::pb::RetryPolicy) -> crate::retry::RetryPolicy {
    crate::retry::RetryPolicy {
        retry_count: rp.retry_count,
        initial_interval_ms: rp.initial_interval_ms,
        max_interval_ms: rp.max_interval_ms,
        randomize_interval: rp.randomize_interval,
        backoff_factor: rp.backoff_factor,
    }
}

/// Convert a proto metadata map to an optional Vec of (key, value) pairs.
/// Returns None if the map is empty.
fn proto_metadata_to_domain(metadata: HashMap<String, String>) -> Option<Vec<(String, String)>> {
    if metadata.is_empty() {
        None
    } else {
        Some(metadata.into_iter().collect())
    }
}

/// gRPC service implementation backed by a `ShardFactory`.
#[derive(Clone)]
pub struct SiloService {
    factory: Arc<ShardFactory>,
    coordinator: Arc<dyn Coordinator>,
    cfg: AppConfig,
    metrics: Option<Metrics>,
}

impl SiloService {
    pub fn new(
        factory: Arc<ShardFactory>,
        coordinator: Arc<dyn Coordinator>,
        cfg: AppConfig,
        metrics: Option<Metrics>,
    ) -> Self {
        Self {
            factory,
            coordinator,
            cfg,
            metrics,
        }
    }

    /// Parse a shard ID string into a ShardId.
    #[allow(clippy::result_large_err)] // Status is required by tonic's API
    fn parse_shard_id(shard_str: &str) -> Result<crate::shard_range::ShardId, Status> {
        crate::shard_range::ShardId::parse(shard_str)
            .map_err(|_| Status::invalid_argument(format!("invalid shard ID: {}", shard_str)))
    }

    #[allow(clippy::result_large_err)] // Status is required by tonic's API
    fn query_parameter_to_scalar(parameter: QueryParameter) -> Result<ScalarValue, Status> {
        match parameter.value {
            Some(query_parameter::Value::BoolValue(v)) => Ok(ScalarValue::Boolean(Some(v))),
            Some(query_parameter::Value::Int64Value(v)) => Ok(ScalarValue::Int64(Some(v))),
            Some(query_parameter::Value::Uint64Value(v)) => Ok(ScalarValue::UInt64(Some(v))),
            Some(query_parameter::Value::Float64Value(v)) => Ok(ScalarValue::Float64(Some(v))),
            Some(query_parameter::Value::StringValue(v)) => Ok(ScalarValue::Utf8(Some(v))),
            Some(query_parameter::Value::BytesValue(v)) => Ok(ScalarValue::Binary(Some(v))),
            Some(query_parameter::Value::NullValue(_)) => Ok(ScalarValue::Null),
            None => Err(Status::invalid_argument(
                "query parameter is missing a value",
            )),
        }
    }

    #[allow(clippy::result_large_err)] // Status is required by tonic's API
    fn query_parameters_to_param_values(
        parameters: Vec<QueryParameter>,
    ) -> Result<Option<ParamValues>, Status> {
        if parameters.is_empty() {
            return Ok(None);
        }

        let positional = parameters
            .into_iter()
            .map(Self::query_parameter_to_scalar)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Some(ParamValues::List(positional)))
    }

    /// Execute a SQL query against a shard and return the resulting record batches.
    async fn execute_shard_query(
        &self,
        shard_str: &str,
        sql: &str,
        parameters: Vec<QueryParameter>,
    ) -> Result<
        (
            datafusion::arrow::datatypes::SchemaRef,
            Vec<datafusion::arrow::array::RecordBatch>,
        ),
        Status,
    > {
        let shard_id = Self::parse_shard_id(shard_str)?;
        let shard = self.shard_with_redirect(&shard_id).await?;
        let query_engine = shard.query_engine();

        let mut dataframe = query_engine
            .sql(sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("SQL error: {}", e)))?;

        if let Some(param_values) = Self::query_parameters_to_param_values(parameters)? {
            dataframe = dataframe
                .with_param_values(param_values)
                .map_err(|e| Status::invalid_argument(format!("SQL parameter error: {}", e)))?;
        }

        let schema = dataframe.schema().inner().clone();

        let stream = dataframe
            .execute_stream()
            .await
            .map_err(|e| Status::internal(format!("Query execution failed: {}", e)))?;

        // DataFusion documents that dropping the stream aborts query execution.
        // Wrapping stream collection in a timeout ensures timed-out queries stop
        // running instead of continuing in the background.
        let collect_future = datafusion::physical_plan::common::collect(stream);
        let batches = if let Some(statement_timeout) = self.cfg.server.statement_timeout() {
            match tokio::time::timeout(statement_timeout, collect_future).await {
                Ok(Ok(batches)) => batches,
                Ok(Err(e)) => {
                    return Err(Status::internal(format!("Query execution failed: {}", e)));
                }
                Err(_) => {
                    return Err(Status::deadline_exceeded(format!(
                        "Query exceeded statement timeout of {} ms",
                        statement_timeout.as_millis()
                    )));
                }
            }
        } else {
            collect_future
                .await
                .map_err(|e| Status::internal(format!("Query execution failed: {}", e)))?
        };

        Ok((schema, batches))
    }

    /// Get shard with async lookup of owner for redirect metadata.
    /// If the shard is not found locally, returns NOT_FOUND with metadata
    /// indicating which server owns the shard (if known).
    ///
    /// If this node is the computed owner but hasn't finished acquiring the shard,
    /// returns UNAVAILABLE to signal the client should retry after a delay.
    ///
    /// If the shard has an active split in a traffic-pausing
    /// phase, returns UNAVAILABLE to signal the client should retry after the split
    /// completes.
    async fn shard_with_redirect(
        &self,
        shard_id: &crate::shard_range::ShardId,
    ) -> Result<Arc<JobStoreShard>, Status> {
        if let Some(shard) = self.factory.get(shard_id) {
            // Check if this shard is paused for split
            if self.coordinator.is_shard_paused(*shard_id).await {
                tracing::debug!(
                    shard_id = %shard_id,
                    "shard is paused for split, returning UNAVAILABLE"
                );
                return Err(Status::unavailable(
                    "shard is temporarily unavailable: split in progress",
                ));
            }
            return Ok(shard);
        }

        // Shard not found locally - determine if we should redirect or signal unavailable
        let mut status = Status::not_found("shard not found");
        let coord = &self.coordinator;

        // Get the owner map to find where this shard lives
        let owner_map = match coord.get_shard_owner_map().await {
            Ok(map) => map,
            Err(e) => {
                tracing::error!(error = %e, "failed to get shard owner map for redirect");
                return Err(status);
            }
        };

        // Check if WE are the computed owner but haven't opened the shard yet.
        // This happens during shard acquisition - the membership list says we own
        // the shard, but we haven't finished acquiring the K8s lease and opening it.
        // In this case, return UNAVAILABLE so clients know to retry with backoff,
        // rather than NOT_FOUND with a redirect to ourselves (which causes loops).
        let this_node_id = coord.node_id();
        let owned_shards = coord.owned_shards().await;
        let members = coord.get_members().await.ok();

        if let Some(computed_owner_node) = owner_map.shard_to_node.get(shard_id)
            && computed_owner_node == this_node_id
        {
            // We're computed to own this shard but don't have it locally
            tracing::warn!(
                shard_id = %shard_id,
                node_id = %this_node_id,
                owned_shards = ?owned_shards,
                members = ?members.as_ref().map(|m| m.iter().map(|mi| &mi.node_id).collect::<Vec<_>>()),
                "shard not ready: this node is computed owner but shard not yet acquired"
            );
            return Err(Status::unavailable(
                "shard not ready: acquisition in progress",
            ));
        }

        // Log details about the routing mismatch to help diagnose production issues
        tracing::warn!(
            shard_id = %shard_id,
            this_node_id = %this_node_id,
            owned_shards = ?owned_shards,
            computed_owner = ?owner_map.shard_to_node.get(shard_id),
            computed_addr = ?owner_map.shard_to_addr.get(shard_id),
            members = ?members.as_ref().map(|m| m.iter().map(|mi| (&mi.node_id, &mi.grpc_addr)).collect::<Vec<_>>()),
            "shard not found: routing mismatch - another node sent us a request for a shard we don't own"
        );

        // Add redirect metadata - point to the actual computed owner
        let metadata = status.metadata_mut();
        if let Some(addr) = owner_map.shard_to_addr.get(shard_id) {
            if let Ok(val) = addr.parse() {
                metadata.insert(SHARD_OWNER_ADDR_METADATA_KEY, val);
            }
        } else {
            tracing::warn!(
                shard_id = %shard_id,
                num_shards = owner_map.num_shards(),
                "shard ID not found in owner map"
            );
        }

        if let Some(node) = owner_map.shard_to_node.get(shard_id)
            && let Ok(val) = node.parse()
        {
            metadata.insert(SHARD_OWNER_NODE_METADATA_KEY, val);
        }

        Err(status)
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
            (false, Some(_)) => Err(Status::failed_precondition(
                "tenant must not be provided when tenancy is disabled on the server",
            )),
            (false, None) => Ok("-".to_string()),
        }
    }

    /// Validate and convert a single import job request from proto to domain types.
    /// Returns the validated shard ID, tenant, and import params.
    async fn validate_import_job(
        &self,
        job_req: ImportJobRequest,
    ) -> Result<
        (
            crate::shard_range::ShardId,
            String,
            crate::job_store_shard::import::ImportJobParams,
        ),
        Status,
    > {
        let shard_id = Self::parse_shard_id(&job_req.shard)?;
        let tenant = self.validate_tenant(job_req.tenant.as_deref())?;
        self.validate_tenant_in_shard_range(&shard_id, &tenant)
            .await?;

        if job_req.id.is_empty() {
            return Err(Status::invalid_argument("job id is required for import"));
        }

        if job_req.task_group.is_empty() {
            return Err(Status::invalid_argument("task_group is required"));
        }

        if job_req.priority > 99 {
            return Err(Status::invalid_argument(
                "priority must be between 0 and 99",
            ));
        }

        Self::validate_metadata(&job_req.metadata)?;

        let payload_bytes = extract_payload_bytes(&job_req.payload);
        let retry = job_req.retry_policy.map(proto_retry_to_domain);
        let limits: Vec<crate::job::Limit> = job_req
            .limits
            .into_iter()
            .filter_map(proto_limit_to_job_limit)
            .collect();
        let metadata = proto_metadata_to_domain(job_req.metadata);

        // Convert proto attempts to domain attempts
        let mut attempts = Vec::with_capacity(job_req.attempts.len());
        for proto_attempt in job_req.attempts {
            let status = match AttemptStatus::try_from(proto_attempt.status) {
                Ok(AttemptStatus::Succeeded) => {
                    let result = proto_attempt
                        .result
                        .and_then(|s| {
                            s.encoding
                                .map(|serialized_bytes::Encoding::Msgpack(data)| data)
                        })
                        .unwrap_or_default();
                    crate::job_store_shard::import::ImportedAttemptStatus::Succeeded { result }
                }
                Ok(AttemptStatus::Failed) => {
                    let error_code = proto_attempt.error_code.unwrap_or_default();
                    let error = proto_attempt
                        .error_data
                        .and_then(|s| {
                            s.encoding
                                .map(|serialized_bytes::Encoding::Msgpack(data)| data)
                        })
                        .unwrap_or_default();
                    crate::job_store_shard::import::ImportedAttemptStatus::Failed {
                        error_code,
                        error,
                    }
                }
                Ok(AttemptStatus::Cancelled) => {
                    crate::job_store_shard::import::ImportedAttemptStatus::Cancelled
                }
                Ok(AttemptStatus::Running) => {
                    return Err(Status::invalid_argument(
                        "import attempts must be terminal (not Running)",
                    ));
                }
                Err(_) => {
                    return Err(Status::invalid_argument("invalid attempt status"));
                }
            };
            attempts.push(crate::job_store_shard::import::ImportedAttempt {
                status,
                started_at_ms: proto_attempt.started_at_ms,
                finished_at_ms: proto_attempt.finished_at_ms,
            });
        }

        let params = crate::job_store_shard::import::ImportJobParams {
            id: job_req.id.clone(),
            priority: job_req.priority as u8,
            enqueue_time_ms: job_req.enqueue_time_ms,
            start_at_ms: job_req.start_at_ms,
            retry_policy: retry,
            payload: payload_bytes,
            limits,
            metadata,
            task_group: job_req.task_group,
            attempts,
        };

        Ok((shard_id, tenant, params))
    }

    /// Parse shard ID, look up the local shard (with redirect metadata on miss), validate tenant, and confirm the tenant falls within the shard's range.
    async fn resolve_shard_and_tenant(
        &self,
        shard: &str,
        tenant: Option<&str>,
    ) -> Result<(Arc<JobStoreShard>, String), Status> {
        let shard_id = Self::parse_shard_id(shard)?;
        let shard = self.shard_with_redirect(&shard_id).await?;
        let tenant = self.validate_tenant(tenant)?;
        self.validate_tenant_in_shard_range(&shard_id, &tenant)
            .await?;
        Ok((shard, tenant))
    }

    /// Validate metadata constraints: at most 16 entries, keys < 64 chars,
    /// values < u16::MAX bytes.
    #[allow(clippy::result_large_err)]
    fn validate_metadata(
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<(), Status> {
        if metadata.len() > 16 {
            return Err(Status::invalid_argument(
                "metadata has too many entries (max 16)",
            ));
        }
        for (k, v) in metadata {
            if k.chars().count() >= 64 {
                return Err(Status::invalid_argument(
                    "metadata key too long (must be < 64 chars)",
                ));
            }
            if v.len() >= u16::MAX as usize {
                return Err(Status::invalid_argument(
                    "metadata value too long (must be < u16::MAX bytes)",
                ));
            }
        }
        Ok(())
    }

    /// Validate that the tenant_id falls within the shard's tenant range.
    ///
    /// This prevents clients from accidentally sending requests to the wrong shard,
    /// which could happen if topology information is stale or the client has a bug.
    /// Returns an error if the tenant_id is outside the shard's range.
    async fn validate_tenant_in_shard_range(
        &self,
        shard_id: &crate::shard_range::ShardId,
        tenant_id: &str,
    ) -> Result<(), Status> {
        // Skip validation when tenancy is disabled (default tenant "-")
        // The synthetic tenant doesn't represent real routing requirements
        if !self.cfg.tenancy.enabled {
            return Ok(());
        }

        // Look up the shard's range from the coordinator's shard map
        let shard_map = self.coordinator.get_shard_map().await.map_err(|e| {
            tracing::error!(error = %e, "failed to get shard map for tenant validation");
            Status::internal("failed to validate tenant routing")
        })?;

        // Find the shard in the map
        let Some(shard_info) = shard_map.shards().iter().find(|s| &s.id == shard_id) else {
            // Shard not in map - this shouldn't happen if shard_with_redirect succeeded
            tracing::warn!(
                shard_id = %shard_id,
                "shard not found in shard map during tenant validation"
            );
            return Ok(()); // Allow the request to proceed
        };

        // Validate tenant_id is within the shard's range
        if !shard_info.range.contains(tenant_id) {
            tracing::warn!(
                shard_id = %shard_id,
                tenant_id = %tenant_id,
                shard_range = %shard_info.range,
                "tenant_id is outside shard's range - client may have stale topology"
            );
            return Err(Status::failed_precondition(format!(
                "tenant '{}' is not within shard {} range {}; refresh topology and retry",
                tenant_id, shard_id, shard_info.range
            )));
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl Silo for SiloService {
    async fn get_cluster_info(
        &self,
        _req: Request<GetClusterInfoRequest>,
    ) -> Result<Response<GetClusterInfoResponse>, Status> {
        let coord = &self.coordinator;

        let owner_map = coord
            .get_shard_owner_map()
            .await
            .map_err(|e| Status::internal(format!("failed to get shard owner map: {}", e)))?;

        let shard_owners: Vec<ShardOwner> = owner_map
            .shard_map
            .shards()
            .iter()
            .map(|shard_info| {
                let grpc_addr = owner_map
                    .shard_to_addr
                    .get(&shard_info.id)
                    .cloned()
                    .unwrap_or_default();
                let node_id = owner_map
                    .shard_to_node
                    .get(&shard_info.id)
                    .cloned()
                    .unwrap_or_default();
                ShardOwner {
                    shard_id: shard_info.id.to_string(),
                    grpc_addr,
                    node_id,
                    range_start: shard_info.range.start.clone(),
                    range_end: shard_info.range.end.clone(),
                    placement_ring: shard_info.placement_ring.clone(),
                }
            })
            .collect();

        // Get all cluster members with their ring participation
        let member_infos = coord
            .get_members()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let members: Vec<ClusterMember> = member_infos
            .iter()
            .map(|m| ClusterMember {
                node_id: m.node_id.clone(),
                grpc_addr: m.grpc_addr.clone(),
                placement_rings: m.placement_rings.clone(),
            })
            .collect();

        Ok(Response::new(GetClusterInfoResponse {
            num_shards: owner_map.num_shards() as u32,
            shard_owners,
            this_node_id: coord.node_id().to_string(),
            this_grpc_addr: coord.grpc_addr().to_string(),
            members,
        }))
    }

    async fn enqueue(
        &self,
        req: Request<EnqueueRequest>,
    ) -> Result<Response<EnqueueResponse>, Status> {
        let r = req.into_inner();
        let (shard, tenant) = self
            .resolve_shard_and_tenant(&r.shard, r.tenant.as_deref())
            .await?;

        if r.priority > 99 {
            return Err(Status::invalid_argument(
                "priority must be between 0 and 99",
            ));
        }

        let payload_bytes = extract_payload_bytes(&r.payload);
        let retry = r.retry_policy.map(proto_retry_to_domain);
        let limits: Vec<crate::job::Limit> = r
            .limits
            .into_iter()
            .filter_map(proto_limit_to_job_limit)
            .collect();

        Self::validate_metadata(&r.metadata)?;
        let metadata = proto_metadata_to_domain(r.metadata);

        // Validate task_group - required and must be <= 64 chars
        if r.task_group.is_empty() {
            return Err(Status::invalid_argument("task_group is required"));
        }
        if r.task_group.chars().count() > 64 {
            return Err(Status::invalid_argument(
                "task_group too long (must be <= 64 chars)",
            ));
        }

        let shard_str = r.shard.to_string();
        let id = shard
            .enqueue(
                &tenant,
                if r.id.is_empty() { None } else { Some(r.id) },
                r.priority as u8,
                r.start_at_ms,
                retry,
                payload_bytes,
                limits,
                metadata,
                &r.task_group,
            )
            .await
            .map_err(map_err)?;

        // Record metrics
        if let Some(ref m) = self.metrics {
            m.record_enqueue(&shard_str, &tenant);
        }

        Ok(Response::new(EnqueueResponse { id }))
    }

    async fn get_job(
        &self,
        req: Request<GetJobRequest>,
    ) -> Result<Response<GetJobResponse>, Status> {
        let r = req.into_inner();
        let (shard, tenant) = self
            .resolve_shard_and_tenant(&r.shard, r.tenant.as_deref())
            .await?;
        let Some(view) = shard.get_job(&tenant, &r.id).await.map_err(map_err)? else {
            return Err(Status::not_found("job not found"));
        };

        // Get job status - should always exist if job exists
        let Some(job_status) = shard
            .get_job_status(&tenant, &r.id)
            .await
            .map_err(map_err)?
        else {
            return Err(Status::internal("job exists but has no status"));
        };
        let status = job_status_kind_to_proto(job_status.kind);
        let status_changed_at_ms = job_status.changed_at_ms;

        let retry = view.retry_policy();
        let retry_policy = retry.map(|p| RetryPolicy {
            retry_count: p.retry_count,
            initial_interval_ms: p.initial_interval_ms,
            max_interval_ms: p.max_interval_ms,
            randomize_interval: p.randomize_interval,
            backoff_factor: p.backoff_factor,
        });

        // Optionally fetch attempts if requested
        let attempts = if r.include_attempts {
            let attempt_views = shard
                .get_job_attempts(&tenant, &r.id)
                .await
                .map_err(map_err)?;
            attempt_views
                .into_iter()
                .map(|a| job_attempt_view_to_proto(&a))
                .collect()
        } else {
            Vec::new()
        };

        // For succeeded jobs, populate the result field
        let result = if job_status.kind == crate::job::JobStatusKind::Succeeded {
            if !attempts.is_empty() {
                // We already have attempts loaded, extract result from them
                result_from_proto_attempts(&attempts)
            } else {
                // Attempts not loaded, query the latest attempt
                let latest = shard
                    .get_latest_job_attempt(&tenant, &r.id)
                    .await
                    .map_err(map_err)?;
                latest.and_then(|a| {
                    let proto = job_attempt_view_to_proto(&a);
                    proto.result
                })
            }
        } else {
            None
        };

        let resp = GetJobResponse {
            id: view.id().to_string(),
            priority: view.priority() as u32,
            enqueue_time_ms: view.enqueue_time_ms(),
            payload: Some(SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(
                    view.payload_bytes().to_vec(),
                )),
            }),
            retry_policy,
            limits: view
                .limits()
                .into_iter()
                .map(job_limit_to_proto_limit)
                .collect(),
            metadata: view.metadata().into_iter().collect(),
            status: status.into(),
            status_changed_at_ms,
            attempts,
            next_attempt_starts_after_ms: job_status.next_attempt_starts_after_ms,
            task_group: view.task_group().to_string(),
            result,
        };
        Ok(Response::new(resp))
    }

    async fn get_job_result(
        &self,
        req: Request<GetJobResultRequest>,
    ) -> Result<Response<GetJobResultResponse>, Status> {
        let r = req.into_inner();
        let (shard, tenant) = self
            .resolve_shard_and_tenant(&r.shard, r.tenant.as_deref())
            .await?;

        // First check if job exists
        let Some(_job_view) = shard.get_job(&tenant, &r.id).await.map_err(map_err)? else {
            return Err(Status::not_found("job not found"));
        };

        // Get job status - must be in a terminal state
        let Some(job_status) = shard
            .get_job_status(&tenant, &r.id)
            .await
            .map_err(map_err)?
        else {
            return Err(Status::internal("job exists but has no status"));
        };

        // Check if job is in a terminal state
        if !job_status.is_terminal() {
            return Err(Status::failed_precondition(format!(
                "job is not complete: status is {:?}",
                job_status.kind
            )));
        }

        // Get the latest attempt to retrieve the result
        let Some(attempt) = shard
            .get_latest_job_attempt(&tenant, &r.id)
            .await
            .map_err(map_err)?
        else {
            // Job is terminal but has no attempts - shouldn't happen normally
            // but could happen for cancelled jobs that were never started
            return Ok(Response::new(GetJobResultResponse {
                id: r.id,
                status: job_status_kind_to_proto(job_status.kind).into(),
                finished_at_ms: job_status.changed_at_ms,
                result: Some(get_job_result_response::Result::Cancelled(JobCancelled {
                    cancelled_at_ms: job_status.changed_at_ms,
                })),
            }));
        };

        // Extract result based on attempt status
        match attempt.state() {
            crate::job_attempt::AttemptStatus::Succeeded {
                finished_at_ms,
                result,
            } => Ok(Response::new(GetJobResultResponse {
                id: r.id,
                status: JobStatus::Succeeded.into(),
                finished_at_ms,
                result: Some(get_job_result_response::Result::SuccessData(
                    SerializedBytes {
                        encoding: Some(serialized_bytes::Encoding::Msgpack(result)),
                    },
                )),
            })),
            crate::job_attempt::AttemptStatus::Failed {
                finished_at_ms,
                error_code,
                error,
            } => Ok(Response::new(GetJobResultResponse {
                id: r.id,
                status: JobStatus::Failed.into(),
                finished_at_ms,
                result: Some(get_job_result_response::Result::Failure(JobFailure {
                    error_code,
                    error_data: Some(SerializedBytes {
                        encoding: Some(serialized_bytes::Encoding::Msgpack(error)),
                    }),
                })),
            })),
            crate::job_attempt::AttemptStatus::Cancelled { finished_at_ms } => {
                Ok(Response::new(GetJobResultResponse {
                    id: r.id,
                    status: JobStatus::Cancelled.into(),
                    finished_at_ms,
                    result: Some(get_job_result_response::Result::Cancelled(JobCancelled {
                        cancelled_at_ms: finished_at_ms,
                    })),
                }))
            }
            crate::job_attempt::AttemptStatus::Running => {
                // This shouldn't happen - job status is terminal but attempt is still running
                Err(Status::internal(
                    "job status is terminal but attempt is still running",
                ))
            }
        }
    }

    async fn delete_job(
        &self,
        req: Request<DeleteJobRequest>,
    ) -> Result<Response<DeleteJobResponse>, Status> {
        let r = req.into_inner();
        let (shard, tenant) = self
            .resolve_shard_and_tenant(&r.shard, r.tenant.as_deref())
            .await?;
        shard.delete_job(&tenant, &r.id).await.map_err(map_err)?;
        Ok(Response::new(DeleteJobResponse {}))
    }

    async fn cancel_job(
        &self,
        req: Request<CancelJobRequest>,
    ) -> Result<Response<CancelJobResponse>, Status> {
        let r = req.into_inner();
        let (shard, tenant) = self
            .resolve_shard_and_tenant(&r.shard, r.tenant.as_deref())
            .await?;
        shard.cancel_job(&tenant, &r.id).await.map_err(map_err)?;
        Ok(Response::new(CancelJobResponse {}))
    }

    async fn restart_job(
        &self,
        req: Request<RestartJobRequest>,
    ) -> Result<Response<RestartJobResponse>, Status> {
        let r = req.into_inner();
        let (shard, tenant) = self
            .resolve_shard_and_tenant(&r.shard, r.tenant.as_deref())
            .await?;
        shard.restart_job(&tenant, &r.id).await.map_err(map_err)?;
        Ok(Response::new(RestartJobResponse {}))
    }

    async fn expedite_job(
        &self,
        req: Request<ExpediteJobRequest>,
    ) -> Result<Response<ExpediteJobResponse>, Status> {
        let r = req.into_inner();
        let (shard, tenant) = self
            .resolve_shard_and_tenant(&r.shard, r.tenant.as_deref())
            .await?;
        shard.expedite_job(&tenant, &r.id).await.map_err(map_err)?;
        Ok(Response::new(ExpediteJobResponse {}))
    }

    async fn lease_task(
        &self,
        req: Request<LeaseTaskRequest>,
    ) -> Result<Response<LeaseTaskResponse>, Status> {
        let r = req.into_inner();
        let (shard, tenant) = self
            .resolve_shard_and_tenant(&r.shard, r.tenant.as_deref())
            .await?;

        if r.worker_id.is_empty() {
            return Err(Status::invalid_argument("worker_id is required"));
        }

        let leased = shard
            .lease_task(&tenant, &r.id, &r.worker_id)
            .await
            .map_err(map_err)?;

        let task = leased_task_to_proto(&leased, &r.shard);
        Ok(Response::new(LeaseTaskResponse { task: Some(task) }))
    }

    async fn lease_tasks(
        &self,
        req: Request<LeaseTasksRequest>,
    ) -> Result<Response<LeaseTasksResponse>, Status> {
        let r = req.into_inner();
        // LeaseTasks is tenant-agnostic - it returns tasks from all tenants on local shards
        let max_tasks = r.max_tasks as usize;

        // Validate task_group - required
        if r.task_group.is_empty() {
            return Err(Status::invalid_argument("task_group is required"));
        }

        // Determine which shards to poll:
        // - If shard filter is specified, only poll that shard
        // - Otherwise, poll all local shards (default behavior for workers)
        let shards_to_poll: Vec<(crate::shard_range::ShardId, Arc<JobStoreShard>)> =
            if let Some(shard_filter) = r.shard {
                // Filter to specific shard
                let shard_id = Self::parse_shard_id(&shard_filter)?;
                let shard = self.shard_with_redirect(&shard_id).await?;
                vec![(shard_id, shard)]
            } else {
                // Poll all local shards - this is the typical worker behavior
                // Shuffle for fair distribution across shards
                let mut shards: Vec<_> = self
                    .factory
                    .instances()
                    .iter()
                    .map(|(id, shard)| (*id, shard.clone()))
                    .collect();
                shards.shuffle(&mut rand::rng());
                shards
            };

        let mut all_tasks = Vec::new();
        let mut all_refresh_tasks = Vec::new();
        let mut remaining = max_tasks;

        // Poll each shard until we have enough tasks or exhausted all shards
        for (shard_id, shard) in shards_to_poll {
            if remaining == 0 {
                break;
            }

            let result = shard
                .dequeue(&r.worker_id, &r.task_group, remaining)
                .await
                .map_err(map_err)?;

            let tasks_added = result.tasks.len();
            let shard_str = shard_id.to_string();
            let now_ms = crate::job_store_shard::now_epoch_ms();

            for lt in result.tasks {
                // Record attempt and wait time metrics
                if let Some(ref m) = self.metrics {
                    let job = lt.job();
                    let task_group = job.task_group();
                    let relative_attempt_number = lt.attempt().relative_attempt_number();
                    m.record_attempt(&shard_str, task_group, relative_attempt_number > 1);

                    let wait_time_secs = (now_ms - job.enqueue_time_ms()).max(0) as f64 / 1000.0;
                    m.record_job_wait_time(&shard_str, task_group, wait_time_secs);
                    m.inc_task_leases_active(&shard_str, task_group);
                }

                all_tasks.push(leased_task_to_proto(&lt, &shard_str));
            }

            for rt in result.refresh_tasks {
                // Get tenant_id, using None if it's the default "-" tenant
                let tenant_id = if rt.tenant_id == "-" {
                    None
                } else {
                    Some(rt.tenant_id)
                };

                all_refresh_tasks.push(RefreshFloatingLimitTask {
                    id: rt.task_id,
                    queue_key: rt.queue_key,
                    current_max_concurrency: rt.current_max_concurrency,
                    last_refreshed_at_ms: rt.last_refreshed_at_ms,
                    metadata: rt.metadata.into_iter().collect(),
                    lease_ms: DEFAULT_LEASE_MS,
                    shard: shard_id.to_string(),
                    task_group: rt.task_group,
                    tenant_id,
                });
            }

            // Record dequeue metrics per shard
            if let Some(ref m) = self.metrics
                && tasks_added > 0
            {
                m.record_dequeue(&shard_str, &r.task_group, tasks_added as u64);
            }

            remaining = remaining.saturating_sub(tasks_added);
        }

        Ok(Response::new(LeaseTasksResponse {
            tasks: all_tasks,
            refresh_tasks: all_refresh_tasks,
        }))
    }

    async fn report_outcome(
        &self,
        req: Request<ReportOutcomeRequest>,
    ) -> Result<Response<ReportOutcomeResponse>, Status> {
        let r = req.into_inner();
        let shard_str = r.shard.to_string();
        let shard_id = Self::parse_shard_id(&r.shard)?;
        let shard = self.shard_with_redirect(&shard_id).await?;
        let (outcome, status_str) = match r
            .outcome
            .ok_or_else(|| Status::invalid_argument("missing outcome"))?
        {
            report_outcome_request::Outcome::Success(s) => {
                let result = match s.encoding {
                    Some(serialized_bytes::Encoding::Msgpack(data)) => data,
                    None => Vec::new(),
                };
                (AttemptOutcome::Success { result }, "succeeded")
            }
            report_outcome_request::Outcome::Failure(f) => {
                let error = f
                    .data
                    .and_then(|d| {
                        d.encoding
                            .map(|serialized_bytes::Encoding::Msgpack(data)| data)
                    })
                    .unwrap_or_default();
                (
                    AttemptOutcome::Error {
                        error_code: f.code,
                        error,
                    },
                    "failed",
                )
            }
            report_outcome_request::Outcome::Cancelled(_) => {
                (AttemptOutcome::Cancelled, "cancelled")
            }
        };
        shard
            .report_attempt_outcome(&r.task_id, outcome)
            .await
            .map_err(map_err)?;

        // Record completion metrics
        if let Some(ref m) = self.metrics {
            m.record_completion(&shard_str, status_str);
        }

        Ok(Response::new(ReportOutcomeResponse {}))
    }

    async fn report_refresh_outcome(
        &self,
        req: Request<ReportRefreshOutcomeRequest>,
    ) -> Result<Response<ReportRefreshOutcomeResponse>, Status> {
        let r = req.into_inner();
        let shard_id = Self::parse_shard_id(&r.shard)?;
        let shard = self.shard_with_redirect(&shard_id).await?;
        // Tenant is extracted from the lease on the server side, not from the request
        let outcome = r
            .outcome
            .ok_or_else(|| Status::invalid_argument("missing outcome"))?;

        match outcome {
            report_refresh_outcome_request::Outcome::Success(s) => {
                shard
                    .report_refresh_success(&r.task_id, s.new_max_concurrency)
                    .await
                    .map_err(map_err)?;
            }
            report_refresh_outcome_request::Outcome::Failure(f) => {
                shard
                    .report_refresh_failure(&r.task_id, &f.code, &f.message)
                    .await
                    .map_err(map_err)?;
            }
        }
        Ok(Response::new(ReportRefreshOutcomeResponse {}))
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let r = req.into_inner();
        let shard_id = Self::parse_shard_id(&r.shard)?;
        let shard = self.shard_with_redirect(&shard_id).await?;
        // Tenant is extracted from the lease on the server side, not from the request
        let result = shard
            .heartbeat_task(&r.worker_id, &r.task_id)
            .await
            .map_err(map_err)?;
        Ok(Response::new(HeartbeatResponse {
            cancelled: result.cancelled,
            cancelled_at_ms: result.cancelled_at_ms,
        }))
    }

    async fn query(&self, req: Request<QueryRequest>) -> Result<Response<QueryResponse>, Status> {
        let r = req.into_inner();
        let (schema, batches) = self
            .execute_shard_query(&r.shard, &r.sql, r.parameters)
            .await?;
        let columns: Vec<ColumnInfo> = schema
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().to_string(),
                data_type: format!("{:?}", f.data_type()),
            })
            .collect();

        // Convert batches directly to MessagePack rows
        let row_bytes =
            crate::query::record_batches_to_msgpack(&batches).map_err(Status::internal)?;
        let rows: Vec<SerializedBytes> = row_bytes
            .into_iter()
            .map(|data| SerializedBytes {
                encoding: Some(serialized_bytes::Encoding::Msgpack(data)),
            })
            .collect();

        let row_count = rows.len() as i32;

        Ok(Response::new(QueryResponse {
            columns,
            rows,
            row_count,
        }))
    }

    type QueryArrowStream =
        Pin<Box<dyn Stream<Item = Result<ArrowIpcMessage, Status>> + Send + 'static>>;

    async fn query_arrow(
        &self,
        req: Request<QueryArrowRequest>,
    ) -> Result<Response<Self::QueryArrowStream>, Status> {
        let r = req.into_inner();
        let (_schema, batches) = self
            .execute_shard_query(&r.shard, &r.sql, r.parameters)
            .await?;

        // Create a stream that yields Arrow IPC messages
        let (tx, rx) = tokio::sync::mpsc::channel(16);

        tokio::spawn(async move {
            for batch in batches {
                match batch_to_ipc(&batch) {
                    Ok(ipc_data) => {
                        if tx.send(Ok(ArrowIpcMessage { ipc_data })).await.is_err() {
                            break; // Client disconnected
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!(
                                "Failed to serialize batch: {}",
                                e
                            ))))
                            .await;
                        break;
                    }
                }
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn import_jobs(
        &self,
        req: Request<ImportJobsRequest>,
    ) -> Result<Response<ImportJobsResponse>, Status> {
        let r = req.into_inner();

        if r.jobs.is_empty() {
            return Ok(Response::new(ImportJobsResponse {
                results: Vec::new(),
            }));
        }

        // Validate and convert all jobs, tracking original index for result ordering
        // Jobs that fail validation get immediate error results
        let mut results: Vec<Option<ImportJobResult>> = vec![None; r.jobs.len()];

        // Group validated jobs by (shard_id, tenant) for batched execution
        // Each entry stores (original_index, params)
        let mut batches: std::collections::HashMap<
            (crate::shard_range::ShardId, String),
            Vec<(usize, crate::job_store_shard::import::ImportJobParams)>,
        > = std::collections::HashMap::new();

        for (i, job_req) in r.jobs.into_iter().enumerate() {
            let job_id = job_req.id.clone();
            match self.validate_import_job(job_req).await {
                Ok((shard_id, tenant, params)) => {
                    batches
                        .entry((shard_id, tenant))
                        .or_default()
                        .push((i, params));
                }
                Err(e) => {
                    results[i] = Some(ImportJobResult {
                        id: job_id,
                        success: false,
                        error: Some(e.to_string()),
                        status: JobStatus::Failed.into(),
                    });
                }
            }
        }

        // Execute each batch against its shard
        for ((shard_id, tenant), batch) in batches {
            let shard = match self.shard_with_redirect(&shard_id).await {
                Ok(s) => s,
                Err(e) => {
                    for (i, params) in batch {
                        results[i] = Some(ImportJobResult {
                            id: params.id,
                            success: false,
                            error: Some(e.to_string()),
                            status: JobStatus::Failed.into(),
                        });
                    }
                    continue;
                }
            };

            let (indices, params): (Vec<usize>, Vec<_>) = batch.into_iter().unzip();

            match shard.import_jobs(&tenant, params).await {
                Ok(import_results) => {
                    for (idx, result) in indices.into_iter().zip(import_results) {
                        results[idx] = Some(ImportJobResult {
                            id: result.job_id,
                            success: result.success,
                            error: result.error,
                            status: job_status_kind_to_proto(result.status).into(),
                        });
                    }
                }
                Err(e) => {
                    for idx in indices {
                        results[idx] = Some(ImportJobResult {
                            id: String::new(),
                            success: false,
                            error: Some(e.to_string()),
                            status: JobStatus::Failed.into(),
                        });
                    }
                }
            }
        }

        // Unwrap all results (every slot should be filled)
        let final_results = results
            .into_iter()
            .map(|r| r.expect("all import results should be filled"))
            .collect();

        Ok(Response::new(ImportJobsResponse {
            results: final_results,
        }))
    }

    async fn reset_shards(
        &self,
        _req: Request<ResetShardsRequest>,
    ) -> Result<Response<ResetShardsResponse>, Status> {
        // Only allow in dev mode
        if !self.cfg.server.dev_mode {
            return Err(Status::permission_denied(
                "ResetShards is only available in dev mode",
            ));
        }

        // Use the coordinator's shard map as the source of truth for which shards
        // should exist, not just what's currently in the factory. This catches cases
        // where shards failed to open during startup (e.g., due to path configuration
        // errors) but are still listed in the shard map.
        let shard_map = self
            .coordinator
            .get_shard_map()
            .await
            .map_err(|e| Status::internal(format!("failed to get shard map: {}", e)))?;

        let mut reset_count = 0u32;
        for shard_info in shard_map.shards() {
            let shard_id = shard_info.id;
            let range = match self.factory.get(&shard_id) {
                Some(shard) => shard.get_range(),
                None => {
                    // Shard is in the shard map but not in the factory - this means it
                    // failed to open during startup. Try to open it now.
                    tracing::warn!(
                        shard = %shard_id,
                        "shard not found in factory during reset, attempting to open it"
                    );
                    shard_info.range.clone()
                }
            };
            match self.factory.reset(&shard_id, &range).await {
                Ok(_) => {
                    reset_count += 1;
                    tracing::debug!(shard = %shard_id, "reset shard successfully");
                }
                Err(e) => {
                    tracing::error!(shard = %shard_id, error = %e, "failed to reset shard");
                    return Err(Status::internal(format!(
                        "Failed to reset shard {}: {}",
                        shard_id, e
                    )));
                }
            }
        }

        // Re-register all reset shards as owned by the coordinator.
        // This ensures the coordinator's owned set is accurate after reset,
        // which improves diagnostic logging in shard_with_redirect().
        {
            let mut owned = self.coordinator.base().owned.lock().await;
            for shard_info in shard_map.shards() {
                owned.insert(shard_info.id);
            }
        }

        // Verify all shards are accessible after reset
        for shard_info in shard_map.shards() {
            if self.factory.get(&shard_info.id).is_none() {
                return Err(Status::internal(format!(
                    "shard {} not accessible after reset - this is a bug",
                    shard_info.id
                )));
            }
        }

        tracing::info!(shards_reset = reset_count, "reset all shards successfully");
        Ok(Response::new(ResetShardsResponse {
            shards_reset: reset_count,
        }))
    }

    async fn cpu_profile(
        &self,
        req: Request<CpuProfileRequest>,
    ) -> Result<Response<CpuProfileResponse>, Status> {
        let r = req.into_inner();

        // Validate and clamp duration (1-300 seconds, default 30)
        let duration = if r.duration_seconds == 0 {
            30
        } else {
            r.duration_seconds.clamp(1, 300)
        };

        // Validate and clamp frequency (1-1000 Hz, default 100)
        let frequency = if r.frequency == 0 {
            100
        } else {
            r.frequency.clamp(1, 1000)
        };

        tracing::info!(
            duration_seconds = duration,
            frequency_hz = frequency,
            "starting CPU profile"
        );

        // Start the profiler
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(frequency as i32)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .map_err(|e| Status::internal(format!("failed to start profiler: {}", e)))?;

        // Profile for the requested duration
        tokio::time::sleep(std::time::Duration::from_secs(duration as u64)).await;

        // Generate pprof protobuf report
        let report = guard
            .report()
            .build()
            .map_err(|e| Status::internal(format!("failed to build report: {}", e)))?;

        let samples = report.data.len() as u64;

        let profile = report
            .pprof()
            .map_err(|e| Status::internal(format!("failed to generate pprof: {}", e)))?;

        // Use the Message trait from pprof's protos module (prost 0.12 compatible)
        use pprof::protos::Message;
        let profile_data = profile.encode_to_vec();

        tracing::info!(
            duration_seconds = duration,
            samples = samples,
            profile_bytes = profile_data.len(),
            "CPU profile completed"
        );

        Ok(Response::new(CpuProfileResponse {
            profile_data,
            duration_seconds: duration,
            samples,
        }))
    }

    async fn request_split(
        &self,
        req: Request<RequestSplitRequest>,
    ) -> Result<Response<RequestSplitResponse>, Status> {
        let r = req.into_inner();

        let shard_id = Self::parse_shard_id(&r.shard_id)?;
        if r.split_point.is_empty() {
            return Err(Status::invalid_argument("split_point is required"));
        }

        let splitter = ShardSplitter::new(Arc::clone(&self.coordinator));

        let split = splitter
            .request_split(shard_id, r.split_point)
            .await
            .map_err(|e| match e {
                crate::coordination::CoordinationError::NotShardOwner(_) => {
                    Status::failed_precondition("this node does not own the shard")
                }
                crate::coordination::CoordinationError::ShardNotFound(_) => {
                    Status::not_found("shard not found")
                }
                crate::coordination::CoordinationError::SplitAlreadyInProgress(_) => {
                    Status::failed_precondition("a split is already in progress for this shard")
                }
                crate::coordination::CoordinationError::ShardMapError(ref sme) => {
                    Status::invalid_argument(format!("invalid split: {}", sme))
                }
                other => Status::internal(format!("split request failed: {}", other)),
            })?;

        // Spawn a background task to execute the split through all phases.
        // The request returns immediately with phase=SplitRequested, and the split
        // progresses asynchronously. Clients can poll GetSplitStatus to track progress.
        let coordinator = Arc::clone(&self.coordinator);
        tokio::spawn(async move {
            let splitter = ShardSplitter::new(Arc::clone(&coordinator));
            if let Err(e) = splitter
                .execute_split(shard_id, || coordinator.get_shard_owner_map())
                .await
            {
                tracing::error!(shard_id = %shard_id, error = %e, "split execution failed");
            }
        });

        Ok(Response::new(RequestSplitResponse {
            left_child_id: split.left_child_id.to_string(),
            right_child_id: split.right_child_id.to_string(),
            phase: split.phase.to_string(),
        }))
    }

    async fn get_split_status(
        &self,
        req: Request<GetSplitStatusRequest>,
    ) -> Result<Response<GetSplitStatusResponse>, Status> {
        let r = req.into_inner();

        // Parse the shard ID
        let shard_id = Self::parse_shard_id(&r.shard_id)?;

        let splitter = ShardSplitter::new(Arc::clone(&self.coordinator));

        let split_opt = splitter
            .get_split_status(shard_id)
            .await
            .map_err(|e| Status::internal(format!("failed to get split status: {}", e)))?;

        match split_opt {
            Some(split) => Ok(Response::new(GetSplitStatusResponse {
                in_progress: true,
                phase: split.phase.to_string(),
                left_child_id: split.left_child_id.to_string(),
                right_child_id: split.right_child_id.to_string(),
                split_point: split.split_point,
                initiator_node_id: split.initiator_node_id,
                requested_at_ms: split.requested_at_ms,
            })),
            None => Ok(Response::new(GetSplitStatusResponse {
                in_progress: false,
                phase: String::new(),
                left_child_id: String::new(),
                right_child_id: String::new(),
                split_point: String::new(),
                initiator_node_id: String::new(),
                requested_at_ms: 0,
            })),
        }
    }

    async fn get_node_info(
        &self,
        _req: Request<GetNodeInfoRequest>,
    ) -> Result<Response<GetNodeInfoResponse>, Status> {
        let coord = &self.coordinator;
        let factory = &self.factory;

        // Get all owned shards and their info (counters + cleanup status)
        let owned_shard_ids = coord.owned_shards().await;
        let mut owned_shards = Vec::with_capacity(owned_shard_ids.len());

        for shard_id in owned_shard_ids {
            // Get the shard from the factory
            if let Some(shard) = factory.get(&shard_id) {
                // Get counters
                let (total_jobs, completed_jobs) = match shard.get_counters().await {
                    Ok(counters) => (counters.total_jobs, counters.completed_jobs),
                    Err(e) => {
                        tracing::warn!(
                            shard_id = %shard_id,
                            error = %e,
                            "failed to get counters from shard"
                        );
                        (0, 0)
                    }
                };

                // Get the cleanup status from the shard's database
                let cleanup_status = match shard.get_cleanup_status().await {
                    Ok(status) => status.to_string(),
                    Err(e) => {
                        tracing::warn!(
                            shard_id = %shard_id,
                            error = %e,
                            "failed to get cleanup status from shard"
                        );
                        // Default to CompactionDone if we can't read the status
                        "CompactionDone".to_string()
                    }
                };

                // Get shard metadata timestamps
                let created_at_ms = shard.get_created_at_ms().await.ok().flatten().unwrap_or(0);
                let cleanup_completed_at_ms = shard
                    .get_cleanup_completed_at_ms()
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0);

                owned_shards.push(OwnedShardInfo {
                    shard_id: shard_id.to_string(),
                    total_jobs,
                    completed_jobs,
                    cleanup_status,
                    created_at_ms,
                    cleanup_completed_at_ms,
                });
            }
        }

        Ok(Response::new(GetNodeInfoResponse {
            node_id: coord.node_id().to_string(),
            owned_shards,
            placement_rings: self.cfg.coordination.placement_rings.clone(),
        }))
    }

    async fn force_release_shard(
        &self,
        req: Request<ForceReleaseShardRequest>,
    ) -> Result<Response<ForceReleaseShardResponse>, Status> {
        let r = req.into_inner();
        let shard_id = Self::parse_shard_id(&r.shard)?;

        self.coordinator
            .force_release_shard_lease(&shard_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ForceReleaseShardResponse { released: true }))
    }

    async fn configure_shard(
        &self,
        req: Request<ConfigureShardRequest>,
    ) -> Result<Response<ConfigureShardResponse>, Status> {
        let r = req.into_inner();
        let shard_id = Self::parse_shard_id(&r.shard)?;

        // Update the shard's placement ring via the coordinator
        let (previous, current) = self
            .coordinator
            .update_shard_placement_ring(&shard_id, r.placement_ring.as_deref())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ConfigureShardResponse {
            previous_ring: previous.unwrap_or_default(),
            current_ring: current.unwrap_or_default(),
        }))
    }
}

/// Create an auth interceptor that validates Bearer tokens on incoming gRPC requests.
/// When `token` is `None`, all requests are allowed (auth disabled).
/// When `token` is `Some`, requests must include a matching `authorization: Bearer <token>` header.
fn make_auth_interceptor(
    token: Option<String>,
) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |req: Request<()>| {
        let Some(ref expected) = token else {
            return Ok(req); // No token configured → auth disabled
        };
        match req.metadata().get("authorization") {
            Some(val) => {
                let val = val
                    .to_str()
                    .map_err(|_| Status::unauthenticated("invalid authorization header"))?;
                if val.strip_prefix("Bearer ").unwrap_or("") == expected.as_str() {
                    Ok(req)
                } else {
                    Err(Status::unauthenticated("invalid auth token"))
                }
            }
            None => Err(Status::unauthenticated("missing authorization header")),
        }
    }
}

/// Run the gRPC server and a periodic reaper task together until shutdown.
pub async fn run_server(
    listener: TcpListener,
    factory: Arc<ShardFactory>,
    coordinator: Arc<dyn Coordinator>,
    cfg: crate::settings::AppConfig,
    metrics: Option<Metrics>,
    shutdown: broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let incoming = TcpListenerStream::new(listener);
    run_server_with_incoming(incoming, factory, coordinator, cfg, metrics, shutdown).await
}

/// Generic variant that accepts any incoming stream of IOs (e.g., Turmoil TcpListener) and runs the server
/// with a periodic reaper. Use this in simulations to inject custom accept loops.
pub async fn run_server_with_incoming<S, IO>(
    incoming: S,
    factory: Arc<ShardFactory>,
    coordinator: Arc<dyn Coordinator>,
    cfg: crate::settings::AppConfig,
    metrics: Option<Metrics>,
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
    let svc = SiloService::new(factory.clone(), coordinator, cfg.clone(), metrics.clone());
    let interceptor = make_auth_interceptor(cfg.server.auth_token.clone());
    let silo_server = SiloServer::new(svc)
        .max_decoding_message_size(128 * 1024 * 1024)
        .max_encoding_message_size(128 * 1024 * 1024);
    let server = tonic::service::interceptor::InterceptedService::new(silo_server, interceptor);

    // Create health service for gRPC health probes
    let (mut health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<SiloServer<SiloService>>()
        .await;

    // Periodic reaper that iterates all shards every 100ms for lease reaping,
    // and collects SlateDB metrics from each shard.
    let (tick_tx, mut tick_rx) = broadcast::channel::<()>(1);
    let reaper_factory = factory.clone();
    let reaper_metrics = metrics.clone();
    let reaper: JoinHandle<()> = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            tokio::select! {
                biased;
                _ = tick_rx.recv() => { break; }
                _ = interval.tick() => {
                    let instances = reaper_factory.instances();

                    // Update shards open metric
                    if let Some(ref m) = reaper_metrics {
                        m.set_coordination_shards_open(instances.len() as u64);
                    }

                    // Use default tenant "-" for system-level reaping
                    for (shard_id, shard) in instances.iter() {
                        let _ = shard.reap_expired_leases("-").await;

                        // Collect SlateDB storage metrics for this shard
                        if let Some(ref m) = reaper_metrics {
                            let stats = shard.slatedb_stats();
                            m.update_slatedb_stats(&shard_id.to_string(), &stats);
                        }
                    }
                }
            }
        }
    });

    // Create reflection service for grpcurl/debugging
    let reflection_service = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build reflection service");

    let serve = tonic::transport::Server::builder()
        .add_service(health_service)
        .add_service(reflection_service)
        .add_service(server)
        .serve_with_incoming_shutdown(incoming, async move {
            let _ = shutdown.recv().await;
            info!("graceful shutdown signal received");
            let _ = tick_tx.send(());
        });

    serve.await?;
    info!("all connections drained, shutting down services");
    // Note: shards are NOT closed here. The coordinator shutdown in main.rs
    // handles the ordered close→release-lease sequence, which is critical for
    // permanent leases. factory.close_all() in main.rs serves as a safety net.
    reaper.await.ok();
    Ok(())
}
