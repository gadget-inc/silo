//! Web UI module for Silo
//!
//! Provides a simple, no-build web interface using htmx and Tailwind CDN.
//! Built with axum for routing and askama for templating.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use askama::Template;
use axum::{
    Form, Router,
    extract::{Path, Query, State},
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
};
use datafusion::arrow::array::Array;
use tokio::sync::broadcast;
use tracing::warn;

use crate::cluster_client::ClusterClient;
use crate::cluster_query::ClusterQueryEngine;
use crate::coordination::{Coordinator, SplitCleanupStatus};
use crate::factory::ShardFactory;
use crate::settings::AppConfig;

#[derive(Clone)]
pub struct AppState {
    pub factory: Arc<ShardFactory>,
    pub coordinator: Arc<dyn Coordinator>,
    pub cluster_client: Arc<ClusterClient>,
    pub query_engine: Arc<ClusterQueryEngine>,
    pub config: AppConfig,
}

#[derive(Clone)]
pub struct JobRow {
    pub id: String,
    pub shard: String,
    pub tenant: String,
    pub status: String,
    pub priority: u8,
    pub scheduled_for: String,
}

#[derive(Clone)]
pub struct QueueRow {
    pub name: String,
    pub shard: String,
    pub holders: usize,
    pub waiters: usize,
    pub total: usize,
}

#[derive(Clone)]
pub struct HolderRow {
    pub task_id: String,
    pub shard: String,
    pub granted_at: String,
}

#[derive(Clone)]
pub struct RequesterRow {
    pub job_id: String,
    pub shard: String,
    pub tenant: String,
    pub priority: u8,
    pub requested_at: String,
}

#[derive(Clone)]
pub struct LimitRow {
    pub limit_type: String,
    pub key: String,
    pub value: String,
}

#[derive(Clone)]
pub struct ShardRow {
    pub name: String,
    pub owner: String,
    pub job_count: usize,
    /// If this shard has a split in progress, the current phase
    pub split_phase: Option<String>,
    /// Cleanup status if not CompactionDone
    pub cleanup_status: Option<String>,
    /// Parent shard ID if this shard was created from a split
    pub parent_shard_id: Option<String>,
    /// Placement ring this shard belongs to (None = default ring)
    pub placement_ring: Option<String>,
}

#[derive(Clone)]
pub struct ActiveSplitRow {
    pub parent_shard_id: String,
    pub phase: String,
    pub left_child_id: String,
    pub right_child_id: String,
    pub split_point: String,
    pub initiator_node_id: String,
    pub requested_at: String,
}

#[derive(Clone)]
pub struct MemberRow {
    pub node_id: String,
    pub grpc_addr: String,
    pub is_self: bool,
    pub shard_count: usize,
    pub uptime: String,
    pub hostname: Option<String>,
    /// Placement rings this member participates in (empty = default only)
    pub placement_rings: Vec<String>,
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    nav_active: &'static str,
    jobs: Vec<JobRow>,
    shard_count: usize,
    error: Option<String>,
}

#[derive(Template)]
#[template(path = "job.html")]
struct JobTemplate {
    nav_active: &'static str,
    job_id: String,
    shard: String,
    status: String,
    is_terminal: bool,
    priority: u8,
    enqueue_time: String,
    status_changed: String,
    metadata: Vec<(String, String)>,
    limits: Vec<LimitRow>,
    payload: String,
}

#[derive(Template)]
#[template(path = "queues.html")]
struct QueuesTemplate {
    nav_active: &'static str,
    queues: Vec<QueueRow>,
    error: Option<String>,
}

#[derive(Template)]
#[template(path = "queue.html")]
struct QueueTemplate {
    nav_active: &'static str,
    name: String,
    holders: Vec<HolderRow>,
    requesters: Vec<RequesterRow>,
}

#[derive(Template)]
#[template(path = "cluster.html")]
struct ClusterTemplate {
    nav_active: &'static str,
    shards: Vec<ShardRow>,
    members: Vec<MemberRow>,
    total_shards: usize,
    owned_shards: usize,
    total_jobs: usize,
    active_splits: Vec<ActiveSplitRow>,
    shards_needing_cleanup: usize,
    error: Option<String>,
}

#[derive(Template)]
#[template(path = "error.html")]
struct ErrorTemplate {
    nav_active: &'static str,
    title: String,
    code: u16,
    message: String,
}

#[derive(Template)]
#[template(path = "sql.html")]
struct SqlTemplate {
    nav_active: &'static str,
    query: String,
    has_result: bool,
    // These are for the included sql_result.html when has_result is true
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    row_count: usize,
    total_rows: usize,
    truncated: bool,
    execution_time_ms: u64,
    error: Option<String>,
}

#[derive(Template)]
#[template(path = "sql_result.html")]
struct SqlResultTemplate {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    row_count: usize,
    total_rows: usize,
    truncated: bool,
    execution_time_ms: u64,
    error: Option<String>,
}

#[derive(Template)]
#[template(path = "status_badge.html")]
struct StatusBadgeTemplate {
    status: String,
}

#[derive(Template)]
#[template(path = "config.html")]
struct ConfigTemplate {
    nav_active: &'static str,
    config_toml: String,
}

#[derive(Template)]
#[template(path = "shard.html")]
struct ShardTemplate {
    nav_active: &'static str,
    shard_id: String,
    range_start: String,
    range_end: String,
    owner: String,
    job_count: usize,
    cleanup_status: Option<String>,
    parent_shard_id: Option<String>,
    split_phase: Option<String>,
    split_message: Option<String>,
    split_error: Option<String>,
}

#[derive(serde::Deserialize)]
pub struct JobParams {
    /// Optional shard ID (UUID string) - used to make lookup more efficient if provided
    shard: Option<String>,
    /// Optional tenant - used to make lookup more efficient if provided
    tenant: Option<String>,
    /// Job ID to look up (required)
    id: String,
}

#[derive(serde::Deserialize)]
pub struct QueueParams {
    /// Queue name to look up
    name: String,
}

#[derive(serde::Deserialize)]
pub struct SqlQueryParams {
    #[serde(default)]
    q: String,
}

#[derive(serde::Deserialize)]
pub struct ShardParams {
    /// Shard ID (UUID)
    id: String,
    /// Optional message to display (e.g., after a successful split request)
    #[serde(default)]
    message: Option<String>,
    /// Optional error message to display
    #[serde(default)]
    error: Option<String>,
}

#[derive(serde::Deserialize)]
pub struct SplitFormParams {
    /// Split point (tenant ID)
    #[serde(default)]
    split_point: String,
    /// Auto-compute midpoint
    #[serde(default)]
    auto: bool,
}

fn format_uptime(startup_time_ms: Option<i64>) -> String {
    let Some(startup_ms) = startup_time_ms else {
        return "unknown".to_string();
    };

    let now_ms = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let uptime_ms = now_ms.saturating_sub(startup_ms);
    if uptime_ms < 0 {
        return "unknown".to_string();
    }

    let total_secs = uptime_ms / 1000;
    let days = total_secs / 86400;
    let hours = (total_secs % 86400) / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;

    if days > 0 {
        format!("{}d {}h {}m", days, hours, minutes)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}s", seconds)
    }
}

fn format_timestamp(ms: i64) -> String {
    let d = UNIX_EPOCH + Duration::from_millis(ms as u64);
    let secs = d.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();

    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hour = (time_of_day / 3600) as u32;
    let minute = ((time_of_day % 3600) / 60) as u32;
    let second = (time_of_day % 60) as u32;

    let mut year = 1970u32;
    let mut remaining_days = days;

    fn is_leap_year(year: u32) -> bool {
        (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
    }

    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        year += 1;
    }

    let mut month = 1u32;
    let days_in_months: [u64; 12] = if is_leap_year(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    for days_in_month in days_in_months.iter() {
        if remaining_days < *days_in_month {
            break;
        }
        remaining_days -= days_in_month;
        month += 1;
    }

    let day = remaining_days as u32 + 1;

    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        year, month, day, hour, minute, second
    )
}

async fn index_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut all_jobs: Vec<JobRow> = Vec::new();
    let mut error: Option<String> = None;

    // Use cluster query engine for proper cross-shard aggregation
    // Show all recent jobs, not just scheduled - jobs transition quickly through states
    let sql = "SELECT shard_id, tenant, id, status_kind, enqueue_time_ms, priority FROM jobs ORDER BY enqueue_time_ms DESC LIMIT 100";

    match state.query_engine.sql(sql).await {
        Ok(df) => match df.collect().await {
            Ok(batches) => {
                for batch in batches {
                    let shard_col = batch.column_by_name("shard_id").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                    });
                    let tenant_col = batch.column_by_name("tenant").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                    });
                    let id_col = batch.column_by_name("id").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                    });
                    let status_col = batch.column_by_name("status_kind").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                    });
                    let time_col = batch.column_by_name("enqueue_time_ms").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    });
                    let priority_col = batch.column_by_name("priority").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::UInt8Array>()
                    });

                    if let (
                        Some(shards),
                        Some(tenants),
                        Some(ids),
                        Some(statuses),
                        Some(times),
                        Some(priorities),
                    ) = (
                        shard_col,
                        tenant_col,
                        id_col,
                        status_col,
                        time_col,
                        priority_col,
                    ) {
                        for i in 0..batch.num_rows() {
                            all_jobs.push(JobRow {
                                id: ids.value(i).to_string(),
                                shard: shards.value(i).to_string(),
                                tenant: tenants.value(i).to_string(),
                                status: statuses.value(i).to_string(),
                                priority: priorities.value(i),
                                scheduled_for: format_timestamp(times.value(i)),
                            });
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to collect query results");
                error = Some(format!("Query execution error: {}", e));
            }
        },
        Err(e) => {
            warn!(error = %e, "failed to query jobs from cluster");
            error = Some(format!("Query error: {}", e));
        }
    }

    // Already sorted by ORDER BY clause

    // Get total shard count from coordinator
    let shard_count = state.coordinator.num_shards().await;

    let template = IndexTemplate {
        nav_active: "index",
        jobs: all_jobs,
        shard_count,
        error,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}

/// Job data fetched from the cluster query engine
struct JobQueryResult {
    shard_id: String,
    priority: u8,
    enqueue_time_ms: i64,
    payload: String,
    status_kind: String,
    status_changed_at_ms: i64,
    metadata: Vec<(String, String)>,
}

async fn job_handler(
    State(state): State<AppState>,
    Query(params): Query<JobParams>,
) -> impl IntoResponse {
    // Build SQL query with optional shard/tenant filters for performance
    // If shard and tenant are provided, the query engine can route directly to that shard
    let mut where_clauses = vec![format!("id = '{}'", params.id.replace('\'', "''"))];
    if let Some(ref shard) = params.shard {
        where_clauses.push(format!("shard_id = '{}'", shard.replace('\'', "''")));
    }
    if let Some(ref tenant) = params.tenant {
        where_clauses.push(format!("tenant = '{}'", tenant.replace('\'', "''")));
    }
    let sql = format!(
        "SELECT shard_id, priority, enqueue_time_ms, payload, status_kind, status_changed_at_ms, metadata FROM jobs WHERE {} LIMIT 1",
        where_clauses.join(" AND ")
    );

    let job_result: Option<JobQueryResult> = match state.query_engine.sql(&sql).await {
        Ok(df) => match df.collect().await {
            Ok(batches) => {
                let mut result: Option<JobQueryResult> = None;
                for batch in batches {
                    if batch.num_rows() == 0 {
                        continue;
                    }

                    let shard_id = batch
                        .column_by_name("shard_id")
                        .and_then(|c| {
                            c.as_any()
                                .downcast_ref::<datafusion::arrow::array::StringArray>()
                        })
                        .map(|a| a.value(0).to_string())
                        .unwrap_or_default();

                    let priority = batch
                        .column_by_name("priority")
                        .and_then(|c| {
                            c.as_any()
                                .downcast_ref::<datafusion::arrow::array::UInt8Array>()
                        })
                        .map(|a| a.value(0))
                        .unwrap_or(0);

                    let enqueue_time_ms = batch
                        .column_by_name("enqueue_time_ms")
                        .and_then(|c| {
                            c.as_any()
                                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                        })
                        .map(|a| a.value(0))
                        .unwrap_or(0);

                    let payload = batch
                        .column_by_name("payload")
                        .and_then(|c| {
                            c.as_any()
                                .downcast_ref::<datafusion::arrow::array::StringArray>()
                        })
                        .map(|a| {
                            if a.is_null(0) {
                                "{}".to_string()
                            } else {
                                a.value(0).to_string()
                            }
                        })
                        .unwrap_or_else(|| "{}".to_string());

                    let status_kind = batch
                        .column_by_name("status_kind")
                        .and_then(|c| {
                            c.as_any()
                                .downcast_ref::<datafusion::arrow::array::StringArray>()
                        })
                        .map(|a| {
                            if a.is_null(0) {
                                "Scheduled".to_string()
                            } else {
                                a.value(0).to_string()
                            }
                        })
                        .unwrap_or_else(|| "Scheduled".to_string());

                    let status_changed_at_ms = batch
                        .column_by_name("status_changed_at_ms")
                        .and_then(|c| {
                            c.as_any()
                                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                        })
                        .map(|a| if a.is_null(0) { 0 } else { a.value(0) })
                        .unwrap_or(0);

                    // Parse metadata from Arrow Map type
                    let metadata = batch
                        .column_by_name("metadata")
                        .and_then(|c| {
                            c.as_any()
                                .downcast_ref::<datafusion::arrow::array::MapArray>()
                        })
                        .map(|map_array| {
                            let mut meta = Vec::new();
                            if !map_array.is_null(0) {
                                let entries = map_array.value(0);
                                let struct_array = entries
                                    .as_any()
                                    .downcast_ref::<datafusion::arrow::array::StructArray>(
                                );
                                if let Some(sa) = struct_array {
                                    let keys = sa
                                        .column(0)
                                        .as_any()
                                        .downcast_ref::<datafusion::arrow::array::StringArray>(
                                    );
                                    let vals = sa
                                        .column(1)
                                        .as_any()
                                        .downcast_ref::<datafusion::arrow::array::StringArray>(
                                    );
                                    if let (Some(k), Some(v)) = (keys, vals) {
                                        for i in 0..k.len() {
                                            if !k.is_null(i) {
                                                let key = k.value(i).to_string();
                                                let val = if v.is_null(i) {
                                                    String::new()
                                                } else {
                                                    v.value(i).to_string()
                                                };
                                                meta.push((key, val));
                                            }
                                        }
                                    }
                                }
                            }
                            meta
                        })
                        .unwrap_or_default();

                    result = Some(JobQueryResult {
                        shard_id,
                        priority,
                        enqueue_time_ms,
                        payload,
                        status_kind,
                        status_changed_at_ms,
                        metadata,
                    });
                    break;
                }
                result
            }
            Err(e) => {
                warn!(error = %e, job_id = %params.id, "failed to query job from cluster");
                None
            }
        },
        Err(e) => {
            warn!(error = %e, job_id = %params.id, "failed to query job from cluster");
            None
        }
    };

    let Some(job) = job_result else {
        return Html(
            ErrorTemplate {
                nav_active: "job",
                title: "Job Not Found".to_string(),
                code: 404,
                message: format!("Job '{}' not found in cluster", params.id),
            }
            .render()
            .unwrap_or_default(),
        );
    };

    let is_terminal = matches!(
        job.status_kind.as_str(),
        "Succeeded" | "Failed" | "Cancelled"
    );

    // Limits are not available from the query engine - would need to be added to the jobs scanner
    let limits: Vec<LimitRow> = Vec::new();

    // Pretty-print the payload JSON
    let payload_str = serde_json::from_str::<serde_json::Value>(&job.payload)
        .map(|v| serde_json::to_string_pretty(&v).unwrap_or_else(|_| job.payload.clone()))
        .unwrap_or_else(|_| job.payload.clone());

    let template = JobTemplate {
        nav_active: "job",
        job_id: params.id,
        shard: job.shard_id.to_string(),
        status: job.status_kind,
        is_terminal,
        priority: job.priority,
        enqueue_time: format_timestamp(job.enqueue_time_ms),
        status_changed: format_timestamp(job.status_changed_at_ms),
        metadata: job.metadata,
        limits,
        payload: payload_str,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}

async fn cancel_job_handler(
    State(state): State<AppState>,
    Query(params): Query<JobParams>,
) -> impl IntoResponse {
    // If both shard and tenant are provided, we can cancel directly without querying
    // Otherwise, query the job's shard and tenant from the cluster query engine
    let (shard_id, tenant): (crate::shard_range::ShardId, String) =
        if let (Some(shard_str), Some(tenant)) = (params.shard.clone(), params.tenant.clone()) {
            match crate::shard_range::ShardId::parse(&shard_str) {
                Ok(shard_id) => (shard_id, tenant),
                Err(_) => {
                    return Html(format!(
                        r#"<span class="text-red-400">Invalid shard ID: {}</span>"#,
                        shard_str
                    ));
                }
            }
        } else {
            // Build query with optional filters for efficiency
            let mut where_clauses = vec![format!("id = '{}'", params.id.replace('\'', "''"))];
            if let Some(ref shard) = params.shard {
                where_clauses.push(format!("shard_id = '{}'", shard.replace('\'', "''")));
            }
            if let Some(ref tenant) = params.tenant {
                where_clauses.push(format!("tenant = '{}'", tenant.replace('\'', "''")));
            }
            let sql = format!(
                "SELECT shard_id, tenant FROM jobs WHERE {} LIMIT 1",
                where_clauses.join(" AND ")
            );

            let job_info: Option<(crate::shard_range::ShardId, String)> =
                match state.query_engine.sql(&sql).await {
                    Ok(df) => match df.collect().await {
                        Ok(batches) => {
                            let mut result = None;
                            for batch in batches {
                                if batch.num_rows() == 0 {
                                    continue;
                                }
                                let shard_id = batch
                                    .column_by_name("shard_id")
                                    .and_then(|c| {
                                        c.as_any()
                                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                                    })
                                    .and_then(|a| {
                                        crate::shard_range::ShardId::parse(a.value(0)).ok()
                                    });
                                let tenant = batch
                                    .column_by_name("tenant")
                                    .and_then(|c| {
                                        c.as_any()
                                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                                    })
                                    .map(|a| a.value(0).to_string());

                                if let (Some(s), Some(t)) = (shard_id, tenant) {
                                    result = Some((s, t));
                                    break;
                                }
                            }
                            result
                        }
                        Err(_) => None,
                    },
                    Err(_) => None,
                };

            let Some((shard_id, tenant)) = job_info else {
                return Html(r#"<span class="text-red-400">Job not found</span>"#.to_string());
            };
            (shard_id, tenant)
        };

    match state
        .cluster_client
        .cancel_job(&shard_id, &tenant, &params.id)
        .await
    {
        Ok(()) => Html(
            StatusBadgeTemplate {
                status: "Cancelled".to_string(),
            }
            .render()
            .unwrap_or_default(),
        ),
        Err(e) => Html(format!(r#"<span class="text-red-400">Error: {}</span>"#, e)),
    }
}

async fn queues_handler(State(state): State<AppState>) -> impl IntoResponse {
    // Key: queue_name -> (holders, waiters)
    let mut all_queues: HashMap<String, (usize, usize)> = HashMap::new();
    let mut error: Option<String> = None;

    // Use cluster query engine for proper aggregation across all shards
    let sql = "SELECT queue_name, entry_type, COUNT(*) as cnt FROM queues GROUP BY queue_name, entry_type";

    match state.query_engine.sql(sql).await {
        Ok(df) => match df.collect().await {
            Ok(batches) => {
                for batch in batches {
                    let name_col = batch.column_by_name("queue_name").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                    });
                    let type_col = batch.column_by_name("entry_type").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                    });
                    let cnt_col = batch.column_by_name("cnt").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    });

                    if let (Some(names), Some(types), Some(counts)) = (name_col, type_col, cnt_col)
                    {
                        for i in 0..batch.num_rows() {
                            let queue_name = names.value(i).to_string();
                            let entry_type = types.value(i);
                            let count = counts.value(i) as usize;

                            if !queue_name.is_empty() {
                                let entry = all_queues.entry(queue_name).or_insert((0, 0));
                                match entry_type {
                                    "holder" => entry.0 += count,
                                    "requester" => entry.1 += count,
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to collect query results");
                error = Some(format!("Query execution error: {}", e));
            }
        },
        Err(e) => {
            warn!(error = %e, "failed to query queues from cluster");
            error = Some(format!("Query error: {}", e));
        }
    }

    let mut queues: Vec<QueueRow> = all_queues
        .into_iter()
        .map(|(name, (holders, waiters))| QueueRow {
            name,
            shard: "cluster".to_string(), // Aggregated across cluster
            holders,
            waiters,
            total: holders + waiters,
        })
        .collect();
    queues.sort_by(|a, b| b.holders.cmp(&a.holders));

    let template = QueuesTemplate {
        nav_active: "queues",
        queues,
        error,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}

async fn queue_handler(
    State(state): State<AppState>,
    Query(params): Query<QueueParams>,
) -> impl IntoResponse {
    let mut holders: Vec<HolderRow> = Vec::new();
    let mut requesters: Vec<RequesterRow> = Vec::new();

    // Query queue data from ALL shards via cluster query engine
    let sql = format!(
        "SELECT shard_id, tenant, task_id, job_id, entry_type, priority, timestamp_ms FROM queues WHERE queue_name = '{}'",
        params.name.replace('\'', "''") // Basic SQL escaping
    );

    match state.query_engine.sql(&sql).await {
        Ok(df) => match df.collect().await {
            Ok(batches) => {
                for batch in batches {
                    let shard_col = batch.column_by_name("shard_id").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                    });
                    let tenant_col = batch.column_by_name("tenant").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                    });
                    let task_col = batch.column_by_name("task_id").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                    });
                    let job_col = batch.column_by_name("job_id").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                    });
                    let entry_col = batch.column_by_name("entry_type").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::StringArray>()
                    });
                    let priority_col = batch.column_by_name("priority").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::UInt8Array>()
                    });
                    let timestamp_col = batch.column_by_name("timestamp_ms").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    });

                    if let (
                        Some(shards),
                        Some(tenants),
                        Some(tasks),
                        Some(jobs),
                        Some(entries),
                        Some(priorities),
                        Some(timestamps),
                    ) = (
                        shard_col,
                        tenant_col,
                        task_col,
                        job_col,
                        entry_col,
                        priority_col,
                        timestamp_col,
                    ) {
                        for i in 0..batch.num_rows() {
                            let shard_id = shards.value(i);
                            let tenant = tenants.value(i).to_string();
                            let task_id = tasks.value(i).to_string();
                            let job_id = jobs.value(i).to_string();
                            let entry_type = entries.value(i);
                            let priority = priorities.value(i);
                            let timestamp_ms = timestamps.value(i);

                            match entry_type {
                                "holder" => {
                                    holders.push(HolderRow {
                                        task_id,
                                        shard: shard_id.to_string(),
                                        granted_at: format_timestamp(timestamp_ms),
                                    });
                                }
                                "requester" => {
                                    requesters.push(RequesterRow {
                                        job_id,
                                        shard: shard_id.to_string(),
                                        tenant,
                                        priority,
                                        requested_at: format_timestamp(timestamp_ms),
                                    });
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to collect queue query results");
            }
        },
        Err(e) => {
            warn!(error = %e, "failed to query queue from cluster");
        }
    }

    let template = QueueTemplate {
        nav_active: "queues",
        name: params.name,
        holders,
        requesters,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}

async fn cluster_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut shards: Vec<ShardRow> = Vec::new();
    let mut members: Vec<MemberRow> = Vec::new();
    let mut active_splits: Vec<ActiveSplitRow> = Vec::new();

    let coordinator = &state.coordinator;
    let owner_map = coordinator.get_shard_owner_map().await.ok();
    let this_node_id = coordinator.node_id().to_string();
    let shard_map = coordinator.get_shard_map().await.ok();

    // Fetch cluster members
    if let Ok(member_infos) = coordinator.get_members().await {
        for member_info in member_infos {
            // Count shards owned by this member
            let shard_count = if let Some(m) = &owner_map {
                m.shard_to_node
                    .values()
                    .filter(|&n| n == &member_info.node_id)
                    .count()
            } else {
                0
            };

            members.push(MemberRow {
                is_self: member_info.node_id == this_node_id,
                node_id: member_info.node_id,
                grpc_addr: member_info.grpc_addr,
                shard_count,
                uptime: format_uptime(member_info.startup_time_ms),
                hostname: member_info.hostname,
                placement_rings: member_info.placement_rings,
            });
        }
    }

    // Fetch active splits
    if let Ok(splits) = coordinator.list_all_splits().await {
        for split in splits {
            active_splits.push(ActiveSplitRow {
                parent_shard_id: split.parent_shard_id.to_string(),
                phase: split.phase.to_string(),
                left_child_id: split.left_child_id.to_string(),
                right_child_id: split.right_child_id.to_string(),
                split_point: split.split_point.clone(),
                initiator_node_id: split.initiator_node_id.clone(),
                requested_at: format_timestamp(split.requested_at_ms),
            });
        }
    }

    // Sort members: self first, then by node_id
    members.sort_by(|a, b| match (a.is_self, b.is_self) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.node_id.cmp(&b.node_id),
    });

    // Get shard IDs from owner map or factory
    let shard_ids: Vec<crate::shard_range::ShardId> = if let Some(ref map) = owner_map {
        map.shard_ids()
    } else {
        state.factory.instances().keys().copied().collect()
    };

    // Get node info from all cluster nodes (counters + cleanup status)
    // This uses the GetNodeInfo RPC for all nodes
    let mut error: Option<String> = None;
    let cluster_info = match state.cluster_client.get_all_node_info().await {
        Ok(info) => info,
        Err(e) => {
            warn!(error = %e, "failed to get node info from cluster");
            error = Some(format!("Failed to get node info: {}", e));
            crate::cluster_client::ClusterNodeInfo { nodes: vec![] }
        }
    };
    let shard_info_map = cluster_info.shard_info_map();

    // Build a lookup map for active splits by parent shard
    let active_split_map: HashMap<String, &ActiveSplitRow> = active_splits
        .iter()
        .map(|s| (s.parent_shard_id.clone(), s))
        .collect();

    // Build shard rows from the node info map, filling in 0 for missing shards
    for shard_id in shard_ids {
        let shard_info = shard_info_map.get(&shard_id);
        let job_count = shard_info.map(|c| c.total_jobs as usize).unwrap_or(0);
        let owner = if let Some(ref map) = owner_map {
            if state.cluster_client.owns_shard(&shard_id) {
                "local".to_string()
            } else {
                map.shard_to_node
                    .get(&shard_id)
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string())
            }
        } else {
            "local".to_string()
        };

        // Check if this shard has a split in progress
        let split_phase = active_split_map
            .get(&shard_id.to_string())
            .map(|s| s.phase.clone());

        // Get cleanup status from the cluster-wide node info
        let cleanup_status = shard_info.and_then(|info| {
            if info.cleanup_status != SplitCleanupStatus::CompactionDone {
                Some(info.cleanup_status.to_string())
            } else {
                None
            }
        });

        let (parent_shard_id, placement_ring) = shard_map
            .as_ref()
            .and_then(|map| map.get_shard(&shard_id))
            .map(|info| {
                (
                    info.parent_shard_id.map(|p| p.to_string()),
                    info.placement_ring.clone(),
                )
            })
            .unwrap_or((None, None));

        shards.push(ShardRow {
            name: shard_id.to_string(),
            owner,
            job_count,
            split_phase,
            cleanup_status,
            parent_shard_id,
            placement_ring,
        });
    }

    // If members list is empty (e.g., get_members failed), add self as fallback
    if members.is_empty() {
        // Use advertised_grpc_addr if set, otherwise fall back to the bind address
        let grpc_addr = state
            .config
            .coordination
            .advertised_grpc_addr
            .as_ref()
            .unwrap_or(&state.config.server.grpc_addr)
            .clone();

        members.push(MemberRow {
            node_id: this_node_id.clone(),
            grpc_addr,
            is_self: true,
            shard_count: shards.len(),
            uptime: "unknown".to_string(),
            hostname: crate::coordination::get_hostname(),
            placement_rings: state.config.coordination.placement_rings.clone(),
        });
    }

    shards.sort_by(|a, b| {
        // Sort numerically if both are numbers, otherwise alphabetically
        match (a.name.parse::<u32>(), b.name.parse::<u32>()) {
            (Ok(a_num), Ok(b_num)) => a_num.cmp(&b_num),
            _ => a.name.cmp(&b.name),
        }
    });

    // Calculate stats
    let total_shards = shards.len();
    let owned_shards = members
        .iter()
        .find(|m| m.is_self)
        .map(|m| m.shard_count)
        .unwrap_or(0);
    let total_jobs: usize = shards.iter().map(|s| s.job_count).sum();
    let shards_needing_cleanup = shards.iter().filter(|s| s.cleanup_status.is_some()).count();

    let template = ClusterTemplate {
        nav_active: "cluster",
        shards,
        members,
        total_shards,
        owned_shards,
        total_jobs,
        active_splits,
        shards_needing_cleanup,
        error,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}

/// Maximum number of rows to return from SQL queries to avoid crashing the browser
const SQL_RESULT_LIMIT: usize = 1000;

async fn sql_handler(Query(params): Query<SqlQueryParams>) -> impl IntoResponse {
    let template = SqlTemplate {
        nav_active: "sql",
        query: params.q,
        has_result: false,
        columns: Vec::new(),
        rows: Vec::new(),
        row_count: 0,
        total_rows: 0,
        truncated: false,
        execution_time_ms: 0,
        error: None,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}

async fn sql_execute_handler(
    State(state): State<AppState>,
    Query(params): Query<SqlQueryParams>,
) -> impl IntoResponse {
    let start = std::time::Instant::now();

    if params.q.trim().is_empty() {
        return Html(
            SqlResultTemplate {
                columns: Vec::new(),
                rows: Vec::new(),
                row_count: 0,
                total_rows: 0,
                truncated: false,
                execution_time_ms: 0,
                error: Some("Please enter a SQL query".to_string()),
            }
            .render()
            .unwrap_or_default(),
        );
    }

    // Execute the query
    match state.query_engine.sql(&params.q).await {
        Ok(df) => {
            match df.collect().await {
                Ok(batches) => {
                    let execution_time_ms = start.elapsed().as_millis() as u64;

                    // Extract column names from schema
                    let columns: Vec<String> = if let Some(batch) = batches.first() {
                        batch
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect()
                    } else {
                        Vec::new()
                    };

                    // Extract rows as strings, with limit
                    let mut rows: Vec<Vec<String>> = Vec::new();
                    let mut total_rows: usize = 0;

                    for batch in &batches {
                        for row_idx in 0..batch.num_rows() {
                            total_rows += 1;
                            if rows.len() >= SQL_RESULT_LIMIT {
                                continue; // Keep counting but don't add more rows
                            }

                            let mut row: Vec<String> = Vec::new();
                            for col_idx in 0..batch.num_columns() {
                                let col = batch.column(col_idx);
                                let value = array_value_to_string(col, row_idx);
                                row.push(value);
                            }
                            rows.push(row);
                        }
                    }

                    let truncated = total_rows > SQL_RESULT_LIMIT;
                    let row_count = rows.len();

                    Html(
                        SqlResultTemplate {
                            columns,
                            rows,
                            row_count,
                            total_rows,
                            truncated,
                            execution_time_ms,
                            error: None,
                        }
                        .render()
                        .unwrap_or_default(),
                    )
                }
                Err(e) => {
                    let execution_time_ms = start.elapsed().as_millis() as u64;
                    Html(
                        SqlResultTemplate {
                            columns: Vec::new(),
                            rows: Vec::new(),
                            row_count: 0,
                            total_rows: 0,
                            truncated: false,
                            execution_time_ms,
                            error: Some(format!("Execution error: {}", e)),
                        }
                        .render()
                        .unwrap_or_default(),
                    )
                }
            }
        }
        Err(e) => {
            let execution_time_ms = start.elapsed().as_millis() as u64;
            Html(
                SqlResultTemplate {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    row_count: 0,
                    total_rows: 0,
                    truncated: false,
                    execution_time_ms,
                    error: Some(format!("Query error: {}", e)),
                }
                .render()
                .unwrap_or_default(),
            )
        }
    }
}

/// Convert an Arrow array value at a given index to a string representation
fn array_value_to_string(array: &dyn datafusion::arrow::array::Array, index: usize) -> String {
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::DataType;

    if array.is_null(index) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|a| format!("<{} bytes>", a.value(index).len()))
            .unwrap_or_default(),
        DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .map(|a| format!("<{} bytes>", a.value(index).len()))
            .unwrap_or_default(),
        _ => format!("<{}>", array.data_type()),
    }
}

async fn config_handler(State(state): State<AppState>) -> impl IntoResponse {
    // Serialize the config to TOML for display
    let config_toml = toml::to_string_pretty(&state.config)
        .unwrap_or_else(|e| format!("# Error serializing config: {}", e));

    let template = ConfigTemplate {
        nav_active: "config",
        config_toml,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}

async fn shard_handler(
    State(state): State<AppState>,
    Query(params): Query<ShardParams>,
) -> impl IntoResponse {
    let shard_id = match crate::shard_range::ShardId::parse(&params.id) {
        Ok(id) => id,
        Err(_) => {
            return Html(
                ErrorTemplate {
                    nav_active: "cluster",
                    title: "Invalid Shard ID".to_string(),
                    code: 400,
                    message: format!("'{}' is not a valid shard UUID", params.id),
                }
                .render()
                .unwrap_or_default(),
            );
        }
    };

    // Get shard info from coordinator
    let coordinator = &state.coordinator;
    let shard_map = coordinator.get_shard_map().await.ok();
    let owner_map = coordinator.get_shard_owner_map().await.ok();

    let shard_info = shard_map
        .as_ref()
        .and_then(|m| m.get_shard(&shard_id).cloned());

    let owner = if let Some(ref map) = owner_map {
        if state.cluster_client.owns_shard(&shard_id) {
            "local".to_string()
        } else {
            map.shard_to_node
                .get(&shard_id)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string())
        }
    } else {
        "local".to_string()
    };

    // Get node info (counters + cleanup status) from the cluster
    let cluster_info = state.cluster_client.get_all_node_info().await.ok();
    let shard_node_info = cluster_info
        .as_ref()
        .and_then(|info| info.get_shard_info(&shard_id).cloned());

    let job_count = shard_node_info
        .as_ref()
        .map(|info| info.total_jobs as usize)
        .unwrap_or(0);

    // Check for active split
    let split_phase = coordinator
        .load_split(&shard_id)
        .await
        .ok()
        .flatten()
        .map(|s| s.phase.to_string());

    let Some(info) = shard_info else {
        return Html(
            ErrorTemplate {
                nav_active: "cluster",
                title: "Shard Not Found".to_string(),
                code: 404,
                message: format!("Shard '{}' not found in cluster", params.id),
            }
            .render()
            .unwrap_or_default(),
        );
    };

    let cleanup_status = shard_node_info.as_ref().and_then(|node_info| {
        if node_info.cleanup_status != SplitCleanupStatus::CompactionDone {
            Some(node_info.cleanup_status.to_string())
        } else {
            None
        }
    });

    let template = ShardTemplate {
        nav_active: "cluster",
        shard_id: params.id,
        range_start: if info.range.start.is_empty() {
            "-∞".to_string()
        } else {
            info.range.start.clone()
        },
        range_end: if info.range.end.is_empty() {
            "+∞".to_string()
        } else {
            info.range.end.clone()
        },
        owner,
        job_count,
        cleanup_status,
        parent_shard_id: info.parent_shard_id.map(|p| p.to_string()),
        split_phase,
        split_message: params.message,
        split_error: params.error,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}

async fn shard_split_handler(
    State(state): State<AppState>,
    Path(shard_id): Path<String>,
    Form(params): Form<SplitFormParams>,
) -> impl IntoResponse {
    let parsed_shard_id = match crate::shard_range::ShardId::parse(&shard_id) {
        Ok(id) => id,
        Err(_) => {
            return Redirect::to(&format!(
                "/shard?id={}&error={}",
                shard_id,
                urlencoding::encode("Invalid shard ID")
            ));
        }
    };

    // Determine split point
    let split_point = if params.auto || params.split_point.is_empty() {
        // Auto-compute midpoint from shard range
        if let Ok(shard_map) = state.coordinator.get_shard_map().await {
            if let Some(info) = shard_map.get_shard(&parsed_shard_id) {
                info.range.midpoint()
            } else {
                None
            }
        } else {
            None
        }
    } else {
        Some(params.split_point.clone())
    };

    let Some(split_point) = split_point else {
        return Redirect::to(&format!(
            "/shard?id={}&error={}",
            shard_id,
            urlencoding::encode("Could not compute split point. Please specify one manually.")
        ));
    };

    // Request the split via gRPC
    let result = state
        .cluster_client
        .request_split(&parsed_shard_id, &split_point)
        .await;

    match result {
        Ok(response) => Redirect::to(&format!(
            "/shard?id={}&message={}",
            shard_id,
            urlencoding::encode(&format!(
                "Split requested successfully. Left child: {}, Right child: {}",
                response.left_child_id, response.right_child_id
            ))
        )),
        Err(e) => Redirect::to(&format!(
            "/shard?id={}&error={}",
            shard_id,
            urlencoding::encode(&format!("Split failed: {}", e))
        )),
    }
}

async fn not_found_handler() -> impl IntoResponse {
    Html(
        ErrorTemplate {
            nav_active: "",
            title: "Not Found".to_string(),
            code: 404,
            message: "Page not found".to_string(),
        }
        .render()
        .unwrap_or_default(),
    )
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index_handler))
        .route("/job", get(job_handler))
        .route("/job/cancel", post(cancel_job_handler))
        .route("/queues", get(queues_handler))
        .route("/queue", get(queue_handler))
        .route("/cluster", get(cluster_handler))
        .route("/shard", get(shard_handler))
        .route("/shard/{id}/split", post(shard_split_handler))
        .route("/sql", get(sql_handler))
        .route("/sql/execute", get(sql_execute_handler))
        .route("/config", get(config_handler))
        .fallback(not_found_handler)
        .with_state(state)
}

/// Run the web UI server
pub async fn run_webui(
    addr: SocketAddr,
    factory: Arc<ShardFactory>,
    coordinator: Arc<dyn Coordinator>,
    cfg: AppConfig,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cluster_client = Arc::new(ClusterClient::new(
        factory.clone(),
        Some(coordinator.clone()),
    ));

    let query_engine = Arc::new(
        ClusterQueryEngine::new(factory.clone(), Some(coordinator.clone()))
            .await
            .expect("Failed to create cluster query engine"),
    );

    let state = AppState {
        factory,
        coordinator,
        cluster_client,
        query_engine,
        config: cfg,
    };

    let app = create_router(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.recv().await;
        })
        .await?;

    Ok(())
}
