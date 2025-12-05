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
    extract::{Query, State},
    response::{Html, IntoResponse},
    routing::{get, post},
    Router,
};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::cluster_client::ClusterClient;
use crate::cluster_query::ClusterQueryEngine;
use crate::coordination::Coordinator;
use crate::factory::ShardFactory;
use crate::settings::AppConfig;

#[derive(Clone)]
pub struct AppState {
    pub factory: Arc<ShardFactory>,
    pub coordinator: Option<Arc<dyn Coordinator>>,
    pub cluster_client: Arc<ClusterClient>,
    pub query_engine: Arc<ClusterQueryEngine>,
}

#[derive(Clone)]
pub struct JobRow {
    pub id: String,
    pub shard: String,
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
}

#[derive(Clone)]
pub struct MemberRow {
    pub node_id: String,
    pub grpc_addr: String,
    pub is_self: bool,
    pub shard_count: usize,
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    nav_active: &'static str,
    jobs: Vec<JobRow>,
    shard_count: usize,
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
#[template(path = "status_badge.html")]
struct StatusBadgeTemplate {
    status: String,
}

#[derive(serde::Deserialize)]
pub struct JobParams {
    shard: String,
    id: String,
}

#[derive(serde::Deserialize)]
pub struct QueueParams {
    shard: String,
    name: String,
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

    // Use cluster query engine for proper cross-shard aggregation
    let sql = "SELECT id, status_kind, enqueue_time_ms, priority FROM jobs WHERE status_kind = 'Scheduled' ORDER BY enqueue_time_ms ASC LIMIT 100";

    match state.query_engine.sql(sql).await {
        Ok(df) => {
            if let Ok(batches) = df.collect().await {
                for batch in batches {
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

                    if let (Some(ids), Some(statuses), Some(times), Some(priorities)) =
                        (id_col, status_col, time_col, priority_col)
                    {
                        for i in 0..batch.num_rows() {
                            all_jobs.push(JobRow {
                                id: ids.value(i).to_string(),
                                shard: "cluster".to_string(), // No longer per-shard
                                status: statuses.value(i).to_string(),
                                priority: priorities.value(i),
                                scheduled_for: format_timestamp(times.value(i)),
                            });
                        }
                    }
                }
            }
        }
        Err(e) => {
            warn!(error = %e, "failed to query jobs from cluster");
        }
    }

    // Already sorted by ORDER BY clause

    // Get total shard count from coordinator if available
    let shard_count = if let Some(coordinator) = &state.coordinator {
        coordinator.num_shards() as usize
    } else {
        state.factory.instances().len()
    };

    let template = IndexTemplate {
        nav_active: "index",
        jobs: all_jobs,
        shard_count,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
}

async fn job_handler(
    State(state): State<AppState>,
    Query(params): Query<JobParams>,
) -> impl IntoResponse {
    // Parse shard ID
    let shard_id: u32 = match params.shard.parse() {
        Ok(id) => id,
        Err(_) => {
            return Html(
                ErrorTemplate {
                    nav_active: "job",
                    title: "Invalid Shard".to_string(),
                    code: 400,
                    message: format!("Invalid shard ID: '{}'", params.shard),
                }
                .render()
                .unwrap_or_default(),
            );
        }
    };

    // Get job from any shard (local or remote)
    let job_response = match state
        .cluster_client
        .get_job(shard_id, "-", &params.id)
        .await
    {
        Ok(j) => j,
        Err(crate::cluster_client::ClusterClientError::JobNotFound) => {
            return Html(
                ErrorTemplate {
                    nav_active: "job",
                    title: "Job Not Found".to_string(),
                    code: 404,
                    message: format!("Job '{}' not found", params.id),
                }
                .render()
                .unwrap_or_default(),
            );
        }
        Err(crate::cluster_client::ClusterClientError::ShardNotFound(_)) => {
            return Html(
                ErrorTemplate {
                    nav_active: "job",
                    title: "Shard Not Found".to_string(),
                    code: 404,
                    message: format!("Shard '{}' not found in cluster", params.shard),
                }
                .render()
                .unwrap_or_default(),
            );
        }
        Err(e) => {
            return Html(
                ErrorTemplate {
                    nav_active: "job",
                    title: "Error".to_string(),
                    code: 500,
                    message: format!("Error fetching job: {}", e),
                }
                .render()
                .unwrap_or_default(),
            );
        }
    };

    // Get job status via SQL query
    let sql = format!(
        "SELECT status_kind, status_changed_at_ms FROM jobs WHERE id = '{}'",
        params.id.replace('\'', "''") // Basic SQL escaping
    );
    let (status_kind, status_changed_at_ms) =
        match state.cluster_client.query_shard(shard_id, &sql).await {
            Ok(result) => {
                if let Some(row) = result.rows.first() {
                    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&row.data) {
                        let kind = json["status_kind"]
                            .as_str()
                            .unwrap_or("Scheduled")
                            .to_string();
                        let changed_at = json["status_changed_at_ms"].as_i64().unwrap_or(0);
                        (kind, changed_at)
                    } else {
                        ("Scheduled".to_string(), 0)
                    }
                } else {
                    ("Scheduled".to_string(), 0)
                }
            }
            Err(_) => ("Scheduled".to_string(), 0),
        };

    let is_terminal = matches!(status_kind.as_str(), "Succeeded" | "Failed" | "Cancelled");

    // Convert proto limits to LimitRow
    let limits: Vec<LimitRow> = job_response
        .limits
        .iter()
        .filter_map(|l| {
            l.limit.as_ref().map(|limit| match limit {
                crate::pb::limit::Limit::Concurrency(c) => LimitRow {
                    limit_type: "Concurrency".to_string(),
                    key: c.key.clone(),
                    value: c.max_concurrency.to_string(),
                },
                crate::pb::limit::Limit::RateLimit(r) => LimitRow {
                    limit_type: "Rate Limit".to_string(),
                    key: r.name.clone(),
                    value: format!("{}/{}ms", r.limit, r.duration_ms),
                },
                crate::pb::limit::Limit::FloatingConcurrency(f) => LimitRow {
                    limit_type: "Floating Concurrency".to_string(),
                    key: f.key.clone(),
                    value: format!(
                        "default={}, refresh={}ms",
                        f.default_max_concurrency, f.refresh_interval_ms
                    ),
                },
            })
        })
        .collect();

    // Parse payload JSON for pretty display
    let payload_str = if let Some(payload) = &job_response.payload {
        serde_json::from_slice::<serde_json::Value>(&payload.data)
            .map(|v| serde_json::to_string_pretty(&v).unwrap_or_else(|_| "{}".to_string()))
            .unwrap_or_else(|_| "{}".to_string())
    } else {
        "{}".to_string()
    };

    let template = JobTemplate {
        nav_active: "job",
        job_id: params.id,
        shard: params.shard,
        status: status_kind,
        is_terminal,
        priority: job_response.priority as u8,
        enqueue_time: format_timestamp(job_response.enqueue_time_ms),
        status_changed: format_timestamp(status_changed_at_ms),
        metadata: job_response.metadata.into_iter().collect(),
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
    // Parse shard ID
    let shard_id: u32 = match params.shard.parse() {
        Ok(id) => id,
        Err(_) => {
            return Html(r#"<span class="text-red-400">Invalid shard ID</span>"#.to_string());
        }
    };

    match state
        .cluster_client
        .cancel_job(shard_id, "-", &params.id)
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

    // Use cluster query engine for proper aggregation across all shards
    let sql = "SELECT queue_name, entry_type, COUNT(*) as cnt FROM queues GROUP BY queue_name, entry_type";

    match state.query_engine.sql(sql).await {
        Ok(df) => {
            if let Ok(batches) = df.collect().await {
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
        }
        Err(e) => {
            warn!(error = %e, "failed to query queues from cluster");
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
    // Parse shard ID
    let shard_id: u32 = match params.shard.parse() {
        Ok(id) => id,
        Err(_) => {
            return Html(
                ErrorTemplate {
                    nav_active: "queues",
                    title: "Invalid Shard".to_string(),
                    code: 400,
                    message: format!("Invalid shard ID: '{}'", params.shard),
                }
                .render()
                .unwrap_or_default(),
            );
        }
    };

    let mut holders: Vec<HolderRow> = Vec::new();
    let mut requesters: Vec<RequesterRow> = Vec::new();

    // Query queue data from the specific shard via SQL
    let sql = format!(
        "SELECT task_id, job_id, entry_type, priority, timestamp_ms FROM queues WHERE queue_name = '{}'",
        params.name.replace('\'', "''") // Basic SQL escaping
    );

    match state.cluster_client.query_shard(shard_id, &sql).await {
        Ok(result) => {
            for row in result.rows {
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&row.data) {
                    let entry_type = json["entry_type"].as_str().unwrap_or("");
                    let task_id = json["task_id"].as_str().unwrap_or("").to_string();
                    let timestamp_ms = json["timestamp_ms"].as_i64().unwrap_or(0);

                    match entry_type {
                        "holder" => {
                            holders.push(HolderRow {
                                task_id,
                                shard: params.shard.clone(),
                                granted_at: format_timestamp(timestamp_ms),
                            });
                        }
                        "requester" => {
                            let job_id = json["job_id"].as_str().unwrap_or("unknown").to_string();
                            let priority = json["priority"].as_u64().unwrap_or(50) as u8;
                            requesters.push(RequesterRow {
                                job_id,
                                shard: params.shard.clone(),
                                priority,
                                requested_at: format_timestamp(timestamp_ms),
                            });
                        }
                        _ => {}
                    }
                }
            }
        }
        Err(crate::cluster_client::ClusterClientError::ShardNotFound(_)) => {
            return Html(
                ErrorTemplate {
                    nav_active: "queues",
                    title: "Shard Not Found".to_string(),
                    code: 404,
                    message: format!("Shard '{}' not found in cluster", params.shard),
                }
                .render()
                .unwrap_or_default(),
            );
        }
        Err(e) => {
            warn!(error = %e, "failed to query queue from shard");
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

    // Get shard owner map and members if coordinator is available
    let (owner_map, this_node_id) = if let Some(coordinator) = &state.coordinator {
        let map = coordinator.get_shard_owner_map().await.ok();
        let node_id = coordinator.node_id().to_string();

        // Fetch cluster members
        if let Ok(member_infos) = coordinator.get_members().await {
            for member_info in member_infos {
                // Count shards owned by this member
                let shard_count = if let Some(ref m) = map {
                    m.shard_to_node
                        .values()
                        .filter(|&n| n == &member_info.node_id)
                        .count()
                } else {
                    0
                };

                members.push(MemberRow {
                    is_self: member_info.node_id == node_id,
                    node_id: member_info.node_id,
                    grpc_addr: member_info.grpc_addr,
                    shard_count,
                });
            }
        }

        (map, Some(node_id))
    } else {
        (None, None)
    };

    // Sort members: self first, then by node_id
    members.sort_by(|a, b| match (a.is_self, b.is_self) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.node_id.cmp(&b.node_id),
    });

    // Get total number of shards
    let num_shards = state
        .coordinator
        .as_ref()
        .map(|c| c.num_shards())
        .unwrap_or_else(|| state.factory.instances().len() as u32);

    // Initialize all shards with 0 job count
    let mut shard_job_counts: HashMap<u32, usize> = HashMap::new();
    for shard_id in 0..num_shards {
        shard_job_counts.insert(shard_id, 0);
    }

    // Query job counts per shard using GROUP BY shard_id
    let sql = "SELECT shard_id, COUNT(*) as cnt FROM jobs GROUP BY shard_id";

    match state.query_engine.sql(sql).await {
        Ok(df) => {
            if let Ok(batches) = df.collect().await {
                for batch in batches {
                    let shard_col = batch.column_by_name("shard_id").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::UInt32Array>()
                    });
                    let count_col = batch.column_by_name("cnt").and_then(|c| {
                        c.as_any()
                            .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    });

                    if let (Some(shard_ids), Some(counts)) = (shard_col, count_col) {
                        for i in 0..batch.num_rows() {
                            let shard_id = shard_ids.value(i);
                            let job_count = counts.value(i) as usize;
                            shard_job_counts.insert(shard_id, job_count);
                        }
                    }
                }
            }
        }
        Err(e) => {
            warn!(error = %e, "failed to query shards");
        }
    }

    // Build shard rows from the counts map
    for (shard_id, job_count) in shard_job_counts {
        let owner = if let Some(ref map) = owner_map {
            if state.cluster_client.owns_shard(shard_id) {
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

        shards.push(ShardRow {
            name: shard_id.to_string(),
            owner,
            job_count,
        });
    }

    // If no coordinator, add a single "self" member for standalone mode
    if members.is_empty() {
        members.push(MemberRow {
            node_id: this_node_id.unwrap_or_else(|| "standalone".to_string()),
            grpc_addr: "localhost".to_string(),
            is_self: true,
            shard_count: shards.len(),
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

    let template = ClusterTemplate {
        nav_active: "cluster",
        shards,
        members,
        total_shards,
        owned_shards,
        total_jobs,
    };

    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {}", e)),
    )
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
        .fallback(not_found_handler)
        .with_state(state)
}

/// Run the web UI server
pub async fn run_webui(
    addr: SocketAddr,
    factory: Arc<ShardFactory>,
    coordinator: Option<Arc<dyn Coordinator>>,
    _cfg: AppConfig,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cluster_client = Arc::new(ClusterClient::new(factory.clone(), coordinator.clone()));

    // Get num_shards from coordinator or default to local shards
    let num_shards = coordinator
        .as_ref()
        .map(|c| c.num_shards())
        .unwrap_or_else(|| factory.instances().len() as u32);

    let query_engine = Arc::new(
        ClusterQueryEngine::new(factory.clone(), coordinator.clone(), num_shards)
            .await
            .expect("Failed to create cluster query engine"),
    );

    let state = AppState {
        factory,
        coordinator,
        cluster_client,
        query_engine,
    };

    let app = create_router(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.recv().await;
            info!("WebUI server shutting down");
        })
        .await?;

    Ok(())
}
