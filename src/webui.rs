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
use crate::coordination::Coordinator;
use crate::factory::ShardFactory;
use crate::job::JobStatusKind;
use crate::settings::AppConfig;

#[derive(Clone)]
pub struct AppState {
    pub factory: Arc<ShardFactory>,
    pub coordinator: Option<Arc<dyn Coordinator>>,
    pub cluster_client: Arc<ClusterClient>,
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
#[template(path = "shards.html")]
struct ShardsTemplate {
    nav_active: &'static str,
    shards: Vec<ShardRow>,
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

fn status_to_string(kind: JobStatusKind) -> String {
    match kind {
        JobStatusKind::Scheduled => "Scheduled".to_string(),
        JobStatusKind::Running => "Running".to_string(),
        JobStatusKind::Succeeded => "Succeeded".to_string(),
        JobStatusKind::Failed => "Failed".to_string(),
        JobStatusKind::Cancelled => "Cancelled".to_string(),
    }
}

async fn index_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut all_jobs: Vec<JobRow> = Vec::new();

    let sql = "SELECT id, status_kind, enqueue_time_ms, priority FROM jobs WHERE status_kind = 'Scheduled' ORDER BY enqueue_time_ms ASC LIMIT 100";

    // Query all shards in the cluster
    match state.cluster_client.query_all_shards(sql).await {
        Ok(results) => {
            for result in results {
                let shard_name = result.shard_id.to_string();
                for row in result.rows {
                    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&row.data) {
                        all_jobs.push(JobRow {
                            id: json["id"].as_str().unwrap_or("").to_string(),
                            shard: shard_name.clone(),
                            status: json["status_kind"].as_str().unwrap_or("").to_string(),
                            priority: json["priority"].as_u64().unwrap_or(50) as u8,
                            scheduled_for: format_timestamp(
                                json["enqueue_time_ms"].as_i64().unwrap_or(0),
                            ),
                        });
                    }
                }
            }
        }
        Err(e) => {
            warn!(error = %e, "failed to query shards");
        }
    }

    all_jobs.sort_by(|a, b| a.scheduled_for.cmp(&b.scheduled_for));

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
    let Some(shard) = state.factory.get(&params.shard) else {
        return Html(
            ErrorTemplate {
                nav_active: "job",
                title: "Shard Not Found".to_string(),
                code: 404,
                message: format!("Shard '{}' not found", params.shard),
            }
            .render()
            .unwrap_or_default(),
        );
    };

    let job_view = match shard.get_job("-", &params.id).await {
        Ok(Some(j)) => j,
        Ok(None) => {
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

    let status = match shard.get_job_status("-", &params.id).await {
        Ok(Some(s)) => s,
        Ok(None) => crate::job::JobStatus::scheduled(0),
        Err(_) => crate::job::JobStatus::scheduled(0),
    };

    let limits: Vec<LimitRow> = job_view
        .limits()
        .iter()
        .map(|l| match l {
            crate::job::Limit::Concurrency(c) => LimitRow {
                limit_type: "Concurrency".to_string(),
                key: c.key.clone(),
                value: c.max_concurrency.to_string(),
            },
            crate::job::Limit::RateLimit(r) => LimitRow {
                limit_type: "Rate Limit".to_string(),
                key: r.name.clone(),
                value: format!("{}/{}ms", r.limit, r.duration_ms),
            },
            crate::job::Limit::FloatingConcurrency(f) => LimitRow {
                limit_type: "Floating Concurrency".to_string(),
                key: f.key.clone(),
                value: format!(
                    "default={}, refresh={}ms",
                    f.default_max_concurrency, f.refresh_interval_ms
                ),
            },
        })
        .collect();

    let payload_str = match job_view.payload_json() {
        Ok(v) => serde_json::to_string_pretty(&v).unwrap_or_else(|_| "{}".to_string()),
        Err(_) => "{}".to_string(),
    };

    let template = JobTemplate {
        nav_active: "job",
        job_id: params.id,
        shard: params.shard,
        status: status_to_string(status.kind),
        is_terminal: status.is_terminal(),
        priority: job_view.priority(),
        enqueue_time: format_timestamp(job_view.enqueue_time_ms()),
        status_changed: format_timestamp(status.changed_at_ms),
        metadata: job_view.metadata(),
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
    let Some(shard) = state.factory.get(&params.shard) else {
        return Html(r#"<span class="text-red-400">Shard not found</span>"#.to_string());
    };

    match shard.cancel_job("-", &params.id).await {
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
    let mut all_queues: HashMap<String, (usize, usize)> = HashMap::new();

    // For queue scanning, we need direct DB access which requires local shards
    // In a distributed setup, this would require a dedicated RPC endpoint
    // For now, query local shards and note that this is a limitation
    for (_shard_name, shard) in state.factory.instances().iter() {
        let db = shard.db();

        // Scan holders
        let holders_start: Vec<u8> = b"holders/".to_vec();
        let mut holders_end: Vec<u8> = b"holders/".to_vec();
        holders_end.push(0xFF);

        if let Ok(mut iter) = db.scan::<Vec<u8>, _>(holders_start..=holders_end).await {
            while let Ok(Some(kv)) = iter.next().await {
                let key_str = String::from_utf8_lossy(&kv.key);
                let parts: Vec<&str> = key_str.split('/').collect();
                if parts.len() >= 4 {
                    let queue = parts[2].to_string();
                    let entry = all_queues.entry(queue).or_insert((0, 0));
                    entry.0 += 1;
                }
            }
        }

        // Scan requests
        let requests_start: Vec<u8> = b"requests/".to_vec();
        let mut requests_end: Vec<u8> = b"requests/".to_vec();
        requests_end.push(0xFF);

        if let Ok(mut iter) = db.scan::<Vec<u8>, _>(requests_start..=requests_end).await {
            while let Ok(Some(kv)) = iter.next().await {
                let key_str = String::from_utf8_lossy(&kv.key);
                let parts: Vec<&str> = key_str.split('/').collect();
                if parts.len() >= 3 {
                    let queue = parts[2].to_string();
                    let entry = all_queues.entry(queue).or_insert((0, 0));
                    entry.1 += 1;
                }
            }
        }
    }

    let mut queues: Vec<QueueRow> = all_queues
        .into_iter()
        .map(|(name, (holders, waiters))| QueueRow {
            name,
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
    let mut holders: Vec<HolderRow> = Vec::new();
    let mut requesters: Vec<RequesterRow> = Vec::new();

    for (shard_name, shard) in state.factory.instances().iter() {
        let db = shard.db();

        // Scan holders for this queue
        let holders_prefix = format!("holders/-/{}/", params.name);
        let holders_start: Vec<u8> = holders_prefix.as_bytes().to_vec();
        let mut holders_end: Vec<u8> = holders_start.clone();
        holders_end.push(0xFF);

        if let Ok(mut iter) = db.scan::<Vec<u8>, _>(holders_start..=holders_end).await {
            while let Ok(Some(kv)) = iter.next().await {
                let key_str = String::from_utf8_lossy(&kv.key);
                let parts: Vec<&str> = key_str.split('/').collect();
                if parts.len() >= 4 {
                    let task_id = parts[3].to_string();
                    let granted_at = if let Ok(decoded) = crate::codec::decode_holder(&kv.value) {
                        format_timestamp(decoded.granted_at_ms())
                    } else {
                        "unknown".to_string()
                    };
                    holders.push(HolderRow {
                        task_id,
                        shard: shard_name.clone(),
                        granted_at,
                    });
                }
            }
        }

        // Scan requests for this queue
        let requests_prefix = format!("requests/-/{}/", params.name);
        let requests_start: Vec<u8> = requests_prefix.as_bytes().to_vec();
        let mut requests_end: Vec<u8> = requests_start.clone();
        requests_end.push(0xFF);

        if let Ok(mut iter) = db.scan::<Vec<u8>, _>(requests_start..=requests_end).await {
            while let Ok(Some(kv)) = iter.next().await {
                let key_str = String::from_utf8_lossy(&kv.key);
                let parts: Vec<&str> = key_str.split('/').collect();
                if parts.len() >= 6 {
                    let start_time: i64 = parts[3].parse().unwrap_or(0);
                    let priority: u8 = parts[4].parse().unwrap_or(50);
                    let job_id = if let Ok(decoded) =
                        crate::codec::decode_concurrency_action(&kv.value)
                    {
                        match decoded.archived() {
                            crate::task::ArchivedConcurrencyAction::EnqueueTask {
                                job_id, ..
                            } => job_id.as_str().to_string(),
                        }
                    } else {
                        "unknown".to_string()
                    };
                    requesters.push(RequesterRow {
                        job_id,
                        shard: shard_name.clone(),
                        priority,
                        requested_at: format_timestamp(start_time),
                    });
                }
            }
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

async fn shards_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut shards: Vec<ShardRow> = Vec::new();

    let sql = "SELECT COUNT(*) as count FROM jobs";

    // Get shard owner map if coordinator is available
    let owner_map = if let Some(coordinator) = &state.coordinator {
        coordinator.get_shard_owner_map().await.ok()
    } else {
        None
    };

    // Query all shards in the cluster
    match state.cluster_client.query_all_shards(sql).await {
        Ok(results) => {
            for result in results {
                let shard_name = result.shard_id.to_string();
                let job_count = result.rows.first().map_or(0, |row| {
                    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&row.data) {
                        json["count"].as_i64().unwrap_or(0) as usize
                    } else {
                        0
                    }
                });

                let owner = if let Some(ref map) = owner_map {
                    if state.cluster_client.owns_shard(result.shard_id) {
                        "local".to_string()
                    } else {
                        map.shard_to_node
                            .get(&result.shard_id)
                            .cloned()
                            .unwrap_or_else(|| "unknown".to_string())
                    }
                } else {
                    "local".to_string()
                };

                shards.push(ShardRow {
                    name: shard_name,
                    owner,
                    job_count,
                });
            }
        }
        Err(e) => {
            warn!(error = %e, "failed to query shards");
            // Fall back to local shards only
            for (shard_name, shard) in state.factory.instances().iter() {
                let query_engine = shard.query_engine();
                let job_count = match query_engine.sql(sql).await {
                    Ok(df) => {
                        if let Ok(batches) = df.collect().await {
                            batches.first().map_or(0, |b| {
                                b.column(0)
                                    .as_any()
                                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                                    .map_or(0, |arr| arr.value(0) as usize)
                            })
                        } else {
                            0
                        }
                    }
                    Err(_) => 0,
                };

                shards.push(ShardRow {
                    name: shard_name.clone(),
                    owner: "local".to_string(),
                    job_count,
                });
            }
        }
    }

    shards.sort_by(|a, b| {
        // Sort numerically if both are numbers, otherwise alphabetically
        match (a.name.parse::<u32>(), b.name.parse::<u32>()) {
            (Ok(a_num), Ok(b_num)) => a_num.cmp(&b_num),
            _ => a.name.cmp(&b.name),
        }
    });

    let template = ShardsTemplate {
        nav_active: "shards",
        shards,
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
        .route("/shards", get(shards_handler))
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

    let state = AppState {
        factory,
        coordinator,
        cluster_client,
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
