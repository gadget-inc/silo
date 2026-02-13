//! Silo administration CLI tool library.
//!
//! This module provides the core functionality for `siloctl`, allowing both
//! the CLI binary and tests to use the same code.

use std::fs::File;
use std::io::Write;
use std::path::Path;

use crate::pb::silo_client::SiloClient;
use crate::pb::{
    CancelJobRequest, ConfigureShardRequest, CpuProfileRequest, DeleteJobRequest,
    ExpediteJobRequest, ForceReleaseShardRequest, GetClusterInfoRequest, GetJobRequest,
    GetSplitStatusRequest, QueryRequest, RequestSplitRequest, RestartJobRequest,
};
use flate2::Compression;
use flate2::write::GzEncoder;
use tonic::transport::Channel;

/// Global options that apply to all siloctl commands
#[derive(Debug, Clone)]
pub struct GlobalOptions {
    /// Silo server address (e.g., http://localhost:7450)
    pub address: String,
    /// Tenant ID for multi-tenant clusters
    pub tenant: Option<String>,
    /// Output in JSON format instead of human-readable tables
    pub json: bool,
}

impl Default for GlobalOptions {
    fn default() -> Self {
        Self {
            address: "http://localhost:7450".to_string(),
            tenant: None,
            json: false,
        }
    }
}

/// Ensure an address has the http:// scheme prefix
fn ensure_http_scheme(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    }
}

async fn connect(address: &str) -> anyhow::Result<SiloClient<Channel>> {
    let url = ensure_http_scheme(address);
    let channel = Channel::from_shared(url)?.connect().await?;
    Ok(SiloClient::new(channel))
}

/// Connect to the node that owns a specific shard.
///
/// Queries GetClusterInfo on the initial address to discover cluster topology,
/// then connects to the node owning the given shard. If the shard is already on
/// the connected node, reuses the existing connection.
async fn connect_to_shard_owner(
    address: &str,
    shard_id: &str,
) -> anyhow::Result<SiloClient<Channel>> {
    let mut client = connect(address).await?;
    let response = client
        .get_cluster_info(GetClusterInfoRequest {})
        .await?
        .into_inner();

    let owner = response
        .shard_owners
        .iter()
        .find(|s| s.shard_id == shard_id)
        .ok_or_else(|| anyhow::anyhow!("shard '{}' not found in cluster", shard_id))?;

    // If the shard is on this node, reuse the existing connection
    if owner.grpc_addr == response.this_grpc_addr {
        return Ok(client);
    }

    // Connect to the shard's owner
    connect(&owner.grpc_addr).await
}

/// Convert job status enum to string
fn job_status_to_string(status: i32) -> &'static str {
    match status {
        0 => "scheduled",
        1 => "running",
        2 => "succeeded",
        3 => "failed",
        4 => "cancelled",
        _ => "unknown",
    }
}

/// Convert attempt status enum to string
fn attempt_status_to_string(status: i32) -> &'static str {
    match status {
        0 => "running",
        1 => "succeeded",
        2 => "failed",
        3 => "cancelled",
        _ => "unknown",
    }
}

/// Format a timestamp in milliseconds to a human-readable string
fn format_timestamp_ms(ms: i64) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let duration = Duration::from_millis(ms as u64);
    let datetime = UNIX_EPOCH + duration;
    let secs = datetime
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{} ({}ms)", secs, ms)
}

/// Get cluster topology and shard ownership information
pub async fn cluster_info<W: Write>(opts: &GlobalOptions, out: &mut W) -> anyhow::Result<()> {
    let mut client = connect(&opts.address).await?;
    let response = client
        .get_cluster_info(GetClusterInfoRequest {})
        .await?
        .into_inner();

    if opts.json {
        let json_output = serde_json::json!({
            "num_shards": response.num_shards,
            "this_node_id": response.this_node_id,
            "this_grpc_addr": response.this_grpc_addr,
            "shard_owners": response.shard_owners.iter().map(|s| {
                serde_json::json!({
                    "shard_id": s.shard_id,
                    "node_id": s.node_id,
                    "grpc_addr": s.grpc_addr,
                    "range_start": s.range_start,
                    "range_end": s.range_end,
                })
            }).collect::<Vec<_>>(),
        });
        writeln!(out, "{}", serde_json::to_string_pretty(&json_output)?)?;
    } else {
        writeln!(out, "Cluster Information")?;
        writeln!(out, "===================")?;
        writeln!(out, "Total shards: {}", response.num_shards)?;
        writeln!(
            out,
            "Connected to: {} ({})",
            response.this_node_id, response.this_grpc_addr
        )?;
        writeln!(out)?;
        writeln!(out, "Shard Ownership:")?;
        writeln!(
            out,
            "{:<38}  {:<14}  {:<20}  gRPC Address",
            "Shard", "Range", "Node ID"
        )?;
        writeln!(out, "{}", "-".repeat(100))?;
        for owner in &response.shard_owners {
            let range = format!(
                "[{}, {})",
                if owner.range_start.is_empty() {
                    "-\u{221e}"
                } else {
                    &owner.range_start
                },
                if owner.range_end.is_empty() {
                    "+\u{221e}"
                } else {
                    &owner.range_end
                }
            );
            writeln!(
                out,
                "{:<38}  {:<14}  {:<20}  {}",
                owner.shard_id, range, owner.node_id, owner.grpc_addr
            )?;
        }
    }

    Ok(())
}

/// Get job details
pub async fn job_get<W: Write>(
    opts: &GlobalOptions,
    out: &mut W,
    shard: &str,
    id: &str,
    include_attempts: bool,
) -> anyhow::Result<()> {
    let mut client = connect_to_shard_owner(&opts.address, shard).await?;
    let response = client
        .get_job(GetJobRequest {
            shard: shard.to_string(),
            id: id.to_string(),
            tenant: opts.tenant.clone(),
            include_attempts,
        })
        .await?
        .into_inner();

    if opts.json {
        let mut json_output = serde_json::json!({
            "id": response.id,
            "status": job_status_to_string(response.status),
            "status_code": response.status,
            "priority": response.priority,
            "enqueue_time_ms": response.enqueue_time_ms,
            "status_changed_at_ms": response.status_changed_at_ms,
            "task_group": response.task_group,
            "metadata": response.metadata,
        });

        if let Some(next_attempt) = response.next_attempt_starts_after_ms {
            json_output["next_attempt_starts_after_ms"] = serde_json::json!(next_attempt);
        }

        if include_attempts && !response.attempts.is_empty() {
            json_output["attempts"] = serde_json::json!(
                response
                    .attempts
                    .iter()
                    .map(|a| {
                        serde_json::json!({
                            "attempt_number": a.attempt_number,
                            "task_id": a.task_id,
                            "status": attempt_status_to_string(a.status),
                            "started_at_ms": a.started_at_ms,
                            "finished_at_ms": a.finished_at_ms,
                        })
                    })
                    .collect::<Vec<_>>()
            );
        }

        writeln!(out, "{}", serde_json::to_string_pretty(&json_output)?)?;
    } else {
        writeln!(out, "Job Details")?;
        writeln!(out, "===========")?;
        writeln!(out, "ID:              {}", response.id)?;
        writeln!(
            out,
            "Status:          {}",
            job_status_to_string(response.status)
        )?;
        writeln!(out, "Priority:        {}", response.priority)?;
        writeln!(out, "Task Group:      {}", response.task_group)?;
        writeln!(
            out,
            "Enqueued:        {}",
            format_timestamp_ms(response.enqueue_time_ms)
        )?;
        writeln!(
            out,
            "Status Changed:  {}",
            format_timestamp_ms(response.status_changed_at_ms)
        )?;

        if let Some(next_attempt) = response.next_attempt_starts_after_ms {
            writeln!(
                out,
                "Next Attempt:    {}",
                format_timestamp_ms(next_attempt)
            )?;
        }

        if !response.metadata.is_empty() {
            writeln!(out)?;
            writeln!(out, "Metadata:")?;
            for (k, v) in &response.metadata {
                writeln!(out, "  {}: {}", k, v)?;
            }
        }

        if include_attempts && !response.attempts.is_empty() {
            writeln!(out)?;
            writeln!(out, "Attempts:")?;
            writeln!(
                out,
                "{:>8}  {:>36}  {:>10}  {:>20}",
                "Attempt", "Task ID", "Status", "Finished"
            )?;
            writeln!(out, "{}", "-".repeat(80))?;
            for attempt in &response.attempts {
                let finished = attempt
                    .finished_at_ms
                    .map(format_timestamp_ms)
                    .unwrap_or_else(|| "-".to_string());
                writeln!(
                    out,
                    "{:>8}  {:>36}  {:>10}  {:>20}",
                    attempt.attempt_number,
                    attempt.task_id,
                    attempt_status_to_string(attempt.status),
                    finished
                )?;
            }
        }
    }

    Ok(())
}

/// Cancel a job
pub async fn job_cancel<W: Write>(
    opts: &GlobalOptions,
    out: &mut W,
    shard: &str,
    id: &str,
) -> anyhow::Result<()> {
    let mut client = connect_to_shard_owner(&opts.address, shard).await?;
    client
        .cancel_job(CancelJobRequest {
            shard: shard.to_string(),
            id: id.to_string(),
            tenant: opts.tenant.clone(),
        })
        .await?;

    if opts.json {
        writeln!(out, r#"{{"status": "cancelled", "job_id": "{}"}}"#, id)?;
    } else {
        writeln!(out, "Job {} cancelled successfully", id)?;
    }

    Ok(())
}

/// Restart a cancelled or failed job
pub async fn job_restart<W: Write>(
    opts: &GlobalOptions,
    out: &mut W,
    shard: &str,
    id: &str,
) -> anyhow::Result<()> {
    let mut client = connect_to_shard_owner(&opts.address, shard).await?;
    client
        .restart_job(RestartJobRequest {
            shard: shard.to_string(),
            id: id.to_string(),
            tenant: opts.tenant.clone(),
        })
        .await?;

    if opts.json {
        writeln!(out, r#"{{"status": "restarted", "job_id": "{}"}}"#, id)?;
    } else {
        writeln!(out, "Job {} restarted successfully", id)?;
    }

    Ok(())
}

/// Expedite a scheduled job to run immediately
pub async fn job_expedite<W: Write>(
    opts: &GlobalOptions,
    out: &mut W,
    shard: &str,
    id: &str,
) -> anyhow::Result<()> {
    let mut client = connect_to_shard_owner(&opts.address, shard).await?;
    client
        .expedite_job(ExpediteJobRequest {
            shard: shard.to_string(),
            id: id.to_string(),
            tenant: opts.tenant.clone(),
        })
        .await?;

    if opts.json {
        writeln!(out, r#"{{"status": "expedited", "job_id": "{}"}}"#, id)?;
    } else {
        writeln!(out, "Job {} expedited successfully", id)?;
    }

    Ok(())
}

/// Delete a job permanently
pub async fn job_delete<W: Write>(
    opts: &GlobalOptions,
    out: &mut W,
    shard: &str,
    id: &str,
) -> anyhow::Result<()> {
    let mut client = connect_to_shard_owner(&opts.address, shard).await?;
    client
        .delete_job(DeleteJobRequest {
            shard: shard.to_string(),
            id: id.to_string(),
            tenant: opts.tenant.clone(),
        })
        .await?;

    if opts.json {
        writeln!(out, r#"{{"status": "deleted", "job_id": "{}"}}"#, id)?;
    } else {
        writeln!(out, "Job {} deleted successfully", id)?;
    }

    Ok(())
}

/// Execute SQL query against a shard
pub async fn query<W: Write>(
    opts: &GlobalOptions,
    out: &mut W,
    shard: &str,
    sql: &str,
) -> anyhow::Result<()> {
    let mut client = connect_to_shard_owner(&opts.address, shard).await?;
    let response = client
        .query(QueryRequest {
            shard: shard.to_string(),
            sql: sql.to_string(),
            tenant: opts.tenant.clone(),
        })
        .await?
        .into_inner();

    if opts.json {
        // Decode msgpack rows to JSON
        let rows: Vec<serde_json::Value> = response
            .rows
            .iter()
            .filter_map(|row| {
                row.encoding.as_ref().and_then(|enc| match enc {
                    crate::pb::serialized_bytes::Encoding::Msgpack(data) => {
                        rmp_serde::from_slice::<serde_json::Value>(data).ok()
                    }
                })
            })
            .collect();

        let json_output = serde_json::json!({
            "columns": response.columns.iter().map(|c| {
                serde_json::json!({
                    "name": c.name,
                    "data_type": c.data_type,
                })
            }).collect::<Vec<_>>(),
            "row_count": response.row_count,
            "rows": rows,
        });
        writeln!(out, "{}", serde_json::to_string_pretty(&json_output)?)?;
    } else {
        if response.columns.is_empty() {
            writeln!(out, "Query returned no columns")?;
            return Ok(());
        }

        // Calculate column widths
        let col_names: Vec<&str> = response.columns.iter().map(|c| c.name.as_str()).collect();

        // Decode rows
        let decoded_rows: Vec<serde_json::Value> = response
            .rows
            .iter()
            .filter_map(|row| {
                row.encoding.as_ref().and_then(|enc| match enc {
                    crate::pb::serialized_bytes::Encoding::Msgpack(data) => {
                        rmp_serde::from_slice::<serde_json::Value>(data).ok()
                    }
                })
            })
            .collect();

        // Print header
        let header: String = col_names
            .iter()
            .map(|n| format!("{:>16}", n))
            .collect::<Vec<_>>()
            .join(" | ");
        writeln!(out, "{}", header)?;
        writeln!(out, "{}", "-".repeat(header.len()))?;

        // Print rows
        for row in &decoded_rows {
            if let Some(obj) = row.as_object() {
                let line: String = col_names
                    .iter()
                    .map(|name| {
                        let val = obj
                            .get(*name)
                            .map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                serde_json::Value::Null => "null".to_string(),
                                other => other.to_string(),
                            })
                            .unwrap_or_default();
                        format!("{:>16}", if val.len() > 16 { &val[..16] } else { &val })
                    })
                    .collect::<Vec<_>>()
                    .join(" | ");
                writeln!(out, "{}", line)?;
            }
        }

        writeln!(out)?;
        writeln!(out, "{} row(s) returned", response.row_count)?;
    }

    Ok(())
}

/// Capture a CPU profile from the connected Silo node
pub async fn profile<W: Write>(
    opts: &GlobalOptions,
    out: &mut W,
    duration: u32,
    frequency: u32,
    output_path: Option<String>,
) -> anyhow::Result<()> {
    let mut client = connect(&opts.address).await?;

    if !opts.json {
        writeln!(
            out,
            "Starting CPU profile for {} seconds at {}Hz...",
            duration, frequency
        )?;
        out.flush()?;
    }

    let response = client
        .cpu_profile(CpuProfileRequest {
            duration_seconds: duration,
            frequency,
        })
        .await?
        .into_inner();

    // Generate output filename with timestamp if not provided
    let output_file = output_path.unwrap_or_else(|| {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        format!("profile-{}.pb.gz", timestamp)
    });

    // Ensure output path ends with .pb.gz for gzip compressed pprof
    let output_file = if !output_file.ends_with(".pb.gz") && !output_file.ends_with(".pb") {
        format!("{}.pb.gz", output_file)
    } else {
        output_file
    };

    // Write profile data (gzip compressed if .gz extension)
    if output_file.ends_with(".gz") {
        let file = File::create(&output_file)?;
        let mut encoder = GzEncoder::new(file, Compression::default());
        std::io::Write::write_all(&mut encoder, &response.profile_data)?;
        encoder.finish()?;
    } else {
        std::fs::write(&output_file, &response.profile_data)?;
    }

    if opts.json {
        let json_output = serde_json::json!({
            "status": "completed",
            "output_file": output_file,
            "duration_seconds": response.duration_seconds,
            "samples": response.samples,
            "profile_bytes": response.profile_data.len(),
        });
        writeln!(out, "{}", serde_json::to_string_pretty(&json_output)?)?;
    } else {
        writeln!(out)?;
        writeln!(out, "Profile saved to: {}", output_file)?;
        writeln!(
            out,
            "Duration: {}s, Samples: {}, Size: {} bytes",
            response.duration_seconds,
            response.samples,
            response.profile_data.len()
        )?;
        writeln!(out)?;
        writeln!(out, "Analyze with:")?;

        let display_path = Path::new(&output_file)
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or(output_file.clone());
        writeln!(out, "  pprof -http=:8080 {}", display_path)?;
        writeln!(out, "  go tool pprof -http=:8080 {}", display_path)?;
    }

    Ok(())
}

/// Request a shard split operation
pub async fn shard_split<W: Write>(
    opts: &GlobalOptions,
    out: &mut W,
    shard: &str,
    split_point: Option<String>,
    auto: bool,
    wait: bool,
) -> anyhow::Result<()> {
    // Discover topology and connect to the shard's owner
    let mut discovery_client = connect(&opts.address).await?;
    let cluster_info = discovery_client
        .get_cluster_info(GetClusterInfoRequest {})
        .await?
        .into_inner();

    let shard_owner = cluster_info
        .shard_owners
        .iter()
        .find(|s| s.shard_id == shard)
        .ok_or_else(|| anyhow::anyhow!("shard '{}' not found in cluster", shard))?;

    // Connect to the shard's owner (reuse connection if same node)
    let mut client = if shard_owner.grpc_addr == cluster_info.this_grpc_addr {
        discovery_client
    } else {
        connect(&shard_owner.grpc_addr).await?
    };

    // Determine the split point
    let split_point = if let Some(point) = split_point {
        point
    } else if auto {
        // Compute midpoint of the shard's range
        let start = if shard_owner.range_start.is_empty() {
            "0".to_string()
        } else {
            shard_owner.range_start.clone()
        };
        let end = if shard_owner.range_end.is_empty() {
            "zzzzzzzz".to_string()
        } else {
            shard_owner.range_end.clone()
        };

        compute_midpoint(&start, &end).ok_or_else(|| {
            anyhow::anyhow!(
                "Cannot compute midpoint for range [{}, {})",
                shard_owner.range_start,
                shard_owner.range_end
            )
        })?
    } else {
        return Err(anyhow::anyhow!(
            "Either --at <split-point> or --auto must be specified"
        ));
    };

    if !opts.json {
        writeln!(
            out,
            "Requesting split of shard {} at '{}'...",
            shard, split_point
        )?;
        out.flush()?;
    }

    let response = client
        .request_split(RequestSplitRequest {
            shard_id: shard.to_string(),
            split_point: split_point.clone(),
        })
        .await?
        .into_inner();

    if opts.json {
        let json_output = serde_json::json!({
            "status": "split_requested",
            "parent_shard_id": shard,
            "split_point": split_point,
            "left_child_id": response.left_child_id,
            "right_child_id": response.right_child_id,
            "phase": response.phase,
        });
        writeln!(out, "{}", serde_json::to_string_pretty(&json_output)?)?;
    } else {
        writeln!(out, "Split requested successfully")?;
        writeln!(out, "  Parent shard:    {}", shard)?;
        writeln!(out, "  Split point:     {}", split_point)?;
        writeln!(out, "  Left child:      {}", response.left_child_id)?;
        writeln!(out, "  Right child:     {}", response.right_child_id)?;
        writeln!(out, "  Initial phase:   {}", response.phase)?;
    }

    // If --wait is specified, poll until split is complete
    if wait {
        if !opts.json {
            writeln!(out)?;
            writeln!(out, "Waiting for split to complete...")?;
        }

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            let status = client
                .get_split_status(GetSplitStatusRequest {
                    shard_id: shard.to_string(),
                })
                .await?
                .into_inner();

            if !status.in_progress {
                if !opts.json {
                    writeln!(out, "Split completed!")?;
                }
                break;
            }

            if !opts.json {
                write!(out, "  Phase: {}...\r", status.phase)?;
                out.flush()?;
            }
        }
    }

    Ok(())
}

/// Get the status of a shard split operation
pub async fn shard_split_status<W: Write>(
    opts: &GlobalOptions,
    out: &mut W,
    shard: &str,
) -> anyhow::Result<()> {
    let mut client = connect_to_shard_owner(&opts.address, shard).await?;

    let response = client
        .get_split_status(GetSplitStatusRequest {
            shard_id: shard.to_string(),
        })
        .await?
        .into_inner();

    if opts.json {
        let json_output = serde_json::json!({
            "shard_id": shard,
            "in_progress": response.in_progress,
            "phase": response.phase,
            "left_child_id": response.left_child_id,
            "right_child_id": response.right_child_id,
            "split_point": response.split_point,
            "initiator_node_id": response.initiator_node_id,
            "requested_at_ms": response.requested_at_ms,
        });
        writeln!(out, "{}", serde_json::to_string_pretty(&json_output)?)?;
    } else if response.in_progress {
        writeln!(out, "Split Status for Shard {}", shard)?;
        writeln!(out, "=========================")?;
        writeln!(out, "In Progress:      Yes")?;
        writeln!(out, "Phase:            {}", response.phase)?;
        writeln!(out, "Split Point:      {}", response.split_point)?;
        writeln!(out, "Left Child:       {}", response.left_child_id)?;
        writeln!(out, "Right Child:      {}", response.right_child_id)?;
        writeln!(out, "Initiator:        {}", response.initiator_node_id)?;
        writeln!(
            out,
            "Requested At:     {}",
            format_timestamp_ms(response.requested_at_ms)
        )?;
    } else {
        writeln!(out, "No split in progress for shard {}", shard)?;
    }

    Ok(())
}

/// Configure a shard's placement ring.
///
/// This command changes which placement ring a shard belongs to. After changing
/// the ring, the shard will be handed off to a node that participates in the
/// new ring.
pub async fn shard_configure<W: Write>(
    opts: &GlobalOptions,
    out: &mut W,
    shard: &str,
    ring: Option<String>,
) -> anyhow::Result<()> {
    let mut client = connect_to_shard_owner(&opts.address, shard).await?;

    let response = client
        .configure_shard(ConfigureShardRequest {
            tenant: opts.tenant.clone(),
            shard: shard.to_string(),
            placement_ring: ring.clone(),
        })
        .await?
        .into_inner();

    if opts.json {
        let json_output = serde_json::json!({
            "shard_id": shard,
            "previous_ring": response.previous_ring,
            "current_ring": response.current_ring,
        });
        writeln!(out, "{}", serde_json::to_string_pretty(&json_output)?)?;
    } else {
        let previous_display = if response.previous_ring.is_empty() {
            "(default)"
        } else {
            &response.previous_ring
        };
        let current_display = if response.current_ring.is_empty() {
            "(default)"
        } else {
            &response.current_ring
        };

        writeln!(out, "Shard {} configured", shard)?;
        writeln!(out, "Previous ring: {}", previous_display)?;
        writeln!(out, "Current ring:  {}", current_display)?;
    }

    Ok(())
}

/// Force-release a shard lease regardless of the current holder.
///
/// This is an operator escape hatch for recovering from permanently lost nodes.
/// After force-releasing, any live node that desires the shard can acquire it.
pub async fn shard_force_release<W: Write>(
    opts: &GlobalOptions,
    out: &mut W,
    shard: &str,
) -> anyhow::Result<()> {
    let mut client = connect_to_shard_owner(&opts.address, shard).await?;

    let response = client
        .force_release_shard(ForceReleaseShardRequest {
            shard: shard.to_string(),
        })
        .await?
        .into_inner();

    if opts.json {
        let json_output = serde_json::json!({
            "shard_id": shard,
            "released": response.released,
        });
        writeln!(out, "{}", serde_json::to_string_pretty(&json_output)?)?;
    } else {
        writeln!(out, "Force-released shard lease for {}", shard)?;
    }

    Ok(())
}

/// Validate a Silo configuration file
pub async fn validate_config<W: Write>(
    opts: &GlobalOptions,
    out: &mut W,
    config_path: &Path,
) -> anyhow::Result<()> {
    match crate::settings::AppConfig::load(Some(config_path)) {
        Ok(_cfg) => {
            if opts.json {
                writeln!(
                    out,
                    r#"{{"status": "valid", "config_path": "{}"}}"#,
                    config_path.display()
                )?;
            } else {
                writeln!(out, "Config is valid: {}", config_path.display())?;
            }
            Ok(())
        }
        Err(e) => {
            if opts.json {
                let json_output = serde_json::json!({
                    "status": "invalid",
                    "config_path": config_path.display().to_string(),
                    "error": e.to_string(),
                });
                writeln!(out, "{}", serde_json::to_string_pretty(&json_output)?)?;
            } else {
                writeln!(out, "Config error: {}", e)?;
            }
            Err(anyhow::anyhow!("Config validation failed: {}", e))
        }
    }
}

/// Compute the lexicographic midpoint of two strings
pub fn compute_midpoint(start: &str, end: &str) -> Option<String> {
    if start >= end {
        return None;
    }

    let start_bytes = start.as_bytes();
    let end_bytes = end.as_bytes();

    // Find the first differing position
    let min_len = start_bytes.len().min(end_bytes.len());
    let mut diff_pos = 0;
    while diff_pos < min_len && start_bytes[diff_pos] == end_bytes[diff_pos] {
        diff_pos += 1;
    }

    if diff_pos == min_len {
        // One is a prefix of the other
        if start_bytes.len() < end_bytes.len() {
            // Start is shorter, add a middle character
            let mut mid = start.to_string();
            mid.push('M');
            if mid.as_str() > start && mid.as_str() < end {
                return Some(mid);
            }
        }
        return None;
    }

    // Compute midpoint at the differing position
    let start_char = start_bytes[diff_pos];
    let end_char = end_bytes[diff_pos];

    if end_char <= start_char + 1 {
        // Too close, extend with a high character
        let mut mid = String::from_utf8_lossy(&start_bytes[..=diff_pos]).to_string();
        mid.push('~');
        if mid.as_str() > start && mid.as_str() < end {
            return Some(mid);
        }
        return None;
    }

    let mid_char = start_char + (end_char - start_char) / 2;
    let mut result = String::from_utf8_lossy(&start_bytes[..diff_pos]).to_string();
    result.push(mid_char as char);

    if result.as_str() > start && result.as_str() < end {
        Some(result)
    } else {
        None
    }
}
