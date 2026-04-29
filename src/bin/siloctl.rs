//! Silo administration CLI tool.
//!
//! `siloctl` provides commands for administrators to interact with production
//! Silo clusters, including cluster inspection, job management, and SQL querying.

use std::io;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use silo::siloctl::{self, BytesOutputFormat, GlobalOptions};

#[derive(Parser, Debug)]
#[command(name = "siloctl")]
#[command(about = "Administration CLI for Silo job queue clusters")]
#[command(version)]
struct Args {
    /// Silo server address (e.g., http://localhost:7450)
    #[arg(
        long,
        short = 'a',
        default_value = "http://localhost:7450",
        global = true
    )]
    address: String,

    /// Tenant ID for multi-tenant clusters
    #[arg(long, short = 't', global = true)]
    tenant: Option<String>,

    /// Output in JSON format instead of human-readable tables
    #[arg(long, global = true)]
    json: bool,

    /// Bearer token for gRPC authentication.
    /// Can also be set via SILO_AUTH_TOKEN environment variable.
    #[arg(long, global = true, env = "SILO_AUTH_TOKEN")]
    auth_token: Option<String>,

    #[command(subcommand)]
    command: Command,
}

impl Args {
    fn to_global_options(&self) -> GlobalOptions {
        GlobalOptions {
            address: self.address.clone(),
            tenant: self.tenant.clone(),
            json: self.json,
            auth_token: self.auth_token.clone(),
        }
    }
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Cluster operations
    Cluster {
        #[command(subcommand)]
        action: ClusterAction,
    },
    /// Job operations
    Job {
        #[command(subcommand)]
        action: JobAction,
    },
    /// Shard operations
    Shard {
        #[command(subcommand)]
        action: ShardAction,
    },
    /// Tenant operations
    Tenant {
        #[command(subcommand)]
        action: TenantAction,
    },
    /// Execute SQL query against a shard
    Query {
        /// Shard ID (UUID) to query
        shard: String,
        /// SQL query string
        sql: String,
    },
    /// Capture a CPU profile from a Silo node
    Profile {
        /// Duration in seconds (1-300)
        #[arg(long, short = 'd', default_value = "30")]
        duration: u32,
        /// Sampling frequency in Hz (1-1000)
        #[arg(long, short = 'f', default_value = "100")]
        frequency: u32,
        /// Output file path (default: profile-{timestamp}.pb.gz)
        #[arg(long, short = 'o')]
        output: Option<String>,
    },
    /// Capture a jemalloc heap profile from a Silo node
    HeapProfile {
        /// Duration in seconds to keep heap profiling active before dumping (1-300)
        #[arg(long, short = 'd', default_value = "30")]
        duration: u32,
        /// Output file path (default: heap-profile-{timestamp}.heap)
        #[arg(long, short = 'o')]
        output: Option<String>,
    },
    /// Validate a config file and exit
    ValidateConfig {
        /// Path to the TOML config file to validate
        #[arg(short = 'c', long = "config")]
        config: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
enum ClusterAction {
    /// Show cluster topology and shard ownership
    Info,
}

#[derive(Subcommand, Debug)]
enum ShardAction {
    /// Split a shard into two at a specified tenant ID
    Split {
        /// Shard ID (UUID) to split
        shard: String,
        /// Tenant ID at which to split the keyspace
        #[arg(long)]
        at: Option<String>,
        /// Automatically compute the midpoint of the shard's range
        #[arg(long, conflicts_with = "at")]
        auto: bool,
        /// Wait for split to complete
        #[arg(long)]
        wait: bool,
    },
    /// Check the status of a split operation
    SplitStatus {
        /// Parent shard ID (UUID) to check
        shard: String,
    },
    /// Configure a shard's placement ring
    Configure {
        /// Shard ID (UUID) to configure
        shard: String,
        /// Placement ring name (omit to move to default ring)
        #[arg(long)]
        ring: Option<String>,
    },
    /// Force-release a shard lease (for operator recovery when a node is permanently lost)
    ForceRelease {
        /// Shard ID (UUID) to force-release
        shard: String,
    },
    /// Trigger a full compaction on a shard to remove tombstones and reclaim space
    Compact {
        /// Shard ID (UUID) to compact
        shard: String,
    },
}

#[derive(Subcommand, Debug)]
enum TenantAction {
    /// Show which shard a tenant belongs to
    Locate {
        /// Tenant ID to look up
        tenant_id: String,
    },
    /// Compute the xxhash for a tenant ID
    Hash {
        /// Tenant ID to hash
        tenant_id: String,
    },
}

#[derive(Subcommand, Debug)]
enum JobAction {
    /// Get job details
    Get {
        /// Shard ID (UUID) where the job is stored
        shard: String,
        /// Job ID
        id: String,
        /// Include attempt history
        #[arg(long)]
        attempts: bool,
    },
    /// Get the result or error data of a completed job
    Result {
        /// Shard ID (UUID) where the job is stored
        shard: String,
        /// Job ID
        id: String,
        /// Output format for the result bytes
        #[arg(long, short = 'f', default_value = "hex")]
        format: OutputFormat,
    },
    /// Cancel a job
    Cancel {
        /// Shard ID (UUID) where the job is stored
        shard: String,
        /// Job ID
        id: String,
    },
    /// Restart a cancelled or failed job
    Restart {
        /// Shard ID (UUID) where the job is stored
        shard: String,
        /// Job ID
        id: String,
    },
    /// Expedite a scheduled job to run immediately
    Expedite {
        /// Shard ID (UUID) where the job is stored
        shard: String,
        /// Job ID
        id: String,
    },
    /// Delete a job permanently
    Delete {
        /// Shard ID (UUID) where the job is stored
        shard: String,
        /// Job ID
        id: String,
    },
}

#[derive(ValueEnum, Debug, Clone, Copy)]
enum OutputFormat {
    /// Hex-encoded bytes
    Hex,
    /// Base64-encoded bytes
    Base64,
    /// Decode msgpack and pretty-print as JSON
    Msgpack,
}

impl OutputFormat {
    fn to_bytes_output_format(self) -> BytesOutputFormat {
        match self {
            OutputFormat::Hex => BytesOutputFormat::Hex,
            OutputFormat::Base64 => BytesOutputFormat::Base64,
            OutputFormat::Msgpack => BytesOutputFormat::MsgpackJson,
        }
    }
}

async fn run(args: Args) -> anyhow::Result<()> {
    let opts = args.to_global_options();
    let mut stdout = io::stdout();

    match &args.command {
        Command::Cluster { action } => match action {
            ClusterAction::Info => siloctl::cluster_info(&opts, &mut stdout).await,
        },
        Command::Job { action } => match action {
            JobAction::Get {
                shard,
                id,
                attempts,
            } => siloctl::job_get(&opts, &mut stdout, shard, id, *attempts).await,
            JobAction::Result { shard, id, format } => {
                siloctl::job_result(
                    &opts,
                    &mut stdout,
                    shard,
                    id,
                    format.to_bytes_output_format(),
                )
                .await
            }
            JobAction::Cancel { shard, id } => {
                siloctl::job_cancel(&opts, &mut stdout, shard, id).await
            }
            JobAction::Restart { shard, id } => {
                siloctl::job_restart(&opts, &mut stdout, shard, id).await
            }
            JobAction::Expedite { shard, id } => {
                siloctl::job_expedite(&opts, &mut stdout, shard, id).await
            }
            JobAction::Delete { shard, id } => {
                siloctl::job_delete(&opts, &mut stdout, shard, id).await
            }
        },
        Command::Shard { action } => match action {
            ShardAction::Split {
                shard,
                at,
                auto,
                wait,
            } => siloctl::shard_split(&opts, &mut stdout, shard, at.clone(), *auto, *wait).await,
            ShardAction::SplitStatus { shard } => {
                siloctl::shard_split_status(&opts, &mut stdout, shard).await
            }
            ShardAction::Configure { shard, ring } => {
                siloctl::shard_configure(&opts, &mut stdout, shard, ring.clone()).await
            }
            ShardAction::ForceRelease { shard } => {
                siloctl::shard_force_release(&opts, &mut stdout, shard).await
            }
            ShardAction::Compact { shard } => {
                siloctl::shard_compact(&opts, &mut stdout, shard).await
            }
        },
        Command::Tenant { action } => match action {
            TenantAction::Locate { tenant_id } => {
                siloctl::tenant_locate(&opts, &mut stdout, tenant_id).await
            }
            TenantAction::Hash { tenant_id } => siloctl::tenant_hash(&opts, &mut stdout, tenant_id),
        },
        Command::Query { shard, sql } => siloctl::query(&opts, &mut stdout, shard, sql).await,
        Command::Profile {
            duration,
            frequency,
            output,
        } => siloctl::profile(&opts, &mut stdout, *duration, *frequency, output.clone()).await,
        Command::HeapProfile { duration, output } => {
            siloctl::heap_profile(&opts, &mut stdout, *duration, output.clone()).await
        }
        Command::ValidateConfig { config } => {
            siloctl::validate_config(&opts, &mut stdout, config).await
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();

    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {}", e);
            ExitCode::FAILURE
        }
    }
}
