//! Silo administration CLI tool.
//!
//! `siloctl` provides commands for administrators to interact with production
//! Silo clusters, including cluster inspection, job management, and SQL querying.

use std::io;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use silo::siloctl::{self, GlobalOptions};

#[derive(Parser, Debug)]
#[command(name = "siloctl")]
#[command(about = "Administration CLI for Silo job queue clusters")]
#[command(version)]
struct Args {
    /// Silo server address (e.g., http://localhost:50051)
    #[arg(long, short = 'a', default_value = "http://localhost:50051", global = true)]
    address: String,

    /// Tenant ID for multi-tenant clusters
    #[arg(long, short = 't', global = true)]
    tenant: Option<String>,

    /// Output in JSON format instead of human-readable tables
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

impl Args {
    fn to_global_options(&self) -> GlobalOptions {
        GlobalOptions {
            address: self.address.clone(),
            tenant: self.tenant.clone(),
            json: self.json,
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
    /// Execute SQL query against a shard
    Query {
        /// Shard ID to query
        shard: u32,
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
}

#[derive(Subcommand, Debug)]
enum ClusterAction {
    /// Show cluster topology and shard ownership
    Info,
}

#[derive(Subcommand, Debug)]
enum JobAction {
    /// Get job details
    Get {
        /// Shard ID where the job is stored
        shard: u32,
        /// Job ID
        id: String,
        /// Include attempt history
        #[arg(long)]
        attempts: bool,
    },
    /// Cancel a job
    Cancel {
        /// Shard ID where the job is stored
        shard: u32,
        /// Job ID
        id: String,
    },
    /// Restart a cancelled or failed job
    Restart {
        /// Shard ID where the job is stored
        shard: u32,
        /// Job ID
        id: String,
    },
    /// Expedite a scheduled job to run immediately
    Expedite {
        /// Shard ID where the job is stored
        shard: u32,
        /// Job ID
        id: String,
    },
    /// Delete a job permanently
    Delete {
        /// Shard ID where the job is stored
        shard: u32,
        /// Job ID
        id: String,
    },
}

async fn run(args: Args) -> anyhow::Result<()> {
    let opts = args.to_global_options();
    let mut stdout = io::stdout();

    match &args.command {
        Command::Cluster { action } => match action {
            ClusterAction::Info => siloctl::cluster_info(&opts, &mut stdout).await,
        },
        Command::Job { action } => match action {
            JobAction::Get { shard, id, attempts } => {
                siloctl::job_get(&opts, &mut stdout, *shard, id, *attempts).await
            }
            JobAction::Cancel { shard, id } => {
                siloctl::job_cancel(&opts, &mut stdout, *shard, id).await
            }
            JobAction::Restart { shard, id } => {
                siloctl::job_restart(&opts, &mut stdout, *shard, id).await
            }
            JobAction::Expedite { shard, id } => {
                siloctl::job_expedite(&opts, &mut stdout, *shard, id).await
            }
            JobAction::Delete { shard, id } => {
                siloctl::job_delete(&opts, &mut stdout, *shard, id).await
            }
        },
        Command::Query { shard, sql } => siloctl::query(&opts, &mut stdout, *shard, sql).await,
        Command::Profile {
            duration,
            frequency,
            output,
        } => siloctl::profile(&opts, &mut stdout, *duration, *frequency, output.clone()).await,
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
