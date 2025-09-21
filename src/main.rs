use clap::Parser;
use std::path::PathBuf;

use silo::factory::ShardFactory;
use silo::settings;

#[derive(Parser, Debug)]
#[clap(author = "Harry Brundage", version, about)]
/// Application CLI arguments
struct Args {
    /// whether to be verbose
    #[arg(short = 'v')]
    verbose: bool,

    /// path to a TOML config file
    #[arg(short = 'c', long = "config")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.verbose {
        println!("DEBUG {args:?}");
    }

    // Load configuration
    let cfg = settings::AppConfig::load(args.config.as_deref())?;

    // Initialize all configured Shard instances (no globals)
    let mut shard_factory = ShardFactory::new();
    for db in &cfg.databases {
        let _handle = shard_factory.open(db).await?;
        if args.verbose {
            println!(
                "opened shard '{}' at '{}' via '{:?}'",
                db.name, db.path, db.backend
            );
        }
    }

    Ok(())
}
