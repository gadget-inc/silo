use clap::Parser;
use std::path::PathBuf;

mod settings;
mod storage;

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

    // Initialize all configured SlateDB instances (no globals)
    let mut db_factory = storage::DbFactory::new();
    for db in &cfg.databases {
        let _handle = db_factory.open(db).await?;
        if args.verbose {
            println!("opened db '{}' at '{}' via '{}'", db.name, db.path, db.backend);
        }
    }

    Ok(())
}
