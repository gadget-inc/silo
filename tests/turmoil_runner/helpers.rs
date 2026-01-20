//! DST Infrastructure - shared helpers for turmoil simulation testing.
//!
//! This module provides the foundational infrastructure for deterministic
//! simulation testing (DST) using turmoil and mad-turmoil.

use std::io::Write;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use core::future::Future;
use hyper::Uri;
use hyper_util::rt::TokioIo;
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::server::run_server_with_incoming;
use silo::settings::{AppConfig, Backend, GubernatorSettings, LoggingConfig, WebUiConfig};
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tower::Service;
use tracing_subscriber::prelude::*;
use turmoil::net::TcpListener;

// Re-export common types needed by scenarios
pub use silo::pb::{
    ConcurrencyLimit, EnqueueRequest, LeaseTasksRequest, Limit, MsgpackBytes, ReportOutcomeRequest,
    RetryPolicy, limit, report_outcome_request,
};
pub use std::collections::HashMap;
pub use turmoil;

/// Get the DST seed from environment, or use a default.
pub fn get_seed() -> u64 {
    std::env::var("DST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(42)
}

/// A writer for deterministic tracing output.
/// Writes directly to stdout so all trace logs are captured between DST markers.
struct DstWriter;

impl Write for DstWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        std::io::stdout().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stdout().flush()
    }
}

/// Initialize deterministic tracing for DST.
/// Configures tracing to output all logs (including TRACE level) in a deterministic format:
/// - No ANSI colors (consistent output)
/// - Thread names/IDs disabled (ordering could vary)
/// - No file/line numbers (could change with code edits)
fn init_deterministic_tracing() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(true)
        .with_level(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .compact()
        .with_writer(|| DstWriter);

    let filter = tracing_subscriber::filter::LevelFilter::TRACE;

    tracing_subscriber::registry()
        .with(fmt_layer.with_filter(filter))
        .init();
}

/// Initialize mad-turmoil for deterministic simulation testing.
/// This should only be called once per process - each DST scenario
/// runs in its own subprocess for true determinism.
fn init_deterministic_sim(seed: u64) -> mad_turmoil::time::SimClocksGuard {
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    // Initialize deterministic tracing first
    init_deterministic_tracing();

    // Set the seeded RNG for mad-turmoil (intercepts getrandom/getentropy at libc level)
    let rng = StdRng::seed_from_u64(seed);
    mad_turmoil::rand::set_rng(rng);

    // Seed fastrand which is used by transitive dependencies like slatedb, tempfile
    fastrand::seed(seed);

    // Enable simulated clocks (intercepts clock_gettime at libc level)
    mad_turmoil::time::SimClocksGuard::init()
}

// Turmoil connector for tonic clients
type Fut =
    Pin<Box<dyn Future<Output = Result<TokioIo<turmoil::net::TcpStream>, std::io::Error>> + Send>>;

pub fn turmoil_connector()
-> impl Service<Uri, Response = TokioIo<turmoil::net::TcpStream>, Error = std::io::Error, Future = Fut>
+ Clone {
    tower::service_fn(|uri: Uri| {
        Box::pin(async move {
            let conn = turmoil::net::TcpStream::connect(uri.authority().unwrap().as_str()).await?;
            Ok::<_, std::io::Error>(TokioIo::new(conn))
        }) as Fut
    })
}

/// Helper to create a standard server host for tests
pub async fn setup_server(port: u16) -> turmoil::Result<()> {
    let cfg = AppConfig {
        server: silo::settings::ServerConfig {
            grpc_addr: format!("0.0.0.0:{}", port),
            dev_mode: false,
        },
        coordination: silo::settings::CoordinationConfig::default(),
        tenancy: silo::settings::TenancyConfig { enabled: false },
        gubernator: GubernatorSettings::default(),
        webui: WebUiConfig::default(),
        logging: LoggingConfig::default(),
        metrics: silo::settings::MetricsConfig::default(),
        database: silo::settings::DatabaseTemplate {
            backend: Backend::Memory,
            path: "mem://shard-{shard}".to_string(),
            wal: None,
            apply_wal_on_close: true,
        },
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let factory = ShardFactory::new(cfg.database.clone(), rate_limiter, None);
    let _ = factory.open(0).await.map_err(|e| e.to_string())?;
    let factory = Arc::new(factory);

    let addr = (IpAddr::from(Ipv4Addr::UNSPECIFIED), port);
    let listener = TcpListener::bind(addr).await.map_err(|e| e.to_string())?;

    struct Accepted(turmoil::net::TcpStream);
    impl tonic::transport::server::Connected for Accepted {
        type ConnectInfo = tonic::transport::server::TcpConnectInfo;
        fn connect_info(&self) -> Self::ConnectInfo {
            Self::ConnectInfo {
                local_addr: self.0.local_addr().ok(),
                remote_addr: self.0.peer_addr().ok(),
            }
        }
    }
    impl AsyncRead for Accepted {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }
    impl AsyncWrite for Accepted {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
            std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::pin::Pin::new(&mut self.0).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }

    let incoming = async_stream::stream! {
        loop {
            let (s, _a) = listener.accept().await.unwrap();
            yield Ok::<_, std::io::Error>(Accepted(s));
        }
    };

    let (_tx, rx) = tokio::sync::broadcast::channel::<()>(1);
    run_server_with_incoming(
        incoming,
        factory,
        None,
        silo::settings::AppConfig::load(None).unwrap(),
        None,
        rx,
    )
    .await
    .map_err(|e| e.to_string())?;

    Ok(())
}

/// Run a scenario and output deterministic markers
pub fn run_scenario_impl<F>(name: &str, seed: u64, duration_secs: u64, setup: F)
where
    F: FnOnce(&mut turmoil::Sim<'_>),
{
    // Output metadata before tracing is initialized
    println!("DST_SCENARIO:{}", name);
    println!("DST_SEED:{}", seed);
    println!("---DST_BEGIN---");

    let _guard = init_deterministic_sim(seed);

    tracing::info!(scenario = name, seed = seed, "starting DST scenario");

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(duration_secs))
        .tick_duration(Duration::from_millis(1))
        .rng_seed(seed)
        .build();

    setup(&mut sim);

    match sim.run() {
        Ok(()) => {
            tracing::info!(scenario = name, "scenario completed successfully");
            println!("---DST_END---");
            println!("DST_RESULT:success");
        }
        Err(e) => {
            tracing::error!(scenario = name, error = %e, "scenario failed");
            println!("---DST_END---");
            println!("DST_RESULT:error:{}", e);
        }
    }
}

// ============================================================================
// Test Entry Point Helpers
// ============================================================================

/// Helper to run a scenario in a subprocess and verify success
pub fn run_in_subprocess(scenario: &str, seed: u64) -> std::process::Output {
    use std::process::Command;

    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());

    Command::new(&cargo)
        .args([
            "test",
            "--test",
            "turmoil_runner",
            scenario,
            "--",
            "--nocapture",
            "--exact",
        ])
        .env("DST_SEED", seed.to_string())
        .env("RUST_BACKTRACE", "0")
        .env("DST_SUBPROCESS", "1") // Marker that we're in subprocess
        .output()
        .expect("Failed to run test subprocess")
}

/// Check if we're running as a subprocess (direct scenario execution)
pub fn is_subprocess() -> bool {
    std::env::var("DST_SUBPROCESS").is_ok()
}

/// Check if we're running in fuzz mode (single run, no determinism verification)
pub fn is_fuzz_mode() -> bool {
    std::env::var("DST_FUZZ").is_ok()
}

/// Extract deterministic output from subprocess.
/// Extracts all lines between the DST_BEGIN and DST_END markers, including
/// all system trace logs from silo. This provides comprehensive determinism validation.
fn extract_dst_output(output: &[u8]) -> String {
    let full = String::from_utf8_lossy(output);
    let start = full.find("---DST_BEGIN---");
    let end = full.find("---DST_END---");

    match (start, end) {
        (Some(s), Some(e)) => {
            // Skip past the BEGIN marker line itself
            let after_begin = s + "---DST_BEGIN---".len();
            let section = &full[after_begin..e];
            // Trim leading/trailing whitespace from the section
            section.trim().to_string()
        }
        _ => panic!("Could not find DST markers in:\n{}", full),
    }
}

/// Verify that a scenario is deterministic by running it twice.
/// Compares the trace output byte-for-byte to ensure complete determinism.
pub fn verify_determinism(scenario: &str, seed: u64) {
    eprintln!("Verifying determinism: {} with seed {}", scenario, seed);

    let output1 = run_in_subprocess(scenario, seed);
    let output2 = run_in_subprocess(scenario, seed);

    assert!(
        output1.status.success(),
        "First run failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output1.stdout),
        String::from_utf8_lossy(&output1.stderr)
    );
    assert!(
        output2.status.success(),
        "Second run failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output2.stdout),
        String::from_utf8_lossy(&output2.stderr)
    );

    let det1 = extract_dst_output(&output1.stdout);
    let det2 = extract_dst_output(&output2.stdout);

    // Byte-for-byte comparison
    if det1 != det2 {
        let lines1: Vec<&str> = det1.lines().collect();
        let lines2: Vec<&str> = det2.lines().collect();

        eprintln!("=== DETERMINISM FAILURE ===");
        eprintln!("Run 1: {} bytes, {} lines", det1.len(), lines1.len());
        eprintln!("Run 2: {} bytes, {} lines", det2.len(), lines2.len());

        // Find first difference
        let min_lines = lines1.len().min(lines2.len());
        for i in 0..min_lines {
            if lines1[i] != lines2[i] {
                eprintln!("\n=== First difference at line {} ===", i + 1);
                eprintln!("Run 1: {:?}", lines1[i]);
                eprintln!("Run 2: {:?}", lines2[i]);

                // Show context around the difference
                let context_start = i.saturating_sub(3);
                let context_end = (i + 4).min(min_lines);

                eprintln!(
                    "\n=== Context (lines {}-{}) ===",
                    context_start + 1,
                    context_end
                );
                eprintln!("--- Run 1 ---");
                for j in context_start..context_end {
                    let marker = if j == i { ">>>" } else { "   " };
                    eprintln!(
                        "{} {}: {}",
                        marker,
                        j + 1,
                        lines1.get(j).unwrap_or(&"<missing>")
                    );
                }
                eprintln!("--- Run 2 ---");
                for j in context_start..context_end {
                    let marker = if j == i { ">>>" } else { "   " };
                    eprintln!(
                        "{} {}: {}",
                        marker,
                        j + 1,
                        lines2.get(j).unwrap_or(&"<missing>")
                    );
                }
                break;
            }
        }

        if lines1.len() != lines2.len() {
            eprintln!("\n=== Line count mismatch ===");
            eprintln!(
                "Run 1 has {} lines, Run 2 has {} lines",
                lines1.len(),
                lines2.len()
            );
            if lines1.len() > lines2.len() {
                eprintln!("Extra lines in Run 1:");
                for line in &lines1[min_lines..] {
                    eprintln!("  {}", line);
                }
            } else {
                eprintln!("Extra lines in Run 2:");
                for line in &lines2[min_lines..] {
                    eprintln!("  {}", line);
                }
            }
        }

        panic!("Trace output is not byte-for-byte identical! See above for details.");
    }

    let line_count = det1.lines().count();
    eprintln!(
        "✓ Determinism verified: {} bytes, {} lines are byte-for-byte identical",
        det1.len(),
        line_count
    );
}
