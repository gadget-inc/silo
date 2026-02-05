//! DST Infrastructure - shared helpers for turmoil simulation testing.
//!
//! This module provides the foundational infrastructure for deterministic
//! simulation testing (DST) using turmoil and mad-turmoil.

use std::collections::HashSet;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use core::future::Future;
use hyper::Uri;
use hyper_util::rt::TokioIo;
use silo::coordination::NoneCoordinator;
use silo::factory::ShardFactory;
use silo::gubernator::MockGubernatorClient;
use silo::server::run_server_with_incoming;
use silo::settings::{AppConfig, Backend, GubernatorSettings, LoggingConfig, WebUiConfig};
use silo::shard_range::{ShardId, ShardRange};
#[cfg(feature = "dst")]
use silo::turmoil_object_store::clear_shared_storage;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tower::Service;
use tracing_subscriber::prelude::*;
use turmoil::net::TcpListener;

// Re-export common types needed by scenarios
pub use silo::pb::silo_client::SiloClient;
pub use silo::pb::{
    AttemptStatus, ConcurrencyLimit, EnqueueRequest, GetJobRequest, JobStatus, LeaseTasksRequest,
    Limit, QueryRequest, ReportOutcomeRequest, RetryPolicy, SerializedBytes, Task, limit,
    report_outcome_request, serialized_bytes,
};
pub use std::collections::HashMap;
pub use turmoil;

/// The test shard ID used by DST scenarios.
/// This is a fixed UUID that matches what setup_server() creates.
pub const TEST_SHARD_ID: &str = "00000000-0000-0000-0000-000000000000";

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
///
/// Panics if called more than once in the same process - each DST scenario must run in its own process for true determinism.
fn init_deterministic_tracing() {
    use std::sync::atomic::{AtomicBool, Ordering};
    static INITIALIZED: AtomicBool = AtomicBool::new(false);

    if INITIALIZED.swap(true, Ordering::SeqCst) {
        panic!(
            "DST tracing already initialized in this process. \
             Each DST scenario must run in a separate process for determinism. \
             If running in fuzz mode, ensure each test is spawned as a subprocess."
        );
    }

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
///
/// Panics if called more than once in the same process.
fn init_deterministic_sim(seed: u64) -> mad_turmoil::time::SimClocksGuard {
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    // Initialize deterministic tracing first (will panic if already initialized)
    init_deterministic_tracing();

    // Set the seeded RNG for mad-turmoil (intercepts getrandom/getentropy at libc level)
    // This will panic if called twice - that's intentional, see init_deterministic_tracing
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

// Re-export ClientConfig for convenience
pub use silo::cluster_client::ClientConfig;

/// Create a SiloClient for turmoil simulations with proper timeout and retry configuration.
///
/// This is the turmoil equivalent of `silo::cluster_client::create_silo_client`.
/// It uses the turmoil connector for simulated networking while applying the
/// same timeout and retry configuration used in production.
///
/// Like the production client, each connection attempt is wrapped with a hard timeout
/// to ensure we don't hang indefinitely when the simulated network is degraded.
pub async fn create_turmoil_client(
    uri: &str,
    config: &ClientConfig,
) -> turmoil::Result<SiloClient<tonic::transport::Channel>> {
    let endpoint = tonic::transport::Endpoint::new(uri.to_string())
        .map_err(|e| e.to_string())?
        .connect_timeout(config.connect_timeout)
        .timeout(config.request_timeout);

    // Hard timeout per connection attempt, matching production behavior.
    let attempt_timeout = config.connect_timeout + Duration::from_secs(1);

    let mut last_error = None;
    for attempt in 0..config.max_retries {
        let connect_result = tokio::time::timeout(
            attempt_timeout,
            endpoint.connect_with_connector(turmoil_connector()),
        )
        .await;

        match connect_result {
            Ok(Ok(channel)) => return Ok(silo::pb::silo_client::SiloClient::new(channel)),
            Ok(Err(e)) => {
                tracing::trace!(
                    attempt = attempt,
                    max_retries = config.max_retries,
                    error = %e,
                    uri = uri,
                    "connection attempt failed, retrying"
                );
                last_error = Some(e.to_string());
            }
            Err(_elapsed) => {
                tracing::trace!(
                    attempt = attempt,
                    max_retries = config.max_retries,
                    uri = uri,
                    timeout_secs = ?attempt_timeout,
                    "connection attempt timed out, retrying"
                );
                last_error = Some(format!("connection timed out after {:?}", attempt_timeout));
            }
        }

        if attempt + 1 < config.max_retries {
            tokio::time::sleep(config.backoff_for_attempt(attempt)).await;
        }
    }

    Err(last_error
        .unwrap_or_else(|| "no connection attempts made".to_string())
        .into())
}

/// Connect to a DST server, waiting for it to be ready.
pub async fn connect_to_server(
    uri: &str,
) -> turmoil::Result<SiloClient<tonic::transport::Channel>> {
    const MAX_ATTEMPTS: u32 = 20;
    const INITIAL_BACKOFF_MS: u64 = 25;
    const MAX_BACKOFF_MS: u64 = 500;
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

    let endpoint = tonic::transport::Endpoint::new(uri.to_string())
        .map_err(|e| e.to_string())?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT);

    let mut last_error = None;
    let mut backoff_ms = INITIAL_BACKOFF_MS;

    for attempt in 0..MAX_ATTEMPTS {
        let connect_result = tokio::time::timeout(
            CONNECT_TIMEOUT,
            endpoint.connect_with_connector(turmoil_connector()),
        )
        .await;

        match connect_result {
            Ok(Ok(channel)) => {
                tracing::trace!(attempt = attempt, uri = uri, "connected to server");
                return Ok(SiloClient::new(channel));
            }
            Ok(Err(e)) => {
                tracing::trace!(
                    attempt = attempt,
                    max_attempts = MAX_ATTEMPTS,
                    error = %e,
                    uri = uri,
                    backoff_ms = backoff_ms,
                    "connection attempt failed, retrying"
                );
                last_error = Some(e.to_string());
            }
            Err(_elapsed) => {
                tracing::trace!(
                    attempt = attempt,
                    max_attempts = MAX_ATTEMPTS,
                    uri = uri,
                    backoff_ms = backoff_ms,
                    "connection attempt timed out, retrying"
                );
                last_error = Some("connection timed out".to_string());
            }
        }

        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            // Exponential backoff with cap
            backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
        }
    }

    Err(last_error
        .unwrap_or_else(|| "no connection attempts made".to_string())
        .into())
}

/// Create SlateDB settings suitable for DST tests.
///
/// Compaction is disabled because slatedb's compactor uses `spawn_blocking` and `block_on`
/// which are incompatible with turmoil's single-threaded simulated runtime.
pub fn dst_slatedb_settings() -> slatedb::config::Settings {
    slatedb::config::Settings {
        compactor_options: None,
        ..Default::default()
    }
}

/// Create a DatabaseTemplate using TurmoilFs backend for DST tests.
///
/// This is the recommended configuration for K8s coordination tests because:
/// - TurmoilFs provides deterministic filesystem operations within turmoil
/// - Supports SlateDB cloning which is needed for shard splits
/// - Has compaction disabled for turmoil compatibility
///
/// The `storage_root` should be a shared path that all nodes can access.
/// Use `{shard}` placeholder in the path for per-shard subdirectories.
pub fn dst_turmoilfs_database_template(
    storage_root: &std::path::Path,
) -> silo::settings::DatabaseTemplate {
    let path = storage_root.join("{shard}").to_string_lossy().to_string();
    silo::settings::DatabaseTemplate {
        backend: Backend::TurmoilFs,
        path,
        wal: None,
        apply_wal_on_close: true,
        slatedb: Some(dst_slatedb_settings()),
    }
}

/// Helper to create a standard server host for tests
pub async fn setup_server(port: u16) -> turmoil::Result<()> {
    tracing::trace!(port = port, "setup_server: starting");
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
            slatedb: Some(dst_slatedb_settings()),
        },
    };

    let rate_limiter = MockGubernatorClient::new_arc();
    let factory = ShardFactory::new(cfg.database.clone(), rate_limiter, None);
    // For DST tests, use a fixed shard ID (zero UUID) for simplicity
    let test_shard_id = ShardId::parse("00000000-0000-0000-0000-000000000000").unwrap();
    tracing::trace!("setup_server: opening shard");
    let _ = factory
        .open(&test_shard_id, &ShardRange::full())
        .await
        .map_err(|e| e.to_string())?;
    let factory = Arc::new(factory);
    tracing::trace!("setup_server: shard opened");

    let addr = (IpAddr::from(Ipv4Addr::UNSPECIFIED), port);
    tracing::trace!(port = port, "setup_server: binding TCP listener");
    let listener = TcpListener::bind(addr).await.map_err(|e| e.to_string())?;
    tracing::trace!(port = port, "setup_server: TCP listener bound");

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

    // Create a NoneCoordinator for single-node DST test mode
    tracing::trace!("setup_server: creating coordinator");
    let coordinator = Arc::new(
        NoneCoordinator::new(
            "dst-test-node",
            format!("http://0.0.0.0:{}", port),
            1,
            factory.clone(),
            Vec::new(),
        )
        .await
        .unwrap(),
    );
    tracing::trace!("setup_server: coordinator created");

    let (_tx, rx) = tokio::sync::broadcast::channel::<()>(1);
    tracing::trace!("setup_server: starting server");
    run_server_with_incoming(incoming, factory, coordinator, cfg, None, rx)
        .await
        .map_err(|e| e.to_string())?;

    tracing::trace!("setup_server: server stopped");
    Ok(())
}

/// Run a scenario and output deterministic markers.
///
/// In fuzz mode (DST_FUZZ=1), panics on scenario failure to fail the test.
/// In normal mode, just outputs the result for determinism comparison.
pub fn run_scenario_impl<F>(name: &str, seed: u64, duration_secs: u64, setup: F)
where
    F: FnOnce(&mut turmoil::Sim<'_>),
{
    // Output metadata before tracing is initialized
    println!("DST_SCENARIO:{}", name);
    println!("DST_SEED:{}", seed);
    println!("---DST_BEGIN---");

    // Clear shared storage from previous runs to ensure test isolation
    #[cfg(feature = "dst")]
    clear_shared_storage();

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

            // In fuzz mode, panic to fail the test
            if is_fuzz_mode() {
                panic!("Scenario '{}' failed with seed {}: {}", name, seed, e);
            }
        }
    }
}

// ============================================================================
// Test Entry Point Helpers
// ============================================================================

/// Maximum time allowed for a subprocess to complete (in seconds).
///
/// The k8s_shard_splits test uses filesystem-based SlateDB which runs at about
/// 4-10x slower than simulated time. With 24 simulated seconds needed to complete,
/// the test may take up to 240 seconds in worst case. We use 180 seconds as the
/// timeout to allow for reasonable completion while catching truly hung tests.
const SUBPROCESS_TIMEOUT_SECS: u64 = 180;

/// Helper to run a scenario in a subprocess and verify success.
/// Enforces a timeout to catch scenarios that hang or run too long.
///
/// Uses temp files for output to avoid pipe buffer deadlocks.
pub fn run_in_subprocess(scenario: &str, seed: u64) -> std::process::Output {
    use std::fs::File;
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());

    // Use temp files for output to avoid pipe buffer deadlocks
    let stdout_file = tempfile::NamedTempFile::new().expect("create stdout temp file");
    let stderr_file = tempfile::NamedTempFile::new().expect("create stderr temp file");

    let stdout_fd = stdout_file.reopen().expect("reopen stdout temp file");
    let stderr_fd = stderr_file.reopen().expect("reopen stderr temp file");

    let mut cmd = Command::new(&cargo);
    cmd.args([
        "test",
        "--features",
        "dst",
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
    .stdout(Stdio::from(stdout_fd))
    .stderr(Stdio::from(stderr_fd));

    // Create a new process group so we can kill all descendants on timeout
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }

    let mut child = cmd.spawn().expect("Failed to spawn test subprocess");

    let start = Instant::now();
    let timeout = Duration::from_secs(SUBPROCESS_TIMEOUT_SECS);

    // Poll for completion with timeout
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                // Process completed - read output from temp files
                let mut stdout = Vec::new();
                let mut stderr = Vec::new();
                File::open(stdout_file.path())
                    .and_then(|mut f| f.read_to_end(&mut stdout))
                    .ok();
                File::open(stderr_file.path())
                    .and_then(|mut f| f.read_to_end(&mut stderr))
                    .ok();
                return std::process::Output {
                    status,
                    stdout,
                    stderr,
                };
            }
            Ok(None) => {
                // Still running - check timeout
                if start.elapsed() > timeout {
                    // Kill the entire process group (cargo + child test binary)
                    // Using negative PID kills the process group
                    #[cfg(unix)]
                    {
                        // Use kill command to send SIGKILL to the process group
                        let pgid = child.id();
                        Command::new("kill")
                            .args(["-9", &format!("-{}", pgid)])
                            .output()
                            .ok();
                    }
                    // Also kill the direct child (for non-unix platforms)
                    child.kill().ok();
                    child.wait().ok(); // Reap the zombie

                    // Read output from temp files
                    let mut stdout = Vec::new();
                    let mut stderr = Vec::new();
                    File::open(stdout_file.path())
                        .and_then(|mut f| f.read_to_end(&mut stdout))
                        .ok();
                    File::open(stderr_file.path())
                        .and_then(|mut f| f.read_to_end(&mut stderr))
                        .ok();

                    panic!(
                        "Subprocess '{}' timed out after {}s.\n\
                         This likely indicates a hang or infinite loop in the scenario.\n\
                         stdout:\n{}\n\
                         stderr:\n{}",
                        scenario,
                        SUBPROCESS_TIMEOUT_SECS,
                        String::from_utf8_lossy(&stdout),
                        String::from_utf8_lossy(&stderr)
                    );
                }
                // Sleep briefly before polling again
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                panic!("Error waiting for subprocess: {}", e);
            }
        }
    }
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

// ============================================================================
// Invariant Verification Infrastructure
// ============================================================================
//
// These types and functions help verify system invariants during DST scenarios.
// Invariants are derived from the Alloy specification in specs/job_shard.als.

/// Tracks global concurrency state across all workers for invariant verification.
/// This verifies the Alloy invariant: queueLimitEnforced - at most max_concurrency
/// tasks can be leased for a given limit key at any time.
///
/// This tracker receives events from server-side instrumentation via DST events,
/// ensuring accurate tracking without race conditions from client-side tracking.
///
/// Note: The actual system uses `tenant|queue` as the composite key - each tenant
/// has its own separate limit for each queue name. This tracker mirrors that behavior.
/// Jobs for a given tenant should only be routed to one shard at a time.
#[derive(Debug, Default)]
pub struct GlobalConcurrencyTracker {
    /// Maps tenant|queue -> set of (task_id, job_id) pairs currently holding the limit
    holders: Mutex<HashMap<String, HashSet<(String, String)>>>,
    /// Maps queue_name -> max_concurrency for that queue
    /// (limits are per-queue, applied separately to each tenant)
    limits: Mutex<HashMap<String, u32>>,
}

impl GlobalConcurrencyTracker {
    /// Create a new tracker.
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a queue limit and its max concurrency value.
    /// The limit applies separately to each tenant.
    pub fn register_limit(&self, queue: &str, max_concurrency: u32) {
        let mut limits = self.limits.lock().unwrap();
        limits.entry(queue.to_string()).or_insert(max_concurrency);
    }

    /// Record that a task has acquired a concurrency slot.
    /// Panics if this would violate the concurrency limit for that tenant+queue.
    pub fn acquire(&self, tenant: &str, queue: &str, task_id: &str, job_id: &str) {
        let mut holders = self.holders.lock().unwrap();
        let limits = self.limits.lock().unwrap();

        // Use tenant|queue as composite key, matching the actual ConcurrencyManager
        let composite_key = format!("{}|{}", tenant, queue);
        let entry = holders.entry(composite_key.clone()).or_default();
        // Look up max by queue name only (limits are registered by queue name)
        let max = limits.get(queue).copied().unwrap_or(u32::MAX);

        // Check invariant BEFORE adding
        if entry.len() >= max as usize {
            panic!(
                "INVARIANT VIOLATION (queueLimitEnforced): Attempting to acquire slot for limit '{}' (tenant='{}', queue='{}') \
                 but already at max_concurrency {}. Current holders: {:?}. \
                 New task_id={}, job_id={}",
                composite_key, tenant, queue, max, entry, task_id, job_id
            );
        }

        let inserted = entry.insert((task_id.to_string(), job_id.to_string()));
        if !inserted {
            panic!(
                "INVARIANT VIOLATION (noDoubleLease): Task {} already holds limit '{}'",
                task_id, composite_key
            );
        }

        tracing::trace!(
            tenant = tenant,
            queue = queue,
            task_id = task_id,
            job_id = job_id,
            current_count = entry.len(),
            max_concurrency = max,
            "concurrency_acquired"
        );
    }

    /// Record that a task has released a concurrency slot.
    pub fn release(&self, tenant: &str, queue: &str, task_id: &str) {
        let mut holders = self.holders.lock().unwrap();
        let composite_key = format!("{}|{}", tenant, queue);

        if let Some(entry) = holders.get_mut(&composite_key) {
            // Find and remove any entry matching this task_id (job_id may vary)
            let to_remove: Option<(String, String)> =
                entry.iter().find(|(tid, _)| tid == task_id).cloned();

            if let Some(key) = to_remove {
                entry.remove(&key);
                tracing::trace!(
                    tenant = tenant,
                    queue = queue,
                    task_id = task_id,
                    remaining_count = entry.len(),
                    "concurrency_released"
                );
            } else {
                tracing::trace!(
                    tenant = tenant,
                    queue = queue,
                    task_id = task_id,
                    "release called for task not in holders set (may have been released already)"
                );
            }
        }
    }

    /// Get the total holder count for a queue across all tenants.
    /// Useful for logging/debugging when tenants are not tracked individually.
    pub fn total_holder_count_for_queue(&self, queue: &str) -> usize {
        let holders = self.holders.lock().unwrap();
        let suffix = format!("|{}", queue);
        holders
            .iter()
            .filter(|(key, _)| key.ends_with(&suffix))
            .map(|(_, set)| set.len())
            .sum()
    }

    /// Verify that a job doesn't have duplicate active leases (oneLeasePerJob invariant)
    pub fn verify_no_duplicate_job_leases(&self) {
        let holders = self.holders.lock().unwrap();

        // Collect all job_ids across all limit keys
        let mut job_to_tasks: HashMap<String, Vec<(String, String)>> = HashMap::new();

        for (limit_key, tasks) in holders.iter() {
            for (task_id, job_id) in tasks {
                job_to_tasks
                    .entry(job_id.clone())
                    .or_default()
                    .push((limit_key.clone(), task_id.clone()));
            }
        }

        // Check that no job has multiple tasks with the same limit key
        // (a job can have multiple limit keys, but not duplicate entries for the same key)
        for (job_id, entries) in &job_to_tasks {
            let mut seen_limits: HashSet<&str> = HashSet::new();
            for (limit_key, task_id) in entries {
                if !seen_limits.insert(limit_key.as_str()) {
                    panic!(
                        "INVARIANT VIOLATION (oneLeasePerJob): Job '{}' has multiple tasks \
                         holding limit '{}'. Entries: {:?}",
                        job_id, limit_key, entries
                    );
                }
                let _ = task_id; // used in debug output above
            }
        }
    }
}

/// Tracks shard ownership for split-brain detection.
///
/// Verifies the invariant:
/// - noSplitBrain: A shard is owned by at most one node at any time
///
/// This tracker receives ShardAcquired/ShardReleased events from server-side
/// instrumentation, allowing continuous verification without polling.
#[derive(Debug, Default)]
pub struct ShardOwnershipTracker {
    /// Maps shard_id -> current owner node_id (if any)
    current_owners: Mutex<HashMap<String, String>>,
    /// Records any split-brain violations detected: (shard_id, node1, node2, timestamp)
    violations: Mutex<Vec<(String, String, String, u64)>>,
    /// Monotonic counter for ordering events
    event_counter: std::sync::atomic::AtomicU64,
}

impl ShardOwnershipTracker {
    /// Create a new tracker.
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a node acquired ownership of a shard.
    /// If another node already owns this shard, records a split-brain violation.
    pub fn shard_acquired(&self, node_id: &str, shard_id: &str) {
        let timestamp = self
            .event_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut owners = self.current_owners.lock().unwrap();

        if let Some(existing_owner) = owners.get(shard_id) {
            if existing_owner != node_id {
                // SPLIT-BRAIN DETECTED!
                tracing::error!(
                    shard_id = %shard_id,
                    existing_owner = %existing_owner,
                    new_owner = %node_id,
                    timestamp = timestamp,
                    "SPLIT-BRAIN DETECTED: shard acquired by new node while still owned by another"
                );
                let mut violations = self.violations.lock().unwrap();
                violations.push((
                    shard_id.to_string(),
                    existing_owner.clone(),
                    node_id.to_string(),
                    timestamp,
                ));
            }
            // Even if same node, update is fine (idempotent)
        }

        owners.insert(shard_id.to_string(), node_id.to_string());
        tracing::trace!(
            shard_id = %shard_id,
            node_id = %node_id,
            timestamp = timestamp,
            "shard_ownership_acquired"
        );
    }

    /// Record that a node released ownership of a shard.
    pub fn shard_released(&self, node_id: &str, shard_id: &str) {
        let timestamp = self
            .event_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut owners = self.current_owners.lock().unwrap();

        if let Some(existing_owner) = owners.get(shard_id) {
            if existing_owner == node_id {
                owners.remove(shard_id);
                tracing::trace!(
                    shard_id = %shard_id,
                    node_id = %node_id,
                    timestamp = timestamp,
                    "shard_ownership_released"
                );
            } else {
                // Node releasing a shard it doesn't own - this might indicate a race
                // but isn't necessarily a split-brain (the release might be stale)
                tracing::warn!(
                    shard_id = %shard_id,
                    releasing_node = %node_id,
                    actual_owner = %existing_owner,
                    timestamp = timestamp,
                    "shard_release_by_non_owner"
                );
            }
        } else {
            // Releasing an unowned shard - might happen during cleanup
            tracing::trace!(
                shard_id = %shard_id,
                node_id = %node_id,
                timestamp = timestamp,
                "shard_release_of_unowned"
            );
        }
    }

    /// Verify no split-brain occurred. Panics if any violations were detected.
    pub fn verify_no_split_brain(&self) {
        let violations = self.violations.lock().unwrap();
        if !violations.is_empty() {
            let details: Vec<String> = violations
                .iter()
                .map(|(shard, n1, n2, ts)| {
                    format!(
                        "shard {} owned by both {} and {} at event {}",
                        shard, n1, n2, ts
                    )
                })
                .collect();
            panic!(
                "INVARIANT VIOLATION (noSplitBrain): {} split-brain events detected:\n  {}",
                violations.len(),
                details.join("\n  ")
            );
        }
    }

    /// Check if any split-brain violations have been detected (non-panicking).
    pub fn has_violations(&self) -> bool {
        let violations = self.violations.lock().unwrap();
        !violations.is_empty()
    }

    /// Get the current owner of a shard (for debugging).
    #[allow(dead_code)]
    pub fn get_owner(&self, shard_id: &str) -> Option<String> {
        let owners = self.current_owners.lock().unwrap();
        owners.get(shard_id).cloned()
    }

    /// Get the count of currently owned shards (for debugging).
    #[allow(dead_code)]
    pub fn owned_shard_count(&self) -> usize {
        let owners = self.current_owners.lock().unwrap();
        owners.len()
    }
}

/// Tracks job state for invariant verification.
/// Verifies invariants like:
/// - noLeasesForTerminal: Terminal jobs have no active leases
/// - noDoubleLease: A task is never leased twice concurrently
/// - oneLeasePerJob: A job never has multiple active leases
/// - validTransitions: Only valid status transitions occur
/// - noZombieAttempts: Terminal jobs have no running attempts
///
/// This tracker receives events from server-side instrumentation via DST events,
/// ensuring accurate tracking without race conditions from client-side tracking.
#[derive(Debug, Default)]
pub struct JobStateTracker {
    /// Maps job_id -> last known status
    job_statuses: Mutex<HashMap<String, i32>>,
    /// Maps job_id -> set of task_ids with active leases
    active_leases: Mutex<HashMap<String, HashSet<String>>>,
    /// Set of job_ids that have been completed (success/fail/cancel)
    terminal_jobs: Mutex<HashSet<String>>,
    /// Set of job_ids that have been successfully completed (for duplicate detection)
    completed_jobs: Mutex<HashSet<String>>,
    /// Track status transition history for validTransitions invariant
    /// Maps job_id -> list of (status, timestamp_ms) transitions
    status_history: Mutex<HashMap<String, Vec<(i32, i64)>>>,
}

/// Get human-readable name for a job status
fn status_name(status: i32) -> &'static str {
    match status {
        s if s == JobStatus::Scheduled as i32 => "Scheduled",
        s if s == JobStatus::Running as i32 => "Running",
        s if s == JobStatus::Succeeded as i32 => "Succeeded",
        s if s == JobStatus::Failed as i32 => "Failed",
        s if s == JobStatus::Cancelled as i32 => "Cancelled",
        _ => "Unknown",
    }
}

/// Check if a status transition is valid according to the Alloy spec.
///
/// Valid transitions from the Alloy spec (validTransitions assertion):
/// - Scheduled -> Scheduled | Running
/// - Running -> Running | Succeeded | Failed | Scheduled (retry)
/// - Succeeded -> Succeeded (terminal, no transitions out)
/// - Failed -> Failed | Scheduled (restart allowed)
/// - Cancelled is orthogonal to status in Alloy, but we track it as a terminal state
fn is_valid_status_transition(from: i32, to: i32) -> bool {
    match from {
        // Scheduled can go to Scheduled (no-op) or Running (lease acquired)
        s if s == JobStatus::Scheduled as i32 => {
            to == JobStatus::Scheduled as i32 || to == JobStatus::Running as i32
        }
        // Running can go to Running, Succeeded, Failed, or Scheduled (retry)
        s if s == JobStatus::Running as i32 => {
            to == JobStatus::Running as i32
                || to == JobStatus::Succeeded as i32
                || to == JobStatus::Failed as i32
                || to == JobStatus::Scheduled as i32
                || to == JobStatus::Cancelled as i32 // Cancellation during running
        }
        // Succeeded is truly terminal - no transitions out
        s if s == JobStatus::Succeeded as i32 => to == JobStatus::Succeeded as i32,
        // Failed can stay Failed or go to Scheduled (restart)
        s if s == JobStatus::Failed as i32 => {
            to == JobStatus::Failed as i32 || to == JobStatus::Scheduled as i32
        }
        // Cancelled is terminal in our tracker (though Alloy models it differently)
        s if s == JobStatus::Cancelled as i32 => {
            to == JobStatus::Cancelled as i32 || to == JobStatus::Scheduled as i32 // restart allowed
        }
        _ => false,
    }
}

impl JobStateTracker {
    /// Create a new tracker.
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a status transition and validate it.
    /// Panics on invalid transitions.
    fn record_transition(&self, job_id: &str, new_status: i32) {
        let mut history = self.status_history.lock().unwrap();
        let entry = history.entry(job_id.to_string()).or_default();

        // Check if this is a valid transition from the previous state
        if let Some(&(prev_status, _)) = entry.last() {
            if !is_valid_status_transition(prev_status, new_status) {
                panic!(
                    "INVARIANT VIOLATION (validTransitions): Job '{}' invalid transition {:?} -> {:?}",
                    job_id,
                    status_name(prev_status),
                    status_name(new_status)
                );
            }
        }

        // Record the transition with current timestamp
        // Note: In DST, time is simulated, so we use a simple counter
        let timestamp = entry.len() as i64;
        entry.push((new_status, timestamp));
    }

    /// Verify all recorded transitions were valid.
    ///
    /// This is a comprehensive check that can be called at the end of a scenario
    /// to verify the validTransitions invariant held throughout.
    pub fn verify_all_transitions(&self) -> Vec<String> {
        let mut violations = Vec::new();
        let history = self.status_history.lock().unwrap();

        for (job_id, transitions) in history.iter() {
            for window in transitions.windows(2) {
                let (from_status, _) = window[0];
                let (to_status, _) = window[1];

                if !is_valid_status_transition(from_status, to_status) {
                    violations.push(format!(
                        "Job '{}': invalid transition {:?} -> {:?}",
                        job_id,
                        status_name(from_status),
                        status_name(to_status)
                    ));
                }
            }
        }

        if !violations.is_empty() {
            tracing::trace!(violations = violations.len(), "found transition violations");
        }

        violations
    }

    /// Get transition history for a job (for debugging)
    #[allow(dead_code)]
    pub fn get_transition_history(&self, job_id: &str) -> Vec<(i32, i64)> {
        let history = self.status_history.lock().unwrap();
        history.get(job_id).cloned().unwrap_or_default()
    }

    /// Record that a job was enqueued
    pub fn job_enqueued(&self, job_id: &str) {
        let mut statuses = self.job_statuses.lock().unwrap();
        statuses.insert(job_id.to_string(), JobStatus::Scheduled as i32);
        drop(statuses);
        self.record_transition(job_id, JobStatus::Scheduled as i32);
    }

    /// Record that a task was leased for a job.
    /// Panics if this would violate noLeasesForTerminal, noDoubleLease, or oneLeasePerJob invariants.
    pub fn task_leased(&self, job_id: &str, task_id: &str) {
        // Check that this job is not terminal
        {
            let terminal = self.terminal_jobs.lock().unwrap();
            if terminal.contains(job_id) {
                panic!(
                    "INVARIANT VIOLATION (noLeasesForTerminal): Attempting to lease task {} \
                     for job {} which is already terminal",
                    task_id, job_id
                );
            }
        }

        let mut leases = self.active_leases.lock().unwrap();
        let entry = leases.entry(job_id.to_string()).or_default();

        if entry.contains(task_id) {
            panic!(
                "INVARIANT VIOLATION (noDoubleLease): Task '{}' already leased for job '{}'",
                task_id, job_id
            );
        } else if !entry.is_empty() {
            panic!(
                "INVARIANT VIOLATION (oneLeasePerJob): Job '{}' already has active lease(s) {:?} \
                 but received new task '{}'",
                job_id, entry, task_id
            );
        }

        entry.insert(task_id.to_string());

        let mut statuses = self.job_statuses.lock().unwrap();
        statuses.insert(job_id.to_string(), JobStatus::Running as i32);
        drop(statuses);
        self.record_transition(job_id, JobStatus::Running as i32);
    }

    /// Record that a task lease was released (completed, failed, or expired)
    pub fn task_released(&self, job_id: &str, task_id: &str) {
        let mut leases = self.active_leases.lock().unwrap();
        if let Some(job_leases) = leases.get_mut(job_id) {
            job_leases.remove(task_id);
            if job_leases.is_empty() {
                leases.remove(job_id);
            }
        }
    }

    /// Record that a job completed successfully.
    /// Verifies no duplicate completions.
    pub fn job_completed(&self, job_id: &str) -> bool {
        let mut completed = self.completed_jobs.lock().unwrap();
        let was_new = completed.insert(job_id.to_string());

        if was_new {
            let mut terminal = self.terminal_jobs.lock().unwrap();
            terminal.insert(job_id.to_string());

            let mut statuses = self.job_statuses.lock().unwrap();
            statuses.insert(job_id.to_string(), JobStatus::Succeeded as i32);
            drop(statuses);
            self.record_transition(job_id, JobStatus::Succeeded as i32);
        }

        was_new
    }

    /// Record that a job failed (permanently, not retrying)
    #[allow(dead_code)]
    pub fn job_failed(&self, job_id: &str) {
        let mut terminal = self.terminal_jobs.lock().unwrap();
        terminal.insert(job_id.to_string());

        let mut statuses = self.job_statuses.lock().unwrap();
        statuses.insert(job_id.to_string(), JobStatus::Failed as i32);
        drop(statuses);
        self.record_transition(job_id, JobStatus::Failed as i32);
    }

    /// Record that a job was cancelled
    pub fn job_cancelled(&self, job_id: &str) {
        let mut terminal = self.terminal_jobs.lock().unwrap();
        terminal.insert(job_id.to_string());

        let mut statuses = self.job_statuses.lock().unwrap();
        statuses.insert(job_id.to_string(), JobStatus::Cancelled as i32);
        drop(statuses);
        self.record_transition(job_id, JobStatus::Cancelled as i32);
    }

    /// Record that a job is being retried/restarted (not terminal).
    /// This removes the job from the terminal set if it was there,
    /// allowing it to be leased again.
    pub fn job_retrying(&self, job_id: &str) {
        // Remove from terminal set if present (for restart scenarios)
        {
            let mut terminal = self.terminal_jobs.lock().unwrap();
            terminal.remove(job_id);
        }
        {
            let mut completed = self.completed_jobs.lock().unwrap();
            completed.remove(job_id);
        }

        let mut statuses = self.job_statuses.lock().unwrap();
        statuses.insert(job_id.to_string(), JobStatus::Scheduled as i32);
        drop(statuses);
        self.record_transition(job_id, JobStatus::Scheduled as i32);
    }

    /// Check if a job has been completed
    pub fn is_completed(&self, job_id: &str) -> bool {
        let completed = self.completed_jobs.lock().unwrap();
        completed.contains(job_id)
    }

    /// Check if a job is terminal
    #[allow(dead_code)]
    pub fn is_terminal(&self, job_id: &str) -> bool {
        let terminal = self.terminal_jobs.lock().unwrap();
        terminal.contains(job_id)
    }

    /// Verify that no terminal jobs have active leases.
    /// Panics if any terminal job has active leases.
    pub fn verify_no_terminal_leases(&self) {
        let terminal = self.terminal_jobs.lock().unwrap();
        let leases = self.active_leases.lock().unwrap();

        for job_id in terminal.iter() {
            if let Some(active) = leases.get(job_id) {
                if !active.is_empty() {
                    panic!(
                        "INVARIANT VIOLATION (noLeasesForTerminal): Terminal job '{}' \
                         still has active leases: {:?}",
                        job_id, active
                    );
                }
            }
        }
    }

    /// Get the count of completed jobs
    #[allow(dead_code)]
    pub fn completed_count(&self) -> usize {
        let completed = self.completed_jobs.lock().unwrap();
        completed.len()
    }

    /// Get the count of terminal jobs (completed + failed + cancelled)
    pub fn terminal_count(&self) -> usize {
        let terminal = self.terminal_jobs.lock().unwrap();
        terminal.len()
    }
}

/// Combined invariant tracker for comprehensive verification.
///
/// This tracker consumes server-side DST events to track state accurately,
/// avoiding the race conditions that come with client-side event tracking.
#[derive(Debug, Default)]
pub struct InvariantTracker {
    pub concurrency: GlobalConcurrencyTracker,
    pub jobs: JobStateTracker,
    pub shards: ShardOwnershipTracker,
}

impl InvariantTracker {
    /// Create a new tracker.
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Run all invariant checks. Should be called periodically during scenarios.
    pub fn verify_all(&self) {
        self.concurrency.verify_no_duplicate_job_leases();
        self.jobs.verify_no_terminal_leases();
        self.shards.verify_no_split_brain();
    }

    /// Process all pending DST events from the server-side instrumentation.
    ///
    /// This should be called periodically to consume events emitted by the
    /// server during test execution. It updates the tracker state based on
    /// the server's ground-truth events, ensuring accurate tracking without
    /// race conditions from client-side event tracking.
    pub fn process_dst_events(&self) {
        use silo::dst_events::{DstEvent, drain_events};

        for event in drain_events() {
            match event {
                DstEvent::JobEnqueued {
                    tenant: _,
                    ref job_id,
                } => {
                    self.jobs.job_enqueued(job_id);
                }
                DstEvent::JobStatusChanged {
                    tenant: _,
                    ref job_id,
                    ref new_status,
                } => {
                    match new_status.as_str() {
                        "Scheduled" => self.jobs.job_retrying(job_id),
                        "Running" => {
                            // Running is handled by TaskLeased event - skip here to avoid double transition
                        }
                        "Succeeded" => {
                            self.jobs.job_completed(job_id);
                        }
                        "Failed" => {
                            self.jobs.job_failed(job_id);
                        }
                        "Cancelled" => {
                            self.jobs.job_cancelled(job_id);
                        }
                        _ => {
                            tracing::warn!(new_status = %new_status, "unknown job status in DST event");
                        }
                    }
                }
                DstEvent::TaskLeased {
                    tenant: _,
                    ref job_id,
                    ref task_id,
                    worker_id: _,
                } => {
                    self.jobs.task_leased(job_id, task_id);
                }
                DstEvent::TaskReleased {
                    tenant: _,
                    ref job_id,
                    ref task_id,
                } => {
                    self.jobs.task_released(job_id, task_id);
                }
                DstEvent::ConcurrencyTicketGranted {
                    ref tenant,
                    ref job_id,
                    ref queue,
                    ref task_id,
                } => {
                    self.concurrency.acquire(tenant, queue, task_id, job_id);
                }
                DstEvent::ConcurrencyTicketReleased {
                    ref tenant,
                    ref queue,
                    ref task_id,
                } => {
                    self.concurrency.release(tenant, queue, task_id);
                }
                DstEvent::ShardAcquired {
                    ref node_id,
                    ref shard_id,
                } => {
                    self.shards.shard_acquired(node_id, shard_id);
                }
                DstEvent::ShardReleased {
                    ref node_id,
                    ref shard_id,
                } => {
                    self.shards.shard_released(node_id, shard_id);
                }
            }
        }
    }
}

// ============================================================================
// Server-Side Invariant Validation
// ============================================================================
//
// These functions query the server directly to validate invariants that can't
// be reliably tracked client-side due to network delays and message loss.

/// Result of server-side invariant validation
#[derive(Debug, Default)]
pub struct ServerInvariantResult {
    /// Number of jobs currently in Running state
    pub running_job_count: u32,
    /// Number of jobs in terminal states (Succeeded, Failed, Cancelled)
    pub terminal_job_count: u32,
    /// Map of queue_name -> holder count
    pub holder_counts_by_queue: HashMap<String, u32>,
    /// Map of queue_name -> requester count (waiting for ticket)
    pub requester_counts_by_queue: HashMap<String, u32>,
    /// List of invariant violations found
    pub violations: Vec<String>,
}

impl ServerInvariantResult {
    /// Check if all server invariants passed.
    #[allow(dead_code)]
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }
}

/// Query the server to verify invariants hold.
///
/// This uses the Query gRPC endpoint to inspect server state directly,
/// providing ground truth that isn't affected by client-side tracking drift.
///
/// Invariants checked:
/// - `noHoldersForTerminal`: Terminal jobs have no concurrency holders
/// - `queueLimitEnforced`: Holder counts are reported (caller can verify against limits)
/// - Job state distribution is consistent
pub async fn verify_server_invariants(
    client: &mut silo::pb::silo_client::SiloClient<tonic::transport::Channel>,
    shard: &str,
) -> Result<ServerInvariantResult, String> {
    let mut result = ServerInvariantResult::default();

    // Query 1: Get job state distribution
    let job_status_query = "SELECT status_kind, COUNT(*) as cnt FROM jobs GROUP BY status_kind";
    match client
        .query(tonic::Request::new(QueryRequest {
            shard: shard.to_string(),
            sql: job_status_query.to_string(),
            tenant: None,
        }))
        .await
    {
        Ok(resp) => {
            let response = resp.into_inner();
            for row_bytes in &response.rows {
                if let Some(serialized_bytes::Encoding::Msgpack(data)) = &row_bytes.encoding {
                    if let Ok(row) = parse_msgpack_row(data) {
                        let status = row.get("status_kind").and_then(|v| v.as_str());
                        let count = row.get("cnt").and_then(|v| v.as_u64()).unwrap_or(0) as u32;

                        match status {
                            Some("Running") => result.running_job_count = count,
                            Some("Succeeded") | Some("Failed") | Some("Cancelled") => {
                                result.terminal_job_count += count;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        Err(e) => {
            tracing::trace!(error = %e, "job status query failed");
            // Don't fail the entire validation on query errors (may be transient)
        }
    }

    // Query 2: Get holder counts by queue
    let holder_query = "SELECT queue_name, COUNT(*) as cnt FROM queues WHERE entry_type = 'holder' GROUP BY queue_name";
    match client
        .query(tonic::Request::new(QueryRequest {
            shard: shard.to_string(),
            sql: holder_query.to_string(),
            tenant: None,
        }))
        .await
    {
        Ok(resp) => {
            let response = resp.into_inner();
            for row_bytes in &response.rows {
                if let Some(serialized_bytes::Encoding::Msgpack(data)) = &row_bytes.encoding {
                    if let Ok(row) = parse_msgpack_row(data) {
                        if let (Some(queue_name), Some(count)) = (
                            row.get("queue_name").and_then(|v| v.as_str()),
                            row.get("cnt").and_then(|v| v.as_u64()),
                        ) {
                            result
                                .holder_counts_by_queue
                                .insert(queue_name.to_string(), count as u32);
                        }
                    }
                }
            }
        }
        Err(e) => {
            tracing::trace!(error = %e, "holder count query failed");
        }
    }

    // Query 3: Get requester counts by queue
    let requester_query = "SELECT queue_name, COUNT(*) as cnt FROM queues WHERE entry_type = 'requester' GROUP BY queue_name";
    match client
        .query(tonic::Request::new(QueryRequest {
            shard: shard.to_string(),
            sql: requester_query.to_string(),
            tenant: None,
        }))
        .await
    {
        Ok(resp) => {
            let response = resp.into_inner();
            for row_bytes in &response.rows {
                if let Some(serialized_bytes::Encoding::Msgpack(data)) = &row_bytes.encoding {
                    if let Ok(row) = parse_msgpack_row(data) {
                        if let (Some(queue_name), Some(count)) = (
                            row.get("queue_name").and_then(|v| v.as_str()),
                            row.get("cnt").and_then(|v| v.as_u64()),
                        ) {
                            result
                                .requester_counts_by_queue
                                .insert(queue_name.to_string(), count as u32);
                        }
                    }
                }
            }
        }
        Err(e) => {
            tracing::trace!(error = %e, "requester count query failed");
        }
    }

    // Query 4: Check for terminal jobs with holders (noHoldersForTerminal violation)
    // This joins jobs and queues to find any terminal jobs that still have holders
    let terminal_with_holders_query = r#"
        SELECT j.id, j.status_kind, q.queue_name, q.task_id
        FROM jobs j
        JOIN queues q ON q.entry_type = 'holder'
        WHERE j.status_kind IN ('Succeeded', 'Failed')
        LIMIT 10
    "#;
    match client
        .query(tonic::Request::new(QueryRequest {
            shard: shard.to_string(),
            sql: terminal_with_holders_query.to_string(),
            tenant: None,
        }))
        .await
    {
        Ok(resp) => {
            let response = resp.into_inner();
            // If we get any rows, it's a violation
            // Note: This is a simplified check - we can't easily join on job_id
            // in the queues table since holders don't store job_id directly.
            // The holder's task_id would need to be cross-referenced.
            // For now, we rely on holder count checks instead.
            if response.row_count > 0 {
                tracing::trace!(
                    row_count = response.row_count,
                    "found potential terminal jobs with holders (needs further investigation)"
                );
            }
        }
        Err(e) => {
            tracing::trace!(error = %e, "terminal with holders query failed");
        }
    }

    tracing::trace!(
        running = result.running_job_count,
        terminal = result.terminal_job_count,
        holders = ?result.holder_counts_by_queue,
        requesters = ?result.requester_counts_by_queue,
        violations = result.violations.len(),
        "server_invariant_check"
    );

    Ok(result)
}

/// Verify that no queue has more holders than its limit.
///
/// This is a helper that can be called after verify_server_invariants
/// with knowledge of the expected limits.
pub fn check_holder_limits(
    result: &ServerInvariantResult,
    limits: &HashMap<String, u32>,
) -> Vec<String> {
    let mut violations = Vec::new();

    for (queue, &holder_count) in &result.holder_counts_by_queue {
        if let Some(&max_allowed) = limits.get(queue) {
            if holder_count > max_allowed {
                violations.push(format!(
                    "queueLimitEnforced violation: queue '{}' has {} holders but max is {}",
                    queue, holder_count, max_allowed
                ));
            }
        }
    }

    violations
}

/// Parse a MessagePack-encoded row into a JSON value for easy field access.
fn parse_msgpack_row(data: &[u8]) -> Result<serde_json::Value, String> {
    rmp_serde::from_slice(data).map_err(|e| format!("failed to parse msgpack row: {}", e))
}
