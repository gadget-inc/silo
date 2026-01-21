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
    ConcurrencyLimit, EnqueueRequest, GetJobRequest, JobStatus, LeaseTasksRequest, Limit,
    MsgpackBytes, ReportOutcomeRequest, RetryPolicy, Task, limit, report_outcome_request,
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
///
/// Panics if called more than once in the same process - each DST scenario
/// must run in its own process for true determinism.
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
pub async fn create_turmoil_client(
    uri: &str,
    config: &ClientConfig,
) -> turmoil::Result<silo::pb::silo_client::SiloClient<tonic::transport::Channel>> {
    let endpoint = tonic::transport::Endpoint::new(uri.to_string())
        .map_err(|e| e.to_string())?
        .connect_timeout(config.connect_timeout)
        .timeout(config.request_timeout);

    let mut last_error = None;
    for attempt in 0..config.max_retries {
        match endpoint.connect_with_connector(turmoil_connector()).await {
            Ok(channel) => return Ok(silo::pb::silo_client::SiloClient::new(channel)),
            Err(e) => {
                tracing::trace!(
                    attempt = attempt,
                    max_retries = config.max_retries,
                    error = %e,
                    uri = uri,
                    "connection attempt failed, retrying"
                );
                last_error = Some(e.to_string());
                if attempt + 1 < config.max_retries {
                    tokio::time::sleep(config.backoff_for_attempt(attempt)).await;
                }
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| "no connection attempts made".to_string())
        .into())
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

// ============================================================================
// Invariant Verification Infrastructure
// ============================================================================
//
// These types and functions help verify system invariants during DST scenarios.
// Invariants are derived from the Alloy specification in specs/job_shard.als.

/// Tracks global concurrency state across all workers for invariant verification.
/// This verifies the Alloy invariant: queueLimitEnforced - at most max_concurrency
/// tasks can be leased for a given limit key at any time.
#[derive(Debug, Default)]
pub struct GlobalConcurrencyTracker {
    /// Maps limit_key -> set of (task_id, job_id) pairs currently holding the limit
    holders: Mutex<HashMap<String, HashSet<(String, String)>>>,
    /// Maps limit_key -> max_concurrency for that key
    limits: Mutex<HashMap<String, u32>>,
    /// If true, log warnings instead of panicking on apparent violations
    /// (can happen with message loss causing stale tracking state)
    lenient_mode: bool,
}

impl GlobalConcurrencyTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a tracker in lenient mode, suitable for chaos testing.
    ///
    /// In lenient mode, apparent concurrency limit violations are logged as
    /// warnings instead of panics. This is necessary for high message loss
    /// scenarios where the client-side tracker can become stale due to:
    /// - Pre-releasing before report_outcome
    /// - Response loss causing re-acquire on failure
    /// - Server already processed the release but client doesn't know
    pub fn new_lenient() -> Self {
        Self {
            lenient_mode: true,
            ..Default::default()
        }
    }

    /// Register a limit key and its max concurrency value
    pub fn register_limit(&self, key: &str, max_concurrency: u32) {
        let mut limits = self.limits.lock().unwrap();
        limits.entry(key.to_string()).or_insert(max_concurrency);
    }

    /// Record that a task has acquired a concurrency slot.
    /// In strict mode, panics if this would violate the concurrency limit.
    /// In lenient mode, logs a warning for apparent violations.
    pub fn acquire(&self, limit_key: &str, task_id: &str, job_id: &str) {
        let mut holders = self.holders.lock().unwrap();
        let limits = self.limits.lock().unwrap();

        let entry = holders.entry(limit_key.to_string()).or_default();
        let max = limits.get(limit_key).copied().unwrap_or(u32::MAX);

        // Check invariant BEFORE adding
        if entry.len() >= max as usize {
            if self.lenient_mode {
                tracing::warn!(
                    limit_key = limit_key,
                    max_concurrency = max,
                    current_holders = ?entry,
                    new_task_id = task_id,
                    new_job_id = job_id,
                    "apparent concurrency limit exceeded (likely stale tracking due to message loss)"
                );
                // Still add the task in lenient mode - the tracker is stale, not the server
            } else {
                panic!(
                    "INVARIANT VIOLATION (queueLimitEnforced): Attempting to acquire slot for limit '{}' \
                     but already at max_concurrency {}. Current holders: {:?}. \
                     New task_id={}, job_id={}",
                    limit_key, max, entry, task_id, job_id
                );
            }
        }

        let inserted = entry.insert((task_id.to_string(), job_id.to_string()));
        if !inserted {
            if self.lenient_mode {
                tracing::warn!(
                    task_id = task_id,
                    limit_key = limit_key,
                    "task already tracked as holding limit (duplicate lease tracking)"
                );
            } else {
                panic!(
                    "INVARIANT VIOLATION (noDoubleLease): Task {} already holds limit '{}'",
                    task_id, limit_key
                );
            }
        }

        tracing::trace!(
            limit_key = limit_key,
            task_id = task_id,
            job_id = job_id,
            current_count = entry.len(),
            max_concurrency = max,
            "concurrency_acquired"
        );
    }

    /// Record that a task has released a concurrency slot.
    pub fn release(&self, limit_key: &str, task_id: &str, job_id: &str) {
        let mut holders = self.holders.lock().unwrap();

        if let Some(entry) = holders.get_mut(limit_key) {
            let removed = entry.remove(&(task_id.to_string(), job_id.to_string()));
            if !removed {
                tracing::warn!(
                    limit_key = limit_key,
                    task_id = task_id,
                    job_id = job_id,
                    "release called for task not in holders set (may have failed/timed out)"
                );
            } else {
                tracing::trace!(
                    limit_key = limit_key,
                    task_id = task_id,
                    job_id = job_id,
                    remaining_count = entry.len(),
                    "concurrency_released"
                );
            }
        }
    }

    /// Get the current holder count for a limit key
    pub fn holder_count(&self, limit_key: &str) -> usize {
        let holders = self.holders.lock().unwrap();
        holders.get(limit_key).map(|s| s.len()).unwrap_or(0)
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
                    if self.lenient_mode {
                        tracing::warn!(
                            job_id = job_id,
                            limit_key = limit_key,
                            entries = ?entries,
                            "job appears to have duplicate task leases (likely stale tracking due to message loss)"
                        );
                    } else {
                        panic!(
                            "INVARIANT VIOLATION (oneLeasePerJob): Job '{}' has multiple tasks \
                             holding limit '{}'. Entries: {:?}",
                            job_id, limit_key, entries
                        );
                    }
                }
                let _ = task_id; // used in debug output above
            }
        }
    }
}

/// Tracks job state for invariant verification.
/// Verifies invariants like:
/// - noLeasesForTerminal: Terminal jobs have no active leases
/// - validTransitions: Only valid status transitions occur
/// - noZombieAttempts: Terminal jobs have no running attempts
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
    /// If true, allow "stale" leases for terminal jobs (can happen with network delays)
    lenient_mode: bool,
}

impl JobStateTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a tracker in lenient mode, suitable for chaos testing.
    ///
    /// In lenient mode, receiving a lease for an already-terminal job is logged
    /// as a warning rather than causing a panic. This can happen legitimately
    /// when network delays cause lease responses to arrive out-of-order relative
    /// to completion reports.
    pub fn new_lenient() -> Self {
        Self {
            lenient_mode: true,
            ..Default::default()
        }
    }

    /// Record that a job was enqueued
    pub fn job_enqueued(&self, job_id: &str) {
        let mut statuses = self.job_statuses.lock().unwrap();
        statuses.insert(job_id.to_string(), JobStatus::Scheduled as i32);
    }

    /// Record that a task was leased for a job
    pub fn task_leased(&self, job_id: &str, task_id: &str) {
        // Check that this job is not terminal
        {
            let terminal = self.terminal_jobs.lock().unwrap();
            if terminal.contains(job_id) {
                if self.lenient_mode {
                    // In lenient mode, just log a warning - this can happen with network delays
                    tracing::warn!(
                        job_id = job_id,
                        task_id = task_id,
                        "received lease for already-terminal job (stale lease due to network delay)"
                    );
                    return;
                } else {
                    panic!(
                        "INVARIANT VIOLATION (noLeasesForTerminal): Attempting to lease task {} \
                         for job {} which is already terminal",
                        task_id, job_id
                    );
                }
            }
        }

        let mut leases = self.active_leases.lock().unwrap();
        leases
            .entry(job_id.to_string())
            .or_default()
            .insert(task_id.to_string());

        let mut statuses = self.job_statuses.lock().unwrap();
        statuses.insert(job_id.to_string(), JobStatus::Running as i32);
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
    }

    /// Record that a job was cancelled
    pub fn job_cancelled(&self, job_id: &str) {
        let mut terminal = self.terminal_jobs.lock().unwrap();
        terminal.insert(job_id.to_string());

        let mut statuses = self.job_statuses.lock().unwrap();
        statuses.insert(job_id.to_string(), JobStatus::Cancelled as i32);
    }

    /// Record that a job is being retried (not terminal)
    pub fn job_retrying(&self, job_id: &str) {
        let mut statuses = self.job_statuses.lock().unwrap();
        statuses.insert(job_id.to_string(), JobStatus::Scheduled as i32);
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

    /// Verify that no terminal jobs have active leases
    pub fn verify_no_terminal_leases(&self) {
        let terminal = self.terminal_jobs.lock().unwrap();
        let leases = self.active_leases.lock().unwrap();

        for job_id in terminal.iter() {
            if let Some(active) = leases.get(job_id) {
                if !active.is_empty() {
                    if self.lenient_mode {
                        tracing::warn!(
                            job_id = job_id,
                            active_leases = ?active,
                            "terminal job still has active leases (possible stale tracking)"
                        );
                    } else {
                        panic!(
                            "INVARIANT VIOLATION (noLeasesForTerminal): Terminal job '{}' \
                             still has active leases: {:?}",
                            job_id, active
                        );
                    }
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

/// Combined invariant tracker for comprehensive verification
#[derive(Debug, Default)]
pub struct InvariantTracker {
    pub concurrency: GlobalConcurrencyTracker,
    pub jobs: JobStateTracker,
}

impl InvariantTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a tracker in lenient mode, suitable for chaos testing.
    ///
    /// In lenient mode, certain timing-related invariant "violations" that can
    /// happen legitimately with network delays are logged as warnings instead
    /// of causing panics.
    pub fn new_lenient() -> Self {
        Self {
            concurrency: GlobalConcurrencyTracker::new_lenient(),
            jobs: JobStateTracker::new_lenient(),
        }
    }

    /// Run all invariant checks. Should be called periodically during scenarios.
    pub fn verify_all(&self) {
        self.concurrency.verify_no_duplicate_job_leases();
        self.jobs.verify_no_terminal_leases();
    }
}
