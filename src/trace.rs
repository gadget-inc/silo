#[cfg(debug_assertions)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once};

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::{Resource, runtime, trace as sdktrace};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::{EnvFilter, filter::LevelFilter, prelude::*};

use crate::settings::LogFormat;

static INIT: Once = Once::new();

fn build_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

#[cfg(feature = "tokio-console")]
fn console_layer() -> Option<console_subscriber::ConsoleLayer> {
    let (layer, server) = console_subscriber::ConsoleLayer::builder()
        .with_default_env()
        .build();
    tokio::spawn(async move { server.serve().await.expect("console subscriber server") });
    Some(layer)
}

#[cfg(not(feature = "tokio-console"))]
fn console_layer() -> Option<tracing_subscriber::layer::Identity> {
    None
}

/// A thread-safe file writer for the debug log layer.
#[derive(Clone)]
struct DebugFileWriter {
    file: Arc<Mutex<std::fs::File>>,
}

impl std::io::Write for DebugFileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.lock().unwrap().flush()
    }
}

impl<'a> MakeWriter<'a> for DebugFileWriter {
    type Writer = DebugFileWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Whether the process-global log capture is armed. Checked per event by the
/// capture layer's filter.
#[cfg(debug_assertions)]
static LOG_CAPTURE_ARMED: AtomicBool = AtomicBool::new(false);

/// Buffer holding formatted log lines while capture is armed.
#[cfg(debug_assertions)]
static LOG_CAPTURE_BUF: Mutex<Vec<u8>> = Mutex::new(Vec::new());

/// Writer that appends formatted events to the process-global capture buffer.
#[cfg(debug_assertions)]
#[derive(Clone)]
struct LogCaptureWriter;

#[cfg(debug_assertions)]
impl std::io::Write for LogCaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        LOG_CAPTURE_BUF
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(debug_assertions)]
impl<'a> MakeWriter<'a> for LogCaptureWriter {
    type Writer = LogCaptureWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LogCaptureWriter
    }
}

/// Begin capturing formatted log output into a process-global buffer.
///
/// Tests use this to assert on operational log markers (e.g. the split state
/// machine's pre-commit verification markers) without swapping the global
/// subscriber, which panics when spans cross subscriber registries. Capture
/// is process-global: arm it only while holding whatever serialization the
/// test suite uses, or expect interleaved output from concurrent tests.
///
/// Debug builds only: the capture layer's dynamic filter defeats tracing's
/// callsite caching, so release builds do not register it (or this API).
#[cfg(debug_assertions)]
pub fn start_log_capture() {
    LOG_CAPTURE_BUF
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    LOG_CAPTURE_ARMED.store(true, Ordering::SeqCst);
}

/// Disarm the log capture and return everything captured since
/// [`start_log_capture`].
#[cfg(debug_assertions)]
pub fn stop_log_capture() -> String {
    LOG_CAPTURE_ARMED.store(false, Ordering::SeqCst);
    let mut buf = LOG_CAPTURE_BUF
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let captured = String::from_utf8_lossy(&buf).into_owned();
    buf.clear();
    captured
}

/// Layer that formats events into the capture buffer while capture is armed.
/// Filtered by an atomic check so it does no formatting work when disarmed.
#[cfg(debug_assertions)]
fn log_capture_layer<S>() -> impl tracing_subscriber::Layer<S>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_level(true)
        .with_ansi(false)
        .compact()
        .with_writer(LogCaptureWriter)
        .with_filter(tracing_subscriber::filter::filter_fn(|_| {
            LOG_CAPTURE_ARMED.load(Ordering::SeqCst)
        }))
}

/// Release builds skip the capture layer entirely so callsite caching stays
/// intact on hot log paths.
#[cfg(not(debug_assertions))]
fn log_capture_layer() -> tracing_subscriber::layer::Identity {
    tracing_subscriber::layer::Identity::new()
}

/// Initialize tracing once based on config and environment:
/// - If `SILO_DEBUG_LOG_FILE` is set, also write debug logs to that file
/// - If `SILO_PERFETTO` is set, enable Perfetto export to that file
/// - else if `OTEL_EXPORTER_OTLP_ENDPOINT` is set, export OTLP traces
/// - otherwise install only the fmt layer
///
/// The `log_format` parameter controls whether logs are output as human-readable
/// text (default) or structured JSON.
pub fn init(log_format: LogFormat) -> anyhow::Result<()> {
    let mut init_result: Option<anyhow::Result<()>> = None;
    INIT.call_once(|| {
        // Install the W3C Trace Context propagator regardless of whether OTLP
        // export is configured. This is what lets the gRPC trace layer extract
        // `traceparent` from incoming requests and link silo spans to the
        // caller's trace. With no propagator installed, `extract` returns an
        // empty context and the layer is a no-op.
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        let result = {
            let env_filter = build_env_filter();

            // Check if debug file logging is requested
            let debug_file = std::env::var("SILO_DEBUG_LOG_FILE").ok().and_then(|path| {
                match std::fs::File::create(&path) {
                    Ok(file) => {
                        eprintln!("Debug logs will be written to: {}", path);
                        Some(DebugFileWriter {
                            file: Arc::new(Mutex::new(file)),
                        })
                    }
                    Err(e) => {
                        eprintln!("Failed to create debug log file {}: {}", path, e);
                        None
                    }
                }
            });

            match (log_format, debug_file) {
                (LogFormat::Text, Some(file_writer)) => {
                    let fmt_layer = tracing_subscriber::fmt::layer()
                        .with_target(true)
                        .with_level(true)
                        .compact()
                        .with_filter(env_filter);
                    init_with_debug_file(fmt_layer, file_writer)
                }
                (LogFormat::Text, None) => {
                    let fmt_layer = tracing_subscriber::fmt::layer()
                        .with_target(true)
                        .with_level(true)
                        .compact()
                        .with_filter(env_filter);
                    init_fmt_only(fmt_layer)
                }
                (LogFormat::Json, Some(file_writer)) => {
                    let fmt_layer = tracing_subscriber::fmt::layer()
                        .with_target(true)
                        .with_level(true)
                        .json()
                        .with_filter(env_filter);
                    init_with_debug_file(fmt_layer, file_writer)
                }
                (LogFormat::Json, None) => {
                    let fmt_layer = tracing_subscriber::fmt::layer()
                        .with_target(true)
                        .with_level(true)
                        .json()
                        .with_filter(env_filter);
                    init_fmt_only(fmt_layer)
                }
            }
        };
        init_result = Some(result);
    });
    if let Some(res) = init_result {
        res
    } else {
        Ok(())
    }
}

fn init_fmt_only<L>(fmt_layer: L) -> anyhow::Result<()>
where
    L: tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
{
    let base = tracing_subscriber::registry()
        .with(fmt_layer)
        .with(log_capture_layer())
        .with(console_layer());

    if let Some(path) = std::env::var_os("SILO_PERFETTO") {
        let file = std::fs::File::create(path)?;
        let perfetto_layer =
            tracing_perfetto::PerfettoLayer::new(Mutex::new(file)).with_filter(LevelFilter::DEBUG);
        base.with(perfetto_layer).init();
    } else if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        let resource = Resource::new(vec![KeyValue::new("service.name", "silo")]);
        match opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .http()
                    .with_endpoint(endpoint),
            )
            .with_trace_config(sdktrace::Config::default().with_resource(resource))
            .install_batch(runtime::Tokio)
        {
            Ok(provider) => {
                let tracer = provider.tracer("silo");
                // Filter spans sent to OTLP independently from console logs.
                // Without this, the otel layer captures all levels and floods
                // the batch processor's bounded channel.
                let otel_filter = EnvFilter::try_from_env("SILO_OTEL_LOG")
                    .unwrap_or_else(|_| EnvFilter::new("info"));
                let otel_layer = tracing_opentelemetry::layer()
                    .with_tracer(tracer)
                    .with_filter(otel_filter);
                base.with(otel_layer).init();
            }
            Err(err) => {
                eprintln!("otlp init failed, falling back to fmt: {err}");
                base.init();
            }
        }
    } else {
        base.init();
    }
    Ok(())
}

fn init_with_debug_file<L>(fmt_layer: L, file_writer: DebugFileWriter) -> anyhow::Result<()>
where
    L: tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
{
    // The file layer writes debug+ logs for silo crate
    let file_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_level(true)
        .with_ansi(false)
        .with_writer(file_writer)
        .with_filter(EnvFilter::new("silo=debug,info"));

    let base = tracing_subscriber::registry()
        .with(fmt_layer)
        .with(file_layer)
        .with(log_capture_layer())
        .with(console_layer());

    // Note: Perfetto and OTEL are not supported when using debug file logging
    // to keep the type system manageable. This is fine for CI debugging use case.
    base.init();
    Ok(())
}

/// Flush OTLP exporter if configured.
pub fn shutdown() {
    opentelemetry::global::shutdown_tracer_provider();
}

/// Run an async test body with a per-test tracing subscriber.
/// If `SILO_PERFETTO` is set, writes Perfetto to that file; otherwise, if
/// `SILO_PERFETTO_DIR` is set, writes a unique `<test>-<nanos>.pftrace` file
/// into that directory. Falls back to fmt-only logs when file creation fails
/// or when neither env var is set.
pub async fn with_test_tracing<F, Fut, T>(_test_name: &str, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    init(LogFormat::Text).unwrap();
    f().await
}

/// Run a sync test body with a per-test tracing subscriber.
/// This is the synchronous equivalent of `with_test_tracing`.
pub fn with_test_tracing_sync<F, T>(_test_name: &str, f: F) -> T
where
    F: FnOnce() -> T,
{
    init(LogFormat::Text).unwrap();
    f()
}
