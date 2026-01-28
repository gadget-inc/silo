use std::sync::{Mutex, Once};

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, runtime, trace as sdktrace};
use tracing_subscriber::{EnvFilter, filter::LevelFilter, prelude::*};

use crate::settings::LogFormat;

static INIT: Once = Once::new();

fn build_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Initialize tracing once based on config and environment:
/// - If `SILO_PERFETTO` is set, enable Perfetto export to that file
/// - else if `OTEL_EXPORTER_OTLP_ENDPOINT` is set, export OTLP traces
/// - otherwise install only the fmt layer
///
/// The `log_format` parameter controls whether logs are output as human-readable
/// text (default) or structured JSON.
pub fn init(log_format: LogFormat) -> anyhow::Result<()> {
    let mut init_result: Option<anyhow::Result<()>> = None;
    INIT.call_once(|| {
        let result = {
            let env_filter = build_env_filter();

            match log_format {
                LogFormat::Text => {
                    let fmt_layer = tracing_subscriber::fmt::layer()
                        .with_target(true)
                        .with_level(true)
                        .compact()
                        .with_filter(env_filter);
                    init_with_fmt_layer(fmt_layer)
                }
                LogFormat::Json => {
                    let fmt_layer = tracing_subscriber::fmt::layer()
                        .with_target(true)
                        .with_level(true)
                        .json()
                        .with_filter(env_filter);
                    init_with_fmt_layer(fmt_layer)
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

fn init_with_fmt_layer<L>(fmt_layer: L) -> anyhow::Result<()>
where
    L: tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
{
    let base = tracing_subscriber::registry().with(fmt_layer);

    if let Some(path) = std::env::var_os("SILO_PERFETTO") {
        let file = std::fs::File::create(path)?;
        let perfetto_layer =
            tracing_perfetto::PerfettoLayer::new(Mutex::new(file)).with_filter(LevelFilter::DEBUG);
        base.with(perfetto_layer).init();
    } else if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        match opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .http()
                    .with_endpoint(endpoint),
            )
            .with_trace_config(
                sdktrace::Config::default()
                    .with_resource(Resource::new(vec![KeyValue::new("service.name", "silo")])),
            )
            .install_batch(runtime::Tokio)
        {
            Ok(provider) => {
                let tracer = provider.tracer("silo");
                let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
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
