//! Tests for `silo_grpc_requests_in_flight`, the gauge `GrpcMetricsLayer`
//! holds up while a gRPC call is being handled.
//!
//! The first group drives the layer directly with a stand-in inner service, so
//! "the call is open" and "the call was dropped" are exact moments. The second
//! group runs the layer under a real tonic server, because the case that
//! matters most (a client cancelling a call) depends on hyper dropping the
//! handler future. The last test checks the layer as `run_server` wires it.

mod grpc_integration_helpers;
mod test_helpers;

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use grpc_integration_helpers::{
    create_test_factory, setup_test_server_with_metrics, shutdown_server,
};
use silo::metrics::{self, GrpcMetricsLayer, Metrics};
use silo::pb::GetClusterInfoRequest;
use silo::settings::AppConfig;
use test_helpers::{extract_metric_value, gather_metrics_text, metric_value_or_zero, poll_until};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codegen::http::{Request, Response};
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::health_server::{Health, HealthServer};
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};
use tower::{Layer, Service, ServiceExt};

/// Current `silo_grpc_requests_in_flight` for a method; 0 when no call to it
/// has been seen yet.
fn in_flight(metrics: &Metrics, method: &str) -> f64 {
    metric_value_or_zero(
        &gather_metrics_text(metrics),
        &[
            "silo_grpc_requests_in_flight{",
            &format!("method=\"{method}\""),
        ],
    )
}

/// `silo_grpc_requests_total` for a method and status; 0 when none was counted.
fn requests_total(metrics: &Metrics, method: &str, status: &str) -> f64 {
    metric_value_or_zero(
        &gather_metrics_text(metrics),
        &[
            "silo_grpc_requests_total{",
            &format!("method=\"{method}\""),
            &format!("status=\"{status}\""),
        ],
    )
}

/// Calls to a method that `silo_grpc_requests_total` has counted under any
/// status.
fn requests_total_any_status(metrics: &Metrics, method: &str) -> f64 {
    let method_label = format!("method=\"{method}\"");
    gather_metrics_text(metrics)
        .lines()
        .filter(|l| l.starts_with("silo_grpc_requests_total{") && l.contains(&method_label))
        .filter_map(|l| l.rsplit_once(' ')?.1.parse::<f64>().ok())
        .sum()
}

/// Observations `silo_grpc_request_duration_seconds` holds for a method.
fn durations_recorded(metrics: &Metrics, method: &str) -> f64 {
    metric_value_or_zero(
        &gather_metrics_text(metrics),
        &[
            "silo_grpc_request_duration_seconds_count{",
            &format!("method=\"{method}\""),
        ],
    )
}

/// Fail the test instead of hanging it when something that should happen
/// promptly never does.
async fn within<T>(what: &str, fut: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), fut)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

/// Wait until `n` calls have reached a held handler.
async fn entered(sem: &Semaphore, n: u32) {
    within("the handler to start", sem.acquire_many(n))
        .await
        .expect("semaphore open")
        .forget();
}

fn request(method: &str) -> Request<()> {
    Request::builder()
        .uri(format!("/silo.v1.Silo/{method}"))
        .body(())
        .unwrap()
}

/// Stand-in for the tonic router beneath the layer. Each call adds a permit to
/// `entered`, then waits for one from `release` before answering OK. A call
/// that is never released is a handler that hangs.
#[derive(Clone)]
struct HeldService {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

impl HeldService {
    fn new() -> Self {
        Self {
            entered: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
        }
    }
}

impl Service<Request<()>> for HeldService {
    type Response = Response<()>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response<()>, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Request<()>) -> Self::Future {
        let (entered, release) = (self.entered.clone(), self.release.clone());
        Box::pin(async move {
            entered.add_permits(1);
            release.acquire().await.expect("semaphore open").forget();
            Ok(Response::new(()))
        })
    }
}

#[silo::test]
async fn in_flight_gauge_is_one_while_a_call_is_held_and_zero_after_it_returns() {
    let metrics = metrics::init().expect("init metrics");
    let held = HeldService::new();
    let svc = GrpcMetricsLayer::new(Some(metrics.clone())).layer(held.clone());

    let mut call = Box::pin(svc.oneshot(request("Enqueue")));
    assert!(futures::poll!(&mut call).is_pending());

    // The call is open. The gauge sees it; the counter and the histogram only
    // hear about a call when it ends, so they still show nothing.
    assert_eq!(in_flight(&metrics, "Enqueue"), 1.0);
    assert_eq!(requests_total_any_status(&metrics, "Enqueue"), 0.0);
    assert_eq!(durations_recorded(&metrics, "Enqueue"), 0.0);

    held.release.add_permits(1);
    call.await.expect("call returns");

    assert_eq!(in_flight(&metrics, "Enqueue"), 0.0);
    assert_eq!(requests_total(&metrics, "Enqueue", "OK"), 1.0);
    assert_eq!(durations_recorded(&metrics, "Enqueue"), 1.0);
}

#[silo::test]
async fn in_flight_gauge_returns_to_zero_when_the_call_is_dropped_mid_flight() {
    let metrics = metrics::init().expect("init metrics");
    let held = HeldService::new();
    let svc = GrpcMetricsLayer::new(Some(metrics.clone())).layer(held.clone());

    let mut call = Box::pin(svc.oneshot(request("Enqueue")));
    assert!(futures::poll!(&mut call).is_pending());
    assert_eq!(in_flight(&metrics, "Enqueue"), 1.0);

    // What hyper does when the client cancels: drop the future unfinished.
    drop(call);

    assert_eq!(in_flight(&metrics, "Enqueue"), 0.0);
    // A cancelled call never reaches the code after the await, so, as before
    // the gauge existed, neither the counter nor the histogram records it.
    assert_eq!(requests_total_any_status(&metrics, "Enqueue"), 0.0);
    assert_eq!(durations_recorded(&metrics, "Enqueue"), 0.0);
}

#[silo::test]
async fn in_flight_gauge_counts_concurrent_calls_per_method() {
    let metrics = metrics::init().expect("init metrics");
    let held = HeldService::new();
    let svc = GrpcMetricsLayer::new(Some(metrics.clone())).layer(held.clone());

    let mut enqueues: Vec<_> = (0..3)
        .map(|_| Box::pin(svc.clone().oneshot(request("Enqueue"))))
        .collect();
    let mut heartbeat = Box::pin(svc.clone().oneshot(request("Heartbeat")));
    for call in &mut enqueues {
        assert!(futures::poll!(call).is_pending());
    }
    assert!(futures::poll!(&mut heartbeat).is_pending());

    assert_eq!(in_flight(&metrics, "Enqueue"), 3.0);
    assert_eq!(in_flight(&metrics, "Heartbeat"), 1.0);

    // One Enqueue is cancelled and one returns. Each takes exactly one off its
    // own method and leaves the other method alone.
    drop(enqueues.pop());
    assert_eq!(in_flight(&metrics, "Enqueue"), 2.0);

    // The semaphore hands permits out in the order the calls started waiting,
    // so this one goes to the first Enqueue.
    held.release.add_permits(1);
    enqueues.remove(0).await.expect("call returns");
    assert_eq!(in_flight(&metrics, "Enqueue"), 1.0);
    assert_eq!(in_flight(&metrics, "Heartbeat"), 1.0);

    held.release.add_permits(2);
    enqueues.remove(0).await.expect("call returns");
    heartbeat.await.expect("call returns");
    assert_eq!(in_flight(&metrics, "Enqueue"), 0.0);
    assert_eq!(in_flight(&metrics, "Heartbeat"), 0.0);
    assert_eq!(requests_total(&metrics, "Enqueue", "OK"), 2.0);
    assert_eq!(requests_total(&metrics, "Heartbeat", "OK"), 1.0);
}

#[silo::test]
async fn counter_and_histogram_still_record_each_finished_call() {
    let metrics = metrics::init().expect("init metrics");
    // Answers at once: `GetJob` with a NOT_FOUND `grpc-status` header, `Fail`
    // with an error from the service itself, anything else with a plain OK.
    let inner = tower::service_fn(|req: Request<()>| async move {
        match req.uri().path() {
            "/silo.v1.Silo/GetJob" => Ok(Response::builder()
                .header("grpc-status", "5")
                .body(())
                .unwrap()),
            "/silo.v1.Silo/Fail" => Err("inner service failed"),
            _ => Ok(Response::new(())),
        }
    });
    let svc = GrpcMetricsLayer::new(Some(metrics.clone())).layer(inner);

    svc.clone().oneshot(request("Enqueue")).await.unwrap();
    svc.clone().oneshot(request("Enqueue")).await.unwrap();
    svc.clone().oneshot(request("GetJob")).await.unwrap();
    svc.clone().oneshot(request("Fail")).await.unwrap_err();

    assert_eq!(requests_total(&metrics, "Enqueue", "OK"), 2.0);
    assert_eq!(requests_total(&metrics, "GetJob", "NOT_FOUND"), 1.0);
    assert_eq!(requests_total(&metrics, "Fail", "INTERNAL"), 1.0);
    assert_eq!(durations_recorded(&metrics, "Enqueue"), 2.0);
    assert_eq!(durations_recorded(&metrics, "GetJob"), 1.0);
    assert_eq!(durations_recorded(&metrics, "Fail"), 1.0);

    // Every way of finishing gives the gauge back, the error path included.
    // `extract_metric_value` panics on a missing series, so this also checks
    // that each call was counted in flight at all.
    let text = gather_metrics_text(&metrics);
    for method in ["Enqueue", "GetJob", "Fail"] {
        let value = extract_metric_value(
            &text,
            &[
                "silo_grpc_requests_in_flight{",
                &format!("method=\"{method}\""),
            ],
        );
        assert_eq!(value, 0.0, "{method} still counted in flight");
    }
}

#[silo::test]
async fn layer_without_metrics_passes_calls_through() {
    let inner = tower::service_fn(|_req: Request<()>| async {
        Ok::<_, Infallible>(
            Response::builder()
                .header("grpc-status", "5")
                .body(())
                .unwrap(),
        )
    });
    let svc = GrpcMetricsLayer::new(None).layer(inner);

    let resp = svc.oneshot(request("Enqueue")).await.unwrap();
    assert_eq!(resp.headers()["grpc-status"], "5");
}

/// A gRPC health service whose `Check` holds every call until the test
/// releases it. It gives the tests a handler that hangs behind a real tonic
/// server without implementing all of Silo's RPCs.
struct HeldHealth {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

#[tonic::async_trait]
impl Health for HeldHealth {
    type WatchStream = futures::stream::Empty<Result<HealthCheckResponse, tonic::Status>>;

    async fn check(
        &self,
        _request: tonic::Request<HealthCheckRequest>,
    ) -> Result<tonic::Response<HealthCheckResponse>, tonic::Status> {
        self.entered.add_permits(1);
        self.release
            .acquire()
            .await
            .expect("semaphore open")
            .forget();
        Ok(tonic::Response::new(HealthCheckResponse {
            status: tonic_health::pb::health_check_response::ServingStatus::Serving as i32,
        }))
    }

    async fn watch(
        &self,
        _request: tonic::Request<HealthCheckRequest>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("watch"))
    }
}

struct HeldServer {
    client: HealthClient<tonic::transport::Channel>,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl HeldServer {
    async fn shutdown(self) -> anyhow::Result<()> {
        drop(self.client);
        let _ = self.shutdown_tx.send(());
        within("the server to stop", self.server).await??;
        Ok(())
    }
}

/// Serve `HeldHealth` behind `GrpcMetricsLayer` on a real tonic server, so a
/// call travels over HTTP/2 through hyper as it does in `run_server`.
async fn serve_held_health(metrics: Metrics) -> anyhow::Result<HeldServer> {
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .layer(GrpcMetricsLayer::new(Some(metrics)))
            .add_service(HealthServer::new(HeldHealth {
                entered: entered.clone(),
                release: release.clone(),
            }))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            }),
    );

    let channel = tonic::transport::Endpoint::new(format!("http://{addr}"))?
        .connect()
        .await?;
    Ok(HeldServer {
        client: HealthClient::new(channel),
        entered,
        release,
        shutdown_tx,
        server,
    })
}

#[silo::test(flavor = "multi_thread")]
async fn server_in_flight_gauge_is_one_while_a_call_is_open_and_zero_after_it_returns()
-> anyhow::Result<()> {
    let metrics = metrics::init()?;
    let held = serve_held_health(metrics.clone()).await?;

    let mut client = held.client.clone();
    let call = tokio::spawn(async move {
        client
            .check(HealthCheckRequest {
                service: String::new(),
            })
            .await
    });

    entered(&held.entered, 1).await;
    assert_eq!(in_flight(&metrics, "Check"), 1.0);
    assert_eq!(requests_total_any_status(&metrics, "Check"), 0.0);
    assert_eq!(durations_recorded(&metrics, "Check"), 0.0);

    held.release.add_permits(1);
    within("the call to return", call).await??;

    // The layer gives the gauge back before it hands the response to hyper, so
    // by the time the client has its answer there is nothing left to wait for.
    assert_eq!(in_flight(&metrics, "Check"), 0.0);
    assert_eq!(requests_total(&metrics, "Check", "OK"), 1.0);
    assert_eq!(durations_recorded(&metrics, "Check"), 1.0);

    held.shutdown().await
}

#[silo::test(flavor = "multi_thread")]
async fn server_in_flight_gauge_returns_to_zero_when_the_client_cancels() -> anyhow::Result<()> {
    let metrics = metrics::init()?;
    let held = serve_held_health(metrics.clone()).await?;

    let mut client = held.client.clone();
    let call = tokio::spawn(async move {
        client
            .check(HealthCheckRequest {
                service: String::new(),
            })
            .await
    });

    entered(&held.entered, 1).await;
    assert_eq!(in_flight(&metrics, "Check"), 1.0);

    // Cancel from the client. Dropping the call's future makes tonic reset the
    // HTTP/2 stream, and the server finds out a moment later.
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());

    let after = poll_until(
        || async { in_flight(&metrics, "Check") },
        |v| *v == 0.0,
        5_000,
    )
    .await;
    assert_eq!(after, 0.0, "a cancelled call is still counted in flight");

    // The handler was never released, so it cannot have returned: the server
    // dropped it. That is why the counter and histogram have nothing, and why
    // a decrement placed after the await would have left the gauge at 1.
    assert_eq!(held.release.available_permits(), 0);
    assert_eq!(requests_total_any_status(&metrics, "Check"), 0.0);
    assert_eq!(durations_recorded(&metrics, "Check"), 0.0);

    held.shutdown().await
}

#[silo::test(flavor = "multi_thread")]
async fn silo_server_counts_its_calls_in_flight() -> anyhow::Result<()> {
    let metrics = metrics::init()?;
    let (factory, _tmp) = create_test_factory().await?;
    let (mut client, shutdown_tx, server, _addr) = setup_test_server_with_metrics(
        factory,
        AppConfig::load(None).unwrap(),
        Some(metrics.clone()),
    )
    .await?;

    within(
        "GetClusterInfo",
        client.get_cluster_info(GetClusterInfoRequest {}),
    )
    .await?;

    // The series exists only once the layer in `run_server` has counted a call
    // in, and reads 0 only if it counted it back out.
    let text = gather_metrics_text(&metrics);
    assert_eq!(
        extract_metric_value(
            &text,
            &["silo_grpc_requests_in_flight{", "method=\"GetClusterInfo\""],
        ),
        0.0
    );
    assert_eq!(requests_total(&metrics, "GetClusterInfo", "OK"), 1.0);

    shutdown_server(shutdown_tx, server).await
}
