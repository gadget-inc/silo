//! Tower layer that extracts W3C Trace Context (`traceparent` / `tracestate`)
//! from incoming gRPC request headers and attaches it as the parent of the
//! request's tracing span.
//!
//! When workers propagate `traceparent` on their RPCs, every `tracing` event
//! emitted while handling that request will carry the worker's `trace_id`,
//! letting a single trace stitch together worker-side and silo-side activity.
//!
//! The layer is a no-op when no propagator is installed or when the incoming
//! request has no `traceparent`. Pair with `trace::install_propagator()` to
//! enable W3C Trace Context globally.
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use opentelemetry::propagation::Extractor;
use tonic::codegen::http::{HeaderMap, HeaderValue};
use tower::{Layer, Service};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// `Extractor` view over `http::HeaderMap` so the OTel propagator can pull
/// `traceparent` / `tracestate` straight from a tonic request.
struct HeaderMapExtractor<'a>(&'a HeaderMap<HeaderValue>);

impl<'a> Extractor for HeaderMapExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

#[derive(Clone, Default)]
pub struct GrpcTraceLayer;

impl GrpcTraceLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for GrpcTraceLayer {
    type Service = GrpcTraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcTraceService { inner }
    }
}

#[derive(Clone)]
pub struct GrpcTraceService<S> {
    inner: S,
}

type HttpRequest<T> = tonic::codegen::http::Request<T>;
type HttpResponse<T> = tonic::codegen::http::Response<T>;

impl<S, ReqBody, ResBody> Service<HttpRequest<ReqBody>> for GrpcTraceService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = HttpResponse<ResBody>> + Clone + Send + 'static,
    <S as Service<HttpRequest<ReqBody>>>::Future: Send + 'static,
    <S as Service<HttpRequest<ReqBody>>>::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = HttpResponse<ResBody>;
    type Error = <S as Service<HttpRequest<ReqBody>>>::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        // Pull the parent context out of the headers. If no propagator is
        // installed or no traceparent was sent, this returns an empty Context
        // and `set_parent` is a no-op below.
        let parent_cx = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&HeaderMapExtractor(req.headers()))
        });

        // The method is the last path segment, e.g. `/silo.v1.Silo/LeaseTasks` -> `LeaseTasks`.
        let method = req
            .uri()
            .path()
            .rsplit_once('/')
            .map(|(_, m)| m.to_string())
            .unwrap_or_else(|| req.uri().path().to_string());

        let span = tracing::info_span!("grpc_request", rpc.method = %method);
        span.set_parent(parent_cx);

        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move { inner.call(req).await }.instrument(span))
    }
}
