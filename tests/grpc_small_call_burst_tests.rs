mod grpc_integration_helpers;

use std::collections::BTreeMap;
use std::time::Duration;

use grpc_integration_helpers::{create_test_factory, setup_test_server, shutdown_server};
use silo::settings::AppConfig;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

/// Health checks fired at once down the one connection. h2 0.4.20 lets 8,355 unread
/// 5-byte request frames sit on the server's 4 MiB connection window. The margin above
/// that covers the handlers that do run early: at 12,000 calls 1 run in 20 still passed
/// on h2 0.4.20, at 15,000 and above none did.
const BURST: usize = 20_000;

/// A burst of small unary calls on one HTTP/2 connection must all be served.
///
/// This is a tripwire for upgrading the `h2` crate, which Silo only gets through tonic
/// and hyper. h2 0.4.16 and later charge every received DATA frame under 256 bytes that
/// is not the last frame of its stream against a per-connection budget, until the handler
/// reads the frame. A unary gRPC request is one such frame. The budget is half the
/// connection receive window, so 2 MiB for the 4 MiB window `run_server_with_incoming`
/// sets, and neither hyper nor tonic exposes h2's `data_frame_budget()` to change it.
/// When the budget runs out h2 closes the whole connection with
/// GOAWAY(ENHANCE_YOUR_CALM, "too_many_data_frames"), and every call in flight on it
/// fails, usually as "connection reset" or "broken pipe" on the client.
///
/// If this fails right after a dependency update, the update moved h2 across that line.
/// Either hold h2 back (`cargo update -p h2 --precise <version>`) or accept the new limit
/// on purpose and change this test with it.
///
/// Plain `tokio::test` because the tracing subscriber `silo::test` installs makes each
/// call about four times slower in a debug build. Keep the multi-thread flavor: on a
/// `current_thread` runtime the handlers keep up with the calls and the same burst passes
/// on h2 0.4.20.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn grpc_burst_of_small_calls_on_one_connection_all_succeed() -> anyhow::Result<()> {
    let (factory, _tmp) = create_test_factory().await?;
    let (_client, shutdown_tx, server, addr) =
        setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

    // One Channel is one HTTP/2 connection.
    let channel = tonic::transport::Endpoint::new(format!("http://{addr}"))?
        .connect()
        .await?;
    let client = HealthClient::new(channel);

    // Fire the burst from a task on the worker thread, so every call is queued before the
    // worker runs any of them and the server's handlers queue up behind the calls. Fired
    // from the test thread, the calls trickle in while the worker is already serving them
    // and the burst no longer reaches h2's limit reliably.
    let burst = tokio::spawn(async move {
        let calls: Vec<_> = (0..BURST)
            .map(|_| {
                let mut client = client.clone();
                tokio::spawn(async move {
                    client
                        .check(HealthCheckRequest {
                            service: String::new(),
                        })
                        .await
                })
            })
            .collect();

        let mut ok = 0usize;
        let mut errors: BTreeMap<String, usize> = BTreeMap::new();
        for call in calls {
            match call.await.expect("call task panicked") {
                Ok(_) => ok += 1,
                Err(status) => {
                    // Walk the source chain so the transport error under the status shows.
                    let mut error = format!("{:?}: {}", status.code(), status.message());
                    let mut source = std::error::Error::source(&status);
                    while let Some(e) = source {
                        error.push_str(&format!(" <- {e}"));
                        source = e.source();
                    }
                    *errors.entry(error).or_default() += 1;
                }
            }
        }
        (ok, errors)
    });
    let (ok, errors) = tokio::time::timeout(Duration::from_secs(120), burst)
        .await
        .expect("burst timed out")?;

    shutdown_server(shutdown_tx, server).await?;

    assert!(
        errors.is_empty(),
        "{ok} of {BURST} calls succeeded, the rest failed with {errors:?}. \
         If h2 was just upgraded, see the comment on this test."
    );
    Ok(())
}
