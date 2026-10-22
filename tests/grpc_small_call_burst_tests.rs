mod grpc_integration_helpers;

use std::collections::BTreeMap;
use std::time::Duration;

use grpc_integration_helpers::{create_test_factory, setup_test_server, shutdown_server};
use silo::settings::AppConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

/// Health checks fired at once down the one connection. h2 0.4.20 lets 8,355 unread
/// 5-byte request frames sit on the server's 4 MiB connection window. The margin above
/// that covers the handlers that do run early: at 12,000 calls 1 run in 20 still passed
/// on h2 0.4.20, at 15,000 and above none did.
const BURST: usize = 20_000;

/// A burst of small unary calls on one HTTP/2 connection must all be served.
///
/// h2 0.4.16 and later charge every received DATA frame under 256 bytes that is not the
/// last frame of its stream against a per-connection budget, until the handler reads the
/// frame. A unary gRPC request is one such frame. The budget is half the connection
/// receive window, so 2 MiB for the 4 MiB window `run_server_with_incoming` sets, and
/// neither hyper nor tonic exposes h2's `data_frame_budget()` to change it. When the
/// budget runs out h2 closes the whole connection with
/// GOAWAY(ENHANCE_YOUR_CALM, "too_many_data_frames"), and every call in flight on it
/// fails, usually as "connection reset" or "broken pipe" on the client.
///
/// `run_server_with_incoming` keeps a burst inside that budget with
/// `max_concurrent_streams`: the client may have that many calls in flight and queues
/// the rest itself. With the limit removed this test failed 10 of 10 runs on h2 0.4.20.
/// h2 0.4.15 and older have no budget, so there the test passes with or without the
/// limit, and `server_stream_limit_fits_h2_small_frame_budget` below is the check that
/// the limit is still in place.
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

/// What h2 charges for one unread 5-byte request, the smallest a gRPC client sends: 256
/// minus the frame's length.
const SMALLEST_REQUEST_COST: u64 = 256 - 5;

/// The stream limit the server advertises must keep a connection's worth of unread small
/// requests inside h2's budget, which is half the connection window (see the test above).
/// It must also be at least 100. Until a client has read the server's SETTINGS frame it
/// assumes it may open 100 streams (hyper and Node both do), and h2 refuses any stream
/// over the limit.
///
/// The handshake is read off a bare TCP socket because tonic's client does not expose the
/// settings it received.
#[tokio::test]
async fn server_stream_limit_fits_h2_small_frame_budget() -> anyhow::Result<()> {
    const SETTINGS: u8 = 0x4;
    const PING: u8 = 0x6;
    const WINDOW_UPDATE: u8 = 0x8;
    const ACK: u8 = 0x1;
    const SETTINGS_MAX_CONCURRENT_STREAMS: u16 = 0x3;

    let (factory, _tmp) = create_test_factory().await?;
    let (_client, shutdown_tx, server, addr) =
        setup_test_server(factory.clone(), AppConfig::load(None).unwrap()).await?;

    let mut tcp = TcpStream::connect(addr).await?;
    tcp.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").await?;
    tcp.write_all(&[0, 0, 0, SETTINGS, 0, 0, 0, 0, 0]).await?;

    let mut max_streams: Option<u32> = None;
    // The HTTP/2 default, which the server raises with a WINDOW_UPDATE on stream 0.
    let mut connection_window: u64 = 65_535;
    let mut window_raised = false;
    let handshake = async {
        // h2 sends that WINDOW_UPDATE once it has nothing left to read, which can be
        // after it answers the first PING. A few PING round trips with no WINDOW_UPDATE
        // mean the server left the window at the default.
        for _ in 0..3 {
            tcp.write_all(&[0, 0, 8, PING, 0, 0, 0, 0, 0]).await?;
            tcp.write_all(&[0; 8]).await?;
            loop {
                let mut head = [0u8; 9];
                tcp.read_exact(&mut head).await?;
                let len = u32::from_be_bytes([0, head[0], head[1], head[2]]) as usize;
                let (kind, flags) = (head[3], head[4]);
                let stream_id = u32::from_be_bytes([head[5], head[6], head[7], head[8]]);
                let mut payload = vec![0u8; len];
                tcp.read_exact(&mut payload).await?;
                match kind {
                    SETTINGS if flags & ACK == 0 => {
                        for entry in payload.chunks_exact(6) {
                            if u16::from_be_bytes([entry[0], entry[1]])
                                == SETTINGS_MAX_CONCURRENT_STREAMS
                            {
                                max_streams = Some(u32::from_be_bytes([
                                    entry[2], entry[3], entry[4], entry[5],
                                ]));
                            }
                        }
                    }
                    WINDOW_UPDATE if stream_id == 0 => {
                        let increment =
                            u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                        connection_window += u64::from(increment & 0x7fff_ffff);
                        window_raised = true;
                    }
                    PING if flags & ACK != 0 => break,
                    _ => {}
                }
            }
            if window_raised {
                break;
            }
        }
        anyhow::Ok(())
    };
    tokio::time::timeout(Duration::from_secs(10), handshake)
        .await
        .expect("server handshake timed out")?;
    drop(tcp);

    shutdown_server(shutdown_tx, server).await?;

    let max_streams =
        u64::from(max_streams.expect("the server advertised no SETTINGS_MAX_CONCURRENT_STREAMS"));
    assert!(
        max_streams >= 100,
        "a stream limit of {max_streams} is below the 100 streams clients assume before \
         they have read the server's SETTINGS"
    );
    let budget = connection_window / 2;
    assert!(
        max_streams * SMALLEST_REQUEST_COST <= budget,
        "{max_streams} unread 5-byte requests cost {} against h2's small-frame budget of \
         {budget} (half the {connection_window}-byte connection window)",
        max_streams * SMALLEST_REQUEST_COST
    );
    Ok(())
}
