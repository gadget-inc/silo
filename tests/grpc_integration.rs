use std::net::SocketAddr;
use std::sync::Arc;

use silo::factory::ShardFactory;
use silo::pb::silo_client::SiloClient;
use silo::pb::*;
use silo::server::run_grpc_with_reaper;
use silo::settings::{Backend, DatabaseTemplate};
use tokio::net::TcpListener;

// Integration test that boots the real gRPC server and talks to it over TCP.
#[tokio::test(flavor = "multi_thread")] // multi_thread to match server expectations
async fn grpc_server_enqueue_and_workflow() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        // Temp storage per test
        let tmp = tempfile::tempdir()?;

        // Create a shard factory with a path template pointing inside tmpdir
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
        };
        let mut factory = ShardFactory::new(template);
        // Open shard 0 so the server can find it
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        // Bind an ephemeral port
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;

        // Shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        // Start server
        let server = tokio::spawn(run_grpc_with_reaper(listener, factory.clone(), shutdown_rx));

        // Connect real client
        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Enqueue a job
        let payload = serde_json::json!({ "hello": "world" });
        let payload_bytes = serde_json::to_vec(&payload)?;
        let mut md = std::collections::HashMap::new();
        md.insert("env".to_string(), "test".to_string());
        let enq = EnqueueRequest {
            shard: "0".to_string(),
            id: "".to_string(),
            priority: 10,
            start_at_ms: 0,
            retry_policy: None,
            payload: Some(JsonValueBytes {
                data: payload_bytes.clone(),
            }),
            concurrency_limits: vec![],
            tenant: None,
            metadata: md,
        };
        let enq_resp = client.enqueue(enq).await?.into_inner();
        let job_id = enq_resp.id;

        // Lease a task
        let lease_resp = client
            .lease_tasks(LeaseTasksRequest {
                shard: "0".to_string(),
                worker_id: "w1".to_string(),
                max_tasks: 1,
                tenant: None,
            })
            .await?
            .into_inner();
        assert_eq!(lease_resp.tasks.len(), 1, "expected one leased task");
        let task = &lease_resp.tasks[0];
        assert_eq!(task.job_id, job_id);
        assert_eq!(
            task.payload.as_ref().map(|p| p.data.clone()),
            Some(payload_bytes.clone())
        );

        // Report success
        let _ = client
            .report_outcome(ReportOutcomeRequest {
                shard: "0".to_string(),
                task_id: task.id.clone(),
                tenant: None,
                outcome: Some(report_outcome_request::Outcome::Success(JsonValueBytes {
                    data: b"{}".to_vec(),
                })),
            })
            .await?;

        // After success, deleting job should work or get_job should be not found eventually.
        // First try get_job to ensure job exists pre-delete
        let _ = client
            .get_job(GetJobRequest {
                shard: "0".to_string(),
                id: job_id.clone(),
                tenant: None,
            })
            .await?;
        // Verify metadata via get_job
        let got = client
            .get_job(GetJobRequest {
                shard: "0".to_string(),
                id: job_id.clone(),
                tenant: None,
            })
            .await?
            .into_inner();
        assert!(got.metadata.iter().any(|(k, v)| k == "env" && v == "test"));
        // Now delete it
        let _ = client
            .delete_job(DeleteJobRequest {
                shard: "0".to_string(),
                id: job_id.clone(),
                tenant: None,
            })
            .await?;

        // Shut down server gracefully
        let _ = shutdown_tx.send(());
        // server is JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>
        // Await join, then unwrap inner result explicitly to avoid From conversion issues
        let join_result = server.await; // Result<Result<(), Box<dyn ..>>, JoinError>
        match join_result {
            Ok(inner) => {
                // Propagate inner error if server returned Err
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => {
                return Err(anyhow::anyhow!(e));
            }
        }

        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")] // multi_thread to match server expectations
async fn grpc_server_metadata_validation_errors() -> anyhow::Result<()> {
    let _guard = tokio::time::timeout(std::time::Duration::from_millis(5000), async {
        let tmp = tempfile::tempdir()?;
        let template = DatabaseTemplate {
            backend: Backend::Fs,
            path: tmp.path().join("%shard%").to_string_lossy().to_string(),
        };
        let mut factory = ShardFactory::new(template);
        let _ = factory.open(0).await?;
        let factory = Arc::new(factory);

        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let server = tokio::spawn(run_grpc_with_reaper(listener, factory.clone(), shutdown_rx));

        let endpoint = format!("http://{}", addr);
        let channel = tonic::transport::Endpoint::new(endpoint.clone())?
            .connect()
            .await?;
        let mut client = SiloClient::new(channel);

        // Helper to assert error contains substring
        async fn expect_invalid_arg(
            client: &mut SiloClient<tonic::transport::Channel>,
            req: EnqueueRequest,
            msg: &str,
        ) {
            let res = client.enqueue(req).await;
            match res {
                Ok(_) => panic!("expected error"),
                Err(status) => {
                    assert_eq!(status.code(), tonic::Code::InvalidArgument);
                    assert!(status.message().contains(msg), "got: {}", status.message());
                }
            }
        }

        // 1) Too many keys (>16)
        {
            let mut md = std::collections::HashMap::new();
            for i in 0..17 {
                md.insert(format!("k{}", i), "v".to_string());
            }
            let req = EnqueueRequest {
                shard: "0".to_string(),
                id: "".to_string(),
                priority: 1,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
                }),
                concurrency_limits: vec![],
                tenant: None,
                metadata: md,
            };
            expect_invalid_arg(&mut client, req, "too many entries").await;
        }

        // 2) Key too long (>=64 chars)
        {
            let mut md = std::collections::HashMap::new();
            let long_key = "x".repeat(64);
            md.insert(long_key, "v".to_string());
            let req = EnqueueRequest {
                shard: "0".to_string(),
                id: "".to_string(),
                priority: 1,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
                }),
                concurrency_limits: vec![],
                tenant: None,
                metadata: md,
            };
            expect_invalid_arg(&mut client, req, "key too long").await;
        }

        // 3) Value too long (>= u16::MAX)
        {
            let mut md = std::collections::HashMap::new();
            let long_val = vec![b'a'; (u16::MAX as usize)];
            md.insert(
                "k".to_string(),
                String::from_utf8(long_val).unwrap_or_default(),
            );
            let req = EnqueueRequest {
                shard: "0".to_string(),
                id: "".to_string(),
                priority: 1,
                start_at_ms: 0,
                retry_policy: None,
                payload: Some(JsonValueBytes {
                    data: b"{}".to_vec(),
                }),
                concurrency_limits: vec![],
                tenant: None,
                metadata: md,
            };
            expect_invalid_arg(&mut client, req, "value too long").await;
        }

        let _ = shutdown_tx.send(());
        let join_result = server.await;
        match join_result {
            Ok(inner) => {
                if let Err(e) = inner {
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
        Ok(())
    })
    .await
    .expect("test timed out")?;
    Ok(())
}
