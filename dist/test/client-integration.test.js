"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const client_1 = require("../src/client");
// Support comma-separated list of servers for multi-node testing
const SILO_SERVERS = (process.env.SILO_SERVERS ||
    process.env.SILO_SERVER ||
    "localhost:50051").split(",");
const RUN_INTEGRATION = process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";
vitest_1.describe.skipIf(!RUN_INTEGRATION)("SiloGRPCClient integration", () => {
    let client;
    (0, vitest_1.beforeAll)(async () => {
        client = new client_1.SiloGRPCClient({
            servers: SILO_SERVERS,
            useTls: false,
            shardRouting: {
                topologyRefreshIntervalMs: 0, // Disable auto-refresh in tests
            },
        });
        // Refresh topology to discover actual cluster state
        await client.refreshTopology();
    });
    (0, vitest_1.afterAll)(() => {
        client.close();
    });
    (0, vitest_1.describe)("topology discovery", () => {
        (0, vitest_1.it)("discovers cluster topology via GetClusterInfo", async () => {
            const topology = client.getTopology();
            // Should have discovered at least 1 shard
            (0, vitest_1.expect)(topology.numShards).toBeGreaterThanOrEqual(1);
            // Should have server addresses for shards
            (0, vitest_1.expect)(topology.shardToServer.size).toBeGreaterThanOrEqual(1);
        });
        (0, vitest_1.it)("refreshes topology on demand", async () => {
            // This should succeed without throwing
            await client.refreshTopology();
            const topology = client.getTopology();
            (0, vitest_1.expect)(topology.numShards).toBeGreaterThanOrEqual(1);
        });
    });
    (0, vitest_1.describe)("tenant-based shard routing", () => {
        (0, vitest_1.it)("routes requests to correct shard based on tenant", async () => {
            const tenant = "test-tenant-123";
            const payload = { task: "routing-test" };
            const jobId = await client.enqueue({
                tenant,
                payload,
                priority: 10,
            });
            (0, vitest_1.expect)(jobId).toBeTruthy();
            // Should be able to retrieve the job using the same tenant
            const job = await client.getJob(jobId, tenant);
            (0, vitest_1.expect)(job).toBeDefined();
            (0, vitest_1.expect)(job?.id).toBe(jobId);
        });
        (0, vitest_1.it)("getShardForTenant returns consistent shard", () => {
            const tenant = "consistent-tenant";
            const shard1 = client.getShardForTenant(tenant);
            const shard2 = client.getShardForTenant(tenant);
            (0, vitest_1.expect)(shard1).toBe(shard2);
            (0, vitest_1.expect)(shard1).toBeGreaterThanOrEqual(0);
        });
        (0, vitest_1.it)("different tenants may route to different shards", () => {
            // With enough tenants, they should distribute across available shards
            const shards = new Set();
            for (let i = 0; i < 100; i++) {
                const shard = client.getShardForTenant(`tenant-${i}`);
                shards.add(shard);
            }
            // Should see at least 1 shard used (may be 1 if single-node)
            (0, vitest_1.expect)(shards.size).toBeGreaterThanOrEqual(1);
        });
    });
    (0, vitest_1.describe)("enqueue and getJob", () => {
        (0, vitest_1.it)("enqueues a job and retrieves it", async () => {
            const payload = { task: "send-email", to: "test@example.com" };
            const tenant = "test-tenant";
            const jobId = await client.enqueue({
                tenant,
                payload,
                priority: 10,
                metadata: { source: "integration-test" },
            });
            (0, vitest_1.expect)(jobId).toBeTruthy();
            (0, vitest_1.expect)(typeof jobId).toBe("string");
            // Retrieve the job
            const job = await client.getJob(jobId, tenant);
            (0, vitest_1.expect)(job).toBeDefined();
            (0, vitest_1.expect)(job?.id).toBe(jobId);
            (0, vitest_1.expect)(job?.priority).toBe(10);
            (0, vitest_1.expect)(job?.metadata?.source).toBe("integration-test");
            // Decode and verify payload
            const retrievedPayload = (0, client_1.decodePayload)(job?.payload?.data);
            (0, vitest_1.expect)(retrievedPayload).toEqual(payload);
        });
        (0, vitest_1.it)("enqueues a job with custom id", async () => {
            const customId = `custom-${Date.now()}`;
            const tenant = "custom-id-tenant";
            const jobId = await client.enqueue({
                tenant,
                id: customId,
                payload: { data: "test" },
            });
            (0, vitest_1.expect)(jobId).toBe(customId);
            const job = await client.getJob(customId, tenant);
            (0, vitest_1.expect)(job?.id).toBe(customId);
        });
        (0, vitest_1.it)("enqueues a job with retry policy", async () => {
            const tenant = "retry-tenant";
            const jobId = await client.enqueue({
                tenant,
                payload: { data: "retry-test" },
                retryPolicy: {
                    retryCount: 3,
                    initialIntervalMs: 1000n,
                    maxIntervalMs: 30000n,
                    randomizeInterval: true,
                    backoffFactor: 2.0,
                },
            });
            (0, vitest_1.expect)(jobId).toBeTruthy();
            const job = await client.getJob(jobId, tenant);
            (0, vitest_1.expect)(job?.retryPolicy?.retryCount).toBe(3);
            (0, vitest_1.expect)(job?.retryPolicy?.backoffFactor).toBe(2.0);
        });
        (0, vitest_1.it)("enqueues a job with concurrency limits", async () => {
            const tenant = "concurrency-tenant";
            const jobId = await client.enqueue({
                tenant,
                payload: { data: "concurrency-test" },
                limits: [{ type: "concurrency", key: "user:123", maxConcurrency: 5 }],
            });
            (0, vitest_1.expect)(jobId).toBeTruthy();
            const job = await client.getJob(jobId, tenant);
            (0, vitest_1.expect)(job?.limits).toHaveLength(1);
            const limit = job.limits[0];
            (0, vitest_1.expect)(limit.type).toBe("concurrency");
            if (limit.type === "concurrency") {
                (0, vitest_1.expect)(limit.key).toBe("user:123");
                (0, vitest_1.expect)(limit.maxConcurrency).toBe(5);
            }
        });
        (0, vitest_1.it)("enqueues a job with rate limits", async () => {
            const tenant = "rate-limit-tenant";
            const jobId = await client.enqueue({
                tenant,
                payload: { data: "rate-limit-test" },
                limits: [
                    {
                        type: "rateLimit",
                        name: "api-requests",
                        uniqueKey: "user:456",
                        limit: 100n,
                        durationMs: 60000n,
                        hits: 1,
                        algorithm: client_1.GubernatorAlgorithm.TOKEN_BUCKET,
                        retryPolicy: {
                            initialBackoffMs: 100n,
                            maxBackoffMs: 5000n,
                            backoffMultiplier: 2.0,
                            maxRetries: 10,
                        },
                    },
                ],
            });
            (0, vitest_1.expect)(jobId).toBeTruthy();
            const job = await client.getJob(jobId, tenant);
            (0, vitest_1.expect)(job?.limits).toHaveLength(1);
            const limit = job.limits[0];
            (0, vitest_1.expect)(limit.type).toBe("rateLimit");
            if (limit.type === "rateLimit") {
                (0, vitest_1.expect)(limit.name).toBe("api-requests");
                (0, vitest_1.expect)(limit.uniqueKey).toBe("user:456");
                (0, vitest_1.expect)(limit.limit).toBe(100n);
                (0, vitest_1.expect)(limit.durationMs).toBe(60000n);
                (0, vitest_1.expect)(limit.hits).toBe(1);
                (0, vitest_1.expect)(limit.algorithm).toBe(client_1.GubernatorAlgorithm.TOKEN_BUCKET);
                (0, vitest_1.expect)(limit.retryPolicy?.initialBackoffMs).toBe(100n);
                (0, vitest_1.expect)(limit.retryPolicy?.maxBackoffMs).toBe(5000n);
                (0, vitest_1.expect)(limit.retryPolicy?.backoffMultiplier).toBe(2.0);
                (0, vitest_1.expect)(limit.retryPolicy?.maxRetries).toBe(10);
            }
        });
        (0, vitest_1.it)("enqueues a job with mixed limits", async () => {
            const tenant = "mixed-limits-tenant";
            const jobId = await client.enqueue({
                tenant,
                payload: { data: "mixed-limits-test" },
                limits: [
                    { type: "concurrency", key: "tenant:abc", maxConcurrency: 10 },
                    {
                        type: "rateLimit",
                        name: "burst-limit",
                        uniqueKey: "tenant:abc",
                        limit: 50n,
                        durationMs: 1000n,
                    },
                ],
            });
            (0, vitest_1.expect)(jobId).toBeTruthy();
            const job = await client.getJob(jobId, tenant);
            (0, vitest_1.expect)(job?.limits).toHaveLength(2);
            (0, vitest_1.expect)(job.limits[0].type).toBe("concurrency");
            (0, vitest_1.expect)(job.limits[1].type).toBe("rateLimit");
        });
    });
    (0, vitest_1.describe)("leaseTasks and reportOutcome", () => {
        (0, vitest_1.it)("leases a task and reports success", async () => {
            const uniqueJobId = `lease-success-${Date.now()}`;
            const tenant = "lease-tenant";
            const payload = { action: "process", value: 42 };
            const jobId = await client.enqueue({
                tenant,
                id: uniqueJobId,
                payload,
                priority: 1,
            });
            // Get the shard for this tenant to poll from the correct server
            const shard = client.getShardForTenant(tenant);
            let task;
            for (let i = 0; i < 5 && !task; i++) {
                const tasks = await client.leaseTasks({
                    workerId: `test-worker-lease-${Date.now()}`,
                    maxTasks: 50,
                    shard,
                });
                task = tasks.find((t) => t.jobId === jobId);
                if (!task) {
                    await new Promise((r) => setTimeout(r, 100));
                }
            }
            (0, vitest_1.expect)(task).toBeDefined();
            (0, vitest_1.expect)(task?.attemptNumber).toBe(1);
            (0, vitest_1.expect)(task?.priority).toBe(1);
            const taskPayload = (0, client_1.decodePayload)(task?.payload?.data);
            (0, vitest_1.expect)(taskPayload).toEqual(payload);
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                tenant,
                outcome: {
                    type: "success",
                    result: { processed: true, output: "done" },
                },
            });
        });
        (0, vitest_1.it)("reports failure outcome", async () => {
            const uniqueJobId = `fail-test-${Date.now()}`;
            const tenant = "fail-tenant";
            const jobId = await client.enqueue({
                tenant,
                id: uniqueJobId,
                payload: { action: "fail-test" },
                priority: 1,
            });
            // Get the shard for this tenant to poll from the correct server
            const shard = client.getShardForTenant(tenant);
            let task;
            for (let i = 0; i < 5 && !task; i++) {
                const tasks = await client.leaseTasks({
                    workerId: `test-worker-fail-${Date.now()}`,
                    maxTasks: 50,
                    shard,
                });
                task = tasks.find((t) => t.jobId === jobId);
                if (!task) {
                    await new Promise((r) => setTimeout(r, 100));
                }
            }
            (0, vitest_1.expect)(task).toBeDefined();
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                tenant,
                outcome: {
                    type: "failure",
                    code: "PROCESSING_ERROR",
                    data: { reason: "Something went wrong" },
                },
            });
        });
    });
    (0, vitest_1.describe)("heartbeat", () => {
        (0, vitest_1.it)("extends a task lease via heartbeat", async () => {
            const tenant = "heartbeat-tenant";
            const jobId = await client.enqueue({
                tenant,
                payload: { action: "heartbeat-test" },
                priority: 1,
            });
            // Get the shard for this tenant to poll from the correct server
            const shard = client.getShardForTenant(tenant);
            const tasks = await client.leaseTasks({
                workerId: "test-worker-3",
                maxTasks: 10,
                shard,
            });
            const task = tasks.find((t) => t.jobId === jobId);
            (0, vitest_1.expect)(task).toBeDefined();
            await client.heartbeat("test-worker-3", task.id, task.shard, tenant);
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                tenant,
                outcome: { type: "success", result: {} },
            });
        });
    });
    (0, vitest_1.describe)("deleteJob", () => {
        (0, vitest_1.it)("deletes a completed job", async () => {
            const uniqueJobId = `delete-test-${Date.now()}`;
            const tenant = "delete-tenant";
            const jobId = await client.enqueue({
                tenant,
                id: uniqueJobId,
                payload: { action: "delete-test" },
                priority: 1,
            });
            const job = await client.getJob(jobId, tenant);
            (0, vitest_1.expect)(job?.id).toBe(jobId);
            // Get the shard for this tenant to poll from the correct server
            const shard = client.getShardForTenant(tenant);
            let task;
            for (let i = 0; i < 5 && !task; i++) {
                const tasks = await client.leaseTasks({
                    workerId: `delete-test-worker-${Date.now()}`,
                    maxTasks: 50,
                    shard,
                });
                task = tasks.find((t) => t.jobId === jobId);
                if (!task) {
                    await new Promise((r) => setTimeout(r, 100));
                }
            }
            (0, vitest_1.expect)(task).toBeDefined();
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                tenant,
                outcome: { type: "success", result: {} },
            });
            await client.deleteJob(jobId, tenant);
        });
    });
    (0, vitest_1.describe)("query", () => {
        (0, vitest_1.it)("queries jobs with SQL", async () => {
            const testBatch = `batch-${Date.now()}`;
            const tenant = "query-tenant";
            for (let i = 0; i < 3; i++) {
                await client.enqueue({
                    tenant,
                    payload: { index: i },
                    priority: 10 + i,
                    metadata: { batch: testBatch },
                });
            }
            // SQL queries require tenant in WHERE clause for filtering
            const result = await client.query(`SELECT * FROM jobs WHERE tenant = '${tenant}'`, tenant);
            (0, vitest_1.expect)(result.rowCount).toBeGreaterThanOrEqual(3);
            (0, vitest_1.expect)(result.columns.length).toBeGreaterThan(0);
            (0, vitest_1.expect)(result.columns.some((c) => c.name === "id")).toBe(true);
            (0, vitest_1.expect)(result.columns.some((c) => c.name === "priority")).toBe(true);
        });
        (0, vitest_1.it)("queries with WHERE clause", async () => {
            const tenant = "query-where-tenant";
            // First ensure there's data
            await client.enqueue({
                tenant,
                payload: { test: true },
                priority: 5,
            });
            const result = await client.query("SELECT id, priority FROM jobs WHERE priority < 20", tenant);
            (0, vitest_1.expect)(result.columns.length).toBe(2);
        });
        (0, vitest_1.it)("queries with aggregation", async () => {
            const tenant = "query-agg-tenant";
            // Ensure there's at least one job
            await client.enqueue({
                tenant,
                payload: { test: true },
            });
            const result = await client.query("SELECT COUNT(*) as count FROM jobs", tenant);
            (0, vitest_1.expect)(result.rowCount).toBe(1);
            (0, vitest_1.expect)(result.columns.some((c) => c.name === "count")).toBe(true);
            const row = (0, client_1.decodePayload)(result.rows[0]?.data);
            (0, vitest_1.expect)(typeof row?.count).toBe("number");
        });
    });
});
vitest_1.describe.skipIf(!RUN_INTEGRATION)("Shard routing integration", () => {
    (0, vitest_1.describe)("wrong shard error handling", () => {
        (0, vitest_1.it)("handles requests to non-existent shards gracefully", async () => {
            // Create a client configured for more shards than exist
            const client = new client_1.SiloGRPCClient({
                servers: SILO_SERVERS,
                useTls: false,
                shardRouting: {
                    numShards: 100, // Way more shards than actually exist
                    maxWrongShardRetries: 2,
                    wrongShardRetryDelayMs: 50,
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                // This tenant will hash to a shard that likely doesn't exist
                // but after topology refresh, it should still work
                await client.refreshTopology();
                // Now requests should work because we've discovered the actual topology
                const jobId = await client.enqueue({
                    tenant: "wrong-shard-test",
                    payload: { test: true },
                });
                (0, vitest_1.expect)(jobId).toBeTruthy();
            }
            finally {
                client.close();
            }
        });
        (0, vitest_1.it)("retries and discovers correct shard on wrong shard error", async () => {
            const client = new client_1.SiloGRPCClient({
                servers: SILO_SERVERS,
                useTls: false,
                shardRouting: {
                    numShards: 1,
                    maxWrongShardRetries: 5,
                    wrongShardRetryDelayMs: 10,
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                // First refresh to get accurate topology
                await client.refreshTopology();
                // Now operations should work
                const jobId = await client.enqueue({
                    tenant: "retry-test-tenant",
                    payload: { data: "retry test" },
                });
                (0, vitest_1.expect)(jobId).toBeTruthy();
                const job = await client.getJob(jobId, "retry-test-tenant");
                (0, vitest_1.expect)(job?.id).toBe(jobId);
            }
            finally {
                client.close();
            }
        });
    });
    (0, vitest_1.describe)("multi-tenant routing", () => {
        (0, vitest_1.it)("routes different tenants consistently", async () => {
            const client = new client_1.SiloGRPCClient({
                servers: SILO_SERVERS,
                useTls: false,
                shardRouting: {
                    numShards: 8,
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                await client.refreshTopology();
                const tenants = ["tenant-a", "tenant-b", "tenant-c"];
                const jobIds = {};
                // Enqueue jobs for different tenants
                for (const tenant of tenants) {
                    jobIds[tenant] = await client.enqueue({
                        tenant,
                        payload: { tenant },
                    });
                }
                // Verify each job can be retrieved with its tenant
                for (const tenant of tenants) {
                    const job = await client.getJob(jobIds[tenant], tenant);
                    (0, vitest_1.expect)(job?.id).toBe(jobIds[tenant]);
                    const payload = (0, client_1.decodePayload)(job?.payload?.data);
                    (0, vitest_1.expect)(payload?.tenant).toBe(tenant);
                }
            }
            finally {
                client.close();
            }
        });
        (0, vitest_1.it)("handles high volume of different tenants", async () => {
            const client = new client_1.SiloGRPCClient({
                servers: SILO_SERVERS,
                useTls: false,
                shardRouting: {
                    numShards: 16,
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                await client.refreshTopology();
                const numTenants = 50;
                const results = [];
                // Enqueue jobs for many tenants
                for (let i = 0; i < numTenants; i++) {
                    const tenant = `bulk-tenant-${i}`;
                    const jobId = await client.enqueue({
                        tenant,
                        payload: { index: i },
                    });
                    results.push({ tenant, jobId });
                }
                // Verify random sample can be retrieved
                const samples = [0, 10, 25, 49].map((i) => results[i]);
                for (const { tenant, jobId } of samples) {
                    const job = await client.getJob(jobId, tenant);
                    (0, vitest_1.expect)(job?.id).toBe(jobId);
                }
            }
            finally {
                client.close();
            }
        });
    });
    (0, vitest_1.describe)("topology changes", () => {
        (0, vitest_1.it)("can refresh topology multiple times", async () => {
            const client = new client_1.SiloGRPCClient({
                servers: SILO_SERVERS,
                useTls: false,
                shardRouting: {
                    numShards: 1,
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                // Multiple refreshes should all succeed
                await client.refreshTopology();
                const topo1 = client.getTopology();
                await client.refreshTopology();
                const topo2 = client.getTopology();
                await client.refreshTopology();
                const topo3 = client.getTopology();
                // Topology should be consistent (cluster hasn't changed)
                (0, vitest_1.expect)(topo1.numShards).toBe(topo2.numShards);
                (0, vitest_1.expect)(topo2.numShards).toBe(topo3.numShards);
            }
            finally {
                client.close();
            }
        });
        (0, vitest_1.it)("operations work after topology refresh", async () => {
            const client = new client_1.SiloGRPCClient({
                servers: SILO_SERVERS,
                useTls: false,
                shardRouting: {
                    numShards: 1,
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                const tenant = "refresh-test";
                // Enqueue before refresh
                await client.refreshTopology();
                const jobId1 = await client.enqueue({
                    tenant,
                    payload: { phase: "before" },
                });
                // Refresh topology
                await client.refreshTopology();
                // Enqueue after refresh
                const jobId2 = await client.enqueue({
                    tenant,
                    payload: { phase: "after" },
                });
                // Both jobs should be retrievable
                const job1 = await client.getJob(jobId1, tenant);
                const job2 = await client.getJob(jobId2, tenant);
                (0, vitest_1.expect)(job1?.id).toBe(jobId1);
                (0, vitest_1.expect)(job2?.id).toBe(jobId2);
            }
            finally {
                client.close();
            }
        });
    });
    (0, vitest_1.describe)("connection management", () => {
        (0, vitest_1.it)("creates client with multiple server addresses", async () => {
            // Even if some addresses are invalid, client should work with valid ones
            const client = new client_1.SiloGRPCClient({
                servers: [...SILO_SERVERS, "invalid-server:99999"],
                useTls: false,
                shardRouting: {
                    numShards: 1,
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                // Should be able to refresh topology using the valid server
                await client.refreshTopology();
                const topology = client.getTopology();
                (0, vitest_1.expect)(topology.numShards).toBeGreaterThanOrEqual(1);
                // Operations should work
                const jobId = await client.enqueue({
                    tenant: "multi-server-test",
                    payload: { test: true },
                });
                (0, vitest_1.expect)(jobId).toBeTruthy();
            }
            finally {
                client.close();
            }
        });
        (0, vitest_1.it)("handles server address as host:port object", async () => {
            const [host, port] = SILO_SERVERS[0].split(":");
            const client = new client_1.SiloGRPCClient({
                servers: { host, port: parseInt(port) },
                useTls: false,
                shardRouting: {
                    numShards: 1,
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                await client.refreshTopology();
                const jobId = await client.enqueue({
                    tenant: "host-port-test",
                    payload: { test: true },
                });
                (0, vitest_1.expect)(jobId).toBeTruthy();
            }
            finally {
                client.close();
            }
        });
    });
});
//# sourceMappingURL=client-integration.test.js.map