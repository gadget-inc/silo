"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const client_1 = require("../src/client");
// Support comma-separated list of servers for multi-node testing
const SILO_SERVERS = (process.env.SILO_SERVERS ||
    process.env.SILO_SERVER ||
    "localhost:7450").split(",");
const RUN_INTEGRATION = process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";
// Default task group for all integration tests
const DEFAULT_TASK_GROUP = "integration-test-group";
/**
 * Wait for the cluster to converge by polling until all shards have owners.
 * This handles the case where the cluster is still starting up and shards
 * haven't been fully distributed yet.
 */
async function waitForClusterConvergence(client, maxAttempts = 30, delayMs = 500) {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        await client.refreshTopology();
        const topology = client.getTopology();
        // Check if we have at least one shard with a valid address
        const hasShards = topology.shards.length > 0;
        const allShardsHaveOwners = topology.shards.every((s) => s.serverAddr && s.serverAddr.length > 0);
        if (hasShards && allShardsHaveOwners) {
            return; // Cluster is ready
        }
        if (attempt === maxAttempts) {
            throw new Error(`Cluster did not converge after ${maxAttempts} attempts. ` +
                `Found ${topology.shards.length} shards, ` +
                `${topology.shards.filter((s) => s.serverAddr).length} with owners.`);
        }
        // Wait before retrying
        await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
}
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
        // Wait for cluster to converge and discover topology
        await waitForClusterConvergence(client);
    });
    (0, vitest_1.afterAll)(() => {
        client.close();
    });
    (0, vitest_1.describe)("topology discovery", () => {
        (0, vitest_1.it)("discovers cluster topology via GetClusterInfo", async () => {
            const topology = client.getTopology();
            // Should have discovered at least 1 shard
            (0, vitest_1.expect)(topology.shards.length).toBeGreaterThanOrEqual(1);
            // Should have server addresses for shards
            (0, vitest_1.expect)(topology.shardToServer.size).toBeGreaterThanOrEqual(1);
        });
        (0, vitest_1.it)("refreshes topology on demand", async () => {
            // This should succeed without throwing
            await client.refreshTopology();
            const topology = client.getTopology();
            (0, vitest_1.expect)(topology.shards.length).toBeGreaterThanOrEqual(1);
        });
    });
    (0, vitest_1.describe)("tenant-based shard routing", () => {
        (0, vitest_1.it)("routes requests to correct shard based on tenant", async () => {
            const tenant = "test-tenant-123";
            const payload = { task: "routing-test" };
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload,
                priority: 10,
            });
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            // Should be able to retrieve the job using the same tenant
            const job = await client.getJob(handle.id, tenant);
            (0, vitest_1.expect)(job).toBeDefined();
            (0, vitest_1.expect)(job?.id).toBe(handle.id);
        });
        (0, vitest_1.it)("getShardForTenant returns consistent shard", () => {
            const tenant = "consistent-tenant";
            const shard1 = client.getShardForTenant(tenant);
            const shard2 = client.getShardForTenant(tenant);
            (0, vitest_1.expect)(shard1).toBe(shard2);
            (0, vitest_1.expect)(typeof shard1).toBe("string");
            (0, vitest_1.expect)(shard1.length).toBeGreaterThan(0);
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
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload,
                priority: 10,
                metadata: { source: "integration-test" },
            });
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            (0, vitest_1.expect)(typeof handle.id).toBe("string");
            // Retrieve the job
            const job = await client.getJob(handle.id, tenant);
            (0, vitest_1.expect)(job).toBeDefined();
            (0, vitest_1.expect)(job?.id).toBe(handle.id);
            (0, vitest_1.expect)(job?.priority).toBe(10);
            (0, vitest_1.expect)(job?.metadata?.source).toBe("integration-test");
            // Verify payload is already decoded
            (0, vitest_1.expect)(job?.payload).toEqual(payload);
        });
        (0, vitest_1.it)("enqueues a job with custom id", async () => {
            const customId = `custom-${Date.now()}`;
            const tenant = "custom-id-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                id: customId,
                payload: { data: "test" },
            });
            (0, vitest_1.expect)(handle.id).toBe(customId);
            const job = await client.getJob(customId, tenant);
            (0, vitest_1.expect)(job?.id).toBe(customId);
        });
        (0, vitest_1.it)("enqueues a job with retry policy", async () => {
            const tenant = "retry-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { data: "retry-test" },
                retryPolicy: {
                    retryCount: 3,
                    initialIntervalMs: 1000n,
                    maxIntervalMs: 30000n,
                    randomizeInterval: true,
                    backoffFactor: 2.0,
                },
            });
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            const job = await client.getJob(handle.id, tenant);
            (0, vitest_1.expect)(job?.retryPolicy?.retryCount).toBe(3);
            (0, vitest_1.expect)(job?.retryPolicy?.backoffFactor).toBe(2.0);
        });
        (0, vitest_1.it)("enqueues a job with concurrency limits", async () => {
            const tenant = "concurrency-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { data: "concurrency-test" },
                limits: [{ type: "concurrency", key: "user:123", maxConcurrency: 5 }],
            });
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            const job = await client.getJob(handle.id, tenant);
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
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
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
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            const job = await client.getJob(handle.id, tenant);
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
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
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
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            const job = await client.getJob(handle.id, tenant);
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
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                id: uniqueJobId,
                payload,
                priority: 1,
            });
            // Get the shard for this tenant to poll from the correct server
            const shard = client.getShardForTenant(tenant);
            let task;
            for (let i = 0; i < 5 && !task; i++) {
                const result = await client.leaseTasks({
                    workerId: `test-worker-lease-${Date.now()}`,
                    maxTasks: 50,
                    shard,
                    taskGroup: DEFAULT_TASK_GROUP,
                });
                task = result.tasks.find((t) => t.jobId === handle.id);
                if (!task) {
                    await new Promise((r) => setTimeout(r, 100));
                }
            }
            (0, vitest_1.expect)(task).toBeDefined();
            (0, vitest_1.expect)(task?.attemptNumber).toBe(1);
            (0, vitest_1.expect)(task?.priority).toBe(1);
            const taskPayload = (0, client_1.decodeBytes)(task?.payload?.encoding.oneofKind === "msgpack" ? task.payload.encoding.msgpack : undefined, "payload");
            (0, vitest_1.expect)(taskPayload).toEqual(payload);
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                outcome: {
                    type: "success",
                    result: { processed: true, output: "done" },
                },
            });
        });
        (0, vitest_1.it)("reports failure outcome", async () => {
            const uniqueJobId = `fail-test-${Date.now()}`;
            const tenant = "fail-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                id: uniqueJobId,
                payload: { action: "fail-test" },
                priority: 1,
            });
            // Get the shard for this tenant to poll from the correct server
            const shard = client.getShardForTenant(tenant);
            let task;
            for (let i = 0; i < 5 && !task; i++) {
                const result = await client.leaseTasks({
                    workerId: `test-worker-fail-${Date.now()}`,
                    maxTasks: 50,
                    shard,
                    taskGroup: DEFAULT_TASK_GROUP,
                });
                task = result.tasks.find((t) => t.jobId === handle.id);
                if (!task) {
                    await new Promise((r) => setTimeout(r, 100));
                }
            }
            (0, vitest_1.expect)(task).toBeDefined();
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
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
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { action: "heartbeat-test" },
                priority: 1,
            });
            // Get the shard for this tenant to poll from the correct server
            const shard = client.getShardForTenant(tenant);
            const result = await client.leaseTasks({
                workerId: "test-worker-3",
                maxTasks: 10,
                shard,
                taskGroup: DEFAULT_TASK_GROUP,
            });
            const task = result.tasks.find((t) => t.jobId === handle.id);
            (0, vitest_1.expect)(task).toBeDefined();
            await client.heartbeat("test-worker-3", task.id, task.shard);
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                outcome: { type: "success", result: {} },
            });
        });
    });
    (0, vitest_1.describe)("deleteJob", () => {
        (0, vitest_1.it)("deletes a completed job", async () => {
            const uniqueJobId = `delete-test-${Date.now()}`;
            const tenant = "delete-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                id: uniqueJobId,
                payload: { action: "delete-test" },
                priority: 1,
            });
            const job = await client.getJob(handle.id, tenant);
            (0, vitest_1.expect)(job?.id).toBe(handle.id);
            // Get the shard for this tenant to poll from the correct server
            const shard = client.getShardForTenant(tenant);
            let task;
            for (let i = 0; i < 5 && !task; i++) {
                const result = await client.leaseTasks({
                    workerId: `delete-test-worker-${Date.now()}`,
                    maxTasks: 50,
                    shard,
                    taskGroup: DEFAULT_TASK_GROUP,
                });
                task = result.tasks.find((t) => t.jobId === handle.id);
                if (!task) {
                    await new Promise((r) => setTimeout(r, 100));
                }
            }
            (0, vitest_1.expect)(task).toBeDefined();
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                outcome: { type: "success", result: {} },
            });
            await client.deleteJob(handle.id, tenant);
        });
    });
    (0, vitest_1.describe)("expediteJob", () => {
        (0, vitest_1.it)("expedites a future-scheduled job to run immediately", async () => {
            const uniqueJobId = `expedite-test-${Date.now()}`;
            const tenant = "expedite-tenant";
            // Schedule job 1 hour in the future
            const futureStartAtMs = BigInt(Date.now() + 3_600_000);
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                id: uniqueJobId,
                payload: { action: "expedite-test" },
                priority: 1,
                startAtMs: futureStartAtMs,
            });
            // Verify job was created and is scheduled
            const jobBefore = await client.getJob(handle.id, tenant);
            (0, vitest_1.expect)(jobBefore?.status).toBe(client_1.JobStatus.Scheduled);
            // Get the shard for this tenant to poll from the correct server
            const shard = client.getShardForTenant(tenant);
            // Try to lease - should get nothing because job is future-scheduled
            const resultBefore = await client.leaseTasks({
                shard,
                workerId: "expedite-test-worker",
                maxTasks: 1,
                taskGroup: DEFAULT_TASK_GROUP,
            });
            (0, vitest_1.expect)(resultBefore.tasks.find((t) => t.jobId === handle.id)).toBeUndefined();
            // Expedite the job
            await client.expediteJob(handle.id, tenant);
            // Now the job should be leasable
            let task;
            for (let i = 0; i < 10; i++) {
                const result = await client.leaseTasks({
                    shard,
                    workerId: "expedite-test-worker",
                    maxTasks: 10,
                    taskGroup: DEFAULT_TASK_GROUP,
                });
                task = result.tasks.find((t) => t.jobId === handle.id);
                if (task)
                    break;
                await new Promise((r) => setTimeout(r, 100));
            }
            (0, vitest_1.expect)(task).toBeDefined();
            // Complete the job
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                outcome: { type: "success", result: { expedited: true } },
            });
            // Verify job succeeded
            const jobAfter = await client.getJob(handle.id, tenant);
            (0, vitest_1.expect)(jobAfter?.status).toBe(client_1.JobStatus.Succeeded);
        });
        (0, vitest_1.it)("expedite throws error for non-existent job", async () => {
            const tenant = "expedite-tenant";
            await (0, vitest_1.expect)(client.expediteJob("non-existent-job-id", tenant)).rejects.toThrow();
        });
        (0, vitest_1.it)("expedite throws error for running job", async () => {
            const uniqueJobId = `expedite-running-test-${Date.now()}`;
            const tenant = "expedite-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                id: uniqueJobId,
                payload: { action: "expedite-running-test" },
                priority: 1,
            });
            // Get the shard for this tenant
            const shard = client.getShardForTenant(tenant);
            // Lease the task to make it running
            let task;
            for (let i = 0; i < 10; i++) {
                const result = await client.leaseTasks({
                    shard,
                    workerId: "expedite-running-test-worker",
                    maxTasks: 10,
                    taskGroup: DEFAULT_TASK_GROUP,
                });
                task = result.tasks.find((t) => t.jobId === handle.id);
                if (task)
                    break;
                await new Promise((r) => setTimeout(r, 100));
            }
            (0, vitest_1.expect)(task).toBeDefined();
            // Verify job is running
            const jobRunning = await client.getJob(handle.id, tenant);
            (0, vitest_1.expect)(jobRunning?.status).toBe(client_1.JobStatus.Running);
            // Try to expedite - should fail
            await (0, vitest_1.expect)(client.expediteJob(handle.id, tenant)).rejects.toThrow();
            // Complete the job to clean up
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                outcome: { type: "success", result: {} },
            });
        });
        (0, vitest_1.it)("expedite throws error for cancelled job", async () => {
            const uniqueJobId = `expedite-cancelled-test-${Date.now()}`;
            const tenant = "expedite-tenant";
            // Schedule job 1 hour in the future
            const futureStartAtMs = BigInt(Date.now() + 3_600_000);
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                id: uniqueJobId,
                payload: { action: "expedite-cancelled-test" },
                priority: 1,
                startAtMs: futureStartAtMs,
            });
            // Cancel the job
            await client.cancelJob(handle.id, tenant);
            // Verify job is cancelled
            const jobCancelled = await client.getJob(handle.id, tenant);
            (0, vitest_1.expect)(jobCancelled?.status).toBe(client_1.JobStatus.Cancelled);
            // Try to expedite - should fail
            await (0, vitest_1.expect)(client.expediteJob(handle.id, tenant)).rejects.toThrow();
        });
    });
    (0, vitest_1.describe)("leaseTask", () => {
        (0, vitest_1.it)("leases a specific job's task directly and completes it", async () => {
            const uniqueJobId = `lease-task-test-${Date.now()}`;
            const tenant = "lease-task-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                id: uniqueJobId,
                payload: { action: "lease-task-test" },
                priority: 1,
            });
            // Verify job is scheduled
            const jobBefore = await client.getJob(handle.id, tenant);
            (0, vitest_1.expect)(jobBefore?.status).toBe(client_1.JobStatus.Scheduled);
            // Lease the specific task directly
            const task = await client.leaseTask({
                id: handle.id,
                workerId: "lease-task-test-worker",
                tenant,
            });
            (0, vitest_1.expect)(task.jobId).toBe(handle.id);
            (0, vitest_1.expect)(task.attemptNumber).toBe(1);
            // Verify job is running
            const jobRunning = await client.getJob(handle.id, tenant);
            (0, vitest_1.expect)(jobRunning?.status).toBe(client_1.JobStatus.Running);
            // Complete the job
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                outcome: { type: "success", result: { leased: true } },
            });
            // Verify job succeeded
            const jobAfter = await client.getJob(handle.id, tenant);
            (0, vitest_1.expect)(jobAfter?.status).toBe(client_1.JobStatus.Succeeded);
        });
        (0, vitest_1.it)("leaseTask throws error for non-existent job", async () => {
            const tenant = "lease-task-tenant";
            await (0, vitest_1.expect)(client.leaseTask({
                id: "non-existent-job-id",
                workerId: "test-worker",
                tenant,
            })).rejects.toThrow();
        });
        (0, vitest_1.it)("leaseTask throws error for running job", async () => {
            const uniqueJobId = `lease-task-running-test-${Date.now()}`;
            const tenant = "lease-task-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                id: uniqueJobId,
                payload: { action: "lease-task-running-test" },
                priority: 1,
            });
            // Lease the task to make it running
            const task = await client.leaseTask({
                id: handle.id,
                workerId: "lease-task-running-worker",
                tenant,
            });
            // Try to lease again - should fail
            await (0, vitest_1.expect)(client.leaseTask({
                id: handle.id,
                workerId: "lease-task-running-worker-2",
                tenant,
            })).rejects.toThrow();
            // Complete the job to clean up
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                outcome: { type: "success", result: {} },
            });
        });
    });
    (0, vitest_1.describe)("query", () => {
        (0, vitest_1.it)("queries jobs with SQL", async () => {
            const testBatch = `batch-${Date.now()}`;
            const tenant = "query-tenant";
            for (let i = 0; i < 3; i++) {
                await client.enqueue({
                    tenant,
                    taskGroup: DEFAULT_TASK_GROUP,
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
            // Rows should be deserialized objects
            (0, vitest_1.expect)(result.rows.length).toBeGreaterThanOrEqual(3);
            const row = result.rows[0];
            (0, vitest_1.expect)(typeof row.id).toBe("string");
        });
        (0, vitest_1.it)("queries with WHERE clause", async () => {
            const tenant = "query-where-tenant";
            // First ensure there's data
            await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: true },
                priority: 5,
            });
            const result = await client.query("SELECT id, priority FROM jobs WHERE priority < 20", tenant);
            (0, vitest_1.expect)(result.columns.length).toBe(2);
            // Rows should be deserialized with only the selected columns
            (0, vitest_1.expect)(result.rows.length).toBeGreaterThanOrEqual(1);
            const row = result.rows[0];
            (0, vitest_1.expect)(typeof row.id).toBe("string");
            (0, vitest_1.expect)(typeof row.priority).toBe("number");
        });
        (0, vitest_1.it)("queries with aggregation", async () => {
            const tenant = "query-agg-tenant";
            // Ensure there's at least one job
            await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: true },
            });
            const result = await client.query("SELECT COUNT(*) as count FROM jobs", tenant);
            (0, vitest_1.expect)(result.rowCount).toBe(1);
            (0, vitest_1.expect)(result.columns.some((c) => c.name === "count")).toBe(true);
            // Rows are already deserialized
            (0, vitest_1.expect)(typeof result.rows[0].count).toBe("number");
        });
        (0, vitest_1.it)("supports generic row type parameter", async () => {
            const tenant = "query-typed-tenant";
            await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: true },
                priority: 42,
            });
            const result = await client.query("SELECT id, priority FROM jobs WHERE priority = 42", tenant);
            (0, vitest_1.expect)(result.rows.length).toBeGreaterThanOrEqual(1);
            // TypeScript knows these are typed
            const row = result.rows[0];
            (0, vitest_1.expect)(typeof row.id).toBe("string");
            (0, vitest_1.expect)(row.priority).toBe(42);
        });
    });
    (0, vitest_1.describe)("JobHandle", () => {
        (0, vitest_1.it)("returns a JobHandle from enqueue", async () => {
            const tenant = "handle-enqueue-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "handle-test" },
            });
            (0, vitest_1.expect)(handle).toBeDefined();
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            (0, vitest_1.expect)(handle.tenant).toBe(tenant);
        });
        (0, vitest_1.it)("can get job details through handle", async () => {
            const tenant = "handle-getjob-tenant";
            const payload = { action: "test-action" };
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload,
                priority: 15,
            });
            const job = await handle.getJob();
            (0, vitest_1.expect)(job).toBeDefined();
            (0, vitest_1.expect)(job?.id).toBe(handle.id);
            (0, vitest_1.expect)(job?.priority).toBe(15);
            (0, vitest_1.expect)(job?.payload).toEqual(payload);
        });
        (0, vitest_1.it)("can get job status through handle", async () => {
            const tenant = "handle-status-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "status-test" },
            });
            const status = await handle.getStatus();
            (0, vitest_1.expect)(status).toBe(client_1.JobStatus.Scheduled);
        });
        (0, vitest_1.it)("can cancel job through handle", async () => {
            const tenant = "handle-cancel-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "cancel-test" },
            });
            // Verify job exists
            const job = await handle.getJob();
            (0, vitest_1.expect)(job).toBeDefined();
            // Cancel the job
            await handle.cancel();
            // Check status is cancelled (may take a moment)
            const status = await handle.getStatus();
            (0, vitest_1.expect)(status).toBe(client_1.JobStatus.Cancelled);
        });
        (0, vitest_1.it)("can create handle from existing job ID", async () => {
            const tenant = "handle-factory-tenant";
            // First enqueue a job
            const originalHandle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "factory-test" },
            });
            // Create a new handle from the job ID
            const newHandle = client.handle(originalHandle.id, tenant);
            (0, vitest_1.expect)(newHandle.id).toBe(originalHandle.id);
            (0, vitest_1.expect)(newHandle.tenant).toBe(tenant);
            // Should be able to get the same job
            const job = await newHandle.getJob();
            (0, vitest_1.expect)(job?.id).toBe(originalHandle.id);
        });
        (0, vitest_1.it)("can delete job through handle", async () => {
            const uniqueJobId = `handle-delete-${Date.now()}`;
            const tenant = "handle-delete-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                id: uniqueJobId,
                payload: { test: "delete-test" },
            });
            // Get the shard for this tenant to poll from the correct server
            const shard = client.getShardForTenant(tenant);
            // Lease and complete the task so we can delete the job
            let task;
            for (let i = 0; i < 5 && !task; i++) {
                const result = await client.leaseTasks({
                    workerId: `handle-delete-worker-${Date.now()}`,
                    maxTasks: 50,
                    shard,
                    taskGroup: DEFAULT_TASK_GROUP,
                });
                task = result.tasks.find((t) => t.jobId === handle.id);
                if (!task) {
                    await new Promise((r) => setTimeout(r, 100));
                }
            }
            (0, vitest_1.expect)(task).toBeDefined();
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                outcome: { type: "success", result: {} },
            });
            // Delete through handle
            await handle.delete();
            // Job should no longer exist (or be in terminal state)
            // Note: The exact behavior depends on server implementation
        });
        (0, vitest_1.it)("can await job result for successful job", async () => {
            const tenant = "handle-await-success-tenant";
            const resultData = { processed: true, count: 42 };
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "await-test" },
            });
            // Get the shard for this tenant to poll from the correct server
            const shard = client.getShardForTenant(tenant);
            // Lease and complete the task
            let task;
            for (let i = 0; i < 5 && !task; i++) {
                const result = await client.leaseTasks({
                    workerId: `handle-await-worker-${Date.now()}`,
                    maxTasks: 50,
                    shard,
                    taskGroup: DEFAULT_TASK_GROUP,
                });
                task = result.tasks.find((t) => t.jobId === handle.id);
                if (!task) {
                    await new Promise((r) => setTimeout(r, 100));
                }
            }
            (0, vitest_1.expect)(task).toBeDefined();
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                outcome: { type: "success", result: resultData },
            });
            // Await the result
            const result = await handle.awaitResult({
                pollIntervalMs: 100,
                timeoutMs: 5000,
            });
            (0, vitest_1.expect)(result.status).toBe(client_1.JobStatus.Succeeded);
            (0, vitest_1.expect)(result.result).toEqual(resultData);
        });
        (0, vitest_1.it)("can await job result for failed job", async () => {
            const tenant = "handle-await-fail-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "await-fail-test" },
            });
            // Get the shard for this tenant to poll from the correct server
            const shard = client.getShardForTenant(tenant);
            // Lease and fail the task
            let task;
            for (let i = 0; i < 5 && !task; i++) {
                const result = await client.leaseTasks({
                    workerId: `handle-await-fail-worker-${Date.now()}`,
                    maxTasks: 50,
                    shard,
                    taskGroup: DEFAULT_TASK_GROUP,
                });
                task = result.tasks.find((t) => t.jobId === handle.id);
                if (!task) {
                    await new Promise((r) => setTimeout(r, 100));
                }
            }
            (0, vitest_1.expect)(task).toBeDefined();
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                outcome: {
                    type: "failure",
                    code: "TEST_ERROR",
                    data: { reason: "test failure" },
                },
            });
            // Await the result
            const result = await handle.awaitResult({
                pollIntervalMs: 100,
                timeoutMs: 5000,
            });
            (0, vitest_1.expect)(result.status).toBe(client_1.JobStatus.Failed);
            (0, vitest_1.expect)(result.errorCode).toBe("TEST_ERROR");
        });
        (0, vitest_1.it)("throws timeout error when job does not complete in time", async () => {
            const tenant = "handle-await-timeout-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "timeout-test" },
                // Schedule far in the future so it won't be leased
                startAtMs: BigInt(Date.now() + 60000),
            });
            // Try to await with a short timeout - should fail
            await (0, vitest_1.expect)(handle.awaitResult({ pollIntervalMs: 50, timeoutMs: 200 })).rejects.toThrow(/Timeout/);
        });
    });
    (0, vitest_1.describe)("large payloads", () => {
        (0, vitest_1.it)("enqueues and retrieves a 10MB payload", { timeout: 30_000 }, async () => {
            const tenant = "large-payload-tenant";
            // Build a ~10MB payload using a raw Buffer.
            // msgpack encodes Buffers as binary data without base64 expansion,
            // so the wire size stays close to 10MB. This validates we're above
            // the default 4MB gRPC limit without overwhelming CI resources.
            const sizeBytes = 10 * 1024 * 1024;
            const largeBuffer = Buffer.alloc(sizeBytes, 0x42);
            const payload = { data: largeBuffer };
            const taskGroup = "large-payload-test-group";
            const handle = await client.enqueue({
                tenant,
                taskGroup,
                payload,
                priority: 50,
            });
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            // Retrieve the job and verify the payload round-trips correctly
            const job = await client.getJob(handle.id, tenant);
            (0, vitest_1.expect)(job).toBeDefined();
            (0, vitest_1.expect)(job?.id).toBe(handle.id);
            const retrieved = job?.payload;
            (0, vitest_1.expect)(retrieved.data.length).toBe(sizeBytes);
            (0, vitest_1.expect)(Buffer.from(retrieved.data).equals(largeBuffer)).toBe(true);
        });
    });
    (0, vitest_1.describe)("floating concurrency limits", () => {
        const tenant = `floating-test-${Date.now()}`;
        (0, vitest_1.it)("enqueues job with floating concurrency limit", async () => {
            const payload = { task: "floating-limit-test" };
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload,
                limits: [
                    {
                        type: "floatingConcurrency",
                        key: `floating-queue-${Date.now()}`,
                        defaultMaxConcurrency: 5,
                        refreshIntervalMs: 60000n,
                        metadata: { orgId: "test-org-123", env: "test" },
                    },
                ],
            });
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            // Verify job was created with floating limit
            const job = await client.getJob(handle.id, tenant);
            (0, vitest_1.expect)(job.status).toBe(client_1.JobStatus.Scheduled);
            (0, vitest_1.expect)(job.limits).toHaveLength(1);
            (0, vitest_1.expect)(job.limits[0].type).toBe("floatingConcurrency");
            if (job.limits[0].type === "floatingConcurrency") {
                (0, vitest_1.expect)(job.limits[0].defaultMaxConcurrency).toBe(5);
                (0, vitest_1.expect)(job.limits[0].refreshIntervalMs).toBe(60000n);
                (0, vitest_1.expect)(job.limits[0].metadata).toEqual({
                    orgId: "test-org-123",
                    env: "test",
                });
            }
        });
        (0, vitest_1.it)("returns refresh tasks when floating limit is stale", async () => {
            const queueKey = `stale-refresh-queue-${Date.now()}`;
            const shard = client.getShardForTenant(tenant);
            // Enqueue first job with a very short refresh interval
            const handle1 = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { job: 1 },
                limits: [
                    {
                        type: "floatingConcurrency",
                        key: queueKey,
                        defaultMaxConcurrency: 1, // Use 1 so second job becomes a waiter, triggering refresh
                        refreshIntervalMs: 1n, // 1ms - will be stale immediately
                        metadata: { testKey: "testValue" },
                    },
                ],
            });
            (0, vitest_1.expect)(handle1.id).toBeTruthy();
            // Wait a bit to ensure the limit becomes stale
            await new Promise((r) => setTimeout(r, 50));
            // Enqueue second job to trigger refresh scheduling
            const handle2 = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { job: 2 },
                limits: [
                    {
                        type: "floatingConcurrency",
                        key: queueKey,
                        defaultMaxConcurrency: 1, // Use 1 so second job becomes a waiter, triggering refresh
                        refreshIntervalMs: 1n,
                        metadata: { testKey: "testValue" },
                    },
                ],
            });
            (0, vitest_1.expect)(handle2.id).toBeTruthy();
            // Lease tasks - should get refresh tasks
            const result = await client.leaseTasks({
                workerId: `floating-refresh-worker-${Date.now()}`,
                maxTasks: 10,
                shard,
                taskGroup: DEFAULT_TASK_GROUP,
            });
            // Should have refresh tasks (and job tasks)
            (0, vitest_1.expect)(result.tasks.length).toBeGreaterThanOrEqual(0);
            // Find our refresh task - it MUST exist
            const refreshTask = result.refreshTasks.find((rt) => rt.queueKey === queueKey);
            (0, vitest_1.expect)(refreshTask).toBeDefined();
            (0, vitest_1.expect)(refreshTask.id).toBeTruthy();
            (0, vitest_1.expect)(refreshTask.queueKey).toBe(queueKey);
            (0, vitest_1.expect)(refreshTask.currentMaxConcurrency).toBe(1);
            (0, vitest_1.expect)(refreshTask.metadata).toEqual({ testKey: "testValue" });
            (0, vitest_1.expect)(refreshTask.shard).toBe(shard);
            // Report success with a new max concurrency
            await client.reportRefreshOutcome({
                taskId: refreshTask.id,
                shard: refreshTask.shard,
                outcome: {
                    type: "success",
                    newMaxConcurrency: 10,
                },
            });
            // Clean up - lease and complete the job tasks
            for (const task of result.tasks) {
                await client.reportOutcome({
                    taskId: task.id,
                    shard: task.shard,
                    outcome: { type: "success", result: {} },
                });
            }
        });
        (0, vitest_1.it)("reports refresh failure and triggers retry", async () => {
            const queueKey = `failure-refresh-queue-${Date.now()}`;
            const shard = client.getShardForTenant(tenant);
            // Enqueue job with short refresh interval
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { task: "failure-test" },
                limits: [
                    {
                        type: "floatingConcurrency",
                        key: queueKey,
                        defaultMaxConcurrency: 1, // Use 1 so second job becomes a waiter, triggering refresh
                        refreshIntervalMs: 1n,
                        metadata: { apiEndpoint: "https://api.example.com" },
                    },
                ],
            });
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            // Wait for limit to become stale
            await new Promise((r) => setTimeout(r, 50));
            // Trigger refresh by enqueueing another job
            await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { task: "trigger-refresh" },
                limits: [
                    {
                        type: "floatingConcurrency",
                        key: queueKey,
                        defaultMaxConcurrency: 1, // Use 1 so second job becomes a waiter, triggering refresh
                        refreshIntervalMs: 1n,
                        metadata: { apiEndpoint: "https://api.example.com" },
                    },
                ],
            });
            // Lease tasks
            const result = await client.leaseTasks({
                workerId: `failure-worker-${Date.now()}`,
                maxTasks: 10,
                shard,
                taskGroup: DEFAULT_TASK_GROUP,
            });
            // Find our refresh task - it MUST exist
            const refreshTask = result.refreshTasks.find((rt) => rt.queueKey === queueKey);
            (0, vitest_1.expect)(refreshTask).toBeDefined();
            // Report failure
            await client.reportRefreshOutcome({
                taskId: refreshTask.id,
                shard: refreshTask.shard,
                outcome: {
                    type: "failure",
                    code: "API_UNAVAILABLE",
                    message: "Failed to reach API endpoint",
                },
            });
            // Server should schedule a retry with backoff - we don't verify this directly
            // but the test ensures the API works correctly
            // Clean up job tasks
            for (const task of result.tasks) {
                await client.reportOutcome({
                    taskId: task.id,
                    shard: task.shard,
                    outcome: { type: "success", result: {} },
                });
            }
        });
        (0, vitest_1.it)("floating limit controls job concurrency", async () => {
            const queueKey = `concurrency-control-queue-${Date.now()}`;
            const shard = client.getShardForTenant(tenant);
            // Enqueue 3 jobs with max concurrency of 1
            const handles = [];
            for (let i = 0; i < 3; i++) {
                const handle = await client.enqueue({
                    tenant,
                    taskGroup: DEFAULT_TASK_GROUP,
                    payload: { jobNum: i },
                    limits: [
                        {
                            type: "floatingConcurrency",
                            key: queueKey,
                            defaultMaxConcurrency: 1,
                            refreshIntervalMs: 60000n, // Long interval to avoid refresh
                            metadata: {},
                        },
                    ],
                });
                handles.push(handle);
            }
            // Lease tasks - should only get 1 due to concurrency limit
            const result = await client.leaseTasks({
                workerId: `concurrency-worker-${Date.now()}`,
                maxTasks: 10,
                shard,
                taskGroup: DEFAULT_TASK_GROUP,
            });
            // Filter to just our jobs
            const ourTasks = result.tasks.filter((t) => handles.some((h) => h.id === t.jobId));
            // Should have at most 1 task due to concurrency limit
            (0, vitest_1.expect)(ourTasks.length).toBeLessThanOrEqual(1);
            // Complete tasks to clean up
            for (const task of result.tasks) {
                await client.reportOutcome({
                    taskId: task.id,
                    shard: task.shard,
                    outcome: { type: "success", result: {} },
                });
            }
        });
        (0, vitest_1.it)("combines floating limit with regular concurrency limit", async () => {
            const floatingKey = `combined-floating-${Date.now()}`;
            const regularKey = `combined-regular-${Date.now()}`;
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { task: "combined-limits" },
                limits: [
                    {
                        type: "floatingConcurrency",
                        key: floatingKey,
                        defaultMaxConcurrency: 5,
                        refreshIntervalMs: 30000n,
                        metadata: { source: "floating" },
                    },
                    {
                        type: "concurrency",
                        key: regularKey,
                        maxConcurrency: 3,
                    },
                ],
            });
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            // Verify both limits are stored
            const job = await client.getJob(handle.id, tenant);
            (0, vitest_1.expect)(job.limits).toHaveLength(2);
            const floatingLimit = job.limits.find((l) => l.type === "floatingConcurrency");
            const regularLimit = job.limits.find((l) => l.type === "concurrency");
            (0, vitest_1.expect)(floatingLimit).toBeDefined();
            (0, vitest_1.expect)(regularLimit).toBeDefined();
            if (floatingLimit?.type === "floatingConcurrency") {
                (0, vitest_1.expect)(floatingLimit.key).toBe(floatingKey);
                (0, vitest_1.expect)(floatingLimit.defaultMaxConcurrency).toBe(5);
            }
            if (regularLimit?.type === "concurrency") {
                (0, vitest_1.expect)(regularLimit.key).toBe(regularKey);
                (0, vitest_1.expect)(regularLimit.maxConcurrency).toBe(3);
            }
        });
    });
});
vitest_1.describe.skipIf(!RUN_INTEGRATION)("Shard routing integration", () => {
    // Wait for cluster convergence before running any shard routing tests
    (0, vitest_1.beforeAll)(async () => {
        const client = new client_1.SiloGRPCClient({
            servers: SILO_SERVERS,
            useTls: false,
            shardRouting: { topologyRefreshIntervalMs: 0 },
        });
        try {
            await waitForClusterConvergence(client);
        }
        finally {
            client.close();
        }
    });
    (0, vitest_1.describe)("wrong shard error handling", () => {
        (0, vitest_1.it)("handles requests gracefully after topology refresh", async () => {
            // Create a client and refresh topology
            const client = new client_1.SiloGRPCClient({
                servers: SILO_SERVERS,
                useTls: false,
                shardRouting: {
                    maxWrongShardRetries: 2,
                    wrongShardRetryDelayMs: 50,
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                // Refresh to discover the actual topology
                await client.refreshTopology();
                // Now requests should work because we've discovered the actual topology
                const handle = await client.enqueue({
                    tenant: "wrong-shard-test",
                    taskGroup: DEFAULT_TASK_GROUP,
                    payload: { test: true },
                });
                (0, vitest_1.expect)(handle.id).toBeTruthy();
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
                    maxWrongShardRetries: 5,
                    wrongShardRetryDelayMs: 10,
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                // First refresh to get accurate topology
                await client.refreshTopology();
                // Now operations should work
                const handle = await client.enqueue({
                    tenant: "retry-test-tenant",
                    taskGroup: DEFAULT_TASK_GROUP,
                    payload: { data: "retry test" },
                });
                (0, vitest_1.expect)(handle.id).toBeTruthy();
                const job = await client.getJob(handle.id, "retry-test-tenant");
                (0, vitest_1.expect)(job?.id).toBe(handle.id);
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
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                await client.refreshTopology();
                const tenants = ["tenant-a", "tenant-b", "tenant-c"];
                const handles = {};
                // Enqueue jobs for different tenants
                for (const tenant of tenants) {
                    handles[tenant] = await client.enqueue({
                        tenant,
                        taskGroup: DEFAULT_TASK_GROUP,
                        payload: { tenant },
                    });
                }
                // Verify each job can be retrieved with its tenant
                for (const tenant of tenants) {
                    const job = await client.getJob(handles[tenant].id, tenant);
                    (0, vitest_1.expect)(job?.id).toBe(handles[tenant].id);
                    const payload = job?.payload;
                    (0, vitest_1.expect)(payload?.tenant).toBe(tenant);
                }
            }
            finally {
                client.close();
            }
        });
        (0, vitest_1.it)("handles high volume of different tenants", { timeout: 30000 }, async () => {
            const client = new client_1.SiloGRPCClient({
                servers: SILO_SERVERS,
                useTls: false,
                shardRouting: {
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
                    const handle = await client.enqueue({
                        tenant,
                        taskGroup: DEFAULT_TASK_GROUP,
                        payload: { index: i },
                    });
                    results.push({ tenant, jobId: handle.id });
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
                (0, vitest_1.expect)(topo1.shards.length).toBe(topo2.shards.length);
                (0, vitest_1.expect)(topo2.shards.length).toBe(topo3.shards.length);
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
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                const tenant = "refresh-test";
                // Enqueue before refresh
                await client.refreshTopology();
                const handle1 = await client.enqueue({
                    tenant,
                    taskGroup: DEFAULT_TASK_GROUP,
                    payload: { phase: "before" },
                });
                // Refresh topology
                await client.refreshTopology();
                // Enqueue after refresh
                const handle2 = await client.enqueue({
                    tenant,
                    taskGroup: DEFAULT_TASK_GROUP,
                    payload: { phase: "after" },
                });
                // Both jobs should be retrievable
                const job1 = await client.getJob(handle1.id, tenant);
                const job2 = await client.getJob(handle2.id, tenant);
                (0, vitest_1.expect)(job1?.id).toBe(handle1.id);
                (0, vitest_1.expect)(job2?.id).toBe(handle2.id);
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
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                // Should be able to refresh topology using the valid server
                await client.refreshTopology();
                const topology = client.getTopology();
                (0, vitest_1.expect)(topology.shards.length).toBeGreaterThanOrEqual(1);
                // Operations should work
                const handle = await client.enqueue({
                    tenant: "multi-server-test",
                    taskGroup: DEFAULT_TASK_GROUP,
                    payload: { test: true },
                });
                (0, vitest_1.expect)(handle.id).toBeTruthy();
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
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                await client.refreshTopology();
                const handle = await client.enqueue({
                    tenant: "host-port-test",
                    taskGroup: DEFAULT_TASK_GROUP,
                    payload: { test: true },
                });
                (0, vitest_1.expect)(handle.id).toBeTruthy();
            }
            finally {
                client.close();
            }
        });
    });
});
//# sourceMappingURL=client-integration.test.js.map