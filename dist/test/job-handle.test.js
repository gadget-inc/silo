"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const client_1 = require("../src/client");
// Support comma-separated list of servers for multi-node testing
const SILO_SERVERS = (process.env.SILO_SERVERS ||
    process.env.SILO_SERVER ||
    "localhost:50051").split(",");
const RUN_INTEGRATION = process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";
/**
 * Integration tests for JobHandle.
 * These tests exercise the full stack from JobHandle -> SiloGRPCClient -> gRPC -> Server.
 */
// Default task group for integration tests
const DEFAULT_TASK_GROUP = "job-handle-test-group";
vitest_1.describe.skipIf(!RUN_INTEGRATION)("JobHandle integration", () => {
    let client;
    (0, vitest_1.beforeAll)(async () => {
        client = new client_1.SiloGRPCClient({
            servers: SILO_SERVERS,
            useTls: false,
            shardRouting: {
                topologyRefreshIntervalMs: 0,
            },
        });
        await client.refreshTopology();
    });
    (0, vitest_1.afterAll)(() => {
        client.close();
    });
    (0, vitest_1.describe)("properties", () => {
        (0, vitest_1.it)("exposes id and tenant from enqueue", async () => {
            const tenant = "handle-props-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "props" },
            });
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            (0, vitest_1.expect)(handle.tenant).toBe(tenant);
        });
        (0, vitest_1.it)("exposes id and tenant from factory", () => {
            const handle = client.handle("test-job-id", "test-tenant");
            (0, vitest_1.expect)(handle.id).toBe("test-job-id");
            (0, vitest_1.expect)(handle.tenant).toBe("test-tenant");
        });
        (0, vitest_1.it)("has undefined tenant when not provided", () => {
            const handle = client.handle("test-job-id");
            (0, vitest_1.expect)(handle.id).toBe("test-job-id");
            (0, vitest_1.expect)(handle.tenant).toBeUndefined();
        });
    });
    (0, vitest_1.describe)("getJob", () => {
        (0, vitest_1.it)("returns job details", async () => {
            const tenant = "handle-getjob-tenant";
            const payload = { action: "test-action", value: 123 };
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload,
                priority: 25,
                metadata: { source: "job-handle-test" },
            });
            const job = await handle.getJob();
            (0, vitest_1.expect)(job.id).toBe(handle.id);
            (0, vitest_1.expect)(job.priority).toBe(25);
            (0, vitest_1.expect)(job.status).toBe(client_1.JobStatus.Scheduled);
            (0, vitest_1.expect)(job.metadata?.source).toBe("job-handle-test");
            (0, vitest_1.expect)(job.payload).toEqual(payload);
        });
        (0, vitest_1.it)("throws JobNotFoundError when job does not exist", async () => {
            const handle = client.handle("nonexistent-job-id", "some-tenant");
            await (0, vitest_1.expect)(handle.getJob()).rejects.toThrow(client_1.JobNotFoundError);
            await (0, vitest_1.expect)(handle.getJob()).rejects.toThrow(/not found/);
        });
    });
    (0, vitest_1.describe)("getStatus", () => {
        (0, vitest_1.it)("returns Scheduled for newly enqueued job", async () => {
            const tenant = "handle-status-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "status" },
            });
            const status = await handle.getStatus();
            (0, vitest_1.expect)(status).toBe(client_1.JobStatus.Scheduled);
        });
        (0, vitest_1.it)("returns Running after task is leased", async () => {
            const tenant = "handle-running-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "running" },
            });
            const shard = client.getShardForTenant(tenant);
            // Lease the task
            let task;
            for (let i = 0; i < 5 && !task; i++) {
                const result = await client.leaseTasks({
                    workerId: `handle-running-worker-${Date.now()}`,
                    maxTasks: 50,
                    shard,
                    taskGroup: DEFAULT_TASK_GROUP,
                });
                task = result.tasks.find((t) => t.jobId === handle.id);
                if (!task)
                    await new Promise((r) => setTimeout(r, 100));
            }
            (0, vitest_1.expect)(task).toBeDefined();
            const status = await handle.getStatus();
            (0, vitest_1.expect)(status).toBe(client_1.JobStatus.Running);
            // Clean up - report success
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                outcome: { type: "success", result: {} },
            });
        });
        (0, vitest_1.it)("throws JobNotFoundError when job does not exist", async () => {
            const handle = client.handle("nonexistent-status-id", "some-tenant");
            await (0, vitest_1.expect)(handle.getStatus()).rejects.toThrow(client_1.JobNotFoundError);
        });
    });
    (0, vitest_1.describe)("cancel", () => {
        (0, vitest_1.it)("cancels a scheduled job", async () => {
            const tenant = "handle-cancel-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "cancel" },
            });
            // Verify it's scheduled
            (0, vitest_1.expect)(await handle.getStatus()).toBe(client_1.JobStatus.Scheduled);
            // Cancel it
            await handle.cancel();
            // Verify it's cancelled
            (0, vitest_1.expect)(await handle.getStatus()).toBe(client_1.JobStatus.Cancelled);
        });
        (0, vitest_1.it)("throws JobNotFoundError when job does not exist", async () => {
            const handle = client.handle("nonexistent-cancel-id", "some-tenant");
            await (0, vitest_1.expect)(handle.cancel()).rejects.toThrow(client_1.JobNotFoundError);
        });
    });
    (0, vitest_1.describe)("delete", () => {
        (0, vitest_1.it)("deletes a completed job", async () => {
            const tenant = "handle-delete-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "delete" },
            });
            const shard = client.getShardForTenant(tenant);
            // Lease and complete the task
            let task;
            for (let i = 0; i < 5 && !task; i++) {
                const result = await client.leaseTasks({
                    workerId: `handle-delete-worker-${Date.now()}`,
                    maxTasks: 50,
                    shard,
                    taskGroup: DEFAULT_TASK_GROUP,
                });
                task = result.tasks.find((t) => t.jobId === handle.id);
                if (!task)
                    await new Promise((r) => setTimeout(r, 100));
            }
            (0, vitest_1.expect)(task).toBeDefined();
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                outcome: { type: "success", result: {} },
            });
            // Delete the completed job
            await handle.delete();
            // Job should no longer exist
            await (0, vitest_1.expect)(handle.getJob()).rejects.toThrow(client_1.JobNotFoundError);
        });
        (0, vitest_1.it)("succeeds silently when job does not exist (idempotent)", async () => {
            const handle = client.handle("nonexistent-delete-id", "some-tenant");
            // Delete is idempotent - doesn't throw for non-existent jobs
            await handle.delete();
        });
    });
    (0, vitest_1.describe)("awaitResult", () => {
        (0, vitest_1.it)("returns success result when job completes successfully", async () => {
            const tenant = "handle-await-success-tenant";
            const resultData = { processed: true, count: 42 };
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "await-success" },
            });
            const shard = client.getShardForTenant(tenant);
            // Lease and complete the task
            let task;
            for (let i = 0; i < 5 && !task; i++) {
                const result = await client.leaseTasks({
                    workerId: `handle-await-success-worker-${Date.now()}`,
                    maxTasks: 50,
                    shard,
                    taskGroup: DEFAULT_TASK_GROUP,
                });
                task = result.tasks.find((t) => t.jobId === handle.id);
                if (!task)
                    await new Promise((r) => setTimeout(r, 100));
            }
            (0, vitest_1.expect)(task).toBeDefined();
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                outcome: { type: "success", result: resultData },
            });
            // Await the result
            const result = await handle.awaitResult({
                pollIntervalMs: 50,
                timeoutMs: 5000,
            });
            (0, vitest_1.expect)(result.status).toBe(client_1.JobStatus.Succeeded);
            (0, vitest_1.expect)(result.result).toEqual(resultData);
        });
        (0, vitest_1.it)("returns failure result when job fails", async () => {
            const tenant = "handle-await-fail-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "await-fail" },
            });
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
                if (!task)
                    await new Promise((r) => setTimeout(r, 100));
            }
            (0, vitest_1.expect)(task).toBeDefined();
            await client.reportOutcome({
                shard: task.shard,
                taskId: task.id,
                outcome: {
                    type: "failure",
                    code: "TEST_ERROR",
                    data: { reason: "intentional failure" },
                },
            });
            // Await the result
            const result = await handle.awaitResult({
                pollIntervalMs: 50,
                timeoutMs: 5000,
            });
            (0, vitest_1.expect)(result.status).toBe(client_1.JobStatus.Failed);
            if (result.status === client_1.JobStatus.Failed) {
                (0, vitest_1.expect)(result.errorCode).toBe("TEST_ERROR");
                (0, vitest_1.expect)(result.errorData).toEqual({ reason: "intentional failure" });
            }
        });
        (0, vitest_1.it)("returns cancelled result when job is cancelled", async () => {
            const tenant = "handle-await-cancel-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "await-cancel" },
            });
            // Cancel the job
            await handle.cancel();
            // Await the result
            const result = await handle.awaitResult({
                pollIntervalMs: 50,
                timeoutMs: 5000,
            });
            (0, vitest_1.expect)(result.status).toBe(client_1.JobStatus.Cancelled);
        });
        (0, vitest_1.it)("throws timeout error when job does not complete in time", async () => {
            const tenant = "handle-await-timeout-tenant";
            const handle = await client.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "await-timeout" },
                // Schedule far in the future so it won't be leased
                startAtMs: BigInt(Date.now() + 60000),
            });
            await (0, vitest_1.expect)(handle.awaitResult({ pollIntervalMs: 50, timeoutMs: 200 })).rejects.toThrow(/Timeout/);
        });
        (0, vitest_1.it)("throws JobNotFoundError when job does not exist", async () => {
            const handle = client.handle("nonexistent-await-id", "some-tenant");
            await (0, vitest_1.expect)(handle.awaitResult({ pollIntervalMs: 50, timeoutMs: 1000 })).rejects.toThrow(client_1.JobNotFoundError);
        });
    });
});
//# sourceMappingURL=job-handle.test.js.map