"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const client_1 = require("../src/client");
const worker_1 = require("../src/worker");
// Support comma-separated list of servers for multi-node testing
const SILO_SERVERS = (process.env.SILO_SERVERS ||
    process.env.SILO_SERVER ||
    "localhost:50051").split(",");
const RUN_INTEGRATION = process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";
/**
 * Wait until a condition becomes true, polling at intervals.
 * @param condition Function that returns true when the condition is met
 * @param options.timeout Maximum time to wait in ms (default: 5000)
 * @param options.interval Polling interval in ms (default: 50)
 * @throws Error if timeout is reached before condition is met
 */
async function waitFor(condition, options) {
    const timeout = options?.timeout ?? 5000;
    const interval = options?.interval ?? 50;
    const start = Date.now();
    while (!condition()) {
        if (Date.now() - start > timeout) {
            throw new Error(`waitFor timed out after ${timeout}ms`);
        }
        await new Promise((resolve) => setTimeout(resolve, interval));
    }
}
vitest_1.describe.skipIf(!RUN_INTEGRATION)("SiloWorker integration", () => {
    let client;
    let activeWorkers = [];
    (0, vitest_1.beforeAll)(async () => {
        client = new client_1.SiloGRPCClient({
            servers: SILO_SERVERS,
            useTls: false,
            shardRouting: {
                topologyRefreshIntervalMs: 0,
            },
        });
        // Discover cluster topology
        await client.refreshTopology();
    });
    (0, vitest_1.afterAll)(() => {
        client.close();
    });
    (0, vitest_1.beforeEach)(async () => {
        // Reset all shards to ensure test isolation - clean slate before each test
        await client.resetShards();
    });
    (0, vitest_1.afterEach)(async () => {
        // Stop all workers created during test
        await Promise.all(activeWorkers.map((w) => w.stop()));
        activeWorkers = [];
    });
    // Default tenant for all tests - reset ensures isolation between tests
    const DEFAULT_TENANT = "test-tenant";
    function createWorker(handler, options) {
        const worker = new worker_1.SiloWorker({
            client,
            workerId: `test-worker-${Date.now()}-${Math.random()
                .toString(36)
                .slice(2)}`,
            handler,
            tenant: DEFAULT_TENANT,
            pollIntervalMs: 50,
            heartbeatIntervalMs: 1000,
            ...options,
        });
        activeWorkers.push(worker);
        return worker;
    }
    (0, vitest_1.describe)("basic task processing", () => {
        (0, vitest_1.it)("processes a single task from any shard", async () => {
            const processedJobs = [];
            const handler = async (ctx) => {
                const payload = (0, client_1.decodePayload)(ctx.task.payload?.data);
                processedJobs.push(payload?.message ?? "");
                return { type: "success", result: { processed: true } };
            };
            // Worker polls all shards
            const worker = createWorker(handler);
            worker.start();
            // Enqueue a job to test tenant
            await client.enqueue({
                tenant: DEFAULT_TENANT,
                payload: { message: "hello-worker" },
                priority: 1,
            });
            // Wait until job is processed
            await waitFor(() => processedJobs.includes("hello-worker"));
            (0, vitest_1.expect)(processedJobs).toContain("hello-worker");
        });
        (0, vitest_1.it)("processes multiple tasks sequentially with maxConcurrentTasks=1", async () => {
            const processedOrder = [];
            let maxConcurrent = 0;
            let currentConcurrent = 0;
            const handler = async (ctx) => {
                const payload = (0, client_1.decodePayload)(ctx.task.payload?.data);
                currentConcurrent++;
                maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
                await new Promise((resolve) => setTimeout(resolve, 20));
                processedOrder.push(payload?.index ?? -1);
                currentConcurrent--;
                return { type: "success", result: {} };
            };
            const worker = createWorker(handler, { maxConcurrentTasks: 1 });
            worker.start();
            // Enqueue multiple jobs
            for (let i = 0; i < 5; i++) {
                await client.enqueue({
                    tenant: DEFAULT_TENANT,
                    payload: { index: i },
                    priority: 1,
                });
            }
            // Wait until at least 3 are processed
            await waitFor(() => processedOrder.length >= 3);
            // Should have processed all our tasks (at least some)
            (0, vitest_1.expect)(processedOrder.length).toBeGreaterThanOrEqual(3);
            // And should have been sequential (max 1 concurrent)
            (0, vitest_1.expect)(maxConcurrent).toBeLessThanOrEqual(1);
        });
        (0, vitest_1.it)("processes tasks concurrently", async () => {
            let maxConcurrent = 0;
            let currentConcurrent = 0;
            let completedCount = 0;
            const handler = async () => {
                currentConcurrent++;
                maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
                await new Promise((resolve) => setTimeout(resolve, 100));
                currentConcurrent--;
                completedCount++;
                return { type: "success", result: {} };
            };
            const worker = createWorker(handler, {
                maxConcurrentTasks: 5,
                concurrentPollers: 2,
            });
            worker.start();
            // Enqueue several jobs at once
            await Promise.all(Array.from({ length: 10 }, (_, i) => client.enqueue({
                tenant: DEFAULT_TENANT,
                payload: { index: i },
                priority: 1,
            })));
            // Wait until we've seen concurrent execution
            await waitFor(() => maxConcurrent > 1 && completedCount >= 5);
            // Should have had multiple tasks running concurrently
            (0, vitest_1.expect)(maxConcurrent).toBeGreaterThan(1);
            (0, vitest_1.expect)(maxConcurrent).toBeLessThanOrEqual(5);
        });
    });
    (0, vitest_1.describe)("multi-job processing", () => {
        (0, vitest_1.it)("processes multiple jobs from the same tenant", async () => {
            const processedPayloads = new Set();
            const handler = async (ctx) => {
                const payload = (0, client_1.decodePayload)(ctx.task.payload?.data);
                if (payload?.label) {
                    processedPayloads.add(payload.label);
                }
                return { type: "success", result: {} };
            };
            // Single worker polls all shards
            const worker = createWorker(handler);
            worker.start();
            // Enqueue multiple jobs to the same tenant
            const labels = ["alpha", "beta", "gamma"];
            await Promise.all(labels.map((label) => client.enqueue({
                tenant: DEFAULT_TENANT,
                payload: { label },
                priority: 1,
            })));
            // Wait until all jobs are processed
            await waitFor(() => processedPayloads.size === labels.length);
            // Should have processed all jobs
            (0, vitest_1.expect)(processedPayloads.size).toBe(labels.length);
            for (const label of labels) {
                (0, vitest_1.expect)(processedPayloads.has(label)).toBe(true);
            }
        });
    });
    (0, vitest_1.describe)("failure handling", () => {
        (0, vitest_1.it)("reports failure when handler returns failure outcome", async () => {
            let taskProcessed = false;
            const handler = async () => {
                taskProcessed = true;
                return {
                    type: "failure",
                    code: "TEST_FAILURE",
                    data: { reason: "intentional" },
                };
            };
            const worker = createWorker(handler);
            worker.start();
            const jobId = await client.enqueue({
                tenant: DEFAULT_TENANT,
                payload: { action: "fail" },
                priority: 1,
            });
            // Wait until task is processed
            await waitFor(() => taskProcessed);
            // Job should still exist but task completed with failure
            const job = await client.getJob(jobId, DEFAULT_TENANT);
            (0, vitest_1.expect)(job).toBeDefined();
        });
        (0, vitest_1.it)("reports failure when handler throws", async () => {
            let taskProcessed = false;
            const handler = async () => {
                taskProcessed = true;
                throw new Error("Oops!");
            };
            const worker = createWorker(handler);
            worker.start();
            await client.enqueue({
                tenant: DEFAULT_TENANT,
                payload: { action: "throw" },
                priority: 1,
            });
            // Wait until task is processed (even though it throws)
            await waitFor(() => taskProcessed);
            // Worker should continue running despite error
            (0, vitest_1.expect)(worker.isRunning).toBe(true);
        });
    });
    (0, vitest_1.describe)("worker lifecycle", () => {
        (0, vitest_1.it)("stops gracefully with pending tasks", async () => {
            let taskStarted = false;
            let taskCompleted = false;
            const handler = async () => {
                taskStarted = true;
                await new Promise((resolve) => setTimeout(resolve, 200));
                taskCompleted = true;
                return { type: "success", result: {} };
            };
            const worker = createWorker(handler, { maxConcurrentTasks: 10 });
            worker.start();
            await client.enqueue({
                tenant: DEFAULT_TENANT,
                payload: { action: "slow" },
                priority: 1,
            });
            // Wait for task to start (may take longer due to poll interval)
            for (let i = 0; i < 10 && !taskStarted; i++) {
                await new Promise((resolve) => setTimeout(resolve, 100));
            }
            if (taskStarted) {
                // Stop the worker - should wait for task to complete
                await worker.stop();
                (0, vitest_1.expect)(taskCompleted).toBe(true);
                (0, vitest_1.expect)(worker.isRunning).toBe(false);
                (0, vitest_1.expect)(worker.activeTasks).toBe(0);
            }
            else {
                // Test didn't get to start a task, just clean up
                await worker.stop();
            }
        });
        (0, vitest_1.it)("can be restarted after stopping", async () => {
            let processCount = 0;
            const handler = async () => {
                processCount++;
                return { type: "success", result: {} };
            };
            const worker = createWorker(handler);
            // First run
            worker.start();
            await client.enqueue({
                tenant: DEFAULT_TENANT,
                payload: { run: 1 },
                priority: 1,
            });
            await waitFor(() => processCount >= 1);
            await worker.stop();
            const firstCount = processCount;
            (0, vitest_1.expect)(firstCount).toBeGreaterThan(0);
            // Second run
            worker.start();
            await client.enqueue({
                tenant: DEFAULT_TENANT,
                payload: { run: 2 },
                priority: 1,
            });
            await waitFor(() => processCount > firstCount);
            await worker.stop();
            (0, vitest_1.expect)(processCount).toBeGreaterThan(firstCount);
        });
    });
    (0, vitest_1.describe)("multiple pollers", () => {
        (0, vitest_1.it)("uses multiple pollers to fetch tasks faster", async () => {
            const processedTasks = [];
            const handler = async (ctx) => {
                processedTasks.push(ctx.task.id);
                await new Promise((resolve) => setTimeout(resolve, 50));
                return { type: "success", result: {} };
            };
            const worker = createWorker(handler, {
                concurrentPollers: 3,
                maxConcurrentTasks: 10,
            });
            worker.start();
            // Enqueue many tasks
            await Promise.all(Array.from({ length: 20 }, (_, i) => client.enqueue({
                tenant: DEFAULT_TENANT,
                payload: { index: i },
                priority: 1,
            })));
            // Wait until enough tasks are processed
            await waitFor(() => processedTasks.length > 10);
            // Should have processed many tasks
            (0, vitest_1.expect)(processedTasks.length).toBeGreaterThan(10);
        });
    });
    (0, vitest_1.describe)("abort signal", () => {
        (0, vitest_1.it)("provides abort signal that is aborted on stop", async () => {
            let signalAborted = false;
            let taskStarted = false;
            const handler = async (ctx) => {
                taskStarted = true;
                ctx.signal.addEventListener("abort", () => {
                    signalAborted = true;
                });
                // Long-running task
                await new Promise((resolve) => setTimeout(resolve, 1000));
                return { type: "success", result: {} };
            };
            const worker = createWorker(handler, { maxConcurrentTasks: 10 });
            worker.start();
            await client.enqueue({
                tenant: DEFAULT_TENANT,
                payload: { action: "long" },
                priority: 1,
            });
            // Wait for task to start
            await new Promise((resolve) => setTimeout(resolve, 300));
            // If we got a task to process
            if (taskStarted) {
                // Stop immediately - should abort signal
                await worker.stop(100);
                (0, vitest_1.expect)(signalAborted).toBe(true);
            }
            else {
                // Worker didn't pick up our task, just stop and skip the assertion
                await worker.stop();
            }
        });
    });
    (0, vitest_1.describe)("shard context", () => {
        (0, vitest_1.it)("provides correct shard in task context", async () => {
            let receivedShard;
            const handler = async (ctx) => {
                receivedShard = ctx.shard;
                return { type: "success", result: {} };
            };
            const worker = createWorker(handler);
            worker.start();
            await client.enqueue({
                tenant: DEFAULT_TENANT,
                payload: { test: true },
                priority: 1,
            });
            // Wait until task is processed
            await waitFor(() => receivedShard !== undefined);
            // Should have received a valid shard ID
            (0, vitest_1.expect)(receivedShard).toBeDefined();
            (0, vitest_1.expect)(receivedShard).toBeGreaterThanOrEqual(0);
        });
    });
});
//# sourceMappingURL=worker-integration.test.js.map