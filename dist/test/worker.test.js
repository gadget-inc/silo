"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const worker_1 = require("../src/worker");
const client_1 = require("../src/client");
// Mock client for unit tests
function createMockClient(options) {
    return {
        leaseTasks: options?.leaseTasks ?? vitest_1.vi.fn().mockResolvedValue({ tasks: [], refreshTasks: [] }),
        reportOutcome: options?.reportOutcome ?? vitest_1.vi.fn().mockResolvedValue(undefined),
        heartbeat: options?.heartbeat ?? vitest_1.vi.fn().mockResolvedValue({ cancelled: false }),
        cancelJob: vitest_1.vi.fn().mockResolvedValue(undefined),
    };
}
// Helper to wrap tasks array in LeaseTasksResult
function tasksResult(tasks) {
    return { tasks, refreshTasks: [] };
}
function createTask(id, jobId, shard = "00000000-0000-0000-0000-000000000001", tenantId) {
    return {
        id,
        jobId,
        attemptNumber: 1,
        relativeAttemptNumber: 1,
        leaseMs: 30000n,
        payload: {
            encoding: {
                oneofKind: "msgpack",
                msgpack: (0, client_1.encodeBytes)({ test: "data" }),
            },
        },
        priority: 10,
        shard,
        taskGroup: "default",
        isLastAttempt: false,
        metadata: {},
        limits: [],
        tenantId,
    };
}
(0, vitest_1.describe)("SiloWorker", () => {
    (0, vitest_1.describe)("constructor", () => {
        (0, vitest_1.it)("creates a worker with default options", () => {
            const client = createMockClient();
            const handler = async () => ({
                type: "success",
                result: {},
            });
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
            });
            (0, vitest_1.expect)(worker.isRunning).toBe(false);
            (0, vitest_1.expect)(worker.activeTasks).toBe(0);
            (0, vitest_1.expect)(worker.workerId).toBe("test-worker");
            (0, vitest_1.expect)(worker.taskGroup).toBe("default");
            (0, vitest_1.expect)(worker.concurrentPollers).toBe(1);
            (0, vitest_1.expect)(worker.maxConcurrentTasks).toBe(10);
            (0, vitest_1.expect)(worker.tasksPerPoll).toBe(5);
            (0, vitest_1.expect)(worker.pollIntervalMs).toBe(1000);
            (0, vitest_1.expect)(worker.heartbeatIntervalMs).toBe(5000);
        });
        (0, vitest_1.it)("creates a worker with custom options", () => {
            const client = createMockClient();
            const handler = async () => ({
                type: "success",
                result: {},
            });
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                concurrentPollers: 3,
                maxConcurrentTasks: 20,
                tasksPerPoll: 5,
                pollIntervalMs: 500,
                heartbeatIntervalMs: 2000,
            });
            (0, vitest_1.expect)(worker.isRunning).toBe(false);
            (0, vitest_1.expect)(worker.workerId).toBe("test-worker");
            (0, vitest_1.expect)(worker.taskGroup).toBe("default");
            (0, vitest_1.expect)(worker.concurrentPollers).toBe(3);
            (0, vitest_1.expect)(worker.maxConcurrentTasks).toBe(20);
            (0, vitest_1.expect)(worker.tasksPerPoll).toBe(5);
            (0, vitest_1.expect)(worker.pollIntervalMs).toBe(500);
            (0, vitest_1.expect)(worker.heartbeatIntervalMs).toBe(2000);
        });
    });
    (0, vitest_1.describe)("start and stop", () => {
        (0, vitest_1.it)("starts and stops the worker", async () => {
            const client = createMockClient();
            const handler = async () => ({
                type: "success",
                result: {},
            });
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 50,
            });
            (0, vitest_1.expect)(worker.isRunning).toBe(false);
            worker.start();
            (0, vitest_1.expect)(worker.isRunning).toBe(true);
            // Starting again should be a no-op
            worker.start();
            (0, vitest_1.expect)(worker.isRunning).toBe(true);
            await worker.stop();
            (0, vitest_1.expect)(worker.isRunning).toBe(false);
            // Stopping again should be a no-op
            await worker.stop();
            (0, vitest_1.expect)(worker.isRunning).toBe(false);
        });
        (0, vitest_1.it)("polls for tasks when started", async () => {
            const leaseTasks = vitest_1.vi.fn().mockResolvedValue(tasksResult([]));
            const client = createMockClient({ leaseTasks });
            const handler = async () => ({
                type: "success",
                result: {},
            });
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
            });
            worker.start();
            // Wait for a few poll cycles
            await new Promise((resolve) => setTimeout(resolve, 50));
            await worker.stop();
            (0, vitest_1.expect)(leaseTasks).toHaveBeenCalled();
            (0, vitest_1.expect)(leaseTasks).toHaveBeenCalledWith({
                workerId: "test-worker",
                maxTasks: vitest_1.expect.any(Number),
                taskGroup: "default",
            }, vitest_1.expect.any(Number));
        });
        (0, vitest_1.it)("uses multiple concurrent pollers", async () => {
            let pollCount = 0;
            const leaseTasks = vitest_1.vi.fn().mockImplementation(async () => {
                pollCount++;
                await new Promise((resolve) => setTimeout(resolve, 20));
                return tasksResult([]);
            });
            const client = createMockClient({ leaseTasks });
            const handler = async () => ({
                type: "success",
                result: {},
            });
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                concurrentPollers: 3,
                pollIntervalMs: 10,
            });
            worker.start();
            // Wait for pollers to start
            await new Promise((resolve) => setTimeout(resolve, 30));
            // With 3 concurrent pollers and 20ms poll time, we should see multiple polls
            const countDuringRun = pollCount;
            await worker.stop();
            // Should have had multiple polls happening
            (0, vitest_1.expect)(countDuringRun).toBeGreaterThanOrEqual(3);
        });
    });
    (0, vitest_1.describe)("task execution", () => {
        (0, vitest_1.it)("executes tasks and reports success", async () => {
            const task = createTask("task-1", "job-1");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const client = createMockClient({ leaseTasks, reportOutcome });
            const handler = vitest_1.vi.fn().mockResolvedValue({
                type: "success",
                result: { processed: true },
            });
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
            });
            worker.start();
            // Wait for task to be processed
            await new Promise((resolve) => setTimeout(resolve, 100));
            await worker.stop();
            (0, vitest_1.expect)(handler).toHaveBeenCalledWith(vitest_1.expect.objectContaining({
                task: vitest_1.expect.objectContaining({
                    id: task.id,
                    jobId: task.jobId,
                    payload: { test: "data" }, // Decoded payload
                }),
            }));
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledWith({
                taskId: "task-1",
                outcome: { type: "success", result: { processed: true } },
                shard: "00000000-0000-0000-0000-000000000001",
                tenant: undefined,
            });
        });
        (0, vitest_1.it)("passes tenant_id through to reportOutcome and heartbeat for multitenant tasks", async () => {
            const task = createTask("task-mt", "job-mt", "00000000-0000-0000-0000-000000000001", "tenant-abc");
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const heartbeat = vitest_1.vi.fn().mockResolvedValue({ cancelled: false });
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            // Handler that takes long enough for at least one heartbeat to fire
            const handler = vitest_1.vi.fn().mockImplementation(async () => {
                await new Promise((resolve) => setTimeout(resolve, 150));
                return { type: "success", result: { result: "ok" } };
            });
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                heartbeatIntervalMs: 50,
                pollIntervalMs: 10,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 500));
            await worker.stop();
            // reportOutcome should include the tenant from the task
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledWith({
                taskId: "task-mt",
                outcome: { type: "success", result: { result: "ok" } },
                shard: "00000000-0000-0000-0000-000000000001",
                tenant: "tenant-abc",
            });
            // heartbeat should include the tenant from the task
            (0, vitest_1.expect)(heartbeat).toHaveBeenCalledWith("test-worker", "task-mt", "00000000-0000-0000-0000-000000000001", "tenant-abc");
        });
        (0, vitest_1.it)("passes limits to handler in task context", async () => {
            const taskWithLimits = {
                id: "task-limits-1",
                jobId: "job-limits-1",
                attemptNumber: 1,
                relativeAttemptNumber: 1,
                leaseMs: 30000n,
                payload: {
                    encoding: {
                        oneofKind: "msgpack",
                        msgpack: (0, client_1.encodeBytes)({ test: "data" }),
                    },
                },
                priority: 10,
                shard: "00000000-0000-0000-0000-000000000001",
                taskGroup: "default",
                isLastAttempt: false,
                metadata: {},
                limits: [
                    {
                        limit: {
                            oneofKind: "concurrency",
                            concurrency: {
                                key: "test-concurrency-key",
                                maxConcurrency: 5,
                            },
                        },
                    },
                    {
                        limit: {
                            oneofKind: "rateLimit",
                            rateLimit: {
                                name: "test-rate-limit",
                                uniqueKey: "test-rate-key",
                                limit: 100n,
                                durationMs: 60000n,
                                hits: 1,
                                algorithm: 0, // TokenBucket
                                behavior: 0,
                                retryPolicy: {
                                    initialBackoffMs: 1000n,
                                    maxBackoffMs: 30000n,
                                    backoffMultiplier: 2.0,
                                    maxRetries: 5,
                                },
                            },
                        },
                    },
                ],
            };
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([taskWithLimits]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const client = createMockClient({ leaseTasks, reportOutcome });
            let receivedLimits = [];
            const handler = async (ctx) => {
                receivedLimits = ctx.task.limits;
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 100));
            await worker.stop();
            // Verify limits were passed to handler
            (0, vitest_1.expect)(receivedLimits).toHaveLength(2);
            // Check concurrency limit
            const concurrencyLimit = receivedLimits[0];
            (0, vitest_1.expect)(concurrencyLimit.limit.oneofKind).toBe("concurrency");
            (0, vitest_1.expect)(concurrencyLimit.limit.concurrency.key).toBe("test-concurrency-key");
            (0, vitest_1.expect)(concurrencyLimit.limit.concurrency.maxConcurrency).toBe(5);
            // Check rate limit
            const rateLimit = receivedLimits[1];
            (0, vitest_1.expect)(rateLimit.limit.oneofKind).toBe("rateLimit");
            (0, vitest_1.expect)(rateLimit.limit.rateLimit.name).toBe("test-rate-limit");
            (0, vitest_1.expect)(rateLimit.limit.rateLimit.uniqueKey).toBe("test-rate-key");
        });
        (0, vitest_1.it)("executes tasks and reports failure", async () => {
            const task = createTask("task-2", "job-2");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const client = createMockClient({ leaseTasks, reportOutcome });
            const handler = async () => ({
                type: "failure",
                code: "VALIDATION_ERROR",
                data: { field: "email" },
            });
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 100));
            await worker.stop();
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledWith({
                taskId: "task-2",
                outcome: {
                    type: "failure",
                    code: "VALIDATION_ERROR",
                    data: { field: "email" },
                },
                shard: "00000000-0000-0000-0000-000000000001",
            });
        });
        (0, vitest_1.it)("reports failure when handler throws", async () => {
            const task = createTask("task-3", "job-3");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const client = createMockClient({ leaseTasks, reportOutcome });
            const handler = async () => {
                throw new Error("Something went wrong");
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                onError: () => { }, // Suppress error logging
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 100));
            await worker.stop();
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledWith({
                taskId: "task-3",
                outcome: {
                    type: "failure",
                    code: "HANDLER_ERROR",
                    data: vitest_1.expect.objectContaining({
                        message: "Something went wrong",
                    }),
                },
                shard: "00000000-0000-0000-0000-000000000001",
            });
        });
        (0, vitest_1.it)("respects maxConcurrentTasks limit", async () => {
            // Create tasks that will be returned in batches
            const batch1 = [createTask("task-a", "job-a"), createTask("task-b", "job-b")];
            const batch2 = [createTask("task-c", "job-c"), createTask("task-d", "job-d")];
            const batch3 = [createTask("task-e", "job-e")];
            let activeTasks = 0;
            let maxActiveTasks = 0;
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult(batch1))
                .mockResolvedValueOnce(tasksResult(batch2))
                .mockResolvedValueOnce(tasksResult(batch3))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const client = createMockClient({ leaseTasks, reportOutcome });
            const handler = async () => {
                activeTasks++;
                maxActiveTasks = Math.max(maxActiveTasks, activeTasks);
                await new Promise((resolve) => setTimeout(resolve, 50));
                activeTasks--;
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                maxConcurrentTasks: 2,
                pollIntervalMs: 10,
            });
            worker.start();
            // Wait for all tasks to complete
            await new Promise((resolve) => setTimeout(resolve, 500));
            await worker.stop();
            // Should never exceed maxConcurrentTasks
            (0, vitest_1.expect)(maxActiveTasks).toBeLessThanOrEqual(2);
            // But should have processed all 5 tasks
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledTimes(5);
        });
        (0, vitest_1.it)("tracks active task count", async () => {
            const task = createTask("task-x", "job-x");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const client = createMockClient({ leaseTasks, reportOutcome });
            let activeCountDuringExecution = 0;
            let workerRef;
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler: async () => {
                    activeCountDuringExecution = workerRef.activeTasks;
                    await new Promise((resolve) => setTimeout(resolve, 50));
                    return { type: "success", result: {} };
                },
                pollIntervalMs: 10,
            });
            workerRef = worker;
            (0, vitest_1.expect)(worker.activeTasks).toBe(0);
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 30));
            // During execution, active count should be > 0
            (0, vitest_1.expect)(activeCountDuringExecution).toBe(1);
            await new Promise((resolve) => setTimeout(resolve, 100));
            await worker.stop();
            // After completion, active count should be 0
            (0, vitest_1.expect)(worker.activeTasks).toBe(0);
        });
    });
    (0, vitest_1.describe)("heartbeats", () => {
        (0, vitest_1.it)("sends heartbeats while task is executing", async () => {
            const task = createTask("task-hb", "job-hb");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const heartbeat = vitest_1.vi.fn().mockResolvedValue({ cancelled: false });
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            const handler = async () => {
                // Simulate long-running task
                await new Promise((resolve) => setTimeout(resolve, 150));
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                heartbeatIntervalMs: 30,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 200));
            await worker.stop();
            // Should have sent multiple heartbeats
            // heartbeat(workerId, taskId, shard, tenant)
            (0, vitest_1.expect)(heartbeat).toHaveBeenCalledWith("test-worker", "task-hb", "00000000-0000-0000-0000-000000000001", undefined);
            (0, vitest_1.expect)(heartbeat.mock.calls.length).toBeGreaterThanOrEqual(2);
        });
        (0, vitest_1.it)("stops heartbeats after task completes", async () => {
            const task = createTask("task-hb2", "job-hb2");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const heartbeat = vitest_1.vi.fn().mockResolvedValue({ cancelled: false });
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            const handler = async () => {
                await new Promise((resolve) => setTimeout(resolve, 20));
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                heartbeatIntervalMs: 50,
            });
            worker.start();
            // Wait for task to complete
            await new Promise((resolve) => setTimeout(resolve, 50));
            const heartbeatCountAfterComplete = heartbeat.mock.calls.length;
            // Wait more time - no more heartbeats should be sent
            await new Promise((resolve) => setTimeout(resolve, 100));
            await worker.stop();
            // Heartbeat count should not have increased after task completed
            (0, vitest_1.expect)(heartbeat.mock.calls.length).toBe(heartbeatCountAfterComplete);
        });
        (0, vitest_1.it)("stops all heartbeats when two intervals for one task id are concurrently live", async () => {
            const task = createTask("task-dup", "job-dup");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const heartbeat = vitest_1.vi.fn().mockResolvedValue({ cancelled: false });
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            // Long enough that both copies are executing concurrently
            const handler = async () => {
                await new Promise((resolve) => setTimeout(resolve, 100));
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                heartbeatIntervalMs: 30,
            });
            // Bypass the duplicate-delivery guard so the second poll registers a
            // second live interval for the same task id, exercising interval
            // cleanup under the exact overlap the guard normally prevents
            const activeExecutions = worker
                ._activeExecutions;
            vitest_1.vi.spyOn(activeExecutions, "has").mockReturnValue(false);
            worker.start();
            // Wait for both executions to settle
            await new Promise((resolve) => setTimeout(resolve, 250));
            const heartbeatCountAfterSettle = heartbeat.mock.calls.length;
            // Wait several heartbeat intervals - no interval may still be live
            await new Promise((resolve) => setTimeout(resolve, 120));
            await worker.stop();
            (0, vitest_1.expect)(heartbeat.mock.calls.length).toBe(heartbeatCountAfterSettle);
        });
        (0, vitest_1.it)("ignores an in-flight heartbeat that rejects lease-gone after normal completion", async () => {
            const task = createTask("task-late-hb", "job-late-hb");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            // The heartbeat is on the wire when the handler settles: the server
            // deletes the lease during reportOutcome, so the late response is a
            // lease-gone rejection for a task that completed normally
            const heartbeat = vitest_1.vi.fn().mockImplementation(() => new Promise((_resolve, reject) => {
                setTimeout(() => reject(new client_1.TaskNotFoundError("task-late-hb", "lease not found")), 100);
            }));
            const onError = vitest_1.vi.fn();
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            const handler = async () => {
                await new Promise((resolve) => setTimeout(resolve, 50));
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                heartbeatIntervalMs: 30,
                onError,
            });
            worker.start();
            // Handler settles at ~50ms; the ~30ms heartbeat's rejection lands at
            // ~130ms, well after the outcome was reported
            await new Promise((resolve) => setTimeout(resolve, 300));
            await worker.stop();
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(onError).not.toHaveBeenCalled();
        });
        (0, vitest_1.it)("issues no heartbeat after reportOutcome is invoked for a completing task", async () => {
            const task = createTask("task-hb-order", "job-hb-order");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            let reportOutcomeInvoked = false;
            let heartbeatsAfterReport = 0;
            // Hold the report pending across several heartbeat intervals so any
            // still-live interval would fire while it is in flight
            const reportOutcome = vitest_1.vi.fn().mockImplementation(async () => {
                reportOutcomeInvoked = true;
                await new Promise((resolve) => setTimeout(resolve, 120));
            });
            const heartbeat = vitest_1.vi.fn().mockImplementation(async () => {
                if (reportOutcomeInvoked) {
                    heartbeatsAfterReport++;
                }
                return { cancelled: false };
            });
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            const handler = async () => {
                await new Promise((resolve) => setTimeout(resolve, 80));
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                heartbeatIntervalMs: 30,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 300));
            await worker.stop();
            // The interval was live during execution...
            (0, vitest_1.expect)(heartbeat).toHaveBeenCalled();
            // ...but stopped before the outcome report went out
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(heartbeatsAfterReport).toBe(0);
        });
        (0, vitest_1.it)("issues no heartbeat after the cancelled outcome report is invoked", async () => {
            const task = createTask("task-hb-cancel", "job-hb-cancel");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            let reportOutcomeInvoked = false;
            let heartbeatsAfterReport = 0;
            const reportOutcome = vitest_1.vi.fn().mockImplementation(async () => {
                reportOutcomeInvoked = true;
                await new Promise((resolve) => setTimeout(resolve, 120));
            });
            const heartbeat = vitest_1.vi.fn().mockImplementation(async () => {
                if (reportOutcomeInvoked) {
                    heartbeatsAfterReport++;
                }
                return { cancelled: false };
            });
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            const handler = async (ctx) => {
                await new Promise((resolve) => setTimeout(resolve, 40));
                await ctx.cancel();
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                heartbeatIntervalMs: 30,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 300));
            await worker.stop();
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ outcome: { type: "cancelled" } }));
            (0, vitest_1.expect)(heartbeatsAfterReport).toBe(0);
        });
    });
    (0, vitest_1.describe)("duplicate delivery", () => {
        (0, vitest_1.it)("executes a task delivered twice only once and reports the duplicate", async () => {
            const task = createTask("task-dedup", "job-dedup");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const onError = vitest_1.vi.fn();
            const client = createMockClient({ leaseTasks, reportOutcome });
            // Long enough that the second delivery arrives while the first executes
            const handler = vitest_1.vi.fn().mockImplementation(async () => {
                await new Promise((resolve) => setTimeout(resolve, 100));
                return { type: "success", result: {} };
            });
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                onError,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 250));
            await worker.stop();
            (0, vitest_1.expect)(handler).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(onError).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(onError).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ code: "SILO_DUPLICATE_TASK_DELIVERY" }), vitest_1.expect.objectContaining({ taskId: "task-dedup" }));
        });
        (0, vitest_1.it)("executes a task re-delivered after its first execution settled", async () => {
            const task = createTask("task-redeliver", "job-redeliver");
            let deliveredSecondCopy = false;
            const leaseTasks = vitest_1.vi.fn().mockImplementation(async () => {
                // First poll delivers the task; a later poll re-delivers it once the
                // first execution has fully settled
                if (leaseTasks.mock.calls.length === 1) {
                    return tasksResult([task]);
                }
                if (reportOutcome.mock.calls.length >= 1 && !deliveredSecondCopy) {
                    deliveredSecondCopy = true;
                    return tasksResult([task]);
                }
                return tasksResult([]);
            });
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const onError = vitest_1.vi.fn();
            const client = createMockClient({ leaseTasks, reportOutcome });
            const handler = vitest_1.vi.fn().mockImplementation(async () => {
                await new Promise((resolve) => setTimeout(resolve, 20));
                return { type: "success", result: {} };
            });
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                onError,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 300));
            await worker.stop();
            (0, vitest_1.expect)(handler).toHaveBeenCalledTimes(2);
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledTimes(2);
            (0, vitest_1.expect)(onError).not.toHaveBeenCalled();
        });
        (0, vitest_1.it)("drops a duplicate delivery of an actively-executing refresh task", async () => {
            const refreshTask = {
                id: "refresh-dup",
                queueKey: "queue-1",
                currentMaxConcurrency: 5,
                lastRefreshedAtMs: 0n,
                metadata: {},
                leaseMs: 30000n,
                shard: "00000000-0000-0000-0000-000000000001",
                taskGroup: "default",
            };
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce({ tasks: [], refreshTasks: [refreshTask] })
                .mockResolvedValueOnce({ tasks: [], refreshTasks: [refreshTask] })
                .mockResolvedValue(tasksResult([]));
            const reportRefreshOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const onError = vitest_1.vi.fn();
            const client = {
                leaseTasks,
                reportOutcome: vitest_1.vi.fn().mockResolvedValue(undefined),
                reportRefreshOutcome,
                heartbeat: vitest_1.vi.fn().mockResolvedValue({ cancelled: false }),
                cancelJob: vitest_1.vi.fn().mockResolvedValue(undefined),
            };
            const refreshHandler = vitest_1.vi.fn().mockImplementation(async () => {
                await new Promise((resolve) => setTimeout(resolve, 100));
                return 7;
            });
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler: async () => ({ type: "success", result: {} }),
                refreshHandler,
                pollIntervalMs: 10,
                onError,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 250));
            await worker.stop();
            (0, vitest_1.expect)(refreshHandler).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(reportRefreshOutcome).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(onError).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(onError).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ code: "SILO_DUPLICATE_TASK_DELIVERY" }), vitest_1.expect.objectContaining({ taskId: "refresh-dup" }));
        });
        (0, vitest_1.it)("exports DuplicateTaskDeliveryError and TaskLeaseLostError from the package barrel", async () => {
            const barrel = await Promise.resolve().then(() => __importStar(require("../src/index")));
            (0, vitest_1.expect)(barrel.DuplicateTaskDeliveryError).toBeDefined();
            const err = new barrel.DuplicateTaskDeliveryError("task-1", "job-1");
            (0, vitest_1.expect)(err.code).toBe("SILO_DUPLICATE_TASK_DELIVERY");
            (0, vitest_1.expect)(err.taskId).toBe("task-1");
            (0, vitest_1.expect)(err.jobId).toBe("job-1");
            (0, vitest_1.expect)(barrel.TaskLeaseLostError).toBeDefined();
            const leaseLost = new barrel.TaskLeaseLostError("task-1", "job-1", new Error("lease not found"));
            (0, vitest_1.expect)(leaseLost.code).toBe("SILO_TASK_LEASE_LOST");
            (0, vitest_1.expect)(leaseLost.taskId).toBe("task-1");
            (0, vitest_1.expect)(leaseLost.jobId).toBe("job-1");
        });
    });
    (0, vitest_1.describe)("lost lease", () => {
        (0, vitest_1.it)("aborts, stops heartbeating, and reports no outcome when the lease is gone", async () => {
            const task = createTask("task-ll", "job-ll");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const heartbeat = vitest_1.vi
                .fn()
                .mockRejectedValue(new client_1.TaskNotFoundError("task-ll", "lease not found"));
            const onError = vitest_1.vi.fn();
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            let signalAbortedInHandler = false;
            const handler = async (ctx) => {
                // Run until the cancellation signal fires (or a generous timeout)
                await new Promise((resolve) => {
                    const timer = setTimeout(resolve, 500);
                    ctx.cancellationSignal.addEventListener("abort", () => {
                        clearTimeout(timer);
                        resolve();
                    }, { once: true });
                });
                signalAbortedInHandler = ctx.cancellationSignal.aborted;
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                heartbeatIntervalMs: 30,
                onError,
            });
            worker.start();
            // Enough time for the heartbeat rejection, handler wind-down, and any
            // further heartbeat intervals that should no longer fire
            await new Promise((resolve) => setTimeout(resolve, 250));
            await worker.stop();
            (0, vitest_1.expect)(signalAbortedInHandler).toBe(true);
            (0, vitest_1.expect)(reportOutcome).not.toHaveBeenCalled();
            (0, vitest_1.expect)(heartbeat).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(onError).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(onError).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ code: "SILO_TASK_LEASE_LOST" }), vitest_1.expect.objectContaining({ taskId: "task-ll" }));
            const err = onError.mock.calls[0][0];
            (0, vitest_1.expect)(err.message).toContain("task-ll");
            (0, vitest_1.expect)(err.message).toContain("job-ll");
        });
        (0, vitest_1.it)("treats a lease-owner-mismatch heartbeat rejection as a lost lease", async () => {
            const task = createTask("task-om", "job-om");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const heartbeat = vitest_1.vi
                .fn()
                .mockRejectedValue(new client_1.SiloFailedPreconditionError("lease owner mismatch"));
            const onError = vitest_1.vi.fn();
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            let signalAbortedInHandler = false;
            const handler = async (ctx) => {
                await new Promise((resolve) => {
                    const timer = setTimeout(resolve, 500);
                    ctx.cancellationSignal.addEventListener("abort", () => {
                        clearTimeout(timer);
                        resolve();
                    }, { once: true });
                });
                signalAbortedInHandler = ctx.cancellationSignal.aborted;
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                heartbeatIntervalMs: 30,
                onError,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 250));
            await worker.stop();
            (0, vitest_1.expect)(signalAbortedInHandler).toBe(true);
            (0, vitest_1.expect)(reportOutcome).not.toHaveBeenCalled();
            (0, vitest_1.expect)(heartbeat).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(onError).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(onError).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ code: "SILO_TASK_LEASE_LOST" }), vitest_1.expect.objectContaining({ taskId: "task-om" }));
        });
        (0, vitest_1.it)("executes a retry re-delivered after the lease was lost", async () => {
            const task = createTask("task-ll-retry", "job-ll-retry");
            let leaseLostReported = false;
            let deliveredRetry = false;
            const leaseTasks = vitest_1.vi.fn().mockImplementation(async () => {
                if (leaseTasks.mock.calls.length === 1) {
                    return tasksResult([task]);
                }
                // Re-deliver the same task id once the lease-lost event fired
                if (leaseLostReported && !deliveredRetry) {
                    deliveredRetry = true;
                    return tasksResult([task]);
                }
                return tasksResult([]);
            });
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            // First delivery's heartbeat loses the lease; the retry's heartbeats succeed
            const heartbeat = vitest_1.vi.fn().mockImplementation(async () => {
                if (!deliveredRetry) {
                    throw new client_1.TaskNotFoundError("task-ll-retry", "lease not found");
                }
                return { cancelled: false };
            });
            const onError = vitest_1.vi.fn().mockImplementation(() => {
                leaseLostReported = true;
            });
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            const handler = vitest_1.vi
                .fn()
                .mockImplementation(async (ctx) => {
                await new Promise((resolve) => {
                    const timer = setTimeout(resolve, 60);
                    ctx.cancellationSignal.addEventListener("abort", () => {
                        clearTimeout(timer);
                        resolve();
                    }, { once: true });
                });
                return { type: "success", result: {} };
            });
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                heartbeatIntervalMs: 30,
                onError,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 400));
            await worker.stop();
            // Handler ran for the original delivery and again for the retry
            (0, vitest_1.expect)(handler).toHaveBeenCalledTimes(2);
            // Only the retry reported an outcome (no duplicate-delivery error either)
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(onError).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(onError).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ code: "SILO_TASK_LEASE_LOST" }), vitest_1.expect.anything());
        });
        (0, vitest_1.it)("never starts the handler for a task whose lease was lost while queued", async () => {
            const task1 = createTask("task-q1", "job-q1");
            const task2 = createTask("task-q2", "job-q2");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task1, task2]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            // task-q2 loses its lease while it waits in the queue behind task-q1
            const heartbeat = vitest_1.vi.fn().mockImplementation(async (_worker, taskId) => {
                if (taskId === "task-q2") {
                    throw new client_1.TaskNotFoundError("task-q2", "lease not found");
                }
                return { cancelled: false };
            });
            const onError = vitest_1.vi.fn();
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            let releaseFirstTask;
            const firstTaskBlocked = new Promise((resolve) => {
                releaseFirstTask = resolve;
            });
            const handledTaskIds = [];
            const handler = async (ctx) => {
                handledTaskIds.push(ctx.task.id);
                if (ctx.task.id === "task-q1") {
                    await firstTaskBlocked;
                }
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                maxConcurrentTasks: 1,
                pollIntervalMs: 10,
                heartbeatIntervalMs: 30,
                onError,
            });
            worker.start();
            // Let task-q2's queued heartbeat reject lease-gone before task-q1 frees the slot
            await new Promise((resolve) => setTimeout(resolve, 100));
            releaseFirstTask();
            await new Promise((resolve) => setTimeout(resolve, 100));
            await worker.stop();
            (0, vitest_1.expect)(handledTaskIds).toEqual(["task-q1"]);
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ taskId: "task-q1" }));
            (0, vitest_1.expect)(onError).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(onError).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ code: "SILO_TASK_LEASE_LOST" }), vitest_1.expect.objectContaining({ taskId: "task-q2" }));
        });
        (0, vitest_1.it)("continues executing when a heartbeat fails with a wrong-shard NOT_FOUND", async () => {
            const task = createTask("task-ws", "job-ws");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const heartbeat = vitest_1.vi
                .fn()
                .mockRejectedValue(new client_1.TaskNotFoundError("task-ws", "shard not found"));
            const onError = vitest_1.vi.fn();
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            let signalAbortedInHandler = false;
            const handler = async (ctx) => {
                await new Promise((resolve) => setTimeout(resolve, 100));
                signalAbortedInHandler = ctx.cancellationSignal.aborted;
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                heartbeatIntervalMs: 30,
                onError,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 200));
            await worker.stop();
            (0, vitest_1.expect)(signalAbortedInHandler).toBe(false);
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ taskId: "task-ws", outcome: { type: "success", result: {} } }));
            // The failures surface as plain heartbeat errors, not lease-lost
            (0, vitest_1.expect)(onError).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ code: "SILO_TASK_NOT_FOUND" }), vitest_1.expect.objectContaining({ taskId: "task-ws" }));
        });
        (0, vitest_1.it)("continues executing when a heartbeat fails with a stale-topology FAILED_PRECONDITION", async () => {
            const task = createTask("task-st", "job-st");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const heartbeat = vitest_1.vi
                .fn()
                .mockRejectedValue(new client_1.SiloFailedPreconditionError("tenant 'a' is not within shard 00000000-0000-0000-0000-000000000001 range [0, 100); refresh topology and retry"));
            const onError = vitest_1.vi.fn();
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            let signalAbortedInHandler = false;
            const handler = async (ctx) => {
                await new Promise((resolve) => setTimeout(resolve, 100));
                signalAbortedInHandler = ctx.cancellationSignal.aborted;
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                heartbeatIntervalMs: 30,
                onError,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 200));
            await worker.stop();
            (0, vitest_1.expect)(signalAbortedInHandler).toBe(false);
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(onError).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ code: "SILO_FAILED_PRECONDITION" }), vitest_1.expect.objectContaining({ taskId: "task-st" }));
        });
        (0, vitest_1.it)("continues executing when a heartbeat fails with a transient error", async () => {
            const task = createTask("task-tr", "job-tr");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const heartbeat = vitest_1.vi.fn().mockRejectedValue(new Error("connection reset"));
            const onError = vitest_1.vi.fn();
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            let signalAbortedInHandler = false;
            const handler = async (ctx) => {
                await new Promise((resolve) => setTimeout(resolve, 100));
                signalAbortedInHandler = ctx.cancellationSignal.aborted;
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                heartbeatIntervalMs: 30,
                onError,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 200));
            await worker.stop();
            (0, vitest_1.expect)(signalAbortedInHandler).toBe(false);
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(onError).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ message: "connection reset" }), vitest_1.expect.objectContaining({ taskId: "task-tr" }));
        });
    });
    (0, vitest_1.describe)("TaskExecution lease-lost state", () => {
        (0, vitest_1.it)("marks lease-lost once, aborts with the lease-lost reason, and suppresses cancelled reporting", async () => {
            const { TaskExecution } = await Promise.resolve().then(() => __importStar(require("../src/TaskExecution")));
            const client = createMockClient();
            const execution = new TaskExecution({ id: "task-1", jobId: "job-1" }, "test-worker", client);
            (0, vitest_1.expect)(execution.markLeaseLost()).toBe(true);
            (0, vitest_1.expect)(execution.signal.aborted).toBe(true);
            (0, vitest_1.expect)(execution.cancellationReason).toBe("lease-lost");
            (0, vitest_1.expect)(execution.isLeaseLost).toBe(true);
            // A concurrent in-flight heartbeat rejecting with the same signal
            // does not win the latch a second time
            (0, vitest_1.expect)(execution.markLeaseLost()).toBe(false);
        });
    });
    (0, vitest_1.describe)("error handling", () => {
        (0, vitest_1.it)("calls onError when polling fails", async () => {
            const leaseTasks = vitest_1.vi.fn().mockRejectedValue(new Error("Connection failed"));
            const client = createMockClient({ leaseTasks });
            const onError = vitest_1.vi.fn();
            const handler = async () => ({
                type: "success",
                result: {},
            });
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                onError,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 50));
            await worker.stop();
            (0, vitest_1.expect)(onError).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ message: "Connection failed" }));
        });
        (0, vitest_1.it)("calls onError when heartbeat fails", async () => {
            const task = createTask("task-err", "job-err");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const heartbeat = vitest_1.vi.fn().mockRejectedValue(new Error("Heartbeat failed"));
            const onError = vitest_1.vi.fn();
            const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });
            const handler = async () => {
                await new Promise((resolve) => setTimeout(resolve, 100));
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                heartbeatIntervalMs: 20,
                onError,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 150));
            await worker.stop();
            (0, vitest_1.expect)(onError).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ message: "Heartbeat failed" }), vitest_1.expect.objectContaining({ taskId: "task-err" }));
        });
        (0, vitest_1.it)("continues processing after errors", async () => {
            const task1 = createTask("task-1", "job-1");
            const task2 = createTask("task-2", "job-2");
            let pollCount = 0;
            const leaseTasks = vitest_1.vi.fn().mockImplementation(async () => {
                pollCount++;
                if (pollCount === 1) {
                    throw new Error("First poll failed");
                }
                if (pollCount === 2) {
                    return tasksResult([task1]);
                }
                if (pollCount === 3) {
                    return tasksResult([task2]);
                }
                return tasksResult([]);
            });
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const client = createMockClient({ leaseTasks, reportOutcome });
            const handler = async () => ({
                type: "success",
                result: {},
            });
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
                onError: () => { }, // Suppress error logging
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 150));
            await worker.stop();
            // Should have processed tasks despite the error
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledTimes(2);
        });
    });
    (0, vitest_1.describe)("graceful shutdown", () => {
        (0, vitest_1.it)("does not abort task signal when shutdown begins", async () => {
            const task = createTask("task-shutdown", "job-shutdown");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const client = createMockClient({ leaseTasks, reportOutcome });
            let signalWasAbortedDuringTask = false;
            let taskStartedResolve;
            let continueTaskResolve;
            const taskStarted = new Promise((r) => {
                taskStartedResolve = r;
            });
            const continueTask = new Promise((r) => {
                continueTaskResolve = r;
            });
            const handler = async (ctx) => {
                // Signal task has started
                taskStartedResolve();
                // Wait until we're told to continue (after stop() is called)
                await continueTask;
                // Check if signal was aborted after shutdown was called
                signalWasAbortedDuringTask = ctx.cancellationSignal.aborted;
                return { type: "success", result: { completed: true } };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
            });
            worker.start();
            // Wait for task to start
            await taskStarted;
            // Call stop - this begins shutdown
            const stopPromise = worker.stop();
            // Allow task to continue after stop has been called
            continueTaskResolve();
            await stopPromise;
            // The signal should NOT have been aborted
            (0, vitest_1.expect)(signalWasAbortedDuringTask).toBe(false);
            // The task should have completed successfully
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledWith({
                taskId: "task-shutdown",
                outcome: { type: "success", result: { completed: true } },
                shard: "00000000-0000-0000-0000-000000000001",
            });
        });
        (0, vitest_1.it)("allows tasks to complete after shutdown begins", async () => {
            const task = createTask("task-complete", "job-complete");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const client = createMockClient({ leaseTasks, reportOutcome });
            let taskCompleted = false;
            let resolveTaskStarted;
            const taskStarted = new Promise((resolve) => {
                resolveTaskStarted = resolve;
            });
            const handler = async () => {
                resolveTaskStarted();
                // Simulate some work that takes time
                await new Promise((resolve) => setTimeout(resolve, 100));
                taskCompleted = true;
                return { type: "success", result: { done: true } };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
            });
            worker.start();
            // Wait for task to start
            await taskStarted;
            // Immediately call stop
            await worker.stop();
            // Task should have completed despite shutdown being called
            (0, vitest_1.expect)(taskCompleted).toBe(true);
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledWith({
                taskId: "task-complete",
                outcome: { type: "success", result: { done: true } },
                shard: "00000000-0000-0000-0000-000000000001",
            });
        });
        (0, vitest_1.it)("does not report cancelled outcome for tasks running during shutdown", async () => {
            const task = createTask("task-not-cancelled", "job-not-cancelled");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const client = createMockClient({ leaseTasks, reportOutcome });
            let resolveTaskStarted;
            const taskStarted = new Promise((resolve) => {
                resolveTaskStarted = resolve;
            });
            const handler = async () => {
                resolveTaskStarted();
                await new Promise((resolve) => setTimeout(resolve, 50));
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
            });
            worker.start();
            await taskStarted;
            await worker.stop();
            // Should NOT have reported a cancelled outcome
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(reportOutcome).toHaveBeenCalledWith({
                taskId: "task-not-cancelled",
                outcome: { type: "success", result: {} },
                shard: "00000000-0000-0000-0000-000000000001",
            });
            // Verify it was NOT called with cancelled
            (0, vitest_1.expect)(reportOutcome).not.toHaveBeenCalledWith(vitest_1.expect.objectContaining({
                outcome: { type: "cancelled" },
            }));
        });
    });
    (0, vitest_1.describe)("TaskContext", () => {
        (0, vitest_1.it)("provides abort signal and cancel method in context", async () => {
            const task = createTask("task-sig", "job-sig");
            const leaseTasks = vitest_1.vi
                .fn()
                .mockResolvedValueOnce(tasksResult([task]))
                .mockResolvedValue(tasksResult([]));
            const reportOutcome = vitest_1.vi.fn().mockResolvedValue(undefined);
            const client = createMockClient({ leaseTasks, reportOutcome });
            let receivedSignal;
            let receivedCancel;
            const handler = async (ctx) => {
                receivedSignal = ctx.cancellationSignal;
                receivedCancel = ctx.cancel.bind(ctx);
                return { type: "success", result: {} };
            };
            const worker = new worker_1.SiloWorker({
                client,
                workerId: "test-worker",
                taskGroup: "default",
                handler,
                pollIntervalMs: 10,
            });
            worker.start();
            await new Promise((resolve) => setTimeout(resolve, 50));
            await worker.stop();
            (0, vitest_1.expect)(receivedSignal).toBeDefined();
            (0, vitest_1.expect)(receivedSignal).toBeInstanceOf(AbortSignal);
            (0, vitest_1.expect)(receivedCancel).toBeDefined();
            (0, vitest_1.expect)(typeof receivedCancel).toBe("function");
        });
    });
});
//# sourceMappingURL=worker.test.js.map