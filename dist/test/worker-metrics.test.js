"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const sdk_metrics_1 = require("@opentelemetry/sdk-metrics");
const worker_1 = require("../src/worker");
const client_1 = require("../src/client");
function createMockClient(options) {
    return {
        leaseTasks: options?.leaseTasks ?? vitest_1.vi.fn().mockResolvedValue({ tasks: [], refreshTasks: [] }),
        reportOutcome: options?.reportOutcome ?? vitest_1.vi.fn().mockResolvedValue(undefined),
        heartbeat: options?.heartbeat ?? vitest_1.vi.fn().mockResolvedValue({ cancelled: false }),
        cancelJob: vitest_1.vi.fn().mockResolvedValue(undefined),
    };
}
function tasksResult(tasks) {
    return { tasks, refreshTasks: [] };
}
function createTask(id, jobId, shard = "00000000-0000-0000-0000-000000000001") {
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
    };
}
/** A simple in-process metric reader for testing */
class TestReader extends sdk_metrics_1.MetricReader {
    onForceFlush() {
        return Promise.resolve();
    }
    onShutdown() {
        return Promise.resolve();
    }
    selectAggregationTemporality() {
        return sdk_metrics_1.AggregationTemporality.CUMULATIVE;
    }
}
async function collectMetrics(reader) {
    const { resourceMetrics } = await reader.collect();
    const allMetrics = {};
    for (const scopeMetrics of resourceMetrics.scopeMetrics) {
        for (const metric of scopeMetrics.metrics) {
            allMetrics[metric.descriptor.name] = metric.dataPoints;
        }
    }
    return allMetrics;
}
function getMetricValue(allMetrics, name) {
    const points = allMetrics[name];
    if (!points || points.length === 0)
        return 0;
    return points[0].value;
}
(0, vitest_1.describe)("SiloWorker metrics", () => {
    let meterProvider;
    let metricReader;
    (0, vitest_1.beforeEach)(() => {
        metricReader = new TestReader();
        meterProvider = new sdk_metrics_1.MeterProvider({ readers: [metricReader] });
    });
    (0, vitest_1.afterEach)(async () => {
        await meterProvider.shutdown();
    });
    (0, vitest_1.it)("exposes a workerMetrics property", () => {
        const client = createMockClient();
        const handler = async () => ({ type: "success", result: {} });
        const worker = new worker_1.SiloWorker({
            client,
            workerId: "test-worker",
            taskGroup: "default",
            handler,
            meter: meterProvider.getMeter("test"),
        });
        (0, vitest_1.expect)(worker.workerMetrics).toBeDefined();
        (0, vitest_1.expect)(worker.workerMetrics.pollCounter).toBeDefined();
        (0, vitest_1.expect)(worker.workerMetrics.emptyPollCounter).toBeDefined();
        (0, vitest_1.expect)(worker.workerMetrics.availableTaskSlots).toBeDefined();
    });
    (0, vitest_1.it)("increments poll counter on each poll", async () => {
        let pollCount = 0;
        const leaseTasks = vitest_1.vi.fn().mockImplementation(async () => {
            pollCount++;
            return tasksResult([]);
        });
        const client = createMockClient({ leaseTasks });
        const handler = async () => ({ type: "success", result: {} });
        const meter = meterProvider.getMeter("test");
        const worker = new worker_1.SiloWorker({
            client,
            workerId: "test-worker",
            taskGroup: "default",
            handler,
            pollIntervalMs: 10,
            meter,
        });
        worker.start();
        // Wait for a few polls
        while (pollCount < 3) {
            await new Promise((r) => setTimeout(r, 10));
        }
        await worker.stop();
        const allMetrics = await collectMetrics(metricReader);
        const value = getMetricValue(allMetrics, "silo.worker.polls");
        (0, vitest_1.expect)(value).toBeGreaterThanOrEqual(3);
    });
    (0, vitest_1.it)("increments empty poll counter when no tasks returned", async () => {
        let pollCount = 0;
        const leaseTasks = vitest_1.vi.fn().mockImplementation(async () => {
            pollCount++;
            return tasksResult([]);
        });
        const client = createMockClient({ leaseTasks });
        const handler = async () => ({ type: "success", result: {} });
        const meter = meterProvider.getMeter("test");
        const worker = new worker_1.SiloWorker({
            client,
            workerId: "test-worker",
            taskGroup: "default",
            handler,
            pollIntervalMs: 10,
            meter,
        });
        worker.start();
        while (pollCount < 3) {
            await new Promise((r) => setTimeout(r, 10));
        }
        await worker.stop();
        const allMetrics = await collectMetrics(metricReader);
        const pollValue = getMetricValue(allMetrics, "silo.worker.polls");
        const emptyValue = getMetricValue(allMetrics, "silo.worker.polls.empty");
        // All polls were empty, so both counters should match
        (0, vitest_1.expect)(emptyValue).toBe(pollValue);
    });
    (0, vitest_1.it)("does not increment empty poll counter when tasks are returned", async () => {
        let pollCount = 0;
        const leaseTasks = vitest_1.vi.fn().mockImplementation(async () => {
            pollCount++;
            // First poll returns a task, subsequent ones are empty
            if (pollCount === 1) {
                return tasksResult([createTask("task-1", "job-1")]);
            }
            return tasksResult([]);
        });
        const client = createMockClient({ leaseTasks });
        const handler = async () => ({ type: "success", result: {} });
        const meter = meterProvider.getMeter("test");
        const worker = new worker_1.SiloWorker({
            client,
            workerId: "test-worker",
            taskGroup: "default",
            handler,
            pollIntervalMs: 10,
            heartbeatIntervalMs: 100000,
            meter,
        });
        worker.start();
        while (pollCount < 3) {
            await new Promise((r) => setTimeout(r, 10));
        }
        await worker.stop();
        const allMetrics = await collectMetrics(metricReader);
        const pollValue = getMetricValue(allMetrics, "silo.worker.polls");
        const emptyValue = getMetricValue(allMetrics, "silo.worker.polls.empty");
        // At least the first poll had tasks, so empty < total
        (0, vitest_1.expect)(emptyValue).toBeLessThan(pollValue);
    });
    (0, vitest_1.it)("reports available task slots gauge", async () => {
        const client = createMockClient();
        const handler = async () => ({ type: "success", result: {} });
        const meter = meterProvider.getMeter("test");
        const worker = new worker_1.SiloWorker({
            client,
            workerId: "test-worker",
            taskGroup: "default",
            handler,
            maxConcurrentTasks: 10,
            meter,
        });
        // Before starting, all slots should be available
        (0, vitest_1.expect)(worker.availableTaskSlots).toBe(10);
        const allMetrics = await collectMetrics(metricReader);
        const slotsValue = getMetricValue(allMetrics, "silo.worker.available_task_slots");
        (0, vitest_1.expect)(slotsValue).toBe(10);
    });
    (0, vitest_1.it)("available task slots decreases when tasks are running", async () => {
        let resolveTask;
        const taskStarted = new Promise((resolve) => {
            resolveTask = resolve;
        });
        let finishTask;
        const taskBlocked = new Promise((resolve) => {
            finishTask = resolve;
        });
        let pollCount = 0;
        const leaseTasks = vitest_1.vi.fn().mockImplementation(async () => {
            pollCount++;
            if (pollCount === 1) {
                return tasksResult([createTask("task-1", "job-1")]);
            }
            return tasksResult([]);
        });
        const client = createMockClient({ leaseTasks });
        const handler = async () => {
            resolveTask();
            await taskBlocked;
            return { type: "success", result: {} };
        };
        const meter = meterProvider.getMeter("test");
        const worker = new worker_1.SiloWorker({
            client,
            workerId: "test-worker",
            taskGroup: "default",
            handler,
            maxConcurrentTasks: 5,
            pollIntervalMs: 10,
            heartbeatIntervalMs: 100000,
            meter,
        });
        worker.start();
        await taskStarted;
        // One task is running, so available slots should be 4
        (0, vitest_1.expect)(worker.availableTaskSlots).toBe(4);
        const allMetrics = await collectMetrics(metricReader);
        const slotsValue = getMetricValue(allMetrics, "silo.worker.available_task_slots");
        (0, vitest_1.expect)(slotsValue).toBe(4);
        finishTask();
        await worker.stop();
    });
    (0, vitest_1.it)("uses global meter provider when no meter is specified", () => {
        const client = createMockClient();
        const handler = async () => ({ type: "success", result: {} });
        // No meter option — should not throw
        const worker = new worker_1.SiloWorker({
            client,
            workerId: "test-worker",
            taskGroup: "default",
            handler,
        });
        (0, vitest_1.expect)(worker.workerMetrics).toBeDefined();
    });
});
//# sourceMappingURL=worker-metrics.test.js.map