import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  MeterProvider,
  MetricReader,
  AggregationTemporality,
  type DataPoint,
} from "@opentelemetry/sdk-metrics";
import { SiloWorker, type TaskHandler } from "../src/worker";
import type { SiloGRPCClient, LeaseTasksResult } from "../src/client";
import { encodeBytes } from "../src/client";
import type { Task } from "../src/pb/silo";

function createMockClient(options?: {
  leaseTasks?: (opts: unknown) => Promise<LeaseTasksResult>;
  reportOutcome?: (opts: unknown) => Promise<void>;
  heartbeat?: (
    workerId: string,
    taskId: string,
    shard: number,
    tenant?: string,
  ) => Promise<{ cancelled: boolean }>;
}): SiloGRPCClient {
  return {
    leaseTasks: options?.leaseTasks ?? vi.fn().mockResolvedValue({ tasks: [], refreshTasks: [] }),
    reportOutcome: options?.reportOutcome ?? vi.fn().mockResolvedValue(undefined),
    heartbeat: options?.heartbeat ?? vi.fn().mockResolvedValue({ cancelled: false }),
    cancelJob: vi.fn().mockResolvedValue(undefined),
  } as unknown as SiloGRPCClient;
}

function tasksResult(tasks: Task[]): LeaseTasksResult {
  return { tasks, refreshTasks: [] };
}

function createTask(
  id: string,
  jobId: string,
  shard: string = "00000000-0000-0000-0000-000000000001",
): Task {
  return {
    id,
    jobId,
    attemptNumber: 1,
    relativeAttemptNumber: 1,
    leaseMs: 30000n,
    payload: {
      encoding: {
        oneofKind: "msgpack",
        msgpack: encodeBytes({ test: "data" }),
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
class TestReader extends MetricReader {
  protected onForceFlush(): Promise<void> {
    return Promise.resolve();
  }
  protected onShutdown(): Promise<void> {
    return Promise.resolve();
  }
  selectAggregationTemporality(): AggregationTemporality {
    return AggregationTemporality.CUMULATIVE;
  }
}

async function collectMetrics(reader: TestReader) {
  const { resourceMetrics } = await reader.collect();
  const allMetrics: Record<string, DataPoint<number>[]> = {};
  for (const scopeMetrics of resourceMetrics.scopeMetrics) {
    for (const metric of scopeMetrics.metrics) {
      allMetrics[metric.descriptor.name] = metric.dataPoints as DataPoint<number>[];
    }
  }
  return allMetrics;
}

function getMetricValue(allMetrics: Record<string, DataPoint<number>[]>, name: string): number {
  const points = allMetrics[name];
  if (!points || points.length === 0) return 0;
  return points[0].value;
}

describe("SiloWorker metrics", () => {
  let meterProvider: MeterProvider;
  let metricReader: TestReader;

  beforeEach(() => {
    metricReader = new TestReader();
    meterProvider = new MeterProvider({ readers: [metricReader] });
  });

  afterEach(async () => {
    await meterProvider.shutdown();
  });

  it("exposes a workerMetrics property", () => {
    const client = createMockClient();
    const handler: TaskHandler = async () => ({ type: "success", result: {} });
    const worker = new SiloWorker({
      client,
      workerId: "test-worker",
      taskGroup: "default",
      handler,
      meter: meterProvider.getMeter("test"),
    });

    expect(worker.workerMetrics).toBeDefined();
    expect(worker.workerMetrics.pollCounter).toBeDefined();
    expect(worker.workerMetrics.emptyPollCounter).toBeDefined();
    expect(worker.workerMetrics.availableTaskSlots).toBeDefined();
  });

  it("increments poll counter on each poll", async () => {
    let pollCount = 0;
    const leaseTasks = vi.fn().mockImplementation(async () => {
      pollCount++;
      return tasksResult([]);
    });
    const client = createMockClient({ leaseTasks });
    const handler: TaskHandler = async () => ({ type: "success", result: {} });
    const meter = meterProvider.getMeter("test");

    const worker = new SiloWorker({
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
    expect(value).toBeGreaterThanOrEqual(3);
  });

  it("increments empty poll counter when no tasks returned", async () => {
    let pollCount = 0;
    const leaseTasks = vi.fn().mockImplementation(async () => {
      pollCount++;
      return tasksResult([]);
    });
    const client = createMockClient({ leaseTasks });
    const handler: TaskHandler = async () => ({ type: "success", result: {} });
    const meter = meterProvider.getMeter("test");

    const worker = new SiloWorker({
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
    expect(emptyValue).toBe(pollValue);
  });

  it("does not increment empty poll counter when tasks are returned", async () => {
    let pollCount = 0;
    const leaseTasks = vi.fn().mockImplementation(async () => {
      pollCount++;
      // First poll returns a task, subsequent ones are empty
      if (pollCount === 1) {
        return tasksResult([createTask("task-1", "job-1")]);
      }
      return tasksResult([]);
    });
    const client = createMockClient({ leaseTasks });
    const handler: TaskHandler = async () => ({ type: "success", result: {} });
    const meter = meterProvider.getMeter("test");

    const worker = new SiloWorker({
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
    expect(emptyValue).toBeLessThan(pollValue);
  });

  it("reports available task slots gauge", async () => {
    const client = createMockClient();
    const handler: TaskHandler = async () => ({ type: "success", result: {} });
    const meter = meterProvider.getMeter("test");

    const worker = new SiloWorker({
      client,
      workerId: "test-worker",
      taskGroup: "default",
      handler,
      maxConcurrentTasks: 10,
      meter,
    });

    // Before starting, all slots should be available
    expect(worker.availableTaskSlots).toBe(10);

    const allMetrics = await collectMetrics(metricReader);
    const slotsValue = getMetricValue(allMetrics, "silo.worker.available_task_slots");
    expect(slotsValue).toBe(10);
  });

  it("available task slots decreases when tasks are running", async () => {
    let resolveTask: (() => void) | undefined;
    const taskStarted = new Promise<void>((resolve) => {
      resolveTask = resolve;
    });
    let finishTask: (() => void) | undefined;
    const taskBlocked = new Promise<void>((resolve) => {
      finishTask = resolve;
    });

    let pollCount = 0;
    const leaseTasks = vi.fn().mockImplementation(async () => {
      pollCount++;
      if (pollCount === 1) {
        return tasksResult([createTask("task-1", "job-1")]);
      }
      return tasksResult([]);
    });
    const client = createMockClient({ leaseTasks });
    const handler: TaskHandler = async () => {
      resolveTask!();
      await taskBlocked;
      return { type: "success" as const, result: {} };
    };
    const meter = meterProvider.getMeter("test");

    const worker = new SiloWorker({
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
    expect(worker.availableTaskSlots).toBe(4);

    const allMetrics = await collectMetrics(metricReader);
    const slotsValue = getMetricValue(allMetrics, "silo.worker.available_task_slots");
    expect(slotsValue).toBe(4);

    finishTask!();
    await worker.stop();
  });

  it("uses global meter provider when no meter is specified", () => {
    const client = createMockClient();
    const handler: TaskHandler = async () => ({ type: "success", result: {} });
    // No meter option — should not throw
    const worker = new SiloWorker({
      client,
      workerId: "test-worker",
      taskGroup: "default",
      handler,
    });
    expect(worker.workerMetrics).toBeDefined();
  });
});
