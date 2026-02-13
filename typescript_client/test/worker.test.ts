import { describe, it, expect, vi } from "vitest";
import { SiloWorker, type TaskHandler } from "../src/worker";
import type { SiloGRPCClient, LeaseTasksResult } from "../src/client";
import { encodeBytes } from "../src/client";
import type { Task } from "../src/pb/silo";

// Mock client for unit tests
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

// Helper to wrap tasks array in LeaseTasksResult
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

describe("SiloWorker", () => {
  describe("constructor", () => {
    it("creates a worker with default options", () => {
      const client = createMockClient();
      const handler: TaskHandler = async () => ({
        type: "success",
        result: {},
      });

      const worker = new SiloWorker({
        client,
        workerId: "test-worker",
        taskGroup: "default",
        handler,
      });

      expect(worker.isRunning).toBe(false);
      expect(worker.activeTasks).toBe(0);
    });

    it("creates a worker with custom options", () => {
      const client = createMockClient();
      const handler: TaskHandler = async () => ({
        type: "success",
        result: {},
      });

      const worker = new SiloWorker({
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

      expect(worker.isRunning).toBe(false);
    });
  });

  describe("start and stop", () => {
    it("starts and stops the worker", async () => {
      const client = createMockClient();
      const handler: TaskHandler = async () => ({
        type: "success",
        result: {},
      });

      const worker = new SiloWorker({
        client,
        workerId: "test-worker",
        taskGroup: "default",
        handler,
        pollIntervalMs: 50,
      });

      expect(worker.isRunning).toBe(false);

      worker.start();
      expect(worker.isRunning).toBe(true);

      // Starting again should be a no-op
      worker.start();
      expect(worker.isRunning).toBe(true);

      await worker.stop();
      expect(worker.isRunning).toBe(false);

      // Stopping again should be a no-op
      await worker.stop();
      expect(worker.isRunning).toBe(false);
    });

    it("polls for tasks when started", async () => {
      const leaseTasks = vi.fn().mockResolvedValue(tasksResult([]));
      const client = createMockClient({ leaseTasks });
      const handler: TaskHandler = async () => ({
        type: "success",
        result: {},
      });

      const worker = new SiloWorker({
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

      expect(leaseTasks).toHaveBeenCalled();
      expect(leaseTasks).toHaveBeenCalledWith(
        {
          workerId: "test-worker",
          maxTasks: expect.any(Number),
          taskGroup: "default",
        },
        expect.any(Number), // serverIndex for per-worker round-robin
      );
    });

    it("uses multiple concurrent pollers", async () => {
      let pollCount = 0;
      const leaseTasks = vi.fn().mockImplementation(async () => {
        pollCount++;
        await new Promise((resolve) => setTimeout(resolve, 20));
        return tasksResult([]);
      });
      const client = createMockClient({ leaseTasks });
      const handler: TaskHandler = async () => ({
        type: "success",
        result: {},
      });

      const worker = new SiloWorker({
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
      expect(countDuringRun).toBeGreaterThanOrEqual(3);
    });
  });

  describe("task execution", () => {
    it("executes tasks and reports success", async () => {
      const task = createTask("task-1", "job-1");
      const leaseTasks = vi
        .fn()
        .mockResolvedValueOnce(tasksResult([task]))
        .mockResolvedValue(tasksResult([]));
      const reportOutcome = vi.fn().mockResolvedValue(undefined);
      const client = createMockClient({ leaseTasks, reportOutcome });

      const handler = vi.fn().mockResolvedValue({
        type: "success",
        result: { processed: true },
      });

      const worker = new SiloWorker({
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

      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({
          task: expect.objectContaining({
            id: task.id,
            jobId: task.jobId,
            payload: { test: "data" }, // Decoded payload
          }),
        }),
      );

      expect(reportOutcome).toHaveBeenCalledWith({
        taskId: "task-1",
        outcome: { type: "success", result: { processed: true } },
        shard: "00000000-0000-0000-0000-000000000001",
      });
    });

    it("passes limits to handler in task context", async () => {
      const taskWithLimits: Task = {
        id: "task-limits-1",
        jobId: "job-limits-1",
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

      const leaseTasks = vi
        .fn()
        .mockResolvedValueOnce(tasksResult([taskWithLimits]))
        .mockResolvedValue(tasksResult([]));
      const reportOutcome = vi.fn().mockResolvedValue(undefined);
      const client = createMockClient({ leaseTasks, reportOutcome });

      let receivedLimits: unknown[] = [];
      const handler: TaskHandler = async (ctx) => {
        receivedLimits = ctx.task.limits;
        return { type: "success", result: {} };
      };

      const worker = new SiloWorker({
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
      expect(receivedLimits).toHaveLength(2);

      // Check concurrency limit
      const concurrencyLimit = receivedLimits[0] as {
        limit: {
          oneofKind: string;
          concurrency: { key: string; maxConcurrency: number };
        };
      };
      expect(concurrencyLimit.limit.oneofKind).toBe("concurrency");
      expect(concurrencyLimit.limit.concurrency.key).toBe("test-concurrency-key");
      expect(concurrencyLimit.limit.concurrency.maxConcurrency).toBe(5);

      // Check rate limit
      const rateLimit = receivedLimits[1] as {
        limit: {
          oneofKind: string;
          rateLimit: { name: string; uniqueKey: string };
        };
      };
      expect(rateLimit.limit.oneofKind).toBe("rateLimit");
      expect(rateLimit.limit.rateLimit.name).toBe("test-rate-limit");
      expect(rateLimit.limit.rateLimit.uniqueKey).toBe("test-rate-key");
    });

    it("executes tasks and reports failure", async () => {
      const task = createTask("task-2", "job-2");
      const leaseTasks = vi
        .fn()
        .mockResolvedValueOnce(tasksResult([task]))
        .mockResolvedValue(tasksResult([]));
      const reportOutcome = vi.fn().mockResolvedValue(undefined);
      const client = createMockClient({ leaseTasks, reportOutcome });

      const handler: TaskHandler = async () => ({
        type: "failure",
        code: "VALIDATION_ERROR",
        data: { field: "email" },
      });

      const worker = new SiloWorker({
        client,
        workerId: "test-worker",
        taskGroup: "default",
        handler,
        pollIntervalMs: 10,
      });

      worker.start();
      await new Promise((resolve) => setTimeout(resolve, 100));
      await worker.stop();

      expect(reportOutcome).toHaveBeenCalledWith({
        taskId: "task-2",
        outcome: {
          type: "failure",
          code: "VALIDATION_ERROR",
          data: { field: "email" },
        },
        shard: "00000000-0000-0000-0000-000000000001",
      });
    });

    it("reports failure when handler throws", async () => {
      const task = createTask("task-3", "job-3");
      const leaseTasks = vi
        .fn()
        .mockResolvedValueOnce(tasksResult([task]))
        .mockResolvedValue(tasksResult([]));
      const reportOutcome = vi.fn().mockResolvedValue(undefined);
      const client = createMockClient({ leaseTasks, reportOutcome });

      const handler: TaskHandler = async () => {
        throw new Error("Something went wrong");
      };

      const worker = new SiloWorker({
        client,
        workerId: "test-worker",
        taskGroup: "default",
        handler,
        pollIntervalMs: 10,
        onError: () => {}, // Suppress error logging
      });

      worker.start();
      await new Promise((resolve) => setTimeout(resolve, 100));
      await worker.stop();

      expect(reportOutcome).toHaveBeenCalledWith({
        taskId: "task-3",
        outcome: {
          type: "failure",
          code: "HANDLER_ERROR",
          data: expect.objectContaining({
            message: "Something went wrong",
          }),
        },
        shard: "00000000-0000-0000-0000-000000000001",
      });
    });

    it("respects maxConcurrentTasks limit", async () => {
      // Create tasks that will be returned in batches
      const batch1 = [createTask("task-a", "job-a"), createTask("task-b", "job-b")];
      const batch2 = [createTask("task-c", "job-c"), createTask("task-d", "job-d")];
      const batch3 = [createTask("task-e", "job-e")];

      let activeTasks = 0;
      let maxActiveTasks = 0;

      const leaseTasks = vi
        .fn()
        .mockResolvedValueOnce(tasksResult(batch1))
        .mockResolvedValueOnce(tasksResult(batch2))
        .mockResolvedValueOnce(tasksResult(batch3))
        .mockResolvedValue(tasksResult([]));
      const reportOutcome = vi.fn().mockResolvedValue(undefined);
      const client = createMockClient({ leaseTasks, reportOutcome });

      const handler: TaskHandler = async () => {
        activeTasks++;
        maxActiveTasks = Math.max(maxActiveTasks, activeTasks);
        await new Promise((resolve) => setTimeout(resolve, 50));
        activeTasks--;
        return { type: "success", result: {} };
      };

      const worker = new SiloWorker({
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
      expect(maxActiveTasks).toBeLessThanOrEqual(2);
      // But should have processed all 5 tasks
      expect(reportOutcome).toHaveBeenCalledTimes(5);
    });

    it("tracks active task count", async () => {
      const task = createTask("task-x", "job-x");
      const leaseTasks = vi
        .fn()
        .mockResolvedValueOnce(tasksResult([task]))
        .mockResolvedValue(tasksResult([]));
      const reportOutcome = vi.fn().mockResolvedValue(undefined);
      const client = createMockClient({ leaseTasks, reportOutcome });

      let activeCountDuringExecution = 0;
      let workerRef: SiloWorker;

      const worker = new SiloWorker({
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

      expect(worker.activeTasks).toBe(0);

      worker.start();
      await new Promise((resolve) => setTimeout(resolve, 30));

      // During execution, active count should be > 0
      expect(activeCountDuringExecution).toBe(1);

      await new Promise((resolve) => setTimeout(resolve, 100));
      await worker.stop();

      // After completion, active count should be 0
      expect(worker.activeTasks).toBe(0);
    });
  });

  describe("heartbeats", () => {
    it("sends heartbeats while task is executing", async () => {
      const task = createTask("task-hb", "job-hb");
      const leaseTasks = vi
        .fn()
        .mockResolvedValueOnce(tasksResult([task]))
        .mockResolvedValue(tasksResult([]));
      const reportOutcome = vi.fn().mockResolvedValue(undefined);
      const heartbeat = vi.fn().mockResolvedValue({ cancelled: false });
      const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });

      const handler: TaskHandler = async () => {
        // Simulate long-running task
        await new Promise((resolve) => setTimeout(resolve, 150));
        return { type: "success", result: {} };
      };

      const worker = new SiloWorker({
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
      // heartbeat(workerId, taskId, shard)
      expect(heartbeat).toHaveBeenCalledWith(
        "test-worker",
        "task-hb",
        "00000000-0000-0000-0000-000000000001",
      );
      expect(heartbeat.mock.calls.length).toBeGreaterThanOrEqual(2);
    });

    it("stops heartbeats after task completes", async () => {
      const task = createTask("task-hb2", "job-hb2");
      const leaseTasks = vi
        .fn()
        .mockResolvedValueOnce(tasksResult([task]))
        .mockResolvedValue(tasksResult([]));
      const reportOutcome = vi.fn().mockResolvedValue(undefined);
      const heartbeat = vi.fn().mockResolvedValue({ cancelled: false });
      const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });

      const handler: TaskHandler = async () => {
        await new Promise((resolve) => setTimeout(resolve, 20));
        return { type: "success", result: {} };
      };

      const worker = new SiloWorker({
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
      expect(heartbeat.mock.calls.length).toBe(heartbeatCountAfterComplete);
    });
  });

  describe("error handling", () => {
    it("calls onError when polling fails", async () => {
      const leaseTasks = vi.fn().mockRejectedValue(new Error("Connection failed"));
      const client = createMockClient({ leaseTasks });
      const onError = vi.fn();

      const handler: TaskHandler = async () => ({
        type: "success",
        result: {},
      });

      const worker = new SiloWorker({
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

      expect(onError).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Connection failed" }),
      );
    });

    it("calls onError when heartbeat fails", async () => {
      const task = createTask("task-err", "job-err");
      const leaseTasks = vi
        .fn()
        .mockResolvedValueOnce(tasksResult([task]))
        .mockResolvedValue(tasksResult([]));
      const reportOutcome = vi.fn().mockResolvedValue(undefined);
      const heartbeat = vi.fn().mockRejectedValue(new Error("Heartbeat failed"));
      const onError = vi.fn();
      const client = createMockClient({ leaseTasks, reportOutcome, heartbeat });

      const handler: TaskHandler = async () => {
        await new Promise((resolve) => setTimeout(resolve, 100));
        return { type: "success", result: {} };
      };

      const worker = new SiloWorker({
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

      expect(onError).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Heartbeat failed" }),
        expect.objectContaining({ taskId: "task-err" }),
      );
    });

    it("continues processing after errors", async () => {
      const task1 = createTask("task-1", "job-1");
      const task2 = createTask("task-2", "job-2");

      let pollCount = 0;
      const leaseTasks = vi.fn().mockImplementation(async () => {
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
      const reportOutcome = vi.fn().mockResolvedValue(undefined);
      const client = createMockClient({ leaseTasks, reportOutcome });

      const handler: TaskHandler = async () => ({
        type: "success",
        result: {},
      });

      const worker = new SiloWorker({
        client,
        workerId: "test-worker",
        taskGroup: "default",
        handler,
        pollIntervalMs: 10,
        onError: () => {}, // Suppress error logging
      });

      worker.start();
      await new Promise((resolve) => setTimeout(resolve, 150));
      await worker.stop();

      // Should have processed tasks despite the error
      expect(reportOutcome).toHaveBeenCalledTimes(2);
    });
  });

  describe("graceful shutdown", () => {
    it("does not abort task signal when shutdown begins", async () => {
      const task = createTask("task-shutdown", "job-shutdown");
      const leaseTasks = vi
        .fn()
        .mockResolvedValueOnce(tasksResult([task]))
        .mockResolvedValue(tasksResult([]));
      const reportOutcome = vi.fn().mockResolvedValue(undefined);
      const client = createMockClient({ leaseTasks, reportOutcome });

      let signalWasAbortedDuringTask = false;
      let taskStartedResolve: () => void;
      let continueTaskResolve: () => void;
      const taskStarted = new Promise<void>((r) => {
        taskStartedResolve = r;
      });
      const continueTask = new Promise<void>((r) => {
        continueTaskResolve = r;
      });

      const handler: TaskHandler = async (ctx) => {
        // Signal task has started
        taskStartedResolve();
        // Wait until we're told to continue (after stop() is called)
        await continueTask;
        // Check if signal was aborted after shutdown was called
        signalWasAbortedDuringTask = ctx.cancellationSignal.aborted;
        return { type: "success", result: { completed: true } };
      };

      const worker = new SiloWorker({
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
      continueTaskResolve!();

      await stopPromise;

      // The signal should NOT have been aborted
      expect(signalWasAbortedDuringTask).toBe(false);

      // The task should have completed successfully
      expect(reportOutcome).toHaveBeenCalledWith({
        taskId: "task-shutdown",
        outcome: { type: "success", result: { completed: true } },
        shard: "00000000-0000-0000-0000-000000000001",
      });
    });

    it("allows tasks to complete after shutdown begins", async () => {
      const task = createTask("task-complete", "job-complete");
      const leaseTasks = vi
        .fn()
        .mockResolvedValueOnce(tasksResult([task]))
        .mockResolvedValue(tasksResult([]));
      const reportOutcome = vi.fn().mockResolvedValue(undefined);
      const client = createMockClient({ leaseTasks, reportOutcome });

      let taskCompleted = false;
      let resolveTaskStarted: () => void;
      const taskStarted = new Promise<void>((resolve) => {
        resolveTaskStarted = resolve;
      });

      const handler: TaskHandler = async () => {
        resolveTaskStarted();
        // Simulate some work that takes time
        await new Promise((resolve) => setTimeout(resolve, 100));
        taskCompleted = true;
        return { type: "success", result: { done: true } };
      };

      const worker = new SiloWorker({
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
      expect(taskCompleted).toBe(true);
      expect(reportOutcome).toHaveBeenCalledWith({
        taskId: "task-complete",
        outcome: { type: "success", result: { done: true } },
        shard: "00000000-0000-0000-0000-000000000001",
      });
    });

    it("does not report cancelled outcome for tasks running during shutdown", async () => {
      const task = createTask("task-not-cancelled", "job-not-cancelled");
      const leaseTasks = vi
        .fn()
        .mockResolvedValueOnce(tasksResult([task]))
        .mockResolvedValue(tasksResult([]));
      const reportOutcome = vi.fn().mockResolvedValue(undefined);
      const client = createMockClient({ leaseTasks, reportOutcome });

      let resolveTaskStarted: () => void;
      const taskStarted = new Promise<void>((resolve) => {
        resolveTaskStarted = resolve;
      });

      const handler: TaskHandler = async () => {
        resolveTaskStarted();
        await new Promise((resolve) => setTimeout(resolve, 50));
        return { type: "success", result: {} };
      };

      const worker = new SiloWorker({
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
      expect(reportOutcome).toHaveBeenCalledTimes(1);
      expect(reportOutcome).toHaveBeenCalledWith({
        taskId: "task-not-cancelled",
        outcome: { type: "success", result: {} },
        shard: "00000000-0000-0000-0000-000000000001",
      });
      // Verify it was NOT called with cancelled
      expect(reportOutcome).not.toHaveBeenCalledWith(
        expect.objectContaining({
          outcome: { type: "cancelled" },
        }),
      );
    });
  });

  describe("TaskContext", () => {
    it("provides abort signal and cancel method in context", async () => {
      const task = createTask("task-sig", "job-sig");
      const leaseTasks = vi
        .fn()
        .mockResolvedValueOnce(tasksResult([task]))
        .mockResolvedValue(tasksResult([]));
      const reportOutcome = vi.fn().mockResolvedValue(undefined);
      const client = createMockClient({ leaseTasks, reportOutcome });

      let receivedSignal: AbortSignal | undefined;
      let receivedCancel: (() => Promise<void>) | undefined;

      const handler: TaskHandler = async (ctx) => {
        receivedSignal = ctx.cancellationSignal;
        receivedCancel = ctx.cancel.bind(ctx);
        return { type: "success", result: {} };
      };

      const worker = new SiloWorker({
        client,
        workerId: "test-worker",
        taskGroup: "default",
        handler,
        pollIntervalMs: 10,
      });

      worker.start();
      await new Promise((resolve) => setTimeout(resolve, 50));
      await worker.stop();

      expect(receivedSignal).toBeDefined();
      expect(receivedSignal).toBeInstanceOf(AbortSignal);
      expect(receivedCancel).toBeDefined();
      expect(typeof receivedCancel).toBe("function");
    });
  });
});
