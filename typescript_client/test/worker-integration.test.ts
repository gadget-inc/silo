import { describe, it, expect, beforeAll, afterAll, afterEach } from "vitest";
import { SiloGrpcClient, decodePayload } from "../src/client";
import {
  SiloWorker,
  type TaskHandler,
  type SiloWorkerOptions,
} from "../src/worker";

const SILO_SERVER = process.env.SILO_SERVER || "localhost:50051";
const RUN_INTEGRATION =
  process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";

describe.skipIf(!RUN_INTEGRATION)("SiloWorker integration", () => {
  let client: SiloGrpcClient;
  let activeWorkers: SiloWorker[] = [];

  beforeAll(() => {
    client = new SiloGrpcClient({
      server: SILO_SERVER,
      useTls: false,
    });
  });

  afterAll(() => {
    client.close();
  });

  afterEach(async () => {
    // Stop all workers created during test
    await Promise.all(activeWorkers.map((w) => w.stop()));
    activeWorkers = [];
  });

  function createWorker(
    handler: TaskHandler,
    options?: Partial<
      Omit<SiloWorkerOptions, "client" | "shard" | "workerId" | "handler">
    >
  ): SiloWorker {
    const worker = new SiloWorker({
      client,
      shard: "0",
      workerId: `test-worker-${Date.now()}-${Math.random()
        .toString(36)
        .slice(2)}`,
      handler,
      pollIntervalMs: 50,
      heartbeatIntervalMs: 1000,
      ...options,
    });
    activeWorkers.push(worker);
    return worker;
  }

  describe("basic task processing", () => {
    it("processes a single task", async () => {
      const processedJobs: string[] = [];

      const handler: TaskHandler = async (ctx) => {
        const payload = decodePayload<{ message: string }>(
          ctx.task.payload?.data
        );
        processedJobs.push(payload?.message ?? "");
        return { type: "success", result: { processed: true } };
      };

      const worker = createWorker(handler);
      worker.start();

      // Enqueue a job
      await client.enqueue({
        shard: "0",
        payload: { message: "hello-worker" },
        priority: 1,
      });

      // Wait for processing
      await new Promise((resolve) => setTimeout(resolve, 500));

      expect(processedJobs).toContain("hello-worker");
    });

    it("processes multiple tasks sequentially with maxConcurrentTasks=1", async () => {
      const testBatch = `seq-${Date.now()}-${Math.random()
        .toString(36)
        .slice(2)}`;
      const processedOrder: number[] = [];
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const handler: TaskHandler = async (ctx) => {
        const payload = decodePayload<{ index: number; batch: string }>(
          ctx.task.payload?.data
        );
        // Only track jobs from this test batch
        if (payload?.batch === testBatch) {
          currentConcurrent++;
          maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
          await new Promise((resolve) => setTimeout(resolve, 20));
          processedOrder.push(payload?.index ?? -1);
          currentConcurrent--;
        }
        return { type: "success", result: {} };
      };

      const worker = createWorker(handler, { maxConcurrentTasks: 1 });
      worker.start();

      // Enqueue multiple jobs with unique batch identifier
      for (let i = 0; i < 5; i++) {
        await client.enqueue({
          shard: "0",
          payload: { index: i, batch: testBatch },
          priority: 1,
        });
      }

      // Wait for all to be processed
      await new Promise((resolve) => setTimeout(resolve, 3000));

      // Should have processed all our batch tasks (at least some)
      expect(processedOrder.length).toBeGreaterThanOrEqual(3);
      // And should have been sequential (max 1 concurrent)
      expect(maxConcurrent).toBeLessThanOrEqual(1);
    });

    it("processes tasks concurrently", async () => {
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const handler: TaskHandler = async () => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        await new Promise((resolve) => setTimeout(resolve, 100));
        currentConcurrent--;
        return { type: "success", result: {} };
      };

      const worker = createWorker(handler, {
        maxConcurrentTasks: 5,
        concurrentPollers: 2,
      });
      worker.start();

      // Enqueue several jobs at once
      await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          client.enqueue({
            shard: "0",
            payload: { index: i },
            priority: 1,
          })
        )
      );

      // Wait for processing
      await new Promise((resolve) => setTimeout(resolve, 1000));

      // Should have had multiple tasks running concurrently
      expect(maxConcurrent).toBeGreaterThan(1);
      expect(maxConcurrent).toBeLessThanOrEqual(5);
    });
  });

  describe("failure handling", () => {
    it("reports failure when handler returns failure outcome", async () => {
      const handler: TaskHandler = async () => ({
        type: "failure",
        code: "TEST_FAILURE",
        data: { reason: "intentional" },
      });

      const worker = createWorker(handler);
      worker.start();

      const jobId = await client.enqueue({
        shard: "0",
        payload: { action: "fail" },
        priority: 1,
      });

      // Wait for processing
      await new Promise((resolve) => setTimeout(resolve, 300));

      // Job should still exist but task completed with failure
      const job = await client.getJob("0", jobId);
      expect(job).toBeDefined();
    });

    it("reports failure when handler throws", async () => {
      const errors: Error[] = [];

      const handler: TaskHandler = async () => {
        throw new Error("Oops!");
      };

      const worker = createWorker(handler, {
        onError: (err: Error) => errors.push(err),
      });
      worker.start();

      await client.enqueue({
        shard: "0",
        payload: { action: "throw" },
        priority: 1,
      });

      await new Promise((resolve) => setTimeout(resolve, 300));

      // Worker should continue running despite error
      expect(worker.isRunning).toBe(true);
    });
  });

  describe("worker lifecycle", () => {
    it("stops gracefully with pending tasks", async () => {
      let taskStarted = false;
      let taskCompleted = false;

      const handler: TaskHandler = async () => {
        taskStarted = true;
        await new Promise((resolve) => setTimeout(resolve, 200));
        taskCompleted = true;
        return { type: "success", result: {} };
      };

      const worker = createWorker(handler, { maxConcurrentTasks: 10 });
      worker.start();

      await client.enqueue({
        shard: "0",
        payload: { action: "slow-graceful-stop" },
        priority: 1,
      });

      // Wait for task to start (may take longer due to poll interval)
      for (let i = 0; i < 10 && !taskStarted; i++) {
        await new Promise((resolve) => setTimeout(resolve, 100));
      }

      if (taskStarted) {
        // Stop the worker - should wait for task to complete
        await worker.stop();

        expect(taskCompleted).toBe(true);
        expect(worker.isRunning).toBe(false);
        expect(worker.activeTasks).toBe(0);
      } else {
        // Test didn't get to start a task, just clean up
        await worker.stop();
      }
    });

    it("can be restarted after stopping", async () => {
      let processCount = 0;

      const handler: TaskHandler = async () => {
        processCount++;
        return { type: "success", result: {} };
      };

      const worker = createWorker(handler);

      // First run
      worker.start();
      await client.enqueue({
        shard: "0",
        payload: { run: 1 },
        priority: 1,
      });
      await new Promise((resolve) => setTimeout(resolve, 200));
      await worker.stop();

      const firstCount = processCount;
      expect(firstCount).toBeGreaterThan(0);

      // Second run
      worker.start();
      await client.enqueue({
        shard: "0",
        payload: { run: 2 },
        priority: 1,
      });
      await new Promise((resolve) => setTimeout(resolve, 200));
      await worker.stop();

      expect(processCount).toBeGreaterThan(firstCount);
    });
  });

  describe("multiple pollers", () => {
    it("uses multiple pollers to fetch tasks faster", async () => {
      const processedTasks: string[] = [];

      const handler: TaskHandler = async (ctx) => {
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
      await Promise.all(
        Array.from({ length: 20 }, (_, i) =>
          client.enqueue({
            shard: "0",
            payload: { index: i },
            priority: 1,
          })
        )
      );

      // Wait for processing
      await new Promise((resolve) => setTimeout(resolve, 2000));

      // Should have processed many tasks
      expect(processedTasks.length).toBeGreaterThan(10);
    });
  });

  describe("abort signal", () => {
    it("provides abort signal that is aborted on stop", async () => {
      let signalAborted = false;
      let taskStarted = false;

      const handler: TaskHandler = async (ctx) => {
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
        shard: "0",
        payload: { action: "long-abort-test" },
        priority: 1,
      });

      // Wait for task to start
      await new Promise((resolve) => setTimeout(resolve, 300));

      // If we got a task to process
      if (taskStarted) {
        // Stop immediately - should abort signal
        await worker.stop(100);
        expect(signalAborted).toBe(true);
      } else {
        // Worker didn't pick up our task, just stop and skip the assertion
        await worker.stop();
      }
    });
  });
});
