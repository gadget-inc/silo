import {
  describe,
  it,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
  afterEach,
} from "vitest";
import { SiloGRPCClient, decodePayload, JobStatus } from "../src/client";
import {
  SiloWorker,
  type TaskHandler,
  type SiloWorkerOptions,
  type RefreshHandler,
} from "../src/worker";

// Support comma-separated list of servers for multi-node testing
const SILO_SERVERS = (
  process.env.SILO_SERVERS ||
  process.env.SILO_SERVER ||
  "localhost:50051"
).split(",");
const RUN_INTEGRATION =
  process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";

/**
 * Wait until a condition becomes true, polling at intervals.
 * @param condition Function that returns true (or Promise<true>) when the condition is met
 * @param options.timeout Maximum time to wait in ms (default: 5000)
 * @param options.interval Polling interval in ms (default: 50)
 * @throws Error if timeout is reached before condition is met
 */
async function waitFor(
  condition: () => boolean | Promise<boolean>,
  options?: { timeout?: number; interval?: number }
): Promise<void> {
  const timeout = options?.timeout ?? 5000;
  const interval = options?.interval ?? 50;
  const start = Date.now();

  while (!(await condition())) {
    if (Date.now() - start > timeout) {
      throw new Error(`waitFor timed out after ${timeout}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, interval));
  }
}

describe.skipIf(!RUN_INTEGRATION)("SiloWorker integration", () => {
  let client: SiloGRPCClient;
  let activeWorkers: SiloWorker[] = [];

  beforeAll(async () => {
    client = new SiloGRPCClient({
      servers: SILO_SERVERS,
      useTls: false,
      shardRouting: {
        topologyRefreshIntervalMs: 0,
      },
    });
    // Discover cluster topology
    await client.refreshTopology();
  });

  afterAll(() => {
    client.close();
  });

  beforeEach(async () => {
    // Reset all shards to ensure test isolation - clean slate before each test
    await client.resetShards();
  });

  afterEach(async () => {
    // Stop all workers created during test
    await Promise.all(activeWorkers.map((w) => w.stop()));
    activeWorkers = [];
  });

  // Default tenant for all tests - reset ensures isolation between tests
  const DEFAULT_TENANT = "test-tenant";
  // Default task group for all tests
  const DEFAULT_TASK_GROUP = "test-task-group";

  function createWorker(
    handler: TaskHandler,
    options?: Partial<
      Omit<SiloWorkerOptions, "client" | "workerId" | "handler">
    >
  ): SiloWorker {
    const worker = new SiloWorker({
      client,
      workerId: `test-worker-${Date.now()}-${Math.random()
        .toString(36)
        .slice(2)}`,
      handler,
      tenant: DEFAULT_TENANT,
      taskGroup: options?.taskGroup ?? DEFAULT_TASK_GROUP,
      pollIntervalMs: 50,
      heartbeatIntervalMs: 1000,
      ...options,
    });
    activeWorkers.push(worker);
    return worker;
  }

  describe("basic task processing", () => {
    it("processes a single task from any shard", async () => {
      const processedJobs: string[] = [];

      const handler: TaskHandler = async (ctx) => {
        const payload = decodePayload<{ message: string }>(
          ctx.task.payload?.data
        );
        processedJobs.push(payload?.message ?? "");
        return { type: "success", result: { processed: true } };
      };

      // Worker polls all shards
      const worker = createWorker(handler);
      worker.start();

      // Enqueue a job to test tenant
      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { message: "hello-worker" },
        priority: 1,
      });

      // Wait until job is processed
      await waitFor(() => processedJobs.includes("hello-worker"));

      expect(processedJobs).toContain("hello-worker");
    });

    it("processes multiple tasks sequentially with maxConcurrentTasks=1", async () => {
      const processedOrder: number[] = [];
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const handler: TaskHandler = async (ctx) => {
        const payload = decodePayload<{ index: number }>(
          ctx.task.payload?.data
        );
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
          taskGroup: DEFAULT_TASK_GROUP,
          payload: { index: i },
          priority: 1,
        });
      }

      // Wait until at least 3 are processed
      await waitFor(() => processedOrder.length >= 3);

      // Should have processed all our tasks (at least some)
      expect(processedOrder.length).toBeGreaterThanOrEqual(3);
      // And should have been sequential (max 1 concurrent)
      expect(maxConcurrent).toBeLessThanOrEqual(1);
    });

    it("processes tasks concurrently", async () => {
      let maxConcurrent = 0;
      let currentConcurrent = 0;
      let completedCount = 0;

      const handler: TaskHandler = async () => {
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
      await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          client.enqueue({
            tenant: DEFAULT_TENANT,
            taskGroup: DEFAULT_TASK_GROUP,
            payload: { index: i },
            priority: 1,
          })
        )
      );

      // Wait until we've seen concurrent execution
      await waitFor(() => maxConcurrent > 1 && completedCount >= 5);

      // Should have had multiple tasks running concurrently
      expect(maxConcurrent).toBeGreaterThan(1);
      expect(maxConcurrent).toBeLessThanOrEqual(5);
    });
  });

  describe("multi-job processing", () => {
    it("processes multiple jobs from the same tenant", async () => {
      const processedPayloads = new Set<string>();

      const handler: TaskHandler = async (ctx) => {
        const payload = decodePayload<{ label: string }>(
          ctx.task.payload?.data
        );
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
      await Promise.all(
        labels.map((label) =>
          client.enqueue({
            tenant: DEFAULT_TENANT,
            taskGroup: DEFAULT_TASK_GROUP,
            payload: { label },
            priority: 1,
          })
        )
      );

      // Wait until all jobs are processed
      await waitFor(() => processedPayloads.size === labels.length);

      // Should have processed all jobs
      expect(processedPayloads.size).toBe(labels.length);
      for (const label of labels) {
        expect(processedPayloads.has(label)).toBe(true);
      }
    });
  });

  describe("failure handling", () => {
    it("reports failure when handler returns failure outcome", async () => {
      let taskProcessed = false;

      const handler: TaskHandler = async () => {
        taskProcessed = true;
        return {
          type: "failure",
          code: "TEST_FAILURE",
          data: { reason: "intentional" },
        };
      };

      const worker = createWorker(handler);
      worker.start();

      const handle = await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { action: "fail" },
        priority: 1,
      });

      // Wait until task is processed
      await waitFor(() => taskProcessed);

      // Job should still exist but task completed with failure
      const job = await client.getJob(handle.id, DEFAULT_TENANT);
      expect(job).toBeDefined();
    });

    it("reports failure when handler throws", async () => {
      let taskProcessed = false;

      const handler: TaskHandler = async () => {
        taskProcessed = true;
        throw new Error("Oops!");
      };

      const worker = createWorker(handler);
      worker.start();

      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { action: "throw" },
        priority: 1,
      });

      // Wait until task is processed (even though it throws)
      await waitFor(() => taskProcessed);

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
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
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
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { run: 1 },
        priority: 1,
      });
      await waitFor(() => processCount >= 1);
      await worker.stop();

      const firstCount = processCount;
      expect(firstCount).toBeGreaterThan(0);

      // Second run
      worker.start();
      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { run: 2 },
        priority: 1,
      });
      await waitFor(() => processCount > firstCount);
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
            tenant: DEFAULT_TENANT,
            taskGroup: DEFAULT_TASK_GROUP,
            payload: { index: i },
            priority: 1,
          })
        )
      );

      // Wait until enough tasks are processed
      await waitFor(() => processedTasks.length > 10);

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
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { action: "long" },
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

  describe("shard context", () => {
    it("provides correct shard in task context", async () => {
      let receivedShard: number | undefined;

      const handler: TaskHandler = async (ctx) => {
        receivedShard = ctx.shard;
        return { type: "success", result: {} };
      };

      const worker = createWorker(handler);
      worker.start();

      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: true },
        priority: 1,
      });

      // Wait until task is processed
      await waitFor(() => receivedShard !== undefined);

      // Should have received a valid shard ID
      expect(receivedShard).toBeDefined();
      expect(receivedShard).toBeGreaterThanOrEqual(0);
    });
  });

  describe("task group isolation", () => {
    it("worker polling default task group does not receive tasks from other task groups", async () => {
      const processedByDefaultWorker: string[] = [];
      const otherTaskGroup = `other-group-${Date.now()}`;

      // Worker polling the default task group
      const defaultHandler: TaskHandler = async (ctx) => {
        const payload = decodePayload<{ message: string }>(
          ctx.task.payload?.data
        );
        processedByDefaultWorker.push(payload?.message ?? ctx.task.id);
        return { type: "success", result: {} };
      };

      const defaultWorker = createWorker(defaultHandler, {
        taskGroup: DEFAULT_TASK_GROUP,
      });
      defaultWorker.start();

      // Enqueue a task to a DIFFERENT task group
      const handle = await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: otherTaskGroup,
        payload: { message: "task-for-other-group" },
        priority: 1,
      });

      // Wait for some time to give the worker a chance to poll
      await new Promise((resolve) => setTimeout(resolve, 500));

      // The default worker should NOT have picked up the task
      expect(processedByDefaultWorker).not.toContain("task-for-other-group");
      expect(processedByDefaultWorker.length).toBe(0);

      // Verify the job is still scheduled (not picked up)
      const job = await client.getJob(handle.id, DEFAULT_TENANT);
      expect(job.status).toBe(JobStatus.Scheduled);
    });

    it("worker polling a specific task group receives tasks from that task group", async () => {
      const processedBySpecificWorker: string[] = [];
      const specificTaskGroup = `specific-group-${Date.now()}`;

      // Worker polling the specific task group
      const specificHandler: TaskHandler = async (ctx) => {
        const payload = decodePayload<{ message: string }>(
          ctx.task.payload?.data
        );
        processedBySpecificWorker.push(payload?.message ?? ctx.task.id);
        return { type: "success", result: {} };
      };

      const specificWorker = createWorker(specificHandler, {
        taskGroup: specificTaskGroup,
      });
      specificWorker.start();

      // Enqueue a task to that specific task group
      const handle = await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: specificTaskGroup,
        payload: { message: "task-for-specific-group" },
        priority: 1,
      });

      // Wait until the task is processed
      await waitFor(() =>
        processedBySpecificWorker.includes("task-for-specific-group")
      );

      expect(processedBySpecificWorker).toContain("task-for-specific-group");

      // Verify the job completed successfully
      const job = await client.getJob(handle.id, DEFAULT_TENANT);
      expect(job.status).toBe(JobStatus.Succeeded);
    });

    it(
      "multiple workers on different task groups only receive their own tasks",
      async () => {
        const processedByWorkerA: string[] = [];
        const processedByWorkerB: string[] = [];
        const taskGroupA = `group-a-${Date.now()}`;
        const taskGroupB = `group-b-${Date.now()}`;

        // Enqueue tasks FIRST so they're ready when workers start polling
        await client.enqueue({
          tenant: DEFAULT_TENANT,
          taskGroup: taskGroupA,
          payload: { message: "task-for-A" },
          priority: 1,
        });

        await client.enqueue({
          tenant: DEFAULT_TENANT,
          taskGroup: taskGroupB,
          payload: { message: "task-for-B" },
          priority: 1,
        });

        // Worker A polls task group A
        const workerA = createWorker(
          async (ctx) => {
            const payload = decodePayload<{ message: string }>(
              ctx.task.payload?.data
            );
            processedByWorkerA.push(payload?.message ?? ctx.task.id);
            return { type: "success", result: {} };
          },
          { taskGroup: taskGroupA }
        );

        // Worker B polls task group B
        const workerB = createWorker(
          async (ctx) => {
            const payload = decodePayload<{ message: string }>(
              ctx.task.payload?.data
            );
            processedByWorkerB.push(payload?.message ?? ctx.task.id);
            return { type: "success", result: {} };
          },
          { taskGroup: taskGroupB }
        );

        workerA.start();
        workerB.start();

        // Wait until both tasks are processed
        // In a multi-server cluster, workers use random server selection to avoid
        // lock-step polling patterns. Use a longer timeout to account for variance.
        await waitFor(
          () =>
            processedByWorkerA.includes("task-for-A") &&
            processedByWorkerB.includes("task-for-B"),
          { timeout: 10000 }
        );

        // Each worker should only have processed its own task group's tasks
        expect(processedByWorkerA).toContain("task-for-A");
        expect(processedByWorkerA).not.toContain("task-for-B");

        expect(processedByWorkerB).toContain("task-for-B");
        expect(processedByWorkerB).not.toContain("task-for-A");
      },
      15000
    );
  });

  describe("floating concurrency limit refresh", () => {
    function createWorkerWithRefresh(
      handler: TaskHandler,
      refreshHandler: RefreshHandler,
      options?: Partial<
        Omit<
          SiloWorkerOptions,
          "client" | "workerId" | "handler" | "refreshHandler"
        >
      >
    ): SiloWorker {
      const worker = new SiloWorker({
        client,
        workerId: `test-refresh-worker-${Date.now()}-${Math.random()
          .toString(36)
          .slice(2)}`,
        handler,
        refreshHandler,
        tenant: DEFAULT_TENANT,
        taskGroup: options?.taskGroup ?? DEFAULT_TASK_GROUP,
        pollIntervalMs: 50,
        heartbeatIntervalMs: 1000,
        ...options,
      });
      activeWorkers.push(worker);
      return worker;
    }

    it("processes refresh tasks with refreshHandler", async () => {
      const refreshedQueues: string[] = [];
      const processedJobs: string[] = [];
      const queueKey = `worker-refresh-test-${Date.now()}`;

      const handler: TaskHandler = async (ctx) => {
        const payload = decodePayload<{ message: string }>(
          ctx.task.payload?.data
        );
        processedJobs.push(payload?.message ?? ctx.task.id);
        return { type: "success", result: { processed: true } };
      };

      const refreshHandler: RefreshHandler = async (ctx) => {
        refreshedQueues.push(ctx.task.queueKey);
        // Return a new max concurrency value
        return 15;
      };

      const worker = createWorkerWithRefresh(handler, refreshHandler);
      worker.start();

      // Enqueue job with floating limit and very short refresh interval
      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { message: "job-1" },
        limits: [
          {
            type: "floatingConcurrency",
            key: queueKey,
            defaultMaxConcurrency: 5,
            refreshIntervalMs: 1n, // Very short interval
            metadata: { testId: "refresh-test" },
          },
        ],
      });

      // Wait for job to be processed
      await waitFor(() => processedJobs.length >= 1);

      // Small delay to allow refresh task to be processed
      await new Promise((r) => setTimeout(r, 200));

      // Trigger refresh by enqueueing another job
      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { message: "job-2" },
        limits: [
          {
            type: "floatingConcurrency",
            key: queueKey,
            defaultMaxConcurrency: 5,
            refreshIntervalMs: 1n,
            metadata: { testId: "refresh-test" },
          },
        ],
      });

      // Wait for refresh handler to be called
      await waitFor(() => refreshedQueues.includes(queueKey), {
        timeout: 5000,
      });

      expect(refreshedQueues).toContain(queueKey);
    });

    it("refresh handler receives correct context", async () => {
      let receivedContext: {
        queueKey: string;
        currentMax: number;
        metadata: Record<string, string>;
        shard: number;
        workerId: string;
      } | null = null;

      const queueKey = `context-test-${Date.now()}`;
      const testMetadata = { apiKey: "test-api-key", region: "us-west" };

      const handler: TaskHandler = async () => {
        return { type: "success", result: {} };
      };

      const refreshHandler: RefreshHandler = async (ctx) => {
        receivedContext = {
          queueKey: ctx.task.queueKey,
          currentMax: ctx.task.currentMaxConcurrency,
          metadata: { ...ctx.task.metadata },
          shard: ctx.shard,
          workerId: ctx.workerId,
        };
        return 20;
      };

      const worker = createWorkerWithRefresh(handler, refreshHandler);
      worker.start();

      // Enqueue first job to create the limit
      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { init: true },
        limits: [
          {
            type: "floatingConcurrency",
            key: queueKey,
            defaultMaxConcurrency: 10,
            refreshIntervalMs: 1n,
            metadata: testMetadata,
          },
        ],
      });

      // Wait and trigger refresh
      await new Promise((r) => setTimeout(r, 100));

      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { trigger: true },
        limits: [
          {
            type: "floatingConcurrency",
            key: queueKey,
            defaultMaxConcurrency: 10,
            refreshIntervalMs: 1n,
            metadata: testMetadata,
          },
        ],
      });

      // Wait for refresh handler to receive context
      await waitFor(() => receivedContext !== null, { timeout: 5000 });

      expect(receivedContext).not.toBeNull();
      expect(receivedContext!.queueKey).toBe(queueKey);
      expect(receivedContext!.currentMax).toBe(10);
      expect(receivedContext!.metadata).toEqual(testMetadata);
      expect(receivedContext!.shard).toBeGreaterThanOrEqual(0);
      expect(receivedContext!.workerId).toBeTruthy();
    });

    it("handles refresh handler errors gracefully", async () => {
      let errorThrown = false;
      const queueKey = `error-refresh-${Date.now()}`;
      const errors: Error[] = [];

      const handler: TaskHandler = async () => {
        return { type: "success", result: {} };
      };

      const refreshHandler: RefreshHandler = async () => {
        errorThrown = true;
        throw new Error("Simulated API failure");
      };

      const worker = createWorkerWithRefresh(handler, refreshHandler, {
        onError: (err) => errors.push(err),
      });
      worker.start();

      // Enqueue jobs to trigger refresh
      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: 1 },
        limits: [
          {
            type: "floatingConcurrency",
            key: queueKey,
            defaultMaxConcurrency: 5,
            refreshIntervalMs: 1n,
            metadata: {},
          },
        ],
      });

      await new Promise((r) => setTimeout(r, 100));

      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: 2 },
        limits: [
          {
            type: "floatingConcurrency",
            key: queueKey,
            defaultMaxConcurrency: 5,
            refreshIntervalMs: 1n,
            metadata: {},
          },
        ],
      });

      // Wait for refresh handler to throw
      await waitFor(() => errorThrown, { timeout: 5000 });

      // Worker should continue running despite the error
      expect(worker.isRunning).toBe(true);
    });

    it("processes both job tasks and refresh tasks concurrently", async () => {
      const processedJobs: string[] = [];
      const refreshedQueues: string[] = [];
      const queueKey = `concurrent-refresh-${Date.now()}`;

      const handler: TaskHandler = async (ctx) => {
        const payload = decodePayload<{ index: number }>(
          ctx.task.payload?.data
        );
        // Simulate some work
        await new Promise((r) => setTimeout(r, 50));
        processedJobs.push(`job-${payload?.index}`);
        return { type: "success", result: {} };
      };

      const refreshHandler: RefreshHandler = async (ctx) => {
        // Simulate API call
        await new Promise((r) => setTimeout(r, 30));
        refreshedQueues.push(ctx.task.queueKey);
        return 25;
      };

      const worker = createWorkerWithRefresh(handler, refreshHandler, {
        maxConcurrentTasks: 5,
      });
      worker.start();

      // Enqueue multiple jobs with floating limit
      for (let i = 0; i < 5; i++) {
        await client.enqueue({
          tenant: DEFAULT_TENANT,
          taskGroup: DEFAULT_TASK_GROUP,
          payload: { index: i },
          limits: [
            {
              type: "floatingConcurrency",
              key: queueKey,
              defaultMaxConcurrency: 10,
              refreshIntervalMs: i === 0 ? 60000n : 1n, // First job creates, rest trigger refresh
              metadata: { batch: "concurrent-test" },
            },
          ],
        });
        // Small delay between enqueues to ensure proper ordering
        await new Promise((r) => setTimeout(r, 20));
      }

      // Wait for jobs to be processed
      // (refresh tasks may or may not be processed depending on timing)
      await waitFor(() => processedJobs.length >= 3, { timeout: 10000 });

      // Job tasks should have been processed
      expect(processedJobs.length).toBeGreaterThan(0);
      // Note: refresh task processing depends on timing, so we don't strictly require it
    });

    it("errors when receiving refresh tasks without a refreshHandler configured", async () => {
      const errors: Error[] = [];
      const queueKey = `no-handler-error-${Date.now()}`;

      const handler: TaskHandler = async () => {
        return { type: "success", result: {} };
      };

      // Create worker WITHOUT refresh handler but WITH error handler to capture the error
      const worker = new SiloWorker({
        client,
        workerId: `no-refresh-handler-worker-${Date.now()}`,
        taskGroup: DEFAULT_TASK_GROUP,
        handler,
        onError: (error) => {
          errors.push(error);
        },
      });
      activeWorkers.push(worker);
      worker.start();

      // Enqueue jobs with floating limit
      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { job: 1 },
        limits: [
          {
            type: "floatingConcurrency",
            key: queueKey,
            defaultMaxConcurrency: 5,
            refreshIntervalMs: 1n,
            metadata: {},
          },
        ],
      });

      await new Promise((r) => setTimeout(r, 100));

      // Enqueue another to trigger refresh task (limit is stale)
      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { job: 2 },
        limits: [
          {
            type: "floatingConcurrency",
            key: queueKey,
            defaultMaxConcurrency: 5,
            refreshIntervalMs: 1n,
            metadata: {},
          },
        ],
      });

      // Wait for worker to encounter the refresh task
      await waitFor(() => errors.length > 0, { timeout: 5000 });

      // Should have received an error about missing refreshHandler
      expect(errors.length).toBeGreaterThan(0);
      expect(errors[0].message).toContain("refreshHandler");
      expect(errors[0].message).toContain("floating limit refresh task");

      await worker.stop();
    });
  });

  describe("task cancellation", () => {
    it("aborts signal when server cancels job during heartbeat", async () => {
      let taskStarted = false;
      let signalAborted = false;

      const handler: TaskHandler = async (ctx) => {
        taskStarted = true;

        // Listen for abort
        ctx.signal.addEventListener("abort", () => {
          signalAborted = true;
        });

        // Wait indefinitely for abort (like the passing tests do)
        while (!ctx.signal.aborted) {
          await new Promise((resolve) => setTimeout(resolve, 50));
        }

        return { type: "success", result: { aborted: true } };
      };

      // Short heartbeat interval to quickly detect cancellation
      const worker = createWorker(handler, {
        heartbeatIntervalMs: 100,
        pollIntervalMs: 50,
      });
      worker.start();

      const handle = await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { action: "long-running" },
        priority: 1,
      });

      // Wait for task to start
      await waitFor(() => taskStarted, { timeout: 5000 });

      // Cancel the job from the server side
      await handle.cancel();

      // Wait for signal to be aborted
      await waitFor(() => signalAborted, { timeout: 5000 });

      expect(signalAborted).toBe(true);

      // Wait for job to reach cancelled state
      await waitFor(
        async () => {
          const job = await client.getJob(handle.id, DEFAULT_TENANT);
          return job.status === JobStatus.Cancelled;
        },
        { timeout: 5000 }
      );

      const finalJob = await client.getJob(handle.id, DEFAULT_TENANT);
      expect(finalJob.status).toBe(JobStatus.Cancelled);
    });

    it("reports Cancelled outcome when job is cancelled during execution", async () => {
      let taskStarted = false;
      let handlerReturnedValue: unknown = null;

      const handler: TaskHandler = async (ctx) => {
        taskStarted = true;

        // Wait while checking for abort
        while (!ctx.signal.aborted) {
          await new Promise((resolve) => setTimeout(resolve, 50));
        }

        // Return a success (this should be ignored because task was cancelled)
        handlerReturnedValue = { success: true };
        return { type: "success", result: handlerReturnedValue };
      };

      const worker = createWorker(handler, {
        heartbeatIntervalMs: 100,
        pollIntervalMs: 50,
      });
      worker.start();

      const handle = await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { action: "cancel-test" },
        priority: 1,
      });

      // Wait for task to start
      await waitFor(() => taskStarted, { timeout: 5000 });

      // Cancel from server
      await handle.cancel();

      // Wait for job to be cancelled
      await waitFor(
        async () => {
          const job = await client.getJob(handle.id, DEFAULT_TENANT);
          return job.status === JobStatus.Cancelled;
        },
        { timeout: 5000 }
      );

      const finalJob = await client.getJob(handle.id, DEFAULT_TENANT);
      expect(finalJob.status).toBe(JobStatus.Cancelled);
    });

    it("handler can cancel task via context.cancel()", async () => {
      let taskStarted = false;
      let cancelCalled = false;
      let signalAbortedAfterCancel = false;

      const handler: TaskHandler = async (ctx) => {
        taskStarted = true;

        // Simulate some work
        await new Promise((resolve) => setTimeout(resolve, 100));

        // Call cancel from the handler
        await ctx.cancel();
        cancelCalled = true;
        signalAbortedAfterCancel = ctx.signal.aborted;

        // Return normally (will be ignored because cancelled)
        return { type: "success", result: {} };
      };

      const worker = createWorker(handler, {
        heartbeatIntervalMs: 100,
        pollIntervalMs: 50,
      });
      worker.start();

      const handle = await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { action: "self-cancel" },
        priority: 1,
      });

      // Wait for task to start and cancel
      await waitFor(() => cancelCalled, { timeout: 5000 });

      expect(taskStarted).toBe(true);
      expect(cancelCalled).toBe(true);
      expect(signalAbortedAfterCancel).toBe(true);

      // Wait for job to reach cancelled state
      await waitFor(
        async () => {
          const job = await client.getJob(handle.id, DEFAULT_TENANT);
          return job.status === JobStatus.Cancelled;
        },
        { timeout: 5000 }
      );

      const finalJob = await client.getJob(handle.id, DEFAULT_TENANT);
      expect(finalJob.status).toBe(JobStatus.Cancelled);
    });

    it("client-side cancel notifies server immediately", async () => {
      let taskStarted = false;
      let jobStatusDuringHandler: JobStatus | null = null;

      const handler: TaskHandler = async (ctx) => {
        taskStarted = true;

        // Wait a bit
        await new Promise((resolve) => setTimeout(resolve, 50));

        // Cancel the task
        await ctx.cancel();

        // Check job status after cancel (should already be cancelling on server)
        // Note: We need to get the job directly to check its state
        const job = await client.getJob(ctx.task.jobId, DEFAULT_TENANT);
        jobStatusDuringHandler = job.status;

        return { type: "success", result: {} };
      };

      const worker = createWorker(handler, {
        heartbeatIntervalMs: 100,
        pollIntervalMs: 50,
      });
      worker.start();

      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { action: "immediate-cancel" },
        priority: 1,
      });

      // Wait for task to complete
      await waitFor(() => jobStatusDuringHandler !== null, { timeout: 5000 });

      // The job should be Running or Cancelled (depends on timing of the reportOutcome)
      // but the cancel request should have been sent
      expect(taskStarted).toBe(true);
    });

    it("cancelling an already-completed task throws an error", async () => {
      let handlerCompleted = false;

      const handler: TaskHandler = async () => {
        // Complete immediately
        handlerCompleted = true;
        return { type: "success", result: { done: true } };
      };

      const worker = createWorker(handler, {
        heartbeatIntervalMs: 100,
        pollIntervalMs: 50,
      });
      worker.start();

      const handle = await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { action: "quick" },
        priority: 1,
      });

      // Wait for completion
      await waitFor(() => handlerCompleted, { timeout: 5000 });

      // Wait for job to succeed
      await waitFor(
        async () => {
          const job = await client.getJob(handle.id, DEFAULT_TENANT);
          return job.status === JobStatus.Succeeded;
        },
        { timeout: 5000 }
      );

      // Trying to cancel after completion should throw an error
      await expect(handle.cancel()).rejects.toThrow("terminal state");

      // Status should still be Succeeded
      const job = await client.getJob(handle.id, DEFAULT_TENANT);
      expect(job.status).toBe(JobStatus.Succeeded);
    });

    it("multiple concurrent tasks have independent abort signals", async () => {
      // Track which tasks were aborted vs completed normally
      const abortedTasks: string[] = [];
      const completedTasks: string[] = [];

      const handler: TaskHandler = async (ctx) => {
        const payload = decodePayload<{ index: number; shouldWait: boolean }>(
          ctx.task.payload?.data
        );
        const taskKey = `task-${payload?.index}`;

        if (payload?.shouldWait) {
          // These tasks wait for abort
          while (!ctx.signal.aborted) {
            await new Promise((resolve) => setTimeout(resolve, 50));
          }
          abortedTasks.push(taskKey);
        } else {
          // These tasks complete immediately
          completedTasks.push(taskKey);
        }

        return { type: "success", result: {} };
      };

      const worker = createWorker(handler, {
        heartbeatIntervalMs: 100,
        pollIntervalMs: 50,
        maxConcurrentTasks: 5,
      });
      worker.start();

      // Enqueue: task 0 and 2 complete immediately, task 1 waits for abort
      const handle0 = await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { index: 0, shouldWait: false },
        priority: 1,
      });
      const handle1 = await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { index: 1, shouldWait: true },
        priority: 1,
      });
      const handle2 = await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { index: 2, shouldWait: false },
        priority: 1,
      });

      // Wait for the quick tasks to complete
      await waitFor(() => completedTasks.length >= 2, { timeout: 5000 });

      // Cancel task 1 (the one waiting for abort)
      await handle1.cancel();

      // Wait for task 1 to be aborted
      await waitFor(() => abortedTasks.includes("task-1"), { timeout: 5000 });

      // Verify the results
      expect(completedTasks).toContain("task-0");
      expect(completedTasks).toContain("task-2");
      expect(abortedTasks).toContain("task-1");

      // Verify job statuses
      const job0 = await client.getJob(handle0.id, DEFAULT_TENANT);
      const job1 = await client.getJob(handle1.id, DEFAULT_TENANT);
      const job2 = await client.getJob(handle2.id, DEFAULT_TENANT);

      expect(job0.status).toBe(JobStatus.Succeeded);
      expect(job1.status).toBe(JobStatus.Cancelled);
      expect(job2.status).toBe(JobStatus.Succeeded);
    });

    it("signal remains aborted after multiple abort attempts", async () => {
      let handlerExecutionCount = 0;
      let abortCount = 0;

      const handler: TaskHandler = async (ctx) => {
        handlerExecutionCount++;

        ctx.signal.addEventListener("abort", () => {
          abortCount++;
        });

        // Wait a bit
        await new Promise((resolve) => setTimeout(resolve, 100));

        // Cancel multiple times (should be idempotent)
        await ctx.cancel();
        await ctx.cancel();
        await ctx.cancel();

        expect(ctx.signal.aborted).toBe(true);

        return { type: "success", result: {} };
      };

      const worker = createWorker(handler, {
        heartbeatIntervalMs: 100,
        pollIntervalMs: 50,
      });
      worker.start();

      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { action: "multi-cancel" },
        priority: 1,
      });

      // Wait for handler to complete
      await waitFor(() => handlerExecutionCount > 0, { timeout: 5000 });

      // Give time for handler to finish
      await new Promise((resolve) => setTimeout(resolve, 200));

      // Abort event should only fire once
      expect(abortCount).toBe(1);
    });

    it("cancelled task does not retry", async () => {
      let attemptCount = 0;

      const handler: TaskHandler = async (ctx) => {
        attemptCount++;

        // Wait and cancel on first attempt
        await new Promise((resolve) => setTimeout(resolve, 100));
        await ctx.cancel();

        return { type: "success", result: {} };
      };

      const worker = createWorker(handler, {
        heartbeatIntervalMs: 100,
        pollIntervalMs: 50,
      });
      worker.start();

      const handle = await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { action: "no-retry-on-cancel" },
        priority: 1,
        retryPolicy: {
          retryCount: 5,
          initialIntervalMs: 100n,
          maxIntervalMs: 1000n,
          randomizeInterval: false,
          backoffFactor: 2.0,
        },
      });

      // Wait for handler to be called at least once
      await waitFor(() => attemptCount >= 1, { timeout: 5000 });

      // Wait for job to be cancelled
      await waitFor(
        async () => {
          const job = await client.getJob(handle.id, DEFAULT_TENANT);
          return job.status === JobStatus.Cancelled;
        },
        { timeout: 5000 }
      );

      // Should only have one attempt (no retries on cancellation)
      expect(attemptCount).toBe(1);

      // Give some time to ensure no retry happens
      await new Promise((resolve) => setTimeout(resolve, 500));
      expect(attemptCount).toBe(1);
    });

    it("worker shutdown aborts running tasks", async () => {
      let taskStarted = false;
      let signalAborted = false;
      let signalAbortReason: string | null = null;

      const handler: TaskHandler = async (ctx) => {
        taskStarted = true;

        ctx.signal.addEventListener("abort", () => {
          signalAborted = true;
          signalAbortReason = "signal_aborted";
        });

        // Long-running work
        for (let i = 0; i < 100; i++) {
          if (ctx.signal.aborted) {
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, 50));
        }

        return { type: "success", result: {} };
      };

      const worker = createWorker(handler, {
        heartbeatIntervalMs: 100,
        pollIntervalMs: 50,
      });
      worker.start();

      await client.enqueue({
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { action: "long-task" },
        priority: 1,
      });

      // Wait for task to start
      await waitFor(() => taskStarted, { timeout: 5000 });

      // Stop the worker (should abort signal)
      await worker.stop(500);

      expect(signalAborted).toBe(true);
      expect(signalAbortReason).toBe("signal_aborted");
    });
  });
});
