"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const client_1 = require("../src/client");
const SILO_SERVER = process.env.SILO_SERVER || "localhost:50051";
const RUN_INTEGRATION = process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";
vitest_1.describe.skipIf(!RUN_INTEGRATION)("SiloGrpcClient integration", () => {
  let client;
  (0, vitest_1.beforeAll)(() => {
    client = new client_1.SiloGrpcClient({
      server: SILO_SERVER,
      useTls: false,
    });
  });
  (0, vitest_1.afterAll)(() => {
    client.close();
  });
  (0, vitest_1.describe)("enqueue and getJob", () => {
    (0, vitest_1.it)("enqueues a job and retrieves it", async () => {
      const payload = { task: "send-email", to: "test@example.com" };
      const jobId = await client.enqueue({
        shard: "0",
        payload,
        priority: 10,
        metadata: { source: "integration-test" },
      });
      (0, vitest_1.expect)(jobId).toBeTruthy();
      (0, vitest_1.expect)(typeof jobId).toBe("string");
      // Retrieve the job
      const job = await client.getJob("0", jobId);
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
      const jobId = await client.enqueue({
        shard: "0",
        id: customId,
        payload: { data: "test" },
      });
      (0, vitest_1.expect)(jobId).toBe(customId);
      const job = await client.getJob("0", customId);
      (0, vitest_1.expect)(job?.id).toBe(customId);
    });
    (0, vitest_1.it)("enqueues a job with retry policy", async () => {
      const jobId = await client.enqueue({
        shard: "0",
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
      const job = await client.getJob("0", jobId);
      (0, vitest_1.expect)(job?.retryPolicy?.retryCount).toBe(3);
      (0, vitest_1.expect)(job?.retryPolicy?.backoffFactor).toBe(2.0);
    });
    (0, vitest_1.it)("enqueues a job with concurrency limits", async () => {
      const jobId = await client.enqueue({
        shard: "0",
        payload: { data: "concurrency-test" },
        limits: [{ type: "concurrency", key: "user:123", maxConcurrency: 5 }],
      });
      (0, vitest_1.expect)(jobId).toBeTruthy();
      const job = await client.getJob("0", jobId);
      (0, vitest_1.expect)(job?.limits).toHaveLength(1);
      // Limits are automatically converted to TypeScript-native format
      const limit = job.limits[0];
      (0, vitest_1.expect)(limit.type).toBe("concurrency");
      if (limit.type === "concurrency") {
        (0, vitest_1.expect)(limit.key).toBe("user:123");
        (0, vitest_1.expect)(limit.maxConcurrency).toBe(5);
      }
    });
    (0, vitest_1.it)("enqueues a job with rate limits", async () => {
      const jobId = await client.enqueue({
        shard: "0",
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
      const job = await client.getJob("0", jobId);
      (0, vitest_1.expect)(job?.limits).toHaveLength(1);
      // Limits are automatically converted to TypeScript-native format
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
      const jobId = await client.enqueue({
        shard: "0",
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
      const job = await client.getJob("0", jobId);
      (0, vitest_1.expect)(job?.limits).toHaveLength(2);
      // Limits are automatically converted to TypeScript-native format
      (0, vitest_1.expect)(job.limits[0].type).toBe("concurrency");
      (0, vitest_1.expect)(job.limits[1].type).toBe("rateLimit");
    });
  });
  (0, vitest_1.describe)("leaseTasks and reportOutcome", () => {
    (0, vitest_1.it)("leases a task and reports success", async () => {
      // Enqueue a job with a unique ID
      const uniqueJobId = `lease-success-${Date.now()}`;
      const payload = { action: "process", value: 42 };
      const jobId = await client.enqueue({
        shard: "0",
        id: uniqueJobId,
        payload,
        priority: 1, // High priority
      });
      // Lease tasks - may need multiple attempts to get our task
      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const tasks = await client.leaseTasks({
          shard: "0",
          workerId: `test-worker-lease-${Date.now()}`,
          maxTasks: 50,
        });
        task = tasks.find((t) => t.jobId === jobId);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }
      (0, vitest_1.expect)(task).toBeDefined();
      (0, vitest_1.expect)(task?.attemptNumber).toBe(1);
      (0, vitest_1.expect)(task?.priority).toBe(1);
      // Verify payload
      const taskPayload = (0, client_1.decodePayload)(task?.payload?.data);
      (0, vitest_1.expect)(taskPayload).toEqual(payload);
      // Report success
      await client.reportOutcome({
        shard: "0",
        taskId: task.id,
        outcome: {
          type: "success",
          result: { processed: true, output: "done" },
        },
      });
    });
    (0, vitest_1.it)("reports failure outcome", async () => {
      const uniqueJobId = `fail-test-${Date.now()}`;
      const jobId = await client.enqueue({
        shard: "0",
        id: uniqueJobId,
        payload: { action: "fail-test" },
        priority: 1,
      });
      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const tasks = await client.leaseTasks({
          shard: "0",
          workerId: `test-worker-fail-${Date.now()}`,
          maxTasks: 50,
        });
        task = tasks.find((t) => t.jobId === jobId);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }
      (0, vitest_1.expect)(task).toBeDefined();
      await client.reportOutcome({
        shard: "0",
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
      const jobId = await client.enqueue({
        shard: "0",
        payload: { action: "heartbeat-test" },
        priority: 1,
      });
      const tasks = await client.leaseTasks({
        shard: "0",
        workerId: "test-worker-3",
        maxTasks: 10,
      });
      const task = tasks.find((t) => t.jobId === jobId);
      (0, vitest_1.expect)(task).toBeDefined();
      // Send heartbeat
      await client.heartbeat("0", "test-worker-3", task.id);
      // Report success to clean up
      await client.reportOutcome({
        shard: "0",
        taskId: task.id,
        outcome: { type: "success", result: {} },
      });
    });
  });
  (0, vitest_1.describe)("deleteJob", () => {
    (0, vitest_1.it)("deletes a completed job", async () => {
      const uniqueJobId = `delete-test-${Date.now()}`;
      const jobId = await client.enqueue({
        shard: "0",
        id: uniqueJobId,
        payload: { action: "delete-test" },
        priority: 1,
      });
      // Verify it exists
      const job = await client.getJob("0", jobId);
      (0, vitest_1.expect)(job?.id).toBe(jobId);
      // Lease and complete the job first
      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const tasks = await client.leaseTasks({
          shard: "0",
          workerId: `delete-test-worker-${Date.now()}`,
          maxTasks: 50,
        });
        task = tasks.find((t) => t.jobId === jobId);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }
      (0, vitest_1.expect)(task).toBeDefined();
      await client.reportOutcome({
        shard: "0",
        taskId: task.id,
        outcome: { type: "success", result: {} },
      });
      // Now delete the completed job
      await client.deleteJob("0", jobId);
    });
  });
  (0, vitest_1.describe)("query", () => {
    (0, vitest_1.it)("queries jobs with SQL", async () => {
      // Enqueue some jobs with specific metadata
      const testBatch = `batch-${Date.now()}`;
      for (let i = 0; i < 3; i++) {
        await client.enqueue({
          shard: "0",
          payload: { index: i },
          priority: 10 + i,
          metadata: { batch: testBatch },
        });
      }
      // Query all jobs
      const result = await client.query("0", "SELECT * FROM jobs");
      (0, vitest_1.expect)(result.rowCount).toBeGreaterThanOrEqual(3);
      (0, vitest_1.expect)(result.columns.length).toBeGreaterThan(0);
      (0, vitest_1.expect)(result.columns.some((c) => c.name === "id")).toBe(true);
      (0, vitest_1.expect)(result.columns.some((c) => c.name === "priority")).toBe(true);
    });
    (0, vitest_1.it)("queries with WHERE clause", async () => {
      const result = await client.query("0", "SELECT id, priority FROM jobs WHERE priority < 20");
      (0, vitest_1.expect)(result.columns.length).toBe(2);
    });
    (0, vitest_1.it)("queries with aggregation", async () => {
      const result = await client.query("0", "SELECT COUNT(*) as count FROM jobs");
      (0, vitest_1.expect)(result.rowCount).toBe(1);
      (0, vitest_1.expect)(result.columns.some((c) => c.name === "count")).toBe(true);
      // Parse the row to verify count
      const row = (0, client_1.decodePayload)(result.rows[0]?.data);
      (0, vitest_1.expect)(typeof row?.count).toBe("number");
    });
  });
});
//# sourceMappingURL=integration.test.js.map
