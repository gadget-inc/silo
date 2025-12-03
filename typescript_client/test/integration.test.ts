import { describe, it, expect, beforeAll, afterAll } from "vitest";
import {
  SiloGrpcClient,
  decodePayload,
  GubernatorAlgorithm,
} from "../src/client";

const SILO_SERVER = process.env.SILO_SERVER || "localhost:50051";
const RUN_INTEGRATION =
  process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";

describe.skipIf(!RUN_INTEGRATION)("SiloGrpcClient integration", () => {
  let client: SiloGrpcClient;

  beforeAll(() => {
    client = new SiloGrpcClient({
      server: SILO_SERVER,
      useTls: false,
    });
  });

  afterAll(() => {
    client.close();
  });

  describe("enqueue and getJob", () => {
    it("enqueues a job and retrieves it", async () => {
      const payload = { task: "send-email", to: "test@example.com" };

      const jobId = await client.enqueue({
        shard: "0",
        payload,
        priority: 10,
        metadata: { source: "integration-test" },
      });

      expect(jobId).toBeTruthy();
      expect(typeof jobId).toBe("string");

      // Retrieve the job
      const job = await client.getJob("0", jobId);
      expect(job).toBeDefined();
      expect(job?.id).toBe(jobId);
      expect(job?.priority).toBe(10);
      expect(job?.metadata?.source).toBe("integration-test");

      // Decode and verify payload
      const retrievedPayload = decodePayload(job?.payload?.data);
      expect(retrievedPayload).toEqual(payload);
    });

    it("enqueues a job with custom id", async () => {
      const customId = `custom-${Date.now()}`;
      const jobId = await client.enqueue({
        shard: "0",
        id: customId,
        payload: { data: "test" },
      });

      expect(jobId).toBe(customId);

      const job = await client.getJob("0", customId);
      expect(job?.id).toBe(customId);
    });

    it("enqueues a job with retry policy", async () => {
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

      expect(jobId).toBeTruthy();
      const job = await client.getJob("0", jobId);
      expect(job?.retryPolicy?.retryCount).toBe(3);
      expect(job?.retryPolicy?.backoffFactor).toBe(2.0);
    });

    it("enqueues a job with concurrency limits", async () => {
      const jobId = await client.enqueue({
        shard: "0",
        payload: { data: "concurrency-test" },
        limits: [{ type: "concurrency", key: "user:123", maxConcurrency: 5 }],
      });

      expect(jobId).toBeTruthy();
      const job = await client.getJob("0", jobId);
      expect(job?.limits).toHaveLength(1);

      // Limits are automatically converted to TypeScript-native format
      const limit = job!.limits[0];
      expect(limit.type).toBe("concurrency");
      if (limit.type === "concurrency") {
        expect(limit.key).toBe("user:123");
        expect(limit.maxConcurrency).toBe(5);
      }
    });

    it("enqueues a job with rate limits", async () => {
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
            algorithm: GubernatorAlgorithm.TOKEN_BUCKET,
            retryPolicy: {
              initialBackoffMs: 100n,
              maxBackoffMs: 5000n,
              backoffMultiplier: 2.0,
              maxRetries: 10,
            },
          },
        ],
      });

      expect(jobId).toBeTruthy();
      const job = await client.getJob("0", jobId);
      expect(job?.limits).toHaveLength(1);

      // Limits are automatically converted to TypeScript-native format
      const limit = job!.limits[0];
      expect(limit.type).toBe("rateLimit");
      if (limit.type === "rateLimit") {
        expect(limit.name).toBe("api-requests");
        expect(limit.uniqueKey).toBe("user:456");
        expect(limit.limit).toBe(100n);
        expect(limit.durationMs).toBe(60000n);
        expect(limit.hits).toBe(1);
        expect(limit.algorithm).toBe(GubernatorAlgorithm.TOKEN_BUCKET);
        expect(limit.retryPolicy?.initialBackoffMs).toBe(100n);
        expect(limit.retryPolicy?.maxBackoffMs).toBe(5000n);
        expect(limit.retryPolicy?.backoffMultiplier).toBe(2.0);
        expect(limit.retryPolicy?.maxRetries).toBe(10);
      }
    });

    it("enqueues a job with mixed limits", async () => {
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

      expect(jobId).toBeTruthy();
      const job = await client.getJob("0", jobId);
      expect(job?.limits).toHaveLength(2);

      // Limits are automatically converted to TypeScript-native format
      expect(job!.limits[0].type).toBe("concurrency");
      expect(job!.limits[1].type).toBe("rateLimit");
    });
  });

  describe("leaseTasks and reportOutcome", () => {
    it("leases a task and reports success", async () => {
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

      expect(task).toBeDefined();
      expect(task?.attemptNumber).toBe(1);
      expect(task?.priority).toBe(1);

      // Verify payload
      const taskPayload = decodePayload(task?.payload?.data);
      expect(taskPayload).toEqual(payload);

      // Report success
      await client.reportOutcome({
        shard: "0",
        taskId: task!.id,
        outcome: {
          type: "success",
          result: { processed: true, output: "done" },
        },
      });
    });

    it("reports failure outcome", async () => {
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

      expect(task).toBeDefined();

      await client.reportOutcome({
        shard: "0",
        taskId: task!.id,
        outcome: {
          type: "failure",
          code: "PROCESSING_ERROR",
          data: { reason: "Something went wrong" },
        },
      });
    });
  });

  describe("heartbeat", () => {
    it("extends a task lease via heartbeat", async () => {
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
      expect(task).toBeDefined();

      // Send heartbeat
      await client.heartbeat("0", "test-worker-3", task!.id);

      // Report success to clean up
      await client.reportOutcome({
        shard: "0",
        taskId: task!.id,
        outcome: { type: "success", result: {} },
      });
    });
  });

  describe("deleteJob", () => {
    it("deletes a completed job", async () => {
      const uniqueJobId = `delete-test-${Date.now()}`;
      const jobId = await client.enqueue({
        shard: "0",
        id: uniqueJobId,
        payload: { action: "delete-test" },
        priority: 1,
      });

      // Verify it exists
      const job = await client.getJob("0", jobId);
      expect(job?.id).toBe(jobId);

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
      expect(task).toBeDefined();

      await client.reportOutcome({
        shard: "0",
        taskId: task!.id,
        outcome: { type: "success", result: {} },
      });

      // Now delete the completed job
      await client.deleteJob("0", jobId);
    });
  });

  describe("query", () => {
    it("queries jobs with SQL", async () => {
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
      expect(result.rowCount).toBeGreaterThanOrEqual(3);
      expect(result.columns.length).toBeGreaterThan(0);
      expect(result.columns.some((c) => c.name === "id")).toBe(true);
      expect(result.columns.some((c) => c.name === "priority")).toBe(true);
    });

    it("queries with WHERE clause", async () => {
      const result = await client.query(
        "0",
        "SELECT id, priority FROM jobs WHERE priority < 20"
      );
      expect(result.columns.length).toBe(2);
    });

    it("queries with aggregation", async () => {
      const result = await client.query(
        "0",
        "SELECT COUNT(*) as count FROM jobs"
      );
      expect(result.rowCount).toBe(1);
      expect(result.columns.some((c) => c.name === "count")).toBe(true);

      // Parse the row to verify count
      const row = decodePayload<{ count: number }>(result.rows[0]?.data);
      expect(typeof row?.count).toBe("number");
    });
  });
});
