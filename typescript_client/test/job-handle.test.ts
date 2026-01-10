import { describe, it, expect, beforeAll, afterAll } from "vitest";
import {
  SiloGRPCClient,
  JobStatus,
  JobNotFoundError,
  decodePayload,
} from "../src/client";

// Support comma-separated list of servers for multi-node testing
const SILO_SERVERS = (
  process.env.SILO_SERVERS ||
  process.env.SILO_SERVER ||
  "localhost:50051"
).split(",");
const RUN_INTEGRATION =
  process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";

/**
 * Integration tests for JobHandle.
 * These tests exercise the full stack from JobHandle -> SiloGRPCClient -> gRPC -> Server.
 */
describe.skipIf(!RUN_INTEGRATION)("JobHandle integration", () => {
  let client: SiloGRPCClient;

  beforeAll(async () => {
    client = new SiloGRPCClient({
      servers: SILO_SERVERS,
      useTls: false,
      shardRouting: {
        topologyRefreshIntervalMs: 0,
      },
    });
    await client.refreshTopology();
  });

  afterAll(() => {
    client.close();
  });

  describe("properties", () => {
    it("exposes id and tenant from enqueue", async () => {
      const tenant = "handle-props-tenant";
      const handle = await client.enqueue({
        tenant,
        payload: { test: "props" },
      });

      expect(handle.id).toBeTruthy();
      expect(handle.tenant).toBe(tenant);
    });

    it("exposes id and tenant from factory", () => {
      const handle = client.handle("test-job-id", "test-tenant");

      expect(handle.id).toBe("test-job-id");
      expect(handle.tenant).toBe("test-tenant");
    });

    it("has undefined tenant when not provided", () => {
      const handle = client.handle("test-job-id");

      expect(handle.id).toBe("test-job-id");
      expect(handle.tenant).toBeUndefined();
    });
  });

  describe("getJob", () => {
    it("returns job details", async () => {
      const tenant = "handle-getjob-tenant";
      const payload = { action: "test-action", value: 123 };

      const handle = await client.enqueue({
        tenant,
        payload,
        priority: 25,
        metadata: { source: "job-handle-test" },
      });

      const job = await handle.getJob();

      expect(job.id).toBe(handle.id);
      expect(job.priority).toBe(25);
      expect(job.status).toBe(JobStatus.Scheduled);
      expect(job.metadata?.source).toBe("job-handle-test");
      expect(decodePayload(job.payload?.data)).toEqual(payload);
    });

    it("throws JobNotFoundError when job does not exist", async () => {
      const handle = client.handle("nonexistent-job-id", "some-tenant");

      await expect(handle.getJob()).rejects.toThrow(JobNotFoundError);
      await expect(handle.getJob()).rejects.toThrow(/not found/);
    });
  });

  describe("getStatus", () => {
    it("returns Scheduled for newly enqueued job", async () => {
      const tenant = "handle-status-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { test: "status" },
      });

      const status = await handle.getStatus();
      expect(status).toBe(JobStatus.Scheduled);
    });

    it("returns Running after task is leased", async () => {
      const tenant = "handle-running-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { test: "running" },
      });

      const shard = client.getShardForJob(handle.id);

      // Lease the task
      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const result = await client.leaseTasks({
          workerId: `handle-running-worker-${Date.now()}`,
          maxTasks: 50,
          shard,
        });
        task = result.tasks.find((t) => t.jobId === handle.id);
        if (!task) await new Promise((r) => setTimeout(r, 100));
      }
      expect(task).toBeDefined();

      const status = await handle.getStatus();
      expect(status).toBe(JobStatus.Running);

      // Clean up - report success
      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
        tenant,
        outcome: { type: "success", result: {} },
      });
    });

    it("throws JobNotFoundError when job does not exist", async () => {
      const handle = client.handle("nonexistent-status-id", "some-tenant");

      await expect(handle.getStatus()).rejects.toThrow(JobNotFoundError);
    });
  });

  describe("cancel", () => {
    it("cancels a scheduled job", async () => {
      const tenant = "handle-cancel-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { test: "cancel" },
      });

      // Verify it's scheduled
      expect(await handle.getStatus()).toBe(JobStatus.Scheduled);

      // Cancel it
      await handle.cancel();

      // Verify it's cancelled
      expect(await handle.getStatus()).toBe(JobStatus.Cancelled);
    });

    it("throws JobNotFoundError when job does not exist", async () => {
      const handle = client.handle("nonexistent-cancel-id", "some-tenant");

      await expect(handle.cancel()).rejects.toThrow(JobNotFoundError);
    });
  });

  describe("delete", () => {
    it("deletes a completed job", async () => {
      const tenant = "handle-delete-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { test: "delete" },
      });

      const shard = client.getShardForJob(handle.id);

      // Lease and complete the task
      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const result = await client.leaseTasks({
          workerId: `handle-delete-worker-${Date.now()}`,
          maxTasks: 50,
          shard,
        });
        task = result.tasks.find((t) => t.jobId === handle.id);
        if (!task) await new Promise((r) => setTimeout(r, 100));
      }
      expect(task).toBeDefined();

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
        tenant,
        outcome: { type: "success", result: {} },
      });

      // Delete the completed job
      await handle.delete();

      // Job should no longer exist
      await expect(handle.getJob()).rejects.toThrow(JobNotFoundError);
    });

    it("succeeds silently when job does not exist (idempotent)", async () => {
      const handle = client.handle("nonexistent-delete-id", "some-tenant");

      // Delete is idempotent - doesn't throw for non-existent jobs
      await handle.delete();
    });
  });

  describe("awaitResult", () => {
    it("returns success result when job completes successfully", async () => {
      const tenant = "handle-await-success-tenant";
      const resultData = { processed: true, count: 42 };

      const handle = await client.enqueue({
        tenant,
        payload: { test: "await-success" },
      });

      const shard = client.getShardForJob(handle.id);

      // Lease and complete the task
      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const result = await client.leaseTasks({
          workerId: `handle-await-success-worker-${Date.now()}`,
          maxTasks: 50,
          shard,
        });
        task = result.tasks.find((t) => t.jobId === handle.id);
        if (!task) await new Promise((r) => setTimeout(r, 100));
      }
      expect(task).toBeDefined();

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
        tenant,
        outcome: { type: "success", result: resultData },
      });

      // Await the result
      const result = await handle.awaitResult<typeof resultData>({
        pollIntervalMs: 50,
        timeoutMs: 5000,
      });

      expect(result.status).toBe(JobStatus.Succeeded);
      expect(result.result).toEqual(resultData);
    });

    it("returns failure result when job fails", async () => {
      const tenant = "handle-await-fail-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { test: "await-fail" },
      });

      const shard = client.getShardForJob(handle.id);

      // Lease and fail the task
      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const result = await client.leaseTasks({
          workerId: `handle-await-fail-worker-${Date.now()}`,
          maxTasks: 50,
          shard,
        });
        task = result.tasks.find((t) => t.jobId === handle.id);
        if (!task) await new Promise((r) => setTimeout(r, 100));
      }
      expect(task).toBeDefined();

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
        tenant,
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

      expect(result.status).toBe(JobStatus.Failed);
      if (result.status === JobStatus.Failed) {
        expect(result.errorCode).toBe("TEST_ERROR");
        expect(result.errorData).toEqual({ reason: "intentional failure" });
      }
    });

    it("returns cancelled result when job is cancelled", async () => {
      const tenant = "handle-await-cancel-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { test: "await-cancel" },
      });

      // Cancel the job
      await handle.cancel();

      // Await the result
      const result = await handle.awaitResult({
        pollIntervalMs: 50,
        timeoutMs: 5000,
      });

      expect(result.status).toBe(JobStatus.Cancelled);
    });

    it("throws timeout error when job does not complete in time", async () => {
      const tenant = "handle-await-timeout-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { test: "await-timeout" },
        // Schedule far in the future so it won't be leased
        startAtMs: BigInt(Date.now() + 60000),
      });

      await expect(
        handle.awaitResult({ pollIntervalMs: 50, timeoutMs: 200 })
      ).rejects.toThrow(/Timeout/);
    });

    it("throws JobNotFoundError when job does not exist", async () => {
      const handle = client.handle("nonexistent-await-id", "some-tenant");

      await expect(
        handle.awaitResult({ pollIntervalMs: 50, timeoutMs: 1000 })
      ).rejects.toThrow(JobNotFoundError);
    });
  });
});
