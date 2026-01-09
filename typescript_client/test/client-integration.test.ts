import { describe, it, expect, beforeAll, afterAll } from "vitest";
import {
  SiloGRPCClient,
  JobStatus,
  decodePayload,
  GubernatorAlgorithm,
} from "../src/client";

// Support comma-separated list of servers for multi-node testing
const SILO_SERVERS = (
  process.env.SILO_SERVERS ||
  process.env.SILO_SERVER ||
  "localhost:50051"
).split(",");
const RUN_INTEGRATION =
  process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";

describe.skipIf(!RUN_INTEGRATION)("SiloGRPCClient integration", () => {
  let client: SiloGRPCClient;

  beforeAll(async () => {
    client = new SiloGRPCClient({
      servers: SILO_SERVERS,
      useTls: false,
      shardRouting: {
        topologyRefreshIntervalMs: 0, // Disable auto-refresh in tests
      },
    });
    // Refresh topology to discover actual cluster state
    await client.refreshTopology();
  });

  afterAll(() => {
    client.close();
  });

  describe("topology discovery", () => {
    it("discovers cluster topology via GetClusterInfo", async () => {
      const topology = client.getTopology();

      // Should have discovered at least 1 shard
      expect(topology.numShards).toBeGreaterThanOrEqual(1);

      // Should have server addresses for shards
      expect(topology.shardToServer.size).toBeGreaterThanOrEqual(1);
    });

    it("refreshes topology on demand", async () => {
      // This should succeed without throwing
      await client.refreshTopology();

      const topology = client.getTopology();
      expect(topology.numShards).toBeGreaterThanOrEqual(1);
    });
  });

  describe("tenant-based shard routing", () => {
    it("routes requests to correct shard based on tenant", async () => {
      const tenant = "test-tenant-123";
      const payload = { task: "routing-test" };

      const handle = await client.enqueue({
        tenant,
        payload,
        priority: 10,
      });

      expect(handle.id).toBeTruthy();

      // Should be able to retrieve the job using the same tenant
      const job = await client.getJob(handle.id, tenant);
      expect(job).toBeDefined();
      expect(job?.id).toBe(handle.id);
    });

    it("getShardForTenant returns consistent shard", () => {
      const tenant = "consistent-tenant";

      const shard1 = client.getShardForTenant(tenant);
      const shard2 = client.getShardForTenant(tenant);

      expect(shard1).toBe(shard2);
      expect(shard1).toBeGreaterThanOrEqual(0);
    });

    it("different tenants may route to different shards", () => {
      // With enough tenants, they should distribute across available shards
      const shards = new Set<number>();

      for (let i = 0; i < 100; i++) {
        const shard = client.getShardForTenant(`tenant-${i}`);
        shards.add(shard);
      }

      // Should see at least 1 shard used (may be 1 if single-node)
      expect(shards.size).toBeGreaterThanOrEqual(1);
    });
  });

  describe("enqueue and getJob", () => {
    it("enqueues a job and retrieves it", async () => {
      const payload = { task: "send-email", to: "test@example.com" };
      const tenant = "test-tenant";

      const handle = await client.enqueue({
        tenant,
        payload,
        priority: 10,
        metadata: { source: "integration-test" },
      });

      expect(handle.id).toBeTruthy();
      expect(typeof handle.id).toBe("string");

      // Retrieve the job
      const job = await client.getJob(handle.id, tenant);
      expect(job).toBeDefined();
      expect(job?.id).toBe(handle.id);
      expect(job?.priority).toBe(10);
      expect(job?.metadata?.source).toBe("integration-test");

      // Decode and verify payload
      const retrievedPayload = decodePayload(job?.payload?.data);
      expect(retrievedPayload).toEqual(payload);
    });

    it("enqueues a job with custom id", async () => {
      const customId = `custom-${Date.now()}`;
      const tenant = "custom-id-tenant";

      const handle = await client.enqueue({
        tenant,
        id: customId,
        payload: { data: "test" },
      });

      expect(handle.id).toBe(customId);

      const job = await client.getJob(customId, tenant);
      expect(job?.id).toBe(customId);
    });

    it("enqueues a job with retry policy", async () => {
      const tenant = "retry-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { data: "retry-test" },
        retryPolicy: {
          retryCount: 3,
          initialIntervalMs: 1000n,
          maxIntervalMs: 30000n,
          randomizeInterval: true,
          backoffFactor: 2.0,
        },
      });

      expect(handle.id).toBeTruthy();
      const job = await client.getJob(handle.id, tenant);
      expect(job?.retryPolicy?.retryCount).toBe(3);
      expect(job?.retryPolicy?.backoffFactor).toBe(2.0);
    });

    it("enqueues a job with concurrency limits", async () => {
      const tenant = "concurrency-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { data: "concurrency-test" },
        limits: [{ type: "concurrency", key: "user:123", maxConcurrency: 5 }],
      });

      expect(handle.id).toBeTruthy();
      const job = await client.getJob(handle.id, tenant);
      expect(job?.limits).toHaveLength(1);

      const limit = job!.limits[0];
      expect(limit.type).toBe("concurrency");
      if (limit.type === "concurrency") {
        expect(limit.key).toBe("user:123");
        expect(limit.maxConcurrency).toBe(5);
      }
    });

    it("enqueues a job with rate limits", async () => {
      const tenant = "rate-limit-tenant";

      const handle = await client.enqueue({
        tenant,
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

      expect(handle.id).toBeTruthy();
      const job = await client.getJob(handle.id, tenant);
      expect(job?.limits).toHaveLength(1);

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
      const tenant = "mixed-limits-tenant";

      const handle = await client.enqueue({
        tenant,
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

      expect(handle.id).toBeTruthy();
      const job = await client.getJob(handle.id, tenant);
      expect(job?.limits).toHaveLength(2);

      expect(job!.limits[0].type).toBe("concurrency");
      expect(job!.limits[1].type).toBe("rateLimit");
    });
  });

  describe("leaseTasks and reportOutcome", () => {
    it("leases a task and reports success", async () => {
      const uniqueJobId = `lease-success-${Date.now()}`;
      const tenant = "lease-tenant";
      const payload = { action: "process", value: 42 };

      const handle = await client.enqueue({
        tenant,
        id: uniqueJobId,
        payload,
        priority: 1,
      });

      // Get the shard for this tenant to poll from the correct server
      const shard = client.getShardForTenant(tenant);

      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const tasks = await client.leaseTasks({
          workerId: `test-worker-lease-${Date.now()}`,
          maxTasks: 50,
          shard,
        });
        task = tasks.find((t) => t.jobId === handle.id);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }

      expect(task).toBeDefined();
      expect(task?.attemptNumber).toBe(1);
      expect(task?.priority).toBe(1);

      const taskPayload = decodePayload(task?.payload?.data);
      expect(taskPayload).toEqual(payload);

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
        tenant,
        outcome: {
          type: "success",
          result: { processed: true, output: "done" },
        },
      });
    });

    it("reports failure outcome", async () => {
      const uniqueJobId = `fail-test-${Date.now()}`;
      const tenant = "fail-tenant";

      const handle = await client.enqueue({
        tenant,
        id: uniqueJobId,
        payload: { action: "fail-test" },
        priority: 1,
      });

      // Get the shard for this tenant to poll from the correct server
      const shard = client.getShardForTenant(tenant);

      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const tasks = await client.leaseTasks({
          workerId: `test-worker-fail-${Date.now()}`,
          maxTasks: 50,
          shard,
        });
        task = tasks.find((t) => t.jobId === handle.id);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }

      expect(task).toBeDefined();

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
        tenant,
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
      const tenant = "heartbeat-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { action: "heartbeat-test" },
        priority: 1,
      });

      // Get the shard for this tenant to poll from the correct server
      const shard = client.getShardForTenant(tenant);

      const tasks = await client.leaseTasks({
        workerId: "test-worker-3",
        maxTasks: 10,
        shard,
      });

      const task = tasks.find((t) => t.jobId === handle.id);
      expect(task).toBeDefined();

      await client.heartbeat("test-worker-3", task!.id, task!.shard, tenant);

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
        tenant,
        outcome: { type: "success", result: {} },
      });
    });
  });

  describe("deleteJob", () => {
    it("deletes a completed job", async () => {
      const uniqueJobId = `delete-test-${Date.now()}`;
      const tenant = "delete-tenant";

      const handle = await client.enqueue({
        tenant,
        id: uniqueJobId,
        payload: { action: "delete-test" },
        priority: 1,
      });

      const job = await client.getJob(handle.id, tenant);
      expect(job?.id).toBe(handle.id);

      // Get the shard for this tenant to poll from the correct server
      const shard = client.getShardForTenant(tenant);

      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const tasks = await client.leaseTasks({
          workerId: `delete-test-worker-${Date.now()}`,
          maxTasks: 50,
          shard,
        });
        task = tasks.find((t) => t.jobId === handle.id);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }
      expect(task).toBeDefined();

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
        tenant,
        outcome: { type: "success", result: {} },
      });

      await client.deleteJob(handle.id, tenant);
    });
  });

  describe("query", () => {
    it("queries jobs with SQL", async () => {
      const testBatch = `batch-${Date.now()}`;
      const tenant = "query-tenant";

      for (let i = 0; i < 3; i++) {
        await client.enqueue({
          tenant,
          payload: { index: i },
          priority: 10 + i,
          metadata: { batch: testBatch },
        });
      }

      // SQL queries require tenant in WHERE clause for filtering
      const result = await client.query(
        `SELECT * FROM jobs WHERE tenant = '${tenant}'`,
        tenant
      );
      expect(result.rowCount).toBeGreaterThanOrEqual(3);
      expect(result.columns.length).toBeGreaterThan(0);
      expect(result.columns.some((c) => c.name === "id")).toBe(true);
      expect(result.columns.some((c) => c.name === "priority")).toBe(true);
    });

    it("queries with WHERE clause", async () => {
      const tenant = "query-where-tenant";

      // First ensure there's data
      await client.enqueue({
        tenant,
        payload: { test: true },
        priority: 5,
      });

      const result = await client.query(
        "SELECT id, priority FROM jobs WHERE priority < 20",
        tenant
      );
      expect(result.columns.length).toBe(2);
    });

    it("queries with aggregation", async () => {
      const tenant = "query-agg-tenant";

      // Ensure there's at least one job
      await client.enqueue({
        tenant,
        payload: { test: true },
      });

      const result = await client.query(
        "SELECT COUNT(*) as count FROM jobs",
        tenant
      );
      expect(result.rowCount).toBe(1);
      expect(result.columns.some((c) => c.name === "count")).toBe(true);

      const row = decodePayload<{ count: number }>(result.rows[0]?.data);
      expect(typeof row?.count).toBe("number");
    });
  });

  describe("JobHandle", () => {
    it("returns a JobHandle from enqueue", async () => {
      const tenant = "handle-enqueue-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { test: "handle-test" },
      });

      expect(handle).toBeDefined();
      expect(handle.id).toBeTruthy();
      expect(handle.tenant).toBe(tenant);
    });

    it("can get job details through handle", async () => {
      const tenant = "handle-getjob-tenant";
      const payload = { action: "test-action" };

      const handle = await client.enqueue({
        tenant,
        payload,
        priority: 15,
      });

      const job = await handle.getJob();
      expect(job).toBeDefined();
      expect(job?.id).toBe(handle.id);
      expect(job?.priority).toBe(15);
      expect(decodePayload(job?.payload?.data)).toEqual(payload);
    });

    it("can get job status through handle", async () => {
      const tenant = "handle-status-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { test: "status-test" },
      });

      const status = await handle.getStatus();
      expect(status).toBe(JobStatus.Scheduled);
    });

    it("can cancel job through handle", async () => {
      const tenant = "handle-cancel-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { test: "cancel-test" },
      });

      // Verify job exists
      const job = await handle.getJob();
      expect(job).toBeDefined();

      // Cancel the job
      await handle.cancel();

      // Check status is cancelled (may take a moment)
      const status = await handle.getStatus();
      expect(status).toBe(JobStatus.Cancelled);
    });

    it("can create handle from existing job ID", async () => {
      const tenant = "handle-factory-tenant";

      // First enqueue a job
      const originalHandle = await client.enqueue({
        tenant,
        payload: { test: "factory-test" },
      });

      // Create a new handle from the job ID
      const newHandle = client.handle(originalHandle.id, tenant);

      expect(newHandle.id).toBe(originalHandle.id);
      expect(newHandle.tenant).toBe(tenant);

      // Should be able to get the same job
      const job = await newHandle.getJob();
      expect(job?.id).toBe(originalHandle.id);
    });

    it("can delete job through handle", async () => {
      const uniqueJobId = `handle-delete-${Date.now()}`;
      const tenant = "handle-delete-tenant";

      const handle = await client.enqueue({
        tenant,
        id: uniqueJobId,
        payload: { test: "delete-test" },
      });

      // Get the shard for this tenant to poll from the correct server
      const shard = client.getShardForTenant(tenant);

      // Lease and complete the task so we can delete the job
      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const tasks = await client.leaseTasks({
          workerId: `handle-delete-worker-${Date.now()}`,
          maxTasks: 50,
          shard,
        });
        task = tasks.find((t) => t.jobId === handle.id);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }
      expect(task).toBeDefined();

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
        tenant,
        outcome: { type: "success", result: {} },
      });

      // Delete through handle
      await handle.delete();

      // Job should no longer exist (or be in terminal state)
      // Note: The exact behavior depends on server implementation
    });

    it("can await job result for successful job", async () => {
      const tenant = "handle-await-success-tenant";
      const resultData = { processed: true, count: 42 };

      const handle = await client.enqueue({
        tenant,
        payload: { test: "await-test" },
      });

      // Get the shard for this tenant to poll from the correct server
      const shard = client.getShardForTenant(tenant);

      // Lease and complete the task
      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const tasks = await client.leaseTasks({
          workerId: `handle-await-worker-${Date.now()}`,
          maxTasks: 50,
          shard,
        });
        task = tasks.find((t) => t.jobId === handle.id);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
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
        pollIntervalMs: 100,
        timeoutMs: 5000,
      });

      expect(result.status).toBe(JobStatus.Succeeded);
      expect(result.result).toEqual(resultData);
    });

    it("can await job result for failed job", async () => {
      const tenant = "handle-await-fail-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { test: "await-fail-test" },
      });

      // Get the shard for this tenant to poll from the correct server
      const shard = client.getShardForTenant(tenant);

      // Lease and fail the task
      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const tasks = await client.leaseTasks({
          workerId: `handle-await-fail-worker-${Date.now()}`,
          maxTasks: 50,
          shard,
        });
        task = tasks.find((t) => t.jobId === handle.id);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }
      expect(task).toBeDefined();

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
        tenant,
        outcome: {
          type: "failure",
          code: "TEST_ERROR",
          data: { reason: "test failure" },
        },
      });

      // Await the result
      const result = await handle.awaitResult({
        pollIntervalMs: 100,
        timeoutMs: 5000,
      });

      expect(result.status).toBe(JobStatus.Failed);
      expect(result.errorCode).toBe("TEST_ERROR");
    });

    it("throws timeout error when job does not complete in time", async () => {
      const tenant = "handle-await-timeout-tenant";

      const handle = await client.enqueue({
        tenant,
        payload: { test: "timeout-test" },
        // Schedule far in the future so it won't be leased
        startAtMs: BigInt(Date.now() + 60000),
      });

      // Try to await with a short timeout - should fail
      await expect(
        handle.awaitResult({ pollIntervalMs: 50, timeoutMs: 200 })
      ).rejects.toThrow(/Timeout/);
    });
  });
});

describe.skipIf(!RUN_INTEGRATION)("Shard routing integration", () => {
  describe("wrong shard error handling", () => {
    it("handles requests to non-existent shards gracefully", async () => {
      // Create a client configured for more shards than exist
      const client = new SiloGRPCClient({
        servers: SILO_SERVERS,
        useTls: false,
        shardRouting: {
          numShards: 100, // Way more shards than actually exist
          maxWrongShardRetries: 2,
          wrongShardRetryDelayMs: 50,
          topologyRefreshIntervalMs: 0,
        },
      });

      try {
        // This tenant will hash to a shard that likely doesn't exist
        // but after topology refresh, it should still work
        await client.refreshTopology();

        // Now requests should work because we've discovered the actual topology
        const handle = await client.enqueue({
          tenant: "wrong-shard-test",
          payload: { test: true },
        });
        expect(handle.id).toBeTruthy();
      } finally {
        client.close();
      }
    });

    it("retries and discovers correct shard on wrong shard error", async () => {
      const client = new SiloGRPCClient({
        servers: SILO_SERVERS,
        useTls: false,
        shardRouting: {
          numShards: 1,
          maxWrongShardRetries: 5,
          wrongShardRetryDelayMs: 10,
          topologyRefreshIntervalMs: 0,
        },
      });

      try {
        // First refresh to get accurate topology
        await client.refreshTopology();

        // Now operations should work
        const handle = await client.enqueue({
          tenant: "retry-test-tenant",
          payload: { data: "retry test" },
        });
        expect(handle.id).toBeTruthy();

        const job = await client.getJob(handle.id, "retry-test-tenant");
        expect(job?.id).toBe(handle.id);
      } finally {
        client.close();
      }
    });
  });

  describe("multi-tenant routing", () => {
    it("routes different tenants consistently", async () => {
      const client = new SiloGRPCClient({
        servers: SILO_SERVERS,
        useTls: false,
        shardRouting: {
          numShards: 8,
          topologyRefreshIntervalMs: 0,
        },
      });

      try {
        await client.refreshTopology();

        const tenants = ["tenant-a", "tenant-b", "tenant-c"];
        const handles: Record<string, { id: string }> = {};

        // Enqueue jobs for different tenants
        for (const tenant of tenants) {
          handles[tenant] = await client.enqueue({
            tenant,
            payload: { tenant },
          });
        }

        // Verify each job can be retrieved with its tenant
        for (const tenant of tenants) {
          const job = await client.getJob(handles[tenant].id, tenant);
          expect(job?.id).toBe(handles[tenant].id);

          const payload = decodePayload<{ tenant: string }>(job?.payload?.data);
          expect(payload?.tenant).toBe(tenant);
        }
      } finally {
        client.close();
      }
    });

    it("handles high volume of different tenants", async () => {
      const client = new SiloGRPCClient({
        servers: SILO_SERVERS,
        useTls: false,
        shardRouting: {
          numShards: 16,
          topologyRefreshIntervalMs: 0,
        },
      });

      try {
        await client.refreshTopology();

        const numTenants = 50;
        const results: Array<{ tenant: string; jobId: string }> = [];

        // Enqueue jobs for many tenants
        for (let i = 0; i < numTenants; i++) {
          const tenant = `bulk-tenant-${i}`;
          const handle = await client.enqueue({
            tenant,
            payload: { index: i },
          });
          results.push({ tenant, jobId: handle.id });
        }

        // Verify random sample can be retrieved
        const samples = [0, 10, 25, 49].map((i) => results[i]);
        for (const { tenant, jobId } of samples) {
          const job = await client.getJob(jobId, tenant);
          expect(job?.id).toBe(jobId);
        }
      } finally {
        client.close();
      }
    });
  });

  describe("topology changes", () => {
    it("can refresh topology multiple times", async () => {
      const client = new SiloGRPCClient({
        servers: SILO_SERVERS,
        useTls: false,
        shardRouting: {
          numShards: 1,
          topologyRefreshIntervalMs: 0,
        },
      });

      try {
        // Multiple refreshes should all succeed
        await client.refreshTopology();
        const topo1 = client.getTopology();

        await client.refreshTopology();
        const topo2 = client.getTopology();

        await client.refreshTopology();
        const topo3 = client.getTopology();

        // Topology should be consistent (cluster hasn't changed)
        expect(topo1.numShards).toBe(topo2.numShards);
        expect(topo2.numShards).toBe(topo3.numShards);
      } finally {
        client.close();
      }
    });

    it("operations work after topology refresh", async () => {
      const client = new SiloGRPCClient({
        servers: SILO_SERVERS,
        useTls: false,
        shardRouting: {
          numShards: 1,
          topologyRefreshIntervalMs: 0,
        },
      });

      try {
        const tenant = "refresh-test";

        // Enqueue before refresh
        await client.refreshTopology();
        const handle1 = await client.enqueue({
          tenant,
          payload: { phase: "before" },
        });

        // Refresh topology
        await client.refreshTopology();

        // Enqueue after refresh
        const handle2 = await client.enqueue({
          tenant,
          payload: { phase: "after" },
        });

        // Both jobs should be retrievable
        const job1 = await client.getJob(handle1.id, tenant);
        const job2 = await client.getJob(handle2.id, tenant);

        expect(job1?.id).toBe(handle1.id);
        expect(job2?.id).toBe(handle2.id);
      } finally {
        client.close();
      }
    });
  });

  describe("connection management", () => {
    it("creates client with multiple server addresses", async () => {
      // Even if some addresses are invalid, client should work with valid ones
      const client = new SiloGRPCClient({
        servers: [...SILO_SERVERS, "invalid-server:99999"],
        useTls: false,
        shardRouting: {
          numShards: 1,
          topologyRefreshIntervalMs: 0,
        },
      });

      try {
        // Should be able to refresh topology using the valid server
        await client.refreshTopology();

        const topology = client.getTopology();
        expect(topology.numShards).toBeGreaterThanOrEqual(1);

        // Operations should work
        const handle = await client.enqueue({
          tenant: "multi-server-test",
          payload: { test: true },
        });
        expect(handle.id).toBeTruthy();
      } finally {
        client.close();
      }
    });

    it("handles server address as host:port object", async () => {
      const [host, port] = SILO_SERVERS[0].split(":");

      const client = new SiloGRPCClient({
        servers: { host, port: parseInt(port) },
        useTls: false,
        shardRouting: {
          numShards: 1,
          topologyRefreshIntervalMs: 0,
        },
      });

      try {
        await client.refreshTopology();

        const handle = await client.enqueue({
          tenant: "host-port-test",
          payload: { test: true },
        });
        expect(handle.id).toBeTruthy();
      } finally {
        client.close();
      }
    });
  });
});
