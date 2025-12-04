import { describe, it, expect, beforeAll, afterAll } from "vitest";
import {
  SiloGRPCClient,
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

      const jobId = await client.enqueue({
        tenant,
        payload,
        priority: 10,
      });

      expect(jobId).toBeTruthy();

      // Should be able to retrieve the job using the same tenant
      const job = await client.getJob(jobId, tenant);
      expect(job).toBeDefined();
      expect(job?.id).toBe(jobId);
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

      const jobId = await client.enqueue({
        tenant,
        payload,
        priority: 10,
        metadata: { source: "integration-test" },
      });

      expect(jobId).toBeTruthy();
      expect(typeof jobId).toBe("string");

      // Retrieve the job
      const job = await client.getJob(jobId, tenant);
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
      const tenant = "custom-id-tenant";

      const jobId = await client.enqueue({
        tenant,
        id: customId,
        payload: { data: "test" },
      });

      expect(jobId).toBe(customId);

      const job = await client.getJob(customId, tenant);
      expect(job?.id).toBe(customId);
    });

    it("enqueues a job with retry policy", async () => {
      const tenant = "retry-tenant";

      const jobId = await client.enqueue({
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

      expect(jobId).toBeTruthy();
      const job = await client.getJob(jobId, tenant);
      expect(job?.retryPolicy?.retryCount).toBe(3);
      expect(job?.retryPolicy?.backoffFactor).toBe(2.0);
    });

    it("enqueues a job with concurrency limits", async () => {
      const tenant = "concurrency-tenant";

      const jobId = await client.enqueue({
        tenant,
        payload: { data: "concurrency-test" },
        limits: [{ type: "concurrency", key: "user:123", maxConcurrency: 5 }],
      });

      expect(jobId).toBeTruthy();
      const job = await client.getJob(jobId, tenant);
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

      const jobId = await client.enqueue({
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

      expect(jobId).toBeTruthy();
      const job = await client.getJob(jobId, tenant);
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

      const jobId = await client.enqueue({
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

      expect(jobId).toBeTruthy();
      const job = await client.getJob(jobId, tenant);
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

      const jobId = await client.enqueue({
        tenant,
        id: uniqueJobId,
        payload,
        priority: 1,
      });

      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const tasks = await client.leaseTasks({
          workerId: `test-worker-lease-${Date.now()}`,
          maxTasks: 50,
          tenant,
        });
        task = tasks.find((t) => t.jobId === jobId);
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

      const jobId = await client.enqueue({
        tenant,
        id: uniqueJobId,
        payload: { action: "fail-test" },
        priority: 1,
      });

      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const tasks = await client.leaseTasks({
          workerId: `test-worker-fail-${Date.now()}`,
          maxTasks: 50,
          tenant,
        });
        task = tasks.find((t) => t.jobId === jobId);
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

      const jobId = await client.enqueue({
        tenant,
        payload: { action: "heartbeat-test" },
        priority: 1,
      });

      const tasks = await client.leaseTasks({
        workerId: "test-worker-3",
        maxTasks: 10,
        tenant,
      });

      const task = tasks.find((t) => t.jobId === jobId);
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

      const jobId = await client.enqueue({
        tenant,
        id: uniqueJobId,
        payload: { action: "delete-test" },
        priority: 1,
      });

      const job = await client.getJob(jobId, tenant);
      expect(job?.id).toBe(jobId);

      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const tasks = await client.leaseTasks({
          workerId: `delete-test-worker-${Date.now()}`,
          maxTasks: 50,
          tenant,
        });
        task = tasks.find((t) => t.jobId === jobId);
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

      await client.deleteJob(jobId, tenant);
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

      const result = await client.query("SELECT * FROM jobs", tenant);
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
        const jobId = await client.enqueue({
          tenant: "wrong-shard-test",
          payload: { test: true },
        });
        expect(jobId).toBeTruthy();
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
        const jobId = await client.enqueue({
          tenant: "retry-test-tenant",
          payload: { data: "retry test" },
        });
        expect(jobId).toBeTruthy();

        const job = await client.getJob(jobId, "retry-test-tenant");
        expect(job?.id).toBe(jobId);
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
        const jobIds: Record<string, string> = {};

        // Enqueue jobs for different tenants
        for (const tenant of tenants) {
          jobIds[tenant] = await client.enqueue({
            tenant,
            payload: { tenant },
          });
        }

        // Verify each job can be retrieved with its tenant
        for (const tenant of tenants) {
          const job = await client.getJob(jobIds[tenant], tenant);
          expect(job?.id).toBe(jobIds[tenant]);

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
          const jobId = await client.enqueue({
            tenant,
            payload: { index: i },
          });
          results.push({ tenant, jobId });
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
        const jobId1 = await client.enqueue({
          tenant,
          payload: { phase: "before" },
        });

        // Refresh topology
        await client.refreshTopology();

        // Enqueue after refresh
        const jobId2 = await client.enqueue({
          tenant,
          payload: { phase: "after" },
        });

        // Both jobs should be retrievable
        const job1 = await client.getJob(jobId1, tenant);
        const job2 = await client.getJob(jobId2, tenant);

        expect(job1?.id).toBe(jobId1);
        expect(job2?.id).toBe(jobId2);
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
        const jobId = await client.enqueue({
          tenant: "multi-server-test",
          payload: { test: true },
        });
        expect(jobId).toBeTruthy();
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

        const jobId = await client.enqueue({
          tenant: "host-port-test",
          payload: { test: true },
        });
        expect(jobId).toBeTruthy();
      } finally {
        client.close();
      }
    });
  });
});
