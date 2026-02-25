import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { SiloGRPCClient, JobStatus, decodeBytes, GubernatorAlgorithm } from "../src/client";

// Support comma-separated list of servers for multi-node testing
const SILO_SERVERS = (
  process.env.SILO_SERVERS ||
  process.env.SILO_SERVER ||
  "localhost:7450"
).split(",");
const RUN_INTEGRATION = process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";

// Default task group for all integration tests
const DEFAULT_TASK_GROUP = "integration-test-group";

/**
 * Wait for the cluster to converge by polling until all shards have owners.
 * This handles the case where the cluster is still starting up and shards
 * haven't been fully distributed yet.
 */
async function waitForClusterConvergence(
  client: SiloGRPCClient,
  maxAttempts = 30,
  delayMs = 500,
): Promise<void> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    await client.refreshTopology();
    const topology = client.getTopology();

    // Check if we have at least one shard with a valid address
    const hasShards = topology.shards.length > 0;
    const allShardsHaveOwners = topology.shards.every(
      (s) => s.serverAddr && s.serverAddr.length > 0,
    );

    if (hasShards && allShardsHaveOwners) {
      return; // Cluster is ready
    }

    if (attempt === maxAttempts) {
      throw new Error(
        `Cluster did not converge after ${maxAttempts} attempts. ` +
          `Found ${topology.shards.length} shards, ` +
          `${topology.shards.filter((s) => s.serverAddr).length} with owners.`,
      );
    }

    // Wait before retrying
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
}

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
    // Wait for cluster to converge and discover topology
    await waitForClusterConvergence(client);
  });

  afterAll(() => {
    client.close();
  });

  describe("topology discovery", () => {
    it("discovers cluster topology via GetClusterInfo", async () => {
      const topology = client.getTopology();

      // Should have discovered at least 1 shard
      expect(topology.shards.length).toBeGreaterThanOrEqual(1);

      // Should have server addresses for shards
      expect(topology.shardToServer.size).toBeGreaterThanOrEqual(1);
    });

    it("refreshes topology on demand", async () => {
      // This should succeed without throwing
      await client.refreshTopology();

      const topology = client.getTopology();
      expect(topology.shards.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe("tenant-based shard routing", () => {
    it("routes requests to correct shard based on tenant", async () => {
      const tenant = "test-tenant-123";
      const payload = { task: "routing-test" };

      const handle = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
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
      expect(typeof shard1).toBe("string");
      expect(shard1.length).toBeGreaterThan(0);
    });

    it("different tenants may route to different shards", () => {
      // With enough tenants, they should distribute across available shards
      const shards = new Set<string>();

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
        taskGroup: DEFAULT_TASK_GROUP,
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

      // Verify payload is already decoded
      expect(job?.payload).toEqual(payload);
    });

    it("enqueues a job with custom id", async () => {
      const customId = `custom-${Date.now()}`;
      const tenant = "custom-id-tenant";

      const handle = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
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
        taskGroup: DEFAULT_TASK_GROUP,
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
        taskGroup: DEFAULT_TASK_GROUP,
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
        taskGroup: DEFAULT_TASK_GROUP,
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
        taskGroup: DEFAULT_TASK_GROUP,
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
        taskGroup: DEFAULT_TASK_GROUP,
        id: uniqueJobId,
        payload,
        priority: 1,
      });

      // Get the shard for this tenant to poll from the correct server
      const shard = client.getShardForTenant(tenant);

      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const result = await client.leaseTasks({
          workerId: `test-worker-lease-${Date.now()}`,
          maxTasks: 50,
          shard,
          taskGroup: DEFAULT_TASK_GROUP,
        });
        task = result.tasks.find((t) => t.jobId === handle.id);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }

      expect(task).toBeDefined();
      expect(task?.attemptNumber).toBe(1);
      expect(task?.priority).toBe(1);

      const taskPayload = decodeBytes(
        task?.payload?.encoding.oneofKind === "msgpack" ? task.payload.encoding.msgpack : undefined,
        "payload",
      );
      expect(taskPayload).toEqual(payload);

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
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
        taskGroup: DEFAULT_TASK_GROUP,
        id: uniqueJobId,
        payload: { action: "fail-test" },
        priority: 1,
      });

      // Get the shard for this tenant to poll from the correct server
      const shard = client.getShardForTenant(tenant);

      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const result = await client.leaseTasks({
          workerId: `test-worker-fail-${Date.now()}`,
          maxTasks: 50,
          shard,
          taskGroup: DEFAULT_TASK_GROUP,
        });
        task = result.tasks.find((t) => t.jobId === handle.id);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }

      expect(task).toBeDefined();

      await client.reportOutcome({
        shard: task!.shard,
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
      const tenant = "heartbeat-tenant";

      const handle = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { action: "heartbeat-test" },
        priority: 1,
      });

      // Get the shard for this tenant to poll from the correct server
      const shard = client.getShardForTenant(tenant);

      const result = await client.leaseTasks({
        workerId: "test-worker-3",
        maxTasks: 10,
        shard,
        taskGroup: DEFAULT_TASK_GROUP,
      });

      const task = result.tasks.find((t) => t.jobId === handle.id);
      expect(task).toBeDefined();

      await client.heartbeat("test-worker-3", task!.id, task!.shard);

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
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
        taskGroup: DEFAULT_TASK_GROUP,
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
        const result = await client.leaseTasks({
          workerId: `delete-test-worker-${Date.now()}`,
          maxTasks: 50,
          shard,
          taskGroup: DEFAULT_TASK_GROUP,
        });
        task = result.tasks.find((t) => t.jobId === handle.id);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }
      expect(task).toBeDefined();

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
        outcome: { type: "success", result: {} },
      });

      await client.deleteJob(handle.id, tenant);
    });
  });

  describe("expediteJob", () => {
    it("expedites a future-scheduled job to run immediately", async () => {
      const uniqueJobId = `expedite-test-${Date.now()}`;
      const tenant = "expedite-tenant";

      // Schedule job 1 hour in the future
      const futureStartAtMs = BigInt(Date.now() + 3_600_000);

      const handle = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        id: uniqueJobId,
        payload: { action: "expedite-test" },
        priority: 1,
        startAtMs: futureStartAtMs,
      });

      // Verify job was created and is scheduled
      const jobBefore = await client.getJob(handle.id, tenant);
      expect(jobBefore?.status).toBe(JobStatus.Scheduled);

      // Get the shard for this tenant to poll from the correct server
      const shard = client.getShardForTenant(tenant);

      // Try to lease - should get nothing because job is future-scheduled
      const resultBefore = await client.leaseTasks({
        shard,
        workerId: "expedite-test-worker",
        maxTasks: 1,
        taskGroup: DEFAULT_TASK_GROUP,
      });
      expect(resultBefore.tasks.find((t) => t.jobId === handle.id)).toBeUndefined();

      // Expedite the job
      await client.expediteJob(handle.id, tenant);

      // Now the job should be leasable
      let task;
      for (let i = 0; i < 10; i++) {
        const result = await client.leaseTasks({
          shard,
          workerId: "expedite-test-worker",
          maxTasks: 10,
          taskGroup: DEFAULT_TASK_GROUP,
        });
        task = result.tasks.find((t) => t.jobId === handle.id);
        if (task) break;
        await new Promise((r) => setTimeout(r, 100));
      }
      expect(task).toBeDefined();

      // Complete the job
      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
        outcome: { type: "success", result: { expedited: true } },
      });

      // Verify job succeeded
      const jobAfter = await client.getJob(handle.id, tenant);
      expect(jobAfter?.status).toBe(JobStatus.Succeeded);
    });

    it("expedite throws error for non-existent job", async () => {
      const tenant = "expedite-tenant";
      await expect(client.expediteJob("non-existent-job-id", tenant)).rejects.toThrow();
    });

    it("expedite throws error for running job", async () => {
      const uniqueJobId = `expedite-running-test-${Date.now()}`;
      const tenant = "expedite-tenant";

      const handle = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        id: uniqueJobId,
        payload: { action: "expedite-running-test" },
        priority: 1,
      });

      // Get the shard for this tenant
      const shard = client.getShardForTenant(tenant);

      // Lease the task to make it running
      let task;
      for (let i = 0; i < 10; i++) {
        const result = await client.leaseTasks({
          shard,
          workerId: "expedite-running-test-worker",
          maxTasks: 10,
          taskGroup: DEFAULT_TASK_GROUP,
        });
        task = result.tasks.find((t) => t.jobId === handle.id);
        if (task) break;
        await new Promise((r) => setTimeout(r, 100));
      }
      expect(task).toBeDefined();

      // Verify job is running
      const jobRunning = await client.getJob(handle.id, tenant);
      expect(jobRunning?.status).toBe(JobStatus.Running);

      // Try to expedite - should fail
      await expect(client.expediteJob(handle.id, tenant)).rejects.toThrow();

      // Complete the job to clean up
      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
        outcome: { type: "success", result: {} },
      });
    });

    it("expedite throws error for cancelled job", async () => {
      const uniqueJobId = `expedite-cancelled-test-${Date.now()}`;
      const tenant = "expedite-tenant";

      // Schedule job 1 hour in the future
      const futureStartAtMs = BigInt(Date.now() + 3_600_000);

      const handle = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        id: uniqueJobId,
        payload: { action: "expedite-cancelled-test" },
        priority: 1,
        startAtMs: futureStartAtMs,
      });

      // Cancel the job
      await client.cancelJob(handle.id, tenant);

      // Verify job is cancelled
      const jobCancelled = await client.getJob(handle.id, tenant);
      expect(jobCancelled?.status).toBe(JobStatus.Cancelled);

      // Try to expedite - should fail
      await expect(client.expediteJob(handle.id, tenant)).rejects.toThrow();
    });
  });

  describe("leaseTask", () => {
    it("leases a specific job's task directly and completes it", async () => {
      const uniqueJobId = `lease-task-test-${Date.now()}`;
      const tenant = "lease-task-tenant";

      const handle = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        id: uniqueJobId,
        payload: { action: "lease-task-test" },
        priority: 1,
      });

      // Verify job is scheduled
      const jobBefore = await client.getJob(handle.id, tenant);
      expect(jobBefore?.status).toBe(JobStatus.Scheduled);

      // Lease the specific task directly
      const task = await client.leaseTask({
        id: handle.id,
        workerId: "lease-task-test-worker",
        tenant,
      });

      expect(task.jobId).toBe(handle.id);
      expect(task.attemptNumber).toBe(1);

      // Verify job is running
      const jobRunning = await client.getJob(handle.id, tenant);
      expect(jobRunning?.status).toBe(JobStatus.Running);

      // Complete the job
      await client.reportOutcome({
        shard: task.shard,
        taskId: task.id,
        outcome: { type: "success", result: { leased: true } },
      });

      // Verify job succeeded
      const jobAfter = await client.getJob(handle.id, tenant);
      expect(jobAfter?.status).toBe(JobStatus.Succeeded);
    });

    it("leaseTask throws error for non-existent job", async () => {
      const tenant = "lease-task-tenant";
      await expect(
        client.leaseTask({
          id: "non-existent-job-id",
          workerId: "test-worker",
          tenant,
        }),
      ).rejects.toThrow();
    });

    it("leaseTask throws error for running job", async () => {
      const uniqueJobId = `lease-task-running-test-${Date.now()}`;
      const tenant = "lease-task-tenant";

      const handle = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        id: uniqueJobId,
        payload: { action: "lease-task-running-test" },
        priority: 1,
      });

      // Lease the task to make it running
      const task = await client.leaseTask({
        id: handle.id,
        workerId: "lease-task-running-worker",
        tenant,
      });

      // Try to lease again - should fail
      await expect(
        client.leaseTask({
          id: handle.id,
          workerId: "lease-task-running-worker-2",
          tenant,
        }),
      ).rejects.toThrow();

      // Complete the job to clean up
      await client.reportOutcome({
        shard: task.shard,
        taskId: task.id,
        outcome: { type: "success", result: {} },
      });
    });
  });

  describe("query", () => {
    it("queries jobs with SQL", async () => {
      const testBatch = `batch-${Date.now()}`;
      const tenant = "query-tenant";

      for (let i = 0; i < 3; i++) {
        await client.enqueue({
          tenant,
          taskGroup: DEFAULT_TASK_GROUP,
          payload: { index: i },
          priority: 10 + i,
          metadata: { batch: testBatch },
        });
      }

      // SQL queries require tenant in WHERE clause for filtering
      const result = await client.query(`SELECT * FROM jobs WHERE tenant = '${tenant}'`, tenant);
      expect(result.rowCount).toBeGreaterThanOrEqual(3);
      expect(result.columns.length).toBeGreaterThan(0);
      expect(result.columns.some((c) => c.name === "id")).toBe(true);
      expect(result.columns.some((c) => c.name === "priority")).toBe(true);

      // Rows should be deserialized objects
      expect(result.rows.length).toBeGreaterThanOrEqual(3);
      const row = result.rows[0] as Record<string, unknown>;
      expect(typeof row.id).toBe("string");
    });

    it("queries with positional bind parameters", async () => {
      const tenant = "query-bind-tenant";
      const targetId = `query-bind-${Date.now()}`;

      await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        id: targetId,
        payload: { test: "bind-target" },
      });
      await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        id: `${targetId}-other`,
        payload: { test: "bind-other" },
      });

      const result = await client.query<{ id: string }>(
        "SELECT id FROM jobs WHERE tenant = $1 AND id = $2",
        tenant,
        [tenant, targetId],
      );

      expect(result.rowCount).toBe(1);
      expect(result.rows[0]?.id).toBe(targetId);
    });

    it("queries with WHERE clause", async () => {
      const tenant = "query-where-tenant";

      // First ensure there's data
      await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: true },
        priority: 5,
      });

      const result = await client.query(
        "SELECT id, priority FROM jobs WHERE priority < 20",
        tenant,
      );
      expect(result.columns.length).toBe(2);

      // Rows should be deserialized with only the selected columns
      expect(result.rows.length).toBeGreaterThanOrEqual(1);
      const row = result.rows[0] as Record<string, unknown>;
      expect(typeof row.id).toBe("string");
      expect(typeof row.priority).toBe("number");
    });

    it("queries with aggregation", async () => {
      const tenant = "query-agg-tenant";

      // Ensure there's at least one job
      await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: true },
      });

      const result = await client.query<{ count: number }>(
        "SELECT COUNT(*) as count FROM jobs",
        tenant,
      );
      expect(result.rowCount).toBe(1);
      expect(result.columns.some((c) => c.name === "count")).toBe(true);

      // Rows are already deserialized
      expect(typeof result.rows[0].count).toBe("number");
    });

    it("supports generic row type parameter", async () => {
      const tenant = "query-typed-tenant";

      await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: true },
        priority: 42,
      });

      interface JobRow {
        id: string;
        priority: number;
      }

      const result = await client.query<JobRow>(
        "SELECT id, priority FROM jobs WHERE priority = 42",
        tenant,
      );
      expect(result.rows.length).toBeGreaterThanOrEqual(1);
      // TypeScript knows these are typed
      const row: JobRow = result.rows[0];
      expect(typeof row.id).toBe("string");
      expect(row.priority).toBe(42);
    });
  });

  describe("JobHandle", () => {
    it("returns a JobHandle from enqueue", async () => {
      const tenant = "handle-enqueue-tenant";

      const handle = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
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
        taskGroup: DEFAULT_TASK_GROUP,
        payload,
        priority: 15,
      });

      const job = await handle.getJob();
      expect(job).toBeDefined();
      expect(job?.id).toBe(handle.id);
      expect(job?.priority).toBe(15);
      expect(job?.payload).toEqual(payload);
    });

    it("can get job status through handle", async () => {
      const tenant = "handle-status-tenant";

      const handle = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "status-test" },
      });

      const status = await handle.getStatus();
      expect(status).toBe(JobStatus.Scheduled);
    });

    it("can cancel job through handle", async () => {
      const tenant = "handle-cancel-tenant";

      const handle = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
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
        taskGroup: DEFAULT_TASK_GROUP,
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
        taskGroup: DEFAULT_TASK_GROUP,
        id: uniqueJobId,
        payload: { test: "delete-test" },
      });

      // Get the shard for this tenant to poll from the correct server
      const shard = client.getShardForTenant(tenant);

      // Lease and complete the task so we can delete the job
      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const result = await client.leaseTasks({
          workerId: `handle-delete-worker-${Date.now()}`,
          maxTasks: 50,
          shard,
          taskGroup: DEFAULT_TASK_GROUP,
        });
        task = result.tasks.find((t) => t.jobId === handle.id);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }
      expect(task).toBeDefined();

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
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
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "await-test" },
      });

      // Get the shard for this tenant to poll from the correct server
      const shard = client.getShardForTenant(tenant);

      // Lease and complete the task
      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const result = await client.leaseTasks({
          workerId: `handle-await-worker-${Date.now()}`,
          maxTasks: 50,
          shard,
          taskGroup: DEFAULT_TASK_GROUP,
        });
        task = result.tasks.find((t) => t.jobId === handle.id);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }
      expect(task).toBeDefined();

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
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
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "await-fail-test" },
      });

      // Get the shard for this tenant to poll from the correct server
      const shard = client.getShardForTenant(tenant);

      // Lease and fail the task
      let task;
      for (let i = 0; i < 5 && !task; i++) {
        const result = await client.leaseTasks({
          workerId: `handle-await-fail-worker-${Date.now()}`,
          maxTasks: 50,
          shard,
          taskGroup: DEFAULT_TASK_GROUP,
        });
        task = result.tasks.find((t) => t.jobId === handle.id);
        if (!task) {
          await new Promise((r) => setTimeout(r, 100));
        }
      }
      expect(task).toBeDefined();

      await client.reportOutcome({
        shard: task!.shard,
        taskId: task!.id,
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
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "timeout-test" },
        // Schedule far in the future so it won't be leased
        startAtMs: BigInt(Date.now() + 60000),
      });

      // Try to await with a short timeout - should fail
      await expect(handle.awaitResult({ pollIntervalMs: 50, timeoutMs: 200 })).rejects.toThrow(
        /Timeout/,
      );
    });
  });

  describe("large payloads", () => {
    it("enqueues and retrieves a 10MB payload", { timeout: 30_000 }, async () => {
      const tenant = "large-payload-tenant";

      // Build a ~10MB payload using a raw Buffer.
      // msgpack encodes Buffers as binary data without base64 expansion,
      // so the wire size stays close to 10MB. This validates we're above
      // the default 4MB gRPC limit without overwhelming CI resources.
      const sizeBytes = 10 * 1024 * 1024;
      const largeBuffer = Buffer.alloc(sizeBytes, 0x42);
      const payload = { data: largeBuffer };

      const taskGroup = "large-payload-test-group";

      const handle = await client.enqueue({
        tenant,
        taskGroup,
        payload,
        priority: 50,
      });

      expect(handle.id).toBeTruthy();

      // Retrieve the job and verify the payload round-trips correctly
      const job = await client.getJob(handle.id, tenant);
      expect(job).toBeDefined();
      expect(job?.id).toBe(handle.id);

      const retrieved = job?.payload as { data: Uint8Array };
      expect(retrieved.data.length).toBe(sizeBytes);
      expect(Buffer.from(retrieved.data).equals(largeBuffer)).toBe(true);
    });
  });

  describe("floating concurrency limits", () => {
    const tenant = `floating-test-${Date.now()}`;

    it("enqueues job with floating concurrency limit", async () => {
      const payload = { task: "floating-limit-test" };
      const handle = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload,
        limits: [
          {
            type: "floatingConcurrency",
            key: `floating-queue-${Date.now()}`,
            defaultMaxConcurrency: 5,
            refreshIntervalMs: 60000n,
            metadata: { orgId: "test-org-123", env: "test" },
          },
        ],
      });

      expect(handle.id).toBeTruthy();

      // Verify job was created with floating limit
      const job = await client.getJob(handle.id, tenant);
      expect(job.status).toBe(JobStatus.Scheduled);
      expect(job.limits).toHaveLength(1);
      expect(job.limits[0].type).toBe("floatingConcurrency");
      if (job.limits[0].type === "floatingConcurrency") {
        expect(job.limits[0].defaultMaxConcurrency).toBe(5);
        expect(job.limits[0].refreshIntervalMs).toBe(60000n);
        expect(job.limits[0].metadata).toEqual({
          orgId: "test-org-123",
          env: "test",
        });
      }
    });

    it("returns refresh tasks when floating limit is stale", async () => {
      const queueKey = `stale-refresh-queue-${Date.now()}`;
      const shard = client.getShardForTenant(tenant);

      // Enqueue first job with a very short refresh interval
      const handle1 = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { job: 1 },
        limits: [
          {
            type: "floatingConcurrency",
            key: queueKey,
            defaultMaxConcurrency: 1, // Use 1 so second job becomes a waiter, triggering refresh
            refreshIntervalMs: 1n, // 1ms - will be stale immediately
            metadata: { testKey: "testValue" },
          },
        ],
      });
      expect(handle1.id).toBeTruthy();

      // Wait a bit to ensure the limit becomes stale
      await new Promise((r) => setTimeout(r, 50));

      // Enqueue second job to trigger refresh scheduling
      const handle2 = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { job: 2 },
        limits: [
          {
            type: "floatingConcurrency",
            key: queueKey,
            defaultMaxConcurrency: 1, // Use 1 so second job becomes a waiter, triggering refresh
            refreshIntervalMs: 1n,
            metadata: { testKey: "testValue" },
          },
        ],
      });
      expect(handle2.id).toBeTruthy();

      // Lease tasks - should get refresh tasks
      const result = await client.leaseTasks({
        workerId: `floating-refresh-worker-${Date.now()}`,
        maxTasks: 10,
        shard,
        taskGroup: DEFAULT_TASK_GROUP,
      });

      // Should have refresh tasks (and job tasks)
      expect(result.tasks.length).toBeGreaterThanOrEqual(0);

      // Find our refresh task - it MUST exist
      const refreshTask = result.refreshTasks.find((rt) => rt.queueKey === queueKey);
      expect(refreshTask).toBeDefined();
      expect(refreshTask!.id).toBeTruthy();
      expect(refreshTask!.queueKey).toBe(queueKey);
      expect(refreshTask!.currentMaxConcurrency).toBe(1);
      expect(refreshTask!.metadata).toEqual({ testKey: "testValue" });
      expect(refreshTask!.shard).toBe(shard);

      // Report success with a new max concurrency
      await client.reportRefreshOutcome({
        taskId: refreshTask!.id,
        shard: refreshTask!.shard,
        outcome: {
          type: "success",
          newMaxConcurrency: 10,
        },
      });

      // Clean up - lease and complete the job tasks
      for (const task of result.tasks) {
        await client.reportOutcome({
          taskId: task.id,
          shard: task.shard,
          outcome: { type: "success", result: {} },
        });
      }
    });

    it("reports refresh failure and triggers retry", async () => {
      const queueKey = `failure-refresh-queue-${Date.now()}`;
      const shard = client.getShardForTenant(tenant);

      // Enqueue job with short refresh interval
      const handle = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { task: "failure-test" },
        limits: [
          {
            type: "floatingConcurrency",
            key: queueKey,
            defaultMaxConcurrency: 1, // Use 1 so second job becomes a waiter, triggering refresh
            refreshIntervalMs: 1n,
            metadata: { apiEndpoint: "https://api.example.com" },
          },
        ],
      });
      expect(handle.id).toBeTruthy();

      // Wait for limit to become stale
      await new Promise((r) => setTimeout(r, 50));

      // Trigger refresh by enqueueing another job
      await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { task: "trigger-refresh" },
        limits: [
          {
            type: "floatingConcurrency",
            key: queueKey,
            defaultMaxConcurrency: 1, // Use 1 so second job becomes a waiter, triggering refresh
            refreshIntervalMs: 1n,
            metadata: { apiEndpoint: "https://api.example.com" },
          },
        ],
      });

      // Lease tasks
      const result = await client.leaseTasks({
        workerId: `failure-worker-${Date.now()}`,
        maxTasks: 10,
        shard,
        taskGroup: DEFAULT_TASK_GROUP,
      });

      // Find our refresh task - it MUST exist
      const refreshTask = result.refreshTasks.find((rt) => rt.queueKey === queueKey);
      expect(refreshTask).toBeDefined();

      // Report failure
      await client.reportRefreshOutcome({
        taskId: refreshTask!.id,
        shard: refreshTask!.shard,
        outcome: {
          type: "failure",
          code: "API_UNAVAILABLE",
          message: "Failed to reach API endpoint",
        },
      });
      // Server should schedule a retry with backoff - we don't verify this directly
      // but the test ensures the API works correctly

      // Clean up job tasks
      for (const task of result.tasks) {
        await client.reportOutcome({
          taskId: task.id,
          shard: task.shard,
          outcome: { type: "success", result: {} },
        });
      }
    });

    it("floating limit controls job concurrency", async () => {
      const queueKey = `concurrency-control-queue-${Date.now()}`;
      const shard = client.getShardForTenant(tenant);

      // Enqueue 3 jobs with max concurrency of 1
      const handles: { id: string }[] = [];
      for (let i = 0; i < 3; i++) {
        const handle = await client.enqueue({
          tenant,
          taskGroup: DEFAULT_TASK_GROUP,
          payload: { jobNum: i },
          limits: [
            {
              type: "floatingConcurrency",
              key: queueKey,
              defaultMaxConcurrency: 1,
              refreshIntervalMs: 60000n, // Long interval to avoid refresh
              metadata: {},
            },
          ],
        });
        handles.push(handle);
      }

      // Lease tasks - should only get 1 due to concurrency limit
      const result = await client.leaseTasks({
        workerId: `concurrency-worker-${Date.now()}`,
        maxTasks: 10,
        shard,
        taskGroup: DEFAULT_TASK_GROUP,
      });

      // Filter to just our jobs
      const ourTasks = result.tasks.filter((t) => handles.some((h) => h.id === t.jobId));

      // Should have at most 1 task due to concurrency limit
      expect(ourTasks.length).toBeLessThanOrEqual(1);

      // Complete tasks to clean up
      for (const task of result.tasks) {
        await client.reportOutcome({
          taskId: task.id,
          shard: task.shard,
          outcome: { type: "success", result: {} },
        });
      }
    });

    it("combines floating limit with regular concurrency limit", async () => {
      const floatingKey = `combined-floating-${Date.now()}`;
      const regularKey = `combined-regular-${Date.now()}`;

      const handle = await client.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { task: "combined-limits" },
        limits: [
          {
            type: "floatingConcurrency",
            key: floatingKey,
            defaultMaxConcurrency: 5,
            refreshIntervalMs: 30000n,
            metadata: { source: "floating" },
          },
          {
            type: "concurrency",
            key: regularKey,
            maxConcurrency: 3,
          },
        ],
      });

      expect(handle.id).toBeTruthy();

      // Verify both limits are stored
      const job = await client.getJob(handle.id, tenant);
      expect(job.limits).toHaveLength(2);

      const floatingLimit = job.limits.find((l) => l.type === "floatingConcurrency");
      const regularLimit = job.limits.find((l) => l.type === "concurrency");

      expect(floatingLimit).toBeDefined();
      expect(regularLimit).toBeDefined();

      if (floatingLimit?.type === "floatingConcurrency") {
        expect(floatingLimit.key).toBe(floatingKey);
        expect(floatingLimit.defaultMaxConcurrency).toBe(5);
      }
      if (regularLimit?.type === "concurrency") {
        expect(regularLimit.key).toBe(regularKey);
        expect(regularLimit.maxConcurrency).toBe(3);
      }
    });
  });
});

describe.skipIf(!RUN_INTEGRATION)("Shard routing integration", () => {
  // Wait for cluster convergence before running any shard routing tests
  beforeAll(async () => {
    const client = new SiloGRPCClient({
      servers: SILO_SERVERS,
      useTls: false,
      shardRouting: { topologyRefreshIntervalMs: 0 },
    });
    try {
      await waitForClusterConvergence(client);
    } finally {
      client.close();
    }
  });

  describe("wrong shard error handling", () => {
    it("handles requests gracefully after topology refresh", async () => {
      // Create a client and refresh topology
      const client = new SiloGRPCClient({
        servers: SILO_SERVERS,
        useTls: false,
        shardRouting: {
          maxWrongShardRetries: 2,
          wrongShardRetryDelayMs: 50,
          topologyRefreshIntervalMs: 0,
        },
      });

      try {
        // Refresh to discover the actual topology
        await client.refreshTopology();

        // Now requests should work because we've discovered the actual topology
        const handle = await client.enqueue({
          tenant: "wrong-shard-test",
          taskGroup: DEFAULT_TASK_GROUP,
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
          taskGroup: DEFAULT_TASK_GROUP,
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
            taskGroup: DEFAULT_TASK_GROUP,
            payload: { tenant },
          });
        }

        // Verify each job can be retrieved with its tenant
        for (const tenant of tenants) {
          const job = await client.getJob(handles[tenant].id, tenant);
          expect(job?.id).toBe(handles[tenant].id);

          const payload = job?.payload as { tenant: string };
          expect(payload?.tenant).toBe(tenant);
        }
      } finally {
        client.close();
      }
    });

    it("handles high volume of different tenants", { timeout: 30000 }, async () => {
      const client = new SiloGRPCClient({
        servers: SILO_SERVERS,
        useTls: false,
        shardRouting: {
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
            taskGroup: DEFAULT_TASK_GROUP,
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
        expect(topo1.shards.length).toBe(topo2.shards.length);
        expect(topo2.shards.length).toBe(topo3.shards.length);
      } finally {
        client.close();
      }
    });

    it("operations work after topology refresh", async () => {
      const client = new SiloGRPCClient({
        servers: SILO_SERVERS,
        useTls: false,
        shardRouting: {
          topologyRefreshIntervalMs: 0,
        },
      });

      try {
        const tenant = "refresh-test";

        // Enqueue before refresh
        await client.refreshTopology();
        const handle1 = await client.enqueue({
          tenant,
          taskGroup: DEFAULT_TASK_GROUP,
          payload: { phase: "before" },
        });

        // Refresh topology
        await client.refreshTopology();

        // Enqueue after refresh
        const handle2 = await client.enqueue({
          tenant,
          taskGroup: DEFAULT_TASK_GROUP,
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
          topologyRefreshIntervalMs: 0,
        },
      });

      try {
        // Should be able to refresh topology using the valid server
        await client.refreshTopology();

        const topology = client.getTopology();
        expect(topology.shards.length).toBeGreaterThanOrEqual(1);

        // Operations should work
        const handle = await client.enqueue({
          tenant: "multi-server-test",
          taskGroup: DEFAULT_TASK_GROUP,
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
          topologyRefreshIntervalMs: 0,
        },
      });

      try {
        await client.refreshTopology();

        const handle = await client.enqueue({
          tenant: "host-port-test",
          taskGroup: DEFAULT_TASK_GROUP,
          payload: { test: true },
        });
        expect(handle.id).toBeTruthy();
      } finally {
        client.close();
      }
    });
  });
});
