import { bench, describe } from "vitest";
import { SiloGRPCClient } from "../src/client";
import { SiloWorker, type TaskHandler } from "../src/worker";

const SILO_SERVERS = (
  process.env.SILO_SERVERS ||
  process.env.SILO_SERVER ||
  "localhost:7450"
).split(",");

const DEFAULT_TENANT = "test-tenant";
const DEFAULT_TASK_GROUP = "bench-task-group";

/**
 * Wait until a condition becomes true, polling at intervals.
 */
async function waitFor(
  condition: () => boolean | Promise<boolean>,
  options?: { timeout?: number; interval?: number },
): Promise<void> {
  const timeout = options?.timeout ?? 10000;
  const interval = options?.interval ?? 10;
  const start = Date.now();
  while (!(await condition())) {
    if (Date.now() - start > timeout) {
      throw new Error(`waitFor timed out after ${timeout}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, interval));
  }
}

// Create client at module level so it's available during warmup
const client = new SiloGRPCClient({
  servers: SILO_SERVERS,
  useTls: false,
  shardRouting: { topologyRefreshIntervalMs: 0 },
});

// Track whether topology has been fetched
let topologyReady = false;

async function ensureReady(): Promise<void> {
  if (!topologyReady) {
    await client.refreshTopology();
    topologyReady = true;
  }
}

describe("enqueue-to-pickup latency", () => {
  bench(
    "short polling (pollIntervalMs=50)",
    async () => {
      await ensureReady();
      await client.resetShards();

      const delays: number[] = [];
      const handler: TaskHandler<{ enqueueTime: number }> = async (ctx) => {
        delays.push(performance.now() - ctx.task.payload.enqueueTime);
        return { type: "success", result: {} };
      };

      const worker = new SiloWorker<{ enqueueTime: number }>({
        client,
        workerId: `bench-worker-${Date.now()}-${Math.random().toString(36).slice(2)}`,
        handler,
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        pollIntervalMs: 50,
        heartbeatIntervalMs: 1000,
        maxConcurrentTasks: 1,
      });
      worker.start();

      for (let i = 0; i < 10; i++) {
        await client.enqueue({
          tenant: DEFAULT_TENANT,
          taskGroup: DEFAULT_TASK_GROUP,
          payload: { enqueueTime: performance.now() },
          priority: 1,
        });
        await waitFor(() => delays.length > i);
      }

      await worker.stop();
    },
    { iterations: 3, time: 0, warmupIterations: 0 },
  );

  bench(
    "long polling",
    async () => {
      await ensureReady();
      await client.resetShards();

      const delays: number[] = [];
      const handler: TaskHandler<{ enqueueTime: number }> = async (ctx) => {
        delays.push(performance.now() - ctx.task.payload.enqueueTime);
        return { type: "success", result: {} };
      };

      const worker = new SiloWorker<{ enqueueTime: number }>({
        client,
        workerId: `bench-worker-${Date.now()}-${Math.random().toString(36).slice(2)}`,
        handler,
        tenant: DEFAULT_TENANT,
        taskGroup: DEFAULT_TASK_GROUP,
        pollIntervalMs: 50,
        heartbeatIntervalMs: 1000,
        maxConcurrentTasks: 10,
        concurrentPollers: client.serverCount,
      });
      worker.startLongPoll(30000);

      for (let i = 0; i < 10; i++) {
        await client.enqueue({
          tenant: DEFAULT_TENANT,
          taskGroup: DEFAULT_TASK_GROUP,
          payload: { enqueueTime: performance.now() },
          priority: 1,
        });
        await waitFor(() => delays.length > i);
      }

      await worker.stop();
    },
    { iterations: 3, time: 0, warmupIterations: 0 },
  );
});
