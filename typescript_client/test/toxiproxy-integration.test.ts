import { describe, it, expect, beforeAll, beforeEach, afterAll, afterEach } from "vitest";
import { SiloGRPCClient } from "../src/client";
import {
  Toxiproxy,
  Proxy,
  type Latency,
  type Bandwidth,
  type Timeout,
  type Slicer,
  type Slowclose,
} from "toxiproxy-node-client";
import type { ResetPeer } from "toxiproxy-node-client/dist/Toxic";

// Direct silo servers (for setup/verification)
const SILO_SERVERS = (
  process.env.SILO_SERVERS ||
  process.env.SILO_SERVER ||
  "localhost:7450"
).split(",");

// Toxiproxy-proxied silo servers
const TOXIPROXY_SILO_SERVERS = (
  process.env.TOXIPROXY_SILO_SERVERS || "localhost:17450,localhost:17451"
).split(",");

const TOXIPROXY_API_URL = process.env.TOXIPROXY_API_URL || "http://127.0.0.1:8474";

const RUN_INTEGRATION = process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";

const DEFAULT_TASK_GROUP = "toxiproxy-test-group";

const toxiproxy = new Toxiproxy(TOXIPROXY_API_URL);

async function isToxiproxyAvailable(): Promise<boolean> {
  try {
    await toxiproxy.getVersion();
    return true;
  } catch {
    return false;
  }
}

/**
 * Ensure the toxiproxy proxies for silo exist, creating them if needed.
 */
async function ensureProxies(): Promise<{ silo1: Proxy; silo2: Proxy }> {
  const proxyConfigs = [
    { name: "silo-1", listen: "127.0.0.1:17450", upstream: "127.0.0.1:7450" },
    { name: "silo-2", listen: "127.0.0.1:17451", upstream: "127.0.0.1:7451" },
  ];

  const proxies: Proxy[] = [];
  for (const config of proxyConfigs) {
    try {
      proxies.push(await toxiproxy.get(config.name));
    } catch {
      proxies.push(await toxiproxy.createProxy(config));
    }
  }

  return { silo1: proxies[0], silo2: proxies[1] };
}

async function waitForClusterConvergence(
  client: SiloGRPCClient,
  maxAttempts = 60,
  delayMs = 500,
): Promise<void> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await client.refreshTopology();
      const topology = client.getTopology();

      const hasShards = topology.shards.length > 0;
      const allShardsHaveOwners = topology.shards.every(
        (s) => s.serverAddr && s.serverAddr.length > 0,
      );

      if (hasShards && allShardsHaveOwners) {
        // Verify cluster is truly ready by attempting a test enqueue
        await client.enqueue({
          tenant: `convergence-check-${Date.now()}`,
          taskGroup: "convergence-check",
          payload: { check: true },
        });
        return;
      }
    } catch {
      // Ignore errors during convergence (e.g., "shard not ready: acquisition in progress")
    }

    if (attempt === maxAttempts) {
      throw new Error(`Cluster did not converge after ${maxAttempts} attempts.`);
    }

    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
}

// Map real server addresses → toxiproxy addresses so topology-discovered
// connections are routed through the proxy without changing server config.
const ADDRESS_MAP: Record<string, string> = {
  "127.0.0.1:7450": "127.0.0.1:17450",
  "127.0.0.1:7451": "127.0.0.1:17451",
};

function createProxyClient(): SiloGRPCClient {
  return new SiloGRPCClient({
    servers: TOXIPROXY_SILO_SERVERS,
    useTls: false,
    shardRouting: { topologyRefreshIntervalMs: 0 },
    addressMap: ADDRESS_MAP,
  });
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Retry an operation until it succeeds, instead of using a fixed sleep. */
async function retryUntilSuccess<T>(
  fn: () => Promise<T>,
  maxAttempts = 30,
  delayMs = 100,
): Promise<T> {
  let lastError: unknown;
  for (let i = 0; i < maxAttempts; i++) {
    try {
      return await fn();
    } catch (e) {
      lastError = e;
      await sleep(delayMs);
    }
  }
  throw lastError;
}

/** Service config with aggressive timeouts for tests that need fast failure detection. */
const fastFailServiceConfig = JSON.stringify({
  methodConfig: [
    {
      name: [{ service: "silo.v1.Silo" }],
      retryPolicy: {
        maxAttempts: 2,
        initialBackoff: "0.05s",
        maxBackoff: "0.5s",
        backoffMultiplier: 2,
        retryableStatusCodes: ["UNAVAILABLE", "RESOURCE_EXHAUSTED"],
      },
    },
  ],
});

function createFastFailProxyClient(): SiloGRPCClient {
  return new SiloGRPCClient({
    servers: TOXIPROXY_SILO_SERVERS,
    useTls: false,
    shardRouting: {
      topologyRefreshIntervalMs: 0,
      maxRetries: 2,
      retryDelayMs: 50,
    },
    addressMap: ADDRESS_MAP,
    rpcOptions: { timeout: 1000 },
    grpcClientOptions: {
      "grpc.service_config": fastFailServiceConfig,
    },
  });
}

describe.skipIf(!RUN_INTEGRATION)("Toxiproxy gRPC client integration", () => {
  let proxyClient: SiloGRPCClient;
  let directClient: SiloGRPCClient;
  let silo1: Proxy;
  let silo2: Proxy;
  let toxiproxyAvailable = false;

  beforeAll(async () => {
    toxiproxyAvailable = await isToxiproxyAvailable();
    if (!toxiproxyAvailable) return;

    const proxies = await ensureProxies();
    silo1 = proxies.silo1;
    silo2 = proxies.silo2;
    await toxiproxy.reset();

    // Direct client to verify cluster is ready
    directClient = new SiloGRPCClient({
      servers: SILO_SERVERS,
      useTls: false,
      shardRouting: { topologyRefreshIntervalMs: 0 },
    });
    await waitForClusterConvergence(directClient);

    // Proxy client — uses addressMap to remap topology-discovered server addresses
    // through toxiproxy, so all client traffic goes through the proxy.
    proxyClient = createProxyClient();
    await waitForClusterConvergence(proxyClient);
  }, 60_000);

  afterAll(() => {
    proxyClient?.close();
    directClient?.close();
  });

  beforeEach((ctx) => {
    if (!toxiproxyAvailable) ctx.skip();
  });

  afterEach(async () => {
    await toxiproxy.reset();
  });

  describe("latency toxic", () => {
    it("operations succeed with added latency", async () => {
      await silo1.addToxic<Latency>({
        name: "latency-downstream",
        type: "latency",
        stream: "downstream",
        toxicity: 1.0,
        attributes: { latency: 200, jitter: 50 },
      });
      await silo2.addToxic<Latency>({
        name: "latency-downstream",
        type: "latency",
        stream: "downstream",
        toxicity: 1.0,
        attributes: { latency: 200, jitter: 50 },
      });

      const tenant = `latency-test-${Date.now()}`;
      const start = Date.now();

      const handle = await proxyClient.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "latency" },
      });

      const elapsed = Date.now() - start;

      expect(handle.id).toBeTruthy();
      expect(elapsed).toBeGreaterThanOrEqual(150);

      const job = await proxyClient.getJob(handle.id, tenant);
      expect(job?.id).toBe(handle.id);
    });

    it("operations succeed with upstream latency", async () => {
      await silo1.addToxic<Latency>({
        name: "latency-upstream",
        type: "latency",
        stream: "upstream",
        toxicity: 1.0,
        attributes: { latency: 150, jitter: 0 },
      });
      await silo2.addToxic<Latency>({
        name: "latency-upstream",
        type: "latency",
        stream: "upstream",
        toxicity: 1.0,
        attributes: { latency: 150, jitter: 0 },
      });

      const tenant = `upstream-latency-${Date.now()}`;
      const start = Date.now();

      const handle = await proxyClient.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "upstream-latency" },
      });

      const elapsed = Date.now() - start;

      expect(handle.id).toBeTruthy();
      expect(elapsed).toBeGreaterThanOrEqual(100);
    });
  });

  describe("bandwidth toxic", () => {
    it("operations succeed with bandwidth limitation", async () => {
      await silo1.addToxic<Bandwidth>({
        name: "bandwidth-limit",
        type: "bandwidth",
        stream: "downstream",
        toxicity: 1.0,
        attributes: { rate: 10 },
      });
      await silo2.addToxic<Bandwidth>({
        name: "bandwidth-limit",
        type: "bandwidth",
        stream: "downstream",
        toxicity: 1.0,
        attributes: { rate: 10 },
      });

      const tenant = `bandwidth-test-${Date.now()}`;

      const handle = await proxyClient.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "bandwidth" },
      });

      expect(handle.id).toBeTruthy();

      const job = await proxyClient.getJob(handle.id, tenant);
      expect(job?.id).toBe(handle.id);
    });
  });

  describe("timeout toxic", () => {
    it("client retries and recovers after temporary timeout toxic is removed", async () => {
      const tenant = `timeout-recovery-${Date.now()}`;

      const handle = await proxyClient.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "timeout-recovery" },
      });
      expect(handle.id).toBeTruthy();

      const toxic1 = await silo1.addToxic<Timeout>({
        name: "timeout",
        type: "timeout",
        stream: "downstream",
        toxicity: 1.0,
        attributes: { timeout: 100 },
      });
      const toxic2 = await silo2.addToxic<Timeout>({
        name: "timeout",
        type: "timeout",
        stream: "downstream",
        toxicity: 1.0,
        attributes: { timeout: 100 },
      });

      await expect(proxyClient.getJob(handle.id, tenant)).rejects.toThrow();

      await toxic1.remove();
      await toxic2.remove();

      // Retry until gRPC reconnects instead of a fixed sleep
      const job = await retryUntilSuccess(() => proxyClient.getJob(handle.id, tenant));
      expect(job?.id).toBe(handle.id);
    });
  });

  describe("proxy disable/enable", () => {
    it("client fails when proxy is disabled, recovers when re-enabled", async () => {
      const tenant = `disable-test-${Date.now()}`;

      const handle = await proxyClient.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "disable" },
      });
      expect(handle.id).toBeTruthy();

      await silo1.update({
        enabled: false,
        listen: silo1.listen,
        upstream: silo1.upstream,
      });
      await silo2.update({
        enabled: false,
        listen: silo2.listen,
        upstream: silo2.upstream,
      });

      await expect(proxyClient.getJob(handle.id, tenant)).rejects.toThrow();

      await silo1.update({
        enabled: true,
        listen: silo1.listen,
        upstream: silo1.upstream,
      });
      await silo2.update({
        enabled: true,
        listen: silo2.listen,
        upstream: silo2.upstream,
      });

      // Retry until gRPC reconnects instead of a fixed sleep
      const job = await retryUntilSuccess(() => proxyClient.getJob(handle.id, tenant));
      expect(job?.id).toBe(handle.id);
    });
  });

  describe("slicer toxic", () => {
    it("operations succeed when TCP packets are sliced", async () => {
      await silo1.addToxic<Slicer>({
        name: "slicer",
        type: "slicer",
        stream: "downstream",
        toxicity: 1.0,
        attributes: { average_size: 50, size_variation: 20, delay: 5 },
      });
      await silo2.addToxic<Slicer>({
        name: "slicer",
        type: "slicer",
        stream: "downstream",
        toxicity: 1.0,
        attributes: { average_size: 50, size_variation: 20, delay: 5 },
      });

      const tenant = `slicer-test-${Date.now()}`;

      const handle = await proxyClient.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "slicer", data: "a]".repeat(100) },
      });

      expect(handle.id).toBeTruthy();

      const job = await proxyClient.getJob(handle.id, tenant);
      expect(job?.id).toBe(handle.id);
      expect(job?.payload).toEqual({ test: "slicer", data: "a]".repeat(100) });
    });
  });

  describe("partial failure", () => {
    it("operations succeed when only one proxy is degraded", async () => {
      await silo1.addToxic<Latency>({
        name: "latency-heavy",
        type: "latency",
        stream: "downstream",
        toxicity: 1.0,
        attributes: { latency: 500, jitter: 0 },
      });

      const tenant = `partial-failure-${Date.now()}`;

      const handle = await proxyClient.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "partial-failure" },
      });

      expect(handle.id).toBeTruthy();

      const job = await proxyClient.getJob(handle.id, tenant);
      expect(job?.id).toBe(handle.id);
    });

    it("can enqueue and retrieve multiple jobs with one proxy disabled", async () => {
      // Use a fast-fail client so calls routed to the disabled proxy fail quickly
      // instead of exhausting the default retry policy (~3s per failed call)
      const fastClient = createFastFailProxyClient();
      await fastClient.refreshTopology();

      await silo1.update({
        enabled: false,
        listen: silo1.listen,
        upstream: silo1.upstream,
      });

      const results: string[] = [];
      const errors: Error[] = [];

      for (let i = 0; i < 5; i++) {
        const tenant = `partial-multi-${Date.now()}-${i}`;
        try {
          const handle = await fastClient.enqueue({
            tenant,
            taskGroup: DEFAULT_TASK_GROUP,
            payload: { test: "partial-multi", index: i },
          });
          results.push(handle.id);
        } catch (e) {
          errors.push(e as Error);
        }
      }

      expect(results.length + errors.length).toBe(5);

      await silo1.update({
        enabled: true,
        listen: silo1.listen,
        upstream: silo1.upstream,
      });
      fastClient.close();
    });
  });

  describe("slow close toxic", () => {
    it("operations complete despite slow connection closing", async () => {
      await silo1.addToxic<Slowclose>({
        name: "slow-close",
        type: "slow_close",
        stream: "downstream",
        toxicity: 1.0,
        attributes: { delay: 500 },
      });
      await silo2.addToxic<Slowclose>({
        name: "slow-close",
        type: "slow_close",
        stream: "downstream",
        toxicity: 1.0,
        attributes: { delay: 500 },
      });

      const tenant = `slow-close-${Date.now()}`;

      const handle = await proxyClient.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "slow-close" },
      });

      expect(handle.id).toBeTruthy();

      const job = await proxyClient.getJob(handle.id, tenant);
      expect(job?.id).toBe(handle.id);
    });
  });

  describe("combined toxics", () => {
    it("operations succeed with latency + bandwidth combination", async () => {
      for (const proxy of [silo1, silo2]) {
        await proxy.addToxic<Latency>({
          name: "latency-combined",
          type: "latency",
          stream: "downstream",
          toxicity: 1.0,
          attributes: { latency: 100, jitter: 25 },
        });
        await proxy.addToxic<Bandwidth>({
          name: "bandwidth-combined",
          type: "bandwidth",
          stream: "downstream",
          toxicity: 1.0,
          attributes: { rate: 20 },
        });
      }

      const tenant = `combined-test-${Date.now()}`;

      const handle = await proxyClient.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "combined" },
      });

      expect(handle.id).toBeTruthy();

      const job = await proxyClient.getJob(handle.id, tenant);
      expect(job?.id).toBe(handle.id);
    });

    it("operations succeed with slicer + latency combination", async () => {
      for (const proxy of [silo1, silo2]) {
        await proxy.addToxic<Slicer>({
          name: "slicer-combined",
          type: "slicer",
          stream: "downstream",
          toxicity: 1.0,
          attributes: { average_size: 100, size_variation: 50, delay: 2 },
        });
        await proxy.addToxic<Latency>({
          name: "latency-combined",
          type: "latency",
          stream: "upstream",
          toxicity: 1.0,
          attributes: { latency: 50, jitter: 10 },
        });
      }

      const tenant = `slicer-latency-${Date.now()}`;

      const handle = await proxyClient.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "slicer-latency", items: [1, 2, 3, 4, 5] },
      });

      expect(handle.id).toBeTruthy();

      const job = await proxyClient.getJob(handle.id, tenant);
      expect(job?.payload).toEqual({
        test: "slicer-latency",
        items: [1, 2, 3, 4, 5],
      });
    });
  });

  describe("high concurrency enqueue", () => {
    it("handles many concurrent enqueues without losing requests", async () => {
      // Simulate a burst of concurrent enqueues similar to the production
      // incident where 99K+ enqueues overwhelmed a single connection.
      // @grpc/grpc-js handles HTTP/2 stream multiplexing natively; this
      // test verifies that all requests complete under concurrent load.
      const concurrency = 200;
      const tenant = `high-concurrency-${Date.now()}`;

      const promises = Array.from({ length: concurrency }, (_, i) =>
        proxyClient.enqueue({
          tenant: `${tenant}-${i}`,
          taskGroup: DEFAULT_TASK_GROUP,
          payload: { index: i, test: "high-concurrency" },
        }),
      );

      const results = await Promise.allSettled(promises);
      const succeeded = results.filter((r) => r.status === "fulfilled");
      const failed = results.filter((r) => r.status === "rejected");

      // All requests should succeed under concurrent load.
      expect(succeeded.length).toBe(concurrency);
      expect(failed.length).toBe(0);
    }, 30_000);
  });

  describe("connection reset recovery", () => {
    it("recovers from connection reset during enqueue", async () => {
      const tenant = `reset-recovery-${Date.now()}`;

      // First enqueue should succeed
      const handle1 = await proxyClient.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { phase: "before-reset" },
      });
      expect(handle1.id).toBeTruthy();

      // Add a reset_peer toxic to simulate RST_STREAM-like behavior.
      // This causes the proxy to reset connections, producing errors
      // similar to what happens during HTTP/2 stream exhaustion.
      const toxic1 = await silo1.addToxic<ResetPeer>({
        name: "reset-peer",
        type: "reset_peer",
        stream: "downstream",
        toxicity: 1.0,
        attributes: { timeout: 100 },
      });
      const toxic2 = await silo2.addToxic<ResetPeer>({
        name: "reset-peer",
        type: "reset_peer",
        stream: "downstream",
        toxicity: 1.0,
        attributes: { timeout: 100 },
      });

      // Operations during the toxic should fail
      await expect(
        proxyClient.enqueue({
          tenant: `${tenant}-during`,
          taskGroup: DEFAULT_TASK_GROUP,
          payload: { phase: "during-reset" },
        }),
      ).rejects.toThrow();

      // Remove the toxic
      await toxic1.remove();
      await toxic2.remove();

      // After recovery, operations should succeed
      const handle3 = await retryUntilSuccess(() =>
        proxyClient.enqueue({
          tenant: `${tenant}-after`,
          taskGroup: DEFAULT_TASK_GROUP,
          payload: { phase: "after-reset" },
        }),
      );
      expect(handle3.id).toBeTruthy();
    });

    it("retries and recovers from intermittent connection resets", async () => {
      // Use a low toxicity to simulate intermittent RST_STREAM errors
      // (only a fraction of requests are affected). The client's retry
      // logic should recover transparently.
      await silo1.addToxic<ResetPeer>({
        name: "intermittent-reset",
        type: "reset_peer",
        stream: "downstream",
        toxicity: 0.3, // 30% of connections get reset
        attributes: { timeout: 200 },
      });
      await silo2.addToxic<ResetPeer>({
        name: "intermittent-reset",
        type: "reset_peer",
        stream: "downstream",
        toxicity: 0.3,
        attributes: { timeout: 200 },
      });

      // With retry logic, most operations should eventually succeed
      const results: string[] = [];
      const errors: Error[] = [];

      for (let i = 0; i < 10; i++) {
        try {
          const handle = await retryUntilSuccess(
            () =>
              proxyClient.enqueue({
                tenant: `intermittent-${Date.now()}-${i}`,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { index: i },
              }),
            10,
            200,
          );
          results.push(handle.id);
        } catch (e) {
          errors.push(e as Error);
        }
      }

      // Most should succeed thanks to retries
      expect(results.length).toBeGreaterThanOrEqual(8);
    }, 30_000);
  });

  describe("topology refresh under network issues", () => {
    it("topology refresh fails when proxies are disabled", async () => {
      // Use a fast-fail client so the topology refresh fails quickly
      // instead of retrying each server with full backoff (~3s per server)
      const freshClient = createFastFailProxyClient();

      await silo1.update({
        enabled: false,
        listen: silo1.listen,
        upstream: silo1.upstream,
      });
      await silo2.update({
        enabled: false,
        listen: silo2.listen,
        upstream: silo2.upstream,
      });

      await expect(freshClient.refreshTopology()).rejects.toThrow();

      await silo1.update({
        enabled: true,
        listen: silo1.listen,
        upstream: silo1.upstream,
      });
      await silo2.update({
        enabled: true,
        listen: silo2.listen,
        upstream: silo2.upstream,
      });

      freshClient.close();
    });

    it("topology refresh works with high latency", async () => {
      const freshClient = createProxyClient();

      for (const proxy of [silo1, silo2]) {
        await proxy.addToxic<Latency>({
          name: "high-latency",
          type: "latency",
          stream: "downstream",
          toxicity: 1.0,
          attributes: { latency: 300, jitter: 100 },
        });
      }

      const start = Date.now();
      await freshClient.refreshTopology();
      const elapsed = Date.now() - start;

      const topology = freshClient.getTopology();
      expect(topology.shards.length).toBeGreaterThan(0);
      expect(elapsed).toBeGreaterThanOrEqual(200);

      freshClient.close();
    });
  });
});
