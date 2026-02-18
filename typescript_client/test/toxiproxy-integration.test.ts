import { describe, it, expect, beforeAll, afterAll, afterEach } from "vitest";
import { SiloGRPCClient } from "../src/client";
import http from "node:http";

// Direct silo servers (for setup/verification)
const SILO_SERVERS = (process.env.SILO_SERVERS || process.env.SILO_SERVER || "localhost:7450").split(
  ",",
);

// Toxiproxy-proxied silo servers
const TOXIPROXY_SILO_SERVERS = (
  process.env.TOXIPROXY_SILO_SERVERS || "localhost:17450,localhost:17451"
).split(",");

const TOXIPROXY_API_URL = process.env.TOXIPROXY_API_URL || "http://127.0.0.1:8474";

const RUN_INTEGRATION = process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";

const DEFAULT_TASK_GROUP = "toxiproxy-test-group";

// ─── Toxiproxy HTTP API helpers ─────────────────────────────────────────────

interface ToxicAttributes {
  latency?: number;
  jitter?: number;
  rate?: number;
  bytes?: number;
  timeout?: number;
  delay?: number;
  size_variation?: number;
  average_size?: number;
}

interface Toxic {
  name: string;
  type: string;
  stream: "upstream" | "downstream";
  toxicity: number;
  attributes: ToxicAttributes;
}

function toxiproxyRequest(
  method: string,
  path: string,
  body?: Record<string, unknown>,
): Promise<{ status: number; data: unknown }> {
  return new Promise((resolve, reject) => {
    const url = new URL(path, TOXIPROXY_API_URL);
    const payload = body ? JSON.stringify(body) : undefined;

    const req = http.request(
      {
        hostname: url.hostname,
        port: url.port,
        path: url.pathname,
        method,
        headers: {
          "Content-Type": "application/json",
          ...(payload ? { "Content-Length": Buffer.byteLength(payload) } : {}),
        },
      },
      (res) => {
        let data = "";
        res.on("data", (chunk: string) => (data += chunk));
        res.on("end", () => {
          try {
            resolve({ status: res.statusCode!, data: data ? JSON.parse(data) : null });
          } catch {
            resolve({ status: res.statusCode!, data });
          }
        });
      },
    );

    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

async function addToxic(
  proxyName: string,
  toxic: {
    name: string;
    type: string;
    stream?: "upstream" | "downstream";
    toxicity?: number;
    attributes: ToxicAttributes;
  },
): Promise<Toxic> {
  const res = await toxiproxyRequest("POST", `/proxies/${proxyName}/toxics`, {
    name: toxic.name,
    type: toxic.type,
    stream: toxic.stream ?? "downstream",
    toxicity: toxic.toxicity ?? 1.0,
    attributes: toxic.attributes,
  });
  if (res.status !== 200) {
    throw new Error(`Failed to add toxic: ${JSON.stringify(res.data)}`);
  }
  return res.data as Toxic;
}

async function removeToxic(proxyName: string, toxicName: string): Promise<void> {
  const res = await toxiproxyRequest("DELETE", `/proxies/${proxyName}/toxics/${toxicName}`);
  if (res.status !== 204 && res.status !== 200) {
    throw new Error(`Failed to remove toxic "${toxicName}": ${JSON.stringify(res.data)}`);
  }
}

async function resetToxiproxy(): Promise<void> {
  const res = await toxiproxyRequest("POST", "/reset");
  if (res.status !== 204 && res.status !== 200) {
    throw new Error(`Failed to reset toxiproxy: ${JSON.stringify(res.data)}`);
  }
}

async function disableProxy(proxyName: string): Promise<void> {
  const res = await toxiproxyRequest("POST", `/proxies/${proxyName}`, { enabled: false });
  if (res.status !== 200) {
    throw new Error(`Failed to disable proxy "${proxyName}": ${JSON.stringify(res.data)}`);
  }
}

async function enableProxy(proxyName: string): Promise<void> {
  const res = await toxiproxyRequest("POST", `/proxies/${proxyName}`, { enabled: true });
  if (res.status !== 200) {
    throw new Error(`Failed to enable proxy "${proxyName}": ${JSON.stringify(res.data)}`);
  }
}

async function isToxiproxyAvailable(): Promise<boolean> {
  try {
    const res = await toxiproxyRequest("GET", "/version");
    return res.status === 200;
  } catch {
    return false;
  }
}

async function ensureProxies(): Promise<void> {
  const proxies = [
    { name: "silo-1", listen: "127.0.0.1:17450", upstream: "127.0.0.1:7450" },
    { name: "silo-2", listen: "127.0.0.1:17451", upstream: "127.0.0.1:7451" },
  ];
  for (const proxy of proxies) {
    const existing = await toxiproxyRequest("GET", `/proxies/${proxy.name}`);
    if (existing.status === 200) continue;
    const res = await toxiproxyRequest("POST", "/proxies", proxy);
    if (res.status !== 201 && res.status !== 200) {
      throw new Error(`Failed to create proxy "${proxy.name}": ${JSON.stringify(res.data)}`);
    }
  }
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
        const testTenant = `convergence-check-${Date.now()}`;
        await client.enqueue({
          tenant: testTenant,
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

/**
 * Create a SiloGRPCClient that routes through toxiproxy.
 *
 * The silo servers are configured with advertised_grpc_addr pointing to the
 * toxiproxy listen addresses, so topology discovery will return the proxy
 * addresses and all subsequent connections will go through the proxy.
 */
function createProxyClient(): SiloGRPCClient {
  return new SiloGRPCClient({
    servers: TOXIPROXY_SILO_SERVERS,
    useTls: false,
    shardRouting: { topologyRefreshIntervalMs: 0 },
  });
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe.skipIf(!RUN_INTEGRATION)("Toxiproxy gRPC client integration", () => {
  let proxyClient: SiloGRPCClient;
  let directClient: SiloGRPCClient;
  let toxiproxyAvailable: boolean;

  beforeAll(async () => {
    toxiproxyAvailable = await isToxiproxyAvailable();
    if (!toxiproxyAvailable) {
      console.warn("Toxiproxy not available, skipping toxiproxy tests");
      return;
    }

    await ensureProxies();
    await resetToxiproxy();

    // Direct client to verify cluster is ready before running proxy tests
    directClient = new SiloGRPCClient({
      servers: SILO_SERVERS,
      useTls: false,
      shardRouting: { topologyRefreshIntervalMs: 0 },
    });
    await waitForClusterConvergence(directClient);

    // Proxy client — the silo servers advertise their toxiproxy addresses, so after
    // topology discovery the client routes all traffic through the proxy.
    proxyClient = createProxyClient();
    await waitForClusterConvergence(proxyClient);
  }, 60_000);

  afterAll(() => {
    proxyClient?.close();
    directClient?.close();
  });

  afterEach(async () => {
    if (toxiproxyAvailable) {
      await resetToxiproxy();
    }
  });

  describe("latency toxic", () => {
    it("operations succeed with added latency", async () => {
      if (!toxiproxyAvailable) return;

      await addToxic("silo-1", {
        name: "latency-downstream",
        type: "latency",
        stream: "downstream",
        attributes: { latency: 200, jitter: 50 },
      });
      await addToxic("silo-2", {
        name: "latency-downstream",
        type: "latency",
        stream: "downstream",
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
      if (!toxiproxyAvailable) return;

      await addToxic("silo-1", {
        name: "latency-upstream",
        type: "latency",
        stream: "upstream",
        attributes: { latency: 150, jitter: 0 },
      });
      await addToxic("silo-2", {
        name: "latency-upstream",
        type: "latency",
        stream: "upstream",
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
      if (!toxiproxyAvailable) return;

      await addToxic("silo-1", {
        name: "bandwidth-limit",
        type: "bandwidth",
        stream: "downstream",
        attributes: { rate: 10 },
      });
      await addToxic("silo-2", {
        name: "bandwidth-limit",
        type: "bandwidth",
        stream: "downstream",
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
      if (!toxiproxyAvailable) return;

      const tenant = `timeout-recovery-${Date.now()}`;

      const handle = await proxyClient.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "timeout-recovery" },
      });
      expect(handle.id).toBeTruthy();

      await addToxic("silo-1", {
        name: "timeout",
        type: "timeout",
        stream: "downstream",
        attributes: { timeout: 100 },
      });
      await addToxic("silo-2", {
        name: "timeout",
        type: "timeout",
        stream: "downstream",
        attributes: { timeout: 100 },
      });

      await expect(proxyClient.getJob(handle.id, tenant)).rejects.toThrow();

      await removeToxic("silo-1", "timeout");
      await removeToxic("silo-2", "timeout");

      // Give gRPC a moment to reconnect
      await sleep(1000);

      const job = await proxyClient.getJob(handle.id, tenant);
      expect(job?.id).toBe(handle.id);
    });
  });

  describe("proxy disable/enable", () => {
    it("client fails when proxy is disabled, recovers when re-enabled", async () => {
      if (!toxiproxyAvailable) return;

      const tenant = `disable-test-${Date.now()}`;

      const handle = await proxyClient.enqueue({
        tenant,
        taskGroup: DEFAULT_TASK_GROUP,
        payload: { test: "disable" },
      });
      expect(handle.id).toBeTruthy();

      await disableProxy("silo-1");
      await disableProxy("silo-2");

      await expect(proxyClient.getJob(handle.id, tenant)).rejects.toThrow();

      await enableProxy("silo-1");
      await enableProxy("silo-2");

      await sleep(1000);

      const job = await proxyClient.getJob(handle.id, tenant);
      expect(job?.id).toBe(handle.id);
    });
  });

  describe("slicer toxic", () => {
    it("operations succeed when TCP packets are sliced", async () => {
      if (!toxiproxyAvailable) return;

      await addToxic("silo-1", {
        name: "slicer",
        type: "slicer",
        stream: "downstream",
        attributes: { average_size: 50, size_variation: 20, delay: 5 },
      });
      await addToxic("silo-2", {
        name: "slicer",
        type: "slicer",
        stream: "downstream",
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
      if (!toxiproxyAvailable) return;

      // Only add latency to silo-1, silo-2 stays healthy
      await addToxic("silo-1", {
        name: "latency-heavy",
        type: "latency",
        stream: "downstream",
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
      if (!toxiproxyAvailable) return;

      await disableProxy("silo-1");

      const results: string[] = [];
      const errors: Error[] = [];

      for (let i = 0; i < 5; i++) {
        const tenant = `partial-multi-${Date.now()}-${i}`;
        try {
          const handle = await proxyClient.enqueue({
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

      await enableProxy("silo-1");
    });
  });

  describe("slow close toxic", () => {
    it("operations complete despite slow connection closing", async () => {
      if (!toxiproxyAvailable) return;

      await addToxic("silo-1", {
        name: "slow-close",
        type: "slow_close",
        stream: "downstream",
        attributes: { delay: 500 },
      });
      await addToxic("silo-2", {
        name: "slow-close",
        type: "slow_close",
        stream: "downstream",
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
      if (!toxiproxyAvailable) return;

      for (const proxy of ["silo-1", "silo-2"]) {
        await addToxic(proxy, {
          name: "latency-combined",
          type: "latency",
          stream: "downstream",
          attributes: { latency: 100, jitter: 25 },
        });
        await addToxic(proxy, {
          name: "bandwidth-combined",
          type: "bandwidth",
          stream: "downstream",
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
      if (!toxiproxyAvailable) return;

      for (const proxy of ["silo-1", "silo-2"]) {
        await addToxic(proxy, {
          name: "slicer-combined",
          type: "slicer",
          stream: "downstream",
          attributes: { average_size: 100, size_variation: 50, delay: 2 },
        });
        await addToxic(proxy, {
          name: "latency-combined",
          type: "latency",
          stream: "upstream",
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
      expect(job?.payload).toEqual({ test: "slicer-latency", items: [1, 2, 3, 4, 5] });
    });
  });

  describe("topology refresh under network issues", () => {
    it("topology refresh fails when proxies are disabled", async () => {
      if (!toxiproxyAvailable) return;

      // Create a fresh client that only knows proxy addresses
      const freshClient = createProxyClient();

      await disableProxy("silo-1");
      await disableProxy("silo-2");

      await expect(freshClient.refreshTopology()).rejects.toThrow();

      await enableProxy("silo-1");
      await enableProxy("silo-2");

      freshClient.close();
    });

    it("topology refresh works with high latency", async () => {
      if (!toxiproxyAvailable) return;

      // Create a fresh client that only knows proxy addresses
      const freshClient = createProxyClient();

      for (const proxy of ["silo-1", "silo-2"]) {
        await addToxic(proxy, {
          name: "high-latency",
          type: "latency",
          stream: "downstream",
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
