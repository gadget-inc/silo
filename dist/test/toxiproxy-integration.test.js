"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const client_1 = require("../src/client");
const toxiproxy_node_client_1 = require("toxiproxy-node-client");
// Direct silo servers (for setup/verification)
const SILO_SERVERS = (process.env.SILO_SERVERS || process.env.SILO_SERVER || "localhost:7450").split(",");
// Toxiproxy-proxied silo servers
const TOXIPROXY_SILO_SERVERS = (process.env.TOXIPROXY_SILO_SERVERS || "localhost:17450,localhost:17451").split(",");
const TOXIPROXY_API_URL = process.env.TOXIPROXY_API_URL || "http://127.0.0.1:8474";
const RUN_INTEGRATION = process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";
const DEFAULT_TASK_GROUP = "toxiproxy-test-group";
const toxiproxy = new toxiproxy_node_client_1.Toxiproxy(TOXIPROXY_API_URL);
async function isToxiproxyAvailable() {
    try {
        await toxiproxy.getVersion();
        return true;
    }
    catch {
        return false;
    }
}
/**
 * Ensure the toxiproxy proxies for silo exist, creating them if needed.
 */
async function ensureProxies() {
    const proxyConfigs = [
        { name: "silo-1", listen: "127.0.0.1:17450", upstream: "127.0.0.1:7450" },
        { name: "silo-2", listen: "127.0.0.1:17451", upstream: "127.0.0.1:7451" },
    ];
    const proxies = [];
    for (const config of proxyConfigs) {
        try {
            proxies.push(await toxiproxy.get(config.name));
        }
        catch {
            proxies.push(await toxiproxy.createProxy(config));
        }
    }
    return { silo1: proxies[0], silo2: proxies[1] };
}
async function waitForClusterConvergence(client, maxAttempts = 60, delayMs = 500) {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            await client.refreshTopology();
            const topology = client.getTopology();
            const hasShards = topology.shards.length > 0;
            const allShardsHaveOwners = topology.shards.every((s) => s.serverAddr && s.serverAddr.length > 0);
            if (hasShards && allShardsHaveOwners) {
                // Verify cluster is truly ready by attempting a test enqueue
                await client.enqueue({
                    tenant: `convergence-check-${Date.now()}`,
                    taskGroup: "convergence-check",
                    payload: { check: true },
                });
                return;
            }
        }
        catch {
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
const ADDRESS_MAP = {
    "127.0.0.1:7450": "127.0.0.1:17450",
    "127.0.0.1:7451": "127.0.0.1:17451",
};
function createProxyClient() {
    return new client_1.SiloGRPCClient({
        servers: TOXIPROXY_SILO_SERVERS,
        useTls: false,
        shardRouting: { topologyRefreshIntervalMs: 0 },
        addressMap: ADDRESS_MAP,
    });
}
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
/** Retry an operation until it succeeds, instead of using a fixed sleep. */
async function retryUntilSuccess(fn, maxAttempts = 30, delayMs = 100) {
    let lastError;
    for (let i = 0; i < maxAttempts; i++) {
        try {
            return await fn();
        }
        catch (e) {
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
function createFastFailProxyClient() {
    return new client_1.SiloGRPCClient({
        servers: TOXIPROXY_SILO_SERVERS,
        useTls: false,
        shardRouting: { topologyRefreshIntervalMs: 0 },
        addressMap: ADDRESS_MAP,
        rpcOptions: { timeout: 1000 },
        grpcClientOptions: {
            "grpc.service_config": fastFailServiceConfig,
        },
    });
}
vitest_1.describe.skipIf(!RUN_INTEGRATION)("Toxiproxy gRPC client integration", () => {
    let proxyClient;
    let directClient;
    let silo1;
    let silo2;
    let toxiproxyAvailable = false;
    (0, vitest_1.beforeAll)(async () => {
        toxiproxyAvailable = await isToxiproxyAvailable();
        if (!toxiproxyAvailable)
            return;
        const proxies = await ensureProxies();
        silo1 = proxies.silo1;
        silo2 = proxies.silo2;
        await toxiproxy.reset();
        // Direct client to verify cluster is ready
        directClient = new client_1.SiloGRPCClient({
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
    (0, vitest_1.afterAll)(() => {
        proxyClient?.close();
        directClient?.close();
    });
    (0, vitest_1.beforeEach)((ctx) => {
        if (!toxiproxyAvailable)
            ctx.skip();
    });
    (0, vitest_1.afterEach)(async () => {
        await toxiproxy.reset();
    });
    (0, vitest_1.describe)("latency toxic", () => {
        (0, vitest_1.it)("operations succeed with added latency", async () => {
            await silo1.addToxic({
                name: "latency-downstream",
                type: "latency",
                stream: "downstream",
                toxicity: 1.0,
                attributes: { latency: 200, jitter: 50 },
            });
            await silo2.addToxic({
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
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            (0, vitest_1.expect)(elapsed).toBeGreaterThanOrEqual(150);
            const job = await proxyClient.getJob(handle.id, tenant);
            (0, vitest_1.expect)(job?.id).toBe(handle.id);
        });
        (0, vitest_1.it)("operations succeed with upstream latency", async () => {
            await silo1.addToxic({
                name: "latency-upstream",
                type: "latency",
                stream: "upstream",
                toxicity: 1.0,
                attributes: { latency: 150, jitter: 0 },
            });
            await silo2.addToxic({
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
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            (0, vitest_1.expect)(elapsed).toBeGreaterThanOrEqual(100);
        });
    });
    (0, vitest_1.describe)("bandwidth toxic", () => {
        (0, vitest_1.it)("operations succeed with bandwidth limitation", async () => {
            await silo1.addToxic({
                name: "bandwidth-limit",
                type: "bandwidth",
                stream: "downstream",
                toxicity: 1.0,
                attributes: { rate: 10 },
            });
            await silo2.addToxic({
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
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            const job = await proxyClient.getJob(handle.id, tenant);
            (0, vitest_1.expect)(job?.id).toBe(handle.id);
        });
    });
    (0, vitest_1.describe)("timeout toxic", () => {
        (0, vitest_1.it)("client retries and recovers after temporary timeout toxic is removed", async () => {
            const tenant = `timeout-recovery-${Date.now()}`;
            const handle = await proxyClient.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "timeout-recovery" },
            });
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            const toxic1 = await silo1.addToxic({
                name: "timeout",
                type: "timeout",
                stream: "downstream",
                toxicity: 1.0,
                attributes: { timeout: 100 },
            });
            const toxic2 = await silo2.addToxic({
                name: "timeout",
                type: "timeout",
                stream: "downstream",
                toxicity: 1.0,
                attributes: { timeout: 100 },
            });
            await (0, vitest_1.expect)(proxyClient.getJob(handle.id, tenant)).rejects.toThrow();
            await toxic1.remove();
            await toxic2.remove();
            // Retry until gRPC reconnects instead of a fixed sleep
            const job = await retryUntilSuccess(() => proxyClient.getJob(handle.id, tenant));
            (0, vitest_1.expect)(job?.id).toBe(handle.id);
        });
    });
    (0, vitest_1.describe)("proxy disable/enable", () => {
        (0, vitest_1.it)("client fails when proxy is disabled, recovers when re-enabled", async () => {
            const tenant = `disable-test-${Date.now()}`;
            const handle = await proxyClient.enqueue({
                tenant,
                taskGroup: DEFAULT_TASK_GROUP,
                payload: { test: "disable" },
            });
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            await silo1.update({ enabled: false, listen: silo1.listen, upstream: silo1.upstream });
            await silo2.update({ enabled: false, listen: silo2.listen, upstream: silo2.upstream });
            await (0, vitest_1.expect)(proxyClient.getJob(handle.id, tenant)).rejects.toThrow();
            await silo1.update({ enabled: true, listen: silo1.listen, upstream: silo1.upstream });
            await silo2.update({ enabled: true, listen: silo2.listen, upstream: silo2.upstream });
            // Retry until gRPC reconnects instead of a fixed sleep
            const job = await retryUntilSuccess(() => proxyClient.getJob(handle.id, tenant));
            (0, vitest_1.expect)(job?.id).toBe(handle.id);
        });
    });
    (0, vitest_1.describe)("slicer toxic", () => {
        (0, vitest_1.it)("operations succeed when TCP packets are sliced", async () => {
            await silo1.addToxic({
                name: "slicer",
                type: "slicer",
                stream: "downstream",
                toxicity: 1.0,
                attributes: { average_size: 50, size_variation: 20, delay: 5 },
            });
            await silo2.addToxic({
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
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            const job = await proxyClient.getJob(handle.id, tenant);
            (0, vitest_1.expect)(job?.id).toBe(handle.id);
            (0, vitest_1.expect)(job?.payload).toEqual({ test: "slicer", data: "a]".repeat(100) });
        });
    });
    (0, vitest_1.describe)("partial failure", () => {
        (0, vitest_1.it)("operations succeed when only one proxy is degraded", async () => {
            await silo1.addToxic({
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
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            const job = await proxyClient.getJob(handle.id, tenant);
            (0, vitest_1.expect)(job?.id).toBe(handle.id);
        });
        (0, vitest_1.it)("can enqueue and retrieve multiple jobs with one proxy disabled", async () => {
            // Use a fast-fail client so calls routed to the disabled proxy fail quickly
            // instead of exhausting the default retry policy (~3s per failed call)
            const fastClient = createFastFailProxyClient();
            await fastClient.refreshTopology();
            await silo1.update({ enabled: false, listen: silo1.listen, upstream: silo1.upstream });
            const results = [];
            const errors = [];
            for (let i = 0; i < 5; i++) {
                const tenant = `partial-multi-${Date.now()}-${i}`;
                try {
                    const handle = await fastClient.enqueue({
                        tenant,
                        taskGroup: DEFAULT_TASK_GROUP,
                        payload: { test: "partial-multi", index: i },
                    });
                    results.push(handle.id);
                }
                catch (e) {
                    errors.push(e);
                }
            }
            (0, vitest_1.expect)(results.length + errors.length).toBe(5);
            await silo1.update({ enabled: true, listen: silo1.listen, upstream: silo1.upstream });
            fastClient.close();
        });
    });
    (0, vitest_1.describe)("slow close toxic", () => {
        (0, vitest_1.it)("operations complete despite slow connection closing", async () => {
            await silo1.addToxic({
                name: "slow-close",
                type: "slow_close",
                stream: "downstream",
                toxicity: 1.0,
                attributes: { delay: 500 },
            });
            await silo2.addToxic({
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
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            const job = await proxyClient.getJob(handle.id, tenant);
            (0, vitest_1.expect)(job?.id).toBe(handle.id);
        });
    });
    (0, vitest_1.describe)("combined toxics", () => {
        (0, vitest_1.it)("operations succeed with latency + bandwidth combination", async () => {
            for (const proxy of [silo1, silo2]) {
                await proxy.addToxic({
                    name: "latency-combined",
                    type: "latency",
                    stream: "downstream",
                    toxicity: 1.0,
                    attributes: { latency: 100, jitter: 25 },
                });
                await proxy.addToxic({
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
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            const job = await proxyClient.getJob(handle.id, tenant);
            (0, vitest_1.expect)(job?.id).toBe(handle.id);
        });
        (0, vitest_1.it)("operations succeed with slicer + latency combination", async () => {
            for (const proxy of [silo1, silo2]) {
                await proxy.addToxic({
                    name: "slicer-combined",
                    type: "slicer",
                    stream: "downstream",
                    toxicity: 1.0,
                    attributes: { average_size: 100, size_variation: 50, delay: 2 },
                });
                await proxy.addToxic({
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
            (0, vitest_1.expect)(handle.id).toBeTruthy();
            const job = await proxyClient.getJob(handle.id, tenant);
            (0, vitest_1.expect)(job?.payload).toEqual({ test: "slicer-latency", items: [1, 2, 3, 4, 5] });
        });
    });
    (0, vitest_1.describe)("topology refresh under network issues", () => {
        (0, vitest_1.it)("topology refresh fails when proxies are disabled", async () => {
            // Use a fast-fail client so the topology refresh fails quickly
            // instead of retrying each server with full backoff (~3s per server)
            const freshClient = createFastFailProxyClient();
            await silo1.update({ enabled: false, listen: silo1.listen, upstream: silo1.upstream });
            await silo2.update({ enabled: false, listen: silo2.listen, upstream: silo2.upstream });
            await (0, vitest_1.expect)(freshClient.refreshTopology()).rejects.toThrow();
            await silo1.update({ enabled: true, listen: silo1.listen, upstream: silo1.upstream });
            await silo2.update({ enabled: true, listen: silo2.listen, upstream: silo2.upstream });
            freshClient.close();
        });
        (0, vitest_1.it)("topology refresh works with high latency", async () => {
            const freshClient = createProxyClient();
            for (const proxy of [silo1, silo2]) {
                await proxy.addToxic({
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
            (0, vitest_1.expect)(topology.shards.length).toBeGreaterThan(0);
            (0, vitest_1.expect)(elapsed).toBeGreaterThanOrEqual(200);
            freshClient.close();
        });
    });
});
//# sourceMappingURL=toxiproxy-integration.test.js.map