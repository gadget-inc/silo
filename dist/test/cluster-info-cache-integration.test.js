"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const client_1 = require("../src/client");
const toxiproxy_node_client_1 = require("toxiproxy-node-client");
const SILO_SERVERS = (process.env.SILO_SERVERS ||
    process.env.SILO_SERVER ||
    "127.0.0.1:7450,127.0.0.1:7451").split(",");
const TOXIPROXY_API_URL = process.env.TOXIPROXY_API_URL || "http://127.0.0.1:8474";
const RUN_INTEGRATION = process.env.RUN_INTEGRATION === "true" || process.env.CI === "true";
const ETCD_PROXY_NAME = "etcd";
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
async function getEtcdProxy() {
    try {
        return await toxiproxy.get(ETCD_PROXY_NAME);
    }
    catch {
        return null;
    }
}
/** Drop the named toxic if present. `toxiproxy-node-client` has no
 * removeToxic helper, so we look it up and call its own `.remove()`. */
async function clearToxic(proxy, name) {
    try {
        const toxic = await proxy.getToxic(name);
        await toxic.remove();
    }
    catch {
        // toxic doesn't exist — nothing to do
    }
}
async function enableProxy(proxy) {
    if (!proxy.enabled) {
        await proxy.update({
            enabled: true,
            listen: proxy.listen,
            upstream: proxy.upstream,
        });
    }
}
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function waitForClusterConvergence(client, maxAttempts = 60, delayMs = 500) {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            await client.refreshTopology();
            const topology = client.getTopology();
            const hasShards = topology.shards.length > 0;
            const allShardsHaveOwners = topology.shards.every((s) => s.serverAddr && s.serverAddr.length > 0);
            if (hasShards && allShardsHaveOwners) {
                await client.enqueue({
                    tenant: `convergence-check-${Date.now()}`,
                    taskGroup: "convergence-check",
                    payload: { check: true },
                });
                return;
            }
        }
        catch {
            // ignore — cluster may still be acquiring shards
        }
        if (attempt === maxAttempts) {
            throw new Error(`Cluster did not converge after ${maxAttempts} attempts`);
        }
        await sleep(delayMs);
    }
}
vitest_1.describe.skipIf(!RUN_INTEGRATION)("GetClusterInfo cache fallback integration", () => {
    let etcdProxy;
    let client;
    (0, vitest_1.beforeAll)(async () => {
        if (!(await isToxiproxyAvailable())) {
            throw new Error(`toxiproxy API not reachable at ${TOXIPROXY_API_URL} — start the dev cluster before running this test`);
        }
        const proxy = await getEtcdProxy();
        if (!proxy) {
            throw new Error(`toxiproxy proxy "${ETCD_PROXY_NAME}" is not configured — check nix/modules/devshell.nix and reload direnv`);
        }
        etcdProxy = proxy;
        // Reset any leftover toxics before we start.
        await clearToxic(etcdProxy, "etcd-timeout");
        await enableProxy(etcdProxy);
        client = new client_1.SiloGRPCClient({
            servers: SILO_SERVERS,
            useTls: false,
            shardRouting: {
                topologyRefreshIntervalMs: 0,
                // Give topology refresh enough time to let the server's own 5s
                // coordination-layer timeout elapse (plus a little slack).
                topologyRefreshTimeoutMs: 15_000,
            },
            rpcOptions: { timeout: 15_000 },
        });
        await waitForClusterConvergence(client);
    }, 90_000);
    (0, vitest_1.afterAll)(async () => {
        if (etcdProxy) {
            await clearToxic(etcdProxy, "etcd-timeout");
            await enableProxy(etcdProxy).catch(() => { });
        }
        client?.close();
    });
    (0, vitest_1.afterEach)(async () => {
        if (etcdProxy) {
            await clearToxic(etcdProxy, "etcd-timeout");
            await enableProxy(etcdProxy).catch(() => { });
        }
    });
    (0, vitest_1.it)("returns cached topology within the server's 5s timeout when etcd stops responding", async () => {
        // Capture the topology while etcd is healthy — this is what the
        // server-side cache should contain after a successful call.
        await client.refreshTopology();
        const snapshot = client.getTopology();
        (0, vitest_1.expect)(snapshot.shards.length).toBeGreaterThan(0);
        const snapshotPairs = [...snapshot.shardToServer.entries()].sort();
        (0, vitest_1.expect)(snapshotPairs.length).toBeGreaterThan(0);
        // Block all etcd traffic from silo. The `timeout` toxic with
        // timeout=0 holds the connection open and never delivers data, so
        // silo's etcd client hangs — exactly the "k8s API unreachable"
        // scenario we care about.
        await etcdProxy.addToxic({
            name: "etcd-timeout",
            type: "timeout",
            stream: "downstream",
            toxicity: 1.0,
            attributes: { timeout: 0 },
        });
        // Call GetClusterInfo and verify:
        //  - it returns within the server's timeout window (≤8s is generous)
        //  - the response matches the pre-outage snapshot (proving it came
        //    from the server-side cache rather than a live etcd read)
        const started = Date.now();
        await client.refreshTopology();
        const elapsed = Date.now() - started;
        (0, vitest_1.expect)(elapsed, `expected cached refresh to return within 8s, took ${elapsed}ms`).toBeLessThan(8_000);
        const cached = client.getTopology();
        const cachedPairs = [...cached.shardToServer.entries()].sort();
        (0, vitest_1.expect)(cachedPairs).toEqual(snapshotPairs);
        (0, vitest_1.expect)(cached.shards.map((s) => s.shardId).sort()).toEqual(snapshot.shards.map((s) => s.shardId).sort());
    }, 60_000);
});
//# sourceMappingURL=cluster-info-cache-integration.test.js.map