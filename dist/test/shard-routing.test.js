"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const client_1 = require("../src/client");
const runtime_rpc_1 = require("@protobuf-ts/runtime-rpc");
(0, vitest_1.describe)("Shard Routing", () => {
    (0, vitest_1.beforeAll)(() => (0, client_1.initHasher)());
    (0, vitest_1.describe)("hashTenant", () => {
        (0, vitest_1.it)("returns a 16-character hex string", () => {
            const hash = (0, client_1.hashTenant)("test-tenant");
            (0, vitest_1.expect)(hash).toMatch(/^[0-9a-f]{16}$/);
        });
        (0, vitest_1.it)("is deterministic", () => {
            (0, vitest_1.expect)((0, client_1.hashTenant)("tenant-1")).toBe((0, client_1.hashTenant)("tenant-1"));
        });
        (0, vitest_1.it)("distributes env- tenants across different first hex digits", () => {
            const firstDigits = new Set();
            for (let i = 0; i < 100; i++) {
                firstDigits.add((0, client_1.hashTenant)(`env-${i}`)[0]);
            }
            // With good hashing, 100 env- tenants should hit many different first digits
            (0, vitest_1.expect)(firstDigits.size).toBeGreaterThan(8);
        });
    });
    (0, vitest_1.describe)("shardForTenant lexicographic lookup (expects hash-space keys)", () => {
        (0, vitest_1.it)("finds correct shard for key in range", () => {
            const shards = [
                {
                    shardId: "shard-1",
                    serverAddr: "server-a:7450",
                    rangeStart: "",
                    rangeEnd: "8000000000000000",
                },
                {
                    shardId: "shard-2",
                    serverAddr: "server-b:7450",
                    rangeStart: "8000000000000000",
                    rangeEnd: "",
                },
            ];
            // Keys in hash space — direct lexicographic comparison
            (0, vitest_1.expect)((0, client_1.shardForTenant)("3000000000000000", shards)?.shardId).toBe("shard-1");
            (0, vitest_1.expect)((0, client_1.shardForTenant)("a000000000000000", shards)?.shardId).toBe("shard-2");
        });
        (0, vitest_1.it)("handles single full-range shard", () => {
            const shards = [
                {
                    shardId: "shard-1",
                    serverAddr: "server-a:7450",
                    rangeStart: "",
                    rangeEnd: "",
                },
            ];
            (0, vitest_1.expect)((0, client_1.shardForTenant)("anything", shards)?.shardId).toBe("shard-1");
        });
        (0, vitest_1.it)("returns undefined for empty shards array", () => {
            (0, vitest_1.expect)((0, client_1.shardForTenant)("key", [])).toBeUndefined();
        });
        (0, vitest_1.it)("handles multiple shard ranges", () => {
            const shards = [
                {
                    shardId: "shard-1",
                    serverAddr: "server-a:7450",
                    rangeStart: "",
                    rangeEnd: "4000000000000000",
                },
                {
                    shardId: "shard-2",
                    serverAddr: "server-b:7450",
                    rangeStart: "4000000000000000",
                    rangeEnd: "8000000000000000",
                },
                {
                    shardId: "shard-3",
                    serverAddr: "server-a:7450",
                    rangeStart: "8000000000000000",
                    rangeEnd: "c000000000000000",
                },
                {
                    shardId: "shard-4",
                    serverAddr: "server-b:7450",
                    rangeStart: "c000000000000000",
                    rangeEnd: "",
                },
            ];
            (0, vitest_1.expect)((0, client_1.shardForTenant)("1000000000000000", shards)?.shardId).toBe("shard-1");
            (0, vitest_1.expect)((0, client_1.shardForTenant)("5000000000000000", shards)?.shardId).toBe("shard-2");
            (0, vitest_1.expect)((0, client_1.shardForTenant)("9000000000000000", shards)?.shardId).toBe("shard-3");
            (0, vitest_1.expect)((0, client_1.shardForTenant)("d000000000000000", shards)?.shardId).toBe("shard-4");
        });
        (0, vitest_1.it)("end-to-end: hashTenant + shardForTenant routes tenants correctly", () => {
            const shards = [
                {
                    shardId: "shard-1",
                    serverAddr: "server-a:7450",
                    rangeStart: "",
                    rangeEnd: "8000000000000000",
                },
                {
                    shardId: "shard-2",
                    serverAddr: "server-b:7450",
                    rangeStart: "8000000000000000",
                    rangeEnd: "",
                },
            ];
            // Hash first, then look up — this is what _resolveShard does
            for (const tenant of ["test-tenant", "env-123", "bench-0"]) {
                const result = (0, client_1.shardForTenant)((0, client_1.hashTenant)(tenant), shards);
                (0, vitest_1.expect)(result).toBeDefined();
                (0, vitest_1.expect)(["shard-1", "shard-2"]).toContain(result?.shardId);
            }
            // Same tenant always routes to same shard
            const r1 = (0, client_1.shardForTenant)((0, client_1.hashTenant)("my-tenant"), shards);
            const r2 = (0, client_1.shardForTenant)((0, client_1.hashTenant)("my-tenant"), shards);
            (0, vitest_1.expect)(r1?.shardId).toBe(r2?.shardId);
        });
    });
    (0, vitest_1.describe)("Topology discovery", () => {
        let client;
        (0, vitest_1.beforeEach)(() => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:7450",
                useTls: false,
                shardRouting: {
                    topologyRefreshIntervalMs: 0,
                },
            });
        });
        (0, vitest_1.afterEach)(() => {
            client.close();
        });
        (0, vitest_1.it)("updates shard-to-server mapping from GetClusterInfo response", async () => {
            const mockGetClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({
                    numShards: 4,
                    shardOwners: [
                        {
                            shardId: "00000000-0000-0000-0000-000000000001",
                            grpcAddr: "server-a:7450",
                            nodeId: "node-a",
                            rangeStart: "",
                            rangeEnd: "4000000000000000",
                        },
                        {
                            shardId: "00000000-0000-0000-0000-000000000002",
                            grpcAddr: "server-b:7450",
                            nodeId: "node-b",
                            rangeStart: "4000000000000000",
                            rangeEnd: "8000000000000000",
                        },
                        {
                            shardId: "00000000-0000-0000-0000-000000000003",
                            grpcAddr: "server-a:7450",
                            nodeId: "node-a",
                            rangeStart: "8000000000000000",
                            rangeEnd: "c000000000000000",
                        },
                        {
                            shardId: "00000000-0000-0000-0000-000000000004",
                            grpcAddr: "server-b:7450",
                            nodeId: "node-b",
                            rangeStart: "c000000000000000",
                            rangeEnd: "",
                        },
                    ],
                    thisNodeId: "node-a",
                    thisGrpcAddr: "server-a:7450",
                }),
            });
            const connections = client._connections;
            const conn = connections.values().next().value;
            conn.client.getClusterInfo = mockGetClusterInfo;
            await client.refreshTopology();
            const topology = client.getTopology();
            (0, vitest_1.expect)(topology.shards.length).toBe(4);
            (0, vitest_1.expect)(topology.shardToServer.get("00000000-0000-0000-0000-000000000001")).toBe("server-a:7450");
            (0, vitest_1.expect)(topology.shardToServer.get("00000000-0000-0000-0000-000000000002")).toBe("server-b:7450");
            (0, vitest_1.expect)(topology.shardToServer.get("00000000-0000-0000-0000-000000000003")).toBe("server-a:7450");
            (0, vitest_1.expect)(topology.shardToServer.get("00000000-0000-0000-0000-000000000004")).toBe("server-b:7450");
        });
        (0, vitest_1.it)("creates connections to discovered servers", async () => {
            const mockGetClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({
                    numShards: 2,
                    shardOwners: [
                        {
                            shardId: "00000000-0000-0000-0000-000000000001",
                            grpcAddr: "server-x:7450",
                            nodeId: "node-x",
                            rangeStart: "",
                            rangeEnd: "8000000000000000",
                        },
                        {
                            shardId: "00000000-0000-0000-0000-000000000002",
                            grpcAddr: "server-y:7450",
                            nodeId: "node-y",
                            rangeStart: "8000000000000000",
                            rangeEnd: "",
                        },
                    ],
                    thisNodeId: "node-x",
                    thisGrpcAddr: "server-x:7450",
                }),
            });
            const connections = client._connections;
            const conn = connections.values().next().value;
            conn.client.getClusterInfo = mockGetClusterInfo;
            await client.refreshTopology();
            (0, vitest_1.expect)(connections.has("server-x:7450")).toBe(true);
            (0, vitest_1.expect)(connections.has("server-y:7450")).toBe(true);
        });
        (0, vitest_1.it)("tries next server if first fails during topology refresh", async () => {
            client.close();
            client = new client_1.SiloGRPCClient({
                servers: ["failing-server:7450", "working-server:7450"],
                useTls: false,
                shardRouting: {
                    topologyRefreshIntervalMs: 0,
                },
            });
            const connections = client._connections;
            // First server fails
            const failingConn = connections.get("failing-server:7450");
            failingConn.client.getClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: Promise.reject(new Error("connection refused")),
            });
            // Second server succeeds
            const workingConn = connections.get("working-server:7450");
            workingConn.client.getClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({
                    numShards: 1,
                    shardOwners: [
                        {
                            shardId: "00000000-0000-0000-0000-000000000001",
                            grpcAddr: "working-server:7450",
                            nodeId: "node-1",
                            rangeStart: "",
                            rangeEnd: "",
                        },
                    ],
                    thisNodeId: "node-1",
                    thisGrpcAddr: "working-server:7450",
                }),
            });
            await client.refreshTopology();
            const topology = client.getTopology();
            (0, vitest_1.expect)(topology.shards.length).toBe(1);
            (0, vitest_1.expect)(topology.shardToServer.get("00000000-0000-0000-0000-000000000001")).toBe("working-server:7450");
        });
        (0, vitest_1.it)("throws if all servers fail during topology refresh after exhausting retries", async () => {
            // Even with gRPC-level retries (configured for UNAVAILABLE, RESOURCE_EXHAUSTED),
            // if all servers persistently fail, topology refresh should eventually throw
            const mockGetClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: Promise.reject(new Error("connection refused")),
            });
            const connections = client._connections;
            const conn = connections.values().next().value;
            conn.client.getClusterInfo = mockGetClusterInfo;
            await (0, vitest_1.expect)(client.refreshTopology()).rejects.toThrow("Failed to refresh cluster topology from any server");
        });
        (0, vitest_1.it)("applies per-server timeout to getClusterInfo calls during refresh", async () => {
            client.close();
            client = new client_1.SiloGRPCClient({
                servers: "localhost:7450",
                useTls: false,
                shardRouting: {
                    topologyRefreshIntervalMs: 0,
                    topologyRefreshTimeoutMs: 5000,
                },
            });
            const capturedOptions = [];
            const mockGetClusterInfo = vitest_1.vi
                .fn()
                .mockImplementation((_req, opts) => {
                capturedOptions.push(opts);
                return {
                    response: Promise.resolve({
                        numShards: 1,
                        shardOwners: [
                            {
                                shardId: "00000000-0000-0000-0000-000000000001",
                                grpcAddr: "localhost:7450",
                                nodeId: "node-1",
                                rangeStart: "",
                                rangeEnd: "",
                            },
                        ],
                        thisNodeId: "node-1",
                        thisGrpcAddr: "localhost:7450",
                    }),
                };
            });
            const connections = client._connections;
            const conn = connections.values().next().value;
            conn.client.getClusterInfo = mockGetClusterInfo;
            await client.refreshTopology();
            (0, vitest_1.expect)(capturedOptions.length).toBe(1);
            (0, vitest_1.expect)(capturedOptions[0].timeout).toBe(5000);
        });
        (0, vitest_1.it)("uses default 10s timeout when topologyRefreshTimeoutMs is not set", async () => {
            const capturedOptions = [];
            const mockGetClusterInfo = vitest_1.vi
                .fn()
                .mockImplementation((_req, opts) => {
                capturedOptions.push(opts);
                return {
                    response: Promise.resolve({
                        numShards: 1,
                        shardOwners: [
                            {
                                shardId: "00000000-0000-0000-0000-000000000001",
                                grpcAddr: "localhost:7450",
                                nodeId: "node-1",
                                rangeStart: "",
                                rangeEnd: "",
                            },
                        ],
                        thisNodeId: "node-1",
                        thisGrpcAddr: "localhost:7450",
                    }),
                };
            });
            const connections = client._connections;
            const conn = connections.values().next().value;
            conn.client.getClusterInfo = mockGetClusterInfo;
            await client.refreshTopology();
            (0, vitest_1.expect)(capturedOptions.length).toBe(1);
            (0, vitest_1.expect)(capturedOptions[0].timeout).toBe(10_000);
        });
        (0, vitest_1.it)("moves to next server when getClusterInfo times out on first server", async () => {
            client.close();
            client = new client_1.SiloGRPCClient({
                servers: ["slow-server:7450", "fast-server:7450"],
                useTls: false,
                shardRouting: {
                    topologyRefreshIntervalMs: 0,
                    topologyRefreshTimeoutMs: 100,
                },
            });
            const connections = client._connections;
            // First server hangs (never resolves, simulating a timeout that gRPC would enforce)
            const slowConn = connections.get("slow-server:7450");
            slowConn.client.getClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: new Promise((_resolve, reject) => {
                    setTimeout(() => reject(new Error("DEADLINE_EXCEEDED")), 50);
                }),
            });
            // Second server responds immediately
            const fastConn = connections.get("fast-server:7450");
            fastConn.client.getClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({
                    numShards: 1,
                    shardOwners: [
                        {
                            shardId: "00000000-0000-0000-0000-000000000001",
                            grpcAddr: "fast-server:7450",
                            nodeId: "node-1",
                            rangeStart: "",
                            rangeEnd: "",
                        },
                    ],
                    thisNodeId: "node-1",
                    thisGrpcAddr: "fast-server:7450",
                }),
            });
            await client.refreshTopology();
            const topology = client.getTopology();
            (0, vitest_1.expect)(topology.shards.length).toBe(1);
            (0, vitest_1.expect)(topology.shardToServer.get("00000000-0000-0000-0000-000000000001")).toBe("fast-server:7450");
        });
        (0, vitest_1.it)("cleans up stale connections after topology refresh", async () => {
            // Simulate first topology refresh that discovers two servers by pod IP
            const mockGetClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({
                    numShards: 2,
                    shardOwners: [
                        {
                            shardId: "00000000-0000-0000-0000-000000000001",
                            grpcAddr: "10.0.0.1:7450",
                            nodeId: "node-a",
                            rangeStart: "",
                            rangeEnd: "8000000000000000",
                        },
                        {
                            shardId: "00000000-0000-0000-0000-000000000002",
                            grpcAddr: "10.0.0.2:7450",
                            nodeId: "node-b",
                            rangeStart: "8000000000000000",
                            rangeEnd: "",
                        },
                    ],
                    thisNodeId: "node-a",
                    thisGrpcAddr: "10.0.0.1:7450",
                }),
            });
            const connections = client._connections;
            const conn = connections.values().next().value;
            conn.client.getClusterInfo = mockGetClusterInfo;
            await client.refreshTopology();
            // Should have connections to initial server + two discovered pod IPs
            (0, vitest_1.expect)(connections.has("localhost:7450")).toBe(true);
            (0, vitest_1.expect)(connections.has("10.0.0.1:7450")).toBe(true);
            (0, vitest_1.expect)(connections.has("10.0.0.2:7450")).toBe(true);
            // Now simulate pod restart: node-b gets new IP 10.0.0.3
            // The response comes from the initial server (localhost:7450)
            const updatedMock = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({
                    numShards: 2,
                    shardOwners: [
                        {
                            shardId: "00000000-0000-0000-0000-000000000001",
                            grpcAddr: "10.0.0.1:7450",
                            nodeId: "node-a",
                            rangeStart: "",
                            rangeEnd: "8000000000000000",
                        },
                        {
                            shardId: "00000000-0000-0000-0000-000000000002",
                            grpcAddr: "10.0.0.3:7450",
                            nodeId: "node-b",
                            rangeStart: "8000000000000000",
                            rangeEnd: "",
                        },
                    ],
                    thisNodeId: "node-a",
                    thisGrpcAddr: "10.0.0.1:7450",
                }),
            });
            // The refresh will try connections in order. Mock all of them to use the updated response.
            for (const [, c] of connections) {
                c.client.getClusterInfo = updatedMock;
            }
            await client.refreshTopology();
            // Old pod IP should be removed
            (0, vitest_1.expect)(connections.has("10.0.0.2:7450")).toBe(false);
            // New pod IP should be present
            (0, vitest_1.expect)(connections.has("10.0.0.3:7450")).toBe(true);
            // Initial server should be preserved (never cleaned up)
            (0, vitest_1.expect)(connections.has("localhost:7450")).toBe(true);
            // Still-active server should be preserved
            (0, vitest_1.expect)(connections.has("10.0.0.1:7450")).toBe(true);
        });
        (0, vitest_1.it)("preserves initial server connections even when not in topology response", async () => {
            // The initial DNS-based server might not appear in the topology
            // (the topology response uses pod IPs), but we should keep it
            const mockGetClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({
                    numShards: 1,
                    shardOwners: [
                        {
                            shardId: "00000000-0000-0000-0000-000000000001",
                            grpcAddr: "10.0.0.1:7450",
                            nodeId: "node-a",
                            rangeStart: "",
                            rangeEnd: "",
                        },
                    ],
                    thisNodeId: "node-a",
                    thisGrpcAddr: "10.0.0.1:7450",
                }),
            });
            const connections = client._connections;
            const conn = connections.values().next().value;
            conn.client.getClusterInfo = mockGetClusterInfo;
            await client.refreshTopology();
            // localhost:7450 is the initial server and should be kept even though
            // the topology only references 10.0.0.1:7450
            (0, vitest_1.expect)(connections.has("localhost:7450")).toBe(true);
            (0, vitest_1.expect)(connections.has("10.0.0.1:7450")).toBe(true);
        });
        (0, vitest_1.it)("closes transport on stale connections during cleanup", async () => {
            const mockGetClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({
                    numShards: 1,
                    shardOwners: [
                        {
                            shardId: "00000000-0000-0000-0000-000000000001",
                            grpcAddr: "10.0.0.1:7450",
                            nodeId: "node-a",
                            rangeStart: "",
                            rangeEnd: "",
                        },
                    ],
                    thisNodeId: "node-a",
                    thisGrpcAddr: "10.0.0.1:7450",
                }),
            });
            const connections = client._connections;
            const conn = connections.values().next().value;
            conn.client.getClusterInfo = mockGetClusterInfo;
            // First refresh: discover 10.0.0.1
            await client.refreshTopology();
            // Grab a reference to the old connection and spy on its transport.close
            const oldConn = connections.get("10.0.0.1:7450");
            const closeSpy = vitest_1.vi.spyOn(oldConn.transport, "close");
            // Second refresh: pod IP changed to 10.0.0.2
            const updatedMock = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({
                    numShards: 1,
                    shardOwners: [
                        {
                            shardId: "00000000-0000-0000-0000-000000000001",
                            grpcAddr: "10.0.0.2:7450",
                            nodeId: "node-a",
                            rangeStart: "",
                            rangeEnd: "",
                        },
                    ],
                    thisNodeId: "node-a",
                    thisGrpcAddr: "10.0.0.2:7450",
                }),
            });
            // Mock the initial server's getClusterInfo since it will be tried first
            const initialConn = connections.get("localhost:7450");
            initialConn.client.getClusterInfo = updatedMock;
            await client.refreshTopology();
            // The old connection's transport should have been closed
            (0, vitest_1.expect)(closeSpy).toHaveBeenCalled();
            (0, vitest_1.expect)(connections.has("10.0.0.1:7450")).toBe(false);
            (0, vitest_1.expect)(connections.has("10.0.0.2:7450")).toBe(true);
        });
    });
    (0, vitest_1.describe)("Connectivity error retry with topology refresh", () => {
        let client;
        (0, vitest_1.afterEach)(() => {
            client.close();
        });
        (0, vitest_1.it)("retries on a different server after UNAVAILABLE triggers topology refresh", async () => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:7450",
                useTls: false,
                shardRouting: {
                    maxRetries: 3,
                    retryDelayMs: 1,
                    topologyRefreshIntervalMs: 0,
                },
            });
            // Set up initial topology: shard → server-a (stale pod IP)
            client._shards = [
                {
                    shardId: "00000000-0000-0000-0000-000000000001",
                    serverAddr: "10.0.0.1:7450",
                    rangeStart: "",
                    rangeEnd: "",
                },
            ];
            client._shardToServer.set("00000000-0000-0000-0000-000000000001", "10.0.0.1:7450");
            client._topologyReady = true;
            const connections = client._connections;
            // Create connection to stale server-a that will return UNAVAILABLE
            const staleConn = client._getOrCreateConnection("10.0.0.1:7450");
            staleConn.client.enqueue = vitest_1.vi.fn().mockImplementation(() => {
                throw new runtime_rpc_1.RpcError("connect ETIMEDOUT 10.0.0.1:7450", "UNAVAILABLE", {});
            });
            // Mock refreshTopology to simulate discovering the shard moved to server-b
            let refreshCalled = false;
            const originalRefresh = client.refreshTopology.bind(client);
            client.refreshTopology = vitest_1.vi.fn().mockImplementation(async () => {
                refreshCalled = true;
                // Update topology: shard now lives on server-b (new pod IP)
                client._shards = [
                    {
                        shardId: "00000000-0000-0000-0000-000000000001",
                        serverAddr: "10.0.0.2:7450",
                        rangeStart: "",
                        rangeEnd: "",
                    },
                ];
                client._shardToServer.set("00000000-0000-0000-0000-000000000001", "10.0.0.2:7450");
                // Create connection to new server-b that succeeds
                const newConn = client._getOrCreateConnection("10.0.0.2:7450");
                newConn.client.enqueue = vitest_1.vi.fn().mockReturnValue({
                    response: Promise.resolve({ id: "job-success" }),
                });
            });
            const handle = await client.enqueue({
                tenant: "test-tenant",
                payload: { test: true },
                taskGroup: "default",
            });
            (0, vitest_1.expect)(handle.id).toBe("job-success");
            (0, vitest_1.expect)(refreshCalled).toBe(true);
            // The stale server should have been tried exactly once before refreshing
            (0, vitest_1.expect)(staleConn.client.enqueue).toHaveBeenCalledTimes(1);
        });
        (0, vitest_1.it)("retries reportOutcome on a different server after UNAVAILABLE triggers topology refresh", async () => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:7450",
                useTls: false,
                shardRouting: {
                    maxRetries: 3,
                    retryDelayMs: 1,
                    topologyRefreshIntervalMs: 0,
                },
            });
            const SHARD_ID = "00000000-0000-0000-0000-000000000001";
            // Set up initial topology: shard → server-a
            client._shards = [
                {
                    shardId: SHARD_ID,
                    serverAddr: "10.0.0.1:7450",
                    rangeStart: "",
                    rangeEnd: "",
                },
            ];
            client._shardToServer.set(SHARD_ID, "10.0.0.1:7450");
            client._topologyReady = true;
            // Create stale connection that returns UNAVAILABLE
            const staleConn = client._getOrCreateConnection("10.0.0.1:7450");
            staleConn.client.reportOutcome = vitest_1.vi.fn().mockImplementation(() => {
                throw new runtime_rpc_1.RpcError("connect ETIMEDOUT 10.0.0.1:7450", "UNAVAILABLE", {});
            });
            // Mock refreshTopology to point shard to server-b
            client.refreshTopology = vitest_1.vi.fn().mockImplementation(async () => {
                client._shardToServer.set(SHARD_ID, "10.0.0.2:7450");
                const newConn = client._getOrCreateConnection("10.0.0.2:7450");
                newConn.client.reportOutcome = vitest_1.vi.fn().mockReturnValue({
                    response: Promise.resolve({}),
                });
            });
            await client.reportOutcome({
                taskId: "task-1",
                shard: SHARD_ID,
                outcome: { type: "success", result: { ok: true } },
            });
            (0, vitest_1.expect)(client.refreshTopology).toHaveBeenCalled();
            (0, vitest_1.expect)(staleConn.client.reportOutcome).toHaveBeenCalledTimes(1);
        });
        (0, vitest_1.it)("retries shard-routed leaseTasks on a different server after UNAVAILABLE", async () => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:7450",
                useTls: false,
                shardRouting: {
                    maxRetries: 3,
                    retryDelayMs: 1,
                    topologyRefreshIntervalMs: 0,
                },
            });
            const SHARD_ID = "00000000-0000-0000-0000-000000000001";
            // Set up initial topology: shard → server-a
            client._shards = [
                {
                    shardId: SHARD_ID,
                    serverAddr: "10.0.0.1:7450",
                    rangeStart: "",
                    rangeEnd: "",
                },
            ];
            client._shardToServer.set(SHARD_ID, "10.0.0.1:7450");
            client._topologyReady = true;
            // Create stale connection that returns UNAVAILABLE
            const staleConn = client._getOrCreateConnection("10.0.0.1:7450");
            staleConn.client.leaseTasks = vitest_1.vi.fn().mockImplementation(() => {
                throw new runtime_rpc_1.RpcError("connect ETIMEDOUT 10.0.0.1:7450", "UNAVAILABLE", {});
            });
            // Mock refreshTopology to point shard to server-b
            client.refreshTopology = vitest_1.vi.fn().mockImplementation(async () => {
                client._shardToServer.set(SHARD_ID, "10.0.0.2:7450");
                const newConn = client._getOrCreateConnection("10.0.0.2:7450");
                newConn.client.leaseTasks = vitest_1.vi.fn().mockReturnValue({
                    response: Promise.resolve({ tasks: [], refreshTasks: [] }),
                });
            });
            const result = await client.leaseTasks({
                shard: SHARD_ID,
                workerId: "worker-1",
                maxTasks: 1,
                taskGroup: "default",
            });
            (0, vitest_1.expect)(result.tasks).toEqual([]);
            (0, vitest_1.expect)(client.refreshTopology).toHaveBeenCalled();
            (0, vitest_1.expect)(staleConn.client.leaseTasks).toHaveBeenCalledTimes(1);
        });
        (0, vitest_1.it)("retries round-robin leaseTasks on a different server after UNAVAILABLE", async () => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:7450",
                useTls: false,
                shardRouting: {
                    maxRetries: 3,
                    retryDelayMs: 1,
                    topologyRefreshIntervalMs: 0,
                },
            });
            // Set up topology with two servers
            client._shards = [
                {
                    shardId: "00000000-0000-0000-0000-000000000001",
                    serverAddr: "10.0.0.1:7450",
                    rangeStart: "",
                    rangeEnd: "8",
                },
                {
                    shardId: "00000000-0000-0000-0000-000000000002",
                    serverAddr: "10.0.0.2:7450",
                    rangeStart: "8",
                    rangeEnd: "",
                },
            ];
            client._shardToServer.set("00000000-0000-0000-0000-000000000001", "10.0.0.1:7450");
            client._shardToServer.set("00000000-0000-0000-0000-000000000002", "10.0.0.2:7450");
            client._topologyReady = true;
            // Clear the initial localhost connection so round-robin only sees our test servers
            client._connections.clear();
            // First server is dead
            const deadConn = client._getOrCreateConnection("10.0.0.1:7450");
            deadConn.client.leaseTasks = vitest_1.vi.fn().mockImplementation(() => {
                throw new runtime_rpc_1.RpcError("connect ETIMEDOUT", "UNAVAILABLE", {});
            });
            // Second server is healthy
            const healthyConn = client._getOrCreateConnection("10.0.0.2:7450");
            healthyConn.client.leaseTasks = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({ tasks: [], refreshTasks: [] }),
            });
            // Mock refreshTopology as no-op (topology is already correct, just need round-robin to advance)
            client.refreshTopology = vitest_1.vi.fn().mockResolvedValue(undefined);
            // Force round-robin to start at the dead server
            client._anyClientCounter = 0;
            const result = await client.leaseTasks({
                workerId: "worker-1",
                maxTasks: 1,
                taskGroup: "default",
            });
            (0, vitest_1.expect)(result.tasks).toEqual([]);
            (0, vitest_1.expect)(client.refreshTopology).toHaveBeenCalled();
            (0, vitest_1.expect)(deadConn.client.leaseTasks).toHaveBeenCalledTimes(1);
        });
        (0, vitest_1.it)("gives up after maxRetries even with UNAVAILABLE errors", async () => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:7450",
                useTls: false,
                shardRouting: {
                    maxRetries: 2,
                    retryDelayMs: 1,
                    topologyRefreshIntervalMs: 0,
                },
            });
            // Set up topology
            client._shards = [
                {
                    shardId: "00000000-0000-0000-0000-000000000001",
                    serverAddr: "10.0.0.1:7450",
                    rangeStart: "",
                    rangeEnd: "",
                },
            ];
            client._shardToServer.set("00000000-0000-0000-0000-000000000001", "10.0.0.1:7450");
            client._topologyReady = true;
            // Every server always returns UNAVAILABLE
            const staleConn = client._getOrCreateConnection("10.0.0.1:7450");
            staleConn.client.enqueue = vitest_1.vi.fn().mockImplementation(() => {
                throw new runtime_rpc_1.RpcError("connect ETIMEDOUT", "UNAVAILABLE", {});
            });
            // Topology refresh doesn't help — same broken server
            client.refreshTopology = vitest_1.vi.fn().mockResolvedValue(undefined);
            await (0, vitest_1.expect)(client.enqueue({
                tenant: "test-tenant",
                payload: { test: true },
                taskGroup: "default",
            })).rejects.toThrow("connect ETIMEDOUT");
            // Initial call + 2 retries = 3 total attempts
            (0, vitest_1.expect)(staleConn.client.enqueue).toHaveBeenCalledTimes(3);
        });
        (0, vitest_1.it)("does not retry non-retryable errors like INVALID_ARGUMENT", async () => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:7450",
                useTls: false,
                shardRouting: {
                    maxRetries: 3,
                    retryDelayMs: 1,
                    topologyRefreshIntervalMs: 0,
                },
            });
            client._shards = [
                {
                    shardId: "00000000-0000-0000-0000-000000000001",
                    serverAddr: "localhost:7450",
                    rangeStart: "",
                    rangeEnd: "",
                },
            ];
            client._shardToServer.set("00000000-0000-0000-0000-000000000001", "localhost:7450");
            client._topologyReady = true;
            const connections = client._connections;
            const conn = connections.values().next().value;
            conn.client.enqueue = vitest_1.vi.fn().mockImplementation(() => {
                throw new runtime_rpc_1.RpcError("invalid payload", "INVALID_ARGUMENT", {});
            });
            await (0, vitest_1.expect)(client.enqueue({
                tenant: "test-tenant",
                payload: { test: true },
                taskGroup: "default",
            })).rejects.toThrow("invalid payload");
            // Should NOT retry — thrown immediately
            (0, vitest_1.expect)(conn.client.enqueue).toHaveBeenCalledTimes(1);
        });
    });
    (0, vitest_1.describe)("gRPC retry configuration", () => {
        (0, vitest_1.it)("configures gRPC-level retries for transient failures", () => {
            const client = new client_1.SiloGRPCClient({
                servers: "localhost:7450",
                useTls: false,
                shardRouting: {
                    topologyRefreshIntervalMs: 0,
                },
            });
            try {
                // Access the internal grpc options to verify retry config is set
                const grpcOptions = client._grpcClientOptions;
                (0, vitest_1.expect)(grpcOptions["grpc.enable_retries"]).toBe(1);
                (0, vitest_1.expect)(grpcOptions["grpc.service_config"]).toBeDefined();
                const serviceConfig = JSON.parse(grpcOptions["grpc.service_config"]);
                (0, vitest_1.expect)(serviceConfig.methodConfig).toBeDefined();
                (0, vitest_1.expect)(serviceConfig.methodConfig[0].retryPolicy).toBeDefined();
                // UNAVAILABLE is NOT retried at the gRPC level — our application-level
                // retry handles it with topology refresh for smarter re-routing
                (0, vitest_1.expect)(serviceConfig.methodConfig[0].retryPolicy.retryableStatusCodes).not.toContain("UNAVAILABLE");
                (0, vitest_1.expect)(serviceConfig.methodConfig[0].retryPolicy.retryableStatusCodes).toContain("RESOURCE_EXHAUSTED");
            }
            finally {
                client.close();
            }
        });
        (0, vitest_1.it)("allows overriding gRPC options", () => {
            const customServiceConfig = {
                methodConfig: [
                    {
                        name: [{ service: "silo.v1.Silo" }],
                        retryPolicy: {
                            maxAttempts: 10,
                            initialBackoff: "0.5s",
                            maxBackoff: "30s",
                            backoffMultiplier: 1.5,
                            retryableStatusCodes: ["UNAVAILABLE"],
                        },
                    },
                ],
            };
            const client = new client_1.SiloGRPCClient({
                servers: "localhost:7450",
                useTls: false,
                shardRouting: {
                    topologyRefreshIntervalMs: 0,
                },
                grpcClientOptions: {
                    "grpc.service_config": JSON.stringify(customServiceConfig),
                },
            });
            try {
                const grpcOptions = client._grpcClientOptions;
                const serviceConfig = JSON.parse(grpcOptions["grpc.service_config"]);
                (0, vitest_1.expect)(serviceConfig.methodConfig[0].retryPolicy.maxAttempts).toBe(10);
            }
            finally {
                client.close();
            }
        });
    });
    (0, vitest_1.describe)("Wrong Shard Retry", () => {
        let client;
        // Helper to set up mock topology so shard resolution works
        const setupMockTopology = (c) => {
            c._shards = [
                {
                    shardId: "00000000-0000-0000-0000-000000000001",
                    serverAddr: "localhost:7450",
                    rangeStart: "",
                    rangeEnd: "",
                },
            ];
            c._shardToServer.set("00000000-0000-0000-0000-000000000001", "localhost:7450");
            c._topologyReady = true;
        };
        (0, vitest_1.beforeEach)(() => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:7450",
                useTls: false,
                shardRouting: {
                    maxRetries: 3,
                    retryDelayMs: 1, // Fast retries for tests
                    topologyRefreshIntervalMs: 0,
                },
            });
            // Set up mock topology so shard resolution works
            setupMockTopology(client);
        });
        (0, vitest_1.afterEach)(() => {
            client.close();
        });
        (0, vitest_1.describe)("error classification", () => {
            (0, vitest_1.it)("NOT_FOUND with 'shard not found' is a wrong shard error", () => {
                const error = new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND", {});
                (0, vitest_1.expect)(error.code).toBe("NOT_FOUND");
                (0, vitest_1.expect)(error.message).toContain("shard not found");
            });
            (0, vitest_1.it)("NOT_FOUND with different message is not a wrong shard error", () => {
                const error = new runtime_rpc_1.RpcError("job not found", "NOT_FOUND", {});
                (0, vitest_1.expect)(error.message).not.toContain("shard not found");
            });
            (0, vitest_1.it)("other error codes are not wrong shard errors", () => {
                const error = new runtime_rpc_1.RpcError("internal error", "INTERNAL", {});
                (0, vitest_1.expect)(error.code).not.toBe("NOT_FOUND");
            });
            (0, vitest_1.it)("extracts redirect address from error metadata", () => {
                const error = new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND", {
                    "x-silo-shard-owner-addr": "other-server:7450",
                    "x-silo-shard-owner-node": "node-2",
                });
                (0, vitest_1.expect)(error.meta?.["x-silo-shard-owner-addr"]).toBe("other-server:7450");
                (0, vitest_1.expect)(error.meta?.["x-silo-shard-owner-node"]).toBe("node-2");
            });
        });
        (0, vitest_1.describe)("retry behavior", () => {
            (0, vitest_1.it)("retries on a new shard when it gets a wrong shard error and succeeds", async () => {
                let callCount = 0;
                const mockEnqueue = vitest_1.vi.fn().mockImplementation(() => {
                    callCount++;
                    if (callCount === 1) {
                        throw new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND", {
                            "x-silo-shard-owner-addr": "localhost:7450",
                        });
                    }
                    return {
                        response: Promise.resolve({ id: "job-123" }),
                    };
                });
                const connections = client._connections;
                const conn = connections.values().next().value;
                conn.client.enqueue = mockEnqueue;
                client.refreshTopology = vitest_1.vi.fn().mockResolvedValue(undefined);
                const handle = await client.enqueue({
                    tenant: "test-tenant",
                    payload: { test: true },
                    taskGroup: "default",
                });
                (0, vitest_1.expect)(handle.id).toBe("job-123");
                (0, vitest_1.expect)(mockEnqueue).toHaveBeenCalledTimes(2);
            });
            (0, vitest_1.it)("stops retrying after maxRetries", async () => {
                const mockEnqueue = vitest_1.vi.fn().mockImplementation(() => {
                    throw new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND", {});
                });
                const connections = client._connections;
                const conn = connections.values().next().value;
                conn.client.enqueue = mockEnqueue;
                client.refreshTopology = vitest_1.vi.fn().mockResolvedValue(undefined);
                await (0, vitest_1.expect)(client.enqueue({
                    tenant: "test-tenant",
                    payload: { test: true },
                    taskGroup: "default",
                })).rejects.toThrow("shard not found");
                // Initial call + 3 retries = 4 total calls
                (0, vitest_1.expect)(mockEnqueue).toHaveBeenCalledTimes(4);
            });
            (0, vitest_1.it)("does not retry on non-wrong-shard errors", async () => {
                const mockEnqueue = vitest_1.vi.fn().mockImplementation(() => {
                    throw new runtime_rpc_1.RpcError("internal server error", "INTERNAL", {});
                });
                const connections = client._connections;
                const conn = connections.values().next().value;
                conn.client.enqueue = mockEnqueue;
                await (0, vitest_1.expect)(client.enqueue({
                    tenant: "test-tenant",
                    payload: { test: true },
                    taskGroup: "default",
                })).rejects.toThrow("internal server error");
                (0, vitest_1.expect)(mockEnqueue).toHaveBeenCalledTimes(1);
            });
            (0, vitest_1.it)("creates new connection when redirect points to different server", async () => {
                let callCount = 0;
                const mockEnqueue = vitest_1.vi.fn().mockImplementation(() => {
                    callCount++;
                    if (callCount === 1) {
                        throw new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND", {
                            "x-silo-shard-owner-addr": "new-server:7450",
                        });
                    }
                    return {
                        response: Promise.resolve({ id: "job-456" }),
                    };
                });
                const connections = client._connections;
                const conn = connections.values().next().value;
                conn.client.enqueue = mockEnqueue;
                const originalGetOrCreate = client._getOrCreateConnection.bind(client);
                const getOrCreateSpy = vitest_1.vi.fn().mockImplementation((addr) => {
                    const newConn = originalGetOrCreate(addr);
                    newConn.client.enqueue = mockEnqueue;
                    return newConn;
                });
                client._getOrCreateConnection = getOrCreateSpy;
                const handle = await client.enqueue({
                    tenant: "test-tenant",
                    payload: { test: true },
                    taskGroup: "default",
                });
                (0, vitest_1.expect)(handle.id).toBe("job-456");
                (0, vitest_1.expect)(getOrCreateSpy).toHaveBeenCalledWith("new-server:7450");
            });
        });
        (0, vitest_1.describe)("topology refresh on retry", () => {
            (0, vitest_1.it)("refreshes topology when no redirect metadata is provided", async () => {
                let callCount = 0;
                const mockEnqueue = vitest_1.vi.fn().mockImplementation(() => {
                    callCount++;
                    if (callCount === 1) {
                        throw new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND", {});
                    }
                    return {
                        response: Promise.resolve({ id: "job-789" }),
                    };
                });
                const connections = client._connections;
                const conn = connections.values().next().value;
                conn.client.enqueue = mockEnqueue;
                const refreshSpy = vitest_1.vi.fn().mockResolvedValue(undefined);
                client.refreshTopology = refreshSpy;
                const handle = await client.enqueue({
                    tenant: "test-tenant",
                    payload: { test: true },
                    taskGroup: "default",
                });
                (0, vitest_1.expect)(handle.id).toBe("job-789");
                (0, vitest_1.expect)(refreshSpy).toHaveBeenCalled();
            });
            (0, vitest_1.it)("does not refresh topology when redirect metadata is provided", async () => {
                let callCount = 0;
                const mockEnqueue = vitest_1.vi.fn().mockImplementation(() => {
                    callCount++;
                    if (callCount === 1) {
                        throw new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND", {
                            "x-silo-shard-owner-addr": "localhost:7450",
                        });
                    }
                    return {
                        response: Promise.resolve({ id: "job-abc" }),
                    };
                });
                const connections = client._connections;
                const conn = connections.values().next().value;
                conn.client.enqueue = mockEnqueue;
                const refreshSpy = vitest_1.vi.fn().mockResolvedValue(undefined);
                client.refreshTopology = refreshSpy;
                const handle = await client.enqueue({
                    tenant: "test-tenant",
                    payload: { test: true },
                    taskGroup: "default",
                });
                (0, vitest_1.expect)(handle.id).toBe("job-abc");
                (0, vitest_1.expect)(refreshSpy).not.toHaveBeenCalled();
            });
        });
    });
    (0, vitest_1.describe)("Wrong Shard Retry for shard-routed operations", () => {
        let client;
        const setupMockTopology = (c) => {
            c._shards = [
                {
                    shardId: "00000000-0000-0000-0000-000000000001",
                    serverAddr: "localhost:7450",
                    rangeStart: "",
                    rangeEnd: "",
                },
            ];
            c._shardToServer.set("00000000-0000-0000-0000-000000000001", "localhost:7450");
            c._topologyReady = true;
        };
        const SHARD_ID = "00000000-0000-0000-0000-000000000001";
        (0, vitest_1.beforeEach)(() => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:7450",
                useTls: false,
                shardRouting: {
                    maxRetries: 3,
                    retryDelayMs: 1,
                    topologyRefreshIntervalMs: 0,
                },
            });
            setupMockTopology(client);
        });
        (0, vitest_1.afterEach)(() => {
            client.close();
        });
        (0, vitest_1.describe)("reportOutcome", () => {
            (0, vitest_1.it)("retries on wrong shard error with redirect metadata", async () => {
                let callCount = 0;
                const mockReportOutcome = vitest_1.vi.fn().mockImplementation(() => {
                    callCount++;
                    if (callCount === 1) {
                        throw new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND", {
                            "x-silo-shard-owner-addr": "localhost:7450",
                        });
                    }
                    return { response: Promise.resolve({}) };
                });
                const connections = client._connections;
                const conn = connections.values().next().value;
                conn.client.reportOutcome = mockReportOutcome;
                client.refreshTopology = vitest_1.vi.fn().mockResolvedValue(undefined);
                await client.reportOutcome({
                    taskId: "task-1",
                    shard: SHARD_ID,
                    outcome: { type: "success", result: { ok: true } },
                });
                (0, vitest_1.expect)(mockReportOutcome).toHaveBeenCalledTimes(2);
            });
            (0, vitest_1.it)("retries on wrong shard error without redirect metadata (triggers topology refresh)", async () => {
                let callCount = 0;
                const mockReportOutcome = vitest_1.vi.fn().mockImplementation(() => {
                    callCount++;
                    if (callCount === 1) {
                        throw new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND", {});
                    }
                    return { response: Promise.resolve({}) };
                });
                const connections = client._connections;
                const conn = connections.values().next().value;
                conn.client.reportOutcome = mockReportOutcome;
                const refreshSpy = vitest_1.vi.fn().mockResolvedValue(undefined);
                client.refreshTopology = refreshSpy;
                await client.reportOutcome({
                    taskId: "task-1",
                    shard: SHARD_ID,
                    outcome: { type: "success", result: { ok: true } },
                });
                (0, vitest_1.expect)(mockReportOutcome).toHaveBeenCalledTimes(2);
                (0, vitest_1.expect)(refreshSpy).toHaveBeenCalled();
            });
            (0, vitest_1.it)("stops retrying after maxRetries", async () => {
                const mockReportOutcome = vitest_1.vi.fn().mockImplementation(() => {
                    throw new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND", {});
                });
                const connections = client._connections;
                const conn = connections.values().next().value;
                conn.client.reportOutcome = mockReportOutcome;
                client.refreshTopology = vitest_1.vi.fn().mockResolvedValue(undefined);
                await (0, vitest_1.expect)(client.reportOutcome({
                    taskId: "task-1",
                    shard: SHARD_ID,
                    outcome: { type: "success", result: { ok: true } },
                })).rejects.toThrow();
                // Initial call + 3 retries = 4 total
                (0, vitest_1.expect)(mockReportOutcome).toHaveBeenCalledTimes(4);
            });
            (0, vitest_1.it)("does not retry on non-wrong-shard errors", async () => {
                const mockReportOutcome = vitest_1.vi.fn().mockImplementation(() => {
                    throw new runtime_rpc_1.RpcError("internal error", "INTERNAL", {});
                });
                const connections = client._connections;
                const conn = connections.values().next().value;
                conn.client.reportOutcome = mockReportOutcome;
                await (0, vitest_1.expect)(client.reportOutcome({
                    taskId: "task-1",
                    shard: SHARD_ID,
                    outcome: { type: "success", result: { ok: true } },
                })).rejects.toThrow("internal error");
                (0, vitest_1.expect)(mockReportOutcome).toHaveBeenCalledTimes(1);
            });
        });
        (0, vitest_1.describe)("heartbeat", () => {
            (0, vitest_1.it)("retries on wrong shard error with redirect metadata", async () => {
                let callCount = 0;
                const mockHeartbeat = vitest_1.vi.fn().mockImplementation(() => {
                    callCount++;
                    if (callCount === 1) {
                        throw new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND", {
                            "x-silo-shard-owner-addr": "localhost:7450",
                        });
                    }
                    return { response: Promise.resolve({ cancelled: false }) };
                });
                const connections = client._connections;
                const conn = connections.values().next().value;
                conn.client.heartbeat = mockHeartbeat;
                client.refreshTopology = vitest_1.vi.fn().mockResolvedValue(undefined);
                const result = await client.heartbeat("worker-1", "task-1", SHARD_ID);
                (0, vitest_1.expect)(result.cancelled).toBe(false);
                (0, vitest_1.expect)(mockHeartbeat).toHaveBeenCalledTimes(2);
            });
            (0, vitest_1.it)("stops retrying after maxRetries", async () => {
                const mockHeartbeat = vitest_1.vi.fn().mockImplementation(() => {
                    throw new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND", {});
                });
                const connections = client._connections;
                const conn = connections.values().next().value;
                conn.client.heartbeat = mockHeartbeat;
                client.refreshTopology = vitest_1.vi.fn().mockResolvedValue(undefined);
                await (0, vitest_1.expect)(client.heartbeat("worker-1", "task-1", SHARD_ID)).rejects.toThrow();
                (0, vitest_1.expect)(mockHeartbeat).toHaveBeenCalledTimes(4);
            });
        });
        (0, vitest_1.describe)("reportRefreshOutcome", () => {
            (0, vitest_1.it)("retries on wrong shard error with redirect metadata", async () => {
                let callCount = 0;
                const mockReportRefreshOutcome = vitest_1.vi.fn().mockImplementation(() => {
                    callCount++;
                    if (callCount === 1) {
                        throw new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND", {
                            "x-silo-shard-owner-addr": "localhost:7450",
                        });
                    }
                    return { response: Promise.resolve({}) };
                });
                const connections = client._connections;
                const conn = connections.values().next().value;
                conn.client.reportRefreshOutcome = mockReportRefreshOutcome;
                client.refreshTopology = vitest_1.vi.fn().mockResolvedValue(undefined);
                await client.reportRefreshOutcome({
                    taskId: "task-1",
                    shard: SHARD_ID,
                    outcome: { type: "success", newMaxConcurrency: 10 },
                });
                (0, vitest_1.expect)(mockReportRefreshOutcome).toHaveBeenCalledTimes(2);
            });
            (0, vitest_1.it)("stops retrying after maxRetries", async () => {
                const mockReportRefreshOutcome = vitest_1.vi.fn().mockImplementation(() => {
                    throw new runtime_rpc_1.RpcError("shard not found", "NOT_FOUND", {});
                });
                const connections = client._connections;
                const conn = connections.values().next().value;
                conn.client.reportRefreshOutcome = mockReportRefreshOutcome;
                client.refreshTopology = vitest_1.vi.fn().mockResolvedValue(undefined);
                await (0, vitest_1.expect)(client.reportRefreshOutcome({
                    taskId: "task-1",
                    shard: SHARD_ID,
                    outcome: { type: "success", newMaxConcurrency: 10 },
                })).rejects.toThrow();
                (0, vitest_1.expect)(mockReportRefreshOutcome).toHaveBeenCalledTimes(4);
            });
        });
    });
    (0, vitest_1.describe)("Lazy topology auto-refresh", () => {
        let client;
        (0, vitest_1.afterEach)(() => {
            client.close();
        });
        // Helper: create a client and mock getClusterInfo to return a single shard
        const setupClientWithMockTopology = () => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:7450",
                useTls: false,
                shardRouting: {
                    topologyRefreshIntervalMs: 0,
                },
            });
            const mockGetClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({
                    numShards: 1,
                    shardOwners: [
                        {
                            shardId: "00000000-0000-0000-0000-000000000001",
                            grpcAddr: "localhost:7450",
                            nodeId: "node-1",
                            rangeStart: "",
                            rangeEnd: "",
                        },
                    ],
                    thisNodeId: "node-1",
                    thisGrpcAddr: "localhost:7450",
                }),
            });
            const mockEnqueue = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({ id: "job-123" }),
            });
            const connections = client._connections;
            const conn = connections.values().next().value;
            conn.client.getClusterInfo = mockGetClusterInfo;
            conn.client.enqueue = mockEnqueue;
            return { mockGetClusterInfo, mockEnqueue };
        };
        (0, vitest_1.it)("auto-refreshes topology on first enqueue if refreshTopology() was never called", async () => {
            const { mockGetClusterInfo, mockEnqueue } = setupClientWithMockTopology();
            // Don't call refreshTopology() - just call enqueue directly
            const handle = await client.enqueue({
                tenant: "test-tenant",
                payload: { test: true },
                taskGroup: "default",
            });
            (0, vitest_1.expect)(handle.id).toBe("job-123");
            // Should have called getClusterInfo to auto-discover topology
            (0, vitest_1.expect)(mockGetClusterInfo).toHaveBeenCalled();
            (0, vitest_1.expect)(mockEnqueue).toHaveBeenCalled();
        });
        (0, vitest_1.it)("does not re-fetch topology if it was already loaded", async () => {
            const { mockGetClusterInfo, mockEnqueue } = setupClientWithMockTopology();
            // Explicitly refresh topology first
            await client.refreshTopology();
            (0, vitest_1.expect)(mockGetClusterInfo).toHaveBeenCalledTimes(1);
            // Now enqueue should not trigger another refresh
            await client.enqueue({
                tenant: "test-tenant",
                payload: { test: true },
                taskGroup: "default",
            });
            (0, vitest_1.expect)(mockGetClusterInfo).toHaveBeenCalledTimes(1);
            (0, vitest_1.expect)(mockEnqueue).toHaveBeenCalled();
        });
        (0, vitest_1.it)("deduplicates concurrent topology refreshes", async () => {
            const { mockGetClusterInfo } = setupClientWithMockTopology();
            const mockEnqueue = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({ id: "job-concurrent" }),
            });
            const connections = client._connections;
            const conn = connections.values().next().value;
            conn.client.enqueue = mockEnqueue;
            // Launch 5 concurrent enqueues without having called refreshTopology
            const promises = Array.from({ length: 5 }, (_, i) => client.enqueue({
                tenant: "test-tenant",
                payload: { index: i },
                taskGroup: "default",
            }));
            const results = await Promise.all(promises);
            // All should succeed
            (0, vitest_1.expect)(results).toHaveLength(5);
            for (const handle of results) {
                (0, vitest_1.expect)(handle.id).toBe("job-concurrent");
            }
            // getClusterInfo should have been called exactly once (deduped)
            (0, vitest_1.expect)(mockGetClusterInfo).toHaveBeenCalledTimes(1);
        });
        (0, vitest_1.it)("resetShards puts client back into lazy-refresh mode", async () => {
            const { mockGetClusterInfo } = setupClientWithMockTopology();
            const mockEnqueue = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({ id: "job-after-reset" }),
            });
            const mockResetShards = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({}),
            });
            const connections = client._connections;
            const conn = connections.values().next().value;
            conn.client.enqueue = mockEnqueue;
            conn.client.resetShards = mockResetShards;
            // First, refresh topology and do an enqueue
            await client.refreshTopology();
            (0, vitest_1.expect)(mockGetClusterInfo).toHaveBeenCalledTimes(1);
            await client.enqueue({
                tenant: "test-tenant",
                payload: { test: true },
                taskGroup: "default",
            });
            // No additional refresh needed
            (0, vitest_1.expect)(mockGetClusterInfo).toHaveBeenCalledTimes(1);
            // Now reset shards - this should clear topology state
            await client.resetShards();
            // Next enqueue should trigger a new topology refresh
            await client.enqueue({
                tenant: "test-tenant",
                payload: { test: true },
                taskGroup: "default",
            });
            // Should have refreshed topology again
            (0, vitest_1.expect)(mockGetClusterInfo).toHaveBeenCalledTimes(2);
        });
    });
});
//# sourceMappingURL=shard-routing.test.js.map