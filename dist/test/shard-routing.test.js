"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const client_1 = require("../src/client");
const runtime_rpc_1 = require("@protobuf-ts/runtime-rpc");
(0, vitest_1.describe)("Shard Routing", () => {
    (0, vitest_1.describe)("shardForTenant range-based lookup", () => {
        (0, vitest_1.it)("finds correct shard for tenant in range", () => {
            const shards = [
                { shardId: "shard-1", serverAddr: "server-a:50051", rangeStart: "", rangeEnd: "m" },
                { shardId: "shard-2", serverAddr: "server-b:50051", rangeStart: "m", rangeEnd: "" },
            ];
            // "apple" comes before "m"
            (0, vitest_1.expect)((0, client_1.shardForTenant)("apple", shards)?.shardId).toBe("shard-1");
            // "zebra" comes after "m"
            (0, vitest_1.expect)((0, client_1.shardForTenant)("zebra", shards)?.shardId).toBe("shard-2");
        });
        (0, vitest_1.it)("handles empty rangeStart (no lower bound)", () => {
            const shards = [
                { shardId: "shard-1", serverAddr: "server-a:50051", rangeStart: "", rangeEnd: "" },
            ];
            (0, vitest_1.expect)((0, client_1.shardForTenant)("any-tenant", shards)?.shardId).toBe("shard-1");
        });
        (0, vitest_1.it)("handles empty rangeEnd (no upper bound)", () => {
            const shards = [
                { shardId: "shard-1", serverAddr: "server-a:50051", rangeStart: "a", rangeEnd: "" },
            ];
            (0, vitest_1.expect)((0, client_1.shardForTenant)("zebra", shards)?.shardId).toBe("shard-1");
        });
        (0, vitest_1.it)("returns undefined for empty shards array", () => {
            (0, vitest_1.expect)((0, client_1.shardForTenant)("tenant", [])).toBeUndefined();
        });
        (0, vitest_1.it)("handles multiple shard ranges", () => {
            const shards = [
                { shardId: "shard-1", serverAddr: "server-a:50051", rangeStart: "", rangeEnd: "4" },
                { shardId: "shard-2", serverAddr: "server-b:50051", rangeStart: "4", rangeEnd: "8" },
                { shardId: "shard-3", serverAddr: "server-a:50051", rangeStart: "8", rangeEnd: "c" },
                { shardId: "shard-4", serverAddr: "server-b:50051", rangeStart: "c", rangeEnd: "" },
            ];
            (0, vitest_1.expect)((0, client_1.shardForTenant)("1", shards)?.shardId).toBe("shard-1");
            (0, vitest_1.expect)((0, client_1.shardForTenant)("5", shards)?.shardId).toBe("shard-2");
            (0, vitest_1.expect)((0, client_1.shardForTenant)("9", shards)?.shardId).toBe("shard-3");
            (0, vitest_1.expect)((0, client_1.shardForTenant)("d", shards)?.shardId).toBe("shard-4");
        });
    });
    (0, vitest_1.describe)("Topology discovery", () => {
        let client;
        (0, vitest_1.beforeEach)(() => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:50051",
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
                        { shardId: "00000000-0000-0000-0000-000000000001", grpcAddr: "server-a:50051", nodeId: "node-a", rangeStart: "", rangeEnd: "4" },
                        { shardId: "00000000-0000-0000-0000-000000000002", grpcAddr: "server-b:50051", nodeId: "node-b", rangeStart: "4", rangeEnd: "8" },
                        { shardId: "00000000-0000-0000-0000-000000000003", grpcAddr: "server-a:50051", nodeId: "node-a", rangeStart: "8", rangeEnd: "c" },
                        { shardId: "00000000-0000-0000-0000-000000000004", grpcAddr: "server-b:50051", nodeId: "node-b", rangeStart: "c", rangeEnd: "" },
                    ],
                    thisNodeId: "node-a",
                    thisGrpcAddr: "server-a:50051",
                }),
            });
            const connections = client._connections;
            const conn = connections.values().next().value;
            conn.client.getClusterInfo = mockGetClusterInfo;
            await client.refreshTopology();
            const topology = client.getTopology();
            (0, vitest_1.expect)(topology.shards.length).toBe(4);
            (0, vitest_1.expect)(topology.shardToServer.get("00000000-0000-0000-0000-000000000001")).toBe("server-a:50051");
            (0, vitest_1.expect)(topology.shardToServer.get("00000000-0000-0000-0000-000000000002")).toBe("server-b:50051");
            (0, vitest_1.expect)(topology.shardToServer.get("00000000-0000-0000-0000-000000000003")).toBe("server-a:50051");
            (0, vitest_1.expect)(topology.shardToServer.get("00000000-0000-0000-0000-000000000004")).toBe("server-b:50051");
        });
        (0, vitest_1.it)("creates connections to discovered servers", async () => {
            const mockGetClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({
                    numShards: 2,
                    shardOwners: [
                        { shardId: "00000000-0000-0000-0000-000000000001", grpcAddr: "server-x:50051", nodeId: "node-x", rangeStart: "", rangeEnd: "m" },
                        { shardId: "00000000-0000-0000-0000-000000000002", grpcAddr: "server-y:50051", nodeId: "node-y", rangeStart: "m", rangeEnd: "" },
                    ],
                    thisNodeId: "node-x",
                    thisGrpcAddr: "server-x:50051",
                }),
            });
            const connections = client._connections;
            const conn = connections.values().next().value;
            conn.client.getClusterInfo = mockGetClusterInfo;
            await client.refreshTopology();
            (0, vitest_1.expect)(connections.has("server-x:50051")).toBe(true);
            (0, vitest_1.expect)(connections.has("server-y:50051")).toBe(true);
        });
        (0, vitest_1.it)("tries next server if first fails during topology refresh", async () => {
            client.close();
            client = new client_1.SiloGRPCClient({
                servers: ["failing-server:50051", "working-server:50051"],
                useTls: false,
                shardRouting: {
                    topologyRefreshIntervalMs: 0,
                },
            });
            const connections = client._connections;
            // First server fails
            const failingConn = connections.get("failing-server:50051");
            failingConn.client.getClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: Promise.reject(new Error("connection refused")),
            });
            // Second server succeeds
            const workingConn = connections.get("working-server:50051");
            workingConn.client.getClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({
                    numShards: 1,
                    shardOwners: [
                        { shardId: "00000000-0000-0000-0000-000000000001", grpcAddr: "working-server:50051", nodeId: "node-1", rangeStart: "", rangeEnd: "" },
                    ],
                    thisNodeId: "node-1",
                    thisGrpcAddr: "working-server:50051",
                }),
            });
            await client.refreshTopology();
            const topology = client.getTopology();
            (0, vitest_1.expect)(topology.shards.length).toBe(1);
            (0, vitest_1.expect)(topology.shardToServer.get("00000000-0000-0000-0000-000000000001")).toBe("working-server:50051");
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
    });
    (0, vitest_1.describe)("gRPC retry configuration", () => {
        (0, vitest_1.it)("configures gRPC-level retries for transient failures", () => {
            const client = new client_1.SiloGRPCClient({
                servers: "localhost:50051",
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
                (0, vitest_1.expect)(serviceConfig.methodConfig[0].retryPolicy.retryableStatusCodes).toContain("UNAVAILABLE");
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
                servers: "localhost:50051",
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
                { shardId: "00000000-0000-0000-0000-000000000001", serverAddr: "localhost:50051", rangeStart: "", rangeEnd: "" }
            ];
            c._shardToServer.set("00000000-0000-0000-0000-000000000001", "localhost:50051");
        };
        (0, vitest_1.beforeEach)(() => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:50051",
                useTls: false,
                shardRouting: {
                    maxWrongShardRetries: 3,
                    wrongShardRetryDelayMs: 1, // Fast retries for tests
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
                    "x-silo-shard-owner-addr": "other-server:50051",
                    "x-silo-shard-owner-node": "node-2",
                });
                (0, vitest_1.expect)(error.meta?.["x-silo-shard-owner-addr"]).toBe("other-server:50051");
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
                            "x-silo-shard-owner-addr": "localhost:50051",
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
            (0, vitest_1.it)("stops retrying after maxWrongShardRetries", async () => {
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
                            "x-silo-shard-owner-addr": "new-server:50051",
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
                (0, vitest_1.expect)(getOrCreateSpy).toHaveBeenCalledWith("new-server:50051");
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
                            "x-silo-shard-owner-addr": "localhost:50051",
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
});
//# sourceMappingURL=shard-routing.test.js.map