"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const client_1 = require("../src/client");
const runtime_rpc_1 = require("@protobuf-ts/runtime-rpc");
(0, vitest_1.describe)("Shard Routing", () => {
    (0, vitest_1.describe)("fnv1a32 hash function", () => {
        (0, vitest_1.it)("produces deterministic results", () => {
            const hash1 = (0, client_1.fnv1a32)("tenant-1");
            const hash2 = (0, client_1.fnv1a32)("tenant-1");
            (0, vitest_1.expect)(hash1).toBe(hash2);
        });
        (0, vitest_1.it)("distributes tenants evenly across shards", () => {
            const numShards = 16;
            const numTenants = 10000;
            const counts = Array.from({ length: numShards }, () => 0);
            for (let i = 0; i < numTenants; i++) {
                const tenant = `tenant-${i}`;
                const shard = (0, client_1.defaultTenantToShard)(tenant, numShards);
                counts[shard]++;
            }
            // Expected count per shard: 10000 / 16 = 625
            // Allow 30% variance
            const expectedPerShard = numTenants / numShards;
            const minExpected = expectedPerShard * 0.7;
            const maxExpected = expectedPerShard * 1.3;
            for (let i = 0; i < numShards; i++) {
                (0, vitest_1.expect)(counts[i]).toBeGreaterThan(minExpected);
                (0, vitest_1.expect)(counts[i]).toBeLessThan(maxExpected);
            }
        });
        (0, vitest_1.it)("is sensitive to small changes in input", () => {
            const hash1 = (0, client_1.fnv1a32)("tenant-1");
            const hash2 = (0, client_1.fnv1a32)("tenant-2");
            const hash3 = (0, client_1.fnv1a32)("tenant-3");
            (0, vitest_1.expect)(hash1).not.toBe(hash2);
            (0, vitest_1.expect)(hash2).not.toBe(hash3);
            (0, vitest_1.expect)(hash1).not.toBe(hash3);
        });
    });
    (0, vitest_1.describe)("defaultTenantToShard", () => {
        (0, vitest_1.it)("handles various shard counts", () => {
            const tenant = "test-tenant";
            for (const numShards of [1, 2, 4, 8, 16, 32, 64, 128, 256]) {
                const shard = (0, client_1.defaultTenantToShard)(tenant, numShards);
                (0, vitest_1.expect)(shard).toBeGreaterThanOrEqual(0);
                (0, vitest_1.expect)(shard).toBeLessThan(numShards);
            }
        });
        (0, vitest_1.it)("is stable - same hash modulo different shard counts", () => {
            const tenant = "stable-tenant";
            const hash = (0, client_1.fnv1a32)(tenant);
            (0, vitest_1.expect)((0, client_1.defaultTenantToShard)(tenant, 16)).toBe(hash % 16);
            (0, vitest_1.expect)((0, client_1.defaultTenantToShard)(tenant, 32)).toBe(hash % 32);
            (0, vitest_1.expect)((0, client_1.defaultTenantToShard)(tenant, 64)).toBe(hash % 64);
        });
        (0, vitest_1.it)("handles UUIDs as tenant IDs", () => {
            const numShards = 16;
            const uuids = [
                "550e8400-e29b-41d4-a716-446655440000",
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                "6ba7b811-9dad-11d1-80b4-00c04fd430c8",
                "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            ];
            const shards = new Set();
            for (const uuid of uuids) {
                const shard = (0, client_1.defaultTenantToShard)(uuid, numShards);
                (0, vitest_1.expect)(shard).toBeGreaterThanOrEqual(0);
                (0, vitest_1.expect)(shard).toBeLessThan(numShards);
                shards.add(shard);
            }
            // UUIDs should distribute across shards
            (0, vitest_1.expect)(shards.size).toBeGreaterThan(1);
        });
        (0, vitest_1.it)("handles special characters in tenant IDs", () => {
            const numShards = 8;
            const tenants = [
                "tenant:with:colons",
                "tenant/with/slashes",
                "tenant.with.dots",
                "tenant-with-dashes",
                "tenant_with_underscores",
                "tenant@with@ats",
            ];
            for (const tenant of tenants) {
                const shard = (0, client_1.defaultTenantToShard)(tenant, numShards);
                (0, vitest_1.expect)(shard).toBeGreaterThanOrEqual(0);
                (0, vitest_1.expect)(shard).toBeLessThan(numShards);
            }
        });
    });
    (0, vitest_1.describe)("client shard resolution", () => {
        let client;
        (0, vitest_1.afterEach)(() => {
            client?.close();
        });
        (0, vitest_1.it)("computes shard from tenant using default hash", () => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:50051",
                useTls: false,
                shardRouting: {
                    numShards: 16,
                    topologyRefreshIntervalMs: 0,
                },
            });
            const shard = client.getShardForTenant("my-tenant");
            const expectedShard = (0, client_1.defaultTenantToShard)("my-tenant", 16);
            (0, vitest_1.expect)(shard).toBe(expectedShard);
        });
        (0, vitest_1.it)("uses custom tenantToShard function when provided", () => {
            const customFn = vitest_1.vi.fn((tenant, numShards) => {
                return tenant.length % numShards;
            });
            client = new client_1.SiloGRPCClient({
                servers: "localhost:50051",
                useTls: false,
                shardRouting: {
                    numShards: 10,
                    tenantToShard: customFn,
                    topologyRefreshIntervalMs: 0,
                },
            });
            // "short" has length 5
            (0, vitest_1.expect)(client.getShardForTenant("short")).toBe(5);
            (0, vitest_1.expect)(customFn).toHaveBeenCalledWith("short", 10);
            // "verylongtenant" has length 14, 14 % 10 = 4
            (0, vitest_1.expect)(client.getShardForTenant("verylongtenant")).toBe(4);
            (0, vitest_1.expect)(customFn).toHaveBeenCalledWith("verylongtenant", 10);
        });
    });
    (0, vitest_1.describe)("Topology discovery", () => {
        let client;
        (0, vitest_1.beforeEach)(() => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:50051",
                useTls: false,
                shardRouting: {
                    numShards: 8,
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
                        { shardId: 0, grpcAddr: "server-a:50051", nodeId: "node-a" },
                        { shardId: 1, grpcAddr: "server-b:50051", nodeId: "node-b" },
                        { shardId: 2, grpcAddr: "server-a:50051", nodeId: "node-a" },
                        { shardId: 3, grpcAddr: "server-b:50051", nodeId: "node-b" },
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
            (0, vitest_1.expect)(topology.numShards).toBe(4);
            (0, vitest_1.expect)(topology.shardToServer.get(0)).toBe("server-a:50051");
            (0, vitest_1.expect)(topology.shardToServer.get(1)).toBe("server-b:50051");
            (0, vitest_1.expect)(topology.shardToServer.get(2)).toBe("server-a:50051");
            (0, vitest_1.expect)(topology.shardToServer.get(3)).toBe("server-b:50051");
        });
        (0, vitest_1.it)("creates connections to discovered servers", async () => {
            const mockGetClusterInfo = vitest_1.vi.fn().mockReturnValue({
                response: Promise.resolve({
                    numShards: 2,
                    shardOwners: [
                        { shardId: 0, grpcAddr: "server-x:50051", nodeId: "node-x" },
                        { shardId: 1, grpcAddr: "server-y:50051", nodeId: "node-y" },
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
                    numShards: 1,
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
                        { shardId: 0, grpcAddr: "working-server:50051", nodeId: "node-1" },
                    ],
                    thisNodeId: "node-1",
                    thisGrpcAddr: "working-server:50051",
                }),
            });
            await client.refreshTopology();
            const topology = client.getTopology();
            (0, vitest_1.expect)(topology.numShards).toBe(1);
            (0, vitest_1.expect)(topology.shardToServer.get(0)).toBe("working-server:50051");
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
                    numShards: 1,
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
                    numShards: 1,
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
        (0, vitest_1.beforeEach)(() => {
            client = new client_1.SiloGRPCClient({
                servers: "localhost:50051",
                useTls: false,
                shardRouting: {
                    numShards: 16,
                    maxWrongShardRetries: 3,
                    wrongShardRetryDelayMs: 1, // Fast retries for tests
                    topologyRefreshIntervalMs: 0,
                },
            });
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
                });
                (0, vitest_1.expect)(handle.id).toBe("job-abc");
                (0, vitest_1.expect)(refreshSpy).not.toHaveBeenCalled();
            });
        });
    });
});
//# sourceMappingURL=shard-routing.test.js.map