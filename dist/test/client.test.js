"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const client_1 = require("../src/client");
(0, vitest_1.describe)("fnv1a32", () => {
    (0, vitest_1.it)("returns consistent hash for same input", () => {
        const hash1 = (0, client_1.fnv1a32)("tenant-123");
        const hash2 = (0, client_1.fnv1a32)("tenant-123");
        (0, vitest_1.expect)(hash1).toBe(hash2);
    });
    (0, vitest_1.it)("returns different hashes for different inputs", () => {
        const hash1 = (0, client_1.fnv1a32)("tenant-123");
        const hash2 = (0, client_1.fnv1a32)("tenant-456");
        (0, vitest_1.expect)(hash1).not.toBe(hash2);
    });
    (0, vitest_1.it)("returns a 32-bit unsigned integer", () => {
        const hash = (0, client_1.fnv1a32)("test-string");
        (0, vitest_1.expect)(hash).toBeGreaterThanOrEqual(0);
        (0, vitest_1.expect)(hash).toBeLessThanOrEqual(0xffffffff);
    });
    (0, vitest_1.it)("handles empty string", () => {
        const hash = (0, client_1.fnv1a32)("");
        (0, vitest_1.expect)(hash).toBe(0x811c9dc5); // FNV offset basis
    });
    (0, vitest_1.it)("handles unicode characters", () => {
        const hash = (0, client_1.fnv1a32)("テスト");
        (0, vitest_1.expect)(hash).toBeGreaterThanOrEqual(0);
        (0, vitest_1.expect)(hash).toBeLessThanOrEqual(0xffffffff);
    });
    (0, vitest_1.it)("produces well-distributed hashes", () => {
        // Test that hashes distribute somewhat evenly
        const buckets = new Map();
        const numBuckets = 16;
        for (let i = 0; i < 1000; i++) {
            const hash = (0, client_1.fnv1a32)(`tenant-${i}`);
            const bucket = hash % numBuckets;
            buckets.set(bucket, (buckets.get(bucket) ?? 0) + 1);
        }
        // Each bucket should have roughly 1000/16 = 62.5 entries
        // Allow significant variance but ensure all buckets are used
        for (let i = 0; i < numBuckets; i++) {
            (0, vitest_1.expect)(buckets.get(i)).toBeGreaterThan(20);
            (0, vitest_1.expect)(buckets.get(i)).toBeLessThan(150);
        }
    });
});
(0, vitest_1.describe)("defaultTenantToShard", () => {
    (0, vitest_1.it)("returns a shard ID between 0 and numShards-1", () => {
        const shardId = (0, client_1.defaultTenantToShard)("tenant-123", 16);
        (0, vitest_1.expect)(shardId).toBeGreaterThanOrEqual(0);
        (0, vitest_1.expect)(shardId).toBeLessThan(16);
    });
    (0, vitest_1.it)("returns consistent shard for same tenant", () => {
        const shard1 = (0, client_1.defaultTenantToShard)("tenant-abc", 10);
        const shard2 = (0, client_1.defaultTenantToShard)("tenant-abc", 10);
        (0, vitest_1.expect)(shard1).toBe(shard2);
    });
    (0, vitest_1.it)("returns different shards for different tenants (usually)", () => {
        // Test multiple tenants - not all will be different but most should be
        const shards = new Set();
        for (let i = 0; i < 100; i++) {
            shards.add((0, client_1.defaultTenantToShard)(`tenant-${i}`, 10));
        }
        // With 100 tenants and 10 shards, we should see most shards used
        (0, vitest_1.expect)(shards.size).toBeGreaterThanOrEqual(8);
    });
    (0, vitest_1.it)("throws for zero numShards", () => {
        (0, vitest_1.expect)(() => (0, client_1.defaultTenantToShard)("tenant", 0)).toThrow("numShards must be positive");
    });
    (0, vitest_1.it)("throws for negative numShards", () => {
        (0, vitest_1.expect)(() => (0, client_1.defaultTenantToShard)("tenant", -1)).toThrow("numShards must be positive");
    });
    (0, vitest_1.it)("handles single shard", () => {
        // With only one shard, all tenants should map to shard 0
        (0, vitest_1.expect)((0, client_1.defaultTenantToShard)("tenant-a", 1)).toBe(0);
        (0, vitest_1.expect)((0, client_1.defaultTenantToShard)("tenant-b", 1)).toBe(0);
        (0, vitest_1.expect)((0, client_1.defaultTenantToShard)("anything", 1)).toBe(0);
    });
});
(0, vitest_1.describe)("encodePayload", () => {
    (0, vitest_1.it)("encodes a string as JSON bytes", () => {
        const result = (0, client_1.encodePayload)("hello");
        (0, vitest_1.expect)(new TextDecoder().decode(result)).toBe('"hello"');
    });
    (0, vitest_1.it)("encodes an object as JSON bytes", () => {
        const result = (0, client_1.encodePayload)({ foo: "bar", count: 42 });
        (0, vitest_1.expect)(new TextDecoder().decode(result)).toBe('{"foo":"bar","count":42}');
    });
    (0, vitest_1.it)("encodes null as JSON bytes", () => {
        const result = (0, client_1.encodePayload)(null);
        (0, vitest_1.expect)(new TextDecoder().decode(result)).toBe("null");
    });
    (0, vitest_1.it)("encodes an array as JSON bytes", () => {
        const result = (0, client_1.encodePayload)([1, 2, 3]);
        (0, vitest_1.expect)(new TextDecoder().decode(result)).toBe("[1,2,3]");
    });
    (0, vitest_1.it)("encodes nested objects as JSON bytes", () => {
        const result = (0, client_1.encodePayload)({ outer: { inner: "value" } });
        (0, vitest_1.expect)(new TextDecoder().decode(result)).toBe('{"outer":{"inner":"value"}}');
    });
});
(0, vitest_1.describe)("decodePayload", () => {
    (0, vitest_1.it)("decodes JSON bytes as a string", () => {
        const bytes = new TextEncoder().encode('"hello"');
        const result = (0, client_1.decodePayload)(bytes);
        (0, vitest_1.expect)(result).toBe("hello");
    });
    (0, vitest_1.it)("decodes JSON bytes as an object", () => {
        const bytes = new TextEncoder().encode('{"foo":"bar","count":42}');
        const result = (0, client_1.decodePayload)(bytes);
        (0, vitest_1.expect)(result).toEqual({ foo: "bar", count: 42 });
    });
    (0, vitest_1.it)("decodes JSON bytes as null", () => {
        const bytes = new TextEncoder().encode("null");
        const result = (0, client_1.decodePayload)(bytes);
        (0, vitest_1.expect)(result).toBe(null);
    });
    (0, vitest_1.it)("decodes JSON bytes as an array", () => {
        const bytes = new TextEncoder().encode("[1,2,3]");
        const result = (0, client_1.decodePayload)(bytes);
        (0, vitest_1.expect)(result).toEqual([1, 2, 3]);
    });
    (0, vitest_1.it)("returns undefined for undefined input", () => {
        const result = (0, client_1.decodePayload)(undefined);
        (0, vitest_1.expect)(result).toBeUndefined();
    });
    (0, vitest_1.it)("returns undefined for empty byte array", () => {
        const result = (0, client_1.decodePayload)(new Uint8Array(0));
        (0, vitest_1.expect)(result).toBeUndefined();
    });
});
(0, vitest_1.describe)("SiloGRPCClient", () => {
    // Track clients created in tests for cleanup
    let clientsToClose = [];
    (0, vitest_1.afterEach)(() => {
        for (const client of clientsToClose) {
            client.close();
        }
        clientsToClose = [];
    });
    // Helper to create a client and track it for cleanup
    const createClient = (options) => {
        const client = new client_1.SiloGRPCClient(options);
        clientsToClose.push(client);
        return client;
    };
    // Default options for tests (disable auto-refresh to avoid background timers)
    const defaultOptions = {
        useTls: false,
        shardRouting: {
            numShards: 16,
            topologyRefreshIntervalMs: 0, // Disable auto-refresh in tests
        },
    };
    (0, vitest_1.describe)("constructor", () => {
        (0, vitest_1.it)("accepts a string server address", () => {
            const client = createClient({
                servers: "localhost:50051",
                ...defaultOptions,
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
        (0, vitest_1.it)("accepts multiple server addresses", () => {
            const client = createClient({
                servers: ["localhost:50051", "localhost:50052"],
                ...defaultOptions,
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
        (0, vitest_1.it)("accepts a host/port server address", () => {
            const client = createClient({
                servers: { host: "localhost", port: 50051 },
                ...defaultOptions,
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
        (0, vitest_1.it)("accepts a string token", () => {
            const client = createClient({
                servers: "localhost:50051",
                token: "my-token",
                ...defaultOptions,
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
        (0, vitest_1.it)("accepts a token function", () => {
            const client = createClient({
                servers: "localhost:50051",
                token: async () => "my-token",
                ...defaultOptions,
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
        (0, vitest_1.it)("accepts custom grpc client options", () => {
            const client = createClient({
                servers: "localhost:50051",
                ...defaultOptions,
                grpcClientOptions: {
                    "grpc.max_send_message_length": 1024 * 1024,
                },
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
        (0, vitest_1.it)("accepts custom rpc options", () => {
            const client = createClient({
                servers: "localhost:50051",
                ...defaultOptions,
                rpcOptions: { timeout: 5000 },
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
        (0, vitest_1.it)("accepts rpc options as a function", () => {
            const client = createClient({
                servers: "localhost:50051",
                ...defaultOptions,
                rpcOptions: () => ({ timeout: 5000 }),
            });
            (0, vitest_1.expect)(client).toBeInstanceOf(client_1.SiloGRPCClient);
        });
    });
    (0, vitest_1.describe)("EnqueueJobOptions type", () => {
        (0, vitest_1.it)("allows minimal options", () => {
            const options = {
                payload: { task: "test" },
            };
            (0, vitest_1.expect)(options.payload).toEqual({ task: "test" });
        });
        (0, vitest_1.it)("allows full options with concurrency limit", () => {
            const options = {
                payload: { task: "test" },
                id: "job-123",
                priority: 10,
                startAtMs: BigInt(Date.now()),
                retryPolicy: {
                    retryCount: 3,
                    initialIntervalMs: 1000n,
                    maxIntervalMs: 30000n,
                    randomizeInterval: true,
                    backoffFactor: 2.0,
                },
                limits: [{ type: "concurrency", key: "user:123", maxConcurrency: 5 }],
                tenant: "tenant-a",
                metadata: { source: "api" },
            };
            (0, vitest_1.expect)(options.id).toBe("job-123");
            (0, vitest_1.expect)(options.priority).toBe(10);
            (0, vitest_1.expect)(options.retryPolicy?.retryCount).toBe(3);
            const limit = options.limits?.[0];
            (0, vitest_1.expect)(limit?.type).toBe("concurrency");
            if (limit?.type === "concurrency") {
                (0, vitest_1.expect)(limit.key).toBe("user:123");
                (0, vitest_1.expect)(limit.maxConcurrency).toBe(5);
            }
        });
        (0, vitest_1.it)("allows full options with rate limit", () => {
            const options = {
                payload: { task: "test" },
                limits: [
                    {
                        type: "rateLimit",
                        name: "api-limit",
                        uniqueKey: "user:456",
                        limit: 100n,
                        durationMs: 60000n,
                        hits: 1,
                        retryPolicy: {
                            initialBackoffMs: 100n,
                            maxBackoffMs: 5000n,
                        },
                    },
                ],
            };
            const limit = options.limits?.[0];
            (0, vitest_1.expect)(limit?.type).toBe("rateLimit");
            if (limit?.type === "rateLimit") {
                (0, vitest_1.expect)(limit.name).toBe("api-limit");
                (0, vitest_1.expect)(limit.uniqueKey).toBe("user:456");
                (0, vitest_1.expect)(limit.limit).toBe(100n);
                (0, vitest_1.expect)(limit.durationMs).toBe(60000n);
            }
        });
        (0, vitest_1.it)("allows mixed limits", () => {
            const options = {
                payload: { task: "test" },
                limits: [
                    { type: "concurrency", key: "tenant:abc", maxConcurrency: 10 },
                    {
                        type: "rateLimit",
                        name: "burst",
                        uniqueKey: "tenant:abc",
                        limit: 50n,
                        durationMs: 1000n,
                    },
                ],
            };
            (0, vitest_1.expect)(options.limits).toHaveLength(2);
            (0, vitest_1.expect)(options.limits?.[0].type).toBe("concurrency");
            (0, vitest_1.expect)(options.limits?.[1].type).toBe("rateLimit");
        });
    });
    (0, vitest_1.describe)("shard routing", () => {
        (0, vitest_1.describe)("getShardForTenant", () => {
            (0, vitest_1.it)("returns the computed shard for a tenant", () => {
                const client = createClient({
                    servers: "localhost:50051",
                    useTls: false,
                    shardRouting: {
                        numShards: 16,
                        topologyRefreshIntervalMs: 0,
                    },
                });
                const shard = client.getShardForTenant("tenant-123");
                (0, vitest_1.expect)(shard).toBeGreaterThanOrEqual(0);
                (0, vitest_1.expect)(shard).toBeLessThan(16);
                // Should be consistent
                (0, vitest_1.expect)(client.getShardForTenant("tenant-123")).toBe(shard);
            });
            (0, vitest_1.it)("uses custom tenantToShard function", () => {
                const customFn = vitest_1.vi.fn((_tenant, _numShards) => 7);
                const client = createClient({
                    servers: "localhost:50051",
                    useTls: false,
                    shardRouting: {
                        numShards: 16,
                        tenantToShard: customFn,
                        topologyRefreshIntervalMs: 0,
                    },
                });
                const shard = client.getShardForTenant("any-tenant");
                (0, vitest_1.expect)(shard).toBe(7);
                (0, vitest_1.expect)(customFn).toHaveBeenCalledWith("any-tenant", 16);
            });
        });
        (0, vitest_1.describe)("getTopology", () => {
            (0, vitest_1.it)("returns the current topology", () => {
                const client = createClient({
                    servers: "localhost:50051",
                    useTls: false,
                    shardRouting: {
                        numShards: 16,
                        topologyRefreshIntervalMs: 0,
                    },
                });
                const topology = client.getTopology();
                (0, vitest_1.expect)(topology.numShards).toBe(16);
                (0, vitest_1.expect)(topology.shardToServer).toBeInstanceOf(Map);
            });
        });
    });
});
//# sourceMappingURL=client.test.js.map