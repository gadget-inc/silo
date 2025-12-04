import { describe, it, expect, vi, afterEach } from "vitest";
import {
  SiloGRPCClient,
  encodePayload,
  decodePayload,
  fnv1a32,
  defaultTenantToShard,
  type EnqueueJobOptions,
  type SiloGRPCClientOptions,
} from "../src/client";

describe("fnv1a32", () => {
  it("returns consistent hash for same input", () => {
    const hash1 = fnv1a32("tenant-123");
    const hash2 = fnv1a32("tenant-123");
    expect(hash1).toBe(hash2);
  });

  it("returns different hashes for different inputs", () => {
    const hash1 = fnv1a32("tenant-123");
    const hash2 = fnv1a32("tenant-456");
    expect(hash1).not.toBe(hash2);
  });

  it("returns a 32-bit unsigned integer", () => {
    const hash = fnv1a32("test-string");
    expect(hash).toBeGreaterThanOrEqual(0);
    expect(hash).toBeLessThanOrEqual(0xffffffff);
  });

  it("handles empty string", () => {
    const hash = fnv1a32("");
    expect(hash).toBe(0x811c9dc5); // FNV offset basis
  });

  it("handles unicode characters", () => {
    const hash = fnv1a32("テスト");
    expect(hash).toBeGreaterThanOrEqual(0);
    expect(hash).toBeLessThanOrEqual(0xffffffff);
  });

  it("produces well-distributed hashes", () => {
    // Test that hashes distribute somewhat evenly
    const buckets = new Map<number, number>();
    const numBuckets = 16;
    for (let i = 0; i < 1000; i++) {
      const hash = fnv1a32(`tenant-${i}`);
      const bucket = hash % numBuckets;
      buckets.set(bucket, (buckets.get(bucket) ?? 0) + 1);
    }
    // Each bucket should have roughly 1000/16 = 62.5 entries
    // Allow significant variance but ensure all buckets are used
    for (let i = 0; i < numBuckets; i++) {
      expect(buckets.get(i)).toBeGreaterThan(20);
      expect(buckets.get(i)).toBeLessThan(150);
    }
  });
});

describe("defaultTenantToShard", () => {
  it("returns a shard ID between 0 and numShards-1", () => {
    const shardId = defaultTenantToShard("tenant-123", 16);
    expect(shardId).toBeGreaterThanOrEqual(0);
    expect(shardId).toBeLessThan(16);
  });

  it("returns consistent shard for same tenant", () => {
    const shard1 = defaultTenantToShard("tenant-abc", 10);
    const shard2 = defaultTenantToShard("tenant-abc", 10);
    expect(shard1).toBe(shard2);
  });

  it("returns different shards for different tenants (usually)", () => {
    // Test multiple tenants - not all will be different but most should be
    const shards = new Set<number>();
    for (let i = 0; i < 100; i++) {
      shards.add(defaultTenantToShard(`tenant-${i}`, 10));
    }
    // With 100 tenants and 10 shards, we should see most shards used
    expect(shards.size).toBeGreaterThanOrEqual(8);
  });

  it("throws for zero numShards", () => {
    expect(() => defaultTenantToShard("tenant", 0)).toThrow(
      "numShards must be positive"
    );
  });

  it("throws for negative numShards", () => {
    expect(() => defaultTenantToShard("tenant", -1)).toThrow(
      "numShards must be positive"
    );
  });

  it("handles single shard", () => {
    // With only one shard, all tenants should map to shard 0
    expect(defaultTenantToShard("tenant-a", 1)).toBe(0);
    expect(defaultTenantToShard("tenant-b", 1)).toBe(0);
    expect(defaultTenantToShard("anything", 1)).toBe(0);
  });
});

describe("encodePayload", () => {
  it("encodes a string as JSON bytes", () => {
    const result = encodePayload("hello");
    expect(new TextDecoder().decode(result)).toBe('"hello"');
  });

  it("encodes an object as JSON bytes", () => {
    const result = encodePayload({ foo: "bar", count: 42 });
    expect(new TextDecoder().decode(result)).toBe('{"foo":"bar","count":42}');
  });

  it("encodes null as JSON bytes", () => {
    const result = encodePayload(null);
    expect(new TextDecoder().decode(result)).toBe("null");
  });

  it("encodes an array as JSON bytes", () => {
    const result = encodePayload([1, 2, 3]);
    expect(new TextDecoder().decode(result)).toBe("[1,2,3]");
  });

  it("encodes nested objects as JSON bytes", () => {
    const result = encodePayload({ outer: { inner: "value" } });
    expect(new TextDecoder().decode(result)).toBe(
      '{"outer":{"inner":"value"}}'
    );
  });
});

describe("decodePayload", () => {
  it("decodes JSON bytes as a string", () => {
    const bytes = new TextEncoder().encode('"hello"');
    const result = decodePayload<string>(bytes);
    expect(result).toBe("hello");
  });

  it("decodes JSON bytes as an object", () => {
    const bytes = new TextEncoder().encode('{"foo":"bar","count":42}');
    const result = decodePayload<{ foo: string; count: number }>(bytes);
    expect(result).toEqual({ foo: "bar", count: 42 });
  });

  it("decodes JSON bytes as null", () => {
    const bytes = new TextEncoder().encode("null");
    const result = decodePayload<null>(bytes);
    expect(result).toBe(null);
  });

  it("decodes JSON bytes as an array", () => {
    const bytes = new TextEncoder().encode("[1,2,3]");
    const result = decodePayload<number[]>(bytes);
    expect(result).toEqual([1, 2, 3]);
  });

  it("returns undefined for undefined input", () => {
    const result = decodePayload(undefined);
    expect(result).toBeUndefined();
  });

  it("returns undefined for empty byte array", () => {
    const result = decodePayload(new Uint8Array(0));
    expect(result).toBeUndefined();
  });
});

describe("SiloGRPCClient", () => {
  // Track clients created in tests for cleanup
  let clientsToClose: SiloGRPCClient[] = [];

  afterEach(() => {
    for (const client of clientsToClose) {
      client.close();
    }
    clientsToClose = [];
  });

  // Helper to create a client and track it for cleanup
  const createClient = (options: SiloGRPCClientOptions) => {
    const client = new SiloGRPCClient(options);
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

  describe("constructor", () => {
    it("accepts a string server address", () => {
      const client = createClient({
        servers: "localhost:50051",
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts multiple server addresses", () => {
      const client = createClient({
        servers: ["localhost:50051", "localhost:50052"],
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts a host/port server address", () => {
      const client = createClient({
        servers: { host: "localhost", port: 50051 },
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts a string token", () => {
      const client = createClient({
        servers: "localhost:50051",
        token: "my-token",
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts a token function", () => {
      const client = createClient({
        servers: "localhost:50051",
        token: async () => "my-token",
        ...defaultOptions,
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts custom grpc client options", () => {
      const client = createClient({
        servers: "localhost:50051",
        ...defaultOptions,
        grpcClientOptions: {
          "grpc.max_send_message_length": 1024 * 1024,
        },
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts custom rpc options", () => {
      const client = createClient({
        servers: "localhost:50051",
        ...defaultOptions,
        rpcOptions: { timeout: 5000 },
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });

    it("accepts rpc options as a function", () => {
      const client = createClient({
        servers: "localhost:50051",
        ...defaultOptions,
        rpcOptions: () => ({ timeout: 5000 }),
      });
      expect(client).toBeInstanceOf(SiloGRPCClient);
    });
  });

  describe("EnqueueJobOptions type", () => {
    it("allows minimal options", () => {
      const options: EnqueueJobOptions = {
        payload: { task: "test" },
      };
      expect(options.payload).toEqual({ task: "test" });
    });

    it("allows full options with concurrency limit", () => {
      const options: EnqueueJobOptions = {
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
      expect(options.id).toBe("job-123");
      expect(options.priority).toBe(10);
      expect(options.retryPolicy?.retryCount).toBe(3);
      const limit = options.limits?.[0];
      expect(limit?.type).toBe("concurrency");
      if (limit?.type === "concurrency") {
        expect(limit.key).toBe("user:123");
        expect(limit.maxConcurrency).toBe(5);
      }
    });

    it("allows full options with rate limit", () => {
      const options: EnqueueJobOptions = {
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
      expect(limit?.type).toBe("rateLimit");
      if (limit?.type === "rateLimit") {
        expect(limit.name).toBe("api-limit");
        expect(limit.uniqueKey).toBe("user:456");
        expect(limit.limit).toBe(100n);
        expect(limit.durationMs).toBe(60000n);
      }
    });

    it("allows mixed limits", () => {
      const options: EnqueueJobOptions = {
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
      expect(options.limits).toHaveLength(2);
      expect(options.limits?.[0].type).toBe("concurrency");
      expect(options.limits?.[1].type).toBe("rateLimit");
    });
  });

  describe("shard routing", () => {
    describe("getShardForTenant", () => {
      it("returns the computed shard for a tenant", () => {
        const client = createClient({
          servers: "localhost:50051",
          useTls: false,
          shardRouting: {
            numShards: 16,
            topologyRefreshIntervalMs: 0,
          },
        });

        const shard = client.getShardForTenant("tenant-123");
        expect(shard).toBeGreaterThanOrEqual(0);
        expect(shard).toBeLessThan(16);

        // Should be consistent
        expect(client.getShardForTenant("tenant-123")).toBe(shard);
      });

      it("uses custom tenantToShard function", () => {
        const customFn = vi.fn((_tenant: string, _numShards: number) => 7);
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
        expect(shard).toBe(7);
        expect(customFn).toHaveBeenCalledWith("any-tenant", 16);
      });
    });

    describe("getTopology", () => {
      it("returns the current topology", () => {
        const client = createClient({
          servers: "localhost:50051",
          useTls: false,
          shardRouting: {
            numShards: 16,
            topologyRefreshIntervalMs: 0,
          },
        });

        const topology = client.getTopology();
        expect(topology.numShards).toBe(16);
        expect(topology.shardToServer).toBeInstanceOf(Map);
      });
    });
  });
});
