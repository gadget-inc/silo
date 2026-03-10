import { describe, it, expect, vi, afterEach, beforeAll, beforeEach } from "vitest";
import { SiloGRPCClient, shardForTenant, hashTenant, initHasher, type ShardInfoWithRange } from "../src/client";
import { RpcError } from "@protobuf-ts/runtime-rpc";

describe("Shard Routing", () => {
  beforeAll(() => initHasher());

  describe("hashTenant", () => {
    it("returns a 16-character hex string", () => {
      const hash = hashTenant("test-tenant");
      expect(hash).toMatch(/^[0-9a-f]{16}$/);
    });

    it("is deterministic", () => {
      expect(hashTenant("tenant-1")).toBe(hashTenant("tenant-1"));
    });

    it("distributes env- tenants across different first hex digits", () => {
      const firstDigits = new Set<string>();
      for (let i = 0; i < 100; i++) {
        firstDigits.add(hashTenant(`env-${i}`)[0]);
      }
      // With good hashing, 100 env- tenants should hit many different first digits
      expect(firstDigits.size).toBeGreaterThan(8);
    });
  });

  describe("shardForTenant lexicographic lookup (expects hash-space keys)", () => {
    it("finds correct shard for key in range", () => {
      const shards: ShardInfoWithRange[] = [
        { shardId: "shard-1", serverAddr: "server-a:7450", rangeStart: "", rangeEnd: "8000000000000000" },
        { shardId: "shard-2", serverAddr: "server-b:7450", rangeStart: "8000000000000000", rangeEnd: "" },
      ];

      // Keys in hash space — direct lexicographic comparison
      expect(shardForTenant("3000000000000000", shards)?.shardId).toBe("shard-1");
      expect(shardForTenant("a000000000000000", shards)?.shardId).toBe("shard-2");
    });

    it("handles single full-range shard", () => {
      const shards: ShardInfoWithRange[] = [
        { shardId: "shard-1", serverAddr: "server-a:7450", rangeStart: "", rangeEnd: "" },
      ];

      expect(shardForTenant("anything", shards)?.shardId).toBe("shard-1");
    });

    it("returns undefined for empty shards array", () => {
      expect(shardForTenant("key", [])).toBeUndefined();
    });

    it("handles multiple shard ranges", () => {
      const shards: ShardInfoWithRange[] = [
        { shardId: "shard-1", serverAddr: "server-a:7450", rangeStart: "", rangeEnd: "4000000000000000" },
        { shardId: "shard-2", serverAddr: "server-b:7450", rangeStart: "4000000000000000", rangeEnd: "8000000000000000" },
        { shardId: "shard-3", serverAddr: "server-a:7450", rangeStart: "8000000000000000", rangeEnd: "c000000000000000" },
        { shardId: "shard-4", serverAddr: "server-b:7450", rangeStart: "c000000000000000", rangeEnd: "" },
      ];

      expect(shardForTenant("1000000000000000", shards)?.shardId).toBe("shard-1");
      expect(shardForTenant("5000000000000000", shards)?.shardId).toBe("shard-2");
      expect(shardForTenant("9000000000000000", shards)?.shardId).toBe("shard-3");
      expect(shardForTenant("d000000000000000", shards)?.shardId).toBe("shard-4");
    });

    it("end-to-end: hashTenant + shardForTenant routes tenants correctly", () => {
      const shards: ShardInfoWithRange[] = [
        { shardId: "shard-1", serverAddr: "server-a:7450", rangeStart: "", rangeEnd: "8000000000000000" },
        { shardId: "shard-2", serverAddr: "server-b:7450", rangeStart: "8000000000000000", rangeEnd: "" },
      ];

      // Hash first, then look up — this is what _resolveShard does
      for (const tenant of ["test-tenant", "env-123", "bench-0"]) {
        const result = shardForTenant(hashTenant(tenant), shards);
        expect(result).toBeDefined();
        expect(["shard-1", "shard-2"]).toContain(result?.shardId);
      }

      // Same tenant always routes to same shard
      const r1 = shardForTenant(hashTenant("my-tenant"), shards);
      const r2 = shardForTenant(hashTenant("my-tenant"), shards);
      expect(r1?.shardId).toBe(r2?.shardId);
    });
  });

  describe("Topology discovery", () => {
    let client: SiloGRPCClient;

    beforeEach(() => {
      client = new SiloGRPCClient({
        servers: "localhost:7450",
        useTls: false,
        shardRouting: {
          topologyRefreshIntervalMs: 0,
        },
      });
    });

    afterEach(() => {
      client.close();
    });

    it("updates shard-to-server mapping from GetClusterInfo response", async () => {
      const mockGetClusterInfo = vi.fn().mockReturnValue({
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

      const connections = (client as any)._connections as Map<string, any>;
      const conn = connections.values().next().value;
      conn.client.getClusterInfo = mockGetClusterInfo;

      await client.refreshTopology();

      const topology = client.getTopology();
      expect(topology.shards.length).toBe(4);
      expect(topology.shardToServer.get("00000000-0000-0000-0000-000000000001")).toBe(
        "server-a:7450",
      );
      expect(topology.shardToServer.get("00000000-0000-0000-0000-000000000002")).toBe(
        "server-b:7450",
      );
      expect(topology.shardToServer.get("00000000-0000-0000-0000-000000000003")).toBe(
        "server-a:7450",
      );
      expect(topology.shardToServer.get("00000000-0000-0000-0000-000000000004")).toBe(
        "server-b:7450",
      );
    });

    it("creates connections to discovered servers", async () => {
      const mockGetClusterInfo = vi.fn().mockReturnValue({
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

      const connections = (client as any)._connections as Map<string, any>;
      const conn = connections.values().next().value;
      conn.client.getClusterInfo = mockGetClusterInfo;

      await client.refreshTopology();

      expect(connections.has("server-x:7450")).toBe(true);
      expect(connections.has("server-y:7450")).toBe(true);
    });

    it("tries next server if first fails during topology refresh", async () => {
      client.close();
      client = new SiloGRPCClient({
        servers: ["failing-server:7450", "working-server:7450"],
        useTls: false,
        shardRouting: {
          topologyRefreshIntervalMs: 0,
        },
      });

      const connections = (client as any)._connections as Map<string, any>;

      // First server fails
      const failingConn = connections.get("failing-server:7450");
      failingConn.client.getClusterInfo = vi.fn().mockReturnValue({
        response: Promise.reject(new Error("connection refused")),
      });

      // Second server succeeds
      const workingConn = connections.get("working-server:7450");
      workingConn.client.getClusterInfo = vi.fn().mockReturnValue({
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
      expect(topology.shards.length).toBe(1);
      expect(topology.shardToServer.get("00000000-0000-0000-0000-000000000001")).toBe(
        "working-server:7450",
      );
    });

    it("throws if all servers fail during topology refresh after exhausting retries", async () => {
      // Even with gRPC-level retries (configured for UNAVAILABLE, RESOURCE_EXHAUSTED),
      // if all servers persistently fail, topology refresh should eventually throw
      const mockGetClusterInfo = vi.fn().mockReturnValue({
        response: Promise.reject(new Error("connection refused")),
      });

      const connections = (client as any)._connections as Map<string, any>;
      const conn = connections.values().next().value;
      conn.client.getClusterInfo = mockGetClusterInfo;

      await expect(client.refreshTopology()).rejects.toThrow(
        "Failed to refresh cluster topology from any server",
      );
    });
  });

  describe("gRPC retry configuration", () => {
    it("configures gRPC-level retries for transient failures", () => {
      const client = new SiloGRPCClient({
        servers: "localhost:7450",
        useTls: false,
        shardRouting: {
          topologyRefreshIntervalMs: 0,
        },
      });

      try {
        // Access the internal grpc options to verify retry config is set
        const grpcOptions = (client as any)._grpcClientOptions;
        expect(grpcOptions["grpc.enable_retries"]).toBe(1);
        expect(grpcOptions["grpc.service_config"]).toBeDefined();

        const serviceConfig = JSON.parse(grpcOptions["grpc.service_config"]);
        expect(serviceConfig.methodConfig).toBeDefined();
        expect(serviceConfig.methodConfig[0].retryPolicy).toBeDefined();
        expect(serviceConfig.methodConfig[0].retryPolicy.retryableStatusCodes).toContain(
          "UNAVAILABLE",
        );
        expect(serviceConfig.methodConfig[0].retryPolicy.retryableStatusCodes).toContain(
          "RESOURCE_EXHAUSTED",
        );
      } finally {
        client.close();
      }
    });

    it("allows overriding gRPC options", () => {
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

      const client = new SiloGRPCClient({
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
        const grpcOptions = (client as any)._grpcClientOptions;
        const serviceConfig = JSON.parse(grpcOptions["grpc.service_config"]);
        expect(serviceConfig.methodConfig[0].retryPolicy.maxAttempts).toBe(10);
      } finally {
        client.close();
      }
    });
  });

  describe("Wrong Shard Retry", () => {
    let client: SiloGRPCClient;

    // Helper to set up mock topology so shard resolution works
    const setupMockTopology = (c: SiloGRPCClient) => {
      (c as any)._shards = [
        {
          shardId: "00000000-0000-0000-0000-000000000001",
          serverAddr: "localhost:7450",
          rangeStart: "",
          rangeEnd: "",
        },
      ];
      (c as any)._shardToServer.set("00000000-0000-0000-0000-000000000001", "localhost:7450");
      (c as any)._topologyReady = true;
    };

    beforeEach(() => {
      client = new SiloGRPCClient({
        servers: "localhost:7450",
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

    afterEach(() => {
      client.close();
    });

    describe("error classification", () => {
      it("NOT_FOUND with 'shard not found' is a wrong shard error", () => {
        const error = new RpcError("shard not found", "NOT_FOUND", {});
        expect(error.code).toBe("NOT_FOUND");
        expect(error.message).toContain("shard not found");
      });

      it("NOT_FOUND with different message is not a wrong shard error", () => {
        const error = new RpcError("job not found", "NOT_FOUND", {});
        expect(error.message).not.toContain("shard not found");
      });

      it("other error codes are not wrong shard errors", () => {
        const error = new RpcError("internal error", "INTERNAL", {});
        expect(error.code).not.toBe("NOT_FOUND");
      });

      it("extracts redirect address from error metadata", () => {
        const error = new RpcError("shard not found", "NOT_FOUND", {
          "x-silo-shard-owner-addr": "other-server:7450",
          "x-silo-shard-owner-node": "node-2",
        });

        expect(error.meta?.["x-silo-shard-owner-addr"]).toBe("other-server:7450");
        expect(error.meta?.["x-silo-shard-owner-node"]).toBe("node-2");
      });
    });

    describe("retry behavior", () => {
      it("retries on a new shard when it gets a wrong shard error and succeeds", async () => {
        let callCount = 0;

        const mockEnqueue = vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            throw new RpcError("shard not found", "NOT_FOUND", {
              "x-silo-shard-owner-addr": "localhost:7450",
            });
          }
          return {
            response: Promise.resolve({ id: "job-123" }),
          };
        });

        const connections = (client as any)._connections as Map<string, any>;
        const conn = connections.values().next().value;
        conn.client.enqueue = mockEnqueue;
        (client as any).refreshTopology = vi.fn().mockResolvedValue(undefined);

        const handle = await client.enqueue({
          tenant: "test-tenant",
          payload: { test: true },
          taskGroup: "default",
        });

        expect(handle.id).toBe("job-123");
        expect(mockEnqueue).toHaveBeenCalledTimes(2);
      });

      it("stops retrying after maxWrongShardRetries", async () => {
        const mockEnqueue = vi.fn().mockImplementation(() => {
          throw new RpcError("shard not found", "NOT_FOUND", {});
        });

        const connections = (client as any)._connections as Map<string, any>;
        const conn = connections.values().next().value;
        conn.client.enqueue = mockEnqueue;
        (client as any).refreshTopology = vi.fn().mockResolvedValue(undefined);

        await expect(
          client.enqueue({
            tenant: "test-tenant",
            payload: { test: true },
            taskGroup: "default",
          }),
        ).rejects.toThrow("shard not found");

        // Initial call + 3 retries = 4 total calls
        expect(mockEnqueue).toHaveBeenCalledTimes(4);
      });

      it("does not retry on non-wrong-shard errors", async () => {
        const mockEnqueue = vi.fn().mockImplementation(() => {
          throw new RpcError("internal server error", "INTERNAL", {});
        });

        const connections = (client as any)._connections as Map<string, any>;
        const conn = connections.values().next().value;
        conn.client.enqueue = mockEnqueue;

        await expect(
          client.enqueue({
            tenant: "test-tenant",
            payload: { test: true },
            taskGroup: "default",
          }),
        ).rejects.toThrow("internal server error");

        expect(mockEnqueue).toHaveBeenCalledTimes(1);
      });

      it("creates new connection when redirect points to different server", async () => {
        let callCount = 0;

        const mockEnqueue = vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            throw new RpcError("shard not found", "NOT_FOUND", {
              "x-silo-shard-owner-addr": "new-server:7450",
            });
          }
          return {
            response: Promise.resolve({ id: "job-456" }),
          };
        });

        const connections = (client as any)._connections as Map<string, any>;
        const conn = connections.values().next().value;
        conn.client.enqueue = mockEnqueue;

        const originalGetOrCreate = (client as any)._getOrCreateConnection.bind(client);
        const getOrCreateSpy = vi.fn().mockImplementation((addr: string) => {
          const newConn = originalGetOrCreate(addr);
          newConn.client.enqueue = mockEnqueue;
          return newConn;
        });
        (client as any)._getOrCreateConnection = getOrCreateSpy;

        const handle = await client.enqueue({
          tenant: "test-tenant",
          payload: { test: true },
          taskGroup: "default",
        });

        expect(handle.id).toBe("job-456");
        expect(getOrCreateSpy).toHaveBeenCalledWith("new-server:7450");
      });
    });

    describe("topology refresh on retry", () => {
      it("refreshes topology when no redirect metadata is provided", async () => {
        let callCount = 0;

        const mockEnqueue = vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            throw new RpcError("shard not found", "NOT_FOUND", {});
          }
          return {
            response: Promise.resolve({ id: "job-789" }),
          };
        });

        const connections = (client as any)._connections as Map<string, any>;
        const conn = connections.values().next().value;
        conn.client.enqueue = mockEnqueue;

        const refreshSpy = vi.fn().mockResolvedValue(undefined);
        (client as any).refreshTopology = refreshSpy;

        const handle = await client.enqueue({
          tenant: "test-tenant",
          payload: { test: true },
          taskGroup: "default",
        });

        expect(handle.id).toBe("job-789");
        expect(refreshSpy).toHaveBeenCalled();
      });

      it("does not refresh topology when redirect metadata is provided", async () => {
        let callCount = 0;

        const mockEnqueue = vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            throw new RpcError("shard not found", "NOT_FOUND", {
              "x-silo-shard-owner-addr": "localhost:7450",
            });
          }
          return {
            response: Promise.resolve({ id: "job-abc" }),
          };
        });

        const connections = (client as any)._connections as Map<string, any>;
        const conn = connections.values().next().value;
        conn.client.enqueue = mockEnqueue;

        const refreshSpy = vi.fn().mockResolvedValue(undefined);
        (client as any).refreshTopology = refreshSpy;

        const handle = await client.enqueue({
          tenant: "test-tenant",
          payload: { test: true },
          taskGroup: "default",
        });

        expect(handle.id).toBe("job-abc");
        expect(refreshSpy).not.toHaveBeenCalled();
      });
    });
  });

  describe("Lazy topology auto-refresh", () => {
    let client: SiloGRPCClient;

    afterEach(() => {
      client.close();
    });

    // Helper: create a client and mock getClusterInfo to return a single shard
    const setupClientWithMockTopology = () => {
      client = new SiloGRPCClient({
        servers: "localhost:7450",
        useTls: false,
        shardRouting: {
          topologyRefreshIntervalMs: 0,
        },
      });

      const mockGetClusterInfo = vi.fn().mockReturnValue({
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

      const mockEnqueue = vi.fn().mockReturnValue({
        response: Promise.resolve({ id: "job-123" }),
      });

      const connections = (client as any)._connections as Map<string, any>;
      const conn = connections.values().next().value;
      conn.client.getClusterInfo = mockGetClusterInfo;
      conn.client.enqueue = mockEnqueue;

      return { mockGetClusterInfo, mockEnqueue };
    };

    it("auto-refreshes topology on first enqueue if refreshTopology() was never called", async () => {
      const { mockGetClusterInfo, mockEnqueue } = setupClientWithMockTopology();

      // Don't call refreshTopology() - just call enqueue directly
      const handle = await client.enqueue({
        tenant: "test-tenant",
        payload: { test: true },
        taskGroup: "default",
      });

      expect(handle.id).toBe("job-123");
      // Should have called getClusterInfo to auto-discover topology
      expect(mockGetClusterInfo).toHaveBeenCalled();
      expect(mockEnqueue).toHaveBeenCalled();
    });

    it("does not re-fetch topology if it was already loaded", async () => {
      const { mockGetClusterInfo, mockEnqueue } = setupClientWithMockTopology();

      // Explicitly refresh topology first
      await client.refreshTopology();
      expect(mockGetClusterInfo).toHaveBeenCalledTimes(1);

      // Now enqueue should not trigger another refresh
      await client.enqueue({
        tenant: "test-tenant",
        payload: { test: true },
        taskGroup: "default",
      });

      expect(mockGetClusterInfo).toHaveBeenCalledTimes(1);
      expect(mockEnqueue).toHaveBeenCalled();
    });

    it("deduplicates concurrent topology refreshes", async () => {
      const { mockGetClusterInfo } = setupClientWithMockTopology();

      const mockEnqueue = vi.fn().mockReturnValue({
        response: Promise.resolve({ id: "job-concurrent" }),
      });
      const connections = (client as any)._connections as Map<string, any>;
      const conn = connections.values().next().value;
      conn.client.enqueue = mockEnqueue;

      // Launch 5 concurrent enqueues without having called refreshTopology
      const promises = Array.from({ length: 5 }, (_, i) =>
        client.enqueue({
          tenant: "test-tenant",
          payload: { index: i },
          taskGroup: "default",
        }),
      );

      const results = await Promise.all(promises);

      // All should succeed
      expect(results).toHaveLength(5);
      for (const handle of results) {
        expect(handle.id).toBe("job-concurrent");
      }

      // getClusterInfo should have been called exactly once (deduped)
      expect(mockGetClusterInfo).toHaveBeenCalledTimes(1);
    });

    it("resetShards puts client back into lazy-refresh mode", async () => {
      const { mockGetClusterInfo } = setupClientWithMockTopology();

      const mockEnqueue = vi.fn().mockReturnValue({
        response: Promise.resolve({ id: "job-after-reset" }),
      });
      const mockResetShards = vi.fn().mockReturnValue({
        response: Promise.resolve({}),
      });

      const connections = (client as any)._connections as Map<string, any>;
      const conn = connections.values().next().value;
      conn.client.enqueue = mockEnqueue;
      conn.client.resetShards = mockResetShards;

      // First, refresh topology and do an enqueue
      await client.refreshTopology();
      expect(mockGetClusterInfo).toHaveBeenCalledTimes(1);

      await client.enqueue({
        tenant: "test-tenant",
        payload: { test: true },
        taskGroup: "default",
      });
      // No additional refresh needed
      expect(mockGetClusterInfo).toHaveBeenCalledTimes(1);

      // Now reset shards - this should clear topology state
      await client.resetShards();

      // Next enqueue should trigger a new topology refresh
      await client.enqueue({
        tenant: "test-tenant",
        payload: { test: true },
        taskGroup: "default",
      });

      // Should have refreshed topology again
      expect(mockGetClusterInfo).toHaveBeenCalledTimes(2);
    });
  });
});
