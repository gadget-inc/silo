import { describe, it, expect, vi, afterEach, beforeEach } from "vitest";
import { SiloGRPCClient, shardForTenant, type ShardInfoWithRange } from "../src/client";
import { RpcError } from "@protobuf-ts/runtime-rpc";

describe("Shard Routing", () => {
  describe("shardForTenant range-based lookup", () => {
    it("finds correct shard for tenant in range", () => {
      const shards: ShardInfoWithRange[] = [
        { shardId: "shard-1", serverAddr: "server-a:7450", rangeStart: "", rangeEnd: "m" },
        { shardId: "shard-2", serverAddr: "server-b:7450", rangeStart: "m", rangeEnd: "" },
      ];

      // "apple" comes before "m"
      expect(shardForTenant("apple", shards)?.shardId).toBe("shard-1");
      // "zebra" comes after "m"
      expect(shardForTenant("zebra", shards)?.shardId).toBe("shard-2");
    });

    it("handles empty rangeStart (no lower bound)", () => {
      const shards: ShardInfoWithRange[] = [
        { shardId: "shard-1", serverAddr: "server-a:7450", rangeStart: "", rangeEnd: "" },
      ];

      expect(shardForTenant("any-tenant", shards)?.shardId).toBe("shard-1");
    });

    it("handles empty rangeEnd (no upper bound)", () => {
      const shards: ShardInfoWithRange[] = [
        { shardId: "shard-1", serverAddr: "server-a:7450", rangeStart: "a", rangeEnd: "" },
      ];

      expect(shardForTenant("zebra", shards)?.shardId).toBe("shard-1");
    });

    it("returns undefined for empty shards array", () => {
      expect(shardForTenant("tenant", [])).toBeUndefined();
    });

    it("handles multiple shard ranges", () => {
      const shards: ShardInfoWithRange[] = [
        { shardId: "shard-1", serverAddr: "server-a:7450", rangeStart: "", rangeEnd: "4" },
        { shardId: "shard-2", serverAddr: "server-b:7450", rangeStart: "4", rangeEnd: "8" },
        { shardId: "shard-3", serverAddr: "server-a:7450", rangeStart: "8", rangeEnd: "c" },
        { shardId: "shard-4", serverAddr: "server-b:7450", rangeStart: "c", rangeEnd: "" },
      ];

      expect(shardForTenant("1", shards)?.shardId).toBe("shard-1");
      expect(shardForTenant("5", shards)?.shardId).toBe("shard-2");
      expect(shardForTenant("9", shards)?.shardId).toBe("shard-3");
      expect(shardForTenant("d", shards)?.shardId).toBe("shard-4");
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
            { shardId: "00000000-0000-0000-0000-000000000001", grpcAddr: "server-a:7450", nodeId: "node-a", rangeStart: "", rangeEnd: "4" },
            { shardId: "00000000-0000-0000-0000-000000000002", grpcAddr: "server-b:7450", nodeId: "node-b", rangeStart: "4", rangeEnd: "8" },
            { shardId: "00000000-0000-0000-0000-000000000003", grpcAddr: "server-a:7450", nodeId: "node-a", rangeStart: "8", rangeEnd: "c" },
            { shardId: "00000000-0000-0000-0000-000000000004", grpcAddr: "server-b:7450", nodeId: "node-b", rangeStart: "c", rangeEnd: "" },
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
      expect(topology.shardToServer.get("00000000-0000-0000-0000-000000000001")).toBe("server-a:7450");
      expect(topology.shardToServer.get("00000000-0000-0000-0000-000000000002")).toBe("server-b:7450");
      expect(topology.shardToServer.get("00000000-0000-0000-0000-000000000003")).toBe("server-a:7450");
      expect(topology.shardToServer.get("00000000-0000-0000-0000-000000000004")).toBe("server-b:7450");
    });

    it("creates connections to discovered servers", async () => {
      const mockGetClusterInfo = vi.fn().mockReturnValue({
        response: Promise.resolve({
          numShards: 2,
          shardOwners: [
            { shardId: "00000000-0000-0000-0000-000000000001", grpcAddr: "server-x:7450", nodeId: "node-x", rangeStart: "", rangeEnd: "m" },
            { shardId: "00000000-0000-0000-0000-000000000002", grpcAddr: "server-y:7450", nodeId: "node-y", rangeStart: "m", rangeEnd: "" },
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
            { shardId: "00000000-0000-0000-0000-000000000001", grpcAddr: "working-server:7450", nodeId: "node-1", rangeStart: "", rangeEnd: "" },
          ],
          thisNodeId: "node-1",
          thisGrpcAddr: "working-server:7450",
        }),
      });

      await client.refreshTopology();

      const topology = client.getTopology();
      expect(topology.shards.length).toBe(1);
      expect(topology.shardToServer.get("00000000-0000-0000-0000-000000000001")).toBe("working-server:7450");
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
        "Failed to refresh cluster topology from any server"
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
        expect(
          serviceConfig.methodConfig[0].retryPolicy.retryableStatusCodes
        ).toContain("UNAVAILABLE");
        expect(
          serviceConfig.methodConfig[0].retryPolicy.retryableStatusCodes
        ).toContain("RESOURCE_EXHAUSTED");
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
        { shardId: "00000000-0000-0000-0000-000000000001", serverAddr: "localhost:7450", rangeStart: "", rangeEnd: "" }
      ];
      (c as any)._shardToServer.set("00000000-0000-0000-0000-000000000001", "localhost:7450");
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

        expect(error.meta?.["x-silo-shard-owner-addr"]).toBe(
          "other-server:7450"
        );
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
          })
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
          })
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

        const originalGetOrCreate = (client as any)._getOrCreateConnection.bind(
          client
        );
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
});
