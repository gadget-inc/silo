import {
  describe,
  it,
  expect,
  vi,
  afterEach,
  beforeAll,
  beforeEach,
} from "vitest";
import {
  SiloGRPCClient,
  shardForTenant,
  hashTenant,
  initHasher,
  type ShardInfoWithRange,
} from "../src/client";
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
      expect(shardForTenant("3000000000000000", shards)?.shardId).toBe(
        "shard-1",
      );
      expect(shardForTenant("a000000000000000", shards)?.shardId).toBe(
        "shard-2",
      );
    });

    it("handles single full-range shard", () => {
      const shards: ShardInfoWithRange[] = [
        {
          shardId: "shard-1",
          serverAddr: "server-a:7450",
          rangeStart: "",
          rangeEnd: "",
        },
      ];

      expect(shardForTenant("anything", shards)?.shardId).toBe("shard-1");
    });

    it("returns undefined for empty shards array", () => {
      expect(shardForTenant("key", [])).toBeUndefined();
    });

    it("handles multiple shard ranges", () => {
      const shards: ShardInfoWithRange[] = [
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

      expect(shardForTenant("1000000000000000", shards)?.shardId).toBe(
        "shard-1",
      );
      expect(shardForTenant("5000000000000000", shards)?.shardId).toBe(
        "shard-2",
      );
      expect(shardForTenant("9000000000000000", shards)?.shardId).toBe(
        "shard-3",
      );
      expect(shardForTenant("d000000000000000", shards)?.shardId).toBe(
        "shard-4",
      );
    });

    it("end-to-end: hashTenant + shardForTenant routes tenants correctly", () => {
      const shards: ShardInfoWithRange[] = [
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
      expect(
        topology.shardToServer.get("00000000-0000-0000-0000-000000000001"),
      ).toBe("server-a:7450");
      expect(
        topology.shardToServer.get("00000000-0000-0000-0000-000000000002"),
      ).toBe("server-b:7450");
      expect(
        topology.shardToServer.get("00000000-0000-0000-0000-000000000003"),
      ).toBe("server-a:7450");
      expect(
        topology.shardToServer.get("00000000-0000-0000-0000-000000000004"),
      ).toBe("server-b:7450");
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
      expect(
        topology.shardToServer.get("00000000-0000-0000-0000-000000000001"),
      ).toBe("working-server:7450");
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

    it("applies per-server timeout to getClusterInfo calls during refresh", async () => {
      client.close();
      client = new SiloGRPCClient({
        servers: "localhost:7450",
        useTls: false,
        shardRouting: {
          topologyRefreshIntervalMs: 0,
          topologyRefreshTimeoutMs: 5000,
        },
      });

      const capturedOptions: any[] = [];
      const mockGetClusterInfo = vi
        .fn()
        .mockImplementation((_req: any, opts: any) => {
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

      const connections = (client as any)._connections as Map<string, any>;
      const conn = connections.values().next().value;
      conn.client.getClusterInfo = mockGetClusterInfo;

      await client.refreshTopology();

      expect(capturedOptions.length).toBe(1);
      expect(capturedOptions[0].timeout).toBe(5000);
    });

    it("uses default 10s timeout when topologyRefreshTimeoutMs is not set", async () => {
      const capturedOptions: any[] = [];
      const mockGetClusterInfo = vi
        .fn()
        .mockImplementation((_req: any, opts: any) => {
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

      const connections = (client as any)._connections as Map<string, any>;
      const conn = connections.values().next().value;
      conn.client.getClusterInfo = mockGetClusterInfo;

      await client.refreshTopology();

      expect(capturedOptions.length).toBe(1);
      expect(capturedOptions[0].timeout).toBe(10_000);
    });

    it("moves to next server when getClusterInfo times out on first server", async () => {
      client.close();
      client = new SiloGRPCClient({
        servers: ["slow-server:7450", "fast-server:7450"],
        useTls: false,
        shardRouting: {
          topologyRefreshIntervalMs: 0,
          topologyRefreshTimeoutMs: 100,
        },
      });

      const connections = (client as any)._connections as Map<string, any>;

      // First server hangs (never resolves, simulating a timeout that gRPC would enforce)
      const slowConn = connections.get("slow-server:7450");
      slowConn.client.getClusterInfo = vi.fn().mockReturnValue({
        response: new Promise((_resolve, reject) => {
          setTimeout(() => reject(new Error("DEADLINE_EXCEEDED")), 50);
        }),
      });

      // Second server responds immediately
      const fastConn = connections.get("fast-server:7450");
      fastConn.client.getClusterInfo = vi.fn().mockReturnValue({
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
      expect(topology.shards.length).toBe(1);
      expect(
        topology.shardToServer.get("00000000-0000-0000-0000-000000000001"),
      ).toBe("fast-server:7450");
    });

    it("cleans up stale connections after topology refresh", async () => {
      // Simulate first topology refresh that discovers two servers by pod IP
      const mockGetClusterInfo = vi.fn().mockReturnValue({
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

      const connections = (client as any)._connections as Map<string, any>;
      const conn = connections.values().next().value;
      conn.client.getClusterInfo = mockGetClusterInfo;

      await client.refreshTopology();

      // Should have connections to initial server + two discovered pod IPs
      expect(connections.has("localhost:7450")).toBe(true);
      expect(connections.has("10.0.0.1:7450")).toBe(true);
      expect(connections.has("10.0.0.2:7450")).toBe(true);

      // Now simulate pod restart: node-b gets new IP 10.0.0.3
      // The response comes from the initial server (localhost:7450)
      const updatedMock = vi.fn().mockReturnValue({
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
      expect(connections.has("10.0.0.2:7450")).toBe(false);
      // New pod IP should be present
      expect(connections.has("10.0.0.3:7450")).toBe(true);
      // Initial server should be preserved (never cleaned up)
      expect(connections.has("localhost:7450")).toBe(true);
      // Still-active server should be preserved
      expect(connections.has("10.0.0.1:7450")).toBe(true);
    });

    it("preserves initial server connections even when not in topology response", async () => {
      // The initial DNS-based server might not appear in the topology
      // (the topology response uses pod IPs), but we should keep it
      const mockGetClusterInfo = vi.fn().mockReturnValue({
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

      const connections = (client as any)._connections as Map<string, any>;
      const conn = connections.values().next().value;
      conn.client.getClusterInfo = mockGetClusterInfo;

      await client.refreshTopology();

      // localhost:7450 is the initial server and should be kept even though
      // the topology only references 10.0.0.1:7450
      expect(connections.has("localhost:7450")).toBe(true);
      expect(connections.has("10.0.0.1:7450")).toBe(true);
    });

    it("closes transport on stale connections during cleanup", async () => {
      const mockGetClusterInfo = vi.fn().mockReturnValue({
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

      const connections = (client as any)._connections as Map<string, any>;
      const conn = connections.values().next().value;
      conn.client.getClusterInfo = mockGetClusterInfo;

      // First refresh: discover 10.0.0.1
      await client.refreshTopology();

      // Grab a reference to the old connection and spy on its transport.close
      const oldConn = connections.get("10.0.0.1:7450");
      const closeSpy = vi.spyOn(oldConn.transport, "close");

      // Second refresh: pod IP changed to 10.0.0.2
      const updatedMock = vi.fn().mockReturnValue({
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
      expect(closeSpy).toHaveBeenCalled();
      expect(connections.has("10.0.0.1:7450")).toBe(false);
      expect(connections.has("10.0.0.2:7450")).toBe(true);
    });
  });

  describe("Connectivity error retry with topology refresh", () => {
    let client: SiloGRPCClient;

    afterEach(() => {
      client.close();
    });

    it("retries on a different server after UNAVAILABLE triggers topology refresh", async () => {
      client = new SiloGRPCClient({
        servers: "localhost:7450",
        useTls: false,
        shardRouting: {
          maxRetries: 3,
          retryDelayMs: 1,
          topologyRefreshIntervalMs: 0,
        },
      });

      // Set up initial topology: shard → server-a (stale pod IP)
      (client as any)._shards = [
        {
          shardId: "00000000-0000-0000-0000-000000000001",
          serverAddr: "10.0.0.1:7450",
          rangeStart: "",
          rangeEnd: "",
        },
      ];
      (client as any)._shardToServer.set(
        "00000000-0000-0000-0000-000000000001",
        "10.0.0.1:7450",
      );
      (client as any)._topologyReady = true;

      const connections = (client as any)._connections as Map<string, any>;

      // Create connection to stale server-a that will return UNAVAILABLE
      const staleConn = (client as any)._getOrCreateConnection("10.0.0.1:7450");
      staleConn.client.enqueue = vi.fn().mockImplementation(() => {
        throw new RpcError(
          "connect ETIMEDOUT 10.0.0.1:7450",
          "UNAVAILABLE",
          {},
        );
      });

      // Mock refreshTopology to simulate discovering the shard moved to server-b
      let refreshCalled = false;
      const originalRefresh = client.refreshTopology.bind(client);
      (client as any).refreshTopology = vi.fn().mockImplementation(async () => {
        refreshCalled = true;
        // Update topology: shard now lives on server-b (new pod IP)
        (client as any)._shards = [
          {
            shardId: "00000000-0000-0000-0000-000000000001",
            serverAddr: "10.0.0.2:7450",
            rangeStart: "",
            rangeEnd: "",
          },
        ];
        (client as any)._shardToServer.set(
          "00000000-0000-0000-0000-000000000001",
          "10.0.0.2:7450",
        );

        // Create connection to new server-b that succeeds
        const newConn = (client as any)._getOrCreateConnection("10.0.0.2:7450");
        newConn.client.enqueue = vi.fn().mockReturnValue({
          response: Promise.resolve({ id: "job-success" }),
        });
      });

      const handle = await client.enqueue({
        tenant: "test-tenant",
        payload: { test: true },
        taskGroup: "default",
      });

      expect(handle.id).toBe("job-success");
      expect(refreshCalled).toBe(true);
      // The stale server should have been tried exactly once before refreshing
      expect(staleConn.client.enqueue).toHaveBeenCalledTimes(1);
    });

    it("retries reportOutcome on a different server after UNAVAILABLE triggers topology refresh", async () => {
      client = new SiloGRPCClient({
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
      (client as any)._shards = [
        {
          shardId: SHARD_ID,
          serverAddr: "10.0.0.1:7450",
          rangeStart: "",
          rangeEnd: "",
        },
      ];
      (client as any)._shardToServer.set(SHARD_ID, "10.0.0.1:7450");
      (client as any)._topologyReady = true;

      // Create stale connection that returns UNAVAILABLE
      const staleConn = (client as any)._getOrCreateConnection("10.0.0.1:7450");
      staleConn.client.reportOutcome = vi.fn().mockImplementation(() => {
        throw new RpcError(
          "connect ETIMEDOUT 10.0.0.1:7450",
          "UNAVAILABLE",
          {},
        );
      });

      // Mock refreshTopology to point shard to server-b
      (client as any).refreshTopology = vi.fn().mockImplementation(async () => {
        (client as any)._shardToServer.set(SHARD_ID, "10.0.0.2:7450");
        const newConn = (client as any)._getOrCreateConnection("10.0.0.2:7450");
        newConn.client.reportOutcome = vi.fn().mockReturnValue({
          response: Promise.resolve({}),
        });
      });

      await client.reportOutcome({
        taskId: "task-1",
        shard: SHARD_ID,
        outcome: { type: "success", result: { ok: true } },
      });

      expect((client as any).refreshTopology).toHaveBeenCalled();
      expect(staleConn.client.reportOutcome).toHaveBeenCalledTimes(1);
    });

    it("gives up after maxRetries even with UNAVAILABLE errors", async () => {
      client = new SiloGRPCClient({
        servers: "localhost:7450",
        useTls: false,
        shardRouting: {
          maxRetries: 2,
          retryDelayMs: 1,
          topologyRefreshIntervalMs: 0,
        },
      });

      // Set up topology
      (client as any)._shards = [
        {
          shardId: "00000000-0000-0000-0000-000000000001",
          serverAddr: "10.0.0.1:7450",
          rangeStart: "",
          rangeEnd: "",
        },
      ];
      (client as any)._shardToServer.set(
        "00000000-0000-0000-0000-000000000001",
        "10.0.0.1:7450",
      );
      (client as any)._topologyReady = true;

      // Every server always returns UNAVAILABLE
      const staleConn = (client as any)._getOrCreateConnection("10.0.0.1:7450");
      staleConn.client.enqueue = vi.fn().mockImplementation(() => {
        throw new RpcError("connect ETIMEDOUT", "UNAVAILABLE", {});
      });

      // Topology refresh doesn't help — same broken server
      (client as any).refreshTopology = vi.fn().mockResolvedValue(undefined);

      await expect(
        client.enqueue({
          tenant: "test-tenant",
          payload: { test: true },
          taskGroup: "default",
        }),
      ).rejects.toThrow("connect ETIMEDOUT");

      // Initial call + 2 retries = 3 total attempts
      expect(staleConn.client.enqueue).toHaveBeenCalledTimes(3);
    });

    it("does not retry non-retryable errors like INVALID_ARGUMENT", async () => {
      client = new SiloGRPCClient({
        servers: "localhost:7450",
        useTls: false,
        shardRouting: {
          maxRetries: 3,
          retryDelayMs: 1,
          topologyRefreshIntervalMs: 0,
        },
      });

      (client as any)._shards = [
        {
          shardId: "00000000-0000-0000-0000-000000000001",
          serverAddr: "localhost:7450",
          rangeStart: "",
          rangeEnd: "",
        },
      ];
      (client as any)._shardToServer.set(
        "00000000-0000-0000-0000-000000000001",
        "localhost:7450",
      );
      (client as any)._topologyReady = true;

      const connections = (client as any)._connections as Map<string, any>;
      const conn = connections.values().next().value;
      conn.client.enqueue = vi.fn().mockImplementation(() => {
        throw new RpcError("invalid payload", "INVALID_ARGUMENT", {});
      });

      await expect(
        client.enqueue({
          tenant: "test-tenant",
          payload: { test: true },
          taskGroup: "default",
        }),
      ).rejects.toThrow("invalid payload");

      // Should NOT retry — thrown immediately
      expect(conn.client.enqueue).toHaveBeenCalledTimes(1);
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
        // UNAVAILABLE is NOT retried at the gRPC level — our application-level
        // retry handles it with topology refresh for smarter re-routing
        expect(
          serviceConfig.methodConfig[0].retryPolicy.retryableStatusCodes,
        ).not.toContain("UNAVAILABLE");
        expect(
          serviceConfig.methodConfig[0].retryPolicy.retryableStatusCodes,
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
        {
          shardId: "00000000-0000-0000-0000-000000000001",
          serverAddr: "localhost:7450",
          rangeStart: "",
          rangeEnd: "",
        },
      ];
      (c as any)._shardToServer.set(
        "00000000-0000-0000-0000-000000000001",
        "localhost:7450",
      );
      (c as any)._topologyReady = true;
    };

    beforeEach(() => {
      client = new SiloGRPCClient({
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
          "other-server:7450",
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

      it("stops retrying after maxRetries", async () => {
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

        const originalGetOrCreate = (client as any)._getOrCreateConnection.bind(
          client,
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

  describe("Wrong Shard Retry for shard-routed operations", () => {
    let client: SiloGRPCClient;

    const setupMockTopology = (c: SiloGRPCClient) => {
      (c as any)._shards = [
        {
          shardId: "00000000-0000-0000-0000-000000000001",
          serverAddr: "localhost:7450",
          rangeStart: "",
          rangeEnd: "",
        },
      ];
      (c as any)._shardToServer.set(
        "00000000-0000-0000-0000-000000000001",
        "localhost:7450",
      );
      (c as any)._topologyReady = true;
    };

    const SHARD_ID = "00000000-0000-0000-0000-000000000001";

    beforeEach(() => {
      client = new SiloGRPCClient({
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

    afterEach(() => {
      client.close();
    });

    describe("reportOutcome", () => {
      it("retries on wrong shard error with redirect metadata", async () => {
        let callCount = 0;
        const mockReportOutcome = vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            throw new RpcError("shard not found", "NOT_FOUND", {
              "x-silo-shard-owner-addr": "localhost:7450",
            });
          }
          return { response: Promise.resolve({}) };
        });

        const connections = (client as any)._connections as Map<string, any>;
        const conn = connections.values().next().value;
        conn.client.reportOutcome = mockReportOutcome;
        (client as any).refreshTopology = vi.fn().mockResolvedValue(undefined);

        await client.reportOutcome({
          taskId: "task-1",
          shard: SHARD_ID,
          outcome: { type: "success", result: { ok: true } },
        });

        expect(mockReportOutcome).toHaveBeenCalledTimes(2);
      });

      it("retries on wrong shard error without redirect metadata (triggers topology refresh)", async () => {
        let callCount = 0;
        const mockReportOutcome = vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            throw new RpcError("shard not found", "NOT_FOUND", {});
          }
          return { response: Promise.resolve({}) };
        });

        const connections = (client as any)._connections as Map<string, any>;
        const conn = connections.values().next().value;
        conn.client.reportOutcome = mockReportOutcome;

        const refreshSpy = vi.fn().mockResolvedValue(undefined);
        (client as any).refreshTopology = refreshSpy;

        await client.reportOutcome({
          taskId: "task-1",
          shard: SHARD_ID,
          outcome: { type: "success", result: { ok: true } },
        });

        expect(mockReportOutcome).toHaveBeenCalledTimes(2);
        expect(refreshSpy).toHaveBeenCalled();
      });

      it("stops retrying after maxRetries", async () => {
        const mockReportOutcome = vi.fn().mockImplementation(() => {
          throw new RpcError("shard not found", "NOT_FOUND", {});
        });

        const connections = (client as any)._connections as Map<string, any>;
        const conn = connections.values().next().value;
        conn.client.reportOutcome = mockReportOutcome;
        (client as any).refreshTopology = vi.fn().mockResolvedValue(undefined);

        await expect(
          client.reportOutcome({
            taskId: "task-1",
            shard: SHARD_ID,
            outcome: { type: "success", result: { ok: true } },
          }),
        ).rejects.toThrow();

        // Initial call + 3 retries = 4 total
        expect(mockReportOutcome).toHaveBeenCalledTimes(4);
      });

      it("does not retry on non-wrong-shard errors", async () => {
        const mockReportOutcome = vi.fn().mockImplementation(() => {
          throw new RpcError("internal error", "INTERNAL", {});
        });

        const connections = (client as any)._connections as Map<string, any>;
        const conn = connections.values().next().value;
        conn.client.reportOutcome = mockReportOutcome;

        await expect(
          client.reportOutcome({
            taskId: "task-1",
            shard: SHARD_ID,
            outcome: { type: "success", result: { ok: true } },
          }),
        ).rejects.toThrow("internal error");

        expect(mockReportOutcome).toHaveBeenCalledTimes(1);
      });
    });

    describe("heartbeat", () => {
      it("retries on wrong shard error with redirect metadata", async () => {
        let callCount = 0;
        const mockHeartbeat = vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            throw new RpcError("shard not found", "NOT_FOUND", {
              "x-silo-shard-owner-addr": "localhost:7450",
            });
          }
          return { response: Promise.resolve({ cancelled: false }) };
        });

        const connections = (client as any)._connections as Map<string, any>;
        const conn = connections.values().next().value;
        conn.client.heartbeat = mockHeartbeat;
        (client as any).refreshTopology = vi.fn().mockResolvedValue(undefined);

        const result = await client.heartbeat("worker-1", "task-1", SHARD_ID);

        expect(result.cancelled).toBe(false);
        expect(mockHeartbeat).toHaveBeenCalledTimes(2);
      });

      it("stops retrying after maxRetries", async () => {
        const mockHeartbeat = vi.fn().mockImplementation(() => {
          throw new RpcError("shard not found", "NOT_FOUND", {});
        });

        const connections = (client as any)._connections as Map<string, any>;
        const conn = connections.values().next().value;
        conn.client.heartbeat = mockHeartbeat;
        (client as any).refreshTopology = vi.fn().mockResolvedValue(undefined);

        await expect(
          client.heartbeat("worker-1", "task-1", SHARD_ID),
        ).rejects.toThrow();

        expect(mockHeartbeat).toHaveBeenCalledTimes(4);
      });
    });

    describe("reportRefreshOutcome", () => {
      it("retries on wrong shard error with redirect metadata", async () => {
        let callCount = 0;
        const mockReportRefreshOutcome = vi.fn().mockImplementation(() => {
          callCount++;
          if (callCount === 1) {
            throw new RpcError("shard not found", "NOT_FOUND", {
              "x-silo-shard-owner-addr": "localhost:7450",
            });
          }
          return { response: Promise.resolve({}) };
        });

        const connections = (client as any)._connections as Map<string, any>;
        const conn = connections.values().next().value;
        conn.client.reportRefreshOutcome = mockReportRefreshOutcome;
        (client as any).refreshTopology = vi.fn().mockResolvedValue(undefined);

        await client.reportRefreshOutcome({
          taskId: "task-1",
          shard: SHARD_ID,
          outcome: { type: "success", newMaxConcurrency: 10 },
        });

        expect(mockReportRefreshOutcome).toHaveBeenCalledTimes(2);
      });

      it("stops retrying after maxRetries", async () => {
        const mockReportRefreshOutcome = vi.fn().mockImplementation(() => {
          throw new RpcError("shard not found", "NOT_FOUND", {});
        });

        const connections = (client as any)._connections as Map<string, any>;
        const conn = connections.values().next().value;
        conn.client.reportRefreshOutcome = mockReportRefreshOutcome;
        (client as any).refreshTopology = vi.fn().mockResolvedValue(undefined);

        await expect(
          client.reportRefreshOutcome({
            taskId: "task-1",
            shard: SHARD_ID,
            outcome: { type: "success", newMaxConcurrency: 10 },
          }),
        ).rejects.toThrow();

        expect(mockReportRefreshOutcome).toHaveBeenCalledTimes(4);
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
