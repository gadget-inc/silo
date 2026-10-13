import { describe, it, expect, afterEach, vi } from "vitest";
import net from "node:net";
import {
  Server,
  ServerCredentials,
  type ServiceDefinition,
  type UntypedServiceImplementation,
  type ServerUnaryCall,
  type sendUnaryData,
} from "@grpc/grpc-js";
import { SiloGRPCClient } from "../src/client";
import { Silo } from "../src/pb/silo";

const SHARD = "00000000-0000-0000-0000-000000000001";

/**
 * A stand-in Silo server that owns one shard and answers every unary call with
 * an empty message. It sits behind a TCP proxy that counts the connections the
 * client opens, and it records which of those connections each call came in on.
 */
async function startFakeSilo() {
  const callsByMethod: Record<string, string[]> = {};
  let address = "";

  const definition: Record<string, ServiceDefinition[string]> = {};
  const implementation: UntypedServiceImplementation = {};
  for (const method of Silo.methods) {
    if (method.serverStreaming || method.clientStreaming) continue;
    definition[method.name] = {
      path: `/${Silo.typeName}/${method.name}`,
      requestStream: false,
      responseStream: false,
      requestSerialize: (value: object) => Buffer.from(method.I.toBinary(value)),
      requestDeserialize: (bytes: Buffer) => method.I.fromBinary(bytes),
      responseSerialize: (value: object) => Buffer.from(method.O.toBinary(value)),
      responseDeserialize: (bytes: Buffer) => method.O.fromBinary(bytes),
    };
    implementation[method.name] = (
      call: ServerUnaryCall<object, object>,
      callback: sendUnaryData<object>,
    ) => {
      // The peer is the proxy's socket for this call's client connection
      (callsByMethod[method.name] ??= []).push(call.getPeer());
      if (method.name === "GetClusterInfo") {
        callback(
          null,
          method.O.create({
            numShards: 1,
            shardOwners: [
              { shardId: SHARD, grpcAddr: address, nodeId: "node-1", rangeStart: "", rangeEnd: "" },
            ],
            thisNodeId: "node-1",
            thisGrpcAddr: address,
          }),
        );
        return;
      }
      callback(null, method.O.create());
    };
  }

  const server = new Server();
  server.addService(definition, implementation);
  const serverPort = await new Promise<number>((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, port) =>
      error ? reject(error) : resolve(port),
    );
  });

  let connectionsOpened = 0;
  const clientSockets = new Set<net.Socket>();
  const proxy = net.createServer((downstream) => {
    connectionsOpened++;
    clientSockets.add(downstream);
    const upstream = net.connect(serverPort, "127.0.0.1");
    downstream.on("error", () => {});
    upstream.on("error", () => {});
    downstream.on("close", () => {
      clientSockets.delete(downstream);
      upstream.destroy();
    });
    upstream.on("close", () => downstream.destroy());
    downstream.pipe(upstream);
    upstream.pipe(downstream);
  });
  await new Promise<void>((resolve) => proxy.listen(0, "127.0.0.1", resolve));
  address = `127.0.0.1:${(proxy.address() as net.AddressInfo).port}`;

  return {
    address,
    /** How many TCP connections clients have opened so far */
    connectionsOpened: () => connectionsOpened,
    /** How many of them are still open */
    connectionsOpen: () => clientSockets.size,
    /** The connections that carried calls to `method`, deduplicated */
    connectionsFor: (...methods: string[]) =>
      new Set(methods.flatMap((method) => callsByMethod[method] ?? [])),
    close: () => {
      for (const socket of clientSockets) socket.destroy();
      proxy.close();
      server.forceShutdown();
    },
  };
}

describe("lease connection", () => {
  const cleanups: Array<() => void> = [];
  afterEach(() => {
    for (const cleanup of cleanups.splice(0).reverse()) cleanup();
  });

  const setup = async () => {
    const silo = await startFakeSilo();
    cleanups.push(silo.close);
    const createClient = () => {
      const client = new SiloGRPCClient({
        servers: silo.address,
        useTls: false,
        shardRouting: { topologyRefreshIntervalMs: 0 },
      });
      cleanups.push(() => client.close());
      return client;
    };
    return { silo, createClient };
  };

  it("sends heartbeats and outcome reports on a connection no other call uses", async () => {
    const { silo, createClient } = await setup();
    const client = createClient();

    await client.enqueue({ payload: { n: 1 }, taskGroup: "default" });
    await client.leaseTasks({ workerId: "worker-1", maxTasks: 1, taskGroup: "default" });
    await client.heartbeat("worker-1", "task-1", SHARD);
    await client.reportOutcome({
      taskId: "task-1",
      shard: SHARD,
      outcome: { type: "success", result: {} },
    });
    await client.reportRefreshOutcome({
      taskId: "task-2",
      shard: SHARD,
      outcome: { type: "success", newMaxConcurrency: 1 },
    });
    await client.enqueue({ payload: { n: 2 }, taskGroup: "default" });

    const regular = silo.connectionsFor("GetClusterInfo", "Enqueue", "LeaseTasks");
    const lease = silo.connectionsFor("Heartbeat", "ReportOutcome", "ReportRefreshOutcome");
    expect(regular.size).toBe(1);
    expect(lease.size).toBe(1);
    expect([...lease][0]).not.toBe([...regular][0]);
    expect(silo.connectionsOpened()).toBe(2);
  });

  it("opens the lease connection only once a lease call is made", async () => {
    const { silo, createClient } = await setup();
    const client = createClient();

    await client.enqueue({ payload: { n: 1 }, taskGroup: "default" });
    await client.leaseTasks({ workerId: "worker-1", maxTasks: 1, taskGroup: "default" });
    expect(silo.connectionsOpened()).toBe(1);

    await client.heartbeat("worker-1", "task-1", SHARD);
    expect(silo.connectionsOpened()).toBe(2);
  });

  it("keeps lease calls apart from another client's calls in the same process", async () => {
    const { silo, createClient } = await setup();
    const app = createClient();
    const worker = createClient();

    // grpc-js gives two clients with the same target and options one connection,
    // so without a separate lease connection the worker's heartbeat would share
    // a connection with the app's enqueues.
    await app.enqueue({ payload: { n: 1 }, taskGroup: "default" });
    await worker.leaseTasks({ workerId: "worker-1", maxTasks: 1, taskGroup: "default" });
    expect(silo.connectionsOpened()).toBe(1);

    await worker.heartbeat("worker-1", "task-1", SHARD);
    const regular = silo.connectionsFor("Enqueue", "LeaseTasks");
    const lease = silo.connectionsFor("Heartbeat");
    expect(regular.size).toBe(1);
    expect(lease.size).toBe(1);
    expect([...lease][0]).not.toBe([...regular][0]);
  });

  it("closes the lease connection with the client", async () => {
    const { silo, createClient } = await setup();
    const client = createClient();
    await client.enqueue({ payload: { n: 1 }, taskGroup: "default" });
    await client.heartbeat("worker-1", "task-1", SHARD);
    expect(silo.connectionsOpen()).toBe(2);

    client.close();

    await vi.waitFor(() => expect(silo.connectionsOpen()).toBe(0), { timeout: 5000 });
  });
});
