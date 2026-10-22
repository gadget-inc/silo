import { afterEach, describe, expect, it, vi } from "vitest";
import { RpcError } from "@protobuf-ts/runtime-rpc";
import { SiloGRPCClient, SiloInvalidArgumentError, SiloUnavailableError } from "../src/client";
import { CancelPendingJobStatus } from "../src/index";

describe("cancelPendingJobs", () => {
  const clients: SiloGRPCClient[] = [];
  const createClient = () => {
    const client = new SiloGRPCClient({
      servers: "localhost:7450",
      shardRouting: { topologyRefreshIntervalMs: 0 },
    });
    clients.push(client);
    return client;
  };
  afterEach(() => {
    clients.splice(0).forEach((client) => client.close());
  });

  it("routes one batch and preserves ordered per-job outcomes", async () => {
    const client = createClient();
    const results = [
      { id: "pending", status: CancelPendingJobStatus.CANCELLED },
      { id: "running", status: CancelPendingJobStatus.RUNNING },
      { id: "failed", status: CancelPendingJobStatus.FAILED, error: "storage unavailable" },
    ];
    const cancelPendingJobs = vi.fn().mockResolvedValue({ response: { results } });
    const route = vi
      .spyOn(client as any, "_withShardRetry")
      .mockImplementation(async (_tenant, callback: any) => {
        return await callback({ cancelPendingJobs }, "shard-1");
      });
    expect(await client.cancelPendingJobs(["pending", "running", "failed"], "tenant")).toEqual(
      results,
    );
    expect(route).toHaveBeenCalledWith("tenant", expect.any(Function));
    expect(cancelPendingJobs).toHaveBeenCalledOnce();
    expect(cancelPendingJobs).toHaveBeenCalledWith(
      { shard: "shard-1", ids: ["pending", "running", "failed"], tenant: "tenant" },
      undefined,
    );
  });

  it("rejects malformed batches before routing and treats an empty batch as a no-op", async () => {
    const client = createClient();
    const route = vi.spyOn(client as any, "_withShardRetry");
    expect(await client.cancelPendingJobs([])).toEqual([]);
    for (const ids of [
      [""],
      ["duplicate", "duplicate"],
      Array.from({ length: 1001 }, (_, i) => `job-${i}`),
    ]) {
      await expect(client.cancelPendingJobs(ids)).rejects.toBeInstanceOf(SiloInvalidArgumentError);
    }
    expect(route).not.toHaveBeenCalled();
  });

  it("maps whole-request transport errors", async () => {
    const client = createClient();
    vi.spyOn(client as any, "_withShardRetry").mockRejectedValue(
      new RpcError("unavailable", "UNAVAILABLE"),
    );
    await expect(client.cancelPendingJobs(["job"])).rejects.toBeInstanceOf(SiloUnavailableError);
  });
});
