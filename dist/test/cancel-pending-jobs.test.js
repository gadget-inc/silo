"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const runtime_rpc_1 = require("@protobuf-ts/runtime-rpc");
const client_1 = require("../src/client");
const index_1 = require("../src/index");
(0, vitest_1.describe)("cancelPendingJobs", () => {
    const clients = [];
    const createClient = () => {
        const client = new client_1.SiloGRPCClient({
            servers: "localhost:7450",
            shardRouting: { topologyRefreshIntervalMs: 0 },
        });
        clients.push(client);
        return client;
    };
    (0, vitest_1.afterEach)(() => {
        clients.splice(0).forEach((client) => client.close());
    });
    (0, vitest_1.it)("routes one batch and preserves ordered per-job outcomes", async () => {
        const client = createClient();
        const results = [
            { id: "pending", status: index_1.CancelPendingJobStatus.CANCELLED },
            { id: "running", status: index_1.CancelPendingJobStatus.RUNNING },
            { id: "failed", status: index_1.CancelPendingJobStatus.FAILED, error: "storage unavailable" },
        ];
        const cancelPendingJobs = vitest_1.vi.fn().mockResolvedValue({ response: { results } });
        const route = vitest_1.vi
            .spyOn(client, "_withShardRetry")
            .mockImplementation(async (_tenant, callback) => {
            return await callback({ cancelPendingJobs }, "shard-1");
        });
        (0, vitest_1.expect)(await client.cancelPendingJobs(["pending", "running", "failed"], "tenant")).toEqual(results);
        (0, vitest_1.expect)(route).toHaveBeenCalledWith("tenant", vitest_1.expect.any(Function));
        (0, vitest_1.expect)(cancelPendingJobs).toHaveBeenCalledOnce();
        (0, vitest_1.expect)(cancelPendingJobs).toHaveBeenCalledWith({ shard: "shard-1", ids: ["pending", "running", "failed"], tenant: "tenant" }, undefined);
    });
    (0, vitest_1.it)("rejects malformed batches before routing and treats an empty batch as a no-op", async () => {
        const client = createClient();
        const route = vitest_1.vi.spyOn(client, "_withShardRetry");
        (0, vitest_1.expect)(await client.cancelPendingJobs([])).toEqual([]);
        for (const ids of [
            [""],
            ["duplicate", "duplicate"],
            Array.from({ length: 1001 }, (_, i) => `job-${i}`),
        ]) {
            await (0, vitest_1.expect)(client.cancelPendingJobs(ids)).rejects.toBeInstanceOf(client_1.SiloInvalidArgumentError);
        }
        (0, vitest_1.expect)(route).not.toHaveBeenCalled();
    });
    (0, vitest_1.it)("maps whole-request transport errors", async () => {
        const client = createClient();
        vitest_1.vi.spyOn(client, "_withShardRetry").mockRejectedValue(new runtime_rpc_1.RpcError("unavailable", "UNAVAILABLE"));
        await (0, vitest_1.expect)(client.cancelPendingJobs(["job"])).rejects.toBeInstanceOf(client_1.SiloUnavailableError);
    });
});
//# sourceMappingURL=cancel-pending-jobs.test.js.map