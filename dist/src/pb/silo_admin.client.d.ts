import type { RpcTransport } from "@protobuf-ts/runtime-rpc";
import type { ServiceInfo } from "@protobuf-ts/runtime-rpc";
import type { ReadLsmStateResponse } from "./silo_admin";
import type { ReadLsmStateRequest } from "./silo_admin";
import type { CompactShardResponse } from "./silo_admin";
import type { CompactShardRequest } from "./silo_admin";
import type { ForceReleaseShardResponse } from "./silo_admin";
import type { ForceReleaseShardRequest } from "./silo_admin";
import type { ResetShardsResponse } from "./silo_admin";
import type { ResetShardsRequest } from "./silo_admin";
import type { ImportJobsResponse } from "./silo_admin";
import type { ImportJobsRequest } from "./silo_admin";
import type { ConfigureShardResponse } from "./silo_admin";
import type { ConfigureShardRequest } from "./silo_admin";
import type { GetSplitStatusResponse } from "./silo_admin";
import type { GetSplitStatusRequest } from "./silo_admin";
import type { RequestSplitResponse } from "./silo_admin";
import type { RequestSplitRequest } from "./silo_admin";
import type { CpuProfileResponse } from "./silo_admin";
import type { CpuProfileRequest } from "./silo_admin";
import type { GetNodeInfoResponse } from "./silo_admin";
import type { GetNodeInfoRequest } from "./silo_admin";
import type { UnaryCall } from "@protobuf-ts/runtime-rpc";
import type { RpcOptions } from "@protobuf-ts/runtime-rpc";
/**
 * Admin service for operator/management operations.
 * Used by the web UI, siloctl CLI, and monitoring tools.
 * These RPCs are not part of the worker-facing API.
 *
 * @generated from protobuf service silo.v1.SiloAdmin
 */
export interface ISiloAdminClient {
    /**
     * Get information about this node and all the shards it owns.
     *
     * @generated from protobuf rpc: GetNodeInfo
     */
    getNodeInfo(input: GetNodeInfoRequest, options?: RpcOptions): UnaryCall<GetNodeInfoRequest, GetNodeInfoResponse>;
    /**
     * Capture a CPU profile from this node.
     * Returns pprof protobuf data that can be analyzed with pprof or go tool pprof.
     * The profile captures CPU usage for the specified duration.
     *
     * @generated from protobuf rpc: CpuProfile
     */
    cpuProfile(input: CpuProfileRequest, options?: RpcOptions): UnaryCall<CpuProfileRequest, CpuProfileResponse>;
    /**
     * Request a shard split operation.
     * Initiates splitting a shard into two child shards at the specified split point.
     * Returns FAILED_PRECONDITION if a split is already in progress.
     * Returns NOT_FOUND if the shard doesn't exist on this node.
     * Returns INVALID_ARGUMENT if the split point is outside the shard's range.
     *
     * @generated from protobuf rpc: RequestSplit
     */
    requestSplit(input: RequestSplitRequest, options?: RpcOptions): UnaryCall<RequestSplitRequest, RequestSplitResponse>;
    /**
     * Get the status of a shard split operation.
     * Returns the current phase and child shard IDs if a split is in progress.
     * If no split is in progress, returns with in_progress=false.
     *
     * @generated from protobuf rpc: GetSplitStatus
     */
    getSplitStatus(input: GetSplitStatusRequest, options?: RpcOptions): UnaryCall<GetSplitStatusRequest, GetSplitStatusResponse>;
    /**
     * Configure a shard's placement ring.
     * Changes which placement ring the shard belongs to, affecting which nodes can own it.
     * The shard will be handed off to a node that participates in the new ring.
     * Returns the previous and current ring assignments.
     *
     * @generated from protobuf rpc: ConfigureShard
     */
    configureShard(input: ConfigureShardRequest, options?: RpcOptions): UnaryCall<ConfigureShardRequest, ConfigureShardResponse>;
    /**
     * Import jobs from another system with historical attempts.
     * Unlike Enqueue, ImportJobs accepts completed attempt records and lets Silo
     * take ownership going forward. Used for migrating workloads from other job queues.
     * Each job is imported independently; per-job errors are returned in the response.
     *
     * @generated from protobuf rpc: ImportJobs
     */
    importJobs(input: ImportJobsRequest, options?: RpcOptions): UnaryCall<ImportJobsRequest, ImportJobsResponse>;
    /**
     * Reset all shards owned by this server.
     * WARNING: Destructive operation. Only available in dev mode.
     * Clears all jobs, tasks, queues, and other data.
     *
     * @generated from protobuf rpc: ResetShards
     */
    resetShards(input: ResetShardsRequest, options?: RpcOptions): UnaryCall<ResetShardsRequest, ResetShardsResponse>;
    /**
     * Force-release a shard lease regardless of the current holder.
     * Operator escape hatch for recovering from permanently lost nodes.
     * After force-release, any live node that desires the shard can acquire it.
     *
     * @generated from protobuf rpc: ForceReleaseShard
     */
    forceReleaseShard(input: ForceReleaseShardRequest, options?: RpcOptions): UnaryCall<ForceReleaseShardRequest, ForceReleaseShardResponse>;
    /**
     * Trigger a full compaction on a shard's SlateDB instance.
     * Merges all L0 SSTs and sorted runs into a single sorted run, removing tombstones.
     * This is useful for reclaiming space after bulk deletes or high job throughput.
     *
     * @generated from protobuf rpc: CompactShard
     */
    compactShard(input: CompactShardRequest, options?: RpcOptions): UnaryCall<CompactShardRequest, CompactShardResponse>;
    /**
     * Read the LSM tree state of a shard's SlateDB instance.
     * Returns information about L0 SSTs and sorted runs to help operators
     * understand storage health and whether compaction is needed.
     *
     * @generated from protobuf rpc: ReadLsmState
     */
    readLsmState(input: ReadLsmStateRequest, options?: RpcOptions): UnaryCall<ReadLsmStateRequest, ReadLsmStateResponse>;
}
/**
 * Admin service for operator/management operations.
 * Used by the web UI, siloctl CLI, and monitoring tools.
 * These RPCs are not part of the worker-facing API.
 *
 * @generated from protobuf service silo.v1.SiloAdmin
 */
export declare class SiloAdminClient implements ISiloAdminClient, ServiceInfo {
    private readonly _transport;
    typeName: string;
    methods: import("@protobuf-ts/runtime-rpc").MethodInfo<any, any>[];
    options: {
        [extensionName: string]: import("@protobuf-ts/runtime").JsonValue;
    };
    constructor(_transport: RpcTransport);
    /**
     * Get information about this node and all the shards it owns.
     *
     * @generated from protobuf rpc: GetNodeInfo
     */
    getNodeInfo(input: GetNodeInfoRequest, options?: RpcOptions): UnaryCall<GetNodeInfoRequest, GetNodeInfoResponse>;
    /**
     * Capture a CPU profile from this node.
     * Returns pprof protobuf data that can be analyzed with pprof or go tool pprof.
     * The profile captures CPU usage for the specified duration.
     *
     * @generated from protobuf rpc: CpuProfile
     */
    cpuProfile(input: CpuProfileRequest, options?: RpcOptions): UnaryCall<CpuProfileRequest, CpuProfileResponse>;
    /**
     * Request a shard split operation.
     * Initiates splitting a shard into two child shards at the specified split point.
     * Returns FAILED_PRECONDITION if a split is already in progress.
     * Returns NOT_FOUND if the shard doesn't exist on this node.
     * Returns INVALID_ARGUMENT if the split point is outside the shard's range.
     *
     * @generated from protobuf rpc: RequestSplit
     */
    requestSplit(input: RequestSplitRequest, options?: RpcOptions): UnaryCall<RequestSplitRequest, RequestSplitResponse>;
    /**
     * Get the status of a shard split operation.
     * Returns the current phase and child shard IDs if a split is in progress.
     * If no split is in progress, returns with in_progress=false.
     *
     * @generated from protobuf rpc: GetSplitStatus
     */
    getSplitStatus(input: GetSplitStatusRequest, options?: RpcOptions): UnaryCall<GetSplitStatusRequest, GetSplitStatusResponse>;
    /**
     * Configure a shard's placement ring.
     * Changes which placement ring the shard belongs to, affecting which nodes can own it.
     * The shard will be handed off to a node that participates in the new ring.
     * Returns the previous and current ring assignments.
     *
     * @generated from protobuf rpc: ConfigureShard
     */
    configureShard(input: ConfigureShardRequest, options?: RpcOptions): UnaryCall<ConfigureShardRequest, ConfigureShardResponse>;
    /**
     * Import jobs from another system with historical attempts.
     * Unlike Enqueue, ImportJobs accepts completed attempt records and lets Silo
     * take ownership going forward. Used for migrating workloads from other job queues.
     * Each job is imported independently; per-job errors are returned in the response.
     *
     * @generated from protobuf rpc: ImportJobs
     */
    importJobs(input: ImportJobsRequest, options?: RpcOptions): UnaryCall<ImportJobsRequest, ImportJobsResponse>;
    /**
     * Reset all shards owned by this server.
     * WARNING: Destructive operation. Only available in dev mode.
     * Clears all jobs, tasks, queues, and other data.
     *
     * @generated from protobuf rpc: ResetShards
     */
    resetShards(input: ResetShardsRequest, options?: RpcOptions): UnaryCall<ResetShardsRequest, ResetShardsResponse>;
    /**
     * Force-release a shard lease regardless of the current holder.
     * Operator escape hatch for recovering from permanently lost nodes.
     * After force-release, any live node that desires the shard can acquire it.
     *
     * @generated from protobuf rpc: ForceReleaseShard
     */
    forceReleaseShard(input: ForceReleaseShardRequest, options?: RpcOptions): UnaryCall<ForceReleaseShardRequest, ForceReleaseShardResponse>;
    /**
     * Trigger a full compaction on a shard's SlateDB instance.
     * Merges all L0 SSTs and sorted runs into a single sorted run, removing tombstones.
     * This is useful for reclaiming space after bulk deletes or high job throughput.
     *
     * @generated from protobuf rpc: CompactShard
     */
    compactShard(input: CompactShardRequest, options?: RpcOptions): UnaryCall<CompactShardRequest, CompactShardResponse>;
    /**
     * Read the LSM tree state of a shard's SlateDB instance.
     * Returns information about L0 SSTs and sorted runs to help operators
     * understand storage health and whether compaction is needed.
     *
     * @generated from protobuf rpc: ReadLsmState
     */
    readLsmState(input: ReadLsmStateRequest, options?: RpcOptions): UnaryCall<ReadLsmStateRequest, ReadLsmStateResponse>;
}
//# sourceMappingURL=silo_admin.client.d.ts.map