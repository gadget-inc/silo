import { ServiceType } from "@protobuf-ts/runtime-rpc";
import type { BinaryWriteOptions } from "@protobuf-ts/runtime";
import type { IBinaryWriter } from "@protobuf-ts/runtime";
import type { BinaryReadOptions } from "@protobuf-ts/runtime";
import type { IBinaryReader } from "@protobuf-ts/runtime";
import type { PartialMessage } from "@protobuf-ts/runtime";
import { MessageType } from "@protobuf-ts/runtime";
import { JobStatus } from "./silo";
import { Limit } from "./silo";
import { RetryPolicy } from "./silo";
import { SerializedBytes } from "./silo";
import { AttemptStatus } from "./silo";
/**
 * Request to reset all shards owned by this server.
 * WARNING: Destructive operation. Only available in dev mode.
 *
 * @generated from protobuf message silo.v1.ResetShardsRequest
 */
export interface ResetShardsRequest {
}
/**
 * Response confirming shards were reset.
 *
 * @generated from protobuf message silo.v1.ResetShardsResponse
 */
export interface ResetShardsResponse {
    /**
     * @generated from protobuf field: uint32 shards_reset = 1
     */
    shardsReset: number;
}
/**
 * Request to capture a CPU profile from this node.
 * Used for production debugging and performance analysis.
 *
 * @generated from protobuf message silo.v1.CpuProfileRequest
 */
export interface CpuProfileRequest {
    /**
     * @generated from protobuf field: uint32 duration_seconds = 1
     */
    durationSeconds: number;
    /**
     * @generated from protobuf field: uint32 frequency = 2
     */
    frequency: number;
}
/**
 * CPU profile data in pprof protobuf format.
 * Can be analyzed with `pprof` or `go tool pprof`.
 *
 * @generated from protobuf message silo.v1.CpuProfileResponse
 */
export interface CpuProfileResponse {
    /**
     * @generated from protobuf field: bytes profile_data = 1
     */
    profileData: Uint8Array;
    /**
     * @generated from protobuf field: uint32 duration_seconds = 2
     */
    durationSeconds: number;
    /**
     * @generated from protobuf field: uint64 samples = 3
     */
    samples: bigint;
}
/**
 * Request to initiate a shard split operation.
 *
 * @generated from protobuf message silo.v1.RequestSplitRequest
 */
export interface RequestSplitRequest {
    /**
     * @generated from protobuf field: string shard_id = 1
     */
    shardId: string;
    /**
     * @generated from protobuf field: string split_point = 2
     */
    splitPoint: string;
}
/**
 * Response after initiating a shard split.
 *
 * @generated from protobuf message silo.v1.RequestSplitResponse
 */
export interface RequestSplitResponse {
    /**
     * @generated from protobuf field: string left_child_id = 1
     */
    leftChildId: string;
    /**
     * @generated from protobuf field: string right_child_id = 2
     */
    rightChildId: string;
    /**
     * @generated from protobuf field: string phase = 3
     */
    phase: string;
}
/**
 * Request to get the status of a shard split operation.
 *
 * @generated from protobuf message silo.v1.GetSplitStatusRequest
 */
export interface GetSplitStatusRequest {
    /**
     * @generated from protobuf field: string shard_id = 1
     */
    shardId: string;
}
/**
 * Response with the current split status.
 * Returns empty if no split is in progress for the shard.
 *
 * @generated from protobuf message silo.v1.GetSplitStatusResponse
 */
export interface GetSplitStatusResponse {
    /**
     * @generated from protobuf field: bool in_progress = 1
     */
    inProgress: boolean;
    /**
     * @generated from protobuf field: string phase = 2
     */
    phase: string;
    /**
     * @generated from protobuf field: string left_child_id = 3
     */
    leftChildId: string;
    /**
     * @generated from protobuf field: string right_child_id = 4
     */
    rightChildId: string;
    /**
     * @generated from protobuf field: string split_point = 5
     */
    splitPoint: string;
    /**
     * @generated from protobuf field: string initiator_node_id = 6
     */
    initiatorNodeId: string;
    /**
     * @generated from protobuf field: int64 requested_at_ms = 7
     */
    requestedAtMs: bigint;
}
/**
 * Information about a shard owned by a node, including counters and cleanup status.
 *
 * @generated from protobuf message silo.v1.OwnedShardInfo
 */
export interface OwnedShardInfo {
    /**
     * @generated from protobuf field: string shard_id = 1
     */
    shardId: string;
    /**
     * @generated from protobuf field: int64 total_jobs = 2
     */
    totalJobs: bigint;
    /**
     * @generated from protobuf field: int64 completed_jobs = 3
     */
    completedJobs: bigint;
    /**
     * @generated from protobuf field: string cleanup_status = 4
     */
    cleanupStatus: string;
    /**
     * @generated from protobuf field: int64 created_at_ms = 5
     */
    createdAtMs: bigint;
    /**
     * @generated from protobuf field: int64 cleanup_completed_at_ms = 6
     */
    cleanupCompletedAtMs: bigint;
}
/**
 * Request to get node information including owned shards with their counters and cleanup status.
 *
 * @generated from protobuf message silo.v1.GetNodeInfoRequest
 */
export interface GetNodeInfoRequest {
}
/**
 * Response with node information and details for all shards owned by this node.
 *
 * @generated from protobuf message silo.v1.GetNodeInfoResponse
 */
export interface GetNodeInfoResponse {
    /**
     * @generated from protobuf field: string node_id = 1
     */
    nodeId: string;
    /**
     * @generated from protobuf field: repeated silo.v1.OwnedShardInfo owned_shards = 2
     */
    ownedShards: OwnedShardInfo[];
    /**
     * @generated from protobuf field: repeated string placement_rings = 3
     */
    placementRings: string[];
}
/**
 * Request to configure a shard's placement ring.
 *
 * @generated from protobuf message silo.v1.ConfigureShardRequest
 */
export interface ConfigureShardRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
    /**
     * @generated from protobuf field: optional string placement_ring = 2
     */
    placementRing?: string;
    /**
     * @generated from protobuf field: optional string tenant = 100
     */
    tenant?: string;
}
/**
 * Response after configuring a shard's placement ring.
 *
 * @generated from protobuf message silo.v1.ConfigureShardResponse
 */
export interface ConfigureShardResponse {
    /**
     * @generated from protobuf field: string previous_ring = 1
     */
    previousRing: string;
    /**
     * @generated from protobuf field: string current_ring = 2
     */
    currentRing: string;
}
/**
 * A historical attempt record for job import.
 * All attempts must be in terminal states (no Running).
 *
 * @generated from protobuf message silo.v1.ImportAttempt
 */
export interface ImportAttempt {
    /**
     * @generated from protobuf field: silo.v1.AttemptStatus status = 1
     */
    status: AttemptStatus;
    /**
     * @generated from protobuf field: int64 started_at_ms = 2
     */
    startedAtMs: bigint;
    /**
     * @generated from protobuf field: int64 finished_at_ms = 3
     */
    finishedAtMs: bigint;
    /**
     * @generated from protobuf field: optional silo.v1.SerializedBytes result = 4
     */
    result?: SerializedBytes;
    /**
     * @generated from protobuf field: optional string error_code = 5
     */
    errorCode?: string;
    /**
     * @generated from protobuf field: optional silo.v1.SerializedBytes error_data = 6
     */
    errorData?: SerializedBytes;
}
/**
 * Request to import a single job from another system.
 * Unlike Enqueue, ImportJob accepts historical attempts and lets Silo take ownership going forward.
 *
 * @generated from protobuf message silo.v1.ImportJobRequest
 */
export interface ImportJobRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
    /**
     * @generated from protobuf field: string id = 2
     */
    id: string;
    /**
     * @generated from protobuf field: uint32 priority = 3
     */
    priority: number;
    /**
     * @generated from protobuf field: int64 enqueue_time_ms = 4
     */
    enqueueTimeMs: bigint;
    /**
     * @generated from protobuf field: int64 start_at_ms = 5
     */
    startAtMs: bigint;
    /**
     * @generated from protobuf field: optional silo.v1.RetryPolicy retry_policy = 6
     */
    retryPolicy?: RetryPolicy;
    /**
     * @generated from protobuf field: silo.v1.SerializedBytes payload = 7
     */
    payload?: SerializedBytes;
    /**
     * @generated from protobuf field: repeated silo.v1.Limit limits = 8
     */
    limits: Limit[];
    /**
     * @generated from protobuf field: optional string tenant = 9
     */
    tenant?: string;
    /**
     * @generated from protobuf field: map<string, string> metadata = 10
     */
    metadata: {
        [key: string]: string;
    };
    /**
     * @generated from protobuf field: string task_group = 11
     */
    taskGroup: string;
    /**
     * @generated from protobuf field: repeated silo.v1.ImportAttempt attempts = 12
     */
    attempts: ImportAttempt[];
}
/**
 * Batch request to import multiple jobs.
 *
 * @generated from protobuf message silo.v1.ImportJobsRequest
 */
export interface ImportJobsRequest {
    /**
     * @generated from protobuf field: repeated silo.v1.ImportJobRequest jobs = 1
     */
    jobs: ImportJobRequest[];
}
/**
 * Result of importing a single job.
 *
 * @generated from protobuf message silo.v1.ImportJobResult
 */
export interface ImportJobResult {
    /**
     * @generated from protobuf field: string id = 1
     */
    id: string;
    /**
     * @generated from protobuf field: bool success = 2
     */
    success: boolean;
    /**
     * @generated from protobuf field: optional string error = 3
     */
    error?: string;
    /**
     * @generated from protobuf field: silo.v1.JobStatus status = 4
     */
    status: JobStatus;
}
/**
 * Response containing results for each imported job.
 *
 * @generated from protobuf message silo.v1.ImportJobsResponse
 */
export interface ImportJobsResponse {
    /**
     * @generated from protobuf field: repeated silo.v1.ImportJobResult results = 1
     */
    results: ImportJobResult[];
}
/**
 * Request to force-release a shard's ownership lease.
 * Operator escape hatch for permanently lost nodes.
 *
 * @generated from protobuf message silo.v1.ForceReleaseShardRequest
 */
export interface ForceReleaseShardRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
}
/**
 * Response after force-releasing a shard lease.
 *
 * @generated from protobuf message silo.v1.ForceReleaseShardResponse
 */
export interface ForceReleaseShardResponse {
    /**
     * @generated from protobuf field: bool released = 1
     */
    released: boolean;
}
/**
 * Request to trigger a full compaction on a shard's SlateDB instance.
 * Merges all L0 SSTs and sorted runs into a single sorted run, removing tombstones.
 *
 * @generated from protobuf message silo.v1.CompactShardRequest
 */
export interface CompactShardRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
}
/**
 * Response after submitting a full compaction.
 *
 * @generated from protobuf message silo.v1.CompactShardResponse
 */
export interface CompactShardResponse {
    /**
     * @generated from protobuf field: string status = 1
     */
    status: string;
}
/**
 * Request to read the LSM tree state of a shard's SlateDB instance.
 *
 * @generated from protobuf message silo.v1.ReadLsmStateRequest
 */
export interface ReadLsmStateRequest {
    /**
     * @generated from protobuf field: string shard = 1
     */
    shard: string;
}
/**
 * Information about a single SST file in the L0 level.
 *
 * @generated from protobuf message silo.v1.LsmSstInfo
 */
export interface LsmSstInfo {
    /**
     * @generated from protobuf field: string id = 1
     */
    id: string;
    /**
     * @generated from protobuf field: uint64 estimated_size = 2
     */
    estimatedSize: bigint;
}
/**
 * Information about a sorted run (compacted level).
 *
 * @generated from protobuf message silo.v1.LsmSortedRunInfo
 */
export interface LsmSortedRunInfo {
    /**
     * @generated from protobuf field: uint32 id = 1
     */
    id: number;
    /**
     * @generated from protobuf field: uint32 sst_count = 2
     */
    sstCount: number;
    /**
     * @generated from protobuf field: uint64 estimated_size = 3
     */
    estimatedSize: bigint;
}
/**
 * Response with the LSM tree state of a shard.
 *
 * @generated from protobuf message silo.v1.ReadLsmStateResponse
 */
export interface ReadLsmStateResponse {
    /**
     * @generated from protobuf field: repeated silo.v1.LsmSstInfo l0_ssts = 1
     */
    l0Ssts: LsmSstInfo[];
    /**
     * @generated from protobuf field: repeated silo.v1.LsmSortedRunInfo sorted_runs = 2
     */
    sortedRuns: LsmSortedRunInfo[];
    /**
     * @generated from protobuf field: uint64 total_l0_size = 3
     */
    totalL0Size: bigint;
    /**
     * @generated from protobuf field: uint64 total_sorted_run_size = 4
     */
    totalSortedRunSize: bigint;
}
declare class ResetShardsRequest$Type extends MessageType<ResetShardsRequest> {
    constructor();
    create(value?: PartialMessage<ResetShardsRequest>): ResetShardsRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ResetShardsRequest): ResetShardsRequest;
    internalBinaryWrite(message: ResetShardsRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ResetShardsRequest
 */
export declare const ResetShardsRequest: ResetShardsRequest$Type;
declare class ResetShardsResponse$Type extends MessageType<ResetShardsResponse> {
    constructor();
    create(value?: PartialMessage<ResetShardsResponse>): ResetShardsResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ResetShardsResponse): ResetShardsResponse;
    internalBinaryWrite(message: ResetShardsResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ResetShardsResponse
 */
export declare const ResetShardsResponse: ResetShardsResponse$Type;
declare class CpuProfileRequest$Type extends MessageType<CpuProfileRequest> {
    constructor();
    create(value?: PartialMessage<CpuProfileRequest>): CpuProfileRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: CpuProfileRequest): CpuProfileRequest;
    internalBinaryWrite(message: CpuProfileRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.CpuProfileRequest
 */
export declare const CpuProfileRequest: CpuProfileRequest$Type;
declare class CpuProfileResponse$Type extends MessageType<CpuProfileResponse> {
    constructor();
    create(value?: PartialMessage<CpuProfileResponse>): CpuProfileResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: CpuProfileResponse): CpuProfileResponse;
    internalBinaryWrite(message: CpuProfileResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.CpuProfileResponse
 */
export declare const CpuProfileResponse: CpuProfileResponse$Type;
declare class RequestSplitRequest$Type extends MessageType<RequestSplitRequest> {
    constructor();
    create(value?: PartialMessage<RequestSplitRequest>): RequestSplitRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: RequestSplitRequest): RequestSplitRequest;
    internalBinaryWrite(message: RequestSplitRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.RequestSplitRequest
 */
export declare const RequestSplitRequest: RequestSplitRequest$Type;
declare class RequestSplitResponse$Type extends MessageType<RequestSplitResponse> {
    constructor();
    create(value?: PartialMessage<RequestSplitResponse>): RequestSplitResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: RequestSplitResponse): RequestSplitResponse;
    internalBinaryWrite(message: RequestSplitResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.RequestSplitResponse
 */
export declare const RequestSplitResponse: RequestSplitResponse$Type;
declare class GetSplitStatusRequest$Type extends MessageType<GetSplitStatusRequest> {
    constructor();
    create(value?: PartialMessage<GetSplitStatusRequest>): GetSplitStatusRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GetSplitStatusRequest): GetSplitStatusRequest;
    internalBinaryWrite(message: GetSplitStatusRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GetSplitStatusRequest
 */
export declare const GetSplitStatusRequest: GetSplitStatusRequest$Type;
declare class GetSplitStatusResponse$Type extends MessageType<GetSplitStatusResponse> {
    constructor();
    create(value?: PartialMessage<GetSplitStatusResponse>): GetSplitStatusResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GetSplitStatusResponse): GetSplitStatusResponse;
    internalBinaryWrite(message: GetSplitStatusResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GetSplitStatusResponse
 */
export declare const GetSplitStatusResponse: GetSplitStatusResponse$Type;
declare class OwnedShardInfo$Type extends MessageType<OwnedShardInfo> {
    constructor();
    create(value?: PartialMessage<OwnedShardInfo>): OwnedShardInfo;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: OwnedShardInfo): OwnedShardInfo;
    internalBinaryWrite(message: OwnedShardInfo, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.OwnedShardInfo
 */
export declare const OwnedShardInfo: OwnedShardInfo$Type;
declare class GetNodeInfoRequest$Type extends MessageType<GetNodeInfoRequest> {
    constructor();
    create(value?: PartialMessage<GetNodeInfoRequest>): GetNodeInfoRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GetNodeInfoRequest): GetNodeInfoRequest;
    internalBinaryWrite(message: GetNodeInfoRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GetNodeInfoRequest
 */
export declare const GetNodeInfoRequest: GetNodeInfoRequest$Type;
declare class GetNodeInfoResponse$Type extends MessageType<GetNodeInfoResponse> {
    constructor();
    create(value?: PartialMessage<GetNodeInfoResponse>): GetNodeInfoResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: GetNodeInfoResponse): GetNodeInfoResponse;
    internalBinaryWrite(message: GetNodeInfoResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.GetNodeInfoResponse
 */
export declare const GetNodeInfoResponse: GetNodeInfoResponse$Type;
declare class ConfigureShardRequest$Type extends MessageType<ConfigureShardRequest> {
    constructor();
    create(value?: PartialMessage<ConfigureShardRequest>): ConfigureShardRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ConfigureShardRequest): ConfigureShardRequest;
    internalBinaryWrite(message: ConfigureShardRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ConfigureShardRequest
 */
export declare const ConfigureShardRequest: ConfigureShardRequest$Type;
declare class ConfigureShardResponse$Type extends MessageType<ConfigureShardResponse> {
    constructor();
    create(value?: PartialMessage<ConfigureShardResponse>): ConfigureShardResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ConfigureShardResponse): ConfigureShardResponse;
    internalBinaryWrite(message: ConfigureShardResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ConfigureShardResponse
 */
export declare const ConfigureShardResponse: ConfigureShardResponse$Type;
declare class ImportAttempt$Type extends MessageType<ImportAttempt> {
    constructor();
    create(value?: PartialMessage<ImportAttempt>): ImportAttempt;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ImportAttempt): ImportAttempt;
    internalBinaryWrite(message: ImportAttempt, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ImportAttempt
 */
export declare const ImportAttempt: ImportAttempt$Type;
declare class ImportJobRequest$Type extends MessageType<ImportJobRequest> {
    constructor();
    create(value?: PartialMessage<ImportJobRequest>): ImportJobRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ImportJobRequest): ImportJobRequest;
    private binaryReadMap10;
    internalBinaryWrite(message: ImportJobRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ImportJobRequest
 */
export declare const ImportJobRequest: ImportJobRequest$Type;
declare class ImportJobsRequest$Type extends MessageType<ImportJobsRequest> {
    constructor();
    create(value?: PartialMessage<ImportJobsRequest>): ImportJobsRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ImportJobsRequest): ImportJobsRequest;
    internalBinaryWrite(message: ImportJobsRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ImportJobsRequest
 */
export declare const ImportJobsRequest: ImportJobsRequest$Type;
declare class ImportJobResult$Type extends MessageType<ImportJobResult> {
    constructor();
    create(value?: PartialMessage<ImportJobResult>): ImportJobResult;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ImportJobResult): ImportJobResult;
    internalBinaryWrite(message: ImportJobResult, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ImportJobResult
 */
export declare const ImportJobResult: ImportJobResult$Type;
declare class ImportJobsResponse$Type extends MessageType<ImportJobsResponse> {
    constructor();
    create(value?: PartialMessage<ImportJobsResponse>): ImportJobsResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ImportJobsResponse): ImportJobsResponse;
    internalBinaryWrite(message: ImportJobsResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ImportJobsResponse
 */
export declare const ImportJobsResponse: ImportJobsResponse$Type;
declare class ForceReleaseShardRequest$Type extends MessageType<ForceReleaseShardRequest> {
    constructor();
    create(value?: PartialMessage<ForceReleaseShardRequest>): ForceReleaseShardRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ForceReleaseShardRequest): ForceReleaseShardRequest;
    internalBinaryWrite(message: ForceReleaseShardRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ForceReleaseShardRequest
 */
export declare const ForceReleaseShardRequest: ForceReleaseShardRequest$Type;
declare class ForceReleaseShardResponse$Type extends MessageType<ForceReleaseShardResponse> {
    constructor();
    create(value?: PartialMessage<ForceReleaseShardResponse>): ForceReleaseShardResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ForceReleaseShardResponse): ForceReleaseShardResponse;
    internalBinaryWrite(message: ForceReleaseShardResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ForceReleaseShardResponse
 */
export declare const ForceReleaseShardResponse: ForceReleaseShardResponse$Type;
declare class CompactShardRequest$Type extends MessageType<CompactShardRequest> {
    constructor();
    create(value?: PartialMessage<CompactShardRequest>): CompactShardRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: CompactShardRequest): CompactShardRequest;
    internalBinaryWrite(message: CompactShardRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.CompactShardRequest
 */
export declare const CompactShardRequest: CompactShardRequest$Type;
declare class CompactShardResponse$Type extends MessageType<CompactShardResponse> {
    constructor();
    create(value?: PartialMessage<CompactShardResponse>): CompactShardResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: CompactShardResponse): CompactShardResponse;
    internalBinaryWrite(message: CompactShardResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.CompactShardResponse
 */
export declare const CompactShardResponse: CompactShardResponse$Type;
declare class ReadLsmStateRequest$Type extends MessageType<ReadLsmStateRequest> {
    constructor();
    create(value?: PartialMessage<ReadLsmStateRequest>): ReadLsmStateRequest;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ReadLsmStateRequest): ReadLsmStateRequest;
    internalBinaryWrite(message: ReadLsmStateRequest, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ReadLsmStateRequest
 */
export declare const ReadLsmStateRequest: ReadLsmStateRequest$Type;
declare class LsmSstInfo$Type extends MessageType<LsmSstInfo> {
    constructor();
    create(value?: PartialMessage<LsmSstInfo>): LsmSstInfo;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: LsmSstInfo): LsmSstInfo;
    internalBinaryWrite(message: LsmSstInfo, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.LsmSstInfo
 */
export declare const LsmSstInfo: LsmSstInfo$Type;
declare class LsmSortedRunInfo$Type extends MessageType<LsmSortedRunInfo> {
    constructor();
    create(value?: PartialMessage<LsmSortedRunInfo>): LsmSortedRunInfo;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: LsmSortedRunInfo): LsmSortedRunInfo;
    internalBinaryWrite(message: LsmSortedRunInfo, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.LsmSortedRunInfo
 */
export declare const LsmSortedRunInfo: LsmSortedRunInfo$Type;
declare class ReadLsmStateResponse$Type extends MessageType<ReadLsmStateResponse> {
    constructor();
    create(value?: PartialMessage<ReadLsmStateResponse>): ReadLsmStateResponse;
    internalBinaryRead(reader: IBinaryReader, length: number, options: BinaryReadOptions, target?: ReadLsmStateResponse): ReadLsmStateResponse;
    internalBinaryWrite(message: ReadLsmStateResponse, writer: IBinaryWriter, options: BinaryWriteOptions): IBinaryWriter;
}
/**
 * @generated MessageType for protobuf message silo.v1.ReadLsmStateResponse
 */
export declare const ReadLsmStateResponse: ReadLsmStateResponse$Type;
/**
 * @generated ServiceType for protobuf service silo.v1.SiloAdmin
 */
export declare const SiloAdmin: ServiceType;
export {};
//# sourceMappingURL=silo_admin.d.ts.map