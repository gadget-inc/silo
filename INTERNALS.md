# Silo internals

Silo is built on top of SlateDB, which is a low-level key value store. So, we implement and maintain higher-level data-structurs within this KV store.

## Entities

### Job

One unit of work that should be attempted until completion or retries exhausted.

### Attempt

One attempt to run a unit of work

### Task

One item that we need to dequeue and run. Usually, its a task for a worker to run an attempt, but there are other task types that represent internal operations that should happen after a point in time in the future. Most tasks need to be processed by the external worker that is polling Silo for what to do next, but some tasks are handled purely internally.

### Task lease

One task that has been leased by a worker. Upon leasing, a task is moved from the task queue into the task leases. A worker must complete the task or heartbeat for it before the lease expired, or else the system will assume the worker has crashed.

## Tenancy

All of Silo is built to house job data for multiple tenants, if necessary. Storage is oriented to group all of one tenant's data together for fast scanning. An individual tenant's data _can't_ be split across shards.

## Key/value layout

- `jobs/<tenant-id>/<job-id>` - stores job payloads
- `attempts/<tenant-id>/<job-id>/<attempt-number>` - stores attempt details
- `tasks/<start-time>/<priority>/<job-id>/<attempt-number>` - stores the work items that need accomplishing in task order. Each stored task has a system-generated UUID.
- `lease/<task-id>` - during leasing, dequeued tasks are written here keyed by task id. The value stores a LeaseRecord `{ worker_id, task, expiry_ms }`. Workers must heartbeat/complete before expiry to retain ownership. Expired lease detection scans this prefix and inspects `expiry_ms`.

Secondary indexes are also mapped as ordered keys in the k/v store:

- `idx/status_ts/<tenant>/<status>/<inv_ts:020>/<job-id>` stores a secondary index for all jobs, by status, ordered by the time of the last transition

## Clustering approach

Silo works by separating the compute and storage tiers. Storage is provided by object storage, and the compute is "stateless", in the sense that it can be started and stopped without fear of dataloss.

The clustering approach is modelled after something similar to Google Cloud Bigtable, where individual compute node members all hold leases over some subset of the overall shards. Unlike Bigtable, there's no central coordiantor managing which bits are assigned to which nodes -- we just manage an internal consistent hash ring that assigns the entire shard space to all the nodes, shifting around as few as possible when cluster membership changes.

### Shard coordination plan (using consistent hashing + etcd control plane)

Goals:

- Elastic compute membership with fast join/leave
- Shards are the atomic unit of load; clients specify a shard on each request
- Minimal downtime on ownership changes and no split-brain
- etcd is used only for control plane (membership, locks, handover), never per-request on the data plane

Key ideas:

- **Membership (ephemeral)**: Each node registers under `coord/members/<node_id>` with a membership lease (TTL, e.g. 10s). Value includes `{ addr, started_at, node_id, capabilities }`.
- **Ring computation**: All nodes watch `coord/members/` and deterministically build a consistent hash ring (e.g. Ketama with N virtual nodes per member).
- **Ownership lock (per shard)**: The active owner of shard `<S>` is the node that successfully holds an etcd key `coord/shards/<S>/owner` attached to a liveness lease. Value: `{ owner_id, owner_addr, acquired_at, lease_id }`.
- **Lease model**: Use two independent leases per node: one membership lease for `coord/members/*`, and one liveness lease shared by all `coord/shards/*/owner` keys that node holds. This lets us drop membership first (triggering ring rebalance) before releasing ownership.
- **Data plane independence**: Nodes serve a shard only if they currently hold its `owner` key. Requests never consult etcd; they rely on local in-memory ownership state maintained via watches/keepalives.

Node lifecycle:

1. **Join**

   - Create two leases (TTL ~10s, keepalive ~2s): membership lease and liveness lease.
   - Put `coord/members/<node_id>` with the membership lease.
   - Build ring from current `coord/members/*` and compute desired shard set.
   - Reconcile: for each desired shard, try to acquire `coord/shards/<S>/owner` via etcd transaction with the liveness lease. Only start serving a shard after this key has been written
   - Start watches on `coord/members/*` and `coord/shards/<S>/owner` for shards you own or are next-in-line for.

2. **Steady state**

   - Maintain keepalives for both leases. If keepalives fail (channel closes or prolonged error), immediately pause serving all owned shards to avoid split-brain.
   - On membership change event, recompute ring and run reconciliation: acquire new shards, release shards no longer owned.

3. **Graceful leave**

- Delete `coord/members/<node_id>` (using the membership lease), triggering ring recompute so contenders begin acquisition retries.
- Quiesce briefly per shard (1–2s) to drain in-flight work.
- Revoke the liveness lease, which deletes all `coord/shards/*/owner` keys for this node; new owners acquire on their next retry.

### Safe ring changes

On ring change, we must carefully change ownership of shards with minimal downtime. To do this, we ensure that the new owners start trying to acquire the shard lock before the old owners release it.

The flow:

1. A node either adds or removes its membership details under `coord/members/<node_id>`
2. All nodes discover this membership change, and update their in-memory hash rings, which shifts which shards they are responsible for.
3. New shard owners immediately start trying to acquire the lease for the shards they want to own. Each runs a jittered loop: try `Put` of `coord/shards/<S>/owner` with its liveness lease using "create only if not exists" semantics (i.e., CAS on missing key). If exists, backoff and retry; never overwrite.
4. Old shard owners wait a little bit, ensuring that the new shard owners discover the change and begin their retry loops in advance.
5. Old shard owners start relinquishing their shard locks, and stop serving requests for those shards when that's happened.

Crash case: the previous owner's liveness lease expiry auto-deletes the key; the next retry succeeds and service starts.

### Correctness and split-brain avoidance

Only the node holding `coord/shards/<S>/owner` may serve `<S>`. All others must return NOT_OWNER immediately. Nodes must pause serving a shard if etcd keepalive fails, even if they still have the key locally, because their lease may have expired in etcd and another node could acquire ownership.

Failure modes:

- **Crash**: lease expires; `owner` keys auto-deleted; next owner acquires.
- **Network partition**: owner loses keepalive; it must cease serving; another node acquires after TTL; clients experiencing NOT_OWNER/timeout will retry and be redirected.
- **Simultaneous join**: consistent hashing minimizes movement; CAS on `owner` guarantees single winner.

### Request handling and redirection

On each RPC, the server checks a local map of owned shards. If not the owner, return NOT_OWNER with an optional hint `{ owner_id, owner_addr }` read from the cached `owner` value. Clients retry a few times and may redirect using the hint. Servers never query etcd on the request path.

Timing parameters (tunable):

- Lease TTL: 10s; keepalive: every 2s; optional quiesce deadline: 1–2s per shard.
- Watch backoff with jitter to avoid thundering herds on membership changes.

Key layout (control plane):

- `coord/members/<node_id>` → ephemeral; node metadata
- `coord/shards/<shard_id>/owner` → ephemeral; ownership record

### Gubernator rate limiting

Incoming jobs may specify that they need to pass a Gubernator rate limit to proceed. For each attempt, Silo will make RPCs to Gubernator to check these rate limits before allowing the attempt to run. This is done by enqueuing and running a task that polls the rate limit in Gubernator. If the rate limit passes, we enqueue the actual task to run the attempt, and if it doesn't, we re-enqueue a task for the future to poll the rate limit again.
