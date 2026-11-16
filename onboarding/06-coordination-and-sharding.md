# 06 — Cluster Coordination & Sharding

How tenants map to shards, shards map to nodes, ownership is coordinated, shards split, and requests get routed. This is the control plane.

---

## 1. Sharding model: hash-based range partitioning

Despite the historical "range" naming, silo is **hash-partitioned**: a tenant ID is hashed into a uniform 64-bit space, and shards own lexicographic ranges over the **hex-encoded hash**, not over raw tenant strings.

### Tenant hashing (`src/shard_range.rs:17`)
```rust
pub fn hash_tenant(tenant_id: &str) -> String {
    let hash = xxhash_rust::xxh64::xxh64(tenant_id.as_bytes(), 0);
    format!("{:016x}", hash)   // fixed 16-char lowercase hex
}
```
XXH64 (seed 0), fixed-width hex so **lexicographic == numeric** comparison. The fixed width is the whole point. This same hashing is replicated in siloctl and the TS client so every layer agrees on routing. The motivation (module doc): uniform distribution regardless of tenant-string structure (shared prefixes like `env-...` would otherwise clump).

### The core types
- **`ShardId`** (`shard_range.rs:26`) — a newtype over `Uuid`. Immutable identity; persists across restarts; used for storage paths and coordination keys.
- **`ShardRange`** (`shard_range.rs:89`) — a half-open `[start, end)` interval over hash-space hex strings. Empty `start` = unbounded below; empty `end` = unbounded above.
  - `contains(tenant)` does a *raw lexicographic* compare (for already-hashed values / split points).
  - `contains_tenant(tenant)` *hashes first*, then `contains`. ← the tenant-facing one.
  - `midpoint()` decodes start/end as hex u64 and returns the numeric midpoint (for auto-split).
  - `split(point)` → `(left=[start,point), right=[point,end))`.
- **`ShardInfo`** (`shard_range.rs:242`) — `ShardId` + `ShardRange` + `created_at_ms` + optional `parent_shard_id` (set on split) + optional `placement_ring`.
- **`ShardMap`** (`shard_range.rs:386`) — "the authoritative source for which shards exist and what range each owns." A `Vec<ShardInfo>` **sorted by `range.start`** + a `version: u64` bumped on each mutation.

### Tenant → shard lookup (`shard_for_tenant`, `shard_range.rs:470`)
Hash the tenant, binary-search (`partition_point`) the sorted shards, confirm with `range.contains`. O(log n). `validate()` enforces the invariants that make this sound: first shard unbounded-start, last unbounded-end, adjacent ranges contiguous, no gaps/overlaps/dupes.

> **Important durability rule** (`shard_range.rs:590`): once children are added to the map, they persist *forever* — there's no "delete a shard from the map." This guarantees a committed split can't be undone.

---

## 2. Rendezvous hashing: shards → nodes

The shard map says *what shards exist*; rendezvous hashing decides *which node owns each* — **deterministically, recomputed independently by every node and client.** No central assigner. All in `src/coordination/mod.rs`.

### Score (`rendezvous_score`, `mod.rs:795`)
```rust
fn rendezvous_score(member_id: &str, shard_id: &ShardId) -> u64 {
    let member_hash = fnv1a64(member_id.as_bytes());
    let shard_hash  = fnv1a64(shard_id.as_uuid().as_bytes());
    mix64(member_hash ^ shard_hash)   // highest score wins
}
```

### Bounded-load assignment (`compute_all_shard_assignments`, `mod.rs:816`)
Plain rendezvous imbalances when shards are few relative to nodes, so silo uses **bounded-load**:
1. Group shards by **placement ring** (`None` = default).
2. Per ring, compute `max_per_node = ceil(shards / eligible_nodes)`.
3. Build all `(score, shard, node)` tuples, sort by score desc.
4. Greedily assign each shard to its highest-scoring node still under cap.

Result: rendezvous's stability-on-resize *plus* shard counts differing by ≤1 per ring. `compute_desired_shards_for_node` filters this to one node — the basis of reconciliation.

### Placement rings
Rings pin shards to subsets of nodes. A member qualifies for the default ring if its `placement_rings` is empty or lists `"default"`; for a named ring it must list that ring. `ConfigureShard` moves a shard between rings, triggering handoff.

---

## 3. Coordination backends

### Shared ownership model (`mod.rs:10`)
- A node is a **member** iff it holds a valid **membership lease** (TTL/keepalive) — `[SILO-COORD-INV-10]`.
- **Shard ownership leases are PERMANENT**: they persist until explicitly released (graceful shutdown) or force-released (operator). They are *not* tied to the membership lease and survive crashes — so a crashed node's WAL can be recovered when it restarts (`[SILO-COORD-INV-14]`). This is the single most important coordination invariant.
- A node partitioned from coordination keeps serving shards it already holds, but can't acquire/release while partitioned (`[SILO-COORD-INV-4/6]`).

### `CoordinatorBase` (`mod.rs:357`)
Shared by all backends: `node_id`, `grpc_addr`, `shard_map`, `owned: HashSet<ShardId>`, shutdown channel, the `ShardFactory`, `placement_rings`, `paused_shards`. Key method `compute_reconcile_actions(members, guards)` (`mod.rs:576`) returns `ReconcileActions { to_cancel, to_acquire }` — with a safety guard: **skip reconciling if the member list is empty or doesn't include this node** ("don't reconcile on incomplete data").

### `ShardGuardState` / `ShardPhase` (`mod.rs:158`)
Per-shard state machine: `Idle → Acquiring → Held → Releasing`, plus `ShuttingDown → ShutDown`. `compute_transition` encodes the legal edges. Each shard has a guard with a run-loop driven by a `desired: bool` flag + a `Notify`.

### The `Coordinator` trait (`mod.rs:629`)
`create_coordinator` dispatches on the backend:

- **`none`** (`none.rs`) — single-node dev. Opens *all* shards immediately, marks them owned, `get_members` = just itself, `wait_converged` = always true. Splits unsupported.
- **`etcd`** (`etcd.rs`) — membership = an etcd **lease with keepalive**; shard ownership = a **separate, leaseless KV key** `{prefix}/coord/shards/{id}/owner = node_id`; shard map = one JSON KV. Background task: keepalive + watch members + watch shard-map + reconcile on a 1s timer. On startup, `reclaim_and_open_shards` reopens shards still owned from a previous run (WAL recovery) *before* the first reconcile.
- **`k8s`** (`k8s.rs`) — membership = a per-node `Lease` object (filtered by `renewTime + duration`); shard ownership = a per-shard `Lease` (release clears `holderIdentity` rather than deleting, to avoid races); shard map = a `ConfigMap`. Ownership token = `K8sOwnershipToken { resource_version, lease_uid }` for CAS fencing.

Both etcd and k8s share `CoordinatorBase`, `ShardGuardState`, and the reconcile machinery — **fix a bug in one, check the other** (`.ai/AGENTS.md:136`).

### The per-shard guard run loop (etcd example, `etcd.rs:1073`)
- **Acquiring**: jittered CAS to claim the owner key; on success, **open the shard via the factory BEFORE marking Held** (a failed open releases ownership rather than claiming a broken shard); insert into `owned`; emit `ShardAcquired`.
- **Releasing**: brief cancellable delay, then `factory.close()` **before** `release_ownership`. If close fails → revert to `Held` (prevent data loss).
- **ShuttingDown**: close, and only release the lease if close succeeded. If close fails, **keep the lease** — mimics crash behavior so another node can't grab unflushed data.

---

## 4. Shard splitting (`src/coordination/split.rs`)

A split divides one parent shard into two children at a tenant-hash midpoint, online. Driven by `ShardSplitter`.

### Phases (`SplitPhase`, `split.rs:100`)
`SplitRequested → SplitPausing → SplitCloning → SplitComplete`. Traffic is paused during `SplitPausing`+`SplitCloning`. **The commit point** is the shard-map update — once children exist in the map, the split is committed; crash before that → abandon.

A separate post-split `SplitCleanupStatus` (`CleanupPending → CleanupRunning → CleanupDone → CompactionDone`) tracks each child purging keys now outside its narrowed range.

### Execution (`execute_split_inner`, `split.rs:438`)
Loops on the persisted `SplitInProgress` record, acting on its phase:
1. **SplitRequested** → advance to Pausing, persist, `mark_shard_paused` (hot path now returns `UNAVAILABLE` with no backend call).
2. **SplitPausing** → advance to Cloning.
3. **SplitCloning** (the heavy phase):
   - Fully **close the parent** (stop in-flight work, flush).
   - `factory.clone_closed_shard(parent, left, right)` (file 05 §5).
   - **Pre-add both children to `owned`** before the map update (dodge a race with the shard-map watch).
   - **`update_shard_map_for_split` — THE COMMIT POINT.** On failure, remove pre-added children and return `PreCommitParentClosed`.
   - Post-commit: reload map, recompute ownership, close the parent, drop ownership of the parent and any children this node shouldn't own, open the children it does, set them `CleanupPending`.
   - Advance to SplitComplete.
4. **SplitComplete** → delete the record, unpause.

### Failure recovery
Errors are typed `PreCommit` / `PreCommitParentClosed` / `PostCommit` so recovery differs: pre-commit → abandon + restore parent; post-commit → don't roll back (it's committed). On restart, `recover_stale_splits` checks each record: children in the map ⇒ committed (clean up record), else abandon. `rehydrate_paused_shards` re-marks shards paused so traffic stays rejected until an operator decides — this is called from `main.rs` on boot (file 08 §1).

---

## 5. Request routing & redirects

Requests carry an explicit `shard` UUID (the **tenant→shard mapping is done client-side**, §1) and the server validates it.

### Server side: `shard_with_redirect` (`src/server.rs:446`)
Three distinct signals:
1. **Local + paused for split** → `Status::UNAVAILABLE("...split in progress")`.
2. **Computed owner is me but not open yet** (acquisition in flight) → `Status::UNAVAILABLE("shard not ready: acquisition in progress")` — avoids redirect-to-self loops.
3. **Owned elsewhere** → `Status::NOT_FOUND` + redirect metadata `x-silo-shard-owner-addr` / `x-silo-shard-owner-node` (`src/pb_convert.rs:9`).

Plus `validate_tenant_in_shard_range` returns `FAILED_PRECONDITION("...refresh topology and retry")` when a client targeted the wrong shard due to stale topology.

### `GetClusterInfo` (`fetch_cluster_info`, `src/server.rs:250`)
Builds the topology (per-shard owner + member list). Singleflighted + **serves a cached response on coordinator timeout** so routing clients can still discover topology when etcd/k8s is unhealthy.

### Two client types
- **`ClusterClient`** (`src/cluster_client.rs`) — **node-to-node**. Has the coordinator directly, so never needs redirects: each method does `factory.get(shard)` (local → serve directly, no network) else looks up the owner address and dials it; on error, drops the cached channel.
- **`RoutingClient`** (`src/routing_client.rs`) — **for outsiders** (benches, workers) with no coordinator. Replicates the server's hashing to pick a shard, then handles errors via `classify_error` → `ErrorAction`:
  - `NotFound + owner-addr metadata` → **Redirect** (install a per-shard address override, retry).
  - `NotFound` w/o metadata, or `FailedPrecondition` → **RefreshTopology**.
  - `Unavailable` → **RefreshTopologyWithBackoff**.
  Topology refresh is *coalesced* (one `GetClusterInfo` even under concurrent errors).

Both share `ClientConfig` (timeouts/retries/auth) and the `AuthInterceptor` (injects W3C trace context + optional Bearer token).

---

## 6. ShardFactory interaction (the bridge)

The coordinator signals "shard gained/lost" by calling `factory.open` / `factory.close` from inside the guard loops:
- **Gained**: `factory.open(shard, &range)` *before* marking Held.
- **Lost**: `factory.close(shard)` *before* releasing the lease; if close fails, factory re-inserts so it can retry (and the guard reverts to Held).

Full chain: **membership change → watch fires → `reconcile_shards` → `compute_reconcile_actions` → guard `set_desired` → guard run loop acquires/releases lease + calls factory open/close.**

---

## 7. Coordination/cluster RPCs (`proto/silo.proto`)

| RPC | Purpose |
|-----|---------|
| `GetClusterInfo` | Topology discovery (shard owners + members). Used by `RoutingClient`. |
| `GetNodeInfo` | This node's owned shards + counters + cleanup status. |
| `RequestSplit` / `GetSplitStatus` | Start a split / poll its phase. |
| `ConfigureShard` | Set a shard's placement ring (ring-based rebalancing). |
| `ForceReleaseShard` | Operator escape hatch for a permanently-lost node. |
| `CompactShard` / `FlushShard` / `GetShardStorageInfo` / `ResetShards` | Per-shard storage ops, routed to the owner. |

---

## 8. Quick mental model

1. **Hash** the tenant (XXH64 → 16 hex). 2. **Range-map** the hash to a `ShardId` via the sorted `ShardMap`. 3. **Rendezvous-hash** (bounded-load, ring-aware) that shard to an owning node — computed identically everywhere. 4. **Coordinate** ownership via *permanent* shard leases gated by *transient* membership leases; guards reconcile desired-vs-owned and drive `factory.open/close`. 5. **Route** by serving locally if owned, else redirect / UNAVAILABLE / FAILED_PRECONDITION. 6. **Split** online via pause → clone-at-checkpoint → atomic-map-update, committing at the map write.

Next: [`07-concurrency-and-rate-limits.md`](./07-concurrency-and-rate-limits.md).
