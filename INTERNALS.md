# Silo internals

Silo is built on top of SlateDB, which is a low-level key value store. So, we implement and maintain higher-level data-structurs within this KV store.

# Job

One unit of work that should be attempted until completion or retries exhausted.

# Attempt

One attempt to run a unit of work

# Task

One item that a worker needs to pickup and action to move the system forward. Is usually running an attempt, but there may be other task types in the future.

# Task lease

One task that has been leased by a worker. Upon leasing, a task is moved from the task queue into the task leases. A worker must complete the task or heartbeat for it before the lease expired, or else the system will assume the worker has crashed.

## Key/value layout

- `jobs/<job-id>` - stores job payloads
- `attempts/<job-id>/<attempt-number>` - stores attempt details
- `tasks/<start-time>/<priority>/<job-id>/<attempt-number>` - stores the work items that need accomplishing in task order. Each stored task has a system-generated UUID.
- `lease/<task-id>` - during leasing, dequeued tasks are written here keyed by task id. The value stores a LeaseRecord `{ worker_id, task, expiry_ms }`. Workers must heartbeat/complete before expiry to retain ownership. Expired lease detection scans this prefix and inspects `expiry_ms`.

TODO: indexes and stuff

- `idx/`
