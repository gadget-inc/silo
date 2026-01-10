/**
 * Silo Job Queue Shard - Dynamic State Machine Specification
 * 
 * This Alloy model verifies the algorithms that create and assign jobs, tasks, and leases.
 * 
 * Distributed Concurrency Model:
 * - Jobs are sharded by job_id: hash(job_id) % num_shards
 * - Concurrency queues are owned by a specific shard: hash(tenant, queue_key) % num_shards
 * - Cross-shard communication uses task-based RPCs for durability
 * - Holder records live on the queue owner shard
 * - Request records live on the queue owner shard
 *
 * Run verification with:
 * ```shell
 * alloy6 exec -f -s glucose -o specs/output specs/job_shard.als
 * ```
 */
module job_shard

open util/ordering[Time]

sig Time {}
sig Job {}
sig Worker {}
sig TaskId {}
sig Attempt {}
sig Shard {}

/** 
 * Maps each job to its owning shard (determined by hash(job_id) % num_shards).
 * This is immutable once the job is created.
 */
sig JobShard {
    js_job: one Job,
    js_shard: one Shard
}

/**
 * Maps each queue to its owning shard (determined by hash(tenant, queue_key) % num_shards).
 * This is immutable (part of the system configuration).
 */
sig QueueOwner {
    qo_queue: one Queue,
    qo_shard: one Shard
}

/** 
 * Job status 
 * Doesn't include Cancelled, we track cancellation separately for performance reasons in the implementation
 */
abstract sig JobStatus {}
one sig Scheduled, Running, Succeeded, Failed extends JobStatus {}

/** 
 * Cancellation is tracked separately from status.
 * This allows dequeue to blindly write Running without losing cancellation info.
 * Once cancelled, always cancelled (monotonic).
 */
sig JobCancelled {
    cancelled_job: one Job,
    cancelled_time: one Time
}

/** Attempt status */
abstract sig AttemptStatus {}
one sig AttemptRunning, AttemptSucceeded, AttemptFailed extends AttemptStatus {}

/** Job -> Status at each time */
sig JobState {
    job: one Job,
    stat: one JobStatus,
    time: one Time
}

/** Attempt -> Status at each time */
sig AttemptState {
    attempt: one Attempt,
    astat: one AttemptStatus,
    atime: one Time
}

/** Job ownership of attempts (structural, doesn't change, but existence does) */
sig JobAttemptRelation {
    j: one Job,
    a: one Attempt
}

/** 
 * Tasks physically waiting in the DB (durable queue)
 * Maps TaskId -> Job at a specific Time
 */
sig DbQueuedTask {
    db_qtask: one TaskId,
    db_qjob: one Job,
    db_qtime: one Time
}

/**
 * Tasks buffered in the TaskBroker (in-memory)
 * Maps TaskId -> Job at a specific Time
 * Represents the broker's view of available work
 */
sig BufferedTask {
    buf_qtask: one TaskId,
    buf_qjob: one Job,
    buf_qtime: one Time
}

/** Active lease at a given time */
sig Lease {
    ltask: one TaskId,
    lworker: one Worker,
    ljob: one Job,
    lattempt: one Attempt, -- The specific attempt this lease is executing
    ltime: one Time,
    lexpiresAt: one Time   -- Time at which this lease expires if not heartbeated
}

/** Tracks which attempts exist at a given time */
sig AttemptExists {
    attempts: set Attempt,
    time: one Time
}

/** Tracks which jobs exist at a given time */
sig JobExists {
    jobs: set Job,
    time: one Time
}

/** A concurrency queue (named limit key like "api-rate-limit") */
sig Queue {}

/** 
 * Static requirement: which queues a job needs (set at enqueue time, immutable).
 * A job can require 0, 1, or more queues.
 */
sig JobQueueRequirement {
    jqr_job: one Job,
    jqr_queue: one Queue
}

/**
 * A pending request for a concurrency ticket.
 * Stored at requests/<tenant>/<queue>/<start_time>/<priority>/<request_id>
 * Contains the job waiting for a ticket and the task that will run when granted.
 */
sig TicketRequest {
    tr_job: one Job,
    tr_queue: one Queue,
    tr_task: one TaskId,  -- The task that will become RunAttempt when granted
    tr_time: one Time     -- Time when this request exists
}

/**
 * A held ticket (task currently holding a concurrency slot).
 * Stored at holders/<tenant>/<queue>/<task_id>
 * IMPORTANT: In distributed mode, holder records live on the QUEUE OWNER shard,
 * not the job shard. The task_id refers to the task on the job shard.
 */
sig TicketHolder {
    th_task: one TaskId,
    th_queue: one Queue,
    th_time: one Time     -- Time when this holder exists
}

/**
 * A pending task to request a remote ticket from a queue owner shard.
 * Created on the job shard when the queue owner is a different shard.
 * Processing this task sends an RPC to the queue owner.
 */
sig RequestRemoteTicketTask {
    rrt_task: one TaskId,      -- The task ID for this request task
    rrt_job: one Job,          -- The job requesting the ticket
    rrt_queue: one Queue,      -- The queue to request from
    rrt_time: one Time         -- Time when this task exists
}

/**
 * A pending task to notify a job shard that a ticket was granted.
 * Created on the queue owner shard when a ticket becomes available.
 * Processing this task sends an RPC to the job shard.
 */
sig NotifyRemoteTicketGrantTask {
    nrt_task: one TaskId,      -- The task ID for this notification task
    nrt_job: one Job,          -- The job that was granted the ticket
    nrt_queue: one Queue,      -- The queue that granted the ticket
    nrt_request_task: one TaskId, -- The original request task ID (for correlation)
    nrt_time: one Time         -- Time when this task exists
}

/**
 * A pending task to release a remote ticket to the queue owner shard.
 * Created on the job shard when a job completes and needs to release a remote ticket.
 * Processing this task sends an RPC to the queue owner.
 */
sig ReleaseRemoteTicketTask {
    relt_task: one TaskId,     -- The task ID for this release task
    relt_job: one Job,         -- The job releasing the ticket
    relt_queue: one Queue,     -- The queue to release from
    relt_holder_task: one TaskId, -- The task that held the ticket (for correlation)
    relt_time: one Time        -- Time when this task exists
}

fact wellFormed {
    -- Each existing job has exactly one status at each time
    all j: Job, t: Time | j in jobExistsAt[t] implies (one js: JobState | js.job = j and js.time = t)
    -- Non-existing jobs have no status
    all j: Job, t: Time | j not in jobExistsAt[t] implies (no js: JobState | js.job = j and js.time = t)
    
    -- Each existing attempt has exactly one status at each time
    all att: Attempt, t: Time | att in attemptExistsAt[t] implies (one astate: AttemptState | astate.attempt = att and astate.atime = t)
    
    -- Attempts belong to exactly one job (static structural relation)
    all att: Attempt | one r: JobAttemptRelation | r.a = att
    
    -- A task can be in DB queue for at most one job at each time
    all taskid: TaskId, t: Time | lone qt: DbQueuedTask | qt.db_qtask = taskid and qt.db_qtime = t
    
    -- A task can be in Buffer for at most one job at each time
    all taskid: TaskId, t: Time | lone bt: BufferedTask | bt.buf_qtask = taskid and bt.buf_qtime = t
    
    -- A task can be leased to at most one worker at each time
    all taskid: TaskId, t: Time | lone l: Lease | l.ltask = taskid and l.ltime = t
    
    -- Task-job binding is permanent: if a task is ever associated with a job 
    -- (in DB, buffer, lease, or concurrency request), it can only be associated with that same job
    -- (In Rust, the task's key contains job_id which is immutable)
    all taskid: TaskId | lone j: Job | 
        (some t: Time | some dbQueuedAt[taskid, t] and dbQueuedAt[taskid, t] = j) or
        (some t: Time | some bufferedAt[taskid, t] and bufferedAt[taskid, t] = j) or
        (some t: Time | some leaseJobAt[taskid, t] and leaseJobAt[taskid, t] = j) or
        (some r: TicketRequest | r.tr_task = taskid and r.tr_job = j)
    
    -- Existential tracking: one tracker per time
    all t: Time | one e: AttemptExists | e.time = t
    all t: Time | one e: JobExists | e.time = t
    
    -- Once a job exists, it always exists (jobs are not deleted in normal operation)
    all t: Time - last, j: Job | j in jobExistsAt[t] implies j in jobExistsAt[t.next]
    
    -- Leases must have expiry at or after their recorded time
    -- When a lease is first created (dequeue/heartbeat), expiry > ltime
    -- When stutter preserves a lease, the expiry stays the same but ltime advances,
    -- so at the expiry time we have expiry = ltime (which means expired)
    all l: Lease | gte[l.lexpiresAt, l.ltime]
    
    -- Cancellation can be cleared by restart 
    -- Removed: all j: Job, t: Time - last | isCancelledAt[j, t] implies isCancelledAt[j, t.next]
    
    -- At most one cancellation record per job per time
    all j: Job, t: Time | lone c: JobCancelled | c.cancelled_job = j and c.cancelled_time = t
    
    -- Cancellation records can only exist for jobs that exist
    all c: JobCancelled | c.cancelled_job in jobExistsAt[c.cancelled_time]
    
    -- JobQueueRequirements are static (modeled implicitly by not having time)
    -- A job has at most one requirement per queue
    all j: Job, q: Queue | lone r: JobQueueRequirement | r.jqr_job = j and r.jqr_queue = q
    
    -- Each job requires at most one concurrency queue (model simplification - 
    -- multi-queue requirements would need more complex enqueue transitions)
    all j: Job | lone q: Queue | q in jobQueues[j]
    
    -- At most one request per (job, queue) at each time
    all j: Job, q: Queue, t: Time | lone r: TicketRequest | r.tr_job = j and r.tr_queue = q and r.tr_time = t
    
    -- At most one holder per (task, queue) at each time
    all tid: TaskId, q: Queue, t: Time | lone h: TicketHolder | h.th_task = tid and h.th_queue = q and h.th_time = t
    
    -- Queue limit = 1: at most one holder per queue at each time
    all q: Queue, t: Time | lone h: TicketHolder | h.th_queue = q and h.th_time = t
    
    -- Requests are only for existing jobs
    all r: TicketRequest | r.tr_job in jobExistsAt[r.tr_time]
    
    -- Requests are only for queues the job requires
    all r: TicketRequest | r.tr_queue in jobQueues[r.tr_job]
    
    -- A holder can only exist for a task that is either:
    -- 1. In the DB queue (just granted at enqueue), OR
    -- 2. In the buffer (scanned from DB), OR  
    -- 3. Has a lease (running), OR
    -- 4. Has a pending ReleaseRemoteTicketTask (holder not yet released), OR
    -- 5. Has a pending NotifyRemoteTicketGrantTask (remote grant in progress)
    -- This allows holders to persist until the release RPC completes
    all h: TicketHolder | 
        some dbQueuedAt[h.th_task, h.th_time] or 
        some bufferedAt[h.th_task, h.th_time] or 
        some leaseAt[h.th_task, h.th_time] or
        some releaseTaskFor[h.th_task, h.th_queue, h.th_time] or
        some notifyTaskFor[h.th_task, h.th_queue, h.th_time]
    
    -- A holder and request can never coexist for the same (task, queue) at the same time
    -- (This is enforced by transitions but stated explicitly for clarity)
    all t: Time, tid: TaskId, q: Queue | 
        tid in holdersAt[q, t] implies tid not in requestTasksAt[q, t]
    
    -- === DISTRIBUTED CONCURRENCY CONSTRAINTS ===
    
    -- Each job has exactly one shard assignment (static, determined at enqueue)
    all j: Job | one js: JobShard | js.js_job = j
    
    -- Each queue has exactly one owner shard (static configuration)
    all q: Queue | one qo: QueueOwner | qo.qo_queue = q
    
    -- RequestRemoteTicketTask: at most one per (job, queue) at each time
    all j: Job, q: Queue, t: Time | lone rrt: RequestRemoteTicketTask | 
        rrt.rrt_job = j and rrt.rrt_queue = q and rrt.rrt_time = t
    
    -- NotifyRemoteTicketGrantTask: at most one per (job, queue) at each time
    all j: Job, q: Queue, t: Time | lone nrt: NotifyRemoteTicketGrantTask | 
        nrt.nrt_job = j and nrt.nrt_queue = q and nrt.nrt_time = t
    
    -- ReleaseRemoteTicketTask: at most one per (job, queue) at each time
    all j: Job, q: Queue, t: Time | lone relt: ReleaseRemoteTicketTask | 
        relt.relt_job = j and relt.relt_queue = q and relt.relt_time = t
    
    -- Remote ticket tasks are only for jobs that exist
    all rrt: RequestRemoteTicketTask | rrt.rrt_job in jobExistsAt[rrt.rrt_time]
    all nrt: NotifyRemoteTicketGrantTask | nrt.nrt_job in jobExistsAt[nrt.nrt_time]
    all relt: ReleaseRemoteTicketTask | relt.relt_job in jobExistsAt[relt.relt_time]
}

/** Check if a job is cancelled at time t */
pred isCancelledAt[j: Job, t: Time] {
    some c: JobCancelled | c.cancelled_job = j and c.cancelled_time = t
}

/** Get the shard a job lives on */
fun jobShardOf[j: Job]: Shard {
    (js_job.j).js_shard
}

/** Get the shard that owns a queue */
fun queueOwnerOf[q: Queue]: Shard {
    (qo_queue.q).qo_shard
}

/** Check if a queue is local to the job's shard */
pred queueIsLocal[j: Job, q: Queue] {
    jobShardOf[j] = queueOwnerOf[q]
}

/** Check if a queue is remote from the job's shard */
pred queueIsRemote[j: Job, q: Queue] {
    jobShardOf[j] != queueOwnerOf[q]
}

/** Get pending RequestRemoteTicketTask for a job/queue at time t */
fun requestRemoteTaskAt[j: Job, q: Queue, t: Time]: set RequestRemoteTicketTask {
    (rrt_job.j) & (rrt_queue.q) & (rrt_time.t)
}

/** Get pending NotifyRemoteTicketGrantTask for a job/queue at time t */
fun notifyGrantTaskAt[j: Job, q: Queue, t: Time]: set NotifyRemoteTicketGrantTask {
    (nrt_job.j) & (nrt_queue.q) & (nrt_time.t)
}

/** Get pending ReleaseRemoteTicketTask for a task/queue at time t */
fun releaseTaskFor[tid: TaskId, q: Queue, t: Time]: set ReleaseRemoteTicketTask {
    (relt_holder_task.tid) & (relt_queue.q) & (relt_time.t)
}

/** Get pending NotifyRemoteTicketGrantTask for a holder task/queue at time t */
fun notifyTaskFor[tid: TaskId, q: Queue, t: Time]: set NotifyRemoteTicketGrantTask {
    (nrt_request_task.tid) & (nrt_queue.q) & (nrt_time.t)
}

/** Frame condition: remote ticket tasks unchanged */
pred remoteTicketTasksUnchanged[t: Time, tnext: Time] {
    all j: Job, q: Queue | requestRemoteTaskAt[j, q, tnext] = requestRemoteTaskAt[j, q, t]
    all j: Job, q: Queue | notifyGrantTaskAt[j, q, tnext] = notifyGrantTaskAt[j, q, t]
    all tid: TaskId, q: Queue | releaseTaskFor[tid, q, tnext] = releaseTaskFor[tid, q, t]
}

fun statusAt[j: Job, t: Time]: JobStatus {
    ((job.j) & (time.t)).stat
}

fun attemptStatusAt[att: Attempt, t: Time]: AttemptStatus {
    ((attempt.att) & (atime.t)).astat
}

fun attemptExistsAt[t: Time]: set Attempt {
    (time.t).attempts
}

fun jobExistsAt[t: Time]: set Job {
    (time.t).jobs
}

fun attemptJob[att: Attempt]: Job {
    (a.att).j
}

fun dbQueuedAt[taskid: TaskId, t: Time]: set Job {
    ((db_qtask.taskid) & (db_qtime.t)).db_qjob
}

fun bufferedAt[taskid: TaskId, t: Time]: set Job {
    ((buf_qtask.taskid) & (buf_qtime.t)).buf_qjob
}

fun leaseAt[taskid: TaskId, t: Time]: set Worker {
    ((ltask.taskid) & (ltime.t)).lworker
}

fun leaseJobAt[taskid: TaskId, t: Time]: set Job {
    ((ltask.taskid) & (ltime.t)).ljob
}

fun leaseAttemptAt[taskid: TaskId, t: Time]: set Attempt {
    ((ltask.taskid) & (ltime.t)).lattempt
}

/** Full lease equality - all fields match (used for frame conditions) */
pred leaseUnchanged[taskid: TaskId, t: Time, tnext: Time] {
    leaseAt[taskid, tnext] = leaseAt[taskid, t]
    leaseJobAt[taskid, tnext] = leaseJobAt[taskid, t]
    leaseAttemptAt[taskid, tnext] = leaseAttemptAt[taskid, t]
    leaseExpiresAt[taskid, tnext] = leaseExpiresAt[taskid, t]
}

/** Terminal status - Cancelled is now separate and orthogonal */
pred isTerminal[s: JobStatus] {
    s in (Succeeded + Failed)
}

/** Get the queues a job requires (static, set at enqueue) */
fun jobQueues[j: Job]: set Queue {
    (jqr_job.j).jqr_queue
}

/** Check if job requires any concurrency queues */
pred jobRequiresConcurrency[j: Job] {
    some jobQueues[j]
}

/** Get task IDs with pending requests for a queue at time t (for frame comparisons) */
fun requestTasksAt[q: Queue, t: Time]: set TaskId {
    ((tr_queue.q) & (tr_time.t)).tr_task
}

/** Get pending request objects for a queue at time t (for iterating/accessing fields) */
fun requestsAt[q: Queue, t: Time]: set TicketRequest {
    (tr_queue.q) & (tr_time.t)
}

/** Get pending requests for a job at time t */
fun jobRequestsAt[j: Job, t: Time]: set TicketRequest {
    (tr_job.j) & (tr_time.t)
}

/** Get task IDs holding tickets for a queue at time t (for frame comparisons) */
fun holdersAt[q: Queue, t: Time]: set TaskId {
    ((th_queue.q) & (th_time.t)).th_task
}

/** Get queues held by a task at time t */
fun taskHeldQueuesAt[tid: TaskId, t: Time]: set Queue {
    ((th_task.tid) & (th_time.t)).th_queue
}

/** Count holders for a queue at time t (for limit checking) */
fun holderCountAt[q: Queue, t: Time]: Int {
    #holdersAt[q, t]
}

/** Check if queue has capacity (limit=1 means no holders) */
pred queueHasCapacity[q: Queue, t: Time] {
    no holdersAt[q, t]
}

/** Check if a task holds all required queues for its job */
pred taskHoldsAllQueues[tid: TaskId, j: Job, t: Time] {
    all q: jobQueues[j] | some h: TicketHolder | h.th_task = tid and h.th_queue = q and h.th_time = t
}

/** Frame condition: all concurrency state unchanged */
pred concurrencyUnchanged[t: Time, tnext: Time] {
    all q: Queue | requestTasksAt[q, tnext] = requestTasksAt[q, t]
    all q: Queue | holdersAt[q, tnext] = holdersAt[q, t]
}

/** Frame condition: requests unchanged, only specific holder changes */
pred requestsUnchanged[t: Time, tnext: Time] {
    all q: Queue | requestTasksAt[q, tnext] = requestTasksAt[q, t]
}

/** Frame condition: holders unchanged */
pred holdersUnchanged[t: Time, tnext: Time] {
    all q: Queue | holdersAt[q, tnext] = holdersAt[q, t]
}

pred isTerminalAttempt[s: AttemptStatus] {
    s in (AttemptSucceeded + AttemptFailed)
}

/** Get the expiry time for a lease on a task at time t */
fun leaseExpiresAt[taskid: TaskId, t: Time]: set Time {
    ((ltask.taskid) & (ltime.t)).lexpiresAt
}

/** Check if a lease is expired at time t (expiry time <= current time) */
pred leaseExpired[taskid: TaskId, t: Time] {
    some l: Lease | l.ltask = taskid and l.ltime = t and lte[l.lexpiresAt, t]
}

-- Initial State
pred init[t: Time] {
    -- No jobs exist initially (they are created by enqueue)
    no jobExistsAt[t]
    no qt: DbQueuedTask | qt.db_qtime = t
    no bt: BufferedTask | bt.buf_qtime = t
    no l: Lease | l.ltime = t
    no attemptExistsAt[t] -- No attempts exist initially
    -- No concurrency state initially
    no r: TicketRequest | r.tr_time = t
    no h: TicketHolder | h.th_time = t
    -- No remote ticket tasks initially
    no rrt: RequestRemoteTicketTask | rrt.rrt_time = t
    no nrt: NotifyRemoteTicketGrantTask | nrt.nrt_time = t
    no relt: ReleaseRemoteTicketTask | relt.relt_time = t
}

pred enqueuePreConditions[tid: TaskId, j: Job, t: Time] {
    -- [SILO-ENQ-1] Pre: job does NOT exist yet (we're creating a new job)
    j not in jobExistsAt[t]
    
    -- Pre: task is not already in use (not in DB, buffer, or leased)
    no dbQueuedAt[tid, t]
    no bufferedAt[tid, t]
    no leaseAt[tid, t]
}

/** Common postconditions for job creation (all enqueue variants) */
pred enqueueJobCreated[j: Job, t: Time, tnext: Time] {
    -- [SILO-ENQ-2] Post: job now exists with status Scheduled, NOT cancelled
    jobExistsAt[tnext] = jobExistsAt[t] + j
    statusAt[j, tnext] = Scheduled
    not isCancelledAt[j, tnext]
}

/** Common frame conditions for enqueue (things that never change) */
pred enqueueFrameConditions[tid: TaskId, j: Job, t: Time, tnext: Time] {
    -- Buffer unchanged (Broker must scan later)
    all tid2: TaskId | bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
    
    -- Frame: other existing jobs unchanged (status and cancellation)
    all j2: Job | j2 in jobExistsAt[t] implies statusAt[j2, tnext] = statusAt[j2, t]
    all j2: Job | j2 in jobExistsAt[t] implies (isCancelledAt[j2, tnext] iff isCancelledAt[j2, t])
    
    -- Frame: attempts unchanged
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    
    -- Frame: leases unchanged
    all tid2: TaskId | {
        leaseAt[tid2, tnext] = leaseAt[tid2, t]
        leaseJobAt[tid2, tnext] = leaseJobAt[tid2, t]
        leaseAttemptAt[tid2, tnext] = leaseAttemptAt[tid2, t]
    }
    
    -- Frame: remote ticket tasks for OTHER jobs unchanged
    all j2: Job, q: Queue | j2 != j implies {
        requestRemoteTaskAt[j2, q, tnext] = requestRemoteTaskAt[j2, q, t]
        notifyGrantTaskAt[j2, q, tnext] = notifyGrantTaskAt[j2, q, t]
    }
    all tid2: TaskId, q: Queue | tid2 != tid implies {
        releaseTaskFor[tid2, q, tnext] = releaseTaskFor[tid2, q, t]
    }
}

-- Transition: ENQUEUE (no concurrency) - Job has no concurrency requirements
pred enqueue[tid: TaskId, j: Job, t: Time, tnext: Time] {
    enqueuePreConditions[tid, j, t]
    enqueueJobCreated[j, t, tnext]
    enqueueFrameConditions[tid, j, t, tnext]
    
    -- Pre: Job does NOT require any concurrency queues
    no jobQueues[j]
    
    -- Post: Create RunAttempt task in DB queue
    one qt: DbQueuedTask | qt.db_qtask = tid and qt.db_qjob = j and qt.db_qtime = tnext
    all tid2: TaskId | tid2 != tid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- Frame: Concurrency state unchanged
    concurrencyUnchanged[t, tnext]
    
    -- Frame: No remote ticket tasks for this job
    all q: Queue | {
        no requestRemoteTaskAt[j, q, tnext]
        no notifyGrantTaskAt[j, q, tnext]
    }
    all q: Queue | no releaseTaskFor[tid, q, tnext]
}

-- Transition: ENQUEUE_WITH_LOCAL_CONCURRENCY_GRANTED - Job requires LOCAL queue, granted immediately
-- This applies when the queue owner is the same shard as the job shard.
pred enqueueWithConcurrencyGranted[tid: TaskId, j: Job, q: Queue, t: Time, tnext: Time] {
    enqueuePreConditions[tid, j, t]
    enqueueJobCreated[j, t, tnext]
    enqueueFrameConditions[tid, j, t, tnext]
    
    -- Pre: Job requires this queue
    q in jobQueues[j]
    
    -- Pre: Queue is LOCAL to this job's shard (same shard owns the queue)
    queueIsLocal[j, q]
    
    -- [SILO-ENQ-CONC-1] Pre: Queue has capacity (no holders)
    queueHasCapacity[q, t]
    
    -- [SILO-ENQ-CONC-2] Post: Create holder for this task/queue
    holdersAt[q, tnext] = tid
    -- [SILO-ENQ-CONC-3] Post: Create RunAttempt task in DB queue (with held_queues populated)
    one qt: DbQueuedTask | qt.db_qtask = tid and qt.db_qjob = j and qt.db_qtime = tnext
    all tid2: TaskId | tid2 != tid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- Frame: other holders unchanged, requests unchanged
    all q2: Queue | q2 != q implies holdersAt[q2, tnext] = holdersAt[q2, t]
    requestsUnchanged[t, tnext]
    
    -- Frame: No remote ticket tasks for this job/queue (local path)
    all q2: Queue | {
        no requestRemoteTaskAt[j, q2, tnext]
        no notifyGrantTaskAt[j, q2, tnext]
    }
    all q2: Queue | no releaseTaskFor[tid, q2, tnext]
}

-- Transition: ENQUEUE_WITH_LOCAL_CONCURRENCY_QUEUED - Job requires LOCAL queue, must wait
-- This applies when the queue owner is the same shard as the job shard.
pred enqueueWithConcurrencyQueued[tid: TaskId, j: Job, q: Queue, t: Time, tnext: Time] {
    enqueuePreConditions[tid, j, t]
    enqueueJobCreated[j, t, tnext]
    enqueueFrameConditions[tid, j, t, tnext]
    
    -- Pre: Job requires this queue
    q in jobQueues[j]
    
    -- Pre: Queue is LOCAL to this job's shard
    queueIsLocal[j, q]
    
    -- [SILO-ENQ-CONC-4] Pre: Queue is at capacity (has a holder)
    not queueHasCapacity[q, t]
    
    -- [SILO-ENQ-CONC-5] Post: NO task in DB queue (cannot proceed yet)
    no qt: DbQueuedTask | qt.db_qtask = tid and qt.db_qtime = tnext
    -- [SILO-ENQ-CONC-6] Post: Create request record in concurrency requests namespace
    one r: TicketRequest | r.tr_job = j and r.tr_queue = q and r.tr_task = tid and r.tr_time = tnext
    all tid2: TaskId | dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- Frame: other requests unchanged, holders unchanged
    all q2: Queue | q2 != q implies requestTasksAt[q2, tnext] = requestTasksAt[q2, t]
    holdersUnchanged[t, tnext]
    
    -- Frame: No remote ticket tasks for this job/queue (local path)
    all q2: Queue | {
        no requestRemoteTaskAt[j, q2, tnext]
        no notifyGrantTaskAt[j, q2, tnext]
    }
    all q2: Queue | no releaseTaskFor[tid, q2, tnext]
}

-- Transition: ENQUEUE_WITH_REMOTE_CONCURRENCY - Job requires REMOTE queue
-- This applies when the queue owner is a DIFFERENT shard from the job shard.
-- Creates a RequestRemoteTicket task to request the ticket from the queue owner.
pred enqueueWithRemoteConcurrency[tid: TaskId, requestTid: TaskId, j: Job, q: Queue, t: Time, tnext: Time] {
    enqueuePreConditions[tid, j, t]
    enqueueJobCreated[j, t, tnext]
    enqueueFrameConditions[tid, j, t, tnext]
    
    -- Pre: Job requires this queue
    q in jobQueues[j]
    
    -- Pre: Queue is REMOTE from this job's shard
    queueIsRemote[j, q]
    
    -- Pre: requestTid is a fresh task ID for the request task
    requestTid != tid
    no dbQueuedAt[requestTid, t]
    no bufferedAt[requestTid, t]
    no leaseAt[requestTid, t]
    
    -- [SILO-ENQ-REMOTE-1] Post: NO RunAttempt task in DB queue (cannot proceed yet)
    no qt: DbQueuedTask | qt.db_qtask = tid and qt.db_qtime = tnext
    
    -- [SILO-ENQ-REMOTE-2] Post: Create RequestRemoteTicket task in DB queue
    one qt: DbQueuedTask | qt.db_qtask = requestTid and qt.db_qjob = j and qt.db_qtime = tnext
    all tid2: TaskId | tid2 != requestTid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- [SILO-ENQ-REMOTE-3] Post: Create RequestRemoteTicketTask record
    one rrt: RequestRemoteTicketTask | 
        rrt.rrt_task = requestTid and rrt.rrt_job = j and rrt.rrt_queue = q and rrt.rrt_time = tnext
    
    -- Frame: local concurrency state unchanged (request lives on remote shard)
    concurrencyUnchanged[t, tnext]
    
    -- Frame: other remote ticket tasks unchanged
    all q2: Queue | q2 != q implies {
        no requestRemoteTaskAt[j, q2, tnext]
        no notifyGrantTaskAt[j, q2, tnext]
    }
    no notifyGrantTaskAt[j, q, tnext]  -- No grant yet
    all q2: Queue | no releaseTaskFor[tid, q2, tnext]
}

-- Transition: BROKER_SCAN - Read from DB to Buffer
-- See: task_broker.rs::scan_tasks
pred brokerScan[t: Time, tnext: Time] {
    -- [SILO-SCAN-1] Pre: There are tasks in DB that are NOT in buffer
    some tid: TaskId | some dbQueuedAt[tid, t] and no bufferedAt[tid, t]
    
    -- [SILO-SCAN-2] Effect: Copy (some) tasks from DB to Buffer
    -- The key invariant: Only tasks in DB can be added to buffer
    all tid: TaskId | {
        -- If it was already buffered, it stays buffered
        some bufferedAt[tid, t] implies bufferedAt[tid, tnext] = bufferedAt[tid, t]
        -- If it wasn't buffered, it MAY become buffered IF it's in DB
        no bufferedAt[tid, t] implies (
            bufferedAt[tid, tnext] in dbQueuedAt[tid, t]
        )
    }
    -- Ensure at least one task is added (progress)
    some tid: TaskId | no bufferedAt[tid, t] and some bufferedAt[tid, tnext]
    
    -- [SILO-SCAN-3] Skip inflight tasks (modeled implicitly by buffer only accepting non-leased tasks)
    
    -- Frame: DB, Leases, Job Status, Cancellation, Attempts unchanged
    all tid: TaskId | dbQueuedAt[tid, tnext] = dbQueuedAt[tid, t]
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    all j: Job | statusAt[j, tnext] = statusAt[j, t]
    all j: Job | isCancelledAt[j, tnext] iff isCancelledAt[j, t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    jobExistsAt[tnext] = jobExistsAt[t]
    -- Frame: Concurrency state unchanged
    concurrencyUnchanged[t, tnext]
    -- Frame: Remote ticket tasks unchanged
    remoteTicketTasksUnchanged[t, tnext]
}

-- Transition: DEQUEUE - Worker claims task from Buffer (non-cancelled job)
-- For jobs with concurrency requirements, the task must already have holders for all queues.
-- See: job_store_shard.rs::dequeue
pred dequeue[tid: TaskId, w: Worker, a: Attempt, t: Time, tnext: Time] {
    -- [SILO-DEQ-1] Pre: Task is in BUFFER
    some bufferedAt[tid, t]
    let j = bufferedAt[tid, t] | {
        one j
        
        -- [SILO-DEQ-2] Pre: Job must exist
        j in jobExistsAt[t]
        
        -- [SILO-DEQ-CXL] Pre: Job is NOT cancelled (checked on dequeue for lazy cleanup)
        not isCancelledAt[j, t]
        
        -- [SILO-DEQ-CONC] Pre: If job requires concurrency, task must hold all required queues
        -- (holders were created at enqueue time or by grant_next)
        (jobRequiresConcurrency[j]) implies taskHoldsAllQueues[tid, j, t]
        
        -- Pre: Attempt `a` does not exist yet
        a not in attemptExistsAt[t]
        attemptJob[a] = j 
        
        -- [SILO-DEQ-3] Post: Remove task from DB and buffer
        no dbQueuedAt[tid, tnext]
        no bufferedAt[tid, tnext]
        
        -- [SILO-DEQ-4] Post: Create lease with expiry
        one l: Lease | l.ltask = tid and l.lworker = w and l.ljob = j and l.lattempt = a 
            and l.ltime = tnext and gt[l.lexpiresAt, tnext]
        
        -- [SILO-DEQ-5] Post: Create attempt with AttemptRunning status
        attemptExistsAt[tnext] = attemptExistsAt[t] + a
        attemptStatusAt[a, tnext] = AttemptRunning
        
        -- [SILO-DEQ-6] Post: Set job status to Running (pure write)
        statusAt[j, tnext] = Running
        
        -- Frame: other existing jobs unchanged
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        
        -- Frame: cancellation preserved
        all j2: Job | isCancelledAt[j2, tnext] iff isCancelledAt[j2, t]
        
        -- Frame: existing attempts unchanged
        all a2: attemptExistsAt[t] | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    
    -- Frame: jobs existence unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    
    -- Frame: other tasks unchanged
    all tid2: TaskId | tid2 != tid implies {
        dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
        bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
        leaseAt[tid2, tnext] = leaseAt[tid2, t]
        leaseJobAt[tid2, tnext] = leaseJobAt[tid2, t]
        leaseAttemptAt[tid2, tnext] = leaseAttemptAt[tid2, t]
    }
    
    -- Concurrency state: holders preserved (move with task), requests unchanged
    -- Holders persist from t to tnext (they're tied to the lease now)
    concurrencyUnchanged[t, tnext]
    
    -- Frame: Remote ticket tasks unchanged
    remoteTicketTasksUnchanged[t, tnext]
}

-- Transition: DEQUEUE_CLEANUP_CANCELLED - Clean up cancelled job's task during dequeue
-- When dequeue encounters a cancelled job's task, it removes the task without creating a lease.
-- [SILO-DEQ-CXL-REL] If the task holds concurrency tickets, release them and grant to next requester.
-- Note: Concurrency requests for cancelled jobs are NOT cleaned up here - they're cleaned up when release_and_grant_next tries to grant them (skips cancelled, deletes request).
-- See: job_store_shard.rs::dequeue (cancellation check)
pred dequeueCleanupCancelled[tid: TaskId, t: Time, tnext: Time] {
    -- Pre: Task is in BUFFER
    some bufferedAt[tid, t]
    let j = bufferedAt[tid, t] | {
        one j
        
        -- Pre: Job must exist
        j in jobExistsAt[t]
        
        -- Pre: Job IS cancelled (this is the cleanup path)
        isCancelledAt[j, t]
        
        -- Post: Remove task from DB and buffer (cleanup)
        no dbQueuedAt[tid, tnext]
        no bufferedAt[tid, tnext]
        
        -- Post: NO lease created (task is just cleaned up)
        no l: Lease | l.ltask = tid and l.ltime = tnext
        
        -- Post: NO attempt created
        attemptExistsAt[tnext] = attemptExistsAt[t]
        
        -- Frame: Job status unchanged (already Cancelled in Rust terms, or Scheduled in Alloy)
        all j2: Job | j2 in jobExistsAt[t] implies statusAt[j2, tnext] = statusAt[j2, t]
        
        -- Frame: cancellation preserved
        all j2: Job | isCancelledAt[j2, tnext] iff isCancelledAt[j2, t]
        
        -- Frame: existing attempts unchanged
        all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    }
    
    -- Frame: jobs existence unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    
    -- Frame: other tasks unchanged
    all tid2: TaskId | tid2 != tid implies {
        dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
        bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
        leaseAt[tid2, tnext] = leaseAt[tid2, t]
        leaseJobAt[tid2, tnext] = leaseJobAt[tid2, t]
        leaseAttemptAt[tid2, tnext] = leaseAttemptAt[tid2, t]
    }
    
    -- Frame: other leases unchanged
    all tid2: TaskId | tid2 != tid implies leaseUnchanged[tid2, t, tnext]
    
    -- [SILO-DEQ-CXL-REL] If task holds tickets, release them (grant_next is a separate transition)
    -- This is required to maintain wellFormed: holders can only exist for active tasks
    (no taskHeldQueuesAt[tid, t]) implies concurrencyUnchanged[t, tnext]
    (some taskHeldQueuesAt[tid, t]) implies {
        -- Release holders for this task's queues
        all q: taskHeldQueuesAt[tid, t] | holdersAt[q, tnext] = holdersAt[q, t] - tid
        -- Other queues unchanged
        all q: Queue | q not in taskHeldQueuesAt[tid, t] implies holdersAt[q, tnext] = holdersAt[q, t]
        -- Requests unchanged (grant_next is a separate transition)
        requestsUnchanged[t, tnext]
    }
    
    -- Frame: Remote ticket tasks unchanged
    remoteTicketTasksUnchanged[t, tnext]
}


/** Common preconditions for completion: worker holds lease, attempt is running */
pred completePreConditions[tid: TaskId, w: Worker, t: Time] {
    -- [SILO-SUCC-1][SILO-FAIL-1][SILO-RETRY-1] Pre: Lease must exist
    leaseAt[tid, t] = w
    some leaseJobAt[tid, t]
    some leaseAttemptAt[tid, t]
    one leaseJobAt[tid, t]
    one leaseAttemptAt[tid, t]
    attemptStatusAt[leaseAttemptAt[tid, t], t] = AttemptRunning
}

/** Common frame conditions for completion (things unchanged for all variants) */
pred completeFrameConditions[tid: TaskId, t: Time, tnext: Time] {
    -- Frame: job existence, cancellation unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    all j: Job | isCancelledAt[j, tnext] iff isCancelledAt[j, t]
    
    -- Frame: other leases unchanged
    all tid2: TaskId | tid2 != tid implies {
        leaseAt[tid2, tnext] = leaseAt[tid2, t]
        leaseJobAt[tid2, tnext] = leaseJobAt[tid2, t]
        leaseAttemptAt[tid2, tnext] = leaseAttemptAt[tid2, t]
    }
    
    -- Frame: DB queue and buffer unchanged
    all tid2: TaskId | dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    all tid2: TaskId | bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
    
    -- Frame: Remote ticket tasks unchanged
    remoteTicketTasksUnchanged[t, tnext]
}

/** 
 * Release a holder for a specific task/queue.
 */
pred releaseHolder[tid: TaskId, q: Queue, t: Time, tnext: Time] {
    -- [SILO-REL-1] Remove this holder: holders for q at tnext = holders at t minus tid
    holdersAt[q, tnext] = holdersAt[q, t] - tid
    -- Other queues unchanged
    all q2: Queue | q2 != q implies holdersAt[q2, tnext] = holdersAt[q2, t]
    -- Requests unchanged (grant_next is a separate transition)
    requestsUnchanged[t, tnext]
}

-- Transition: COMPLETE_SUCCESS - Worker reports success
pred completeSuccess[tid: TaskId, w: Worker, t: Time, tnext: Time] {
    completePreConditions[tid, w, t]
    completeFrameConditions[tid, t, tnext]
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        -- [SILO-SUCC-2] Post: release lease
        no leaseAt[tid, tnext]
        -- [SILO-SUCC-3] Post: set job status to Succeeded
        statusAt[j, tnext] = Succeeded
        -- [SILO-SUCC-4] Post: set attempt status to AttemptSucceeded
        attemptStatusAt[a, tnext] = AttemptSucceeded
        -- Frame: other jobs unchanged
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        attemptExistsAt[tnext] = attemptExistsAt[t]
        all a2: attemptExistsAt[t] - a | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    
    -- Case 1: No tickets held -> concurrency unchanged
    (no taskHeldQueuesAt[tid, t]) implies concurrencyUnchanged[t, tnext]
    
    -- Case 2: Holding ticket(s) -> release them
    (some taskHeldQueuesAt[tid, t]) implies {
        all q: taskHeldQueuesAt[tid, t] | releaseHolder[tid, q, t, tnext]
    }
}

-- Keep separate for explicit queue parameter in step (simplifies analysis)
pred completeSuccessReleaseTicket[tid: TaskId, w: Worker, q: Queue, t: Time, tnext: Time] {
    completePreConditions[tid, w, t]
    completeFrameConditions[tid, t, tnext]
    
    -- Pre: Task holds a ticket for this queue
    some h: TicketHolder | h.th_task = tid and h.th_queue = q and h.th_time = t
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        no leaseAt[tid, tnext]
        statusAt[j, tnext] = Succeeded
        attemptStatusAt[a, tnext] = AttemptSucceeded
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        attemptExistsAt[tnext] = attemptExistsAt[t]
        all a2: attemptExistsAt[t] - a | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    
    releaseHolder[tid, q, t, tnext]
}

-- Transition: COMPLETE_SUCCESS_WITH_REMOTE_TICKET - Complete and create remote release task
-- For jobs with REMOTE queue tickets, we can't directly release the holder (it lives on queue owner).
-- Instead, we create a ReleaseRemoteTicketTask that will be processed later.
pred completeSuccessCreateRemoteRelease[tid: TaskId, w: Worker, q: Queue, releaseTid: TaskId, t: Time, tnext: Time] {
    completePreConditions[tid, w, t]
    
    -- Pre: Task holds a ticket for this REMOTE queue
    some h: TicketHolder | h.th_task = tid and h.th_queue = q and h.th_time = t
    let j = leaseJobAt[tid, t] | queueIsRemote[j, q]
    
    -- Pre: releaseTid is fresh
    releaseTid != tid
    no dbQueuedAt[releaseTid, t]
    no bufferedAt[releaseTid, t]
    no leaseAt[releaseTid, t]
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        -- Post: release lease
        no leaseAt[tid, tnext]
        -- Post: set job status to Succeeded
        statusAt[j, tnext] = Succeeded
        -- Post: set attempt status to AttemptSucceeded
        attemptStatusAt[a, tnext] = AttemptSucceeded
        -- Frame: other jobs unchanged
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        attemptExistsAt[tnext] = attemptExistsAt[t]
        all a2: attemptExistsAt[t] - a | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
        
        -- Post: Create ReleaseRemoteTicketTask (holder stays until release is processed)
        one qt: DbQueuedTask | qt.db_qtask = releaseTid and qt.db_qjob = j and qt.db_qtime = tnext
        one relt: ReleaseRemoteTicketTask |
            relt.relt_task = releaseTid and relt.relt_job = j and relt.relt_queue = q and
            relt.relt_holder_task = tid and relt.relt_time = tnext
    }
    
    -- Frame: job existence, cancellation unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    all j: Job | isCancelledAt[j, tnext] iff isCancelledAt[j, t]
    
    -- Frame: other leases unchanged
    all tid2: TaskId | tid2 != tid implies {
        leaseAt[tid2, tnext] = leaseAt[tid2, t]
        leaseJobAt[tid2, tnext] = leaseJobAt[tid2, t]
        leaseAttemptAt[tid2, tnext] = leaseAttemptAt[tid2, t]
    }
    
    -- Frame: other DB queue entries unchanged
    all tid2: TaskId | tid2 != releaseTid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- Frame: buffer unchanged
    all tid2: TaskId | bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
    
    -- Concurrency: Holder is NOT released (stays until release task is processed)
    holdersUnchanged[t, tnext]
    requestsUnchanged[t, tnext]
    
    -- Frame: other remote ticket tasks unchanged (only release task for this tid/q created)
    all j2: Job, q2: Queue | requestRemoteTaskAt[j2, q2, tnext] = requestRemoteTaskAt[j2, q2, t]
    all j2: Job, q2: Queue | notifyGrantTaskAt[j2, q2, tnext] = notifyGrantTaskAt[j2, q2, t]
    all tid2: TaskId, q2: Queue | (tid2 != tid or q2 != q) implies 
        releaseTaskFor[tid2, q2, tnext] = releaseTaskFor[tid2, q2, t]
}

-- Transition: GRANT_NEXT_REQUEST - Grant ticket to next waiting request
-- When a queue has capacity (no holders) and there's a pending request, grant it.
-- Skips cancelled job requests (lazy cleanup).
pred grantNextRequest[q: Queue, reqTid: TaskId, t: Time, tnext: Time] {
    -- [SILO-GRANT-1] Pre: Queue has capacity (limit=1, no holders)
    queueHasCapacity[q, t]
    
    -- [SILO-GRANT-2] Pre: There is a pending request for this queue
    some r: TicketRequest | r.tr_queue = q and r.tr_time = t and r.tr_task = reqTid
    let r = { req: TicketRequest | req.tr_queue = q and req.tr_time = t and req.tr_task = reqTid } | {
        one r
        let j = r.tr_job | {
            -- [SILO-GRANT-CXL] Pre: Job is NOT cancelled (skip cancelled requests)
            not isCancelledAt[j, t]
            
            -- [SILO-GRANT-3] Post: Create holder for this task/queue
            holdersAt[q, tnext] = reqTid
            
            -- [SILO-GRANT-4] Post: Create a RunAttempt task in DB queue
            one qt: DbQueuedTask | qt.db_qtask = reqTid and qt.db_qjob = j and qt.db_qtime = tnext
        }
    }
    
    -- Frame: other requests unchanged (for this queue, remove reqTid; other queues unchanged)
    requestTasksAt[q, tnext] = requestTasksAt[q, t] - reqTid
    all q2: Queue | q2 != q implies requestTasksAt[q2, tnext] = requestTasksAt[q2, t]
    
    -- Frame: other holders unchanged (only this queue gets new holder)
    all q2: Queue | q2 != q implies holdersAt[q2, tnext] = holdersAt[q2, t]
    
    -- Frame: job status, existence, cancellation unchanged
    all j: Job | j in jobExistsAt[t] implies statusAt[j, tnext] = statusAt[j, t]
    all j: Job | isCancelledAt[j, tnext] iff isCancelledAt[j, t]
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    
    -- Frame: other DB queue entries unchanged
    all tid2: TaskId | tid2 != reqTid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- Frame: buffer, leases unchanged
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    -- Frame: Remote ticket tasks unchanged
    remoteTicketTasksUnchanged[t, tnext]
}

-- Transition: CLEANUP_CANCELLED_REQUEST - Remove cancelled job's request from queue
-- When trying to grant and finding a cancelled request, delete it without granting.
pred cleanupCancelledRequest[q: Queue, reqTid: TaskId, t: Time, tnext: Time] {
    -- Pre: There is a pending request for this queue
    some r: TicketRequest | r.tr_queue = q and r.tr_time = t and r.tr_task = reqTid
    let r = { req: TicketRequest | req.tr_queue = q and req.tr_time = t and req.tr_task = reqTid } | {
        one r
        let j = r.tr_job | {
            -- [SILO-GRANT-CXL] Pre: Job IS cancelled (this is the cleanup path)
            isCancelledAt[j, t]
            
            -- [SILO-GRANT-CXL-2] Post: Remove the request (no holder created, no task created)
            no req2: TicketRequest | req2.tr_job = j and req2.tr_queue = q and req2.tr_time = tnext
        }
    }
    
    -- Frame: other requests unchanged (for this queue, remove reqTid; other queues unchanged)
    requestTasksAt[q, tnext] = requestTasksAt[q, t] - reqTid
    all q2: Queue | q2 != q implies requestTasksAt[q2, tnext] = requestTasksAt[q2, t]
    
    -- Frame: holders unchanged
    holdersUnchanged[t, tnext]
    
    -- Frame: job status, existence, cancellation unchanged
    all j: Job | j in jobExistsAt[t] implies statusAt[j, tnext] = statusAt[j, t]
    all j: Job | isCancelledAt[j, tnext] iff isCancelledAt[j, t]
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    
    -- Frame: DB queue, buffer, leases unchanged
    all tid: TaskId | dbQueuedAt[tid, tnext] = dbQueuedAt[tid, t]
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    -- Frame: Remote ticket tasks unchanged
    remoteTicketTasksUnchanged[t, tnext]
}

-- Transition: COMPLETE_FAILURE_PERMANENT - Worker reports permanent failure
pred completeFailurePermanent[tid: TaskId, w: Worker, t: Time, tnext: Time] {
    completePreConditions[tid, w, t]
    completeFrameConditions[tid, t, tnext]
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        -- [SILO-FAIL-2] Post: release lease
        no leaseAt[tid, tnext]
        -- [SILO-FAIL-3] Post: set job status to Failed
        statusAt[j, tnext] = Failed
        -- [SILO-FAIL-4] Post: set attempt status to AttemptFailed
        attemptStatusAt[a, tnext] = AttemptFailed
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        attemptExistsAt[tnext] = attemptExistsAt[t]
        all a2: attemptExistsAt[t] - a | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    
    -- Case 1: No tickets held -> concurrency unchanged
    (no taskHeldQueuesAt[tid, t]) implies concurrencyUnchanged[t, tnext]
    
    -- Case 2: Holding ticket(s) -> release them
    (some taskHeldQueuesAt[tid, t]) implies {
        all q: taskHeldQueuesAt[tid, t] | releaseHolder[tid, q, t, tnext]
    }
}

-- Keep separate for explicit queue parameter in step
pred completeFailurePermanentReleaseTicket[tid: TaskId, w: Worker, q: Queue, t: Time, tnext: Time] {
    completePreConditions[tid, w, t]
    completeFrameConditions[tid, t, tnext]
    
    some h: TicketHolder | h.th_task = tid and h.th_queue = q and h.th_time = t
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        no leaseAt[tid, tnext]
        
        -- [SILO-FAIL-3] Post: set job status to Failed (pure write, overwrites ANY previous status)
        statusAt[j, tnext] = Failed
        
        -- [SILO-FAIL-4] Post: set attempt status to AttemptFailed
        attemptStatusAt[a, tnext] = AttemptFailed
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        attemptExistsAt[tnext] = attemptExistsAt[t]
        all a2: attemptExistsAt[t] - a | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    
    releaseHolder[tid, q, t, tnext]
}

-- Transition: COMPLETE_FAILURE_RETRY (no concurrency tickets held)
-- Note: Worker's result takes precedence over any previously reported results, so this always enqueues a retry task regardless of previous status
pred completeFailureRetry[tid: TaskId, w: Worker, newTid: TaskId, t: Time, tnext: Time] {
    -- [SILO-RETRY-1] Pre: worker holds lease
    leaseAt[tid, t] = w
    some leaseJobAt[tid, t]
    some leaseAttemptAt[tid, t]
    
    newTid != tid
    no dbQueuedAt[newTid, t]
    no bufferedAt[newTid, t]
    no leaseAt[newTid, t]
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        one j
        one a
        attemptStatusAt[a, t] = AttemptRunning
        
        -- Pre: Task does NOT hold any concurrency tickets
        no taskHeldQueuesAt[tid, t]
        
        -- [SILO-RETRY-2] Post: release lease
        no leaseAt[tid, tnext]
        
        -- [SILO-RETRY-4] Post: set attempt status to AttemptFailed
        attemptStatusAt[a, tnext] = AttemptFailed
        
        -- [SILO-RETRY-5] Post: enqueue new task to DB queue
        one qt: DbQueuedTask | qt.db_qtask = newTid and qt.db_qjob = j and qt.db_qtime = tnext
        
        -- [SILO-RETRY-3] Post: set job status to Scheduled (pure write, overwrites ANY previous status)
        statusAt[j, tnext] = Scheduled
        
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        attemptExistsAt[tnext] = attemptExistsAt[t]
        all a2: attemptExistsAt[t] - a | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    -- Frame: job existence, cancellation unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    all j: Job | isCancelledAt[j, tnext] iff isCancelledAt[j, t]
    
    all tid2: TaskId | tid2 != tid implies {
        leaseAt[tid2, tnext] = leaseAt[tid2, t]
        leaseJobAt[tid2, tnext] = leaseJobAt[tid2, t]
        leaseAttemptAt[tid2, tnext] = leaseAttemptAt[tid2, t]
    }
    all tid2: TaskId | tid2 != newTid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    all tid2: TaskId | bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
    
    -- Frame: Concurrency state unchanged (no tickets to release)
    concurrencyUnchanged[t, tnext]
    -- Frame: Remote ticket tasks unchanged
    remoteTicketTasksUnchanged[t, tnext]
}

-- Transition: CANCEL - mark job as cancelled
-- Note: Tasks are NOT removed from DB queue here (lazy cleanup).
-- Tasks will be cleaned up when dequeue encounters them.
-- Concurrency requests are also NOT removed - cleaned up when grant_next skips them.
-- Concurrency holders are NOT removed - released when task completes/reaps.
pred cancelJob[j: Job, t: Time, tnext: Time] {
    -- [SILO-CXL-1] Pre: job exists and not already cancelled
    j in jobExistsAt[t]
    not isCancelledAt[j, t]
    
    -- [SILO-CXL-2] Post: Mark job as cancelled (add cancellation record)
    one c: JobCancelled | c.cancelled_job = j and c.cancelled_time = tnext
    
    -- Post: Status stays the same (cancellation is orthogonal to status)
    statusAt[j, tnext] = statusAt[j, t]
    
    -- [SILO-CXL-3] Tasks are NOT removed here - instead, tasks are cleaned up when dequeue checks cancellation
    all tid: TaskId | dbQueuedAt[tid, tnext] = dbQueuedAt[tid, t]
    
    -- Buffer unchanged (stale buffer possible, cleaned on dequeue)
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    
    -- Leases unchanged - worker will discover cancellation on heartbeat
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    
    -- Frame: other jobs, job existence, attempts unchanged
    all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
    -- Frame: cancellation for other jobs unchanged
    all j2: Job | j2 != j implies (isCancelledAt[j2, tnext] iff isCancelledAt[j2, t])
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    
    -- Concurrency state unchanged (lazy cleanup)
    -- Requests cleaned up when grant_next tries to grant (skips cancelled)
    -- Holders released when task completes or is reaped
    concurrencyUnchanged[t, tnext]
    -- Frame: Remote ticket tasks unchanged
    remoteTicketTasksUnchanged[t, tnext]
}

-- Transition: RESTART_CANCELLED_JOB - Re-enable a cancelled job
-- A cancelled job can be restarted if it hasn't completed (not terminal)
-- and has no active tasks (cleaned up by dequeue).
-- This clears the cancellation flag and creates a new task.
pred restartCancelledJob[j: Job, newTid: TaskId, t: Time, tnext: Time] {
    -- [SILO-RESTART-1] Pre: job exists and IS cancelled
    j in jobExistsAt[t]
    isCancelledAt[j, t]
    
    -- [SILO-RESTART-2] Pre: job is NOT terminal (can't restart completed jobs)
    not isTerminal[statusAt[j, t]]
    
    -- [SILO-RESTART-3] Pre: no active tasks for this job (must be cleaned up first)
    -- No task in DB queue, buffer, or leased for this job
    no tid: TaskId | dbQueuedAt[tid, t] = j
    no tid: TaskId | bufferedAt[tid, t] = j
    no tid: TaskId | leaseJobAt[tid, t] = j
    
    -- Pre: new task ID is not in use
    no dbQueuedAt[newTid, t]
    no bufferedAt[newTid, t]
    no leaseAt[newTid, t]
    
    -- [SILO-RESTART-4] Post: Clear cancellation (remove cancellation record)
    not isCancelledAt[j, tnext]
    
    -- [SILO-RESTART-5] Post: Create new task in DB queue
    one qt: DbQueuedTask | qt.db_qtask = newTid and qt.db_qjob = j and qt.db_qtime = tnext
    
    -- [SILO-RESTART-6] Post: Set status to Scheduled (job can run again)
    statusAt[j, tnext] = Scheduled
    
    -- Frame: other DB queue entries unchanged
    all tid2: TaskId | tid2 != newTid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- Frame: buffer unchanged
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    
    -- Frame: leases unchanged
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    
    -- Frame: other jobs unchanged (status and cancellation)
    all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
    all j2: Job | j2 != j implies (isCancelledAt[j2, tnext] iff isCancelledAt[j2, t])
    
    -- Frame: job existence, attempts unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    
    -- Frame: Concurrency state unchanged
    -- Note: If job had pending requests, they were cleaned up by cleanupCancelledRequest
    -- before restart could happen (since job had no active tasks)
    concurrencyUnchanged[t, tnext]
    -- Frame: Remote ticket tasks unchanged
    remoteTicketTasksUnchanged[t, tnext]
}

-- Transition: RESTART_FAILED_JOB - Re-enable a finally-failed job
-- A job that has permanently failed (status = Failed) can be restarted.
-- Mid-retry jobs (Scheduled with pending retry task) cannot use this - they have active tasks.
-- This clears the Failed status and optionally cancellation, creating a new task.
-- Note: Uses same SILO-RESTART-* sigils as restartCancelledJob since Rust handles both in one function.
pred restartFailedJob[j: Job, newTid: TaskId, t: Time, tnext: Time] {
    -- [SILO-RESTART-1] Pre: job exists and is in restartable state (Failed is restartable)
    j in jobExistsAt[t]
    statusAt[j, t] = Failed
    
    -- [SILO-RESTART-3] Pre: no active tasks for this job
    -- This ensures we're not restarting a mid-retry job
    no tid: TaskId | dbQueuedAt[tid, t] = j
    no tid: TaskId | bufferedAt[tid, t] = j
    no tid: TaskId | leaseJobAt[tid, t] = j
    
    -- Pre: new task ID is not in use
    no dbQueuedAt[newTid, t]
    no bufferedAt[newTid, t]
    no leaseAt[newTid, t]
    
    -- [SILO-RESTART-4] Post: Clear cancellation if it was cancelled
    -- (A job can be both Failed and cancelled if worker finished with failure after cancellation)
    not isCancelledAt[j, tnext]
    
    -- [SILO-RESTART-5] Post: Create new task in DB queue
    one qt: DbQueuedTask | qt.db_qtask = newTid and qt.db_qjob = j and qt.db_qtime = tnext
    
    -- [SILO-RESTART-6] Post: Set status to Scheduled (job can run again)
    statusAt[j, tnext] = Scheduled
    
    -- Frame: other DB queue entries unchanged
    all tid2: TaskId | tid2 != newTid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- Frame: buffer unchanged
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    
    -- Frame: leases unchanged
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    
    -- Frame: other jobs unchanged (status and cancellation)
    all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
    all j2: Job | j2 != j implies (isCancelledAt[j2, tnext] iff isCancelledAt[j2, t])
    
    -- Frame: job existence, attempts unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    
    -- Frame: Concurrency state unchanged
    concurrencyUnchanged[t, tnext]
    -- Frame: Remote ticket tasks unchanged
    remoteTicketTasksUnchanged[t, tnext]
}

-- Transition: HEARTBEAT - Worker extends lease expiry
-- Note: Heartbeat ALWAYS renews the lease, even for cancelled jobs.
-- The worker discovers cancellation from the heartbeat RESPONSE, but can keep heartbeating while gracefully winding down. Lease is only released on completion.
pred heartbeat[tid: TaskId, w: Worker, t: Time, tnext: Time] {
    -- [SILO-HB-1] Pre: Worker holds this lease (check worker_id matches)
    leaseAt[tid, t] = w
    
    -- [SILO-HB-2] Pre: Lease exists
    some leaseJobAt[tid, t]
    some leaseAttemptAt[tid, t]
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        one j
        one a
        
        -- [SILO-HB-3] Always renew lease (worker can keep heartbeating during graceful shutdown)
        -- Worker discovers cancellation from heartbeat RESPONSE (isCancelledAt check)
        one l: Lease | l.ltask = tid and l.lworker = w and l.ljob = j and l.lattempt = a 
            and l.ltime = tnext and gt[l.lexpiresAt, tnext]
        
        -- Frame: job and attempt statuses unchanged
        all j2: Job | j2 in jobExistsAt[t] implies statusAt[j2, tnext] = statusAt[j2, t]
        all a2: attemptExistsAt[t] | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    
    -- Frame: everything else unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all j: Job | isCancelledAt[j, tnext] iff isCancelledAt[j, t]
    all tid2: TaskId | dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    all tid2: TaskId | bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
    all tid2: TaskId | tid2 != tid implies {
        leaseAt[tid2, tnext] = leaseAt[tid2, t]
        leaseJobAt[tid2, tnext] = leaseJobAt[tid2, t]
        leaseAttemptAt[tid2, tnext] = leaseAttemptAt[tid2, t]
    }
    -- Frame: Concurrency state unchanged (holders preserved with lease)
    concurrencyUnchanged[t, tnext]
    -- Frame: Remote ticket tasks unchanged
    remoteTicketTasksUnchanged[t, tnext]
}

/** Preconditions for reaping: lease exists and expired */
pred reapPreConditions[tid: TaskId, t: Time] {
    -- [SILO-REAP-1] Pre: Lease exists
    some leaseAt[tid, t]
    -- [SILO-REAP-2] Pre: Lease is expired
    leaseExpired[tid, t]
    one leaseJobAt[tid, t]
    one leaseAttemptAt[tid, t]
    attemptStatusAt[leaseAttemptAt[tid, t], t] = AttemptRunning
}

-- Transition: REAP_EXPIRED_LEASE - System reclaims expired lease
-- Unified: handles both with and without concurrency tickets
pred reapExpiredLease[tid: TaskId, t: Time, tnext: Time] {
    reapPreConditions[tid, t]
    completeFrameConditions[tid, t, tnext]  -- Reuse completion frame conditions
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        -- [SILO-REAP-REL] Post: release lease
        no leaseAt[tid, tnext]
        
        -- [SILO-REAP-3] Post: Set job status to Failed (pure write, overwrites ANY previous status)
        statusAt[j, tnext] = Failed
        
        -- [SILO-REAP-4] Post: Set attempt status to AttemptFailed
        attemptStatusAt[a, tnext] = AttemptFailed
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        attemptExistsAt[tnext] = attemptExistsAt[t]
        all a2: attemptExistsAt[t] - a | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    
    -- Case 1: No tickets held -> concurrency unchanged
    (no taskHeldQueuesAt[tid, t]) implies concurrencyUnchanged[t, tnext]
    
    -- Case 2: Holding ticket(s) -> release them
    (some taskHeldQueuesAt[tid, t]) implies {
        all q: taskHeldQueuesAt[tid, t] | releaseHolder[tid, q, t, tnext]
    }
}

-- Keep separate for explicit queue parameter in step
pred reapExpiredLeaseReleaseTicket[tid: TaskId, q: Queue, t: Time, tnext: Time] {
    reapPreConditions[tid, t]
    completeFrameConditions[tid, t, tnext]
    
    some h: TicketHolder | h.th_task = tid and h.th_queue = q and h.th_time = t
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        no leaseAt[tid, tnext]
        statusAt[j, tnext] = Failed
        attemptStatusAt[a, tnext] = AttemptFailed
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        attemptExistsAt[tnext] = attemptExistsAt[t]
        all a2: attemptExistsAt[t] - a | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    
    releaseHolder[tid, q, t, tnext]
}

-- ========================================================================
-- DISTRIBUTED CONCURRENCY PROTOCOL TRANSITIONS
-- ========================================================================

/**
 * Transition: PROCESS_REMOTE_TICKET_REQUEST
 * Job shard processes a RequestRemoteTicket task by sending RPC to queue owner.
 * On success, the queue owner has stored the request, and we delete the task.
 * 
 * This models the combined effect of:
 * 1. Job shard dequeues RequestRemoteTicket task
 * 2. Job shard sends RPC to queue owner
 * 3. Queue owner stores the request record
 * 4. RPC returns success, job shard deletes the task
 */
pred processRemoteTicketRequest[requestTid: TaskId, runTid: TaskId, j: Job, q: Queue, t: Time, tnext: Time] {
    -- Pre: RequestRemoteTicketTask exists in DB queue
    some rrt: RequestRemoteTicketTask | 
        rrt.rrt_task = requestTid and rrt.rrt_job = j and rrt.rrt_queue = q and rrt.rrt_time = t
    dbQueuedAt[requestTid, t] = j
    
    -- Pre: Job exists and is not cancelled
    j in jobExistsAt[t]
    not isCancelledAt[j, t]
    
    -- Post: Delete the RequestRemoteTicket task from DB queue
    no dbQueuedAt[requestTid, tnext]
    
    -- Post: Delete the RequestRemoteTicketTask record
    no rrt: RequestRemoteTicketTask | 
        rrt.rrt_task = requestTid and rrt.rrt_job = j and rrt.rrt_queue = q and rrt.rrt_time = tnext
    
    -- Post: Create request record on queue owner (models RPC effect)
    one r: TicketRequest | r.tr_job = j and r.tr_queue = q and r.tr_task = runTid and r.tr_time = tnext
    
    -- Frame: other DB queue entries unchanged
    all tid2: TaskId | tid2 != requestTid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- Frame: buffer, leases unchanged
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    
    -- Frame: job status, existence, cancellation unchanged
    all j2: Job | j2 in jobExistsAt[t] implies statusAt[j2, tnext] = statusAt[j2, t]
    all j2: Job | isCancelledAt[j2, tnext] iff isCancelledAt[j2, t]
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    
    -- Frame: holders unchanged, requests at other queues unchanged
    holdersUnchanged[t, tnext]
    all q2: Queue | q2 != q implies requestTasksAt[q2, tnext] = requestTasksAt[q2, t]
    -- Frame: requests from OTHER jobs at queue q are preserved
    all r: TicketRequest | r.tr_queue = q and r.tr_time = t and r.tr_job != j implies
        some r2: TicketRequest | r2.tr_job = r.tr_job and r2.tr_queue = q and r2.tr_task = r.tr_task and r2.tr_time = tnext
    -- Frame: no extra requests appear at queue q (only preserved ones + new one)
    all r: TicketRequest | r.tr_queue = q and r.tr_time = tnext implies
        (r.tr_job = j and r.tr_task = runTid) or 
        (some r2: TicketRequest | r2.tr_queue = q and r2.tr_time = t and r2.tr_job = r.tr_job and r2.tr_task = r.tr_task)
    
    -- Frame: other remote ticket tasks unchanged
    all j2: Job, q2: Queue | (j2 != j or q2 != q) implies 
        requestRemoteTaskAt[j2, q2, tnext] = requestRemoteTaskAt[j2, q2, t]
    all j2: Job, q2: Queue | notifyGrantTaskAt[j2, q2, tnext] = notifyGrantTaskAt[j2, q2, t]
    all tid: TaskId, q2: Queue | releaseTaskFor[tid, q2, tnext] = releaseTaskFor[tid, q2, t]
}

/**
 * Transition: GRANT_REMOTE_TICKET
 * Queue owner grants a ticket to a remote job by creating a NotifyRemoteTicketGrant task.
 * This extends grantNextRequest for remote jobs.
 */
pred grantRemoteTicket[q: Queue, reqTid: TaskId, notifyTid: TaskId, t: Time, tnext: Time] {
    -- Pre: Queue has capacity
    queueHasCapacity[q, t]
    
    -- Pre: There is a pending request for this queue
    some r: TicketRequest | r.tr_queue = q and r.tr_time = t and r.tr_task = reqTid
    let r = { req: TicketRequest | req.tr_queue = q and req.tr_time = t and req.tr_task = reqTid } | {
        one r
        let j = r.tr_job | {
            -- Pre: Job is NOT cancelled
            not isCancelledAt[j, t]
            
            -- Pre: Queue is REMOTE from job's shard
            queueIsRemote[j, q]
            
            -- Pre: notifyTid is a fresh task ID
            no dbQueuedAt[notifyTid, t]
            no bufferedAt[notifyTid, t]
            no leaseAt[notifyTid, t]
            
            -- Post: Create holder for this task/queue (on queue owner)
            holdersAt[q, tnext] = reqTid
            
            -- Post: Remove the request
            requestTasksAt[q, tnext] = requestTasksAt[q, t] - reqTid
            
            -- Post: Create NotifyRemoteTicketGrant task in DB queue (on queue owner)
            one qt: DbQueuedTask | qt.db_qtask = notifyTid and qt.db_qjob = j and qt.db_qtime = tnext
            
            -- Post: Create NotifyRemoteTicketGrantTask record
            one nrt: NotifyRemoteTicketGrantTask |
                nrt.nrt_task = notifyTid and nrt.nrt_job = j and nrt.nrt_queue = q and 
                nrt.nrt_request_task = reqTid and nrt.nrt_time = tnext
        }
    }
    
    -- Frame: other requests unchanged
    all q2: Queue | q2 != q implies requestTasksAt[q2, tnext] = requestTasksAt[q2, t]
    
    -- Frame: other holders unchanged
    all q2: Queue | q2 != q implies holdersAt[q2, tnext] = holdersAt[q2, t]
    
    -- Frame: other DB queue entries unchanged
    all tid2: TaskId | tid2 != notifyTid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- Frame: job status, existence, cancellation unchanged
    all j: Job | j in jobExistsAt[t] implies statusAt[j, tnext] = statusAt[j, t]
    all j: Job | isCancelledAt[j, tnext] iff isCancelledAt[j, t]
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    
    -- Frame: buffer, leases unchanged
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    
    -- Frame: other remote ticket tasks unchanged
    all j2: Job, q2: Queue | requestRemoteTaskAt[j2, q2, tnext] = requestRemoteTaskAt[j2, q2, t]
    -- Update notify task for this job/queue
    all j2: Job, q2: Queue | {
        let targetJob = { req: TicketRequest | req.tr_queue = q and req.tr_time = t and req.tr_task = reqTid }.tr_job |
            (j2 != targetJob or q2 != q) implies notifyGrantTaskAt[j2, q2, tnext] = notifyGrantTaskAt[j2, q2, t]
    }
    all tid: TaskId, q2: Queue | releaseTaskFor[tid, q2, tnext] = releaseTaskFor[tid, q2, t]
}

/**
 * Transition: PROCESS_REMOTE_TICKET_GRANT
 * Queue owner processes a NotifyRemoteTicketGrant task by sending RPC to job shard.
 * On success, the job shard creates a RunAttempt task, and we delete the notification task.
 */
pred processRemoteTicketGrant[notifyTid: TaskId, runTid: TaskId, j: Job, q: Queue, t: Time, tnext: Time] {
    -- Pre: NotifyRemoteTicketGrantTask exists in DB queue
    some nrt: NotifyRemoteTicketGrantTask |
        nrt.nrt_task = notifyTid and nrt.nrt_job = j and nrt.nrt_queue = q and nrt.nrt_time = t
    dbQueuedAt[notifyTid, t] = j
    
    -- Pre: Job exists and is not cancelled
    j in jobExistsAt[t]
    not isCancelledAt[j, t]
    
    -- Pre: runTid is fresh on the job shard
    no dbQueuedAt[runTid, t]
    no bufferedAt[runTid, t]
    no leaseAt[runTid, t]
    
    -- Get the holder task ID from the notification
    let nrt = { n: NotifyRemoteTicketGrantTask | n.nrt_task = notifyTid and n.nrt_job = j and n.nrt_queue = q and n.nrt_time = t },
        holderTid = nrt.nrt_request_task | {
        
        -- Pre: runTid must equal the holder task ID from the notification
        -- This ensures the RunAttempt task created has the same ID as the holder
        runTid = holderTid
        
        -- Post: Delete the NotifyRemoteTicketGrant task from DB queue (on queue owner)
        no dbQueuedAt[notifyTid, tnext]
        
        -- Post: Delete the NotifyRemoteTicketGrantTask record
        no n: NotifyRemoteTicketGrantTask |
            n.nrt_task = notifyTid and n.nrt_job = j and n.nrt_queue = q and n.nrt_time = tnext
        
        -- Post: Create RunAttempt task on job shard (models RPC effect)
        -- Uses holderTid (= runTid) so the holder now has a task in DB queue
        one qt: DbQueuedTask | qt.db_qtask = holderTid and qt.db_qjob = j and qt.db_qtime = tnext
    }
    
    -- Frame: other DB queue entries unchanged
    all tid2: TaskId | tid2 != notifyTid and tid2 != runTid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- Frame: buffer, leases unchanged
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    
    -- Frame: job status, existence, cancellation unchanged
    all j2: Job | j2 in jobExistsAt[t] implies statusAt[j2, tnext] = statusAt[j2, t]
    all j2: Job | isCancelledAt[j2, tnext] iff isCancelledAt[j2, t]
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    
    -- Frame: concurrency state unchanged (holder already exists on queue owner)
    concurrencyUnchanged[t, tnext]
    
    -- Frame: other remote ticket tasks unchanged
    all j2: Job, q2: Queue | requestRemoteTaskAt[j2, q2, tnext] = requestRemoteTaskAt[j2, q2, t]
    all j2: Job, q2: Queue | (j2 != j or q2 != q) implies 
        notifyGrantTaskAt[j2, q2, tnext] = notifyGrantTaskAt[j2, q2, t]
    all tid: TaskId, q2: Queue | releaseTaskFor[tid, q2, tnext] = releaseTaskFor[tid, q2, t]
}

/**
 * Transition: CREATE_REMOTE_TICKET_RELEASE
 * Job shard creates a ReleaseRemoteTicket task when a job completes and needs to release a remote ticket.
 * This is called as part of job completion when the job held remote queue tickets.
 */
pred createRemoteTicketRelease[releaseTid: TaskId, runTid: TaskId, j: Job, q: Queue, t: Time, tnext: Time] {
    -- Pre: Job exists
    j in jobExistsAt[t]
    
    -- Pre: Queue is remote from job's shard
    queueIsRemote[j, q]
    
    -- Pre: There's a holder for this task on the queue owner
    some h: TicketHolder | h.th_task = runTid and h.th_queue = q and h.th_time = t
    
    -- Pre: releaseTid is fresh
    no dbQueuedAt[releaseTid, t]
    no bufferedAt[releaseTid, t]
    no leaseAt[releaseTid, t]
    
    -- Post: Create ReleaseRemoteTicket task in DB queue
    one qt: DbQueuedTask | qt.db_qtask = releaseTid and qt.db_qjob = j and qt.db_qtime = tnext
    
    -- Post: Create ReleaseRemoteTicketTask record
    one relt: ReleaseRemoteTicketTask |
        relt.relt_task = releaseTid and relt.relt_job = j and relt.relt_queue = q and
        relt.relt_holder_task = runTid and relt.relt_time = tnext
    
    -- Frame: other DB queue entries unchanged
    all tid2: TaskId | tid2 != releaseTid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- Frame: buffer, leases unchanged
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    
    -- Frame: job status, existence, cancellation unchanged
    all j2: Job | j2 in jobExistsAt[t] implies statusAt[j2, tnext] = statusAt[j2, t]
    all j2: Job | isCancelledAt[j2, tnext] iff isCancelledAt[j2, t]
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    
    -- Frame: concurrency state unchanged (holder still exists until release processed)
    concurrencyUnchanged[t, tnext]
    
    -- Frame: other remote ticket tasks unchanged
    all j2: Job, q2: Queue | requestRemoteTaskAt[j2, q2, tnext] = requestRemoteTaskAt[j2, q2, t]
    all j2: Job, q2: Queue | notifyGrantTaskAt[j2, q2, tnext] = notifyGrantTaskAt[j2, q2, t]
    all tid2: TaskId, q2: Queue | (tid2 != runTid or q2 != q) implies 
        releaseTaskFor[tid2, q2, tnext] = releaseTaskFor[tid2, q2, t]
}

/**
 * Transition: PROCESS_REMOTE_TICKET_RELEASE
 * Job shard processes a ReleaseRemoteTicket task by sending RPC to queue owner.
 * On success, the queue owner releases the holder, and we delete the release task.
 */
pred processRemoteTicketRelease[releaseTid: TaskId, holderTid: TaskId, j: Job, q: Queue, t: Time, tnext: Time] {
    -- Pre: ReleaseRemoteTicketTask exists in DB queue
    some relt: ReleaseRemoteTicketTask |
        relt.relt_task = releaseTid and relt.relt_job = j and relt.relt_queue = q and 
        relt.relt_holder_task = holderTid and relt.relt_time = t
    dbQueuedAt[releaseTid, t] = j
    
    -- Pre: Holder exists on queue owner
    some h: TicketHolder | h.th_task = holderTid and h.th_queue = q and h.th_time = t
    
    -- Post: Delete the ReleaseRemoteTicket task from DB queue
    no dbQueuedAt[releaseTid, tnext]
    
    -- Post: Delete the ReleaseRemoteTicketTask record
    no relt: ReleaseRemoteTicketTask |
        relt.relt_task = releaseTid and relt.relt_job = j and relt.relt_queue = q and relt.relt_time = tnext
    
    -- Post: Release the holder on queue owner (models RPC effect)
    holdersAt[q, tnext] = holdersAt[q, t] - holderTid
    
    -- Frame: other holders unchanged
    all q2: Queue | q2 != q implies holdersAt[q2, tnext] = holdersAt[q2, t]
    
    -- Frame: requests unchanged
    requestsUnchanged[t, tnext]
    
    -- Frame: other DB queue entries unchanged
    all tid2: TaskId | tid2 != releaseTid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- Frame: buffer, leases unchanged
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    
    -- Frame: job status, existence, cancellation unchanged
    all j2: Job | j2 in jobExistsAt[t] implies statusAt[j2, tnext] = statusAt[j2, t]
    all j2: Job | isCancelledAt[j2, tnext] iff isCancelledAt[j2, t]
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    
    -- Frame: other remote ticket tasks unchanged
    all j2: Job, q2: Queue | requestRemoteTaskAt[j2, q2, tnext] = requestRemoteTaskAt[j2, q2, t]
    all j2: Job, q2: Queue | notifyGrantTaskAt[j2, q2, tnext] = notifyGrantTaskAt[j2, q2, t]
    all tid2: TaskId, q2: Queue | (tid2 != holderTid or q2 != q) implies 
        releaseTaskFor[tid2, q2, tnext] = releaseTaskFor[tid2, q2, t]
}

-- ========================================================================
-- END DISTRIBUTED CONCURRENCY PROTOCOL TRANSITIONS
-- ========================================================================

-- Transition: STUTTER
-- Necessary for alloy to make arbitrary time steps forward without changing any state
pred stutter[t: Time, tnext: Time] {
    -- All existing jobs unchanged (status and cancellation)
    all j: Job | j in jobExistsAt[t] implies statusAt[j, tnext] = statusAt[j, t]
    all j: Job | isCancelledAt[j, tnext] iff isCancelledAt[j, t]
    all tid: TaskId | dbQueuedAt[tid, tnext] = dbQueuedAt[tid, t]
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    -- Frame: Concurrency state unchanged
    concurrencyUnchanged[t, tnext]
    -- Frame: Remote ticket tasks unchanged
    remoteTicketTasksUnchanged[t, tnext]
}

-- System Trace
pred step[t: Time, tnext: Time] {
    -- Job lifecycle (no concurrency)
    (some tid: TaskId, j: Job | enqueue[tid, j, t, tnext])
    or (brokerScan[t, tnext])
    or (some tid: TaskId, w: Worker, a: Attempt | dequeue[tid, w, a, t, tnext])
    or (some tid: TaskId | dequeueCleanupCancelled[tid, t, tnext])
    or (some tid: TaskId, w: Worker | heartbeat[tid, w, t, tnext])
    or (some tid: TaskId | reapExpiredLease[tid, t, tnext])
    or (some tid: TaskId, w: Worker | completeSuccess[tid, w, t, tnext])
    or (some tid: TaskId, w: Worker | completeFailurePermanent[tid, w, t, tnext])
    or (some tid: TaskId, w: Worker, newTid: TaskId | completeFailureRetry[tid, w, newTid, t, tnext])
    or (some j: Job | cancelJob[j, t, tnext])
    or (some j: Job, newTid: TaskId | restartCancelledJob[j, newTid, t, tnext])
    or (some j: Job, newTid: TaskId | restartFailedJob[j, newTid, t, tnext])
    -- Local concurrency ticket management (queue owner == job shard)
    or (some tid: TaskId, j: Job, q: Queue | enqueueWithConcurrencyGranted[tid, j, q, t, tnext])
    or (some tid: TaskId, j: Job, q: Queue | enqueueWithConcurrencyQueued[tid, j, q, t, tnext])
    or (some q: Queue, tid: TaskId | grantNextRequest[q, tid, t, tnext])
    or (some q: Queue, tid: TaskId | cleanupCancelledRequest[q, tid, t, tnext])
    or (some tid: TaskId, w: Worker, q: Queue | completeSuccessReleaseTicket[tid, w, q, t, tnext])
    or (some tid: TaskId, w: Worker, q: Queue, releaseTid: TaskId | completeSuccessCreateRemoteRelease[tid, w, q, releaseTid, t, tnext])
    or (some tid: TaskId, w: Worker, q: Queue | completeFailurePermanentReleaseTicket[tid, w, q, t, tnext])
    or (some tid: TaskId, q: Queue | reapExpiredLeaseReleaseTicket[tid, q, t, tnext])
    -- Distributed concurrency protocol (queue owner != job shard)
    or (some tid: TaskId, reqTid: TaskId, j: Job, q: Queue | enqueueWithRemoteConcurrency[tid, reqTid, j, q, t, tnext])
    or (some reqTid: TaskId, runTid: TaskId, j: Job, q: Queue | processRemoteTicketRequest[reqTid, runTid, j, q, t, tnext])
    or (some q: Queue, reqTid: TaskId, notifyTid: TaskId | grantRemoteTicket[q, reqTid, notifyTid, t, tnext])
    or (some notifyTid: TaskId, runTid: TaskId, j: Job, q: Queue | processRemoteTicketGrant[notifyTid, runTid, j, q, t, tnext])
    or (some releaseTid: TaskId, runTid: TaskId, j: Job, q: Queue | createRemoteTicketRelease[releaseTid, runTid, j, q, t, tnext])
    or (some releaseTid: TaskId, holderTid: TaskId, j: Job, q: Queue | processRemoteTicketRelease[releaseTid, holderTid, j, q, t, tnext])
    -- Stutter
    or stutter[t, tnext]
}

fact traces {
    init[first]
    all t: Time - last | step[t, t.next]
}


/** 
 * A task is never leased to two workers at once.
 * Enforced by: dequeue creates lease atomically with task removal from buffer.
 */
assert noDoubleLease {
    all t: Time, tid: TaskId | lone leaseAt[tid, t]
}

/** 
 * A job never has two active leases.
 * Enforced by: only one attempt runs at a time, lease is per-attempt.
 */
assert oneLeasePerJob {
    all t: Time, j: Job | lone tid: TaskId | leaseJobAt[tid, t] = j
}

/** 
 * Leases can only exist for Running jobs (status-wise).
 * The job may also be cancelled (orthogonal flag), but status must be Running.
 * Succeeded/Failed jobs cannot have leases because they're set when lease is released.
 */
assert leaseJobMustBeRunning {
    all t: Time, tid: TaskId | some leaseAt[tid, t] implies 
        statusAt[leaseJobAt[tid, t], t] = Running
}

/** A running job has exactly one running attempt */
assert runningJobHasRunningAttempt {
    all t: Time, j: Job | (j in jobExistsAt[t] and statusAt[j, t] = Running) implies 
        (one a: attemptExistsAt[t] | attemptJob[a] = j and attemptStatusAt[a, t] = AttemptRunning)
}

/** A completed attempt is never running again */
assert attemptTerminalIsForever {
    all t: Time - last, a: attemptExistsAt[t] | 
        isTerminalAttempt[attemptStatusAt[a, t]] implies 
        attemptStatusAt[a, t.next] = attemptStatusAt[a, t]
}

/**
 * Valid job status transitions.
 * Note: Cancellation is a separate flag, not a status in alloy, so that we can blindly write status without having to check cancellation always.
 * Status is just: Scheduled, Running, Succeeded, Failed
 */
assert validTransitions {
    all t: Time - last, j: Job | j in jobExistsAt[t] implies {
        statusAt[j, t] = Scheduled implies statusAt[j, t.next] in (Scheduled + Running)
        statusAt[j, t] = Running implies statusAt[j, t.next] in (Running + Succeeded + Failed + Scheduled)
        -- Succeeded is truly terminal (cannot be restarted)
        statusAt[j, t] = Succeeded implies statusAt[j, t.next] = Succeeded
        -- Failed can transition to Scheduled via restart
        statusAt[j, t] = Failed implies statusAt[j, t.next] in (Failed + Scheduled)
    }
}

/** 
 * No "Zombie" Attempts for terminal jobs (Succeeded/Failed).
 * If job is Succeeded/Failed, its attempts can't be AttemptRunning.
 */
assert noZombieAttempts {
    all t: Time, att: attemptExistsAt[t] | 
        let j = attemptJob[att] | 
        (isTerminal[statusAt[j, t]] implies attemptStatusAt[att, t] != AttemptRunning)
}

/** Queue Consistency: Terminal jobs have no DB queued tasks */
assert noQueuedTasksForTerminal {
    -- Terminal jobs have no RunAttempt tasks in DB queue.
    -- Exception: ReleaseRemoteTicketTask is allowed for terminal jobs (cleanup task).
    all t: Time, j: Job | (j in jobExistsAt[t] and isTerminal[statusAt[j, t]]) implies 
        all qt: DbQueuedTask | qt.db_qjob = j and qt.db_qtime = t implies
            some relt: ReleaseRemoteTicketTask | relt.relt_task = qt.db_qtask and relt.relt_time = t
}

/**
 * Succeeded/Failed jobs never have leases.
 * These states are only set when the lease is released (completion/reap).
 */
assert noLeasesForTerminal {
    all t: Time, j: Job | (j in jobExistsAt[t] and isTerminal[statusAt[j, t]]) implies 
        no l: Lease | l.ljob = j and l.ltime = t
}

/**
 * Cancellation can only be cleared for restartable jobs.
 * If a job is cancelled at t but not at t.next, the job must have been restartable:
 * - Non-terminal (Scheduled/Running) via restartCancelledJob, OR
 * - Failed via restartFailedJob
 * Succeeded jobs cannot be restarted or have their cancellation cleared.
 */
assert cancellationClearedRequiresRestartable {
    all j: Job, t: Time - last | 
        (isCancelledAt[j, t] and not isCancelledAt[j, t.next]) implies 
            statusAt[j, t] != Succeeded
}

/**
 * When cancellation is cleared, the job becomes Scheduled with a new task.
 * This verifies the restart postconditions.
 */
assert restartedJobIsScheduledWithTask {
    all j: Job, t: Time - last | 
        (isCancelledAt[j, t] and not isCancelledAt[j, t.next]) implies {
            statusAt[j, t.next] = Scheduled
            some tid: TaskId | dbQueuedAt[tid, t.next] = j
        }
}

/**
 * Queue limit is enforced: at most one holder per queue at any time.
 * This is key for correctness - we use limit=1 (mutex) semantics.
 * Enforced by: only granting when queueHasCapacity[q, t] is true.
 */
assert queueLimitEnforced {
    all q: Queue, t: Time | lone h: TicketHolder | h.th_queue = q and h.th_time = t
}

/**
 * Holders only exist for tasks that are active (in DB queue, buffer, or leased).
 * A ticket holder is created at enqueue (granted) or grant_next, and released when:
 * - Task completes successfully or fails: [SILO-REL-1]
 * - Lease expires: [SILO-REAP-*] releases via completion path
 * - Cancelled task cleaned up at dequeue: [SILO-DEQ-CXL-REL]
 * See: [SILO-ENQ-CONC-2], [SILO-GRANT-3], [SILO-REL-1], [SILO-DEQ-CXL-REL]
 */
assert holdersRequireActiveTask {
    all h: TicketHolder | 
        some dbQueuedAt[h.th_task, h.th_time] or 
        some bufferedAt[h.th_task, h.th_time] or 
        some leaseAt[h.th_task, h.th_time] or
        some releaseTaskFor[h.th_task, h.th_queue, h.th_time] or
        some notifyTaskFor[h.th_task, h.th_queue, h.th_time]
}

/**
 * Terminal jobs have no holders.
 * Tickets are released when the job completes (success/failure).
 */
assert noHoldersForTerminal {
    all t: Time, j: Job, h: TicketHolder | 
        (j in jobExistsAt[t] and isTerminal[statusAt[j, t]] and h.th_time = t) implies
        leaseJobAt[h.th_task, t] != j
}

/**
 * Cancelled job's requests are eventually cleaned up.
 * Note: This is a liveness property - we can only check safety (requests exist only for non-cancelled OR are in process of cleanup)
 * In practice, cleanup happens on grant_next, so cancelled requests may exist temporarily.
 */
-- Skipped: liveness property, hard to check in Alloy without temporal logic

/**
 * Granted requests don't exist anymore.
 * Once a holder exists for a (task, queue), there's no request for that task.
 */
assert grantedMeansNoRequest {
    all t: Time, h: TicketHolder | h.th_time = t implies
        no r: TicketRequest | r.tr_task = h.th_task and r.tr_queue = h.th_queue and r.tr_time = t
}

-- ========================================================================
-- DISTRIBUTED CONCURRENCY PROTOCOL ASSERTIONS
-- ========================================================================

/**
 * Global concurrency limit is enforced across ALL shards.
 * Even with jobs on different shards, at most one holder per queue at any time.
 * This is the KEY invariant for the distributed protocol.
 */
assert globalConcurrencyLimitEnforced {
    all q: Queue, t: Time | lone h: TicketHolder | h.th_queue = q and h.th_time = t
}

/**
 * Remote ticket request tasks are only for remote queues.
 * A RequestRemoteTicketTask should only exist when the queue owner is different from job shard.
 */
assert remoteRequestTasksOnlyForRemoteQueues {
    all rrt: RequestRemoteTicketTask | 
        queueIsRemote[rrt.rrt_job, rrt.rrt_queue]
}

/**
 * Remote grant notification tasks are only for remote queues.
 */
assert remoteGrantTasksOnlyForRemoteQueues {
    all nrt: NotifyRemoteTicketGrantTask |
        queueIsRemote[nrt.nrt_job, nrt.nrt_queue]
}

/**
 * Remote release tasks are only for remote queues.
 */
assert remoteReleaseTasksOnlyForRemoteQueues {
    all relt: ReleaseRemoteTicketTask |
        queueIsRemote[relt.relt_job, relt.relt_queue]
}

/**
 * A job can have at most one active ticket lifecycle state per queue.
 * Either: requesting, holding, or releasing - but not multiple.
 */
assert oneTicketStatePerJobQueue {
    all j: Job, q: Queue, t: Time | {
        -- Can't have both a request and a holder for the same job/queue
        (some r: TicketRequest | r.tr_job = j and r.tr_queue = q and r.tr_time = t) implies
            no h: TicketHolder | (some l: Lease | l.ljob = j and l.ltask = h.th_task and l.ltime = t) and h.th_queue = q and h.th_time = t
    }
}


-- Examples
pred exampleSuccess {
    some j: Job | j in jobExistsAt[last] and statusAt[j, last] = Succeeded
}

pred exampleRetry {
    some j: Job | j in jobExistsAt[last] and statusAt[j, last] = Succeeded and #{a: attemptExistsAt[last] | attemptJob[a] = j} > 1
}

pred examplePermanentFailureWithRetry {
    some j: Job | j in jobExistsAt[last] and statusAt[j, last] = Failed and #{a: attemptExistsAt[last] | attemptJob[a] = j} > 1
}

/** 
 * Scenario: Stale Buffer
 * Job is cancelled while task is in buffer.
 */
pred exampleStaleBuffer {
    some tid: TaskId, j: Job, t: Time | {
        bufferedAt[tid, t] = j
        isCancelledAt[j, t]
    }
}

/**
 * Scenario: Cancellation with lazy cleanup on dequeue
 * Job is cancelled, task stays in DB/buffer, cleaned up when dequeue encounters it.
 * This is the lazy cleanup path - task is removed without creating a lease.
 */
pred exampleCancellationLazyCleanup {
    some tid: TaskId, j: Job, t1, t2, t3, t4: Time | {
        lt[t1, t2] and lt[t2, t3] and lt[t3, t4]
        -- t1: job scheduled, task in DB queue, NOT cancelled
        j in jobExistsAt[t1]
        statusAt[j, t1] = Scheduled
        not isCancelledAt[j, t1]
        some dbQueuedAt[tid, t1]
        dbQueuedAt[tid, t1] = j
        -- t2: job cancelled, task STILL in DB queue (lazy cleanup)
        isCancelledAt[j, t2]
        some dbQueuedAt[tid, t2]  -- Still in DB queue (not immediately removed)
        -- t3: task enters buffer via broker scan
        isCancelledAt[j, t3]
        some bufferedAt[tid, t3]  -- Now in buffer (stale)
        -- t4: dequeueCleanupCancelled removes task without creating lease
        isCancelledAt[j, t4]
        no dbQueuedAt[tid, t4]    -- Cleaned up
        no bufferedAt[tid, t4]    -- Cleaned up
        no l: Lease | l.ljob = j and l.ltime = t4  -- No lease created
        statusAt[j, t4] = Scheduled  -- Status never changed to Running
    }
}

/**
 * Scenario: Dequeue skips cancelled task and cleans it up
 * When dequeue encounters a cancelled job's task, it removes the task
 * without creating a lease. Job never enters Running state.
 */
pred exampleDequeueSkipsCancelledTask {
    some tid: TaskId, j: Job, t: Time - last | {
        -- At time t: job is cancelled, task is in buffer
        bufferedAt[tid, t] = j
        isCancelledAt[j, t]
        -- At time t.next: task was cleaned up, NO lease created
        no bufferedAt[tid, t.next]
        no dbQueuedAt[tid, t.next]
        no l: Lease | l.ltask = tid and l.ltime = t.next  -- No lease!
        statusAt[j, t.next] = Scheduled  -- Job status unchanged (still Scheduled in Alloy)
    }
}

/**
 * Scenario: Worker discovers cancellation on heartbeat but continues gracefully
 * Job is dequeued (creating lease), cancelled while worker runs,
 * worker heartbeats (discovering cancellation but keeping lease),
 * worker eventually completes (releasing lease).
 */
pred exampleCancellationDiscoveryOnHeartbeat {
    some tid: TaskId, j: Job, t1, t2, t3: Time | {
        lt[t1, t2] and lt[t2, t3]
        -- t1: job running with lease, not yet cancelled
        some leaseAt[tid, t1]
        leaseJobAt[tid, t1] = j
        statusAt[j, t1] = Running
        not isCancelledAt[j, t1]
        -- t2: job cancelled but lease still exists (worker discovers on heartbeat)
        some leaseAt[tid, t2]
        statusAt[j, t2] = Running  -- Status is still Running
        isCancelledAt[j, t2]       -- But cancellation flag is set
        -- t3: worker completes successfully (releases lease, status becomes Succeeded)
        no leaseAt[tid, t3]
        statusAt[j, t3] = Succeeded  -- Worker success takes precedence
        isCancelledAt[j, t3]         -- But cancellation flag is preserved!
    }
}

/**
 * Scenario: Worker heartbeats to keep lease alive
 * Job is dequeued, heartbeated, then completes successfully
 */
pred exampleHeartbeat {
    some tid: TaskId, j: Job, t1, t2, t3: Time | {
        -- t1 < t2 < t3
        lt[t1, t2] and lt[t2, t3]
        -- Lease exists at t1, t2, t3
        some leaseAt[tid, t1]
        some leaseAt[tid, t2]
        -- Job succeeds at t3
        j in jobExistsAt[t3]
        statusAt[j, t3] = Succeeded
    }
}

/**
 * Scenario: Lease expires and is reaped (worker crash)
 * Job is dequeued but worker crashes, lease expires, system reaps it
 */
pred exampleLeaseExpiry {
    some tid: TaskId, j: Job, t1, t2: Time | {
        lt[t1, t2]
        -- Lease exists and expires at t1
        some leaseAt[tid, t1]
        leaseExpired[tid, t1]
        -- Lease gone at t2 and job failed
        no leaseAt[tid, t2]
        j in jobExistsAt[t2]
        statusAt[j, t2] = Failed
    }
}

/**
 * Scenario: Job with concurrency requirement is granted immediately
 * Queue has capacity, ticket is granted at enqueue time.
 */
pred exampleConcurrencyGrantedImmediately {
    some tid: TaskId, j: Job, q: Queue, t1, t2: Time | {
        lt[t1, t2]
        -- t1: job enqueued with concurrency requirement, granted immediately
        j in jobExistsAt[t1]
        q in jobQueues[j]
        some h: TicketHolder | h.th_task = tid and h.th_queue = q and h.th_time = t1
        some dbQueuedAt[tid, t1]
        -- t2: job completes, holder released
        statusAt[j, t2] = Succeeded
        no h: TicketHolder | h.th_queue = q and h.th_time = t2
    }
}

/**
 * Scenario: Job with concurrency requirement waits in queue
 * Queue is at capacity, job waits as a request until holder releases.
 */
pred exampleConcurrencyWaitsInQueue {
    some tid1, tid2: TaskId, j1, j2: Job, q: Queue, t1, t2, t3: Time | {
        lt[t1, t2] and lt[t2, t3]
        tid1 != tid2 and j1 != j2
        -- t1: j1 holds the queue, j2 is waiting as a request
        some h: TicketHolder | h.th_task = tid1 and h.th_queue = q and h.th_time = t1
        some r: TicketRequest | r.tr_job = j2 and r.tr_queue = q and r.tr_time = t1
        -- t2: j1 completes, releases holder
        statusAt[j1, t2] = Succeeded
        no h: TicketHolder | h.th_task = tid1 and h.th_time = t2
        -- t3: j2 is granted (holder created)
        some h: TicketHolder | h.th_task = tid2 and h.th_queue = q and h.th_time = t3
    }
}

/**
 * Scenario: Cancelled job's request is cleaned up during grant_next
 * Job is cancelled while waiting in request queue. When the holder releases,
 * the cancelled request is skipped and deleted (not granted).
 * See: concurrency.rs::release_and_grant_next [SILO-GRANT-CXL]
 */
pred exampleCancelledRequestCleanup {
    some tid1, tid2: TaskId, j1, j2: Job, q: Queue, t1, t2, t3: Time | {
        lt[t1, t2] and lt[t2, t3]
        tid1 != tid2 and j1 != j2
        -- t1: j1 holds the queue, j2 is waiting as a request
        some h: TicketHolder | h.th_task = tid1 and h.th_queue = q and h.th_time = t1
        some r: TicketRequest | r.tr_job = j2 and r.tr_queue = q and r.tr_time = t1
        not isCancelledAt[j2, t1]
        -- t2: j2 is cancelled while still waiting (request still exists)
        isCancelledAt[j2, t2]
        some r: TicketRequest | r.tr_job = j2 and r.tr_queue = q and r.tr_time = t2
        -- t3: j1 completes, j2's cancelled request is cleaned up (not granted)
        statusAt[j1, t3] = Succeeded
        no r: TicketRequest | r.tr_job = j2 and r.tr_queue = q and r.tr_time = t3  -- Request removed
        no h: TicketHolder | h.th_task = tid2 and h.th_queue = q and h.th_time = t3  -- No holder for j2
    }
}

/**
 * Scenario: Running job with ticket is cancelled, worker completes, ticket released
 * Job holds a ticket and is running, gets cancelled, worker discovers on heartbeat,
 * completes work, ticket is released, next request can be granted.
 */
pred exampleCancelledJobReleasesTicketOnComplete {
    some tid: TaskId, j: Job, q: Queue, t1, t2, t3: Time | {
        lt[t1, t2] and lt[t2, t3]
        -- t1: job running with ticket held, not cancelled
        j in jobExistsAt[t1]
        some leaseAt[tid, t1]
        some h: TicketHolder | h.th_task = tid and h.th_queue = q and h.th_time = t1
        not isCancelledAt[j, t1]
        -- t2: job cancelled, still running (holder preserved)
        isCancelledAt[j, t2]
        some leaseAt[tid, t2]
        some h: TicketHolder | h.th_task = tid and h.th_queue = q and h.th_time = t2
        -- t3: worker completes, lease and holder released
        no leaseAt[tid, t3]
        no h: TicketHolder | h.th_task = tid and h.th_queue = q and h.th_time = t3
    }
}

/**
 * Scenario: Lease expires with ticket held, reaper releases both
 * Job holds a ticket and is running, worker crashes (lease expires),
 * reaper releases both the lease and the ticket.
 */
pred exampleLeaseExpiryReleasesTicket {
    some tid: TaskId, j: Job, q: Queue, t1, t2: Time | {
        lt[t1, t2]
        -- t1: lease expired, job has holder
        some leaseAt[tid, t1]
        leaseExpired[tid, t1]
        some h: TicketHolder | h.th_task = tid and h.th_queue = q and h.th_time = t1
        -- t2: lease reaped, holder also released
        no leaseAt[tid, t2]
        no h: TicketHolder | h.th_task = tid and h.th_queue = q and h.th_time = t2
        statusAt[j, t2] = Failed
    }
}

/**
 * Scenario: Cancelled job is restarted and completes successfully
 * Job is enqueued, cancelled before running, cleaned up by dequeue,
 * then restarted and successfully completes.
 */
pred exampleRestartCancelledJob {
    some tid1, tid2: TaskId, j: Job, t1, t2, t3, t4, t5: Time | {
        lt[t1, t2] and lt[t2, t3] and lt[t3, t4] and lt[t4, t5]
        tid1 != tid2
        -- t1: job enqueued and scheduled, NOT cancelled
        j in jobExistsAt[t1]
        statusAt[j, t1] = Scheduled
        not isCancelledAt[j, t1]
        some dbQueuedAt[tid1, t1]
        dbQueuedAt[tid1, t1] = j
        -- t2: job cancelled, task still exists (lazy cleanup)
        isCancelledAt[j, t2]
        statusAt[j, t2] = Scheduled
        -- t3: task cleaned up (via dequeueCleanupCancelled), no active tasks
        isCancelledAt[j, t3]
        no tid: TaskId | dbQueuedAt[tid, t3] = j
        no tid: TaskId | bufferedAt[tid, t3] = j
        no tid: TaskId | leaseJobAt[tid, t3] = j
        -- t4: job restarted, cancellation cleared, new task created
        not isCancelledAt[j, t4]
        statusAt[j, t4] = Scheduled
        some dbQueuedAt[tid2, t4]
        dbQueuedAt[tid2, t4] = j
        -- t5: job completes successfully
        statusAt[j, t5] = Succeeded
        not isCancelledAt[j, t5]
    }
}

/**
 * Scenario: Running job cancelled, worker acknowledges via retry, then restarted and completes
 * Job starts running, is cancelled mid-execution, worker discovers cancellation and 
 * acknowledges by doing a "retry" (which sets status to Scheduled and creates a new task).
 * The retry task is cleaned up by dequeueCleanupCancelled, leaving the job in a clean
 * cancelled+Scheduled state. Then the job is restarted and completes successfully.
 * 
 * This demonstrates:
 * 1. Cancellation during execution - worker discovers on heartbeat
 * 2. Worker can acknowledge cancellation via retry (status → Scheduled)
 * 3. Retry task gets cleaned up since job is cancelled
 * 4. Cancelled non-terminal job can be restarted
 * 5. Restarted job runs fresh and completes normally
 */
pred exampleCancelWhileRunningThenRestartAndComplete {
    some tid1, tid2, tid3: TaskId, j: Job, a1, a2: Attempt, t1, t2, t3, t4, t5, t6, t7: Time | {
        lt[t1, t2] and lt[t2, t3] and lt[t3, t4] and lt[t4, t5] and lt[t5, t6] and lt[t6, t7]
        tid1 != tid2 and tid2 != tid3 and tid1 != tid3
        a1 != a2
        attemptJob[a1] = j
        attemptJob[a2] = j
        
        -- t1: job is running (has lease), NOT cancelled
        j in jobExistsAt[t1]
        statusAt[j, t1] = Running
        not isCancelledAt[j, t1]
        some leaseAt[tid1, t1]
        leaseJobAt[tid1, t1] = j
        leaseAttemptAt[tid1, t1] = a1
        attemptStatusAt[a1, t1] = AttemptRunning
        
        -- t2: job cancelled while still running (worker discovers on heartbeat)
        isCancelledAt[j, t2]
        statusAt[j, t2] = Running
        some leaseAt[tid1, t2]  -- lease still active
        leaseJobAt[tid1, t2] = j
        attemptStatusAt[a1, t2] = AttemptRunning
        
        -- t3: worker acknowledges cancellation via retry (releases lease, creates retry task)
        isCancelledAt[j, t3]  -- still cancelled
        statusAt[j, t3] = Scheduled  -- retry sets status to Scheduled
        no leaseAt[tid1, t3]  -- lease released
        attemptStatusAt[a1, t3] = AttemptFailed  -- first attempt failed
        some dbQueuedAt[tid2, t3]  -- retry task created
        dbQueuedAt[tid2, t3] = j
        
        -- t4: retry task cleaned up (dequeueCleanupCancelled) since job is cancelled
        isCancelledAt[j, t4]
        statusAt[j, t4] = Scheduled  -- still Scheduled (non-terminal)
        no tid: TaskId | dbQueuedAt[tid, t4] = j  -- no tasks in DB queue
        no tid: TaskId | bufferedAt[tid, t4] = j  -- no tasks in buffer
        no tid: TaskId | leaseJobAt[tid, t4] = j  -- no leases
        
        -- t5: job restarted (cancellation cleared, new task created)
        not isCancelledAt[j, t5]  -- cancellation cleared
        statusAt[j, t5] = Scheduled
        some dbQueuedAt[tid3, t5]
        dbQueuedAt[tid3, t5] = j
        
        -- t6: new task is running
        statusAt[j, t6] = Running
        not isCancelledAt[j, t6]
        some leaseAt[tid3, t6]
        leaseJobAt[tid3, t6] = j
        leaseAttemptAt[tid3, t6] = a2
        attemptStatusAt[a2, t6] = AttemptRunning
        
        -- t7: second execution completes successfully
        statusAt[j, t7] = Succeeded
        not isCancelledAt[j, t7]
        no leaseAt[tid3, t7]
        attemptStatusAt[a2, t7] = AttemptSucceeded
        
        -- Both attempts exist at the end
        a1 in attemptExistsAt[t7]
        a2 in attemptExistsAt[t7]
    }
}

/**
 * Scenario: Failed job is restarted and completes successfully
 * Job runs, fails permanently (not a retry), then is restarted and completes.
 * 
 * This demonstrates:
 * 1. Job reaches terminal Failed state
 * 2. Failed job can be restarted (unlike Succeeded)
 * 3. Restarted job runs fresh and completes normally
 */
pred exampleRestartFailedJob {
    some tid1, tid2: TaskId, j: Job, a1, a2: Attempt, t1, t2, t3, t4, t5: Time | {
        lt[t1, t2] and lt[t2, t3] and lt[t3, t4] and lt[t4, t5]
        tid1 != tid2
        a1 != a2
        attemptJob[a1] = j
        attemptJob[a2] = j
        
        -- t1: job is running
        j in jobExistsAt[t1]
        statusAt[j, t1] = Running
        not isCancelledAt[j, t1]
        some leaseAt[tid1, t1]
        leaseJobAt[tid1, t1] = j
        leaseAttemptAt[tid1, t1] = a1
        attemptStatusAt[a1, t1] = AttemptRunning
        
        -- t2: job fails permanently (no retry)
        statusAt[j, t2] = Failed
        no leaseAt[tid1, t2]
        attemptStatusAt[a1, t2] = AttemptFailed
        not isCancelledAt[j, t2]
        -- No active tasks (final failure, no retry task)
        no tid: TaskId | dbQueuedAt[tid, t2] = j
        no tid: TaskId | bufferedAt[tid, t2] = j
        no tid: TaskId | leaseJobAt[tid, t2] = j
        
        -- t3: job is restarted (restartFailedJob transition)
        statusAt[j, t3] = Scheduled
        not isCancelledAt[j, t3]
        some dbQueuedAt[tid2, t3]
        dbQueuedAt[tid2, t3] = j
        
        -- t4: new task is running
        statusAt[j, t4] = Running
        not isCancelledAt[j, t4]
        some leaseAt[tid2, t4]
        leaseJobAt[tid2, t4] = j
        leaseAttemptAt[tid2, t4] = a2
        attemptStatusAt[a2, t4] = AttemptRunning
        
        -- t5: job completes successfully
        statusAt[j, t5] = Succeeded
        not isCancelledAt[j, t5]
        no leaseAt[tid2, t5]
        attemptStatusAt[a2, t5] = AttemptSucceeded
        
        -- Both attempts exist at the end
        a1 in attemptExistsAt[t5]
        a2 in attemptExistsAt[t5]
    }
}

/**
 * Scenario: Failed+cancelled job is restarted
 * Job runs, is cancelled while running, fails (worker finishes with failure),
 * then is restarted (clearing both Failed status and cancellation).
 */
pred exampleRestartFailedAndCancelledJob {
    some tid1, tid2: TaskId, j: Job, a1, a2: Attempt, t1, t2, t3, t4, t5, t6: Time | {
        lt[t1, t2] and lt[t2, t3] and lt[t3, t4] and lt[t4, t5] and lt[t5, t6]
        tid1 != tid2
        a1 != a2
        attemptJob[a1] = j
        attemptJob[a2] = j
        
        -- t1: job is running, not cancelled
        j in jobExistsAt[t1]
        statusAt[j, t1] = Running
        not isCancelledAt[j, t1]
        some leaseAt[tid1, t1]
        leaseJobAt[tid1, t1] = j
        
        -- t2: job cancelled while running
        statusAt[j, t2] = Running
        isCancelledAt[j, t2]
        some leaseAt[tid1, t2]
        
        -- t3: worker finishes with failure (despite cancellation)
        statusAt[j, t3] = Failed
        isCancelledAt[j, t3]  -- still cancelled
        no leaseAt[tid1, t3]
        attemptStatusAt[a1, t3] = AttemptFailed
        -- No active tasks
        no tid: TaskId | dbQueuedAt[tid, t3] = j
        no tid: TaskId | leaseJobAt[tid, t3] = j
        
        -- t4: job is restarted (clears both Failed and cancellation)
        statusAt[j, t4] = Scheduled
        not isCancelledAt[j, t4]  -- cancellation cleared!
        some dbQueuedAt[tid2, t4]
        dbQueuedAt[tid2, t4] = j
        
        -- t5: new task is running
        statusAt[j, t5] = Running
        not isCancelledAt[j, t5]
        some leaseAt[tid2, t5]
        leaseJobAt[tid2, t5] = j
        leaseAttemptAt[tid2, t5] = a2
        
        -- t6: job completes successfully
        statusAt[j, t6] = Succeeded
        not isCancelledAt[j, t6]
        no leaseAt[tid2, t6]
        attemptStatusAt[a2, t6] = AttemptSucceeded
    }
}

-- Note: JobState count = Jobs × Times where job exists (not all times)
-- AttemptExists and JobExists need 1 per Time
-- JobCancelled: needs entries for times when job is cancelled
run exampleSuccess for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 6 Time,
    6 JobState, 6 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 6 AttemptExists, 6 JobExists, 2 JobAttemptRelation, 6 JobCancelled
    
run exampleRetry for 3 but exactly 1 Job, 1 Worker, 3 TaskId, 2 Attempt, 10 Time,
    10 JobState, 10 AttemptState, 5 DbQueuedTask, 5 BufferedTask, 4 Lease, 10 AttemptExists, 10 JobExists, 2 JobAttemptRelation, 10 JobCancelled

run examplePermanentFailureWithRetry for 3 but exactly 1 Job, 1 Worker, 3 TaskId, 3 Attempt, 10 Time,
    10 JobState, 10 AttemptState, 5 DbQueuedTask, 5 BufferedTask, 4 Lease, 10 AttemptExists, 10 JobExists, 3 JobAttemptRelation, 10 JobCancelled

run exampleStaleBuffer for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 1 Attempt, 6 Time,
    6 JobState, 6 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 6 AttemptExists, 6 JobExists, 1 JobAttemptRelation, 6 JobCancelled
run exampleCancellationLazyCleanup for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 1 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 8 AttemptExists, 8 JobExists, 1 JobAttemptRelation, 8 JobCancelled

run exampleDequeueSkipsCancelledTask for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled
run exampleCancellationDiscoveryOnHeartbeat for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled

run exampleHeartbeat for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled

run exampleLeaseExpiry for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled

run exampleRestartCancelledJob for 3 but exactly 1 Job, 1 Worker, 3 TaskId, 2 Attempt, 12 Time,
    12 JobState, 12 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 12 AttemptExists, 12 JobExists, 2 JobAttemptRelation, 12 JobCancelled

run exampleCancelWhileRunningThenRestartAndComplete for 3 but exactly 1 Job, 1 Worker, 4 TaskId, 3 Attempt, 14 Time,
    14 JobState, 14 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 6 Lease, 14 AttemptExists, 14 JobExists, 3 JobAttemptRelation, 14 JobCancelled

run exampleRestartFailedJob for 3 but exactly 1 Job, 1 Worker, 3 TaskId, 3 Attempt, 12 Time,
    12 JobState, 12 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 12 AttemptExists, 12 JobExists, 3 JobAttemptRelation, 12 JobCancelled

run exampleRestartFailedAndCancelledJob for 3 but exactly 1 Job, 1 Worker, 3 TaskId, 3 Attempt, 14 Time,
    14 JobState, 14 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 14 AttemptExists, 14 JobExists, 3 JobAttemptRelation, 14 JobCancelled

-- Concurrency examples need Queue, TicketRequest, TicketHolder, JobQueueRequirement
run exampleConcurrencyGrantedImmediately for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time, 1 Queue,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled,
    1 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder

run exampleConcurrencyWaitsInQueue for 4 but exactly 2 Job, 1 Worker, 3 TaskId, 3 Attempt, 10 Time, 1 Queue,
    20 JobState, 10 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 10 AttemptExists, 10 JobExists, 3 JobAttemptRelation, 20 JobCancelled,
    2 JobQueueRequirement, 10 TicketRequest, 10 TicketHolder

run exampleCancelledRequestCleanup for 4 but exactly 2 Job, 1 Worker, 3 TaskId, 3 Attempt, 10 Time, 1 Queue,
    20 JobState, 10 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 10 AttemptExists, 10 JobExists, 3 JobAttemptRelation, 20 JobCancelled,
    2 JobQueueRequirement, 10 TicketRequest, 10 TicketHolder

run exampleCancelledJobReleasesTicketOnComplete for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time, 1 Queue,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled,
    1 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder

run exampleLeaseExpiryReleasesTicket for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time, 1 Queue,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled,
    1 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder

-- ========================================================================
-- DISTRIBUTED CONCURRENCY PROTOCOL EXAMPLES
-- ========================================================================

/**
 * Example: Remote ticket request flow - job on shard A, queue owned by shard B
 * 1. Job enqueued on shard A, creates RequestRemoteTicket task
 * 2. Task processed, request stored on shard B (queue owner)
 * 3. Queue grants ticket, creates NotifyRemoteTicketGrant task
 * 4. Notification processed, RunAttempt task created on shard A
 * 5. Job runs and completes successfully
 */
pred exampleRemoteTicketFlow {
    some j: Job, q: Queue, s1, s2: Shard, t1, t2, t3, t4, t5, t6, t7: Time,
         reqTid, notifyTid, runTid: TaskId | {
        lt[t1, t2] and lt[t2, t3] and lt[t3, t4] and lt[t4, t5] and lt[t5, t6] and lt[t6, t7]
        reqTid != notifyTid and notifyTid != runTid and reqTid != runTid
        s1 != s2
        
        -- Setup: job shard is s1, queue owner is s2 (remote)
        jobShardOf[j] = s1
        queueOwnerOf[q] = s2
        q in jobQueues[j]
        
        -- t1: Job enqueued, RequestRemoteTicket task created
        j in jobExistsAt[t1]
        statusAt[j, t1] = Scheduled
        some rrt: RequestRemoteTicketTask | rrt.rrt_job = j and rrt.rrt_queue = q and rrt.rrt_time = t1
        some dbQueuedAt[reqTid, t1]
        
        -- t2: Request task processed, request stored on queue owner
        no rrt: RequestRemoteTicketTask | rrt.rrt_job = j and rrt.rrt_queue = q and rrt.rrt_time = t2
        some r: TicketRequest | r.tr_job = j and r.tr_queue = q and r.tr_time = t2
        
        -- t3: Queue grants ticket, creates NotifyRemoteTicketGrant task
        some nrt: NotifyRemoteTicketGrantTask | nrt.nrt_job = j and nrt.nrt_queue = q and nrt.nrt_time = t3
        some h: TicketHolder | h.th_queue = q and h.th_time = t3
        
        -- t4: Notification processed, RunAttempt task created on job shard
        no nrt: NotifyRemoteTicketGrantTask | nrt.nrt_job = j and nrt.nrt_queue = q and nrt.nrt_time = t4
        some dbQueuedAt[runTid, t4]
        dbQueuedAt[runTid, t4] = j
        
        -- t5: Job is running
        statusAt[j, t5] = Running
        some leaseAt[runTid, t5]
        
        -- t6/t7: Job completes successfully
        statusAt[j, t7] = Succeeded
    }
}

/**
 * Example: Remote ticket release flow
 * Job completes on shard A, creates ReleaseRemoteTicket task,
 * release processed and holder deleted on shard B.
 */
pred exampleRemoteTicketRelease {
    some j: Job, q: Queue, s1, s2: Shard, t1, t2, t3: Time, runTid, releaseTid: TaskId | {
        lt[t1, t2] and lt[t2, t3]
        runTid != releaseTid
        s1 != s2
        
        -- Setup: remote queue
        jobShardOf[j] = s1
        queueOwnerOf[q] = s2
        
        -- t1: Job running with remote holder
        j in jobExistsAt[t1]
        statusAt[j, t1] = Running
        some leaseAt[runTid, t1]
        leaseJobAt[runTid, t1] = j
        some h: TicketHolder | h.th_task = runTid and h.th_queue = q and h.th_time = t1
        
        -- t2: Job completed, release task exists, holder still exists
        statusAt[j, t2] = Succeeded
        no leaseAt[runTid, t2]
        some relt: ReleaseRemoteTicketTask | relt.relt_job = j and relt.relt_queue = q and relt.relt_time = t2
        some h: TicketHolder | h.th_task = runTid and h.th_queue = q and h.th_time = t2
        
        -- t3: Release processed, holder deleted
        no relt: ReleaseRemoteTicketTask | relt.relt_job = j and relt.relt_queue = q and relt.relt_time = t3
        no h: TicketHolder | h.th_task = runTid and h.th_queue = q and h.th_time = t3
    }
}

/**
 * Example: Two jobs on different shards competing for same queue
 * Demonstrates global concurrency enforcement across shards.
 */
pred exampleCrossShardConcurrency {
    some j1, j2: Job, q: Queue, s1, s2, sqo: Shard, t1, t2, t3: Time | {
        lt[t1, t2] and lt[t2, t3]
        j1 != j2
        s1 != s2
        -- sqo could be either s1, s2, or a third shard
        
        -- Setup: jobs on different shards, same queue requirement
        jobShardOf[j1] = s1
        jobShardOf[j2] = s2
        queueOwnerOf[q] = sqo
        q in jobQueues[j1]
        q in jobQueues[j2]
        
        -- t1: j1 holds the queue, j2 is waiting
        some h: TicketHolder | h.th_queue = q and h.th_time = t1
        (some r: TicketRequest | r.tr_job = j2 and r.tr_queue = q and r.tr_time = t1) or
        (some rrt: RequestRemoteTicketTask | rrt.rrt_job = j2 and rrt.rrt_queue = q and rrt.rrt_time = t1)
        
        -- t2: j1 completes
        statusAt[j1, t2] = Succeeded
        
        -- t3: j2 now holds the queue
        some h: TicketHolder | h.th_queue = q and h.th_time = t3
    }
}

/**
 * Example: Start a job immediately (no concurrency requirement with remote queue)
 * Tests the fast path where queue owner == job shard.
 */
pred exampleStartNowInitial {
    some j: Job, t: Time | {
        j in jobExistsAt[t]
        statusAt[j, t] = Running
    }
}

/**
 * Example: Start now with retry path
 */
pred exampleStartNowRetry {
    some j: Job, t: Time, a1, a2: Attempt | {
        a1 != a2
        attemptJob[a1] = j
        attemptJob[a2] = j
        j in jobExistsAt[t]
        statusAt[j, t] = Succeeded
        attemptStatusAt[a1, t] = AttemptFailed
        attemptStatusAt[a2, t] = AttemptSucceeded
    }
}

-- Bounds analysis for checks:
-- JobState: Jobs can only exist from creation onwards, so max ~= Jobs × Times (16 for 2×8)
-- AttemptState: Attempts created during execution, max = Attempts × remaining_times (~18 for 3 attempts)
-- DbQueuedTask: Task in queue for limited time, max ~= TaskIds × avg_queue_time (~6)
-- BufferedTask: Similar to DbQueuedTask (~6)
-- Lease: One per task at a time, max = TaskIds (~3-4)
-- AttemptExists/JobExists: Exactly 1 per Time (8)
-- JobAttemptRelation: One per Attempt (3)
-- JobCancelled: Once cancelled stays cancelled, max = Jobs × remaining_times after cancellation (~12)
-- TicketRequest: Jobs × Queues × Times but requests are short-lived (~8)
-- TicketHolder: Queues × Times with limit=1 per queue (~8)

check noDoubleLease for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 18 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 12 JobCancelled
check oneLeasePerJob for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 18 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 12 JobCancelled
check leaseJobMustBeRunning for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 18 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 12 JobCancelled
check runningJobHasRunningAttempt for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 18 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 12 JobCancelled
check attemptTerminalIsForever for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 18 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 12 JobCancelled
check validTransitions for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 18 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 12 JobCancelled
check noZombieAttempts for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 18 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 12 JobCancelled
check noQueuedTasksForTerminal for 4 but 2 Job, 2 Worker, 4 TaskId, 3 Attempt, 8 Time, 2 Queue, 2 Shard,
    16 JobState, 18 AttemptState, 8 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 12 JobCancelled,
    2 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder, 2 JobShard, 2 QueueOwner,
    8 RequestRemoteTicketTask, 8 NotifyRemoteTicketGrantTask, 8 ReleaseRemoteTicketTask
check noLeasesForTerminal for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 18 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 12 JobCancelled
check cancellationClearedRequiresRestartable for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 18 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled
check restartedJobIsScheduledWithTask for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 18 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled

-- Concurrency ticket assertions (with Queue, TicketRequest, TicketHolder, JobQueueRequirement bounds)
check queueLimitEnforced for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time, 2 Queue,
    16 JobState, 18 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 12 JobCancelled,
    4 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder

check holdersRequireActiveTask for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time, 2 Queue,
    16 JobState, 18 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 12 JobCancelled,
    4 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder

check grantedMeansNoRequest for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time, 2 Queue,
    16 JobState, 18 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 12 JobCancelled,
    4 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder

-- ========================================================================
-- DISTRIBUTED CONCURRENCY PROTOCOL CHECKS AND RUNS
-- ========================================================================

-- Distributed protocol needs: Shard, JobShard, QueueOwner, RequestRemoteTicketTask, NotifyRemoteTicketGrantTask, ReleaseRemoteTicketTask
-- Typical bounds: 2-3 Shards (to model remote vs local), more TaskIds for request/notify/release tasks

run exampleRemoteTicketFlow for 5 but exactly 1 Job, 1 Worker, 4 TaskId, 2 Attempt, 14 Time, 1 Queue, 2 Shard,
    14 JobState, 14 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 14 AttemptExists, 14 JobExists, 2 JobAttemptRelation, 14 JobCancelled,
    1 JobQueueRequirement, 14 TicketRequest, 14 TicketHolder, 1 JobShard, 1 QueueOwner,
    14 RequestRemoteTicketTask, 14 NotifyRemoteTicketGrantTask, 14 ReleaseRemoteTicketTask

run exampleRemoteTicketRelease for 4 but exactly 1 Job, 1 Worker, 3 TaskId, 2 Attempt, 10 Time, 1 Queue, 2 Shard,
    10 JobState, 10 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 10 AttemptExists, 10 JobExists, 2 JobAttemptRelation, 10 JobCancelled,
    1 JobQueueRequirement, 10 TicketRequest, 10 TicketHolder, 1 JobShard, 1 QueueOwner,
    10 RequestRemoteTicketTask, 10 NotifyRemoteTicketGrantTask, 10 ReleaseRemoteTicketTask

run exampleCrossShardConcurrency for 5 but exactly 2 Job, 1 Worker, 4 TaskId, 3 Attempt, 10 Time, 1 Queue, 3 Shard,
    20 JobState, 10 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 10 AttemptExists, 10 JobExists, 3 JobAttemptRelation, 20 JobCancelled,
    2 JobQueueRequirement, 10 TicketRequest, 10 TicketHolder, 2 JobShard, 1 QueueOwner,
    10 RequestRemoteTicketTask, 10 NotifyRemoteTicketGrantTask, 10 ReleaseRemoteTicketTask

run exampleStartNowInitial for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 6 Time, 0 Queue, 1 Shard,
    6 JobState, 6 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 6 AttemptExists, 6 JobExists, 2 JobAttemptRelation, 6 JobCancelled,
    0 JobQueueRequirement, 0 TicketRequest, 0 TicketHolder, 1 JobShard, 0 QueueOwner,
    0 RequestRemoteTicketTask, 0 NotifyRemoteTicketGrantTask, 0 ReleaseRemoteTicketTask

run exampleStartNowRetry for 3 but exactly 1 Job, 1 Worker, 3 TaskId, 2 Attempt, 10 Time, 0 Queue, 1 Shard,
    10 JobState, 10 AttemptState, 5 DbQueuedTask, 5 BufferedTask, 4 Lease, 10 AttemptExists, 10 JobExists, 2 JobAttemptRelation, 10 JobCancelled,
    0 JobQueueRequirement, 0 TicketRequest, 0 TicketHolder, 1 JobShard, 0 QueueOwner,
    0 RequestRemoteTicketTask, 0 NotifyRemoteTicketGrantTask, 0 ReleaseRemoteTicketTask

-- Distributed concurrency protocol assertions
check globalConcurrencyLimitEnforced for 5 but 2 Job, 2 Worker, 4 TaskId, 3 Attempt, 8 Time, 2 Queue, 2 Shard,
    16 JobState, 18 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled,
    4 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder, 2 JobShard, 2 QueueOwner,
    8 RequestRemoteTicketTask, 8 NotifyRemoteTicketGrantTask, 8 ReleaseRemoteTicketTask

check oneTicketStatePerJobQueue for 5 but 2 Job, 2 Worker, 4 TaskId, 3 Attempt, 8 Time, 2 Queue, 2 Shard,
    16 JobState, 18 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled,
    4 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder, 2 JobShard, 2 QueueOwner,
    8 RequestRemoteTicketTask, 8 NotifyRemoteTicketGrantTask, 8 ReleaseRemoteTicketTask
