/**
 * Silo Job Queue Shard - Dynamic State Machine Specification
 * 
 * This Alloy model verifies the algorithms that create and assign jobs, tasks, and leases.
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
 */
sig TicketHolder {
    th_task: one TaskId,
    th_queue: one Queue,
    th_time: one Time     -- Time when this holder exists
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
    -- (in DB, buffer, lease, or request), it can only be associated with that same job
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
    
    -- At most one request per (task, queue) at each time.
    -- Multiple attempts of the same job may have outstanding requests concurrently.
    all tid: TaskId, q: Queue, t: Time | lone r: TicketRequest |
        r.tr_task = tid and r.tr_queue = q and r.tr_time = t
    
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
    -- 3. Has a lease (running)
    -- This allows holders to exist before lease (granted at enqueue, leased at dequeue)
    all h: TicketHolder | 
        some dbQueuedAt[h.th_task, h.th_time] or 
        some bufferedAt[h.th_task, h.th_time] or 
        some leaseAt[h.th_task, h.th_time]
    
    -- A holder and request can never coexist for the same (task, queue) at the same time
    -- (This is enforced by transitions but stated explicitly for clarity)
    all t: Time, tid: TaskId, q: Queue | 
        tid in holdersAt[q, t] implies tid not in requestTasksAt[q, t]
}

/** Check if a job is cancelled at time t */
pred isCancelledAt[j: Job, t: Time] {
    some c: JobCancelled | c.cancelled_job = j and c.cancelled_time = t
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
    some s
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
pred enqueueFrameConditions[tid: TaskId, t: Time, tnext: Time] {
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
}

-- Transition: ENQUEUE (no concurrency) - Job has no concurrency requirements
pred enqueue[tid: TaskId, j: Job, t: Time, tnext: Time] {
    enqueuePreConditions[tid, j, t]
    enqueueJobCreated[j, t, tnext]
    enqueueFrameConditions[tid, t, tnext]
    
    -- Pre: Job does NOT require any concurrency queues
    no jobQueues[j]
    
    -- Post: Create RunAttempt task in DB queue
    one qt: DbQueuedTask | qt.db_qtask = tid and qt.db_qjob = j and qt.db_qtime = tnext
    all tid2: TaskId | tid2 != tid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- Frame: Concurrency state unchanged
    concurrencyUnchanged[t, tnext]
}

-- Transition: ENQUEUE_WITH_CONCURRENCY_GRANTED - Job requires queue, granted immediately
pred enqueueWithConcurrencyGranted[tid: TaskId, j: Job, q: Queue, t: Time, tnext: Time] {
    enqueuePreConditions[tid, j, t]
    enqueueJobCreated[j, t, tnext]
    enqueueFrameConditions[tid, t, tnext]
    
    -- Pre: Job requires this queue
    q in jobQueues[j]
    
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
}

-- Transition: ENQUEUE_WITH_CONCURRENCY_QUEUED - Job requires queue, must wait
pred enqueueWithConcurrencyQueued[tid: TaskId, j: Job, q: Queue, t: Time, tnext: Time] {
    enqueuePreConditions[tid, j, t]
    enqueueJobCreated[j, t, tnext]
    enqueueFrameConditions[tid, t, tnext]
    
    -- Pre: Job requires this queue
    q in jobQueues[j]
    
    -- [SILO-ENQ-CONC-4] Pre: Queue is at capacity (has a holder)
    not queueHasCapacity[q, t]
    
    -- [SILO-ENQ-CONC-5] Post: NO task in DB queue (cannot proceed yet)
    no qt: DbQueuedTask | qt.db_qtask = tid and qt.db_qtime = tnext
    -- [SILO-ENQ-CONC-6] Post: Create request record in concurrency requests namespace
    one r: TicketRequest | r.tr_job = j and r.tr_queue = q and r.tr_task = tid and r.tr_time = tnext
    all tid2: TaskId | dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]

    -- Frame: requests in queue q = old + new request, other queues unchanged
    requestTasksAt[q, tnext] = requestTasksAt[q, t] + tid
    all q2: Queue | q2 != q implies requestTasksAt[q2, tnext] = requestTasksAt[q2, t]
    holdersUnchanged[t, tnext]
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

        -- Note: No cancelled check here. Cancelled jobs' tasks are eagerly removed
        -- by cancelJob. If a task for a cancelled job appears (e.g., retry after cancel),
        -- it will be leased normally and the worker discovers cancellation via heartbeat.

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

-- Transition: GRANT_NEXT_REQUEST - Grant ticket to next waiting request
-- When a queue has capacity (no holders) and there's a pending request for a
-- currently scheduled job, grant it.
-- Implementation: driven by a background grant scanner
-- (ConcurrencyManager::process_grants) rather than being called synchronously
-- during release. Callers signal the scanner via request_grant() after
-- releasing a holder; the scanner serializes all grants to prevent
-- double-grant races from concurrent releases.
pred grantNextRequest[q: Queue, reqTid: TaskId, t: Time, tnext: Time] {
    -- [SILO-GRANT-1] Pre: Queue has capacity (limit=1, no holders)
    queueHasCapacity[q, t]

    -- [SILO-GRANT-2] Pre: There is a pending request for this queue
    some r: TicketRequest | r.tr_queue = q and r.tr_time = t and r.tr_task = reqTid
    let r = { req: TicketRequest | req.tr_queue = q and req.tr_time = t and req.tr_task = reqTid } | {
        one r
        let j = r.tr_job | {
            -- [SILO-GRANT-5] Pre: Request still targets a currently scheduled job.
            -- (Rust also validates attempt number; attempt identity is abstracted in this model.)
            statusAt[j, t] = Scheduled

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
}

-- Transition: DROP_STALE_REQUEST - Remove a stale request without granting work.
-- This models lazy cleanup in the grant scanner when request status drift is detected.
pred dropStaleRequest[q: Queue, reqTid: TaskId, t: Time, tnext: Time] {
    some r: TicketRequest | r.tr_queue = q and r.tr_time = t and r.tr_task = reqTid
    let r = { req: TicketRequest | req.tr_queue = q and req.tr_time = t and req.tr_task = reqTid } | {
        one r
        -- [SILO-GRANT-6] Post: stale request record is deleted (no holder/task grant).
        statusAt[r.tr_job, t] != Scheduled
    }

    -- Delete only the selected request key.
    requestTasksAt[q, tnext] = requestTasksAt[q, t] - reqTid
    all q2: Queue | q2 != q implies requestTasksAt[q2, tnext] = requestTasksAt[q2, t]

    -- Frame: grant side effects do not happen on stale cleanup.
    holdersUnchanged[t, tnext]
    all tid: TaskId | dbQueuedAt[tid, tnext] = dbQueuedAt[tid, t]
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    all j: Job | j in jobExistsAt[t] implies statusAt[j, tnext] = statusAt[j, t]
    all j: Job | isCancelledAt[j, tnext] iff isCancelledAt[j, t]
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
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
}

-- Transition: COMPLETE_FAILURE_RETRY_RELEASE_TICKET - Worker reports retriable failure while holding concurrency ticket
-- The ticket is released so other jobs can use it during the retry backoff period.
-- When the retry task runs later, it will need to re-acquire the ticket (via grantNextRequest or enqueue path).
pred completeFailureRetryReleaseTicket[tid: TaskId, w: Worker, q: Queue, newTid: TaskId, t: Time, tnext: Time] {
    -- [SILO-RETRY-1] Pre: worker holds lease
    leaseAt[tid, t] = w
    some leaseJobAt[tid, t]
    some leaseAttemptAt[tid, t]
    
    newTid != tid
    no dbQueuedAt[newTid, t]
    no bufferedAt[newTid, t]
    no leaseAt[newTid, t]
    
    -- Pre: Task holds a ticket for this queue
    some h: TicketHolder | h.th_task = tid and h.th_queue = q and h.th_time = t
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        one j
        one a
        attemptStatusAt[a, t] = AttemptRunning
        
        -- [SILO-RETRY-2] Post: release lease
        no leaseAt[tid, tnext]
        
        -- [SILO-RETRY-4] Post: set attempt status to AttemptFailed
        attemptStatusAt[a, tnext] = AttemptFailed
        
        -- [SILO-RETRY-5-CONC] Post: Create a TicketRequest for the new retry task
        -- The retry task doesn't get immediate holder - it must wait in the request queue
        -- This allows other jobs to acquire the ticket during the retry backoff
        one r: TicketRequest | r.tr_job = j and r.tr_queue = q and r.tr_task = newTid and r.tr_time = tnext
        
        -- Note: NO DbQueuedTask is created yet - the task will be created when grantNextRequest grants the ticket
        -- This matches the enqueueWithConcurrencyQueued pattern
        all tid2: TaskId | dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
        
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
    all tid2: TaskId | bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
    
    -- [SILO-RETRY-REL] Release the holder so other jobs can acquire the ticket
    -- (inline releaseHolder without requestsUnchanged, since we also create a new request)
    holdersAt[q, tnext] = holdersAt[q, t] - tid
    all q2: Queue | q2 != q implies holdersAt[q2, tnext] = holdersAt[q2, t]
    -- Frame: requests in queue q = old + newTid, other queues unchanged
    requestTasksAt[q, tnext] = requestTasksAt[q, t] + newTid
    all q2: Queue | q2 != q implies requestTasksAt[q2, tnext] = requestTasksAt[q2, t]
}

-- Transition: CANCEL - mark job as cancelled
-- [SILO-CXL-3] Eagerly removes tasks from DB queue and buffer for scheduled jobs.
-- Eagerly releases concurrency holders and deletes requests for the cancelled job's tasks.
-- For running jobs (lease exists), no task/concurrency cleanup (worker discovers on heartbeat).
-- See: job_store_shard/cancel.rs
pred cancelJob[j: Job, t: Time, tnext: Time] {
    -- [SILO-CXL-1] Pre: job exists and not already cancelled
    j in jobExistsAt[t]
    not isCancelledAt[j, t]

    -- [SILO-CXL-2] Post: Mark job as cancelled (add cancellation record)
    one c: JobCancelled | c.cancelled_job = j and c.cancelled_time = tnext

    -- Post: Status stays the same (cancellation is orthogonal to status)
    statusAt[j, tnext] = statusAt[j, t]

    -- [SILO-CXL-3] Eagerly remove tasks for this job from DB queue
    -- Buffer is NOT cleaned atomically: evict_keys is post-commit and best-effort,
    -- so a stale copy may remain if fillBuffer races with cancel.
    all tid: TaskId | dbQueuedAt[tid, t] = j implies no dbQueuedAt[tid, tnext]
    all tid: TaskId | dbQueuedAt[tid, t] != j implies dbQueuedAt[tid, tnext] = dbQueuedAt[tid, t]

    -- Buffer unchanged (evict_keys is post-commit best-effort, may miss stale entries)
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

    -- Concurrency cleanup: release holders for cancelled job's tasks (removed from DB queue)
    -- and delete requests for cancelled job
    all q: Queue | holdersAt[q, tnext] = { tid: holdersAt[q, t] | dbQueuedAt[tid, t] != j }
    all q: Queue | requestTasksAt[q, tnext] = { tid: requestTasksAt[q, t] |
        no r: TicketRequest | r.tr_job = j and r.tr_queue = q and r.tr_task = tid and r.tr_time = t }
}

-- Transition: RESTART_CANCELLED_JOB - Re-enable a cancelled job
-- A cancelled job can be restarted if it hasn't completed (not terminal)
-- and has no active tasks (eagerly cleaned up by cancelJob).
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
    -- Note: If job had pending requests/holders, they were eagerly cleaned up by cancelJob
    concurrencyUnchanged[t, tnext]
}

-- Transition: EXPEDITE_NEXT_ATTEMPT - Move a future-scheduled task to run immediately
-- This allows dragging forward a job that was scheduled to run in the future.
-- 
-- Race safety:
-- - Task must be in DB queue but NOT in buffer (future-scheduled, not yet ready)
-- - No lease must exist for the job (not currently running)
-- - If the task is already in the buffer (ready) or leased (running), expedite is rejected
--
-- In the real implementation:
-- - Tasks are keyed by timestamp, so "future-scheduled" means timestamp > now
-- - Expedite deletes the old task key and creates a new one with timestamp = now
-- - The broker then picks it up on the next scan
--
-- See: job_store_shard/expedite.rs
pred expediteNextAttempt[tid: TaskId, j: Job, t: Time, tnext: Time] {
    -- [SILO-EXP-1] Pre: job exists
    j in jobExistsAt[t]
    
    -- [SILO-EXP-2] Pre: job is NOT terminal (Succeeded or Failed cannot be expedited)
    not isTerminal[statusAt[j, t]]
    
    -- [SILO-EXP-3] Pre: job is NOT cancelled
    not isCancelledAt[j, t]
    
    -- [SILO-EXP-4] Pre: task exists in DB queue for this job
    dbQueuedAt[tid, t] = j
    
    -- [SILO-EXP-5] Pre: task is NOT in buffer (task is future-scheduled, not yet ready)
    -- If task is in buffer, it's already ready to run and expedite should be rejected.
    -- This provides race safety against the broker picking up the task.
    no bufferedAt[tid, t]
    
    -- [SILO-EXP-6] Pre: job has no active lease (not currently running)
    -- This provides race safety against dequeue starting the task.
    no tid2: TaskId | leaseJobAt[tid2, t] = j
    
    -- Post: Task remains in DB queue (now conceptually at current time)
    -- In the real implementation, the task is deleted and re-inserted with timestamp = now.
    -- In Alloy, we model this as the task staying in DB queue (it will be scanned soon).
    dbQueuedAt[tid, tnext] = j
    
    -- Frame: other DB queue entries unchanged
    all tid2: TaskId | tid2 != tid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    
    -- Frame: buffer unchanged
    all tid2: TaskId | bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
    
    -- Frame: leases unchanged
    all tid2: TaskId | {
        leaseAt[tid2, tnext] = leaseAt[tid2, t]
        leaseJobAt[tid2, tnext] = leaseJobAt[tid2, t]
        leaseAttemptAt[tid2, tnext] = leaseAttemptAt[tid2, t]
    }
    
    -- Frame: job status unchanged (still Scheduled)
    all j2: Job | j2 in jobExistsAt[t] implies statusAt[j2, tnext] = statusAt[j2, t]
    
    -- Frame: cancellation unchanged
    all j2: Job | isCancelledAt[j2, tnext] iff isCancelledAt[j2, t]
    
    -- Frame: job existence unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    
    -- Frame: attempts unchanged
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    
    -- Frame: concurrency state unchanged
    -- Note: If the job had concurrency requirements and was granted immediately,
    -- the holder stays with the task. If it was queued (TicketRequest), there's
    -- no DbQueuedTask to expedite anyway.
    concurrencyUnchanged[t, tnext]
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
}

/** Import precondition: job must not exist */
pred importPreConditions[j: Job, t: Time] {
    -- [SILO-IMP-1] Pre: job does NOT exist yet
    j not in jobExistsAt[t]
}

/**
 * Shared postcondition for adding terminal attempts during import/reimport.
 * Does NOT require `some` new attempts - callers add that constraint when needed.
 */
pred importNewAttempts[j: Job, t: Time, tnext: Time] {
    -- Post: existing attempts preserved (no removals)
    attemptExistsAt[t] in attemptExistsAt[tnext]
    -- [SILO-IMP-2] Post: existing attempt statuses unchanged
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    -- [SILO-IMP-3] Post: all new attempts belong to j and are terminal
    all a: attemptExistsAt[tnext] - attemptExistsAt[t] | attemptJob[a] = j and isTerminalAttempt[attemptStatusAt[a, tnext]]
}

/** Frame conditions for import (buffer, existing jobs, leases unchanged) */
pred importFrameConditions[t: Time, tnext: Time] {
    -- Buffer unchanged
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    -- Frame: other existing jobs unchanged (status and cancellation)
    all j2: Job | j2 in jobExistsAt[t] implies statusAt[j2, tnext] = statusAt[j2, t]
    all j2: Job | j2 in jobExistsAt[t] implies (isCancelledAt[j2, tnext] iff isCancelledAt[j2, t])
    -- Frame: leases unchanged
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
}

-- Transition: IMPORT_TERMINAL - Import a job with terminal status (Succeeded/Failed)
-- Job is created with pre-existing terminal attempts and no scheduling state.
pred importTerminal[j: Job, t: Time, tnext: Time] {
    importPreConditions[j, t]
    importNewAttempts[j, t, tnext]
    importFrameConditions[t, tnext]

    -- [SILO-IMP-5] Post: status is terminal (Succeeded or Failed)
    isTerminal[statusAt[j, tnext]]
    -- Post: at least one new attempt (terminal status requires attempt history)
    some attemptExistsAt[tnext] - attemptExistsAt[t]
    -- Post: job exists, not cancelled
    jobExistsAt[tnext] = jobExistsAt[t] + j
    not isCancelledAt[j, tnext]
    -- No task created
    all tid: TaskId | dbQueuedAt[tid, tnext] = dbQueuedAt[tid, t]
    -- Concurrency unchanged
    concurrencyUnchanged[t, tnext]
}

-- Transition: IMPORT_NON_TERMINAL - Import a job as Scheduled (no concurrency)
-- Job is created with optional terminal attempts and a task in DB queue.
pred importNonTerminal[tid: TaskId, j: Job, t: Time, tnext: Time] {
    importPreConditions[j, t]
    importNewAttempts[j, t, tnext]
    importFrameConditions[t, tnext]

    -- Pre: task is not already in use
    no dbQueuedAt[tid, t]
    no bufferedAt[tid, t]
    no leaseAt[tid, t]

    -- Pre: Job does NOT require any concurrency queues
    no jobQueues[j]

    -- [SILO-IMP-6] Post: status = Scheduled
    statusAt[j, tnext] = Scheduled
    -- Post: job exists, not cancelled
    jobExistsAt[tnext] = jobExistsAt[t] + j
    not isCancelledAt[j, tnext]
    -- [SILO-IMP-7] Post: task created in DB queue
    one qt: DbQueuedTask | qt.db_qtask = tid and qt.db_qjob = j and qt.db_qtime = tnext
    all tid2: TaskId | tid2 != tid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    -- Concurrency unchanged
    concurrencyUnchanged[t, tnext]
}

-- Transition: IMPORT_NON_TERMINAL_CONCURRENCY_GRANTED - Import job, granted concurrency immediately
pred importNonTerminalConcurrencyGranted[tid: TaskId, j: Job, q: Queue, t: Time, tnext: Time] {
    importPreConditions[j, t]
    importNewAttempts[j, t, tnext]
    importFrameConditions[t, tnext]

    -- Pre: task is not already in use
    no dbQueuedAt[tid, t]
    no bufferedAt[tid, t]
    no leaseAt[tid, t]

    -- Pre: Job requires this queue
    q in jobQueues[j]

    -- [SILO-IMP-CONC-1] Pre: Queue has capacity
    queueHasCapacity[q, t]

    -- Post: status = Scheduled, job exists, not cancelled
    statusAt[j, tnext] = Scheduled
    jobExistsAt[tnext] = jobExistsAt[t] + j
    not isCancelledAt[j, tnext]
    -- [SILO-IMP-CONC-2] Post: holder + task in DB queue
    holdersAt[q, tnext] = tid
    one qt: DbQueuedTask | qt.db_qtask = tid and qt.db_qjob = j and qt.db_qtime = tnext
    all tid2: TaskId | tid2 != tid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    -- Frame: other holders unchanged, requests unchanged
    all q2: Queue | q2 != q implies holdersAt[q2, tnext] = holdersAt[q2, t]
    requestsUnchanged[t, tnext]
}

-- Transition: IMPORT_NON_TERMINAL_CONCURRENCY_QUEUED - Import job, must wait for concurrency
pred importNonTerminalConcurrencyQueued[tid: TaskId, j: Job, q: Queue, t: Time, tnext: Time] {
    importPreConditions[j, t]
    importNewAttempts[j, t, tnext]
    importFrameConditions[t, tnext]

    -- Pre: task is not already in use
    no dbQueuedAt[tid, t]
    no bufferedAt[tid, t]
    no leaseAt[tid, t]

    -- Pre: Job requires this queue
    q in jobQueues[j]

    -- [SILO-IMP-CONC-3] Pre: Queue at capacity
    not queueHasCapacity[q, t]

    -- Post: status = Scheduled, job exists, not cancelled
    statusAt[j, tnext] = Scheduled
    jobExistsAt[tnext] = jobExistsAt[t] + j
    not isCancelledAt[j, tnext]
    -- [SILO-IMP-CONC-4] Post: request created (no task in DB)
    one r: TicketRequest | r.tr_job = j and r.tr_queue = q and r.tr_task = tid and r.tr_time = tnext
    no qt: DbQueuedTask | qt.db_qtask = tid and qt.db_qtime = tnext
    all tid2: TaskId | dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    -- Frame: requests in queue q = old + new request, other queues unchanged
    requestTasksAt[q, tnext] = requestTasksAt[q, t] + tid
    all q2: Queue | q2 != q implies requestTasksAt[q2, tnext] = requestTasksAt[q2, t]
    holdersUnchanged[t, tnext]
}

/** Reimport preconditions: job exists, not running, not succeeded, all attempts terminal */
pred reimportPreConditions[j: Job, t: Time] {
    -- [SILO-REIMP-1] Pre: job exists
    j in jobExistsAt[t]
    -- [SILO-REIMP-2] Pre: no active lease for this job (not running)
    no tid: TaskId | leaseJobAt[tid, t] = j
    -- [SILO-REIMP-3] Pre: all existing attempts for this job are terminal
    all a: attemptExistsAt[t] | attemptJob[a] = j implies isTerminalAttempt[attemptStatusAt[a, t]]
    -- [SILO-REIMP-4] Pre: job is NOT Succeeded
    statusAt[j, t] != Succeeded
}

/** Reimport cleanup: remove buffer entries for the job */
pred reimportCleanup[j: Job, t: Time, tnext: Time] {
    -- [SILO-REIMP-6] Post: remove any BufferedTask for this job
    no tid: TaskId | bufferedAt[tid, tnext] = j
    -- Frame: buffer entries for other jobs unchanged
    all tid: TaskId | bufferedAt[tid, t] != j implies bufferedAt[tid, tnext] = bufferedAt[tid, t]
}

/** Reimport frame conditions: job existence, other jobs, leases, cancellation preserved */
pred reimportFrameConditions[j: Job, t: Time, tnext: Time] {
    -- Job existence unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    -- Other jobs' status and cancellation unchanged
    all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
    all j2: Job | j2 != j implies (isCancelledAt[j2, tnext] iff isCancelledAt[j2, t])
    -- All leases unchanged (reimport doesn't touch leases)
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    -- Cancellation for this job preserved
    isCancelledAt[j, tnext] iff isCancelledAt[j, t]
}

-- Transition: REIMPORT_TERMINAL - Reimport job to terminal status
-- Removes all scheduling state and concurrency state for the job.
pred reimportTerminal[j: Job, t: Time, tnext: Time] {
    reimportPreConditions[j, t]
    importNewAttempts[j, t, tnext]
    reimportCleanup[j, t, tnext]
    reimportFrameConditions[j, t, tnext]

    -- [SILO-REIMP-7] Post: status is terminal
    isTerminal[statusAt[j, tnext]]
    -- Post: at least one new attempt
    some attemptExistsAt[tnext] - attemptExistsAt[t]

    -- [SILO-REIMP-5] Post: remove any DbQueuedTask for this job, preserve others
    all tid: TaskId | {
        (dbQueuedAt[tid, t] = j) implies no dbQueuedAt[tid, tnext]
        (dbQueuedAt[tid, t] != j) implies dbQueuedAt[tid, tnext] = dbQueuedAt[tid, t]
    }

    -- [SILO-REIMP-CONC-5] Old Scheduled state performs targeted request cleanup.
    (statusAt[j, t] = Scheduled) implies {
        all q: Queue, tid: TaskId |
            tid in requestTasksAt[q, tnext] iff (tid in requestTasksAt[q, t] and
                no r: TicketRequest | r.tr_job = j and r.tr_queue = q and r.tr_task = tid and r.tr_time = t)
        -- Remove holders for this job's old scheduled tasks, preserve others.
        all q: Queue, tid: TaskId |
            tid in holdersAt[q, tnext] iff (tid in holdersAt[q, t] and dbQueuedAt[tid, t] != j)
    }
    -- [SILO-REIMP-CONC-6] Non-scheduled old state leaves requests untouched; grant scanner drops stale keys lazily.
    (statusAt[j, t] != Scheduled) implies {
        all q: Queue | requestTasksAt[q, tnext] = requestTasksAt[q, t]
        all q: Queue | holdersAt[q, tnext] = holdersAt[q, t]
    }
}

-- Transition: REIMPORT_NON_TERMINAL - Reimport job as Scheduled (no concurrency)
pred reimportNonTerminal[newTid: TaskId, j: Job, t: Time, tnext: Time] {
    reimportPreConditions[j, t]
    importNewAttempts[j, t, tnext]
    reimportCleanup[j, t, tnext]
    reimportFrameConditions[j, t, tnext]

    -- Pre: new task is not already in use
    no dbQueuedAt[newTid, t]
    no bufferedAt[newTid, t]
    no leaseAt[newTid, t]

    -- Pre: Job does NOT require any concurrency queues
    no jobQueues[j]

    -- [SILO-REIMP-8] Post: status = Scheduled
    statusAt[j, tnext] = Scheduled

    -- [SILO-REIMP-9] Post: new task in DB queue, old tasks for j removed
    one qt: DbQueuedTask | qt.db_qtask = newTid and qt.db_qjob = j and qt.db_qtime = tnext
    all tid: TaskId | tid != newTid implies {
        (dbQueuedAt[tid, t] = j) implies no dbQueuedAt[tid, tnext]
        (dbQueuedAt[tid, t] != j) implies dbQueuedAt[tid, tnext] = dbQueuedAt[tid, t]
    }

    -- Concurrency unchanged (no concurrency requirements)
    concurrencyUnchanged[t, tnext]
}

-- Transition: REIMPORT_NON_TERMINAL_CONCURRENCY_GRANTED - Reimport with concurrency granted
pred reimportNonTerminalConcurrencyGranted[newTid: TaskId, j: Job, q: Queue, t: Time, tnext: Time] {
    reimportPreConditions[j, t]
    importNewAttempts[j, t, tnext]
    reimportCleanup[j, t, tnext]
    reimportFrameConditions[j, t, tnext]

    -- Pre: new task is not already in use
    no dbQueuedAt[newTid, t]
    no bufferedAt[newTid, t]
    no leaseAt[newTid, t]

    -- Pre: Job requires this queue
    q in jobQueues[j]

    -- [SILO-REIMP-CONC-1] Pre: queue has capacity after cleanup
    -- Either empty or holder is for this job's task (will be cleaned up)
    all tid: TaskId | tid in holdersAt[q, t] implies dbQueuedAt[tid, t] = j

    -- Post: status = Scheduled
    statusAt[j, tnext] = Scheduled

    -- [SILO-REIMP-CONC-2] Post: new task + holder, old tasks for j removed
    one qt: DbQueuedTask | qt.db_qtask = newTid and qt.db_qjob = j and qt.db_qtime = tnext
    all tid: TaskId | tid != newTid implies {
        (dbQueuedAt[tid, t] = j) implies no dbQueuedAt[tid, tnext]
        (dbQueuedAt[tid, t] != j) implies dbQueuedAt[tid, tnext] = dbQueuedAt[tid, t]
    }

    -- Post: new holder for newTid in queue q
    holdersAt[q, tnext] = newTid

    -- [SILO-REIMP-CONC-5] Scheduled old state removes old request/holder records for this job.
    (statusAt[j, t] = Scheduled) implies {
        all q2: Queue | q2 != q implies {
            all tid: TaskId |
                tid in holdersAt[q2, tnext] iff (tid in holdersAt[q2, t] and dbQueuedAt[tid, t] != j)
        }
        all q2: Queue, tid: TaskId |
            tid in requestTasksAt[q2, tnext] iff (tid in requestTasksAt[q2, t] and
                no r: TicketRequest | r.tr_job = j and r.tr_queue = q2 and r.tr_task = tid and r.tr_time = t)
    }
    -- [SILO-REIMP-CONC-6] Non-scheduled old state preserves pending requests.
    (statusAt[j, t] != Scheduled) implies {
        all q2: Queue | q2 != q implies holdersAt[q2, tnext] = holdersAt[q2, t]
        all q2: Queue | requestTasksAt[q2, tnext] = requestTasksAt[q2, t]
    }
}

-- Transition: REIMPORT_NON_TERMINAL_CONCURRENCY_QUEUED - Reimport with concurrency queued
pred reimportNonTerminalConcurrencyQueued[newTid: TaskId, j: Job, q: Queue, t: Time, tnext: Time] {
    reimportPreConditions[j, t]
    importNewAttempts[j, t, tnext]
    reimportCleanup[j, t, tnext]
    reimportFrameConditions[j, t, tnext]

    -- Pre: new task is not already in use
    no dbQueuedAt[newTid, t]
    no bufferedAt[newTid, t]
    no leaseAt[newTid, t]

    -- Pre: Job requires this queue
    q in jobQueues[j]

    -- [SILO-REIMP-CONC-3] Pre: queue at capacity (holder is for a non-j task)
    some tid: TaskId | tid in holdersAt[q, t] and dbQueuedAt[tid, t] != j

    -- Post: status = Scheduled
    statusAt[j, tnext] = Scheduled

    -- [SILO-REIMP-5] Post: remove old DB queue tasks for this job, no new DB task
    -- [SILO-REIMP-CONC-4] Post: NO task in DB queue for new task (must wait for grant)
    all tid: TaskId | {
        (dbQueuedAt[tid, t] = j) implies no dbQueuedAt[tid, tnext]
        (dbQueuedAt[tid, t] != j) implies dbQueuedAt[tid, tnext] = dbQueuedAt[tid, t]
    }

    -- Post: create new request for newTid.
    one r: TicketRequest | r.tr_job = j and r.tr_queue = q and r.tr_task = newTid and r.tr_time = tnext
    -- [SILO-REIMP-CONC-5] Scheduled old state removes old requests for this job.
    (statusAt[j, t] = Scheduled) implies {
        all q2: Queue, tid: TaskId | tid != newTid implies {
            tid in requestTasksAt[q2, tnext] iff (tid in requestTasksAt[q2, t] and
                no r: TicketRequest | r.tr_job = j and r.tr_queue = q2 and r.tr_task = tid and r.tr_time = t)
        }
    }
    -- [SILO-REIMP-CONC-6] Non-scheduled old state preserves old requests; stale ones are dropped lazily.
    (statusAt[j, t] != Scheduled) implies {
        all q2: Queue, tid: TaskId | tid != newTid implies {
            tid in requestTasksAt[q2, tnext] iff tid in requestTasksAt[q2, t]
        }
    }
    all q2: Queue | q2 != q implies newTid not in requestTasksAt[q2, tnext]
    -- [SILO-REIMP-CONC-5] Scheduled old state removes holders for old scheduled tasks.
    (statusAt[j, t] = Scheduled) implies {
        all q2: Queue, tid: TaskId |
            tid in holdersAt[q2, tnext] iff (tid in holdersAt[q2, t] and dbQueuedAt[tid, t] != j)
    }
    -- [SILO-REIMP-CONC-6] Non-scheduled old state preserves holder state.
    (statusAt[j, t] != Scheduled) implies {
        all q2: Queue | holdersAt[q2, tnext] = holdersAt[q2, t]
    }
}

-- System Trace
pred step[t: Time, tnext: Time] {
    -- Job lifecycle (no concurrency)
    (some tid: TaskId, j: Job | enqueue[tid, j, t, tnext])
    or (brokerScan[t, tnext])
    or (some tid: TaskId, w: Worker, a: Attempt | dequeue[tid, w, a, t, tnext])
    or (some tid: TaskId, w: Worker | heartbeat[tid, w, t, tnext])
    or (some tid: TaskId | reapExpiredLease[tid, t, tnext])
    or (some tid: TaskId, w: Worker | completeSuccess[tid, w, t, tnext])
    or (some tid: TaskId, w: Worker | completeFailurePermanent[tid, w, t, tnext])
    or (some tid: TaskId, w: Worker, newTid: TaskId | completeFailureRetry[tid, w, newTid, t, tnext])
    or (some j: Job | cancelJob[j, t, tnext])
    or (some j: Job, newTid: TaskId | restartCancelledJob[j, newTid, t, tnext])
    or (some j: Job, newTid: TaskId | restartFailedJob[j, newTid, t, tnext])
    or (some tid: TaskId, j: Job | expediteNextAttempt[tid, j, t, tnext])
    -- Concurrency ticket management
    or (some tid: TaskId, j: Job, q: Queue | enqueueWithConcurrencyGranted[tid, j, q, t, tnext])
    or (some tid: TaskId, j: Job, q: Queue | enqueueWithConcurrencyQueued[tid, j, q, t, tnext])
    or (some q: Queue, tid: TaskId | grantNextRequest[q, tid, t, tnext])
    or (some q: Queue, tid: TaskId | dropStaleRequest[q, tid, t, tnext])
    or (some tid: TaskId, w: Worker, q: Queue | completeSuccessReleaseTicket[tid, w, q, t, tnext])
    or (some tid: TaskId, w: Worker, q: Queue | completeFailurePermanentReleaseTicket[tid, w, q, t, tnext])
    or (some tid: TaskId, w: Worker, q: Queue, newTid: TaskId | completeFailureRetryReleaseTicket[tid, w, q, newTid, t, tnext])
    or (some tid: TaskId, q: Queue | reapExpiredLeaseReleaseTicket[tid, q, t, tnext])
    -- Import transitions
    or (some j: Job | importTerminal[j, t, tnext])
    or (some tid: TaskId, j: Job | importNonTerminal[tid, j, t, tnext])
    or (some tid: TaskId, j: Job, q: Queue | importNonTerminalConcurrencyGranted[tid, j, q, t, tnext])
    or (some tid: TaskId, j: Job, q: Queue | importNonTerminalConcurrencyQueued[tid, j, q, t, tnext])
    -- Reimport transitions
    or (some j: Job | reimportTerminal[j, t, tnext])
    or (some newTid: TaskId, j: Job | reimportNonTerminal[newTid, j, t, tnext])
    or (some newTid: TaskId, j: Job, q: Queue | reimportNonTerminalConcurrencyGranted[newTid, j, q, t, tnext])
    or (some newTid: TaskId, j: Job, q: Queue | reimportNonTerminalConcurrencyQueued[newTid, j, q, t, tnext])
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
        -- Scheduled can go to Running (dequeue), stay Scheduled (stutter/reimport),
        -- or go to Succeeded/Failed (reimport with terminal attempts)
        statusAt[j, t] = Scheduled implies statusAt[j, t.next] in (Scheduled + Running + Succeeded + Failed)
        statusAt[j, t] = Running implies statusAt[j, t.next] in (Running + Succeeded + Failed + Scheduled)
        -- Succeeded is truly terminal (cannot be restarted or reimported)
        statusAt[j, t] = Succeeded implies statusAt[j, t.next] = Succeeded
        -- Failed can transition to Scheduled (restart) or Succeeded/Failed (reimport)
        statusAt[j, t] = Failed implies statusAt[j, t.next] in (Failed + Scheduled + Succeeded)
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
    all t: Time, j: Job | (j in jobExistsAt[t] and isTerminal[statusAt[j, t]]) implies 
        no qt: DbQueuedTask | qt.db_qjob = j and qt.db_qtime = t
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
 * - Job cancelled with task in queue: [SILO-CXL-3] eagerly removes holders
 * See: [SILO-ENQ-CONC-2], [SILO-GRANT-3], [SILO-REL-1], [SILO-CXL-3]
 */
assert holdersRequireActiveTask {
    all h: TicketHolder | 
        some dbQueuedAt[h.th_task, h.th_time] or 
        some bufferedAt[h.th_task, h.th_time] or 
        some leaseAt[h.th_task, h.th_time]
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

/**
 * Expedite doesn't change job status.
 * After expediting, the job remains in the same status (Scheduled).
 * See: [SILO-EXP-2] pre: not terminal, so status must be Scheduled or Running.
 * But [SILO-EXP-6] requires no lease, which rules out Running.
 * So expedited jobs must be Scheduled before and after.
 */
assert expeditePreservesStatus {
    all t: Time - last, tid: TaskId, j: Job |
        expediteNextAttempt[tid, j, t, t.next] implies
            statusAt[j, t.next] = statusAt[j, t]
}

/**
 * Expedite only works on non-terminal jobs.
 * Terminal jobs (Succeeded/Failed) cannot be expedited.
 * See: [SILO-EXP-2]
 */
assert expediteRejectsTerminal {
    all t: Time, tid: TaskId, j: Job |
        (j in jobExistsAt[t] and isTerminal[statusAt[j, t]]) implies
            not expediteNextAttempt[tid, j, t, t.next]
}

/**
 * Expedite only works on non-cancelled jobs.
 * Cancelled jobs cannot be expedited - must restart first.
 * See: [SILO-EXP-3]
 */
assert expediteRejectsCancelled {
    all t: Time, tid: TaskId, j: Job |
        (j in jobExistsAt[t] and isCancelledAt[j, t]) implies
            not expediteNextAttempt[tid, j, t, t.next]
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
 * Job is cancelled while task is in buffer. Cancel removes from DB queue
 * but buffer eviction is best-effort, so a stale copy may remain.
 */
pred exampleStaleBuffer {
    some tid: TaskId, j: Job, t: Time | {
        bufferedAt[tid, t] = j
        isCancelledAt[j, t]
    }
}

/**
 * Scenario: Cancellation with eager cleanup
 * Job is cancelled, tasks are immediately removed from DB queue.
 * No dequeue cleanup step needed.
 */
pred exampleCancellationEagerCleanup {
    some tid: TaskId, j: Job, t1, t2: Time | {
        lt[t1, t2]
        -- t1: job scheduled, task in DB queue, NOT cancelled
        j in jobExistsAt[t1]
        statusAt[j, t1] = Scheduled
        not isCancelledAt[j, t1]
        some dbQueuedAt[tid, t1]
        dbQueuedAt[tid, t1] = j
        -- t2: job cancelled, task eagerly removed from DB queue
        isCancelledAt[j, t2]
        no dbQueuedAt[tid, t2]    -- Eagerly cleaned up by cancelJob
        -- Note: buffer may or may not still have a stale copy (evict_keys is best-effort)
        no l: Lease | l.ljob = j and l.ltime = t2  -- No lease created
        statusAt[j, t2] = Scheduled  -- Status never changed to Running
    }
}

/**
 * Scenario: Cancelled job has no tasks to dequeue
 * After cancelJob eagerly removes tasks, there is nothing left for dequeue.
 * Job stays Scheduled (in Alloy terms) with cancellation flag set.
 */
pred exampleCancelledJobHasNoTasks {
    some j: Job, t: Time | {
        -- Job is cancelled, no tasks anywhere
        j in jobExistsAt[t]
        isCancelledAt[j, t]
        statusAt[j, t] = Scheduled
        no tid: TaskId | dbQueuedAt[tid, t] = j
        no tid: TaskId | bufferedAt[tid, t] = j
        no tid: TaskId | leaseJobAt[tid, t] = j
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
 * Scenario: Cancelled job's request is eagerly cleaned up at cancel time
 * Job is cancelled while waiting in request queue. The request is immediately
 * deleted by cancelJob (not left for grant_next to clean up).
 * See: job_store_shard/cancel.rs [SILO-CXL-3]
 */
pred exampleCancelledRequestCleanup {
    some tid1: TaskId, j1, j2: Job, q: Queue, t1, t2: Time | {
        lt[t1, t2]
        j1 != j2
        -- t1: j1 holds the queue, j2 is waiting as a request, NOT cancelled
        some h: TicketHolder | h.th_task = tid1 and h.th_queue = q and h.th_time = t1
        some r: TicketRequest | r.tr_job = j2 and r.tr_queue = q and r.tr_time = t1
        not isCancelledAt[j2, t1]
        -- t2: j2 is cancelled, request eagerly removed
        isCancelledAt[j2, t2]
        no r: TicketRequest | r.tr_job = j2 and r.tr_queue = q and r.tr_time = t2  -- Request eagerly removed
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
 * Job is enqueued, cancelled (task eagerly removed), then restarted
 * and successfully completes.
 */
pred exampleRestartCancelledJob {
    some tid1, tid2: TaskId, j: Job, t1, t2, t3, t4: Time | {
        lt[t1, t2] and lt[t2, t3] and lt[t3, t4]
        tid1 != tid2
        -- t1: job enqueued and scheduled, NOT cancelled
        j in jobExistsAt[t1]
        statusAt[j, t1] = Scheduled
        not isCancelledAt[j, t1]
        some dbQueuedAt[tid1, t1]
        dbQueuedAt[tid1, t1] = j
        -- t2: job cancelled, task eagerly removed (no cleanup step needed)
        isCancelledAt[j, t2]
        statusAt[j, t2] = Scheduled
        no tid: TaskId | dbQueuedAt[tid, t2] = j  -- Task eagerly removed
        no tid: TaskId | bufferedAt[tid, t2] = j
        no tid: TaskId | leaseJobAt[tid, t2] = j
        -- t3: job restarted, cancellation cleared, new task created
        not isCancelledAt[j, t3]
        statusAt[j, t3] = Scheduled
        some dbQueuedAt[tid2, t3]
        dbQueuedAt[tid2, t3] = j
        -- t4: job completes successfully
        statusAt[j, t4] = Succeeded
        not isCancelledAt[j, t4]
    }
}

/**
 * Scenario: Running job cancelled, worker completes with failure, then restarted and completes
 * Job starts running, is cancelled mid-execution, worker discovers cancellation on heartbeat
 * and completes with permanent failure. Then the job is restarted and completes successfully.
 *
 * This demonstrates:
 * 1. Cancellation during execution - worker discovers on heartbeat
 * 2. Worker completes with failure (acknowledging cancellation)
 * 3. Cancelled+Failed job can be restarted (restartFailedJob)
 * 4. Restarted job runs fresh and completes normally
 *
 * Note: With eager cancel cleanup, retry tasks for cancelled jobs are dequeued normally
 * (no cancelled check) and the worker discovers cancellation via heartbeat. This scenario
 * shows the simpler failure path instead.
 */
pred exampleCancelWhileRunningThenRestartAndComplete {
    some tid1, tid3: TaskId, j: Job, a1, a2: Attempt, t1, t2, t3, t4, t5, t6, t7: Time | {
        lt[t1, t2] and lt[t2, t3] and lt[t3, t4] and lt[t4, t5] and lt[t5, t6] and lt[t6, t7]
        tid1 != tid3
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

        -- t3: worker completes with permanent failure (acknowledging cancellation)
        isCancelledAt[j, t3]  -- still cancelled
        statusAt[j, t3] = Failed  -- permanent failure
        no leaseAt[tid1, t3]  -- lease released
        attemptStatusAt[a1, t3] = AttemptFailed
        -- No active tasks
        no tid: TaskId | dbQueuedAt[tid, t3] = j
        no tid: TaskId | bufferedAt[tid, t3] = j
        no tid: TaskId | leaseJobAt[tid, t3] = j

        -- t4: job restarted via restartFailedJob (cancellation cleared, new task created)
        not isCancelledAt[j, t4]  -- cancellation cleared
        statusAt[j, t4] = Scheduled
        some dbQueuedAt[tid3, t4]
        dbQueuedAt[tid3, t4] = j

        -- t5: broker scan - task enters buffer
        some bufferedAt[tid3, t5]

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
 * Scenario: Future-scheduled job is expedited to run now
 * Job is enqueued with future start time (task in DB queue, not in buffer),
 * then expedited to run immediately. After expedite, the task is picked up
 * by the broker and completes successfully.
 * 
 * This demonstrates:
 * 1. Task starts in DB queue but not in buffer (future-scheduled)
 * 2. Expedite is called, task remains in DB queue (now at current time)
 * 3. Broker scans and moves task to buffer
 * 4. Worker dequeues and completes successfully
 */
pred exampleExpediteJob {
    some tid: TaskId, j: Job, a: Attempt, t1, t2, t3, t4, t5: Time | {
        lt[t1, t2] and lt[t2, t3] and lt[t3, t4] and lt[t4, t5]
        attemptJob[a] = j
        
        -- t1: job enqueued with future start time
        -- Task is in DB queue but NOT in buffer (future-scheduled)
        j in jobExistsAt[t1]
        statusAt[j, t1] = Scheduled
        not isCancelledAt[j, t1]
        dbQueuedAt[tid, t1] = j
        no bufferedAt[tid, t1]  -- Not in buffer = future-scheduled
        no l: Lease | l.ljob = j and l.ltime = t1
        
        -- t2: expedite is called
        -- Task remains in DB queue (conceptually now at current time)
        j in jobExistsAt[t2]
        statusAt[j, t2] = Scheduled  -- Status unchanged
        not isCancelledAt[j, t2]
        dbQueuedAt[tid, t2] = j  -- Still in DB queue
        
        -- t3: broker scans, task now in buffer (expedited, so now "ready")
        j in jobExistsAt[t3]
        statusAt[j, t3] = Scheduled
        bufferedAt[tid, t3] = j  -- Now in buffer
        
        -- t4: worker dequeues, job is running
        j in jobExistsAt[t4]
        statusAt[j, t4] = Running
        some l: Lease | l.ljob = j and l.lattempt = a and l.ltime = t4
        attemptStatusAt[a, t4] = AttemptRunning
        
        -- t5: job completes successfully
        j in jobExistsAt[t5]
        statusAt[j, t5] = Succeeded
        attemptStatusAt[a, t5] = AttemptSucceeded
        no l: Lease | l.ljob = j and l.ltime = t5
    }
}

/**
 * Scenario: Expedite is rejected for running job (race safety)
 * Job starts running (has lease), expedite cannot be called.
 * This shows the race-safety mechanism - expedite preconditions reject
 * jobs that are already running.
 */
pred exampleExpediteRejectedForRunningJob {
    some tid: TaskId, j: Job, t: Time | {
        -- Job is running with a lease
        j in jobExistsAt[t]
        statusAt[j, t] = Running
        some tid2: TaskId | leaseJobAt[tid2, t] = j
        -- No task in DB queue for this job (it's been dequeued)
        no dbQueuedAt[tid, t]
        -- Therefore expedite preconditions cannot be satisfied
        -- (need task in DB queue AND no lease - both fail)
    }
}

/**
 * Scenario: Expedite mid-retry job (skip retry backoff)
 * Job fails with retry, new task is scheduled for future (retry backoff),
 * operator expedites to skip the backoff and retry immediately.
 */
pred exampleExpediteMidRetry {
    some tid1, tid2: TaskId, j: Job, a1, a2: Attempt, t1, t2, t3, t4, t5, t6: Time | {
        lt[t1, t2] and lt[t2, t3] and lt[t3, t4] and lt[t4, t5] and lt[t5, t6]
        tid1 != tid2 and a1 != a2
        attemptJob[a1] = j and attemptJob[a2] = j
        
        -- t1: first attempt is running
        j in jobExistsAt[t1]
        statusAt[j, t1] = Running
        some leaseAt[tid1, t1]
        leaseJobAt[tid1, t1] = j
        leaseAttemptAt[tid1, t1] = a1
        
        -- t2: attempt fails with retry, new task created (future-scheduled for backoff)
        statusAt[j, t2] = Scheduled  -- completeFailureRetry sets Scheduled
        no leaseAt[tid1, t2]
        attemptStatusAt[a1, t2] = AttemptFailed
        dbQueuedAt[tid2, t2] = j  -- Retry task in DB queue
        no bufferedAt[tid2, t2]  -- Not in buffer yet (future = backoff delay)
        
        -- t3: expedite is called to skip backoff
        statusAt[j, t3] = Scheduled
        dbQueuedAt[tid2, t3] = j  -- Still in DB queue after expedite
        
        -- t4: broker scans, retry task now in buffer
        bufferedAt[tid2, t4] = j
        
        -- t5: second attempt running
        statusAt[j, t5] = Running
        some leaseAt[tid2, t5]
        leaseJobAt[tid2, t5] = j
        leaseAttemptAt[tid2, t5] = a2
        attemptStatusAt[a2, t5] = AttemptRunning
        
        -- t6: second attempt succeeds
        statusAt[j, t6] = Succeeded
        no leaseAt[tid2, t6]
        attemptStatusAt[a2, t6] = AttemptSucceeded
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

/**
 * Scenario: Job with concurrency retries, releasing ticket so another job can run in between
 * 
 * This is the key scenario for verifying that:
 * 1. When a job with concurrency requirements fails and schedules a retry, its ticket is released
 * 2. Another job can acquire the ticket and run during the retry backoff period
 * 3. When the first job's retry runs, it re-acquires the ticket
 * 
 * Timeline:
 * - t1: Job A is running with concurrency ticket for queue Q
 * - t2: Job A fails with retry, releases ticket, creates request for retry task
 * - t3: Job B (waiting for queue Q) is granted the ticket and starts running
 * - t4: Job B completes successfully, releases ticket
 * - t5: Job A's retry is granted the ticket
 * - t6: Job A's retry runs
 * - t7: Job A's retry succeeds
 */
pred exampleRetryReleasesTicketForOtherJob {
    some tidA1, tidA2, tidB: TaskId, jA, jB: Job, aA1, aA2, aB: Attempt, q: Queue,
         t1, t2, t3, t4, t5, t6, t7: Time | {
        lt[t1, t2] and lt[t2, t3] and lt[t3, t4] and lt[t4, t5] and lt[t5, t6] and lt[t6, t7]
        tidA1 != tidA2 and tidA1 != tidB and tidA2 != tidB
        jA != jB
        aA1 != aA2 and aA1 != aB and aA2 != aB
        attemptJob[aA1] = jA
        attemptJob[aA2] = jA
        attemptJob[aB] = jB
        
        -- Both jobs require the same queue
        q in jobQueues[jA]
        q in jobQueues[jB]
        
        -- t1: Job A is running with the concurrency ticket
        jA in jobExistsAt[t1]
        jB in jobExistsAt[t1]
        statusAt[jA, t1] = Running
        some leaseAt[tidA1, t1]
        leaseJobAt[tidA1, t1] = jA
        leaseAttemptAt[tidA1, t1] = aA1
        attemptStatusAt[aA1, t1] = AttemptRunning
        -- Job A holds the ticket
        some h: TicketHolder | h.th_task = tidA1 and h.th_queue = q and h.th_time = t1
        -- Job B is waiting for the ticket (has a request)
        some r: TicketRequest | r.tr_job = jB and r.tr_queue = q and r.tr_task = tidB and r.tr_time = t1
        
        -- t2: Job A fails with retry, releases ticket, creates request for retry
        statusAt[jA, t2] = Scheduled  -- completeFailureRetryReleaseTicket sets Scheduled
        no leaseAt[tidA1, t2]  -- lease released
        attemptStatusAt[aA1, t2] = AttemptFailed
        -- Job A's ticket is released (no holder for tidA1)
        no h: TicketHolder | h.th_task = tidA1 and h.th_queue = q and h.th_time = t2
        -- Job A has a request for the retry task
        some r: TicketRequest | r.tr_job = jA and r.tr_queue = q and r.tr_task = tidA2 and r.tr_time = t2
        
        -- t3: Job B is granted the ticket (since queue now has capacity) and runs
        statusAt[jB, t3] = Running
        some leaseAt[tidB, t3]
        leaseJobAt[tidB, t3] = jB
        leaseAttemptAt[tidB, t3] = aB
        attemptStatusAt[aB, t3] = AttemptRunning
        -- Job B now holds the ticket
        some h: TicketHolder | h.th_task = tidB and h.th_queue = q and h.th_time = t3
        -- Job A is still waiting (request exists)
        some r: TicketRequest | r.tr_job = jA and r.tr_queue = q and r.tr_task = tidA2 and r.tr_time = t3
        
        -- t4: Job B completes successfully, releases ticket
        statusAt[jB, t4] = Succeeded
        no leaseAt[tidB, t4]
        attemptStatusAt[aB, t4] = AttemptSucceeded
        -- Job B's ticket is released
        no h: TicketHolder | h.th_task = tidB and h.th_queue = q and h.th_time = t4
        
        -- t5: Job A's retry is granted the ticket (grantNextRequest)
        some h: TicketHolder | h.th_task = tidA2 and h.th_queue = q and h.th_time = t5
        some dbQueuedAt[tidA2, t5]  -- grantNextRequest creates DB task
        dbQueuedAt[tidA2, t5] = jA
        -- No more request for Job A
        no r: TicketRequest | r.tr_job = jA and r.tr_queue = q and r.tr_time = t5
        
        -- t6: Job A's retry runs (dequeued)
        statusAt[jA, t6] = Running
        some leaseAt[tidA2, t6]
        leaseJobAt[tidA2, t6] = jA
        leaseAttemptAt[tidA2, t6] = aA2
        attemptStatusAt[aA2, t6] = AttemptRunning
        
        -- t7: Job A's retry succeeds
        statusAt[jA, t7] = Succeeded
        no leaseAt[tidA2, t7]
        attemptStatusAt[aA2, t7] = AttemptSucceeded
        -- All attempts exist
        aA1 in attemptExistsAt[t7]
        aA2 in attemptExistsAt[t7]
        aB in attemptExistsAt[t7]
    }
}

-- ========== IMPORT/REIMPORT EXAMPLES ==========

/**
 * Scenario: Import a terminal (succeeded) job
 * Job is imported directly as Succeeded with a succeeded attempt.
 * Job was never Running.
 */
pred exampleImportSucceeded {
    some j: Job | {
        j in jobExistsAt[last]
        statusAt[j, last] = Succeeded
        -- Job was never Running
        all t: Time | j in jobExistsAt[t] implies statusAt[j, t] != Running
        -- Has a succeeded attempt
        some a: attemptExistsAt[last] | attemptJob[a] = j and attemptStatusAt[a, last] = AttemptSucceeded
    }
}

/**
 * Scenario: Import job with failed attempt (retries remain), then run and succeed
 * Job is imported as Scheduled with a pre-existing failed attempt,
 * then goes through normal dequeue/complete cycle and succeeds.
 */
pred exampleImportNonTerminalThenSucceeds {
    some j: Job, a1, a2: Attempt, t1: Time | {
        a1 != a2
        attemptJob[a1] = j
        attemptJob[a2] = j
        -- Job is Scheduled with a failed attempt (from import)
        statusAt[j, t1] = Scheduled
        a1 in attemptExistsAt[t1]
        attemptStatusAt[a1, t1] = AttemptFailed
        -- Job eventually succeeds
        statusAt[j, last] = Succeeded
        attemptStatusAt[a2, last] = AttemptSucceeded
    }
}

/**
 * Scenario: Import job with concurrency into full queue
 * Job is imported with concurrency requirement into a queue at capacity.
 * A request is created. When capacity frees, job is granted and runs to success.
 */
pred exampleImportWithConcurrencyQueued {
    some tid1, tid2: TaskId, j1, j2: Job, q: Queue, t1, t2, t3: Time | {
        lt[t1, t2] and lt[t2, t3]
        j1 != j2
        tid1 != tid2
        -- t1: j1 holds the queue, j2 is imported with concurrency queued (has request)
        some h: TicketHolder | h.th_queue = q and h.th_time = t1
        j2 in jobExistsAt[t1]
        some r: TicketRequest | r.tr_job = j2 and r.tr_queue = q and r.tr_time = t1
        -- t2: j2 granted and running
        some h: TicketHolder | h.th_task = tid2 and h.th_queue = q and h.th_time = t2
        statusAt[j2, t2] = Running
        -- t3: j2 succeeds
        statusAt[j2, t3] = Succeeded
    }
}

/**
 * Scenario: Import job with 1 failed attempt (Scheduled), then reimport with additional failed attempt
 * After reimport, job is still Scheduled with 2 failed attempts and a new task.
 */
pred exampleReimportAddAttempts {
    some j: Job, a1, a2: Attempt, t1, t2: Time | {
        lt[t1, t2]
        a1 != a2
        attemptJob[a1] = j
        attemptJob[a2] = j
        -- t1: job has 1 failed attempt, Scheduled
        statusAt[j, t1] = Scheduled
        a1 in attemptExistsAt[t1]
        attemptStatusAt[a1, t1] = AttemptFailed
        a2 not in attemptExistsAt[t1]
        -- t2: reimported with additional failed attempt, still Scheduled
        statusAt[j, t2] = Scheduled
        a1 in attemptExistsAt[t2]
        a2 in attemptExistsAt[t2]
        attemptStatusAt[a1, t2] = AttemptFailed
        attemptStatusAt[a2, t2] = AttemptFailed
    }
}

/**
 * Scenario: Import job as Scheduled with task, then reimport with succeeded attempt
 * Old task is cleaned up, job becomes terminal Succeeded.
 */
pred exampleReimportScheduledToSucceeded {
    some j: Job, a1: Attempt, tid: TaskId, t1, t2: Time | {
        lt[t1, t2]
        attemptJob[a1] = j
        -- t1: job Scheduled with task in DB queue
        statusAt[j, t1] = Scheduled
        dbQueuedAt[tid, t1] = j
        a1 not in attemptExistsAt[t1]
        -- t2: reimported with succeeded attempt, now terminal
        statusAt[j, t2] = Succeeded
        a1 in attemptExistsAt[t2]
        attemptStatusAt[a1, t2] = AttemptSucceeded
        -- Old task cleaned up
        no dbQueuedAt[tid, t2]
    }
}

/**
 * Scenario: Import job, cancel it, reimport - cancellation preserved
 * Job is imported, cancelled, then reimported with a terminal attempt.
 * Cancellation flag is preserved through reimport.
 */
pred exampleReimportCancelledJob {
    some j: Job, a1: Attempt, t1, t2, t3: Time | {
        lt[t1, t2] and lt[t2, t3]
        attemptJob[a1] = j
        -- t1: job imported as Scheduled
        j in jobExistsAt[t1]
        statusAt[j, t1] = Scheduled
        not isCancelledAt[j, t1]
        -- t2: job cancelled
        isCancelledAt[j, t2]
        -- t3: reimported with failed attempt, cancellation preserved
        a1 in attemptExistsAt[t3]
        isTerminalAttempt[attemptStatusAt[a1, t3]]
        isCancelledAt[j, t3]
    }
}

/**
 * Scenario: Full migration cycle - import, reimport, reimport
 * Import with 0 attempts (Scheduled), reimport adding 1 failed attempt (still Scheduled),
 * reimport adding succeeded attempt (now terminal Succeeded).
 */
pred exampleImportReimportReimportThenRun {
    some j: Job, a1, a2: Attempt, t1, t2, t3: Time | {
        lt[t1, t2] and lt[t2, t3]
        a1 != a2
        attemptJob[a1] = j
        attemptJob[a2] = j
        -- t1: import with 0 attempts, Scheduled
        j in jobExistsAt[t1]
        statusAt[j, t1] = Scheduled
        no a: attemptExistsAt[t1] | attemptJob[a] = j
        -- t2: reimport with 1 failed attempt, still Scheduled
        statusAt[j, t2] = Scheduled
        a1 in attemptExistsAt[t2]
        attemptStatusAt[a1, t2] = AttemptFailed
        -- t3: reimport with additional succeeded attempt, terminal
        statusAt[j, t3] = Succeeded
        a1 in attemptExistsAt[t3]
        a2 in attemptExistsAt[t3]
        attemptStatusAt[a2, t3] = AttemptSucceeded
    }
}

/**
 * Asserts that when a job with concurrency releases its ticket on retry,
 * other jobs can acquire the ticket before the retry runs.
 * 
 * This is verified by the queueLimitEnforced and holdersRequireActiveTask assertions,
 * combined with the fact that completeFailureRetryReleaseTicket releases the holder.
 * If the holder wasn't released, another job couldn't get it (due to queueLimitEnforced).
 */
assert retryReleasesTicket {
    -- After completeFailureRetryReleaseTicket, the original task no longer holds the ticket
    all tid: TaskId, w: Worker, q: Queue, newTid: TaskId, t: Time - last |
        completeFailureRetryReleaseTicket[tid, w, q, newTid, t, t.next] implies
            no h: TicketHolder | h.th_task = tid and h.th_queue = q and h.th_time = t.next
}

-- ========== IMPORT/REIMPORT ASSERTIONS ==========

/** All attempts newly created by import are terminal */
assert importCreatesTerminalAttempts {
    all j: Job, t: Time - last |
        (importTerminal[j, t, t.next]
         or (some tid: TaskId | importNonTerminal[tid, j, t, t.next])
         or (some tid: TaskId, q: Queue | importNonTerminalConcurrencyGranted[tid, j, q, t, t.next])
         or (some tid: TaskId, q: Queue | importNonTerminalConcurrencyQueued[tid, j, q, t, t.next]))
        implies
            all a: attemptExistsAt[t.next] - attemptExistsAt[t] | isTerminalAttempt[attemptStatusAt[a, t.next]]
}

/** Existing attempt statuses are unchanged after reimport */
assert reimportPreservesExistingAttempts {
    all j: Job, t: Time - last |
        (reimportTerminal[j, t, t.next]
         or (some newTid: TaskId | reimportNonTerminal[newTid, j, t, t.next])
         or (some newTid: TaskId, q: Queue | reimportNonTerminalConcurrencyGranted[newTid, j, q, t, t.next])
         or (some newTid: TaskId, q: Queue | reimportNonTerminalConcurrencyQueued[newTid, j, q, t, t.next]))
        implies
            all a: attemptExistsAt[t] | attemptStatusAt[a, t.next] = attemptStatusAt[a, t]
}

/** Can't reimport a job with status Running (has lease) */
assert reimportRejectsRunning {
    all j: Job, t: Time |
        (j in jobExistsAt[t] and statusAt[j, t] = Running) implies (
            not reimportTerminal[j, t, t.next] and
            not (some newTid: TaskId | reimportNonTerminal[newTid, j, t, t.next]) and
            not (some newTid: TaskId, q: Queue | reimportNonTerminalConcurrencyGranted[newTid, j, q, t, t.next]) and
            not (some newTid: TaskId, q: Queue | reimportNonTerminalConcurrencyQueued[newTid, j, q, t, t.next])
        )
}

/** Can't reimport a Succeeded job */
assert reimportRejectsSucceeded {
    all j: Job, t: Time |
        (j in jobExistsAt[t] and statusAt[j, t] = Succeeded) implies (
            not reimportTerminal[j, t, t.next] and
            not (some newTid: TaskId | reimportNonTerminal[newTid, j, t, t.next]) and
            not (some newTid: TaskId, q: Queue | reimportNonTerminalConcurrencyGranted[newTid, j, q, t, t.next]) and
            not (some newTid: TaskId, q: Queue | reimportNonTerminalConcurrencyQueued[newTid, j, q, t, t.next])
        )
}

/** Cancellation flag unchanged after reimport */
assert reimportPreservesCancellation {
    all j: Job, t: Time - last |
        (reimportTerminal[j, t, t.next]
         or (some newTid: TaskId | reimportNonTerminal[newTid, j, t, t.next])
         or (some newTid: TaskId, q: Queue | reimportNonTerminalConcurrencyGranted[newTid, j, q, t, t.next])
         or (some newTid: TaskId, q: Queue | reimportNonTerminalConcurrencyQueued[newTid, j, q, t, t.next]))
        implies
            (isCancelledAt[j, t.next] iff isCancelledAt[j, t])
}

/** Import only works on non-existing jobs */
assert importRejectsExistingJobs {
    all j: Job, t: Time |
        j in jobExistsAt[t] implies (
            not importTerminal[j, t, t.next] and
            not (some tid: TaskId | importNonTerminal[tid, j, t, t.next]) and
            not (some tid: TaskId, q: Queue | importNonTerminalConcurrencyGranted[tid, j, q, t, t.next]) and
            not (some tid: TaskId, q: Queue | importNonTerminalConcurrencyQueued[tid, j, q, t, t.next])
        )
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
run exampleCancellationEagerCleanup for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 1 Attempt, 6 Time,
    6 JobState, 6 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 6 AttemptExists, 6 JobExists, 1 JobAttemptRelation, 6 JobCancelled

run exampleCancelledJobHasNoTasks for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 1 Attempt, 6 Time,
    6 JobState, 6 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 6 AttemptExists, 6 JobExists, 1 JobAttemptRelation, 6 JobCancelled
run exampleCancellationDiscoveryOnHeartbeat for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled

run exampleHeartbeat for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled

run exampleLeaseExpiry for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled

run exampleRestartCancelledJob for 3 but exactly 1 Job, 1 Worker, 3 TaskId, 2 Attempt, 10 Time,
    10 JobState, 10 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 10 AttemptExists, 10 JobExists, 2 JobAttemptRelation, 10 JobCancelled

run exampleCancelWhileRunningThenRestartAndComplete for 3 but exactly 1 Job, 1 Worker, 3 TaskId, 3 Attempt, 14 Time,
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

run exampleCancelledRequestCleanup for 4 but exactly 2 Job, 1 Worker, 3 TaskId, 3 Attempt, 8 Time, 1 Queue,
    16 JobState, 8 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled,
    2 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder

run exampleCancelledJobReleasesTicketOnComplete for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time, 1 Queue,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled,
    1 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder

run exampleLeaseExpiryReleasesTicket for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time, 1 Queue,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled,
    1 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder

run exampleExpediteJob for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 10 Time,
    10 JobState, 10 AttemptState, 5 DbQueuedTask, 5 BufferedTask, 4 Lease, 10 AttemptExists, 10 JobExists, 2 JobAttemptRelation, 10 JobCancelled

run exampleExpediteMidRetry for 3 but exactly 1 Job, 1 Worker, 3 TaskId, 3 Attempt, 12 Time,
    12 JobState, 12 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 12 AttemptExists, 12 JobExists, 3 JobAttemptRelation, 12 JobCancelled

-- Retry with concurrency release example
-- 2 Jobs, 3 TaskIds (A1, A2, B), 3 Attempts (A1, A2, B), 1 Queue, 14 Time steps
-- Need more bounds for interleaved scenario
run exampleRetryReleasesTicketForOtherJob for 5 but exactly 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 14 Time, 1 Queue,
    28 JobState, 28 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 6 Lease, 14 AttemptExists, 14 JobExists, 3 JobAttemptRelation, 28 JobCancelled,
    2 JobQueueRequirement, 14 TicketRequest, 14 TicketHolder

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

check noDoubleLease for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled
check oneLeasePerJob for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled
check leaseJobMustBeRunning for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled
check runningJobHasRunningAttempt for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled
check attemptTerminalIsForever for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled
check validTransitions for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled
check noZombieAttempts for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled
check noQueuedTasksForTerminal for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled
check noLeasesForTerminal for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled
check cancellationClearedRequiresRestartable for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 16 JobCancelled
check restartedJobIsScheduledWithTask for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 16 JobCancelled

-- Concurrency ticket assertions (with Queue, TicketRequest, TicketHolder, JobQueueRequirement bounds)
check queueLimitEnforced for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time, 2 Queue,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled,
    4 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder

check holdersRequireActiveTask for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time, 2 Queue,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled,
    4 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder

check grantedMeansNoRequest for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time, 2 Queue,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled,
    4 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder

check noHoldersForTerminal for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time, 2 Queue,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled,
    4 JobQueueRequirement, 8 TicketRequest, 8 TicketHolder

-- Expedite assertions
check expeditePreservesStatus for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled
check expediteRejectsTerminal for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled
check expediteRejectsCancelled for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled

-- Retry with concurrency release assertion
check retryReleasesTicket for 4 but 2 Job, 2 Worker, 4 TaskId, 4 Attempt, 8 Time, 2 Queue,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled,
    4 JobQueueRequirement, 12 TicketRequest, 12 TicketHolder

-- Import/Reimport examples
run exampleImportSucceeded for 3 but exactly 1 Job, 1 Worker, 1 TaskId, 1 Attempt, 4 Time,
    4 JobState, 4 AttemptState, 2 DbQueuedTask, 2 BufferedTask, 1 Lease, 4 AttemptExists, 4 JobExists, 1 JobAttemptRelation, 4 JobCancelled

run exampleImportNonTerminalThenSucceeds for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled

run exampleImportWithConcurrencyQueued for 4 but exactly 2 Job, 1 Worker, 3 TaskId, 3 Attempt, 12 Time, 1 Queue,
    24 JobState, 12 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 12 AttemptExists, 12 JobExists, 3 JobAttemptRelation, 24 JobCancelled,
    2 JobQueueRequirement, 12 TicketRequest, 12 TicketHolder

run exampleReimportAddAttempts for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 6 Time,
    6 JobState, 6 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 6 AttemptExists, 6 JobExists, 2 JobAttemptRelation, 6 JobCancelled

run exampleReimportScheduledToSucceeded for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 6 Time,
    6 JobState, 6 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 6 AttemptExists, 6 JobExists, 2 JobAttemptRelation, 6 JobCancelled

run exampleReimportCancelledJob for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled

run exampleImportReimportReimportThenRun for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 3 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 8 JobCancelled

-- Import/Reimport assertions
check importCreatesTerminalAttempts for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled

check reimportPreservesExistingAttempts for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled

check reimportRejectsRunning for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled

check reimportRejectsSucceeded for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled

check reimportPreservesCancellation for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 16 JobCancelled

check importRejectsExistingJobs for 4 but 2 Job, 2 Worker, 3 TaskId, 4 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 6 DbQueuedTask, 6 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 4 JobAttemptRelation, 12 JobCancelled
