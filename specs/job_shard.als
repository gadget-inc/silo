/**
 * Silo Job Queue Shard - Dynamic State Machine Specification
 * 
 * This Alloy model verifies the algorithms that create and assign jobs, tasks, and leases.
 *
 * Run verification with:
 * ```shell
 * alloy6 exec -f -o specs/output specs/job_shard.als
 * ```
 */
module job_shard

open util/ordering[Time]

sig Time {}
sig Job {}
sig Worker {}
sig TaskId {}
sig Attempt {}

/** Job status (NOT including Cancelled - that's tracked separately) */
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
    -- (in DB, buffer, or lease), it can only be associated with that same job
    -- (In Rust, the task's key contains job_id which is immutable)
    all taskid: TaskId | lone j: Job | 
        (some t: Time | some dbQueuedAt[taskid, t] and dbQueuedAt[taskid, t] = j) or
        (some t: Time | some bufferedAt[taskid, t] and bufferedAt[taskid, t] = j) or
        (some t: Time | some leaseJobAt[taskid, t] and leaseJobAt[taskid, t] = j)
    
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
    
    -- Cancellation is monotonic: once cancelled at time t, cancelled at all future times
    all j: Job, t: Time - last | isCancelledAt[j, t] implies isCancelledAt[j, t.next]
    
    -- At most one cancellation record per job per time
    all j: Job, t: Time | lone c: JobCancelled | c.cancelled_job = j and c.cancelled_time = t
}

/** Check if a job is cancelled at time t */
pred isCancelledAt[j: Job, t: Time] {
    some c: JobCancelled | c.cancelled_job = j and c.cancelled_time = t
}

fun statusAt[j: Job, t: Time]: JobStatus {
    { s: JobStatus | some js: JobState | js.job = j and js.time = t and js.stat = s }
}

fun attemptStatusAt[att: Attempt, t: Time]: AttemptStatus {
    { s: AttemptStatus | some astate: AttemptState | astate.attempt = att and astate.atime = t and astate.astat = s }
}

fun attemptExistsAt[t: Time]: set Attempt {
    { a: Attempt | some e: AttemptExists | e.time = t and a in e.attempts }
}

fun jobExistsAt[t: Time]: set Job {
    { j: Job | some e: JobExists | e.time = t and j in e.jobs }
}

fun attemptJob[att: Attempt]: Job {
    { jb: Job | some r: JobAttemptRelation | r.a = att and r.j = jb }
}

fun dbQueuedAt[taskid: TaskId, t: Time]: set Job {
    { j: Job | some qt: DbQueuedTask | qt.db_qtask = taskid and qt.db_qtime = t and qt.db_qjob = j }
}

fun bufferedAt[taskid: TaskId, t: Time]: set Job {
    { j: Job | some bt: BufferedTask | bt.buf_qtask = taskid and bt.buf_qtime = t and bt.buf_qjob = j }
}

fun leaseAt[taskid: TaskId, t: Time]: set Worker {
    { w: Worker | some l: Lease | l.ltask = taskid and l.ltime = t and l.lworker = w }
}

fun leaseJobAt[taskid: TaskId, t: Time]: set Job {
    { j: Job | some l: Lease | l.ltask = taskid and l.ltime = t and l.ljob = j }
}

fun leaseAttemptAt[taskid: TaskId, t: Time]: set Attempt {
    { a: Attempt | some l: Lease | l.ltask = taskid and l.ltime = t and l.lattempt = a }
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

pred isTerminalAttempt[s: AttemptStatus] {
    s in (AttemptSucceeded + AttemptFailed)
}

/** Get the expiry time for a lease on a task at time t */
fun leaseExpiresAt[taskid: TaskId, t: Time]: set Time {
    { exp: Time | some l: Lease | l.ltask = taskid and l.ltime = t and l.lexpiresAt = exp }
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
}

-- Transition: ENQUEUE - Create a new job with its first task
-- See: job_store_shard.rs::enqueue_with_metadata
pred enqueue[tid: TaskId, j: Job, t: Time, tnext: Time] {
    -- [SILO-ENQ-1] Pre: job does NOT exist yet (we're creating a new job)
    j not in jobExistsAt[t]
    
    -- Pre: task is not already in use (not in DB, buffer, or leased)
    no dbQueuedAt[tid, t]
    no bufferedAt[tid, t]
    no leaseAt[tid, t]
    
    -- [SILO-ENQ-2] Post: job now exists with status Scheduled, NOT cancelled
    jobExistsAt[tnext] = jobExistsAt[t] + j
    statusAt[j, tnext] = Scheduled
    not isCancelledAt[j, tnext]
    
    -- [SILO-ENQ-3] Post: first task added to DB queue
    one qt: DbQueuedTask | qt.db_qtask = tid and qt.db_qjob = j and qt.db_qtime = tnext
    
    -- Buffer unchanged (Broker must scan later)
    all tid2: TaskId | bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
    
    -- Frame: other existing jobs unchanged (status and cancellation)
    all j2: Job | j2 in jobExistsAt[t] implies statusAt[j2, tnext] = statusAt[j2, t]
    all j2: Job | j2 in jobExistsAt[t] implies (isCancelledAt[j2, tnext] iff isCancelledAt[j2, t])
    
    -- Frame: attempts unchanged
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    
    -- Frame: other tasks unchanged
    all tid2: TaskId | tid2 != tid implies {
        dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    }
    all tid2: TaskId | {
        leaseAt[tid2, tnext] = leaseAt[tid2, t]
        leaseJobAt[tid2, tnext] = leaseJobAt[tid2, t]
        leaseAttemptAt[tid2, tnext] = leaseAttemptAt[tid2, t]
    }
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
}

-- Transition: DEQUEUE - Worker claims task from Buffer
-- Note: We intentionally DO NOT check job status here for performance.
-- If the job was cancelled while in the buffer, the worker will discover
-- this on their next heartbeat or when they try to complete.
pred dequeue[tid: TaskId, w: Worker, a: Attempt, t: Time, tnext: Time] {
    -- [SILO-DEQ-1] Pre: Task is in BUFFER
    some bufferedAt[tid, t]
    let j = bufferedAt[tid, t] | {
        one j
        
        -- [SILO-DEQ-2] Pre: Job must exist
        j in jobExistsAt[t]
        
        -- Pre: Attempt `a` does not exist yet
        a not in attemptExistsAt[t]
        attemptJob[a] = j 
        
        -- [SILO-DEQ-3] Always: Remove task from DB and buffer
        no dbQueuedAt[tid, tnext]
        no bufferedAt[tid, tnext]
        
        -- [SILO-DEQ-4] Always: Create lease with expiry
        -- Note: We create the lease even for cancelled jobs - worker discovers on heartbeat
        one l: Lease | l.ltask = tid and l.lworker = w and l.ljob = j and l.lattempt = a 
            and l.ltime = tnext and gt[l.lexpiresAt, tnext]
        
        -- [SILO-DEQ-5] Always: Create attempt with AttemptRunning status
        attemptExistsAt[tnext] = attemptExistsAt[t] + a
        attemptStatusAt[a, tnext] = AttemptRunning
        
        -- [SILO-DEQ-6] Job status: unconditionally set to Running (pure write, no status read)
        -- Cancellation is preserved separately, so this doesn't lose cancel info
        statusAt[j, tnext] = Running
        
        -- Frame: other existing jobs unchanged
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        
        -- Frame: cancellation preserved (key property!)
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
}

-- Transition: COMPLETE_SUCCESS
-- Note: Worker's result ALWAYS takes precedence.
-- This is a pure WRITE operation - no status read needed for performance.
pred completeSuccess[tid: TaskId, w: Worker, t: Time, tnext: Time] {
    -- [SILO-SUCC-1] Pre: worker holds lease
    leaseAt[tid, t] = w
    some leaseJobAt[tid, t]
    some leaseAttemptAt[tid, t]
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        one j
        one a
        attemptStatusAt[a, t] = AttemptRunning
        
        -- [SILO-SUCC-2] Post: release lease
        no leaseAt[tid, tnext]
        
        -- [SILO-SUCC-3] Post: set job status to Succeeded (pure write, overwrites ANY previous status)
        statusAt[j, tnext] = Succeeded
        
        -- [SILO-SUCC-4] Post: set attempt status to AttemptSucceeded
        attemptStatusAt[a, tnext] = AttemptSucceeded
        
        -- Frame: other existing jobs unchanged
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        
        -- Frame: attempt existence unchanged, other attempts unchanged
        attemptExistsAt[tnext] = attemptExistsAt[t]
        all a2: attemptExistsAt[t] - a | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    -- Frame: job existence, cancellation unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    all j: Job | isCancelledAt[j, tnext] iff isCancelledAt[j, t]
    
    -- Frame: other tasks and queue unchanged
    all tid2: TaskId | tid2 != tid implies {
        leaseAt[tid2, tnext] = leaseAt[tid2, t]
        leaseJobAt[tid2, tnext] = leaseJobAt[tid2, t]
        leaseAttemptAt[tid2, tnext] = leaseAttemptAt[tid2, t]
    }
    all tid2: TaskId | dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    all tid2: TaskId | bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
}

-- Transition: COMPLETE_FAILURE_PERMANENT
-- Note: Worker's result takes precedence over any previously reported results
pred completeFailurePermanent[tid: TaskId, w: Worker, t: Time, tnext: Time] {
    -- [SILO-FAIL-1] Pre: worker holds lease
    leaseAt[tid, t] = w
    some leaseJobAt[tid, t]
    some leaseAttemptAt[tid, t]
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        one j
        one a
        attemptStatusAt[a, t] = AttemptRunning
        
        -- [SILO-FAIL-2] Post: release lease
        no leaseAt[tid, tnext]
        
        -- [SILO-FAIL-3] Post: set job status to Failed (pure write, overwrites ANY previous status)
        statusAt[j, tnext] = Failed
        
        -- [SILO-FAIL-4] Post: set attempt status to AttemptFailed
        attemptStatusAt[a, tnext] = AttemptFailed
        
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
    all tid2: TaskId | dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    all tid2: TaskId | bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
}

-- Transition: COMPLETE_FAILURE_RETRY
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
}

-- Transition: CANCEL - mark job as cancelled
pred cancelJob[j: Job, t: Time, tnext: Time] {
    -- [SILO-CXL-1] Pre: job exists and not already cancelled
    j in jobExistsAt[t]
    not isCancelledAt[j, t]
    
    -- [SILO-CXL-2] Post: Mark job as cancelled (add cancellation record)
    one c: JobCancelled | c.cancelled_job = j and c.cancelled_time = tnext
    
    -- Post: Status stays the same (cancellation is orthogonal to status)
    statusAt[j, tnext] = statusAt[j, t]
    
    -- [SILO-CXL-3] Remove from DB queue (prevent new dequeues of new tasks)
    no qt: DbQueuedTask | qt.db_qjob = j and qt.db_qtime = tnext
    
    -- Frame: other DB queue entries unchanged
    all tid: TaskId | dbQueuedAt[tid, t] != j implies dbQueuedAt[tid, tnext] = dbQueuedAt[tid, t]
    
    -- NOTE: Does NOT remove from buffer immediately (stale buffer possible)
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    
    -- NOTE: Does NOT remove lease - worker will discover cancellation on heartbeat
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
}

-- Transition: REAP_EXPIRED_LEASE - System reclaims expired lease (worker crashed)
-- Similar to completeFailurePermanent but triggered by expiry, not worker report
pred reapExpiredLease[tid: TaskId, t: Time, tnext: Time] {
    -- [SILO-REAP-1] Pre: Lease exists and has expired
    some leaseAt[tid, t]
    leaseExpired[tid, t]
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        one j
        one a
        attemptStatusAt[a, t] = AttemptRunning
        
        -- [SILO-REAP-2] Post: Lease removed
        no leaseAt[tid, tnext]
        
        -- [SILO-REAP-3] Post: Set job status to Failed (pure write, overwrites ANY previous status)
        statusAt[j, tnext] = Failed
        
        -- [SILO-REAP-4] Post: Set attempt status to AttemptFailed
        attemptStatusAt[a, tnext] = AttemptFailed
        
        -- Frame: other existing jobs unchanged
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        attemptExistsAt[tnext] = attemptExistsAt[t]
        all a2: attemptExistsAt[t] - a | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    
    -- Frame: job existence and cancellation unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    all j: Job | isCancelledAt[j, tnext] iff isCancelledAt[j, t]
    
    -- Frame: other tasks unchanged
    all tid2: TaskId | tid2 != tid implies {
        leaseAt[tid2, tnext] = leaseAt[tid2, t]
        leaseJobAt[tid2, tnext] = leaseJobAt[tid2, t]
        leaseAttemptAt[tid2, tnext] = leaseAttemptAt[tid2, t]
    }
    all tid2: TaskId | dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    all tid2: TaskId | bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
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
}

-- System Trace
pred step[t: Time, tnext: Time] {
    (some tid: TaskId, j: Job | enqueue[tid, j, t, tnext])
    or (brokerScan[t, tnext])
    or (some tid: TaskId, w: Worker, a: Attempt | dequeue[tid, w, a, t, tnext])
    or (some tid: TaskId, w: Worker | heartbeat[tid, w, t, tnext])
    or (some tid: TaskId | reapExpiredLease[tid, t, tnext])
    or (some tid: TaskId, w: Worker | completeSuccess[tid, w, t, tnext])
    or (some tid: TaskId, w: Worker | completeFailurePermanent[tid, w, t, tnext])
    or (some tid: TaskId, w: Worker, newTid: TaskId | completeFailureRetry[tid, w, newTid, t, tnext])
    or (some j: Job | cancelJob[j, t, tnext])
    or stutter[t, tnext]
}

fact traces {
    init[first]
    all t: Time - last | step[t, t.next]
}


/** A task is never leased to two workers at once */
assert noDoubleLease {
    all t: Time, tid: TaskId | lone leaseAt[tid, t]
}

/** A job never has two active leases */
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
        -- Succeeded and Failed are truly terminal
        statusAt[j, t] = Succeeded implies statusAt[j, t.next] = Succeeded
        statusAt[j, t] = Failed implies statusAt[j, t.next] = Failed
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
 * Cancellation is monotonic: once cancelled, always cancelled.
 * This is enforced by wellFormed, but we verify it here.
 */
assert cancellationIsMonotonic {
    all j: Job, t: Time - last | 
        isCancelledAt[j, t] implies isCancelledAt[j, t.next]
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
 * Scenario: Early cancellation prevents execution
 * Job is cancelled BEFORE its task enters the buffer.
 * Task is removed from DB queue, never enters buffer, job never runs.
 * This is the ideal cancellation path - no wasted work.
 */
pred exampleEarlyCancellationPreventsRun {
    some tid: TaskId, j: Job, t1, t2, t3: Time | {
        lt[t1, t2] and lt[t2, t3]
        -- t1: job scheduled, task in DB queue, NOT in buffer yet, NOT cancelled
        j in jobExistsAt[t1]
        statusAt[j, t1] = Scheduled
        not isCancelledAt[j, t1]
        some dbQueuedAt[tid, t1]
        dbQueuedAt[tid, t1] = j
        no bufferedAt[tid, t1]  -- Not yet in buffer
        -- t2: job cancelled, task removed from DB queue
        isCancelledAt[j, t2]
        no dbQueuedAt[tid, t2]  -- Removed by cancelJob
        no bufferedAt[tid, t2]  -- Never entered buffer
        -- t3: job stays cancelled, never got a lease, never ran
        isCancelledAt[j, t3]
        statusAt[j, t3] = Scheduled  -- Status never changed to Running
        no l: Lease | l.ljob = j and l.ltime = t3  -- No lease ever created
    }
}

/**
 * Scenario: Job cancelled while task in buffer, worker dequeues stale task
 * Worker gets a lease for a cancelled job (allowed now for performance).
 * This demonstrates that dequeue doesn't check cancellation status, which allows cancelled jobs to be dequeued which isn't great, but it means we can avoid an extra read on every dequeue.
 */
pred exampleCancelThenDequeue {
    some tid: TaskId, j: Job, t: Time - last | {
        -- At time t: job is cancelled but task is in buffer
        bufferedAt[tid, t] = j
        isCancelledAt[j, t]
        -- At time t.next: a lease was created for this task/job
        some l: Lease | l.ltask = tid and l.ljob = j and l.ltime = t.next
    }
}

/**
 * GOOD EXAMPLE: Cancellation preserved during stale buffer dequeue
 * This demonstrates the fix where:
 * 1. Job scheduled, task in buffer
 * 2. Cancel arrives - job gets cancelled flag
 * 3. Worker dequeues stale task - job status becomes Running BUT cancellation preserved!
 * 4. Worker discovers cancellation on heartbeat
 */
pred exampleCancellationPreservedOnDequeue {
    some tid: TaskId, j: Job, t1, t2, t3, t4: Time | {
        lt[t1, t2] and lt[t2, t3] and lt[t3, t4]
        -- t1: job scheduled, task in buffer, NOT cancelled
        j in jobExistsAt[t1]
        statusAt[j, t1] = Scheduled
        not isCancelledAt[j, t1]
        some bufferedAt[tid, t1]
        bufferedAt[tid, t1] = j
        -- t2: job cancelled while task still in buffer (status unchanged, flag set)
        isCancelledAt[j, t2]
        some bufferedAt[tid, t2]  -- still in buffer (stale)
        -- t3: worker dequeues - status becomes Running BUT cancellation flag preserved!
        some leaseAt[tid, t3]
        statusAt[j, t3] = Running  -- Status is Running (pure write)
        isCancelledAt[j, t3]       -- But cancellation flag is preserved!
        -- t4: worker still has lease, still sees cancellation
        some leaseAt[tid, t4]
        isCancelledAt[j, t4]  -- Worker sees cancellation via heartbeat response
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
run exampleEarlyCancellationPreventsRun for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 1 Attempt, 6 Time,
    6 JobState, 6 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 6 AttemptExists, 6 JobExists, 1 JobAttemptRelation, 6 JobCancelled

run exampleCancelThenDequeue for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled
run exampleCancellationPreservedOnDequeue for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled
run exampleCancellationDiscoveryOnHeartbeat for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled

run exampleHeartbeat for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled

run exampleLeaseExpiry for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation, 8 JobCancelled

-- Bounds: Jobs may not exist at all times, so JobState <= Jobs × Times
-- AttemptExists and JobExists = 1 per Time
-- JobCancelled: at most 1 per job (once cancelled, always cancelled)
check noDoubleLease for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled
check oneLeasePerJob for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled
check leaseJobMustBeRunning for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled
check runningJobHasRunningAttempt for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled
check attemptTerminalIsForever for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled
check validTransitions for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled
check noZombieAttempts for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled
check noQueuedTasksForTerminal for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled
check noLeasesForTerminal for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled
check cancellationIsMonotonic for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation, 16 JobCancelled

