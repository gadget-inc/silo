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

/** Job status */
abstract sig JobStatus {}
one sig Scheduled, Running, Succeeded, Failed, Cancelled extends JobStatus {}

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

pred isTerminal[s: JobStatus] {
    s in (Succeeded + Failed + Cancelled)
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
-- (Matches Rust: enqueue creates job_info, job_status, AND first task atomically)
pred enqueue[tid: TaskId, j: Job, t: Time, tnext: Time] {
    -- Pre: job does NOT exist yet (this is job creation)
    j not in jobExistsAt[t]
    
    -- Pre: task not already used
    no dbQueuedAt[tid, t]
    no bufferedAt[tid, t]
    no leaseAt[tid, t]
    
    -- Post: job now exists with status Scheduled
    jobExistsAt[tnext] = jobExistsAt[t] + j
    statusAt[j, tnext] = Scheduled
    
    -- Post: first task added to DB queue
    one qt: DbQueuedTask | qt.db_qtask = tid and qt.db_qjob = j and qt.db_qtime = tnext
    
    -- Buffer unchanged (Broker must scan later)
    all tid2: TaskId | bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
    
    -- Frame: other existing jobs unchanged
    all j2: Job | j2 in jobExistsAt[t] implies statusAt[j2, tnext] = statusAt[j2, t]
    
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
-- The broker picks up tasks from DB and puts them in memory.
pred brokerScan[t: Time, tnext: Time] {
    -- Pre: There are tasks in DB that are NOT in buffer
    some tid: TaskId | some dbQueuedAt[tid, t] and no bufferedAt[tid, t]
    
    -- Effect: Copy (some) tasks from DB to Buffer
    -- We define this broadly: Buffer set grows to include subset of DB tasks
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
    
    -- Frame: DB, Leases, Job Status, Attempts unchanged
    all tid: TaskId | dbQueuedAt[tid, tnext] = dbQueuedAt[tid, t]
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    all j: Job | statusAt[j, tnext] = statusAt[j, t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
    jobExistsAt[tnext] = jobExistsAt[t]
}

-- Transition: DEQUEUE - Worker claims task from Buffer
-- Critical: This checks DB consistency at moment of claim
pred dequeue[tid: TaskId, w: Worker, a: Attempt, t: Time, tnext: Time] {
    -- Pre: Task is in BUFFER
    some bufferedAt[tid, t]
    let j = bufferedAt[tid, t] | {
        one j
        
        -- Pre: Job must exist (Rust checks maybe_job.is_some())
        j in jobExistsAt[t]
        
        -- Pre: Attempt `a` does not exist yet (needed for active case)
        a not in attemptExistsAt[t]
        attemptJob[a] = j 
        
        -- Always: Remove task from DB and buffer
        no dbQueuedAt[tid, tnext]
        no bufferedAt[tid, tnext]
        
        -- Branch: If job is active, create lease and mark running
        --         If job is terminal, just clean up (no lease)
        (not isTerminal[statusAt[j, t]]) implies {
            -- Active job: create lease with expiry, mark running, create attempt
            -- Lease expires at some future time (must heartbeat before then)
            one l: Lease | l.ltask = tid and l.lworker = w and l.ljob = j and l.lattempt = a 
                and l.ltime = tnext and gt[l.lexpiresAt, tnext]
            statusAt[j, tnext] = Running
            attemptExistsAt[tnext] = attemptExistsAt[t] + a
            attemptStatusAt[a, tnext] = AttemptRunning
            -- Frame: other existing jobs unchanged
            all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        }
        
        isTerminal[statusAt[j, t]] implies {
            -- Terminal job: just clean up, no lease created
            no l: Lease | l.ltask = tid and l.ltime = tnext
            -- Frame: all job statuses unchanged
            all j2: Job | j2 in jobExistsAt[t] implies statusAt[j2, tnext] = statusAt[j2, t]
            -- Frame: attempts unchanged
            attemptExistsAt[tnext] = attemptExistsAt[t]
            all a2: attemptExistsAt[t] | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
        }
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
    
    -- Frame for active case: existing attempts unchanged
    (not isTerminal[statusAt[bufferedAt[tid, t], t]]) implies {
        all a2: attemptExistsAt[t] | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
}

-- Transition: COMPLETE_SUCCESS
pred completeSuccess[tid: TaskId, w: Worker, t: Time, tnext: Time] {
    -- Pre: worker holds lease
    leaseAt[tid, t] = w
    some leaseJobAt[tid, t]
    some leaseAttemptAt[tid, t]
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        one j
        one a
        statusAt[j, t] = Running
        attemptStatusAt[a, t] = AttemptRunning
        
        -- Post: lease released, job succeeded, attempt succeeded
        no leaseAt[tid, tnext]
        statusAt[j, tnext] = Succeeded
        attemptStatusAt[a, tnext] = AttemptSucceeded
        
        -- Frame: other existing jobs unchanged
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        
        -- Frame: attempt existence unchanged, other attempts unchanged
        attemptExistsAt[tnext] = attemptExistsAt[t]
        all a2: attemptExistsAt[t] - a | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    -- Frame: job existence unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    
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
pred completeFailurePermanent[tid: TaskId, w: Worker, t: Time, tnext: Time] {
    leaseAt[tid, t] = w
    some leaseJobAt[tid, t]
    some leaseAttemptAt[tid, t]
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        one j
        one a
        statusAt[j, t] = Running
        attemptStatusAt[a, t] = AttemptRunning
        
        no leaseAt[tid, tnext]
        statusAt[j, tnext] = Failed
        attemptStatusAt[a, tnext] = AttemptFailed
        
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        attemptExistsAt[tnext] = attemptExistsAt[t]
        all a2: attemptExistsAt[t] - a | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    -- Frame: job existence unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    
    all tid2: TaskId | tid2 != tid implies {
        leaseAt[tid2, tnext] = leaseAt[tid2, t]
        leaseJobAt[tid2, tnext] = leaseJobAt[tid2, t]
        leaseAttemptAt[tid2, tnext] = leaseAttemptAt[tid2, t]
    }
    all tid2: TaskId | dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    all tid2: TaskId | bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
}

-- Transition: COMPLETE_FAILURE_RETRY
pred completeFailureRetry[tid: TaskId, w: Worker, newTid: TaskId, t: Time, tnext: Time] {
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
        statusAt[j, t] = Running
        attemptStatusAt[a, t] = AttemptRunning
        
        no leaseAt[tid, tnext]
        -- Enqueue new task in DB (not buffer yet)
        one qt: DbQueuedTask | qt.db_qtask = newTid and qt.db_qjob = j and qt.db_qtime = tnext
        statusAt[j, tnext] = Scheduled
        attemptStatusAt[a, tnext] = AttemptFailed
        
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        attemptExistsAt[tnext] = attemptExistsAt[t]
        all a2: attemptExistsAt[t] - a | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    -- Frame: job existence unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    
    all tid2: TaskId | tid2 != tid implies {
        leaseAt[tid2, tnext] = leaseAt[tid2, t]
        leaseJobAt[tid2, tnext] = leaseJobAt[tid2, t]
        leaseAttemptAt[tid2, tnext] = leaseAttemptAt[tid2, t]
    }
    all tid2: TaskId | tid2 != newTid implies dbQueuedAt[tid2, tnext] = dbQueuedAt[tid2, t]
    all tid2: TaskId | bufferedAt[tid2, tnext] = bufferedAt[tid2, t]
}

-- Transition: CANCEL - cancel a scheduled job
pred cancelJob[j: Job, t: Time, tnext: Time] {
    -- Pre: job exists and is scheduled
    j in jobExistsAt[t]
    statusAt[j, t] = Scheduled
    no tid: TaskId | leaseJobAt[tid, t] = j
    
    statusAt[j, tnext] = Cancelled
    
    -- Remove from DB queue
    no qt: DbQueuedTask | qt.db_qjob = j and qt.db_qtime = tnext
    
    -- NOTE: Does NOT remove from buffer immediately (stale buffer possible)
    -- Stale entries are handled at dequeue time when the job check fails
    all tid: TaskId | bufferedAt[tid, tnext] = bufferedAt[tid, t]
    
    all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
    all tid: TaskId | {
        leaseAt[tid, tnext] = leaseAt[tid, t]
        leaseJobAt[tid, tnext] = leaseJobAt[tid, t]
        leaseAttemptAt[tid, tnext] = leaseAttemptAt[tid, t]
    }
    all tid: TaskId | dbQueuedAt[tid, t] != j implies dbQueuedAt[tid, tnext] = dbQueuedAt[tid, t]
    
    -- Frame: job existence unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
}

-- Transition: HEARTBEAT - Worker extends lease expiry
pred heartbeat[tid: TaskId, w: Worker, t: Time, tnext: Time] {
    -- Pre: Worker holds this lease (existence check only - matches Rust implementation)
    -- Note: Rust does NOT check expiry on heartbeat - only that lease exists and worker matches
    -- This means heartbeat can "save" an expired lease if it runs before the reaper
    leaseAt[tid, t] = w
    some leaseJobAt[tid, t]
    some leaseAttemptAt[tid, t]
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        one j
        one a
        
        -- Post: Lease renewed with new expiry in the future
        one l: Lease | l.ltask = tid and l.lworker = w and l.ljob = j and l.lattempt = a 
            and l.ltime = tnext and gt[l.lexpiresAt, tnext]
        
        -- Frame: job status unchanged
        all j2: Job | j2 in jobExistsAt[t] implies statusAt[j2, tnext] = statusAt[j2, t]
    }
    
    -- Frame: everything else unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    attemptExistsAt[tnext] = attemptExistsAt[t]
    all a: attemptExistsAt[t] | attemptStatusAt[a, tnext] = attemptStatusAt[a, t]
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
    -- Pre: Lease exists and has expired
    some leaseAt[tid, t]
    leaseExpired[tid, t]
    
    let j = leaseJobAt[tid, t], a = leaseAttemptAt[tid, t] | {
        one j
        one a
        statusAt[j, t] = Running
        attemptStatusAt[a, t] = AttemptRunning
        
        -- Post: Lease removed, job and attempt marked failed (worker crashed)
        no leaseAt[tid, tnext]
        statusAt[j, tnext] = Failed
        attemptStatusAt[a, tnext] = AttemptFailed
        
        -- Frame: other existing jobs unchanged
        all j2: Job | j2 in jobExistsAt[t] and j2 != j implies statusAt[j2, tnext] = statusAt[j2, t]
        attemptExistsAt[tnext] = attemptExistsAt[t]
        all a2: attemptExistsAt[t] - a | attemptStatusAt[a2, tnext] = attemptStatusAt[a2, t]
    }
    
    -- Frame: job existence unchanged
    jobExistsAt[tnext] = jobExistsAt[t]
    
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
pred stutter[t: Time, tnext: Time] {
    -- All existing jobs unchanged
    all j: Job | j in jobExistsAt[t] implies statusAt[j, tnext] = statusAt[j, t]
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

/** Only Running jobs have leases */
assert onlyRunningHasLease {
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

/** Valid transitions only (for existing jobs) */
assert validTransitions {
    all t: Time - last, j: Job | j in jobExistsAt[t] implies {
        statusAt[j, t] = Scheduled implies statusAt[j, t.next] in (Scheduled + Running + Cancelled)
        statusAt[j, t] = Running implies statusAt[j, t.next] in (Running + Succeeded + Failed + Scheduled)
        isTerminal[statusAt[j, t]] implies statusAt[j, t.next] = statusAt[j, t]
    }
}

/** No "Zombie" Attempts: If job is Cancelled, it should not have running attempts */
assert noZombieAttempts {
    all t: Time, att: attemptExistsAt[t] | 
        let j = attemptJob[att] | 
        (statusAt[j, t] = Cancelled implies attemptStatusAt[att, t] != AttemptRunning)
}

/** Queue Consistency: Terminal jobs have no DB queued tasks */
assert noQueuedTasksForTerminal {
    all t: Time, j: Job | (j in jobExistsAt[t] and isTerminal[statusAt[j, t]]) implies 
        no qt: DbQueuedTask | qt.db_qjob = j and qt.db_qtime = t
}

/** Lease Consistency: Terminal jobs have no active leases */
assert noLeasesForTerminal {
    all t: Time, j: Job | (j in jobExistsAt[t] and isTerminal[statusAt[j, t]]) implies 
        no l: Lease | l.ljob = j and l.ltime = t
}

/** 
 * Buffer can be stale (contain cancelled job tasks), but we should never
 * successfully dequeue a cancelled job's task into a lease.
 * This is caught by noLeasesForTerminal, but this check makes the intent explicit.
 */
assert noDequeueCancelledJob {
    all t: Time - last, tid: TaskId, j: Job |
        (some bufferedAt[tid, t] and bufferedAt[tid, t] = j and statusAt[j, t] = Cancelled)
        implies not (some l: Lease | l.ltask = tid and l.ljob = j and l.ltime = t.next)
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
 * Verify we don't execute it.
 */
pred exampleStaleBuffer {
    some tid: TaskId, j: Job, t: Time | {
        bufferedAt[tid, t] = j
        statusAt[j, t] = Cancelled
        -- And we reach end of time without violating safety
    }
}

/**
 * TEST: Can we even produce the bad scenario?
 * If this is UNSAT, the model structurally prevents cancelled jobs from being dequeued.
 */
pred exampleCancelThenDequeue {
    some tid: TaskId, j: Job, t: Time - last | {
        -- At time t: job is cancelled but task is in buffer
        bufferedAt[tid, t] = j
        statusAt[j, t] = Cancelled
        -- At time t.next: a lease was created for this task/job
        some l: Lease | l.ltask = tid and l.ljob = j and l.ltime = t.next
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
run exampleSuccess for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 6 Time,
    6 JobState, 6 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 6 AttemptExists, 6 JobExists, 2 JobAttemptRelation
    
run exampleRetry for 3 but exactly 1 Job, 1 Worker, 3 TaskId, 2 Attempt, 10 Time,
    10 JobState, 10 AttemptState, 5 DbQueuedTask, 5 BufferedTask, 4 Lease, 10 AttemptExists, 10 JobExists, 2 JobAttemptRelation

run examplePermanentFailureWithRetry for 3 but exactly 1 Job, 1 Worker, 3 TaskId, 3 Attempt, 10 Time,
    10 JobState, 10 AttemptState, 5 DbQueuedTask, 5 BufferedTask, 4 Lease, 10 AttemptExists, 10 JobExists, 3 JobAttemptRelation

run exampleStaleBuffer for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 1 Attempt, 6 Time,
    6 JobState, 6 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 6 AttemptExists, 6 JobExists, 1 JobAttemptRelation

run exampleCancelThenDequeue for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 2 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation

run exampleHeartbeat for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation

run exampleLeaseExpiry for 3 but exactly 1 Job, 1 Worker, 2 TaskId, 2 Attempt, 8 Time,
    8 JobState, 8 AttemptState, 4 DbQueuedTask, 4 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 2 JobAttemptRelation

-- Bounds: Jobs may not exist at all times, so JobState <= Jobs × Times
-- AttemptExists and JobExists = 1 per Time
check noDoubleLease for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation
check oneLeasePerJob for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation
check onlyRunningHasLease for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation
check runningJobHasRunningAttempt for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation
check attemptTerminalIsForever for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation
check validTransitions for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation
check noZombieAttempts for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation
check noQueuedTasksForTerminal for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation
check noLeasesForTerminal for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation
check noDequeueCancelledJob for 4 but 2 Job, 2 Worker, 3 TaskId, 3 Attempt, 8 Time,
    16 JobState, 24 AttemptState, 8 DbQueuedTask, 8 BufferedTask, 4 Lease, 8 AttemptExists, 8 JobExists, 3 JobAttemptRelation

