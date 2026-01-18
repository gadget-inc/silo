fn escape_tenant(tenant: &str) -> String {
    // Minimal escaping to avoid "/" in path segments and preserve arbitrary strings
    tenant.replace('%', "%25").replace('/', "%2F")
}

/// Decode an escaped tenant string back to its original form
pub fn decode_tenant(encoded: &str) -> String {
    // Reverse the escaping: %2F -> /, %25 -> %
    // Order matters: decode %2F first, then %25
    encoded.replace("%2F", "/").replace("%25", "%")
}

pub fn escape_segment(segment: &str) -> String {
    // Apply the same escaping used for tenants to arbitrary path segments
    segment.replace('%', "%25").replace('/', "%2F")
}

fn validate_tenant_len(tenant: &str) {
    // Enforced by callers; keep panic-free here for perf in hot paths
    debug_assert!(tenant.chars().count() <= 64, "tenant id exceeds 64 chars");
}

fn validate_id_len(id: &str) {
    // Enforced by callers; keep panic-free here for perf in hot paths
    debug_assert!(id.chars().count() <= 64, "id exceeds 64 chars");
}

/// The KV store key for a given job's info by id
pub fn job_info_key(tenant: &str, id: &str) -> String {
    validate_tenant_len(tenant);
    validate_id_len(id);
    format!("jobs/{}/{}", escape_tenant(tenant), id)
}

/// The KV store key for a given job's status
pub fn job_status_key(tenant: &str, id: &str) -> String {
    validate_tenant_len(tenant);
    validate_id_len(id);
    format!("job_status/{}/{}", escape_tenant(tenant), id)
}

/// Index: time-ordered by status, newest-first using inverted timestamp
/// idx/status_ts/<tenant>/<status>/<inv_ts:020>/<job-id>
pub fn idx_status_time_key(tenant: &str, status: &str, changed_at_ms: i64, job_id: &str) -> String {
    validate_tenant_len(tenant);
    validate_id_len(job_id);
    let inv: u64 = u64::MAX - (changed_at_ms.max(0) as u64);
    format!(
        "idx/status_ts/{}/{}/{:020}/{}",
        escape_tenant(tenant),
        status,
        inv,
        job_id
    )
}

/// Index: jobs by metadata key/value (unsorted)
/// Key format: idx/meta/<tenant>/<key>/<value>/<job-id>
pub fn idx_metadata_key(tenant: &str, key: &str, value: &str, job_id: &str) -> String {
    validate_tenant_len(tenant);
    validate_id_len(job_id);
    format!(
        "idx/meta/{}/{}/{}/{}",
        escape_tenant(tenant),
        escape_segment(key),
        escape_segment(value),
        job_id
    )
}

/// Prefix for scanning jobs by metadata key/value
/// Prefix format: idx/meta/<tenant>/<key>/<value>/
pub fn idx_metadata_prefix(tenant: &str, key: &str, value: &str) -> String {
    validate_tenant_len(tenant);
    format!(
        "idx/meta/{}/{}/{}/",
        escape_tenant(tenant),
        escape_segment(key),
        escape_segment(value)
    )
}

/// Construct the key for a task, ordered by task group then start time.
/// Format: tasks/{task_group}/{time}/{priority}/{job_id}/{attempt}
pub fn task_key(
    task_group: &str,
    start_time_ms: i64,
    priority: u8,
    job_id: &str,
    attempt: u32,
) -> String {
    // Zero-pad time to 20 digits and priority to 2 digits; 00 is highest, 99 lowest
    // Lexicographic order: task_group, then time asc, then priority asc (so higher priority first)
    format!(
        "tasks/{}/{:020}/{:02}/{}/{}",
        escape_segment(task_group),
        start_time_ms.max(0) as u64,
        priority,
        job_id,
        attempt
    )
}

/// Get the prefix for scanning all tasks in a specific task group.
/// Format: tasks/{task_group}/
pub fn task_group_prefix(task_group: &str) -> String {
    format!("tasks/{}/", escape_segment(task_group))
}

/// Get the prefix for scanning all task groups.
/// Format: tasks/
pub fn tasks_prefix() -> &'static str {
    "tasks/"
}

/// Construct the key for a leased task by task id
pub fn leased_task_key(task_id: &str) -> String {
    format!("lease/{}", task_id)
}

/// Construct the key for an attempt record
/// attempts/<tenant>/<job-id>/<attempt-number>
pub fn attempt_key(tenant: &str, job_id: &str, attempt: u32) -> String {
    validate_tenant_len(tenant);
    validate_id_len(job_id);
    format!("attempts/{}/{}/{}", escape_tenant(tenant), job_id, attempt)
}

/// Construct the prefix for scanning all attempts of a job
/// attempts/<tenant>/<job-id>/
pub fn attempt_prefix(tenant: &str, job_id: &str) -> String {
    validate_tenant_len(tenant);
    validate_id_len(job_id);
    format!("attempts/{}/{}/", escape_tenant(tenant), job_id)
}

/// Concurrency request queue: requests/<tenant>/<queue-name>/<start_time_ms>/<priority>/<request_id>
/// Ordered by start time (when job should run), then priority (lower = higher), then request ID
pub fn concurrency_request_key(
    tenant: &str,
    queue: &str,
    start_time_ms: i64,
    priority: u8,
    request_id: &str,
) -> String {
    validate_tenant_len(tenant);
    validate_id_len(queue);
    format!(
        "requests/{}/{}/{:020}/{:02}/{}",
        escape_tenant(tenant),
        queue,
        start_time_ms.max(0) as u64,
        priority,
        request_id
    )
}

/// Concurrency holders set: holders/<tenant>/<queue-name>/<task-id>
pub fn concurrency_holder_key(tenant: &str, queue: &str, task_id: &str) -> String {
    validate_tenant_len(tenant);
    format!("holders/{}/{}/{}", escape_tenant(tenant), queue, task_id)
}

/// The KV store key for a job's cancellation flag.
/// Cancellation is stored separately from status to allow dequeue to blindly write Running
/// without losing cancellation info. See Alloy spec: JobCancelled is orthogonal to JobStatus.
pub fn job_cancelled_key(tenant: &str, id: &str) -> String {
    validate_tenant_len(tenant);
    validate_id_len(id);
    format!("job_cancelled/{}/{}", escape_tenant(tenant), id)
}

/// The KV store key for a floating concurrency limit's state.
/// Stores the current max concurrency, last refresh time, and refresh task status.
pub fn floating_limit_state_key(tenant: &str, queue_key: &str) -> String {
    validate_tenant_len(tenant);
    format!(
        "floating_limits/{}/{}",
        escape_tenant(tenant),
        escape_segment(queue_key)
    )
}
