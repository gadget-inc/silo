fn escape_tenant(tenant: &str) -> String {
    // Minimal escaping to avoid "/" in path segments and preserve arbitrary strings
    tenant.replace('%', "%25").replace('/', "%2F")
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

/// Construct the key for a task, ordered by start time.
pub fn task_key(start_time_ms: i64, priority: u8, job_id: &str, attempt: u32) -> String {
    // Zero-pad time to 20 digits and priority to 2 digits; 00 is highest, 99 lowest
    // Lexicographic order: time asc, then priority asc (so higher priority first)
    format!(
        "tasks/{:020}/{:02}/{}/{}",
        start_time_ms.max(0) as u64,
        priority,
        job_id,
        attempt
    )
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
