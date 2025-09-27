/// The KV store key for a given job's info by id
pub fn job_info_key(id: &str) -> String {
    format!("jobs/{}", id)
}

/// The KV store key for a given job's status
pub fn job_status_key(id: &str) -> String {
    format!("job_status/{}", id)
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
pub fn attempt_key(job_id: &str, attempt: u32) -> String {
    format!("attempts/{}/{}", job_id, attempt)
}

/// Concurrency request queue: requests/<queue-name>/<request_time_ms>/<request_id>
pub fn concurrency_request_key(queue: &str, request_time_ms: i64, request_id: &str) -> String {
    format!(
        "requests/{}/{:020}/{}",
        queue,
        request_time_ms.max(0) as u64,
        request_id
    )
}

/// Concurrency holders set: holders/<queue-name>/<task-id>
pub fn concurrency_holder_key(queue: &str, task_id: &str) -> String {
    format!("holders/{}/{}", queue, task_id)
}

/// Optional per-queue dynamic concurrency limit: limits/<queue-name>
pub fn concurrency_limit_key(queue: &str) -> String {
    format!("limits/{}", queue)
}
