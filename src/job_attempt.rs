// Re-export the codec's zero-copy JobAttemptView directly.
pub use crate::codec::JobAttemptView;

/// Outcome passed by callers when reporting an attempt's completion.
#[derive(Debug, Clone)]
pub enum AttemptOutcome {
    Success {
        result: Vec<u8>,
    },
    Error {
        error_code: String,
        error: Vec<u8>,
    },
    /// Worker acknowledges cancellation after discovering it via heartbeat.
    /// This is the clean shutdown path where worker reports it has stopped work.
    Cancelled,
}

/// Attempt status lifecycle
#[derive(Debug, Clone)]
pub enum AttemptStatus {
    Running,
    Succeeded {
        finished_at_ms: i64,
        result: Vec<u8>,
    },
    Failed {
        finished_at_ms: i64,
        error_code: String,
        error: Vec<u8>,
    },
    /// Attempt was cancelled (either worker acknowledged or lease expired while job was cancelled)
    Cancelled {
        finished_at_ms: i64,
    },
}

/// Stored representation of a job attempt
#[derive(Debug, Clone)]
pub struct JobAttempt {
    pub job_id: String,
    /// Total attempt number (monotonically increasing, 1-based)
    pub attempt_number: u32,
    /// Attempt number within current run (resets to 1 on job restart)
    pub relative_attempt_number: u32,
    pub task_id: String,
    /// When the attempt started (epoch ms). Set at dequeue time for running attempts,
    /// or preserved from the source system for imported attempts.
    pub started_at_ms: i64,
    pub status: AttemptStatus,
}
