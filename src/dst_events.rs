//! DST (Deterministic Simulation Testing) instrumentation events.
//!
//! This module provides zero-overhead instrumentation hooks for DST testing.
//! When the `dst` feature is disabled, all functions compile to no-ops.
//!
//! Events are emitted at the exact moment state changes occur server-side,
//! providing ground-truth information to invariant trackers without the
//! race conditions inherent in client-side tracking.

/// Events emitted by server-side code for DST invariant checking.
#[derive(Debug, Clone)]
pub enum DstEvent {
    /// A job was successfully enqueued.
    JobEnqueued {
        tenant: String,
        job_id: String,
    },

    /// A job's status changed.
    JobStatusChanged {
        tenant: String,
        job_id: String,
        new_status: String,
    },

    /// A task was leased to a worker.
    TaskLeased {
        tenant: String,
        job_id: String,
        task_id: String,
        worker_id: String,
    },

    /// A task lease was released (completed, failed, or expired).
    TaskReleased {
        tenant: String,
        job_id: String,
        task_id: String,
    },

    /// A concurrency ticket was granted to a task.
    ConcurrencyTicketGranted {
        tenant: String,
        queue: String,
        task_id: String,
        job_id: String,
    },

    /// A concurrency ticket was released by a task.
    ConcurrencyTicketReleased {
        tenant: String,
        queue: String,
        task_id: String,
    },
}

#[cfg(feature = "dst")]
thread_local! {
    static DST_EVENTS: std::cell::RefCell<Vec<DstEvent>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Emit a DST event. No-op when `dst` feature is disabled.
#[cfg(feature = "dst")]
pub fn emit(event: DstEvent) {
    DST_EVENTS.with(|events| {
        events.borrow_mut().push(event);
    });
}

/// Emit a DST event. No-op when `dst` feature is disabled.
#[cfg(not(feature = "dst"))]
#[inline(always)]
pub fn emit(_event: DstEvent) {
    // Compiles to nothing when dst feature is disabled
}

/// Drain all pending DST events. Returns empty vec when `dst` feature is disabled.
#[cfg(feature = "dst")]
pub fn drain_events() -> Vec<DstEvent> {
    DST_EVENTS.with(|events| {
        std::mem::take(&mut *events.borrow_mut())
    })
}

/// Drain all pending DST events. Returns empty vec when `dst` feature is disabled.
#[cfg(not(feature = "dst"))]
#[inline(always)]
pub fn drain_events() -> Vec<DstEvent> {
    Vec::new()
}

/// Clear all pending DST events without processing them.
#[cfg(feature = "dst")]
pub fn clear_events() {
    DST_EVENTS.with(|events| {
        events.borrow_mut().clear();
    });
}

/// Clear all pending DST events without processing them.
#[cfg(not(feature = "dst"))]
#[inline(always)]
pub fn clear_events() {
    // No-op when dst feature is disabled
}
