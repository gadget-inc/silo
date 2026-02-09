//! DST (Deterministic Simulation Testing) instrumentation events.
//!
//! This module provides zero-overhead instrumentation hooks for DST testing.
//! When the `dst` feature is disabled, all functions compile to no-ops.
//!
//! # Two-Phase Event Emission
//!
//! Events that are emitted before an async DB write use two-phase emission:
//!
//! 1. **Pre-write**: `emit_pending(event, write_op)` records the event at the correct
//!    causal position (before the write's yield point), tagged with a write operation ID.
//! 2. **Post-write**: `confirm_write(write_op)` marks the events as confirmed (write succeeded),
//!    or `cancel_write(write_op)` removes them (write failed).
//!
//! Events emitted with plain `emit()` are immediately confirmed (no write needed).
//!
//! At the end of the simulation, `take_all_confirmed()` returns all confirmed events
//! in their original emission order for invariant validation. This avoids the ordering
//! issues that arise from turmoil's cooperative scheduling interleaving events between
//! a write's visibility point and its Future resolution.

/// Events emitted by server-side code for DST invariant checking.
#[derive(Debug, Clone)]
pub enum DstEvent {
    /// A job was successfully enqueued.
    JobEnqueued { tenant: String, job_id: String },

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

    /// A node acquired ownership of a shard.
    ShardAcquired { node_id: String, shard_id: String },

    /// A node successfully closed a shard's storage (flushed to object storage).
    ShardClosed { node_id: String, shard_id: String },

    /// A node released ownership of a shard.
    ShardReleased { node_id: String, shard_id: String },
}

#[cfg(feature = "dst")]
mod dst_impl {
    use super::DstEvent;
    use std::cell::{Cell, RefCell};
    use std::sync::atomic::{AtomicU64, Ordering};

    /// An event with its write operation tag.
    /// `write_op == None` means confirmed (either emitted with `emit()` or confirmed via `confirm_write()`).
    /// `write_op == Some(id)` means pending confirmation for that write operation.
    pub(super) struct EmittedEvent {
        pub event: DstEvent,
        pub write_op: Option<u64>,
    }

    static NEXT_WRITE_OP: AtomicU64 = AtomicU64::new(1);

    thread_local! {
        pub(super) static DST_EVENTS: RefCell<Vec<EmittedEvent>> = const { RefCell::new(Vec::new()) };
        /// Running count of terminal status events (Succeeded/Failed/Cancelled) for convergence checking.
        /// Counts all events including pending ones, since this is used as a heuristic, not for correctness.
        static TERMINAL_COUNT: Cell<usize> = const { Cell::new(0) };
    }

    fn is_terminal_event(event: &DstEvent) -> bool {
        matches!(
            event,
            DstEvent::JobStatusChanged { new_status, .. }
                if new_status == "Succeeded" || new_status == "Failed" || new_status == "Cancelled"
        )
    }

    /// Get a unique write operation ID for two-phase event emission.
    pub fn next_write_op() -> u64 {
        NEXT_WRITE_OP.fetch_add(1, Ordering::Relaxed)
    }

    /// Emit a DST event that is immediately confirmed (no pending write).
    pub fn emit(event: DstEvent) {
        if is_terminal_event(&event) {
            TERMINAL_COUNT.with(|c| c.set(c.get() + 1));
        }
        DST_EVENTS.with(|events| {
            events.borrow_mut().push(EmittedEvent {
                event,
                write_op: None,
            });
        });
    }

    /// Emit a DST event that is pending confirmation of a write operation.
    /// The event is recorded at this position for correct causal ordering,
    /// but will only be included in validation if `confirm_write(write_op)` is called.
    pub fn emit_pending(event: DstEvent, write_op: u64) {
        if is_terminal_event(&event) {
            TERMINAL_COUNT.with(|c| c.set(c.get() + 1));
        }
        DST_EVENTS.with(|events| {
            events.borrow_mut().push(EmittedEvent {
                event,
                write_op: Some(write_op),
            });
        });
    }

    /// Confirm that a write operation succeeded. All pending events tagged with this
    /// write_op will be included in the final validated event stream.
    pub fn confirm_write(write_op: u64) {
        DST_EVENTS.with(|events| {
            for e in events.borrow_mut().iter_mut() {
                if e.write_op == Some(write_op) {
                    e.write_op = None;
                }
            }
        });
    }

    /// Cancel a write operation (write failed). Removes all pending events tagged
    /// with this write_op. Terminal count is not decremented since it's a heuristic.
    pub fn cancel_write(write_op: u64) {
        DST_EVENTS.with(|events| {
            events.borrow_mut().retain(|e| e.write_op != Some(write_op));
        });
    }

    /// Take all confirmed events in their original emission order, for end-of-test validation.
    /// Unconfirmed events are discarded (their writes never completed).
    /// This clears the event buffer.
    pub fn take_all_confirmed() -> Vec<DstEvent> {
        DST_EVENTS.with(|events| {
            let all = std::mem::take(&mut *events.borrow_mut());
            all.into_iter()
                .filter(|e| e.write_op.is_none())
                .map(|e| e.event)
                .collect()
        })
    }

    /// Get the approximate count of terminal events (Succeeded/Failed/Cancelled).
    /// This includes pending events and is intended as a convergence heuristic,
    /// not for invariant validation.
    pub fn terminal_event_count() -> usize {
        TERMINAL_COUNT.with(|c| c.get())
    }

    /// Clear all events and reset counters.
    pub fn clear_events() {
        DST_EVENTS.with(|events| events.borrow_mut().clear());
        TERMINAL_COUNT.with(|c| c.set(0));
    }
}

#[cfg(feature = "dst")]
pub use dst_impl::*;

// No-op stubs when dst feature is disabled
#[cfg(not(feature = "dst"))]
#[inline(always)]
pub fn next_write_op() -> u64 {
    0
}

#[cfg(not(feature = "dst"))]
#[inline(always)]
pub fn emit(_event: DstEvent) {}

#[cfg(not(feature = "dst"))]
#[inline(always)]
pub fn emit_pending(_event: DstEvent, _write_op: u64) {}

#[cfg(not(feature = "dst"))]
#[inline(always)]
pub fn confirm_write(_write_op: u64) {}

#[cfg(not(feature = "dst"))]
#[inline(always)]
pub fn cancel_write(_write_op: u64) {}

#[cfg(not(feature = "dst"))]
#[inline(always)]
pub fn take_all_confirmed() -> Vec<DstEvent> {
    Vec::new()
}

#[cfg(not(feature = "dst"))]
#[inline(always)]
pub fn terminal_event_count() -> usize {
    0
}

#[cfg(not(feature = "dst"))]
#[inline(always)]
pub fn clear_events() {}
