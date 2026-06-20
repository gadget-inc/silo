//! Cancellation-safety guard for post-commit in-memory concurrency holder
//! releases.

use crate::concurrency::ConcurrencyManager;

/// RAII guard over durable concurrency holders that a write batch has staged
/// for deletion, paired with an in-memory `atomic_release` + `request_grant`
/// that the caller runs post-commit.
///
/// **Why this exists.** A handler that finishes/releases a task deletes the
/// durable holder row in its batch and queues a matching in-memory release.
/// `write_with_options(... await_durable: true)` applies the batch to the WAL
/// on first poll, so by the time the await yields, the durable row may already
/// be gone — but the post-commit `atomic_release` loop only runs if the await
/// resolves cleanly. Cancellation mid-await (the request future is dropped)
/// leaves durable=0, in_memory=1: a ghost holder that wedges the queue (the
/// prod 300/300 bug).
///
/// **What it does.** On normal exit the caller calls `take_all()` (commit
/// success) or `disarm()` (commit failure) to neutralize the guard and run the
/// in-memory release loop itself. If the future is dropped or panics, `Drop`
/// enqueues each tuple for the reconciler, which point-looks-up the durable
/// holder row to decide whether to release the in-memory entry (the WAL apply
/// landed) or leave it alone (the WAL apply did not). Per-task_id, idempotent,
/// correct in both branches of the ambiguity.
///
/// Used by `dequeue.rs` and `lease.rs::report_attempt_outcome`.
pub(crate) struct PendingHolderReleaseGuard<'a> {
    concurrency: &'a ConcurrencyManager,
    pending: Option<Vec<(String, String, String)>>,
}

impl<'a> PendingHolderReleaseGuard<'a> {
    pub(crate) fn new(concurrency: &'a ConcurrencyManager) -> Self {
        Self {
            concurrency,
            pending: Some(Vec::new()),
        }
    }

    pub(crate) fn extend<I: IntoIterator<Item = (String, String, String)>>(&mut self, iter: I) {
        if let Some(v) = self.pending.as_mut() {
            v.extend(iter);
        }
    }

    /// Drain and return the current pending releases while leaving the guard
    /// armed (with an empty buffer) for the next iteration. Used on the
    /// commit-success path so the caller runs the existing
    /// `atomic_release` + `request_grant` loop itself.
    pub(crate) fn take_all(&mut self) -> Vec<(String, String, String)> {
        self.pending
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }

    /// Neutralize the guard. Used at terminal exit paths (commit-failure /
    /// Ok return). On commit-failure the caller discards the returned vec: the
    /// batch didn't land so the durable holder is still there and the in-memory
    /// state is already correct.
    pub(crate) fn disarm(&mut self) -> Vec<(String, String, String)> {
        self.pending.take().unwrap_or_default()
    }
}

impl Drop for PendingHolderReleaseGuard<'_> {
    fn drop(&mut self) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        if pending.is_empty() {
            return;
        }
        tracing::warn!(
            count = pending.len(),
            "future dropped/panicked with pending holder releases; queueing reconciliation"
        );
        for (tenant, queue, task_id) in pending {
            self.concurrency
                .request_reconciliation(tenant, queue, task_id);
        }
    }
}
