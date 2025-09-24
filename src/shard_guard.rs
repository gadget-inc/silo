use etcd_client::LockOptions;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex, Notify};
use tracing::Instrument;
use tracing::{debug, info, info_span};

use crate::coordination::keys;
use etcd_client::Client;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardPhase {
    Idle,
    Acquiring,
    Held,
    Releasing,
    ShuttingDown,
    ShutDown,
}

impl std::fmt::Display for ShardPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardPhase::Idle => write!(f, "Idle"),
            ShardPhase::Acquiring => write!(f, "Acquiring"),
            ShardPhase::Held => write!(f, "Held"),
            ShardPhase::Releasing => write!(f, "Releasing"),
            ShardPhase::ShuttingDown => write!(f, "ShuttingDown"),
            ShardPhase::ShutDown => write!(f, "ShutDown"),
        }
    }
}

pub struct ShardState {
    pub desired: bool,
    pub phase: ShardPhase,
    pub held_key: Option<Vec<u8>>,
}

/// ShardGuard owns the lifecycle of a single shard's etcd ownership lock.
///
/// Responsibilities:
/// - Serialize desired state transitions per shard (Idle/Acquiring/Held/Releasing)
/// - Acquire the `coord/shards/<id>/owner` lock using the node's liveness lease
/// - Delay and perform safe releases to allow new owners to spin up
/// - Maintain `owned` set updates atomically when lock is acquired/released
/// - Coalesce redundant intents via `set_desired` and a single background loop
///
/// It isolates lock retries, jitter, cancellation, and state changes so the
/// coordinator can simply compute the desired shard set and call `set_desired`.
pub struct ShardGuard {
    pub shard_id: u32,
    pub client: Client,
    pub cluster_prefix: String,
    pub liveness_lease_id: i64,
    pub state: Mutex<ShardState>,
    pub notify: Notify,
    pub shutdown: watch::Receiver<bool>,
}

impl ShardGuard {
    pub fn new(
        shard_id: u32,
        client: Client,
        cluster_prefix: String,
        liveness_lease_id: i64,
        shutdown: watch::Receiver<bool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            shard_id,
            client,
            cluster_prefix,
            liveness_lease_id,
            state: Mutex::new(ShardState {
                desired: false,
                phase: ShardPhase::Idle,
                held_key: None,
            }),
            notify: Notify::new(),
            shutdown,
        })
    }

    fn owner_key(&self) -> String {
        keys::shard_owner_key(&self.cluster_prefix, self.shard_id)
    }

    pub async fn set_desired(&self, desired: bool) {
        let mut st = self.state.lock().await;
        if matches!(st.phase, ShardPhase::ShutDown | ShardPhase::ShuttingDown) {
            debug!(shard_id = self.shard_id, desired = desired, phase = %st.phase, "shard: can't change desired state after shut down");
            return;
        }

        if st.desired != desired {
            let prev_desired = st.desired;
            let prev_phase = st.phase;
            st.desired = desired;
            debug!(shard_id = self.shard_id, prev_desired = prev_desired, desired = desired, phase = %prev_phase, "shard: set_desired");
            self.notify.notify_one();
        }
    }

    pub async fn run(self: Arc<Self>, owned_arc: Arc<Mutex<HashSet<u32>>>) {
        loop {
            // Convert external shutdown signal into internal ShuttingDown phase
            if *self.shutdown.borrow() {
                let mut st = self.state.lock().await;
                st.phase = ShardPhase::ShuttingDown;
            }
            // Transition based on desired and current phase
            {
                let mut st = self.state.lock().await;
                match (st.phase, st.desired, st.held_key.is_some()) {
                    (ShardPhase::ShutDown, _, _) => { /* stay in ShutDown */ }
                    (ShardPhase::ShuttingDown, _, _) => { /* stay in ShuttingDown */ }
                    (ShardPhase::Idle, true, false) => {
                        debug!(
                            shard_id = self.shard_id,
                            "shard: transition Idle -> Acquiring"
                        );
                        st.phase = ShardPhase::Acquiring;
                    }
                    (ShardPhase::Held, false, true) => {
                        debug!(
                            shard_id = self.shard_id,
                            "shard: transition Held -> Releasing"
                        );
                        st.phase = ShardPhase::Releasing;
                    }
                    _ => {}
                }
            }

            let phase = { self.state.lock().await.phase };
            match phase {
                ShardPhase::ShutDown => {
                    break;
                }
                ShardPhase::Acquiring => {
                    let name = self.owner_key();
                    let span =
                        info_span!("shard.acquire", shard_id = self.shard_id, lock_key = %name);
                    async {
                        let mut lock_cli = self.client.lock_client();
                        let mut attempt: u32 = 0;

                        // Small deterministic initial jitter to avoid thundering herd
                        let initial_jitter_ms = ((self.shard_id as u64).wrapping_mul(13)) % 80;

                        tokio::time::sleep(Duration::from_millis(initial_jitter_ms)).await;
                        loop {
                            // Abort promptly if phase switched (e.g., to ShuttingDown or Idle)
                            if { self.state.lock().await.phase } != ShardPhase::Acquiring {
                                break;
                            }
                            {
                                let mut st = self.state.lock().await;
                                if !st.desired || st.phase != ShardPhase::Acquiring {
                                    if !st.desired && st.phase == ShardPhase::Acquiring {
                                        // Ensure we do not livelock in Acquiring when desire is withdrawn
                                        st.phase = ShardPhase::Idle;
                                    }
                                    info!(shard_id = self.shard_id, desired = st.desired, phase = %st.phase, "shard: acquire abort (state changed)");
                                    break;
                                }
                            }
                            match tokio::time::timeout(
                                Duration::from_millis(500),
                                lock_cli.lock(
                                    name.as_bytes().to_vec(),
                                    Some(LockOptions::new().with_lease(self.liveness_lease_id)),
                                ),
                            )
                            .await
                            {
                                Ok(Ok(resp)) => {
                                    let key = resp.key().to_vec();
                                    {
                                        let mut st = self.state.lock().await;
                                        st.held_key = Some(key);
                                        st.phase = ShardPhase::Held;
                                    }
                                    let mut owned = owned_arc.lock().await;
                                    owned.insert(self.shard_id);
                                    debug!(
                                        shard_id = self.shard_id,
                                        attempts = attempt,
                                        "shard: acquire success"
                                    );
                                    break;
                                }
                                Ok(Err(_)) | Err(_) => {
                                    attempt = attempt.wrapping_add(1);
                                    let jitter_ms = ((self.shard_id as u64)
                                        .wrapping_mul(31)
                                        .wrapping_add(attempt as u64 * 17))
                                        % 150;
                                    tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                                    if attempt % 8 == 0 {
                                        debug!(
                                            shard_id = self.shard_id,
                                            attempt = attempt,
                                            jitter_ms = jitter_ms,
                                            "shard: acquire retry"
                                        );
                                    }
                                }
                            }
                        }
                    }
                    .instrument(span)
                    .await;
                }
                ShardPhase::Releasing => {
                    let name = self.owner_key();
                    let span =
                        info_span!("shard.release", shard_id = self.shard_id, lock_key = %name);
                    async {
                        debug!(shard_id = self.shard_id, "shard: release start (delay)");
                        tokio::time::sleep(Duration::from_millis(100)).await;

                        // If desire flipped back to true during the delay, cancel the release
                        let mut cancelled = false;
                        let key_opt = {
                            let mut st = self.state.lock().await;
                            if st.phase == ShardPhase::ShuttingDown {
                                // fall through to unlock path
                            } else if st.desired {
                                st.phase = ShardPhase::Held;
                                cancelled = true;
                                debug!(
                                    shard_id = self.shard_id,
                                    "shard: release cancelled (desired=true)"
                                );
                            }
                            st.held_key.clone()
                        };
                        if cancelled {
                            return;
                        }
                        if let Some(key) = key_opt {
                            let mut lock_cli = self.client.lock_client();
                            let _ = lock_cli.unlock(key).await;
                            {
                                let mut st = self.state.lock().await;
                                st.held_key = None;
                                st.phase = ShardPhase::Idle;
                            }
                            let mut owned = owned_arc.lock().await;
                            owned.remove(&self.shard_id);
                            debug!(shard_id = self.shard_id, "shard: release done");
                        } else {
                            let mut st = self.state.lock().await;
                            st.phase = ShardPhase::Idle;
                            debug!(
                                shard_id = self.shard_id,
                                "shard: release noop (no held key)"
                            );
                        }
                    }
                    .instrument(span)
                    .await;
                }
                ShardPhase::ShuttingDown => {
                    // One-time best-effort unlock, ensure ShutDown, and exit
                    let key_opt = { self.state.lock().await.held_key.clone() };
                    if let Some(key) = key_opt {
                        let mut lock_cli = self.client.lock_client();
                        let _ = lock_cli.unlock(key).await;
                        let mut owned = owned_arc.lock().await;
                        owned.remove(&self.shard_id);
                    }
                    {
                        let mut st = self.state.lock().await;
                        st.held_key = None;
                        st.phase = ShardPhase::ShutDown;
                    }
                    break;
                }
                ShardPhase::Idle | ShardPhase::Held => {
                    let mut shutdown_rx = self.shutdown.clone();
                    tokio::select! {
                        _ = self.notify.notified() => {}
                        _ = shutdown_rx.changed() => {
                            // Will transition to ShuttingDown at top of next loop
                        }
                    }
                }
            }
        }
    }
}
