//! Load-aware shard placement engine.
//!
//! The placement engine runs on the leader node and monitors load across all nodes
//! in the cluster. When it detects significant load imbalance, it decides to migrate
//! shards from heavily-loaded nodes to lightly-loaded ones.
//!
//! Key design principles:
//! - **Conservative**: Requires sustained imbalance before acting
//! - **Rate-limited**: Cooldown between migrations prevents thrashing
//! - **Self-healing**: Overrides have TTL, system returns to hashing if leader fails
//! - **Observable**: All decisions are logged for debugging

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::coordination::{Coordinator, PlacementOverride, PlacementOverrides};
use crate::settings::PlacementConfig;

/// A sample of load data for a single shard at a point in time.
#[derive(Debug, Clone)]
pub struct ShardLoadSample {
    pub shard_id: u32,
    pub load_score: f64,
    pub timestamp: Instant,
}

/// Load data for a single node, aggregating all shards it owns.
#[derive(Debug, Clone)]
pub struct NodeLoadSnapshot {
    pub node_id: String,
    pub grpc_addr: String,
    pub shards: Vec<ShardLoadSnapshot>,
    pub total_load_score: f64,
}

/// Load data for a single shard.
#[derive(Debug, Clone)]
pub struct ShardLoadSnapshot {
    pub shard_id: u32,
    pub enqueue_rate_per_sec: f64,
    pub dequeue_rate_per_sec: f64,
    pub pending_jobs: u64,
    pub running_jobs: u64,
    pub load_score: f64,
}

/// Cluster-wide load snapshot.
#[derive(Debug, Clone)]
pub struct ClusterLoad {
    pub nodes: Vec<NodeLoadSnapshot>,
    pub timestamp: Instant,
}

impl ClusterLoad {
    /// Calculate the average load per node.
    pub fn average_load_per_node(&self) -> f64 {
        if self.nodes.is_empty() {
            return 0.0;
        }
        let total: f64 = self.nodes.iter().map(|n| n.total_load_score).sum();
        total / self.nodes.len() as f64
    }

    /// Find the node with the highest load.
    pub fn max_loaded_node(&self) -> Option<&NodeLoadSnapshot> {
        self.nodes
            .iter()
            .max_by(|a, b| a.total_load_score.partial_cmp(&b.total_load_score).unwrap())
    }

    /// Find the node with the lowest load.
    pub fn min_loaded_node(&self) -> Option<&NodeLoadSnapshot> {
        self.nodes
            .iter()
            .min_by(|a, b| a.total_load_score.partial_cmp(&b.total_load_score).unwrap())
    }
}

/// A pending or completed migration.
#[derive(Debug, Clone)]
pub struct Migration {
    pub shard_id: u32,
    pub source_node: String,
    pub target_node: String,
    pub reason: String,
    pub started_at: Instant,
}

/// Tracks migration history for rate limiting.
#[derive(Debug, Default)]
struct MigrationHistory {
    /// Recent migrations for rate limiting
    migrations: VecDeque<Instant>,
    /// Last migration time for cooldown
    last_migration: Option<Instant>,
}

impl MigrationHistory {
    fn record_migration(&mut self) {
        let now = Instant::now();
        self.migrations.push_back(now);
        self.last_migration = Some(now);

        // Prune old migrations (older than 1 hour)
        let cutoff = now - Duration::from_secs(3600);
        while let Some(&ts) = self.migrations.front() {
            if ts < cutoff {
                self.migrations.pop_front();
            } else {
                break;
            }
        }
    }

    fn migrations_in_last_hour(&self) -> usize {
        let cutoff = Instant::now() - Duration::from_secs(3600);
        self.migrations.iter().filter(|&&ts| ts >= cutoff).count()
    }

    fn time_since_last_migration(&self) -> Option<Duration> {
        self.last_migration.map(|t| t.elapsed())
    }
}

/// The main placement engine.
///
/// Runs on the leader node and makes shard placement decisions.
pub struct PlacementEngine {
    coordinator: Arc<dyn Coordinator>,
    config: PlacementConfig,
    /// Historical load samples for stability checking
    load_history: HashMap<String, VecDeque<f64>>, // node_id -> recent load scores
    /// Migration history for rate limiting
    migration_history: MigrationHistory,
    /// Currently in-progress migrations (shard_id -> Migration)
    migrations_in_progress: HashMap<u32, Migration>,
    /// Active placement overrides
    overrides: PlacementOverrides,
}

impl PlacementEngine {
    /// Create a new placement engine.
    pub fn new(coordinator: Arc<dyn Coordinator>, config: PlacementConfig) -> Self {
        Self {
            coordinator,
            config,
            load_history: HashMap::new(),
            migration_history: MigrationHistory::default(),
            migrations_in_progress: HashMap::new(),
            overrides: PlacementOverrides::new(),
        }
    }

    /// Run the placement engine main loop.
    ///
    /// This should be spawned as a background task. It will only take action
    /// when this node is the leader.
    pub async fn run(&mut self, mut shutdown_rx: watch::Receiver<bool>) {
        let interval = Duration::from_secs(self.config.load_collection_interval_secs as u64);

        loop {
            // Check for shutdown
            if *shutdown_rx.borrow() {
                info!("placement engine shutting down");
                break;
            }

            // Only run if we're the leader
            if !self.coordinator.is_leader() {
                debug!("not the leader, skipping placement engine cycle");
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = shutdown_rx.changed() => {}
                }
                continue;
            }

            // Run one cycle
            if let Err(e) = self.run_cycle().await {
                warn!(error = %e, "placement engine cycle failed");
            }

            // Wait for next cycle
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = shutdown_rx.changed() => {}
            }
        }
    }

    /// Run a single placement engine cycle.
    async fn run_cycle(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Prune expired overrides
        let pruned = self.overrides.prune_expired();
        if pruned > 0 {
            debug!(pruned, "pruned expired placement overrides");
        }

        // Collect load from all nodes
        let cluster_load = self.collect_cluster_load().await?;

        // Update load history
        self.update_load_history(&cluster_load);

        // Check if migration is warranted
        if let Some(migration) = self.decide_migration(&cluster_load) {
            if self.can_migrate() {
                info!(
                    shard_id = migration.shard_id,
                    source = %migration.source_node,
                    target = %migration.target_node,
                    reason = %migration.reason,
                    "executing shard migration"
                );
                self.execute_migration(migration).await?;
            } else {
                debug!("migration would be beneficial but rate limit prevents it");
            }
        }

        Ok(())
    }

    /// Collect load data from all nodes in the cluster.
    async fn collect_cluster_load(
        &self,
    ) -> Result<ClusterLoad, Box<dyn std::error::Error + Send + Sync>> {
        // For now, we only have local load data available via the gRPC service
        // In a full implementation, we would call GetClusterLoad on the leader
        // which would aggregate data from ReportLoad calls from all nodes

        // Get members from coordinator
        let members = self.coordinator.get_members().await?;

        // Build a placeholder cluster load (in a real implementation,
        // this would aggregate from all nodes via gRPC)
        let nodes: Vec<NodeLoadSnapshot> = members
            .iter()
            .map(|m| NodeLoadSnapshot {
                node_id: m.node_id.clone(),
                grpc_addr: m.grpc_addr.clone(),
                shards: Vec::new(), // Would be populated from actual load reports
                total_load_score: 0.0,
            })
            .collect();

        Ok(ClusterLoad {
            nodes,
            timestamp: Instant::now(),
        })
    }

    /// Update the load history with the latest snapshot.
    fn update_load_history(&mut self, cluster_load: &ClusterLoad) {
        const HISTORY_SIZE: usize = 10; // Keep last 10 samples

        for node in &cluster_load.nodes {
            let history = self
                .load_history
                .entry(node.node_id.clone())
                .or_insert_with(VecDeque::new);

            history.push_back(node.total_load_score);

            while history.len() > HISTORY_SIZE {
                history.pop_front();
            }
        }
    }

    /// Decide if a migration should be performed.
    ///
    /// Returns Some(Migration) if a migration is warranted, None otherwise.
    fn decide_migration(&self, cluster_load: &ClusterLoad) -> Option<Migration> {
        // Need at least 2 nodes to migrate
        if cluster_load.nodes.len() < 2 {
            return None;
        }

        let max_node = cluster_load.max_loaded_node()?;
        let min_node = cluster_load.min_loaded_node()?;
        let avg_load = cluster_load.average_load_per_node();

        // Check if max load exceeds threshold
        if avg_load == 0.0 {
            return None; // No load, nothing to balance
        }

        let load_ratio = max_node.total_load_score / avg_load;
        if load_ratio < self.config.load_imbalance_threshold {
            debug!(
                load_ratio,
                threshold = self.config.load_imbalance_threshold,
                "load imbalance below threshold"
            );
            return None;
        }

        // Check stability - has the imbalance been sustained?
        if !self.has_stable_imbalance(&max_node.node_id) {
            debug!(
                node_id = %max_node.node_id,
                intervals_required = self.config.stability_intervals,
                "imbalance not yet stable"
            );
            return None;
        }

        // Find the busiest shard on the busiest node
        let busiest_shard = max_node
            .shards
            .iter()
            .max_by(|a, b| a.load_score.partial_cmp(&b.load_score).unwrap())?;

        // Don't migrate if the shard is already being migrated
        if self.migrations_in_progress.contains_key(&busiest_shard.shard_id) {
            debug!(
                shard_id = busiest_shard.shard_id,
                "shard already being migrated"
            );
            return None;
        }

        // Don't migrate to the same node
        if max_node.node_id == min_node.node_id {
            return None;
        }

        Some(Migration {
            shard_id: busiest_shard.shard_id,
            source_node: max_node.node_id.clone(),
            target_node: min_node.node_id.clone(),
            reason: format!(
                "load imbalance: {} (load={:.2}) -> {} (load={:.2}), ratio={:.2}",
                max_node.node_id,
                max_node.total_load_score,
                min_node.node_id,
                min_node.total_load_score,
                load_ratio
            ),
            started_at: Instant::now(),
        })
    }

    /// Check if the node has had sustained high load.
    fn has_stable_imbalance(&self, node_id: &str) -> bool {
        let history = match self.load_history.get(node_id) {
            Some(h) => h,
            None => return false,
        };

        if history.len() < self.config.stability_intervals as usize {
            return false;
        }

        // Check that all recent samples show imbalance
        // For simplicity, we check if the node has been consistently above average
        // A more sophisticated check would compare against the actual threshold
        let avg: f64 = history.iter().sum::<f64>() / history.len() as f64;
        let recent: Vec<f64> = history
            .iter()
            .rev()
            .take(self.config.stability_intervals as usize)
            .copied()
            .collect();

        // All recent samples should be above average (indicating sustained high load)
        recent.iter().all(|&load| load > avg * 0.9)
    }

    /// Check if we're allowed to perform a migration (rate limiting).
    fn can_migrate(&self) -> bool {
        // Check cooldown
        if let Some(elapsed) = self.migration_history.time_since_last_migration() {
            if elapsed < Duration::from_secs(self.config.migration_cooldown_secs as u64) {
                debug!(
                    elapsed_secs = elapsed.as_secs(),
                    cooldown_secs = self.config.migration_cooldown_secs,
                    "migration cooldown not elapsed"
                );
                return false;
            }
        }

        // Check hourly rate limit (circuit breaker)
        let migrations_this_hour = self.migration_history.migrations_in_last_hour();
        if migrations_this_hour >= self.config.max_migrations_per_hour as usize {
            debug!(
                migrations_this_hour,
                max = self.config.max_migrations_per_hour,
                "hourly migration limit reached"
            );
            return false;
        }

        true
    }

    /// Execute a migration by creating a placement override.
    async fn execute_migration(
        &mut self,
        migration: Migration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Create the placement override
        let override_ = PlacementOverride::with_reason(
            migration.shard_id,
            migration.target_node.clone(),
            self.config.override_ttl_secs,
            migration.reason.clone(),
        );

        // Store the override locally
        self.overrides.set(override_.clone());

        // Record the migration
        self.migration_history.record_migration();
        self.migrations_in_progress
            .insert(migration.shard_id, migration);

        // TODO: Write the override to etcd/k8s for distribution to all nodes
        // For now, the override is only stored locally in the placement engine
        // In a full implementation, we would:
        // 1. Write the override to coordination storage (etcd/k8s)
        // 2. Other nodes would watch for override changes
        // 3. The override would be applied during shard reconciliation

        info!(
            shard_id = override_.shard_id,
            target_node = %override_.target_node,
            ttl_secs = override_.ttl_secs,
            "created placement override"
        );

        Ok(())
    }

    /// Get the current active overrides (for debugging/monitoring).
    pub fn active_overrides(&self) -> &PlacementOverrides {
        &self.overrides
    }

    /// Get the number of migrations in the last hour (for monitoring).
    pub fn migrations_last_hour(&self) -> usize {
        self.migration_history.migrations_in_last_hour()
    }

    /// Get current in-progress migrations (for monitoring).
    pub fn in_progress_migrations(&self) -> &HashMap<u32, Migration> {
        &self.migrations_in_progress
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_load_average() {
        let cluster = ClusterLoad {
            nodes: vec![
                NodeLoadSnapshot {
                    node_id: "a".to_string(),
                    grpc_addr: "a:50051".to_string(),
                    shards: vec![],
                    total_load_score: 100.0,
                },
                NodeLoadSnapshot {
                    node_id: "b".to_string(),
                    grpc_addr: "b:50051".to_string(),
                    shards: vec![],
                    total_load_score: 50.0,
                },
            ],
            timestamp: Instant::now(),
        };

        assert!((cluster.average_load_per_node() - 75.0).abs() < 0.001);
    }

    #[test]
    fn test_migration_history_rate_limiting() {
        let mut history = MigrationHistory::default();

        // No migrations yet
        assert_eq!(history.migrations_in_last_hour(), 0);
        assert!(history.time_since_last_migration().is_none());

        // Record a migration
        history.record_migration();
        assert_eq!(history.migrations_in_last_hour(), 1);
        assert!(history.time_since_last_migration().is_some());

        // Record more
        for _ in 0..5 {
            history.record_migration();
        }
        assert_eq!(history.migrations_in_last_hour(), 6);
    }
}
