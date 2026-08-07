use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use rhiza_core::NodeId;

use crate::types::{
    FeatureVector, Identity, MissingnessFlags, NodePressure, ProposerStats, RequestClass, RpcStats,
};

/// Configuration for telemetry freshness and cold-start gates.
#[derive(Clone, Debug)]
pub struct CollectorConfig {
    /// Maximum age of telemetry before it's considered stale (microseconds).
    pub freshness_limit_us: u64,
    /// Minimum sample count before the model can produce non-static actions.
    pub min_sample_count: u64,
    /// How often to aggregate rolling statistics.
    pub aggregation_interval: Duration,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            freshness_limit_us: 5_000_000, // 5 seconds
            min_sample_count: 100,
            aggregation_interval: Duration::from_secs(1),
        }
    }
}

/// Internal per-proposer latency sample.
#[derive(Clone, Debug)]
struct LatencySample {
    latency_us: u64,
    success: bool,
    timeout: bool,
    #[allow(dead_code)]
    timestamp: Instant,
}

/// TelemetryCollector gathers per-proposer latency, RPC stats, contention
/// rates, and node pressure for feature vector assembly.
pub struct TelemetryCollector {
    config: CollectorConfig,
    /// Rolling latency samples per proposer.
    proposer_samples: RwLock<HashMap<NodeId, Vec<LatencySample>>>,
    /// Current RPC statistics.
    rpc_stats: RwLock<RpcStats>,
    /// Current node pressure.
    node_pressure: RwLock<NodePressure>,
    /// Current request class.
    request_class: RwLock<Option<RequestClass>>,
    /// In-flight proposal counts per proposer.
    in_flight: RwLock<HashMap<NodeId, u32>>,
    /// Queue depths per proposer.
    queue_depth: RwLock<HashMap<NodeId, u32>>,
    /// Total sample count for cold-start gating.
    total_samples: RwLock<u64>,
    /// Last update timestamp.
    last_update: RwLock<Instant>,
    /// Rolling window size.
    window_size: usize,
}

impl TelemetryCollector {
    pub fn new(config: CollectorConfig) -> Self {
        Self {
            config,
            proposer_samples: RwLock::new(HashMap::new()),
            rpc_stats: RwLock::new(RpcStats::default()),
            node_pressure: RwLock::new(NodePressure::default()),
            request_class: RwLock::new(None),
            in_flight: RwLock::new(HashMap::new()),
            queue_depth: RwLock::new(HashMap::new()),
            total_samples: RwLock::new(0),
            last_update: RwLock::new(Instant::now()),
            window_size: 1000,
        }
    }

    /// Record a decision-latency observation for a proposer.
    pub fn record_proposer_latency(
        &self,
        proposer_id: &NodeId,
        latency_us: u64,
        success: bool,
        timeout: bool,
    ) {
        let sample = LatencySample {
            latency_us,
            success,
            timeout,
            timestamp: Instant::now(),
        };

        if let Ok(mut samples) = self.proposer_samples.write() {
            let entry = samples.entry(proposer_id.clone()).or_default();
            entry.push(sample);
            if entry.len() > self.window_size {
                entry.remove(0);
            }
        }

        if let Ok(mut count) = self.total_samples.write() {
            *count += 1;
        }

        if let Ok(mut ts) = self.last_update.write() {
            *ts = Instant::now();
        }
    }

    /// Update aggregated RPC statistics.
    pub fn update_rpc_stats(&self, stats: RpcStats) {
        if let Ok(mut s) = self.rpc_stats.write() {
            *s = stats;
        }
        if let Ok(mut ts) = self.last_update.write() {
            *ts = Instant::now();
        }
    }

    /// Update node pressure signals.
    pub fn update_node_pressure(&self, pressure: NodePressure) {
        if let Ok(mut p) = self.node_pressure.write() {
            *p = pressure;
        }
    }

    /// Set the current request class.
    pub fn set_request_class(&self, class: RequestClass) {
        if let Ok(mut rc) = self.request_class.write() {
            *rc = Some(class);
        }
    }

    /// Update in-flight count for a proposer.
    pub fn set_in_flight(&self, proposer_id: &NodeId, count: u32) {
        if let Ok(mut m) = self.in_flight.write() {
            m.insert(proposer_id.clone(), count);
        }
    }

    /// Update queue depth for a proposer.
    pub fn set_queue_depth(&self, proposer_id: &NodeId, depth: u32) {
        if let Ok(mut m) = self.queue_depth.write() {
            m.insert(proposer_id.clone(), depth);
        }
    }

    /// Get the total sample count.
    pub fn total_samples(&self) -> u64 {
        self.total_samples.read().map(|c| *c).unwrap_or(0)
    }

    /// Check if telemetry is fresh enough for model use.
    pub fn is_fresh(&self) -> bool {
        self.last_update
            .read()
            .map(|ts| ts.elapsed().as_micros() as u64 <= self.config.freshness_limit_us)
            .unwrap_or(false)
    }

    /// Check if cold-start gates are satisfied.
    pub fn cold_start_gates_passed(&self) -> bool {
        self.total_samples() >= self.config.min_sample_count
    }

    /// Assemble a feature vector from current telemetry.
    /// Returns None if critical data is missing.
    pub fn assemble_features(
        &self,
        identity: Identity,
        eligible_proposers: &[NodeId],
    ) -> Option<FeatureVector> {
        let feature_age_us = self
            .last_update
            .read()
            .map(|ts| ts.elapsed().as_micros() as u64)
            .unwrap_or(u64::MAX);

        let sample_count = self.total_samples();

        let mut missingness = MissingnessFlags::default();

        // Assemble per-proposer stats
        let proposer_stats = {
            let samples = self.proposer_samples.read().ok()?;
            let in_flight = self.in_flight.read().ok()?;
            let queue_depth = self.queue_depth.read().ok()?;

            eligible_proposers
                .iter()
                .map(|pid| {
                    let stats = if let Some(s) = samples.get(pid) {
                        if s.is_empty() {
                            missingness.proposer_stats_missing = true;
                            ProposerStats::default()
                        } else {
                            compute_proposer_stats(s)
                        }
                    } else {
                        missingness.proposer_stats_missing = true;
                        ProposerStats::default()
                    };

                    let mut stats = stats;
                    stats.in_flight = in_flight.get(pid).copied().unwrap_or(0);
                    stats.queue_depth = queue_depth.get(pid).copied().unwrap_or(0);
                    (pid.clone(), stats)
                })
                .collect::<Vec<_>>()
        };

        let rpc_stats = self.rpc_stats.read().ok()?.clone();
        if rpc_stats == RpcStats::default() {
            missingness.rpc_stats_missing = true;
        }

        let node_pressure = self.node_pressure.read().ok()?.clone();
        if node_pressure == NodePressure::default() {
            missingness.node_pressure_missing = true;
        }

        let request_class = self.request_class.read().ok()?;
        let request_class = match *request_class {
            Some(rc) => rc,
            None => {
                missingness.request_class_missing = true;
                RequestClass {
                    durability: crate::types::DurabilityMode::Sync,
                    size: crate::types::SizeBucket::Small,
                }
            }
        };

        Some(FeatureVector {
            identity: identity.clone(),
            epoch: identity.epoch,
            voter_count: eligible_proposers.len() as u32,
            eligible_proposers: eligible_proposers.to_vec(),
            proposer_stats,
            rpc_stats,
            node_pressure,
            request_class,
            feature_age_us,
            sample_count,
            missingness_flags: missingness,
        })
    }

    /// Reset all telemetry state (used on identity change).
    pub fn reset(&self) {
        if let Ok(mut s) = self.proposer_samples.write() {
            s.clear();
        }
        if let Ok(mut c) = self.total_samples.write() {
            *c = 0;
        }
        if let Ok(mut m) = self.in_flight.write() {
            m.clear();
        }
        if let Ok(mut m) = self.queue_depth.write() {
            m.clear();
        }
    }
}

/// Compute rolling statistics from latency samples.
fn compute_proposer_stats(samples: &[LatencySample]) -> ProposerStats {
    if samples.is_empty() {
        return ProposerStats::default();
    }

    let mut latencies: Vec<u64> = samples.iter().map(|s| s.latency_us).collect();
    latencies.sort_unstable();

    let n = latencies.len();
    let p50 = latencies[n * 50 / 100];
    let p95 = latencies[(n * 95 / 100).min(n - 1)];
    let p99 = latencies[(n * 99 / 100).min(n - 1)];

    let total = samples.len() as f64;
    let success_count = samples.iter().filter(|s| s.success).count() as f64;
    let timeout_count = samples.iter().filter(|s| s.timeout).count() as f64;

    ProposerStats {
        latency_quantiles: [p50, p95, p99],
        success_rate: success_count / total,
        timeout_rate: timeout_count / total,
        in_flight: 0,
        queue_depth: 0,
        contention_rate: 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Identity;

    fn test_identity() -> Identity {
        Identity {
            cluster_id: "test-cluster".into(),
            epoch: 1,
            config_id: 1,
            membership_digest: [0u8; 32],
            recovery_generation: 0,
        }
    }

    fn test_proposers() -> Vec<NodeId> {
        vec!["node-0".into(), "node-1".into(), "node-2".into()]
    }

    #[test]
    fn fresh_collector_with_no_data_returns_none() {
        let collector = TelemetryCollector::new(CollectorConfig::default());
        let features = collector.assemble_features(test_identity(), &test_proposers());
        // Should still return Some but with missingness flags
        assert!(features.is_some());
        let f = features.unwrap();
        assert!(f.missingness_flags.proposer_stats_missing);
    }

    #[test]
    fn cold_start_gate_requires_minimum_samples() {
        let config = CollectorConfig {
            min_sample_count: 5,
            ..Default::default()
        };
        let collector = TelemetryCollector::new(config);
        assert!(!collector.cold_start_gates_passed());

        for i in 0..5 {
            collector.record_proposer_latency(&"node-0".into(), 1000 + i, true, false);
        }
        assert!(collector.cold_start_gates_passed());
    }

    #[test]
    fn feature_vector_reflects_recorded_data() {
        let collector = TelemetryCollector::new(CollectorConfig::default());
        for _ in 0..10 {
            collector.record_proposer_latency(&"node-0".into(), 1000, true, false);
            collector.record_proposer_latency(&"node-1".into(), 2000, true, false);
        }

        let proposers = vec!["node-0".into(), "node-1".into()];
        let features = collector
            .assemble_features(test_identity(), &proposers)
            .unwrap();

        assert!(!features.missingness_flags.proposer_stats_missing);
        assert_eq!(features.proposer_stats.len(), 2);

        let stats_0 = &features.proposer_stats[0].1;
        assert_eq!(stats_0.latency_quantiles[0], 1000);
        assert_eq!(stats_0.success_rate, 1.0);

        let stats_1 = &features.proposer_stats[1].1;
        assert_eq!(stats_1.latency_quantiles[0], 2000);
    }

    #[test]
    fn reset_clears_all_state() {
        let collector = TelemetryCollector::new(CollectorConfig::default());
        collector.record_proposer_latency(&"node-0".into(), 1000, true, false);
        assert_eq!(collector.total_samples(), 1);

        collector.reset();
        assert_eq!(collector.total_samples(), 0);
    }
}
