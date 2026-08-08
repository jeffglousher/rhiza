use std::sync::{Arc, Mutex};

use crate::types::{ActionRecord, RewardComponents, TerminalOutcome};

/// Observability configuration.
#[derive(Clone, Debug)]
pub struct ObservabilityConfig {
    /// Maximum number of action records to retain in memory.
    pub max_records: usize,
    /// Whether to log action records via tracing.
    pub enable_tracing: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            max_records: 10_000,
            enable_tracing: true,
        }
    }
}

/// Aggregate metrics for dashboard comparison (tuned vs baseline cohorts).
#[derive(Clone, Debug, Default)]
pub struct AggregateMetrics {
    /// Total actions selected.
    pub total_actions: u64,
    /// Actions that used the tuned model.
    pub tuned_actions: u64,
    /// Actions that fell back to static.
    pub fallback_actions: u64,
    /// Exploratory actions.
    pub exploratory_actions: u64,
    /// Total fallbacks by reason.
    pub fallback_reasons: std::collections::HashMap<String, u64>,
    /// Sum of decision latencies (for computing p50/p95/p99 approximations).
    pub latency_sum_us: u64,
    /// Count of latency observations.
    pub latency_count: u64,
    /// Sum of additional RPC counts.
    pub additional_rpc_sum: u64,
    /// Count of duplicate work occurrences.
    pub duplicate_work_count: u64,
    /// Count of requests with contention/round escalation.
    pub contention_count: u64,
    /// Count of terminal errors.
    pub error_count: u64,
    /// Count of censored outcomes.
    pub censored_count: u64,
    /// Sum of rewards.
    pub reward_sum: f64,
}

impl AggregateMetrics {
    pub fn fallback_rate(&self) -> f64 {
        if self.total_actions == 0 {
            0.0
        } else {
            self.fallback_actions as f64 / self.total_actions as f64
        }
    }

    pub fn exploration_rate(&self) -> f64 {
        if self.total_actions == 0 {
            0.0
        } else {
            self.exploratory_actions as f64 / self.total_actions as f64
        }
    }

    pub fn mean_reward(&self) -> f64 {
        if self.total_actions == 0 {
            0.0
        } else {
            self.reward_sum / self.total_actions as f64
        }
    }
}

/// Observability system emitting action records keyed by request correlation ID.
///
/// Does not log command payloads or unbounded feature vectors.
pub struct Observability {
    config: ObservabilityConfig,
    records: Arc<Mutex<Vec<ActionRecord>>>,
    metrics: Arc<Mutex<AggregateMetrics>>,
}

impl Observability {
    pub fn new(config: ObservabilityConfig) -> Self {
        Self {
            config,
            records: Arc::new(Mutex::new(Vec::new())),
            metrics: Arc::new(Mutex::new(AggregateMetrics::default())),
        }
    }

    /// Record an action selection.
    pub fn record_action(&self, record: ActionRecord) {
        // Update aggregate metrics
        if let Ok(mut m) = self.metrics.lock() {
            m.total_actions += 1;
            if let Some(reason) = &record.fallback_reason {
                m.fallback_actions += 1;
                let reason = format!("{reason:?}");
                *m.fallback_reasons.entry(reason).or_insert(0) += 1;
            } else {
                m.tuned_actions += 1;
            }
            if record.exploration {
                m.exploratory_actions += 1;
            }
            if let Some(latency) = record.latency_us {
                m.latency_sum_us += latency;
                m.latency_count += 1;
            }
        }

        // Log via tracing if enabled
        if self.config.enable_tracing {
            tracing::debug!(
                correlation_id = %record.correlation_id,
                epoch = record.configuration_epoch,
                proposer = %record.chosen_proposer,
                delay = ?record.chosen_delay,
                exploration = record.exploration,
                confidence = record.confidence,
                fallback = ?record.fallback_reason,
                "tuner action selected"
            );
        }

        // Store record (bounded)
        if let Ok(mut records) = self.records.lock() {
            if records.len() >= self.config.max_records {
                records.remove(0);
            }
            records.push(record);
        }
    }

    /// Record a terminal outcome and update metrics.
    pub fn record_outcome(
        &self,
        correlation_id: &str,
        outcome: &TerminalOutcome,
        reward: &RewardComponents,
    ) {
        if let Ok(mut m) = self.metrics.lock() {
            m.reward_sum += reward.total;
            match outcome {
                TerminalOutcome::Success {
                    additional_rpcs,
                    duplicate_proposer_work,
                    contention_or_round_escalation,
                    ..
                } => {
                    m.additional_rpc_sum += *additional_rpcs as u64;
                    if *duplicate_proposer_work {
                        m.duplicate_work_count += 1;
                    }
                    if *contention_or_round_escalation {
                        m.contention_count += 1;
                    }
                }
                TerminalOutcome::Timeout | TerminalOutcome::Error { .. } => {
                    m.error_count += 1;
                }
                TerminalOutcome::Censored => {
                    m.censored_count += 1;
                }
            }
        }

        // Update the matching action record
        if let Ok(mut records) = self.records.lock() {
            if let Some(record) = records
                .iter_mut()
                .rfind(|r| r.correlation_id == correlation_id)
            {
                record.terminal_outcome = Some(outcome.clone());
                record.reward_components = Some(reward.clone());
            }
        }

        if self.config.enable_tracing {
            tracing::debug!(
                correlation_id = %correlation_id,
                reward_total = reward.total,
                censored = reward.censored,
                "tuner outcome recorded"
            );
        }
    }

    /// Get current aggregate metrics.
    pub fn metrics(&self) -> AggregateMetrics {
        self.metrics.lock().map(|m| m.clone()).unwrap_or_default()
    }

    /// Get recent action records.
    pub fn recent_records(&self, limit: usize) -> Vec<ActionRecord> {
        self.records
            .lock()
            .map(|records| {
                let start = records.len().saturating_sub(limit);
                records[start..].to_vec()
            })
            .unwrap_or_default()
    }

    /// Get the count of recorded actions.
    pub fn record_count(&self) -> usize {
        self.records.lock().map(|r| r.len()).unwrap_or(0)
    }

    /// Clear all records and reset metrics.
    pub fn clear(&self) {
        if let Ok(mut records) = self.records.lock() {
            records.clear();
        }
        if let Ok(mut metrics) = self.metrics.lock() {
            *metrics = AggregateMetrics::default();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FallbackReason, HedgeDelayBucket, TerminalOutcome};

    fn test_record(correlation_id: &str) -> ActionRecord {
        ActionRecord {
            correlation_id: correlation_id.into(),
            configuration_epoch: 1,
            feature_version: 1,
            model_version: 1,
            chosen_proposer: "node-0".into(),
            chosen_delay: HedgeDelayBucket::Ms10,
            baseline_proposer: "node-0".into(),
            baseline_delay: HedgeDelayBucket::Static,
            exploration: false,
            confidence: 0.8,
            fallback_reason: None,
            terminal_outcome: None,
            latency_us: Some(10_000),
            duplicate_work: false,
            reward_components: None,
        }
    }

    #[test]
    fn record_action_updates_metrics() {
        let obs = Observability::new(ObservabilityConfig::default());
        obs.record_action(test_record("req-1"));
        obs.record_action(test_record("req-2"));

        let metrics = obs.metrics();
        assert_eq!(metrics.total_actions, 2);
        assert_eq!(metrics.tuned_actions, 2);
        assert_eq!(metrics.fallback_actions, 0);
    }

    #[test]
    fn fallback_record_tracked() {
        let obs = Observability::new(ObservabilityConfig::default());
        let mut record = test_record("req-1");
        record.fallback_reason = Some(FallbackReason::StaleFeatures);
        obs.record_action(record);

        let metrics = obs.metrics();
        assert_eq!(metrics.fallback_actions, 1);
    }

    #[test]
    fn record_outcome_updates_metrics() {
        let obs = Observability::new(ObservabilityConfig::default());
        obs.record_action(test_record("req-1"));

        let outcome = TerminalOutcome::Success {
            decision_latency_us: 5_000,
            additional_rpcs: 1,
            duplicate_proposer_work: false,
            contention_or_round_escalation: false,
        };
        let reward = RewardComponents {
            normalized_latency: -0.5,
            rpc_penalty: -0.05,
            work_penalty: 0.0,
            contention_penalty: 0.0,
            error_penalty: 0.0,
            slo_penalty: 0.0,
            total: -0.55,
            censored: false,
        };
        obs.record_outcome("req-1", &outcome, &reward);

        let metrics = obs.metrics();
        assert!((metrics.reward_sum - (-0.55)).abs() < 0.001);
    }

    #[test]
    fn bounded_records_evict_oldest() {
        let config = ObservabilityConfig {
            max_records: 3,
            enable_tracing: false,
        };
        let obs = Observability::new(config);
        for i in 0..5 {
            obs.record_action(test_record(&format!("req-{i}")));
        }
        assert_eq!(obs.record_count(), 3);
        let records = obs.recent_records(10);
        assert_eq!(records[0].correlation_id, "req-2");
        assert_eq!(records[2].correlation_id, "req-4");
    }
}
