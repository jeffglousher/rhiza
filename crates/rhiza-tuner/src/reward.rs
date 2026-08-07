use crate::types::{RewardComponents, TerminalOutcome};

/// Reward computation configuration.
/// Coefficients are versioned configuration.
#[derive(Clone, Debug)]
pub struct RewardConfig {
    /// Weight for additional RPC count penalty.
    pub lambda_rpc: f64,
    /// Weight for duplicate proposer work penalty.
    pub lambda_work: f64,
    /// Weight for contention or round escalation penalty.
    pub lambda_contend: f64,
    /// Weight for terminal error penalty.
    pub lambda_error: f64,
    /// Maximum latency for training robustness (microseconds).
    /// Latencies above this are capped.
    pub latency_cap_us: u64,
    /// Penalty applied to censored outcomes.
    pub censored_penalty: f64,
}

impl Default for RewardConfig {
    fn default() -> Self {
        Self {
            lambda_rpc: 0.05,
            lambda_work: 0.1,
            lambda_contend: 0.15,
            lambda_error: 1.0,
            latency_cap_us: 1_000_000, // 1 second
            censored_penalty: -0.5,
        }
    }
}

/// Reward pipeline scores terminal observations.
pub struct RewardPipeline {
    config: RewardConfig,
}

impl RewardPipeline {
    pub fn new(config: RewardConfig) -> Self {
        Self { config }
    }

    /// Compute reward for a terminal outcome.
    ///
    /// reward = -normalized(decision_latency)
    ///          - lambda_rpc * additional_rpc_count
    ///          - lambda_work * duplicate_proposer_work
    ///          - lambda_contend * contention_or_round_escalation
    ///          - lambda_error * terminal_error
    ///
    /// Censored outcomes get a conservative penalty.
    pub fn compute(&self, outcome: &TerminalOutcome) -> RewardComponents {
        match outcome {
            TerminalOutcome::Success {
                decision_latency_us,
                additional_rpcs,
                duplicate_proposer_work,
                contention_or_round_escalation,
            } => {
                let capped_latency = (*decision_latency_us).min(self.config.latency_cap_us);
                let normalized_latency =
                    -(capped_latency as f64 / self.config.latency_cap_us as f64);

                let rpc_penalty = -self.config.lambda_rpc * (*additional_rpcs as f64);
                let work_penalty = if *duplicate_proposer_work {
                    -self.config.lambda_work
                } else {
                    0.0
                };
                let contention_penalty = if *contention_or_round_escalation {
                    -self.config.lambda_contend
                } else {
                    0.0
                };

                RewardComponents {
                    normalized_latency,
                    rpc_penalty,
                    work_penalty,
                    contention_penalty,
                    error_penalty: 0.0,
                    total: normalized_latency + rpc_penalty + work_penalty + contention_penalty,
                    censored: false,
                }
            }
            TerminalOutcome::Timeout => RewardComponents {
                normalized_latency: -1.0,
                rpc_penalty: 0.0,
                work_penalty: 0.0,
                contention_penalty: 0.0,
                error_penalty: -self.config.lambda_error,
                total: -1.0 - self.config.lambda_error,
                censored: false,
            },
            TerminalOutcome::Error {
                additional_rpcs,
                duplicate_proposer_work,
            } => {
                let rpc_penalty = -self.config.lambda_rpc * (*additional_rpcs as f64);
                let work_penalty = if *duplicate_proposer_work {
                    -self.config.lambda_work
                } else {
                    0.0
                };

                RewardComponents {
                    normalized_latency: 0.0,
                    rpc_penalty,
                    work_penalty,
                    contention_penalty: 0.0,
                    error_penalty: -self.config.lambda_error,
                    total: rpc_penalty + work_penalty - self.config.lambda_error,
                    censored: false,
                }
            }
            TerminalOutcome::Censored => RewardComponents {
                normalized_latency: 0.0,
                rpc_penalty: 0.0,
                work_penalty: 0.0,
                contention_penalty: 0.0,
                error_penalty: 0.0,
                total: self.config.censored_penalty,
                censored: true,
            },
        }
    }

    /// Get the current config.
    pub fn config(&self) -> &RewardConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_reward_reflects_latency_and_penalties() {
        let pipeline = RewardPipeline::new(RewardConfig::default());
        let outcome = TerminalOutcome::Success {
            decision_latency_us: 10_000, // 10ms
            additional_rpcs: 2,
            duplicate_proposer_work: true,
            contention_or_round_escalation: false,
        };

        let reward = pipeline.compute(&outcome);
        assert!(!reward.censored);
        // normalized latency: -(10000/1000000) = -0.01
        assert!((reward.normalized_latency - (-0.01)).abs() < 0.001);
        // rpc penalty: -0.05 * 2 = -0.1
        assert!((reward.rpc_penalty - (-0.1)).abs() < 0.001);
        // work penalty: -0.1
        assert!((reward.work_penalty - (-0.1)).abs() < 0.001);
        assert_eq!(reward.contention_penalty, 0.0);
        assert_eq!(reward.error_penalty, 0.0);
    }

    #[test]
    fn timeout_has_max_penalty() {
        let pipeline = RewardPipeline::new(RewardConfig::default());
        let reward = pipeline.compute(&TerminalOutcome::Timeout);
        assert!(!reward.censored);
        assert_eq!(reward.normalized_latency, -1.0);
        assert_eq!(reward.error_penalty, -1.0);
        assert_eq!(reward.total, -2.0);
    }

    #[test]
    fn censored_gets_conservative_penalty() {
        let pipeline = RewardPipeline::new(RewardConfig::default());
        let reward = pipeline.compute(&TerminalOutcome::Censored);
        assert!(reward.censored);
        assert_eq!(reward.total, -0.5);
    }

    #[test]
    fn latency_is_capped() {
        let pipeline = RewardPipeline::new(RewardConfig {
            latency_cap_us: 50_000,
            ..Default::default()
        });
        let outcome = TerminalOutcome::Success {
            decision_latency_us: 100_000, // above cap
            additional_rpcs: 0,
            duplicate_proposer_work: false,
            contention_or_round_escalation: false,
        };
        let reward = pipeline.compute(&outcome);
        // Should be capped at -1.0
        assert!((reward.normalized_latency - (-1.0)).abs() < 0.001);
    }
}
