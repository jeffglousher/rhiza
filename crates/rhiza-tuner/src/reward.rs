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
    /// SLO latency threshold in microseconds.
    pub slo_latency_us: u64,
    /// Exponential penalty factor when exceeding SLO.
    pub slo_exponential_factor: f64,
    /// Weight for SLO violation penalty.
    pub lambda_slo: f64,
    /// Exponential latency normalization factor.
    pub latency_base_us: f64,
    /// Time decay factor for exponential moving average (0.0 to 1.0).
    pub time_decay_factor: f64,
    /// Minimum reward floor to prevent extreme negative values.
    pub reward_floor: f64,
    /// Maximum reward ceiling to prevent extreme positive values.
    pub reward_ceiling: f64,
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
            slo_latency_us: 50_000,    // 50ms SLO
            slo_exponential_factor: 2.0,
            lambda_slo: 0.2,
            latency_base_us: 1_000.0,  // 1ms base for log normalization
            time_decay_factor: 0.95,
            reward_floor: -3.0,
            reward_ceiling: 0.0,
        }
    }
}

/// Reward pipeline scores terminal observations with improved reward shaping.
pub struct RewardPipeline {
    config: RewardConfig,
    ema_reward: f64,
    observation_count: u64,
}

impl RewardPipeline {
    pub fn new(config: RewardConfig) -> Self {
        Self {
            config,
            ema_reward: 0.0,
            observation_count: 0,
        }
    }

    pub fn compute(&mut self, outcome: &TerminalOutcome) -> RewardComponents {
        let raw_reward = match outcome {
            TerminalOutcome::Success {
                decision_latency_us,
                additional_rpcs,
                duplicate_proposer_work,
                contention_or_round_escalation,
            } => {
                self.compute_success_reward(
                    *decision_latency_us,
                    *additional_rpcs,
                    *duplicate_proposer_work,
                    *contention_or_round_escalation,
                )
            }
            TerminalOutcome::Timeout => self.compute_timeout_reward(),
            TerminalOutcome::Error {
                additional_rpcs,
                duplicate_proposer_work,
            } => self.compute_error_reward(*additional_rpcs, *duplicate_proposer_work),
            TerminalOutcome::Censored => self.compute_censored_reward(),
        };

        self.observation_count += 1;
        if self.observation_count == 1 {
            self.ema_reward = raw_reward.total;
        } else {
            self.ema_reward = self.config.time_decay_factor * raw_reward.total
                + (1.0 - self.config.time_decay_factor) * self.ema_reward;
        }

        raw_reward
    }

    fn compute_success_reward(
        &self,
        decision_latency_us: u64,
        additional_rpcs: u32,
        duplicate_proposer_work: bool,
        contention_or_round_escalation: bool,
    ) -> RewardComponents {
        let capped_latency = decision_latency_us.min(self.config.latency_cap_us);

        // Log-normalized latency
        let normalized_latency = if capped_latency == 0 {
            0.0
        } else {
            let log_latency = (1.0 + capped_latency as f64 / self.config.latency_base_us).ln();
            let log_cap = (1.0 + self.config.latency_cap_us as f64 / self.config.latency_base_us).ln();
            -(log_latency / log_cap)
        };

        // SLO-aware exponential penalty
        let slo_penalty = if capped_latency > self.config.slo_latency_us {
            let excess_ratio = (capped_latency - self.config.slo_latency_us) as f64
                / self.config.slo_latency_us as f64;
            -self.config.lambda_slo * (excess_ratio * self.config.slo_exponential_factor).exp()
        } else {
            0.0
        };

        let rpc_penalty = -self.config.lambda_rpc * (additional_rpcs as f64);

        let work_penalty = if duplicate_proposer_work {
            -self.config.lambda_work * (1.0 + additional_rpcs as f64 * 0.1)
        } else {
            0.0
        };

        let contention_penalty = if contention_or_round_escalation {
            let base_penalty = -self.config.lambda_contend;
            if capped_latency > self.config.slo_latency_us / 2 {
                base_penalty * 1.5
            } else {
                base_penalty
            }
        } else {
            0.0
        };

        let total = normalized_latency
            + slo_penalty
            + rpc_penalty
            + work_penalty
            + contention_penalty;

        RewardComponents {
            normalized_latency,
            rpc_penalty,
            work_penalty,
            contention_penalty,
            error_penalty: 0.0,
            slo_penalty,
            total: total.clamp(self.config.reward_floor, self.config.reward_ceiling),
            censored: false,
        }
    }

    fn compute_timeout_reward(&self) -> RewardComponents {
        let normalized_latency = -1.0;
        let slo_penalty = -self.config.lambda_slo;
        let total = normalized_latency + slo_penalty - self.config.lambda_error;

        RewardComponents {
            normalized_latency,
            rpc_penalty: 0.0,
            work_penalty: 0.0,
            contention_penalty: 0.0,
            error_penalty: -self.config.lambda_error,
            slo_penalty,
            total: total.clamp(self.config.reward_floor, self.config.reward_ceiling),
            censored: false,
        }
    }

    fn compute_error_reward(
        &self,
        additional_rpcs: u32,
        duplicate_proposer_work: bool,
    ) -> RewardComponents {
        let rpc_penalty = -self.config.lambda_rpc * (additional_rpcs as f64);
        let work_penalty = if duplicate_proposer_work {
            -self.config.lambda_work
        } else {
            0.0
        };

        let total = rpc_penalty + work_penalty - self.config.lambda_error;

        RewardComponents {
            normalized_latency: 0.0,
            rpc_penalty,
            work_penalty,
            contention_penalty: 0.0,
            error_penalty: -self.config.lambda_error,
            slo_penalty: 0.0,
            total: total.clamp(self.config.reward_floor, self.config.reward_ceiling),
            censored: false,
        }
    }

    fn compute_censored_reward(&self) -> RewardComponents {
        let adaptive_penalty = if self.observation_count > 10 {
            self.config.censored_penalty * (1.0 + self.ema_reward.abs() * 0.1)
        } else {
            self.config.censored_penalty
        };

        RewardComponents {
            normalized_latency: 0.0,
            rpc_penalty: 0.0,
            work_penalty: 0.0,
            contention_penalty: 0.0,
            error_penalty: 0.0,
            slo_penalty: 0.0,
            total: adaptive_penalty.clamp(self.config.reward_floor, self.config.reward_ceiling),
            censored: true,
        }
    }

    pub fn config(&self) -> &RewardConfig {
        &self.config
    }

    pub fn ema_reward(&self) -> f64 {
        self.ema_reward
    }

    pub fn observation_count(&self) -> u64 {
        self.observation_count
    }

    pub fn reset(&mut self) {
        self.ema_reward = 0.0;
        self.observation_count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_reward_reflects_log_latency_normalization() {
        let mut pipeline = RewardPipeline::new(RewardConfig::default());
        let outcome = TerminalOutcome::Success {
            decision_latency_us: 10_000,
            additional_rpcs: 2,
            duplicate_proposer_work: true,
            contention_or_round_escalation: false,
        };

        let reward = pipeline.compute(&outcome);
        assert!(!reward.censored);
        assert!(reward.normalized_latency < 0.0);
        assert!(reward.normalized_latency > -1.0);
        assert!((reward.rpc_penalty - (-0.1)).abs() < 0.001);
        assert!(reward.work_penalty < 0.0);
    }

    #[test]
    fn timeout_has_max_penalty() {
        let mut pipeline = RewardPipeline::new(RewardConfig::default());
        let reward = pipeline.compute(&TerminalOutcome::Timeout);
        assert!(!reward.censored);
        assert_eq!(reward.normalized_latency, -1.0);
        assert_eq!(reward.error_penalty, -1.0);
        assert_eq!(reward.slo_penalty, -0.2);
    }

    #[test]
    fn censored_gets_adaptive_penalty() {
        let mut pipeline = RewardPipeline::new(RewardConfig::default());
        let reward = pipeline.compute(&TerminalOutcome::Censored);
        assert!(reward.censored);
        assert!(reward.total < 0.0);
        assert!(reward.total > -2.0);
    }

    #[test]
    fn slo_penalty_increases_exponentially() {
        let config = RewardConfig {
            slo_latency_us: 10_000,
            slo_exponential_factor: 1.5,
            lambda_slo: 0.1,
            ..Default::default()
        };
        let mut pipeline = RewardPipeline::new(config);

        let under = pipeline.compute(&TerminalOutcome::Success {
            decision_latency_us: 9_000,
            additional_rpcs: 0,
            duplicate_proposer_work: false,
            contention_or_round_escalation: false,
        });
        assert_eq!(under.slo_penalty, 0.0);

        let over1 = pipeline.compute(&TerminalOutcome::Success {
            decision_latency_us: 11_000,
            additional_rpcs: 0,
            duplicate_proposer_work: false,
            contention_or_round_escalation: false,
        });
        assert!(over1.slo_penalty < 0.0);

        let over2 = pipeline.compute(&TerminalOutcome::Success {
            decision_latency_us: 20_000,
            additional_rpcs: 0,
            duplicate_proposer_work: false,
            contention_or_round_escalation: false,
        });
        assert!(over2.slo_penalty < over1.slo_penalty);
    }

    #[test]
    fn ema_tracks_recent_performance() {
        let mut pipeline = RewardPipeline::new(RewardConfig::default());

        pipeline.compute(&TerminalOutcome::Success {
            decision_latency_us: 10_000,
            additional_rpcs: 0,
            duplicate_proposer_work: false,
            contention_or_round_escalation: false,
        });
        let ema1 = pipeline.ema_reward();

        pipeline.compute(&TerminalOutcome::Success {
            decision_latency_us: 50_000,
            additional_rpcs: 0,
            duplicate_proposer_work: false,
            contention_or_round_escalation: false,
        });
        let ema2 = pipeline.ema_reward();

        assert!(ema2 < ema1);
    }
}
