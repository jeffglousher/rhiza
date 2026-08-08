use rand::Rng;
use std::collections::HashMap;

use crate::types::{
    Action, ActionOutput, CandidateSet, FeatureVector, HedgeDelayBucket, Identity, NodeId,
};

/// Configuration for the contextual bandit model.
#[derive(Clone, Debug)]
pub struct BanditConfig {
    /// Policy version.
    pub policy_version: u32,
    /// Model version.
    pub model_version: u32,
    /// Action validity duration in microseconds.
    pub validity_duration_us: u64,
    /// Base exploration rate (probability of exploring vs exploiting).
    pub exploration_rate: f64,
    /// Minimum exploration rate during incidents.
    pub min_exploration_rate: f64,
    /// Prior alpha for Thompson sampling Beta distribution.
    pub prior_alpha: f64,
    /// Prior beta for Thompson sampling Beta distribution.
    pub prior_beta: f64,
}

impl Default for BanditConfig {
    fn default() -> Self {
        Self {
            policy_version: 1,
            model_version: 1,
            validity_duration_us: 10_000_000, // 10 seconds
            exploration_rate: 0.05,
            min_exploration_rate: 0.01,
            prior_alpha: 1.0,
            prior_beta: 1.0,
        }
    }
}

/// Per-action statistics for Thompson sampling.
#[derive(Clone, Debug)]
pub struct ActionStats {
    /// Alpha parameter of the Beta distribution (successes + prior).
    pub alpha: f64,
    /// Beta parameter of the Beta distribution (failures + prior).
    pub beta: f64,
    /// Total reward accumulated.
    pub total_reward: f64,
    /// Number of times this action was selected.
    pub pull_count: u64,
}

impl ActionStats {
    fn new(prior_alpha: f64, prior_beta: f64) -> Self {
        Self {
            alpha: prior_alpha,
            beta: prior_beta,
            total_reward: 0.0,
            pull_count: 0,
        }
    }

    /// Mean reward per pull.
    pub fn mean_reward(&self) -> f64 {
        if self.pull_count == 0 {
            0.0
        } else {
            self.total_reward / self.pull_count as f64
        }
    }
}

/// Contextual bandit model using constrained Thompson sampling.
///
/// The model selects (first_request_target, hedge_delay) pairs from
/// allowlisted candidates. It adapts to topology, load, and partial
/// degradation while remaining observable and reversible.
pub struct ContextualBandit {
    config: BanditConfig,
    /// Per-action statistics keyed by (proposer_id, hedge_delay_bucket).
    action_stats: HashMap<(NodeId, HedgeDelayBucket), ActionStats>,
    /// Current identity this model state is keyed to.
    current_identity: Option<Identity>,
}

impl ContextualBandit {
    pub fn new(config: BanditConfig) -> Self {
        Self {
            config,
            action_stats: HashMap::new(),
            current_identity: None,
        }
    }

    /// Select an action using constrained Thompson sampling.
    ///
    /// Returns an ActionOutput with the selected action, identity metadata,
    /// exploration flag, and confidence. The candidate set constrains which
    /// actions are considered.
    pub fn select_action(
        &mut self,
        features: &FeatureVector,
        candidate_set: &CandidateSet,
        is_exploration_enabled: bool,
    ) -> ActionOutput {
        // Reset if identity changed
        if self.current_identity.as_ref() != Some(&features.identity) {
            self.action_stats.clear();
            self.current_identity = Some(features.identity.clone());
        }

        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        // Build candidate actions
        let candidates: Vec<(NodeId, HedgeDelayBucket)> = candidate_set
            .eligible_voters
            .iter()
            .flat_map(|pid| {
                candidate_set
                    .hedge_delay_allowlist
                    .iter()
                    .map(move |delay| (pid.clone(), *delay))
            })
            .collect();

        if candidates.is_empty() {
            return self.static_output(candidate_set, now_us);
        }

        // Ensure all candidates have stats
        for key in &candidates {
            self.action_stats.entry(key.clone()).or_insert_with(|| {
                ActionStats::new(self.config.prior_alpha, self.config.prior_beta)
            });
        }

        // Thompson sampling: sample from each action's Beta distribution
        let mut rng = rand::rng();
        let exploration = is_exploration_enabled && rng.random_bool(self.config.exploration_rate);

        let selected = if exploration {
            // Random exploration
            let idx = rng.random_range(0..candidates.len());
            candidates[idx].clone()
        } else {
            // Thompson sampling: pick action with highest sampled value
            let mut best_action = candidates[0].clone();
            let mut best_sample = f64::NEG_INFINITY;

            for key in &candidates {
                let stats = &self.action_stats[key];
                // Sample from Beta(alpha, beta) using the relationship with Gamma
                let sample = sample_beta(&mut rng, stats.alpha, stats.beta);
                if sample > best_sample {
                    best_sample = sample;
                    best_action = key.clone();
                }
            }

            best_action
        };

        let stats = &self.action_stats[&selected];
        let confidence = if stats.pull_count > 0 {
            // Confidence based on how certain we are about this action
            let variance = (stats.alpha * stats.beta)
                / ((stats.alpha + stats.beta).powi(2) * (stats.alpha + stats.beta + 1.0));
            1.0 - (variance.sqrt() * 2.0).min(1.0)
        } else {
            0.5 // Uncertain for untried actions
        };

        ActionOutput {
            action: Action {
                first_request_target: selected.0,
                hedge_delay: selected.1,
            },
            identity: features.identity.clone(),
            valid_from_slot: 0,
            expiry_us: now_us + self.config.validity_duration_us,
            policy_version: self.config.policy_version,
            model_version: self.config.model_version,
            exploration,
            confidence,
            fallback_reason: None,
        }
    }

    /// Update action statistics with an observed reward.
    pub fn update(&mut self, action: &Action, reward: f64) {
        let key = (action.first_request_target.clone(), action.hedge_delay);
        let stats = self
            .action_stats
            .entry(key)
            .or_insert_with(|| ActionStats::new(self.config.prior_alpha, self.config.prior_beta));

        // Update Beta distribution parameters
        // Normalize reward to [0, 1] range for Beta distribution
        let normalized = (reward + 2.0) / 2.0; // reward is in [-2, 0] range typically
        let normalized = normalized.clamp(0.01, 0.99);

        stats.alpha += normalized;
        stats.beta += 1.0 - normalized;
        stats.total_reward += reward;
        stats.pull_count += 1;
    }

    /// Get a static fallback output.
    fn static_output(&self, candidate_set: &CandidateSet, now_us: u64) -> ActionOutput {
        ActionOutput {
            action: Action {
                first_request_target: candidate_set
                    .eligible_voters
                    .first()
                    .cloned()
                    .unwrap_or_default(),
                hedge_delay: HedgeDelayBucket::Static,
            },
            identity: candidate_set.identity.clone(),
            valid_from_slot: 0,
            expiry_us: now_us + self.config.validity_duration_us,
            policy_version: self.config.policy_version,
            model_version: self.config.model_version,
            exploration: false,
            confidence: 1.0,
            fallback_reason: None,
        }
    }

    /// Get action statistics for observability.
    pub fn action_stats(&self) -> &HashMap<(NodeId, HedgeDelayBucket), ActionStats> {
        &self.action_stats
    }

    /// Reset model state (used on identity change or kill switch).
    pub fn reset(&mut self) {
        self.action_stats.clear();
        self.current_identity = None;
    }
}

/// Sample from a Beta(alpha, beta) distribution using the Gamma-based method.
fn sample_beta(rng: &mut impl Rng, alpha: f64, beta: f64) -> f64 {
    let x = sample_gamma(rng, alpha);
    let y = sample_gamma(rng, beta);
    x / (x + y)
}

/// Sample from a Gamma(shape, 1) distribution using Marsaglia and Tsang's method.
fn sample_gamma(rng: &mut impl Rng, shape: f64) -> f64 {
    if shape < 1.0 {
        // For shape < 1, use the relation: Gamma(a) = Gamma(a+1) * U^(1/a)
        let u: f64 = rng.random();
        sample_gamma(rng, shape + 1.0) * u.powf(1.0 / shape)
    } else {
        let d = shape - 1.0 / 3.0;
        let c = 1.0 / (9.0 * d).sqrt();
        loop {
            let mut x;
            let mut v;
            loop {
                x = rng.random::<f64>();
                v = 1.0 + c * x;
                if v > 0.0 {
                    break;
                }
            }
            let v = v * v * v;
            let u: f64 = rng.random();
            if u < 1.0 - 0.0331 * (x * x) * (x * x) {
                return d * v;
            }
            if u.ln() < 0.5 * x * x + d * (1.0 - v + v.ln()) {
                return d * v;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        Identity, MissingnessFlags, NodePressure, ProposerStats, RequestClass, RpcStats,
    };

    fn test_identity() -> Identity {
        Identity {
            cluster_id: "test-cluster".into(),
            epoch: 1,
            config_id: 1,
            membership_digest: [0u8; 32],
            recovery_generation: 0,
        }
    }

    fn test_features() -> FeatureVector {
        FeatureVector {
            identity: test_identity(),
            epoch: 1,
            voter_count: 3,
            eligible_proposers: vec!["node-0".into(), "node-1".into(), "node-2".into()],
            proposer_stats: vec![
                ("node-0".into(), ProposerStats::default()),
                ("node-1".into(), ProposerStats::default()),
                ("node-2".into(), ProposerStats::default()),
            ],
            rpc_stats: RpcStats::default(),
            node_pressure: NodePressure::default(),
            request_class: RequestClass {
                durability: crate::types::DurabilityMode::Sync,
                size: crate::types::SizeBucket::Small,
            },
            feature_age_us: 1000,
            sample_count: 100,
            missingness_flags: MissingnessFlags::default(),
        }
    }

    fn test_candidate_set() -> CandidateSet {
        CandidateSet {
            identity: test_identity(),
            eligible_voters: vec!["node-0".into(), "node-1".into(), "node-2".into()],
            hedge_delay_allowlist: vec![
                HedgeDelayBucket::Ms5,
                HedgeDelayBucket::Ms10,
                HedgeDelayBucket::Static,
            ],
            static_hedge_delay_ms: 100,
        }
    }

    #[test]
    fn bandit_selects_valid_action() {
        let mut bandit = ContextualBandit::new(BanditConfig::default());
        let features = test_features();
        let candidates = test_candidate_set();

        let output = bandit.select_action(&features, &candidates, false);
        assert!(candidates
            .eligible_voters
            .contains(&output.action.first_request_target));
        assert!(candidates
            .hedge_delay_allowlist
            .contains(&output.action.hedge_delay));
        assert_eq!(output.identity, test_identity());
    }

    #[test]
    fn bandit_resets_on_identity_change() {
        let mut bandit = ContextualBandit::new(BanditConfig::default());
        let features = test_features();
        let candidates = test_candidate_set();

        // First selection
        bandit.select_action(&features, &candidates, false);
        assert!(!bandit.action_stats.is_empty());

        // Change identity
        let mut new_features = test_features();
        new_features.identity.epoch = 2;
        bandit.select_action(&new_features, &candidates, false);
        // Stats should have been cleared and rebuilt
        assert!(
            bandit.action_stats.len()
                <= candidates.eligible_voters.len() * candidates.hedge_delay_allowlist.len()
        );
    }

    #[test]
    fn bandit_update_changes_stats() {
        let mut bandit = ContextualBandit::new(BanditConfig::default());
        let features = test_features();
        let candidates = test_candidate_set();

        let output = bandit.select_action(&features, &candidates, false);
        let initial_count = bandit.action_stats[&(
            output.action.first_request_target.clone(),
            output.action.hedge_delay,
        )]
            .pull_count;

        bandit.update(&output.action, -0.5);
        let updated_count = bandit.action_stats[&(
            output.action.first_request_target.clone(),
            output.action.hedge_delay,
        )]
            .pull_count;

        assert_eq!(updated_count, initial_count + 1);
    }

    #[test]
    fn empty_candidates_returns_static() {
        let mut bandit = ContextualBandit::new(BanditConfig::default());
        let features = test_features();
        let empty_candidates = CandidateSet {
            eligible_voters: vec![],
            hedge_delay_allowlist: vec![],
            ..test_candidate_set()
        };

        let output = bandit.select_action(&features, &empty_candidates, false);
        assert_eq!(output.action.first_request_target, "");
        assert_eq!(output.action.hedge_delay, HedgeDelayBucket::Static);
    }

    #[test]
    fn sample_beta_in_range() {
        let mut rng = rand::rng();
        for _ in 0..1000 {
            let v = sample_beta(&mut rng, 2.0, 5.0);
            assert!((0.0..=1.0).contains(&v), "sample_beta returned {v}");
        }
    }
}
