use std::sync::Mutex;

use rhiza_core::NodeId;

use crate::bandit::{BanditConfig, ContextualBandit};
use crate::collector::{CollectorConfig, TelemetryCollector};
use crate::killswitch::KillSwitch;
use crate::observability::{Observability, ObservabilityConfig};
use crate::reward::{RewardConfig, RewardPipeline};
use crate::rollout::{RolloutGuard, RolloutStage};
use crate::safety::{SafetyBoundary, SafetyConfig};
use crate::types::*;

/// Top-level configuration for the MAB tuner.
#[derive(Clone, Debug)]
pub struct TunerConfig {
    pub collector: CollectorConfig,
    pub bandit: BanditConfig,
    pub safety: SafetyConfig,
    pub reward: RewardConfig,
    pub observability: ObservabilityConfig,
    /// Static preferred proposer (usually membership[0]).
    pub static_preferred_proposer: NodeId,
    /// Static hedge delay in milliseconds.
    pub static_hedge_delay_ms: u64,
    /// Minimum samples before leaving cold-start mode.
    pub cold_start_min_samples: u64,
}

impl Default for TunerConfig {
    fn default() -> Self {
        Self {
            collector: CollectorConfig::default(),
            bandit: BanditConfig::default(),
            safety: SafetyConfig::default(),
            reward: RewardConfig::default(),
            observability: ObservabilityConfig::default(),
            static_preferred_proposer: NodeId::default(),
            static_hedge_delay_ms: 100,
            cold_start_min_samples: 100,
        }
    }
}

/// MAB-based auto-tuner orchestrating all components.
///
/// Integrates telemetry collection, contextual bandit model, safety boundary,
/// reward pipeline, kill switch, observability, and rollout stage control.
pub struct MabTuner {
    config: TunerConfig,
    collector: TelemetryCollector,
    bandit: Mutex<ContextualBandit>,
    safety: SafetyBoundary,
    reward: Mutex<RewardPipeline>,
    killswitch: KillSwitch,
    observability: Observability,
    rollout: RolloutGuard,
}

impl MabTuner {
    pub fn new(config: TunerConfig) -> Self {
        Self::with_stage(config, RolloutStage::from_features())
    }

    pub fn with_stage(config: TunerConfig, stage: RolloutStage) -> Self {
        Self {
            collector: TelemetryCollector::new(config.collector.clone()),
            bandit: Mutex::new(ContextualBandit::new(config.bandit.clone())),
            safety: SafetyBoundary::new(config.safety.clone()),
            reward: Mutex::new(RewardPipeline::new(config.reward.clone())),
            killswitch: KillSwitch::new(),
            observability: Observability::new(config.observability.clone()),
            rollout: RolloutGuard::with_stage(stage),
            config,
        }
    }

    /// Select an action for a request.
    ///
    /// This is the main entry point. It:
    /// 1. Checks kill switch
    /// 2. Assembles features from telemetry
    /// 3. Checks cold-start gates
    /// 4. Selects action via bandit or static fallback
    /// 5. Validates via safety boundary
    /// 6. Records action for observability
    /// 7. Returns action with rollout-stage-appropriate flags
    pub fn select_action(
        &self,
        identity: &Identity,
        eligible_proposers: &[NodeId],
        candidate_set: &CandidateSet,
        correlation_id: &str,
    ) -> ActionSelectionResult {
        let (apply_proposer, apply_hedge, is_shadow) = self.rollout.evaluate();

        // 1. Kill switch check
        if self.killswitch.is_active() {
            let fallback = self
                .safety
                .static_fallback_with_reason(candidate_set, FallbackReason::KillSwitchActive);
            self.record_and_return(
                correlation_id,
                &fallback,
                apply_proposer,
                apply_hedge,
                is_shadow,
            );
            return ActionSelectionResult {
                output: fallback,
                apply_proposer,
                apply_hedge_delay: apply_hedge,
                is_shadow,
            };
        }

        // 2. Rollout stage disabled - use static
        if !self.rollout.stage().computes_actions() {
            let fallback = self.safety.static_fallback(candidate_set);
            self.record_and_return(
                correlation_id,
                &fallback,
                apply_proposer,
                apply_hedge,
                is_shadow,
            );
            return ActionSelectionResult {
                output: fallback,
                apply_proposer,
                apply_hedge_delay: apply_hedge,
                is_shadow,
            };
        }

        // 3. Assemble features
        let features = match self
            .collector
            .assemble_features(identity.clone(), eligible_proposers)
        {
            Some(f) => f,
            None => {
                let fallback = self
                    .safety
                    .static_fallback_with_reason(candidate_set, FallbackReason::StaleFeatures);
                self.record_and_return(
                    correlation_id,
                    &fallback,
                    apply_proposer,
                    apply_hedge,
                    is_shadow,
                );
                return ActionSelectionResult {
                    output: fallback,
                    apply_proposer,
                    apply_hedge_delay: apply_hedge,
                    is_shadow,
                };
            }
        };

        // 4. Freshness check
        if let Err(reason) = self.safety.check_freshness(features.feature_age_us) {
            let fallback = self
                .safety
                .static_fallback_with_reason(candidate_set, reason);
            self.record_and_return(
                correlation_id,
                &fallback,
                apply_proposer,
                apply_hedge,
                is_shadow,
            );
            return ActionSelectionResult {
                output: fallback,
                apply_proposer,
                apply_hedge_delay: apply_hedge,
                is_shadow,
            };
        }

        // 5. Cold-start gate
        if !self.collector.cold_start_gates_passed() {
            let fallback = self
                .safety
                .static_fallback_with_reason(candidate_set, FallbackReason::ColdStart);
            self.record_and_return(
                correlation_id,
                &fallback,
                apply_proposer,
                apply_hedge,
                is_shadow,
            );
            return ActionSelectionResult {
                output: fallback,
                apply_proposer,
                apply_hedge_delay: apply_hedge,
                is_shadow,
            };
        }

        // 6. Select action via bandit
        let exploration = self.rollout.stage().exploration_enabled();
        let output = self
            .bandit
            .lock()
            .map_err(|_| ())
            .map(|mut b| b.select_action(&features, candidate_set, exploration));

        let output = match output {
            Ok(o) => o,
            Err(()) => {
                let fallback = self
                    .safety
                    .static_fallback_with_reason(candidate_set, FallbackReason::ModelCrash);
                self.record_and_return(
                    correlation_id,
                    &fallback,
                    apply_proposer,
                    apply_hedge,
                    is_shadow,
                );
                return ActionSelectionResult {
                    output: fallback,
                    apply_proposer,
                    apply_hedge_delay: apply_hedge,
                    is_shadow,
                };
            }
        };

        // 7. Validate via safety boundary
        match self.safety.validate(&output, candidate_set, identity) {
            Ok(()) => {
                self.record_and_return(
                    correlation_id,
                    &output,
                    apply_proposer,
                    apply_hedge,
                    is_shadow,
                );
                ActionSelectionResult {
                    output,
                    apply_proposer,
                    apply_hedge_delay: apply_hedge,
                    is_shadow,
                }
            }
            Err(reason) => {
                let fallback = self
                    .safety
                    .static_fallback_with_reason(candidate_set, reason);
                self.record_and_return(
                    correlation_id,
                    &fallback,
                    apply_proposer,
                    apply_hedge,
                    is_shadow,
                );
                ActionSelectionResult {
                    output: fallback,
                    apply_proposer,
                    apply_hedge_delay: apply_hedge,
                    is_shadow,
                }
            }
        }
    }

    /// Record a terminal outcome and update the model.
    pub fn record_outcome(&self, correlation_id: &str, action: &Action, outcome: &TerminalOutcome) {
        let reward = if let Ok(mut reward) = self.reward.lock() {
            reward.compute(outcome)
        } else {
            return;
        };
        self.observability
            .record_outcome(correlation_id, outcome, &reward);
        // Legacy compatibility path: never train a counterfactual shadow action.
        if !self.killswitch.is_active() && self.rollout.stage().applies_proposer_choice() {
            if let Ok(mut bandit) = self.bandit.lock() {
                bandit.update(action, reward.total);
            }
        }
    }

    /// Activate the kill switch.
    pub fn activate_kill_switch(&self, reason: impl Into<String>) {
        self.killswitch.activate(reason);
        tracing::warn!("MAB tuner kill switch activated");
    }

    /// Deactivate the kill switch.
    pub fn deactivate_kill_switch(&self) {
        self.killswitch.deactivate();
        tracing::info!("MAB tuner kill switch deactivated");
    }

    /// Check if kill switch is active.
    pub fn is_killed(&self) -> bool {
        self.killswitch.is_active()
    }

    /// Get the telemetry collector for external data feeding.
    pub fn collector(&self) -> &TelemetryCollector {
        &self.collector
    }

    /// Get observability metrics.
    pub fn metrics(&self) -> crate::observability::AggregateMetrics {
        self.observability.metrics()
    }

    /// Get recent action records.
    pub fn recent_records(&self, limit: usize) -> Vec<ActionRecord> {
        self.observability.recent_records(limit)
    }

    /// Get the current rollout stage.
    pub fn stage(&self) -> RolloutStage {
        self.rollout.stage()
    }

    /// Change the legacy tuner's rollout stage without rebuilding it.
    pub fn set_stage(&self, stage: RolloutStage) {
        self.rollout.set_stage(stage);
    }

    /// Reset all state (used on identity change).
    pub fn reset(&self) {
        self.collector.reset();
        if let Ok(mut bandit) = self.bandit.lock() {
            bandit.reset();
        }
        self.observability.clear();
    }

    fn record_and_return(
        &self,
        correlation_id: &str,
        output: &ActionOutput,
        apply_proposer: bool,
        apply_hedge: bool,
        is_shadow: bool,
    ) {
        let record = ActionRecord {
            correlation_id: correlation_id.into(),
            configuration_epoch: output.identity.epoch,
            feature_version: output.policy_version,
            model_version: output.model_version,
            chosen_proposer: output.action.first_request_target.clone(),
            chosen_delay: output.action.hedge_delay,
            baseline_proposer: self.config.static_preferred_proposer.clone(),
            baseline_delay: HedgeDelayBucket::Static,
            exploration: output.exploration,
            confidence: output.confidence,
            fallback_reason: output.fallback_reason.clone(),
            terminal_outcome: None,
            latency_us: None,
            duplicate_work: false,
            reward_components: None,
            proposer_applied: apply_proposer,
            hedge_delay_applied: apply_hedge,
            is_shadow,
        };
        self.observability.record_action(record);
    }
}

/// Result of an action selection.
#[derive(Clone, Debug)]
pub struct ActionSelectionResult {
    pub output: ActionOutput,
    pub apply_proposer: bool,
    pub apply_hedge_delay: bool,
    pub is_shadow: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> TunerConfig {
        TunerConfig {
            static_preferred_proposer: "node-0".into(),
            static_hedge_delay_ms: 100,
            cold_start_min_samples: 0, // disable cold start for basic tests
            ..Default::default()
        }
    }

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

    fn test_candidate_set() -> CandidateSet {
        CandidateSet {
            identity: test_identity(),
            eligible_voters: test_proposers(),
            hedge_delay_allowlist: vec![
                HedgeDelayBucket::Ms5,
                HedgeDelayBucket::Ms10,
                HedgeDelayBucket::Ms25,
                HedgeDelayBucket::Ms50,
                HedgeDelayBucket::Ms100,
                HedgeDelayBucket::Static,
            ],
            static_hedge_delay_ms: 100,
        }
    }

    #[test]
    fn kill_switch_forces_static_fallback() {
        let tuner = MabTuner::with_stage(test_config(), RolloutStage::DefaultOn);
        tuner.activate_kill_switch("test");

        let result = tuner.select_action(
            &test_identity(),
            &test_proposers(),
            &test_candidate_set(),
            "req-1",
        );

        assert_eq!(
            result.output.fallback_reason,
            Some(FallbackReason::KillSwitchActive)
        );
    }

    #[test]
    fn disabled_stage_uses_static() {
        let tuner = MabTuner::with_stage(test_config(), RolloutStage::Disabled);

        let result = tuner.select_action(
            &test_identity(),
            &test_proposers(),
            &test_candidate_set(),
            "req-1",
        );

        // Static fallback should use first voter
        assert_eq!(result.output.action.first_request_target, "node-0");
        assert!(!result.apply_proposer);
        assert!(!result.apply_hedge_delay);
        assert!(!result.is_shadow);
    }

    #[test]
    fn shadow_stage_does_not_apply() {
        let tuner = MabTuner::with_stage(test_config(), RolloutStage::Shadow);

        let result = tuner.select_action(
            &test_identity(),
            &test_proposers(),
            &test_candidate_set(),
            "req-1",
        );

        assert!(!result.apply_proposer);
        assert!(!result.apply_hedge_delay);
        assert!(result.is_shadow);
    }

    #[test]
    fn proposer_canary_applies_proposer_only() {
        let tuner = MabTuner::with_stage(test_config(), RolloutStage::ProposerCanary);

        let result = tuner.select_action(
            &test_identity(),
            &test_proposers(),
            &test_candidate_set(),
            "req-1",
        );

        assert!(result.apply_proposer);
        assert!(!result.apply_hedge_delay);
        assert!(!result.is_shadow);
    }

    #[test]
    fn cold_start_returns_static() {
        let config = TunerConfig {
            cold_start_min_samples: 100,
            ..test_config()
        };
        let tuner = MabTuner::with_stage(config, RolloutStage::DefaultOn);

        let result = tuner.select_action(
            &test_identity(),
            &test_proposers(),
            &test_candidate_set(),
            "req-1",
        );

        assert_eq!(
            result.output.fallback_reason,
            Some(FallbackReason::ColdStart)
        );
    }

    #[test]
    fn outcome_recording_works() {
        let tuner = MabTuner::with_stage(test_config(), RolloutStage::DefaultOn);

        tuner.select_action(
            &test_identity(),
            &test_proposers(),
            &test_candidate_set(),
            "req-1",
        );

        let action = Action {
            first_request_target: "node-0".into(),
            hedge_delay: HedgeDelayBucket::Ms10,
        };
        let outcome = TerminalOutcome::Success {
            decision_latency_us: 5_000,
            additional_rpcs: 0,
            duplicate_proposer_work: false,
            contention_or_round_escalation: false,
        };
        tuner.record_outcome("req-1", &action, &outcome);

        let metrics = tuner.metrics();
        assert_eq!(metrics.total_actions, 1);
    }
}
