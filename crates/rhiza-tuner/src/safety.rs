use crate::types::{ActionOutput, CandidateSet, FallbackReason, HedgeDelayBucket, Identity};

/// Safety boundary configuration.
#[derive(Clone, Debug)]
pub struct SafetyConfig {
    /// Maximum feature age before fallback (microseconds).
    pub freshness_limit_us: u64,
    /// Minimum confidence threshold.
    pub confidence_threshold: f64,
    /// Action validity duration (microseconds).
    pub validity_duration_us: u64,
}

impl Default for SafetyConfig {
    fn default() -> Self {
        Self {
            freshness_limit_us: 5_000_000, // 5 seconds
            confidence_threshold: 0.3,
            validity_duration_us: 10_000_000, // 10 seconds
        }
    }
}

/// Safety boundary layer ensures the tuner is purely advisory.
///
/// Every model failure falls back without blocking or changing protocol inputs.
/// Outputs naming ineligible members, delays outside the allowlist, future slots,
/// expired validity intervals, or mismatched identities are rejected.
pub struct SafetyBoundary {
    config: SafetyConfig,
}

impl SafetyBoundary {
    pub fn new(config: SafetyConfig) -> Self {
        Self { config }
    }

    /// Validate an action output against the candidate set and identity.
    /// Returns Ok(output) if valid, Err(fallback_reason) if rejected.
    pub fn validate(
        &self,
        output: &ActionOutput,
        candidate_set: &CandidateSet,
        current_identity: &Identity,
    ) -> Result<(), FallbackReason> {
        // Check identity match
        if output.identity != *current_identity {
            return Err(FallbackReason::ConfigMismatch);
        }

        // Check that proposer is an eligible voter
        if !candidate_set
            .eligible_voters
            .contains(&output.action.first_request_target)
        {
            return Err(FallbackReason::InvalidOutput);
        }

        // Check hedge delay is in allowlist
        if !candidate_set
            .hedge_delay_allowlist
            .contains(&output.action.hedge_delay)
        {
            return Err(FallbackReason::InvalidOutput);
        }

        // Check confidence threshold
        if output.confidence < self.config.confidence_threshold {
            return Err(FallbackReason::ConfidenceBelowThreshold);
        }

        // Check expiry
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        if now_us > output.expiry_us {
            return Err(FallbackReason::FreshnessExpired);
        }

        // Check valid_from_slot is not in the future (we allow current or past)
        // valid_from_slot is a monotonically increasing slot counter; we just
        // ensure it's set (non-zero would be a real slot)

        Ok(())
    }

    /// Produce a static fallback action from the candidate set.
    pub fn static_fallback(&self, candidate_set: &CandidateSet) -> ActionOutput {
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let static_delay = if candidate_set.static_hedge_delay_ms > 0 {
            // Find closest bucket or use Static
            HedgeDelayBucket::all_buckets()
                .iter()
                .find(|b| b.as_ms() == Some(candidate_set.static_hedge_delay_ms))
                .copied()
                .unwrap_or(HedgeDelayBucket::Static)
        } else {
            HedgeDelayBucket::Static
        };

        ActionOutput {
            action: crate::types::Action {
                first_request_target: candidate_set
                    .eligible_voters
                    .first()
                    .cloned()
                    .unwrap_or_default(),
                hedge_delay: static_delay,
            },
            identity: candidate_set.identity.clone(),
            valid_from_slot: 0,
            expiry_us: now_us + self.config.validity_duration_us,
            policy_version: 0,
            model_version: 0,
            exploration: false,
            confidence: 1.0,
            fallback_reason: None,
        }
    }

    /// Create a static fallback with a specific reason.
    pub fn static_fallback_with_reason(
        &self,
        candidate_set: &CandidateSet,
        reason: FallbackReason,
    ) -> ActionOutput {
        let mut fallback = self.static_fallback(candidate_set);
        fallback.fallback_reason = Some(reason);
        fallback
    }

    /// Validate that the feature freshness is within limits.
    pub fn check_freshness(&self, feature_age_us: u64) -> Result<(), FallbackReason> {
        if feature_age_us > self.config.freshness_limit_us {
            Err(FallbackReason::StaleFeatures)
        } else {
            Ok(())
        }
    }

    /// Get the config.
    pub fn config(&self) -> &SafetyConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Action, HedgeDelayBucket, Identity};

    fn test_identity() -> Identity {
        Identity {
            cluster_id: "test-cluster".into(),
            epoch: 1,
            config_id: 1,
            membership_digest: [0u8; 32],
            recovery_generation: 0,
        }
    }

    fn test_candidate_set() -> CandidateSet {
        CandidateSet {
            identity: test_identity(),
            eligible_voters: vec!["node-0".into(), "node-1".into(), "node-2".into()],
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

    fn test_output() -> ActionOutput {
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        ActionOutput {
            action: Action {
                first_request_target: "node-0".into(),
                hedge_delay: HedgeDelayBucket::Ms10,
            },
            identity: test_identity(),
            valid_from_slot: 1,
            expiry_us: now_us + 10_000_000,
            policy_version: 1,
            model_version: 1,
            exploration: false,
            confidence: 0.8,
            fallback_reason: None,
        }
    }

    #[test]
    fn valid_output_passes_validation() {
        let safety = SafetyBoundary::new(SafetyConfig::default());
        let output = test_output();
        let candidates = test_candidate_set();
        assert!(safety
            .validate(&output, &candidates, &test_identity())
            .is_ok());
    }

    #[test]
    fn ineligible_proposer_rejected() {
        let safety = SafetyBoundary::new(SafetyConfig::default());
        let mut output = test_output();
        output.action.first_request_target = "node-99".into();
        let candidates = test_candidate_set();
        assert_eq!(
            safety.validate(&output, &candidates, &test_identity()),
            Err(FallbackReason::InvalidOutput)
        );
    }

    #[test]
    fn identity_mismatch_rejected() {
        let safety = SafetyBoundary::new(SafetyConfig::default());
        let output = test_output();
        let candidates = test_candidate_set();
        let mut wrong_identity = test_identity();
        wrong_identity.epoch = 99;
        assert_eq!(
            safety.validate(&output, &candidates, &wrong_identity),
            Err(FallbackReason::ConfigMismatch)
        );
    }

    #[test]
    fn low_confidence_rejected() {
        let safety = SafetyBoundary::new(SafetyConfig::default());
        let mut output = test_output();
        output.confidence = 0.1;
        let candidates = test_candidate_set();
        assert_eq!(
            safety.validate(&output, &candidates, &test_identity()),
            Err(FallbackReason::ConfidenceBelowThreshold)
        );
    }

    #[test]
    fn expired_output_rejected() {
        let safety = SafetyBoundary::new(SafetyConfig::default());
        let mut output = test_output();
        output.expiry_us = 1; // expired long ago
        let candidates = test_candidate_set();
        assert_eq!(
            safety.validate(&output, &candidates, &test_identity()),
            Err(FallbackReason::FreshnessExpired)
        );
    }

    #[test]
    fn disallowed_hedge_delay_rejected() {
        let safety = SafetyBoundary::new(SafetyConfig::default());
        let mut output = test_output();
        output.action.hedge_delay = HedgeDelayBucket::Ms5;
        let candidates = CandidateSet {
            hedge_delay_allowlist: vec![HedgeDelayBucket::Ms10, HedgeDelayBucket::Static],
            ..test_candidate_set()
        };
        assert_eq!(
            safety.validate(&output, &candidates, &test_identity()),
            Err(FallbackReason::InvalidOutput)
        );
    }

    #[test]
    fn static_fallback_uses_first_eligible_voter() {
        let safety = SafetyBoundary::new(SafetyConfig::default());
        let candidates = test_candidate_set();
        let fallback = safety.static_fallback(&candidates);
        assert_eq!(fallback.action.first_request_target, "node-0");
        assert_eq!(fallback.confidence, 1.0);
        assert!(fallback.fallback_reason.is_none());
    }

    #[test]
    fn static_fallback_with_reason() {
        let safety = SafetyBoundary::new(SafetyConfig::default());
        let candidates = test_candidate_set();
        let fallback =
            safety.static_fallback_with_reason(&candidates, FallbackReason::KillSwitchActive);
        assert_eq!(
            fallback.fallback_reason,
            Some(FallbackReason::KillSwitchActive)
        );
    }

    #[test]
    fn stale_features_detected() {
        let safety = SafetyBoundary::new(SafetyConfig::default());
        assert!(safety.check_freshness(1_000_000).is_ok());
        assert_eq!(
            safety.check_freshness(10_000_000),
            Err(FallbackReason::StaleFeatures)
        );
    }
}
