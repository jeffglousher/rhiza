/// Rollout stage configuration using Cargo feature flags.
///
/// Stages are cumulative: each stage enables all previous stages.
///
/// - `shadow`: compute actions but never apply them
/// - `proposer-canary` (implies `shadow`): apply proposer choice only, with static hedge delay
/// - `hedge-canary` (implies `proposer-canary`): apply bounded delay actions
/// - `default-on` (implies `hedge-canary`): full tuning enabled by default
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum RolloutStage {
    /// Disabled: use static policy only.
    Disabled,
    /// Compute actions but never apply them; validate attribution and freshness.
    Shadow,
    /// Apply proposer choice only, with static hedge delay.
    ProposerCanary,
    /// Apply bounded delay actions with strict duplicate-work and error budgets.
    HedgeCanary,
    /// Full tuning enabled by default with automated rollback.
    DefaultOn,
}

impl RolloutStage {
    /// Determine the current rollout stage from Cargo feature flags.
    pub fn from_features() -> Self {
        if cfg!(feature = "default-on") {
            Self::DefaultOn
        } else if cfg!(feature = "hedge-canary") {
            Self::HedgeCanary
        } else if cfg!(feature = "proposer-canary") {
            Self::ProposerCanary
        } else if cfg!(feature = "shadow") {
            Self::Shadow
        } else {
            Self::Disabled
        }
    }

    /// Whether the tuner should compute actions at all.
    pub fn computes_actions(&self) -> bool {
        *self >= Self::Shadow
    }

    /// Whether proposer choice should be applied to real traffic.
    pub fn applies_proposer_choice(&self) -> bool {
        *self >= Self::ProposerCanary
    }

    /// Whether hedge delay actions should be applied.
    pub fn applies_hedge_delay(&self) -> bool {
        *self >= Self::HedgeCanary
    }

    /// Whether exploration is enabled.
    pub fn exploration_enabled(&self) -> bool {
        *self >= Self::Shadow
    }

    /// Whether this is a canary stage (for cohort comparison).
    pub fn is_canary(&self) -> bool {
        *self == Self::ProposerCanary || *self == Self::HedgeCanary
    }
}

/// Rollout guard that wraps an action with stage-appropriate behavior.
pub struct RolloutGuard {
    stage: RolloutStage,
}

impl RolloutGuard {
    pub fn new() -> Self {
        Self {
            stage: RolloutStage::from_features(),
        }
    }

    pub fn with_stage(stage: RolloutStage) -> Self {
        Self { stage }
    }

    pub fn stage(&self) -> RolloutStage {
        self.stage
    }

    /// Determine whether to apply the tuned action or fall back to static.
    ///
    /// Returns (apply_proposer, apply_hedge_delay, is_shadow).
    pub fn evaluate(&self) -> (bool, bool, bool) {
        let is_shadow = self.stage == RolloutStage::Shadow;
        let apply_proposer = self.stage.applies_proposer_choice();
        let apply_hedge = self.stage.applies_hedge_delay();
        (apply_proposer, apply_hedge, is_shadow)
    }
}

impl Default for RolloutGuard {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_ordering() {
        assert!(RolloutStage::Disabled < RolloutStage::Shadow);
        assert!(RolloutStage::Shadow < RolloutStage::ProposerCanary);
        assert!(RolloutStage::ProposerCanary < RolloutStage::HedgeCanary);
        assert!(RolloutStage::HedgeCanary < RolloutStage::DefaultOn);
    }

    #[test]
    fn disabled_stage_no_actions() {
        let guard = RolloutGuard::with_stage(RolloutStage::Disabled);
        let (apply_proposer, apply_hedge, is_shadow) = guard.evaluate();
        assert!(!apply_proposer);
        assert!(!apply_hedge);
        assert!(!is_shadow);
    }

    #[test]
    fn shadow_stage_computes_but_does_not_apply() {
        let guard = RolloutGuard::with_stage(RolloutStage::Shadow);
        let (apply_proposer, apply_hedge, is_shadow) = guard.evaluate();
        assert!(!apply_proposer);
        assert!(!apply_hedge);
        assert!(is_shadow);
    }

    #[test]
    fn proposer_canary_applies_proposer_only() {
        let guard = RolloutGuard::with_stage(RolloutStage::ProposerCanary);
        let (apply_proposer, apply_hedge, is_shadow) = guard.evaluate();
        assert!(apply_proposer);
        assert!(!apply_hedge);
        assert!(!is_shadow);
    }

    #[test]
    fn hedge_canary_applies_both() {
        let guard = RolloutGuard::with_stage(RolloutStage::HedgeCanary);
        let (apply_proposer, apply_hedge, is_shadow) = guard.evaluate();
        assert!(apply_proposer);
        assert!(apply_hedge);
        assert!(!is_shadow);
    }

    #[test]
    fn default_on_applies_both() {
        let guard = RolloutGuard::with_stage(RolloutStage::DefaultOn);
        let (apply_proposer, apply_hedge, is_shadow) = guard.evaluate();
        assert!(apply_proposer);
        assert!(apply_hedge);
        assert!(!is_shadow);
    }

    #[test]
    fn canary_stages_detected() {
        assert!(RolloutStage::ProposerCanary.is_canary());
        assert!(RolloutStage::HedgeCanary.is_canary());
        assert!(!RolloutStage::Disabled.is_canary());
        assert!(!RolloutStage::Shadow.is_canary());
        assert!(!RolloutStage::DefaultOn.is_canary());
    }
}
