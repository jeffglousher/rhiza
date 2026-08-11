/// Runtime rollout stage. Builds always start disabled; operators advance stages
/// through [`RolloutGuard::set_stage`] without rebuilding or restarting.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum RolloutStage {
    /// Disabled: use static policy only.
    Disabled,
    /// Compute actions but never apply them; validate attribution and freshness.
    Shadow,
    /// Apply proposer choice only, with static hedge delay.
    ProposerCanary,
    /// Apply proposer selection to all traffic while retaining the fixed hedge delay.
    ProposerDefault,
    /// Apply bounded delay actions with strict duplicate-work and error budgets.
    HedgeCanary,
    /// Reserved for bounded hedge tuning; v1 routing still uses the fixed delay.
    BoundedDefault,
    /// Full tuning enabled by default with automated rollback.
    DefaultOn,
}

impl RolloutStage {
    /// Compatibility constructor for the legacy [`crate::MabTuner`] API.
    /// [`crate::RoutingTuner`] does not use this constructor and always starts
    /// disabled unless its caller supplies an explicit runtime stage.
    pub const fn from_features() -> Self {
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
        !matches!(self, Self::Disabled)
    }

    /// Whether proposer choice should be applied to real traffic.
    pub fn applies_proposer_choice(&self) -> bool {
        matches!(
            self,
            Self::ProposerCanary
                | Self::HedgeCanary
                | Self::ProposerDefault
                | Self::BoundedDefault
                | Self::DefaultOn
        )
    }

    /// Whether hedge delay actions should be applied.
    pub fn applies_hedge_delay(&self) -> bool {
        matches!(
            self,
            Self::HedgeCanary | Self::BoundedDefault | Self::DefaultOn
        )
    }

    /// Whether exploration is enabled.
    pub fn exploration_enabled(&self) -> bool {
        matches!(self, Self::Shadow | Self::ProposerCanary)
    }

    /// Whether this is a canary stage (for cohort comparison).
    pub fn is_canary(&self) -> bool {
        *self == Self::ProposerCanary || *self == Self::HedgeCanary
    }
}

/// Rollout guard that wraps an action with stage-appropriate behavior.
pub struct RolloutGuard {
    stage: std::sync::atomic::AtomicU8,
}

impl RolloutGuard {
    pub fn new() -> Self {
        Self::with_stage(RolloutStage::from_features())
    }

    pub fn with_stage(stage: RolloutStage) -> Self {
        Self {
            stage: std::sync::atomic::AtomicU8::new(stage as u8),
        }
    }

    pub fn stage(&self) -> RolloutStage {
        match self.stage.load(std::sync::atomic::Ordering::Acquire) {
            0 => RolloutStage::Disabled,
            1 => RolloutStage::Shadow,
            2 => RolloutStage::ProposerCanary,
            3 => RolloutStage::ProposerDefault,
            4 => RolloutStage::HedgeCanary,
            5 => RolloutStage::BoundedDefault,
            _ => RolloutStage::DefaultOn,
        }
    }

    /// Change rollout behavior without a process restart.
    pub fn set_stage(&self, stage: RolloutStage) {
        self.stage
            .store(stage as u8, std::sync::atomic::Ordering::Release);
    }

    /// Determine whether to apply the tuned action or fall back to static.
    ///
    /// Returns (apply_proposer, apply_hedge_delay, is_shadow).
    pub fn evaluate(&self) -> (bool, bool, bool) {
        let stage = self.stage();
        let is_shadow = stage == RolloutStage::Shadow;
        let apply_proposer = stage.applies_proposer_choice();
        let apply_hedge = stage.applies_hedge_delay();
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
        assert!(RolloutStage::ProposerCanary < RolloutStage::ProposerDefault);
        assert!(RolloutStage::ProposerCanary < RolloutStage::HedgeCanary);
        assert!(RolloutStage::ProposerDefault < RolloutStage::HedgeCanary);
        assert!(RolloutStage::HedgeCanary < RolloutStage::DefaultOn);
    }

    #[test]
    fn legacy_feature_default_is_preserved() {
        let expected = if cfg!(feature = "default-on") {
            RolloutStage::DefaultOn
        } else if cfg!(feature = "hedge-canary") {
            RolloutStage::HedgeCanary
        } else if cfg!(feature = "proposer-canary") {
            RolloutStage::ProposerCanary
        } else if cfg!(feature = "shadow") {
            RolloutStage::Shadow
        } else {
            RolloutStage::Disabled
        };
        assert_eq!(RolloutStage::from_features(), expected);
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
