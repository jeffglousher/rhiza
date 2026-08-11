//! # rhiza-tuner
//!
//! Runtime-controlled first-target routing tuning for rhiza.
//!
//! [`RoutingTuner`] selects only the first healthy request target. Hedge delay
//! remains a fixed caller policy in v1. The legacy experimental [`MabTuner`] API
//! remains for source and benchmark compatibility, may still produce the old
//! target×delay action shape, and is not used by `rhiza-client` or covered by the
//! phase-one routing safety contract.
//!
//! ## Safety Boundary
//!
//! The tuner is an advisory scheduling component outside the QuePaxa state machine.
//! It never affects validity, quorum, ballots, certificates, membership, recovery,
//! or liveness. Every model failure falls back to static policy.
//!
//! ## Rollout Stages
//!
//! [`RoutingTuner`] is controlled at runtime with [`RoutingTuner::set_stage`],
//! starts disabled regardless of legacy Cargo features, and returns the next
//! plan to validated static routing when its kill switch is active.

pub mod bandit;
pub mod collector;
pub mod killswitch;
pub mod observability;
pub mod reward;
pub mod rollout;
pub mod routing;
pub mod safety;
pub mod tuner;
pub mod types;

// Re-export the main orchestrator and key types at crate level.
pub use bandit::{BanditConfig, ContextualBandit};
pub use collector::{CollectorConfig, TelemetryCollector};
pub use killswitch::KillSwitch;
pub use observability::{AggregateMetrics, Observability, ObservabilityConfig};
pub use reward::{RewardConfig, RewardPipeline};
pub use rollout::{RolloutGuard, RolloutStage};
pub use routing::*;
pub use safety::{SafetyBoundary, SafetyConfig};
pub use tuner::{ActionSelectionResult, MabTuner, TunerConfig};
pub use types::*;
