//! # rhiza-tuner
//!
//! MAB-based preferred-proposer and hedge-delay auto-tuning for rhiza.
//!
//! This crate implements a contextual bandit system that selects the preferred
//! proposer most likely to complete a write quickly and tunes hedge delay to
//! reduce tail latency without excessive duplicate work.
//!
//! ## Safety Boundary
//!
//! The tuner is an advisory scheduling component outside the QuePaxa state machine.
//! It never affects validity, quorum, ballots, certificates, membership, recovery,
//! or liveness. Every model failure falls back to static policy.
//!
//! ## Rollout Stages
//!
//! Controlled by Cargo feature flags:
//!
//! - `shadow`: compute actions but never apply them
//! - `proposer-canary`: apply proposer choice only, with static hedge delay
//! - `hedge-canary`: apply bounded delay actions
//! - `default-on`: full tuning enabled by default

pub mod bandit;
pub mod collector;
pub mod killswitch;
pub mod observability;
pub mod reward;
pub mod rollout;
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
pub use safety::{SafetyBoundary, SafetyConfig};
pub use tuner::{ActionSelectionResult, MabTuner, TunerConfig};
pub use types::*;
