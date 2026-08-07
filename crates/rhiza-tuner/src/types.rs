use serde::{Deserialize, Serialize};

pub use rhiza_core::NodeId;
use rhiza_core::{ClusterId, ConfigId, Epoch};

/// Exact configuration identity tuple that keys model and policy state.
/// Reset whenever any field changes.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Identity {
    pub cluster_id: ClusterId,
    pub epoch: Epoch,
    pub config_id: ConfigId,
    pub membership_digest: [u8; 32],
    pub recovery_generation: u64,
}

impl Identity {
    pub fn digest(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(self.cluster_id.as_bytes());
        hasher.update(self.epoch.to_le_bytes());
        hasher.update(self.config_id.to_le_bytes());
        hasher.update(self.membership_digest);
        hasher.update(self.recovery_generation.to_le_bytes());
        hasher.finalize().into()
    }
}

/// Hedge delay bucket values in milliseconds.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum HedgeDelayBucket {
    Ms5,
    Ms10,
    Ms25,
    Ms50,
    Ms100,
    Static,
}

impl HedgeDelayBucket {
    pub fn as_ms(&self) -> Option<u64> {
        match self {
            Self::Ms5 => Some(5),
            Self::Ms10 => Some(10),
            Self::Ms25 => Some(25),
            Self::Ms50 => Some(50),
            Self::Ms100 => Some(100),
            Self::Static => None,
        }
    }

    pub fn all_buckets() -> &'static [HedgeDelayBucket] {
        &[
            Self::Ms5,
            Self::Ms10,
            Self::Ms25,
            Self::Ms50,
            Self::Ms100,
            Self::Static,
        ]
    }
}

/// Request class limited to stable operational buckets.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum DurabilityMode {
    Sync,
    Bounded,
    Periodic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum SizeBucket {
    Small,
    Medium,
    Large,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct RequestClass {
    pub durability: DurabilityMode,
    pub size: SizeBucket,
}

/// Per-proposer rolling statistics.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProposerStats {
    /// Rolling decision-latency quantiles in microseconds (p50, p95, p99).
    pub latency_quantiles: [u64; 3],
    /// Success rate in [0.0, 1.0].
    pub success_rate: f64,
    /// Timeout rate in [0.0, 1.0].
    pub timeout_rate: f64,
    /// Current in-flight proposal count.
    pub in_flight: u32,
    /// Queue depth.
    pub queue_depth: u32,
    /// Recent contention rate in [0.0, 1.0].
    pub contention_rate: f64,
}

/// Aggregated proposer-to-recorder RPC statistics.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RpcStats {
    /// Median RPC latency in microseconds.
    pub latency_p50: u64,
    /// p95 RPC latency in microseconds.
    pub latency_p95: u64,
    /// Timeout rate in [0.0, 1.0].
    pub timeout_rate: f64,
    /// Error rate in [0.0, 1.0].
    pub error_rate: f64,
}

/// Node resource pressure signals.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodePressure {
    /// CPU utilization in [0.0, 1.0].
    pub cpu: f64,
    /// I/O pressure in [0.0, 1.0].
    pub io: f64,
    /// Event-loop or executor delay in microseconds.
    pub executor_delay_us: u64,
}

/// Bounded, versioned feature vector for the contextual bandit.
/// Inputs are lagged measurements available before action selection;
/// no outcome-derived or command-payload data is included.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FeatureVector {
    pub identity: Identity,
    /// Configuration epoch.
    pub epoch: Epoch,
    /// Number of voters in current membership.
    pub voter_count: u32,
    /// Which members are eligible proposers.
    pub eligible_proposers: Vec<NodeId>,
    /// Per-proposer statistics keyed by node id.
    pub proposer_stats: Vec<(NodeId, ProposerStats)>,
    /// Aggregated RPC statistics.
    pub rpc_stats: RpcStats,
    /// Node resource pressure.
    pub node_pressure: NodePressure,
    /// Request class.
    pub request_class: RequestClass,
    /// Feature age in microseconds (time since last telemetry update).
    pub feature_age_us: u64,
    /// Total sample count for cold-start gating.
    pub sample_count: u64,
    /// Whether telemetry data was incomplete or missing.
    pub missingness_flags: MissingnessFlags,
}

/// Flags indicating which telemetry signals were missing or stale.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MissingnessFlags {
    pub proposer_stats_missing: bool,
    pub rpc_stats_missing: bool,
    pub node_pressure_missing: bool,
    pub request_class_missing: bool,
}

/// An action is a pair: (first_request_target, hedge_delay).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Action {
    /// One currently eligible voter from the active membership.
    pub first_request_target: NodeId,
    /// Hedge delay bucket.
    pub hedge_delay: HedgeDelayBucket,
}

/// The complete action output with metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionOutput {
    /// The selected action.
    pub action: Action,
    /// Configuration identity this output is valid for.
    pub identity: Identity,
    /// Slot from which this output is valid.
    pub valid_from_slot: u64,
    /// Expiry timestamp in microseconds since epoch.
    pub expiry_us: u64,
    /// Policy version.
    pub policy_version: u32,
    /// Model version.
    pub model_version: u32,
    /// Whether this was an exploratory action.
    pub exploration: bool,
    /// Confidence in [0.0, 1.0].
    pub confidence: f64,
    /// If this was a fallback, the reason.
    pub fallback_reason: Option<FallbackReason>,
}

/// Candidate set rebuilt for each exact configuration identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandidateSet {
    pub identity: Identity,
    pub eligible_voters: Vec<NodeId>,
    pub hedge_delay_allowlist: Vec<HedgeDelayBucket>,
    pub static_hedge_delay_ms: u64,
}

/// Reasons for falling back to static policy.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum FallbackReason {
    ModelTimeout,
    ModelCrash,
    InvalidOutput,
    StaleFeatures,
    StorageCorruption,
    ConfigMismatch,
    ConfidenceBelowThreshold,
    GuardrailBreach,
    KillSwitchActive,
    ColdStart,
    ModelServiceLost,
    MembershipChange,
    FreshnessExpired,
}

/// Terminal outcome of a request for reward computation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TerminalOutcome {
    Success {
        decision_latency_us: u64,
        additional_rpcs: u32,
        duplicate_proposer_work: bool,
        contention_or_round_escalation: bool,
    },
    Timeout,
    Error {
        additional_rpcs: u32,
        duplicate_proposer_work: bool,
    },
    /// Censored: cancelled, reconfigured, or telemetry-incomplete.
    Censored,
}

/// Reward components for observability.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RewardComponents {
    pub normalized_latency: f64,
    pub rpc_penalty: f64,
    pub work_penalty: f64,
    pub contention_penalty: f64,
    pub error_penalty: f64,
    pub total: f64,
    pub censored: bool,
}

/// Observability action record keyed by request correlation ID.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionRecord {
    pub correlation_id: String,
    pub configuration_epoch: Epoch,
    pub feature_version: u32,
    pub model_version: u32,
    pub chosen_proposer: NodeId,
    pub chosen_delay: HedgeDelayBucket,
    pub baseline_proposer: NodeId,
    pub baseline_delay: HedgeDelayBucket,
    pub exploration: bool,
    pub confidence: f64,
    pub fallback_reason: Option<FallbackReason>,
    pub terminal_outcome: Option<TerminalOutcome>,
    pub latency_us: Option<u64>,
    pub duplicate_work: bool,
    pub reward_components: Option<RewardComponents>,
}
