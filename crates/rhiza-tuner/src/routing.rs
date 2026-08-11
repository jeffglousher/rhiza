//! Safe, runtime-controlled routing tuner.  It only learns a first target;
//! hedging remains the caller's fixed static policy in v1.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use crate::killswitch::KillSwitch;
use crate::rollout::RolloutGuard;
use crate::{Identity, NodeId, RequestClass, RolloutStage};

const FIXED_HEDGE_DELAY: Duration = Duration::from_millis(100);
const MAX_PENDING_TICKETS: usize = 4_096;

/// Identity of a routing policy and the consensus configuration it applies to.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct LearningIdentity {
    pub identity: Identity,
    pub routing_policy_version: u32,
}

impl LearningIdentity {
    pub fn new(identity: Identity, routing_policy_version: u32) -> Self {
        Self {
            identity,
            routing_policy_version,
        }
    }

    pub fn consensus_identity(&self) -> &Identity {
        &self.identity
    }
}

/// One trusted node-to-transport mapping available to routing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutingEndpoint {
    node_id: NodeId,
    url: String,
    eligible: bool,
}

impl RoutingEndpoint {
    pub fn new(node_id: NodeId, url: impl Into<String>, eligible: bool) -> Self {
        Self {
            node_id,
            url: url.into(),
            eligible,
        }
    }
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }
    pub fn url(&self) -> &str {
        &self.url
    }
    pub fn eligible(&self) -> bool {
        self.eligible
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutingSnapshotError {
    Empty,
    EmptyNodeId,
    EmptyUrl,
    DuplicateNodeId(NodeId),
    DuplicateUrl(String),
    MissingStaticPrimary(NodeId),
    NoEligibleEndpoints,
}

/// A validated topology. Endpoint order supplies the static fallback order.
#[derive(Clone, Debug)]
pub struct RoutingSnapshot {
    identity: LearningIdentity,
    topology_generation: u64,
    endpoints: Vec<RoutingEndpoint>,
    static_primary: NodeId,
}

impl RoutingSnapshot {
    pub fn new(
        identity: LearningIdentity,
        topology_generation: u64,
        endpoints: Vec<RoutingEndpoint>,
        static_primary: NodeId,
    ) -> Result<Self, RoutingSnapshotError> {
        if endpoints.is_empty() {
            return Err(RoutingSnapshotError::Empty);
        }
        let mut node_ids = HashSet::new();
        let mut urls = HashSet::new();
        for endpoint in &endpoints {
            if endpoint.node_id.trim().is_empty() {
                return Err(RoutingSnapshotError::EmptyNodeId);
            }
            if endpoint.url.trim().is_empty() {
                return Err(RoutingSnapshotError::EmptyUrl);
            }
            if !node_ids.insert(endpoint.node_id.clone()) {
                return Err(RoutingSnapshotError::DuplicateNodeId(
                    endpoint.node_id.clone(),
                ));
            }
            if !urls.insert(endpoint.url.clone()) {
                return Err(RoutingSnapshotError::DuplicateUrl(endpoint.url.clone()));
            }
        }
        endpoints
            .iter()
            .find(|endpoint| endpoint.node_id == static_primary)
            .ok_or_else(|| RoutingSnapshotError::MissingStaticPrimary(static_primary.clone()))?;
        if !endpoints.iter().any(|endpoint| endpoint.eligible) {
            return Err(RoutingSnapshotError::NoEligibleEndpoints);
        }
        Ok(Self {
            identity,
            topology_generation,
            endpoints,
            static_primary,
        })
    }

    pub fn identity(&self) -> &LearningIdentity {
        &self.identity
    }
    pub fn topology_generation(&self) -> u64 {
        self.topology_generation
    }
    pub fn endpoints(&self) -> &[RoutingEndpoint] {
        &self.endpoints
    }
    pub fn static_primary(&self) -> &NodeId {
        &self.static_primary
    }
    pub fn static_order(&self) -> Vec<NodeId> {
        let mut order = self
            .endpoints
            .iter()
            .map(|endpoint| endpoint.node_id.clone())
            .collect::<Vec<_>>();
        move_first(&mut order, &self.static_primary);
        order
    }

    fn eligible_order(&self) -> Vec<NodeId> {
        let mut order = self
            .endpoints
            .iter()
            .filter(|endpoint| endpoint.eligible)
            .map(|endpoint| endpoint.node_id.clone())
            .collect::<Vec<_>>();
        move_first(&mut order, &self.static_primary);
        order
    }
}

impl std::fmt::Display for RoutingSnapshotError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => formatter.write_str("routing snapshot has no endpoints"),
            Self::EmptyNodeId => formatter.write_str("routing endpoint has an empty node id"),
            Self::EmptyUrl => formatter.write_str("routing endpoint has an empty URL"),
            Self::DuplicateNodeId(node_id) => {
                write!(formatter, "duplicate routing node id: {node_id}")
            }
            Self::DuplicateUrl(url) => write!(formatter, "duplicate routing URL: {url}"),
            Self::MissingStaticPrimary(node_id) => {
                write!(
                    formatter,
                    "static primary is not in the snapshot: {node_id}"
                )
            }
            Self::NoEligibleEndpoints => {
                formatter.write_str("routing snapshot has no eligible endpoints")
            }
        }
    }
}

impl std::error::Error for RoutingSnapshotError {}

#[derive(Clone, Debug)]
pub struct RoutingConfig {
    pub canary_basis_points: u16,
    pub ticket_ttl: Duration,
    pub ucb_coefficient: f64,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            canary_basis_points: 0,
            ticket_ttl: Duration::from_secs(30),
            ucb_coefficient: 0.5,
        }
    }
}

impl RoutingConfig {
    fn normalized(&self) -> Self {
        Self {
            canary_basis_points: self.canary_basis_points.min(10_000),
            ticket_ttl: self.ticket_ttl,
            ucb_coefficient: self.ucb_coefficient.max(0.0),
        }
    }
}

/// The actual attempts performed by a caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptTrace {
    pub node_id: NodeId,
    pub started_after: Duration,
    pub completed_after: Option<Duration>,
    pub outcome: AttemptOutcome,
}
impl AttemptTrace {
    pub fn new(node_id: NodeId, outcome: AttemptOutcome) -> Self {
        Self {
            node_id,
            started_after: Duration::ZERO,
            completed_after: None,
            outcome,
        }
    }

    pub fn with_timing(
        node_id: NodeId,
        started_after: Duration,
        completed_after: Option<Duration>,
        outcome: AttemptOutcome,
    ) -> Self {
        Self {
            node_id,
            started_after,
            completed_after,
            outcome,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttemptOutcome {
    Success { latency: Duration },
    Timeout,
    Error,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionTrace {
    pub correlation_id: String,
    pub attempts: Vec<AttemptTrace>,
    pub hedge_launched_at: Option<Duration>,
    pub winner: Option<NodeId>,
    pub terminal_outcome: AttemptOutcome,
}
impl ExecutionTrace {
    pub fn new(
        correlation_id: impl Into<String>,
        attempts: Vec<AttemptTrace>,
        terminal_outcome: AttemptOutcome,
    ) -> Self {
        let winner = attempts.iter().find_map(|attempt| {
            matches!(attempt.outcome, AttemptOutcome::Success { .. })
                .then(|| attempt.node_id.clone())
        });
        Self {
            correlation_id: correlation_id.into(),
            attempts,
            hedge_launched_at: None,
            winner,
            terminal_outcome,
        }
    }

    pub fn with_metadata(
        correlation_id: impl Into<String>,
        attempts: Vec<AttemptTrace>,
        hedge_launched_at: Option<Duration>,
        winner: Option<NodeId>,
        terminal_outcome: AttemptOutcome,
    ) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            attempts,
            hedge_launched_at,
            winner,
            terminal_outcome,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CensorReason {
    UnknownTicket,
    ExpiredTicket,
    IdentityMismatch,
    TopologyMismatch,
    CorrelationMismatch,
    FirstAttemptMismatch,
    ActionNotApplied,
    StageNotLearnable,
    KillSwitchActive,
    RoutingStateUnavailable,
    TelemetryIncomplete,
    Cancelled,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservationResult {
    Updated,
    Censored(CensorReason),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RoutingMetrics {
    pub plans: u64,
    pub applied_plans: u64,
    pub static_plans: u64,
    pub updated_observations: u64,
    pub censored_observations: u64,
    pub plan_execution_mismatches: u64,
    pub topology_mismatches: u64,
    pub ticket_replays: u64,
    pub kill_switch_fallbacks: u64,
}

#[derive(Default)]
struct RoutingCounters {
    plans: AtomicU64,
    applied_plans: AtomicU64,
    static_plans: AtomicU64,
    updated_observations: AtomicU64,
    censored_observations: AtomicU64,
    plan_execution_mismatches: AtomicU64,
    topology_mismatches: AtomicU64,
    ticket_replays: AtomicU64,
    observation_admission_shed: AtomicU64,
    kill_switch_fallbacks: AtomicU64,
}

/// A single-use capability issued with a routing plan. Its internals are intentionally private.
#[derive(Debug)]
pub struct ObservationTicket {
    id: u64,
}

#[derive(Debug)]
pub struct RoutingPlan {
    pub actual_order: Vec<NodeId>,
    pub model_order: Vec<NodeId>,
    pub hedge_delay: Duration,
    pub stage: RolloutStage,
    pub proposer_applied: bool,
    pub is_shadow: bool,
    pub selection_propensity: f64,
    pub ticket: ObservationTicket,
}

#[derive(Default)]
struct Arm {
    count: u64,
    mean_reward: f64,
}
struct Pending {
    identity: LearningIdentity,
    topology_generation: u64,
    correlation_id: String,
    first_actual: NodeId,
    first_model: NodeId,
    stage: RolloutStage,
    applied: bool,
    learnable: bool,
    expires_at_ms: u64,
}
#[derive(Default)]
struct State {
    identity: Option<LearningIdentity>,
    next_ticket: u64,
    arms: BTreeMap<NodeId, Arm>,
    pending: BTreeMap<u64, Pending>,
}
/// Deterministic first-target UCB router. No hedge combinations are modelled.
pub struct RoutingTuner {
    config: RoutingConfig,
    state: Mutex<State>,
    rollout: RolloutGuard,
    killswitch: KillSwitch,
    counters: RoutingCounters,
}

impl RoutingTuner {
    pub fn new(config: RoutingConfig) -> Self {
        Self::with_stage(config, RolloutStage::Disabled)
    }
    pub fn with_stage(config: RoutingConfig, stage: RolloutStage) -> Self {
        Self {
            config: config.normalized(),
            state: Mutex::new(State::default()),
            rollout: RolloutGuard::with_stage(stage),
            killswitch: KillSwitch::new(),
            counters: RoutingCounters::default(),
        }
    }
    pub fn set_stage(&self, stage: RolloutStage) {
        self.rollout.set_stage(stage);
    }
    pub fn stage(&self) -> RolloutStage {
        self.rollout.stage()
    }
    pub fn activate_kill_switch(&self, reason: impl Into<String>) {
        self.killswitch.activate(reason);
    }
    pub fn deactivate_kill_switch(&self) {
        self.killswitch.deactivate();
    }
    pub fn is_killed(&self) -> bool {
        self.killswitch.is_active()
    }

    pub fn metrics(&self) -> RoutingMetrics {
        RoutingMetrics {
            plans: self.counters.plans.load(Ordering::Relaxed),
            applied_plans: self.counters.applied_plans.load(Ordering::Relaxed),
            static_plans: self.counters.static_plans.load(Ordering::Relaxed),
            updated_observations: self.counters.updated_observations.load(Ordering::Relaxed),
            censored_observations: self.counters.censored_observations.load(Ordering::Relaxed),
            plan_execution_mismatches: self
                .counters
                .plan_execution_mismatches
                .load(Ordering::Relaxed),
            topology_mismatches: self.counters.topology_mismatches.load(Ordering::Relaxed),
            ticket_replays: self.counters.ticket_replays.load(Ordering::Relaxed),
            kill_switch_fallbacks: self.counters.kill_switch_fallbacks.load(Ordering::Relaxed),
        }
    }

    /// Number of plans forced to static routing because observation capacity was full.
    pub fn observation_admission_shed_count(&self) -> u64 {
        self.counters
            .observation_admission_shed
            .load(Ordering::Relaxed)
    }

    pub fn plan(
        &self,
        snapshot: &RoutingSnapshot,
        request_class: RequestClass,
        correlation_id: impl AsRef<str>,
    ) -> RoutingPlan {
        let correlation_id = correlation_id.as_ref().to_owned();
        self.plan_for_cohort(snapshot, request_class, &correlation_id, &correlation_id)
    }

    /// Plan a request while assigning rollout from a stable caller cohort.
    pub fn plan_for_cohort(
        &self,
        snapshot: &RoutingSnapshot,
        _request_class: RequestClass,
        correlation_id: impl AsRef<str>,
        cohort_id: impl AsRef<str>,
    ) -> RoutingPlan {
        let correlation_id = correlation_id.as_ref();
        self.counters.plans.fetch_add(1, Ordering::Relaxed);
        let static_order = snapshot.static_order();
        let eligible_order = snapshot.eligible_order();
        let stage = self.stage();
        let killed = self.killswitch.is_active();
        let (mut state, state_available) = match self.state.lock() {
            Ok(state) => (state, true),
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                *state = State::default();
                self.state.clear_poison();
                (state, false)
            }
        };
        if state.identity.as_ref() != Some(snapshot.identity()) {
            state.identity = Some(snapshot.identity().clone());
            state.arms.clear();
            state.pending.clear();
        }
        let now = now_ms();
        state
            .pending
            .retain(|_, pending| pending.expires_at_ms >= now);
        let observation_admitted = state.pending.len() < MAX_PENDING_TICKETS;
        let exploration = stage.exploration_enabled();
        let candidate = if state_available && observation_admitted {
            select_candidate(
                &mut state.arms,
                &eligible_order,
                self.config.ucb_coefficient,
                exploration,
            )
        } else {
            static_order[0].clone()
        };
        let mut model_order = static_order.clone();
        move_first(&mut model_order, &candidate);
        let canary = in_canary(
            cohort_id.as_ref(),
            snapshot.identity(),
            self.config.canary_basis_points,
        );
        let proposer_applied = state_available
            && observation_admitted
            && !killed
            && match stage {
                RolloutStage::ProposerCanary | RolloutStage::HedgeCanary => canary,
                RolloutStage::ProposerDefault
                | RolloutStage::BoundedDefault
                | RolloutStage::DefaultOn => true,
                _ => false,
            };
        if proposer_applied {
            self.counters.applied_plans.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.static_plans.fetch_add(1, Ordering::Relaxed);
        }
        if killed {
            self.counters
                .kill_switch_fallbacks
                .fetch_add(1, Ordering::Relaxed);
        }
        let actual_order = if proposer_applied {
            model_order.clone()
        } else {
            static_order
        };
        let learnable = proposer_applied
            && matches!(
                stage,
                RolloutStage::ProposerCanary
                    | RolloutStage::HedgeCanary
                    | RolloutStage::ProposerDefault
                    | RolloutStage::BoundedDefault
                    | RolloutStage::DefaultOn
            );
        let selection_propensity = if !proposer_applied {
            0.0
        } else {
            match stage {
                RolloutStage::ProposerCanary | RolloutStage::HedgeCanary => {
                    self.config.canary_basis_points as f64 / 10_000.0
                }
                RolloutStage::ProposerDefault
                | RolloutStage::BoundedDefault
                | RolloutStage::DefaultOn => 1.0,
                _ => 0.0,
            }
        };
        let id = if observation_admitted {
            state.next_ticket = state.next_ticket.wrapping_add(1).max(1);
            let id = state.next_ticket;
            state.pending.insert(
                id,
                Pending {
                    identity: snapshot.identity().clone(),
                    topology_generation: snapshot.topology_generation(),
                    correlation_id: correlation_id.into(),
                    first_actual: actual_order[0].clone(),
                    first_model: model_order[0].clone(),
                    stage,
                    applied: proposer_applied,
                    learnable,
                    expires_at_ms: now.saturating_add(self.config.ticket_ttl.as_millis() as u64),
                },
            );
            id
        } else {
            self.counters
                .observation_admission_shed
                .fetch_add(1, Ordering::Relaxed);
            // Zero is reserved for a static plan whose observation was not admitted.
            0
        };
        RoutingPlan {
            actual_order,
            model_order,
            hedge_delay: FIXED_HEDGE_DELAY,
            stage,
            proposer_applied,
            is_shadow: stage == RolloutStage::Shadow,
            selection_propensity,
            ticket: ObservationTicket { id },
        }
    }

    pub fn observe(
        &self,
        ticket: ObservationTicket,
        snapshot: &RoutingSnapshot,
        trace: ExecutionTrace,
    ) -> ObservationResult {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                *state = State::default();
                self.state.clear_poison();
                return self.censored(CensorReason::RoutingStateUnavailable);
            }
        };
        if ticket.id == 0 {
            return self.censored(CensorReason::ActionNotApplied);
        }
        let Some(pending) = state.pending.remove(&ticket.id) else {
            return self.censored(CensorReason::UnknownTicket);
        };
        if pending.expires_at_ms < now_ms() {
            return self.censored(CensorReason::ExpiredTicket);
        }
        if &pending.identity != snapshot.identity() {
            return self.censored(CensorReason::IdentityMismatch);
        }
        if pending.topology_generation != snapshot.topology_generation() {
            return self.censored(CensorReason::TopologyMismatch);
        }
        if pending.correlation_id != trace.correlation_id {
            return self.censored(CensorReason::CorrelationMismatch);
        }
        if trace.attempts.first().map(|a| &a.node_id) != Some(&pending.first_actual) {
            return self.censored(CensorReason::FirstAttemptMismatch);
        }
        let successful_attempt = trace.attempts.iter().find(|attempt| {
            matches!(attempt.outcome, AttemptOutcome::Success { .. })
                && trace.winner.as_ref() == Some(&attempt.node_id)
        });
        if matches!(trace.terminal_outcome, AttemptOutcome::Success { .. })
            != successful_attempt.is_some()
            || trace.attempts.iter().any(|attempt| {
                attempt
                    .completed_after
                    .is_some_and(|completed| completed < attempt.started_after)
            })
        {
            return self.censored(CensorReason::TelemetryIncomplete);
        }
        if !pending.applied {
            return self.censored(CensorReason::ActionNotApplied);
        }
        if pending.first_actual != pending.first_model {
            return self.censored(CensorReason::FirstAttemptMismatch);
        }
        if !pending.learnable {
            return self.censored(CensorReason::StageNotLearnable);
        }
        if !matches!(
            pending.stage,
            RolloutStage::ProposerCanary
                | RolloutStage::HedgeCanary
                | RolloutStage::ProposerDefault
                | RolloutStage::BoundedDefault
                | RolloutStage::DefaultOn
        ) {
            return self.censored(CensorReason::StageNotLearnable);
        }
        if self.killswitch.is_active() {
            return self.censored(CensorReason::KillSwitchActive);
        }
        let reward = match &trace.attempts[0].outcome {
            AttemptOutcome::Success { latency } => 1.0 / (1.0 + latency.as_secs_f64() * 1_000.0),
            AttemptOutcome::Timeout | AttemptOutcome::Error | AttemptOutcome::Cancelled => 0.0,
        };
        let arm = state.arms.entry(pending.first_actual).or_default();
        arm.count += 1;
        // Discount old observations so the small model responds to changed conditions.
        arm.mean_reward = if arm.count == 1 {
            reward
        } else {
            arm.mean_reward * 0.95 + reward * 0.05
        };
        self.counters
            .updated_observations
            .fetch_add(1, Ordering::Relaxed);
        ObservationResult::Updated
    }

    fn censored(&self, reason: CensorReason) -> ObservationResult {
        self.counters
            .censored_observations
            .fetch_add(1, Ordering::Relaxed);
        match reason {
            CensorReason::FirstAttemptMismatch
            | CensorReason::CorrelationMismatch
            | CensorReason::TelemetryIncomplete => {
                self.counters
                    .plan_execution_mismatches
                    .fetch_add(1, Ordering::Relaxed);
            }
            CensorReason::TopologyMismatch | CensorReason::IdentityMismatch => {
                self.counters
                    .topology_mismatches
                    .fetch_add(1, Ordering::Relaxed);
            }
            CensorReason::UnknownTicket => {
                self.counters.ticket_replays.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        ObservationResult::Censored(reason)
    }
}

fn select_candidate(
    arms: &mut BTreeMap<NodeId, Arm>,
    order: &[NodeId],
    coefficient: f64,
    exploration: bool,
) -> NodeId {
    let total: u64 = arms.values().map(|arm| arm.count).sum();
    if exploration && total > 0 {
        if let Some(untried) = order.iter().find(|node| arms[*node].count == 0) {
            return untried.clone();
        }
    }
    let mut best = order[0].clone();
    let mut best_score = f64::NEG_INFINITY;
    for node in order {
        let arm = arms.entry(node.clone()).or_default();
        // Static primary wins exact ties and receives a small conservative prior.
        let mean = if arm.count == 0 {
            if node == &order[0] {
                0.05
            } else {
                0.0
            }
        } else {
            arm.mean_reward
        };
        let bonus = if exploration {
            coefficient * (((total + 1) as f64).ln() / (arm.count + 1) as f64).sqrt()
        } else {
            0.0
        };
        let score = mean + bonus;
        if score > best_score {
            best_score = score;
            best = node.clone();
        }
    }
    best
}

fn move_first(order: &mut Vec<NodeId>, node: &NodeId) {
    if let Some(index) = order.iter().position(|value| value == node) {
        let node = order.remove(index);
        order.insert(0, node);
    }
}
fn in_canary(correlation_id: &str, identity: &LearningIdentity, basis_points: u16) -> bool {
    if basis_points == 0 {
        return false;
    }
    if basis_points >= 10_000 {
        return true;
    }
    let mut hasher = Sha256::new();
    hasher.update(identity.identity.digest());
    hasher.update(identity.routing_policy_version.to_le_bytes());
    hasher.update(correlation_id.as_bytes());
    let hash = hasher.finalize();
    u16::from_le_bytes([hash[0], hash[1]]) as u32 % 10_000 < basis_points as u32
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity() -> LearningIdentity {
        LearningIdentity::new(
            Identity {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                membership_digest: [0; 32],
                recovery_generation: 0,
            },
            1,
        )
    }
    fn request() -> RequestClass {
        RequestClass {
            durability: crate::DurabilityMode::Sync,
            size: crate::SizeBucket::Small,
        }
    }
    fn snapshot(generation: u64) -> RoutingSnapshot {
        RoutingSnapshot::new(
            identity(),
            generation,
            vec![
                RoutingEndpoint::new("a".into(), "http://a", true),
                RoutingEndpoint::new("b".into(), "http://b", true),
            ],
            "a".into(),
        )
        .unwrap()
    }
    fn trace(correlation_id: &str, node_id: &str, latency: Duration) -> ExecutionTrace {
        ExecutionTrace::new(
            correlation_id,
            vec![AttemptTrace::new(
                node_id.into(),
                AttemptOutcome::Success { latency },
            )],
            AttemptOutcome::Success { latency },
        )
    }
    #[test]
    fn snapshot_validation() {
        assert!(RoutingSnapshot::new(identity(), 1, vec![], "a".into()).is_err());
        assert!(matches!(
            RoutingSnapshot::new(
                identity(),
                1,
                vec![RoutingEndpoint::new("a".into(), "", true)],
                "a".into(),
            ),
            Err(RoutingSnapshotError::EmptyUrl)
        ));
        assert!(RoutingSnapshot::new(
            identity(),
            1,
            vec![
                RoutingEndpoint::new("a".into(), "u", true),
                RoutingEndpoint::new("a".into(), "v", true)
            ],
            "a".into()
        )
        .is_err());
    }

    #[test]
    fn static_route_and_eligible_candidates_are_independent() {
        let snapshot = RoutingSnapshot::new(
            identity(),
            1,
            vec![
                RoutingEndpoint::new("a".into(), "http://a", false),
                RoutingEndpoint::new("b".into(), "http://b", true),
            ],
            "a".into(),
        )
        .unwrap();
        assert_eq!(
            snapshot.static_order(),
            vec![NodeId::from("a"), NodeId::from("b")]
        );
        assert_eq!(snapshot.eligible_order(), vec![NodeId::from("b")]);

        let disabled = RoutingTuner::new(RoutingConfig::default());
        assert_eq!(
            disabled.plan(&snapshot, request(), "static").actual_order,
            vec![NodeId::from("a"), NodeId::from("b")],
        );
        let canary = RoutingTuner::with_stage(
            RoutingConfig {
                canary_basis_points: 10_000,
                ..Default::default()
            },
            RolloutStage::ProposerCanary,
        );
        assert_eq!(
            canary.plan(&snapshot, request(), "healthy").actual_order,
            vec![NodeId::from("b"), NodeId::from("a")],
        );
    }
    #[test]
    fn shadow_keeps_static_and_never_learns() {
        let tuner = RoutingTuner::with_stage(RoutingConfig::default(), RolloutStage::Shadow);
        let snapshot = snapshot(1);
        let plan = tuner.plan(&snapshot, request(), "x");
        assert_eq!(
            plan.actual_order,
            vec![NodeId::from("a"), NodeId::from("b")]
        );
        assert_eq!(
            tuner.observe(
                plan.ticket,
                &snapshot,
                ExecutionTrace::new(
                    "x",
                    vec![AttemptTrace::new(
                        "a".into(),
                        AttemptOutcome::Success {
                            latency: Duration::from_millis(1)
                        }
                    )],
                    AttemptOutcome::Success {
                        latency: Duration::from_millis(1)
                    }
                )
            ),
            ObservationResult::Censored(CensorReason::ActionNotApplied)
        );
    }
    #[test]
    fn applied_observation_and_mismatch_are_fenced() {
        let tuner = RoutingTuner::with_stage(
            RoutingConfig {
                canary_basis_points: 10_000,
                ..Default::default()
            },
            RolloutStage::ProposerCanary,
        );
        let snapshot = snapshot(1);
        let plan = tuner.plan(&snapshot, request(), "x");
        assert_eq!(plan.hedge_delay, Duration::from_millis(100));
        assert_eq!(
            tuner.observe(
                plan.ticket,
                &snapshot,
                ExecutionTrace::new(
                    "wrong",
                    vec![AttemptTrace::new(
                        "a".into(),
                        AttemptOutcome::Success {
                            latency: Duration::ZERO
                        }
                    )],
                    AttemptOutcome::Success {
                        latency: Duration::ZERO
                    }
                )
            ),
            ObservationResult::Censored(CensorReason::CorrelationMismatch)
        );
        let plan = tuner.plan(&snapshot, request(), "y");
        assert_eq!(
            tuner.observe(
                plan.ticket,
                &snapshot,
                ExecutionTrace::new(
                    "y",
                    vec![AttemptTrace::new(
                        "a".into(),
                        AttemptOutcome::Success {
                            latency: Duration::ZERO
                        }
                    )],
                    AttemptOutcome::Success {
                        latency: Duration::ZERO
                    }
                )
            ),
            ObservationResult::Updated
        );
    }

    #[test]
    fn fallback_success_does_not_reward_the_failed_first_target() {
        let tuner = RoutingTuner::with_stage(
            RoutingConfig {
                canary_basis_points: 10_000,
                ..Default::default()
            },
            RolloutStage::ProposerCanary,
        );
        let snapshot = snapshot(1);
        let plan = tuner.plan(&snapshot, request(), "fallback");
        let first = plan.actual_order[0].clone();
        let second = plan.actual_order[1].clone();
        let trace = ExecutionTrace::with_metadata(
            "fallback",
            vec![
                AttemptTrace::new(first.clone(), AttemptOutcome::Error),
                AttemptTrace::new(
                    second.clone(),
                    AttemptOutcome::Success {
                        latency: Duration::from_millis(1),
                    },
                ),
            ],
            Some(Duration::from_millis(100)),
            Some(second),
            AttemptOutcome::Success {
                latency: Duration::from_millis(101),
            },
        );

        assert_eq!(
            tuner.observe(plan.ticket, &snapshot, trace),
            ObservationResult::Updated,
        );
        let state = tuner.state.lock().unwrap();
        let first_arm = state.arms.get(&first).unwrap();
        assert_eq!(first_arm.count, 1);
        assert_eq!(first_arm.mean_reward, 0.0);
    }

    #[test]
    fn topology_change_and_ticket_replay_are_censored() {
        let tuner = RoutingTuner::with_stage(
            RoutingConfig {
                canary_basis_points: 10_000,
                ..Default::default()
            },
            RolloutStage::ProposerCanary,
        );
        let original = snapshot(1);
        let changed = snapshot(2);
        let plan = tuner.plan(&original, request(), "topology");
        assert_eq!(
            tuner.observe(
                plan.ticket,
                &changed,
                trace("topology", &plan.actual_order[0], Duration::from_millis(1)),
            ),
            ObservationResult::Censored(CensorReason::TopologyMismatch),
        );

        let plan = tuner.plan(&original, request(), "single-use");
        let ticket_id = plan.ticket.id;
        assert_eq!(
            tuner.observe(
                plan.ticket,
                &original,
                trace(
                    "single-use",
                    &plan.actual_order[0],
                    Duration::from_millis(1),
                ),
            ),
            ObservationResult::Updated,
        );
        assert_eq!(
            tuner.observe(
                ObservationTicket { id: ticket_id },
                &original,
                trace("single-use", "a", Duration::from_millis(1)),
            ),
            ObservationResult::Censored(CensorReason::UnknownTicket),
        );
        let metrics = tuner.metrics();
        assert_eq!(metrics.updated_observations, 1);
        assert_eq!(metrics.topology_mismatches, 1);
        assert_eq!(metrics.ticket_replays, 1);
    }
    #[test]
    fn stage_kill_and_canary_are_runtime_deterministic() {
        assert_eq!(
            in_canary("same", &identity(), 3_000),
            in_canary("same", &identity(), 3_000)
        );
        let tuner = RoutingTuner::new(RoutingConfig {
            canary_basis_points: 10_000,
            ..Default::default()
        });
        let snapshot = snapshot(1);
        assert!(!tuner.plan(&snapshot, request(), "x").proposer_applied);
        tuner.set_stage(RolloutStage::ProposerCanary);
        assert!(tuner.plan(&snapshot, request(), "x").proposer_applied);
        tuner.activate_kill_switch("test");
        assert!(!tuner.plan(&snapshot, request(), "x").proposer_applied);
    }

    #[test]
    fn stable_cohort_does_not_change_assignment_between_requests() {
        let tuner = RoutingTuner::with_stage(
            RoutingConfig {
                canary_basis_points: 5_000,
                ..Default::default()
            },
            RolloutStage::ProposerCanary,
        );
        let snapshot = snapshot(1);
        let first = tuner.plan_for_cohort(&snapshot, request(), "request-a", "client-cohort");
        let second = tuner.plan_for_cohort(&snapshot, request(), "request-b", "client-cohort");
        assert_eq!(first.proposer_applied, second.proposer_applied);
        assert_eq!(first.selection_propensity, second.selection_propensity);
    }

    #[test]
    fn pending_observation_tickets_are_bounded() {
        let tuner = RoutingTuner::with_stage(RoutingConfig::default(), RolloutStage::DefaultOn);
        let snapshot = snapshot(1);
        let first = tuner.plan(&snapshot, request(), "first");
        for index in 1..MAX_PENDING_TICKETS {
            tuner.plan(&snapshot, request(), format!("request-{index}"));
        }
        let overflow = tuner.plan(&snapshot, request(), "overflow");
        assert!(!overflow.proposer_applied);
        assert_eq!(overflow.actual_order, snapshot.static_order());
        assert_eq!(
            tuner.state.lock().unwrap().pending.len(),
            MAX_PENDING_TICKETS
        );
        assert_eq!(
            tuner.observe(
                first.ticket,
                &snapshot,
                trace("first", "a", Duration::from_millis(1)),
            ),
            ObservationResult::Updated,
        );
        assert_eq!(
            tuner.observe(
                overflow.ticket,
                &snapshot,
                trace("overflow", "a", Duration::from_millis(1)),
            ),
            ObservationResult::Censored(CensorReason::ActionNotApplied),
        );
        assert_eq!(tuner.observation_admission_shed_count(), 1);
        assert_eq!(tuner.metrics().ticket_replays, 0);
    }

    #[test]
    fn untried_endpoint_is_explored_then_can_be_preferred() {
        let tuner = RoutingTuner::with_stage(
            RoutingConfig {
                canary_basis_points: 10_000,
                ..Default::default()
            },
            RolloutStage::ProposerCanary,
        );
        let snapshot = RoutingSnapshot::new(
            identity(),
            1,
            vec![
                RoutingEndpoint::new("a".into(), "http://a", true),
                RoutingEndpoint::new("b".into(), "http://b", true),
                RoutingEndpoint::new("c".into(), "http://c", true),
            ],
            "a".into(),
        )
        .unwrap();

        let first = tuner.plan(&snapshot, request(), "first");
        assert_eq!(
            first.actual_order,
            vec![NodeId::from("a"), NodeId::from("b"), NodeId::from("c")]
        );
        assert_eq!(
            tuner.observe(
                first.ticket,
                &snapshot,
                trace("first", "a", Duration::from_millis(100))
            ),
            ObservationResult::Updated
        );

        let second = tuner.plan(&snapshot, request(), "second");
        assert_eq!(
            second.actual_order,
            vec![NodeId::from("b"), NodeId::from("a"), NodeId::from("c")]
        );
        assert_eq!(
            tuner.observe(
                second.ticket,
                &snapshot,
                trace("second", "b", Duration::from_millis(1))
            ),
            ObservationResult::Updated
        );

        let third = tuner.plan(&snapshot, request(), "third");
        assert_eq!(third.actual_order[0], NodeId::from("c"));
        assert_eq!(
            tuner.observe(
                third.ticket,
                &snapshot,
                trace("third", "c", Duration::from_millis(100))
            ),
            ObservationResult::Updated
        );
        assert_eq!(
            tuner.plan(&snapshot, request(), "fourth").actual_order[0],
            NodeId::from("b")
        );
    }
}
