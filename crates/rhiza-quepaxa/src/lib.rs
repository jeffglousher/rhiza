#![doc = include_str!("../README.md")]

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    cmp::Ordering as CmpOrdering,
    collections::{hash_map, BTreeMap, BTreeSet, HashMap, VecDeque},
    fmt, fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use rhiza_core::canonical_membership_digest;

pub use rhiza_core::{
    ClusterId, Command, CommandKind, ConfigChange, ConfigId, EntryType, Epoch, LogEntry, LogHash,
    LogIndex, NodeId, StoredCommand,
};

pub type Result<T> = std::result::Result<T, Error>;
pub type Slot = u64;
pub type Round = u64;
pub type Phase = u8;
pub type Step = u64;
pub type Priority = u128;

/// Read-only classification of a recorder root before startup recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecorderPreflight {
    Missing,
    Valid,
    Recoverable,
}

const RECORDER_STATE_VERSION: u16 = 4;
const CONFIGURATION_STATE_VERSION: u16 = 3;
const RECORD_WORKER_QUEUE_CAPACITY: usize = 1;
const CONTROL_WORKER_QUEUE_CAPACITY: usize = 1;

// The largest replicated command supported by the bounded WAL rotation policy below. The
// remaining limits follow directly from the on-disk envelopes: seven membership entries,
// two durable slot snapshots, and at most one oversized WAL frame past the soft limit.
const MAX_REPLICATED_COMMAND_BYTES: usize = 512 * 1024;
const MAX_COMMAND_CACHE_BYTES: usize = 4 + 2 + 1 + 8 + MAX_REPLICATED_COMMAND_BYTES + 32;
const MAX_CONFIGURATION_BYTES: usize = 512 * 1024;
const MAX_RECORDER_STATE_BYTES: usize = 1024 * 1024;
const MAX_RECORDED_HEAD_BYTES: usize = 2 * MAX_RECORDER_STATE_BYTES + MAX_CONFIGURATION_BYTES;
const MAX_TRANSITION_INTENT_BYTES: usize =
    MAX_RECORDER_STATE_BYTES + MAX_CONFIGURATION_BYTES + 4 + 2 + 8 + 2 + 2;
const MAX_CONFIGURATION_HEAD_INTENT_BYTES: usize =
    MAX_CONFIGURATION_BYTES + MAX_RECORDED_HEAD_BYTES + 4 + 2 + 8 + 8;
const MAX_RECORDER_WAL_BYTES: usize = 64 * 1024 * 1024 + 2 * 1024 * 1024;

#[cfg(any(target_os = "linux", target_os = "android"))]
const O_NOFOLLOW_FLAG: i32 = 0o400000;
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
const O_NOFOLLOW_FLAG: i32 = 0x0100;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    ChainConflict {
        slot: Slot,
        expected_prev_hash: LogHash,
        actual_prev_hash: LogHash,
    },
    CommandHashMismatch,
    CommandUnavailable,
    Cancelled,
    ConflictingCertificates,
    Decode(String),
    DuplicateRecorderIdentity,
    EmptyRecorderIdentity,
    EmptyFixedMembership,
    InvalidFixedMembershipSize,
    InvalidRecoveredTip,
    Io(String),
    NoQuorum,
    ProposeFailed,
    RandomnessUnavailable(String),
    RecorderRootLocked(PathBuf),
    Rejected(RejectReason),
    ReadFenceUnsupported,
    TypedProofInstallRequired,
    TypedRecordRequired,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChainConflict { slot, .. } => {
                write!(f, "QuePaxa predecessor conflicts at slot {slot}")
            }
            Self::CommandHashMismatch => write!(f, "QuePaxa command hash mismatch"),
            Self::CommandUnavailable => write!(f, "QuePaxa command bytes unavailable"),
            Self::Cancelled => write!(f, "QuePaxa proposal cancelled"),
            Self::ConflictingCertificates => {
                write!(f, "QuePaxa recovered conflicting decision certificates")
            }
            Self::Decode(message) => write!(f, "QuePaxa decode failed: {message}"),
            Self::DuplicateRecorderIdentity => write!(f, "recorder identities must be unique"),
            Self::EmptyRecorderIdentity => write!(f, "recorder identity must not be empty"),
            Self::EmptyFixedMembership => {
                write!(f, "fixed membership must include at least one node")
            }
            Self::InvalidFixedMembershipSize => {
                write!(f, "membership requires between three and seven recorders")
            }
            Self::InvalidRecoveredTip => write!(f, "recovered qlog next index must be positive"),
            Self::Io(message) => write!(f, "QuePaxa io failed: {message}"),
            Self::NoQuorum => write!(f, "QuePaxa quorum was not reached"),
            Self::ProposeFailed => write!(f, "QuePaxa propose failed"),
            Self::RandomnessUnavailable(message) => {
                write!(f, "QuePaxa OS randomness unavailable: {message}")
            }
            Self::RecorderRootLocked(root) => {
                write!(f, "recorder root is already owned: {}", root.display())
            }
            Self::Rejected(reason) => write!(f, "QuePaxa recorder rejected request: {reason:?}"),
            Self::ReadFenceUnsupported => {
                write!(f, "recorder does not implement context-bound read fences")
            }
            Self::TypedProofInstallRequired => {
                write!(
                    f,
                    "recorder does not implement typed decision-proof installation"
                )
            }
            Self::TypedRecordRequired => {
                write!(f, "recorder does not implement the typed Record operation")
            }
        }
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Membership {
    members: Vec<NodeId>,
    digest: LogHash,
}

impl Membership {
    pub fn new<const N: usize>(members: [&str; N]) -> Result<Self> {
        Self::from_voters(members.into_iter().map(String::from))
    }

    pub fn from_voters(voters: impl IntoIterator<Item = NodeId>) -> Result<Self> {
        Self::from_members(voters.into_iter().collect())
    }

    pub fn members(&self) -> &[NodeId] {
        &self.members
    }

    pub fn contains(&self, recorder_id: &str) -> bool {
        self.members
            .binary_search_by(|member| member.as_str().cmp(recorder_id))
            .is_ok()
    }

    pub const fn digest(&self) -> LogHash {
        self.digest
    }

    pub fn quorum_size(&self) -> usize {
        quorum_size(self.members.len())
    }

    fn from_members(members: Vec<NodeId>) -> Result<Self> {
        if members.is_empty() {
            return Err(Error::EmptyFixedMembership);
        }
        if !(3..=7).contains(&members.len()) {
            return Err(Error::InvalidFixedMembershipSize);
        }
        if members.iter().any(String::is_empty) {
            return Err(Error::EmptyRecorderIdentity);
        }
        let member_count = members.len();
        let members: Vec<_> = members
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if members.len() != member_count {
            return Err(Error::DuplicateRecorderIdentity);
        }
        Ok(Self {
            digest: canonical_membership_digest(&members)
                .map_err(|_| Error::InvalidFixedMembershipSize)?,
            members,
        })
    }
}

pub type FixedMembership = Membership;

pub trait Consensus {
    fn propose(&self, command: Command) -> Result<LogEntry>;
}

pub fn quorum_size(n: usize) -> usize {
    n / 2 + 1
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
pub struct Ballot {
    pub round: Round,
    pub priority: Priority,
    pub proposer_id: NodeId,
}

impl Ballot {
    pub fn new(round: Round, priority: Priority, proposer_id: impl Into<NodeId>) -> Self {
        Self {
            round,
            priority,
            proposer_id: proposer_id.into(),
        }
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct LogicalStep {
    pub round: Round,
    pub phase: Phase,
}

impl LogicalStep {
    pub const fn as_u64(&self) -> Step {
        self.round * 4 + self.phase as u64
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct ProposalPriority(pub [u8; 32]);

impl ProposalPriority {
    pub const ZERO: Self = Self([0; 32]);
    pub const MAX: Self = Self([u8::MAX; 32]);

    pub const fn from_u64(value: u64) -> Self {
        let mut bytes = [0; 32];
        let encoded = value.to_be_bytes();
        let mut index = 0;
        while index < encoded.len() {
            bytes[24 + index] = encoded[index];
            index += 1;
        }
        Self(bytes)
    }

    fn low_u128(self) -> u128 {
        u128::from_be_bytes(self.0[16..].try_into().expect("fixed priority suffix"))
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Proposal {
    pub priority: ProposalPriority,
    pub proposer_id: NodeId,
    pub proposal_id: u64,
    pub value: Option<AcceptedValue>,
}

impl Proposal {
    pub fn new(
        priority: ProposalPriority,
        proposer_id: impl Into<NodeId>,
        proposal_id: u64,
        value: AcceptedValue,
    ) -> Self {
        Self {
            priority,
            proposer_id: proposer_id.into(),
            proposal_id,
            value: Some(value),
        }
    }

    pub fn nil() -> Self {
        Self {
            priority: ProposalPriority::ZERO,
            proposer_id: String::new(),
            proposal_id: 0,
            value: None,
        }
    }

    fn identity(&self) -> (ProposalPriority, &str, u64, Option<&AcceptedValue>) {
        (
            self.priority,
            &self.proposer_id,
            self.proposal_id,
            self.value.as_ref(),
        )
    }

    fn is_nil(&self) -> bool {
        self.value.is_none()
    }
}

impl PartialEq for Proposal {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}

impl Eq for Proposal {}

impl PartialOrd for Proposal {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for Proposal {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        match (self.is_nil(), other.is_nil()) {
            (true, true) => CmpOrdering::Equal,
            (true, false) => CmpOrdering::Less,
            (false, true) => CmpOrdering::Greater,
            (false, false) => self.identity().cmp(&other.identity()),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IsrState {
    step: Step,
    first_current: Option<Proposal>,
    aggregate_current: Option<Proposal>,
    aggregate_prior: Option<Proposal>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct IsrReply {
    pub step: Step,
    pub first_current: Option<Proposal>,
    pub aggregate_prior: Option<Proposal>,
}

impl IsrState {
    pub const fn step(&self) -> Step {
        self.step
    }

    pub const fn first_current(&self) -> Option<&Proposal> {
        self.first_current.as_ref()
    }

    pub const fn aggregate_current(&self) -> Option<&Proposal> {
        self.aggregate_current.as_ref()
    }

    pub const fn aggregate_prior(&self) -> Option<&Proposal> {
        self.aggregate_prior.as_ref()
    }

    /// Pure Algorithm 3 transition. Stale inputs return an unchanged state.
    pub fn record(&self, step: Step, proposal: Proposal) -> (Self, IsrReply) {
        let mut next = self.clone();
        if step == next.step {
            if next.first_current.is_none() {
                next.first_current = Some(proposal.clone());
            }
            if next.aggregate_current.as_ref() < Some(&proposal) {
                next.aggregate_current = Some(proposal);
            }
        } else if step > next.step {
            next.aggregate_prior = if step == next.step.saturating_add(1) {
                next.aggregate_current.take()
            } else {
                None
            };
            next.step = step;
            next.first_current = Some(proposal.clone());
            next.aggregate_current = Some(proposal);
        }
        let reply = IsrReply {
            step: next.step,
            first_current: next.first_current.clone(),
            aggregate_prior: next.aggregate_prior.clone(),
        };
        (next, reply)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct AcceptedValue {
    pub command_hash: LogHash,
    pub prev_hash: LogHash,
    pub entry_hash: LogHash,
}

impl PartialOrd for AcceptedValue {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for AcceptedValue {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        (
            self.entry_hash.as_bytes(),
            self.command_hash.as_bytes(),
            self.prev_hash.as_bytes(),
        )
            .cmp(&(
                other.entry_hash.as_bytes(),
                other.command_hash.as_bytes(),
                other.prev_hash.as_bytes(),
            ))
    }
}

impl AcceptedValue {
    pub fn from_command(
        cluster_id: &str,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        prev_hash: LogHash,
        command: &StoredCommand,
    ) -> Self {
        Self {
            command_hash: command.hash(),
            prev_hash,
            entry_hash: LogEntry::calculate_hash(
                cluster_id,
                slot,
                epoch,
                config_id,
                command.entry_type,
                prev_hash,
                &command.payload,
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct AcceptedSummary {
    pub ballot: Ballot,
    pub value: AcceptedValue,
}

pub type ProposalSummary = AcceptedSummary;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct DecisionCertificate {
    pub slot: Slot,
    pub epoch: Epoch,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub ballot: Ballot,
    pub value: AcceptedValue,
    pub recorder_ids: Vec<NodeId>,
}

impl DecisionCertificate {
    pub fn cluster_id(&self) -> Option<&str> {
        decode_certificate_proposer(&self.ballot.proposer_id).map(|(cluster_id, _)| cluster_id)
    }

    pub fn validate_for_cluster(
        &self,
        cluster_id: &str,
        config_id: ConfigId,
        membership: &Membership,
    ) -> std::result::Result<(), RejectReason> {
        if self.cluster_id() != Some(cluster_id) {
            return Err(RejectReason::WrongCluster);
        }
        self.validate_for(config_id, membership)
    }

    pub fn validate(&self, membership: &FixedMembership) -> std::result::Result<(), RejectReason> {
        if self.config_digest != membership.digest() {
            return Err(RejectReason::WrongConfig);
        }
        if self.recorder_ids.len() != membership.quorum_size()
            || !self.recorder_ids.windows(2).all(|pair| pair[0] < pair[1])
            || self
                .recorder_ids
                .iter()
                .any(|recorder_id| !membership.contains(recorder_id))
        {
            return Err(RejectReason::InvalidCertificate);
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        config_id: ConfigId,
        membership: &Membership,
    ) -> std::result::Result<(), RejectReason> {
        if self.config_id != config_id {
            return Err(RejectReason::WrongConfig);
        }
        self.validate(membership)
    }

    fn validate_context(
        &self,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
    ) -> std::result::Result<(), RejectReason> {
        if self.slot != slot || self.epoch != epoch {
            return Err(RejectReason::MalformedDecision);
        }
        if self.config_id != config_id || self.config_digest != config_digest {
            return Err(RejectReason::WrongConfig);
        }
        Ok(())
    }
}

pub type DecisionRecord = DecisionCertificate;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecorderSummary {
    pub recorder_id: NodeId,
    pub slot: Slot,
    pub step: Step,
    pub first_current: Option<Proposal>,
    pub aggregate_prior: Option<Proposal>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum DecisionProof {
    FastPath {
        cluster_id: ClusterId,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        proposal: Proposal,
        summaries: Vec<RecorderSummary>,
    },
    Phase2 {
        cluster_id: ClusterId,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        step: Step,
        proposal: Proposal,
        summaries: Vec<RecorderSummary>,
    },
}

impl DecisionProof {
    pub fn proposal(&self) -> &Proposal {
        match self {
            Self::FastPath { proposal, .. } | Self::Phase2 { proposal, .. } => proposal,
        }
    }

    pub fn validate_for(
        &self,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        membership: &Membership,
    ) -> std::result::Result<(), RejectReason> {
        let (proof_slot, proof_epoch, proof_config, digest, step, proposal, summaries, fast) =
            match self {
                Self::FastPath {
                    slot,
                    epoch,
                    config_id,
                    config_digest,
                    proposal,
                    summaries,
                    ..
                } => (
                    *slot,
                    *epoch,
                    *config_id,
                    *config_digest,
                    4,
                    proposal,
                    summaries,
                    true,
                ),
                Self::Phase2 {
                    slot,
                    epoch,
                    config_id,
                    config_digest,
                    step,
                    proposal,
                    summaries,
                    ..
                } => (
                    *slot,
                    *epoch,
                    *config_id,
                    *config_digest,
                    *step,
                    proposal,
                    summaries,
                    false,
                ),
            };
        if proof_slot != slot || proof_epoch != epoch {
            return Err(RejectReason::MalformedDecision);
        }
        if proof_config != config_id || digest != membership.digest() {
            return Err(RejectReason::WrongConfig);
        }
        if proposal.is_nil() || proposal.value.is_none() {
            return Err(RejectReason::InvalidCertificate);
        }
        if summaries.len() != membership.quorum_size()
            || !summaries
                .windows(2)
                .all(|pair| pair[0].recorder_id < pair[1].recorder_id)
            || summaries.iter().any(|summary| {
                !membership.contains(&summary.recorder_id)
                    || summary.slot != slot
                    || summary.step != step
            })
        {
            return Err(RejectReason::InvalidCertificate);
        }
        if fast {
            if step != 4
                || proposal.priority != ProposalPriority::MAX
                || summaries.iter().any(|summary| {
                    !summary
                        .first_current
                        .as_ref()
                        .is_some_and(|candidate| proposal_exact(candidate, proposal))
                })
            {
                return Err(RejectReason::InvalidCertificate);
            }
        } else {
            if step % 4 != 2 {
                return Err(RejectReason::InvalidCertificate);
            }
            let maximum = summaries
                .iter()
                .filter_map(|summary| summary.aggregate_prior.as_ref())
                .max();
            if maximum != Some(proposal)
                || !maximum.is_some_and(|candidate| proposal_exact(candidate, proposal))
            {
                return Err(RejectReason::InvalidCertificate);
            }
        }
        Ok(())
    }

    pub fn validate_for_cluster(
        &self,
        cluster_id: &str,
        slot: Slot,
        epoch: Epoch,
        config_id: ConfigId,
        membership: &Membership,
    ) -> std::result::Result<(), RejectReason> {
        if proof_cluster_id(self) != cluster_id {
            return Err(RejectReason::WrongCluster);
        }
        self.validate_for(slot, epoch, config_id, membership)
    }
}

fn proposal_exact(left: &Proposal, right: &Proposal) -> bool {
    left == right && left.value == right.value
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecordRequest {
    pub cluster_id: ClusterId,
    pub epoch: Epoch,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub slot: Slot,
    pub step: Step,
    pub proposal: Proposal,
    #[serde(deserialize_with = "deserialize_required_command")]
    pub command: Option<StoredCommand>,
}

fn deserialize_required_command<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<StoredCommand>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    serde::Deserialize::deserialize(deserializer)
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecordSummary {
    pub recorder_id: NodeId,
    pub slot: Slot,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub step: Step,
    pub first_current: Option<Proposal>,
    pub aggregate_prior: Option<Proposal>,
    pub decided: Option<DecisionProof>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ReadFenceRequest {
    pub cluster_id: ClusterId,
    pub epoch: Epoch,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub slot: Slot,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum ReadFenceSlotState {
    Empty,
    /// The exact slot is present, or a durable later slot proves that this
    /// recorder has already crossed the requested position. A crossed gap has
    /// no exact summary and must therefore fail closed as pending.
    Occupied {
        summary: Option<Box<RecordSummary>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ReadFenceObservation {
    pub recorder_id: NodeId,
    pub cluster_id: ClusterId,
    pub epoch: Epoch,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub slot: Slot,
    pub max_head: Option<Slot>,
    pub slot_state: ReadFenceSlotState,
}

fn valid_read_fence_observation(
    observation: &ReadFenceObservation,
    expected_recorder_id: &str,
    request: &ReadFenceRequest,
) -> bool {
    if observation.recorder_id != expected_recorder_id
        || observation.cluster_id != request.cluster_id
        || observation.epoch != request.epoch
        || observation.config_id != request.config_id
        || observation.config_digest != request.config_digest
        || observation.slot != request.slot
    {
        return false;
    }
    match &observation.slot_state {
        ReadFenceSlotState::Empty => observation
            .max_head
            .is_none_or(|max_head| max_head < request.slot),
        ReadFenceSlotState::Occupied { summary } => {
            observation
                .max_head
                .is_some_and(|max_head| max_head >= request.slot)
                && summary.as_ref().is_none_or(|summary| {
                    summary.recorder_id == observation.recorder_id
                        && summary.slot == request.slot
                        && summary.config_id == request.config_id
                        && summary.config_digest == request.config_digest
                })
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecordResponse {
    pub from: NodeId,
    pub slot: Slot,
    pub step: Step,
    pub highest_promised: Option<Ballot>,
    pub accepted: Option<AcceptedSummary>,
    pub recorder_epoch: Epoch,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RecorderReply {
    pub recorder_id: NodeId,
    pub slot: Slot,
    pub config_id: ConfigId,
    pub config_digest: LogHash,
    pub step: Step,
    pub highest_promised: Option<Ballot>,
    pub accepted: Option<AcceptedSummary>,
    pub decided: Option<DecisionCertificate>,
    pub command: Option<StoredCommand>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RecorderRequest {
    Identity,
    StoreCommand {
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    },
    FetchCommand {
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
    },
    Inspect {
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        slot: Slot,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RejectReason {
    StaleEpoch,
    FutureEpoch,
    WrongCluster,
    WrongConfig,
    WrongSlot,
    AlreadyDecided,
    MalformedDecision,
    BallotPromised { promised: Ballot },
    ConflictingValue,
    InvalidValue,
    InvalidCertificate,
    ConfigurationSealed { stop_slot: Slot },
    ConfigurationNotInstalled,
    ActivationRequired,
    TransitionInProgress,
    InvalidTransition,
    LocalVoterRequired,
    StepRegression,
    InvalidRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigurationSeal {
    pub stop_slot: Slot,
    pub command_hash: LogHash,
    pub prefix_hash: LogHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigurationState {
    config_id: ConfigId,
    config_digest: LogHash,
    membership: Option<Membership>,
    predecessor: Option<ConfigurationSeal>,
    seal: Option<ConfigurationSeal>,
    max_accepted_or_decided_slot: Option<Slot>,
    activated: bool,
}

impl ConfigurationState {
    pub const fn config_id(&self) -> ConfigId {
        self.config_id
    }

    pub const fn config_digest(&self) -> LogHash {
        self.config_digest
    }

    pub const fn membership(&self) -> Option<&Membership> {
        self.membership.as_ref()
    }

    pub const fn predecessor(&self) -> Option<&ConfigurationSeal> {
        self.predecessor.as_ref()
    }

    pub const fn seal(&self) -> Option<&ConfigurationSeal> {
        self.seal.as_ref()
    }

    pub const fn is_activated(&self) -> bool {
        self.activated
    }

    pub const fn max_accepted_or_decided_slot(&self) -> Option<Slot> {
        self.max_accepted_or_decided_slot
    }

    fn initial(
        config_id: ConfigId,
        config_digest: LogHash,
        membership: Option<Membership>,
    ) -> Self {
        Self {
            config_id,
            config_digest,
            membership,
            predecessor: None,
            seal: None,
            max_accepted_or_decided_slot: None,
            activated: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SealFaultPoint {
    AfterIntent,
    AfterSlot,
    AfterConfiguration,
    BeforeRecordManifest,
    AfterRecordManifest,
    AfterRecordCache,
    AfterHeadIntent,
    AfterHeadConfiguration,
    AfterHead,
    AfterWalWrite,
    AfterWalSync,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecorderSlotState {
    slot: Slot,
    cluster_id: ClusterId,
    epoch: Epoch,
    config_id: ConfigId,
    config_digest: LogHash,
    highest_promised: Option<Ballot>,
    accepted: Option<AcceptedSummary>,
    decided: Option<DecisionCertificate>,
    isr: IsrState,
    decided_proof: Option<DecisionProof>,
}

impl RecorderSlotState {
    pub fn new(
        slot: Slot,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
    ) -> Self {
        Self::new_with_digest(slot, cluster_id, epoch, config_id, LogHash::ZERO)
    }

    pub fn new_with_digest(
        slot: Slot,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
    ) -> Self {
        Self {
            slot,
            cluster_id: cluster_id.into(),
            epoch,
            config_id,
            config_digest,
            highest_promised: None,
            accepted: None,
            decided: None,
            isr: IsrState::default(),
            decided_proof: None,
        }
    }

    pub fn apply(
        &mut self,
        request: RecorderRequest,
    ) -> std::result::Result<RecorderReply, RejectReason> {
        match request {
            RecorderRequest::Inspect {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                slot,
            } => self.inspect(cluster_id, epoch, config_id, config_digest, slot),
            RecorderRequest::Identity
            | RecorderRequest::StoreCommand { .. }
            | RecorderRequest::FetchCommand { .. } => Err(RejectReason::InvalidRequest),
        }
    }

    pub fn decided(&self) -> Option<&DecisionCertificate> {
        self.decided.as_ref()
    }

    pub fn decision_proof(&self) -> Option<&DecisionProof> {
        self.decided_proof.as_ref()
    }

    pub fn isr(&self) -> &IsrState {
        &self.isr
    }

    pub fn record(
        &self,
        request: &RecordRequest,
    ) -> std::result::Result<(Self, IsrReply), RejectReason> {
        self.validate(
            request.cluster_id.clone(),
            request.epoch,
            request.config_id,
            request.config_digest,
            request.slot,
        )?;
        let mut next = self.clone();
        let (isr, reply) = self.isr.record(request.step, request.proposal.clone());
        next.isr = isr;
        Ok((next, reply))
    }

    fn install_proof(&mut self, proof: DecisionProof) -> std::result::Result<(), RejectReason> {
        if let Some(existing) = &self.decided_proof {
            if existing.proposal().value != proof.proposal().value {
                return Err(RejectReason::AlreadyDecided);
            }
            return Ok(());
        }
        self.decided_proof = Some(proof);
        Ok(())
    }

    pub const fn slot(&self) -> Slot {
        self.slot
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub const fn config_id(&self) -> ConfigId {
        self.config_id
    }

    pub const fn config_digest(&self) -> LogHash {
        self.config_digest
    }

    pub fn highest_promised(&self) -> Option<&Ballot> {
        self.highest_promised.as_ref()
    }

    pub fn accepted(&self) -> Option<&AcceptedSummary> {
        self.accepted.as_ref()
    }

    pub fn max_step_seen(&self) -> Step {
        self.highest_promised
            .as_ref()
            .map_or(0, |ballot| ballot.round)
    }

    fn inspect(
        &self,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        slot: Slot,
    ) -> std::result::Result<RecorderReply, RejectReason> {
        self.validate(cluster_id, epoch, config_id, config_digest, slot)?;
        Ok(self.reply())
    }

    fn validate(
        &self,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        slot: Slot,
    ) -> std::result::Result<(), RejectReason> {
        if cluster_id != self.cluster_id {
            return Err(RejectReason::WrongCluster);
        }
        if slot != self.slot {
            return Err(RejectReason::WrongSlot);
        }
        if epoch < self.epoch {
            return Err(RejectReason::StaleEpoch);
        }
        if epoch > self.epoch {
            return Err(RejectReason::FutureEpoch);
        }
        if config_id != self.config_id {
            return Err(RejectReason::WrongConfig);
        }
        if config_digest != self.config_digest {
            return Err(RejectReason::WrongConfig);
        }
        Ok(())
    }

    fn reply(&self) -> RecorderReply {
        RecorderReply {
            recorder_id: String::new(),
            slot: self.slot,
            config_id: self.config_id,
            config_digest: self.config_digest,
            step: self.max_step_seen(),
            highest_promised: self.highest_promised.clone(),
            accepted: self.accepted.clone(),
            decided: self.decided.clone(),
            command: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RecorderFileStore {
    root: PathBuf,
    recorder_id: NodeId,
    cluster_id: ClusterId,
    epoch: Epoch,
    config_id: ConfigId,
    config_digest: LogHash,
    configuration: Arc<Mutex<ConfigurationState>>,
    recorded_head: Arc<Mutex<RecordedHeadProvenance>>,
    recent_slots: Arc<Mutex<Vec<DurableSlotSnapshot>>>,
    wal: Arc<Mutex<RecorderWal>>,
    seal_fault: Arc<Mutex<Option<SealFaultPoint>>>,
    _root_lock: Arc<fs::File>,
    sync: Arc<Mutex<()>>,
}

const RECORDED_HEAD_MAGIC: &[u8; 4] = b"QRHD";
const RECORDED_HEAD_VERSION: u16 = 3;
const RECORDER_WAL_MAGIC: &[u8; 4] = b"QWAL";
const RECORDER_WAL_VERSION: u16 = 1;
// Keep rotation bounded while allowing 512 KiB replicated commands to amortize
// quorum fsyncs without forcing a synchronous checkpoint every few dozen slots.
const RECORDER_WAL_SOFT_BYTE_LIMIT: u64 = 64 * 1024 * 1024;
#[cfg(not(test))]
const RECORDER_WAL_HARD_FRAME_LIMIT: u64 = 1_024;
#[cfg(test)]
const RECORDER_WAL_HARD_FRAME_LIMIT: u64 = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WalCheckpoint {
    generation: u64,
    through_sequence: u64,
}

impl Default for WalCheckpoint {
    fn default() -> Self {
        Self {
            generation: 1,
            through_sequence: 0,
        }
    }
}

#[derive(Debug)]
struct RecorderWal {
    checkpoint: WalCheckpoint,
    next_sequence: u64,
    last_digest: LogHash,
    frame_count: u64,
    byte_count: u64,
    slots: BTreeMap<Slot, Vec<u8>>,
    commands: HashMap<LogHash, StoredCommand>,
    file: Option<fs::File>,
    failed: bool,
}

impl Default for RecorderWal {
    fn default() -> Self {
        Self {
            checkpoint: WalCheckpoint::default(),
            next_sequence: 1,
            last_digest: LogHash::ZERO,
            frame_count: 0,
            byte_count: 0,
            slots: BTreeMap::new(),
            commands: HashMap::new(),
            file: None,
            failed: false,
        }
    }
}

#[derive(Debug)]
struct WalFrame {
    generation: u64,
    sequence: u64,
    prev_digest: LogHash,
    digest: LogHash,
    slot: Slot,
    slot_bytes: Vec<u8>,
    configuration_bytes: Vec<u8>,
    head: RecordedHeadProvenance,
    command: Option<(LogHash, StoredCommand)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DurableSlotSnapshot {
    slot: Slot,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RecordedHeadProvenance {
    Empty,
    SlotBacked {
        slot: Slot,
    },
    CheckpointBacked {
        stop_slot: Slot,
        prefix_hash: LogHash,
        recovered_tip: Slot,
        recovered_hash: LogHash,
    },
}

pub trait RecorderRpc: Send + Sync {
    /// Performs one genuine QuePaxa Record operation.
    ///
    /// Network implementations must enforce a finite deadline and return an
    /// error when it expires. The same bounded-call requirement applies to all
    /// process-bound methods on this trait. Lifecycle owners retain and join
    /// admitted calls after their shutdown deadline, so a non-returning custom
    /// implementation intentionally prevents a false quiescent shutdown.
    fn record(&self, _request: RecordRequest) -> Result<RecordSummary> {
        Err(Error::TypedRecordRequired)
    }

    /// Installs a verified decision proof durably.
    ///
    /// An `Ok(())` must mean the proof survives recorder recovery; ordinary
    /// Phase2 acknowledgements rely on a quorum of these durable successes.
    fn install_decision_proof(
        &self,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> Result<()> {
        Err(Error::TypedProofInstallRequired)
    }

    fn inspect_decision_proof(&self, _slot: Slot) -> Result<Option<DecisionProof>> {
        Ok(None)
    }

    fn inspect_record_summary(&self, _slot: Slot) -> Result<Option<RecordSummary>> {
        Err(Error::TypedRecordRequired)
    }

    /// Whether this recorder can atomically bind an exact slot observation to
    /// the durable maximum accepted-or-decided head and the requested config.
    fn supports_context_read_fence(&self) -> bool {
        false
    }

    fn observe_read_fence(&self, _request: ReadFenceRequest) -> Result<ReadFenceObservation> {
        Err(Error::ReadFenceUnsupported)
    }

    fn recorder_id(&self) -> Result<NodeId> {
        Err(Error::TypedRecordRequired)
    }

    fn store_command(&self, command_hash: LogHash, command: StoredCommand) -> Result<()> {
        self.store_command_for(String::new(), 0, 0, LogHash::ZERO, command_hash, command)
    }

    fn store_command_for(
        &self,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> Result<()> {
        let _ = (
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
            command,
        );
        Err(Error::TypedRecordRequired)
    }

    fn fetch_command(&self, command_hash: LogHash) -> Result<Option<StoredCommand>> {
        self.fetch_command_for(String::new(), 0, 0, LogHash::ZERO, command_hash)
    }

    fn fetch_command_for(
        &self,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> Result<Option<StoredCommand>> {
        let _ = (cluster_id, epoch, config_id, config_digest, command_hash);
        Err(Error::TypedRecordRequired)
    }
}

impl RecorderFileStore {
    /// Classifies an existing recorder root without creating, recovering, truncating, or
    /// rewriting local state. `Recoverable` is limited to durable normal-crash artifacts that a
    /// subsequent locked open knows how to finish.
    #[doc(hidden)]
    pub fn preflight_existing_with_membership_outcome(
        root: impl AsRef<Path>,
        cluster_id: &str,
        epoch: Epoch,
        config_id: ConfigId,
        membership: &Membership,
    ) -> Result<RecorderPreflight> {
        let root = root.as_ref();
        let metadata = match fs::symlink_metadata(root) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(RecorderPreflight::Missing)
            }
            Err(error) => return Err(Error::Io(error.to_string())),
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(Error::Decode(
                "recorder root must be a real directory".into(),
            ));
        }
        let entries = fs::read_dir(root)
            .map_err(|error| Error::Io(error.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| Error::Io(error.to_string()))?;
        if entries.is_empty()
            || entries
                .iter()
                .all(|entry| entry.file_name() == ".recorder.lock")
        {
            return Ok(RecorderPreflight::Missing);
        }
        let transition_intent =
            read_preflight_intent(root, "configuration.intent", MAX_TRANSITION_INTENT_BYTES)?;
        let configuration_head_intent = read_preflight_intent(
            root,
            "configuration-head.intent",
            MAX_CONFIGURATION_HEAD_INTENT_BYTES,
        )?;
        match (transition_intent, configuration_head_intent) {
            (Some(_), Some(_)) => {
                return Err(Error::Decode(
                    "recorder has conflicting recovery intents".into(),
                ))
            }
            (Some(bytes), None) => {
                validate_recoverable_transition_intent(
                    root, &bytes, cluster_id, epoch, config_id, membership,
                )?;
                validate_empty_recovery_wal(root)?;
                return Ok(RecorderPreflight::Recoverable);
            }
            (None, Some(bytes)) => {
                validate_recoverable_configuration_head_intent(
                    root, &bytes, cluster_id, epoch, config_id, membership,
                )?;
                validate_empty_recovery_wal(root)?;
                return Ok(RecorderPreflight::Recoverable);
            }
            (None, None) => {}
        }
        validate_current_recorder_layout(root)?;
        let configuration = decode_configuration_state(&read_regular_file_bounded(
            &root.join("configuration.rec"),
            MAX_CONFIGURATION_BYTES,
            "configuration.rec",
        )?)?;
        validate_preflight_configuration(&configuration, config_id, membership)?;
        let (head, recent_slots, checkpoint) = decode_recorded_head(
            &read_regular_file_bounded(
                &root.join("recorded-head.rec"),
                MAX_RECORDED_HEAD_BYTES,
                "recorded-head.rec",
            )?,
            cluster_id,
            epoch,
            &configuration,
        )?;
        validate_existing_snapshots(root, &recent_slots, cluster_id, epoch, &configuration)?;
        let torn_wal = validate_existing_wal(
            root,
            &read_regular_file_bounded(
                &root.join("recorder.wal"),
                MAX_RECORDER_WAL_BYTES,
                "recorder.wal",
            )?,
            cluster_id,
            epoch,
            &configuration,
            &head,
            checkpoint,
        )?;
        Ok(if torn_wal {
            RecorderPreflight::Recoverable
        } else {
            RecorderPreflight::Valid
        })
    }

    pub fn new(
        root: impl Into<PathBuf>,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
    ) -> Result<Self> {
        let root = root.into();
        let recorder_id = root
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("recorder")
            .to_string();
        Self::new_with_id(root, recorder_id, cluster_id, epoch, config_id)
    }

    pub fn new_with_id(
        root: impl Into<PathBuf>,
        recorder_id: impl Into<NodeId>,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
    ) -> Result<Self> {
        let root = root.into();
        let existing_format = current_recorder_layout(&root)?;
        let (store, _) = Self::open_root(root, recorder_id, cluster_id, epoch, config_id)?;
        if current_recorder_layout(&store.root)? != existing_format {
            return Err(Error::Decode(
                "recorder layout changed while opening".into(),
            ));
        }
        store.open_or_initialize_recorded_head(existing_format)?;
        store.open_or_replay_wal()?;
        Ok(store)
    }

    fn open_root(
        root: impl Into<PathBuf>,
        recorder_id: impl Into<NodeId>,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
    ) -> Result<(Self, bool)> {
        let root = root.into();
        let recorder_id = recorder_id.into();
        if recorder_id.is_empty() {
            return Err(Error::EmptyRecorderIdentity);
        }
        prepare_fresh_recorder_root(&root)?;
        if current_recorder_layout(&root)? {
            return Self::open_existing_root(
                root,
                recorder_id,
                cluster_id.into(),
                epoch,
                config_id,
            );
        }
        let root_lock = open_or_create_fresh_root_lock(&root)?;
        if current_recorder_layout(&root)? {
            return Err(Error::Decode(
                "recorder layout changed while opening".into(),
            ));
        }
        match root_lock.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(Error::RecorderRootLocked(root));
            }
            Err(fs::TryLockError::Error(err)) => return Err(Error::Io(err.to_string())),
        }
        Ok((
            Self {
                root,
                recorder_id,
                cluster_id: cluster_id.into(),
                epoch,
                config_id,
                config_digest: LogHash::ZERO,
                configuration: Arc::new(Mutex::new(ConfigurationState::initial(
                    config_id,
                    LogHash::ZERO,
                    None,
                ))),
                recorded_head: Arc::new(Mutex::new(RecordedHeadProvenance::Empty)),
                recent_slots: Arc::new(Mutex::new(Vec::new())),
                wal: Arc::new(Mutex::new(RecorderWal::default())),
                seal_fault: Arc::new(Mutex::new(None)),
                _root_lock: Arc::new(root_lock),
                sync: Arc::new(Mutex::new(())),
            },
            false,
        ))
    }

    fn open_existing_root(
        root: PathBuf,
        recorder_id: NodeId,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
    ) -> Result<(Self, bool)> {
        let root_metadata = match fs::symlink_metadata(&root) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(Error::Decode("recorder root does not exist".into()))
            }
            Err(error) => return Err(Error::Io(error.to_string())),
        };
        if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
            return Err(Error::Decode(
                "recorder root must be an existing real directory".into(),
            ));
        }
        if recorder_id.is_empty() {
            return Err(Error::EmptyRecorderIdentity);
        }
        let root_lock_path = root.join(".recorder.lock");
        let root_lock = open_existing_root_lock(&root_lock_path)?;
        match root_lock.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(Error::RecorderRootLocked(root));
            }
            Err(fs::TryLockError::Error(error)) => return Err(Error::Io(error.to_string())),
        }
        Ok((
            Self {
                root,
                recorder_id,
                cluster_id,
                epoch,
                config_id,
                config_digest: LogHash::ZERO,
                configuration: Arc::new(Mutex::new(ConfigurationState::initial(
                    config_id,
                    LogHash::ZERO,
                    None,
                ))),
                recorded_head: Arc::new(Mutex::new(RecordedHeadProvenance::Empty)),
                recent_slots: Arc::new(Mutex::new(Vec::new())),
                wal: Arc::new(Mutex::new(RecorderWal::default())),
                seal_fault: Arc::new(Mutex::new(None)),
                _root_lock: Arc::new(root_lock),
                sync: Arc::new(Mutex::new(())),
            },
            true,
        ))
    }

    pub fn new_with_membership(
        root: impl Into<PathBuf>,
        recorder_id: impl Into<NodeId>,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
        membership: Membership,
    ) -> Result<Self> {
        Self::open_with_membership(
            root.into(),
            recorder_id.into(),
            cluster_id.into(),
            epoch,
            config_id,
            membership,
            false,
        )
    }

    /// Opens a recorder that must already exist. Unlike `new_with_membership`, this path never
    /// creates a directory, lock file, or fresh recorder state.
    #[doc(hidden)]
    pub fn open_existing_with_membership(
        root: impl Into<PathBuf>,
        recorder_id: impl Into<NodeId>,
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
        membership: Membership,
    ) -> Result<Self> {
        Self::open_with_membership(
            root.into(),
            recorder_id.into(),
            cluster_id.into(),
            epoch,
            config_id,
            membership,
            true,
        )
    }

    fn open_with_membership(
        root: PathBuf,
        recorder_id: NodeId,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        membership: Membership,
        existing_only: bool,
    ) -> Result<Self> {
        let preflight = Self::preflight_existing_with_membership_outcome(
            &root,
            &cluster_id,
            epoch,
            config_id,
            &membership,
        )?;
        if existing_only && preflight == RecorderPreflight::Missing {
            return Err(Error::Decode(
                "recorder root is missing durable state".into(),
            ));
        }
        let existing_format = preflight != RecorderPreflight::Missing;
        let (mut store, _) = if existing_format {
            Self::open_existing_root(root, recorder_id, cluster_id, epoch, config_id)?
        } else {
            Self::open_root(root, recorder_id, cluster_id, epoch, config_id)?
        };
        let revalidated = Self::preflight_existing_with_membership_outcome(
            &store.root,
            &store.cluster_id,
            store.epoch,
            config_id,
            &membership,
        )?;
        if revalidated != preflight {
            return Err(Error::Decode(
                "recorder layout changed while opening".into(),
            ));
        }
        store.recover_configuration_head_intent()?;
        let configured = match read_regular_file_bounded(
            &store.configuration_path(),
            MAX_CONFIGURATION_BYTES,
            "configuration.rec",
        ) {
            Ok(bytes) => decode_configuration_state(&bytes)?,
            Err(Error::Decode(message)) if message == "recorder is missing configuration.rec" => {
                let configured =
                    ConfigurationState::initial(config_id, membership.digest(), Some(membership));
                store.commit_configuration_head_unlocked(
                    &configured,
                    &RecordedHeadProvenance::Empty,
                )?;
                configured
            }
            Err(error) => return Err(error),
        };
        if configured
            .membership
            .as_ref()
            .is_some_and(|current| current.digest() != configured.config_digest)
        {
            return Err(Error::Decode("installed membership digest mismatch".into()));
        }
        store.config_id = configured.config_id;
        store.config_digest = configured.config_digest;
        store.configuration = Arc::new(Mutex::new(configured));
        store.recover_intent()?;
        store.open_or_initialize_recorded_head(existing_format)?;
        store.open_or_replay_wal()?;
        Ok(store)
    }

    pub fn configuration_state(&self) -> Result<ConfigurationState> {
        self.configuration
            .lock()
            .map(|state| state.clone())
            .map_err(|_| Error::Io("configuration lock poisoned".into()))
    }

    #[doc(hidden)]
    pub fn set_seal_fault(&self, fault: Option<SealFaultPoint>) -> Result<()> {
        *self
            .seal_fault
            .lock()
            .map_err(|_| Error::Io("seal fault lock poisoned".into()))? = fault;
        Ok(())
    }

    pub fn install_successor(
        &self,
        _next_config_id: ConfigId,
        _membership: Membership,
        _stop_certificate: &DecisionCertificate,
        _stop_slot: Slot,
        _prefix_hash: LogHash,
    ) -> Result<ConfigurationState> {
        Err(Error::Rejected(RejectReason::InvalidTransition))
    }

    pub fn install_successor_from_proof(
        &self,
        membership: Membership,
        stop_proof: &DecisionProof,
    ) -> Result<ConfigurationState> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let current = self.configuration_state()?;
        let Some(old_membership) = current.membership.as_ref() else {
            return Err(Error::Rejected(RejectReason::ConfigurationNotInstalled));
        };
        let next_config_id = current
            .config_id
            .checked_add(1)
            .ok_or(Error::Rejected(RejectReason::InvalidTransition))?;
        if current.predecessor.is_some() && !current.activated {
            return Err(Error::Rejected(RejectReason::TransitionInProgress));
        }
        if !membership.contains(&self.recorder_id) {
            return Err(Error::Rejected(RejectReason::LocalVoterRequired));
        }
        if proof_cluster_id(stop_proof) != self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        let (stop_slot, epoch, config_id, config_digest) = proof_context(stop_proof);
        if epoch != self.epoch
            || config_id != current.config_id
            || config_digest != current.config_digest
        {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        stop_proof
            .validate_for_cluster(
                &self.cluster_id,
                stop_slot,
                self.epoch,
                current.config_id,
                old_membership,
            )
            .map_err(Error::Rejected)?;
        let stop_command = ConfigChange::bound_stop(
            self.cluster_id.clone(),
            current.config_id,
            current.config_digest,
            next_config_id,
            membership.members().to_vec(),
        )
        .map_err(|_| Error::Rejected(RejectReason::InvalidTransition))?
        .to_stored_command();
        let stop_value = stop_proof
            .proposal()
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
        let expected_stop = AcceptedValue::from_command(
            &self.cluster_id,
            stop_slot,
            self.epoch,
            current.config_id,
            stop_value.prev_hash,
            &stop_command,
        );
        if *stop_value != expected_stop {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        let prefix_hash = expected_stop.entry_hash;
        let expected_seal = ConfigurationSeal {
            stop_slot,
            command_hash: expected_stop.command_hash,
            prefix_hash,
        };
        if current
            .seal
            .as_ref()
            .is_some_and(|seal| seal != &expected_seal)
        {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        self.checkpoint_wal_unlocked()?;
        self.store_command_unlocked(expected_stop.command_hash, &stop_command)?;
        let installed = ConfigurationState {
            config_id: next_config_id,
            config_digest: membership.digest(),
            membership: Some(membership),
            predecessor: Some(expected_seal),
            seal: None,
            max_accepted_or_decided_slot: None,
            activated: false,
        };
        let head = RecordedHeadProvenance::Empty;
        self.commit_configuration_head_unlocked(&installed, &head)?;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = installed.clone();
        *self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))? = head;
        self.recent_slots
            .lock()
            .map_err(|_| Error::Io("recorder recent-slot lock poisoned".into()))?
            .clear();
        Ok(installed)
    }

    pub fn recover_successor_activation_from_checkpoint(
        &self,
        stop_slot: Slot,
        prefix_hash: LogHash,
        recovered_tip: Slot,
        recovered_hash: LogHash,
    ) -> Result<ConfigurationState> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let current = self.configuration_state()?;
        let predecessor = current
            .predecessor
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidTransition))?;
        if predecessor.stop_slot != stop_slot
            || predecessor.prefix_hash != prefix_hash
            || recovered_tip <= stop_slot
        {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        if current.activated {
            if current
                .max_accepted_or_decided_slot
                .is_some_and(|slot| slot > recovered_tip)
            {
                return Err(Error::Rejected(RejectReason::InvalidTransition));
            }
            return Ok(current);
        }
        let mut recovered = current;
        recovered.activated = true;
        recovered.max_accepted_or_decided_slot = Some(recovered_tip);
        let head = RecordedHeadProvenance::CheckpointBacked {
            stop_slot,
            prefix_hash,
            recovered_tip,
            recovered_hash,
        };
        self.checkpoint_wal_unlocked()?;
        self.commit_configuration_head_unlocked(&recovered, &head)?;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = recovered.clone();
        *self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))? = head;
        self.recent_slots
            .lock()
            .map_err(|_| Error::Io("recorder recent-slot lock poisoned".into()))?
            .clear();
        Ok(recovered)
    }

    pub fn apply(&self, request: RecorderRequest) -> Result<RecorderReply> {
        if !matches!(request, RecorderRequest::Identity) {
            self.validate_request_context(&request)?;
        }
        match request {
            RecorderRequest::Identity => Ok(self.reply(0, None)),
            RecorderRequest::StoreCommand {
                config_id,
                config_digest,
                command_hash,
                command,
                ..
            } => {
                let _guard = self
                    .sync
                    .lock()
                    .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
                self.recover_intent()?;
                let context = RecorderRequest::StoreCommand {
                    cluster_id: self.cluster_id.clone(),
                    epoch: self.epoch,
                    config_id,
                    config_digest,
                    command_hash,
                    command: command.clone(),
                };
                self.validate_request_context(&context)?;
                self.store_command_unlocked(command_hash, &command)?;
                let mut reply = self.reply(0, None);
                reply.config_id = config_id;
                reply.config_digest = config_digest;
                Ok(reply)
            }
            RecorderRequest::FetchCommand {
                config_id,
                config_digest,
                command_hash,
                ..
            } => {
                let _guard = self
                    .sync
                    .lock()
                    .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
                self.recover_intent()?;
                let command = self.fetch_command_unlocked(command_hash)?;
                let mut reply = self.reply(0, command);
                reply.config_id = config_id;
                reply.config_digest = config_digest;
                Ok(reply)
            }
            request => {
                let slot =
                    request_slot(&request).ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
                let request_digest = request_context(&request)
                    .ok_or(Error::Rejected(RejectReason::InvalidRequest))?
                    .3;
                let should_save = !matches!(request, RecorderRequest::Inspect { .. });
                let _guard = self
                    .sync
                    .lock()
                    .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
                self.recover_intent()?;
                self.validate_request_context(&request)?;
                let configuration = self.configuration_state()?;
                self.validate_slot_gate(&configuration, slot, None)?;
                let mut state = self.load_unlocked(slot, request_digest)?;
                let mut reply = state.apply(request).map_err(Error::Rejected)?;
                let next_configuration =
                    self.transition_after_apply(&configuration, &state, None, None)?;
                if should_save || next_configuration != configuration {
                    self.persist_state_transition_unlocked(
                        &state,
                        &configuration,
                        &next_configuration,
                    )?;
                }
                reply.recorder_id = self.recorder_id.clone();
                Ok(reply)
            }
        }
    }

    pub fn record_proposal(&self, request: RecordRequest) -> Result<RecordSummary> {
        self.validate_record_context(&request)?;
        let value = request
            .proposal
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let configuration = self.configuration_state()?;
        let command = if let Some(command) = request.command.as_ref() {
            self.validate_resolved_command_for_value(
                request.slot,
                configuration.config_id,
                value,
                command,
            )?;
            std::borrow::Cow::Borrowed(command)
        } else {
            std::borrow::Cow::Owned(self.command_for_value_unlocked(value)?)
        };
        let change = Self::change_for_command(&command)?;
        if !configuration.activated && change.is_none() {
            return Err(Error::Rejected(RejectReason::ActivationRequired));
        }
        let state = self.load_unlocked(request.slot, request.config_digest)?;
        if let Some(proof) = state.decision_proof() {
            if proof.proposal().value.as_ref() == Some(value) {
                return Ok(record_summary(
                    &self.recorder_id,
                    &state,
                    Some(proof.clone()),
                ));
            }
        }
        self.validate_slot_gate(&configuration, request.slot, change.as_ref())?;
        if request.command.is_none() {
            self.validate_resolved_command_for_value(
                request.slot,
                configuration.config_id,
                value,
                &command,
            )?;
        }
        if let Some(proof) = state.decision_proof() {
            return Ok(record_summary(
                &self.recorder_id,
                &state,
                Some(proof.clone()),
            ));
        }
        let (mut next, _) = state.record(&request).map_err(Error::Rejected)?;

        // Legacy fields are intentionally not synthesized from ISR state.
        next.highest_promised = next.isr.first_current().and_then(proposal_ballot);
        next.accepted = None;
        let next_configuration =
            self.transition_after_apply(&configuration, &next, change.as_ref(), Some(value))?;
        self.persist_state_transition_with_command_unlocked(
            &next,
            &configuration,
            &next_configuration,
            request
                .command
                .as_ref()
                .map(|command| (value.command_hash, command)),
        )?;
        Ok(record_summary(&self.recorder_id, &next, None))
    }

    pub fn install_decision_proof_record(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> Result<()> {
        let (slot, epoch, config_id, digest) = proof_context(&proof);
        if proof_cluster_id(&proof) != self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let configuration = self.configuration_state()?;
        if epoch != self.epoch
            || config_id != configuration.config_id
            || digest != configuration.config_digest
            || configuration.membership.as_ref() != Some(membership)
        {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        proof
            .validate_for_cluster(&self.cluster_id, slot, epoch, config_id, membership)
            .map_err(Error::Rejected)?;
        let value = proof
            .proposal()
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
        self.validate_value_unlocked(slot, value)?;
        let mut state = self.load_unlocked(slot, digest)?;
        if state.decision_proof().is_some() {
            state.install_proof(proof).map_err(Error::Rejected)?;
            return Ok(());
        }
        let change = self.change_for_value_unlocked(value)?;
        self.validate_slot_gate(&configuration, slot, change.as_ref())?;
        state
            .install_proof(proof.clone())
            .map_err(Error::Rejected)?;
        let certificate = certificate_from_proof(&proof)?;
        if let Some(existing) = &state.decided {
            if existing.value != certificate.value {
                return Err(Error::Rejected(RejectReason::AlreadyDecided));
            }
        } else {
            state.decided = Some(certificate);
        }
        let next =
            self.transition_after_apply(&configuration, &state, change.as_ref(), Some(value))?;
        self.persist_state_transition_unlocked(&state, &configuration, &next)
    }

    fn validate_record_context(&self, request: &RecordRequest) -> Result<()> {
        if request.cluster_id != self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        if request.epoch != self.epoch {
            return Err(Error::Rejected(if request.epoch < self.epoch {
                RejectReason::StaleEpoch
            } else {
                RejectReason::FutureEpoch
            }));
        }
        let configuration = self.configuration_state()?;
        if request.config_id != configuration.config_id
            || (configuration.config_digest != LogHash::ZERO
                && request.config_digest != configuration.config_digest)
        {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        Ok(())
    }

    pub fn load(&self, slot: Slot) -> Result<RecorderSlotState> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.load_unlocked(slot, self.config_digest())
    }

    pub fn save(&self, state: &RecorderSlotState) -> Result<()> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let configuration = self.configuration_state()?;
        if state.cluster_id != self.cluster_id
            || state.epoch != self.epoch
            || state.config_id != configuration.config_id
            || (configuration.config_digest != LogHash::ZERO
                && state.config_digest != configuration.config_digest)
        {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        let change = state
            .decided()
            .map(|decision| &decision.value)
            .or_else(|| state.accepted().map(|accepted| &accepted.value))
            .map(|value| self.change_for_value_unlocked(value))
            .transpose()?
            .flatten();
        self.validate_slot_gate(&configuration, state.slot(), change.as_ref())?;
        let applied_value = state
            .decided()
            .map(|decision| &decision.value)
            .or_else(|| state.accepted().map(|accepted| &accepted.value));
        let next =
            self.transition_after_apply(&configuration, state, change.as_ref(), applied_value)?;
        self.persist_state_transition_unlocked(state, &configuration, &next)
    }

    pub fn store_command(&self, command_hash: LogHash, command: StoredCommand) -> Result<()> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.store_command_unlocked(command_hash, &command)
    }

    fn store_command_unlocked(&self, command_hash: LogHash, command: &StoredCommand) -> Result<()> {
        if command.hash() != command_hash {
            return Err(Error::CommandHashMismatch);
        }
        {
            let wal = self
                .wal
                .lock()
                .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
            match wal.commands.get(&command_hash) {
                Some(existing) if existing == command => return Ok(()),
                Some(_) => return Err(Error::CommandHashMismatch),
                None => {}
            }
        }
        self.stage_command_unlocked(command_hash, command)?;
        self.sync_root()
    }

    fn stage_command_unlocked(&self, command_hash: LogHash, command: &StoredCommand) -> Result<()> {
        let path = self.command_path(command_hash);
        if path.exists() {
            return match self.fetch_command_cache_unlocked(command_hash)? {
                Some(existing) if existing == *command => Ok(()),
                _ => Err(Error::CommandHashMismatch),
            };
        }
        atomic_replace(&path, &encode_stored_command(command))?;
        Ok(())
    }

    pub fn fetch_command(&self, command_hash: LogHash) -> Result<Option<StoredCommand>> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.fetch_command_unlocked(command_hash)
    }

    fn load_unlocked(&self, slot: Slot, config_digest: LogHash) -> Result<RecorderSlotState> {
        let wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
        if let Some(bytes) = wal.slots.get(&slot) {
            let state = decode_recorder_state(bytes)?;
            drop(wal);
            if state.cluster_id != self.cluster_id
                || state.epoch != self.epoch
                || state.config_id != self.current_config_id()
                || (config_digest != LogHash::ZERO && state.config_digest != config_digest)
            {
                return Err(Error::Decode("recorder WAL state identity mismatch".into()));
            }
            return Ok(state);
        }
        drop(wal);
        let path = self.path(slot);
        let bytes = match read_regular_file_bounded(&path, MAX_RECORDER_STATE_BYTES, "slot cache") {
            Ok(bytes) => bytes,
            Err(Error::Decode(message)) if message == "recorder is missing slot cache" => {
                return Ok(RecorderSlotState::new_with_digest(
                    slot,
                    self.cluster_id.clone(),
                    self.epoch,
                    self.current_config_id(),
                    config_digest,
                ))
            }
            Err(error) => return Err(error),
        };
        let state = decode_recorder_state(&bytes)?;
        if state.slot != slot
            || state.cluster_id != self.cluster_id
            || state.epoch != self.epoch
            || state.config_id != self.current_config_id()
            || (config_digest != LogHash::ZERO && state.config_digest != config_digest)
        {
            return Err(Error::Decode("recorder state identity mismatch".into()));
        }
        Ok(state)
    }

    fn open_or_initialize_recorded_head(&self, existing_format: bool) -> Result<()> {
        let configuration = self.configuration_state()?;
        let (head, recent_slots, wal_checkpoint) = match read_regular_file_bounded(
            &self.recorded_head_path(),
            MAX_RECORDED_HEAD_BYTES,
            "recorded-head.rec",
        ) {
            Ok(bytes) => {
                decode_recorded_head(&bytes, &self.cluster_id, self.epoch, &configuration)?
            }
            Err(Error::Decode(message)) if message == "recorder is missing recorded-head.rec" => {
                if existing_format {
                    return Err(noncurrent_recorder_layout_error());
                }
                let head = RecordedHeadProvenance::Empty;
                let wal_checkpoint = WalCheckpoint::default();
                atomic_write(
                    &self.recorded_head_path(),
                    &encode_recorded_head(
                        &self.cluster_id,
                        self.epoch,
                        &configuration,
                        &head,
                        &[],
                        wal_checkpoint,
                    )?,
                )?;
                (head, Vec::new(), wal_checkpoint)
            }
            Err(error) => return Err(error),
        };
        self.install_recorded_head(&configuration, head, recent_slots, wal_checkpoint)
    }

    fn open_or_replay_wal(&self) -> Result<()> {
        let path = self.wal_path();
        let created = match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(_) => true,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => false,
            Err(error) => return Err(Error::Io(error.to_string())),
        };
        if created {
            self.sync_root()?;
        }
        let bytes = read_regular_file_bounded(&path, MAX_RECORDER_WAL_BYTES, "recorder.wal")?;
        let checkpoint = self.wal_checkpoint()?;
        let mut replayed = RecorderWal {
            checkpoint,
            next_sequence: checkpoint
                .through_sequence
                .checked_add(1)
                .ok_or_else(|| Error::Decode("recorder WAL sequence exhausted".into()))?,
            ..RecorderWal::default()
        };
        let mut configuration = self.configuration_state()?;
        let mut head = self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))?
            .clone();
        let mut offset = 0usize;
        while offset < bytes.len() {
            let Some((frame, end)) = decode_wal_frame(&bytes, offset)? else {
                break;
            };
            if frame.generation < checkpoint.generation {
                offset = end;
                continue;
            }
            if frame.generation != checkpoint.generation
                || frame.sequence != replayed.next_sequence
                || frame.prev_digest != replayed.last_digest
            {
                return Err(Error::Decode(
                    "recorder WAL sequence or digest chain mismatch".into(),
                ));
            }
            let state = decode_recorder_state(&frame.slot_bytes)?;
            let next_configuration = decode_configuration_state(&frame.configuration_bytes)?;
            if state.slot() != frame.slot
                || state.cluster_id != self.cluster_id
                || state.epoch != self.epoch
                || state.config_id != next_configuration.config_id
                || state.config_digest != next_configuration.config_digest
                || configuration_structure_changed(&configuration, &next_configuration)
            {
                return Err(Error::Decode("recorder WAL state identity mismatch".into()));
            }
            if let Some((hash, command)) = &frame.command {
                if command.hash() != *hash {
                    return Err(Error::Decode(
                        "recorder WAL inline command hash mismatch".into(),
                    ));
                }
                upsert_wal_command(&mut replayed.commands, *hash, command)?;
            }
            for value in recorder_state_values(&state) {
                let cached_command;
                let command = match replayed.commands.get(&value.command_hash) {
                    Some(command) => command,
                    None => {
                        cached_command = self
                            .fetch_command_cache_unlocked(value.command_hash)
                            .ok()
                            .flatten()
                            .ok_or(Error::CommandUnavailable)?;
                        &cached_command
                    }
                };
                if AcceptedValue::from_command(
                    &self.cluster_id,
                    frame.slot,
                    self.epoch,
                    next_configuration.config_id,
                    value.prev_hash,
                    command,
                ) != *value
                {
                    return Err(Error::Decode("recorder WAL value mismatch".into()));
                }
            }
            let expected_head = if next_configuration.max_accepted_or_decided_slot
                == Some(frame.slot)
                && recorder_state_values(&state).next().is_some()
            {
                RecordedHeadProvenance::SlotBacked { slot: frame.slot }
            } else {
                head.clone()
            };
            if frame.head != expected_head {
                return Err(Error::Decode("recorder WAL head mismatch".into()));
            }
            replayed.slots.insert(frame.slot, frame.slot_bytes);
            replayed.next_sequence = replayed
                .next_sequence
                .checked_add(1)
                .ok_or_else(|| Error::Decode("recorder WAL sequence exhausted".into()))?;
            replayed.last_digest = frame.digest;
            replayed.frame_count += 1;
            configuration = next_configuration;
            head = frame.head;
            offset = end;
        }
        if offset != bytes.len() {
            let file = fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(|error| Error::Io(error.to_string()))?;
            file.set_len(offset as u64)
                .and_then(|_| sync_wal_metadata(&file))
                .map_err(|error| Error::Io(error.to_string()))?;
        }
        replayed.file = Some(
            fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .map_err(|error| Error::Io(error.to_string()))?,
        );
        replayed.byte_count = offset as u64;
        *self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))? = replayed;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = configuration;
        *self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))? = head;
        Ok(())
    }

    fn install_recorded_head(
        &self,
        configuration: &ConfigurationState,
        head: RecordedHeadProvenance,
        recent_slots: Vec<DurableSlotSnapshot>,
        wal_checkpoint: WalCheckpoint,
    ) -> Result<()> {
        let mut recovered_cache = false;
        for snapshot in &recent_slots {
            let state = decode_recorder_state(&snapshot.bytes)?;
            if state.slot() != snapshot.slot
                || state.cluster_id != self.cluster_id
                || state.epoch != self.epoch
                || state.config_id != configuration.config_id
                || state.config_digest != configuration.config_digest
            {
                return Err(Error::Decode(
                    "durable recorder snapshot identity mismatch".into(),
                ));
            }
            for value in recorder_state_values(&state) {
                self.validate_value_unlocked(snapshot.slot, value)?;
            }
            let path = self.path(snapshot.slot);
            if read_regular_file_bounded(&path, MAX_RECORDER_STATE_BYTES, "slot cache")
                .ok()
                .as_deref()
                != Some(snapshot.bytes.as_slice())
            {
                atomic_replace(&path, &snapshot.bytes)?;
                recovered_cache = true;
            }
        }
        if recovered_cache {
            self.sync_root()?;
        }
        let recovered_max = match &head {
            RecordedHeadProvenance::Empty => None,
            RecordedHeadProvenance::SlotBacked { slot } => {
                let state = self.load_unlocked(*slot, configuration.config_digest)?;
                let mut values = recorder_state_values(&state).peekable();
                if values.peek().is_none() {
                    return Err(Error::Decode(
                        "slot-backed recorder head references a state without a value".into(),
                    ));
                }
                for value in values {
                    self.validate_value_unlocked(*slot, value)?;
                }
                Some(*slot)
            }
            RecordedHeadProvenance::CheckpointBacked {
                stop_slot,
                prefix_hash,
                recovered_tip,
                recovered_hash,
            } => {
                let predecessor = configuration.predecessor.as_ref().ok_or_else(|| {
                    Error::Decode("checkpoint-backed head has no predecessor binding".into())
                })?;
                if !configuration.activated
                    || predecessor.stop_slot != *stop_slot
                    || predecessor.prefix_hash != *prefix_hash
                    || recovered_tip <= stop_slot
                    || *recovered_hash == LogHash::ZERO
                    || configuration.max_accepted_or_decided_slot != Some(*recovered_tip)
                {
                    return Err(Error::Decode(
                        "checkpoint-backed recorder head evidence is invalid".into(),
                    ));
                }
                Some(*recovered_tip)
            }
        };
        self.configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))?
            .max_accepted_or_decided_slot = recovered_max;
        *self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))? = head;
        *self
            .recent_slots
            .lock()
            .map_err(|_| Error::Io("recorder recent-slot lock poisoned".into()))? = recent_slots;
        let mut wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
        wal.checkpoint = wal_checkpoint;
        wal.next_sequence = wal_checkpoint
            .through_sequence
            .checked_add(1)
            .ok_or_else(|| Error::Decode("recorder WAL sequence exhausted".into()))?;
        Ok(())
    }

    fn fetch_command_unlocked(&self, command_hash: LogHash) -> Result<Option<StoredCommand>> {
        let wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
        if let Some(command) = wal.commands.get(&command_hash).cloned() {
            return Ok(Some(command));
        }
        drop(wal);
        self.fetch_command_cache_unlocked(command_hash)
    }

    fn fetch_command_cache_unlocked(&self, command_hash: LogHash) -> Result<Option<StoredCommand>> {
        #[cfg(test)]
        COMMAND_FILE_READS.with(|reads| reads.set(reads.get() + 1));
        match read_existing_command_cache(&self.root, command_hash) {
            Err(Error::CommandUnavailable) => Ok(None),
            result => result.map(Some),
        }
    }

    fn validate_value_unlocked(&self, slot: Slot, value: &AcceptedValue) -> Result<()> {
        let config_id = self.current_config_id();
        let command = self.command_for_value_unlocked(value)?;
        self.validate_resolved_command_for_value(slot, config_id, value, &command)
    }

    fn validate_resolved_command_for_value(
        &self,
        slot: Slot,
        config_id: ConfigId,
        value: &AcceptedValue,
        command: &StoredCommand,
    ) -> Result<()> {
        let expected = AcceptedValue::from_command(
            &self.cluster_id,
            slot,
            self.epoch,
            config_id,
            value.prev_hash,
            command,
        );
        if expected != *value {
            return Err(Error::Rejected(RejectReason::InvalidValue));
        }
        Ok(())
    }

    fn change_for_value_unlocked(&self, value: &AcceptedValue) -> Result<Option<ConfigChange>> {
        let command = self.command_for_value_unlocked(value)?;
        Self::change_for_command(&command)
    }

    fn change_for_command(command: &StoredCommand) -> Result<Option<ConfigChange>> {
        if command.entry_type != EntryType::ConfigChange {
            return Ok(None);
        }
        ConfigChange::recognize(command)
            .map_err(|_| Error::Rejected(RejectReason::InvalidRequest))
            .map(Some)
    }

    fn command_for_value_unlocked(&self, value: &AcceptedValue) -> Result<StoredCommand> {
        self.fetch_command_unlocked(value.command_hash)?
            .ok_or(Error::CommandUnavailable)
    }

    fn validate_slot_gate(
        &self,
        configuration: &ConfigurationState,
        slot: Slot,
        change: Option<&ConfigChange>,
    ) -> Result<()> {
        if let Some(predecessor) = &configuration.predecessor {
            if slot <= predecessor.stop_slot {
                return Err(Error::Rejected(RejectReason::ConfigurationNotInstalled));
            }
        }
        if let Some(seal) = &configuration.seal {
            if slot > seal.stop_slot {
                return Err(Error::Rejected(RejectReason::ConfigurationSealed {
                    stop_slot: seal.stop_slot,
                }));
            }
            if matches!(
                change,
                Some(ConfigChange::Stop { .. } | ConfigChange::BoundStop { .. })
            ) && (slot != seal.stop_slot || seal.command_hash == LogHash::ZERO)
            {
                return Err(Error::Rejected(RejectReason::TransitionInProgress));
            }
        }
        if matches!(
            change,
            Some(ConfigChange::Stop { .. } | ConfigChange::BoundStop { .. })
        ) && configuration
            .max_accepted_or_decided_slot
            .is_some_and(|accepted_slot| accepted_slot > slot)
        {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        if !configuration.activated {
            let Some(predecessor) = &configuration.predecessor else {
                return Err(Error::Rejected(RejectReason::InvalidTransition));
            };
            match change {
                Some(ConfigChange::BoundActivationBarrier {
                    successor,
                    stop_slot,
                    prefix_hash,
                    stop_command_hash,
                }) if successor.cluster_id() == self.cluster_id
                    && successor.config_id() == configuration.config_id
                    && successor.digest() == configuration.config_digest
                    && successor.predecessor_config_id().checked_add(1)
                        == Some(configuration.config_id)
                    && *stop_slot == predecessor.stop_slot
                    && *prefix_hash == predecessor.prefix_hash
                    && *stop_command_hash == predecessor.command_hash
                    && slot == predecessor.stop_slot + 1 => {}
                None if slot == predecessor.stop_slot + 1 => {}
                _ => return Err(Error::Rejected(RejectReason::ActivationRequired)),
            }
        } else if matches!(
            change,
            Some(
                ConfigChange::ActivationBarrier { .. }
                    | ConfigChange::BoundActivationBarrier { .. }
            )
        ) {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        if let Some(change) = change {
            let (config_id, config_digest) = change.binding();
            if config_id != configuration.config_id || config_digest != configuration.config_digest
            {
                return Err(Error::Rejected(RejectReason::WrongConfig));
            }
        }
        Ok(())
    }

    fn transition_after_apply(
        &self,
        configuration: &ConfigurationState,
        state: &RecorderSlotState,
        change: Option<&ConfigChange>,
        applied_value: Option<&AcceptedValue>,
    ) -> Result<ConfigurationState> {
        let mut next = configuration.clone();
        if applied_value.is_some() {
            next.max_accepted_or_decided_slot = Some(
                next.max_accepted_or_decided_slot
                    .map_or(state.slot(), |current| current.max(state.slot())),
            );
        }
        if state.decision_proof().is_some()
            && next.seal.as_ref().is_some_and(|seal| {
                seal.stop_slot == state.slot()
                    && applied_value.is_some_and(|value| value.command_hash != seal.command_hash)
            })
        {
            next.seal = None;
        }
        match change {
            Some(ConfigChange::Stop { .. } | ConfigChange::BoundStop { .. })
                if applied_value.is_some() =>
            {
                let value = applied_value.expect("checked applied value");
                let proposed = ConfigurationSeal {
                    stop_slot: state.slot(),
                    command_hash: value.command_hash,
                    prefix_hash: value.entry_hash,
                };
                if let Some(existing) = &next.seal {
                    if existing != &proposed {
                        return Err(Error::Rejected(RejectReason::TransitionInProgress));
                    }
                } else {
                    next.seal = Some(proposed);
                }
            }
            Some(
                ConfigChange::ActivationBarrier { .. }
                | ConfigChange::BoundActivationBarrier { .. },
            ) if state.decision_proof().is_some() => {
                next.activated = true;
            }
            _ => {}
        }
        Ok(next)
    }

    fn validate_request_context(&self, request: &RecorderRequest) -> Result<()> {
        let (cluster_id, epoch, config_id, config_digest) =
            request_context(request).ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
        if cluster_id != &self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        if epoch < self.epoch {
            return Err(Error::Rejected(RejectReason::StaleEpoch));
        }
        if epoch > self.epoch {
            return Err(Error::Rejected(RejectReason::FutureEpoch));
        }
        let configuration = self.configuration_state()?;
        if config_id != configuration.config_id {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        if configuration.config_digest != LogHash::ZERO
            && config_digest != configuration.config_digest
        {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        Ok(())
    }

    fn reply(&self, slot: Slot, command: Option<StoredCommand>) -> RecorderReply {
        RecorderReply {
            recorder_id: self.recorder_id.clone(),
            slot,
            config_id: self.current_config_id(),
            config_digest: self.config_digest(),
            step: 0,
            highest_promised: None,
            accepted: None,
            decided: None,
            command,
        }
    }

    fn path(&self, slot: Slot) -> PathBuf {
        self.root.join(format!("slot-{slot:020}.rec"))
    }

    fn command_path(&self, command_hash: LogHash) -> PathBuf {
        self.root
            .join(format!("command-{}.cmd", command_hash.to_hex()))
    }

    fn configuration_path(&self) -> PathBuf {
        self.root.join("configuration.rec")
    }

    fn intent_path(&self) -> PathBuf {
        self.root.join("configuration.intent")
    }

    fn configuration_head_intent_path(&self) -> PathBuf {
        self.root.join("configuration-head.intent")
    }

    fn recorded_head_path(&self) -> PathBuf {
        self.root.join("recorded-head.rec")
    }

    fn wal_path(&self) -> PathBuf {
        self.root.join("recorder.wal")
    }

    fn head_after_slot_state(
        &self,
        configuration: &ConfigurationState,
        slot_state: &RecorderSlotState,
    ) -> Result<RecordedHeadProvenance> {
        let current = self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))?
            .clone();
        if configuration.max_accepted_or_decided_slot == Some(slot_state.slot())
            && recorder_state_values(slot_state).next().is_some()
        {
            Ok(RecordedHeadProvenance::SlotBacked {
                slot: slot_state.slot(),
            })
        } else {
            Ok(current)
        }
    }

    fn recover_intent(&self) -> Result<()> {
        self.recover_configuration_head_intent()?;
        let path = self.intent_path();
        let Some(intent_bytes) = read_preflight_intent(
            &self.root,
            "configuration.intent",
            MAX_TRANSITION_INTENT_BYTES,
        )?
        else {
            return Ok(());
        };
        let (slot, slot_bytes, configuration_bytes) = decode_transition_intent(&intent_bytes)?;
        let configuration = decode_configuration_state(&configuration_bytes)?;
        let slot_state = decode_recorder_state(&slot_bytes)?;
        let head = self.head_after_slot_state(&configuration, &slot_state)?;
        atomic_write(&self.path(slot), &slot_bytes)?;
        atomic_write(&self.configuration_path(), &configuration_bytes)?;
        atomic_write(
            &self.recorded_head_path(),
            &encode_recorded_head(
                &self.cluster_id,
                self.epoch,
                &configuration,
                &head,
                &[],
                self.wal_checkpoint()?,
            )?,
        )?;
        fs::remove_file(path).map_err(|err| Error::Io(err.to_string()))?;
        fs::File::open(&self.root)
            .and_then(|directory| directory.sync_all())
            .map_err(|err| Error::Io(err.to_string()))?;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = configuration;
        *self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))? = head;
        self.recent_slots
            .lock()
            .map_err(|_| Error::Io("recorder recent-slot lock poisoned".into()))?
            .clear();
        Ok(())
    }

    fn recover_configuration_head_intent(&self) -> Result<()> {
        let path = self.configuration_head_intent_path();
        let Some(intent_bytes) = read_preflight_intent(
            &self.root,
            "configuration-head.intent",
            MAX_CONFIGURATION_HEAD_INTENT_BYTES,
        )?
        else {
            return Ok(());
        };
        let (configuration_bytes, head_bytes) = decode_configuration_head_intent(&intent_bytes)?;
        atomic_write(&self.configuration_path(), configuration_bytes)?;
        atomic_write(&self.recorded_head_path(), head_bytes)?;
        fs::remove_file(path).map_err(|err| Error::Io(err.to_string()))?;
        self.sync_root()
    }

    fn commit_configuration_head_unlocked(
        &self,
        configuration: &ConfigurationState,
        head: &RecordedHeadProvenance,
    ) -> Result<()> {
        let configuration_bytes = encode_configuration_state(configuration)?;
        let head_bytes = encode_recorded_head(
            &self.cluster_id,
            self.epoch,
            configuration,
            head,
            &[],
            self.wal_checkpoint()?,
        )?;
        atomic_write(
            &self.configuration_head_intent_path(),
            &encode_configuration_head_intent(&configuration_bytes, &head_bytes),
        )?;
        self.fail_seal_at(SealFaultPoint::AfterHeadIntent)?;
        atomic_write(&self.configuration_path(), &configuration_bytes)?;
        self.fail_seal_at(SealFaultPoint::AfterHeadConfiguration)?;
        atomic_write(&self.recorded_head_path(), &head_bytes)?;
        self.fail_seal_at(SealFaultPoint::AfterHead)?;
        fs::remove_file(self.configuration_head_intent_path())
            .map_err(|err| Error::Io(err.to_string()))?;
        self.sync_root()
    }

    fn persist_state_transition_unlocked(
        &self,
        slot_state: &RecorderSlotState,
        previous: &ConfigurationState,
        next: &ConfigurationState,
    ) -> Result<()> {
        self.persist_state_transition_with_command_unlocked(slot_state, previous, next, None)
    }

    fn persist_state_transition_with_command_unlocked(
        &self,
        slot_state: &RecorderSlotState,
        previous: &ConfigurationState,
        next: &ConfigurationState,
        command: Option<(LogHash, &StoredCommand)>,
    ) -> Result<()> {
        if configuration_structure_changed(previous, next) {
            self.checkpoint_wal_unlocked()?;
            if let Some((hash, command)) = command {
                self.store_command_unlocked(hash, command)?;
            }
            return self.commit_transition_unlocked(slot_state, next);
        }
        let head = self.head_after_slot_state(next, slot_state)?;
        self.append_wal_unlocked(slot_state, next, &head, command)?;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = next.clone();
        *self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))? = head;
        self.recent_slots
            .lock()
            .map_err(|_| Error::Io("recorder recent-slot lock poisoned".into()))?
            .clear();
        Ok(())
    }

    fn append_wal_unlocked(
        &self,
        slot_state: &RecorderSlotState,
        configuration: &ConfigurationState,
        head: &RecordedHeadProvenance,
        command: Option<(LogHash, &StoredCommand)>,
    ) -> Result<()> {
        let should_checkpoint = {
            let wal = self
                .wal
                .lock()
                .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
            if wal.failed {
                return Err(Error::Io(
                    "recorder WAL is unavailable after an I/O failure".into(),
                ));
            }
            wal.frame_count >= RECORDER_WAL_HARD_FRAME_LIMIT
                || wal.byte_count >= RECORDER_WAL_SOFT_BYTE_LIMIT
        };
        if should_checkpoint {
            self.checkpoint_wal_unlocked()?;
        }
        let (generation, sequence, prev_digest) = {
            let wal = self
                .wal
                .lock()
                .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
            (
                wal.checkpoint.generation,
                wal.next_sequence,
                wal.last_digest,
            )
        };
        let (frame, digest, slot_bytes) = encode_wal_frame(
            generation,
            sequence,
            prev_digest,
            slot_state,
            configuration,
            head,
            command,
        )?;
        let mut wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
        let append_result = (|| {
            let file = wal
                .file
                .as_mut()
                .ok_or_else(|| Error::Io("recorder WAL file is not open".into()))?;
            file.write_all(&frame)
                .map_err(|error| Error::Io(error.to_string()))?;
            self.fail_seal_at(SealFaultPoint::AfterWalWrite)?;
            sync_wal_append(file).map_err(|error| Error::Io(error.to_string()))?;
            self.fail_seal_at(SealFaultPoint::AfterWalSync)
        })();
        if let Err(error) = append_result {
            wal.failed = true;
            return Err(error);
        }
        wal.slots.insert(slot_state.slot(), slot_bytes);
        if let Some((hash, command)) = command {
            wal.commands.entry(hash).or_insert_with(|| command.clone());
        }
        wal.next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| Error::Io("recorder WAL sequence exhausted".into()))?;
        wal.last_digest = digest;
        wal.frame_count += 1;
        wal.byte_count = wal
            .byte_count
            .checked_add(frame.len() as u64)
            .ok_or_else(|| Error::Io("recorder WAL byte count overflow".into()))?;
        Ok(())
    }

    fn checkpoint_wal_unlocked(&self) -> Result<()> {
        let (checkpoint, next_sequence, slots, commands) = {
            let wal = self
                .wal
                .lock()
                .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
            if wal.failed {
                return Err(Error::Io(
                    "recorder WAL is unavailable after an I/O failure".into(),
                ));
            }
            if wal.frame_count == 0 {
                return Ok(());
            }
            (
                wal.checkpoint,
                wal.next_sequence,
                wal.slots.clone(),
                wal.commands.clone(),
            )
        };
        let next_checkpoint = WalCheckpoint {
            generation: checkpoint
                .generation
                .checked_add(1)
                .ok_or_else(|| Error::Io("recorder WAL generation exhausted".into()))?,
            through_sequence: next_sequence - 1,
        };
        for (hash, command) in &commands {
            atomic_replace(&self.command_path(*hash), &encode_stored_command(command))?;
        }
        for (slot, bytes) in &slots {
            atomic_replace(&self.path(*slot), bytes)?;
        }
        let configuration = self.configuration_state()?;
        let head = self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))?
            .clone();
        atomic_replace(
            &self.configuration_path(),
            &encode_configuration_state(&configuration)?,
        )?;
        atomic_write(
            &self.recorded_head_path(),
            &encode_recorded_head(
                &self.cluster_id,
                self.epoch,
                &configuration,
                &head,
                &[],
                next_checkpoint,
            )?,
        )?;
        let truncate_result = fs::OpenOptions::new()
            .write(true)
            .open(self.wal_path())
            .and_then(|file| file.set_len(0).and_then(|_| sync_wal_metadata(&file)));
        if let Err(error) = truncate_result {
            if let Ok(mut wal) = self.wal.lock() {
                wal.failed = true;
            }
            return Err(Error::Io(error.to_string()));
        }
        let mut wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?;
        wal.checkpoint = next_checkpoint;
        wal.last_digest = LogHash::ZERO;
        wal.frame_count = 0;
        wal.byte_count = 0;
        wal.slots.clear();
        wal.commands.clear();
        self.recent_slots
            .lock()
            .map_err(|_| Error::Io("recorder recent-slot lock poisoned".into()))?
            .clear();
        Ok(())
    }

    fn sync_root(&self) -> Result<()> {
        fs::File::open(&self.root)
            .and_then(|directory| directory.sync_all())
            .map_err(|err| Error::Io(err.to_string()))?;
        #[cfg(test)]
        record_directory_sync();
        Ok(())
    }

    fn commit_transition_unlocked(
        &self,
        slot_state: &RecorderSlotState,
        configuration: &ConfigurationState,
    ) -> Result<()> {
        let slot_bytes = encode_recorder_state(slot_state)?;
        let configuration_bytes = encode_configuration_state(configuration)?;
        let head = self.head_after_slot_state(configuration, slot_state)?;
        let head_bytes = encode_recorded_head(
            &self.cluster_id,
            self.epoch,
            configuration,
            &head,
            &[],
            self.wal_checkpoint()?,
        )?;
        atomic_write(
            &self.intent_path(),
            &encode_transition_intent(slot_state.slot(), &slot_bytes, &configuration_bytes)?,
        )?;
        self.fail_seal_at(SealFaultPoint::AfterIntent)?;
        atomic_write(&self.path(slot_state.slot()), &slot_bytes)?;
        self.fail_seal_at(SealFaultPoint::AfterSlot)?;
        atomic_write(&self.configuration_path(), &configuration_bytes)?;
        self.fail_seal_at(SealFaultPoint::AfterConfiguration)?;
        atomic_write(&self.recorded_head_path(), &head_bytes)?;
        fs::remove_file(self.intent_path()).map_err(|err| Error::Io(err.to_string()))?;
        fs::File::open(&self.root)
            .and_then(|directory| directory.sync_all())
            .map_err(|err| Error::Io(err.to_string()))?;
        *self
            .configuration
            .lock()
            .map_err(|_| Error::Io("configuration lock poisoned".into()))? = configuration.clone();
        *self
            .recorded_head
            .lock()
            .map_err(|_| Error::Io("recorder head lock poisoned".into()))? = head;
        self.recent_slots
            .lock()
            .map_err(|_| Error::Io("recorder recent-slot lock poisoned".into()))?
            .clear();
        Ok(())
    }

    fn fail_seal_at(&self, point: SealFaultPoint) -> Result<()> {
        let mut fault = self
            .seal_fault
            .lock()
            .map_err(|_| Error::Io("seal fault lock poisoned".into()))?;
        if *fault == Some(point) {
            *fault = None;
            return Err(Error::Io(format!("injected seal fault at {point:?}")));
        }
        Ok(())
    }

    fn config_digest(&self) -> LogHash {
        self.configuration
            .lock()
            .map(|state| state.config_digest)
            .unwrap_or(self.config_digest)
    }

    fn current_config_id(&self) -> ConfigId {
        self.configuration
            .lock()
            .map(|state| state.config_id)
            .unwrap_or(self.config_id)
    }

    fn wal_checkpoint(&self) -> Result<WalCheckpoint> {
        self.wal
            .lock()
            .map(|wal| wal.checkpoint)
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))
    }

    #[cfg(test)]
    fn wal_stats(&self) -> Result<(u64, u64, u64)> {
        self.wal
            .lock()
            .map(|wal| {
                (
                    wal.checkpoint.generation,
                    wal.checkpoint.through_sequence,
                    wal.frame_count,
                )
            })
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))
    }
}

fn configuration_structure_changed(
    previous: &ConfigurationState,
    next: &ConfigurationState,
) -> bool {
    previous.config_id != next.config_id
        || previous.config_digest != next.config_digest
        || previous.membership != next.membership
        || previous.predecessor != next.predecessor
        || previous.seal != next.seal
        || previous.activated != next.activated
}

fn recorder_state_values(state: &RecorderSlotState) -> impl Iterator<Item = &AcceptedValue> {
    [
        state.accepted.as_ref().map(|accepted| &accepted.value),
        state.decided.as_ref().map(|decided| &decided.value),
        state
            .isr
            .first_current
            .as_ref()
            .and_then(|proposal| proposal.value.as_ref()),
        state
            .isr
            .aggregate_current
            .as_ref()
            .and_then(|proposal| proposal.value.as_ref()),
        state
            .isr
            .aggregate_prior
            .as_ref()
            .and_then(|proposal| proposal.value.as_ref()),
        state
            .decided_proof
            .as_ref()
            .and_then(|proof| proof.proposal().value.as_ref()),
    ]
    .into_iter()
    .flatten()
}

pub struct ThreeNodeConsensus {
    cluster_id: ClusterId,
    proposer_id: NodeId,
    epoch: Epoch,
    config_id: ConfigId,
    config_digest: LogHash,
    membership: FixedMembership,
    recorders: Vec<Arc<dyn RecorderRpc>>,
    record_workers: Vec<RecordWorker>,
    control_workers: Vec<ControlWorker>,
    // Read fences must not queue behind recovery/control RPCs whose network
    // deadline is intentionally longer. A lost majority can otherwise occupy
    // two control workers and turn a read-only quorum check into the caller's
    // HTTP timeout instead of a prompt Unavailable result.
    read_fence_workers: Vec<ControlWorker>,
    priority_source: Arc<dyn PrioritySource>,
    proposal_sequence: AtomicU64,
    sequential_tip: Mutex<SingleNodeState>,
}

struct RecordJob {
    index: usize,
    request: RecordRequest,
    result: std::sync::mpsc::SyncSender<(usize, Result<RecordSummary>)>,
}

struct RecordWorker {
    sender: Option<std::sync::mpsc::SyncSender<RecordJob>>,
    handle: Option<thread::JoinHandle<()>>,
    pending: Arc<AtomicUsize>,
}

impl RecordWorker {
    fn spawn(
        recorder_id: NodeId,
        recorder: Arc<dyn RecorderRpc>,
        config_id: ConfigId,
        config_digest: LogHash,
    ) -> Result<Self> {
        let expected_id = recorder_id;
        let (sender, receiver) =
            std::sync::mpsc::sync_channel::<RecordJob>(RECORD_WORKER_QUEUE_CAPACITY);
        let pending = Arc::new(AtomicUsize::new(0));
        let worker_pending = Arc::clone(&pending);
        let handle = thread::Builder::new()
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    let expected_slot = job.request.slot;
                    let reply = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        recorder.record(job.request)
                    }))
                    .unwrap_or(Err(Error::ProposeFailed))
                    .and_then(|reply| {
                        if reply.recorder_id == expected_id
                            && reply.slot == expected_slot
                            && reply.config_id == config_id
                            && reply.config_digest == config_digest
                        {
                            Ok(reply)
                        } else {
                            Err(Error::Rejected(RejectReason::InvalidRequest))
                        }
                    });
                    let _ = job.result.send((job.index, reply));
                    worker_pending.fetch_sub(1, Ordering::Release);
                }
            })
            .map_err(|error| Error::Io(error.to_string()))?;
        Ok(Self {
            sender: Some(sender),
            handle: Some(handle),
            pending,
        })
    }

    fn dispatch(&self, job: RecordJob) -> bool {
        self.pending.fetch_add(1, Ordering::Relaxed);
        let (failed, saturated) = match &self.sender {
            Some(sender) => match sender.try_send(job) {
                Ok(()) => (None, false),
                Err(std::sync::mpsc::TrySendError::Full(job)) => (
                    Some((
                        job,
                        Error::Io("recorder worker queue is temporarily full".into()),
                    )),
                    true,
                ),
                Err(std::sync::mpsc::TrySendError::Disconnected(job)) => {
                    (Some((job, Error::ProposeFailed)), false)
                }
            },
            None => (Some((job, Error::ProposeFailed)), false),
        };
        if let Some((job, error)) = failed {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            let _ = job.result.send((job.index, Err(error)));
        }
        saturated
    }

    fn is_idle(&self) -> bool {
        self.pending.load(Ordering::Acquire) == 0
    }

    fn shutdown(&mut self) {
        self.sender.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for RecordWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

enum ControlJob {
    InstallProof {
        index: usize,
        proof: DecisionProof,
        membership: Membership,
        result: std::sync::mpsc::SyncSender<(usize, Result<()>)>,
    },
    InspectProof {
        index: usize,
        slot: Slot,
        result: std::sync::mpsc::SyncSender<(usize, Result<Option<DecisionProof>>)>,
    },
    InspectSummary {
        index: usize,
        slot: Slot,
        result: std::sync::mpsc::SyncSender<(usize, Result<Option<RecordSummary>>)>,
    },
    ObserveReadFence {
        index: usize,
        request: ReadFenceRequest,
        result: std::sync::mpsc::SyncSender<(usize, Result<ReadFenceObservation>)>,
    },
    StoreCommand {
        index: usize,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
        result: std::sync::mpsc::SyncSender<(usize, Result<()>)>,
    },
    FetchCommand {
        index: usize,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
        result: std::sync::mpsc::SyncSender<(usize, Result<Option<StoredCommand>>)>,
    },
}

impl ControlJob {
    fn run(self, recorder: &dyn RecorderRpc) {
        match self {
            Self::InstallProof {
                index,
                proof,
                membership,
                result,
            } => {
                let reply = control_rpc(|| recorder.install_decision_proof(proof, &membership));
                let _ = result.send((index, reply));
            }
            Self::InspectProof {
                index,
                slot,
                result,
            } => {
                let reply = control_rpc(|| recorder.inspect_decision_proof(slot));
                let _ = result.send((index, reply));
            }
            Self::InspectSummary {
                index,
                slot,
                result,
            } => {
                let reply = control_rpc(|| recorder.inspect_record_summary(slot));
                let _ = result.send((index, reply));
            }
            Self::ObserveReadFence {
                index,
                request,
                result,
            } => {
                let reply = control_rpc(|| recorder.observe_read_fence(request));
                let _ = result.send((index, reply));
            }
            Self::StoreCommand {
                index,
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
                command,
                result,
            } => {
                let reply = control_rpc(|| {
                    recorder.store_command_for(
                        cluster_id,
                        epoch,
                        config_id,
                        config_digest,
                        command_hash,
                        command,
                    )
                });
                let _ = result.send((index, reply));
            }
            Self::FetchCommand {
                index,
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
                result,
            } => {
                let reply = control_rpc(|| {
                    recorder.fetch_command_for(
                        cluster_id,
                        epoch,
                        config_id,
                        config_digest,
                        command_hash,
                    )
                });
                let _ = result.send((index, reply));
            }
        }
    }

    fn fail(self, error: Error) {
        match self {
            Self::InstallProof { index, result, .. } | Self::StoreCommand { index, result, .. } => {
                let _ = result.send((index, Err(error)));
            }
            Self::InspectProof { index, result, .. } => {
                let _ = result.send((index, Err(error)));
            }
            Self::InspectSummary { index, result, .. } => {
                let _ = result.send((index, Err(error)));
            }
            Self::ObserveReadFence { index, result, .. } => {
                let _ = result.send((index, Err(error)));
            }
            Self::FetchCommand { index, result, .. } => {
                let _ = result.send((index, Err(error)));
            }
        }
    }
}

fn control_rpc<T>(call: impl FnOnce() -> Result<T>) -> Result<T> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(call))
        .unwrap_or(Err(Error::ProposeFailed))
}

struct QueuedControlJob {
    job: ControlJob,
    cancelled: Option<Arc<AtomicBool>>,
}

impl QueuedControlJob {
    fn run(self, recorder: &dyn RecorderRpc) {
        if !self
            .cancelled
            .as_ref()
            .is_some_and(|cancelled| cancelled.load(Ordering::Acquire))
        {
            self.job.run(recorder);
        }
    }

    fn fail(self, error: Error) {
        self.job.fail(error);
    }
}

struct ControlJobCancellation {
    cancelled: Arc<AtomicBool>,
}

impl ControlJobCancellation {
    fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    fn token(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }
}

impl Drop for ControlJobCancellation {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

struct ControlWorker {
    queue: Option<Arc<ControlQueue>>,
    handle: Option<thread::JoinHandle<()>>,
    pending: Arc<AtomicUsize>,
}

struct ControlQueue {
    state: Mutex<ControlQueueState>,
    available: Condvar,
}

struct ControlQueueState {
    jobs: VecDeque<QueuedControlJob>,
    closed: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ControlDispatch {
    Accepted,
    Saturated,
    Failed,
}

impl ControlWorker {
    fn spawn(recorder: Arc<dyn RecorderRpc>) -> Result<Self> {
        let queue = Arc::new(ControlQueue {
            state: Mutex::new(ControlQueueState {
                jobs: VecDeque::with_capacity(CONTROL_WORKER_QUEUE_CAPACITY),
                closed: false,
            }),
            available: Condvar::new(),
        });
        let worker_queue = Arc::clone(&queue);
        let pending = Arc::new(AtomicUsize::new(0));
        let worker_pending = Arc::clone(&pending);
        let handle = thread::Builder::new()
            .spawn(move || loop {
                let job = {
                    let mut state = worker_queue.state.lock().unwrap();
                    while state.jobs.is_empty() && !state.closed {
                        state = worker_queue.available.wait(state).unwrap();
                    }
                    state.jobs.pop_front()
                };
                let Some(job) = job else {
                    break;
                };
                job.run(recorder.as_ref());
                worker_pending.fetch_sub(1, Ordering::Release);
            })
            .map_err(|error| Error::Io(error.to_string()))?;
        Ok(Self {
            queue: Some(queue),
            handle: Some(handle),
            pending,
        })
    }

    fn dispatch(&self, job: ControlJob) -> ControlDispatch {
        self.dispatch_with_cancellation(job, None)
    }

    fn dispatch_cancellable(
        &self,
        job: ControlJob,
        cancellation: Arc<AtomicBool>,
    ) -> ControlDispatch {
        self.dispatch_with_cancellation(job, Some(cancellation))
    }

    fn dispatch_with_cancellation(
        &self,
        job: ControlJob,
        cancelled: Option<Arc<AtomicBool>>,
    ) -> ControlDispatch {
        let mut queued_job = Some(QueuedControlJob { job, cancelled });
        let (error, outcome) = match &self.queue {
            Some(queue) => {
                let mut state = queue.state.lock().unwrap();
                let queued_before = state.jobs.len();
                // A completed hedge must release queued capacity without waiting for a blocked peer.
                state.jobs.retain(|job| {
                    !job.cancelled
                        .as_ref()
                        .is_some_and(|cancelled| cancelled.load(Ordering::Acquire))
                });
                let removed = queued_before - state.jobs.len();
                if removed != 0 {
                    self.pending.fetch_sub(removed, Ordering::Release);
                }

                if state.closed {
                    (Some(Error::ProposeFailed), ControlDispatch::Failed)
                } else if state.jobs.len() >= CONTROL_WORKER_QUEUE_CAPACITY {
                    (
                        Some(Error::Io(
                            "recorder control worker queue is temporarily full".into(),
                        )),
                        ControlDispatch::Saturated,
                    )
                } else {
                    self.pending.fetch_add(1, Ordering::Relaxed);
                    state.jobs.push_back(queued_job.take().unwrap());
                    queue.available.notify_one();
                    (None, ControlDispatch::Accepted)
                }
            }
            None => (Some(Error::ProposeFailed), ControlDispatch::Failed),
        };
        if let Some(error) = error {
            queued_job.unwrap().fail(error);
        }
        outcome
    }

    fn is_idle(&self) -> bool {
        self.pending.load(Ordering::Acquire) == 0
    }

    fn shutdown(&mut self) {
        if let Some(queue) = self.queue.take() {
            queue.state.lock().unwrap().closed = true;
            queue.available.notify_one();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn control_quorum_reachable(successful: usize, saturated: usize, quorum: usize) -> bool {
    successful.saturating_add(saturated) >= quorum
}

impl Drop for ControlWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub trait PrioritySource: Send + Sync {
    fn sample(
        &self,
        slot: Slot,
        round: Round,
        proposer_id: &str,
        recorder_id: &str,
    ) -> Result<ProposalPriority>;
}

#[derive(Debug, Default)]
pub struct OsPrioritySource;

impl PrioritySource for OsPrioritySource {
    fn sample(
        &self,
        slot: Slot,
        round: Round,
        proposer_id: &str,
        recorder_id: &str,
    ) -> Result<ProposalPriority> {
        let mut bytes = [0; 32];
        let _ = (slot, round, proposer_id, recorder_id);
        getrandom::fill(&mut bytes)
            .map_err(|error| Error::RandomnessUnavailable(error.to_string()))?;
        if bytes == ProposalPriority::ZERO.0 || bytes == ProposalPriority::MAX.0 {
            bytes[31] = 1;
        }
        Ok(ProposalPriority(bytes))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposerProgress {
    pub slot: Slot,
    pub step: Step,
    pub proposal: Proposal,
    phase_zero_priorities: BTreeMap<(Round, NodeId), ProposalPriority>,
    command: Option<StoredCommand>,
    command_holders: BTreeSet<NodeId>,
    transition_involved: bool,
}

impl ProposerProgress {
    pub fn new(slot: Slot, proposal: Proposal) -> Self {
        Self {
            slot,
            step: 4,
            proposal,
            phase_zero_priorities: BTreeMap::new(),
            command: None,
            command_holders: BTreeSet::new(),
            transition_involved: false,
        }
    }

    fn with_command(mut self, command: StoredCommand) -> Self {
        self.transition_involved = command.entry_type == EntryType::ConfigChange;
        self.command = Some(command);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DriveOutcome {
    Progress(ProposerProgress),
    Pending(ProposerProgress),
    Decision(DecisionProof),
}

impl fmt::Debug for ThreeNodeConsensus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThreeNodeConsensus")
            .field("cluster_id", &self.cluster_id)
            .field("proposer_id", &self.proposer_id)
            .field("epoch", &self.epoch)
            .field("config_id", &self.config_id)
            .field("recorders", &self.membership.members())
            .finish_non_exhaustive()
    }
}

impl Drop for ThreeNodeConsensus {
    fn drop(&mut self) {
        for worker in &mut self.record_workers {
            worker.shutdown();
        }
        for worker in &mut self.control_workers {
            worker.shutdown();
        }
        for worker in &mut self.read_fence_workers {
            worker.shutdown();
        }
    }
}

impl ThreeNodeConsensus {
    /// Waits up to `timeout` for accepted recorder and control RPC jobs.
    /// Callers must first quiesce proposal admission and ensure no proposal
    /// can still dispatch jobs; this only drains already accepted jobs.
    pub fn finish_pending_rpcs(&self, timeout: Duration) -> bool {
        let started = Instant::now();
        loop {
            if self.record_workers.iter().all(RecordWorker::is_idle)
                && self.control_workers.iter().all(ControlWorker::is_idle)
                && self.read_fence_workers.iter().all(ControlWorker::is_idle)
            {
                return true;
            }
            if started.elapsed() >= timeout {
                return false;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    pub const fn config_id(&self) -> ConfigId {
        self.config_id
    }

    pub const fn membership(&self) -> &FixedMembership {
        &self.membership
    }

    pub fn new(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorder_roots: [PathBuf; 3],
    ) -> Result<Self> {
        Self::from_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorder_roots,
            1,
            LogHash::ZERO,
        )
    }

    pub fn from_recovered_tip(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorder_roots: [PathBuf; 3],
        next_index: LogIndex,
        last_hash: LogHash,
    ) -> Result<Self> {
        let cluster_id = cluster_id.into();
        let recorder_roots: Vec<_> = recorder_roots.into_iter().collect();
        let recorder_ids: Vec<_> = recorder_roots
            .iter()
            .map(|root| {
                root.file_name()
                    .and_then(|name| name.to_str())
                    .filter(|name| !name.is_empty())
                    .unwrap_or("recorder")
                    .to_owned()
            })
            .collect();
        let membership = Membership::from_voters(recorder_ids.iter().cloned())?;
        let recorders = recorder_roots
            .into_iter()
            .zip(recorder_ids)
            .map(|(root, recorder_id)| -> Result<Box<dyn RecorderRpc>> {
                Ok(Box::new(RecorderFileStore::new_with_membership(
                    root,
                    recorder_id,
                    cluster_id.clone(),
                    epoch,
                    config_id,
                    membership.clone(),
                )?) as Box<dyn RecorderRpc>)
            })
            .collect::<Result<Vec<_>>>()?;
        Self::from_recorders_with_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorders,
            next_index,
            last_hash,
        )
    }

    pub fn from_recorders(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorders: Vec<Box<dyn RecorderRpc>>,
    ) -> Result<Self> {
        Self::from_recorders_with_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorders,
            1,
            LogHash::ZERO,
        )
    }

    /// Constructs a proposer from expected recorder identities paired with RPC clients.
    ///
    /// This path does not issue `Identity` RPCs. Reply identities are still
    /// checked against the corresponding expected identity on every call.
    pub fn from_recorders_with_ids(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorders: Vec<(NodeId, Box<dyn RecorderRpc>)>,
    ) -> Result<Self> {
        Self::from_recorders_with_ids_and_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorders,
            1,
            LogHash::ZERO,
        )
    }

    pub fn from_recorders_with_recovered_tip(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        recorders: Vec<Box<dyn RecorderRpc>>,
        next_index: LogIndex,
        last_hash: LogHash,
    ) -> Result<Self> {
        let recorder_ids = recorders
            .iter()
            .map(|recorder| recorder.recorder_id())
            .collect::<Result<Vec<_>>>()?;
        Self::from_recorders_with_ids_and_recovered_tip(
            cluster_id,
            proposer_id,
            epoch,
            config_id,
            recorder_ids.into_iter().zip(recorders).collect(),
            next_index,
            last_hash,
        )
    }

    /// Recovered-tip variant of [`Self::from_recorders_with_ids`].
    pub fn from_recorders_with_ids_and_recovered_tip(
        cluster_id: impl Into<ClusterId>,
        proposer_id: impl Into<NodeId>,
        epoch: Epoch,
        config_id: ConfigId,
        mut recorders: Vec<(NodeId, Box<dyn RecorderRpc>)>,
        next_index: LogIndex,
        last_hash: LogHash,
    ) -> Result<Self> {
        if next_index == 0 {
            return Err(Error::InvalidRecoveredTip);
        }
        recorders.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let (recorder_ids, recorders): (Vec<_>, Vec<_>) = recorders.into_iter().unzip();
        let recorders: Vec<Arc<dyn RecorderRpc>> = recorders.into_iter().map(Arc::from).collect();
        let membership = FixedMembership::from_members(recorder_ids)?;
        let config_digest = membership.digest();
        let record_workers = membership
            .members()
            .iter()
            .cloned()
            .zip(&recorders)
            .map(|(recorder_id, recorder)| {
                RecordWorker::spawn(recorder_id, Arc::clone(recorder), config_id, config_digest)
            })
            .collect::<Result<Vec<_>>>()?;
        let control_workers = recorders
            .iter()
            .cloned()
            .map(ControlWorker::spawn)
            .collect::<Result<Vec<_>>>()?;
        let read_fence_workers = recorders
            .iter()
            .cloned()
            .map(ControlWorker::spawn)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            cluster_id: cluster_id.into(),
            proposer_id: proposer_id.into(),
            epoch,
            config_id,
            config_digest,
            membership,
            recorders,
            record_workers,
            control_workers,
            read_fence_workers,
            priority_source: Arc::new(OsPrioritySource),
            proposal_sequence: AtomicU64::new(1),
            sequential_tip: Mutex::new(SingleNodeState {
                next_index,
                last_hash,
            }),
        })
    }

    pub fn with_priority_source(mut self, source: Arc<dyn PrioritySource>) -> Self {
        self.priority_source = source;
        self
    }

    /// Stores command bytes on a recorder quorum after verifying their hash.
    ///
    /// [`Error::NoQuorum`] is retryable, including when bounded control-worker
    /// queues are temporarily saturated.
    pub fn register_command(&self, command_hash: LogHash, command_bytes: Vec<u8>) -> Result<()> {
        let command = StoredCommand::new(EntryType::Command, command_bytes);
        if command.hash() != command_hash {
            return Err(Error::CommandHashMismatch);
        }
        self.store_command_on_quorum(command_hash, &command)
    }

    fn propose_next(&self, command: Command) -> Result<LogEntry> {
        let mut tip = self
            .sequential_tip
            .lock()
            .map_err(|_| Error::ProposeFailed)?;
        let entry = self.propose_at(tip.next_index, tip.last_hash, command)?;
        tip.next_index = entry.index + 1;
        tip.last_hash = entry.hash;
        Ok(entry)
    }

    pub fn propose_at(&self, slot: Slot, prev_hash: LogHash, command: Command) -> Result<LogEntry> {
        self.propose_stored_at(slot, prev_hash, stored_command(command)?)
    }

    pub fn propose_at_cancellable(
        &self,
        slot: Slot,
        prev_hash: LogHash,
        command: Command,
        cancelled: &std::sync::atomic::AtomicBool,
    ) -> Result<LogEntry> {
        self.propose_stored_at_until(slot, prev_hash, stored_command(command)?, || {
            cancelled.load(Ordering::Acquire)
        })
    }

    pub fn propose_stop_at(&self, slot: Slot, prev_hash: LogHash) -> Result<LogEntry> {
        self.propose_stored_at(
            slot,
            prev_hash,
            ConfigChange::stop(self.config_id, self.config_digest).to_stored_command(),
        )
    }

    pub fn propose_stop_for_successor_at(
        &self,
        slot: Slot,
        prev_hash: LogHash,
        successor: &Membership,
    ) -> Result<LogEntry> {
        let next_config_id = self
            .config_id
            .checked_add(1)
            .ok_or(Error::Rejected(RejectReason::InvalidTransition))?;
        let stop = ConfigChange::bound_stop(
            self.cluster_id.clone(),
            self.config_id,
            self.config_digest,
            next_config_id,
            successor.members().to_vec(),
        )
        .map_err(|_| Error::Rejected(RejectReason::InvalidTransition))?;
        self.propose_stored_at(slot, prev_hash, stop.to_stored_command())
    }

    pub fn propose_activation_barrier_at(
        &self,
        stop_slot: Slot,
        prefix_hash: LogHash,
    ) -> Result<LogEntry> {
        self.propose_stored_at(
            stop_slot.checked_add(1).ok_or(Error::InvalidRecoveredTip)?,
            prefix_hash,
            ConfigChange::activation_barrier(
                self.config_id,
                self.config_digest,
                stop_slot,
                prefix_hash,
            )
            .to_stored_command(),
        )
    }

    pub fn propose_activation_for_stop_entry(&self, stop: &LogEntry) -> Result<LogEntry> {
        let command = StoredCommand::new(stop.entry_type, stop.payload.clone());
        let change = ConfigChange::recognize(&command)
            .map_err(|_| Error::Rejected(RejectReason::InvalidTransition))?;
        let successor = change
            .successor()
            .filter(|successor| {
                successor.cluster_id() == self.cluster_id
                    && successor.config_id() == self.config_id
                    && successor.digest() == self.config_digest
                    && successor.members() == self.membership.members()
            })
            .ok_or(Error::Rejected(RejectReason::InvalidTransition))?
            .clone();
        self.propose_stored_at(
            stop.index
                .checked_add(1)
                .ok_or(Error::InvalidRecoveredTip)?,
            stop.hash,
            ConfigChange::bound_activation_barrier(
                successor,
                stop.index,
                stop.hash,
                command.hash(),
            )
            .to_stored_command(),
        )
    }

    pub fn propose_activation_for_stop_at(&self, stop_proof: &DecisionProof) -> Result<LogEntry> {
        if proof_cluster_id(stop_proof) != self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        let (stop_slot, epoch, predecessor_config_id, _) = proof_context(stop_proof);
        if epoch != self.epoch || predecessor_config_id.checked_add(1) != Some(self.config_id) {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        let value = stop_proof
            .proposal()
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
        let bound_stop = ConfigChange::bound_stop(
            self.cluster_id.clone(),
            predecessor_config_id,
            proof_context(stop_proof).3,
            self.config_id,
            self.membership.members().to_vec(),
        )
        .map_err(|_| Error::Rejected(RejectReason::InvalidTransition))?;
        let stop_command = bound_stop.to_stored_command();
        let expected = AcceptedValue::from_command(
            &self.cluster_id,
            stop_slot,
            self.epoch,
            predecessor_config_id,
            value.prev_hash,
            &stop_command,
        );
        if &expected != value {
            return Err(Error::Rejected(RejectReason::InvalidTransition));
        }
        let successor = bound_stop
            .successor()
            .expect("bound stop has successor")
            .clone();
        self.propose_stored_at(
            stop_slot.checked_add(1).ok_or(Error::InvalidRecoveredTip)?,
            value.entry_hash,
            ConfigChange::bound_activation_barrier(
                successor,
                stop_slot,
                value.entry_hash,
                value.command_hash,
            )
            .to_stored_command(),
        )
    }

    pub fn propose_stored_at(
        &self,
        slot: Slot,
        prev_hash: LogHash,
        command: StoredCommand,
    ) -> Result<LogEntry> {
        self.propose_stored_at_until(slot, prev_hash, command, || false)
    }

    fn propose_stored_at_until<F>(
        &self,
        slot: Slot,
        prev_hash: LogHash,
        offered_command: StoredCommand,
        cancelled: F,
    ) -> Result<LogEntry>
    where
        F: Fn() -> bool,
    {
        if cancelled() {
            return Err(Error::Cancelled);
        }
        let offered_value = AcceptedValue::from_command(
            &self.cluster_id,
            slot,
            self.epoch,
            self.config_id,
            prev_hash,
            &offered_command,
        );
        let proposal_id = self.proposal_sequence.fetch_add(1, Ordering::Relaxed);
        let mut progress = ProposerProgress::new(
            slot,
            Proposal::new(
                ProposalPriority::MAX,
                self.proposer_id.clone(),
                proposal_id,
                offered_value,
            ),
        )
        .with_command(offered_command.clone());
        loop {
            if cancelled() {
                return Err(Error::Cancelled);
            }
            match self.drive(progress)? {
                DriveOutcome::Progress(next) => progress = next,
                DriveOutcome::Pending(next) => {
                    progress = next;
                    thread::sleep(std::time::Duration::from_millis(10));
                }
                DriveOutcome::Decision(proof) => {
                    let value = proof
                        .proposal()
                        .value
                        .as_ref()
                        .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
                    self.ensure_predecessor(slot, prev_hash, value.prev_hash)?;
                    let command = if self.command_matches_value(slot, value, &offered_command) {
                        offered_command.clone()
                    } else {
                        self.fetch_verified_value(slot, value)?
                            .ok_or(Error::CommandUnavailable)?
                    };
                    return self.log_entry_from_value(slot, command, value);
                }
            }
        }
    }

    pub fn drive(&self, mut progress: ProposerProgress) -> Result<DriveOutcome> {
        self.ensure_progress_command(&mut progress)?;
        let round = progress.step / 4;
        let phase = progress.step % 4;
        if phase == 0 {
            progress
                .phase_zero_priorities
                .retain(|(cached_round, _), _| *cached_round == round);
        } else {
            progress.phase_zero_priorities.clear();
        }
        let command_targets: BTreeSet<_> = self
            .membership
            .members()
            .iter()
            .filter(|recorder_id| !progress.command_holders.contains(*recorder_id))
            .cloned()
            .collect();
        let requests: Vec<_> = self
            .membership
            .members()
            .iter()
            .map(|recorder_id| -> Result<RecordRequest> {
                let mut proposal = progress.proposal.clone();
                if phase == 0 {
                    proposal.priority =
                        if progress.step == 4 && self.proposer_id == self.membership.members()[0] {
                            ProposalPriority::MAX
                        } else {
                            match progress
                                .phase_zero_priorities
                                .entry((round, recorder_id.clone()))
                            {
                                std::collections::btree_map::Entry::Occupied(entry) => *entry.get(),
                                std::collections::btree_map::Entry::Vacant(entry) => {
                                    *entry.insert(self.priority_source.sample(
                                        progress.slot,
                                        round,
                                        &self.proposer_id,
                                        recorder_id,
                                    )?)
                                }
                            }
                        };
                }
                Ok(RecordRequest {
                    cluster_id: self.cluster_id.clone(),
                    epoch: self.epoch,
                    config_id: self.config_id,
                    config_digest: self.config_digest,
                    slot: progress.slot,
                    step: progress.step,
                    proposal,
                    command: command_targets
                        .contains(recorder_id)
                        .then(|| progress.command.clone())
                        .flatten(),
                })
            })
            .collect::<Result<_>>()?;
        let mut replies = self.record_broadcast(requests)?;
        progress.command_holders.extend(
            replies
                .iter()
                .filter(|reply| command_targets.contains(&reply.recorder_id))
                .map(|reply| reply.recorder_id.clone()),
        );
        for reply in &replies {
            if let Some(proof) = &reply.decided {
                if proof_cluster_id(proof) != self.cluster_id {
                    return Err(Error::Rejected(RejectReason::WrongCluster));
                }
                proof
                    .validate_for_cluster(
                        &self.cluster_id,
                        progress.slot,
                        self.epoch,
                        self.config_id,
                        &self.membership,
                    )
                    .map_err(Error::Rejected)?;
                return self.finish_decision(
                    proof.clone(),
                    progress.command.as_ref(),
                    progress.transition_involved,
                );
            }
        }
        if let Some(highest) = replies.iter().map(|reply| reply.step).max() {
            if highest > progress.step {
                let caught_up = replies
                    .iter()
                    .filter(|reply| reply.step == highest)
                    .min_by(|left, right| left.recorder_id.cmp(&right.recorder_id))
                    .expect("highest reply exists");
                progress.step = highest;
                if let Some(proposal) = &caught_up.first_current {
                    progress.proposal = proposal.clone();
                }
                self.ensure_progress_command(&mut progress)?;
                progress.phase_zero_priorities.clear();
                return Ok(DriveOutcome::Progress(progress));
            }
        }
        replies.retain(|reply| reply.step == progress.step);
        replies.sort_by(|left, right| left.recorder_id.cmp(&right.recorder_id));
        replies.dedup_by(|left, right| left.recorder_id == right.recorder_id);
        if replies.len() < self.membership.quorum_size() {
            return Ok(DriveOutcome::Pending(progress));
        }
        replies.truncate(self.membership.quorum_size());
        let summaries: Vec<_> = replies
            .iter()
            .map(|reply| RecorderSummary {
                recorder_id: reply.recorder_id.clone(),
                slot: reply.slot,
                step: reply.step,
                first_current: reply.first_current.clone(),
                aggregate_prior: reply.aggregate_prior.clone(),
            })
            .collect();
        match phase {
            0 => {
                let fast_proposal = summaries
                    .first()
                    .and_then(|summary| summary.first_current.as_ref())
                    .filter(|proposal| proposal.priority == ProposalPriority::MAX)
                    .filter(|proposal| {
                        progress.step == 4
                            && summaries.iter().all(|summary| {
                                summary
                                    .first_current
                                    .as_ref()
                                    .is_some_and(|candidate| proposal_exact(candidate, proposal))
                            })
                    })
                    .cloned();
                if let Some(proposal) = fast_proposal {
                    let proof = DecisionProof::FastPath {
                        cluster_id: self.cluster_id.clone(),
                        slot: progress.slot,
                        epoch: self.epoch,
                        config_id: self.config_id,
                        config_digest: self.config_digest,
                        proposal,
                        summaries,
                    };
                    return self.finish_decision(
                        proof,
                        progress.command.as_ref(),
                        progress.transition_involved,
                    );
                }
                progress.proposal = replies
                    .iter()
                    .filter_map(|reply| reply.first_current.clone())
                    .max()
                    .ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
            }
            1 => {}
            2 => {
                let maximum = replies
                    .iter()
                    .filter_map(|reply| reply.aggregate_prior.clone())
                    .max();
                if maximum.as_ref() == Some(&progress.proposal) {
                    let proof = DecisionProof::Phase2 {
                        cluster_id: self.cluster_id.clone(),
                        slot: progress.slot,
                        epoch: self.epoch,
                        config_id: self.config_id,
                        config_digest: self.config_digest,
                        step: progress.step,
                        proposal: progress.proposal.clone(),
                        summaries,
                    };
                    return self.finish_decision(
                        proof,
                        progress.command.as_ref(),
                        progress.transition_involved,
                    );
                }
            }
            3 => {
                progress.proposal = replies
                    .iter()
                    .filter_map(|reply| reply.aggregate_prior.clone())
                    .max()
                    .ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
            }
            _ => unreachable!("phase is step modulo four"),
        }
        self.ensure_progress_command(&mut progress)?;
        progress.step = progress.step.checked_add(1).ok_or(Error::ProposeFailed)?;
        progress.phase_zero_priorities.clear();
        Ok(DriveOutcome::Progress(progress))
    }

    fn finish_decision(
        &self,
        proof: DecisionProof,
        known_command: Option<&StoredCommand>,
        transition_involved: bool,
    ) -> Result<DriveOutcome> {
        proof
            .validate_for_cluster(
                &self.cluster_id,
                proof_context(&proof).0,
                self.epoch,
                self.config_id,
                &self.membership,
            )
            .map_err(Error::Rejected)?;
        let value = proof
            .proposal()
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
        let command = match known_command {
            Some(command)
                if self.command_matches_value(proof_context(&proof).0, value, command) =>
            {
                command.clone()
            }
            _ => self
                .fetch_verified_value(proof_context(&proof).0, value)?
                .ok_or(Error::CommandUnavailable)?,
        };
        if !matches!(&proof, DecisionProof::Phase2 { .. })
            && command.entry_type != EntryType::ConfigChange
            && !transition_involved
        {
            return Ok(DriveOutcome::Decision(proof));
        }
        self.install_decision_proof_quorum(proof.clone())?;
        Ok(DriveOutcome::Decision(proof))
    }

    fn install_decision_proof_quorum(&self, proof: DecisionProof) -> Result<()> {
        let membership = self.membership.clone();
        let quorum = membership.quorum_size();
        let total = self.control_workers.len();
        let (sender, receiver) = std::sync::mpsc::sync_channel(total.max(1));
        let mut saturated = 0;
        for (index, worker) in self.control_workers.iter().enumerate() {
            if worker.dispatch(ControlJob::InstallProof {
                index,
                proof: proof.clone(),
                membership: membership.clone(),
                result: sender.clone(),
            }) == ControlDispatch::Saturated
            {
                saturated += 1;
            }
        }
        drop(sender);
        let mut installed = 0;
        let mut worker_failed = false;
        for (_, result) in receiver {
            match result {
                Ok(()) => installed += 1,
                Err(Error::ProposeFailed) => worker_failed = true,
                Err(_) => {}
            }
            if installed >= quorum {
                break;
            }
        }
        if installed < quorum {
            return Err(
                if worker_failed && !control_quorum_reachable(installed, saturated, quorum) {
                    Error::ProposeFailed
                } else {
                    Error::NoQuorum
                },
            );
        }
        Ok(())
    }

    fn record_broadcast(&self, requests: Vec<RecordRequest>) -> Result<Vec<RecordSummary>> {
        let quorum = self.membership.quorum_size();
        let total = self.record_workers.len().min(requests.len());
        let (sender, receiver) = std::sync::mpsc::sync_channel(total.max(1));
        let mut saturated_workers = vec![false; total];
        for (index, (worker, request)) in self.record_workers.iter().zip(requests).enumerate() {
            saturated_workers[index] = worker.dispatch(RecordJob {
                index,
                request,
                result: sender.clone(),
            });
        }
        drop(sender);
        let saturated = saturated_workers
            .iter()
            .filter(|saturated| **saturated)
            .count();
        let accepted = total.saturating_sub(saturated);
        let mut accepted_completed = 0;
        let mut typed_errors = vec![None; total];
        let mut worker_failed = false;
        let mut replies = Vec::with_capacity(quorum);
        for (index, result) in receiver {
            if !saturated_workers[index] {
                accepted_completed += 1;
            }
            match result {
                Ok(reply) => {
                    if !replies
                        .iter()
                        .any(|seen: &RecordSummary| seen.recorder_id == reply.recorder_id)
                    {
                        replies.push(reply);
                    }
                    if replies.len() >= quorum {
                        return Ok(replies);
                    }
                }
                Err(error @ Error::Rejected(_)) | Err(error @ Error::TypedRecordRequired) => {
                    typed_errors[index] = Some(error);
                }
                Err(Error::ProposeFailed) => worker_failed = true,
                Err(_) => {}
            }
            let accepted_remaining = accepted.saturating_sub(accepted_completed);
            if replies.len() + saturated + accepted_remaining < quorum {
                return match typed_errors.into_iter().flatten().next() {
                    Some(error) => Err(error),
                    None if worker_failed => Err(Error::ProposeFailed),
                    None => Ok(replies),
                };
            }
        }
        if replies.len() + saturated >= quorum {
            return Ok(replies);
        }
        match typed_errors.into_iter().flatten().next() {
            Some(error) => Err(error),
            None if worker_failed => Err(Error::ProposeFailed),
            None => Ok(replies),
        }
    }

    pub fn inspect_decision_at(
        &self,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<DecisionInspection> {
        Ok(match self.inspect_certified_decision_at(slot, prev_hash)? {
            CertifiedDecisionInspection::Committed(certified) => {
                DecisionInspection::Committed(certified.entry)
            }
            CertifiedDecisionInspection::Empty => DecisionInspection::Empty,
            CertifiedDecisionInspection::Pending => DecisionInspection::Pending,
            CertifiedDecisionInspection::Unavailable => DecisionInspection::Unavailable,
        })
    }

    pub fn inspect_decision_proof_at(&self, slot: Slot) -> Result<Option<DecisionProof>> {
        let quorum = self.membership.quorum_size();
        let total = self.control_workers.len();
        let (sender, receiver) = std::sync::mpsc::sync_channel(total.max(1));
        let mut saturated = 0;
        for (index, worker) in self.control_workers.iter().enumerate() {
            if worker.dispatch(ControlJob::InspectProof {
                index,
                slot,
                result: sender.clone(),
            }) == ControlDispatch::Saturated
            {
                saturated += 1;
            }
        }
        drop(sender);
        let mut successful = BTreeSet::new();
        let mut proofs = Vec::new();
        let mut worker_failed = false;
        for (index, result) in receiver {
            match result {
                Ok(proof) => {
                    successful.insert(self.membership.members()[index].clone());
                    proofs.extend(proof);
                }
                Err(Error::ProposeFailed) => worker_failed = true,
                Err(_) => {}
            }
            if successful.len() >= quorum {
                break;
            }
        }
        if successful.len() < quorum {
            return Err(
                if worker_failed && !control_quorum_reachable(successful.len(), saturated, quorum) {
                    Error::ProposeFailed
                } else {
                    Error::NoQuorum
                },
            );
        }
        self.select_decision_proof(slot, proofs)
    }

    fn select_decision_proof(
        &self,
        slot: Slot,
        mut proofs: Vec<DecisionProof>,
    ) -> Result<Option<DecisionProof>> {
        for proof in &proofs {
            if proof_cluster_id(proof) != self.cluster_id {
                return Err(Error::Rejected(RejectReason::WrongCluster));
            }
            proof
                .validate_for_cluster(
                    &self.cluster_id,
                    slot,
                    self.epoch,
                    self.config_id,
                    &self.membership,
                )
                .map_err(Error::Rejected)?;
        }
        let Some(first) = proofs.first() else {
            return Ok(None);
        };
        if proofs
            .iter()
            .skip(1)
            .any(|proof| proof.proposal().value != first.proposal().value)
        {
            return Err(Error::ConflictingCertificates);
        }
        proofs.sort_by_key(|proof| match proof {
            DecisionProof::FastPath { .. } => 4,
            DecisionProof::Phase2 { step, .. } => *step,
        });
        Ok(proofs.pop())
    }

    fn certified_inspection_from_proof(
        &self,
        slot: Slot,
        prev_hash: LogHash,
        proof: DecisionProof,
    ) -> Result<CertifiedDecisionInspection> {
        let decision = certificate_from_proof(&proof)?;
        self.ensure_predecessor(slot, prev_hash, decision.value.prev_hash)?;
        let Some(command) = self.fetch_verified_value(slot, &decision.value)? else {
            return Ok(CertifiedDecisionInspection::Unavailable);
        };
        if command.entry_type == EntryType::ConfigChange {
            self.install_decision_proof_quorum(proof.clone())?;
        }
        let entry = self.log_entry_from_value(slot, command, &decision.value)?;
        Ok(CertifiedDecisionInspection::Committed(Box::new(
            CertifiedDecision {
                entry,
                certificate: decision,
                proof,
            },
        )))
    }

    pub fn inspect_certified_decision_at(
        &self,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<CertifiedDecisionInspection> {
        self.inspect_typed_record_summaries(slot, prev_hash)
    }

    pub fn supports_context_read_fence(&self) -> bool {
        self.recorders
            .iter()
            .all(|recorder| recorder.supports_context_read_fence())
    }

    /// Observes whether `slot` is still empty at a quorum of recorders without
    /// mutating durable state. Any occupied or ambiguous quorum is delegated to
    /// the existing certified inspection path and can never become Empty.
    pub fn inspect_context_read_fence_at(
        &self,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<CertifiedDecisionInspection> {
        if !self.supports_context_read_fence() {
            return Err(Error::ReadFenceUnsupported);
        }
        let quorum = self.membership.quorum_size();
        let total = self.read_fence_workers.len();
        let request = ReadFenceRequest {
            cluster_id: self.cluster_id.clone(),
            epoch: self.epoch,
            config_id: self.config_id,
            config_digest: self.config_digest,
            slot,
        };
        let (sender, receiver) = std::sync::mpsc::sync_channel(total.max(1));
        let mut saturated = 0;
        for (index, worker) in self.read_fence_workers.iter().enumerate() {
            if worker.dispatch(ControlJob::ObserveReadFence {
                index,
                request: request.clone(),
                result: sender.clone(),
            }) == ControlDispatch::Saturated
            {
                saturated += 1;
            }
        }
        drop(sender);
        let mut successful = 0_usize;
        let mut empty = 0_usize;
        let mut worker_failed = false;
        let mut received = 0_usize;
        for (index, result) in receiver {
            received += 1;
            match result {
                Ok(observation)
                    if valid_read_fence_observation(
                        &observation,
                        &self.membership.members()[index],
                        &request,
                    ) =>
                {
                    successful += 1;
                    if observation.slot_state == ReadFenceSlotState::Empty {
                        empty += 1;
                        if empty >= quorum {
                            return Ok(CertifiedDecisionInspection::Empty);
                        }
                    }
                }
                Err(Error::ProposeFailed) => worker_failed = true,
                Ok(_) | Err(_) => {}
            }
            let remaining = total.saturating_sub(received);
            if successful.saturating_add(remaining) < quorum {
                return Ok(CertifiedDecisionInspection::Unavailable);
            }
        }
        if successful < quorum {
            if worker_failed && !control_quorum_reachable(successful, saturated, quorum) {
                return Err(Error::ProposeFailed);
            }
            return Ok(CertifiedDecisionInspection::Unavailable);
        }
        Ok(match self.inspect_certified_decision_at(slot, prev_hash)? {
            // An occupied fence quorum cannot be weakened by the typed
            // summary path's context-free absence classification.
            CertifiedDecisionInspection::Empty => CertifiedDecisionInspection::Pending,
            inspection => inspection,
        })
    }

    fn inspect_typed_record_summaries(
        &self,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<CertifiedDecisionInspection> {
        let quorum = self.membership.quorum_size();
        let config_id = self.config_id;
        let config_digest = self.config_digest;
        let total = self.control_workers.len();
        let (sender, receiver) = std::sync::mpsc::sync_channel(total.max(1));
        let cancellation = ControlJobCancellation::new();
        let mut saturated = 0;
        for (index, worker) in self.control_workers.iter().enumerate() {
            if worker.dispatch_cancellable(
                ControlJob::InspectSummary {
                    index,
                    slot,
                    result: sender.clone(),
                },
                cancellation.token(),
            ) == ControlDispatch::Saturated
            {
                saturated += 1;
            }
        }
        drop(sender);
        let mut successful = 0;
        let mut summaries = Vec::new();
        let mut worker_failed = false;
        for (index, result) in receiver {
            match result {
                Ok(summary)
                    if summary.as_ref().is_none_or(|summary| {
                        summary.recorder_id == self.membership.members()[index]
                            && summary.slot == slot
                            && summary.config_id == config_id
                            && summary.config_digest == config_digest
                    }) =>
                {
                    successful += 1;
                    summaries.extend(summary);
                }
                Err(Error::ProposeFailed) => worker_failed = true,
                Ok(_) | Err(_) => {}
            }
            if successful >= quorum {
                if let Some(proof) = self.proof_from_record_summaries(slot, &summaries)? {
                    return self.certified_inspection_from_proof(slot, prev_hash, proof);
                }
            }
        }
        if successful < quorum {
            if worker_failed && !control_quorum_reachable(successful, saturated, quorum) {
                return Err(Error::ProposeFailed);
            }
            return Ok(CertifiedDecisionInspection::Unavailable);
        }
        if summaries.is_empty() {
            return Ok(CertifiedDecisionInspection::Empty);
        }
        if summaries.len() < quorum {
            return Ok(CertifiedDecisionInspection::Unavailable);
        }
        if let Some(proof) = self.proof_from_record_summaries(slot, &summaries)? {
            return self.certified_inspection_from_proof(slot, prev_hash, proof);
        }
        Ok(CertifiedDecisionInspection::Pending)
    }

    fn proof_from_record_summaries(
        &self,
        slot: Slot,
        summaries: &[RecordSummary],
    ) -> Result<Option<DecisionProof>> {
        let quorum = self.membership.quorum_size();
        let mut summaries = summaries.to_vec();
        summaries.sort_by(|left, right| left.recorder_id.cmp(&right.recorder_id));
        summaries.dedup_by(|left, right| left.recorder_id == right.recorder_id);
        let installed_proofs = summaries
            .iter()
            .filter_map(|summary| summary.decided.clone())
            .collect();
        if let Some(proof) = self.select_decision_proof(slot, installed_proofs)? {
            return Ok(Some(proof));
        }
        for step in summaries
            .iter()
            .map(|summary| summary.step)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .rev()
        {
            let mut step_summaries: Vec<_> = summaries
                .iter()
                .filter(|summary| summary.step == step)
                .cloned()
                .collect();
            if step_summaries.len() < quorum {
                continue;
            }
            step_summaries.truncate(quorum);
            let proof_summaries: Vec<_> = step_summaries
                .iter()
                .map(|summary| RecorderSummary {
                    recorder_id: summary.recorder_id.clone(),
                    slot: summary.slot,
                    step: summary.step,
                    first_current: summary.first_current.clone(),
                    aggregate_prior: summary.aggregate_prior.clone(),
                })
                .collect();
            let proof = if step == 4 {
                step_summaries
                    .first()
                    .and_then(|summary| summary.first_current.clone())
                    .filter(|proposal| proposal.priority == ProposalPriority::MAX)
                    .filter(|proposal| {
                        step_summaries.iter().all(|summary| {
                            summary
                                .first_current
                                .as_ref()
                                .is_some_and(|candidate| proposal_exact(candidate, proposal))
                        })
                    })
                    .map(|proposal| DecisionProof::FastPath {
                        cluster_id: self.cluster_id.clone(),
                        slot,
                        epoch: self.epoch,
                        config_id: self.config_id,
                        config_digest: self.config_digest,
                        proposal,
                        summaries: proof_summaries.clone(),
                    })
            } else {
                None
            };
            let Some(proof) = proof else {
                continue;
            };
            let Ok(Some(proof)) = self.select_decision_proof(slot, vec![proof]) else {
                continue;
            };
            return Ok(Some(proof));
        }
        Ok(None)
    }

    pub fn recover_decision_at(
        &self,
        slot: Slot,
        prev_hash: LogHash,
    ) -> Result<DecisionInspection> {
        match self.inspect_decision_at(slot, prev_hash)? {
            DecisionInspection::Pending => self
                .propose_stored_at(
                    slot,
                    prev_hash,
                    StoredCommand::new(EntryType::Noop, Vec::new()),
                )
                .map(DecisionInspection::Committed),
            inspection => Ok(inspection),
        }
    }

    pub fn recover_decided_at(&self, slot: Slot, prev_hash: LogHash) -> Result<Option<LogEntry>> {
        match self.inspect_decision_at(slot, prev_hash)? {
            DecisionInspection::Committed(entry) => Ok(Some(entry)),
            DecisionInspection::Empty | DecisionInspection::Pending => Ok(None),
            DecisionInspection::Unavailable => Err(Error::CommandUnavailable),
        }
    }

    pub fn recover_decided_next(&self) -> Result<Option<LogEntry>> {
        let mut tip = self
            .sequential_tip
            .lock()
            .map_err(|_| Error::ProposeFailed)?;
        let Some(entry) = self.recover_decided_at(tip.next_index, tip.last_hash)? else {
            return Ok(None);
        };
        tip.next_index = entry
            .index
            .checked_add(1)
            .ok_or(Error::InvalidRecoveredTip)?;
        tip.last_hash = entry.hash;
        Ok(Some(entry))
    }

    fn ensure_predecessor(
        &self,
        slot: Slot,
        actual_prev_hash: LogHash,
        expected_prev_hash: LogHash,
    ) -> Result<()> {
        if actual_prev_hash != expected_prev_hash {
            return Err(Error::ChainConflict {
                slot,
                expected_prev_hash,
                actual_prev_hash,
            });
        }
        Ok(())
    }

    fn store_command_on_quorum(
        &self,
        command_hash: LogHash,
        command: &StoredCommand,
    ) -> Result<()> {
        let quorum = quorum_size(self.control_workers.len());
        let total = self.control_workers.len();
        let (sender, receiver) = std::sync::mpsc::sync_channel(total.max(1));
        let mut saturated = 0;
        for (index, worker) in self.control_workers.iter().enumerate() {
            if worker.dispatch(ControlJob::StoreCommand {
                index,
                cluster_id: self.cluster_id.clone(),
                epoch: self.epoch,
                config_id: self.config_id,
                config_digest: self.config_digest,
                command_hash,
                command: command.clone(),
                result: sender.clone(),
            }) == ControlDispatch::Saturated
            {
                saturated += 1;
            }
        }
        drop(sender);
        let mut stored = 0;
        let mut worker_failed = false;
        for (_, result) in receiver {
            match result {
                Ok(()) => stored += 1,
                Err(Error::ProposeFailed) => worker_failed = true,
                Err(_) => {}
            }
            if stored >= quorum {
                break;
            }
        }
        if stored < quorum {
            return Err(
                if worker_failed && !control_quorum_reachable(stored, saturated, quorum) {
                    Error::ProposeFailed
                } else {
                    Error::NoQuorum
                },
            );
        }
        Ok(())
    }

    fn fetch_verified_value(
        &self,
        slot: Slot,
        value: &AcceptedValue,
    ) -> Result<Option<StoredCommand>> {
        let quorum = quorum_size(self.control_workers.len());
        let total = self.control_workers.len();
        let (sender, receiver) = std::sync::mpsc::sync_channel(total.max(1));
        let cancellation = ControlJobCancellation::new();
        let mut saturated = 0;
        for (index, worker) in self.control_workers.iter().enumerate() {
            if worker.dispatch_cancellable(
                ControlJob::FetchCommand {
                    index,
                    cluster_id: self.cluster_id.clone(),
                    epoch: self.epoch,
                    config_id: self.config_id,
                    config_digest: self.config_digest,
                    command_hash: value.command_hash,
                    result: sender.clone(),
                },
                cancellation.token(),
            ) == ControlDispatch::Saturated
            {
                saturated += 1;
            }
        }
        drop(sender);
        let mut mismatch = false;
        let mut successful = 0;
        let mut worker_failed = false;
        for (_, result) in receiver {
            match result {
                Ok(command) => {
                    successful += 1;
                    if let Some(command) = command {
                        if command.hash() != value.command_hash {
                            continue;
                        }
                        let expected = AcceptedValue::from_command(
                            &self.cluster_id,
                            slot,
                            self.epoch,
                            self.config_id,
                            value.prev_hash,
                            &command,
                        );
                        if expected == *value {
                            return Ok(Some(command));
                        }
                        mismatch = true;
                    }
                }
                Err(Error::ProposeFailed) => worker_failed = true,
                Err(_) => {}
            }
        }
        if mismatch {
            Err(Error::Rejected(RejectReason::InvalidValue))
        } else if successful < quorum && saturated > 0 {
            Err(Error::NoQuorum)
        } else if worker_failed && !control_quorum_reachable(successful, saturated, quorum) {
            Err(Error::ProposeFailed)
        } else {
            Ok(None)
        }
    }

    fn ensure_progress_command(&self, progress: &mut ProposerProgress) -> Result<()> {
        let value = progress
            .proposal
            .value
            .as_ref()
            .ok_or(Error::Rejected(RejectReason::InvalidRequest))?;
        if progress
            .command
            .as_ref()
            .is_some_and(|command| self.command_matches_value(progress.slot, value, command))
        {
            return Ok(());
        }
        progress.command_holders.clear();
        progress.command = self.fetch_verified_value(progress.slot, value)?;
        if let Some(command) = &progress.command {
            progress.transition_involved |= command.entry_type == EntryType::ConfigChange;
            Ok(())
        } else {
            Err(Error::CommandUnavailable)
        }
    }

    fn command_matches_value(
        &self,
        slot: Slot,
        value: &AcceptedValue,
        command: &StoredCommand,
    ) -> bool {
        AcceptedValue::from_command(
            &self.cluster_id,
            slot,
            self.epoch,
            self.config_id,
            value.prev_hash,
            command,
        ) == *value
    }

    fn log_entry_from_value(
        &self,
        slot: Slot,
        command: StoredCommand,
        value: &AcceptedValue,
    ) -> Result<LogEntry> {
        let entry = LogEntry {
            cluster_id: self.cluster_id.clone(),
            epoch: self.epoch,
            config_id: self.config_id,
            index: slot,
            entry_type: command.entry_type,
            payload: command.payload,
            prev_hash: value.prev_hash,
            hash: value.entry_hash,
        };
        if entry.recompute_hash() != entry.hash {
            return Err(Error::Rejected(RejectReason::InvalidValue));
        }
        Ok(entry)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecisionInspection {
    Committed(LogEntry),
    Empty,
    Pending,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CertifiedDecision {
    pub entry: LogEntry,
    pub certificate: DecisionCertificate,
    pub proof: DecisionProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CertifiedDecisionInspection {
    Empty,
    Pending,
    Committed(Box<CertifiedDecision>),
    Unavailable,
}

impl RecorderRpc for RecorderFileStore {
    fn recorder_id(&self) -> Result<NodeId> {
        Ok(self.recorder_id.clone())
    }

    fn record(&self, request: RecordRequest) -> Result<RecordSummary> {
        self.record_proposal(request)
    }

    fn install_decision_proof(&self, proof: DecisionProof, membership: &Membership) -> Result<()> {
        self.install_decision_proof_record(proof, membership)
    }

    fn inspect_decision_proof(&self, slot: Slot) -> Result<Option<DecisionProof>> {
        Ok(self.load(slot)?.decision_proof().cloned())
    }

    fn inspect_record_summary(&self, slot: Slot) -> Result<Option<RecordSummary>> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        let configuration = self.configuration_state()?;
        let exists_in_wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?
            .slots
            .contains_key(&slot);
        if !exists_in_wal && !self.path(slot).exists() {
            return Ok(None);
        }
        let state = self.load_unlocked(slot, configuration.config_digest)?;
        Ok(Some(record_summary(
            &self.recorder_id,
            &state,
            state.decision_proof().cloned(),
        )))
    }

    fn supports_context_read_fence(&self) -> bool {
        true
    }

    fn observe_read_fence(&self, request: ReadFenceRequest) -> Result<ReadFenceObservation> {
        let _guard = self
            .sync
            .lock()
            .map_err(|_| Error::Io("recorder lock poisoned".into()))?;
        self.recover_intent()?;
        if request.cluster_id != self.cluster_id {
            return Err(Error::Rejected(RejectReason::WrongCluster));
        }
        if request.epoch != self.epoch {
            return Err(Error::Rejected(if request.epoch < self.epoch {
                RejectReason::StaleEpoch
            } else {
                RejectReason::FutureEpoch
            }));
        }
        let configuration = self.configuration_state()?;
        if request.config_id != configuration.config_id
            || request.config_digest != configuration.config_digest
        {
            return Err(Error::Rejected(RejectReason::WrongConfig));
        }
        let max_head = configuration.max_accepted_or_decided_slot;
        let exists_in_wal = self
            .wal
            .lock()
            .map_err(|_| Error::Io("recorder WAL lock poisoned".into()))?
            .slots
            .contains_key(&request.slot);
        let exact_exists = exists_in_wal || self.path(request.slot).exists();
        let summary = if exact_exists {
            let state = self.load_unlocked(request.slot, configuration.config_digest)?;
            Some(Box::new(record_summary(
                &self.recorder_id,
                &state,
                state.decision_proof().cloned(),
            )))
        } else {
            None
        };
        let slot_state =
            if summary.is_none() && max_head.is_none_or(|max_head| max_head < request.slot) {
                ReadFenceSlotState::Empty
            } else {
                ReadFenceSlotState::Occupied { summary }
            };
        Ok(ReadFenceObservation {
            recorder_id: self.recorder_id.clone(),
            cluster_id: request.cluster_id,
            epoch: request.epoch,
            config_id: request.config_id,
            config_digest: request.config_digest,
            slot: request.slot,
            max_head,
            slot_state,
        })
    }

    fn store_command(&self, command_hash: LogHash, command: StoredCommand) -> Result<()> {
        RecorderFileStore::store_command(self, command_hash, command)
    }

    fn store_command_for(
        &self,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> Result<()> {
        self.apply(RecorderRequest::StoreCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
            command,
        })?;
        Ok(())
    }

    fn fetch_command(&self, command_hash: LogHash) -> Result<Option<StoredCommand>> {
        RecorderFileStore::fetch_command(self, command_hash)
    }

    fn fetch_command_for(
        &self,
        cluster_id: ClusterId,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> Result<Option<StoredCommand>> {
        Ok(self
            .apply(RecorderRequest::FetchCommand {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
            })?
            .command)
    }
}

fn proposal_ballot(proposal: &Proposal) -> Option<Ballot> {
    proposal.value.as_ref()?;
    Some(Ballot::new(
        proposal.proposal_id,
        proposal.priority.low_u128(),
        proposal.proposer_id.clone(),
    ))
}

fn record_summary(
    recorder_id: &str,
    state: &RecorderSlotState,
    decided: Option<DecisionProof>,
) -> RecordSummary {
    RecordSummary {
        recorder_id: recorder_id.to_string(),
        slot: state.slot,
        config_id: state.config_id,
        config_digest: state.config_digest,
        step: state.isr.step(),
        first_current: state.isr.first_current().cloned(),
        aggregate_prior: state.isr.aggregate_prior().cloned(),
        decided,
    }
}

fn proof_context(proof: &DecisionProof) -> (Slot, Epoch, ConfigId, LogHash) {
    match proof {
        DecisionProof::FastPath {
            slot,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | DecisionProof::Phase2 {
            slot,
            epoch,
            config_id,
            config_digest,
            ..
        } => (*slot, *epoch, *config_id, *config_digest),
    }
}

fn proof_cluster_id(proof: &DecisionProof) -> &str {
    match proof {
        DecisionProof::FastPath { cluster_id, .. } | DecisionProof::Phase2 { cluster_id, .. } => {
            cluster_id
        }
    }
}

fn certificate_from_proof(proof: &DecisionProof) -> Result<DecisionCertificate> {
    let (slot, epoch, config_id, config_digest) = proof_context(proof);
    let proposal = proof.proposal();
    let value = proposal
        .value
        .clone()
        .ok_or(Error::Rejected(RejectReason::InvalidCertificate))?;
    let recorder_ids = match proof {
        DecisionProof::FastPath { summaries, .. } | DecisionProof::Phase2 { summaries, .. } => {
            summaries
                .iter()
                .map(|summary| summary.recorder_id.clone())
                .collect()
        }
    };
    Ok(DecisionCertificate {
        slot,
        epoch,
        config_id,
        config_digest,
        ballot: Ballot::new(
            proposal.proposal_id,
            proposal.priority.low_u128(),
            encode_certificate_proposer(proof_cluster_id(proof), &proposal.proposer_id),
        ),
        value,
        recorder_ids,
    })
}

const CERTIFICATE_PROPOSER_PREFIX: &str = "QDC1:";

fn encode_certificate_proposer(cluster_id: &str, proposer_id: &str) -> String {
    format!(
        "{CERTIFICATE_PROPOSER_PREFIX}{}:{cluster_id}{proposer_id}",
        cluster_id.len()
    )
}

fn decode_certificate_proposer(encoded: &str) -> Option<(&str, &str)> {
    let rest = encoded.strip_prefix(CERTIFICATE_PROPOSER_PREFIX)?;
    let (length, joined) = rest.split_once(':')?;
    let length: usize = length.parse().ok()?;
    Some((joined.get(..length)?, joined.get(length..)?))
}

impl Consensus for ThreeNodeConsensus {
    fn propose(&self, command: Command) -> Result<LogEntry> {
        self.propose_next(command)
    }
}

fn request_slot(request: &RecorderRequest) -> Option<Slot> {
    match request {
        RecorderRequest::Inspect { slot, .. } => Some(*slot),
        RecorderRequest::Identity
        | RecorderRequest::StoreCommand { .. }
        | RecorderRequest::FetchCommand { .. } => None,
    }
}

fn request_context(request: &RecorderRequest) -> Option<(&ClusterId, Epoch, ConfigId, LogHash)> {
    match request {
        RecorderRequest::StoreCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | RecorderRequest::FetchCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | RecorderRequest::Inspect {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        } => Some((cluster_id, *epoch, *config_id, *config_digest)),
        _ => None,
    }
}

fn stored_command(command: Command) -> Result<StoredCommand> {
    let entry_type = match command.kind() {
        CommandKind::Deterministic => EntryType::Command,
        CommandKind::ReadBarrier => EntryType::Noop,
    };
    Ok(StoredCommand::new(entry_type, command.payload().to_vec()))
}

fn encode_configuration_state(state: &ConfigurationState) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(b"QCON");
    put_u16(&mut out, CONFIGURATION_STATE_VERSION);
    put_u64(&mut out, state.config_id);
    out.extend_from_slice(state.config_digest.as_bytes());
    out.push(u8::from(state.activated));
    match state.max_accepted_or_decided_slot {
        Some(slot) => {
            out.push(1);
            put_u64(&mut out, slot);
        }
        None => out.push(0),
    }
    match &state.membership {
        Some(membership) => {
            out.push(membership.members().len() as u8);
            for member in membership.members() {
                put_bytes(&mut out, member.as_bytes())?;
            }
        }
        None => out.push(0),
    }
    encode_optional_seal(&mut out, state.predecessor.as_ref());
    encode_optional_seal(&mut out, state.seal.as_ref());
    let digest = LogHash::digest(&[&out]);
    out.extend_from_slice(digest.as_bytes());
    Ok(out)
}

fn decode_configuration_state(bytes: &[u8]) -> Result<ConfigurationState> {
    if bytes.len() < 4 + 2 + 32 || bytes.get(..4) != Some(b"QCON") {
        return Err(Error::Decode("invalid configuration state".into()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if LogHash::digest(&[body]).as_bytes() != digest {
        return Err(Error::Decode("configuration digest mismatch".into()));
    }
    let mut cursor = 4;
    if read_u16(body, &mut cursor)? != CONFIGURATION_STATE_VERSION {
        return Err(noncurrent_recorder_layout_error());
    }
    let config_id = read_u64(body, &mut cursor)?;
    let config_digest = read_hash(body, &mut cursor)?;
    let activated = match read_u8(body, &mut cursor)? {
        0 => false,
        1 => true,
        _ => return Err(Error::Decode("invalid activation flag".into())),
    };
    let max_accepted_or_decided_slot = match read_u8(body, &mut cursor)? {
        0 => None,
        1 => Some(read_u64(body, &mut cursor)?),
        _ => return Err(Error::Decode("invalid accepted-slot flag".into())),
    };
    let member_count = read_u8(body, &mut cursor)? as usize;
    let membership = if member_count == 0 {
        None
    } else {
        let members = (0..member_count)
            .map(|_| {
                String::from_utf8(read_bytes(body, &mut cursor)?)
                    .map_err(|err| Error::Decode(err.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let membership = Membership::from_voters(members)?;
        if membership.digest() != config_digest {
            return Err(Error::Decode("membership digest mismatch".into()));
        }
        Some(membership)
    };
    let predecessor = decode_optional_seal(body, &mut cursor)?;
    let seal = decode_optional_seal(body, &mut cursor)?;
    if cursor != body.len() {
        return Err(Error::Decode("trailing configuration bytes".into()));
    }
    Ok(ConfigurationState {
        config_id,
        config_digest,
        membership,
        predecessor,
        seal,
        max_accepted_or_decided_slot,
        activated,
    })
}

fn encode_optional_seal(out: &mut Vec<u8>, seal: Option<&ConfigurationSeal>) {
    match seal {
        Some(seal) => {
            out.push(1);
            put_u64(out, seal.stop_slot);
            out.extend_from_slice(seal.command_hash.as_bytes());
            out.extend_from_slice(seal.prefix_hash.as_bytes());
        }
        None => out.push(0),
    }
}

fn decode_optional_seal(bytes: &[u8], cursor: &mut usize) -> Result<Option<ConfigurationSeal>> {
    match read_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(ConfigurationSeal {
            stop_slot: read_u64(bytes, cursor)?,
            command_hash: read_hash(bytes, cursor)?,
            prefix_hash: read_hash(bytes, cursor)?,
        })),
        _ => Err(Error::Decode("invalid configuration seal flag".into())),
    }
}

fn encode_transition_intent(
    slot: Slot,
    slot_bytes: &[u8],
    configuration_bytes: &[u8],
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(b"QINT");
    put_u16(&mut out, 1);
    put_u64(&mut out, slot);
    put_bytes(&mut out, slot_bytes)?;
    put_bytes(&mut out, configuration_bytes)?;
    let digest = LogHash::digest(&[&out]);
    out.extend_from_slice(digest.as_bytes());
    Ok(out)
}

fn decode_transition_intent(bytes: &[u8]) -> Result<(Slot, Vec<u8>, Vec<u8>)> {
    if bytes.len() < 4 + 2 + 32 || bytes.get(..4) != Some(b"QINT") {
        return Err(Error::Decode("invalid transition intent".into()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if LogHash::digest(&[body]).as_bytes() != digest {
        return Err(Error::Decode("transition intent digest mismatch".into()));
    }
    let mut cursor = 4;
    if read_u16(body, &mut cursor)? != 1 {
        return Err(Error::Decode(
            "unsupported transition intent version".into(),
        ));
    }
    let slot = read_u64(body, &mut cursor)?;
    let slot_bytes = read_bytes(body, &mut cursor)?;
    let configuration_bytes = read_bytes(body, &mut cursor)?;
    if cursor != body.len() {
        return Err(Error::Decode("trailing transition intent bytes".into()));
    }
    Ok((slot, slot_bytes, configuration_bytes))
}

fn encode_recorder_state(state: &RecorderSlotState) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(b"QREC");
    put_u16(&mut out, RECORDER_STATE_VERSION);
    put_u64(&mut out, state.slot);
    put_u64(&mut out, state.epoch);
    put_u64(&mut out, state.config_id);
    out.extend_from_slice(state.config_digest.as_bytes());
    put_bytes(&mut out, state.cluster_id.as_bytes())?;
    encode_optional_ballot(&mut out, state.highest_promised.as_ref())?;
    match &state.accepted {
        Some(accepted) => {
            out.push(1);
            encode_ballot(&mut out, &accepted.ballot)?;
            encode_value(&mut out, &accepted.value);
        }
        None => out.push(0),
    }
    match &state.decided {
        Some(decided) => {
            out.push(1);
            encode_certificate(&mut out, decided)?;
        }
        None => out.push(0),
    }
    put_u64(&mut out, state.isr.step);
    encode_optional_proposal(&mut out, state.isr.first_current.as_ref())?;
    encode_optional_proposal(&mut out, state.isr.aggregate_current.as_ref())?;
    encode_optional_proposal(&mut out, state.isr.aggregate_prior.as_ref())?;
    encode_optional_proof(&mut out, state.decided_proof.as_ref())?;
    let digest = LogHash::digest(&[&out]);
    out.extend_from_slice(digest.as_bytes());
    Ok(out)
}

fn decode_recorder_state(bytes: &[u8]) -> Result<RecorderSlotState> {
    if bytes.len() < 4 + 2 + 32 || &bytes[..4] != b"QREC" {
        return Err(Error::Decode("invalid recorder magic".into()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if LogHash::digest(&[body]).as_bytes() != digest {
        return Err(Error::Decode("recorder digest mismatch".into()));
    }
    let mut cursor = 4;
    if read_u16(body, &mut cursor)? != RECORDER_STATE_VERSION {
        return Err(noncurrent_recorder_layout_error());
    }
    let slot = read_u64(body, &mut cursor)?;
    let epoch = read_u64(body, &mut cursor)?;
    let config_id = read_u64(body, &mut cursor)?;
    let config_digest = read_hash(body, &mut cursor)?;
    let cluster_id = String::from_utf8(read_bytes(body, &mut cursor)?)
        .map_err(|err| Error::Decode(err.to_string()))?;
    let highest_promised = decode_optional_ballot(body, &mut cursor)?;
    let accepted = match read_u8(body, &mut cursor)? {
        0 => None,
        1 => Some(AcceptedSummary {
            ballot: decode_ballot(body, &mut cursor)?,
            value: decode_value(body, &mut cursor)?,
        }),
        _ => return Err(Error::Decode("invalid accepted flag".into())),
    };
    let decided = match read_u8(body, &mut cursor)? {
        0 => None,
        1 => Some(decode_certificate(body, &mut cursor)?),
        _ => return Err(Error::Decode("invalid decided flag".into())),
    };
    let isr = IsrState {
        step: read_u64(body, &mut cursor)?,
        first_current: decode_optional_proposal(body, &mut cursor)?,
        aggregate_current: decode_optional_proposal(body, &mut cursor)?,
        aggregate_prior: decode_optional_proposal(body, &mut cursor)?,
    };
    let decided_proof = decode_optional_proof(body, &mut cursor)?;
    if cursor != body.len() {
        return Err(Error::Decode("trailing recorder bytes".into()));
    }
    if let Some(accepted) = &accepted {
        if highest_promised.as_ref() < Some(&accepted.ballot) {
            return Err(Error::Decode("accepted ballot exceeds promise".into()));
        }
    }
    if let Some(decided) = &decided {
        decided
            .validate_context(slot, epoch, config_id, config_digest)
            .map_err(|_| Error::Decode("invalid decision certificate".into()))?;
    }
    if let Some(proof) = &decided_proof {
        let proof_value = proof.proposal().value.as_ref();
        if proof_context(proof) != (slot, epoch, config_id, config_digest)
            || proof_cluster_id(proof) != cluster_id
            || !matches!((decided.as_ref(), proof_value), (Some(certificate), Some(value)) if &certificate.value == value)
        {
            return Err(Error::Decode("invalid persisted decision proof".into()));
        }
    }
    Ok(RecorderSlotState {
        slot,
        cluster_id,
        epoch,
        config_id,
        config_digest,
        highest_promised,
        accepted,
        decided,
        isr,
        decided_proof,
    })
}

fn encode_optional_proposal(out: &mut Vec<u8>, proposal: Option<&Proposal>) -> Result<()> {
    match proposal {
        None => out.push(0),
        Some(proposal) => {
            out.push(1);
            out.extend_from_slice(&proposal.priority.0);
            put_bytes(out, proposal.proposer_id.as_bytes())?;
            put_u64(out, proposal.proposal_id);
            match &proposal.value {
                None => out.push(0),
                Some(value) => {
                    out.push(1);
                    encode_value(out, value);
                }
            }
        }
    }
    Ok(())
}

fn decode_optional_proposal(bytes: &[u8], cursor: &mut usize) -> Result<Option<Proposal>> {
    match read_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => {
            let end = cursor
                .checked_add(32)
                .ok_or_else(|| Error::Decode("priority overflow".into()))?;
            let priority = ProposalPriority(
                bytes
                    .get(*cursor..end)
                    .ok_or_else(|| Error::Decode("truncated priority".into()))?
                    .try_into()
                    .expect("checked priority length"),
            );
            *cursor = end;
            let proposer_id = String::from_utf8(read_bytes(bytes, cursor)?)
                .map_err(|error| Error::Decode(error.to_string()))?;
            let proposal_id = read_u64(bytes, cursor)?;
            let value = match read_u8(bytes, cursor)? {
                0 => None,
                1 => Some(decode_value(bytes, cursor)?),
                _ => return Err(Error::Decode("invalid proposal value flag".into())),
            };
            Ok(Some(Proposal {
                priority,
                proposer_id,
                proposal_id,
                value,
            }))
        }
        _ => Err(Error::Decode("invalid proposal flag".into())),
    }
}

fn encode_summary(out: &mut Vec<u8>, summary: &RecorderSummary) -> Result<()> {
    put_bytes(out, summary.recorder_id.as_bytes())?;
    put_u64(out, summary.slot);
    put_u64(out, summary.step);
    encode_optional_proposal(out, summary.first_current.as_ref())?;
    encode_optional_proposal(out, summary.aggregate_prior.as_ref())
}

fn decode_summary(bytes: &[u8], cursor: &mut usize) -> Result<RecorderSummary> {
    Ok(RecorderSummary {
        recorder_id: String::from_utf8(read_bytes(bytes, cursor)?)
            .map_err(|error| Error::Decode(error.to_string()))?,
        slot: read_u64(bytes, cursor)?,
        step: read_u64(bytes, cursor)?,
        first_current: decode_optional_proposal(bytes, cursor)?,
        aggregate_prior: decode_optional_proposal(bytes, cursor)?,
    })
}

fn encode_optional_proof(out: &mut Vec<u8>, proof: Option<&DecisionProof>) -> Result<()> {
    let Some(proof) = proof else {
        out.push(0);
        return Ok(());
    };
    let (tag, cluster_id, slot, epoch, config_id, digest, step, proposal, summaries) = match proof {
        DecisionProof::FastPath {
            cluster_id,
            slot,
            epoch,
            config_id,
            config_digest,
            proposal,
            summaries,
        } => (
            1,
            cluster_id,
            *slot,
            *epoch,
            *config_id,
            *config_digest,
            4,
            proposal,
            summaries,
        ),
        DecisionProof::Phase2 {
            cluster_id,
            slot,
            epoch,
            config_id,
            config_digest,
            step,
            proposal,
            summaries,
        } => (
            2,
            cluster_id,
            *slot,
            *epoch,
            *config_id,
            *config_digest,
            *step,
            proposal,
            summaries,
        ),
    };
    out.push(tag);
    put_bytes(out, cluster_id.as_bytes())?;
    put_u64(out, slot);
    put_u64(out, epoch);
    put_u64(out, config_id);
    out.extend_from_slice(digest.as_bytes());
    put_u64(out, step);
    encode_optional_proposal(out, Some(proposal))?;
    put_u16(
        out,
        u16::try_from(summaries.len())
            .map_err(|_| Error::Decode("too many proof summaries".into()))?,
    );
    for summary in summaries {
        encode_summary(out, summary)?;
    }
    Ok(())
}

fn decode_optional_proof(bytes: &[u8], cursor: &mut usize) -> Result<Option<DecisionProof>> {
    let tag = read_u8(bytes, cursor)?;
    if tag == 0 {
        return Ok(None);
    }
    let cluster_id = String::from_utf8(read_bytes(bytes, cursor)?)
        .map_err(|error| Error::Decode(error.to_string()))?;
    let slot = read_u64(bytes, cursor)?;
    let epoch = read_u64(bytes, cursor)?;
    let config_id = read_u64(bytes, cursor)?;
    let config_digest = read_hash(bytes, cursor)?;
    let step = read_u64(bytes, cursor)?;
    let proposal = decode_optional_proposal(bytes, cursor)?
        .ok_or_else(|| Error::Decode("nil decision proposal".into()))?;
    let summaries = (0..read_u16(bytes, cursor)? as usize)
        .map(|_| decode_summary(bytes, cursor))
        .collect::<Result<Vec<_>>>()?;
    match tag {
        1 if step == 4 => Ok(Some(DecisionProof::FastPath {
            cluster_id,
            slot,
            epoch,
            config_id,
            config_digest,
            proposal,
            summaries,
        })),
        2 => Ok(Some(DecisionProof::Phase2 {
            cluster_id,
            slot,
            epoch,
            config_id,
            config_digest,
            step,
            proposal,
            summaries,
        })),
        _ => Err(Error::Decode("invalid decision proof tag".into())),
    }
}

fn encode_optional_ballot(out: &mut Vec<u8>, ballot: Option<&Ballot>) -> Result<()> {
    match ballot {
        Some(ballot) => {
            out.push(1);
            encode_ballot(out, ballot)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

fn decode_optional_ballot(bytes: &[u8], cursor: &mut usize) -> Result<Option<Ballot>> {
    match read_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(decode_ballot(bytes, cursor)?)),
        _ => Err(Error::Decode("invalid ballot flag".into())),
    }
}

fn encode_ballot(out: &mut Vec<u8>, ballot: &Ballot) -> Result<()> {
    put_u64(out, ballot.round);
    put_u128(out, ballot.priority);
    put_bytes(out, ballot.proposer_id.as_bytes())
}

fn decode_ballot(bytes: &[u8], cursor: &mut usize) -> Result<Ballot> {
    Ok(Ballot {
        round: read_u64(bytes, cursor)?,
        priority: read_u128(bytes, cursor)?,
        proposer_id: String::from_utf8(read_bytes(bytes, cursor)?)
            .map_err(|err| Error::Decode(err.to_string()))?,
    })
}

fn encode_value(out: &mut Vec<u8>, value: &AcceptedValue) {
    out.extend_from_slice(value.command_hash.as_bytes());
    out.extend_from_slice(value.prev_hash.as_bytes());
    out.extend_from_slice(value.entry_hash.as_bytes());
}

fn decode_value(bytes: &[u8], cursor: &mut usize) -> Result<AcceptedValue> {
    Ok(AcceptedValue {
        command_hash: read_hash(bytes, cursor)?,
        prev_hash: read_hash(bytes, cursor)?,
        entry_hash: read_hash(bytes, cursor)?,
    })
}

fn encode_certificate(out: &mut Vec<u8>, decision: &DecisionCertificate) -> Result<()> {
    put_u64(out, decision.slot);
    put_u64(out, decision.epoch);
    put_u64(out, decision.config_id);
    out.extend_from_slice(decision.config_digest.as_bytes());
    encode_ballot(out, &decision.ballot)?;
    encode_value(out, &decision.value);
    put_u16(
        out,
        u16::try_from(decision.recorder_ids.len())
            .map_err(|_| Error::Decode("too many certificate recorders".into()))?,
    );
    for recorder_id in &decision.recorder_ids {
        put_bytes(out, recorder_id.as_bytes())?;
    }
    Ok(())
}

fn decode_certificate(bytes: &[u8], cursor: &mut usize) -> Result<DecisionCertificate> {
    let slot = read_u64(bytes, cursor)?;
    let epoch = read_u64(bytes, cursor)?;
    let config_id = read_u64(bytes, cursor)?;
    let config_digest = read_hash(bytes, cursor)?;
    let ballot = decode_ballot(bytes, cursor)?;
    let value = decode_value(bytes, cursor)?;
    let recorder_count = read_u16(bytes, cursor)? as usize;
    let recorder_ids = (0..recorder_count)
        .map(|_| {
            String::from_utf8(read_bytes(bytes, cursor)?)
                .map_err(|err| Error::Decode(err.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DecisionCertificate {
        slot,
        epoch,
        config_id,
        config_digest,
        ballot,
        value,
        recorder_ids,
    })
}

fn encode_stored_command(command: &StoredCommand) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"QCMD");
    put_u16(&mut out, 1);
    out.push(command.entry_type.as_u8());
    put_u64(&mut out, command.payload.len() as u64);
    out.extend_from_slice(&command.payload);
    let digest = LogHash::digest(&[&out]);
    out.extend_from_slice(digest.as_bytes());
    out
}

fn decode_stored_command(bytes: &[u8]) -> Result<StoredCommand> {
    if bytes.len() < 4 + 2 + 1 + 8 + 32 || &bytes[..4] != b"QCMD" {
        return Err(Error::Decode("invalid command magic".into()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if LogHash::digest(&[body]).as_bytes() != digest {
        return Err(Error::Decode("command digest mismatch".into()));
    }
    let mut cursor = 4;
    if read_u16(body, &mut cursor)? != 1 {
        return Err(Error::Decode("unsupported command version".into()));
    }
    let entry_type = EntryType::from_u8(read_u8(body, &mut cursor)?)
        .ok_or_else(|| Error::Decode("invalid command entry type".into()))?;
    let payload_len = usize::try_from(read_u64(body, &mut cursor)?)
        .map_err(|_| Error::Decode("command payload too large".into()))?;
    let end = cursor
        .checked_add(payload_len)
        .ok_or_else(|| Error::Decode("command payload length overflow".into()))?;
    let payload = body
        .get(cursor..end)
        .ok_or_else(|| Error::Decode("short command payload".into()))?
        .to_vec();
    cursor = end;
    if cursor != body.len() {
        return Err(Error::Decode("trailing command bytes".into()));
    }
    Ok(StoredCommand::new(entry_type, payload))
}

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
std::thread_local! {
    static SYNC_COUNTS: std::cell::Cell<(usize, usize)> = const {
        std::cell::Cell::new((0, 0))
    };
    static LAST_FILE_SYNC_KIND: std::cell::Cell<Option<FileSyncKind>> = const {
        std::cell::Cell::new(None)
    };
    static COMMAND_FILE_READS: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileSyncKind {
    #[cfg(target_os = "linux")]
    Data,
    All,
}

#[cfg(target_os = "linux")]
fn sync_wal_append(file: &fs::File) -> io::Result<()> {
    // Linux fdatasync (File::sync_data) also flushes metadata required for later data retrieval,
    // including the file size extended by this append, so a complete WAL frame remains replayable.
    file.sync_data()?;
    #[cfg(test)]
    record_file_sync_kind(FileSyncKind::Data);
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn sync_wal_append(file: &fs::File) -> io::Result<()> {
    // Keep non-Linux durability conservative because sync_data semantics vary by platform.
    file.sync_all()?;
    #[cfg(test)]
    record_file_sync_kind(FileSyncKind::All);
    Ok(())
}

fn sync_wal_metadata(file: &fs::File) -> io::Result<()> {
    file.sync_all()?;
    #[cfg(test)]
    record_file_sync_kind(FileSyncKind::All);
    Ok(())
}

#[cfg(test)]
fn record_file_sync() {
    SYNC_COUNTS.with(|counts| {
        let (file, directory) = counts.get();
        counts.set((file + 1, directory));
    });
}

#[cfg(test)]
fn record_file_sync_kind(kind: FileSyncKind) {
    record_file_sync();
    LAST_FILE_SYNC_KIND.with(|last| last.set(Some(kind)));
}

#[cfg(test)]
fn record_directory_sync() {
    SYNC_COUNTS.with(|counts| {
        let (file, directory) = counts.get();
        counts.set((file, directory + 1));
    });
}

#[cfg(test)]
fn reset_sync_counts() {
    SYNC_COUNTS.with(|counts| counts.set((0, 0)));
    LAST_FILE_SYNC_KIND.with(|last| last.set(None));
}

#[cfg(test)]
fn sync_counts() -> (usize, usize) {
    SYNC_COUNTS.with(std::cell::Cell::get)
}

#[cfg(test)]
fn last_file_sync_kind() -> Option<FileSyncKind> {
    LAST_FILE_SYNC_KIND.with(std::cell::Cell::get)
}

#[cfg(test)]
fn reset_command_file_reads() {
    COMMAND_FILE_READS.with(|reads| reads.set(0));
}

#[cfg(test)]
fn command_file_reads() -> usize {
    COMMAND_FILE_READS.with(std::cell::Cell::get)
}

const CONFIGURATION_HEAD_INTENT_MAGIC: &[u8; 4] = b"QCHI";

fn encode_configuration_head_intent(configuration: &[u8], head: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(CONFIGURATION_HEAD_INTENT_MAGIC);
    put_u16(&mut encoded, 1);
    put_u64(&mut encoded, configuration.len() as u64);
    encoded.extend_from_slice(configuration);
    put_u64(&mut encoded, head.len() as u64);
    encoded.extend_from_slice(head);
    encoded
}

fn decode_configuration_head_intent(bytes: &[u8]) -> Result<(&[u8], &[u8])> {
    let mut cursor = 0;
    if bytes.get(..CONFIGURATION_HEAD_INTENT_MAGIC.len()) != Some(CONFIGURATION_HEAD_INTENT_MAGIC) {
        return Err(Error::Decode(
            "invalid configuration-head intent magic".into(),
        ));
    }
    cursor += CONFIGURATION_HEAD_INTENT_MAGIC.len();
    if read_u16(bytes, &mut cursor)? != 1 {
        return Err(Error::Decode(
            "unsupported configuration-head intent version".into(),
        ));
    }
    let configuration_len = usize::try_from(read_u64(bytes, &mut cursor)?)
        .map_err(|_| Error::Decode("configuration-head intent length overflow".into()))?;
    let configuration_end = cursor
        .checked_add(configuration_len)
        .ok_or_else(|| Error::Decode("configuration-head intent length overflow".into()))?;
    let configuration = bytes
        .get(cursor..configuration_end)
        .ok_or_else(|| Error::Decode("truncated configuration-head intent".into()))?;
    cursor = configuration_end;
    let head_len = usize::try_from(read_u64(bytes, &mut cursor)?)
        .map_err(|_| Error::Decode("configuration-head intent length overflow".into()))?;
    let head_end = cursor
        .checked_add(head_len)
        .ok_or_else(|| Error::Decode("configuration-head intent length overflow".into()))?;
    let head = bytes
        .get(cursor..head_end)
        .ok_or_else(|| Error::Decode("truncated configuration-head intent".into()))?;
    if head_end != bytes.len() {
        return Err(Error::Decode(
            "trailing configuration-head intent bytes".into(),
        ));
    }
    Ok((configuration, head))
}

fn encode_wal_frame(
    generation: u64,
    sequence: u64,
    prev_digest: LogHash,
    slot_state: &RecorderSlotState,
    configuration: &ConfigurationState,
    head: &RecordedHeadProvenance,
    command: Option<(LogHash, &StoredCommand)>,
) -> Result<(Vec<u8>, LogHash, Vec<u8>)> {
    let slot_bytes = encode_recorder_state(slot_state)?;
    let configuration_bytes = encode_configuration_state(configuration)?;
    let mut payload = Vec::new();
    put_u64(&mut payload, generation);
    put_u64(&mut payload, sequence);
    payload.extend_from_slice(prev_digest.as_bytes());
    put_u64(&mut payload, slot_state.slot());
    put_blob(&mut payload, &slot_bytes)?;
    put_blob(&mut payload, &configuration_bytes)?;
    encode_head_provenance(&mut payload, head);
    match command {
        Some((hash, command)) => {
            payload.push(1);
            payload.extend_from_slice(hash.as_bytes());
            put_blob(&mut payload, &encode_stored_command(command))?;
        }
        None => payload.push(0),
    }
    let total_len = 4usize
        .checked_add(2)
        .and_then(|len| len.checked_add(8))
        .and_then(|len| len.checked_add(payload.len()))
        .and_then(|len| len.checked_add(32))
        .ok_or_else(|| Error::Io("recorder WAL frame length overflow".into()))?;
    let mut frame = Vec::with_capacity(total_len);
    frame.extend_from_slice(RECORDER_WAL_MAGIC);
    put_u16(&mut frame, RECORDER_WAL_VERSION);
    put_u64(&mut frame, total_len as u64);
    frame.extend_from_slice(&payload);
    let digest = LogHash::digest(&[&frame]);
    frame.extend_from_slice(digest.as_bytes());
    Ok((frame, digest, slot_bytes))
}

fn decode_wal_frame(bytes: &[u8], offset: usize) -> Result<Option<(WalFrame, usize)>> {
    const PREFIX_LEN: usize = 4 + 2 + 8;
    let remaining = bytes
        .get(offset..)
        .ok_or_else(|| Error::Decode("recorder WAL offset overflow".into()))?;
    if remaining.len() < PREFIX_LEN {
        return Ok(None);
    }
    if remaining.get(..4) != Some(RECORDER_WAL_MAGIC) {
        return Err(Error::Decode("recorder WAL frame magic mismatch".into()));
    }
    let mut cursor = offset + 4;
    if read_u16(bytes, &mut cursor)? != RECORDER_WAL_VERSION {
        return Err(Error::Decode("recorder WAL frame version mismatch".into()));
    }
    let frame_len = usize::try_from(read_u64(bytes, &mut cursor)?)
        .map_err(|_| Error::Decode("recorder WAL frame length overflow".into()))?;
    if frame_len < PREFIX_LEN + 32 {
        return Err(Error::Decode("recorder WAL frame length is invalid".into()));
    }
    let end = offset
        .checked_add(frame_len)
        .ok_or_else(|| Error::Decode("recorder WAL frame length overflow".into()))?;
    if end > bytes.len() {
        return Ok(None);
    }
    let digest_offset = end - 32;
    let digest = read_hash(bytes, &mut { digest_offset })?;
    let expected = LogHash::digest(&[&bytes[offset..digest_offset]]);
    if digest != expected {
        return Err(Error::Decode("recorder WAL frame checksum mismatch".into()));
    }
    let generation = read_u64(bytes, &mut cursor)?;
    let sequence = read_u64(bytes, &mut cursor)?;
    let prev_digest = read_hash(bytes, &mut cursor)?;
    let slot = read_u64(bytes, &mut cursor)?;
    let slot_bytes = read_blob(bytes, &mut cursor)?;
    let configuration_bytes = read_blob(bytes, &mut cursor)?;
    let head = decode_head_provenance(bytes, &mut cursor)?;
    let command = match read_u8(bytes, &mut cursor)? {
        0 => None,
        1 => {
            let hash = read_hash(bytes, &mut cursor)?;
            let command = decode_stored_command(&read_blob(bytes, &mut cursor)?)?;
            Some((hash, command))
        }
        _ => return Err(Error::Decode("recorder WAL command flag is invalid".into())),
    };
    if cursor != digest_offset {
        return Err(Error::Decode(
            "recorder WAL frame has trailing bytes".into(),
        ));
    }
    Ok(Some((
        WalFrame {
            generation,
            sequence,
            prev_digest,
            digest,
            slot,
            slot_bytes,
            configuration_bytes,
            head,
            command,
        },
        end,
    )))
}

fn read_regular_file_bounded(path: &Path, maximum: usize, name: &str) -> Result<Vec<u8>> {
    let before = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(Error::Decode(format!("recorder is missing {name}")))
        }
        Err(error) => return Err(Error::Io(error.to_string())),
    };
    if before.file_type().is_symlink() || !before.is_file() || before.len() > maximum as u64 {
        return Err(Error::Decode(format!(
            "recorder {name} must be a bounded regular file"
        )));
    }
    let mut file = open_regular_file_no_follow(path)?;
    let opened = file
        .metadata()
        .map_err(|error| Error::Io(error.to_string()))?;
    if !opened.is_file()
        || !same_opened_file(&before, &opened)
        || opened.len() != before.len()
        || opened.len() > maximum as u64
    {
        return Err(Error::Decode(format!(
            "recorder {name} changed before no-follow open"
        )));
    }
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    Read::by_ref(&mut file)
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| Error::Io(error.to_string()))?;
    let after_opened = file
        .metadata()
        .map_err(|error| Error::Io(error.to_string()))?;
    let after_path = fs::symlink_metadata(path).map_err(|error| Error::Io(error.to_string()))?;
    if after_path.file_type().is_symlink()
        || !after_path.is_file()
        || !after_opened.is_file()
        || !same_opened_file(&opened, &after_opened)
        || !same_opened_file(&opened, &after_path)
        || after_opened.len() != opened.len()
        || after_path.len() != opened.len()
    {
        return Err(Error::Decode(format!(
            "recorder {name} changed during bounded read"
        )));
    }
    if bytes.len() > maximum || bytes.len() as u64 != opened.len() {
        return Err(Error::Decode(format!(
            "recorder {name} exceeds its size limit"
        )));
    }
    Ok(bytes)
}

fn open_regular_file_no_follow(path: &Path) -> Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    options.custom_flags(O_NOFOLLOW_FLAG);
    options
        .open(path)
        .map_err(|error| Error::Io(error.to_string()))
}

fn prepare_fresh_recorder_root(root: &Path) -> Result<()> {
    if let Some(parent) = root
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        create_real_directory_if_missing(parent)?;
    }
    match fs::create_dir(root) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(Error::Io(error.to_string())),
    }
    ensure_real_directory(root, "recorder root")
}

fn create_real_directory_if_missing(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => return ensure_real_directory(path, "recorder root parent"),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(Error::Io(error.to_string())),
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        create_real_directory_if_missing(parent)?;
    }
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(Error::Io(error.to_string())),
    }
    ensure_real_directory(path, "recorder root parent")
}

fn ensure_real_directory(path: &Path, name: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| Error::Io(error.to_string()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::Decode(format!("{name} must be a real directory")));
    }
    Ok(())
}

fn open_or_create_fresh_root_lock(root: &Path) -> Result<fs::File> {
    let path = root.join(".recorder.lock");
    let mut options = fs::OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    options.custom_flags(O_NOFOLLOW_FLAG);
    match options.open(&path) {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            open_existing_root_lock(&path)
        }
        Err(error) => Err(Error::Io(error.to_string())),
    }
}

fn open_existing_root_lock(path: &Path) -> Result<fs::File> {
    let before = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(Error::Decode("recorder root lock does not exist".into()))
        }
        Err(error) => return Err(Error::Io(error.to_string())),
    };
    if before.file_type().is_symlink() || !before.is_file() {
        return Err(Error::Decode(
            "recorder root lock must be an existing regular file".into(),
        ));
    }
    let mut options = fs::OpenOptions::new();
    options.read(true).write(true);
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    options.custom_flags(O_NOFOLLOW_FLAG);
    let file = options
        .open(path)
        .map_err(|error| Error::Io(error.to_string()))?;
    let opened = file
        .metadata()
        .map_err(|error| Error::Io(error.to_string()))?;
    let after = fs::symlink_metadata(path).map_err(|error| Error::Io(error.to_string()))?;
    if after.file_type().is_symlink()
        || !after.is_file()
        || !opened.is_file()
        || !same_opened_file(&before, &opened)
        || !same_opened_file(&opened, &after)
        || before.len() != opened.len()
        || opened.len() != after.len()
    {
        return Err(Error::Decode(
            "recorder root lock changed before open".into(),
        ));
    }
    Ok(file)
}

#[cfg(unix)]
fn same_opened_file(opened: &fs::Metadata, linked: &fs::Metadata) -> bool {
    opened.dev() == linked.dev() && opened.ino() == linked.ino()
}

#[cfg(not(unix))]
fn same_opened_file(opened: &fs::Metadata, linked: &fs::Metadata) -> bool {
    opened.file_type() == linked.file_type()
        && opened.len() == linked.len()
        && opened.modified().ok() == linked.modified().ok()
}

fn current_recorder_layout(root: &Path) -> Result<bool> {
    let metadata = match fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(Error::Io(error.to_string())),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::Decode(
            "recorder root must be a real directory".into(),
        ));
    }
    let entries = fs::read_dir(root)
        .map_err(|error| Error::Io(error.to_string()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| Error::Io(error.to_string()))?;
    if entries.is_empty()
        || entries
            .iter()
            .all(|entry| entry.file_name() == ".recorder.lock")
    {
        return Ok(false);
    }
    validate_current_recorder_layout(root)?;
    Ok(true)
}

fn noncurrent_recorder_layout_error() -> Error {
    Error::Decode("recorder directory is not a current durable layout".into())
}

fn validate_current_recorder_layout(root: &Path) -> Result<()> {
    for name in [
        ".recorder.lock",
        "configuration.rec",
        "recorded-head.rec",
        "recorder.wal",
    ] {
        let metadata = fs::symlink_metadata(root.join(name)).ok();
        if !metadata
            .is_some_and(|metadata| !metadata.file_type().is_symlink() && metadata.is_file())
        {
            return Err(noncurrent_recorder_layout_error());
        }
    }
    Ok(())
}

fn read_preflight_intent(root: &Path, name: &str, maximum: usize) -> Result<Option<Vec<u8>>> {
    let path = root.join(name);
    match read_regular_file_bounded(&path, maximum, name) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(Error::Decode(message)) if message == format!("recorder is missing {name}") => Ok(None),
        Err(error) => Err(error),
    }
}

fn validate_preflight_configuration(
    configuration: &ConfigurationState,
    config_id: ConfigId,
    membership: &Membership,
) -> Result<()> {
    if configuration.config_id != config_id
        || configuration.config_digest != membership.digest()
        || configuration.membership.as_ref() != Some(membership)
    {
        return Err(Error::Decode(
            "recorder configuration does not exactly match expected membership".into(),
        ));
    }
    Ok(())
}

fn validate_existing_snapshots(
    root: &Path,
    snapshots: &[DurableSlotSnapshot],
    cluster_id: &str,
    epoch: Epoch,
    configuration: &ConfigurationState,
) -> Result<()> {
    let no_inline_commands = HashMap::new();
    for snapshot in snapshots {
        let state = decode_recorder_state(&snapshot.bytes)?;
        if state.slot() != snapshot.slot
            || state.cluster_id != cluster_id
            || state.epoch != epoch
            || state.config_id != configuration.config_id
            || state.config_digest != configuration.config_digest
        {
            return Err(Error::Decode(
                "durable recorder snapshot identity mismatch".into(),
            ));
        }
        for value in recorder_state_values(&state) {
            validate_existing_value(
                root,
                cluster_id,
                snapshot.slot,
                epoch,
                configuration.config_id,
                value,
                &no_inline_commands,
            )?;
        }
    }
    Ok(())
}

fn validate_recoverable_transition_intent(
    root: &Path,
    bytes: &[u8],
    cluster_id: &str,
    epoch: Epoch,
    config_id: ConfigId,
    membership: &Membership,
) -> Result<()> {
    let (slot, slot_bytes, configuration_bytes) = decode_transition_intent(bytes)?;
    let configuration = decode_configuration_state(&configuration_bytes)?;
    validate_preflight_configuration(&configuration, config_id, membership)?;
    let state = decode_recorder_state(&slot_bytes)?;
    if state.slot() != slot
        || state.cluster_id != cluster_id
        || state.epoch != epoch
        || state.config_id != configuration.config_id
        || state.config_digest != configuration.config_digest
    {
        return Err(Error::Decode(
            "recorder transition intent state identity mismatch".into(),
        ));
    }
    let no_inline_commands = HashMap::new();
    for value in recorder_state_values(&state) {
        validate_existing_value(
            root,
            cluster_id,
            slot,
            epoch,
            configuration.config_id,
            value,
            &no_inline_commands,
        )?;
    }
    Ok(())
}

fn validate_recoverable_configuration_head_intent(
    root: &Path,
    bytes: &[u8],
    cluster_id: &str,
    epoch: Epoch,
    config_id: ConfigId,
    membership: &Membership,
) -> Result<()> {
    let (configuration_bytes, head_bytes) = decode_configuration_head_intent(bytes)?;
    let configuration = decode_configuration_state(configuration_bytes)?;
    validate_preflight_configuration(&configuration, config_id, membership)?;
    let (_, snapshots, _) = decode_recorded_head(head_bytes, cluster_id, epoch, &configuration)?;
    validate_existing_snapshots(root, &snapshots, cluster_id, epoch, &configuration)
}

fn validate_empty_recovery_wal(root: &Path) -> Result<()> {
    let path = root.join("recorder.wal");
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(Error::Io(error.to_string())),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::Decode(
            "recorder WAL must be a regular file during recovery".into(),
        ));
    }
    if metadata.len() != 0 {
        return Err(Error::Decode(
            "recorder recovery intent requires an empty WAL".into(),
        ));
    }
    Ok(())
}

fn validate_existing_wal(
    root: &Path,
    bytes: &[u8],
    cluster_id: &str,
    epoch: Epoch,
    initial_configuration: &ConfigurationState,
    initial_head: &RecordedHeadProvenance,
    checkpoint: WalCheckpoint,
) -> Result<bool> {
    let mut commands = HashMap::new();
    let mut next_sequence = checkpoint
        .through_sequence
        .checked_add(1)
        .ok_or_else(|| Error::Decode("recorder WAL sequence exhausted".into()))?;
    let mut last_digest = LogHash::ZERO;
    let mut configuration = initial_configuration.clone();
    let mut head = initial_head.clone();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let Some((frame, end)) = decode_wal_frame(bytes, offset)? else {
            return Ok(true);
        };
        if frame.generation < checkpoint.generation {
            offset = end;
            continue;
        }
        if frame.generation != checkpoint.generation
            || frame.sequence != next_sequence
            || frame.prev_digest != last_digest
        {
            return Err(Error::Decode(
                "recorder WAL sequence or digest chain mismatch".into(),
            ));
        }
        let state = decode_recorder_state(&frame.slot_bytes)?;
        let next_configuration = decode_configuration_state(&frame.configuration_bytes)?;
        if state.slot() != frame.slot
            || state.cluster_id != cluster_id
            || state.epoch != epoch
            || state.config_id != next_configuration.config_id
            || state.config_digest != next_configuration.config_digest
            || configuration_structure_changed(&configuration, &next_configuration)
        {
            return Err(Error::Decode("recorder WAL state identity mismatch".into()));
        }
        if let Some((hash, command)) = &frame.command {
            if command.hash() != *hash {
                return Err(Error::Decode(
                    "recorder WAL inline command hash mismatch".into(),
                ));
            }
            upsert_wal_command(&mut commands, *hash, command)?;
        }
        for value in recorder_state_values(&state) {
            validate_existing_value(
                root,
                cluster_id,
                frame.slot,
                epoch,
                next_configuration.config_id,
                value,
                &commands,
            )?;
        }
        let expected_head = if next_configuration.max_accepted_or_decided_slot == Some(frame.slot)
            && recorder_state_values(&state).next().is_some()
        {
            RecordedHeadProvenance::SlotBacked { slot: frame.slot }
        } else {
            head.clone()
        };
        if frame.head != expected_head {
            return Err(Error::Decode("recorder WAL head mismatch".into()));
        }
        next_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| Error::Decode("recorder WAL sequence exhausted".into()))?;
        last_digest = frame.digest;
        configuration = next_configuration;
        head = frame.head;
        offset = end;
    }
    Ok(false)
}

fn validate_existing_value(
    root: &Path,
    cluster_id: &str,
    slot: Slot,
    epoch: Epoch,
    config_id: ConfigId,
    value: &AcceptedValue,
    inline_commands: &HashMap<LogHash, StoredCommand>,
) -> Result<()> {
    let cached_command;
    let command = match inline_commands.get(&value.command_hash) {
        Some(command) => command,
        None => {
            cached_command = read_existing_command_cache(root, value.command_hash)?;
            &cached_command
        }
    };
    if AcceptedValue::from_command(cluster_id, slot, epoch, config_id, value.prev_hash, command)
        != *value
    {
        return Err(Error::Decode("recorder WAL value mismatch".into()));
    }
    Ok(())
}

fn read_existing_command_cache(root: &Path, command_hash: LogHash) -> Result<StoredCommand> {
    let path = root.join(format!("command-{}.cmd", command_hash.to_hex()));
    let bytes =
        match read_regular_file_bounded(&path, MAX_COMMAND_CACHE_BYTES, "command cache entry") {
            Err(Error::Decode(message)) if message == "recorder is missing command cache entry" => {
                return Err(Error::CommandUnavailable)
            }
            result => result?,
        };
    let command = decode_stored_command(&bytes)?;
    if command.hash() != command_hash {
        return Err(Error::CommandHashMismatch);
    }
    Ok(command)
}

fn encode_head_provenance(out: &mut Vec<u8>, head: &RecordedHeadProvenance) {
    match head {
        RecordedHeadProvenance::Empty => out.push(0),
        RecordedHeadProvenance::SlotBacked { slot } => {
            out.push(1);
            put_u64(out, *slot);
        }
        RecordedHeadProvenance::CheckpointBacked {
            stop_slot,
            prefix_hash,
            recovered_tip,
            recovered_hash,
        } => {
            out.push(2);
            put_u64(out, *stop_slot);
            out.extend_from_slice(prefix_hash.as_bytes());
            put_u64(out, *recovered_tip);
            out.extend_from_slice(recovered_hash.as_bytes());
        }
    }
}

fn decode_head_provenance(bytes: &[u8], cursor: &mut usize) -> Result<RecordedHeadProvenance> {
    match read_u8(bytes, cursor)? {
        0 => Ok(RecordedHeadProvenance::Empty),
        1 => Ok(RecordedHeadProvenance::SlotBacked {
            slot: read_u64(bytes, cursor)?,
        }),
        2 => Ok(RecordedHeadProvenance::CheckpointBacked {
            stop_slot: read_u64(bytes, cursor)?,
            prefix_hash: read_hash(bytes, cursor)?,
            recovered_tip: read_u64(bytes, cursor)?,
            recovered_hash: read_hash(bytes, cursor)?,
        }),
        _ => Err(Error::Decode(
            "recorder WAL head provenance is invalid".into(),
        )),
    }
}

fn upsert_wal_command(
    commands: &mut HashMap<LogHash, StoredCommand>,
    hash: LogHash,
    command: &StoredCommand,
) -> Result<()> {
    match commands.entry(hash) {
        hash_map::Entry::Occupied(existing) if existing.get() != command => {
            Err(Error::CommandHashMismatch)
        }
        hash_map::Entry::Occupied(_) => Ok(()),
        hash_map::Entry::Vacant(vacant) => {
            vacant.insert(command.clone());
            Ok(())
        }
    }
}

fn encode_recorded_head(
    cluster_id: &str,
    epoch: Epoch,
    configuration: &ConfigurationState,
    provenance: &RecordedHeadProvenance,
    recent_slots: &[DurableSlotSnapshot],
    wal_checkpoint: WalCheckpoint,
) -> Result<Vec<u8>> {
    if recent_slots.len() > 2 {
        return Err(Error::Io(
            "recorder manifest can retain at most two slot snapshots".into(),
        ));
    }
    let mut encoded = Vec::new();
    encoded.extend_from_slice(RECORDED_HEAD_MAGIC);
    put_u16(&mut encoded, RECORDED_HEAD_VERSION);
    put_bytes(&mut encoded, cluster_id.as_bytes())?;
    put_u64(&mut encoded, epoch);
    put_u64(&mut encoded, configuration.config_id);
    encoded.extend_from_slice(configuration.config_digest.as_bytes());
    match provenance {
        RecordedHeadProvenance::Empty => encoded.push(0),
        RecordedHeadProvenance::SlotBacked { slot } => {
            encoded.push(1);
            put_u64(&mut encoded, *slot);
        }
        RecordedHeadProvenance::CheckpointBacked {
            stop_slot,
            prefix_hash,
            recovered_tip,
            recovered_hash,
        } => {
            encoded.push(2);
            put_u64(&mut encoded, *stop_slot);
            encoded.extend_from_slice(prefix_hash.as_bytes());
            put_u64(&mut encoded, *recovered_tip);
            encoded.extend_from_slice(recovered_hash.as_bytes());
        }
    }
    put_u64(&mut encoded, wal_checkpoint.generation);
    put_u64(&mut encoded, wal_checkpoint.through_sequence);
    put_u16(&mut encoded, recent_slots.len() as u16);
    for snapshot in recent_slots {
        put_u64(&mut encoded, snapshot.slot);
        put_bytes(&mut encoded, &snapshot.bytes)?;
    }
    let digest = LogHash::digest(&[&encoded]);
    encoded.extend_from_slice(digest.as_bytes());
    Ok(encoded)
}

fn decode_recorded_head(
    bytes: &[u8],
    expected_cluster_id: &str,
    expected_epoch: Epoch,
    configuration: &ConfigurationState,
) -> Result<(
    RecordedHeadProvenance,
    Vec<DurableSlotSnapshot>,
    WalCheckpoint,
)> {
    if bytes.get(..RECORDED_HEAD_MAGIC.len()) != Some(RECORDED_HEAD_MAGIC) {
        return Err(Error::Decode("invalid recorder durable head magic".into()));
    }
    let mut version_cursor = RECORDED_HEAD_MAGIC.len();
    if read_u16(bytes, &mut version_cursor)? != RECORDED_HEAD_VERSION {
        return Err(noncurrent_recorder_layout_error());
    }
    if bytes.len() < 32 {
        return Err(Error::Decode("truncated recorder durable head".into()));
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if LogHash::digest(&[body]).as_bytes() != digest {
        return Err(Error::Decode(
            "recorder durable head digest mismatch".into(),
        ));
    }
    let mut cursor = 0;
    cursor += RECORDED_HEAD_MAGIC.len();
    let _version = read_u16(body, &mut cursor)?;
    let cluster_id = String::from_utf8(read_bytes(body, &mut cursor)?)
        .map_err(|error| Error::Decode(error.to_string()))?;
    let epoch = read_u64(body, &mut cursor)?;
    let config_id = read_u64(body, &mut cursor)?;
    let config_digest = read_hash(body, &mut cursor)?;
    if cluster_id != expected_cluster_id
        || epoch != expected_epoch
        || config_id != configuration.config_id
        || config_digest != configuration.config_digest
    {
        return Err(Error::Decode(
            "recorder durable head identity mismatch".into(),
        ));
    }
    let provenance = match read_u8(body, &mut cursor)? {
        0 => RecordedHeadProvenance::Empty,
        1 => RecordedHeadProvenance::SlotBacked {
            slot: read_u64(body, &mut cursor)?,
        },
        2 => RecordedHeadProvenance::CheckpointBacked {
            stop_slot: read_u64(body, &mut cursor)?,
            prefix_hash: read_hash(body, &mut cursor)?,
            recovered_tip: read_u64(body, &mut cursor)?,
            recovered_hash: read_hash(body, &mut cursor)?,
        },
        value => {
            return Err(Error::Decode(format!(
                "invalid recorder durable head provenance {value}"
            )));
        }
    };
    let wal_checkpoint = WalCheckpoint {
        generation: read_u64(body, &mut cursor)?,
        through_sequence: read_u64(body, &mut cursor)?,
    };
    if wal_checkpoint.generation == 0 {
        return Err(Error::Decode(
            "recorder durable head has zero WAL generation".into(),
        ));
    }
    let recent_count = usize::from(read_u16(body, &mut cursor)?);
    if recent_count > 2 {
        return Err(Error::Decode(
            "recorder manifest contains too many slot snapshots".into(),
        ));
    }
    let mut recent_slots = Vec::with_capacity(recent_count);
    for _ in 0..recent_count {
        let slot = read_u64(body, &mut cursor)?;
        if recent_slots
            .iter()
            .any(|snapshot: &DurableSlotSnapshot| snapshot.slot == slot)
        {
            return Err(Error::Decode(
                "recorder manifest contains duplicate slot snapshots".into(),
            ));
        }
        recent_slots.push(DurableSlotSnapshot {
            slot,
            bytes: read_bytes(body, &mut cursor)?,
        });
    }
    if cursor != body.len() {
        return Err(Error::Decode("trailing recorder durable head bytes".into()));
    }
    Ok((provenance, recent_slots, wal_checkpoint))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    atomic_replace(path, bytes)?;
    let parent = path
        .parent()
        .ok_or_else(|| Error::Io("atomic write path has no parent".into()))?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|err| Error::Io(err.to_string()))?;
    #[cfg(test)]
    record_directory_sync();
    Ok(())
}

fn atomic_replace(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::Io("atomic write path has no parent".into()))?;
    fs::create_dir_all(parent).map_err(|err| Error::Io(err.to_string()))?;
    let file_name = path
        .file_name()
        .ok_or_else(|| Error::Io("atomic write path has no file name".into()))?
        .to_string_lossy();
    let (temp_path, mut file) = loop {
        let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temp_path = parent.join(format!(".{file_name}.tmp-{}-{counter}", std::process::id()));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
        {
            Ok(file) => break (temp_path, file),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(Error::Io(err.to_string())),
        }
    };
    let result = (|| -> io::Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        #[cfg(test)]
        record_file_sync();
        drop(file);
        fs::rename(&temp_path, path)?;
        Ok(())
    })();
    if let Err(err) = result {
        let _ = fs::remove_file(&temp_path);
        return Err(Error::Io(err.to_string()));
    }
    Ok(())
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u128(out: &mut Vec<u8>, value: u128) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u16::try_from(value.len())
        .map_err(|_| Error::Decode("recorder string is too long".into()))?;
    put_u16(out, len);
    out.extend_from_slice(value);
    Ok(())
}

fn put_blob(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u64::try_from(value.len())
        .map_err(|_| Error::Decode("recorder blob is too long".into()))?;
    put_u64(out, len);
    out.extend_from_slice(value);
    Ok(())
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8> {
    let value = *bytes
        .get(*cursor)
        .ok_or_else(|| Error::Decode("short recorder u8".into()))?;
    *cursor += 1;
    Ok(value)
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16> {
    let end = cursor
        .checked_add(2)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder u16".into()))?;
    *cursor = end;
    Ok(u16::from_be_bytes(slice.try_into().expect("u16 slice")))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder u64".into()))?;
    *cursor = end;
    Ok(u64::from_be_bytes(slice.try_into().expect("u64 slice")))
}

fn read_u128(bytes: &[u8], cursor: &mut usize) -> Result<u128> {
    let end = cursor
        .checked_add(16)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder u128".into()))?;
    *cursor = end;
    Ok(u128::from_be_bytes(slice.try_into().expect("u128 slice")))
}

fn read_hash(bytes: &[u8], cursor: &mut usize) -> Result<LogHash> {
    let end = cursor
        .checked_add(32)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder hash".into()))?;
    *cursor = end;
    let mut out = [0; 32];
    out.copy_from_slice(slice);
    Ok(LogHash::from_bytes(out))
}

fn read_bytes(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = read_u16(bytes, cursor)? as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::Decode("recorder cursor overflow".into()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder bytes".into()))?;
    *cursor = end;
    Ok(slice.to_vec())
}

fn read_blob(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = usize::try_from(read_u64(bytes, cursor)?)
        .map_err(|_| Error::Decode("recorder blob length overflow".into()))?;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::Decode("recorder blob length overflow".into()))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Decode("short recorder blob".into()))?
        .to_vec();
    *cursor = end;
    Ok(value)
}

#[derive(Debug)]
pub struct SingleNodeConsensus {
    cluster_id: String,
    epoch: Epoch,
    config_id: ConfigId,
    state: Mutex<SingleNodeState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SingleNodeState {
    next_index: LogIndex,
    last_hash: LogHash,
}

impl SingleNodeConsensus {
    pub fn new(cluster_id: impl Into<String>, epoch: Epoch, config_id: ConfigId) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            epoch,
            config_id,
            state: Mutex::new(SingleNodeState {
                next_index: 1,
                last_hash: LogHash::ZERO,
            }),
        }
    }
}

impl Consensus for SingleNodeConsensus {
    fn propose(&self, command: Command) -> Result<LogEntry> {
        let mut state = self.state.lock().map_err(|_| Error::ProposeFailed)?;
        let command = stored_command(command)?;
        let prev_hash = state.last_hash;
        let hash = LogEntry::calculate_hash(
            &self.cluster_id,
            state.next_index,
            self.epoch,
            self.config_id,
            command.entry_type,
            prev_hash,
            &command.payload,
        );
        let entry = LogEntry {
            cluster_id: self.cluster_id.clone(),
            epoch: self.epoch,
            config_id: self.config_id,
            index: state.next_index,
            entry_type: command.entry_type,
            payload: command.payload,
            prev_hash,
            hash,
        };
        state.next_index += 1;
        state.last_hash = hash;
        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        command_file_reads, decode_configuration_state, decode_recorder_state, decode_wal_frame,
        encode_stored_command, encode_wal_frame, last_file_sync_kind, reset_command_file_reads,
        reset_sync_counts, sync_counts, sync_wal_append, sync_wal_metadata, upsert_wal_command,
        AcceptedValue, CertifiedDecisionInspection, ConfigChange, ConfigurationState, Consensus,
        ControlDispatch, ControlJob, ControlJobCancellation, ControlWorker, DecisionInspection,
        DecisionProof, DriveOutcome, Error, FileSyncKind, Membership, PrioritySource, Proposal,
        ProposalPriority, ProposerProgress, ReadFenceObservation, ReadFenceRequest,
        ReadFenceSlotState, RecordRequest, RecordSummary, RecordedHeadProvenance,
        RecorderFileStore, RecorderPreflight, RecorderRequest, RecorderRpc, RecorderSlotState,
        RecorderSummary, RejectReason, SealFaultPoint, SingleNodeConsensus, ThreeNodeConsensus,
    };
    use proptest::prelude::*;
    use rhiza_core::{Command, CommandKind, EntryType, LogHash, StoredCommand};
    use std::{
        collections::{BTreeMap, BTreeSet, HashMap, HashSet},
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc, Arc, Condvar, Mutex,
        },
        thread,
        time::{Duration, Instant},
    };

    fn directory_files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        fn visit(base: &Path, current: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
            if !current.exists() {
                return;
            }
            for entry in std::fs::read_dir(current).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    visit(base, &entry.path(), files);
                } else {
                    files.insert(
                        entry.path().strip_prefix(base).unwrap().to_path_buf(),
                        std::fs::read(entry.path()).unwrap(),
                    );
                }
            }
        }
        let mut files = BTreeMap::new();
        visit(root, root, &mut files);
        files
    }

    fn cache_backed_recorder_for_preflight(
        root: &Path,
        membership: Membership,
    ) -> (LogHash, StoredCommand) {
        let store =
            RecorderFileStore::new_with_membership(root, "n1", "cluster", 1, 1, membership.clone())
                .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"preflight-cache".to_vec());
        store
            .store_command(command.hash(), command.clone())
            .unwrap();
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        store
            .record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                command: None,
            })
            .unwrap();
        (command.hash(), command)
    }

    fn record_requests(consensus: &ThreeNodeConsensus, slot: u64) -> Vec<RecordRequest> {
        let proposal = Proposal::new(
            ProposalPriority::MAX,
            "n1",
            slot,
            AcceptedValue {
                command_hash: LogHash::ZERO,
                prev_hash: LogHash::ZERO,
                entry_hash: LogHash::ZERO,
            },
        );
        consensus
            .membership()
            .members()
            .iter()
            .map(|_| RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: consensus.membership().digest(),
                slot,
                step: 4,
                proposal: proposal.clone(),
                command: None,
            })
            .collect()
    }

    #[test]
    fn record_request_requires_the_current_command_field() {
        let request = RecordRequest {
            cluster_id: "cluster".into(),
            epoch: 1,
            config_id: 1,
            config_digest: LogHash::ZERO,
            slot: 1,
            step: 4,
            proposal: Proposal::new(
                ProposalPriority::MAX,
                "n1",
                1,
                AcceptedValue {
                    command_hash: LogHash::ZERO,
                    prev_hash: LogHash::ZERO,
                    entry_hash: LogHash::ZERO,
                },
            ),
            command: None,
        };
        let mut encoded = serde_json::to_value(request).unwrap();
        encoded.as_object_mut().unwrap().remove("command");

        assert!(serde_json::from_value::<RecordRequest>(encoded).is_err());
    }

    fn record_summary(recorder_id: &str, request: RecordRequest) -> RecordSummary {
        RecordSummary {
            recorder_id: recorder_id.into(),
            slot: request.slot,
            config_id: request.config_id,
            config_digest: request.config_digest,
            step: request.step,
            first_current: Some(request.proposal),
            aggregate_prior: None,
            decided: None,
        }
    }

    struct ThreadRecordingRecorder {
        recorder_id: &'static str,
        threads: Arc<Mutex<HashSet<thread::ThreadId>>>,
    }

    impl RecorderRpc for ThreadRecordingRecorder {
        fn record(&self, request: RecordRequest) -> super::Result<RecordSummary> {
            self.threads.lock().unwrap().insert(thread::current().id());
            Ok(record_summary(self.recorder_id, request))
        }
    }

    struct ThreadRecordingControlRecorder {
        threads: Arc<Mutex<HashSet<thread::ThreadId>>>,
    }

    impl RecorderRpc for ThreadRecordingControlRecorder {
        fn inspect_decision_proof(
            &self,
            _slot: u64,
        ) -> super::Result<Option<super::DecisionProof>> {
            self.threads.lock().unwrap().insert(thread::current().id());
            Ok(None)
        }
    }

    struct BlockingControlRecorder {
        recorder_id: &'static str,
        started: mpsc::SyncSender<u64>,
        release_first: Mutex<mpsc::Receiver<()>>,
    }

    struct BlockingInspectionReadFenceRecorder {
        recorder_id: &'static str,
        block_inspection: bool,
        started: mpsc::SyncSender<&'static str>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    struct BlockingCommandStoreRecorder {
        started: mpsc::SyncSender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    struct SuccessfulCommandStoreRecorder;

    impl RecorderRpc for SuccessfulCommandStoreRecorder {
        fn store_command_for(
            &self,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
            _command: StoredCommand,
        ) -> super::Result<()> {
            Ok(())
        }
    }

    struct FailingCommandStoreRecorder;

    impl RecorderRpc for FailingCommandStoreRecorder {
        fn store_command_for(
            &self,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
            _command: StoredCommand,
        ) -> super::Result<()> {
            Err(Error::ProposeFailed)
        }
    }

    impl RecorderRpc for BlockingCommandStoreRecorder {
        fn store_command_for(
            &self,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
            _command: StoredCommand,
        ) -> super::Result<()> {
            self.started.send(()).unwrap();
            let (released, condition) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
            Ok(())
        }
    }

    impl RecorderRpc for BlockingControlRecorder {
        fn record(&self, request: RecordRequest) -> super::Result<RecordSummary> {
            Ok(record_summary(self.recorder_id, request))
        }

        fn inspect_decision_proof(&self, slot: u64) -> super::Result<Option<super::DecisionProof>> {
            self.started.send(slot).unwrap();
            if slot == 1 {
                self.release_first.lock().unwrap().recv().unwrap();
            }
            Ok(None)
        }
    }

    #[test]
    fn control_worker_reclaims_a_cancelled_queued_job_for_the_next_operation() {
        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let recorder = Arc::new(BlockingControlRecorder {
            recorder_id: "n1",
            started: started_tx,
            release_first: Mutex::new(release_rx),
        });
        let worker = ControlWorker::spawn(recorder).unwrap();
        let (result_tx, result_rx) = mpsc::sync_channel(3);

        assert!(matches!(
            worker.dispatch(ControlJob::InspectProof {
                index: 1,
                slot: 1,
                result: result_tx.clone(),
            }),
            ControlDispatch::Accepted
        ));
        assert_eq!(started_rx.recv().unwrap(), 1);

        let cancellation = ControlJobCancellation::new();
        assert!(matches!(
            worker.dispatch_cancellable(
                ControlJob::InspectProof {
                    index: 2,
                    slot: 2,
                    result: result_tx.clone(),
                },
                cancellation.token(),
            ),
            ControlDispatch::Accepted
        ));
        drop(cancellation);

        assert!(matches!(
            worker.dispatch(ControlJob::InspectProof {
                index: 3,
                slot: 3,
                result: result_tx,
            }),
            ControlDispatch::Accepted
        ));

        release_tx.send(()).unwrap();
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 3);
        assert!(started_rx.try_recv().is_err());
        assert_eq!(result_rx.recv().unwrap().0, 1);
        assert_eq!(result_rx.recv().unwrap().0, 3);
    }

    impl RecorderRpc for BlockingInspectionReadFenceRecorder {
        fn inspect_record_summary(&self, _slot: u64) -> super::Result<Option<RecordSummary>> {
            if self.block_inspection {
                self.started.send(self.recorder_id).unwrap();
                let (released, condition) = &*self.release;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }
            Ok(None)
        }

        fn supports_context_read_fence(&self) -> bool {
            true
        }

        fn observe_read_fence(
            &self,
            request: ReadFenceRequest,
        ) -> super::Result<ReadFenceObservation> {
            Ok(ReadFenceObservation {
                recorder_id: self.recorder_id.into(),
                cluster_id: request.cluster_id,
                epoch: request.epoch,
                config_id: request.config_id,
                config_digest: request.config_digest,
                slot: request.slot,
                max_head: None,
                slot_state: ReadFenceSlotState::Empty,
            })
        }
    }

    struct BlockingRecorder {
        recorder_id: &'static str,
        started: mpsc::SyncSender<u64>,
        release_first: Mutex<mpsc::Receiver<()>>,
    }

    impl RecorderRpc for BlockingRecorder {
        fn record(&self, request: RecordRequest) -> super::Result<RecordSummary> {
            self.started.send(request.slot).unwrap();
            if request.slot == 1 {
                self.release_first.lock().unwrap().recv().unwrap();
            }
            Ok(record_summary(self.recorder_id, request))
        }
    }

    struct SlotRecorder {
        recorder_id: &'static str,
        reject_slot: Option<u64>,
        observed: Option<mpsc::SyncSender<u64>>,
    }

    impl RecorderRpc for SlotRecorder {
        fn record(&self, request: RecordRequest) -> super::Result<RecordSummary> {
            if let Some(observed) = &self.observed {
                observed.send(request.slot).unwrap();
            }
            if self.reject_slot == Some(request.slot) {
                Err(Error::Rejected(RejectReason::InvalidRequest))
            } else {
                Ok(record_summary(self.recorder_id, request))
            }
        }
    }

    struct RejectFromSlotRecorder {
        recorder_id: &'static str,
        reject_from: u64,
    }

    impl RecorderRpc for RejectFromSlotRecorder {
        fn record(&self, request: RecordRequest) -> super::Result<RecordSummary> {
            if request.slot >= self.reject_from {
                Err(Error::Rejected(RejectReason::InvalidRequest))
            } else {
                Ok(record_summary(self.recorder_id, request))
            }
        }
    }

    struct PanickingRecorder;

    impl RecorderRpc for PanickingRecorder {
        fn record(&self, _request: RecordRequest) -> super::Result<RecordSummary> {
            panic!("injected recorder panic")
        }
    }

    struct FailingFromSlotRecorder {
        recorder_id: &'static str,
        fail_from: u64,
    }

    impl RecorderRpc for FailingFromSlotRecorder {
        fn record(&self, request: RecordRequest) -> super::Result<RecordSummary> {
            if request.slot >= self.fail_from {
                Err(Error::ProposeFailed)
            } else {
                Ok(record_summary(self.recorder_id, request))
            }
        }
    }

    struct AlwaysIoRecorder;

    impl RecorderRpc for AlwaysIoRecorder {
        fn record(&self, _request: RecordRequest) -> super::Result<RecordSummary> {
            Err(Error::Io("injected recorder unavailable".into()))
        }
    }

    struct MissingCommandRecorder {
        observed: mpsc::SyncSender<()>,
    }

    impl RecorderRpc for MissingCommandRecorder {
        fn fetch_command_for(
            &self,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            self.observed.send(()).unwrap();
            Ok(None)
        }
    }

    struct BlockingCommandRecorder {
        started: mpsc::SyncSender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        command: StoredCommand,
    }

    struct AvailableCommandRecorder {
        command: StoredCommand,
    }

    impl RecorderRpc for AvailableCommandRecorder {
        fn fetch_command_for(
            &self,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            Ok(Some(self.command.clone()))
        }
    }

    struct FailingCommandFetchRecorder;

    impl RecorderRpc for FailingCommandFetchRecorder {
        fn fetch_command_for(
            &self,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            Err(Error::ProposeFailed)
        }
    }

    impl RecorderRpc for BlockingCommandRecorder {
        fn fetch_command_for(
            &self,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> super::Result<Option<StoredCommand>> {
            self.started.send(()).unwrap();
            let _ = self.release.lock().unwrap().recv();
            Ok(Some(self.command.clone()))
        }
    }

    struct FailingPrioritySource;

    impl PrioritySource for FailingPrioritySource {
        fn sample(
            &self,
            _slot: u64,
            _round: u64,
            _proposer_id: &str,
            _recorder_id: &str,
        ) -> super::Result<ProposalPriority> {
            Err(Error::RandomnessUnavailable("unexpected sample".into()))
        }
    }

    #[derive(Default)]
    struct CountingPrioritySource {
        samples: AtomicUsize,
    }

    impl PrioritySource for CountingPrioritySource {
        fn sample(
            &self,
            _slot: u64,
            _round: u64,
            _proposer_id: &str,
            _recorder_id: &str,
        ) -> super::Result<ProposalPriority> {
            let sample = self.samples.fetch_add(1, Ordering::Relaxed) + 1;
            Ok(ProposalPriority::from_u64(sample as u64))
        }
    }

    struct CatchUpRecorder {
        recorder_id: &'static str,
        step: u64,
    }

    impl RecorderRpc for CatchUpRecorder {
        fn record(&self, request: RecordRequest) -> super::Result<RecordSummary> {
            let mut summary = record_summary(self.recorder_id, request);
            summary.step = self.step;
            Ok(summary)
        }
    }

    #[test]
    fn single_node_consensus_commits_contiguous_hash_chain() {
        let consensus = SingleNodeConsensus::new("cluster-a", 1, 1);
        let first = consensus
            .propose(Command::new(CommandKind::Deterministic, b"first".to_vec()))
            .unwrap();
        let second = consensus
            .propose(Command::new(CommandKind::Deterministic, b"second".to_vec()))
            .unwrap();

        assert_eq!(first.index, 1);
        assert_eq!(first.prev_hash, LogHash::ZERO);
        assert_eq!(first.hash, first.recompute_hash());
        assert_eq!(second.index, 2);
        assert_eq!(second.prev_hash, first.hash);
        assert_eq!(second.hash, second.recompute_hash());
    }

    #[test]
    fn cached_phase_zero_priorities_do_not_resample_randomness() {
        let consensus = ThreeNodeConsensus::from_recorders_with_ids(
            "cluster",
            "writer",
            1,
            1,
            ["n1", "n2", "n3"]
                .into_iter()
                .map(|recorder_id| {
                    (
                        recorder_id.into(),
                        Box::new(SlotRecorder {
                            recorder_id,
                            reject_slot: None,
                            observed: None,
                        }) as Box<dyn RecorderRpc>,
                    )
                })
                .collect(),
        )
        .unwrap()
        .with_priority_source(Arc::new(FailingPrioritySource));
        let command = StoredCommand::new(EntryType::Command, b"cached-priority".to_vec());
        let value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
        let mut progress =
            ProposerProgress::new(1, Proposal::new(ProposalPriority::MAX, "writer", 1, value))
                .with_command(command);
        progress.step = 0;
        for recorder_id in ["n1", "n2", "n3"] {
            progress
                .phase_zero_priorities
                .insert((0, recorder_id.into()), ProposalPriority::from_u64(1));
        }

        let DriveOutcome::Progress(progress) = consensus.drive(progress).unwrap() else {
            panic!("phase zero quorum should advance progress");
        };
        assert!(progress.phase_zero_priorities.is_empty());
    }

    #[test]
    fn phase_zero_priorities_are_stable_only_for_pending_retries_in_the_current_round() {
        let source = Arc::new(CountingPrioritySource::default());
        let consensus = ThreeNodeConsensus::from_recorders_with_ids(
            "cluster",
            "writer",
            1,
            1,
            vec![
                (
                    "n1".into(),
                    Box::new(SlotRecorder {
                        recorder_id: "n1",
                        reject_slot: None,
                        observed: None,
                    }) as Box<dyn RecorderRpc>,
                ),
                ("n2".into(), Box::new(AlwaysIoRecorder)),
                ("n3".into(), Box::new(AlwaysIoRecorder)),
            ],
        )
        .unwrap()
        .with_priority_source(source.clone());
        let command = StoredCommand::new(EntryType::Command, b"bounded-priorities".to_vec());
        let value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
        let mut progress =
            ProposerProgress::new(1, Proposal::new(ProposalPriority::MAX, "writer", 1, value))
                .with_command(command);
        progress.step = 0;

        let DriveOutcome::Pending(mut progress) = consensus.drive(progress).unwrap() else {
            panic!("one recorder reply should leave progress pending");
        };
        assert_eq!(progress.phase_zero_priorities.len(), 3);
        assert_eq!(source.samples.load(Ordering::Relaxed), 3);

        let DriveOutcome::Pending(retry) = consensus.drive(progress.clone()).unwrap() else {
            panic!("same-round retry should remain pending");
        };
        assert_eq!(retry.phase_zero_priorities, progress.phase_zero_priorities);
        assert_eq!(source.samples.load(Ordering::Relaxed), 3);

        for round in 1..=64 {
            progress.step = round * 4;
            let DriveOutcome::Pending(next) = consensus.drive(progress).unwrap() else {
                panic!("one recorder reply should leave progress pending");
            };
            assert_eq!(next.phase_zero_priorities.len(), 3);
            assert!(next
                .phase_zero_priorities
                .keys()
                .all(|(cached_round, _)| *cached_round == round));
            progress = next;
        }
        assert_eq!(source.samples.load(Ordering::Relaxed), 3 * 65);
    }

    #[test]
    fn phase_zero_priorities_are_cleared_when_progress_catches_up() {
        let consensus = ThreeNodeConsensus::from_recorders_with_ids(
            "cluster",
            "writer",
            1,
            1,
            ["n1", "n2", "n3"]
                .into_iter()
                .map(|recorder_id| {
                    (
                        recorder_id.into(),
                        Box::new(CatchUpRecorder {
                            recorder_id,
                            step: 8,
                        }) as Box<dyn RecorderRpc>,
                    )
                })
                .collect(),
        )
        .unwrap()
        .with_priority_source(Arc::new(FailingPrioritySource));
        let command = StoredCommand::new(EntryType::Command, b"catch-up".to_vec());
        let value = AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, &command);
        let mut progress =
            ProposerProgress::new(1, Proposal::new(ProposalPriority::MAX, "writer", 1, value))
                .with_command(command);
        progress.step = 0;
        for recorder_id in ["n1", "n2", "n3"] {
            progress
                .phase_zero_priorities
                .insert((0, recorder_id.into()), ProposalPriority::from_u64(1));
        }

        let DriveOutcome::Progress(progress) = consensus.drive(progress).unwrap() else {
            panic!("higher recorder steps should catch progress up");
        };
        assert_eq!(progress.step, 8);
        assert!(progress.phase_zero_priorities.is_empty());
    }

    #[test]
    fn wal_command_cache_rejects_same_hash_with_different_payload() {
        let hash = LogHash::digest(&[b"forced-cache-key"]);
        let first = StoredCommand::new(EntryType::Command, b"first".to_vec());
        let second = StoredCommand::new(EntryType::Command, b"second".to_vec());
        let mut commands = HashMap::new();

        upsert_wal_command(&mut commands, hash, &first).unwrap();
        upsert_wal_command(&mut commands, hash, &first).unwrap();
        assert_eq!(
            upsert_wal_command(&mut commands, hash, &second),
            Err(Error::CommandHashMismatch)
        );
        assert_eq!(commands.len(), 1);
    }

    #[test]
    fn normal_record_uses_one_file_sync_and_no_directory_barrier() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"barrier-count".to_vec());
        store
            .store_command(command.hash(), command.clone())
            .unwrap();
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        reset_sync_counts();

        store
            .record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                command: None,
            })
            .unwrap();

        assert_eq!(sync_counts(), (1, 0));
        assert!(!root.path().join("slot-head.intent").exists());

        let inline = StoredCommand::new(EntryType::Command, b"inline-command".to_vec());
        let inline_value = AcceptedValue::from_command("cluster", 9, 1, 1, LogHash::ZERO, &inline);
        reset_sync_counts();
        store
            .record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 9,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 2, inline_value),
                command: Some(inline),
            })
            .unwrap();

        assert_eq!(sync_counts(), (1, 0));
    }

    #[test]
    fn wal_durable_command_store_adds_no_sync_and_survives_proof_checkpoint_recovery() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"proof-worker-command".to_vec());
        let command_hash = command.hash();
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        let proposal = Proposal::new(ProposalPriority::MAX, "writer", 1, value);
        let proof = DecisionProof::FastPath {
            cluster_id: "cluster".into(),
            slot: 8,
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            proposal: proposal.clone(),
            summaries: ["n1", "n2"]
                .into_iter()
                .map(|recorder_id| RecorderSummary {
                    recorder_id: recorder_id.into(),
                    slot: 8,
                    step: 4,
                    first_current: Some(proposal.clone()),
                    aggregate_prior: None,
                })
                .collect(),
        };
        let store = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        store
            .record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal,
                command: Some(command.clone()),
            })
            .unwrap();

        reset_sync_counts();
        RecorderRpc::store_command_for(
            &store,
            "cluster".into(),
            1,
            1,
            membership.digest(),
            command_hash,
            command.clone(),
        )
        .unwrap();
        assert_eq!(sync_counts(), (0, 0));
        assert!(!store.command_path(command_hash).exists());

        store
            .install_decision_proof_record(proof.clone(), &membership)
            .unwrap();
        drop(store);

        let reopened = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        assert_eq!(
            reopened.fetch_command(command_hash).unwrap(),
            Some(command.clone())
        );
        assert_eq!(reopened.load(8).unwrap().decision_proof(), Some(&proof));
        reopened.checkpoint_wal_unlocked().unwrap();
        assert!(reopened.command_path(command_hash).exists());
        drop(reopened);

        let checkpointed =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        assert_eq!(
            checkpointed.fetch_command(command_hash).unwrap(),
            Some(command)
        );
        assert_eq!(checkpointed.load(8).unwrap().decision_proof(), Some(&proof));
    }

    #[test]
    fn duplicate_command_file_store_keeps_the_root_directory_barrier() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"durable-command".to_vec());
        store
            .store_command(command.hash(), command.clone())
            .unwrap();
        reset_sync_counts();

        store.store_command(command.hash(), command).unwrap();

        assert_eq!(sync_counts(), (0, 1));
    }

    #[test]
    fn direct_store_command_rejects_a_claimed_hash_without_creating_a_file() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"mismatched-hash".to_vec());
        let claimed_hash = LogHash::digest(&[b"claimed-hash"]);

        assert_eq!(
            store.apply(RecorderRequest::StoreCommand {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                command_hash: claimed_hash,
                command,
            }),
            Err(Error::CommandHashMismatch)
        );
        assert!(!store.command_path(claimed_hash).exists());
    }

    #[test]
    fn wal_command_store_rejects_conflicting_bytes_without_syncing() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"expected".to_vec());
        let conflicting = StoredCommand::new(EntryType::Command, b"conflicting".to_vec());
        store
            .wal
            .lock()
            .unwrap()
            .commands
            .insert(command.hash(), conflicting);
        reset_sync_counts();

        assert_eq!(
            store.store_command(command.hash(), command.clone()),
            Err(Error::CommandHashMismatch)
        );
        assert_eq!(sync_counts(), (0, 0));
        assert!(!store.command_path(command.hash()).exists());
    }

    #[test]
    fn prestored_command_resolution_revalidates_the_durable_file() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"pre-stored-command".to_vec());
        store
            .store_command(command.hash(), command.clone())
            .unwrap();
        reset_command_file_reads();

        for slot in [8, 9] {
            let value = AcceptedValue::from_command("cluster", slot, 1, 1, LogHash::ZERO, &command);
            store
                .record_proposal(RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot,
                    step: 4,
                    proposal: Proposal::new(ProposalPriority::MAX, "writer", slot, value),
                    command: None,
                })
                .unwrap();
        }

        assert_eq!(command_file_reads(), 2);
    }

    #[test]
    fn commandless_record_rejects_a_prestored_command_replaced_while_open() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"pre-stored".to_vec());
        let replacement = StoredCommand::new(EntryType::Command, b"replacement".to_vec());
        let command_hash = command.hash();
        store.store_command(command_hash, command.clone()).unwrap();
        std::fs::write(
            store.command_path(command_hash),
            encode_stored_command(&replacement),
        )
        .unwrap();
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);

        assert_eq!(
            store.record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value.clone()),
                command: None,
            }),
            Err(Error::CommandHashMismatch)
        );
        assert_eq!(store.load(8).unwrap().isr.step(), 0);
        drop(store);

        let reopened = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        assert_eq!(
            reopened.record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                command: None,
            }),
            Err(Error::CommandHashMismatch)
        );
        assert_eq!(reopened.load(8).unwrap().isr.step(), 0);
    }

    #[test]
    fn duplicate_store_rejects_a_prestored_command_replaced_while_open() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"pre-stored".to_vec());
        let replacement = StoredCommand::new(EntryType::Command, b"replacement".to_vec());
        let command_hash = command.hash();
        store.store_command(command_hash, command.clone()).unwrap();
        std::fs::write(
            store.command_path(command_hash),
            encode_stored_command(&replacement),
        )
        .unwrap();

        assert_eq!(
            store.store_command(command_hash, command),
            Err(Error::CommandHashMismatch)
        );
    }

    #[test]
    fn record_rejects_mismatched_inline_command_independent_of_prestored_command_state() {
        for prestored in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
            let store = RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            let expected = StoredCommand::new(EntryType::Command, b"expected".to_vec());
            if prestored {
                store
                    .store_command(expected.hash(), expected.clone())
                    .unwrap();
            }
            let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &expected);
            let mismatched = StoredCommand::new(EntryType::Command, b"mismatched".to_vec());

            assert_eq!(
                store.record_proposal(RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot: 8,
                    step: 4,
                    proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                    command: Some(mismatched),
                }),
                Err(Error::Rejected(RejectReason::InvalidValue)),
                "prestored={prestored}"
            );
        }
    }

    #[test]
    fn inline_record_uses_the_bound_command_without_reading_the_durable_command_file() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"inline-hot-path".to_vec());
        let command_hash = command.hash();
        let store = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        std::fs::write(store.command_path(command_hash), b"corrupt cache entry").unwrap();
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        reset_command_file_reads();

        store
            .record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                command: Some(command.clone()),
            })
            .unwrap();

        assert_eq!(command_file_reads(), 0);
        assert_eq!(
            store.fetch_command(command_hash).unwrap(),
            Some(command.clone())
        );
        drop(store);

        reset_command_file_reads();
        let reopened =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        assert_eq!(command_file_reads(), 0);
        assert_eq!(reopened.fetch_command(command_hash).unwrap(), Some(command));
    }

    #[test]
    fn record_rejects_malformed_config_change_piggyback_as_invalid_value_before_parsing() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let expected = StoredCommand::new(EntryType::Command, b"expected".to_vec());
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &expected);
        let malformed = StoredCommand::new(EntryType::ConfigChange, b"malformed".to_vec());

        assert_eq!(
            store.record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                command: Some(malformed),
            }),
            Err(Error::Rejected(RejectReason::InvalidValue))
        );
    }

    #[test]
    fn wal_append_uses_the_platform_safe_file_sync() {
        let file = tempfile::tempfile().unwrap();
        reset_sync_counts();

        sync_wal_append(&file).unwrap();

        #[cfg(target_os = "linux")]
        assert_eq!(last_file_sync_kind(), Some(FileSyncKind::Data));
        #[cfg(not(target_os = "linux"))]
        assert_eq!(last_file_sync_kind(), Some(FileSyncKind::All));
    }

    #[test]
    fn wal_metadata_changes_keep_full_file_sync() {
        let file = tempfile::tempfile().unwrap();
        reset_sync_counts();

        sync_wal_metadata(&file).unwrap();

        assert_eq!(last_file_sync_kind(), Some(FileSyncKind::All));
    }

    #[test]
    fn wal_replays_acknowledged_records_after_reopen() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"wal-reopen".to_vec());
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        {
            let store = RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            store
                .record_proposal(RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot: 8,
                    step: 4,
                    proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                    command: Some(command.clone()),
                })
                .unwrap();
        }

        let reopened =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        assert_eq!(
            reopened.fetch_command(command.hash()).unwrap(),
            Some(command)
        );
        assert_eq!(reopened.load(8).unwrap().isr.step(), 4);
    }

    #[test]
    fn wal_sync_fault_never_acknowledges_before_the_durable_frame_is_replayable() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"wal-sync-fault".to_vec());
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        {
            let store = RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            store
                .set_seal_fault(Some(SealFaultPoint::AfterWalSync))
                .unwrap();
            assert!(matches!(
                store.record_proposal(RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot: 8,
                    step: 4,
                    proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                    command: Some(command.clone()),
                }),
                Err(Error::Io(message)) if message.contains("AfterWalSync")
            ));
            assert_eq!(
                store
                    .configuration_state()
                    .unwrap()
                    .max_accepted_or_decided_slot(),
                None
            );
            assert_eq!(store.load(8).unwrap().isr.step(), 0);
        }

        let reopened =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        assert_eq!(
            reopened.fetch_command(command.hash()).unwrap(),
            Some(command)
        );
        assert_eq!(reopened.load(8).unwrap().isr.step(), 4);
    }

    #[test]
    fn wal_ignores_a_torn_final_frame_but_replays_the_committed_prefix() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let first = StoredCommand::new(EntryType::Command, b"wal-first".to_vec());
        let second = StoredCommand::new(EntryType::Command, b"wal-second".to_vec());
        {
            let store = RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            for (slot, command) in [(8, first.clone()), (9, second.clone())] {
                let value =
                    AcceptedValue::from_command("cluster", slot, 1, 1, LogHash::ZERO, &command);
                store
                    .record_proposal(RecordRequest {
                        cluster_id: "cluster".into(),
                        epoch: 1,
                        config_id: 1,
                        config_digest: membership.digest(),
                        slot,
                        step: 4,
                        proposal: Proposal::new(ProposalPriority::MAX, "writer", slot, value),
                        command: Some(command),
                    })
                    .unwrap();
            }
        }
        let wal = root.path().join("recorder.wal");
        let len = std::fs::metadata(&wal).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&wal)
            .unwrap()
            .set_len(len - 7)
            .unwrap();

        let reopened =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        assert_eq!(reopened.fetch_command(first.hash()).unwrap(), Some(first));
        assert_eq!(reopened.load(8).unwrap().isr.step(), 4);
        assert_eq!(reopened.fetch_command(second.hash()).unwrap(), None);
        assert_eq!(reopened.load(9).unwrap().isr.step(), 0);
    }

    #[test]
    fn recorder_preflight_distinguishes_absent_from_valid_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        let absent = root.path().join("absent");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        assert_eq!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &absent,
                "cluster",
                1,
                1,
                &membership,
            )
            .unwrap(),
            RecorderPreflight::Missing
        );
        assert!(!absent.exists());

        let valid = root.path().join("valid");
        drop(
            RecorderFileStore::new_with_membership(
                &valid,
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap(),
        );
        let before = directory_files(&valid);
        assert_eq!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &valid,
                "cluster",
                1,
                1,
                &membership,
            )
            .unwrap(),
            RecorderPreflight::Valid
        );
        assert_eq!(directory_files(&valid), before);
    }

    #[test]
    fn recorder_preflight_rejects_partial_or_foreign_state_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let partial = root.path().join("partial");
        std::fs::create_dir(&partial).unwrap();
        std::fs::write(partial.join("recorded-head.rec"), b"partial").unwrap();
        let partial_before = directory_files(&partial);
        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &partial,
                "cluster",
                1,
                1,
                &membership,
            )
            .is_err()
        );
        assert_eq!(directory_files(&partial), partial_before);

        let valid = root.path().join("foreign");
        drop(
            RecorderFileStore::new_with_membership(
                &valid,
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap(),
        );
        let before = directory_files(&valid);
        let other_membership = Membership::new(["n1", "n2", "n4"]).unwrap();
        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &valid,
                "cluster",
                1,
                2,
                &membership,
            )
            .is_err()
        );
        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &valid,
                "cluster",
                1,
                1,
                &other_membership,
            )
            .is_err()
        );
        assert_eq!(directory_files(&valid), before);
    }

    #[test]
    fn recorder_preflight_validates_command_cache_backed_values_without_mutation() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        for case in ["valid", "missing", "corrupt", "foreign"] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join(case);
            let (command_hash, command) =
                cache_backed_recorder_for_preflight(&root, membership.clone());
            let command_path = root.join(format!("command-{}.cmd", command_hash.to_hex()));
            match case {
                "valid" => {}
                "missing" => std::fs::remove_file(&command_path).unwrap(),
                "corrupt" => std::fs::write(&command_path, b"corrupt command cache").unwrap(),
                "foreign" => std::fs::write(
                    &command_path,
                    encode_stored_command(&StoredCommand::new(
                        EntryType::Command,
                        b"different command".to_vec(),
                    )),
                )
                .unwrap(),
                _ => unreachable!(),
            }
            let before = directory_files(&root);
            let result = RecorderFileStore::preflight_existing_with_membership_outcome(
                &root,
                "cluster",
                1,
                1,
                &membership,
            );
            if case == "valid" {
                assert_eq!(result.unwrap(), RecorderPreflight::Valid, "{case}");
                assert_eq!(
                    std::fs::read(&command_path).unwrap(),
                    encode_stored_command(&command),
                    "{case}"
                );
            } else {
                assert!(result.is_err(), "{case}");
            }
            assert_eq!(directory_files(&root), before, "{case}");
        }
    }

    #[test]
    fn recorder_preflight_rejects_nonregular_command_cache_without_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("recorder");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let (command_hash, _) = cache_backed_recorder_for_preflight(&root, membership.clone());
        let command_path = root.join(format!("command-{}.cmd", command_hash.to_hex()));
        std::fs::remove_file(&command_path).unwrap();
        std::fs::create_dir(&command_path).unwrap();
        let before = directory_files(&root);

        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &root,
                "cluster",
                1,
                1,
                &membership,
            )
            .is_err()
        );
        assert_eq!(directory_files(&root), before);
    }

    #[cfg(unix)]
    #[test]
    fn recorder_preflight_rejects_symlinked_command_cache_without_mutation() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("recorder");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let (command_hash, command) =
            cache_backed_recorder_for_preflight(&root, membership.clone());
        let command_path = root.join(format!("command-{}.cmd", command_hash.to_hex()));
        let outside = temp.path().join("outside-command.cmd");
        std::fs::write(&outside, encode_stored_command(&command)).unwrap();
        std::fs::remove_file(&command_path).unwrap();
        symlink(&outside, &command_path).unwrap();

        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &root,
                "cluster",
                1,
                1,
                &membership,
            )
            .is_err()
        );
        assert!(std::fs::symlink_metadata(&command_path)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            std::fs::read(&outside).unwrap(),
            encode_stored_command(&command)
        );
    }

    #[test]
    fn recorder_preflight_rejects_command_cache_backed_value_mismatch_without_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("recorder");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        cache_backed_recorder_for_preflight(&root, membership.clone());
        let wal_path = root.join("recorder.wal");
        let original = std::fs::read(&wal_path).unwrap();
        let (frame, end) = decode_wal_frame(&original, 0).unwrap().unwrap();
        assert_eq!(end, original.len());
        let mut state = decode_recorder_state(&frame.slot_bytes).unwrap();
        state
            .isr
            .first_current
            .as_mut()
            .unwrap()
            .value
            .as_mut()
            .unwrap()
            .entry_hash = LogHash::ZERO;
        let configuration = decode_configuration_state(&frame.configuration_bytes).unwrap();
        let (tampered, _, _) = encode_wal_frame(
            frame.generation,
            frame.sequence,
            frame.prev_digest,
            &state,
            &configuration,
            &frame.head,
            None,
        )
        .unwrap();
        std::fs::write(&wal_path, tampered).unwrap();
        let before = directory_files(&root);

        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &root,
                "cluster",
                1,
                1,
                &membership,
            )
            .is_err()
        );
        assert_eq!(directory_files(&root), before);
    }

    #[test]
    fn recorder_preflight_allows_normal_configuration_intent_recovery_before_open() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let configuration = store.configuration_state().unwrap();
        let state = RecorderSlotState::new_with_digest(8, "cluster", 1, 1, membership.digest());
        store
            .set_seal_fault(Some(SealFaultPoint::AfterIntent))
            .unwrap();
        assert!(store
            .commit_transition_unlocked(&state, &configuration)
            .is_err());
        drop(store);
        let before = directory_files(root.path());

        assert_eq!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                root.path(),
                "cluster",
                1,
                1,
                &membership,
            )
            .unwrap(),
            RecorderPreflight::Recoverable,
        );
        assert_eq!(directory_files(root.path()), before);
        RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
            .unwrap();
        assert!(!root.path().join("configuration.intent").exists());
    }

    #[test]
    fn recorder_preflight_allows_torn_final_wal_recovery_before_open() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"torn-preflight".to_vec());
        {
            let store = RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
            store
                .record_proposal(RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot: 8,
                    step: 4,
                    proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                    command: Some(command.clone()),
                })
                .unwrap();
        }
        let wal = root.path().join("recorder.wal");
        let len = std::fs::metadata(&wal).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&wal)
            .unwrap()
            .set_len(len - 7)
            .unwrap();
        let before = directory_files(root.path());

        assert_eq!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                root.path(),
                "cluster",
                1,
                1,
                &membership,
            )
            .unwrap(),
            RecorderPreflight::Recoverable,
        );
        assert_eq!(directory_files(root.path()), before);
        let reopened =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        assert_eq!(reopened.load(8).unwrap().isr.step(), 0);
    }

    #[test]
    fn recorder_preflight_keeps_interior_wal_corruption_invalid_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"interior-preflight".to_vec());
        let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
        store
            .record_proposal(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership.digest(),
                slot: 8,
                step: 4,
                proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                command: Some(command),
            })
            .unwrap();
        drop(store);
        let wal = root.path().join("recorder.wal");
        let mut bytes = std::fs::read(&wal).unwrap();
        bytes[100] ^= 0x80;
        std::fs::write(&wal, bytes).unwrap();
        let before = directory_files(root.path());

        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                root.path(),
                "cluster",
                1,
                1,
                &membership,
            )
            .is_err()
        );
        assert_eq!(directory_files(root.path()), before);
    }

    #[test]
    fn recorder_preflight_rejects_oversized_wal_cache_and_intent_without_mutation() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        for (name, limit) in [
            ("recorder.wal", super::MAX_RECORDER_WAL_BYTES),
            ("configuration.intent", super::MAX_TRANSITION_INTENT_BYTES),
        ] {
            let root = tempfile::tempdir().unwrap();
            drop(
                RecorderFileStore::new_with_membership(
                    root.path(),
                    "n1",
                    "cluster",
                    1,
                    1,
                    membership.clone(),
                )
                .unwrap(),
            );
            let path = root.path().join(name);
            std::fs::File::create(&path)
                .unwrap()
                .set_len(limit as u64 + 1)
                .unwrap();
            let before_len = std::fs::symlink_metadata(&path).unwrap().len();
            assert!(
                RecorderFileStore::preflight_existing_with_membership_outcome(
                    root.path(),
                    "cluster",
                    1,
                    1,
                    &membership,
                )
                .is_err()
            );
            assert_eq!(std::fs::symlink_metadata(&path).unwrap().len(), before_len);
        }

        let root = tempfile::tempdir().unwrap();
        let (command_hash, _) =
            cache_backed_recorder_for_preflight(root.path(), membership.clone());
        let path = root
            .path()
            .join(format!("command-{}.cmd", command_hash.to_hex()));
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(super::MAX_COMMAND_CACHE_BYTES as u64 + 1)
            .unwrap();
        let before_len = std::fs::symlink_metadata(&path).unwrap().len();
        assert!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                root.path(),
                "cluster",
                1,
                1,
                &membership,
            )
            .is_err()
        );
        assert_eq!(std::fs::symlink_metadata(&path).unwrap().len(), before_len);
    }

    #[cfg(unix)]
    #[test]
    fn recorder_preflight_rejects_symlinked_authority_files_without_mutation() {
        use std::os::unix::fs::symlink;

        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        for name in [
            "configuration.rec",
            "recorded-head.rec",
            "recorder.wal",
            "configuration.intent",
            "configuration-head.intent",
        ] {
            let root = tempfile::tempdir().unwrap();
            drop(
                RecorderFileStore::new_with_membership(
                    root.path(),
                    "n1",
                    "cluster",
                    1,
                    1,
                    membership.clone(),
                )
                .unwrap(),
            );
            let path = root.path().join(name);
            let outside = root.path().join(format!("outside-{name}"));
            if path.exists() {
                std::fs::rename(&path, &outside).unwrap();
            } else {
                std::fs::write(&outside, b"intent").unwrap();
            }
            symlink(&outside, &path).unwrap();

            assert!(
                RecorderFileStore::preflight_existing_with_membership_outcome(
                    root.path(),
                    "cluster",
                    1,
                    1,
                    &membership,
                )
                .is_err(),
                "{name}"
            );
            assert!(
                std::fs::symlink_metadata(&path)
                    .unwrap()
                    .file_type()
                    .is_symlink(),
                "{name}"
            );
        }
    }

    #[test]
    fn recorder_open_revalidates_after_preflight_before_recovery_mutation() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        drop(
            RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap(),
        );
        assert_eq!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                root.path(),
                "cluster",
                1,
                1,
                &membership,
            )
            .unwrap(),
            RecorderPreflight::Valid,
        );
        let configuration = root.path().join("configuration.rec");
        std::fs::write(&configuration, b"replaced after preflight").unwrap();
        let before = directory_files(root.path());

        assert!(RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership,
        )
        .is_err());
        assert_eq!(directory_files(root.path()), before);
    }

    #[test]
    fn existing_open_never_recreates_a_deleted_or_replaced_valid_recorder_root() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("recorder");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        drop(
            RecorderFileStore::new_with_membership(
                &root,
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap(),
        );
        assert_eq!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &root,
                "cluster",
                1,
                1,
                &membership,
            )
            .unwrap(),
            RecorderPreflight::Valid,
        );
        std::fs::remove_dir_all(&root).unwrap();
        let parent_before = directory_files(parent.path());
        assert!(RecorderFileStore::open_existing_with_membership(
            &root,
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .is_err());
        assert!(!root.exists());
        assert_eq!(directory_files(parent.path()), parent_before);

        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("replacement"), b"foreign").unwrap();
        let parent_before = directory_files(parent.path());
        assert!(RecorderFileStore::open_existing_with_membership(
            &root, "n1", "cluster", 1, 1, membership,
        )
        .is_err());
        assert!(!root.join(".recorder.lock").exists());
        assert_eq!(directory_files(parent.path()), parent_before);
    }

    #[test]
    fn existing_open_never_recreates_a_deleted_or_replaced_recoverable_recorder_root() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("recorder");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store = RecorderFileStore::new_with_membership(
            &root,
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let configuration = store.configuration_state().unwrap();
        let state = RecorderSlotState::new_with_digest(8, "cluster", 1, 1, membership.digest());
        store
            .set_seal_fault(Some(SealFaultPoint::AfterIntent))
            .unwrap();
        assert!(store
            .commit_transition_unlocked(&state, &configuration)
            .is_err());
        drop(store);
        assert_eq!(
            RecorderFileStore::preflight_existing_with_membership_outcome(
                &root,
                "cluster",
                1,
                1,
                &membership,
            )
            .unwrap(),
            RecorderPreflight::Recoverable,
        );
        std::fs::remove_dir_all(&root).unwrap();
        let parent_before = directory_files(parent.path());
        assert!(RecorderFileStore::open_existing_with_membership(
            &root,
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .is_err());
        assert!(!root.exists());
        assert_eq!(directory_files(parent.path()), parent_before);

        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("replacement"), b"foreign").unwrap();
        let parent_before = directory_files(parent.path());
        assert!(RecorderFileStore::open_existing_with_membership(
            &root, "n1", "cluster", 1, 1, membership,
        )
        .is_err());
        assert!(!root.join(".recorder.lock").exists());
        assert_eq!(directory_files(parent.path()), parent_before);
    }

    #[cfg(unix)]
    #[test]
    fn existing_open_rejects_a_recorder_root_symlink_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let parent = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        let root = parent.path().join("recorder");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        std::fs::write(target.path().join("sentinel"), b"untouched").unwrap();
        let before = directory_files(target.path());
        symlink(target.path(), &root).unwrap();

        assert!(RecorderFileStore::open_existing_with_membership(
            &root, "n1", "cluster", 1, 1, membership,
        )
        .is_err());
        assert!(std::fs::symlink_metadata(&root)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(directory_files(target.path()), before);
    }

    #[cfg(unix)]
    #[test]
    fn fresh_open_rejects_a_symlinked_parent_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let parent = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        let root = parent.path().join("linked-parent").join("recorder");
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        std::fs::write(target.path().join("sentinel"), b"untouched").unwrap();
        let before = directory_files(target.path());
        symlink(target.path(), parent.path().join("linked-parent")).unwrap();

        assert!(
            RecorderFileStore::new_with_membership(&root, "n1", "cluster", 1, 1, membership,)
                .is_err()
        );
        assert!(
            std::fs::symlink_metadata(parent.path().join("linked-parent"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(!target
            .path()
            .join("recorder")
            .join(".recorder.lock")
            .exists());
        assert_eq!(directory_files(target.path()), before);
    }

    #[test]
    fn wal_fails_closed_on_interior_corruption() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        {
            let store = RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            for slot in [8, 9] {
                let command = StoredCommand::new(
                    EntryType::Command,
                    format!("wal-corrupt-{slot}").into_bytes(),
                );
                let value =
                    AcceptedValue::from_command("cluster", slot, 1, 1, LogHash::ZERO, &command);
                store
                    .record_proposal(RecordRequest {
                        cluster_id: "cluster".into(),
                        epoch: 1,
                        config_id: 1,
                        config_digest: membership.digest(),
                        slot,
                        step: 4,
                        proposal: Proposal::new(ProposalPriority::MAX, "writer", slot, value),
                        command: Some(command),
                    })
                    .unwrap();
            }
        }
        let wal = root.path().join("recorder.wal");
        let mut bytes = std::fs::read(&wal).unwrap();
        bytes[100] ^= 0x80;
        std::fs::write(&wal, bytes).unwrap();

        assert!(matches!(
            RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership,
            ),
            Err(Error::Decode(message)) if message.contains("WAL")
        ));
    }

    #[test]
    fn wal_fails_closed_on_full_length_final_frame_corruption() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        {
            let store = RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            let command = StoredCommand::new(EntryType::Command, b"wal-final-corrupt".to_vec());
            let value = AcceptedValue::from_command("cluster", 8, 1, 1, LogHash::ZERO, &command);
            store
                .record_proposal(RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot: 8,
                    step: 4,
                    proposal: Proposal::new(ProposalPriority::MAX, "writer", 1, value),
                    command: Some(command),
                })
                .unwrap();
        }
        let wal = root.path().join("recorder.wal");
        let mut bytes = std::fs::read(&wal).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x80;
        std::fs::write(&wal, bytes).unwrap();

        assert!(matches!(
            RecorderFileStore::new_with_membership(
                root.path(),
                "n1",
                "cluster",
                1,
                1,
                membership,
            ),
            Err(Error::Decode(message)) if message.contains("WAL frame checksum")
        ));
    }

    #[test]
    fn wal_rotation_checkpoints_before_reusing_the_stable_file() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let store = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        for slot in 1..=super::RECORDER_WAL_HARD_FRAME_LIMIT + 1 {
            let command = StoredCommand::new(
                EntryType::Command,
                format!("wal-rotate-{slot}").into_bytes(),
            );
            let value = AcceptedValue::from_command("cluster", slot, 1, 1, LogHash::ZERO, &command);
            store
                .record_proposal(RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    slot,
                    step: 4,
                    proposal: Proposal::new(ProposalPriority::MAX, "writer", slot, value),
                    command: Some(command),
                })
                .unwrap();
        }
        let (generation, through_sequence, frames) = store.wal_stats().unwrap();
        assert!(generation > 1);
        assert!(through_sequence >= super::RECORDER_WAL_HARD_FRAME_LIMIT);
        assert_eq!(frames, 1);
        drop(store);

        let reopened =
            RecorderFileStore::new_with_membership(root.path(), "n1", "cluster", 1, 1, membership)
                .unwrap();
        assert_eq!(
            reopened
                .load(super::RECORDER_WAL_HARD_FRAME_LIMIT + 1)
                .unwrap()
                .isr
                .step(),
            4
        );
    }

    proptest! {
        #[test]
        fn wal_frame_round_trips_arbitrary_inline_commands(
            sequence in 1u64..u64::MAX,
            payload in proptest::collection::vec(any::<u8>(), 0..2048),
        ) {
            let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
            let configuration = ConfigurationState::initial(
                1,
                membership.digest(),
                Some(membership),
            );
            let state = RecorderSlotState::new_with_digest(
                8,
                "cluster",
                1,
                1,
                configuration.config_digest(),
            );
            let command = StoredCommand::new(EntryType::Command, payload);
            let (encoded, digest, slot_bytes) = encode_wal_frame(
                3,
                sequence,
                LogHash::ZERO,
                &state,
                &configuration,
                &RecordedHeadProvenance::Empty,
                Some((command.hash(), &command)),
            ).unwrap();
            let (decoded, end) = decode_wal_frame(&encoded, 0).unwrap().unwrap();
            prop_assert_eq!(end, encoded.len());
            prop_assert_eq!(decoded.generation, 3);
            prop_assert_eq!(decoded.sequence, sequence);
            prop_assert_eq!(decoded.digest, digest);
            prop_assert_eq!(decoded.slot_bytes, slot_bytes);
            prop_assert_eq!(decoded.command, Some((command.hash(), command)));
        }
    }

    #[test]
    fn fresh_initialization_recovers_when_configuration_was_published_before_head() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let (store, _) = RecorderFileStore::open_root(root.path(), "n1", "cluster", 1, 1).unwrap();
        let configuration =
            ConfigurationState::initial(1, membership.digest(), Some(membership.clone()));
        store
            .set_seal_fault(Some(SealFaultPoint::AfterHeadConfiguration))
            .unwrap();

        assert!(matches!(
            store.commit_configuration_head_unlocked(
                &configuration,
                &RecordedHeadProvenance::Empty,
            ),
            Err(Error::Io(message))
                if message.contains("AfterHeadConfiguration")
        ));
        assert!(root.path().join("configuration.rec").exists());
        assert!(!root.path().join("recorded-head.rec").exists());
        assert!(root.path().join("configuration-head.intent").exists());
        drop(store);

        let reopened = RecorderFileStore::new_with_membership(
            root.path(),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        assert_eq!(
            reopened.configuration_state().unwrap().membership(),
            Some(&membership)
        );
        assert!(root.path().join("recorded-head.rec").exists());
        assert!(!root.path().join("configuration-head.intent").exists());
    }

    #[test]
    fn progress_remembers_config_change_after_later_normal_adoption() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let stores: Vec<_> = membership
            .members()
            .iter()
            .map(|id| {
                RecorderFileStore::new_with_membership(
                    root.path().join(id),
                    id.clone(),
                    "cluster",
                    1,
                    1,
                    membership.clone(),
                )
                .unwrap()
            })
            .collect();
        let offered = StoredCommand::new(EntryType::Command, b"offered".to_vec());
        let transition = ConfigChange::bound_stop(
            "cluster",
            1,
            membership.digest(),
            2,
            membership.members().to_vec(),
        )
        .unwrap()
        .to_stored_command();
        let adopted = StoredCommand::new(EntryType::Command, b"adopted".to_vec());
        for store in &stores {
            for command in [&transition, &adopted] {
                store
                    .store_command(command.hash(), command.clone())
                    .unwrap();
            }
        }
        let recorders = membership
            .members()
            .iter()
            .zip(&stores)
            .map(|(id, store)| (id.clone(), Box::new(store.clone()) as Box<dyn RecorderRpc>))
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let proposal = |command: &StoredCommand| {
            Proposal::new(
                ProposalPriority::from_u64(1),
                "other",
                1,
                AcceptedValue::from_command("cluster", 1, 1, 1, LogHash::ZERO, command),
            )
        };
        let mut progress = ProposerProgress::new(1, proposal(&offered)).with_command(offered);

        progress.proposal = proposal(&transition);
        consensus.ensure_progress_command(&mut progress).unwrap();
        progress.proposal = proposal(&adopted);
        consensus.ensure_progress_command(&mut progress).unwrap();

        assert_eq!(progress.command, Some(adopted));
        assert!(progress.transition_involved);
    }

    #[test]
    fn record_broadcast_reuses_one_worker_thread_per_recorder() {
        let seen: Vec<_> = (0..3)
            .map(|_| Arc::new(Mutex::new(HashSet::new())))
            .collect();
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .zip(&seen)
            .map(|(recorder_id, threads)| {
                (
                    recorder_id.into(),
                    Box::new(ThreadRecordingRecorder {
                        recorder_id,
                        threads: Arc::clone(threads),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        for slot in 1..=16 {
            assert_eq!(
                consensus
                    .record_broadcast(record_requests(&consensus, slot))
                    .unwrap()
                    .len(),
                2
            );
        }
        drop(consensus);

        assert!(seen
            .iter()
            .all(|threads| threads.lock().unwrap().len() == 1));
    }

    #[test]
    fn unsorted_explicit_recorder_ids_preserve_rpc_pairing_across_worker_paths() {
        let recorders = ["n3", "n1", "n2"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(SlotRecorder {
                        recorder_id,
                        reject_slot: None,
                        observed: None,
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        assert_eq!(
            consensus
                .record_broadcast(record_requests(&consensus, 1))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(consensus.inspect_decision_proof_at(1).unwrap(), None);
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn repeated_control_operations_reuse_one_worker_thread_per_recorder() {
        let seen: Vec<_> = (0..3)
            .map(|_| Arc::new(Mutex::new(HashSet::new())))
            .collect();
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .zip(&seen)
            .map(|(recorder_id, threads)| {
                (
                    recorder_id.into(),
                    Box::new(ThreadRecordingControlRecorder {
                        threads: Arc::clone(threads),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        for slot in 1..=16 {
            assert_eq!(consensus.inspect_decision_proof_at(slot).unwrap(), None);
        }
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));

        assert!(seen
            .iter()
            .all(|threads| threads.lock().unwrap().len() == 1));
    }

    #[test]
    fn blocked_control_minority_does_not_delay_record_quorum() {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n1",
                    reject_slot: None,
                    observed: None,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(BlockingControlRecorder {
                    recorder_id: "n2",
                    started: started_tx,
                    release_first: Mutex::new(release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n3",
                    reject_slot: None,
                    observed: None,
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        assert_eq!(consensus.inspect_decision_proof_at(1).unwrap(), None);
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        let replies = consensus
            .record_broadcast(record_requests(&consensus, 1))
            .unwrap();
        assert_eq!(replies.len(), 2);

        release_tx.send(()).unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn blocked_control_majority_does_not_head_of_line_block_read_fence() {
        let (started_tx, started_rx) = mpsc::sync_channel(2);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(BlockingInspectionReadFenceRecorder {
                        recorder_id,
                        block_inspection: recorder_id != "n3",
                        started: started_tx.clone(),
                        release: Arc::clone(&release),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let inspecting = Arc::clone(&consensus);
        let inspection = thread::spawn(move || inspecting.inspect_decision_at(1, LogHash::ZERO));
        let mut started = BTreeSet::new();
        started.insert(started_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        started.insert(started_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        assert_eq!(started, BTreeSet::from(["n1", "n2"]));

        let before = Instant::now();
        assert_eq!(
            consensus
                .inspect_context_read_fence_at(1, LogHash::ZERO)
                .unwrap(),
            CertifiedDecisionInspection::Empty
        );
        assert!(before.elapsed() < Duration::from_millis(250));

        let (released, condition) = &*release;
        *released.lock().unwrap() = true;
        condition.notify_all();
        assert_eq!(
            inspection.join().unwrap().unwrap(),
            DecisionInspection::Empty
        );
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn saturated_control_queue_does_not_contaminate_later_requests() {
        let (started_tx, started_rx) = mpsc::sync_channel(4);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n1",
                    reject_slot: None,
                    observed: None,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(BlockingControlRecorder {
                    recorder_id: "n2",
                    started: started_tx,
                    release_first: Mutex::new(release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n3",
                    reject_slot: None,
                    observed: None,
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        assert_eq!(consensus.inspect_decision_proof_at(1).unwrap(), None);
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        assert_eq!(consensus.inspect_decision_proof_at(2).unwrap(), None);
        assert_eq!(consensus.inspect_decision_proof_at(3).unwrap(), None);

        release_tx.send(()).unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(2));
        assert!(matches!(
            started_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        assert_eq!(consensus.inspect_decision_proof_at(4).unwrap(), None);
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(4));
    }

    #[test]
    fn command_registration_returns_no_quorum_when_all_control_queues_are_full() {
        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let recorders = ["n1", "n2", "n3"]
            .into_iter()
            .map(|recorder_id| {
                (
                    recorder_id.into(),
                    Box::new(BlockingCommandStoreRecorder {
                        started: started_tx.clone(),
                        release: Arc::clone(&release),
                    }) as Box<dyn RecorderRpc>,
                )
            })
            .collect();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let first = StoredCommand::new(EntryType::Command, b"first".to_vec());
        let registering = Arc::clone(&consensus);
        let first_hash = first.hash();
        let first_payload = first.payload.clone();
        let registration =
            thread::spawn(move || registering.register_command(first_hash, first_payload));

        for _ in 0..3 {
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }

        let queued = StoredCommand::new(EntryType::Command, b"queued".to_vec());
        let (queued_tx, _queued_rx) = mpsc::sync_channel(3);
        for (index, worker) in consensus.control_workers.iter().enumerate() {
            worker.dispatch(ControlJob::StoreCommand {
                index,
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: consensus.membership().digest(),
                command_hash: queued.hash(),
                command: queued.clone(),
                result: queued_tx.clone(),
            });
        }

        let saturated = StoredCommand::new(EntryType::Command, b"saturated".to_vec());
        assert_eq!(
            consensus.register_command(saturated.hash(), saturated.payload),
            Err(Error::NoQuorum)
        );

        let (released, condition) = &*release;
        *released.lock().unwrap() = true;
        condition.notify_all();
        assert_eq!(registration.join().unwrap(), Ok(()));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn saturated_control_worker_keeps_command_quorum_retryable_after_worker_failure() {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let recorders = vec![
            (
                "n1".into(),
                Box::new(BlockingCommandStoreRecorder {
                    started: started_tx,
                    release: Arc::clone(&release),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(SuccessfulCommandStoreRecorder) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(FailingCommandStoreRecorder) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );
        let first = StoredCommand::new(EntryType::Command, b"first".to_vec());
        let registering = Arc::clone(&consensus);
        let registration =
            thread::spawn(move || registering.register_command(first.hash(), first.payload));
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let queued = StoredCommand::new(EntryType::Command, b"queued".to_vec());
        let (queued_tx, _queued_rx) = mpsc::sync_channel(1);
        consensus.control_workers[0].dispatch(ControlJob::StoreCommand {
            index: 0,
            cluster_id: "cluster".into(),
            epoch: 1,
            config_id: 1,
            config_digest: consensus.membership().digest(),
            command_hash: queued.hash(),
            command: queued,
            result: queued_tx,
        });

        let retry = StoredCommand::new(EntryType::Command, b"retry".to_vec());
        assert_eq!(
            consensus.register_command(retry.hash(), retry.payload),
            Err(Error::NoQuorum)
        );

        let (released, condition) = &*release;
        *released.lock().unwrap() = true;
        condition.notify_all();
        assert_eq!(registration.join().unwrap(), Ok(()));
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn control_worker_finish_and_drop_are_bounded() {
        let new_consensus = || {
            let (started_tx, started_rx) = mpsc::sync_channel(1);
            let (release_tx, release_rx) = mpsc::sync_channel(0);
            let recorders = vec![
                (
                    "n1".into(),
                    Box::new(SlotRecorder {
                        recorder_id: "n1",
                        reject_slot: None,
                        observed: None,
                    }) as Box<dyn RecorderRpc>,
                ),
                (
                    "n2".into(),
                    Box::new(BlockingControlRecorder {
                        recorder_id: "n2",
                        started: started_tx,
                        release_first: Mutex::new(release_rx),
                    }) as Box<dyn RecorderRpc>,
                ),
                (
                    "n3".into(),
                    Box::new(SlotRecorder {
                        recorder_id: "n3",
                        reject_slot: None,
                        observed: None,
                    }) as Box<dyn RecorderRpc>,
                ),
            ];
            (
                ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders)
                    .unwrap(),
                started_rx,
                release_tx,
            )
        };

        let (consensus, started_rx, release_tx) = new_consensus();
        let consensus = Arc::new(consensus);
        assert_eq!(consensus.inspect_decision_proof_at(1).unwrap(), None);
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        let (finished_tx, finished_rx) = mpsc::sync_channel(1);
        let finishing = Arc::clone(&consensus);
        let finisher = thread::spawn(move || {
            finished_tx
                .send(finishing.finish_pending_rpcs(Duration::from_millis(10)))
                .unwrap();
        });
        assert_eq!(finished_rx.recv_timeout(Duration::from_secs(1)), Ok(false));
        finisher.join().unwrap();
        release_tx.send(()).unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));

        let (consensus, started_rx, release_tx) = new_consensus();
        assert_eq!(consensus.inspect_decision_proof_at(1).unwrap(), None);
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        let (dropped_tx, dropped_rx) = mpsc::sync_channel(1);
        let dropper = thread::spawn(move || {
            drop(consensus);
            dropped_tx.send(()).unwrap();
        });
        assert_eq!(
            dropped_rx.recv_timeout(Duration::from_millis(10)),
            Err(mpsc::RecvTimeoutError::Timeout)
        );
        release_tx.send(()).unwrap();
        assert_eq!(dropped_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
        dropper.join().unwrap();
    }

    #[test]
    fn saturated_minority_queue_does_not_delay_or_contaminate_later_broadcasts() {
        let (started_tx, started_rx) = mpsc::sync_channel(2);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let (n1_seen_tx, n1_seen_rx) = mpsc::sync_channel(1);
        let (reject_seen_tx, reject_seen_rx) = mpsc::sync_channel(1);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n1",
                    reject_slot: None,
                    observed: Some(n1_seen_tx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(BlockingRecorder {
                    recorder_id: "n2",
                    started: started_tx,
                    release_first: Mutex::new(release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n3",
                    reject_slot: Some(2),
                    observed: Some(reject_seen_tx),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap(),
        );

        let first = consensus
            .record_broadcast(record_requests(&consensus, 1))
            .unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        assert_eq!(n1_seen_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        assert_eq!(reject_seen_rx.recv_timeout(Duration::from_secs(1)), Ok(1));

        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let second_consensus = Arc::clone(&consensus);
        let second = thread::spawn(move || {
            done_tx
                .send(second_consensus.record_broadcast(record_requests(&second_consensus, 2)))
                .unwrap();
        });
        assert_eq!(reject_seen_rx.recv_timeout(Duration::from_secs(1)), Ok(2));
        assert_eq!(n1_seen_rx.recv_timeout(Duration::from_secs(1)), Ok(2));
        assert!(matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));

        let (third_done_tx, third_done_rx) = mpsc::sync_channel(1);
        let third_consensus = Arc::clone(&consensus);
        let third = thread::spawn(move || {
            third_done_tx
                .send(third_consensus.record_broadcast(record_requests(&third_consensus, 3)))
                .unwrap();
        });
        let third_replies = third_done_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(third_replies.len(), 2);
        assert!(third_replies.iter().all(|reply| reply.slot == 3));

        release_tx.send(()).unwrap();

        let replies = done_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(replies.len(), 2);
        assert!(replies.iter().all(|reply| reply.slot == 2));
        assert!(replies.iter().any(|reply| reply.recorder_id == "n2"));
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(2));
        second.join().unwrap();
        third.join().unwrap();
    }

    #[test]
    fn saturated_recorder_keeps_a_minority_rejection_retryable() {
        let (started_tx, started_rx) = mpsc::sync_channel(2);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n1",
                    reject_slot: None,
                    observed: None,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(BlockingRecorder {
                    recorder_id: "n2",
                    started: started_tx,
                    release_first: Mutex::new(release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(RejectFromSlotRecorder {
                    recorder_id: "n3",
                    reject_from: 3,
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        assert_eq!(
            consensus
                .record_broadcast(record_requests(&consensus, 1))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        assert_eq!(
            consensus
                .record_broadcast(record_requests(&consensus, 2))
                .unwrap()
                .len(),
            2
        );

        let failure = (3..=514).find_map(|slot| {
            let result = consensus.record_broadcast(record_requests(&consensus, slot));
            match &result {
                Ok(replies)
                    if replies.len() == 1
                        && replies[0].recorder_id == "n1"
                        && replies[0].slot == slot =>
                {
                    None
                }
                _ => Some((slot, result)),
            }
        });
        release_tx.send(()).unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(2));
        assert!(
            failure.is_none(),
            "a saturated recorder must keep minority rejection retryable: {failure:?}"
        );
    }

    #[test]
    fn saturated_recorder_keeps_quorum_reachable_when_another_worker_fails() {
        let (started_tx, started_rx) = mpsc::sync_channel(2);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(BlockingRecorder {
                    recorder_id: "n1",
                    started: started_tx,
                    release_first: Mutex::new(release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(FailingFromSlotRecorder {
                    recorder_id: "n2",
                    fail_from: 3,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n3",
                    reject_slot: None,
                    observed: None,
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        assert_eq!(
            consensus
                .record_broadcast(record_requests(&consensus, 1))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));
        assert_eq!(
            consensus
                .record_broadcast(record_requests(&consensus, 2))
                .unwrap()
                .len(),
            2
        );

        let third = consensus.record_broadcast(record_requests(&consensus, 3));
        assert!(
            matches!(third, Ok(ref replies) if replies.len() == 1 && replies[0].recorder_id == "n3"),
            "a saturated healthy voter must keep the quorum retryable after a worker failure: {third:?}"
        );

        release_tx.send(()).unwrap();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(2));
    }

    #[test]
    fn command_lookup_waits_for_valid_reply_after_quorum_reports_missing() {
        let command = StoredCommand::new(EntryType::Command, b"available".to_vec());
        let value = AcceptedValue::from_command("cluster", 7, 1, 1, LogHash::ZERO, &command);
        let (observed_tx, observed_rx) = mpsc::sync_channel(2);
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(MissingCommandRecorder {
                    observed: observed_tx.clone(),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(MissingCommandRecorder {
                    observed: observed_tx,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(BlockingCommandRecorder {
                    started: started_tx,
                    release: Mutex::new(release_rx),
                    command: command.clone(),
                }) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let fetch = thread::spawn(move || {
            done_tx
                .send(consensus.fetch_verified_value(7, &value))
                .unwrap();
        });

        assert_eq!(observed_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
        assert_eq!(observed_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
        assert!(matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));

        release_tx.send(()).unwrap();
        assert_eq!(
            done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Ok(Some(command))
        );
        fetch.join().unwrap();
    }

    #[test]
    fn command_lookup_rejects_cryptographic_mismatch_despite_reachable_quorum() {
        let command = StoredCommand::new(EntryType::Command, b"mismatched".to_vec());
        let mut value = AcceptedValue::from_command("cluster", 7, 1, 1, LogHash::ZERO, &command);
        value.entry_hash = LogHash::ZERO;
        let (observed_tx, observed_rx) = mpsc::sync_channel(1);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(AvailableCommandRecorder {
                    command: command.clone(),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(MissingCommandRecorder {
                    observed: observed_tx,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(FailingCommandFetchRecorder) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        assert_eq!(
            consensus.fetch_verified_value(7, &value),
            Err(Error::Rejected(RejectReason::InvalidValue))
        );
        assert_eq!(observed_rx.recv_timeout(Duration::from_secs(1)), Ok(()));
    }

    #[test]
    fn command_lookup_returns_no_quorum_when_a_control_worker_queue_is_full() {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let (observed_tx, observed_rx) = mpsc::sync_channel(1);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(BlockingCommandStoreRecorder {
                    started: started_tx,
                    release: Arc::clone(&release),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(MissingCommandRecorder {
                    observed: observed_tx,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(FailingCommandFetchRecorder) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();
        let blocking = StoredCommand::new(EntryType::Command, b"blocking".to_vec());
        let (blocking_tx, _blocking_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            consensus.control_workers[0].dispatch(ControlJob::StoreCommand {
                index: 0,
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: consensus.membership().digest(),
                command_hash: blocking.hash(),
                command: blocking,
                result: blocking_tx,
            }),
            ControlDispatch::Accepted
        ));
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let queued = StoredCommand::new(EntryType::Command, b"queued".to_vec());
        let (queued_tx, _queued_rx) = mpsc::sync_channel(1);
        assert!(matches!(
            consensus.control_workers[0].dispatch(ControlJob::StoreCommand {
                index: 0,
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: consensus.membership().digest(),
                command_hash: queued.hash(),
                command: queued,
                result: queued_tx,
            }),
            ControlDispatch::Accepted
        ));

        let command = StoredCommand::new(EntryType::Command, b"missing".to_vec());
        let value = AcceptedValue::from_command("cluster", 7, 1, 1, LogHash::ZERO, &command);
        assert_eq!(
            consensus.fetch_verified_value(7, &value),
            Err(Error::NoQuorum)
        );
        assert_eq!(observed_rx.recv_timeout(Duration::from_secs(1)), Ok(()));

        let (released, condition) = &*release;
        *released.lock().unwrap() = true;
        condition.notify_all();
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    }

    #[test]
    fn full_record_worker_queue_is_transient_unavailable_not_fatal() {
        let (started_tx, started_rx) = mpsc::sync_channel(2);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let recorders = vec![
            (
                "n1".into(),
                Box::new(BlockingRecorder {
                    recorder_id: "n1",
                    started: started_tx,
                    release_first: Mutex::new(release_rx),
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(AlwaysIoRecorder) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(AlwaysIoRecorder) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        assert!(consensus
            .record_broadcast(record_requests(&consensus, 1))
            .unwrap()
            .is_empty());
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(1));

        assert!(consensus
            .record_broadcast(record_requests(&consensus, 2))
            .unwrap()
            .is_empty());

        let third = consensus.record_broadcast(record_requests(&consensus, 3));
        assert!(
            matches!(third, Ok(ref replies) if replies.is_empty()),
            "a full worker queue must remain retryable, got {third:?}"
        );

        release_tx.send(()).unwrap();
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(1)), Ok(2));
    }

    #[test]
    fn disconnected_record_worker_is_fatal() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let proposal = Proposal::new(
            ProposalPriority::MAX,
            "n1",
            1,
            AcceptedValue {
                command_hash: LogHash::ZERO,
                prev_hash: LogHash::ZERO,
                entry_hash: LogHash::ZERO,
            },
        );
        let request = RecordRequest {
            cluster_id: "cluster".into(),
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            slot: 1,
            step: 4,
            proposal,
            command: None,
        };
        let (job_tx, job_rx) = mpsc::sync_channel(1);
        drop(job_rx);
        let pending = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let worker = super::RecordWorker {
            sender: Some(job_tx),
            handle: None,
            pending: Arc::clone(&pending),
        };
        let (result_tx, result_rx) = mpsc::sync_channel(1);

        worker.dispatch(super::RecordJob {
            index: 0,
            request,
            result: result_tx,
        });

        assert_eq!(result_rx.recv().unwrap().1, Err(Error::ProposeFailed));
        assert_eq!(pending.load(std::sync::atomic::Ordering::Acquire), 0);
    }

    #[test]
    fn recorder_panics_are_reported_without_panicking_the_proposer() {
        let recorders = vec![
            (
                "n1".into(),
                Box::new(SlotRecorder {
                    recorder_id: "n1",
                    reject_slot: None,
                    observed: None,
                }) as Box<dyn RecorderRpc>,
            ),
            (
                "n2".into(),
                Box::new(PanickingRecorder) as Box<dyn RecorderRpc>,
            ),
            (
                "n3".into(),
                Box::new(PanickingRecorder) as Box<dyn RecorderRpc>,
            ),
        ];
        let consensus =
            ThreeNodeConsensus::from_recorders_with_ids("cluster", "n1", 1, 1, recorders).unwrap();

        assert_eq!(
            consensus.record_broadcast(record_requests(&consensus, 1)),
            Err(Error::ProposeFailed)
        );
    }
}
