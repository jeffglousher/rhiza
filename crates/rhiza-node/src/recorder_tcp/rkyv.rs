use rhiza_core::{EntryType, LogHash, StoredCommand};
use rhiza_quepaxa::{
    AcceptedValue, Ballot, DecisionProof, Proposal, ProposalPriority, ReadFenceObservation,
    ReadFenceRequest, ReadFenceSlotState, RecordRequest, RecordSummary, RecorderSummary,
    RejectReason,
};
use rkyv::{rancor::Error as RkyvError, util::AlignedVec};

use super::{
    Hello, HelloReply, Operation, RecorderRequestBody, RecorderResponseBody, RequestFrame,
    ResponseFrame, RpcResult,
};

use crate::{DEFAULT_PEER_CONCURRENCY, MAX_COMMAND_BYTES, MAX_REQUEST_ID_BYTES};

const MAGIC: &[u8; 4] = b"RZRV";
const ARCHIVE_VERSION: u16 = 2;
const HEADER_LEN: usize = 28;

const KIND_HELLO: u8 = 1;
const KIND_HELLO_REPLY: u8 = 2;
const KIND_REQUEST: u8 = 3;
const KIND_RESPONSE: u8 = 4;

const STATUS_OK: u8 = 0;
const STATUS_REJECTED: u8 = 1;
const STATUS_ERROR: u8 = 2;
const STATUS_OVERLOADED: u8 = 3;

macro_rules! wire {
    ($item:item) => {
        #[derive(rkyv::Archive, rkyv::Deserialize, rkyv::Serialize)]
        $item
    };
}

wire!(
    struct WireHello {
        version: u16,
        node_id: String,
        recovery_generation: u64,
        token: String,
    }
);
wire!(
    struct WireHash([u8; 32]);
);
wire!(
    enum WireEntryType {
        Command,
        ConfigChange,
        SnapshotBarrier,
        SnapshotPublished,
        Noop,
    }
);
wire!(
    struct WireStoredCommand {
        entry_type: WireEntryType,
        payload: Vec<u8>,
    }
);
wire!(
    struct WireAcceptedValue {
        command_hash: WireHash,
        prev_hash: WireHash,
        entry_hash: WireHash,
    }
);
wire!(
    struct WireProposal {
        priority: [u8; 32],
        proposer_id: String,
        proposal_id: u64,
        value: Option<WireAcceptedValue>,
    }
);
wire!(
    struct WireRecorderSummary {
        recorder_id: String,
        slot: u64,
        step: u64,
        first_current: Option<WireProposal>,
        aggregate_prior: Option<WireProposal>,
    }
);
wire!(
    enum WireDecisionProof {
        FastPath {
            cluster_id: String,
            slot: u64,
            epoch: u64,
            config_id: u64,
            config_digest: WireHash,
            proposal: WireProposal,
            summaries: Vec<WireRecorderSummary>,
        },
        Phase2 {
            cluster_id: String,
            slot: u64,
            epoch: u64,
            config_id: u64,
            config_digest: WireHash,
            step: u64,
            proposal: WireProposal,
            summaries: Vec<WireRecorderSummary>,
        },
    }
);
wire!(
    struct WireRecordRequest {
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: WireHash,
        slot: u64,
        step: u64,
        proposal: WireProposal,
        command: Option<WireStoredCommand>,
    }
);
wire!(
    struct WireRecordSummary {
        recorder_id: String,
        slot: u64,
        config_id: u64,
        config_digest: WireHash,
        step: u64,
        first_current: Option<WireProposal>,
        aggregate_prior: Option<WireProposal>,
        decided: Option<WireDecisionProof>,
    }
);
wire!(
    struct WireReadFenceRequest {
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: WireHash,
        slot: u64,
    }
);
wire!(
    enum WireReadFenceSlotState {
        Empty,
        Occupied {
            summary: Option<Box<WireRecordSummary>>,
        },
    }
);
wire!(
    struct WireReadFenceObservation {
        recorder_id: String,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: WireHash,
        slot: u64,
        max_head: Option<u64>,
        slot_state: WireReadFenceSlotState,
    }
);
wire!(
    struct WireBallot {
        round: u64,
        priority: u128,
        proposer_id: String,
    }
);
wire!(
    enum WireRejectReason {
        StaleEpoch,
        FutureEpoch,
        WrongCluster,
        WrongConfig,
        WrongSlot,
        AlreadyDecided,
        MalformedDecision,
        BallotPromised { promised: WireBallot },
        ConflictingValue,
        InvalidValue,
        InvalidCertificate,
        ConfigurationSealed { stop_slot: u64 },
        ConfigurationNotInstalled,
        ActivationRequired,
        TransitionInProgress,
        InvalidTransition,
        LocalVoterRequired,
        StepRegression,
        InvalidRequest,
    }
);
wire!(
    struct WireStoreCommandRequest {
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: WireHash,
        command_hash: WireHash,
        command: WireStoredCommand,
    }
);
wire!(
    struct WireFetchCommandRequest {
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: WireHash,
        command_hash: WireHash,
    }
);
wire!(
    struct WireInstallDecisionProofRequest {
        proof: WireDecisionProof,
        members: Vec<String>,
    }
);

fn archive<T>(value: &T) -> Result<AlignedVec, String>
where
    for<'a> T: rkyv::Serialize<
        rkyv::api::high::HighSerializer<
            AlignedVec,
            rkyv::ser::allocator::ArenaHandle<'a>,
            RkyvError,
        >,
    >,
{
    rkyv::to_bytes::<RkyvError>(value).map_err(|error| error.to_string())
}

fn operation_tag(operation: Operation) -> u8 {
    match operation {
        Operation::Identity => 1,
        Operation::StoreCommand => 2,
        Operation::FetchCommand => 3,
        Operation::Record => 4,
        Operation::InstallDecisionProof => 5,
        Operation::InspectDecisionProof => 6,
        Operation::InspectRecordSummary => 7,
        Operation::ObserveReadFence => 8,
    }
}

fn operation_from_tag(tag: u8) -> Result<Operation, String> {
    match tag {
        1 => Ok(Operation::Identity),
        2 => Ok(Operation::StoreCommand),
        3 => Ok(Operation::FetchCommand),
        4 => Ok(Operation::Record),
        5 => Ok(Operation::InstallDecisionProof),
        6 => Ok(Operation::InspectDecisionProof),
        7 => Ok(Operation::InspectRecordSummary),
        8 => Ok(Operation::ObserveReadFence),
        _ => Err("invalid rkyv recorder operation".into()),
    }
}

fn encode_envelope(
    kind: u8,
    operation: Option<Operation>,
    status: u8,
    version: u16,
    request_id: u64,
    remaining_deadline_ms: u32,
    body: &[u8],
) -> Result<Vec<u8>, String> {
    let body_len = u32::try_from(body.len()).map_err(|_| "rkyv recorder archive is too large")?;
    let mut bytes = Vec::with_capacity(HEADER_LEN + body.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&ARCHIVE_VERSION.to_be_bytes());
    bytes.push(kind);
    bytes.push(operation.map(operation_tag).unwrap_or(0));
    bytes.push(status);
    bytes.push(0);
    bytes.extend_from_slice(&version.to_be_bytes());
    bytes.extend_from_slice(&request_id.to_be_bytes());
    bytes.extend_from_slice(&remaining_deadline_ms.to_be_bytes());
    bytes.extend_from_slice(&body_len.to_be_bytes());
    bytes.extend_from_slice(body);
    Ok(bytes)
}

struct Envelope {
    operation: Option<Operation>,
    status: u8,
    version: u16,
    request_id: u64,
    remaining_deadline_ms: u32,
    body: AlignedVec,
}

fn decode_envelope(network: &[u8], expected_kind: u8) -> Result<Envelope, String> {
    if network.len() < HEADER_LEN || &network[..4] != MAGIC {
        return Err("invalid rkyv recorder archive header".into());
    }
    if u16::from_be_bytes([network[4], network[5]]) != ARCHIVE_VERSION
        || network[6] != expected_kind
        || network[9] != 0
    {
        return Err("invalid rkyv recorder archive header".into());
    }
    let declared = usize::try_from(u32::from_be_bytes([
        network[24],
        network[25],
        network[26],
        network[27],
    ]))
    .unwrap_or(usize::MAX);
    if declared != network.len() - HEADER_LEN {
        return Err("invalid rkyv recorder archive length".into());
    }
    let mut aligned: AlignedVec = AlignedVec::with_capacity(declared);
    aligned.extend_from_slice(&network[HEADER_LEN..]);
    let operation = match expected_kind {
        KIND_REQUEST | KIND_RESPONSE => Some(operation_from_tag(network[7])?),
        _ if network[7] == 0 => None,
        _ => return Err("invalid rkyv recorder operation".into()),
    };
    let request_id = u64::from_be_bytes(network[12..20].try_into().unwrap());
    let remaining_deadline_ms = u32::from_be_bytes(network[20..24].try_into().unwrap());
    if matches!(expected_kind, KIND_HELLO | KIND_HELLO_REPLY)
        && (request_id != 0 || remaining_deadline_ms != 0)
    {
        return Err("invalid rkyv recorder handshake envelope".into());
    }
    Ok(Envelope {
        operation,
        status: network[8],
        version: u16::from_be_bytes([network[10], network[11]]),
        request_id,
        remaining_deadline_ms,
        body: aligned,
    })
}

pub(super) fn encode_hello(value: &Hello) -> Result<Vec<u8>, String> {
    encode_envelope(
        KIND_HELLO,
        None,
        STATUS_OK,
        value.version,
        0,
        0,
        &archive(&WireHello::from(value))?,
    )
}
pub(super) fn decode_hello(bytes: &[u8]) -> Result<Hello, String> {
    let envelope = decode_envelope(bytes, KIND_HELLO)?;
    if envelope.status != STATUS_OK || envelope.body.is_empty() {
        return Err("invalid rkyv recorder hello envelope".into());
    }
    let archived = rkyv::access::<ArchivedWireHello, RkyvError>(&envelope.body)
        .map_err(|error| error.to_string())?;
    check_string(archived.node_id.len())?;
    check_string(archived.token.len())?;
    let hello: Hello = rkyv::deserialize::<WireHello, RkyvError>(archived)
        .map_err(|error| error.to_string())?
        .into();
    if hello.version != envelope.version {
        return Err("rkyv recorder hello version mismatch".into());
    }
    Ok(hello)
}
pub(super) fn encode_hello_reply(value: &HelloReply) -> Result<Vec<u8>, String> {
    match value {
        HelloReply::Accepted {
            version,
            recorder_id,
        } => encode_envelope(
            KIND_HELLO_REPLY,
            None,
            STATUS_OK,
            *version,
            0,
            0,
            &archive(recorder_id)?,
        ),
        HelloReply::Rejected => {
            encode_envelope(KIND_HELLO_REPLY, None, STATUS_REJECTED, 0, 0, 0, &[])
        }
    }
}
pub(super) fn decode_hello_reply(bytes: &[u8]) -> Result<HelloReply, String> {
    let envelope = decode_envelope(bytes, KIND_HELLO_REPLY)?;
    match envelope.status {
        STATUS_OK => {
            let archived = rkyv::access::<rkyv::string::ArchivedString, RkyvError>(&envelope.body)
                .map_err(|error| error.to_string())?;
            check_string(archived.len())?;
            Ok(HelloReply::Accepted {
                version: envelope.version,
                recorder_id: archived.as_str().to_owned(),
            })
        }
        STATUS_REJECTED if envelope.body.is_empty() => Ok(HelloReply::Rejected),
        _ => Err("invalid rkyv recorder hello reply envelope".into()),
    }
}
pub(super) fn encode_request(value: &RequestFrame) -> Result<Vec<u8>, String> {
    let (operation, body) = encode_request_body(&value.body)?;
    encode_envelope(
        KIND_REQUEST,
        Some(operation),
        STATUS_OK,
        value.version,
        value.request_id,
        value.remaining_deadline_ms,
        &body,
    )
}

pub(super) struct RequestPreflight {
    body: AlignedVec,
    pub(super) version: u16,
    pub(super) request_id: u64,
    pub(super) remaining_deadline_ms: u32,
    pub(super) operation: Operation,
}

pub(super) fn preflight_request(bytes: &[u8]) -> Result<RequestPreflight, String> {
    let envelope = decode_envelope(bytes, KIND_REQUEST)?;
    if envelope.status != STATUS_OK {
        return Err("invalid rkyv recorder request status".into());
    }
    let operation = envelope.operation.unwrap();
    check_request_body(operation, &envelope.body)?;
    Ok(RequestPreflight {
        version: envelope.version,
        request_id: envelope.request_id,
        remaining_deadline_ms: envelope.remaining_deadline_ms,
        operation,
        body: envelope.body,
    })
}

pub(super) fn materialize_request(preflight: RequestPreflight) -> Result<RequestFrame, String> {
    #[cfg(test)]
    REQUEST_MATERIALIZATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(RequestFrame {
        version: preflight.version,
        request_id: preflight.request_id,
        remaining_deadline_ms: preflight.remaining_deadline_ms,
        body: decode_request_body(preflight.operation, &preflight.body)?,
    })
}
pub(super) fn encode_response(value: &ResponseFrame) -> Result<Vec<u8>, String> {
    let (operation, status, body) = encode_response_body(&value.body)?;
    encode_envelope(
        KIND_RESPONSE,
        Some(operation),
        status,
        value.version,
        value.request_id,
        0,
        &body,
    )
}
pub(super) fn decode_response(bytes: &[u8]) -> Result<ResponseFrame, String> {
    let envelope = decode_envelope(bytes, KIND_RESPONSE)?;
    if envelope.remaining_deadline_ms != 0 {
        return Err("invalid rkyv recorder response envelope".into());
    }
    Ok(ResponseFrame {
        version: envelope.version,
        request_id: envelope.request_id,
        body: decode_response_body(envelope.operation.unwrap(), envelope.status, &envelope.body)?,
    })
}

fn check_string(len: usize) -> Result<(), String> {
    if len > MAX_REQUEST_ID_BYTES {
        Err("rkyv recorder string exceeds allocation limit".into())
    } else {
        Ok(())
    }
}

fn check_proposal(value: &ArchivedWireProposal) -> Result<(), String> {
    check_string(value.proposer_id.len())
}

fn check_record_request(value: &ArchivedWireRecordRequest) -> Result<(), String> {
    check_string(value.cluster_id.len())?;
    check_proposal(&value.proposal)?;
    if let Some(command) = value.command.as_ref() {
        if command.payload.len() > MAX_COMMAND_BYTES {
            return Err("rkyv recorder command exceeds allocation limit".into());
        }
    }
    Ok(())
}

fn check_decision_proof(value: &ArchivedWireDecisionProof) -> Result<(), String> {
    let (cluster_id, proposal, summaries) = match value {
        ArchivedWireDecisionProof::FastPath {
            cluster_id,
            proposal,
            summaries,
            ..
        }
        | ArchivedWireDecisionProof::Phase2 {
            cluster_id,
            proposal,
            summaries,
            ..
        } => (cluster_id, proposal, summaries),
    };
    check_string(cluster_id.len())?;
    check_proposal(proposal)?;
    if summaries.len() > DEFAULT_PEER_CONCURRENCY {
        return Err("rkyv recorder collection exceeds allocation limit".into());
    }
    for summary in summaries.iter() {
        check_string(summary.recorder_id.len())?;
        if let Some(proposal) = summary.first_current.as_ref() {
            check_proposal(proposal)?;
        }
        if let Some(proposal) = summary.aggregate_prior.as_ref() {
            check_proposal(proposal)?;
        }
    }
    Ok(())
}

fn check_record_summary(value: &ArchivedWireRecordSummary) -> Result<(), String> {
    check_string(value.recorder_id.len())?;
    if let Some(proposal) = value.first_current.as_ref() {
        check_proposal(proposal)?;
    }
    if let Some(proposal) = value.aggregate_prior.as_ref() {
        check_proposal(proposal)?;
    }
    if let Some(proof) = value.decided.as_ref() {
        check_decision_proof(proof)?;
    }
    Ok(())
}

#[cfg(test)]
static REQUEST_MATERIALIZATIONS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
pub(super) fn request_materializations() -> usize {
    REQUEST_MATERIALIZATIONS.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
pub(super) fn reset_request_materializations() {
    REQUEST_MATERIALIZATIONS.store(0, std::sync::atomic::Ordering::Relaxed);
}

impl From<&Hello> for WireHello {
    fn from(value: &Hello) -> Self {
        Self {
            version: value.version,
            node_id: value.node_id.clone(),
            recovery_generation: value.recovery_generation,
            token: value.token.clone(),
        }
    }
}
impl From<WireHello> for Hello {
    fn from(value: WireHello) -> Self {
        Self {
            version: value.version,
            node_id: value.node_id,
            recovery_generation: value.recovery_generation,
            token: value.token,
        }
    }
}
impl From<LogHash> for WireHash {
    fn from(value: LogHash) -> Self {
        Self(*value.as_bytes())
    }
}
impl From<WireHash> for LogHash {
    fn from(value: WireHash) -> Self {
        Self::from_bytes(value.0)
    }
}
impl From<EntryType> for WireEntryType {
    fn from(value: EntryType) -> Self {
        match value {
            EntryType::Command => Self::Command,
            EntryType::ConfigChange => Self::ConfigChange,
            EntryType::SnapshotBarrier => Self::SnapshotBarrier,
            EntryType::SnapshotPublished => Self::SnapshotPublished,
            EntryType::Noop => Self::Noop,
        }
    }
}
impl From<WireEntryType> for EntryType {
    fn from(value: WireEntryType) -> Self {
        match value {
            WireEntryType::Command => Self::Command,
            WireEntryType::ConfigChange => Self::ConfigChange,
            WireEntryType::SnapshotBarrier => Self::SnapshotBarrier,
            WireEntryType::SnapshotPublished => Self::SnapshotPublished,
            WireEntryType::Noop => Self::Noop,
        }
    }
}
impl From<&StoredCommand> for WireStoredCommand {
    fn from(value: &StoredCommand) -> Self {
        Self {
            entry_type: value.entry_type.into(),
            payload: value.payload.clone(),
        }
    }
}
impl From<WireStoredCommand> for StoredCommand {
    fn from(value: WireStoredCommand) -> Self {
        Self::new(value.entry_type.into(), value.payload)
    }
}
impl From<&AcceptedValue> for WireAcceptedValue {
    fn from(value: &AcceptedValue) -> Self {
        Self {
            command_hash: value.command_hash.into(),
            prev_hash: value.prev_hash.into(),
            entry_hash: value.entry_hash.into(),
        }
    }
}
impl From<WireAcceptedValue> for AcceptedValue {
    fn from(value: WireAcceptedValue) -> Self {
        Self {
            command_hash: value.command_hash.into(),
            prev_hash: value.prev_hash.into(),
            entry_hash: value.entry_hash.into(),
        }
    }
}
impl From<&Proposal> for WireProposal {
    fn from(value: &Proposal) -> Self {
        Self {
            priority: value.priority.0,
            proposer_id: value.proposer_id.clone(),
            proposal_id: value.proposal_id,
            value: value.value.as_ref().map(Into::into),
        }
    }
}
impl From<WireProposal> for Proposal {
    fn from(value: WireProposal) -> Self {
        Self {
            priority: ProposalPriority(value.priority),
            proposer_id: value.proposer_id,
            proposal_id: value.proposal_id,
            value: value.value.map(Into::into),
        }
    }
}
impl From<&RecorderSummary> for WireRecorderSummary {
    fn from(value: &RecorderSummary) -> Self {
        Self {
            recorder_id: value.recorder_id.clone(),
            slot: value.slot,
            step: value.step,
            first_current: value.first_current.as_ref().map(Into::into),
            aggregate_prior: value.aggregate_prior.as_ref().map(Into::into),
        }
    }
}
impl From<WireRecorderSummary> for RecorderSummary {
    fn from(value: WireRecorderSummary) -> Self {
        Self {
            recorder_id: value.recorder_id,
            slot: value.slot,
            step: value.step,
            first_current: value.first_current.map(Into::into),
            aggregate_prior: value.aggregate_prior.map(Into::into),
        }
    }
}
impl From<&DecisionProof> for WireDecisionProof {
    fn from(value: &DecisionProof) -> Self {
        match value {
            DecisionProof::FastPath {
                cluster_id,
                slot,
                epoch,
                config_id,
                config_digest,
                proposal,
                summaries,
            } => Self::FastPath {
                cluster_id: cluster_id.clone(),
                slot: *slot,
                epoch: *epoch,
                config_id: *config_id,
                config_digest: (*config_digest).into(),
                proposal: proposal.into(),
                summaries: summaries.iter().map(Into::into).collect(),
            },
            DecisionProof::Phase2 {
                cluster_id,
                slot,
                epoch,
                config_id,
                config_digest,
                step,
                proposal,
                summaries,
            } => Self::Phase2 {
                cluster_id: cluster_id.clone(),
                slot: *slot,
                epoch: *epoch,
                config_id: *config_id,
                config_digest: (*config_digest).into(),
                step: *step,
                proposal: proposal.into(),
                summaries: summaries.iter().map(Into::into).collect(),
            },
        }
    }
}
impl From<WireDecisionProof> for DecisionProof {
    fn from(value: WireDecisionProof) -> Self {
        match value {
            WireDecisionProof::FastPath {
                cluster_id,
                slot,
                epoch,
                config_id,
                config_digest,
                proposal,
                summaries,
            } => Self::FastPath {
                cluster_id,
                slot,
                epoch,
                config_id,
                config_digest: config_digest.into(),
                proposal: proposal.into(),
                summaries: summaries.into_iter().map(Into::into).collect(),
            },
            WireDecisionProof::Phase2 {
                cluster_id,
                slot,
                epoch,
                config_id,
                config_digest,
                step,
                proposal,
                summaries,
            } => Self::Phase2 {
                cluster_id,
                slot,
                epoch,
                config_id,
                config_digest: config_digest.into(),
                step,
                proposal: proposal.into(),
                summaries: summaries.into_iter().map(Into::into).collect(),
            },
        }
    }
}
impl From<&RecordRequest> for WireRecordRequest {
    fn from(value: &RecordRequest) -> Self {
        Self {
            cluster_id: value.cluster_id.clone(),
            epoch: value.epoch,
            config_id: value.config_id,
            config_digest: value.config_digest.into(),
            slot: value.slot,
            step: value.step,
            proposal: (&value.proposal).into(),
            command: value.command.as_ref().map(Into::into),
        }
    }
}
impl From<WireRecordRequest> for RecordRequest {
    fn from(value: WireRecordRequest) -> Self {
        Self {
            cluster_id: value.cluster_id,
            epoch: value.epoch,
            config_id: value.config_id,
            config_digest: value.config_digest.into(),
            slot: value.slot,
            step: value.step,
            proposal: value.proposal.into(),
            command: value.command.map(Into::into),
        }
    }
}
impl From<&RecordSummary> for WireRecordSummary {
    fn from(value: &RecordSummary) -> Self {
        Self {
            recorder_id: value.recorder_id.clone(),
            slot: value.slot,
            config_id: value.config_id,
            config_digest: value.config_digest.into(),
            step: value.step,
            first_current: value.first_current.as_ref().map(Into::into),
            aggregate_prior: value.aggregate_prior.as_ref().map(Into::into),
            decided: value.decided.as_ref().map(Into::into),
        }
    }
}
impl From<WireRecordSummary> for RecordSummary {
    fn from(value: WireRecordSummary) -> Self {
        Self {
            recorder_id: value.recorder_id,
            slot: value.slot,
            config_id: value.config_id,
            config_digest: value.config_digest.into(),
            step: value.step,
            first_current: value.first_current.map(Into::into),
            aggregate_prior: value.aggregate_prior.map(Into::into),
            decided: value.decided.map(Into::into),
        }
    }
}
impl From<&ReadFenceRequest> for WireReadFenceRequest {
    fn from(value: &ReadFenceRequest) -> Self {
        Self {
            cluster_id: value.cluster_id.clone(),
            epoch: value.epoch,
            config_id: value.config_id,
            config_digest: value.config_digest.into(),
            slot: value.slot,
        }
    }
}
impl From<WireReadFenceRequest> for ReadFenceRequest {
    fn from(value: WireReadFenceRequest) -> Self {
        Self {
            cluster_id: value.cluster_id,
            epoch: value.epoch,
            config_id: value.config_id,
            config_digest: value.config_digest.into(),
            slot: value.slot,
        }
    }
}
impl From<&ReadFenceSlotState> for WireReadFenceSlotState {
    fn from(value: &ReadFenceSlotState) -> Self {
        match value {
            ReadFenceSlotState::Empty => Self::Empty,
            ReadFenceSlotState::Occupied { summary } => Self::Occupied {
                summary: summary
                    .as_ref()
                    .map(|value| Box::new(value.as_ref().into())),
            },
        }
    }
}
impl From<WireReadFenceSlotState> for ReadFenceSlotState {
    fn from(value: WireReadFenceSlotState) -> Self {
        match value {
            WireReadFenceSlotState::Empty => Self::Empty,
            WireReadFenceSlotState::Occupied { summary } => Self::Occupied {
                summary: summary.map(|value| Box::new((*value).into())),
            },
        }
    }
}
impl From<&ReadFenceObservation> for WireReadFenceObservation {
    fn from(value: &ReadFenceObservation) -> Self {
        Self {
            recorder_id: value.recorder_id.clone(),
            cluster_id: value.cluster_id.clone(),
            epoch: value.epoch,
            config_id: value.config_id,
            config_digest: value.config_digest.into(),
            slot: value.slot,
            max_head: value.max_head,
            slot_state: (&value.slot_state).into(),
        }
    }
}
impl From<WireReadFenceObservation> for ReadFenceObservation {
    fn from(value: WireReadFenceObservation) -> Self {
        Self {
            recorder_id: value.recorder_id,
            cluster_id: value.cluster_id,
            epoch: value.epoch,
            config_id: value.config_id,
            config_digest: value.config_digest.into(),
            slot: value.slot,
            max_head: value.max_head,
            slot_state: value.slot_state.into(),
        }
    }
}
impl From<&Ballot> for WireBallot {
    fn from(value: &Ballot) -> Self {
        Self {
            round: value.round,
            priority: value.priority,
            proposer_id: value.proposer_id.clone(),
        }
    }
}
impl From<WireBallot> for Ballot {
    fn from(value: WireBallot) -> Self {
        Self {
            round: value.round,
            priority: value.priority,
            proposer_id: value.proposer_id,
        }
    }
}

macro_rules! reject_conversions {
    ($($variant:ident),* $(,)?) => {
        impl From<&RejectReason> for WireRejectReason {
            fn from(value: &RejectReason) -> Self {
                match value {
                    $(RejectReason::$variant => Self::$variant,)*
                    RejectReason::BallotPromised { promised } => Self::BallotPromised { promised: promised.into() },
                    RejectReason::ConfigurationSealed { stop_slot } => Self::ConfigurationSealed { stop_slot: *stop_slot },
                }
            }
        }
        impl From<WireRejectReason> for RejectReason {
            fn from(value: WireRejectReason) -> Self {
                match value {
                    $(WireRejectReason::$variant => Self::$variant,)*
                    WireRejectReason::BallotPromised { promised } => Self::BallotPromised { promised: promised.into() },
                    WireRejectReason::ConfigurationSealed { stop_slot } => Self::ConfigurationSealed { stop_slot },
                }
            }
        }
    };
}
reject_conversions!(
    StaleEpoch,
    FutureEpoch,
    WrongCluster,
    WrongConfig,
    WrongSlot,
    AlreadyDecided,
    MalformedDecision,
    ConflictingValue,
    InvalidValue,
    InvalidCertificate,
    ConfigurationNotInstalled,
    ActivationRequired,
    TransitionInProgress,
    InvalidTransition,
    LocalVoterRequired,
    StepRegression,
    InvalidRequest,
);

fn encode_result<T>(
    value: &RpcResult<T>,
    encode_ok: impl FnOnce(&T) -> Result<AlignedVec, String>,
) -> Result<(u8, AlignedVec), String> {
    match value {
        RpcResult::Ok(value) => Ok((STATUS_OK, encode_ok(value)?)),
        RpcResult::Rejected(reason) => {
            Ok((STATUS_REJECTED, archive(&WireRejectReason::from(reason))?))
        }
        RpcResult::Error(message) => Ok((STATUS_ERROR, archive(message)?)),
        RpcResult::Overloaded => Ok((STATUS_OVERLOADED, AlignedVec::new())),
    }
}

fn decode_failure<T>(status: u8, body: &AlignedVec) -> Result<Option<RpcResult<T>>, String> {
    match status {
        STATUS_OK => Ok(None),
        STATUS_REJECTED => {
            let archived = rkyv::access::<ArchivedWireRejectReason, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            if let ArchivedWireRejectReason::BallotPromised { promised } = archived {
                check_string(promised.proposer_id.len())?;
            }
            Ok(Some(RpcResult::Rejected(
                rkyv::deserialize::<WireRejectReason, RkyvError>(archived)
                    .map_err(|error| error.to_string())?
                    .into(),
            )))
        }
        STATUS_ERROR => {
            let archived = rkyv::access::<rkyv::string::ArchivedString, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            check_string(archived.len())?;
            Ok(Some(RpcResult::Error(archived.as_str().to_owned())))
        }
        STATUS_OVERLOADED if body.is_empty() => Ok(Some(RpcResult::Overloaded)),
        _ => Err("invalid rkyv recorder response status".into()),
    }
}

fn encode_request_body(value: &RecorderRequestBody) -> Result<(Operation, AlignedVec), String> {
    match value {
        RecorderRequestBody::Identity => Ok((Operation::Identity, AlignedVec::new())),
        RecorderRequestBody::StoreCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
            command,
        } => Ok((
            Operation::StoreCommand,
            archive(&WireStoreCommandRequest {
                cluster_id: cluster_id.clone(),
                epoch: *epoch,
                config_id: *config_id,
                config_digest: (*config_digest).into(),
                command_hash: (*command_hash).into(),
                command: command.into(),
            })?,
        )),
        RecorderRequestBody::FetchCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
        } => Ok((
            Operation::FetchCommand,
            archive(&WireFetchCommandRequest {
                cluster_id: cluster_id.clone(),
                epoch: *epoch,
                config_id: *config_id,
                config_digest: (*config_digest).into(),
                command_hash: (*command_hash).into(),
            })?,
        )),
        RecorderRequestBody::Record(value) => {
            Ok((Operation::Record, archive(&WireRecordRequest::from(value))?))
        }
        RecorderRequestBody::InstallDecisionProof { proof, members } => Ok((
            Operation::InstallDecisionProof,
            archive(&WireInstallDecisionProofRequest {
                proof: proof.into(),
                members: members.clone(),
            })?,
        )),
        RecorderRequestBody::InspectDecisionProof { slot } => {
            Ok((Operation::InspectDecisionProof, archive(slot)?))
        }
        RecorderRequestBody::InspectRecordSummary { slot } => {
            Ok((Operation::InspectRecordSummary, archive(slot)?))
        }
        RecorderRequestBody::ObserveReadFence(value) => Ok((
            Operation::ObserveReadFence,
            archive(&WireReadFenceRequest::from(value))?,
        )),
    }
}

fn check_request_body(operation: Operation, body: &AlignedVec) -> Result<(), String> {
    match operation {
        Operation::Identity if body.is_empty() => Ok(()),
        Operation::Identity => Err("identity request body must be empty".into()),
        Operation::StoreCommand => {
            let value = rkyv::access::<ArchivedWireStoreCommandRequest, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            check_string(value.cluster_id.len())?;
            if value.command.payload.len() > MAX_COMMAND_BYTES {
                return Err("rkyv recorder command exceeds allocation limit".into());
            }
            Ok(())
        }
        Operation::FetchCommand => {
            let value = rkyv::access::<ArchivedWireFetchCommandRequest, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            check_string(value.cluster_id.len())
        }
        Operation::Record => {
            let value = rkyv::access::<ArchivedWireRecordRequest, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            check_record_request(value)
        }
        Operation::InstallDecisionProof => {
            let value = rkyv::access::<ArchivedWireInstallDecisionProofRequest, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            check_decision_proof(&value.proof)?;
            if value.members.len() > DEFAULT_PEER_CONCURRENCY {
                return Err("rkyv recorder collection exceeds allocation limit".into());
            }
            for member in value.members.iter() {
                check_string(member.len())?;
            }
            Ok(())
        }
        Operation::InspectDecisionProof | Operation::InspectRecordSummary => {
            rkyv::access::<rkyv::primitive::ArchivedU64, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            Ok(())
        }
        Operation::ObserveReadFence => {
            let value = rkyv::access::<ArchivedWireReadFenceRequest, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            check_string(value.cluster_id.len())
        }
    }
}

fn decode_request_body(
    operation: Operation,
    body: &AlignedVec,
) -> Result<RecorderRequestBody, String> {
    match operation {
        Operation::Identity => Ok(RecorderRequestBody::Identity),
        Operation::StoreCommand => {
            let value = rkyv::access::<ArchivedWireStoreCommandRequest, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            let value = rkyv::deserialize::<WireStoreCommandRequest, RkyvError>(value)
                .map_err(|error| error.to_string())?;
            Ok(RecorderRequestBody::StoreCommand {
                cluster_id: value.cluster_id,
                epoch: value.epoch,
                config_id: value.config_id,
                config_digest: value.config_digest.into(),
                command_hash: value.command_hash.into(),
                command: value.command.into(),
            })
        }
        Operation::FetchCommand => {
            let value = rkyv::access::<ArchivedWireFetchCommandRequest, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            let value = rkyv::deserialize::<WireFetchCommandRequest, RkyvError>(value)
                .map_err(|error| error.to_string())?;
            Ok(RecorderRequestBody::FetchCommand {
                cluster_id: value.cluster_id,
                epoch: value.epoch,
                config_id: value.config_id,
                config_digest: value.config_digest.into(),
                command_hash: value.command_hash.into(),
            })
        }
        Operation::Record => {
            let value = rkyv::access::<ArchivedWireRecordRequest, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            Ok(RecorderRequestBody::Record(
                rkyv::deserialize::<WireRecordRequest, RkyvError>(value)
                    .map_err(|error| error.to_string())?
                    .into(),
            ))
        }
        Operation::InstallDecisionProof => {
            let value = rkyv::access::<ArchivedWireInstallDecisionProofRequest, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            let value = rkyv::deserialize::<WireInstallDecisionProofRequest, RkyvError>(value)
                .map_err(|error| error.to_string())?;
            Ok(RecorderRequestBody::InstallDecisionProof {
                proof: value.proof.into(),
                members: value.members,
            })
        }
        Operation::InspectDecisionProof | Operation::InspectRecordSummary => {
            let slot = rkyv::access::<rkyv::primitive::ArchivedU64, RkyvError>(body)
                .map_err(|error| error.to_string())?
                .to_native();
            if operation == Operation::InspectDecisionProof {
                Ok(RecorderRequestBody::InspectDecisionProof { slot })
            } else {
                Ok(RecorderRequestBody::InspectRecordSummary { slot })
            }
        }
        Operation::ObserveReadFence => {
            let value = rkyv::access::<ArchivedWireReadFenceRequest, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            Ok(RecorderRequestBody::ObserveReadFence(
                rkyv::deserialize::<WireReadFenceRequest, RkyvError>(value)
                    .map_err(|error| error.to_string())?
                    .into(),
            ))
        }
    }
}

fn encode_response_body(
    value: &RecorderResponseBody,
) -> Result<(Operation, u8, AlignedVec), String> {
    let (operation, encoded) = match value {
        RecorderResponseBody::Identity(result) => {
            (Operation::Identity, encode_result(result, archive)?)
        }
        RecorderResponseBody::StoreCommand(result) => (
            Operation::StoreCommand,
            encode_result(result, |_| Ok(AlignedVec::new()))?,
        ),
        RecorderResponseBody::FetchCommand(result) => (
            Operation::FetchCommand,
            encode_result(result, |value| {
                archive(&value.as_ref().map(WireStoredCommand::from))
            })?,
        ),
        RecorderResponseBody::Record(result) => (
            Operation::Record,
            encode_result(result, |value| archive(&WireRecordSummary::from(value)))?,
        ),
        RecorderResponseBody::InstallDecisionProof(result) => (
            Operation::InstallDecisionProof,
            encode_result(result, |_| Ok(AlignedVec::new()))?,
        ),
        RecorderResponseBody::InspectDecisionProof(result) => (
            Operation::InspectDecisionProof,
            encode_result(result, |value| {
                archive(&value.as_ref().map(WireDecisionProof::from))
            })?,
        ),
        RecorderResponseBody::InspectRecordSummary(result) => (
            Operation::InspectRecordSummary,
            encode_result(result, |value| {
                archive(&value.as_ref().map(WireRecordSummary::from))
            })?,
        ),
        RecorderResponseBody::ObserveReadFence(result) => (
            Operation::ObserveReadFence,
            encode_result(result, |value| {
                archive(&WireReadFenceObservation::from(value))
            })?,
        ),
    };
    Ok((operation, encoded.0, encoded.1))
}

fn decode_response_body(
    operation: Operation,
    status: u8,
    body: &AlignedVec,
) -> Result<RecorderResponseBody, String> {
    macro_rules! failure {
        ($variant:ident) => {
            if let Some(value) = decode_failure(status, body)? {
                return Ok(RecorderResponseBody::$variant(value));
            }
        };
    }
    match operation {
        Operation::Identity => {
            failure!(Identity);
            let value = rkyv::access::<rkyv::string::ArchivedString, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            check_string(value.len())?;
            Ok(RecorderResponseBody::Identity(RpcResult::Ok(
                value.as_str().to_owned(),
            )))
        }
        Operation::StoreCommand => {
            failure!(StoreCommand);
            if !body.is_empty() {
                return Err("unit response body must be empty".into());
            }
            Ok(RecorderResponseBody::StoreCommand(RpcResult::Ok(())))
        }
        Operation::FetchCommand => {
            failure!(FetchCommand);
            let value = rkyv::access::<
                rkyv::option::ArchivedOption<ArchivedWireStoredCommand>,
                RkyvError,
            >(body)
            .map_err(|error| error.to_string())?;
            if let rkyv::option::ArchivedOption::Some(command) = value {
                if command.payload.len() > MAX_COMMAND_BYTES {
                    return Err("rkyv recorder command exceeds allocation limit".into());
                }
            }
            let value = rkyv::deserialize::<Option<WireStoredCommand>, RkyvError>(value)
                .map_err(|error| error.to_string())?;
            Ok(RecorderResponseBody::FetchCommand(RpcResult::Ok(
                value.map(Into::into),
            )))
        }
        Operation::Record => {
            failure!(Record);
            let value = rkyv::access::<ArchivedWireRecordSummary, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            check_record_summary(value)?;
            Ok(RecorderResponseBody::Record(RpcResult::Ok(
                rkyv::deserialize::<WireRecordSummary, RkyvError>(value)
                    .map_err(|error| error.to_string())?
                    .into(),
            )))
        }
        Operation::InstallDecisionProof => {
            failure!(InstallDecisionProof);
            if !body.is_empty() {
                return Err("unit response body must be empty".into());
            }
            Ok(RecorderResponseBody::InstallDecisionProof(
                RpcResult::Ok(()),
            ))
        }
        Operation::InspectDecisionProof => {
            failure!(InspectDecisionProof);
            let value = rkyv::access::<
                rkyv::option::ArchivedOption<ArchivedWireDecisionProof>,
                RkyvError,
            >(body)
            .map_err(|error| error.to_string())?;
            if let rkyv::option::ArchivedOption::Some(proof) = value {
                check_decision_proof(proof)?;
            }
            let value = rkyv::deserialize::<Option<WireDecisionProof>, RkyvError>(value)
                .map_err(|error| error.to_string())?;
            Ok(RecorderResponseBody::InspectDecisionProof(RpcResult::Ok(
                value.map(Into::into),
            )))
        }
        Operation::InspectRecordSummary => {
            failure!(InspectRecordSummary);
            let value = rkyv::access::<
                rkyv::option::ArchivedOption<ArchivedWireRecordSummary>,
                RkyvError,
            >(body)
            .map_err(|error| error.to_string())?;
            if let rkyv::option::ArchivedOption::Some(summary) = value {
                check_record_summary(summary)?;
            }
            let value = rkyv::deserialize::<Option<WireRecordSummary>, RkyvError>(value)
                .map_err(|error| error.to_string())?;
            Ok(RecorderResponseBody::InspectRecordSummary(RpcResult::Ok(
                value.map(Into::into),
            )))
        }
        Operation::ObserveReadFence => {
            failure!(ObserveReadFence);
            let value = rkyv::access::<ArchivedWireReadFenceObservation, RkyvError>(body)
                .map_err(|error| error.to_string())?;
            check_string(value.recorder_id.len())?;
            check_string(value.cluster_id.len())?;
            if let ArchivedWireReadFenceSlotState::Occupied {
                summary: rkyv::option::ArchivedOption::Some(summary),
            } = &value.slot_state
            {
                check_record_summary(summary)?;
            }
            Ok(RecorderResponseBody::ObserveReadFence(RpcResult::Ok(
                rkyv::deserialize::<WireReadFenceObservation, RkyvError>(value)
                    .map_err(|error| error.to_string())?
                    .into(),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_decode_accepts_unaligned_network_slice_and_rejects_malformed_or_trailing_bytes() {
        let encoded = encode_hello(&Hello {
            version: 6,
            node_id: "node-1".into(),
            recovery_generation: 7,
            token: "token".into(),
        })
        .unwrap();
        let mut unaligned = vec![0xff];
        unaligned.extend_from_slice(&encoded);
        assert_eq!(decode_hello(&unaligned[1..]).unwrap().node_id, "node-1");

        let mut malformed = encoded.clone();
        malformed.pop();
        let shorter = u32::try_from(malformed.len() - HEADER_LEN).unwrap();
        malformed[24..28].copy_from_slice(&shorter.to_be_bytes());
        assert!(decode_hello(&malformed).is_err());

        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_hello(&trailing).is_err());

        let oversized = encode_hello(&Hello {
            version: 6,
            node_id: "x".repeat(MAX_REQUEST_ID_BYTES + 1),
            recovery_generation: 7,
            token: "token".into(),
        })
        .unwrap();
        assert!(decode_hello(&oversized).is_err());
    }

    #[test]
    fn compact_envelope_uses_empty_body_for_identity_request_and_unit_ack() {
        let identity = encode_request(&RequestFrame {
            version: 6,
            request_id: 11,
            remaining_deadline_ms: 500,
            body: RecorderRequestBody::Identity,
        })
        .unwrap();
        assert_eq!(identity.len(), HEADER_LEN);
        assert_eq!(&identity[24..28], &[0, 0, 0, 0]);

        let ack = encode_response(&ResponseFrame {
            version: 6,
            request_id: 11,
            body: RecorderResponseBody::StoreCommand(RpcResult::Ok(())),
        })
        .unwrap();
        assert_eq!(ack.len(), HEADER_LEN);
        assert_eq!(&ack[24..28], &[0, 0, 0, 0]);
        assert!(matches!(
            decode_response(&ack).unwrap().body,
            RecorderResponseBody::StoreCommand(RpcResult::Ok(()))
        ));
    }

    #[test]
    fn envelope_rejects_wrong_schema_operation_and_archive_length() {
        let encoded = encode_request(&RequestFrame {
            version: 6,
            request_id: 17,
            remaining_deadline_ms: 500,
            body: RecorderRequestBody::InspectDecisionProof { slot: 3 },
        })
        .unwrap();

        let mut wrong_schema = encoded.clone();
        wrong_schema[5] = 1;
        assert!(preflight_request(&wrong_schema).is_err());

        let mut wrong_operation = encoded.clone();
        wrong_operation[7] = operation_tag(Operation::Identity);
        assert!(preflight_request(&wrong_operation).is_err());

        let mut wrong_length = encoded;
        wrong_length[27] = wrong_length[27].saturating_add(1);
        assert!(preflight_request(&wrong_length).is_err());
    }

    #[test]
    fn decoded_response_exposes_envelope_metadata_for_expected_value_validation() {
        let encoded = encode_response(&ResponseFrame {
            version: 6,
            request_id: 19,
            body: RecorderResponseBody::Identity(RpcResult::Ok("node-1".into())),
        })
        .unwrap();
        let decoded = decode_response(&encoded).unwrap();
        assert_eq!(decoded.version, 6);
        assert_eq!(decoded.request_id, 19);
        assert!(super::super::response_matches(
            Operation::Identity,
            &decoded.body
        ));
        assert!(!super::super::response_matches(
            Operation::StoreCommand,
            &decoded.body
        ));

        let mut wrong_request_id = encoded.clone();
        wrong_request_id[19] = 18;
        assert_ne!(decode_response(&wrong_request_id).unwrap().request_id, 19);

        let mut wrong_version = encoded;
        wrong_version[11] = 5;
        assert_ne!(decode_response(&wrong_version).unwrap().version, 6);
    }

    #[test]
    fn response_status_body_matrix_fails_closed() {
        let mut store_ok = encode_response(&ResponseFrame {
            version: 6,
            request_id: 1,
            body: RecorderResponseBody::StoreCommand(RpcResult::Ok(())),
        })
        .unwrap();
        store_ok.push(0);
        store_ok[24..28].copy_from_slice(&1_u32.to_be_bytes());
        assert!(decode_response(&store_ok).is_err());

        let mut install_ok = encode_response(&ResponseFrame {
            version: 6,
            request_id: 1,
            body: RecorderResponseBody::InstallDecisionProof(RpcResult::Ok(())),
        })
        .unwrap();
        install_ok.push(0);
        install_ok[24..28].copy_from_slice(&1_u32.to_be_bytes());
        assert!(decode_response(&install_ok).is_err());

        let mut overloaded = encode_response(&ResponseFrame {
            version: 6,
            request_id: 1,
            body: RecorderResponseBody::StoreCommand(RpcResult::Overloaded),
        })
        .unwrap();
        overloaded.push(0);
        overloaded[24..28].copy_from_slice(&1_u32.to_be_bytes());
        assert!(decode_response(&overloaded).is_err());

        let mut unknown_status = encode_response(&ResponseFrame {
            version: 6,
            request_id: 1,
            body: RecorderResponseBody::StoreCommand(RpcResult::Ok(())),
        })
        .unwrap();
        unknown_status[8] = 0xff;
        assert!(decode_response(&unknown_status).is_err());
    }

    #[test]
    fn hello_envelopes_reject_noncanonical_request_metadata() {
        let hello = encode_hello(&Hello {
            version: 6,
            node_id: "node-1".into(),
            recovery_generation: 7,
            token: "token".into(),
        })
        .unwrap();
        for offset in [19, 23] {
            let mut noncanonical = hello.clone();
            noncanonical[offset] = 1;
            assert!(decode_hello(&noncanonical).is_err());
        }

        let reply = encode_hello_reply(&HelloReply::Accepted {
            version: 6,
            recorder_id: "node-1".into(),
        })
        .unwrap();
        for offset in [19, 23] {
            let mut noncanonical = reply.clone();
            noncanonical[offset] = 1;
            assert!(decode_hello_reply(&noncanonical).is_err());
        }
    }
}
