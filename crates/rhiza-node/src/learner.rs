use std::{
    fmt,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use axum::{
    extract::{Json, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use rhiza_core::{
    ConfigChange, ConfigurationState, ExecutionProfile, LogAnchor, LogEntry, LogHash, StoredCommand,
};
use rhiza_log::{FileLogStore, LogStore};
use rhiza_quepaxa::{CertifiedDecisionInspection, DecisionProof, Membership, ThreeNodeConsensus};
use serde::{Deserialize, Serialize};

use crate::{
    durability::{
        adopt_finalized_successor_prestage, finalize_successor_prestage_for_stop,
        inspect_successor_prestage, SuccessorPrestage, SuccessorPrestageState,
        SuccessorRestorePreparation,
    },
    header_text, secrets_equal, valid_auth_token, valid_nonblank_header_value,
    validate_profile_entry_shape, Materializer, NodeConfig, NodeRuntime, StopInformation,
    RECOVERY_GENERATION_HEADER,
};

pub const CERTIFIED_TAIL_PATH: &str = "/v1/learner/certified-tail";
pub const TAIL_PROTOCOL_VERSION: &str = "1";
pub const TAIL_VERSION_HEADER: &str = "x-rhiza-tail-version";
pub const TAIL_CLUSTER_ID_HEADER: &str = "x-rhiza-tail-cluster-id";
pub const TAIL_EPOCH_HEADER: &str = "x-rhiza-tail-epoch";
pub const TAIL_CONFIG_ID_HEADER: &str = "x-rhiza-tail-config-id";
pub const TAIL_MEMBERSHIP_DIGEST_HEADER: &str = "x-rhiza-tail-membership-digest";
pub const MAX_CERTIFIED_TAIL_ENTRIES: u32 = 1_024;
pub const DEFAULT_CERTIFIED_TAIL_ENTRIES: u32 = 64;
pub const MAX_CERTIFIED_TAIL_ENCODED_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CERTIFIED_TAIL_ENTRY_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const MAX_CONCURRENT_CERTIFIED_TAIL_READS: usize = 2;
pub const CERTIFIED_TAIL_READ_TIMEOUT: Duration = Duration::from_secs(10);
const TAIL_AUTHORIZATION_SCHEME: &str = "Rhiza-Tail ";

#[derive(Clone, Eq, PartialEq)]
pub struct TailReaderConfig {
    cluster_id: String,
    epoch: u64,
    config_id: u64,
    membership: Membership,
    recovery_generation: u64,
    token: String,
}

impl TailReaderConfig {
    pub fn new(
        cluster_id: impl Into<String>,
        epoch: u64,
        config_id: u64,
        membership: Membership,
        recovery_generation: u64,
        token: impl Into<String>,
    ) -> Result<Self, TailReaderConfigError> {
        let cluster_id = cluster_id.into();
        if !valid_nonblank_header_value(&cluster_id) {
            return Err(TailReaderConfigError::InvalidClusterId);
        }
        if epoch == 0 {
            return Err(TailReaderConfigError::InvalidEpoch);
        }
        if config_id == 0 {
            return Err(TailReaderConfigError::InvalidConfigId);
        }
        if recovery_generation == 0 {
            return Err(TailReaderConfigError::InvalidRecoveryGeneration);
        }
        let token = token.into();
        if !valid_auth_token(&token) {
            return Err(TailReaderConfigError::InvalidToken);
        }
        Ok(Self {
            cluster_id,
            epoch,
            config_id,
            membership,
            recovery_generation,
            token,
        })
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn config_id(&self) -> u64 {
        self.config_id
    }

    pub const fn membership(&self) -> &Membership {
        &self.membership
    }

    pub const fn membership_digest(&self) -> LogHash {
        self.membership.digest()
    }

    pub const fn recovery_generation(&self) -> u64 {
        self.recovery_generation
    }
}

impl fmt::Debug for TailReaderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TailReaderConfig")
            .field("cluster_id", &self.cluster_id)
            .field("epoch", &self.epoch)
            .field("config_id", &self.config_id)
            .field("membership_digest", &self.membership.digest())
            .field("recovery_generation", &self.recovery_generation)
            .field("token", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TailReaderConfigError {
    InvalidClusterId,
    InvalidEpoch,
    InvalidConfigId,
    InvalidRecoveryGeneration,
    InvalidToken,
    ConsensusContextMismatch,
}

impl fmt::Display for TailReaderConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidClusterId => "tail reader cluster_id must be a nonblank HTTP header value",
            Self::InvalidEpoch => "tail reader epoch must be positive",
            Self::InvalidConfigId => "tail reader config_id must be positive",
            Self::InvalidRecoveryGeneration => "tail reader recovery_generation must be positive",
            Self::InvalidToken => "tail reader token must be nonblank and contain no whitespace",
            Self::ConsensusContextMismatch => {
                "tail reader membership and config_id must match consensus"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for TailReaderConfigError {}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CertifiedTailRequest {
    pub from: LogAnchor,
    pub max_entries: u32,
}

impl CertifiedTailRequest {
    pub const fn from_anchor(from: LogAnchor) -> Self {
        Self {
            from,
            max_entries: DEFAULT_CERTIFIED_TAIL_ENTRIES,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct CertifiedTailRecord {
    pub entry: LogEntry,
    pub proof: DecisionProof,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct CertifiedTailResponse {
    pub records: Vec<CertifiedTailRecord>,
    pub observed_tip: LogAnchor,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "error", rename_all = "snake_case", deny_unknown_fields)]
pub enum CertifiedTailErrorResponse {
    RebaseRequired { checkpoint: LogAnchor },
}

#[derive(Clone)]
struct TailReaderState {
    consensus: Arc<ThreeNodeConsensus>,
    log: TailLogSource,
    config: TailReaderConfig,
    admission: Arc<tokio::sync::Semaphore>,
}

#[derive(Clone)]
enum TailLogSource {
    Shared(Arc<FileLogStore>),
    Runtime(Arc<NodeRuntime>),
}

impl TailLogSource {
    fn store(&self) -> &FileLogStore {
        match self {
            Self::Shared(log) => log,
            Self::Runtime(runtime) => runtime.log_store(),
        }
    }
}

pub fn certified_tail_router(
    consensus: Arc<ThreeNodeConsensus>,
    log: Arc<FileLogStore>,
    config: TailReaderConfig,
) -> Result<Router, TailReaderConfigError> {
    build_certified_tail_router(consensus, TailLogSource::Shared(log), config)
}

pub fn certified_tail_router_for_runtime(
    runtime: Arc<NodeRuntime>,
    config: TailReaderConfig,
) -> Result<Router, TailReaderConfigError> {
    let consensus = Arc::clone(runtime.consensus());
    build_certified_tail_router(consensus, TailLogSource::Runtime(runtime), config)
}

fn build_certified_tail_router(
    consensus: Arc<ThreeNodeConsensus>,
    log: TailLogSource,
    config: TailReaderConfig,
) -> Result<Router, TailReaderConfigError> {
    if consensus.config_id() != config.config_id || consensus.membership() != &config.membership {
        return Err(TailReaderConfigError::ConsensusContextMismatch);
    }
    Ok(Router::new()
        .route(CERTIFIED_TAIL_PATH, post(handle_certified_tail))
        .with_state(TailReaderState {
            consensus,
            log,
            config,
            admission: Arc::new(tokio::sync::Semaphore::new(
                MAX_CONCURRENT_CERTIFIED_TAIL_READS,
            )),
        }))
}

async fn handle_certified_tail(
    State(state): State<TailReaderState>,
    headers: HeaderMap,
    request: Result<Json<CertifiedTailRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if !tail_reader_authenticated(&headers, &state.config) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Ok(Json(request)) = request else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if request.max_entries == 0
        || request.max_entries > MAX_CERTIFIED_TAIL_ENTRIES
        || (request.from.index() == 0 && request.from.hash() != LogHash::ZERO)
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let permit = match state.admission.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return StatusCode::TOO_MANY_REQUESTS.into_response(),
    };
    let cancellation = TailReadCancellation::new();
    let cancelled = cancellation.signal();
    let deadline = Instant::now() + CERTIFIED_TAIL_READ_TIMEOUT;
    let permit = Arc::new(permit);
    let worker_permit = Arc::clone(&permit);
    let task = tokio::task::spawn_blocking(move || {
        let _permit = worker_permit;
        read_certified_tail(&state, request, &cancelled, deadline)
    });
    let _permit = permit;
    match tokio::time::timeout(CERTIFIED_TAIL_READ_TIMEOUT, task).await {
        Ok(Ok(Ok(response))) => Json(response).into_response(),
        Ok(Ok(Err(TailReadError::DecisionMismatch))) => StatusCode::CONFLICT.into_response(),
        Ok(Ok(Err(TailReadError::ResponseTooLarge))) => {
            StatusCode::PAYLOAD_TOO_LARGE.into_response()
        }
        Ok(Ok(Err(TailReadError::RebaseRequired(checkpoint)))) => (
            StatusCode::CONFLICT,
            Json(CertifiedTailErrorResponse::RebaseRequired { checkpoint }),
        )
            .into_response(),
        Ok(Ok(Err(TailReadError::Unavailable))) | Err(_) => {
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
        Ok(Err(_)) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

fn read_certified_tail(
    state: &TailReaderState,
    request: CertifiedTailRequest,
    cancelled: &AtomicBool,
    deadline: Instant,
) -> Result<CertifiedTailResponse, TailReadError> {
    ensure_tail_read_active(cancelled, deadline)?;
    let logical = state
        .log
        .store()
        .logical_state()
        .map_err(|_| TailReadError::Unavailable)?;
    if let Some(checkpoint) = logical.anchor.as_ref().map(|anchor| *anchor.compacted()) {
        if request.from.index() < checkpoint.index() {
            return Err(TailReadError::RebaseRequired(checkpoint));
        }
    }
    let observed_tip = logical
        .tip
        .unwrap_or_else(|| LogAnchor::new(0, LogHash::ZERO));
    if request.from.index() > observed_tip.index()
        || (request.from.index() == observed_tip.index()
            && request.from.hash() != observed_tip.hash())
    {
        return Err(TailReadError::DecisionMismatch);
    }
    let available = observed_tip.index().saturating_sub(request.from.index());
    let count = available.min(u64::from(request.max_entries));
    let mut records = Vec::with_capacity(count as usize);
    let mut encoded_bytes = encoded_json_len(&CertifiedTailResponse {
        records: Vec::new(),
        observed_tip,
    })
    .map_err(|_| TailReadError::Unavailable)?;
    let mut previous = request.from;
    for offset in 1..=count {
        ensure_tail_read_active(cancelled, deadline)?;
        let index = request
            .from
            .index()
            .checked_add(offset)
            .ok_or(TailReadError::Unavailable)?;
        let Some(local) = state
            .log
            .store()
            .read(index)
            .map_err(|_| TailReadError::Unavailable)?
        else {
            break;
        };
        if local.payload.len() > MAX_CERTIFIED_TAIL_ENTRY_PAYLOAD_BYTES {
            return Err(TailReadError::ResponseTooLarge);
        }
        if local.cluster_id != state.config.cluster_id
            || local.epoch != state.config.epoch
            || local.config_id != state.config.config_id
            || local.prev_hash != previous.hash()
        {
            return Err(TailReadError::DecisionMismatch);
        }
        let inspection = state
            .consensus
            .inspect_certified_decision_at(index, local.prev_hash)
            .map_err(|_| TailReadError::Unavailable)?;
        let CertifiedDecisionInspection::Committed(certified) = inspection else {
            break;
        };
        ensure_tail_read_active(cancelled, deadline)?;
        certified
            .proof
            .validate_for_cluster(
                &state.config.cluster_id,
                index,
                state.config.epoch,
                state.config.config_id,
                &state.config.membership,
            )
            .map_err(|_| TailReadError::DecisionMismatch)?;
        if certified.entry != local {
            return Err(TailReadError::DecisionMismatch);
        }
        let record = CertifiedTailRecord {
            entry: local,
            proof: certified.proof,
        };
        let record_bytes = encoded_json_len(&record).map_err(|_| TailReadError::Unavailable)?;
        let next_encoded_bytes = encoded_bytes
            .checked_add(record_bytes)
            .and_then(|bytes| bytes.checked_add(usize::from(!records.is_empty())))
            .ok_or(TailReadError::ResponseTooLarge)?;
        if next_encoded_bytes > MAX_CERTIFIED_TAIL_ENCODED_BYTES {
            if records.is_empty() {
                return Err(TailReadError::ResponseTooLarge);
            }
            break;
        }
        encoded_bytes = next_encoded_bytes;
        let local = &record.entry;
        previous = LogAnchor::new(local.index, local.hash);
        records.push(record);
    }
    Ok(CertifiedTailResponse {
        records,
        observed_tip,
    })
}

fn tail_reader_authenticated(headers: &HeaderMap, config: &TailReaderConfig) -> bool {
    let Some(token) = header_text(headers, "authorization")
        .and_then(|value| value.strip_prefix(TAIL_AUTHORIZATION_SCHEME))
    else {
        return false;
    };
    header_text(headers, TAIL_VERSION_HEADER) == Some(TAIL_PROTOCOL_VERSION)
        && header_text(headers, TAIL_CLUSTER_ID_HEADER) == Some(config.cluster_id.as_str())
        && header_text(headers, TAIL_EPOCH_HEADER) == Some(config.epoch.to_string().as_str())
        && header_text(headers, TAIL_CONFIG_ID_HEADER)
            == Some(config.config_id.to_string().as_str())
        && header_text(headers, TAIL_MEMBERSHIP_DIGEST_HEADER)
            == Some(config.membership.digest().to_hex().as_str())
        && header_text(headers, RECOVERY_GENERATION_HEADER)
            == Some(config.recovery_generation.to_string().as_str())
        && secrets_equal(token.as_bytes(), config.token.as_bytes())
}

enum TailReadError {
    DecisionMismatch,
    RebaseRequired(LogAnchor),
    ResponseTooLarge,
    Unavailable,
}

struct TailReadCancellation(Arc<AtomicBool>);

impl TailReadCancellation {
    fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    fn signal(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.0)
    }
}

impl Drop for TailReadCancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn ensure_tail_read_active(cancelled: &AtomicBool, deadline: Instant) -> Result<(), TailReadError> {
    if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
        Err(TailReadError::Unavailable)
    } else {
        Ok(())
    }
}

#[derive(Default)]
struct EncodedByteCounter(usize);

impl Write for EncodedByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encoded_json_len(value: &impl Serialize) -> Result<usize, serde_json::Error> {
    let mut counter = EncodedByteCounter::default();
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CertifiedTailValidationError {
    InvalidRequest,
    TooManyRecords,
    EncodedPageTooLarge,
    EntryTooLarge,
    RegressedObservedTip,
    InvalidChain,
    ForeignContext,
    InvalidEntryHash,
    InvalidProof,
    ProofEntryMismatch,
}

impl fmt::Display for CertifiedTailValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidRequest => "certified tail request is invalid",
            Self::TooManyRecords => "certified tail response exceeds the requested bound",
            Self::EncodedPageTooLarge => "certified tail response exceeds the encoded byte bound",
            Self::EntryTooLarge => "certified tail response contains an oversized entry",
            Self::RegressedObservedTip => {
                "certified tail observed tip regresses the request anchor"
            }
            Self::InvalidChain => "certified tail records are not consecutive from the request",
            Self::ForeignContext => "certified tail record has a foreign cluster or configuration",
            Self::InvalidEntryHash => "certified tail record has an invalid entry hash",
            Self::InvalidProof => "certified tail record has an invalid decision proof",
            Self::ProofEntryMismatch => {
                "certified tail decision proof does not identify the returned entry"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for CertifiedTailValidationError {}

pub fn validate_certified_tail_response(
    config: &TailReaderConfig,
    request: &CertifiedTailRequest,
    response: &CertifiedTailResponse,
) -> Result<(), CertifiedTailValidationError> {
    if request.max_entries == 0
        || request.max_entries > MAX_CERTIFIED_TAIL_ENTRIES
        || (request.from.index() == 0 && request.from.hash() != LogHash::ZERO)
    {
        return Err(CertifiedTailValidationError::InvalidRequest);
    }
    if response.records.len() > request.max_entries as usize {
        return Err(CertifiedTailValidationError::TooManyRecords);
    }
    if response
        .records
        .iter()
        .any(|record| record.entry.payload.len() > MAX_CERTIFIED_TAIL_ENTRY_PAYLOAD_BYTES)
    {
        return Err(CertifiedTailValidationError::EntryTooLarge);
    }
    if encoded_json_len(response).unwrap_or(usize::MAX) > MAX_CERTIFIED_TAIL_ENCODED_BYTES {
        return Err(CertifiedTailValidationError::EncodedPageTooLarge);
    }
    if response.observed_tip.index() < request.from.index()
        || (response.observed_tip.index() == request.from.index()
            && response.observed_tip.hash() != request.from.hash())
    {
        return Err(CertifiedTailValidationError::RegressedObservedTip);
    }

    let mut previous = request.from;
    for record in &response.records {
        let entry = &record.entry;
        let Some(expected_index) = previous.index().checked_add(1) else {
            return Err(CertifiedTailValidationError::InvalidChain);
        };
        if entry.index != expected_index
            || entry.prev_hash != previous.hash()
            || entry.index > response.observed_tip.index()
        {
            return Err(CertifiedTailValidationError::InvalidChain);
        }
        if entry.cluster_id != config.cluster_id
            || entry.epoch != config.epoch
            || entry.config_id != config.config_id
        {
            return Err(CertifiedTailValidationError::ForeignContext);
        }
        if entry.recompute_hash() != entry.hash {
            return Err(CertifiedTailValidationError::InvalidEntryHash);
        }
        record
            .proof
            .validate_for_cluster(
                &config.cluster_id,
                entry.index,
                config.epoch,
                config.config_id,
                &config.membership,
            )
            .map_err(|_| CertifiedTailValidationError::InvalidProof)?;
        let Some(value) = record.proof.proposal().value.as_ref() else {
            return Err(CertifiedTailValidationError::ProofEntryMismatch);
        };
        let command_hash = StoredCommand::new(entry.entry_type, entry.payload.clone()).hash();
        if value.command_hash != command_hash
            || value.prev_hash != entry.prev_hash
            || value.entry_hash != entry.hash
        {
            return Err(CertifiedTailValidationError::ProofEntryMismatch);
        }
        previous = LogAnchor::new(entry.index, entry.hash);
    }
    if previous.index() == response.observed_tip.index()
        && previous.hash() != response.observed_tip.hash()
    {
        return Err(CertifiedTailValidationError::InvalidChain);
    }
    Ok(())
}

pub struct LearnerStore {
    prestage: SuccessorPrestage,
    config: TailReaderConfig,
    log: FileLogStore,
    materializer: Materializer,
    apply_lock: Mutex<()>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LearnerProgress {
    pub durable: LogAnchor,
    pub applied: LogAnchor,
    pub observed_source_tip: LogAnchor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LearnerStoreError {
    PrestageNotPublished,
    ContextMismatch,
    MissingMaterializer(PathBuf),
    Storage(String),
    InvalidPage(CertifiedTailValidationError),
    ReanchorRequired(LogAnchor),
    AnchorMismatch,
    InvalidStop,
    Poisoned,
}

impl fmt::Display for LearnerStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrestageNotPublished => {
                formatter.write_str("learner requires a published successor prestage")
            }
            Self::ContextMismatch => {
                formatter.write_str("learner context does not match successor prestage")
            }
            Self::MissingMaterializer(path) => write!(
                formatter,
                "published successor prestage is missing materializer {}",
                path.display()
            ),
            Self::Storage(message) => write!(formatter, "learner storage failed: {message}"),
            Self::InvalidPage(error) => write!(formatter, "learner page is invalid: {error}"),
            Self::ReanchorRequired(anchor) => write!(
                formatter,
                "learner request must restart from durable anchor {}/{}",
                anchor.index(),
                anchor.hash().to_hex()
            ),
            Self::AnchorMismatch => {
                formatter.write_str("learner durable and applied anchors do not match")
            }
            Self::InvalidStop => {
                formatter.write_str("learner finalization requires the exact bound Stop")
            }
            Self::Poisoned => formatter.write_str("learner apply lock is poisoned"),
        }
    }
}

impl std::error::Error for LearnerStoreError {}

impl LearnerStore {
    pub fn open(
        data_dir: impl Into<PathBuf>,
        config: TailReaderConfig,
    ) -> Result<Self, LearnerStoreError> {
        let data_dir = data_dir.into();
        let prestage = inspect_successor_prestage(
            &data_dir,
            ConfigurationState::active(config.config_id, config.membership.digest()),
        )
        .map_err(|error| LearnerStoreError::Storage(error.to_string()))?;
        if prestage.state() != SuccessorPrestageState::Published {
            return Err(LearnerStoreError::PrestageNotPublished);
        }
        let identity = prestage.identity();
        let target_config_id = config
            .config_id
            .checked_add(1)
            .ok_or(LearnerStoreError::ContextMismatch)?;
        if identity.cluster_id() != config.cluster_id
            || identity.epoch() != config.epoch
            || identity.predecessor_config_id() != config.config_id
            || identity.predecessor_recovery_generation() != config.recovery_generation
            || identity.target_config_id() != target_config_id
        {
            return Err(LearnerStoreError::ContextMismatch);
        }
        let materializer_path = detached_materializer_path(&data_dir, identity.execution_profile());
        if !materializer_path.is_file() {
            return Err(LearnerStoreError::MissingMaterializer(materializer_path));
        }
        let initial_configuration =
            ConfigurationState::active(config.config_id, config.membership.digest());
        let log = FileLogStore::open_with_configuration(
            data_dir.join("consensus/log"),
            &config.cluster_id,
            config.epoch,
            initial_configuration,
        )
        .map_err(|error| LearnerStoreError::Storage(error.to_string()))?;
        let persisted_configuration = log
            .configuration_state()
            .map_err(|error| LearnerStoreError::Storage(error.to_string()))?;
        let materializer = Materializer::open_detached(
            &data_dir,
            identity.execution_profile(),
            &config.cluster_id,
            identity.node_id(),
            config.epoch,
            &persisted_configuration,
        )
        .map_err(|error| LearnerStoreError::Storage(error.to_string()))?;
        let store = Self {
            prestage,
            config,
            log,
            materializer,
            apply_lock: Mutex::new(()),
        };
        store.reconcile()?;
        store.validate_seed()?;
        Ok(store)
    }

    pub fn durable_anchor(&self) -> Result<LogAnchor, LearnerStoreError> {
        self.log
            .logical_state()
            .map_err(|error| LearnerStoreError::Storage(error.to_string()))
            .map(|state| state.tip.unwrap_or_else(genesis_anchor))
    }

    pub fn applied_anchor(&self) -> Result<LogAnchor, LearnerStoreError> {
        self.materializer
            .applied_tip()
            .map_err(LearnerStoreError::Storage)
    }

    pub fn tail_request(
        &self,
        max_entries: u32,
    ) -> Result<CertifiedTailRequest, LearnerStoreError> {
        if max_entries == 0 || max_entries > MAX_CERTIFIED_TAIL_ENTRIES {
            return Err(LearnerStoreError::InvalidPage(
                CertifiedTailValidationError::InvalidRequest,
            ));
        }
        let _guard = self
            .apply_lock
            .lock()
            .map_err(|_| LearnerStoreError::Poisoned)?;
        self.reconcile()?;
        let durable = self.durable_anchor()?;
        if self.applied_anchor()? != durable {
            return Err(LearnerStoreError::AnchorMismatch);
        }
        Ok(CertifiedTailRequest {
            from: durable,
            max_entries,
        })
    }

    pub fn apply_page(
        &self,
        request: &CertifiedTailRequest,
        response: &CertifiedTailResponse,
    ) -> Result<LearnerProgress, LearnerStoreError> {
        let _guard = self
            .apply_lock
            .lock()
            .map_err(|_| LearnerStoreError::Poisoned)?;
        validate_certified_tail_response(&self.config, request, response)
            .map_err(LearnerStoreError::InvalidPage)?;
        self.reconcile()?;
        let durable = self.durable_anchor()?;
        let applied = self.applied_anchor()?;
        if durable != applied {
            return Err(LearnerStoreError::AnchorMismatch);
        }
        if request.from != durable {
            return Err(LearnerStoreError::ReanchorRequired(durable));
        }
        let mut configuration = self
            .log
            .configuration_state()
            .map_err(|error| LearnerStoreError::Storage(error.to_string()))?;
        let mut entries = Vec::with_capacity(response.records.len());
        for record in &response.records {
            configuration = validate_detached_entry(
                self.prestage.identity().execution_profile(),
                &self.config,
                &configuration,
                &record.entry,
            )?;
            entries.push(record.entry.clone());
        }
        if !entries.is_empty() {
            self.log
                .append_batch_buffered(&entries)
                .map_err(|error| LearnerStoreError::Storage(error.to_string()))?;
            self.log
                .sync()
                .map_err(|error| LearnerStoreError::Storage(error.to_string()))?;
        }
        for entry in &entries {
            self.materializer
                .apply_entry(entry)
                .map_err(LearnerStoreError::Storage)?;
        }
        let durable = self.durable_anchor()?;
        let applied = self.applied_anchor()?;
        if durable != applied {
            return Err(LearnerStoreError::AnchorMismatch);
        }
        Ok(LearnerProgress {
            durable,
            applied,
            observed_source_tip: response.observed_tip,
        })
    }

    pub fn finalize(
        self,
        successor_config: &NodeConfig,
        stop: &StopInformation,
    ) -> Result<SuccessorRestorePreparation, LearnerStoreError> {
        self.validate_stop(successor_config, stop)?;
        let anchor = LogAnchor::new(stop.entry.index, stop.entry.hash);
        if self.durable_anchor()? != anchor || self.applied_anchor()? != anchor {
            return Err(LearnerStoreError::AnchorMismatch);
        }
        let predecessor_membership = self.config.membership.clone();
        let Self {
            prestage,
            log,
            materializer,
            ..
        } = self;
        materializer
            .close_for_handoff()
            .map_err(LearnerStoreError::Storage)?;
        drop(log);
        let finalized =
            finalize_successor_prestage_for_stop(prestage, stop, &predecessor_membership)
                .map_err(|error| LearnerStoreError::Storage(error.to_string()))?;
        adopt_finalized_successor_prestage(
            finalized,
            successor_config,
            stop,
            &predecessor_membership,
        )
        .map_err(|error| LearnerStoreError::Storage(error.to_string()))
    }

    fn reconcile(&self) -> Result<(), LearnerStoreError> {
        let durable = self.durable_anchor()?;
        let mut applied = self.applied_anchor()?;
        if applied.index() > durable.index()
            || (applied.index() == durable.index() && applied.hash() != durable.hash())
            || !self.log_contains(applied)?
        {
            return Err(LearnerStoreError::AnchorMismatch);
        }
        let mut configuration =
            ConfigurationState::active(self.config.config_id, self.config.membership.digest());
        while applied.index() < durable.index() {
            let index = applied
                .index()
                .checked_add(1)
                .ok_or(LearnerStoreError::AnchorMismatch)?;
            let entry = self
                .log
                .read(index)
                .map_err(|error| LearnerStoreError::Storage(error.to_string()))?
                .ok_or(LearnerStoreError::AnchorMismatch)?;
            configuration = validate_detached_entry(
                self.prestage.identity().execution_profile(),
                &self.config,
                &configuration,
                &entry,
            )?;
            self.materializer
                .apply_entry(&entry)
                .map_err(LearnerStoreError::Storage)?;
            applied = LogAnchor::new(entry.index, entry.hash);
        }
        if self.applied_anchor()? != durable {
            return Err(LearnerStoreError::AnchorMismatch);
        }
        Ok(())
    }

    fn validate_seed(&self) -> Result<(), LearnerStoreError> {
        let seed = self.prestage.identity().seed_anchor();
        if self.durable_anchor()?.index() < seed.index()
            || self.applied_anchor()?.index() < seed.index()
            || !self.log_contains(seed)?
        {
            return Err(LearnerStoreError::AnchorMismatch);
        }
        Ok(())
    }

    fn log_contains(&self, anchor: LogAnchor) -> Result<bool, LearnerStoreError> {
        if anchor == genesis_anchor() {
            return Ok(true);
        }
        let state = self
            .log
            .logical_state()
            .map_err(|error| LearnerStoreError::Storage(error.to_string()))?;
        if state
            .anchor
            .as_ref()
            .is_some_and(|recovery| *recovery.compacted() == anchor)
        {
            return Ok(true);
        }
        self.log
            .read(anchor.index())
            .map_err(|error| LearnerStoreError::Storage(error.to_string()))
            .map(|entry| entry.is_some_and(|entry| entry.hash == anchor.hash()))
    }

    fn validate_stop(
        &self,
        successor_config: &NodeConfig,
        stop: &StopInformation,
    ) -> Result<(), LearnerStoreError> {
        let identity = self.prestage.identity();
        if stop.entry.cluster_id != self.config.cluster_id
            || stop.entry.epoch != self.config.epoch
            || stop.entry.config_id != self.config.config_id
            || stop.entry.recompute_hash() != stop.entry.hash
        {
            return Err(LearnerStoreError::InvalidStop);
        }
        let change = ConfigChange::recognize_parts(stop.entry.entry_type, &stop.entry.payload)
            .map_err(|_| LearnerStoreError::InvalidStop)?;
        let ConfigChange::BoundStop { successor } = change else {
            return Err(LearnerStoreError::InvalidStop);
        };
        if successor.cluster_id() != self.config.cluster_id
            || successor.predecessor_config_id() != self.config.config_id
            || successor.predecessor_config_digest() != self.config.membership.digest()
            || successor.config_id() != identity.target_config_id()
            || successor.digest() != identity.target_membership_digest()
            || successor_config.cluster_id() != successor.cluster_id()
            || successor_config.epoch() != self.config.epoch
            || successor_config.config_id().checked_add(1) != Some(successor.config_id())
            || successor_config.membership().digest() != successor.digest()
            || successor_config.data_dir() != self.prestage.path()
        {
            return Err(LearnerStoreError::InvalidStop);
        }
        stop.proof
            .validate_for_cluster(
                &self.config.cluster_id,
                stop.entry.index,
                self.config.epoch,
                self.config.config_id,
                &self.config.membership,
            )
            .map_err(|_| LearnerStoreError::InvalidStop)?;
        if !proof_matches_entry(&stop.proof, &stop.entry) {
            return Err(LearnerStoreError::InvalidStop);
        }
        let configuration = self
            .log
            .configuration_state()
            .map_err(|error| LearnerStoreError::Storage(error.to_string()))?;
        if configuration.stop() != Some(&LogAnchor::new(stop.entry.index, stop.entry.hash)) {
            return Err(LearnerStoreError::InvalidStop);
        }
        Ok(())
    }
}

fn validate_detached_entry(
    profile: ExecutionProfile,
    config: &TailReaderConfig,
    configuration: &ConfigurationState,
    entry: &LogEntry,
) -> Result<ConfigurationState, LearnerStoreError> {
    if entry.cluster_id != config.cluster_id
        || entry.epoch != config.epoch
        || entry.config_id != config.config_id
        || entry.recompute_hash() != entry.hash
    {
        return Err(LearnerStoreError::ContextMismatch);
    }
    validate_profile_entry_shape(profile, entry).map_err(LearnerStoreError::Storage)?;
    configuration
        .validate_entry(entry)
        .map_err(|error| LearnerStoreError::Storage(error.to_string()))
}

fn detached_materializer_path(data_dir: &Path, profile: ExecutionProfile) -> PathBuf {
    match profile {
        ExecutionProfile::Sqlite => data_dir.join("sqlite/db.sqlite"),
        ExecutionProfile::Graph => data_dir.join("ladybug/graph.lbug"),
        ExecutionProfile::Kv => data_dir.join("kv/data.redb"),
    }
}

const fn genesis_anchor() -> LogAnchor {
    LogAnchor::new(0, LogHash::ZERO)
}

fn proof_matches_entry(proof: &DecisionProof, entry: &LogEntry) -> bool {
    proof.proposal().value.as_ref().is_some_and(|value| {
        value.command_hash == StoredCommand::new(entry.entry_type, entry.payload.clone()).hash()
            && value.prev_hash == entry.prev_hash
            && value.entry_hash == entry.hash
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tail_read_admission_rejects_work_past_the_fixed_capacity() {
        let admission = Arc::new(tokio::sync::Semaphore::new(
            MAX_CONCURRENT_CERTIFIED_TAIL_READS,
        ));
        let permits = (0..MAX_CONCURRENT_CERTIFIED_TAIL_READS)
            .map(|_| admission.clone().try_acquire_owned().unwrap())
            .collect::<Vec<_>>();

        assert!(admission.clone().try_acquire_owned().is_err());
        drop(permits);
        assert!(admission.try_acquire_owned().is_ok());
    }

    #[test]
    fn tail_read_stops_after_deadline_or_request_cancellation() {
        let cancellation = TailReadCancellation::new();
        let signal = cancellation.signal();
        assert!(matches!(
            ensure_tail_read_active(&signal, Instant::now()),
            Err(TailReadError::Unavailable)
        ));

        drop(cancellation);
        assert!(matches!(
            ensure_tail_read_active(&signal, Instant::now() + Duration::from_secs(1)),
            Err(TailReadError::Unavailable)
        ));
    }
}
