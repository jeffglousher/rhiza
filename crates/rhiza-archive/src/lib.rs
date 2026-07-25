use rhiza_core::{
    ConfigChange, ConfigId, ConfigurationState, Epoch, LogAnchor, LogEntry, LogHash, LogIndex,
    RecoveryAnchor, Snapshot, SnapshotManifest, StoredCommand, SuccessorDescriptor,
    RECOVERY_ANCHOR_FORMAT_VERSION,
};
use rhiza_log::{decode_segment_for_cluster, encode_segment, SegmentFile};
use std::{
    collections::HashSet,
    process,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use rhiza_obj_store::{
    Error as ObjStoreError, ObjStore, ObjectMetadata, ObjectVersion, UpdateVersion,
};
use serde::{Deserialize, Serialize};

pub const ARCHIVE_FORMAT_VERSION: u32 = 1;
pub const CHECKPOINT_FORMAT_VERSION: u32 = 2;
const CHECKPOINT_SEGMENT_FORMAT_VERSION: u32 = 1;
const MAX_CHECKPOINT_CAS_ATTEMPTS: usize = 16;
const MAX_GC_CONTROL_CAS_ATTEMPTS: usize = 128;
const GC_FORMAT_VERSION: u32 = 1;
const DEFAULT_LEASE_MS: u64 = 60_000;
pub const DEFAULT_CHECKPOINT_COMPACTION_SEGMENTS: usize = 64;
static LEASE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    ObjectStore(ObjStoreError),
    WeakCompareAndSwap,
    Serialization(String),
    UnsupportedFormatVersion {
        object: &'static str,
        version: u32,
    },
    ClusterMismatch {
        expected: String,
        actual: String,
    },
    SnapshotIdentityMismatch {
        field: &'static str,
        expected: String,
        actual: String,
    },
    SizeMismatch {
        object_key: String,
        expected: u64,
        actual: u64,
    },
    ChecksumMismatch {
        object_key: String,
        expected: String,
        actual: String,
    },
    CheckpointUnbound,
    CheckpointIdentityMismatch {
        field: &'static str,
        expected: String,
        actual: String,
    },
    InvalidCheckpoint(String),
    LogDecode(String),
    PublicationConflict {
        index: LogIndex,
        expected: String,
        actual: String,
    },
    SnapshotBaseRequiresStructuredRestore,
    CheckpointBaseRegression {
        current: LogIndex,
        proposed: LogIndex,
    },
    CheckpointBaseConflict {
        index: LogIndex,
    },
    CheckpointTargetConflict,
    CompareAndSwapRetriesExhausted {
        attempts: usize,
    },
    GcBarrierActive {
        operation_id: String,
    },
    GcBarrierBusy {
        until_ms: u64,
    },
    GcPlanStale {
        message: String,
    },
    GcPlanHashMismatch {
        expected: String,
        actual: String,
    },
    InvalidGc(String),
    GenerationRetired {
        generation: u64,
        plan_hash: String,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ObjectStore(error) => error.fmt(f),
            Self::WeakCompareAndSwap => {
                write!(
                    f,
                    "archive store requires strong cross-process compare-and-swap"
                )
            }
            Self::Serialization(message) => write!(f, "archive JSON failed: {message}"),
            Self::UnsupportedFormatVersion { object, version } => {
                write!(f, "unsupported {object} format version {version}")
            }
            Self::ClusterMismatch { expected, actual } => {
                write!(
                    f,
                    "archive cluster mismatch: expected {expected}, got {actual}"
                )
            }
            Self::SnapshotIdentityMismatch {
                field,
                expected,
                actual,
            } => write!(
                f,
                "snapshot {field} mismatch: expected {expected}, got {actual}"
            ),
            Self::SizeMismatch {
                object_key,
                expected,
                actual,
            } => write!(
                f,
                "archive object {object_key} size mismatch: expected {expected}, got {actual}"
            ),
            Self::ChecksumMismatch {
                object_key,
                expected,
                actual,
            } => write!(
                f,
                "archive object {object_key} SHA-256 mismatch: expected {expected}, got {actual}"
            ),
            Self::CheckpointUnbound => {
                write!(f, "archive store is not bound to a checkpoint identity")
            }
            Self::CheckpointIdentityMismatch {
                field,
                expected,
                actual,
            } => write!(
                f,
                "checkpoint {field} mismatch: expected {expected}, got {actual}"
            ),
            Self::InvalidCheckpoint(message) => write!(f, "invalid checkpoint: {message}"),
            Self::LogDecode(message) => write!(f, "checkpoint qlog decode failed: {message}"),
            Self::PublicationConflict {
                index,
                expected,
                actual,
            } => write!(
                f,
                "checkpoint publication conflicts at index {index}: expected hash {expected}, got {actual}"
            ),
            Self::SnapshotBaseRequiresStructuredRestore => write!(
                f,
                "checkpoint has a snapshot base; use restore_checkpoint_v2 to restore snapshot state and its log suffix"
            ),
            Self::CheckpointBaseRegression { current, proposed } => write!(
                f,
                "checkpoint base regression: current index {current}, proposed index {proposed}"
            ),
            Self::CheckpointBaseConflict { index } => {
                write!(f, "checkpoint base conflicts at index {index}")
            }
            Self::CheckpointTargetConflict => {
                write!(f, "target checkpoint already exists with different content")
            }
            Self::CompareAndSwapRetriesExhausted { attempts } => write!(
                f,
                "checkpoint manifest compare-and-swap did not converge after {attempts} attempts"
            ),
            Self::GcBarrierActive { operation_id } => {
                write!(f, "object GC barrier is active: {operation_id}")
            }
            Self::GcBarrierBusy { until_ms } => {
                write!(f, "object GC is waiting for leases until {until_ms}")
            }
            Self::GcPlanStale { message } => write!(f, "object GC plan is stale: {message}"),
            Self::GcPlanHashMismatch { expected, actual } => write!(
                f,
                "object GC plan hash mismatch: expected {expected}, got {actual}"
            ),
            Self::InvalidGc(message) => write!(f, "invalid object GC state: {message}"),
            Self::GenerationRetired {
                generation,
                plan_hash,
            } => write!(
                f,
                "checkpoint recovery generation {generation} was retired by GC plan {plan_hash}"
            ),
        }
    }
}

impl std::error::Error for Error {}

impl From<ObjStoreError> for Error {
    fn from(error: ObjStoreError) -> Self {
        Self::ObjectStore(error)
    }
}

pub fn archive_lag(committed_index: LogIndex, archived_index: LogIndex) -> u64 {
    committed_index.saturating_sub(archived_index)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GcLeaseKind {
    Publisher,
    Reader,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcPolicy {
    operation_id: String,
    root: CheckpointIdentity,
    retain_recovery_generations: usize,
    grace_ms: u64,
    min_age_ms: u64,
}

impl GcPolicy {
    pub fn new(
        operation_id: impl Into<String>,
        root: CheckpointIdentity,
        retain_recovery_generations: usize,
        grace_ms: u64,
        min_age_ms: u64,
    ) -> Self {
        Self {
            operation_id: operation_id.into(),
            root,
            retain_recovery_generations,
            grace_ms,
            min_age_ms,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GcCandidateReason {
    SupersededRecoveryGeneration,
    UnreferencedCheckpointObject,
}

impl GcCandidateReason {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SupersededRecoveryGeneration => "superseded_recovery_generation",
            Self::UnreferencedCheckpointObject => "unreferenced_checkpoint_object",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GcCandidate {
    generation: CheckpointIdentity,
    key: String,
    size_bytes: u64,
    last_modified_ms: u64,
    version: ObjectVersion,
    reason: GcCandidateReason,
}

impl GcCandidate {
    pub fn key(&self) -> &str {
        &self.key
    }
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
    pub const fn last_modified_ms(&self) -> u64 {
        self.last_modified_ms
    }
    pub const fn version(&self) -> &ObjectVersion {
        &self.version
    }
    pub const fn reason(&self) -> &GcCandidateReason {
        &self.reason
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GcPlan {
    format_version: u32,
    operation_id: String,
    cluster_id: String,
    fence: u64,
    observed_control_version: ObjectVersion,
    catalog_sha256: String,
    observed_catalog: Vec<GenerationCatalogEntry>,
    root: CheckpointIdentity,
    root_manifest_key: String,
    root_manifest_sha256: String,
    root_manifest_version: ObjectVersion,
    created_at_ms: u64,
    not_before_ms: u64,
    min_age_ms: u64,
    swept_generations: Vec<CheckpointIdentity>,
    candidates: Vec<GcCandidate>,
    plan_hash: String,
}

impl GcPlan {
    pub fn plan_hash(&self) -> &str {
        &self.plan_hash
    }
    pub const fn root(&self) -> &CheckpointIdentity {
        &self.root
    }
    pub fn candidates(&self) -> &[GcCandidate] {
        &self.candidates
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GcDeleteOutcome {
    Deleted,
    AlreadyMissing,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GcEvidence {
    format_version: u32,
    plan_hash: String,
    key: String,
    version: ObjectVersion,
    outcome: GcDeleteOutcome,
    observed_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GcExecutionReport {
    format_version: u32,
    plan_hash: String,
    fence: u64,
    completed_at_ms: u64,
    results: Vec<GcEvidence>,
}

impl GcExecutionReport {
    pub fn plan_hash(&self) -> &str {
        &self.plan_hash
    }
    pub fn results(&self) -> &[GcEvidence] {
        &self.results
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GenerationCatalogEntry {
    identity: CheckpointIdentity,
    manifest_key: String,
    registered_at_ms: u64,
    lifecycle: GenerationLifecycle,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum GenerationLifecycle {
    Active,
    Retired {
        plan_hash: String,
        retired_at_ms: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GcLease {
    lease_id: String,
    kind: GcLeaseKind,
    fence: u64,
    expires_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ActiveGc {
    operation_id: String,
    plan_hash: String,
    fence: u64,
    root: CheckpointIdentity,
    expires_at_ms: u64,
    phase: GcBarrierPhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum GcBarrierPhase {
    Draining,
    Deleting,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GcControl {
    format_version: u32,
    cluster_id: String,
    fence: u64,
    root: Option<CheckpointIdentity>,
    generations: Vec<GenerationCatalogEntry>,
    leases: Vec<GcLease>,
    active_gc: Option<ActiveGc>,
}

#[derive(Clone)]
struct LoadedGcControl {
    control: GcControl,
    version: UpdateVersion,
}

struct HeldLease {
    lease_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentRecord {
    format_version: u32,
    cluster_id: String,
    epoch: u64,
    start_index: LogIndex,
    end_index: LogIndex,
    object_key: String,
    sha256: String,
    size_bytes: u64,
}

impl SegmentRecord {
    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn start_index(&self) -> LogIndex {
        self.start_index
    }

    pub const fn end_index(&self) -> LogIndex {
        self.end_index
    }

    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotRecord {
    format_version: u32,
    manifest: SnapshotManifest,
    object_key: String,
    sha256: String,
    size_bytes: u64,
}

impl SnapshotRecord {
    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    pub const fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }

    pub fn cluster_id(&self) -> &str {
        self.manifest.cluster_id()
    }

    pub const fn epoch(&self) -> u64 {
        self.manifest.epoch()
    }

    pub const fn snapshot_index(&self) -> LogIndex {
        self.manifest.index()
    }

    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveManifest {
    format_version: u32,
    cluster_id: String,
    latest_snapshot: Option<SnapshotRecord>,
    segments: Vec<SegmentRecord>,
}

impl ArchiveManifest {
    pub fn new(cluster_id: impl Into<String>) -> Self {
        Self {
            format_version: ARCHIVE_FORMAT_VERSION,
            cluster_id: cluster_id.into(),
            latest_snapshot: None,
            segments: Vec::new(),
        }
    }

    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub const fn latest_snapshot(&self) -> Option<&SnapshotRecord> {
        self.latest_snapshot.as_ref()
    }

    pub fn segments(&self) -> &[SegmentRecord] {
        &self.segments
    }

    pub fn set_latest_snapshot(&mut self, snapshot: SnapshotRecord) {
        self.latest_snapshot = Some(snapshot);
    }

    pub fn add_segment(&mut self, segment: SegmentRecord) {
        self.segments.push(segment);
    }

    pub fn latest_snapshot_index(&self) -> Option<LogIndex> {
        self.latest_snapshot
            .as_ref()
            .map(SnapshotRecord::snapshot_index)
    }

    pub fn latest_archived_index(&self) -> Option<LogIndex> {
        self.segments.iter().map(SegmentRecord::end_index).max()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedArchiveManifest {
    manifest: ArchiveManifest,
    version: UpdateVersion,
}

impl LoadedArchiveManifest {
    pub const fn manifest(&self) -> &ArchiveManifest {
        &self.manifest
    }

    pub const fn version(&self) -> &UpdateVersion {
        &self.version
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointIdentity {
    cluster_id: String,
    epoch: Epoch,
    config_id: ConfigId,
    recovery_generation: u64,
}

impl CheckpointIdentity {
    pub fn new(
        cluster_id: impl Into<String>,
        epoch: Epoch,
        config_id: ConfigId,
        recovery_generation: u64,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            epoch,
            config_id,
            recovery_generation,
        }
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

    pub const fn recovery_generation(&self) -> u64 {
        self.recovery_generation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointTip {
    index: LogIndex,
    hash: LogHash,
}

impl CheckpointTip {
    pub const fn new(index: LogIndex, hash: LogHash) -> Self {
        Self { index, hash }
    }

    pub const fn index(&self) -> LogIndex {
        self.index
    }

    pub const fn hash(&self) -> LogHash {
        self.hash
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointSegmentRecord {
    format_version: u32,
    start_index: LogIndex,
    end_index: LogIndex,
    first_prev_hash: LogHash,
    last_hash: LogHash,
    object_key: String,
    sha256: String,
    size_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointSnapshotBase {
    anchor: RecoveryAnchor,
    object_key: String,
    digest: LogHash,
    size_bytes: u64,
    executor_fingerprint: LogHash,
}

impl CheckpointSnapshotBase {
    pub const fn anchor(&self) -> &RecoveryAnchor {
        &self.anchor
    }

    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    pub const fn digest(&self) -> LogHash {
        self.digest
    }

    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub const fn executor_fingerprint(&self) -> LogHash {
        self.executor_fingerprint
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointBase {
    Genesis,
    Snapshot(Box<CheckpointSnapshotBase>),
}

impl CheckpointBase {
    pub fn snapshot(&self) -> Option<&CheckpointSnapshotBase> {
        match self {
            Self::Genesis => None,
            Self::Snapshot(snapshot) => Some(snapshot.as_ref()),
        }
    }

    fn tip(&self) -> CheckpointTip {
        match self {
            Self::Genesis => CheckpointTip::new(0, LogHash::ZERO),
            Self::Snapshot(snapshot) => CheckpointTip::new(
                snapshot.anchor.compacted().index(),
                snapshot.anchor.compacted().hash(),
            ),
        }
    }
}

impl CheckpointSegmentRecord {
    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    pub const fn start_index(&self) -> LogIndex {
        self.start_index
    }

    pub const fn end_index(&self) -> LogIndex {
        self.end_index
    }

    pub const fn first_prev_hash(&self) -> LogHash {
        self.first_prev_hash
    }

    pub const fn last_hash(&self) -> LogHash {
        self.last_hash
    }

    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointManifest {
    format_version: u32,
    identity: CheckpointIdentity,
    successor_transition: Option<CheckpointSuccessorTransition>,
    base: CheckpointBase,
    segments: Vec<CheckpointSegmentRecord>,
    tip: CheckpointTip,
}

impl CheckpointManifest {
    pub fn new(identity: CheckpointIdentity) -> Self {
        Self {
            format_version: CHECKPOINT_FORMAT_VERSION,
            identity,
            successor_transition: None,
            base: CheckpointBase::Genesis,
            segments: Vec::new(),
            tip: CheckpointTip::new(0, LogHash::ZERO),
        }
    }

    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    pub const fn identity(&self) -> &CheckpointIdentity {
        &self.identity
    }

    pub const fn successor_transition(&self) -> Option<&CheckpointSuccessorTransition> {
        self.successor_transition.as_ref()
    }

    pub fn segments(&self) -> &[CheckpointSegmentRecord] {
        &self.segments
    }

    pub const fn base(&self) -> &CheckpointBase {
        &self.base
    }

    pub const fn tip(&self) -> &CheckpointTip {
        &self.tip
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointSuccessorTransition {
    predecessor: CheckpointIdentity,
    stop_entry: LogEntry,
    successor: SuccessorDescriptor,
}

impl CheckpointSuccessorTransition {
    pub const fn predecessor(&self) -> &CheckpointIdentity {
        &self.predecessor
    }

    pub const fn stop_entry(&self) -> &LogEntry {
        &self.stop_entry
    }

    pub const fn successor(&self) -> &SuccessorDescriptor {
        &self.successor
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoredCheckpointSnapshot {
    anchor: RecoveryAnchor,
    bytes: Vec<u8>,
}

impl RestoredCheckpointSnapshot {
    pub const fn anchor(&self) -> &RecoveryAnchor {
        &self.anchor
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoredCheckpoint {
    snapshot: Option<RestoredCheckpointSnapshot>,
    suffix: Vec<LogEntry>,
    tip: CheckpointTip,
}

impl RestoredCheckpoint {
    pub const fn snapshot(&self) -> Option<&RestoredCheckpointSnapshot> {
        self.snapshot.as_ref()
    }

    pub fn suffix(&self) -> &[LogEntry] {
        &self.suffix
    }

    pub const fn tip(&self) -> &CheckpointTip {
        &self.tip
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedCheckpointManifest {
    manifest: CheckpointManifest,
    version: UpdateVersion,
}

impl LoadedCheckpointManifest {
    pub const fn manifest(&self) -> &CheckpointManifest {
        &self.manifest
    }

    pub const fn version(&self) -> &UpdateVersion {
        &self.version
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointPublisherOptions {
    lease_duration_ms: u64,
    compaction_segment_limit: usize,
}

impl CheckpointPublisherOptions {
    pub const fn new(lease_duration_ms: u64) -> Self {
        Self {
            lease_duration_ms,
            compaction_segment_limit: DEFAULT_CHECKPOINT_COMPACTION_SEGMENTS,
        }
    }

    pub const fn with_compaction_segment_limit(mut self, limit: usize) -> Self {
        self.compaction_segment_limit = limit;
        self
    }

    pub const fn lease_duration_ms(&self) -> u64 {
        self.lease_duration_ms
    }

    pub const fn compaction_segment_limit(&self) -> usize {
        self.compaction_segment_limit
    }
}

impl Default for CheckpointPublisherOptions {
    fn default() -> Self {
        Self::new(DEFAULT_LEASE_MS)
    }
}

struct PendingPublisherFlush {
    entries: Vec<LogEntry>,
    result: tokio::sync::oneshot::Sender<Result<LoadedCheckpointManifest>>,
}

fn coalesce_pending_entries(
    pending: &[PendingPublisherFlush],
    published_index: LogIndex,
) -> Result<Vec<LogEntry>> {
    let mut entries = pending
        .iter()
        .flat_map(|flush| flush.entries.iter())
        .filter(|entry| entry.index > published_index)
        .cloned()
        .collect::<Vec<_>>();
    entries.sort_unstable_by_key(|entry| entry.index);

    let mut coalesced = Vec::<LogEntry>::with_capacity(entries.len());
    for entry in entries {
        if let Some(previous) = coalesced.last() {
            if previous.index == entry.index {
                if previous != &entry {
                    return Err(Error::InvalidCheckpoint(format!(
                        "conflicting concurrent publications at index {}",
                        entry.index
                    )));
                }
                continue;
            }
        }
        coalesced.push(entry);
    }
    Ok(coalesced)
}

struct CheckpointPublisherState {
    loaded: LoadedCheckpointManifest,
    pending: Vec<PendingPublisherFlush>,
}

pub struct CheckpointPublisher {
    store: ObjectArchiveStore,
    lease_id: String,
    options: CheckpointPublisherOptions,
    operation: tokio::sync::Mutex<()>,
    state: tokio::sync::Mutex<CheckpointPublisherState>,
}

impl CheckpointPublisher {
    pub async fn renew(&self) -> Result<()> {
        self.renew_at(now_ms()).await
    }

    async fn renew_at(&self, now_ms: u64) -> Result<()> {
        self.store
            .renew_gc_lease(
                GcLeaseKind::Publisher,
                &self.lease_id,
                now_ms,
                self.options.lease_duration_ms,
            )
            .await
    }

    pub async fn cached_checkpoint(&self) -> LoadedCheckpointManifest {
        self.state.lock().await.loaded.clone()
    }

    pub async fn reload(&self) -> Result<LoadedCheckpointManifest> {
        let _operation = self.operation.lock().await;
        self.renew().await?;
        let loaded = self
            .store
            .load_checkpoint_unleased()
            .await?
            .ok_or_else(|| {
                Error::InvalidCheckpoint(
                    "manifest disappeared while reloading publisher cache".into(),
                )
            })?;
        self.state.lock().await.loaded = loaded.clone();
        Ok(loaded)
    }

    pub async fn observe_checkpoint(&self) -> Result<LoadedCheckpointManifest> {
        let _operation = self.operation.lock().await;
        self.renew().await?;
        self.store.load_checkpoint_unleased().await?.ok_or_else(|| {
            Error::InvalidCheckpoint(
                "manifest disappeared while observing publisher checkpoint".into(),
            )
        })
    }

    pub async fn cache_observed_checkpoint(
        &self,
        observed: LoadedCheckpointManifest,
    ) -> Result<LoadedCheckpointManifest> {
        let _operation = self.operation.lock().await;
        self.store
            .validate_checkpoint_manifest(observed.manifest())?;
        let mut state = self.state.lock().await;
        let current_tip = *state.loaded.manifest().tip();
        let observed_tip = *observed.manifest().tip();
        if observed_tip.index() == current_tip.index() && observed_tip.hash() != current_tip.hash()
        {
            return Err(Error::InvalidCheckpoint(format!(
                "observed checkpoint hash conflicts with publisher cache at index {}",
                observed_tip.index()
            )));
        }
        if observed_tip.index() < current_tip.index() {
            return Err(Error::InvalidCheckpoint(format!(
                "observed checkpoint rolled back from index {} to {}",
                current_tip.index(),
                observed_tip.index()
            )));
        }
        if observed_tip.index() > current_tip.index() {
            state.loaded = observed;
        }
        Ok(state.loaded.clone())
    }

    pub async fn compaction_recommended(&self) -> bool {
        let state = self.state.lock().await;
        self.options.compaction_segment_limit != 0
            && state.loaded.manifest.segments.len() >= self.options.compaction_segment_limit
    }

    pub async fn publish_committed(
        &self,
        entries: &[LogEntry],
    ) -> Result<LoadedCheckpointManifest> {
        self.store.validate_publication_entries(entries)?;
        if entries.is_empty() {
            self.renew().await?;
            return Ok(self.cached_checkpoint().await);
        }

        let (result, receiver) = tokio::sync::oneshot::channel();
        {
            let mut state = self.state.lock().await;
            state.pending.push(PendingPublisherFlush {
                entries: entries.to_vec(),
                result,
            });
        }
        tokio::task::yield_now().await;
        self.drive_flushes().await;

        receiver.await.unwrap_or_else(|_| {
            Err(Error::InvalidCheckpoint(
                "publisher flush driver stopped before reporting a result".into(),
            ))
        })
    }

    pub async fn publish_checkpoint_snapshot(
        &self,
        anchor: RecoveryAnchor,
        snapshot_bytes: &[u8],
    ) -> Result<LoadedCheckpointManifest> {
        let _operation = self.operation.lock().await;
        let loaded = self.state.lock().await.loaded.clone();
        let published = self
            .store
            .publish_checkpoint_snapshot_from_loaded_unleased(
                anchor,
                snapshot_bytes,
                &self.lease_id,
                self.options.lease_duration_ms,
                loaded,
            )
            .await?;
        self.state.lock().await.loaded = published.clone();
        Ok(published)
    }

    pub async fn close(self) -> Result<()> {
        let _operation = self.operation.lock().await;
        self.store.release_gc_lease(&self.lease_id).await
    }

    async fn drive_flushes(&self) {
        let _operation = self.operation.lock().await;
        let (pending, loaded) = {
            let mut state = self.state.lock().await;
            if state.pending.is_empty() {
                return;
            }
            (std::mem::take(&mut state.pending), state.loaded.clone())
        };
        let published_index = loaded.manifest().tip().index();
        let published = match coalesce_pending_entries(&pending, published_index) {
            Ok(entries) if entries.is_empty() => Ok(loaded),
            Ok(entries) => {
                self.store
                    .publish_committed_from_loaded_unleased(
                        &entries,
                        &self.lease_id,
                        self.options.lease_duration_ms,
                        loaded,
                    )
                    .await
            }
            Err(error) => Err(error),
        };

        if let Ok(loaded) = &published {
            self.state.lock().await.loaded = loaded.clone();
        }
        for flush in pending {
            let result = match &published {
                Ok(loaded) => match self
                    .store
                    .publication_suffix_start(loaded.manifest(), &flush.entries)
                    .await
                {
                    Ok(None) => Ok(loaded.clone()),
                    Ok(Some(_)) => Err(Error::InvalidCheckpoint(
                        "coalesced publication did not reach the requested index".into(),
                    )),
                    Err(error) => Err(error),
                },
                Err(error) => Err(error.clone()),
            };
            let _ = flush.result.send(result);
        }
    }
}

#[derive(Clone)]
pub struct ObjectArchiveStore {
    store: ObjStore,
    cluster_id: String,
    checkpoint_identity: Option<CheckpointIdentity>,
}

impl ObjectArchiveStore {
    pub fn new(store: ObjStore, cluster_id: impl Into<String>) -> Result<Self> {
        if !store.supports_strong_cross_process_cas() {
            return Err(Error::WeakCompareAndSwap);
        }
        Ok(Self::new_for_single_process(store, cluster_id))
    }

    pub fn new_for_single_process(store: ObjStore, cluster_id: impl Into<String>) -> Self {
        Self {
            store,
            cluster_id: cluster_id.into(),
            checkpoint_identity: None,
        }
    }

    pub fn new_checkpoint(store: ObjStore, identity: CheckpointIdentity) -> Result<Self> {
        if !store.supports_strong_cross_process_cas() {
            return Err(Error::WeakCompareAndSwap);
        }
        Ok(Self::new_checkpoint_for_single_process(store, identity))
    }

    pub fn new_checkpoint_for_single_process(
        store: ObjStore,
        identity: CheckpointIdentity,
    ) -> Self {
        Self {
            store,
            cluster_id: identity.cluster_id.clone(),
            checkpoint_identity: Some(identity),
        }
    }

    pub fn checkpoint_identity(&self) -> Result<&CheckpointIdentity> {
        self.checkpoint_identity
            .as_ref()
            .ok_or(Error::CheckpointUnbound)
    }

    pub fn checkpoint_manifest_key(&self) -> Result<String> {
        Ok(checkpoint_namespace(self.checkpoint_identity()?) + "/manifest.json")
    }

    pub async fn open_checkpoint_publisher(
        &self,
        holder: impl Into<String>,
        options: CheckpointPublisherOptions,
    ) -> Result<CheckpointPublisher> {
        let holder = holder.into();
        if holder.trim().is_empty() || options.lease_duration_ms == 0 {
            return Err(Error::InvalidGc(
                "publisher holder and lease duration must be non-empty".into(),
            ));
        }
        self.ensure_generation_not_retired().await?;
        self.load_checkpoint().await?;
        let opened_at_ms = now_ms();
        let lease_id = format!(
            "{holder}-{}-{opened_at_ms}-{}",
            process::id(),
            LEASE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let lease = self
            .acquire_named_lease(
                GcLeaseKind::Publisher,
                lease_id,
                opened_at_ms,
                options.lease_duration_ms,
            )
            .await?;
        let loaded = match self
            .initialize_checkpoint_unleased(&lease.lease_id, options.lease_duration_ms)
            .await
        {
            Ok(loaded) => loaded,
            Err(error) => {
                let _ = self.release_gc_lease(&lease.lease_id).await;
                return Err(error);
            }
        };
        Ok(CheckpointPublisher {
            store: self.clone(),
            lease_id: lease.lease_id,
            options,
            operation: tokio::sync::Mutex::new(()),
            state: tokio::sync::Mutex::new(CheckpointPublisherState {
                loaded,
                pending: Vec::new(),
            }),
        })
    }

    pub async fn initialize_checkpoint(&self) -> Result<LoadedCheckpointManifest> {
        self.ensure_generation_not_retired().await?;
        self.load_checkpoint().await?;
        let lease = self
            .acquire_operation_lease(GcLeaseKind::Publisher, now_ms(), DEFAULT_LEASE_MS)
            .await?;
        let result = self
            .initialize_checkpoint_unleased(&lease.lease_id, DEFAULT_LEASE_MS)
            .await;
        let release = self.release_gc_lease(&lease.lease_id).await;
        match (result, release) {
            (Ok(loaded), Ok(())) => Ok(loaded),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }

    async fn initialize_checkpoint_unleased(
        &self,
        lease_id: &str,
        lease_duration_ms: u64,
    ) -> Result<LoadedCheckpointManifest> {
        self.ensure_generation_not_retired().await?;
        self.renew_gc_lease(
            GcLeaseKind::Publisher,
            lease_id,
            now_ms(),
            lease_duration_ms,
        )
        .await?;
        let identity = self.checkpoint_identity()?.clone();
        let manifest = CheckpointManifest::new(identity);
        let bytes = serialize_json(&manifest)?;
        let key = self.checkpoint_manifest_key()?;

        let loaded = match self.store.create(&key, bytes).await {
            Ok(version) => LoadedCheckpointManifest { manifest, version },
            Err(ObjStoreError::AlreadyExists { .. }) => {
                self.load_checkpoint_unleased().await?.ok_or_else(|| {
                    Error::InvalidCheckpoint("manifest disappeared during initialization".into())
                })?
            }
            Err(error) => return Err(error.into()),
        };
        self.renew_gc_lease(
            GcLeaseKind::Publisher,
            lease_id,
            now_ms(),
            lease_duration_ms,
        )
        .await?;
        self.register_generation(now_ms()).await?;
        Ok(loaded)
    }

    pub async fn load_checkpoint(&self) -> Result<Option<LoadedCheckpointManifest>> {
        self.load_checkpoint_unleased().await
    }

    async fn load_checkpoint_unleased(&self) -> Result<Option<LoadedCheckpointManifest>> {
        self.checkpoint_identity()?;
        let object = match self
            .store
            .get_versioned(&self.checkpoint_manifest_key()?)
            .await
        {
            Ok(object) => object,
            Err(ObjStoreError::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let manifest: CheckpointManifest = deserialize_json(object.bytes())?;
        self.validate_checkpoint_manifest(&manifest)?;
        Ok(Some(LoadedCheckpointManifest {
            manifest,
            version: object.version().clone(),
        }))
    }

    pub async fn publish_committed(
        &self,
        entries: &[LogEntry],
    ) -> Result<LoadedCheckpointManifest> {
        self.ensure_generation_not_retired().await?;
        self.load_checkpoint().await?;
        let lease = self
            .acquire_operation_lease(GcLeaseKind::Publisher, now_ms(), DEFAULT_LEASE_MS)
            .await?;
        let result = self
            .publish_committed_unleased(entries, &lease.lease_id, DEFAULT_LEASE_MS)
            .await;
        let release = self.release_gc_lease(&lease.lease_id).await;
        match (result, release) {
            (Ok(loaded), Ok(())) => Ok(loaded),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }

    async fn publish_committed_unleased(
        &self,
        entries: &[LogEntry],
        lease_id: &str,
        lease_duration_ms: u64,
    ) -> Result<LoadedCheckpointManifest> {
        self.validate_publication_entries(entries)?;
        let loaded = self
            .initialize_checkpoint_unleased(lease_id, lease_duration_ms)
            .await?;
        self.publish_committed_from_loaded_unleased(entries, lease_id, lease_duration_ms, loaded)
            .await
    }

    async fn publish_committed_from_loaded_unleased(
        &self,
        entries: &[LogEntry],
        lease_id: &str,
        lease_duration_ms: u64,
        mut loaded: LoadedCheckpointManifest,
    ) -> Result<LoadedCheckpointManifest> {
        self.ensure_generation_not_retired().await?;
        self.renew_gc_lease(
            GcLeaseKind::Publisher,
            lease_id,
            now_ms(),
            lease_duration_ms,
        )
        .await?;
        if entries.is_empty() {
            return Ok(loaded);
        }

        for _ in 0..MAX_CHECKPOINT_CAS_ATTEMPTS {
            let suffix_start = self
                .publication_suffix_start(loaded.manifest(), entries)
                .await?;
            let Some(suffix_start) = suffix_start else {
                return Ok(loaded);
            };
            let bytes = encode_segment(&entries[suffix_start..]);
            let decoded =
                decode_segment_for_cluster(&bytes, self.checkpoint_identity()?.cluster_id())
                    .map_err(|error| Error::LogDecode(error.to_string()))?;
            self.validate_decoded_entries(&decoded, loaded.manifest().tip())?;
            let record = self.checkpoint_segment_record(&decoded, &bytes)?;
            self.renew_gc_lease(
                GcLeaseKind::Publisher,
                lease_id,
                now_ms(),
                lease_duration_ms,
            )
            .await?;
            self.store.create(record.object_key(), &bytes).await?;

            let mut next = loaded.manifest.clone();
            next.tip = CheckpointTip::new(record.end_index, record.last_hash);
            next.segments.push(record);
            self.validate_checkpoint_manifest(&next)?;
            let next_bytes = serialize_json(&next)?;
            self.renew_gc_lease(
                GcLeaseKind::Publisher,
                lease_id,
                now_ms(),
                lease_duration_ms,
            )
            .await?;
            match self
                .store
                .update(
                    &self.checkpoint_manifest_key()?,
                    next_bytes,
                    loaded.version.clone(),
                )
                .await
            {
                Ok(version) => {
                    return Ok(LoadedCheckpointManifest {
                        manifest: next,
                        version,
                    });
                }
                Err(ObjStoreError::Precondition { .. }) => {
                    loaded = self.load_checkpoint_unleased().await?.ok_or_else(|| {
                        Error::InvalidCheckpoint("manifest disappeared after stale CAS".into())
                    })?;
                }
                Err(error) => return Err(error.into()),
            }
        }

        Err(Error::CompareAndSwapRetriesExhausted {
            attempts: MAX_CHECKPOINT_CAS_ATTEMPTS,
        })
    }

    pub async fn publish_checkpoint_snapshot(
        &self,
        anchor: RecoveryAnchor,
        snapshot_bytes: &[u8],
    ) -> Result<LoadedCheckpointManifest> {
        self.ensure_generation_not_retired().await?;
        self.load_checkpoint().await?;
        let lease = self
            .acquire_operation_lease(GcLeaseKind::Publisher, now_ms(), DEFAULT_LEASE_MS)
            .await?;
        let result = self
            .publish_checkpoint_snapshot_unleased(
                anchor,
                snapshot_bytes,
                &lease.lease_id,
                DEFAULT_LEASE_MS,
            )
            .await;
        let release = self.release_gc_lease(&lease.lease_id).await;
        match (result, release) {
            (Ok(loaded), Ok(())) => Ok(loaded),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }

    async fn publish_checkpoint_snapshot_unleased(
        &self,
        anchor: RecoveryAnchor,
        snapshot_bytes: &[u8],
        lease_id: &str,
        lease_duration_ms: u64,
    ) -> Result<LoadedCheckpointManifest> {
        let loaded = self
            .initialize_checkpoint_unleased(lease_id, lease_duration_ms)
            .await?;
        self.publish_checkpoint_snapshot_from_loaded_unleased(
            anchor,
            snapshot_bytes,
            lease_id,
            lease_duration_ms,
            loaded,
        )
        .await
    }

    async fn publish_checkpoint_snapshot_from_loaded_unleased(
        &self,
        anchor: RecoveryAnchor,
        snapshot_bytes: &[u8],
        lease_id: &str,
        lease_duration_ms: u64,
        mut loaded: LoadedCheckpointManifest,
    ) -> Result<LoadedCheckpointManifest> {
        self.ensure_generation_not_retired().await?;
        self.validate_recovery_anchor(&anchor)?;
        let digest = LogHash::digest(&[snapshot_bytes]);
        if anchor.snapshot().digest() != digest {
            return Err(Error::ChecksumMismatch {
                object_key: anchor.snapshot().snapshot_id().to_string(),
                expected: anchor.snapshot().digest().to_hex(),
                actual: digest.to_hex(),
            });
        }
        if anchor.snapshot().size_bytes() != snapshot_bytes.len() as u64 {
            return Err(Error::SizeMismatch {
                object_key: anchor.snapshot().snapshot_id().to_string(),
                expected: anchor.snapshot().size_bytes(),
                actual: snapshot_bytes.len() as u64,
            });
        }

        let snapshot = CheckpointSnapshotBase {
            object_key: checkpoint_snapshot_key(self.checkpoint_identity()?, &anchor),
            executor_fingerprint: anchor.executor_fingerprint(),
            anchor,
            digest,
            size_bytes: snapshot_bytes.len() as u64,
        };
        self.validate_checkpoint_snapshot_base(&snapshot)?;
        self.renew_gc_lease(
            GcLeaseKind::Publisher,
            lease_id,
            now_ms(),
            lease_duration_ms,
        )
        .await?;
        match self
            .store
            .create(&snapshot.object_key, snapshot_bytes)
            .await
        {
            Ok(_) | Err(ObjStoreError::AlreadyExists { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        self.download_verified(
            &snapshot.object_key,
            snapshot.size_bytes,
            &snapshot.digest.to_hex(),
        )
        .await?;

        let proposed = CheckpointBase::Snapshot(Box::new(snapshot));
        for _ in 0..MAX_CHECKPOINT_CAS_ATTEMPTS {
            let current_tip = loaded.manifest.base.tip();
            let proposed_tip = proposed.tip();
            if current_tip.index == proposed_tip.index {
                if loaded.manifest.base == proposed {
                    return Ok(loaded);
                }
                return Err(Error::CheckpointBaseConflict {
                    index: proposed_tip.index,
                });
            }
            if current_tip.index > proposed_tip.index {
                return Err(Error::CheckpointBaseRegression {
                    current: current_tip.index,
                    proposed: proposed_tip.index,
                });
            }

            let boundary = loaded
                .manifest
                .segments
                .iter()
                .find(|record| record.end_index == proposed_tip.index)
                .ok_or_else(|| {
                    Error::InvalidCheckpoint(format!(
                        "snapshot anchor {} is not an exact segment boundary",
                        proposed_tip.index
                    ))
                })?;
            if boundary.last_hash != proposed_tip.hash {
                return Err(Error::CheckpointBaseConflict {
                    index: proposed_tip.index,
                });
            }

            let mut next = loaded.manifest.clone();
            next.base = proposed.clone();
            next.segments
                .retain(|record| record.start_index > proposed_tip.index);
            self.validate_checkpoint_manifest(&next)?;
            self.renew_gc_lease(
                GcLeaseKind::Publisher,
                lease_id,
                now_ms(),
                lease_duration_ms,
            )
            .await?;
            match self
                .store
                .update(
                    &self.checkpoint_manifest_key()?,
                    serialize_json(&next)?,
                    loaded.version.clone(),
                )
                .await
            {
                Ok(version) => {
                    return Ok(LoadedCheckpointManifest {
                        manifest: next,
                        version,
                    });
                }
                Err(ObjStoreError::Precondition { .. }) => {
                    loaded = self.load_checkpoint_unleased().await?.ok_or_else(|| {
                        Error::InvalidCheckpoint("manifest disappeared after stale CAS".into())
                    })?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(Error::CompareAndSwapRetriesExhausted {
            attempts: MAX_CHECKPOINT_CAS_ATTEMPTS,
        })
    }

    pub async fn restore_checkpoint(&self) -> Result<Vec<LogEntry>> {
        let lease = self
            .acquire_operation_lease(GcLeaseKind::Reader, now_ms(), DEFAULT_LEASE_MS)
            .await?;
        let result = match self.ensure_generation_not_retired().await {
            Ok(()) => match self.restore_checkpoint_unleased(&lease.lease_id).await {
                Ok(restored) if restored.snapshot.is_none() => Ok(restored.suffix),
                Ok(_) => Err(Error::SnapshotBaseRequiresStructuredRestore),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        let release = self.release_gc_lease(&lease.lease_id).await;
        match (result, release) {
            (Ok(entries), Ok(())) => Ok(entries),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }

    pub async fn restore_checkpoint_v2(&self) -> Result<RestoredCheckpoint> {
        let lease = self
            .acquire_operation_lease(GcLeaseKind::Reader, now_ms(), DEFAULT_LEASE_MS)
            .await?;
        let result = match self.ensure_generation_not_retired().await {
            Ok(()) => self.restore_checkpoint_unleased(&lease.lease_id).await,
            Err(error) => Err(error),
        };
        let release = self.release_gc_lease(&lease.lease_id).await;
        match (result, release) {
            (Ok(restored), Ok(())) => Ok(restored),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }

    pub async fn roll_recovery_generation(
        &self,
        target: &ObjectArchiveStore,
    ) -> Result<LoadedCheckpointManifest> {
        let source_identity = self.checkpoint_identity()?;
        let target_identity = target.checkpoint_identity()?;
        if source_identity.cluster_id != target_identity.cluster_id
            || source_identity.epoch != target_identity.epoch
            || source_identity.config_id != target_identity.config_id
            || source_identity.recovery_generation.checked_add(1)
                != Some(target_identity.recovery_generation)
        {
            return Err(Error::InvalidCheckpoint(
                "recovery-generation roll requires the same cluster/epoch/config and generation + 1"
                    .into(),
            ));
        }
        self.copy_checkpoint_to(target, None).await
    }

    pub async fn fork_stopped_successor(
        &self,
        target: &ObjectArchiveStore,
        stop_entry: &LogEntry,
    ) -> Result<LoadedCheckpointManifest> {
        let source_identity = self.checkpoint_identity()?;
        let target_identity = target.checkpoint_identity()?;
        validate_entry_identity(source_identity, stop_entry)?;
        if stop_entry.recompute_hash() != stop_entry.hash {
            return Err(Error::InvalidCheckpoint(
                "predecessor Stop entry hash is invalid".into(),
            ));
        }
        let command = StoredCommand::new(stop_entry.entry_type, stop_entry.payload.clone());
        let successor = ConfigChange::recognize(&command)
            .ok()
            .and_then(|change| change.successor().cloned())
            .ok_or_else(|| {
                Error::InvalidCheckpoint(
                    "predecessor Stop entry is not bound to an exact successor".into(),
                )
            })?;
        if source_identity.cluster_id != target_identity.cluster_id
            || source_identity.epoch != target_identity.epoch
            || source_identity.config_id.checked_add(1) != Some(target_identity.config_id)
            || successor.cluster_id() != target_identity.cluster_id
            || successor.predecessor_config_id() != source_identity.config_id
            || successor.config_id() != target_identity.config_id
        {
            return Err(Error::InvalidCheckpoint(
                "bound successor does not match the target checkpoint identity".into(),
            ));
        }

        let source = self
            .load_checkpoint()
            .await?
            .ok_or_else(|| Error::InvalidCheckpoint("source checkpoint is missing".into()))?;
        if source.manifest.format_version != CHECKPOINT_FORMAT_VERSION {
            return Err(Error::InvalidCheckpoint(
                "successor fork requires a format-2 source checkpoint".into(),
            ));
        }
        let restored = self.restore_checkpoint_v2().await?;
        let snapshot = restored.snapshot().ok_or_else(|| {
            Error::InvalidCheckpoint("successor fork requires a stopped snapshot base".into())
        })?;
        let expected_stop = LogAnchor::new(stop_entry.index, stop_entry.hash);
        let expected_state = ConfigurationState::active(
            source_identity.config_id,
            successor.predecessor_config_digest(),
        )
        .validate_entry(stop_entry)
        .map_err(|_| Error::InvalidCheckpoint("predecessor Stop state is invalid".into()))?;
        if snapshot.anchor().configuration_state() != &expected_state
            || snapshot.anchor().compacted() != &expected_stop
            || restored.tip() != &CheckpointTip::new(stop_entry.index, stop_entry.hash)
            || !restored.suffix().is_empty()
        {
            return Err(Error::InvalidCheckpoint(
                "source checkpoint is not the exact bound stopped root".into(),
            ));
        }
        let transition = CheckpointSuccessorTransition {
            predecessor: source_identity.clone(),
            stop_entry: stop_entry.clone(),
            successor,
        };
        if let Some(advanced) = target
            .load_valid_advanced_successor(&transition, stop_entry.index)
            .await?
        {
            return Ok(advanced);
        }
        match self
            .copy_checkpoint_to(target, Some(transition.clone()))
            .await
        {
            Err(Error::CheckpointTargetConflict) => target
                .load_valid_advanced_successor(&transition, stop_entry.index)
                .await?
                .ok_or(Error::CheckpointTargetConflict),
            result => result,
        }
    }

    async fn load_valid_advanced_successor(
        &self,
        transition: &CheckpointSuccessorTransition,
        stop_index: u64,
    ) -> Result<Option<LoadedCheckpointManifest>> {
        let Some(loaded) = self.load_checkpoint().await? else {
            return Ok(None);
        };
        if loaded.manifest.successor_transition.as_ref() != Some(transition)
            || loaded.manifest.tip.index() <= stop_index
        {
            return Ok(None);
        }
        self.restore_checkpoint_v2().await?;
        let reloaded = self.load_checkpoint().await?.ok_or_else(|| {
            Error::InvalidCheckpoint("advanced successor checkpoint disappeared".into())
        })?;
        if reloaded.manifest.successor_transition.as_ref() != Some(transition)
            || reloaded.manifest.tip.index() <= stop_index
        {
            return Err(Error::CheckpointTargetConflict);
        }
        Ok(Some(reloaded))
    }

    async fn copy_checkpoint_to(
        &self,
        target: &ObjectArchiveStore,
        successor_transition: Option<CheckpointSuccessorTransition>,
    ) -> Result<LoadedCheckpointManifest> {
        self.ensure_generation_not_retired().await?;
        target.ensure_generation_not_retired().await?;
        self.load_checkpoint().await?;
        target.load_checkpoint().await?;
        let source_lease = self
            .acquire_operation_lease(GcLeaseKind::Reader, now_ms(), DEFAULT_LEASE_MS)
            .await?;
        let target_lease = match target
            .acquire_operation_lease(GcLeaseKind::Publisher, now_ms(), DEFAULT_LEASE_MS)
            .await
        {
            Ok(lease) => lease,
            Err(error) => {
                let _ = self.release_gc_lease(&source_lease.lease_id).await;
                return Err(error);
            }
        };
        let result = self
            .copy_checkpoint_to_unleased(
                target,
                successor_transition,
                &source_lease.lease_id,
                &target_lease.lease_id,
            )
            .await;
        let source_release = self.release_gc_lease(&source_lease.lease_id).await;
        let target_release = target.release_gc_lease(&target_lease.lease_id).await;
        match (result, source_release, target_release) {
            (Ok(loaded), Ok(()), Ok(())) => Ok(loaded),
            (Err(error), _, _) | (_, Err(error), _) | (_, _, Err(error)) => Err(error),
        }
    }

    async fn copy_checkpoint_to_unleased(
        &self,
        target: &ObjectArchiveStore,
        successor_transition: Option<CheckpointSuccessorTransition>,
        source_lease_id: &str,
        target_lease_id: &str,
    ) -> Result<LoadedCheckpointManifest> {
        self.ensure_generation_not_retired().await?;
        target.ensure_generation_not_retired().await?;
        let source = self
            .load_checkpoint_unleased()
            .await?
            .ok_or_else(|| Error::InvalidCheckpoint("source checkpoint is missing".into()))?;
        if source.manifest.format_version != CHECKPOINT_FORMAT_VERSION {
            return Err(Error::InvalidCheckpoint(
                "checkpoint copy requires a format-2 source".into(),
            ));
        }
        let successor_transition =
            successor_transition.or_else(|| source.manifest.successor_transition.clone());
        self.restore_checkpoint_unleased(source_lease_id).await?;
        let target_identity = target.checkpoint_identity()?.clone();
        let base = match &source.manifest.base {
            CheckpointBase::Genesis => CheckpointBase::Genesis,
            CheckpointBase::Snapshot(snapshot) => {
                let bytes = self
                    .download_verified(
                        &snapshot.object_key,
                        snapshot.size_bytes,
                        &snapshot.digest.to_hex(),
                    )
                    .await?;
                let anchor = RecoveryAnchor::new(
                    snapshot.anchor.cluster_id(),
                    snapshot.anchor.epoch(),
                    snapshot.anchor.configuration_state().clone(),
                    target_identity.recovery_generation,
                    *snapshot.anchor.compacted(),
                    snapshot.anchor.snapshot().clone(),
                );
                let copied = CheckpointSnapshotBase {
                    object_key: checkpoint_snapshot_key(&target_identity, &anchor),
                    anchor,
                    digest: snapshot.digest,
                    size_bytes: snapshot.size_bytes,
                    executor_fingerprint: snapshot.executor_fingerprint,
                };
                target
                    .create_verified_checkpoint_object(
                        &copied.object_key,
                        &bytes,
                        copied.size_bytes,
                        &copied.digest.to_hex(),
                    )
                    .await?;
                CheckpointBase::Snapshot(Box::new(copied))
            }
        };
        let mut segments = Vec::with_capacity(source.manifest.segments.len());
        for record in &source.manifest.segments {
            self.renew_gc_lease(
                GcLeaseKind::Reader,
                source_lease_id,
                now_ms(),
                DEFAULT_LEASE_MS,
            )
            .await?;
            target
                .renew_gc_lease(
                    GcLeaseKind::Publisher,
                    target_lease_id,
                    now_ms(),
                    DEFAULT_LEASE_MS,
                )
                .await?;
            let bytes = self
                .download_verified(&record.object_key, record.size_bytes, &record.sha256)
                .await?;
            let mut copied = record.clone();
            copied.object_key =
                checkpoint_segment_key(&target_identity, copied.start_index, copied.end_index);
            target
                .create_verified_checkpoint_object(
                    &copied.object_key,
                    &bytes,
                    copied.size_bytes,
                    &copied.sha256,
                )
                .await?;
            segments.push(copied);
        }
        let manifest = CheckpointManifest {
            format_version: CHECKPOINT_FORMAT_VERSION,
            identity: target_identity,
            successor_transition,
            base,
            segments,
            tip: source.manifest.tip,
        };
        target.validate_checkpoint_manifest(&manifest)?;
        if let Some(existing) = target.load_checkpoint_unleased().await? {
            return if existing.manifest == manifest {
                target.register_generation(now_ms()).await?;
                Ok(existing)
            } else {
                Err(Error::CheckpointTargetConflict)
            };
        }
        let target_manifest_key = target.checkpoint_manifest_key()?;
        let version = match target
            .store
            .create(&target_manifest_key, serialize_json(&manifest)?)
            .await
        {
            Ok(version) => version,
            Err(ObjStoreError::AlreadyExists { .. }) => {
                let existing = target.load_checkpoint_unleased().await?.ok_or_else(|| {
                    Error::InvalidCheckpoint("target manifest disappeared after create".into())
                })?;
                return if existing.manifest == manifest {
                    target.register_generation(now_ms()).await?;
                    Ok(existing)
                } else {
                    Err(Error::CheckpointTargetConflict)
                };
            }
            Err(error) => return Err(error.into()),
        };
        let source_after = self.load_checkpoint_unleased().await?;
        if source_after.as_ref() != Some(&source) {
            let published_version: ObjectVersion = version.clone().into();
            if !target
                .store
                .delete_exact(&target_manifest_key, &published_version)
                .await?
            {
                return Err(Error::CheckpointTargetConflict);
            }
            return Err(Error::InvalidCheckpoint(
                "source checkpoint changed during copy".into(),
            ));
        }
        target.register_generation(now_ms()).await?;
        Ok(LoadedCheckpointManifest { manifest, version })
    }

    async fn create_verified_checkpoint_object(
        &self,
        key: &str,
        bytes: &[u8],
        size_bytes: u64,
        sha256: &str,
    ) -> Result<()> {
        match self.store.create(key, bytes).await {
            Ok(_) | Err(ObjStoreError::AlreadyExists { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        self.download_verified(key, size_bytes, sha256).await?;
        Ok(())
    }

    async fn restore_checkpoint_unleased(&self, lease_id: &str) -> Result<RestoredCheckpoint> {
        let Some(loaded) = self.load_checkpoint_unleased().await? else {
            return Ok(RestoredCheckpoint {
                snapshot: None,
                suffix: Vec::new(),
                tip: CheckpointTip::new(0, LogHash::ZERO),
            });
        };
        let mut restored = Vec::new();
        let mut expected_tip = loaded.manifest.base.tip();
        let snapshot = match &loaded.manifest.base {
            CheckpointBase::Genesis => None,
            CheckpointBase::Snapshot(snapshot) => {
                self.renew_gc_lease(GcLeaseKind::Reader, lease_id, now_ms(), DEFAULT_LEASE_MS)
                    .await?;
                let bytes = self
                    .download_verified(
                        &snapshot.object_key,
                        snapshot.size_bytes,
                        &snapshot.digest.to_hex(),
                    )
                    .await?;
                Some(RestoredCheckpointSnapshot {
                    anchor: snapshot.anchor.clone(),
                    bytes,
                })
            }
        };
        for record in &loaded.manifest.segments {
            self.renew_gc_lease(GcLeaseKind::Reader, lease_id, now_ms(), DEFAULT_LEASE_MS)
                .await?;
            let entries = self.load_checkpoint_segment(record).await?;
            self.validate_decoded_entries(&entries, &expected_tip)?;
            let last = entries
                .last()
                .ok_or_else(|| Error::InvalidCheckpoint("empty qlog segment".into()))?;
            expected_tip = CheckpointTip::new(last.index, last.hash);
            restored.extend(entries);
        }
        if expected_tip != loaded.manifest.tip {
            return Err(Error::InvalidCheckpoint(
                "restored entries do not match manifest tip".into(),
            ));
        }
        Ok(RestoredCheckpoint {
            snapshot,
            suffix: restored,
            tip: expected_tip,
        })
    }

    pub async fn publish_segment(
        &self,
        epoch: u64,
        segment: &SegmentFile,
    ) -> Result<SegmentRecord> {
        let range = segment.range();
        let object_key = format!(
            "rhiza/{}/archive/segments/epoch-{epoch:020}/{:020}-{:020}.qlog",
            self.cluster_id,
            range.start(),
            range.end()
        );
        let record = SegmentRecord {
            format_version: ARCHIVE_FORMAT_VERSION,
            cluster_id: self.cluster_id.clone(),
            epoch,
            start_index: range.start(),
            end_index: range.end(),
            object_key,
            sha256: sha256_hex(segment.bytes()),
            size_bytes: segment.bytes().len() as u64,
        };
        self.store
            .create(record.object_key(), segment.bytes())
            .await?;
        Ok(record)
    }

    pub async fn publish_snapshot(&self, snapshot: &Snapshot) -> Result<SnapshotRecord> {
        let manifest = snapshot.manifest();
        self.validate_record(
            "snapshot record",
            ARCHIVE_FORMAT_VERSION,
            manifest.cluster_id(),
        )?;
        let object_key = snapshot_object_key(manifest);
        let record = SnapshotRecord {
            format_version: ARCHIVE_FORMAT_VERSION,
            manifest: manifest.clone(),
            object_key,
            sha256: sha256_hex(snapshot.db_bytes()),
            size_bytes: snapshot.db_bytes().len() as u64,
        };
        self.store
            .create(record.object_key(), snapshot.db_bytes())
            .await?;
        Ok(record)
    }

    pub async fn download_segment(&self, record: &SegmentRecord) -> Result<Vec<u8>> {
        self.validate_record("segment record", record.format_version, &record.cluster_id)?;
        self.download_verified(record.object_key(), record.size_bytes, &record.sha256)
            .await
    }

    pub async fn download_snapshot(&self, record: &SnapshotRecord) -> Result<Vec<u8>> {
        self.validate_snapshot_record(record)?;
        self.download_verified(record.object_key(), record.size_bytes, &record.sha256)
            .await
    }

    pub async fn publish_manifest(
        &self,
        manifest: &ArchiveManifest,
        expected: Option<UpdateVersion>,
    ) -> Result<UpdateVersion> {
        self.validate_manifest(manifest)?;
        let bytes = serde_json::to_vec(manifest)
            .map_err(|error| Error::Serialization(error.to_string()))?;
        let key = self.manifest_key();
        match expected {
            Some(version) => self
                .store
                .update(&key, bytes, version)
                .await
                .map_err(Into::into),
            None => self.store.create(&key, bytes).await.map_err(Into::into),
        }
    }

    pub async fn load_manifest(&self) -> Result<Option<LoadedArchiveManifest>> {
        let object = match self.store.get_versioned(&self.manifest_key()).await {
            Ok(object) => object,
            Err(ObjStoreError::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let manifest: ArchiveManifest = serde_json::from_slice(object.bytes())
            .map_err(|error| Error::Serialization(error.to_string()))?;
        self.validate_manifest(&manifest)?;
        Ok(Some(LoadedArchiveManifest {
            manifest,
            version: object.version().clone(),
        }))
    }

    pub async fn acquire_gc_lease(
        &self,
        kind: GcLeaseKind,
        holder: &str,
        now_ms: u64,
        duration_ms: u64,
    ) -> Result<String> {
        if holder.trim().is_empty() || duration_ms == 0 {
            return Err(Error::InvalidGc(
                "lease holder and duration must be non-empty".into(),
            ));
        }
        self.acquire_named_lease(kind, holder.to_string(), now_ms, duration_ms)
            .await
            .map(|lease| lease.lease_id)
    }

    pub async fn set_gc_root(&self, root: CheckpointIdentity, now_ms: u64) -> Result<()> {
        self.validate_gc_identity(&root)?;
        self.ensure_gc_control().await?;
        for _ in 0..MAX_GC_CONTROL_CAS_ATTEMPTS {
            let mut loaded = self.load_gc_control().await?;
            self.expire_gc_state(&mut loaded.control, now_ms);
            if let Some(active) = &loaded.control.active_gc {
                return Err(Error::GcBarrierActive {
                    operation_id: active.operation_id.clone(),
                });
            }
            if !loaded
                .control
                .generations
                .iter()
                .any(|entry| entry.identity == root)
            {
                return Err(Error::InvalidGc("root generation is not cataloged".into()));
            }
            loaded.control.root = Some(root.clone());
            match self.update_gc_control(&loaded).await {
                Ok(_) => return Ok(()),
                Err(Error::ObjectStore(ObjStoreError::Precondition { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::CompareAndSwapRetriesExhausted {
            attempts: MAX_GC_CONTROL_CAS_ATTEMPTS,
        })
    }

    pub async fn abort_gc(&self, operation_id: &str) -> Result<()> {
        for _ in 0..MAX_GC_CONTROL_CAS_ATTEMPTS {
            let mut loaded = self.load_gc_control().await?;
            let Some(active) = &loaded.control.active_gc else {
                return Ok(());
            };
            if active.operation_id != operation_id {
                return Err(Error::GcBarrierActive {
                    operation_id: active.operation_id.clone(),
                });
            }
            loaded.control.active_gc = None;
            match self.update_gc_control(&loaded).await {
                Ok(_) => return Ok(()),
                Err(Error::ObjectStore(ObjStoreError::Precondition { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::CompareAndSwapRetriesExhausted {
            attempts: MAX_GC_CONTROL_CAS_ATTEMPTS,
        })
    }

    pub async fn plan_gc(&self, policy: GcPolicy, now_ms: u64) -> Result<GcPlan> {
        if policy.operation_id.trim().is_empty() {
            return Err(Error::InvalidGc("GC operation id must not be empty".into()));
        }
        self.validate_gc_identity(&policy.root)?;
        self.ensure_gc_control().await?;

        let loaded_control = self.load_gc_control().await?;
        let mut control = loaded_control.control.clone();
        self.expire_gc_state(&mut control, now_ms);
        if let Some(active) = &control.active_gc {
            return Err(Error::GcBarrierActive {
                operation_id: active.operation_id.clone(),
            });
        }
        if control.root.as_ref() != Some(&policy.root) {
            return Err(Error::GcPlanStale {
                message: "requested root is not the authoritative root".into(),
            });
        }
        let root_catalog = control
            .generations
            .iter()
            .find(|entry| entry.identity == policy.root)
            .ok_or_else(|| Error::InvalidGc("root generation is not cataloged".into()))?;
        if !matches!(root_catalog.lifecycle, GenerationLifecycle::Active) {
            return Err(Error::InvalidGc("root generation is retired".into()));
        }
        let root_key = checkpoint_namespace(&policy.root) + "/manifest.json";
        let root_object = self.store.get_versioned(&root_key).await?;
        let root_manifest: CheckpointManifest = deserialize_json(root_object.bytes())?;
        validate_checkpoint_identity(&policy.root, root_manifest.identity())?;
        self.validate_checkpoint_manifest(&root_manifest)?;
        let mut root_references = root_manifest
            .segments
            .iter()
            .map(|record| record.object_key.clone())
            .collect::<HashSet<_>>();
        if let CheckpointBase::Snapshot(snapshot) = &root_manifest.base {
            root_references.insert(snapshot.object_key.clone());
        }

        let mut generations = control
            .generations
            .iter()
            .filter(|entry| matches!(entry.lifecycle, GenerationLifecycle::Active))
            .cloned()
            .collect::<Vec<_>>();
        generations.sort_by_key(|entry| std::cmp::Reverse(entry.identity.recovery_generation()));
        let retained = generations
            .iter()
            .filter(|entry| entry.identity != policy.root)
            .take(policy.retain_recovery_generations)
            .map(|entry| entry.identity.clone())
            .collect::<Vec<_>>();
        let mut candidates = Vec::new();
        let mut swept_generations = Vec::new();
        for generation in generations {
            if generation.identity == policy.root {
                let prefix = checkpoint_namespace(&generation.identity) + "/";
                let metadata = self.store.list_metadata(&prefix).await?;
                for metadata in metadata {
                    if is_known_checkpoint_object(&generation.identity, metadata.key())
                        && !root_references.contains(metadata.key())
                        && now_ms.saturating_sub(metadata.last_modified_ms()) >= policy.min_age_ms
                    {
                        candidates.push(gc_candidate(
                            generation.identity.clone(),
                            metadata,
                            GcCandidateReason::UnreferencedCheckpointObject,
                        ));
                    }
                }
                continue;
            }
            if retained.contains(&generation.identity) {
                continue;
            }
            let prefix = checkpoint_namespace(&generation.identity) + "/";
            let metadata = self
                .store
                .list_metadata(&prefix)
                .await?
                .into_iter()
                .filter(|metadata| is_known_checkpoint_object(&generation.identity, metadata.key()))
                .collect::<Vec<_>>();
            if metadata.iter().any(|metadata| {
                now_ms.saturating_sub(metadata.last_modified_ms()) < policy.min_age_ms
            }) {
                continue;
            }
            swept_generations.push(generation.identity.clone());
            for metadata in metadata {
                candidates.push(gc_candidate(
                    generation.identity.clone(),
                    metadata,
                    GcCandidateReason::SupersededRecoveryGeneration,
                ));
            }
        }
        candidates.sort_by(|left, right| left.key.cmp(&right.key));
        let mut plan = GcPlan {
            format_version: GC_FORMAT_VERSION,
            operation_id: policy.operation_id,
            cluster_id: self.cluster_id.clone(),
            fence: control.fence,
            observed_control_version: loaded_control.version.clone().into(),
            catalog_sha256: hash_generation_catalog(&control.generations)?,
            observed_catalog: control.generations.clone(),
            root: policy.root,
            root_manifest_key: root_key,
            root_manifest_sha256: sha256_hex(root_object.bytes()),
            root_manifest_version: root_object.version().clone().into(),
            created_at_ms: now_ms,
            not_before_ms: now_ms.saturating_add(policy.grace_ms),
            min_age_ms: policy.min_age_ms,
            swept_generations,
            candidates,
            plan_hash: String::new(),
        };
        plan.plan_hash = hash_gc_plan(&plan)?;
        self.store
            .create(&self.gc_plan_key(&plan.plan_hash), serialize_json(&plan)?)
            .await?;
        Ok(plan)
    }

    pub async fn execute_gc(&self, plan_hash: &str, now_ms: u64) -> Result<GcExecutionReport> {
        if let Some(report) = self.load_gc_report(plan_hash).await? {
            return Ok(report);
        }
        let plan_bytes = self.store.get(&self.gc_plan_key(plan_hash)).await?;
        let plan: GcPlan = deserialize_json(&plan_bytes)?;
        let actual_hash = hash_gc_plan(&plan)?;
        if plan.plan_hash != plan_hash || actual_hash != plan_hash {
            return Err(Error::GcPlanHashMismatch {
                expected: plan_hash.to_string(),
                actual: actual_hash,
            });
        }
        if now_ms < plan.not_before_ms {
            return Err(Error::GcBarrierBusy {
                until_ms: plan.not_before_ms,
            });
        }
        if let Err(error) = self.acquire_gc_barrier(&plan, now_ms).await {
            return self.gc_report_after_stale(plan_hash, error).await;
        }
        if let Err(error) = self.enter_delete_phase(&plan, now_ms).await {
            return self.gc_report_after_stale(plan_hash, error).await;
        }
        if let Err(error) = self.fence_gc_root(&plan).await {
            return self.gc_report_after_stale(plan_hash, error).await;
        }
        if let Err(error) = self.retire_plan_generations(&plan, now_ms).await {
            return self.gc_report_after_stale(plan_hash, error).await;
        }

        let mut results = Vec::with_capacity(plan.candidates.len());
        for candidate in &plan.candidates {
            if let Err(error) = self.validate_gc_fence(&plan, now_ms).await {
                return self.gc_report_after_stale(plan_hash, error).await;
            }
            if !is_known_checkpoint_object(&candidate.generation, &candidate.key) {
                return Err(Error::InvalidGc(format!(
                    "candidate is outside a known checkpoint layout: {}",
                    candidate.key
                )));
            }
            if let Some(evidence) = self.load_gc_evidence(plan_hash, candidate).await? {
                results.push(evidence);
                continue;
            }
            let deleted = self
                .store
                .delete_exact(&candidate.key, &candidate.version)
                .await?;
            let evidence = GcEvidence {
                format_version: GC_FORMAT_VERSION,
                plan_hash: plan_hash.to_string(),
                key: candidate.key.clone(),
                version: candidate.version.clone(),
                outcome: if deleted {
                    GcDeleteOutcome::Deleted
                } else {
                    GcDeleteOutcome::AlreadyMissing
                },
                observed_at_ms: now_ms,
            };
            match self
                .store
                .create(
                    &self.gc_evidence_key(plan_hash, &candidate.key),
                    serialize_json(&evidence)?,
                )
                .await
            {
                Ok(_) => results.push(evidence),
                Err(ObjStoreError::AlreadyExists { .. }) => results.push(
                    self.load_gc_evidence(plan_hash, candidate)
                        .await?
                        .ok_or_else(|| Error::InvalidGc("deletion evidence disappeared".into()))?,
                ),
                Err(error) => return Err(error.into()),
            }
        }
        let report = GcExecutionReport {
            format_version: GC_FORMAT_VERSION,
            plan_hash: plan_hash.to_string(),
            fence: execution_fence(&plan),
            completed_at_ms: now_ms,
            results,
        };
        let report = match self
            .store
            .create(&self.gc_report_key(plan_hash), serialize_json(&report)?)
            .await
        {
            Ok(_) => report,
            Err(ObjStoreError::AlreadyExists { .. }) => self
                .load_gc_report(plan_hash)
                .await?
                .ok_or_else(|| Error::InvalidGc("execution report disappeared".into()))?,
            Err(error) => return Err(error.into()),
        };
        self.clear_gc_barrier(&plan).await?;
        Ok(report)
    }

    async fn gc_report_after_stale(
        &self,
        plan_hash: &str,
        error: Error,
    ) -> Result<GcExecutionReport> {
        if matches!(error, Error::GcPlanStale { .. }) {
            if let Some(report) = self.load_gc_report(plan_hash).await? {
                return Ok(report);
            }
        }
        Err(error)
    }

    pub fn gc_plan_key(&self, plan_hash: &str) -> String {
        format!("{}/plans/{plan_hash}.json", self.gc_prefix())
    }

    pub fn gc_evidence_prefix(&self, plan_hash: &str) -> String {
        format!("{}/evidence/{plan_hash}/", self.gc_prefix())
    }

    fn gc_evidence_key(&self, plan_hash: &str, key: &str) -> String {
        format!(
            "{}{}.json",
            self.gc_evidence_prefix(plan_hash),
            sha256_hex(key.as_bytes())
        )
    }

    fn gc_report_key(&self, plan_hash: &str) -> String {
        format!("{}/reports/{plan_hash}.json", self.gc_prefix())
    }

    fn gc_prefix(&self) -> String {
        format!("rhiza/{}/gc", self.cluster_id)
    }

    fn gc_control_key(&self) -> String {
        format!("{}/control.json", self.gc_prefix())
    }

    async fn acquire_operation_lease(
        &self,
        kind: GcLeaseKind,
        now_ms: u64,
        duration_ms: u64,
    ) -> Result<HeldLease> {
        let id = format!(
            "{}-{now_ms}-{}",
            process::id(),
            LEASE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        self.acquire_named_lease(kind, id, now_ms, duration_ms)
            .await
    }

    async fn acquire_named_lease(
        &self,
        kind: GcLeaseKind,
        lease_id: String,
        now_ms: u64,
        duration_ms: u64,
    ) -> Result<HeldLease> {
        self.ensure_gc_control().await?;
        for _ in 0..MAX_GC_CONTROL_CAS_ATTEMPTS {
            let mut loaded = self.load_gc_control().await?;
            self.expire_gc_state(&mut loaded.control, now_ms);
            if let Some(active) = &loaded.control.active_gc {
                return Err(Error::GcBarrierActive {
                    operation_id: active.operation_id.clone(),
                });
            }
            loaded
                .control
                .leases
                .retain(|lease| lease.lease_id != lease_id);
            loaded.control.leases.push(GcLease {
                lease_id: lease_id.clone(),
                kind,
                fence: loaded.control.fence,
                expires_at_ms: now_ms.saturating_add(duration_ms),
            });
            match self.update_gc_control(&loaded).await {
                Ok(_) => return Ok(HeldLease { lease_id }),
                Err(Error::ObjectStore(ObjStoreError::Precondition { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::CompareAndSwapRetriesExhausted {
            attempts: MAX_GC_CONTROL_CAS_ATTEMPTS,
        })
    }

    async fn release_gc_lease(&self, lease_id: &str) -> Result<()> {
        for _ in 0..MAX_GC_CONTROL_CAS_ATTEMPTS {
            let mut loaded = self.load_gc_control().await?;
            let before = loaded.control.leases.len();
            loaded
                .control
                .leases
                .retain(|lease| lease.lease_id != lease_id);
            if before == loaded.control.leases.len() {
                return Ok(());
            }
            match self.update_gc_control(&loaded).await {
                Ok(_) => return Ok(()),
                Err(Error::ObjectStore(ObjStoreError::Precondition { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::CompareAndSwapRetriesExhausted {
            attempts: MAX_GC_CONTROL_CAS_ATTEMPTS,
        })
    }

    async fn renew_gc_lease(
        &self,
        kind: GcLeaseKind,
        lease_id: &str,
        now_ms: u64,
        duration_ms: u64,
    ) -> Result<()> {
        for _ in 0..MAX_GC_CONTROL_CAS_ATTEMPTS {
            let mut loaded = self.load_gc_control().await?;
            self.expire_gc_state(&mut loaded.control, now_ms);
            if let Some(active) = &loaded.control.active_gc {
                if active.phase == GcBarrierPhase::Deleting {
                    return Err(Error::GcBarrierActive {
                        operation_id: active.operation_id.clone(),
                    });
                }
            }
            let expires_at_ms = now_ms.saturating_add(duration_ms);
            if let Some(lease) = loaded
                .control
                .leases
                .iter_mut()
                .find(|lease| lease.lease_id == lease_id)
            {
                if lease.kind != kind {
                    return Err(Error::InvalidGc("operation lease kind changed".into()));
                }
                lease.fence = loaded.control.fence;
                lease.expires_at_ms = expires_at_ms;
            } else {
                loaded.control.leases.push(GcLease {
                    lease_id: lease_id.to_string(),
                    kind,
                    fence: loaded.control.fence,
                    expires_at_ms,
                });
            }
            if let Some(active) = loaded.control.active_gc.as_mut() {
                active.expires_at_ms = active
                    .expires_at_ms
                    .max(expires_at_ms.saturating_add(DEFAULT_LEASE_MS));
            }
            match self.update_gc_control(&loaded).await {
                Ok(_) => return Ok(()),
                Err(Error::ObjectStore(ObjStoreError::Precondition { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::CompareAndSwapRetriesExhausted {
            attempts: MAX_GC_CONTROL_CAS_ATTEMPTS,
        })
    }

    async fn register_generation(&self, registered_at_ms: u64) -> Result<()> {
        let identity = self.checkpoint_identity()?.clone();
        for _ in 0..MAX_GC_CONTROL_CAS_ATTEMPTS {
            let mut loaded = self.load_gc_control().await?;
            if loaded
                .control
                .generations
                .iter()
                .any(|entry| entry.identity == identity)
            {
                return Ok(());
            }
            loaded.control.generations.push(GenerationCatalogEntry {
                identity: identity.clone(),
                manifest_key: self.checkpoint_manifest_key()?,
                registered_at_ms,
                lifecycle: GenerationLifecycle::Active,
            });
            loaded.control.generations.sort_by_key(|entry| {
                (
                    entry.identity.epoch(),
                    entry.identity.config_id(),
                    entry.identity.recovery_generation(),
                )
            });
            if loaded.control.root.is_none() {
                loaded.control.root = Some(identity.clone());
            }
            match self.update_gc_control(&loaded).await {
                Ok(_) => return Ok(()),
                Err(Error::ObjectStore(ObjStoreError::Precondition { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::CompareAndSwapRetriesExhausted {
            attempts: MAX_GC_CONTROL_CAS_ATTEMPTS,
        })
    }

    async fn acquire_gc_barrier(&self, plan: &GcPlan, now_ms: u64) -> Result<GcControl> {
        for _ in 0..MAX_GC_CONTROL_CAS_ATTEMPTS {
            let mut loaded = self.load_gc_control().await?;
            self.expire_gc_state(&mut loaded.control, now_ms);
            if let Some(active) = &loaded.control.active_gc {
                if active.plan_hash == plan.plan_hash
                    && active.root == plan.root
                    && active.fence == execution_fence(plan)
                {
                    return Ok(loaded.control);
                }
                return Err(Error::GcBarrierActive {
                    operation_id: active.operation_id.clone(),
                });
            }
            let fresh = loaded.control.fence == plan.fence
                && hash_generation_catalog(&loaded.control.generations)? == plan.catalog_sha256;
            let resumed = loaded.control.fence == execution_fence(plan)
                && catalog_matches_retired_plan(&loaded.control.generations, plan);
            if loaded.control.root.as_ref() != Some(&plan.root) || (!fresh && !resumed) {
                return Err(Error::GcPlanStale {
                    message: "catalog, root, or fence changed after planning".into(),
                });
            }
            loaded.control.fence = execution_fence(plan);
            loaded.control.active_gc = Some(ActiveGc {
                operation_id: plan.operation_id.clone(),
                plan_hash: plan.plan_hash.clone(),
                fence: loaded.control.fence,
                root: plan.root.clone(),
                expires_at_ms: now_ms.saturating_add(DEFAULT_LEASE_MS),
                phase: GcBarrierPhase::Draining,
            });
            match self.update_gc_control(&loaded).await {
                Ok(version) => {
                    loaded.version = version;
                    return Ok(loaded.control);
                }
                Err(Error::ObjectStore(ObjStoreError::Precondition { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::CompareAndSwapRetriesExhausted {
            attempts: MAX_GC_CONTROL_CAS_ATTEMPTS,
        })
    }

    async fn validate_gc_fence(&self, plan: &GcPlan, now_ms: u64) -> Result<()> {
        let loaded = self.load_gc_control().await?;
        let active = loaded
            .control
            .active_gc
            .as_ref()
            .ok_or_else(|| Error::GcPlanStale {
                message: "GC barrier is no longer active".into(),
            })?;
        if active.operation_id != plan.operation_id
            || active.plan_hash != plan.plan_hash
            || active.fence != execution_fence(plan)
            || active.root != plan.root
            || loaded.control.root.as_ref() != Some(&plan.root)
            || active.expires_at_ms <= now_ms
            || active.phase != GcBarrierPhase::Deleting
        {
            return Err(Error::GcPlanStale {
                message: "root or fence changed".into(),
            });
        }
        if let Some(until_ms) = loaded
            .control
            .leases
            .iter()
            .filter(|lease| lease.expires_at_ms > now_ms)
            .map(|lease| lease.expires_at_ms)
            .max()
        {
            return Err(Error::GcBarrierBusy { until_ms });
        }
        Ok(())
    }

    async fn enter_delete_phase(&self, plan: &GcPlan, now_ms: u64) -> Result<()> {
        for _ in 0..MAX_GC_CONTROL_CAS_ATTEMPTS {
            let mut loaded = self.load_gc_control().await?;
            let active = loaded
                .control
                .active_gc
                .as_ref()
                .ok_or_else(|| Error::GcPlanStale {
                    message: "GC barrier is no longer active".into(),
                })?;
            if active.operation_id != plan.operation_id
                || active.plan_hash != plan.plan_hash
                || active.fence != execution_fence(plan)
                || active.root != plan.root
                || loaded.control.root.as_ref() != Some(&plan.root)
                || active.expires_at_ms <= now_ms
            {
                return Err(Error::GcPlanStale {
                    message: "root or fence changed".into(),
                });
            }
            if active.phase == GcBarrierPhase::Deleting {
                return Ok(());
            }
            if let Some(until_ms) = loaded
                .control
                .leases
                .iter()
                .filter(|lease| lease.expires_at_ms > now_ms)
                .map(|lease| lease.expires_at_ms)
                .max()
            {
                return Err(Error::GcBarrierBusy { until_ms });
            }
            loaded
                .control
                .active_gc
                .as_mut()
                .expect("checked active GC")
                .phase = GcBarrierPhase::Deleting;
            match self.update_gc_control(&loaded).await {
                Ok(_) => return Ok(()),
                Err(Error::ObjectStore(ObjStoreError::Precondition { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::CompareAndSwapRetriesExhausted {
            attempts: MAX_GC_CONTROL_CAS_ATTEMPTS,
        })
    }

    async fn retire_plan_generations(&self, plan: &GcPlan, now_ms: u64) -> Result<()> {
        for _ in 0..MAX_GC_CONTROL_CAS_ATTEMPTS {
            let mut loaded = self.load_gc_control().await?;
            let active = loaded
                .control
                .active_gc
                .as_ref()
                .ok_or_else(|| Error::GcPlanStale {
                    message: "GC barrier is no longer active".into(),
                })?;
            if active.plan_hash != plan.plan_hash
                || active.fence != execution_fence(plan)
                || active.phase != GcBarrierPhase::Deleting
            {
                return Err(Error::GcPlanStale {
                    message: "execution fence changed before retirement".into(),
                });
            }
            let mut changed = false;
            for identity in &plan.swept_generations {
                let entry = loaded
                    .control
                    .generations
                    .iter_mut()
                    .find(|entry| entry.identity == *identity)
                    .ok_or_else(|| Error::GcPlanStale {
                        message: "planned generation disappeared from the catalog".into(),
                    })?;
                match &entry.lifecycle {
                    GenerationLifecycle::Active => {
                        entry.lifecycle = GenerationLifecycle::Retired {
                            plan_hash: plan.plan_hash.clone(),
                            retired_at_ms: now_ms,
                        };
                        changed = true;
                    }
                    GenerationLifecycle::Retired { plan_hash, .. }
                        if plan_hash == &plan.plan_hash => {}
                    GenerationLifecycle::Retired { .. } => {
                        return Err(Error::GcPlanStale {
                            message: "generation was retired by another plan".into(),
                        });
                    }
                }
            }
            if !changed {
                return Ok(());
            }
            match self.update_gc_control(&loaded).await {
                Ok(_) => return Ok(()),
                Err(Error::ObjectStore(ObjStoreError::Precondition { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::CompareAndSwapRetriesExhausted {
            attempts: MAX_GC_CONTROL_CAS_ATTEMPTS,
        })
    }

    async fn fence_gc_root(&self, plan: &GcPlan) -> Result<()> {
        let object = self.store.get_versioned(&plan.root_manifest_key).await?;
        if sha256_hex(object.bytes()) != plan.root_manifest_sha256 {
            return Err(Error::GcPlanStale {
                message: "root checkpoint manifest changed".into(),
            });
        }
        // Rewriting identical bytes with strong CAS invalidates every publisher version loaded
        // before the delete phase. Publishers that reload are stopped by the GC control fence.
        match self
            .store
            .update(
                &plan.root_manifest_key,
                object.bytes(),
                object.version().clone(),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(ObjStoreError::Precondition { .. }) => {
                let current = self.store.get_versioned(&plan.root_manifest_key).await?;
                if sha256_hex(current.bytes()) == plan.root_manifest_sha256 {
                    Ok(())
                } else {
                    Err(Error::GcPlanStale {
                        message: "root checkpoint manifest changed".into(),
                    })
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn ensure_generation_not_retired(&self) -> Result<()> {
        let identity = self.checkpoint_identity()?;
        let loaded = match self.load_gc_control().await {
            Ok(loaded) => loaded,
            Err(Error::ObjectStore(ObjStoreError::NotFound { .. })) => return Ok(()),
            Err(error) => return Err(error),
        };
        if let Some(GenerationCatalogEntry {
            lifecycle:
                GenerationLifecycle::Retired {
                    plan_hash,
                    retired_at_ms: _,
                },
            ..
        }) = loaded
            .control
            .generations
            .iter()
            .find(|entry| entry.identity == *identity)
        {
            return Err(Error::GenerationRetired {
                generation: identity.recovery_generation(),
                plan_hash: plan_hash.clone(),
            });
        }
        Ok(())
    }

    async fn clear_gc_barrier(&self, plan: &GcPlan) -> Result<()> {
        for _ in 0..MAX_GC_CONTROL_CAS_ATTEMPTS {
            let mut loaded = self.load_gc_control().await?;
            let Some(active) = &loaded.control.active_gc else {
                return Ok(());
            };
            if active.operation_id != plan.operation_id
                || active.plan_hash != plan.plan_hash
                || active.fence != execution_fence(plan)
            {
                return Err(Error::GcPlanStale {
                    message: "GC barrier changed before completion".into(),
                });
            }
            loaded.control.active_gc = None;
            loaded
                .control
                .leases
                .retain(|lease| lease.expires_at_ms > plan.created_at_ms);
            match self.update_gc_control(&loaded).await {
                Ok(_) => return Ok(()),
                Err(Error::ObjectStore(ObjStoreError::Precondition { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::CompareAndSwapRetriesExhausted {
            attempts: MAX_GC_CONTROL_CAS_ATTEMPTS,
        })
    }

    async fn load_gc_report(&self, plan_hash: &str) -> Result<Option<GcExecutionReport>> {
        match self.store.get(&self.gc_report_key(plan_hash)).await {
            Ok(bytes) => {
                let report: GcExecutionReport = deserialize_json(&bytes)?;
                if report.plan_hash != plan_hash {
                    return Err(Error::GcPlanHashMismatch {
                        expected: plan_hash.to_string(),
                        actual: report.plan_hash,
                    });
                }
                Ok(Some(report))
            }
            Err(ObjStoreError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn load_gc_evidence(
        &self,
        plan_hash: &str,
        candidate: &GcCandidate,
    ) -> Result<Option<GcEvidence>> {
        match self
            .store
            .get(&self.gc_evidence_key(plan_hash, &candidate.key))
            .await
        {
            Ok(bytes) => {
                let evidence: GcEvidence = deserialize_json(&bytes)?;
                if evidence.plan_hash != plan_hash
                    || evidence.key != candidate.key
                    || evidence.version != candidate.version
                {
                    return Err(Error::InvalidGc(
                        "stored deletion evidence does not match the plan".into(),
                    ));
                }
                Ok(Some(evidence))
            }
            Err(ObjStoreError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn ensure_gc_control(&self) -> Result<()> {
        let control = GcControl {
            format_version: GC_FORMAT_VERSION,
            cluster_id: self.cluster_id.clone(),
            fence: 0,
            root: None,
            generations: Vec::new(),
            leases: Vec::new(),
            active_gc: None,
        };
        match self
            .store
            .create(&self.gc_control_key(), serialize_json(&control)?)
            .await
        {
            Ok(_) | Err(ObjStoreError::AlreadyExists { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn load_gc_control(&self) -> Result<LoadedGcControl> {
        let object = self.store.get_versioned(&self.gc_control_key()).await?;
        let control: GcControl = deserialize_json(object.bytes())?;
        if control.format_version != GC_FORMAT_VERSION || control.cluster_id != self.cluster_id {
            return Err(Error::InvalidGc(
                "control identity or version mismatch".into(),
            ));
        }
        Ok(LoadedGcControl {
            control,
            version: object.version().clone(),
        })
    }

    async fn update_gc_control(&self, loaded: &LoadedGcControl) -> Result<UpdateVersion> {
        self.store
            .update(
                &self.gc_control_key(),
                serialize_json(&loaded.control)?,
                loaded.version.clone(),
            )
            .await
            .map_err(Into::into)
    }

    fn expire_gc_state(&self, control: &mut GcControl, now_ms: u64) {
        control.leases.retain(|lease| lease.expires_at_ms > now_ms);
        if control
            .active_gc
            .as_ref()
            .is_some_and(|active| active.expires_at_ms <= now_ms)
        {
            control.active_gc = None;
        }
    }

    fn validate_gc_identity(&self, identity: &CheckpointIdentity) -> Result<()> {
        if identity.cluster_id() != self.cluster_id {
            return Err(Error::ClusterMismatch {
                expected: self.cluster_id.clone(),
                actual: identity.cluster_id().to_string(),
            });
        }
        Ok(())
    }

    fn validate_publication_entries(&self, entries: &[LogEntry]) -> Result<()> {
        let identity = self.checkpoint_identity()?;
        for (position, entry) in entries.iter().enumerate() {
            validate_entry_identity(identity, entry)?;
            if entry.recompute_hash() != entry.hash {
                return Err(Error::InvalidCheckpoint(format!(
                    "entry {} hash is invalid",
                    entry.index
                )));
            }
            if let Some(previous) = position.checked_sub(1).and_then(|index| entries.get(index)) {
                if entry.index
                    != previous
                        .index
                        .checked_add(1)
                        .ok_or_else(|| Error::InvalidCheckpoint("entry index overflow".into()))?
                {
                    return Err(Error::InvalidCheckpoint(format!(
                        "entry index gap or overlap at {}",
                        entry.index
                    )));
                }
                if entry.prev_hash != previous.hash {
                    return Err(Error::InvalidCheckpoint(format!(
                        "entry hash chain mismatch at {}",
                        entry.index
                    )));
                }
            }
        }
        Ok(())
    }

    async fn publication_suffix_start(
        &self,
        manifest: &CheckpointManifest,
        entries: &[LogEntry],
    ) -> Result<Option<usize>> {
        let first = entries.first().expect("non-empty publication");
        let last = entries.last().expect("non-empty publication");
        let tip = manifest.tip;

        if tip.index >= last.index {
            let archived = self.archived_hash_at(manifest, last.index).await?;
            verify_publication_hash(last.index, archived, last.hash)?;
            return Ok(None);
        }

        let next_index = tip
            .index
            .checked_add(1)
            .ok_or_else(|| Error::InvalidCheckpoint("checkpoint tip index overflow".into()))?;
        if next_index < first.index {
            return Err(Error::InvalidCheckpoint(format!(
                "publication gap: checkpoint tip is {}, batch starts at {}",
                tip.index, first.index
            )));
        }

        if tip.index >= first.index {
            let offset = usize::try_from(tip.index - first.index)
                .map_err(|_| Error::InvalidCheckpoint("publication range is too large".into()))?;
            let boundary = entries.get(offset).ok_or_else(|| {
                Error::InvalidCheckpoint("checkpoint tip is outside publication batch".into())
            })?;
            let archived = self.archived_hash_at(manifest, tip.index).await?;
            verify_publication_hash(tip.index, archived, boundary.hash)?;
            return Ok(Some(offset + 1));
        }

        verify_publication_hash(first.index.saturating_sub(1), tip.hash, first.prev_hash)?;
        Ok(Some(0))
    }

    async fn archived_hash_at(
        &self,
        manifest: &CheckpointManifest,
        index: LogIndex,
    ) -> Result<LogHash> {
        let base_tip = manifest.base.tip();
        if index == base_tip.index {
            return Ok(base_tip.hash);
        }
        if index < base_tip.index {
            return Err(Error::InvalidCheckpoint(format!(
                "manifest base no longer covers index {index}"
            )));
        }
        let record = manifest
            .segments
            .iter()
            .find(|record| record.start_index <= index && index <= record.end_index)
            .ok_or_else(|| {
                Error::InvalidCheckpoint(format!("manifest does not cover index {index}"))
            })?;
        let entries = self.load_checkpoint_segment(record).await?;
        let offset = usize::try_from(index - record.start_index)
            .map_err(|_| Error::InvalidCheckpoint("checkpoint range is too large".into()))?;
        entries.get(offset).map(|entry| entry.hash).ok_or_else(|| {
            Error::InvalidCheckpoint(format!("segment does not cover index {index}"))
        })
    }

    fn checkpoint_segment_record(
        &self,
        decoded: &[LogEntry],
        bytes: &[u8],
    ) -> Result<CheckpointSegmentRecord> {
        let first = decoded
            .first()
            .ok_or_else(|| Error::InvalidCheckpoint("refusing to publish empty segment".into()))?;
        let last = decoded.last().expect("non-empty decoded segment");
        let object_key =
            checkpoint_segment_key(self.checkpoint_identity()?, first.index, last.index);
        Ok(CheckpointSegmentRecord {
            format_version: CHECKPOINT_SEGMENT_FORMAT_VERSION,
            start_index: first.index,
            end_index: last.index,
            first_prev_hash: first.prev_hash,
            last_hash: last.hash,
            object_key,
            sha256: sha256_hex(bytes),
            size_bytes: bytes.len() as u64,
        })
    }

    async fn load_checkpoint_segment(
        &self,
        record: &CheckpointSegmentRecord,
    ) -> Result<Vec<LogEntry>> {
        let bytes = self
            .download_verified(record.object_key(), record.size_bytes, &record.sha256)
            .await?;
        let identity = self.checkpoint_identity()?;
        let entries = decode_segment_for_cluster(&bytes, identity.cluster_id())
            .map_err(|error| Error::LogDecode(error.to_string()))?;
        for entry in &entries {
            validate_entry_identity(identity, entry)?;
        }
        let first = entries
            .first()
            .ok_or_else(|| Error::InvalidCheckpoint("empty qlog segment".into()))?;
        let last = entries.last().expect("non-empty qlog segment");
        let expected_count = record
            .end_index
            .checked_sub(record.start_index)
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| Error::InvalidCheckpoint("invalid segment range".into()))?;
        if entries.len() as u64 != expected_count
            || first.index != record.start_index
            || last.index != record.end_index
            || first.prev_hash != record.first_prev_hash
            || last.hash != record.last_hash
        {
            return Err(Error::InvalidCheckpoint(format!(
                "segment record metadata does not match decoded qlog {}",
                record.object_key
            )));
        }
        Ok(entries)
    }

    fn validate_decoded_entries(
        &self,
        entries: &[LogEntry],
        expected_tip: &CheckpointTip,
    ) -> Result<()> {
        let identity = self.checkpoint_identity()?;
        let mut index = expected_tip.index;
        let mut hash = expected_tip.hash;
        for entry in entries {
            validate_entry_identity(identity, entry)?;
            index = index
                .checked_add(1)
                .ok_or_else(|| Error::InvalidCheckpoint("entry index overflow".into()))?;
            if entry.index != index {
                return Err(Error::InvalidCheckpoint(format!(
                    "entry index gap or overlap: expected {index}, got {}",
                    entry.index
                )));
            }
            if entry.prev_hash != hash {
                return Err(Error::InvalidCheckpoint(format!(
                    "entry hash chain mismatch at {}",
                    entry.index
                )));
            }
            if entry.recompute_hash() != entry.hash {
                return Err(Error::InvalidCheckpoint(format!(
                    "entry {} hash is invalid",
                    entry.index
                )));
            }
            hash = entry.hash;
        }
        Ok(())
    }

    fn validate_checkpoint_manifest(&self, manifest: &CheckpointManifest) -> Result<()> {
        if manifest.format_version != CHECKPOINT_FORMAT_VERSION {
            return Err(Error::UnsupportedFormatVersion {
                object: "checkpoint manifest",
                version: manifest.format_version,
            });
        }
        validate_checkpoint_identity(self.checkpoint_identity()?, &manifest.identity)?;

        if let Some(transition) = &manifest.successor_transition {
            self.validate_successor_transition(transition, &manifest.base)?;
        }

        if let CheckpointBase::Snapshot(snapshot) = &manifest.base {
            if manifest
                .successor_transition
                .as_ref()
                .is_some_and(|transition| {
                    snapshot.anchor.configuration_state().config_id()
                        == transition.predecessor.config_id
                })
            {
                self.validate_transition_snapshot_base(snapshot)?;
            } else {
                self.validate_checkpoint_snapshot_base(snapshot)?;
            }
        }

        let base_tip = manifest.base.tip();
        let mut expected_start = base_tip
            .index
            .checked_add(1)
            .ok_or_else(|| Error::InvalidCheckpoint("checkpoint base index overflow".into()))?;
        let mut expected_hash = base_tip.hash;
        for record in &manifest.segments {
            if record.format_version != CHECKPOINT_SEGMENT_FORMAT_VERSION {
                return Err(Error::UnsupportedFormatVersion {
                    object: "checkpoint segment record",
                    version: record.format_version,
                });
            }
            if record.start_index > record.end_index {
                return Err(Error::InvalidCheckpoint(format!(
                    "segment {} has an inverted range",
                    record.object_key
                )));
            }
            if record.start_index > expected_start {
                return Err(Error::InvalidCheckpoint(format!(
                    "segment gap: expected start {expected_start}, got {}",
                    record.start_index
                )));
            }
            if record.start_index < expected_start {
                return Err(Error::InvalidCheckpoint(format!(
                    "segment overlap: expected start {expected_start}, got {}",
                    record.start_index
                )));
            }
            if record.first_prev_hash != expected_hash {
                return Err(Error::InvalidCheckpoint(format!(
                    "segment hash chain mismatch at {}",
                    record.start_index
                )));
            }
            let expected_key = checkpoint_segment_key(
                self.checkpoint_identity()?,
                record.start_index,
                record.end_index,
            );
            if record.object_key != expected_key {
                return Err(Error::InvalidCheckpoint(format!(
                    "segment object key mismatch: expected {expected_key}, got {}",
                    record.object_key
                )));
            }
            if record.size_bytes == 0 || LogHash::from_hex(&record.sha256).is_none() {
                return Err(Error::InvalidCheckpoint(format!(
                    "segment {} has invalid size or checksum metadata",
                    record.object_key
                )));
            }
            expected_start = record
                .end_index
                .checked_add(1)
                .ok_or_else(|| Error::InvalidCheckpoint("segment end index overflow".into()))?;
            expected_hash = record.last_hash;
        }

        let expected_tip = manifest
            .segments
            .last()
            .map(|record| CheckpointTip::new(record.end_index, record.last_hash))
            .unwrap_or(base_tip);
        if manifest.tip != expected_tip {
            return Err(Error::InvalidCheckpoint(
                "manifest tip does not match its immutable segments".into(),
            ));
        }
        Ok(())
    }

    fn validate_successor_transition(
        &self,
        transition: &CheckpointSuccessorTransition,
        base: &CheckpointBase,
    ) -> Result<()> {
        let identity = self.checkpoint_identity()?;
        if transition.predecessor.cluster_id != identity.cluster_id
            || transition.predecessor.epoch != identity.epoch
            || transition.predecessor.config_id.checked_add(1) != Some(identity.config_id)
            || transition.successor.cluster_id() != identity.cluster_id
            || transition.successor.predecessor_config_id() != transition.predecessor.config_id
            || transition.successor.config_id() != identity.config_id
        {
            return Err(Error::InvalidCheckpoint(
                "successor transition identity is invalid".into(),
            ));
        }
        validate_entry_identity(&transition.predecessor, &transition.stop_entry)?;
        let command = StoredCommand::new(
            transition.stop_entry.entry_type,
            transition.stop_entry.payload.clone(),
        );
        let recognized = ConfigChange::recognize(&command)
            .ok()
            .and_then(|change| change.successor().cloned());
        if transition.stop_entry.recompute_hash() != transition.stop_entry.hash
            || recognized.as_ref() != Some(&transition.successor)
        {
            return Err(Error::InvalidCheckpoint(
                "successor transition Stop entry is invalid".into(),
            ));
        }
        match base {
            CheckpointBase::Snapshot(snapshot) => {
                if snapshot.anchor.configuration_state().config_id()
                    != transition.predecessor.config_id
                {
                    return Ok(());
                }
                let stop = LogAnchor::new(transition.stop_entry.index, transition.stop_entry.hash);
                let expected = ConfigurationState::active(
                    transition.predecessor.config_id,
                    transition.successor.predecessor_config_digest(),
                )
                .validate_entry(&transition.stop_entry)
                .map_err(|_| {
                    Error::InvalidCheckpoint("successor transition Stop state is invalid".into())
                })?;
                if snapshot.anchor.configuration_state() != &expected
                    || snapshot.anchor.compacted() != &stop
                {
                    return Err(Error::InvalidCheckpoint(
                        "successor transition snapshot is not the exact predecessor Stop".into(),
                    ));
                }
            }
            CheckpointBase::Genesis => {
                return Err(Error::InvalidCheckpoint(
                    "successor transition requires a stopped snapshot base".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_recovery_anchor(&self, anchor: &RecoveryAnchor) -> Result<()> {
        if anchor.format_version() != RECOVERY_ANCHOR_FORMAT_VERSION {
            return Err(Error::UnsupportedFormatVersion {
                object: "recovery anchor",
                version: anchor.format_version(),
            });
        }
        let identity = self.checkpoint_identity()?;
        if anchor.cluster_id() != identity.cluster_id() {
            return Err(checkpoint_identity_mismatch(
                "cluster_id",
                identity.cluster_id(),
                anchor.cluster_id(),
            ));
        }
        if anchor.epoch() != identity.epoch() {
            return Err(checkpoint_identity_mismatch(
                "epoch",
                identity.epoch(),
                anchor.epoch(),
            ));
        }
        if anchor.config_id() != identity.config_id() {
            return Err(checkpoint_identity_mismatch(
                "config_id",
                identity.config_id(),
                anchor.config_id(),
            ));
        }
        if anchor.recovery_generation() != identity.recovery_generation() {
            return Err(checkpoint_identity_mismatch(
                "recovery_generation",
                identity.recovery_generation(),
                anchor.recovery_generation(),
            ));
        }
        if anchor.configuration_state().config_id() != anchor.config_id()
            || anchor
                .configuration_state()
                .stop()
                .is_some_and(|stop| stop != anchor.compacted())
        {
            return Err(Error::InvalidCheckpoint(
                "recovery anchor configuration state is invalid".into(),
            ));
        }
        Ok(())
    }

    fn validate_checkpoint_snapshot_base(&self, snapshot: &CheckpointSnapshotBase) -> Result<()> {
        self.validate_recovery_anchor(&snapshot.anchor)?;
        if snapshot.digest != snapshot.anchor.snapshot().digest() {
            return Err(Error::InvalidCheckpoint(
                "snapshot base digest does not match its recovery anchor".into(),
            ));
        }
        if snapshot.executor_fingerprint != snapshot.anchor.executor_fingerprint() {
            return Err(Error::InvalidCheckpoint(
                "snapshot base executor fingerprint does not match its recovery anchor".into(),
            ));
        }
        if snapshot.size_bytes == 0
            || snapshot.size_bytes != snapshot.anchor.snapshot().size_bytes()
        {
            return Err(Error::InvalidCheckpoint(
                "snapshot base size does not match its recovery anchor".into(),
            ));
        }
        let expected_key = checkpoint_snapshot_key(self.checkpoint_identity()?, &snapshot.anchor);
        if snapshot.object_key != expected_key {
            return Err(Error::InvalidCheckpoint(format!(
                "snapshot object key mismatch: expected {expected_key}, got {}",
                snapshot.object_key
            )));
        }
        Ok(())
    }

    fn validate_transition_snapshot_base(&self, snapshot: &CheckpointSnapshotBase) -> Result<()> {
        let identity = self.checkpoint_identity()?;
        if snapshot.anchor.cluster_id() != identity.cluster_id()
            || snapshot.anchor.epoch() != identity.epoch()
            || snapshot.anchor.recovery_generation() != identity.recovery_generation()
        {
            return Err(Error::InvalidCheckpoint(
                "successor transition snapshot identity is invalid".into(),
            ));
        }
        if snapshot.digest != snapshot.anchor.snapshot().digest()
            || snapshot.executor_fingerprint != snapshot.anchor.executor_fingerprint()
            || snapshot.size_bytes == 0
            || snapshot.size_bytes != snapshot.anchor.snapshot().size_bytes()
        {
            return Err(Error::InvalidCheckpoint(
                "successor transition snapshot metadata is invalid".into(),
            ));
        }
        let expected_key = checkpoint_snapshot_key(identity, &snapshot.anchor);
        if snapshot.object_key != expected_key {
            return Err(Error::InvalidCheckpoint(format!(
                "snapshot object key mismatch: expected {expected_key}, got {}",
                snapshot.object_key
            )));
        }
        Ok(())
    }

    fn manifest_key(&self) -> String {
        format!("rhiza/{}/archive/manifest.json", self.cluster_id)
    }

    fn validate_manifest(&self, manifest: &ArchiveManifest) -> Result<()> {
        self.validate_record(
            "archive manifest",
            manifest.format_version,
            &manifest.cluster_id,
        )?;
        if let Some(snapshot) = &manifest.latest_snapshot {
            self.validate_snapshot_record(snapshot)?;
        }
        for segment in &manifest.segments {
            self.validate_record(
                "segment record",
                segment.format_version,
                &segment.cluster_id,
            )?;
        }
        Ok(())
    }

    fn validate_snapshot_record(&self, record: &SnapshotRecord) -> Result<()> {
        self.validate_record(
            "snapshot record",
            record.format_version,
            record.manifest.cluster_id(),
        )?;
        let expected_key = snapshot_object_key(&record.manifest);
        if record.object_key != expected_key {
            return Err(Error::SnapshotIdentityMismatch {
                field: "object key",
                expected: expected_key,
                actual: record.object_key.clone(),
            });
        }
        Ok(())
    }

    fn validate_record(
        &self,
        object: &'static str,
        format_version: u32,
        cluster_id: &str,
    ) -> Result<()> {
        if format_version != ARCHIVE_FORMAT_VERSION {
            return Err(Error::UnsupportedFormatVersion {
                object,
                version: format_version,
            });
        }
        if cluster_id != self.cluster_id {
            return Err(Error::ClusterMismatch {
                expected: self.cluster_id.clone(),
                actual: cluster_id.to_string(),
            });
        }
        Ok(())
    }

    async fn download_verified(
        &self,
        object_key: &str,
        expected_size: u64,
        expected_sha256: &str,
    ) -> Result<Vec<u8>> {
        let bytes = self.store.get(object_key).await?;
        let actual_size = bytes.len() as u64;
        if actual_size != expected_size {
            return Err(Error::SizeMismatch {
                object_key: object_key.to_string(),
                expected: expected_size,
                actual: actual_size,
            });
        }
        let actual_sha256 = sha256_hex(&bytes);
        if actual_sha256 != expected_sha256 {
            return Err(Error::ChecksumMismatch {
                object_key: object_key.to_string(),
                expected: expected_sha256.to_string(),
                actual: actual_sha256,
            });
        }
        Ok(bytes)
    }
}

fn validate_checkpoint_identity(
    expected: &CheckpointIdentity,
    actual: &CheckpointIdentity,
) -> Result<()> {
    if expected.cluster_id != actual.cluster_id {
        return Err(checkpoint_identity_mismatch(
            "cluster_id",
            &expected.cluster_id,
            &actual.cluster_id,
        ));
    }
    if expected.epoch != actual.epoch {
        return Err(checkpoint_identity_mismatch(
            "epoch",
            expected.epoch,
            actual.epoch,
        ));
    }
    if expected.config_id != actual.config_id {
        return Err(checkpoint_identity_mismatch(
            "config_id",
            expected.config_id,
            actual.config_id,
        ));
    }
    if expected.recovery_generation != actual.recovery_generation {
        return Err(checkpoint_identity_mismatch(
            "recovery_generation",
            expected.recovery_generation,
            actual.recovery_generation,
        ));
    }
    Ok(())
}

fn validate_entry_identity(identity: &CheckpointIdentity, entry: &LogEntry) -> Result<()> {
    if identity.cluster_id != entry.cluster_id {
        return Err(checkpoint_identity_mismatch(
            "cluster_id",
            &identity.cluster_id,
            &entry.cluster_id,
        ));
    }
    if identity.epoch != entry.epoch {
        return Err(checkpoint_identity_mismatch(
            "epoch",
            identity.epoch,
            entry.epoch,
        ));
    }
    if identity.config_id != entry.config_id {
        return Err(checkpoint_identity_mismatch(
            "config_id",
            identity.config_id,
            entry.config_id,
        ));
    }
    Ok(())
}

fn checkpoint_identity_mismatch(
    field: &'static str,
    expected: impl ToString,
    actual: impl ToString,
) -> Error {
    Error::CheckpointIdentityMismatch {
        field,
        expected: expected.to_string(),
        actual: actual.to_string(),
    }
}

fn verify_publication_hash(index: LogIndex, expected: LogHash, actual: LogHash) -> Result<()> {
    if expected != actual {
        return Err(Error::PublicationConflict {
            index,
            expected: expected.to_hex(),
            actual: actual.to_hex(),
        });
    }
    Ok(())
}

fn checkpoint_namespace(identity: &CheckpointIdentity) -> String {
    format!(
        "rhiza/{}/checkpoints/epoch-{:020}/config-{:020}/generation-{:020}",
        identity.cluster_id, identity.epoch, identity.config_id, identity.recovery_generation
    )
}

fn checkpoint_segment_key(
    identity: &CheckpointIdentity,
    start_index: LogIndex,
    end_index: LogIndex,
) -> String {
    format!(
        "{}/segments/{start_index:020}-{end_index:020}.qlog",
        checkpoint_namespace(identity)
    )
}

fn checkpoint_snapshot_key(identity: &CheckpointIdentity, anchor: &RecoveryAnchor) -> String {
    let prefix = format!(
        "{}/snapshots/{:020}-{}-{}",
        checkpoint_namespace(identity),
        anchor.compacted().index(),
        anchor.compacted().hash().to_hex(),
        anchor.snapshot().digest().to_hex()
    );
    format!(
        "{prefix}-{}.snapshot",
        anchor.executor_fingerprint().to_hex()
    )
}

fn serialize_json(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| Error::Serialization(error.to_string()))
}

fn deserialize_json<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|error| Error::Serialization(error.to_string()))
}

fn snapshot_object_key(manifest: &SnapshotManifest) -> String {
    let prefix = format!(
        "rhiza/{}/archive/snapshots/epoch-{:020}/snapshot-{:020}",
        manifest.cluster_id(),
        manifest.epoch(),
        manifest.index()
    );
    format!(
        "{prefix}-{}.snapshot",
        manifest.executor_fingerprint().to_hex()
    )
}

fn gc_candidate(
    generation: CheckpointIdentity,
    metadata: ObjectMetadata,
    reason: GcCandidateReason,
) -> GcCandidate {
    GcCandidate {
        generation,
        key: metadata.key().to_string(),
        size_bytes: metadata.size_bytes(),
        last_modified_ms: metadata.last_modified_ms(),
        version: metadata.version().clone(),
        reason,
    }
}

fn is_known_checkpoint_segment(identity: &CheckpointIdentity, key: &str) -> bool {
    let prefix = checkpoint_namespace(identity) + "/segments/";
    let Some(file_name) = key.strip_prefix(&prefix) else {
        return false;
    };
    if file_name.contains('/') || !file_name.ends_with(".qlog") {
        return false;
    }
    let Some((start, end)) = file_name.trim_end_matches(".qlog").split_once('-') else {
        return false;
    };
    start.len() == 20
        && end.len() == 20
        && start.bytes().all(|byte| byte.is_ascii_digit())
        && end.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_known_checkpoint_snapshot(identity: &CheckpointIdentity, key: &str) -> bool {
    let prefix = checkpoint_namespace(identity) + "/snapshots/";
    let Some(file_name) = key.strip_prefix(&prefix) else {
        return false;
    };
    if file_name.contains('/') || !file_name.ends_with(".snapshot") {
        return false;
    }
    let parts = file_name
        .trim_end_matches(".snapshot")
        .split('-')
        .collect::<Vec<_>>();
    matches!(parts.len(), 3 | 4)
        && parts[0].len() == 20
        && parts[0].bytes().all(|byte| byte.is_ascii_digit())
        && parts[1..]
            .iter()
            .all(|part| part.len() == 64 && part.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

fn is_known_checkpoint_object(identity: &CheckpointIdentity, key: &str) -> bool {
    is_known_checkpoint_segment(identity, key) || is_known_checkpoint_snapshot(identity, key)
}

fn hash_gc_plan(plan: &GcPlan) -> Result<String> {
    let mut material = plan.clone();
    material.plan_hash.clear();
    Ok(sha256_hex(&serialize_json(&material)?))
}

fn hash_generation_catalog(catalog: &[GenerationCatalogEntry]) -> Result<String> {
    Ok(sha256_hex(&serialize_json(&catalog)?))
}

fn catalog_matches_retired_plan(catalog: &[GenerationCatalogEntry], plan: &GcPlan) -> bool {
    catalog.len() == plan.observed_catalog.len()
        && plan.observed_catalog.iter().all(|observed| {
            let Some(current) = catalog
                .iter()
                .find(|entry| entry.identity == observed.identity)
            else {
                return false;
            };
            if current.manifest_key != observed.manifest_key
                || current.registered_at_ms != observed.registered_at_ms
            {
                return false;
            }
            if plan.swept_generations.contains(&observed.identity) {
                matches!(
                    &current.lifecycle,
                    GenerationLifecycle::Retired { plan_hash, .. }
                        if plan_hash == &plan.plan_hash
                )
            } else {
                current.lifecycle == observed.lifecycle
            }
        })
}

fn execution_fence(plan: &GcPlan) -> u64 {
    plan.fence.saturating_add(1)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn sha256_hex(bytes: &[u8]) -> String {
    LogHash::digest(&[bytes]).to_hex()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rhiza_core::EntryType;
    use rhiza_obj_store::{Error as ObjStoreError, ObjStoreConfig};

    #[tokio::test]
    async fn publisher_continues_when_lease_is_expired_or_missing() {
        let (_dir, _store, archive) = fixture();
        let publisher = archive
            .open_checkpoint_publisher("publisher", CheckpointPublisherOptions::default())
            .await
            .unwrap();

        publisher.renew_at(100).await.unwrap();
        publisher.renew_at(60_101).await.unwrap();
        archive.release_gc_lease(&publisher.lease_id).await.unwrap();
        publisher.renew_at(60_102).await.unwrap();

        let loaded = publisher.publish_committed(&[entry()]).await.unwrap();
        assert_eq!(loaded.manifest().tip().index(), 1);
    }

    #[tokio::test]
    async fn gc_fence_wins_before_in_flight_manifest_cas() {
        let (_dir, store, archive, loaded, next, plan) = gc_race_fixture().await;
        archive
            .acquire_gc_lease(GcLeaseKind::Publisher, "publisher", 100, 10)
            .await
            .unwrap();
        archive.acquire_gc_barrier(&plan, 111).await.unwrap();
        archive.enter_delete_phase(&plan, 111).await.unwrap();
        archive.fence_gc_root(&plan).await.unwrap();

        assert!(matches!(
            store
                .update(
                    &archive.checkpoint_manifest_key().unwrap(),
                    serialize_json(&next).unwrap(),
                    loaded.version,
                )
                .await,
            Err(ObjStoreError::Precondition { .. })
        ));

        let candidate = &plan.candidates()[0];
        assert!(store
            .delete_exact(candidate.key(), candidate.version())
            .await
            .unwrap());
        let current = archive.load_checkpoint().await.unwrap().unwrap();
        assert!(current.manifest().segments().is_empty());
    }

    #[tokio::test]
    async fn manifest_cas_wins_before_gc_fence() {
        let (_dir, store, archive, loaded, next, plan) = gc_race_fixture().await;
        store
            .update(
                &archive.checkpoint_manifest_key().unwrap(),
                serialize_json(&next).unwrap(),
                loaded.version,
            )
            .await
            .unwrap();

        assert!(matches!(
            archive.execute_gc(plan.plan_hash(), 111).await,
            Err(Error::GcPlanStale { .. })
        ));
        assert!(store.get(plan.candidates()[0].key()).await.is_ok());
        assert_eq!(
            archive
                .load_checkpoint()
                .await
                .unwrap()
                .unwrap()
                .manifest()
                .segments()
                .len(),
            1
        );
    }

    async fn gc_race_fixture() -> (
        tempfile::TempDir,
        ObjStore,
        ObjectArchiveStore,
        LoadedCheckpointManifest,
        CheckpointManifest,
        GcPlan,
    ) {
        let (dir, store, archive) = fixture();
        let loaded = archive.initialize_checkpoint().await.unwrap();
        let entry = entry();
        let bytes = encode_segment(std::slice::from_ref(&entry));
        let record = archive
            .checkpoint_segment_record(std::slice::from_ref(&entry), &bytes)
            .unwrap();
        store.create(record.object_key(), bytes).await.unwrap();

        let mut next = loaded.manifest().clone();
        next.tip = CheckpointTip::new(entry.index, entry.hash);
        next.segments.push(record);
        archive.validate_checkpoint_manifest(&next).unwrap();
        archive.set_gc_root(identity(), 100).await.unwrap();
        let plan = archive
            .plan_gc(GcPolicy::new("gc-race", identity(), 0, 0, 0), 100)
            .await
            .unwrap();
        assert_eq!(plan.candidates().len(), 1);
        (dir, store, archive, loaded, next, plan)
    }

    fn fixture() -> (tempfile::TempDir, ObjStore, ObjectArchiveStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjStore::new(ObjStoreConfig::Local {
            root: dir.path().to_path_buf(),
        })
        .unwrap();
        let archive =
            ObjectArchiveStore::new_checkpoint_for_single_process(store.clone(), identity());
        (dir, store, archive)
    }

    fn identity() -> CheckpointIdentity {
        CheckpointIdentity::new("cluster-a", 7, 3, 1)
    }

    fn entry() -> LogEntry {
        let payload = b"entry".to_vec();
        let hash = LogEntry::calculate_hash(
            "cluster-a",
            1,
            7,
            3,
            EntryType::Command,
            LogHash::ZERO,
            &payload,
        );
        LogEntry {
            cluster_id: "cluster-a".into(),
            epoch: 7,
            config_id: 3,
            index: 1,
            entry_type: EntryType::Command,
            payload,
            prev_hash: LogHash::ZERO,
            hash,
        }
    }
}
