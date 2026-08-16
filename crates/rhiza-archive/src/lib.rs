use rhiza_core::{
    CheckpointGcAnchor, ConfigId, Epoch, ExternalEffectCommand, LogAnchor, LogEntry, LogHash,
    LogIndex, RecoveryAnchor, Snapshot, SnapshotManifest, MAX_EXTERNAL_EFFECT_BYTES,
    MAX_EXTERNAL_EFFECT_CHUNKS, RECOVERY_ANCHOR_FORMAT_VERSION,
};
use rhiza_log::{
    decode_segment_for_cluster_bounded, encode_segment, SegmentFile,
    QLOG_DECODED_ENTRY_OVERHEAD_BUDGET_BYTES,
};
#[cfg(test)]
use std::sync::{Arc, Mutex, OnceLock};
use std::{
    collections::HashSet,
    process,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rhiza_obj_store::{
    Error as ObjStoreError, ObjStore, ObjectMetadata, ObjectVersion, UpdateVersion, VersionedObject,
};
use serde::{Deserialize, Serialize};

pub const ARCHIVE_FORMAT_VERSION: u32 = 1;
pub const CHECKPOINT_FORMAT_VERSION: u32 = 3;
const CHECKPOINT_SEGMENT_FORMAT_VERSION: u32 = 1;
const MAX_CHECKPOINT_CAS_ATTEMPTS: usize = 16;
const CHECKPOINT_RESTORE_LIMITS: CheckpointRestoreLimits = CheckpointRestoreLimits {
    // Compact QEFX manifests can exceed 1 MiB at the default compaction
    // boundary, so keep a bounded 2 MiB manifest ceiling.
    manifest_encoded_bytes: 2 * 1024 * 1024,
    segment_count: 256,
    object_count: 257,
    object_encoded_bytes: 256 * 1024 * 1024,
    aggregate_encoded_bytes: 512 * 1024 * 1024,
    object_decoded_bytes: 256 * 1024 * 1024,
    aggregate_decoded_bytes: 512 * 1024 * 1024,
};
// Stable aggregate allowance for the final restored suffix Vec. The qlog
// budget separately covers each segment's LogEntry Vec and transient hash Vec;
// retaining both charges is deliberately conservative across segment moves.
const CHECKPOINT_RESTORED_ENTRY_OVERHEAD_BUDGET_BYTES: u64 = 256;
const _: () = assert!(
    std::mem::size_of::<LogEntry>() <= CHECKPOINT_RESTORED_ENTRY_OVERHEAD_BUDGET_BYTES as usize
);
const MAX_GC_CONTROL_CAS_ATTEMPTS: usize = 128;
// Seven total attempts plus equal-jitter sleeps remain below the GCS minimum
// 60s GCS Publisher lease while spanning multiple one-second mutation windows for
// a simultaneous three-peer startup.
const MAX_GC_CONTROL_THROTTLE_RETRIES: usize = 6;
const GC_CONTROL_THROTTLE_BACKOFF_BASE_MS: u64 = 100;
const GC_CONTROL_THROTTLE_BACKOFF_MAX_MS: u64 = 1_600;
const GC_FORMAT_VERSION: u32 = 1;
const GC_CONTROL_ENCODED_BYTES: u64 = 1024 * 1024;
const DEFAULT_LEASE_MS: u64 = 60_000;
// A reader can spend an arbitrarily long time awaiting one remote object.
// Renew well before the lease expires so that a live fetch remains fenced from
// GC, while leaving enough room for a transient control-plane retry.
const READER_LEASE_RENEW_DIVISOR: u64 = 3;
const PUBLISHER_LEASE_RENEW_DIVISOR: u64 = 3;
pub const DEFAULT_CHECKPOINT_COMPACTION_SEGMENTS: usize = 64;
static LEASE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
#[cfg(test)]
static NEXT_TEST_OBJECT_ARCHIVE_STORE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy)]
struct CheckpointRestoreLimits {
    manifest_encoded_bytes: u64,
    segment_count: usize,
    object_count: usize,
    object_encoded_bytes: u64,
    aggregate_encoded_bytes: u64,
    object_decoded_bytes: u64,
    aggregate_decoded_bytes: u64,
}

#[derive(Clone, Copy)]
struct CheckpointSnapshotPublicationPolicy {
    allow_empty_baseline: bool,
    limits: CheckpointRestoreLimits,
}

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
    RestoreLimitExceeded {
        resource: &'static str,
        object_key: Option<String>,
        limit: u64,
        actual: u64,
    },
    RestoreSizeOverflow {
        resource: &'static str,
    },
    PublicationConflict {
        index: LogIndex,
        expected: String,
        actual: String,
    },
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
    GcLeaseMissing {
        lease_id: String,
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
            Self::RestoreLimitExceeded {
                resource,
                object_key,
                limit,
                actual,
            } => {
                if let Some(object_key) = object_key {
                    write!(
                        f,
                        "checkpoint restore {resource} exceeds limit for {object_key}: limit {limit}, got at least {actual}"
                    )
                } else {
                    write!(
                        f,
                        "checkpoint restore {resource} exceeds limit: limit {limit}, got at least {actual}"
                    )
                }
            }
            Self::RestoreSizeOverflow { resource } => {
                write!(f, "checkpoint restore {resource} size overflow")
            }
            Self::PublicationConflict {
                index,
                expected,
                actual,
            } => write!(
                f,
                "checkpoint publication conflicts at index {index}: expected hash {expected}, got {actual}"
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
            Self::GcLeaseMissing { lease_id } => {
                write!(f, "object GC operation lease is no longer held: {lease_id}")
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

// Publisher operations may prove the same still-fresh lease several times per
// checkpoint append.  Avoid rewriting the shared GC-control CAS object until
// the last third of the lease, while still forcing a write after a fence change
// or during an active GC barrier.
fn publisher_lease_has_renewal_margin(lease: &GcLease, now_ms: u64, duration_ms: u64) -> bool {
    let renewal_margin_ms = (duration_ms / PUBLISHER_LEASE_RENEW_DIVISOR).max(1);
    lease.expires_at_ms.saturating_sub(now_ms) > renewal_margin_ms
}

fn gc_control_throttled(error: &ObjStoreError) -> bool {
    matches!(
        error,
        ObjStoreError::Transport { message, .. }
            if message.contains("429 Too Many Requests")
                || message.contains("<Code>SlowDown</Code>")
    )
}

fn gc_control_throttle_backoff(seed: &[u8], retry: usize) -> Duration {
    let exponent = u32::try_from(retry).unwrap_or(u32::MAX).min(16);
    let cap_ms = GC_CONTROL_THROTTLE_BACKOFF_BASE_MS
        .saturating_mul(1_u64.checked_shl(exponent).unwrap_or(u64::MAX))
        .min(GC_CONTROL_THROTTLE_BACKOFF_MAX_MS);
    let floor_ms = (cap_ms / 2).max(1);
    let retry_bytes = u64::try_from(retry).unwrap_or(u64::MAX).to_be_bytes();
    let digest = LogHash::digest(&[b"rhiza-gc-control-throttle-v1", seed, &retry_bytes]);
    let jitter = u64::from_be_bytes(digest.as_bytes()[..8].try_into().expect("eight bytes"));
    Duration::from_millis(floor_ms.saturating_add(jitter % (cap_ms - floor_ms + 1)))
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
    config_digest: LogHash,
    recovery_generation: u64,
}

impl CheckpointIdentity {
    pub fn new(
        cluster_id: impl Into<String>,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        recovery_generation: u64,
    ) -> Self {
        assert_ne!(
            config_digest,
            LogHash::ZERO,
            "checkpoint config digest must be nonzero"
        );
        Self {
            cluster_id: cluster_id.into(),
            epoch,
            config_id,
            config_digest,
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

    pub const fn config_digest(&self) -> LogHash {
        self.config_digest
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
    effects: Vec<CheckpointEffectRecord>,
}

/// Immutable archive references for one QEFX bundle. The manifest object is
/// the canonical QEFX command bytes themselves; no second effect codec exists.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointEffectRecord {
    entry_index: LogIndex,
    manifest_object_key: String,
    manifest_sha256: String,
    manifest_size_bytes: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    chunk_object_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    chunk_sha256: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    chunk_size_bytes: Vec<u64>,
}

impl CheckpointEffectRecord {
    pub const fn entry_index(&self) -> LogIndex {
        self.entry_index
    }
}

/// Verified bytes for one checkpoint-owned QEFX reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoredCheckpointEffect {
    manifest: Vec<u8>,
    chunks: Vec<Vec<u8>>,
}

impl RestoredCheckpointEffect {
    pub fn manifest(&self) -> &[u8] {
        &self.manifest
    }
    pub fn chunks(&self) -> &[Vec<u8>] {
        &self.chunks
    }
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

    pub fn effects(&self) -> &[CheckpointEffectRecord] {
        &self.effects
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointManifest {
    format_version: u32,
    identity: CheckpointIdentity,
    base: CheckpointBase,
    segments: Vec<CheckpointSegmentRecord>,
    tip: CheckpointTip,
}

impl CheckpointManifest {
    pub fn new(identity: CheckpointIdentity) -> Self {
        Self {
            format_version: CHECKPOINT_FORMAT_VERSION,
            identity,
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

struct CheckpointRestoreBudget {
    limits: CheckpointRestoreLimits,
    decoded_bytes: u64,
}

impl CheckpointRestoreBudget {
    fn new(manifest: &CheckpointManifest, limits: CheckpointRestoreLimits) -> Result<Self> {
        let decoded_bytes = manifest
            .base
            .snapshot()
            .map_or(0, CheckpointSnapshotBase::size_bytes);
        ensure_restore_limit(
            "aggregate decoded bytes",
            None,
            decoded_bytes,
            limits.aggregate_decoded_bytes,
        )?;
        Ok(Self {
            limits,
            decoded_bytes,
        })
    }

    fn next_object_limit(&self) -> Result<u64> {
        let aggregate_remaining = self
            .limits
            .aggregate_decoded_bytes
            .checked_sub(self.decoded_bytes)
            .ok_or(Error::RestoreSizeOverflow {
                resource: "aggregate decoded bytes",
            })?;
        Ok(self.limits.object_decoded_bytes.min(aggregate_remaining))
    }

    fn charge(&mut self, object_key: &str, decoded_bytes: u64) -> Result<()> {
        ensure_restore_limit(
            "object decoded bytes",
            Some(object_key),
            decoded_bytes,
            self.limits.object_decoded_bytes,
        )?;
        self.decoded_bytes =
            checked_restore_add("aggregate decoded bytes", self.decoded_bytes, decoded_bytes)?;
        ensure_restore_limit(
            "aggregate decoded bytes",
            None,
            self.decoded_bytes,
            self.limits.aggregate_decoded_bytes,
        )
    }

    fn charge_aggregate(&mut self, decoded_bytes: u64) -> Result<()> {
        self.decoded_bytes =
            checked_restore_add("aggregate decoded bytes", self.decoded_bytes, decoded_bytes)?;
        ensure_restore_limit(
            "aggregate decoded bytes",
            None,
            self.decoded_bytes,
            self.limits.aggregate_decoded_bytes,
        )
    }

    const fn decoded_bytes(&self) -> u64 {
        self.decoded_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CheckpointSuffixShape {
    entry_count: usize,
    stable_outer_bytes: u64,
}

struct PreparedCheckpointAppend {
    suffix_start: Option<usize>,
    candidate: Option<PreparedCheckpointAppendCandidate>,
}

struct PreparedCheckpointAppendCandidate {
    bytes: Vec<u8>,
    record: CheckpointSegmentRecord,
}

struct FinalizedCheckpointAppend {
    bytes: Vec<u8>,
    next: CheckpointManifest,
    next_bytes: Vec<u8>,
}

struct PreparedCheckpointSnapshot {
    proposed: CheckpointBase,
    next: Option<CheckpointManifest>,
    next_bytes: Option<Vec<u8>>,
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

/// A checkpoint manifest and the state restored from that exact manifest.
///
/// The pair is assembled while one reader lease is held. The lease protects
/// the manifest's referenced objects from GC, while the initially loaded
/// manifest (including its version) pins the logical checkpoint that is
/// restored. It does not freeze concurrent publication.
#[derive(Debug)]
pub struct LoadedCheckpointRestore {
    loaded: LoadedCheckpointManifest,
    restored: RestoredCheckpoint,
}

impl LoadedCheckpointRestore {
    pub const fn loaded(&self) -> &LoadedCheckpointManifest {
        &self.loaded
    }

    pub const fn restored(&self) -> &RestoredCheckpoint {
        &self.restored
    }

    pub fn into_parts(self) -> (LoadedCheckpointManifest, RestoredCheckpoint) {
        (self.loaded, self.restored)
    }
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

const MAX_CHECKPOINT_PUBLICATION_RECEIPT_BYTES: usize = 16 * 1024;

/// A bounded, durable encoding of the immutable result of one successful
/// checkpoint-manifest conditional write. Its fields stay private so callers
/// cannot assemble GC authority independently of an archive publication.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointPublicationReceipt {
    identity: CheckpointIdentity,
    tip: CheckpointTip,
    manifest_digest: LogHash,
    object_version: ObjectVersion,
}

impl CheckpointPublicationReceipt {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let bytes =
            serde_json::to_vec(self).map_err(|error| Error::Serialization(error.to_string()))?;
        if bytes.len() > MAX_CHECKPOINT_PUBLICATION_RECEIPT_BYTES {
            return Err(Error::Serialization(
                "checkpoint publication receipt exceeds its size limit".into(),
            ));
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_CHECKPOINT_PUBLICATION_RECEIPT_BYTES {
            return Err(Error::Serialization(
                "checkpoint publication receipt exceeds its size limit".into(),
            ));
        }
        let receipt: Self = serde_json::from_slice(bytes)
            .map_err(|error| Error::Serialization(error.to_string()))?;
        let canonical = receipt.encode()?;
        if canonical != bytes {
            return Err(Error::Serialization(
                "checkpoint publication receipt is not canonical".into(),
            ));
        }
        if receipt.identity.cluster_id().is_empty()
            || receipt.tip.index() == 0
            || receipt.tip.hash() == LogHash::ZERO
            || receipt.manifest_digest == LogHash::ZERO
        {
            return Err(Error::InvalidCheckpoint(
                "checkpoint publication receipt is incomplete".into(),
            ));
        }
        Ok(receipt)
    }

    pub fn gc_anchor(&self) -> Result<CheckpointGcAnchor> {
        CheckpointGcAnchor::new(
            self.identity.cluster_id(),
            self.identity.epoch(),
            self.identity.config_id(),
            self.identity.config_digest(),
            LogAnchor::new(self.tip.index(), self.tip.hash()),
            self.manifest_digest,
        )
        .ok_or_else(|| {
            Error::InvalidCheckpoint("checkpoint publication receipt is incomplete".into())
        })
    }
}

impl LoadedCheckpointManifest {
    pub const fn manifest(&self) -> &CheckpointManifest {
        &self.manifest
    }

    pub const fn version(&self) -> &UpdateVersion {
        &self.version
    }

    /// Returns immutable evidence for the exact manifest generation whose
    /// conditional create/update completed. A later valid manifest generation
    /// cannot invalidate this successful CAS receipt.
    pub fn publication_gc_anchor(&self) -> Result<CheckpointGcAnchor> {
        self.publication_receipt()?.gc_anchor()
    }

    pub fn publication_receipt(&self) -> Result<CheckpointPublicationReceipt> {
        let bytes = serialize_checkpoint_manifest(self.manifest())?;
        Ok(CheckpointPublicationReceipt {
            identity: self.manifest.identity().clone(),
            tip: *self.manifest.tip(),
            manifest_digest: LogHash::digest(&[&bytes]),
            object_version: self.version.clone().into(),
        })
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
    holder: String,
    lease_id: String,
    options: CheckpointPublisherOptions,
    operation: tokio::sync::Mutex<()>,
    state: tokio::sync::Mutex<CheckpointPublisherState>,
}

impl CheckpointPublisher {
    pub fn receipt_holder(&self) -> &str {
        &self.holder
    }

    pub async fn publish_receipt_for_loaded(
        &self,
        published: &LoadedCheckpointManifest,
    ) -> Result<CheckpointPublicationReceipt> {
        self.store
            .validate_checkpoint_manifest(published.manifest())?;
        self.store
            .publish_checkpoint_receipt(&self.holder, published)
            .await
    }

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
        self.drive_flushes_with_public_renewal().await;

        receiver.await.unwrap_or_else(|_| {
            Err(Error::InvalidCheckpoint(
                "publisher flush driver stopped before reporting a result".into(),
            ))
        })
    }

    /// Appends one exact batch together with the immutable QEFX references
    /// required to replay its external SQL effects.  This path is deliberately
    /// not coalesced: the references belong to this exact segment and are
    /// verified before its manifest CAS becomes visible.
    pub async fn publish_committed_with_effects(
        &self,
        entries: &[LogEntry],
        effects: &[CheckpointEffectRecord],
    ) -> Result<LoadedCheckpointManifest> {
        let _operation = self.operation.lock().await;
        self.renew().await?;
        let loaded = self.state.lock().await.loaded.clone();
        let published = self
            .store
            .publish_committed_from_loaded_unleased_with_effects_and_limits(
                entries,
                effects,
                &self.lease_id,
                self.options.lease_duration_ms,
                loaded,
                CHECKPOINT_RESTORE_LIMITS,
            )
            .await?;
        self.state.lock().await.loaded = published.clone();
        Ok(published)
    }

    pub async fn publish_checkpoint_snapshot(
        &self,
        anchor: RecoveryAnchor,
        snapshot_bytes: &[u8],
    ) -> Result<LoadedCheckpointManifest> {
        let _operation = self.operation.lock().await;
        self.renew().await?;
        let loaded = self.state.lock().await.loaded.clone();
        let published = self
            .store
            .publish_checkpoint_snapshot_from_loaded_unleased(
                anchor,
                snapshot_bytes,
                &self.lease_id,
                self.options.lease_duration_ms,
                loaded,
                false,
            )
            .await?;
        self.state.lock().await.loaded = published.clone();
        Ok(published)
    }

    /// Establishes a non-genesis snapshot as the first content in an empty checkpoint namespace.
    #[doc(hidden)]
    pub async fn publish_initial_checkpoint_snapshot(
        &self,
        anchor: RecoveryAnchor,
        snapshot_bytes: &[u8],
    ) -> Result<LoadedCheckpointManifest> {
        let _operation = self.operation.lock().await;
        self.renew().await?;
        let loaded = self.state.lock().await.loaded.clone();
        let published = self
            .store
            .publish_checkpoint_snapshot_from_loaded_unleased(
                anchor,
                snapshot_bytes,
                &self.lease_id,
                self.options.lease_duration_ms,
                loaded,
                true,
            )
            .await?;
        self.state.lock().await.loaded = published.clone();
        Ok(published)
    }

    pub async fn close(self) -> Result<()> {
        let _operation = self.operation.lock().await;
        self.store.release_gc_lease(&self.lease_id).await
    }

    #[cfg(test)]
    async fn drive_flushes(&self) {
        self.drive_flushes_inner(false).await;
    }

    async fn drive_flushes_with_public_renewal(&self) {
        self.drive_flushes_inner(true).await;
    }

    async fn drive_flushes_inner(&self, renew_missing_lease: bool) {
        let _operation = self.operation.lock().await;
        let renewal = if renew_missing_lease {
            self.renew().await
        } else {
            Ok(())
        };
        let (pending, loaded) = {
            let mut state = self.state.lock().await;
            if state.pending.is_empty() {
                return;
            }
            (std::mem::take(&mut state.pending), state.loaded.clone())
        };
        let published_index = loaded.manifest().tip().index();
        let published =
            match renewal.and_then(|()| coalesce_pending_entries(&pending, published_index)) {
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
                Ok(loaded) => {
                    let proof = async {
                        let prepared = self.store.prepare_local_checkpoint_append(
                            loaded.manifest(),
                            &flush.entries,
                            &[],
                            CHECKPOINT_RESTORE_LIMITS,
                        )?;
                        self.store
                            .renew_active_publisher_gc_lease(
                                &self.lease_id,
                                self.options.lease_duration_ms,
                            )
                            .await?;
                        self.store
                            .finalize_checkpoint_append_under_lease(
                                loaded.manifest(),
                                &flush.entries,
                                &[],
                                prepared,
                                CHECKPOINT_RESTORE_LIMITS,
                            )
                            .await
                    }
                    .await;
                    match proof {
                        Ok(None) => Ok(loaded.clone()),
                        Ok(Some(_)) => Err(Error::InvalidCheckpoint(
                            "coalesced publication did not reach the requested index".into(),
                        )),
                        Err(error) => Err(error),
                    }
                }
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
    #[cfg(test)]
    test_store_identity: u64,
}

#[cfg(test)]
#[derive(Clone)]
struct TestCheckpointDownloadGate {
    store_identity: u64,
    object_key: String,
    after_create: bool,
    entered: std::sync::mpsc::SyncSender<()>,
    cancelled: std::sync::mpsc::SyncSender<()>,
    released: Arc<std::sync::atomic::AtomicBool>,
    release_notification: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl TestCheckpointDownloadGate {
    fn new(
        store_identity: u64,
        object_key: impl Into<String>,
    ) -> (
        Self,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::Receiver<()>,
    ) {
        let (entered, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let (cancelled, cancelled_receiver) = std::sync::mpsc::sync_channel(1);
        (
            Self {
                store_identity,
                object_key: object_key.into(),
                after_create: false,
                entered,
                cancelled,
                released: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                release_notification: Arc::new(tokio::sync::Notify::new()),
            },
            entered_receiver,
            cancelled_receiver,
        )
    }

    fn after_create(
        store_identity: u64,
        object_key: impl Into<String>,
    ) -> (
        Self,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::Receiver<()>,
    ) {
        let (mut gate, entered, cancelled) = Self::new(store_identity, object_key);
        gate.after_create = true;
        (gate, entered, cancelled)
    }

    fn release_guard(&self) -> TestCheckpointFetchRelease {
        TestCheckpointFetchRelease {
            released: Arc::clone(&self.released),
            release_notification: Arc::clone(&self.release_notification),
        }
    }

    async fn wait(&self) {
        if self.entered.send(()).is_err() {
            return;
        }
        let mut cancellation = TestCheckpointFetchCancellation {
            released: Arc::clone(&self.released),
            cancelled: self.cancelled.clone(),
            armed: true,
        };
        loop {
            if self.released.load(Ordering::Acquire) {
                cancellation.armed = false;
                return;
            }
            let notified = self.release_notification.notified();
            if self.released.load(Ordering::Acquire) {
                cancellation.armed = false;
                return;
            }
            notified.await;
        }
    }
}

/// Releasing the test scope also releases a paused read if an assertion
/// unwinds. The hook is test-only and keyed by the exact archive instance and
/// object key below.
#[cfg(test)]
struct TestCheckpointFetchRelease {
    released: Arc<std::sync::atomic::AtomicBool>,
    release_notification: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl Drop for TestCheckpointFetchRelease {
    fn drop(&mut self) {
        self.released.store(true, Ordering::Release);
        self.release_notification.notify_waiters();
    }
}

/// Signals that the pending object read was dropped while still paused. This
/// makes the renewal-failure test prove cancellation rather than merely the
/// error returned by its parent restore operation.
#[cfg(test)]
struct TestCheckpointFetchCancellation {
    released: Arc<std::sync::atomic::AtomicBool>,
    cancelled: std::sync::mpsc::SyncSender<()>,
    armed: bool,
}

#[cfg(test)]
impl Drop for TestCheckpointFetchCancellation {
    fn drop(&mut self) {
        if self.armed && !self.released.load(Ordering::Acquire) {
            let _ = self.cancelled.send(());
        }
    }
}

#[cfg(test)]
#[derive(Clone)]
struct InstalledTestCheckpointDownloadGate {
    id: u64,
}

#[cfg(test)]
type TestCheckpointDownloadGateRegistry = Vec<(u64, Arc<TestCheckpointDownloadGate>)>;

#[cfg(test)]
static TEST_CHECKPOINT_DOWNLOAD_GATES: OnceLock<Mutex<TestCheckpointDownloadGateRegistry>> =
    OnceLock::new();

#[cfg(test)]
static NEXT_TEST_CHECKPOINT_DOWNLOAD_GATE: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
fn test_checkpoint_download_gates() -> &'static Mutex<TestCheckpointDownloadGateRegistry> {
    TEST_CHECKPOINT_DOWNLOAD_GATES.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
impl Drop for InstalledTestCheckpointDownloadGate {
    fn drop(&mut self) {
        test_checkpoint_download_gates()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|(id, _)| *id != self.id);
    }
}

#[cfg(test)]
fn install_test_checkpoint_download_gate(
    gate: TestCheckpointDownloadGate,
) -> InstalledTestCheckpointDownloadGate {
    let id = NEXT_TEST_CHECKPOINT_DOWNLOAD_GATE.fetch_add(1, Ordering::Relaxed);
    assert_ne!(id, 0, "checkpoint download test gate identity exhausted");
    let mut gates = test_checkpoint_download_gates()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    assert!(
        !gates.iter().any(|(_, existing)| {
            existing.store_identity == gate.store_identity
                && existing.object_key == gate.object_key
                && existing.after_create == gate.after_create
        }),
        "checkpoint download test gate already installed for this exact archive object"
    );
    gates.push((id, Arc::new(gate)));
    InstalledTestCheckpointDownloadGate { id }
}

#[cfg(test)]
async fn test_checkpoint_download_gate(store_identity: u64, object_key: &str) {
    let gate = test_checkpoint_download_gates()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .iter()
        .find(|(_, gate)| {
            gate.store_identity == store_identity
                && gate.object_key == object_key
                && !gate.after_create
        })
        .map(|(_, gate)| Arc::clone(gate));
    if let Some(gate) = gate {
        gate.wait().await;
    }
}

#[cfg(test)]
async fn test_checkpoint_object_created_gate(store_identity: u64, object_key: &str) {
    let gate = test_checkpoint_download_gates()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .iter()
        .find(|(_, gate)| {
            gate.store_identity == store_identity
                && gate.object_key == object_key
                && gate.after_create
        })
        .map(|(_, gate)| Arc::clone(gate));
    if let Some(gate) = gate {
        gate.wait().await;
    }
}

/// A test-only pause at the checkpoint manifest read. It is scoped to one
/// archive instance, not merely the logical manifest key, so parallel local
/// fixtures cannot observe or unblock each other.
#[cfg(test)]
#[derive(Clone)]
struct TestCheckpointManifestGate {
    store_identity: u64,
    entered: std::sync::mpsc::SyncSender<()>,
    cancelled: std::sync::mpsc::SyncSender<()>,
    released: Arc<std::sync::atomic::AtomicBool>,
    release_notification: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl TestCheckpointManifestGate {
    fn new(
        store_identity: u64,
    ) -> (
        Self,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::Receiver<()>,
    ) {
        let (entered, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let (cancelled, cancelled_receiver) = std::sync::mpsc::sync_channel(1);
        (
            Self {
                store_identity,
                entered,
                cancelled,
                released: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                release_notification: Arc::new(tokio::sync::Notify::new()),
            },
            entered_receiver,
            cancelled_receiver,
        )
    }

    fn release_guard(&self) -> TestCheckpointFetchRelease {
        TestCheckpointFetchRelease {
            released: Arc::clone(&self.released),
            release_notification: Arc::clone(&self.release_notification),
        }
    }

    async fn wait(&self) {
        if self.entered.send(()).is_err() {
            return;
        }
        let mut cancellation = TestCheckpointFetchCancellation {
            released: Arc::clone(&self.released),
            cancelled: self.cancelled.clone(),
            armed: true,
        };
        loop {
            if self.released.load(Ordering::Acquire) {
                cancellation.armed = false;
                return;
            }
            let notified = self.release_notification.notified();
            if self.released.load(Ordering::Acquire) {
                cancellation.armed = false;
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
#[derive(Clone)]
struct InstalledTestCheckpointManifestGate {
    id: u64,
}

#[cfg(test)]
type TestCheckpointManifestGateRegistry = Vec<(u64, Arc<TestCheckpointManifestGate>)>;

#[cfg(test)]
static TEST_CHECKPOINT_MANIFEST_GATES: OnceLock<Mutex<TestCheckpointManifestGateRegistry>> =
    OnceLock::new();

#[cfg(test)]
fn test_checkpoint_manifest_gates() -> &'static Mutex<TestCheckpointManifestGateRegistry> {
    TEST_CHECKPOINT_MANIFEST_GATES.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
impl Drop for InstalledTestCheckpointManifestGate {
    fn drop(&mut self) {
        test_checkpoint_manifest_gates()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|(id, _)| *id != self.id);
    }
}

#[cfg(test)]
fn install_test_checkpoint_manifest_gate(
    gate: TestCheckpointManifestGate,
) -> InstalledTestCheckpointManifestGate {
    let id = NEXT_TEST_CHECKPOINT_DOWNLOAD_GATE.fetch_add(1, Ordering::Relaxed);
    assert_ne!(id, 0, "checkpoint manifest test gate identity exhausted");
    let mut gates = test_checkpoint_manifest_gates()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    assert!(
        !gates
            .iter()
            .any(|(_, existing)| existing.store_identity == gate.store_identity),
        "checkpoint manifest test gate already installed for this exact archive instance"
    );
    gates.push((id, Arc::new(gate)));
    InstalledTestCheckpointManifestGate { id }
}

#[cfg(test)]
async fn test_checkpoint_manifest_gate(store_identity: u64) {
    let gate = test_checkpoint_manifest_gates()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .iter()
        .find(|(_, gate)| gate.store_identity == store_identity)
        .map(|(_, gate)| Arc::clone(gate));
    if let Some(gate) = gate {
        gate.wait().await;
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TestGcControlOperation {
    Load,
    Update,
    MutationError,
}

/// A test-only pause inside one exact GC-control operation. The gate is
/// installed only after Reader acquisition, so it cannot accidentally pause
/// setup traffic for the same archive instance.
#[cfg(test)]
#[derive(Clone)]
struct TestGcControlGate {
    store_identity: u64,
    operation: TestGcControlOperation,
    injected_error: Option<ObjStoreError>,
    injected_failures_remaining: Option<Arc<AtomicU64>>,
    minimum_update_interval: Option<Duration>,
    last_allowed_update: Option<Arc<Mutex<Option<std::time::Instant>>>>,
    entered: std::sync::mpsc::SyncSender<()>,
    released: Arc<std::sync::atomic::AtomicBool>,
    release_notification: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl TestGcControlGate {
    fn new(
        store_identity: u64,
        operation: TestGcControlOperation,
    ) -> (Self, std::sync::mpsc::Receiver<()>) {
        let (entered, entered_receiver) = std::sync::mpsc::sync_channel(1);
        (
            Self {
                store_identity,
                operation,
                injected_error: None,
                injected_failures_remaining: None,
                minimum_update_interval: None,
                last_allowed_update: None,
                entered,
                released: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                release_notification: Arc::new(tokio::sync::Notify::new()),
            },
            entered_receiver,
        )
    }

    fn failing_update(
        store_identity: u64,
        error: ObjStoreError,
    ) -> (Self, std::sync::mpsc::Receiver<()>) {
        let (mut gate, entered) = Self::new(store_identity, TestGcControlOperation::MutationError);
        gate.injected_error = Some(error);
        (gate, entered)
    }

    fn throttled_mutations(
        store_identity: u64,
        failures: u64,
    ) -> (Self, std::sync::mpsc::Receiver<()>) {
        let (mut gate, entered) = Self::new(store_identity, TestGcControlOperation::MutationError);
        gate.injected_error = Some(ObjStoreError::Transport {
            key: "gc/control.json".into(),
            message: "429 Too Many Requests: <Code>SlowDown</Code>".into(),
        });
        gate.injected_failures_remaining = Some(Arc::new(AtomicU64::new(failures)));
        (gate, entered)
    }

    fn rate_limited_mutations(
        store_identity: u64,
        interval: Duration,
    ) -> (Self, std::sync::mpsc::Receiver<()>) {
        let (mut gate, entered) = Self::new(store_identity, TestGcControlOperation::MutationError);
        gate.injected_error = Some(ObjStoreError::Transport {
            key: "gc/control.json".into(),
            message: "429 Too Many Requests: <Code>SlowDown</Code>".into(),
        });
        gate.minimum_update_interval = Some(interval);
        gate.last_allowed_update = Some(Arc::new(Mutex::new(None)));
        (gate, entered)
    }

    fn release_guard(&self) -> TestCheckpointFetchRelease {
        TestCheckpointFetchRelease {
            released: Arc::clone(&self.released),
            release_notification: Arc::clone(&self.release_notification),
        }
    }

    async fn wait(&self) -> Option<ObjStoreError> {
        if matches!(
            self.entered.try_send(()),
            Err(std::sync::mpsc::TrySendError::Disconnected(_))
        ) {
            return self.injected_error.clone();
        }
        loop {
            if self.released.load(Ordering::Acquire) {
                return self.next_error();
            }
            let notified = self.release_notification.notified();
            if self.released.load(Ordering::Acquire) {
                return self.next_error();
            }
            notified.await;
        }
    }

    fn next_error(&self) -> Option<ObjStoreError> {
        if let (Some(interval), Some(last_allowed)) =
            (self.minimum_update_interval, &self.last_allowed_update)
        {
            let now = std::time::Instant::now();
            let mut last_allowed = last_allowed
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let window_open = match *last_allowed {
                Some(last) => now.duration_since(last) >= interval,
                None => true,
            };
            if window_open {
                *last_allowed = Some(now);
                return None;
            }
            return self.injected_error.clone();
        }
        let Some(remaining) = &self.injected_failures_remaining else {
            return self.injected_error.clone();
        };
        remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(1)
            })
            .ok()
            .and_then(|_| self.injected_error.clone())
    }
}

#[cfg(test)]
#[derive(Clone)]
struct InstalledTestGcControlGate {
    id: u64,
}

#[cfg(test)]
type TestGcControlGateRegistry = Vec<(u64, Arc<TestGcControlGate>)>;

#[cfg(test)]
static TEST_GC_CONTROL_GATES: OnceLock<Mutex<TestGcControlGateRegistry>> = OnceLock::new();

#[cfg(test)]
static NEXT_TEST_GC_CONTROL_GATE: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
fn test_gc_control_gates() -> &'static Mutex<TestGcControlGateRegistry> {
    TEST_GC_CONTROL_GATES.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
impl Drop for InstalledTestGcControlGate {
    fn drop(&mut self) {
        test_gc_control_gates()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|(id, _)| *id != self.id);
    }
}

#[cfg(test)]
fn install_test_gc_control_gate(gate: TestGcControlGate) -> InstalledTestGcControlGate {
    let id = NEXT_TEST_GC_CONTROL_GATE.fetch_add(1, Ordering::Relaxed);
    assert_ne!(id, 0, "GC-control test gate identity exhausted");
    let mut gates = test_gc_control_gates()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    assert!(
        !gates.iter().any(|(_, existing)| {
            existing.store_identity == gate.store_identity && existing.operation == gate.operation
        }),
        "GC-control test gate already installed for this archive operation"
    );
    gates.push((id, Arc::new(gate)));
    InstalledTestGcControlGate { id }
}

#[cfg(test)]
async fn test_gc_control_gate(
    store_identity: u64,
    operation: TestGcControlOperation,
) -> Option<ObjStoreError> {
    let gate = test_gc_control_gates()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .iter()
        .find(|(_, gate)| gate.store_identity == store_identity && gate.operation == operation)
        .map(|(_, gate)| Arc::clone(gate));
    if let Some(gate) = gate {
        gate.wait().await
    } else {
        None
    }
}

impl ObjectArchiveStore {
    pub async fn checkpoint_readback_gc_anchor(
        &self,
        expected: &LoadedCheckpointManifest,
    ) -> Result<CheckpointGcAnchor> {
        let actual = self.load_checkpoint().await?.ok_or_else(|| {
            Error::InvalidCheckpoint("checkpoint disappeared before GC certificate readback".into())
        })?;
        if actual.manifest != expected.manifest || actual.version != expected.version {
            return Err(Error::InvalidCheckpoint(
                "checkpoint changed before GC certificate readback".into(),
            ));
        }
        actual.publication_gc_anchor()
    }
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
            #[cfg(test)]
            test_store_identity: NEXT_TEST_OBJECT_ARCHIVE_STORE.fetch_add(1, Ordering::Relaxed),
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
            #[cfg(test)]
            test_store_identity: NEXT_TEST_OBJECT_ARCHIVE_STORE.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn checkpoint_identity(&self) -> Result<&CheckpointIdentity> {
        self.checkpoint_identity
            .as_ref()
            .ok_or(Error::CheckpointUnbound)
    }

    /// Publishes one already quorum-verified QEFX bundle. Every immutable
    /// chunk and the canonical QEFX manifest are read back before this method
    /// returns a reference suitable for a later checkpoint-manifest CAS.
    pub async fn publish_verified_qefx_bundle(
        &self,
        entry: &LogEntry,
        chunks: &[Vec<u8>],
    ) -> Result<CheckpointEffectRecord> {
        let identity = self.checkpoint_identity()?;
        let command = ExternalEffectCommand::decode(&entry.payload).map_err(|error| {
            Error::InvalidCheckpoint(format!("invalid QEFX archive entry: {error}"))
        })?;
        if command.cluster_id() != identity.cluster_id
            || command.epoch() != identity.epoch
            || command.config_id() != identity.config_id
            || command.config_digest() != identity.config_digest
            || command.intended_slot() != entry.index
            || command.prev_hash() != entry.prev_hash
            || chunks.len() != command.chunks().len()
            || chunks.len() > MAX_EXTERNAL_EFFECT_CHUNKS
        {
            return Err(Error::InvalidCheckpoint(
                "QEFX archive binding mismatch".into(),
            ));
        }
        let total = chunks
            .iter()
            .try_fold(0usize, |sum, chunk| sum.checked_add(chunk.len()))
            .ok_or(Error::InvalidCheckpoint(
                "QEFX archive size overflow".into(),
            ))?;
        if total > MAX_EXTERNAL_EFFECT_BYTES || total != command.total_effect_bytes() as usize {
            return Err(Error::InvalidCheckpoint(
                "QEFX archive size exceeds canonical bound".into(),
            ));
        }
        let prefix = checkpoint_effect_prefix(identity, entry.index, command.effect_digest_value());
        for (ordinal, (chunk, expected)) in chunks.iter().zip(command.chunks()).enumerate() {
            if chunk.len() != expected.encoded_len() as usize
                || ExternalEffectCommand::chunk_digest(chunk) != expected.digest()
            {
                return Err(Error::InvalidCheckpoint(
                    "QEFX archive chunk mismatch".into(),
                ));
            }
            let key = format!(
                "{prefix}/chunks/{ordinal:03}-{}.qefc",
                expected.digest().to_hex()
            );
            match self.store.create(&key, chunk).await {
                Ok(_) | Err(ObjStoreError::AlreadyExists { .. }) => {}
                Err(error) => return Err(error.into()),
            }
            let sha256 = sha256_hex(chunk);
            self.download_verified(&key, chunk.len() as u64, &sha256)
                .await?;
        }
        let manifest_object_key = format!("{prefix}/binding.qefx");
        match self
            .store
            .create(&manifest_object_key, &entry.payload)
            .await
        {
            Ok(_) | Err(ObjStoreError::AlreadyExists { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        let manifest_sha256 = sha256_hex(&entry.payload);
        let manifest = self
            .download_verified(
                &manifest_object_key,
                entry.payload.len() as u64,
                &manifest_sha256,
            )
            .await?;
        if ExternalEffectCommand::decode(&manifest).map_err(|error| {
            Error::InvalidCheckpoint(format!("invalid archived QEFX binding: {error}"))
        })? != command
        {
            return Err(Error::InvalidCheckpoint(
                "archived QEFX binding readback differs".into(),
            ));
        }
        Ok(CheckpointEffectRecord {
            entry_index: entry.index,
            manifest_object_key,
            manifest_sha256,
            manifest_size_bytes: entry.payload.len() as u64,
            // The canonical QEFX binding already commits the ordered chunk
            // lengths and digests, and the archive keys are deterministic.
            // Keep accepting the three legacy arrays on restore, but do not
            // repeat them in every root checkpoint manifest record.
            chunk_object_keys: Vec::new(),
            chunk_sha256: Vec::new(),
            chunk_size_bytes: Vec::new(),
        })
    }

    pub async fn restore_checkpoint_effect(
        &self,
        effect: &CheckpointEffectRecord,
    ) -> Result<RestoredCheckpointEffect> {
        let manifest = self
            .download_verified(
                &effect.manifest_object_key,
                effect.manifest_size_bytes,
                &effect.manifest_sha256,
            )
            .await?;
        let command = ExternalEffectCommand::decode(&manifest).map_err(|error| {
            Error::InvalidCheckpoint(format!("invalid archived QEFX binding: {error}"))
        })?;
        let identity = self.checkpoint_identity()?;
        let prefix =
            checkpoint_effect_prefix(identity, effect.entry_index, command.effect_digest_value());
        let expected_manifest_key = format!("{prefix}/binding.qefx");
        let compact = effect.chunk_object_keys.is_empty()
            && effect.chunk_sha256.is_empty()
            && effect.chunk_size_bytes.is_empty();
        let legacy = effect.chunk_object_keys.len() == command.chunks().len()
            && effect.chunk_sha256.len() == command.chunks().len()
            && effect.chunk_size_bytes.len() == command.chunks().len();
        if command.cluster_id() != identity.cluster_id()
            || command.epoch() != identity.epoch()
            || command.config_id() != identity.config_id()
            || command.config_digest() != identity.config_digest()
            || command.intended_slot() != effect.entry_index
            || effect.manifest_object_key != expected_manifest_key
            || (!compact && !legacy)
        {
            return Err(Error::InvalidCheckpoint(
                "archived QEFX reference shape mismatch".into(),
            ));
        }
        let mut chunks = Vec::with_capacity(command.chunks().len());
        for (ordinal, expected) in command.chunks().iter().enumerate() {
            let expected_key = format!(
                "{prefix}/chunks/{ordinal:03}-{}.qefc",
                expected.digest().to_hex()
            );
            let chunk = if compact {
                self.store
                    .get_bounded(&expected_key, u64::from(expected.encoded_len()))
                    .await
                    .map_err(|error| map_restore_object_error(error, "object encoded bytes"))?
            } else {
                let key = &effect.chunk_object_keys[ordinal];
                let size = effect.chunk_size_bytes[ordinal];
                if key != &expected_key || size != u64::from(expected.encoded_len()) {
                    return Err(Error::InvalidCheckpoint(
                        "archived QEFX chunk reference differs from binding".into(),
                    ));
                }
                self.download_verified(key, size, &effect.chunk_sha256[ordinal])
                    .await?
            };
            if chunk.len() != expected.encoded_len() as usize
                || ExternalEffectCommand::chunk_digest(&chunk) != expected.digest()
            {
                return Err(Error::InvalidCheckpoint(
                    "archived QEFX chunk differs from binding".into(),
                ));
            }
            chunks.push(chunk);
        }
        Ok(RestoredCheckpointEffect { manifest, chunks })
    }

    pub fn checkpoint_manifest_key(&self) -> Result<String> {
        Ok(checkpoint_namespace(self.checkpoint_identity()?) + "/manifest.json")
    }

    async fn publish_checkpoint_receipt(
        &self,
        holder: &str,
        published: &LoadedCheckpointManifest,
    ) -> Result<CheckpointPublicationReceipt> {
        let receipt = published.publication_receipt()?;
        let identity = self.checkpoint_identity()?;
        if receipt.identity != *identity {
            return Err(Error::InvalidCheckpoint(
                "checkpoint receipt identity differs from this archive".into(),
            ));
        }
        let bytes = receipt.encode()?;
        let key = checkpoint_publication_receipt_key(identity, holder, receipt.manifest_digest)?;
        match self.store.create(&key, &bytes).await {
            Ok(_) | Err(ObjStoreError::AlreadyExists { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        let readback = self
            .store
            .get_bounded(
                &key,
                u64::try_from(MAX_CHECKPOINT_PUBLICATION_RECEIPT_BYTES)
                    .expect("receipt bound fits u64"),
            )
            .await?;
        if readback != bytes || CheckpointPublicationReceipt::decode(&readback)? != receipt {
            return Err(Error::InvalidCheckpoint(
                "checkpoint publication receipt readback differs".into(),
            ));
        }
        Ok(receipt)
    }

    pub async fn load_checkpoint_receipt(
        &self,
        holder: &str,
        manifest_digest: LogHash,
    ) -> Result<CheckpointPublicationReceipt> {
        let identity = self.checkpoint_identity()?;
        let key = checkpoint_publication_receipt_key(identity, holder, manifest_digest)?;
        let bytes = self
            .store
            .get_bounded(
                &key,
                u64::try_from(MAX_CHECKPOINT_PUBLICATION_RECEIPT_BYTES)
                    .expect("receipt bound fits u64"),
            )
            .await?;
        let receipt = CheckpointPublicationReceipt::decode(&bytes)?;
        if receipt.identity != *identity || receipt.manifest_digest != manifest_digest {
            return Err(Error::InvalidCheckpoint(
                "checkpoint publication receipt binding differs".into(),
            ));
        }
        Ok(receipt)
    }

    pub async fn prune_checkpoint_receipts_through(
        &self,
        holder: &str,
        through_slot: LogIndex,
    ) -> Result<()> {
        let identity = self.checkpoint_identity()?;
        let holder_hash =
            LogHash::digest(&[b"rhiza-checkpoint-receipt-holder-v1\0", holder.as_bytes()]);
        let prefix = format!(
            "{}/receipts/{}/",
            checkpoint_namespace(identity),
            holder_hash.to_hex()
        );
        for metadata in self.store.list_metadata(&prefix).await? {
            let bytes = self
                .store
                .get_bounded(
                    metadata.key(),
                    u64::try_from(MAX_CHECKPOINT_PUBLICATION_RECEIPT_BYTES)
                        .expect("receipt bound fits u64"),
                )
                .await?;
            let receipt = CheckpointPublicationReceipt::decode(&bytes)?;
            if receipt.identity != *identity {
                return Err(Error::InvalidCheckpoint(
                    "checkpoint receipt cleanup found a foreign identity".into(),
                ));
            }
            if receipt.tip.index() <= through_slot {
                self.store
                    .delete_exact(metadata.key(), metadata.version())
                    .await?;
            }
        }
        Ok(())
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
            holder,
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
        let bytes = serialize_checkpoint_manifest(&manifest)?;
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

    /// Loads one manifest and restores only the objects named by that manifest.
    ///
    /// A missing manifest is distinct from an initialized genesis manifest:
    /// the former returns `None`, and the latter returns `Some` with a genesis
    /// [`RestoredCheckpoint`]. Operation errors take precedence over a later
    /// lease-release error, matching every other leased archive operation.
    pub async fn load_checkpoint_restore(&self) -> Result<Option<LoadedCheckpointRestore>> {
        self.load_checkpoint_restore_with_reader_lease_duration(DEFAULT_LEASE_MS)
            .await
    }

    async fn load_checkpoint_restore_with_reader_lease_duration(
        &self,
        lease_duration_ms: u64,
    ) -> Result<Option<LoadedCheckpointRestore>> {
        // This private duration seam is exercised by short-lease tests. Keep
        // a future caller that accidentally supplies zero from producing a
        // zero-length interval or an immediately expired reader lease.
        let lease_duration_ms = lease_duration_ms.max(1);
        let hard_deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_millis(lease_duration_ms))
            .ok_or_else(|| Error::InvalidGc("reader lease deadline overflow".into()))?;
        let acquired = self
            .acquire_operation_lease(GcLeaseKind::Reader, now_ms(), lease_duration_ms)
            .await;
        let acquired_at = tokio::time::Instant::now();
        let lease = acquired?;
        if acquired_at >= hard_deadline {
            let _ = self.release_gc_lease(&lease.lease_id).await;
            return Err(Error::GcLeaseMissing {
                lease_id: lease.lease_id,
            });
        }
        let result = self
            .with_reader_lease_renewal(&lease.lease_id, lease_duration_ms, hard_deadline, async {
                // This complete remote half, including the generation
                // control read and manifest read, stays behind one reader
                // lease. No local mutation is possible here.
                self.ensure_generation_not_retired().await?;
                let Some(loaded) = self.load_checkpoint_unleased().await? else {
                    return Ok(None);
                };
                // `with_reader_lease_renewal` is the sole renewal owner for
                // this complete remote restore. In particular, the object
                // reads below must not start nested renewal loops: a single
                // failed renewal has to cancel the whole pinned operation.
                let restored = self
                    .restore_loaded_checkpoint_with_active_reader_lease(&loaded)
                    .await?;
                Ok(Some(LoadedCheckpointRestore { loaded, restored }))
            })
            .await;
        let release = self.release_gc_lease(&lease.lease_id).await;
        match (result, release) {
            (Ok(restored), Ok(())) => Ok(restored),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }

    async fn load_checkpoint_unleased(&self) -> Result<Option<LoadedCheckpointManifest>> {
        self.checkpoint_identity()?;
        #[cfg(test)]
        test_checkpoint_manifest_gate(self.test_store_identity).await;
        let object = match self
            .store
            .get_with_version_bounded(
                &self.checkpoint_manifest_key()?,
                CHECKPOINT_RESTORE_LIMITS.manifest_encoded_bytes,
            )
            .await
        {
            Ok(object) => object,
            Err(ObjStoreError::NotFound { .. }) => return Ok(None),
            Err(error) => {
                return Err(map_restore_object_error(error, "manifest encoded bytes"));
            }
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
        self.publish_committed_with_effects(entries, &[]).await
    }

    /// Atomically publishes `entries` and their already-read-back QEFX
    /// references in the same checkpoint manifest generation.
    pub async fn publish_committed_with_effects(
        &self,
        entries: &[LogEntry],
        effects: &[CheckpointEffectRecord],
    ) -> Result<LoadedCheckpointManifest> {
        self.publish_committed_with_effects_and_limits(entries, effects, CHECKPOINT_RESTORE_LIMITS)
            .await
    }

    async fn publish_committed_with_effects_and_limits(
        &self,
        entries: &[LogEntry],
        effects: &[CheckpointEffectRecord],
        limits: CheckpointRestoreLimits,
    ) -> Result<LoadedCheckpointManifest> {
        self.validate_publication_entries_with_effects(entries, effects)?;
        self.ensure_generation_not_retired().await?;
        let observed = self.load_checkpoint_unleased().await?;
        if let Some(loaded) = &observed {
            self.prepare_local_checkpoint_append(loaded.manifest(), entries, effects, limits)?;
            if entries.is_empty() {
                return Ok(loaded.clone());
            }
        } else {
            let genesis = CheckpointManifest::new(self.checkpoint_identity()?.clone());
            self.prepare_local_checkpoint_append(&genesis, entries, effects, limits)?;
        }
        let lease = self
            .acquire_operation_lease(GcLeaseKind::Publisher, now_ms(), DEFAULT_LEASE_MS)
            .await?;
        let result = self
            .publish_committed_unleased_with_effects_and_limits(
                entries,
                effects,
                &lease.lease_id,
                DEFAULT_LEASE_MS,
                limits,
            )
            .await;
        let release = self.release_gc_lease(&lease.lease_id).await;
        match (result, release) {
            (Ok(loaded), Ok(())) => Ok(loaded),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }

    #[cfg(test)]
    async fn publish_committed_with_limits(
        &self,
        entries: &[LogEntry],
        limits: CheckpointRestoreLimits,
    ) -> Result<LoadedCheckpointManifest> {
        self.publish_committed_with_effects_and_limits(entries, &[], limits)
            .await
    }

    async fn publish_committed_unleased_with_effects_and_limits(
        &self,
        entries: &[LogEntry],
        effects: &[CheckpointEffectRecord],
        lease_id: &str,
        lease_duration_ms: u64,
        limits: CheckpointRestoreLimits,
    ) -> Result<LoadedCheckpointManifest> {
        self.validate_publication_entries_with_effects(entries, effects)?;
        let loaded = if let Some(loaded) = self.load_checkpoint_unleased().await? {
            self.prepare_local_checkpoint_append(loaded.manifest(), entries, effects, limits)?;
            loaded
        } else {
            let genesis = CheckpointManifest::new(self.checkpoint_identity()?.clone());
            self.prepare_local_checkpoint_append(&genesis, entries, effects, limits)?;
            self.initialize_checkpoint_unleased(lease_id, lease_duration_ms)
                .await?
        };
        self.publish_committed_from_loaded_unleased_with_effects_and_limits(
            entries,
            effects,
            lease_id,
            lease_duration_ms,
            loaded,
            limits,
        )
        .await
    }

    async fn publish_committed_from_loaded_unleased(
        &self,
        entries: &[LogEntry],
        lease_id: &str,
        lease_duration_ms: u64,
        loaded: LoadedCheckpointManifest,
    ) -> Result<LoadedCheckpointManifest> {
        self.publish_committed_from_loaded_unleased_with_effects_and_limits(
            entries,
            &[],
            lease_id,
            lease_duration_ms,
            loaded,
            CHECKPOINT_RESTORE_LIMITS,
        )
        .await
    }

    #[cfg(test)]
    async fn publish_committed_from_loaded_unleased_with_limits(
        &self,
        entries: &[LogEntry],
        lease_id: &str,
        lease_duration_ms: u64,
        loaded: LoadedCheckpointManifest,
        limits: CheckpointRestoreLimits,
    ) -> Result<LoadedCheckpointManifest> {
        self.publish_committed_from_loaded_unleased_with_effects_and_limits(
            entries,
            &[],
            lease_id,
            lease_duration_ms,
            loaded,
            limits,
        )
        .await
    }

    async fn publish_committed_from_loaded_unleased_with_effects_and_limits(
        &self,
        entries: &[LogEntry],
        effects: &[CheckpointEffectRecord],
        lease_id: &str,
        lease_duration_ms: u64,
        mut loaded: LoadedCheckpointManifest,
        limits: CheckpointRestoreLimits,
    ) -> Result<LoadedCheckpointManifest> {
        for _ in 0..MAX_CHECKPOINT_CAS_ATTEMPTS {
            self.ensure_generation_not_retired().await?;
            let prepared =
                self.prepare_local_checkpoint_append(loaded.manifest(), entries, effects, limits)?;
            self.renew_active_publisher_gc_lease(lease_id, lease_duration_ms)
                .await?;
            let Some(prepared) = self
                .finalize_checkpoint_append_under_lease(
                    loaded.manifest(),
                    entries,
                    effects,
                    prepared,
                    limits,
                )
                .await?
            else {
                return Ok(loaded);
            };
            let record = prepared
                .next
                .segments
                .last()
                .expect("new checkpoint segment");
            match self
                .store
                .create(record.object_key(), &prepared.bytes)
                .await
            {
                Ok(_) => {}
                Err(ObjStoreError::AlreadyExists { .. }) => {
                    self.download_verified_with_limits(
                        record.object_key(),
                        record.size_bytes(),
                        record.sha256(),
                        limits,
                    )
                    .await?;
                }
                Err(error) => return Err(error.into()),
            }
            self.renew_active_publisher_gc_lease(lease_id, lease_duration_ms)
                .await?;
            match self
                .store
                .update(
                    &self.checkpoint_manifest_key()?,
                    prepared.next_bytes,
                    loaded.version.clone(),
                )
                .await
            {
                Ok(version) => {
                    return Ok(LoadedCheckpointManifest {
                        manifest: prepared.next,
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

    fn prepare_checkpoint_snapshot_candidate(
        &self,
        anchor: &RecoveryAnchor,
        snapshot_bytes: &[u8],
        manifest: &CheckpointManifest,
        policy: CheckpointSnapshotPublicationPolicy,
    ) -> Result<PreparedCheckpointSnapshot> {
        let CheckpointSnapshotPublicationPolicy {
            allow_empty_baseline,
            limits,
        } = policy;
        self.validate_recovery_anchor(anchor)?;
        self.checkpoint_declared_decoded_budget(manifest, limits)?;
        let digest = LogHash::digest(&[snapshot_bytes]);
        if anchor.snapshot().digest() != digest {
            return Err(Error::ChecksumMismatch {
                object_key: anchor.snapshot().snapshot_id().to_string(),
                expected: anchor.snapshot().digest().to_hex(),
                actual: digest.to_hex(),
            });
        }
        let snapshot_size =
            u64::try_from(snapshot_bytes.len()).map_err(|_| Error::RestoreSizeOverflow {
                resource: "object encoded bytes",
            })?;
        if anchor.snapshot().size_bytes() != snapshot_size {
            return Err(Error::SizeMismatch {
                object_key: anchor.snapshot().snapshot_id().to_string(),
                expected: anchor.snapshot().size_bytes(),
                actual: snapshot_size,
            });
        }
        let snapshot = CheckpointSnapshotBase {
            object_key: checkpoint_snapshot_key(self.checkpoint_identity()?, anchor),
            executor_fingerprint: anchor.executor_fingerprint(),
            anchor: anchor.clone(),
            digest,
            size_bytes: snapshot_size,
        };
        self.validate_checkpoint_snapshot_base(&snapshot)?;
        ensure_restore_limit(
            "object encoded bytes",
            Some(&snapshot.object_key),
            snapshot.size_bytes,
            limits.object_encoded_bytes,
        )?;
        ensure_restore_limit(
            "object decoded bytes",
            Some(&snapshot.object_key),
            snapshot.size_bytes,
            limits.object_decoded_bytes,
        )?;

        let proposed = CheckpointBase::Snapshot(Box::new(snapshot));
        let current_tip = manifest.base.tip();
        let proposed_tip = proposed.tip();
        if current_tip.index == proposed_tip.index {
            if manifest.base == proposed {
                return Ok(PreparedCheckpointSnapshot {
                    proposed,
                    next: None,
                    next_bytes: None,
                });
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

        let mut next = manifest.clone();
        let empty_baseline = allow_empty_baseline
            && matches!(&manifest.base, CheckpointBase::Genesis)
            && manifest.segments.is_empty()
            && manifest.tip == CheckpointTip::new(0, LogHash::ZERO);
        if empty_baseline {
            next.base = proposed.clone();
            next.tip = proposed_tip;
        } else {
            let boundary = manifest
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
            next.base = proposed.clone();
            next.segments
                .retain(|record| record.start_index > proposed_tip.index);
        }
        self.validate_checkpoint_manifest_with_limits(&next, limits)?;
        self.checkpoint_declared_decoded_budget(&next, limits)?;
        let next_bytes = serialize_checkpoint_manifest(&next)?;
        Ok(PreparedCheckpointSnapshot {
            proposed,
            next: Some(next),
            next_bytes: Some(next_bytes),
        })
    }

    pub async fn publish_checkpoint_snapshot(
        &self,
        anchor: RecoveryAnchor,
        snapshot_bytes: &[u8],
    ) -> Result<LoadedCheckpointManifest> {
        self.publish_checkpoint_snapshot_with_limits(
            anchor,
            snapshot_bytes,
            CHECKPOINT_RESTORE_LIMITS,
        )
        .await
    }

    async fn publish_checkpoint_snapshot_with_limits(
        &self,
        anchor: RecoveryAnchor,
        snapshot_bytes: &[u8],
        limits: CheckpointRestoreLimits,
    ) -> Result<LoadedCheckpointManifest> {
        let policy = CheckpointSnapshotPublicationPolicy {
            allow_empty_baseline: false,
            limits,
        };
        self.ensure_generation_not_retired().await?;
        let observed = self.load_checkpoint_unleased().await?;
        let genesis;
        let manifest = if let Some(loaded) = &observed {
            loaded.manifest()
        } else {
            genesis = CheckpointManifest::new(self.checkpoint_identity()?.clone());
            &genesis
        };
        self.prepare_checkpoint_snapshot_candidate(&anchor, snapshot_bytes, manifest, policy)?;
        let lease = self
            .acquire_operation_lease(GcLeaseKind::Publisher, now_ms(), DEFAULT_LEASE_MS)
            .await?;
        let result = self
            .publish_checkpoint_snapshot_unleased_with_policy(
                anchor,
                snapshot_bytes,
                &lease.lease_id,
                DEFAULT_LEASE_MS,
                policy,
            )
            .await;
        let release = self.release_gc_lease(&lease.lease_id).await;
        match (result, release) {
            (Ok(loaded), Ok(())) => Ok(loaded),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }

    async fn publish_checkpoint_snapshot_unleased_with_policy(
        &self,
        anchor: RecoveryAnchor,
        snapshot_bytes: &[u8],
        lease_id: &str,
        lease_duration_ms: u64,
        policy: CheckpointSnapshotPublicationPolicy,
    ) -> Result<LoadedCheckpointManifest> {
        let loaded = if let Some(loaded) = self.load_checkpoint_unleased().await? {
            self.prepare_checkpoint_snapshot_candidate(
                &anchor,
                snapshot_bytes,
                loaded.manifest(),
                policy,
            )?;
            loaded
        } else {
            let genesis = CheckpointManifest::new(self.checkpoint_identity()?.clone());
            self.prepare_checkpoint_snapshot_candidate(&anchor, snapshot_bytes, &genesis, policy)?;
            self.initialize_checkpoint_unleased(lease_id, lease_duration_ms)
                .await?
        };
        self.publish_checkpoint_snapshot_from_loaded_unleased_with_limits(
            anchor,
            snapshot_bytes,
            lease_id,
            lease_duration_ms,
            loaded,
            policy,
        )
        .await
    }

    async fn publish_checkpoint_snapshot_from_loaded_unleased(
        &self,
        anchor: RecoveryAnchor,
        snapshot_bytes: &[u8],
        lease_id: &str,
        lease_duration_ms: u64,
        loaded: LoadedCheckpointManifest,
        allow_empty_baseline: bool,
    ) -> Result<LoadedCheckpointManifest> {
        self.publish_checkpoint_snapshot_from_loaded_unleased_with_limits(
            anchor,
            snapshot_bytes,
            lease_id,
            lease_duration_ms,
            loaded,
            CheckpointSnapshotPublicationPolicy {
                allow_empty_baseline,
                limits: CHECKPOINT_RESTORE_LIMITS,
            },
        )
        .await
    }

    async fn publish_checkpoint_snapshot_from_loaded_unleased_with_limits(
        &self,
        anchor: RecoveryAnchor,
        snapshot_bytes: &[u8],
        lease_id: &str,
        lease_duration_ms: u64,
        mut loaded: LoadedCheckpointManifest,
        policy: CheckpointSnapshotPublicationPolicy,
    ) -> Result<LoadedCheckpointManifest> {
        let limits = policy.limits;
        for _ in 0..MAX_CHECKPOINT_CAS_ATTEMPTS {
            let prepared = self.prepare_checkpoint_snapshot_candidate(
                &anchor,
                snapshot_bytes,
                loaded.manifest(),
                policy,
            )?;
            self.ensure_generation_not_retired().await?;
            let snapshot = prepared
                .proposed
                .snapshot()
                .expect("proposed snapshot base");
            self.renew_gc_lease(
                GcLeaseKind::Publisher,
                lease_id,
                now_ms(),
                lease_duration_ms,
            )
            .await?;
            match self
                .store
                .create(snapshot.object_key(), snapshot_bytes)
                .await
            {
                Ok(_) | Err(ObjStoreError::AlreadyExists { .. }) => {}
                Err(error) => return Err(error.into()),
            }
            self.download_verified_with_limits(
                snapshot.object_key(),
                snapshot.size_bytes(),
                &snapshot.digest().to_hex(),
                limits,
            )
            .await?;
            let Some(next) = prepared.next else {
                return Ok(loaded);
            };
            let next_bytes = prepared
                .next_bytes
                .expect("updated snapshot manifest serialization");
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

    pub async fn roll_recovery_generation(
        &self,
        target: &ObjectArchiveStore,
    ) -> Result<LoadedCheckpointManifest> {
        let source_identity = self.checkpoint_identity()?;
        let target_identity = target.checkpoint_identity()?;
        if source_identity.cluster_id != target_identity.cluster_id
            || source_identity.epoch != target_identity.epoch
            || source_identity.config_id != target_identity.config_id
            || source_identity.config_digest != target_identity.config_digest
            || source_identity.recovery_generation.checked_add(1)
                != Some(target_identity.recovery_generation)
        {
            return Err(Error::InvalidCheckpoint(
                "recovery-generation roll requires the same cluster/epoch/config identity and generation + 1"
                    .into(),
            ));
        }
        self.copy_checkpoint_to(target).await
    }

    async fn copy_checkpoint_to(
        &self,
        target: &ObjectArchiveStore,
    ) -> Result<LoadedCheckpointManifest> {
        self.ensure_generation_not_retired().await?;
        target.ensure_generation_not_retired().await?;
        self.load_checkpoint().await?;
        target.load_checkpoint().await?;
        let source_hard_deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_millis(DEFAULT_LEASE_MS))
            .ok_or_else(|| Error::InvalidGc("reader lease deadline overflow".into()))?;
        let source_lease = self
            .acquire_operation_lease(GcLeaseKind::Reader, now_ms(), DEFAULT_LEASE_MS)
            .await?;
        if tokio::time::Instant::now() >= source_hard_deadline {
            let _ = self.release_gc_lease(&source_lease.lease_id).await;
            return Err(Error::GcLeaseMissing {
                lease_id: source_lease.lease_id,
            });
        }
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
            .with_reader_lease_renewal(
                &source_lease.lease_id,
                DEFAULT_LEASE_MS,
                source_hard_deadline,
                self.copy_checkpoint_to_unleased(target, &target_lease.lease_id),
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
                "checkpoint copy requires the canonical checkpoint format".into(),
            ));
        }
        target
            .renew_active_publisher_gc_lease(target_lease_id, DEFAULT_LEASE_MS)
            .await?;
        self.restore_loaded_checkpoint_with_active_reader_lease(&source)
            .await?;
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
                    .renew_active_publisher_gc_lease(target_lease_id, DEFAULT_LEASE_MS)
                    .await?;
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
            target
                .renew_active_publisher_gc_lease(target_lease_id, DEFAULT_LEASE_MS)
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
            base,
            segments,
            tip: source.manifest.tip,
        };
        target.validate_checkpoint_manifest(&manifest)?;
        target
            .renew_active_publisher_gc_lease(target_lease_id, DEFAULT_LEASE_MS)
            .await?;
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
            .create(
                &target_manifest_key,
                serialize_checkpoint_manifest(&manifest)?,
            )
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
        #[cfg(test)]
        test_checkpoint_object_created_gate(self.test_store_identity, key).await;
        Ok(())
    }

    #[cfg(test)]
    async fn restore_loaded_checkpoint_unleased(
        &self,
        loaded: &LoadedCheckpointManifest,
        lease_id: &str,
    ) -> Result<RestoredCheckpoint> {
        self.restore_loaded_checkpoint_with_reader_lease_duration_unleased(
            loaded,
            lease_id,
            DEFAULT_LEASE_MS,
        )
        .await
    }

    #[cfg(test)]
    async fn restore_loaded_checkpoint_with_reader_lease_duration_unleased(
        &self,
        loaded: &LoadedCheckpointManifest,
        lease_id: &str,
        lease_duration_ms: u64,
    ) -> Result<RestoredCheckpoint> {
        self.restore_loaded_checkpoint_with_reader_lease_duration_and_limits_unleased(
            loaded,
            lease_id,
            lease_duration_ms,
            CHECKPOINT_RESTORE_LIMITS,
        )
        .await
    }

    #[cfg(test)]
    async fn restore_loaded_checkpoint_with_reader_lease_duration_and_limits_unleased(
        &self,
        loaded: &LoadedCheckpointManifest,
        lease_id: &str,
        lease_duration_ms: u64,
        limits: CheckpointRestoreLimits,
    ) -> Result<RestoredCheckpoint> {
        let stable_budget = self.checkpoint_declared_decoded_budget(&loaded.manifest, limits)?;
        let suffix_shape = checkpoint_suffix_shape(&loaded.manifest)?;
        let (mut restored, restored_capacity, restored_outer_bytes) =
            allocate_restored_suffix(suffix_shape)?;
        let mut expected_tip = loaded.manifest.base.tip();
        let mut budget = CheckpointRestoreBudget::new(&loaded.manifest, limits)?;
        budget.charge_aggregate(restored_outer_bytes)?;
        ensure_restore_limit(
            "stable aggregate decoded bytes",
            None,
            budget.decoded_bytes(),
            stable_budget.decoded_bytes(),
        )?;
        let snapshot = match &loaded.manifest.base {
            CheckpointBase::Genesis => None,
            CheckpointBase::Snapshot(snapshot) => {
                self.renew_gc_lease(GcLeaseKind::Reader, lease_id, now_ms(), lease_duration_ms)
                    .await?;
                let bytes = self
                    .download_verified_with_limits(
                        &snapshot.object_key,
                        snapshot.size_bytes,
                        &snapshot.digest.to_hex(),
                        limits,
                    )
                    .await?;
                Some(RestoredCheckpointSnapshot {
                    anchor: snapshot.anchor.clone(),
                    bytes,
                })
            }
        };
        for record in &loaded.manifest.segments {
            self.renew_gc_lease(GcLeaseKind::Reader, lease_id, now_ms(), lease_duration_ms)
                .await?;
            let bytes = self
                .download_verified_with_limits(
                    record.object_key(),
                    record.size_bytes(),
                    record.sha256(),
                    limits,
                )
                .await?;
            let max_decoded_bytes = budget.next_object_limit()?;
            let limit_resource = if max_decoded_bytes < limits.object_decoded_bytes {
                "aggregate decoded bytes"
            } else {
                "object decoded bytes"
            };
            let (entries, decoded_bytes) = self.decode_checkpoint_segment_bounded(
                record,
                &bytes,
                max_decoded_bytes,
                limit_resource,
            )?;
            budget.charge(record.object_key(), decoded_bytes)?;
            ensure_restore_limit(
                "stable aggregate decoded bytes",
                None,
                budget.decoded_bytes(),
                stable_budget.decoded_bytes(),
            )?;
            self.validate_decoded_entries(&entries, &expected_tip)?;
            let last = entries
                .last()
                .ok_or_else(|| Error::InvalidCheckpoint("empty qlog segment".into()))?;
            expected_tip = CheckpointTip::new(last.index, last.hash);
            restored.extend(entries);
            validate_restored_suffix_allocation(
                restored.capacity(),
                restored.len(),
                restored_capacity,
                suffix_shape,
            )?;
        }
        if expected_tip != loaded.manifest.tip || restored.len() != suffix_shape.entry_count {
            return Err(Error::InvalidCheckpoint(
                "restored entries do not match manifest tip".into(),
            ));
        }
        validate_restored_suffix_allocation(
            restored.capacity(),
            restored.len(),
            restored_capacity,
            suffix_shape,
        )?;
        ensure_restore_limit(
            "stable aggregate decoded bytes",
            None,
            budget.decoded_bytes(),
            stable_budget.decoded_bytes(),
        )?;
        Ok(RestoredCheckpoint {
            snapshot,
            suffix: restored,
            tip: expected_tip,
        })
    }

    /// Restores a manifest while its caller owns one continuously renewed
    /// reader lease. This deliberately contains no lease or timer operation:
    /// [`with_reader_lease_renewal`] owns the entire remote lifetime, from the
    /// generation check through every object download.
    async fn restore_loaded_checkpoint_with_active_reader_lease(
        &self,
        loaded: &LoadedCheckpointManifest,
    ) -> Result<RestoredCheckpoint> {
        self.restore_loaded_checkpoint_with_active_reader_lease_and_limits(
            loaded,
            CHECKPOINT_RESTORE_LIMITS,
        )
        .await
    }

    async fn restore_loaded_checkpoint_with_active_reader_lease_and_limits(
        &self,
        loaded: &LoadedCheckpointManifest,
        limits: CheckpointRestoreLimits,
    ) -> Result<RestoredCheckpoint> {
        let stable_budget = self.checkpoint_declared_decoded_budget(&loaded.manifest, limits)?;
        let suffix_shape = checkpoint_suffix_shape(&loaded.manifest)?;
        let (mut restored, restored_capacity, restored_outer_bytes) =
            allocate_restored_suffix(suffix_shape)?;
        let mut expected_tip = loaded.manifest.base.tip();
        let mut budget = CheckpointRestoreBudget::new(&loaded.manifest, limits)?;
        budget.charge_aggregate(restored_outer_bytes)?;
        ensure_restore_limit(
            "stable aggregate decoded bytes",
            None,
            budget.decoded_bytes(),
            stable_budget.decoded_bytes(),
        )?;
        let snapshot = match &loaded.manifest.base {
            CheckpointBase::Genesis => None,
            CheckpointBase::Snapshot(snapshot) => {
                let bytes = self
                    .download_verified_with_limits(
                        &snapshot.object_key,
                        snapshot.size_bytes,
                        &snapshot.digest.to_hex(),
                        limits,
                    )
                    .await?;
                Some(RestoredCheckpointSnapshot {
                    anchor: snapshot.anchor.clone(),
                    bytes,
                })
            }
        };
        for record in &loaded.manifest.segments {
            let bytes = self
                .download_verified_with_limits(
                    record.object_key(),
                    record.size_bytes(),
                    record.sha256(),
                    limits,
                )
                .await?;
            let max_decoded_bytes = budget.next_object_limit()?;
            let limit_resource = if max_decoded_bytes < limits.object_decoded_bytes {
                "aggregate decoded bytes"
            } else {
                "object decoded bytes"
            };
            let (entries, decoded_bytes) = self.decode_checkpoint_segment_bounded(
                record,
                &bytes,
                max_decoded_bytes,
                limit_resource,
            )?;
            budget.charge(record.object_key(), decoded_bytes)?;
            ensure_restore_limit(
                "stable aggregate decoded bytes",
                None,
                budget.decoded_bytes(),
                stable_budget.decoded_bytes(),
            )?;
            self.validate_decoded_entries(&entries, &expected_tip)?;
            let last = entries
                .last()
                .ok_or_else(|| Error::InvalidCheckpoint("empty qlog segment".into()))?;
            expected_tip = CheckpointTip::new(last.index, last.hash);
            restored.extend(entries);
            validate_restored_suffix_allocation(
                restored.capacity(),
                restored.len(),
                restored_capacity,
                suffix_shape,
            )?;
        }
        if expected_tip != loaded.manifest.tip || restored.len() != suffix_shape.entry_count {
            return Err(Error::InvalidCheckpoint(
                "restored entries do not match manifest tip".into(),
            ));
        }
        validate_restored_suffix_allocation(
            restored.capacity(),
            restored.len(),
            restored_capacity,
            suffix_shape,
        )?;
        ensure_restore_limit(
            "stable aggregate decoded bytes",
            None,
            budget.decoded_bytes(),
            stable_budget.decoded_bytes(),
        )?;
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
        self.publish_segment_with_limits(epoch, segment, CHECKPOINT_RESTORE_LIMITS)
            .await
    }

    async fn publish_segment_with_limits(
        &self,
        epoch: u64,
        segment: &SegmentFile,
        limits: CheckpointRestoreLimits,
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
            size_bytes: u64::try_from(segment.bytes().len()).map_err(|_| {
                Error::RestoreSizeOverflow {
                    resource: "object encoded bytes",
                }
            })?,
        };
        ensure_restore_limit(
            "object encoded bytes",
            Some(record.object_key()),
            record.size_bytes(),
            limits.object_encoded_bytes,
        )?;
        self.store
            .create(record.object_key(), segment.bytes())
            .await?;
        Ok(record)
    }

    pub async fn publish_snapshot(&self, snapshot: &Snapshot) -> Result<SnapshotRecord> {
        self.publish_snapshot_with_limits(snapshot, CHECKPOINT_RESTORE_LIMITS)
            .await
    }

    async fn publish_snapshot_with_limits(
        &self,
        snapshot: &Snapshot,
        limits: CheckpointRestoreLimits,
    ) -> Result<SnapshotRecord> {
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
            size_bytes: u64::try_from(snapshot.db_bytes().len()).map_err(|_| {
                Error::RestoreSizeOverflow {
                    resource: "object encoded bytes",
                }
            })?,
        };
        ensure_restore_limit(
            "object encoded bytes",
            Some(record.object_key()),
            record.size_bytes(),
            limits.object_encoded_bytes,
        )?;
        ensure_restore_limit(
            "object decoded bytes",
            Some(record.object_key()),
            record.size_bytes(),
            limits.object_decoded_bytes,
        )?;
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
        let root_object = self
            .get_checkpoint_manifest_versioned_bounded(&root_key)
            .await?;
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
                .filter(|metadata| {
                    is_known_retired_checkpoint_object(&generation.identity, metadata.key())
                })
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
        let existing_report = self.load_gc_report(plan_hash).await?;
        let plan_bytes = self.store.get(&self.gc_plan_key(plan_hash)).await?;
        let plan: GcPlan = deserialize_json(&plan_bytes)?;
        let actual_hash = hash_gc_plan(&plan)?;
        if plan.plan_hash != plan_hash || actual_hash != plan_hash {
            return Err(Error::GcPlanHashMismatch {
                expected: plan_hash.to_string(),
                actual: actual_hash,
            });
        }
        if let Some(report) = existing_report {
            self.validate_gc_report(&plan, &report)?;
            self.clear_gc_barrier(&plan).await?;
            return Ok(report);
        }
        if now_ms < plan.not_before_ms {
            return Err(Error::GcBarrierBusy {
                until_ms: plan.not_before_ms,
            });
        }
        if let Err(error) = self.acquire_gc_barrier(&plan, now_ms).await {
            return self.gc_report_after_stale(&plan, error).await;
        }
        if let Err(error) = self.enter_delete_phase(&plan, now_ms).await {
            return self.gc_report_after_stale(&plan, error).await;
        }
        if let Err(error) = self.fence_gc_root(&plan).await {
            return self.gc_report_after_stale(&plan, error).await;
        }
        if let Err(error) = self.retire_plan_generations(&plan, now_ms).await {
            return self.gc_report_after_stale(&plan, error).await;
        }

        let mut results = Vec::with_capacity(plan.candidates.len());
        for candidate in &plan.candidates {
            if let Err(error) = self.validate_gc_fence(&plan, now_ms).await {
                return self.gc_report_after_stale(&plan, error).await;
            }
            let known = match candidate.reason {
                GcCandidateReason::SupersededRecoveryGeneration => {
                    is_known_retired_checkpoint_object(&candidate.generation, &candidate.key)
                }
                GcCandidateReason::UnreferencedCheckpointObject => {
                    is_known_checkpoint_object(&candidate.generation, &candidate.key)
                }
            };
            if !known {
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
        self.validate_gc_report(&plan, &report)?;
        self.clear_gc_barrier(&plan).await?;
        Ok(report)
    }

    async fn gc_report_after_stale(
        &self,
        plan: &GcPlan,
        error: Error,
    ) -> Result<GcExecutionReport> {
        if matches!(error, Error::GcPlanStale { .. }) {
            if let Some(report) = self.load_gc_report(plan.plan_hash()).await? {
                self.validate_gc_report(plan, &report)?;
                return Ok(report);
            }
        }
        Err(error)
    }

    fn validate_gc_report(&self, plan: &GcPlan, report: &GcExecutionReport) -> Result<()> {
        if report.format_version != GC_FORMAT_VERSION {
            return Err(Error::InvalidGc(
                "execution report format version mismatch".into(),
            ));
        }
        if report.plan_hash != plan.plan_hash {
            return Err(Error::GcPlanHashMismatch {
                expected: plan.plan_hash.clone(),
                actual: report.plan_hash.clone(),
            });
        }
        if report.fence != execution_fence(plan) {
            return Err(Error::InvalidGc(
                "execution report fence does not match the plan".into(),
            ));
        }
        if report.results.len() != plan.candidates.len() {
            return Err(Error::InvalidGc(
                "execution report candidates do not match the plan".into(),
            ));
        }
        for (candidate, evidence) in plan.candidates.iter().zip(&report.results) {
            if evidence.format_version != GC_FORMAT_VERSION
                || evidence.plan_hash != plan.plan_hash
                || evidence.key != candidate.key
                || evidence.version != candidate.version
            {
                return Err(Error::InvalidGc(
                    "execution report candidates do not match the plan".into(),
                ));
            }
        }
        Ok(())
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
            if kind == GcLeaseKind::Publisher
                && loaded.control.active_gc.is_none()
                && loaded.control.leases.iter().any(|lease| {
                    lease.lease_id == lease_id
                        && lease.kind == kind
                        && lease.fence == loaded.control.fence
                        && publisher_lease_has_renewal_margin(lease, now_ms, duration_ms)
                })
            {
                return Ok(());
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
                // The established publisher/copy contract is deliberately
                // forgiving: their next operation renews a missing or expired
                // lease. Reader restore uses the stricter helper below.
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

    /// Renews the exact Publisher lease that already protects a publication
    /// mutation or archived-overlap proof. Unlike the general publisher/copy
    /// renewal contract, this fence must never recreate a missing or expired
    /// lease after GC could have started deleting a referenced object.
    async fn renew_active_publisher_gc_lease(
        &self,
        lease_id: &str,
        duration_ms: u64,
    ) -> Result<()> {
        let identity = self.checkpoint_identity()?.clone();
        let mut prior_hard_deadline = None;
        let mut prior_expires_at_ms = None;
        for _ in 0..MAX_GC_CONTROL_CAS_ATTEMPTS {
            let loaded = self.load_gc_control().await;
            let loaded_at = tokio::time::Instant::now();
            let loaded_at_ms = now_ms();
            let mut loaded = loaded?;
            self.expire_gc_state(&mut loaded.control, loaded_at_ms);
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
                .find(|entry| entry.identity == identity)
            {
                return Err(Error::GenerationRetired {
                    generation: identity.recovery_generation(),
                    plan_hash: plan_hash.clone(),
                });
            }
            if let Some(active) = &loaded.control.active_gc {
                if active.phase == GcBarrierPhase::Deleting {
                    return Err(Error::GcBarrierActive {
                        operation_id: active.operation_id.clone(),
                    });
                }
            }
            if prior_hard_deadline.is_some_and(|deadline| loaded_at >= deadline)
                || prior_expires_at_ms.is_some_and(|expires_at_ms| loaded_at_ms >= expires_at_ms)
            {
                return Err(Error::GcLeaseMissing {
                    lease_id: lease_id.to_string(),
                });
            }
            let Some(lease) = loaded
                .control
                .leases
                .iter()
                .find(|lease| lease.lease_id == lease_id)
            else {
                return Err(Error::GcLeaseMissing {
                    lease_id: lease_id.to_string(),
                });
            };
            if lease.kind != GcLeaseKind::Publisher {
                return Err(Error::InvalidGc(
                    "publisher proof renewal lease kind changed".into(),
                ));
            }
            if loaded.control.active_gc.is_none()
                && lease.fence == loaded.control.fence
                && publisher_lease_has_renewal_margin(lease, loaded_at_ms, duration_ms)
            {
                return Ok(());
            }
            if prior_hard_deadline.is_none() {
                let remaining_ms =
                    lease
                        .expires_at_ms
                        .checked_sub(loaded_at_ms)
                        .ok_or_else(|| Error::GcLeaseMissing {
                            lease_id: lease_id.to_string(),
                        })?;
                prior_hard_deadline = Some(
                    loaded_at
                        .checked_add(Duration::from_millis(remaining_ms))
                        .ok_or_else(|| {
                            Error::InvalidGc("publisher lease deadline overflow".into())
                        })?,
                );
                prior_expires_at_ms = Some(lease.expires_at_ms);
            }
            let lease = loaded
                .control
                .leases
                .iter_mut()
                .find(|lease| lease.lease_id == lease_id)
                .expect("validated Publisher lease remains present");
            let expires_at_ms = loaded_at_ms.saturating_add(duration_ms);
            let next_hard_deadline = loaded_at
                .checked_add(Duration::from_millis(duration_ms))
                .ok_or_else(|| Error::InvalidGc("publisher lease deadline overflow".into()))?;
            lease.fence = loaded.control.fence;
            lease.expires_at_ms = expires_at_ms;
            if let Some(active) = loaded.control.active_gc.as_mut() {
                active.expires_at_ms = active
                    .expires_at_ms
                    .max(expires_at_ms.saturating_add(DEFAULT_LEASE_MS));
            }
            let update = self.update_gc_control(&loaded).await;
            let updated_at = tokio::time::Instant::now();
            let updated_at_ms = now_ms();
            let prior_elapsed = prior_hard_deadline.is_some_and(|deadline| updated_at >= deadline)
                || prior_expires_at_ms.is_some_and(|expires_at_ms| updated_at_ms >= expires_at_ms);
            let renewed_elapsed =
                updated_at >= next_hard_deadline || updated_at_ms >= expires_at_ms;
            match update {
                Ok(_) if prior_elapsed || renewed_elapsed => {
                    let _ = self.release_gc_lease(lease_id).await;
                    return Err(Error::GcLeaseMissing {
                        lease_id: lease_id.to_string(),
                    });
                }
                Ok(_) => return Ok(()),
                Err(Error::ObjectStore(ObjStoreError::Precondition { .. })) if prior_elapsed => {
                    return Err(Error::GcLeaseMissing {
                        lease_id: lease_id.to_string(),
                    });
                }
                Err(Error::ObjectStore(ObjStoreError::Precondition { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::CompareAndSwapRetriesExhausted {
            attempts: MAX_GC_CONTROL_CAS_ATTEMPTS,
        })
    }

    /// Renews an already-active reader lease without changing the established
    /// writer/publisher/copy renewal semantics. Missing or expired readers are
    /// never recreated; a retired generation is rejected from the same
    /// control-plane snapshot before a remote restore can continue.
    async fn renew_active_reader_gc_lease(
        &self,
        lease_id: &str,
        prior_hard_deadline: tokio::time::Instant,
        duration_ms: u64,
    ) -> Result<tokio::time::Instant> {
        let identity = self.checkpoint_identity()?.clone();
        for _ in 0..MAX_GC_CONTROL_CAS_ATTEMPTS {
            let loaded = self.load_gc_control().await;
            let loaded_at = tokio::time::Instant::now();
            let mut loaded = loaded?;
            let loaded_at_ms = now_ms();
            self.expire_gc_state(&mut loaded.control, loaded_at_ms);
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
                .find(|entry| entry.identity == identity)
            {
                return Err(Error::GenerationRetired {
                    generation: identity.recovery_generation(),
                    plan_hash: plan_hash.clone(),
                });
            }
            if let Some(active) = &loaded.control.active_gc {
                if active.phase == GcBarrierPhase::Deleting {
                    return Err(Error::GcBarrierActive {
                        operation_id: active.operation_id.clone(),
                    });
                }
            }
            if loaded_at >= prior_hard_deadline {
                return Err(Error::GcLeaseMissing {
                    lease_id: lease_id.to_string(),
                });
            }
            let Some(lease) = loaded
                .control
                .leases
                .iter_mut()
                .find(|lease| lease.lease_id == lease_id)
            else {
                return Err(Error::GcLeaseMissing {
                    lease_id: lease_id.to_string(),
                });
            };
            if lease.kind != GcLeaseKind::Reader {
                return Err(Error::InvalidGc("reader renewal lease kind changed".into()));
            }
            let expires_at_ms = loaded_at_ms.saturating_add(duration_ms);
            lease.fence = loaded.control.fence;
            lease.expires_at_ms = expires_at_ms;
            if let Some(active) = loaded.control.active_gc.as_mut() {
                active.expires_at_ms = active
                    .expires_at_ms
                    .max(expires_at_ms.saturating_add(DEFAULT_LEASE_MS));
            }
            let next_hard_deadline = loaded_at
                .checked_add(Duration::from_millis(duration_ms))
                .ok_or_else(|| Error::InvalidGc("reader lease deadline overflow".into()))?;
            let update = self.update_gc_control(&loaded).await;
            let updated_at = tokio::time::Instant::now();
            match update {
                Ok(_) if updated_at >= prior_hard_deadline => {
                    let _ = self.release_gc_lease(lease_id).await;
                    return Err(Error::GcLeaseMissing {
                        lease_id: lease_id.to_string(),
                    });
                }
                Ok(_) => return Ok(next_hard_deadline),
                Err(Error::ObjectStore(ObjStoreError::Precondition { .. }))
                    if updated_at >= prior_hard_deadline =>
                {
                    return Err(Error::GcLeaseMissing {
                        lease_id: lease_id.to_string(),
                    });
                }
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
        let object = self
            .get_checkpoint_manifest_versioned_bounded(&plan.root_manifest_key)
            .await?;
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
                let current = self
                    .get_checkpoint_manifest_versioned_bounded(&plan.root_manifest_key)
                    .await?;
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

    async fn get_checkpoint_manifest_versioned_bounded(
        &self,
        key: &str,
    ) -> Result<VersionedObject> {
        self.store
            .get_with_version_bounded(key, CHECKPOINT_RESTORE_LIMITS.manifest_encoded_bytes)
            .await
            .map_err(|error| map_restore_object_error(error, "manifest encoded bytes"))
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
                || active.root != plan.root
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
        let bytes = serialize_gc_control(&control)?;
        for retry in 0..=MAX_GC_CONTROL_THROTTLE_RETRIES {
            #[cfg(test)]
            let injected = test_gc_control_gate(
                self.test_store_identity,
                TestGcControlOperation::MutationError,
            )
            .await;
            #[cfg(not(test))]
            let injected: Option<ObjStoreError> = None;
            let result = match injected {
                Some(error) => Err(error),
                None => {
                    self.store
                        .create_conditional_once(&self.gc_control_key(), &bytes)
                        .await
                }
            };
            match result {
                Ok(_) | Err(ObjStoreError::AlreadyExists { .. }) => return Ok(()),
                Err(error)
                    if retry < MAX_GC_CONTROL_THROTTLE_RETRIES && gc_control_throttled(&error) =>
                {
                    tokio::time::sleep(gc_control_throttle_backoff(&bytes, retry)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        unreachable!("bounded GC-control throttle loop always returns")
    }

    async fn load_gc_control(&self) -> Result<LoadedGcControl> {
        #[cfg(test)]
        let _ = test_gc_control_gate(self.test_store_identity, TestGcControlOperation::Load).await;
        let object = self
            .store
            .get_with_version_bounded(&self.gc_control_key(), GC_CONTROL_ENCODED_BYTES)
            .await?;
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
        let bytes = serialize_gc_control(&loaded.control)?;
        for retry in 0..=MAX_GC_CONTROL_THROTTLE_RETRIES {
            #[cfg(test)]
            let injected = test_gc_control_gate(
                self.test_store_identity,
                TestGcControlOperation::MutationError,
            )
            .await;
            #[cfg(not(test))]
            let injected: Option<ObjStoreError> = None;
            let result = match injected {
                Some(error) => Err(error),
                None => {
                    self.store
                        .update_conditional_once(
                            &self.gc_control_key(),
                            &bytes,
                            loaded.version.clone(),
                        )
                        .await
                }
            };
            match result {
                Err(error)
                    if retry < MAX_GC_CONTROL_THROTTLE_RETRIES && gc_control_throttled(&error) =>
                {
                    tokio::time::sleep(gc_control_throttle_backoff(&bytes, retry)).await;
                }
                result => {
                    #[cfg(test)]
                    let _ = test_gc_control_gate(
                        self.test_store_identity,
                        TestGcControlOperation::Update,
                    )
                    .await;
                    return result.map_err(Into::into);
                }
            }
        }
        unreachable!("bounded GC-control throttle loop always returns")
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

    fn prepare_local_checkpoint_append(
        &self,
        manifest: &CheckpointManifest,
        entries: &[LogEntry],
        effects: &[CheckpointEffectRecord],
        limits: CheckpointRestoreLimits,
    ) -> Result<PreparedCheckpointAppend> {
        self.validate_publication_entries_with_effects(entries, effects)?;
        if entries.is_empty() {
            self.checkpoint_declared_decoded_budget(manifest, limits)?;
            return Ok(PreparedCheckpointAppend {
                suffix_start: None,
                candidate: None,
            });
        }
        let suffix_start = local_publication_suffix_start(manifest, entries)?;
        let Some(suffix_start) = suffix_start else {
            self.checkpoint_declared_decoded_budget(manifest, limits)?;
            return Ok(PreparedCheckpointAppend {
                suffix_start: None,
                candidate: None,
            });
        };
        let suffix_effects = checkpoint_effect_refs_for_suffix(entries, effects, suffix_start)?;
        let mut budget = self.checkpoint_declared_decoded_budget(manifest, limits)?;
        let bytes = encode_segment(&entries[suffix_start..]);
        let record = self.checkpoint_segment_record_with_limits(
            &entries[suffix_start..],
            &bytes,
            &suffix_effects,
            limits,
        )?;
        let max_decoded_bytes = budget.next_object_limit()?;
        let decoded_upper_bound = self.checkpoint_segment_decoded_upper_bound(&record)?;
        budget.charge(record.object_key(), decoded_upper_bound)?;
        let candidate_entry_count = record
            .end_index()
            .checked_sub(record.start_index())
            .and_then(|distance| distance.checked_add(1))
            .ok_or(Error::RestoreSizeOverflow {
                resource: "checkpoint suffix entry count",
            })?;
        budget.charge_aggregate(
            candidate_entry_count
                .checked_mul(CHECKPOINT_RESTORED_ENTRY_OVERHEAD_BUDGET_BYTES)
                .ok_or(Error::RestoreSizeOverflow {
                    resource: "restored suffix container bytes",
                })?,
        )?;
        let limit_resource = if max_decoded_bytes < limits.object_decoded_bytes {
            "aggregate decoded bytes"
        } else {
            "object decoded bytes"
        };
        let (decoded, decoded_bytes) = self.decode_checkpoint_segment_bounded(
            &record,
            &bytes,
            max_decoded_bytes,
            limit_resource,
        )?;
        if decoded != entries[suffix_start..] {
            return Err(Error::InvalidCheckpoint(
                "encoded checkpoint segment does not round-trip exactly".into(),
            ));
        }
        if decoded_bytes > decoded_upper_bound {
            return Err(Error::InvalidCheckpoint(format!(
                "decoded checkpoint segment {} exceeds its stable publication budget",
                record.object_key()
            )));
        }

        Ok(PreparedCheckpointAppend {
            suffix_start: Some(suffix_start),
            candidate: Some(PreparedCheckpointAppendCandidate { bytes, record }),
        })
    }

    async fn finalize_checkpoint_append_under_lease(
        &self,
        manifest: &CheckpointManifest,
        entries: &[LogEntry],
        effects: &[CheckpointEffectRecord],
        prepared: PreparedCheckpointAppend,
        limits: CheckpointRestoreLimits,
    ) -> Result<Option<FinalizedCheckpointAppend>> {
        let proven_suffix_start = if entries.is_empty() {
            None
        } else {
            self.publication_suffix_start(manifest, entries).await?
        };
        if proven_suffix_start != prepared.suffix_start {
            return Err(Error::InvalidCheckpoint(
                "local checkpoint suffix changed before archived proof".into(),
            ));
        }
        let Some(candidate) = prepared.candidate else {
            return Ok(None);
        };
        let suffix_start = prepared
            .suffix_start
            .expect("prepared append candidate has suffix");
        let expected_effects = checkpoint_effect_refs_for_suffix(entries, effects, suffix_start)?;
        if candidate.record.effects != expected_effects {
            return Err(Error::InvalidCheckpoint(
                "checkpoint QEFX effect references changed before manifest publication".into(),
            ));
        }
        self.validate_decoded_entries(&entries[suffix_start..], manifest.tip())?;
        self.verify_checkpoint_effect_refs(&candidate.record.effects)
            .await?;
        let mut next = manifest.clone();
        next.tip = CheckpointTip::new(candidate.record.end_index, candidate.record.last_hash);
        next.segments.push(candidate.record);
        self.validate_checkpoint_manifest_with_limits(&next, limits)?;
        self.checkpoint_declared_decoded_budget(&next, limits)?;
        let next_bytes = serialize_checkpoint_manifest(&next)?;
        Ok(Some(FinalizedCheckpointAppend {
            bytes: candidate.bytes,
            next,
            next_bytes,
        }))
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

    fn checkpoint_declared_decoded_budget(
        &self,
        manifest: &CheckpointManifest,
        limits: CheckpointRestoreLimits,
    ) -> Result<CheckpointRestoreBudget> {
        self.validate_checkpoint_manifest_with_limits(manifest, limits)?;
        let suffix_shape = checkpoint_suffix_shape(manifest)?;
        let mut budget = CheckpointRestoreBudget::new(manifest, limits)?;
        for record in &manifest.segments {
            budget.charge(
                record.object_key(),
                self.checkpoint_segment_decoded_upper_bound(record)?,
            )?;
        }
        budget.charge_aggregate(suffix_shape.stable_outer_bytes)?;
        Ok(budget)
    }

    fn checkpoint_segment_decoded_upper_bound(
        &self,
        record: &CheckpointSegmentRecord,
    ) -> Result<u64> {
        let entry_count = record
            .end_index()
            .checked_sub(record.start_index())
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| Error::InvalidCheckpoint("invalid segment range".into()))?;
        let cluster_id_bytes = u64::try_from(self.checkpoint_identity()?.cluster_id().len())
            .map_err(|_| Error::RestoreSizeOverflow {
                resource: "segment decoded upper bound",
            })?;
        let per_entry = cluster_id_bytes
            .checked_add(
                u64::try_from(QLOG_DECODED_ENTRY_OVERHEAD_BUDGET_BYTES).map_err(|_| {
                    Error::RestoreSizeOverflow {
                        resource: "segment decoded upper bound",
                    }
                })?,
            )
            .ok_or(Error::RestoreSizeOverflow {
                resource: "segment decoded upper bound",
            })?;
        entry_count
            .checked_mul(per_entry)
            .and_then(|bytes| bytes.checked_add(record.size_bytes()))
            .ok_or(Error::RestoreSizeOverflow {
                resource: "segment decoded upper bound",
            })
    }

    #[cfg(test)]
    fn checkpoint_segment_record(
        &self,
        entries: &[LogEntry],
        bytes: &[u8],
    ) -> Result<CheckpointSegmentRecord> {
        self.checkpoint_segment_record_with_limits(entries, bytes, &[], CHECKPOINT_RESTORE_LIMITS)
    }

    fn checkpoint_segment_record_with_limits(
        &self,
        entries: &[LogEntry],
        bytes: &[u8],
        effects: &[CheckpointEffectRecord],
        limits: CheckpointRestoreLimits,
    ) -> Result<CheckpointSegmentRecord> {
        let first = entries
            .first()
            .ok_or_else(|| Error::InvalidCheckpoint("refusing to publish empty segment".into()))?;
        let last = entries.last().expect("non-empty checkpoint segment");
        let object_key =
            checkpoint_segment_key(self.checkpoint_identity()?, first.index, last.index);
        let size_bytes = u64::try_from(bytes.len()).map_err(|_| Error::RestoreSizeOverflow {
            resource: "object encoded bytes",
        })?;
        ensure_restore_limit(
            "object encoded bytes",
            Some(&object_key),
            size_bytes,
            limits.object_encoded_bytes,
        )?;
        Ok(CheckpointSegmentRecord {
            format_version: CHECKPOINT_SEGMENT_FORMAT_VERSION,
            start_index: first.index,
            end_index: last.index,
            first_prev_hash: first.prev_hash,
            last_hash: last.hash,
            object_key,
            sha256: sha256_hex(bytes),
            size_bytes,
            effects: effects.to_vec(),
        })
    }

    fn validate_publication_entries_with_effects(
        &self,
        entries: &[LogEntry],
        effects: &[CheckpointEffectRecord],
    ) -> Result<()> {
        self.validate_publication_entries(entries)?;
        let mut entries_by_index = std::collections::BTreeMap::new();
        for entry in entries {
            if entries_by_index.insert(entry.index, entry).is_some() {
                return Err(Error::InvalidCheckpoint(
                    "checkpoint publication repeats an entry index".into(),
                ));
            }
        }
        let mut effects_by_index = std::collections::BTreeSet::new();
        for effect in effects {
            if !effects_by_index.insert(effect.entry_index) {
                return Err(Error::InvalidCheckpoint(
                    "checkpoint publication repeats a QEFX effect reference".into(),
                ));
            }
            let entry = entries_by_index.get(&effect.entry_index).ok_or_else(|| {
                Error::InvalidCheckpoint(
                    "checkpoint QEFX effect reference has no matching entry".into(),
                )
            })?;
            let command = ExternalEffectCommand::decode(&entry.payload).map_err(|error| {
                Error::InvalidCheckpoint(format!(
                    "checkpoint QEFX effect reference targets a non-QEFX entry: {error}"
                ))
            })?;
            if command.intended_slot() != entry.index {
                return Err(Error::InvalidCheckpoint(
                    "checkpoint QEFX effect reference has a wrong slot binding".into(),
                ));
            }
        }
        Ok(())
    }

    async fn verify_checkpoint_effect_refs(
        &self,
        effects: &[CheckpointEffectRecord],
    ) -> Result<()> {
        if effects.len() > MAX_EXTERNAL_EFFECT_CHUNKS {
            return Err(Error::InvalidCheckpoint(
                "checkpoint segment has too many QEFX refs".into(),
            ));
        }
        for effect in effects {
            self.restore_checkpoint_effect(effect).await?;
        }
        Ok(())
    }

    async fn load_checkpoint_segment(
        &self,
        record: &CheckpointSegmentRecord,
    ) -> Result<Vec<LogEntry>> {
        let bytes = self
            .download_verified(record.object_key(), record.size_bytes, &record.sha256)
            .await?;
        self.decode_checkpoint_segment(record, bytes)
    }

    fn decode_checkpoint_segment(
        &self,
        record: &CheckpointSegmentRecord,
        bytes: Vec<u8>,
    ) -> Result<Vec<LogEntry>> {
        self.decode_checkpoint_segment_bounded(
            record,
            &bytes,
            CHECKPOINT_RESTORE_LIMITS.object_decoded_bytes,
            "object decoded bytes",
        )
        .map(|(entries, _)| entries)
    }

    fn decode_checkpoint_segment_bounded(
        &self,
        record: &CheckpointSegmentRecord,
        bytes: &[u8],
        max_decoded_bytes: u64,
        limit_resource: &'static str,
    ) -> Result<(Vec<LogEntry>, u64)> {
        let identity = self.checkpoint_identity()?;
        let decoder_limit =
            usize::try_from(max_decoded_bytes).map_err(|_| Error::RestoreSizeOverflow {
                resource: limit_resource,
            })?;
        let (entries, decoded_bytes) =
            match decode_segment_for_cluster_bounded(bytes, identity.cluster_id(), decoder_limit) {
                Ok(decoded) => decoded,
                Err(rhiza_log::Error::DecodeLimitExceeded { limit, actual }) => {
                    return Err(Error::RestoreLimitExceeded {
                        resource: limit_resource,
                        object_key: Some(record.object_key.clone()),
                        limit: u64::try_from(limit).unwrap_or(u64::MAX),
                        actual: u64::try_from(actual).unwrap_or(u64::MAX),
                    });
                }
                Err(error) => return Err(Error::LogDecode(error.to_string())),
            };
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
        let decoded_bytes =
            u64::try_from(decoded_bytes).map_err(|_| Error::RestoreSizeOverflow {
                resource: limit_resource,
            })?;
        Ok((entries, decoded_bytes))
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
        self.validate_checkpoint_manifest_with_limits(manifest, CHECKPOINT_RESTORE_LIMITS)
    }

    fn validate_checkpoint_manifest_with_limits(
        &self,
        manifest: &CheckpointManifest,
        limits: CheckpointRestoreLimits,
    ) -> Result<()> {
        if manifest.format_version != CHECKPOINT_FORMAT_VERSION {
            return Err(Error::UnsupportedFormatVersion {
                object: "checkpoint manifest",
                version: manifest.format_version,
            });
        }
        validate_checkpoint_identity(self.checkpoint_identity()?, &manifest.identity)?;

        ensure_restore_count_limit(
            "segment count",
            manifest.segments.len(),
            limits.segment_count,
        )?;
        let object_count = manifest
            .segments
            .len()
            .checked_add(usize::from(manifest.base.snapshot().is_some()))
            .ok_or(Error::RestoreSizeOverflow {
                resource: "object count",
            })?;
        ensure_restore_count_limit("object count", object_count, limits.object_count)?;

        let mut aggregate_encoded_bytes = 0_u64;
        let mut declared_decoded_bytes = 0_u64;

        if let CheckpointBase::Snapshot(snapshot) = &manifest.base {
            self.validate_checkpoint_snapshot_base(snapshot)?;
            ensure_restore_limit(
                "object encoded bytes",
                Some(snapshot.object_key()),
                snapshot.size_bytes(),
                limits.object_encoded_bytes,
            )?;
            ensure_restore_limit(
                "object decoded bytes",
                Some(snapshot.object_key()),
                snapshot.size_bytes(),
                limits.object_decoded_bytes,
            )?;
            aggregate_encoded_bytes = checked_restore_add(
                "aggregate encoded bytes",
                aggregate_encoded_bytes,
                snapshot.size_bytes(),
            )?;
            declared_decoded_bytes = checked_restore_add(
                "aggregate decoded bytes",
                declared_decoded_bytes,
                snapshot.size_bytes(),
            )?;
        }

        let base_tip = manifest.base.tip();
        let mut expected_start = base_tip
            .index
            .checked_add(1)
            .ok_or_else(|| Error::InvalidCheckpoint("checkpoint base index overflow".into()))?;
        let mut expected_hash = base_tip.hash;
        let mut effect_indices = std::collections::BTreeSet::new();
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
            for effect in &record.effects {
                if effect.entry_index < record.start_index
                    || effect.entry_index > record.end_index
                    || !effect_indices.insert(effect.entry_index)
                {
                    return Err(Error::InvalidCheckpoint(
                        "checkpoint has duplicate or out-of-segment QEFX effect references".into(),
                    ));
                }
            }
            ensure_restore_limit(
                "object encoded bytes",
                Some(record.object_key()),
                record.size_bytes(),
                limits.object_encoded_bytes,
            )?;
            aggregate_encoded_bytes = checked_restore_add(
                "aggregate encoded bytes",
                aggregate_encoded_bytes,
                record.size_bytes(),
            )?;
            expected_start = record
                .end_index
                .checked_add(1)
                .ok_or_else(|| Error::InvalidCheckpoint("segment end index overflow".into()))?;
            expected_hash = record.last_hash;
        }

        ensure_restore_limit(
            "aggregate encoded bytes",
            None,
            aggregate_encoded_bytes,
            limits.aggregate_encoded_bytes,
        )?;
        ensure_restore_limit(
            "aggregate decoded bytes",
            None,
            declared_decoded_bytes,
            limits.aggregate_decoded_bytes,
        )?;

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
        if anchor.configuration_state().digest() != identity.config_digest() {
            return Err(checkpoint_identity_mismatch(
                "config_digest",
                identity.config_digest().to_hex(),
                anchor.configuration_state().digest().to_hex(),
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
        ensure_restore_limit(
            "object encoded bytes",
            Some(snapshot.object_key()),
            snapshot.size_bytes(),
            CHECKPOINT_RESTORE_LIMITS.object_encoded_bytes,
        )?;
        ensure_restore_limit(
            "object decoded bytes",
            Some(snapshot.object_key()),
            snapshot.size_bytes(),
            CHECKPOINT_RESTORE_LIMITS.object_decoded_bytes,
        )?;
        let expected_key = checkpoint_snapshot_key(self.checkpoint_identity()?, &snapshot.anchor);
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
        self.download_verified_with_limits(
            object_key,
            expected_size,
            expected_sha256,
            CHECKPOINT_RESTORE_LIMITS,
        )
        .await
    }

    async fn download_verified_with_limits(
        &self,
        object_key: &str,
        expected_size: u64,
        expected_sha256: &str,
        limits: CheckpointRestoreLimits,
    ) -> Result<Vec<u8>> {
        #[cfg(test)]
        test_checkpoint_download_gate(self.test_store_identity, object_key).await;
        ensure_restore_limit(
            "object encoded bytes",
            Some(object_key),
            expected_size,
            limits.object_encoded_bytes,
        )?;
        let bytes = self
            .store
            .get_bounded(object_key, limits.object_encoded_bytes)
            .await
            .map_err(|error| map_restore_object_error(error, "object encoded bytes"))?;
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

    /// Keeps one reader lease live while an arbitrary remote archive operation
    /// is pending.
    ///
    /// The operation future is deliberately owned by this `select!`: a failed
    /// renewal drops it before this method returns the renewal error. This
    /// prevents a stale, later-completing manifest or object read from being
    /// accepted after GC has crossed its deletion fence.
    async fn with_reader_lease_renewal<T>(
        &self,
        lease_id: &str,
        lease_duration_ms: u64,
        mut hard_deadline: tokio::time::Instant,
        operation: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let renewal_interval_ms = (lease_duration_ms / READER_LEASE_RENEW_DIVISOR).max(1);
        let renewal_lead =
            Duration::from_millis(lease_duration_ms.saturating_sub(renewal_interval_ms));

        tokio::pin!(operation);
        loop {
            if tokio::time::Instant::now() >= hard_deadline {
                return Err(Error::GcLeaseMissing {
                    lease_id: lease_id.to_string(),
                });
            }
            let renewal_at = hard_deadline
                .checked_sub(renewal_lead)
                .unwrap_or(hard_deadline);
            tokio::select! {
                // A completed operation is authoritative over a same-turn
                // timer tick, but its data is accepted only while the prior
                // hard deadline is still live.
                biased;
                result = &mut operation => {
                    let completed_at = tokio::time::Instant::now();
                    return match result {
                        Ok(_) if completed_at >= hard_deadline => Err(Error::GcLeaseMissing {
                            lease_id: lease_id.to_string(),
                        }),
                        result => result,
                    };
                },
                _ = tokio::time::sleep_until(renewal_at) => {
                    hard_deadline = self.renew_active_reader_gc_lease(
                        lease_id,
                        hard_deadline,
                        lease_duration_ms,
                    ).await?;
                }
            }
        }
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
    if expected.config_digest != actual.config_digest {
        return Err(checkpoint_identity_mismatch(
            "config_digest",
            expected.config_digest.to_hex(),
            actual.config_digest.to_hex(),
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

fn ensure_restore_count_limit(resource: &'static str, actual: usize, limit: usize) -> Result<()> {
    let actual = u64::try_from(actual).map_err(|_| Error::RestoreSizeOverflow { resource })?;
    let limit = u64::try_from(limit).map_err(|_| Error::RestoreSizeOverflow { resource })?;
    ensure_restore_limit(resource, None, actual, limit)
}

fn ensure_restore_limit(
    resource: &'static str,
    object_key: Option<&str>,
    actual: u64,
    limit: u64,
) -> Result<()> {
    if actual > limit {
        return Err(Error::RestoreLimitExceeded {
            resource,
            object_key: object_key.map(str::to_string),
            limit,
            actual,
        });
    }
    Ok(())
}

fn checked_restore_add(resource: &'static str, left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or(Error::RestoreSizeOverflow { resource })
}

fn checked_checkpoint_suffix_entry_count(manifest: &CheckpointManifest) -> Result<u64> {
    manifest.segments.iter().try_fold(0_u64, |total, record| {
        let count = record
            .end_index()
            .checked_sub(record.start_index())
            .and_then(|distance| distance.checked_add(1))
            .ok_or(Error::RestoreSizeOverflow {
                resource: "checkpoint suffix entry count",
            })?;
        checked_restore_add("checkpoint suffix entry count", total, count)
    })
}

fn checkpoint_suffix_shape(manifest: &CheckpointManifest) -> Result<CheckpointSuffixShape> {
    let entry_count = checked_checkpoint_suffix_entry_count(manifest)?;
    let stable_outer_bytes = entry_count
        .checked_mul(CHECKPOINT_RESTORED_ENTRY_OVERHEAD_BUDGET_BYTES)
        .ok_or(Error::RestoreSizeOverflow {
            resource: "restored suffix container bytes",
        })?;
    let entry_count = usize::try_from(entry_count).map_err(|_| Error::RestoreSizeOverflow {
        resource: "checkpoint suffix entry count",
    })?;
    Ok(CheckpointSuffixShape {
        entry_count,
        stable_outer_bytes,
    })
}

fn restored_suffix_outer_bytes(capacity: usize) -> Result<u64> {
    capacity
        .checked_mul(std::mem::size_of::<LogEntry>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(Error::RestoreSizeOverflow {
            resource: "restored suffix container bytes",
        })
}

fn validate_restored_suffix_allocation(
    capacity: usize,
    len: usize,
    expected_capacity: usize,
    shape: CheckpointSuffixShape,
) -> Result<u64> {
    if capacity != expected_capacity || capacity < shape.entry_count || len > shape.entry_count {
        return Err(Error::InvalidCheckpoint(
            "restored suffix container changed outside its exact allocation".into(),
        ));
    }
    let actual = restored_suffix_outer_bytes(capacity)?;
    ensure_restore_limit(
        "restored suffix container bytes",
        None,
        actual,
        shape.stable_outer_bytes,
    )?;
    Ok(actual)
}

fn allocate_restored_suffix(shape: CheckpointSuffixShape) -> Result<(Vec<LogEntry>, usize, u64)> {
    let mut restored = Vec::new();
    restored
        .try_reserve_exact(shape.entry_count)
        .map_err(|error| {
            Error::InvalidCheckpoint(format!("checkpoint suffix allocation failed: {error}"))
        })?;
    let capacity = restored.capacity();
    let outer_bytes = validate_restored_suffix_allocation(capacity, 0, capacity, shape)?;
    Ok((restored, capacity, outer_bytes))
}

fn map_restore_object_error(error: ObjStoreError, resource: &'static str) -> Error {
    match error {
        ObjStoreError::ReadLimitExceeded { key, limit, actual } => Error::RestoreLimitExceeded {
            resource,
            object_key: Some(key),
            limit,
            actual,
        },
        ObjStoreError::ContentLengthMismatch {
            key,
            expected,
            actual,
        } => Error::SizeMismatch {
            object_key: key,
            expected,
            actual,
        },
        error => Error::ObjectStore(error),
    }
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

/// Determines the shape of a checkpoint append using only the manifest and
/// the caller's batch. Hash equality at an archived overlap is deliberately
/// not proven here: that proof can require downloading a segment, so it must
/// be repeated while the exact Publisher lease is active.
fn local_publication_suffix_start(
    manifest: &CheckpointManifest,
    entries: &[LogEntry],
) -> Result<Option<usize>> {
    let first = entries.first().expect("non-empty publication");
    let last = entries.last().expect("non-empty publication");
    let tip = manifest.tip;

    if tip.index >= last.index {
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
        entries.get(offset).ok_or_else(|| {
            Error::InvalidCheckpoint("checkpoint tip is outside publication batch".into())
        })?;
        return Ok(Some(offset + 1));
    }

    verify_publication_hash(first.index.saturating_sub(1), tip.hash, first.prev_hash)?;
    Ok(Some(0))
}

fn checkpoint_effect_refs_for_suffix(
    entries: &[LogEntry],
    effects: &[CheckpointEffectRecord],
    suffix_start: usize,
) -> Result<Vec<CheckpointEffectRecord>> {
    // The caller may replay an overlapping batch and therefore provide refs
    // for its already-archived prefix. Validate the complete input first, but
    // serialize only the exact new suffix in entry-index order.
    let effects_by_index = effects
        .iter()
        .map(|effect| (effect.entry_index, effect))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut suffix_effects = Vec::new();
    for entry in entries.get(suffix_start..).ok_or_else(|| {
        Error::InvalidCheckpoint("checkpoint suffix is outside publication batch".into())
    })? {
        let is_external_effect = ExternalEffectCommand::decode(&entry.payload).is_ok();
        match (is_external_effect, effects_by_index.get(&entry.index)) {
            (true, Some(effect)) => suffix_effects.push((*effect).clone()),
            (true, None) => {
                return Err(Error::InvalidCheckpoint(
                    "checkpoint QEFX suffix entry has no effect reference".into(),
                ));
            }
            (false, Some(_)) => {
                return Err(Error::InvalidCheckpoint(
                    "checkpoint effect reference targets a non-QEFX suffix entry".into(),
                ));
            }
            (false, None) => {}
        }
    }
    Ok(suffix_effects)
}

fn checkpoint_namespace(identity: &CheckpointIdentity) -> String {
    format!(
        "rhiza/{}/checkpoints/epoch-{:020}/config-{:020}-digest-{}/generation-{:020}",
        identity.cluster_id,
        identity.epoch,
        identity.config_id,
        identity.config_digest.to_hex(),
        identity.recovery_generation
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

fn checkpoint_effect_prefix(
    identity: &CheckpointIdentity,
    index: LogIndex,
    digest: LogHash,
) -> String {
    format!(
        "{}/effects/{index:020}-{}",
        checkpoint_namespace(identity),
        digest.to_hex()
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

fn checkpoint_publication_receipt_key(
    identity: &CheckpointIdentity,
    holder: &str,
    manifest_digest: LogHash,
) -> Result<String> {
    if holder.is_empty() || holder.len() > 1024 {
        return Err(Error::InvalidCheckpoint(
            "checkpoint receipt holder is not a portable identifier".into(),
        ));
    }
    let holder_hash =
        LogHash::digest(&[b"rhiza-checkpoint-receipt-holder-v1\0", holder.as_bytes()]);
    Ok(format!(
        "{}/receipts/{}/{}.json",
        checkpoint_namespace(identity),
        holder_hash.to_hex(),
        manifest_digest.to_hex()
    ))
}

fn serialize_json(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| Error::Serialization(error.to_string()))
}

fn serialize_checkpoint_manifest(manifest: &CheckpointManifest) -> Result<Vec<u8>> {
    let bytes = serialize_json(manifest)?;
    ensure_restore_limit(
        "manifest encoded bytes",
        None,
        u64::try_from(bytes.len()).map_err(|_| Error::RestoreSizeOverflow {
            resource: "manifest encoded bytes",
        })?,
        CHECKPOINT_RESTORE_LIMITS.manifest_encoded_bytes,
    )?;
    Ok(bytes)
}

fn serialize_gc_control(control: &GcControl) -> Result<Vec<u8>> {
    let bytes = serialize_json(control)?;
    let actual = u64::try_from(bytes.len())
        .map_err(|_| Error::InvalidGc("control serialization size overflow".into()))?;
    if actual > GC_CONTROL_ENCODED_BYTES {
        return Err(Error::InvalidGc(format!(
            "control encoded bytes exceed limit {GC_CONTROL_ENCODED_BYTES}: {actual}"
        )));
    }
    Ok(bytes)
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

fn is_known_retired_checkpoint_object(identity: &CheckpointIdentity, key: &str) -> bool {
    if is_known_checkpoint_object(identity, key) {
        return true;
    }
    let prefix = checkpoint_namespace(identity) + "/receipts/";
    let Some(relative) = key.strip_prefix(&prefix) else {
        return false;
    };
    let Some((holder, file_name)) = relative.split_once('/') else {
        return false;
    };
    holder.len() == 64
        && holder.bytes().all(|byte| byte.is_ascii_hexdigit())
        && !file_name.contains('/')
        && file_name.len() == 69
        && file_name.ends_with(".json")
        && file_name[..64].bytes().all(|byte| byte.is_ascii_hexdigit())
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
    use rhiza_core::{
        ConfigurationState, EntryType, ExternalEffectProfile, LogAnchor, SnapshotIdentity,
    };
    use rhiza_obj_store::{Error as ObjStoreError, ObjStoreConfig};

    const TEST_READER_LEASE_MS: u64 = 60_000;

    async fn wait_for_test_gate(entered: std::sync::mpsc::Receiver<()>, message: &'static str) {
        tokio::task::spawn_blocking(move || entered.recv_timeout(Duration::from_secs(2)))
            .await
            .expect("test gate receiver task must not panic")
            .expect(message);
    }

    async fn enqueue_publisher_flush(
        publisher: &CheckpointPublisher,
        entries: Vec<LogEntry>,
    ) -> tokio::sync::oneshot::Receiver<Result<LoadedCheckpointManifest>> {
        let (result, receiver) = tokio::sync::oneshot::channel();
        publisher
            .state
            .lock()
            .await
            .pending
            .push(PendingPublisherFlush { entries, result });
        receiver
    }

    async fn reader_renewal_fixture(
        operation_id: &str,
    ) -> (
        tempfile::TempDir,
        ObjectArchiveStore,
        GcPlan,
        TestCheckpointManifestGate,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::Receiver<()>,
    ) {
        let (directory, _store, archive) = fixture();
        archive.publish_committed(&[entry()]).await.unwrap();
        let now = now_ms();
        archive.set_gc_root(identity(), now).await.unwrap();
        let plan = archive
            .plan_gc(GcPolicy::new(operation_id, identity(), 0, 0, 0), now)
            .await
            .unwrap();
        let (manifest_gate, manifest_entered, manifest_cancelled) =
            TestCheckpointManifestGate::new(archive.test_store_identity);
        (
            directory,
            archive,
            plan,
            manifest_gate,
            manifest_entered,
            manifest_cancelled,
        )
    }

    async fn complete_one_timely_reader_renewal(archive: &ObjectArchiveStore) {
        let (control_gate, control_entered) =
            TestGcControlGate::new(archive.test_store_identity, TestGcControlOperation::Update);
        let installed_control = install_test_gc_control_gate(control_gate.clone());
        let control_release = control_gate.release_guard();
        tokio::time::advance(Duration::from_millis(
            TEST_READER_LEASE_MS / READER_LEASE_RENEW_DIVISOR,
        ))
        .await;
        wait_for_test_gate(
            control_entered,
            "Reader renewal did not reach its timely GC-control CAS",
        )
        .await;
        drop(control_release);
        tokio::task::yield_now().await;
        assert!(archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .iter()
            .any(|lease| lease.kind == GcLeaseKind::Reader));
        drop(installed_control);
    }

    #[tokio::test]
    async fn publisher_explicit_renew_recovers_expired_or_missing_lease() {
        let (_dir, _store, archive) = fixture();
        let publisher = archive
            .open_checkpoint_publisher("publisher", CheckpointPublisherOptions::default())
            .await
            .unwrap();

        publisher.renew_at(100).await.unwrap();
        publisher.renew_at(60_101).await.unwrap();
        archive.release_gc_lease(&publisher.lease_id).await.unwrap();
        publisher.renew().await.unwrap();

        let loaded = publisher.publish_committed(&[entry()]).await.unwrap();
        assert_eq!(loaded.manifest().tip().index(), 1);
    }

    #[tokio::test]
    async fn fresh_publisher_lease_renewals_do_not_rewrite_gc_control() {
        let (_dir, _store, archive) = fixture();
        let duration_ms = 60_000;
        let now = now_ms();
        let publisher = archive
            .open_checkpoint_publisher(
                "coalesced-renewal",
                CheckpointPublisherOptions::new(duration_ms),
            )
            .await
            .unwrap();

        let before = archive.load_gc_control().await.unwrap();
        publisher.renew_at(now).await.unwrap();
        archive
            .renew_active_publisher_gc_lease(&publisher.lease_id, duration_ms)
            .await
            .unwrap();
        let after_fresh_renewals = archive.load_gc_control().await.unwrap();
        assert_eq!(after_fresh_renewals.control, before.control);
        assert_eq!(after_fresh_renewals.version, before.version);

        let published = publisher.publish_committed(&[entry()]).await.unwrap();
        assert_eq!(published.manifest().tip().index(), 1);
        let after_publish = archive.load_gc_control().await.unwrap();
        assert_eq!(after_publish.control, before.control);
        assert_eq!(after_publish.version, before.version);

        let mut expiring = after_publish;
        let lease = expiring
            .control
            .leases
            .iter_mut()
            .find(|lease| lease.lease_id == publisher.lease_id)
            .unwrap();
        lease.expires_at_ms = now.saturating_add(duration_ms / PUBLISHER_LEASE_RENEW_DIVISOR);
        archive.update_gc_control(&expiring).await.unwrap();
        let before_required_renewal = archive.load_gc_control().await.unwrap();

        publisher.renew_at(now).await.unwrap();
        let after_required_renewal = archive.load_gc_control().await.unwrap();
        assert_ne!(
            after_required_renewal.version,
            before_required_renewal.version
        );
        let renewed = after_required_renewal
            .control
            .leases
            .iter()
            .find(|lease| lease.lease_id == publisher.lease_id)
            .unwrap();
        assert_eq!(renewed.expires_at_ms, now.saturating_add(duration_ms));
    }

    #[tokio::test]
    async fn publisher_lease_acquisition_retries_bounded_gc_control_throttling() {
        let (_dir, _store, archive) = fixture();
        archive.ensure_gc_control().await.unwrap();
        let (gate, _entered) =
            TestGcControlGate::throttled_mutations(archive.test_store_identity, 2);
        let release = gate.release_guard();
        let _installed = install_test_gc_control_gate(gate);
        drop(release);

        let lease = archive
            .acquire_named_lease(
                GcLeaseKind::Publisher,
                "startup-node-1".into(),
                now_ms(),
                30_000,
            )
            .await
            .unwrap();
        assert!(archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .iter()
            .any(|stored| stored.lease_id == lease.lease_id));
    }

    #[tokio::test]
    async fn three_publishers_acquire_through_one_shared_mutation_window() {
        let (_dir, _store, archive) = fixture();
        let (gate, _entered) = TestGcControlGate::rate_limited_mutations(
            archive.test_store_identity,
            Duration::from_millis(50),
        );
        let release = gate.release_guard();
        let _installed = install_test_gc_control_gate(gate);
        drop(release);

        let first = archive.clone();
        let second = archive.clone();
        let third = archive.clone();
        let (first, second, third) = tokio::join!(
            first.acquire_named_lease(
                GcLeaseKind::Publisher,
                "startup-node-1".into(),
                now_ms(),
                30_000,
            ),
            second.acquire_named_lease(
                GcLeaseKind::Publisher,
                "startup-node-2".into(),
                now_ms(),
                30_000,
            ),
            third.acquire_named_lease(
                GcLeaseKind::Publisher,
                "startup-node-3".into(),
                now_ms(),
                30_000,
            ),
        );
        let leases = [first.unwrap(), second.unwrap(), third.unwrap()];
        let loaded = archive.load_gc_control().await.unwrap();
        assert!(leases.iter().all(|expected| loaded
            .control
            .leases
            .iter()
            .any(|actual| actual.lease_id == expected.lease_id)));
    }

    #[test]
    fn gc_control_throttle_classifier_is_narrow() {
        assert!(gc_control_throttled(&ObjStoreError::Transport {
            key: "gc/control.json".into(),
            message: "429 Too Many Requests".into(),
        }));
        assert!(gc_control_throttled(&ObjStoreError::Transport {
            key: "gc/control.json".into(),
            message: "<Code>SlowDown</Code>".into(),
        }));
        assert!(!gc_control_throttled(&ObjStoreError::Transport {
            key: "gc/control.json".into(),
            message: "connection reset".into(),
        }));
        assert!(!gc_control_throttled(&ObjStoreError::Precondition {
            key: "gc/control.json".into(),
        }));
    }

    #[tokio::test]
    async fn coalesced_replay_rejects_missing_or_expired_lease_before_archived_read() {
        let (_directory, _store, archive) = fixture();
        let first = entry();
        let first_loaded = archive
            .publish_committed(std::slice::from_ref(&first))
            .await
            .unwrap();
        let first_key = first_loaded.manifest().segments()[0]
            .object_key()
            .to_string();
        let publisher = archive
            .open_checkpoint_publisher("coalesced-replay", CheckpointPublisherOptions::default())
            .await
            .unwrap();
        let (gate, entered, _cancelled) =
            TestCheckpointDownloadGate::new(archive.test_store_identity, first_key);
        let _installed = install_test_checkpoint_download_gate(gate.clone());
        let release = gate.release_guard();

        archive.release_gc_lease(&publisher.lease_id).await.unwrap();
        let missing_control_before = archive.load_gc_control().await.unwrap();
        let missing = enqueue_publisher_flush(&publisher, vec![first.clone()]).await;
        tokio::time::timeout(Duration::from_secs(2), publisher.drive_flushes())
            .await
            .expect("missing Publisher replay proof must not deadlock");
        assert!(matches!(
            missing.await.unwrap(),
            Err(Error::GcLeaseMissing { ref lease_id }) if lease_id == &publisher.lease_id
        ));
        let missing_control_after = archive.load_gc_control().await.unwrap();
        assert_eq!(
            missing_control_after.control,
            missing_control_before.control
        );
        assert_eq!(
            missing_control_after.version,
            missing_control_before.version
        );

        publisher.renew_at(0).await.unwrap();
        let expired_control_before = archive.load_gc_control().await.unwrap();
        let expired = enqueue_publisher_flush(&publisher, vec![first]).await;
        tokio::time::timeout(Duration::from_secs(2), publisher.drive_flushes())
            .await
            .expect("expired Publisher replay proof must not deadlock");
        assert!(matches!(
            expired.await.unwrap(),
            Err(Error::GcLeaseMissing { ref lease_id }) if lease_id == &publisher.lease_id
        ));
        let expired_control_after = archive.load_gc_control().await.unwrap();
        assert_eq!(
            expired_control_after.control,
            expired_control_before.control
        );
        assert_eq!(
            expired_control_after.version,
            expired_control_before.version
        );
        assert!(matches!(
            entered.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        drop(release);
    }

    #[tokio::test]
    async fn renewed_coalesced_replay_holds_gc_fence_during_archived_read() {
        let (_directory, store, archive) = fixture();
        let first = entry();
        let first_loaded = archive
            .publish_committed(std::slice::from_ref(&first))
            .await
            .unwrap();
        let first_key = first_loaded.manifest().segments()[0]
            .object_key()
            .to_string();
        archive.set_gc_root(identity(), now_ms()).await.unwrap();
        let plan_now = now_ms();
        let plan = archive
            .plan_gc(
                GcPolicy::new("coalesced-replay-proof", identity(), 0, 0, 0),
                plan_now,
            )
            .await
            .unwrap();
        let publisher = Arc::new(
            archive
                .open_checkpoint_publisher(
                    "coalesced-replay",
                    CheckpointPublisherOptions::default(),
                )
                .await
                .unwrap(),
        );
        let receiver = enqueue_publisher_flush(&publisher, vec![first]).await;
        let (gate, entered, _cancelled) =
            TestCheckpointDownloadGate::new(archive.test_store_identity, first_key.clone());
        let _installed = install_test_checkpoint_download_gate(gate.clone());
        let release = gate.release_guard();
        let drive_publisher = Arc::clone(&publisher);
        let drive = tokio::spawn(async move {
            drive_publisher.drive_flushes().await;
        });
        wait_for_test_gate(
            entered,
            "renewed coalesced replay did not reach the archived segment read",
        )
        .await;

        let proof_now = now_ms();
        assert!(archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .iter()
            .any(|lease| {
                lease.lease_id == publisher.lease_id
                    && lease.kind == GcLeaseKind::Publisher
                    && lease.expires_at_ms > proof_now
            }));
        archive.acquire_gc_barrier(&plan, proof_now).await.unwrap();
        let delete = archive.enter_delete_phase(&plan, proof_now).await;
        assert!(matches!(delete, Err(Error::GcBarrierBusy { .. })));
        assert!(store.get(&first_key).await.is_ok());

        drop(release);
        drive.await.unwrap();
        assert_eq!(receiver.await.unwrap().unwrap(), first_loaded);
        archive.abort_gc("coalesced-replay-proof").await.unwrap();
        archive.release_gc_lease(&publisher.lease_id).await.unwrap();
    }

    #[tokio::test]
    async fn coherent_restore_distinguishes_missing_and_initialized_genesis() {
        let (_dir, _store, archive) = fixture();
        assert!(archive.load_checkpoint_restore().await.unwrap().is_none());

        let initialized = archive.initialize_checkpoint().await.unwrap();
        let restored = archive.load_checkpoint_restore().await.unwrap().unwrap();
        assert_eq!(restored.loaded(), &initialized);
        assert_eq!(restored.restored().tip(), initialized.manifest().tip());
        assert!(restored.restored().snapshot().is_none());
        assert!(restored.restored().suffix().is_empty());
    }

    #[tokio::test]
    async fn coherent_restore_is_pinned_to_the_loaded_manifest_and_renews_reader_lease() {
        let (_dir, _store, archive) = fixture();
        let first = entry();
        archive
            .publish_committed(std::slice::from_ref(&first))
            .await
            .unwrap();
        let loaded = archive.load_checkpoint_unleased().await.unwrap().unwrap();
        let reader = archive
            .acquire_operation_lease(GcLeaseKind::Reader, 100, 1)
            .await
            .unwrap();

        let second = LogEntry {
            index: 2,
            prev_hash: first.hash,
            hash: LogEntry::calculate_hash(
                "cluster-a",
                2,
                7,
                3,
                EntryType::Command,
                first.hash,
                b"second",
            ),
            payload: b"second".to_vec(),
            ..first.clone()
        };
        archive
            .publish_committed(std::slice::from_ref(&second))
            .await
            .unwrap();

        let restored = archive
            .restore_loaded_checkpoint_unleased(&loaded, &reader.lease_id)
            .await
            .unwrap();
        assert_eq!(restored.tip(), loaded.manifest().tip());
        assert_eq!(restored.suffix(), std::slice::from_ref(&first));
        let control = archive.load_gc_control().await.unwrap();
        assert!(control
            .control
            .leases
            .iter()
            .any(|lease| lease.lease_id == reader.lease_id && lease.expires_at_ms > 100));
        archive.release_gc_lease(&reader.lease_id).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn reader_renewal_load_crossing_prior_hard_deadline_is_terminal_and_unblocks_gc() {
        let (_directory, archive, plan, manifest_gate, manifest_entered, manifest_cancelled) =
            reader_renewal_fixture("reader-late-load").await;
        let _installed_manifest = install_test_checkpoint_manifest_gate(manifest_gate.clone());
        let _manifest_release = manifest_gate.release_guard();
        let restore_archive = archive.clone();
        let restore = tokio::spawn(async move {
            restore_archive
                .load_checkpoint_restore_with_reader_lease_duration(TEST_READER_LEASE_MS)
                .await
        });
        wait_for_test_gate(
            manifest_entered,
            "Reader restore did not reach the manifest gate",
        )
        .await;
        let reader_lease_id = archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .iter()
            .find(|lease| lease.kind == GcLeaseKind::Reader)
            .expect("gated restore must hold one Reader lease")
            .lease_id
            .clone();
        let (control_gate, control_entered) =
            TestGcControlGate::new(archive.test_store_identity, TestGcControlOperation::Load);
        let _installed_control = install_test_gc_control_gate(control_gate.clone());
        let control_release = control_gate.release_guard();

        tokio::time::advance(Duration::from_millis(
            TEST_READER_LEASE_MS / READER_LEASE_RENEW_DIVISOR,
        ))
        .await;
        wait_for_test_gate(
            control_entered,
            "Reader renewal did not reach the GC-control load gate",
        )
        .await;
        tokio::time::advance(Duration::from_millis(
            TEST_READER_LEASE_MS - TEST_READER_LEASE_MS / READER_LEASE_RENEW_DIVISOR + 1,
        ))
        .await;
        drop(control_release);

        let result = restore.await.unwrap();
        assert!(
            matches!(result, Err(Error::GcLeaseMissing { ref lease_id }) if lease_id == &reader_lease_id),
            "a GC-control load completing after the prior hard deadline must terminate the Reader: {result:?}"
        );
        wait_for_test_gate(
            manifest_cancelled,
            "late Reader renewal did not cancel the protected manifest read",
        )
        .await;
        assert!(archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .is_empty());
        archive.acquire_gc_barrier(&plan, now_ms()).await.unwrap();
        archive.enter_delete_phase(&plan, now_ms()).await.unwrap();
        archive.abort_gc(&plan.operation_id).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn reader_renewal_cas_crossing_prior_hard_deadline_is_terminal_and_unblocks_gc() {
        let (_directory, archive, plan, manifest_gate, manifest_entered, manifest_cancelled) =
            reader_renewal_fixture("reader-late-cas").await;
        let _installed_manifest = install_test_checkpoint_manifest_gate(manifest_gate.clone());
        let _manifest_release = manifest_gate.release_guard();
        let restore_archive = archive.clone();
        let restore = tokio::spawn(async move {
            restore_archive
                .load_checkpoint_restore_with_reader_lease_duration(TEST_READER_LEASE_MS)
                .await
        });
        wait_for_test_gate(
            manifest_entered,
            "Reader restore did not reach the manifest gate",
        )
        .await;
        let reader_lease_id = archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .iter()
            .find(|lease| lease.kind == GcLeaseKind::Reader)
            .expect("gated restore must hold one Reader lease")
            .lease_id
            .clone();
        let (control_gate, control_entered) =
            TestGcControlGate::new(archive.test_store_identity, TestGcControlOperation::Update);
        let _installed_control = install_test_gc_control_gate(control_gate.clone());
        let control_release = control_gate.release_guard();

        tokio::time::advance(Duration::from_millis(
            TEST_READER_LEASE_MS / READER_LEASE_RENEW_DIVISOR,
        ))
        .await;
        wait_for_test_gate(
            control_entered,
            "Reader renewal did not reach the GC-control CAS gate",
        )
        .await;
        tokio::time::advance(Duration::from_millis(
            TEST_READER_LEASE_MS - TEST_READER_LEASE_MS / READER_LEASE_RENEW_DIVISOR + 1,
        ))
        .await;
        drop(control_release);

        let result = restore.await.unwrap();
        assert!(
            matches!(result, Err(Error::GcLeaseMissing { ref lease_id }) if lease_id == &reader_lease_id),
            "a Reader CAS completing after the prior hard deadline must be cleaned up and terminate: {result:?}"
        );
        wait_for_test_gate(
            manifest_cancelled,
            "late Reader CAS did not cancel the protected manifest read",
        )
        .await;
        assert!(archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .is_empty());
        archive.acquire_gc_barrier(&plan, now_ms()).await.unwrap();
        archive.enter_delete_phase(&plan, now_ms()).await.unwrap();
        archive.abort_gc(&plan.operation_id).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn reader_renewal_late_cas_error_preserves_exact_error_without_cleanup() {
        let (_directory, _store, archive) = fixture();
        archive.publish_committed(&[entry()]).await.unwrap();
        let reader = archive
            .acquire_operation_lease(GcLeaseKind::Reader, now_ms(), TEST_READER_LEASE_MS)
            .await
            .unwrap();
        let original_lease = archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .iter()
            .find(|lease| lease.lease_id == reader.lease_id)
            .expect("acquired Reader lease must be stored")
            .clone();
        let prior_hard_deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_millis(TEST_READER_LEASE_MS))
            .unwrap();
        let injected = ObjStoreError::Transport {
            key: archive.gc_control_key(),
            message: "injected Reader renewal CAS failure".into(),
        };
        let (control_gate, control_entered) =
            TestGcControlGate::failing_update(archive.test_store_identity, injected.clone());
        let installed_control = install_test_gc_control_gate(control_gate.clone());
        let control_release = control_gate.release_guard();
        let renewal_archive = archive.clone();
        let renewal_lease_id = reader.lease_id.clone();
        let renewal = tokio::spawn(async move {
            renewal_archive
                .renew_active_reader_gc_lease(
                    &renewal_lease_id,
                    prior_hard_deadline,
                    TEST_READER_LEASE_MS,
                )
                .await
        });
        wait_for_test_gate(
            control_entered,
            "Reader renewal did not reach the injected CAS failure gate",
        )
        .await;
        tokio::time::advance(Duration::from_millis(TEST_READER_LEASE_MS + 1)).await;
        drop(control_release);

        assert_eq!(renewal.await.unwrap(), Err(Error::ObjectStore(injected)));
        drop(installed_control);
        let stored_lease = archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .into_iter()
            .find(|lease| lease.lease_id == reader.lease_id)
            .expect("failed unwritten CAS must not clean up the existing Reader lease");
        assert_eq!(stored_lease, original_lease);
        archive.release_gc_lease(&reader.lease_id).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn missing_reader_lease_cancels_protected_load_without_recreation() {
        let (_directory, archive, plan, manifest_gate, manifest_entered, manifest_cancelled) =
            reader_renewal_fixture("reader-missing").await;
        let _installed_manifest = install_test_checkpoint_manifest_gate(manifest_gate.clone());
        let _manifest_release = manifest_gate.release_guard();
        let restore_archive = archive.clone();
        let restore = tokio::spawn(async move {
            restore_archive
                .load_checkpoint_restore_with_reader_lease_duration(TEST_READER_LEASE_MS)
                .await
        });
        wait_for_test_gate(
            manifest_entered,
            "Reader restore did not reach the manifest gate",
        )
        .await;
        let reader_lease_id = archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .iter()
            .find(|lease| lease.kind == GcLeaseKind::Reader)
            .expect("gated restore must hold one Reader lease")
            .lease_id
            .clone();
        archive.release_gc_lease(&reader_lease_id).await.unwrap();
        tokio::time::advance(Duration::from_millis(
            TEST_READER_LEASE_MS / READER_LEASE_RENEW_DIVISOR,
        ))
        .await;

        let result = restore.await.unwrap();
        assert!(
            matches!(result, Err(Error::GcLeaseMissing { ref lease_id }) if lease_id == &reader_lease_id),
            "a missing Reader must remain terminal and must not be recreated: {result:?}"
        );
        wait_for_test_gate(
            manifest_cancelled,
            "missing Reader did not cancel the protected manifest read",
        )
        .await;
        assert!(archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .is_empty());
        archive.acquire_gc_barrier(&plan, now_ms()).await.unwrap();
        archive.enter_delete_phase(&plan, now_ms()).await.unwrap();
        archive.abort_gc(&plan.operation_id).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn reader_renewal_completed_before_prior_hard_deadline_keeps_gc_fenced() {
        let (_directory, archive, plan, manifest_gate, manifest_entered, _manifest_cancelled) =
            reader_renewal_fixture("reader-timely-cas").await;
        let _installed_manifest = install_test_checkpoint_manifest_gate(manifest_gate.clone());
        let manifest_release = manifest_gate.release_guard();
        let restore_archive = archive.clone();
        let restore = tokio::spawn(async move {
            restore_archive
                .load_checkpoint_restore_with_reader_lease_duration(TEST_READER_LEASE_MS)
                .await
        });
        wait_for_test_gate(
            manifest_entered,
            "Reader restore did not reach the manifest gate",
        )
        .await;
        let (control_gate, control_entered) =
            TestGcControlGate::new(archive.test_store_identity, TestGcControlOperation::Update);
        let _installed_control = install_test_gc_control_gate(control_gate.clone());
        let control_release = control_gate.release_guard();

        tokio::time::advance(Duration::from_millis(
            TEST_READER_LEASE_MS / READER_LEASE_RENEW_DIVISOR,
        ))
        .await;
        wait_for_test_gate(
            control_entered,
            "Reader renewal did not reach the GC-control CAS gate",
        )
        .await;
        tokio::time::advance(Duration::from_millis(1)).await;
        drop(control_release);

        archive.acquire_gc_barrier(&plan, now_ms()).await.unwrap();
        let delete = archive.enter_delete_phase(&plan, now_ms()).await;
        assert!(
            matches!(delete, Err(Error::GcBarrierBusy { .. })),
            "a timely Reader renewal must keep the generation protected: {delete:?}"
        );
        archive.abort_gc(&plan.operation_id).await.unwrap();
        drop(manifest_release);
        let restored = restore.await.unwrap().unwrap().unwrap();
        assert_eq!(restored.restored().suffix(), std::slice::from_ref(&entry()));
    }

    #[tokio::test(start_paused = true)]
    async fn coherent_restore_renews_a_short_reader_lease_during_one_delayed_fetch() {
        let (_dir, _store, archive) = fixture();
        archive.publish_committed(&[entry()]).await.unwrap();
        let now = now_ms();
        archive.set_gc_root(identity(), now).await.unwrap();
        let plan = archive
            .plan_gc(GcPolicy::new("delayed-reader", identity(), 0, 0, 0), now)
            .await
            .unwrap();
        let object_key = archive
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .segments()[0]
            .object_key()
            .to_owned();
        let (gate, entered, _cancelled) =
            TestCheckpointDownloadGate::new(archive.test_store_identity, object_key);
        let _installed = install_test_checkpoint_download_gate(gate.clone());
        let release = gate.release_guard();
        let restore_archive = archive.clone();
        let restore = tokio::spawn(async move {
            restore_archive
                .load_checkpoint_restore_with_reader_lease_duration(TEST_READER_LEASE_MS)
                .await
        });
        wait_for_test_gate(entered, "Reader restore did not reach the object gate").await;

        // Three exact, timely control-store updates keep one Reader live while
        // the object fetch remains paused beyond its original hard deadline.
        for _ in 0..3 {
            complete_one_timely_reader_renewal(&archive).await;
        }
        archive.acquire_gc_barrier(&plan, now_ms()).await.unwrap();
        let delete = archive.enter_delete_phase(&plan, now_ms()).await;
        assert!(
            matches!(delete, Err(Error::GcBarrierBusy { .. })),
            "{delete:?}"
        );

        drop(release);
        let restored = restore.await.unwrap().unwrap().unwrap();
        assert_eq!(restored.restored().suffix(), std::slice::from_ref(&entry()));
        archive.abort_gc(&plan.operation_id).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn coherent_restore_keeps_one_reader_lease_live_across_a_long_manifest() {
        let (_dir, _store, archive) = fixture();
        let first = entry();
        let second = next_entry(&first);
        let third = next_entry(&second);
        archive
            .publish_committed(std::slice::from_ref(&first))
            .await
            .unwrap();
        archive
            .publish_committed(std::slice::from_ref(&second))
            .await
            .unwrap();
        archive
            .publish_committed(std::slice::from_ref(&third))
            .await
            .unwrap();
        let now = now_ms();
        archive.set_gc_root(identity(), now).await.unwrap();
        let plan = archive
            .plan_gc(
                GcPolicy::new("long-manifest-reader", identity(), 0, 0, 0),
                now,
            )
            .await
            .unwrap();
        let final_object_key = archive
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .segments()
            .last()
            .expect("three independent publications create three manifest segments")
            .object_key()
            .to_owned();
        let (gate, entered, _cancelled) =
            TestCheckpointDownloadGate::new(archive.test_store_identity, final_object_key);
        let _installed = install_test_checkpoint_download_gate(gate.clone());
        let release = gate.release_guard();
        let restore_archive = archive.clone();
        let restore = tokio::spawn(async move {
            restore_archive
                .load_checkpoint_restore_with_reader_lease_duration(TEST_READER_LEASE_MS)
                .await
        });
        wait_for_test_gate(
            entered,
            "Reader restore did not reach the final object gate",
        )
        .await;

        // The first two objects have already been restored. Keeping the
        // final fetch paused demonstrates that the one outer renewal owner
        // covers the complete, multi-object manifest rather than each object
        // opportunistically creating its own reader lease loop.
        for _ in 0..3 {
            complete_one_timely_reader_renewal(&archive).await;
        }
        archive.acquire_gc_barrier(&plan, now_ms()).await.unwrap();
        let delete = archive.enter_delete_phase(&plan, now_ms()).await;
        assert!(
            matches!(delete, Err(Error::GcBarrierBusy { .. })),
            "{delete:?}"
        );

        drop(release);
        let restored = restore.await.unwrap().unwrap().unwrap();
        assert_eq!(restored.restored().suffix(), &[first, second, third]);
        archive.abort_gc(&plan.operation_id).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn coherent_restore_renews_a_short_reader_lease_during_a_delayed_manifest_fetch() {
        let (_dir, _store, archive) = fixture();
        archive.initialize_checkpoint().await.unwrap();
        let now = now_ms();
        archive.set_gc_root(identity(), now).await.unwrap();
        let plan = archive
            .plan_gc(
                GcPolicy::new("delayed-manifest-reader", identity(), 0, 0, 0),
                now,
            )
            .await
            .unwrap();
        let (gate, entered, _cancelled) =
            TestCheckpointManifestGate::new(archive.test_store_identity);
        let _installed = install_test_checkpoint_manifest_gate(gate.clone());
        let release = gate.release_guard();
        let restore_archive = archive.clone();
        let restore = tokio::spawn(async move {
            restore_archive
                .load_checkpoint_restore_with_reader_lease_duration(TEST_READER_LEASE_MS)
                .await
        });
        wait_for_test_gate(entered, "Reader restore did not reach the manifest gate").await;

        // The manifest remains pinned across three deterministic renewals and
        // beyond the original hard deadline.
        for _ in 0..3 {
            complete_one_timely_reader_renewal(&archive).await;
        }
        archive.acquire_gc_barrier(&plan, now_ms()).await.unwrap();
        let delete = archive.enter_delete_phase(&plan, now_ms()).await;
        assert!(
            matches!(delete, Err(Error::GcBarrierBusy { .. })),
            "{delete:?}"
        );

        drop(release);
        assert!(restore.await.unwrap().unwrap().is_some());
        archive.abort_gc(&plan.operation_id).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn reader_restore_blocks_then_reuses_the_same_gc_plan_after_release() {
        let (_dir, store, archive) = fixture();
        let root_identity = CheckpointIdentity::new(
            "cluster-a",
            7,
            3,
            LogHash::digest(&[b"archive-test-config"]),
            3,
        );
        let root = ObjectArchiveStore::new_checkpoint_for_single_process(
            store.clone(),
            root_identity.clone(),
        );
        archive.publish_committed(&[entry()]).await.unwrap();
        root.publish_committed(&[entry()]).await.unwrap();
        let now = now_ms();
        root.set_gc_root(root_identity.clone(), now.saturating_sub(1))
            .await
            .unwrap();
        let plan = root
            .plan_gc(
                GcPolicy::new("reader-load-blocks-gc", root_identity.clone(), 0, 0, 0),
                now,
            )
            .await
            .unwrap();
        let alternate = root
            .plan_gc(
                GcPolicy::new(
                    "reader-load-blocks-gc-alternate",
                    root_identity.clone(),
                    0,
                    0,
                    0,
                ),
                now,
            )
            .await
            .unwrap();
        assert!(plan.swept_generations.contains(&identity()));
        assert!(!plan.candidates.is_empty());

        let object_key = archive
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .segments()[0]
            .object_key()
            .to_owned();
        let (gate, entered, _cancelled) =
            TestCheckpointDownloadGate::new(archive.test_store_identity, object_key);
        let _installed = install_test_checkpoint_download_gate(gate.clone());
        let release = gate.release_guard();
        let restore_archive = archive.clone();
        let restore = tokio::spawn(async move {
            restore_archive
                .load_checkpoint_restore_with_reader_lease_duration(TEST_READER_LEASE_MS)
                .await
        });
        wait_for_test_gate(entered, "Reader restore did not reach the object gate").await;
        assert!(archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .iter()
            .any(|lease| lease.kind == GcLeaseKind::Reader));

        let blocked = root.execute_gc(plan.plan_hash(), now).await;
        let control = root.load_gc_control().await.unwrap();
        let active = control
            .control
            .active_gc
            .as_ref()
            .expect("blocked GC must retain its draining barrier");
        assert!(matches!(blocked, Err(Error::GcBarrierBusy { .. })));
        assert_eq!(active.phase, GcBarrierPhase::Draining);
        assert_eq!(active.fence, execution_fence(&plan));
        assert_eq!(active.plan_hash, plan.plan_hash());
        assert!(matches!(
            archive
                .acquire_gc_lease(
                    GcLeaseKind::Reader,
                    "late-reader",
                    now,
                    TEST_READER_LEASE_MS
                )
                .await,
            Err(Error::GcBarrierActive { .. })
        ));
        assert!(matches!(
            root.execute_gc(alternate.plan_hash(), now).await,
            Err(Error::GcBarrierActive { .. })
        ));
        assert!(root
            .load_gc_report(plan.plan_hash())
            .await
            .unwrap()
            .is_none());
        for candidate in &plan.candidates {
            assert!(store.get(&candidate.key).await.is_ok());
        }

        drop(release);
        let restored = restore.await.unwrap().unwrap().unwrap();
        assert_eq!(restored.restored().suffix(), std::slice::from_ref(&entry()));
        assert!(archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .is_empty());

        let report = root.execute_gc(plan.plan_hash(), now).await.unwrap();
        assert_eq!(report.results().len(), plan.candidates.len());
        assert!(report
            .results()
            .iter()
            .all(|evidence| evidence.outcome == GcDeleteOutcome::Deleted));
        assert_eq!(
            root.load_gc_report(plan.plan_hash()).await.unwrap(),
            Some(report)
        );
        assert!(root
            .load_gc_control()
            .await
            .unwrap()
            .control
            .active_gc
            .is_none());
        for candidate in &plan.candidates {
            assert!(matches!(
                store.get(&candidate.key).await,
                Err(ObjStoreError::NotFound { .. })
            ));
        }
        assert!(matches!(
            archive.load_checkpoint_restore().await,
            Err(Error::GenerationRetired { .. })
        ));
        assert_eq!(
            root.load_checkpoint_restore()
                .await
                .unwrap()
                .unwrap()
                .restored()
                .suffix(),
            std::slice::from_ref(&entry())
        );
    }

    #[tokio::test]
    async fn reported_gc_retry_from_reopened_archive_clears_matching_barrier() {
        let (_dir, store, archive) = fixture();
        let root_identity = CheckpointIdentity::new(
            "cluster-a",
            7,
            3,
            LogHash::digest(&[b"archive-test-config"]),
            3,
        );
        let root = ObjectArchiveStore::new_checkpoint_for_single_process(
            store.clone(),
            root_identity.clone(),
        );
        archive.publish_committed(&[entry()]).await.unwrap();
        root.publish_committed(&[entry()]).await.unwrap();
        let now = now_ms();
        root.set_gc_root(root_identity.clone(), now.saturating_sub(1))
            .await
            .unwrap();
        let plan = root
            .plan_gc(
                GcPolicy::new("reported-gc-retry", root_identity.clone(), 0, 0, 0),
                now,
            )
            .await
            .unwrap();
        let alternate = root
            .plan_gc(
                GcPolicy::new(
                    "reported-gc-retry-alternate",
                    root_identity.clone(),
                    0,
                    0,
                    0,
                ),
                now,
            )
            .await
            .unwrap();

        root.acquire_gc_barrier(&plan, now).await.unwrap();
        root.enter_delete_phase(&plan, now).await.unwrap();
        root.fence_gc_root(&plan).await.unwrap();
        root.retire_plan_generations(&plan, now).await.unwrap();
        let (gate, _entered) = TestGcControlGate::failing_update(
            root.test_store_identity,
            ObjStoreError::Transport {
                key: "gc/control.json".into(),
                message: "injected report-clear failure".into(),
            },
        );
        let release = gate.release_guard();
        let installed = install_test_gc_control_gate(gate);
        drop(release);
        assert!(matches!(
            root.execute_gc(plan.plan_hash(), now).await,
            Err(Error::ObjectStore(ObjStoreError::Transport { .. }))
        ));
        let report = root
            .load_gc_report(plan.plan_hash())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.results().len(), plan.candidates.len());
        assert!(root
            .load_gc_control()
            .await
            .unwrap()
            .control
            .active_gc
            .is_some());
        drop(installed);

        let reopened =
            ObjectArchiveStore::new_checkpoint_for_single_process(store.clone(), root_identity);
        let report_key = root.gc_report_key(plan.plan_hash());
        let mut invalid_format = report.clone();
        invalid_format.format_version += 1;
        let object = store.get_versioned(&report_key).await.unwrap();
        store
            .update(
                &report_key,
                serialize_json(&invalid_format).unwrap(),
                object.version().clone(),
            )
            .await
            .unwrap();
        assert!(matches!(
            reopened.execute_gc(plan.plan_hash(), now).await,
            Err(Error::InvalidGc(message)) if message == "execution report format version mismatch"
        ));
        assert!(reopened
            .load_gc_control()
            .await
            .unwrap()
            .control
            .active_gc
            .is_some());

        let mut mismatched_candidate = report.clone();
        mismatched_candidate.results[0].key.push('x');
        let object = store.get_versioned(&report_key).await.unwrap();
        store
            .update(
                &report_key,
                serialize_json(&mismatched_candidate).unwrap(),
                object.version().clone(),
            )
            .await
            .unwrap();
        assert!(matches!(
            reopened.execute_gc(plan.plan_hash(), now).await,
            Err(Error::InvalidGc(message)) if message == "execution report candidates do not match the plan"
        ));

        let mut duplicated_candidate = report.clone();
        duplicated_candidate
            .results
            .push(duplicated_candidate.results[0].clone());
        let object = store.get_versioned(&report_key).await.unwrap();
        store
            .update(
                &report_key,
                serialize_json(&duplicated_candidate).unwrap(),
                object.version().clone(),
            )
            .await
            .unwrap();
        assert!(matches!(
            reopened.execute_gc(plan.plan_hash(), now).await,
            Err(Error::InvalidGc(message)) if message == "execution report candidates do not match the plan"
        ));

        let object = store.get_versioned(&report_key).await.unwrap();
        store
            .update(
                &report_key,
                serialize_json(&report).unwrap(),
                object.version().clone(),
            )
            .await
            .unwrap();
        assert!(matches!(
            reopened.execute_gc(alternate.plan_hash(), now).await,
            Err(Error::GcBarrierActive { .. })
        ));
        assert_eq!(
            reopened.execute_gc(plan.plan_hash(), now).await.unwrap(),
            report
        );
        assert!(reopened
            .load_gc_control()
            .await
            .unwrap()
            .control
            .active_gc
            .is_none());
        for candidate in &plan.candidates {
            assert!(matches!(
                store.get(&candidate.key).await,
                Err(ObjStoreError::NotFound { .. })
            ));
        }
        assert!(matches!(
            archive.load_checkpoint_restore().await,
            Err(Error::GenerationRetired { .. })
        ));
        assert!(reopened.load_checkpoint_restore().await.unwrap().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn retired_generation_cancels_a_delayed_manifest_fetch_without_reinserting_its_lease() {
        let (_dir, store, archive) = fixture();
        let root_identity = CheckpointIdentity::new(
            "cluster-a",
            7,
            3,
            LogHash::digest(&[b"archive-test-config"]),
            3,
        );
        let root =
            ObjectArchiveStore::new_checkpoint_for_single_process(store, root_identity.clone());
        archive.publish_committed(&[entry()]).await.unwrap();
        root.publish_committed(&[entry()]).await.unwrap();
        let now = now_ms();
        root.set_gc_root(root_identity.clone(), now.saturating_sub(1))
            .await
            .unwrap();
        let plan = root
            .plan_gc(
                GcPolicy::new("actual-retirement-cancellation", root_identity, 0, 0, 0),
                now,
            )
            .await
            .unwrap();
        assert!(
            plan.swept_generations.contains(&identity()),
            "the actual GC plan must retire the restoring generation"
        );
        let (gate, entered, cancelled) =
            TestCheckpointManifestGate::new(archive.test_store_identity);
        let _installed = install_test_checkpoint_manifest_gate(gate.clone());
        let _release_on_unwind = gate.release_guard();
        let restore_archive = archive.clone();
        let restore = tokio::spawn(async move {
            restore_archive
                .load_checkpoint_restore_with_reader_lease_duration(TEST_READER_LEASE_MS)
                .await
        });
        wait_for_test_gate(
            entered,
            "retired-generation restore did not reach the manifest gate",
        )
        .await;

        // The exact manifest gate gives us a live reader lease before any
        // object read. Simulate its owner disappearing, then run the real GC
        // execution path. The following periodic renewal must see the actual
        // retirement and drop the gated read rather than reinserting a lease.
        let reader_lease_id = archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .iter_mut()
            .find(|lease| lease.kind == GcLeaseKind::Reader)
            .expect("paused restore must hold a reader lease")
            .lease_id
            .clone();
        archive.release_gc_lease(&reader_lease_id).await.unwrap();
        let (control_gate, control_entered) =
            TestGcControlGate::new(archive.test_store_identity, TestGcControlOperation::Load);
        let _installed_control = install_test_gc_control_gate(control_gate.clone());
        let control_release = control_gate.release_guard();
        tokio::time::advance(Duration::from_millis(
            TEST_READER_LEASE_MS / READER_LEASE_RENEW_DIVISOR,
        ))
        .await;
        wait_for_test_gate(
            control_entered,
            "retired-generation Reader did not reach the GC-control load gate",
        )
        .await;
        root.execute_gc(plan.plan_hash(), now_ms()).await.unwrap();
        drop(control_release);
        let result = restore.await.unwrap();
        assert!(
            matches!(result, Err(Error::GenerationRetired { .. })),
            "{result:?}"
        );
        wait_for_test_gate(
            cancelled,
            "generation retirement did not cancel the delayed manifest fetch",
        )
        .await;
        let control = archive.load_gc_control().await.unwrap();
        assert!(
            control.control.leases.is_empty(),
            "retired generation renewal must not leave or recreate a reader lease"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn failed_reader_renewal_cancels_the_delayed_fetch_before_it_can_complete() {
        let (_dir, _store, archive) = fixture();
        archive.publish_committed(&[entry()]).await.unwrap();
        let now = now_ms();
        archive.set_gc_root(identity(), now).await.unwrap();
        let plan = archive
            .plan_gc(
                GcPolicy::new("failed-delayed-reader", identity(), 0, 0, 0),
                now,
            )
            .await
            .unwrap();
        let object_key = archive
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .segments()[0]
            .object_key()
            .to_owned();
        let (gate, entered, cancelled) =
            TestCheckpointDownloadGate::new(archive.test_store_identity, object_key);
        let _installed = install_test_checkpoint_download_gate(gate.clone());
        let _release_on_unwind = gate.release_guard();
        let restore_archive = archive.clone();
        let restore = tokio::spawn(async move {
            restore_archive
                .load_checkpoint_restore_with_reader_lease_duration(TEST_READER_LEASE_MS)
                .await
        });
        wait_for_test_gate(
            entered,
            "delete-barrier restore did not reach the object gate",
        )
        .await;

        // Advance the control-plane timestamp beyond the short test lease,
        // enter delete, then let the next periodic renewal fail. The gated
        // object future must be dropped instead of completing after delete.
        let delete_now = now_ms().saturating_add(TEST_READER_LEASE_MS + 1);
        archive.acquire_gc_barrier(&plan, delete_now).await.unwrap();
        archive.enter_delete_phase(&plan, delete_now).await.unwrap();
        tokio::time::advance(Duration::from_millis(
            TEST_READER_LEASE_MS / READER_LEASE_RENEW_DIVISOR,
        ))
        .await;
        let result = restore.await.unwrap();
        assert!(
            matches!(result, Err(Error::GcBarrierActive { .. })),
            "{result:?}"
        );
        wait_for_test_gate(
            cancelled,
            "reader renewal failure did not cancel the delayed fetch",
        )
        .await;
        archive.abort_gc(&plan.operation_id).await.unwrap();
    }

    #[tokio::test]
    async fn coherent_restore_stops_when_reader_renewal_hits_a_delete_barrier() {
        let (_dir, _store, archive) = fixture();
        let now = now_ms();
        archive.publish_committed(&[entry()]).await.unwrap();
        archive.set_gc_root(identity(), now).await.unwrap();
        let plan = archive
            .plan_gc(GcPolicy::new("restore-renewal", identity(), 0, 0, 0), now)
            .await
            .unwrap();
        let reader = archive
            .acquire_operation_lease(GcLeaseKind::Reader, now, 1)
            .await
            .unwrap();
        archive.acquire_gc_barrier(&plan, now).await.unwrap();
        archive.enter_delete_phase(&plan, now + 2).await.unwrap();
        let loaded = archive.load_checkpoint_unleased().await.unwrap().unwrap();

        let result = archive
            .restore_loaded_checkpoint_unleased(&loaded, &reader.lease_id)
            .await;
        assert!(
            matches!(result, Err(Error::GcBarrierActive { .. })),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn initial_snapshot_can_seed_only_an_empty_checkpoint_namespace() {
        let (_dir, _store, archive) = fixture();
        let publisher = archive
            .open_checkpoint_publisher("successor", CheckpointPublisherOptions::default())
            .await
            .unwrap();
        let bytes = b"active successor snapshot";
        let compacted = LogAnchor::new(3, LogHash::digest(&[b"activate"]));
        let anchor = RecoveryAnchor::new(
            "cluster-a",
            7,
            ConfigurationState::active(3, identity().config_digest()),
            1,
            compacted,
            SnapshotIdentity::new(
                "successor-baseline",
                LogHash::digest(&[bytes]),
                bytes.len() as u64,
                LogHash::digest(&[b"executor"]),
            ),
        );

        assert!(publisher
            .publish_checkpoint_snapshot(anchor.clone(), bytes)
            .await
            .unwrap_err()
            .to_string()
            .contains("not an exact segment boundary"));
        let loaded = publisher
            .publish_initial_checkpoint_snapshot(anchor.clone(), bytes)
            .await
            .unwrap();
        assert_eq!(
            *loaded.manifest().tip(),
            CheckpointTip::new(3, compacted.hash())
        );
        assert!(loaded.manifest().segments().is_empty());
        let restored = archive.load_checkpoint_restore().await.unwrap().unwrap();
        assert_eq!(restored.restored().snapshot().unwrap().anchor(), &anchor);
        assert_eq!(restored.restored().snapshot().unwrap().bytes(), bytes);
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

    #[tokio::test]
    async fn gc_plan_rejects_oversized_root_manifest_before_parse_or_state_change() {
        let (_dir, store, archive, _loaded, _next, plan) = gc_race_fixture().await;
        let mut oversized =
            vec![b' '; usize::try_from(CHECKPOINT_RESTORE_LIMITS.manifest_encoded_bytes,).unwrap()];
        oversized.push(b' ');
        store
            .put(&plan.root_manifest_key, &oversized)
            .await
            .unwrap();
        let control_before = archive.load_gc_control().await.unwrap().control;
        let candidate_before = store.get(plan.candidates()[0].key()).await.unwrap();

        assert_eq!(
            archive
                .plan_gc(GcPolicy::new("oversized-root", identity(), 0, 0, 0), 112)
                .await
                .unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "manifest encoded bytes",
                object_key: Some(plan.root_manifest_key.clone()),
                limit: CHECKPOINT_RESTORE_LIMITS.manifest_encoded_bytes,
                actual: CHECKPOINT_RESTORE_LIMITS.manifest_encoded_bytes + 1,
            }
        );
        assert_eq!(
            archive.load_gc_control().await.unwrap().control,
            control_before
        );
        assert_eq!(
            store.get(plan.candidates()[0].key()).await.unwrap(),
            candidate_before
        );
        assert_eq!(store.get(&plan.root_manifest_key).await.unwrap(), oversized);
    }

    #[tokio::test]
    async fn gc_fence_rejects_oversized_replacement_before_cas_or_delete() {
        let (_dir, store, archive, _loaded, _next, plan) = gc_race_fixture().await;
        let mut oversized =
            vec![b' '; usize::try_from(CHECKPOINT_RESTORE_LIMITS.manifest_encoded_bytes,).unwrap()];
        oversized.push(b' ');
        store
            .put(&plan.root_manifest_key, &oversized)
            .await
            .unwrap();
        let root_before = store.get_versioned(&plan.root_manifest_key).await.unwrap();
        let control_before = archive.load_gc_control().await.unwrap().control;
        let candidate_before = store.get(plan.candidates()[0].key()).await.unwrap();

        assert_eq!(
            archive.fence_gc_root(&plan).await.unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "manifest encoded bytes",
                object_key: Some(plan.root_manifest_key.clone()),
                limit: CHECKPOINT_RESTORE_LIMITS.manifest_encoded_bytes,
                actual: CHECKPOINT_RESTORE_LIMITS.manifest_encoded_bytes + 1,
            }
        );
        let root_after = store.get_versioned(&plan.root_manifest_key).await.unwrap();
        assert_eq!(root_after.bytes(), root_before.bytes());
        assert_eq!(root_after.version(), root_before.version());
        assert_eq!(
            archive.load_gc_control().await.unwrap().control,
            control_before
        );
        assert_eq!(
            store.get(plan.candidates()[0].key()).await.unwrap(),
            candidate_before
        );
    }

    #[tokio::test]
    async fn checkpoint_manifest_read_accepts_exact_cap_and_rejects_one_byte_over() {
        let (_dir, store, archive) = fixture();
        let manifest = CheckpointManifest::new(identity());
        let mut bytes = serialize_json(&manifest).unwrap();
        bytes.resize(
            usize::try_from(CHECKPOINT_RESTORE_LIMITS.manifest_encoded_bytes).unwrap(),
            b' ',
        );
        let key = archive.checkpoint_manifest_key().unwrap();
        store.put(&key, &bytes).await.unwrap();
        assert_eq!(
            archive
                .load_checkpoint_unleased()
                .await
                .unwrap()
                .unwrap()
                .manifest(),
            &manifest
        );

        bytes.push(b' ');
        store.put(&key, &bytes).await.unwrap();
        assert_eq!(
            archive.load_checkpoint_unleased().await.unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "manifest encoded bytes",
                object_key: Some(key),
                limit: CHECKPOINT_RESTORE_LIMITS.manifest_encoded_bytes,
                actual: CHECKPOINT_RESTORE_LIMITS.manifest_encoded_bytes + 1,
            }
        );
    }

    #[tokio::test]
    async fn gc_control_read_accepts_exact_cap_and_rejects_declared_oversize() {
        let (_dir, store, archive) = fixture();
        archive.ensure_gc_control().await.unwrap();
        let key = archive.gc_control_key();
        let mut bytes = store.get(&key).await.unwrap();
        bytes.resize(usize::try_from(GC_CONTROL_ENCODED_BYTES).unwrap(), b' ');
        store.put(&key, &bytes).await.unwrap();
        assert_eq!(archive.load_gc_control().await.unwrap().control.fence, 0);

        bytes.push(b' ');
        store.put(&key, &bytes).await.unwrap();
        assert_eq!(
            archive.load_gc_control().await.err(),
            Some(Error::ObjectStore(ObjStoreError::ReadLimitExceeded {
                key,
                limit: GC_CONTROL_ENCODED_BYTES,
                actual: GC_CONTROL_ENCODED_BYTES + 1,
            }))
        );
    }

    #[tokio::test]
    async fn gc_control_write_rejects_an_unreadable_control_envelope() {
        let (_dir, store, archive) = fixture();
        archive.ensure_gc_control().await.unwrap();
        let mut loaded = archive.load_gc_control().await.unwrap();
        let key = archive.gc_control_key();
        let before = store.get_versioned(&key).await.unwrap();
        loaded.control.leases.push(GcLease {
            lease_id: "x".repeat(usize::try_from(GC_CONTROL_ENCODED_BYTES).unwrap()),
            kind: GcLeaseKind::Reader,
            fence: 0,
            expires_at_ms: 0,
        });

        assert!(matches!(
            archive.update_gc_control(&loaded).await,
            Err(Error::InvalidGc(message)) if message.starts_with("control encoded bytes exceed limit")
        ));
        let after = store.get_versioned(&key).await.unwrap();
        assert_eq!(after.bytes(), before.bytes());
        assert_eq!(after.version(), before.version());
    }

    #[tokio::test]
    async fn checkpoint_manifest_rejects_segment_and_object_counts_before_object_reads() {
        let (_dir, _store, archive) = fixture();
        let loaded = archive.publish_committed(&[entry()]).await.unwrap();

        let segment_limits = CheckpointRestoreLimits {
            segment_count: 0,
            ..CHECKPOINT_RESTORE_LIMITS
        };
        assert_eq!(
            archive
                .validate_checkpoint_manifest_with_limits(loaded.manifest(), segment_limits)
                .unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "segment count",
                object_key: None,
                limit: 0,
                actual: 1,
            }
        );

        let object_limits = CheckpointRestoreLimits {
            object_count: 0,
            ..CHECKPOINT_RESTORE_LIMITS
        };
        assert_eq!(
            archive
                .validate_checkpoint_manifest_with_limits(loaded.manifest(), object_limits)
                .unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "object count",
                object_key: None,
                limit: 0,
                actual: 1,
            }
        );
    }

    #[tokio::test]
    async fn successful_publication_receipt_survives_a_later_manifest_generation() {
        let (_dir, _store, archive) = fixture();
        let first = entry();
        let published = archive
            .publish_committed(std::slice::from_ref(&first))
            .await
            .unwrap();
        let encoded = published.publication_receipt().unwrap().encode().unwrap();
        let publication_receipt = CheckpointPublicationReceipt::decode(&encoded).unwrap();
        assert_eq!(publication_receipt.encode().unwrap(), encoded);
        let publisher = archive
            .open_checkpoint_publisher("receipt-test", CheckpointPublisherOptions::default())
            .await
            .unwrap();
        publisher
            .publish_receipt_for_loaded(&published)
            .await
            .unwrap();
        let receipt = archive
            .load_checkpoint_receipt(
                publisher.receipt_holder(),
                publication_receipt.gc_anchor().unwrap().manifest_digest(),
            )
            .await
            .unwrap()
            .gc_anchor()
            .unwrap();

        let second = next_entry(&first);
        let later = archive
            .publish_committed(std::slice::from_ref(&second))
            .await
            .unwrap();

        assert_eq!(
            receipt.cluster_id(),
            published.manifest().identity().cluster_id()
        );
        assert_eq!(receipt.epoch(), published.manifest().identity().epoch());
        assert_eq!(
            receipt.config_id(),
            published.manifest().identity().config_id()
        );
        assert_eq!(
            receipt.config_digest(),
            published.manifest().identity().config_digest()
        );
        assert_eq!(receipt.tip().index(), published.manifest().tip().index());
        assert_eq!(receipt.tip().hash(), published.manifest().tip().hash());
        assert_ne!(receipt.tip().index(), later.manifest().tip().index());
        assert_ne!(published.version(), later.version());
        assert!(matches!(
            archive.checkpoint_readback_gc_anchor(&published).await,
            Err(Error::InvalidCheckpoint(message))
                if message == "checkpoint changed before GC certificate readback"
        ));
    }

    #[tokio::test]
    async fn checkpoint_manifest_enforces_declared_object_and_aggregate_encoded_limits() {
        let (_dir, _store, archive) = fixture();
        let first = entry();
        archive
            .publish_committed(std::slice::from_ref(&first))
            .await
            .unwrap();
        let second = next_entry(&first);
        let loaded = archive
            .publish_committed(std::slice::from_ref(&second))
            .await
            .unwrap();
        let records = loaded.manifest().segments();
        let first_size = records[0].size_bytes();
        let aggregate = records
            .iter()
            .try_fold(0_u64, |total, record| {
                total.checked_add(record.size_bytes())
            })
            .unwrap();

        let object_limits = CheckpointRestoreLimits {
            object_encoded_bytes: first_size - 1,
            aggregate_encoded_bytes: u64::MAX,
            ..CHECKPOINT_RESTORE_LIMITS
        };
        assert_eq!(
            archive
                .validate_checkpoint_manifest_with_limits(loaded.manifest(), object_limits)
                .unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "object encoded bytes",
                object_key: Some(records[0].object_key().to_string()),
                limit: first_size - 1,
                actual: first_size,
            }
        );

        let aggregate_limits = CheckpointRestoreLimits {
            object_encoded_bytes: u64::MAX,
            aggregate_encoded_bytes: aggregate - 1,
            ..CHECKPOINT_RESTORE_LIMITS
        };
        assert_eq!(
            archive
                .validate_checkpoint_manifest_with_limits(loaded.manifest(), aggregate_limits)
                .unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "aggregate encoded bytes",
                object_key: None,
                limit: aggregate - 1,
                actual: aggregate,
            }
        );

        let exact_limits = CheckpointRestoreLimits {
            object_encoded_bytes: records
                .iter()
                .map(CheckpointSegmentRecord::size_bytes)
                .max()
                .unwrap(),
            aggregate_encoded_bytes: aggregate,
            ..CHECKPOINT_RESTORE_LIMITS
        };
        archive
            .validate_checkpoint_manifest_with_limits(loaded.manifest(), exact_limits)
            .unwrap();
    }

    #[tokio::test]
    async fn checkpoint_manifest_uses_checked_aggregate_arithmetic() {
        let (_dir, _store, archive) = fixture();
        let first = entry();
        archive
            .publish_committed(std::slice::from_ref(&first))
            .await
            .unwrap();
        let second = next_entry(&first);
        let loaded = archive
            .publish_committed(std::slice::from_ref(&second))
            .await
            .unwrap();
        let mut manifest = loaded.manifest().clone();
        manifest.segments[0].size_bytes = u64::MAX;
        let limits = CheckpointRestoreLimits {
            object_encoded_bytes: u64::MAX,
            aggregate_encoded_bytes: u64::MAX,
            ..CHECKPOINT_RESTORE_LIMITS
        };

        assert_eq!(
            archive
                .validate_checkpoint_manifest_with_limits(&manifest, limits)
                .unwrap_err(),
            Error::RestoreSizeOverflow {
                resource: "aggregate encoded bytes",
            }
        );
    }

    #[test]
    fn checkpoint_decoded_budget_enforces_object_and_multi_object_aggregate_boundaries() {
        let manifest = CheckpointManifest::new(identity());
        let limits = CheckpointRestoreLimits {
            object_decoded_bytes: 6,
            aggregate_decoded_bytes: 10,
            ..CHECKPOINT_RESTORE_LIMITS
        };
        let mut budget = CheckpointRestoreBudget::new(&manifest, limits).unwrap();
        assert_eq!(budget.next_object_limit().unwrap(), 6);
        budget.charge("one", 6).unwrap();
        assert_eq!(budget.next_object_limit().unwrap(), 4);
        budget.charge("two", 4).unwrap();
        assert_eq!(budget.next_object_limit().unwrap(), 0);
        assert_eq!(
            budget.charge("three", 1).unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "aggregate decoded bytes",
                object_key: None,
                limit: 10,
                actual: 11,
            }
        );
    }

    #[test]
    fn restored_suffix_allocation_rejects_allocator_overcapacity_and_count_overflow() {
        let shape = CheckpointSuffixShape {
            entry_count: 1,
            stable_outer_bytes: CHECKPOINT_RESTORED_ENTRY_OVERHEAD_BUDGET_BYTES,
        };
        let oversized_capacity = usize::try_from(shape.stable_outer_bytes).unwrap()
            / std::mem::size_of::<LogEntry>()
            + 1;
        let oversized_bytes = restored_suffix_outer_bytes(oversized_capacity).unwrap();
        assert_eq!(
            validate_restored_suffix_allocation(oversized_capacity, 0, oversized_capacity, shape,)
                .unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "restored suffix container bytes",
                object_key: None,
                limit: shape.stable_outer_bytes,
                actual: oversized_bytes,
            }
        );

        let mut manifest = CheckpointManifest::new(identity());
        manifest.segments = vec![
            CheckpointSegmentRecord {
                format_version: CHECKPOINT_SEGMENT_FORMAT_VERSION,
                start_index: 0,
                end_index: u64::MAX - 1,
                first_prev_hash: LogHash::ZERO,
                last_hash: LogHash::ZERO,
                object_key: "one".into(),
                sha256: LogHash::ZERO.to_hex(),
                size_bytes: 1,
                effects: Vec::new(),
            },
            CheckpointSegmentRecord {
                format_version: CHECKPOINT_SEGMENT_FORMAT_VERSION,
                start_index: 0,
                end_index: 0,
                first_prev_hash: LogHash::ZERO,
                last_hash: LogHash::ZERO,
                object_key: "two".into(),
                sha256: LogHash::ZERO.to_hex(),
                size_bytes: 1,
                effects: Vec::new(),
            },
        ];
        assert_eq!(
            checked_checkpoint_suffix_entry_count(&manifest).unwrap_err(),
            Error::RestoreSizeOverflow {
                resource: "checkpoint suffix entry count",
            }
        );
    }

    #[tokio::test]
    async fn multi_segment_restore_accounts_for_the_final_suffix_container_on_both_paths() {
        let (_directory, _store, archive) = fixture();
        let first = entry();
        archive
            .publish_committed(std::slice::from_ref(&first))
            .await
            .unwrap();
        let second = next_entry(&first);
        let loaded = archive
            .publish_committed(std::slice::from_ref(&second))
            .await
            .unwrap();
        assert_eq!(loaded.manifest().segments().len(), 2);
        let stable_aggregate = archive
            .checkpoint_declared_decoded_budget(
                loaded.manifest(),
                CheckpointRestoreLimits {
                    object_decoded_bytes: u64::MAX,
                    aggregate_decoded_bytes: u64::MAX,
                    ..CHECKPOINT_RESTORE_LIMITS
                },
            )
            .unwrap()
            .decoded_bytes();
        let exact_limits = CheckpointRestoreLimits {
            object_decoded_bytes: u64::MAX,
            aggregate_decoded_bytes: stable_aggregate,
            ..CHECKPOINT_RESTORE_LIMITS
        };
        let lower_limits = CheckpointRestoreLimits {
            aggregate_decoded_bytes: stable_aggregate - 1,
            ..exact_limits
        };

        let reader = archive
            .acquire_operation_lease(GcLeaseKind::Reader, now_ms(), DEFAULT_LEASE_MS)
            .await
            .unwrap();
        let ordinary = archive
            .restore_loaded_checkpoint_with_reader_lease_duration_and_limits_unleased(
                &loaded,
                &reader.lease_id,
                DEFAULT_LEASE_MS,
                exact_limits,
            )
            .await
            .unwrap();
        assert_eq!(ordinary.suffix(), &[first.clone(), second.clone()]);
        assert_eq!(
            archive
                .restore_loaded_checkpoint_with_reader_lease_duration_and_limits_unleased(
                    &loaded,
                    &reader.lease_id,
                    DEFAULT_LEASE_MS,
                    lower_limits,
                )
                .await
                .unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "aggregate decoded bytes",
                object_key: None,
                limit: stable_aggregate - 1,
                actual: stable_aggregate,
            }
        );
        archive.release_gc_lease(&reader.lease_id).await.unwrap();

        let active = archive
            .restore_loaded_checkpoint_with_active_reader_lease_and_limits(&loaded, exact_limits)
            .await
            .unwrap();
        assert_eq!(active.suffix(), &[first, second]);
        assert_eq!(
            archive
                .restore_loaded_checkpoint_with_active_reader_lease_and_limits(
                    &loaded,
                    lower_limits,
                )
                .await
                .unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "aggregate decoded bytes",
                object_key: None,
                limit: stable_aggregate - 1,
                actual: stable_aggregate,
            }
        );
    }

    #[test]
    fn checkpoint_segment_decode_rejects_compact_repeated_identity_expansion() {
        let directory = tempfile::tempdir().unwrap();
        let store = ObjStore::new(ObjStoreConfig::Local {
            root: directory.path().to_path_buf(),
        })
        .unwrap();
        let cluster_id = "c".repeat(200);
        let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
            store,
            CheckpointIdentity::new(
                &cluster_id,
                7,
                3,
                LogHash::digest(&[b"archive-test-config"]),
                1,
            ),
        );
        let payload = b"small".to_vec();
        let hash = LogEntry::calculate_hash(
            &cluster_id,
            1,
            7,
            3,
            EntryType::Command,
            LogHash::ZERO,
            &payload,
        );
        let entry = LogEntry {
            cluster_id: cluster_id.clone(),
            epoch: 7,
            config_id: 3,
            index: 1,
            entry_type: EntryType::Command,
            payload,
            prev_hash: LogHash::ZERO,
            hash,
        };
        let bytes = encode_segment(std::slice::from_ref(&entry));
        let record = archive
            .checkpoint_segment_record(std::slice::from_ref(&entry), &bytes)
            .unwrap();
        let (decoded, charged) = archive
            .decode_checkpoint_segment_bounded(&record, &bytes, u64::MAX, "object decoded bytes")
            .unwrap();
        assert_eq!(decoded, vec![entry.clone()]);
        let retained_bytes = decoded.capacity() * std::mem::size_of::<LogEntry>()
            + decoded
                .iter()
                .map(|entry| entry.cluster_id.capacity() + entry.payload.capacity())
                .sum::<usize>();
        assert!(charged >= u64::try_from(retained_bytes).unwrap());
        let conservative_charge = archive
            .checkpoint_segment_decoded_upper_bound(&record)
            .unwrap();
        assert!(charged <= conservative_charge);

        let (exact, exact_charge) = archive
            .decode_checkpoint_segment_bounded(
                &record,
                &bytes,
                conservative_charge,
                "object decoded bytes",
            )
            .unwrap();
        assert_eq!(exact, vec![entry]);
        assert_eq!(exact_charge, charged);
        assert_eq!(
            archive
                .decode_checkpoint_segment_bounded(
                    &record,
                    &bytes,
                    conservative_charge - 1,
                    "object decoded bytes",
                )
                .unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "object decoded bytes",
                object_key: Some(record.object_key().to_string()),
                limit: conservative_charge - 1,
                actual: conservative_charge,
            }
        );
    }

    #[tokio::test]
    async fn checkpoint_publisher_rejects_decoded_upper_bound_before_object_visibility() {
        let directory = tempfile::tempdir().unwrap();
        let store = ObjStore::new(ObjStoreConfig::Local {
            root: directory.path().to_path_buf(),
        })
        .unwrap();
        let cluster_id = "c".repeat(200);
        let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
            store.clone(),
            CheckpointIdentity::new(
                &cluster_id,
                7,
                3,
                LogHash::digest(&[b"archive-test-config"]),
                1,
            ),
        );
        let loaded = archive.initialize_checkpoint().await.unwrap();
        let payload = b"compact".to_vec();
        let hash = LogEntry::calculate_hash(
            &cluster_id,
            1,
            7,
            3,
            EntryType::Command,
            LogHash::ZERO,
            &payload,
        );
        let entry = LogEntry {
            cluster_id,
            epoch: 7,
            config_id: 3,
            index: 1,
            entry_type: EntryType::Command,
            payload,
            prev_hash: LogHash::ZERO,
            hash,
        };
        let bytes = encode_segment(std::slice::from_ref(&entry));
        let record = archive
            .checkpoint_segment_record(std::slice::from_ref(&entry), &bytes)
            .unwrap();
        let conservative_charge = archive
            .checkpoint_segment_decoded_upper_bound(&record)
            .unwrap();
        let stable_aggregate = conservative_charge
            .checked_add(CHECKPOINT_RESTORED_ENTRY_OVERHEAD_BUDGET_BYTES)
            .unwrap();
        let manifest_key = archive.checkpoint_manifest_key().unwrap();
        let manifest_before = store.get_versioned(&manifest_key).await.unwrap();
        let control_before = store
            .get_versioned(&archive.gc_control_key())
            .await
            .unwrap();
        let rejected_limits = CheckpointRestoreLimits {
            object_decoded_bytes: conservative_charge - 1,
            aggregate_decoded_bytes: stable_aggregate,
            ..CHECKPOINT_RESTORE_LIMITS
        };

        assert_eq!(
            archive
                .publish_committed_with_limits(std::slice::from_ref(&entry), rejected_limits)
                .await
                .unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "object decoded bytes",
                object_key: Some(record.object_key().to_string()),
                limit: conservative_charge - 1,
                actual: conservative_charge,
            }
        );
        assert!(matches!(
            store.get(record.object_key()).await,
            Err(ObjStoreError::NotFound { .. })
        ));
        let manifest_after = store.get_versioned(&manifest_key).await.unwrap();
        assert_eq!(manifest_after.bytes(), manifest_before.bytes());
        assert_eq!(manifest_after.version(), manifest_before.version());
        let control_after = store
            .get_versioned(&archive.gc_control_key())
            .await
            .unwrap();
        assert_eq!(control_after.bytes(), control_before.bytes());
        assert_eq!(control_after.version(), control_before.version());

        let exact_limits = CheckpointRestoreLimits {
            object_encoded_bytes: u64::try_from(bytes.len()).unwrap(),
            aggregate_encoded_bytes: u64::try_from(bytes.len()).unwrap(),
            object_decoded_bytes: conservative_charge,
            aggregate_decoded_bytes: stable_aggregate,
            ..CHECKPOINT_RESTORE_LIMITS
        };
        let published = archive
            .publish_committed_with_limits(std::slice::from_ref(&entry), exact_limits)
            .await
            .unwrap();
        let published_record = &published.manifest().segments()[0];
        let downloaded = archive
            .download_verified_with_limits(
                published_record.object_key(),
                published_record.size_bytes(),
                published_record.sha256(),
                exact_limits,
            )
            .await
            .unwrap();
        let (decoded, actual_charge) = archive
            .decode_checkpoint_segment_bounded(
                published_record,
                &downloaded,
                conservative_charge,
                "object decoded bytes",
            )
            .unwrap();
        assert_eq!(decoded, vec![entry]);
        assert!(actual_charge <= conservative_charge);
        assert_eq!(loaded.manifest().segments().len(), 0);
    }

    #[tokio::test]
    async fn checkpoint_snapshot_over_limit_does_not_touch_publisher_control() {
        let (_directory, store, archive) = fixture();
        archive.initialize_checkpoint().await.unwrap();
        let snapshot_bytes = b"oversized local snapshot candidate";
        let anchor = RecoveryAnchor::new(
            "cluster-a",
            7,
            ConfigurationState::active(3, identity().config_digest()),
            1,
            LogAnchor::new(3, LogHash::digest(&[b"tip"])),
            SnapshotIdentity::new(
                "snapshot-over-limit",
                LogHash::digest(&[snapshot_bytes]),
                u64::try_from(snapshot_bytes.len()).unwrap(),
                LogHash::digest(&[b"executor"]),
            ),
        );
        let snapshot_key = checkpoint_snapshot_key(archive.checkpoint_identity().unwrap(), &anchor);
        let manifest_key = archive.checkpoint_manifest_key().unwrap();
        let manifest_before = store.get_versioned(&manifest_key).await.unwrap();
        let control_before = store
            .get_versioned(&archive.gc_control_key())
            .await
            .unwrap();
        let limit = u64::try_from(snapshot_bytes.len()).unwrap() - 1;

        assert_eq!(
            archive
                .publish_checkpoint_snapshot_with_limits(
                    anchor,
                    snapshot_bytes,
                    CheckpointRestoreLimits {
                        object_encoded_bytes: limit,
                        ..CHECKPOINT_RESTORE_LIMITS
                    },
                )
                .await
                .unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "object encoded bytes",
                object_key: Some(snapshot_key.clone()),
                limit,
                actual: u64::try_from(snapshot_bytes.len()).unwrap(),
            }
        );
        assert!(matches!(
            store.get(&snapshot_key).await,
            Err(ObjStoreError::NotFound { .. })
        ));
        let manifest_after = store.get_versioned(&manifest_key).await.unwrap();
        assert_eq!(manifest_after.bytes(), manifest_before.bytes());
        assert_eq!(manifest_after.version(), manifest_before.version());
        let control_after = store
            .get_versioned(&archive.gc_control_key())
            .await
            .unwrap();
        assert_eq!(control_after.bytes(), control_before.bytes());
        assert_eq!(control_after.version(), control_before.version());
    }

    #[tokio::test]
    async fn checkpoint_overlap_over_limit_is_pure_before_publisher_lease() {
        let (_directory, store, archive) = fixture();
        let first = entry();
        let first_loaded = archive
            .publish_committed(std::slice::from_ref(&first))
            .await
            .unwrap();
        let first_key = first_loaded.manifest().segments()[0]
            .object_key()
            .to_string();
        let second = next_entry(&first);
        let second_bytes = encode_segment(std::slice::from_ref(&second));
        let second_key = checkpoint_segment_key(
            archive.checkpoint_identity().unwrap(),
            second.index,
            second.index,
        );
        let encoded_limit = u64::try_from(second_bytes.len()).unwrap() - 1;
        let manifest_key = archive.checkpoint_manifest_key().unwrap();
        let manifest_before = store.get_versioned(&manifest_key).await.unwrap();
        let control_before = store
            .get_versioned(&archive.gc_control_key())
            .await
            .unwrap();
        let (gate, entered, _cancelled) =
            TestCheckpointDownloadGate::new(archive.test_store_identity, first_key);
        let _installed = install_test_checkpoint_download_gate(gate.clone());
        let release = gate.release_guard();

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            archive.publish_committed_with_limits(
                &[first, second],
                CheckpointRestoreLimits {
                    object_encoded_bytes: encoded_limit,
                    ..CHECKPOINT_RESTORE_LIMITS
                },
            ),
        )
        .await
        .expect("local over-limit rejection must not await the archived overlap proof");
        assert_eq!(
            result.unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "object encoded bytes",
                object_key: Some(second_key.clone()),
                limit: encoded_limit,
                actual: u64::try_from(second_bytes.len()).unwrap(),
            }
        );
        assert!(matches!(
            entered.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        assert!(matches!(
            store.get(&second_key).await,
            Err(ObjStoreError::NotFound { .. })
        ));
        let manifest_after = store.get_versioned(&manifest_key).await.unwrap();
        assert_eq!(manifest_after.bytes(), manifest_before.bytes());
        assert_eq!(manifest_after.version(), manifest_before.version());
        let control_after = store
            .get_versioned(&archive.gc_control_key())
            .await
            .unwrap();
        assert_eq!(control_after.bytes(), control_before.bytes());
        assert_eq!(control_after.version(), control_before.version());
        drop(release);
    }

    #[tokio::test]
    async fn stale_cas_reprepare_rejects_newer_over_limit_manifest_before_renewal() {
        let (_directory, store, archive) = fixture();
        let first = entry();
        let stale = archive
            .publish_committed(std::slice::from_ref(&first))
            .await
            .unwrap();
        let second = next_entry(&first);
        let second_bytes = encode_segment(std::slice::from_ref(&second));
        let second_record = archive
            .checkpoint_segment_record(std::slice::from_ref(&second), &second_bytes)
            .unwrap();
        let object_encoded_limit = stale.manifest().segments()[0]
            .size_bytes()
            .max(second_record.size_bytes());
        let snapshot_bytes =
            vec![b'x'; usize::try_from(object_encoded_limit.checked_add(1).unwrap()).unwrap()];
        let anchor = RecoveryAnchor::new(
            "cluster-a",
            7,
            ConfigurationState::active(3, identity().config_digest()),
            1,
            LogAnchor::new(first.index, first.hash),
            SnapshotIdentity::new(
                "newer-over-limit",
                LogHash::digest(&[snapshot_bytes.as_slice()]),
                u64::try_from(snapshot_bytes.len()).unwrap(),
                LogHash::digest(&[b"executor"]),
            ),
        );
        let snapshot_key = checkpoint_snapshot_key(archive.checkpoint_identity().unwrap(), &anchor);
        let prepared_snapshot = archive
            .prepare_checkpoint_snapshot_candidate(
                &anchor,
                &snapshot_bytes,
                stale.manifest(),
                CheckpointSnapshotPublicationPolicy {
                    allow_empty_baseline: false,
                    limits: CHECKPOINT_RESTORE_LIMITS,
                },
            )
            .unwrap();
        let mut newer = prepared_snapshot.next.unwrap();
        newer.tip = CheckpointTip::new(second.index, second.hash);
        newer.segments.push(second_record.clone());
        archive.validate_checkpoint_manifest(&newer).unwrap();
        store.create(&snapshot_key, &snapshot_bytes).await.unwrap();
        store
            .create(second_record.object_key(), &second_bytes)
            .await
            .unwrap();
        let manifest_key = archive.checkpoint_manifest_key().unwrap();
        store
            .update(
                &manifest_key,
                serialize_checkpoint_manifest(&newer).unwrap(),
                stale.version().clone(),
            )
            .await
            .unwrap();

        let lease = archive
            .acquire_operation_lease(GcLeaseKind::Publisher, now_ms(), DEFAULT_LEASE_MS)
            .await
            .unwrap();
        let lease_id = lease.lease_id;
        let (manifest_gate, manifest_entered, _manifest_cancelled) =
            TestCheckpointManifestGate::new(archive.test_store_identity);
        let _installed_manifest = install_test_checkpoint_manifest_gate(manifest_gate.clone());
        let manifest_release = manifest_gate.release_guard();
        let publish_archive = archive.clone();
        let publish_lease_id = lease_id.clone();
        let publish_second = second.clone();
        let limits = CheckpointRestoreLimits {
            object_encoded_bytes: object_encoded_limit,
            ..CHECKPOINT_RESTORE_LIMITS
        };
        let publish = tokio::spawn(async move {
            publish_archive
                .publish_committed_from_loaded_unleased_with_limits(
                    std::slice::from_ref(&publish_second),
                    &publish_lease_id,
                    DEFAULT_LEASE_MS,
                    stale,
                    limits,
                )
                .await
        });
        wait_for_test_gate(
            manifest_entered,
            "stale checkpoint CAS did not reach exact-manifest reload",
        )
        .await;

        // Reaching the reload gate proves that the first attempt already
        // completed exact AlreadyExists verification, its post-object strict
        // renewal, and the stale CAS. Install the object gate only now so it
        // observes the retry and cannot be satisfied by first-attempt work.
        let (segment_gate, segment_entered, _segment_cancelled) = TestCheckpointDownloadGate::new(
            archive.test_store_identity,
            second_record.object_key(),
        );
        let _installed_segment = install_test_checkpoint_download_gate(segment_gate.clone());
        let segment_release = segment_gate.release_guard();
        // Everything captured here must remain byte/version exact when the
        // reloaded manifest fails pure preparation on the next loop.
        let control_before = store
            .get_versioned(&archive.gc_control_key())
            .await
            .unwrap();
        let manifest_before = store.get_versioned(&manifest_key).await.unwrap();
        let segment_before = store
            .get_versioned(second_record.object_key())
            .await
            .unwrap();
        drop(manifest_release);

        let publish = tokio::time::timeout(Duration::from_secs(2), publish)
            .await
            .expect("newer over-limit retry must not deadlock")
            .unwrap();
        assert_eq!(
            publish.unwrap_err(),
            Error::RestoreLimitExceeded {
                resource: "object encoded bytes",
                object_key: Some(snapshot_key),
                limit: object_encoded_limit,
                actual: u64::try_from(snapshot_bytes.len()).unwrap(),
            }
        );
        assert!(matches!(
            segment_entered.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        let control_after = store
            .get_versioned(&archive.gc_control_key())
            .await
            .unwrap();
        assert_eq!(control_after.bytes(), control_before.bytes());
        assert_eq!(control_after.version(), control_before.version());
        let manifest_after = store.get_versioned(&manifest_key).await.unwrap();
        assert_eq!(manifest_after.bytes(), manifest_before.bytes());
        assert_eq!(manifest_after.version(), manifest_before.version());
        let segment_after = store
            .get_versioned(second_record.object_key())
            .await
            .unwrap();
        assert_eq!(segment_after.bytes(), segment_before.bytes());
        assert_eq!(segment_after.version(), segment_before.version());
        drop(segment_release);
        archive.release_gc_lease(&lease_id).await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_append_does_not_download_retained_segments() {
        let (_directory, _store, archive) = fixture();
        let first = entry();
        let first_loaded = archive
            .publish_committed(std::slice::from_ref(&first))
            .await
            .unwrap();
        let first_key = first_loaded.manifest().segments()[0]
            .object_key()
            .to_string();
        let (gate, entered, _cancelled) =
            TestCheckpointDownloadGate::new(archive.test_store_identity, first_key);
        let installed = install_test_checkpoint_download_gate(gate.clone());
        let release = gate.release_guard();

        let second = next_entry(&first);
        let published = tokio::time::timeout(
            Duration::from_secs(2),
            archive.publish_committed(std::slice::from_ref(&second)),
        )
        .await
        .expect("append must not wait on a retained-segment object read")
        .unwrap();
        assert_eq!(published.manifest().tip().index(), second.index);
        assert!(matches!(
            entered.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        drop(release);
        drop(installed);
    }

    #[tokio::test]
    async fn overlap_proof_holds_publisher_lease_across_snapshot_trim_and_gc() {
        let (_directory, store, archive) = fixture();
        let first = entry();
        let first_loaded = archive
            .publish_committed(std::slice::from_ref(&first))
            .await
            .unwrap();
        let first_key = first_loaded.manifest().segments()[0]
            .object_key()
            .to_string();
        archive.set_gc_root(identity(), now_ms()).await.unwrap();

        let second = next_entry(&first);
        let (gate, entered, _cancelled) =
            TestCheckpointDownloadGate::new(archive.test_store_identity, first_key.clone());
        let _installed = install_test_checkpoint_download_gate(gate.clone());
        let release = gate.release_guard();
        let append_archive = archive.clone();
        let append_first = first.clone();
        let append_second = second.clone();
        let append = tokio::spawn(async move {
            append_archive
                .publish_committed(&[append_first, append_second])
                .await
        });
        wait_for_test_gate(
            entered,
            "overlap proof did not reach the archived segment read",
        )
        .await;

        let proof_now = now_ms();
        let control = archive.load_gc_control().await.unwrap();
        assert!(control.control.leases.iter().any(|lease| {
            lease.kind == GcLeaseKind::Publisher && lease.expires_at_ms > proof_now
        }));

        let snapshot_bytes = b"trimmed overlap base";
        let anchor = RecoveryAnchor::new(
            "cluster-a",
            7,
            ConfigurationState::active(3, identity().config_digest()),
            1,
            LogAnchor::new(first.index, first.hash),
            SnapshotIdentity::new(
                "overlap-trim",
                LogHash::digest(&[snapshot_bytes]),
                u64::try_from(snapshot_bytes.len()).unwrap(),
                LogHash::digest(&[b"executor"]),
            ),
        );
        let trimmed = archive
            .publish_checkpoint_snapshot(anchor.clone(), snapshot_bytes)
            .await
            .unwrap();
        assert_eq!(trimmed.manifest().base().tip().index(), first.index);
        assert!(trimmed.manifest().segments().is_empty());

        let gc_now = now_ms();
        let blocked_plan = archive
            .plan_gc(
                GcPolicy::new("overlap-proof-pending", identity(), 0, 0, 0),
                gc_now,
            )
            .await
            .unwrap();
        assert!(blocked_plan
            .candidates()
            .iter()
            .any(|candidate| candidate.key() == first_key));
        let delete = archive
            .execute_gc(blocked_plan.plan_hash(), blocked_plan.not_before_ms)
            .await;
        assert!(
            matches!(delete, Err(Error::GcBarrierBusy { .. })),
            "{delete:?}"
        );
        assert_eq!(
            store.get(&first_key).await.unwrap(),
            encode_segment(std::slice::from_ref(&first))
        );

        drop(release);
        let published = append.await.unwrap().unwrap();
        assert_eq!(published.manifest().base().tip().index(), first.index);
        assert_eq!(published.manifest().segments().len(), 1);
        assert_eq!(
            published.manifest().segments()[0].start_index(),
            second.index
        );
        assert_eq!(published.manifest().segments()[0].end_index(), second.index);
        assert_eq!(
            *published.manifest().tip(),
            CheckpointTip::new(second.index, second.hash)
        );

        assert!(!archive
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .iter()
            .any(|lease| lease.kind == GcLeaseKind::Publisher));
        archive.abort_gc("overlap-proof-pending").await.unwrap();

        let replayed = archive
            .publish_committed(&[first.clone(), second.clone()])
            .await
            .unwrap();
        assert_eq!(replayed, published);
        let restored = archive.load_checkpoint_restore().await.unwrap().unwrap();
        assert_eq!(restored.restored().snapshot().unwrap().anchor(), &anchor);
        assert_eq!(
            restored.restored().snapshot().unwrap().bytes(),
            snapshot_bytes
        );
        assert_eq!(restored.restored().suffix(), std::slice::from_ref(&second));

        let final_gc_now = now_ms();
        let final_plan = archive
            .plan_gc(
                GcPolicy::new("overlap-proof-complete", identity(), 0, 0, 0),
                final_gc_now,
            )
            .await
            .unwrap();
        assert!(final_plan
            .candidates()
            .iter()
            .any(|candidate| candidate.key() == first_key));
        let report = archive
            .execute_gc(final_plan.plan_hash(), final_plan.not_before_ms)
            .await
            .unwrap();
        assert!(report.results().iter().any(|evidence| {
            evidence.key == first_key && evidence.outcome == GcDeleteOutcome::Deleted
        }));
        assert!(matches!(
            store.get(&first_key).await,
            Err(ObjStoreError::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn legacy_publishers_match_their_bounded_reader_limits() {
        let directory = tempfile::tempdir().unwrap();
        let store = ObjStore::new(ObjStoreConfig::Local {
            root: directory.path().to_path_buf(),
        })
        .unwrap();
        let archive = ObjectArchiveStore::new_for_single_process(store.clone(), "cluster-a");
        let segment = SegmentFile::new(
            rhiza_log::IndexRange::new(1, 1).unwrap(),
            b"segment".to_vec(),
        );
        let segment_size = u64::try_from(segment.bytes().len()).unwrap();
        let segment_limits = CheckpointRestoreLimits {
            object_encoded_bytes: segment_size,
            ..CHECKPOINT_RESTORE_LIMITS
        };
        let record = archive
            .publish_segment_with_limits(7, &segment, segment_limits)
            .await
            .unwrap();
        assert_eq!(
            archive
                .download_verified_with_limits(
                    record.object_key(),
                    record.size_bytes(),
                    record.sha256(),
                    segment_limits,
                )
                .await
                .unwrap(),
            segment.bytes()
        );

        let rejected = SegmentFile::new(
            rhiza_log::IndexRange::new(2, 2).unwrap(),
            b"too-large".to_vec(),
        );
        let rejected_key = format!(
            "rhiza/cluster-a/archive/segments/epoch-{epoch:020}/{start:020}-{end:020}.qlog",
            epoch = 7,
            start = 2,
            end = 2,
        );
        assert!(matches!(
            archive
                .publish_segment_with_limits(7, &rejected, segment_limits)
                .await,
            Err(Error::RestoreLimitExceeded {
                resource: "object encoded bytes",
                ..
            })
        ));
        assert!(matches!(
            store.get(&rejected_key).await,
            Err(ObjStoreError::NotFound { .. })
        ));

        let snapshot_bytes = b"snapshot".to_vec();
        let snapshot = Snapshot::new(
            SnapshotManifest::new(
                "cluster-a",
                ConfigurationState::active(1, LogHash::ZERO),
                7,
                1,
                LogHash::ZERO,
                1,
                "node-a",
                LogHash::from_bytes([7; 32]),
            ),
            snapshot_bytes.clone(),
        );
        let snapshot_size = u64::try_from(snapshot_bytes.len()).unwrap();
        let snapshot_limits = CheckpointRestoreLimits {
            object_encoded_bytes: snapshot_size,
            object_decoded_bytes: snapshot_size,
            ..CHECKPOINT_RESTORE_LIMITS
        };
        let snapshot_record = archive
            .publish_snapshot_with_limits(&snapshot, snapshot_limits)
            .await
            .unwrap();
        assert_eq!(
            archive
                .download_verified_with_limits(
                    snapshot_record.object_key(),
                    snapshot_record.size_bytes(),
                    snapshot_record.sha256(),
                    snapshot_limits,
                )
                .await
                .unwrap(),
            snapshot_bytes
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

    #[tokio::test(start_paused = true)]
    async fn recovery_roll_does_not_revive_an_expired_source_reader() {
        let (_source_directory, source_store, source) = fixture();
        let source_checkpoint = source.publish_committed(&[entry()]).await.unwrap();
        let source_root_identity = CheckpointIdentity::new(
            "cluster-a",
            7,
            3,
            LogHash::digest(&[b"archive-test-config"]),
            2,
        );
        let source_root = ObjectArchiveStore::new_checkpoint_for_single_process(
            source_store.clone(),
            source_root_identity.clone(),
        );
        source_root.initialize_checkpoint().await.unwrap();
        source_root
            .set_gc_root(source_root_identity.clone(), now_ms())
            .await
            .unwrap();

        let target_directory = tempfile::tempdir().unwrap();
        let target_store = ObjStore::new(ObjStoreConfig::Local {
            root: target_directory.path().to_path_buf(),
        })
        .unwrap();
        let target = ObjectArchiveStore::new_checkpoint_for_single_process(
            target_store,
            CheckpointIdentity::new(
                "cluster-a",
                7,
                3,
                LogHash::digest(&[b"archive-test-config"]),
                2,
            ),
        );
        let source_segment_key = source_checkpoint.manifest().segments()[0]
            .object_key()
            .to_owned();
        let (gate, entered, cancelled) =
            TestCheckpointDownloadGate::new(source.test_store_identity, source_segment_key);
        let _installed = install_test_checkpoint_download_gate(gate.clone());
        let _release_on_unwind = gate.release_guard();
        let copy_source = source.clone();
        let copy_target = target.clone();
        let copy =
            tokio::spawn(async move { copy_source.roll_recovery_generation(&copy_target).await });
        wait_for_test_gate(
            entered,
            "recovery roll did not reach its source object read",
        )
        .await;

        let mut control = source.load_gc_control().await.unwrap();
        let source_lease = control
            .control
            .leases
            .iter_mut()
            .find(|lease| lease.kind == GcLeaseKind::Reader)
            .expect("paused copy must hold its source Reader lease");
        source_lease.expires_at_ms = 0;
        source.update_gc_control(&control).await.unwrap();
        let plan = source_root
            .plan_gc(
                GcPolicy::new("copy-source-expired", source_root_identity, 0, 0, 0),
                now_ms(),
            )
            .await
            .unwrap();
        source_root
            .execute_gc(plan.plan_hash(), now_ms())
            .await
            .unwrap();

        tokio::time::advance(Duration::from_millis(
            DEFAULT_LEASE_MS / READER_LEASE_RENEW_DIVISOR,
        ))
        .await;
        let result = copy.await.unwrap();
        assert!(
            matches!(result, Err(Error::GenerationRetired { generation: 1, .. })),
            "source Reader retirement must abort the copy without recreation: {result:?}"
        );
        wait_for_test_gate(
            cancelled,
            "strict source renewal did not cancel the paused source object read",
        )
        .await;
        assert!(!source
            .load_gc_control()
            .await
            .unwrap()
            .control
            .leases
            .iter()
            .any(|lease| lease.kind == GcLeaseKind::Reader));
        assert!(target.load_checkpoint_unleased().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn recovery_roll_aborts_when_target_gc_retires_a_copied_object_mid_copy() {
        let (_source_directory, _source_store, source) = fixture();
        let first = entry();
        let second = next_entry(&first);
        let source_checkpoint = source
            .publish_committed(&[first.clone(), second])
            .await
            .unwrap();

        let target_directory = tempfile::tempdir().unwrap();
        let target_store = ObjStore::new(ObjStoreConfig::Local {
            root: target_directory.path().to_path_buf(),
        })
        .unwrap();
        let target_identity = CheckpointIdentity::new(
            "cluster-a",
            7,
            3,
            LogHash::digest(&[b"archive-test-config"]),
            2,
        );
        let target = ObjectArchiveStore::new_checkpoint_for_single_process(
            target_store.clone(),
            target_identity.clone(),
        );
        // The copied generation is intentionally cataloged but has no
        // manifest yet, so GC can classify its just-created object as a
        // superseded generation after the copy lease expires.
        target.ensure_gc_control().await.unwrap();
        target.register_generation(now_ms()).await.unwrap();
        let root_identity = CheckpointIdentity::new(
            "cluster-a",
            7,
            3,
            LogHash::digest(&[b"archive-test-config"]),
            3,
        );
        let root = ObjectArchiveStore::new_checkpoint_for_single_process(
            target_store.clone(),
            root_identity.clone(),
        );
        root.initialize_checkpoint().await.unwrap();
        root.set_gc_root(root_identity.clone(), now_ms())
            .await
            .unwrap();

        let source_first_segment = &source_checkpoint.manifest().segments()[0];
        let copied_first_key = checkpoint_segment_key(
            &target_identity,
            source_first_segment.start_index(),
            source_first_segment.end_index(),
        );
        let (gate, entered, _cancelled) = TestCheckpointDownloadGate::after_create(
            target.test_store_identity,
            copied_first_key.clone(),
        );
        let _installed = install_test_checkpoint_download_gate(gate.clone());
        let release = gate.release_guard();
        let copy_source = source.clone();
        let copy_target = target.clone();
        let copy =
            tokio::spawn(async move { copy_source.roll_recovery_generation(&copy_target).await });
        wait_for_test_gate(
            entered,
            "recovery roll did not create and verify its first target object",
        )
        .await;

        let mut control = target.load_gc_control().await.unwrap();
        let target_lease = control
            .control
            .leases
            .iter_mut()
            .find(|lease| lease.kind == GcLeaseKind::Publisher)
            .expect("paused copy must hold its target Publisher lease");
        target_lease.expires_at_ms = 0;
        target.update_gc_control(&control).await.unwrap();
        let plan = root
            .plan_gc(
                GcPolicy::new("copy-target-expired", root_identity, 0, 0, 0),
                now_ms(),
            )
            .await
            .unwrap();
        assert!(plan
            .candidates()
            .iter()
            .any(|candidate| candidate.key() == copied_first_key));
        let report = root.execute_gc(plan.plan_hash(), now_ms()).await.unwrap();
        assert!(report.results().iter().any(|evidence| {
            evidence.key == copied_first_key && evidence.outcome == GcDeleteOutcome::Deleted
        }));

        drop(release);
        let result = copy.await.unwrap();
        assert!(
            matches!(result, Err(Error::GenerationRetired { generation: 2, .. })),
            "a target generation retired after object copy must abort before manifest publication: {result:?}"
        );
        assert!(target.load_checkpoint_unleased().await.unwrap().is_none());
        assert!(matches!(
            target_store.get(&copied_first_key).await,
            Err(ObjStoreError::NotFound { .. })
        ));
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

    #[tokio::test]
    async fn overlapping_qefx_append_attaches_only_new_suffix_effects() {
        let (_directory, _store, archive) = fixture();
        let (first, first_chunks) = external_entry(None);
        let first_effect = archive
            .publish_verified_qefx_bundle(&first, &first_chunks)
            .await
            .unwrap();
        assert!(first_effect.chunk_object_keys.is_empty());
        assert!(first_effect.chunk_sha256.is_empty());
        assert!(first_effect.chunk_size_bytes.is_empty());
        let compact_json = serialize_json(&first_effect).unwrap();
        assert!(!compact_json
            .windows(b"chunk_object_keys".len())
            .any(|window| window == b"chunk_object_keys"));

        // Manifests already published by the first QEFX release carried
        // redundant chunk arrays. They remain readable long enough for a
        // current node to compact the checkpoint into the compact shape.
        let first_command = ExternalEffectCommand::decode(&first.payload).unwrap();
        let first_prefix = checkpoint_effect_prefix(
            archive.checkpoint_identity().unwrap(),
            first.index,
            first_command.effect_digest_value(),
        );
        let mut legacy_effect = first_effect.clone();
        for (ordinal, (chunk, expected)) in
            first_chunks.iter().zip(first_command.chunks()).enumerate()
        {
            legacy_effect.chunk_object_keys.push(format!(
                "{first_prefix}/chunks/{ordinal:03}-{}.qefc",
                expected.digest().to_hex()
            ));
            legacy_effect.chunk_sha256.push(sha256_hex(chunk));
            legacy_effect.chunk_size_bytes.push(chunk.len() as u64);
        }
        archive
            .restore_checkpoint_effect(&legacy_effect)
            .await
            .unwrap();
        assert!(serialize_json(&legacy_effect).unwrap().len() > compact_json.len());
        archive
            .publish_committed_with_effects(
                std::slice::from_ref(&first),
                std::slice::from_ref(&first_effect),
            )
            .await
            .unwrap();

        let (second, second_chunks) = external_entry(Some(&first));
        let second_effect = archive
            .publish_verified_qefx_bundle(&second, &second_chunks)
            .await
            .unwrap();
        let before_missing = archive.load_checkpoint().await.unwrap().unwrap();
        let missing = archive
            .publish_committed_with_effects(
                &[first.clone(), second.clone()],
                std::slice::from_ref(&first_effect),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            missing,
            Error::InvalidCheckpoint(ref message)
                if message == "checkpoint QEFX suffix entry has no effect reference"
        ));
        let after_missing = archive.load_checkpoint().await.unwrap().unwrap();
        assert_eq!(after_missing.manifest(), before_missing.manifest());
        assert_eq!(after_missing.version(), before_missing.version());

        let loaded = archive
            .publish_committed_with_effects(
                &[first.clone(), second.clone()],
                &[second_effect.clone(), first_effect.clone()],
            )
            .await
            .unwrap();

        let segments = loaded.manifest().segments();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].effects(), std::slice::from_ref(&first_effect));
        assert_eq!(segments[1].effects(), std::slice::from_ref(&second_effect));
        let effect_indices = segments
            .iter()
            .flat_map(|segment| segment.effects())
            .map(CheckpointEffectRecord::entry_index)
            .collect::<Vec<_>>();
        assert_eq!(effect_indices, vec![first.index, second.index]);

        let restored = archive.load_checkpoint_restore().await.unwrap().unwrap();
        assert_eq!(
            restored.restored().suffix(),
            &[first.clone(), second.clone()]
        );
        for effect in segments.iter().flat_map(|segment| segment.effects()) {
            archive.restore_checkpoint_effect(effect).await.unwrap();
        }

        let replayed = archive
            .publish_committed_with_effects(&[first, second], &[second_effect, first_effect])
            .await
            .unwrap();
        assert_eq!(replayed.manifest(), loaded.manifest());
        assert_eq!(replayed.version(), loaded.version());
    }

    #[test]
    fn legacy_qefx_manifest_has_bounded_transition_headroom() {
        let mut manifest = CheckpointManifest::new(identity());
        let mut previous_hash = LogHash::ZERO;
        for start_index in (1_u64..=1_400).step_by(32) {
            let end_index = start_index.saturating_add(31).min(1_400);
            let last_hash = LogHash::digest(&[&end_index.to_le_bytes()]);
            manifest.segments.push(CheckpointSegmentRecord {
                format_version: CHECKPOINT_SEGMENT_FORMAT_VERSION,
                start_index,
                end_index,
                first_prev_hash: previous_hash,
                last_hash,
                object_key: checkpoint_segment_key(&identity(), start_index, end_index),
                sha256: "c".repeat(64),
                size_bytes: 1,
                effects: (start_index..=end_index)
                    .map(|entry_index| CheckpointEffectRecord {
                        entry_index,
                        manifest_object_key: format!(
                            "{}/effects/{entry_index}/{}",
                            "m".repeat(220),
                            "binding.qefx"
                        ),
                        manifest_sha256: "a".repeat(64),
                        manifest_size_bytes: 512,
                        chunk_object_keys: vec![format!(
                            "{}/effects/{entry_index}/chunks/000-{}.qefc",
                            "c".repeat(180),
                            "d".repeat(64)
                        )],
                        chunk_sha256: vec!["b".repeat(64)],
                        chunk_size_bytes: vec![256 * 1024],
                    })
                    .collect(),
            });
            previous_hash = last_hash;
        }
        manifest.tip = CheckpointTip::new(1_400, previous_hash);

        let bytes = serialize_json(&manifest).unwrap();
        assert!(bytes.len() > 1024 * 1024);
        assert!(bytes.len() < CHECKPOINT_RESTORE_LIMITS.manifest_encoded_bytes as usize);
        assert_eq!(serialize_checkpoint_manifest(&manifest).unwrap(), bytes);
        let (_directory, _store, archive) = fixture();
        archive.validate_checkpoint_manifest(&manifest).unwrap();

        let mut compact = manifest;
        for effect in compact
            .segments
            .iter_mut()
            .flat_map(|segment| &mut segment.effects)
        {
            effect.chunk_object_keys.clear();
            effect.chunk_sha256.clear();
            effect.chunk_size_bytes.clear();
        }
        let compact_bytes = serialize_checkpoint_manifest(&compact).unwrap();
        assert!(compact_bytes.len() < 1024 * 1024);
        assert!(compact_bytes.len() + 256 * 1024 < bytes.len());
    }

    fn identity() -> CheckpointIdentity {
        CheckpointIdentity::new(
            "cluster-a",
            7,
            3,
            LogHash::digest(&[b"archive-test-config"]),
            1,
        )
    }

    #[test]
    fn checkpoint_digest_qualifies_namespace_and_manifest_identity() {
        let expected = identity();
        let different = CheckpointIdentity::new(
            expected.cluster_id(),
            expected.epoch(),
            expected.config_id(),
            LogHash::digest(&[b"different-membership"]),
            expected.recovery_generation(),
        );
        assert_ne!(
            checkpoint_namespace(&expected),
            checkpoint_namespace(&different)
        );
        assert!(checkpoint_namespace(&expected).contains(&expected.config_digest().to_hex()));

        let (_directory, _store, archive) = fixture();
        let manifest = CheckpointManifest::new(different);
        assert_eq!(manifest.format_version(), 3);
        assert!(matches!(
            archive.validate_checkpoint_manifest(&manifest),
            Err(Error::CheckpointIdentityMismatch {
                field: "config_digest",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn checkpoint_v3_never_falls_back_to_the_legacy_namespace() {
        let (_directory, store, archive) = fixture();
        archive.initialize_checkpoint().await.unwrap();
        let legacy = "rhiza/cluster-a/checkpoints/epoch-00000000000000000007/config-00000000000000000003/generation-00000000000000000001/manifest.json";
        assert!(matches!(
            store.get(legacy).await,
            Err(ObjStoreError::NotFound { .. })
        ));
    }

    #[test]
    fn checkpoint_anchor_rejects_same_config_id_with_different_digest() {
        let (_directory, _store, archive) = fixture();
        let anchor = RecoveryAnchor::new(
            "cluster-a",
            7,
            ConfigurationState::active(3, LogHash::digest(&[b"different-membership"])),
            1,
            LogAnchor::new(0, LogHash::ZERO),
            SnapshotIdentity::new(
                "snapshot",
                LogHash::digest(&[b"snapshot"]),
                8,
                LogHash::digest(&[b"executor"]),
            ),
        );
        assert!(matches!(
            archive.validate_recovery_anchor(&anchor),
            Err(Error::CheckpointIdentityMismatch {
                field: "config_digest",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn recovery_roll_rejects_same_config_id_with_different_digest() {
        let (_directory, store, source) = fixture();
        let target = ObjectArchiveStore::new_checkpoint_for_single_process(
            store,
            CheckpointIdentity::new(
                "cluster-a",
                7,
                3,
                LogHash::digest(&[b"different-membership"]),
                2,
            ),
        );
        assert!(matches!(
            source.roll_recovery_generation(&target).await,
            Err(Error::InvalidCheckpoint(message))
                if message.contains("same cluster/epoch/config identity")
        ));
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

    fn next_entry(previous: &LogEntry) -> LogEntry {
        let index = previous.index + 1;
        let payload = format!("entry-{index}").into_bytes();
        let hash = LogEntry::calculate_hash(
            "cluster-a",
            index,
            7,
            3,
            EntryType::Command,
            previous.hash,
            &payload,
        );
        LogEntry {
            cluster_id: "cluster-a".into(),
            epoch: 7,
            config_id: 3,
            index,
            entry_type: EntryType::Command,
            payload,
            prev_hash: previous.hash,
            hash,
        }
    }

    fn external_entry(previous: Option<&LogEntry>) -> (LogEntry, Vec<Vec<u8>>) {
        let index = previous.map_or(1, |entry| entry.index + 1);
        let prev_hash = previous.map_or(LogHash::ZERO, |entry| entry.hash);
        let chunks = vec![format!("external-effect-{index}").into_bytes()];
        let command = ExternalEffectCommand::from_profile_bytes_and_chunks(
            "cluster-a",
            7,
            3,
            identity().config_digest(),
            index,
            prev_hash,
            ExternalEffectProfile::sql(format!("sql-profile-{index}").into_bytes()),
            &chunks,
        )
        .unwrap();
        let payload = command.encode().unwrap();
        let hash = LogEntry::calculate_hash(
            "cluster-a",
            index,
            7,
            3,
            EntryType::Command,
            prev_hash,
            &payload,
        );
        (
            LogEntry {
                cluster_id: "cluster-a".into(),
                epoch: 7,
                config_id: 3,
                index,
                entry_type: EntryType::Command,
                payload,
                prev_hash,
                hash,
            },
            chunks,
        )
    }
}
