#[cfg(feature = "sql")]
use std::collections::BTreeMap;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::{
    error::Error,
    fmt, fs,
    future::Future,
    io::{Read, Write},
    path::{Path, PathBuf},
    process,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::{Duration, Instant},
};

use rhiza_archive::{
    CheckpointIdentity, CheckpointPublisher, CheckpointPublisherOptions, CheckpointTip,
    ObjectArchiveStore, RestoredCheckpoint,
};
#[cfg(any(feature = "graph", feature = "kv"))]
use rhiza_core::SnapshotIdentity;
use rhiza_core::{
    ConfigChange, ConfigurationState, ExecutionProfile, LogAnchor, LogEntry, LogHash, LogIndex,
    RecoveryAnchor, StopBinding,
};
#[cfg(feature = "sql")]
use rhiza_core::{EntryType, ExternalEffectCommand};
#[cfg(feature = "graph")]
use rhiza_graph::{
    decode_snapshot as decode_graph_snapshot, encode_snapshot as encode_graph_snapshot,
    restore_snapshot_file as restore_graph_snapshot_file, LadybugStateMachine,
};
#[cfg(feature = "kv")]
use rhiza_kv::{
    decode_snapshot as decode_kv_snapshot, encode_snapshot as encode_kv_snapshot,
    restore_snapshot_file as restore_kv_snapshot_file, RedbStateMachine,
};
use rhiza_log::{FileLogStore, IndexRange, LogStore};
use rhiza_quepaxa::{Membership, RecorderFileStore};
#[cfg(feature = "sql")]
use rhiza_sql::{
    restore_recovery_snapshot_file, sql_executor_fingerprint, QwalEffectManifestV4,
    SqliteStateMachine, VerifiedQwalEffectBundleV4,
};
use serde::{Deserialize, Serialize};

use crate::{Materializer, NodeConfig, NodeRuntime, StopInformation};

const FLUSH_BATCH_ENTRIES: LogIndex = 32;
const SYNC_COMPACTION_POLL_INTERVAL: Duration = Duration::from_millis(100);
const SYNC_RECOVERY_RETRY_INITIAL: Duration = Duration::from_millis(100);
const SYNC_RECOVERY_RETRY_MAX: Duration = Duration::from_secs(1);
#[cfg(feature = "sql")]
const ONLINE_QEFX_GC_INTERVAL: Duration = Duration::from_millis(250);
#[cfg(feature = "sql")]
const ONLINE_QEFX_GC_REMOVAL_BUDGET: usize = 1;
const RESTORE_INTENT_FILE: &str = ".rhiza-restore.json";
const RESTORE_RECEIPT_FILE: &str = ".rhiza-checkpoint-install.json";
const RESTORE_STAGING_PREFIX: &str = ".restore-stage-";
#[cfg(feature = "sql")]
pub(crate) const QEFX_RESTORE_HANDOFF_DIR: &str = "consensus/qefx-restore";
#[cfg(feature = "sql")]
const PENDING_QEFX_GC_FILE: &str = "consensus/pending-qefx-gc.json";
const RESTORE_MARKER_TMP_PREFIX: &str = ".restore-marker-tmp-";
const SUCCESSOR_RESTORE_LOCK_FILE: &str = ".successor-restore.lock";
const SUCCESSOR_RESTORE_INTENT_FILE: &str = ".successor-restore.intent";
const SUCCESSOR_RESTORE_COMPLETE_FILE: &str = ".successor-restore.complete";
const SUCCESSOR_PRESTAGE_LOCK_FILE: &str = ".successor-prestage.lock";
pub(crate) const SUCCESSOR_PRESTAGE_INTENT_FILE: &str = ".successor-prestage.intent";
pub(crate) const SUCCESSOR_PRESTAGE_READY_FILE: &str = ".successor-prestage.ready";
const SUCCESSOR_PRESTAGE_PUBLISHED_FILE: &str = ".successor-prestage.published";
const SUCCESSOR_PRESTAGE_FINALIZED_FILE: &str = ".successor-prestage.finalized";
const REPAIR_ARTIFACT_OWNER_FILE: &str = ".rhiza-recovery-owner.json";
pub const LOCAL_CHECKPOINT_IDENTITY_FILE: &str = ".rhiza-checkpoint-identity.json";
static RESTORE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn publisher_lease_renewal_interval(lease_duration_ms: u64) -> Duration {
    (Duration::from_millis(lease_duration_ms) / 3).max(Duration::from_micros(1))
}

fn retryable_sync_archive_error(error: &rhiza_archive::Error) -> bool {
    matches!(
        error,
        rhiza_archive::Error::ObjectStore(_)
            | rhiza_archive::Error::CompareAndSwapRetriesExhausted { .. }
            | rhiza_archive::Error::GcBarrierActive { .. }
            | rhiza_archive::Error::GcBarrierBusy { .. }
            | rhiza_archive::Error::GcLeaseMissing { .. }
    )
}

fn retryable_sync_recovery_error(error: &DurabilityError) -> bool {
    matches!(error, DurabilityError::Io(_) | DurabilityError::Unavailable)
        || matches!(error, DurabilityError::Archive(error) if retryable_sync_archive_error(error))
}

fn next_sync_recovery_retry(current: Duration) -> Duration {
    current.saturating_mul(2).min(SYNC_RECOVERY_RETRY_MAX)
}

#[cfg(test)]
use std::sync::{mpsc, OnceLock};

#[cfg(test)]
struct TestRestoreLockGate {
    id: u64,
    data_dir: PathBuf,
    entered: mpsc::SyncSender<()>,
    release: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
}

#[cfg(test)]
impl Clone for TestRestoreLockGate {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            data_dir: self.data_dir.clone(),
            entered: self.entered.clone(),
            release: Arc::clone(&self.release),
        }
    }
}

#[cfg(test)]
struct InstalledTestRestoreLockGate {
    id: u64,
}

#[cfg(test)]
static TEST_RESTORE_LOCK_GATES: OnceLock<Mutex<Vec<TestRestoreLockGate>>> = OnceLock::new();

#[cfg(test)]
fn test_restore_lock_gates() -> &'static Mutex<Vec<TestRestoreLockGate>> {
    TEST_RESTORE_LOCK_GATES.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn install_test_restore_lock_gate(
    data_dir: PathBuf,
    entered: mpsc::SyncSender<()>,
    release: mpsc::Receiver<()>,
) -> InstalledTestRestoreLockGate {
    let id = RESTORE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut gates = test_restore_lock_gates()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert!(
        !gates.iter().any(|gate| gate.data_dir == data_dir),
        "restore lock gate is already installed for this data directory"
    );
    gates.push(TestRestoreLockGate {
        id,
        data_dir,
        entered,
        release: Arc::new(Mutex::new(Some(release))),
    });
    InstalledTestRestoreLockGate { id }
}

#[cfg(test)]
impl Drop for InstalledTestRestoreLockGate {
    fn drop(&mut self) {
        test_restore_lock_gates()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|gate| gate.id != self.id);
    }
}

#[cfg(test)]
fn test_restore_lock_acquired(data_dir: &Path) {
    let gate = test_restore_lock_gates()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .find(|gate| gate.data_dir == data_dir)
        .cloned();
    if let Some(gate) = gate {
        gate.entered
            .send(())
            .expect("restore lock test gate receiver must remain live");
        let release = gate
            .release
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .expect("restore lock test gate may fire only once");
        release
            .recv_timeout(Duration::from_secs(5))
            .expect("restore lock test gate must release within its bounded wait");
    }
}

/// Test-only interposition exactly after the installer owns its lock handle
/// and before the pathname is trusted again. The replacement file is sent on
/// a bounded channel so the test retains its lock while the installer
/// validates the pathname-to-handle identity fence.
#[cfg(test)]
struct TestRestoreLockPathReplacementHook {
    id: u64,
    data_dir: PathBuf,
    replacement: mpsc::SyncSender<fs::File>,
}

#[cfg(test)]
impl Clone for TestRestoreLockPathReplacementHook {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            data_dir: self.data_dir.clone(),
            replacement: self.replacement.clone(),
        }
    }
}

#[cfg(test)]
struct InstalledTestRestoreLockPathReplacementHook {
    id: u64,
}

#[cfg(test)]
static TEST_RESTORE_LOCK_PATH_REPLACEMENT_HOOKS: OnceLock<
    Mutex<Vec<TestRestoreLockPathReplacementHook>>,
> = OnceLock::new();

#[cfg(test)]
fn test_restore_lock_path_replacement_hooks(
) -> &'static Mutex<Vec<TestRestoreLockPathReplacementHook>> {
    TEST_RESTORE_LOCK_PATH_REPLACEMENT_HOOKS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn install_test_restore_lock_path_replacement_hook(
    data_dir: PathBuf,
    replacement: mpsc::SyncSender<fs::File>,
) -> InstalledTestRestoreLockPathReplacementHook {
    let id = RESTORE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut hooks = test_restore_lock_path_replacement_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert!(
        !hooks.iter().any(|hook| hook.data_dir == data_dir),
        "restore lock path replacement hook is already installed for this data directory"
    );
    hooks.push(TestRestoreLockPathReplacementHook {
        id,
        data_dir,
        replacement,
    });
    InstalledTestRestoreLockPathReplacementHook { id }
}

#[cfg(test)]
impl Drop for InstalledTestRestoreLockPathReplacementHook {
    fn drop(&mut self) {
        test_restore_lock_path_replacement_hooks()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|hook| hook.id != self.id);
    }
}

#[cfg(test)]
fn test_restore_lock_before_path_revalidation(data_dir: &Path) {
    let hook = test_restore_lock_path_replacement_hooks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .find(|hook| hook.data_dir == data_dir)
        .cloned();
    if let Some(hook) = hook {
        let path = data_dir.join(crate::NODE_DATA_ROOT_LOCK_FILE);
        fs::remove_file(&path).expect("test hook must remove the installer-created lock");
        let replacement = crate::acquire_node_data_root_lock(data_dir)
            .expect("test hook must create and lock the replacement lock file");
        hook.replacement
            .send(replacement.into_file())
            .expect("replacement lock test receiver must remain live");
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RestoreIntentIdentity {
    cluster_id: String,
    node_id: String,
    execution_profile: ExecutionProfile,
    epoch: u64,
    config_id: u64,
    recovery_generation: u64,
    checkpoint_index: LogIndex,
    checkpoint_hash: String,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "identity", rename_all = "snake_case")]
enum RecoveryArtifactIdentity {
    Prestage(SuccessorPrestageIdentity),
    Restore(RestoreIntentIdentity),
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RepairArtifactRole {
    Staging,
    Quarantine,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RepairArtifactOwnership {
    role: RepairArtifactRole,
    name: String,
    identity: RecoveryArtifactIdentity,
}

#[derive(Serialize)]
struct SuccessorRestoreIdentity<'a> {
    cluster_id: &'a str,
    epoch: u64,
    target_config_id: u64,
    recovery_generation: u64,
    node_id: &'a str,
    membership_digest: String,
    predecessor_config_id: u64,
    stop_index: LogIndex,
    stop_hash: String,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SuccessorRestoreReceipt {
    cluster_id: String,
    epoch: u64,
    target_config_id: u64,
    recovery_generation: u64,
    node_id: String,
    membership_digest: String,
    predecessor_config_id: u64,
    stop_index: LogIndex,
    stop_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SuccessorPrestageIdentity {
    cluster_id: String,
    epoch: u64,
    predecessor_config_id: u64,
    predecessor_membership_digest: String,
    predecessor_recovery_generation: u64,
    node_id: String,
    execution_profile: ExecutionProfile,
    target_config_id: u64,
    target_membership_digest: String,
    seed_index: LogIndex,
    seed_hash: String,
}

impl SuccessorPrestageIdentity {
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn predecessor_config_id(&self) -> u64 {
        self.predecessor_config_id
    }

    pub fn predecessor_membership_digest(&self) -> LogHash {
        LogHash::from_hex(&self.predecessor_membership_digest)
            .expect("validated predecessor membership digest")
    }

    pub const fn predecessor_recovery_generation(&self) -> u64 {
        self.predecessor_recovery_generation
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub const fn execution_profile(&self) -> ExecutionProfile {
        self.execution_profile
    }

    pub const fn target_config_id(&self) -> u64 {
        self.target_config_id
    }

    pub fn target_membership_digest(&self) -> LogHash {
        LogHash::from_hex(&self.target_membership_digest)
            .expect("validated successor prestage membership digest")
    }

    pub fn seed_anchor(&self) -> LogAnchor {
        LogAnchor::new(
            self.seed_index,
            LogHash::from_hex(&self.seed_hash).expect("validated successor prestage seed hash"),
        )
    }

    fn checkpoint_identity(&self) -> CheckpointIdentity {
        CheckpointIdentity::new(
            self.cluster_id.clone(),
            self.epoch,
            self.predecessor_config_id,
            LogHash::from_hex(&self.predecessor_membership_digest)
                .expect("validated successor prestage membership digest"),
            self.predecessor_recovery_generation,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SuccessorPrestageState {
    Preparing,
    Ready,
    Published,
    Finalized,
}

#[derive(Debug)]
pub struct SuccessorPrestage {
    path: PathBuf,
    identity: SuccessorPrestageIdentity,
    state: SuccessorPrestageState,
    _lock: fs::File,
}

impl SuccessorPrestage {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub const fn identity(&self) -> &SuccessorPrestageIdentity {
        &self.identity
    }

    pub const fn state(&self) -> SuccessorPrestageState {
        self.state
    }
}

pub struct SuccessorRestorePreparation {
    tip: CheckpointTip,
    data_dir: PathBuf,
    identity: Vec<u8>,
    requires_recorder_install: bool,
    _lock: fs::File,
}

/// A verified remote checkpoint that is ready for a synchronous local install.
///
/// This owns the downloaded snapshot and log suffix. It is deliberately not
/// `Clone`: callers retain and retry the same prepared bytes rather than
/// re-reading a potentially newer remote checkpoint or copying a large buffer.
pub struct PreparedCheckpointRestore {
    identity: CheckpointIdentity,
    execution_profile: ExecutionProfile,
    restored: RestoredCheckpoint,
    checkpoint_root: LogAnchor,
    #[cfg(feature = "sql")]
    external_sql_effects: Vec<PreparedCheckpointEffect>,
}

/// One QEFX suffix effect verified during remote preparation. Installers only
/// consume these owned bytes; they never contact a recorder or archive.
#[cfg(feature = "sql")]
struct PreparedCheckpointEffect {
    entry_index: LogIndex,
    effect: VerifiedQwalEffectBundleV4,
    manifest: Vec<u8>,
    chunks: Vec<Vec<u8>>,
}

/// The two local installation contracts. Fresh installation owns every
/// rebuildable component; rejoin deliberately leaves the recorder outside the
/// mutation and comparison set.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointInstallMode {
    Fresh,
    RejoinPreservingRecorder,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ExpectedPathIdentity {
    Missing,
    Directory(PathIdentity),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PathIdentity {
    kind: ExpectedEntryKind,
    len: u64,
    object: crate::NodeDataPathIdentity,
    #[cfg(unix)]
    modified_seconds: i64,
    #[cfg(unix)]
    modified_nanoseconds: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExpectedEntryKind {
    Directory,
    RegularFile,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ExpectedRegularFile {
    Missing,
    Exact {
        identity: PathIdentity,
        bytes: Vec<u8>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExpectedArtifact {
    name: String,
    identity: PathIdentity,
    owner: ExpectedRegularFile,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExpectedPathEntry {
    name: String,
    identity: PathIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ExpectedQlogState {
    Missing,
    Logical {
        state: Box<rhiza_log::LogState>,
        paths: Vec<ExpectedPathEntry>,
    },
    /// A corrupt or interrupted qlog is still represented by a stable,
    /// bounded read-only stamp. The installer will fail closed later during
    /// normal preflight; this state exists so a concurrent qlog repair or
    /// replacement cannot make an old restore token appear current.
    Unreadable {
        reason: String,
        entries: Vec<ExpectedPathEntry>,
    },
}

/// An opaque, non-cloneable snapshot of every local state an installer may
/// mutate or trust. It is captured before remote checkpoint I/O, consumed by
/// exactly one local installer, and revalidated while `.node.lock` is held.
///
/// Keeping the token private-fielded and non-`Clone` prevents callers from
/// reusing an old observation after a different recovery generation has
/// completed locally.
pub struct ExpectedLocalRestoreState {
    data_dir: PathBuf,
    parent: PathIdentity,
    data_dir_identity: ExpectedPathIdentity,
    lock: ExpectedRegularFile,
    mode: CheckpointInstallMode,
    target_node_id: String,
    identity: CheckpointIdentity,
    execution_profile: ExecutionProfile,
    initial_configuration: ConfigurationState,
    completion_marker_name: Option<String>,
    completion_marker: ExpectedRegularFile,
    restore_intent: ExpectedRegularFile,
    restore_receipt: ExpectedRegularFile,
    recovery_artifacts: Vec<ExpectedArtifact>,
    qlog: ExpectedQlogState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RestoreInstallReceipt {
    format_version: u8,
    mode: CheckpointInstallMode,
    identity: RestoreIntentIdentity,
    checkpoint_index: LogIndex,
    checkpoint_hash: String,
    completion_marker_name: Option<String>,
    completion_marker_hash: Option<String>,
}

impl PreparedCheckpointRestore {
    pub const fn identity(&self) -> &CheckpointIdentity {
        &self.identity
    }

    pub const fn execution_profile(&self) -> ExecutionProfile {
        self.execution_profile
    }

    pub const fn restored(&self) -> &RestoredCheckpoint {
        &self.restored
    }

    pub const fn checkpoint_root(&self) -> LogAnchor {
        self.checkpoint_root
    }

    fn validate(&self) -> Result<(), DurabilityError> {
        if snapshot_profile(self.identity.cluster_id())? != self.execution_profile {
            return Err(DurabilityError::SnapshotVerification(
                "prepared checkpoint profile does not match checkpoint identity".into(),
            ));
        }
        let restored_root = LogAnchor::new(self.restored.tip().index(), self.restored.tip().hash());
        if self.checkpoint_root != restored_root {
            return Err(DurabilityError::SnapshotVerification(
                "prepared checkpoint root does not match restored tip".into(),
            ));
        }
        validate_restored_suffix(self.execution_profile, self.restored.suffix())
    }
}

impl PathIdentity {
    fn capture(path: &Path, metadata: &fs::Metadata) -> Result<Self, DurabilityError> {
        let kind = if metadata.is_dir() {
            ExpectedEntryKind::Directory
        } else if metadata.is_file() {
            ExpectedEntryKind::RegularFile
        } else {
            return Err(DurabilityError::SnapshotVerification(
                "restore expected-state path is not a regular file or directory".into(),
            ));
        };
        if metadata.file_type().is_symlink() {
            return Err(DurabilityError::SnapshotVerification(
                "restore expected-state path is a symlink".into(),
            ));
        }
        let object = crate::capture_node_data_path_identity(
            path,
            matches!(kind, ExpectedEntryKind::Directory),
        )
        .map_err(|error| {
            DurabilityError::SnapshotVerification(format!(
                "cannot capture exact restore expected-state path identity: {error}"
            ))
        })?;
        Ok(Self {
            kind,
            len: metadata.len(),
            object,
            #[cfg(unix)]
            modified_seconds: metadata.mtime(),
            #[cfg(unix)]
            modified_nanoseconds: metadata.mtime_nsec(),
        })
    }

    fn same_path_identity(&self, other: &Self) -> bool {
        if self.kind != other.kind {
            return false;
        }
        if self.kind == ExpectedEntryKind::Directory {
            return self.object == other.object;
        }
        self == other
    }
}

/// Captures the local state that must remain unchanged while a remote
/// checkpoint is downloaded. This function is deliberately read-only: it
/// does not create the data directory, the shared lock, qlog paths, intents,
/// staging directories, or receipts.
pub fn capture_expected_local_restore_state(
    data_dir: impl AsRef<Path>,
    mode: CheckpointInstallMode,
    target_node_id: &str,
    identity: &CheckpointIdentity,
    execution_profile: ExecutionProfile,
    initial_configuration: ConfigurationState,
    completion_marker_name: Option<&str>,
) -> Result<ExpectedLocalRestoreState, DurabilityError> {
    validate_restore_target_node_id(target_node_id)?;
    if snapshot_profile(identity.cluster_id())? != execution_profile {
        return Err(DurabilityError::SnapshotVerification(
            "expected local restore profile does not match checkpoint identity".into(),
        ));
    }
    if let Some(name) = completion_marker_name {
        validate_restore_completion_marker_name(name)?;
    }
    let data_dir = data_dir.as_ref().to_path_buf();
    let parent = data_dir.parent().unwrap_or_else(|| Path::new("."));
    let parent_metadata = fs::symlink_metadata(parent)?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(DurabilityError::DataDirNotFresh(data_dir));
    }
    let parent = PathIdentity::capture(parent, &parent_metadata)?;
    let data_dir_identity = match fs::symlink_metadata(&data_dir) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(DurabilityError::DataDirNotFresh(data_dir));
        }
        Ok(metadata) => {
            ExpectedPathIdentity::Directory(PathIdentity::capture(&data_dir, &metadata)?)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => ExpectedPathIdentity::Missing,
        Err(error) => return Err(error.into()),
    };

    let (lock, completion_marker, restore_intent, restore_receipt, recovery_artifacts, qlog) =
        match &data_dir_identity {
            ExpectedPathIdentity::Missing => (
                ExpectedRegularFile::Missing,
                ExpectedRegularFile::Missing,
                ExpectedRegularFile::Missing,
                ExpectedRegularFile::Missing,
                Vec::new(),
                ExpectedQlogState::Missing,
            ),
            ExpectedPathIdentity::Directory(_) => {
                let lock =
                    capture_restore_regular_file(&data_dir.join(crate::NODE_DATA_ROOT_LOCK_FILE))?;
                if let ExpectedRegularFile::Exact { identity, bytes } = &lock {
                    if identity.kind != ExpectedEntryKind::RegularFile || !bytes.is_empty() {
                        return Err(DurabilityError::SnapshotVerification(
                            "node data lock is not a zero-length regular file".into(),
                        ));
                    }
                }
                let completion_marker = match completion_marker_name {
                    Some(name) => capture_restore_regular_file(&data_dir.join(name))?,
                    None => ExpectedRegularFile::Missing,
                };
                let restore_intent =
                    capture_restore_regular_file(&data_dir.join(RESTORE_INTENT_FILE))?;
                let restore_receipt =
                    capture_restore_regular_file(&data_dir.join(RESTORE_RECEIPT_FILE))?;
                if matches!(&restore_receipt, ExpectedRegularFile::Exact { .. })
                    && !is_valid_restore_install_receipt(&data_dir.join(RESTORE_RECEIPT_FILE))?
                {
                    return Err(DurabilityError::SnapshotVerification(
                        "local checkpoint restore receipt is invalid".into(),
                    ));
                }
                let recovery_artifacts = capture_recovery_artifact_set(&data_dir)?;
                let qlog = capture_expected_qlog_state(
                    &data_dir,
                    identity,
                    initial_configuration.clone(),
                )?;
                (
                    lock,
                    completion_marker,
                    restore_intent,
                    restore_receipt,
                    recovery_artifacts,
                    qlog,
                )
            }
        };
    Ok(ExpectedLocalRestoreState {
        data_dir,
        parent,
        data_dir_identity,
        lock,
        mode,
        target_node_id: target_node_id.to_owned(),
        identity: identity.clone(),
        execution_profile,
        initial_configuration,
        completion_marker_name: completion_marker_name.map(str::to_owned),
        completion_marker,
        restore_intent,
        restore_receipt,
        recovery_artifacts,
        qlog,
    })
}

fn capture_restore_regular_file(path: &Path) -> Result<ExpectedRegularFile, DurabilityError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ExpectedRegularFile::Missing);
        }
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 16 * 1024 {
        return Err(DurabilityError::SnapshotVerification(
            "restore control file is not a bounded regular file".into(),
        ));
    }
    let bytes = read_bounded_regular_file(path, 16 * 1024)?.ok_or_else(|| {
        DurabilityError::SnapshotVerification(
            "restore control file disappeared during capture".into(),
        )
    })?;
    Ok(ExpectedRegularFile::Exact {
        identity: PathIdentity::capture(path, &metadata)?,
        bytes,
    })
}

fn capture_recovery_artifact_set(
    data_dir: &Path,
) -> Result<Vec<ExpectedArtifact>, DurabilityError> {
    let mut artifacts = Vec::new();
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with(RESTORE_STAGING_PREFIX)
            && !name.starts_with(".rebuildable-quarantine-")
            && !name.starts_with(RESTORE_MARKER_TMP_PREFIX)
        {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            return Err(DurabilityError::SnapshotVerification(
                "recovery artifact is a symlink".into(),
            ));
        }
        let owner = if metadata.is_dir() {
            capture_restore_regular_file(&entry.path().join(REPAIR_ARTIFACT_OWNER_FILE))?
        } else {
            ExpectedRegularFile::Missing
        };
        artifacts.push(ExpectedArtifact {
            name,
            identity: PathIdentity::capture(&entry.path(), &metadata)?,
            owner,
        });
    }
    artifacts.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(artifacts)
}

fn capture_expected_qlog_state(
    data_dir: &Path,
    identity: &CheckpointIdentity,
    initial_configuration: ConfigurationState,
) -> Result<ExpectedQlogState, DurabilityError> {
    let qlog_dir = data_dir.join("consensus/log");
    match FileLogStore::inspect_logical_state_read_only(
        &qlog_dir,
        identity.cluster_id(),
        identity.epoch(),
        initial_configuration,
    ) {
        Ok(None) => Ok(ExpectedQlogState::Missing),
        Ok(Some(state)) => Ok(ExpectedQlogState::Logical {
            state: Box::new(state),
            paths: capture_qlog_metadata_stamp(&qlog_dir)?,
        }),
        Err(error) => Ok(ExpectedQlogState::Unreadable {
            reason: error.to_string(),
            entries: capture_qlog_metadata_stamp(&qlog_dir)?,
        }),
    }
}

fn capture_qlog_metadata_stamp(qlog_dir: &Path) -> Result<Vec<ExpectedPathEntry>, DurabilityError> {
    let metadata = match fs::symlink_metadata(qlog_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut entries = vec![ExpectedPathEntry {
        name: String::new(),
        identity: PathIdentity::capture(qlog_dir, &metadata)?,
    }];
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Ok(entries);
    }
    for entry in fs::read_dir(qlog_dir)? {
        let entry = entry?;
        if entries.len() == 129 {
            return Err(DurabilityError::SnapshotVerification(
                "qlog metadata stamp exceeds its bounded entry limit".into(),
            ));
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        entries.push(ExpectedPathEntry {
            name: entry.file_name().to_string_lossy().into_owned(),
            identity: PathIdentity::capture(&path, &metadata)?,
        });
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(entries)
}

/// A caller-owned completion marker published only after checkpoint component
/// promotion. Its constructor validates that the marker cannot collide with
/// recovery-control files or escape the configured data directory.
pub struct RestoreCompletionMarker<'a> {
    name: &'a str,
    contents: &'a [u8],
}

impl<'a> RestoreCompletionMarker<'a> {
    pub fn new(name: &'a str, contents: &'a [u8]) -> Result<Self, DurabilityError> {
        validate_restore_completion_marker_name(name)?;
        Ok(Self { name, contents })
    }

    fn as_parts(&self) -> (&'a str, &'a [u8]) {
        (self.name, self.contents)
    }
}

impl SuccessorRestorePreparation {
    pub const fn tip(&self) -> CheckpointTip {
        self.tip
    }

    pub const fn requires_recorder_install(&self) -> bool {
        self.requires_recorder_install
    }

    pub fn complete(mut self) -> Result<CheckpointTip, DurabilityError> {
        if !self.requires_recorder_install {
            return Ok(self.tip);
        }
        complete_adopted_successor_prestage(&self.data_dir, &self.identity)?;
        self.requires_recorder_install = false;
        Ok(self.tip)
    }
}

pub fn complete_adopted_successor_prestage(
    data_dir: &Path,
    expected_identity: &[u8],
) -> Result<(), DurabilityError> {
    let intent = data_dir.join(SUCCESSOR_RESTORE_INTENT_FILE);
    let actual = read_regular_successor_control_file(&intent)?.ok_or_else(|| {
        DurabilityError::SnapshotVerification("successor restore intent is missing".into())
    })?;
    if parse_successor_restore_receipt(&actual).is_none()
        || parse_successor_restore_receipt(expected_identity).is_none()
        || actual != expected_identity
    {
        return Err(DurabilityError::SnapshotVerification(
            "successor restore intent changed before completion".into(),
        ));
    }
    let complete = data_dir.join(SUCCESSOR_RESTORE_COMPLETE_FILE);
    match fs::symlink_metadata(&complete) {
        Ok(_) => {
            return Err(DurabilityError::SnapshotVerification(
                "successor restore completion target already exists".into(),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::rename(intent, complete)?;
    sync_directory(data_dir)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurabilityMode {
    Sync,
    Bounded { max_lag: Duration },
    Periodic { interval: Duration },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurabilityHealth {
    Available,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointRestoreState {
    None,
    IdentityBound,
}

impl DurabilityMode {
    pub fn validate(&self) -> Result<(), DurabilityError> {
        match self {
            Self::Sync => Ok(()),
            Self::Bounded { max_lag } if max_lag.is_zero() => {
                Err(DurabilityError::InvalidDuration { mode: "bounded" })
            }
            Self::Periodic { interval } if interval.is_zero() => {
                Err(DurabilityError::InvalidDuration { mode: "periodic" })
            }
            Self::Bounded { .. } | Self::Periodic { .. } => Ok(()),
        }
    }
}

#[derive(Debug)]
pub enum DurabilityError {
    InvalidDuration {
        mode: &'static str,
    },
    MissingCheckpoint,
    Unavailable,
    LagExceeded {
        committed_index: LogIndex,
        durable_index: LogIndex,
        max_lag: Duration,
    },
    ArchiveAheadOfLocal {
        durable_index: LogIndex,
        local_index: LogIndex,
    },
    SnapshotRequired {
        anchor: Box<RecoveryAnchor>,
    },
    LocalLogGap {
        expected: LogIndex,
        actual: Option<LogIndex>,
    },
    LocalLogConflict {
        index: LogIndex,
    },
    SnapshotVerification(String),
    PreconditionFailed,
    DataDirNotFresh(PathBuf),
    Archive(rhiza_archive::Error),
    Log(rhiza_log::Error),
    Io(std::io::Error),
}

impl fmt::Display for DurabilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDuration { mode } => {
                write!(f, "{mode} durability duration must be non-zero")
            }
            Self::MissingCheckpoint => write!(f, "checkpoint manifest is missing"),
            Self::Unavailable => write!(f, "sync durability is unavailable"),
            Self::LagExceeded {
                committed_index,
                durable_index,
                max_lag,
            } => write!(
                f,
                "checkpoint lag exceeded {max_lag:?}: committed index {committed_index}, durable index {durable_index}"
            ),
            Self::ArchiveAheadOfLocal {
                durable_index,
                local_index,
            } => write!(
                f,
                "checkpoint tip {durable_index} is ahead of local qlog tip {local_index}"
            ),
            Self::SnapshotRequired { anchor } => write!(
                f,
                "snapshot restore required at qlog anchor {} before checkpoint flush",
                anchor.compacted().index()
            ),
            Self::LocalLogGap { expected, actual } => {
                write!(
                    f,
                    "local qlog gap: expected index {expected}, got {actual:?}"
                )
            }
            Self::LocalLogConflict { index } => {
                write!(f, "local qlog hash chain conflicts at index {index}")
            }
            Self::SnapshotVerification(message) => {
                write!(f, "checkpoint snapshot verification failed: {message}")
            }
            Self::PreconditionFailed => write!(f, "checkpoint precondition failed"),
            Self::DataDirNotFresh(path) => write!(
                f,
                "restore data directory contains existing state: {}",
                path.display()
            ),
            Self::Archive(error) => error.fmt(f),
            Self::Log(error) => error.fmt(f),
            Self::Io(error) => error.fmt(f),
        }
    }
}

impl Error for DurabilityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Archive(error) => Some(error),
            Self::Log(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rhiza_archive::Error> for DurabilityError {
    fn from(error: rhiza_archive::Error) -> Self {
        Self::Archive(error)
    }
}

impl From<rhiza_log::Error> for DurabilityError {
    fn from(error: rhiza_log::Error) -> Self {
        Self::Log(error)
    }
}

impl From<std::io::Error> for DurabilityError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug)]
enum PendingLag {
    New(Instant),
    Recovered,
}

#[derive(Debug)]
struct CoordinatorState {
    durable_tip: CheckpointTip,
    committed_index: LogIndex,
    pending_lag: Option<PendingLag>,
    health: DurabilityHealth,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg(feature = "sql")]
struct PendingQefxGc {
    cluster_id: String,
    epoch: u64,
    config_id: u64,
    config_digest: String,
    through_slot: LogIndex,
    checkpoint_hash: String,
    manifest_digest: String,
}

pub struct CheckpointCoordinator {
    store: ObjectArchiveStore,
    publisher: CheckpointPublisher,
    publisher_lease_renewal_interval: Duration,
    mode: DurabilityMode,
    state: Mutex<CoordinatorState>,
    successor_baseline_required: AtomicBool,
    publication_attempts: AtomicU64,
    #[cfg(test)]
    injected_flush_unavailable: AtomicU64,
    local_recorder: Mutex<Option<RecorderFileStore>>,
    #[cfg(feature = "sql")]
    qefx_gc_maintenance: tokio::sync::Mutex<()>,
}

/// Marks a Sync coordinator unavailable if its post-commit durability confirmation is dropped.
pub(crate) struct SyncDurabilityConfirmation<'a> {
    coordinator: &'a CheckpointCoordinator,
    armed: bool,
}

impl SyncDurabilityConfirmation<'_> {
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SyncDurabilityConfirmation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.coordinator.mark_unavailable();
        }
    }
}

struct RuntimeCheckpointSnapshot {
    anchor: RecoveryAnchor,
    archive_bytes: Vec<u8>,
}

#[cfg(any(feature = "graph", feature = "kv"))]
struct EngineSnapshotIdentity<'a> {
    cluster_id: &'a str,
    epoch: u64,
    config_id: u64,
    applied_index: LogIndex,
    applied_hash: LogHash,
}

impl CheckpointCoordinator {
    pub async fn open(
        store: ObjectArchiveStore,
        mode: DurabilityMode,
    ) -> Result<Self, DurabilityError> {
        Self::open_with_holder(store, mode, "anonymous-node").await
    }

    pub async fn open_with_holder(
        store: ObjectArchiveStore,
        mode: DurabilityMode,
        holder: impl AsRef<str>,
    ) -> Result<Self, DurabilityError> {
        Self::open_with_holder_and_options(
            store,
            mode,
            holder,
            CheckpointPublisherOptions::default(),
        )
        .await
    }

    pub async fn open_with_holder_and_options(
        store: ObjectArchiveStore,
        mode: DurabilityMode,
        holder: impl AsRef<str>,
        publisher_options: CheckpointPublisherOptions,
    ) -> Result<Self, DurabilityError> {
        mode.validate()?;
        let publisher_lease_renewal_interval =
            publisher_lease_renewal_interval(publisher_options.lease_duration_ms());
        store
            .load_checkpoint()
            .await?
            .ok_or(DurabilityError::MissingCheckpoint)?;
        let identity = store.checkpoint_identity()?;
        let holder = format!(
            "checkpoint-coordinator-{}-{}-{}-{}-{}",
            identity.cluster_id(),
            identity.epoch(),
            identity.config_id(),
            identity.recovery_generation(),
            holder.as_ref()
        );
        let publisher = store
            .open_checkpoint_publisher(holder, publisher_options)
            .await?;
        let durable_tip = load_coordinator_restore_baseline(&store, &publisher).await?;
        Ok(Self {
            store,
            publisher,
            publisher_lease_renewal_interval,
            mode,
            state: Mutex::new(CoordinatorState {
                durable_tip,
                committed_index: durable_tip.index(),
                pending_lag: None,
                health: DurabilityHealth::Available,
            }),
            successor_baseline_required: AtomicBool::new(false),
            publication_attempts: AtomicU64::new(0),
            #[cfg(test)]
            injected_flush_unavailable: AtomicU64::new(0),
            local_recorder: Mutex::new(None),
            #[cfg(feature = "sql")]
            qefx_gc_maintenance: tokio::sync::Mutex::new(()),
        })
    }

    pub async fn open_with_holder_options_local_state(
        store: ObjectArchiveStore,
        mode: DurabilityMode,
        holder: impl AsRef<str>,
        publisher_options: CheckpointPublisherOptions,
        recorder: RecorderFileStore,
        _data_dir: impl AsRef<Path>,
    ) -> Result<Self, DurabilityError> {
        let coordinator =
            Self::open_with_holder_and_options(store, mode, holder, publisher_options).await?;
        coordinator.attach_local_recorder(recorder)?;
        #[cfg(feature = "sql")]
        coordinator
            .retry_pending_qefx_gc_at(_data_dir.as_ref())
            .await?;
        Ok(coordinator)
    }

    pub const fn mode(&self) -> &DurabilityMode {
        &self.mode
    }

    #[doc(hidden)]
    pub(crate) fn sync_durability_confirmation(&self) -> SyncDurabilityConfirmation<'_> {
        SyncDurabilityConfirmation {
            coordinator: self,
            armed: true,
        }
    }

    pub fn durable_tip(&self) -> CheckpointTip {
        self.lock_state().durable_tip
    }

    pub async fn refresh_durable_tip(&self) -> Result<CheckpointTip, DurabilityError> {
        let loaded = self.publisher.observe_checkpoint().await?;
        let accepted = self.publisher.cache_observed_checkpoint(loaded).await?;
        observe_durable_tip(&self.state, *accepted.manifest().tip())
    }

    async fn refresh_after_retryable_flush_error(
        &self,
        error: &DurabilityError,
    ) -> Result<(), DurabilityError> {
        let DurabilityError::Archive(error) = error else {
            return Ok(());
        };
        if !retryable_sync_archive_error(error) {
            return Err(DurabilityError::Archive(error.clone()));
        }
        match self.refresh_durable_tip().await {
            Ok(_) => Ok(()),
            Err(refresh_error) if retryable_sync_recovery_error(&refresh_error) => Ok(()),
            Err(refresh_error) => Err(refresh_error),
        }
    }

    pub fn health(&self) -> DurabilityHealth {
        self.lock_state().health
    }

    #[doc(hidden)]
    pub fn checkpoint_publication_attempts(&self) -> u64 {
        self.publication_attempts.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn inject_flush_unavailable(&self, attempts: u64) {
        self.injected_flush_unavailable
            .store(attempts, Ordering::Release);
    }

    pub fn note_committed(&self, index: LogIndex) {
        let mut state = self.lock_state();
        if index <= state.committed_index {
            return;
        }
        state.committed_index = index;
        if index > state.durable_tip.index() && state.pending_lag.is_none() {
            state.pending_lag = Some(PendingLag::New(Instant::now()));
        }
    }

    pub fn attach_local_recorder(
        &self,
        recorder: RecorderFileStore,
    ) -> Result<(), DurabilityError> {
        let mut slot = self.local_recorder.lock().map_err(|_| {
            DurabilityError::SnapshotVerification(
                "local recorder attachment lock is poisoned".into(),
            )
        })?;
        *slot = Some(recorder);
        Ok(())
    }

    pub fn note_recovered_committed(&self, index: LogIndex) {
        let mut state = self.lock_state();
        state.committed_index = state.committed_index.max(index);
        if state.committed_index > state.durable_tip.index() {
            state.pending_lag = Some(PendingLag::Recovered);
        }
    }

    pub fn write_allowed(&self) -> Result<(), DurabilityError> {
        if self.successor_baseline_required.load(Ordering::Acquire) {
            return Err(DurabilityError::Unavailable);
        }
        if matches!(self.mode, DurabilityMode::Sync)
            && self.health() == DurabilityHealth::Unavailable
        {
            return Err(DurabilityError::Unavailable);
        }
        let DurabilityMode::Bounded { max_lag } = self.mode else {
            return Ok(());
        };
        let state = self.lock_state();
        let exceeded = state.committed_index > state.durable_tip.index()
            && match state.pending_lag {
                Some(PendingLag::Recovered) => true,
                Some(PendingLag::New(pending)) => pending.elapsed() >= max_lag,
                None => false,
            };
        if exceeded {
            return Err(DurabilityError::LagExceeded {
                committed_index: state.committed_index,
                durable_index: state.durable_tip.index(),
                max_lag,
            });
        }
        Ok(())
    }

    /// Prevents writes until a successor runtime has established its own checkpoint baseline.
    #[doc(hidden)]
    pub fn require_successor_checkpoint_baseline(&self) {
        self.successor_baseline_required
            .store(true, Ordering::Release);
    }

    #[doc(hidden)]
    pub fn successor_checkpoint_baseline_required(&self) -> bool {
        self.successor_baseline_required.load(Ordering::Acquire)
    }

    /// Publishes the first target-configuration snapshot after the exact Activate entry.
    ///
    /// A live successor inherits a compacted predecessor qlog, so an empty target archive cannot
    /// flush the missing prefix as ordinary segments. The first target checkpoint is therefore an
    /// active snapshot rooted at the successor's Activate entry. Until this succeeds,
    /// [`Self::write_allowed`] remains closed for every durability mode.
    #[doc(hidden)]
    pub async fn establish_successor_checkpoint_baseline(
        &self,
        runtime: &NodeRuntime,
        predecessor_stop: LogAnchor,
    ) -> Result<RecoveryAnchor, DurabilityError> {
        let (snapshot, _fence) = {
            let _commit = runtime.commit.lock().map_err(|_| {
                DurabilityError::SnapshotVerification("commit mutex is poisoned".into())
            })?;
            runtime
                .ensure_ready()
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            let configuration = runtime
                .configuration_state()
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            if !configuration.is_active() {
                return Err(DurabilityError::SnapshotVerification(
                    "successor checkpoint baseline requires the active target configuration".into(),
                ));
            }
            let stop_entry = runtime
                .config
                .predecessor_stop_entry
                .as_ref()
                .filter(|entry| {
                    LogAnchor::new(entry.index, entry.hash) == predecessor_stop
                        && entry.recompute_hash() == entry.hash
                })
                .ok_or_else(|| {
                    DurabilityError::SnapshotVerification(
                        "successor checkpoint baseline Stop binding changed".into(),
                    )
                })?;
            let stopped = runtime
                .config
                .log_initial_configuration
                .validate_entry(stop_entry)
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            let root = runtime
                .log_root_unlocked()
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            let expected_activation_index =
                predecessor_stop.index().checked_add(1).ok_or_else(|| {
                    DurabilityError::SnapshotVerification(
                        "successor Activate index cannot advance".into(),
                    )
                })?;
            if root.index() != expected_activation_index {
                return Err(DurabilityError::SnapshotVerification(
                    "successor checkpoint baseline is not rooted at the Activate entry".into(),
                ));
            }
            let activation = runtime
                .log_store
                .read(root.index())?
                .filter(|entry| {
                    entry.hash == root.hash() && entry.prev_hash == predecessor_stop.hash()
                })
                .ok_or_else(|| {
                    DurabilityError::SnapshotVerification(
                        "successor checkpoint baseline Activate entry is unavailable".into(),
                    )
                })?;
            let activated = stopped
                .validate_entry(&activation)
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            if activated != configuration {
                return Err(DurabilityError::SnapshotVerification(
                    "successor Activate entry does not produce the active target configuration"
                        .into(),
                ));
            }
            if runtime.checkpointing.swap(true, Ordering::AcqRel) {
                return Err(DurabilityError::SnapshotVerification(
                    "checkpoint transition is already in progress".into(),
                ));
            }
            let fence = CheckpointFence(&runtime.checkpointing);
            let (target, target_hash) = runtime
                .ensure_materialized_tip()
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            if target != root.index() || target_hash != root.hash() {
                return Err(DurabilityError::SnapshotVerification(
                    "materialized successor state does not match the Activate entry".into(),
                ));
            }
            let snapshot =
                create_runtime_checkpoint_snapshot(runtime, target, target_hash, &configuration)?;
            (snapshot, fence)
        };

        let expected_tip = CheckpointTip::new(
            snapshot.anchor.compacted().index(),
            snapshot.anchor.compacted().hash(),
        );
        let durable_tip = self.durable_tip();
        if durable_tip == CheckpointTip::new(0, LogHash::ZERO) {
            self.publisher
                .publish_initial_checkpoint_snapshot(
                    snapshot.anchor.clone(),
                    &snapshot.archive_bytes,
                )
                .await?;
        } else if durable_tip != expected_tip {
            return Err(DurabilityError::SnapshotVerification(
                "target checkpoint namespace conflicts with the successor baseline".into(),
            ));
        }
        let restored = self
            .store
            .load_checkpoint_restore()
            .await?
            .ok_or(DurabilityError::MissingCheckpoint)?;
        let published = restored.restored().snapshot().ok_or_else(|| {
            DurabilityError::SnapshotVerification(
                "successor checkpoint baseline has no snapshot".into(),
            )
        })?;
        if published.anchor() != &snapshot.anchor || published.bytes() != snapshot.archive_bytes {
            return Err(DurabilityError::SnapshotVerification(
                "successor checkpoint baseline read-back differs from the active runtime".into(),
            ));
        }
        {
            let _commit = runtime.commit.lock().map_err(|_| {
                DurabilityError::SnapshotVerification("commit mutex is poisoned".into())
            })?;
            runtime.log_store.compact_prefix(&snapshot.anchor)?;
            runtime
                .compact_embedded_log_before(snapshot.anchor.compacted().index())
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
        }
        self.mark_durable(expected_tip);
        self.successor_baseline_required
            .store(false, Ordering::Release);
        Ok(snapshot.anchor)
    }

    pub async fn flush_runtime(
        &self,
        runtime: &NodeRuntime,
        target_index: LogIndex,
    ) -> Result<CheckpointTip, DurabilityError> {
        let result = self.flush_runtime_inner(runtime, target_index).await;
        if result.is_err() {
            self.mark_unavailable();
        }
        result
    }

    async fn flush_runtime_inner(
        &self,
        runtime: &NodeRuntime,
        target_index: LogIndex,
    ) -> Result<CheckpointTip, DurabilityError> {
        #[cfg(test)]
        if self
            .injected_flush_unavailable
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(DurabilityError::Unavailable);
        }
        let log_state = runtime.log_store().logical_state()?;
        let local_index = log_state.tip.as_ref().map_or(0, |tip| tip.index());
        let mut durable_tip = self.durable_tip();
        if durable_tip.index() > local_index {
            return Err(DurabilityError::ArchiveAheadOfLocal {
                durable_index: durable_tip.index(),
                local_index,
            });
        }
        let target_index = target_index.min(local_index);
        if target_index <= durable_tip.index() {
            return Ok(durable_tip);
        }
        if let Some(anchor) = log_state.anchor {
            if durable_tip.index() < anchor.compacted().index() {
                return Err(DurabilityError::SnapshotRequired {
                    anchor: Box::new(anchor),
                });
            }
        }

        let mut next =
            durable_tip
                .index()
                .checked_add(1)
                .ok_or_else(|| DurabilityError::LocalLogGap {
                    expected: durable_tip.index(),
                    actual: None,
                })?;
        while next <= target_index {
            let end = next
                .saturating_add(FLUSH_BATCH_ENTRIES - 1)
                .min(target_index);
            let entries = runtime
                .log_store()
                .read_range(IndexRange::new(next, end)?)?;
            validate_local_batch(&entries, next, end, durable_tip)?;
            #[cfg(feature = "sql")]
            let effects = self
                .prepare_external_sql_effect_refs(runtime, &entries)
                .await?;
            self.publication_attempts.fetch_add(1, Ordering::Relaxed);
            #[cfg(feature = "sql")]
            let published = self
                .publisher
                .publish_committed_with_effects(&entries, &effects)
                .await?;
            #[cfg(not(feature = "sql"))]
            let published = self.publisher.publish_committed(&entries).await?;
            #[cfg(feature = "sql")]
            if !effects.is_empty() {
                let certificate = self
                    .store
                    .checkpoint_readback_certificate(&published)
                    .await?;
                let configuration = runtime
                    .configuration_state()
                    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
                let _maintenance = self.qefx_gc_maintenance.lock().await;
                persist_pending_qefx_gc(
                    runtime.config.data_dir(),
                    &PendingQefxGc {
                        cluster_id: certificate.identity().cluster_id().to_owned(),
                        epoch: certificate.identity().epoch(),
                        config_id: configuration.config_id(),
                        config_digest: configuration.digest().to_hex(),
                        through_slot: certificate.tip().index(),
                        checkpoint_hash: certificate.tip().hash().to_hex(),
                        manifest_digest: certificate.manifest_digest().to_hex(),
                    },
                )?;
            }
            durable_tip = *published.manifest().tip();
            self.mark_durable(durable_tip);
            if durable_tip.index() >= target_index {
                break;
            }
            next =
                durable_tip
                    .index()
                    .checked_add(1)
                    .ok_or_else(|| DurabilityError::LocalLogGap {
                        expected: durable_tip.index(),
                        actual: None,
                    })?;
        }
        Ok(durable_tip)
    }

    #[cfg(feature = "sql")]
    async fn retry_pending_qefx_gc_at(&self, data_dir: &Path) -> Result<(), DurabilityError> {
        let _maintenance = self.qefx_gc_maintenance.lock().await;
        let Some(pending) = load_pending_qefx_gc(data_dir)? else {
            return Ok(());
        };
        let Some(recorder) = self
            .local_recorder
            .lock()
            .map_err(|_| {
                DurabilityError::SnapshotVerification(
                    "local recorder attachment lock is poisoned".into(),
                )
            })?
            .clone()
        else {
            return Ok(());
        };
        let loaded = self
            .store
            .load_checkpoint()
            .await?
            .ok_or(DurabilityError::MissingCheckpoint)?;
        let certificate = self.store.checkpoint_readback_certificate(&loaded).await?;
        let identity_mismatch = certificate.identity().cluster_id() != pending.cluster_id
            || certificate.identity().epoch() != pending.epoch;
        let visible_tip = certificate.tip().index();
        let rollback = visible_tip < pending.through_slot;
        let same_tip_conflict = visible_tip == pending.through_slot
            && (certificate.identity().config_id() != pending.config_id
                || certificate.identity().config_digest().to_hex() != pending.config_digest
                || certificate.tip().hash().to_hex() != pending.checkpoint_hash
                || certificate.manifest_digest().to_hex() != pending.manifest_digest);
        if identity_mismatch || rollback || same_tip_conflict {
            return Err(DurabilityError::SnapshotVerification(
                "pending QEFX GC evidence no longer matches the visible checkpoint".into(),
            ));
        }
        let outcome = tokio::task::spawn_blocking(move || {
            recorder.advance_effect_bundle_gc_anchor_bounded(
                &certificate,
                &[],
                ONLINE_QEFX_GC_REMOVAL_BUDGET,
            )
        })
        .await
        .map_err(|error| {
            DurabilityError::SnapshotVerification(format!(
                "QEFX GC maintenance task failed: {error}"
            ))
        })?;
        if outcome.is_ok_and(|outcome| outcome.sweep_complete) {
            clear_pending_qefx_gc(data_dir)?;
        }
        Ok(())
    }

    #[cfg(feature = "sql")]
    async fn prepare_external_sql_effect_refs(
        &self,
        runtime: &NodeRuntime,
        entries: &[LogEntry],
    ) -> Result<Vec<rhiza_archive::CheckpointEffectRecord>, DurabilityError> {
        if runtime.config.execution_profile != ExecutionProfile::Sqlite {
            return Ok(Vec::new());
        }
        let mut effects = Vec::new();
        for entry in entries {
            if entry.entry_type != EntryType::Command || entry.payload.is_empty() {
                continue;
            }
            ExternalEffectCommand::decode(&entry.payload).map_err(|error| {
                DurabilityError::SnapshotVerification(format!(
                    "committed SQLite entry is not canonical QEFX during archive publication: {error}"
                ))
            })?;
            let bundle = runtime
                .resolve_external_sql_bundle_for_archive_async(entry)
                .await
                .map_err(|error| match error {
                    crate::NodeError::Unavailable(_) | crate::NodeError::Contention(_) => {
                        DurabilityError::Unavailable
                    }
                    other => DurabilityError::SnapshotVerification(format!(
                        "committed QEFX bundle cannot be resolved for archive publication: {other}"
                    )),
                })?;
            effects.push(
                self.store
                    .publish_verified_qefx_bundle(entry, bundle.chunks())
                    .await?,
            );
        }
        Ok(effects)
    }

    pub async fn checkpoint_compact(
        &self,
        runtime: &NodeRuntime,
    ) -> Result<RecoveryAnchor, DurabilityError> {
        self.checkpoint_compact_inner(runtime, None).await
    }

    pub async fn checkpoint_compact_fenced(
        &self,
        runtime: &NodeRuntime,
        expected_config_id: u64,
        expected_recovery_generation: u64,
        expected_root: LogAnchor,
    ) -> Result<RecoveryAnchor, DurabilityError> {
        self.checkpoint_compact_inner(
            runtime,
            Some((
                expected_config_id,
                expected_recovery_generation,
                expected_root,
            )),
        )
        .await
    }

    async fn checkpoint_compact_inner(
        &self,
        runtime: &NodeRuntime,
        fence: Option<(u64, u64, LogAnchor)>,
    ) -> Result<RecoveryAnchor, DurabilityError> {
        let (target, snapshot, _fence) = {
            let _commit = runtime.commit.lock().map_err(|_| {
                DurabilityError::SnapshotVerification("commit mutex is poisoned".into())
            })?;
            runtime
                .ensure_ready()
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            let configuration = runtime
                .configuration_state()
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            if !configuration.is_active() && configuration.stop().is_none() {
                return Err(DurabilityError::SnapshotVerification(
                    "runtime configuration is not compactable".into(),
                ));
            }
            if let Some((config_id, generation, root)) = fence {
                let actual_config_id = configuration.config_id();
                let actual_generation = runtime.config.recovery_generation();
                let actual_root = runtime.log_root_unlocked().ok();
                if actual_config_id != config_id
                    || actual_generation != generation
                    || actual_root != Some(root)
                {
                    eprintln!(
                        "checkpoint fence mismatch: config {actual_config_id}/{config_id}, \
                         generation {actual_generation}/{generation}, root {actual_root:?}/{root:?}"
                    );
                    return Err(DurabilityError::PreconditionFailed);
                }
            }
            if runtime
                .checkpointing
                .swap(true, std::sync::atomic::Ordering::AcqRel)
            {
                return Err(DurabilityError::SnapshotVerification(
                    "checkpoint transition is already in progress".into(),
                ));
            }
            let fence = CheckpointFence(&runtime.checkpointing);
            let (target, target_hash) = runtime
                .ensure_materialized_tip()
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            let snapshot =
                create_runtime_checkpoint_snapshot(runtime, target, target_hash, &configuration)?;
            (target, snapshot, fence)
        };
        self.flush_runtime(runtime, target).await?;
        let anchor = snapshot.anchor.clone();
        self.publisher
            .publish_checkpoint_snapshot(anchor.clone(), &snapshot.archive_bytes)
            .await?;
        let restored = self
            .store
            .load_checkpoint_restore()
            .await?
            .ok_or(DurabilityError::MissingCheckpoint)?;
        let published = restored.restored().snapshot().ok_or_else(|| {
            DurabilityError::SnapshotVerification("published checkpoint has no snapshot".into())
        })?;
        if published.anchor() != &anchor || published.bytes() != snapshot.archive_bytes {
            return Err(DurabilityError::SnapshotVerification(
                "read-back anchor or snapshot bytes differ".into(),
            ));
        }
        {
            let _commit = runtime.commit.lock().map_err(|_| {
                DurabilityError::SnapshotVerification("commit mutex is poisoned".into())
            })?;
            runtime.log_store.compact_prefix(&anchor)?;
            runtime
                .compact_embedded_log_before(anchor.compacted().index())
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
        }
        self.mark_durable(CheckpointTip::new(
            anchor.compacted().index(),
            anchor.compacted().hash(),
        ));
        Ok(anchor)
    }

    pub async fn run_background<F>(
        self: Arc<Self>,
        runtime: Arc<NodeRuntime>,
        shutdown: F,
    ) -> Result<(), DurabilityError>
    where
        F: Future<Output = ()> + Send,
    {
        let compaction_cadence = match self.mode {
            DurabilityMode::Sync => SYNC_COMPACTION_POLL_INTERVAL,
            DurabilityMode::Bounded { max_lag } => {
                let half = max_lag / 2;
                half.min(Duration::from_secs(1))
            }
            DurabilityMode::Periodic { interval } => interval,
        };
        let now = Instant::now();
        let mut next_publisher_lease_renewal = now;
        let mut heartbeat_retry_delay = SYNC_RECOVERY_RETRY_INITIAL;
        if matches!(self.mode, DurabilityMode::Sync) {
            let attempted_at = Instant::now();
            match self.publisher.renew().await {
                Ok(()) => {
                    next_publisher_lease_renewal = attempted_at
                        .checked_add(self.publisher_lease_renewal_interval)
                        .unwrap_or_else(Instant::now);
                }
                Err(error) if retryable_sync_archive_error(&error) => {
                    next_publisher_lease_renewal = attempted_at
                        .checked_add(heartbeat_retry_delay)
                        .unwrap_or_else(Instant::now);
                    heartbeat_retry_delay = next_sync_recovery_retry(heartbeat_retry_delay);
                }
                Err(error) => return Err(DurabilityError::Archive(error)),
            }
        }
        let mut next_compaction = Instant::now()
            .checked_add(compaction_cadence)
            .unwrap_or_else(Instant::now);
        let mut recovery_retry_at = None;
        let mut recovery_retry_delay = SYNC_RECOVERY_RETRY_INITIAL;
        #[cfg(feature = "sql")]
        let mut next_qefx_gc = now;
        tokio::pin!(shutdown);
        loop {
            let heartbeat_at = if matches!(self.mode, DurabilityMode::Sync) {
                next_publisher_lease_renewal
            } else {
                next_compaction
            };
            let mut wake_at = heartbeat_at.min(next_compaction);
            if let Some(recovery_at) = recovery_retry_at {
                wake_at = wake_at.min(recovery_at);
            }
            #[cfg(feature = "sql")]
            {
                wake_at = wake_at.min(next_qefx_gc);
            }
            tokio::select! {
                () = &mut shutdown => return Ok(()),
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(wake_at)) => {
                    let now = Instant::now();
                    let durability_unavailable =
                        self.health() == DurabilityHealth::Unavailable;
                    if durability_unavailable && recovery_retry_at.is_none() {
                        recovery_retry_at = Some(now);
                    }
                    let recovery_due = recovery_retry_at.is_some_and(|deadline| now >= deadline);
                    #[cfg(feature = "sql")]
                    if now >= next_qefx_gc {
                        // GC is post-durability maintenance. It runs on the
                        // blocking pool in a one-object slice and never changes
                        // Sync health; failures retain the fsynced pending record
                        // for an exact later retry.
                        if let Err(error) = self
                            .retry_pending_qefx_gc_at(runtime.config.data_dir())
                            .await
                        {
                            eprintln!("QEFX GC maintenance deferred: {error}");
                        }
                        next_qefx_gc = Instant::now()
                            .checked_add(ONLINE_QEFX_GC_INTERVAL)
                            .unwrap_or_else(Instant::now);
                    }
                    if matches!(self.mode, DurabilityMode::Sync)
                        && (recovery_due || now >= next_publisher_lease_renewal)
                    {
                        let attempted_at = Instant::now();
                        match self.publisher.renew().await {
                            Ok(()) => {
                                next_publisher_lease_renewal = attempted_at
                                    .checked_add(self.publisher_lease_renewal_interval)
                                    .unwrap_or_else(Instant::now);
                                heartbeat_retry_delay = SYNC_RECOVERY_RETRY_INITIAL;
                            }
                            Err(error) if retryable_sync_archive_error(&error) => {
                                next_publisher_lease_renewal = attempted_at
                                    .checked_add(heartbeat_retry_delay)
                                    .unwrap_or_else(Instant::now);
                                heartbeat_retry_delay =
                                    next_sync_recovery_retry(heartbeat_retry_delay);
                            }
                            Err(error) => return Err(DurabilityError::Archive(error)),
                        }
                    }
                    if recovery_due && self.health() == DurabilityHealth::Unavailable {
                        match self.flush_runtime(&runtime, LogIndex::MAX).await {
                            Ok(_) => {
                                recovery_retry_at = None;
                                recovery_retry_delay = SYNC_RECOVERY_RETRY_INITIAL;
                            }
                            Err(error) if retryable_sync_recovery_error(&error) => {
                                eprintln!("durability recovery deferred: {error}");
                                self.refresh_after_retryable_flush_error(&error).await?;
                                if self.health() == DurabilityHealth::Available {
                                    recovery_retry_at = None;
                                    recovery_retry_delay = SYNC_RECOVERY_RETRY_INITIAL;
                                } else {
                                    recovery_retry_at =
                                        Instant::now().checked_add(recovery_retry_delay);
                                    recovery_retry_delay =
                                        next_sync_recovery_retry(recovery_retry_delay);
                                }
                            }
                            Err(error) => return Err(error),
                        }
                    } else if !durability_unavailable {
                        recovery_retry_at = None;
                        recovery_retry_delay = SYNC_RECOVERY_RETRY_INITIAL;
                    }
                    if !matches!(self.mode, DurabilityMode::Sync)
                        && self.health() == DurabilityHealth::Available
                    {
                        match self.flush_runtime(&runtime, LogIndex::MAX).await {
                            Ok(_) => {}
                            Err(error) if retryable_sync_recovery_error(&error) => {
                                eprintln!("durability recovery scheduled: {error}");
                                self.refresh_after_retryable_flush_error(&error).await?;
                                if self.health() == DurabilityHealth::Unavailable {
                                    recovery_retry_at =
                                        Instant::now().checked_add(recovery_retry_delay);
                                    recovery_retry_delay =
                                        next_sync_recovery_retry(recovery_retry_delay);
                                }
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    if self.health() == DurabilityHealth::Available
                        && now >= next_compaction
                        && self.publisher.compaction_recommended().await
                    {
                        match self.checkpoint_compact(&runtime).await {
                            Ok(_) => {}
                            Err(DurabilityError::Archive(_) | DurabilityError::Io(_)) => {
                                let _ = self.publisher.reload().await;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    if now >= next_compaction {
                        next_compaction = Instant::now()
                            .checked_add(compaction_cadence)
                            .unwrap_or_else(Instant::now);
                    }
                }
            }
        }
    }

    fn mark_durable(&self, durable_tip: CheckpointTip) {
        let mut state = self.lock_state();
        mark_durable_state(&mut state, durable_tip);
    }

    fn mark_unavailable(&self) {
        let mut state = self.lock_state();
        if state.committed_index > state.durable_tip.index() {
            state.health = DurabilityHealth::Unavailable;
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, CoordinatorState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Loads one manifest/state pair and makes that exact pair the coordinator's
/// startup baseline. The publisher cache is observational state: another
/// publisher may advance the archive after this publisher was opened, so a
/// separately sampled cached tip must never be compared with the restored
/// tip for equality.
async fn load_coordinator_restore_baseline(
    store: &ObjectArchiveStore,
    publisher: &CheckpointPublisher,
) -> Result<CheckpointTip, DurabilityError> {
    let restored = store
        .load_checkpoint_restore()
        .await?
        .ok_or(DurabilityError::MissingCheckpoint)?;
    let (loaded, restored) = restored.into_parts();
    let durable_tip = *restored.tip();
    if loaded.manifest().tip() != &durable_tip {
        return Err(DurabilityError::Archive(
            rhiza_archive::Error::InvalidCheckpoint(
                "restored state does not match its loaded manifest".into(),
            ),
        ));
    }
    publisher.cache_observed_checkpoint(loaded).await?;
    Ok(durable_tip)
}

fn create_runtime_checkpoint_snapshot(
    runtime: &NodeRuntime,
    target: LogIndex,
    target_hash: LogHash,
    configuration: &ConfigurationState,
) -> Result<RuntimeCheckpointSnapshot, DurabilityError> {
    #[cfg(not(any(feature = "graph", feature = "kv")))]
    let _ = target_hash;
    let materializer = runtime
        .lock_materializer()
        .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
    match &*materializer {
        #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
        Materializer::Unavailable => unreachable!("no execution profiles are compiled in"),
        #[cfg(feature = "sql")]
        Materializer::Sql(state) => {
            let snapshot = state
                .create_recovery_snapshot(runtime.config().recovery_generation())
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            if snapshot.anchor().compacted().index() != target
                || snapshot.anchor().configuration_state() != configuration
            {
                return Err(DurabilityError::SnapshotVerification(
                    "SQLite snapshot does not match the compacted runtime state".into(),
                ));
            }
            Ok(RuntimeCheckpointSnapshot {
                anchor: snapshot.anchor().clone(),
                archive_bytes: snapshot.db_bytes().to_vec(),
            })
        }
        #[cfg(feature = "graph")]
        Materializer::Graph(state) => {
            let snapshot = state
                .create_snapshot(target)
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            validate_engine_snapshot_identity(
                runtime,
                configuration,
                EngineSnapshotIdentity {
                    cluster_id: snapshot.cluster_id(),
                    epoch: snapshot.epoch(),
                    config_id: snapshot.config_id(),
                    applied_index: snapshot.applied_index(),
                    applied_hash: snapshot.applied_hash(),
                },
                target,
                target_hash,
            )?;
            let archive_bytes = encode_graph_snapshot(&snapshot)
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            Ok(RuntimeCheckpointSnapshot {
                anchor: engine_recovery_anchor(
                    runtime,
                    configuration,
                    target,
                    snapshot.applied_hash(),
                    snapshot.materializer_fingerprint(),
                    &archive_bytes,
                )?,
                archive_bytes,
            })
        }
        #[cfg(feature = "kv")]
        Materializer::Kv(state) => {
            let snapshot = state
                .create_snapshot(target)
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            validate_engine_snapshot_identity(
                runtime,
                configuration,
                EngineSnapshotIdentity {
                    cluster_id: snapshot.cluster_id(),
                    epoch: snapshot.epoch(),
                    config_id: snapshot.config_id(),
                    applied_index: snapshot.applied_index(),
                    applied_hash: snapshot.applied_hash(),
                },
                target,
                target_hash,
            )?;
            let archive_bytes = encode_kv_snapshot(&snapshot)
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            Ok(RuntimeCheckpointSnapshot {
                anchor: engine_recovery_anchor(
                    runtime,
                    configuration,
                    target,
                    snapshot.applied_hash(),
                    snapshot.materializer_fingerprint(),
                    &archive_bytes,
                )?,
                archive_bytes,
            })
        }
    }
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn validate_engine_snapshot_identity(
    runtime: &NodeRuntime,
    configuration: &ConfigurationState,
    snapshot: EngineSnapshotIdentity<'_>,
    expected_index: LogIndex,
    expected_hash: LogHash,
) -> Result<(), DurabilityError> {
    let config = runtime.config();
    if snapshot.cluster_id != config.cluster_id()
        || snapshot.epoch != config.epoch()
        || snapshot.config_id != configuration.config_id()
        || snapshot.applied_index == 0
        || snapshot.applied_index != expected_index
        || snapshot.applied_hash != expected_hash
    {
        return Err(DurabilityError::SnapshotVerification(
            "engine snapshot identity does not match the compacted runtime state".into(),
        ));
    }
    Ok(())
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn engine_recovery_anchor(
    runtime: &NodeRuntime,
    configuration: &ConfigurationState,
    applied_index: LogIndex,
    applied_hash: LogHash,
    materializer_fingerprint: LogHash,
    archive_bytes: &[u8],
) -> Result<RecoveryAnchor, DurabilityError> {
    let size_bytes = u64::try_from(archive_bytes.len()).map_err(|_| {
        DurabilityError::SnapshotVerification("snapshot envelope size exceeds u64".into())
    })?;
    Ok(RecoveryAnchor::new(
        runtime.config().cluster_id(),
        runtime.config().epoch(),
        configuration.clone(),
        runtime.config().recovery_generation(),
        LogAnchor::new(applied_index, applied_hash),
        SnapshotIdentity::new(
            format!("snapshot-{applied_index:020}"),
            LogHash::digest(&[archive_bytes]),
            size_bytes,
            materializer_fingerprint,
        ),
    ))
}

fn mark_durable_state(state: &mut CoordinatorState, durable_tip: CheckpointTip) {
    if durable_tip.index() > state.durable_tip.index() {
        state.durable_tip = durable_tip;
    }
    if state.committed_index <= state.durable_tip.index() {
        state.pending_lag = None;
        state.health = DurabilityHealth::Available;
    }
}

fn observe_durable_tip(
    state: &Mutex<CoordinatorState>,
    observed: CheckpointTip,
) -> Result<CheckpointTip, DurabilityError> {
    let mut state = state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let current = state.durable_tip;
    if observed.index() < current.index() {
        return Err(DurabilityError::SnapshotVerification(format!(
            "checkpoint tip rolled back from index {} to {}",
            current.index(),
            observed.index()
        )));
    }
    if observed.index() == current.index() && observed.hash() != current.hash() {
        return Err(DurabilityError::SnapshotVerification(format!(
            "checkpoint tip hash changed at index {}",
            observed.index()
        )));
    }
    mark_durable_state(&mut state, observed);
    Ok(state.durable_tip)
}

struct CheckpointFence<'a>(&'a std::sync::atomic::AtomicBool);

impl Drop for CheckpointFence<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// Downloads and validates one remote checkpoint without touching local data.
///
/// The archive layer pins the manifest and all restored bytes under one reader
/// lease. Local installers borrow this value so an interrupted install retries
/// the exact same remote checkpoint without re-fetching or copying it.
pub async fn prepare_checkpoint_restore(
    store: &ObjectArchiveStore,
) -> Result<PreparedCheckpointRestore, DurabilityError> {
    let loaded_restore = store
        .load_checkpoint_restore()
        .await?
        .ok_or(DurabilityError::MissingCheckpoint)?;
    let (loaded, restored) = loaded_restore.into_parts();
    let identity = loaded.manifest().identity().clone();
    let execution_profile = snapshot_profile(identity.cluster_id())?;
    let checkpoint_root = LogAnchor::new(restored.tip().index(), restored.tip().hash());
    if loaded.manifest().tip() != restored.tip() {
        return Err(DurabilityError::SnapshotVerification(
            "loaded checkpoint manifest tip does not match restored checkpoint tip".into(),
        ));
    }
    #[cfg(feature = "sql")]
    let external_sql_effects = prepare_checkpoint_external_sql_effects(
        store,
        loaded.manifest(),
        restored.suffix(),
        execution_profile,
    )
    .await?;
    let prepared = PreparedCheckpointRestore {
        identity,
        execution_profile,
        restored,
        checkpoint_root,
        #[cfg(feature = "sql")]
        external_sql_effects,
    };
    prepared.validate()?;
    Ok(prepared)
}

#[cfg(feature = "sql")]
async fn prepare_checkpoint_external_sql_effects(
    store: &ObjectArchiveStore,
    manifest: &rhiza_archive::CheckpointManifest,
    suffix: &[LogEntry],
    profile: ExecutionProfile,
) -> Result<Vec<PreparedCheckpointEffect>, DurabilityError> {
    let mut refs = BTreeMap::new();
    for segment in manifest.segments() {
        for effect in segment.effects() {
            if refs.insert(effect.entry_index(), effect).is_some() {
                return Err(DurabilityError::SnapshotVerification(
                    "checkpoint has duplicate QEFX effect references".into(),
                ));
            }
        }
    }
    if profile != ExecutionProfile::Sqlite {
        if refs.is_empty() {
            return Ok(Vec::new());
        }
        return Err(DurabilityError::SnapshotVerification(
            "non-SQL checkpoint has QEFX effect references".into(),
        ));
    }

    let mut configuration = manifest
        .base()
        .snapshot()
        .map(|snapshot| snapshot.anchor().configuration_state().clone())
        .unwrap_or_else(|| checkpoint_identity_configuration(manifest.identity()));
    let mut qefx_entries = BTreeMap::new();
    for entry in suffix {
        if entry.entry_type == EntryType::Command && !entry.payload.is_empty() {
            let command = ExternalEffectCommand::decode(&entry.payload).map_err(|error| {
                DurabilityError::SnapshotVerification(format!(
                    "checkpoint SQL suffix entry is not canonical QEFX: {error}"
                ))
            })?;
            if command.intended_slot() != entry.index
                || command.cluster_id() != entry.cluster_id
                || command.epoch() != entry.epoch
                || command.config_id() != entry.config_id
                || command.config_id() != configuration.config_id()
                || command.config_digest() != configuration.digest()
                || command.prev_hash() != entry.prev_hash
            {
                return Err(DurabilityError::SnapshotVerification(
                    "checkpoint QEFX command does not match its suffix entry".into(),
                ));
            }
            if qefx_entries.insert(entry.index, command).is_some() {
                return Err(DurabilityError::SnapshotVerification(
                    "checkpoint suffix repeats a QEFX entry index".into(),
                ));
            }
        }
        configuration = configuration.validate_entry(entry).map_err(|error| {
            DurabilityError::SnapshotVerification(format!(
                "checkpoint suffix configuration transition is invalid: {error}"
            ))
        })?;
    }
    if refs.len() != qefx_entries.len() || refs.keys().copied().ne(qefx_entries.keys().copied()) {
        return Err(DurabilityError::SnapshotVerification(
            "checkpoint QEFX suffix entries and effect references are not bijective".into(),
        ));
    }

    let mut total = 0usize;
    let mut prepared = Vec::with_capacity(qefx_entries.len());
    for (index, command) in qefx_entries {
        let reference = refs.get(&index).expect("bijection checked above");
        let restored = store.restore_checkpoint_effect(reference).await?;
        if restored.manifest()
            != command.encode().map_err(|error| {
                DurabilityError::SnapshotVerification(format!(
                    "checkpoint QEFX encode failed: {error}"
                ))
            })?
        {
            return Err(DurabilityError::SnapshotVerification(
                "checkpoint QEFX manifest differs from suffix entry".into(),
            ));
        }
        let effect_bytes = restored.chunks().iter().try_fold(0usize, |sum, chunk| {
            sum.checked_add(chunk.len()).ok_or_else(|| {
                DurabilityError::SnapshotVerification(
                    "checkpoint QEFX aggregate byte count overflows".into(),
                )
            })
        })?;
        total = total.checked_add(effect_bytes).ok_or_else(|| {
            DurabilityError::SnapshotVerification(
                "checkpoint QEFX aggregate byte count overflows".into(),
            )
        })?;
        let mut bytes = Vec::with_capacity(effect_bytes);
        for chunk in restored.chunks() {
            bytes.extend_from_slice(chunk);
        }
        let effect = QwalEffectManifestV4::verify_external_bundle(&command, &bytes)
            .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
        prepared.push(PreparedCheckpointEffect {
            entry_index: index,
            effect,
            manifest: restored.manifest().to_vec(),
            chunks: restored.chunks().to_vec(),
        });
    }
    let _ = total;
    Ok(prepared)
}

/// Synchronously installs a prepared checkpoint into a fresh node directory.
///
/// The configured data-directory parent is trusted by deployment configuration.
/// Protecting against a hostile ancestor replacement would require a separate
/// descriptor-relative (`openat`) filesystem design; this installer rejects a
/// symlink or non-directory final path before making any local mutation.
pub fn install_prepared_checkpoint_to_fresh_data_dir(
    prepared: &PreparedCheckpointRestore,
    expected: ExpectedLocalRestoreState,
    completion_marker: Option<RestoreCompletionMarker<'_>>,
) -> Result<CheckpointTip, DurabilityError> {
    install_prepared_checkpoint(
        prepared,
        expected,
        CheckpointInstallMode::Fresh,
        completion_marker,
    )
}

fn install_prepared_checkpoint(
    prepared: &PreparedCheckpointRestore,
    expected: ExpectedLocalRestoreState,
    mode: CheckpointInstallMode,
    completion_marker: Option<RestoreCompletionMarker<'_>>,
) -> Result<CheckpointTip, DurabilityError> {
    validate_expected_restore_request(&expected, prepared, mode, completion_marker.as_ref())?;
    let _lock = expected.acquire_lock_and_revalidate()?;
    #[cfg(test)]
    test_restore_lock_acquired(&expected.data_dir);
    if let Some(tip) = finalize_completed_restore_if_present(
        prepared,
        &expected,
        mode,
        completion_marker.as_ref(),
    )? {
        return Ok(tip);
    }
    let data_dir = expected.data_dir.as_path();
    let target_node_id = expected.target_node_id.as_str();
    validate_prepared_fresh_install(
        prepared,
        data_dir,
        target_node_id,
        completion_marker.as_ref(),
    )?;
    let marker = completion_marker
        .as_ref()
        .map(RestoreCompletionMarker::as_parts);
    let intent = checkpoint_restore_intent_bytes(
        prepared.identity(),
        target_node_id,
        prepared.execution_profile(),
        prepared.checkpoint_root(),
    )?;
    let recovery_identity = RecoveryArtifactIdentity::Restore(restore_intent_identity(
        prepared.identity(),
        target_node_id,
        prepared.execution_profile(),
        prepared.checkpoint_root(),
    ));
    // The validation above precedes every create/remove/rename. The existing
    // recovery artifact protocol then preserves its crash-retriable semantics.
    prepare_fresh_restore_data_dir(data_dir, marker.map(|(name, _)| name), &intent)?;
    publish_restore_marker(data_dir, RESTORE_INTENT_FILE, &intent)?;
    install_restored_checkpoint(
        prepared.identity(),
        prepared.restored(),
        #[cfg(feature = "sql")]
        &prepared.external_sql_effects,
        data_dir,
        RestoreInstallOptions {
            target_node_id: Some(target_node_id),
            replace_rebuildable: false,
            recovery_identity: Some(&recovery_identity),
            completion_marker: marker,
        },
    )?;
    publish_restore_receipt(data_dir, prepared, mode, target_node_id, marker)?;
    fs::remove_file(data_dir.join(RESTORE_INTENT_FILE))?;
    sync_directory(data_dir)?;
    Ok(*prepared.restored().tip())
}

fn restore_intent_identity(
    identity: &CheckpointIdentity,
    node_id: &str,
    execution_profile: ExecutionProfile,
    checkpoint_root: LogAnchor,
) -> RestoreIntentIdentity {
    RestoreIntentIdentity {
        cluster_id: identity.cluster_id().to_owned(),
        node_id: node_id.to_owned(),
        execution_profile,
        epoch: identity.epoch(),
        config_id: identity.config_id(),
        recovery_generation: identity.recovery_generation(),
        checkpoint_index: checkpoint_root.index(),
        checkpoint_hash: checkpoint_root.hash().to_hex(),
    }
}

/// Encodes the byte-exact restore-intent protocol used to resume an
/// interrupted local checkpoint installation.
///
/// Callers that persist the intent must use these bytes verbatim: recovery
/// compares the durable file byte-for-byte before permitting cleanup.
pub fn checkpoint_restore_intent_bytes(
    identity: &CheckpointIdentity,
    node_id: &str,
    execution_profile: ExecutionProfile,
    checkpoint_root: LogAnchor,
) -> Result<Vec<u8>, DurabilityError> {
    serde_json::to_vec(&restore_intent_identity(
        identity,
        node_id,
        execution_profile,
        checkpoint_root,
    ))
    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))
}

fn parse_restore_intent_identity(bytes: &[u8]) -> Option<RestoreIntentIdentity> {
    let intent = serde_json::from_slice::<RestoreIntentIdentity>(bytes).ok()?;
    (!intent.cluster_id.is_empty()
        && !intent.node_id.is_empty()
        && LogHash::from_hex(&intent.checkpoint_hash).is_some())
    .then_some(intent)
}

pub fn checkpoint_restore_in_progress(
    data_dir: impl AsRef<Path>,
    identity: &CheckpointIdentity,
    node_id: &str,
    execution_profile: ExecutionProfile,
    checkpoint_root: LogAnchor,
) -> Result<CheckpointRestoreState, DurabilityError> {
    let data_dir = data_dir.as_ref();
    let intent = data_dir.join(RESTORE_INTENT_FILE);
    let metadata = match fs::symlink_metadata(&intent) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let Some(metadata) = metadata else {
        return Ok(CheckpointRestoreState::None);
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 4096 {
        return Err(DurabilityError::SnapshotVerification(
            "local checkpoint restore intent is invalid".into(),
        ));
    }
    let bytes = read_bounded_regular_file(&intent, 4096)?.ok_or_else(|| {
        DurabilityError::SnapshotVerification("local checkpoint restore intent disappeared".into())
    })?;
    let actual: RestoreIntentIdentity = serde_json::from_slice(&bytes).map_err(|_| {
        DurabilityError::SnapshotVerification("local checkpoint restore intent is invalid".into())
    })?;
    let expected = restore_intent_identity(identity, node_id, execution_profile, checkpoint_root);
    if actual.cluster_id != expected.cluster_id
        || actual.node_id != expected.node_id
        || actual.execution_profile != expected.execution_profile
        || actual.epoch != expected.epoch
        || actual.config_id != expected.config_id
        || actual.recovery_generation != expected.recovery_generation
        || actual.checkpoint_index != expected.checkpoint_index
        || actual.checkpoint_hash != expected.checkpoint_hash
    {
        return Err(DurabilityError::SnapshotVerification(
            "local checkpoint restore intent does not exactly match this node and checkpoint"
                .into(),
        ));
    }
    Ok(CheckpointRestoreState::IdentityBound)
}

pub fn validate_local_recovery_view(
    data_dir: impl AsRef<Path>,
    identity: &CheckpointIdentity,
    target_node_id: &str,
    execution_profile: ExecutionProfile,
    checkpoint_root: LogAnchor,
) -> Result<(), DurabilityError> {
    let data_dir = data_dir.as_ref();
    if checkpoint_restore_in_progress(
        data_dir,
        identity,
        target_node_id,
        execution_profile,
        checkpoint_root,
    )? != CheckpointRestoreState::None
    {
        return Err(DurabilityError::SnapshotVerification(
            "local recovery view has an incomplete checkpoint restore intent".into(),
        ));
    }
    #[cfg(not(feature = "kv"))]
    let _ = target_node_id;
    if snapshot_profile(identity.cluster_id())? != execution_profile {
        return Err(DurabilityError::SnapshotVerification(
            "local recovery view profile does not match checkpoint identity".into(),
        ));
    }
    let recovery_identity = RecoveryArtifactIdentity::Restore(restore_intent_identity(
        identity,
        target_node_id,
        execution_profile,
        checkpoint_root,
    ));
    cleanup_owned_recovery_artifacts(data_dir, &recovery_identity)?;
    let validate_qlog = validate_local_materializer_identity(
        data_dir,
        identity,
        target_node_id,
        execution_profile,
    )?;

    if validate_qlog {
        // NodeRuntime reconciles valid materializer/qlog crash skew. This preflight only opens the
        // expected local identity and fences startup to an exactly included authoritative root.
        validate_local_qlog(data_dir, identity, checkpoint_root)?;
    }
    Ok(())
}

fn validate_local_materializer_identity(
    data_dir: &Path,
    identity: &CheckpointIdentity,
    target_node_id: &str,
    execution_profile: ExecutionProfile,
) -> Result<bool, DurabilityError> {
    Ok(match execution_profile {
        ExecutionProfile::Sqlite => {
            #[cfg(feature = "sql")]
            {
                let path = data_dir.join("sqlite/db.sqlite");
                if !fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.is_file()) {
                    return Err(DurabilityError::SnapshotVerification(
                        "SQL materializer is missing or is not a regular file".into(),
                    ));
                }
                let _state = rhiza_sql::SqliteStateMachine::open(
                    path,
                    identity.cluster_id(),
                    target_node_id,
                    identity.epoch(),
                    identity.config_id(),
                )
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
                true
            }
            #[cfg(not(feature = "sql"))]
            return Err(DurabilityError::SnapshotVerification(
                "sql execution profile is not compiled in".into(),
            ));
        }
        ExecutionProfile::Kv => {
            #[cfg(feature = "kv")]
            {
                let path = data_dir.join("kv/data.redb");
                if !fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.is_file()) {
                    return Err(DurabilityError::SnapshotVerification(
                        "KV materializer is missing or is not a regular file".into(),
                    ));
                }
                let _state = RedbStateMachine::open(
                    &path,
                    identity.cluster_id(),
                    target_node_id,
                    identity.epoch(),
                    identity.config_id(),
                )
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
                true
            }
            #[cfg(not(feature = "kv"))]
            return Err(DurabilityError::SnapshotVerification(
                "kv execution profile is not compiled in".into(),
            ));
        }
        ExecutionProfile::Graph => {
            #[cfg(feature = "graph")]
            {
                let path = data_dir.join("ladybug/graph.lbug");
                if !fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.is_file()) {
                    return Err(DurabilityError::SnapshotVerification(
                        "Graph materializer is missing or is not a regular file".into(),
                    ));
                }
                let _state = LadybugStateMachine::open(
                    path,
                    identity.cluster_id(),
                    target_node_id,
                    identity.epoch(),
                    identity.config_id(),
                )
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
                true
            }
            #[cfg(not(feature = "graph"))]
            return Err(DurabilityError::SnapshotVerification(
                "graph execution profile is not compiled in".into(),
            ));
        }
    })
}

/// Synchronously replaces only the rebuildable recovery view of a rejoining
/// node. Recorder files are intentionally not part of the quarantine or
/// promotion set and therefore survive byte-for-byte across the install.
pub fn install_prepared_checkpoint_for_rejoin_preserving_recorder(
    prepared: &PreparedCheckpointRestore,
    expected: ExpectedLocalRestoreState,
    completion_marker: RestoreCompletionMarker<'_>,
) -> Result<CheckpointTip, DurabilityError> {
    install_prepared_checkpoint_for_rejoin(prepared, expected, completion_marker)
}

fn install_prepared_checkpoint_for_rejoin(
    prepared: &PreparedCheckpointRestore,
    expected: ExpectedLocalRestoreState,
    completion_marker: RestoreCompletionMarker<'_>,
) -> Result<CheckpointTip, DurabilityError> {
    validate_expected_restore_request(
        &expected,
        prepared,
        CheckpointInstallMode::RejoinPreservingRecorder,
        Some(&completion_marker),
    )?;
    let _lock = expected.acquire_lock_and_revalidate()?;
    #[cfg(test)]
    test_restore_lock_acquired(&expected.data_dir);
    if let Some(tip) = finalize_completed_restore_if_present(
        prepared,
        &expected,
        CheckpointInstallMode::RejoinPreservingRecorder,
        Some(&completion_marker),
    )? {
        return Ok(tip);
    }
    let data_dir = expected.data_dir.as_path();
    let target_node_id = expected.target_node_id.as_str();
    let execution_profile = expected.execution_profile;
    validate_prepared_rejoin_install(
        prepared,
        data_dir,
        target_node_id,
        execution_profile,
        &completion_marker,
    )?;
    let completion_marker = completion_marker.as_parts();
    let intent = checkpoint_restore_intent_bytes(
        prepared.identity(),
        target_node_id,
        execution_profile,
        prepared.checkpoint_root(),
    )?;
    let recovery_identity = RecoveryArtifactIdentity::Restore(restore_intent_identity(
        prepared.identity(),
        target_node_id,
        execution_profile,
        prepared.checkpoint_root(),
    ));
    cleanup_owned_recovery_artifacts(data_dir, &recovery_identity)?;
    publish_restore_marker(data_dir, RESTORE_INTENT_FILE, &intent)?;
    install_restored_checkpoint(
        prepared.identity(),
        prepared.restored(),
        #[cfg(feature = "sql")]
        &prepared.external_sql_effects,
        data_dir,
        RestoreInstallOptions {
            target_node_id: Some(target_node_id),
            replace_rebuildable: true,
            recovery_identity: Some(&recovery_identity),
            completion_marker: Some(completion_marker),
        },
    )?;
    publish_restore_receipt(
        data_dir,
        prepared,
        CheckpointInstallMode::RejoinPreservingRecorder,
        target_node_id,
        Some(completion_marker),
    )?;
    fs::remove_file(data_dir.join(RESTORE_INTENT_FILE))?;
    sync_directory(data_dir)?;
    Ok(*prepared.restored().tip())
}

impl ExpectedLocalRestoreState {
    fn acquire_lock_and_revalidate(&self) -> Result<crate::NodeDataRootLock, DurabilityError> {
        self.revalidate_parent_and_data_dir_before_lock()?;
        let created_data_dir_identity =
            if matches!(&self.data_dir_identity, ExpectedPathIdentity::Missing) {
                match fs::create_dir(&self.data_dir) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        return Err(DurabilityError::SnapshotVerification(
                        "expected missing restore data directory appeared before lock acquisition"
                            .into(),
                    ));
                    }
                    Err(error) => return Err(error.into()),
                }
                sync_directory(self.data_dir.parent().unwrap_or_else(|| Path::new(".")))?;
                let metadata = fs::symlink_metadata(&self.data_dir)?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(DurabilityError::SnapshotVerification(
                        "installer-created restore data directory is invalid".into(),
                    ));
                }
                Some(PathIdentity::capture(&self.data_dir, &metadata)?)
            } else {
                None
            };
        let lock = crate::acquire_node_data_root_lock(&self.data_dir).map_err(|error| {
            DurabilityError::SnapshotVerification(format!(
                "cannot acquire the node data-root restore lock: {error}"
            ))
        })?;
        if matches!(&self.lock, ExpectedRegularFile::Missing) && !lock.was_created() {
            return Err(DurabilityError::SnapshotVerification(
                "expected missing node data lock appeared before lock acquisition".into(),
            ));
        }
        #[cfg(test)]
        test_restore_lock_before_path_revalidation(&self.data_dir);
        self.revalidate_under_lock(created_data_dir_identity.as_ref(), &lock)?;
        Ok(lock)
    }

    fn revalidate_parent_and_data_dir_before_lock(&self) -> Result<(), DurabilityError> {
        let parent = self.data_dir.parent().unwrap_or_else(|| Path::new("."));
        let parent_metadata = fs::symlink_metadata(parent)?;
        if parent_metadata.file_type().is_symlink()
            || !parent_metadata.is_dir()
            || !self
                .parent
                .same_path_identity(&PathIdentity::capture(parent, &parent_metadata)?)
        {
            return Err(DurabilityError::SnapshotVerification(
                "restore data directory parent changed after expected state capture".into(),
            ));
        }
        match (
            &self.data_dir_identity,
            fs::symlink_metadata(&self.data_dir),
        ) {
            (ExpectedPathIdentity::Missing, Err(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(())
            }
            (ExpectedPathIdentity::Directory(expected), Ok(actual))
                if !actual.file_type().is_symlink()
                    && actual.is_dir()
                    && expected
                        .same_path_identity(&PathIdentity::capture(&self.data_dir, &actual)?) =>
            {
                Ok(())
            }
            _ => Err(DurabilityError::SnapshotVerification(
                "restore data directory changed after expected state capture".into(),
            )),
        }
    }

    fn revalidate_under_lock(
        &self,
        created_data_dir_identity: Option<&PathIdentity>,
        lock: &crate::NodeDataRootLock,
    ) -> Result<(), DurabilityError> {
        lock.revalidate_path(&self.data_dir.join(crate::NODE_DATA_ROOT_LOCK_FILE))
            .map_err(|error| {
                DurabilityError::SnapshotVerification(format!(
                    "cannot revalidate the node data-root restore lock: {error}"
                ))
            })?;
        let data_metadata = fs::symlink_metadata(&self.data_dir)?;
        if data_metadata.file_type().is_symlink() || !data_metadata.is_dir() {
            return Err(DurabilityError::SnapshotVerification(
                "restore data directory changed to an invalid form under lock".into(),
            ));
        }
        match (&self.data_dir_identity, created_data_dir_identity) {
            (ExpectedPathIdentity::Directory(expected), _)
                if !expected.same_path_identity(&PathIdentity::capture(
                    &self.data_dir,
                    &data_metadata,
                )?) =>
            {
                return Err(DurabilityError::SnapshotVerification(
                    "restore data directory identity changed under lock".into(),
                ));
            }
            (ExpectedPathIdentity::Missing, Some(created))
                if !created.same_path_identity(&PathIdentity::capture(
                    &self.data_dir,
                    &data_metadata,
                )?) =>
            {
                return Err(DurabilityError::SnapshotVerification(
                    "installer-created restore data directory changed before lock revalidation"
                        .into(),
                ));
            }
            (ExpectedPathIdentity::Missing, None) => {
                return Err(DurabilityError::SnapshotVerification(
                    "missing restore data directory was not created by this installer".into(),
                ));
            }
            _ => {}
        }
        let current_lock =
            capture_restore_regular_file(&self.data_dir.join(crate::NODE_DATA_ROOT_LOCK_FILE))?;
        match (&self.lock, &current_lock) {
            (ExpectedRegularFile::Missing, ExpectedRegularFile::Exact { identity, bytes })
                if identity.kind == ExpectedEntryKind::RegularFile && bytes.is_empty() => {}
            (expected, current) if expected == current => {}
            _ => {
                return Err(DurabilityError::SnapshotVerification(
                    "node data lock changed after expected state capture".into(),
                ));
            }
        }
        self.revalidate_control_file(
            self.completion_marker_name.as_deref(),
            &self.completion_marker,
        )?;
        self.revalidate_control_file(Some(RESTORE_INTENT_FILE), &self.restore_intent)?;
        self.revalidate_control_file(Some(RESTORE_RECEIPT_FILE), &self.restore_receipt)?;
        if capture_recovery_artifact_set(&self.data_dir)? != self.recovery_artifacts {
            return Err(DurabilityError::SnapshotVerification(
                "recovery artifact set changed after expected state capture".into(),
            ));
        }
        let current_qlog = capture_expected_qlog_state(
            &self.data_dir,
            &self.identity,
            self.initial_configuration.clone(),
        )?;
        if current_qlog != self.qlog {
            return Err(DurabilityError::SnapshotVerification(
                "local qlog state changed after expected state capture".into(),
            ));
        }
        Ok(())
    }

    fn revalidate_control_file(
        &self,
        name: Option<&str>,
        expected: &ExpectedRegularFile,
    ) -> Result<(), DurabilityError> {
        let Some(name) = name else {
            return Ok(());
        };
        if capture_restore_regular_file(&self.data_dir.join(name))? != *expected {
            return Err(DurabilityError::SnapshotVerification(format!(
                "restore control file {name} changed after expected state capture"
            )));
        }
        Ok(())
    }
}

fn validate_expected_restore_request(
    expected: &ExpectedLocalRestoreState,
    prepared: &PreparedCheckpointRestore,
    mode: CheckpointInstallMode,
    completion_marker: Option<&RestoreCompletionMarker<'_>>,
) -> Result<(), DurabilityError> {
    prepared.validate()?;
    validate_restore_target_node_id(&expected.target_node_id)?;
    if expected.mode != mode
        || expected.identity != *prepared.identity()
        || expected.execution_profile != prepared.execution_profile()
    {
        return Err(DurabilityError::SnapshotVerification(
            "prepared checkpoint does not match the expected local restore contract".into(),
        ));
    }
    match (
        expected.completion_marker_name.as_deref(),
        completion_marker,
    ) {
        (None, None) => {}
        (Some(expected_name), Some(marker)) if expected_name == marker.name => {}
        _ => {
            return Err(DurabilityError::SnapshotVerification(
                "restore completion marker does not match the expected local restore contract"
                    .into(),
            ));
        }
    }
    Ok(())
}

fn restore_install_receipt(
    prepared: &PreparedCheckpointRestore,
    mode: CheckpointInstallMode,
    target_node_id: &str,
    completion_marker: Option<(&str, &[u8])>,
) -> RestoreInstallReceipt {
    RestoreInstallReceipt {
        format_version: 1,
        mode,
        identity: restore_intent_identity(
            prepared.identity(),
            target_node_id,
            prepared.execution_profile(),
            prepared.checkpoint_root(),
        ),
        checkpoint_index: prepared.restored().tip().index(),
        checkpoint_hash: prepared.restored().tip().hash().to_hex(),
        completion_marker_name: completion_marker.map(|(name, _)| name.to_owned()),
        completion_marker_hash: completion_marker
            .map(|(_, bytes)| LogHash::digest(&[bytes]).to_hex()),
    }
}

fn publish_restore_receipt(
    data_dir: &Path,
    prepared: &PreparedCheckpointRestore,
    mode: CheckpointInstallMode,
    target_node_id: &str,
    completion_marker: Option<(&str, &[u8])>,
) -> Result<(), DurabilityError> {
    let receipt = serde_json::to_vec(&restore_install_receipt(
        prepared,
        mode,
        target_node_id,
        completion_marker,
    ))
    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
    publish_restore_marker(data_dir, RESTORE_RECEIPT_FILE, &receipt)
}

fn finalize_completed_restore_if_present(
    prepared: &PreparedCheckpointRestore,
    expected: &ExpectedLocalRestoreState,
    mode: CheckpointInstallMode,
    completion_marker: Option<&RestoreCompletionMarker<'_>>,
) -> Result<Option<CheckpointTip>, DurabilityError> {
    let ExpectedRegularFile::Exact { bytes, .. } = &expected.restore_receipt else {
        return Ok(None);
    };
    let actual: RestoreInstallReceipt = serde_json::from_slice(bytes).map_err(|_| {
        DurabilityError::SnapshotVerification("local checkpoint restore receipt is invalid".into())
    })?;
    let marker = completion_marker.map(RestoreCompletionMarker::as_parts);
    if actual != restore_install_receipt(prepared, mode, &expected.target_node_id, marker) {
        if mode == CheckpointInstallMode::RejoinPreservingRecorder {
            // A rejoin may deliberately advance from a previously committed
            // checkpoint. The old receipt remains durable until this install
            // publishes its replacement; the exact expected-state token keeps
            // a concurrently completed restore from being mistaken for that
            // known predecessor receipt.
            return Ok(None);
        }
        return Err(DurabilityError::SnapshotVerification(
            "local checkpoint restore receipt does not match the prepared checkpoint".into(),
        ));
    }
    if let Some((name, contents)) = marker {
        let actual_marker = read_bounded_regular_file(&expected.data_dir.join(name), 16 * 1024)?;
        if actual_marker.as_deref() != Some(contents) {
            return Err(DurabilityError::SnapshotVerification(
                "local checkpoint completion marker does not match its receipt".into(),
            ));
        }
    }
    let intent = checkpoint_restore_intent_bytes(
        prepared.identity(),
        &expected.target_node_id,
        prepared.execution_profile(),
        prepared.checkpoint_root(),
    )?;
    match read_bounded_regular_file(&expected.data_dir.join(RESTORE_INTENT_FILE), 16 * 1024)? {
        None => {}
        Some(actual) if actual == intent => {
            fs::remove_file(expected.data_dir.join(RESTORE_INTENT_FILE))?;
            sync_directory(&expected.data_dir)?;
        }
        Some(_) => {
            return Err(DurabilityError::SnapshotVerification(
                "local checkpoint restore intent does not match its receipt".into(),
            ));
        }
    }
    Ok(Some(*prepared.restored().tip()))
}

fn validate_prepared_fresh_install(
    prepared: &PreparedCheckpointRestore,
    data_dir: &Path,
    target_node_id: &str,
    completion_marker: Option<&RestoreCompletionMarker<'_>>,
) -> Result<(), DurabilityError> {
    prepared.validate()?;
    validate_restore_target_node_id(target_node_id)?;
    validate_restore_data_dir_path(data_dir)?;
    if let Some(marker) = completion_marker {
        validate_restore_completion_marker_name(marker.name)?;
    }
    let intent = checkpoint_restore_intent_bytes(
        prepared.identity(),
        target_node_id,
        prepared.execution_profile(),
        prepared.checkpoint_root(),
    )?;
    validate_fresh_restore_data_dir(
        data_dir,
        completion_marker.map(|marker| marker.name),
        &intent,
    )
}

fn validate_prepared_rejoin_install(
    prepared: &PreparedCheckpointRestore,
    data_dir: &Path,
    target_node_id: &str,
    execution_profile: ExecutionProfile,
    completion_marker: &RestoreCompletionMarker<'_>,
) -> Result<(), DurabilityError> {
    prepared.validate()?;
    validate_restore_target_node_id(target_node_id)?;
    validate_restore_completion_marker_name(completion_marker.name)?;
    validate_restore_data_dir_path(data_dir)?;
    if prepared.execution_profile() != execution_profile {
        return Err(DurabilityError::SnapshotVerification(
            "rejoin recovery profile does not match prepared checkpoint".into(),
        ));
    }
    checkpoint_restore_in_progress(
        data_dir,
        prepared.identity(),
        target_node_id,
        execution_profile,
        prepared.checkpoint_root(),
    )?;
    let recovery_identity = RecoveryArtifactIdentity::Restore(restore_intent_identity(
        prepared.identity(),
        target_node_id,
        execution_profile,
        prepared.checkpoint_root(),
    ));
    collect_owned_recovery_artifacts(data_dir, &recovery_identity)?;
    validate_rebuildable_recovery_view(data_dir, execution_profile)?;
    Ok(())
}

fn validate_restore_target_node_id(target_node_id: &str) -> Result<(), DurabilityError> {
    if target_node_id.is_empty() {
        return Err(DurabilityError::SnapshotVerification(
            "target node_id is empty".into(),
        ));
    }
    Ok(())
}

fn validate_restore_data_dir_path(data_dir: &Path) -> Result<(), DurabilityError> {
    match fs::symlink_metadata(data_dir) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

struct RestoreInstallOptions<'a> {
    target_node_id: Option<&'a str>,
    replace_rebuildable: bool,
    recovery_identity: Option<&'a RecoveryArtifactIdentity>,
    completion_marker: Option<(&'a str, &'a [u8])>,
}

struct RestoredCheckpointStaging {
    path: PathBuf,
    tip: CheckpointTip,
}

fn install_restored_checkpoint(
    identity: &CheckpointIdentity,
    restored: &RestoredCheckpoint,
    #[cfg(feature = "sql")] external_sql_effects: &[PreparedCheckpointEffect],
    data_dir: &Path,
    options: RestoreInstallOptions<'_>,
) -> Result<CheckpointTip, DurabilityError> {
    let staged = stage_restored_checkpoint(
        identity,
        restored,
        #[cfg(feature = "sql")]
        external_sql_effects,
        data_dir,
        options.target_node_id,
        options.recovery_identity,
    )?;
    let tip = staged.tip;
    let staging = staged.path;
    let profile = snapshot_profile(identity.cluster_id())?;
    let result = (|| -> Result<(), DurabilityError> {
        if options.replace_rebuildable {
            quarantine_rebuildable_view(data_dir, profile, options.recovery_identity)?;
        }
        publish_restore_staging(&staging, data_dir, options.completion_marker)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result?;
    Ok(tip)
}

fn stage_restored_checkpoint(
    identity: &CheckpointIdentity,
    restored: &RestoredCheckpoint,
    #[cfg(feature = "sql")] external_sql_effects: &[PreparedCheckpointEffect],
    staging_parent: &Path,
    target_node_id: Option<&str>,
    recovery_identity: Option<&RecoveryArtifactIdentity>,
) -> Result<RestoredCheckpointStaging, DurabilityError> {
    let tip = *restored.tip();
    let profile = snapshot_profile(identity.cluster_id())?;
    validate_restored_suffix(profile, restored.suffix())?;
    let staging = create_restore_staging_dir(staging_parent, recovery_identity)?;
    let result = (|| -> Result<(), DurabilityError> {
        if let Some(snapshot) = restored.snapshot() {
            install_profile_snapshot(identity, snapshot, &staging, target_node_id)?;
        }

        #[cfg(feature = "sql")]
        if profile == ExecutionProfile::Sqlite && !external_sql_effects.is_empty() {
            apply_prepared_external_sql_suffix(
                identity,
                restored,
                external_sql_effects,
                &staging,
                target_node_id,
            )?;
            write_prepared_external_sql_handoff(external_sql_effects, &staging)?;
        }

        if restored.snapshot().is_some() || !restored.suffix().is_empty() {
            let initial_configuration = restored
                .snapshot()
                .map(|snapshot| snapshot.anchor().configuration_state().clone())
                .unwrap_or_else(|| checkpoint_identity_configuration(identity));
            let log = FileLogStore::open_with_configuration(
                staging.join("consensus/log"),
                identity.cluster_id(),
                identity.epoch(),
                initial_configuration,
            )?;
            if let Some(snapshot) = restored.snapshot() {
                log.install_recovery_anchor(
                    snapshot.anchor(),
                    identity.recovery_generation(),
                    snapshot.anchor().configuration_state(),
                )?;
            }
            for batch in restored.suffix().chunks(FLUSH_BATCH_ENTRIES as usize) {
                log.append_batch(batch)?;
            }
            let installed_tip = log.logical_state()?.tip;
            if installed_tip.as_ref().map(|tip| (tip.index(), tip.hash()))
                != Some((tip.index(), tip.hash()))
            {
                return Err(DurabilityError::SnapshotVerification(
                    "installed qlog tip does not match checkpoint tip".into(),
                ));
            }
        }
        sync_directory(&staging)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result?;
    Ok(RestoredCheckpointStaging { path: staging, tip })
}

#[cfg(feature = "sql")]
fn write_prepared_external_sql_handoff(
    effects: &[PreparedCheckpointEffect],
    staging: &Path,
) -> Result<(), DurabilityError> {
    let root = staging.join(QEFX_RESTORE_HANDOFF_DIR);
    fs::create_dir_all(&root)?;
    for effect in effects {
        let entry_dir = root.join(effect.entry_index.to_string());
        fs::create_dir(&entry_dir)?;
        fs::write(entry_dir.join("binding.qefx"), &effect.manifest)?;
        for (ordinal, chunk) in effect.chunks.iter().enumerate() {
            fs::write(entry_dir.join(format!("{ordinal:03}.qefc")), chunk)?;
        }
        sync_directory(&entry_dir)?;
    }
    sync_directory(&root)
}

#[cfg(feature = "sql")]
fn apply_prepared_external_sql_suffix(
    identity: &CheckpointIdentity,
    restored: &RestoredCheckpoint,
    effects: &[PreparedCheckpointEffect],
    staging: &Path,
    target_node_id: Option<&str>,
) -> Result<(), DurabilityError> {
    let mut by_index = effects
        .iter()
        .map(|effect| (effect.entry_index, &effect.effect))
        .collect::<BTreeMap<_, _>>();
    let configuration = restored
        .snapshot()
        .map(|snapshot| snapshot.anchor().configuration_state().clone())
        .unwrap_or_else(|| checkpoint_identity_configuration(identity));
    let node_id = target_node_id.ok_or_else(|| {
        DurabilityError::SnapshotVerification(
            "QEFX checkpoint restore requires a target node_id".into(),
        )
    })?;
    let database = SqliteStateMachine::open_with_configuration(
        staging.join("sqlite/db.sqlite"),
        identity.cluster_id(),
        node_id,
        identity.epoch(),
        configuration,
    )
    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
    for entry in restored.suffix() {
        if entry.entry_type == EntryType::Command && !entry.payload.is_empty() {
            let effect = by_index.remove(&entry.index).ok_or_else(|| {
                DurabilityError::SnapshotVerification(
                    "checkpoint QEFX suffix effect was not prepared".into(),
                )
            })?;
            database
                .apply_verified_external_effect(effect)
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
        } else {
            database
                .apply_entry(entry)
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
        }
    }
    if !by_index.is_empty() {
        return Err(DurabilityError::SnapshotVerification(
            "checkpoint prepared QEFX effect has no suffix entry".into(),
        ));
    }
    Ok(())
}

fn validate_restored_suffix(
    profile: ExecutionProfile,
    suffix: &[LogEntry],
) -> Result<(), DurabilityError> {
    for entry in suffix {
        crate::validate_profile_entry_shape(profile, entry)
            .map_err(DurabilityError::SnapshotVerification)?;
    }
    Ok(())
}

fn validate_local_qlog(
    data_dir: &Path,
    identity: &CheckpointIdentity,
    checkpoint_root: LogAnchor,
) -> Result<LogAnchor, DurabilityError> {
    validate_local_qlog_with_configuration(
        data_dir,
        identity,
        checkpoint_root,
        checkpoint_identity_configuration(identity),
    )
}

fn checkpoint_identity_configuration(identity: &CheckpointIdentity) -> ConfigurationState {
    ConfigurationState::active(identity.config_id(), identity.config_digest())
}

fn validate_local_qlog_with_configuration(
    data_dir: &Path,
    identity: &CheckpointIdentity,
    checkpoint_root: LogAnchor,
    initial_configuration: ConfigurationState,
) -> Result<LogAnchor, DurabilityError> {
    let path = data_dir.join("consensus/log");
    if !path_has_state(&path)? {
        if checkpoint_root == LogAnchor::new(0, LogHash::ZERO) {
            return Ok(checkpoint_root);
        }
        return Err(DurabilityError::SnapshotVerification(
            "local qlog is missing or empty".into(),
        ));
    }
    let log = FileLogStore::open_with_configuration(
        path,
        identity.cluster_id(),
        identity.epoch(),
        initial_configuration,
    )?;
    let state = log.logical_state()?;
    let tip = state
        .tip
        .ok_or_else(|| DurabilityError::SnapshotVerification("local qlog has no tip".into()))?;
    if tip.index() < checkpoint_root.index() {
        return Err(DurabilityError::SnapshotVerification(format!(
            "local qlog tip {}/{} is behind checkpoint root {}/{}",
            tip.index(),
            tip.hash().to_hex(),
            checkpoint_root.index(),
            checkpoint_root.hash().to_hex(),
        )));
    }
    if checkpoint_root.index() == 0 {
        if checkpoint_root.hash() != LogHash::ZERO {
            return Err(DurabilityError::SnapshotVerification(
                "checkpoint genesis hash is not zero".into(),
            ));
        }
        return Ok(tip);
    }
    let included_hash = match state.anchor.as_ref() {
        Some(anchor) if anchor.compacted().index() == checkpoint_root.index() => {
            Some(anchor.compacted().hash())
        }
        Some(anchor) if anchor.compacted().index() > checkpoint_root.index() => {
            return Err(DurabilityError::SnapshotVerification(
                "local qlog compacted past checkpoint root without exact inclusion evidence".into(),
            ));
        }
        _ => log.read(checkpoint_root.index())?.map(|entry| entry.hash),
    };
    if included_hash != Some(checkpoint_root.hash()) {
        return Err(DurabilityError::SnapshotVerification(format!(
            "local qlog does not include checkpoint root {} with its exact hash",
            checkpoint_root.index(),
        )));
    }
    Ok(tip)
}

fn install_profile_snapshot(
    identity: &CheckpointIdentity,
    snapshot: &rhiza_archive::RestoredCheckpointSnapshot,
    staging: &Path,
    target_node_id: Option<&str>,
) -> Result<(), DurabilityError> {
    match snapshot_profile(identity.cluster_id())? {
        ExecutionProfile::Sqlite => {
            #[cfg(feature = "sql")]
            {
                validate_anchor_fingerprint(
                    snapshot.anchor(),
                    sql_executor_fingerprint().map_err(|error| {
                        DurabilityError::SnapshotVerification(error.to_string())
                    })?,
                )?;
                let path = staging.join("sqlite/db.sqlite");
                let node_id = target_node_id.ok_or_else(|| {
                    DurabilityError::SnapshotVerification(
                        "SQLite QWAL snapshot restore requires a target node_id".into(),
                    )
                })?;
                restore_recovery_snapshot_file(path, snapshot.bytes(), snapshot.anchor(), node_id)
                    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))
            }
            #[cfg(not(feature = "sql"))]
            Err(DurabilityError::SnapshotVerification(
                "sql execution profile is not compiled in".into(),
            ))
        }
        ExecutionProfile::Graph => {
            #[cfg(feature = "graph")]
            {
                let decoded = decode_graph_snapshot(snapshot.bytes())
                    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
                validate_decoded_snapshot_anchor(
                    snapshot.anchor(),
                    decoded.cluster_id(),
                    decoded.epoch(),
                    decoded.config_id(),
                    decoded.applied_index(),
                    decoded.applied_hash(),
                    decoded.materializer_fingerprint(),
                )?;
                let target_node_id = target_node_id.unwrap_or(decoded.created_by());
                restore_graph_snapshot_file(
                    staging.join("ladybug/graph.lbug"),
                    &decoded,
                    target_node_id,
                )
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))
            }
            #[cfg(not(feature = "graph"))]
            Err(DurabilityError::SnapshotVerification(
                "graph recovery support is not compiled in".into(),
            ))
        }
        ExecutionProfile::Kv => {
            #[cfg(feature = "kv")]
            {
                let decoded = decode_kv_snapshot(snapshot.bytes())
                    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
                validate_decoded_snapshot_anchor(
                    snapshot.anchor(),
                    decoded.cluster_id(),
                    decoded.epoch(),
                    decoded.config_id(),
                    decoded.applied_index(),
                    decoded.applied_hash(),
                    decoded.materializer_fingerprint(),
                )?;
                let target_node_id = target_node_id.unwrap_or(decoded.created_by());
                restore_kv_snapshot_file(staging.join("kv/data.redb"), &decoded, target_node_id)
                    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))
            }
            #[cfg(not(feature = "kv"))]
            Err(DurabilityError::SnapshotVerification(
                "KV recovery support is not compiled in".into(),
            ))
        }
    }
}

fn snapshot_profile(cluster_id: &str) -> Result<ExecutionProfile, DurabilityError> {
    if matches!(cluster_id.strip_prefix("rhiza:graph:"), Some(logical) if !logical.is_empty()) {
        Ok(ExecutionProfile::Graph)
    } else if matches!(cluster_id.strip_prefix("rhiza:kv:"), Some(logical) if !logical.is_empty()) {
        Ok(ExecutionProfile::Kv)
    } else if matches!(cluster_id.strip_prefix("rhiza:sql:"), Some(logical) if !logical.is_empty())
    {
        Ok(ExecutionProfile::Sqlite)
    } else {
        Err(DurabilityError::SnapshotVerification(
            "snapshot checkpoint identity has no canonical execution profile prefix".into(),
        ))
    }
}

fn validate_anchor_fingerprint(
    anchor: &RecoveryAnchor,
    expected: LogHash,
) -> Result<(), DurabilityError> {
    if anchor.executor_fingerprint() != expected {
        return Err(DurabilityError::SnapshotVerification(
            "snapshot executor fingerprint does not match this binary".into(),
        ));
    }
    Ok(())
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn validate_decoded_snapshot_anchor(
    anchor: &RecoveryAnchor,
    cluster_id: &str,
    epoch: u64,
    config_id: u64,
    applied_index: LogIndex,
    applied_hash: LogHash,
    materializer_fingerprint: LogHash,
) -> Result<(), DurabilityError> {
    validate_anchor_fingerprint(anchor, materializer_fingerprint)?;
    if anchor.cluster_id() != cluster_id
        || anchor.epoch() != epoch
        || anchor.config_id() != config_id
        || anchor.compacted().index() != applied_index
        || anchor.compacted().hash() != applied_hash
    {
        return Err(DurabilityError::SnapshotVerification(
            "decoded snapshot identity does not match its recovery anchor".into(),
        ));
    }
    Ok(())
}

pub async fn prestage_successor_checkpoint(
    store: ObjectArchiveStore,
    prestage_dir: impl AsRef<Path>,
    predecessor_configuration: ConfigurationState,
    target_node_id: &str,
    execution_profile: ExecutionProfile,
    target_config_id: u64,
    target_membership_digest: LogHash,
) -> Result<SuccessorPrestage, DurabilityError> {
    if target_node_id.is_empty() {
        return Err(DurabilityError::SnapshotVerification(
            "successor prestage target node_id is empty".into(),
        ));
    }
    let identity = store.checkpoint_identity()?.clone();
    if !predecessor_configuration.is_active()
        || predecessor_configuration.config_id() != identity.config_id()
    {
        return Err(DurabilityError::SnapshotVerification(
            "successor prestage predecessor configuration does not match the checkpoint".into(),
        ));
    }
    if snapshot_profile(identity.cluster_id())? != execution_profile {
        return Err(DurabilityError::SnapshotVerification(
            "successor prestage profile does not match checkpoint identity".into(),
        ));
    }
    if identity
        .config_id()
        .checked_add(1)
        .filter(|next| *next == target_config_id)
        .is_none()
    {
        return Err(DurabilityError::SnapshotVerification(
            "successor prestage target config_id is not the next configuration".into(),
        ));
    }
    let loaded_restore = store
        .load_checkpoint_restore()
        .await?
        .ok_or(DurabilityError::MissingCheckpoint)?;
    let (loaded, restored) = loaded_restore.into_parts();
    if loaded.manifest().identity() != &identity {
        return Err(DurabilityError::SnapshotVerification(
            "successor prestage checkpoint identity changed while loading".into(),
        ));
    }
    #[cfg(feature = "sql")]
    let external_sql_effects = prepare_checkpoint_external_sql_effects(
        &store,
        loaded.manifest(),
        restored.suffix(),
        execution_profile,
    )
    .await?;
    let expected = SuccessorPrestageIdentity {
        cluster_id: identity.cluster_id().to_owned(),
        epoch: identity.epoch(),
        predecessor_config_id: identity.config_id(),
        predecessor_membership_digest: predecessor_configuration.digest().to_hex(),
        predecessor_recovery_generation: identity.recovery_generation(),
        node_id: target_node_id.to_owned(),
        execution_profile,
        target_config_id,
        target_membership_digest: target_membership_digest.to_hex(),
        seed_index: loaded.manifest().tip().index(),
        seed_hash: loaded.manifest().tip().hash().to_hex(),
    };
    validate_successor_prestage_identity(&expected)?;
    let prestage_dir = prestage_dir.as_ref();
    let mut prestage =
        prepare_successor_prestage_root(prestage_dir, Some(&expected), &predecessor_configuration)?;
    match prestage.state {
        SuccessorPrestageState::Ready
        | SuccessorPrestageState::Published
        | SuccessorPrestageState::Finalized => return Ok(prestage),
        SuccessorPrestageState::Preparing => {}
    }

    cleanup_preparing_successor_prestage(prestage_dir, &expected)?;
    if restored.tip() != loaded.manifest().tip() {
        return Err(DurabilityError::SnapshotVerification(
            "successor prestage checkpoint changed during restore".into(),
        ));
    }
    let recovery_identity = RecoveryArtifactIdentity::Prestage(expected.clone());
    let staged = stage_restored_checkpoint(
        &identity,
        &restored,
        #[cfg(feature = "sql")]
        &external_sql_effects,
        prestage_dir,
        Some(target_node_id),
        Some(&recovery_identity),
    )?;
    if let Err(error) = publish_restore_staging(&staged.path, prestage_dir, None) {
        let _ = fs::remove_dir_all(&staged.path);
        return Err(error);
    }
    fs::rename(
        prestage_dir.join(SUCCESSOR_PRESTAGE_INTENT_FILE),
        prestage_dir.join(SUCCESSOR_PRESTAGE_READY_FILE),
    )?;
    sync_directory(prestage_dir)?;
    prestage.state = SuccessorPrestageState::Ready;
    Ok(prestage)
}

pub fn inspect_successor_prestage(
    prestage_dir: impl AsRef<Path>,
    predecessor_configuration: ConfigurationState,
) -> Result<SuccessorPrestage, DurabilityError> {
    prepare_successor_prestage_root(prestage_dir.as_ref(), None, &predecessor_configuration)
}

pub fn publish_successor_prestage(
    mut prestage: SuccessorPrestage,
    data_dir: impl AsRef<Path>,
) -> Result<SuccessorPrestage, DurabilityError> {
    let data_dir = data_dir.as_ref();
    match prestage.state {
        SuccessorPrestageState::Published if prestage.path == data_dir => return Ok(prestage),
        SuccessorPrestageState::Ready => {}
        _ => return Err(DurabilityError::PreconditionFailed),
    }
    if prestage.path != data_dir {
        match fs::symlink_metadata(data_dir) {
            Ok(_) => return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let source_parent = prestage.path.parent().ok_or_else(|| {
            DurabilityError::SnapshotVerification(
                "successor prestage path has no parent directory".into(),
            )
        })?;
        let target_parent = data_dir.parent().ok_or_else(|| {
            DurabilityError::SnapshotVerification(
                "successor data path has no parent directory".into(),
            )
        })?;
        fs::create_dir_all(target_parent)?;
        #[cfg(unix)]
        if fs::metadata(source_parent)?.dev() != fs::metadata(target_parent)?.dev() {
            return Err(DurabilityError::SnapshotVerification(
                "successor prestage and final data directory must share a filesystem".into(),
            ));
        }
        fs::rename(&prestage.path, data_dir)?;
        sync_directory(source_parent)?;
        if target_parent != source_parent {
            sync_directory(target_parent)?;
        }
        prestage.path = data_dir.to_path_buf();
    }
    fs::rename(
        prestage.path.join(SUCCESSOR_PRESTAGE_READY_FILE),
        prestage.path.join(SUCCESSOR_PRESTAGE_PUBLISHED_FILE),
    )?;
    sync_directory(&prestage.path)?;
    prestage.state = SuccessorPrestageState::Published;
    Ok(prestage)
}

fn validate_successor_prestage_stop(
    identity: &SuccessorPrestageIdentity,
    stop: &StopInformation,
    predecessor_membership: &Membership,
) -> Result<rhiza_core::SuccessorDescriptor, DurabilityError> {
    if stop.entry.cluster_id != identity.cluster_id()
        || stop.entry.epoch != identity.epoch()
        || stop.entry.config_id != identity.predecessor_config_id()
        || stop.entry.recompute_hash() != stop.entry.hash
    {
        return Err(DurabilityError::SnapshotVerification(
            "successor prestage requires the exact bound Stop".into(),
        ));
    }
    let change = ConfigChange::recognize_parts(stop.entry.entry_type, &stop.entry.payload)
        .map_err(|_| {
            DurabilityError::SnapshotVerification(
                "successor prestage Stop entry is not a bound configuration change".into(),
            )
        })?;
    let ConfigChange::BoundStop { successor } = change else {
        return Err(DurabilityError::SnapshotVerification(
            "successor prestage requires a bound Stop entry".into(),
        ));
    };
    if successor.cluster_id() != identity.cluster_id()
        || successor.predecessor_config_id() != identity.predecessor_config_id()
        || successor.predecessor_config_digest() != predecessor_membership.digest()
        || successor.config_id() != identity.target_config_id()
        || successor.digest() != identity.target_membership_digest()
    {
        return Err(DurabilityError::SnapshotVerification(
            "successor prestage Stop binding conflicts with the prestage target".into(),
        ));
    }
    stop.proof
        .validate_for_cluster(
            identity.cluster_id(),
            stop.entry.index,
            identity.epoch(),
            identity.predecessor_config_id(),
            predecessor_membership,
        )
        .map_err(|error| {
            DurabilityError::SnapshotVerification(format!(
                "successor prestage Stop proof is not quorum-certified: {error:?}"
            ))
        })?;
    let command = rhiza_core::StoredCommand::new(stop.entry.entry_type, stop.entry.payload.clone());
    let expected_value = rhiza_quepaxa::AcceptedValue::from_command(
        identity.cluster_id(),
        stop.entry.index,
        identity.epoch(),
        identity.predecessor_config_id(),
        stop.entry.prev_hash,
        &command,
    );
    if stop.proof.proposal().value.as_ref() != Some(&expected_value) {
        return Err(DurabilityError::SnapshotVerification(
            "successor prestage Stop proof value does not match the exact Stop entry".into(),
        ));
    }
    Ok(successor)
}

pub fn finalize_successor_prestage_for_stop(
    mut prestage: SuccessorPrestage,
    stop: &StopInformation,
    predecessor_membership: &Membership,
) -> Result<SuccessorPrestage, DurabilityError> {
    if !matches!(
        prestage.state,
        SuccessorPrestageState::Published | SuccessorPrestageState::Finalized
    ) {
        return Err(DurabilityError::PreconditionFailed);
    }
    let successor =
        validate_successor_prestage_stop(&prestage.identity, stop, predecessor_membership)?;
    let expected_stop = LogAnchor::new(stop.entry.index, stop.entry.hash);
    let local_tip = validate_local_qlog_with_configuration(
        &prestage.path,
        &prestage.identity.checkpoint_identity(),
        prestage.identity.seed_anchor(),
        ConfigurationState::active(
            prestage.identity.predecessor_config_id(),
            successor.predecessor_config_digest(),
        ),
    )?;
    if local_tip != expected_stop {
        return Err(DurabilityError::SnapshotVerification(
            "successor prestage final qlog tip does not exactly match the bound Stop".into(),
        ));
    }
    if prestage.state == SuccessorPrestageState::Published {
        fs::rename(
            prestage.path.join(SUCCESSOR_PRESTAGE_PUBLISHED_FILE),
            prestage.path.join(SUCCESSOR_PRESTAGE_FINALIZED_FILE),
        )?;
        sync_directory(&prestage.path)?;
        prestage.state = SuccessorPrestageState::Finalized;
    }
    Ok(prestage)
}

pub fn adopt_finalized_successor_prestage(
    prestage: SuccessorPrestage,
    config: &NodeConfig,
    stop: &StopInformation,
    predecessor_membership: &Membership,
) -> Result<SuccessorRestorePreparation, DurabilityError> {
    if prestage.state != SuccessorPrestageState::Finalized
        || prestage.path.as_path() != config.data_dir().as_path()
        || prestage.identity.cluster_id() != config.cluster_id()
        || prestage.identity.epoch() != config.epoch()
        || prestage.identity.predecessor_recovery_generation() != config.recovery_generation()
        || prestage.identity.node_id() != config.node_id()
        || prestage.identity.execution_profile() != config.execution_profile()
        || prestage.identity.target_membership_digest() != config.membership().digest()
        || stop.entry.cluster_id != config.cluster_id()
        || stop.entry.epoch != config.epoch()
        || stop.entry.config_id != prestage.identity.predecessor_config_id()
        || stop.entry.recompute_hash() != stop.entry.hash
        || config.predecessor_stop_entry.as_ref() != Some(&stop.entry)
    {
        return Err(DurabilityError::SnapshotVerification(
            "finalized successor prestage does not match the target configuration and Stop".into(),
        ));
    }
    let successor =
        validate_successor_prestage_stop(&prestage.identity, stop, predecessor_membership)?;
    let predecessor_digest = successor.predecessor_config_digest();
    let expected_stop = LogAnchor::new(stop.entry.index, stop.entry.hash);
    if successor.cluster_id() != config.cluster_id()
        || successor.predecessor_config_id() != prestage.identity.predecessor_config_id()
        || successor.config_id() != prestage.identity.target_config_id()
        || successor.digest() != prestage.identity.target_membership_digest()
        || successor.members() != config.membership().members()
        || config.log_initial_configuration()
            != &ConfigurationState::active(
                prestage.identity.predecessor_config_id(),
                predecessor_digest,
            )
        || config.configuration_state()
            != &ConfigurationState::stopped(
                prestage.identity.predecessor_config_id(),
                predecessor_digest,
                expected_stop,
                StopBinding::Bound {
                    successor: successor.clone(),
                    stop_command_hash: rhiza_core::StoredCommand::new(
                        stop.entry.entry_type,
                        stop.entry.payload.clone(),
                    )
                    .hash(),
                },
            )
    {
        return Err(DurabilityError::SnapshotVerification(
            "finalized successor prestage Stop binding conflicts with the target configuration"
                .into(),
        ));
    }
    let local_tip = validate_local_qlog_with_configuration(
        &prestage.path,
        &prestage.identity.checkpoint_identity(),
        prestage.identity.seed_anchor(),
        ConfigurationState::active(
            prestage.identity.predecessor_config_id(),
            predecessor_digest,
        ),
    )?;
    if local_tip != expected_stop {
        return Err(DurabilityError::SnapshotVerification(
            "finalized successor prestage qlog does not end at the exact Stop".into(),
        ));
    }

    let receipt = serde_json::to_vec(&SuccessorRestoreIdentity {
        cluster_id: config.cluster_id(),
        epoch: config.epoch(),
        target_config_id: prestage.identity.target_config_id(),
        recovery_generation: config.recovery_generation(),
        node_id: config.node_id(),
        membership_digest: config.membership().digest().to_hex(),
        predecessor_config_id: prestage.identity.predecessor_config_id(),
        stop_index: stop.entry.index,
        stop_hash: stop.entry.hash.to_hex(),
    })
    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
    let successor_lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(prestage.path.join(SUCCESSOR_RESTORE_LOCK_FILE))?;
    successor_lock
        .try_lock()
        .map_err(|_| DurabilityError::PreconditionFailed)?;
    let intent = prestage.path.join(SUCCESSOR_RESTORE_INTENT_FILE);
    let complete = prestage.path.join(SUCCESSOR_RESTORE_COMPLETE_FILE);
    match (
        read_regular_successor_control_file(&intent)?,
        read_regular_successor_control_file(&complete)?,
    ) {
        (None, None) => {
            publish_restore_marker(&prestage.path, SUCCESSOR_RESTORE_INTENT_FILE, &receipt)?
        }
        (Some(actual), None) if actual == receipt => {}
        _ => {
            return Err(DurabilityError::DataDirNotFresh(
                prestage.path.to_path_buf(),
            ));
        }
    }
    fs::remove_file(prestage.path.join(SUCCESSOR_PRESTAGE_FINALIZED_FILE))?;
    sync_directory(&prestage.path)?;
    Ok(SuccessorRestorePreparation {
        tip: CheckpointTip::new(stop.entry.index, stop.entry.hash),
        data_dir: prestage.path.clone(),
        identity: receipt,
        requires_recorder_install: true,
        _lock: successor_lock,
    })
}

fn write_repair_artifact_ownership(
    artifact: &Path,
    role: RepairArtifactRole,
    identity: &RecoveryArtifactIdentity,
) -> Result<(), DurabilityError> {
    let name = artifact
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            DurabilityError::SnapshotVerification(
                "repair artifact path must have a UTF-8 final component".into(),
            )
        })?
        .to_owned();
    let contents = serde_json::to_vec(&RepairArtifactOwnership {
        role,
        name,
        identity: identity.clone(),
    })
    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
    let owner = artifact.join(REPAIR_ARTIFACT_OWNER_FILE);
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(owner)?;
    file.write_all(&contents)?;
    file.sync_all()?;
    sync_directory(artifact)
}

fn create_restore_staging_dir(
    data_dir: &Path,
    recovery_identity: Option<&RecoveryArtifactIdentity>,
) -> Result<PathBuf, DurabilityError> {
    fs::create_dir_all(data_dir)?;
    for _ in 0..128 {
        let sequence = RESTORE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let staging = data_dir.join(format!(
            "{RESTORE_STAGING_PREFIX}{}-{sequence}",
            process::id()
        ));
        match fs::create_dir(&staging) {
            Ok(()) => {
                if let Some(identity) = recovery_identity {
                    if let Err(error) = write_repair_artifact_ownership(
                        &staging,
                        RepairArtifactRole::Staging,
                        identity,
                    ) {
                        let _ = fs::remove_dir_all(&staging);
                        return Err(error);
                    }
                }
                return Ok(staging);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not allocate restore staging directory",
    )
    .into())
}

fn publish_restore_staging(
    staging: &Path,
    data_dir: &Path,
    completion_marker: Option<(&str, &[u8])>,
) -> Result<(), DurabilityError> {
    sync_directory(staging)?;
    for name in ["sqlite", "ladybug", "kv", "consensus"] {
        let source = staging.join(name);
        if source.exists() {
            fs::rename(&source, data_dir.join(name))?;
        }
    }
    // A recovery-owned staging directory carries its ownership record inside the staging root.
    // Remove that sidecar together with the now-empty staging root; it must never be promoted
    // into the live data directory.
    fs::remove_dir_all(staging)?;
    sync_directory(data_dir)?;
    if let Some((marker_name, marker_contents)) = completion_marker {
        publish_restore_marker(data_dir, marker_name, marker_contents)?;
    }
    sync_directory(data_dir)
}

fn quarantine_rebuildable_view(
    data_dir: &Path,
    profile: ExecutionProfile,
    recovery_identity: Option<&RecoveryArtifactIdentity>,
) -> Result<Option<PathBuf>, DurabilityError> {
    if !validate_rebuildable_recovery_view(data_dir, profile)? {
        return Ok(None);
    }
    let materializer = match profile {
        ExecutionProfile::Sqlite => "sqlite",
        ExecutionProfile::Kv => "kv",
        ExecutionProfile::Graph => "ladybug",
    };
    let names = [materializer, "consensus"];
    for _ in 0..128 {
        let sequence = RESTORE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let quarantine = data_dir.join(format!(
            ".rebuildable-quarantine-{}-{sequence}",
            process::id()
        ));
        match fs::create_dir(&quarantine) {
            Ok(()) => {
                if let Some(identity) = recovery_identity {
                    if let Err(error) = write_repair_artifact_ownership(
                        &quarantine,
                        RepairArtifactRole::Quarantine,
                        identity,
                    ) {
                        let _ = fs::remove_dir_all(&quarantine);
                        return Err(error);
                    }
                }
                for name in names {
                    let source = data_dir.join(name);
                    match fs::symlink_metadata(&source) {
                        Ok(_) => fs::rename(source, quarantine.join(name))?,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                sync_directory(&quarantine)?;
                sync_directory(data_dir)?;
                return Ok(Some(quarantine));
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not allocate rebuildable recovery quarantine",
    )
    .into())
}

fn validate_rebuildable_recovery_view(
    data_dir: &Path,
    profile: ExecutionProfile,
) -> Result<bool, DurabilityError> {
    let materializer = match profile {
        ExecutionProfile::Sqlite => "sqlite",
        ExecutionProfile::Kv => "kv",
        ExecutionProfile::Graph => "ladybug",
    };
    let mut has_rebuildable_view = false;
    for name in [materializer, "consensus"] {
        match fs::symlink_metadata(data_dir.join(name)) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(DurabilityError::SnapshotVerification(
                    "rebuildable recovery view is not a regular directory".into(),
                ));
            }
            Ok(_) => has_rebuildable_view = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(has_rebuildable_view)
}

fn publish_restore_marker(
    data_dir: &Path,
    marker_name: &str,
    contents: &[u8],
) -> Result<(), DurabilityError> {
    validate_restore_marker_name(marker_name)?;
    fs::create_dir_all(data_dir)?;
    let sequence = RESTORE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = data_dir.join(format!(
        "{RESTORE_MARKER_TMP_PREFIX}{}-{sequence}",
        process::id()
    ));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(contents)?;
    file.sync_all()?;
    fs::rename(temporary, data_dir.join(marker_name))?;
    sync_directory(data_dir)
}

fn is_valid_node_data_root_lock(path: &Path) -> Result<bool, DurabilityError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    Ok(!metadata.file_type().is_symlink() && metadata.is_file() && metadata.len() == 0)
}

fn is_valid_restore_install_receipt(path: &Path) -> Result<bool, DurabilityError> {
    let Some(bytes) = read_bounded_regular_file(path, 16 * 1024)? else {
        return Ok(false);
    };
    Ok(
        serde_json::from_slice::<RestoreInstallReceipt>(&bytes).is_ok_and(|receipt| {
            receipt.format_version == 1
                && !receipt.identity.cluster_id.is_empty()
                && !receipt.identity.node_id.is_empty()
                && LogHash::from_hex(&receipt.identity.checkpoint_hash).is_some()
                && LogHash::from_hex(&receipt.checkpoint_hash).is_some()
        }),
    )
}

fn validate_restore_marker_name(marker_name: &str) -> Result<(), DurabilityError> {
    let mut components = Path::new(marker_name).components();
    let is_exactly_one_normal_component = matches!(
        (components.next(), components.next()),
        (Some(std::path::Component::Normal(_)), None)
    );
    // `Path::components` recognizes a drive prefix only on Windows. Reject
    // the portable drive-prefix spelling here as well, so a marker accepted
    // on Unix cannot become rooted or prefixed when the same data is handled
    // on Windows. Backslashes are forbidden independently for the same
    // cross-platform reason.
    let has_windows_drive_prefix = marker_name
        .as_bytes()
        .get(0..2)
        .is_some_and(|prefix| prefix[0].is_ascii_alphabetic() && prefix[1] == b':');
    if marker_name.is_empty()
        || marker_name.contains(['/', '\\', '\0'])
        || has_windows_drive_prefix
        || !is_exactly_one_normal_component
    {
        return Err(DurabilityError::SnapshotVerification(
            "restore marker name must be one local file name".into(),
        ));
    }
    Ok(())
}

fn validate_restore_completion_marker_name(marker_name: &str) -> Result<(), DurabilityError> {
    validate_restore_marker_name(marker_name)?;
    if !is_portable_completion_marker_name(marker_name) {
        return Err(DurabilityError::SnapshotVerification(
            "restore completion marker name is not portable ASCII".into(),
        ));
    }
    // Recovery control names are ASCII protocol names. Compare them with
    // ASCII case folding so a marker accepted on a case-sensitive filesystem
    // cannot later alias a control path on a case-insensitive filesystem.
    let folded = marker_name.to_ascii_lowercase();
    let internal_name = matches!(
        folded.as_str(),
        RESTORE_INTENT_FILE
            | SUCCESSOR_RESTORE_LOCK_FILE
            | SUCCESSOR_RESTORE_INTENT_FILE
            | SUCCESSOR_RESTORE_COMPLETE_FILE
            | SUCCESSOR_PRESTAGE_LOCK_FILE
            | SUCCESSOR_PRESTAGE_INTENT_FILE
            | SUCCESSOR_PRESTAGE_READY_FILE
            | SUCCESSOR_PRESTAGE_PUBLISHED_FILE
            | SUCCESSOR_PRESTAGE_FINALIZED_FILE
            | REPAIR_ARTIFACT_OWNER_FILE
            | "sqlite"
            | "ladybug"
            | "kv"
            | "consensus"
    );
    let internal_prefix = folded.starts_with(RESTORE_STAGING_PREFIX)
        || folded.starts_with(RESTORE_MARKER_TMP_PREFIX)
        || folded.starts_with(".rebuildable-quarantine-");
    if internal_name || internal_prefix {
        return Err(DurabilityError::SnapshotVerification(
            "restore completion marker collides with an internal recovery path".into(),
        ));
    }
    Ok(())
}

fn is_portable_completion_marker_name(marker_name: &str) -> bool {
    let bytes = marker_name.as_bytes();
    if bytes.is_empty()
        || bytes.ends_with(b".")
        || bytes.ends_with(b" ")
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return false;
    }
    let stem = marker_name.split('.').next().unwrap_or_default();
    let reserved_device = stem.eq_ignore_ascii_case("con")
        || stem.eq_ignore_ascii_case("prn")
        || stem.eq_ignore_ascii_case("aux")
        || stem.eq_ignore_ascii_case("nul")
        || (stem.len() == 4
            && (stem[..3].eq_ignore_ascii_case("com") || stem[..3].eq_ignore_ascii_case("lpt"))
            && matches!(stem.as_bytes()[3], b'1'..=b'9'));
    !reserved_device
}

fn is_owned_generated_recovery_name(name: &str, prefix: &str) -> bool {
    let Some(suffix) = name.strip_prefix(prefix) else {
        return false;
    };
    let mut parts = suffix.split('-');
    let (Some(process_id), Some(sequence), None) = (parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    process_id.parse::<u32>().is_ok_and(|id| id > 0) && sequence.parse::<u64>().is_ok()
}

fn is_safe_restore_marker_tmp(path: &Path, name: &str) -> Result<bool, DurabilityError> {
    if !is_owned_generated_recovery_name(name, RESTORE_MARKER_TMP_PREFIX) {
        return Ok(false);
    }
    Ok(read_bounded_regular_file(path, 16384)?.is_some())
}

fn is_owned_recovery_directory(
    path: &Path,
    allowed_children: &[&str],
    expected_role: RepairArtifactRole,
    expected_identity: &RecoveryArtifactIdentity,
) -> Result<bool, DurabilityError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Ok(false);
    }
    let owner = path.join(REPAIR_ARTIFACT_OWNER_FILE);
    let Some(owner_bytes) = read_bounded_regular_file(&owner, 16384)? else {
        return Ok(false);
    };
    let Ok(ownership) = serde_json::from_slice::<RepairArtifactOwnership>(&owner_bytes) else {
        return Ok(false);
    };
    if ownership.role != expected_role
        || ownership.identity != *expected_identity
        || path.file_name().and_then(|name| name.to_str()) != Some(ownership.name.as_str())
    {
        return Ok(false);
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == REPAIR_ARTIFACT_OWNER_FILE {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        if !allowed_children.contains(&name.as_ref())
            || metadata.file_type().is_symlink()
            || !metadata.is_dir()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn collect_owned_recovery_artifacts(
    data_dir: &Path,
    identity: &RecoveryArtifactIdentity,
) -> Result<Vec<PathBuf>, DurabilityError> {
    let mut cleanup = Vec::new();
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let has_staging_prefix = name.starts_with(RESTORE_STAGING_PREFIX);
        let has_quarantine_prefix = name.starts_with(".rebuildable-quarantine-");
        if (has_staging_prefix && !is_owned_generated_recovery_name(&name, RESTORE_STAGING_PREFIX))
            || (has_quarantine_prefix
                && !is_owned_generated_recovery_name(&name, ".rebuildable-quarantine-"))
        {
            return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
        }
        let artifact = if has_staging_prefix {
            Some((
                ["sqlite", "ladybug", "kv", "consensus"].as_slice(),
                RepairArtifactRole::Staging,
            ))
        } else if has_quarantine_prefix {
            Some((
                ["sqlite", "ladybug", "kv", "consensus"].as_slice(),
                RepairArtifactRole::Quarantine,
            ))
        } else {
            None
        };
        let Some((allowed_children, role)) = artifact else {
            continue;
        };
        if !is_owned_recovery_directory(&entry.path(), allowed_children, role, identity)? {
            return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
        }
        cleanup.push(entry.path());
    }
    Ok(cleanup)
}

fn cleanup_owned_recovery_artifacts(
    data_dir: &Path,
    identity: &RecoveryArtifactIdentity,
) -> Result<(), DurabilityError> {
    let cleanup = collect_owned_recovery_artifacts(data_dir, identity)?;
    for path in cleanup.iter() {
        fs::remove_dir_all(path)?;
    }
    if !cleanup.is_empty() {
        sync_directory(data_dir)?;
    }
    Ok(())
}

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

fn open_recovery_file_no_follow(path: &Path) -> Result<fs::File, DurabilityError> {
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
    Ok(options.open(path)?)
}

fn read_bounded_regular_file(
    path: &Path,
    max_bytes: u64,
) -> Result<Option<Vec<u8>>, DurabilityError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > max_bytes {
        return Err(DurabilityError::SnapshotVerification(
            "recovery file is not a bounded regular file".into(),
        ));
    }
    let mut file = open_recovery_file_no_follow(path)?;
    let opened = file.metadata()?;
    if !opened.is_file() || opened.len() > max_bytes {
        return Err(DurabilityError::SnapshotVerification(
            "recovery file changed to an invalid form before read".into(),
        ));
    }
    #[cfg(unix)]
    if metadata.dev() != opened.dev()
        || metadata.ino() != opened.ino()
        || metadata.len() != opened.len()
    {
        return Err(DurabilityError::SnapshotVerification(
            "recovery file changed before no-follow open".into(),
        ));
    }
    let mut contents = Vec::with_capacity(usize::try_from(opened.len()).unwrap_or(0));
    Read::by_ref(&mut file)
        .take(max_bytes + 1)
        .read_to_end(&mut contents)?;
    if contents.len() as u64 > max_bytes || contents.len() as u64 != opened.len() {
        return Err(DurabilityError::SnapshotVerification(
            "recovery file changed during bounded read".into(),
        ));
    }
    Ok(Some(contents))
}

fn read_regular_successor_control_file(path: &Path) -> Result<Option<Vec<u8>>, DurabilityError> {
    read_bounded_regular_file(path, 16384)
}

fn parse_successor_restore_receipt(bytes: &[u8]) -> Option<SuccessorRestoreReceipt> {
    let receipt = serde_json::from_slice::<SuccessorRestoreReceipt>(bytes).ok()?;
    (!receipt.cluster_id.is_empty()
        && !receipt.node_id.is_empty()
        && LogHash::from_hex(&receipt.membership_digest).is_some()
        && LogHash::from_hex(&receipt.stop_hash).is_some())
    .then_some(receipt)
}

fn successor_receipt_matches_finalized_prestage(
    receipt: &SuccessorRestoreReceipt,
    identity: &SuccessorPrestageIdentity,
) -> bool {
    receipt.cluster_id == identity.cluster_id
        && receipt.epoch == identity.epoch
        && receipt.target_config_id == identity.target_config_id
        && receipt.recovery_generation == identity.predecessor_recovery_generation
        && receipt.node_id == identity.node_id
        && receipt.membership_digest == identity.target_membership_digest
        && receipt.predecessor_config_id == identity.predecessor_config_id
}

fn parse_successor_prestage_identity(bytes: &[u8]) -> Option<SuccessorPrestageIdentity> {
    let identity = serde_json::from_slice::<SuccessorPrestageIdentity>(bytes).ok()?;
    validate_successor_prestage_identity(&identity)
        .is_ok()
        .then_some(identity)
}

fn validate_successor_prestage_identity(
    identity: &SuccessorPrestageIdentity,
) -> Result<(), DurabilityError> {
    let valid = !identity.cluster_id.is_empty()
        && !identity.node_id.is_empty()
        && snapshot_profile(&identity.cluster_id)? == identity.execution_profile
        && identity
            .predecessor_config_id
            .checked_add(1)
            .is_some_and(|next| next == identity.target_config_id)
        && LogHash::from_hex(&identity.predecessor_membership_digest).is_some()
        && LogHash::from_hex(&identity.target_membership_digest).is_some()
        && LogHash::from_hex(&identity.seed_hash).is_some();
    if !valid {
        return Err(DurabilityError::SnapshotVerification(
            "successor prestage identity is invalid".into(),
        ));
    }
    Ok(())
}

fn prepare_successor_prestage_root(
    prestage_dir: &Path,
    expected_identity: Option<&SuccessorPrestageIdentity>,
    predecessor_configuration: &ConfigurationState,
) -> Result<SuccessorPrestage, DurabilityError> {
    if expected_identity.is_some() {
        fs::create_dir_all(prestage_dir)?;
    }
    let metadata = fs::symlink_metadata(prestage_dir)
        .map_err(|_| DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()));
    }
    let lock_path = prestage_dir.join(SUCCESSOR_PRESTAGE_LOCK_FILE);
    let mut lock_options = fs::OpenOptions::new();
    lock_options.read(true).write(true);
    if expected_identity.is_some() {
        lock_options.create(true).truncate(false);
    }
    let lock = lock_options
        .open(&lock_path)
        .map_err(|_| DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()))?;
    lock.try_lock()
        .map_err(|_| DurabilityError::PreconditionFailed)?;

    let marker_files = [
        (
            SUCCESSOR_PRESTAGE_INTENT_FILE,
            SuccessorPrestageState::Preparing,
        ),
        (SUCCESSOR_PRESTAGE_READY_FILE, SuccessorPrestageState::Ready),
        (
            SUCCESSOR_PRESTAGE_PUBLISHED_FILE,
            SuccessorPrestageState::Published,
        ),
        (
            SUCCESSOR_PRESTAGE_FINALIZED_FILE,
            SuccessorPrestageState::Finalized,
        ),
    ];
    let mut marker = None;
    for (name, state) in marker_files {
        let Some(bytes) = read_bounded_regular_file(&prestage_dir.join(name), 16384)
            .map_err(|_| DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()))?
        else {
            continue;
        };
        if marker.is_some() {
            return Err(DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()));
        }
        let identity = parse_successor_prestage_identity(&bytes)
            .ok_or_else(|| DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()))?;
        marker = Some((name, state, identity));
    }

    let (marker_name, state, identity) = match marker {
        Some(marker) => marker,
        None => {
            let expected = expected_identity
                .ok_or_else(|| DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()))?;
            for entry in fs::read_dir(prestage_dir)? {
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name == SUCCESSOR_PRESTAGE_LOCK_FILE {
                    continue;
                }
                if is_safe_restore_marker_tmp(&entry.path(), &name)? {
                    fs::remove_file(entry.path())?;
                    continue;
                }
                return Err(DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()));
            }
            let bytes = serde_json::to_vec(expected)
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            publish_restore_marker(prestage_dir, SUCCESSOR_PRESTAGE_INTENT_FILE, &bytes)?;
            (
                SUCCESSOR_PRESTAGE_INTENT_FILE,
                SuccessorPrestageState::Preparing,
                expected.clone(),
            )
        }
    };
    if expected_identity.is_some_and(|expected| expected != &identity) {
        return Err(DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()));
    }
    if !predecessor_configuration.is_active()
        || predecessor_configuration.config_id() != identity.predecessor_config_id()
        || predecessor_configuration.digest() != identity.predecessor_membership_digest()
    {
        return Err(DurabilityError::SnapshotVerification(
            "successor prestage predecessor configuration does not match its identity".into(),
        ));
    }

    let recovery_identity = RecoveryArtifactIdentity::Prestage(identity.clone());
    for entry in fs::read_dir(prestage_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == SUCCESSOR_PRESTAGE_LOCK_FILE || name == marker_name {
            continue;
        }
        if state == SuccessorPrestageState::Finalized && name == SUCCESSOR_RESTORE_INTENT_FILE {
            let bytes = read_regular_successor_control_file(&entry.path())?
                .ok_or_else(|| DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()))?;
            let receipt = parse_successor_restore_receipt(&bytes)
                .ok_or_else(|| DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()))?;
            if successor_receipt_matches_finalized_prestage(&receipt, &identity) {
                continue;
            }
            return Err(DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()));
        }
        if state == SuccessorPrestageState::Finalized
            && name == SUCCESSOR_RESTORE_LOCK_FILE
            && fs::symlink_metadata(entry.path())
                .is_ok_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink())
        {
            continue;
        }
        if is_safe_restore_marker_tmp(&entry.path(), &name)? {
            if state == SuccessorPrestageState::Preparing {
                fs::remove_file(entry.path())?;
                continue;
            }
            return Err(DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()));
        }
        if ["sqlite", "ladybug", "kv", "consensus"].contains(&name.as_ref()) {
            let metadata = fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()));
            }
            continue;
        }
        if name.starts_with(RESTORE_STAGING_PREFIX)
            && state == SuccessorPrestageState::Preparing
            && is_owned_generated_recovery_name(&name, RESTORE_STAGING_PREFIX)
            && is_owned_recovery_directory(
                &entry.path(),
                &["sqlite", "ladybug", "kv", "consensus"],
                RepairArtifactRole::Staging,
                &recovery_identity,
            )?
        {
            continue;
        }
        return Err(DurabilityError::DataDirNotFresh(prestage_dir.to_path_buf()));
    }
    if !matches!(
        state,
        SuccessorPrestageState::Preparing | SuccessorPrestageState::Finalized
    ) {
        validate_local_qlog_with_configuration(
            prestage_dir,
            &identity.checkpoint_identity(),
            identity.seed_anchor(),
            predecessor_configuration.clone(),
        )?;
    }
    Ok(SuccessorPrestage {
        path: prestage_dir.to_path_buf(),
        identity,
        state,
        _lock: lock,
    })
}

fn cleanup_preparing_successor_prestage(
    prestage_dir: &Path,
    identity: &SuccessorPrestageIdentity,
) -> Result<(), DurabilityError> {
    let recovery_identity = RecoveryArtifactIdentity::Prestage(identity.clone());
    for entry in fs::read_dir(prestage_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let owned_component = ["sqlite", "ladybug", "kv", "consensus"].contains(&name.as_ref());
        let owned_staging = name.starts_with(RESTORE_STAGING_PREFIX)
            && is_owned_generated_recovery_name(&name, RESTORE_STAGING_PREFIX)
            && is_owned_recovery_directory(
                &entry.path(),
                &["sqlite", "ladybug", "kv", "consensus"],
                RepairArtifactRole::Staging,
                &recovery_identity,
            )?;
        if owned_component || owned_staging {
            fs::remove_dir_all(entry.path())?;
        }
    }
    sync_directory(prestage_dir)
}

fn sync_directory(path: &Path) -> Result<(), DurabilityError> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(feature = "sql")]
fn persist_pending_qefx_gc(
    data_dir: &Path,
    pending: &PendingQefxGc,
) -> Result<(), DurabilityError> {
    if let Some(existing) = load_pending_qefx_gc(data_dir)? {
        if existing.cluster_id != pending.cluster_id || existing.epoch != pending.epoch {
            return Err(DurabilityError::SnapshotVerification(
                "pending QEFX GC record belongs to a different cluster or epoch".into(),
            ));
        }
        if existing.through_slot > pending.through_slot {
            return Ok(());
        }
        if existing.through_slot == pending.through_slot {
            if existing.checkpoint_hash == pending.checkpoint_hash
                && existing.manifest_digest == pending.manifest_digest
            {
                return Ok(());
            }
            return Err(DurabilityError::SnapshotVerification(
                "pending QEFX GC record conflicts at the same checkpoint slot".into(),
            ));
        }
    }
    let path = data_dir.join(PENDING_QEFX_GC_FILE);
    let parent = path.parent().ok_or_else(|| {
        DurabilityError::SnapshotVerification("pending QEFX GC path has no parent".into())
    })?;
    fs::create_dir_all(parent)?;
    let bytes = serde_json::to_vec(pending)
        .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
    let temporary = parent.join(format!(
        ".pending-qefx-gc.tmp-{}-{}",
        process::id(),
        RESTORE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, &path)?;
    sync_directory(parent)
}

#[cfg(feature = "sql")]
fn clear_pending_qefx_gc(data_dir: &Path) -> Result<(), DurabilityError> {
    let path = data_dir.join(PENDING_QEFX_GC_FILE);
    if path.exists() {
        fs::remove_file(&path)?;
        sync_directory(path.parent().expect("pending QEFX GC path has parent"))?;
    }
    Ok(())
}

#[cfg(feature = "sql")]
fn load_pending_qefx_gc(data_dir: &Path) -> Result<Option<PendingQefxGc>, DurabilityError> {
    let path = data_dir.join(PENDING_QEFX_GC_FILE);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > 4096 {
        return Err(DurabilityError::SnapshotVerification(
            "pending QEFX GC record is unsafe".into(),
        ));
    }
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))
}

fn validate_local_batch(
    entries: &[rhiza_core::LogEntry],
    start: LogIndex,
    end: LogIndex,
    durable_tip: CheckpointTip,
) -> Result<(), DurabilityError> {
    let expected_len =
        usize::try_from(end - start + 1).map_err(|_| DurabilityError::LocalLogGap {
            expected: start,
            actual: entries.first().map(|entry| entry.index),
        })?;
    if entries.len() != expected_len {
        let actual = entries
            .iter()
            .zip(start..=end)
            .find_map(|(entry, expected)| (entry.index != expected).then_some(entry.index));
        return Err(DurabilityError::LocalLogGap {
            expected: start + entries.len() as u64,
            actual,
        });
    }

    let mut expected_hash = durable_tip.hash();
    for (expected_index, entry) in (start..).zip(entries) {
        if entry.index != expected_index {
            return Err(DurabilityError::LocalLogGap {
                expected: expected_index,
                actual: Some(entry.index),
            });
        }
        if entry.prev_hash != expected_hash || entry.recompute_hash() != entry.hash {
            return Err(DurabilityError::LocalLogConflict { index: entry.index });
        }
        expected_hash = entry.hash;
    }
    Ok(())
}

fn prepare_fresh_restore_data_dir(
    data_dir: &Path,
    completion_marker_name: Option<&str>,
    expected_intent: &[u8],
) -> Result<(), DurabilityError> {
    validate_fresh_restore_data_dir(data_dir, completion_marker_name, expected_intent)?;
    if !path_has_state(data_dir)? {
        return Ok(());
    }

    let intent = data_dir.join(RESTORE_INTENT_FILE);
    let intent_metadata = match fs::symlink_metadata(&intent) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let (active_intent, recovery_identity) = if let Some(metadata) = intent_metadata {
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || read_bounded_regular_file(&intent, 4096)?.as_deref() != Some(expected_intent)
        {
            return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
        }
        (
            &intent,
            Some(RecoveryArtifactIdentity::Restore(
                parse_restore_intent_identity(expected_intent)
                    .ok_or_else(|| DurabilityError::DataDirNotFresh(data_dir.to_path_buf()))?,
            )),
        )
    } else {
        let entries = fs::read_dir(data_dir)?.collect::<Result<Vec<_>, _>>()?;
        if entries.iter().all(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            is_safe_restore_marker_tmp(&entry.path(), &name).unwrap_or(false)
                || (name == crate::NODE_DATA_ROOT_LOCK_FILE
                    && is_valid_node_data_root_lock(&entry.path()).unwrap_or(false))
                || (name == RESTORE_RECEIPT_FILE
                    && is_valid_restore_install_receipt(&entry.path()).unwrap_or(false))
        }) {
            for entry in entries {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if is_safe_restore_marker_tmp(&entry.path(), &name)? {
                    fs::remove_file(entry.path())?;
                }
            }
            sync_directory(data_dir)?;
            return Ok(());
        }
        return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
    };

    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let marker_tmp = is_safe_restore_marker_tmp(&entry.path(), &name)?;
        if name.starts_with(RESTORE_MARKER_TMP_PREFIX) && !marker_tmp {
            return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
        }
        let is_staging = name.starts_with(RESTORE_STAGING_PREFIX);
        if is_staging
            && (!is_owned_generated_recovery_name(&name, RESTORE_STAGING_PREFIX)
                || !recovery_identity.as_ref().is_some_and(|identity| {
                    is_owned_recovery_directory(
                        &entry.path(),
                        &["sqlite", "ladybug", "kv", "consensus"],
                        RepairArtifactRole::Staging,
                        identity,
                    )
                    .unwrap_or(false)
                }))
        {
            return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
        }
        let owned = entry.path() == active_intent.as_path()
            || completion_marker_name.is_some_and(|marker| name == marker)
            || name == "sqlite"
            || name == "ladybug"
            || name == "kv"
            || name == "consensus"
            || (name == crate::NODE_DATA_ROOT_LOCK_FILE
                && is_valid_node_data_root_lock(&entry.path())?)
            || (name == RESTORE_RECEIPT_FILE && is_valid_restore_install_receipt(&entry.path())?)
            || marker_tmp
            || is_staging;
        if !owned {
            return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
        }
    }

    for name in ["sqlite", "ladybug", "kv", "consensus"] {
        let path = data_dir.join(name);
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
    }
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(RESTORE_STAGING_PREFIX) {
            fs::remove_dir_all(entry.path())?;
        } else if is_safe_restore_marker_tmp(&entry.path(), &name)? {
            fs::remove_file(entry.path())?;
        }
    }
    fs::remove_file(active_intent)?;
    sync_directory(data_dir)?;
    Ok(())
}

/// Validates every existing fresh-install artifact before the installer creates,
/// removes, renames, or quarantines any path. The data-directory parent is a
/// trusted configuration boundary; this does not attempt to defend a hostile
/// ancestor replacement between this check and the subsequent filesystem work.
fn validate_fresh_restore_data_dir(
    data_dir: &Path,
    completion_marker_name: Option<&str>,
    expected_intent: &[u8],
) -> Result<(), DurabilityError> {
    if !path_has_state(data_dir)? {
        return Ok(());
    }

    let intent = data_dir.join(RESTORE_INTENT_FILE);
    let intent_metadata = match fs::symlink_metadata(&intent) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let (active_intent, recovery_identity) = if let Some(metadata) = intent_metadata {
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || read_bounded_regular_file(&intent, 4096)?.as_deref() != Some(expected_intent)
        {
            return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
        }
        (
            &intent,
            Some(RecoveryArtifactIdentity::Restore(
                parse_restore_intent_identity(expected_intent)
                    .ok_or_else(|| DurabilityError::DataDirNotFresh(data_dir.to_path_buf()))?,
            )),
        )
    } else {
        for entry in fs::read_dir(data_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !(is_safe_restore_marker_tmp(&entry.path(), &name)?
                || (name == crate::NODE_DATA_ROOT_LOCK_FILE
                    && is_valid_node_data_root_lock(&entry.path())?)
                || (name == RESTORE_RECEIPT_FILE
                    && is_valid_restore_install_receipt(&entry.path())?))
            {
                return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
            }
        }
        return Ok(());
    };

    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let marker_tmp = is_safe_restore_marker_tmp(&entry.path(), &name)?;
        if name.starts_with(RESTORE_MARKER_TMP_PREFIX) && !marker_tmp {
            return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
        }
        let is_staging = name.starts_with(RESTORE_STAGING_PREFIX);
        if is_staging
            && (!is_owned_generated_recovery_name(&name, RESTORE_STAGING_PREFIX)
                || !recovery_identity.as_ref().is_some_and(|identity| {
                    is_owned_recovery_directory(
                        &entry.path(),
                        &["sqlite", "ladybug", "kv", "consensus"],
                        RepairArtifactRole::Staging,
                        identity,
                    )
                    .unwrap_or(false)
                }))
        {
            return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
        }
        let owned = entry.path() == active_intent.as_path()
            || completion_marker_name.is_some_and(|marker| name == marker)
            || ["sqlite", "ladybug", "kv", "consensus"].contains(&name.as_ref())
            || (name == crate::NODE_DATA_ROOT_LOCK_FILE
                && is_valid_node_data_root_lock(&entry.path())?)
            || (name == RESTORE_RECEIPT_FILE && is_valid_restore_install_receipt(&entry.path())?)
            || marker_tmp
            || is_staging;
        if !owned {
            return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
        }
    }
    Ok(())
}

fn path_has_state(path: &Path) -> Result<bool, std::io::Error> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() {
        return Ok(true);
    }
    fs::read_dir(path)?
        .next()
        .transpose()
        .map(|entry| entry.is_some())
}

#[cfg(test)]
#[path = "durability/prestage_tests.rs"]
mod prestage_tests;

#[cfg(test)]
mod tests {
    #[cfg(feature = "kv")]
    use super::validate_local_recovery_view;
    use super::{
        capture_expected_local_restore_state, capture_expected_qlog_state,
        checkpoint_identity_configuration, checkpoint_restore_intent_bytes,
        install_prepared_checkpoint_for_rejoin_preserving_recorder, install_test_restore_lock_gate,
        install_test_restore_lock_path_replacement_hook, load_coordinator_restore_baseline,
        mark_durable_state, next_sync_recovery_retry, observe_durable_tip,
        publisher_lease_renewal_interval, retryable_sync_archive_error, snapshot_profile,
        validate_local_qlog, validate_restored_suffix, write_repair_artifact_ownership,
        CheckpointCoordinator, CheckpointInstallMode, CheckpointTip, CoordinatorState,
        DurabilityError, DurabilityHealth, DurabilityMode, ExecutionProfile,
        ExpectedLocalRestoreState, ExpectedQlogState, LogAnchor, LogHash, PendingLag,
        PreparedCheckpointRestore, RecoveryArtifactIdentity, RepairArtifactRole,
        RestoreCompletionMarker, SuccessorRestorePreparation, RESTORE_INTENT_FILE,
        SUCCESSOR_RESTORE_COMPLETE_FILE, SUCCESSOR_RESTORE_INTENT_FILE,
        SUCCESSOR_RESTORE_LOCK_FILE, SYNC_RECOVERY_RETRY_INITIAL,
    };
    use super::{install_prepared_checkpoint_to_fresh_data_dir, prepare_checkpoint_restore};
    #[cfg(feature = "kv")]
    use crate::KvCommandV1;
    #[cfg(any(feature = "sql", feature = "kv"))]
    use crate::{NodeConfig, NodeRuntime};
    use rhiza_archive::{CheckpointIdentity, CheckpointPublisherOptions, ObjectArchiveStore};
    use rhiza_core::{ConfigurationState, EntryType, LogEntry};
    use rhiza_log::{FileLogStore, LogStore};
    use rhiza_obj_store::{ObjStore, ObjStoreConfig};
    use rhiza_quepaxa::ThreeNodeConsensus;
    use std::{
        collections::BTreeMap,
        fs,
        path::{Path, PathBuf},
        sync::{mpsc, Arc, Barrier, Mutex},
        time::Duration,
    };

    #[test]
    fn genesis_restore_uses_checkpoint_configuration_digest() {
        let digest = LogHash::digest(&[b"membership"]);
        let identity = CheckpointIdentity::new("rhiza:sql:test", 1, 7, digest, 1);

        assert_eq!(
            checkpoint_identity_configuration(&identity),
            ConfigurationState::active(7, digest)
        );
    }

    #[test]
    fn completion_marker_names_follow_one_portable_ascii_grammar() {
        let cases = [
            ("identity.json", true),
            ("marker-1_2", true),
            ("CON.txt", false),
            ("con.JSON", false),
            ("COM1.log", false),
            ("lpt9.txt", false),
            ("file:ads", false),
            ("file.", false),
            ("file ", false),
            ("é.json", false),
            ("control\u{001f}.json", false),
            ("<", false),
            (">", false),
            ("\"", false),
            ("|", false),
            ("?", false),
            ("*", false),
            (".", false),
            ("..", false),
            ("dir/file", false),
            ("dir\\file", false),
            ("C:marker", false),
            ("\\\\server\\share", false),
            (".RHIZA-RESTORE.JSON", false),
            ("CONSENSUS", false),
        ];
        for (name, accepted) in cases {
            assert_eq!(
                RestoreCompletionMarker::new(name, b"marker").is_ok(),
                accepted,
                "completion marker grammar result for {name:?}"
            );
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RestoreTreeEntry {
        kind: &'static str,
        bytes: Vec<u8>,
        len: u64,
        #[cfg(unix)]
        mode: u32,
        #[cfg(unix)]
        dev: u64,
        #[cfg(unix)]
        ino: u64,
    }

    /// Captures all restore-owned and recorder state so a failed installer can
    /// prove it did not publish an intent, receipt, marker, staging artifact,
    /// qlog mutation, or recorder mutation. The hook deliberately replaces
    /// only `.node.lock`, so only that synchronization pathname is omitted.
    fn restore_tree_snapshot(root: &Path) -> BTreeMap<PathBuf, RestoreTreeEntry> {
        fn visit(root: &Path, path: &Path, entries: &mut BTreeMap<PathBuf, RestoreTreeEntry>) {
            for entry in fs::read_dir(path).unwrap() {
                let entry = entry.unwrap();
                let relative = entry.path().strip_prefix(root).unwrap().to_path_buf();
                if relative == Path::new(crate::NODE_DATA_ROOT_LOCK_FILE) {
                    continue;
                }
                let metadata = fs::symlink_metadata(entry.path()).unwrap();
                let kind = if metadata.file_type().is_symlink() {
                    "symlink"
                } else if metadata.is_dir() {
                    "directory"
                } else if metadata.is_file() {
                    "file"
                } else {
                    "other"
                };
                entries.insert(
                    relative.clone(),
                    RestoreTreeEntry {
                        kind,
                        bytes: if metadata.is_file() {
                            fs::read(entry.path()).unwrap()
                        } else {
                            Vec::new()
                        },
                        len: metadata.len(),
                        #[cfg(unix)]
                        mode: {
                            use std::os::unix::fs::MetadataExt;
                            metadata.mode()
                        },
                        #[cfg(unix)]
                        dev: {
                            use std::os::unix::fs::MetadataExt;
                            metadata.dev()
                        },
                        #[cfg(unix)]
                        ino: {
                            use std::os::unix::fs::MetadataExt;
                            metadata.ino()
                        },
                    },
                );
                if metadata.is_dir() {
                    visit(root, &entry.path(), entries);
                }
            }
        }

        let mut entries = BTreeMap::new();
        visit(root, root, &mut entries);
        entries
    }

    fn expected_restore_state_for_test(
        prepared: &PreparedCheckpointRestore,
        data_dir: &Path,
        node_id: &str,
        mode: CheckpointInstallMode,
        marker: Option<&str>,
    ) -> Result<ExpectedLocalRestoreState, DurabilityError> {
        capture_expected_local_restore_state(
            data_dir,
            mode,
            node_id,
            prepared.identity(),
            prepared.execution_profile(),
            checkpoint_identity_configuration(prepared.identity()),
            marker,
        )
    }

    #[cfg(feature = "sql")]
    async fn prepared_sql_target(root: &tempfile::TempDir) -> (PreparedCheckpointRestore, PathBuf) {
        let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
            ObjStore::new(ObjStoreConfig::Local {
                root: root.path().join("archive"),
            })
            .unwrap(),
            CheckpointIdentity::new(
                "rhiza:sql:cluster-a",
                1,
                1,
                LogHash::digest(&[b"node-test-config"]),
                1,
            ),
        );
        archive.initialize_checkpoint().await.unwrap();
        let prepared = prepare_checkpoint_restore(&archive).await.unwrap();
        let target = root.path().join("target");
        install_prepared_checkpoint_to_fresh_data_dir(
            &prepared,
            expected_restore_state_for_test(
                &prepared,
                &target,
                "node-1",
                CheckpointInstallMode::Fresh,
                Some("identity.json"),
            )
            .unwrap(),
            Some(RestoreCompletionMarker::new("identity.json", b"identity").unwrap()),
        )
        .unwrap();
        (prepared, target)
    }

    #[cfg(feature = "sql")]
    fn replace_regular_file_with_same_bytes(path: &Path, bytes: &[u8]) {
        let replacement = path.with_extension("identity-replacement");
        fs::write(&replacement, bytes).unwrap();
        fs::rename(replacement, path).unwrap();
    }

    #[cfg(feature = "sql")]
    fn qlog_state_for_test(
        prepared: &PreparedCheckpointRestore,
        target: &Path,
    ) -> ExpectedQlogState {
        capture_expected_qlog_state(
            target,
            prepared.identity(),
            checkpoint_identity_configuration(prepared.identity()),
        )
        .unwrap()
    }

    #[cfg(feature = "sql")]
    fn assert_same_logical_qlog_with_replaced_paths(
        before: &ExpectedQlogState,
        after: &ExpectedQlogState,
    ) {
        match (before, after) {
            (
                ExpectedQlogState::Logical {
                    state: before_state,
                    paths: before_paths,
                },
                ExpectedQlogState::Logical {
                    state: after_state,
                    paths: after_paths,
                },
            ) => {
                assert_eq!(before_state, after_state);
                assert_ne!(before_paths, after_paths);
            }
            _ => panic!("qlog replacement fixture must retain an exact logical qlog state"),
        }
    }

    #[cfg(feature = "sql")]
    fn copy_regular_tree(source: &Path, destination: &Path) {
        fs::create_dir(destination).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let source_path = entry.path();
            let destination_path = destination.join(entry.file_name());
            let metadata = fs::symlink_metadata(&source_path).unwrap();
            assert!(!metadata.file_type().is_symlink());
            if metadata.is_dir() {
                copy_regular_tree(&source_path, &destination_path);
            } else {
                assert!(metadata.is_file());
                fs::copy(source_path, destination_path).unwrap();
            }
        }
    }

    #[cfg(feature = "sql")]
    fn seed_qlog_identity_test_entry(prepared: &PreparedCheckpointRestore, target: &Path) {
        let identity = prepared.identity();
        let log = FileLogStore::open(
            target.join("consensus/log"),
            identity.cluster_id(),
            identity.epoch(),
            identity.config_id(),
        )
        .unwrap();
        let hash = LogEntry::calculate_hash(
            identity.cluster_id(),
            1,
            identity.epoch(),
            identity.config_id(),
            EntryType::Noop,
            LogHash::ZERO,
            &[],
        );
        log.append(&LogEntry {
            cluster_id: identity.cluster_id().into(),
            epoch: identity.epoch(),
            config_id: identity.config_id(),
            index: 1,
            entry_type: EntryType::Noop,
            payload: Vec::new(),
            prev_hash: LogHash::ZERO,
            hash,
        })
        .unwrap();
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn held_installer_lock_rejects_node_runtime_open() {
        let root = tempfile::tempdir().unwrap();
        let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
            ObjStore::new(ObjStoreConfig::Local {
                root: root.path().join("archive"),
            })
            .unwrap(),
            CheckpointIdentity::new(
                "rhiza:sql:cluster-a",
                1,
                1,
                LogHash::digest(&[b"node-test-config"]),
                1,
            ),
        );
        archive.initialize_checkpoint().await.unwrap();
        let prepared = prepare_checkpoint_restore(&archive).await.unwrap();
        let target = root.path().join("target");
        install_prepared_checkpoint_to_fresh_data_dir(
            &prepared,
            expected_restore_state_for_test(
                &prepared,
                &target,
                "node-1",
                CheckpointInstallMode::Fresh,
                Some("identity.json"),
            )
            .unwrap(),
            Some(RestoreCompletionMarker::new("identity.json", b"identity").unwrap()),
        )
        .unwrap();
        let expected = expected_restore_state_for_test(
            &prepared,
            &target,
            "node-1",
            CheckpointInstallMode::RejoinPreservingRecorder,
            Some("identity.json"),
        )
        .unwrap();
        let (entered, entered_rx) = mpsc::sync_channel(1);
        let (release, release_rx) = mpsc::sync_channel(1);
        let _gate = install_test_restore_lock_gate(target.clone(), entered, release_rx);
        let worker = std::thread::spawn(move || {
            install_prepared_checkpoint_for_rejoin_preserving_recorder(
                &prepared,
                expected,
                RestoreCompletionMarker::new("identity.json", b"identity").unwrap(),
            )
        });
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("installer must acquire its lock before runtime contention check");

        let config = NodeConfig::new_embedded(
            "cluster-a",
            "node-1",
            target.clone(),
            1,
            1,
            ["node-1", "node-2", "node-3"],
        )
        .unwrap();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                "rhiza:sql:cluster-a",
                "node-1",
                1,
                1,
                [
                    root.path().join("recorders/node-1"),
                    root.path().join("recorders/node-2"),
                    root.path().join("recorders/node-3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        );
        assert!(matches!(
            NodeRuntime::open(config, consensus, &[]),
            Err(crate::NodeError::DataRootLocked(path)) if path == target
        ));
        release.send(()).unwrap();
        assert!(worker.join().unwrap().is_ok());
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn missing_lock_path_replacement_after_acquisition_fails_before_restore_mutation() {
        let root = tempfile::tempdir().unwrap();
        let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
            ObjStore::new(ObjStoreConfig::Local {
                root: root.path().join("archive"),
            })
            .unwrap(),
            CheckpointIdentity::new(
                "rhiza:sql:cluster-a",
                1,
                1,
                LogHash::digest(&[b"node-test-config"]),
                1,
            ),
        );
        archive.initialize_checkpoint().await.unwrap();
        let prepared = prepare_checkpoint_restore(&archive).await.unwrap();
        let target = root.path().join("target");
        install_prepared_checkpoint_to_fresh_data_dir(
            &prepared,
            expected_restore_state_for_test(
                &prepared,
                &target,
                "node-1",
                CheckpointInstallMode::Fresh,
                Some("identity.json"),
            )
            .unwrap(),
            Some(RestoreCompletionMarker::new("identity.json", b"identity").unwrap()),
        )
        .unwrap();

        // The expected state deliberately begins with no lock. The hook then
        // replaces the lock A owns with a separately locked zero-length L2.
        fs::remove_file(target.join(crate::NODE_DATA_ROOT_LOCK_FILE)).unwrap();
        let recorder_sentinel = target.join("recorders/node-1/retained.bin");
        fs::create_dir_all(recorder_sentinel.parent().unwrap()).unwrap();
        fs::write(&recorder_sentinel, b"recorder bytes must survive").unwrap();
        let expected = expected_restore_state_for_test(
            &prepared,
            &target,
            "node-1",
            CheckpointInstallMode::RejoinPreservingRecorder,
            Some("identity.json"),
        )
        .unwrap();
        let before = restore_tree_snapshot(&target);
        let (replacement_tx, replacement_rx) = mpsc::sync_channel(1);
        let _hook = install_test_restore_lock_path_replacement_hook(target.clone(), replacement_tx);

        let error = install_prepared_checkpoint_for_rejoin_preserving_recorder(
            &prepared,
            expected,
            RestoreCompletionMarker::new("identity.json", b"identity").unwrap(),
        )
        .unwrap_err();
        let replacement_lock = replacement_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("hook must retain the replacement L2 lock");

        assert!(matches!(
            error,
            DurabilityError::SnapshotVerification(message)
                if message.contains("does not identify the acquired lock")
        ));
        assert_eq!(restore_tree_snapshot(&target), before);
        assert_eq!(
            fs::read(&recorder_sentinel).unwrap(),
            b"recorder bytes must survive"
        );
        assert!(replacement_lock.metadata().unwrap().is_file());
        assert_eq!(replacement_lock.metadata().unwrap().len(), 0);
        drop(replacement_lock);
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn same_size_completion_marker_replacement_after_capture_fails_before_mutation() {
        let root = tempfile::tempdir().unwrap();
        let (prepared, target) = prepared_sql_target(&root).await;
        let expected = expected_restore_state_for_test(
            &prepared,
            &target,
            "node-1",
            CheckpointInstallMode::RejoinPreservingRecorder,
            Some("identity.json"),
        )
        .unwrap();
        let marker = target.join("identity.json");
        replace_regular_file_with_same_bytes(&marker, &fs::read(&marker).unwrap());
        let before = restore_tree_snapshot(&target);

        let error = install_prepared_checkpoint_for_rejoin_preserving_recorder(
            &prepared,
            expected,
            RestoreCompletionMarker::new("identity.json", b"identity").unwrap(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DurabilityError::SnapshotVerification(message)
                if message.contains("restore control file identity.json changed")
        ));
        assert_eq!(restore_tree_snapshot(&target), before);
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn same_size_restore_intent_replacement_after_capture_fails_before_mutation() {
        let root = tempfile::tempdir().unwrap();
        let (prepared, target) = prepared_sql_target(&root).await;
        let intent = checkpoint_restore_intent_bytes(
            prepared.identity(),
            "node-1",
            prepared.execution_profile(),
            prepared.checkpoint_root(),
        )
        .unwrap();
        let intent_path = target.join(RESTORE_INTENT_FILE);
        fs::write(&intent_path, &intent).unwrap();
        let expected = expected_restore_state_for_test(
            &prepared,
            &target,
            "node-1",
            CheckpointInstallMode::RejoinPreservingRecorder,
            Some("identity.json"),
        )
        .unwrap();
        replace_regular_file_with_same_bytes(&intent_path, &intent);
        let before = restore_tree_snapshot(&target);

        let error = install_prepared_checkpoint_for_rejoin_preserving_recorder(
            &prepared,
            expected,
            RestoreCompletionMarker::new("identity.json", b"identity").unwrap(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DurabilityError::SnapshotVerification(message)
                if message.contains("restore control file .rhiza-restore.json changed")
        ));
        assert_eq!(restore_tree_snapshot(&target), before);
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn same_size_existing_lock_replacement_after_capture_fails_before_mutation() {
        let root = tempfile::tempdir().unwrap();
        let (prepared, target) = prepared_sql_target(&root).await;
        let expected = expected_restore_state_for_test(
            &prepared,
            &target,
            "node-1",
            CheckpointInstallMode::RejoinPreservingRecorder,
            Some("identity.json"),
        )
        .unwrap();
        let lock_path = target.join(crate::NODE_DATA_ROOT_LOCK_FILE);
        replace_regular_file_with_same_bytes(&lock_path, b"");
        let before = restore_tree_snapshot(&target);

        let error = install_prepared_checkpoint_for_rejoin_preserving_recorder(
            &prepared,
            expected,
            RestoreCompletionMarker::new("identity.json", b"identity").unwrap(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DurabilityError::SnapshotVerification(message)
                if message.contains("node data lock changed after expected state capture")
        ));
        assert_eq!(restore_tree_snapshot(&target), before);
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn data_directory_replacement_after_capture_fails_before_mutation() {
        let root = tempfile::tempdir().unwrap();
        let (prepared, target) = prepared_sql_target(&root).await;
        let expected = expected_restore_state_for_test(
            &prepared,
            &target,
            "node-1",
            CheckpointInstallMode::RejoinPreservingRecorder,
            Some("identity.json"),
        )
        .unwrap();
        let displaced = root.path().join("displaced-target");
        fs::rename(&target, &displaced).unwrap();
        fs::create_dir(&target).unwrap();
        let target_before = restore_tree_snapshot(&target);
        let displaced_before = restore_tree_snapshot(&displaced);

        let error = install_prepared_checkpoint_for_rejoin_preserving_recorder(
            &prepared,
            expected,
            RestoreCompletionMarker::new("identity.json", b"identity").unwrap(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DurabilityError::SnapshotVerification(message)
                if message.contains("restore data directory changed after expected state capture")
        ));
        assert_eq!(restore_tree_snapshot(&target), target_before);
        assert_eq!(restore_tree_snapshot(&displaced), displaced_before);
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn same_content_qlog_root_replacement_after_capture_fails_before_mutation() {
        let root = tempfile::tempdir().unwrap();
        let (prepared, target) = prepared_sql_target(&root).await;
        seed_qlog_identity_test_entry(&prepared, &target);
        let qlog_dir = target.join("consensus/log");
        let qlog_before = qlog_state_for_test(&prepared, &target);
        let expected = expected_restore_state_for_test(
            &prepared,
            &target,
            "node-1",
            CheckpointInstallMode::RejoinPreservingRecorder,
            Some("identity.json"),
        )
        .unwrap();
        let displaced_qlog = root.path().join("replacement-owned-qlog");
        fs::rename(&qlog_dir, &displaced_qlog).unwrap();
        copy_regular_tree(&displaced_qlog, &qlog_dir);
        let qlog_after = qlog_state_for_test(&prepared, &target);
        assert_same_logical_qlog_with_replaced_paths(&qlog_before, &qlog_after);
        let target_before = restore_tree_snapshot(&target);
        let displaced_before = restore_tree_snapshot(&displaced_qlog);

        let error = install_prepared_checkpoint_for_rejoin_preserving_recorder(
            &prepared,
            expected,
            RestoreCompletionMarker::new("identity.json", b"identity").unwrap(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DurabilityError::SnapshotVerification(message)
                if message.contains("local qlog state changed after expected state capture")
        ));
        assert_eq!(restore_tree_snapshot(&target), target_before);
        assert_eq!(restore_tree_snapshot(&displaced_qlog), displaced_before);
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn same_content_qlog_entry_replacement_after_capture_fails_before_mutation() {
        let root = tempfile::tempdir().unwrap();
        let (prepared, target) = prepared_sql_target(&root).await;
        seed_qlog_identity_test_entry(&prepared, &target);
        let qlog_dir = target.join("consensus/log");
        let qlog_before = qlog_state_for_test(&prepared, &target);
        let expected = expected_restore_state_for_test(
            &prepared,
            &target,
            "node-1",
            CheckpointInstallMode::RejoinPreservingRecorder,
            Some("identity.json"),
        )
        .unwrap();
        let entry = fs::read_dir(&qlog_dir)
            .unwrap()
            .map(Result::unwrap)
            .map(|entry| entry.path())
            .find(|path| fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file()))
            .expect("fresh restored qlog must contain a regular anchor or segment entry");
        replace_regular_file_with_same_bytes(&entry, &fs::read(&entry).unwrap());
        let qlog_after = qlog_state_for_test(&prepared, &target);
        assert_same_logical_qlog_with_replaced_paths(&qlog_before, &qlog_after);
        let before = restore_tree_snapshot(&target);

        let error = install_prepared_checkpoint_for_rejoin_preserving_recorder(
            &prepared,
            expected,
            RestoreCompletionMarker::new("identity.json", b"identity").unwrap(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DurabilityError::SnapshotVerification(message)
                if message.contains("local qlog state changed after expected state capture")
        ));
        assert_eq!(restore_tree_snapshot(&target), before);
    }

    #[test]
    fn snapshot_profile_requires_a_canonical_effective_cluster_identity() {
        assert_eq!(
            snapshot_profile("rhiza:sql:cluster-a").unwrap(),
            ExecutionProfile::Sqlite
        );
        assert_eq!(
            snapshot_profile("rhiza:graph:cluster-a").unwrap(),
            ExecutionProfile::Graph
        );
        assert_eq!(
            snapshot_profile("rhiza:kv:cluster-a").unwrap(),
            ExecutionProfile::Kv
        );
        assert!(matches!(
            snapshot_profile("cluster-a"),
            Err(DurabilityError::SnapshotVerification(_))
        ));
        assert!(snapshot_profile("rhiza:graph:").is_err());
    }

    #[tokio::test]
    async fn prepared_install_rejects_inconsistent_root_or_profile_before_mutation() {
        let root = tempfile::tempdir().unwrap();
        let store = ObjStore::new(ObjStoreConfig::Local {
            root: root.path().join("archive"),
        })
        .unwrap();
        let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
            store,
            CheckpointIdentity::new(
                "rhiza:sql:cluster-a",
                1,
                1,
                LogHash::digest(&[b"node-test-config"]),
                1,
            ),
        );
        archive.initialize_checkpoint().await.unwrap();
        let mut prepared = prepare_checkpoint_restore(&archive).await.unwrap();
        let data_dir = root.path().join("data");
        let valid_root = prepared.checkpoint_root;

        let expected = expected_restore_state_for_test(
            &prepared,
            &data_dir,
            "node-1",
            CheckpointInstallMode::Fresh,
            None,
        )
        .unwrap();
        prepared.checkpoint_root = LogAnchor::new(1, LogHash::digest(&[b"wrong-root"]));
        assert!(matches!(
            install_prepared_checkpoint_to_fresh_data_dir(&prepared, expected, None),
            Err(DurabilityError::SnapshotVerification(_))
        ));
        assert!(!data_dir.exists());

        prepared.checkpoint_root = valid_root;
        let expected = expected_restore_state_for_test(
            &prepared,
            &data_dir,
            "node-1",
            CheckpointInstallMode::Fresh,
            None,
        )
        .unwrap();
        prepared.execution_profile = ExecutionProfile::Graph;
        assert!(matches!(
            install_prepared_checkpoint_to_fresh_data_dir(&prepared, expected, None),
            Err(DurabilityError::SnapshotVerification(_))
        ));
        assert!(!data_dir.exists());
    }

    #[test]
    fn generic_restore_prepare_keeps_prefix_spoofed_staging_and_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let identity = CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            LogHash::digest(&[b"node-test-config"]),
            1,
        );
        let intent = super::checkpoint_restore_intent_bytes(
            &identity,
            "node-1",
            ExecutionProfile::Sqlite,
            LogAnchor::new(0, LogHash::ZERO),
        )
        .unwrap();
        std::fs::write(root.path().join(RESTORE_INTENT_FILE), &intent).unwrap();
        let staging = root.path().join(".restore-stage-4242-1");
        std::fs::create_dir_all(staging.join("sqlite")).unwrap();

        assert!(matches!(
            super::prepare_fresh_restore_data_dir(root.path(), None, &intent),
            Err(DurabilityError::DataDirNotFresh(_))
        ));
        assert!(staging.join("sqlite").is_dir());
    }

    #[test]
    fn bounded_regular_reader_rejects_file_larger_than_its_limit() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("oversized");
        std::fs::write(&path, b"12345").unwrap();

        assert!(super::read_bounded_regular_file(&path, 4).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"12345");
    }

    #[cfg(feature = "sql")]
    #[test]
    fn pending_qefx_gc_record_never_regresses_or_conflicts_at_one_slot() {
        let root = tempfile::tempdir().unwrap();
        let record = |through_slot, checkpoint_hash: &str| super::PendingQefxGc {
            cluster_id: "cluster-a".into(),
            epoch: 1,
            config_id: 1,
            config_digest: LogHash::digest(&[b"config"]).to_hex(),
            through_slot,
            checkpoint_hash: checkpoint_hash.into(),
            manifest_digest: LogHash::digest(&[b"manifest", &through_slot.to_be_bytes()]).to_hex(),
        };
        let newer = record(12, &LogHash::digest(&[b"tip-12"]).to_hex());
        super::persist_pending_qefx_gc(root.path(), &newer).unwrap();

        super::persist_pending_qefx_gc(
            root.path(),
            &record(11, &LogHash::digest(&[b"tip-11"]).to_hex()),
        )
        .unwrap();
        assert_eq!(
            super::load_pending_qefx_gc(root.path())
                .unwrap()
                .unwrap()
                .through_slot,
            12
        );

        assert!(super::persist_pending_qefx_gc(
            root.path(),
            &record(12, &LogHash::digest(&[b"conflict"]).to_hex()),
        )
        .is_err());
        assert_eq!(
            super::load_pending_qefx_gc(root.path()).unwrap().unwrap(),
            newer
        );
    }

    #[test]
    fn rejoin_artifact_cleanup_removes_only_owned_stale_stage_and_quarantine() {
        let root = tempfile::tempdir().unwrap();
        let checkpoint = LogAnchor::new(0, LogHash::ZERO);
        let identity = RecoveryArtifactIdentity::Restore(super::restore_intent_identity(
            &CheckpointIdentity::new(
                "rhiza:sql:cluster-a",
                1,
                1,
                LogHash::digest(&[b"node-test-config"]),
                1,
            ),
            "node-1",
            ExecutionProfile::Sqlite,
            checkpoint,
        ));
        let stage = root.path().join(".restore-stage-4242-1");
        std::fs::create_dir_all(stage.join("sqlite")).unwrap();
        write_repair_artifact_ownership(&stage, RepairArtifactRole::Staging, &identity).unwrap();
        let quarantine = root.path().join(".rebuildable-quarantine-4242-2");
        std::fs::create_dir_all(quarantine.join("sqlite")).unwrap();
        write_repair_artifact_ownership(&quarantine, RepairArtifactRole::Quarantine, &identity)
            .unwrap();

        super::cleanup_owned_recovery_artifacts(root.path(), &identity).unwrap();
        assert!(!stage.exists());
        assert!(!quarantine.exists());
    }

    #[test]
    fn rejoin_artifact_cleanup_keeps_foreign_prefix_artifact_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        let spoof = root.path().join(".restore-stage-foreign");
        std::fs::create_dir(&spoof).unwrap();
        let identity = RecoveryArtifactIdentity::Restore(super::restore_intent_identity(
            &CheckpointIdentity::new(
                "rhiza:sql:cluster-a",
                1,
                1,
                LogHash::digest(&[b"node-test-config"]),
                1,
            ),
            "node-1",
            ExecutionProfile::Sqlite,
            LogAnchor::new(0, LogHash::ZERO),
        ));

        assert!(matches!(
            super::cleanup_owned_recovery_artifacts(root.path(), &identity),
            Err(DurabilityError::DataDirNotFresh(_))
        ));
        assert!(spoof.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn successor_completion_keeps_existing_complete_symlink_and_intent() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let receipt = br#"{"cluster_id":"rhiza:sql:cluster-a","epoch":1,"target_config_id":2,"recovery_generation":1,"node_id":"node-1","membership_digest":"0000000000000000000000000000000000000000000000000000000000000000","predecessor_config_id":1,"stop_index":0,"stop_hash":"0000000000000000000000000000000000000000000000000000000000000000"}"#;
        let intent = root.path().join(SUCCESSOR_RESTORE_INTENT_FILE);
        std::fs::write(&intent, receipt).unwrap();
        let target = root.path().join("target");
        std::fs::write(&target, b"do not replace").unwrap();
        let complete = root.path().join(SUCCESSOR_RESTORE_COMPLETE_FILE);
        symlink(&target, &complete).unwrap();
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.path().join(SUCCESSOR_RESTORE_LOCK_FILE))
            .unwrap();
        let preparation = SuccessorRestorePreparation {
            tip: CheckpointTip::new(0, LogHash::ZERO),
            data_dir: root.path().to_path_buf(),
            identity: receipt.to_vec(),
            requires_recorder_install: true,
            _lock: lock,
        };

        assert!(preparation.complete().is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"do not replace");
        assert_eq!(std::fs::read(&intent).unwrap(), receipt);
        assert!(std::fs::symlink_metadata(&complete)
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[test]
    fn sqlite_restore_suffix_rejects_noncanonical_commands_during_preflight() {
        let payload = b"put\tnoncanonical\tkey\tvalue".to_vec();
        let entry = LogEntry {
            cluster_id: "rhiza:sql:cluster-a".into(),
            epoch: 1,
            config_id: 1,
            index: 1,
            entry_type: EntryType::Command,
            prev_hash: LogHash::ZERO,
            hash: LogEntry::calculate_hash(
                "rhiza:sql:cluster-a",
                1,
                1,
                1,
                EntryType::Command,
                LogHash::ZERO,
                &payload,
            ),
            payload,
        };

        assert!(matches!(
            validate_restored_suffix(ExecutionProfile::Sqlite, &[entry]),
            Err(DurabilityError::SnapshotVerification(message)) if message.contains("QEFX")
        ));
    }

    #[test]
    fn local_qlog_accepts_ahead_tip_when_checkpoint_entry_is_retained() {
        let root = tempfile::tempdir().unwrap();
        let identity = CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            LogHash::digest(&[b"node-test-config"]),
            1,
        );
        let log = FileLogStore::open(
            root.path().join("consensus/log"),
            identity.cluster_id(),
            1,
            1,
        )
        .unwrap();
        let entry = |index, previous| {
            let hash = LogEntry::calculate_hash(
                identity.cluster_id(),
                index,
                1,
                1,
                EntryType::Noop,
                previous,
                &[],
            );
            LogEntry {
                cluster_id: identity.cluster_id().into(),
                epoch: 1,
                config_id: 1,
                index,
                entry_type: EntryType::Noop,
                payload: Vec::new(),
                prev_hash: previous,
                hash,
            }
        };
        let first = entry(1, LogHash::ZERO);
        let second = entry(2, first.hash);
        log.append_batch(&[first.clone(), second.clone()]).unwrap();

        assert_eq!(
            validate_local_qlog(
                root.path(),
                &identity,
                rhiza_core::LogAnchor::new(first.index, first.hash),
            )
            .unwrap(),
            rhiza_core::LogAnchor::new(2, second.hash)
        );
        assert!(validate_local_qlog(
            root.path(),
            &identity,
            rhiza_core::LogAnchor::new(2, LogHash::digest(&[b"conflicting"])),
        )
        .is_err());
        assert!(validate_local_qlog(
            root.path(),
            &identity,
            rhiza_core::LogAnchor::new(3, LogHash::digest(&[b"ahead checkpoint"])),
        )
        .is_err());
    }

    #[test]
    fn local_qlog_treats_absent_log_as_genesis_only() {
        let root = tempfile::tempdir().unwrap();
        let identity = CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            LogHash::digest(&[b"node-test-config"]),
            1,
        );
        let genesis = rhiza_core::LogAnchor::new(0, LogHash::ZERO);

        assert_eq!(
            validate_local_qlog(root.path(), &identity, genesis).unwrap(),
            genesis
        );
        assert!(validate_local_qlog(
            root.path(),
            &identity,
            rhiza_core::LogAnchor::new(1, LogHash::digest(&[b"checkpoint"])),
        )
        .is_err());
    }

    #[test]
    fn restore_intent_survives_failed_or_pre_receipt_staging_for_retry() {
        let root = tempfile::tempdir().unwrap();
        let data_dir = root.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let intent = super::checkpoint_restore_intent_bytes(
            &CheckpointIdentity::new(
                "rhiza:sql:cluster-a",
                1,
                1,
                LogHash::digest(&[b"node-test-config"]),
                1,
            ),
            "node-1",
            ExecutionProfile::Sqlite,
            rhiza_core::LogAnchor::new(0, LogHash::ZERO),
        )
        .unwrap();
        std::fs::write(data_dir.join(RESTORE_INTENT_FILE), &intent).unwrap();
        std::fs::create_dir(data_dir.join("identity.json")).unwrap();
        let staging = super::create_restore_staging_dir(&data_dir, None).unwrap();

        assert!(super::publish_restore_staging(
            &staging,
            &data_dir,
            Some(("identity.json", b"identity-fixture")),
        )
        .is_err());
        assert_eq!(
            std::fs::read(data_dir.join(RESTORE_INTENT_FILE)).unwrap(),
            intent
        );

        std::fs::remove_dir(data_dir.join("identity.json")).unwrap();
        let retry_staging = super::create_restore_staging_dir(&data_dir, None).unwrap();
        super::publish_restore_staging(
            &retry_staging,
            &data_dir,
            Some(("identity.json", b"identity-fixture")),
        )
        .unwrap();
        assert_eq!(
            std::fs::read(data_dir.join("identity.json")).unwrap(),
            b"identity-fixture"
        );
        assert_eq!(
            std::fs::read(data_dir.join(RESTORE_INTENT_FILE)).unwrap(),
            intent,
            "the generic intent may be removed only after the durable install receipt"
        );
    }

    #[cfg(feature = "kv")]
    #[tokio::test]
    async fn kv_compacted_rejoin_restores_missing_or_corrupt_views_without_touching_recorder() {
        let root = tempfile::tempdir().unwrap();
        let identity = CheckpointIdentity::new(
            "rhiza:kv:cluster-a",
            1,
            1,
            LogHash::digest(&[b"node-test-config"]),
            1,
        );
        let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
            ObjStore::new(ObjStoreConfig::Local {
                root: root.path().join("archive"),
            })
            .unwrap(),
            identity.clone(),
        );
        archive.initialize_checkpoint().await.unwrap();
        let source_dir = root.path().join("source");
        let config = NodeConfig::new_embedded(
            "cluster-a",
            "node-1",
            source_dir,
            1,
            1,
            ["node-1", "node-2", "node-3"],
        )
        .unwrap()
        .with_execution_profile(ExecutionProfile::Kv)
        .unwrap()
        .with_recovery_generation(1)
        .unwrap();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                "rhiza:kv:cluster-a",
                "node-1",
                1,
                1,
                [
                    root.path().join("recorders/node-1"),
                    root.path().join("recorders/node-2"),
                    root.path().join("recorders/node-3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        );
        let runtime = NodeRuntime::open(config, consensus, &[]).unwrap();
        let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
            .await
            .unwrap();
        let committed = runtime
            .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap())
            .unwrap();
        coordinator.note_committed(committed.applied_index());
        coordinator
            .flush_runtime(&runtime, committed.applied_index())
            .await
            .unwrap();
        let checkpoint_root = runtime.checkpoint_compact(&coordinator).await.unwrap();

        let target = root.path().join("target");
        let prepared = prepare_checkpoint_restore(&archive).await.unwrap();
        install_prepared_checkpoint_to_fresh_data_dir(
            &prepared,
            expected_restore_state_for_test(
                &prepared,
                &target,
                "node-1",
                CheckpointInstallMode::Fresh,
                None,
            )
            .unwrap(),
            None,
        )
        .unwrap();
        validate_local_recovery_view(
            &target,
            &identity,
            "node-1",
            ExecutionProfile::Kv,
            *checkpoint_root.compacted(),
        )
        .unwrap();
        std::fs::create_dir_all(target.join("recorder")).unwrap();
        std::fs::write(target.join("recorder/sentinel"), b"keep-me").unwrap();

        std::fs::remove_dir_all(target.join("consensus")).unwrap();
        assert!(validate_local_recovery_view(
            &target,
            &identity,
            "node-1",
            ExecutionProfile::Kv,
            *checkpoint_root.compacted(),
        )
        .is_err());
        install_prepared_checkpoint_for_rejoin_preserving_recorder(
            &prepared,
            expected_restore_state_for_test(
                &prepared,
                &target,
                "node-1",
                CheckpointInstallMode::RejoinPreservingRecorder,
                Some("identity.json"),
            )
            .unwrap(),
            RestoreCompletionMarker::new("identity.json", b"identity-fixture").unwrap(),
        )
        .unwrap();

        std::fs::write(target.join("kv/data.redb"), b"corrupt").unwrap();
        assert!(validate_local_recovery_view(
            &target,
            &identity,
            "node-1",
            ExecutionProfile::Kv,
            *checkpoint_root.compacted(),
        )
        .is_err());
        install_prepared_checkpoint_for_rejoin_preserving_recorder(
            &prepared,
            expected_restore_state_for_test(
                &prepared,
                &target,
                "node-1",
                CheckpointInstallMode::RejoinPreservingRecorder,
                Some("identity.json"),
            )
            .unwrap(),
            RestoreCompletionMarker::new("identity.json", b"identity-fixture").unwrap(),
        )
        .unwrap();
        assert_eq!(
            std::fs::read(target.join("recorder/sentinel")).unwrap(),
            b"keep-me"
        );
    }

    #[test]
    fn concurrent_flush_completion_cannot_regress_the_durable_tip() {
        let newer = CheckpointTip::new(8, LogHash::digest(&[b"newer"]));
        let older = CheckpointTip::new(4, LogHash::digest(&[b"older"]));
        let mut state = CoordinatorState {
            durable_tip: newer,
            committed_index: 8,
            pending_lag: None,
            health: DurabilityHealth::Available,
        };

        mark_durable_state(&mut state, older);

        assert_eq!(state.durable_tip, newer);
    }

    #[test]
    fn checkpoint_observation_rejects_same_index_hash_conflict_without_mutation() {
        let current = CheckpointTip::new(8, LogHash::digest(&[b"current"]));
        let conflicting = CheckpointTip::new(8, LogHash::digest(&[b"conflicting"]));
        let state = Mutex::new(CoordinatorState {
            durable_tip: current,
            committed_index: 8,
            pending_lag: None,
            health: DurabilityHealth::Available,
        });

        assert!(matches!(
            observe_durable_tip(&state, conflicting),
            Err(DurabilityError::SnapshotVerification(_))
        ));
        assert_eq!(state.lock().unwrap().durable_tip, current);
    }

    #[test]
    fn checkpoint_observation_rejects_remote_rollback_without_mutation() {
        let current = CheckpointTip::new(8, LogHash::digest(&[b"current"]));
        let older = CheckpointTip::new(7, LogHash::digest(&[b"older"]));
        let state = Mutex::new(CoordinatorState {
            durable_tip: current,
            committed_index: 8,
            pending_lag: None,
            health: DurabilityHealth::Available,
        });

        assert!(matches!(
            observe_durable_tip(&state, older),
            Err(DurabilityError::SnapshotVerification(_))
        ));
        assert_eq!(state.lock().unwrap().durable_tip, current);
    }

    #[test]
    fn concurrent_conflicting_checkpoint_observations_accept_exactly_one_tip() {
        let state = Arc::new(Mutex::new(CoordinatorState {
            durable_tip: CheckpointTip::new(0, LogHash::ZERO),
            committed_index: 0,
            pending_lag: None,
            health: DurabilityHealth::Available,
        }));
        let start = Arc::new(Barrier::new(3));
        let results = [b"first".as_slice(), b"second".as_slice()]
            .into_iter()
            .map(|label| {
                let state = Arc::clone(&state);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    let tip = CheckpointTip::new(1, LogHash::digest(&[label]));
                    start.wait();
                    (tip, observe_durable_tip(&state, tip))
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        let results = results
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            results.iter().filter(|(_, result)| result.is_ok()).count(),
            1
        );
        assert_eq!(
            results.iter().filter(|(_, result)| result.is_err()).count(),
            1
        );
        let accepted = results
            .iter()
            .find_map(|(tip, result)| result.is_ok().then_some(*tip))
            .unwrap();
        assert_eq!(state.lock().unwrap().durable_tip, accepted);
    }

    #[test]
    fn durable_progress_clears_lag_only_after_reaching_the_committed_index() {
        let mut state = CoordinatorState {
            durable_tip: CheckpointTip::new(2, LogHash::digest(&[b"two"])),
            committed_index: 4,
            pending_lag: Some(PendingLag::Recovered),
            health: DurabilityHealth::Unavailable,
        };
        let partial = CheckpointTip::new(3, LogHash::digest(&[b"three"]));
        mark_durable_state(&mut state, partial);
        assert!(state.pending_lag.is_some());
        assert_eq!(state.health, DurabilityHealth::Unavailable);

        let complete = CheckpointTip::new(4, LogHash::digest(&[b"four"]));
        mark_durable_state(&mut state, complete);

        assert_eq!(state.durable_tip, complete);
        assert!(state.pending_lag.is_none());
        assert_eq!(state.health, DurabilityHealth::Available);
    }

    #[test]
    fn publisher_lease_renewal_interval_is_positive_and_before_lease_expiry() {
        for lease_duration_ms in [1, 2, 3, 5_000, 60_000] {
            let lease_duration = Duration::from_millis(lease_duration_ms);
            let renewal_interval = publisher_lease_renewal_interval(lease_duration_ms);
            assert!(!renewal_interval.is_zero());
            assert!(renewal_interval <= lease_duration / 3);
        }
    }

    #[test]
    fn sync_archive_retry_classifier_preserves_structural_fail_closed_errors() {
        assert!(retryable_sync_archive_error(
            &rhiza_archive::Error::CompareAndSwapRetriesExhausted { attempts: 16 }
        ));
        assert!(retryable_sync_archive_error(
            &rhiza_archive::Error::GcBarrierActive {
                operation_id: "gc".into(),
            }
        ));
        assert!(retryable_sync_archive_error(
            &rhiza_archive::Error::GcBarrierBusy { until_ms: 1 }
        ));
        assert!(retryable_sync_archive_error(
            &rhiza_archive::Error::GcLeaseMissing {
                lease_id: "publisher".into(),
            }
        ));
        assert!(!retryable_sync_archive_error(
            &rhiza_archive::Error::InvalidGc("corrupt control state".into())
        ));
        assert!(!retryable_sync_archive_error(
            &rhiza_archive::Error::GenerationRetired {
                generation: 1,
                plan_hash: "plan".into(),
            }
        ));
        assert!(!retryable_sync_archive_error(
            &rhiza_archive::Error::InvalidCheckpoint("corrupt manifest".into())
        ));
    }

    #[test]
    fn sync_recovery_retry_backoff_is_bounded() {
        let mut delay = SYNC_RECOVERY_RETRY_INITIAL;
        for expected in [
            Duration::from_millis(200),
            Duration::from_millis(400),
            Duration::from_millis(800),
            Duration::from_secs(1),
            Duration::from_secs(1),
        ] {
            delay = next_sync_recovery_retry(delay);
            assert_eq!(delay, expected);
        }
    }

    #[tokio::test]
    async fn coordinator_startup_uses_one_loaded_restore_when_archive_advances() {
        let root = tempfile::tempdir().unwrap();
        let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
            ObjStore::new(ObjStoreConfig::Local {
                root: root.path().join("archive"),
            })
            .unwrap(),
            CheckpointIdentity::new(
                "rhiza:sql:cluster-a",
                1,
                1,
                LogHash::digest(&[b"node-test-config"]),
                1,
            ),
        );
        archive.initialize_checkpoint().await.unwrap();

        // This publisher models the coordinator's cache sampled at open.
        let stale = archive
            .open_checkpoint_publisher("stale-coordinator", Default::default())
            .await
            .unwrap();
        assert_eq!(stale.cached_checkpoint().await.manifest().tip().index(), 0);

        // A peer advances the archive before the coordinator loads its exact
        // restore pair. The old equality check rejected this valid sequence.
        let peer = archive
            .open_checkpoint_publisher("peer", Default::default())
            .await
            .unwrap();
        let entry = LogEntry {
            cluster_id: "rhiza:sql:cluster-a".into(),
            epoch: 1,
            config_id: 1,
            index: 1,
            entry_type: EntryType::Noop,
            prev_hash: LogHash::ZERO,
            hash: LogEntry::calculate_hash(
                "rhiza:sql:cluster-a",
                1,
                1,
                1,
                EntryType::Noop,
                LogHash::ZERO,
                &[],
            ),
            payload: Vec::new(),
        };
        peer.publish_committed(std::slice::from_ref(&entry))
            .await
            .unwrap();

        let tip = load_coordinator_restore_baseline(&archive, &stale)
            .await
            .unwrap();
        assert_eq!(tip, CheckpointTip::new(1, entry.hash));
        assert_eq!(stale.cached_checkpoint().await.manifest().tip(), &tip);
    }

    #[tokio::test]
    async fn dropped_sync_durability_confirmation_closes_the_write_gate() {
        let root = tempfile::tempdir().unwrap();
        let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
            ObjStore::new(ObjStoreConfig::Local {
                root: root.path().join("archive"),
            })
            .unwrap(),
            CheckpointIdentity::new(
                "rhiza:sql:cluster-a",
                1,
                1,
                LogHash::digest(&[b"node-test-config"]),
                1,
            ),
        );
        archive.initialize_checkpoint().await.unwrap();
        let coordinator = CheckpointCoordinator::open(archive, DurabilityMode::Sync)
            .await
            .unwrap();
        coordinator.note_committed(1);
        drop(coordinator.sync_durability_confirmation());
        assert_eq!(coordinator.health(), DurabilityHealth::Unavailable);
        assert!(coordinator.write_allowed().is_err());

        let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
            ObjStore::new(ObjStoreConfig::Local {
                root: root.path().join("disarmed-archive"),
            })
            .unwrap(),
            CheckpointIdentity::new(
                "rhiza:sql:cluster-b",
                1,
                1,
                LogHash::digest(&[b"node-test-config"]),
                1,
            ),
        );
        archive.initialize_checkpoint().await.unwrap();
        let coordinator = CheckpointCoordinator::open(archive, DurabilityMode::Sync)
            .await
            .unwrap();
        coordinator.note_committed(1);
        let mut confirmation = coordinator.sync_durability_confirmation();
        confirmation.disarm();
        drop(confirmation);
        assert_eq!(coordinator.health(), DurabilityHealth::Available);
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn sync_flush_renews_an_expired_and_idle_publisher_lease_before_mutation() {
        let root = tempfile::tempdir().unwrap();
        let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
            ObjStore::new(ObjStoreConfig::Local {
                root: root.path().join("archive"),
            })
            .unwrap(),
            CheckpointIdentity::new(
                "rhiza:sql:cluster-a",
                1,
                1,
                LogHash::digest(&[b"node-test-config"]),
                1,
            ),
        );
        archive.initialize_checkpoint().await.unwrap();
        let coordinator = CheckpointCoordinator::open_with_holder_and_options(
            archive,
            DurabilityMode::Sync,
            "sync-lease",
            CheckpointPublisherOptions::new(300),
        )
        .await
        .unwrap();
        let config = NodeConfig::new_embedded(
            "cluster-a",
            "node-1",
            root.path().join("node"),
            1,
            1,
            ["node-1", "node-2", "node-3"],
        )
        .unwrap();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                "rhiza:sql:cluster-a",
                "node-1",
                1,
                1,
                [
                    root.path().join("recorders/node-1"),
                    root.path().join("recorders/node-2"),
                    root.path().join("recorders/node-3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        );
        let runtime = NodeRuntime::open(config, consensus, &[]).unwrap();

        tokio::time::sleep(Duration::from_millis(600)).await;
        let committed = runtime.write("request-1", "alpha", "one").unwrap();
        coordinator.note_committed(committed.applied_index);
        coordinator
            .flush_runtime(&runtime, committed.applied_index)
            .await
            .unwrap();
        assert!(coordinator.write_allowed().is_ok());
        assert_eq!(coordinator.health(), DurabilityHealth::Available);
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn sync_background_recovers_an_unavailable_coordinator_after_transient_failure() {
        let root = tempfile::tempdir().unwrap();
        let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
            ObjStore::new(ObjStoreConfig::Local {
                root: root.path().join("archive"),
            })
            .unwrap(),
            CheckpointIdentity::new(
                "rhiza:sql:cluster-a",
                1,
                1,
                LogHash::digest(&[b"node-test-config"]),
                1,
            ),
        );
        archive.initialize_checkpoint().await.unwrap();
        let coordinator = Arc::new(
            CheckpointCoordinator::open_with_holder_and_options(
                archive.clone(),
                DurabilityMode::Sync,
                "sync-recovery",
                CheckpointPublisherOptions::new(300),
            )
            .await
            .unwrap(),
        );
        let config = NodeConfig::new_embedded(
            "cluster-a",
            "node-1",
            root.path().join("node"),
            1,
            1,
            ["node-1", "node-2", "node-3"],
        )
        .unwrap();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                "rhiza:sql:cluster-a",
                "node-1",
                1,
                1,
                [
                    root.path().join("recorders/node-1"),
                    root.path().join("recorders/node-2"),
                    root.path().join("recorders/node-3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        );
        let runtime = Arc::new(NodeRuntime::open(config, consensus, &[]).unwrap());

        let committed = runtime.write("request-1", "alpha", "one").unwrap();
        coordinator.note_committed(committed.applied_index);
        drop(coordinator.sync_durability_confirmation());
        assert_eq!(coordinator.health(), DurabilityHealth::Unavailable);
        assert!(coordinator.write_allowed().is_err());

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let worker = tokio::spawn(coordinator.clone().run_background(
            runtime.clone(),
            async move {
                if !*shutdown_rx.borrow() {
                    let _ = shutdown_rx.changed().await;
                }
            },
        ));
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if coordinator.durable_tip().index() >= committed.applied_index
                    && coordinator.health() == DurabilityHealth::Available
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(coordinator.write_allowed().is_ok());

        shutdown_tx.send(true).unwrap();
        worker.await.unwrap().unwrap();
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn periodic_background_retries_transient_unavailability_without_new_writes() {
        let root = tempfile::tempdir().unwrap();
        let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
            ObjStore::new(ObjStoreConfig::Local {
                root: root.path().join("archive"),
            })
            .unwrap(),
            CheckpointIdentity::new(
                "rhiza:sql:cluster-a",
                1,
                1,
                LogHash::digest(&[b"node-test-config"]),
                1,
            ),
        );
        archive.initialize_checkpoint().await.unwrap();
        let coordinator = Arc::new(
            CheckpointCoordinator::open_with_holder_and_options(
                archive,
                DurabilityMode::Periodic {
                    interval: Duration::from_millis(10),
                },
                "periodic-recovery",
                CheckpointPublisherOptions::default().with_compaction_segment_limit(1),
            )
            .await
            .unwrap(),
        );
        let config = NodeConfig::new_embedded(
            "cluster-a",
            "node-1",
            root.path().join("node"),
            1,
            1,
            ["node-1", "node-2", "node-3"],
        )
        .unwrap();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                "rhiza:sql:cluster-a",
                "node-1",
                1,
                1,
                [
                    root.path().join("recorders/node-1"),
                    root.path().join("recorders/node-2"),
                    root.path().join("recorders/node-3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        );
        let runtime = Arc::new(NodeRuntime::open(config, consensus, &[]).unwrap());

        let baseline = runtime.write("request-0", "alpha", "zero").unwrap();
        coordinator.note_committed(baseline.applied_index);
        coordinator
            .flush_runtime(&runtime, baseline.applied_index)
            .await
            .unwrap();
        assert!(coordinator.publisher.compaction_recommended().await);

        let committed = runtime.write("request-1", "alpha", "one").unwrap();
        coordinator.note_committed(committed.applied_index);
        // Two consecutive failures cover both the initial periodic flush and
        // the first recovery attempt. Compaction must not bypass that backoff
        // with its own nested flush while durability is unavailable.
        coordinator.inject_flush_unavailable(2);

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let mut worker = tokio::spawn(coordinator.clone().run_background(runtime, async move {
            if !*shutdown_rx.borrow() {
                let _ = shutdown_rx.changed().await;
            }
        }));
        let recovered = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if coordinator.durable_tip().index() >= committed.applied_index
                    && coordinator.health() == DurabilityHealth::Available
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        });
        tokio::select! {
            result = &mut worker => panic!("periodic durability worker exited before recovery: {result:?}"),
            result = recovered => result.unwrap(),
        }

        shutdown_tx.send(true).unwrap();
        worker.await.unwrap().unwrap();
    }
}
