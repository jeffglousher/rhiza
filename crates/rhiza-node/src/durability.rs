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
        atomic::{AtomicU64, Ordering},
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
    ConfigurationState, ExecutionProfile, LogAnchor, LogEntry, LogHash, LogIndex, RecoveryAnchor,
};
#[cfg(feature = "graph")]
use rhiza_graph::{
    decode_snapshot as decode_graph_snapshot, encode_snapshot as encode_graph_snapshot,
    restore_snapshot_file as restore_graph_snapshot_file,
};
#[cfg(feature = "kv")]
use rhiza_kv::{
    decode_snapshot as decode_kv_snapshot, encode_snapshot as encode_kv_snapshot,
    restore_snapshot_file as restore_kv_snapshot_file, RedbStateMachine,
};
use rhiza_log::{FileLogStore, IndexRange, LogStore};
#[cfg(feature = "sql")]
use rhiza_sql::{restore_recovery_snapshot_file, sql_executor_fingerprint};
use serde::{Deserialize, Serialize};

use crate::{Materializer, NodeConfig, NodeRuntime};

const FLUSH_BATCH_ENTRIES: LogIndex = 32;
const RESTORE_INTENT_FILE: &str = ".rhiza-restore-v2.json";
const RESTORE_INTENT_PREFIX: &str = ".rhiza-restore-";
const RESTORE_STAGING_PREFIX: &str = ".restore-stage-";
const RESTORE_MARKER_TMP_PREFIX: &str = ".restore-marker-tmp-";
const SUCCESSOR_RESTORE_LOCK_FILE: &str = ".successor-restore.lock";
const SUCCESSOR_RESTORE_INTENT_FILE: &str = ".successor-restore.intent";
const SUCCESSOR_RESTORE_COMPLETE_FILE: &str = ".successor-restore.complete";
const REPAIR_ARTIFACT_OWNER_FILE: &str = ".rhiza-recovery-owner.json";
pub const LOCAL_CHECKPOINT_IDENTITY_FILE: &str = ".rhiza-checkpoint-identity-v2.json";
static RESTORE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RestoreIntentIdentity {
    version: u32,
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
    Successor(SuccessorRestoreReceipt),
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
    format_version: u32,
    role: RepairArtifactRole,
    name: String,
    identity: RecoveryArtifactIdentity,
}

#[derive(Serialize)]
struct SuccessorRestoreIdentity<'a> {
    version: u32,
    cluster_id: &'a str,
    epoch: u64,
    target_config_id: u64,
    recovery_generation: u64,
    node_id: &'a str,
    membership_digest: String,
    predecessor_config_id: u64,
    stop_index: LogIndex,
    stop_hash: String,
    checkpoint_index: LogIndex,
    checkpoint_hash: String,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SuccessorRestoreReceipt {
    version: u32,
    cluster_id: String,
    epoch: u64,
    target_config_id: u64,
    recovery_generation: u64,
    node_id: String,
    membership_digest: String,
    predecessor_config_id: u64,
    stop_index: LogIndex,
    stop_hash: String,
    checkpoint_index: LogIndex,
    checkpoint_hash: String,
}

pub struct SuccessorRestorePreparation {
    tip: CheckpointTip,
    data_dir: PathBuf,
    identity: Vec<u8>,
    requires_recorder_install: bool,
    _lock: fs::File,
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
        let intent = self.data_dir.join(SUCCESSOR_RESTORE_INTENT_FILE);
        let actual = read_regular_successor_control_file(&intent)?.ok_or_else(|| {
            DurabilityError::SnapshotVerification("successor restore intent is missing".into())
        })?;
        if parse_successor_restore_receipt(&actual).is_none()
            || parse_successor_restore_receipt(&self.identity).is_none()
            || actual != self.identity
        {
            return Err(DurabilityError::SnapshotVerification(
                "successor restore intent changed before completion".into(),
            ));
        }
        let complete = self.data_dir.join(SUCCESSOR_RESTORE_COMPLETE_FILE);
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
        sync_directory(&self.data_dir)?;
        self.requires_recorder_install = false;
        Ok(self.tip)
    }
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
    IdentityBoundV2,
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
                write!(f, "local qlog gap: expected index {expected}, got {actual:?}")
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

pub struct CheckpointCoordinator {
    store: ObjectArchiveStore,
    publisher: CheckpointPublisher,
    mode: DurabilityMode,
    state: Mutex<CoordinatorState>,
    publication_attempts: AtomicU64,
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
        let loaded = publisher.cached_checkpoint().await;
        let durable_tip = *loaded.manifest().tip();
        let restored = store.restore_checkpoint_v2().await?;
        let restored_tip = *restored.tip();
        if restored_tip != durable_tip {
            return Err(DurabilityError::Archive(
                rhiza_archive::Error::InvalidCheckpoint(
                    "restored entries changed while verifying the loaded manifest".into(),
                ),
            ));
        }
        Ok(Self {
            store,
            publisher,
            mode,
            state: Mutex::new(CoordinatorState {
                durable_tip,
                committed_index: durable_tip.index(),
                pending_lag: None,
                health: DurabilityHealth::Available,
            }),
            publication_attempts: AtomicU64::new(0),
        })
    }

    pub const fn mode(&self) -> &DurabilityMode {
        &self.mode
    }

    pub fn durable_tip(&self) -> CheckpointTip {
        self.lock_state().durable_tip
    }

    pub async fn refresh_durable_tip(&self) -> Result<CheckpointTip, DurabilityError> {
        let loaded = self.publisher.observe_checkpoint().await?;
        let accepted = self.publisher.cache_observed_checkpoint(loaded).await?;
        observe_durable_tip(&self.state, *accepted.manifest().tip())
    }

    pub fn health(&self) -> DurabilityHealth {
        self.lock_state().health
    }

    #[doc(hidden)]
    pub fn checkpoint_publication_attempts(&self) -> u64 {
        self.publication_attempts.load(Ordering::Relaxed)
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

    pub fn note_recovered_committed(&self, index: LogIndex) {
        let mut state = self.lock_state();
        state.committed_index = state.committed_index.max(index);
        if state.committed_index > state.durable_tip.index() {
            state.pending_lag = Some(PendingLag::Recovered);
        }
    }

    pub fn write_allowed(&self) -> Result<(), DurabilityError> {
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
            self.publication_attempts.fetch_add(1, Ordering::Relaxed);
            let published = self.publisher.publish_committed(&entries).await?;
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
        let restored = self.store.restore_checkpoint_v2().await?;
        let published = restored.snapshot().ok_or_else(|| {
            DurabilityError::SnapshotVerification("published V2 root has no snapshot".into())
        })?;
        if published.anchor() != &anchor || published.bytes() != snapshot.archive_bytes {
            return Err(DurabilityError::SnapshotVerification(
                "read-back anchor or snapshot bytes differ".into(),
            ));
        }
        runtime.log_store.compact_prefix(&anchor)?;
        runtime
            .compact_embedded_log_before(anchor.compacted().index())
            .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
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
        let cadence = match self.mode {
            DurabilityMode::Sync => return Ok(()),
            DurabilityMode::Bounded { max_lag } => {
                let half = max_lag / 2;
                half.min(Duration::from_secs(1))
            }
            DurabilityMode::Periodic { interval } => interval,
        };
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => return Ok(()),
                () = tokio::time::sleep(cadence) => {
                    match self.flush_runtime(&runtime, LogIndex::MAX).await {
                        Ok(_) | Err(DurabilityError::Archive(_) | DurabilityError::Io(_)) => {}
                        Err(error) => return Err(error),
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

pub async fn restore_checkpoint_to_fresh_data_dir(
    store: ObjectArchiveStore,
    data_dir: impl AsRef<Path>,
) -> Result<CheckpointTip, DurabilityError> {
    restore_checkpoint_to_fresh_data_dir_with_target(store, data_dir.as_ref(), None, None).await
}

pub async fn restore_checkpoint_to_fresh_data_dir_for_node(
    store: ObjectArchiveStore,
    data_dir: impl AsRef<Path>,
    target_node_id: &str,
) -> Result<CheckpointTip, DurabilityError> {
    if target_node_id.is_empty() {
        return Err(DurabilityError::SnapshotVerification(
            "target node_id is empty".into(),
        ));
    }
    restore_checkpoint_to_fresh_data_dir_with_target(
        store,
        data_dir.as_ref(),
        Some(target_node_id),
        None,
    )
    .await
}

pub async fn restore_checkpoint_to_fresh_data_dir_for_node_with_marker(
    store: ObjectArchiveStore,
    data_dir: impl AsRef<Path>,
    target_node_id: &str,
    marker_name: &str,
    marker_contents: &[u8],
) -> Result<CheckpointTip, DurabilityError> {
    if target_node_id.is_empty() {
        return Err(DurabilityError::SnapshotVerification(
            "target node_id is empty".into(),
        ));
    }
    validate_restore_marker_name(marker_name)?;
    restore_checkpoint_to_fresh_data_dir_with_target(
        store,
        data_dir.as_ref(),
        Some(target_node_id),
        Some((marker_name, marker_contents)),
    )
    .await
}

fn restore_intent_identity(
    identity: &CheckpointIdentity,
    node_id: &str,
    execution_profile: ExecutionProfile,
    checkpoint_root: LogAnchor,
) -> RestoreIntentIdentity {
    RestoreIntentIdentity {
        version: 2,
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

fn encode_restore_intent(
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
    (intent.version == 2
        && !intent.cluster_id.is_empty()
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
    reject_unsupported_restore_intent_artifacts(data_dir)?;
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
    if actual.version != expected.version
        || actual.cluster_id != expected.cluster_id
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
    Ok(CheckpointRestoreState::IdentityBoundV2)
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
        ExecutionProfile::Graph => false,
    })
}

pub async fn restore_checkpoint_for_rejoin_preserving_recorder(
    store: ObjectArchiveStore,
    data_dir: impl AsRef<Path>,
    target_node_id: &str,
    execution_profile: ExecutionProfile,
    marker_name: &str,
    marker_contents: &[u8],
) -> Result<CheckpointTip, DurabilityError> {
    if target_node_id.is_empty() {
        return Err(DurabilityError::SnapshotVerification(
            "target node_id is empty".into(),
        ));
    }
    validate_restore_marker_name(marker_name)?;
    let data_dir = data_dir.as_ref();
    let identity = store.checkpoint_identity()?.clone();
    if snapshot_profile(identity.cluster_id())? != execution_profile
        || execution_profile == ExecutionProfile::Graph
    {
        return Err(DurabilityError::SnapshotVerification(
            "rejoin recovery only replaces matching SQL or KV recovery views".into(),
        ));
    }
    let restored = store.restore_checkpoint_v2().await?;
    let checkpoint_root = LogAnchor::new(restored.tip().index(), restored.tip().hash());
    let intent = encode_restore_intent(
        &identity,
        target_node_id,
        execution_profile,
        checkpoint_root,
    )?;
    let recovery_identity = RecoveryArtifactIdentity::Restore(restore_intent_identity(
        &identity,
        target_node_id,
        execution_profile,
        checkpoint_root,
    ));
    checkpoint_restore_in_progress(
        data_dir,
        &identity,
        target_node_id,
        execution_profile,
        checkpoint_root,
    )?;
    cleanup_owned_recovery_artifacts(data_dir, &recovery_identity)?;
    publish_restore_marker(data_dir, RESTORE_INTENT_FILE, &intent)?;
    install_restored_checkpoint(
        &identity,
        &restored,
        data_dir,
        RestoreInstallOptions {
            target_node_id: Some(target_node_id),
            remove_generic_intent: true,
            replace_rebuildable: true,
            recovery_identity: Some(&recovery_identity),
            completion_marker: Some((marker_name, marker_contents)),
        },
    )
}

async fn restore_checkpoint_to_fresh_data_dir_with_target(
    store: ObjectArchiveStore,
    data_dir: &Path,
    target_node_id: Option<&str>,
    completion_marker: Option<(&str, &[u8])>,
) -> Result<CheckpointTip, DurabilityError> {
    let identity = store.checkpoint_identity()?.clone();
    store
        .load_checkpoint()
        .await?
        .ok_or(DurabilityError::MissingCheckpoint)?;
    let restored = store.restore_checkpoint_v2().await?;
    let target_node_id = target_node_id.unwrap_or("<unbound-restore>");
    let profile = snapshot_profile(identity.cluster_id())?;
    let checkpoint_root = LogAnchor::new(restored.tip().index(), restored.tip().hash());
    let intent = encode_restore_intent(&identity, target_node_id, profile, checkpoint_root)?;
    let recovery_identity = RecoveryArtifactIdentity::Restore(restore_intent_identity(
        &identity,
        target_node_id,
        profile,
        checkpoint_root,
    ));
    prepare_fresh_restore_data_dir(data_dir, completion_marker.map(|(name, _)| name), &intent)?;
    publish_restore_marker(data_dir, RESTORE_INTENT_FILE, &intent)?;
    install_restored_checkpoint(
        &identity,
        &restored,
        data_dir,
        RestoreInstallOptions {
            target_node_id: Some(target_node_id),
            remove_generic_intent: true,
            replace_rebuildable: false,
            recovery_identity: Some(&recovery_identity),
            completion_marker,
        },
    )
}

struct RestoreInstallOptions<'a> {
    target_node_id: Option<&'a str>,
    remove_generic_intent: bool,
    replace_rebuildable: bool,
    recovery_identity: Option<&'a RecoveryArtifactIdentity>,
    completion_marker: Option<(&'a str, &'a [u8])>,
}

fn install_restored_checkpoint(
    identity: &CheckpointIdentity,
    restored: &RestoredCheckpoint,
    data_dir: &Path,
    options: RestoreInstallOptions<'_>,
) -> Result<CheckpointTip, DurabilityError> {
    let tip = *restored.tip();
    let profile = snapshot_profile(identity.cluster_id())?;
    validate_restored_suffix(profile, restored.suffix())?;
    let staging = create_restore_staging_dir(data_dir, options.recovery_identity)?;
    let result = (|| -> Result<(), DurabilityError> {
        if let Some(snapshot) = restored.snapshot() {
            install_profile_snapshot(identity, snapshot, &staging, options.target_node_id)?;
        }

        if restored.snapshot().is_some() || !restored.suffix().is_empty() {
            let initial_configuration = restored
                .snapshot()
                .map(|snapshot| snapshot.anchor().configuration_state().clone())
                .unwrap_or_else(|| ConfigurationState::active(identity.config_id(), LogHash::ZERO));
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
        if options.replace_rebuildable {
            quarantine_rebuildable_view(data_dir, profile, options.recovery_identity)?;
        }
        publish_restore_staging(
            &staging,
            data_dir,
            options.remove_generic_intent,
            options.completion_marker,
        )
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result?;
    Ok(tip)
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
    let path = data_dir.join("consensus/log");
    if !path_has_state(&path)? {
        if checkpoint_root == LogAnchor::new(0, LogHash::ZERO) {
            return Ok(checkpoint_root);
        }
        return Err(DurabilityError::SnapshotVerification(
            "local qlog is missing or empty".into(),
        ));
    }
    let log = FileLogStore::open(
        path,
        identity.cluster_id(),
        identity.epoch(),
        identity.config_id(),
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

pub async fn restore_successor_checkpoint_to_fresh_data_dir(
    store: ObjectArchiveStore,
    config: &NodeConfig,
) -> Result<SuccessorRestorePreparation, DurabilityError> {
    let identity = store.checkpoint_identity()?;
    let loaded = store
        .load_checkpoint()
        .await?
        .ok_or(DurabilityError::MissingCheckpoint)?;
    let transition = loaded.manifest().successor_transition().ok_or_else(|| {
        DurabilityError::SnapshotVerification(
            "successor startup requires transition provenance".into(),
        )
    })?;
    let expected_initial = ConfigurationState::active(
        transition.predecessor().config_id(),
        transition.successor().predecessor_config_digest(),
    );
    let expected_stopped = expected_initial
        .clone()
        .validate_entry(transition.stop_entry())
        .map_err(|_| {
            DurabilityError::SnapshotVerification(
                "successor transition Stop entry is not a valid bound configuration change".into(),
            )
        })?;
    if identity.cluster_id() != config.cluster_id()
        || identity.epoch() != config.epoch()
        || identity.config_id() != transition.successor().config_id()
        || identity.recovery_generation() != config.recovery_generation()
        || transition.successor().cluster_id() != config.cluster_id()
        || transition.successor().members() != config.membership().members()
        || transition.successor().digest() != config.membership().digest()
        || config.configuration_state() != &expected_stopped
        || config.log_initial_configuration() != &expected_initial
    {
        return Err(DurabilityError::SnapshotVerification(
            "successor checkpoint does not match target node configuration".into(),
        ));
    }
    let receipt = serde_json::to_vec(&SuccessorRestoreIdentity {
        version: 1,
        cluster_id: config.cluster_id(),
        epoch: config.epoch(),
        target_config_id: identity.config_id(),
        recovery_generation: config.recovery_generation(),
        node_id: config.node_id(),
        membership_digest: config.membership().digest().to_hex(),
        predecessor_config_id: transition.predecessor().config_id(),
        stop_index: transition.stop_entry().index,
        stop_hash: transition.stop_entry().hash.to_hex(),
        checkpoint_index: loaded.manifest().tip().index(),
        checkpoint_hash: loaded.manifest().tip().hash().to_hex(),
    })
    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
    let intent_identity =
        RecoveryArtifactIdentity::Successor(parse_successor_restore_receipt(&receipt).ok_or_else(
            || DurabilityError::SnapshotVerification("successor restore receipt is invalid".into()),
        )?);
    let (lock, state, complete_marker) =
        prepare_successor_restore_root(config.data_dir(), &receipt)?;
    if state == SuccessorRestoreRootState::Complete {
        let checkpoint_root = LogAnchor::new(
            loaded.manifest().tip().index(),
            loaded.manifest().tip().hash(),
        );
        if let Err(error) = validate_local_recovery_view(
            config.data_dir(),
            identity,
            config.node_id(),
            config.execution_profile(),
            checkpoint_root,
        ) {
            if config.execution_profile() == ExecutionProfile::Graph {
                return Err(error);
            }
            let restored = store.restore_checkpoint_v2().await?;
            if restored.tip() != loaded.manifest().tip() {
                return Err(DurabilityError::SnapshotVerification(
                    "successor checkpoint changed during repair".into(),
                ));
            }
            install_restored_checkpoint(
                identity,
                &restored,
                config.data_dir(),
                RestoreInstallOptions {
                    target_node_id: Some(config.node_id()),
                    remove_generic_intent: false,
                    replace_rebuildable: true,
                    recovery_identity: Some(&RecoveryArtifactIdentity::Successor(
                        complete_marker
                            .as_ref()
                            .expect("Complete state has a validated identity")
                            .clone(),
                    )),
                    completion_marker: None,
                },
            )?;
        }
        return Ok(SuccessorRestorePreparation {
            tip: *loaded.manifest().tip(),
            data_dir: config.data_dir().clone(),
            identity: receipt,
            requires_recorder_install: false,
            _lock: lock,
        });
    }

    let restored = store.restore_checkpoint_v2().await?;
    if restored.tip() != loaded.manifest().tip() {
        return Err(DurabilityError::SnapshotVerification(
            "successor checkpoint changed during restore".into(),
        ));
    }
    if state == SuccessorRestoreRootState::Fresh {
        publish_restore_marker(config.data_dir(), SUCCESSOR_RESTORE_INTENT_FILE, &receipt)?;
    }
    install_restored_checkpoint(
        identity,
        &restored,
        config.data_dir(),
        RestoreInstallOptions {
            target_node_id: Some(config.node_id()),
            remove_generic_intent: false,
            replace_rebuildable: false,
            recovery_identity: Some(&intent_identity),
            completion_marker: None,
        },
    )?;
    Ok(SuccessorRestorePreparation {
        tip: *restored.tip(),
        data_dir: config.data_dir().clone(),
        identity: receipt,
        requires_recorder_install: true,
        _lock: lock,
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
        format_version: 1,
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
    remove_generic_intent: bool,
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
    if remove_generic_intent {
        fs::remove_file(data_dir.join(RESTORE_INTENT_FILE))?;
    }
    sync_directory(data_dir)
}

fn quarantine_rebuildable_view(
    data_dir: &Path,
    profile: ExecutionProfile,
    recovery_identity: Option<&RecoveryArtifactIdentity>,
) -> Result<Option<PathBuf>, DurabilityError> {
    let materializer = match profile {
        ExecutionProfile::Sqlite => "sqlite",
        ExecutionProfile::Kv => "kv",
        ExecutionProfile::Graph => {
            return Err(DurabilityError::SnapshotVerification(
                "graph recovery view replacement is outside this recovery path".into(),
            ))
        }
    };
    let names = [materializer, "consensus"];
    let mut has_rebuildable_view = false;
    for name in names {
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
    if !has_rebuildable_view {
        return Ok(None);
    }
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

fn validate_restore_marker_name(marker_name: &str) -> Result<(), DurabilityError> {
    if marker_name.is_empty()
        || matches!(marker_name, "." | "..")
        || marker_name.contains(std::path::MAIN_SEPARATOR)
    {
        return Err(DurabilityError::SnapshotVerification(
            "restore marker name must be one local file name".into(),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SuccessorRestoreRootState {
    Fresh,
    Intent,
    Complete,
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
    if ownership.format_version != 1
        || ownership.role != expected_role
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

fn cleanup_owned_recovery_artifacts(
    data_dir: &Path,
    identity: &RecoveryArtifactIdentity,
) -> Result<(), DurabilityError> {
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
    (receipt.version == 1
        && !receipt.cluster_id.is_empty()
        && !receipt.node_id.is_empty()
        && LogHash::from_hex(&receipt.membership_digest).is_some()
        && LogHash::from_hex(&receipt.stop_hash).is_some()
        && LogHash::from_hex(&receipt.checkpoint_hash).is_some())
    .then_some(receipt)
}

fn prepare_successor_restore_root(
    data_dir: &Path,
    expected_identity: &[u8],
) -> Result<
    (
        fs::File,
        SuccessorRestoreRootState,
        Option<SuccessorRestoreReceipt>,
    ),
    DurabilityError,
> {
    fs::create_dir_all(data_dir)?;
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(data_dir.join(SUCCESSOR_RESTORE_LOCK_FILE))?;
    lock.lock()?;

    let intent = data_dir.join(SUCCESSOR_RESTORE_INTENT_FILE);
    let complete = data_dir.join(SUCCESSOR_RESTORE_COMPLETE_FILE);
    let intent_contents = read_regular_successor_control_file(&intent)
        .map_err(|_| DurabilityError::DataDirNotFresh(data_dir.to_path_buf()))?;
    let complete_contents = read_regular_successor_control_file(&complete)
        .map_err(|_| DurabilityError::DataDirNotFresh(data_dir.to_path_buf()))?;
    let (state, complete_marker) = match (intent_contents, complete_contents) {
        (Some(actual), None) => {
            if actual != expected_identity || parse_successor_restore_receipt(&actual).is_none() {
                return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
            }
            (SuccessorRestoreRootState::Intent, None)
        }
        (None, Some(actual)) => {
            if !completed_successor_identity_matches(&actual, expected_identity) {
                return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
            }
            let receipt = parse_successor_restore_receipt(&actual)
                .ok_or_else(|| DurabilityError::DataDirNotFresh(data_dir.to_path_buf()))?;
            (SuccessorRestoreRootState::Complete, Some(receipt))
        }
        (None, None) => (SuccessorRestoreRootState::Fresh, None),
        _ => return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf())),
    };

    if state == SuccessorRestoreRootState::Complete {
        let recovery_identity = RecoveryArtifactIdentity::Successor(
            complete_marker
                .as_ref()
                .expect("Complete state has a validated identity")
                .clone(),
        );
        cleanup_owned_recovery_artifacts(data_dir, &recovery_identity)?;
    } else if state == SuccessorRestoreRootState::Intent {
        let recovery_identity = RecoveryArtifactIdentity::Successor(
            parse_successor_restore_receipt(expected_identity)
                .ok_or_else(|| DurabilityError::DataDirNotFresh(data_dir.to_path_buf()))?,
        );
        cleanup_owned_recovery_artifacts(data_dir, &recovery_identity)?;
    }

    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let marker_tmp = is_safe_restore_marker_tmp(&entry.path(), &name)?;
        if name.starts_with(RESTORE_MARKER_TMP_PREFIX) && !marker_tmp {
            return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
        }
        let common = name == SUCCESSOR_RESTORE_LOCK_FILE || marker_tmp;
        let allowed = match state {
            SuccessorRestoreRootState::Fresh => common,
            SuccessorRestoreRootState::Intent => {
                common
                    || name == SUCCESSOR_RESTORE_INTENT_FILE
                    || name == LOCAL_CHECKPOINT_IDENTITY_FILE
                    || name == "sqlite"
                    || name == "ladybug"
                    || name == "kv"
                    || name == "consensus"
                    || name == "recorder"
            }
            SuccessorRestoreRootState::Complete => {
                common
                    || name == SUCCESSOR_RESTORE_COMPLETE_FILE
                    || name == LOCAL_CHECKPOINT_IDENTITY_FILE
                    || name == ".node.lock"
                    || name == "sqlite"
                    || name == "ladybug"
                    || name == "kv"
                    || name == "consensus"
                    || name == "recorder"
            }
        };
        if !allowed {
            return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
        }
    }

    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if is_safe_restore_marker_tmp(&entry.path(), &name)? {
            fs::remove_file(entry.path())?;
        }
    }
    if state == SuccessorRestoreRootState::Intent {
        for name in ["sqlite", "ladybug", "kv", "consensus", "recorder"] {
            let path = data_dir.join(name);
            if path.exists() {
                fs::remove_dir_all(path)?;
            }
        }
        for entry in fs::read_dir(data_dir)? {
            let entry = entry?;
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(RESTORE_STAGING_PREFIX)
            {
                fs::remove_dir_all(entry.path())?;
            }
        }
        sync_directory(data_dir)?;
    }
    Ok((lock, state, complete_marker))
}

fn completed_successor_identity_matches(actual: &[u8], expected: &[u8]) -> bool {
    let (Ok(mut actual), Ok(mut expected)) = (
        serde_json::from_slice::<serde_json::Value>(actual),
        serde_json::from_slice::<serde_json::Value>(expected),
    ) else {
        return false;
    };
    let Some(actual_index) = actual["checkpoint_index"].as_u64() else {
        return false;
    };
    let Some(expected_index) = expected["checkpoint_index"].as_u64() else {
        return false;
    };
    let (Some(actual_hash), Some(expected_hash)) = (
        actual["checkpoint_hash"].as_str(),
        expected["checkpoint_hash"].as_str(),
    ) else {
        return false;
    };
    if LogHash::from_hex(actual_hash).is_none() || LogHash::from_hex(expected_hash).is_none() {
        return false;
    }
    if actual_index > expected_index
        || (actual_index == expected_index && actual_hash != expected_hash)
    {
        return false;
    }
    for receipt in [&mut actual, &mut expected] {
        let Some(receipt) = receipt.as_object_mut() else {
            return false;
        };
        receipt.remove("checkpoint_index");
        receipt.remove("checkpoint_hash");
    }
    actual == expected
}

fn sync_directory(path: &Path) -> Result<(), DurabilityError> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
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
    if !path_has_state(data_dir)? {
        return Ok(());
    }

    reject_unsupported_restore_intent_artifacts(data_dir)?;
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
        }) {
            for entry in entries {
                fs::remove_file(entry.path())?;
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

fn reject_unsupported_restore_intent_artifacts(data_dir: &Path) -> Result<(), DurabilityError> {
    let entries = match fs::read_dir(data_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(RESTORE_INTENT_PREFIX) && name != RESTORE_INTENT_FILE {
            return Err(DurabilityError::SnapshotVerification(
                "unsupported local checkpoint restore artifact".into(),
            ));
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
mod tests {
    use super::{
        completed_successor_identity_matches, mark_durable_state, observe_durable_tip,
        parse_successor_restore_receipt, prepare_successor_restore_root, snapshot_profile,
        validate_local_qlog, validate_restored_suffix, write_repair_artifact_ownership,
        CheckpointTip, CoordinatorState, DurabilityError, DurabilityHealth, ExecutionProfile,
        LogAnchor, LogHash, PendingLag, RecoveryArtifactIdentity, RepairArtifactRole,
        SuccessorRestorePreparation, SuccessorRestoreRootState, RESTORE_INTENT_FILE,
        SUCCESSOR_RESTORE_COMPLETE_FILE, SUCCESSOR_RESTORE_INTENT_FILE,
        SUCCESSOR_RESTORE_LOCK_FILE,
    };
    #[cfg(feature = "kv")]
    use crate::{KvCommandV1, NodeConfig, NodeRuntime};
    use rhiza_archive::CheckpointIdentity;
    #[cfg(feature = "kv")]
    use rhiza_archive::ObjectArchiveStore;
    use rhiza_core::{EntryType, LogEntry};
    use rhiza_log::{FileLogStore, LogStore};
    #[cfg(feature = "kv")]
    use rhiza_obj_store::{ObjStore, ObjStoreConfig};
    #[cfg(feature = "kv")]
    use rhiza_quepaxa::ThreeNodeConsensus;
    use std::sync::{Arc, Barrier, Mutex};

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

    #[test]
    fn completed_successor_prepare_discards_owned_interrupted_repair_artifacts() {
        let root = tempfile::tempdir().unwrap();
        let receipt = br#"{"version":1,"cluster_id":"rhiza:sql:cluster-a","epoch":1,"target_config_id":2,"recovery_generation":1,"node_id":"node-1","membership_digest":"0000000000000000000000000000000000000000000000000000000000000000","predecessor_config_id":1,"stop_index":0,"stop_hash":"0000000000000000000000000000000000000000000000000000000000000000","checkpoint_index":0,"checkpoint_hash":"0000000000000000000000000000000000000000000000000000000000000000"}"#;
        std::fs::write(root.path().join(SUCCESSOR_RESTORE_COMPLETE_FILE), receipt).unwrap();

        let staging = root.path().join(".restore-stage-4242-0");
        std::fs::create_dir_all(staging.join("sqlite")).unwrap();
        let quarantine = root.path().join(".rebuildable-quarantine-4242-1");
        std::fs::create_dir_all(quarantine.join("sqlite")).unwrap();
        std::fs::create_dir_all(quarantine.join("consensus")).unwrap();
        let complete_marker = parse_successor_restore_receipt(receipt).unwrap();
        let complete_identity = RecoveryArtifactIdentity::Successor(complete_marker);
        write_repair_artifact_ownership(&staging, RepairArtifactRole::Staging, &complete_identity)
            .unwrap();
        write_repair_artifact_ownership(
            &quarantine,
            RepairArtifactRole::Quarantine,
            &complete_identity,
        )
        .unwrap();

        let (lock, state, _) = prepare_successor_restore_root(root.path(), receipt).unwrap();
        assert!(state == SuccessorRestoreRootState::Complete);
        drop(lock);
        assert!(!staging.exists());
        assert!(!quarantine.exists());
    }

    #[test]
    fn completed_successor_prepare_keeps_unowned_repair_artifact_and_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let receipt = br#"{"version":1,"cluster_id":"rhiza:sql:cluster-a","epoch":1,"target_config_id":2,"recovery_generation":1,"node_id":"node-1","membership_digest":"membership","predecessor_config_id":1,"stop_index":0,"stop_hash":"0000000000000000000000000000000000000000000000000000000000000000","checkpoint_index":0,"checkpoint_hash":"0000000000000000000000000000000000000000000000000000000000000000"}"#;
        std::fs::write(root.path().join(SUCCESSOR_RESTORE_COMPLETE_FILE), receipt).unwrap();
        let unowned = root.path().join(".rebuildable-quarantine-not-owned");
        std::fs::create_dir_all(&unowned).unwrap();
        std::fs::write(unowned.join("keep"), b"do not remove").unwrap();

        assert!(matches!(
            prepare_successor_restore_root(root.path(), receipt),
            Err(DurabilityError::DataDirNotFresh(_))
        ));
        assert_eq!(
            std::fs::read(unowned.join("keep")).unwrap(),
            b"do not remove"
        );
    }

    #[test]
    fn completed_successor_prepare_keeps_exact_shaped_lookalike_without_ownership_record() {
        let root = tempfile::tempdir().unwrap();
        let receipt = br#"{"version":1,"cluster_id":"rhiza:sql:cluster-a","epoch":1,"target_config_id":2,"recovery_generation":1,"node_id":"node-1","membership_digest":"membership","predecessor_config_id":1,"stop_index":0,"stop_hash":"0000000000000000000000000000000000000000000000000000000000000000","checkpoint_index":0,"checkpoint_hash":"0000000000000000000000000000000000000000000000000000000000000000"}"#;
        std::fs::write(root.path().join(SUCCESSOR_RESTORE_COMPLETE_FILE), receipt).unwrap();
        let lookalike = root.path().join(".rebuildable-quarantine-4242-1");
        std::fs::create_dir_all(lookalike.join("sqlite")).unwrap();
        std::fs::create_dir_all(lookalike.join("consensus")).unwrap();

        assert!(matches!(
            prepare_successor_restore_root(root.path(), receipt),
            Err(DurabilityError::DataDirNotFresh(_))
        ));
        assert!(lookalike.join("sqlite").is_dir());
        assert!(lookalike.join("consensus").is_dir());
    }

    #[test]
    fn intent_successor_prepare_keeps_ownerless_staging_and_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let receipt = br#"{"version":1,"cluster_id":"rhiza:sql:cluster-a","epoch":1,"target_config_id":2,"recovery_generation":1,"node_id":"node-1","membership_digest":"0000000000000000000000000000000000000000000000000000000000000000","predecessor_config_id":1,"stop_index":0,"stop_hash":"0000000000000000000000000000000000000000000000000000000000000000","checkpoint_index":0,"checkpoint_hash":"0000000000000000000000000000000000000000000000000000000000000000"}"#;
        std::fs::write(root.path().join(SUCCESSOR_RESTORE_INTENT_FILE), receipt).unwrap();
        let staging = root.path().join(".restore-stage-4242-1");
        std::fs::create_dir_all(staging.join("sqlite")).unwrap();

        assert!(matches!(
            prepare_successor_restore_root(root.path(), receipt),
            Err(DurabilityError::DataDirNotFresh(_))
        ));
        assert!(staging.join("sqlite").is_dir());
    }

    #[test]
    fn intent_successor_prepare_discards_exactly_owned_staging_after_interruption() {
        let root = tempfile::tempdir().unwrap();
        let receipt = br#"{"version":1,"cluster_id":"rhiza:sql:cluster-a","epoch":1,"target_config_id":2,"recovery_generation":1,"node_id":"node-1","membership_digest":"0000000000000000000000000000000000000000000000000000000000000000","predecessor_config_id":1,"stop_index":0,"stop_hash":"0000000000000000000000000000000000000000000000000000000000000000","checkpoint_index":0,"checkpoint_hash":"0000000000000000000000000000000000000000000000000000000000000000"}"#;
        std::fs::write(root.path().join(SUCCESSOR_RESTORE_INTENT_FILE), receipt).unwrap();
        let staging = root.path().join(".restore-stage-4242-1");
        std::fs::create_dir_all(staging.join("sqlite")).unwrap();
        write_repair_artifact_ownership(
            &staging,
            RepairArtifactRole::Staging,
            &RecoveryArtifactIdentity::Successor(parse_successor_restore_receipt(receipt).unwrap()),
        )
        .unwrap();

        let (lock, state, _) = prepare_successor_restore_root(root.path(), receipt).unwrap();
        assert!(state == SuccessorRestoreRootState::Intent);
        drop(lock);
        assert!(!staging.exists());
    }

    #[test]
    fn generic_restore_prepare_keeps_prefix_spoofed_staging_and_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let identity = CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 1);
        let intent = super::encode_restore_intent(
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

    #[cfg(unix)]
    #[test]
    fn successor_intent_symlink_fails_without_following_target() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let receipt = br#"{"version":1,"cluster_id":"rhiza:sql:cluster-a","epoch":1,"target_config_id":2,"recovery_generation":1,"node_id":"node-1","membership_digest":"0000000000000000000000000000000000000000000000000000000000000000","predecessor_config_id":1,"stop_index":0,"stop_hash":"0000000000000000000000000000000000000000000000000000000000000000","checkpoint_index":0,"checkpoint_hash":"0000000000000000000000000000000000000000000000000000000000000000"}"#;
        let target = root.path().join("target");
        std::fs::write(&target, receipt).unwrap();
        let intent = root.path().join(SUCCESSOR_RESTORE_INTENT_FILE);
        symlink(&target, &intent).unwrap();

        assert!(matches!(
            prepare_successor_restore_root(root.path(), receipt),
            Err(DurabilityError::DataDirNotFresh(_))
        ));
        assert_eq!(std::fs::read(&target).unwrap(), receipt);
        assert!(std::fs::symlink_metadata(&intent)
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[test]
    fn successor_prepare_keeps_spoofed_restore_marker_tmp_directory_and_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let receipt = br#"{"version":1,"cluster_id":"rhiza:sql:cluster-a","epoch":1,"target_config_id":2,"recovery_generation":1,"node_id":"node-1","membership_digest":"0000000000000000000000000000000000000000000000000000000000000000","predecessor_config_id":1,"stop_index":0,"stop_hash":"0000000000000000000000000000000000000000000000000000000000000000","checkpoint_index":0,"checkpoint_hash":"0000000000000000000000000000000000000000000000000000000000000000"}"#;
        std::fs::write(root.path().join(SUCCESSOR_RESTORE_COMPLETE_FILE), receipt).unwrap();
        let spoof = root.path().join(".restore-marker-tmp-not-generated");
        std::fs::create_dir(&spoof).unwrap();

        assert!(matches!(
            prepare_successor_restore_root(root.path(), receipt),
            Err(DurabilityError::DataDirNotFresh(_))
        ));
        assert!(spoof.is_dir());
    }

    #[test]
    fn bounded_regular_reader_rejects_file_larger_than_its_limit() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("oversized");
        std::fs::write(&path, b"12345").unwrap();

        assert!(super::read_bounded_regular_file(&path, 4).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"12345");
    }

    #[test]
    fn rejoin_artifact_cleanup_removes_only_owned_stale_stage_and_quarantine() {
        let root = tempfile::tempdir().unwrap();
        let checkpoint = LogAnchor::new(0, LogHash::ZERO);
        let identity = RecoveryArtifactIdentity::Restore(super::restore_intent_identity(
            &CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 1),
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
            &CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 1),
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
        let receipt = br#"{"version":1,"cluster_id":"rhiza:sql:cluster-a","epoch":1,"target_config_id":2,"recovery_generation":1,"node_id":"node-1","membership_digest":"0000000000000000000000000000000000000000000000000000000000000000","predecessor_config_id":1,"stop_index":0,"stop_hash":"0000000000000000000000000000000000000000000000000000000000000000","checkpoint_index":0,"checkpoint_hash":"0000000000000000000000000000000000000000000000000000000000000000"}"#;
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
    fn completed_successor_prepare_keeps_artifact_bound_to_a_different_complete_marker() {
        let root = tempfile::tempdir().unwrap();
        let receipt = br#"{"version":1,"cluster_id":"rhiza:sql:cluster-a","epoch":1,"target_config_id":2,"recovery_generation":1,"node_id":"node-1","membership_digest":"0000000000000000000000000000000000000000000000000000000000000000","predecessor_config_id":1,"stop_index":0,"stop_hash":"0000000000000000000000000000000000000000000000000000000000000000","checkpoint_index":0,"checkpoint_hash":"0000000000000000000000000000000000000000000000000000000000000000"}"#;
        let foreign_receipt = br#"{"version":1,"cluster_id":"rhiza:sql:cluster-a","epoch":1,"target_config_id":2,"recovery_generation":1,"node_id":"other-node","membership_digest":"0000000000000000000000000000000000000000000000000000000000000000","predecessor_config_id":1,"stop_index":0,"stop_hash":"0000000000000000000000000000000000000000000000000000000000000000","checkpoint_index":0,"checkpoint_hash":"0000000000000000000000000000000000000000000000000000000000000000"}"#;
        std::fs::write(root.path().join(SUCCESSOR_RESTORE_COMPLETE_FILE), receipt).unwrap();
        let artifact = root.path().join(".rebuildable-quarantine-4242-1");
        std::fs::create_dir_all(artifact.join("sqlite")).unwrap();
        write_repair_artifact_ownership(
            &artifact,
            RepairArtifactRole::Quarantine,
            &RecoveryArtifactIdentity::Successor(
                parse_successor_restore_receipt(foreign_receipt).unwrap(),
            ),
        )
        .unwrap();

        assert!(matches!(
            prepare_successor_restore_root(root.path(), receipt),
            Err(DurabilityError::DataDirNotFresh(_))
        ));
        assert!(artifact.join("sqlite").is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn completed_successor_prepare_keeps_symlinked_repair_lookalike_and_fails_closed() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let receipt = br#"{"version":1,"cluster_id":"rhiza:sql:cluster-a","epoch":1,"target_config_id":2,"recovery_generation":1,"node_id":"node-1","membership_digest":"0000000000000000000000000000000000000000000000000000000000000000","predecessor_config_id":1,"stop_index":0,"stop_hash":"0000000000000000000000000000000000000000000000000000000000000000","checkpoint_index":0,"checkpoint_hash":"0000000000000000000000000000000000000000000000000000000000000000"}"#;
        std::fs::write(root.path().join(SUCCESSOR_RESTORE_COMPLETE_FILE), receipt).unwrap();
        let target = root.path().join("target");
        std::fs::create_dir_all(&target).unwrap();
        let lookalike = root.path().join(".rebuildable-quarantine-4242-1");
        symlink(&target, &lookalike).unwrap();

        assert!(matches!(
            prepare_successor_restore_root(root.path(), receipt),
            Err(DurabilityError::DataDirNotFresh(_))
        ));
        assert!(target.is_dir());
        assert!(std::fs::symlink_metadata(&lookalike)
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[test]
    fn sqlite_restore_suffix_rejects_legacy_commands_during_preflight() {
        let payload = b"put\tlegacy\tkey\tvalue".to_vec();
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
            Err(DurabilityError::SnapshotVerification(message)) if message.contains("QWAL")
        ));
    }

    #[test]
    fn local_qlog_accepts_ahead_tip_when_checkpoint_entry_is_retained() {
        let root = tempfile::tempdir().unwrap();
        let identity = CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 1);
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
        let identity = CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 1);
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
    fn restore_intent_remains_until_completion_marker_is_durable_and_retryable() {
        let root = tempfile::tempdir().unwrap();
        let data_dir = root.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let intent = super::encode_restore_intent(
            &CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 1),
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
            true,
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
            true,
            Some(("identity.json", b"identity-fixture")),
        )
        .unwrap();
        assert_eq!(
            std::fs::read(data_dir.join("identity.json")).unwrap(),
            b"identity-fixture"
        );
        assert!(!data_dir.join(RESTORE_INTENT_FILE).exists());
    }

    #[cfg(feature = "kv")]
    #[tokio::test]
    async fn kv_compacted_rejoin_restores_missing_or_corrupt_views_without_touching_recorder() {
        let root = tempfile::tempdir().unwrap();
        let identity = CheckpointIdentity::new("rhiza:kv:cluster-a", 1, 1, 1);
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
        restore_checkpoint_to_fresh_data_dir_for_node(archive.clone(), &target, "node-1")
            .await
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
        restore_checkpoint_for_rejoin_preserving_recorder(
            archive.clone(),
            &target,
            "node-1",
            ExecutionProfile::Kv,
            "identity.json",
            b"identity-fixture",
        )
        .await
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
        restore_checkpoint_for_rejoin_preserving_recorder(
            archive,
            &target,
            "node-1",
            ExecutionProfile::Kv,
            "identity.json",
            b"identity-fixture",
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(target.join("recorder/sentinel")).unwrap(),
            b"keep-me"
        );
    }

    fn successor_receipt(index: u64, hash_byte: char, generation: u64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "cluster_id": "cluster-a",
            "epoch": 1,
            "target_config_id": 2,
            "recovery_generation": generation,
            "node_id": "node-1",
            "membership_digest": "digest",
            "predecessor_config_id": 1,
            "stop_index": 4,
            "stop_hash": "stop",
            "checkpoint_index": index,
            "checkpoint_hash": hash_byte.to_string().repeat(64),
        }))
        .unwrap()
    }

    #[test]
    fn completed_successor_receipt_allows_only_forward_checkpoint_progress() {
        let expected = successor_receipt(8, '8', 1);

        assert!(completed_successor_identity_matches(
            &successor_receipt(4, '4', 1),
            &expected
        ));
        assert!(!completed_successor_identity_matches(
            &successor_receipt(8, '9', 1),
            &expected
        ));
        assert!(!completed_successor_identity_matches(
            &successor_receipt(9, '9', 1),
            &expected
        ));
        assert!(!completed_successor_identity_matches(
            &successor_receipt(4, '4', 2),
            &expected
        ));
        let mut malformed =
            serde_json::from_slice::<serde_json::Value>(&successor_receipt(4, '4', 1)).unwrap();
        malformed["checkpoint_hash"] = serde_json::json!(7);
        assert!(!completed_successor_identity_matches(
            &serde_json::to_vec(&malformed).unwrap(),
            &expected
        ));
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
}
