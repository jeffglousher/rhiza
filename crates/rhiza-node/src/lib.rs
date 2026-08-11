#![cfg_attr(
    not(any(feature = "sql", feature = "graph", feature = "kv")),
    allow(dead_code, unreachable_code, unused_imports, unused_variables)
)]

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt, fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Condvar, Mutex, MutexGuard, OnceLock,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
#[cfg(windows)]
use std::os::windows::fs::{MetadataExt as WindowsMetadataExt, OpenOptionsExt};

#[cfg(any(feature = "sql", feature = "kv"))]
use std::collections::VecDeque;

#[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
compile_error!("rhiza-node requires at least one execution profile feature: sql, graph, or kv");

#[cfg(any(feature = "sql", feature = "kv", test))]
use std::sync::atomic::AtomicUsize;

use axum::{
    body::{Body, Bytes, HttpBody},
    extract::{rejection::JsonRejection, DefaultBodyLimit, Extension, Request, State},
    http::{header::CONTENT_TYPE, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use http_body::Frame;
#[cfg(feature = "sql")]
use rhiza_archive::SnapshotRecord;
#[cfg(feature = "sql")]
use rhiza_core::ExternalEffectCommand;
use rhiza_core::{
    Command, CommandKind, ConfigChange, ConfigurationState, EntryType, ErrorClassification,
    ExecutionProfile, LogAnchor, LogEntry, LogHash, LogIndex, RecoveryAnchor, StoredCommand,
};
#[cfg(feature = "graph")]
use rhiza_graph::{
    encode_replicated_graph_batch, encode_replicated_graph_command, CanonicalF64, GraphColumn,
    GraphInternalId, GraphLogicalType, GraphNode, GraphParameterValue, GraphQueryResult, GraphRel,
    GraphResultValue, LadybugStateMachine, RequestRecord as GraphRequestRecord,
};
#[cfg(feature = "kv")]
use rhiza_kv::{
    encode_replicated_kv_batch, encode_replicated_kv_command, KvRequestRecord, RedbStateMachine,
    MAX_KV_BATCH_MEMBERS,
};
#[cfg(feature = "kv")]
pub use rhiza_kv::{KvScanResult, KvScanRow, MAX_KV_SCAN_RESULT_BYTES, MAX_KV_SCAN_ROWS};
#[cfg(feature = "sql")]
use rhiza_log::{decode_segment_for_cluster, write_segment_file};
use rhiza_log::{FileLogStore, IndexRange, LogStore};
#[cfg(feature = "sql")]
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{
    CertifiedDecisionInspection, DecisionInspection, DecisionProof, EffectBundleBinding,
    Membership, ReadFenceObservation, ReadFenceRequest, RecordRequest, RecordSummary,
    RecorderFileStore, RecorderRpc, RecorderRpcContext, RejectReason, ThreeNodeConsensus,
    DEFAULT_RECORDER_RPC_TIMEOUT,
};
#[cfg(feature = "sql")]
use rhiza_quepaxa::{EffectBundleFinalizeRequest, RecorderEffectBundle};
#[cfg(feature = "sql")]
use rhiza_sql::{
    encode_put_request, encode_sql_command, restore_snapshot_file, QwalEffectManifestV4,
    RecoverySnapshot, RequestConflict, RequestOutcome, SqlBatchMember, SqlCommand,
    SqlCommandResult, SqlQueryResult, SqlStatement, SqlValue, SqliteStateMachine,
    VerifiedQwalEffectBundleV4, MAX_QWAL_V3_RECEIPTS, MAX_SQL_STATEMENTS,
};
#[cfg(not(feature = "sql"))]
type SqlCommandResult = ();

mod admin;
pub mod durability;
#[cfg(feature = "graph")]
mod graph;
#[cfg(feature = "kv")]
mod kv;
mod learner;
mod recorder_decode;
mod recorder_tcp;
pub use admin::*;
pub use durability::{
    capture_expected_local_restore_state,
    install_prepared_checkpoint_for_rejoin_preserving_recorder,
    install_prepared_checkpoint_to_fresh_data_dir, prepare_checkpoint_restore,
    CheckpointCoordinator, CheckpointInstallMode, DurabilityError, DurabilityHealth,
    DurabilityMode, ExpectedLocalRestoreState, PreparedCheckpointRestore, RestoreCompletionMarker,
    SuccessorRestorePreparation,
};
#[cfg(feature = "graph")]
pub use graph::*;
#[cfg(feature = "kv")]
pub use kv::*;
pub use learner::*;
#[cfg(feature = "recorder-postcard-rpc")]
pub use recorder_tcp::{
    serve_recorder_postcard_rpc, serve_recorder_postcard_rpc_tls,
    RecorderPostcardRpcTlsClientConfig, RecorderPostcardRpcTlsServerConfig,
    TcpPostcardRpcRecorderClient,
};
pub use recorder_tcp::{
    serve_recorder_tcp, serve_recorder_tcp_tls, validate_recorder_tcp_endpoint,
    RecorderIngressExit, RecorderIngressLifecycle, RecorderTaskDisposition,
    RecorderTlsClientConfig, RecorderTlsServerConfig, TcpPostcardRecorderClient,
};

pub const MAX_FETCH_ENTRIES: u32 = 1_024;
pub const MAX_COMMAND_BYTES: usize = 512 * 1024;
pub const MAX_REQUEST_ID_BYTES: usize = 256;
pub const MAX_KEY_BYTES: usize = 4 * 1024;
pub const MAX_VALUE_BYTES: usize = 240 * 1024;
pub const MAX_HTTP_BODY_BYTES: usize = MAX_COMMAND_BYTES * 6 + 16 * 1024;
pub const DEFAULT_CLIENT_CONCURRENCY: usize = 16;
pub const DEFAULT_PEER_CONCURRENCY: usize = 32;
pub const HTTP_RECORDER_MAX_CONCURRENT_WORKERS: usize = DEFAULT_PEER_CONCURRENCY;
pub const DEFAULT_WRITER_BATCH_MAX: usize = 8;
const MAX_WRITE_BATCH_MEMBERS: usize = 64;
const PEER_RESPONSE_CHUNK_BYTES: usize = 8 * 1024;
#[cfg(feature = "sql")]
const MAX_SQL_WRITE_BATCH_MEMBERS: usize = MAX_QWAL_V3_RECEIPTS;
#[cfg(feature = "sql")]
pub const MAX_TYPED_SQL_WRITE_BATCH_MEMBERS: usize = 256;
#[cfg(feature = "sql")]
pub const DEFAULT_SQL_GROUP_COMMIT_QUEUE_CAPACITY: usize = 64;
#[cfg(feature = "sql")]
pub const MAX_SQL_GROUP_COMMIT_QUEUE_CAPACITY: usize = 4_096;
// One full 1,024-receipt group is four aggregate-capped public calls.
#[cfg(feature = "sql")]
const MAX_SQL_GROUP_COMMIT_ACTIVE_BYTES: usize = 4 * MAX_COMMAND_BYTES;
// Keep the configured call limit for compatibility, but never retain more than 64 full calls.
#[cfg(feature = "sql")]
const MAX_SQL_GROUP_COMMIT_PENDING_BYTES: usize =
    DEFAULT_SQL_GROUP_COMMIT_QUEUE_CAPACITY * MAX_COMMAND_BYTES;
#[cfg(feature = "kv")]
const MAX_KV_GROUP_COMMIT_MEMBERS: usize = 1_024;
#[cfg(feature = "kv")]
const KV_GROUP_COMMIT_QUEUE_CAPACITY: usize = 64;
#[cfg(feature = "kv")]
const MAX_KV_GROUP_COMMIT_PENDING_BYTES: usize = KV_GROUP_COMMIT_QUEUE_CAPACITY * MAX_COMMAND_BYTES;
pub const DEFAULT_WRITER_BATCH_WINDOW: Duration = Duration::from_micros(500);
pub const PROTOCOL_VERSION: &str = "1";
pub const RECORDER_PROTOCOL_VERSION: &str = "5";
const RECORDER_WIRE_VERSION: u16 = 5;
pub const VERSION_HEADER: &str = "x-rhiza-version";
pub const NODE_ID_HEADER: &str = "x-rhiza-node-id";
pub const RECOVERY_GENERATION_HEADER: &str = "x-rhiza-recovery-generation";
pub const RECORDER_IDENTITY_PATH: &str = "/v2/quepaxa/recorder/identity";

pub(crate) const NODE_DATA_ROOT_LOCK_FILE: &str = ".node.lock";

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

// Open the final component itself on Windows so a reparse point cannot turn a
// checked `.node.lock` pathname into a different target between metadata
// validation and handle identity validation.
#[cfg(windows)]
const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
#[cfg(windows)]
const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
#[cfg(windows)]
const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;

/// Mirrors Windows `FILE_ID_INFO`: a 64-bit volume serial followed by the
/// full 128-bit file ID returned by `GetFileInformationByHandleEx(FileIdInfo)`.
/// The older BY_HANDLE_FILE_INFORMATION FileIndex is only 64 bits and is not
/// collision-safe on filesystems such as ReFS, so it is deliberately unused.
#[cfg(windows)]
#[repr(C)]
struct WindowsFileIdInfo {
    volume_serial_number: u64,
    file_id: [u8; 16],
}

#[cfg(windows)]
const _: [(); 24] = [(); std::mem::size_of::<WindowsFileIdInfo>()];

#[cfg(windows)]
const WINDOWS_FILE_ID_INFO_CLASS: i32 = 18;

#[cfg(windows)]
#[link(name = "kernel32")]
extern "system" {
    fn GetFileInformationByHandleEx(
        file: *mut std::ffi::c_void,
        information_class: i32,
        information: *mut std::ffi::c_void,
        information_size: u32,
    ) -> i32;
}

/// An identity derived from the exact file handle whose advisory lock is
/// held. Pathname metadata such as length or mtime is not an identity: a
/// replacement zero-length lock must never become interchangeable with the
/// locked inode/file object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NodeDataPathIdentity {
    #[cfg(unix)]
    Unix { dev: u64, ino: u64 },
    #[cfg(windows)]
    Windows {
        volume_serial_number: u64,
        file_id: [u8; 16],
    },
}

/// The only cross-process mutation lock for a node data root. Runtime open
/// and checkpoint restore intentionally share this helper so neither can
/// observe or modify qlog/materializer state while the other owns it.
#[derive(Debug)]
pub(crate) enum NodeDataRootLockError {
    Locked,
    Invalid(String),
    Io(std::io::Error),
}

pub(crate) struct NodeDataRootLock {
    file: fs::File,
    created: bool,
    identity: NodeDataPathIdentity,
}

impl NodeDataRootLock {
    pub(crate) const fn was_created(&self) -> bool {
        self.created
    }

    /// Verifies that `path` still resolves to the exact file object locked by
    /// this guard. This is intentionally no-follow and must run under the
    /// guard before an installer trusts or mutates the data root.
    pub(crate) fn revalidate_path(&self, path: &Path) -> Result<(), NodeDataRootLockError> {
        let metadata = fs::symlink_metadata(path).map_err(NodeDataRootLockError::Io)?;
        validate_node_data_root_lock_metadata(
            &metadata,
            "node data lock changed to an invalid form",
        )?;
        let current = open_node_data_root_lock(path, false)?;
        let opened = current.metadata().map_err(NodeDataRootLockError::Io)?;
        validate_node_data_root_lock_metadata(
            &opened,
            "node data lock changed to an invalid form before revalidation",
        )?;
        if node_data_path_identity(&current)? != self.identity {
            return Err(NodeDataRootLockError::Invalid(
                "node data lock path does not identify the acquired lock".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn into_file(self) -> fs::File {
        self.file
    }
}

impl fmt::Display for NodeDataRootLockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Locked => formatter.write_str("node data root is already locked"),
            Self::Invalid(message) => formatter.write_str(message),
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for NodeDataRootLockError {}

fn node_data_path_is_reparse_point(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        return metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0;
    }
    #[cfg(not(windows))]
    {
        let _ = metadata;
        false
    }
}

fn validate_node_data_root_lock_metadata(
    metadata: &fs::Metadata,
    message: &str,
) -> Result<(), NodeDataRootLockError> {
    if node_data_path_is_reparse_point(metadata)
        || metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() != 0
    {
        return Err(NodeDataRootLockError::Invalid(message.into()));
    }
    Ok(())
}

fn validate_node_data_path_metadata(
    metadata: &fs::Metadata,
    directory: bool,
    message: &str,
) -> Result<(), NodeDataRootLockError> {
    if node_data_path_is_reparse_point(metadata)
        || metadata.file_type().is_symlink()
        || (directory && !metadata.is_dir())
        || (!directory && !metadata.is_file())
    {
        return Err(NodeDataRootLockError::Invalid(message.into()));
    }
    Ok(())
}

fn open_node_data_root_lock(
    path: &Path,
    create_new: bool,
) -> Result<fs::File, NodeDataRootLockError> {
    let mut options = fs::OpenOptions::new();
    options
        .read(true)
        .write(true)
        .truncate(false)
        .create_new(create_new);
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
    #[cfg(windows)]
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    options.open(path).map_err(NodeDataRootLockError::Io)
}

/// Captures an exact object identity through a no-follow handle. The restore
/// expected-state token uses this for every path it may trust or mutate, so
/// replacement by an equal-length file or a different directory cannot evade
/// its stale-state fence on Windows or Unix.
pub(crate) fn capture_node_data_path_identity(
    path: &Path,
    directory: bool,
) -> Result<NodeDataPathIdentity, NodeDataRootLockError> {
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (path, directory);
        return Err(NodeDataRootLockError::Invalid(
            "node data path filesystem identity is unsupported on this platform".into(),
        ));
    }
    #[cfg(any(unix, windows))]
    {
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
        #[cfg(windows)]
        options.custom_flags(
            FILE_FLAG_OPEN_REPARSE_POINT
                | if directory {
                    FILE_FLAG_BACKUP_SEMANTICS
                } else {
                    0
                },
        );
        let file = options.open(path).map_err(NodeDataRootLockError::Io)?;
        let metadata = file.metadata().map_err(NodeDataRootLockError::Io)?;
        validate_node_data_path_metadata(
            &metadata,
            directory,
            "node data path changed to an invalid form before identity capture",
        )?;
        node_data_path_identity(&file)
    }
}

fn node_data_path_identity(file: &fs::File) -> Result<NodeDataPathIdentity, NodeDataRootLockError> {
    #[cfg(unix)]
    {
        let metadata = file.metadata().map_err(NodeDataRootLockError::Io)?;
        Ok(NodeDataPathIdentity::Unix {
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;

        let mut information = std::mem::MaybeUninit::<WindowsFileIdInfo>::zeroed();
        let information_size = u32::try_from(std::mem::size_of::<WindowsFileIdInfo>())
            .expect("Windows FILE_ID_INFO size always fits in u32");
        // SAFETY: the OS owns the raw handle borrowed from `file`; the output
        // pointer and buffer length exactly describe one FILE_ID_INFO, and
        // FileIdInfo requests its complete 128-bit object identity.
        let result = unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle(),
                WINDOWS_FILE_ID_INFO_CLASS,
                information.as_mut_ptr().cast(),
                information_size,
            )
        };
        if result == 0 {
            return Err(NodeDataRootLockError::Io(std::io::Error::last_os_error()));
        }
        // SAFETY: a nonzero GetFileInformationByHandleEx result initializes
        // the complete FILE_ID_INFO output structure.
        let information = unsafe { information.assume_init() };
        Ok(NodeDataPathIdentity::Windows {
            volume_serial_number: information.volume_serial_number,
            file_id: information.file_id,
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = file;
        Err(NodeDataRootLockError::Invalid(
            "node data lock filesystem identity is unsupported on this platform".into(),
        ))
    }
}

/// Creates the configured data directory if absent and acquires its only
/// runtime/restore lock. The lock is a zero-length regular file opened with
/// no-follow semantics; hostile file types and a changed lock identity fail
/// before a caller may mutate recovery state.
pub(crate) fn acquire_node_data_root_lock(
    data_dir: &Path,
) -> Result<NodeDataRootLock, NodeDataRootLockError> {
    fs::create_dir_all(data_dir).map_err(NodeDataRootLockError::Io)?;
    let data_metadata = fs::symlink_metadata(data_dir).map_err(NodeDataRootLockError::Io)?;
    if data_metadata.file_type().is_symlink() || !data_metadata.is_dir() {
        return Err(NodeDataRootLockError::Invalid(
            "node data directory is not a regular directory".into(),
        ));
    }

    let lock_path = data_dir.join(NODE_DATA_ROOT_LOCK_FILE);
    let before = match fs::symlink_metadata(&lock_path) {
        Ok(metadata) => {
            validate_node_data_root_lock_metadata(
                &metadata,
                "node data lock is not a zero-length regular file",
            )?;
            Some(metadata)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(NodeDataRootLockError::Io(error)),
    };

    let (file, created) = if before.is_none() {
        match open_node_data_root_lock(&lock_path, true) {
            Ok(file) => (file, true),
            Err(NodeDataRootLockError::Io(error))
                if error.kind() == std::io::ErrorKind::AlreadyExists =>
            {
                let metadata =
                    fs::symlink_metadata(&lock_path).map_err(NodeDataRootLockError::Io)?;
                validate_node_data_root_lock_metadata(
                    &metadata,
                    "node data lock appeared in an invalid form",
                )?;
                (open_node_data_root_lock(&lock_path, false)?, false)
            }
            Err(error) => return Err(error),
        }
    } else {
        (open_node_data_root_lock(&lock_path, false)?, false)
    };
    let opened = file.metadata().map_err(NodeDataRootLockError::Io)?;
    validate_node_data_root_lock_metadata(
        &opened,
        "node data lock changed to an invalid form before open",
    )?;
    #[cfg(unix)]
    if let Some(before) = before {
        if before.dev() != opened.dev() || before.ino() != opened.ino() {
            return Err(NodeDataRootLockError::Invalid(
                "node data lock changed before no-follow open".into(),
            ));
        }
    }
    let identity = node_data_path_identity(&file)?;
    match file.try_lock() {
        Ok(()) => {
            let lock = NodeDataRootLock {
                file,
                created,
                identity,
            };
            lock.revalidate_path(&lock_path)?;
            Ok(lock)
        }
        Err(fs::TryLockError::WouldBlock) => Err(NodeDataRootLockError::Locked),
        Err(fs::TryLockError::Error(error)) => Err(NodeDataRootLockError::Io(error)),
    }
}

/// A bounded, clone-shared observer for successful physical SQL write batches.
///
/// Installing this observer enables write-phase timing. A runtime without an observer does not
/// read the clock or synchronize on the profiling path.
#[cfg(feature = "sql")]
#[derive(Clone)]
pub struct SqlWriteProfiler {
    inner: Arc<SqlWriteProfilerInner>,
}

#[cfg(feature = "sql")]
struct SqlWriteProfilerInner {
    capacity: usize,
    state: Mutex<SqlWriteProfilerState>,
}

#[cfg(feature = "sql")]
#[derive(Default)]
struct SqlWriteProfilerState {
    samples: VecDeque<SqlWriteProfileSample>,
    dropped_samples: u64,
}

/// Timing for one successfully committed physical SQL QWAL batch.
#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlWriteProfileSample {
    pub batch_member_count: usize,
    pub commit_lock_wait_us: u64,
    pub precheck_classification_us: u64,
    pub qwal_prepare_us: u64,
    pub consensus_propose_us: u64,
    pub local_qlog_mirror_append_us: u64,
    pub sql_materializer_apply_us: u64,
    pub response_other_total_us: u64,
    pub total_service_us: u64,
}

/// A point-in-time copy of a SQL write profiler's bounded samples.
#[cfg(feature = "sql")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SqlWriteProfileSnapshot {
    pub samples: Vec<SqlWriteProfileSample>,
    pub dropped_samples: u64,
}

#[cfg(feature = "sql")]
impl SqlWriteProfiler {
    /// Creates an observer that retains at most `capacity` of the newest samples.
    ///
    /// # Panics
    ///
    /// Panics when `capacity` is zero.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "SQL write profiler capacity must be non-zero");
        Self {
            inner: Arc::new(SqlWriteProfilerInner {
                capacity,
                state: Mutex::new(SqlWriteProfilerState::default()),
            }),
        }
    }

    pub fn snapshot(&self) -> SqlWriteProfileSnapshot {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        SqlWriteProfileSnapshot {
            samples: state.samples.iter().cloned().collect(),
            dropped_samples: state.dropped_samples,
        }
    }

    /// Returns and clears retained samples while preserving the cumulative dropped count.
    pub fn drain(&self) -> SqlWriteProfileSnapshot {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        SqlWriteProfileSnapshot {
            samples: state.samples.drain(..).collect(),
            dropped_samples: state.dropped_samples,
        }
    }

    fn record(&self, sample: SqlWriteProfileSample) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.samples.len() == self.inner.capacity {
            state.samples.pop_front();
            state.dropped_samples = state.dropped_samples.saturating_add(1);
        }
        state.samples.push_back(sample);
    }
}

#[cfg(feature = "sql")]
impl fmt::Debug for SqlWriteProfiler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlWriteProfiler")
            .field("capacity", &self.inner.capacity)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "sql")]
impl PartialEq for SqlWriteProfiler {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

#[cfg(feature = "sql")]
impl Eq for SqlWriteProfiler {}

/// Telemetry bridge for MAB-based auto-tuning.
#[cfg(feature = "tuner")]
#[derive(Clone)]
pub struct TunerTelemetry {
    inner: Arc<rhiza_tuner::TelemetryCollector>,
}

#[cfg(feature = "tuner")]
impl TunerTelemetry {
    pub fn new(collector: rhiza_tuner::TelemetryCollector) -> Self {
        Self {
            inner: Arc::new(collector),
        }
    }
    pub fn collector(&self) -> &rhiza_tuner::TelemetryCollector {
        &self.inner
    }
}

#[cfg(feature = "tuner")]
impl PartialEq for TunerTelemetry {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

#[cfg(feature = "tuner")]
impl Eq for TunerTelemetry {}

#[cfg(feature = "tuner")]
impl fmt::Debug for TunerTelemetry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TunerTelemetry").finish_non_exhaustive()
    }
}

pub const RECORDER_STORE_COMMAND_PATH: &str = "/v2/quepaxa/recorder/store-command";
pub const RECORDER_FETCH_COMMAND_PATH: &str = "/v2/quepaxa/recorder/fetch-command";
pub const RECORDER_STAGE_EFFECT_CHUNK_PATH: &str = "/v3/quepaxa/recorder/stage-effect-chunk";
pub const RECORDER_FINALIZE_EFFECT_BUNDLE_PATH: &str =
    "/v3/quepaxa/recorder/finalize-effect-bundle";
pub const RECORDER_FETCH_EFFECT_MANIFEST_PATH: &str = "/v3/quepaxa/recorder/fetch-effect-manifest";
pub const RECORDER_FETCH_EFFECT_CHUNK_PATH: &str = "/v3/quepaxa/recorder/fetch-effect-chunk";
pub const RECORDER_INSPECT_PROOF_PATH: &str = "/v2/quepaxa/recorder/inspect-proof";
pub const RECORDER_INSPECT_RECORD_PATH: &str = "/v2/quepaxa/recorder/inspect-record";
pub const RECORDER_READ_FENCE_PATH: &str = "/v3/quepaxa/recorder/read-fence";
pub const RECORDER_RECORD_PATH: &str = "/v2/quepaxa/recorder/record";
pub const RECORDER_INSTALL_PROOF_PATH: &str = "/v2/quepaxa/recorder/install-decision-proof";
pub const LOG_FETCH_PATH: &str = "/v1/log/fetch";
#[cfg(feature = "sql")]
pub const WRITE_PATH: &str = "/v1/write";
#[cfg(feature = "sql")]
pub const READ_PATH: &str = "/v1/read";
#[cfg(feature = "sql")]
pub const SQL_EXECUTE_PATH: &str = "/v1/sql/execute";
#[cfg(feature = "sql")]
pub const SQL_QUERY_PATH: &str = "/v1/sql/query";
#[cfg(feature = "graph")]
pub const GRAPH_PUT_DOCUMENT_PATH: &str = "/v1/graph/documents/put";
#[cfg(feature = "graph")]
pub const GRAPH_DELETE_DOCUMENT_PATH: &str = "/v1/graph/documents/delete";
#[cfg(feature = "graph")]
pub const GRAPH_GET_DOCUMENT_PATH: &str = "/v1/graph/documents/get";
#[cfg(feature = "graph")]
pub const GRAPH_QUERY_PATH: &str = "/v1/graph/query";
#[cfg(feature = "kv")]
pub const KV_PUT_PATH: &str = "/v1/kv/put";
#[cfg(feature = "kv")]
pub const KV_DELETE_PATH: &str = "/v1/kv/delete";
#[cfg(feature = "kv")]
pub const KV_GET_PATH: &str = "/v1/kv/get";
#[cfg(feature = "kv")]
pub const KV_SCAN_PATH: &str = "/v1/kv/scan";
#[cfg(feature = "kv")]
pub const KV_BATCH_PATH: &str = "/v1/kv/batch";
pub const LIVEZ_PATH: &str = "/livez";
pub const READYZ_PATH: &str = "/readyz";
const MAX_STARTUP_RECOVERY_ENTRIES: usize = 100_000;
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const RECORDER_HTTP_CONNECT_TIMEOUT: Duration = Duration::from_millis(150);
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const READ_FENCE_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
// Leave enough of the public one-second write budget to classify and return a
// lost-quorum attempt instead of racing the caller's ambiguous timeout.
const QUORUM_RECORD_REQUEST_TIMEOUT: Duration = Duration::from_millis(250);
const CLIENT_WRITE_WAIT_TIMEOUT: Duration = Duration::from_secs(1);
const SYNC_FLUSH_RETRY_INITIAL: Duration = Duration::from_millis(50);
const SYNC_FLUSH_RETRY_MAX: Duration = Duration::from_secs(1);

#[cfg(feature = "sql")]
pub const DEFAULT_SQL_MAX_ROWS: u32 = 1_000;
#[cfg(feature = "sql")]
pub const MAX_SQL_MAX_ROWS: u32 = 10_000;
#[cfg(feature = "sql")]
pub const MAX_SQL_RESULT_BYTES: usize = 1024 * 1024;
#[cfg(feature = "sql")]
pub const MAX_SQL_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
#[cfg(feature = "kv")]
type KvMemberCheck = (usize, Result<Option<KvRequestRecord>, NodeError>);
#[cfg(feature = "kv")]
pub const DEFAULT_KV_SCAN_LIMIT: u32 = 100;
#[cfg(feature = "kv")]
pub const MAX_KV_SCAN_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
#[cfg(feature = "graph")]
pub const DEFAULT_GRAPH_MAX_ROWS: u32 = 1_000;
#[cfg(feature = "graph")]
pub const MAX_GRAPH_MAX_ROWS: u32 = 10_000;
#[cfg(feature = "graph")]
pub const MAX_GRAPH_RESULT_BYTES: usize = 1024 * 1024;
#[cfg(feature = "graph")]
pub const MAX_GRAPH_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
#[cfg(feature = "graph")]
const GRAPH_QUERY_TIMEOUT_MS: u64 = 5_000;

pub fn effective_cluster_id(
    profile: ExecutionProfile,
    logical_cluster_id: &str,
) -> Result<String, ConfigError> {
    if let Some(actual) = canonical_cluster_profile(logical_cluster_id) {
        if actual != profile {
            return Err(ConfigError::ClusterIdProfileMismatch {
                expected: profile,
                actual,
            });
        }
        return Ok(logical_cluster_id.to_owned());
    }
    Ok(format!("rhiza:{}:{logical_cluster_id}", profile.as_str()))
}

pub const fn execution_profile_compiled(profile: ExecutionProfile) -> bool {
    match profile {
        ExecutionProfile::Sqlite => cfg!(feature = "sql"),
        ExecutionProfile::Graph => cfg!(feature = "graph"),
        ExecutionProfile::Kv => cfg!(feature = "kv"),
    }
}

fn canonical_cluster_profile(cluster_id: &str) -> Option<ExecutionProfile> {
    [
        ("rhiza:sql:", ExecutionProfile::Sqlite),
        ("rhiza:graph:", ExecutionProfile::Graph),
        ("rhiza:kv:", ExecutionProfile::Kv),
    ]
    .into_iter()
    .find_map(|(prefix, profile)| cluster_id.starts_with(prefix).then_some(profile))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigError {
    EmptyClusterId,
    ClusterIdProfileMismatch {
        expected: ExecutionProfile,
        actual: ExecutionProfile,
    },
    EmptyNodeId,
    EmptyDataDir,
    InvalidEpoch,
    InvalidConfigId,
    InvalidRecoveryGeneration,
    InvalidWriterBatchMax(usize),
    InvalidWriterBatchWindow,
    #[cfg(feature = "sql")]
    InvalidSqlGroupCommitQueueCapacity(usize),
    EmptyPeerNodeId,
    EmptyPeerBaseUrl,
    InvalidPeerBaseUrl(String),
    EmptyPeerToken,
    DuplicatePeerToken,
    InvalidPeerCount(usize),
    DuplicatePeerNodeId(String),
    LocalNodeMissing,
    PeerMembershipMismatch,
    InvalidPredecessorTransition(String),
    EmptyClientToken,
    ClientTokenConflictsWithPeer,
    EmptyAdminToken,
    AdminTokenConflictsWithRuntime,
    HttpClient(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyClusterId => write!(f, "cluster_id must not be empty"),
            Self::ClusterIdProfileMismatch { expected, actual } => write!(
                f,
                "cluster_id is canonical for the {} profile, not {}",
                actual.as_str(),
                expected.as_str()
            ),
            Self::EmptyNodeId => write!(f, "node_id must not be empty"),
            Self::EmptyDataDir => write!(f, "data_dir must not be empty"),
            Self::InvalidEpoch => write!(f, "epoch must be positive"),
            Self::InvalidConfigId => write!(f, "config_id must be positive"),
            Self::InvalidRecoveryGeneration => {
                write!(f, "recovery_generation must be positive")
            }
            Self::InvalidWriterBatchMax(max) => write!(
                f,
                "writer batch max must be within 1..={MAX_WRITE_BATCH_MEMBERS}, got {max}"
            ),
            Self::InvalidWriterBatchWindow => write!(
                f,
                "writer batch window must be positive and shorter than the client deadline"
            ),
            #[cfg(feature = "sql")]
            Self::InvalidSqlGroupCommitQueueCapacity(capacity) => write!(
                f,
                "SQL group commit queue capacity must be within 1..={MAX_SQL_GROUP_COMMIT_QUEUE_CAPACITY}, got {capacity}"
            ),
            Self::EmptyPeerNodeId => write!(f, "peer node_id must not be empty"),
            Self::EmptyPeerBaseUrl => write!(f, "peer base_url must not be empty"),
            Self::InvalidPeerBaseUrl(url) => write!(f, "invalid peer base_url: {url}"),
            Self::EmptyPeerToken => write!(f, "peer token must not be empty"),
            Self::DuplicatePeerToken => write!(f, "peer tokens must be unique"),
            Self::InvalidPeerCount(count) => {
                write!(
                    f,
                    "peer membership requires between three and seven nodes, got {count}"
                )
            }
            Self::DuplicatePeerNodeId(node_id) => {
                write!(f, "peer node_id must be unique: {node_id}")
            }
            Self::LocalNodeMissing => write!(f, "peer set must include the local node_id"),
            Self::PeerMembershipMismatch => {
                write!(
                    f,
                    "peer identities must exactly match the canonical membership"
                )
            }
            Self::InvalidPredecessorTransition(message) => {
                write!(f, "invalid predecessor transition: {message}")
            }
            Self::EmptyClientToken => write!(f, "client token must not be empty"),
            Self::ClientTokenConflictsWithPeer => {
                write!(f, "client token must differ from every peer token")
            }
            Self::EmptyAdminToken => write!(f, "admin token must not be empty"),
            Self::AdminTokenConflictsWithRuntime => {
                write!(f, "admin token must differ from client and peer tokens")
            }
            Self::HttpClient(message) => write!(f, "HTTP client configuration failed: {message}"),
        }
    }
}

impl std::error::Error for ConfigError {}

fn validate_recovery_generation(recovery_generation: u64) -> Result<(), ConfigError> {
    if recovery_generation == 0 {
        Err(ConfigError::InvalidRecoveryGeneration)
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckMode {
    HaFirst,
    DrStrong,
}

pub use rhiza_core::ReadConsistency;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct FetchLogRequest {
    pub from_index: LogIndex,
    pub max_entries: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct FetchLogResponse {
    pub entries: Vec<LogEntry>,
    pub last_index: LogIndex,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum FetchLogError {
    SnapshotRequired {
        anchor: Box<RecoveryAnchor>,
    },
    Gap {
        expected: LogIndex,
        actual: Option<LogIndex>,
    },
    Decode {
        message: String,
    },
    Transport {
        message: String,
    },
    InvalidAnchor {
        expected: LogHash,
        actual: LogHash,
    },
    InvalidEntry {
        index: LogIndex,
        message: String,
    },
    ForeignIdentity {
        index: LogIndex,
    },
    InvalidRequest {
        message: String,
    },
}

impl fmt::Display for FetchLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SnapshotRequired { anchor } => {
                write!(
                    f,
                    "snapshot restore required at qlog anchor {}",
                    anchor.compacted().index()
                )
            }
            Self::Gap { expected, actual } => {
                write!(f, "qlog gap: expected {expected}, got {actual:?}")
            }
            Self::Decode { message } => write!(f, "qlog response decode failed: {message}"),
            Self::Transport { message } => write!(f, "qlog transport failed: {message}"),
            Self::InvalidAnchor { .. } => write!(f, "qlog response has an invalid anchor"),
            Self::InvalidEntry { index, message } => {
                write!(f, "qlog entry {index} is invalid: {message}")
            }
            Self::ForeignIdentity { index } => {
                write!(f, "qlog entry {index} has a foreign identity")
            }
            Self::InvalidRequest { message } => write!(f, "invalid qlog request: {message}"),
        }
    }
}

impl std::error::Error for FetchLogError {}

pub trait LogPeer: Send + Sync {
    /// Fetches one bounded page.
    ///
    /// Process-bound implementations must use a finite deadline. HA shutdown
    /// retains and joins an admitted call even after its deadline is reported,
    /// so a non-returning custom implementation prevents quiescent shutdown.
    fn fetch_log(&self, request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError>;
}

#[derive(Clone, Eq, PartialEq)]
pub struct StartupCancellationToken {
    generation: u64,
}

impl fmt::Debug for StartupCancellationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StartupCancellationToken(..)")
    }
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupCancellationAuthority {
    Internal,
    External,
}

impl StartupCancellationAuthority {
    #[doc(hidden)]
    pub fn replaces(
        self,
        current: Self,
        requested_deadline: Instant,
        current_deadline: Instant,
    ) -> bool {
        matches!((self, current), (Self::External, Self::Internal))
            || (self == current && requested_deadline < current_deadline)
    }
}

#[derive(Clone)]
struct StartupCancellation {
    token: StartupCancellationToken,
    deadline: Instant,
    authority: StartupCancellationAuthority,
    receipt: StartupCloseReceipt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupCloseReceipt {
    context_id: u64,
    receipt_id: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupCloseOutcome {
    Drained,
    Replaced,
    Unresolved,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartupIoGate {
    Open,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartupCloseAttribution {
    /// The receipt currently owns the closed startup-I/O gate.  Only this
    /// receipt's deadline may prove an effect unresolved.
    Active,
    /// The receipt was superseded (or never won installation).  It remains
    /// recognizable so its waiter can observe `Replaced`, but its deadline is
    /// no longer part of the current close proof.
    Retired,
    /// This receipt's active deadline reached an extant effect before it could
    /// be replaced.  That failed proof is permanently attributable to this
    /// receipt as well as globally sticky.
    Unresolved,
}

#[derive(Clone)]
struct StartupCloseRecord {
    deadline: Instant,
    attribution: StartupCloseAttribution,
}

struct StartupIoState {
    cancellation: Option<StartupCancellation>,
    stage: &'static str,
    gate: StartupIoGate,
    effects: BTreeMap<u64, &'static str>,
    receipts: BTreeMap<u64, StartupCloseRecord>,
    next_receipt_id: u64,
    unresolved: bool,
    // Preserve the oldest effect that was still active at the exact point a
    // close deadline became conclusive.  In particular, a final permit drop
    // must not erase the diagnostic evidence before it latches unresolved.
    unresolved_stage: Option<&'static str>,
}

static NEXT_STARTUP_CANCELLATION_GENERATION: AtomicU64 = AtomicU64::new(1);
static NEXT_STARTUP_IO_CONTEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
static NEXT_STARTUP_LOCAL_IO_EFFECT_ID: AtomicU64 = AtomicU64::new(1);

struct StartupIoInner {
    context_id: u64,
    state: Mutex<StartupIoState>,
    changes: tokio::sync::watch::Sender<u64>,
    recorder_cancelled: Arc<AtomicBool>,
}

pub struct StartupLocalIoPermit {
    inner: Arc<StartupIoInner>,
    effect_id: u64,
}

impl fmt::Debug for StartupLocalIoPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StartupLocalIoPermit(..)")
    }
}

impl Drop for StartupLocalIoPermit {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // This is deliberately before removing `effect_id`: a runtime may
        // have been unable to poll `await_close` across the deadline, but the
        // effect itself still provides decisive proof when it finishes.
        state.latch_expired_active_close(Instant::now());
        let removed = state.effects.remove(&self.effect_id);
        debug_assert!(removed.is_some(), "startup local I/O permit dropped twice");
        drop(state);
        self.inner.notify();
    }
}

impl StartupIoState {
    fn oldest_effect_stage(&self) -> Option<&'static str> {
        self.effects.first_key_value().map(|(_, stage)| *stage)
    }

    /// Linearize expiry only for the receipt that currently owns the closed
    /// gate.  Retired receipts deliberately remain observable as `Replaced`,
    /// but can never turn a newer close receipt unresolved.
    fn latch_expired_active_close(&mut self, now: Instant) -> bool {
        if self.unresolved {
            return true;
        }
        let Some(cancellation) = self.cancellation.as_ref() else {
            return false;
        };
        let Some(receipt_id) = cancellation.receipt.receipt_id else {
            self.unresolved = true;
            self.unresolved_stage = self.oldest_effect_stage();
            return true;
        };
        let Some(record) = self.receipts.get(&receipt_id) else {
            self.unresolved = true;
            self.unresolved_stage = self.oldest_effect_stage();
            return true;
        };
        let deadline = record.deadline;
        if record.attribution != StartupCloseAttribution::Active {
            self.unresolved = true;
            self.unresolved_stage = self.oldest_effect_stage();
            return true;
        }
        if now >= deadline && !self.effects.is_empty() {
            self.unresolved = true;
            self.unresolved_stage = self.oldest_effect_stage();
            // `receipt_id` was observed as the current active attribution
            // above, so preserve that fact even after a later replacement.
            self.receipts
                .get_mut(&receipt_id)
                .expect("active startup close receipt must remain recorded")
                .attribution = StartupCloseAttribution::Unresolved;
        }
        self.unresolved
    }

    fn retire_current_close(&mut self) {
        let Some(cancellation) = self.cancellation.as_ref() else {
            return;
        };
        let Some(receipt_id) = cancellation.receipt.receipt_id else {
            self.unresolved = true;
            self.unresolved_stage = self.oldest_effect_stage();
            return;
        };
        let Some(record) = self.receipts.get_mut(&receipt_id) else {
            self.unresolved = true;
            self.unresolved_stage = self.oldest_effect_stage();
            return;
        };
        match record.attribution {
            StartupCloseAttribution::Active => {
                record.attribution = if self.unresolved {
                    StartupCloseAttribution::Unresolved
                } else {
                    StartupCloseAttribution::Retired
                };
            }
            // `latch_expired_active_close` already bound this receipt to the
            // failed proof while it was active.  Replacement must preserve,
            // rather than overwrite, that ordering evidence.
            StartupCloseAttribution::Unresolved => debug_assert!(self.unresolved),
            StartupCloseAttribution::Retired => {
                debug_assert!(false, "current startup close receipt was already retired")
            }
        }
    }
}

impl StartupIoInner {
    fn notify(&self) {
        self.changes.send_modify(|revision| {
            *revision = revision.wrapping_add(1);
        });
    }
}

#[cfg(test)]
#[derive(Clone)]
struct TestStartupLocalIoHook {
    context_id: u64,
    stage: &'static str,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

#[cfg(test)]
static TEST_STARTUP_LOCAL_IO_HOOK: OnceLock<Mutex<Option<Arc<TestStartupLocalIoHook>>>> =
    OnceLock::new();

#[cfg(test)]
static TEST_STARTUP_PERSISTENCE_HOOK: OnceLock<Mutex<Option<Arc<TestStartupPersistenceHook>>>> =
    OnceLock::new();

#[cfg(test)]
static TEST_STARTUP_EFFECT_HOOK_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

#[cfg(test)]
#[derive(Clone)]
struct TestStartupPersistenceHook {
    context_id: u64,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
    error: NodeError,
}

#[cfg(test)]
fn test_startup_effect_hook_lock() -> &'static tokio::sync::Mutex<()> {
    TEST_STARTUP_EFFECT_HOOK_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[cfg(test)]
fn test_startup_local_io_hook_slot() -> &'static Mutex<Option<Arc<TestStartupLocalIoHook>>> {
    TEST_STARTUP_LOCAL_IO_HOOK.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn test_startup_persistence_hook_slot() -> &'static Mutex<Option<Arc<TestStartupPersistenceHook>>> {
    TEST_STARTUP_PERSISTENCE_HOOK.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
struct TestStartupLocalIoHookGuard;

#[cfg(test)]
impl Drop for TestStartupLocalIoHookGuard {
    fn drop(&mut self) {
        *test_startup_local_io_hook_slot()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

#[cfg(test)]
fn install_test_startup_local_io_hook(hook: TestStartupLocalIoHook) -> TestStartupLocalIoHookGuard {
    let mut slot = test_startup_local_io_hook_slot()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(slot.is_none(), "startup local I/O hook already installed");
    *slot = Some(Arc::new(hook));
    TestStartupLocalIoHookGuard
}

#[cfg(test)]
struct TestStartupPersistenceHookGuard;

#[cfg(test)]
impl Drop for TestStartupPersistenceHookGuard {
    fn drop(&mut self) {
        *test_startup_persistence_hook_slot()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

#[cfg(test)]
fn install_test_startup_persistence_hook(
    hook: TestStartupPersistenceHook,
) -> TestStartupPersistenceHookGuard {
    let mut slot = test_startup_persistence_hook_slot()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(slot.is_none(), "startup persistence hook already installed");
    *slot = Some(Arc::new(hook));
    TestStartupPersistenceHookGuard
}

#[cfg(test)]
fn test_startup_local_io_hook_after_admission(context_id: u64, stage: &'static str) {
    let Some(hook) = test_startup_local_io_hook_slot()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
    else {
        return;
    };
    if hook.context_id != context_id || hook.stage != stage {
        return;
    }
    hook.entered
        .send(())
        .expect("startup local I/O test receiver");
    let (released, condition) = &*hook.release;
    let mut released = released
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while !*released {
        released = condition
            .wait(released)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
}

#[cfg(test)]
fn test_startup_persistence_hook_after_qlog_append(context_id: u64) -> Result<(), NodeError> {
    let Some(hook) = test_startup_persistence_hook_slot()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
    else {
        return Ok(());
    };
    if hook.context_id != context_id {
        return Ok(());
    }
    hook.entered
        .send(())
        .expect("startup persistence test receiver");
    let (released, condition) = &*hook.release;
    let mut released = released
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while !*released {
        released = condition
            .wait(released)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
    Err(hook.error.clone())
}

#[derive(Clone)]
pub struct StartupIoContext {
    inner: Arc<StartupIoInner>,
}

impl fmt::Debug for StartupIoContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StartupIoContext(..)")
    }
}

impl Default for StartupIoContext {
    fn default() -> Self {
        Self::new()
    }
}

impl StartupIoContext {
    #[doc(hidden)]
    pub fn recorder_context(&self) -> RecorderRpcContext {
        RecorderRpcContext::with_timeout_and_cancellation(
            DEFAULT_RECORDER_RPC_TIMEOUT,
            Arc::clone(&self.inner.recorder_cancelled),
        )
    }

    pub fn new() -> Self {
        let (changes, _) = tokio::sync::watch::channel(0_u64);
        Self {
            inner: Arc::new(StartupIoInner {
                context_id: allocate_startup_cancellation_generation(
                    &NEXT_STARTUP_IO_CONTEXT_GENERATION,
                ),
                state: Mutex::new(StartupIoState {
                    cancellation: None,
                    stage: "runtime initialization",
                    gate: StartupIoGate::Open,
                    effects: BTreeMap::new(),
                    receipts: BTreeMap::new(),
                    next_receipt_id: 1,
                    unresolved: false,
                    unresolved_stage: None,
                }),
                changes,
                recorder_cancelled: Arc::new(AtomicBool::new(false)),
            }),
        }
    }

    pub fn cancel(&self, deadline: Instant) -> StartupCloseReceipt {
        self.cancel_for_shutdown(
            Self::issue_cancellation_token(),
            deadline,
            StartupCancellationAuthority::Internal,
        )
    }

    #[doc(hidden)]
    pub fn issue_cancellation_token() -> StartupCancellationToken {
        let generation =
            allocate_startup_cancellation_generation(&NEXT_STARTUP_CANCELLATION_GENERATION);
        StartupCancellationToken { generation }
    }

    #[doc(hidden)]
    pub fn cancel_for_shutdown(
        &self,
        token: StartupCancellationToken,
        deadline: Instant,
        authority: StartupCancellationAuthority,
    ) -> StartupCloseReceipt {
        let mut state = self.lock_state();
        let replace = state.cancellation.as_ref().is_none_or(|existing| {
            authority.replaces(existing.authority, deadline, existing.deadline)
        });
        if !replace
            && state
                .cancellation
                .as_ref()
                .is_some_and(|existing| existing.token == token)
        {
            return state
                .cancellation
                .as_ref()
                .expect("checked cancellation exists")
                .receipt
                .clone();
        }

        // A replacement is ordered after the active receipt's deadline check.
        // This makes deadline-first sticky while allowing a replacement that
        // wins before the old deadline to retire the old receipt cleanly.
        if replace {
            state.latch_expired_active_close(Instant::now());
        }

        let receipt = if state.next_receipt_id == 0 {
            // A close receipt must never be reused: the only safe result once
            // its namespace is exhausted is a permanent inability to prove
            // that startup effects drained.
            state.unresolved = true;
            StartupCloseReceipt {
                context_id: self.inner.context_id,
                receipt_id: None,
            }
        } else {
            let receipt_id = state.next_receipt_id;
            state.next_receipt_id = state.next_receipt_id.checked_add(1).unwrap_or(0);
            state.receipts.insert(
                receipt_id,
                StartupCloseRecord {
                    deadline,
                    attribution: if replace {
                        StartupCloseAttribution::Active
                    } else {
                        // A lower-precedence close request is recognized
                        // but never owns the gate.
                        StartupCloseAttribution::Retired
                    },
                },
            );
            StartupCloseReceipt {
                context_id: self.inner.context_id,
                receipt_id: Some(receipt_id),
            }
        };
        state.gate = StartupIoGate::Closed;
        if replace {
            state.retire_current_close();
            state.cancellation = Some(StartupCancellation {
                token,
                deadline,
                authority,
                receipt: receipt.clone(),
            });
            // Installing an already-expired deadline is itself conclusive
            // when any local I/O effect remains active; it must not depend on
            // an awaiter getting CPU time later.
            state.latch_expired_active_close(Instant::now());
        }
        drop(state);
        self.inner.recorder_cancelled.store(true, Ordering::Release);
        self.inner.notify();
        receipt
    }

    #[doc(hidden)]
    pub fn is_cancelled_by(&self, token: &StartupCancellationToken) -> bool {
        self.lock_state()
            .cancellation
            .as_ref()
            .is_some_and(|cancellation| &cancellation.token == token)
    }

    pub fn unfinished_stage(&self) -> &'static str {
        let state = self.lock_state();
        if let Some(stage) = state.unresolved_stage {
            return stage;
        }
        state
            .effects
            .first_key_value()
            .map_or(state.stage, |(_, stage)| *stage)
    }

    pub fn check(&self, stage: &'static str) -> Result<(), NodeError> {
        let mut state = self.lock_state();
        state.stage = stage;
        if let Some(cancellation) = state.cancellation.as_ref() {
            return Err(NodeError::StartupCancelled {
                token: cancellation.token.clone(),
                stage: stage.into(),
            });
        }
        Ok(())
    }

    /// Waits until startup closes, using the same state transition feed as
    /// close receipts without creating a second cancellation mechanism.
    #[doc(hidden)]
    pub async fn wait_for_cancellation(&self, stage: &'static str) -> NodeError {
        let mut changes = self.inner.changes.subscribe();
        loop {
            if let Err(error) = self.check(stage) {
                return error;
            }
            if changes.changed().await.is_err() {
                return NodeError::Invariant(
                    "startup cancellation state channel closed unexpectedly".into(),
                );
            }
        }
    }

    pub fn admit_local_io(&self, stage: &'static str) -> Result<StartupLocalIoPermit, NodeError> {
        let mut state = self.lock_state();
        state.stage = stage;
        if state.gate == StartupIoGate::Closed {
            let cancellation = state
                .cancellation
                .as_ref()
                .expect("closed startup I/O gate must have cancellation attribution");
            return Err(NodeError::StartupCancelled {
                token: cancellation.token.clone(),
                stage: format!("before {stage}"),
            });
        }
        let effect_id = allocate_startup_local_io_effect_id(&NEXT_STARTUP_LOCAL_IO_EFFECT_ID)?;
        let replaced = state.effects.insert(effect_id, stage);
        debug_assert!(replaced.is_none(), "startup local I/O effect id was reused");
        drop(state);
        self.inner.notify();
        #[cfg(test)]
        test_startup_local_io_hook_after_admission(self.inner.context_id, stage);
        Ok(StartupLocalIoPermit {
            inner: Arc::clone(&self.inner),
            effect_id,
        })
    }

    pub async fn await_close(&self, receipt: &StartupCloseReceipt) -> StartupCloseOutcome {
        let mut changes = self.inner.changes.subscribe();
        loop {
            let decision = {
                let mut state = self.lock_state();
                if receipt.context_id != self.inner.context_id {
                    Some(StartupCloseOutcome::Unresolved)
                } else {
                    let Some(receipt_id) = receipt.receipt_id else {
                        return StartupCloseOutcome::Unresolved;
                    };
                    let Some(record) = state.receipts.get(&receipt_id) else {
                        return StartupCloseOutcome::Unresolved;
                    };
                    if record.attribution == StartupCloseAttribution::Unresolved {
                        Some(StartupCloseOutcome::Unresolved)
                    } else if record.attribution == StartupCloseAttribution::Retired {
                        Some(StartupCloseOutcome::Replaced)
                    } else if state.unresolved {
                        Some(StartupCloseOutcome::Unresolved)
                    } else if state
                        .cancellation
                        .as_ref()
                        .is_none_or(|current| current.receipt != *receipt)
                    {
                        Some(StartupCloseOutcome::Replaced)
                    } else if state.latch_expired_active_close(Instant::now()) {
                        Some(StartupCloseOutcome::Unresolved)
                    } else if state.effects.is_empty() {
                        Some(StartupCloseOutcome::Drained)
                    } else {
                        None
                    }
                }
            };
            if let Some(outcome) = decision {
                if outcome == StartupCloseOutcome::Unresolved {
                    self.inner.notify();
                }
                return outcome;
            }

            let deadline = {
                let state = self.lock_state();
                state
                    .receipts
                    .get(&receipt.receipt_id.expect("pending receipt has an id"))
                    .expect("pending startup close receipt must remain recognized")
                    .deadline
            };
            tokio::select! {
                changed = changes.changed() => {
                    if changed.is_err() {
                        return StartupCloseOutcome::Unresolved;
                    }
                }
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
            }
        }
    }

    pub fn recognizes_close_receipt(&self, receipt: &StartupCloseReceipt) -> bool {
        receipt.context_id == self.inner.context_id
            && receipt
                .receipt_id
                .is_some_and(|receipt_id| self.lock_state().receipts.contains_key(&receipt_id))
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, StartupIoState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn allocate_startup_cancellation_generation(next: &AtomicU64) -> u64 {
    match next.fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
        (generation != 0).then(|| generation.checked_add(1).unwrap_or(0))
    }) {
        Ok(generation) => generation,
        Err(_) => panic!("startup cancellation generation exhausted"),
    }
}

fn allocate_startup_local_io_effect_id(next: &AtomicU64) -> Result<u64, NodeError> {
    next.fetch_update(Ordering::AcqRel, Ordering::Acquire, |effect_id| {
        (effect_id != 0).then(|| effect_id.checked_add(1).unwrap_or(0))
    })
    .map_err(|_| NodeError::Invariant("startup local I/O effect id exhausted".into()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InMemoryLogPeer {
    entries: Vec<LogEntry>,
    anchor: Option<RecoveryAnchor>,
}

impl InMemoryLogPeer {
    pub fn new(mut entries: Vec<LogEntry>) -> Self {
        entries.sort_by_key(|entry| entry.index);
        Self {
            entries,
            anchor: None,
        }
    }

    pub fn with_anchor(mut entries: Vec<LogEntry>, anchor: RecoveryAnchor) -> Self {
        entries.sort_by_key(|entry| entry.index);
        Self {
            entries,
            anchor: Some(anchor),
        }
    }
}

impl LogPeer for InMemoryLogPeer {
    fn fetch_log(&self, request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError> {
        if let Some(anchor) = &self.anchor {
            if request.from_index <= anchor.compacted().index() {
                return Err(FetchLogError::SnapshotRequired {
                    anchor: Box::new(anchor.clone()),
                });
            }
        }
        let entries = self
            .entries
            .iter()
            .filter(|entry| entry.index >= request.from_index)
            .take(request.max_entries as usize)
            .cloned()
            .collect();
        let last_index = self
            .entries
            .last()
            .map(|entry| entry.index)
            .or_else(|| {
                self.anchor
                    .as_ref()
                    .map(|anchor| anchor.compacted().index())
            })
            .unwrap_or(0);
        Ok(FetchLogResponse {
            entries,
            last_index,
        })
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct RecorderWire<T> {
    version: u16,
    /// The caller's remaining budget at serialization time.  This is a
    /// duration, rather than a clock timestamp, because peers do not share a
    /// wall clock.  Servers reconstruct a local absolute deadline from it.
    remaining_deadline_ms: u32,
    body: T,
}

impl<T> RecorderWire<T> {
    fn rpc_context(&self) -> rhiza_quepaxa::Result<rhiza_quepaxa::RecorderRpcContext> {
        let budget = Duration::from_millis(u64::from(self.remaining_deadline_ms));
        if budget.is_zero() {
            return Err(rhiza_quepaxa::Error::RpcDeadlineExceeded);
        }
        Ok(rhiza_quepaxa::RecorderRpcContext::with_timeout(
            budget.min(HTTP_REQUEST_TIMEOUT),
        ))
    }
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(tag = "status", content = "body")]
enum RecorderV2Result<T> {
    Ok(T),
    Rejected(RejectReason),
    Error(RecorderWireError),
}

#[derive(serde::Deserialize)]
enum RecorderV2Status {
    Ok,
    Rejected,
    Error,
}

enum RecorderV2Field {
    Status,
    Body,
}

impl<'de> serde::Deserialize<'de> for RecorderV2Field {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FieldVisitor;

        impl<'de> serde::de::Visitor<'de> for FieldVisitor {
            type Value = RecorderV2Field;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("the recorder result status or body field")
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "status" => Ok(RecorderV2Field::Status),
                    "body" => Ok(RecorderV2Field::Body),
                    _ => Err(E::custom("unknown recorder result field")),
                }
            }

            fn visit_borrowed_str<E>(self, value: &'de str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(value)
            }
        }

        deserializer.deserialize_identifier(FieldVisitor)
    }
}

impl<'de, T> serde::Deserialize<'de> for RecorderV2Result<T>
where
    T: serde::Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            deserializer.deserialize_struct(
                "RecorderV2Result",
                &["status", "body"],
                RecorderV2HumanVisitor(std::marker::PhantomData),
            )
        } else {
            deserializer.deserialize_enum(
                "RecorderV2Result",
                &["Ok", "Rejected", "Error"],
                RecorderV2PostcardVisitor(std::marker::PhantomData),
            )
        }
    }
}

struct RecorderV2HumanVisitor<T>(std::marker::PhantomData<T>);

impl<'de, T> serde::de::Visitor<'de> for RecorderV2HumanVisitor<T>
where
    T: serde::Deserialize<'de>,
{
    type Value = RecorderV2Result<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a recorder result object with status followed by body")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let Some(RecorderV2Field::Status) = map.next_key()? else {
            return Err(<A::Error as serde::de::Error>::custom(
                "recorder result must start with status",
            ));
        };
        let status: RecorderV2Status = map.next_value()?;
        let Some(RecorderV2Field::Body) = map.next_key()? else {
            return Err(<A::Error as serde::de::Error>::custom(
                "recorder result must contain body immediately after status",
            ));
        };
        let result = match status {
            RecorderV2Status::Ok => Self::Value::Ok(map.next_value()?),
            RecorderV2Status::Rejected => Self::Value::Rejected(map.next_value()?),
            RecorderV2Status::Error => Self::Value::Error(map.next_value()?),
        };
        if map.next_key::<RecorderV2Field>()?.is_some() {
            return Err(<A::Error as serde::de::Error>::custom(
                "recorder result has extra or duplicate fields",
            ));
        }
        Ok(result)
    }
}

struct RecorderV2PostcardVisitor<T>(std::marker::PhantomData<T>);

impl<'de, T> serde::de::Visitor<'de> for RecorderV2PostcardVisitor<T>
where
    T: serde::Deserialize<'de>,
{
    type Value = RecorderV2Result<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a recorder result status followed by its body")
    }

    fn visit_enum<A>(self, data: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: serde::de::EnumAccess<'de>,
    {
        let (status, variant) = data.variant::<RecorderV2Status>()?;
        match status {
            RecorderV2Status::Ok => Ok(Self::Value::Ok(serde::de::VariantAccess::newtype_variant(
                variant,
            )?)),
            RecorderV2Status::Rejected => Ok(Self::Value::Rejected(
                serde::de::VariantAccess::newtype_variant(variant)?,
            )),
            RecorderV2Status::Error => Ok(Self::Value::Error(
                serde::de::VariantAccess::newtype_variant(variant)?,
            )),
        }
    }
}

/// Stable recorder-RPC error identity shared by HTTP and both framed
/// transports. Text remains diagnostic only; callers must not parse it to
/// decide retry or recovery behavior.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct RecorderWireError {
    code: RecorderWireErrorCode,
    message: String,
    detail: Option<RecorderWireErrorDetail>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) enum RecorderWireErrorCode {
    ChainConflict,
    CommandHashMismatch,
    CommandTooLarge,
    RpcCancelled,
    RpcDeadlineExceeded,
    Cancelled,
    ConflictingCertificates,
    DuplicateRecorderIdentity,
    EmptyRecorderIdentity,
    EmptyFixedMembership,
    InvalidFixedMembershipSize,
    InvalidRecoveredTip,
    RandomnessUnavailable,
    RecorderRootLocked,
    Rejected,
    ReadFenceUnsupported,
    TypedProofInstallRequired,
    TypedRecordRequired,
    UnknownOutcome,
    Io,
    Decode,
    NoQuorum,
    ProposeFailed,
    CommandUnavailable,
    EffectBundleConflict,
    EffectBundleInvalid,
    EffectBundleQuotaExceeded,
    EffectBundleUnavailable,
    Other,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) enum RecorderWireErrorDetail {
    ChainConflict {
        slot: u64,
        expected_prev_hash: LogHash,
        actual_prev_hash: LogHash,
    },
    CommandTooLarge {
        actual: usize,
        limit: usize,
    },
    EffectBundleQuotaExceeded {
        actual: u64,
        limit: u64,
    },
    Message(String),
    Path(PathBuf),
    Rejected(RejectReason),
}

pub(crate) fn recorder_wire_error(error: rhiza_quepaxa::Error) -> RecorderWireError {
    use rhiza_quepaxa::Error;
    let (code, detail) = match &error {
        Error::ChainConflict {
            slot,
            expected_prev_hash,
            actual_prev_hash,
        } => (
            RecorderWireErrorCode::ChainConflict,
            Some(RecorderWireErrorDetail::ChainConflict {
                slot: *slot,
                expected_prev_hash: *expected_prev_hash,
                actual_prev_hash: *actual_prev_hash,
            }),
        ),
        Error::CommandHashMismatch => (RecorderWireErrorCode::CommandHashMismatch, None),
        Error::CommandTooLarge { actual, limit } => (
            RecorderWireErrorCode::CommandTooLarge,
            Some(RecorderWireErrorDetail::CommandTooLarge {
                actual: *actual,
                limit: *limit,
            }),
        ),
        Error::CommandUnavailable => (RecorderWireErrorCode::CommandUnavailable, None),
        Error::EffectBundleConflict => (RecorderWireErrorCode::EffectBundleConflict, None),
        Error::EffectBundleInvalid(message) => (
            RecorderWireErrorCode::EffectBundleInvalid,
            Some(RecorderWireErrorDetail::Message(message.clone())),
        ),
        Error::EffectBundleQuotaExceeded { actual, limit } => (
            RecorderWireErrorCode::EffectBundleQuotaExceeded,
            Some(RecorderWireErrorDetail::EffectBundleQuotaExceeded {
                actual: *actual,
                limit: *limit,
            }),
        ),
        Error::EffectBundleUnavailable => (RecorderWireErrorCode::EffectBundleUnavailable, None),
        Error::Cancelled => (RecorderWireErrorCode::Cancelled, None),
        Error::RpcCancelled => (RecorderWireErrorCode::RpcCancelled, None),
        Error::RpcDeadlineExceeded => (RecorderWireErrorCode::RpcDeadlineExceeded, None),
        Error::ConflictingCertificates => (RecorderWireErrorCode::ConflictingCertificates, None),
        Error::Decode(message) => (
            RecorderWireErrorCode::Decode,
            Some(RecorderWireErrorDetail::Message(message.clone())),
        ),
        Error::DuplicateRecorderIdentity => {
            (RecorderWireErrorCode::DuplicateRecorderIdentity, None)
        }
        Error::EmptyRecorderIdentity => (RecorderWireErrorCode::EmptyRecorderIdentity, None),
        Error::EmptyFixedMembership => (RecorderWireErrorCode::EmptyFixedMembership, None),
        Error::InvalidFixedMembershipSize => {
            (RecorderWireErrorCode::InvalidFixedMembershipSize, None)
        }
        Error::InvalidRecoveredTip => (RecorderWireErrorCode::InvalidRecoveredTip, None),
        Error::Io(message) => (
            RecorderWireErrorCode::Io,
            Some(RecorderWireErrorDetail::Message(message.clone())),
        ),
        Error::NoQuorum => (RecorderWireErrorCode::NoQuorum, None),
        Error::ProposeFailed => (RecorderWireErrorCode::ProposeFailed, None),
        Error::RandomnessUnavailable(message) => (
            RecorderWireErrorCode::RandomnessUnavailable,
            Some(RecorderWireErrorDetail::Message(message.clone())),
        ),
        Error::RecorderRootLocked(path) => (
            RecorderWireErrorCode::RecorderRootLocked,
            Some(RecorderWireErrorDetail::Path(path.clone())),
        ),
        Error::Rejected(reason) => (
            RecorderWireErrorCode::Rejected,
            Some(RecorderWireErrorDetail::Rejected(reason.clone())),
        ),
        Error::ReadFenceUnsupported => (RecorderWireErrorCode::ReadFenceUnsupported, None),
        Error::TypedProofInstallRequired => {
            (RecorderWireErrorCode::TypedProofInstallRequired, None)
        }
        Error::TypedRecordRequired => (RecorderWireErrorCode::TypedRecordRequired, None),
        Error::UnknownOutcome => (RecorderWireErrorCode::UnknownOutcome, None),
    };
    RecorderWireError {
        code,
        message: error.to_string(),
        detail,
    }
}

pub(crate) fn recorder_error_from_wire(error: RecorderWireError) -> rhiza_quepaxa::Error {
    use rhiza_quepaxa::Error;
    let invalid = || Error::Decode(format!("invalid recorder error payload: {}", error.message));
    match (error.code, error.detail) {
        (
            RecorderWireErrorCode::ChainConflict,
            Some(RecorderWireErrorDetail::ChainConflict {
                slot,
                expected_prev_hash,
                actual_prev_hash,
            }),
        ) => Error::ChainConflict {
            slot,
            expected_prev_hash,
            actual_prev_hash,
        },
        (RecorderWireErrorCode::CommandHashMismatch, None) => Error::CommandHashMismatch,
        (
            RecorderWireErrorCode::CommandTooLarge,
            Some(RecorderWireErrorDetail::CommandTooLarge { actual, limit }),
        ) => Error::CommandTooLarge { actual, limit },
        (RecorderWireErrorCode::RpcCancelled, None) => Error::RpcCancelled,
        (RecorderWireErrorCode::RpcDeadlineExceeded, None) => Error::RpcDeadlineExceeded,
        (RecorderWireErrorCode::CommandUnavailable, None) => Error::CommandUnavailable,
        (RecorderWireErrorCode::EffectBundleConflict, None) => Error::EffectBundleConflict,
        (
            RecorderWireErrorCode::EffectBundleInvalid,
            Some(RecorderWireErrorDetail::Message(message)),
        ) => Error::EffectBundleInvalid(message),
        (
            RecorderWireErrorCode::EffectBundleQuotaExceeded,
            Some(RecorderWireErrorDetail::EffectBundleQuotaExceeded { actual, limit }),
        ) => Error::EffectBundleQuotaExceeded { actual, limit },
        (RecorderWireErrorCode::EffectBundleUnavailable, None) => Error::EffectBundleUnavailable,
        (RecorderWireErrorCode::Cancelled, None) => Error::Cancelled,
        (RecorderWireErrorCode::ConflictingCertificates, None) => Error::ConflictingCertificates,
        (RecorderWireErrorCode::Decode, Some(RecorderWireErrorDetail::Message(message))) => {
            Error::Decode(message)
        }
        (RecorderWireErrorCode::DuplicateRecorderIdentity, None) => {
            Error::DuplicateRecorderIdentity
        }
        (RecorderWireErrorCode::EmptyRecorderIdentity, None) => Error::EmptyRecorderIdentity,
        (RecorderWireErrorCode::EmptyFixedMembership, None) => Error::EmptyFixedMembership,
        (RecorderWireErrorCode::InvalidFixedMembershipSize, None) => {
            Error::InvalidFixedMembershipSize
        }
        (RecorderWireErrorCode::InvalidRecoveredTip, None) => Error::InvalidRecoveredTip,
        (RecorderWireErrorCode::Io, Some(RecorderWireErrorDetail::Message(message))) => {
            Error::Io(message)
        }
        (RecorderWireErrorCode::NoQuorum, None) => Error::NoQuorum,
        (RecorderWireErrorCode::ProposeFailed, None) => Error::ProposeFailed,
        (
            RecorderWireErrorCode::RandomnessUnavailable,
            Some(RecorderWireErrorDetail::Message(message)),
        ) => Error::RandomnessUnavailable(message),
        (RecorderWireErrorCode::RecorderRootLocked, Some(RecorderWireErrorDetail::Path(path))) => {
            Error::RecorderRootLocked(path)
        }
        (RecorderWireErrorCode::Rejected, Some(RecorderWireErrorDetail::Rejected(reason))) => {
            Error::Rejected(reason)
        }
        (RecorderWireErrorCode::ReadFenceUnsupported, None) => Error::ReadFenceUnsupported,
        (RecorderWireErrorCode::TypedProofInstallRequired, None) => {
            Error::TypedProofInstallRequired
        }
        (RecorderWireErrorCode::TypedRecordRequired, None) => Error::TypedRecordRequired,
        (RecorderWireErrorCode::UnknownOutcome, None) => Error::UnknownOutcome,
        _ => invalid(),
    }
}

pub(crate) fn preserve_mutation_outcome(error: rhiza_quepaxa::Error) -> rhiza_quepaxa::Error {
    match error {
        rhiza_quepaxa::Error::RpcCancelled
        | rhiza_quepaxa::Error::RpcDeadlineExceeded
        | rhiza_quepaxa::Error::UnknownOutcome => rhiza_quepaxa::Error::UnknownOutcome,
        error => error,
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct StoreCommandV2 {
    cluster_id: String,
    epoch: u64,
    config_id: u64,
    config_digest: LogHash,
    command_hash: LogHash,
    command: StoredCommand,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct FetchCommandV2 {
    cluster_id: String,
    epoch: u64,
    config_id: u64,
    config_digest: LogHash,
    command_hash: LogHash,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct StageEffectChunkV3 {
    binding: EffectBundleBinding,
    manifest_command: StoredCommand,
    ordinal: u16,
    chunk: Vec<u8>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct FinalizeEffectBundleV3 {
    binding: EffectBundleBinding,
    manifest_command: StoredCommand,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct FetchEffectManifestV3 {
    binding: EffectBundleBinding,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct FetchEffectChunkV3 {
    binding: EffectBundleBinding,
    ordinal: u16,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct InspectProofV2 {
    slot: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct InstallProofV2 {
    proof: DecisionProof,
    members: Vec<String>,
}

#[derive(Clone)]
pub struct HttpRecorderClient {
    base_url: String,
    local_node_id: String,
    peer_token: String,
    recovery_generation: u64,
    shared: Arc<HttpRecorderShared>,
}

struct HttpRecorderShared {
    client: OnceLock<reqwest::blocking::Client>,
    client_initialization: Mutex<()>,
    available_workers: Mutex<usize>,
    workers_available: Condvar,
}

struct HttpRecorderWorkerPermit {
    shared: Arc<HttpRecorderShared>,
}

enum HttpCall<U> {
    Response(StatusCode, RecorderWire<RecorderV2Result<U>>),
    NotSent(String),
    Transport(String),
    Decode(String),
}

const RECORDER_HTTP_REQUEST_LIMIT_ERROR: &str = "recorder HTTP request body exceeds limit";
const RECORDER_HTTP_RESPONSE_LIMIT_ERROR: &str = "recorder HTTP response body exceeds limit";

/// Counts JSON bytes without retaining them. `serde_json::to_writer` otherwise
/// grows an allocation before callers can enforce the transport body cap.
struct BoundedJsonSize {
    size: usize,
    limit: usize,
    exceeded: bool,
}

impl Write for BoundedJsonSize {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(size) = self.size.checked_add(bytes.len()) else {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "JSON body exceeds limit",
            ));
        };
        if size > self.limit {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "JSON body exceeds limit",
            ));
        }
        self.size = size;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Writes the second serialization pass into its exactly sized allocation.
struct ExactJsonBuffer<'a> {
    bytes: &'a mut [u8],
    written: usize,
}

impl Write for ExactJsonBuffer<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(end) = self.written.checked_add(bytes.len()) else {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "JSON serialization size changed",
            ));
        };
        let Some(target) = self.bytes.get_mut(self.written..end) else {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "JSON serialization size changed",
            ));
        };
        target.copy_from_slice(bytes);
        self.written = end;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn bounded_json_size<T: serde::Serialize + ?Sized>(
    value: &T,
    limit: usize,
    limit_error: &'static str,
) -> Result<usize, String> {
    let mut counter = BoundedJsonSize {
        size: 0,
        limit,
        exceeded: false,
    };
    if let Err(error) = serde_json::to_writer(&mut counter, value) {
        return Err(if counter.exceeded {
            limit_error.into()
        } else {
            error.to_string()
        });
    }
    Ok(counter.size)
}

fn bounded_json_encode<T: serde::Serialize + ?Sized>(
    value: &T,
    limit: usize,
    limit_error: &'static str,
) -> Result<Vec<u8>, String> {
    let size = bounded_json_size(value, limit, limit_error)?;
    let mut encoded = vec![0_u8; size];
    let mut writer = ExactJsonBuffer {
        bytes: &mut encoded,
        written: 0,
    };
    serde_json::to_writer(&mut writer, value).map_err(|error| error.to_string())?;
    if writer.written != encoded.len() {
        return Err("recorder HTTP JSON serialization size changed".into());
    }
    Ok(encoded)
}

impl fmt::Debug for HttpRecorderClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpRecorderClient")
            .field("base_url", &self.base_url)
            .field("local_node_id", &self.local_node_id)
            .field("peer_token", &"[redacted]")
            .field("recovery_generation", &self.recovery_generation)
            .finish_non_exhaustive()
    }
}

impl HttpRecorderShared {
    fn new() -> Self {
        Self {
            client: OnceLock::new(),
            client_initialization: Mutex::new(()),
            available_workers: Mutex::new(HTTP_RECORDER_MAX_CONCURRENT_WORKERS),
            workers_available: Condvar::new(),
        }
    }

    fn acquire_worker(
        self: &Arc<Self>,
        context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<HttpRecorderWorkerPermit> {
        let mut available = self
            .available_workers
            .lock()
            .map_err(|_| rhiza_quepaxa::Error::Io("recorder HTTP worker slots poisoned".into()))?;
        loop {
            context.check()?;
            if *available > 0 {
                *available -= 1;
                return Ok(HttpRecorderWorkerPermit {
                    shared: Arc::clone(self),
                });
            }
            let Some(remaining) = context.remaining() else {
                return Err(rhiza_quepaxa::Error::RpcDeadlineExceeded);
            };
            let (next, _) = self
                .workers_available
                .wait_timeout(available, remaining.min(Duration::from_millis(10)))
                .map_err(|_| {
                    rhiza_quepaxa::Error::Io("recorder HTTP worker slots poisoned".into())
                })?;
            available = next;
        }
    }

    fn client(&self) -> rhiza_quepaxa::Result<reqwest::blocking::Client> {
        if let Some(client) = self.client.get() {
            return Ok(client.clone());
        }
        let _initializing = self.client_initialization.lock().map_err(|_| {
            rhiza_quepaxa::Error::Io("recorder HTTP client initialization poisoned".into())
        })?;
        if let Some(client) = self.client.get() {
            return Ok(client.clone());
        }
        let client = reqwest::blocking::Client::builder()
            .connect_timeout(RECORDER_HTTP_CONNECT_TIMEOUT)
            // Recorder membership binds one canonical endpoint. Redirects, implicit retries,
            // and environment proxies would make a final Connect error insufficient evidence
            // that no earlier endpoint received a mutating request.
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .build()
            .map_err(|error| rhiza_quepaxa::Error::Io(error.to_string()))?;
        self.client
            .set(client)
            .expect("HTTP client initialization lock serializes initialization");
        Ok(self.client.get().expect("HTTP client initialized").clone())
    }
}

impl Drop for HttpRecorderShared {
    fn drop(&mut self) {
        let client = self.client.take();
        let Some(client) = client else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            // reqwest's blocking client owns a Tokio runtime and panics if the
            // final handle is dropped from an async context. Its final drop is
            // short and joined, so it neither leaks nor outlives this client.
            if let Ok(join) = std::thread::Builder::new()
                .name("rhiza-recorder-http-drop".into())
                .spawn(move || drop(client))
            {
                let _ = join.join();
            }
        } else {
            drop(client);
        }
    }
}

impl Drop for HttpRecorderWorkerPermit {
    fn drop(&mut self) {
        if let Ok(mut available) = self.shared.available_workers.lock() {
            *available += 1;
            debug_assert!(*available <= HTTP_RECORDER_MAX_CONCURRENT_WORKERS);
            self.shared.workers_available.notify_one();
        }
    }
}

fn read_bounded_http_recorder_response<U>(response: reqwest::blocking::Response) -> HttpCall<U>
where
    RecorderWire<RecorderV2Result<U>>: recorder_decode::RecorderWireRoot,
{
    let status = response.status();
    if response
        .content_length()
        .is_some_and(|length| length > u64::try_from(MAX_HTTP_BODY_BYTES).unwrap_or(u64::MAX))
    {
        return HttpCall::Decode("recorder HTTP response body exceeds limit".into());
    }
    let mut bytes = Vec::new();
    let limit = u64::try_from(MAX_HTTP_BODY_BYTES.saturating_add(1)).unwrap_or(u64::MAX);
    if let Err(error) = response.take(limit).read_to_end(&mut bytes) {
        return HttpCall::Transport(error.to_string());
    }
    if bytes.len() > MAX_HTTP_BODY_BYTES {
        return HttpCall::Decode("recorder HTTP response body exceeds limit".into());
    }
    match recorder_decode::decode_json_exact_bounded::<RecorderWire<RecorderV2Result<U>>>(
        &bytes,
        recorder_decode::RecorderDecodeLimits::for_wire_bytes(MAX_HTTP_BODY_BYTES),
    ) {
        Ok(wire) => HttpCall::Response(status, wire),
        Err(error) => HttpCall::Decode(error.to_string()),
    }
}

impl HttpRecorderClient {
    pub fn new(
        base_url: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        Self::new_with_recovery_generation(base_url, local_node_id, peer_token, 1)
    }

    pub fn new_with_recovery_generation(
        base_url: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
        recovery_generation: u64,
    ) -> Result<Self, ConfigError> {
        validate_recovery_generation(recovery_generation)?;
        let peer = PeerConfig::new(local_node_id, base_url, peer_token)?;
        Ok(Self {
            base_url: peer.base_url,
            local_node_id: peer.node_id,
            peer_token: peer.token,
            recovery_generation,
            shared: Arc::new(HttpRecorderShared::new()),
        })
    }

    pub fn with_recovery_generation(
        mut self,
        recovery_generation: u64,
    ) -> Result<Self, ConfigError> {
        validate_recovery_generation(recovery_generation)?;
        self.recovery_generation = recovery_generation;
        Ok(self)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn post_v2<T, U>(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        path: &str,
        body: T,
        mutating: bool,
    ) -> rhiza_quepaxa::Result<U>
    where
        T: serde::Serialize + Send + 'static,
        U: serde::de::DeserializeOwned + Send + 'static,
        RecorderWire<RecorderV2Result<U>>: recorder_decode::RecorderWireRoot,
    {
        self.post_v2_with_timeout(context, path, body, HTTP_REQUEST_TIMEOUT, mutating)
    }

    fn post_v2_with_timeout<T, U>(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        path: &str,
        body: T,
        transport_cap: Duration,
        mutating: bool,
    ) -> rhiza_quepaxa::Result<U>
    where
        T: serde::Serialize + Send + 'static,
        U: serde::de::DeserializeOwned + Send + 'static,
        RecorderWire<RecorderV2Result<U>>: recorder_decode::RecorderWireRoot,
    {
        context.check()?;
        // Count a maximum-width deadline envelope before worker admission. The
        // final envelope is encoded after this preflight so its advertised
        // deadline cannot go stale while an oversized caller is rejected.
        bounded_json_size(
            &RecorderWire {
                version: RECORDER_WIRE_VERSION,
                remaining_deadline_ms: u32::MAX,
                body: &body,
            },
            MAX_HTTP_BODY_BYTES,
            RECORDER_HTTP_REQUEST_LIMIT_ERROR,
        )
        .map_err(rhiza_quepaxa::Error::Decode)?;
        context.check()?;
        let timeout = context
            .remaining()
            .ok_or(rhiza_quepaxa::Error::RpcDeadlineExceeded)?
            .min(transport_cap);
        if timeout.is_zero() {
            return Err(rhiza_quepaxa::Error::RpcDeadlineExceeded);
        }
        let remaining_deadline_ms = u32::try_from(timeout.as_millis())
            .unwrap_or(u32::MAX)
            .max(1);
        // Serialize before worker admission or networking. A caller body that
        // cannot fit the transport cap is a definite local Decode error even
        // for mutations, rather than an ambiguous remote outcome.
        let request = bounded_json_encode(
            &RecorderWire {
                version: RECORDER_WIRE_VERSION,
                remaining_deadline_ms,
                body,
            },
            MAX_HTTP_BODY_BYTES,
            RECORDER_HTTP_REQUEST_LIMIT_ERROR,
        )
        .map_err(rhiza_quepaxa::Error::Decode)?;
        // Admission is shared by all public clones. Waiting is deliberately
        // context-polled before any thread is created, so a cancelled or
        // expired caller is a definite local failure rather than an outcome
        // ambiguity.
        context.check()?;
        let permit = self.shared.acquire_worker(context)?;
        context.check()?;
        let (reply, receive) = std::sync::mpsc::sync_channel(1);
        let url = self.url(path);
        let shared = Arc::clone(&self.shared);
        let local_node_id = self.local_node_id.clone();
        let recovery_generation = self.recovery_generation;
        let peer_token = self.peer_token.clone();
        std::thread::Builder::new()
            .name("rhiza-recorder-http".into())
            .spawn(move || {
                let _permit = permit;
                let outcome = match shared.client() {
                    Ok(client) => match client
                        .post(url)
                        .timeout(timeout)
                        .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
                        .header(NODE_ID_HEADER, local_node_id)
                        .header(RECOVERY_GENERATION_HEADER, recovery_generation.to_string())
                        .bearer_auth(peer_token)
                        .header(CONTENT_TYPE, "application/json")
                        .body(request)
                        .send()
                    {
                        Ok(response) => read_bounded_http_recorder_response(response),
                        // reqwest reports DNS/TCP/TLS establishment failures as Connect. No
                        // HTTP request was available to the recorder, so a mutating call is a
                        // definite proposal failure rather than an unknown remote outcome.
                        Err(error) if error.is_connect() => HttpCall::NotSent(error.to_string()),
                        Err(error) => HttpCall::Transport(error.to_string()),
                    },
                    Err(error) => HttpCall::NotSent(error.to_string()),
                };
                let _ = reply.send(outcome);
            })
            .map_err(|error| {
                if mutating {
                    rhiza_quepaxa::Error::ProposeFailed
                } else {
                    rhiza_quepaxa::Error::Io(error.to_string())
                }
            })?;
        let outcome = loop {
            match context.check() {
                Ok(()) => {}
                Err(_) if mutating => return Err(rhiza_quepaxa::Error::UnknownOutcome),
                Err(error) => return Err(error),
            }
            let Some(remaining) = context.remaining() else {
                return Err(if mutating {
                    rhiza_quepaxa::Error::UnknownOutcome
                } else {
                    rhiza_quepaxa::Error::RpcDeadlineExceeded
                });
            };
            match receive.recv_timeout(remaining.min(Duration::from_millis(10))) {
                Ok(outcome) => break outcome,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(if mutating {
                        rhiza_quepaxa::Error::UnknownOutcome
                    } else {
                        rhiza_quepaxa::Error::Io("recorder HTTP worker closed".into())
                    });
                }
            }
        };
        let (status, wire) = match outcome {
            HttpCall::Response(status, wire) => (status, wire),
            HttpCall::NotSent(error) => {
                return Err(if mutating {
                    rhiza_quepaxa::Error::ProposeFailed
                } else {
                    rhiza_quepaxa::Error::Io(error)
                });
            }
            HttpCall::Transport(error) => {
                return Err(if mutating {
                    rhiza_quepaxa::Error::UnknownOutcome
                } else {
                    rhiza_quepaxa::Error::Io(error)
                });
            }
            HttpCall::Decode(error) => {
                return Err(if mutating {
                    rhiza_quepaxa::Error::UnknownOutcome
                } else {
                    rhiza_quepaxa::Error::Decode(error)
                });
            }
        };
        if wire.version != RECORDER_WIRE_VERSION {
            return Err(if mutating {
                rhiza_quepaxa::Error::UnknownOutcome
            } else {
                rhiza_quepaxa::Error::Decode("recorder wire version mismatch".into())
            });
        }
        let envelope_mismatch = || {
            if mutating {
                rhiza_quepaxa::Error::UnknownOutcome
            } else {
                rhiza_quepaxa::Error::Decode("recorder HTTP status/envelope mismatch".into())
            }
        };
        let result = match wire.body {
            RecorderV2Result::Ok(value) if status == StatusCode::OK => Ok(value),
            RecorderV2Result::Rejected(reason) if status == StatusCode::CONFLICT => {
                Err(rhiza_quepaxa::Error::Rejected(reason))
            }
            RecorderV2Result::Error(error)
                if matches!(
                    status,
                    StatusCode::BAD_REQUEST
                        | StatusCode::REQUEST_TIMEOUT
                        | StatusCode::TOO_MANY_REQUESTS
                        | StatusCode::SERVICE_UNAVAILABLE
                        | StatusCode::INTERNAL_SERVER_ERROR
                ) =>
            {
                Err(recorder_error_from_wire(error))
            }
            _ => Err(envelope_mismatch()),
        };
        if mutating {
            result.map_err(preserve_mutation_outcome)
        } else {
            result
        }
    }
}

impl RecorderRpc for HttpRecorderClient {
    fn recorder_id(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        self.post_v2(context, RECORDER_IDENTITY_PATH, (), false)
    }

    fn store_command_for(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        self.post_v2(
            context,
            RECORDER_STORE_COMMAND_PATH,
            StoreCommandV2 {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
                command,
            },
            true,
        )
    }

    fn fetch_command_for(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        self.post_v2(
            context,
            RECORDER_FETCH_COMMAND_PATH,
            FetchCommandV2 {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
            },
            false,
        )
    }

    fn stage_effect_bundle_chunk(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        binding: EffectBundleBinding,
        manifest_command: StoredCommand,
        ordinal: u16,
        chunk: Vec<u8>,
    ) -> rhiza_quepaxa::Result<()> {
        self.post_v2(
            context,
            RECORDER_STAGE_EFFECT_CHUNK_PATH,
            StageEffectChunkV3 {
                binding,
                manifest_command,
                ordinal,
                chunk,
            },
            true,
        )
    }

    fn finalize_staged_effect_bundle(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        binding: EffectBundleBinding,
        manifest_command: StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        self.post_v2(
            context,
            RECORDER_FINALIZE_EFFECT_BUNDLE_PATH,
            FinalizeEffectBundleV3 {
                binding,
                manifest_command,
            },
            true,
        )
    }

    fn fetch_effect_bundle_manifest(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        binding: EffectBundleBinding,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        self.post_v2(
            context,
            RECORDER_FETCH_EFFECT_MANIFEST_PATH,
            FetchEffectManifestV3 { binding },
            false,
        )
    }

    fn fetch_effect_bundle_chunk(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        binding: EffectBundleBinding,
        ordinal: u16,
    ) -> rhiza_quepaxa::Result<Option<Vec<u8>>> {
        self.post_v2(
            context,
            RECORDER_FETCH_EFFECT_CHUNK_PATH,
            FetchEffectChunkV3 { binding, ordinal },
            false,
        )
    }

    fn record(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        request: RecordRequest,
    ) -> rhiza_quepaxa::Result<RecordSummary> {
        self.post_v2_with_timeout(
            context,
            RECORDER_RECORD_PATH,
            request,
            QUORUM_RECORD_REQUEST_TIMEOUT,
            true,
        )
    }

    fn install_decision_proof(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        proof: DecisionProof,
        membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        self.post_v2(
            context,
            RECORDER_INSTALL_PROOF_PATH,
            InstallProofV2 {
                proof,
                members: membership.members().to_vec(),
            },
            true,
        )
    }

    fn inspect_decision_proof(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        self.post_v2(
            context,
            RECORDER_INSPECT_PROOF_PATH,
            InspectProofV2 { slot },
            false,
        )
    }

    fn inspect_record_summary(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        self.post_v2(
            context,
            RECORDER_INSPECT_RECORD_PATH,
            InspectProofV2 { slot },
            false,
        )
    }

    fn supports_context_read_fence(&self) -> bool {
        true
    }

    fn observe_read_fence(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        request: ReadFenceRequest,
    ) -> rhiza_quepaxa::Result<ReadFenceObservation> {
        self.post_v2_with_timeout(
            context,
            RECORDER_READ_FENCE_PATH,
            request,
            READ_FENCE_REQUEST_TIMEOUT,
            false,
        )
    }
}

#[derive(Clone, Debug)]
pub struct HttpLogPeer {
    base_url: String,
    local_node_id: String,
    peer_token: String,
    recovery_generation: u64,
    client: Arc<std::sync::OnceLock<reqwest::blocking::Client>>,
}

impl HttpLogPeer {
    pub fn new(
        base_url: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        Self::new_with_recovery_generation(base_url, local_node_id, peer_token, 1)
    }

    pub fn new_with_recovery_generation(
        base_url: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
        recovery_generation: u64,
    ) -> Result<Self, ConfigError> {
        validate_recovery_generation(recovery_generation)?;
        let peer = PeerConfig::new(local_node_id, base_url, peer_token)?;
        Ok(Self {
            base_url: peer.base_url,
            local_node_id: peer.node_id,
            peer_token: peer.token,
            recovery_generation,
            client: Arc::new(std::sync::OnceLock::new()),
        })
    }

    pub fn with_recovery_generation(
        mut self,
        recovery_generation: u64,
    ) -> Result<Self, ConfigError> {
        validate_recovery_generation(recovery_generation)?;
        self.recovery_generation = recovery_generation;
        Ok(self)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn client(&self) -> Result<&reqwest::blocking::Client, FetchLogError> {
        if self.client.get().is_none() {
            let client = reqwest::blocking::Client::builder()
                .connect_timeout(HTTP_CONNECT_TIMEOUT)
                .timeout(HTTP_REQUEST_TIMEOUT)
                .build()
                .map_err(|error| FetchLogError::Transport {
                    message: error.to_string(),
                })?;
            let _ = self.client.set(client);
        }
        self.client.get().ok_or_else(|| FetchLogError::Transport {
            message: "HTTP client initialization failed".into(),
        })
    }
}

impl LogPeer for HttpLogPeer {
    fn fetch_log(&self, request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError> {
        let response = self
            .client()?
            .post(self.url(LOG_FETCH_PATH))
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .header(NODE_ID_HEADER, &self.local_node_id)
            .header(
                RECOVERY_GENERATION_HEADER,
                self.recovery_generation.to_string(),
            )
            .bearer_auth(&self.peer_token)
            .json(&request)
            .send()
            .map_err(|err| FetchLogError::Transport {
                message: err.to_string(),
            })?;
        let status = response.status();
        if !status.is_success() {
            return match response.json::<FetchLogHttpResponse>() {
                Ok(FetchLogHttpResponse::Failed(error)) => Err(error),
                Ok(FetchLogHttpResponse::Fetched(_)) | Err(_) => Err(FetchLogError::Transport {
                    message: format!("log rpc returned HTTP {status}"),
                }),
            };
        }
        match response
            .json::<FetchLogHttpResponse>()
            .map_err(|err| FetchLogError::Decode {
                message: err.to_string(),
            })? {
            FetchLogHttpResponse::Fetched(response) => Ok(response),
            FetchLogHttpResponse::Failed(error) => Err(error),
        }
    }
}

#[derive(Clone)]
struct RecorderRouteState<R> {
    recorder: R,
    peers: Vec<PeerConfig>,
    decode_slots: Arc<tokio::sync::Semaphore>,
}

#[cfg(test)]
#[derive(Clone)]
struct TestHttpDecodeHook {
    request_ids: Arc<HashSet<usize>>,
    entered: tokio::sync::mpsc::UnboundedSender<usize>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
static TEST_HTTP_DECODE_HOOK: OnceLock<std::sync::Mutex<Option<Arc<TestHttpDecodeHook>>>> =
    OnceLock::new();

#[cfg(test)]
static TEST_HTTP_DECODE_HOOK_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

#[cfg(test)]
fn test_http_decode_hook_slot() -> &'static std::sync::Mutex<Option<Arc<TestHttpDecodeHook>>> {
    TEST_HTTP_DECODE_HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn test_http_decode_hook_lock() -> &'static tokio::sync::Mutex<()> {
    TEST_HTTP_DECODE_HOOK_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[cfg(test)]
struct TestHttpDecodeHookGuard;

#[cfg(test)]
impl Drop for TestHttpDecodeHookGuard {
    fn drop(&mut self) {
        *test_http_decode_hook_slot().lock().unwrap() = None;
    }
}

#[cfg(test)]
fn install_test_http_decode_hook(hook: TestHttpDecodeHook) -> TestHttpDecodeHookGuard {
    let mut slot = test_http_decode_hook_slot().lock().unwrap();
    assert!(
        slot.is_none(),
        "HTTP recorder decode hook already installed"
    );
    *slot = Some(Arc::new(hook));
    TestHttpDecodeHookGuard
}

#[cfg(test)]
async fn test_http_decode_hook_after_permit(headers: &HeaderMap) {
    let Some(request_id) = header_text(headers, "x-rhiza-test-decode-id")
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return;
    };
    let Some(hook) = test_http_decode_hook_slot().lock().unwrap().clone() else {
        return;
    };
    if !hook.request_ids.contains(&request_id) {
        return;
    }
    let released = hook.release.notified();
    tokio::pin!(released);
    released.as_mut().enable();
    let _ = hook.entered.send(request_id);
    released.await;
}

#[derive(Clone)]
struct AuthenticatedPeer(String);

#[derive(Clone)]
struct LogRouteState<P> {
    peer: P,
}

#[derive(Clone)]
struct NodeRouteState {
    runtime: Arc<NodeRuntime>,
    coordinator: Option<Arc<CheckpointCoordinator>>,
    write_operations: Arc<tokio::sync::Mutex<HashMap<String, WriteOperation>>>,
    writer: tokio::sync::mpsc::Sender<QueuedWrite>,
}

#[derive(Clone)]
struct WriteOperation {
    payload: Vec<u8>,
    result: tokio::sync::watch::Receiver<Option<WriteOperationResult>>,
}

#[derive(Clone)]
enum WriteOperationResult {
    Runtime(Result<ClientWriteResponse, NodeError>),
    DurabilityUnavailable,
}

#[derive(Clone)]
enum ClientWriteResponse {
    #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
    Unavailable,
    #[cfg(feature = "sql")]
    KeyValue(WriteResponse),
    #[cfg(feature = "sql")]
    Sql(SqlExecuteResponse),
    #[cfg(feature = "graph")]
    Graph(GraphMutationOutcome),
    #[cfg(feature = "kv")]
    Kv(KvMutationOutcome),
}

struct QueuedWrite {
    request_id: String,
    payload: Vec<u8>,
    operation: QueuedOperation,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    sender: tokio::sync::watch::Sender<Option<WriteOperationResult>>,
}

enum QueuedOperation {
    #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
    Unavailable,
    #[cfg(feature = "sql")]
    KeyValue { key: String, value: String },
    #[cfg(feature = "sql")]
    Sql(SqlCommand),
    #[cfg(feature = "graph")]
    Graph(GraphCommandV1),
    #[cfg(feature = "kv")]
    Kv(KvCommandV1),
}

struct RuntimeBatchMember {
    #[cfg(feature = "sql")]
    request_id: String,
    payload: Vec<u8>,
    operation: QueuedOperation,
}

#[cfg(feature = "sql")]
type SqlGroupCommitResult = Result<Vec<Result<ClientWriteResponse, NodeError>>, NodeError>;

#[cfg(feature = "sql")]
struct SqlGroupCommitJob {
    member_count: usize,
    encoded_bytes: usize,
    members: Mutex<Option<Vec<RuntimeBatchMember>>>,
    result: Mutex<Option<SqlGroupCommitResult>>,
    changed: Condvar,
}

#[cfg(feature = "sql")]
impl SqlGroupCommitJob {
    fn new(members: Vec<RuntimeBatchMember>) -> Self {
        Self {
            member_count: members.len(),
            encoded_bytes: members.iter().fold(0_usize, |bytes, member| {
                bytes.saturating_add(member.payload.len())
            }),
            members: Mutex::new(Some(members)),
            result: Mutex::new(None),
            changed: Condvar::new(),
        }
    }

    fn take_members(&self) -> Result<Vec<RuntimeBatchMember>, NodeError> {
        self.members
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                NodeError::Invariant("SQL group commit job members were already consumed".into())
            })
    }

    fn publish(&self, result: SqlGroupCommitResult) {
        let mut slot = self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(result);
            self.changed.notify_all();
        }
    }

    fn wait(&self, cancelled: &AtomicBool) -> SqlGroupCommitResult {
        let mut result = self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if let Some(result) = result.take() {
                return result;
            }
            if cancelled.load(Ordering::Acquire) {
                return Err(NodeError::Unavailable(
                    "SQL group commit cancelled during shutdown".into(),
                ));
            }
            result = self
                .changed
                .wait(result)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

#[cfg(feature = "sql")]
struct SqlGroupCommitQueue {
    capacity: usize,
    state: Mutex<SqlGroupCommitQueueState>,
    changed: Condvar,
}

#[cfg(feature = "sql")]
#[derive(Default)]
struct SqlGroupCommitQueueState {
    pending: VecDeque<Arc<SqlGroupCommitJob>>,
    pending_encoded_bytes: usize,
    leader_active: bool,
}

#[cfg(feature = "sql")]
impl SqlGroupCommitQueue {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(SqlGroupCommitQueueState::default()),
            changed: Condvar::new(),
        }
    }

    fn enqueue(
        &self,
        members: Vec<RuntimeBatchMember>,
        cancelled: &AtomicBool,
    ) -> Result<(Arc<SqlGroupCommitJob>, bool), NodeError> {
        if cancelled.load(Ordering::Acquire) {
            return Err(NodeError::Unavailable(
                "SQL group commit is unavailable during shutdown".into(),
            ));
        }
        let job = Arc::new(SqlGroupCommitJob::new(members));
        if job.encoded_bytes > MAX_COMMAND_BYTES {
            return Err(NodeError::ResourceExhausted(format!(
                "SQL group commit call exceeds {MAX_COMMAND_BYTES} encoded bytes"
            )));
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.pending.len() >= self.capacity {
            return Err(NodeError::ResourceExhausted(format!(
                "SQL group commit queue is full (capacity {})",
                self.capacity
            )));
        }
        let pending_encoded_bytes = state
            .pending_encoded_bytes
            .saturating_add(job.encoded_bytes);
        if pending_encoded_bytes > MAX_SQL_GROUP_COMMIT_PENDING_BYTES {
            return Err(NodeError::ResourceExhausted(format!(
                "SQL group commit queue exceeds {MAX_SQL_GROUP_COMMIT_PENDING_BYTES} pending encoded bytes"
            )));
        }
        state.pending_encoded_bytes = pending_encoded_bytes;
        state.pending.push_back(Arc::clone(&job));
        self.changed.notify_all();
        let leader = !state.leader_active;
        if leader {
            state.leader_active = true;
        }
        Ok((job, leader))
    }

    fn drain_next_group(&self) -> Option<Vec<Arc<SqlGroupCommitJob>>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.pending.is_empty() {
            state.leader_active = false;
            return None;
        }
        let mut member_count = 0_usize;
        let mut encoded_bytes = 0_usize;
        let mut jobs = Vec::new();
        while let Some(job) = state.pending.front() {
            let next_count = member_count.saturating_add(job.member_count);
            let next_encoded_bytes = encoded_bytes.saturating_add(job.encoded_bytes);
            if !jobs.is_empty()
                && (next_count > MAX_SQL_WRITE_BATCH_MEMBERS
                    || next_encoded_bytes > MAX_SQL_GROUP_COMMIT_ACTIVE_BYTES)
            {
                break;
            }
            let job = state.pending.pop_front().expect("front job exists");
            member_count = next_count;
            encoded_bytes = next_encoded_bytes;
            state.pending_encoded_bytes = state
                .pending_encoded_bytes
                .checked_sub(job.encoded_bytes)
                .expect("queued SQL byte reservation covers every pending job");
            jobs.push(job);
        }
        Some(jobs)
    }

    fn collect_until_full_or_timeout(&self, timeout: Duration) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.pending.is_empty() {
            state.leader_active = false;
            return false;
        }
        let hard_deadline = Instant::now() + timeout.saturating_mul(4);
        loop {
            let member_count = state
                .pending
                .iter()
                .fold(0_usize, |count, job| count.saturating_add(job.member_count));
            if member_count >= MAX_SQL_WRITE_BATCH_MEMBERS
                || state.pending_encoded_bytes >= MAX_SQL_GROUP_COMMIT_ACTIVE_BYTES
            {
                return true;
            }
            let remaining = hard_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return true;
            }
            let observed_calls = state.pending.len();
            let (next, quiet) = self
                .changed
                .wait_timeout_while(state, timeout.min(remaining), |state| {
                    state.pending.len() == observed_calls
                        && state
                            .pending
                            .iter()
                            .fold(0_usize, |count, job| count.saturating_add(job.member_count))
                            < MAX_SQL_WRITE_BATCH_MEMBERS
                        && state.pending_encoded_bytes < MAX_SQL_GROUP_COMMIT_ACTIVE_BYTES
                })
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next;
            if quiet.timed_out() {
                return true;
            }
        }
    }

    fn fail_pending(&self, error: NodeError) {
        let jobs = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.leader_active = false;
            state.pending_encoded_bytes = 0;
            state.pending.drain(..).collect::<Vec<_>>()
        };
        for job in jobs {
            job.publish(Err(error.clone()));
        }
    }

    #[cfg(test)]
    fn wait_for_pending_calls(&self, expected: usize, timeout: Duration) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (state, timed_out) = self
            .changed
            .wait_timeout_while(state, timeout, |state| state.pending.len() != expected)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            !timed_out.timed_out(),
            "expected {expected} pending SQL group commit calls, got {}",
            state.pending.len()
        );
    }
}

#[cfg(feature = "kv")]
type KvGroupCommitResult = Result<Vec<Result<ClientWriteResponse, NodeError>>, NodeError>;

#[cfg(feature = "kv")]
struct KvGroupCommitJob {
    member_count: usize,
    encoded_bytes: usize,
    members: Mutex<Option<Vec<RuntimeBatchMember>>>,
    result: Mutex<Option<KvGroupCommitResult>>,
    changed: Condvar,
}

#[cfg(feature = "kv")]
impl KvGroupCommitJob {
    fn new(members: Vec<RuntimeBatchMember>) -> Self {
        Self {
            member_count: members.len(),
            encoded_bytes: members.iter().fold(0_usize, |bytes, member| {
                bytes.saturating_add(member.payload.len())
            }),
            members: Mutex::new(Some(members)),
            result: Mutex::new(None),
            changed: Condvar::new(),
        }
    }

    fn take_members(&self) -> Result<Vec<RuntimeBatchMember>, NodeError> {
        self.members
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                NodeError::Invariant("KV group commit job members were already consumed".into())
            })
    }

    fn publish(&self, result: KvGroupCommitResult) {
        let mut slot = self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(result);
            self.changed.notify_all();
        }
    }

    fn wait(&self, cancelled: &AtomicBool) -> KvGroupCommitResult {
        let mut result = self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if let Some(result) = result.take() {
                return result;
            }
            if cancelled.load(Ordering::Acquire) {
                return Err(NodeError::Unavailable(
                    "KV group commit cancelled during shutdown".into(),
                ));
            }
            result = self
                .changed
                .wait(result)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

#[cfg(feature = "kv")]
struct KvGroupCommitQueue {
    state: Mutex<KvGroupCommitQueueState>,
    changed: Condvar,
}

#[cfg(feature = "kv")]
#[derive(Default)]
struct KvGroupCommitQueueState {
    pending: VecDeque<Arc<KvGroupCommitJob>>,
    pending_encoded_bytes: usize,
    leader_active: bool,
}

#[cfg(feature = "kv")]
impl KvGroupCommitQueue {
    fn new() -> Self {
        Self {
            state: Mutex::new(KvGroupCommitQueueState::default()),
            changed: Condvar::new(),
        }
    }

    fn enqueue(
        &self,
        members: Vec<RuntimeBatchMember>,
        cancelled: &AtomicBool,
    ) -> Result<(Arc<KvGroupCommitJob>, bool), NodeError> {
        if cancelled.load(Ordering::Acquire) {
            return Err(NodeError::Unavailable(
                "KV group commit is unavailable during shutdown".into(),
            ));
        }
        let job = Arc::new(KvGroupCommitJob::new(members));
        if job.member_count == 0 || job.member_count > MAX_KV_BATCH_MEMBERS {
            return Err(NodeError::InvalidRequest(format!(
                "KV group commit call must contain 1..={MAX_KV_BATCH_MEMBERS} members"
            )));
        }
        if job.encoded_bytes > MAX_KV_GROUP_COMMIT_PENDING_BYTES {
            return Err(NodeError::ResourceExhausted(format!(
                "KV group commit call exceeds {MAX_KV_GROUP_COMMIT_PENDING_BYTES} encoded bytes"
            )));
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.pending.len() >= KV_GROUP_COMMIT_QUEUE_CAPACITY {
            return Err(NodeError::ResourceExhausted(format!(
                "KV group commit queue is full (capacity {KV_GROUP_COMMIT_QUEUE_CAPACITY})"
            )));
        }
        let pending_encoded_bytes = state
            .pending_encoded_bytes
            .saturating_add(job.encoded_bytes);
        if pending_encoded_bytes > MAX_KV_GROUP_COMMIT_PENDING_BYTES {
            return Err(NodeError::ResourceExhausted(format!(
                "KV group commit queue exceeds {MAX_KV_GROUP_COMMIT_PENDING_BYTES} pending encoded bytes"
            )));
        }
        state.pending_encoded_bytes = pending_encoded_bytes;
        state.pending.push_back(Arc::clone(&job));
        self.changed.notify_all();
        let leader = !state.leader_active;
        if leader {
            state.leader_active = true;
        }
        Ok((job, leader))
    }

    fn drain_next_group(&self) -> Option<Vec<Arc<KvGroupCommitJob>>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.pending.is_empty() {
            state.leader_active = false;
            return None;
        }
        let mut member_count = 0_usize;
        let mut encoded_bytes = 0_usize;
        let mut jobs = Vec::new();
        while let Some(job) = state.pending.front() {
            let next_count = member_count.saturating_add(job.member_count);
            let next_encoded_bytes = encoded_bytes.saturating_add(job.encoded_bytes);
            if !jobs.is_empty()
                && (next_count > MAX_KV_GROUP_COMMIT_MEMBERS
                    || next_encoded_bytes > MAX_KV_GROUP_COMMIT_PENDING_BYTES)
            {
                break;
            }
            let job = state.pending.pop_front().expect("front job exists");
            member_count = next_count;
            encoded_bytes = next_encoded_bytes;
            state.pending_encoded_bytes = state
                .pending_encoded_bytes
                .checked_sub(job.encoded_bytes)
                .expect("queued KV byte reservation covers every pending job");
            jobs.push(job);
        }
        Some(jobs)
    }

    fn collect_until_full_or_timeout(&self, timeout: Duration) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.pending.is_empty() {
            state.leader_active = false;
            return false;
        }
        let hard_deadline = Instant::now() + timeout.saturating_mul(4);
        loop {
            let member_count = state
                .pending
                .iter()
                .fold(0_usize, |count, job| count.saturating_add(job.member_count));
            if member_count >= MAX_KV_GROUP_COMMIT_MEMBERS
                || state.pending_encoded_bytes >= MAX_KV_GROUP_COMMIT_PENDING_BYTES
            {
                return true;
            }
            let remaining = hard_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return true;
            }
            let observed_calls = state.pending.len();
            let (next, quiet) = self
                .changed
                .wait_timeout_while(state, timeout.min(remaining), |state| {
                    state.pending.len() == observed_calls
                        && state
                            .pending
                            .iter()
                            .fold(0_usize, |count, job| count.saturating_add(job.member_count))
                            < MAX_KV_GROUP_COMMIT_MEMBERS
                        && state.pending_encoded_bytes < MAX_KV_GROUP_COMMIT_PENDING_BYTES
                })
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next;
            if quiet.timed_out() {
                return true;
            }
        }
    }

    fn fail_pending(&self, error: NodeError) {
        let jobs = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.leader_active = false;
            state.pending_encoded_bytes = 0;
            state.pending.drain(..).collect::<Vec<_>>()
        };
        for job in jobs {
            job.publish(Err(error.clone()));
        }
    }

    #[cfg(test)]
    fn wait_for_pending_calls(&self, expected: usize, timeout: Duration) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (state, timed_out) = self
            .changed
            .wait_timeout_while(state, timeout, |state| state.pending.len() != expected)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            !timed_out.timed_out(),
            "expected {expected} pending KV group commit calls, got {}",
            state.pending.len()
        );
    }
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn classify_pending_request(
    canonical_by_request: &mut HashMap<String, usize>,
    members: &[RuntimeBatchMember],
    index: usize,
    request_id: &str,
) -> Result<Option<usize>, NodeError> {
    let Some(&canonical) = canonical_by_request.get(request_id) else {
        canonical_by_request.insert(request_id.to_owned(), index);
        return Ok(None);
    };
    if members[canonical].payload == members[index].payload {
        Ok(Some(canonical))
    } else {
        Err(NodeError::InvalidRequest(format!(
            "request id {request_id:?} was reused with another command in the same writer batch"
        )))
    }
}

impl ClientWriteResponse {
    fn applied_index(&self) -> LogIndex {
        match self {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Self::KeyValue(response) => response.applied_index,
            #[cfg(feature = "sql")]
            Self::Sql(response) => response.applied_index,
            #[cfg(feature = "graph")]
            Self::Graph(response) => response.applied_index(),
            #[cfg(feature = "kv")]
            Self::Kv(response) => response.applied_index(),
        }
    }
}

#[cfg(feature = "sql")]
#[derive(Clone)]
pub struct NodeService {
    runtime: Arc<NodeRuntime>,
    coordinator: Option<Arc<CheckpointCoordinator>>,
    #[cfg(feature = "sql")]
    sql_reads_in_flight: Arc<AtomicUsize>,
}

#[cfg(feature = "sql")]
struct SqlReadActivity {
    active: Arc<AtomicUsize>,
}

#[cfg(feature = "sql")]
impl SqlReadActivity {
    fn enter(active: &Arc<AtomicUsize>) -> (Self, bool) {
        let previous = active.fetch_add(1, Ordering::Relaxed);
        debug_assert_ne!(previous, usize::MAX);
        (
            Self {
                active: active.clone(),
            },
            previous == 0,
        )
    }
}

#[cfg(feature = "sql")]
impl Drop for SqlReadActivity {
    fn drop(&mut self) {
        let previous = self.active.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0);
    }
}

#[cfg(feature = "sql")]
impl NodeService {
    pub fn new(runtime: Arc<NodeRuntime>, coordinator: Option<Arc<CheckpointCoordinator>>) -> Self {
        Self {
            runtime,
            coordinator,
            #[cfg(feature = "sql")]
            sql_reads_in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[cfg(feature = "sql")]
    pub async fn put(
        &self,
        request_id: &str,
        key: &str,
        value: &str,
    ) -> Result<WriteResponse, NodeError> {
        self.write(WriteRequest {
            request_id: request_id.into(),
            key: key.into(),
            value: value.into(),
        })
        .await
    }

    #[cfg(feature = "sql")]
    pub async fn write(&self, request: WriteRequest) -> Result<WriteResponse, NodeError> {
        self.write_allowed()?;
        let runtime = self.runtime.clone();
        let response = tokio::task::spawn_blocking(move || {
            runtime.write(&request.request_id, &request.key, &request.value)
        })
        .await
        .map_err(node_service_task_error)??;
        self.confirm_committed(response.applied_index).await?;
        Ok(response)
    }

    #[cfg(feature = "sql")]
    pub async fn execute_sql(&self, command: SqlCommand) -> Result<SqlExecuteResponse, NodeError> {
        self.write_allowed()?;
        let runtime = self.runtime.clone();
        let response =
            tokio::task::spawn_blocking(move || runtime.execute_sql_with_results(command))
                .await
                .map_err(node_service_task_error)??;
        self.confirm_committed(response.applied_index).await?;
        Ok(response)
    }

    #[cfg(feature = "sql")]
    pub async fn read(
        &self,
        key: &str,
        consistency: ReadConsistency,
    ) -> Result<ReadResponse, NodeError> {
        let runtime = self.runtime.clone();
        let key = key.to_owned();
        self.run_sql_read_operation(consistency, move || runtime.read(&key, consistency))
            .await
            .map_err(node_service_task_error)?
    }

    #[cfg(feature = "sql")]
    pub async fn query(
        &self,
        statement: SqlStatement,
        consistency: ReadConsistency,
        max_rows: u32,
    ) -> Result<SqlQueryResponse, NodeError> {
        let runtime = self.runtime.clone();
        self.run_sql_read_operation(consistency, move || {
            runtime.query_sql(&statement, consistency, max_rows)
        })
        .await
        .map_err(node_service_task_error)?
    }

    #[cfg(feature = "sql")]
    async fn run_sql_read_operation<F, T>(
        &self,
        consistency: ReadConsistency,
        operation: F,
    ) -> Result<T, tokio::task::JoinError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        if consistency == ReadConsistency::ReadBarrier {
            return run_read_operation(consistency, operation).await;
        }
        let (activity, sole_read) = SqlReadActivity::enter(&self.sql_reads_in_flight);
        let operation = move || {
            let _activity = activity;
            operation()
        };
        if sole_read {
            run_read_operation(consistency, operation).await
        } else {
            tokio::task::spawn_blocking(operation).await
        }
    }

    #[cfg(feature = "sql")]
    fn write_allowed(&self) -> Result<(), NodeError> {
        self.coordinator
            .as_ref()
            .map_or(Ok(()), |coordinator| coordinator.write_allowed())
            .map_err(|error| NodeError::Unavailable(error.to_string()))
    }

    #[cfg(feature = "sql")]
    async fn confirm_committed(&self, index: LogIndex) -> Result<(), NodeError> {
        confirm_write_durability(self.runtime.as_ref(), self.coordinator.as_deref(), index)
            .await
            .map_err(|error| NodeError::Unavailable(error.to_string()))
    }
}

#[cfg(feature = "sql")]
fn node_service_task_error(error: tokio::task::JoinError) -> NodeError {
    NodeError::Fatal(format!("node service task failed: {error}"))
}

#[doc(hidden)]
pub async fn run_read_operation<F, T>(
    consistency: ReadConsistency,
    operation: F,
) -> Result<T, tokio::task::JoinError>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    if consistency != ReadConsistency::ReadBarrier
        && matches!(
            tokio::runtime::Handle::current().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        )
    {
        Ok(tokio::task::block_in_place(operation))
    } else {
        tokio::task::spawn_blocking(operation).await
    }
}

#[derive(Clone)]
struct PeerGateState {
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    protocol_version: &'static str,
    slots: Arc<tokio::sync::Semaphore>,
}

struct PeerPermitBody {
    inner: Body,
    pending_data: Option<axum::body::Bytes>,
    permit: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
}

impl HttpBody for PeerPermitBody {
    type Data = axum::body::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        loop {
            if let Some(data) = self.pending_data.as_mut() {
                let chunk = data.split_to(data.len().min(PEER_RESPONSE_CHUNK_BYTES));
                if data.is_empty() {
                    self.pending_data = None;
                }
                return Poll::Ready(Some(Ok(Frame::data(chunk))));
            }
            match Pin::new(&mut self.inner).poll_frame(cx) {
                Poll::Ready(None) => {
                    self.permit.take();
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(error))) => {
                    self.permit.take();
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) if data.is_empty() => continue,
                    Ok(data) => self.pending_data = Some(data),
                    Err(frame) => return Poll::Ready(Some(Ok(frame))),
                },
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.pending_data.is_none() && self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

fn retain_peer_permit_until_response_body_complete(
    response: Response,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
) -> Response {
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(PeerPermitBody {
            inner: body,
            pending_data: None,
            permit: Some(permit),
        }),
    )
}

#[derive(Clone)]
struct ClientGateState {
    runtime: Arc<NodeRuntime>,
    slots: Arc<tokio::sync::Semaphore>,
    coordinator: Option<Arc<CheckpointCoordinator>>,
}

#[derive(Clone)]
struct RuntimeLogPeer {
    runtime: Arc<NodeRuntime>,
}

impl LogPeer for RuntimeLogPeer {
    fn fetch_log(&self, request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError> {
        self.runtime.fetch_log(request)
    }
}

fn fetch_runtime_log(
    runtime: &NodeRuntime,
    request: FetchLogRequest,
) -> Result<FetchLogResponse, FetchLogError> {
    if request.from_index == 0 || request.max_entries > MAX_FETCH_ENTRIES {
        return Err(FetchLogError::InvalidRequest {
            message: "invalid fetch bounds".into(),
        });
    }
    let state = runtime
        .log_store
        .logical_state()
        .map_err(|error| FetchLogError::Transport {
            message: error.to_string(),
        })?;
    if let Some(anchor) = state.anchor {
        if request.from_index <= anchor.compacted().index() {
            return Err(FetchLogError::SnapshotRequired {
                anchor: Box::new(anchor),
            });
        }
    }
    let last_index = state.tip.map_or(0, |tip| tip.index());
    if request.max_entries == 0 || request.from_index > last_index {
        return Ok(FetchLogResponse {
            entries: Vec::new(),
            last_index,
        });
    }
    let end = request
        .from_index
        .saturating_add(u64::from(request.max_entries) - 1)
        .min(last_index);
    let range = IndexRange::new(request.from_index, end).map_err(|error| {
        FetchLogError::InvalidRequest {
            message: error.to_string(),
        }
    })?;
    let entries =
        runtime
            .log_store
            .read_range(range)
            .map_err(|error| FetchLogError::Transport {
                message: error.to_string(),
            })?;
    Ok(FetchLogResponse {
        entries,
        last_index,
    })
}

pub fn recorder_router<R, P>(recorder: R, peers: P) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    P: Into<Vec<PeerConfig>>,
{
    recorder_router_for_generation(recorder, peers, 1)
}

pub fn recorder_router_for_generation<R, P>(
    recorder: R,
    peers: P,
    recovery_generation: u64,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    P: Into<Vec<PeerConfig>>,
{
    recorder_routes(
        recorder,
        peers.into(),
        recovery_generation,
        Arc::new(tokio::sync::Semaphore::new(DEFAULT_PEER_CONCURRENCY)),
    )
    .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
}

#[cfg(test)]
fn recorder_router_with_test_peer_concurrency<R>(
    recorder: R,
    peers: Vec<PeerConfig>,
    peer_concurrency: usize,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    recorder_routes(
        recorder,
        peers,
        1,
        Arc::new(tokio::sync::Semaphore::new(peer_concurrency)),
    )
    .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
}

pub fn log_peer_router<P, C>(peer: P, peers: C) -> Router
where
    P: LogPeer + Clone + Send + Sync + 'static,
    C: Into<Vec<PeerConfig>>,
{
    log_peer_router_for_generation(peer, peers, 1)
}

pub fn log_peer_router_for_generation<P, C>(peer: P, peers: C, recovery_generation: u64) -> Router
where
    P: LogPeer + Clone + Send + Sync + 'static,
    C: Into<Vec<PeerConfig>>,
{
    log_routes(
        peer,
        peers.into(),
        recovery_generation,
        Arc::new(tokio::sync::Semaphore::new(DEFAULT_PEER_CONCURRENCY)),
    )
    .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
}

pub fn node_rpc_router<R, P, C>(recorder: R, peer: P, peers: C) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    P: LogPeer + Clone + Send + Sync + 'static,
    C: Into<Vec<PeerConfig>>,
{
    node_rpc_router_for_generation(recorder, peer, peers, 1)
}

pub fn node_rpc_router_for_generation<R, P, C>(
    recorder: R,
    peer: P,
    peers: C,
    recovery_generation: u64,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    P: LogPeer + Clone + Send + Sync + 'static,
    C: Into<Vec<PeerConfig>>,
{
    node_rpc_router_with_limits_for_generation(
        recorder,
        peer,
        peers,
        DEFAULT_PEER_CONCURRENCY,
        DEFAULT_PEER_CONCURRENCY,
        recovery_generation,
    )
}

pub fn node_rpc_router_with_limits<R, P, C>(
    recorder: R,
    peer: P,
    peers: C,
    recorder_concurrency: usize,
    log_concurrency: usize,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    P: LogPeer + Clone + Send + Sync + 'static,
    C: Into<Vec<PeerConfig>>,
{
    node_rpc_router_with_limits_for_generation(
        recorder,
        peer,
        peers,
        recorder_concurrency,
        log_concurrency,
        1,
    )
}

pub fn node_rpc_router_with_limits_for_generation<R, P, C>(
    recorder: R,
    peer: P,
    peers: C,
    recorder_concurrency: usize,
    log_concurrency: usize,
    recovery_generation: u64,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    P: LogPeer + Clone + Send + Sync + 'static,
    C: Into<Vec<PeerConfig>>,
{
    let peers = peers.into();
    let recorder_slots = Arc::new(tokio::sync::Semaphore::new(recorder_concurrency));
    let log_slots = Arc::new(tokio::sync::Semaphore::new(log_concurrency));
    recorder_routes(recorder, peers.clone(), recovery_generation, recorder_slots)
        .merge(log_routes(peer, peers, recovery_generation, log_slots))
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
}

pub fn node_router<R>(runtime: Arc<NodeRuntime>, recorder: R) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    node_router_with_limits(
        runtime,
        recorder,
        DEFAULT_CLIENT_CONCURRENCY,
        DEFAULT_PEER_CONCURRENCY,
    )
}

pub fn node_router_with_limits<R>(
    runtime: Arc<NodeRuntime>,
    recorder: R,
    client_concurrency: usize,
    peer_concurrency: usize,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    node_router_with_optional_checkpoint(
        runtime,
        recorder,
        None,
        client_concurrency,
        peer_concurrency,
    )
}

pub fn node_router_with_checkpoint<R>(
    runtime: Arc<NodeRuntime>,
    recorder: R,
    coordinator: Arc<CheckpointCoordinator>,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    node_router_with_checkpoint_and_limits(
        runtime,
        recorder,
        coordinator,
        DEFAULT_CLIENT_CONCURRENCY,
        DEFAULT_PEER_CONCURRENCY,
    )
}

pub fn node_router_with_checkpoint_and_limits<R>(
    runtime: Arc<NodeRuntime>,
    recorder: R,
    coordinator: Arc<CheckpointCoordinator>,
    client_concurrency: usize,
    peer_concurrency: usize,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    node_router_with_optional_checkpoint(
        runtime,
        recorder,
        Some(coordinator),
        client_concurrency,
        peer_concurrency,
    )
}

fn node_router_with_optional_checkpoint<R>(
    runtime: Arc<NodeRuntime>,
    recorder: R,
    coordinator: Option<Arc<CheckpointCoordinator>>,
    client_concurrency: usize,
    peer_concurrency: usize,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let peers = runtime.config.peers.clone();
    let recovery_generation = runtime.config.recovery_generation();
    let client_slots = Arc::new(tokio::sync::Semaphore::new(client_concurrency));
    let recorder_slots = Arc::new(tokio::sync::Semaphore::new(peer_concurrency));
    let log_slots = Arc::new(tokio::sync::Semaphore::new(peer_concurrency));
    let write_operations = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let (writer, writer_receiver) = tokio::sync::mpsc::channel(client_concurrency.max(1));
    tokio::spawn(writer_loop(
        Arc::downgrade(&runtime),
        coordinator.clone(),
        write_operations.clone(),
        writer_receiver,
        runtime.config.writer_batch_max,
        runtime.config.writer_batch_window,
    ));
    #[cfg(any(feature = "sql", feature = "graph", feature = "kv"))]
    let client_routes: Router = match runtime.config().execution_profile() {
        ExecutionProfile::Sqlite => {
            #[cfg(feature = "sql")]
            {
                Router::new()
                    .route(WRITE_PATH, post(handle_write))
                    .route(READ_PATH, post(handle_read))
                    .route(SQL_EXECUTE_PATH, post(handle_sql_execute))
                    .route(SQL_QUERY_PATH, post(handle_sql_query))
            }
            #[cfg(not(feature = "sql"))]
            unreachable!("SQL runtime cannot open without the sql feature")
        }
        ExecutionProfile::Graph => {
            #[cfg(feature = "graph")]
            {
                Router::new()
                    .route(GRAPH_PUT_DOCUMENT_PATH, post(handle_graph_put_document))
                    .route(
                        GRAPH_DELETE_DOCUMENT_PATH,
                        post(handle_graph_delete_document),
                    )
                    .route(GRAPH_GET_DOCUMENT_PATH, post(handle_graph_get_document))
                    .route(GRAPH_QUERY_PATH, post(handle_graph_query))
            }
            #[cfg(not(feature = "graph"))]
            unreachable!("graph runtime cannot open without the graph feature")
        }
        ExecutionProfile::Kv => {
            #[cfg(feature = "kv")]
            {
                Router::new()
                    .route(KV_PUT_PATH, post(handle_kv_put))
                    .route(KV_DELETE_PATH, post(handle_kv_delete))
                    .route(KV_GET_PATH, post(handle_kv_get))
                    .route(KV_SCAN_PATH, post(handle_kv_scan))
                    .route(KV_BATCH_PATH, post(handle_kv_batch))
            }
            #[cfg(not(feature = "kv"))]
            unreachable!("KV runtime cannot open without the kv feature")
        }
    }
    .route_layer(middleware::from_fn_with_state(
        ClientGateState {
            runtime: runtime.clone(),
            slots: client_slots,
            coordinator: coordinator.clone(),
        },
        client_gate,
    ))
    .with_state(NodeRouteState {
        runtime: runtime.clone(),
        coordinator: coordinator.clone(),
        write_operations: write_operations.clone(),
        writer: writer.clone(),
    });
    #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
    let client_routes: Router = Router::new();
    let health_routes = Router::new()
        .route(LIVEZ_PATH, get(handle_livez))
        .route(READYZ_PATH, get(handle_readyz))
        .with_state(NodeRouteState {
            runtime: runtime.clone(),
            coordinator,
            write_operations,
            writer,
        });
    recorder_routes(recorder, peers.clone(), recovery_generation, recorder_slots)
        .merge(log_routes(
            RuntimeLogPeer { runtime },
            peers,
            recovery_generation,
            log_slots,
        ))
        .merge(client_routes)
        .merge(health_routes)
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
}

fn recorder_routes<R>(
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    slots: Arc<tokio::sync::Semaphore>,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let recorder_peers = peers.clone();
    Router::new()
        .route(RECORDER_IDENTITY_PATH, post(handle_recorder_identity::<R>))
        .route(
            RECORDER_STORE_COMMAND_PATH,
            post(handle_recorder_store_command::<R>),
        )
        .route(
            RECORDER_FETCH_COMMAND_PATH,
            post(handle_recorder_fetch_command::<R>),
        )
        .route(
            RECORDER_STAGE_EFFECT_CHUNK_PATH,
            post(handle_recorder_stage_effect_chunk::<R>),
        )
        .route(
            RECORDER_FINALIZE_EFFECT_BUNDLE_PATH,
            post(handle_recorder_finalize_effect_bundle::<R>),
        )
        .route(
            RECORDER_FETCH_EFFECT_MANIFEST_PATH,
            post(handle_recorder_fetch_effect_manifest::<R>),
        )
        .route(
            RECORDER_FETCH_EFFECT_CHUNK_PATH,
            post(handle_recorder_fetch_effect_chunk::<R>),
        )
        .route(
            RECORDER_INSPECT_PROOF_PATH,
            post(handle_recorder_inspect_proof::<R>),
        )
        .route(
            RECORDER_INSPECT_RECORD_PATH,
            post(handle_recorder_inspect_record::<R>),
        )
        .route(
            RECORDER_READ_FENCE_PATH,
            post(handle_recorder_read_fence::<R>),
        )
        .route(RECORDER_RECORD_PATH, post(handle_recorder_record::<R>))
        .route(
            RECORDER_INSTALL_PROOF_PATH,
            post(handle_recorder_install_proof::<R>),
        )
        .route_layer(middleware::from_fn_with_state(
            PeerGateState {
                peers,
                recovery_generation,
                protocol_version: RECORDER_PROTOCOL_VERSION,
                slots,
            },
            peer_gate,
        ))
        .with_state(RecorderRouteState {
            recorder,
            peers: recorder_peers,
            decode_slots: Arc::new(tokio::sync::Semaphore::new(32)),
        })
}

async fn decode_recorder_json<T, R>(
    state: &RecorderRouteState<R>,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<RecorderWire<T>, Response>
where
    RecorderWire<T>: recorder_decode::RecorderWireRoot,
{
    if !recorder_json_content_type(headers) {
        return Err(recorder_v2_error_response(
            StatusCode::BAD_REQUEST,
            rhiza_quepaxa::Error::Decode(
                "recorder requests require Content-Type: application/json".into(),
            ),
        ));
    }
    let _permit = state.decode_slots.acquire().await.map_err(|_| {
        recorder_v2_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            rhiza_quepaxa::Error::Decode("recorder decode semaphore closed".into()),
        )
    })?;
    #[cfg(test)]
    test_http_decode_hook_after_permit(headers).await;
    recorder_decode::decode_json_exact_bounded(
        &body,
        recorder_decode::RecorderDecodeLimits::for_wire_bytes(MAX_HTTP_BODY_BYTES),
    )
    .map_err(|error| {
        recorder_v2_error_response(
            StatusCode::BAD_REQUEST,
            rhiza_quepaxa::Error::Decode(error.to_string()),
        )
    })
}

fn recorder_json_content_type(headers: &HeaderMap) -> bool {
    header_text(headers, CONTENT_TYPE.as_str()).is_some_and(|value| {
        value
            .split_once(';')
            .map_or(value, |(media_type, _)| media_type)
            .trim()
            .eq_ignore_ascii_case("application/json")
    })
}

fn log_routes<P>(
    peer: P,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    slots: Arc<tokio::sync::Semaphore>,
) -> Router
where
    P: LogPeer + Clone + Send + Sync + 'static,
{
    Router::new()
        .route(LOG_FETCH_PATH, post(handle_fetch_log::<P>))
        .route_layer(middleware::from_fn_with_state(
            PeerGateState {
                peers,
                recovery_generation,
                protocol_version: PROTOCOL_VERSION,
                slots,
            },
            peer_gate,
        ))
        .with_state(LogRouteState { peer })
}

async fn handle_recorder_identity<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let request = match decode_recorder_json::<(), _>(&state, &headers, body).await {
        Ok(request) => request,
        Err(response) => return response,
    };
    if request.version != RECORDER_WIRE_VERSION {
        return recorder_v2_error_response(
            StatusCode::BAD_REQUEST,
            rhiza_quepaxa::Error::Decode("recorder wire version mismatch".into()),
        );
    }
    let context = match request.rpc_context() {
        Ok(context) => context,
        Err(error) => return recorder_v2_error_response(StatusCode::REQUEST_TIMEOUT, error),
    };
    let recorder = state.recorder;
    recorder_v2_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            recorder.recorder_id(&context)
        })
        .await,
    )
}

async fn handle_recorder_store_command<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let request = match decode_recorder_json::<StoreCommandV2, _>(&state, &headers, body).await {
        Ok(request) => request,
        Err(response) => return response,
    };
    if request.version != RECORDER_WIRE_VERSION || !valid_recorder_command(&request.body.command) {
        return recorder_v2_error_response(
            StatusCode::BAD_REQUEST,
            rhiza_quepaxa::Error::Decode("invalid recorder request".into()),
        );
    }
    let context = match request.rpc_context() {
        Ok(context) => context,
        Err(error) => return recorder_v2_error_response(StatusCode::REQUEST_TIMEOUT, error),
    };
    let recorder = state.recorder;
    recorder_v2_mutation_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let body = request.body;
            recorder.store_command_for(
                &context,
                body.cluster_id,
                body.epoch,
                body.config_id,
                body.config_digest,
                body.command_hash,
                body.command,
            )
        })
        .await,
    )
}

async fn handle_recorder_fetch_command<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let request = match decode_recorder_json::<FetchCommandV2, _>(&state, &headers, body).await {
        Ok(request) => request,
        Err(response) => return response,
    };
    if request.version != RECORDER_WIRE_VERSION {
        return recorder_v2_error_response(
            StatusCode::BAD_REQUEST,
            rhiza_quepaxa::Error::Decode("recorder wire version mismatch".into()),
        );
    }
    let context = match request.rpc_context() {
        Ok(context) => context,
        Err(error) => return recorder_v2_error_response(StatusCode::REQUEST_TIMEOUT, error),
    };
    let recorder = state.recorder;
    recorder_v2_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let body = request.body;
            recorder.fetch_command_for(
                &context,
                body.cluster_id,
                body.epoch,
                body.config_id,
                body.config_digest,
                body.command_hash,
            )
        })
        .await,
    )
}

async fn handle_recorder_stage_effect_chunk<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let request = match decode_recorder_json::<StageEffectChunkV3, _>(&state, &headers, body).await
    {
        Ok(request) => request,
        Err(response) => return response,
    };
    if request.version != RECORDER_WIRE_VERSION
        || !valid_recorder_command(&request.body.manifest_command)
    {
        return recorder_v2_error_response(
            StatusCode::BAD_REQUEST,
            rhiza_quepaxa::Error::Decode("invalid recorder effect request".into()),
        );
    }
    let context = match request.rpc_context() {
        Ok(context) => context,
        Err(error) => return recorder_v2_error_response(StatusCode::REQUEST_TIMEOUT, error),
    };
    let recorder = state.recorder;
    recorder_v2_mutation_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let body = request.body;
            recorder.stage_effect_bundle_chunk(
                &context,
                body.binding,
                body.manifest_command,
                body.ordinal,
                body.chunk,
            )
        })
        .await,
    )
}

async fn handle_recorder_finalize_effect_bundle<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let request =
        match decode_recorder_json::<FinalizeEffectBundleV3, _>(&state, &headers, body).await {
            Ok(request) => request,
            Err(response) => return response,
        };
    if request.version != RECORDER_WIRE_VERSION
        || !valid_recorder_command(&request.body.manifest_command)
    {
        return recorder_v2_error_response(
            StatusCode::BAD_REQUEST,
            rhiza_quepaxa::Error::Decode("invalid recorder effect request".into()),
        );
    }
    let context = match request.rpc_context() {
        Ok(context) => context,
        Err(error) => return recorder_v2_error_response(StatusCode::REQUEST_TIMEOUT, error),
    };
    let recorder = state.recorder;
    recorder_v2_mutation_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let body = request.body;
            recorder.finalize_staged_effect_bundle(&context, body.binding, body.manifest_command)
        })
        .await,
    )
}

async fn handle_recorder_fetch_effect_manifest<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let request =
        match decode_recorder_json::<FetchEffectManifestV3, _>(&state, &headers, body).await {
            Ok(request) => request,
            Err(response) => return response,
        };
    if request.version != RECORDER_WIRE_VERSION {
        return recorder_v2_error_response(
            StatusCode::BAD_REQUEST,
            rhiza_quepaxa::Error::Decode("recorder wire version mismatch".into()),
        );
    }
    let context = match request.rpc_context() {
        Ok(context) => context,
        Err(error) => return recorder_v2_error_response(StatusCode::REQUEST_TIMEOUT, error),
    };
    let recorder = state.recorder;
    recorder_v2_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            recorder.fetch_effect_bundle_manifest(&context, request.body.binding)
        })
        .await,
    )
}

async fn handle_recorder_fetch_effect_chunk<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let request = match decode_recorder_json::<FetchEffectChunkV3, _>(&state, &headers, body).await
    {
        Ok(request) => request,
        Err(response) => return response,
    };
    if request.version != RECORDER_WIRE_VERSION {
        return recorder_v2_error_response(
            StatusCode::BAD_REQUEST,
            rhiza_quepaxa::Error::Decode("recorder wire version mismatch".into()),
        );
    }
    let context = match request.rpc_context() {
        Ok(context) => context,
        Err(error) => return recorder_v2_error_response(StatusCode::REQUEST_TIMEOUT, error),
    };
    let recorder = state.recorder;
    recorder_v2_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let body = request.body;
            recorder.fetch_effect_bundle_chunk(&context, body.binding, body.ordinal)
        })
        .await,
    )
}

async fn handle_recorder_inspect_proof<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let request = match decode_recorder_json::<InspectProofV2, _>(&state, &headers, body).await {
        Ok(request) => request,
        Err(response) => return response,
    };
    if request.version != RECORDER_WIRE_VERSION {
        return recorder_v2_error_response(
            StatusCode::BAD_REQUEST,
            rhiza_quepaxa::Error::Decode("recorder wire version mismatch".into()),
        );
    }
    let context = match request.rpc_context() {
        Ok(context) => context,
        Err(error) => return recorder_v2_error_response(StatusCode::REQUEST_TIMEOUT, error),
    };
    let recorder = state.recorder;
    recorder_v2_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            recorder.inspect_decision_proof(&context, request.body.slot)
        })
        .await,
    )
}

async fn handle_recorder_inspect_record<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let request = match decode_recorder_json::<InspectProofV2, _>(&state, &headers, body).await {
        Ok(request) => request,
        Err(response) => return response,
    };
    if request.version != RECORDER_WIRE_VERSION {
        return recorder_v2_error_response(
            StatusCode::BAD_REQUEST,
            rhiza_quepaxa::Error::Decode("recorder wire version mismatch".into()),
        );
    }
    let context = match request.rpc_context() {
        Ok(context) => context,
        Err(error) => return recorder_v2_error_response(StatusCode::REQUEST_TIMEOUT, error),
    };
    let recorder = state.recorder;
    recorder_v2_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            recorder.inspect_record_summary(&context, request.body.slot)
        })
        .await,
    )
}

async fn handle_recorder_read_fence<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let request = match decode_recorder_json::<ReadFenceRequest, _>(&state, &headers, body).await {
        Ok(request) => request,
        Err(response) => return response,
    };
    if request.version != RECORDER_WIRE_VERSION {
        return recorder_v2_error_response(
            StatusCode::BAD_REQUEST,
            rhiza_quepaxa::Error::Decode("recorder wire version mismatch".into()),
        );
    }
    let context = match request.rpc_context() {
        Ok(context) => context,
        Err(error) => return recorder_v2_error_response(StatusCode::REQUEST_TIMEOUT, error),
    };
    let recorder = state.recorder;
    recorder_v2_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            recorder.observe_read_fence(&context, request.body)
        })
        .await,
    )
}

async fn handle_recorder_record<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Extension(authenticated_peer): Extension<AuthenticatedPeer>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let request = match decode_recorder_json::<RecordRequest, _>(&state, &headers, body).await {
        Ok(request) => request,
        Err(response) => return response,
    };
    if request.version != RECORDER_WIRE_VERSION || !valid_recorder_record(&request.body) {
        return recorder_v2_error_response(
            StatusCode::BAD_REQUEST,
            rhiza_quepaxa::Error::Decode("invalid recorder request".into()),
        );
    }
    let context = match request.rpc_context() {
        Ok(context) => context,
        Err(error) => return recorder_v2_error_response(StatusCode::REQUEST_TIMEOUT, error),
    };
    if !authenticated_proposer_admitted(
        &authenticated_peer.0,
        &request.body.proposal.proposer_id,
        &state.peers,
    ) {
        return recorder_v2_response::<RecordSummary>(Ok(Err(rhiza_quepaxa::Error::Rejected(
            RejectReason::InvalidRequest,
        ))));
    }
    let recorder = state.recorder;
    recorder_v2_mutation_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            recorder.record(&context, request.body)
        })
        .await,
    )
}

fn valid_recorder_command(command: &StoredCommand) -> bool {
    command.payload.len() <= MAX_COMMAND_BYTES
}

fn valid_recorder_record(request: &RecordRequest) -> bool {
    !request.cluster_id.is_empty()
        && request.cluster_id.len() <= MAX_REQUEST_ID_BYTES
        && request.command.as_ref().is_none_or(valid_recorder_command)
}

fn authenticated_proposer_admitted(
    authenticated_peer_id: &str,
    proposer_id: &str,
    peers: &[PeerConfig],
) -> bool {
    // Record requests carry config identity but not its membership. Configured peers are therefore
    // the transport identity authority for records and proofs until rebuilt after a transition.
    peers
        .iter()
        .any(|peer| peer.node_id == authenticated_peer_id)
        && peers.iter().any(|peer| peer.node_id == proposer_id)
}

async fn handle_recorder_install_proof<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Extension(authenticated_peer): Extension<AuthenticatedPeer>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let request = match decode_recorder_json::<InstallProofV2, _>(&state, &headers, body).await {
        Ok(request) => request,
        Err(response) => return response,
    };
    if request.version != RECORDER_WIRE_VERSION {
        return recorder_v2_error_response(
            StatusCode::BAD_REQUEST,
            rhiza_quepaxa::Error::Decode("recorder wire version mismatch".into()),
        );
    }
    let context = match request.rpc_context() {
        Ok(context) => context,
        Err(error) => return recorder_v2_error_response(StatusCode::REQUEST_TIMEOUT, error),
    };
    if !authenticated_proposer_admitted(
        &authenticated_peer.0,
        &request.body.proof.proposal().proposer_id,
        &state.peers,
    ) {
        return recorder_v2_response::<()>(Ok(Err(rhiza_quepaxa::Error::Rejected(
            RejectReason::InvalidRequest,
        ))));
    }
    let recorder = state.recorder;
    recorder_v2_mutation_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let membership = Membership::from_voters(request.body.members)?;
            recorder.install_decision_proof(&context, request.body.proof, &membership)
        })
        .await,
    )
}

fn recorder_v2_response<T: serde::Serialize>(
    result: Result<rhiza_quepaxa::Result<T>, tokio::task::JoinError>,
) -> Response {
    let (status, body) = match result {
        Ok(Ok(value)) => (StatusCode::OK, RecorderV2Result::Ok(value)),
        Ok(Err(rhiza_quepaxa::Error::Rejected(reason))) => {
            (StatusCode::CONFLICT, RecorderV2Result::Rejected(reason))
        }
        Ok(Err(error)) => (
            recorder_error_status(&error),
            RecorderV2Result::Error(recorder_wire_error(error)),
        ),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            RecorderV2Result::Error(RecorderWireError {
                code: RecorderWireErrorCode::ProposeFailed,
                message: error.to_string(),
                detail: None,
            }),
        ),
    };
    recorder_v2_json_response(
        status,
        body,
        StatusCode::INTERNAL_SERVER_ERROR,
        rhiza_quepaxa::Error::Decode(RECORDER_HTTP_RESPONSE_LIMIT_ERROR.into()),
    )
}

fn recorder_v2_mutation_response<T: serde::Serialize>(
    result: Result<rhiza_quepaxa::Result<T>, tokio::task::JoinError>,
) -> Response {
    match result {
        // The backend has completed successfully at this point. If its success
        // body cannot reach the peer, the peer cannot distinguish that from a
        // completed mutation whose response was lost.
        Ok(Ok(value)) => recorder_v2_json_response(
            StatusCode::OK,
            RecorderV2Result::Ok(value),
            StatusCode::INTERNAL_SERVER_ERROR,
            rhiza_quepaxa::Error::UnknownOutcome,
        ),
        Ok(Err(error)) => recorder_v2_response::<T>(Ok(Err(error))),
        Err(_) => recorder_v2_response::<T>(Ok(Err(rhiza_quepaxa::Error::UnknownOutcome))),
    }
}

fn recorder_v2_error_response(status: StatusCode, error: rhiza_quepaxa::Error) -> Response {
    recorder_v2_json_response(
        status,
        RecorderV2Result::<()>::Error(recorder_wire_error(error)),
        status,
        rhiza_quepaxa::Error::Decode(RECORDER_HTTP_RESPONSE_LIMIT_ERROR.into()),
    )
}

fn recorder_v2_json_response<T: serde::Serialize>(
    status: StatusCode,
    body: RecorderV2Result<T>,
    overflow_status: StatusCode,
    overflow_error: rhiza_quepaxa::Error,
) -> Response {
    let wire = RecorderWire {
        version: RECORDER_WIRE_VERSION,
        remaining_deadline_ms: 0,
        body,
    };
    let encoded = match bounded_json_encode(
        &wire,
        MAX_HTTP_BODY_BYTES,
        RECORDER_HTTP_RESPONSE_LIMIT_ERROR,
    ) {
        Ok(encoded) => return recorder_v2_json_bytes(status, encoded),
        Err(_) => RecorderWire {
            version: RECORDER_WIRE_VERSION,
            remaining_deadline_ms: 0,
            body: RecorderV2Result::<T>::Error(recorder_wire_error(overflow_error)),
        },
    };
    match bounded_json_encode(
        &encoded,
        MAX_HTTP_BODY_BYTES,
        RECORDER_HTTP_RESPONSE_LIMIT_ERROR,
    ) {
        Ok(encoded) => recorder_v2_json_bytes(overflow_status, encoded),
        // The fallback contains only fixed protocol fields. Do not recurse if
        // a future serializer change somehow makes even it unrepresentable.
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

fn recorder_v2_json_bytes(status: StatusCode, encoded: Vec<u8>) -> Response {
    (status, [(CONTENT_TYPE, "application/json")], encoded).into_response()
}

async fn handle_fetch_log<P>(
    State(state): State<LogRouteState<P>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Json(request): Json<FetchLogRequest>,
) -> Response
where
    P: LogPeer + Clone + Send + Sync + 'static,
{
    if request.from_index == 0 || request.max_entries > MAX_FETCH_ENTRIES {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let peer = state.peer;
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        peer.fetch_log(request)
    })
    .await;
    match result {
        Ok(Ok(response)) => Json(FetchLogHttpResponse::Fetched(response)).into_response(),
        Ok(Err(error)) => (
            fetch_log_error_status(&error),
            Json(FetchLogHttpResponse::Failed(error)),
        )
            .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[cfg(feature = "sql")]
async fn handle_write(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<WriteRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let payload = match canonical_put(&request.request_id, &request.key, &request.value) {
        Ok(payload) => payload,
        Err(error) => return node_error_response(error),
    };
    let request_id = request.request_id.clone();
    let operation = QueuedOperation::KeyValue {
        key: request.key,
        value: request.value,
    };
    coordinate_write(state, permit, request_id, payload, operation).await
}

#[cfg(feature = "sql")]
async fn handle_sql_execute(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<SqlExecuteRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    if let Err(error) = validate_field(
        "request_id",
        &request.request_id,
        MAX_REQUEST_ID_BYTES,
        false,
    ) {
        return node_error_response(error);
    }
    let command = SqlCommand {
        request_id: request.request_id.clone(),
        statements: request.statements,
    };
    let payload = match encode_sql_command_with_index(&command) {
        Ok(payload) if payload.len() <= MAX_COMMAND_BYTES => payload,
        Ok(_) => {
            return node_error_response(NodeError::InvalidRequest(format!(
                "command exceeds {MAX_COMMAND_BYTES} bytes"
            )));
        }
        Err(error) => return node_error_response(error),
    };
    let request_id = command.request_id.clone();
    coordinate_write(
        state,
        permit,
        request_id,
        payload,
        QueuedOperation::Sql(command),
    )
    .await
}

async fn coordinate_write(
    state: NodeRouteState,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    request_id: String,
    payload: Vec<u8>,
    operation: QueuedOperation,
) -> Response {
    let deadline = tokio::time::Instant::now() + CLIENT_WRITE_WAIT_TIMEOUT;
    let (mut receiver, queued) = {
        let mut operations = state.write_operations.lock().await;
        if let Some(operation) = operations.get(&request_id) {
            if operation.payload != payload {
                return client_error_response(
                    StatusCode::CONFLICT,
                    "request_conflict",
                    false,
                    "request id is already in flight with a different payload",
                    None,
                );
            }
            (operation.result.clone(), None)
        } else {
            let (sender, receiver) = tokio::sync::watch::channel(None);
            operations.insert(
                request_id.clone(),
                WriteOperation {
                    payload: payload.clone(),
                    result: receiver.clone(),
                },
            );
            (
                receiver,
                Some(QueuedWrite {
                    request_id: request_id.clone(),
                    payload,
                    operation,
                    permit,
                    sender,
                }),
            )
        }
    };
    if let Some(queued) = queued {
        match tokio::time::timeout_at(deadline, state.writer.send(queued)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                state.write_operations.lock().await.remove(&request_id);
                return client_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "durability_unavailable",
                    true,
                    "writer queue is unavailable",
                    None,
                );
            }
            Err(_) => {
                state.write_operations.lock().await.remove(&request_id);
                return client_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "write_timeout",
                    true,
                    "write did not enter the queue before the response deadline",
                    None,
                );
            }
        }
    }
    let wait = async {
        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result;
            }
            if receiver.changed().await.is_err() {
                return WriteOperationResult::DurabilityUnavailable;
            }
        }
    };
    match tokio::time::timeout_at(deadline, wait).await {
        Ok(WriteOperationResult::Runtime(Ok(response))) => match response {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            ClientWriteResponse::Unavailable => {
                unreachable!("no execution profiles are compiled in")
            }
            #[cfg(feature = "sql")]
            ClientWriteResponse::KeyValue(response) => Json(response).into_response(),
            #[cfg(feature = "sql")]
            ClientWriteResponse::Sql(response) => Json(response).into_response(),
            #[cfg(feature = "graph")]
            ClientWriteResponse::Graph(outcome) => {
                Json(graph_mutation_response(outcome)).into_response()
            }
            #[cfg(feature = "kv")]
            ClientWriteResponse::Kv(outcome) => Json(kv_mutation_response(outcome)).into_response(),
        },
        Ok(WriteOperationResult::Runtime(Err(error))) => node_error_response(error),
        Ok(WriteOperationResult::DurabilityUnavailable) => client_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "durability_unavailable",
            true,
            "durability confirmation is unavailable",
            None,
        ),
        Err(_) => client_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "write_outcome_unknown",
            true,
            "write was admitted but its durable outcome was not observed before the response deadline; retry the same request id",
            None,
        ),
    }
}

async fn writer_loop(
    runtime: std::sync::Weak<NodeRuntime>,
    coordinator: Option<Arc<CheckpointCoordinator>>,
    write_operations: Arc<tokio::sync::Mutex<HashMap<String, WriteOperation>>>,
    mut receiver: tokio::sync::mpsc::Receiver<QueuedWrite>,
    batch_max: usize,
    batch_window: Duration,
) {
    // Hoist batch vector allocation outside the loop to reuse across iterations.
    let mut queued = Vec::with_capacity(batch_max);
    while let Some(first) = receiver.recv().await {
        queued.clear();
        queued.push(first);
        let deadline = tokio::time::Instant::now() + batch_window;
        while queued.len() < batch_max {
            match tokio::time::timeout_at(deadline, receiver.recv()).await {
                Ok(Some(next)) => queued.push(next),
                Ok(None) | Err(_) => break,
            }
        }

        let mut dispatch = Vec::with_capacity(queued.len());
        let mut members = Vec::with_capacity(queued.len());
        for queued in queued.drain(..) {
            members.push(RuntimeBatchMember {
                #[cfg(feature = "sql")]
                request_id: queued.request_id.clone(),
                payload: queued.payload,
                operation: queued.operation,
            });
            dispatch.push((queued.request_id, queued.sender, queued.permit));
        }

        let Some(runtime) = runtime.upgrade() else {
            for (request_id, sender, _permit) in dispatch {
                sender.send_replace(Some(WriteOperationResult::DurabilityUnavailable));
                write_operations.lock().await.remove(&request_id);
            }
            break;
        };

        let blocking_runtime = runtime.clone();
        let results =
            tokio::task::spawn_blocking(move || blocking_runtime.execute_client_batch(members))
                .await
                .unwrap_or_else(|error| {
                    (0..dispatch.len())
                        .map(|_| {
                            Err(NodeError::Fatal(format!(
                                "writer batch task failed: {error}"
                            )))
                        })
                        .collect()
                });
        let dispatch = dispatch
            .into_iter()
            .map(|(request_id, sender, permit)| {
                drop(permit);
                (request_id, sender)
            })
            .collect::<Vec<_>>();

        let committed_index = results
            .iter()
            .filter_map(|result| result.as_ref().ok().map(ClientWriteResponse::applied_index))
            .max();
        let durability_available = if let Some(index) = committed_index {
            confirm_write_durability(runtime.as_ref(), coordinator.as_deref(), index)
                .await
                .is_ok()
        } else {
            true
        };

        for ((request_id, sender), result) in dispatch.into_iter().zip(results) {
            let delivered = if !durability_available && result.is_ok() {
                WriteOperationResult::DurabilityUnavailable
            } else {
                WriteOperationResult::Runtime(result)
            };
            sender.send_replace(Some(delivered));
            write_operations.lock().await.remove(&request_id);
        }
    }
}

#[cfg(feature = "sql")]
async fn handle_sql_query(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<SqlQueryRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let runtime = state.runtime;
    let consistency = request
        .consistency
        .unwrap_or(runtime.config.read_consistency());
    let max_rows = request.max_rows.unwrap_or(DEFAULT_SQL_MAX_ROWS);
    if max_rows == 0 || max_rows > MAX_SQL_MAX_ROWS {
        return node_error_response(NodeError::InvalidRequest(format!(
            "max_rows must be between 1 and {MAX_SQL_MAX_ROWS}"
        )));
    }
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        runtime.query_sql(&request.statement, consistency, max_rows)
    })
    .await;
    match result {
        Ok(Ok(response)) => sql_query_http_response(response),
        Ok(Err(error)) => node_error_response(error),
        Err(error) => client_task_error(error),
    }
}

#[cfg(feature = "sql")]
fn sql_query_http_response(response: SqlQueryResponse) -> Response {
    match serde_json::to_vec(&response) {
        Ok(encoded) if encoded.len() <= MAX_SQL_RESPONSE_BYTES => (
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            encoded,
        )
            .into_response(),
        Ok(_) => node_error_response(NodeError::InvalidRequest(format!(
            "SQL response exceeds {MAX_SQL_RESPONSE_BYTES} bytes"
        ))),
        Err(error) => node_error_response(NodeError::InvalidRequest(error.to_string())),
    }
}

#[cfg(feature = "graph")]
async fn handle_graph_put_document(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<GraphPutDocumentRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let value = match GraphValueV1::try_from(request.value) {
        Ok(value) => value,
        Err(error) => return node_error_response(error),
    };
    let command = match GraphCommandV1::put_document(request.request_id, request.id, value) {
        Ok(command) => command,
        Err(error) => return node_error_response(NodeError::InvalidRequest(error.to_string())),
    };
    execute_graph_mutation(state, permit, command).await
}

#[cfg(feature = "graph")]
async fn handle_graph_delete_document(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<GraphDeleteDocumentRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let command = match GraphCommandV1::delete_document(request.request_id, request.id) {
        Ok(command) => command,
        Err(error) => return node_error_response(NodeError::InvalidRequest(error.to_string())),
    };
    execute_graph_mutation(state, permit, command).await
}

#[cfg(feature = "graph")]
async fn execute_graph_mutation(
    state: NodeRouteState,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    command: GraphCommandV1,
) -> Response {
    let payload = match encode_replicated_graph_command(&command) {
        Ok(payload) if payload.len() <= MAX_COMMAND_BYTES => payload,
        Ok(_) => {
            return node_error_response(NodeError::InvalidRequest(format!(
                "command exceeds {MAX_COMMAND_BYTES} bytes"
            )));
        }
        Err(error) => return node_error_response(NodeError::InvalidRequest(error.to_string())),
    };
    coordinate_write(
        state,
        permit,
        command.request_id().to_owned(),
        payload,
        QueuedOperation::Graph(command),
    )
    .await
}

#[cfg(feature = "graph")]
async fn handle_graph_get_document(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<GraphGetDocumentRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let runtime = state.runtime;
    let consistency = request
        .consistency
        .unwrap_or(runtime.config.read_consistency());
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        runtime.get_graph_document(&request.id, consistency)
    })
    .await;
    match result {
        Ok(Ok(response)) => Json(GraphGetDocumentResponse {
            value: response.value.map(GraphValueDto::from),
            applied_index: response.applied_index,
            hash: response.hash,
        })
        .into_response(),
        Ok(Err(error)) => node_error_response(error),
        Err(error) => client_task_error(error),
    }
}

#[cfg(feature = "graph")]
fn with_graph_client_permit<T>(
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    response_work: impl FnOnce() -> T,
) -> T {
    let result = response_work();
    drop(permit);
    result
}

#[cfg(feature = "graph")]
async fn handle_graph_query(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<GraphQueryRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let parameters = match request
        .statement
        .parameters
        .into_iter()
        .map(|(name, value)| GraphParameterValue::try_from(value).map(|value| (name, value)))
        .collect::<Result<BTreeMap<_, _>, _>>()
    {
        Ok(parameters) => parameters,
        Err(error) => return node_error_response(error),
    };
    let runtime = state.runtime;
    let consistency = request
        .consistency
        .unwrap_or(runtime.config.read_consistency());
    let max_rows = request.max_rows.unwrap_or(DEFAULT_GRAPH_MAX_ROWS);
    if max_rows == 0 || max_rows > MAX_GRAPH_MAX_ROWS {
        return node_error_response(NodeError::InvalidRequest(format!(
            "max_rows must be between 1 and {MAX_GRAPH_MAX_ROWS}"
        )));
    }
    let result = tokio::task::spawn_blocking(move || {
        runtime.query_graph(
            &request.statement.cypher,
            &parameters,
            consistency,
            max_rows,
        )
    })
    .await;
    with_graph_client_permit(permit, || match result {
        Ok(Ok(result)) => {
            let response = GraphQueryResponse::from(result);
            match serde_json::to_vec(&response) {
                Ok(encoded) if encoded.len() <= MAX_GRAPH_RESPONSE_BYTES => (
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    encoded,
                )
                    .into_response(),
                Ok(_) => node_error_response(NodeError::InvalidRequest(format!(
                    "graph response exceeds {MAX_GRAPH_RESPONSE_BYTES} bytes"
                ))),
                Err(error) => node_error_response(NodeError::InvalidRequest(error.to_string())),
            }
        }
        Ok(Err(error)) => node_error_response(error),
        Err(error) => client_task_error(error),
    })
}

#[cfg(feature = "kv")]
async fn handle_kv_put(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<KvPutRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let key = match decode_base64("key", &request.key) {
        Ok(value) => value,
        Err(error) => return node_error_response(error),
    };
    let value = match decode_base64("value", &request.value) {
        Ok(value) => value,
        Err(error) => return node_error_response(error),
    };
    let command = match KvCommandV1::put(request.request_id, key, value) {
        Ok(command) => command,
        Err(error) => return node_error_response(NodeError::InvalidRequest(error.to_string())),
    };
    execute_kv_mutation(state, permit, command).await
}

#[cfg(feature = "kv")]
async fn handle_kv_delete(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<KvDeleteRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let key = match decode_base64("key", &request.key) {
        Ok(value) => value,
        Err(error) => return node_error_response(error),
    };
    let command = match KvCommandV1::delete(request.request_id, key) {
        Ok(command) => command,
        Err(error) => return node_error_response(NodeError::InvalidRequest(error.to_string())),
    };
    execute_kv_mutation(state, permit, command).await
}

#[cfg(feature = "kv")]
async fn execute_kv_mutation(
    state: NodeRouteState,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    command: KvCommandV1,
) -> Response {
    let payload = match encode_replicated_kv_command(&command) {
        Ok(payload) if payload.len() <= MAX_COMMAND_BYTES => payload,
        Ok(_) => {
            return node_error_response(NodeError::InvalidRequest(format!(
                "command exceeds {MAX_COMMAND_BYTES} bytes"
            )));
        }
        Err(error) => return node_error_response(NodeError::InvalidRequest(error.to_string())),
    };
    coordinate_write(
        state,
        permit,
        command.request_id().to_owned(),
        payload,
        QueuedOperation::Kv(command),
    )
    .await
}

#[cfg(feature = "kv")]
async fn handle_kv_get(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<KvGetRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let key = match decode_base64("key", &request.key) {
        Ok(value) => value,
        Err(error) => return node_error_response(error),
    };
    let runtime = state.runtime;
    let consistency = request
        .consistency
        .unwrap_or(runtime.config.read_consistency());
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        runtime.get_kv(&key, consistency)
    })
    .await;
    match result {
        Ok(Ok(response)) => Json(KvGetResponse {
            value: response.value.as_deref().map(encode_base64),
            applied_index: response.applied_index,
            hash: response.hash,
        })
        .into_response(),
        Ok(Err(error)) => node_error_response(error),
        Err(error) => client_task_error(error),
    }
}

#[cfg(feature = "kv")]
enum DecodedKvScan {
    Range {
        start: Vec<u8>,
        end: Option<Vec<u8>>,
    },
    Prefix(Vec<u8>),
}

#[cfg(feature = "kv")]
async fn handle_kv_scan(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<KvScanRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let limit = request
        .limit
        .unwrap_or(usize::try_from(DEFAULT_KV_SCAN_LIMIT).expect("u32 fits usize"));
    if limit == 0 || limit > MAX_KV_SCAN_ROWS {
        return node_error_response(NodeError::InvalidRequest(format!(
            "limit must be between 1 and {MAX_KV_SCAN_ROWS}"
        )));
    }
    let cursor = match request.cursor {
        Some(cursor) => match decode_base64("cursor", &cursor) {
            Ok(cursor) => Some(cursor),
            Err(error) => return node_error_response(error),
        },
        None => None,
    };
    let scan = match (request.prefix, request.start, request.end) {
        (Some(prefix), None, None) => match decode_base64("prefix", &prefix) {
            Ok(prefix) => DecodedKvScan::Prefix(prefix),
            Err(error) => return node_error_response(error),
        },
        (None, Some(start), end) => {
            let start = match decode_base64("start", &start) {
                Ok(start) => start,
                Err(error) => return node_error_response(error),
            };
            let end = match end {
                Some(end) => match decode_base64("end", &end) {
                    Ok(end) => Some(end),
                    Err(error) => return node_error_response(error),
                },
                None => None,
            };
            DecodedKvScan::Range { start, end }
        }
        _ => {
            return node_error_response(NodeError::InvalidRequest(
                "provide either prefix alone or start with optional end".into(),
            ));
        }
    };
    let runtime = state.runtime;
    let consistency = request
        .consistency
        .unwrap_or(runtime.config.read_consistency());
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        match scan {
            DecodedKvScan::Range { start, end } => runtime.scan_kv_range(
                &start,
                end.as_deref(),
                limit,
                cursor.as_deref(),
                consistency,
            ),
            DecodedKvScan::Prefix(prefix) => {
                runtime.scan_kv_prefix(&prefix, limit, cursor.as_deref(), consistency)
            }
        }
    })
    .await;
    match result {
        Ok(Ok(result)) => {
            let response = KvScanResponse {
                entries: result
                    .rows()
                    .iter()
                    .map(|row| KvScanEntryDto {
                        key: encode_base64(row.key()),
                        value: encode_base64(row.value()),
                    })
                    .collect(),
                next_cursor: result.next_cursor().map(encode_base64),
                applied_index: result.tip().applied_index(),
                hash: result.tip().applied_hash(),
            };
            match serde_json::to_vec(&response) {
                Ok(encoded) if encoded.len() <= MAX_KV_SCAN_RESPONSE_BYTES => {
                    Json(response).into_response()
                }
                Ok(_) => node_error_response(NodeError::ResourceExhausted(format!(
                    "KV scan response exceeds {MAX_KV_SCAN_RESPONSE_BYTES} bytes"
                ))),
                Err(error) => node_error_response(NodeError::InvalidRequest(error.to_string())),
            }
        }
        Ok(Err(error)) => node_error_response(error),
        Err(error) => client_task_error(error),
    }
}

#[cfg(feature = "kv")]
async fn handle_kv_batch(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<KvBatchRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    if request.members.is_empty() || request.members.len() > MAX_KV_BATCH_MEMBERS {
        return node_error_response(NodeError::InvalidRequest(format!(
            "batch must contain 1..={MAX_KV_BATCH_MEMBERS} members"
        )));
    }
    let mut commands = Vec::with_capacity(request.members.len());
    for member in request.members {
        match member {
            KvBatchMemberRequest::Put(put) => {
                let key = match decode_base64("key", &put.key) {
                    Ok(key) => key,
                    Err(error) => return node_error_response(error),
                };
                let value = match decode_base64("value", &put.value) {
                    Ok(value) => value,
                    Err(error) => return node_error_response(error),
                };
                match KvCommandV1::put(put.request_id, key, value) {
                    Ok(command) => commands.push(command),
                    Err(error) => {
                        return node_error_response(NodeError::InvalidRequest(error.to_string()))
                    }
                }
            }
            KvBatchMemberRequest::Delete(delete) => {
                let key = match decode_base64("key", &delete.key) {
                    Ok(key) => key,
                    Err(error) => return node_error_response(error),
                };
                match KvCommandV1::delete(delete.request_id, key) {
                    Ok(command) => commands.push(command),
                    Err(error) => {
                        return node_error_response(NodeError::InvalidRequest(error.to_string()))
                    }
                }
            }
        }
    }
    let batch_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut member_responses = Vec::with_capacity(commands.len());
    for (idx, command) in commands.into_iter().enumerate() {
        let payload = match encode_replicated_kv_command(&command) {
            Ok(payload) if payload.len() <= MAX_COMMAND_BYTES => payload,
            Ok(_) => {
                return node_error_response(NodeError::InvalidRequest(format!(
                    "command exceeds {MAX_COMMAND_BYTES} bytes"
                )));
            }
            Err(error) => return node_error_response(NodeError::InvalidRequest(error.to_string())),
        };
        let request_id = format!("__rhiza_kv_batch_{batch_id}_{idx}");
        let deadline = tokio::time::Instant::now() + CLIENT_WRITE_WAIT_TIMEOUT;
        let (mut receiver, queued) = {
            let mut operations = state.write_operations.lock().await;
            if let Some(operation) = operations.get(&request_id) {
                if operation.payload != payload {
                    return client_error_response(
                        StatusCode::CONFLICT,
                        "request_conflict",
                        false,
                        "request id is already in flight with a different payload",
                        None,
                    );
                }
                (operation.result.clone(), None)
            } else {
                let (sender, receiver) = tokio::sync::watch::channel(None);
                operations.insert(
                    request_id.clone(),
                    WriteOperation {
                        payload: payload.clone(),
                        result: receiver.clone(),
                    },
                );
                (
                    receiver,
                    Some(QueuedWrite {
                        request_id: request_id.clone(),
                        payload,
                        operation: QueuedOperation::Kv(command),
                        permit: permit.clone(),
                        sender,
                    }),
                )
            }
        };
        if let Some(queued) = queued {
            match tokio::time::timeout_at(deadline, state.writer.send(queued)).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    state.write_operations.lock().await.remove(&request_id);
                    return node_error_response(NodeError::Unavailable(
                        "writer queue is unavailable".into(),
                    ));
                }
                Err(_) => {
                    state.write_operations.lock().await.remove(&request_id);
                    return node_error_response(NodeError::Unavailable(
                        "write did not enter the queue before the response deadline".into(),
                    ));
                }
            }
        }
        let wait = async {
            loop {
                if let Some(result) = receiver.borrow().clone() {
                    return result;
                }
                if receiver.changed().await.is_err() {
                    return WriteOperationResult::DurabilityUnavailable;
                }
            }
        };
        match tokio::time::timeout_at(deadline, wait).await {
            Ok(WriteOperationResult::Runtime(Ok(ClientWriteResponse::Kv(outcome)))) => {
                member_responses.push(KvBatchMemberResponse {
                    applied_index: outcome.applied_index(),
                    hash: outcome.hash(),
                    result: match outcome.result() {
                        KvCommandResultV1::Put { replaced } => KvMutationResultDto::Put {
                            replaced: *replaced,
                        },
                        KvCommandResultV1::Delete { existed } => {
                            KvMutationResultDto::Delete { existed: *existed }
                        }
                    },
                });
            }
            #[cfg(any(feature = "sql", feature = "graph"))]
            Ok(WriteOperationResult::Runtime(Ok(_))) => {
                return node_error_response(NodeError::Invariant(
                    "KV batch returned a response for another profile".into(),
                ));
            }
            Ok(WriteOperationResult::Runtime(Err(error))) => {
                return node_error_response(error);
            }
            Ok(WriteOperationResult::DurabilityUnavailable) => {
                return node_error_response(NodeError::Unavailable(
                    "durability confirmation is unavailable".into(),
                ));
            }
            Err(_) => {
                return node_error_response(NodeError::Unavailable("write timed out".into()));
            }
        }
    }
    Json(KvBatchResponse {
        members: member_responses,
    })
    .into_response()
}

#[cfg(feature = "sql")]
async fn handle_read(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<ReadRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let runtime = state.runtime;
    let consistency = request
        .consistency
        .unwrap_or(runtime.config.read_consistency());
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        runtime.read(&request.key, consistency)
    })
    .await;
    match result {
        Ok(Ok(response)) => Json(response).into_response(),
        Ok(Err(error)) => node_error_response(error),
        Err(error) => client_task_error(error),
    }
}

async fn peer_gate(
    State(state): State<PeerGateState>,
    mut request: Request,
    next: Next,
) -> Response {
    if !recovery_generation_matches(request.headers(), state.recovery_generation) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(authenticated_peer) =
        authenticated_peer(request.headers(), &state.peers, state.protocol_version)
    else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let permit = match state.slots.try_acquire_owned() {
        Ok(permit) => Arc::new(permit),
        Err(_)
            if request.uri().path().starts_with("/v2/quepaxa/recorder/")
                || request.uri().path().starts_with("/v3/quepaxa/recorder/") =>
        {
            return recorder_v2_error_response(
                StatusCode::TOO_MANY_REQUESTS,
                rhiza_quepaxa::Error::Io("recorder RPC overloaded".into()),
            );
        }
        Err(_) => return StatusCode::TOO_MANY_REQUESTS.into_response(),
    };
    let response_permit = Arc::clone(&permit);
    request.extensions_mut().insert(permit);
    request
        .extensions_mut()
        .insert(AuthenticatedPeer(authenticated_peer));
    retain_peer_permit_until_response_body_complete(next.run(request).await, response_permit)
}

async fn client_gate(
    State(state): State<ClientGateState>,
    mut request: Request,
    next: Next,
) -> Response {
    if !client_authenticated(request.headers(), state.runtime.config.client_token()) {
        return client_error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            false,
            "client authentication failed",
            None,
        );
    }
    if let Some(response) = runtime_readiness_response(state.runtime.as_ref()) {
        return response;
    }
    if client_write_path(request.uri().path())
        && state
            .coordinator
            .as_ref()
            .is_some_and(|coordinator| coordinator.write_allowed().is_err())
    {
        return client_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "writes_unavailable",
            true,
            "writes are temporarily unavailable",
            None,
        );
    }
    let permit = match state.slots.try_acquire_owned() {
        Ok(permit) => Arc::new(permit),
        Err(_) => {
            return client_error_response(
                StatusCode::TOO_MANY_REQUESTS,
                "overloaded",
                true,
                "client request capacity is exhausted",
                None,
            );
        }
    };
    request.extensions_mut().insert(permit);
    next.run(request).await
}

fn client_write_path(path: &str) -> bool {
    #[cfg(feature = "sql")]
    if matches!(path, WRITE_PATH | SQL_EXECUTE_PATH) {
        return true;
    }
    #[cfg(feature = "graph")]
    if matches!(path, GRAPH_PUT_DOCUMENT_PATH | GRAPH_DELETE_DOCUMENT_PATH) {
        return true;
    }
    #[cfg(feature = "kv")]
    if matches!(path, KV_PUT_PATH | KV_DELETE_PATH) {
        return true;
    }
    false
}

async fn handle_livez() -> StatusCode {
    StatusCode::OK
}

async fn handle_readyz(State(state): State<NodeRouteState>) -> StatusCode {
    if state.runtime.is_ready()
        && state
            .runtime
            .configuration_state()
            .is_ok_and(|configuration| configuration.is_active())
        && state
            .coordinator
            .as_ref()
            .is_none_or(|coordinator| coordinator.health() == DurabilityHealth::Available)
    {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

fn next_sync_flush_retry(current: Duration) -> Duration {
    current.saturating_mul(2).min(SYNC_FLUSH_RETRY_MAX)
}

fn retryable_sync_flush_error(error: &DurabilityError) -> bool {
    matches!(
        error,
        DurabilityError::Archive(_) | DurabilityError::Io(_) | DurabilityError::Unavailable
    )
}

/// Confirms that a committed write has reached the configured durability boundary.
///
/// Transient archive I/O and Recorder effect-read unavailability are retried
/// with bounded backoff until durability recovers or shutdown begins.
pub async fn confirm_write_durability(
    runtime: &NodeRuntime,
    coordinator: Option<&CheckpointCoordinator>,
    index: LogIndex,
) -> Result<(), DurabilityError> {
    let Some(coordinator) = coordinator else {
        return Ok(());
    };
    coordinator.note_committed(index);
    if !matches!(coordinator.mode(), DurabilityMode::Sync) {
        return Ok(());
    }

    let mut confirmation = coordinator.sync_durability_confirmation();
    let mut retry_delay = SYNC_FLUSH_RETRY_INITIAL;
    loop {
        if runtime.operation_cancelled.load(Ordering::Acquire) {
            return Err(DurabilityError::Unavailable);
        }
        match coordinator.flush_runtime(runtime, index).await {
            Ok(tip) if tip.index() >= index => {
                confirmation.disarm();
                return Ok(());
            }
            Ok(tip) => {
                return Err(DurabilityError::LocalLogGap {
                    expected: index,
                    actual: Some(tip.index()),
                });
            }
            Err(error) if retryable_sync_flush_error(&error) => {
                let cancelled = runtime.operation_cancelled_notify.notified();
                tokio::pin!(cancelled);
                cancelled.as_mut().enable();
                if runtime.operation_cancelled.load(Ordering::Acquire) {
                    return Err(DurabilityError::Unavailable);
                }
                tokio::select! {
                    () = tokio::time::sleep(retry_delay) => {}
                    () = &mut cancelled => return Err(DurabilityError::Unavailable),
                }
                retry_delay = next_sync_flush_retry(retry_delay);
            }
            Err(error) => return Err(error),
        }
    }
}

fn authenticated_peer(
    headers: &HeaderMap,
    peers: &[PeerConfig],
    protocol_version: &str,
) -> Option<String> {
    if header_text(headers, VERSION_HEADER) != Some(protocol_version) {
        return None;
    }
    let node_id = header_text(headers, NODE_ID_HEADER)?;
    let token = bearer_token(headers)?;
    peer_credentials_authenticated(node_id, token, peers).then(|| node_id.to_owned())
}

fn peer_credentials_authenticated(node_id: &str, token: &str, peers: &[PeerConfig]) -> bool {
    peers
        .iter()
        .find(|peer| peer.node_id == node_id)
        .is_some_and(|peer| secrets_equal(peer.token.as_bytes(), token.as_bytes()))
}

fn recovery_generation_matches(headers: &HeaderMap, expected: u64) -> bool {
    let expected = expected.to_string();
    header_text(headers, RECOVERY_GENERATION_HEADER) == Some(expected.as_str())
}

fn client_authenticated(headers: &HeaderMap, expected_token: &str) -> bool {
    !expected_token.is_empty()
        && version_matches(headers)
        && bearer_token(headers)
            .is_some_and(|token| secrets_equal(expected_token.as_bytes(), token.as_bytes()))
}

fn version_matches(headers: &HeaderMap) -> bool {
    header_text(headers, VERSION_HEADER) == Some(PROTOCOL_VERSION)
}

fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    header_text(headers, "authorization")?.strip_prefix("Bearer ")
}

fn secrets_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn node_error_response(error: NodeError) -> Response {
    let (status, statement_index) = match &error {
        NodeError::InvalidRequest(_) => (StatusCode::BAD_REQUEST, None),
        #[cfg(feature = "sql")]
        NodeError::InvalidSqlStatement {
            statement_index, ..
        } => (StatusCode::BAD_REQUEST, Some(*statement_index)),
        #[cfg(feature = "sql")]
        NodeError::RequestConflict(_) => (StatusCode::CONFLICT, None),
        NodeError::PreconditionFailed(_) => (StatusCode::CONFLICT, None),
        NodeError::SnapshotRequired(_)
        | NodeError::Unavailable(_)
        | NodeError::OutcomeUnknown(_)
        | NodeError::StartupCancelled { .. }
        | NodeError::ResourceExhausted(_)
        | NodeError::ConfigurationTransition { .. }
        | NodeError::Contention(_)
        | NodeError::WinnerLimitExceeded => (StatusCode::SERVICE_UNAVAILABLE, None),
        NodeError::DataRootLocked(_)
        | NodeError::UnsupportedAckMode(_)
        | NodeError::ExecutionProfileMismatch { .. }
        | NodeError::Storage(_)
        | NodeError::Reconciliation(_)
        | NodeError::Invariant(_)
        | NodeError::Fatal(_) => (StatusCode::INTERNAL_SERVER_ERROR, None),
    };
    let classification = error.classification();
    if !matches!(&error, NodeError::Fatal(_)) {
        eprintln!(
            "node request failed (code={}, retryable={}): {}",
            classification.code(),
            classification.retryable(),
            escaped_error_detail(&error)
        );
    }
    client_error_response(
        status,
        classification.code(),
        classification.retryable(),
        node_error_message(status),
        statement_index,
    )
}

fn node_error_message(status: StatusCode) -> &'static str {
    match status {
        StatusCode::BAD_REQUEST => "request could not be processed",
        StatusCode::CONFLICT => "request conflicts with current state",
        StatusCode::SERVICE_UNAVAILABLE => "service is temporarily unavailable",
        _ => "internal server error",
    }
}

const MAX_ESCAPED_ERROR_DETAIL_BYTES: usize = 4 * 1024;
const ESCAPED_ERROR_DETAIL_TRUNCATION_MARKER: &str = "...[truncated]";

fn escaped_error_detail(error: &dyn fmt::Display) -> String {
    let detail = error.to_string();
    let mut escaped = String::with_capacity(detail.len().min(MAX_ESCAPED_ERROR_DETAIL_BYTES));
    for character in detail.chars() {
        let character_start = escaped.len();
        for escaped_character in character.escape_default() {
            if escaped.len()
                + escaped_character.len_utf8()
                + ESCAPED_ERROR_DETAIL_TRUNCATION_MARKER.len()
                > MAX_ESCAPED_ERROR_DETAIL_BYTES
            {
                escaped.truncate(character_start);
                escaped.push_str(ESCAPED_ERROR_DETAIL_TRUNCATION_MARKER);
                return escaped;
            }
            escaped.push(escaped_character);
        }
    }
    escaped
}

#[allow(clippy::result_large_err)]
fn client_json<T>(request: Result<Json<T>, JsonRejection>) -> Result<T, Response> {
    request.map(|Json(request)| request).map_err(|rejection| {
        let status = rejection.status();
        eprintln!("invalid JSON request: {}", escaped_error_detail(&rejection));
        client_json_error_response(status)
    })
}

fn client_json_error_response(status: StatusCode) -> Response {
    let code = if status == StatusCode::PAYLOAD_TOO_LARGE {
        "payload_too_large"
    } else {
        "invalid_json"
    };
    client_error_response(status, code, false, "request body is invalid", None)
}

fn client_task_error(error: tokio::task::JoinError) -> Response {
    eprintln!("request task failed: {}", escaped_error_detail(&error));
    client_error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "task_failed",
        false,
        "request task failed",
        None,
    )
}

fn client_error_response(
    status: StatusCode,
    code: impl Into<String>,
    retryable: bool,
    message: impl Into<String>,
    statement_index: Option<usize>,
) -> Response {
    (
        status,
        Json(ClientErrorResponse {
            code: code.into(),
            retryable,
            message: message.into(),
            statement_index,
        }),
    )
        .into_response()
}

pub fn install_successor_recorder(
    recorder: &RecorderFileStore,
    next_config_id: u64,
    membership: Membership,
    stop: &StopInformation,
) -> Result<rhiza_quepaxa::ConfigurationState, NodeError> {
    if stop.entry.config_id.checked_add(1) != Some(next_config_id) {
        return Err(NodeError::PreconditionFailed(
            "successor identity does not match the Stop proof".into(),
        ));
    }
    recorder
        .install_successor_from_proof(membership, &stop.proof)
        .map_err(|error| NodeError::Reconciliation(error.to_string()))
}

pub fn recover_successor_recorder_after_checkpoint(
    recorder: &RecorderFileStore,
    config: &NodeConfig,
    next_config_id: u64,
    membership: Membership,
    stop: &StopInformation,
) -> Result<rhiza_quepaxa::ConfigurationState, NodeError> {
    let installed = install_successor_recorder(recorder, next_config_id, membership.clone(), stop)?;
    let log = FileLogStore::open_with_configuration(
        config.data_dir.join("consensus/log"),
        &config.cluster_id,
        config.epoch,
        config.log_initial_configuration.clone(),
    )
    .map_err(|error| NodeError::Storage(error.to_string()))?;
    let recovered_configuration = log
        .configuration_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    if !recovered_configuration.is_active() {
        return Ok(installed);
    }
    if recovered_configuration.config_id() != next_config_id
        || recovered_configuration.digest() != membership.digest()
    {
        return Err(NodeError::Reconciliation(
            "recovered successor qlog configuration does not match the target bundle".into(),
        ));
    }
    let tip = log
        .logical_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?
        .tip
        .ok_or_else(|| NodeError::Reconciliation("recovered successor qlog is empty".into()))?;
    recorder
        .recover_successor_activation_from_checkpoint(
            stop.entry.index,
            stop.entry.hash,
            tip.index(),
            tip.hash(),
        )
        .map_err(|error| NodeError::Reconciliation(error.to_string()))
}

fn recorder_error_status(error: &rhiza_quepaxa::Error) -> StatusCode {
    match error {
        rhiza_quepaxa::Error::NoQuorum
        | rhiza_quepaxa::Error::CommandUnavailable
        | rhiza_quepaxa::Error::Io(_)
        | rhiza_quepaxa::Error::RecorderRootLocked(_) => StatusCode::SERVICE_UNAVAILABLE,
        rhiza_quepaxa::Error::Rejected(_) => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn fetch_log_error_status(error: &FetchLogError) -> StatusCode {
    match error {
        FetchLogError::InvalidRequest { .. } => StatusCode::BAD_REQUEST,
        FetchLogError::SnapshotRequired { .. } | FetchLogError::Gap { .. } => StatusCode::CONFLICT,
        FetchLogError::Decode { .. } | FetchLogError::Transport { .. } => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        FetchLogError::InvalidAnchor { .. }
        | FetchLogError::InvalidEntry { .. }
        | FetchLogError::ForeignIdentity { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn runtime_readiness_response(runtime: &NodeRuntime) -> Option<Response> {
    if runtime.is_fatal() {
        Some(client_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "fatal",
            false,
            "node is fatally unavailable",
            None,
        ))
    } else if !runtime.is_ready() {
        Some(client_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            true,
            "node is not ready",
            None,
        ))
    } else {
        None
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "status", content = "body")]
enum FetchLogHttpResponse {
    Fetched(FetchLogResponse),
    Failed(FetchLogError),
}

pub fn catch_up_missing_entries<P: LogPeer + ?Sized>(
    local_last_index: LogIndex,
    local_last_hash: LogHash,
    cluster_id: &str,
    epoch: u64,
    config_id: u64,
    peer: &P,
    max_entries: u32,
) -> Result<Vec<LogEntry>, FetchLogError> {
    if max_entries == 0 {
        return Ok(Vec::new());
    }
    if max_entries > MAX_FETCH_ENTRIES {
        return Err(FetchLogError::InvalidRequest {
            message: format!("max_entries exceeds {MAX_FETCH_ENTRIES}"),
        });
    }
    if cluster_id.is_empty() {
        return Err(FetchLogError::InvalidRequest {
            message: "cluster_id must not be empty".into(),
        });
    }
    let from_index =
        local_last_index
            .checked_add(1)
            .ok_or_else(|| FetchLogError::InvalidRequest {
                message: "local qlog index is exhausted".into(),
            })?;
    let response = peer.fetch_log(FetchLogRequest {
        from_index,
        max_entries,
    })?;
    if response.entries.len() > max_entries as usize {
        return Err(FetchLogError::InvalidRequest {
            message: "peer returned more entries than requested".into(),
        });
    }
    if response.last_index < local_last_index {
        return Err(FetchLogError::Gap {
            expected: local_last_index,
            actual: Some(response.last_index),
        });
    }
    if response.entries.is_empty() && response.last_index >= from_index {
        return Err(FetchLogError::Gap {
            expected: from_index,
            actual: None,
        });
    }
    validate_fetched_entries(
        from_index,
        local_last_hash,
        cluster_id,
        epoch,
        config_id,
        response.entries,
    )
}

fn validate_fetched_entries(
    from_index: LogIndex,
    local_last_hash: LogHash,
    cluster_id: &str,
    epoch: u64,
    config_id: u64,
    entries: Vec<LogEntry>,
) -> Result<Vec<LogEntry>, FetchLogError> {
    validate_fetched_entries_with_configuration(
        from_index,
        local_last_hash,
        cluster_id,
        epoch,
        ConfigurationState::active(config_id, LogHash::ZERO),
        entries,
    )
}

fn validate_fetched_entries_with_configuration(
    from_index: LogIndex,
    local_last_hash: LogHash,
    cluster_id: &str,
    epoch: u64,
    mut configuration_state: ConfigurationState,
    entries: Vec<LogEntry>,
) -> Result<Vec<LogEntry>, FetchLogError> {
    let mut expected = from_index;
    let mut previous_hash = local_last_hash;
    for entry in &entries {
        if entry.index != expected {
            return Err(FetchLogError::Gap {
                expected,
                actual: Some(entry.index),
            });
        }
        if entry.cluster_id != cluster_id || entry.epoch != epoch {
            return Err(FetchLogError::ForeignIdentity { index: entry.index });
        }
        if entry.prev_hash != previous_hash {
            return Err(FetchLogError::InvalidAnchor {
                expected: previous_hash,
                actual: entry.prev_hash,
            });
        }
        if entry.recompute_hash() != entry.hash {
            return Err(FetchLogError::InvalidEntry {
                index: entry.index,
                message: "hash does not match entry contents".into(),
            });
        }
        validate_entry_shape(entry).map_err(|message| FetchLogError::InvalidEntry {
            index: entry.index,
            message,
        })?;
        configuration_state = configuration_state.validate_entry(entry).map_err(|error| {
            FetchLogError::InvalidEntry {
                index: entry.index,
                message: error.to_string(),
            }
        })?;
        expected = expected
            .checked_add(1)
            .ok_or_else(|| FetchLogError::InvalidEntry {
                index: entry.index,
                message: "qlog index is exhausted".into(),
            })?;
        previous_hash = entry.hash;
    }
    Ok(entries)
}

#[derive(Clone, Eq, PartialEq)]
pub struct PeerConfig {
    node_id: String,
    base_url: String,
    log_base_url: String,
    token: String,
}

impl fmt::Debug for PeerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerConfig")
            .field("node_id", &self.node_id)
            .field("base_url", &self.base_url)
            .field("log_base_url", &self.log_base_url)
            .field("token", &"[redacted]")
            .finish()
    }
}

impl PeerConfig {
    pub fn new(
        node_id: impl Into<String>,
        base_url: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let base_url = base_url.into();
        Self::new_with_log_url(node_id, base_url.clone(), base_url, token)
    }

    pub fn new_with_log_url(
        node_id: impl Into<String>,
        base_url: impl Into<String>,
        log_base_url: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let node_id = node_id.into();
        if !valid_nonblank_header_value(&node_id) {
            return Err(ConfigError::EmptyPeerNodeId);
        }
        let base_url = validate_peer_base_url(base_url.into())?;
        let log_base_url = validate_peer_base_url(log_base_url.into())?;
        let token = token.into();
        if !valid_auth_token(&token) {
            return Err(ConfigError::EmptyPeerToken);
        }
        Ok(Self {
            node_id,
            base_url,
            log_base_url,
            token,
        })
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn log_base_url(&self) -> &str {
        &self.log_base_url
    }

    pub fn token(&self) -> &str {
        &self.token
    }
}

fn validate_peer_base_url(url: String) -> Result<String, ConfigError> {
    let url = url.trim_end_matches('/').to_string();
    if url.trim().is_empty() {
        return Err(ConfigError::EmptyPeerBaseUrl);
    }
    let parsed =
        reqwest::Url::parse(&url).map_err(|_| ConfigError::InvalidPeerBaseUrl(url.clone()))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(ConfigError::InvalidPeerBaseUrl(url));
    }
    Ok(url)
}

pub(crate) fn valid_nonblank_header_value(value: &str) -> bool {
    !value.trim().is_empty()
        && axum::http::HeaderValue::try_from(value).is_ok_and(|value| value.to_str().is_ok())
}

pub(crate) fn valid_auth_token(value: &str) -> bool {
    valid_nonblank_header_value(value) && !value.chars().any(char::is_whitespace)
}

#[derive(Clone, Eq, PartialEq)]
pub struct NodeConfig {
    cluster_id_source: String,
    logical_cluster_id: String,
    cluster_id: String,
    node_id: String,
    data_dir: PathBuf,
    epoch: u64,
    membership: Membership,
    log_initial_configuration: ConfigurationState,
    configuration_state: ConfigurationState,
    predecessor_stop_entry: Option<LogEntry>,
    recovery_generation: u64,
    peers: Vec<PeerConfig>,
    client_token: String,
    read_consistency: ReadConsistency,
    ack_mode: AckMode,
    writer_batch_max: usize,
    writer_batch_window: Duration,
    execution_profile: ExecutionProfile,
    #[cfg(feature = "sql")]
    sql_write_profiler: Option<SqlWriteProfiler>,
    #[cfg(feature = "sql")]
    sql_group_commit_queue_capacity: usize,
    #[cfg(feature = "tuner")]
    tuner_telemetry: Option<TunerTelemetry>,
}

impl fmt::Debug for NodeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("NodeConfig");
        debug
            .field("cluster_id_source", &self.cluster_id_source)
            .field("logical_cluster_id", &self.logical_cluster_id)
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.node_id)
            .field("data_dir", &self.data_dir)
            .field("epoch", &self.epoch)
            .field("membership", &self.membership.members())
            .field("log_initial_configuration", &self.log_initial_configuration)
            .field("configuration_state", &self.configuration_state)
            .field(
                "predecessor_stop_entry",
                &self
                    .predecessor_stop_entry
                    .as_ref()
                    .map(|entry| (entry.index, entry.hash)),
            )
            .field("recovery_generation", &self.recovery_generation)
            .field("peers", &self.peers)
            .field("client_token", &"[redacted]")
            .field("read_consistency", &self.read_consistency)
            .field("ack_mode", &self.ack_mode)
            .field("writer_batch_max", &self.writer_batch_max)
            .field("writer_batch_window", &self.writer_batch_window)
            .field("execution_profile", &self.execution_profile);
        #[cfg(feature = "sql")]
        debug.field(
            "sql_write_profiler",
            &self.sql_write_profiler.as_ref().map(|_| "installed"),
        );
        #[cfg(feature = "sql")]
        debug.field(
            "sql_group_commit_queue_capacity",
            &self.sql_group_commit_queue_capacity,
        );
        debug.finish()
    }
}

impl NodeConfig {
    pub fn new<P>(
        cluster_id: impl Into<String>,
        node_id: impl Into<String>,
        data_dir: PathBuf,
        epoch: u64,
        config_id: u64,
        peers: P,
        client_token: impl Into<String>,
    ) -> Result<Self, ConfigError>
    where
        P: Into<Vec<PeerConfig>>,
    {
        let peers = peers.into();
        let membership = membership_from_peers(&peers)?;
        let configuration_state = ConfigurationState::active(config_id, membership.digest());
        Self::new_with_configuration(
            cluster_id,
            node_id,
            data_dir,
            epoch,
            membership,
            configuration_state,
            peers,
            client_token,
        )
    }

    pub fn new_embedded<I, S>(
        cluster_id: impl Into<String>,
        node_id: impl Into<String>,
        data_dir: PathBuf,
        epoch: u64,
        config_id: u64,
        members: I,
    ) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let cluster_id = cluster_id.into();
        let node_id = node_id.into();
        validate_node_identity(&cluster_id, &node_id, &data_dir, epoch, config_id)?;
        let membership = membership_from_node_ids(members.into_iter().map(Into::into).collect())?;
        if !membership.contains(&node_id) {
            return Err(ConfigError::LocalNodeMissing);
        }
        let configuration_state = ConfigurationState::active(config_id, membership.digest());
        Self::from_validated_parts(
            cluster_id,
            node_id,
            data_dir,
            epoch,
            membership,
            configuration_state,
            Vec::new(),
            String::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_configuration<P>(
        cluster_id: impl Into<String>,
        node_id: impl Into<String>,
        data_dir: PathBuf,
        epoch: u64,
        membership: Membership,
        configuration_state: ConfigurationState,
        peers: P,
        client_token: impl Into<String>,
    ) -> Result<Self, ConfigError>
    where
        P: Into<Vec<PeerConfig>>,
    {
        let cluster_id = cluster_id.into();
        let node_id = node_id.into();
        let client_token = client_token.into();
        let peers = peers.into();

        validate_node_identity(
            &cluster_id,
            &node_id,
            &data_dir,
            epoch,
            configuration_state.config_id(),
        )?;
        if !(3..=7).contains(&peers.len()) {
            return Err(ConfigError::InvalidPeerCount(peers.len()));
        }
        let mut peer_ids = HashSet::with_capacity(peers.len());
        let mut peer_tokens = HashSet::with_capacity(peers.len());
        for peer in &peers {
            if !peer_ids.insert(peer.node_id.clone()) {
                return Err(ConfigError::DuplicatePeerNodeId(peer.node_id.clone()));
            }
            if !peer_tokens.insert(peer.token.as_str()) {
                return Err(ConfigError::DuplicatePeerToken);
            }
        }
        if !peer_ids.contains(&node_id) {
            return Err(ConfigError::LocalNodeMissing);
        }
        if peer_ids.len() != membership.members().len()
            || membership
                .members()
                .iter()
                .any(|member| !peer_ids.contains(member))
        {
            return Err(ConfigError::PeerMembershipMismatch);
        }
        if configuration_state.is_active() && configuration_state.digest() != membership.digest() {
            return Err(ConfigError::PeerMembershipMismatch);
        }
        if !valid_auth_token(&client_token) {
            return Err(ConfigError::EmptyClientToken);
        }
        if peer_tokens.contains(client_token.as_str()) {
            return Err(ConfigError::ClientTokenConflictsWithPeer);
        }

        Self::from_validated_parts(
            cluster_id,
            node_id,
            data_dir,
            epoch,
            membership,
            configuration_state,
            peers,
            client_token,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_validated_parts(
        cluster_id: String,
        node_id: String,
        data_dir: PathBuf,
        epoch: u64,
        membership: Membership,
        configuration_state: ConfigurationState,
        peers: Vec<PeerConfig>,
        client_token: String,
    ) -> Result<Self, ConfigError> {
        let log_initial_configuration = ConfigurationState::active(
            configuration_state.config_id(),
            configuration_state.digest(),
        );
        let execution_profile =
            canonical_cluster_profile(&cluster_id).unwrap_or(ExecutionProfile::Sqlite);
        let logical_cluster_id = ["rhiza:sql:", "rhiza:graph:", "rhiza:kv:"]
            .into_iter()
            .find_map(|prefix| cluster_id.strip_prefix(prefix))
            .unwrap_or(&cluster_id)
            .to_owned();
        let effective_cluster_id = effective_cluster_id(execution_profile, &cluster_id)?;
        Ok(Self {
            cluster_id_source: cluster_id,
            cluster_id: effective_cluster_id,
            logical_cluster_id,
            node_id,
            data_dir,
            epoch,
            membership,
            log_initial_configuration,
            configuration_state,
            predecessor_stop_entry: None,
            recovery_generation: 1,
            peers,
            client_token,
            read_consistency: ReadConsistency::ReadBarrier,
            ack_mode: AckMode::HaFirst,
            writer_batch_max: DEFAULT_WRITER_BATCH_MAX,
            writer_batch_window: DEFAULT_WRITER_BATCH_WINDOW,
            execution_profile,
            #[cfg(feature = "sql")]
            sql_write_profiler: None,
            #[cfg(feature = "sql")]
            sql_group_commit_queue_capacity: DEFAULT_SQL_GROUP_COMMIT_QUEUE_CAPACITY,
            #[cfg(feature = "tuner")]
            tuner_telemetry: None,
        })
    }

    pub fn with_execution_profile(
        mut self,
        execution_profile: ExecutionProfile,
    ) -> Result<Self, ConfigError> {
        self.cluster_id = effective_cluster_id(execution_profile, &self.cluster_id_source)?;
        self.execution_profile = execution_profile;
        Ok(self)
    }

    pub fn with_read_consistency(mut self, read_consistency: ReadConsistency) -> Self {
        self.read_consistency = read_consistency;
        self
    }

    #[cfg(feature = "sql")]
    pub fn with_sql_write_profiler(mut self, profiler: SqlWriteProfiler) -> Self {
        self.sql_write_profiler = Some(profiler);
        self
    }

    #[cfg(feature = "tuner")]
    pub fn with_tuner_telemetry(mut self, telemetry: TunerTelemetry) -> Self {
        self.tuner_telemetry = Some(telemetry);
        self
    }

    #[cfg(feature = "sql")]
    pub fn with_sql_group_commit_queue_capacity(
        mut self,
        capacity: usize,
    ) -> Result<Self, ConfigError> {
        if !(1..=MAX_SQL_GROUP_COMMIT_QUEUE_CAPACITY).contains(&capacity) {
            return Err(ConfigError::InvalidSqlGroupCommitQueueCapacity(capacity));
        }
        self.sql_group_commit_queue_capacity = capacity;
        Ok(self)
    }

    pub fn with_ack_mode(mut self, ack_mode: AckMode) -> Self {
        self.ack_mode = ack_mode;
        self
    }

    pub fn with_writer_batching(
        mut self,
        max: usize,
        window: Duration,
    ) -> Result<Self, ConfigError> {
        if max == 0 || max > MAX_WRITE_BATCH_MEMBERS {
            return Err(ConfigError::InvalidWriterBatchMax(max));
        }
        if window.is_zero() || window >= CLIENT_WRITE_WAIT_TIMEOUT {
            return Err(ConfigError::InvalidWriterBatchWindow);
        }
        self.writer_batch_max = max;
        self.writer_batch_window = window;
        Ok(self)
    }

    pub fn with_log_initial_configuration(mut self, configuration: ConfigurationState) -> Self {
        self.log_initial_configuration = configuration;
        self
    }

    pub fn with_predecessor_stop_entry(mut self, entry: LogEntry) -> Self {
        self.predecessor_stop_entry = Some(entry);
        self
    }

    /// Binds an unactivated successor draft to the exact predecessor Stop it will adopt.
    pub fn bind_predecessor_stop(
        mut self,
        predecessor_membership: &Membership,
        entry: LogEntry,
    ) -> Result<Self, ConfigError> {
        let target_config_id = self.config_id();
        if entry.cluster_id != self.cluster_id
            || entry.epoch != self.epoch
            || entry.config_id.checked_add(1) != Some(target_config_id)
            || entry.recompute_hash() != entry.hash
        {
            return Err(ConfigError::InvalidPredecessorTransition(
                "Stop identity does not match the successor draft".into(),
            ));
        }
        let ConfigChange::BoundStop { successor } =
            ConfigChange::recognize_parts(entry.entry_type, &entry.payload).map_err(|_| {
                ConfigError::InvalidPredecessorTransition(
                    "predecessor entry is not a bound Stop".into(),
                )
            })?
        else {
            return Err(ConfigError::InvalidPredecessorTransition(
                "predecessor entry is not a bound Stop".into(),
            ));
        };
        if successor.cluster_id() != self.cluster_id
            || successor.predecessor_config_id() != entry.config_id
            || successor.predecessor_config_digest() != predecessor_membership.digest()
            || successor.config_id() != target_config_id
            || successor.digest() != self.membership.digest()
            || successor.members() != self.membership.members()
        {
            return Err(ConfigError::InvalidPredecessorTransition(
                "Stop successor descriptor does not match the successor draft".into(),
            ));
        }
        let predecessor =
            ConfigurationState::active(entry.config_id, predecessor_membership.digest());
        let stopped = predecessor
            .validate_entry(&entry)
            .map_err(|error| ConfigError::InvalidPredecessorTransition(error.to_string()))?;
        self.log_initial_configuration = predecessor;
        self.configuration_state = stopped;
        self.predecessor_stop_entry = Some(entry);
        Ok(self)
    }

    pub fn with_recovery_generation(
        mut self,
        recovery_generation: u64,
    ) -> Result<Self, ConfigError> {
        validate_recovery_generation(recovery_generation)?;
        self.recovery_generation = recovery_generation;
        Ok(self)
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn logical_cluster_id(&self) -> &str {
        &self.logical_cluster_id
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn config_id(&self) -> u64 {
        self.configuration_state.config_id()
    }

    pub const fn recovery_generation(&self) -> u64 {
        self.recovery_generation
    }

    pub fn peers(&self) -> &[PeerConfig] {
        &self.peers
    }

    pub const fn membership(&self) -> &Membership {
        &self.membership
    }

    pub const fn configuration_state(&self) -> &ConfigurationState {
        &self.configuration_state
    }

    pub const fn log_initial_configuration(&self) -> &ConfigurationState {
        &self.log_initial_configuration
    }

    pub fn client_token(&self) -> &str {
        &self.client_token
    }

    pub const fn read_consistency(&self) -> ReadConsistency {
        self.read_consistency
    }

    pub const fn ack_mode(&self) -> AckMode {
        self.ack_mode
    }

    pub const fn writer_batch_max(&self) -> usize {
        self.writer_batch_max
    }

    pub const fn writer_batch_window(&self) -> Duration {
        self.writer_batch_window
    }

    pub const fn execution_profile(&self) -> ExecutionProfile {
        self.execution_profile
    }

    #[cfg(feature = "sql")]
    pub const fn sql_write_profiler(&self) -> Option<&SqlWriteProfiler> {
        self.sql_write_profiler.as_ref()
    }

    #[cfg(feature = "sql")]
    pub const fn sql_group_commit_queue_capacity(&self) -> usize {
        self.sql_group_commit_queue_capacity
    }

    #[cfg(feature = "tuner")]
    pub const fn tuner_telemetry(&self) -> Option<&TunerTelemetry> {
        self.tuner_telemetry.as_ref()
    }

    #[cfg(feature = "tuner")]
    pub fn tuner_identity(&self) -> rhiza_tuner::Identity {
        rhiza_tuner::Identity {
            cluster_id: self.cluster_id.clone(),
            epoch: self.epoch,
            config_id: self.configuration_state.config_id(),
            membership_digest: *self.membership.digest().as_bytes(),
            recovery_generation: self.recovery_generation,
        }
    }

    #[cfg(feature = "tuner")]
    pub fn eligible_proposers(&self) -> Vec<rhiza_tuner::NodeId> {
        self.membership.members().to_vec()
    }
}

fn validate_node_identity(
    cluster_id: &str,
    node_id: &str,
    data_dir: &Path,
    epoch: u64,
    config_id: u64,
) -> Result<(), ConfigError> {
    if cluster_id.trim().is_empty() {
        return Err(ConfigError::EmptyClusterId);
    }
    if node_id.trim().is_empty() {
        return Err(ConfigError::EmptyNodeId);
    }
    if data_dir.as_os_str().is_empty() {
        return Err(ConfigError::EmptyDataDir);
    }
    if epoch == 0 {
        return Err(ConfigError::InvalidEpoch);
    }
    if config_id == 0 {
        return Err(ConfigError::InvalidConfigId);
    }
    Ok(())
}

fn membership_from_node_ids(members: Vec<String>) -> Result<Membership, ConfigError> {
    if !(3..=7).contains(&members.len()) {
        return Err(ConfigError::InvalidPeerCount(members.len()));
    }
    Membership::from_voters(members.clone()).map_err(|error| match error {
        rhiza_quepaxa::Error::DuplicateRecorderIdentity => {
            let duplicate = members
                .iter()
                .find(|candidate| {
                    members
                        .iter()
                        .filter(|member| *member == *candidate)
                        .count()
                        > 1
                })
                .cloned()
                .unwrap_or_default();
            ConfigError::DuplicatePeerNodeId(duplicate)
        }
        rhiza_quepaxa::Error::EmptyRecorderIdentity => ConfigError::EmptyPeerNodeId,
        _ => ConfigError::InvalidPeerCount(members.len()),
    })
}

fn membership_from_peers(peers: &[PeerConfig]) -> Result<Membership, ConfigError> {
    membership_from_node_ids(peers.iter().map(|peer| peer.node_id.clone()).collect())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeError {
    UnsupportedAckMode(AckMode),
    ExecutionProfileMismatch {
        expected: ExecutionProfile,
        actual: ExecutionProfile,
    },
    DataRootLocked(PathBuf),
    SnapshotRequired(Box<RecoveryAnchor>),
    Storage(String),
    Reconciliation(String),
    OutcomeUnknown(String),
    Invariant(String),
    Unavailable(String),
    StartupCancelled {
        token: StartupCancellationToken,
        stage: String,
    },
    ResourceExhausted(String),
    ConfigurationTransition {
        state: Box<ConfigurationState>,
    },
    Contention(String),
    WinnerLimitExceeded,
    #[cfg(feature = "sql")]
    RequestConflict(RequestConflict),
    InvalidRequest(String),
    #[cfg(feature = "sql")]
    InvalidSqlStatement {
        statement_index: usize,
        message: String,
    },
    PreconditionFailed(String),
    Fatal(String),
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedAckMode(mode) => {
                write!(
                    f,
                    "ack mode {mode:?} is unsupported without synchronous archive"
                )
            }
            Self::ExecutionProfileMismatch { expected, actual } => write!(
                f,
                "execution profile mismatch: expected {expected}, got {actual}"
            ),
            Self::DataRootLocked(path) => {
                write!(f, "node data root is already owned: {}", path.display())
            }
            Self::SnapshotRequired(anchor) => write!(
                f,
                "snapshot restore required at qlog anchor {}",
                anchor.compacted().index()
            ),
            Self::Storage(message) => write!(f, "node storage failed: {message}"),
            Self::Reconciliation(message) => write!(f, "node reconciliation failed: {message}"),
            Self::OutcomeUnknown(message) => write!(f, "write outcome is unknown: {message}"),
            Self::Invariant(message) => write!(f, "node invariant failed: {message}"),
            Self::Unavailable(message) => write!(f, "node unavailable: {message}"),
            Self::StartupCancelled { stage, .. } => {
                write!(f, "node unavailable: startup cancelled {stage}")
            }
            Self::ResourceExhausted(message) => {
                write!(f, "node query resources exhausted: {message}")
            }
            Self::ConfigurationTransition { state } => write!(
                f,
                "node unavailable during configuration transition: {state:?}"
            ),
            Self::Contention(message) => write!(f, "node contention: {message}"),
            Self::WinnerLimitExceeded => write!(f, "foreign winner retry limit exceeded"),
            #[cfg(feature = "sql")]
            Self::RequestConflict(conflict) => conflict.fmt(f),
            Self::InvalidRequest(message) => write!(f, "invalid request: {message}"),
            #[cfg(feature = "sql")]
            Self::InvalidSqlStatement {
                statement_index,
                message,
            } => write!(
                f,
                "invalid SQL statement at index {statement_index}: {message}"
            ),
            Self::PreconditionFailed(message) => write!(f, "precondition failed: {message}"),
            Self::Fatal(message) => write!(f, "node is fatally unavailable: {message}"),
        }
    }
}

impl std::error::Error for NodeError {}

impl NodeError {
    pub fn classification(&self) -> ErrorClassification {
        let (code, retryable) = match self {
            Self::InvalidRequest(_) => ("invalid_request", false),
            #[cfg(feature = "sql")]
            Self::InvalidSqlStatement { .. } => ("invalid_request", false),
            #[cfg(feature = "sql")]
            Self::RequestConflict(_) => ("request_conflict", false),
            Self::PreconditionFailed(_) => ("precondition_failed", false),
            Self::SnapshotRequired(_) => ("snapshot_required", false),
            Self::Unavailable(_) => ("unavailable", true),
            Self::OutcomeUnknown(_) => ("write_outcome_unknown", true),
            Self::StartupCancelled { .. } => ("unavailable", true),
            Self::ResourceExhausted(_) => ("resource_exhausted", true),
            Self::ConfigurationTransition { .. } => ("configuration_transition", true),
            Self::Contention(_) => ("contention", true),
            Self::WinnerLimitExceeded => ("winner_limit_exceeded", true),
            Self::DataRootLocked(_) => ("data_root_locked", false),
            Self::UnsupportedAckMode(_) => ("unsupported_ack_mode", false),
            Self::ExecutionProfileMismatch { .. } => ("execution_profile_mismatch", false),
            Self::Storage(_) => ("storage_error", false),
            Self::Reconciliation(_) => ("reconciliation_error", false),
            Self::Invariant(_) => ("invariant_violation", false),
            Self::Fatal(_) => ("fatal", false),
        };
        ErrorClassification::from_server_code(code, retryable)
    }
}

pub type RuntimeError = NodeError;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeConfigurationStatus {
    Active,
    Stopped,
    AwaitingActivation,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct NodeStatus {
    pub ready: bool,
    pub configuration_status: RuntimeConfigurationStatus,
    pub configuration_state: ConfigurationState,
    pub stop_anchor: Option<rhiza_core::LogAnchor>,
    pub active_config_id: u64,
    pub active_membership_digest: LogHash,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StopInformation {
    pub entry: LogEntry,
    pub proof: DecisionProof,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct WriteRequest {
    pub request_id: String,
    pub key: String,
    pub value: String,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WriteResponse {
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientErrorResponse {
    pub code: String,
    pub retryable: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statement_index: Option<usize>,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReadRequest {
    pub key: String,
    pub consistency: Option<ReadConsistency>,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ReadResponse {
    pub value: Option<String>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlExecuteRequest {
    pub request_id: String,
    pub statements: Vec<SqlStatement>,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlExecuteResponse {
    pub applied_index: LogIndex,
    pub hash: LogHash,
    pub results: Vec<SqlStatementResult>,
}

#[cfg(feature = "sql")]
impl From<WriteResponse> for SqlExecuteResponse {
    fn from(response: WriteResponse) -> Self {
        sql_execute_response(response, None)
    }
}

#[cfg(feature = "sql")]
fn sql_execute_response(
    response: WriteResponse,
    result: Option<SqlCommandResult>,
) -> SqlExecuteResponse {
    let results = result
        .map(|result| {
            result
                .statement_results
                .into_iter()
                .enumerate()
                .map(|(statement_index, result)| SqlStatementResult {
                    statement_index,
                    rows_affected: result.rows_affected,
                    returning: result.returning,
                })
                .collect()
        })
        .unwrap_or_default();
    SqlExecuteResponse {
        applied_index: response.applied_index,
        hash: response.hash,
        results,
    }
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SqlStatementResult {
    pub statement_index: usize,
    pub rows_affected: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub returning: Option<SqlQueryResult>,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlQueryRequest {
    pub statement: SqlStatement,
    pub consistency: Option<ReadConsistency>,
    pub max_rows: Option<u32>,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SqlQueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum GraphValueDto {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(String),
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum GraphQueryParameterDto {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(String),
    List(Vec<Self>),
    Struct(BTreeMap<String, Self>),
}

#[cfg(feature = "graph")]
impl TryFrom<GraphQueryParameterDto> for GraphParameterValue {
    type Error = NodeError;

    fn try_from(value: GraphQueryParameterDto) -> Result<Self, Self::Error> {
        Ok(match value {
            GraphQueryParameterDto::Null => Self::Null,
            GraphQueryParameterDto::Bool(value) => Self::Bool(value),
            GraphQueryParameterDto::I64(value) => Self::I64(value),
            GraphQueryParameterDto::U64(value) => Self::U64(value),
            GraphQueryParameterDto::F64(value) => Self::F64(
                CanonicalF64::new(value)
                    .map_err(|error| NodeError::InvalidRequest(error.to_string()))?,
            ),
            GraphQueryParameterDto::String(value) => Self::String(value),
            GraphQueryParameterDto::Bytes(value) => {
                Self::Bytes(decode_base64("graph parameter bytes", &value)?)
            }
            GraphQueryParameterDto::List(values) => Self::List(
                values
                    .into_iter()
                    .map(Self::try_from)
                    .collect::<Result<_, _>>()?,
            ),
            GraphQueryParameterDto::Struct(values) => Self::Struct(
                values
                    .into_iter()
                    .map(|(name, value)| Self::try_from(value).map(|value| (name, value)))
                    .collect::<Result<_, _>>()?,
            ),
        })
    }
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphQueryStatementDto {
    pub cypher: String,
    #[serde(default)]
    pub parameters: BTreeMap<String, GraphQueryParameterDto>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphQueryRequest {
    pub statement: GraphQueryStatementDto,
    pub consistency: Option<ReadConsistency>,
    pub max_rows: Option<u32>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphInternalIdDto {
    pub offset: u64,
    pub table_id: u64,
}

#[cfg(feature = "graph")]
impl From<GraphInternalId> for GraphInternalIdDto {
    fn from(value: GraphInternalId) -> Self {
        Self {
            offset: value.offset,
            table_id: value.table_id,
        }
    }
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphNamedValueDto {
    pub name: String,
    pub value: GraphResultValueDto,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphNodeDto {
    pub id: GraphInternalIdDto,
    pub label: String,
    pub properties: Vec<GraphNamedValueDto>,
}

#[cfg(feature = "graph")]
impl From<GraphNode> for GraphNodeDto {
    fn from(value: GraphNode) -> Self {
        Self {
            id: value.id.into(),
            label: value.label,
            properties: named_graph_values(value.properties),
        }
    }
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphRelDto {
    pub src: GraphInternalIdDto,
    pub dst: GraphInternalIdDto,
    pub label: String,
    pub properties: Vec<GraphNamedValueDto>,
}

#[cfg(feature = "graph")]
impl From<GraphRel> for GraphRelDto {
    fn from(value: GraphRel) -> Self {
        Self {
            src: value.src.into(),
            dst: value.dst.into(),
            label: value.label,
            properties: named_graph_values(value.properties),
        }
    }
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphRecursiveRelDto {
    pub nodes: Vec<GraphNodeDto>,
    pub rels: Vec<GraphRelDto>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphMapEntryDto {
    pub key: GraphResultValueDto,
    pub value: GraphResultValueDto,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphNamedLogicalTypeDto {
    pub name: String,
    pub logical_type: GraphLogicalTypeDto,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphArrayTypeDto {
    pub element_type: Box<GraphLogicalTypeDto>,
    pub length: u64,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphMapTypeDto {
    pub key_type: Box<GraphLogicalTypeDto>,
    pub value_type: Box<GraphLogicalTypeDto>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphDecimalTypeDto {
    pub precision: u32,
    pub scale: u32,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum GraphLogicalTypeDto {
    Any,
    Bool,
    Serial,
    I64,
    I32,
    I16,
    I8,
    U64,
    U32,
    U16,
    U8,
    I128,
    F64,
    F32,
    Date,
    Interval,
    Timestamp,
    TimestampTz,
    TimestampNs,
    TimestampMs,
    TimestampSec,
    InternalId,
    String,
    Json,
    Bytes,
    List(Box<Self>),
    Array(GraphArrayTypeDto),
    Struct(Vec<GraphNamedLogicalTypeDto>),
    Node,
    Rel,
    RecursiveRel,
    Map(GraphMapTypeDto),
    Union(Vec<GraphNamedLogicalTypeDto>),
    Uuid,
    Decimal(GraphDecimalTypeDto),
}

#[cfg(feature = "graph")]
impl From<GraphLogicalType> for GraphLogicalTypeDto {
    fn from(value: GraphLogicalType) -> Self {
        match value {
            GraphLogicalType::Any => Self::Any,
            GraphLogicalType::Bool => Self::Bool,
            GraphLogicalType::Serial => Self::Serial,
            GraphLogicalType::I64 => Self::I64,
            GraphLogicalType::I32 => Self::I32,
            GraphLogicalType::I16 => Self::I16,
            GraphLogicalType::I8 => Self::I8,
            GraphLogicalType::U64 => Self::U64,
            GraphLogicalType::U32 => Self::U32,
            GraphLogicalType::U16 => Self::U16,
            GraphLogicalType::U8 => Self::U8,
            GraphLogicalType::I128 => Self::I128,
            GraphLogicalType::F64 => Self::F64,
            GraphLogicalType::F32 => Self::F32,
            GraphLogicalType::Date => Self::Date,
            GraphLogicalType::Interval => Self::Interval,
            GraphLogicalType::Timestamp => Self::Timestamp,
            GraphLogicalType::TimestampTz => Self::TimestampTz,
            GraphLogicalType::TimestampNs => Self::TimestampNs,
            GraphLogicalType::TimestampMs => Self::TimestampMs,
            GraphLogicalType::TimestampSec => Self::TimestampSec,
            GraphLogicalType::InternalId => Self::InternalId,
            GraphLogicalType::String => Self::String,
            GraphLogicalType::Json => Self::Json,
            GraphLogicalType::Bytes => Self::Bytes,
            GraphLogicalType::List(element_type) => Self::List(Box::new((*element_type).into())),
            GraphLogicalType::Array {
                element_type,
                length,
            } => Self::Array(GraphArrayTypeDto {
                element_type: Box::new((*element_type).into()),
                length,
            }),
            GraphLogicalType::Struct(fields) => Self::Struct(named_graph_logical_types(fields)),
            GraphLogicalType::Node => Self::Node,
            GraphLogicalType::Rel => Self::Rel,
            GraphLogicalType::RecursiveRel => Self::RecursiveRel,
            GraphLogicalType::Map {
                key_type,
                value_type,
            } => Self::Map(GraphMapTypeDto {
                key_type: Box::new((*key_type).into()),
                value_type: Box::new((*value_type).into()),
            }),
            GraphLogicalType::Union(types) => Self::Union(named_graph_logical_types(types)),
            GraphLogicalType::Uuid => Self::Uuid,
            GraphLogicalType::Decimal { precision, scale } => {
                Self::Decimal(GraphDecimalTypeDto { precision, scale })
            }
        }
    }
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphCollectionValueDto {
    pub element_type: GraphLogicalTypeDto,
    pub values: Vec<GraphResultValueDto>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphMapValueDto {
    pub key_type: GraphLogicalTypeDto,
    pub value_type: GraphLogicalTypeDto,
    pub entries: Vec<GraphMapEntryDto>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphUnionValueDto {
    pub variants: Vec<GraphNamedLogicalTypeDto>,
    pub value: Box<GraphResultValueDto>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum GraphResultValueDto {
    Null(GraphLogicalTypeDto),
    Bool(bool),
    I64(i64),
    I32(i32),
    I16(i16),
    I8(i8),
    U64(u64),
    U32(u32),
    U16(u16),
    U8(u8),
    I128(String),
    F64(f64),
    F32(String),
    Date(String),
    Interval(String),
    Timestamp(String),
    TimestampTz(String),
    TimestampNs(String),
    TimestampMs(String),
    TimestampSec(String),
    InternalId(GraphInternalIdDto),
    String(String),
    Json(String),
    Bytes(String),
    List(GraphCollectionValueDto),
    Array(GraphCollectionValueDto),
    Struct(Vec<GraphNamedValueDto>),
    Node(GraphNodeDto),
    Rel(GraphRelDto),
    RecursiveRel(GraphRecursiveRelDto),
    Map(GraphMapValueDto),
    Union(GraphUnionValueDto),
    Uuid(String),
    Decimal(String),
}

#[cfg(feature = "graph")]
impl From<GraphResultValue> for GraphResultValueDto {
    fn from(value: GraphResultValue) -> Self {
        match value {
            GraphResultValue::Null(value) => Self::Null(value.into()),
            GraphResultValue::Bool(value) => Self::Bool(value),
            GraphResultValue::I64(value) => Self::I64(value),
            GraphResultValue::I32(value) => Self::I32(value),
            GraphResultValue::I16(value) => Self::I16(value),
            GraphResultValue::I8(value) => Self::I8(value),
            GraphResultValue::U64(value) => Self::U64(value),
            GraphResultValue::U32(value) => Self::U32(value),
            GraphResultValue::U16(value) => Self::U16(value),
            GraphResultValue::U8(value) => Self::U8(value),
            GraphResultValue::I128(value) => Self::I128(value),
            GraphResultValue::F64(value) => Self::F64(value.get()),
            GraphResultValue::F32(value) => Self::F32(value),
            GraphResultValue::Date(value) => Self::Date(value),
            GraphResultValue::Interval(value) => Self::Interval(value),
            GraphResultValue::Timestamp(value) => Self::Timestamp(value),
            GraphResultValue::TimestampTz(value) => Self::TimestampTz(value),
            GraphResultValue::TimestampNs(value) => Self::TimestampNs(value),
            GraphResultValue::TimestampMs(value) => Self::TimestampMs(value),
            GraphResultValue::TimestampSec(value) => Self::TimestampSec(value),
            GraphResultValue::InternalId(value) => Self::InternalId(value.into()),
            GraphResultValue::String(value) => Self::String(value),
            GraphResultValue::Json(value) => Self::Json(value),
            GraphResultValue::Bytes(value) => Self::Bytes(encode_base64(&value)),
            GraphResultValue::List {
                element_type,
                values,
            } => Self::List(GraphCollectionValueDto {
                element_type: element_type.into(),
                values: values.into_iter().map(Self::from).collect(),
            }),
            GraphResultValue::Array {
                element_type,
                values,
            } => Self::Array(GraphCollectionValueDto {
                element_type: element_type.into(),
                values: values.into_iter().map(Self::from).collect(),
            }),
            GraphResultValue::Struct(values) => Self::Struct(named_graph_values(values)),
            GraphResultValue::Node(value) => Self::Node(value.into()),
            GraphResultValue::Rel(value) => Self::Rel(value.into()),
            GraphResultValue::RecursiveRel { nodes, rels } => {
                Self::RecursiveRel(GraphRecursiveRelDto {
                    nodes: nodes.into_iter().map(Into::into).collect(),
                    rels: rels.into_iter().map(Into::into).collect(),
                })
            }
            GraphResultValue::Map {
                key_type,
                value_type,
                entries,
            } => Self::Map(GraphMapValueDto {
                key_type: key_type.into(),
                value_type: value_type.into(),
                entries: entries
                    .into_iter()
                    .map(|(key, value)| GraphMapEntryDto {
                        key: key.into(),
                        value: value.into(),
                    })
                    .collect(),
            }),
            GraphResultValue::Union { variants, value } => Self::Union(GraphUnionValueDto {
                variants: named_graph_logical_types(variants),
                value: Box::new(Self::from(*value)),
            }),
            GraphResultValue::Uuid(value) => Self::Uuid(value),
            GraphResultValue::Decimal(value) => Self::Decimal(value),
        }
    }
}

#[cfg(feature = "graph")]
fn named_graph_values(values: Vec<(String, GraphResultValue)>) -> Vec<GraphNamedValueDto> {
    values
        .into_iter()
        .map(|(name, value)| GraphNamedValueDto {
            name,
            value: value.into(),
        })
        .collect()
}

#[cfg(feature = "graph")]
fn named_graph_logical_types(
    values: Vec<(String, GraphLogicalType)>,
) -> Vec<GraphNamedLogicalTypeDto> {
    values
        .into_iter()
        .map(|(name, logical_type)| GraphNamedLogicalTypeDto {
            name,
            logical_type: logical_type.into(),
        })
        .collect()
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphColumnDto {
    pub name: String,
    pub logical_type: GraphLogicalTypeDto,
}

#[cfg(feature = "graph")]
impl From<GraphColumn> for GraphColumnDto {
    fn from(value: GraphColumn) -> Self {
        Self {
            name: value.name,
            logical_type: value.logical_type.into(),
        }
    }
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphQueryResponse {
    pub columns: Vec<GraphColumnDto>,
    pub rows: Vec<Vec<GraphResultValueDto>>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[cfg(feature = "graph")]
impl From<GraphQueryResult> for GraphQueryResponse {
    fn from(value: GraphQueryResult) -> Self {
        Self {
            columns: value.columns.into_iter().map(Into::into).collect(),
            rows: value
                .rows
                .into_iter()
                .map(|row| row.into_iter().map(Into::into).collect())
                .collect(),
            applied_index: value.applied_index,
            hash: value.hash,
        }
    }
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphPutDocumentRequest {
    pub request_id: String,
    pub id: String,
    pub value: GraphValueDto,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphDeleteDocumentRequest {
    pub request_id: String,
    pub id: String,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphGetDocumentRequest {
    pub id: String,
    pub consistency: Option<ReadConsistency>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum GraphMutationResultDto {
    PutDocument { created: bool },
    DeleteDocument { existed: bool },
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphMutationResponse {
    pub applied_index: LogIndex,
    pub hash: LogHash,
    pub result: GraphMutationResultDto,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphGetDocumentResponse {
    pub value: Option<GraphValueDto>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct KvPutRequest {
    pub request_id: String,
    pub key: String,
    pub value: String,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct KvDeleteRequest {
    pub request_id: String,
    pub key: String,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct KvGetRequest {
    pub key: String,
    pub consistency: Option<ReadConsistency>,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct KvScanRequest {
    pub start: Option<String>,
    pub end: Option<String>,
    pub prefix: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
    pub consistency: Option<ReadConsistency>,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum KvMutationResultDto {
    Put { replaced: bool },
    Delete { existed: bool },
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct KvMutationResponse {
    pub applied_index: LogIndex,
    pub hash: LogHash,
    pub result: KvMutationResultDto,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct KvGetResponse {
    pub value: Option<String>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct KvScanEntryDto {
    pub key: String,
    pub value: String,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct KvScanResponse {
    pub entries: Vec<KvScanEntryDto>,
    pub next_cursor: Option<String>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum KvBatchMemberRequest {
    Put(KvPutRequest),
    Delete(KvDeleteRequest),
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct KvBatchRequest {
    pub members: Vec<KvBatchMemberRequest>,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct KvBatchMemberResponse {
    pub applied_index: LogIndex,
    pub hash: LogHash,
    pub result: KvMutationResultDto,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct KvBatchResponse {
    pub members: Vec<KvBatchMemberResponse>,
}

#[cfg(feature = "graph")]
fn graph_mutation_response(outcome: GraphMutationOutcome) -> GraphMutationResponse {
    GraphMutationResponse {
        applied_index: outcome.applied_index(),
        hash: outcome.hash(),
        result: match outcome.result() {
            GraphCommandResultV1::PutDocument { created } => {
                GraphMutationResultDto::PutDocument { created: *created }
            }
            GraphCommandResultV1::DeleteDocument { existed } => {
                GraphMutationResultDto::DeleteDocument { existed: *existed }
            }
        },
    }
}

#[cfg(feature = "kv")]
fn kv_mutation_response(outcome: KvMutationOutcome) -> KvMutationResponse {
    KvMutationResponse {
        applied_index: outcome.applied_index(),
        hash: outcome.hash(),
        result: match outcome.result() {
            KvCommandResultV1::Put { replaced } => KvMutationResultDto::Put {
                replaced: *replaced,
            },
            KvCommandResultV1::Delete { existed } => {
                KvMutationResultDto::Delete { existed: *existed }
            }
        },
    }
}

#[cfg(feature = "kv")]
fn validate_kv_scan_required_index(
    result: &KvScanResult,
    required_index: Option<LogIndex>,
) -> Result<(), NodeError> {
    let applied_index = result.tip().applied_index();
    if required_index.is_some_and(|required| applied_index < required) {
        return Err(NodeError::Unavailable(format!(
            "local applied index {applied_index} has not reached {}",
            required_index.expect("checked above")
        )));
    }
    Ok(())
}

#[cfg(feature = "graph")]
impl TryFrom<GraphValueDto> for GraphValueV1 {
    type Error = NodeError;

    fn try_from(value: GraphValueDto) -> Result<Self, Self::Error> {
        match value {
            GraphValueDto::Null => Ok(Self::Null),
            GraphValueDto::Bool(value) => Ok(Self::Bool(value)),
            GraphValueDto::I64(value) => Ok(Self::I64(value)),
            GraphValueDto::U64(value) => Ok(Self::U64(value)),
            GraphValueDto::F64(value) => {
                Self::from_f64(value).map_err(|error| NodeError::InvalidRequest(error.to_string()))
            }
            GraphValueDto::String(value) => Ok(Self::String(value)),
            GraphValueDto::Bytes(value) => decode_base64("value", &value).map(Self::Bytes),
        }
    }
}

#[cfg(feature = "graph")]
impl From<GraphValueV1> for GraphValueDto {
    fn from(value: GraphValueV1) -> Self {
        match value {
            GraphValueV1::Null => Self::Null,
            GraphValueV1::Bool(value) => Self::Bool(value),
            GraphValueV1::I64(value) => Self::I64(value),
            GraphValueV1::U64(value) => Self::U64(value),
            GraphValueV1::F64(value) => Self::F64(value.get()),
            GraphValueV1::String(value) => Self::String(value),
            GraphValueV1::Bytes(value) => Self::Bytes(encode_base64(&value)),
        }
    }
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        encoded.push(ALPHABET[usize::from(first >> 2)] as char);
        encoded.push(ALPHABET[usize::from(((first & 0x03) << 4) | (second >> 4))] as char);
        if chunk.len() > 1 {
            encoded.push(ALPHABET[usize::from(((second & 0x0f) << 2) | (third >> 6))] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(ALPHABET[usize::from(third & 0x3f)] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn decode_base64(field: &str, encoded: &str) -> Result<Vec<u8>, NodeError> {
    fn sextet(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    let bytes = encoded.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(NodeError::InvalidRequest(format!(
            "{field} must be canonical padded base64"
        )));
    }
    let mut decoded = Vec::with_capacity(bytes.len() / 4 * 3);
    for (chunk_index, chunk) in bytes.chunks_exact(4).enumerate() {
        let last = chunk_index + 1 == bytes.len() / 4;
        let first = sextet(chunk[0]);
        let second = sextet(chunk[1]);
        let third = (chunk[2] != b'=').then(|| sextet(chunk[2])).flatten();
        let fourth = (chunk[3] != b'=').then(|| sextet(chunk[3])).flatten();
        let has_padding = chunk[2] == b'=' || chunk[3] == b'=';
        if first.is_none()
            || second.is_none()
            || (chunk[2] != b'=' && third.is_none())
            || (chunk[3] != b'=' && fourth.is_none())
            || (!last && has_padding)
            || (chunk[2] == b'=' && chunk[3] != b'=')
        {
            return Err(NodeError::InvalidRequest(format!(
                "{field} must be canonical padded base64"
            )));
        }
        let first = first.unwrap();
        let second = second.unwrap();
        decoded.push((first << 2) | (second >> 4));
        if let Some(third) = third {
            decoded.push((second << 4) | (third >> 2));
            if let Some(fourth) = fourth {
                decoded.push((third << 6) | fourth);
            } else if third & 0x03 != 0 {
                return Err(NodeError::InvalidRequest(format!(
                    "{field} must be canonical padded base64"
                )));
            }
        } else if second & 0x0f != 0 {
            return Err(NodeError::InvalidRequest(format!(
                "{field} must be canonical padded base64"
            )));
        }
    }
    Ok(decoded)
}

enum Materializer {
    #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
    Unavailable,
    #[cfg(feature = "sql")]
    Sql(Box<SqliteStateMachine>),
    #[cfg(feature = "graph")]
    Graph(Arc<LadybugStateMachine>),
    #[cfg(feature = "kv")]
    Kv(Arc<RedbStateMachine>),
}

#[cfg(any(feature = "sql", feature = "kv"))]
fn quarantine_materializer(data_dir: &Path, directory: &str) -> Result<(), NodeError> {
    static SEQUENCE: AtomicUsize = AtomicUsize::new(0);
    let source = data_dir.join(directory);
    if !source.exists() {
        return Ok(());
    }
    loop {
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let target = data_dir.join(format!(
            "{directory}.quarantine-{}-{sequence}",
            std::process::id()
        ));
        match fs::rename(&source, target) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(NodeError::Storage(error.to_string())),
        }
    }
}

#[cfg(feature = "sql")]
struct SqlMaterializerGuard<'a>(MutexGuard<'a, Materializer>);

#[cfg(feature = "sql")]
impl std::ops::Deref for SqlMaterializerGuard<'_> {
    type Target = SqliteStateMachine;

    fn deref(&self) -> &Self::Target {
        match &*self.0 {
            Materializer::Sql(state) => state,
            #[cfg(feature = "graph")]
            Materializer::Graph(_) => unreachable!("SQL guard validated the materializer profile"),
            #[cfg(feature = "kv")]
            Materializer::Kv(_) => unreachable!("SQL guard validated the materializer profile"),
        }
    }
}

impl Materializer {
    fn close_for_handoff(self) -> Result<(), String> {
        match self {
            #[cfg(feature = "sql")]
            Self::Sql(state) => state.close_for_handoff().map_err(|error| error.to_string()),
            #[cfg(feature = "graph")]
            Self::Graph(_) => Ok(()),
            #[cfg(feature = "kv")]
            Self::Kv(_) => Ok(()),
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => Ok(()),
        }
    }

    fn ensure_profile_available(profile: ExecutionProfile) -> Result<(), NodeError> {
        if execution_profile_compiled(profile) {
            Ok(())
        } else {
            Err(NodeError::Unavailable(format!(
                "{} execution profile is not compiled in",
                profile.as_str()
            )))
        }
    }

    #[cfg_attr(not(feature = "sql"), allow(unused_variables))]
    fn open(
        config: &NodeConfig,
        configuration_state: &ConfigurationState,
        recovery_anchor: Option<&RecoveryAnchor>,
    ) -> Result<Self, NodeError> {
        match config.execution_profile() {
            ExecutionProfile::Sqlite => {
                #[cfg(feature = "sql")]
                {
                    let path = config.data_dir().join("sqlite/db.sqlite");
                    let open = || {
                        rhiza_sql::cleanup_empty_wal_sidecars_after_owner_fence(&path)?;
                        SqliteStateMachine::open_with_configuration(
                            &path,
                            config.cluster_id(),
                            config.node_id(),
                            config.epoch(),
                            configuration_state.clone(),
                        )
                    };
                    let state = match open() {
                        Ok(state) => state,
                        Err(open_error) => match recovery_anchor {
                            Some(anchor) => {
                                eprintln!(
                                    "SQL materializer strict reopen failed before snapshot recovery: {open_error}"
                                );
                                return Err(NodeError::SnapshotRequired(Box::new(anchor.clone())));
                            }
                            None => {
                                quarantine_materializer(config.data_dir(), "sqlite")?;
                                open().map_err(|error| NodeError::Storage(error.to_string()))?
                            }
                        },
                    };
                    Ok(Self::Sql(Box::new(state)))
                }
                #[cfg(not(feature = "sql"))]
                Err(NodeError::Unavailable(
                    "sql execution profile is not compiled in".into(),
                ))
            }
            ExecutionProfile::Graph => {
                #[cfg(feature = "graph")]
                {
                    LadybugStateMachine::open(
                        config.data_dir().join("ladybug/graph.lbug"),
                        config.cluster_id(),
                        config.node_id(),
                        config.epoch(),
                        configuration_state.config_id(),
                    )
                    .map(Arc::new)
                    .map(Self::Graph)
                    .map_err(|error| NodeError::Storage(error.to_string()))
                }
                #[cfg(not(feature = "graph"))]
                Err(NodeError::Unavailable(
                    "graph execution profile is not compiled in".into(),
                ))
            }
            ExecutionProfile::Kv => {
                #[cfg(feature = "kv")]
                {
                    let open = || {
                        RedbStateMachine::open(
                            config.data_dir().join("kv/data.redb"),
                            config.cluster_id(),
                            config.node_id(),
                            config.epoch(),
                            configuration_state.config_id(),
                        )
                    };
                    let state = match open() {
                        Ok(state) => state,
                        Err(_) => match recovery_anchor {
                            Some(anchor) => {
                                return Err(NodeError::SnapshotRequired(Box::new(anchor.clone())));
                            }
                            _ => {
                                quarantine_materializer(config.data_dir(), "kv")?;
                                open().map_err(|error| NodeError::Storage(error.to_string()))?
                            }
                        },
                    };
                    Ok(Self::Kv(Arc::new(state)))
                }
                #[cfg(not(feature = "kv"))]
                Err(NodeError::Unavailable(
                    "kv execution profile is not compiled in".into(),
                ))
            }
        }
    }

    #[cfg_attr(not(feature = "sql"), allow(unused_variables))]
    fn open_detached(
        data_dir: &Path,
        profile: ExecutionProfile,
        cluster_id: &str,
        node_id: &str,
        epoch: u64,
        configuration_state: &ConfigurationState,
    ) -> Result<Self, NodeError> {
        Self::ensure_profile_available(profile)?;
        match profile {
            ExecutionProfile::Sqlite => {
                #[cfg(feature = "sql")]
                {
                    SqliteStateMachine::open_with_configuration(
                        data_dir.join("sqlite/db.sqlite"),
                        cluster_id,
                        node_id,
                        epoch,
                        configuration_state.clone(),
                    )
                    .map(Box::new)
                    .map(Self::Sql)
                    .map_err(|error| NodeError::Storage(error.to_string()))
                }
                #[cfg(not(feature = "sql"))]
                Err(NodeError::Unavailable(
                    "sql execution profile is not compiled in".into(),
                ))
            }
            ExecutionProfile::Graph => {
                #[cfg(feature = "graph")]
                {
                    LadybugStateMachine::open(
                        data_dir.join("ladybug/graph.lbug"),
                        cluster_id,
                        node_id,
                        epoch,
                        configuration_state.config_id(),
                    )
                    .map(Arc::new)
                    .map(Self::Graph)
                    .map_err(|error| NodeError::Storage(error.to_string()))
                }
                #[cfg(not(feature = "graph"))]
                Err(NodeError::Unavailable(
                    "graph execution profile is not compiled in".into(),
                ))
            }
            ExecutionProfile::Kv => {
                #[cfg(feature = "kv")]
                {
                    RedbStateMachine::open(
                        data_dir.join("kv/data.redb"),
                        cluster_id,
                        node_id,
                        epoch,
                        configuration_state.config_id(),
                    )
                    .map(Arc::new)
                    .map(Self::Kv)
                    .map_err(|error| NodeError::Storage(error.to_string()))
                }
                #[cfg(not(feature = "kv"))]
                Err(NodeError::Unavailable(
                    "kv execution profile is not compiled in".into(),
                ))
            }
        }
    }

    fn profile(&self) -> ExecutionProfile {
        match self {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Self::Sql(_) => ExecutionProfile::Sqlite,
            #[cfg(feature = "graph")]
            Self::Graph(_) => ExecutionProfile::Graph,
            #[cfg(feature = "kv")]
            Self::Kv(_) => ExecutionProfile::Kv,
        }
    }

    fn applied_index(&self) -> Result<LogIndex, String> {
        match self {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Self::Sql(state) => state
                .applied_index_value()
                .map_err(|error| error.to_string()),
            #[cfg(feature = "graph")]
            Self::Graph(state) => state.applied_index().map_err(|error| error.to_string()),
            #[cfg(feature = "kv")]
            Self::Kv(state) => state.applied_index().map_err(|error| error.to_string()),
        }
    }

    fn applied_hash(&self) -> Result<LogHash, String> {
        match self {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Self::Sql(state) => state
                .applied_hash_value()
                .map_err(|error| error.to_string()),
            #[cfg(feature = "graph")]
            Self::Graph(state) => state.applied_hash().map_err(|error| error.to_string()),
            #[cfg(feature = "kv")]
            Self::Kv(state) => state.applied_hash().map_err(|error| error.to_string()),
        }
    }

    fn applied_tip(&self) -> Result<LogAnchor, String> {
        match self {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Self::Sql(state) => state
                .applied_tip()
                .map(|tip| LogAnchor::new(tip.applied_index(), tip.applied_hash()))
                .map_err(|error| error.to_string()),
            #[cfg(feature = "graph")]
            Self::Graph(state) => state.applied_tip().map_err(|error| error.to_string()),
            #[cfg(feature = "kv")]
            Self::Kv(state) => state.applied_tip().map_err(|error| error.to_string()),
        }
    }

    fn configuration_state(&self) -> Result<Option<ConfigurationState>, String> {
        match self {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Self::Sql(state) => state
                .configuration_state_value()
                .map(Some)
                .map_err(|error| error.to_string()),
            #[cfg(feature = "graph")]
            Self::Graph(_) => Ok(None),
            #[cfg(feature = "kv")]
            Self::Kv(_) => Ok(None),
        }
    }

    fn apply_entry(&self, entry: &LogEntry) -> Result<Option<SqlCommandResult>, String> {
        match self {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Self::Sql(state) => state
                .apply_entry_with_result(entry)
                .map(|outcome| outcome.sql_result().cloned())
                .map_err(|error| error.to_string()),
            #[cfg(feature = "graph")]
            Self::Graph(state) => state
                .apply_entry(entry)
                .map(|_| None)
                .map_err(|error| error.to_string()),
            #[cfg(feature = "kv")]
            Self::Kv(state) => state
                .apply_entry(entry)
                .map(|_| None)
                .map_err(|error| error.to_string()),
        }
    }

    #[cfg(feature = "sql")]
    pub(crate) fn apply_verified_external_sql_effect(
        &self,
        effect: &VerifiedQwalEffectBundleV4,
    ) -> Result<Option<SqlCommandResult>, String> {
        match self {
            Self::Sql(state) => state
                .apply_verified_external_effect(effect)
                .map(|outcome| outcome.sql_result().cloned())
                .map_err(|error| error.to_string()),
            #[cfg(feature = "graph")]
            Self::Graph(_) => Err("external SQL effect reached graph materializer".into()),
            #[cfg(feature = "kv")]
            Self::Kv(_) => Err("external SQL effect reached KV materializer".into()),
        }
    }
}

const READ_BARRIER_COALESCE_WINDOW: Duration = Duration::from_micros(50);

struct ReadBarrierRounds {
    state: Mutex<ReadBarrierRoundsState>,
    collection_window: Duration,
}

struct ReadBarrierRoundsState {
    tail: Option<Arc<ReadBarrierRound>>,
    next_generation: u64,
}

struct ReadBarrierRound {
    #[cfg(test)]
    generation: u64,
    collection_deadline: Instant,
    predecessor: Mutex<Option<Arc<ReadBarrierRound>>>,
    phase: Mutex<ReadBarrierRoundPhase>,
    changed: Condvar,
}

#[derive(Clone)]
enum ReadBarrierRoundPhase {
    Collecting,
    Running,
    Complete(Result<LogAnchor, NodeError>),
}

struct ReadBarrierParticipant {
    round: Arc<ReadBarrierRound>,
    leader: bool,
}

struct ReadBarrierPublication {
    round: Arc<ReadBarrierRound>,
    published: bool,
}

impl ReadBarrierRounds {
    fn new(collection_window: Duration) -> Self {
        Self {
            state: Mutex::new(ReadBarrierRoundsState {
                tail: None,
                next_generation: 0,
            }),
            collection_window,
        }
    }

    fn join(&self) -> Result<ReadBarrierParticipant, NodeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| NodeError::Invariant("read barrier generation lock is poisoned".into()))?;
        if let Some(tail) = state.tail.as_ref() {
            let collecting = matches!(
                *tail.phase.lock().map_err(|_| {
                    NodeError::Invariant("read barrier round lock is poisoned".into())
                })?,
                ReadBarrierRoundPhase::Collecting
            );
            if collecting {
                return Ok(ReadBarrierParticipant {
                    round: Arc::clone(tail),
                    leader: false,
                });
            }
        }

        let generation = state
            .next_generation
            .checked_add(1)
            .ok_or_else(|| NodeError::Invariant("read barrier generation is exhausted".into()))?;
        state.next_generation = generation;
        let collection_deadline = Instant::now()
            .checked_add(self.collection_window)
            .unwrap_or_else(Instant::now);
        let round = Arc::new(ReadBarrierRound {
            #[cfg(test)]
            generation,
            collection_deadline,
            predecessor: Mutex::new(state.tail.clone()),
            phase: Mutex::new(ReadBarrierRoundPhase::Collecting),
            changed: Condvar::new(),
        });
        state.tail = Some(Arc::clone(&round));
        Ok(ReadBarrierParticipant {
            round,
            leader: true,
        })
    }

    fn cancel_waiters(&self) {
        let mut round = self.state.lock().ok().and_then(|state| state.tail.clone());
        while let Some(current) = round {
            if let Ok(_phase) = current.phase.lock() {
                current.changed.notify_all();
            }
            round = current
                .predecessor
                .lock()
                .ok()
                .and_then(|predecessor| predecessor.clone());
        }
    }
}

impl ReadBarrierRound {
    fn wait_complete(&self, cancelled: &AtomicBool) -> Result<(), NodeError> {
        let mut phase = self
            .phase
            .lock()
            .map_err(|_| NodeError::Invariant("read barrier round lock is poisoned".into()))?;
        loop {
            if matches!(*phase, ReadBarrierRoundPhase::Complete(_)) {
                return Ok(());
            }
            if cancelled.load(Ordering::Acquire) {
                return Err(NodeError::Unavailable(
                    "read barrier cancelled during shutdown".into(),
                ));
            }
            phase = self
                .changed
                .wait(phase)
                .map_err(|_| NodeError::Invariant("read barrier round lock is poisoned".into()))?;
        }
    }

    fn result(&self, cancelled: &AtomicBool) -> Result<LogAnchor, NodeError> {
        let mut phase = self
            .phase
            .lock()
            .map_err(|_| NodeError::Invariant("read barrier round lock is poisoned".into()))?;
        loop {
            if let ReadBarrierRoundPhase::Complete(result) = &*phase {
                return result.clone();
            }
            if cancelled.load(Ordering::Acquire) {
                return Err(NodeError::Unavailable(
                    "read barrier cancelled during shutdown".into(),
                ));
            }
            phase = self
                .changed
                .wait(phase)
                .map_err(|_| NodeError::Invariant("read barrier round lock is poisoned".into()))?;
        }
    }

    fn complete(&self, result: Result<LogAnchor, NodeError>) {
        if let Ok(mut phase) = self.phase.lock() {
            if !matches!(*phase, ReadBarrierRoundPhase::Complete(_)) {
                *phase = ReadBarrierRoundPhase::Complete(result);
            }
            self.changed.notify_all();
        } else {
            self.changed.notify_all();
        }
    }
}

impl ReadBarrierParticipant {
    #[cfg(test)]
    fn generation(&self) -> u64 {
        self.round.generation
    }

    #[cfg(test)]
    fn is_leader(&self) -> bool {
        self.leader
    }

    fn publication(&self) -> Option<ReadBarrierPublication> {
        self.leader.then(|| ReadBarrierPublication {
            round: Arc::clone(&self.round),
            published: false,
        })
    }

    fn wait(&self, cancelled: &AtomicBool) -> Result<LogAnchor, NodeError> {
        self.round.result(cancelled)
    }
}

impl ReadBarrierPublication {
    fn wait_turn(&self, cancelled: &AtomicBool) -> Result<(), NodeError> {
        let predecessor = self
            .round
            .predecessor
            .lock()
            .map_err(|_| NodeError::Invariant("read barrier predecessor lock is poisoned".into()))?
            .clone();
        if let Some(predecessor) = predecessor {
            predecessor.wait_complete(cancelled)?;
            self.round
                .predecessor
                .lock()
                .map_err(|_| {
                    NodeError::Invariant("read barrier predecessor lock is poisoned".into())
                })?
                .take();
        }

        let mut phase = self
            .round
            .phase
            .lock()
            .map_err(|_| NodeError::Invariant("read barrier round lock is poisoned".into()))?;
        loop {
            if cancelled.load(Ordering::Acquire) {
                return Err(NodeError::Unavailable(
                    "read barrier cancelled during shutdown".into(),
                ));
            }
            if !matches!(*phase, ReadBarrierRoundPhase::Collecting) {
                return Err(NodeError::Invariant(
                    "read barrier leader left the collecting phase early".into(),
                ));
            }
            let now = Instant::now();
            if now >= self.round.collection_deadline {
                return Ok(());
            }
            let wait = self
                .round
                .collection_deadline
                .saturating_duration_since(now);
            let (next, _) =
                self.round.changed.wait_timeout(phase, wait).map_err(|_| {
                    NodeError::Invariant("read barrier round lock is poisoned".into())
                })?;
            phase = next;
        }
    }

    fn start(&self, cancelled: &AtomicBool) -> Result<(), NodeError> {
        let mut phase = self
            .round
            .phase
            .lock()
            .map_err(|_| NodeError::Invariant("read barrier round lock is poisoned".into()))?;
        if cancelled.load(Ordering::Acquire) {
            return Err(NodeError::Unavailable(
                "read barrier cancelled during shutdown".into(),
            ));
        }
        if !matches!(*phase, ReadBarrierRoundPhase::Collecting) {
            return Err(NodeError::Invariant(
                "read barrier generation cannot start twice".into(),
            ));
        }
        *phase = ReadBarrierRoundPhase::Running;
        Ok(())
    }

    fn publish(&mut self, result: Result<LogAnchor, NodeError>) {
        self.round.complete(result);
        self.published = true;
    }
}

impl Drop for ReadBarrierPublication {
    fn drop(&mut self) {
        if !self.published {
            self.round.complete(Err(NodeError::Unavailable(
                "read barrier generation leader terminated".into(),
            )));
        }
    }
}

#[cfg(all(test, feature = "kv"))]
type KvGroupCommitAfterExecuteHook = Arc<dyn Fn(&NodeRuntime) + Send + Sync>;

const READ_BARRIER_CACHE_TTL: Duration = Duration::from_millis(1);

struct CachedBarrier {
    anchor: LogAnchor,
    cached_at: Instant,
}

pub struct NodeRuntime {
    config: NodeConfig,
    consensus: Arc<ThreeNodeConsensus>,
    log_store: FileLogStore,
    materializer: Mutex<Materializer>,
    commit: Mutex<()>,
    #[cfg(feature = "sql")]
    sql_group_commit: SqlGroupCommitQueue,
    #[cfg(feature = "kv")]
    kv_group_commit: KvGroupCommitQueue,
    read_barriers: ReadBarrierRounds,
    cached_barrier: Mutex<Option<CachedBarrier>>,
    checkpointing: AtomicBool,
    operation_cancelled: Arc<AtomicBool>,
    operation_cancelled_notify: tokio::sync::Notify,
    ready: AtomicBool,
    fatal: AtomicBool,
    fatal_reason: Mutex<Option<String>>,
    #[cfg(test)]
    materialized_tip_checks: AtomicUsize,
    #[cfg(all(test, feature = "sql"))]
    sql_group_commit_before_execute_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(all(test, feature = "kv"))]
    kv_group_commit_before_execute_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(all(test, feature = "kv"))]
    kv_group_commit_after_execute_hook: Option<KvGroupCommitAfterExecuteHook>,
    #[cfg(all(test, any(feature = "graph", feature = "kv")))]
    read_barrier_before_snapshot_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    _data_root_lock: fs::File,
}

#[cfg(feature = "sql")]
struct PreparedExternalSqlBundleResolution {
    consensus: Arc<ThreeNodeConsensus>,
    binding: EffectBundleBinding,
    manifest_command: StoredCommand,
    command: ExternalEffectCommand,
}

#[cfg(feature = "sql")]
impl PreparedExternalSqlBundleResolution {
    fn resolve(self) -> Result<Option<RecorderEffectBundle>, rhiza_quepaxa::Error> {
        let bundle = self.consensus.resolve_effect_bundle_from_quorum(
            // Archive publication remains available during shutdown/recovery,
            // but the synchronous RPC wait runs on Tokio's blocking pool.
            &RecorderRpcContext::with_timeout(DEFAULT_RECORDER_RPC_TIMEOUT),
            &self.binding,
            &self.manifest_command,
        )?;
        bundle
            .map(|bundle| {
                verify_resolved_external_sql_bundle(&self.command, &bundle)
                    .map(|_| bundle)
                    .map_err(|error| rhiza_quepaxa::Error::EffectBundleInvalid(error.to_string()))
            })
            .transpose()
    }
}

#[cfg(feature = "sql")]
struct ExecutedPayload {
    response: WriteResponse,
    sql_result: Option<SqlCommandResult>,
}

#[cfg(feature = "sql")]
trait SqlWritePhaseProfile {
    type Mark;

    fn mark(&self) -> Self::Mark;
    fn commit_lock_acquired(&mut self);
    fn add_precheck_classification(&mut self, mark: Self::Mark);
    fn add_qwal_prepare(&mut self, mark: Self::Mark);
    fn add_consensus_propose(&mut self, mark: Self::Mark);
    fn add_local_qlog_mirror_append(&mut self, mark: Self::Mark);
    fn add_sql_materializer_apply(&mut self, mark: Self::Mark);
    fn record_success(&mut self, batch_member_count: usize);
}

#[cfg(feature = "sql")]
struct DisabledSqlWritePhaseProfile;

#[cfg(feature = "sql")]
impl SqlWritePhaseProfile for DisabledSqlWritePhaseProfile {
    type Mark = ();

    #[inline]
    fn mark(&self) {}

    #[inline]
    fn commit_lock_acquired(&mut self) {}

    #[inline]
    fn add_precheck_classification(&mut self, (): ()) {}

    #[inline]
    fn add_qwal_prepare(&mut self, (): ()) {}

    #[inline]
    fn add_consensus_propose(&mut self, (): ()) {}

    #[inline]
    fn add_local_qlog_mirror_append(&mut self, (): ()) {}

    #[inline]
    fn add_sql_materializer_apply(&mut self, (): ()) {}

    #[inline]
    fn record_success(&mut self, _batch_member_count: usize) {}
}

#[cfg(feature = "sql")]
struct EnabledSqlWritePhaseProfile {
    observer: SqlWriteProfiler,
    service_started: Instant,
    commit_lock_wait: Duration,
    precheck_classification: Duration,
    qwal_prepare: Duration,
    consensus_propose: Duration,
    local_qlog_mirror_append: Duration,
    sql_materializer_apply: Duration,
}

#[cfg(feature = "sql")]
impl EnabledSqlWritePhaseProfile {
    fn new(observer: SqlWriteProfiler) -> Self {
        Self {
            observer,
            service_started: Instant::now(),
            commit_lock_wait: Duration::ZERO,
            precheck_classification: Duration::ZERO,
            qwal_prepare: Duration::ZERO,
            consensus_propose: Duration::ZERO,
            local_qlog_mirror_append: Duration::ZERO,
            sql_materializer_apply: Duration::ZERO,
        }
    }

    fn reset(&mut self) {
        self.service_started = Instant::now();
        self.commit_lock_wait = Duration::ZERO;
        self.precheck_classification = Duration::ZERO;
        self.qwal_prepare = Duration::ZERO;
        self.consensus_propose = Duration::ZERO;
        self.local_qlog_mirror_append = Duration::ZERO;
        self.sql_materializer_apply = Duration::ZERO;
    }
}

#[cfg(feature = "sql")]
impl SqlWritePhaseProfile for EnabledSqlWritePhaseProfile {
    type Mark = Instant;

    #[inline]
    fn mark(&self) -> Instant {
        Instant::now()
    }

    fn commit_lock_acquired(&mut self) {
        self.commit_lock_wait = self.service_started.elapsed();
    }

    fn add_precheck_classification(&mut self, mark: Instant) {
        self.precheck_classification += mark.elapsed();
    }

    fn add_qwal_prepare(&mut self, mark: Instant) {
        self.qwal_prepare += mark.elapsed();
    }

    fn add_consensus_propose(&mut self, mark: Instant) {
        self.consensus_propose += mark.elapsed();
    }

    fn add_local_qlog_mirror_append(&mut self, mark: Instant) {
        self.local_qlog_mirror_append += mark.elapsed();
    }

    fn add_sql_materializer_apply(&mut self, mark: Instant) {
        self.sql_materializer_apply += mark.elapsed();
    }

    fn record_success(&mut self, batch_member_count: usize) {
        let total_service_us = duration_as_u64_micros(self.service_started.elapsed());
        let commit_lock_wait_us = duration_as_u64_micros(self.commit_lock_wait);
        let precheck_classification_us = duration_as_u64_micros(self.precheck_classification);
        let qwal_prepare_us = duration_as_u64_micros(self.qwal_prepare);
        let consensus_propose_us = duration_as_u64_micros(self.consensus_propose);
        let local_qlog_mirror_append_us = duration_as_u64_micros(self.local_qlog_mirror_append);
        let sql_materializer_apply_us = duration_as_u64_micros(self.sql_materializer_apply);
        let named_us = commit_lock_wait_us
            .saturating_add(precheck_classification_us)
            .saturating_add(qwal_prepare_us)
            .saturating_add(consensus_propose_us)
            .saturating_add(local_qlog_mirror_append_us)
            .saturating_add(sql_materializer_apply_us);
        self.observer.record(SqlWriteProfileSample {
            batch_member_count,
            commit_lock_wait_us,
            precheck_classification_us,
            qwal_prepare_us,
            consensus_propose_us,
            local_qlog_mirror_append_us,
            sql_materializer_apply_us,
            response_other_total_us: total_service_us.saturating_sub(named_us),
            total_service_us,
        });
        self.reset();
    }
}

#[cfg(feature = "sql")]
fn duration_as_u64_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(feature = "sql")]
type CheckedSqlRequest = Result<Option<(RequestOutcome, SqlCommandResult)>, NodeError>;

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedSnapshotPublication {
    anchor: RecoveryAnchor,
}

impl fmt::Debug for NodeRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeRuntime")
            .field("config", &self.config)
            .field("ready", &self.ready.load(Ordering::Acquire))
            .field("fatal", &self.fatal.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl NodeRuntime {
    fn consensus_context(&self) -> RecorderRpcContext {
        RecorderRpcContext::with_timeout_and_cancellation(
            DEFAULT_RECORDER_RPC_TIMEOUT,
            Arc::clone(&self.operation_cancelled),
        )
    }

    #[cfg(all(feature = "tuner", feature = "sql"))]
    fn record_tuner_propose_outcome(&self, latency_us: u64, success: bool, timeout: bool) {
        if let Some(telemetry) = self.config.tuner_telemetry() {
            telemetry.collector().record_proposer_latency(
                &self.config.node_id,
                latency_us,
                success,
                timeout,
            );
        }
    }

    #[cfg(all(feature = "tuner", feature = "sql"))]
    fn record_tuner_consensus_error(&self, error: &rhiza_quepaxa::Error) {
        let is_timeout = matches!(
            error,
            rhiza_quepaxa::Error::RpcDeadlineExceeded
                | rhiza_quepaxa::Error::NoQuorum
                | rhiza_quepaxa::Error::Cancelled
                | rhiza_quepaxa::Error::RpcCancelled
        );
        self.record_tuner_propose_outcome(0, false, is_timeout);
    }

    #[cfg(all(feature = "tuner", feature = "sql"))]
    fn record_tuner_consensus_success(&self, latency: Duration) {
        let latency_us = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
        self.record_tuner_propose_outcome(latency_us, true, false);
    }

    pub fn open(
        config: NodeConfig,
        consensus: Arc<ThreeNodeConsensus>,
        peer_candidates: &[&dyn LogPeer],
    ) -> Result<Self, NodeError> {
        Self::open_cancellable(config, consensus, peer_candidates, &StartupIoContext::new())
    }

    pub fn open_cancellable(
        config: NodeConfig,
        consensus: Arc<ThreeNodeConsensus>,
        peer_candidates: &[&dyn LogPeer],
        startup: &StartupIoContext,
    ) -> Result<Self, NodeError> {
        startup.check("runtime configuration validation")?;
        if config.ack_mode == AckMode::DrStrong {
            return Err(NodeError::UnsupportedAckMode(AckMode::DrStrong));
        }
        Materializer::ensure_profile_available(config.execution_profile())?;
        {
            let _permit = startup.admit_local_io("runtime data directory creation")?;
            fs::create_dir_all(&config.data_dir)
                .map_err(|error| NodeError::Storage(error.to_string()))?;
        }
        let data_root_lock = {
            let _permit = startup.admit_local_io("runtime data lock acquisition")?;
            match acquire_node_data_root_lock(&config.data_dir) {
                Ok(lock) => lock.into_file(),
                Err(NodeDataRootLockError::Locked) => {
                    return Err(NodeError::DataRootLocked(config.data_dir.clone()));
                }
                Err(error) => return Err(NodeError::Storage(error.to_string())),
            }
        };

        let (log_store, persisted_configuration, recovery_anchor) = {
            let _permit = startup.admit_local_io("qlog open and recovery")?;
            let log_store = FileLogStore::open_with_configuration(
                config.data_dir.join("consensus/log"),
                &config.cluster_id,
                config.epoch,
                config.log_initial_configuration.clone(),
            )
            .map_err(|error| NodeError::Storage(error.to_string()))?;
            let persisted_configuration = log_store
                .configuration_state()
                .map_err(|error| NodeError::Storage(error.to_string()))?;
            let recovery_anchor = log_store
                .logical_state()
                .map_err(|error| NodeError::Storage(error.to_string()))?
                .anchor;
            (log_store, persisted_configuration, recovery_anchor)
        };
        let materializer = {
            let _permit = startup.admit_local_io("materializer open and reconciliation")?;
            let materializer =
                Materializer::open(&config, &persisted_configuration, recovery_anchor.as_ref())?;
            reconcile_local_storage(&config, consensus.as_ref(), &log_store, &materializer)?;
            materializer
        };
        startup.check("peer recovery")?;
        recover_peer_candidates(
            &config,
            consensus.as_ref(),
            &log_store,
            &materializer,
            peer_candidates,
            startup,
        )?;
        recover_startup_decisions(
            &config,
            consensus.as_ref(),
            &log_store,
            &materializer,
            startup,
        )?;
        startup.check("runtime readiness publication")?;

        Ok(Self {
            #[cfg(feature = "sql")]
            sql_group_commit: SqlGroupCommitQueue::new(config.sql_group_commit_queue_capacity),
            #[cfg(feature = "kv")]
            kv_group_commit: KvGroupCommitQueue::new(),
            config,
            consensus,
            log_store,
            materializer: Mutex::new(materializer),
            commit: Mutex::new(()),
            read_barriers: ReadBarrierRounds::new(READ_BARRIER_COALESCE_WINDOW),
            cached_barrier: Mutex::new(None),
            checkpointing: AtomicBool::new(false),
            operation_cancelled: Arc::new(AtomicBool::new(false)),
            operation_cancelled_notify: tokio::sync::Notify::new(),
            ready: AtomicBool::new(true),
            fatal: AtomicBool::new(false),
            fatal_reason: Mutex::new(None),
            #[cfg(test)]
            materialized_tip_checks: AtomicUsize::new(0),
            #[cfg(all(test, feature = "sql"))]
            sql_group_commit_before_execute_hook: None,
            #[cfg(all(test, feature = "kv"))]
            kv_group_commit_before_execute_hook: None,
            #[cfg(all(test, feature = "kv"))]
            kv_group_commit_after_execute_hook: None,
            #[cfg(all(test, any(feature = "graph", feature = "kv")))]
            read_barrier_before_snapshot_hook: None,
            _data_root_lock: data_root_lock,
        })
    }

    #[cfg(feature = "sql")]
    pub fn write(
        &self,
        request_id: &str,
        key: &str,
        value: &str,
    ) -> Result<WriteResponse, NodeError> {
        let payload = canonical_put(request_id, key, value)?;
        let _commit = self.lock_commit()?;
        self.execute_put_payload_locked(request_id, key, value, payload)
            .map(|outcome| outcome.response)
    }

    #[cfg(feature = "graph")]
    pub fn mutate_graph(&self, command: GraphCommandV1) -> Result<GraphMutationOutcome, NodeError> {
        let payload = encode_replicated_graph_command(&command)
            .map_err(|error| NodeError::InvalidRequest(error.to_string()))?;
        if payload.len() > MAX_COMMAND_BYTES {
            return Err(NodeError::InvalidRequest(format!(
                "command exceeds {MAX_COMMAND_BYTES} bytes"
            )));
        }
        let _commit = self.lock_commit()?;
        self.mutate_graph_payload_locked(&command, payload)
    }

    /// Executes an ordered, non-atomic batch of graph mutations.
    ///
    /// Valid commands may share one QuePaxa log entry. Per-command conflicts remain isolated in
    /// the returned vector, whose order and length match `commands`. The whole vector is validated
    /// before the first write attempt, so an outer `Err` guarantees that nothing was attempted.
    #[cfg(feature = "graph")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "kv")),
        allow(unreachable_patterns)
    )]
    pub fn mutate_graph_batch(
        &self,
        commands: Vec<GraphCommandV1>,
    ) -> Result<Vec<Result<GraphMutationOutcome, NodeError>>, NodeError> {
        self.require_execution_profile(ExecutionProfile::Graph)?;
        validate_typed_batch_len(commands.len())?;
        let mut members = Vec::with_capacity(commands.len());
        for command in commands {
            let payload = encode_replicated_graph_command(&command)
                .map_err(|error| NodeError::InvalidRequest(error.to_string()))?;
            validate_command_size(&payload)?;
            members.push(RuntimeBatchMember {
                #[cfg(feature = "sql")]
                request_id: command.request_id().to_owned(),
                payload,
                operation: QueuedOperation::Graph(command),
            });
        }
        Ok(self
            .execute_client_batch(members)
            .into_iter()
            .map(|result| {
                result.and_then(|response| match response {
                    ClientWriteResponse::Graph(outcome) => Ok(outcome),
                    _ => Err(NodeError::Invariant(
                        "graph batch returned a response for another profile".into(),
                    )),
                })
            })
            .collect())
    }

    #[cfg(feature = "graph")]
    fn mutate_graph_payload_locked(
        &self,
        command: &GraphCommandV1,
        payload: Vec<u8>,
    ) -> Result<GraphMutationOutcome, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        if let Some(record) = self.check_graph_request(command.request_id(), &payload)? {
            return Ok(GraphMutationOutcome::from_record(record));
        }
        loop {
            self.ensure_writes_active()?;
            let (last_index, last_hash) = self.ensure_materialized_tip()?;
            let slot = last_index.checked_add(1).ok_or_else(|| {
                self.latch(NodeError::Invariant("qlog index is exhausted".into()))
            })?;
            let entry = self
                .consensus
                .propose_at(
                    self.consensus_context(),
                    slot,
                    last_hash,
                    Command::new(CommandKind::Deterministic, payload.clone()),
                )
                .map_err(|error| self.map_consensus_error(error))?;
            self.persist_entry(&entry, slot, last_hash)?;
            if let Some(record) = self.check_graph_request(command.request_id(), &payload)? {
                return Ok(GraphMutationOutcome::from_record(record));
            }
            if entry.entry_type == EntryType::Command && entry.payload == payload {
                return Err(self.latch(NodeError::Invariant(
                    "committed graph request was not recorded by Ladybug".into(),
                )));
            }
        }
    }

    #[cfg(feature = "graph")]
    pub fn get_graph_document(
        &self,
        id: &str,
        consistency: ReadConsistency,
    ) -> Result<GraphReadResponse, NodeError> {
        match consistency {
            ReadConsistency::Local => self.get_graph_document_local(id, None),
            ReadConsistency::AppliedIndex(required) => {
                self.get_graph_document_local(id, Some(required))
            }
            ReadConsistency::ReadBarrier => {
                let anchor = self.establish_read_barrier()?;
                self.validate_read_barrier_before_snapshot(anchor)?;
                let response = self.get_graph_document_local(id, Some(anchor.index()))?;
                self.validate_read_barrier_snapshot(
                    anchor,
                    LogAnchor::new(response.applied_index, response.hash),
                )?;
                Ok(response)
            }
        }
    }

    #[cfg(feature = "graph")]
    pub fn query_graph(
        &self,
        statement: &str,
        parameters: &BTreeMap<String, GraphParameterValue>,
        consistency: ReadConsistency,
        max_rows: u32,
    ) -> Result<GraphQueryResult, NodeError> {
        if max_rows == 0 || max_rows > MAX_GRAPH_MAX_ROWS {
            return Err(NodeError::InvalidRequest(format!(
                "max_rows must be between 1 and {MAX_GRAPH_MAX_ROWS}"
            )));
        }
        match consistency {
            ReadConsistency::Local => self.query_graph_local(statement, parameters, None, max_rows),
            ReadConsistency::AppliedIndex(required) => {
                self.query_graph_local(statement, parameters, Some(required), max_rows)
            }
            ReadConsistency::ReadBarrier => {
                let anchor = self.establish_read_barrier()?;
                self.validate_read_barrier_before_snapshot(anchor)?;
                let result =
                    self.query_graph_local(statement, parameters, Some(anchor.index()), max_rows)?;
                self.validate_read_barrier_snapshot(
                    anchor,
                    LogAnchor::new(result.applied_index, result.hash),
                )?;
                Ok(result)
            }
        }
    }

    #[cfg(feature = "kv")]
    pub fn mutate_kv(&self, command: KvCommandV1) -> Result<KvMutationOutcome, NodeError> {
        self.mutate_kv_batch(vec![command])?
            .into_iter()
            .next()
            .expect("one-member KV batch returns one result")
    }

    /// Executes an ordered, non-atomic batch of KV mutations.
    ///
    /// Valid commands may share one QuePaxa log entry. Per-command conflicts remain isolated in
    /// the returned vector, whose order and length match `commands`. The whole vector is validated
    /// before the first write attempt, so an outer `Err` guarantees that nothing was attempted.
    #[cfg(feature = "kv")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "graph")),
        allow(unreachable_patterns)
    )]
    pub fn mutate_kv_batch(
        &self,
        commands: Vec<KvCommandV1>,
    ) -> Result<Vec<Result<KvMutationOutcome, NodeError>>, NodeError> {
        self.require_execution_profile(ExecutionProfile::Kv)?;
        validate_kv_batch_len(commands.len())?;
        let mut members = Vec::with_capacity(commands.len());
        for command in commands {
            let payload = encode_replicated_kv_command(&command)
                .map_err(|error| NodeError::InvalidRequest(error.to_string()))?;
            validate_command_size(&payload)?;
            members.push(RuntimeBatchMember {
                #[cfg(feature = "sql")]
                request_id: command.request_id().to_owned(),
                payload,
                operation: QueuedOperation::Kv(command),
            });
        }
        Ok(self
            .execute_kv_group_commit(members)?
            .into_iter()
            .map(|result| {
                result.and_then(|response| match response {
                    ClientWriteResponse::Kv(outcome) => Ok(outcome),
                    _ => Err(NodeError::Invariant(
                        "KV batch returned a response for another profile".into(),
                    )),
                })
            })
            .collect())
    }

    #[cfg(feature = "kv")]
    fn mutate_kv_payload_locked(
        &self,
        command: &KvCommandV1,
        payload: Vec<u8>,
    ) -> Result<KvMutationOutcome, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        if let Some(record) = self.check_kv_request(command.request_id(), &payload)? {
            return Ok(KvMutationOutcome::from_record(record));
        }
        loop {
            self.ensure_writes_active()?;
            let (last_index, last_hash) = self.ensure_materialized_tip()?;
            let slot = last_index.checked_add(1).ok_or_else(|| {
                self.latch(NodeError::Invariant("qlog index is exhausted".into()))
            })?;
            let entry = self
                .consensus
                .propose_at(
                    self.consensus_context(),
                    slot,
                    last_hash,
                    Command::new(CommandKind::Deterministic, payload.clone()),
                )
                .map_err(|error| self.map_consensus_error(error))?;
            self.persist_entry(&entry, slot, last_hash)?;
            if let Some(record) = self.check_kv_request(command.request_id(), &payload)? {
                return Ok(KvMutationOutcome::from_record(record));
            }
            if entry.entry_type == EntryType::Command && entry.payload == payload {
                return Err(self.latch(NodeError::Invariant(
                    "committed KV request was not recorded by redb".into(),
                )));
            }
        }
    }

    #[cfg(feature = "kv")]
    pub fn get_kv(
        &self,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<KvReadResponse, NodeError> {
        match consistency {
            ReadConsistency::Local => self.get_kv_local(key, None),
            ReadConsistency::AppliedIndex(required) => self.get_kv_local(key, Some(required)),
            ReadConsistency::ReadBarrier => {
                let anchor = self.establish_read_barrier()?;
                self.validate_read_barrier_before_snapshot(anchor)?;
                let response = self.get_kv_local(key, Some(anchor.index()))?;
                self.validate_read_barrier_snapshot(
                    anchor,
                    LogAnchor::new(response.applied_index, response.hash),
                )?;
                Ok(response)
            }
        }
    }

    #[cfg(feature = "kv")]
    pub fn scan_kv_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        cursor: Option<&[u8]>,
        consistency: ReadConsistency,
    ) -> Result<KvScanResult, NodeError> {
        match consistency {
            ReadConsistency::Local => self.scan_kv_range_local(start, end, limit, cursor, None),
            ReadConsistency::AppliedIndex(required) => {
                self.scan_kv_range_local(start, end, limit, cursor, Some(required))
            }
            ReadConsistency::ReadBarrier => {
                let anchor = self.establish_read_barrier()?;
                self.validate_read_barrier_before_snapshot(anchor)?;
                let result =
                    self.scan_kv_range_local(start, end, limit, cursor, Some(anchor.index()))?;
                self.validate_read_barrier_snapshot(
                    anchor,
                    LogAnchor::new(result.tip().applied_index(), result.tip().applied_hash()),
                )?;
                Ok(result)
            }
        }
    }

    #[cfg(feature = "kv")]
    pub fn scan_kv_prefix(
        &self,
        prefix: &[u8],
        limit: usize,
        cursor: Option<&[u8]>,
        consistency: ReadConsistency,
    ) -> Result<KvScanResult, NodeError> {
        match consistency {
            ReadConsistency::Local => self.scan_kv_prefix_local(prefix, limit, cursor, None),
            ReadConsistency::AppliedIndex(required) => {
                self.scan_kv_prefix_local(prefix, limit, cursor, Some(required))
            }
            ReadConsistency::ReadBarrier => {
                let anchor = self.establish_read_barrier()?;
                self.validate_read_barrier_before_snapshot(anchor)?;
                let result =
                    self.scan_kv_prefix_local(prefix, limit, cursor, Some(anchor.index()))?;
                self.validate_read_barrier_snapshot(
                    anchor,
                    LogAnchor::new(result.tip().applied_index(), result.tip().applied_hash()),
                )?;
                Ok(result)
            }
        }
    }

    #[cfg(feature = "sql")]
    pub fn execute_sql(&self, command: SqlCommand) -> Result<WriteResponse, NodeError> {
        self.execute_sql_with_results(command)
            .map(|response| WriteResponse {
                applied_index: response.applied_index,
                hash: response.hash,
            })
    }

    /// Executes an ordered, non-atomic batch of SQL commands.
    ///
    /// All successful, previously unseen members that fit the replicated command byte cap share
    /// one exact-base QWAL entry. Failed members are rolled back independently and later members
    /// continue. The aggregate canonical input is limited to [`MAX_COMMAND_BYTES`]. The whole
    /// vector is validated before the first write attempt, so an outer `Err` guarantees that
    /// nothing was attempted.
    #[cfg(feature = "sql")]
    pub fn execute_sql_batch(
        &self,
        commands: Vec<SqlCommand>,
    ) -> Result<Vec<Result<SqlExecuteResponse, NodeError>>, NodeError> {
        self.require_execution_profile(ExecutionProfile::Sqlite)?;
        validate_sql_batch_len(commands.len())?;
        let mut members = Vec::with_capacity(commands.len());
        let mut aggregate_encoded_bytes = 0_usize;
        for command in commands {
            validate_field(
                "request_id",
                &command.request_id,
                MAX_REQUEST_ID_BYTES,
                false,
            )?;
            let payload = encode_sql_command_with_index(&command)?;
            validate_command_size(&payload)?;
            aggregate_encoded_bytes = aggregate_encoded_bytes.saturating_add(payload.len());
            if aggregate_encoded_bytes > MAX_COMMAND_BYTES {
                return Err(NodeError::ResourceExhausted(format!(
                    "SQL write batch exceeds {MAX_COMMAND_BYTES} aggregate encoded bytes"
                )));
            }
            members.push(RuntimeBatchMember {
                request_id: command.request_id.clone(),
                payload,
                operation: QueuedOperation::Sql(command),
            });
        }
        let single_statement_batch = members.iter().all(|member| {
            matches!(
                &member.operation,
                QueuedOperation::Sql(command) if command.statements.len() == 1
            )
        });
        let results = if single_statement_batch {
            self.execute_sql_group_commit(members)?
        } else {
            self.execute_client_batch(members)
        };
        Ok(results
            .into_iter()
            .map(|result| {
                result.and_then(|response| match response {
                    ClientWriteResponse::Sql(response) => Ok(response),
                    _ => Err(NodeError::Invariant(
                        "SQL batch returned a response for another profile".into(),
                    )),
                })
            })
            .collect())
    }

    #[cfg(feature = "sql")]
    fn execute_sql_with_results(
        &self,
        command: SqlCommand,
    ) -> Result<SqlExecuteResponse, NodeError> {
        self.execute_sql_batch(vec![command])?
            .into_iter()
            .next()
            .expect("one-member SQL batch returns one result")
    }

    #[cfg(feature = "sql")]
    fn execute_sql_group_commit(&self, members: Vec<RuntimeBatchMember>) -> SqlGroupCommitResult {
        let (job, leader) = self
            .sql_group_commit
            .enqueue(members, &self.operation_cancelled)?;
        if leader {
            if let Some(observer) = self.config.sql_write_profiler.clone() {
                self.run_sql_group_commit_leader(&mut EnabledSqlWritePhaseProfile::new(observer));
            } else {
                self.run_sql_group_commit_leader(&mut DisabledSqlWritePhaseProfile);
            }
        }
        job.wait(&self.operation_cancelled)
    }

    #[cfg(feature = "sql")]
    fn run_sql_group_commit_leader<P: SqlWritePhaseProfile>(&self, profile: &mut P) {
        let _commit = match self.lock_commit() {
            Ok(commit) => commit,
            Err(error) => {
                self.sql_group_commit.fail_pending(error);
                return;
            }
        };
        profile.commit_lock_acquired();

        loop {
            if !self
                .sql_group_commit
                .collect_until_full_or_timeout(self.config.writer_batch_window())
            {
                break;
            }
            let Some(jobs) = self.sql_group_commit.drain_next_group() else {
                break;
            };
            if let Err(error) = self
                .ensure_ready()
                .and_then(|_| self.ensure_writes_active())
            {
                for job in &jobs {
                    job.publish(Err(error.clone()));
                }
                self.sql_group_commit.fail_pending(error);
                return;
            }
            let execution = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                #[cfg(test)]
                if let Some(hook) = &self.sql_group_commit_before_execute_hook {
                    hook();
                }
                self.execute_sql_group_locked(&jobs, profile)
            }));
            let grouped_results = match execution {
                Ok(Ok(grouped_results)) => grouped_results,
                Ok(Err(error)) => {
                    let error = if matches!(
                        error,
                        NodeError::Unavailable(_) | NodeError::OutcomeUnknown(_)
                    ) {
                        error
                    } else {
                        self.latch(error)
                    };
                    for job in &jobs {
                        job.publish(Err(error.clone()));
                    }
                    self.sql_group_commit.fail_pending(error);
                    return;
                }
                Err(_) => {
                    let error =
                        self.latch(NodeError::Fatal("SQL group commit leader panicked".into()));
                    for job in &jobs {
                        job.publish(Err(error.clone()));
                    }
                    self.sql_group_commit.fail_pending(error);
                    return;
                }
            };
            for (job, results) in jobs.iter().zip(grouped_results) {
                job.publish(Ok(results));
            }
        }
    }

    #[cfg(feature = "sql")]
    fn execute_sql_group_locked<P: SqlWritePhaseProfile>(
        &self,
        jobs: &[Arc<SqlGroupCommitJob>],
        profile: &mut P,
    ) -> Result<Vec<Vec<Result<ClientWriteResponse, NodeError>>>, NodeError> {
        if self.operation_cancelled.load(Ordering::Acquire) {
            return Err(NodeError::Unavailable(
                "SQL group commit cancelled during shutdown".into(),
            ));
        }
        let total_members = jobs.iter().map(|job| job.member_count).sum();
        if total_members == 0 || total_members > MAX_SQL_WRITE_BATCH_MEMBERS {
            return Err(NodeError::Invariant(format!(
                "SQL group commit drained {total_members} members outside 1..={MAX_SQL_WRITE_BATCH_MEMBERS}"
            )));
        }
        let mut members = Vec::with_capacity(total_members);
        for job in jobs {
            let job_members = job.take_members()?;
            if job_members.len() != job.member_count {
                return Err(NodeError::Invariant(
                    "SQL group commit job member count changed while queued".into(),
                ));
            }
            members.extend(job_members);
        }
        let results = self.execute_sql_client_batch_locked(&members, profile);
        if self.operation_cancelled.load(Ordering::Acquire) {
            return Err(NodeError::Unavailable(
                "SQL group commit cancelled during shutdown".into(),
            ));
        }
        if self.is_fatal() {
            return Err(NodeError::Fatal(
                self.fatal_reason()
                    .unwrap_or_else(|| "SQL group commit failed fatally".into()),
            ));
        }
        if results.len() != total_members {
            return Err(NodeError::Invariant(format!(
                "SQL group commit returned {} results for {total_members} members",
                results.len()
            )));
        }
        let mut results = results.into_iter();
        let grouped = jobs
            .iter()
            .map(|job| results.by_ref().take(job.member_count).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        if results.next().is_some() || grouped.iter().map(Vec::len).sum::<usize>() != total_members
        {
            return Err(NodeError::Invariant(
                "SQL group commit result offsets were misaligned".into(),
            ));
        }
        Ok(grouped)
    }

    #[cfg(feature = "sql")]
    fn execute_client_batch(
        &self,
        members: Vec<RuntimeBatchMember>,
    ) -> Vec<Result<ClientWriteResponse, NodeError>> {
        if self.config.execution_profile != ExecutionProfile::Sqlite {
            return self.execute_profile_client_batch(members);
        }
        let is_single_statement_sql = members.iter().all(|member| {
            matches!(
                &member.operation,
                QueuedOperation::Sql(command) if command.statements.len() == 1
            )
        });
        if is_single_statement_sql {
            if let Some(observer) = self.config.sql_write_profiler.clone() {
                let mut profile = EnabledSqlWritePhaseProfile::new(observer);
                let _commit = match self.lock_commit() {
                    Ok(commit) => commit,
                    Err(error) => return members.into_iter().map(|_| Err(error.clone())).collect(),
                };
                profile.commit_lock_acquired();
                if let Err(error) = self
                    .ensure_ready()
                    .and_then(|_| self.ensure_writes_active())
                {
                    return members.into_iter().map(|_| Err(error.clone())).collect();
                }
                return self.execute_sql_client_batch_locked(&members, &mut profile);
            }
        }
        let _commit = match self.lock_commit() {
            Ok(commit) => commit,
            Err(error) => return members.into_iter().map(|_| Err(error.clone())).collect(),
        };
        if let Err(error) = self
            .ensure_ready()
            .and_then(|_| self.ensure_writes_active())
        {
            return members.into_iter().map(|_| Err(error.clone())).collect();
        }

        if is_single_statement_sql {
            return self
                .execute_sql_client_batch_locked(&members, &mut DisabledSqlWritePhaseProfile);
        }

        members
            .iter()
            .map(|member| self.execute_single_member_locked(member))
            .collect()
    }

    #[cfg(feature = "sql")]
    fn execute_sql_client_batch_locked<P: SqlWritePhaseProfile>(
        &self,
        members: &[RuntimeBatchMember],
        profile: &mut P,
    ) -> Vec<Result<ClientWriteResponse, NodeError>> {
        let classification_mark = profile.mark();
        let mut results = vec![None; members.len()];
        let mut pending = Vec::with_capacity(members.len());
        let mut canonical_by_request: HashMap<String, Vec<usize>> =
            HashMap::with_capacity(members.len());
        let mut lookup_indices = Vec::with_capacity(members.len());
        let mut aliases = vec![None; members.len()];
        let mut blocked_by = vec![None; members.len()];

        for (index, member) in members.iter().enumerate() {
            let QueuedOperation::Sql(command) = &member.operation else {
                unreachable!("SQL batch members were validated by the caller");
            };
            let canonicals = canonical_by_request
                .entry(command.request_id.clone())
                .or_default();
            if let Some(canonical) = canonicals
                .iter()
                .copied()
                .find(|canonical| members[*canonical].payload == member.payload)
            {
                aliases[index] = Some(canonical);
                continue;
            }
            blocked_by[index] = canonicals.last().copied();
            canonicals.push(index);
            lookup_indices.push(index);
        }

        let preflight = self.check_sql_members_bulk(members, &lookup_indices);
        let preflight = match preflight {
            Ok(preflight) => preflight,
            Err(error) => {
                profile.add_precheck_classification(classification_mark);
                return members.iter().map(|_| Err(error.clone())).collect();
            }
        };
        for (index, lookup) in preflight {
            match lookup {
                Ok(Some((outcome, sql_result))) => {
                    results[index] = Some(Ok(ClientWriteResponse::Sql(sql_execute_response(
                        write_response(outcome),
                        Some(sql_result),
                    ))));
                }
                Ok(None) => pending.push(index),
                Err(error) => results[index] = Some(Err(error)),
            }
        }
        profile.add_precheck_classification(classification_mark);

        'sql_batches: while !pending.is_empty() {
            if let Err(error) = self.ensure_writes_active() {
                for index in pending.drain(..) {
                    results[index] = Some(Err(error.clone()));
                }
                break;
            }
            let eligible = pending
                .iter()
                .copied()
                .filter(|index| {
                    blocked_by[*index].is_none_or(|predecessor| results[predecessor].is_some())
                })
                .take(MAX_SQL_WRITE_BATCH_MEMBERS)
                .collect::<Vec<_>>();
            if eligible.is_empty() {
                let error = self.latch(NodeError::Invariant(
                    "SQL writer batch has an unresolved duplicate dependency".into(),
                ));
                for index in pending.drain(..) {
                    results[index] = Some(Err(error.clone()));
                }
                break;
            }

            let (last_index, last_hash) = match self.ensure_materialized_tip() {
                Ok(tip) => tip,
                Err(error) => {
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            let slot = match last_index.checked_add(1) {
                Some(slot) => slot,
                None => {
                    let error = self.latch(NodeError::Invariant("qlog index is exhausted".into()));
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            let configuration = match self.configuration_state() {
                Ok(configuration) => configuration,
                Err(error) => {
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            let mut attempt_count = eligible.len();
            let (proposal_payload, prepared_results) = loop {
                let attempted = &eligible[..attempt_count];
                let batch_members = attempted
                    .iter()
                    .map(|index| {
                        let QueuedOperation::Sql(command) = &members[*index].operation else {
                            unreachable!("SQL batch members were validated above");
                        };
                        SqlBatchMember {
                            command,
                            request_payload: &members[*index].payload,
                        }
                    })
                    .collect::<Vec<_>>();
                let preparation_mark = profile.mark();
                let preparation = self.lock_sqlite().and_then(|sqlite| {
                    sqlite
                        .prepare_external_sql_batch_effect(&batch_members, last_index, last_hash)
                        .map_err(|error| self.map_sqlite_error(error))
                });
                profile.add_qwal_prepare(preparation_mark);
                let preparation = match preparation {
                    Ok(preparation) => preparation,
                    Err(NodeError::ResourceExhausted(message)) if attempt_count > 1 => {
                        attempt_count = attempt_count.div_ceil(2);
                        let _ = message;
                        continue;
                    }
                    Err(NodeError::ResourceExhausted(message)) => {
                        results[attempted[0]] = Some(Err(NodeError::ResourceExhausted(message)));
                        break (None, Vec::new());
                    }
                    Err(error) => {
                        for index in pending.drain(..) {
                            results[index] = Some(Err(error.clone()));
                        }
                        break (None, Vec::new());
                    }
                };
                if preparation.results.len() != attempted.len() {
                    let error = self.latch(NodeError::Invariant(
                        "SQLite batch preparation returned a misaligned result vector".into(),
                    ));
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break (None, Vec::new());
                }

                let mut proposed = Vec::with_capacity(attempted.len());
                for (index, member_result) in attempted
                    .iter()
                    .copied()
                    .zip(preparation.results.iter().cloned())
                {
                    match member_result {
                        Ok(result) => proposed.push((index, result)),
                        Err(error) => {
                            results[index] = Some(Err(self.map_sql_batch_member_error(error)))
                        }
                    }
                }
                let Some(preparation) = preparation.effect else {
                    if proposed.is_empty() {
                        break (None, Vec::new());
                    }
                    let error = self.latch(NodeError::Invariant(
                        "SQLite omitted the external effect for successful SQL members".into(),
                    ));
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break (None, Vec::new());
                };
                let command = match preparation
                    .build_command(configuration.digest(), slot, last_hash)
                    .map_err(|error| self.map_sqlite_error(error))
                {
                    Ok(command) => command,
                    Err(error) => {
                        for index in pending.drain(..) {
                            results[index] = Some(Err(error.clone()));
                        }
                        break (None, Vec::new());
                    }
                };
                let payload = match command.encode() {
                    Ok(payload) if payload.len() <= MAX_COMMAND_BYTES => payload,
                    Ok(_) => {
                        let error = self.latch(NodeError::Invariant(
                            "canonical QEFX command exceeds the generic command bound".into(),
                        ));
                        for index in pending.drain(..) {
                            results[index] = Some(Err(error.clone()));
                        }
                        break (None, Vec::new());
                    }
                    Err(error) => {
                        let error = self.latch(NodeError::Invariant(format!(
                            "canonical QEFX command encoding failed: {error}"
                        )));
                        for index in pending.drain(..) {
                            results[index] = Some(Err(error.clone()));
                        }
                        break (None, Vec::new());
                    }
                };
                let manifest_command = StoredCommand::new(EntryType::Command, payload.clone());
                let binding = effect_bundle_binding(&command, &manifest_command);
                let bundle = match RecorderEffectBundle::new(binding, preparation.chunks().to_vec())
                {
                    Ok(bundle) => bundle,
                    Err(error) => {
                        let error = self.latch(NodeError::Invariant(error.to_string()));
                        for index in pending.drain(..) {
                            results[index] = Some(Err(error.clone()));
                        }
                        break (None, Vec::new());
                    }
                };
                let request = match EffectBundleFinalizeRequest::new(bundle, manifest_command) {
                    Ok(request) => request,
                    Err(error) => {
                        let error = self.latch(NodeError::Invariant(error.to_string()));
                        for index in pending.drain(..) {
                            results[index] = Some(Err(error.clone()));
                        }
                        break (None, Vec::new());
                    }
                };
                if let Err(error) = self
                    .consensus
                    .finalize_effect_bundle_on_quorum(&self.consensus_context(), &request)
                {
                    // A competing proposer can decide this exact slot after
                    // the SQLite sandbox was prepared but before its bundle
                    // was finalized.  The rejected orphan must not turn into
                    // a client-visible no-quorum: materialize the certified
                    // winner and prepare the same requests at its successor.
                    if let Ok(DecisionInspection::Committed(winner)) = self
                        .consensus
                        .inspect_decision_at(&self.consensus_context(), slot, last_hash)
                    {
                        if let Err(error) =
                            self.persist_sql_entry_profiled(&winner, slot, last_hash, profile)
                        {
                            for index in pending.drain(..) {
                                results[index] = Some(Err(error.clone()));
                            }
                            break (None, Vec::new());
                        }
                        continue 'sql_batches;
                    }
                    let error = self.map_consensus_error(error);
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break (None, Vec::new());
                }
                break (Some(payload), proposed);
            };

            pending.retain(|index| results[*index].is_none());
            let Some(proposal_payload) = proposal_payload else {
                continue;
            };
            let consensus_mark = profile.mark();
            #[cfg(feature = "tuner")]
            let tuner_propose_start = std::time::Instant::now();
            let entry = self.consensus.propose_at(
                self.consensus_context(),
                slot,
                last_hash,
                Command::new(CommandKind::Deterministic, proposal_payload.clone()),
            );
            profile.add_consensus_propose(consensus_mark);
            #[cfg(feature = "tuner")]
            {
                let elapsed = tuner_propose_start.elapsed();
                match &entry {
                    Ok(_) => self.record_tuner_consensus_success(elapsed),
                    Err(error) => self.record_tuner_consensus_error(error),
                }
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    let error = self.map_consensus_error(error);
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            if let Err(error) = self.persist_sql_entry_profiled(&entry, slot, last_hash, profile) {
                for index in pending.drain(..) {
                    results[index] = Some(Err(error.clone()));
                }
                break;
            }

            let exact_winner =
                entry.entry_type == EntryType::Command && entry.payload == proposal_payload;
            let exact_winner_member_count = exact_winner.then_some(prepared_results.len());
            if exact_winner {
                for (index, sql_result) in prepared_results {
                    results[index] = Some(Ok(ClientWriteResponse::Sql(sql_execute_response(
                        WriteResponse {
                            applied_index: entry.index,
                            hash: entry.hash,
                        },
                        Some(sql_result),
                    ))));
                }
                pending.retain(|index| results[*index].is_none());
            }

            let current_pending = std::mem::take(&mut pending);
            let classification_mark = profile.mark();
            let post_commit = self.check_sql_members_bulk(members, &current_pending);
            let post_commit = match post_commit {
                Ok(post_commit) => post_commit,
                Err(error) => {
                    for index in current_pending {
                        results[index] = Some(Err(error.clone()));
                    }
                    profile.add_precheck_classification(classification_mark);
                    if let Some(batch_member_count) = exact_winner_member_count {
                        profile.record_success(batch_member_count);
                    }
                    break;
                }
            };
            for (index, lookup) in post_commit {
                match lookup {
                    Ok(Some((outcome, sql_result))) => {
                        results[index] = Some(Ok(ClientWriteResponse::Sql(sql_execute_response(
                            write_response(outcome),
                            Some(sql_result),
                        ))));
                    }
                    Ok(None) => pending.push(index),
                    Err(error) => results[index] = Some(Err(error)),
                }
            }
            profile.add_precheck_classification(classification_mark);
            if let Some(batch_member_count) = exact_winner_member_count {
                profile.record_success(batch_member_count);
            }
        }

        for (index, canonical) in aliases.into_iter().enumerate() {
            if let Some(canonical) = canonical {
                results[index] = results[canonical].clone();
            }
        }
        results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(self.latch(NodeError::Invariant(
                        "SQL writer batch omitted a request result".into(),
                    )))
                })
            })
            .collect()
    }

    #[cfg(feature = "sql")]
    fn check_sql_members_bulk(
        &self,
        members: &[RuntimeBatchMember],
        indices: &[usize],
    ) -> Result<Vec<(usize, CheckedSqlRequest)>, NodeError> {
        if indices.is_empty() {
            return Ok(Vec::new());
        }
        let mut rounds = Vec::<Vec<usize>>::new();
        let mut occurrence_by_request = HashMap::<String, usize>::new();
        for index in indices {
            let QueuedOperation::Sql(command) = &members[*index].operation else {
                return Err(self.latch(NodeError::Invariant(
                    "non-SQL member reached SQL receipt precheck".into(),
                )));
            };
            let round = occurrence_by_request
                .entry(command.request_id.clone())
                .or_default();
            if rounds.len() == *round {
                rounds.push(Vec::new());
            }
            rounds[*round].push(*index);
            *round += 1;
        }

        let sqlite = self.lock_sqlite()?;
        let mut checked = Vec::with_capacity(indices.len());
        for round in rounds {
            let requests = round
                .iter()
                .map(|index| {
                    let QueuedOperation::Sql(command) = &members[*index].operation else {
                        unreachable!("SQL receipt round contains only SQL members");
                    };
                    (
                        command.request_id.as_str(),
                        members[*index].payload.as_slice(),
                    )
                })
                .collect::<Vec<_>>();
            let lookups = sqlite
                .check_sql_requests(&requests)
                .map_err(|error| self.map_sqlite_error(error))?;
            if lookups.len() != round.len() {
                return Err(self.latch(NodeError::Invariant(
                    "SQLite bulk receipt precheck returned a misaligned result vector".into(),
                )));
            }
            for (index, lookup) in round.into_iter().zip(lookups) {
                checked.push((
                    index,
                    match lookup {
                        Ok(Some((outcome, Some(sql_result)))) => Ok(Some((outcome, sql_result))),
                        Ok(Some((_, None))) => Err(self.latch(NodeError::Invariant(
                            "stored SQL receipt omitted its command result".into(),
                        ))),
                        Ok(None) => Ok(None),
                        Err(error) => Err(self.map_sql_batch_member_error(error)),
                    },
                ));
            }
        }
        Ok(checked)
    }

    #[cfg(feature = "kv")]
    fn execute_kv_group_commit(&self, members: Vec<RuntimeBatchMember>) -> KvGroupCommitResult {
        let (job, leader) = self
            .kv_group_commit
            .enqueue(members, &self.operation_cancelled)?;
        if leader {
            self.run_kv_group_commit_leader();
        }
        job.wait(&self.operation_cancelled)
    }

    #[cfg(feature = "kv")]
    fn run_kv_group_commit_leader(&self) {
        let _commit = match self.lock_commit() {
            Ok(commit) => commit,
            Err(error) => {
                self.kv_group_commit.fail_pending(error);
                return;
            }
        };

        loop {
            if !self
                .kv_group_commit
                .collect_until_full_or_timeout(self.config.writer_batch_window())
            {
                break;
            }
            let Some(jobs) = self.kv_group_commit.drain_next_group() else {
                break;
            };
            if let Err(error) = self
                .ensure_ready()
                .and_then(|_| self.ensure_writes_active())
            {
                for job in &jobs {
                    job.publish(Err(error.clone()));
                }
                self.kv_group_commit.fail_pending(error);
                return;
            }
            let execution = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                #[cfg(test)]
                if let Some(hook) = &self.kv_group_commit_before_execute_hook {
                    hook();
                }
                self.execute_kv_group_locked(&jobs)
            }));
            let grouped_results = match execution {
                Ok(Ok(grouped_results)) => grouped_results,
                Ok(Err(error)) => {
                    let error = if matches!(
                        error,
                        NodeError::Unavailable(_) | NodeError::OutcomeUnknown(_)
                    ) {
                        error
                    } else {
                        self.latch(error)
                    };
                    for job in &jobs {
                        job.publish(Err(error.clone()));
                    }
                    self.kv_group_commit.fail_pending(error);
                    return;
                }
                Err(_) => {
                    let error =
                        self.latch(NodeError::Fatal("KV group commit leader panicked".into()));
                    for job in &jobs {
                        job.publish(Err(error.clone()));
                    }
                    self.kv_group_commit.fail_pending(error);
                    return;
                }
            };
            for (job, results) in jobs.iter().zip(grouped_results) {
                job.publish(Ok(results));
            }
        }
    }

    #[cfg(feature = "kv")]
    fn execute_kv_group_locked(
        &self,
        jobs: &[Arc<KvGroupCommitJob>],
    ) -> Result<Vec<Vec<Result<ClientWriteResponse, NodeError>>>, NodeError> {
        if self.operation_cancelled.load(Ordering::Acquire) {
            return Err(NodeError::Unavailable(
                "KV group commit cancelled during shutdown".into(),
            ));
        }
        let total_members = jobs.iter().map(|job| job.member_count).sum();
        if total_members == 0 || total_members > MAX_KV_GROUP_COMMIT_MEMBERS {
            return Err(NodeError::Invariant(format!(
                "KV group commit drained {total_members} members outside 1..={MAX_KV_GROUP_COMMIT_MEMBERS}"
            )));
        }
        let mut members = Vec::with_capacity(total_members);
        for job in jobs {
            let job_members = job.take_members()?;
            if job_members.len() != job.member_count {
                return Err(NodeError::Invariant(
                    "KV group commit job member count changed while queued".into(),
                ));
            }
            members.extend(job_members);
        }
        let results = self.execute_kv_client_batch_locked(&members);
        #[cfg(test)]
        if let Some(hook) = &self.kv_group_commit_after_execute_hook {
            hook(self);
        }
        if results.len() != total_members {
            let error = self.latch(NodeError::Invariant(format!(
                "KV group commit returned {} results for {total_members} members",
                results.len()
            )));
            return Ok(jobs
                .iter()
                .map(|job| vec![Err(error.clone()); job.member_count])
                .collect());
        }
        let mut results = results.into_iter();
        let grouped = jobs
            .iter()
            .map(|job| results.by_ref().take(job.member_count).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        debug_assert!(results.next().is_none());
        debug_assert_eq!(grouped.iter().map(Vec::len).sum::<usize>(), total_members);
        Ok(grouped)
    }

    #[cfg(not(feature = "sql"))]
    fn execute_client_batch(
        &self,
        members: Vec<RuntimeBatchMember>,
    ) -> Vec<Result<ClientWriteResponse, NodeError>> {
        self.execute_profile_client_batch(members)
    }

    fn execute_profile_client_batch(
        &self,
        members: Vec<RuntimeBatchMember>,
    ) -> Vec<Result<ClientWriteResponse, NodeError>> {
        #[cfg(feature = "graph")]
        if self.config.execution_profile == ExecutionProfile::Graph {
            return self.execute_graph_client_batch(members);
        }
        #[cfg(feature = "kv")]
        if self.config.execution_profile == ExecutionProfile::Kv {
            return self.execute_kv_client_batch(members);
        }
        let _commit = match self.lock_commit() {
            Ok(commit) => commit,
            Err(error) => return members.into_iter().map(|_| Err(error.clone())).collect(),
        };
        if let Err(error) = self
            .ensure_ready()
            .and_then(|_| self.ensure_writes_active())
        {
            return members.into_iter().map(|_| Err(error.clone())).collect();
        }
        members
            .iter()
            .map(|member| self.execute_profile_member_locked(member))
            .collect()
    }

    #[cfg(feature = "graph")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "kv")),
        allow(irrefutable_let_patterns, unreachable_patterns)
    )]
    fn execute_graph_client_batch(
        &self,
        members: Vec<RuntimeBatchMember>,
    ) -> Vec<Result<ClientWriteResponse, NodeError>> {
        let _commit = match self.lock_commit() {
            Ok(commit) => commit,
            Err(error) => return members.into_iter().map(|_| Err(error.clone())).collect(),
        };
        if let Err(error) = self
            .ensure_ready()
            .and_then(|_| self.ensure_writes_active())
        {
            return members.into_iter().map(|_| Err(error.clone())).collect();
        }

        let mut results = vec![None; members.len()];
        let mut pending = Vec::with_capacity(members.len());
        let mut canonical_by_request = HashMap::with_capacity(members.len());
        let mut aliases = vec![None; members.len()];
        for (index, member) in members.iter().enumerate() {
            let QueuedOperation::Graph(command) = &member.operation else {
                results[index] = Some(Err(NodeError::ExecutionProfileMismatch {
                    expected: ExecutionProfile::Graph,
                    actual: ExecutionProfile::Sqlite,
                }));
                continue;
            };
            match self.check_graph_request(command.request_id(), &member.payload) {
                Ok(Some(record)) => {
                    results[index] = Some(Ok(ClientWriteResponse::Graph(
                        GraphMutationOutcome::from_record(record),
                    )));
                }
                Ok(None) => match classify_pending_request(
                    &mut canonical_by_request,
                    &members,
                    index,
                    command.request_id(),
                ) {
                    Ok(None) => pending.push(index),
                    Ok(Some(canonical)) => aliases[index] = Some(canonical),
                    Err(error) => results[index] = Some(Err(error)),
                },
                Err(error) => results[index] = Some(Err(error)),
            }
        }

        while !pending.is_empty() {
            if let Err(error) = self.ensure_writes_active() {
                for index in pending.drain(..) {
                    results[index] = Some(Err(error.clone()));
                }
                break;
            }
            if pending.len() == 1 {
                let index = pending[0];
                results[index] = Some(self.execute_profile_member_locked(&members[index]));
                break;
            }
            let commands = pending
                .iter()
                .map(|index| match &members[*index].operation {
                    QueuedOperation::Graph(command) => command.clone(),
                    _ => unreachable!("graph pending members were validated above"),
                })
                .collect::<Vec<_>>();
            let full_payload = match encode_replicated_graph_batch(&commands) {
                Ok(payload) => payload,
                Err(error) => {
                    let error = NodeError::InvalidRequest(error.to_string());
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            let (proposal_count, proposal_payload) = if full_payload.len() <= MAX_COMMAND_BYTES {
                (commands.len(), full_payload)
            } else {
                let Some(prefix) = largest_fitting_graph_batch_prefix(&commands) else {
                    let index = pending.remove(0);
                    results[index] = Some(self.execute_profile_member_locked(&members[index]));
                    continue;
                };
                prefix
            };
            let proposed_indices = pending[..proposal_count].to_vec();
            let (last_index, last_hash) = match self.ensure_materialized_tip() {
                Ok(tip) => tip,
                Err(error) => {
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            let slot = match last_index.checked_add(1) {
                Some(slot) => slot,
                None => {
                    let error = self.latch(NodeError::Invariant("qlog index is exhausted".into()));
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            let entry = match self.consensus.propose_at(
                self.consensus_context(),
                slot,
                last_hash,
                Command::new(CommandKind::Deterministic, proposal_payload.clone()),
            ) {
                Ok(entry) => entry,
                Err(error) => {
                    let error = self.map_consensus_error(error);
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            if let Err(error) = self.persist_entry(&entry, slot, last_hash) {
                for index in pending.drain(..) {
                    results[index] = Some(Err(error.clone()));
                }
                break;
            }

            let mut remaining = Vec::new();
            for index in pending.drain(..) {
                let member = &members[index];
                let QueuedOperation::Graph(command) = &member.operation else {
                    unreachable!("graph pending members were validated above");
                };
                match self.check_graph_request(command.request_id(), &member.payload) {
                    Ok(Some(record)) => {
                        results[index] = Some(Ok(ClientWriteResponse::Graph(
                            GraphMutationOutcome::from_record(record),
                        )));
                    }
                    Ok(None) => remaining.push(index),
                    Err(error) => results[index] = Some(Err(error)),
                }
            }
            if entry.entry_type == EntryType::Command
                && entry.payload == proposal_payload
                && remaining
                    .iter()
                    .any(|index| proposed_indices.contains(index))
            {
                let error = self.latch(NodeError::Invariant(
                    "committed graph batch did not record every request".into(),
                ));
                for index in remaining.drain(..) {
                    results[index] = Some(Err(error.clone()));
                }
            }
            pending = remaining;
        }

        for (index, canonical) in aliases.into_iter().enumerate() {
            if let Some(canonical) = canonical {
                results[index] = results[canonical].clone();
            }
        }

        results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(self.latch(NodeError::Invariant(
                        "graph writer batch omitted a request result".into(),
                    )))
                })
            })
            .collect()
    }

    #[cfg(feature = "kv")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "graph")),
        allow(irrefutable_let_patterns, unreachable_patterns)
    )]
    fn execute_kv_client_batch(
        &self,
        members: Vec<RuntimeBatchMember>,
    ) -> Vec<Result<ClientWriteResponse, NodeError>> {
        let _commit = match self.lock_commit() {
            Ok(commit) => commit,
            Err(error) => return members.into_iter().map(|_| Err(error.clone())).collect(),
        };
        if let Err(error) = self
            .ensure_ready()
            .and_then(|_| self.ensure_writes_active())
        {
            return members.into_iter().map(|_| Err(error.clone())).collect();
        }
        self.execute_kv_client_batch_locked(&members)
    }

    #[cfg(feature = "kv")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "graph")),
        allow(irrefutable_let_patterns, unreachable_patterns)
    )]
    fn execute_kv_client_batch_locked(
        &self,
        members: &[RuntimeBatchMember],
    ) -> Vec<Result<ClientWriteResponse, NodeError>> {
        let mut results = vec![None; members.len()];
        let mut pending = Vec::with_capacity(members.len());
        let mut canonical_by_request = HashMap::with_capacity(members.len());
        let mut aliases = vec![None; members.len()];
        let mut lookup_indices = Vec::with_capacity(members.len());
        for (index, member) in members.iter().enumerate() {
            let QueuedOperation::Kv(command) = &member.operation else {
                results[index] = Some(Err(NodeError::ExecutionProfileMismatch {
                    expected: ExecutionProfile::Kv,
                    actual: ExecutionProfile::Sqlite,
                }));
                continue;
            };
            match classify_pending_request(
                &mut canonical_by_request,
                members,
                index,
                command.request_id(),
            ) {
                Ok(None) => lookup_indices.push(index),
                Ok(Some(canonical)) => aliases[index] = Some(canonical),
                Err(error) => results[index] = Some(Err(error)),
            }
        }
        let preflight = match self.check_kv_members_bulk(members, &lookup_indices) {
            Ok(preflight) => preflight,
            Err(error) => return members.iter().map(|_| Err(error.clone())).collect(),
        };
        for (index, lookup) in preflight {
            match lookup {
                Ok(Some(record)) => {
                    results[index] = Some(Ok(ClientWriteResponse::Kv(
                        KvMutationOutcome::from_record(record),
                    )));
                }
                Ok(None) => pending.push(index),
                Err(error) => results[index] = Some(Err(error)),
            }
        }

        while !pending.is_empty() {
            if let Err(error) = self.ensure_writes_active() {
                for index in pending.drain(..) {
                    results[index] = Some(Err(error.clone()));
                }
                break;
            }
            if pending.len() == 1 {
                let index = pending[0];
                results[index] = Some(self.execute_profile_member_locked(&members[index]));
                break;
            }
            let commands = pending
                .iter()
                .map(|index| match &members[*index].operation {
                    QueuedOperation::Kv(command) => command.clone(),
                    _ => unreachable!("KV pending members were validated above"),
                })
                .collect::<Vec<_>>();
            let full_payload = match encode_replicated_kv_batch(&commands) {
                Ok(payload) => payload,
                Err(error) => {
                    let error = NodeError::InvalidRequest(error.to_string());
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            let (proposal_count, proposal_payload) = if full_payload.len() <= MAX_COMMAND_BYTES {
                (commands.len(), full_payload)
            } else {
                let Some(prefix) = largest_fitting_kv_batch_prefix(&commands) else {
                    let index = pending.remove(0);
                    results[index] = Some(self.execute_profile_member_locked(&members[index]));
                    continue;
                };
                prefix
            };
            let proposed_indices = pending[..proposal_count].to_vec();
            let (last_index, last_hash) = match self.ensure_materialized_tip() {
                Ok(tip) => tip,
                Err(error) => {
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            let slot = match last_index.checked_add(1) {
                Some(slot) => slot,
                None => {
                    let error = self.latch(NodeError::Invariant("qlog index is exhausted".into()));
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            let entry = match self.consensus.propose_at(
                self.consensus_context(),
                slot,
                last_hash,
                Command::new(CommandKind::Deterministic, proposal_payload.clone()),
            ) {
                Ok(entry) => entry,
                Err(error) => {
                    let error = self.map_consensus_error(error);
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            if let Err(error) = self.persist_entry(&entry, slot, last_hash) {
                for index in pending.drain(..) {
                    results[index] = Some(Err(error.clone()));
                }
                break;
            }

            let current_pending = std::mem::take(&mut pending);
            let post_commit = match self.check_kv_members_bulk(members, &current_pending) {
                Ok(post_commit) => post_commit,
                Err(error) => {
                    for index in current_pending {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            let mut remaining = Vec::new();
            for (index, lookup) in post_commit {
                match lookup {
                    Ok(Some(record)) => {
                        results[index] = Some(Ok(ClientWriteResponse::Kv(
                            KvMutationOutcome::from_record(record),
                        )));
                    }
                    Ok(None) => remaining.push(index),
                    Err(error) => results[index] = Some(Err(error)),
                }
            }
            if entry.entry_type == EntryType::Command
                && entry.payload == proposal_payload
                && remaining
                    .iter()
                    .any(|index| proposed_indices.contains(index))
            {
                let error = self.latch(NodeError::Invariant(
                    "committed KV batch did not record every request".into(),
                ));
                for index in remaining.drain(..) {
                    results[index] = Some(Err(error.clone()));
                }
            }
            pending = remaining;
        }

        for (index, canonical) in aliases.into_iter().enumerate() {
            if let Some(canonical) = canonical {
                results[index] = results[canonical].clone();
            }
        }

        results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(self.latch(NodeError::Invariant(
                        "KV writer batch omitted a request result".into(),
                    )))
                })
            })
            .collect()
    }

    #[cfg(feature = "kv")]
    #[allow(clippy::infallible_destructuring_match)]
    fn check_kv_members_bulk(
        &self,
        members: &[RuntimeBatchMember],
        indices: &[usize],
    ) -> Result<Vec<KvMemberCheck>, NodeError> {
        if indices.is_empty() {
            return Ok(Vec::new());
        }
        let materializer = self.lock_materializer()?;
        let kv = match &*materializer {
            Materializer::Kv(kv) => kv,
            #[cfg(feature = "sql")]
            Materializer::Sql(_) => {
                return Err(NodeError::ExecutionProfileMismatch {
                    expected: ExecutionProfile::Kv,
                    actual: ExecutionProfile::Sqlite,
                });
            }
            #[cfg(feature = "graph")]
            Materializer::Graph(_) => {
                return Err(NodeError::ExecutionProfileMismatch {
                    expected: ExecutionProfile::Kv,
                    actual: ExecutionProfile::Graph,
                });
            }
        };
        let requests = indices
            .iter()
            .map(|index| {
                let command = match &members[*index].operation {
                    QueuedOperation::Kv(command) => command,
                    #[cfg(feature = "sql")]
                    QueuedOperation::KeyValue { .. } | QueuedOperation::Sql(_) => {
                        unreachable!("KV receipt lookup contains only KV members")
                    }
                    #[cfg(feature = "graph")]
                    QueuedOperation::Graph(_) => {
                        unreachable!("KV receipt lookup contains only KV members")
                    }
                };
                (command.request_id(), members[*index].payload.as_slice())
            })
            .collect::<Vec<_>>();
        let lookups = kv
            .check_requests(&requests)
            .map_err(|error| NodeError::InvalidRequest(error.to_string()))?;
        if lookups.len() != indices.len() {
            return Err(self.latch(NodeError::Invariant(
                "KV bulk receipt lookup returned a misaligned result vector".into(),
            )));
        }
        Ok(indices
            .iter()
            .copied()
            .zip(
                lookups.into_iter().map(|lookup| {
                    lookup.map_err(|error| NodeError::InvalidRequest(error.to_string()))
                }),
            )
            .collect())
    }

    fn execute_profile_member_locked(
        &self,
        member: &RuntimeBatchMember,
    ) -> Result<ClientWriteResponse, NodeError> {
        match &member.operation {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            QueuedOperation::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "graph")]
            QueuedOperation::Graph(command) => self
                .mutate_graph_payload_locked(command, member.payload.clone())
                .map(ClientWriteResponse::Graph),
            #[cfg(feature = "kv")]
            QueuedOperation::Kv(command) => self
                .mutate_kv_payload_locked(command, member.payload.clone())
                .map(ClientWriteResponse::Kv),
            #[cfg(feature = "sql")]
            QueuedOperation::KeyValue { .. } | QueuedOperation::Sql(_) => {
                Err(NodeError::ExecutionProfileMismatch {
                    expected: self.config.execution_profile,
                    actual: ExecutionProfile::Sqlite,
                })
            }
        }
    }

    #[cfg(feature = "sql")]
    fn execute_single_member_locked(
        &self,
        member: &RuntimeBatchMember,
    ) -> Result<ClientWriteResponse, NodeError> {
        if let QueuedOperation::Sql(command) = &member.operation {
            self.execute_sql_payload_locked(command, member.payload.clone())
                .map(|outcome| {
                    ClientWriteResponse::Sql(sql_execute_response(
                        outcome.response,
                        outcome.sql_result,
                    ))
                })
        } else if let QueuedOperation::KeyValue { key, value } = &member.operation {
            self.execute_put_payload_locked(&member.request_id, key, value, member.payload.clone())
                .map(|outcome| ClientWriteResponse::KeyValue(outcome.response))
        } else {
            Err(NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Sqlite,
                actual: self.config.execution_profile,
            })
        }
    }

    #[cfg(feature = "sql")]
    fn execute_sql_payload_locked(
        &self,
        command: &SqlCommand,
        request_payload: Vec<u8>,
    ) -> Result<ExecutedPayload, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        if let Some(outcome) = self.check_request(&command.request_id, &request_payload)? {
            return Ok(ExecutedPayload {
                response: write_response(outcome),
                sql_result: self.replay_sql_result(
                    &command.request_id,
                    &request_payload,
                    outcome,
                )?,
            });
        }

        loop {
            self.ensure_writes_active()?;
            let (last_index, last_hash) = self.ensure_materialized_tip()?;
            let slot = last_index.checked_add(1).ok_or_else(|| {
                self.latch(NodeError::Invariant("qlog index is exhausted".into()))
            })?;
            let proposal_payload = self.prepare_external_sql_proposal(
                command,
                &request_payload,
                last_index,
                last_hash,
                slot,
            )?;
            let entry = self
                .consensus
                .propose_at(
                    self.consensus_context(),
                    slot,
                    last_hash,
                    Command::new(CommandKind::Deterministic, proposal_payload.clone()),
                )
                .map_err(|error| self.map_consensus_error(error))?;
            let sql_result = self.persist_sql_entry_profiled(
                &entry,
                slot,
                last_hash,
                &mut DisabledSqlWritePhaseProfile,
            )?;
            if let Some(outcome) = self.check_request(&command.request_id, &request_payload)? {
                return Ok(ExecutedPayload {
                    response: write_response(outcome),
                    sql_result: sql_result.or(self.replay_sql_result(
                        &command.request_id,
                        &request_payload,
                        outcome,
                    )?),
                });
            }
            if entry.entry_type == EntryType::Command && entry.payload == proposal_payload {
                return Err(self.latch(NodeError::Invariant(
                    "committed SQL request was not recorded by SQLite".into(),
                )));
            }
        }
    }

    #[cfg(feature = "sql")]
    fn prepare_external_sql_proposal(
        &self,
        command: &SqlCommand,
        request_payload: &[u8],
        base_index: LogIndex,
        base_hash: LogHash,
        intended_slot: LogIndex,
    ) -> Result<Vec<u8>, NodeError> {
        let sqlite = self.lock_sqlite()?;
        let preparation = sqlite.prepare_external_sql_batch_effect(
            &[SqlBatchMember {
                command,
                request_payload,
            }],
            base_index,
            base_hash,
        );
        let preparation = match preparation {
            Ok(preparation) => preparation,
            Err(rhiza_sql::Error::ResourceExhausted(message)) => {
                return Err(NodeError::ResourceExhausted(message));
            }
            Err(error) => return Err(self.map_sqlite_error(error)),
        };
        let result = preparation
            .results
            .first()
            .cloned()
            .expect("one-member SQL preparation returns one result");
        if let Err(error) = result {
            if let rhiza_sql::Error::ResourceExhausted(message) = error {
                return Err(NodeError::ResourceExhausted(message));
            }
            if let rhiza_sql::Error::RequestConflict(conflict) = error {
                return Err(NodeError::RequestConflict(conflict));
            }
            let message = error.to_string();
            let statement_index = first_invalid_sql_statement(command, |prefix| {
                let Ok(prefix_payload) = encode_sql_command(prefix) else {
                    return true;
                };
                let prefix_member = [SqlBatchMember {
                    command: prefix,
                    request_payload: &prefix_payload,
                }];
                match sqlite.prepare_external_sql_batch_effect(
                    &prefix_member,
                    base_index,
                    base_hash,
                ) {
                    Ok(preparation) => preparation
                        .results
                        .first()
                        .cloned()
                        .is_none_or(|result| result.is_err()),
                    Err(_) => true,
                }
            });
            return match statement_index {
                Some(statement_index) => Err(NodeError::InvalidSqlStatement {
                    statement_index,
                    message,
                }),
                None => Err(NodeError::InvalidRequest(message)),
            };
        }
        let preparation = preparation.effect.ok_or_else(|| {
            self.latch(NodeError::Invariant(
                "successful SQL preparation omitted its external effect".into(),
            ))
        })?;
        let configuration = self.configuration_state()?;
        let external = preparation
            .build_command(configuration.digest(), intended_slot, base_hash)
            .map_err(|error| self.map_sqlite_error(error))?;
        let payload = external.encode().map_err(|error| {
            self.latch(NodeError::Invariant(format!(
                "canonical QEFX encoding failed: {error}"
            )))
        })?;
        validate_command_size(&payload)?;
        let manifest_command = StoredCommand::new(EntryType::Command, payload.clone());
        let binding = effect_bundle_binding(&external, &manifest_command);
        let bundle = RecorderEffectBundle::new(binding, preparation.chunks().to_vec())
            .map_err(|error| self.latch(NodeError::Invariant(error.to_string())))?;
        let request = EffectBundleFinalizeRequest::new(bundle, manifest_command)
            .map_err(|error| self.latch(NodeError::Invariant(error.to_string())))?;
        self.consensus
            .finalize_effect_bundle_on_quorum(&self.consensus_context(), &request)
            .map_err(|error| self.map_consensus_error(error))?;
        Ok(payload)
    }

    #[cfg(feature = "sql")]
    fn execute_put_payload_locked(
        &self,
        request_id: &str,
        key: &str,
        value: &str,
        payload: Vec<u8>,
    ) -> Result<ExecutedPayload, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;

        if let Some(outcome) = self.check_request(request_id, &payload)? {
            return Ok(ExecutedPayload {
                response: write_response(outcome),
                sql_result: None,
            });
        }

        loop {
            self.ensure_writes_active()?;
            let (last_index, last_hash) = self.ensure_materialized_tip()?;
            let slot = last_index.checked_add(1).ok_or_else(|| {
                self.latch(NodeError::Invariant("qlog index is exhausted".into()))
            })?;
            let preparation = self
                .lock_sqlite()?
                .prepare_external_put_effect(
                    request_id, key, value, &payload, last_index, last_hash,
                )
                .map_err(|error| self.map_sqlite_error(error))?;
            let configuration = self.configuration_state()?;
            let external = preparation
                .build_command(configuration.digest(), slot, last_hash)
                .map_err(|error| self.map_sqlite_error(error))?;
            let proposal_payload = external.encode().map_err(|error| {
                self.latch(NodeError::Invariant(format!(
                    "canonical QEFX encoding failed: {error}"
                )))
            })?;
            validate_command_size(&proposal_payload)?;
            let manifest_command = StoredCommand::new(EntryType::Command, proposal_payload.clone());
            let binding = effect_bundle_binding(&external, &manifest_command);
            let bundle = RecorderEffectBundle::new(binding, preparation.chunks().to_vec())
                .map_err(|error| self.latch(NodeError::Invariant(error.to_string())))?;
            let request = EffectBundleFinalizeRequest::new(bundle, manifest_command)
                .map_err(|error| self.latch(NodeError::Invariant(error.to_string())))?;
            self.consensus
                .finalize_effect_bundle_on_quorum(&self.consensus_context(), &request)
                .map_err(|error| self.map_consensus_error(error))?;
            let entry = self
                .consensus
                .propose_at(
                    self.consensus_context(),
                    slot,
                    last_hash,
                    Command::new(CommandKind::Deterministic, proposal_payload.clone()),
                )
                .map_err(|error| self.map_consensus_error(error))?;
            self.persist_sql_entry_profiled(
                &entry,
                slot,
                last_hash,
                &mut DisabledSqlWritePhaseProfile,
            )?;

            if let Some(outcome) = self.check_request(request_id, &payload)? {
                return Ok(ExecutedPayload {
                    response: write_response(outcome),
                    sql_result: None,
                });
            }
            if entry.entry_type == EntryType::Command && entry.payload == proposal_payload {
                return Err(self.latch(NodeError::Invariant(
                    "committed key/value write request was not recorded by SQLite QWAL".into(),
                )));
            }
        }
    }

    #[cfg(feature = "sql")]
    fn replay_sql_result(
        &self,
        request_id: &str,
        payload: &[u8],
        outcome: RequestOutcome,
    ) -> Result<Option<SqlCommandResult>, NodeError> {
        let sqlite = self.lock_sqlite()?;
        let stored = sqlite
            .check_sql_request(request_id, payload)
            .map_err(|error| self.map_sqlite_error(error))?
            .ok_or_else(|| {
                self.latch(NodeError::Invariant(
                    "committed SQL request result is missing".into(),
                ))
            })?;
        if stored.0 != outcome {
            return Err(self.latch(NodeError::Invariant(
                "stored SQL request outcome changed".into(),
            )));
        }
        Ok(stored.1)
    }

    #[cfg(feature = "sql")]
    fn map_sql_batch_member_error(&self, error: rhiza_sql::Error) -> NodeError {
        match error {
            rhiza_sql::Error::RequestConflict(conflict) => NodeError::RequestConflict(conflict),
            rhiza_sql::Error::ResourceExhausted(message) => NodeError::ResourceExhausted(message),
            rhiza_sql::Error::InvalidCommand(message) | rhiza_sql::Error::Sqlite(message) => {
                NodeError::InvalidSqlStatement {
                    statement_index: 0,
                    message,
                }
            }
            other => self.map_sqlite_error(other),
        }
    }

    #[cfg(feature = "sql")]
    pub fn read(&self, key: &str, consistency: ReadConsistency) -> Result<ReadResponse, NodeError> {
        validate_key(key)?;
        match consistency {
            ReadConsistency::Local => self.read_local(key, None),
            ReadConsistency::AppliedIndex(required) => self.read_local(key, Some(required)),
            ReadConsistency::ReadBarrier => {
                let anchor = self.establish_read_barrier()?;
                let _commit = self.lock_commit()?;
                self.ensure_ready()?;
                self.ensure_writes_active()?;
                self.validate_read_barrier_descendant_locked(anchor)?;
                self.read_local(key, Some(anchor.index()))
            }
        }
    }

    #[cfg(feature = "sql")]
    pub fn query_sql(
        &self,
        statement: &SqlStatement,
        consistency: ReadConsistency,
        max_rows: u32,
    ) -> Result<SqlQueryResponse, NodeError> {
        if max_rows == 0 || max_rows > MAX_SQL_MAX_ROWS {
            return Err(NodeError::InvalidRequest(format!(
                "max_rows must be between 1 and {MAX_SQL_MAX_ROWS}"
            )));
        }
        match consistency {
            ReadConsistency::Local => self.query_sql_local(statement, None, max_rows),
            ReadConsistency::AppliedIndex(required) => {
                self.query_sql_local(statement, Some(required), max_rows)
            }
            ReadConsistency::ReadBarrier => {
                let anchor = self.establish_read_barrier()?;
                let _commit = self.lock_commit()?;
                self.ensure_ready()?;
                self.ensure_writes_active()?;
                self.validate_read_barrier_descendant_locked(anchor)?;
                self.query_sql_local(statement, Some(anchor.index()), max_rows)
            }
        }
    }

    pub fn applied_index(&self) -> Result<LogIndex, NodeError> {
        self.ensure_ready()?;
        self.lock_materializer()?
            .applied_index()
            .map_err(|error| self.latch(NodeError::Storage(error)))
    }

    pub fn applied_hash(&self) -> Result<LogHash, NodeError> {
        self.ensure_ready()?;
        self.lock_materializer()?
            .applied_hash()
            .map_err(|error| self.latch(NodeError::Storage(error)))
    }

    pub fn cancel_operations(&self) {
        self.operation_cancelled.store(true, Ordering::Release);
        #[cfg(feature = "sql")]
        self.sql_group_commit.fail_pending(NodeError::Unavailable(
            "SQL group commit cancelled during shutdown".into(),
        ));
        #[cfg(feature = "kv")]
        self.kv_group_commit.fail_pending(NodeError::Unavailable(
            "KV group commit cancelled during shutdown".into(),
        ));
        self.read_barriers.cancel_waiters();
        self.operation_cancelled_notify.notify_waiters();
    }

    pub fn materialize_next_decision(&self) -> Result<bool, NodeError> {
        let _commit = self.lock_commit()?;
        self.ensure_ready()?;
        let (last_index, last_hash) = self.ensure_materialized_tip()?;
        let slot = last_index
            .checked_add(1)
            .ok_or_else(|| self.latch(NodeError::Invariant("qlog index is exhausted".into())))?;
        match self
            .consensus
            .inspect_decision_at(&self.consensus_context(), slot, last_hash)
            .map_err(|error| self.map_consensus_error(error))?
        {
            DecisionInspection::Committed(entry) => {
                self.persist_entry(&entry, slot, last_hash)?;
                Ok(true)
            }
            DecisionInspection::Empty | DecisionInspection::Pending => Ok(false),
            DecisionInspection::Unavailable => Err(NodeError::Unavailable(
                "decision proof inspection did not reach quorum".into(),
            )),
        }
    }

    pub async fn run_background_materializer<F>(
        self: Arc<Self>,
        poll_interval: Duration,
        shutdown: F,
    ) -> Result<(), NodeError>
    where
        F: std::future::Future<Output = ()> + Send,
    {
        let poll_interval = poll_interval.max(Duration::from_millis(10));
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => return Ok(()),
                () = tokio::time::sleep(poll_interval) => {
                    loop {
                        let runtime = Arc::clone(&self);
                        let mut operation = tokio::task::spawn_blocking(move || runtime.materialize_next_decision());
                        let (result, shutting_down) = tokio::select! {
                            () = &mut shutdown => {
                                self.cancel_operations();
                                (operation.await, true)
                            }
                            result = &mut operation => (result, false),
                        };
                        if shutting_down {
                            return match result {
                                Ok(Ok(_) | Err(NodeError::Unavailable(_) | NodeError::Contention(_))) => Ok(()),
                                Ok(Err(error)) => Err(error),
                                Err(error) => Err(NodeError::Fatal(format!("materializer task failed: {error}"))),
                            };
                        }
                        match result {
                            Ok(Ok(true)) => continue,
                            Ok(Ok(false) | Err(NodeError::Unavailable(_) | NodeError::Contention(_))) => break,
                            Ok(Err(error)) => return Err(error),
                            Err(error) => return Err(NodeError::Fatal(format!("materializer task failed: {error}"))),
                        }
                    }
                }
            }
        }
    }

    pub const fn config(&self) -> &NodeConfig {
        &self.config
    }

    pub const fn consensus(&self) -> &Arc<ThreeNodeConsensus> {
        &self.consensus
    }

    pub const fn log_store(&self) -> &FileLogStore {
        &self.log_store
    }

    pub fn configuration_state(&self) -> Result<ConfigurationState, NodeError> {
        self.log_store
            .configuration_state()
            .map_err(|error| NodeError::Storage(error.to_string()))
    }

    pub fn status(&self) -> Result<NodeStatus, NodeError> {
        let configuration_state = self.configuration_state()?;
        let (configuration_status, active_config_id) = if configuration_state.is_active() {
            (
                RuntimeConfigurationStatus::Active,
                configuration_state.config_id(),
            )
        } else if configuration_state.config_id() == self.consensus.config_id() {
            (
                RuntimeConfigurationStatus::Stopped,
                configuration_state.config_id(),
            )
        } else {
            (
                RuntimeConfigurationStatus::AwaitingActivation,
                configuration_state
                    .config_id()
                    .checked_add(1)
                    .ok_or_else(|| {
                        NodeError::Invariant("successor configuration id is exhausted".into())
                    })?,
            )
        };
        Ok(NodeStatus {
            ready: self.is_ready(),
            stop_anchor: configuration_state.stop().copied(),
            active_config_id,
            active_membership_digest: self.config.membership.digest(),
            configuration_status,
            configuration_state,
        })
    }

    pub fn stop_current_configuration_for_successor(
        &self,
        successor: &Membership,
    ) -> Result<StopInformation, NodeError> {
        let _commit = self.lock_commit()?;
        self.stop_current_configuration_locked(successor)
    }

    fn stop_current_configuration_locked(
        &self,
        successor: &Membership,
    ) -> Result<StopInformation, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        let state = self.configuration_state()?;
        let stop_command = ConfigChange::bound_stop(
            self.config.cluster_id.clone(),
            state.config_id(),
            state.digest(),
            state
                .config_id()
                .checked_add(1)
                .ok_or_else(|| NodeError::Invariant("successor config id is exhausted".into()))?,
            successor.members().to_vec(),
        )
        .map_err(|error| NodeError::Invariant(error.to_string()))?
        .to_stored_command();
        loop {
            self.ensure_writes_active()?;
            let (last_index, last_hash) = self.ensure_materialized_tip()?;
            let slot = last_index
                .checked_add(1)
                .ok_or_else(|| NodeError::Invariant("qlog index is exhausted".into()))?;
            let entry = self
                .consensus
                .propose_stored_at(
                    self.consensus_context(),
                    slot,
                    last_hash,
                    stop_command.clone(),
                )
                .map_err(|error| self.map_consensus_error(error))?;
            self.persist_entry(&entry, slot, last_hash)?;
            let decided = StoredCommand::new(entry.entry_type, entry.payload.clone());
            if decided != stop_command {
                let current = self.configuration_state()?;
                if current.is_active() {
                    continue;
                }
                return Err(NodeError::ConfigurationTransition {
                    state: Box::new(current),
                });
            }
            let proof = self
                .consensus
                .inspect_decision_proof_at(&self.consensus_context(), slot)
                .map_err(|error| self.map_consensus_error(error))?
                .ok_or_else(|| {
                    NodeError::Unavailable("durable Stop proof is unavailable".into())
                })?;
            if proof
                .proposal()
                .value
                .as_ref()
                .map(|value| value.entry_hash)
                != Some(entry.hash)
            {
                return Err(self.latch(NodeError::Reconciliation(
                    "Stop proof differs from committed stop entry".into(),
                )));
            }
            return Ok(StopInformation { entry, proof });
        }
    }

    pub fn activate_successor(&self) -> Result<LogEntry, NodeError> {
        let _commit = self.lock_commit()?;
        self.activate_successor_locked(None)
    }

    pub fn activate_successor_if(&self, expected_config_id: u64) -> Result<LogEntry, NodeError> {
        let _commit = self.lock_commit()?;
        self.activate_successor_locked(Some(expected_config_id))
    }

    fn activate_successor_locked(
        &self,
        expected_config_id: Option<u64>,
    ) -> Result<LogEntry, NodeError> {
        self.ensure_ready()?;
        let state = self.configuration_state()?;
        let stop = state
            .stop()
            .copied()
            .ok_or_else(|| NodeError::ConfigurationTransition {
                state: Box::new(state.clone()),
            })?;
        if state.config_id() == self.consensus.config_id() {
            return Err(NodeError::ConfigurationTransition {
                state: Box::new(state),
            });
        }
        let successor_config_id = state.config_id().checked_add(1).ok_or_else(|| {
            NodeError::Invariant("successor configuration id is exhausted".into())
        })?;
        if expected_config_id.is_some_and(|expected| expected != successor_config_id) {
            return Err(NodeError::PreconditionFailed(
                "successor configuration does not match expected_config_id".into(),
            ));
        }
        let stop_entry = self.recover_stop_entry(stop)?;
        let entry = self
            .consensus
            .propose_activation_for_stop_entry(self.consensus_context(), &stop_entry)
            .map_err(|error| self.map_consensus_error(error))?;
        self.persist_entry(&entry, stop.index() + 1, stop.hash())?;
        Ok(entry)
    }

    pub(crate) fn recover_stop_entry(&self, stop: LogAnchor) -> Result<LogEntry, NodeError> {
        if let Some(entry) = self
            .log_store
            .read(stop.index())
            .map_err(|error| NodeError::Storage(error.to_string()))?
            .filter(|entry| entry.hash == stop.hash())
        {
            return Ok(entry);
        }
        if let Some(entry) = self
            .config
            .predecessor_stop_entry
            .as_ref()
            .filter(|entry| entry.index == stop.index() && entry.hash == stop.hash())
        {
            validate_entry_envelope(&self.config, entry, entry.index, entry.prev_hash)?;
            return Ok(entry.clone());
        }
        let proof = self
            .consensus
            .inspect_decision_proof_at(&self.consensus_context(), stop.index())
            .map_err(|error| self.map_consensus_error(error))?
            .ok_or_else(|| NodeError::Unavailable("durable Stop proof is unavailable".into()))?;
        let value = proof
            .proposal()
            .value
            .as_ref()
            .filter(|value| value.entry_hash == stop.hash())
            .ok_or_else(|| {
                self.latch(NodeError::Reconciliation(
                    "Stop proof differs from compacted anchor".into(),
                ))
            })?;
        match self
            .consensus
            .inspect_decision_at(&self.consensus_context(), stop.index(), value.prev_hash)
            .map_err(|error| self.map_consensus_error(error))?
        {
            DecisionInspection::Committed(entry) if entry.hash == stop.hash() => Ok(entry),
            DecisionInspection::Unavailable => Err(NodeError::Unavailable(
                "durable Stop command is unavailable".into(),
            )),
            DecisionInspection::Committed(_)
            | DecisionInspection::Empty
            | DecisionInspection::Pending => Err(self.latch(NodeError::Reconciliation(
                "Stop decision differs from compacted anchor".into(),
            ))),
        }
    }

    /// Returns the exact Stop entry only when this process already holds it in
    /// its local qlog or immutable startup configuration.  Status observation
    /// must never consult recorders or latch runtime state merely to describe
    /// a compacted predecessor transition.
    pub(crate) fn observe_stop_entry_locally(
        &self,
        stop: LogAnchor,
    ) -> Result<LogEntry, NodeError> {
        if let Some(entry) = self
            .log_store
            .read(stop.index())
            .map_err(|error| NodeError::Storage(error.to_string()))?
            .filter(|entry| entry.hash == stop.hash())
        {
            return Ok(entry);
        }
        if let Some(entry) = self
            .config
            .predecessor_stop_entry
            .as_ref()
            .filter(|entry| entry.index == stop.index() && entry.hash == stop.hash())
        {
            validate_entry_envelope(&self.config, entry, entry.index, entry.prev_hash)?;
            return Ok(entry.clone());
        }
        Err(NodeError::Unavailable(
            "locally retained Stop command is unavailable".into(),
        ))
    }

    pub fn log_root(&self) -> Result<LogAnchor, NodeError> {
        let _commit = self.lock_commit()?;
        self.log_root_unlocked()
    }

    fn log_root_unlocked(&self) -> Result<LogAnchor, NodeError> {
        let state = self
            .log_store
            .logical_state()
            .map_err(|error| NodeError::Storage(error.to_string()))?;
        Ok(state.tip.map_or_else(
            || {
                state
                    .anchor
                    .map_or(LogAnchor::new(0, LogHash::ZERO), |anchor| {
                        *anchor.compacted()
                    })
            },
            |entry| LogAnchor::new(entry.index(), entry.hash()),
        ))
    }

    pub fn fetch_log(&self, request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError> {
        fetch_runtime_log(self, request)
    }

    #[cfg(feature = "sql")]
    pub fn create_recovery_snapshot(&self) -> Result<RecoverySnapshot, NodeError> {
        let _commit = self.lock_commit()?;
        self.ensure_ready()?;
        self.ensure_materialized_tip()?;
        self.lock_sqlite()?
            .create_recovery_snapshot(self.config.recovery_generation)
            .map_err(|error| self.map_sqlite_error(error))
    }

    pub async fn checkpoint_compact(
        &self,
        coordinator: &CheckpointCoordinator,
    ) -> Result<RecoveryAnchor, DurabilityError> {
        coordinator.checkpoint_compact(self).await
    }

    #[cfg_attr(not(any(feature = "sql", feature = "kv")), allow(unused_variables))]
    pub(crate) fn compact_embedded_log_before(
        &self,
        anchor_index: LogIndex,
    ) -> Result<(), NodeError> {
        let materializer = self.lock_materializer()?;
        match &*materializer {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Materializer::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Materializer::Sql(sql) => sql
                .compact_embedded_log_before(anchor_index)
                .map_err(|error| self.map_sqlite_error(error)),
            #[cfg(feature = "kv")]
            Materializer::Kv(kv) => kv
                .compact_embedded_log_before(anchor_index)
                .map_err(|error| NodeError::Storage(error.to_string())),
            #[cfg(feature = "graph")]
            Materializer::Graph(_) => Ok(()),
        }
    }

    #[cfg(feature = "sql")]
    pub fn verify_snapshot_publication(
        &self,
        snapshot: &RecoverySnapshot,
        publication: &SnapshotRecord,
    ) -> Result<VerifiedSnapshotPublication, NodeError> {
        let anchor = snapshot.anchor();
        let manifest = publication.manifest();
        let publication_digest = LogHash::from_hex(publication.sha256()).ok_or_else(|| {
            NodeError::Reconciliation("published snapshot digest is invalid".into())
        })?;
        if anchor.cluster_id() != self.config.cluster_id
            || anchor.epoch() != self.config.epoch
            || anchor.config_id() != self.config.config_id()
            || anchor.recovery_generation() != self.config.recovery_generation
            || manifest.cluster_id() != anchor.cluster_id()
            || manifest.epoch() != anchor.epoch()
            || manifest.config_id() != anchor.config_id()
            || manifest.configuration_state() != anchor.configuration_state()
            || manifest.index() != anchor.compacted().index()
            || manifest.applied_hash() != anchor.compacted().hash()
            || manifest.snapshot_id() != anchor.snapshot().snapshot_id()
            || manifest.executor_fingerprint() != anchor.snapshot().executor_fingerprint()
            || publication_digest != anchor.snapshot().digest()
            || publication.size_bytes() != anchor.snapshot().size_bytes()
            || LogHash::digest(&[snapshot.db_bytes()]) != anchor.snapshot().digest()
            || snapshot.db_bytes().len() as u64 != anchor.snapshot().size_bytes()
        {
            return Err(NodeError::Reconciliation(
                "published snapshot does not match the runtime recovery anchor".into(),
            ));
        }
        Ok(VerifiedSnapshotPublication {
            anchor: anchor.clone(),
        })
    }

    #[cfg(feature = "sql")]
    pub fn compact_log(&self, publication: &VerifiedSnapshotPublication) -> Result<(), NodeError> {
        let _commit = self.lock_commit()?;
        self.ensure_ready()?;
        let applied_index = self.applied_index()?;
        let applied_hash = self.applied_hash()?;
        let anchor = &publication.anchor;
        if anchor.cluster_id() != self.config.cluster_id
            || anchor.epoch() != self.config.epoch
            || anchor.config_id() != self.config.config_id()
            || anchor.recovery_generation() != self.config.recovery_generation
            || anchor.compacted().index() != applied_index
            || anchor.compacted().hash() != applied_hash
        {
            return Err(NodeError::Reconciliation(
                "verified snapshot anchor does not match the current applied entry".into(),
            ));
        }
        self.log_store
            .compact_prefix(anchor)
            .map_err(|error| NodeError::Storage(error.to_string()))?;
        self.compact_embedded_log_before(anchor.compacted().index())
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire) && !self.fatal.load(Ordering::Acquire)
    }

    pub fn is_fatal(&self) -> bool {
        self.fatal.load(Ordering::Acquire)
    }

    pub fn fatal_reason(&self) -> Option<String> {
        self.fatal_reason
            .lock()
            .ok()
            .and_then(|reason| reason.clone())
    }

    #[cfg(feature = "sql")]
    fn read_local(
        &self,
        key: &str,
        required_index: Option<LogIndex>,
    ) -> Result<ReadResponse, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        let sqlite = self.lock_sqlite()?;
        let (applied_index, hash) = sqlite
            .applied_tip_value()
            .map_err(|error| self.map_sqlite_error(error))?;
        if required_index.is_some_and(|required| applied_index < required) {
            return Err(NodeError::Unavailable(format!(
                "local applied index {applied_index} has not reached {}",
                required_index.expect("checked above")
            )));
        }
        let value = sqlite
            .get_value(key)
            .map_err(|error| self.map_sqlite_error(error))?;
        Ok(ReadResponse {
            value,
            applied_index,
            hash,
        })
    }

    #[cfg(feature = "graph")]
    fn get_graph_document_local(
        &self,
        id: &str,
        required_index: Option<LogIndex>,
    ) -> Result<GraphReadResponse, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        let graph = self.graph_materializer()?;
        let (value, applied_index, hash) = graph
            .get_document_with_tip(id)
            .map_err(|error| self.map_graph_read_error(error))?;
        if required_index.is_some_and(|required| applied_index < required) {
            return Err(NodeError::Unavailable(format!(
                "local applied index {applied_index} has not reached {}",
                required_index.expect("checked above")
            )));
        }
        Ok(GraphReadResponse {
            value,
            applied_index,
            hash,
        })
    }

    #[cfg(feature = "graph")]
    fn query_graph_local(
        &self,
        statement: &str,
        parameters: &BTreeMap<String, GraphParameterValue>,
        required_index: Option<LogIndex>,
        max_rows: u32,
    ) -> Result<GraphQueryResult, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        let graph = self.graph_materializer()?;
        let result = graph
            .query_read_only(
                statement,
                parameters,
                usize::try_from(max_rows).expect("u32 fits usize"),
                MAX_GRAPH_RESULT_BYTES,
                GRAPH_QUERY_TIMEOUT_MS,
            )
            .map_err(|error| self.map_graph_read_error(error))?;
        if required_index.is_some_and(|required| result.applied_index < required) {
            return Err(NodeError::Unavailable(format!(
                "local applied index {} has not reached {}",
                result.applied_index,
                required_index.expect("checked above")
            )));
        }
        Ok(result)
    }

    #[cfg(feature = "kv")]
    fn get_kv_local(
        &self,
        key: &[u8],
        required_index: Option<LogIndex>,
    ) -> Result<KvReadResponse, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        let kv = self.kv_materializer()?;
        let result = kv
            .get_with_tip(key)
            .map_err(|error| self.map_kv_read_error(error))?;
        let (value, tip) = result.into_parts();
        let applied_index = tip.applied_index();
        if required_index.is_some_and(|required| applied_index < required) {
            return Err(NodeError::Unavailable(format!(
                "local applied index {applied_index} has not reached {}",
                required_index.expect("checked above")
            )));
        }
        Ok(KvReadResponse {
            value,
            applied_index,
            hash: tip.applied_hash(),
        })
    }

    #[cfg(feature = "kv")]
    fn scan_kv_range_local(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        cursor: Option<&[u8]>,
        required_index: Option<LogIndex>,
    ) -> Result<KvScanResult, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        let kv = self.kv_materializer()?;
        let result = kv
            .scan_range(start, end, limit, cursor)
            .map_err(|error| self.map_kv_read_error(error))?;
        validate_kv_scan_required_index(&result, required_index)?;
        Ok(result)
    }

    #[cfg(feature = "kv")]
    fn scan_kv_prefix_local(
        &self,
        prefix: &[u8],
        limit: usize,
        cursor: Option<&[u8]>,
        required_index: Option<LogIndex>,
    ) -> Result<KvScanResult, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        let kv = self.kv_materializer()?;
        let result = kv
            .scan_prefix(prefix, limit, cursor)
            .map_err(|error| self.map_kv_read_error(error))?;
        validate_kv_scan_required_index(&result, required_index)?;
        Ok(result)
    }

    #[cfg(feature = "sql")]
    fn query_sql_local(
        &self,
        statement: &SqlStatement,
        required_index: Option<LogIndex>,
        max_rows: u32,
    ) -> Result<SqlQueryResponse, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        let sqlite = self.sql_read_handle()?;
        let (applied_index, hash, query_result) = sqlite
            .query_sql_with_applied_tip(
                statement,
                usize::try_from(max_rows).expect("u32 fits usize"),
                MAX_SQL_RESULT_BYTES,
            )
            .map_err(|error| match error {
                rhiza_sql::Error::ResourceExhausted(message) => {
                    NodeError::ResourceExhausted(message)
                }
                other => NodeError::InvalidSqlStatement {
                    statement_index: 0,
                    message: other.to_string(),
                },
            })?;
        if required_index.is_some_and(|required| applied_index < required) {
            return Err(NodeError::Unavailable(format!(
                "local applied index {applied_index} has not reached {}",
                required_index.expect("checked above")
            )));
        }
        let SqlQueryResult { columns, rows } = query_result;
        Ok(SqlQueryResponse {
            columns,
            rows,
            applied_index,
            hash,
        })
    }

    fn establish_read_barrier(&self) -> Result<LogAnchor, NodeError> {
        // Check cache first — avoids quorum RPC for repeated reads within TTL.
        if let Ok(cache) = self.cached_barrier.lock() {
            if let Some(cached) = cache.as_ref() {
                if cached.cached_at.elapsed() < READ_BARRIER_CACHE_TTL {
                    return Ok(cached.anchor);
                }
            }
        }

        let participant = self.read_barriers.join().map_err(|error| match error {
            NodeError::Invariant(_) => self.latch(error),
            other => other,
        })?;
        let Some(mut publication) = participant.publication() else {
            return participant.wait(&self.operation_cancelled);
        };

        let result = (|| {
            publication.wait_turn(&self.operation_cancelled)?;
            // The public read path must not own this mutex before entering the
            // coalescer. The generation cutoff happens only after the round
            // leader owns commit, immediately before consensus begins.
            let _commit = self.lock_commit()?;
            publication.start(&self.operation_cancelled)?;
            self.ensure_ready()?;
            self.commit_read_barrier_locked()
        })();
        publication.publish(result.clone());

        // Cache the result for subsequent reads.
        if let Ok(anchor) = &result {
            if let Ok(mut cache) = self.cached_barrier.lock() {
                *cache = Some(CachedBarrier {
                    anchor: *anchor,
                    cached_at: Instant::now(),
                });
            }
        }

        result
    }

    #[cfg(any(feature = "graph", feature = "kv"))]
    fn validate_read_barrier_before_snapshot(&self, anchor: LogAnchor) -> Result<(), NodeError> {
        {
            let _commit = self.lock_commit()?;
            self.ensure_ready()?;
            self.ensure_writes_active()?;
            self.validate_read_barrier_qlog_descendant_locked(anchor)?;
        }
        #[cfg(test)]
        if let Some(hook) = &self.read_barrier_before_snapshot_hook {
            hook();
        }
        Ok(())
    }

    #[cfg(any(feature = "graph", feature = "kv"))]
    fn validate_read_barrier_snapshot(
        &self,
        anchor: LogAnchor,
        observed: LogAnchor,
    ) -> Result<(), NodeError> {
        if observed.index() < anchor.index() {
            return Err(NodeError::Unavailable(format!(
                "read snapshot tip {} precedes read barrier {}",
                observed.index(),
                anchor.index()
            )));
        }
        if observed.index() == anchor.index() && observed.hash() != anchor.hash() {
            return Err(self.latch(NodeError::Invariant(
                "read snapshot tip hash differs from the read barrier anchor".into(),
            )));
        }
        Ok(())
    }

    #[cfg(feature = "sql")]
    fn validate_read_barrier_descendant_locked(&self, anchor: LogAnchor) -> Result<(), NodeError> {
        let (applied_index, applied_hash) = self.ensure_materialized_tip()?;
        self.validate_read_barrier_descendant_from_tip(
            anchor,
            LogAnchor::new(applied_index, applied_hash),
            "materialized",
        )
    }

    #[cfg(any(feature = "graph", feature = "kv"))]
    fn validate_read_barrier_qlog_descendant_locked(
        &self,
        anchor: LogAnchor,
    ) -> Result<(), NodeError> {
        let (qlog_index, qlog_hash) = self.durable_tip()?;
        self.validate_read_barrier_descendant_from_tip(
            anchor,
            LogAnchor::new(qlog_index, qlog_hash),
            "qlog",
        )
    }

    fn validate_read_barrier_descendant_from_tip(
        &self,
        anchor: LogAnchor,
        tip: LogAnchor,
        tip_kind: &str,
    ) -> Result<(), NodeError> {
        if tip.index() < anchor.index() {
            return Err(self.latch(NodeError::Invariant(format!(
                "{tip_kind} tip {} precedes read barrier {}",
                tip.index(),
                anchor.index()
            ))));
        }
        if tip.index() == anchor.index() {
            if tip.hash() != anchor.hash() {
                return Err(self.latch(NodeError::Invariant(format!(
                    "{tip_kind} tip hash differs from the read barrier anchor"
                ))));
            }
            return Ok(());
        }
        if anchor.index() == 0 {
            if anchor.hash() == LogHash::ZERO {
                return Ok(());
            }
            return Err(self.latch(NodeError::Invariant(
                "genesis read barrier anchor has a non-zero hash".into(),
            )));
        }

        let logical = self
            .log_store
            .logical_state()
            .map_err(|error| self.latch(NodeError::Storage(error.to_string())))?;
        if let Some(compacted) = logical.anchor.as_ref().map(RecoveryAnchor::compacted) {
            if compacted.index() > anchor.index() {
                return Ok(());
            }
            if compacted.index() == anchor.index() {
                if compacted.hash() == anchor.hash() {
                    return Ok(());
                }
                return Err(self.latch(NodeError::Invariant(
                    "compacted qlog hash differs from the read barrier anchor".into(),
                )));
            }
        }
        let retained = self
            .log_store
            .read(anchor.index())
            .map_err(|error| self.latch(NodeError::Storage(error.to_string())))?
            .ok_or_else(|| {
                self.latch(NodeError::Invariant(
                    "read barrier anchor is neither retained nor compacted".into(),
                ))
            })?;
        if retained.hash != anchor.hash() {
            return Err(self.latch(NodeError::Invariant(
                "retained qlog hash differs from the read barrier anchor".into(),
            )));
        }
        Ok(())
    }

    fn commit_read_barrier_locked(&self) -> Result<LogAnchor, NodeError> {
        self.ensure_writes_active()?;
        let context_read_fence = self.consensus.supports_context_read_fence();
        loop {
            self.ensure_ready()?;
            let (last_index, last_hash) = self.ensure_materialized_tip()?;
            let slot = last_index.checked_add(1).ok_or_else(|| {
                self.latch(NodeError::Invariant("qlog index is exhausted".into()))
            })?;
            let inspection = if context_read_fence {
                match self
                    .consensus
                    .inspect_context_read_fence_at(&self.consensus_context(), slot, last_hash)
                    .map_err(|error| self.map_consensus_error(error))?
                {
                    CertifiedDecisionInspection::Committed(certified) => {
                        DecisionInspection::Committed(certified.entry)
                    }
                    CertifiedDecisionInspection::Empty => DecisionInspection::Empty,
                    CertifiedDecisionInspection::Pending => DecisionInspection::Pending,
                    CertifiedDecisionInspection::Unavailable => DecisionInspection::Unavailable,
                }
            } else {
                self.consensus
                    .inspect_decision_at(&self.consensus_context(), slot, last_hash)
                    .map_err(|error| self.map_consensus_error(error))?
            };
            match inspection {
                DecisionInspection::Committed(entry) => {
                    self.persist_entry(&entry, slot, last_hash)?;
                }
                DecisionInspection::Empty => {
                    // Quorum confirmed the slot is empty. This is a valid
                    // linearization point — no committed decision exists.
                    // Return anchor immediately without writing a Noop.
                    self.ensure_writes_active()?;
                    return Ok(LogAnchor::new(last_index, last_hash));
                }
                DecisionInspection::Pending => {
                    // Slot is pending — re-check to catch any decision that
                    // was committed between the first check and now.
                    // Mirror the first inspection: use context_read_fence if
                    // available to avoid missing decisions certified via that
                    // path.
                    let re_inspection = if context_read_fence {
                        match self
                            .consensus
                            .inspect_context_read_fence_at(
                                &self.consensus_context(),
                                slot,
                                last_hash,
                            )
                            .map_err(|error| self.map_consensus_error(error))?
                        {
                            CertifiedDecisionInspection::Committed(certified) => {
                                DecisionInspection::Committed(certified.entry)
                            }
                            CertifiedDecisionInspection::Empty => DecisionInspection::Empty,
                            CertifiedDecisionInspection::Pending => DecisionInspection::Pending,
                            CertifiedDecisionInspection::Unavailable => {
                                DecisionInspection::Unavailable
                            }
                        }
                    } else {
                        self.consensus
                            .inspect_decision_at(&self.consensus_context(), slot, last_hash)
                            .map_err(|error| self.map_consensus_error(error))?
                    };
                    match re_inspection {
                        DecisionInspection::Committed(entry) => {
                            self.persist_entry(&entry, slot, last_hash)?;
                        }
                        DecisionInspection::Empty => {
                            self.ensure_writes_active()?;
                            return Ok(LogAnchor::new(last_index, last_hash));
                        }
                        DecisionInspection::Pending => {
                            // Still pending — may conceal a historical phase-2
                            // Noop whose durable proof quorum was never
                            // installed. Write a Noop to recover.
                            let entry = self
                                .consensus
                                .propose_at(
                                    self.consensus_context(),
                                    slot,
                                    last_hash,
                                    Command::new(CommandKind::ReadBarrier, Vec::new()),
                                )
                                .map_err(|error| self.map_consensus_error(error))?;
                            self.persist_entry(&entry, slot, last_hash)?;
                        }
                        DecisionInspection::Unavailable => {
                            return Err(NodeError::Unavailable(
                                "decision inspection did not reach quorum".into(),
                            ));
                        }
                    }
                }
                DecisionInspection::Unavailable => {
                    return Err(NodeError::Unavailable(
                        "decision inspection did not reach quorum".into(),
                    ));
                }
            }
        }
    }

    #[cfg(feature = "sql")]
    fn check_request(
        &self,
        request_id: &str,
        payload: &[u8],
    ) -> Result<Option<RequestOutcome>, NodeError> {
        let sqlite = self.lock_sqlite()?;
        sqlite
            .check_request(request_id, payload)
            .map_err(|error| self.map_sqlite_error(error))
    }

    #[cfg(feature = "graph")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "kv")),
        allow(irrefutable_let_patterns)
    )]
    fn check_graph_request(
        &self,
        request_id: &str,
        payload: &[u8],
    ) -> Result<Option<GraphRequestRecord>, NodeError> {
        let materializer = self.lock_materializer()?;
        let Materializer::Graph(graph) = &*materializer else {
            return Err(NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Graph,
                actual: materializer.profile(),
            });
        };
        graph
            .check_request(request_id, payload)
            .map_err(|error| NodeError::InvalidRequest(error.to_string()))
    }

    #[cfg(feature = "kv")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "graph")),
        allow(irrefutable_let_patterns)
    )]
    fn check_kv_request(
        &self,
        request_id: &str,
        payload: &[u8],
    ) -> Result<Option<KvRequestRecord>, NodeError> {
        let materializer = self.lock_materializer()?;
        let Materializer::Kv(kv) = &*materializer else {
            return Err(NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Kv,
                actual: materializer.profile(),
            });
        };
        kv.check_request(request_id, payload)
            .map_err(|error| NodeError::InvalidRequest(error.to_string()))
    }

    fn ensure_materialized_tip(&self) -> Result<(LogIndex, LogHash), NodeError> {
        #[cfg(test)]
        self.materialized_tip_checks.fetch_add(1, Ordering::Relaxed);
        let (last_index, last_hash) = self.durable_tip()?;
        let materializer = self.lock_materializer()?;
        let applied_tip = materializer
            .applied_tip()
            .map_err(|error| self.latch(NodeError::Storage(error)))?;
        let applied_index = applied_tip.index();
        let applied_hash = applied_tip.hash();
        if (applied_index, applied_hash) != (last_index, last_hash) {
            return Err(self.latch(NodeError::Invariant(format!(
                "qlog tip {last_index}/{} differs from {} materializer tip {applied_index}/{}",
                last_hash.to_hex(),
                materializer.profile(),
                applied_hash.to_hex()
            ))));
        }
        Ok((last_index, last_hash))
    }

    fn durable_tip(&self) -> Result<(LogIndex, LogHash), NodeError> {
        static_log_tip(&self.log_store).map_err(|error| self.latch(error))
    }

    fn persist_entry(
        &self,
        entry: &LogEntry,
        expected_index: LogIndex,
        expected_prev_hash: LogHash,
    ) -> Result<Option<SqlCommandResult>, NodeError> {
        #[cfg(feature = "sql")]
        if self.config.execution_profile == ExecutionProfile::Sqlite
            && entry.entry_type == EntryType::Command
            && !entry.payload.is_empty()
        {
            return self.persist_sql_entry_profiled(
                entry,
                expected_index,
                expected_prev_hash,
                &mut DisabledSqlWritePhaseProfile,
            );
        }
        let configuration_state = self.configuration_state()?;
        validate_runtime_entry(
            &self.config,
            &configuration_state,
            entry,
            expected_index,
            expected_prev_hash,
        )
        .map_err(|error| self.latch(error))?;
        if matches!(
            self.config.execution_profile,
            ExecutionProfile::Sqlite | ExecutionProfile::Kv
        ) {
            // SQL/KV embed the complete entry in their durable materializer state. The file qlog
            // remains a buffered serving mirror and is rehydrated on startup.
            self.log_store
                .append_batch_buffered(std::slice::from_ref(entry))
                .map_err(|error| self.latch(NodeError::Storage(error.to_string())))?;
        } else {
            self.log_store
                .append(entry)
                .map_err(|error| self.latch(NodeError::Storage(error.to_string())))?;
        }
        self.lock_materializer()?
            .apply_entry(entry)
            .map_err(|error| self.latch(NodeError::Invariant(error)))?;

        // Invalidate cached barrier — a new entry was committed.
        if let Ok(mut cache) = self.cached_barrier.lock() {
            *cache = None;
        }

        Ok(None)
    }

    #[cfg(feature = "sql")]
    fn persist_sql_entry_profiled<P: SqlWritePhaseProfile>(
        &self,
        entry: &LogEntry,
        expected_index: LogIndex,
        expected_prev_hash: LogHash,
        profile: &mut P,
    ) -> Result<Option<SqlCommandResult>, NodeError> {
        if entry.entry_type != EntryType::Command || entry.payload.is_empty() {
            return self.persist_entry(entry, expected_index, expected_prev_hash);
        }
        let configuration_state = self.configuration_state()?;
        validate_runtime_entry(
            &self.config,
            &configuration_state,
            entry,
            expected_index,
            expected_prev_hash,
        )
        .map_err(|error| self.latch(error))?;

        let verified = self.resolve_external_sql_effect(entry)?;

        let qlog_mark = profile.mark();
        let append_result = self
            .log_store
            .append_batch_buffered(std::slice::from_ref(entry))
            .map_err(|error| self.latch(NodeError::Storage(error.to_string())));
        profile.add_local_qlog_mirror_append(qlog_mark);
        append_result?;

        let materializer_mark = profile.mark();
        let apply_result = self
            .lock_materializer()?
            .apply_verified_external_sql_effect(&verified)
            .map_err(|error| self.latch(NodeError::Invariant(error)));
        profile.add_sql_materializer_apply(materializer_mark);

        // Invalidate cached barrier — a new entry was committed.
        if apply_result.is_ok() {
            if let Ok(mut cache) = self.cached_barrier.lock() {
                *cache = None;
            }
        }

        apply_result
    }

    #[cfg(feature = "sql")]
    fn resolve_external_sql_effect(
        &self,
        entry: &LogEntry,
    ) -> Result<VerifiedQwalEffectBundleV4, NodeError> {
        let bundle = self.resolve_external_sql_bundle_for_archive(entry)?;
        let command = ExternalEffectCommand::decode(&entry.payload).map_err(|error| {
            self.latch(NodeError::Invariant(format!(
                "invalid committed QEFX command: {error}"
            )))
        })?;
        verify_resolved_external_sql_bundle(&command, &bundle)
            .map_err(|error| self.latch(NodeError::Invariant(error.to_string())))
    }

    /// Resolves and fully verifies the Recorder-held bytes for a committed
    /// QEFX entry before exposing their immutable chunks to archive code.
    /// Archive publishers must retain the command and re-verify its digest at
    /// their own persistence boundary.
    #[cfg(feature = "sql")]
    pub(crate) fn resolve_external_sql_bundle_for_archive(
        &self,
        entry: &LogEntry,
    ) -> Result<RecorderEffectBundle, NodeError> {
        self.prepare_external_sql_bundle_resolution(entry)?
            .resolve()
            .map_err(|error| self.map_consensus_error(error))?
            .ok_or_else(|| {
                NodeError::Unavailable("committed QEFX bundle is temporarily unavailable".into())
            })
    }

    #[cfg(feature = "sql")]
    fn prepare_external_sql_bundle_resolution(
        &self,
        entry: &LogEntry,
    ) -> Result<PreparedExternalSqlBundleResolution, NodeError> {
        let configuration = self.configuration_state()?;
        let command = ExternalEffectCommand::decode(&entry.payload).map_err(|error| {
            self.latch(NodeError::Invariant(format!(
                "invalid committed QEFX command: {error}"
            )))
        })?;
        validate_external_sql_entry(entry, &configuration)
            .map_err(|error| self.latch(NodeError::Invariant(error)))?;
        let manifest_command = StoredCommand::new(EntryType::Command, entry.payload.clone());
        let binding = effect_bundle_binding(&command, &manifest_command);
        Ok(PreparedExternalSqlBundleResolution {
            consensus: self.consensus.clone(),
            binding,
            manifest_command,
            command,
        })
    }

    /// Resolves archive payloads on Tokio's blocking pool. Recorder quorum
    /// reads are synchronous and may consume their full bounded deadline; they
    /// must never starve HTTP liveness or the durability executor.
    #[cfg(feature = "sql")]
    pub(crate) async fn resolve_external_sql_bundle_for_archive_async(
        &self,
        entry: &LogEntry,
    ) -> Result<RecorderEffectBundle, NodeError> {
        let prepared = self.prepare_external_sql_bundle_resolution(entry)?;
        let result = tokio::task::spawn_blocking(move || prepared.resolve())
            .await
            .map_err(node_service_task_error)?
            .map_err(|error| self.map_consensus_error(error))?
            .ok_or_else(|| {
                NodeError::Unavailable("committed QEFX bundle is temporarily unavailable".into())
            })?;
        Ok(result)
    }

    fn require_execution_profile(&self, expected: ExecutionProfile) -> Result<(), NodeError> {
        if self.config.execution_profile == expected {
            Ok(())
        } else {
            Err(NodeError::ExecutionProfileMismatch {
                expected,
                actual: self.config.execution_profile,
            })
        }
    }

    fn ensure_ready(&self) -> Result<(), NodeError> {
        if self.fatal.load(Ordering::Acquire) {
            return Err(NodeError::Fatal(
                self.fatal_reason()
                    .unwrap_or_else(|| "fatal state is latched".into()),
            ));
        }
        if !self.ready.load(Ordering::Acquire) {
            return Err(NodeError::Unavailable("runtime is not ready".into()));
        }
        Ok(())
    }

    fn ensure_writes_active(&self) -> Result<(), NodeError> {
        let state = self.configuration_state()?;
        if state.is_active() {
            Ok(())
        } else {
            Err(NodeError::ConfigurationTransition {
                state: Box::new(state),
            })
        }
    }

    fn lock_commit(&self) -> Result<MutexGuard<'_, ()>, NodeError> {
        self.commit
            .lock()
            .map_err(|_| self.latch(NodeError::Invariant("commit mutex is poisoned".into())))
    }

    /// Acquires commit only for a read-only status observation.
    ///
    /// A poisoned observation lock is deliberately reported without changing
    /// readiness or installing a fatal latch: status must fail closed without
    /// turning an observational failure into a runtime transition.
    pub(crate) fn lock_commit_for_status_observation(
        &self,
    ) -> Result<MutexGuard<'_, ()>, NodeError> {
        self.commit
            .lock()
            .map_err(|_| NodeError::Invariant("commit mutex is poisoned".into()))
    }

    fn lock_materializer(&self) -> Result<MutexGuard<'_, Materializer>, NodeError> {
        self.materializer.lock().map_err(|_| {
            self.latch(NodeError::Invariant(
                "materializer mutex is poisoned".into(),
            ))
        })
    }

    #[cfg(feature = "graph")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "kv")),
        allow(irrefutable_let_patterns)
    )]
    fn graph_materializer(&self) -> Result<Arc<LadybugStateMachine>, NodeError> {
        let materializer = self.lock_materializer()?;
        let Materializer::Graph(graph) = &*materializer else {
            return Err(NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Graph,
                actual: materializer.profile(),
            });
        };
        Ok(Arc::clone(graph))
    }

    #[cfg(feature = "kv")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "graph")),
        allow(irrefutable_let_patterns)
    )]
    fn kv_materializer(&self) -> Result<Arc<RedbStateMachine>, NodeError> {
        let materializer = self.lock_materializer()?;
        let Materializer::Kv(kv) = &*materializer else {
            return Err(NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Kv,
                actual: materializer.profile(),
            });
        };
        Ok(Arc::clone(kv))
    }

    #[cfg(feature = "sql")]
    fn lock_sqlite(&self) -> Result<SqlMaterializerGuard<'_>, NodeError> {
        let guard = self.lock_materializer()?;
        if !matches!(&*guard, Materializer::Sql(_)) {
            return Err(NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Sqlite,
                actual: guard.profile(),
            });
        }
        Ok(SqlMaterializerGuard(guard))
    }

    #[cfg(feature = "sql")]
    #[cfg_attr(
        all(not(feature = "graph"), not(feature = "kv")),
        allow(irrefutable_let_patterns)
    )]
    fn sql_read_handle(&self) -> Result<rhiza_sql::SqlReadHandle, NodeError> {
        let materializer = self.lock_materializer()?;
        let Materializer::Sql(sqlite) = &*materializer else {
            return Err(NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Sqlite,
                actual: materializer.profile(),
            });
        };
        Ok(sqlite.read_handle())
    }

    #[cfg(feature = "sql")]
    fn map_sqlite_error(&self, error: rhiza_sql::Error) -> NodeError {
        match error {
            rhiza_sql::Error::RequestConflict(conflict) => NodeError::RequestConflict(conflict),
            rhiza_sql::Error::ResourceExhausted(message) => NodeError::ResourceExhausted(message),
            rhiza_sql::Error::InvalidCommand(message)
                if message == "external SQL batch produced no successful member" =>
            {
                NodeError::InvalidSqlStatement {
                    statement_index: 0,
                    message,
                }
            }
            rhiza_sql::Error::InvalidCommand(message)
            | rhiza_sql::Error::IdentityMismatch(message)
            | rhiza_sql::Error::InvalidEntry(message)
            | rhiza_sql::Error::InvalidSnapshot(message) => {
                self.latch(NodeError::Invariant(message))
            }
            other => self.latch(NodeError::Storage(other.to_string())),
        }
    }

    #[cfg(feature = "graph")]
    fn map_graph_read_error(&self, error: rhiza_graph::Error) -> NodeError {
        match error {
            rhiza_graph::Error::InvalidCommand(_) => NodeError::InvalidRequest(error.to_string()),
            rhiza_graph::Error::ResourceExhausted(message) => NodeError::ResourceExhausted(message),
            rhiza_graph::Error::Ladybug(_) | rhiza_graph::Error::Io(_) => {
                self.latch(NodeError::Storage(error.to_string()))
            }
            rhiza_graph::Error::Closed
            | rhiza_graph::Error::Codec(_)
            | rhiza_graph::Error::InvalidEntry(_)
            | rhiza_graph::Error::IdentityMismatch(_)
            | rhiza_graph::Error::RequestConflict { .. }
            | rhiza_graph::Error::InvalidSnapshot(_) => {
                self.latch(NodeError::Invariant(error.to_string()))
            }
        }
    }

    #[cfg(feature = "kv")]
    fn map_kv_read_error(&self, error: rhiza_kv::Error) -> NodeError {
        match error {
            rhiza_kv::Error::InvalidCommand(_) | rhiza_kv::Error::InvalidQuery(_) => {
                NodeError::InvalidRequest(error.to_string())
            }
            rhiza_kv::Error::ResourceExhausted(message) => NodeError::ResourceExhausted(message),
            rhiza_kv::Error::Database(_) | rhiza_kv::Error::Io(_) => {
                self.latch(NodeError::Storage(error.to_string()))
            }
            rhiza_kv::Error::Codec(_)
            | rhiza_kv::Error::InvalidEntry(_)
            | rhiza_kv::Error::PartialInitialization
            | rhiza_kv::Error::RequestConflict { .. }
            | rhiza_kv::Error::InvalidSnapshot(_) => {
                self.latch(NodeError::Invariant(error.to_string()))
            }
        }
    }

    fn map_consensus_error(&self, error: rhiza_quepaxa::Error) -> NodeError {
        match error {
            rhiza_quepaxa::Error::NoQuorum
            | rhiza_quepaxa::Error::ProposeFailed
            | rhiza_quepaxa::Error::CommandUnavailable
            | rhiza_quepaxa::Error::EffectBundleUnavailable
            | rhiza_quepaxa::Error::Cancelled
            | rhiza_quepaxa::Error::RpcCancelled
            | rhiza_quepaxa::Error::RpcDeadlineExceeded
            | rhiza_quepaxa::Error::Io(_) => NodeError::Unavailable(error.to_string()),
            // UnknownOutcome is the caller's observation of an admitted,
            // idempotent Recorder mutation. The next operation revisits the
            // same consensus slot and reconciles the durable result; a lost
            // reply is not contradictory storage evidence and must not make
            // the whole runtime permanently fatal.
            rhiza_quepaxa::Error::UnknownOutcome => NodeError::OutcomeUnknown(error.to_string()),
            rhiza_quepaxa::Error::EffectBundleConflict
            | rhiza_quepaxa::Error::EffectBundleInvalid(_)
            | rhiza_quepaxa::Error::EffectBundleQuotaExceeded { .. }
            | rhiza_quepaxa::Error::ConflictingCertificates
            | rhiza_quepaxa::Error::ChainConflict { .. } => {
                self.latch(NodeError::Reconciliation(error.to_string()))
            }
            other => self.latch(NodeError::Invariant(other.to_string())),
        }
    }

    fn latch(&self, error: NodeError) -> NodeError {
        self.ready.store(false, Ordering::Release);
        if !self.fatal.swap(true, Ordering::AcqRel) {
            eprintln!(
                "node runtime entered fatal state: {}",
                escaped_error_detail(&error)
            );
            if let Ok(mut reason) = self.fatal_reason.lock() {
                *reason = Some(error.to_string());
            }
        }
        error
    }
}

pub fn rehydrate_recorder_after_checkpoint(
    runtime: &NodeRuntime,
    recorder: &RecorderFileStore,
    checkpoint_index: LogIndex,
    startup: &StartupIoContext,
) -> Result<(), NodeError> {
    startup.check("recorder rehydration anchor validation")?;
    if let Some(anchor) = runtime
        .log_store()
        .logical_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?
        .anchor
    {
        if checkpoint_index < anchor.compacted().index() {
            return Err(NodeError::SnapshotRequired(Box::new(anchor)));
        }
    }
    let applied_index = runtime.applied_index()?;
    if checkpoint_index > applied_index {
        return Err(NodeError::Reconciliation(format!(
            "checkpoint tip {checkpoint_index} is ahead of local applied index {applied_index}"
        )));
    }

    #[cfg(feature = "sql")]
    rehydrate_checkpoint_qefx_handoff(runtime, recorder)?;

    for index in checkpoint_index.saturating_add(1)..=applied_index {
        startup.check("recorder rehydration qlog read")?;
        let entry = runtime
            .log_store()
            .read(index)
            .map_err(|error| NodeError::Storage(error.to_string()))?
            .ok_or_else(|| {
                NodeError::Reconciliation(format!(
                    "qlog entry {index} is missing during recorder rehydration"
                ))
            })?;
        startup.check("recorder rehydration decision inspection")?;
        // The call-local consensus budget/group retains every admitted
        // inspection until its bounded drain completes. Sharing the startup
        // context therefore preserves cancellation certainty without letting
        // rehydration return while a recorder worker can still persist later.
        let inspection_context = startup.recorder_context();
        let certified = match runtime
            .consensus()
            .inspect_certified_decision_at(&inspection_context, index, entry.prev_hash)
            .map_err(startup_consensus_error)?
        {
            CertifiedDecisionInspection::Committed(certified) => certified,
            CertifiedDecisionInspection::Empty => {
                return Err(NodeError::Reconciliation(format!(
                    "qlog entry {index} has no recorder decision certificate"
                )));
            }
            CertifiedDecisionInspection::Pending => {
                return Err(NodeError::Reconciliation(format!(
                    "qlog entry {index} has only a pending recorder decision"
                )));
            }
            CertifiedDecisionInspection::Unavailable => {
                return Err(NodeError::Unavailable(format!(
                    "recorder decision certificate is unavailable at qlog index {index}"
                )));
            }
        };
        startup.check("recorder rehydration decision inspection")?;
        if certified.entry != entry {
            return Err(NodeError::Reconciliation(format!(
                "recorder decision certificate differs from qlog entry {index}"
            )));
        }
        let command = StoredCommand::new(entry.entry_type, entry.payload.clone());
        let proof = certified.proof.clone();
        #[cfg(feature = "sql")]
        if runtime.config.execution_profile == ExecutionProfile::Sqlite
            && entry.entry_type == EntryType::Command
            && !entry.payload.is_empty()
        {
            // Rehydrating only the small command/proof would leave this
            // recorder unable to serve a committed QEFX after restart.
            let bundle = runtime.resolve_external_sql_bundle_for_archive(&entry)?;
            let request =
                EffectBundleFinalizeRequest::new(bundle, command.clone()).map_err(|error| {
                    NodeError::Reconciliation(format!(
                        "cannot reconstruct recorder QEFX bundle at qlog index {index}: {error}"
                    ))
                })?;
            recorder.finalize_effect_bundle(&request).map_err(|error| {
                NodeError::Reconciliation(format!(
                    "cannot install recorder QEFX bundle at qlog index {index}: {error}"
                ))
            })?;
        }
        // Recorder rehydration is outside the local startup-I/O publication
        // fence. It retains its existing cooperative cancellation check, but
        // does not claim quiescence for recorder persistence here.
        startup.check("recorder rehydration persistence")?;
        recorder
            .store_command(command.hash(), command)
            .map_err(|error| {
                NodeError::Reconciliation(format!(
                    "cannot restore recorder command at qlog index {index}: {error}"
                ))
            })?;
        recorder
            .install_decision_proof_record(proof, runtime.consensus().membership())
            .map_err(|error| {
                NodeError::Reconciliation(format!(
                    "cannot install recorder decision at qlog index {index}: {error}"
                ))
            })?;
    }
    Ok(())
}

/// Installs checkpoint-owned QEFX bundles from the local restore staging
/// handoff. The archive was fully verified before the restore was published;
/// this startup step performs no recorder RPC or archive read, and runs before
/// command/proof rehydration can make the recorder advertise the entry.
#[cfg(feature = "sql")]
fn rehydrate_checkpoint_qefx_handoff(
    runtime: &NodeRuntime,
    recorder: &RecorderFileStore,
) -> Result<(), NodeError> {
    let root = runtime
        .config
        .data_dir
        .join(durability::QEFX_RESTORE_HANDOFF_DIR);
    if !root.exists() {
        return Ok(());
    }
    let mut directories = fs::read_dir(&root)
        .map_err(|error| {
            NodeError::Reconciliation(format!("cannot read QEFX restore handoff: {error}"))
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            NodeError::Reconciliation(format!("cannot enumerate QEFX restore handoff: {error}"))
        })?;
    directories.sort_by_key(|entry| entry.file_name());
    for directory in directories {
        let metadata = directory.metadata().map_err(|error| {
            NodeError::Reconciliation(format!("cannot inspect QEFX restore handoff: {error}"))
        })?;
        if !metadata.is_dir() {
            return Err(NodeError::Reconciliation(
                "QEFX restore handoff contains a non-directory entry".into(),
            ));
        }
        let index = directory
            .file_name()
            .to_str()
            .ok_or_else(|| {
                NodeError::Reconciliation("QEFX restore handoff index is not UTF-8".into())
            })?
            .parse::<LogIndex>()
            .map_err(|_| {
                NodeError::Reconciliation("QEFX restore handoff index is invalid".into())
            })?;
        let entry = runtime
            .log_store()
            .read(index)
            .map_err(|error| NodeError::Storage(error.to_string()))?
            .ok_or_else(|| {
                NodeError::Reconciliation(format!(
                    "QEFX restore handoff entry {index} is absent from qlog"
                ))
            })?;
        let manifest = fs::read(directory.path().join("binding.qefx")).map_err(|error| {
            NodeError::Reconciliation(format!("cannot read QEFX restore binding {index}: {error}"))
        })?;
        if manifest != entry.payload {
            return Err(NodeError::Reconciliation(format!(
                "QEFX restore binding {index} differs from its qlog entry"
            )));
        }
        let command = ExternalEffectCommand::decode(&manifest).map_err(|error| {
            NodeError::Reconciliation(format!("invalid QEFX restore binding {index}: {error}"))
        })?;
        if command.cluster_id() != entry.cluster_id
            || command.epoch() != entry.epoch
            || command.config_id() != entry.config_id
            || command.intended_slot() != entry.index
            || command.prev_hash() != entry.prev_hash
        {
            return Err(NodeError::Reconciliation(format!(
                "QEFX restore binding {index} does not match its qlog entry"
            )));
        }
        let mut chunks = Vec::with_capacity(command.chunks().len());
        for ordinal in 0..command.chunks().len() {
            chunks.push(
                fs::read(directory.path().join(format!("{ordinal:03}.qefc"))).map_err(|error| {
                    NodeError::Reconciliation(format!(
                        "cannot read QEFX restore chunk {index}/{ordinal}: {error}"
                    ))
                })?,
            );
        }
        let mut encoded = Vec::new();
        for chunk in &chunks {
            encoded.extend_from_slice(chunk);
        }
        QwalEffectManifestV4::verify_external_bundle(&command, &encoded).map_err(|error| {
            NodeError::Reconciliation(format!("invalid QEFX restore bundle {index}: {error}"))
        })?;
        let stored = StoredCommand::new(EntryType::Command, manifest);
        let bundle = RecorderEffectBundle::new(effect_bundle_binding(&command, &stored), chunks)
            .map_err(|error| {
                NodeError::Reconciliation(format!(
                    "cannot reconstruct QEFX restore bundle {index}: {error}"
                ))
            })?;
        let request = EffectBundleFinalizeRequest::new(bundle, stored).map_err(|error| {
            NodeError::Reconciliation(format!(
                "cannot finalize QEFX restore bundle {index}: {error}"
            ))
        })?;
        recorder.finalize_effect_bundle(&request).map_err(|error| {
            NodeError::Reconciliation(format!(
                "cannot install QEFX restore bundle {index}: {error}"
            ))
        })?;
    }
    fs::remove_dir_all(&root).map_err(|error| {
        NodeError::Reconciliation(format!(
            "cannot clear completed QEFX restore handoff: {error}"
        ))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::http::{header::CONTENT_TYPE, HeaderValue, StatusCode};

    use rhiza_core::{
        Command, CommandKind, ConfigurationState, EntryType, ErrorCategory, ErrorClassification,
        ExecutionProfile, ExternalEffectCommand, LogAnchor, LogEntry, LogHash, RecoveryAnchor,
        SnapshotIdentity, StoredCommand,
    };
    #[cfg(feature = "graph")]
    use rhiza_graph::{GraphCommandV1, GraphValueV1};
    #[cfg(feature = "kv")]
    use rhiza_kv::KvCommandV1;
    use rhiza_log::LogStore as _;
    use rhiza_quepaxa::{
        AcceptedValue, DecisionProof, Membership, Proposal, ProposalPriority, RecordRequest,
        RecordSummary, RecorderFileStore, RecorderRpc, RecorderSummary, ThreeNodeConsensus,
    };
    #[cfg(all(feature = "sql", unix))]
    use std::os::unix::fs::PermissionsExt;
    use std::{
        fs,
        sync::{
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
            mpsc, Arc, Barrier, Condvar, Mutex,
        },
    };

    #[cfg(any(feature = "sql", feature = "graph", feature = "kv"))]
    use super::node_error_response;
    #[cfg(feature = "graph")]
    use super::with_graph_client_permit;
    use super::ReadBarrierRounds;
    use super::{
        allocate_startup_cancellation_generation, allocate_startup_local_io_effect_id,
        install_test_http_decode_hook, install_test_startup_local_io_hook,
        install_test_startup_persistence_hook, persist_startup_entry, recorder_router,
        recorder_router_with_test_peer_concurrency, recorder_routes, test_http_decode_hook_lock,
        test_startup_effect_hook_lock, ConfigError, HttpRecorderClient, NodeConfig, NodeRuntime,
        NodeService, PeerConfig, StartupCancellationAuthority, StartupCloseOutcome,
        StartupIoContext, TestHttpDecodeHook, TestStartupLocalIoHook, TestStartupPersistenceHook,
        HTTP_RECORDER_MAX_CONCURRENT_WORKERS, MAX_HTTP_BODY_BYTES, RECORDER_FETCH_COMMAND_PATH,
        RECORDER_IDENTITY_PATH, RECORDER_INSPECT_PROOF_PATH, RECORDER_INSTALL_PROOF_PATH,
        RECORDER_PROTOCOL_VERSION, RECORDER_RECORD_PATH, RECORDER_WIRE_VERSION,
    };
    use super::{
        client_authenticated, next_sync_flush_retry, post,
        retain_peer_permit_until_response_body_complete, retryable_sync_flush_error,
        run_read_operation, sql_query_http_response, valid_recorder_record, Body, DurabilityError,
        Duration, FileLogStore, HeaderMap, Instant, Json, NodeError, ReadConsistency, Request,
        Response, Router, SqlCommand, SqlQueryResponse, SqlStatement, SqlValue, SqlWriteProfiler,
        MAX_COMMAND_BYTES, MAX_SQL_RESPONSE_BYTES, PROTOCOL_VERSION, SYNC_FLUSH_RETRY_INITIAL,
        VERSION_HEADER,
    };

    struct PendingThenDataBody {
        state: u8,
    }

    impl http_body::Body for PendingThenDataBody {
        type Data = axum::body::Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            match self.state {
                0 => {
                    self.state = 1;
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                }
                1 => {
                    self.state = 2;
                    std::task::Poll::Ready(Some(Ok(http_body::Frame::data(
                        axum::body::Bytes::from_static(b"response"),
                    ))))
                }
                _ => std::task::Poll::Ready(None),
            }
        }
    }

    struct BlockingStartupInspection {
        recorder: RecorderFileStore,
        started: mpsc::SyncSender<()>,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    #[derive(Clone)]
    struct HttpIdentityRecorder;

    #[derive(Clone)]
    struct GatedRecorder {
        enabled: Arc<AtomicBool>,
        inner: RecorderFileStore,
    }

    #[cfg(feature = "graph")]
    #[derive(Clone)]
    struct UnknownAfterFirstRecord {
        first: Arc<AtomicBool>,
        inner: RecorderFileStore,
    }

    #[cfg(feature = "graph")]
    impl RecorderRpc for UnknownAfterFirstRecord {
        fn record(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            request: RecordRequest,
        ) -> rhiza_quepaxa::Result<RecordSummary> {
            let summary = RecorderRpc::record(&self.inner, context, request)?;
            if self.first.swap(false, Ordering::AcqRel) {
                return Err(rhiza_quepaxa::Error::UnknownOutcome);
            }
            Ok(summary)
        }

        fn install_decision_proof(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            proof: DecisionProof,
            membership: &Membership,
        ) -> rhiza_quepaxa::Result<()> {
            RecorderRpc::install_decision_proof(&self.inner, context, proof, membership)
        }

        fn inspect_decision_proof(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            slot: u64,
        ) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
            RecorderRpc::inspect_decision_proof(&self.inner, context, slot)
        }

        fn inspect_record_summary(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            slot: u64,
        ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
            RecorderRpc::inspect_record_summary(&self.inner, context, slot)
        }

        fn recorder_id(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            RecorderRpc::recorder_id(&self.inner, context)
        }

        fn store_command_for(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            cluster_id: String,
            epoch: u64,
            config_id: u64,
            config_digest: LogHash,
            command_hash: LogHash,
            command: StoredCommand,
        ) -> rhiza_quepaxa::Result<()> {
            RecorderRpc::store_command_for(
                &self.inner,
                context,
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
                command,
            )
        }

        fn fetch_command_for(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            cluster_id: String,
            epoch: u64,
            config_id: u64,
            config_digest: LogHash,
            command_hash: LogHash,
        ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
            RecorderRpc::fetch_command_for(
                &self.inner,
                context,
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
            )
        }
    }

    impl RecorderRpc for GatedRecorder {
        fn record(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            request: RecordRequest,
        ) -> rhiza_quepaxa::Result<RecordSummary> {
            if !self.enabled.load(Ordering::Acquire) {
                return Err(rhiza_quepaxa::Error::ProposeFailed);
            }
            RecorderRpc::record(&self.inner, context, request)
        }

        fn install_decision_proof(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            proof: DecisionProof,
            membership: &Membership,
        ) -> rhiza_quepaxa::Result<()> {
            RecorderRpc::install_decision_proof(&self.inner, context, proof, membership)
        }

        fn inspect_decision_proof(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            slot: u64,
        ) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
            RecorderRpc::inspect_decision_proof(&self.inner, context, slot)
        }

        fn inspect_record_summary(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            slot: u64,
        ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
            RecorderRpc::inspect_record_summary(&self.inner, context, slot)
        }

        fn recorder_id(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            RecorderRpc::recorder_id(&self.inner, context)
        }

        fn store_command_for(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            cluster_id: String,
            epoch: u64,
            config_id: u64,
            config_digest: LogHash,
            command_hash: LogHash,
            command: StoredCommand,
        ) -> rhiza_quepaxa::Result<()> {
            RecorderRpc::store_command_for(
                &self.inner,
                context,
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
                command,
            )
        }

        fn fetch_command_for(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            cluster_id: String,
            epoch: u64,
            config_id: u64,
            config_digest: LogHash,
            command_hash: LogHash,
        ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
            RecorderRpc::fetch_command_for(
                &self.inner,
                context,
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
            )
        }
    }

    impl RecorderRpc for HttpIdentityRecorder {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }
    }

    #[derive(Clone)]
    struct CountingHttpIdentity {
        calls: Arc<AtomicUsize>,
    }

    impl RecorderRpc for CountingHttpIdentity {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok("node-1".into())
        }

        fn install_decision_proof(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            _proof: DecisionProof,
            _membership: &Membership,
        ) -> rhiza_quepaxa::Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct OversizedHttpFetch {
        calls: Arc<AtomicUsize>,
    }

    impl RecorderRpc for OversizedHttpFetch {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }

        fn fetch_command_for(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(StoredCommand::new(
                EntryType::Command,
                vec![0_u8; MAX_HTTP_BODY_BYTES],
            )))
        }
    }

    #[derive(Clone)]
    struct OversizedHttpRecord {
        calls: Arc<AtomicUsize>,
    }

    impl RecorderRpc for OversizedHttpRecord {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }

        fn record(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            request: RecordRequest,
        ) -> rhiza_quepaxa::Result<RecordSummary> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(RecordSummary {
                recorder_id: "x".repeat(MAX_HTTP_BODY_BYTES),
                slot: request.slot,
                config_id: request.config_id,
                config_digest: request.config_digest,
                step: request.step,
                first_current: Some(request.proposal),
                aggregate_prior: None,
                decided: None,
            })
        }
    }

    impl RecorderRpc for BlockingStartupInspection {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            RecorderRpc::recorder_id(
                &self.recorder,
                &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
            )
        }

        fn inspect_record_summary(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            slot: u64,
        ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
            self.started.send(()).unwrap();
            let (released, condition) = &*self.gate;
            let mut released = released.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
            RecorderRpc::inspect_record_summary(
                &self.recorder,
                &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                slot,
            )
        }

        fn fetch_command_for(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            cluster_id: String,
            epoch: u64,
            config_id: u64,
            config_digest: LogHash,
            command_hash: LogHash,
        ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
            RecorderRpc::fetch_command_for(
                &self.recorder,
                &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
            )
        }
    }

    #[test]
    fn runtime_open_does_not_persist_a_decision_returned_after_cancellation() {
        let root = tempfile::tempdir().unwrap();
        let node_ids = ["n1", "n2", "n3"];
        let membership = Membership::new(node_ids).unwrap();
        let recorders = node_ids
            .iter()
            .map(|node_id| {
                (
                    (*node_id).to_owned(),
                    RecorderFileStore::new_with_membership(
                        root.path().join("recorders").join(node_id),
                        *node_id,
                        "rhiza:sql:startup-cancel-test",
                        1,
                        1,
                        membership.clone(),
                    )
                    .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let seed_consensus = ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:startup-cancel-test",
            "n1",
            1,
            1,
            recorders
                .iter()
                .map(|(id, recorder)| {
                    (
                        id.clone(),
                        Box::new(recorder.clone()) as Box<dyn RecorderRpc>,
                    )
                })
                .collect(),
        )
        .unwrap();
        seed_consensus
            .propose_at(
                rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                1,
                LogHash::ZERO,
                Command::new(CommandKind::ReadBarrier, Vec::new()),
            )
            .unwrap();
        assert!(seed_consensus.finish_pending_rpcs(Duration::from_secs(1)));
        drop(seed_consensus);

        let (started, blocked) = mpsc::sync_channel(3);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "rhiza:sql:startup-cancel-test",
                "n1",
                1,
                1,
                recorders
                    .into_iter()
                    .map(|(id, recorder)| {
                        (
                            id,
                            Box::new(BlockingStartupInspection {
                                recorder,
                                started: started.clone(),
                                gate: Arc::clone(&gate),
                            }) as Box<dyn RecorderRpc>,
                        )
                    })
                    .collect(),
            )
            .unwrap(),
        );
        let config = NodeConfig::new_embedded(
            "startup-cancel-test",
            "n1",
            root.path().join("node"),
            1,
            1,
            node_ids,
        )
        .unwrap();
        let startup = StartupIoContext::new();
        let attempt_startup = startup.clone();
        let attempt_config = config.clone();
        let attempt = std::thread::spawn(move || {
            NodeRuntime::open_cancellable(attempt_config, consensus, &[], &attempt_startup)
        });

        blocked.recv().unwrap();
        startup.cancel(Instant::now() + Duration::from_secs(1));
        let (released, condition) = &*gate;
        *released.lock().unwrap() = true;
        condition.notify_all();

        let error = attempt.join().unwrap().unwrap_err();
        assert!(matches!(error, NodeError::Unavailable(_)), "{error}");
        let log_store = FileLogStore::open_with_configuration(
            config.data_dir.join("consensus/log"),
            &config.cluster_id,
            config.epoch,
            config.log_initial_configuration.clone(),
        )
        .unwrap();
        assert_eq!(log_store.last_index().unwrap(), None);
    }

    #[test]
    fn startup_cancellation_closes_local_io_admission_after_an_active_effect_finishes() {
        let startup = StartupIoContext::new();
        let attempt_startup = startup.clone();
        let writes = Arc::new(AtomicUsize::new(0));
        let attempt_writes = Arc::clone(&writes);
        let (entered, active) = mpsc::sync_channel(1);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let attempt_gate = Arc::clone(&gate);
        let attempt = std::thread::spawn(move || {
            let _permit = attempt_startup.admit_local_io("test local effect")?;
            entered.send(()).unwrap();
            let (released, condition) = &*attempt_gate;
            let mut released = released.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
            attempt_writes.fetch_add(1, Ordering::AcqRel);
            Ok::<(), NodeError>(())
        });

        active.recv().unwrap();
        startup.cancel(Instant::now() + Duration::from_secs(1));
        let (released, condition) = &*gate;
        *released.lock().unwrap() = true;
        condition.notify_all();
        attempt.join().unwrap().unwrap();

        assert!(startup.admit_local_io("late local effect").is_err());
        assert_eq!(writes.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn startup_local_io_admit_first_drains_and_close_never_reopens() {
        let startup = StartupIoContext::new();
        let permit = startup.admit_local_io("admitted local effect").unwrap();
        let receipt = startup.cancel(Instant::now() + Duration::from_secs(1));

        let waiting_startup = startup.clone();
        let waiting_receipt = receipt.clone();
        let waiting =
            tokio::spawn(async move { waiting_startup.await_close(&waiting_receipt).await });
        tokio::task::yield_now().await;
        drop(permit);

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), waiting)
                .await
                .unwrap()
                .unwrap(),
            StartupCloseOutcome::Drained
        );
        assert!(startup.recognizes_close_receipt(&receipt));
        assert!(startup.admit_local_io("must remain closed").is_err());
    }

    #[tokio::test]
    async fn startup_local_io_close_first_has_zero_effects_and_is_immediately_drained() {
        let startup = StartupIoContext::new();
        let receipt = startup.cancel(Instant::now() + Duration::from_secs(1));

        assert_eq!(
            startup.await_close(&receipt).await,
            StartupCloseOutcome::Drained
        );
        assert!(startup.admit_local_io("close-first effect").is_err());
    }

    #[test]
    fn startup_local_io_unfinished_stage_is_the_oldest_remaining_effect() {
        let startup = StartupIoContext::new();
        let oldest = startup.admit_local_io("oldest local effect").unwrap();
        let newer = startup.admit_local_io("newer local effect").unwrap();

        assert_eq!(startup.unfinished_stage(), "oldest local effect");
        drop(oldest);
        assert_eq!(startup.unfinished_stage(), "newer local effect");
        drop(newer);
        assert_eq!(startup.unfinished_stage(), "newer local effect");
    }

    #[tokio::test]
    async fn startup_local_io_permit_drops_during_panic_and_cloned_contexts_share_effects() {
        let startup = StartupIoContext::new();
        let panic_startup = startup.clone();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _permit = panic_startup.admit_local_io("panic local effect").unwrap();
            panic!("test local effect panic");
        }))
        .is_err());
        assert!(startup.lock_state().effects.is_empty());

        let permit = startup.admit_local_io("shared local effect").unwrap();
        let closer = startup.clone();
        let receipt = closer.cancel(Instant::now() + Duration::from_secs(1));
        let waiter = tokio::spawn(async move { closer.await_close(&receipt).await });
        tokio::task::yield_now().await;
        drop(permit);
        assert_eq!(waiter.await.unwrap(), StartupCloseOutcome::Drained);
    }

    #[tokio::test]
    async fn startup_local_io_close_receipts_replace_without_reopening_and_unresolved_is_sticky() {
        let startup = StartupIoContext::new();
        let internal = StartupIoContext::issue_cancellation_token();
        let old = startup.cancel_for_shutdown(
            internal,
            Instant::now() + Duration::from_secs(1),
            StartupCancellationAuthority::Internal,
        );
        let external = StartupIoContext::issue_cancellation_token();
        let current = startup.cancel_for_shutdown(
            external,
            Instant::now() + Duration::from_secs(1),
            StartupCancellationAuthority::External,
        );

        assert_eq!(
            startup.await_close(&old).await,
            StartupCloseOutcome::Replaced
        );
        assert!(startup.recognizes_close_receipt(&old));
        assert!(startup.recognizes_close_receipt(&current));
        assert_eq!(
            startup.await_close(&current).await,
            StartupCloseOutcome::Drained
        );
        assert!(startup.admit_local_io("replacement cannot reopen").is_err());

        let sticky = StartupIoContext::new();
        let permit = sticky
            .admit_local_io("non-preemptible local effect")
            .unwrap();
        let expired = sticky.cancel(Instant::now());
        let replacement = sticky.cancel_for_shutdown(
            StartupIoContext::issue_cancellation_token(),
            Instant::now() + Duration::from_secs(1),
            StartupCancellationAuthority::External,
        );
        assert_eq!(
            sticky.await_close(&expired).await,
            StartupCloseOutcome::Unresolved
        );
        drop(permit);
        assert_eq!(
            sticky.await_close(&replacement).await,
            StartupCloseOutcome::Unresolved
        );
        assert_eq!(sticky.unfinished_stage(), "non-preemptible local effect");
    }

    #[tokio::test]
    async fn startup_local_io_drop_latches_an_expired_deadline_when_the_awaiter_is_starved() {
        let startup = StartupIoContext::new();
        let permit = startup.admit_local_io("starved local I/O effect").unwrap();
        let receipt = startup.cancel(Instant::now() + Duration::from_millis(10));

        // Poll exactly once so `await_close` has installed its sleep, then
        // block the current-thread runtime across D.  The permit drop, rather
        // than the delayed awaiter, must establish the close proof.
        let mut close = Box::pin(startup.await_close(&receipt));
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        assert_eq!(
            std::future::Future::poll(std::pin::Pin::as_mut(&mut close), &mut context),
            std::task::Poll::Pending
        );
        std::thread::sleep(Duration::from_millis(30));
        drop(permit);

        assert_eq!(close.await, StartupCloseOutcome::Unresolved);
        assert_eq!(startup.unfinished_stage(), "starved local I/O effect");
    }

    #[tokio::test]
    async fn startup_local_io_replacement_before_old_deadline_retires_old_deadline_proof() {
        let startup = StartupIoContext::new();
        let permit = startup
            .admit_local_io("replacement race local I/O effect")
            .unwrap();
        let old = startup.cancel_for_shutdown(
            StartupIoContext::issue_cancellation_token(),
            Instant::now() + Duration::from_secs(1),
            StartupCancellationAuthority::Internal,
        );
        let current = startup.cancel_for_shutdown(
            StartupIoContext::issue_cancellation_token(),
            Instant::now() + Duration::from_millis(30),
            StartupCancellationAuthority::External,
        );

        // The old deadline has not reached D, but the replacement's deadline
        // has.  A later permit drop is governed solely by the active current
        // receipt: old remains `Replaced` even though current is unresolved.
        std::thread::sleep(Duration::from_millis(60));
        drop(permit);

        assert_eq!(
            startup.await_close(&old).await,
            StartupCloseOutcome::Replaced
        );
        assert_eq!(
            startup.await_close(&current).await,
            StartupCloseOutcome::Unresolved
        );
    }

    #[tokio::test]
    async fn startup_local_io_old_deadline_before_replacement_latches_unresolved() {
        let startup = StartupIoContext::new();
        let permit = startup
            .admit_local_io("old deadline local I/O effect")
            .unwrap();
        let old = startup.cancel_for_shutdown(
            StartupIoContext::issue_cancellation_token(),
            Instant::now() + Duration::from_millis(10),
            StartupCancellationAuthority::Internal,
        );

        // D linearizes while the old receipt still owns the gate.  The
        // replacement below observes that fact under the same mutex before it
        // retires the old receipt, so the result remains sticky.
        std::thread::sleep(Duration::from_millis(30));
        let current = startup.cancel_for_shutdown(
            StartupIoContext::issue_cancellation_token(),
            Instant::now() + Duration::from_secs(1),
            StartupCancellationAuthority::External,
        );
        drop(permit);

        assert_eq!(
            startup.await_close(&old).await,
            StartupCloseOutcome::Unresolved
        );
        assert_eq!(
            startup.await_close(&current).await,
            StartupCloseOutcome::Unresolved
        );
        assert_eq!(startup.unfinished_stage(), "old deadline local I/O effect");
    }

    #[tokio::test]
    async fn startup_local_io_close_does_not_lose_a_drop_after_subscription() {
        let startup = StartupIoContext::new();
        let permit = startup.admit_local_io("lost wake local effect").unwrap();
        let receipt = startup.cancel(Instant::now() + Duration::from_secs(1));

        let mut changes = startup.inner.changes.subscribe();
        {
            let state = startup.lock_state();
            assert_eq!(state.effects.len(), 1);
        }
        drop(permit);
        tokio::time::timeout(Duration::from_secs(1), changes.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            startup.await_close(&receipt).await,
            StartupCloseOutcome::Drained
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_local_io_spawn_blocking_abort_leaves_a_stable_unresolved_receipt() {
        let startup = StartupIoContext::new();
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let worker_gate = Arc::clone(&gate);
        let worker_startup = startup.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let _permit = worker_startup
                .admit_local_io("spawn blocking local effect")
                .unwrap();
            started.send(()).unwrap();
            let (released, condition) = &*worker_gate;
            let mut released = released.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
        });
        started_rx.await.unwrap();
        worker.abort();

        let receipt = startup.cancel(Instant::now() + Duration::from_millis(20));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), startup.await_close(&receipt))
                .await
                .unwrap(),
            StartupCloseOutcome::Unresolved
        );
        let (released, condition) = &*gate;
        *released.lock().unwrap() = true;
        condition.notify_all();
        worker.await.unwrap();
        assert_eq!(
            startup.await_close(&receipt).await,
            StartupCloseOutcome::Unresolved
        );
    }

    #[test]
    fn startup_local_io_effect_id_overflow_fails_without_reuse() {
        let allocator = AtomicU64::new(u64::MAX);
        assert_eq!(
            allocate_startup_local_io_effect_id(&allocator).unwrap(),
            u64::MAX
        );
        assert!(matches!(
            allocate_startup_local_io_effect_id(&allocator),
            Err(NodeError::Invariant(message)) if message == "startup local I/O effect id exhausted"
        ));
        assert!(matches!(
            allocate_startup_local_io_effect_id(&allocator),
            Err(NodeError::Invariant(message)) if message == "startup local I/O effect id exhausted"
        ));
        assert_eq!(allocator.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn startup_close_receipt_overflow_fails_closed_without_reuse() {
        let startup = StartupIoContext::new();
        startup.lock_state().next_receipt_id = u64::MAX;
        let final_receipt = startup.cancel_for_shutdown(
            StartupIoContext::issue_cancellation_token(),
            Instant::now() + Duration::from_secs(1),
            StartupCancellationAuthority::Internal,
        );
        let overflow = startup.cancel_for_shutdown(
            StartupIoContext::issue_cancellation_token(),
            Instant::now() + Duration::from_secs(1),
            StartupCancellationAuthority::External,
        );

        assert!(startup.recognizes_close_receipt(&final_receipt));
        assert!(!startup.recognizes_close_receipt(&overflow));
        assert_ne!(final_receipt, overflow);
        assert_eq!(
            startup.await_close(&final_receipt).await,
            StartupCloseOutcome::Unresolved
        );
        assert_eq!(
            startup.await_close(&overflow).await,
            StartupCloseOutcome::Unresolved
        );
    }

    #[cfg(feature = "sql")]
    fn startup_local_io_runtime_parts(
        root: &tempfile::TempDir,
        name: &str,
    ) -> (NodeConfig, Arc<ThreeNodeConsensus>) {
        let cluster_id = format!("rhiza:sql:startup-local-io-{name}");
        let config = NodeConfig::new_embedded(
            &cluster_id,
            "n1",
            root.path().join("node"),
            1,
            1,
            ["n1", "n2", "n3"],
        )
        .unwrap();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                &cluster_id,
                "n1",
                1,
                1,
                [
                    root.path().join("recorders/n1"),
                    root.path().join("recorders/n2"),
                    root.path().join("recorders/n3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        );
        (config, consensus)
    }

    #[cfg(feature = "sql")]
    #[test]
    fn runtime_open_close_first_retries_with_the_same_context_create_no_node_files() {
        let root = tempfile::tempdir().unwrap();
        let (config, consensus) = startup_local_io_runtime_parts(&root, "close-first");
        let startup = StartupIoContext::new();
        let receipt = startup.cancel(Instant::now() + Duration::from_secs(1));

        let first =
            NodeRuntime::open_cancellable(config.clone(), Arc::clone(&consensus), &[], &startup)
                .unwrap_err();
        let retry =
            NodeRuntime::open_cancellable(config.clone(), consensus, &[], &startup).unwrap_err();
        assert!(
            matches!(first, NodeError::StartupCancelled { .. }),
            "{first}"
        );
        assert!(
            matches!(retry, NodeError::StartupCancelled { .. }),
            "{retry}"
        );
        assert!(!config.data_dir().exists());
        assert!(startup.recognizes_close_receipt(&receipt));
    }

    #[cfg(feature = "sql")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_lock_effect_admitted_before_close_drains_after_file_lock_completes() {
        let _hook_lock = test_startup_effect_hook_lock().lock().await;
        let root = tempfile::tempdir().unwrap();
        let (config, consensus) = startup_local_io_runtime_parts(&root, "lock-drain");
        let startup = StartupIoContext::new();
        let (entered, entered_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let _hook = install_test_startup_local_io_hook(TestStartupLocalIoHook {
            context_id: startup.inner.context_id,
            stage: "runtime data lock acquisition",
            entered,
            release: Arc::clone(&release),
        });
        let worker_config = config.clone();
        let worker_startup = startup.clone();
        let worker = tokio::task::spawn_blocking(move || {
            NodeRuntime::open_cancellable(worker_config, consensus, &[], &worker_startup)
        });
        tokio::task::spawn_blocking(move || entered_rx.recv())
            .await
            .unwrap()
            .unwrap();

        let receipt = startup.cancel(Instant::now() + Duration::from_secs(1));
        let mut close = Box::pin(startup.await_close(&receipt));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut close)
                .await
                .is_err(),
            "close must wait for the admitted lock effect"
        );
        assert!(config.data_dir().exists());
        assert!(!config.data_dir().join(".node.lock").exists());

        let (released, condition) = &*release;
        *released.lock().unwrap() = true;
        condition.notify_all();
        let error = worker.await.unwrap().unwrap_err();
        assert!(
            matches!(error, NodeError::StartupCancelled { .. }),
            "{error}"
        );
        assert!(config.data_dir().join(".node.lock").exists());
        assert_eq!(close.await, StartupCloseOutcome::Drained);
    }

    #[cfg(feature = "sql")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_persist_partial_qlog_append_preserves_injected_materializer_source_and_drains()
    {
        let _hook_lock = test_startup_effect_hook_lock().lock().await;
        let root = tempfile::tempdir().unwrap();
        let (config, consensus) = startup_local_io_runtime_parts(&root, "persist-partial");
        fs::create_dir_all(config.data_dir()).unwrap();
        let log_store = FileLogStore::open_with_configuration(
            config.data_dir().join("consensus/log"),
            config.cluster_id(),
            config.epoch(),
            config.log_initial_configuration().clone(),
        )
        .unwrap();
        let materializer =
            super::Materializer::open(&config, config.log_initial_configuration(), None).unwrap();
        let payload = Vec::new();
        let entry = LogEntry {
            cluster_id: config.cluster_id().to_owned(),
            epoch: config.epoch(),
            config_id: config.config_id(),
            index: 1,
            entry_type: EntryType::Noop,
            prev_hash: LogHash::ZERO,
            hash: LogEntry::calculate_hash(
                config.cluster_id(),
                1,
                config.epoch(),
                config.config_id(),
                EntryType::Noop,
                LogHash::ZERO,
                &payload,
            ),
            payload,
        };
        let injected = NodeError::Reconciliation("injected materializer apply source".into());
        let startup = StartupIoContext::new();
        let (entered, entered_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let _hook = install_test_startup_persistence_hook(TestStartupPersistenceHook {
            context_id: startup.inner.context_id,
            entered,
            release: Arc::clone(&release),
            error: injected.clone(),
        });
        let worker_config = config.clone();
        let worker_startup = startup.clone();
        let worker = tokio::task::spawn_blocking(move || {
            persist_startup_entry(
                &worker_config,
                consensus.as_ref(),
                &log_store,
                &materializer,
                &entry,
                1,
                LogHash::ZERO,
                &worker_startup,
            )
        });
        tokio::task::spawn_blocking(move || entered_rx.recv())
            .await
            .unwrap()
            .unwrap();

        let receipt = startup.cancel(Instant::now() + Duration::from_secs(1));
        let mut close = Box::pin(startup.await_close(&receipt));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut close)
                .await
                .is_err(),
            "close must wait after qlog append until the persistence effect returns"
        );
        let (released, condition) = &*release;
        *released.lock().unwrap() = true;
        condition.notify_all();
        assert_eq!(worker.await.unwrap().unwrap_err(), injected);
        let appended = FileLogStore::open_with_configuration(
            config.data_dir().join("consensus/log"),
            config.cluster_id(),
            config.epoch(),
            config.log_initial_configuration().clone(),
        )
        .unwrap();
        assert_eq!(appended.last_index().unwrap(), Some(1));
        assert_eq!(close.await, StartupCloseOutcome::Drained);
    }

    #[cfg(feature = "sql")]
    #[test]
    fn runtime_qlog_open_failure_preserves_source_and_retry_uses_the_same_context() {
        let root = tempfile::tempdir().unwrap();
        let (config, consensus) = startup_local_io_runtime_parts(&root, "qlog-retry");
        fs::create_dir_all(config.data_dir()).unwrap();
        fs::write(config.data_dir().join("consensus"), b"not a directory").unwrap();
        let expected = FileLogStore::open_with_configuration(
            config.data_dir().join("consensus/log"),
            config.cluster_id(),
            config.epoch(),
            config.log_initial_configuration().clone(),
        )
        .unwrap_err()
        .to_string();
        let startup = StartupIoContext::new();

        let error =
            NodeRuntime::open_cancellable(config.clone(), Arc::clone(&consensus), &[], &startup)
                .unwrap_err();
        assert_eq!(error, NodeError::Storage(expected));
        assert!(startup.lock_state().effects.is_empty());

        fs::remove_file(config.data_dir().join("consensus")).unwrap();
        let runtime = NodeRuntime::open_cancellable(config, consensus, &[], &startup).unwrap();
        assert!(runtime.is_ready());
    }

    #[cfg(all(feature = "sql", unix))]
    #[test]
    fn runtime_materializer_open_failure_preserves_its_exact_source() {
        let root = tempfile::tempdir().unwrap();
        let (config, consensus) = startup_local_io_runtime_parts(&root, "materializer-source");
        fs::create_dir_all(config.data_dir()).unwrap();
        FileLogStore::open_with_configuration(
            config.data_dir().join("consensus/log"),
            config.cluster_id(),
            config.epoch(),
            config.log_initial_configuration().clone(),
        )
        .unwrap();
        fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(config.data_dir().join(".node.lock"))
            .unwrap();
        let original_permissions = fs::metadata(config.data_dir()).unwrap().permissions();
        let mut read_only = original_permissions.clone();
        read_only.set_mode(0o500);
        fs::set_permissions(config.data_dir(), read_only).unwrap();

        let expected =
            match super::Materializer::open(&config, config.log_initial_configuration(), None) {
                Ok(_) => panic!("materializer open unexpectedly succeeded"),
                Err(error) => error,
            };
        let actual =
            NodeRuntime::open_cancellable(config.clone(), consensus, &[], &StartupIoContext::new())
                .unwrap_err();
        fs::set_permissions(config.data_dir(), original_permissions).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn startup_cancellation_publication_and_precedence_are_atomic() {
        for _ in 0..1_000 {
            let startup = StartupIoContext::new();
            let internal = StartupIoContext::issue_cancellation_token();
            let external = StartupIoContext::issue_cancellation_token();
            let internal_deadline = Instant::now() + Duration::from_secs(25);
            let external_deadline = Instant::now() + Duration::from_millis(250);
            startup.cancel_for_shutdown(
                internal.clone(),
                internal_deadline,
                StartupCancellationAuthority::Internal,
            );

            let barrier = Arc::new(Barrier::new(2));
            let checker_startup = startup.clone();
            let checker_internal = internal.clone();
            let checker_external = external.clone();
            let checker_barrier = Arc::clone(&barrier);
            let checker = std::thread::spawn(move || {
                checker_barrier.wait();
                for _ in 0..8 {
                    match checker_startup.check("concurrent startup cancellation check") {
                        Err(NodeError::StartupCancelled { token, .. }) => {
                            assert!(token == checker_internal || token == checker_external);
                        }
                        result => {
                            panic!("cancellation metadata was not atomically published: {result:?}")
                        }
                    }
                }
            });
            barrier.wait();
            startup.cancel_for_shutdown(
                external.clone(),
                external_deadline,
                StartupCancellationAuthority::External,
            );
            checker.join().unwrap();
            let state = startup.lock_state();
            let cancellation = state.cancellation.as_ref().unwrap();
            assert_eq!(cancellation.token, external);
            assert_eq!(cancellation.deadline, external_deadline);
            assert_eq!(
                cancellation.authority,
                StartupCancellationAuthority::External
            );
            drop(state);

            let later_internal = StartupIoContext::issue_cancellation_token();
            startup.cancel_for_shutdown(
                later_internal,
                Instant::now(),
                StartupCancellationAuthority::Internal,
            );
            let state = startup.lock_state();
            let cancellation = state.cancellation.as_ref().unwrap();
            assert_eq!(cancellation.token, external);
            assert_eq!(cancellation.deadline, external_deadline);
        }
    }

    #[test]
    fn startup_cancellation_allocator_exhausts_without_reusing_a_generation() {
        let allocator = AtomicU64::new(u64::MAX);
        let last = allocate_startup_cancellation_generation(&allocator);
        assert_eq!(last, u64::MAX);
        assert_eq!(allocator.load(Ordering::Acquire), 0);
        assert!(std::panic::catch_unwind(|| {
            allocate_startup_cancellation_generation(&allocator)
        })
        .is_err());
        assert!(std::panic::catch_unwind(|| {
            allocate_startup_cancellation_generation(&allocator)
        })
        .is_err());
        assert_eq!(allocator.load(Ordering::Acquire), 0);
    }

    #[test]
    fn embedded_config_accepts_matching_canonical_profile_ids() {
        for (cluster_id, profile) in [
            ("rhiza:graph:embedded", ExecutionProfile::Graph),
            ("rhiza:kv:embedded", ExecutionProfile::Kv),
        ] {
            let config = NodeConfig::new_embedded(
                cluster_id,
                "n1",
                std::env::temp_dir().join(profile.as_str()),
                1,
                1,
                ["n1", "n2", "n3"],
            )
            .unwrap()
            .with_execution_profile(profile)
            .unwrap();

            assert_eq!(config.cluster_id(), cluster_id);
            assert_eq!(config.logical_cluster_id(), "embedded");
        }
    }

    #[test]
    fn embedded_config_rejects_conflicting_canonical_profile_and_preserves_logical_ids() {
        let conflicting = NodeConfig::new_embedded(
            "rhiza:graph:embedded",
            "n1",
            std::env::temp_dir().join("conflicting-profile"),
            1,
            1,
            ["n1", "n2", "n3"],
        )
        .unwrap()
        .with_execution_profile(ExecutionProfile::Sqlite)
        .unwrap_err();
        assert!(matches!(
            conflicting,
            ConfigError::ClusterIdProfileMismatch {
                expected: ExecutionProfile::Sqlite,
                actual: ExecutionProfile::Graph,
            }
        ));

        let logical = NodeConfig::new_embedded(
            "embedded",
            "n1",
            std::env::temp_dir().join("logical-profile"),
            1,
            1,
            ["n1", "n2", "n3"],
        )
        .unwrap();
        assert_eq!(logical.cluster_id(), "rhiza:sql:embedded");
        assert_eq!(logical.logical_cluster_id(), "embedded");
    }

    #[test]
    fn node_error_classification_reports_observable_retry_semantics() {
        let cases = [
            (
                NodeError::InvalidRequest("missing key".into()),
                "invalid_request",
                ErrorCategory::InvalidRequest,
                false,
            ),
            (
                NodeError::PreconditionFailed("stale version".into()),
                "precondition_failed",
                ErrorCategory::Conflict,
                false,
            ),
            (
                NodeError::Unavailable("no quorum".into()),
                "unavailable",
                ErrorCategory::Unavailable,
                true,
            ),
            (
                NodeError::ResourceExhausted("result too large".into()),
                "resource_exhausted",
                ErrorCategory::ResourceExhausted,
                true,
            ),
            (
                NodeError::Invariant("invalid log".into()),
                "invariant_violation",
                ErrorCategory::Internal,
                false,
            ),
        ];

        for (error, code, category, retryable) in cases {
            let classification = error.classification();

            assert_eq!(classification.code(), code);
            assert_eq!(classification.category(), category);
            assert_eq!(classification.retryable(), retryable);
        }
    }

    #[cfg(feature = "sql")]
    #[test]
    fn sql_batch_error_classification_preserves_statement_index_category() {
        let error = NodeError::InvalidSqlStatement {
            statement_index: 3,
            message: "syntax error".into(),
        };

        let classification = error.classification();

        assert_eq!(classification.code(), "invalid_request");
        assert_eq!(classification.category(), ErrorCategory::InvalidRequest);
        assert!(!classification.retryable());
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn node_error_http_response_preserves_contract() {
        let snapshot = RecoveryAnchor::new(
            "cluster",
            1,
            ConfigurationState::active(1, LogHash::digest(&[b"node-error-test-config"])),
            1,
            LogAnchor::new(1, LogHash::ZERO),
            SnapshotIdentity::new(
                "snapshot",
                LogHash::digest(&[b"node-error-test-snapshot"]),
                0,
                rhiza_sql::sql_executor_fingerprint().unwrap(),
            ),
        );
        let cases = vec![
            (
                NodeError::InvalidSqlStatement {
                    statement_index: 3,
                    message: "syntax error".into(),
                },
                axum::http::StatusCode::BAD_REQUEST,
                "invalid_request",
                false,
                Some(3),
            ),
            (
                NodeError::PreconditionFailed("stale version".into()),
                axum::http::StatusCode::CONFLICT,
                "precondition_failed",
                false,
                None,
            ),
            (
                NodeError::SnapshotRequired(Box::new(snapshot)),
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "snapshot_required",
                false,
                None,
            ),
            (
                NodeError::Storage("disk failed".into()),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "storage_error",
                false,
                None,
            ),
        ];

        for (node_error, status, code, retryable, statement_index) in cases {
            let response = node_error_response(node_error);
            assert_eq!(response.status(), status);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let error: super::ClientErrorResponse = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(error.code, code);
            assert_eq!(error.retryable, retryable);
            assert_eq!(error.statement_index, statement_index);
            assert!(value.get("category").is_none());
            assert!(matches!(
                error.message.as_str(),
                "request could not be processed"
                    | "request conflicts with current state"
                    | "service is temporarily unavailable"
                    | "internal server error"
            ));
        }
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn node_error_http_response_hides_display_details() {
        let detail = "/srv/rhiza/private/consensus/log: checksum mismatch";
        let cases = [
            (
                NodeError::Storage(detail.into()),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "storage_error",
                None,
                "internal server error",
            ),
            (
                NodeError::Reconciliation(detail.into()),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "reconciliation_error",
                None,
                "internal server error",
            ),
            (
                NodeError::InvalidSqlStatement {
                    statement_index: 7,
                    message: detail.into(),
                },
                axum::http::StatusCode::BAD_REQUEST,
                "invalid_request",
                Some(7),
                "request could not be processed",
            ),
        ];

        for (node_error, status, code, statement_index, message) in cases {
            let response = node_error_response(node_error);
            assert_eq!(response.status(), status);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let error: super::ClientErrorResponse = serde_json::from_slice(&body).unwrap();
            assert_eq!(error.code, code);
            assert_eq!(error.statement_index, statement_index);
            assert_eq!(error.message, message);
            assert!(!String::from_utf8(body.to_vec()).unwrap().contains(detail));
        }
    }

    #[tokio::test]
    async fn client_json_error_response_uses_a_stable_message() {
        for (status, code) in [
            (axum::http::StatusCode::BAD_REQUEST, "invalid_json"),
            (
                axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
            ),
        ] {
            let response = super::client_json_error_response(status);

            assert_eq!(response.status(), status);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let error: super::ClientErrorResponse = serde_json::from_slice(&body).unwrap();
            assert_eq!(error.code, code);
            assert!(!error.retryable);
            assert_eq!(error.message, "request body is invalid");
            assert_eq!(error.statement_index, None);
        }
    }

    #[tokio::test]
    async fn client_task_error_hides_join_error_details() {
        let detail = "private task detail\nforged log entry";
        let error = tokio::spawn(async move { panic!("{detail}") })
            .await
            .unwrap_err();
        let response = super::client_task_error(error);

        assert_eq!(
            response.status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let error: super::ClientErrorResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(error.code, "task_failed");
        assert!(!error.retryable);
        assert_eq!(error.message, "request task failed");
        assert!(!String::from_utf8(body.to_vec()).unwrap().contains(detail));
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn fatal_readiness_response_hides_fatal_reason() {
        let (_dir, runtime) = sql_test_runtime();
        let detail = "/srv/rhiza/private\nforged log entry";
        runtime.latch(NodeError::Storage(detail.into()));
        let response = super::runtime_readiness_response(&runtime).unwrap();

        assert_eq!(
            response.status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let error: super::ClientErrorResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(error.code, "fatal");
        assert!(!error.retryable);
        assert_eq!(error.message, "node is fatally unavailable");
        assert!(!String::from_utf8(body.to_vec()).unwrap().contains(detail));
    }

    #[test]
    fn escaped_error_detail_escapes_control_characters() {
        let detail = "checksum mismatch\nforged entry\r\u{1b}[2J";

        assert_eq!(
            super::escaped_error_detail(&detail),
            r"checksum mismatch\nforged entry\r\u{1b}[2J"
        );
    }

    #[test]
    fn escaped_error_detail_bounds_escape_expansion() {
        let detail = "\n".repeat(super::MAX_ESCAPED_ERROR_DETAIL_BYTES / 2 + 1);
        let escaped = super::escaped_error_detail(&detail);

        assert!(
            escaped.len() <= super::MAX_ESCAPED_ERROR_DETAIL_BYTES,
            "escaped detail must stay within the log budget"
        );
        assert!(
            escaped.ends_with(super::ESCAPED_ERROR_DETAIL_TRUNCATION_MARKER),
            "truncated details must be explicit"
        );
        assert!(!escaped.contains('\n'));
    }

    #[tokio::test]
    async fn client_error_responses_preserve_payload_and_authentication_wire_codes() {
        for (status, code, retryable, category) in [
            (
                axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                false,
                ErrorCategory::ResourceExhausted,
            ),
            (
                axum::http::StatusCode::UNAUTHORIZED,
                "unauthorized",
                false,
                ErrorCategory::Authentication,
            ),
        ] {
            let response =
                super::client_error_response(status, code, retryable, "request failed", None);
            assert_eq!(response.status(), status);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let error: super::ClientErrorResponse = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(error.code, code);
            assert_eq!(error.retryable, retryable);
            assert!(value.get("category").is_none());
            assert_eq!(
                ErrorClassification::from_server_code(code, retryable).category(),
                category
            );
        }
    }

    #[test]
    fn concurrent_read_barriers_registered_before_cutoff_share_one_generation() {
        let rounds = ReadBarrierRounds::new(Duration::ZERO);
        let cancelled = AtomicBool::new(false);
        let participants = (0..4).map(|_| rounds.join().unwrap()).collect::<Vec<_>>();
        let generation = participants[0].generation();
        assert!(participants[0].is_leader());
        assert!(participants[1..]
            .iter()
            .all(|participant| !participant.is_leader() && participant.generation() == generation));

        let calls = AtomicUsize::new(0);
        let mut publication = participants[0].publication().unwrap();
        publication.wait_turn(&cancelled).unwrap();
        publication.start(&cancelled).unwrap();
        let anchor = LogAnchor::new(7, LogHash::digest(&[b"shared-barrier"]));
        calls.fetch_add(1, Ordering::Relaxed);
        publication.publish(Ok(anchor));

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        for participant in &participants[1..] {
            assert_eq!(participant.wait(&cancelled).unwrap(), anchor);
        }
    }

    #[test]
    fn read_barrier_arriving_after_running_cutoff_uses_next_generation() {
        let rounds = Arc::new(ReadBarrierRounds::new(Duration::ZERO));
        let cancelled = Arc::new(AtomicBool::new(false));
        let first = rounds.join().unwrap();
        let first_generation = first.generation();
        let (running_tx, running_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let first_cancelled = Arc::clone(&cancelled);
        let first_worker = std::thread::spawn(move || {
            let mut publication = first.publication().unwrap();
            publication.wait_turn(&first_cancelled).unwrap();
            publication.start(&first_cancelled).unwrap();
            running_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            let anchor = LogAnchor::new(1, LogHash::digest(&[b"first"]));
            publication.publish(Ok(anchor));
            anchor
        });
        running_rx.recv().unwrap();

        let late = rounds.join().unwrap();
        assert!(late.is_leader());
        assert_eq!(late.generation(), first_generation + 1);
        release_tx.send(()).unwrap();
        assert_eq!(first_worker.join().unwrap().index(), 1);

        let mut publication = late.publication().unwrap();
        publication.wait_turn(&cancelled).unwrap();
        publication.start(&cancelled).unwrap();
        let second = LogAnchor::new(2, LogHash::digest(&[b"second"]));
        publication.publish(Ok(second));
        assert_eq!(late.wait(&cancelled).unwrap(), second);
    }

    #[test]
    fn completed_read_barrier_is_not_reused_and_predecessor_failure_retries_independently() {
        let rounds = ReadBarrierRounds::new(Duration::ZERO);
        let cancelled = AtomicBool::new(false);
        let failed = rounds.join().unwrap();
        let failed_generation = failed.generation();
        let mut publication = failed.publication().unwrap();
        publication.wait_turn(&cancelled).unwrap();
        publication.start(&cancelled).unwrap();
        publication.publish(Err(NodeError::Unavailable("no quorum".into())));
        assert!(matches!(
            failed.wait(&cancelled),
            Err(NodeError::Unavailable(_))
        ));

        let retry = rounds.join().unwrap();
        assert!(retry.is_leader());
        assert_eq!(retry.generation(), failed_generation + 1);
        let mut publication = retry.publication().unwrap();
        publication.wait_turn(&cancelled).unwrap();
        publication.start(&cancelled).unwrap();
        let anchor = LogAnchor::new(1, LogHash::digest(&[b"retry"]));
        publication.publish(Ok(anchor));
        assert_eq!(retry.wait(&cancelled).unwrap(), anchor);

        let later = rounds.join().unwrap();
        assert!(later.is_leader());
        assert_eq!(later.generation(), retry.generation() + 1);
    }

    #[test]
    fn read_barrier_leader_drop_and_global_cancel_wake_waiters() {
        let rounds = Arc::new(ReadBarrierRounds::new(Duration::ZERO));
        let cancelled = Arc::new(AtomicBool::new(false));
        let abandoned = rounds.join().unwrap();
        let follower = rounds.join().unwrap();
        drop(abandoned.publication().unwrap());
        assert!(matches!(
            follower.wait(&cancelled),
            Err(NodeError::Unavailable(_))
        ));

        let leader = rounds.join().unwrap();
        let waiting = rounds.join().unwrap();
        let waiting_cancelled = Arc::clone(&cancelled);
        let waiter = std::thread::spawn(move || waiting.wait(&waiting_cancelled));
        cancelled.store(true, Ordering::Release);
        rounds.cancel_waiters();
        assert!(matches!(
            waiter.join().unwrap(),
            Err(NodeError::Unavailable(_))
        ));
        drop(leader.publication().unwrap());
    }

    #[test]
    fn sql_c4_read_barrier_shares_one_qlog_anchor_and_preserves_snapshot_tip() {
        let (_dir, mut runtime) = sql_test_runtime();
        runtime.read_barriers = ReadBarrierRounds::new(Duration::from_millis(20));
        let runtime = Arc::new(runtime);
        let start = Arc::new(Barrier::new(4));
        let workers = (0..4)
            .map(|_| {
                let runtime = Arc::clone(&runtime);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    runtime.read("missing", ReadConsistency::ReadBarrier)
                })
            })
            .collect::<Vec<_>>();

        let responses = workers
            .into_iter()
            .map(|worker| worker.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert!(responses
            .iter()
            .all(|response| response.applied_index == 0 && response.hash == LogHash::ZERO));
        assert_eq!(runtime.log_store().last_index().unwrap(), None);
    }

    #[test]
    fn read_barrier_anchor_remains_valid_when_materialized_tip_advances() {
        let (_dir, runtime) = sql_test_runtime();
        let anchor = runtime.establish_read_barrier().unwrap();
        let write = runtime.write("request-1", "alpha", "one").unwrap();
        assert!(write.applied_index > anchor.index());

        let _commit = runtime.lock_commit().unwrap();
        runtime.ensure_ready().unwrap();
        runtime.ensure_writes_active().unwrap();
        runtime
            .validate_read_barrier_descendant_locked(anchor)
            .unwrap();
        let read = runtime.read_local("alpha", Some(anchor.index())).unwrap();

        assert_eq!(read.value.as_deref(), Some("one"));
        assert_eq!(read.applied_index, write.applied_index);
        assert_eq!(read.hash, write.hash);
    }

    #[test]
    fn checkpoint_publication_does_not_close_runtime_readiness_or_writes() {
        let (_dir, runtime) = sql_test_runtime();
        runtime.checkpointing.store(true, Ordering::Release);

        assert!(runtime.is_ready());
        runtime.ensure_ready().unwrap();
        let write = runtime.write("request-1", "alpha", "one").unwrap();

        assert_eq!(write.applied_index, 1);
        assert_eq!(
            runtime.read("alpha", ReadConsistency::Local).unwrap().value,
            Some("one".into())
        );
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_read_barrier_checks_materialized_tip_once_before_snapshot() {
        let (_dir, runtime) = graph_test_runtime();

        let response = runtime
            .get_graph_document("missing", ReadConsistency::ReadBarrier)
            .unwrap();

        assert_eq!(response.applied_index, 0);
        assert_eq!(response.hash, LogHash::ZERO);
        assert_eq!(runtime.materialized_tip_checks.load(Ordering::Relaxed), 1);
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_c4_read_barrier_shares_one_qlog_anchor_and_preserves_snapshot_tip() {
        let (_dir, mut runtime) = graph_test_runtime();
        runtime.read_barriers = ReadBarrierRounds::new(Duration::from_millis(20));
        let runtime = Arc::new(runtime);
        let start = Arc::new(Barrier::new(4));
        let workers = (0..4)
            .map(|_| {
                let runtime = Arc::clone(&runtime);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    runtime.get_graph_document("missing", ReadConsistency::ReadBarrier)
                })
            })
            .collect::<Vec<_>>();

        let responses = workers
            .into_iter()
            .map(|worker| worker.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert!(responses
            .iter()
            .all(|response| response.applied_index == 0 && response.hash == LogHash::ZERO));
        assert_eq!(runtime.log_store().last_index().unwrap(), None);
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_read_barrier_releases_commit_lock_before_backend_snapshot() {
        let (_dir, mut runtime) = graph_test_runtime();
        let initial = runtime
            .mutate_graph(
                GraphCommandV1::put_document(
                    "request-1",
                    "document-1",
                    GraphValueV1::String("one".into()),
                )
                .unwrap(),
            )
            .unwrap();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        runtime.read_barrier_before_snapshot_hook = Some(Arc::new({
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            move || {
                entered.wait();
                release.wait();
            }
        }));
        let runtime = Arc::new(runtime);
        let reader = {
            let runtime = Arc::clone(&runtime);
            std::thread::spawn(move || {
                runtime.get_graph_document("document-1", ReadConsistency::ReadBarrier)
            })
        };
        entered.wait();
        let (advanced_tx, advanced_rx) = mpsc::channel();
        let writer = {
            let runtime = Arc::clone(&runtime);
            std::thread::spawn(move || {
                let outcome = runtime
                    .mutate_graph(
                        GraphCommandV1::put_document(
                            "request-2",
                            "document-1",
                            GraphValueV1::String("two".into()),
                        )
                        .unwrap(),
                    )
                    .unwrap();
                advanced_tx
                    .send((outcome.applied_index(), outcome.hash()))
                    .unwrap();
                outcome
            })
        };

        let advanced_before_snapshot = advanced_rx.recv_timeout(Duration::from_secs(2));
        release.wait();
        let written = writer.join().unwrap();
        let read = reader.join().unwrap().unwrap();

        assert!(
            advanced_before_snapshot.is_ok(),
            "graph write must advance while the read is paused before its backend snapshot"
        );
        assert!(read.applied_index >= initial.applied_index());
        if read.applied_index == initial.applied_index() {
            assert_eq!(read.hash, initial.hash());
        }
        assert_eq!(read.applied_index, written.applied_index());
        assert_eq!(read.hash, written.hash());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn read_barrier_rejects_same_index_snapshot_with_different_hash() {
        let (_dir, runtime) = graph_test_runtime();
        let anchor = LogAnchor::new(7, LogHash::digest(&[b"barrier-anchor"]));
        let observed = LogAnchor::new(7, LogHash::digest(&[b"divergent-snapshot"]));

        assert!(matches!(
            runtime.validate_read_barrier_snapshot(anchor, observed),
            Err(NodeError::Invariant(message))
                if message.contains("snapshot tip hash differs")
        ));
        assert!(runtime.is_fatal());
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_read_barrier_checks_materialized_tip_once_before_snapshot() {
        let (_dir, runtime) = kv_test_runtime();

        let response = runtime
            .get_kv(b"missing", ReadConsistency::ReadBarrier)
            .unwrap();

        assert_eq!(response.applied_index, 0);
        assert_eq!(response.hash, LogHash::ZERO);
        assert_eq!(runtime.materialized_tip_checks.load(Ordering::Relaxed), 1);
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_c4_read_barrier_shares_one_qlog_anchor_and_preserves_snapshot_tip() {
        let (_dir, mut runtime) = kv_test_runtime();
        runtime.read_barriers = ReadBarrierRounds::new(Duration::from_millis(20));
        let runtime = Arc::new(runtime);
        let start = Arc::new(Barrier::new(4));
        let workers = (0..4)
            .map(|_| {
                let runtime = Arc::clone(&runtime);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    runtime.get_kv(b"missing", ReadConsistency::ReadBarrier)
                })
            })
            .collect::<Vec<_>>();

        let responses = workers
            .into_iter()
            .map(|worker| worker.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert!(responses
            .iter()
            .all(|response| response.applied_index == 0 && response.hash == LogHash::ZERO));
        assert_eq!(runtime.log_store().last_index().unwrap(), None);
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_read_barrier_releases_commit_lock_before_backend_snapshot() {
        let (_dir, mut runtime) = kv_test_runtime();
        let initial = runtime
            .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"one".to_vec()).unwrap())
            .unwrap();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        runtime.read_barrier_before_snapshot_hook = Some(Arc::new({
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            move || {
                entered.wait();
                release.wait();
            }
        }));
        let runtime = Arc::new(runtime);
        let reader = {
            let runtime = Arc::clone(&runtime);
            std::thread::spawn(move || runtime.get_kv(b"key", ReadConsistency::ReadBarrier))
        };
        entered.wait();
        let (advanced_tx, advanced_rx) = mpsc::channel();
        let writer = {
            let runtime = Arc::clone(&runtime);
            std::thread::spawn(move || {
                let outcome = runtime
                    .mutate_kv(
                        KvCommandV1::put("request-2", b"key".to_vec(), b"two".to_vec()).unwrap(),
                    )
                    .unwrap();
                advanced_tx
                    .send((outcome.applied_index(), outcome.hash()))
                    .unwrap();
                outcome
            })
        };

        let advanced_before_snapshot = advanced_rx.recv_timeout(Duration::from_secs(2));
        release.wait();
        let written = writer.join().unwrap();
        let read = reader.join().unwrap().unwrap();

        assert!(
            advanced_before_snapshot.is_ok(),
            "KV write must advance while the read is paused before its backend snapshot"
        );
        assert!(read.applied_index >= initial.applied_index());
        if read.applied_index == initial.applied_index() {
            assert_eq!(read.hash, initial.hash());
        }
        assert_eq!(read.applied_index, written.applied_index());
        assert_eq!(read.hash, written.hash());
    }

    #[test]
    fn client_authentication_rejects_empty_expected_token() {
        let mut headers = HeaderMap::new();
        headers.insert(VERSION_HEADER, HeaderValue::from_static(PROTOCOL_VERSION));
        headers.insert("authorization", HeaderValue::from_static("Bearer "));

        assert!(!client_authenticated(&headers, ""));
    }

    #[test]
    fn recorder_record_rejects_oversized_inline_command() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(
            EntryType::Command,
            vec![0_u8; MAX_COMMAND_BYTES.saturating_add(1)],
        );
        let request = RecordRequest {
            cluster_id: "rhiza:sql:node-unit-test".into(),
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            slot: 1,
            step: 1,
            proposal: Proposal::new(
                ProposalPriority::MAX,
                "n1",
                1,
                AcceptedValue::from_command(
                    "rhiza:sql:node-unit-test",
                    1,
                    1,
                    1,
                    LogHash::ZERO,
                    &command,
                ),
            ),
            command: Some(command),
        };

        assert!(!valid_recorder_record(&request));
    }

    #[test]
    fn sync_flush_retry_doubles_to_a_jitter_free_cap() {
        let mut delay = SYNC_FLUSH_RETRY_INITIAL;
        let mut delays = Vec::new();
        for _ in 0..7 {
            delays.push(delay);
            delay = next_sync_flush_retry(delay);
        }

        assert_eq!(
            delays,
            [50, 100, 200, 400, 800, 1_000, 1_000].map(Duration::from_millis)
        );
    }

    #[test]
    fn transient_qefx_unavailability_is_a_retryable_sync_flush_error() {
        assert!(retryable_sync_flush_error(&DurabilityError::Unavailable));
        assert!(!retryable_sync_flush_error(
            &DurabilityError::SnapshotVerification("corrupt QEFX evidence".into())
        ));
    }

    #[test]
    fn blocking_operation_offloads_on_current_thread_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let caller = std::thread::current().id();
            let worker = run_read_operation(ReadConsistency::Local, || std::thread::current().id())
                .await
                .unwrap();

            assert_ne!(worker, caller);
        });
    }

    #[test]
    fn blocking_operation_runs_inline_on_multi_thread_runtime() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        runtime.block_on(async {
            let caller = std::thread::current().id();
            let worker = run_read_operation(ReadConsistency::AppliedIndex(1), || {
                std::thread::current().id()
            })
            .await
            .unwrap();

            assert_eq!(worker, caller);
        });
    }

    #[test]
    fn read_barrier_offloads_on_multi_thread_runtime() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        runtime.block_on(async {
            let caller = std::thread::current().id();
            let worker =
                run_read_operation(ReadConsistency::ReadBarrier, || std::thread::current().id())
                    .await
                    .unwrap();

            assert_ne!(worker, caller);
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_service_adaptive_sql_read_returns_point_and_query_results() {
        let (_dir, runtime) = sql_test_runtime();
        let service = NodeService::new(Arc::new(runtime), None);

        let point = service
            .read("missing", ReadConsistency::Local)
            .await
            .unwrap();
        let query = service
            .query(
                SqlStatement {
                    sql: "SELECT ?1 AS value".into(),
                    parameters: vec![SqlValue::Integer(7)],
                },
                ReadConsistency::AppliedIndex(0),
                1,
            )
            .await
            .unwrap();

        assert_eq!(point.value, None);
        assert_eq!(query.columns, vec!["value"]);
        assert_eq!(query.rows, vec![vec![SqlValue::Integer(7)]]);
    }

    #[test]
    fn node_service_adaptive_sql_read_stays_inline_and_recovers_direct_panic() {
        let (_dir, runtime) = sql_test_runtime();
        let service = NodeService::new(Arc::new(runtime), None);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        runtime.block_on(async {
            let caller = std::thread::current().id();
            let worker = service
                .run_sql_read_operation(ReadConsistency::Local, || std::thread::current().id())
                .await
                .unwrap();

            assert_eq!(worker, caller);
            assert_eq!(
                service
                    .sql_reads_in_flight
                    .load(std::sync::atomic::Ordering::Acquire),
                0
            );
        });

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(
                service.run_sql_read_operation(ReadConsistency::AppliedIndex(0), || -> () {
                    panic!("inline SQL read panic")
                }),
            )
        }));
        assert!(panic.is_err());
        assert_eq!(
            service
                .sql_reads_in_flight
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_service_adaptive_sql_read_offloads_overlap_and_recovers_join_error() {
        let (_dir, runtime) = sql_test_runtime();
        let service = NodeService::new(Arc::new(runtime), None);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first_service = service.clone();
        let first = tokio::spawn(async move {
            first_service
                .run_sql_read_operation(ReadConsistency::Local, move || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .await
                .unwrap();
        });
        entered_rx.await.unwrap();
        assert_eq!(
            service
                .sql_reads_in_flight
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );

        let caller = std::thread::current().id();
        let worker = service
            .run_sql_read_operation(ReadConsistency::AppliedIndex(0), || {
                std::thread::current().id()
            })
            .await
            .unwrap();
        assert_ne!(worker, caller);
        assert_eq!(
            service
                .sql_reads_in_flight
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );

        let error = service
            .run_sql_read_operation(ReadConsistency::Local, || -> () {
                panic!("contended SQL read panic")
            })
            .await
            .unwrap_err();
        assert!(error.is_panic());
        assert_eq!(
            service
                .sql_reads_in_flight
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );

        release_tx.send(()).unwrap();
        first.await.unwrap();
        assert_eq!(
            service
                .sql_reads_in_flight
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
    }

    #[test]
    fn node_service_adaptive_sql_read_offloads_on_current_thread() {
        let (_dir, node) = sql_test_runtime();
        let service = NodeService::new(Arc::new(node), None);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let caller = std::thread::current().id();
            let worker = service
                .run_sql_read_operation(ReadConsistency::Local, || std::thread::current().id())
                .await
                .unwrap();

            assert_ne!(worker, caller);
            assert_eq!(
                service
                    .sql_reads_in_flight
                    .load(std::sync::atomic::Ordering::Acquire),
                0
            );
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_service_read_barrier_offloads_without_counting_fast_reads() {
        let (_dir, runtime) = sql_test_runtime();
        let service = NodeService::new(Arc::new(runtime), None);
        let caller = std::thread::current().id();
        let worker = service
            .run_sql_read_operation(ReadConsistency::ReadBarrier, || std::thread::current().id())
            .await
            .unwrap();

        assert_ne!(worker, caller);
        assert_eq!(
            service
                .sql_reads_in_flight
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
    }

    #[test]
    fn embedded_sql_query_keeps_raw_budget_without_http_json_budget() {
        let (_dir, runtime) = sql_test_runtime();
        let response = runtime
            .query_sql(
                &SqlStatement {
                    sql: "SELECT replace(hex(zeroblob(700000)), '00', char(1)) AS value".into(),
                    parameters: Vec::new(),
                },
                ReadConsistency::Local,
                1,
            )
            .unwrap();

        assert!(serde_json::to_vec(&response).unwrap().len() > MAX_SQL_RESPONSE_BYTES);
    }

    #[test]
    fn sql_http_response_rejects_encoded_body_over_limit() {
        let response = SqlQueryResponse {
            columns: vec!["value".into()],
            rows: vec![vec![SqlValue::Text("\u{1}".repeat(700_000))]],
            applied_index: 0,
            hash: LogHash::ZERO,
        };

        assert_eq!(
            sql_query_http_response(response).status(),
            axum::http::StatusCode::BAD_REQUEST
        );
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_response_work_holds_client_capacity_until_completion() {
        let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let permit = std::sync::Arc::new(slots.clone().try_acquire_owned().unwrap());

        let capacity_exhausted_during_response =
            with_graph_client_permit(permit, || slots.clone().try_acquire_owned().is_err());

        assert!(capacity_exhausted_during_response);
        assert!(slots.try_acquire().is_ok());
    }

    #[test]
    fn peer_permit_body_holds_capacity_through_frames_until_eof_or_drop() {
        use http_body::Body as _;

        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::new(slots.clone().try_acquire_owned().unwrap());
        let response = Response::new(Body::new(PendingThenDataBody { state: 0 }));
        let mut body =
            retain_peer_permit_until_response_body_complete(response, permit).into_body();
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);

        assert!(slots.try_acquire().is_err());
        assert!(matches!(
            std::pin::Pin::new(&mut body).poll_frame(&mut context),
            std::task::Poll::Pending
        ));
        assert!(slots.try_acquire().is_err());
        assert!(matches!(
            std::pin::Pin::new(&mut body).poll_frame(&mut context),
            std::task::Poll::Ready(Some(Ok(_)))
        ));
        assert!(slots.try_acquire().is_err());
        assert!(matches!(
            std::pin::Pin::new(&mut body).poll_frame(&mut context),
            std::task::Poll::Ready(None)
        ));
        assert!(slots.try_acquire().is_ok());

        let dropped_permit = Arc::new(slots.clone().try_acquire_owned().unwrap());
        let dropped_response = Response::new(Body::new(PendingThenDataBody { state: 0 }));
        let dropped_body =
            retain_peer_permit_until_response_body_complete(dropped_response, dropped_permit);
        assert!(slots.try_acquire().is_err());
        drop(dropped_body);
        assert!(slots.try_acquire().is_ok());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_client_query_error_returns_400_without_latching_readiness() {
        let (_dir, runtime) = graph_test_runtime();

        let error = runtime.map_graph_read_error(rhiza_graph::Error::InvalidCommand(
            "unknown property".into(),
        ));
        let response = node_error_response(error);

        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        assert!(runtime.is_ready());
        assert!(!runtime.is_fatal());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_resource_exhaustion_returns_503_without_latching_readiness() {
        let (_dir, runtime) = graph_test_runtime();

        let error = runtime.map_graph_read_error(rhiza_graph::Error::ResourceExhausted(
            "buffer pool is full".into(),
        ));
        let response = node_error_response(error);

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(runtime.is_ready());
        assert!(!runtime.is_fatal());
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_resource_exhaustion_returns_503_without_latching_readiness() {
        let (_dir, runtime) = kv_test_runtime();

        let error = runtime.map_kv_read_error(rhiza_kv::Error::ResourceExhausted(
            "scan result is too large".into(),
        ));
        let response = node_error_response(error);

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(runtime.is_ready());
        assert!(!runtime.is_fatal());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_batch_coalesces_exact_retry_and_isolates_conflicting_duplicate() {
        let (_dir, runtime) = graph_test_runtime();
        let canonical =
            GraphCommandV1::put_document("same", "document", GraphValueV1::String("first".into()))
                .unwrap();
        let conflict = GraphCommandV1::put_document(
            "same",
            "document",
            GraphValueV1::String("conflict".into()),
        )
        .unwrap();
        let unrelated =
            GraphCommandV1::put_document("other", "other", GraphValueV1::U64(2)).unwrap();
        let results = runtime
            .mutate_graph_batch(vec![canonical.clone(), canonical, conflict, unrelated])
            .unwrap();

        let canonical = results[0].as_ref().unwrap().applied_index();
        assert_eq!(results[1].as_ref().unwrap().applied_index(), canonical);
        assert!(matches!(
            results[2],
            Err(super::NodeError::InvalidRequest(_))
        ));
        assert_eq!(results[3].as_ref().unwrap().applied_index(), canonical);
        assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));
        assert!(runtime.is_ready());
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_batch_coalesces_exact_retry_and_isolates_conflicting_duplicate() {
        let (_dir, runtime) = kv_test_runtime();
        let canonical = KvCommandV1::put("same", b"key".to_vec(), b"first".to_vec()).unwrap();
        let conflict = KvCommandV1::put("same", b"key".to_vec(), b"conflict".to_vec()).unwrap();
        let unrelated = KvCommandV1::put("other", b"other".to_vec(), b"second".to_vec()).unwrap();
        let results = runtime
            .mutate_kv_batch(vec![canonical.clone(), canonical, conflict, unrelated])
            .unwrap();

        let canonical = results[0].as_ref().unwrap().applied_index();
        assert_eq!(results[1].as_ref().unwrap().applied_index(), canonical);
        assert!(matches!(
            results[2],
            Err(super::NodeError::InvalidRequest(_))
        ));
        assert_eq!(results[3].as_ref().unwrap().applied_index(), canonical);
        assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));
        assert!(runtime.is_ready());
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_group_commit_coalesces_four_waiting_64_member_calls_into_one_qlog() {
        let (_dir, runtime) = kv_test_runtime();
        let runtime = Arc::new(runtime);
        let commit = runtime.lock_commit().unwrap();
        let start = Arc::new(Barrier::new(5));
        let workers = (0..4)
            .map(|call| {
                let runtime = Arc::clone(&runtime);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    let commands = (0..64)
                        .map(|member| {
                            let id = call * 64 + member;
                            KvCommandV1::put(
                                format!("kv-group-{id}"),
                                format!("key-{id}").into_bytes(),
                                vec![u8::try_from(call).unwrap(); 128],
                            )
                            .unwrap()
                        })
                        .collect();
                    start.wait();
                    runtime.mutate_kv_batch(commands)
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        runtime
            .kv_group_commit
            .wait_for_pending_calls(4, Duration::from_secs(5));
        drop(commit);

        let responses = workers
            .into_iter()
            .map(|worker| worker.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        let anchors = responses
            .iter()
            .flatten()
            .map(|result| {
                let outcome = result.as_ref().unwrap();
                (outcome.applied_index(), outcome.hash())
            })
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(anchors.len(), 1);
        assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_group_commit_rejects_public_257_member_call_before_writing() {
        let (_dir, runtime) = kv_test_runtime();
        let commands = (0..257)
            .map(|id| {
                KvCommandV1::put(
                    format!("kv-over-{id}"),
                    format!("key-{id}").into_bytes(),
                    b"value".to_vec(),
                )
                .unwrap()
            })
            .collect();

        let error = runtime.mutate_kv_batch(commands).unwrap_err();

        assert!(matches!(error, NodeError::InvalidRequest(_)));
        assert_eq!(runtime.log_store().last_index().unwrap(), None);
        assert!(runtime
            .kv_group_commit
            .state
            .lock()
            .unwrap()
            .pending
            .is_empty());
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_group_commit_lone_call_completes_and_leaves_queue_idle() {
        let (_dir, runtime) = kv_test_runtime();

        let outcome = runtime
            .mutate_kv(KvCommandV1::put("kv-lone", b"key".to_vec(), b"value".to_vec()).unwrap())
            .unwrap();

        assert_eq!(outcome.applied_index(), 1);
        let state = runtime
            .kv_group_commit
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.pending.is_empty());
        assert_eq!(state.pending_encoded_bytes, 0);
        assert!(!state.leader_active);
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_group_commit_shutdown_wakes_waiters_without_writing() {
        let (_dir, runtime) = kv_test_runtime();
        let runtime = Arc::new(runtime);
        let commit = runtime.lock_commit().unwrap();
        let start = Arc::new(Barrier::new(3));
        let workers = (0..2)
            .map(|id| {
                let runtime = Arc::clone(&runtime);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    runtime.mutate_kv(
                        KvCommandV1::put(
                            format!("kv-shutdown-{id}"),
                            format!("key-{id}").into_bytes(),
                            b"value".to_vec(),
                        )
                        .unwrap(),
                    )
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        runtime
            .kv_group_commit
            .wait_for_pending_calls(2, Duration::from_secs(5));

        runtime.cancel_operations();
        drop(commit);

        for worker in workers {
            assert!(matches!(
                worker.join().unwrap(),
                Err(NodeError::Unavailable(_))
            ));
        }
        assert_eq!(runtime.log_store().last_index().unwrap(), None);
        let state = runtime
            .kv_group_commit
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.pending.is_empty());
        assert_eq!(state.pending_encoded_bytes, 0);
        assert!(!state.leader_active);
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_group_commit_preserves_cross_call_retry_conflict_and_new_result_offsets() {
        let (_dir, runtime) = kv_test_runtime();
        let runtime = Arc::new(runtime);
        let stored = KvCommandV1::put(
            "kv-stored",
            b"stored-key".to_vec(),
            b"stored-value".to_vec(),
        )
        .unwrap();
        let stored_outcome = runtime.mutate_kv(stored.clone()).unwrap();
        let conflict =
            KvCommandV1::put("kv-stored", b"stored-key".to_vec(), b"conflict".to_vec()).unwrap();
        let commit = runtime.lock_commit().unwrap();
        let start = Arc::new(Barrier::new(3));
        let retry_worker = {
            let runtime = Arc::clone(&runtime);
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                runtime.mutate_kv_batch(vec![stored, conflict])
            })
        };
        let new_worker = {
            let runtime = Arc::clone(&runtime);
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                runtime.mutate_kv_batch(vec![
                    KvCommandV1::put("kv-new-1", b"new-1".to_vec(), b"one".to_vec()).unwrap(),
                    KvCommandV1::put("kv-new-2", b"new-2".to_vec(), b"two".to_vec()).unwrap(),
                ])
            })
        };
        start.wait();
        runtime
            .kv_group_commit
            .wait_for_pending_calls(2, Duration::from_secs(5));
        drop(commit);

        let retry_results = retry_worker.join().unwrap().unwrap();
        let new_results = new_worker.join().unwrap().unwrap();

        assert_eq!(
            retry_results[0].as_ref().unwrap().applied_index(),
            stored_outcome.applied_index()
        );
        assert!(matches!(
            retry_results[1],
            Err(NodeError::InvalidRequest(_))
        ));
        let new_anchors = new_results
            .iter()
            .map(|result| {
                let outcome = result.as_ref().unwrap();
                (outcome.applied_index(), outcome.hash())
            })
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(new_anchors.len(), 1);
        assert_eq!(runtime.log_store().last_index().unwrap(), Some(2));
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_group_commit_releases_pending_byte_budget_after_drain_and_failure() {
        let queue = super::KvGroupCommitQueue::new();
        let cancelled = AtomicBool::new(false);
        let member = |id: usize, bytes: usize| super::RuntimeBatchMember {
            #[cfg(feature = "sql")]
            request_id: format!("kv-byte-{id}"),
            payload: vec![u8::try_from(id).unwrap_or_default(); bytes],
            operation: super::QueuedOperation::Kv(
                KvCommandV1::put(
                    format!("kv-byte-{id}"),
                    format!("key-{id}").into_bytes(),
                    b"value".to_vec(),
                )
                .unwrap(),
            ),
        };
        for id in 0..63 {
            queue
                .enqueue(vec![member(id, MAX_COMMAND_BYTES)], &cancelled)
                .unwrap();
        }

        let overflow = match queue.enqueue(vec![member(63, MAX_COMMAND_BYTES * 2)], &cancelled) {
            Ok(_) => panic!("pending KV byte budget must reject oversized aggregate work"),
            Err(error) => error,
        };
        assert!(matches!(overflow, NodeError::ResourceExhausted(_)));

        let drained = queue.drain_next_group().unwrap();
        assert_eq!(drained.len(), 63);
        let released = queue
            .enqueue(vec![member(64, MAX_COMMAND_BYTES)], &cancelled)
            .unwrap()
            .0;
        queue.fail_pending(NodeError::Unavailable("test failure".into()));
        assert!(matches!(
            released.wait(&cancelled),
            Err(NodeError::Unavailable(_))
        ));
        let state = queue
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.pending.is_empty());
        assert_eq!(state.pending_encoded_bytes, 0);
        assert!(!state.leader_active);
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_group_commit_window_restarts_after_staggered_enqueue() {
        let queue = Arc::new(super::KvGroupCommitQueue::new());
        let cancelled = AtomicBool::new(false);
        let member = |id: usize| super::RuntimeBatchMember {
            #[cfg(feature = "sql")]
            request_id: format!("kv-debounce-{id}"),
            payload: vec![u8::try_from(id).unwrap_or_default()],
            operation: super::QueuedOperation::Kv(
                KvCommandV1::put(
                    format!("kv-debounce-{id}"),
                    format!("key-{id}").into_bytes(),
                    b"value".to_vec(),
                )
                .unwrap(),
            ),
        };
        queue.enqueue(vec![member(1)], &cancelled).unwrap();
        let collector = Arc::clone(&queue);
        let (finished, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let collected = collector.collect_until_full_or_timeout(Duration::from_millis(100));
            finished.send(collected).unwrap();
        });

        std::thread::sleep(Duration::from_millis(75));
        queue.enqueue(vec![member(2)], &cancelled).unwrap();
        std::thread::sleep(Duration::from_millis(75));
        queue.enqueue(vec![member(3)], &cancelled).unwrap();
        assert!(receive.recv_timeout(Duration::from_millis(50)).is_err());
        assert!(receive.recv_timeout(Duration::from_millis(150)).unwrap());
        worker.join().unwrap();
        assert_eq!(queue.drain_next_group().unwrap().len(), 3);
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_group_commit_returns_committed_group_when_cancelled_after_execution() {
        let (_dir, mut runtime) = kv_test_runtime();
        runtime.kv_group_commit_after_execute_hook = Some(Arc::new(NodeRuntime::cancel_operations));
        let runtime = Arc::new(runtime);
        let commit = runtime.lock_commit().unwrap();
        let start = Arc::new(Barrier::new(3));
        let workers = (0..2)
            .map(|call| {
                let runtime = Arc::clone(&runtime);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    let commands = (0..64)
                        .map(|member| {
                            let id = call * 64 + member;
                            KvCommandV1::put(
                                format!("kv-cancel-after-{id}"),
                                format!("key-{id}").into_bytes(),
                                b"value".to_vec(),
                            )
                            .unwrap()
                        })
                        .collect();
                    start.wait();
                    runtime.mutate_kv_batch(commands)
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        runtime
            .kv_group_commit
            .wait_for_pending_calls(2, Duration::from_secs(5));
        drop(commit);

        let results = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(results.len(), 128);
        let anchors = results
            .iter()
            .map(|result| {
                let outcome = result.as_ref().unwrap();
                (outcome.applied_index(), outcome.hash())
            })
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(anchors.len(), 1);
        assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));
        assert!(runtime.operation_cancelled.load(Ordering::Acquire));
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_largest_fitting_prefix_is_exact_for_large_grouped_batch() {
        let commands = (0..256)
            .map(|id| {
                KvCommandV1::put(
                    format!("kv-prefix-{id:04}"),
                    format!("key-{id:04}").into_bytes(),
                    vec![b'x'; 4 * 1024],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(super::encode_replicated_kv_batch(&commands).unwrap().len() > MAX_COMMAND_BYTES);
        let expected = (2..commands.len())
            .filter(|count| {
                super::encode_replicated_kv_batch(&commands[..*count])
                    .unwrap()
                    .len()
                    <= MAX_COMMAND_BYTES
            })
            .max()
            .unwrap();

        let (count, payload) = super::largest_fitting_kv_batch_prefix(&commands).unwrap();

        assert_eq!(count, expected);
        assert!(payload.len() <= MAX_COMMAND_BYTES);
        assert!(
            super::encode_replicated_kv_batch(&commands[..count + 1])
                .unwrap()
                .len()
                > MAX_COMMAND_BYTES
        );
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_largest_fitting_prefix_is_exact_for_large_grouped_batch() {
        let commands = (0..64)
            .map(|id| {
                GraphCommandV1::put_document(
                    format!("graph-prefix-{id:04}"),
                    format!("document-{id:04}"),
                    GraphValueV1::String("x".repeat(16 * 1024)),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(
            super::encode_replicated_graph_batch(&commands)
                .unwrap()
                .len()
                > MAX_COMMAND_BYTES
        );
        let expected = (2..commands.len())
            .rev()
            .find(|count| {
                super::encode_replicated_graph_batch(&commands[..*count])
                    .unwrap()
                    .len()
                    <= MAX_COMMAND_BYTES
            })
            .unwrap();

        let (count, payload) = super::largest_fitting_graph_batch_prefix(&commands).unwrap();

        assert_eq!(count, expected);
        assert_eq!(
            payload,
            super::encode_replicated_graph_batch(&commands[..count]).unwrap()
        );
        assert!(payload.len() <= MAX_COMMAND_BYTES);
        assert!(
            super::encode_replicated_graph_batch(&commands[..count + 1])
                .unwrap()
                .len()
                > MAX_COMMAND_BYTES
        );
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_largest_fitting_prefix_returns_none_when_no_multi_command_prefix_fits() {
        let commands = (0..3)
            .map(|id| {
                GraphCommandV1::put_document(
                    format!("graph-single-{id}"),
                    format!("document-{id}"),
                    GraphValueV1::String("x".repeat(256 * 1024)),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        assert!(
            super::encode_replicated_graph_batch(&commands)
                .unwrap()
                .len()
                > MAX_COMMAND_BYTES
        );
        assert_eq!(super::largest_fitting_graph_batch_prefix(&commands), None);
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_group_commit_large_batch_uses_largest_fitting_fifo_sub_batches() {
        let (_dir, runtime) = kv_test_runtime();
        let runtime = Arc::new(runtime);
        let calls = (0..4)
            .map(|call| {
                (0..64)
                    .map(|member| {
                        let id = call * 64 + member;
                        KvCommandV1::put(
                            format!("kv-large-{id:04}"),
                            format!("key-{id:04}").into_bytes(),
                            vec![b'x'; 4 * 1024],
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let flattened = calls.iter().flatten().cloned().collect::<Vec<_>>();
        let (largest_prefix, _) = super::largest_fitting_kv_batch_prefix(&flattened).unwrap();
        let expected_entries = flattened.len().div_ceil(largest_prefix);
        let commit = runtime.lock_commit().unwrap();
        let start = Arc::new(Barrier::new(5));
        let workers = calls
            .into_iter()
            .map(|commands| {
                let runtime = Arc::clone(&runtime);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    runtime.mutate_kv_batch(commands)
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        runtime
            .kv_group_commit
            .wait_for_pending_calls(4, Duration::from_secs(5));
        drop(commit);

        let mut counts = std::collections::BTreeMap::new();
        for result in workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap().unwrap())
        {
            *counts
                .entry(result.unwrap().applied_index())
                .or_insert(0_usize) += 1;
        }

        assert_eq!(counts.len(), expected_entries);
        let counts = counts.into_values().collect::<Vec<_>>();
        assert!(counts[..counts.len() - 1]
            .iter()
            .all(|count| *count == largest_prefix));
        assert_eq!(counts.iter().sum::<usize>(), 256);
        assert_eq!(
            runtime.log_store().last_index().unwrap(),
            Some(u64::try_from(expected_entries).unwrap())
        );
        for index in 1..=u64::try_from(expected_entries).unwrap() {
            assert!(runtime
                .log_store()
                .read(index)
                .unwrap()
                .is_some_and(|entry| entry.payload.len() <= MAX_COMMAND_BYTES));
        }
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_group_commit_leader_panic_wakes_active_and_pending_calls_with_same_fatal() {
        let (_dir, mut runtime) = kv_test_runtime();
        runtime.kv_group_commit_before_execute_hook =
            Some(Arc::new(|| panic!("injected KV group leader panic")));
        let runtime = Arc::new(runtime);
        let commit = runtime.lock_commit().unwrap();
        let mut workers = Vec::new();
        for call in 0..17 {
            let worker_runtime = Arc::clone(&runtime);
            workers.push(std::thread::spawn(move || {
                worker_runtime.mutate_kv_batch(
                    (0..64)
                        .map(|member| {
                            let id = call * 64 + member;
                            KvCommandV1::put(
                                format!("kv-panic-{id}"),
                                format!("key-{id}").into_bytes(),
                                b"value".to_vec(),
                            )
                            .unwrap()
                        })
                        .collect(),
                )
            }));
            runtime
                .kv_group_commit
                .wait_for_pending_calls(call + 1, Duration::from_secs(5));
        }
        drop(commit);

        let errors = workers
            .into_iter()
            .map(|worker| worker.join().unwrap().unwrap_err())
            .collect::<Vec<_>>();
        assert!(errors
            .iter()
            .all(|error| matches!(error, NodeError::Fatal(_))));
        assert_eq!(errors[0].to_string(), errors[1].to_string());
        assert!(runtime.is_fatal());
        assert_eq!(runtime.log_store().last_index().unwrap(), None);
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_group_commit_reopens_all_four_grouped_calls_at_shared_durable_anchor() {
        let (dir, runtime) = kv_test_runtime();
        let runtime = Arc::new(runtime);
        let commit = runtime.lock_commit().unwrap();
        let start = Arc::new(Barrier::new(5));
        let workers = (0..4)
            .map(|call| {
                let runtime = Arc::clone(&runtime);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    let commands = (0..64)
                        .map(|member| {
                            let id = call * 64 + member;
                            KvCommandV1::put(
                                format!("kv-reopen-{id}"),
                                format!("key-{id:04}").into_bytes(),
                                vec![u8::try_from(call).unwrap(); 128],
                            )
                            .unwrap()
                        })
                        .collect();
                    start.wait();
                    runtime.mutate_kv_batch(commands)
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        runtime
            .kv_group_commit
            .wait_for_pending_calls(4, Duration::from_secs(5));
        drop(commit);

        let results = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        let anchors = results
            .iter()
            .map(|result| {
                let outcome = result.as_ref().unwrap();
                (outcome.applied_index(), outcome.hash())
            })
            .collect::<std::collections::HashSet<_>>();
        let [(applied_index, applied_hash)] = anchors.into_iter().collect::<Vec<_>>()[..] else {
            panic!("four grouped calls must share one durable anchor");
        };
        assert_eq!(
            runtime.log_store().last_index().unwrap(),
            Some(applied_index)
        );
        let config = runtime.config().clone();
        drop(runtime);

        let consensus = Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                config.cluster_id().to_owned(),
                config.node_id().to_owned(),
                config.epoch(),
                config.config_id(),
                [
                    dir.path().join("recorders/n1"),
                    dir.path().join("recorders/n2"),
                    dir.path().join("recorders/n3"),
                ],
                applied_index + 1,
                applied_hash,
            )
            .unwrap(),
        );
        let reopened = NodeRuntime::open(config, consensus, &[]).unwrap();

        assert_eq!(reopened.applied_index().unwrap(), applied_index);
        assert_eq!(reopened.applied_hash().unwrap(), applied_hash);
        assert_eq!(reopened.log_store().last_index().unwrap(), Some(1));
        for id in 0..256 {
            let response = reopened
                .get_kv(format!("key-{id:04}").as_bytes(), ReadConsistency::Local)
                .unwrap();
            assert_eq!(
                response.value,
                Some(vec![u8::try_from(id / 64).unwrap(); 128])
            );
            assert_eq!(response.applied_index, applied_index);
            assert_eq!(response.hash, applied_hash);
        }
    }

    #[test]
    fn sql_batch_preflight_rejects_entire_vector_without_growing_log() {
        let (_dir, runtime) = sql_test_runtime();
        let valid = SqlCommand {
            request_id: "valid".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE batch_items(id INTEGER PRIMARY KEY)".into(),
                parameters: vec![],
            }],
        };
        let invalid = SqlCommand {
            request_id: String::new(),
            statements: valid.statements.clone(),
        };

        let error = runtime.execute_sql_batch(vec![valid, invalid]).unwrap_err();

        assert!(matches!(error, NodeError::InvalidRequest(_)));
        assert_eq!(runtime.log_store().last_index().unwrap(), None);
    }

    #[test]
    fn sql_batch_rejects_aggregate_encoded_input_over_command_cap_before_io() {
        let (_dir, runtime) = sql_test_runtime();
        let command = |request_id: &str, fill: char| SqlCommand {
            request_id: request_id.into(),
            statements: vec![SqlStatement {
                sql: "SELECT ?1".into(),
                parameters: vec![SqlValue::Text(
                    std::iter::repeat_n(fill, MAX_COMMAND_BYTES / 2).collect(),
                )],
            }],
        };

        let error = runtime
            .execute_sql_batch(vec![
                command("aggregate-a", 'a'),
                command("aggregate-b", 'b'),
            ])
            .unwrap_err();

        assert!(matches!(error, NodeError::ResourceExhausted(_)));
        assert_eq!(runtime.log_store().last_index().unwrap(), None);
        assert!(runtime
            .sql_group_commit
            .state
            .lock()
            .unwrap()
            .pending
            .is_empty());
    }

    #[test]
    fn sql_write_profiling_records_nothing_when_observer_is_not_installed() {
        let profiler = SqlWriteProfiler::new(8);
        let (_dir, runtime) = sql_test_runtime();

        runtime
            .execute_sql(SqlCommand {
                request_id: "schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE profiled_items(id INTEGER PRIMARY KEY)".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();

        assert!(runtime.config().sql_write_profiler().is_none());
        assert!(profiler.snapshot().samples.is_empty());
    }

    #[test]
    fn sql_write_profiling_records_one_consistent_sample_for_one_physical_batch() {
        let profiler = SqlWriteProfiler::new(8);
        let (_dir, runtime) = sql_test_runtime_with_profiler(Some(profiler.clone()));
        runtime
            .execute_sql(SqlCommand {
                request_id: "schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE profiled_items(id INTEGER PRIMARY KEY)".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        profiler.drain();

        let commands = (1..=3)
            .map(|id| SqlCommand {
                request_id: format!("insert-{id}"),
                statements: vec![SqlStatement {
                    sql: "INSERT INTO profiled_items(id) VALUES (?1)".into(),
                    parameters: vec![SqlValue::Integer(id)],
                }],
            })
            .collect();
        let responses = runtime.execute_sql_batch(commands).unwrap();

        let snapshot = profiler.snapshot();
        assert_eq!(snapshot.dropped_samples, 0);
        let [sample] = snapshot.samples.as_slice() else {
            panic!("one physical SQL batch must emit one sample: {snapshot:?}");
        };
        assert_eq!(sample.batch_member_count, 3);
        assert_eq!(
            sample.total_service_us,
            sample
                .commit_lock_wait_us
                .saturating_add(sample.precheck_classification_us)
                .saturating_add(sample.qwal_prepare_us)
                .saturating_add(sample.consensus_propose_us)
                .saturating_add(sample.local_qlog_mirror_append_us)
                .saturating_add(sample.sql_materializer_apply_us)
                .saturating_add(sample.response_other_total_us)
        );
        let (applied_index, applied_hash) = runtime.ensure_materialized_tip().unwrap();
        assert!(responses.iter().all(|response| {
            response.as_ref().is_ok_and(|response| {
                response.applied_index == applied_index && response.hash == applied_hash
            })
        }));
    }

    #[test]
    fn sql_write_profiling_does_not_fabricate_sample_for_failed_batch() {
        let profiler = SqlWriteProfiler::new(8);
        let (_dir, runtime) = sql_test_runtime_with_profiler(Some(profiler.clone()));
        runtime
            .execute_sql(SqlCommand {
                request_id: "schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE profiled_items(id INTEGER PRIMARY KEY)".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        runtime
            .execute_sql(SqlCommand {
                request_id: "first".into(),
                statements: vec![SqlStatement {
                    sql: "INSERT INTO profiled_items(id) VALUES (1)".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        profiler.drain();
        let last_index = runtime.log_store().last_index().unwrap();

        let error = runtime
            .execute_sql(SqlCommand {
                request_id: "duplicate".into(),
                statements: vec![SqlStatement {
                    sql: "INSERT INTO profiled_items(id) VALUES (1)".into(),
                    parameters: vec![],
                }],
            })
            .unwrap_err();

        assert!(matches!(error, NodeError::InvalidSqlStatement { .. }));
        assert!(profiler.snapshot().samples.is_empty());
        assert_eq!(runtime.log_store().last_index().unwrap(), last_index);
    }

    #[test]
    fn sql_group_commit_coalesces_four_waiting_typed_calls_into_one_1024_receipt_qwal() {
        let profiler = SqlWriteProfiler::new(8);
        let (_dir, runtime) = sql_test_runtime_with_profiler(Some(profiler.clone()));
        let runtime = Arc::new(runtime);
        runtime
            .execute_sql(SqlCommand {
                request_id: "group-schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE grouped_items(id INTEGER PRIMARY KEY)".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        profiler.drain();

        let commit = runtime.lock_commit().unwrap();
        let start = Arc::new(Barrier::new(5));
        let workers = (0..4)
            .map(|call| {
                let runtime = Arc::clone(&runtime);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    let commands = (0..256)
                        .map(|offset| {
                            let id = call * 256 + offset;
                            SqlCommand {
                                request_id: format!("group-{id}"),
                                statements: vec![SqlStatement {
                                    sql: "INSERT INTO grouped_items(id) VALUES (?1)".into(),
                                    parameters: vec![SqlValue::Integer(id)],
                                }],
                            }
                        })
                        .collect();
                    start.wait();
                    runtime.execute_sql_batch(commands).unwrap()
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        runtime
            .sql_group_commit
            .wait_for_pending_calls(4, Duration::from_secs(5));
        drop(commit);

        let results = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 1024);
        let first = results[0].as_ref().unwrap();
        assert!(results.iter().all(|result| {
            result.as_ref().is_ok_and(|result| {
                result.applied_index == first.applied_index && result.hash == first.hash
            })
        }));
        let entry = runtime
            .log_store()
            .read(first.applied_index)
            .unwrap()
            .unwrap();
        let command = ExternalEffectCommand::decode(&entry.payload).unwrap();
        assert_eq!(
            rhiza_sql::QwalEffectManifestV4::from_external_command(&command)
                .unwrap()
                .receipts
                .len(),
            1024
        );
        assert_eq!(runtime.log_store().last_index().unwrap(), Some(2));
        let snapshot = profiler.snapshot();
        let [sample] = snapshot.samples.as_slice() else {
            panic!("one grouped physical commit must emit one sample: {snapshot:?}");
        };
        assert_eq!(sample.batch_member_count, 1024);
    }

    #[test]
    fn sql_group_commit_keeps_fifth_whole_call_for_the_next_physical_group() {
        let (_dir, runtime) = sql_test_runtime();
        let runtime = Arc::new(runtime);
        runtime
            .execute_sql(SqlCommand {
                request_id: "next-group-schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE next_group_items(id INTEGER PRIMARY KEY)".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();

        let commit = runtime.lock_commit().unwrap();
        let mut workers = Vec::new();
        for call in 0..5 {
            let worker_runtime = Arc::clone(&runtime);
            workers.push(std::thread::spawn(move || {
                worker_runtime
                    .execute_sql_batch(
                        (0..256)
                            .map(|offset| {
                                let id = call * 256 + offset;
                                SqlCommand {
                                    request_id: format!("next-group-{id}"),
                                    statements: vec![SqlStatement {
                                        sql: "INSERT INTO next_group_items(id) VALUES (?1)".into(),
                                        parameters: vec![SqlValue::Integer(id)],
                                    }],
                                }
                            })
                            .collect(),
                    )
                    .unwrap()
            }));
            runtime
                .sql_group_commit
                .wait_for_pending_calls(call as usize + 1, Duration::from_secs(5));
        }
        drop(commit);

        let calls = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        for call in &calls[..4] {
            assert!(call
                .iter()
                .all(|result| result.as_ref().unwrap().applied_index == 2));
        }
        assert!(calls[4]
            .iter()
            .all(|result| result.as_ref().unwrap().applied_index == 3));
        let first = runtime.log_store().read(2).unwrap().unwrap();
        let first = ExternalEffectCommand::decode(&first.payload).unwrap();
        assert_eq!(
            rhiza_sql::QwalEffectManifestV4::from_external_command(&first)
                .unwrap()
                .receipts
                .len(),
            1024
        );
        let second = runtime.log_store().read(3).unwrap().unwrap();
        let second = ExternalEffectCommand::decode(&second.payload).unwrap();
        assert_eq!(
            rhiza_sql::QwalEffectManifestV4::from_external_command(&second)
                .unwrap()
                .receipts
                .len(),
            256
        );
    }

    #[test]
    fn sql_group_commit_preserves_fifo_call_offsets_for_retries_conflicts_aliases_and_failures() {
        let (_dir, runtime) = sql_test_runtime();
        let runtime = Arc::new(runtime);
        runtime
            .execute_sql(SqlCommand {
                request_id: "fifo-schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE fifo_items(id INTEGER PRIMARY KEY, value TEXT UNIQUE)"
                        .into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        let stored_command = SqlCommand {
            request_id: "fifo-stored".into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO fifo_items(id, value) VALUES (1, 'stored')".into(),
                parameters: vec![],
            }],
        };
        let stored = runtime.execute_sql(stored_command.clone()).unwrap();
        let valid_alias = SqlCommand {
            request_id: "fifo-alias".into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO fifo_items(id, value) VALUES (2, 'alias')".into(),
                parameters: vec![],
            }],
        };
        let conflict = SqlCommand {
            request_id: stored_command.request_id.clone(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO fifo_items(id, value) VALUES (3, 'conflict')".into(),
                parameters: vec![],
            }],
        };
        let failed = SqlCommand {
            request_id: "fifo-failed".into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO fifo_items(id, value) VALUES (4, 'stored')".into(),
                parameters: vec![],
            }],
        };
        let valid = SqlCommand {
            request_id: "fifo-valid".into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO fifo_items(id, value) VALUES (5, 'valid')".into(),
                parameters: vec![],
            }],
        };

        let commit = runtime.lock_commit().unwrap();
        let first_runtime = Arc::clone(&runtime);
        let first = std::thread::spawn(move || {
            first_runtime
                .execute_sql_batch(vec![
                    stored_command,
                    conflict,
                    valid_alias.clone(),
                    valid_alias,
                ])
                .unwrap()
        });
        runtime
            .sql_group_commit
            .wait_for_pending_calls(1, Duration::from_secs(5));
        let second_runtime = Arc::clone(&runtime);
        let second = std::thread::spawn(move || {
            second_runtime
                .execute_sql_batch(vec![failed, valid])
                .unwrap()
        });
        runtime
            .sql_group_commit
            .wait_for_pending_calls(2, Duration::from_secs(5));
        drop(commit);

        let first = first.join().unwrap();
        let second = second.join().unwrap();
        assert_eq!(
            first[0].as_ref().unwrap().applied_index,
            stored.applied_index
        );
        assert!(matches!(first[1], Err(NodeError::RequestConflict(_))));
        assert_eq!(first[2], first[3]);
        assert_eq!(first[2].as_ref().unwrap().applied_index, 3);
        assert!(matches!(
            second[0],
            Err(NodeError::InvalidSqlStatement { .. })
        ));
        assert_eq!(second[1].as_ref().unwrap().applied_index, 3);
    }

    #[test]
    fn sql_group_commit_rejects_overload_before_enqueue_without_orphaning_the_leader() {
        let (_dir, runtime) = sql_test_runtime_configured(None, Some(1));
        let runtime = Arc::new(runtime);
        runtime
            .execute_sql(SqlCommand {
                request_id: "overload-schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE overload_items(id INTEGER PRIMARY KEY)".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        let command = |request_id: &str, id| SqlCommand {
            request_id: request_id.into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO overload_items(id) VALUES (?1)".into(),
                parameters: vec![SqlValue::Integer(id)],
            }],
        };

        let commit = runtime.lock_commit().unwrap();
        let leader_runtime = Arc::clone(&runtime);
        let leader = std::thread::spawn(move || {
            leader_runtime.execute_sql_batch(vec![command("overload-first", 1)])
        });
        runtime
            .sql_group_commit
            .wait_for_pending_calls(1, Duration::from_secs(5));
        let overload = runtime
            .execute_sql_batch(vec![command("overload-second", 2)])
            .unwrap_err();
        assert!(matches!(overload, NodeError::ResourceExhausted(_)));
        drop(commit);
        assert!(leader.join().unwrap().unwrap()[0].is_ok());
        assert_eq!(runtime.log_store().last_index().unwrap(), Some(2));
    }

    #[test]
    fn sql_group_commit_bounds_pending_bytes_and_releases_reservations() {
        let queue = super::SqlGroupCommitQueue::new(super::MAX_SQL_GROUP_COMMIT_QUEUE_CAPACITY);
        let cancelled = AtomicBool::new(false);
        let member = |id: usize| super::RuntimeBatchMember {
            request_id: format!("queued-{id}"),
            payload: vec![u8::try_from(id).unwrap_or_default(); MAX_COMMAND_BYTES],
            operation: super::QueuedOperation::Sql(SqlCommand {
                request_id: format!("queued-{id}"),
                statements: vec![SqlStatement {
                    sql: "SELECT 1".into(),
                    parameters: vec![],
                }],
            }),
        };
        let mut queued = Vec::new();
        for id in 0..super::DEFAULT_SQL_GROUP_COMMIT_QUEUE_CAPACITY {
            queued.push(queue.enqueue(vec![member(id)], &cancelled).unwrap().0);
        }

        let overflow = match queue.enqueue(
            vec![member(super::DEFAULT_SQL_GROUP_COMMIT_QUEUE_CAPACITY)],
            &cancelled,
        ) {
            Ok(_) => panic!("pending byte budget must reject one more full command"),
            Err(error) => error,
        };
        assert!(matches!(overflow, NodeError::ResourceExhausted(_)));

        let drained = queue.drain_next_group().unwrap();
        assert_eq!(drained.len(), 4);
        let released = queue
            .enqueue(
                vec![member(super::DEFAULT_SQL_GROUP_COMMIT_QUEUE_CAPACITY + 1)],
                &cancelled,
            )
            .unwrap()
            .0;

        queue.fail_pending(NodeError::Unavailable("test failure".into()));
        for job in queued.into_iter().skip(drained.len()) {
            assert!(matches!(
                job.wait(&cancelled),
                Err(NodeError::Unavailable(_))
            ));
        }
        assert!(matches!(
            released.wait(&cancelled),
            Err(NodeError::Unavailable(_))
        ));
        let state = queue
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.pending.is_empty());
        assert_eq!(state.pending_encoded_bytes, 0);
        assert!(!state.leader_active);
    }

    #[test]
    fn sql_group_commit_window_restarts_after_staggered_enqueue() {
        let queue = Arc::new(super::SqlGroupCommitQueue::new(
            super::DEFAULT_SQL_GROUP_COMMIT_QUEUE_CAPACITY,
        ));
        let cancelled = AtomicBool::new(false);
        let member = |id: usize| super::RuntimeBatchMember {
            request_id: format!("sql-debounce-{id}"),
            payload: vec![u8::try_from(id).unwrap_or_default()],
            operation: super::QueuedOperation::Sql(SqlCommand {
                request_id: format!("sql-debounce-{id}"),
                statements: vec![SqlStatement {
                    sql: "SELECT 1".into(),
                    parameters: vec![],
                }],
            }),
        };
        queue.enqueue(vec![member(1)], &cancelled).unwrap();
        let collector = Arc::clone(&queue);
        let (finished, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let collected = collector.collect_until_full_or_timeout(Duration::from_millis(100));
            finished.send(collected).unwrap();
        });

        std::thread::sleep(Duration::from_millis(75));
        queue.enqueue(vec![member(2)], &cancelled).unwrap();
        std::thread::sleep(Duration::from_millis(75));
        queue.enqueue(vec![member(3)], &cancelled).unwrap();
        assert!(receive.recv_timeout(Duration::from_millis(50)).is_err());
        assert!(receive.recv_timeout(Duration::from_millis(150)).unwrap());
        worker.join().unwrap();
        assert_eq!(queue.drain_next_group().unwrap().len(), 3);
    }

    #[test]
    fn sql_group_commit_leader_panic_wakes_every_queued_call_with_the_same_fatal_error() {
        let (_dir, mut runtime) = sql_test_runtime();
        runtime
            .execute_sql(SqlCommand {
                request_id: "panic-schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE panic_items(id INTEGER PRIMARY KEY)".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        runtime.sql_group_commit_before_execute_hook =
            Some(Arc::new(|| panic!("injected SQL group leader panic")));
        let runtime = Arc::new(runtime);
        let commit = runtime.lock_commit().unwrap();
        let mut workers = Vec::new();
        for id in 1..=2 {
            let worker_runtime = Arc::clone(&runtime);
            workers.push(std::thread::spawn(move || {
                worker_runtime.execute_sql_batch(vec![SqlCommand {
                    request_id: format!("panic-{id}"),
                    statements: vec![SqlStatement {
                        sql: "INSERT INTO panic_items(id) VALUES (?1)".into(),
                        parameters: vec![SqlValue::Integer(id)],
                    }],
                }])
            }));
            runtime
                .sql_group_commit
                .wait_for_pending_calls(id as usize, Duration::from_secs(5));
        }
        drop(commit);

        let errors = workers
            .into_iter()
            .map(|worker| worker.join().unwrap().unwrap_err())
            .collect::<Vec<_>>();
        assert!(errors
            .iter()
            .all(|error| matches!(error, NodeError::Fatal(_))));
        assert_eq!(errors[0].to_string(), errors[1].to_string());
        assert!(runtime.is_fatal());
    }

    #[test]
    fn sql_group_commit_shutdown_wakes_queued_calls_with_the_same_unavailable_error() {
        let (_dir, runtime) = sql_test_runtime();
        let runtime = Arc::new(runtime);
        runtime
            .execute_sql(SqlCommand {
                request_id: "shutdown-schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE shutdown_items(id INTEGER PRIMARY KEY)".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        let command = |id| SqlCommand {
            request_id: format!("shutdown-{id}"),
            statements: vec![SqlStatement {
                sql: "INSERT INTO shutdown_items(id) VALUES (?1)".into(),
                parameters: vec![SqlValue::Integer(id)],
            }],
        };

        let commit = runtime.lock_commit().unwrap();
        let leader_runtime = Arc::clone(&runtime);
        let leader = std::thread::spawn(move || leader_runtime.execute_sql_batch(vec![command(1)]));
        runtime
            .sql_group_commit
            .wait_for_pending_calls(1, Duration::from_secs(5));
        let follower_runtime = Arc::clone(&runtime);
        let follower =
            std::thread::spawn(move || follower_runtime.execute_sql_batch(vec![command(2)]));
        runtime
            .sql_group_commit
            .wait_for_pending_calls(2, Duration::from_secs(5));

        runtime.cancel_operations();
        let follower_error = follower.join().unwrap().unwrap_err();
        drop(commit);
        let leader_error = leader.join().unwrap().unwrap_err();

        assert!(matches!(leader_error, NodeError::Unavailable(_)));
        assert_eq!(leader_error.to_string(), follower_error.to_string());
        assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));
    }

    #[test]
    fn sql_group_commit_reprepares_combined_calls_after_a_foreign_slot_winner() {
        let (_dir, runtime) = sql_test_runtime();
        let runtime = Arc::new(runtime);
        let schema = runtime
            .execute_sql(SqlCommand {
                request_id: "group-winner-schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE group_winner(id INTEGER PRIMARY KEY)".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        let winner = runtime
            .consensus()
            .propose_at(
                rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                2,
                schema.hash,
                Command::new(CommandKind::ReadBarrier, Vec::new()),
            )
            .unwrap();

        let commit = runtime.lock_commit().unwrap();
        let mut workers = Vec::new();
        for id in 1..=2 {
            let worker_runtime = Arc::clone(&runtime);
            workers.push(std::thread::spawn(move || {
                worker_runtime
                    .execute_sql_batch(vec![SqlCommand {
                        request_id: format!("group-winner-{id}"),
                        statements: vec![SqlStatement {
                            sql: "INSERT INTO group_winner(id) VALUES (?1)".into(),
                            parameters: vec![SqlValue::Integer(id)],
                        }],
                    }])
                    .unwrap()
            }));
            runtime
                .sql_group_commit
                .wait_for_pending_calls(id as usize, Duration::from_secs(5));
        }
        drop(commit);

        let results = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(runtime.log_store().read(2).unwrap(), Some(winner));
        assert!(results
            .iter()
            .all(|result| result.as_ref().unwrap().applied_index == 3));
        assert_eq!(
            results[0].as_ref().unwrap().hash,
            results[1].as_ref().unwrap().hash
        );
    }

    #[test]
    fn sql_group_commit_all_failed_calls_return_aligned_without_consensus() {
        let (_dir, runtime) = sql_test_runtime();
        let runtime = Arc::new(runtime);
        runtime
            .execute_sql(SqlCommand {
                request_id: "all-failed-schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE all_failed(value TEXT UNIQUE)".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        runtime
            .execute_sql(SqlCommand {
                request_id: "all-failed-existing".into(),
                statements: vec![SqlStatement {
                    sql: "INSERT INTO all_failed(value) VALUES ('existing')".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();

        let commit = runtime.lock_commit().unwrap();
        let mut workers = Vec::new();
        for id in 1..=2 {
            let worker_runtime = Arc::clone(&runtime);
            workers.push(std::thread::spawn(move || {
                worker_runtime
                    .execute_sql_batch(vec![SqlCommand {
                        request_id: format!("all-failed-{id}"),
                        statements: vec![SqlStatement {
                            sql: "INSERT INTO all_failed(value) VALUES ('existing')".into(),
                            parameters: vec![],
                        }],
                    }])
                    .unwrap()
            }));
            runtime
                .sql_group_commit
                .wait_for_pending_calls(id as usize, Duration::from_secs(5));
        }
        drop(commit);

        for worker in workers {
            let results = worker.join().unwrap();
            assert_eq!(results.len(), 1);
            assert!(matches!(
                results[0],
                Err(NodeError::InvalidSqlStatement { .. })
            ));
        }
        assert_eq!(runtime.log_store().last_index().unwrap(), Some(2));
    }

    #[test]
    fn sql_group_commit_lone_call_completes_after_the_bounded_collection_round() {
        let (_dir, runtime) = sql_test_runtime();

        let result = runtime
            .execute_sql_batch(vec![SqlCommand {
                request_id: "lone-call".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE lone_call(id INTEGER PRIMARY KEY)".into(),
                    parameters: vec![],
                }],
            }])
            .unwrap();

        assert!(result[0].is_ok());
        let queue = runtime
            .sql_group_commit
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(queue.pending.is_empty());
        assert!(!queue.leader_active);
    }

    #[test]
    fn key_value_write_commits_external_effect_instead_of_raw_put_payload() {
        let (_dir, runtime) = sql_test_runtime();

        let response = runtime.write("key-value-put", "key", "value").unwrap();

        let entry = runtime
            .log_store()
            .read(response.applied_index)
            .unwrap()
            .unwrap();
        assert!(ExternalEffectCommand::decode(&entry.payload).is_ok());
        assert!(!entry.payload.starts_with(b"put\t"));
        assert_eq!(
            runtime.read("key", ReadConsistency::Local).unwrap().value,
            Some("value".into())
        );
    }

    #[test]
    fn sql_batch_preserves_order_commits_one_external_effect_and_retries_exactly() {
        let (_dir, runtime) = sql_test_runtime();
        runtime
            .execute_sql(SqlCommand {
                request_id: "schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE batch_items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                        .into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        let commands = (1..=3)
            .map(|id| SqlCommand {
                request_id: format!("insert-{id}"),
                statements: vec![SqlStatement {
                    sql: "INSERT INTO batch_items(id, value) VALUES (?1, ?2)".into(),
                    parameters: vec![SqlValue::Integer(id), SqlValue::Text(format!("value-{id}"))],
                }],
            })
            .collect::<Vec<_>>();

        let first = runtime.execute_sql_batch(commands.clone()).unwrap();
        let first_indices = first
            .iter()
            .map(|result| result.as_ref().unwrap().applied_index)
            .collect::<Vec<_>>();
        let log_index = runtime.log_store().last_index().unwrap();
        let replay = runtime.execute_sql_batch(commands).unwrap();

        assert_eq!(first_indices, vec![2, 2, 2]);
        assert_eq!(
            first
                .iter()
                .map(|result| result.as_ref().unwrap().hash)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            1
        );
        for index in 1..=2 {
            let entry = runtime.log_store().read(index).unwrap().unwrap();
            assert!(entry.payload.len() <= MAX_COMMAND_BYTES);
            assert!(ExternalEffectCommand::decode(&entry.payload).is_ok());
        }
        assert_eq!(
            replay
                .iter()
                .map(|result| result.as_ref().unwrap().applied_index)
                .collect::<Vec<_>>(),
            first_indices
        );
        assert_eq!(runtime.log_store().last_index().unwrap(), log_index);
    }

    #[test]
    fn sql_effect_over_qlog_limit_is_stored_as_external_bundle() {
        let (_dir, runtime) = sql_test_runtime();
        runtime
            .execute_sql(SqlCommand {
                request_id: "schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE large_effect(value BLOB NOT NULL)".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();

        let response = runtime
            .execute_sql(SqlCommand {
                request_id: "large-effect".into(),
                statements: vec![SqlStatement {
                    sql: "INSERT INTO large_effect(value) VALUES (randomblob(700000))".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();

        assert_eq!(response.applied_index, 2);
        let entry = runtime.log_store().read(2).unwrap().unwrap();
        assert!(entry.payload.len() <= MAX_COMMAND_BYTES);
        let command = ExternalEffectCommand::decode(&entry.payload).unwrap();
        assert!(command.total_effect_bytes() > MAX_COMMAND_BYTES as u32);
    }

    #[test]
    fn sql_batch_isolates_request_conflict_from_unrelated_member() {
        let (_dir, runtime) = sql_test_runtime();
        runtime
            .execute_sql(SqlCommand {
                request_id: "schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE batch_items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                        .into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        let insert = |request_id: &str, id: i64| SqlCommand {
            request_id: request_id.into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO batch_items(id, value) VALUES (?1, ?2)".into(),
                parameters: vec![SqlValue::Integer(id), SqlValue::Text(format!("value-{id}"))],
            }],
        };

        let results = runtime
            .execute_sql_batch(vec![
                insert("same", 1),
                insert("same", 2),
                insert("other", 3),
            ])
            .unwrap();

        assert!(results[0].is_ok());
        assert!(matches!(results[1], Err(NodeError::RequestConflict(_))));
        let conflict = results[1].as_ref().unwrap_err().classification();
        assert_eq!(conflict.code(), "request_conflict");
        assert_eq!(conflict.category(), ErrorCategory::Conflict);
        assert!(!conflict.retryable());
        assert!(results[2].is_ok());
        assert_eq!(
            results[0].as_ref().unwrap().applied_index,
            results[2].as_ref().unwrap().applied_index
        );
        assert!(runtime.is_ready());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn typed_batch_wrong_profile_is_rejected_before_log_attempt() {
        let (_dir, runtime) = graph_test_runtime();
        let command = SqlCommand {
            request_id: "wrong-profile".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE should_not_exist(id INTEGER PRIMARY KEY)".into(),
                parameters: vec![],
            }],
        };

        let error = runtime.execute_sql_batch(vec![command]).unwrap_err();

        assert!(matches!(
            error,
            NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Sqlite,
                actual: ExecutionProfile::Graph
            }
        ));
        assert_eq!(runtime.log_store().last_index().unwrap(), None);
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_query_timeout_returns_503_without_latching_readiness() {
        let (_dir, runtime) = graph_test_runtime();
        let graph = runtime.graph_materializer().unwrap();
        let graph_error = graph
            .query_read_only(
                "UNWIND range(1, 10000) AS x UNWIND range(1, 10000) AS y RETURN sum(x * y) AS total LIMIT 1",
                &std::collections::BTreeMap::new(),
                1,
                1024 * 1024,
                1,
            )
            .unwrap_err();

        let response = node_error_response(runtime.map_graph_read_error(graph_error));

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(runtime.is_ready());
        assert!(!runtime.is_fatal());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_internal_error_returns_500_and_latches_readiness() {
        let (_dir, runtime) = graph_test_runtime();

        let error =
            runtime.map_graph_read_error(rhiza_graph::Error::Ladybug("connection failed".into()));
        let response = node_error_response(error);

        assert_eq!(
            response.status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(!runtime.is_ready());
        assert!(runtime.is_fatal());
    }

    #[test]
    fn recorder_unknown_outcome_is_retryable_without_killing_runtime() {
        let (_dir, runtime) = sql_test_runtime();

        let error = runtime.map_consensus_error(rhiza_quepaxa::Error::UnknownOutcome);

        assert!(matches!(error, NodeError::OutcomeUnknown(_)));
        assert!(error.classification().retryable());
        assert!(runtime.is_ready());
        assert!(!runtime.is_fatal());

        let conflict = runtime.map_consensus_error(rhiza_quepaxa::Error::ConflictingCertificates);
        assert!(matches!(conflict, NodeError::Reconciliation(_)));
        assert!(!runtime.is_ready());
        assert!(runtime.is_fatal());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_write_retries_the_same_request_after_unknown_recorder_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let mut recorders = Vec::<(String, Box<dyn RecorderRpc>)>::new();
        for node_id in ["n1", "n2"] {
            let recorder = RecorderFileStore::new_with_membership(
                dir.path().join("recorders").join(node_id),
                node_id,
                "rhiza:graph:unknown-outcome",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            recorders.push((
                node_id.into(),
                Box::new(UnknownAfterFirstRecord {
                    first: Arc::new(AtomicBool::new(true)),
                    inner: recorder,
                }),
            ));
        }
        let unavailable = RecorderFileStore::new_with_membership(
            dir.path().join("recorders/n3"),
            "n3",
            "rhiza:graph:unknown-outcome",
            1,
            1,
            membership,
        )
        .unwrap();
        recorders.push((
            "n3".into(),
            Box::new(GatedRecorder {
                enabled: Arc::new(AtomicBool::new(false)),
                inner: unavailable,
            }),
        ));
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "rhiza:graph:unknown-outcome",
                "n1",
                1,
                1,
                recorders,
            )
            .unwrap(),
        );
        let config = NodeConfig::new_embedded(
            "unknown-outcome",
            "n1",
            dir.path().join("node"),
            1,
            1,
            ["n1", "n2", "n3"],
        )
        .unwrap()
        .with_execution_profile(ExecutionProfile::Graph)
        .unwrap();
        let runtime = NodeRuntime::open(config, consensus, &[]).unwrap();
        let command = GraphCommandV1::put_document(
            "stable-request",
            "document",
            GraphValueV1::String("value".into()),
        )
        .unwrap();

        let first = runtime.mutate_graph(command.clone()).unwrap_err();
        assert!(matches!(first, NodeError::OutcomeUnknown(_)));
        assert!(runtime.is_ready());
        assert!(!runtime.is_fatal());

        let retried = runtime.mutate_graph(command).unwrap();
        assert_eq!(retried.applied_index(), 1);
        assert!(runtime.is_ready());
        assert!(!runtime.is_fatal());
    }

    fn sql_test_runtime() -> (tempfile::TempDir, NodeRuntime) {
        sql_test_runtime_configured(None, None)
    }

    fn sql_test_runtime_with_profiler(
        profiler: Option<SqlWriteProfiler>,
    ) -> (tempfile::TempDir, NodeRuntime) {
        sql_test_runtime_configured(profiler, None)
    }

    fn sql_test_runtime_configured(
        profiler: Option<SqlWriteProfiler>,
        queue_capacity: Option<usize>,
    ) -> (tempfile::TempDir, NodeRuntime) {
        let dir = tempfile::tempdir().unwrap();
        let cluster_id = "node-unit-test";
        let mut config = NodeConfig::new_embedded(
            cluster_id,
            "n1",
            dir.path().join("node"),
            1,
            1,
            ["n1", "n2", "n3"],
        )
        .unwrap()
        .with_execution_profile(ExecutionProfile::Sqlite)
        .unwrap();
        if let Some(profiler) = profiler {
            config = config.with_sql_write_profiler(profiler);
        }
        if let Some(queue_capacity) = queue_capacity {
            config = config
                .with_sql_group_commit_queue_capacity(queue_capacity)
                .unwrap();
        }
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                "rhiza:sql:node-unit-test",
                "n1",
                1,
                1,
                [
                    dir.path().join("recorders/n1"),
                    dir.path().join("recorders/n2"),
                    dir.path().join("recorders/n3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        );
        let runtime = NodeRuntime::open(config, consensus, &[]).unwrap();
        (dir, runtime)
    }

    #[cfg(feature = "graph")]
    fn graph_test_runtime() -> (tempfile::TempDir, NodeRuntime) {
        let dir = tempfile::tempdir().unwrap();
        let cluster_id = "node-unit-test";
        let config = NodeConfig::new_embedded(
            cluster_id,
            "n1",
            dir.path().join("node"),
            1,
            1,
            ["n1", "n2", "n3"],
        )
        .unwrap()
        .with_execution_profile(ExecutionProfile::Graph)
        .unwrap();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                "rhiza:graph:node-unit-test",
                "n1",
                1,
                1,
                [
                    dir.path().join("recorders/n1"),
                    dir.path().join("recorders/n2"),
                    dir.path().join("recorders/n3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        );
        let runtime = NodeRuntime::open(config, consensus, &[]).unwrap();
        (dir, runtime)
    }

    #[cfg(feature = "kv")]
    fn kv_test_runtime() -> (tempfile::TempDir, NodeRuntime) {
        let dir = tempfile::tempdir().unwrap();
        let cluster_id = "node-unit-test";
        let config = NodeConfig::new_embedded(
            cluster_id,
            "n1",
            dir.path().join("node"),
            1,
            1,
            ["n1", "n2", "n3"],
        )
        .unwrap()
        .with_execution_profile(ExecutionProfile::Kv)
        .unwrap();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                "rhiza:kv:node-unit-test",
                "n1",
                1,
                1,
                [
                    dir.path().join("recorders/n1"),
                    dir.path().join("recorders/n2"),
                    dir.path().join("recorders/n3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        );
        let runtime = NodeRuntime::open(config, consensus, &[]).unwrap();
        (dir, runtime)
    }

    #[test]
    fn recorder_wire_preserves_rpc_error_kinds() {
        for error in [
            rhiza_quepaxa::Error::RpcCancelled,
            rhiza_quepaxa::Error::RpcDeadlineExceeded,
            rhiza_quepaxa::Error::UnknownOutcome,
            rhiza_quepaxa::Error::Io("definite transport failure".into()),
        ] {
            let decoded = super::recorder_error_from_wire(super::recorder_wire_error(error));
            assert!(matches!(
                decoded,
                rhiza_quepaxa::Error::RpcCancelled
                    | rhiza_quepaxa::Error::RpcDeadlineExceeded
                    | rhiza_quepaxa::Error::UnknownOutcome
                    | rhiza_quepaxa::Error::Io(_)
            ));
        }
        let chain = rhiza_quepaxa::Error::ChainConflict {
            slot: 7,
            expected_prev_hash: LogHash::ZERO,
            actual_prev_hash: LogHash::digest(&[b"conflict"]),
        };
        assert_eq!(
            super::recorder_error_from_wire(super::recorder_wire_error(chain.clone())),
            chain
        );
        assert_eq!(
            super::recorder_error_from_wire(super::recorder_wire_error(
                rhiza_quepaxa::Error::ConflictingCertificates,
            )),
            rhiza_quepaxa::Error::ConflictingCertificates
        );
        assert!(matches!(
            super::recorder_error_from_wire(super::RecorderWireError {
                code: super::RecorderWireErrorCode::Other,
                message: "unknown future safety code".into(),
                detail: None,
            }),
            rhiza_quepaxa::Error::Decode(_)
        ));
    }

    #[test]
    fn bounded_json_encoder_accepts_exact_output_and_rejects_one_byte_over() {
        let value = vec!["x"; 31];
        let expected = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            super::bounded_json_encode(
                &value,
                expected.len(),
                super::RECORDER_HTTP_REQUEST_LIMIT_ERROR,
            )
            .unwrap(),
            expected
        );
        assert!(super::bounded_json_encode(
            &value,
            expected.len() - 1,
            super::RECORDER_HTTP_REQUEST_LIMIT_ERROR,
        )
        .is_err());
    }

    #[test]
    fn oversized_http_mutation_is_definite_before_worker_or_network_admission() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let client =
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap();
        let command = StoredCommand::new(EntryType::Command, vec![0_u8; MAX_HTTP_BODY_BYTES]);
        let result = client.store_command_for(
            &rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(5)),
            "rhiza:sql:cluster-a".into(),
            1,
            1,
            LogHash::ZERO,
            command.hash(),
            command,
        );
        assert!(matches!(
            result,
            Err(rhiza_quepaxa::Error::Decode(message))
                if message == super::RECORDER_HTTP_REQUEST_LIMIT_ERROR
        ));
        assert_eq!(
            *client.shared.available_workers.lock().unwrap(),
            HTTP_RECORDER_MAX_CONCURRENT_WORKERS
        );
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn http_recorder_connect_failure_is_definite_for_mutation() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let client =
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap();
        assert!(matches!(
            client.recorder_id(&rhiza_quepaxa::RecorderRpcContext::with_timeout(
                Duration::from_secs(2)
            )),
            Err(rhiza_quepaxa::Error::Io(_))
        ));
        let result = client.record(
            &rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(2)),
            RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: LogHash::ZERO,
                slot: 1,
                step: 1,
                proposal: rhiza_quepaxa::Proposal::nil(),
                command: None,
            },
        );
        assert_eq!(result, Err(rhiza_quepaxa::Error::ProposeFailed));
    }

    #[test]
    fn recorder_connect_and_result_delivery_fit_the_default_consensus_budget() {
        let delivery_margin = Duration::from_millis(50);
        assert!(
            super::RECORDER_HTTP_CONNECT_TIMEOUT + delivery_margin
                < super::QUORUM_RECORD_REQUEST_TIMEOUT
        );
        assert!(
            super::QUORUM_RECORD_REQUEST_TIMEOUT + Duration::from_millis(100)
                < rhiza_quepaxa::DEFAULT_RECORDER_RPC_TIMEOUT
        );
    }

    #[test]
    fn http_recorder_post_send_disconnect_remains_unknown_for_mutation() {
        use std::io::Read;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            let (header_bytes, content_length) = loop {
                let bytes = stream.read(&mut chunk).unwrap();
                assert!(bytes > 0, "request closed before complete headers");
                request.extend_from_slice(&chunk[..bytes]);
                let Some(header_bytes) = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4)
                else {
                    continue;
                };
                let headers = std::str::from_utf8(&request[..header_bytes]).unwrap();
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':').and_then(|(name, value)| {
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                    })
                    .expect("mutation request declares content-length");
                break (header_bytes, content_length);
            };
            while request.len() < header_bytes + content_length {
                let bytes = stream.read(&mut chunk).unwrap();
                assert!(bytes > 0, "request closed before complete body");
                request.extend_from_slice(&chunk[..bytes]);
            }
        });
        let client =
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap();
        let result = client.record(
            &rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(2)),
            RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: LogHash::ZERO,
                slot: 1,
                step: 1,
                proposal: rhiza_quepaxa::Proposal::nil(),
                command: None,
            },
        );
        assert_eq!(result, Err(rhiza_quepaxa::Error::UnknownOutcome));
        server.join().unwrap();
    }

    #[test]
    fn http_recorder_mutation_never_follows_a_redirect_after_full_post() {
        use std::io::{Read, Write};

        let redirected = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        redirected.set_nonblocking(true).unwrap();
        let redirected_address = redirected.local_addr().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            let (header_bytes, content_length) = loop {
                let bytes = stream.read(&mut chunk).unwrap();
                assert!(bytes > 0, "request closed before complete headers");
                request.extend_from_slice(&chunk[..bytes]);
                let Some(header_bytes) = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4)
                else {
                    continue;
                };
                let headers = std::str::from_utf8(&request[..header_bytes]).unwrap();
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':').and_then(|(name, value)| {
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                    })
                    .expect("mutation request declares content-length");
                break (header_bytes, content_length);
            };
            while request.len() < header_bytes + content_length {
                let bytes = stream.read(&mut chunk).unwrap();
                assert!(bytes > 0, "request closed before complete body");
                request.extend_from_slice(&chunk[..bytes]);
            }
            write!(
                stream,
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{redirected_address}/record\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            stream.flush().unwrap();
        });
        let client =
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap();
        let result = client.record(
            &rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(2)),
            RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: LogHash::ZERO,
                slot: 1,
                step: 1,
                proposal: rhiza_quepaxa::Proposal::nil(),
                command: None,
            },
        );
        assert_eq!(result, Err(rhiza_quepaxa::Error::UnknownOutcome));
        server.join().unwrap();
        assert_eq!(
            redirected.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn definite_remote_failures_preserve_local_accept_for_same_slot_retry() {
        let dir = tempfile::tempdir().unwrap();
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let local = RecorderFileStore::new_with_membership(
            dir.path().join("n1"),
            "n1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let remotes_enabled = Arc::new(AtomicBool::new(false));
        let n2 = RecorderFileStore::new_with_membership(
            dir.path().join("n2"),
            "n2",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let n3 = RecorderFileStore::new_with_membership(
            dir.path().join("n3"),
            "n3",
            "cluster",
            1,
            1,
            membership,
        )
        .unwrap();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "cluster",
                "n1",
                1,
                1,
                vec![
                    ("n1".into(), Box::new(local.clone()) as Box<dyn RecorderRpc>),
                    (
                        "n2".into(),
                        Box::new(GatedRecorder {
                            enabled: Arc::clone(&remotes_enabled),
                            inner: n2.clone(),
                        }),
                    ),
                    (
                        "n3".into(),
                        Box::new(GatedRecorder {
                            enabled: Arc::clone(&remotes_enabled),
                            inner: n3.clone(),
                        }),
                    ),
                ],
            )
            .unwrap(),
        );
        let offered = Command::new(CommandKind::Deterministic, b"original".to_vec());
        let first = consensus.propose_at(
            rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(2)),
            1,
            LogHash::ZERO,
            offered.clone(),
        );
        assert_eq!(first, Err(rhiza_quepaxa::Error::ProposeFailed));
        assert!(local.inspect_record_summary(1).unwrap().is_some());

        let (_runtime_dir, runtime) = sql_test_runtime();
        let response =
            node_error_response(runtime.map_consensus_error(rhiza_quepaxa::Error::ProposeFailed));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(runtime.is_ready());
        assert!(!runtime.is_fatal());

        let local_enabled = Arc::new(AtomicBool::new(false));
        let foreign = ThreeNodeConsensus::from_recorders_with_ids(
            "cluster",
            "n2",
            1,
            1,
            vec![
                (
                    "n1".into(),
                    Box::new(GatedRecorder {
                        enabled: local_enabled,
                        inner: local,
                    }) as Box<dyn RecorderRpc>,
                ),
                ("n2".into(), Box::new(n2)),
                ("n3".into(), Box::new(n3)),
            ],
        )
        .unwrap();
        let foreign_winner = foreign
            .propose_at(
                rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(5)),
                1,
                LogHash::ZERO,
                Command::new(CommandKind::Deterministic, b"foreign".to_vec()),
            )
            .unwrap();
        assert_eq!(foreign_winner.payload, b"foreign");

        remotes_enabled.store(true, Ordering::Release);
        let learned = consensus
            .propose_at(
                rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(5)),
                1,
                LogHash::ZERO,
                offered.clone(),
            )
            .unwrap();
        assert_eq!(learned.hash, foreign_winner.hash);
        assert_eq!(learned.payload, b"foreign");

        let decided = consensus
            .propose_at(
                rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(5)),
                2,
                learned.hash,
                offered,
            )
            .unwrap();
        assert_eq!(decided.payload, b"original");
        assert_ne!(decided.hash, learned.hash);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_recorder_rejects_schema_valid_many_member_request_at_decode_budget_and_recovers()
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_calls = Arc::clone(&calls);
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                recorder_router(
                    CountingHttpIdentity {
                        calls: server_calls,
                    },
                    vec![PeerConfig::new("node-2", "http://node-2:8081", "peer-token-2").unwrap()],
                ),
            )
            .await
            .unwrap();
        });
        let body = super::RecorderWire {
            version: RECORDER_WIRE_VERSION,
            remaining_deadline_ms: 1_000,
            body: super::InstallProofV2 {
                proof: DecisionProof::FastPath {
                    cluster_id: "cluster".into(),
                    slot: 1,
                    epoch: 1,
                    config_id: 1,
                    config_digest: LogHash::ZERO,
                    proposal: Proposal::nil(),
                    summaries: Vec::new(),
                },
                // The request is schema-valid. Empty members would be
                // rejected later by Membership::from_voters, but this
                // many-small Vec<String> must fail first at the bounded
                // owned-decode allocation gate.
                members: vec![String::new(); 200_000],
            },
        };
        let encoded = serde_json::to_vec(&body).unwrap();
        assert!(encoded.len() < MAX_HTTP_BODY_BYTES);
        let response = reqwest::Client::new()
            .post(format!("http://{address}{RECORDER_INSTALL_PROOF_PATH}"))
            .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
            .header(super::NODE_ID_HEADER, "node-2")
            .header(super::RECOVERY_GENERATION_HEADER, "1")
            .bearer_auth("peer-token-2")
            .header(CONTENT_TYPE, "application/json")
            .body(encoded)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let error = response.json::<serde_json::Value>().await.unwrap();
        assert_eq!(error["body"]["status"], "Error");
        assert_eq!(error["body"]["body"]["code"], "Decode");
        assert_eq!(
            error["body"]["body"]["detail"],
            serde_json::json!({"Message": "recorder decode heap budget exceeded"})
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let recovered = tokio::task::spawn_blocking(move || {
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2")
                .unwrap()
                .recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
        })
        .await
        .unwrap();
        assert_eq!(recovered.unwrap(), "node-1");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_recorder_round_trips_external_effect_bundle() {
        let root = tempfile::tempdir().unwrap();
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        let store = RecorderFileStore::new_with_membership(
            root.path(),
            "node-1",
            "cluster",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                recorder_router(
                    store,
                    vec![PeerConfig::new("node-2", "http://node-2:8081", "peer-token-2").unwrap()],
                ),
            )
            .await
            .unwrap();
        });

        let chunks = vec![b"http-effect-bundle".to_vec()];
        let command = ExternalEffectCommand::from_profile_bytes_and_chunks(
            "cluster",
            1,
            1,
            membership.digest(),
            1,
            LogHash::ZERO,
            rhiza_core::ExternalEffectProfile::sql(vec![1]),
            &chunks,
        )
        .unwrap();
        let manifest_command = StoredCommand::new(EntryType::Command, command.encode().unwrap());
        let binding = rhiza_quepaxa::EffectBundleBinding {
            cluster_id: command.cluster_id().into(),
            epoch: command.epoch(),
            config_id: command.config_id(),
            config_digest: command.config_digest(),
            intended_slot: command.intended_slot(),
            prev_hash: command.prev_hash(),
            manifest_command_hash: manifest_command.hash(),
            effect_digest: command.effect_digest_value(),
        };
        let bundle =
            rhiza_quepaxa::RecorderEffectBundle::new(binding.clone(), chunks.clone()).unwrap();
        let request =
            rhiza_quepaxa::EffectBundleFinalizeRequest::new(bundle, manifest_command.clone())
                .unwrap();
        let result = tokio::task::spawn_blocking(move || {
            let client =
                HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2")
                    .unwrap();
            let context = rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(5));
            client.stage_effect_bundle_chunk(
                &context,
                binding.clone(),
                manifest_command.clone(),
                0,
                chunks[0].clone(),
            )?;
            client.finalize_staged_effect_bundle(
                &context,
                binding.clone(),
                manifest_command.clone(),
            )?;
            let fetched_manifest =
                client.fetch_effect_bundle_manifest(&context, binding.clone())?;
            let fetched_chunk = client.fetch_effect_bundle_chunk(&context, binding, 0)?;
            Ok::<_, rhiza_quepaxa::Error>((fetched_manifest, fetched_chunk))
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.0, Some(request.manifest_command));
        assert_eq!(result.1, Some(b"http-effect-bundle".to_vec()));
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_recorder_shared_decode_gate_is_32_and_recovers_after_release() {
        let _lock = test_http_decode_hook_lock().lock().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let _hook = install_test_http_decode_hook(TestHttpDecodeHook {
            request_ids: Arc::new((0..33).collect()),
            entered: entered_tx,
            release: Arc::clone(&release),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_calls = Arc::clone(&calls);
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                recorder_router_with_test_peer_concurrency(
                    CountingHttpIdentity {
                        calls: server_calls,
                    },
                    vec![PeerConfig::new("node-2", "http://node-2:8081", "peer-token-2").unwrap()],
                    40,
                ),
            )
            .await
            .unwrap();
        });
        let http = reqwest::Client::new();
        let mut requests = tokio::task::JoinSet::new();
        for request_id in 0..33 {
            let http = http.clone();
            let url = format!("http://{address}{RECORDER_IDENTITY_PATH}");
            requests.spawn(async move {
                http.post(url)
                    .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
                    .header(super::NODE_ID_HEADER, "node-2")
                    .header(super::RECOVERY_GENERATION_HEADER, "1")
                    .header("x-rhiza-test-decode-id", request_id.to_string())
                    .bearer_auth("peer-token-2")
                    .json(&serde_json::json!({
                        "version": RECORDER_WIRE_VERSION,
                        "remaining_deadline_ms": 10_000,
                        "body": null,
                    }))
                    .send()
                    .await
                    .unwrap()
                    .status()
            });
        }
        let mut entered = std::collections::HashSet::new();
        for _ in 0..32 {
            let request_id = tokio::time::timeout(Duration::from_secs(2), entered_rx.recv())
                .await
                .expect("decode gate did not admit 32 requests")
                .expect("decode hook event channel closed");
            assert!(
                entered.insert(request_id),
                "duplicate decode event {request_id}"
            );
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), entered_rx.recv())
                .await
                .is_err(),
            "33rd request entered the 32-slot decode gate"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        release.notify_waiters();
        let last = tokio::time::timeout(Duration::from_secs(2), entered_rx.recv())
            .await
            .expect("33rd request did not enter after a decode permit was released")
            .expect("decode hook event channel closed");
        assert!(entered.insert(last));
        assert_eq!(entered.len(), 33);
        release.notify_waiters();
        while let Some(result) = requests.join_next().await {
            assert_eq!(result.unwrap(), StatusCode::OK);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 33);

        let recovered = http
            .post(format!("http://{address}{RECORDER_IDENTITY_PATH}"))
            .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
            .header(super::NODE_ID_HEADER, "node-2")
            .header(super::RECOVERY_GENERATION_HEADER, "1")
            .bearer_auth("peer-token-2")
            .json(&serde_json::json!({
                "version": RECORDER_WIRE_VERSION,
                "remaining_deadline_ms": 1_000,
                "body": null,
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(recovered.status(), StatusCode::OK);
        assert_eq!(calls.load(Ordering::SeqCst), 34);
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn oversized_http_backend_fetch_is_typed_decode_json_with_canonical_status() {
        let response = super::recorder_v2_response::<Option<StoredCommand>>(Ok(Ok(Some(
            StoredCommand::new(EntryType::Command, vec![0_u8; MAX_HTTP_BODY_BYTES]),
        ))));
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let body = axum::body::to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES)
            .await
            .unwrap();
        assert!(matches!(
            serde_json::from_slice::<super::RecorderWire<
                super::RecorderV2Result<Option<StoredCommand>>,
            >>(&body)
            .unwrap()
            .body,
            super::RecorderV2Result::Error(error)
                if matches!(error.code, super::RecorderWireErrorCode::Decode)
                    && matches!(
                        &error.detail,
                        Some(super::RecorderWireErrorDetail::Message(message))
                            if message == super::RECORDER_HTTP_RESPONSE_LIMIT_ERROR
                    )
        ));

        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                recorder_router(
                    OversizedHttpFetch {
                        calls: server_calls,
                    },
                    vec![PeerConfig::new("node-2", "http://node-2:8081", "peer-token-2").unwrap()],
                ),
            )
            .await
            .unwrap();
        });
        let client =
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap();
        let result = tokio::task::spawn_blocking(move || {
            client.fetch_command_for(
                &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                "rhiza:sql:cluster-a".into(),
                1,
                1,
                LogHash::ZERO,
                LogHash::ZERO,
            )
        })
        .await
        .unwrap();
        assert!(matches!(
            result,
            Err(rhiza_quepaxa::Error::Decode(message))
                if message == super::RECORDER_HTTP_RESPONSE_LIMIT_ERROR
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn oversized_http_backend_record_is_typed_unknown_outcome_json() {
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        let command = StoredCommand::new(EntryType::Command, b"record".to_vec());
        let request = RecordRequest {
            cluster_id: "rhiza:sql:cluster-a".into(),
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            slot: 1,
            step: 1,
            proposal: Proposal::new(
                ProposalPriority::MAX,
                "node-2",
                1,
                AcceptedValue::from_command(
                    "rhiza:sql:cluster-a",
                    1,
                    1,
                    1,
                    LogHash::ZERO,
                    &command,
                ),
            ),
            command: Some(command),
        };
        let summary = RecordSummary {
            recorder_id: "node-1".into(),
            slot: request.slot,
            config_id: request.config_id,
            config_digest: request.config_digest,
            step: request.step,
            first_current: Some(request.proposal.clone()),
            aggregate_prior: None,
            decided: None,
        };
        let under_bound = super::recorder_v2_mutation_response(Ok(Ok(summary)));
        assert_eq!(under_bound.status(), StatusCode::OK);
        assert_eq!(
            under_bound.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );

        let oversized = RecordSummary {
            recorder_id: "x".repeat(MAX_HTTP_BODY_BYTES),
            slot: request.slot,
            config_id: request.config_id,
            config_digest: request.config_digest,
            step: request.step,
            first_current: Some(request.proposal.clone()),
            aggregate_prior: None,
            decided: None,
        };
        let response = super::recorder_v2_mutation_response(Ok(Ok(oversized)));
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let body = axum::body::to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES)
            .await
            .unwrap();
        assert!(matches!(
            serde_json::from_slice::<super::RecorderWire<
                super::RecorderV2Result<RecordSummary>,
            >>(&body)
            .unwrap()
            .body,
            super::RecorderV2Result::Error(error)
                if matches!(error.code, super::RecorderWireErrorCode::UnknownOutcome)
                    && error.detail.is_none()
        ));

        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                recorder_router(
                    OversizedHttpRecord {
                        calls: server_calls,
                    },
                    vec![PeerConfig::new("node-2", "http://node-2:8081", "peer-token-2").unwrap()],
                ),
            )
            .await
            .unwrap();
        });
        let client =
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap();
        let result = tokio::task::spawn_blocking(move || {
            client.record(
                &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                request,
            )
        })
        .await
        .unwrap();
        assert!(matches!(result, Err(rhiza_quepaxa::Error::UnknownOutcome)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_recorder_clones_share_bounded_workers_and_cancel_waiters_without_spawning() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let blackhole = std::thread::spawn(move || {
            let mut streams = Vec::new();
            loop {
                let (stream, _) = listener.accept().unwrap();
                if release_rx.try_recv().is_ok() {
                    return;
                }
                accepted_tx.send(()).unwrap();
                streams.push(stream);
            }
        });
        let client = Arc::new(
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap(),
        );
        let active = (0..HTTP_RECORDER_MAX_CONCURRENT_WORKERS)
            .map(|_| {
                let client = Arc::clone(&client);
                std::thread::spawn(move || {
                    client.recorder_id(&rhiza_quepaxa::RecorderRpcContext::with_timeout(
                        Duration::from_secs(5),
                    ))
                })
            })
            .collect::<Vec<_>>();
        for _ in 0..HTTP_RECORDER_MAX_CONCURRENT_WORKERS {
            accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }

        let cancelled = Arc::new(AtomicBool::new(false));
        let waiting_client = client.as_ref().clone();
        let waiting_cancelled = Arc::clone(&cancelled);
        let waiting = std::thread::spawn(move || {
            waiting_client.recorder_id(
                &rhiza_quepaxa::RecorderRpcContext::with_timeout_and_cancellation(
                    Duration::from_secs(5),
                    waiting_cancelled,
                ),
            )
        });
        let started = Instant::now();
        cancelled.store(true, Ordering::SeqCst);
        assert!(matches!(
            waiting.join().unwrap(),
            Err(rhiza_quepaxa::Error::RpcCancelled)
        ));
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "cancelled waiter did not observe the 10ms admission poll promptly"
        );
        assert!(
            accepted_rx.try_recv().is_err(),
            "a cancelled waiter spawned an HTTP worker despite no available slot"
        );

        let expired_client = client.as_ref().clone();
        let expired = std::thread::spawn(move || {
            expired_client.recorder_id(&rhiza_quepaxa::RecorderRpcContext::with_timeout(
                Duration::from_millis(20),
            ))
        });
        assert!(matches!(
            expired.join().unwrap(),
            Err(rhiza_quepaxa::Error::RpcDeadlineExceeded)
        ));
        assert!(
            accepted_rx.try_recv().is_err(),
            "an expired waiter spawned an HTTP worker despite no available slot"
        );

        release_tx.send(()).unwrap();
        let _ = std::net::TcpStream::connect(address);
        blackhole.join().unwrap();
        for call in active {
            assert!(call.join().unwrap().is_err());
        }

        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        let recovered_server = tokio::spawn(async move {
            let peers =
                vec![PeerConfig::new("node-2", "http://node-2:8081", "peer-token-2").unwrap()];
            axum::serve(listener, recorder_router(HttpIdentityRecorder, peers))
                .await
                .unwrap();
        });
        let recovered = Arc::clone(&client);
        assert_eq!(
            tokio::task::spawn_blocking(move || {
                recovered.recorder_id(&rhiza_quepaxa::RecorderRpcContext::with_timeout(
                    Duration::from_secs(2),
                ))
            })
            .await
            .unwrap()
            .unwrap(),
            "node-1"
        );
        recovered_server.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_recorder_rejects_content_length_oversize_before_body_read() {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            accepted_tx.send(()).unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                MAX_HTTP_BODY_BYTES + 1
            )
            .unwrap();
            stream.flush().unwrap();
            release_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        });
        let client =
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            client.recorder_id(&rhiza_quepaxa::RecorderRpcContext::with_timeout(
                Duration::from_secs(2),
            ))
        });
        accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(
            result.await.unwrap(),
            Err(rhiza_quepaxa::Error::Decode(_))
        ));
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "content-length oversize should reject before waiting for a body"
        );
        release_tx.send(()).unwrap();
        server.join().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_recorder_bounds_chunked_oversize_and_preserves_mutation_ambiguity() {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let body = vec![b'x'; MAX_HTTP_BODY_BYTES + 1];
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0_u8; 4096];
                let _ = stream.read(&mut request);
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n",
                    body.len()
                );
                let _ = stream.write_all(&body);
                let _ = stream.write_all(b"\r\n0\r\n\r\n");
                let _ = stream.flush();
            }
        });
        let client =
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap();
        let read_client = client.clone();
        let read = tokio::task::spawn_blocking(move || {
            read_client.recorder_id(&rhiza_quepaxa::RecorderRpcContext::with_timeout(
                Duration::from_secs(2),
            ))
        })
        .await
        .unwrap();
        assert!(
            matches!(read, Err(rhiza_quepaxa::Error::Decode(_))),
            "chunked oversized read result: {read:?}"
        );

        let mutation = tokio::task::spawn_blocking(move || {
            client.record(
                &rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(2)),
                RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: LogHash::ZERO,
                    slot: 1,
                    step: 1,
                    proposal: rhiza_quepaxa::Proposal::nil(),
                    command: None,
                },
            )
        })
        .await
        .unwrap();
        assert!(matches!(
            mutation,
            Err(rhiza_quepaxa::Error::UnknownOutcome)
        ));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn http_response_wire_mismatch_is_unknown_only_for_a_mutation() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let malicious = || async {
            Json(serde_json::json!({
                "version": RECORDER_WIRE_VERSION - 1,
                "remaining_deadline_ms": 1_000,
                "body": {
                    "status": "error",
                    "body": {"code": "Other", "message": "wrong response envelope"}
                }
            }))
        };
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route(RECORDER_IDENTITY_PATH, post(malicious))
                    .route(RECORDER_RECORD_PATH, post(malicious)),
            )
            .await
            .unwrap();
        });
        let client =
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap();
        let read_client = client.clone();
        let read = tokio::task::spawn_blocking(move || {
            read_client.recorder_id(&rhiza_quepaxa::RecorderRpcContext::with_timeout(
                Duration::from_secs(2),
            ))
        })
        .await
        .unwrap();
        assert!(matches!(read, Err(rhiza_quepaxa::Error::Decode(_))));

        let mutation = tokio::task::spawn_blocking(move || {
            client.record(
                &rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(2)),
                RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: LogHash::ZERO,
                    slot: 1,
                    step: 1,
                    proposal: rhiza_quepaxa::Proposal::nil(),
                    command: None,
                },
            )
        })
        .await
        .unwrap();
        assert!(matches!(
            mutation,
            Err(rhiza_quepaxa::Error::UnknownOutcome)
        ));
        server.abort();
    }

    #[tokio::test]
    async fn http_recorder_bounded_response_decode_keeps_local_and_declared_errors_distinct() {
        type ProofResponse = super::RecorderWire<super::RecorderV2Result<Option<DecisionProof>>>;
        let summary = RecorderSummary {
            recorder_id: "r".into(),
            slot: 1,
            step: 1,
            first_current: None,
            aggregate_prior: None,
        };
        let per_summary = std::mem::size_of::<RecorderSummary>() * 3;
        let summary_count = MAX_HTTP_BODY_BYTES
            .saturating_mul(4)
            .checked_div(per_summary)
            .unwrap()
            .saturating_add(1);
        let response = ProofResponse {
            version: RECORDER_WIRE_VERSION,
            remaining_deadline_ms: 1_000,
            body: super::RecorderV2Result::Ok(Some(DecisionProof::FastPath {
                cluster_id: "cluster".into(),
                slot: 1,
                epoch: 1,
                config_id: 1,
                config_digest: LogHash::ZERO,
                proposal: Proposal::nil(),
                summaries: vec![summary; summary_count],
            })),
        };
        let response = Arc::new(serde_json::to_vec(&response).unwrap());
        assert!(response.len() < MAX_HTTP_BODY_BYTES);
        assert!(matches!(
            super::recorder_decode::decode_json_exact_bounded::<ProofResponse>(
                response.as_ref(),
                super::recorder_decode::RecorderDecodeLimits::for_wire_bytes(MAX_HTTP_BODY_BYTES),
            ),
            Err(super::recorder_decode::RecorderDecodeError::Decode(message))
                if message == "recorder decode heap budget exceeded"
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (observed_tx, mut observed_rx) = tokio::sync::mpsc::unbounded_channel();
        let inspect_response = Arc::clone(&response);
        let inspect_observed = observed_tx.clone();
        let record_response = Arc::clone(&response);
        let record_observed = observed_tx;
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route(
                        RECORDER_INSPECT_PROOF_PATH,
                        post(move |request: Request| {
                            let response = Arc::clone(&inspect_response);
                            let observed = inspect_observed.clone();
                            async move {
                                let _ = observed.send((
                                    request.uri().path().to_owned(),
                                    request
                                        .headers()
                                        .get(VERSION_HEADER)
                                        .and_then(|value| value.to_str().ok())
                                        .map(str::to_owned),
                                    request
                                        .headers()
                                        .get(super::NODE_ID_HEADER)
                                        .and_then(|value| value.to_str().ok())
                                        .map(str::to_owned),
                                    request
                                        .headers()
                                        .get(super::RECOVERY_GENERATION_HEADER)
                                        .and_then(|value| value.to_str().ok())
                                        .map(str::to_owned),
                                    request
                                        .headers()
                                        .get(CONTENT_TYPE)
                                        .and_then(|value| value.to_str().ok())
                                        .map(str::to_owned),
                                ));
                                (
                                    StatusCode::OK,
                                    [(CONTENT_TYPE, "application/json")],
                                    response.as_ref().clone(),
                                )
                            }
                        }),
                    )
                    .route(
                        RECORDER_RECORD_PATH,
                        post(move |request: Request| {
                            let response = Arc::clone(&record_response);
                            let observed = record_observed.clone();
                            async move {
                                let _ = observed.send((
                                    request.uri().path().to_owned(),
                                    request
                                        .headers()
                                        .get(VERSION_HEADER)
                                        .and_then(|value| value.to_str().ok())
                                        .map(str::to_owned),
                                    request
                                        .headers()
                                        .get(super::NODE_ID_HEADER)
                                        .and_then(|value| value.to_str().ok())
                                        .map(str::to_owned),
                                    request
                                        .headers()
                                        .get(super::RECOVERY_GENERATION_HEADER)
                                        .and_then(|value| value.to_str().ok())
                                        .map(str::to_owned),
                                    request
                                        .headers()
                                        .get(CONTENT_TYPE)
                                        .and_then(|value| value.to_str().ok())
                                        .map(str::to_owned),
                                ));
                                (
                                    StatusCode::OK,
                                    [(CONTENT_TYPE, "application/json")],
                                    response.as_ref().clone(),
                                )
                            }
                        }),
                    ),
            )
            .await
            .unwrap();
        });
        let client =
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2").unwrap();
        let read_client = client.clone();
        let read = tokio::task::spawn_blocking(move || {
            read_client.inspect_decision_proof(
                &rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(2)),
                1,
            )
        })
        .await
        .unwrap();
        assert!(matches!(
            read,
            Err(rhiza_quepaxa::Error::Decode(message))
                if message == "recorder decode heap budget exceeded"
        ));
        let mutation = tokio::task::spawn_blocking(move || {
            client.record(
                &rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(2)),
                RecordRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: LogHash::ZERO,
                    slot: 1,
                    step: 1,
                    proposal: rhiza_quepaxa::Proposal::nil(),
                    command: None,
                },
            )
        })
        .await
        .unwrap();
        assert!(matches!(
            mutation,
            Err(rhiza_quepaxa::Error::UnknownOutcome)
        ));
        let mut paths = std::collections::HashSet::new();
        for _ in 0..2 {
            let (path, version, node_id, generation, content_type) =
                tokio::time::timeout(Duration::from_secs(2), observed_rx.recv())
                    .await
                    .expect("fake peer did not observe the public HTTP client request")
                    .expect("fake peer observation channel closed");
            assert_eq!(version.as_deref(), Some(RECORDER_PROTOCOL_VERSION));
            assert_eq!(node_id.as_deref(), Some("node-2"));
            assert_eq!(generation.as_deref(), Some("1"));
            assert_eq!(content_type.as_deref(), Some("application/json"));
            paths.insert(path);
        }
        assert_eq!(
            paths,
            std::collections::HashSet::from([
                RECORDER_INSPECT_PROOF_PATH.to_owned(),
                RECORDER_RECORD_PATH.to_owned(),
            ])
        );
        server.abort();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    RECORDER_FETCH_COMMAND_PATH,
                    post(|| async {
                        super::recorder_v2_response::<Option<StoredCommand>>(Ok(Err(
                            rhiza_quepaxa::Error::Decode("declared recorder decode".into()),
                        )))
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let typed = tokio::task::spawn_blocking(move || {
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2")
                .unwrap()
                .fetch_command_for(
                    &rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(2)),
                    "cluster".into(),
                    1,
                    1,
                    LogHash::ZERO,
                    LogHash::ZERO,
                )
        })
        .await
        .unwrap();
        assert!(matches!(
            typed,
            Err(rhiza_quepaxa::Error::Decode(message)) if message == "declared recorder decode"
        ));
        server.abort();
    }

    #[tokio::test]
    async fn http_authenticated_recorder_saturation_is_a_definite_mutation_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let peers =
                vec![PeerConfig::new("node-2", "http://node-2:8081", "peer-token-2").unwrap()];
            axum::serve(
                listener,
                recorder_routes(
                    HttpIdentityRecorder,
                    peers,
                    1,
                    Arc::new(tokio::sync::Semaphore::new(0)),
                ),
            )
            .await
            .unwrap();
        });
        let result = tokio::task::spawn_blocking(move || {
            HttpRecorderClient::new(format!("http://{address}"), "node-2", "peer-token-2")
                .unwrap()
                .record(
                    &rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(2)),
                    RecordRequest {
                        cluster_id: "cluster".into(),
                        epoch: 1,
                        config_id: 1,
                        config_digest: LogHash::ZERO,
                        slot: 1,
                        step: 1,
                        proposal: rhiza_quepaxa::Proposal::nil(),
                        command: None,
                    },
                )
        })
        .await
        .unwrap();
        assert!(
            matches!(result, Err(rhiza_quepaxa::Error::Io(_))),
            "saturated mutation result: {result:?}"
        );
        server.abort();
    }

    #[test]
    fn startup_consensus_error_keeps_retry_and_reconciliation_classes() {
        assert!(matches!(
            super::startup_consensus_error(rhiza_quepaxa::Error::RpcCancelled),
            NodeError::Unavailable(_)
        ));
        assert!(matches!(
            super::startup_consensus_error(rhiza_quepaxa::Error::RpcDeadlineExceeded),
            NodeError::Unavailable(_)
        ));
        assert!(matches!(
            super::startup_consensus_error(rhiza_quepaxa::Error::UnknownOutcome),
            NodeError::Reconciliation(_)
        ));
        assert!(matches!(
            super::startup_consensus_error(rhiza_quepaxa::Error::ConflictingCertificates),
            NodeError::Reconciliation(_)
        ));
        assert!(matches!(
            super::startup_consensus_error(rhiza_quepaxa::Error::ChainConflict {
                slot: 9,
                expected_prev_hash: LogHash::ZERO,
                actual_prev_hash: LogHash::digest(&[b"conflict"]),
            }),
            NodeError::Reconciliation(_)
        ));
    }
}

#[cfg(feature = "kv")]
fn largest_fitting_kv_batch_prefix(commands: &[KvCommandV1]) -> Option<(usize, Vec<u8>)> {
    if commands.len() < 3 {
        return None;
    }
    let mut lower = 2_usize;
    let mut upper = commands.len() - 1;
    let mut largest = None;
    while lower <= upper {
        let count = lower + (upper - lower) / 2;
        let payload = encode_replicated_kv_batch(&commands[..count])
            .expect("the validated KV batch prefix remains valid");
        if payload.len() <= MAX_COMMAND_BYTES {
            largest = Some((count, payload));
            lower = count + 1;
        } else {
            upper = count - 1;
        }
    }
    largest
}

#[cfg(feature = "graph")]
fn largest_fitting_graph_batch_prefix(commands: &[GraphCommandV1]) -> Option<(usize, Vec<u8>)> {
    for count in (2..commands.len()).rev() {
        let payload = encode_replicated_graph_batch(&commands[..count])
            .expect("the validated graph batch prefix remains valid");
        if payload.len() <= MAX_COMMAND_BYTES {
            return Some((count, payload));
        }
    }
    None
}

#[cfg(feature = "graph")]
fn validate_typed_batch_len(len: usize) -> Result<(), NodeError> {
    if (1..=MAX_WRITE_BATCH_MEMBERS).contains(&len) {
        Ok(())
    } else {
        Err(NodeError::InvalidRequest(format!(
            "write batch must contain 1..={MAX_WRITE_BATCH_MEMBERS} commands"
        )))
    }
}

#[cfg(feature = "kv")]
fn validate_kv_batch_len(len: usize) -> Result<(), NodeError> {
    if (1..=MAX_KV_BATCH_MEMBERS).contains(&len) {
        Ok(())
    } else {
        Err(NodeError::InvalidRequest(format!(
            "KV write batch must contain 1..={MAX_KV_BATCH_MEMBERS} commands"
        )))
    }
}

#[cfg(feature = "sql")]
fn validate_sql_batch_len(len: usize) -> Result<(), NodeError> {
    if (1..=MAX_TYPED_SQL_WRITE_BATCH_MEMBERS).contains(&len) {
        Ok(())
    } else {
        Err(NodeError::InvalidRequest(format!(
            "SQL write batch must contain 1..={MAX_TYPED_SQL_WRITE_BATCH_MEMBERS} commands"
        )))
    }
}

fn validate_command_size(payload: &[u8]) -> Result<(), NodeError> {
    if payload.len() <= MAX_COMMAND_BYTES {
        Ok(())
    } else {
        Err(NodeError::InvalidRequest(format!(
            "command exceeds {MAX_COMMAND_BYTES} bytes"
        )))
    }
}

#[cfg(feature = "sql")]
fn canonical_put(request_id: &str, key: &str, value: &str) -> Result<Vec<u8>, NodeError> {
    validate_field("request_id", request_id, MAX_REQUEST_ID_BYTES, false)?;
    validate_key(key)?;
    validate_field("value", value, MAX_VALUE_BYTES, true)?;
    let payload = encode_put_request(request_id, key, value)
        .map_err(|error| NodeError::InvalidRequest(error.to_string()))?;
    validate_command_size(&payload)?;
    Ok(payload)
}

#[cfg(feature = "sql")]
fn encode_sql_command_with_index(command: &SqlCommand) -> Result<Vec<u8>, NodeError> {
    encode_sql_command(command).map_err(|error| {
        let message = error.to_string();
        match first_invalid_sql_statement(command, |prefix| encode_sql_command(prefix).is_err()) {
            Some(statement_index) => NodeError::InvalidSqlStatement {
                statement_index,
                message,
            },
            None => NodeError::InvalidRequest(message),
        }
    })
}

#[cfg(feature = "sql")]
fn first_invalid_sql_statement(
    command: &SqlCommand,
    mut invalid: impl FnMut(&SqlCommand) -> bool,
) -> Option<usize> {
    if command.statements.is_empty() || command.statements.len() > MAX_SQL_STATEMENTS {
        return None;
    }
    (0..command.statements.len()).find(|statement_index| {
        let prefix = SqlCommand {
            request_id: command.request_id.clone(),
            statements: command.statements[..=*statement_index].to_vec(),
        };
        invalid(&prefix)
    })
}

#[cfg(feature = "sql")]
fn validate_key(key: &str) -> Result<(), NodeError> {
    validate_field("key", key, MAX_KEY_BYTES, false)
}

#[cfg(feature = "sql")]
fn validate_field(
    name: &str,
    value: &str,
    max_bytes: usize,
    allow_empty: bool,
) -> Result<(), NodeError> {
    if !allow_empty && value.is_empty() {
        return Err(NodeError::InvalidRequest(format!(
            "{name} must not be empty"
        )));
    }
    if value.len() > max_bytes {
        return Err(NodeError::InvalidRequest(format!(
            "{name} exceeds {max_bytes} bytes"
        )));
    }
    if value.contains('\t') {
        return Err(NodeError::InvalidRequest(format!(
            "{name} must not contain a tab"
        )));
    }
    Ok(())
}

#[cfg(feature = "sql")]
fn write_response(outcome: RequestOutcome) -> WriteResponse {
    WriteResponse {
        applied_index: outcome.original_log_index(),
        hash: outcome.original_log_hash(),
    }
}

fn reconcile_local_storage(
    config: &NodeConfig,
    consensus: &ThreeNodeConsensus,
    log_store: &FileLogStore,
    materializer: &Materializer,
) -> Result<(), NodeError> {
    let mut log_state = log_store
        .logical_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    let mut log_last_index = log_state.tip.as_ref().map_or(0, |tip| tip.index());
    let applied_index = materializer
        .applied_index()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    let applied_hash = materializer
        .applied_hash()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    let mut materializer_configuration = materializer
        .configuration_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?;

    if let Some(anchor) = &log_state.anchor {
        if anchor.recovery_generation() != config.recovery_generation {
            return Err(NodeError::Reconciliation(format!(
                "qlog anchor recovery generation {} differs from runtime generation {}",
                anchor.recovery_generation(),
                config.recovery_generation
            )));
        }
        if applied_index < anchor.compacted().index() {
            return Err(NodeError::SnapshotRequired(Box::new(anchor.clone())));
        }
    }
    if applied_index > log_last_index {
        let entries: Option<Vec<LogEntry>> = match materializer {
            #[cfg(feature = "sql")]
            Materializer::Sql(sql) => Some(
                sql.embedded_log_entries(log_last_index.saturating_add(1), applied_index)
                    .map_err(|error| NodeError::Reconciliation(error.to_string()))?,
            ),
            #[cfg(feature = "kv")]
            Materializer::Kv(kv) => Some(
                kv.embedded_log_entries(log_last_index.saturating_add(1), applied_index)
                    .map_err(|error| NodeError::Reconciliation(error.to_string()))?,
            ),
            #[cfg(feature = "graph")]
            Materializer::Graph(_) => None,
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Materializer::Unavailable => None,
        };
        if let Some(entries) = entries {
            log_store
                .append_batch(&entries)
                .map_err(|error| NodeError::Storage(error.to_string()))?;
            log_state = log_store
                .logical_state()
                .map_err(|error| NodeError::Storage(error.to_string()))?;
            log_last_index = log_state.tip.as_ref().map_or(0, |tip| tip.index());
        }
    }
    if applied_index > log_last_index {
        return Err(NodeError::Reconciliation(format!(
            "{} materializer is ahead at {applied_index}, qlog ends at {log_last_index}",
            materializer.profile()
        )));
    }
    if applied_index == 0 {
        if applied_hash != LogHash::ZERO {
            return Err(NodeError::Reconciliation(format!(
                "{} materializer genesis hash is not zero",
                materializer.profile()
            )));
        }
    } else if !log_state.anchor.as_ref().is_some_and(|anchor| {
        applied_index == anchor.compacted().index() && applied_hash == anchor.compacted().hash()
    }) {
        let entry = log_store
            .read(applied_index)
            .map_err(|error| NodeError::Storage(error.to_string()))?
            .ok_or_else(|| {
                NodeError::Reconciliation(format!(
                    "qlog prefix is missing {} materializer index {applied_index}",
                    materializer.profile()
                ))
            })?;
        validate_entry_envelope(config, &entry, applied_index, entry.prev_hash)?;
        if entry.hash != applied_hash {
            return Err(NodeError::Reconciliation(format!(
                "{} materializer hash diverges from qlog at index {applied_index}",
                materializer.profile()
            )));
        }
    }

    let mut expected_prev_hash = applied_hash;
    for index in (applied_index + 1)..=log_last_index {
        let entry = log_store
            .read(index)
            .map_err(|error| NodeError::Storage(error.to_string()))?
            .ok_or_else(|| {
                NodeError::Reconciliation(format!("qlog prefix is missing index {index}"))
            })?;
        match &materializer_configuration {
            Some(configuration) => {
                validate_runtime_entry(config, configuration, &entry, index, expected_prev_hash)?
            }
            None => validate_entry_envelope(config, &entry, index, expected_prev_hash)?,
        };
        apply_reconciled_entry(
            config,
            consensus,
            materializer,
            materializer_configuration.as_ref(),
            &entry,
            &RecorderRpcContext::default_timeout(),
        )?;
        materializer_configuration = materializer
            .configuration_state()
            .map_err(|error| NodeError::Reconciliation(error.to_string()))?;
        expected_prev_hash = entry.hash;
    }
    let log_configuration = log_store
        .configuration_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    if materializer_configuration
        .as_ref()
        .is_some_and(|configuration| configuration != &log_configuration)
    {
        return Err(NodeError::Reconciliation(format!(
            "qlog and {} materializer configuration states disagree",
            materializer.profile()
        )));
    }
    Ok(())
}

fn apply_reconciled_entry(
    _config: &NodeConfig,
    _consensus: &ThreeNodeConsensus,
    materializer: &Materializer,
    _configuration: Option<&ConfigurationState>,
    entry: &LogEntry,
    _context: &RecorderRpcContext,
) -> Result<(), NodeError> {
    #[cfg(feature = "sql")]
    if _config.execution_profile() == ExecutionProfile::Sqlite
        && entry.entry_type == EntryType::Command
        && !entry.payload.is_empty()
    {
        let configuration = _configuration.ok_or_else(|| {
            NodeError::Reconciliation("SQL materializer omitted its configuration state".into())
        })?;
        let verified =
            resolve_external_sql_effect_from_consensus(_consensus, configuration, entry, _context)?;
        return materializer
            .apply_verified_external_sql_effect(&verified)
            .map(|_| ())
            .map_err(|error| NodeError::Reconciliation(error.to_string()));
    }
    materializer
        .apply_entry(entry)
        .map(|_| ())
        .map_err(|error| NodeError::Reconciliation(error.to_string()))
}

#[cfg(feature = "sql")]
pub(crate) fn resolve_external_sql_effect_from_consensus(
    consensus: &ThreeNodeConsensus,
    configuration: &ConfigurationState,
    entry: &LogEntry,
    context: &RecorderRpcContext,
) -> Result<VerifiedQwalEffectBundleV4, NodeError> {
    validate_external_sql_entry(entry, configuration).map_err(NodeError::Reconciliation)?;
    let command = ExternalEffectCommand::decode(&entry.payload).map_err(|error| {
        NodeError::Reconciliation(format!("invalid committed QEFX command: {error}"))
    })?;
    let manifest_command = StoredCommand::new(EntryType::Command, entry.payload.clone());
    let binding = effect_bundle_binding(&command, &manifest_command);
    let bundle = consensus
        .resolve_effect_bundle_from_quorum(context, &binding, &manifest_command)
        .map_err(startup_consensus_error)?
        .ok_or_else(|| {
            NodeError::Unavailable("committed QEFX bundle is temporarily unavailable".into())
        })?;
    verify_resolved_external_sql_bundle(&command, &bundle)
        .map_err(|error| NodeError::Reconciliation(error.to_string()))
}

fn recover_peer_candidates(
    config: &NodeConfig,
    consensus: &ThreeNodeConsensus,
    log_store: &FileLogStore,
    materializer: &Materializer,
    peer_candidates: &[&dyn LogPeer],
    startup: &StartupIoContext,
) -> Result<(), NodeError> {
    for peer in peer_candidates {
        let (last_index, last_hash) = static_log_tip(log_store)?;
        startup.check("peer log fetch")?;
        let candidates = match peer.fetch_log(FetchLogRequest {
            from_index: last_index.saturating_add(1),
            max_entries: MAX_FETCH_ENTRIES,
        }) {
            Ok(response) => validate_fetched_entries_with_configuration(
                last_index.saturating_add(1),
                last_hash,
                &config.cluster_id,
                config.epoch,
                log_store
                    .configuration_state()
                    .map_err(|error| NodeError::Storage(error.to_string()))?,
                response.entries,
            )
            .map_err(|error| {
                NodeError::Reconciliation(format!("peer candidate validation failed: {error}"))
            })?,
            Err(FetchLogError::Transport { .. }) => continue,
            Err(FetchLogError::SnapshotRequired { anchor }) => {
                return Err(NodeError::SnapshotRequired(anchor));
            }
            Err(error) => {
                return Err(NodeError::Reconciliation(format!(
                    "peer candidate validation failed: {error}"
                )));
            }
        };
        startup.check("peer log fetch")?;

        let mut expected_index = last_index.checked_add(1).ok_or_else(|| {
            NodeError::Reconciliation("qlog index is exhausted during peer catch-up".into())
        })?;
        let mut expected_prev_hash = last_hash;
        for candidate in candidates {
            startup.check("peer decision inspection")?;
            match consensus
                .inspect_decision_at(
                    &startup.recorder_context(),
                    expected_index,
                    expected_prev_hash,
                )
                .map_err(startup_consensus_error)?
            {
                DecisionInspection::Committed(committed) if committed == candidate => {
                    startup.check("peer decision inspection")?;
                    persist_startup_entry(
                        config,
                        consensus,
                        log_store,
                        materializer,
                        &candidate,
                        expected_index,
                        expected_prev_hash,
                        startup,
                    )?;
                    expected_prev_hash = candidate.hash;
                    expected_index = expected_index.checked_add(1).ok_or_else(|| {
                        NodeError::Reconciliation(
                            "qlog index is exhausted during peer catch-up".into(),
                        )
                    })?;
                }
                DecisionInspection::Committed(_) => {
                    return Err(NodeError::Reconciliation(format!(
                        "peer candidate at index {expected_index} differs from committed decision"
                    )));
                }
                DecisionInspection::Unavailable => {
                    return Err(NodeError::Unavailable(format!(
                        "decision inspection unavailable for peer candidate at index {expected_index}"
                    )));
                }
                DecisionInspection::Empty | DecisionInspection::Pending => {
                    return Err(NodeError::Reconciliation(format!(
                        "peer candidate at index {expected_index} is not committed"
                    )));
                }
            }
        }
    }
    Ok(())
}

fn recover_startup_decisions(
    config: &NodeConfig,
    consensus: &ThreeNodeConsensus,
    log_store: &FileLogStore,
    materializer: &Materializer,
    startup: &StartupIoContext,
) -> Result<(), NodeError> {
    for _ in 0..MAX_STARTUP_RECOVERY_ENTRIES {
        let (last_index, last_hash) = static_log_tip(log_store)?;
        let slot = last_index.checked_add(1).ok_or_else(|| {
            NodeError::Reconciliation("qlog index is exhausted during startup".into())
        })?;
        startup.check("recorder decision inspection")?;
        match consensus
            .inspect_decision_at(&startup.recorder_context(), slot, last_hash)
            .map_err(startup_consensus_error)?
        {
            DecisionInspection::Committed(entry) => {
                startup.check("recorder decision inspection")?;
                persist_startup_entry(
                    config,
                    consensus,
                    log_store,
                    materializer,
                    &entry,
                    slot,
                    last_hash,
                    startup,
                )?;
            }
            DecisionInspection::Pending => {
                let entry = consensus
                    .propose_at(
                        startup.recorder_context(),
                        slot,
                        last_hash,
                        Command::new(CommandKind::ReadBarrier, Vec::new()),
                    )
                    .map_err(startup_consensus_error)?;
                startup.check("recorder pending decision recovery")?;
                persist_startup_entry(
                    config,
                    consensus,
                    log_store,
                    materializer,
                    &entry,
                    slot,
                    last_hash,
                    startup,
                )?;
            }
            DecisionInspection::Empty => return Ok(()),
            DecisionInspection::Unavailable => {
                return Err(NodeError::Unavailable(
                    "decision inspection unavailable during startup".into(),
                ));
            }
        }
    }
    Err(NodeError::Reconciliation(format!(
        "startup recovery exceeded {MAX_STARTUP_RECOVERY_ENTRIES} entries"
    )))
}

#[allow(clippy::too_many_arguments)] // Startup recovery needs the explicit consensus resolver.
fn persist_startup_entry(
    config: &NodeConfig,
    _consensus: &ThreeNodeConsensus,
    log_store: &FileLogStore,
    materializer: &Materializer,
    entry: &LogEntry,
    expected_index: LogIndex,
    expected_prev_hash: LogHash,
    startup: &StartupIoContext,
) -> Result<(), NodeError> {
    let _permit = startup.admit_local_io("startup qlog and materializer persistence")?;
    let configuration_state = log_store
        .configuration_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    validate_runtime_entry(
        config,
        &configuration_state,
        entry,
        expected_index,
        expected_prev_hash,
    )?;
    #[cfg(feature = "sql")]
    let verified = if config.execution_profile() == ExecutionProfile::Sqlite
        && entry.entry_type == EntryType::Command
        && !entry.payload.is_empty()
    {
        Some(resolve_external_sql_effect_from_consensus(
            _consensus,
            &configuration_state,
            entry,
            &startup.recorder_context(),
        )?)
    } else {
        None
    };
    log_store
        .append(entry)
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    #[cfg(test)]
    test_startup_persistence_hook_after_qlog_append(startup.inner.context_id)?;
    #[cfg(feature = "sql")]
    if let Some(verified) = verified {
        materializer
            .apply_verified_external_sql_effect(&verified)
            .map_err(|error| NodeError::Reconciliation(error.to_string()))?;
    } else {
        materializer
            .apply_entry(entry)
            .map_err(|error| NodeError::Reconciliation(error.to_string()))?;
    }
    #[cfg(not(feature = "sql"))]
    materializer
        .apply_entry(entry)
        .map_err(|error| NodeError::Reconciliation(error.to_string()))?;
    Ok(())
}

fn static_log_tip(log_store: &FileLogStore) -> Result<(LogIndex, LogHash), NodeError> {
    Ok(log_store
        .logical_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?
        .tip
        .map_or((0, LogHash::ZERO), |tip| (tip.index(), tip.hash())))
}

fn validate_runtime_entry(
    config: &NodeConfig,
    configuration_state: &ConfigurationState,
    entry: &LogEntry,
    expected_index: LogIndex,
    expected_prev_hash: LogHash,
) -> Result<(), NodeError> {
    validate_entry_envelope(config, entry, expected_index, expected_prev_hash)?;
    validate_profile_entry_shape(config.execution_profile(), entry)
        .map_err(NodeError::Invariant)?;
    #[cfg(feature = "sql")]
    if config.execution_profile() == ExecutionProfile::Sqlite
        && entry.entry_type == EntryType::Command
        && !entry.payload.is_empty()
    {
        validate_external_sql_entry(entry, configuration_state).map_err(NodeError::Invariant)?;
    }
    configuration_state
        .validate_entry(entry)
        .map_err(|error| NodeError::Reconciliation(error.to_string()))?;
    Ok(())
}

fn validate_entry_envelope(
    config: &NodeConfig,
    entry: &LogEntry,
    expected_index: LogIndex,
    expected_prev_hash: LogHash,
) -> Result<(), NodeError> {
    if entry.index != expected_index {
        return Err(NodeError::Reconciliation(format!(
            "expected decision index {expected_index}, got {}",
            entry.index
        )));
    }
    if entry.cluster_id != config.cluster_id || entry.epoch != config.epoch {
        return Err(NodeError::Reconciliation(format!(
            "decision {} has a foreign identity",
            entry.index
        )));
    }
    if entry.prev_hash != expected_prev_hash {
        return Err(NodeError::Reconciliation(format!(
            "decision {} has a conflicting predecessor",
            entry.index
        )));
    }
    if entry.recompute_hash() != entry.hash {
        return Err(NodeError::Reconciliation(format!(
            "decision {} has an invalid hash",
            entry.index
        )));
    }
    validate_entry_shape(entry).map_err(NodeError::Invariant)
}

fn validate_entry_shape(entry: &LogEntry) -> Result<(), String> {
    match entry.entry_type {
        EntryType::Command if entry.payload.len() <= MAX_COMMAND_BYTES => Ok(()),
        EntryType::Command => Err(format!("command exceeds {MAX_COMMAND_BYTES} bytes")),
        EntryType::Noop if entry.payload.is_empty() => Ok(()),
        EntryType::Noop => Err("Noop payload must be empty".into()),
        EntryType::ConfigChange => ConfigChange::recognize(&StoredCommand::new(
            EntryType::ConfigChange,
            entry.payload.clone(),
        ))
        .map(|_| ())
        .map_err(|error| error.to_string()),
        other => Err(format!("unsupported runtime entry type {other:?}")),
    }
}

pub(crate) fn validate_profile_entry_shape(
    _profile: ExecutionProfile,
    entry: &LogEntry,
) -> Result<(), String> {
    validate_entry_shape(entry)?;
    #[cfg(feature = "sql")]
    if _profile == ExecutionProfile::Sqlite
        && entry.entry_type == EntryType::Command
        && !entry.payload.is_empty()
    {
        ExternalEffectCommand::decode(&entry.payload)
            .map_err(|error| format!("SQLite command is not canonical QEFX: {error}"))?;
    }
    Ok(())
}

#[cfg(feature = "sql")]
fn validate_external_sql_entry(
    entry: &LogEntry,
    configuration: &ConfigurationState,
) -> Result<(), String> {
    let command = ExternalEffectCommand::decode(&entry.payload)
        .map_err(|error| format!("SQLite command is not canonical QEFX: {error}"))?;
    if command.cluster_id() != entry.cluster_id
        || command.epoch() != entry.epoch
        || command.config_id() != entry.config_id
        || command.config_digest() != configuration.digest()
        || command.intended_slot() != entry.index
        || command.prev_hash() != entry.prev_hash
    {
        return Err("QEFX command context does not match its log entry".into());
    }
    Ok(())
}

#[cfg(feature = "sql")]
fn effect_bundle_binding(
    command: &ExternalEffectCommand,
    manifest_command: &StoredCommand,
) -> EffectBundleBinding {
    EffectBundleBinding {
        cluster_id: command.cluster_id().into(),
        epoch: command.epoch(),
        config_id: command.config_id(),
        config_digest: command.config_digest(),
        intended_slot: command.intended_slot(),
        prev_hash: command.prev_hash(),
        manifest_command_hash: manifest_command.hash(),
        effect_digest: command.effect_digest_value(),
    }
}

#[cfg(feature = "sql")]
fn verify_resolved_external_sql_bundle(
    command: &ExternalEffectCommand,
    bundle: &RecorderEffectBundle,
) -> rhiza_sql::Result<VerifiedQwalEffectBundleV4> {
    let mut bytes = Vec::with_capacity(bundle.total_len());
    for chunk in bundle.chunks() {
        bytes.extend_from_slice(chunk);
    }
    QwalEffectManifestV4::verify_external_bundle(command, &bytes)
}

fn startup_consensus_error(error: rhiza_quepaxa::Error) -> NodeError {
    match error {
        rhiza_quepaxa::Error::NoQuorum
        | rhiza_quepaxa::Error::ProposeFailed
        | rhiza_quepaxa::Error::CommandUnavailable
        | rhiza_quepaxa::Error::EffectBundleUnavailable
        | rhiza_quepaxa::Error::Cancelled
        | rhiza_quepaxa::Error::RpcCancelled
        | rhiza_quepaxa::Error::RpcDeadlineExceeded
        | rhiza_quepaxa::Error::Io(_) => NodeError::Unavailable(error.to_string()),
        rhiza_quepaxa::Error::UnknownOutcome => NodeError::Reconciliation(error.to_string()),
        other => NodeError::Reconciliation(other.to_string()),
    }
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct E2eConfig {
    pub data_dir: PathBuf,
    pub object_store: ObjStoreConfig,
    pub cluster_id: String,
    pub node_id: String,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct E2eReport {
    pub applied_index: LogIndex,
    pub restored_value: String,
    pub object_keys: Vec<String>,
}

#[cfg(feature = "sql")]
pub async fn run_e2e(config: E2eConfig) -> Result<E2eReport, Box<dyn std::error::Error>> {
    let sqlite_dir = config.data_dir.join("sqlite");
    let log_dir = config.data_dir.join("consensus").join("log");
    ensure_fresh_e2e_data_dir(&config.data_dir, &sqlite_dir, &log_dir)?;

    fs::create_dir_all(&config.data_dir)?;
    let db_path = sqlite_dir.join("db.sqlite");
    let restore_path = config.data_dir.join("restore").join("db.sqlite");
    let db = SqliteStateMachine::open(&db_path, &config.cluster_id, &config.node_id, 1, 1)?;
    let recorder_dir = config.data_dir.join("consensus").join("recorder");
    let consensus = ThreeNodeConsensus::new(
        &config.cluster_id,
        &config.node_id,
        1,
        1,
        [
            recorder_dir.join("node-1"),
            recorder_dir.join("node-2"),
            recorder_dir.join("node-3"),
        ],
    )?;
    let base_request = canonical_put("e2e-base", "alpha", "bravo")?;
    let base_effect = db.prepare_put_effect(
        "e2e-base",
        "alpha",
        "bravo",
        &base_request,
        0,
        LogHash::ZERO,
    )?;
    let base_entry = consensus.propose(
        RecorderRpcContext::default_timeout(),
        Command::new(CommandKind::Deterministic, base_effect),
    )?;
    db.apply_entry(&base_entry)?;
    let snapshot = db.create_snapshot(base_entry.index)?;

    let tail_request = canonical_put("e2e-tail", "alpha", "charlie")?;
    let tail_effect = db.prepare_put_effect(
        "e2e-tail",
        "alpha",
        "charlie",
        &tail_request,
        base_entry.index,
        base_entry.hash,
    )?;
    let tail_entry = consensus.propose(
        RecorderRpcContext::default_timeout(),
        Command::new(CommandKind::Deterministic, tail_effect),
    )?;
    let segment_path = write_segment_file(&log_dir, std::slice::from_ref(&tail_entry))?;
    let segment = rhiza_log::SegmentFile::new(
        IndexRange::new(tail_entry.index, tail_entry.index)?,
        fs::read(&segment_path)?,
    );
    db.apply_entry(&tail_entry)?;

    let local_archive = matches!(&config.object_store, ObjStoreConfig::Local { .. });
    let store = ObjStore::new(config.object_store)?;
    let archive = if local_archive {
        rhiza_archive::ObjectArchiveStore::new_for_single_process(
            store.clone(),
            config.cluster_id.clone(),
        )
    } else {
        rhiza_archive::ObjectArchiveStore::new(store.clone(), config.cluster_id.clone())?
    };

    let segment_record = archive.publish_segment(tail_entry.epoch, &segment).await?;
    let snapshot_record = archive.publish_snapshot(&snapshot).await?;
    let (mut archive_manifest, expected_manifest_version) = match archive.load_manifest().await? {
        Some(loaded) => (loaded.manifest().clone(), Some(loaded.version().clone())),
        None => (
            rhiza_archive::ArchiveManifest::new(config.cluster_id.clone()),
            None,
        ),
    };
    archive_manifest.set_latest_snapshot(snapshot_record);
    archive_manifest.add_segment(segment_record);
    archive
        .publish_manifest(&archive_manifest, expected_manifest_version)
        .await?;

    let loaded_manifest = archive
        .load_manifest()
        .await?
        .ok_or("published archive manifest is missing")?;
    if loaded_manifest.manifest() != &archive_manifest {
        return Err("reloaded archive manifest did not match the published manifest".into());
    }
    let archived_snapshot = loaded_manifest
        .manifest()
        .latest_snapshot()
        .ok_or("archive manifest is missing its snapshot")?;
    let archived_segment = loaded_manifest
        .manifest()
        .segments()
        .iter()
        .find(|record| {
            record.start_index() == tail_entry.index && record.end_index() == tail_entry.index
        })
        .ok_or("archive manifest is missing its post-snapshot segment")?;

    let downloaded_segment = archive.download_segment(archived_segment).await?;
    let downloaded_entries = decode_segment_for_cluster(&downloaded_segment, &config.cluster_id)?;
    if downloaded_entries.as_slice() != std::slice::from_ref(&tail_entry) {
        return Err("downloaded qlog segment did not match written entry".into());
    }
    let downloaded_snapshot = rhiza_core::Snapshot::new(
        archived_snapshot.manifest().clone(),
        archive.download_snapshot(archived_snapshot).await?,
    );
    restore_snapshot_file(&restore_path, &downloaded_snapshot, &config.node_id)?;
    let restored_db = SqliteStateMachine::open_existing(&restore_path)?;
    if restored_db.get_value("alpha")?.as_deref() != Some("bravo") {
        return Err("restored base snapshot is missing alpha=bravo".into());
    }
    for entry in &downloaded_entries {
        restored_db.apply_entry(entry)?;
    }
    let restored_value = restored_db
        .get_value("alpha")?
        .ok_or("restored SQLite state is missing alpha")?;
    let applied_index = restored_db.applied_index_value()?;
    if applied_index != tail_entry.index || restored_value != "charlie" {
        return Err("restored SQLite state did not include the archived log tail".into());
    }
    let object_keys = store.list(&format!("rhiza/{}", config.cluster_id)).await?;

    Ok(E2eReport {
        applied_index,
        restored_value,
        object_keys,
    })
}

#[cfg(feature = "sql")]
fn ensure_fresh_e2e_data_dir(
    data_dir: &std::path::Path,
    sqlite_dir: &std::path::Path,
    log_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if directory_has_entries(sqlite_dir)? || directory_has_entries(log_dir)? {
        return Err(format!(
            "e2e data directory is not fresh: prior SQLite/qlog data exists in {}",
            data_dir.display()
        )
        .into());
    }
    Ok(())
}

#[cfg(feature = "sql")]
fn directory_has_entries(path: &std::path::Path) -> Result<bool, std::io::Error> {
    match fs::read_dir(path) {
        Ok(mut entries) => entries.next().transpose().map(|entry| entry.is_some()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}
