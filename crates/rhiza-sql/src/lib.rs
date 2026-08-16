use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock, RwLock,
    },
    time::{Duration, Instant},
};

use rhiza_core::{
    ConfigurationState, EntryType, ExternalEffectCommand, LogAnchor, LogEntry, LogHash, LogIndex,
    RecoveryAnchor, Snapshot, SnapshotIdentity, SnapshotManifest, MAX_EXTERNAL_EFFECT_BYTES,
    MAX_EXTERNAL_EFFECT_CHUNK_BYTES,
};
use rusqlite::{
    config::DbConfig,
    hooks::{AuthAction, AuthContext, Authorization},
    params, params_from_iter,
    types::{ToSql, ToSqlOutput, Value, ValueRef},
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use crate::page_state::{PageStateCacheV3, PageStatePatchV3};
use crate::wal_capture::{capture_wal, WalCapture, WalCommit};

mod control;
mod page_state;
mod qwal;
mod wal_capture;

pub use control::{ControlIdentity, ControlStore, PendingApply, RequestReceipt};
pub use page_state::StateIdentityV3;
pub use qwal::{
    decode_qwal_v3, encode_qwal_v3, sqlite_page_size, QwalEffectManifestV4, QwalEnvelopeV3,
    QwalPageEffectV4, QwalPageV3, QwalReceiptReferenceV4, QwalReceiptV3,
    VerifiedQwalEffectBundleV4, MAX_QWAL_V3_BYTES, MAX_QWAL_V3_RECEIPTS, QWAL_V3_MAGIC,
};
const CREATE_KV_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS __rhiza_kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"#;

const SQL_COMMAND_V2_MAGIC: &[u8] = b"QSQL\0\x02";
const SQL_RESULT_V1_MAGIC: &[u8] = b"QRES\0\x01";
const QWAL_SNAPSHOT_V3_MAGIC: &[u8] = b"QSNP\0\x04";
const SQL_EXECUTOR_POLICY_VERSION: &str = "rhiza-sql-qwal-batch-v3-policy-v8-compat";
const SQL_CONNECTION_PROFILE: &str = "qwal_batch_v3;wal_autocheckpoint=0;canonical_synchronous=OFF;control_synchronous=OFF;page_state_cache=rebuildable;staging_synchronous=OFF;foreign_keys=ON;trusted_schema=OFF;temp=command_scoped;attach=denied;vtable=bundled";
pub const MAX_SQL_STATEMENTS: usize = 64;
pub const MAX_SQL_PARAMETERS: usize = 999;
pub const MAX_SQL_TEXT_BYTES: usize = 64 * 1024;
pub const MAX_RETURNING_ROWS: usize = 1_024;
pub const MAX_RETURNING_BYTES: usize = 1024 * 1024;
pub const MAX_SQL_EFFECT_BYTES: usize = 512 * 1024;
// Native WAL capture may scan repeated overwrites without retaining them. A
// 16x work budget keeps that scan bounded while the final unique page set and
// encoded QWAL remain constrained by MAX_SQL_EFFECT_BYTES.
pub const MAX_NATIVE_WAL_CAPTURE_BYTES: u64 = 8 * 1024 * 1024;
pub const MAX_EXTERNAL_NATIVE_WAL_CAPTURE_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_SQL_QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const SQL_PROGRESS_HANDLER_OPS: i32 = 1_000;
const MAX_CONCURRENT_SQL_READS: usize = 64;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct QwalSnapshotV3 {
    user_db: Vec<u8>,
    replicated_control: Vec<u8>,
    user_state: StateIdentityV3,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl ToSql for SqlValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            Self::Null => ToSqlOutput::Owned(Value::Null),
            Self::Integer(value) => ToSqlOutput::Owned(Value::Integer(*value)),
            Self::Real(value) => ToSqlOutput::Owned(Value::Real(*value)),
            Self::Text(value) => ToSqlOutput::Borrowed(ValueRef::Text(value.as_bytes())),
            Self::Blob(value) => ToSqlOutput::Borrowed(ValueRef::Blob(value)),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlStatement {
    pub sql: String,
    #[serde(default)]
    pub parameters: Vec<SqlValue>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlCommand {
    pub request_id: String,
    pub statements: Vec<SqlStatement>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlStatementResult {
    pub rows_affected: u64,
    pub returning: Option<SqlQueryResult>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlCommandResult {
    pub statement_results: Vec<SqlStatementResult>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SqlCommandV2Envelope {
    executor_fingerprint: LogHash,
    command: SqlCommand,
}

#[derive(Clone, Copy, Debug)]
pub struct SqlBatchMember<'a> {
    pub command: &'a SqlCommand,
    pub request_payload: &'a [u8],
}

#[derive(Clone, Debug, PartialEq)]
pub struct SqlBatchPreparation {
    pub effect: Option<Vec<u8>>,
    pub results: Vec<Result<SqlCommandResult>>,
}

pub struct ExternalSqlBatchPreparation {
    pub effect: Option<PreparedExternalSqlEffect>,
    pub results: Vec<Result<SqlCommandResult>>,
}

pub struct PreparedExternalSqlEffect {
    manifest: QwalEffectManifestV4,
    chunks: Vec<Vec<u8>>,
    cluster_id: String,
    epoch: u64,
    configuration_id: u64,
    configuration_digest: LogHash,
    base_index: LogIndex,
    base_hash: LogHash,
}

impl PreparedExternalSqlEffect {
    fn from_effect(effect: QwalEnvelopeV3, configuration_digest: LogHash) -> Result<Self> {
        let mut bundle_bytes = Vec::new();
        let mut receipts = Vec::with_capacity(effect.receipts.len());
        for receipt in &effect.receipts {
            let result_offset = u32::try_from(bundle_bytes.len()).map_err(|_| {
                Error::ResourceExhausted("external SQL receipt offset overflows".into())
            })?;
            let result_len = u32::try_from(receipt.result_blob.len()).map_err(|_| {
                Error::ResourceExhausted("external SQL receipt length overflows".into())
            })?;
            bundle_bytes.extend_from_slice(&receipt.result_blob);
            receipts.push(QwalReceiptReferenceV4 {
                request_id: receipt.request_id.clone(),
                request_digest: receipt.request_digest,
                result_offset,
                result_len,
                result_digest: qwal::qwal_effect_receipt_digest(&receipt.result_blob),
            });
        }
        let page_effect = QwalPageEffectV4 {
            pages: effect.pages,
        }
        .encode(&effect.base_state, &effect.target_state)?;
        bundle_bytes.extend_from_slice(&page_effect);
        if bundle_bytes.len() > MAX_EXTERNAL_EFFECT_BYTES {
            return Err(Error::ResourceExhausted(format!(
                "external SQL effect exceeds {MAX_EXTERNAL_EFFECT_BYTES} bytes"
            )));
        }
        let chunks = bundle_bytes
            .chunks(MAX_EXTERNAL_EFFECT_CHUNK_BYTES)
            .map(<[u8]>::to_vec)
            .collect::<Vec<_>>();
        let manifest = QwalEffectManifestV4 {
            recovery_generation: effect.recovery_generation,
            base_state: effect.base_state,
            target_state: effect.target_state,
            materializer_fingerprint: effect.materializer_fingerprint,
            receipts,
        };
        manifest.validate()?;
        Ok(Self {
            manifest,
            chunks,
            cluster_id: effect.cluster_id,
            epoch: effect.epoch,
            configuration_id: effect.configuration_id,
            configuration_digest,
            base_index: effect.base_index,
            base_hash: effect.base_hash,
        })
    }

    pub const fn manifest(&self) -> &QwalEffectManifestV4 {
        &self.manifest
    }

    pub fn chunks(&self) -> &[Vec<u8>] {
        &self.chunks
    }

    pub fn total_effect_bytes(&self) -> usize {
        self.chunks.iter().map(Vec::len).sum()
    }

    pub fn build_command(
        &self,
        configuration_digest: LogHash,
        intended_slot: LogIndex,
        prev_hash: LogHash,
    ) -> Result<ExternalEffectCommand> {
        let expected_slot = self
            .base_index
            .checked_add(1)
            .ok_or_else(|| Error::InvalidEntry("external SQL effect slot is exhausted".into()))?;
        if configuration_digest != self.configuration_digest
            || intended_slot != expected_slot
            || prev_hash != self.base_hash
        {
            return Err(Error::InvalidEntry(
                "QEFX decision context does not match the prepared SQL base".into(),
            ));
        }
        self.manifest.external_command(
            &self.cluster_id,
            self.epoch,
            self.configuration_id,
            configuration_digest,
            intended_slot,
            prev_hash,
            &self.chunks,
        )
    }

    pub fn verify(&self, command: &ExternalEffectCommand) -> Result<VerifiedQwalEffectBundleV4> {
        let total = self.total_effect_bytes();
        if total > MAX_EXTERNAL_EFFECT_BYTES {
            return Err(Error::ResourceExhausted(
                "external SQL effect assembly exceeds its bound".into(),
            ));
        }
        let mut bundle = Vec::with_capacity(total);
        for chunk in &self.chunks {
            bundle.extend_from_slice(chunk);
        }
        QwalEffectManifestV4::verify_external_bundle(command, &bundle)
    }
}

struct PreparedQwalMutation {
    receipts: Vec<QwalReceiptV3>,
    results: Vec<Result<SqlCommandResult>>,
}

pub fn sql_executor_fingerprint() -> Result<LogHash> {
    static FINGERPRINT: OnceLock<std::result::Result<LogHash, String>> = OnceLock::new();
    FINGERPRINT
        .get_or_init(compute_sql_executor_fingerprint)
        .clone()
        .map_err(Error::Sqlite)
}

fn compute_sql_executor_fingerprint() -> std::result::Result<LogHash, String> {
    let conn = Connection::open_in_memory().map_err(|error| error.to_string())?;
    let source_id: String = conn
        .query_row("SELECT sqlite_source_id()", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    let mut statement = conn
        .prepare("PRAGMA compile_options")
        .map_err(|error| error.to_string())?;
    let mut compile_options = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| error.to_string())?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    compile_options.sort_unstable();
    let canonical = format!(
        "{SQL_EXECUTOR_POLICY_VERSION}\n{SQL_CONNECTION_PROFILE}\n{}\n{}\n{}",
        env!("CARGO_PKG_VERSION"),
        source_id,
        compile_options.join("\n")
    );
    Ok(LogHash::digest(&[canonical.as_bytes()]))
}

pub fn encode_sql_command(command: &SqlCommand) -> Result<Vec<u8>> {
    validate_sql_command(command)?;
    let encoded = serde_json::to_vec(&SqlCommandV2Envelope {
        executor_fingerprint: sql_executor_fingerprint()?,
        command: command.clone(),
    })
    .map_err(|error| Error::InvalidCommand(format!("cannot encode SQL command: {error}")))?;
    let mut payload = Vec::with_capacity(SQL_COMMAND_V2_MAGIC.len() + encoded.len());
    payload.extend_from_slice(SQL_COMMAND_V2_MAGIC);
    payload.extend_from_slice(&encoded);
    Ok(payload)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplyProgress {
    applied_index: LogIndex,
    applied_hash: LogHash,
}

impl ApplyProgress {
    pub const fn new(applied_index: LogIndex, applied_hash: LogHash) -> Self {
        Self {
            applied_index,
            applied_hash,
        }
    }

    pub const fn applied_index(&self) -> LogIndex {
        self.applied_index
    }

    pub const fn applied_hash(&self) -> LogHash {
        self.applied_hash
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ApplyOutcome {
    progress: ApplyProgress,
    sql_result: Option<SqlCommandResult>,
}

impl ApplyOutcome {
    pub const fn progress(&self) -> ApplyProgress {
        self.progress
    }

    pub const fn sql_result(&self) -> Option<&SqlCommandResult> {
        self.sql_result.as_ref()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestOutcome {
    original_log_index: LogIndex,
    original_log_hash: LogHash,
}

impl RequestOutcome {
    pub const fn new(original_log_index: LogIndex, original_log_hash: LogHash) -> Self {
        Self {
            original_log_index,
            original_log_hash,
        }
    }

    pub const fn original_log_index(&self) -> LogIndex {
        self.original_log_index
    }

    pub const fn original_log_hash(&self) -> LogHash {
        self.original_log_hash
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestConflict {
    request_id: String,
    original_outcome: RequestOutcome,
}

impl RequestConflict {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub const fn original_outcome(&self) -> RequestOutcome {
        self.original_outcome
    }
}

impl std::fmt::Display for RequestConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "request id reused with different payload: {}",
            self.request_id
        )
    }
}

impl std::error::Error for RequestConflict {}

pub type Result<T> = std::result::Result<T, Error>;
pub type SqlRequestLookup = Result<Option<(RequestOutcome, Option<SqlCommandResult>)>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    ApplyFailed,
    RestoreFailed,
    Io(String),
    Sqlite(String),
    ResourceExhausted(String),
    InvalidCommand(String),
    IdentityMismatch(String),
    InvalidEntry(String),
    RequestConflict(RequestConflict),
    InvalidSnapshot(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApplyFailed => write!(f, "SQLite apply failed"),
            Self::RestoreFailed => write!(f, "SQLite restore failed"),
            Self::Io(message) => write!(f, "SQLite io failed: {message}"),
            Self::Sqlite(message) => write!(f, "SQLite error: {message}"),
            Self::ResourceExhausted(message) => write!(f, "SQLite resource exhausted: {message}"),
            Self::InvalidCommand(message) => write!(f, "invalid deterministic command: {message}"),
            Self::IdentityMismatch(field) => {
                write!(f, "SQLite database identity mismatch for {field}")
            }
            Self::InvalidEntry(message) => write!(f, "invalid log entry: {message}"),
            Self::RequestConflict(conflict) => conflict.fmt(f),
            Self::InvalidSnapshot(message) => write!(f, "invalid SQLite snapshot: {message}"),
        }
    }
}

impl std::error::Error for Error {}

pub trait StateMachine {
    fn applied_index(&self) -> Result<LogIndex>;
    fn apply(&self, entry: &LogEntry) -> Result<ApplyProgress>;
    fn create_snapshot(&self, target: LogIndex) -> Result<Snapshot>;
}

pub struct SqliteStateMachine {
    path: PathBuf,
    // QWAL installation can replace the canonical file. Read-only connections
    // keep this shared guard for their whole query so every result is from one
    // canonical generation.
    read_state: Arc<SqlReadState>,
    // Declared after read_state so pooled readers close before the canonical
    // connection when the last owner is dropped.
    conn: Mutex<Option<Connection>>,
    control: ControlStore,
    page_state: Mutex<CanonicalPageStateV3>,
    uncommitted_effect: Mutex<Option<LogHash>>,
}

struct SqlReadState {
    lifecycle: RwLock<()>,
    admission: Mutex<SqlReadAdmission>,
    pool: Mutex<SqlReadPool>,
    #[cfg(test)]
    writer_waiting_hook: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    #[cfg(test)]
    opened_read_connections: std::sync::atomic::AtomicUsize,
    pending_fence: AtomicBool,
    applied_tip: RwLock<ApplyProgress>,
}

#[derive(Default)]
struct SqlReadAdmission {
    active_readers: usize,
    waiting_writers: usize,
    retired: bool,
}

#[derive(Default)]
struct SqlReadPool {
    idle: Vec<Connection>,
    open: usize,
}

struct PooledReadConnection<'a> {
    conn: Option<Connection>,
    reusable: bool,
    state: &'a SqlReadState,
}

impl PooledReadConnection<'_> {
    fn connection(&self) -> &Connection {
        self.conn
            .as_ref()
            .expect("pooled SQLite read connection is checked out")
    }

    fn discard(&mut self) {
        self.reusable = false;
    }
}

impl Drop for PooledReadConnection<'_> {
    fn drop(&mut self) {
        let Some(conn) = self.conn.take() else {
            return;
        };
        let Ok(mut pool) = self.state.pool.lock() else {
            return;
        };
        if self.reusable {
            pool.idle.push(conn);
        } else {
            pool.open -= 1;
        }
    }
}

struct SqlReadGuard<'a> {
    _lifecycle: std::sync::RwLockReadGuard<'a, ()>,
    state: &'a SqlReadState,
}

impl Drop for SqlReadGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut admission) = self.state.admission.lock() {
            admission.active_readers -= 1;
        }
    }
}

struct SqlWriteGuard<'a> {
    _lifecycle: std::sync::RwLockWriteGuard<'a, ()>,
    state: &'a SqlReadState,
}

impl Drop for SqlWriteGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut admission) = self.state.admission.lock() {
            admission.waiting_writers -= 1;
        }
    }
}

impl SqlReadState {
    fn checkout(&self, path: &Path) -> Result<PooledReadConnection<'_>> {
        {
            let mut pool = self
                .pool
                .lock()
                .map_err(|_| Error::Sqlite("SQLite read-pool lock is poisoned".into()))?;
            if let Some(conn) = pool.idle.pop() {
                return Ok(PooledReadConnection {
                    conn: Some(conn),
                    reusable: true,
                    state: self,
                });
            }
            if pool.open == MAX_CONCURRENT_SQL_READS {
                return Err(Error::ResourceExhausted(format!(
                    "SQLite read-connection pool limit is {MAX_CONCURRENT_SQL_READS}"
                )));
            }
            pool.open += 1;
        }
        let conn = match open_read_connection(path) {
            Ok(conn) => conn,
            Err(error) => {
                self.pool
                    .lock()
                    .map_err(|_| Error::Sqlite("SQLite read-pool lock is poisoned".into()))?
                    .open -= 1;
                return Err(error);
            }
        };
        #[cfg(test)]
        self.opened_read_connections.fetch_add(1, Ordering::Relaxed);
        Ok(PooledReadConnection {
            conn: Some(conn),
            reusable: true,
            state: self,
        })
    }

    fn drain_pool(&self) -> Result<()> {
        let mut pool = self
            .pool
            .lock()
            .map_err(|_| Error::Sqlite("SQLite read-pool lock is poisoned".into()))?;
        pool.idle.clear();
        pool.open = 0;
        Ok(())
    }

    fn lock_read(&self) -> Result<SqlReadGuard<'_>> {
        {
            let mut admission = self
                .admission
                .lock()
                .map_err(|_| Error::Sqlite("SQLite read-admission lock is poisoned".into()))?;
            if admission.retired {
                return Err(Error::Sqlite("SQLite read generation is retired".into()));
            }
            if admission.waiting_writers != 0 {
                return Err(Error::ResourceExhausted(
                    "SQLite read admission paused for an exclusive operation".into(),
                ));
            }
            if admission.active_readers == MAX_CONCURRENT_SQL_READS {
                return Err(Error::ResourceExhausted(format!(
                    "SQLite concurrent read limit is {MAX_CONCURRENT_SQL_READS}"
                )));
            }
            admission.active_readers += 1;
        }
        let lifecycle = match self.lifecycle.read() {
            Ok(lifecycle) => lifecycle,
            Err(_) => {
                self.admission
                    .lock()
                    .map_err(|_| Error::Sqlite("SQLite read-admission lock is poisoned".into()))?
                    .active_readers -= 1;
                return Err(Error::Sqlite("SQLite lifecycle lock is poisoned".into()));
            }
        };
        if self
            .admission
            .lock()
            .map_err(|_| Error::Sqlite("SQLite read-admission lock is poisoned".into()))?
            .retired
        {
            drop(lifecycle);
            self.admission
                .lock()
                .map_err(|_| Error::Sqlite("SQLite read-admission lock is poisoned".into()))?
                .active_readers -= 1;
            return Err(Error::Sqlite("SQLite read generation is retired".into()));
        }
        Ok(SqlReadGuard {
            _lifecycle: lifecycle,
            state: self,
        })
    }

    fn lock_write(&self) -> Result<SqlWriteGuard<'_>> {
        self.admission
            .lock()
            .map_err(|_| Error::Sqlite("SQLite read-admission lock is poisoned".into()))?
            .waiting_writers += 1;
        #[cfg(test)]
        if let Some(hook) = self
            .writer_waiting_hook
            .lock()
            .map_err(|_| Error::Sqlite("SQLite writer hook lock is poisoned".into()))?
            .take()
        {
            let _ = hook.send(());
        }
        let lifecycle = match self.lifecycle.write() {
            Ok(lifecycle) => lifecycle,
            Err(_) => {
                self.admission
                    .lock()
                    .map_err(|_| Error::Sqlite("SQLite read-admission lock is poisoned".into()))?
                    .waiting_writers -= 1;
                return Err(Error::Sqlite("SQLite lifecycle lock is poisoned".into()));
            }
        };
        let guard = SqlWriteGuard {
            _lifecycle: lifecycle,
            state: self,
        };
        self.drain_pool()?;
        Ok(guard)
    }
}

#[derive(Clone)]
pub struct SqlReadHandle {
    path: PathBuf,
    state: Arc<SqlReadState>,
}

impl SqlReadHandle {
    pub fn query_sql(
        &self,
        query: &SqlStatement,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<SqlQueryResult> {
        self.query_sql_with_timeout(query, max_rows, max_bytes, DEFAULT_SQL_QUERY_TIMEOUT)
    }

    pub fn query_sql_with_timeout(
        &self,
        query: &SqlStatement,
        max_rows: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<SqlQueryResult> {
        let _lifecycle = self.state.lock_read()?;
        if self.state.pending_fence.load(Ordering::Acquire) {
            return Err(Error::InvalidEntry(
                "canonical SQLite state is unavailable while a QWAL apply is pending".into(),
            ));
        }
        let mut pooled = self.state.checkout(&self.path)?;
        #[cfg(test)]
        gate_sql_read(&self.path)?;
        let (result, reusable) =
            query_sql_read_only(pooled.connection(), query, max_rows, max_bytes, timeout);
        if !reusable {
            pooled.discard();
        }
        result
    }

    pub fn query_sql_with_applied_tip(
        &self,
        query: &SqlStatement,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<(LogIndex, LogHash, SqlQueryResult)> {
        let _lifecycle = self.state.lock_read()?;
        if self.state.pending_fence.load(Ordering::Acquire) {
            return Err(Error::InvalidEntry(
                "canonical SQLite state is unavailable while a QWAL apply is pending".into(),
            ));
        }
        let tip = *self
            .state
            .applied_tip
            .read()
            .map_err(|_| Error::Sqlite("SQLite applied-tip lock is poisoned".into()))?;
        let mut pooled = self.state.checkout(&self.path)?;
        #[cfg(test)]
        gate_sql_read(&self.path)?;
        let (result, reusable) = query_sql_read_only(
            pooled.connection(),
            query,
            max_rows,
            max_bytes,
            DEFAULT_SQL_QUERY_TIMEOUT,
        );
        if !reusable {
            pooled.discard();
        }
        let result = result?;
        Ok((tip.applied_index(), tip.applied_hash(), result))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanonicalPageStateV3 {
    cache: PageStateCacheV3,
    seal: Option<PreparedBaseSeal>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PreparedBaseSeal {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: i64,
    mtime_nsec: i64,
    ctime: i64,
    ctime_nsec: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoverySnapshot {
    snapshot: Snapshot,
    anchor: RecoveryAnchor,
}

impl RecoverySnapshot {
    pub const fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub fn db_bytes(&self) -> &[u8] {
        self.snapshot.db_bytes()
    }

    pub const fn anchor(&self) -> &RecoveryAnchor {
        &self.anchor
    }
}

impl SqliteStateMachine {
    pub fn open(
        path: impl AsRef<Path>,
        cluster_id: &str,
        node_id: &str,
        epoch: u64,
        config_id: u64,
    ) -> Result<Self> {
        Self::open_with_configuration(
            path,
            cluster_id,
            node_id,
            epoch,
            ConfigurationState::active(config_id, LogHash::ZERO),
        )
    }

    pub fn open_with_configuration(
        path: impl AsRef<Path>,
        cluster_id: &str,
        node_id: &str,
        epoch: u64,
        configuration_state: ConfigurationState,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        ensure_parent(&path)?;

        let control_path = control_sidecar_path(&path);
        match (path.exists(), control_path.exists()) {
            (false, false) => {
                Self::create_new(&path, cluster_id, node_id, epoch, configuration_state)
            }
            (true, true) => {
                let db = Self::open_existing_file(&path)?;
                db.validate_control_identity(cluster_id, node_id, epoch)?;
                Ok(db)
            }
            (true, false) => Err(Error::IdentityMismatch(
                "QWAL control sidecar is missing; install a QWAL snapshot instead of auto-migrating"
                    .into(),
            )),
            (false, true) => Err(Error::IdentityMismatch(
                "canonical SQLite database is missing beside its QWAL control sidecar".into(),
            )),
        }
    }

    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_existing_file(path.as_ref())
    }

    fn create_new(
        path: &Path,
        cluster_id: &str,
        node_id: &str,
        epoch: u64,
        configuration_state: ConfigurationState,
    ) -> Result<Self> {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|err| Error::Io(err.to_string()))?;

        let control_path = control_sidecar_path(path);
        let created = (|| {
            let conn = open_connection(path)?;
            conn.execute_batch(CREATE_KV_TABLE_SQL)
                .map_err(sqlite_error)?;
            conn.execute_batch("VACUUM; PRAGMA wal_checkpoint(TRUNCATE);")
                .map_err(sqlite_error)?;
            conn.close()
                .map_err(|(_, error)| Error::Sqlite(error.to_string()))?;
            let page_state = rebuild_sealed_page_state(path)?;
            let identity = ControlIdentity::new(
                cluster_id,
                node_id,
                epoch,
                configuration_state,
                1,
                sql_executor_fingerprint()?,
                page_state.cache.identity(),
            );
            let control = ControlStore::create(&control_path, &identity)?;
            let conn = open_connection(path)?;
            Ok(Self {
                path: path.to_path_buf(),
                conn: Mutex::new(Some(conn)),
                control,
                read_state: Arc::new(SqlReadState {
                    lifecycle: RwLock::new(()),
                    admission: Mutex::new(SqlReadAdmission::default()),
                    pool: Mutex::new(SqlReadPool::default()),
                    #[cfg(test)]
                    writer_waiting_hook: Mutex::new(None),
                    #[cfg(test)]
                    opened_read_connections: std::sync::atomic::AtomicUsize::new(0),
                    pending_fence: AtomicBool::new(false),
                    applied_tip: RwLock::new(ApplyProgress::new(0, LogHash::ZERO)),
                }),
                page_state: Mutex::new(page_state),
                uncommitted_effect: Mutex::new(None),
            })
        })();
        if created.is_err() {
            let _ = fs::remove_file(path);
            let _ = fs::remove_file(&control_path);
        }
        created
    }

    fn open_existing_file(path: &Path) -> Result<Self> {
        let control_path = control_sidecar_path(path);
        if !path.exists() || !control_path.exists() {
            return Err(Error::IdentityMismatch(
                "QWAL database and control sidecar must both exist".into(),
            ));
        }
        let control = ControlStore::open_existing_unchecked(&control_path)?;
        discard_speculative_sidecars_if_base_matches_control(path, &control)?;
        let (page_state, pending) = validate_control_database_pair(path, &control)?;
        let conn = open_connection(path)?;
        let applied_tip = control.applied_tip()?;
        Ok(Self {
            path: path.to_path_buf(),
            conn: Mutex::new(Some(conn)),
            control,
            read_state: Arc::new(SqlReadState {
                lifecycle: RwLock::new(()),
                admission: Mutex::new(SqlReadAdmission::default()),
                pool: Mutex::new(SqlReadPool::default()),
                #[cfg(test)]
                writer_waiting_hook: Mutex::new(None),
                #[cfg(test)]
                opened_read_connections: std::sync::atomic::AtomicUsize::new(0),
                pending_fence: AtomicBool::new(pending),
                applied_tip: RwLock::new(applied_tip),
            }),
            page_state: Mutex::new(page_state),
            uncommitted_effect: Mutex::new(None),
        })
    }

    fn validate_control_identity(&self, cluster_id: &str, node_id: &str, epoch: u64) -> Result<()> {
        let identity = self.control.identity()?;
        if identity.cluster_id() != cluster_id {
            return Err(Error::IdentityMismatch("cluster_id".into()));
        }
        if identity.node_id() != node_id {
            return Err(Error::IdentityMismatch("node_id".into()));
        }
        if identity.epoch() != epoch {
            return Err(Error::IdentityMismatch("epoch".into()));
        }
        if identity.materializer_fingerprint() != sql_executor_fingerprint()? {
            return Err(Error::IdentityMismatch(
                "SQLite QWAL materializer fingerprint".into(),
            ));
        }
        Ok(())
    }

    fn with_connection<T>(&self, operation: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let guard = self
            .conn
            .lock()
            .map_err(|_| Error::Sqlite("SQLite connection lock is poisoned".into()))?;
        let conn = guard
            .as_ref()
            .ok_or_else(|| Error::Sqlite("SQLite connection is closed".into()))?;
        operation(conn)
    }

    fn lock_lifecycle(&self) -> Result<SqlWriteGuard<'_>> {
        self.read_state.lock_write()
    }

    fn lock_read_lifecycle(&self) -> Result<SqlReadGuard<'_>> {
        self.read_state.lock_read()
    }

    fn ensure_no_pending_apply(&self) -> Result<()> {
        if self.read_state.pending_fence.load(Ordering::Acquire) {
            return Err(Error::InvalidEntry(
                "canonical SQLite state is unavailable while a QWAL apply is pending".into(),
            ));
        }
        Ok(())
    }

    fn close_connection(&self) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| Error::Sqlite("SQLite connection lock is poisoned".into()))?
            .take();
        if let Some(conn) = conn {
            conn.close()
                .map_err(|(_, error)| Error::Sqlite(error.to_string()))?;
        }
        Ok(())
    }

    /// Closes a detached state machine so another owner can reopen the canonical database.
    ///
    /// SQLite may retain empty WAL sidecars after the last connection closes on some
    /// filesystems. Once the WAL is checkpointed and the connection is closed, those sidecars
    /// are owned crash artifacts and must be removed before the strict reopen validation.
    pub fn close_for_handoff(self) -> Result<()> {
        let _lifecycle = self.lock_lifecycle()?;
        self.read_state
            .admission
            .lock()
            .map_err(|_| Error::Sqlite("SQLite read-admission lock is poisoned".into()))?
            .retired = true;
        #[cfg(test)]
        inject_close_for_handoff_fault(&self.path)?;
        self.ensure_no_pending_apply()?;
        self.with_connection(checkpoint_truncate)?;
        self.close_connection()?;
        cleanup_empty_wal_sidecars_after_owner_fence(&self.path)?;
        validate_control_database_pair(&self.path, &self.control)?;
        Ok(())
    }

    fn reopen_connection(&self) -> Result<()> {
        let reopened = open_connection(&self.path)?;
        let mut guard = self
            .conn
            .lock()
            .map_err(|_| Error::Sqlite("SQLite connection lock is poisoned".into()))?;
        if guard.is_some() {
            return Err(Error::Sqlite(
                "refusing to replace an open SQLite connection".into(),
            ));
        }
        *guard = Some(reopened);
        Ok(())
    }

    pub fn apply_entry(&self, entry: &LogEntry) -> Result<ApplyProgress> {
        Ok(self.apply_entry_with_result(entry)?.progress())
    }

    pub fn apply_entry_with_result(&self, entry: &LogEntry) -> Result<ApplyOutcome> {
        let _lifecycle = self.lock_lifecycle()?;
        self.ensure_page_state_sealed()?;
        if entry.recompute_hash() != entry.hash {
            return Err(Error::InvalidEntry(
                "hash does not match entry contents".into(),
            ));
        }
        let installed = *self
            .uncommitted_effect
            .lock()
            .map_err(|_| Error::Sqlite("uncommitted QWAL effect lock is poisoned".into()))?;
        if let Some(installed) = installed {
            if entry.entry_type != EntryType::Command
                || installed != LogHash::digest(&[&entry.payload])
            {
                return Err(Error::InvalidEntry(
                    "only the exact installed QWAL effect may retry after control failure".into(),
                ));
            }
        } else if self.read_state.pending_fence.load(Ordering::Acquire) {
            let incoming = LogAnchor::new(entry.index, entry.hash);
            if self.control.pending()?.map(|pending| pending.entry()) != Some(incoming) {
                return Err(Error::InvalidEntry(
                    "only the exact durable pending entry may retry physical apply".into(),
                ));
            }
        }
        self.apply_qwal_entry(entry)
    }

    pub fn apply_verified_external_effect(
        &self,
        verified: &VerifiedQwalEffectBundleV4,
    ) -> Result<ApplyOutcome> {
        let _lifecycle = self.lock_lifecycle()?;
        self.ensure_page_state_sealed()?;
        let command = verified.command();
        let payload = command
            .encode()
            .map_err(|error| Error::InvalidEntry(format!("QEFX encode failed: {error}")))?;
        let hash = LogEntry::calculate_hash(
            command.cluster_id(),
            command.intended_slot(),
            command.epoch(),
            command.config_id(),
            EntryType::Command,
            command.prev_hash(),
            &payload,
        );
        let entry = LogEntry {
            cluster_id: command.cluster_id().to_owned(),
            epoch: command.epoch(),
            config_id: command.config_id(),
            index: command.intended_slot(),
            entry_type: EntryType::Command,
            payload: payload.clone(),
            prev_hash: command.prev_hash(),
            hash,
        };
        let identity = self.control.identity()?;
        let configuration = self.control.configuration_state()?;
        if command.cluster_id() != identity.cluster_id()
            || command.epoch() != identity.epoch()
            || command.config_id() != configuration.config_id()
            || command.config_digest() != configuration.digest()
            || verified.manifest().recovery_generation != identity.recovery_generation()
            || verified.manifest().materializer_fingerprint
                != identity.materializer_fingerprint().to_hex()
        {
            return Err(Error::InvalidEntry(
                "verified external SQL effect identity mismatch".into(),
            ));
        }
        let tip = self.control.applied_tip()?;
        if entry.index == tip.applied_index() {
            if entry.hash != tip.applied_hash() {
                return Err(Error::InvalidEntry(
                    "external SQL slot was reapplied with a different hash".into(),
                ));
            }
            let result = if let [receipt] = verified.receipts() {
                self.control
                    .lookup_request(&receipt.request_id, receipt.request_digest)?
                    .map(|stored| decode_sql_result(stored.result_blob()))
                    .transpose()?
            } else {
                None
            };
            return Ok(ApplyOutcome {
                progress: tip,
                sql_result: result,
            });
        }
        if entry.index
            != tip
                .applied_index()
                .checked_add(1)
                .ok_or_else(|| Error::InvalidEntry("applied index is exhausted".into()))?
            || entry.prev_hash != tip.applied_hash()
        {
            return Err(Error::InvalidEntry(
                "external SQL effect does not extend the applied tip".into(),
            ));
        }
        configuration
            .validate_entry(&entry)
            .map_err(|error| Error::InvalidEntry(error.to_string()))?;
        let manifest = verified.manifest();
        if manifest.base_state != self.control.user_state()? {
            return Err(Error::InvalidEntry(
                "external SQL effect base does not match control state".into(),
            ));
        }
        let entry_anchor = LogAnchor::new(entry.index, entry.hash);
        let installed = *self
            .uncommitted_effect
            .lock()
            .map_err(|_| Error::Sqlite("uncommitted QWAL effect lock is poisoned".into()))?;
        if let Some(installed) = installed {
            if installed != LogHash::digest(&[&payload]) {
                return Err(Error::InvalidEntry(
                    "only the exact installed external SQL effect may retry".into(),
                ));
            }
        } else if self.read_state.pending_fence.load(Ordering::Acquire)
            && self.control.pending()?.map(|pending| pending.entry()) != Some(entry_anchor)
        {
            return Err(Error::InvalidEntry(
                "only the exact pending external SQL effect may retry".into(),
            ));
        }
        let needs_install =
            self.preverify_external_page_state_transition(verified, &payload, entry_anchor)?;
        let mut results = Vec::with_capacity(verified.receipts().len());
        let mut receipts = Vec::with_capacity(verified.receipts().len());
        for receipt in verified.receipts() {
            let result = decode_sql_result(&receipt.result_blob)?;
            if encode_sql_result(&result)? != receipt.result_blob {
                return Err(Error::InvalidEntry(
                    "external SQL result is not canonically encoded".into(),
                ));
            }
            results.push(result);
            receipts.push(RequestReceipt::new(
                receipt.request_id.clone(),
                receipt.request_digest,
                entry_anchor,
                receipt.result_blob.clone(),
            ));
        }
        let lookup_keys = verified
            .receipts()
            .iter()
            .map(|receipt| (receipt.request_id.as_str(), receipt.request_digest))
            .collect::<Vec<_>>();
        for (expected, existing) in receipts
            .iter()
            .zip(self.control.lookup_requests(&lookup_keys)?)
        {
            match existing? {
                None => {}
                Some(existing) if existing == *expected => {}
                Some(_) => {
                    return Err(Error::InvalidEntry(
                        "external SQL receipt already belongs to another entry".into(),
                    ))
                }
            }
        }
        let pending = PendingApply::new(
            LogAnchor::new(tip.applied_index(), tip.applied_hash()),
            entry_anchor,
            manifest.base_state,
            manifest.target_state,
        );
        self.control.begin_pending_with_entry(&pending, &entry)?;
        self.read_state.pending_fence.store(true, Ordering::Release);
        self.install_verified_external_effect(verified, &payload, needs_install)?;
        #[cfg(test)]
        let control_commit = inject_qwal_control_fault(&self.path).and_then(|()| {
            self.control
                .commit_rebuildable_apply(&pending, &entry, &configuration, &receipts)
        });
        #[cfg(not(test))]
        let control_commit =
            self.control
                .commit_rebuildable_apply(&pending, &entry, &configuration, &receipts);
        if let Err(error) = control_commit {
            let _ = self.close_connection();
            return Err(error);
        }
        self.read_state
            .pending_fence
            .store(false, Ordering::Release);
        *self
            .read_state
            .applied_tip
            .write()
            .map_err(|_| Error::Sqlite("SQLite applied-tip lock is poisoned".into()))? =
            ApplyProgress::new(entry.index, entry.hash);
        self.uncommitted_effect
            .lock()
            .map_err(|_| Error::Sqlite("uncommitted QWAL effect lock is poisoned".into()))?
            .take();
        Ok(ApplyOutcome {
            progress: ApplyProgress::new(entry.index, entry.hash),
            sql_result: if results.len() == 1 {
                results.pop()
            } else {
                None
            },
        })
    }

    fn apply_qwal_entry(&self, entry: &LogEntry) -> Result<ApplyOutcome> {
        let identity = self.control.identity()?;
        if entry.cluster_id != identity.cluster_id() {
            return Err(Error::InvalidEntry(
                "cluster_id does not match sidecar".into(),
            ));
        }
        if entry.epoch != identity.epoch() {
            return Err(Error::InvalidEntry("epoch does not match sidecar".into()));
        }
        let tip = self.control.applied_tip()?;
        if entry.index == tip.applied_index() {
            if entry.hash != tip.applied_hash() {
                return Err(Error::InvalidEntry(
                    "current index was reapplied with a different hash".into(),
                ));
            }
            let sql_result = if entry.entry_type == EntryType::Command {
                let effect = decode_qwal_command(&entry.payload)?;
                if let [effect_receipt] = effect.receipts.as_slice() {
                    self.control
                        .lookup_request(&effect_receipt.request_id, effect_receipt.request_digest)?
                        .map(|receipt| decode_sql_result(receipt.result_blob()))
                        .transpose()?
                } else {
                    None
                }
            } else {
                None
            };
            return Ok(ApplyOutcome {
                progress: tip,
                sql_result,
            });
        }
        let next_index = tip
            .applied_index()
            .checked_add(1)
            .ok_or_else(|| Error::InvalidEntry("applied index is exhausted".into()))?;
        if entry.index != next_index || entry.prev_hash != tip.applied_hash() {
            return Err(Error::InvalidEntry(
                "entry does not extend the QWAL applied tip".into(),
            ));
        }
        let current_configuration = self.control.configuration_state()?;
        let next_configuration = current_configuration
            .validate_entry(entry)
            .map_err(|error| Error::InvalidEntry(error.to_string()))?;
        let base_anchor = LogAnchor::new(tip.applied_index(), tip.applied_hash());
        let entry_anchor = LogAnchor::new(entry.index, entry.hash);

        if entry.entry_type != EntryType::Command {
            match entry.entry_type {
                EntryType::Noop if entry.payload.is_empty() => {}
                EntryType::ConfigChange => {}
                EntryType::Noop => {
                    return Err(Error::InvalidEntry("Noop payload must be empty".into()))
                }
                _ => {
                    return Err(Error::InvalidEntry(format!(
                        "entry type {:?} is unsupported by QWAL",
                        entry.entry_type
                    )))
                }
            }
            let user_state = self.control.user_state()?;
            if entry.entry_type == EntryType::Noop && next_configuration == current_configuration {
                self.control.commit_metadata_only_entry_with_log(
                    base_anchor,
                    entry,
                    &current_configuration,
                    user_state,
                )?;
            } else {
                let pending = PendingApply::new(base_anchor, entry_anchor, user_state, user_state);
                #[cfg(test)]
                inject_pending_begin_fault(&self.path)?;
                self.control.begin_pending_with_entry(&pending, entry)?;
                self.read_state.pending_fence.store(true, Ordering::Release);
                #[cfg(test)]
                inject_pending_commit_fault(&self.path)?;
                self.control
                    .commit_applied(&pending, &next_configuration, &[])?;
                self.read_state
                    .pending_fence
                    .store(false, Ordering::Release);
            }
            *self
                .read_state
                .applied_tip
                .write()
                .map_err(|_| Error::Sqlite("SQLite applied-tip lock is poisoned".into()))? =
                ApplyProgress::new(entry.index, entry.hash);
            return Ok(ApplyOutcome {
                progress: ApplyProgress::new(entry.index, entry.hash),
                sql_result: None,
            });
        }

        let effect = decode_qwal_command(&entry.payload)?;
        validate_qwal_identity(&effect, &identity, &current_configuration)?;
        if effect.base_index != tip.applied_index() || effect.base_hash != tip.applied_hash() {
            return Err(Error::InvalidEntry(
                "QWAL effect base does not match the applied tip".into(),
            ));
        }
        let mut results = Vec::with_capacity(effect.receipts.len());
        let mut receipts = Vec::with_capacity(effect.receipts.len());
        for effect_receipt in &effect.receipts {
            let result = decode_sql_result(&effect_receipt.result_blob)?;
            if encode_sql_result(&result)? != effect_receipt.result_blob {
                return Err(Error::InvalidEntry(
                    "QWAL result is not canonically encoded".into(),
                ));
            }
            results.push(result);
            receipts.push(RequestReceipt::new(
                effect_receipt.request_id.clone(),
                effect_receipt.request_digest,
                entry_anchor,
                effect_receipt.result_blob.clone(),
            ));
        }
        let lookup_keys = effect
            .receipts
            .iter()
            .map(|receipt| (receipt.request_id.as_str(), receipt.request_digest))
            .collect::<Vec<_>>();
        let existing = self.control.lookup_requests(&lookup_keys)?;
        for (expected, existing) in receipts.iter().zip(existing) {
            match existing? {
                None => {}
                Some(existing) if existing == *expected => {}
                Some(_) => {
                    return Err(Error::InvalidEntry(
                        "QWAL request receipt already belongs to another entry or result".into(),
                    ));
                }
            }
        }
        let pending = PendingApply::new(
            base_anchor,
            entry_anchor,
            effect.base_state,
            effect.target_state,
        );
        let needs_install =
            self.preverify_page_state_transition(&effect, &entry.payload, entry_anchor)?;
        self.control.begin_pending_with_entry(&pending, entry)?;
        self.read_state.pending_fence.store(true, Ordering::Release);
        self.install_qwal_effect(&effect, &entry.payload, needs_install)?;
        #[cfg(test)]
        let control_commit = inject_qwal_control_fault(&self.path).and_then(|()| {
            self.control
                .commit_rebuildable_apply(&pending, entry, &next_configuration, &receipts)
        });
        #[cfg(not(test))]
        let control_commit =
            self.control
                .commit_rebuildable_apply(&pending, entry, &next_configuration, &receipts);
        if let Err(error) = control_commit {
            let _ = self.close_connection();
            return Err(error);
        }
        self.read_state
            .pending_fence
            .store(false, Ordering::Release);
        *self
            .read_state
            .applied_tip
            .write()
            .map_err(|_| Error::Sqlite("SQLite applied-tip lock is poisoned".into()))? =
            ApplyProgress::new(entry.index, entry.hash);
        self.uncommitted_effect
            .lock()
            .map_err(|_| Error::Sqlite("uncommitted QWAL effect lock is poisoned".into()))?
            .take();
        Ok(ApplyOutcome {
            progress: ApplyProgress::new(entry.index, entry.hash),
            sql_result: if results.len() == 1 {
                results.pop()
            } else {
                None
            },
        })
    }

    fn install_qwal_effect(
        &self,
        effect: &QwalEnvelopeV3,
        effect_payload: &[u8],
        needs_install: bool,
    ) -> Result<()> {
        if !needs_install {
            self.remember_uncommitted_effect(effect_payload)?;
            if self
                .conn
                .lock()
                .map_err(|_| Error::Sqlite("SQLite connection lock is poisoned".into()))?
                .is_none()
            {
                self.reopen_connection()?;
            }
            return Ok(());
        };
        #[cfg(test)]
        inject_before_install_checkpoint_fault(&self.path)?;
        self.with_connection(checkpoint_truncate)?;
        self.close_connection()?;
        let mut canonical = self.open_bound_canonical()?;
        qwal::apply_preverified_qwal_in_place(&mut canonical, effect, |page_no| {
            #[cfg(test)]
            inject_qwal_apply_fault(&self.path, page_no)?;
            #[cfg(not(test))]
            let _ = page_no;
            Ok(())
        })?;
        self.publish_page_state_after_install(effect, &canonical)?;
        self.remember_uncommitted_effect(effect_payload)?;
        #[cfg(test)]
        note_prepared_install(&self.path, PreparedInstallPath::Patched);
        self.reopen_connection()
    }

    fn install_verified_external_effect(
        &self,
        effect: &VerifiedQwalEffectBundleV4,
        effect_payload: &[u8],
        needs_install: bool,
    ) -> Result<()> {
        if !needs_install {
            self.remember_uncommitted_effect(effect_payload)?;
            if self
                .conn
                .lock()
                .map_err(|_| Error::Sqlite("SQLite connection lock is poisoned".into()))?
                .is_none()
            {
                self.reopen_connection()?;
            }
            return Ok(());
        }
        #[cfg(test)]
        inject_before_install_checkpoint_fault(&self.path)?;
        self.with_connection(checkpoint_truncate)?;
        self.close_connection()?;
        let mut canonical = self.open_bound_canonical()?;
        qwal::apply_verified_qwal_effect_in_place(&mut canonical, effect, |page_no| {
            #[cfg(test)]
            inject_qwal_apply_fault(&self.path, page_no)?;
            #[cfg(not(test))]
            let _ = page_no;
            Ok(())
        })?;
        self.publish_external_page_state_after_install(effect, &canonical)?;
        self.remember_uncommitted_effect(effect_payload)?;
        self.reopen_connection()
    }

    fn preverify_external_page_state_transition(
        &self,
        effect: &VerifiedQwalEffectBundleV4,
        effect_payload: &[u8],
        entry_anchor: LogAnchor,
    ) -> Result<bool> {
        let manifest = effect.manifest();
        let page_state = self
            .page_state
            .lock()
            .map_err(|_| Error::Sqlite("SQLite page-state cache lock is poisoned".into()))?;
        verify_bound_canonical(&self.path, &page_state)?;
        let current = page_state.cache.identity();
        if current == manifest.target_state && current != manifest.base_state {
            let digest = LogHash::digest(&[effect_payload]);
            let exact_in_process_replay =
                *self.uncommitted_effect.lock().map_err(|_| {
                    Error::Sqlite("uncommitted QWAL effect lock is poisoned".into())
                })? == Some(digest);
            let durable_replay = self
                .control
                .pending()?
                .is_some_and(|pending| pending.entry() == entry_anchor);
            if !exact_in_process_replay && !durable_replay {
                return Err(Error::InvalidEntry(
                    "external SQL target is installed without exact replay state".into(),
                ));
            }
            return Ok(false);
        }
        if current != manifest.base_state {
            return Err(Error::InvalidEntry(
                "external SQL base page state does not match canonical state".into(),
            ));
        }
        let patches = effect
            .page_effect()
            .pages
            .iter()
            .map(|page| PageStatePatchV3::new(page.page_no, &page.after_image))
            .collect::<Vec<_>>();
        if page_state
            .cache
            .overlay(manifest.target_state.page_count, &patches)?
            != manifest.target_state
        {
            return Err(Error::InvalidEntry(
                "external SQL target page state mismatch".into(),
            ));
        }
        Ok(current != manifest.target_state)
    }

    fn publish_external_page_state_after_install(
        &self,
        effect: &VerifiedQwalEffectBundleV4,
        canonical: &File,
    ) -> Result<()> {
        let seal = seal_held_canonical(&self.path, canonical)?;
        let manifest = effect.manifest();
        let patches = effect
            .page_effect()
            .pages
            .iter()
            .map(|page| PageStatePatchV3::new(page.page_no, &page.after_image))
            .collect::<Vec<_>>();
        let mut page_state = self
            .page_state
            .lock()
            .map_err(|_| Error::Sqlite("SQLite page-state cache lock is poisoned".into()))?;
        if page_state
            .cache
            .apply_patch(manifest.target_state.page_count, &patches)?
            != manifest.target_state
        {
            return Err(Error::InvalidEntry(
                "installed external SQL target page state invariant failed".into(),
            ));
        }
        page_state.seal = seal;
        Ok(())
    }

    fn preverify_page_state_transition(
        &self,
        effect: &QwalEnvelopeV3,
        effect_payload: &[u8],
        entry_anchor: LogAnchor,
    ) -> Result<bool> {
        let page_state = self
            .page_state
            .lock()
            .map_err(|_| Error::Sqlite("SQLite page-state cache lock is poisoned".into()))?;
        verify_bound_canonical(&self.path, &page_state)?;
        let current = page_state.cache.identity();
        if current == effect.target_state && current != effect.base_state {
            let digest = LogHash::digest(&[effect_payload]);
            let installed = self
                .uncommitted_effect
                .lock()
                .map_err(|_| Error::Sqlite("uncommitted QWAL effect lock is poisoned".into()))?;
            let exact_in_process_replay = *installed == Some(digest);
            drop(installed);
            let durable_replay = if exact_in_process_replay
                || !self.read_state.pending_fence.load(Ordering::Acquire)
            {
                false
            } else {
                self.control.pending()?.is_some_and(|pending| {
                    pending.base_state() == effect.base_state
                        && pending.target_state() == effect.target_state
                        && pending.entry() == entry_anchor
                })
            };
            if !exact_in_process_replay && !durable_replay {
                return Err(Error::InvalidEntry(
                    "QWAL target is installed without an exact in-process replay seal".into(),
                ));
            }
            return Ok(false);
        }
        if current != effect.base_state {
            return Err(Error::InvalidEntry(
                "QWAL base page state does not match the canonical cache".into(),
            ));
        }
        let patches = effect
            .pages
            .iter()
            .map(|page| PageStatePatchV3::new(page.page_no, &page.after_image))
            .collect::<Vec<_>>();
        let target = page_state
            .cache
            .overlay(effect.target_state.page_count, &patches)?;
        if target != effect.target_state {
            return Err(Error::InvalidEntry(
                "QWAL target page state mismatch".into(),
            ));
        }
        Ok(current != effect.target_state)
    }

    fn ensure_page_state_sealed(&self) -> Result<()> {
        let page_state = self
            .page_state
            .lock()
            .map_err(|_| Error::Sqlite("SQLite page-state cache lock is poisoned".into()))?;
        verify_bound_canonical(&self.path, &page_state)
    }

    fn open_bound_canonical(&self) -> Result<File> {
        if !sqlite_sidecars_absent(&self.path)? {
            return Err(Error::InvalidEntry(
                "closed canonical SQLite database still has WAL sidecars".into(),
            ));
        }
        let page_state = self
            .page_state
            .lock()
            .map_err(|_| Error::Sqlite("SQLite page-state cache lock is poisoned".into()))?;
        open_bound_canonical(&self.path, &page_state)
    }

    fn publish_page_state_after_install(
        &self,
        effect: &QwalEnvelopeV3,
        canonical: &File,
    ) -> Result<()> {
        let seal = seal_held_canonical(&self.path, canonical)?;
        let mut page_state = self
            .page_state
            .lock()
            .map_err(|_| Error::Sqlite("SQLite page-state cache lock is poisoned".into()))?;
        let patches = effect
            .pages
            .iter()
            .map(|page| PageStatePatchV3::new(page.page_no, &page.after_image))
            .collect::<Vec<_>>();
        let target = page_state
            .cache
            .apply_patch(effect.target_state.page_count, &patches)?;
        if target != effect.target_state {
            return Err(Error::InvalidEntry(
                "installed QWAL target page state invariant failed".into(),
            ));
        }
        page_state.seal = seal;
        Ok(())
    }

    fn remember_uncommitted_effect(&self, effect_payload: &[u8]) -> Result<()> {
        *self
            .uncommitted_effect
            .lock()
            .map_err(|_| Error::Sqlite("uncommitted QWAL effect lock is poisoned".into()))? =
            Some(LogHash::digest(&[effect_payload]));
        Ok(())
    }

    pub fn get_value(&self, key: &str) -> Result<Option<String>> {
        let _lifecycle = self.lock_read_lifecycle()?;
        self.ensure_no_pending_apply()?;
        let pooled = self.read_state.checkout(&self.path)?;
        pooled
            .connection()
            .query_row(
                "SELECT value FROM __rhiza_kv WHERE key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_error)
    }

    pub fn query_sql(
        &self,
        query: &SqlStatement,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<SqlQueryResult> {
        self.query_sql_with_timeout(query, max_rows, max_bytes, DEFAULT_SQL_QUERY_TIMEOUT)
    }

    pub fn query_sql_with_timeout(
        &self,
        query: &SqlStatement,
        max_rows: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<SqlQueryResult> {
        self.read_handle()
            .query_sql_with_timeout(query, max_rows, max_bytes, timeout)
    }

    pub fn read_handle(&self) -> SqlReadHandle {
        SqlReadHandle {
            path: self.path.clone(),
            state: Arc::clone(&self.read_state),
        }
    }

    pub fn validate_sql_write(&self, command: &SqlCommand) -> Result<()> {
        let request = encode_sql_command(command)?;
        let tip = self.control.applied_tip()?;
        let preparation = self.prepare_sql_batch_effect(
            &[SqlBatchMember {
                command,
                request_payload: &request,
            }],
            tip.applied_index(),
            tip.applied_hash(),
        )?;
        preparation
            .results
            .into_iter()
            .next()
            .expect("one-member batch returns one result")
            .map(|_| ())
    }

    /// Prepares one physical QWAL v3 effect for the successful subset of an
    /// ordered SQL batch.
    ///
    /// Each member runs inside its own savepoint nested under one outer SQLite
    /// transaction. A failed member is rolled back and reported at the same
    /// input position; later members still observe all prior successful
    /// members. Every successful member is bound into the effect as an ordered
    /// receipt template and is committed at the effect's single log anchor.
    pub fn prepare_sql_batch_effect(
        &self,
        members: &[SqlBatchMember<'_>],
        base_index: LogIndex,
        base_hash: LogHash,
    ) -> Result<SqlBatchPreparation> {
        let (effect, results, _) = self.prepare_sql_batch_envelope(
            members,
            base_index,
            base_hash,
            MAX_QWAL_V3_BYTES,
            MAX_NATIVE_WAL_CAPTURE_BYTES,
        )?;
        Ok(SqlBatchPreparation {
            effect: effect.map(|effect| encode_qwal_v3(&effect)).transpose()?,
            results,
        })
    }

    pub fn prepare_external_sql_batch_effect(
        &self,
        members: &[SqlBatchMember<'_>],
        base_index: LogIndex,
        base_hash: LogHash,
    ) -> Result<ExternalSqlBatchPreparation> {
        let (effect, results, configuration_digest) = self.prepare_sql_batch_envelope(
            members,
            base_index,
            base_hash,
            MAX_EXTERNAL_EFFECT_BYTES,
            MAX_EXTERNAL_NATIVE_WAL_CAPTURE_BYTES,
        )?;
        let effect = effect
            .map(|effect| PreparedExternalSqlEffect::from_effect(effect, configuration_digest))
            .transpose()?;
        Ok(ExternalSqlBatchPreparation { effect, results })
    }

    fn prepare_sql_batch_envelope(
        &self,
        members: &[SqlBatchMember<'_>],
        base_index: LogIndex,
        base_hash: LogHash,
        max_changed_bytes: usize,
        max_raw_wal_bytes: u64,
    ) -> Result<(
        Option<QwalEnvelopeV3>,
        Vec<Result<SqlCommandResult>>,
        LogHash,
    )> {
        if members.is_empty() || members.len() > MAX_QWAL_V3_RECEIPTS {
            return Err(Error::InvalidCommand(format!(
                "SQL batch must contain 1..={MAX_QWAL_V3_RECEIPTS} members"
            )));
        }
        self.prepare_qwal_effect(
            base_index,
            base_hash,
            max_changed_bytes,
            max_raw_wal_bytes,
            |staging| {
                let mut preflight = std::iter::repeat_with(|| None)
                    .take(members.len())
                    .collect::<Vec<Option<Result<Option<RequestReceipt>>>>>();
                let mut lookup_members: Vec<usize> = Vec::with_capacity(members.len());
                let mut lookup_keys = Vec::with_capacity(members.len());
                let mut seen_request_ids = HashSet::with_capacity(members.len());
                for (index, member) in members.iter().enumerate() {
                    let request_digest = LogHash::digest(&[member.request_payload]);
                    let validation = validate_sql_command(member.command).and_then(|()| {
                        if decode_sql_command(member.request_payload)? != *member.command {
                            return Err(Error::InvalidCommand(
                                "SQL batch member is not the canonical QSQL v2 command".into(),
                            ));
                        }
                        if !seen_request_ids.insert(member.command.request_id.as_str()) {
                            return Err(Error::InvalidCommand(
                                "SQL batch member repeats a request_id".into(),
                            ));
                        }
                        Ok(())
                    });
                    if let Err(error) = validation {
                        preflight[index] = Some(Err(error));
                        continue;
                    }
                    lookup_members.push(index);
                    lookup_keys.push((member.command.request_id.as_str(), request_digest));
                }
                for (member_index, lookup) in lookup_members
                    .iter()
                    .copied()
                    .zip(self.control.lookup_requests(&lookup_keys)?)
                {
                    preflight[member_index] = Some(lookup);
                }

                let mut tx = Transaction::new_unchecked(staging, TransactionBehavior::Immediate)
                    .map_err(sqlite_error)?;
                let mut receipts = Vec::with_capacity(members.len());
                let mut results = Vec::with_capacity(members.len());
                let mut result_bytes = 0usize;
                let mut result_budget_exhausted = false;
                for (member, preflight) in members.iter().zip(preflight) {
                    let request_digest = LogHash::digest(&[member.request_payload]);
                    match preflight.expect("every SQL batch member has one preflight result") {
                        Err(error) => {
                            results.push(Err(error));
                            continue;
                        }
                        Ok(Some(_)) => {
                            results.push(Err(Error::InvalidCommand(
                                "request was already materialized; return its stored receipt"
                                    .into(),
                            )));
                            continue;
                        }
                        Ok(None) => {}
                    }
                    if result_budget_exhausted {
                        results.push(Err(Error::ResourceExhausted(format!(
                            "SQL batch results exceed {MAX_QWAL_V3_BYTES} bytes"
                        ))));
                        continue;
                    }

                    let savepoint = tx.savepoint().map_err(sqlite_error)?;
                    match execute_sql_statements(&savepoint, &member.command.statements)
                        .and_then(|result| encode_sql_result(&result).map(|blob| (result, blob)))
                    {
                        Ok((result, result_blob)) => {
                            let next_result_bytes =
                                result_bytes.checked_add(result_blob.len()).ok_or_else(|| {
                                    Error::ResourceExhausted(
                                        "SQL batch result bytes overflow".into(),
                                    )
                                })?;
                            if next_result_bytes > MAX_QWAL_V3_BYTES {
                                savepoint.finish().map_err(sqlite_error)?;
                                result_budget_exhausted = true;
                                results.push(Err(Error::ResourceExhausted(format!(
                                    "SQL batch results exceed {MAX_QWAL_V3_BYTES} bytes"
                                ))));
                                continue;
                            }
                            savepoint.commit().map_err(sqlite_error)?;
                            result_bytes = next_result_bytes;
                            receipts.push(QwalReceiptV3 {
                                request_id: member.command.request_id.clone(),
                                request_digest,
                                result_blob,
                            });
                            results.push(Ok(result));
                        }
                        Err(error) => {
                            savepoint.finish().map_err(sqlite_error)?;
                            results.push(Err(error));
                        }
                    }
                }
                if receipts.is_empty() {
                    tx.rollback().map_err(sqlite_error)?;
                } else {
                    tx.commit().map_err(sqlite_error)?;
                }
                Ok(PreparedQwalMutation { receipts, results })
            },
        )
    }

    pub fn prepare_put_effect(
        &self,
        request_id: &str,
        key: &str,
        value: &str,
        request_payload: &[u8],
        base_index: LogIndex,
        base_hash: LogHash,
    ) -> Result<Vec<u8>> {
        let (effect, _, _) = self.prepare_put_envelope(
            request_id,
            key,
            value,
            request_payload,
            base_index,
            base_hash,
            MAX_QWAL_V3_BYTES,
            MAX_NATIVE_WAL_CAPTURE_BYTES,
        )?;
        encode_qwal_v3(&effect)
    }

    pub fn prepare_external_put_effect(
        &self,
        request_id: &str,
        key: &str,
        value: &str,
        request_payload: &[u8],
        base_index: LogIndex,
        base_hash: LogHash,
    ) -> Result<PreparedExternalSqlEffect> {
        let (effect, results, configuration_digest) = self.prepare_put_envelope(
            request_id,
            key,
            value,
            request_payload,
            base_index,
            base_hash,
            MAX_EXTERNAL_EFFECT_BYTES,
            MAX_EXTERNAL_NATIVE_WAL_CAPTURE_BYTES,
        )?;
        debug_assert_eq!(results.len(), 1);
        PreparedExternalSqlEffect::from_effect(effect, configuration_digest)
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_put_envelope(
        &self,
        request_id: &str,
        key: &str,
        value: &str,
        request_payload: &[u8],
        base_index: LogIndex,
        base_hash: LogHash,
        max_changed_bytes: usize,
        max_raw_wal_bytes: u64,
    ) -> Result<(QwalEnvelopeV3, Vec<Result<SqlCommandResult>>, LogHash)> {
        let canonical_request = encode_put_request(request_id, key, value)?;
        if request_payload != canonical_request {
            return Err(Error::InvalidCommand(
                "put effect request is not the canonical put command".into(),
            ));
        }
        let (effect, results, configuration_digest) = self.prepare_qwal_effect(
            base_index,
            base_hash,
            max_changed_bytes,
            max_raw_wal_bytes,
            |staging| {
                let request_digest = LogHash::digest(&[request_payload]);
                if self
                    .control
                    .lookup_request(request_id, request_digest)?
                    .is_some()
                {
                    return Err(Error::InvalidCommand(
                        "request was already materialized; return its stored receipt".into(),
                    ));
                }
                let tx = Transaction::new_unchecked(staging, TransactionBehavior::Immediate)
                    .map_err(sqlite_error)?;
                tx.execute(
                    "INSERT INTO __rhiza_kv(key, value) VALUES (?1, ?2)\n                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    params![key, value],
                )
                .map_err(sqlite_error)?;
                tx.commit().map_err(sqlite_error)?;
                let result = SqlCommandResult {
                    statement_results: Vec::new(),
                };
                Ok(PreparedQwalMutation {
                    receipts: vec![QwalReceiptV3 {
                        request_id: request_id.to_owned(),
                        request_digest,
                        result_blob: encode_sql_result(&result)?,
                    }],
                    results: vec![Ok(result)],
                })
            },
        )?;
        effect
            .map(|effect| (effect, results, configuration_digest))
            .ok_or_else(|| {
                Error::InvalidCommand(
                    "put effect unexpectedly produced no successful member".into(),
                )
            })
    }

    fn prepare_qwal_effect(
        &self,
        base_index: LogIndex,
        base_hash: LogHash,
        max_changed_bytes: usize,
        max_raw_wal_bytes: u64,
        mutation: impl FnOnce(&mut Connection) -> Result<PreparedQwalMutation>,
    ) -> Result<(
        Option<QwalEnvelopeV3>,
        Vec<Result<SqlCommandResult>>,
        LogHash,
    )> {
        #[cfg(not(unix))]
        return Err(Error::Sqlite(
            "canonical WAL speculative preparation requires Unix inode identity".into(),
        ));
        let _lifecycle = self.lock_lifecycle()?;
        self.ensure_page_state_sealed()?;
        self.ensure_no_pending_apply()?;
        let tip = self.control.applied_tip()?;
        if tip != ApplyProgress::new(base_index, base_hash) {
            return Err(Error::InvalidEntry(
                "QWAL effect base does not match the materialized SQLite tip".into(),
            ));
        }
        let identity = self.control.identity()?;
        let configuration_digest = identity.configuration_state().digest();
        #[cfg(test)]
        if max_changed_bytes == MAX_EXTERNAL_EFFECT_BYTES {
            gate_external_prepare(&self.path)?;
        }
        let base_state = self
            .page_state
            .lock()
            .map_err(|_| Error::Sqlite("SQLite page-state cache lock is poisoned".into()))?
            .cache
            .identity();
        if base_state != identity.user_state() {
            return Err(Error::InvalidEntry(
                "cached SQLite base state does not match the control sidecar".into(),
            ));
        }

        let prepare_result = (|| {
            self.close_connection()?;
            cleanup_empty_wal_sidecars_after_owner_fence(&self.path)?;

            let (base_file, base_seal) = open_sealed_prepared_base(&self.path)?;
            {
                let page_state = self.page_state.lock().map_err(|_| {
                    Error::Sqlite("SQLite page-state cache lock is poisoned".into())
                })?;
                if !prepared_base_metadata_matches(
                    page_state.seal.as_ref(),
                    &base_file.metadata().map_err(io_error)?,
                    &fs::symlink_metadata(&self.path).map_err(io_error)?,
                ) {
                    return Err(Error::InvalidEntry(
                        "closed SQLite base no longer matches the cached page-state seal".into(),
                    ));
                }
            }
            let base_file_bytes = fs::metadata(&self.path).map_err(io_error)?.len();
            let page_size = base_state.page_size;
            let base_db_pages = u32::try_from(base_file_bytes / u64::from(page_size))
                .map_err(|_| Error::ResourceExhausted("SQLite base page count overflows".into()))?;
            let sidecar_cleanup = StagingSidecarCleanup::new(&self.path);
            #[cfg(test)]
            note_speculative_staging(&self.path, 0, 0);
            let mut staging = open_connection(&self.path)?;
            if !staging
                .set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)
                .map_err(sqlite_error)?
                || !staging
                    .db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE)
                    .map_err(sqlite_error)?
            {
                return Err(Error::Sqlite(
                    "SQLite refused to disable checkpoint-on-close for QWAL capture".into(),
                ));
            }
            #[cfg(test)]
            note_native_wal_capture(&self.path);
            staging
                .pragma_update(None, "synchronous", "OFF")
                .map_err(sqlite_error)?;
            #[cfg(test)]
            note_speculative_synchronous(&self.path, &staging)?;
            let mutation = mutation(&mut staging)?;
            if mutation.receipts.is_empty() {
                staging
                    .close()
                    .map_err(|(_, error)| Error::Sqlite(error.to_string()))?;
                sidecar_cleanup.cleanup()?;
                verify_speculative_base_unchanged(
                    &self.path,
                    &base_file,
                    base_seal.as_ref(),
                    base_state,
                )?;
                return Ok((None, mutation.results));
            }
            let held_wal = open_fresh_staging_wal(&self.path)?;
            staging
                .close()
                .map_err(|(_, error)| Error::Sqlite(error.to_string()))?;
            #[cfg(test)]
            inject_wal_capture_fault(&self.path, held_wal.as_ref())?;
            let capture = match held_wal {
                Some((mut wal, seal)) => {
                    verify_staging_wal_seal(&self.path, &wal, seal)?;
                    let capture = capture_wal(
                        &mut wal,
                        base_db_pages,
                        max_changed_bytes,
                        max_raw_wal_bytes,
                    )?;
                    verify_staging_wal_seal(&self.path, &wal, seal)?;
                    capture
                }
                None => WalCapture::NoChange,
            };
            let (pages, target_page_count) =
                qwal_pages_from_capture(&base_file, page_size, base_file_bytes, capture)?;
            sidecar_cleanup.cleanup()?;
            verify_speculative_base_unchanged(
                &self.path,
                &base_file,
                base_seal.as_ref(),
                base_state,
            )?;
            let patches = pages
                .iter()
                .map(|page| PageStatePatchV3::new(page.page_no, &page.after_image))
                .collect::<Vec<_>>();
            let target_state = self
                .page_state
                .lock()
                .map_err(|_| Error::Sqlite("SQLite page-state cache lock is poisoned".into()))?
                .cache
                .overlay(target_page_count, &patches)?;

            let effect = QwalEnvelopeV3 {
                cluster_id: identity.cluster_id().to_owned(),
                epoch: identity.epoch(),
                configuration_id: identity.configuration_state().config_id(),
                recovery_generation: identity.recovery_generation(),
                base_index,
                base_hash,
                base_state,
                target_state,
                materializer_fingerprint: identity.materializer_fingerprint().to_hex(),
                receipts: mutation.receipts,
                pages,
            };
            Ok((Some(effect), mutation.results))
        })();

        let connection_closed = self
            .conn
            .lock()
            .map_err(|_| Error::Sqlite("SQLite connection lock is poisoned".into()))?
            .is_none();
        if connection_closed {
            cleanup_speculative_sidecars(&self.path)?;
            let sealed = rebuild_sealed_page_state(&self.path)?;
            if sealed.cache.identity() != base_state || self.control.user_state()? != base_state {
                return Err(Error::InvalidEntry(
                    "canonical SQLite base changed during speculative WAL preparation".into(),
                ));
            }
            *self
                .page_state
                .lock()
                .map_err(|_| Error::Sqlite("SQLite page-state cache lock is poisoned".into()))? =
                sealed;
            self.reopen_connection()?;
        }
        let (effect, results) = prepare_result?;
        Ok((effect, results, configuration_digest))
    }

    pub fn check_request(
        &self,
        request_id: &str,
        command_payload: &[u8],
    ) -> Result<Option<RequestOutcome>> {
        let _lifecycle = self.lock_lifecycle()?;
        self.with_connection(|_| Ok(()))?;
        self.ensure_no_pending_apply()?;
        let Some(receipt) = self
            .control
            .lookup_request(request_id, LogHash::digest(&[command_payload]))?
        else {
            return Ok(None);
        };
        Ok(Some(RequestOutcome::new(
            receipt.original_anchor().index(),
            receipt.original_anchor().hash(),
        )))
    }

    pub fn connection_pragmas(&self) -> Result<(String, i64)> {
        let _lifecycle = self.lock_lifecycle()?;
        self.ensure_no_pending_apply()?;
        self.with_connection(|conn| {
            let journal_mode = conn
                .query_row("PRAGMA journal_mode;", [], |row| row.get(0))
                .map_err(sqlite_error)?;
            let synchronous = conn
                .query_row("PRAGMA synchronous;", [], |row| row.get(0))
                .map_err(sqlite_error)?;
            Ok((journal_mode, synchronous))
        })
    }

    pub fn check_sql_request(
        &self,
        request_id: &str,
        command_payload: &[u8],
    ) -> Result<Option<(RequestOutcome, Option<SqlCommandResult>)>> {
        self.check_sql_requests(&[(request_id, command_payload)])?
            .pop()
            .expect("one SQL request produces one aligned lookup")
    }

    /// Checks unique SQL request ids with one bounded control-sidecar query.
    ///
    /// Results preserve input order. A missing receipt is `Ok(None)`, an exact
    /// receipt is decoded into its original outcome and result, and a digest
    /// conflict or invalid payload is returned in that member's aligned slot.
    /// Duplicate request ids are rejected before querying; callers that accept
    /// aliases must deduplicate by id and fan the aligned result back out.
    pub fn check_sql_requests(&self, requests: &[(&str, &[u8])]) -> Result<Vec<SqlRequestLookup>> {
        let _lifecycle = self.lock_lifecycle()?;
        self.with_connection(|_| Ok(()))?;
        self.ensure_no_pending_apply()?;
        let mut validations = Vec::with_capacity(requests.len());
        let mut lookup_keys = Vec::with_capacity(requests.len());
        for (request_id, command_payload) in requests {
            let validation = decode_sql_command(command_payload).and_then(|command| {
                if command.request_id != *request_id {
                    return Err(Error::InvalidCommand(
                        "SQL payload request_id does not match lookup request_id".into(),
                    ));
                }
                Ok(())
            });
            validations.push(validation);
            lookup_keys.push((*request_id, LogHash::digest(&[command_payload])));
        }
        let receipts = self.control.lookup_requests(&lookup_keys)?;
        let mut aligned = Vec::with_capacity(requests.len());
        for (validation, receipt) in validations.into_iter().zip(receipts) {
            let checked = match validation {
                Err(error) => Err(error),
                Ok(()) => match receipt {
                    Err(error) => Err(error),
                    Ok(None) => Ok(None),
                    Ok(Some(receipt)) => decode_sql_result(receipt.result_blob()).map(|result| {
                        Some((
                            RequestOutcome::new(
                                receipt.original_anchor().index(),
                                receipt.original_anchor().hash(),
                            ),
                            Some(result),
                        ))
                    }),
                },
            };
            aligned.push(checked);
        }
        Ok(aligned)
    }

    pub fn applied_index_value(&self) -> Result<LogIndex> {
        Ok(self.control.applied_tip()?.applied_index())
    }

    pub fn applied_hash_value(&self) -> Result<LogHash> {
        Ok(self.control.applied_tip()?.applied_hash())
    }

    /// Returns the applied index and hash observed by one control-store snapshot.
    pub fn applied_tip(&self) -> Result<ApplyProgress> {
        self.control.applied_tip()
    }

    pub fn applied_tip_value(&self) -> Result<(LogIndex, LogHash)> {
        let tip = self.applied_tip()?;
        Ok((tip.applied_index(), tip.applied_hash()))
    }

    pub fn configuration_state_value(&self) -> Result<ConfigurationState> {
        self.control.configuration_state()
    }

    pub fn embedded_log_entries(
        &self,
        from_index: LogIndex,
        through_index: LogIndex,
    ) -> Result<Vec<LogEntry>> {
        self.control.embedded_log_entries(from_index, through_index)
    }

    pub fn compact_embedded_log_before(&self, anchor_index: LogIndex) -> Result<()> {
        self.control.compact_embedded_log_before(anchor_index)
    }

    pub fn canonical_db_digest(&self) -> Result<LogHash> {
        let _lifecycle = self.lock_lifecycle()?;
        self.ensure_no_pending_apply()?;
        self.with_connection(checkpoint_truncate)?;
        self.close_connection()?;
        let digest = file_digest(&self.path);
        let reopen = self.reopen_connection();
        match (digest, reopen) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(digest), Ok(())) => Ok(digest),
        }
    }

    pub fn create_snapshot(&self, target: LogIndex) -> Result<Snapshot> {
        let _lifecycle = self.lock_lifecycle()?;
        self.ensure_page_state_sealed()?;
        self.ensure_no_pending_apply()?;
        let tip = self.control.applied_tip()?;
        if tip.applied_index() != target {
            return Err(Error::InvalidSnapshot(format!(
                "snapshot target {target} does not match applied index {}",
                tip.applied_index()
            )));
        }
        let identity = self.control.identity()?;
        let manifest = SnapshotManifest::new(
            identity.cluster_id(),
            identity.configuration_state().clone(),
            identity.epoch(),
            target,
            tip.applied_hash(),
            1,
            identity.node_id(),
            sql_executor_fingerprint()?,
        );
        self.with_connection(checkpoint_truncate)?;
        self.close_connection()?;
        let snapshot = (|| {
            let mut canonical = self.open_bound_canonical()?;
            canonical.seek(SeekFrom::Start(0)).map_err(io_error)?;
            let mut user_db = Vec::new();
            canonical.read_to_end(&mut user_db).map_err(io_error)?;
            seal_held_canonical(&self.path, &canonical)?;
            let page_state = page_state_from_database_bytes(&user_db)?;
            if page_state.identity() != identity.user_state() {
                return Err(Error::InvalidSnapshot(
                    "canonical database page state does not match control sidecar".into(),
                ));
            }
            let container = QwalSnapshotV3 {
                user_db,
                replicated_control: self.control.export_replicated_snapshot()?,
                user_state: page_state.identity(),
            };
            encode_qwal_snapshot(&container).map(|bytes| Snapshot::new(manifest, bytes))
        })();
        let reopen = self.reopen_connection();
        match (snapshot, reopen) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(snapshot), Ok(())) => Ok(snapshot),
        }
    }

    pub fn create_recovery_snapshot(&self, recovery_generation: u64) -> Result<RecoverySnapshot> {
        if recovery_generation == 0 {
            return Err(Error::InvalidSnapshot(
                "recovery_generation must be positive".into(),
            ));
        }
        let target = self.applied_index_value()?;
        if target == 0 {
            return Err(Error::InvalidSnapshot(
                "recovery snapshot requires an applied entry".into(),
            ));
        }
        let snapshot = self.create_snapshot(target)?;
        let manifest = snapshot.manifest();
        let size_bytes = u64::try_from(snapshot.db_bytes().len())
            .map_err(|_| Error::InvalidSnapshot("snapshot size exceeds u64".into()))?;
        let anchor = RecoveryAnchor::new(
            manifest.cluster_id(),
            manifest.epoch(),
            manifest.configuration_state().clone(),
            recovery_generation,
            LogAnchor::new(manifest.index(), manifest.applied_hash()),
            SnapshotIdentity::new(
                manifest.snapshot_id(),
                LogHash::digest(&[snapshot.db_bytes()]),
                size_bytes,
                manifest.executor_fingerprint(),
            ),
        );
        Ok(RecoverySnapshot { snapshot, anchor })
    }
}

pub fn encode_put_request(request_id: &str, key: &str, value: &str) -> Result<Vec<u8>> {
    if request_id.is_empty() || request_id.len() > 256 || key.is_empty() {
        return Err(Error::InvalidCommand(
            "put request_id and key must be non-empty and request_id at most 256 bytes".into(),
        ));
    }
    if [request_id, key, value]
        .iter()
        .any(|field| field.as_bytes().contains(&b'\t'))
    {
        return Err(Error::InvalidCommand(
            "put request fields must not contain a tab".into(),
        ));
    }
    Ok(format!("put\t{request_id}\t{key}\t{value}").into_bytes())
}

impl StateMachine for SqliteStateMachine {
    fn applied_index(&self) -> Result<LogIndex> {
        self.applied_index_value()
    }

    fn apply(&self, entry: &LogEntry) -> Result<ApplyProgress> {
        self.apply_entry(entry)
    }

    fn create_snapshot(&self, target: LogIndex) -> Result<Snapshot> {
        self.create_snapshot(target)
    }
}

pub fn restore_snapshot_file(
    path: impl AsRef<Path>,
    snapshot: &Snapshot,
    target_node_id: &str,
) -> Result<()> {
    restore_snapshot_file_with_recovery_generation(path, snapshot, target_node_id, None)
}

fn restore_snapshot_file_with_recovery_generation(
    path: impl AsRef<Path>,
    snapshot: &Snapshot,
    target_node_id: &str,
    recovery_generation: Option<u64>,
) -> Result<()> {
    if target_node_id.is_empty() {
        return Err(Error::InvalidSnapshot("target node_id is empty".into()));
    }
    let path = path.as_ref();
    ensure_parent(path)?;
    let parent = parent_dir(path);
    let control_path = control_sidecar_path(path);
    let mut wal_path = path.as_os_str().to_os_string();
    wal_path.push("-wal");
    let mut shm_path = path.as_os_str().to_os_string();
    shm_path.push("-shm");
    if path.exists()
        || control_path.exists()
        || Path::new(&wal_path).exists()
        || Path::new(&shm_path).exists()
    {
        return Err(Error::InvalidSnapshot(
            "QWAL restore destination and SQLite sidecars must not exist".into(),
        ));
    }
    let container = decode_qwal_snapshot(snapshot.db_bytes())?;
    if snapshot.manifest().executor_fingerprint() != sql_executor_fingerprint()? {
        return Err(Error::InvalidSnapshot(
            "QWAL snapshot materializer fingerprint does not match local SQLite".into(),
        ));
    }
    let mut restore_file = NamedTempFile::new_in(parent).map_err(io_error)?;
    restore_file
        .write_all(&container.user_db)
        .map_err(io_error)?;
    restore_file.as_file().sync_all().map_err(io_error)?;

    {
        let restore_conn = Connection::open_with_flags(
            restore_file.path(),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
        integrity_check(&restore_conn)?;
    }

    let page_state = page_state_from_database_bytes(&container.user_db)?;
    if page_state.identity() != container.user_state {
        return Err(Error::InvalidSnapshot(
            "QWAL snapshot database does not match its page state".into(),
        ));
    }
    let control_temp_dir = tempfile::tempdir_in(parent).map_err(io_error)?;
    let control_temp_path = control_temp_dir.path().join("control.sqlite");
    let control_identity = ControlIdentity::new(
        snapshot.manifest().cluster_id(),
        target_node_id,
        snapshot.manifest().epoch(),
        snapshot.manifest().configuration_state().clone(),
        1,
        sql_executor_fingerprint()?,
        container.user_state,
    );
    let control = ControlStore::create(&control_temp_path, &control_identity)?;
    control.import_replicated_snapshot_with_recovery_generation(
        &container.replicated_control,
        recovery_generation,
    )?;
    let imported_tip = control.applied_tip()?;
    if imported_tip.applied_index() != snapshot.manifest().index()
        || imported_tip.applied_hash() != snapshot.manifest().applied_hash()
        || control.configuration_state()? != *snapshot.manifest().configuration_state()
        || control.user_state()? != container.user_state
        || recovery_generation
            .is_some_and(|expected| control.recovery_generation().ok() != Some(expected))
    {
        return Err(Error::InvalidSnapshot(
            "QWAL snapshot manifest does not match replicated control state".into(),
        ));
    }
    drop(control);
    File::open(&control_temp_path)
        .and_then(|file| file.sync_all())
        .map_err(io_error)?;

    let restored = restore_file
        .persist_noclobber(path)
        .map_err(|err| Error::Io(err.error.to_string()))?;
    restored.sync_all().map_err(io_error)?;
    if let Err(error) = fs::hard_link(&control_temp_path, &control_path) {
        drop(restored);
        let cleanup = fs::remove_file(path);
        let synced = sync_parent(parent);
        if let Err(cleanup_error) = cleanup {
            return Err(Error::Io(format!(
                "control publish failed ({error}); database cleanup failed ({cleanup_error})"
            )));
        }
        synced?;
        return Err(io_error(error));
    }
    sync_parent(parent)
}

pub fn restore_recovery_snapshot_file(
    path: impl AsRef<Path>,
    db_bytes: &[u8],
    anchor: &RecoveryAnchor,
    target_node_id: &str,
) -> Result<()> {
    if target_node_id.is_empty() {
        return Err(Error::InvalidSnapshot("target node_id is empty".into()));
    }
    if anchor.snapshot().size_bytes() != db_bytes.len() as u64
        || anchor.snapshot().digest() != LogHash::digest(&[db_bytes])
    {
        return Err(Error::InvalidSnapshot(
            "recovery anchor does not match snapshot bytes".into(),
        ));
    }
    let fingerprint = anchor.executor_fingerprint();
    let expected = sql_executor_fingerprint()?;
    if fingerprint != expected {
        return Err(Error::InvalidSnapshot(format!(
            "recovery snapshot executor fingerprint {} does not match local {}",
            fingerprint.to_hex(),
            expected.to_hex()
        )));
    }
    let manifest = SnapshotManifest::new(
        anchor.cluster_id(),
        anchor.configuration_state().clone(),
        anchor.epoch(),
        anchor.compacted().index(),
        anchor.compacted().hash(),
        1,
        target_node_id,
        sql_executor_fingerprint()?,
    );
    if manifest.snapshot_id() != anchor.snapshot().snapshot_id() {
        return Err(Error::InvalidSnapshot(
            "recovery snapshot id does not match compacted index".into(),
        ));
    }
    restore_snapshot_file_with_recovery_generation(
        path,
        &Snapshot::new(manifest, db_bytes.to_vec()),
        target_node_id,
        Some(anchor.recovery_generation()),
    )
}

fn open_connection(path: &Path) -> Result<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = Connection::open_with_flags(path, flags).map_err(sqlite_error)?;
    let journal_mode: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(sqlite_error)?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(Error::Sqlite(format!(
            "SQLite refused WAL journal mode: {journal_mode}"
        )));
    }
    conn.pragma_update(None, "synchronous", "OFF")
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "trusted_schema", "OFF")
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "wal_autocheckpoint", 0)
        .map_err(sqlite_error)?;
    Ok(conn)
}

fn open_read_connection(path: &Path) -> Result<Connection> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(sqlite_error)
}

fn query_sql_read_only(
    conn: &Connection,
    query: &SqlStatement,
    max_rows: usize,
    max_bytes: usize,
    timeout: Duration,
) -> (Result<SqlQueryResult>, bool) {
    if let Err(error) = validate_sql_statement(query) {
        return (Err(error), true);
    }
    if max_rows == 0 || max_bytes == 0 {
        return (
            Err(Error::InvalidCommand(
                "SQL query limits must be positive".into(),
            )),
            true,
        );
    }
    let deadline = Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(Instant::now);
    if let Err(error) = conn
        .progress_handler(
            SQL_PROGRESS_HANDLER_OPS,
            Some(move || Instant::now() >= deadline),
        )
        .map_err(sqlite_error)
    {
        return (Err(error), false);
    }
    let result = with_sql_authorizer(conn, SqlAuthorizationMode::ReadOnly, || {
        let mut statement = conn.prepare(&query.sql).map_err(sql_query_error)?;
        if !statement.readonly() {
            return Err(Error::InvalidCommand("SQL query must be read-only".into()));
        }
        let columns = statement
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let column_count = columns.len();
        let mut rows = statement
            .query(params_from_iter(query.parameters.iter()))
            .map_err(sql_query_error)?;
        let mut result_rows = Vec::new();
        let mut result_bytes = columns.iter().map(String::len).sum::<usize>();
        while let Some(row) = rows.next().map_err(sql_query_error)? {
            if result_rows.len() == max_rows {
                return Err(Error::InvalidCommand(format!(
                    "SQL query exceeds {max_rows} rows"
                )));
            }
            let mut values = Vec::with_capacity(column_count);
            for column in 0..column_count {
                let value = sql_value(row.get_ref(column).map_err(sql_query_error)?)?;
                result_bytes = result_bytes
                    .checked_add(sql_value_size(&value))
                    .ok_or_else(|| Error::InvalidCommand("SQL result size overflow".into()))?;
                if result_bytes > max_bytes {
                    return Err(Error::InvalidCommand(format!(
                        "SQL query exceeds {max_bytes} result bytes"
                    )));
                }
                values.push(value);
            }
            result_rows.push(values);
        }
        Ok(SqlQueryResult {
            columns,
            rows: result_rows,
        })
    });
    let authorizer_clear = conn
        .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .map_err(sqlite_error);
    let progress_clear = conn
        .progress_handler(0, None::<fn() -> bool>)
        .map_err(sqlite_error);
    let reusable = authorizer_clear.is_ok() && progress_clear.is_ok();
    let result = match (result, authorizer_clear, progress_clear) {
        (Err(error), _, _) => Err(error),
        (Ok(_), Err(error), _) | (Ok(_), Ok(()), Err(error)) => Err(error),
        (Ok(result), Ok(()), Ok(())) => Ok(result),
    };
    (result, reusable)
}

#[cfg(test)]
struct SqlReadTestGate {
    entered: std::sync::mpsc::Sender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

#[cfg(test)]
fn sql_read_test_gates() -> &'static Mutex<Vec<(PathBuf, Arc<SqlReadTestGate>)>> {
    type Gates = Mutex<Vec<(PathBuf, Arc<SqlReadTestGate>)>>;
    static GATES: OnceLock<Gates> = OnceLock::new();
    GATES.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn arm_sql_read_test_gate(
    path: &Path,
) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    sql_read_test_gates().lock().unwrap().push((
        path.to_path_buf(),
        Arc::new(SqlReadTestGate {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        }),
    ));
    (entered_rx, release_tx)
}

#[cfg(test)]
fn gate_sql_read(path: &Path) -> Result<()> {
    let gate = sql_read_test_gates()
        .lock()
        .map_err(|_| Error::Sqlite("SQLite read test-gate lock is poisoned".into()))?
        .iter()
        .rev()
        .find(|(candidate, _)| candidate == path)
        .map(|(_, gate)| Arc::clone(gate));
    let Some(gate) = gate else {
        return Ok(());
    };
    gate.entered
        .send(())
        .map_err(|_| Error::Sqlite("SQLite read test gate lost its observer".into()))?;
    let released = gate
        .release
        .lock()
        .map_err(|_| Error::Sqlite("SQLite read test gate is poisoned".into()))?
        .recv()
        .map_err(|_| Error::Sqlite("SQLite read test gate was not released".into()));
    released
}

#[cfg(test)]
fn disarm_sql_read_test_gate(path: &Path) {
    sql_read_test_gates()
        .lock()
        .unwrap()
        .retain(|(candidate, _)| candidate != path);
}

#[cfg(test)]
fn external_prepare_test_gates() -> &'static Mutex<Vec<(PathBuf, Arc<SqlReadTestGate>)>> {
    type Gates = Mutex<Vec<(PathBuf, Arc<SqlReadTestGate>)>>;
    static GATES: OnceLock<Gates> = OnceLock::new();
    GATES.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn arm_external_prepare_test_gate(
    path: &Path,
) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    external_prepare_test_gates().lock().unwrap().push((
        path.to_path_buf(),
        Arc::new(SqlReadTestGate {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        }),
    ));
    (entered_rx, release_tx)
}

#[cfg(test)]
fn gate_external_prepare(path: &Path) -> Result<()> {
    let gate = external_prepare_test_gates()
        .lock()
        .map_err(|_| Error::Sqlite("external SQL prepare test-gate lock is poisoned".into()))?
        .iter()
        .rev()
        .find(|(candidate, _)| candidate == path)
        .map(|(_, gate)| Arc::clone(gate));
    let Some(gate) = gate else {
        return Ok(());
    };
    gate.entered
        .send(())
        .map_err(|_| Error::Sqlite("external SQL prepare test gate lost its observer".into()))?;
    let released = gate
        .release
        .lock()
        .map_err(|_| Error::Sqlite("external SQL prepare test gate is poisoned".into()))?
        .recv()
        .map_err(|_| Error::Sqlite("external SQL prepare test gate was not released".into()));
    released
}

#[cfg(test)]
fn disarm_external_prepare_test_gate(path: &Path) {
    external_prepare_test_gates()
        .lock()
        .unwrap()
        .retain(|(candidate, _)| candidate != path);
}

#[cfg(test)]
fn close_for_handoff_faults() -> &'static Mutex<Vec<PathBuf>> {
    static FAULTS: OnceLock<Mutex<Vec<PathBuf>>> = OnceLock::new();
    FAULTS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn arm_close_for_handoff_fault(path: &Path) {
    close_for_handoff_faults()
        .lock()
        .unwrap()
        .push(path.to_path_buf());
}

#[cfg(test)]
fn inject_close_for_handoff_fault(path: &Path) -> Result<()> {
    let mut faults = close_for_handoff_faults()
        .lock()
        .map_err(|_| Error::Sqlite("close-for-handoff fault lock is poisoned".into()))?;
    if let Some(index) = faults.iter().position(|candidate| candidate == path) {
        faults.swap_remove(index);
        return Err(Error::Sqlite("injected close-for-handoff fault".into()));
    }
    Ok(())
}

fn checkpoint_truncate(conn: &Connection) -> Result<()> {
    let (busy, _, _): (i64, i64, i64) = conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(sqlite_error)?;
    if busy != 0 {
        return Err(Error::Sqlite("SQLite WAL checkpoint is busy".into()));
    }
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SpeculativePrepareAudit {
    copy_count: usize,
    copy_bytes: u64,
    synchronous: i64,
    native_vfs: bool,
    no_checkpoint_on_close: bool,
}

#[cfg(test)]
fn speculative_prepare_audits() -> &'static Mutex<Vec<(PathBuf, SpeculativePrepareAudit)>> {
    static AUDITS: OnceLock<Mutex<Vec<(PathBuf, SpeculativePrepareAudit)>>> = OnceLock::new();
    AUDITS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn note_speculative_staging(path: &Path, copy_count: usize, copy_bytes: u64) {
    if let Ok(mut audits) = speculative_prepare_audits().lock() {
        audits.push((
            path.to_path_buf(),
            SpeculativePrepareAudit {
                copy_count,
                copy_bytes,
                synchronous: -1,
                native_vfs: false,
                no_checkpoint_on_close: false,
            },
        ));
    }
}

#[cfg(test)]
fn note_native_wal_capture(path: &Path) {
    if let Ok(mut audits) = speculative_prepare_audits().lock() {
        if let Some((_, audit)) = audits.iter_mut().rev().find(|(audited, _)| audited == path) {
            audit.native_vfs = true;
            audit.no_checkpoint_on_close = true;
        }
    }
}

#[cfg(test)]
fn note_speculative_synchronous(path: &Path, staging: &Connection) -> Result<()> {
    let synchronous = staging
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .map_err(sqlite_error)?;
    if let Ok(mut audits) = speculative_prepare_audits().lock() {
        let Some((_, audit)) = audits.iter_mut().rev().find(|(audited, _)| audited == path) else {
            return Err(Error::Sqlite(
                "speculative SQLite synchronous audit is missing its copy".into(),
            ));
        };
        audit.synchronous = synchronous;
    }
    Ok(())
}

#[cfg(test)]
fn speculative_prepare_audit(path: &Path) -> Option<SpeculativePrepareAudit> {
    speculative_prepare_audits().lock().ok().and_then(|audits| {
        audits
            .iter()
            .rev()
            .find_map(|(audited, audit)| (audited == path).then_some(*audit))
    })
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreparedInstallPath {
    Patched,
}

#[cfg(test)]
fn prepared_installs() -> &'static Mutex<Vec<(PathBuf, PreparedInstallPath)>> {
    static INSTALLS: OnceLock<Mutex<Vec<(PathBuf, PreparedInstallPath)>>> = OnceLock::new();
    INSTALLS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn note_prepared_install(path: &Path, install: PreparedInstallPath) {
    if let Ok(mut installs) = prepared_installs().lock() {
        installs.push((path.to_path_buf(), install));
    }
}

#[cfg(test)]
fn prepared_install_path(path: &Path) -> Option<PreparedInstallPath> {
    prepared_installs().lock().ok().and_then(|installs| {
        installs
            .iter()
            .rev()
            .find_map(|(installed, method)| (installed == path).then_some(*method))
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StagingWalSeal {
    len: u64,
    modified: Option<std::time::SystemTime>,
    #[cfg(unix)]
    unix: PreparedBaseSeal,
}

impl StagingWalSeal {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            unix: unix_metadata_seal(metadata),
        }
    }
}

struct StagingSidecarCleanup {
    path: PathBuf,
}

impl StagingSidecarCleanup {
    fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
        }
    }

    fn cleanup(&self) -> Result<()> {
        for suffix in ["-wal", "-shm"] {
            let sidecar = sqlite_sidecar_path(&self.path, suffix);
            match fs::symlink_metadata(&sidecar) {
                Ok(metadata) if metadata.file_type().is_file() => {
                    fs::remove_file(&sidecar).map_err(io_error)?;
                }
                Ok(_) => {
                    return Err(Error::InvalidEntry(format!(
                        "owned speculative SQLite sidecar {} is not a regular file",
                        sidecar.display()
                    )));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(io_error(error)),
            }
        }
        Ok(())
    }
}

impl Drop for StagingSidecarCleanup {
    fn drop(&mut self) {
        for suffix in ["-wal", "-shm"] {
            let _ = fs::remove_file(sqlite_sidecar_path(&self.path, suffix));
        }
    }
}

fn cleanup_speculative_sidecars(path: &Path) -> Result<()> {
    StagingSidecarCleanup::new(path).cleanup()?;
    sync_parent(parent_dir(path))
}

fn verify_speculative_base_unchanged(
    path: &Path,
    base_file: &File,
    base_seal: Option<&PreparedBaseSeal>,
    expected: StateIdentityV3,
) -> Result<()> {
    let owned = base_file.metadata().map_err(io_error)?;
    let named = fs::symlink_metadata(path).map_err(io_error)?;
    if !prepared_base_metadata_matches(base_seal, &owned, &named) || !sqlite_sidecars_absent(path)?
    {
        return Err(Error::InvalidEntry(
            "canonical SQLite base changed during speculative WAL capture".into(),
        ));
    }
    let mut readable = base_file.try_clone().map_err(io_error)?;
    if rebuild_page_state_from_file(&mut readable, owned.len())?.identity() != expected {
        return Err(Error::InvalidEntry(
            "canonical SQLite page state changed during speculative WAL capture".into(),
        ));
    }
    Ok(())
}

fn sqlite_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = path.as_os_str().to_os_string();
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

/// Removes sidecars that cannot contain committed WAL frames after the caller has fenced every
/// possible database owner.
///
/// A non-empty WAL or a non-regular sidecar is rejected and preserved for recovery inspection.
pub fn cleanup_empty_wal_sidecars_after_owner_fence(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    let wal = sqlite_sidecar_path(path, "-wal");
    let shm = sqlite_sidecar_path(path, "-shm");
    let wal_metadata = symlink_metadata_if_exists(&wal)?;
    let shm_metadata = symlink_metadata_if_exists(&shm)?;
    for (name, metadata) in [
        ("WAL", wal_metadata.as_ref()),
        ("SHM", shm_metadata.as_ref()),
    ] {
        if metadata.is_some_and(|metadata| !metadata.file_type().is_file()) {
            return Err(Error::InvalidEntry(format!(
                "SQLite {name} sidecar is not a regular file"
            )));
        }
    }
    if wal_metadata
        .as_ref()
        .is_some_and(|metadata| metadata.len() != 0)
    {
        return Err(Error::InvalidEntry(
            "SQLite WAL sidecar contains uncheckpointed frames".into(),
        ));
    }
    let mut changed = false;
    for sidecar in [&wal, &shm] {
        match fs::remove_file(sidecar) {
            Ok(()) => changed = true,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
    }
    if changed {
        sync_parent(parent_dir(path))?;
    }
    Ok(())
}

fn sqlite_sidecars_absent(path: &Path) -> Result<bool> {
    for suffix in ["-wal", "-shm"] {
        if sqlite_sidecar_path(path, suffix)
            .try_exists()
            .map_err(io_error)?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn discard_speculative_sidecars_if_base_matches_control(
    path: &Path,
    control: &ControlStore,
) -> Result<()> {
    if sqlite_sidecars_absent(path)? {
        return Ok(());
    }
    let page_state = rebuild_sealed_main_ignoring_sidecars(path)?;
    if page_state.cache.identity() != control.user_state()? {
        return Err(Error::InvalidEntry(
            "canonical SQLite has leftover WAL sidecars but its main file does not match the committed control page state"
                .into(),
        ));
    }
    cleanup_speculative_sidecars(path)?;
    Ok(())
}

fn open_fresh_staging_wal(path: &Path) -> Result<Option<(File, StagingWalSeal)>> {
    let wal_path = sqlite_sidecar_path(path, "-wal");
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(test)]
    options.write(true);
    let wal = match options.open(&wal_path) {
        Ok(wal) => wal,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(error)),
    };
    let owned = wal.metadata().map_err(io_error)?;
    let named = fs::symlink_metadata(&wal_path).map_err(io_error)?;
    if !owned.file_type().is_file() || !named.file_type().is_file() {
        return Err(Error::InvalidEntry(
            "fresh speculative SQLite WAL is not a regular file".into(),
        ));
    }
    #[cfg(unix)]
    if !same_file(&owned, &named) {
        return Err(Error::InvalidEntry(
            "held speculative SQLite WAL inode is no longer named by its path".into(),
        ));
    }
    let seal = StagingWalSeal::from_metadata(&owned);
    if StagingWalSeal::from_metadata(&named) != seal {
        return Err(Error::InvalidEntry(
            "fresh speculative SQLite WAL metadata is unstable".into(),
        ));
    }
    Ok(Some((wal, seal)))
}

fn verify_staging_wal_seal(path: &Path, wal: &File, seal: StagingWalSeal) -> Result<()> {
    let owned = wal.metadata().map_err(io_error)?;
    let named = fs::symlink_metadata(sqlite_sidecar_path(path, "-wal")).map_err(io_error)?;
    if StagingWalSeal::from_metadata(&owned) != seal
        || StagingWalSeal::from_metadata(&named) != seal
        || !owned.file_type().is_file()
        || !named.file_type().is_file()
        || !same_file(&owned, &named)
    {
        return Err(Error::InvalidEntry(
            "held speculative SQLite WAL changed after capture was sealed or lost path ownership"
                .into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WalCaptureFault {
    ChangeHeldWalAfterSeal,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QwalApplyFault {
    BeforePendingBegin,
    BeforeInstallCheckpoint,
    AfterPage(u32),
    BeforeControlCommit,
    BeforePendingCommit,
}

#[cfg(test)]
fn qwal_apply_faults() -> &'static Mutex<Vec<(PathBuf, QwalApplyFault)>> {
    static FAULTS: OnceLock<Mutex<Vec<(PathBuf, QwalApplyFault)>>> = OnceLock::new();
    FAULTS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn arm_qwal_apply_fault(path: &Path, fault: QwalApplyFault) {
    qwal_apply_faults()
        .lock()
        .unwrap()
        .push((path.to_path_buf(), fault));
}

#[cfg(test)]
fn take_qwal_apply_fault(path: &Path, matches: impl Fn(QwalApplyFault) -> bool) -> Result<bool> {
    let mut faults = qwal_apply_faults()
        .lock()
        .map_err(|_| Error::Sqlite("QWAL apply fault lock is poisoned".into()))?;
    Ok(faults
        .iter()
        .position(|(armed, fault)| armed == path && matches(*fault))
        .map(|position| faults.swap_remove(position))
        .is_some())
}

#[cfg(test)]
fn inject_qwal_apply_fault(path: &Path, page_no: u32) -> Result<()> {
    if take_qwal_apply_fault(
        path,
        |fault| matches!(fault, QwalApplyFault::AfterPage(expected) if expected == page_no),
    )? {
        return Err(Error::Io(
            "injected failure during canonical QWAL page writes".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn inject_before_install_checkpoint_fault(path: &Path) -> Result<()> {
    if take_qwal_apply_fault(path, |fault| {
        fault == QwalApplyFault::BeforeInstallCheckpoint
    })? {
        return Err(Error::Sqlite(
            "injected failure before canonical QWAL checkpoint".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn inject_qwal_control_fault(path: &Path) -> Result<()> {
    if take_qwal_apply_fault(path, |fault| fault == QwalApplyFault::BeforeControlCommit)? {
        return Err(Error::Sqlite(
            "injected post-install control commit failure".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn inject_pending_commit_fault(path: &Path) -> Result<()> {
    if take_qwal_apply_fault(path, |fault| fault == QwalApplyFault::BeforePendingCommit)? {
        return Err(Error::Sqlite(
            "injected pending control commit failure".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn inject_pending_begin_fault(path: &Path) -> Result<()> {
    if take_qwal_apply_fault(path, |fault| fault == QwalApplyFault::BeforePendingBegin)? {
        return Err(Error::Sqlite(
            "injected failure before pending control publication".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn wal_capture_faults() -> &'static Mutex<Vec<(PathBuf, WalCaptureFault)>> {
    static FAULTS: OnceLock<Mutex<Vec<(PathBuf, WalCaptureFault)>>> = OnceLock::new();
    FAULTS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn arm_wal_capture_fault(path: &Path, fault: WalCaptureFault) {
    wal_capture_faults()
        .lock()
        .unwrap()
        .push((path.to_path_buf(), fault));
}

#[cfg(test)]
fn inject_wal_capture_fault(path: &Path, held_wal: Option<&(File, StagingWalSeal)>) -> Result<()> {
    let fault = {
        let mut faults = wal_capture_faults()
            .lock()
            .map_err(|_| Error::Sqlite("WAL capture fault lock is poisoned".into()))?;
        faults
            .iter()
            .position(|(armed, _)| armed == path)
            .map(|position| faults.swap_remove(position).1)
    };
    match (fault, held_wal) {
        (Some(WalCaptureFault::ChangeHeldWalAfterSeal), Some((wal, seal))) => wal
            .set_len(
                seal.len
                    .checked_add(1)
                    .ok_or_else(|| Error::ResourceExhausted("test WAL length overflow".into()))?,
            )
            .map_err(io_error),
        (Some(_), None) => Err(Error::InvalidEntry(
            "test expected a held speculative SQLite WAL".into(),
        )),
        (None, _) => Ok(()),
    }
}

fn qwal_pages_from_capture(
    base: &File,
    expected_page_size: u32,
    base_file_bytes: u64,
    capture: WalCapture,
) -> Result<(Vec<QwalPageV3>, u32)> {
    let WalCapture::Committed(WalCommit {
        page_size,
        target_db_pages,
        target_file_bytes,
        pages: captured_pages,
    }) = capture
    else {
        let base_pages = u32::try_from(base_file_bytes / u64::from(expected_page_size))
            .map_err(|_| Error::ResourceExhausted("SQLite base page count overflows".into()))?;
        return Ok((Vec::new(), base_pages));
    };
    if page_size != expected_page_size {
        return Err(Error::InvalidEntry(
            "SQLite WAL page size differs from its closed base".into(),
        ));
    }
    let expected_target_bytes = u64::from(target_db_pages)
        .checked_mul(u64::from(page_size))
        .ok_or_else(|| Error::InvalidEntry("SQLite WAL target size overflows".into()))?;
    if target_file_bytes != expected_target_bytes {
        return Err(Error::InvalidEntry(
            "SQLite WAL target size does not match its commit page count".into(),
        ));
    }

    let page_bytes = u64::from(page_size);
    let base_pages = base_file_bytes / page_bytes;
    let mut base = base.try_clone().map_err(io_error)?;
    let mut changed = Vec::with_capacity(captured_pages.len());
    let mut base_page = vec![0; page_size as usize];
    for page in captured_pages {
        let page_no = u64::from(page.page_no);
        let offset = page_no
            .checked_sub(1)
            .and_then(|index| index.checked_mul(page_bytes))
            .ok_or_else(|| Error::InvalidEntry("SQLite WAL page offset overflows".into()))?;
        let differs_from_base = if page_no <= base_pages {
            base.seek(SeekFrom::Start(offset)).map_err(io_error)?;
            base.read_exact(&mut base_page).map_err(io_error)?;
            base_page != page.after_image
        } else {
            true
        };
        if differs_from_base {
            changed.push(QwalPageV3 {
                page_no: u32::try_from(page_no)
                    .map_err(|_| Error::ResourceExhausted("QWAL page count exceeds u32".into()))?,
                after_image: page.after_image,
            });
        }
    }
    Ok((changed, target_db_pages))
}

fn open_file_digest(file: &File) -> Result<LogHash> {
    let mut file = file.try_clone().map_err(io_error)?;
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(io_error)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(LogHash::from_bytes(hasher.finalize().into()))
}

fn file_digest(path: impl AsRef<Path>) -> Result<LogHash> {
    open_file_digest(&File::open(path.as_ref()).map_err(io_error)?)
}

fn page_state_from_database_bytes(bytes: &[u8]) -> Result<PageStateCacheV3> {
    let header = bytes
        .get(..100)
        .ok_or_else(|| Error::InvalidEntry("SQLite database header is truncated".into()))?;
    let file_bytes = u64::try_from(bytes.len())
        .map_err(|_| Error::ResourceExhausted("SQLite database size exceeds u64".into()))?;
    let page_size = qwal::sqlite_page_size_from_header(header, file_bytes)?;
    PageStateCacheV3::from_pages(page_size, bytes.chunks_exact(page_size as usize))
}

#[cfg(test)]
fn rebuild_page_state(path: &Path) -> Result<PageStateCacheV3> {
    page_state_from_database_bytes(&fs::read(path).map_err(io_error)?)
}

fn rebuild_sealed_page_state(path: &Path) -> Result<CanonicalPageStateV3> {
    if !sqlite_sidecars_absent(path)? {
        return Err(Error::InvalidEntry(
            "closed canonical SQLite database has WAL sidecars during page-state rebuild".into(),
        ));
    }
    let named_before = fs::symlink_metadata(path).map_err(io_error)?;
    let mut file = File::open(path).map_err(io_error)?;
    let owned_before = file.metadata().map_err(io_error)?;
    let seal = prepared_base_seal(&owned_before, &named_before)?;
    let expected_bytes = owned_before.len();
    let cache = rebuild_page_state_from_file(&mut file, expected_bytes)?;
    let owned_after = file.metadata().map_err(io_error)?;
    let named_after = fs::symlink_metadata(path).map_err(io_error)?;
    if !prepared_base_metadata_matches(seal.as_ref(), &owned_after, &named_after)
        || owned_after.len() != expected_bytes
        || !sqlite_sidecars_absent(path)?
    {
        return Err(Error::InvalidEntry(
            "SQLite database changed while rebuilding page state".into(),
        ));
    }
    Ok(CanonicalPageStateV3 { cache, seal })
}

fn rebuild_sealed_main_ignoring_sidecars(path: &Path) -> Result<CanonicalPageStateV3> {
    let named_before = fs::symlink_metadata(path).map_err(io_error)?;
    let mut file = File::open(path).map_err(io_error)?;
    let owned_before = file.metadata().map_err(io_error)?;
    let seal = prepared_base_seal(&owned_before, &named_before)?;
    let expected_bytes = owned_before.len();
    let cache = rebuild_page_state_from_file(&mut file, expected_bytes)?;
    let owned_after = file.metadata().map_err(io_error)?;
    let named_after = fs::symlink_metadata(path).map_err(io_error)?;
    if !prepared_base_metadata_matches(seal.as_ref(), &owned_after, &named_after)
        || owned_after.len() != expected_bytes
    {
        return Err(Error::InvalidEntry(
            "SQLite main database changed while validating leftover WAL sidecars".into(),
        ));
    }
    Ok(CanonicalPageStateV3 { cache, seal })
}

fn rebuild_page_state_from_file(file: &mut File, expected_bytes: u64) -> Result<PageStateCacheV3> {
    let mut header = [0_u8; 100];
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    file.read_exact(&mut header).map_err(io_error)?;
    let page_size = qwal::sqlite_page_size_from_header(&header, expected_bytes)?;
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    let mut remaining = expected_bytes / u64::from(page_size);
    let mut stream_error = None;
    let cache = PageStateCacheV3::from_pages(
        page_size,
        std::iter::from_fn(|| {
            if remaining == 0 || stream_error.is_some() {
                return None;
            }
            let mut page = vec![0; page_size as usize];
            match file.read_exact(&mut page) {
                Ok(()) => {
                    remaining -= 1;
                    Some(page)
                }
                Err(error) => {
                    stream_error = Some(io_error(error));
                    None
                }
            }
        }),
    )?;
    if let Some(error) = stream_error {
        return Err(error);
    }
    Ok(cache)
}

fn open_bound_canonical(path: &Path, page_state: &CanonicalPageStateV3) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(io_error)?;
    let owned = file.metadata().map_err(io_error)?;
    let named = fs::symlink_metadata(path).map_err(io_error)?;
    if !prepared_base_metadata_matches(page_state.seal.as_ref(), &owned, &named) {
        return Err(Error::InvalidEntry(
            "canonical SQLite file no longer matches the cached page-state seal".into(),
        ));
    }
    Ok(file)
}

fn verify_bound_canonical(path: &Path, page_state: &CanonicalPageStateV3) -> Result<()> {
    open_bound_canonical(path, page_state).map(drop)
}

fn seal_held_canonical(path: &Path, file: &File) -> Result<Option<PreparedBaseSeal>> {
    if !sqlite_sidecars_absent(path)? {
        return Err(Error::InvalidEntry(
            "closed canonical SQLite database has WAL sidecars while refreshing its seal".into(),
        ));
    }
    let owned_before = file.metadata().map_err(io_error)?;
    let named_before = fs::symlink_metadata(path).map_err(io_error)?;
    let seal = prepared_base_seal(&owned_before, &named_before)?;
    let owned_after = file.metadata().map_err(io_error)?;
    let named_after = fs::symlink_metadata(path).map_err(io_error)?;
    if !prepared_base_metadata_matches(seal.as_ref(), &owned_after, &named_after)
        || !sqlite_sidecars_absent(path)?
    {
        return Err(Error::InvalidEntry(
            "canonical SQLite file changed while refreshing its page-state seal".into(),
        ));
    }
    Ok(seal)
}

fn open_sealed_prepared_base(path: &Path) -> Result<(File, Option<PreparedBaseSeal>)> {
    let named_before = fs::symlink_metadata(path).map_err(io_error)?;
    if !named_before.file_type().is_file() {
        return Err(Error::InvalidEntry(
            "closed SQLite base is not a regular file".into(),
        ));
    }
    let file = File::open(path).map_err(io_error)?;
    let owned_before = file.metadata().map_err(io_error)?;
    if !owned_before.file_type().is_file() {
        return Err(Error::InvalidEntry(
            "owned SQLite base is not a regular file".into(),
        ));
    }
    let seal = prepared_base_seal(&owned_before, &named_before)?;
    if !sqlite_sidecars_absent(path)? {
        return Err(Error::InvalidEntry(
            "closed SQLite base still has WAL sidecars".into(),
        ));
    }
    let owned_after = file.metadata().map_err(io_error)?;
    let named_after = fs::symlink_metadata(path).map_err(io_error)?;
    if !prepared_base_metadata_matches(seal.as_ref(), &owned_after, &named_after)
        || !sqlite_sidecars_absent(path)?
    {
        return Err(Error::InvalidEntry(
            "closed SQLite base changed while it was sealed".into(),
        ));
    }
    Ok((file, seal))
}

#[cfg(unix)]
fn prepared_base_seal(
    owned: &fs::Metadata,
    named: &fs::Metadata,
) -> Result<Option<PreparedBaseSeal>> {
    if !owned_regular_file_still_named(owned, named) {
        return Err(Error::InvalidEntry(
            "owned SQLite base inode is no longer named by its path".into(),
        ));
    }
    let seal = unix_metadata_seal(owned);
    if unix_metadata_seal(named) != seal {
        return Err(Error::InvalidEntry(
            "owned SQLite base metadata is not stable".into(),
        ));
    }
    Ok(Some(seal))
}

#[cfg(not(unix))]
fn prepared_base_seal(
    _owned: &fs::Metadata,
    _named: &fs::Metadata,
) -> Result<Option<PreparedBaseSeal>> {
    Ok(None)
}

#[cfg(unix)]
fn unix_metadata_seal(metadata: &fs::Metadata) -> PreparedBaseSeal {
    use std::os::unix::fs::MetadataExt;

    PreparedBaseSeal {
        dev: metadata.dev(),
        ino: metadata.ino(),
        len: metadata.len(),
        mtime: metadata.mtime(),
        mtime_nsec: metadata.mtime_nsec(),
        ctime: metadata.ctime(),
        ctime_nsec: metadata.ctime_nsec(),
    }
}

#[cfg(unix)]
fn prepared_base_metadata_matches(
    seal: Option<&PreparedBaseSeal>,
    owned: &fs::Metadata,
    named: &fs::Metadata,
) -> bool {
    seal.is_some_and(|seal| {
        owned_regular_file_still_named(owned, named)
            && unix_metadata_seal(owned) == *seal
            && unix_metadata_seal(named) == *seal
    })
}

#[cfg(not(unix))]
fn prepared_base_metadata_matches(
    _seal: Option<&PreparedBaseSeal>,
    _owned: &fs::Metadata,
    _named: &fs::Metadata,
) -> bool {
    false
}

fn symlink_metadata_if_exists(path: &Path) -> Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error(error)),
    }
}

#[cfg(unix)]
fn same_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file(_left: &fs::Metadata, _right: &fs::Metadata) -> bool {
    // The optimization is intentionally disabled when the platform cannot
    // prove that the owned temporary inode is still the one named by its path.
    false
}

fn owned_regular_file_still_named(owned: &fs::Metadata, named: &fs::Metadata) -> bool {
    owned.file_type().is_file() && named.file_type().is_file() && same_file(owned, named)
}

fn control_sidecar_path(path: &Path) -> PathBuf {
    let mut sidecar = path.as_os_str().to_os_string();
    sidecar.push(".control");
    PathBuf::from(sidecar)
}

fn validate_control_database_pair(
    path: &Path,
    control: &ControlStore,
) -> Result<(CanonicalPageStateV3, bool)> {
    let page_state = rebuild_sealed_page_state(path)?;
    let actual = page_state.cache.identity();
    let pending = control.pending()?;
    if let Some(pending) = pending.as_ref() {
        let tip = control.applied_tip()?;
        let expected_entry_index = tip
            .applied_index()
            .checked_add(1)
            .ok_or_else(|| Error::InvalidEntry("pending QWAL entry index is exhausted".into()))?;
        if pending.base() != LogAnchor::new(tip.applied_index(), tip.applied_hash())
            || pending.base_state() != control.user_state()?
            || pending.entry().index() != expected_entry_index
        {
            return Err(Error::InvalidEntry(
                "pending QWAL intent does not extend the committed control state".into(),
            ));
        }
        if actual == pending.base_state() {
            return Ok((page_state, true));
        }
        if actual == pending.target_state() {
            let verify = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(sqlite_error)?;
            integrity_check(&verify)?;
            return Ok((page_state, true));
        }
        return Err(Error::InvalidEntry(
            "pending QWAL database state matches neither base nor target".into(),
        ));
    }
    if actual != control.user_state()? {
        return Err(Error::InvalidEntry(
            "canonical SQLite page state does not match the control sidecar".into(),
        ));
    }
    Ok((page_state, false))
}

fn decode_qwal_command(payload: &[u8]) -> Result<QwalEnvelopeV3> {
    if !payload.starts_with(QWAL_V3_MAGIC) {
        return Err(Error::InvalidCommand(
            "QWAL-only SQLite apply requires a QWAL v3 payload".into(),
        ));
    }
    decode_qwal_v3(payload)
}

fn validate_qwal_identity(
    effect: &QwalEnvelopeV3,
    identity: &ControlIdentity,
    configuration: &ConfigurationState,
) -> Result<()> {
    let field = if effect.cluster_id != identity.cluster_id() {
        "cluster_id"
    } else if effect.epoch != identity.epoch() {
        "epoch"
    } else if effect.configuration_id != configuration.config_id() {
        "configuration_id"
    } else if effect.recovery_generation != identity.recovery_generation() {
        "recovery_generation"
    } else if effect.materializer_fingerprint != identity.materializer_fingerprint().to_hex() {
        "materializer_fingerprint"
    } else if effect.base_state != identity.user_state() {
        "base_state"
    } else {
        return Ok(());
    };
    Err(Error::InvalidEntry(format!(
        "QWAL effect identity mismatch: {field}"
    )))
}

fn encode_qwal_snapshot(snapshot: &QwalSnapshotV3) -> Result<Vec<u8>> {
    let body = postcard::to_allocvec(snapshot)
        .map_err(|error| Error::InvalidSnapshot(format!("QSNP encode failed: {error}")))?;
    let mut encoded = Vec::with_capacity(QWAL_SNAPSHOT_V3_MAGIC.len() + body.len());
    encoded.extend_from_slice(QWAL_SNAPSHOT_V3_MAGIC);
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

fn decode_qwal_snapshot(encoded: &[u8]) -> Result<QwalSnapshotV3> {
    let body = encoded
        .strip_prefix(QWAL_SNAPSHOT_V3_MAGIC)
        .ok_or_else(|| Error::InvalidSnapshot("QWAL snapshot magic is missing".into()))?;
    let snapshot: QwalSnapshotV3 = postcard::from_bytes(body)
        .map_err(|error| Error::InvalidSnapshot(format!("QSNP decode failed: {error}")))?;
    if postcard::to_allocvec(&snapshot)
        .map_err(|error| Error::InvalidSnapshot(format!("QSNP re-encode failed: {error}")))?
        != body
    {
        return Err(Error::InvalidSnapshot(
            "QWAL snapshot is not canonically encoded".into(),
        ));
    }
    Ok(snapshot)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlAuthorizationMode {
    ReadOnly,
    PhysicalWrite,
}

fn execute_sql_statements(
    conn: &Connection,
    statements: &[SqlStatement],
) -> Result<SqlCommandResult> {
    let result = with_sql_authorizer(conn, SqlAuthorizationMode::PhysicalWrite, || {
        let mut statement_results = Vec::with_capacity(statements.len());
        let mut returning_rows = 0usize;
        let mut returning_bytes = 0usize;
        for operation in statements {
            let mut statement = conn.prepare(&operation.sql).map_err(sqlite_error)?;
            let column_count = statement.column_count();
            let total_changes_before = conn.total_changes();
            let returning = if column_count == 0 {
                statement
                    .execute(params_from_iter(operation.parameters.iter()))
                    .map_err(sqlite_error)?;
                None
            } else {
                let columns = statement
                    .column_names()
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                for column in &columns {
                    add_returning_bytes(&mut returning_bytes, column.len())?;
                }
                let mut result_rows = Vec::new();
                {
                    let mut rows = statement
                        .query(params_from_iter(operation.parameters.iter()))
                        .map_err(sqlite_error)?;
                    while let Some(row) = rows.next().map_err(sqlite_error)? {
                        returning_rows = returning_rows.checked_add(1).ok_or_else(|| {
                            Error::InvalidCommand("SQL RETURNING row count overflow".into())
                        })?;
                        if returning_rows > MAX_RETURNING_ROWS {
                            return Err(Error::InvalidCommand(format!(
                                "SQL RETURNING exceeds {MAX_RETURNING_ROWS} rows"
                            )));
                        }
                        let mut values = Vec::with_capacity(column_count);
                        for column in 0..column_count {
                            let value = sql_value(row.get_ref(column).map_err(sqlite_error)?)?;
                            add_returning_bytes(&mut returning_bytes, sql_value_size(&value))?;
                            values.push(value);
                        }
                        result_rows.push(values);
                    }
                }
                Some(SqlQueryResult {
                    columns,
                    rows: result_rows,
                })
            };
            let total_changes = conn
                .total_changes()
                .checked_sub(total_changes_before)
                .ok_or_else(|| Error::Sqlite("SQLite total_changes moved backwards".into()))?;
            let rows_affected = if total_changes == 0 {
                0
            } else {
                conn.changes()
            };
            statement_results.push(SqlStatementResult {
                rows_affected,
                returning,
            });
        }
        validate_temp_schema_empty(conn)?;
        Ok(SqlCommandResult { statement_results })
    })?;
    validate_reserved_schema(conn)?;
    Ok(result)
}

fn add_returning_bytes(total: &mut usize, bytes: usize) -> Result<()> {
    *total = total
        .checked_add(bytes)
        .ok_or_else(|| Error::InvalidCommand("SQL RETURNING result size overflow".into()))?;
    if *total > MAX_RETURNING_BYTES {
        return Err(Error::InvalidCommand(format!(
            "SQL RETURNING exceeds {MAX_RETURNING_BYTES} result bytes"
        )));
    }
    Ok(())
}

fn decode_sql_command(payload: &[u8]) -> Result<SqlCommand> {
    let encoded = payload
        .strip_prefix(SQL_COMMAND_V2_MAGIC)
        .ok_or_else(|| Error::InvalidCommand("canonical QSQL v2 magic is missing".into()))?;
    let envelope: SqlCommandV2Envelope = serde_json::from_slice(encoded)
        .map_err(|error| Error::InvalidCommand(format!("invalid SQL command: {error}")))?;
    let expected = sql_executor_fingerprint()?;
    if envelope.executor_fingerprint != expected {
        return Err(Error::InvalidCommand(format!(
            "SQL executor fingerprint {} does not match local {}",
            envelope.executor_fingerprint.to_hex(),
            expected.to_hex()
        )));
    }
    let command = envelope.command;
    validate_sql_command(&command)?;
    if encode_sql_command(&command)? != payload {
        return Err(Error::InvalidCommand(
            "QSQL v2 command is not canonically encoded".into(),
        ));
    }
    Ok(command)
}

fn validate_sql_command(command: &SqlCommand) -> Result<()> {
    if command.request_id.is_empty() || command.request_id.len() > 256 {
        return Err(Error::InvalidCommand(
            "SQL request_id must contain 1..=256 bytes".into(),
        ));
    }
    if command.statements.is_empty() || command.statements.len() > MAX_SQL_STATEMENTS {
        return Err(Error::InvalidCommand(format!(
            "SQL command must contain 1..={MAX_SQL_STATEMENTS} statements"
        )));
    }
    for statement in &command.statements {
        validate_sql_statement(statement)?;
    }
    Ok(())
}

fn validate_sql_statement(statement: &SqlStatement) -> Result<()> {
    if statement.sql.trim().is_empty() || statement.sql.len() > MAX_SQL_TEXT_BYTES {
        return Err(Error::InvalidCommand(format!(
            "SQL text must contain 1..={MAX_SQL_TEXT_BYTES} bytes"
        )));
    }
    if statement.sql.contains('\0') {
        return Err(Error::InvalidCommand(
            "SQL text must not contain NUL".into(),
        ));
    }
    if statement.parameters.len() > MAX_SQL_PARAMETERS {
        return Err(Error::InvalidCommand(format!(
            "SQL statement exceeds {MAX_SQL_PARAMETERS} parameters"
        )));
    }
    if statement
        .parameters
        .iter()
        .any(|value| matches!(value, SqlValue::Real(number) if !number.is_finite()))
    {
        return Err(Error::InvalidCommand(
            "SQL real parameters must be finite".into(),
        ));
    }
    Ok(())
}

fn with_sql_authorizer<T>(
    conn: &Connection,
    mode: SqlAuthorizationMode,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    conn.authorizer(Some(move |context: AuthContext<'_>| {
        authorize_sql(context, mode)
    }))
    .map_err(sqlite_error)?;
    let result = operation();
    let cleared = conn
        .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .map_err(sqlite_error);
    match (result, cleared) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn authorize_sql(context: AuthContext<'_>, mode: SqlAuthorizationMode) -> Authorization {
    if context.database_name.is_some_and(|name| {
        name != "main" && !(mode == SqlAuthorizationMode::PhysicalWrite && name == "temp")
    }) {
        return Authorization::Deny;
    }
    match context.action {
        AuthAction::Unknown { .. }
        | AuthAction::Transaction { .. }
        | AuthAction::Attach { .. }
        | AuthAction::Detach { .. }
        | AuthAction::Savepoint { .. } => Authorization::Deny,
        AuthAction::CreateTempIndex { .. }
        | AuthAction::CreateTempTable { .. }
        | AuthAction::CreateTempTrigger { .. }
        | AuthAction::CreateTempView { .. }
        | AuthAction::DropTempIndex { .. }
        | AuthAction::DropTempTable { .. }
        | AuthAction::DropTempTrigger { .. }
        | AuthAction::DropTempView { .. }
            if mode == SqlAuthorizationMode::PhysicalWrite =>
        {
            Authorization::Allow
        }
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } if authorized_pragma(mode, pragma_name, pragma_value) => Authorization::Allow,
        AuthAction::Pragma { .. } => Authorization::Deny,
        AuthAction::CreateVtable { module_name, .. }
        | AuthAction::DropVtable { module_name, .. }
            if mode == SqlAuthorizationMode::PhysicalWrite && bundled_vtable(module_name) =>
        {
            Authorization::Allow
        }
        AuthAction::CreateVtable { .. } | AuthAction::DropVtable { .. } => Authorization::Deny,
        AuthAction::Function { function_name } if unsafe_sql_function(function_name) => {
            Authorization::Deny
        }
        AuthAction::CreateIndex {
            index_name,
            table_name,
        }
        | AuthAction::DropIndex {
            index_name,
            table_name,
        } if reserved_name(index_name) || reserved_name(table_name) => Authorization::Deny,
        AuthAction::CreateTrigger {
            trigger_name,
            table_name,
        }
        | AuthAction::DropTrigger {
            trigger_name,
            table_name,
        } if reserved_name(trigger_name) || reserved_name(table_name) => Authorization::Deny,
        AuthAction::CreateTable { table_name }
        | AuthAction::Delete { table_name }
        | AuthAction::DropTable { table_name }
        | AuthAction::Insert { table_name }
        | AuthAction::Read { table_name, .. }
        | AuthAction::Update { table_name, .. }
        | AuthAction::AlterTable { table_name, .. }
        | AuthAction::Analyze { table_name }
            if reserved_name(table_name) =>
        {
            Authorization::Deny
        }
        AuthAction::CreateView { view_name } | AuthAction::DropView { view_name }
            if reserved_name(view_name) =>
        {
            Authorization::Deny
        }
        AuthAction::Reindex { index_name } if reserved_name(index_name) => Authorization::Deny,
        _ => Authorization::Allow,
    }
}

fn authorized_pragma(mode: SqlAuthorizationMode, name: &str, value: Option<&str>) -> bool {
    if observational_pragma(name, value) {
        return true;
    }
    mode == SqlAuthorizationMode::PhysicalWrite
        && ((matches_ignore_ascii_case(name, &["application_id", "user_version"])
            && value.is_some())
            || (name.eq_ignore_ascii_case("optimize")
                && value.is_none_or(|value| {
                    value.parse::<u32>().is_ok()
                        || value
                            .strip_prefix("0x")
                            .or_else(|| value.strip_prefix("0X"))
                            .is_some_and(|hex| u32::from_str_radix(hex, 16).is_ok())
                })))
}

fn observational_pragma(name: &str, value: Option<&str>) -> bool {
    const ARGUMENT_SAFE: &[&str] = &[
        "foreign_key_check",
        "foreign_key_list",
        "index_info",
        "index_list",
        "index_xinfo",
        "integrity_check",
        "quick_check",
        "table_info",
        "table_list",
        "table_xinfo",
    ];
    const NO_ARGUMENT_ONLY: &[&str] = &[
        "analysis_limit",
        "application_id",
        "auto_vacuum",
        "automatic_index",
        "busy_timeout",
        "cache_size",
        "cache_spill",
        "case_sensitive_like",
        "cell_size_check",
        "checkpoint_fullfsync",
        "collation_list",
        "compile_options",
        "count_changes",
        "data_version",
        "default_cache_size",
        "defer_foreign_keys",
        "empty_result_callbacks",
        "encoding",
        "freelist_count",
        "foreign_keys",
        "full_column_names",
        "fullfsync",
        "function_list",
        "hard_heap_limit",
        "ignore_check_constraints",
        "journal_size_limit",
        "legacy_alter_table",
        "locking_mode",
        "max_page_count",
        "mmap_size",
        "module_list",
        "page_count",
        "page_size",
        "pragma_list",
        "query_only",
        "read_uncommitted",
        "recursive_triggers",
        "reverse_unordered_selects",
        "schema_version",
        "secure_delete",
        "short_column_names",
        "soft_heap_limit",
        "synchronous",
        "temp_store",
        "threads",
        "trusted_schema",
        "user_version",
    ];

    if value.is_some_and(reserved_name) {
        return false;
    }
    ARGUMENT_SAFE
        .iter()
        .any(|allowed| name.eq_ignore_ascii_case(allowed))
        || value.is_none()
            && NO_ARGUMENT_ONLY
                .iter()
                .any(|allowed| name.eq_ignore_ascii_case(allowed))
}

fn reserved_name(name: &str) -> bool {
    const TABLES: &[&str] = &["__rhiza_kv"];
    reserved_table_name(name)
        || strip_ascii_prefix_ignore_case(name, "sqlite_autoindex_").is_some_and(|suffix| {
            TABLES.iter().any(|table| {
                suffix
                    .get(..table.len())
                    .is_some_and(|candidate| candidate.eq_ignore_ascii_case(table))
                    && suffix.as_bytes().get(table.len()) == Some(&b'_')
            })
        })
}

fn reserved_table_name(name: &str) -> bool {
    matches_ignore_ascii_case(name, &["__rhiza_kv"])
}

fn validate_reserved_schema(conn: &Connection) -> Result<()> {
    let unexpected: Option<String> = conn
        .query_row(
            "SELECT name
             FROM sqlite_schema
             WHERE
               (lower(name) = '__rhiza_kv' OR lower(tbl_name) = '__rhiza_kv')
                AND NOT (
                    (type = 'table' AND name = '__rhiza_kv' AND tbl_name = '__rhiza_kv')
                    OR
                    (type = 'index'
                     AND name GLOB 'sqlite_autoindex___rhiza_kv_*'
                     AND tbl_name = '__rhiza_kv')
                )
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_error)?;
    if let Some(name) = unexpected {
        return Err(Error::InvalidCommand(format!(
            "SQL object uses reserved rhiza namespace: {name}"
        )));
    }
    let mut statement = conn
        .prepare(
            "SELECT name, sql FROM sqlite_schema
             WHERE type = 'table' AND lower(sql) LIKE 'create virtual table%'",
        )
        .map_err(sqlite_error)?;
    let definitions = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)?;
    if let Some((name, _)) = definitions
        .into_iter()
        .find(|(_, sql)| virtual_table_uses_reserved_content(sql))
    {
        return Err(Error::InvalidCommand(format!(
            "SQL virtual table uses reserved rhiza content: {name}"
        )));
    }
    Ok(())
}

fn virtual_table_uses_reserved_content(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let Some(mut index) = unquoted_open_paren(bytes) else {
        return false;
    };
    index += 1;
    let mut depth = 1usize;
    let mut argument = Vec::new();
    while index < bytes.len() {
        match bytes[index] {
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while bytes.get(index).is_some_and(|byte| *byte != b'\n') {
                    index += 1;
                }
                argument.push(b' ');
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && (bytes[index] != b'*' || bytes[index + 1] != b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
                argument.push(b' ');
            }
            quote @ (b'\'' | b'"' | b'`' | b'[') => {
                let close = if quote == b'[' { b']' } else { quote };
                argument.push(quote);
                index += 1;
                while let Some(byte) = bytes.get(index).copied() {
                    argument.push(byte);
                    index += 1;
                    if byte == close {
                        if bytes.get(index) == Some(&close) {
                            argument.push(close);
                            index += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            b'(' => {
                depth += 1;
                argument.push(b'(');
                index += 1;
            }
            b')' if depth == 1 => {
                return content_argument_uses_reserved_table(&argument);
            }
            b')' => {
                depth -= 1;
                argument.push(b')');
                index += 1;
            }
            b',' if depth == 1 => {
                if content_argument_uses_reserved_table(&argument) {
                    return true;
                }
                argument.clear();
                index += 1;
            }
            byte => {
                argument.push(byte);
                index += 1;
            }
        }
    }
    content_argument_uses_reserved_table(&argument)
}

fn unquoted_open_paren(bytes: &[u8]) -> Option<usize> {
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while bytes.get(index).is_some_and(|byte| *byte != b'\n') {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && (bytes[index] != b'*' || bytes[index + 1] != b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            quote @ (b'\'' | b'"' | b'`' | b'[') => {
                let close = if quote == b'[' { b']' } else { quote };
                index += 1;
                while let Some(byte) = bytes.get(index).copied() {
                    index += 1;
                    if byte == close {
                        if bytes.get(index) == Some(&close) {
                            index += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            b'(' => return Some(index),
            _ => index += 1,
        }
    }
    None
}

fn content_argument_uses_reserved_table(argument: &[u8]) -> bool {
    let argument = trim_ascii(argument);
    let Some(equal) = unquoted_equal(argument) else {
        return false;
    };
    let key = dequote_whole_sql_token(trim_ascii(&argument[..equal]));
    if !key.eq_ignore_ascii_case(b"content") {
        return false;
    }
    let value = dequote_whole_sql_token(trim_ascii(&argument[equal + 1..]));
    std::str::from_utf8(&value).is_ok_and(reserved_table_name)
}

fn unquoted_equal(token: &[u8]) -> Option<usize> {
    let mut index = 0;
    let mut depth = 0usize;
    while index < token.len() {
        match token[index] {
            quote @ (b'\'' | b'"' | b'`' | b'[') => {
                let close = if quote == b'[' { b']' } else { quote };
                index += 1;
                while let Some(byte) = token.get(index).copied() {
                    index += 1;
                    if byte == close {
                        if token.get(index) == Some(&close) {
                            index += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            b'(' => {
                depth += 1;
                index += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                index += 1;
            }
            b'=' if depth == 0 => return Some(index),
            _ => index += 1,
        }
    }
    None
}

fn dequote_whole_sql_token(token: &[u8]) -> Vec<u8> {
    let Some(open) = token.first().copied() else {
        return Vec::new();
    };
    let close = match open {
        b'\'' | b'"' | b'`' => open,
        b'[' => b']',
        _ => return token.to_vec(),
    };
    if token.last() != Some(&close) || token.len() < 2 {
        return token.to_vec();
    }
    let mut dequoted = Vec::with_capacity(token.len() - 2);
    let mut index = 1;
    while index + 1 < token.len() {
        let byte = token[index];
        dequoted.push(byte);
        index += 1;
        if byte == close && token.get(index) == Some(&close) {
            index += 1;
        }
    }
    dequoted
}

fn trim_ascii(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(u8::is_ascii_whitespace) {
        value = &value[1..];
    }
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value = &value[..value.len() - 1];
    }
    value
}

fn strip_ascii_prefix_ignore_case<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    value
        .get(..prefix.len())
        .filter(|candidate| candidate.eq_ignore_ascii_case(prefix))
        .map(|_| &value[prefix.len()..])
}

fn validate_temp_schema_empty(conn: &Connection) -> Result<()> {
    let name = conn
        .query_row("SELECT name FROM sqlite_temp_schema LIMIT 1", [], |row| {
            row.get::<_, String>(0)
        })
        .optional()
        .map_err(sqlite_error)?;
    if let Some(name) = name {
        return Err(Error::InvalidCommand(format!(
            "replicated SQL member left TEMP object {name} behind"
        )));
    }
    Ok(())
}

fn bundled_vtable(module_name: &str) -> bool {
    matches_ignore_ascii_case(
        module_name,
        &[
            "fts3",
            "fts3tokenize",
            "fts4",
            "fts4aux",
            "fts5",
            "fts5vocab",
            "dbstat",
            "rtree",
            "rtree_i32",
        ],
    )
}

fn matches_ignore_ascii_case(value: &str, choices: &[&str]) -> bool {
    choices
        .iter()
        .any(|choice| value.eq_ignore_ascii_case(choice))
}

fn unsafe_sql_function(name: &str) -> bool {
    name.eq_ignore_ascii_case("load_extension")
}

fn sql_value(value: ValueRef<'_>) -> Result<SqlValue> {
    Ok(match value {
        ValueRef::Null => SqlValue::Null,
        ValueRef::Integer(value) => SqlValue::Integer(value),
        ValueRef::Real(value) if value.is_finite() => SqlValue::Real(value),
        ValueRef::Real(_) => {
            return Err(Error::InvalidCommand(
                "SQL real result must be finite".into(),
            ));
        }
        ValueRef::Text(value) => SqlValue::Text(
            String::from_utf8(value.to_vec())
                .map_err(|_| Error::InvalidCommand("SQL TEXT result is not valid UTF-8".into()))?,
        ),
        ValueRef::Blob(value) => SqlValue::Blob(value.to_vec()),
    })
}

fn sql_value_size(value: &SqlValue) -> usize {
    match value {
        SqlValue::Null => 0,
        SqlValue::Integer(_) | SqlValue::Real(_) => 8,
        SqlValue::Text(value) => value.len(),
        SqlValue::Blob(value) => value.len(),
    }
}

fn encode_sql_result(result: &SqlCommandResult) -> Result<Vec<u8>> {
    validate_sql_result_bounds(result)?;
    let encoded = serde_json::to_vec(result)
        .map_err(|error| Error::InvalidCommand(format!("cannot encode SQL result: {error}")))?;
    let mut blob = Vec::with_capacity(SQL_RESULT_V1_MAGIC.len() + encoded.len());
    blob.extend_from_slice(SQL_RESULT_V1_MAGIC);
    blob.extend_from_slice(&encoded);
    Ok(blob)
}

fn decode_sql_result(blob: &[u8]) -> Result<SqlCommandResult> {
    let encoded = blob
        .strip_prefix(SQL_RESULT_V1_MAGIC)
        .ok_or_else(|| Error::Sqlite("unsupported SQL result encoding".into()))?;
    let result: SqlCommandResult = serde_json::from_slice(encoded)
        .map_err(|error| Error::Sqlite(format!("invalid SQL result: {error}")))?;
    validate_sql_result_bounds(&result)?;
    if encode_sql_result(&result)? != blob {
        return Err(Error::Sqlite("SQL result blob is not canonical".into()));
    }
    Ok(result)
}

pub(crate) fn validate_sql_result_blob_bounds(blob: &[u8]) -> Result<()> {
    decode_sql_result(blob).map(|_| ())
}

fn validate_sql_result_bounds(result: &SqlCommandResult) -> Result<()> {
    let mut row_count = 0usize;
    let mut bytes = 0usize;
    for statement in &result.statement_results {
        let Some(returning) = &statement.returning else {
            continue;
        };
        row_count = row_count
            .checked_add(returning.rows.len())
            .ok_or_else(|| Error::InvalidCommand("SQL RETURNING row count overflow".into()))?;
        if row_count > MAX_RETURNING_ROWS {
            return Err(Error::InvalidCommand(format!(
                "SQL RETURNING exceeds {MAX_RETURNING_ROWS} rows"
            )));
        }
        for column in &returning.columns {
            add_returning_bytes(&mut bytes, column.len())?;
        }
        for row in &returning.rows {
            if row.len() != returning.columns.len() {
                return Err(Error::InvalidCommand(
                    "SQL RETURNING row width does not match its columns".into(),
                ));
            }
            for value in row {
                add_returning_bytes(&mut bytes, sql_value_size(value))?;
            }
        }
    }
    Ok(())
}

fn integrity_check(conn: &Connection) -> Result<()> {
    let mut statement = conn
        .prepare("PRAGMA integrity_check;")
        .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
    let mut rows = statement
        .query([])
        .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
    let mut messages = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|err| Error::InvalidSnapshot(err.to_string()))?
    {
        messages.push(
            row.get::<_, String>(0)
                .map_err(|err| Error::InvalidSnapshot(err.to_string()))?,
        );
    }
    if messages == ["ok"] {
        return Ok(());
    }
    Err(Error::InvalidSnapshot(if messages.is_empty() {
        "integrity_check returned no result".into()
    } else {
        messages.join("; ")
    }))
}

fn ensure_parent(path: &Path) -> Result<()> {
    let parent = parent_dir(path);
    fs::create_dir_all(parent).map_err(io_error)
}

fn parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn sync_parent(parent: &Path) -> Result<()> {
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(io_error)
}

fn sqlite_error(error: rusqlite::Error) -> Error {
    Error::Sqlite(error.to_string())
}

fn sql_query_error(error: rusqlite::Error) -> Error {
    match &error {
        rusqlite::Error::SqliteFailure(code, _)
            if code.code == rusqlite::ffi::ErrorCode::OperationInterrupted =>
        {
            Error::ResourceExhausted("SQL query execution timed out".into())
        }
        _ => sqlite_error(error),
    }
}

fn io_error(error: std::io::Error) -> Error {
    Error::Io(error.to_string())
}

#[cfg(test)]
mod qwal_identity_tests {
    use super::*;

    #[test]
    fn qwal_identity_reports_the_first_static_mismatch() {
        let state = StateIdentityV3 {
            page_size: 4096,
            page_count: 1,
            state_root: LogHash::ZERO,
        };
        let configuration = ConfigurationState::active(7, LogHash::ZERO);
        let identity = ControlIdentity::new(
            "cluster-a",
            "node-a",
            3,
            configuration.clone(),
            2,
            LogHash::digest(&[b"fingerprint"]),
            state,
        );
        let effect = QwalEnvelopeV3 {
            cluster_id: "cluster-a".into(),
            epoch: 3,
            configuration_id: 7,
            recovery_generation: 2,
            base_index: 0,
            base_hash: LogHash::ZERO,
            base_state: state,
            target_state: state,
            materializer_fingerprint: identity.materializer_fingerprint().to_hex(),
            receipts: Vec::new(),
            pages: Vec::new(),
        };
        let assert_field = |effect: QwalEnvelopeV3, field| {
            assert!(matches!(
                validate_qwal_identity(&effect, &identity, &configuration),
                Err(Error::InvalidEntry(message)) if message == format!("QWAL effect identity mismatch: {field}")
            ));
        };
        for field in [
            "cluster_id",
            "epoch",
            "configuration_id",
            "recovery_generation",
            "materializer_fingerprint",
            "base_state",
        ] {
            let mut changed = effect.clone();
            match field {
                "cluster_id" => changed.cluster_id.push('!'),
                "epoch" => changed.epoch += 1,
                "configuration_id" => changed.configuration_id += 1,
                "recovery_generation" => changed.recovery_generation += 1,
                "materializer_fingerprint" => changed.materializer_fingerprint.push('!'),
                "base_state" => changed.base_state.state_root = LogHash::digest(&[b"other-state"]),
                _ => unreachable!(),
            }
            assert_field(changed, field);
        }
        let mut changed = effect;
        changed.cluster_id.push('!');
        changed.base_state.state_root = LogHash::digest(&[b"other-state"]);
        assert_field(changed, "cluster_id");
    }
}

#[cfg(test)]
mod query_policy_tests {
    use super::*;

    #[test]
    fn reserved_name_and_vtable_content_checks_match_only_structural_sentinels() {
        for name in ["__rhiza_kv", "sqlite_autoindex___rhiza_kv_1"] {
            assert!(reserved_name(name));
        }
        for name in [
            "__rhiza_user_table",
            "__RHIZA_META",
            "__rhiza_requests",
            "x__rhiza_kv",
            "sqlite_autoindex_user_1",
        ] {
            assert!(!reserved_name(name));
        }
        for sql in [
            "CREATE VIRTUAL TABLE x USING fts5(body, content='__rhiza_kv')",
            "CREATE VIRTUAL TABLE x USING fts5(prefix=(a,b), CoNtEnT /* option */ = '__rhiza_kv')",
        ] {
            assert!(virtual_table_uses_reserved_content(sql));
        }
        for sql in [
            "CREATE VIRTUAL TABLE x USING fts5(__rhiza_kv)",
            "CREATE VIRTUAL TABLE x USING fts5(body, tokenize='__rhiza_kv')",
            "CREATE VIRTUAL TABLE x USING fts5(body, content='__rhiza_kv_user')",
            "CREATE VIRTUAL TABLE x USING fts5(body, CONTENT = [__RHIZA_META])",
            "CREATE VIRTUAL TABLE x USING fts5(body, content=__rhiza_requests)",
            "CREATE VIRTUAL TABLE x USING fts5(body, 'content=__rhiza_kv')",
            "CREATE VIRTUAL TABLE x USING fts5(body, \"content=__rhiza_meta\")",
            "CREATE VIRTUAL TABLE x USING fts5(body, `content=__rhiza_requests`)",
            "CREATE VIRTUAL TABLE x USING fts5(body, [content=__rhiza_kv])",
            "CREATE VIRTUAL TABLE x USING fts5(body, 'content=''__rhiza_kv''')",
            "CREATE VIRTUAL TABLE x USING fts5(body, /* content='__rhiza_kv' */ tokenize='porter')",
            "CREATE VIRTUAL TABLE x USING fts5(body, -- content='__rhiza_kv'\n tokenize='porter')",
        ] {
            assert!(!virtual_table_uses_reserved_content(sql));
        }
    }

    fn prepare_single_sql_effect(
        database: &SqliteStateMachine,
        command: &SqlCommand,
        request: &[u8],
        base_index: LogIndex,
        base_hash: LogHash,
    ) -> Result<Vec<u8>> {
        let preparation = database.prepare_sql_batch_effect(
            &[SqlBatchMember {
                command,
                request_payload: request,
            }],
            base_index,
            base_hash,
        )?;
        preparation
            .results
            .into_iter()
            .next()
            .expect("one-member batch returns one result")?;
        preparation
            .effect
            .ok_or_else(|| Error::InvalidCommand("successful batch produced no effect".into()))
    }

    #[test]
    fn speculative_prepare_uses_canonical_wal_without_weakening_canonical() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "speculative-durability".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE speculative(value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let base_digest = database.canonical_db_digest().unwrap();
        let base_control = fs::read(control_sidecar_path(&path)).unwrap();

        prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();

        let audit = speculative_prepare_audit(&path).expect("prepare records its test audit");
        assert_eq!((audit.copy_count, audit.copy_bytes), (0, 0));
        assert_eq!(audit.synchronous, 0);
        assert!(audit.native_vfs);
        assert!(audit.no_checkpoint_on_close);
        assert_eq!(database.connection_pragmas().unwrap(), ("wal".into(), 0));
        assert_eq!(database.canonical_db_digest().unwrap(), base_digest);
        assert_eq!(fs::read(control_sidecar_path(&path)).unwrap(), base_control);
        assert_eq!(
            fs::metadata(sqlite_sidecar_path(&path, "-wal"))
                .map(|metadata| metadata.len())
                .unwrap_or(0),
            0
        );
    }

    #[test]
    fn batch_capacity_rejects_1025_members_before_speculative_io() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "over-capacity".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE must_not_run(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let payload = encode_sql_command(&command).unwrap();
        let member = SqlBatchMember {
            command: &command,
            request_payload: &payload,
        };

        assert!(matches!(
            database.prepare_sql_batch_effect(&vec![member; 1025], 0, LogHash::ZERO),
            Err(Error::InvalidCommand(_))
        ));
        assert_eq!(speculative_prepare_audit(&path), None);
        assert_eq!(database.applied_tip_value().unwrap(), (0, LogHash::ZERO));
    }

    #[test]
    fn capacity_sized_batch_rejects_a_duplicate_request_id_at_the_tail() {
        let dir = tempfile::tempdir().unwrap();
        let database =
            SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
                .unwrap();
        let commands = (0usize..MAX_QWAL_V3_RECEIPTS)
            .map(|index| SqlCommand {
                request_id: if index + 1 == MAX_QWAL_V3_RECEIPTS {
                    "request-0000".into()
                } else {
                    format!("request-{index:04}")
                },
                statements: vec![SqlStatement {
                    sql: "INSERT INTO absent_table(value) VALUES (?1)".into(),
                    parameters: vec![SqlValue::Integer(index as i64)],
                }],
            })
            .collect::<Vec<_>>();
        let payloads = commands
            .iter()
            .map(|command| encode_sql_command(command).unwrap())
            .collect::<Vec<_>>();
        let members = commands
            .iter()
            .zip(&payloads)
            .map(|(command, request_payload)| SqlBatchMember {
                command,
                request_payload,
            })
            .collect::<Vec<_>>();

        let preparation = database
            .prepare_sql_batch_effect(&members, 0, LogHash::ZERO)
            .unwrap();

        assert!(preparation.effect.is_none());
        assert_eq!(preparation.results.len(), MAX_QWAL_V3_RECEIPTS);
        assert!(matches!(
            preparation.results.last().unwrap(),
            Err(Error::InvalidCommand(message)) if message.contains("repeats a request_id")
        ));
        assert_eq!(database.applied_tip_value().unwrap(), (0, LogHash::ZERO));
    }

    #[test]
    fn applied_tip_returns_index_and_hash_from_the_same_database_state() {
        let dir = tempfile::tempdir().unwrap();
        let database =
            SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
                .unwrap();
        let request = b"put\trequest-1\tkey-1\tvalue-1";
        let payload = database
            .prepare_put_effect("request-1", "key-1", "value-1", request, 0, LogHash::ZERO)
            .unwrap();
        let hash = LogEntry::calculate_hash(
            "cluster-a",
            1,
            1,
            1,
            EntryType::Command,
            LogHash::ZERO,
            &payload,
        );
        database
            .apply_entry(&LogEntry {
                cluster_id: "cluster-a".into(),
                epoch: 1,
                config_id: 1,
                index: 1,
                entry_type: EntryType::Command,
                payload: payload.to_vec(),
                prev_hash: LogHash::ZERO,
                hash,
            })
            .unwrap();

        assert_eq!(database.applied_tip().unwrap(), ApplyProgress::new(1, hash));
        assert_eq!(database.applied_tip_value().unwrap(), (1, hash));
    }

    #[test]
    fn bulk_sql_request_check_aligns_exact_absent_and_conflict_results() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let committed = SqlCommand {
            request_id: "committed".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE committed(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let committed_payload = encode_sql_command(&committed).unwrap();
        let effect =
            prepare_single_sql_effect(&database, &committed, &committed_payload, 0, LogHash::ZERO)
                .unwrap();
        let entry = command_entry(1, LogHash::ZERO, effect);
        database.apply_entry(&entry).unwrap();
        let absent = SqlCommand {
            request_id: "absent".into(),
            statements: vec![SqlStatement {
                sql: "SELECT 1".into(),
                parameters: vec![],
            }],
        };
        let absent_payload = encode_sql_command(&absent).unwrap();

        let aligned = database
            .check_sql_requests(&[
                ("committed", committed_payload.as_slice()),
                ("absent", absent_payload.as_slice()),
            ])
            .unwrap();
        assert!(matches!(
            aligned.as_slice(),
            [Ok(Some((outcome, Some(_)))), Ok(None)]
                if *outcome == RequestOutcome::new(1, entry.hash)
        ));

        let conflicting = SqlCommand {
            request_id: "committed".into(),
            statements: vec![SqlStatement {
                sql: "SELECT 2".into(),
                parameters: vec![],
            }],
        };
        let conflicting_payload = encode_sql_command(&conflicting).unwrap();
        assert!(matches!(
            database
                .check_sql_requests(&[("committed", conflicting_payload.as_slice())])
                .unwrap()
                .pop()
                .unwrap(),
            Err(Error::RequestConflict(_))
        ));
        assert!(matches!(
            database.check_sql_requests(&[
                ("committed", committed_payload.as_slice()),
                ("committed", committed_payload.as_slice()),
            ]),
            Err(Error::InvalidCommand(message)) if message.contains("duplicate")
        ));
    }

    #[test]
    fn read_query_timeout_interrupts_work_and_releases_the_connection() {
        let dir = tempfile::tempdir().unwrap();
        let database =
            SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
                .unwrap();
        let expensive = SqlStatement {
            sql: "WITH RECURSIVE count(value) AS (VALUES(0) UNION ALL SELECT value + 1 FROM count WHERE value < 100000000) SELECT sum(value) FROM count".into(),
            parameters: vec![],
        };

        assert_eq!(
            database
                .query_sql_with_timeout(&expensive, 1, 1024, Duration::ZERO)
                .unwrap_err(),
            Error::ResourceExhausted("SQL query execution timed out".into())
        );

        let quick = database
            .query_sql(
                &SqlStatement {
                    sql: "SELECT 1".into(),
                    parameters: vec![],
                },
                1,
                1024,
            )
            .unwrap();
        assert_eq!(quick.rows, vec![vec![SqlValue::Integer(1)]]);
    }

    #[test]
    fn read_query_allows_nondeterministic_and_runtime_introspection_functions() {
        let dir = tempfile::tempdir().unwrap();
        let database =
            SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
                .unwrap();

        let result = database
            .query_sql(
                &SqlStatement {
                    sql: "SELECT random(), datetime('now'), sqlite_version()".into(),
                    parameters: vec![],
                },
                1,
                4096,
            )
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].len(), 3);
    }

    #[test]
    fn normal_reads_do_not_query_the_control_pending_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let control_path = control_sidecar_path(&path);
        control::begin_pending_query_audit(&control_path);
        let command = SqlCommand {
            request_id: "pending-audit".into(),
            statements: vec![SqlStatement {
                sql: "SELECT 1".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();

        assert_eq!(database.get_value("absent").unwrap(), None);
        database
            .query_sql(
                &SqlStatement {
                    sql: "SELECT 1".into(),
                    parameters: vec![],
                },
                1,
                64,
            )
            .unwrap();
        assert_eq!(
            database.check_request("pending-audit", &request).unwrap(),
            None
        );
        assert!(matches!(
            database
                .check_sql_requests(&[("pending-audit", request.as_slice())])
                .unwrap()
                .as_slice(),
            [Ok(None)]
        ));

        assert_eq!(control::pending_query_count(&control_path), Some(0));
    }

    #[test]
    fn physical_effect_preparation_accepts_nondeterministic_functions() {
        let dir = tempfile::tempdir().unwrap();
        let database =
            SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
                .unwrap();
        let command = SqlCommand {
            request_id: "nondeterministic-write".into(),
            statements: vec![
                SqlStatement {
                    sql: "CREATE TABLE generated(value INTEGER DEFAULT (random()))".into(),
                    parameters: vec![],
                },
                SqlStatement {
                    sql: "INSERT INTO generated DEFAULT VALUES".into(),
                    parameters: vec![],
                },
            ],
        };

        let payload = encode_sql_command(&command).unwrap();
        let effect =
            prepare_single_sql_effect(&database, &command, &payload, 0, LogHash::ZERO).unwrap();
        assert!(effect.starts_with(QWAL_V3_MAGIC));
        assert_eq!(database.applied_index_value().unwrap(), 0);
    }

    #[test]
    fn non_durable_staging_uses_native_wal_capture_without_checkpoint_or_full_diff() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "recorded-write".into(),
            statements: vec![
                SqlStatement {
                    sql: "CREATE TABLE recorded(value TEXT NOT NULL)".into(),
                    parameters: vec![],
                },
                SqlStatement {
                    sql: "INSERT INTO recorded(value) VALUES ('shadow')".into(),
                    parameters: vec![],
                },
            ],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();

        let audit = speculative_prepare_audit(&path).unwrap();
        assert!(audit.native_vfs);
        assert!(audit.no_checkpoint_on_close);
        assert_eq!(
            fs::metadata(sqlite_sidecar_path(&path, "-wal"))
                .map(|metadata| metadata.len())
                .unwrap_or(0),
            0
        );
        let effect = decode_qwal_v3(&payload).unwrap();
        let hash = LogEntry::calculate_hash(
            "cluster-a",
            1,
            1,
            1,
            EntryType::Command,
            LogHash::ZERO,
            &payload,
        );
        database
            .apply_entry(&LogEntry {
                cluster_id: "cluster-a".into(),
                epoch: 1,
                config_id: 1,
                index: 1,
                entry_type: EntryType::Command,
                payload,
                prev_hash: LogHash::ZERO,
                hash,
            })
            .unwrap();
        assert_eq!(
            rebuild_page_state(&path).unwrap().identity(),
            effect.target_state
        );
    }

    #[test]
    fn prepare_fails_closed_without_changing_canonical_or_control_when_held_wal_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "unstable-held-wal".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE unstable(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let base_digest = database.canonical_db_digest().unwrap();
        arm_wal_capture_fault(&path, WalCaptureFault::ChangeHeldWalAfterSeal);

        let error =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap_err();

        assert!(
            matches!(
                &error,
                Error::InvalidEntry(message) if message.contains("changed after capture was sealed")
            ),
            "{error:?}"
        );
        assert_eq!(database.canonical_db_digest().unwrap(), base_digest);
        assert_eq!(database.applied_tip_value().unwrap(), (0, LogHash::ZERO));
        assert_eq!(
            database
                .check_sql_request("unstable-held-wal", &request)
                .unwrap(),
            None
        );
        assert_eq!(
            fs::metadata(sqlite_sidecar_path(&path, "-wal"))
                .map(|metadata| metadata.len())
                .unwrap_or(0),
            0
        );
        assert!(fs::read_dir(dir.path()).unwrap().all(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("state.sqlite")
        }));
    }

    #[test]
    fn successful_noop_uses_explicit_no_change_effect_and_still_replays_its_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let setup = SqlCommand {
            request_id: "no-physical-change-setup".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE noop_target(value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let setup_request = encode_sql_command(&setup).unwrap();
        let setup_payload =
            prepare_single_sql_effect(&database, &setup, &setup_request, 0, LogHash::ZERO).unwrap();
        let setup_entry = command_entry(1, LogHash::ZERO, setup_payload);
        database.apply_entry(&setup_entry).unwrap();
        let command = SqlCommand {
            request_id: "no-physical-change".into(),
            statements: vec![SqlStatement {
                sql: "UPDATE noop_target SET value = 'unused' WHERE rowid = -1".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let base_digest = database.canonical_db_digest().unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 1, setup_entry.hash).unwrap();
        let effect = decode_qwal_v3(&payload).unwrap();
        assert!(effect.pages.is_empty());
        assert_eq!(effect.base_state, effect.target_state);

        let entry = command_entry(2, setup_entry.hash, payload);
        database.apply_entry(&entry).unwrap();

        assert_eq!(database.canonical_db_digest().unwrap(), base_digest);
        assert!(database
            .check_sql_request("no-physical-change", &request)
            .unwrap()
            .is_some());
    }

    #[test]
    fn no_change_control_failure_blocks_everything_except_exact_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let setup = SqlCommand {
            request_id: "no-change-fault-setup".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE no_change_fault(value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let setup_request = encode_sql_command(&setup).unwrap();
        let setup_payload =
            prepare_single_sql_effect(&database, &setup, &setup_request, 0, LogHash::ZERO).unwrap();
        let setup_entry = command_entry(1, LogHash::ZERO, setup_payload);
        database.apply_entry(&setup_entry).unwrap();

        let command = SqlCommand {
            request_id: "no-change-fault".into(),
            statements: vec![SqlStatement {
                sql: "UPDATE no_change_fault SET value = 'unused' WHERE rowid = -1".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 1, setup_entry.hash).unwrap();
        let effect = decode_qwal_v3(&payload).unwrap();
        assert!(effect.pages.is_empty());
        assert_eq!(effect.base_state, effect.target_state);
        let entry = command_entry(2, setup_entry.hash, payload);
        arm_qwal_apply_fault(&path, QwalApplyFault::BeforeControlCommit);

        assert!(database.apply_entry(&entry).is_err());
        assert!(database.read_state.pending_fence.load(Ordering::Acquire));
        assert!(database.get_value("blocked").is_err());
        assert!(database
            .query_sql(
                &SqlStatement {
                    sql: "SELECT 1".into(),
                    parameters: vec![],
                },
                1,
                64,
            )
            .is_err());
        assert!(database.check_request("no-change-fault", &request).is_err());
        assert!(database
            .prepare_sql_batch_effect(
                &[SqlBatchMember {
                    command: &command,
                    request_payload: &request,
                }],
                1,
                setup_entry.hash,
            )
            .is_err());
        let different = LogEntry {
            cluster_id: "cluster-a".into(),
            epoch: 1,
            config_id: 1,
            index: 2,
            entry_type: EntryType::Noop,
            payload: Vec::new(),
            prev_hash: setup_entry.hash,
            hash: LogEntry::calculate_hash(
                "cluster-a",
                2,
                1,
                1,
                EntryType::Noop,
                setup_entry.hash,
                &[],
            ),
        };
        assert!(database.apply_entry(&different).is_err());

        database.apply_entry(&entry).unwrap();
        assert!(!database.read_state.pending_fence.load(Ordering::Acquire));
        assert_eq!(database.applied_tip_value().unwrap(), (2, entry.hash));
        assert_eq!(database.get_value("unblocked").unwrap(), None);
        assert!(database
            .check_sql_request("no-change-fault", &request)
            .unwrap()
            .is_some());
    }

    #[test]
    fn exact_consensus_winner_applies_the_decided_effect() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "prepared-winner".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE promoted(value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        assert_eq!(
            database
                .query_sql(
                    &SqlStatement {
                        sql: "SELECT 1".into(),
                        parameters: vec![],
                    },
                    1,
                    1024,
                )
                .unwrap()
                .rows,
            vec![vec![SqlValue::Integer(1)]]
        );
        let entry = command_entry(1, LogHash::ZERO, payload);

        let outcome = database.apply_entry_with_result(&entry).unwrap();

        assert_eq!(
            prepared_install_path(&path),
            Some(PreparedInstallPath::Patched)
        );
        assert_eq!(outcome.progress(), ApplyProgress::new(1, entry.hash));
        assert_eq!(
            database
                .check_sql_request("prepared-winner", &request)
                .unwrap()
                .unwrap()
                .0,
            RequestOutcome::new(1, entry.hash)
        );
    }

    #[test]
    fn competing_prepared_effects_apply_only_the_decided_winner() {
        let dir = tempfile::tempdir().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let foreign_path = dir.path().join("foreign.sqlite");
        let local = SqliteStateMachine::open(&local_path, "cluster-a", "node-1", 1, 1).unwrap();
        let foreign = SqliteStateMachine::open(&foreign_path, "cluster-a", "node-2", 1, 1).unwrap();
        let local_command = SqlCommand {
            request_id: "local-loser".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE local_only(value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let local_request = encode_sql_command(&local_command).unwrap();
        let _ = prepare_single_sql_effect(&local, &local_command, &local_request, 0, LogHash::ZERO)
            .unwrap();
        let foreign_command = SqlCommand {
            request_id: "foreign-winner".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE foreign_only(value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let foreign_request = encode_sql_command(&foreign_command).unwrap();
        let foreign_effect = prepare_single_sql_effect(
            &foreign,
            &foreign_command,
            &foreign_request,
            0,
            LogHash::ZERO,
        )
        .unwrap();

        local
            .apply_entry(&command_entry(1, LogHash::ZERO, foreign_effect))
            .unwrap();

        assert_eq!(
            prepared_install_path(&local_path),
            Some(PreparedInstallPath::Patched)
        );
        assert_eq!(
            local
                .query_sql(
                    &SqlStatement {
                        sql: "SELECT name FROM sqlite_schema WHERE name = 'foreign_only'".into(),
                        parameters: vec![],
                    },
                    1,
                    1024,
                )
                .unwrap()
                .rows,
            vec![vec![SqlValue::Text("foreign_only".into())]]
        );
    }

    #[cfg(unix)]
    #[test]
    fn same_size_canonical_mutation_is_rejected_before_qwal_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "sealed-base-mutation".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE must_not_be_installed(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let entry = command_entry(1, LogHash::ZERO, payload);
        {
            let _lifecycle = database.lock_lifecycle().unwrap();
            database.close_connection().unwrap();
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let offset = file.metadata().unwrap().len() - 1;
            file.seek(SeekFrom::Start(offset)).unwrap();
            let mut byte = [0_u8; 1];
            file.read_exact(&mut byte).unwrap();
            byte[0] ^= 0xff;
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.write_all(&byte).unwrap();
        }
        let mutated = fs::read(&path).unwrap();

        assert!(matches!(
            database.apply_entry(&entry),
            Err(Error::InvalidEntry(message)) if message.contains("page-state seal")
        ));
        assert_eq!(fs::read(&path).unwrap(), mutated);
        assert_eq!(database.applied_tip_value().unwrap(), (0, LogHash::ZERO));
    }

    #[test]
    fn interrupted_in_place_apply_is_rejected_on_reopen_for_recorder_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let proposer_path = dir.path().join("proposer.sqlite");
        let follower_path = dir.path().join("follower.sqlite");
        let proposer =
            SqliteStateMachine::open(&proposer_path, "cluster-a", "node-1", 1, 1).unwrap();
        let follower =
            SqliteStateMachine::open(&follower_path, "cluster-a", "node-2", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "interrupt-in-place".into(),
            statements: vec![
                SqlStatement {
                    sql: "CREATE TABLE interrupted(value BLOB NOT NULL)".into(),
                    parameters: vec![],
                },
                SqlStatement {
                    sql: "INSERT INTO interrupted VALUES (zeroblob(20000))".into(),
                    parameters: vec![],
                },
            ],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&proposer, &command, &request, 0, LogHash::ZERO).unwrap();
        let effect = decode_qwal_v3(&payload).unwrap();
        assert!(effect.pages.len() > 1);
        arm_qwal_apply_fault(
            &follower_path,
            QwalApplyFault::AfterPage(effect.pages[0].page_no),
        );

        assert!(follower
            .apply_entry(&command_entry(1, LogHash::ZERO, payload))
            .is_err());
        assert!(matches!(
            follower.query_sql(&SqlStatement { sql: "SELECT 1".into(), parameters: vec![] }, 1, 64),
            Err(Error::InvalidEntry(message)) if message.contains("pending")
        ));
        drop(follower);
        assert!(matches!(
            SqliteStateMachine::open_existing(&follower_path),
            Err(Error::InvalidEntry(message)) if message.contains("neither base nor target")
        ));
    }

    #[test]
    fn startup_discards_leftover_wal_when_main_matches_control() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        drop(database);

        let external = Connection::open(&path).unwrap();
        external
            .pragma_update(None, "wal_autocheckpoint", 0)
            .unwrap();
        assert!(external
            .set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)
            .unwrap());
        external
            .execute_batch(
                "CREATE TABLE untracked_wal(value TEXT NOT NULL);
                 INSERT INTO untracked_wal VALUES ('must-not-be-visible');",
            )
            .unwrap();
        assert!(
            fs::metadata(sqlite_sidecar_path(&path, "-wal"))
                .unwrap()
                .len()
                > 0
        );

        drop(external);
        let reopened = SqliteStateMachine::open_existing(&path).unwrap();
        assert!(reopened
            .query_sql(
                &SqlStatement {
                    sql: "SELECT * FROM untracked_wal".into(),
                    parameters: vec![],
                },
                1,
                1024,
            )
            .is_err());
    }

    #[test]
    fn startup_preserves_leftover_wal_when_main_mismatches_control() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        database.close_for_handoff().unwrap();

        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.last_mut().unwrap();
        *last ^= 1;
        fs::write(&path, bytes).unwrap();
        let wal = sqlite_sidecar_path(&path, "-wal");
        fs::write(&wal, b"leftover speculative WAL").unwrap();

        assert!(matches!(
            SqliteStateMachine::open_existing(&path),
            Err(Error::InvalidEntry(message)) if message.contains("does not match the committed control")
        ));
        assert_eq!(fs::read(&wal).unwrap(), b"leftover speculative WAL");
    }

    #[test]
    fn post_install_control_failure_closes_reads_until_exact_replay_commits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "control-failure".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE installed_before_control(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let entry = command_entry(1, LogHash::ZERO, payload);
        arm_qwal_apply_fault(&path, QwalApplyFault::BeforeControlCommit);

        assert!(database.apply_entry(&entry).is_err());
        assert_eq!(database.applied_tip_value().unwrap(), (0, LogHash::ZERO));
        assert!(matches!(
            database.query_sql(&SqlStatement { sql: "SELECT 1".into(), parameters: vec![] }, 1, 64),
            Err(Error::InvalidEntry(message)) if message.contains("pending")
        ));
        assert!(matches!(
            database.check_request("control-failure", &request),
            Err(Error::Sqlite(message)) if message.contains("closed")
        ));
        assert!(matches!(
            database.check_sql_requests(&[("control-failure", request.as_slice())]),
            Err(Error::Sqlite(message)) if message.contains("closed")
        ));

        let control_path = control_sidecar_path(&path);
        control::begin_pending_query_audit(&control_path);
        database.apply_entry(&entry).unwrap();
        assert_eq!(control::pending_query_count(&control_path), Some(0));
        assert_eq!(database.applied_tip_value().unwrap(), (1, entry.hash));
        assert_eq!(
            database
                .query_sql(
                    &SqlStatement {
                        sql:
                            "SELECT name FROM sqlite_schema WHERE name = 'installed_before_control'"
                                .into(),
                        parameters: vec![],
                    },
                    1,
                    256,
                )
                .unwrap()
                .rows,
            vec![vec![SqlValue::Text("installed_before_control".into())]]
        );
    }

    #[test]
    fn pre_install_failure_keeps_exact_retry_state_and_retry_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "pre-install-retry".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE pre_install_retry(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let effect = decode_qwal_v3(&payload).unwrap();
        let entry = command_entry(1, LogHash::ZERO, payload);
        arm_qwal_apply_fault(&path, QwalApplyFault::BeforeInstallCheckpoint);

        assert!(matches!(
            database.apply_entry(&entry),
            Err(Error::Sqlite(message)) if message.contains("before canonical QWAL checkpoint")
        ));
        assert!(database.read_state.pending_fence.load(Ordering::Acquire));
        assert_eq!(
            database.control.pending().unwrap(),
            Some(pending_for(&entry, &effect))
        );
        assert_eq!(
            rebuild_page_state(&path).unwrap().identity(),
            effect.base_state
        );

        database.apply_entry(&entry).unwrap();
        assert!(!database.read_state.pending_fence.load(Ordering::Acquire));
        assert_eq!(database.control.pending().unwrap(), None);
        assert_eq!(
            rebuild_page_state(&path).unwrap().identity(),
            effect.target_state
        );
    }

    #[test]
    fn external_effect_faults_preserve_exact_pending_retry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("external-retry.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let prepare = |request_id: &str, sql: &str, base_index, base_hash| {
            let command = SqlCommand {
                request_id: request_id.into(),
                statements: vec![SqlStatement {
                    sql: sql.into(),
                    parameters: vec![],
                }],
            };
            let request = encode_sql_command(&command).unwrap();
            database
                .prepare_external_sql_batch_effect(
                    &[SqlBatchMember {
                        command: &command,
                        request_payload: &request,
                    }],
                    base_index,
                    base_hash,
                )
                .unwrap()
                .effect
                .unwrap()
        };

        let prepared = prepare(
            "external-preinstall",
            "CREATE TABLE external_retry(v)",
            0,
            LogHash::ZERO,
        );
        let command = prepared
            .build_command(LogHash::ZERO, 1, LogHash::ZERO)
            .unwrap();
        let verified = prepared.verify(&command).unwrap();
        let competing = prepare(
            "external-competing-retry",
            "CREATE TABLE competing_retry(v)",
            0,
            LogHash::ZERO,
        );
        let competing_command = competing
            .build_command(LogHash::ZERO, 1, LogHash::ZERO)
            .unwrap();
        let competing = competing.verify(&competing_command).unwrap();
        arm_qwal_apply_fault(&path, QwalApplyFault::BeforeInstallCheckpoint);
        assert!(database.apply_verified_external_effect(&verified).is_err());
        assert!(database.read_state.pending_fence.load(Ordering::Acquire));
        assert!(matches!(
            database.apply_verified_external_effect(&competing),
            Err(Error::InvalidEntry(message)) if message.contains("exact")
        ));
        database.apply_verified_external_effect(&verified).unwrap();
        assert!(!database.read_state.pending_fence.load(Ordering::Acquire));

        let tip = database.applied_tip_value().unwrap();
        let prepared = prepare(
            "external-postinstall",
            "INSERT INTO external_retry VALUES (1)",
            tip.0,
            tip.1,
        );
        let command = prepared.build_command(LogHash::ZERO, 2, tip.1).unwrap();
        let verified = prepared.verify(&command).unwrap();
        arm_qwal_apply_fault(&path, QwalApplyFault::BeforeControlCommit);
        assert!(database.apply_verified_external_effect(&verified).is_err());
        assert!(database.read_state.pending_fence.load(Ordering::Acquire));
        database.apply_verified_external_effect(&verified).unwrap();
        assert!(!database.read_state.pending_fence.load(Ordering::Acquire));
        assert_eq!(database.applied_index_value().unwrap(), 2);
    }

    #[test]
    fn external_prepare_captures_one_lifecycle_configuration_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("external-configuration.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let old_identity = database.control.identity().unwrap();
        let old_configuration = old_identity.configuration_state();
        let old_digest = old_configuration.digest();
        let lifecycle = Arc::clone(&database.read_state);
        let (entered, release) = arm_external_prepare_test_gate(&path);

        let prepared_thread = std::thread::spawn(move || {
            let command = SqlCommand {
                request_id: "configuration-snapshot".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE configuration_snapshot(v)".into(),
                    parameters: vec![],
                }],
            };
            let request = encode_sql_command(&command).unwrap();
            let preparation = database
                .prepare_external_sql_batch_effect(
                    &[SqlBatchMember {
                        command: &command,
                        request_payload: &request,
                    }],
                    0,
                    LogHash::ZERO,
                )
                .unwrap();
            assert!(preparation.results[0].is_ok());
            let prepared = preparation.effect.unwrap();
            (database, prepared)
        });
        entered.recv().unwrap();

        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        *lifecycle.writer_waiting_hook.lock().unwrap() = Some(waiting_tx);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let _writer = lifecycle.lock_write().unwrap();
            done_tx.send(()).unwrap();
        });
        waiting_rx.recv().unwrap();
        assert!(matches!(
            done_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));

        release.send(()).unwrap();
        let (database, prepared) = prepared_thread.join().unwrap();
        done_rx.recv().unwrap();
        writer.join().unwrap();
        disarm_external_prepare_test_gate(&path);

        assert_eq!(prepared.configuration_id, 1);
        assert_eq!(prepared.configuration_digest, old_digest);
        let command = prepared
            .build_command(old_digest, 1, LogHash::ZERO)
            .unwrap();
        let verified = prepared.verify(&command).unwrap();
        database.apply_verified_external_effect(&verified).unwrap();
        assert!(prepared
            .build_command(LogHash::digest(&[b"new-configuration"]), 1, LogHash::ZERO)
            .is_err());
    }

    #[test]
    fn external_put_applies_and_rejects_duplicate_or_conflicting_preparation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("external-put.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let request = encode_put_request("external-put", "key", "value").unwrap();
        let prepared = database
            .prepare_external_put_effect("external-put", "key", "value", &request, 0, LogHash::ZERO)
            .unwrap();
        let command = prepared
            .build_command(LogHash::ZERO, 1, LogHash::ZERO)
            .unwrap();
        let verified = prepared.verify(&command).unwrap();
        let applied = database.apply_verified_external_effect(&verified).unwrap();
        assert!(applied.sql_result().is_some());
        assert_eq!(database.get_value("key").unwrap().as_deref(), Some("value"));

        let duplicate = database.apply_verified_external_effect(&verified).unwrap();
        assert_eq!(duplicate.progress(), applied.progress());
        assert!(duplicate.sql_result().is_some());
        assert!(matches!(
            database.prepare_external_put_effect(
                "external-put",
                "key",
                "value",
                &request,
                applied.progress().applied_index(),
                applied.progress().applied_hash(),
            ),
            Err(Error::InvalidCommand(message)) if message.contains("already materialized")
        ));

        let conflict_request =
            encode_put_request("external-put", "different-key", "different-value").unwrap();
        assert!(matches!(
            database.prepare_external_put_effect(
                "external-put",
                "different-key",
                "different-value",
                &conflict_request,
                applied.progress().applied_index(),
                applied.progress().applied_hash(),
            ),
            Err(Error::RequestConflict(_))
        ));
        assert_eq!(database.get_value("different-key").unwrap(), None);
    }

    #[test]
    fn pending_commit_failure_keeps_reads_fenced_until_exact_replay_clears_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let change = rhiza_core::ConfigChange::bound_stop(
            "cluster-a",
            1,
            LogHash::ZERO,
            2,
            vec!["node-1".into(), "node-2".into(), "node-3".into()],
        )
        .unwrap()
        .to_stored_command();
        let hash = LogEntry::calculate_hash(
            "cluster-a",
            1,
            1,
            1,
            change.entry_type,
            LogHash::ZERO,
            &change.payload,
        );
        let entry = LogEntry {
            cluster_id: "cluster-a".into(),
            epoch: 1,
            config_id: 1,
            index: 1,
            entry_type: change.entry_type,
            payload: change.payload,
            prev_hash: LogHash::ZERO,
            hash,
        };
        arm_qwal_apply_fault(&path, QwalApplyFault::BeforePendingCommit);

        assert!(database.apply_entry(&entry).is_err());
        assert!(database.read_state.pending_fence.load(Ordering::Acquire));
        assert!(database.control.pending().unwrap().is_some());
        assert!(matches!(
            database.get_value("blocked"),
            Err(Error::InvalidEntry(message)) if message.contains("pending")
        ));

        database.apply_entry(&entry).unwrap();
        assert!(!database.read_state.pending_fence.load(Ordering::Acquire));
        assert_eq!(database.control.pending().unwrap(), None);
        assert_eq!(database.get_value("unblocked").unwrap(), None);
    }

    #[test]
    fn pending_begin_failure_does_not_orphan_the_read_fence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let change = rhiza_core::ConfigChange::bound_stop(
            "cluster-a",
            1,
            LogHash::ZERO,
            2,
            vec!["node-1".into(), "node-2".into(), "node-3".into()],
        )
        .unwrap()
        .to_stored_command();
        let entry = LogEntry {
            cluster_id: "cluster-a".into(),
            epoch: 1,
            config_id: 1,
            index: 1,
            entry_type: change.entry_type,
            hash: LogEntry::calculate_hash(
                "cluster-a",
                1,
                1,
                1,
                change.entry_type,
                LogHash::ZERO,
                &change.payload,
            ),
            payload: change.payload,
            prev_hash: LogHash::ZERO,
        };
        arm_qwal_apply_fault(&path, QwalApplyFault::BeforePendingBegin);

        assert!(matches!(
            database.apply_entry(&entry),
            Err(Error::Sqlite(message)) if message.contains("before pending control publication")
        ));
        assert!(!database.read_state.pending_fence.load(Ordering::Acquire));
        assert_eq!(database.control.pending().unwrap(), None);
        assert_eq!(database.get_value("readable").unwrap(), None);

        database.apply_entry(&entry).unwrap();
        assert_eq!(database.applied_index_value().unwrap(), 1);
        assert!(!database.read_state.pending_fence.load(Ordering::Acquire));
        assert_eq!(database.control.pending().unwrap(), None);
    }

    #[cfg(unix)]
    #[test]
    fn prepared_base_new_inode_is_rejected_by_the_page_state_seal() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "new-base-inode".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE rebuilt_from_new_inode(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let original_inode = fs::metadata(&path).unwrap().ino();
        {
            let _lifecycle = database.lock_lifecycle().unwrap();
            database.close_connection().unwrap();
            let replacement = path.with_extension("replacement");
            fs::copy(&path, &replacement).unwrap();
            fs::rename(&replacement, &path).unwrap();
        }
        assert_ne!(fs::metadata(&path).unwrap().ino(), original_inode);

        assert!(database
            .apply_entry(&command_entry(1, LogHash::ZERO, payload))
            .is_err());
        assert_eq!(prepared_install_path(&path), None);
    }

    #[cfg(unix)]
    #[test]
    fn prepared_base_symlink_falls_back_without_promoting_through_it() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "symlinked-base".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE rebuilt_from_symlink(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let backing = path.with_extension("backing");
        {
            let _lifecycle = database.lock_lifecycle().unwrap();
            database.close_connection().unwrap();
            fs::rename(&path, &backing).unwrap();
            symlink(&backing, &path).unwrap();
        }

        assert!(database
            .apply_entry(&command_entry(1, LogHash::ZERO, payload))
            .is_err());
        assert_eq!(prepared_install_path(&path), None);
        assert!(fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[cfg(unix)]
    #[test]
    fn prepared_base_same_inode_mutation_rejects_rebuildable_apply() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "mutated-base".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE should_not_promote(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let entry = command_entry(1, LogHash::ZERO, payload);
        let original_inode = fs::metadata(&path).unwrap().ino();
        {
            let _lifecycle = database.lock_lifecycle().unwrap();
            database.close_connection().unwrap();
            let mutation = open_connection(&path).unwrap();
            mutation
                .execute_batch(
                    "CREATE TABLE external_mutation(value INTEGER NOT NULL); PRAGMA wal_checkpoint(TRUNCATE);",
                )
                .unwrap();
            mutation.close().unwrap();
        }
        assert_eq!(fs::metadata(&path).unwrap().ino(), original_inode);

        assert!(database.apply_entry(&entry).is_err());

        assert_eq!(database.control.pending().unwrap(), None);
    }

    #[test]
    fn stale_prepared_entry_cannot_promote_after_the_tip_moves() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "stale-prepared".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE stale_target(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let stale = command_entry(1, LogHash::ZERO, payload);
        let noop_hash =
            LogEntry::calculate_hash("cluster-a", 1, 1, 1, EntryType::Noop, LogHash::ZERO, &[]);
        database
            .apply_entry(&LogEntry {
                cluster_id: "cluster-a".into(),
                epoch: 1,
                config_id: 1,
                index: 1,
                entry_type: EntryType::Noop,
                payload: vec![],
                prev_hash: LogHash::ZERO,
                hash: noop_hash,
            })
            .unwrap();

        assert!(database.apply_entry(&stale).is_err());
    }

    #[test]
    fn reopening_preserves_decided_effect_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let command = SqlCommand {
            request_id: "restart-fallback".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE restarted(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        drop(database);
        let database = SqliteStateMachine::open_existing(&path).unwrap();
        let entry = command_entry(1, LogHash::ZERO, payload);

        let outcome = database.apply_entry_with_result(&entry).unwrap();

        assert_eq!(
            prepared_install_path(&path),
            Some(PreparedInstallPath::Patched)
        );
        assert_eq!(
            database
                .check_sql_request("restart-fallback", &request)
                .unwrap()
                .unwrap(),
            (
                RequestOutcome::new(1, entry.hash),
                outcome.sql_result().cloned()
            )
        );
    }

    #[test]
    fn detached_owner_closes_canonical_database_for_strict_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let command = SqlCommand {
            request_id: "handoff".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE handed_off(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let entry = command_entry(1, LogHash::ZERO, payload);
        database.apply_entry(&entry).unwrap();

        database.close_for_handoff().unwrap();

        assert!(sqlite_sidecars_absent(&path).unwrap());
        let reopened = SqliteStateMachine::open_existing(&path).unwrap();
        assert_eq!(
            reopened.applied_tip_value().unwrap(),
            (entry.index, entry.hash)
        );
    }

    #[test]
    fn read_handle_is_retired_before_handoff_to_a_new_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let old = database.read_handle();
        let query = SqlStatement {
            sql: "SELECT 1".into(),
            parameters: vec![],
        };

        database.close_for_handoff().unwrap();
        let reopened = SqliteStateMachine::open_existing(&path).unwrap();
        reopened.query_sql(&query, 1, 1024).unwrap();

        assert!(matches!(
            old.query_sql(&query, 1, 1024),
            Err(Error::Sqlite(message)) if message.contains("retired")
        ));
    }

    #[test]
    fn failed_consuming_handoff_still_retires_read_handles() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let old = database.read_handle();
        let query = SqlStatement {
            sql: "SELECT 1".into(),
            parameters: vec![],
        };
        arm_close_for_handoff_fault(&path);

        assert!(database.close_for_handoff().is_err());
        assert!(matches!(
            old.query_sql(&query, 1, 1024),
            Err(Error::Sqlite(message)) if message.contains("retired")
        ));
    }

    #[test]
    fn read_admission_is_bounded_and_waiting_handoff_excludes_new_readers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let handle = database.read_handle();
        let query = SqlStatement {
            sql: "SELECT 1".into(),
            parameters: vec![],
        };
        let (entered, release) = arm_sql_read_test_gate(&path);
        let mut readers = Vec::new();
        for _ in 0..MAX_CONCURRENT_SQL_READS {
            let reader = handle.clone();
            let query = query.clone();
            readers.push(std::thread::spawn(move || {
                reader.query_sql(&query, 1, 1024)
            }));
        }
        for _ in 0..MAX_CONCURRENT_SQL_READS {
            entered.recv().unwrap();
        }
        assert_eq!(
            handle.state.opened_read_connections.load(Ordering::Relaxed),
            MAX_CONCURRENT_SQL_READS
        );
        assert!(matches!(
            handle.query_sql(&query, 1, 1024),
            Err(Error::ResourceExhausted(message)) if message.contains("limit")
        ));

        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        *database.read_state.writer_waiting_hook.lock().unwrap() = Some(waiting_tx);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let result = database.close_for_handoff();
            done_tx.send(result).unwrap();
        });
        waiting_rx.recv().unwrap();
        assert!(matches!(
            done_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        assert!(matches!(
            handle.query_sql(&query, 1, 1024),
            Err(Error::ResourceExhausted(message)) if message.contains("exclusive")
        ));

        for _ in 0..MAX_CONCURRENT_SQL_READS {
            release.send(()).unwrap();
        }
        for reader in readers {
            reader.join().unwrap().unwrap();
        }
        done_rx.recv().unwrap().unwrap();
        writer.join().unwrap();
        disarm_sql_read_test_gate(&path);
        assert!(matches!(
            handle.query_sql(&query, 1, 1024),
            Err(Error::Sqlite(message)) if message.contains("retired")
        ));
    }

    #[test]
    fn sequential_queries_reuse_one_pooled_connection_after_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let handle = database.read_handle();
        let invalid = SqlStatement {
            sql: "SELECT FROM".into(),
            parameters: vec![],
        };
        let valid = SqlStatement {
            sql: "SELECT 1".into(),
            parameters: vec![],
        };

        assert!(handle.query_sql(&invalid, 1, 1024).is_err());
        assert_eq!(
            handle.state.opened_read_connections.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            handle.query_sql(&valid, 1, 1024).unwrap().rows,
            vec![vec![SqlValue::Integer(1)]]
        );
        assert_eq!(
            handle.state.opened_read_connections.load(Ordering::Relaxed),
            1
        );
        let pool = handle.state.pool.lock().unwrap();
        assert_eq!((pool.open, pool.idle.len()), (1, 1));
    }

    #[test]
    fn concurrent_queries_bind_one_generation_and_block_apply() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "concurrent-read-apply".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE applied_generation(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let entry = command_entry(1, LogHash::ZERO, payload);
        let entry_hash = entry.hash;
        let query = SqlStatement {
            sql: "SELECT COUNT(*) FROM sqlite_master WHERE name = 'applied_generation'".into(),
            parameters: vec![],
        };
        let handle = database.read_handle();
        let (entered, release) = arm_sql_read_test_gate(&path);
        let mut readers = Vec::new();
        for _ in 0..2 {
            let reader = handle.clone();
            let query = query.clone();
            readers.push(std::thread::spawn(move || {
                reader.query_sql_with_applied_tip(&query, 1, 1024)
            }));
        }
        entered.recv().unwrap();
        entered.recv().unwrap();
        assert_eq!(
            handle.state.opened_read_connections.load(Ordering::Relaxed),
            2
        );

        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        *database.read_state.writer_waiting_hook.lock().unwrap() = Some(waiting_tx);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            done_tx.send(database.apply_entry(&entry)).unwrap();
        });
        waiting_rx.recv().unwrap();
        assert!(matches!(
            done_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));

        release.send(()).unwrap();
        release.send(()).unwrap();
        for reader in readers {
            let (index, hash, result) = reader.join().unwrap().unwrap();
            assert_eq!((index, hash), (0, LogHash::ZERO));
            assert_eq!(result.rows, vec![vec![SqlValue::Integer(0)]]);
        }
        done_rx.recv().unwrap().unwrap();
        writer.join().unwrap();
        disarm_sql_read_test_gate(&path);
        {
            let pool = handle.state.pool.lock().unwrap();
            assert_eq!((pool.open, pool.idle.len()), (0, 0));
        }

        let (index, hash, result) = handle.query_sql_with_applied_tip(&query, 1, 1024).unwrap();
        assert_eq!((index, hash), (1, entry_hash));
        assert_eq!(result.rows, vec![vec![SqlValue::Integer(1)]]);
        assert_eq!(
            handle.state.opened_read_connections.load(Ordering::Relaxed),
            3
        );
    }

    #[test]
    fn fenced_reopen_removes_only_empty_wal_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        drop(database);
        let wal = sqlite_sidecar_path(&path, "-wal");
        let shm = sqlite_sidecar_path(&path, "-shm");
        File::create(&wal).unwrap();
        File::create(&shm)
            .unwrap()
            .write_all(b"empty-wal-shm")
            .unwrap();

        cleanup_empty_wal_sidecars_after_owner_fence(&path).unwrap();

        assert!(sqlite_sidecars_absent(&path).unwrap());
        SqliteStateMachine::open_existing(&path).unwrap();
    }

    #[test]
    fn replay_after_legacy_pending_with_canonical_base_patches_target_and_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let command = SqlCommand {
            request_id: "pending-base-replay".into(),
            statements: vec![
                SqlStatement {
                    sql: "CREATE TABLE base_recovery(value TEXT NOT NULL)".into(),
                    parameters: vec![],
                },
                SqlStatement {
                    sql: "INSERT INTO base_recovery VALUES ('recovered') RETURNING value".into(),
                    parameters: vec![],
                },
            ],
        };
        let request = encode_sql_command(&command).unwrap();
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let effect = decode_qwal_v3(&payload).unwrap();
        let expected_result = decode_sql_result(&effect.receipts[0].result_blob).unwrap();
        let entry = command_entry(1, LogHash::ZERO, payload);
        let different_command = SqlCommand {
            request_id: "different-pending-base".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE wrong_pending_winner(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let different_request = encode_sql_command(&different_command).unwrap();
        let different_payload = prepare_single_sql_effect(
            &database,
            &different_command,
            &different_request,
            0,
            LogHash::ZERO,
        )
        .unwrap();
        let different_entry = command_entry(1, LogHash::ZERO, different_payload);
        let pending = pending_for(&entry, &effect);
        database.control.begin_pending(&pending).unwrap();
        let canonical_before_replay = fs::read(&path).unwrap();
        drop(database);

        let database = SqliteStateMachine::open_existing(&path).unwrap();
        assert!(database.read_state.pending_fence.load(Ordering::Acquire));
        assert!(matches!(
            database.get_value("blocked"),
            Err(Error::InvalidEntry(message)) if message.contains("pending")
        ));
        assert!(database
            .query_sql(
                &SqlStatement {
                    sql: "SELECT 1".into(),
                    parameters: vec![],
                },
                1,
                64,
            )
            .is_err());
        assert!(database
            .check_request("pending-base-replay", &request)
            .is_err());
        assert!(database.canonical_db_digest().is_err());
        assert!(database.create_snapshot(0).is_err());
        assert!(database
            .prepare_sql_batch_effect(
                &[SqlBatchMember {
                    command: &command,
                    request_payload: &request,
                }],
                0,
                LogHash::ZERO,
            )
            .is_err());
        assert!(matches!(
            database.apply_entry_with_result(&different_entry),
            Err(Error::InvalidEntry(_))
        ));
        assert_eq!(fs::read(&path).unwrap(), canonical_before_replay);
        assert_eq!(
            rebuild_page_state(&path).unwrap().identity(),
            effect.base_state
        );
        assert!(database.read_state.pending_fence.load(Ordering::Acquire));
        assert!(database.get_value("still-blocked").is_err());
        let outcome = database.apply_entry_with_result(&entry).unwrap();

        assert!(!database.read_state.pending_fence.load(Ordering::Acquire));
        assert_eq!(outcome.sql_result(), Some(&expected_result));
        assert_eq!(
            rebuild_page_state(&path).unwrap().identity(),
            effect.target_state
        );
        assert_eq!(
            prepared_install_path(&path),
            Some(PreparedInstallPath::Patched)
        );
        assert_eq!(
            database
                .check_sql_request("pending-base-replay", &request)
                .unwrap()
                .unwrap(),
            (RequestOutcome::new(1, entry.hash), Some(expected_result))
        );
    }

    #[test]
    fn open_rejects_pending_that_no_longer_extends_the_committed_tip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let command = SqlCommand {
            request_id: "corrupt-pending".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE corrupt_pending(id INTEGER PRIMARY KEY)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let effect = decode_qwal_v3(&payload).unwrap();
        let entry = command_entry(1, LogHash::ZERO, payload);
        database
            .control
            .begin_pending(&pending_for(&entry, &effect))
            .unwrap();
        drop(database);

        Connection::open(control_sidecar_path(&path))
            .unwrap()
            .execute(
                "UPDATE pending_apply SET base_index = 1 WHERE singleton = 1",
                [],
            )
            .unwrap();

        assert!(matches!(
            SqliteStateMachine::open_existing(&path),
            Err(Error::InvalidEntry(_))
        ));
    }

    fn pending_for(entry: &LogEntry, effect: &QwalEnvelopeV3) -> PendingApply {
        PendingApply::new(
            LogAnchor::new(effect.base_index, effect.base_hash),
            LogAnchor::new(entry.index, entry.hash),
            effect.base_state,
            effect.target_state,
        )
    }

    fn command_entry(index: u64, prev_hash: LogHash, payload: Vec<u8>) -> LogEntry {
        let hash = LogEntry::calculate_hash(
            "cluster-a",
            index,
            1,
            1,
            EntryType::Command,
            prev_hash,
            &payload,
        );
        LogEntry {
            cluster_id: "cluster-a".into(),
            epoch: 1,
            config_id: 1,
            index,
            entry_type: EntryType::Command,
            payload,
            prev_hash,
            hash,
        }
    }

    #[test]
    fn result_decoder_rejects_semantically_oversized_returning_rows() {
        let result = SqlCommandResult {
            statement_results: vec![SqlStatementResult {
                rows_affected: 0,
                returning: Some(SqlQueryResult {
                    columns: vec!["value".into()],
                    rows: (0..=MAX_RETURNING_ROWS)
                        .map(|_| vec![SqlValue::Integer(1)])
                        .collect(),
                }),
            }],
        };
        let mut blob = SQL_RESULT_V1_MAGIC.to_vec();
        blob.extend_from_slice(&serde_json::to_vec(&result).unwrap());

        assert!(matches!(
            decode_sql_result(&blob),
            Err(Error::InvalidCommand(_))
        ));
    }
}
