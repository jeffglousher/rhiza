//! Durable control-plane state for physical SQLite effects.
//!
//! This database is deliberately separate from the canonical user database so
//! node-local identity and qlog progress cannot change replicated user pages.

#[cfg(test)]
use std::sync::{Mutex, OnceLock};
use std::{
    collections::HashSet,
    fmt::Write as _,
    fs::OpenOptions,
    path::{Path, PathBuf},
};

use rhiza_core::{ConfigurationState, LogAnchor, LogEntry, LogHash};
use rusqlite::{
    params, params_from_iter, types::Value, Connection, OpenFlags, OptionalExtension, Transaction,
    TransactionBehavior,
};
use serde::{Deserialize, Serialize};

use super::{ApplyProgress, Error, RequestConflict, RequestOutcome, Result};
use crate::page_state::StateIdentityV3;

const CONTROL_MAGIC: &[u8] = b"RHIZA-SQL-CONTROL\0\x06";
const CONTROL_SCHEMA_VERSION: u64 = 6;
const SNAPSHOT_MAGIC: &[u8] = b"QCTL\0\x06";
const MAX_RESULT_BLOB_BYTES: usize = super::MAX_SQL_EFFECT_BYTES;
const SQLITE_VARIABLE_LIMIT: usize = 999;
const RECEIPT_LOOKUP_CHUNK_SIZE: usize = SQLITE_VARIABLE_LIMIT;
const RECEIPT_INSERT_CHUNK_SIZE: usize = (SQLITE_VARIABLE_LIMIT - 2) / 3;

const CREATE_CONTROL_META_SQL: &str = r#"CREATE TABLE control_meta (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL
) WITHOUT ROWID;"#;
const CREATE_REQUEST_RECEIPTS_SQL: &str = r#"CREATE TABLE request_receipts (
    request_id TEXT PRIMARY KEY,
    request_digest BLOB NOT NULL CHECK(length(request_digest) = 32),
    original_log_index INTEGER NOT NULL CHECK(original_log_index >= 0),
    original_log_hash BLOB NOT NULL CHECK(length(original_log_hash) = 32),
    result_blob BLOB NOT NULL
) WITHOUT ROWID;"#;
const CREATE_PENDING_APPLY_SQL: &str = r#"CREATE TABLE pending_apply (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    base_index INTEGER NOT NULL CHECK(base_index >= 0),
    base_hash BLOB NOT NULL CHECK(length(base_hash) = 32),
    entry_index INTEGER NOT NULL CHECK(entry_index > 0),
    entry_hash BLOB NOT NULL CHECK(length(entry_hash) = 32),
    base_page_size INTEGER NOT NULL CHECK(base_page_size >= 512 AND base_page_size <= 65536),
    base_page_count INTEGER NOT NULL CHECK(base_page_count > 0),
    base_state_root BLOB NOT NULL CHECK(length(base_state_root) = 32),
    target_page_size INTEGER NOT NULL CHECK(target_page_size >= 512 AND target_page_size <= 65536),
    target_page_count INTEGER NOT NULL CHECK(target_page_count > 0),
    target_state_root BLOB NOT NULL CHECK(length(target_state_root) = 32)
);"#;
const CREATE_EMBEDDED_LOG_SQL: &str = r#"CREATE TABLE embedded_qlog (
    log_index INTEGER PRIMARY KEY CHECK(log_index > 0),
    initial_configuration BLOB NOT NULL,
    entry_bytes BLOB NOT NULL
) WITHOUT ROWID;"#;

const REQUIRED_META_KEYS: [&str; 12] = [
    "magic",
    "schema_version",
    "cluster_id",
    "node_id",
    "epoch",
    "configuration_state",
    "recovery_generation",
    "materializer_fingerprint",
    "page_size",
    "page_count",
    "state_root",
    "applied_tip",
];

type ExpectedColumn = (&'static str, &'static str, bool, i64);

const CONTROL_META_COLUMNS: &[ExpectedColumn] =
    &[("key", "TEXT", true, 1), ("value", "BLOB", true, 0)];
const REQUEST_RECEIPT_COLUMNS: &[ExpectedColumn] = &[
    ("request_id", "TEXT", true, 1),
    ("request_digest", "BLOB", true, 0),
    ("original_log_index", "INTEGER", true, 0),
    ("original_log_hash", "BLOB", true, 0),
    ("result_blob", "BLOB", true, 0),
];
const PENDING_APPLY_COLUMNS: &[ExpectedColumn] = &[
    ("singleton", "INTEGER", false, 1),
    ("base_index", "INTEGER", true, 0),
    ("base_hash", "BLOB", true, 0),
    ("entry_index", "INTEGER", true, 0),
    ("entry_hash", "BLOB", true, 0),
    ("base_page_size", "INTEGER", true, 0),
    ("base_page_count", "INTEGER", true, 0),
    ("base_state_root", "BLOB", true, 0),
    ("target_page_size", "INTEGER", true, 0),
    ("target_page_count", "INTEGER", true, 0),
    ("target_state_root", "BLOB", true, 0),
];
const EMBEDDED_LOG_COLUMNS: &[ExpectedColumn] = &[
    ("log_index", "INTEGER", true, 1),
    ("initial_configuration", "BLOB", true, 0),
    ("entry_bytes", "BLOB", true, 0),
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlIdentity {
    cluster_id: String,
    node_id: String,
    epoch: u64,
    configuration_state: ConfigurationState,
    recovery_generation: u64,
    materializer_fingerprint: LogHash,
    user_state: StateIdentityV3,
}

impl ControlIdentity {
    pub fn new(
        cluster_id: impl Into<String>,
        node_id: impl Into<String>,
        epoch: u64,
        configuration_state: ConfigurationState,
        recovery_generation: u64,
        materializer_fingerprint: LogHash,
        user_state: StateIdentityV3,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            node_id: node_id.into(),
            epoch,
            configuration_state,
            recovery_generation,
            materializer_fingerprint,
            user_state,
        }
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }
    pub const fn configuration_state(&self) -> &ConfigurationState {
        &self.configuration_state
    }
    pub const fn recovery_generation(&self) -> u64 {
        self.recovery_generation
    }
    pub const fn materializer_fingerprint(&self) -> LogHash {
        self.materializer_fingerprint
    }
    pub const fn user_state(&self) -> StateIdentityV3 {
        self.user_state
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RequestReceipt {
    request_id: String,
    request_digest: LogHash,
    original_anchor: LogAnchor,
    result_blob: Vec<u8>,
}

impl RequestReceipt {
    pub fn new(
        request_id: impl Into<String>,
        request_digest: LogHash,
        original_anchor: LogAnchor,
        result_blob: Vec<u8>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            request_digest,
            original_anchor,
            result_blob,
        }
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub const fn request_digest(&self) -> LogHash {
        self.request_digest
    }
    pub const fn original_anchor(&self) -> LogAnchor {
        self.original_anchor
    }
    pub fn result_blob(&self) -> &[u8] {
        &self.result_blob
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PendingApply {
    base: LogAnchor,
    entry: LogAnchor,
    base_state: StateIdentityV3,
    target_state: StateIdentityV3,
}

impl PendingApply {
    pub const fn new(
        base: LogAnchor,
        entry: LogAnchor,
        base_state: StateIdentityV3,
        target_state: StateIdentityV3,
    ) -> Self {
        Self {
            base,
            entry,
            base_state,
            target_state,
        }
    }

    pub const fn base(&self) -> LogAnchor {
        self.base
    }
    pub const fn entry(&self) -> LogAnchor {
        self.entry
    }
    pub const fn base_state(&self) -> StateIdentityV3 {
        self.base_state
    }
    pub const fn target_state(&self) -> StateIdentityV3 {
        self.target_state
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplicatedSnapshot {
    cluster_id: String,
    epoch: u64,
    configuration_state: ConfigurationState,
    recovery_generation: u64,
    materializer_fingerprint: LogHash,
    user_state: StateIdentityV3,
    applied_tip: LogAnchor,
    receipts: Vec<RequestReceipt>,
}

pub struct ControlStore {
    path: PathBuf,
    conn: Connection,
}

#[cfg(test)]
fn pending_query_audits() -> &'static Mutex<Vec<(PathBuf, usize)>> {
    static AUDITS: OnceLock<Mutex<Vec<(PathBuf, usize)>>> = OnceLock::new();
    AUDITS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
pub(crate) fn begin_pending_query_audit(path: &Path) {
    pending_query_audits()
        .lock()
        .unwrap()
        .push((path.to_path_buf(), 0));
}

#[cfg(test)]
pub(crate) fn pending_query_count(path: &Path) -> Option<usize> {
    pending_query_audits().lock().ok().and_then(|audits| {
        audits
            .iter()
            .rev()
            .find(|(audited, _)| audited == path)
            .map(|(_, count)| *count)
    })
}

#[cfg(test)]
fn note_pending_query(path: &Path) {
    if let Ok(mut audits) = pending_query_audits().lock() {
        if let Some((_, count)) = audits.iter_mut().rev().find(|(audited, _)| audited == path) {
            *count += 1;
        }
    }
}

impl ControlStore {
    /// Opens and validates an existing sidecar, or creates it if absent.
    pub fn open(path: impl AsRef<Path>, identity: &ControlIdentity) -> Result<Self> {
        let path = path.as_ref();
        if path.exists() {
            Self::open_existing(path, identity)
        } else {
            Self::create(path, identity)
        }
    }

    pub fn create(path: impl AsRef<Path>, identity: &ControlIdentity) -> Result<Self> {
        validate_new_identity(identity)?;
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|error| Error::Io(error.to_string()))?;
        }
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| Error::Io(error.to_string()))?;

        let store = match Self::open_file(path) {
            Ok(store) => store,
            Err(error) => {
                let _ = std::fs::remove_file(path);
                return Err(error);
            }
        };
        if let Err(error) = store.initialize(identity) {
            drop(store);
            let _ = std::fs::remove_file(path);
            return Err(error);
        }
        sync_parent(path)?;
        Ok(store)
    }

    pub fn open_existing(path: impl AsRef<Path>, identity: &ControlIdentity) -> Result<Self> {
        validate_new_identity(identity)?;
        let store = Self::open_existing_unchecked(path)?;
        store.validate_identity(identity)?;
        Ok(store)
    }

    /// Opens a sidecar and validates its durable format without imposing an
    /// expected runtime identity. This is used by recovery paths that must load
    /// the persisted identity before they can validate the paired user DB.
    pub fn open_existing_unchecked(path: impl AsRef<Path>) -> Result<Self> {
        let store = Self::open_file(path.as_ref())?;
        store.validate_schema()?;
        Ok(store)
    }

    pub fn read_identity(path: impl AsRef<Path>) -> Result<ControlIdentity> {
        Self::open_existing_unchecked(path)?.identity()
    }

    fn open_file(path: &Path) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(sqlite_error)?;
        let journal: String = conn
            .query_row("PRAGMA journal_mode = DELETE", [], |row| row.get(0))
            .map_err(sqlite_error)?;
        if !journal.eq_ignore_ascii_case("delete") {
            return Err(Error::Sqlite(format!(
                "control sidecar refused DELETE journal mode: {journal}"
            )));
        }
        conn.pragma_update(None, "synchronous", "OFF")
            .map_err(sqlite_error)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(sqlite_error)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(sqlite_error)?;
        Ok(Self {
            path: path.to_path_buf(),
            conn,
        })
    }

    fn initialize(&self, identity: &ControlIdentity) -> Result<()> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        tx.execute_batch(CREATE_CONTROL_META_SQL)
            .map_err(sqlite_error)?;
        tx.execute_batch(CREATE_REQUEST_RECEIPTS_SQL)
            .map_err(sqlite_error)?;
        tx.execute_batch(CREATE_PENDING_APPLY_SQL)
            .map_err(sqlite_error)?;
        tx.execute_batch(CREATE_EMBEDDED_LOG_SQL)
            .map_err(sqlite_error)?;
        put_meta(&tx, "magic", CONTROL_MAGIC)?;
        put_u64(&tx, "schema_version", CONTROL_SCHEMA_VERSION)?;
        put_meta(&tx, "cluster_id", identity.cluster_id.as_bytes())?;
        put_meta(&tx, "node_id", identity.node_id.as_bytes())?;
        put_u64(&tx, "epoch", identity.epoch)?;
        put_configuration(&tx, &identity.configuration_state)?;
        put_u64(&tx, "recovery_generation", identity.recovery_generation)?;
        put_hash(
            &tx,
            "materializer_fingerprint",
            identity.materializer_fingerprint,
        )?;
        put_state(&tx, identity.user_state)?;
        put_anchor(&tx, "applied_tip", LogAnchor::new(0, LogHash::ZERO))?;
        tx.commit().map_err(sqlite_error)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn identity(&self) -> Result<ControlIdentity> {
        Ok(ControlIdentity::new(
            meta_text(&self.conn, "cluster_id")?,
            meta_text(&self.conn, "node_id")?,
            meta_u64(&self.conn, "epoch")?,
            meta_configuration(&self.conn)?,
            meta_u64(&self.conn, "recovery_generation")?,
            meta_hash(&self.conn, "materializer_fingerprint")?,
            meta_state(&self.conn)?,
        ))
    }

    pub fn applied_tip(&self) -> Result<ApplyProgress> {
        let anchor = meta_anchor(&self.conn, "applied_tip")?;
        Ok(ApplyProgress::new(anchor.index(), anchor.hash()))
    }

    pub fn configuration_state(&self) -> Result<ConfigurationState> {
        meta_configuration(&self.conn)
    }

    pub fn recovery_generation(&self) -> Result<u64> {
        meta_u64(&self.conn, "recovery_generation")
    }

    pub fn materializer_fingerprint(&self) -> Result<LogHash> {
        meta_hash(&self.conn, "materializer_fingerprint")
    }

    pub fn user_state(&self) -> Result<StateIdentityV3> {
        meta_state(&self.conn)
    }

    pub fn lookup_request(
        &self,
        request_id: &str,
        request_digest: LogHash,
    ) -> Result<Option<RequestReceipt>> {
        self.lookup_requests(&[(request_id, request_digest)])?
            .pop()
            .expect("one request produces one aligned lookup")
    }

    /// Returns receipts in the exact order of the requested `(id, digest)`
    /// pairs using one bounded control-sidecar query.
    ///
    /// Request ids must be unique. Missing ids produce `Ok(None)`; an existing
    /// id with another digest produces an aligned request-conflict error so a
    /// caller can isolate that member without issuing another query.
    pub fn lookup_requests(
        &self,
        requests: &[(&str, LogHash)],
    ) -> Result<Vec<Result<Option<RequestReceipt>>>> {
        lookup_requests_from(&self.conn, requests)
    }

    #[cfg(test)]
    pub(crate) fn begin_pending(&self, pending: &PendingApply) -> Result<()> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        if let Some(existing) = pending_from(&tx)? {
            return if existing == *pending {
                tx.commit().map_err(sqlite_error)
            } else {
                Err(Error::InvalidEntry(
                    "a different physical apply is already pending".into(),
                ))
            };
        }
        let tip = meta_anchor(&tx, "applied_tip")?;
        let user_state = meta_state(&tx)?;
        if pending.base != tip || pending.base_state != user_state {
            return Err(Error::InvalidEntry(
                "pending apply does not match the committed base".into(),
            ));
        }
        if pending.entry.index()
            != tip
                .index()
                .checked_add(1)
                .ok_or_else(|| Error::InvalidEntry("applied index is exhausted".into()))?
        {
            return Err(Error::InvalidEntry(
                "pending apply entry is not the next slot".into(),
            ));
        }
        insert_pending(&tx, pending)?;
        tx.commit().map_err(sqlite_error)
    }

    /// Atomically makes the physical-apply intent and its complete local qlog entry durable.
    pub(crate) fn begin_pending_with_entry(
        &self,
        pending: &PendingApply,
        entry: &LogEntry,
    ) -> Result<()> {
        if LogAnchor::new(entry.index, entry.hash) != pending.entry
            || entry.recompute_hash() != entry.hash
        {
            return Err(Error::InvalidEntry(
                "embedded qlog entry does not match the pending apply".into(),
            ));
        }
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        insert_or_validate_embedded_entry(&tx, entry)?;
        if let Some(existing) = pending_from(&tx)? {
            return if existing == *pending {
                tx.commit().map_err(sqlite_error)
            } else {
                Err(Error::InvalidEntry(
                    "a different physical apply is already pending".into(),
                ))
            };
        }
        let tip = meta_anchor(&tx, "applied_tip")?;
        let user_state = meta_state(&tx)?;
        if pending.base != tip || pending.base_state != user_state {
            return Err(Error::InvalidEntry(
                "pending apply does not match the committed base".into(),
            ));
        }
        if pending.entry.index()
            != tip
                .index()
                .checked_add(1)
                .ok_or_else(|| Error::InvalidEntry("applied index is exhausted".into()))?
        {
            return Err(Error::InvalidEntry(
                "pending apply entry is not the next slot".into(),
            ));
        }
        insert_pending(&tx, pending)?;
        tx.commit().map_err(sqlite_error)
    }

    /// Returns a contiguous interval from the qlog embedded in the control sidecar.
    pub fn embedded_log_entries(
        &self,
        from_index: u64,
        through_index: u64,
    ) -> Result<Vec<LogEntry>> {
        if from_index > through_index {
            return Ok(Vec::new());
        }
        let mut statement = self
            .conn
            .prepare(
                "SELECT log_index,initial_configuration,entry_bytes FROM embedded_qlog
                 WHERE log_index >= ?1 AND log_index <= ?2 ORDER BY log_index",
            )
            .map_err(sqlite_error)?;
        let rows = statement
            .query_map(
                params![u64_to_sql(from_index)?, u64_to_sql(through_index)?],
                |row| {
                    Ok((
                        u64_from_sql(row.get(0)?)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .map_err(sqlite_error)?;
        let cluster_id = meta_text(&self.conn, "cluster_id")?;
        let mut expected = from_index;
        let mut expected_configuration = None;
        let mut entries = Vec::new();
        for row in rows {
            let (index, initial_configuration, encoded) = row.map_err(sqlite_error)?;
            if index != expected {
                return Err(Error::InvalidEntry(format!(
                    "embedded qlog is missing index {expected}"
                )));
            }
            let initial_configuration =
                decode_configuration_bytes(&initial_configuration, "embedded qlog")?;
            if expected_configuration
                .as_ref()
                .is_some_and(|expected| expected != &initial_configuration)
            {
                return Err(Error::InvalidEntry(
                    "embedded qlog configuration chain is discontinuous".into(),
                ));
            }
            let (entry, resulting_configuration) =
                decode_embedded_log_entry(&encoded, &cluster_id, initial_configuration)?;
            if entry.index != index {
                return Err(Error::InvalidEntry(
                    "embedded qlog key does not match its entry index".into(),
                ));
            }
            entries.push(entry);
            expected_configuration = Some(resulting_configuration);
            expected = expected
                .checked_add(1)
                .ok_or_else(|| Error::InvalidEntry("embedded qlog index overflow".into()))?;
        }
        if expected <= through_index {
            return Err(Error::InvalidEntry(format!(
                "embedded qlog is missing index {expected}"
            )));
        }
        Ok(entries)
    }

    /// Removes embedded qlog entries before a verified checkpoint anchor.
    pub(crate) fn compact_embedded_log_before(&self, anchor_index: u64) -> Result<()> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let applied_index = meta_anchor(&tx, "applied_tip")?.index();
        if anchor_index > applied_index {
            return Err(Error::InvalidEntry(format!(
                "cannot compact embedded qlog before anchor {anchor_index} beyond applied index {applied_index}"
            )));
        }
        tx.execute(
            "DELETE FROM embedded_qlog WHERE log_index < ?1",
            [u64_to_sql(anchor_index)?],
        )
        .map_err(sqlite_error)?;
        tx.commit().map_err(sqlite_error)
    }

    pub fn pending(&self) -> Result<Option<PendingApply>> {
        #[cfg(test)]
        note_pending_query(&self.path);
        pending_from(&self.conn)
    }

    #[cfg(test)]
    pub(crate) fn clear_pending(&self, expected: &PendingApply) -> Result<()> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        match pending_from(&tx)? {
            Some(actual) if actual == *expected => {
                tx.execute("DELETE FROM pending_apply WHERE singleton = 1", [])
                    .map_err(sqlite_error)?;
                tx.commit().map_err(sqlite_error)
            }
            Some(_) => Err(Error::InvalidEntry(
                "refusing to clear a different pending apply".into(),
            )),
            None => tx.commit().map_err(sqlite_error),
        }
    }

    /// Durably advances a log entry that changes no replicated user data and
    /// does not transition the configuration. The exact committed base is
    /// checked again inside the same FULL-synchronous transaction as the tip
    /// update, so this path cannot bypass an older physical apply intent.
    #[cfg(test)]
    pub(crate) fn commit_metadata_only_entry(
        &self,
        expected_base: LogAnchor,
        entry: LogAnchor,
        expected_configuration: &ConfigurationState,
        expected_user_state: StateIdentityV3,
    ) -> Result<()> {
        let expected_entry_index = expected_base
            .index()
            .checked_add(1)
            .ok_or_else(|| Error::InvalidEntry("applied index is exhausted".into()))?;
        if entry.index() != expected_entry_index {
            return Err(Error::InvalidEntry(
                "metadata-only entry is not the next slot".into(),
            ));
        }

        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        if pending_from(&tx)?.is_some() {
            return Err(Error::InvalidEntry(
                "metadata-only commit cannot bypass a pending physical apply".into(),
            ));
        }
        if meta_anchor(&tx, "applied_tip")? != expected_base {
            return Err(Error::InvalidEntry(
                "metadata-only commit does not match the committed base".into(),
            ));
        }
        if meta_configuration(&tx)? != *expected_configuration {
            return Err(Error::InvalidEntry(
                "metadata-only commit configuration changed".into(),
            ));
        }
        if meta_state(&tx)? != expected_user_state {
            return Err(Error::InvalidEntry(
                "metadata-only commit user state changed".into(),
            ));
        }

        let changed = tx
            .execute(
                "UPDATE control_meta SET value = ?1 WHERE key = 'applied_tip' AND value = ?2",
                params![
                    anchor_bytes(entry).as_slice(),
                    anchor_bytes(expected_base).as_slice()
                ],
            )
            .map_err(sqlite_error)?;
        if changed != 1 {
            return Err(Error::InvalidEntry(
                "metadata-only tip compare-and-swap affected an unexpected row count".into(),
            ));
        }
        tx.commit().map_err(sqlite_error)
    }

    pub(crate) fn commit_metadata_only_entry_with_log(
        &self,
        expected_base: LogAnchor,
        entry: &LogEntry,
        expected_configuration: &ConfigurationState,
        expected_user_state: StateIdentityV3,
    ) -> Result<()> {
        let entry_anchor = LogAnchor::new(entry.index, entry.hash);
        let expected_entry_index = expected_base
            .index()
            .checked_add(1)
            .ok_or_else(|| Error::InvalidEntry("applied index is exhausted".into()))?;
        if entry.index != expected_entry_index || entry.recompute_hash() != entry.hash {
            return Err(Error::InvalidEntry(
                "metadata-only embedded entry is invalid or not the next slot".into(),
            ));
        }
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        if pending_from(&tx)?.is_some()
            || meta_anchor(&tx, "applied_tip")? != expected_base
            || meta_configuration(&tx)? != *expected_configuration
            || meta_state(&tx)? != expected_user_state
        {
            return Err(Error::InvalidEntry(
                "metadata-only embedded commit does not match the committed base".into(),
            ));
        }
        insert_or_validate_embedded_entry(&tx, entry)?;
        put_anchor(&tx, "applied_tip", entry_anchor)?;
        tx.commit().map_err(sqlite_error)
    }

    pub(crate) fn commit_applied(
        &self,
        pending: &PendingApply,
        configuration_state: &ConfigurationState,
        receipts: &[RequestReceipt],
    ) -> Result<()> {
        if receipts.len() > super::MAX_QWAL_V3_RECEIPTS {
            return Err(Error::ResourceExhausted(format!(
                "applied receipt batch exceeds {} members",
                super::MAX_QWAL_V3_RECEIPTS
            )));
        }
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        if pending_from(&tx)?.as_ref() != Some(pending) {
            return Err(Error::InvalidEntry(
                "pending apply intent is missing or different".into(),
            ));
        }
        commit_applied_transaction(tx, pending, configuration_state, receipts)
    }

    /// Atomically publishes a locally installed QWAL target, its receipts, and
    /// its rebuildable qlog mirror without a pre-install durability intent.
    pub(crate) fn commit_rebuildable_apply(
        &self,
        pending: &PendingApply,
        entry: &LogEntry,
        configuration_state: &ConfigurationState,
        receipts: &[RequestReceipt],
    ) -> Result<()> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        if pending_from(&tx)?
            .as_ref()
            .is_some_and(|existing| existing != pending)
            || meta_anchor(&tx, "applied_tip")? != pending.base
            || meta_state(&tx)? != pending.base_state
            || LogAnchor::new(entry.index, entry.hash) != pending.entry
            || entry.recompute_hash() != entry.hash
        {
            return Err(Error::InvalidEntry(
                "rebuildable apply does not exactly extend the committed base".into(),
            ));
        }
        insert_or_validate_embedded_entry(&tx, entry)?;
        commit_applied_transaction(tx, pending, configuration_state, receipts)
    }

    pub fn export_replicated_snapshot(&self) -> Result<Vec<u8>> {
        if self.pending()?.is_some() {
            return Err(Error::InvalidSnapshot(
                "cannot export control state while apply is pending".into(),
            ));
        }
        let mut statement = self.conn.prepare(
            "SELECT request_id, request_digest, original_log_index, original_log_hash, result_blob FROM request_receipts ORDER BY request_id",
        ).map_err(sqlite_error)?;
        let receipts = statement
            .query_map([], |row| {
                let request_id: String = row.get(0)?;
                Ok(RequestReceipt::new(
                    request_id,
                    hash_from_blob(row.get(1)?)?,
                    LogAnchor::new(u64_from_sql(row.get(2)?)?, hash_from_blob(row.get(3)?)?),
                    row.get(4)?,
                ))
            })
            .map_err(sqlite_error)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sqlite_error)?;
        let tip = meta_anchor(&self.conn, "applied_tip")?;
        let snapshot = ReplicatedSnapshot {
            cluster_id: meta_text(&self.conn, "cluster_id")?,
            epoch: meta_u64(&self.conn, "epoch")?,
            configuration_state: meta_configuration(&self.conn)?,
            recovery_generation: meta_u64(&self.conn, "recovery_generation")?,
            materializer_fingerprint: meta_hash(&self.conn, "materializer_fingerprint")?,
            user_state: meta_state(&self.conn)?,
            applied_tip: tip,
            receipts,
        };
        encode_snapshot(&snapshot)
    }

    #[cfg(test)]
    pub(crate) fn import_replicated_snapshot(&self, encoded: &[u8]) -> Result<()> {
        self.import_replicated_snapshot_with_recovery_generation(encoded, None)
    }

    /// Atomically imports replicated state while optionally rebinding it to a
    /// recovery anchor generation. The destination node identity is retained.
    pub(crate) fn import_replicated_snapshot_with_recovery_generation(
        &self,
        encoded: &[u8],
        recovery_generation: Option<u64>,
    ) -> Result<()> {
        let snapshot = decode_snapshot(encoded)?;
        let recovery_generation = recovery_generation.unwrap_or(snapshot.recovery_generation);
        if recovery_generation == 0 {
            return Err(Error::InvalidSnapshot(
                "control snapshot recovery generation must be positive".into(),
            ));
        }
        let local = self.identity()?;
        if snapshot.cluster_id != local.cluster_id {
            return Err(Error::IdentityMismatch("cluster_id".into()));
        }
        if snapshot.epoch != local.epoch {
            return Err(Error::IdentityMismatch("epoch".into()));
        }
        if snapshot.materializer_fingerprint != local.materializer_fingerprint {
            return Err(Error::IdentityMismatch("materializer_fingerprint".into()));
        }
        for receipt in &snapshot.receipts {
            validate_receipt(receipt)?;
        }

        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        tx.execute("DELETE FROM request_receipts", [])
            .map_err(sqlite_error)?;
        for receipt in &snapshot.receipts {
            insert_or_validate_receipt(&tx, receipt)?;
        }
        put_configuration(&tx, &snapshot.configuration_state)?;
        put_u64(&tx, "recovery_generation", recovery_generation)?;
        validate_state(snapshot.user_state)?;
        put_state(&tx, snapshot.user_state)?;
        put_anchor(&tx, "applied_tip", snapshot.applied_tip)?;
        tx.execute("DELETE FROM pending_apply", [])
            .map_err(sqlite_error)?;
        tx.execute("DELETE FROM embedded_qlog", [])
            .map_err(sqlite_error)?;
        tx.commit().map_err(sqlite_error)
    }

    fn validate_schema(&self) -> Result<()> {
        let magic = get_meta(&self.conn, "magic")?;
        if magic != CONTROL_MAGIC {
            return Err(Error::Sqlite("invalid control sidecar magic".into()));
        }
        let version = meta_u64(&self.conn, "schema_version")?;
        if version != CONTROL_SCHEMA_VERSION {
            return Err(Error::Sqlite(format!(
                "unsupported control sidecar schema version {version}"
            )));
        }
        for key in REQUIRED_META_KEYS {
            let _: Vec<u8> = get_meta(&self.conn, key)?;
        }
        for (table, expected_sql, expected_columns) in [
            (
                "control_meta",
                CREATE_CONTROL_META_SQL,
                CONTROL_META_COLUMNS,
            ),
            (
                "request_receipts",
                CREATE_REQUEST_RECEIPTS_SQL,
                REQUEST_RECEIPT_COLUMNS,
            ),
            (
                "pending_apply",
                CREATE_PENDING_APPLY_SQL,
                PENDING_APPLY_COLUMNS,
            ),
            (
                "embedded_qlog",
                CREATE_EMBEDDED_LOG_SQL,
                EMBEDDED_LOG_COLUMNS,
            ),
        ] {
            validate_table_schema(&self.conn, table, expected_sql, expected_columns)?;
        }
        let unexpected_objects: i64 = self.conn.query_row(
            "SELECT count(*) FROM sqlite_schema
             WHERE name NOT LIKE 'sqlite_%'
               AND (type <> 'table' OR name NOT IN ('control_meta','request_receipts','pending_apply','embedded_qlog'))",
            [],
            |row| row.get(0),
        ).map_err(sqlite_error)?;
        if unexpected_objects != 0 {
            return Err(Error::Sqlite(
                "control sidecar contains unexpected schema objects".into(),
            ));
        }
        let meta_count: i64 = self
            .conn
            .query_row("SELECT count(*) FROM control_meta", [], |row| row.get(0))
            .map_err(sqlite_error)?;
        if meta_count != i64::try_from(REQUIRED_META_KEYS.len()).expect("small key count") {
            return Err(Error::Sqlite(
                "control sidecar has unknown or duplicate metadata".into(),
            ));
        }
        // Decode every typed value here so corrupt state fails before serving.
        let identity = self.identity()?;
        validate_new_identity(&identity)?;
        let _ = meta_anchor(&self.conn, "applied_tip")?;
        let pending = self.pending()?;
        let pending_rows: i64 = self
            .conn
            .query_row("SELECT count(*) FROM pending_apply", [], |row| row.get(0))
            .map_err(sqlite_error)?;
        let expected_pending_rows = if pending.is_some() { 1 } else { 0 };
        if pending_rows != expected_pending_rows {
            return Err(Error::Sqlite(
                "control sidecar has invalid pending apply rows".into(),
            ));
        }
        validate_all_receipts(&self.conn)?;
        Ok(())
    }

    fn validate_identity(&self, expected: &ControlIdentity) -> Result<()> {
        let actual = self.identity()?;
        if actual.cluster_id != expected.cluster_id {
            return Err(Error::IdentityMismatch("cluster_id".into()));
        }
        if actual.node_id != expected.node_id {
            return Err(Error::IdentityMismatch("node_id".into()));
        }
        if actual.epoch != expected.epoch {
            return Err(Error::IdentityMismatch("epoch".into()));
        }
        if actual.configuration_state != expected.configuration_state {
            return Err(Error::IdentityMismatch("configuration_state".into()));
        }
        if actual.recovery_generation != expected.recovery_generation {
            return Err(Error::IdentityMismatch("recovery_generation".into()));
        }
        if actual.materializer_fingerprint != expected.materializer_fingerprint {
            return Err(Error::IdentityMismatch("materializer_fingerprint".into()));
        }
        if actual.user_state != expected.user_state {
            return Err(Error::IdentityMismatch("user_state".into()));
        }
        Ok(())
    }
}

fn validate_new_identity(identity: &ControlIdentity) -> Result<()> {
    if identity.cluster_id.is_empty() {
        return Err(Error::IdentityMismatch("cluster_id".into()));
    }
    if identity.node_id.is_empty() {
        return Err(Error::IdentityMismatch("node_id".into()));
    }
    if identity.epoch == 0 {
        return Err(Error::IdentityMismatch("epoch".into()));
    }
    validate_state(identity.user_state)?;
    Ok(())
}

fn validate_state(state: StateIdentityV3) -> Result<()> {
    if !(512..=65_536).contains(&state.page_size)
        || !state.page_size.is_power_of_two()
        || state.page_count == 0
        || state.state_root == LogHash::ZERO
    {
        return Err(Error::IdentityMismatch("user_state".into()));
    }
    Ok(())
}

fn validate_table_schema(
    conn: &Connection,
    table: &str,
    expected_sql: &str,
    expected_columns: &[ExpectedColumn],
) -> Result<()> {
    let declared_sql: String = conn
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            params![table],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_error)?
        .ok_or_else(|| Error::Sqlite(format!("control sidecar is missing table {table}")))?;
    if normalize_schema_sql(&declared_sql) != normalize_schema_sql(expected_sql) {
        return Err(Error::Sqlite(format!(
            "invalid control sidecar declaration for table {table}"
        )));
    }

    let pragma =
        format!("SELECT name, type, [notnull], pk FROM pragma_table_info('{table}') ORDER BY cid");
    let mut statement = conn.prepare(&pragma).map_err(sqlite_error)?;
    let actual = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? != 0,
                row.get::<_, i64>(3)?,
            ))
        })
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)?;
    let matches = actual.len() == expected_columns.len()
        && actual.iter().zip(expected_columns).all(
            |((name, declared_type, not_null, primary_key), expected)| {
                name == expected.0
                    && declared_type.eq_ignore_ascii_case(expected.1)
                    && *not_null == expected.2
                    && *primary_key == expected.3
            },
        );
    if !matches {
        return Err(Error::Sqlite(format!(
            "invalid control sidecar columns for table {table}"
        )));
    }
    Ok(())
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace() && *character != ';')
        .flat_map(char::to_lowercase)
        .collect()
}

fn commit_applied_transaction(
    tx: Transaction<'_>,
    pending: &PendingApply,
    configuration_state: &ConfigurationState,
    receipts: &[RequestReceipt],
) -> Result<()> {
    if receipts.len() > super::MAX_QWAL_V3_RECEIPTS {
        return Err(Error::ResourceExhausted(format!(
            "applied receipt batch exceeds {} members",
            super::MAX_QWAL_V3_RECEIPTS
        )));
    }
    if configuration_state.config_id() < meta_configuration(&tx)?.config_id() {
        return Err(Error::InvalidEntry(
            "configuration state moved backwards".into(),
        ));
    }
    let mut result_bytes = 0usize;
    let mut request_ids = HashSet::with_capacity(receipts.len());
    for receipt in receipts {
        validate_receipt(receipt)?;
        result_bytes = result_bytes
            .checked_add(receipt.result_blob.len())
            .ok_or_else(|| Error::ResourceExhausted("receipt result bytes overflow".into()))?;
        if result_bytes > super::MAX_QWAL_V3_BYTES {
            return Err(Error::ResourceExhausted(format!(
                "receipt results exceed {} bytes",
                super::MAX_QWAL_V3_BYTES
            )));
        }
        if receipt.original_anchor != pending.entry {
            return Err(Error::InvalidEntry(
                "request receipt anchor does not match applied entry".into(),
            ));
        }
        if !request_ids.insert(receipt.request_id.as_str()) {
            return Err(Error::InvalidEntry(
                "applied receipt request ids must be unique".into(),
            ));
        }
    }
    let lookups = receipts
        .iter()
        .map(|receipt| (receipt.request_id(), receipt.request_digest()))
        .collect::<Vec<_>>();
    let existing = lookup_requests_from(&tx, &lookups)?;
    let mut absent = Vec::with_capacity(receipts.len());
    for (receipt, existing) in receipts.iter().zip(existing) {
        match existing? {
            None => absent.push(receipt),
            Some(existing) if existing == *receipt => {}
            Some(existing) => {
                return Err(Error::RequestConflict(RequestConflict {
                    request_id: receipt.request_id.clone(),
                    original_outcome: RequestOutcome::new(
                        existing.original_anchor.index(),
                        existing.original_anchor.hash(),
                    ),
                }));
            }
        }
    }
    insert_receipts_bulk(&tx, pending.entry, &absent)?;
    put_anchor(&tx, "applied_tip", pending.entry)?;
    put_configuration(&tx, configuration_state)?;
    put_state(&tx, pending.target_state)?;
    tx.execute("DELETE FROM pending_apply WHERE singleton = 1", [])
        .map_err(sqlite_error)?;
    tx.commit().map_err(sqlite_error)
}

fn validate_receipt(receipt: &RequestReceipt) -> Result<()> {
    if receipt.request_id.is_empty() || receipt.request_id.len() > usize::from(u16::MAX) {
        return Err(Error::InvalidCommand(
            "request id must contain 1..=65535 bytes".into(),
        ));
    }
    if receipt.result_blob.len() > MAX_RESULT_BLOB_BYTES {
        return Err(Error::ResourceExhausted(format!(
            "request result exceeds {MAX_RESULT_BLOB_BYTES} bytes"
        )));
    }
    super::validate_sql_result_blob_bounds(receipt.result_blob())
}

fn validate_all_receipts(conn: &Connection) -> Result<()> {
    let mut statement = conn
        .prepare(
            "SELECT request_id, request_digest, original_log_index, original_log_hash, result_blob FROM request_receipts ORDER BY request_id",
        )
        .map_err(sqlite_error)?;
    let receipts = statement
        .query_map([], |row| {
            Ok(RequestReceipt::new(
                row.get::<_, String>(0)?,
                hash_from_blob(row.get(1)?)?,
                LogAnchor::new(u64_from_sql(row.get(2)?)?, hash_from_blob(row.get(3)?)?),
                row.get(4)?,
            ))
        })
        .map_err(sqlite_error)?;
    for receipt in receipts {
        validate_receipt(&receipt.map_err(sqlite_error)?)?;
    }
    Ok(())
}

fn lookup_requests_from(
    conn: &Connection,
    requests: &[(&str, LogHash)],
) -> Result<Vec<Result<Option<RequestReceipt>>>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    if requests.len() > super::MAX_QWAL_V3_RECEIPTS {
        return Err(Error::ResourceExhausted(format!(
            "request lookup exceeds {} members",
            super::MAX_QWAL_V3_RECEIPTS
        )));
    }
    let mut request_ids = HashSet::with_capacity(requests.len());
    for (request_id, _) in requests {
        if !request_ids.insert(*request_id) {
            return Err(Error::InvalidCommand(format!(
                "duplicate request id in bulk lookup: {request_id}"
            )));
        }
    }

    let mut aligned = Vec::with_capacity(requests.len());
    for chunk in requests.chunks(RECEIPT_LOOKUP_CHUNK_SIZE) {
        aligned.extend(lookup_request_chunk(conn, chunk)?);
    }
    Ok(aligned)
}

fn lookup_request_chunk(
    conn: &Connection,
    requests: &[(&str, LogHash)],
) -> Result<Vec<Result<Option<RequestReceipt>>>> {
    let mut sql = String::from("WITH requested(position,request_id) AS (VALUES ");
    for index in 0..requests.len() {
        if index != 0 {
            sql.push(',');
        }
        write!(sql, "({index},?{})", index + 1)
            .expect("writing to an in-memory SQL string cannot fail");
    }
    sql.push_str(
        ") SELECT requested.request_id,receipts.request_digest,receipts.original_log_index,receipts.original_log_hash,receipts.result_blob FROM requested LEFT JOIN request_receipts AS receipts ON receipts.request_id=requested.request_id ORDER BY requested.position",
    );
    let mut statement = conn.prepare(&sql).map_err(sqlite_error)?;
    let receipts = statement
        .query_map(
            params_from_iter(requests.iter().map(|(request_id, _)| *request_id)),
            |row| {
                let request_id: String = row.get(0)?;
                let Some(request_digest) = row.get::<_, Option<Vec<u8>>>(1)? else {
                    return Ok(None);
                };
                Ok(Some(RequestReceipt::new(
                    request_id,
                    hash_from_blob(request_digest)?,
                    LogAnchor::new(u64_from_sql(row.get(2)?)?, hash_from_blob(row.get(3)?)?),
                    row.get(4)?,
                )))
            },
        )
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)?;
    if receipts.len() != requests.len() {
        return Err(Error::Sqlite(
            "bulk receipt lookup did not preserve request cardinality".into(),
        ));
    }
    let mut aligned = Vec::with_capacity(receipts.len());
    for ((request_id, request_digest), receipt) in requests.iter().zip(receipts) {
        if let Some(receipt) = &receipt {
            if receipt.request_id() != *request_id {
                return Err(Error::Sqlite(
                    "bulk receipt lookup did not preserve request order".into(),
                ));
            }
            if receipt.request_digest() != *request_digest {
                aligned.push(Err(Error::RequestConflict(RequestConflict {
                    request_id: (*request_id).to_owned(),
                    original_outcome: RequestOutcome::new(
                        receipt.original_anchor.index(),
                        receipt.original_anchor.hash(),
                    ),
                })));
                continue;
            }
        }
        aligned.push(Ok(receipt));
    }
    Ok(aligned)
}

fn insert_receipts_bulk(
    conn: &Connection,
    anchor: LogAnchor,
    receipts: &[&RequestReceipt],
) -> Result<()> {
    if receipts.is_empty() {
        return Ok(());
    }
    if receipts.len() > super::MAX_QWAL_V3_RECEIPTS {
        return Err(Error::ResourceExhausted(
            "bulk receipt insert exceeds the QWAL receipt limit".into(),
        ));
    }
    for chunk in receipts.chunks(RECEIPT_INSERT_CHUNK_SIZE) {
        insert_receipt_chunk(conn, anchor, chunk)?;
    }
    Ok(())
}

fn insert_receipt_chunk(
    conn: &Connection,
    anchor: LogAnchor,
    receipts: &[&RequestReceipt],
) -> Result<()> {
    let bind_count = 2 + receipts.len() * 3;
    debug_assert!(bind_count <= SQLITE_VARIABLE_LIMIT);

    let mut sql = String::from(
        "INSERT INTO request_receipts(request_id,request_digest,original_log_index,original_log_hash,result_blob) VALUES ",
    );
    for index in 0..receipts.len() {
        if index != 0 {
            sql.push(',');
        }
        let request_id = 3 + index * 3;
        let request_digest = request_id + 1;
        let result_blob = request_id + 2;
        write!(
            sql,
            "(?{request_id},?{request_digest},?1,?2,?{result_blob})"
        )
        .expect("writing to an in-memory SQL string cannot fail");
    }
    let mut values = Vec::with_capacity(bind_count);
    values.push(Value::Integer(u64_to_sql(anchor.index())?));
    values.push(Value::Blob(anchor.hash().as_bytes().to_vec()));
    for receipt in receipts {
        values.push(Value::Text(receipt.request_id.clone()));
        values.push(Value::Blob(receipt.request_digest.as_bytes().to_vec()));
        values.push(Value::Blob(receipt.result_blob.clone()));
    }
    let inserted = conn
        .execute(&sql, params_from_iter(values.iter()))
        .map_err(sqlite_error)?;
    if inserted != receipts.len() {
        return Err(Error::Sqlite(
            "bulk receipt insert affected an unexpected row count".into(),
        ));
    }
    Ok(())
}

fn insert_or_validate_receipt(conn: &Connection, receipt: &RequestReceipt) -> Result<()> {
    if let Some(existing) = conn.query_row(
        "SELECT request_digest, original_log_index, original_log_hash, result_blob FROM request_receipts WHERE request_id=?1",
        params![receipt.request_id],
        |row| Ok(RequestReceipt::new(
            &receipt.request_id,
            hash_from_blob(row.get(0)?)?,
            LogAnchor::new(u64_from_sql(row.get(1)?)?, hash_from_blob(row.get(2)?)?),
            row.get(3)?,
        )),
    ).optional().map_err(sqlite_error)? {
        if existing == *receipt { return Ok(()); }
        return Err(Error::RequestConflict(RequestConflict {
            request_id: receipt.request_id.clone(),
            original_outcome: RequestOutcome::new(existing.original_anchor.index(), existing.original_anchor.hash()),
        }));
    }
    conn.execute(
        "INSERT INTO request_receipts(request_id,request_digest,original_log_index,original_log_hash,result_blob) VALUES(?1,?2,?3,?4,?5)",
        params![receipt.request_id, receipt.request_digest.as_bytes().as_slice(), u64_to_sql(receipt.original_anchor.index())?, receipt.original_anchor.hash().as_bytes().as_slice(), receipt.result_blob],
    ).map_err(sqlite_error)?;
    Ok(())
}

fn insert_pending(conn: &Connection, value: &PendingApply) -> Result<()> {
    validate_state(value.base_state)?;
    validate_state(value.target_state)?;
    conn.execute(
        "INSERT INTO pending_apply(singleton,base_index,base_hash,entry_index,entry_hash,base_page_size,base_page_count,base_state_root,target_page_size,target_page_count,target_state_root) VALUES(1,?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![u64_to_sql(value.base.index())?, value.base.hash().as_bytes().as_slice(), u64_to_sql(value.entry.index())?, value.entry.hash().as_bytes().as_slice(), i64::from(value.base_state.page_size), i64::from(value.base_state.page_count), value.base_state.state_root.as_bytes().as_slice(), i64::from(value.target_state.page_size), i64::from(value.target_state.page_count), value.target_state.state_root.as_bytes().as_slice()],
    ).map_err(sqlite_error)?;
    Ok(())
}

fn insert_or_validate_embedded_entry(conn: &Connection, entry: &LogEntry) -> Result<()> {
    let cluster_id = meta_text(conn, "cluster_id")?;
    let epoch = meta_u64(conn, "epoch")?;
    let configuration = meta_configuration(conn)?;
    if entry.cluster_id != cluster_id
        || entry.epoch != epoch
        || configuration.validate_entry(entry).is_err()
    {
        return Err(Error::InvalidEntry(
            "embedded qlog entry identity is invalid".into(),
        ));
    }
    let index = u64_to_sql(entry.index)?;
    let existing = conn
        .query_row(
            "SELECT initial_configuration,entry_bytes FROM embedded_qlog WHERE log_index=?1",
            params![index],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()
        .map_err(sqlite_error)?;
    if let Some((initial_configuration, existing)) = existing {
        let initial_configuration =
            decode_configuration_bytes(&initial_configuration, "embedded qlog")?;
        if initial_configuration == configuration
            && decode_embedded_log_entry(&existing, &cluster_id, initial_configuration)?.0 == *entry
        {
            return Ok(());
        }
        return Err(Error::InvalidEntry(
            "embedded qlog index already contains another entry".into(),
        ));
    }
    let encoded = rhiza_log::encode_segment(std::slice::from_ref(entry));
    let initial_configuration =
        serde_json::to_vec(&configuration).map_err(|error| Error::Sqlite(error.to_string()))?;
    conn.execute(
        "INSERT INTO embedded_qlog(log_index,initial_configuration,entry_bytes) VALUES(?1,?2,?3)",
        params![index, initial_configuration, encoded],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn decode_embedded_log_entry(
    encoded: &[u8],
    cluster_id: &str,
    initial_configuration: ConfigurationState,
) -> Result<(LogEntry, ConfigurationState)> {
    let (entries, resulting_configuration) =
        rhiza_log::decode_segment_for_cluster_with_configuration(
            encoded,
            cluster_id,
            initial_configuration,
        )
        .map_err(|error| Error::InvalidEntry(error.to_string()))?;
    let [entry] = entries.as_slice() else {
        return Err(Error::InvalidEntry(
            "embedded qlog value must contain exactly one entry".into(),
        ));
    };
    Ok((entry.clone(), resulting_configuration))
}

fn pending_from(conn: &Connection) -> Result<Option<PendingApply>> {
    conn.query_row(
        "SELECT base_index,base_hash,entry_index,entry_hash,base_page_size,base_page_count,base_state_root,target_page_size,target_page_count,target_state_root FROM pending_apply WHERE singleton=1",
        [],
        |row| Ok(PendingApply::new(
            LogAnchor::new(u64_from_sql(row.get(0)?)?, hash_from_blob(row.get(1)?)?),
            LogAnchor::new(u64_from_sql(row.get(2)?)?, hash_from_blob(row.get(3)?)?),
            StateIdentityV3 {
                page_size: u32_from_sql(row.get(4)?)?,
                page_count: u32_from_sql(row.get(5)?)?,
                state_root: hash_from_blob(row.get(6)?)?,
            },
            StateIdentityV3 {
                page_size: u32_from_sql(row.get(7)?)?,
                page_count: u32_from_sql(row.get(8)?)?,
                state_root: hash_from_blob(row.get(9)?)?,
            },
        )),
    ).optional().map_err(sqlite_error)?.map(|pending| {
        validate_state(pending.base_state)?;
        validate_state(pending.target_state)?;
        Ok(pending)
    }).transpose()
}

fn encode_snapshot(snapshot: &ReplicatedSnapshot) -> Result<Vec<u8>> {
    let body =
        serde_json::to_vec(snapshot).map_err(|error| Error::InvalidSnapshot(error.to_string()))?;
    let digest = LogHash::digest(&[&body]);
    let mut encoded = Vec::with_capacity(SNAPSHOT_MAGIC.len() + 32 + body.len());
    encoded.extend_from_slice(SNAPSHOT_MAGIC);
    encoded.extend_from_slice(digest.as_bytes());
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

fn decode_snapshot(encoded: &[u8]) -> Result<ReplicatedSnapshot> {
    let payload = encoded.strip_prefix(SNAPSHOT_MAGIC).ok_or_else(|| {
        Error::InvalidSnapshot("control snapshot magic/version is invalid".into())
    })?;
    if payload.len() < 32 {
        return Err(Error::InvalidSnapshot(
            "control snapshot is truncated".into(),
        ));
    }
    let expected = LogHash::from_bytes(payload[..32].try_into().expect("32-byte checked slice"));
    let body = &payload[32..];
    if LogHash::digest(&[body]) != expected {
        return Err(Error::InvalidSnapshot(
            "control snapshot digest mismatch".into(),
        ));
    }
    let snapshot: ReplicatedSnapshot =
        serde_json::from_slice(body).map_err(|error| Error::InvalidSnapshot(error.to_string()))?;
    let canonical =
        serde_json::to_vec(&snapshot).map_err(|error| Error::InvalidSnapshot(error.to_string()))?;
    if canonical != body {
        return Err(Error::InvalidSnapshot(
            "control snapshot encoding is not canonical".into(),
        ));
    }
    let mut previous = None;
    for receipt in &snapshot.receipts {
        validate_receipt(receipt)?;
        if previous.is_some_and(|id: &str| id >= receipt.request_id.as_str()) {
            return Err(Error::InvalidSnapshot(
                "control snapshot receipts are not uniquely sorted".into(),
            ));
        }
        previous = Some(receipt.request_id.as_str());
    }
    Ok(snapshot)
}

fn put_meta(conn: &Connection, key: &str, value: &[u8]) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO control_meta(key,value) VALUES(?1,?2)",
        params![key, value],
    )
    .map_err(sqlite_error)?;
    Ok(())
}
fn get_meta(conn: &Connection, key: &str) -> Result<Vec<u8>> {
    conn.query_row(
        "SELECT value FROM control_meta WHERE key=?1",
        params![key],
        |row| row.get(0),
    )
    .optional()
    .map_err(sqlite_error)?
    .ok_or_else(|| Error::Sqlite(format!("control sidecar is missing {key}")))
}
fn put_u64(conn: &Connection, key: &str, value: u64) -> Result<()> {
    put_meta(conn, key, &value.to_be_bytes())
}
fn meta_u64(conn: &Connection, key: &str) -> Result<u64> {
    let bytes = get_meta(conn, key)?;
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::Sqlite(format!("invalid control u64 {key}")))?;
    Ok(u64::from_be_bytes(bytes))
}
fn put_hash(conn: &Connection, key: &str, value: LogHash) -> Result<()> {
    put_meta(conn, key, value.as_bytes())
}
fn meta_hash(conn: &Connection, key: &str) -> Result<LogHash> {
    let bytes = get_meta(conn, key)?;
    Ok(LogHash::from_bytes(bytes.try_into().map_err(|_| {
        Error::Sqlite(format!("invalid control hash {key}"))
    })?))
}
fn put_state(conn: &Connection, state: StateIdentityV3) -> Result<()> {
    validate_state(state)?;
    put_u64(conn, "page_size", u64::from(state.page_size))?;
    put_u64(conn, "page_count", u64::from(state.page_count))?;
    put_hash(conn, "state_root", state.state_root)
}
fn meta_state(conn: &Connection) -> Result<StateIdentityV3> {
    let state = StateIdentityV3 {
        page_size: u32::try_from(meta_u64(conn, "page_size")?)
            .map_err(|_| Error::Sqlite("invalid control page_size".into()))?,
        page_count: u32::try_from(meta_u64(conn, "page_count")?)
            .map_err(|_| Error::Sqlite("invalid control page_count".into()))?,
        state_root: meta_hash(conn, "state_root")?,
    };
    validate_state(state)?;
    Ok(state)
}
fn meta_text(conn: &Connection, key: &str) -> Result<String> {
    String::from_utf8(get_meta(conn, key)?)
        .map_err(|_| Error::Sqlite(format!("invalid control text {key}")))
}
fn put_configuration(conn: &Connection, value: &ConfigurationState) -> Result<()> {
    let encoded = serde_json::to_vec(value).map_err(|error| Error::Sqlite(error.to_string()))?;
    put_meta(conn, "configuration_state", &encoded)
}
fn meta_configuration(conn: &Connection) -> Result<ConfigurationState> {
    decode_configuration_bytes(&get_meta(conn, "configuration_state")?, "control")
}
fn decode_configuration_bytes(encoded: &[u8], source: &str) -> Result<ConfigurationState> {
    serde_json::from_slice(encoded)
        .map_err(|error| Error::Sqlite(format!("invalid {source} configuration: {error}")))
}
fn put_anchor(conn: &Connection, key: &str, value: LogAnchor) -> Result<()> {
    put_meta(conn, key, &anchor_bytes(value))
}
fn anchor_bytes(value: LogAnchor) -> [u8; 40] {
    let mut encoded = [0_u8; 40];
    encoded[..8].copy_from_slice(&value.index().to_be_bytes());
    encoded[8..].copy_from_slice(value.hash().as_bytes());
    encoded
}
fn meta_anchor(conn: &Connection, key: &str) -> Result<LogAnchor> {
    let bytes = get_meta(conn, key)?;
    if bytes.len() != 40 {
        return Err(Error::Sqlite(format!("invalid control anchor {key}")));
    }
    Ok(LogAnchor::new(
        u64::from_be_bytes(bytes[..8].try_into().expect("length checked")),
        LogHash::from_bytes(bytes[8..].try_into().expect("length checked")),
    ))
}

fn u64_to_sql(value: u64) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| Error::ResourceExhausted("control integer exceeds SQLite i64".into()))
}
fn u64_from_sql(value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(0, value))
}
fn u32_from_sql(value: i64) -> rusqlite::Result<u32> {
    u32::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(0, value))
}
fn hash_from_blob(bytes: Vec<u8>) -> rusqlite::Result<LogHash> {
    let bytes: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Blob,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected 32-byte hash, got {} bytes", bytes.len()),
            )),
        )
    })?;
    Ok(LogHash::from_bytes(bytes))
}
fn sqlite_error(error: rusqlite::Error) -> Error {
    Error::Sqlite(error.to_string())
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::Io(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(label: &[u8]) -> LogHash {
        LogHash::digest(&[label])
    }
    fn state(label: &[u8]) -> StateIdentityV3 {
        StateIdentityV3 {
            page_size: 512,
            page_count: 8,
            state_root: hash(label),
        }
    }
    fn identity(node: &str) -> ControlIdentity {
        ControlIdentity::new(
            "cluster",
            node,
            7,
            ConfigurationState::active(3, hash(b"config")),
            11,
            hash(b"fingerprint"),
            state(b"base-db"),
        )
    }
    fn pending() -> PendingApply {
        PendingApply::new(
            LogAnchor::new(0, LogHash::ZERO),
            LogAnchor::new(1, hash(b"entry")),
            state(b"base-db"),
            state(b"target-db"),
        )
    }
    fn receipt(digest: LogHash) -> RequestReceipt {
        receipt_for("request-1", digest)
    }
    fn receipt_for(request_id: &str, digest: LogHash) -> RequestReceipt {
        RequestReceipt::new(
            request_id,
            digest,
            LogAnchor::new(1, hash(b"entry")),
            crate::encode_sql_result(&crate::SqlCommandResult {
                statement_results: Vec::new(),
            })
            .unwrap(),
        )
    }

    #[test]
    fn open_rejects_identity_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        ControlStore::create(&path, &identity("node-a")).unwrap();
        let error = match ControlStore::open_existing(&path, &identity("node-b")) {
            Ok(_) => panic!("mismatched node identity must fail"),
            Err(error) => error,
        };
        assert_eq!(error, Error::IdentityMismatch("node_id".into()));
    }

    #[test]
    fn open_rejects_state_root_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        ControlStore::create(&path, &identity("node-a")).unwrap();
        let mut expected = identity("node-a");
        expected.user_state.state_root = hash(b"different-root");

        assert!(matches!(
            ControlStore::open_existing(&path, &expected),
            Err(Error::IdentityMismatch(field)) if field == "user_state"
        ));
    }

    #[test]
    fn control_identity_and_snapshot_reject_zero_state_root() {
        let dir = tempfile::tempdir().unwrap();
        let mut invalid = identity("node-a");
        invalid.user_state.state_root = LogHash::ZERO;
        assert!(matches!(
            ControlStore::create(dir.path().join("invalid.control"), &invalid),
            Err(Error::IdentityMismatch(field)) if field == "user_state"
        ));

        let destination =
            ControlStore::create(dir.path().join("destination.control"), &identity("node-b"))
                .unwrap();
        let snapshot = ReplicatedSnapshot {
            cluster_id: "cluster".into(),
            epoch: 7,
            configuration_state: ConfigurationState::active(3, hash(b"config")),
            recovery_generation: 11,
            materializer_fingerprint: hash(b"fingerprint"),
            user_state: invalid.user_state(),
            applied_tip: LogAnchor::new(0, LogHash::ZERO),
            receipts: Vec::new(),
        };

        assert!(matches!(
            destination.import_replicated_snapshot(&encode_snapshot(&snapshot).unwrap()),
            Err(Error::IdentityMismatch(field)) if field == "user_state"
        ));
        assert_eq!(destination.user_state().unwrap(), state(b"base-db"));
    }

    #[test]
    fn control_identity_round_trips_page_state() {
        let dir = tempfile::tempdir().unwrap();
        let expected = identity("node-a");
        let store = ControlStore::create(dir.path().join("sqlite.control"), &expected).unwrap();

        assert_eq!(store.identity().unwrap(), expected);
        assert_eq!(store.user_state().unwrap(), state(b"base-db"));
    }

    #[test]
    fn quorum_authoritative_control_cache_disables_local_sync() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("sqlite.control"), &identity("node-a")).unwrap();

        let synchronous: i64 = store
            .conn
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap();

        assert_eq!(synchronous, 0);
    }

    #[test]
    fn rebuildable_apply_publishes_entry_tip_receipt_and_state_together() {
        let dir = tempfile::tempdir().unwrap();
        let original = identity("node-a");
        let store = ControlStore::create(dir.path().join("sqlite.control"), &original).unwrap();
        let configuration = original.configuration_state().clone();
        let entry_hash = LogEntry::calculate_hash(
            "cluster",
            1,
            7,
            configuration.config_id(),
            rhiza_core::EntryType::Noop,
            LogHash::ZERO,
            &[],
        );
        let entry = LogEntry {
            cluster_id: "cluster".into(),
            epoch: 7,
            config_id: configuration.config_id(),
            index: 1,
            entry_type: rhiza_core::EntryType::Noop,
            payload: Vec::new(),
            prev_hash: LogHash::ZERO,
            hash: entry_hash,
        };
        let pending = PendingApply::new(
            LogAnchor::new(0, LogHash::ZERO),
            LogAnchor::new(1, entry_hash),
            original.user_state(),
            state(b"target-db"),
        );
        let receipt = RequestReceipt::new(
            "request-1",
            hash(b"request"),
            pending.entry(),
            crate::encode_sql_result(&crate::SqlCommandResult {
                statement_results: Vec::new(),
            })
            .unwrap(),
        );

        store
            .commit_rebuildable_apply(
                &pending,
                &entry,
                &configuration,
                std::slice::from_ref(&receipt),
            )
            .unwrap();

        assert_eq!(store.pending().unwrap(), None);
        assert_eq!(store.embedded_log_entries(1, 1).unwrap(), [entry]);
        assert_eq!(store.user_state().unwrap(), pending.target_state());
        assert_eq!(
            store.applied_tip().unwrap(),
            ApplyProgress::new(pending.entry().index(), pending.entry().hash())
        );
        assert_eq!(
            store.lookup_request("request-1", hash(b"request")).unwrap(),
            Some(receipt)
        );
    }

    #[test]
    fn bound_stop_embedded_log_survives_control_reopen_with_its_initial_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        let original = identity("node-a");
        let stop_change = rhiza_core::ConfigChange::stop(
            original.cluster_id(),
            original.configuration_state().config_id(),
            original.configuration_state().digest(),
            original.configuration_state().config_id() + 1,
            vec!["node-a".into(), "node-b".into(), "node-c".into()],
        )
        .unwrap();
        let command = stop_change.to_stored_command();
        let entry = LogEntry {
            cluster_id: original.cluster_id().into(),
            epoch: original.epoch(),
            config_id: original.configuration_state().config_id(),
            index: 1,
            entry_type: command.entry_type,
            payload: command.payload,
            prev_hash: LogHash::ZERO,
            hash: LogHash::ZERO,
        };
        let entry = LogEntry {
            hash: entry.recompute_hash(),
            ..entry
        };
        let stopped = original
            .configuration_state()
            .validate_entry(&entry)
            .unwrap();
        let pending = PendingApply::new(
            LogAnchor::new(0, LogHash::ZERO),
            LogAnchor::new(entry.index, entry.hash),
            original.user_state(),
            state(b"target-db"),
        );

        let store = ControlStore::create(&path, &original).unwrap();
        store
            .commit_rebuildable_apply(&pending, &entry, &stopped, &[])
            .unwrap();
        drop(store);

        let reopened_identity = ControlIdentity::new(
            original.cluster_id(),
            original.node_id(),
            original.epoch(),
            stopped,
            original.recovery_generation(),
            original.materializer_fingerprint(),
            pending.target_state(),
        );
        let reopened = ControlStore::open_existing(&path, &reopened_identity).unwrap();
        assert_eq!(reopened.embedded_log_entries(1, 1).unwrap(), [entry]);
    }

    #[test]
    fn v6_control_and_snapshot_reject_v5_without_migration() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        let store = ControlStore::create(&path, &identity("node-a")).unwrap();
        let mut old_snapshot = store.export_replicated_snapshot().unwrap();
        old_snapshot[SNAPSHOT_MAGIC.len() - 1] = 5;
        assert!(matches!(
            store.import_replicated_snapshot(&old_snapshot),
            Err(Error::InvalidSnapshot(_))
        ));
        drop(store);

        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "UPDATE control_meta SET value=?1 WHERE key='schema_version'",
            [5_u64.to_be_bytes().as_slice()],
        )
        .unwrap();
        drop(conn);
        assert!(matches!(
            ControlStore::open_existing_unchecked(&path),
            Err(Error::Sqlite(message)) if message.contains("version")
        ));
    }

    #[test]
    fn verified_checkpoint_compaction_removes_only_the_covered_embedded_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("sqlite.control"), &identity("node-a")).unwrap();
        let configuration = identity("node-a").configuration_state().clone();
        let entry = |index, prev_hash| {
            let hash = LogEntry::calculate_hash(
                "cluster",
                index,
                7,
                configuration.config_id(),
                rhiza_core::EntryType::Noop,
                prev_hash,
                &[],
            );
            LogEntry {
                cluster_id: "cluster".into(),
                epoch: 7,
                config_id: configuration.config_id(),
                index,
                entry_type: rhiza_core::EntryType::Noop,
                payload: Vec::new(),
                prev_hash,
                hash,
            }
        };
        let first = entry(1, LogHash::ZERO);
        let second = entry(2, first.hash);
        store
            .commit_metadata_only_entry_with_log(
                LogAnchor::new(0, LogHash::ZERO),
                &first,
                &configuration,
                state(b"base-db"),
            )
            .unwrap();
        store
            .commit_metadata_only_entry_with_log(
                LogAnchor::new(1, first.hash),
                &second,
                &configuration,
                state(b"base-db"),
            )
            .unwrap();

        store.compact_embedded_log_before(2).unwrap();

        assert_eq!(store.embedded_log_entries(2, 2).unwrap(), [second]);
        assert!(store.embedded_log_entries(1, 1).is_err());
        assert!(store.compact_embedded_log_before(3).is_err());
    }

    #[test]
    fn open_rejects_request_table_with_same_columns_but_no_primary_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        drop(ControlStore::create(&path, &identity("node-a")).unwrap());
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "DROP TABLE request_receipts;
             CREATE TABLE request_receipts (
                 request_id TEXT NOT NULL,
                 request_digest BLOB NOT NULL,
                 original_log_index INTEGER NOT NULL,
                 original_log_hash BLOB NOT NULL,
                 result_blob BLOB NOT NULL
             );",
        )
        .unwrap();
        drop(conn);

        assert!(matches!(
            ControlStore::open_existing_unchecked(&path),
            Err(Error::Sqlite(_))
        ));
    }

    #[test]
    fn open_rejects_pending_table_without_singleton_constraint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        drop(ControlStore::create(&path, &identity("node-a")).unwrap());
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "DROP TABLE pending_apply;
             CREATE TABLE pending_apply (
                 singleton INTEGER PRIMARY KEY,
                 base_index INTEGER NOT NULL,
                 base_hash BLOB NOT NULL,
                 entry_index INTEGER NOT NULL,
                 entry_hash BLOB NOT NULL,
                 base_page_size INTEGER NOT NULL,
                 base_page_count INTEGER NOT NULL,
                 base_state_root BLOB NOT NULL,
                 target_page_size INTEGER NOT NULL,
                 target_page_count INTEGER NOT NULL,
                 target_state_root BLOB NOT NULL
             );",
        )
        .unwrap();
        drop(conn);

        assert!(matches!(
            ControlStore::open_existing_unchecked(&path),
            Err(Error::Sqlite(_))
        ));
    }

    #[test]
    fn lookup_returns_duplicate_and_rejects_conflicting_digest() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("sqlite.control"), &identity("node-a")).unwrap();
        let pending = pending();
        let receipt = receipt(hash(b"request"));
        store.begin_pending(&pending).unwrap();
        store
            .commit_applied(
                &pending,
                identity("node-a").configuration_state(),
                std::slice::from_ref(&receipt),
            )
            .unwrap();
        assert_eq!(
            store.lookup_request("request-1", hash(b"request")).unwrap(),
            Some(receipt)
        );
        assert!(matches!(
            store.lookup_request("request-1", hash(b"other")),
            Err(Error::RequestConflict(_))
        ));
    }

    #[test]
    fn bulk_lookup_is_aligned_and_rejects_conflicts_or_duplicate_inputs() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("sqlite.control"), &identity("node-a")).unwrap();
        let pending = pending();
        let existing = receipt_for("request-a", hash(b"request-a"));
        store.begin_pending(&pending).unwrap();
        store
            .commit_applied(
                &pending,
                identity("node-a").configuration_state(),
                std::slice::from_ref(&existing),
            )
            .unwrap();

        assert_eq!(
            store
                .lookup_requests(&[
                    ("request-a", hash(b"request-a")),
                    ("request-b", hash(b"request-b")),
                ])
                .unwrap(),
            vec![Ok(Some(existing.clone())), Ok(None)]
        );
        assert!(matches!(
            store
                .lookup_requests(&[("request-a", hash(b"different"))])
                .unwrap()
                .pop()
                .unwrap(),
            Err(Error::RequestConflict(_))
        ));
        assert!(matches!(
            store.lookup_requests(&[
                ("request-a", hash(b"request-a")),
                ("request-a", hash(b"request-a")),
            ]),
            Err(Error::InvalidCommand(message)) if message.contains("duplicate")
        ));
    }

    #[test]
    fn capacity_sized_receipt_paths_reject_a_duplicate_at_the_tail() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("sqlite.control"), &identity("node-a")).unwrap();
        let pending = pending();
        let mut receipts = (0usize..super::super::MAX_QWAL_V3_RECEIPTS)
            .map(|index| receipt_for(&format!("request-{index:04}"), hash(&index.to_le_bytes())))
            .collect::<Vec<_>>();
        receipts.last_mut().unwrap().request_id = receipts[0].request_id.clone();
        store.begin_pending(&pending).unwrap();

        assert!(matches!(
            store.commit_applied(
                &pending,
                identity("node-a").configuration_state(),
                &receipts,
            ),
            Err(Error::InvalidEntry(message)) if message.contains("unique")
        ));
        assert_eq!(store.pending().unwrap(), Some(pending));

        let lookups = receipts
            .iter()
            .map(|receipt| (receipt.request_id(), receipt.request_digest()))
            .collect::<Vec<_>>();
        assert!(matches!(
            store.lookup_requests(&lookups),
            Err(Error::InvalidCommand(message)) if message.contains("duplicate")
        ));
    }

    #[test]
    fn batch_receipts_commit_at_one_anchor_or_not_at_all() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("sqlite.control"), &identity("node-a")).unwrap();
        let pending = pending();
        let receipts = [
            receipt_for("request-a", hash(b"request-a")),
            receipt_for("request-b", hash(b"request-b")),
        ];
        store.begin_pending(&pending).unwrap();
        store
            .commit_applied(
                &pending,
                identity("node-a").configuration_state(),
                &receipts,
            )
            .unwrap();
        assert_eq!(store.pending().unwrap(), None);
        for receipt in &receipts {
            assert_eq!(
                store
                    .lookup_request(receipt.request_id(), receipt.request_digest())
                    .unwrap(),
                Some(receipt.clone())
            );
            assert_eq!(receipt.original_anchor(), pending.entry());
        }

        let pending = PendingApply::new(
            pending.entry(),
            LogAnchor::new(2, hash(b"entry-2")),
            pending.target_state(),
            state(b"target-2"),
        );
        let conflicting = [
            RequestReceipt::new(
                "request-c",
                hash(b"request-c"),
                pending.entry(),
                receipts[0].result_blob().to_vec(),
            ),
            RequestReceipt::new(
                "request-a",
                hash(b"different"),
                pending.entry(),
                receipts[0].result_blob().to_vec(),
            ),
        ];
        store.begin_pending(&pending).unwrap();
        assert!(store
            .commit_applied(
                &pending,
                identity("node-a").configuration_state(),
                &conflicting,
            )
            .is_err());
        assert_eq!(store.pending().unwrap(), Some(pending));
        assert_eq!(
            store
                .lookup_request("request-c", hash(b"request-c"))
                .unwrap(),
            None
        );
    }

    #[test]
    fn all_exact_receipts_across_chunks_finish_pending_recovery_without_reinsertion() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("sqlite.control"), &identity("node-a")).unwrap();
        let pending = pending();
        let receipts = (0usize..1024)
            .map(|index| receipt_for(&format!("request-{index:04}"), hash(&index.to_le_bytes())))
            .collect::<Vec<_>>();
        store.begin_pending(&pending).unwrap();
        for receipt in &receipts {
            insert_or_validate_receipt(&store.conn, receipt).unwrap();
        }

        store
            .commit_applied(
                &pending,
                identity("node-a").configuration_state(),
                &receipts,
            )
            .unwrap();

        assert_eq!(store.pending().unwrap(), None);
        assert_eq!(
            store.applied_tip().unwrap(),
            ApplyProgress::new(pending.entry().index(), pending.entry().hash())
        );
        assert_eq!(
            store
                .lookup_requests(
                    &receipts
                        .iter()
                        .map(|receipt| (receipt.request_id(), receipt.request_digest()))
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            receipts
                .into_iter()
                .map(|receipt| Ok(Some(receipt)))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn one_thousand_twenty_four_receipts_share_one_anchor_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        let pending = pending();
        let receipts = (0usize..1024)
            .map(|index| receipt_for(&format!("request-{index:04}"), hash(&index.to_le_bytes())))
            .collect::<Vec<_>>();
        {
            let store = ControlStore::create(&path, &identity("node-a")).unwrap();
            store.begin_pending(&pending).unwrap();
            store
                .commit_applied(
                    &pending,
                    identity("node-a").configuration_state(),
                    &receipts,
                )
                .unwrap();
        }
        let reopened_identity = ControlIdentity::new(
            "cluster",
            "node-a",
            7,
            identity("node-a").configuration_state().clone(),
            11,
            hash(b"fingerprint"),
            pending.target_state(),
        );
        let store = ControlStore::open_existing(&path, &reopened_identity).unwrap();
        let lookups = receipts
            .iter()
            .map(|receipt| (receipt.request_id(), receipt.request_digest()))
            .collect::<Vec<_>>();
        let restored = store.lookup_requests(&lookups).unwrap();
        assert_eq!(restored.len(), 1024);
        assert!(restored.iter().all(|receipt| {
            receipt
                .as_ref()
                .ok()
                .and_then(Option::as_ref)
                .is_some_and(|receipt| receipt.original_anchor() == pending.entry())
        }));
    }

    #[test]
    fn conflict_in_a_later_lookup_chunk_changes_nothing_and_retains_pending() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("sqlite.control"), &identity("node-a")).unwrap();
        let first = pending();
        let existing = receipt_for("request-1000", hash(b"original"));
        store.begin_pending(&first).unwrap();
        store
            .commit_applied(
                &first,
                identity("node-a").configuration_state(),
                std::slice::from_ref(&existing),
            )
            .unwrap();
        let pending = PendingApply::new(
            first.entry(),
            LogAnchor::new(2, hash(b"entry-2")),
            first.target_state(),
            state(b"target-2"),
        );
        let receipts = (0usize..1024)
            .map(|index| {
                RequestReceipt::new(
                    format!("request-{index:04}"),
                    hash(&index.to_le_bytes()),
                    pending.entry(),
                    existing.result_blob().to_vec(),
                )
            })
            .collect::<Vec<_>>();
        store.begin_pending(&pending).unwrap();

        assert!(matches!(
            store.commit_applied(
                &pending,
                identity("node-a").configuration_state(),
                &receipts,
            ),
            Err(Error::RequestConflict(_))
        ));
        assert_eq!(store.pending().unwrap(), Some(pending));
        assert_eq!(
            store
                .lookup_request("request-0000", hash(&0usize.to_le_bytes()))
                .unwrap(),
            None
        );
        assert_eq!(
            store.applied_tip().unwrap(),
            ApplyProgress::new(first.entry().index(), first.entry().hash())
        );
    }

    #[test]
    fn error_in_a_later_insert_chunk_rolls_back_earlier_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("sqlite.control"), &identity("node-a")).unwrap();
        let pending = pending();
        let receipts = (0usize..1024)
            .map(|index| receipt_for(&format!("request-{index:04}"), hash(&index.to_le_bytes())))
            .collect::<Vec<_>>();
        store
            .conn
            .execute_batch(
                "CREATE TRIGGER abort_later_receipt BEFORE INSERT ON request_receipts
                 WHEN NEW.request_id = 'request-0500'
                 BEGIN SELECT RAISE(ABORT, 'later insert failure'); END;",
            )
            .unwrap();
        store.begin_pending(&pending).unwrap();

        assert!(matches!(
            store.commit_applied(
                &pending,
                identity("node-a").configuration_state(),
                &receipts,
            ),
            Err(Error::Sqlite(_))
        ));
        assert_eq!(store.pending().unwrap(), Some(pending));
        assert_eq!(
            store
                .lookup_request("request-0000", hash(&0usize.to_le_bytes()))
                .unwrap(),
            None
        );
        assert_eq!(
            store.applied_tip().unwrap(),
            ApplyProgress::new(0, LogHash::ZERO)
        );
    }

    #[test]
    fn commit_rejects_result_larger_than_inline_qwal_limit_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("sqlite.control"), &identity("node-a")).unwrap();
        let pending = pending();
        let oversized = RequestReceipt::new(
            "request-1",
            hash(b"request"),
            pending.entry(),
            vec![0; MAX_RESULT_BLOB_BYTES + 1],
        );
        store.begin_pending(&pending).unwrap();

        assert!(matches!(
            store.commit_applied(
                &pending,
                identity("node-a").configuration_state(),
                std::slice::from_ref(&oversized),
            ),
            Err(Error::ResourceExhausted(_))
        ));
        assert_eq!(store.pending().unwrap(), Some(pending));
        assert_eq!(
            store.applied_tip().unwrap(),
            ApplyProgress::new(0, LogHash::ZERO)
        );
    }

    #[test]
    fn commit_rejects_noncanonical_sql_result_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("sqlite.control"), &identity("node-a")).unwrap();
        let pending = pending();
        let malformed = RequestReceipt::new(
            "request-1",
            hash(b"request"),
            pending.entry(),
            b"not-qres".to_vec(),
        );
        store.begin_pending(&pending).unwrap();

        assert!(store
            .commit_applied(
                &pending,
                identity("node-a").configuration_state(),
                std::slice::from_ref(&malformed),
            )
            .is_err());
        assert_eq!(store.pending().unwrap(), Some(pending));
        assert_eq!(
            store.applied_tip().unwrap(),
            ApplyProgress::new(0, LogHash::ZERO)
        );
    }

    #[test]
    fn pending_lifecycle_is_idempotent_and_guarded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        let store = ControlStore::create(&path, &identity("node-a")).unwrap();
        let pending = pending();
        store.begin_pending(&pending).unwrap();
        store.begin_pending(&pending).unwrap();
        drop(store);
        let store = ControlStore::open_existing(&path, &identity("node-a")).unwrap();
        assert_eq!(store.pending().unwrap(), Some(pending.clone()));
        let different = PendingApply::new(
            pending.base(),
            LogAnchor::new(1, hash(b"different")),
            pending.base_state(),
            pending.target_state(),
        );
        assert!(store.clear_pending(&different).is_err());
        store.clear_pending(&pending).unwrap();
        store.clear_pending(&pending).unwrap();
        assert_eq!(store.pending().unwrap(), None);
    }

    #[test]
    fn metadata_only_commit_advances_exact_tip_without_pending_and_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        let original = identity("node-a");
        let base = LogAnchor::new(0, LogHash::ZERO);
        let entry = LogAnchor::new(1, hash(b"noop"));
        {
            let store = ControlStore::create(&path, &original).unwrap();
            store
                .commit_metadata_only_entry(
                    base,
                    entry,
                    original.configuration_state(),
                    original.user_state(),
                )
                .unwrap();
            assert_eq!(store.pending().unwrap(), None);
        }

        let store = ControlStore::open_existing_unchecked(&path).unwrap();
        assert_eq!(
            store.applied_tip().unwrap(),
            ApplyProgress::new(entry.index(), entry.hash())
        );
        assert_eq!(
            store.configuration_state().unwrap(),
            *original.configuration_state()
        );
        assert_eq!(store.user_state().unwrap(), original.user_state());
        assert_eq!(store.pending().unwrap(), None);
    }

    #[test]
    fn metadata_only_commit_rejects_an_inexact_base_without_changing_state() {
        let dir = tempfile::tempdir().unwrap();
        let original = identity("node-a");
        let store = ControlStore::create(dir.path().join("sqlite.control"), &original).unwrap();

        assert!(matches!(
            store.commit_metadata_only_entry(
                LogAnchor::new(0, hash(b"wrong-base")),
                LogAnchor::new(1, hash(b"noop")),
                original.configuration_state(),
                original.user_state(),
            ),
            Err(Error::InvalidEntry(_))
        ));
        assert_eq!(
            store.applied_tip().unwrap(),
            ApplyProgress::new(0, LogHash::ZERO)
        );
        assert_eq!(store.pending().unwrap(), None);
    }

    #[test]
    fn metadata_only_commit_does_not_bypass_legacy_pending_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let original = identity("node-a");
        let store = ControlStore::create(dir.path().join("sqlite.control"), &original).unwrap();
        let pending = pending();
        store.begin_pending(&pending).unwrap();

        assert!(matches!(
            store.commit_metadata_only_entry(
                pending.base(),
                pending.entry(),
                original.configuration_state(),
                original.user_state(),
            ),
            Err(Error::InvalidEntry(_))
        ));
        assert_eq!(store.pending().unwrap(), Some(pending));
        assert_eq!(
            store.applied_tip().unwrap(),
            ApplyProgress::new(0, LogHash::ZERO)
        );
    }

    #[test]
    fn committed_state_and_receipt_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        let original = identity("node-a");
        let pending = pending();
        let receipt = receipt(hash(b"request"));
        let next_config = ConfigurationState::active(4, hash(b"next-config"));
        {
            let store = ControlStore::create(&path, &original).unwrap();
            store.begin_pending(&pending).unwrap();
            store
                .commit_applied(&pending, &next_config, std::slice::from_ref(&receipt))
                .unwrap();
        }
        let reopened_identity = ControlIdentity::new(
            "cluster",
            "node-a",
            7,
            next_config.clone(),
            11,
            hash(b"fingerprint"),
            state(b"target-db"),
        );
        let store = ControlStore::open_existing(&path, &reopened_identity).unwrap();
        assert_eq!(
            store.applied_tip().unwrap(),
            ApplyProgress::new(1, hash(b"entry"))
        );
        assert_eq!(store.configuration_state().unwrap(), next_config);
        assert_eq!(
            store.lookup_request("request-1", hash(b"request")).unwrap(),
            Some(receipt)
        );
    }

    #[test]
    fn snapshot_import_keeps_destination_node_and_excludes_pending() {
        let dir = tempfile::tempdir().unwrap();
        let source =
            ControlStore::create(dir.path().join("source.control"), &identity("node-a")).unwrap();
        let source_pending = pending();
        source.begin_pending(&source_pending).unwrap();
        source
            .commit_applied(
                &source_pending,
                identity("node-a").configuration_state(),
                std::slice::from_ref(&receipt(hash(b"request"))),
            )
            .unwrap();
        let snapshot = source.export_replicated_snapshot().unwrap();

        let destination =
            ControlStore::create(dir.path().join("destination.control"), &identity("node-b"))
                .unwrap();
        destination.begin_pending(&pending()).unwrap();
        destination
            .import_replicated_snapshot_with_recovery_generation(&snapshot, Some(42))
            .unwrap();
        assert_eq!(destination.identity().unwrap().node_id(), "node-b");
        assert_eq!(destination.recovery_generation().unwrap(), 42);
        assert_eq!(
            destination.user_state().unwrap(),
            source_pending.target_state()
        );
        assert_eq!(
            destination.applied_tip().unwrap(),
            ApplyProgress::new(1, hash(b"entry"))
        );
        assert_eq!(destination.pending().unwrap(), None);
    }

    #[test]
    fn snapshot_import_rejects_result_larger_than_inline_qwal_limit() {
        let dir = tempfile::tempdir().unwrap();
        let destination =
            ControlStore::create(dir.path().join("destination.control"), &identity("node-b"))
                .unwrap();
        let snapshot = ReplicatedSnapshot {
            cluster_id: "cluster".into(),
            epoch: 7,
            configuration_state: ConfigurationState::active(3, hash(b"config")),
            recovery_generation: 11,
            materializer_fingerprint: hash(b"fingerprint"),
            user_state: state(b"base-db"),
            applied_tip: LogAnchor::new(1, hash(b"entry")),
            receipts: vec![RequestReceipt::new(
                "oversized",
                hash(b"request"),
                LogAnchor::new(1, hash(b"entry")),
                vec![0; MAX_RESULT_BLOB_BYTES + 1],
            )],
        };
        let encoded = encode_snapshot(&snapshot).unwrap();

        assert!(matches!(
            destination.import_replicated_snapshot(&encoded),
            Err(Error::ResourceExhausted(_))
        ));
        assert_eq!(
            destination.applied_tip().unwrap(),
            ApplyProgress::new(0, LogHash::ZERO)
        );
    }
}
