//! A bounded, deterministic key/value materializer backed by redb.
//!
//! The replicated surface is deliberately semantic: callers may submit only
//! versioned put/delete commands. Arbitrary redb transactions are not exposed.

use std::collections::BTreeSet;
use std::fmt;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::ops::Bound;
use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use rhiza_core::{
    ConfigChange, EntryType, ExecutionProfile, LogAnchor, LogEntry, LogHash,
    ReplicatedCommandEnvelope,
};
use tempfile::NamedTempFile;

const COMMAND_MAGIC: &[u8; 6] = b"RHKV\0\x01";
const BATCH_COMMAND_MAGIC: &[u8; 6] = b"RHKB\0\x01";
const RECEIPT_MAGIC: &[u8; 6] = b"RHKR\0\x01";
const SNAPSHOT_DOMAIN: &[u8] = b"rhiza-kv-snapshot-v1\0";
const SNAPSHOT_WIRE_MAGIC: &[u8; 4] = b"RHKS";
const SNAPSHOT_WIRE_VERSION: u16 = 1;
const MATERIALIZER_DOMAIN: &[u8] = b"rhiza-kv-materializer-v3\0";
const COMMAND_VERSION: u16 = 1;
const BATCH_COMMAND_VERSION: u16 = 3;
const BATCH_REQUEST_ID: &str = "__rhiza_kv_batch_v1";

const DATA_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__rhiza_kv_data_v1");
const REQUEST_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__rhiza_kv_requests_v1");
const PROGRESS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__rhiza_kv_progress_v1");
const EMBEDDED_LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("__rhiza_kv_qlog_v1");

const META_CLUSTER_ID: &str = "cluster_id";
const META_NODE_ID: &str = "node_id";
const META_EPOCH: &str = "epoch";
const META_CONFIG_ID: &str = "config_id";
const META_APPLIED_INDEX: &str = "applied_index";
const META_APPLIED_HASH: &str = "applied_hash";
const META_MATERIALIZER_FINGERPRINT: &str = "materializer_fingerprint";

/// Maximum accepted request-id size in bytes.
pub const MAX_REQUEST_ID_BYTES: usize = 256;
/// Maximum accepted key size in bytes.
pub const MAX_KV_KEY_BYTES: usize = 4 * 1024;
/// Maximum accepted value size in bytes.
pub const MAX_KV_VALUE_BYTES: usize = 256 * 1024;
/// Maximum commands accepted by one public typed KV batch.
pub const MAX_KV_BATCH_MEMBERS: usize = 256;
/// Maximum commands carried by one internal replicated KV batch.
const MAX_REPLICATED_KV_BATCH_MEMBERS: usize = 1024;
/// Maximum rows returned by one ordered scan.
pub const MAX_KV_SCAN_ROWS: usize = 1024;
/// Maximum combined key and value bytes returned by one ordered scan.
pub const MAX_KV_SCAN_RESULT_BYTES: usize = 1024 * 1024;
const _: () = assert!(MAX_KV_SCAN_RESULT_BYTES >= MAX_KV_KEY_BYTES + MAX_KV_VALUE_BYTES);

/// Stable compatibility identity for redb bytes and deterministic KV semantics.
pub fn kv_materializer_fingerprint() -> LogHash {
    LogHash::digest(&[
        MATERIALIZER_DOMAIN,
        b"redb=4.1.0",
        b"schema=2;embedded_qlog=qlog_segment_v3",
        COMMAND_MAGIC,
        &COMMAND_VERSION.to_be_bytes(),
        BATCH_COMMAND_MAGIC,
        &BATCH_COMMAND_VERSION.to_be_bytes(),
    ])
}

/// Errors returned by the bounded KV codec or materializer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    Codec(String),
    InvalidCommand(String),
    InvalidQuery(String),
    InvalidEntry(String),
    PartialInitialization,
    RequestConflict { request_id: String },
    ResourceExhausted(String),
    Database(String),
    Io(String),
    InvalidSnapshot(String),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(message) => write!(formatter, "KV codec error: {message}"),
            Self::InvalidCommand(message) => write!(formatter, "invalid KV command: {message}"),
            Self::InvalidQuery(message) => write!(formatter, "invalid KV query: {message}"),
            Self::InvalidEntry(message) => write!(formatter, "invalid log entry: {message}"),
            Self::PartialInitialization => {
                formatter.write_str("partial or corrupt KV initialization")
            }
            Self::RequestConflict { request_id } => {
                write!(
                    formatter,
                    "request id {request_id:?} was reused with another command"
                )
            }
            Self::ResourceExhausted(message) => {
                write!(formatter, "KV query resources exhausted: {message}")
            }
            Self::Database(message) => write!(formatter, "redb error: {message}"),
            Self::Io(message) => write!(formatter, "KV snapshot I/O failed: {message}"),
            Self::InvalidSnapshot(message) => write!(formatter, "invalid KV snapshot: {message}"),
        }
    }
}

impl std::error::Error for Error {}

fn database_error(error: impl fmt::Display) -> Error {
    Error::Database(error.to_string())
}

fn io_error(error: impl fmt::Display) -> Error {
    Error::Io(error.to_string())
}

fn decode_embedded_log_entry(encoded: &[u8], cluster_id: &str) -> Result<LogEntry, Error> {
    let entries = rhiza_log::decode_segment_for_cluster(encoded, cluster_id)
        .map_err(|error| Error::InvalidEntry(error.to_string()))?;
    let [entry] = entries.as_slice() else {
        return Err(Error::InvalidEntry(
            "embedded qlog value must contain exactly one entry".into(),
        ));
    };
    Ok(entry.clone())
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum KvOperationV1 {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Stable `RHKV v1` semantic command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvCommandV1 {
    request_id: String,
    operation: KvOperationV1,
}

impl KvCommandV1 {
    pub fn put(request_id: impl Into<String>, key: Vec<u8>, value: Vec<u8>) -> Result<Self, Error> {
        let command = Self {
            request_id: request_id.into(),
            operation: KvOperationV1::Put { key, value },
        };
        command.validate()?;
        Ok(command)
    }

    pub fn delete(request_id: impl Into<String>, key: Vec<u8>) -> Result<Self, Error> {
        let command = Self {
            request_id: request_id.into(),
            operation: KvOperationV1::Delete { key },
        };
        command.validate()?;
        Ok(command)
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn encode(&self) -> std::result::Result<Vec<u8>, Error> {
        self.validate()?;
        let (key, value_len) = match &self.operation {
            KvOperationV1::Put { key, value } => (key, Some(value.len())),
            KvOperationV1::Delete { key } => (key, None),
        };
        let mut encoded = Vec::with_capacity(
            COMMAND_MAGIC.len()
                + 2
                + self.request_id.len()
                + 1
                + 4
                + key.len()
                + value_len.map_or(0, |length| 4 + length),
        );
        encoded.extend_from_slice(COMMAND_MAGIC);
        encoded.extend_from_slice(&(self.request_id.len() as u16).to_be_bytes());
        encoded.extend_from_slice(self.request_id.as_bytes());
        match &self.operation {
            KvOperationV1::Put { key, value } => {
                encoded.push(1);
                encoded.extend_from_slice(&(key.len() as u32).to_be_bytes());
                encoded.extend_from_slice(key);
                encoded.extend_from_slice(&(value.len() as u32).to_be_bytes());
                encoded.extend_from_slice(value);
            }
            KvOperationV1::Delete { key } => {
                encoded.push(2);
                encoded.extend_from_slice(&(key.len() as u32).to_be_bytes());
                encoded.extend_from_slice(key);
            }
        }
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, Error> {
        let mut decoder = Decoder::new(encoded);
        if decoder.take(COMMAND_MAGIC.len())? != COMMAND_MAGIC {
            return Err(Error::Codec("invalid RHKV magic or version".into()));
        }
        let request_len = usize::from(decoder.u16()?);
        let request_bytes = decoder.take(request_len)?;
        let request_id = std::str::from_utf8(request_bytes)
            .map_err(|_| Error::Codec("request id is not UTF-8".into()))?
            .to_owned();
        let operation = match decoder.u8()? {
            1 => {
                let key = decoder.length_prefixed_u32(MAX_KV_KEY_BYTES, "key")?;
                let value = decoder.length_prefixed_u32(MAX_KV_VALUE_BYTES, "value")?;
                KvOperationV1::Put { key, value }
            }
            2 => {
                let key = decoder.length_prefixed_u32(MAX_KV_KEY_BYTES, "key")?;
                KvOperationV1::Delete { key }
            }
            tag => return Err(Error::Codec(format!("unknown operation tag {tag}"))),
        };
        decoder.finish()?;
        let command = Self {
            request_id,
            operation,
        };
        command.validate()?;
        Ok(command)
    }

    fn validate(&self) -> Result<(), Error> {
        if self.request_id.is_empty() {
            return Err(Error::InvalidCommand("request id must not be empty".into()));
        }
        if self.request_id.len() > MAX_REQUEST_ID_BYTES {
            return Err(Error::InvalidCommand(format!(
                "request id exceeds {MAX_REQUEST_ID_BYTES} bytes"
            )));
        }
        let (key, value) = match &self.operation {
            KvOperationV1::Put { key, value } => (key.as_slice(), Some(value.as_slice())),
            KvOperationV1::Delete { key } => (key.as_slice(), None),
        };
        validate_key(key)?;
        if value.is_some_and(|value| value.len() > MAX_KV_VALUE_BYTES) {
            return Err(Error::InvalidCommand(format!(
                "value exceeds {MAX_KV_VALUE_BYTES} bytes"
            )));
        }
        Ok(())
    }
}

fn validate_key(key: &[u8]) -> Result<(), Error> {
    if key.is_empty() {
        return Err(Error::InvalidCommand("key must not be empty".into()));
    }
    if key.len() > MAX_KV_KEY_BYTES {
        return Err(Error::InvalidCommand(format!(
            "key exceeds {MAX_KV_KEY_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_scan_bound(name: &str, value: &[u8]) -> Result<(), Error> {
    if value.len() > MAX_KV_KEY_BYTES {
        return Err(Error::InvalidQuery(format!(
            "{name} exceeds {MAX_KV_KEY_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_scan_cursor(cursor: Option<&[u8]>) -> Result<(), Error> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    if cursor.is_empty() {
        return Err(Error::InvalidQuery("scan cursor must not be empty".into()));
    }
    validate_scan_bound("scan cursor", cursor)
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    let index = successor.iter().rposition(|byte| *byte != u8::MAX)?;
    successor[index] += 1;
    successor.truncate(index + 1);
    Some(successor)
}

/// Encodes a command inside the shared `QCMD` envelope for the KV profile.
pub fn encode_replicated_kv_command(command: &KvCommandV1) -> Result<Vec<u8>, Error> {
    command.validate()?;
    ReplicatedCommandEnvelope::new(
        ExecutionProfile::Kv,
        COMMAND_VERSION,
        command.request_id(),
        command.encode()?,
    )
    .and_then(|envelope| envelope.encode())
    .map_err(|error| Error::Codec(error.to_string()))
}

/// Encodes ordered individual KV commands as one canonical replicated batch.
///
/// Internal group commit may combine up to 1,024 commands. Public typed APIs must continue to
/// enforce [`MAX_KV_BATCH_MEMBERS`] before calling this replication codec.
pub fn encode_replicated_kv_batch(commands: &[KvCommandV1]) -> Result<Vec<u8>, Error> {
    if commands.is_empty() || commands.len() > MAX_REPLICATED_KV_BATCH_MEMBERS {
        return Err(Error::InvalidCommand(format!(
            "replicated KV batch must contain 1..={MAX_REPLICATED_KV_BATCH_MEMBERS} commands"
        )));
    }
    let mut request_ids = BTreeSet::new();
    let mut body = Vec::from(BATCH_COMMAND_MAGIC.as_slice());
    body.extend_from_slice(&(commands.len() as u16).to_be_bytes());
    for command in commands {
        command.validate()?;
        if !request_ids.insert(command.request_id()) {
            return Err(Error::InvalidCommand(format!(
                "KV batch repeats request id {:?}",
                command.request_id()
            )));
        }
        let encoded = command.encode()?;
        body.extend_from_slice(
            &u32::try_from(encoded.len())
                .map_err(|_| Error::Codec("KV batch member is too large".into()))?
                .to_be_bytes(),
        );
        body.extend_from_slice(&encoded);
    }
    ReplicatedCommandEnvelope::new(
        ExecutionProfile::Kv,
        BATCH_COMMAND_VERSION,
        BATCH_REQUEST_ID,
        body,
    )
    .and_then(|envelope| envelope.encode())
    .map_err(|error| Error::Codec(error.to_string()))
}

fn decode_replicated_kv_command(payload: &[u8]) -> Result<KvCommandV1, Error> {
    let envelope = ReplicatedCommandEnvelope::decode(payload)
        .map_err(|error| Error::Codec(error.to_string()))?;
    if envelope.profile() != ExecutionProfile::Kv {
        return Err(Error::InvalidCommand(format!(
            "expected kv profile, got {}",
            envelope.profile()
        )));
    }
    if envelope.command_version() != COMMAND_VERSION {
        return Err(Error::InvalidCommand(format!(
            "unsupported command version {}",
            envelope.command_version()
        )));
    }
    let command = KvCommandV1::decode(envelope.body())?;
    if command.request_id() != envelope.request_id() {
        return Err(Error::InvalidCommand(
            "envelope and body request ids differ".into(),
        ));
    }
    Ok(command)
}

struct DecodedKvCommand {
    command: KvCommandV1,
    individual_payload: Vec<u8>,
}

fn decode_replicated_kv_commands(payload: &[u8]) -> Result<Vec<DecodedKvCommand>, Error> {
    let envelope = ReplicatedCommandEnvelope::decode(payload)
        .map_err(|error| Error::Codec(error.to_string()))?;
    if envelope.profile() != ExecutionProfile::Kv {
        return Err(Error::InvalidCommand(format!(
            "expected kv profile, got {}",
            envelope.profile()
        )));
    }
    match envelope.command_version() {
        COMMAND_VERSION => {
            let command = decode_replicated_kv_command(payload)?;
            Ok(vec![DecodedKvCommand {
                command,
                individual_payload: payload.to_vec(),
            }])
        }
        BATCH_COMMAND_VERSION => {
            if envelope.request_id() != BATCH_REQUEST_ID {
                return Err(Error::InvalidCommand(
                    "KV batch envelope request id is invalid".into(),
                ));
            }
            let mut decoder = Decoder::new(envelope.body());
            if decoder.take(BATCH_COMMAND_MAGIC.len())? != BATCH_COMMAND_MAGIC {
                return Err(Error::Codec("invalid KV batch magic or version".into()));
            }
            let count = usize::from(decoder.u16()?);
            if count == 0 || count > MAX_REPLICATED_KV_BATCH_MEMBERS {
                return Err(Error::InvalidCommand(format!(
                    "replicated KV batch must contain 1..={MAX_REPLICATED_KV_BATCH_MEMBERS} commands"
                )));
            }
            let mut request_ids = BTreeSet::new();
            let mut commands = Vec::with_capacity(count);
            for _ in 0..count {
                let length = usize::try_from(decoder.u32()?)
                    .map_err(|_| Error::Codec("KV batch member length overflow".into()))?;
                let encoded = decoder.take(length)?;
                let command = KvCommandV1::decode(encoded)?;
                if command.encode()? != encoded {
                    return Err(Error::Codec("noncanonical KV batch member".into()));
                }
                if !request_ids.insert(command.request_id().to_owned()) {
                    return Err(Error::InvalidCommand(format!(
                        "KV batch repeats request id {:?}",
                        command.request_id()
                    )));
                }
                let individual_payload = encode_replicated_kv_command(&command)?;
                commands.push(DecodedKvCommand {
                    command,
                    individual_payload,
                });
            }
            decoder.finish()?;
            let canonical = commands
                .iter()
                .map(|member| member.command.clone())
                .collect::<Vec<_>>();
            if encode_replicated_kv_batch(&canonical)? != payload {
                return Err(Error::Codec("noncanonical KV batch command".into()));
            }
            Ok(commands)
        }
        version => Err(Error::InvalidCommand(format!(
            "unsupported command version {version}"
        ))),
    }
}

/// Observable result of a semantic KV command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvCommandResultV1 {
    Put { replaced: bool },
    Delete { existed: bool },
}

/// Durable idempotency record for one request id.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvRequestRecord {
    payload_hash: LogHash,
    original_log_index: u64,
    original_log_hash: LogHash,
    result: KvCommandResultV1,
}

impl KvRequestRecord {
    pub const fn original_log_index(&self) -> u64 {
        self.original_log_index
    }

    pub const fn original_log_hash(&self) -> LogHash {
        self.original_log_hash
    }

    pub const fn result(&self) -> &KvCommandResultV1 {
        &self.result
    }

    fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(80);
        encoded.extend_from_slice(RECEIPT_MAGIC);
        encoded.extend_from_slice(self.payload_hash.as_bytes());
        encoded.extend_from_slice(&self.original_log_index.to_be_bytes());
        encoded.extend_from_slice(self.original_log_hash.as_bytes());
        match self.result {
            KvCommandResultV1::Put { replaced } => {
                encoded.push(1);
                encoded.push(u8::from(replaced));
            }
            KvCommandResultV1::Delete { existed } => {
                encoded.push(2);
                encoded.push(u8::from(existed));
            }
        }
        encoded
    }

    fn decode(encoded: &[u8]) -> Result<Self, Error> {
        let mut decoder = Decoder::new(encoded);
        if decoder.take(RECEIPT_MAGIC.len())? != RECEIPT_MAGIC {
            return Err(Error::Codec(
                "invalid request receipt magic or version".into(),
            ));
        }
        let payload_hash = LogHash::from_bytes(decoder.array_32()?);
        let original_log_index = decoder.u64()?;
        let original_log_hash = LogHash::from_bytes(decoder.array_32()?);
        let tag = decoder.u8()?;
        let flag = match decoder.u8()? {
            0 => false,
            1 => true,
            value => return Err(Error::Codec(format!("invalid receipt boolean {value}"))),
        };
        let result = match tag {
            1 => KvCommandResultV1::Put { replaced: flag },
            2 => KvCommandResultV1::Delete { existed: flag },
            value => return Err(Error::Codec(format!("invalid receipt result tag {value}"))),
        };
        decoder.finish()?;
        Ok(Self {
            payload_hash,
            original_log_index,
            original_log_hash,
            result,
        })
    }
}

/// Result of applying one committed qlog entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyOutcome {
    applied_index: u64,
    applied_hash: LogHash,
    result: Option<KvCommandResultV1>,
}

/// Applied qlog tip observed by a point read or scan snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KvReadTip {
    applied_index: u64,
    applied_hash: LogHash,
}

impl KvReadTip {
    pub const fn applied_index(self) -> u64 {
        self.applied_index
    }

    pub const fn applied_hash(self) -> LogHash {
        self.applied_hash
    }
}

/// Exact-key result and the applied tip from the same redb read transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvGetResult {
    value: Option<Vec<u8>>,
    tip: KvReadTip,
}

impl KvGetResult {
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    pub const fn tip(&self) -> KvReadTip {
        self.tip
    }

    pub fn into_parts(self) -> (Option<Vec<u8>>, KvReadTip) {
        (self.value, self.tip)
    }
}

/// One copied row from a deterministic byte-ordered scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvScanRow {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl KvScanRow {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

/// Bounded ordered rows and the applied tip from one redb read transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvScanResult {
    rows: Vec<KvScanRow>,
    next_cursor: Option<Vec<u8>>,
    tip: KvReadTip,
}

impl KvScanResult {
    pub fn rows(&self) -> &[KvScanRow] {
        &self.rows
    }

    pub fn next_cursor(&self) -> Option<&[u8]> {
        self.next_cursor.as_deref()
    }

    pub const fn tip(&self) -> KvReadTip {
        self.tip
    }
}

/// A self-verifying point-in-time image of the authoritative redb materializer.
#[derive(Debug, Eq, PartialEq)]
pub struct RedbSnapshot {
    cluster_id: String,
    created_by: String,
    epoch: u64,
    config_id: u64,
    applied_index: u64,
    applied_hash: LogHash,
    materializer_fingerprint: LogHash,
    digest: LogHash,
    db_bytes: Vec<u8>,
}

impl RedbSnapshot {
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn created_by(&self) -> &str {
        &self.created_by
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn config_id(&self) -> u64 {
        self.config_id
    }

    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub const fn applied_hash(&self) -> LogHash {
        self.applied_hash
    }

    pub const fn materializer_fingerprint(&self) -> LogHash {
        self.materializer_fingerprint
    }

    pub const fn digest(&self) -> LogHash {
        self.digest
    }

    pub fn db_bytes(&self) -> &[u8] {
        &self.db_bytes
    }

    fn recompute_digest(&self) -> LogHash {
        let cluster_id = length_prefixed(self.cluster_id.as_bytes());
        let created_by = length_prefixed(self.created_by.as_bytes());
        let database_length = u64::try_from(self.db_bytes.len()).expect("usize fits in u64");
        LogHash::digest(&[
            SNAPSHOT_DOMAIN,
            &cluster_id,
            &created_by,
            &self.epoch.to_be_bytes(),
            &self.config_id.to_be_bytes(),
            &self.applied_index.to_be_bytes(),
            self.applied_hash.as_bytes(),
            self.materializer_fingerprint.as_bytes(),
            &database_length.to_be_bytes(),
            &self.db_bytes,
        ])
    }
}

/// Encodes a complete redb snapshot as one canonical, versioned archive object.
pub fn encode_snapshot(snapshot: &RedbSnapshot) -> Result<Vec<u8>, Error> {
    validate_snapshot_envelope(snapshot)?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(SNAPSHOT_WIRE_MAGIC);
    encoded.extend_from_slice(&SNAPSHOT_WIRE_VERSION.to_be_bytes());
    encode_snapshot_bytes(&mut encoded, snapshot.cluster_id.as_bytes());
    encode_snapshot_bytes(&mut encoded, snapshot.created_by.as_bytes());
    encoded.extend_from_slice(&snapshot.epoch.to_be_bytes());
    encoded.extend_from_slice(&snapshot.config_id.to_be_bytes());
    encoded.extend_from_slice(&snapshot.applied_index.to_be_bytes());
    encoded.extend_from_slice(snapshot.applied_hash.as_bytes());
    encoded.extend_from_slice(snapshot.materializer_fingerprint.as_bytes());
    encoded.extend_from_slice(snapshot.digest.as_bytes());
    encode_snapshot_bytes(&mut encoded, &snapshot.db_bytes);
    Ok(encoded)
}

/// Decodes and verifies a canonical redb snapshot archive object.
pub fn decode_snapshot(encoded: &[u8]) -> Result<RedbSnapshot, Error> {
    let mut decoder = SnapshotDecoder::new(encoded);
    if decoder.take(SNAPSHOT_WIRE_MAGIC.len())? != SNAPSHOT_WIRE_MAGIC {
        return Err(Error::InvalidSnapshot(
            "snapshot envelope magic does not match RHKS".into(),
        ));
    }
    let version = decoder.u16()?;
    if version != SNAPSHOT_WIRE_VERSION {
        return Err(Error::InvalidSnapshot(format!(
            "unsupported snapshot envelope version {version}"
        )));
    }
    let snapshot = RedbSnapshot {
        cluster_id: decoder.string()?,
        created_by: decoder.string()?,
        epoch: decoder.u64()?,
        config_id: decoder.u64()?,
        applied_index: decoder.u64()?,
        applied_hash: LogHash::from_bytes(decoder.array()?),
        materializer_fingerprint: LogHash::from_bytes(decoder.array()?),
        digest: LogHash::from_bytes(decoder.array()?),
        db_bytes: decoder.bytes()?.to_vec(),
    };
    if !decoder.is_empty() {
        return Err(Error::InvalidSnapshot(
            "snapshot envelope has trailing bytes".into(),
        ));
    }
    validate_snapshot_envelope(&snapshot)?;
    Ok(snapshot)
}

fn validate_snapshot_envelope(snapshot: &RedbSnapshot) -> Result<(), Error> {
    if snapshot.cluster_id.is_empty() || snapshot.created_by.is_empty() {
        return Err(Error::InvalidSnapshot(
            "snapshot identity contains an empty cluster or source node".into(),
        ));
    }
    if snapshot.materializer_fingerprint != kv_materializer_fingerprint() {
        return Err(Error::InvalidSnapshot(
            "materializer fingerprint does not match this binary".into(),
        ));
    }
    if snapshot.recompute_digest() != snapshot.digest {
        return Err(Error::InvalidSnapshot(
            "snapshot digest does not match its contents".into(),
        ));
    }
    Ok(())
}

fn encode_snapshot_bytes(encoded: &mut Vec<u8>, value: &[u8]) {
    encoded.extend_from_slice(
        &u64::try_from(value.len())
            .expect("usize fits in u64")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(value);
}

struct SnapshotDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SnapshotDecoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], Error> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| Error::InvalidSnapshot("snapshot envelope length overflow".into()))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| Error::InvalidSnapshot("snapshot envelope is truncated".into()))?;
        self.offset = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        Ok(self.take(N)?.try_into().expect("length checked"))
    }

    fn u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn bytes(&mut self) -> Result<&'a [u8], Error> {
        let length = usize::try_from(self.u64()?).map_err(|_| {
            Error::InvalidSnapshot("snapshot envelope length exceeds this platform".into())
        })?;
        self.take(length)
    }

    fn string(&mut self) -> Result<String, Error> {
        String::from_utf8(self.bytes()?.to_vec())
            .map_err(|_| Error::InvalidSnapshot("snapshot identity is not valid UTF-8".into()))
    }

    const fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

impl ApplyOutcome {
    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub const fn applied_hash(&self) -> LogHash {
        self.applied_hash
    }

    pub const fn result(&self) -> Option<&KvCommandResultV1> {
        self.result.as_ref()
    }
}

/// redb-backed, continuity-checking materialized state machine.
pub struct RedbStateMachine {
    database: Database,
    cluster_id: String,
    node_id: String,
    epoch: u64,
    config_id: u64,
}

impl RedbStateMachine {
    #[tracing::instrument(level = "info", skip_all, fields(epoch, config_id))]
    pub fn open(
        path: impl AsRef<Path>,
        cluster_id: impl Into<String>,
        node_id: impl Into<String>,
        epoch: u64,
        config_id: u64,
    ) -> Result<Self, Error> {
        let cluster_id = cluster_id.into();
        let node_id = node_id.into();
        if cluster_id.is_empty() || node_id.is_empty() {
            return Err(Error::InvalidEntry(
                "cluster and node ids must not be empty".into(),
            ));
        }
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent).map_err(database_error)?;
        }
        let database = Database::create(path).map_err(database_error)?;
        initialize_or_validate(&database, &cluster_id, &node_id, epoch, config_id)?;
        tracing::info!(cluster_id, node_id, epoch, config_id, "kv state machine opened");
        Ok(Self {
            database,
            cluster_id,
            node_id,
            epoch,
            config_id,
        })
    }

    /// Returns a copied value. No raw transaction or iterator is exposed.
    #[tracing::instrument(skip(self), fields(key_len = key.len()))]
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.get_with_tip(key)?.value)
    }

    /// Returns a copied value and applied tip from exactly one redb read transaction.
    #[tracing::instrument(skip(self), fields(key_len = key.len()))]
    pub fn get_with_tip(&self, key: &[u8]) -> Result<KvGetResult, Error> {
        validate_key(key)?;
        let read = self.database.begin_read().map_err(database_error)?;
        let data = read.open_table(DATA_TABLE).map_err(database_error)?;
        let progress = read.open_table(PROGRESS_TABLE).map_err(database_error)?;
        let value = data
            .get(key)
            .map_err(database_error)?
            .map(|value| value.value().to_vec());
        let tip = read_tip(&progress)?;
        Ok(KvGetResult { value, tip })
    }

    /// Scans keys from an inclusive start to an optional exclusive end.
    ///
    /// A cursor is the last key returned by the preceding page and is excluded
    /// from this page. Limits must be within `1..=MAX_KV_SCAN_ROWS`.
    pub fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        cursor: Option<&[u8]>,
    ) -> Result<KvScanResult, Error> {
        validate_scan_bound("range start", start)?;
        if let Some(end) = end {
            validate_scan_bound("range end", end)?;
        }
        validate_scan_cursor(cursor)?;
        if cursor.is_some_and(|cursor| cursor < start || end.is_some_and(|end| cursor >= end)) {
            return Err(Error::InvalidQuery(
                "cursor is outside the requested range".into(),
            ));
        }
        self.scan_snapshot(start, end, limit, cursor)
    }

    /// Scans all keys with a byte prefix in deterministic ascending order.
    pub fn scan_prefix(
        &self,
        prefix: &[u8],
        limit: usize,
        cursor: Option<&[u8]>,
    ) -> Result<KvScanResult, Error> {
        validate_scan_bound("prefix", prefix)?;
        validate_scan_cursor(cursor)?;
        if cursor.is_some_and(|cursor| !cursor.starts_with(prefix)) {
            return Err(Error::InvalidQuery(
                "cursor does not belong to the requested prefix".into(),
            ));
        }
        let end = prefix_successor(prefix);
        self.scan_snapshot(prefix, end.as_deref(), limit, cursor)
    }

    fn scan_snapshot(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        cursor: Option<&[u8]>,
    ) -> Result<KvScanResult, Error> {
        if !(1..=MAX_KV_SCAN_ROWS).contains(&limit) {
            return Err(Error::InvalidQuery(format!(
                "scan limit must be within 1..={MAX_KV_SCAN_ROWS}"
            )));
        }
        let row_limit = limit;
        let read = self.database.begin_read().map_err(database_error)?;
        let data = read.open_table(DATA_TABLE).map_err(database_error)?;
        let progress = read.open_table(PROGRESS_TABLE).map_err(database_error)?;
        let tip = read_tip(&progress)?;
        if end.is_some_and(|end| start >= end) {
            return Ok(KvScanResult {
                rows: Vec::new(),
                next_cursor: None,
                tip,
            });
        }
        let lower = cursor.map_or(Bound::Included(start), Bound::Excluded);
        let upper = end.map_or(Bound::Unbounded, Bound::Excluded);
        let mut rows = Vec::with_capacity(row_limit);
        let mut result_bytes = 0_usize;
        let mut has_more = false;
        for row in data
            .range::<&[u8]>((lower, upper))
            .map_err(database_error)?
        {
            let (key, value) = row.map_err(database_error)?;
            let row_bytes = key
                .value()
                .len()
                .checked_add(value.value().len())
                .ok_or_else(|| Error::ResourceExhausted("KV scan row size overflow".into()))?;
            let next_result_bytes = result_bytes
                .checked_add(row_bytes)
                .ok_or_else(|| Error::ResourceExhausted("KV scan result size overflow".into()))?;
            if rows.len() == row_limit || next_result_bytes > MAX_KV_SCAN_RESULT_BYTES {
                has_more = true;
                break;
            }
            result_bytes = next_result_bytes;
            rows.push(KvScanRow::new(key.value().to_vec(), value.value().to_vec()));
        }
        if has_more && rows.is_empty() {
            return Err(Error::ResourceExhausted(
                "KV row exceeds the maximum scan result size".into(),
            ));
        }
        let next_cursor = has_more.then(|| rows.last().expect("non-empty page").key.clone());
        Ok(KvScanResult {
            rows,
            next_cursor,
            tip,
        })
    }

    pub fn applied_index(&self) -> Result<u64, Error> {
        let read = self.database.begin_read().map_err(database_error)?;
        let table = read.open_table(PROGRESS_TABLE).map_err(database_error)?;
        read_u64_meta(&table, META_APPLIED_INDEX)
    }

    pub fn applied_hash(&self) -> Result<LogHash, Error> {
        let read = self.database.begin_read().map_err(database_error)?;
        let table = read.open_table(PROGRESS_TABLE).map_err(database_error)?;
        read_hash_meta(&table, META_APPLIED_HASH)
    }

    /// Returns the applied index and hash observed by one redb read transaction.
    pub fn applied_tip(&self) -> Result<LogAnchor, Error> {
        let read = self.database.begin_read().map_err(database_error)?;
        let table = read.open_table(PROGRESS_TABLE).map_err(database_error)?;
        let tip = read_tip(&table)?;
        Ok(LogAnchor::new(tip.applied_index(), tip.applied_hash()))
    }

    /// Builds a consistent online snapshot by logically copying one redb read transaction.
    pub fn create_snapshot(&self, target_index: u64) -> Result<RedbSnapshot, Error> {
        let read = self.database.begin_read().map_err(database_error)?;
        let data = read.open_table(DATA_TABLE).map_err(database_error)?;
        let requests = read.open_table(REQUEST_TABLE).map_err(database_error)?;
        let progress = read.open_table(PROGRESS_TABLE).map_err(database_error)?;
        validate_snapshot_identity(
            &progress,
            &self.cluster_id,
            &self.node_id,
            self.epoch,
            self.config_id,
        )?;
        let applied_index = snapshot_u64_meta(&progress, META_APPLIED_INDEX)?;
        if applied_index != target_index {
            return Err(Error::InvalidSnapshot(format!(
                "snapshot target {target_index} does not match applied index {applied_index}"
            )));
        }
        let applied_hash = snapshot_hash_meta(&progress, META_APPLIED_HASH)?;

        let temporary = NamedTempFile::new().map_err(io_error)?;
        let snapshot_database = Database::builder()
            .create_file(temporary.reopen().map_err(io_error)?)
            .map_err(database_error)?;
        let write = snapshot_database.begin_write().map_err(database_error)?;
        {
            let mut destination = write.open_table(DATA_TABLE).map_err(database_error)?;
            for row in data.iter().map_err(database_error)? {
                let (key, value) = row.map_err(database_error)?;
                destination
                    .insert(key.value(), value.value())
                    .map_err(database_error)?;
            }
        }
        {
            let mut destination = write.open_table(REQUEST_TABLE).map_err(database_error)?;
            for row in requests.iter().map_err(database_error)? {
                let (key, value) = row.map_err(database_error)?;
                destination
                    .insert(key.value(), value.value())
                    .map_err(database_error)?;
            }
        }
        // A snapshot is the compacted base. Post-snapshot entries are restored from the
        // external qlog tail, so the embedded hot-log mirror intentionally starts empty.
        write
            .open_table(EMBEDDED_LOG_TABLE)
            .map_err(database_error)?;
        {
            let mut destination = write.open_table(PROGRESS_TABLE).map_err(database_error)?;
            for row in progress.iter().map_err(database_error)? {
                let (key, value) = row.map_err(database_error)?;
                destination
                    .insert(key.value(), value.value())
                    .map_err(database_error)?;
            }
        }
        write.commit().map_err(database_error)?;
        drop(snapshot_database);
        temporary.as_file().sync_all().map_err(io_error)?;
        let db_bytes = read_stable_file(temporary.path())?;

        let mut snapshot = RedbSnapshot {
            cluster_id: self.cluster_id.clone(),
            created_by: self.node_id.clone(),
            epoch: self.epoch,
            config_id: self.config_id,
            applied_index,
            applied_hash,
            materializer_fingerprint: kv_materializer_fingerprint(),
            digest: LogHash::ZERO,
            db_bytes,
        };
        snapshot.digest = snapshot.recompute_digest();
        Ok(snapshot)
    }

    pub fn check_request(
        &self,
        request_id: &str,
        replicated_payload: &[u8],
    ) -> Result<Option<KvRequestRecord>, Error> {
        let read = self.database.begin_read().map_err(database_error)?;
        let table = read.open_table(REQUEST_TABLE).map_err(database_error)?;
        check_request_in_table(&table, request_id, replicated_payload)
    }

    /// Checks an ordered set of idempotency receipts in one redb read transaction.
    ///
    /// Database and table-open failures abort the lookup. Payload, identity, receipt decoding, and
    /// request-conflict failures remain aligned with their individual request.
    pub fn check_requests(
        &self,
        requests: &[(&str, &[u8])],
    ) -> Result<Vec<Result<Option<KvRequestRecord>, Error>>, Error> {
        let read = self.database.begin_read().map_err(database_error)?;
        let table = read.open_table(REQUEST_TABLE).map_err(database_error)?;
        Ok(requests
            .iter()
            .map(|(request_id, payload)| check_request_in_table(&table, request_id, payload))
            .collect())
    }

    /// Returns the exact locally durable qlog interval stored atomically with KV state.
    pub fn embedded_log_entries(
        &self,
        from_index: u64,
        through_index: u64,
    ) -> Result<Vec<LogEntry>, Error> {
        if from_index > through_index {
            return Ok(Vec::new());
        }
        let read = self.database.begin_read().map_err(database_error)?;
        let table = read
            .open_table(EMBEDDED_LOG_TABLE)
            .map_err(database_error)?;
        let mut expected = from_index;
        let mut entries = Vec::new();
        for row in table
            .range(from_index..=through_index)
            .map_err(database_error)?
        {
            let (index, encoded) = row.map_err(database_error)?;
            if index.value() != expected {
                return Err(Error::InvalidEntry(format!(
                    "embedded qlog is missing index {expected}"
                )));
            }
            let entry = decode_embedded_log_entry(encoded.value(), &self.cluster_id)?;
            if entry.index != expected {
                return Err(Error::InvalidEntry(
                    "embedded qlog key does not match its entry index".into(),
                ));
            }
            entries.push(entry);
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
    ///
    /// Callers must compact the serving qlog first. Retaining extra embedded entries after a
    /// crash is safe; deleting them before the serving qlog anchor is durable is not.
    pub fn compact_embedded_log_before(&self, anchor_index: u64) -> Result<(), Error> {
        let write = self.database.begin_write().map_err(database_error)?;
        let progress = write.open_table(PROGRESS_TABLE).map_err(database_error)?;
        let applied_index = read_u64_meta(&progress, META_APPLIED_INDEX)?;
        drop(progress);
        if anchor_index > applied_index {
            return Err(Error::InvalidEntry(format!(
                "cannot compact embedded qlog before anchor {anchor_index} beyond applied index {applied_index}"
            )));
        }
        let keys = {
            let table = write
                .open_table(EMBEDDED_LOG_TABLE)
                .map_err(database_error)?;
            table
                .range(..anchor_index)
                .map_err(database_error)?
                .map(|row| row.map(|(index, _)| index.value()).map_err(database_error))
                .collect::<Result<Vec<_>, _>>()?
        };
        {
            let mut table = write
                .open_table(EMBEDDED_LOG_TABLE)
                .map_err(database_error)?;
            for index in keys {
                table.remove(index).map_err(database_error)?;
            }
        }
        write.commit().map_err(database_error)
    }

    /// Applies one qlog entry in exactly one redb write transaction.
    #[tracing::instrument(skip(self, entry), fields(index = entry.index))]
    pub fn apply_entry(&self, entry: &LogEntry) -> Result<ApplyOutcome, Error> {
        self.validate_entry_identity(entry)?;
        if entry.recompute_hash() != entry.hash {
            return Err(Error::InvalidEntry(
                "entry hash does not match content".into(),
            ));
        }
        let decoded_commands = (entry.entry_type == EntryType::Command)
            .then(|| decode_replicated_kv_commands(&entry.payload))
            .transpose()?;

        let write = self.database.begin_write().map_err(database_error)?;
        let mut progress = write.open_table(PROGRESS_TABLE).map_err(database_error)?;
        let current_index = read_u64_meta(&progress, META_APPLIED_INDEX)?;
        let current_hash = read_hash_meta(&progress, META_APPLIED_HASH)?;

        if entry.index == current_index {
            if entry.hash != current_hash {
                return Err(Error::InvalidEntry(
                    "replayed index has a different hash".into(),
                ));
            }
            let embedded_log = write
                .open_table(EMBEDDED_LOG_TABLE)
                .map_err(database_error)?;
            let stored = embedded_log
                .get(entry.index)
                .map_err(database_error)?
                .ok_or_else(|| {
                    Error::InvalidEntry("replayed entry is missing from embedded qlog".into())
                })?;
            if decode_embedded_log_entry(stored.value(), &self.cluster_id)? != *entry {
                return Err(Error::InvalidEntry(
                    "replayed entry differs from embedded qlog".into(),
                ));
            }
            let result = if let Some(commands) = decoded_commands.as_ref() {
                let requests = write.open_table(REQUEST_TABLE).map_err(database_error)?;
                let mut result = None;
                for member in commands {
                    let record =
                        read_request(&requests, member.command.request_id())?.ok_or_else(|| {
                            Error::InvalidEntry("replayed command has no durable receipt".into())
                        })?;
                    if record.payload_hash != LogHash::digest(&[&member.individual_payload]) {
                        return Err(Error::RequestConflict {
                            request_id: member.command.request_id().into(),
                        });
                    }
                    if commands.len() == 1 {
                        result = Some(record.result);
                    }
                }
                result
            } else {
                None
            };
            return Ok(ApplyOutcome {
                applied_index: current_index,
                applied_hash: current_hash,
                result,
            });
        }

        let expected_index = current_index
            .checked_add(1)
            .ok_or_else(|| Error::InvalidEntry("applied index overflow".into()))?;
        if entry.index != expected_index {
            return Err(Error::InvalidEntry(format!(
                "expected log index {expected_index}, got {}",
                entry.index
            )));
        }
        if entry.prev_hash != current_hash {
            return Err(Error::InvalidEntry(
                "entry previous hash does not match applied hash".into(),
            ));
        }

        let result = match entry.entry_type {
            EntryType::Command => {
                let commands = decoded_commands
                    .as_ref()
                    .expect("command entries were decoded before opening the transaction");
                let mut single_result = None;
                {
                    let mut requests = write.open_table(REQUEST_TABLE).map_err(database_error)?;
                    let mut data = write.open_table(DATA_TABLE).map_err(database_error)?;
                    for member in commands {
                        let payload_hash = LogHash::digest(&[&member.individual_payload]);
                        let command_result = if let Some(record) =
                            read_request(&requests, member.command.request_id())?
                        {
                            if record.payload_hash != payload_hash {
                                return Err(Error::RequestConflict {
                                    request_id: member.command.request_id().into(),
                                });
                            }
                            record.result
                        } else {
                            let command_result = match &member.command.operation {
                                KvOperationV1::Put { key, value } => {
                                    let replaced =
                                        data.get(key.as_slice()).map_err(database_error)?.is_some();
                                    data.insert(key.as_slice(), value.as_slice())
                                        .map_err(database_error)?;
                                    KvCommandResultV1::Put { replaced }
                                }
                                KvOperationV1::Delete { key } => {
                                    let existed = data
                                        .remove(key.as_slice())
                                        .map_err(database_error)?
                                        .is_some();
                                    KvCommandResultV1::Delete { existed }
                                }
                            };
                            let record = KvRequestRecord {
                                payload_hash,
                                original_log_index: entry.index,
                                original_log_hash: entry.hash,
                                result: command_result.clone(),
                            };
                            let encoded_record = record.encode();
                            requests
                                .insert(
                                    member.command.request_id().as_bytes(),
                                    encoded_record.as_slice(),
                                )
                                .map_err(database_error)?;
                            command_result
                        };
                        if commands.len() == 1 {
                            single_result = Some(command_result);
                        }
                    }
                }
                single_result
            }
            EntryType::Noop => {
                if !entry.payload.is_empty() {
                    return Err(Error::InvalidEntry(
                        "noop entry payload must be empty".into(),
                    ));
                }
                None
            }
            EntryType::ConfigChange => {
                ConfigChange::recognize_parts(entry.entry_type, &entry.payload)
                    .map_err(|_| Error::InvalidEntry("invalid configuration change".into()))?;
                None
            }
            EntryType::SnapshotBarrier | EntryType::SnapshotPublished => None,
        };

        {
            let mut embedded_log = write
                .open_table(EMBEDDED_LOG_TABLE)
                .map_err(database_error)?;
            if embedded_log
                .get(entry.index)
                .map_err(database_error)?
                .is_some()
            {
                return Err(Error::InvalidEntry(
                    "next entry already exists in embedded qlog".into(),
                ));
            }
            let encoded = rhiza_log::encode_segment(std::slice::from_ref(entry));
            embedded_log
                .insert(entry.index, encoded.as_slice())
                .map_err(database_error)?;
        }
        progress
            .insert(META_APPLIED_INDEX, entry.index.to_be_bytes().as_slice())
            .map_err(database_error)?;
        progress
            .insert(META_APPLIED_HASH, entry.hash.as_bytes().as_slice())
            .map_err(database_error)?;
        drop(progress);
        write.commit().map_err(database_error)?;

        Ok(ApplyOutcome {
            applied_index: entry.index,
            applied_hash: entry.hash,
            result,
        })
    }

    fn validate_entry_identity(&self, entry: &LogEntry) -> Result<(), Error> {
        if entry.cluster_id != self.cluster_id {
            return Err(Error::InvalidEntry(
                "entry belongs to another cluster".into(),
            ));
        }
        if entry.epoch != self.epoch {
            return Err(Error::InvalidEntry("entry epoch does not match".into()));
        }
        if entry.config_id != self.config_id {
            return Err(Error::InvalidEntry(
                "entry configuration id does not match".into(),
            ));
        }
        Ok(())
    }
}

/// Installs a verified snapshot at a new path without replacing existing bytes.
pub fn restore_snapshot_file(
    path: impl AsRef<Path>,
    snapshot: &RedbSnapshot,
    target_node_id: &str,
) -> Result<(), Error> {
    if target_node_id.is_empty() {
        return Err(Error::InvalidSnapshot("target node id is empty".into()));
    }
    if snapshot.recompute_digest() != snapshot.digest {
        return Err(Error::InvalidSnapshot(
            "snapshot digest does not match its contents".into(),
        ));
    }
    let path = path.as_ref();
    if path.exists() {
        return Err(Error::InvalidSnapshot(
            "restore target already exists".into(),
        ));
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(io_error)?;
    let mut temporary = NamedTempFile::new_in(parent).map_err(io_error)?;
    temporary.write_all(&snapshot.db_bytes).map_err(io_error)?;
    temporary.as_file().sync_all().map_err(io_error)?;

    let database = Database::builder()
        .create_file(temporary.reopen().map_err(io_error)?)
        .map_err(|error| {
            Error::InvalidSnapshot(format!("redb database validation failed: {error}"))
        })?;
    validate_restored_database(&database, snapshot, &snapshot.created_by)?;
    rebind_snapshot_node(&database, target_node_id)?;
    let validation = validate_restored_database(&database, snapshot, target_node_id);
    drop(database);
    validation?;

    temporary.persist_noclobber(path).map_err(|error| {
        if error.error.kind() == std::io::ErrorKind::AlreadyExists {
            Error::InvalidSnapshot("restore target already exists".into())
        } else {
            io_error(error.error)
        }
    })?;
    if let Err(error) = File::open(path).and_then(|file| file.sync_all()) {
        remove_failed_install(path, parent);
        return Err(io_error(error));
    }
    if let Err(error) = File::open(parent).and_then(|directory| directory.sync_all()) {
        remove_failed_install(path, parent);
        return Err(io_error(error));
    }
    Ok(())
}

fn remove_failed_install(path: &Path, parent: &Path) {
    let _ = fs::remove_file(path);
    let _ = File::open(parent).and_then(|directory| directory.sync_all());
}

fn read_stable_file(path: &Path) -> Result<Vec<u8>, Error> {
    let mut file = File::open(path).map_err(io_error)?;
    let expected_length = usize::try_from(file.metadata().map_err(io_error)?.len())
        .map_err(|_| Error::Io("snapshot file length does not fit this platform".into()))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(expected_length)
        .map_err(|error| Error::Io(format!("could not allocate snapshot buffer: {error}")))?;
    file.read_to_end(&mut bytes).map_err(io_error)?;
    if bytes.len() != expected_length {
        return Err(Error::Io(format!(
            "snapshot file length changed while reading: expected {expected_length}, got {}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn validate_restored_database(
    database: &Database,
    snapshot: &RedbSnapshot,
    expected_node_id: &str,
) -> Result<(), Error> {
    if snapshot.materializer_fingerprint != kv_materializer_fingerprint() {
        return Err(Error::InvalidSnapshot(
            "materializer fingerprint does not match this binary".into(),
        ));
    }
    let read = database.begin_read().map_err(invalid_snapshot_error)?;
    let mut data_exists = false;
    let mut requests_exist = false;
    let mut progress_exists = false;
    let mut embedded_log_exists = false;
    for table in read.list_tables().map_err(invalid_snapshot_error)? {
        match table.name() {
            name if name == DATA_TABLE.name() => data_exists = true,
            name if name == REQUEST_TABLE.name() => requests_exist = true,
            name if name == PROGRESS_TABLE.name() => progress_exists = true,
            name if name == EMBEDDED_LOG_TABLE.name() => embedded_log_exists = true,
            _ => {}
        }
    }
    if !(data_exists && requests_exist && progress_exists && embedded_log_exists) {
        return Err(Error::InvalidSnapshot(
            "snapshot is missing a required KV table".into(),
        ));
    }
    read.open_table(DATA_TABLE)
        .map_err(invalid_snapshot_error)?;
    read.open_table(REQUEST_TABLE)
        .map_err(invalid_snapshot_error)?;
    read.open_table(EMBEDDED_LOG_TABLE)
        .map_err(invalid_snapshot_error)?;
    let progress = read
        .open_table(PROGRESS_TABLE)
        .map_err(invalid_snapshot_error)?;
    validate_snapshot_identity(
        &progress,
        &snapshot.cluster_id,
        expected_node_id,
        snapshot.epoch,
        snapshot.config_id,
    )?;
    if snapshot_u64_meta(&progress, META_APPLIED_INDEX)? != snapshot.applied_index {
        return Err(Error::InvalidSnapshot(
            "inner applied index does not match snapshot metadata".into(),
        ));
    }
    if snapshot_hash_meta(&progress, META_APPLIED_HASH)? != snapshot.applied_hash {
        return Err(Error::InvalidSnapshot(
            "inner applied hash does not match snapshot metadata".into(),
        ));
    }
    Ok(())
}

fn rebind_snapshot_node(database: &Database, target_node_id: &str) -> Result<(), Error> {
    let write = database.begin_write().map_err(invalid_snapshot_error)?;
    {
        let mut progress = write
            .open_table(PROGRESS_TABLE)
            .map_err(invalid_snapshot_error)?;
        progress
            .insert(META_NODE_ID, target_node_id.as_bytes())
            .map_err(invalid_snapshot_error)?;
    }
    write.commit().map_err(invalid_snapshot_error)
}

fn validate_snapshot_identity(
    progress: &impl ReadableTable<&'static str, &'static [u8]>,
    cluster_id: &str,
    node_id: &str,
    epoch: u64,
    config_id: u64,
) -> Result<(), Error> {
    snapshot_meta(progress, META_CLUSTER_ID, cluster_id.as_bytes())?;
    snapshot_meta(progress, META_NODE_ID, node_id.as_bytes())?;
    snapshot_meta(progress, META_EPOCH, &epoch.to_be_bytes())?;
    snapshot_meta(progress, META_CONFIG_ID, &config_id.to_be_bytes())?;
    snapshot_meta(
        progress,
        META_MATERIALIZER_FINGERPRINT,
        kv_materializer_fingerprint().as_bytes(),
    )
}

fn snapshot_meta(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    key: &str,
    expected: &[u8],
) -> Result<(), Error> {
    let actual = table
        .get(key)
        .map_err(invalid_snapshot_error)?
        .ok_or_else(|| Error::InvalidSnapshot(format!("missing progress metadata {key}")))?;
    if actual.value() != expected {
        return Err(Error::InvalidSnapshot(format!(
            "progress metadata {key} does not match the snapshot identity"
        )));
    }
    Ok(())
}

fn snapshot_u64_meta(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    key: &str,
) -> Result<u64, Error> {
    let value = table
        .get(key)
        .map_err(invalid_snapshot_error)?
        .ok_or_else(|| Error::InvalidSnapshot(format!("missing progress metadata {key}")))?;
    let bytes: [u8; 8] = value
        .value()
        .try_into()
        .map_err(|_| Error::InvalidSnapshot(format!("invalid u64 progress metadata {key}")))?;
    Ok(u64::from_be_bytes(bytes))
}

fn snapshot_hash_meta(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    key: &str,
) -> Result<LogHash, Error> {
    let value = table
        .get(key)
        .map_err(invalid_snapshot_error)?
        .ok_or_else(|| Error::InvalidSnapshot(format!("missing progress metadata {key}")))?;
    let bytes: [u8; 32] = value
        .value()
        .try_into()
        .map_err(|_| Error::InvalidSnapshot(format!("invalid hash progress metadata {key}")))?;
    Ok(LogHash::from_bytes(bytes))
}

fn invalid_snapshot_error(error: impl fmt::Display) -> Error {
    Error::InvalidSnapshot(error.to_string())
}

fn length_prefixed(value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(8 + value.len());
    let length = u64::try_from(value.len()).expect("usize fits in u64");
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(value);
    encoded
}

fn initialize_or_validate(
    database: &Database,
    cluster_id: &str,
    node_id: &str,
    epoch: u64,
    config_id: u64,
) -> Result<(), Error> {
    let read = database.begin_read().map_err(database_error)?;
    let mut data_exists = false;
    let mut requests_exist = false;
    let mut progress_exists = false;
    let mut embedded_log_exists = false;
    for table in read.list_tables().map_err(database_error)? {
        match table.name() {
            name if name == DATA_TABLE.name() => data_exists = true,
            name if name == REQUEST_TABLE.name() => requests_exist = true,
            name if name == PROGRESS_TABLE.name() => progress_exists = true,
            name if name == EMBEDDED_LOG_TABLE.name() => embedded_log_exists = true,
            _ => {}
        }
    }

    match (
        data_exists,
        requests_exist,
        progress_exists,
        embedded_log_exists,
    ) {
        (false, false, false, false) => {}
        (true, true, true, true) => {
            let progress = read.open_table(PROGRESS_TABLE).map_err(database_error)?;
            if progress
                .get(META_CLUSTER_ID)
                .map_err(database_error)?
                .is_none()
            {
                return Err(Error::PartialInitialization);
            }
            require_meta(&progress, META_CLUSTER_ID, cluster_id.as_bytes())?;
            require_meta(&progress, META_NODE_ID, node_id.as_bytes())?;
            require_meta(&progress, META_EPOCH, &epoch.to_be_bytes())?;
            require_meta(&progress, META_CONFIG_ID, &config_id.to_be_bytes())?;
            require_meta(
                &progress,
                META_MATERIALIZER_FINGERPRINT,
                kv_materializer_fingerprint().as_bytes(),
            )?;
            read_u64_meta(&progress, META_APPLIED_INDEX)?;
            read_hash_meta(&progress, META_APPLIED_HASH)?;
            return Ok(());
        }
        _ => return Err(Error::PartialInitialization),
    }
    drop(read);

    let write = database.begin_write().map_err(database_error)?;
    write.open_table(DATA_TABLE).map_err(database_error)?;
    write.open_table(REQUEST_TABLE).map_err(database_error)?;
    write
        .open_table(EMBEDDED_LOG_TABLE)
        .map_err(database_error)?;
    let mut progress = write.open_table(PROGRESS_TABLE).map_err(database_error)?;
    progress
        .insert(META_CLUSTER_ID, cluster_id.as_bytes())
        .map_err(database_error)?;
    progress
        .insert(META_NODE_ID, node_id.as_bytes())
        .map_err(database_error)?;
    progress
        .insert(META_EPOCH, epoch.to_be_bytes().as_slice())
        .map_err(database_error)?;
    progress
        .insert(META_CONFIG_ID, config_id.to_be_bytes().as_slice())
        .map_err(database_error)?;
    progress
        .insert(META_APPLIED_INDEX, 0_u64.to_be_bytes().as_slice())
        .map_err(database_error)?;
    progress
        .insert(META_APPLIED_HASH, LogHash::ZERO.as_bytes().as_slice())
        .map_err(database_error)?;
    progress
        .insert(
            META_MATERIALIZER_FINGERPRINT,
            kv_materializer_fingerprint().as_bytes().as_slice(),
        )
        .map_err(database_error)?;
    drop(progress);
    write.commit().map_err(database_error)
}

fn require_meta(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    key: &str,
    expected: &[u8],
) -> Result<(), Error> {
    let actual = table
        .get(key)
        .map_err(database_error)?
        .ok_or_else(|| Error::Database(format!("missing progress metadata {key}")))?;
    if actual.value() != expected {
        return Err(Error::InvalidEntry(format!(
            "database metadata {key} does not match open parameters"
        )));
    }
    Ok(())
}

fn read_u64_meta(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    key: &str,
) -> Result<u64, Error> {
    let value = table
        .get(key)
        .map_err(database_error)?
        .ok_or_else(|| Error::Database(format!("missing progress metadata {key}")))?;
    let bytes: [u8; 8] = value
        .value()
        .try_into()
        .map_err(|_| Error::Database(format!("invalid u64 progress metadata {key}")))?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_hash_meta(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    key: &str,
) -> Result<LogHash, Error> {
    let value = table
        .get(key)
        .map_err(database_error)?
        .ok_or_else(|| Error::Database(format!("missing progress metadata {key}")))?;
    let bytes: [u8; 32] = value
        .value()
        .try_into()
        .map_err(|_| Error::Database(format!("invalid hash progress metadata {key}")))?;
    Ok(LogHash::from_bytes(bytes))
}

fn read_tip(table: &impl ReadableTable<&'static str, &'static [u8]>) -> Result<KvReadTip, Error> {
    Ok(KvReadTip {
        applied_index: read_u64_meta(table, META_APPLIED_INDEX)?,
        applied_hash: read_hash_meta(table, META_APPLIED_HASH)?,
    })
}

fn read_request(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    request_id: &str,
) -> Result<Option<KvRequestRecord>, Error> {
    table
        .get(request_id.as_bytes())
        .map_err(database_error)?
        .map(|value| KvRequestRecord::decode(value.value()))
        .transpose()
}

fn check_request_in_table(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    request_id: &str,
    replicated_payload: &[u8],
) -> Result<Option<KvRequestRecord>, Error> {
    let command = decode_replicated_kv_command(replicated_payload)?;
    if command.request_id() != request_id {
        return Err(Error::InvalidCommand(
            "provided and encoded request ids differ".into(),
        ));
    }
    let payload_hash = LogHash::digest(&[replicated_payload]);
    let record = read_request(table, request_id)?;
    if record
        .as_ref()
        .is_some_and(|record| record.payload_hash != payload_hash)
    {
        return Err(Error::RequestConflict {
            request_id: request_id.into(),
        });
    }
    Ok(record)
}

struct Decoder<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    const fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], Error> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| Error::Codec("length overflow".into()))?;
        let value = self
            .encoded
            .get(self.offset..end)
            .ok_or_else(|| Error::Codec("truncated encoding".into()))?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("u16 slice length"),
        ))
    }

    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("u32 slice length"),
        ))
    }

    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("u64 slice length"),
        ))
    }

    fn array_32(&mut self) -> Result<[u8; 32], Error> {
        Ok(self.take(32)?.try_into().expect("hash slice length"))
    }

    fn length_prefixed_u32(&mut self, maximum: usize, name: &str) -> Result<Vec<u8>, Error> {
        let length = usize::try_from(self.u32()?)
            .map_err(|_| Error::Codec(format!("{name} length does not fit usize")))?;
        if length > maximum {
            return Err(Error::Codec(format!("{name} exceeds {maximum} bytes")));
        }
        Ok(self.take(length)?.to_vec())
    }

    fn finish(self) -> Result<(), Error> {
        if self.offset != self.encoded.len() {
            return Err(Error::Codec("trailing bytes".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
fn batch_v2_materializer_fingerprint() -> LogHash {
    LogHash::digest(&[
        b"rhiza-kv-materializer-v1\0",
        b"redb=4.1.0",
        b"schema=1",
        COMMAND_MAGIC,
        &COMMAND_VERSION.to_be_bytes(),
        BATCH_COMMAND_MAGIC,
        &2_u16.to_be_bytes(),
    ])
}

#[cfg(test)]
fn pre_embedded_qlog_materializer_fingerprint() -> LogHash {
    LogHash::digest(&[
        b"rhiza-kv-materializer-v2\0",
        b"redb=4.1.0",
        b"schema=1",
        COMMAND_MAGIC,
        &COMMAND_VERSION.to_be_bytes(),
        BATCH_COMMAND_MAGIC,
        &BATCH_COMMAND_VERSION.to_be_bytes(),
    ])
}

#[cfg(test)]
mod replicated_batch_tests {
    use super::*;

    #[test]
    fn replicated_batch_codec_accepts_1024_canonically_and_rejects_1025() {
        let commands = (0..1024)
            .map(|index| {
                KvCommandV1::delete(
                    format!("request-{index}"),
                    format!("key-{index}").into_bytes(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        let encoded = encode_replicated_kv_batch(&commands).unwrap();
        assert_eq!(encode_replicated_kv_batch(&commands).unwrap(), encoded);
        let envelope = ReplicatedCommandEnvelope::decode(&encoded).unwrap();
        assert_eq!(envelope.command_version(), 3);
        let previous_wire = ReplicatedCommandEnvelope::new(
            ExecutionProfile::Kv,
            2,
            BATCH_REQUEST_ID,
            envelope.body().to_vec(),
        )
        .unwrap()
        .encode()
        .unwrap();
        assert!(matches!(
            decode_replicated_kv_commands(&previous_wire),
            Err(Error::InvalidCommand(message)) if message.contains("unsupported command version 2")
        ));
        let decoded = decode_replicated_kv_commands(&encoded).unwrap();
        assert_eq!(decoded.len(), 1024);
        assert!(decoded
            .iter()
            .zip(&commands)
            .all(|(decoded, command)| decoded.command == *command));

        let mut oversized = commands;
        oversized.push(KvCommandV1::delete("request-1024", b"key-1024".to_vec()).unwrap());
        assert!(matches!(
            encode_replicated_kv_batch(&oversized),
            Err(Error::InvalidCommand(_))
        ));

        let mut forged_body = envelope.body().to_vec();
        let count_offset = BATCH_COMMAND_MAGIC.len();
        forged_body[count_offset..count_offset + 2].copy_from_slice(&1025_u16.to_be_bytes());
        let extra = oversized.last().unwrap().encode().unwrap();
        forged_body.extend_from_slice(&(extra.len() as u32).to_be_bytes());
        forged_body.extend_from_slice(&extra);
        let forged = ReplicatedCommandEnvelope::new(
            ExecutionProfile::Kv,
            BATCH_COMMAND_VERSION,
            BATCH_REQUEST_ID,
            forged_body,
        )
        .unwrap()
        .encode()
        .unwrap();
        assert!(matches!(
            decode_replicated_kv_commands(&forged),
            Err(Error::InvalidCommand(_))
        ));
    }

    #[test]
    fn materializer_fingerprint_identifies_the_embedded_qlog_contract() {
        let expected = LogHash::digest(&[
            b"rhiza-kv-materializer-v3\0",
            b"redb=4.1.0",
            b"schema=2;embedded_qlog=qlog_segment_v3",
            COMMAND_MAGIC,
            &COMMAND_VERSION.to_be_bytes(),
            BATCH_COMMAND_MAGIC,
            &3_u16.to_be_bytes(),
        ]);

        assert_eq!(kv_materializer_fingerprint(), expected);
        assert_ne!(
            kv_materializer_fingerprint(),
            batch_v2_materializer_fingerprint()
        );
        assert_ne!(
            kv_materializer_fingerprint(),
            pre_embedded_qlog_materializer_fingerprint()
        );
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    fn entry(index: u64, prev_hash: LogHash, payload: Vec<u8>) -> LogEntry {
        let hash = LogEntry::calculate_hash(
            "cluster-1",
            index,
            7,
            3,
            EntryType::Command,
            prev_hash,
            &payload,
        );
        LogEntry {
            cluster_id: "cluster-1".into(),
            epoch: 7,
            config_id: 3,
            index,
            entry_type: EntryType::Command,
            payload,
            prev_hash,
            hash,
        }
    }

    #[test]
    fn strict_apply_reopens_materialized_value_and_embedded_log_entry_together() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("strict.redb");
        let command = KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap();
        let committed = entry(
            1,
            LogHash::ZERO,
            encode_replicated_kv_command(&command).unwrap(),
        );
        {
            let state = RedbStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
            state.apply_entry(&committed).unwrap();
        }

        let reopened = RedbStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();

        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
        assert_eq!(
            reopened.applied_tip().unwrap(),
            LogAnchor::new(1, committed.hash)
        );
        assert_eq!(reopened.embedded_log_entries(1, 1).unwrap(), [committed]);
    }

    #[test]
    fn verified_checkpoint_compaction_removes_only_the_covered_embedded_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let state = RedbStateMachine::open(dir.path().join("kv.redb"), "cluster-1", "node-1", 7, 3)
            .unwrap();
        let first_command =
            KvCommandV1::put("request-1", b"key".to_vec(), b"one".to_vec()).unwrap();
        let first = entry(
            1,
            LogHash::ZERO,
            encode_replicated_kv_command(&first_command).unwrap(),
        );
        let second_command =
            KvCommandV1::put("request-2", b"key".to_vec(), b"two".to_vec()).unwrap();
        let second = entry(
            2,
            first.hash,
            encode_replicated_kv_command(&second_command).unwrap(),
        );
        state.apply_entry(&first).unwrap();
        state.apply_entry(&second).unwrap();

        state.compact_embedded_log_before(2).unwrap();

        assert_eq!(
            state.embedded_log_entries(2, 2).unwrap(),
            std::slice::from_ref(&second)
        );
        assert!(state.embedded_log_entries(1, 1).is_err());
        state.apply_entry(&second).unwrap();
        assert!(state.compact_embedded_log_before(3).is_err());
    }

    fn snapshot_fixture() -> (tempfile::TempDir, RedbSnapshot) {
        let dir = tempfile::tempdir().unwrap();
        let source =
            RedbStateMachine::open(dir.path().join("source.redb"), "cluster-1", "node-1", 7, 3)
                .unwrap();
        let command = KvCommandV1::put("put-1", b"key".to_vec(), b"value".to_vec()).unwrap();
        let payload = encode_replicated_kv_command(&command).unwrap();
        source
            .apply_entry(&entry(1, LogHash::ZERO, payload))
            .unwrap();
        let snapshot = source.create_snapshot(1).unwrap();
        (dir, snapshot)
    }

    fn seed_rows(
        state: &RedbStateMachine,
        rows: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
        applied_index: u64,
        applied_hash: LogHash,
    ) {
        let write = state.database.begin_write().unwrap();
        {
            let mut data = write.open_table(DATA_TABLE).unwrap();
            for (key, value) in rows {
                data.insert(key.as_slice(), value.as_slice()).unwrap();
            }
        }
        {
            let mut progress = write.open_table(PROGRESS_TABLE).unwrap();
            progress
                .insert(META_APPLIED_INDEX, applied_index.to_be_bytes().as_slice())
                .unwrap();
            progress
                .insert(META_APPLIED_HASH, applied_hash.as_bytes().as_slice())
                .unwrap();
        }
        write.commit().unwrap();
    }

    #[test]
    fn exact_get_returns_value_and_applied_tip_from_one_snapshot_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("get.redb");
        let hash = LogHash::digest(&[b"get-tip"]);
        {
            let state = RedbStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
            seed_rows(&state, [(b"key".to_vec(), b"value".to_vec())], 41, hash);
        }

        let reopened = RedbStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
        let result = reopened.get_with_tip(b"key").unwrap();

        assert_eq!(result.value(), Some(b"value".as_slice()));
        assert_eq!(result.tip().applied_index(), 41);
        assert_eq!(result.tip().applied_hash(), hash);
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));

        let (value, tip) = result.into_parts();
        assert_eq!(value, Some(b"value".to_vec()));
        assert_eq!(tip.applied_index(), 41);
        assert_eq!(tip.applied_hash(), hash);
    }

    #[test]
    fn applied_tip_returns_index_and_hash_from_one_read_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            RedbStateMachine::open(dir.path().join("tip.redb"), "cluster-1", "node-1", 7, 3)
                .unwrap();
        let hash = LogHash::digest(&[b"tip"]);
        seed_rows(&state, [], 41, hash);

        assert_eq!(state.applied_tip().unwrap(), LogAnchor::new(41, hash));
        assert_eq!(state.applied_index().unwrap(), 41);
        assert_eq!(state.applied_hash().unwrap(), hash);
    }

    #[test]
    fn open_rejects_storage_without_the_embedded_qlog_contract() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("previous-fingerprint.redb");
        {
            let state = RedbStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
            let write = state.database.begin_write().unwrap();
            {
                let mut progress = write.open_table(PROGRESS_TABLE).unwrap();
                progress
                    .insert(
                        META_MATERIALIZER_FINGERPRINT,
                        pre_embedded_qlog_materializer_fingerprint()
                            .as_bytes()
                            .as_slice(),
                    )
                    .unwrap();
            }
            write.commit().unwrap();
        }

        assert!(matches!(
            RedbStateMachine::open(&path, "cluster-1", "node-1", 7, 3),
            Err(Error::InvalidEntry(message)) if message.contains("materializer_fingerprint")
        ));
    }

    #[test]
    fn range_scan_is_byte_ordered_end_exclusive_and_cursor_paged() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            RedbStateMachine::open(dir.path().join("range.redb"), "cluster-1", "node-1", 7, 3)
                .unwrap();
        let hash = LogHash::digest(&[b"range-tip"]);
        seed_rows(
            &state,
            [
                (b"b".to_vec(), b"4".to_vec()),
                (b"aa".to_vec(), b"2".to_vec()),
                (b"a".to_vec(), b"1".to_vec()),
                (b"ab".to_vec(), b"3".to_vec()),
            ],
            9,
            hash,
        );

        let first = state.scan_range(b"a", Some(b"b"), 2, None).unwrap();
        assert_eq!(
            first.rows(),
            &[
                KvScanRow::new(b"a".to_vec(), b"1".to_vec()),
                KvScanRow::new(b"aa".to_vec(), b"2".to_vec()),
            ]
        );
        assert_eq!(first.next_cursor(), Some(b"aa".as_slice()));
        assert_eq!(first.tip().applied_index(), 9);
        assert_eq!(first.tip().applied_hash(), hash);

        let second = state
            .scan_range(b"a", Some(b"b"), 2, first.next_cursor())
            .unwrap();
        assert_eq!(
            second.rows(),
            &[KvScanRow::new(b"ab".to_vec(), b"3".to_vec())]
        );
        assert_eq!(second.next_cursor(), None);
    }

    #[test]
    fn prefix_scan_handles_empty_and_all_ff_prefixes() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            RedbStateMachine::open(dir.path().join("prefix.redb"), "cluster-1", "node-1", 7, 3)
                .unwrap();
        seed_rows(
            &state,
            [
                (vec![0xfe], b"before".to_vec()),
                (vec![0xff], b"root".to_vec()),
                (vec![0xff, 0x00], b"zero".to_vec()),
                (vec![0xff, 0xff], b"max".to_vec()),
            ],
            4,
            LogHash::digest(&[b"prefix-tip"]),
        );

        let all = state.scan_prefix(b"", 10, None).unwrap();
        assert_eq!(all.rows().len(), 4);
        assert_eq!(all.rows()[0].key(), &[0xfe]);
        assert_eq!(all.rows()[3].key(), &[0xff, 0xff]);

        let ff = state.scan_prefix(&[0xff], 10, None).unwrap();
        assert_eq!(
            ff.rows().iter().map(KvScanRow::key).collect::<Vec<_>>(),
            vec![&[0xff][..], &[0xff, 0x00][..], &[0xff, 0xff][..]]
        );
        assert_eq!(ff.next_cursor(), None);
    }

    #[test]
    fn scan_rejects_invalid_row_limits_caps_bytes_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("caps.redb");
        let hash = LogHash::digest(&[b"caps-tip"]);
        {
            let state = RedbStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
            let rows = (0..MAX_KV_SCAN_ROWS + 2).map(|index| {
                (
                    format!("small-{index:05}").into_bytes(),
                    vec![u8::try_from(index % 251).unwrap()],
                )
            });
            seed_rows(&state, rows, 17, hash);

            assert!(matches!(
                state.scan_prefix(b"small-", 0, None),
                Err(Error::InvalidQuery(_))
            ));
            assert!(matches!(
                state.scan_prefix(b"small-", MAX_KV_SCAN_ROWS + 1, None),
                Err(Error::InvalidQuery(_))
            ));
            let capped = state
                .scan_prefix(b"small-", MAX_KV_SCAN_ROWS, None)
                .unwrap();
            assert_eq!(capped.rows().len(), MAX_KV_SCAN_ROWS);
            assert!(capped.next_cursor().is_some());
        }

        let reopened = RedbStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
        let large_rows = (0..5).map(|index| {
            (
                format!("wide-{index}").into_bytes(),
                vec![u8::try_from(index).unwrap(); MAX_KV_VALUE_BYTES],
            )
        });
        seed_rows(&reopened, large_rows, 18, hash);
        let capped = reopened.scan_prefix(b"wide-", 5, None).unwrap();
        assert!(capped.rows().len() < 5);
        assert!(
            capped
                .rows()
                .iter()
                .map(|row| row.key().len() + row.value().len())
                .sum::<usize>()
                <= MAX_KV_SCAN_RESULT_BYTES
        );
        assert!(capped.next_cursor().is_some());
        assert_eq!(capped.tip().applied_index(), 18);
    }

    #[test]
    fn scan_returns_one_largest_valid_record_without_exhausting_the_page() {
        let dir = tempfile::tempdir().unwrap();
        let state = RedbStateMachine::open(
            dir.path().join("largest-record.redb"),
            "cluster-1",
            "node-1",
            7,
            3,
        )
        .unwrap();
        let key = vec![b'k'; MAX_KV_KEY_BYTES];
        let value = vec![b'v'; MAX_KV_VALUE_BYTES];
        seed_rows(
            &state,
            [(key.clone(), value.clone())],
            1,
            LogHash::digest(&[b"largest-record"]),
        );

        let result = state.scan_prefix(b"", 1, None).unwrap();

        assert_eq!(result.rows(), &[KvScanRow::new(key, value)]);
        assert_eq!(result.next_cursor(), None);
    }

    #[test]
    fn snapshot_codec_round_trips_one_canonical_envelope() {
        let (_dir, snapshot) = snapshot_fixture();

        let encoded = encode_snapshot(&snapshot).unwrap();
        let decoded = decode_snapshot(&encoded).unwrap();

        assert_eq!(decoded, snapshot);
        assert_eq!(encode_snapshot(&decoded).unwrap(), encoded);
    }

    #[test]
    fn snapshot_codec_rejects_unknown_version_and_tamper() {
        let (_dir, snapshot) = snapshot_fixture();
        let encoded = encode_snapshot(&snapshot).unwrap();

        let mut unknown_version = encoded.clone();
        unknown_version[4..6].copy_from_slice(&2_u16.to_be_bytes());
        assert!(matches!(
            decode_snapshot(&unknown_version),
            Err(Error::InvalidSnapshot(message)) if message.contains("version")
        ));

        let mut tampered = encoded;
        *tampered.last_mut().unwrap() ^= 0xff;
        assert!(matches!(
            decode_snapshot(&tampered),
            Err(Error::InvalidSnapshot(_))
        ));
    }

    #[test]
    fn restore_rejects_tampered_bytes_and_leaves_target_absent() {
        let (dir, mut snapshot) = snapshot_fixture();
        snapshot.db_bytes[0] ^= 0xff;
        let target = dir.path().join("restored.redb");

        assert!(matches!(
            restore_snapshot_file(&target, &snapshot, "node-2"),
            Err(Error::InvalidSnapshot(_))
        ));
        assert!(!target.exists());
    }

    #[test]
    fn restore_rejects_tampered_digest_and_leaves_target_absent() {
        let (dir, mut snapshot) = snapshot_fixture();
        snapshot.digest = LogHash::ZERO;
        let target = dir.path().join("restored.redb");

        assert!(matches!(
            restore_snapshot_file(&target, &snapshot, "node-2"),
            Err(Error::InvalidSnapshot(_))
        ));
        assert!(!target.exists());
    }

    #[test]
    fn restore_rejects_outer_identity_that_differs_from_inner_metadata() {
        let (dir, mut snapshot) = snapshot_fixture();
        snapshot.cluster_id.push_str("-other");
        snapshot.digest = snapshot.recompute_digest();
        let target = dir.path().join("restored.redb");

        assert!(matches!(
            restore_snapshot_file(&target, &snapshot, "node-2"),
            Err(Error::InvalidSnapshot(_))
        ));
        assert!(!target.exists());
    }

    #[test]
    fn restore_rejects_truncated_physical_database_with_a_valid_outer_digest() {
        let (dir, mut snapshot) = snapshot_fixture();
        snapshot.db_bytes.truncate(16);
        snapshot.digest = snapshot.recompute_digest();
        let target = dir.path().join("restored.redb");

        assert!(matches!(
            restore_snapshot_file(&target, &snapshot, "node-2"),
            Err(Error::InvalidSnapshot(_))
        ));
        assert!(!target.exists());
    }

    #[test]
    fn restore_rejects_a_tampered_materializer_fingerprint() {
        let (dir, mut snapshot) = snapshot_fixture();
        snapshot.materializer_fingerprint = LogHash::ZERO;
        snapshot.digest = snapshot.recompute_digest();
        let target = dir.path().join("restored.redb");

        assert!(matches!(
            restore_snapshot_file(&target, &snapshot, "node-2"),
            Err(Error::InvalidSnapshot(_))
        ));
        assert!(!target.exists());
    }

    #[test]
    fn restore_rejects_the_previous_batch_v2_materializer_fingerprint() {
        let (dir, mut snapshot) = snapshot_fixture();
        snapshot.materializer_fingerprint = batch_v2_materializer_fingerprint();
        snapshot.digest = snapshot.recompute_digest();
        let target = dir.path().join("restored.redb");

        assert!(matches!(
            restore_snapshot_file(&target, &snapshot, "node-2"),
            Err(Error::InvalidSnapshot(_))
        ));
        assert!(!target.exists());
    }
}
