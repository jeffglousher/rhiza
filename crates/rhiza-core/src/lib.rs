#![doc = include_str!("../README.md")]

use sha2::{Digest, Sha256};

pub type LogIndex = u64;
pub type Epoch = u64;
pub type ConfigId = u64;
pub type NodeId = String;
pub type ClusterId = String;

/// A transport-neutral category for a public error.
///
/// Downstream matches must include a wildcard arm because new categories may be
/// added in future releases.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCategory {
    InvalidRequest,
    Authentication,
    Conflict,
    Unavailable,
    ResourceExhausted,
    Internal,
    Unknown,
}

/// Public retry guidance and a stable machine-readable code for an error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorClassification {
    code: String,
    category: ErrorCategory,
    retryable: bool,
}

impl ErrorClassification {
    pub fn new(code: impl Into<String>, category: ErrorCategory, retryable: bool) -> Self {
        Self {
            code: code.into(),
            category,
            retryable,
        }
    }

    pub fn from_server_code(code: impl Into<String>, retryable: bool) -> Self {
        let code = code.into();
        let category = match code.as_str() {
            "invalid_request" | "invalid_json" | "invalid_content_type" => {
                ErrorCategory::InvalidRequest
            }
            "unauthorized" => ErrorCategory::Authentication,
            "request_conflict" | "precondition_failed" => ErrorCategory::Conflict,
            "unavailable"
            | "durability_unavailable"
            | "write_timeout"
            | "write_outcome_unknown"
            | "writes_unavailable"
            | "configuration_transition"
            | "contention"
            | "winner_limit_exceeded"
            | "leader_unavailable"
            | "snapshot_required" => ErrorCategory::Unavailable,
            "resource_exhausted" | "overloaded" | "payload_too_large" => {
                ErrorCategory::ResourceExhausted
            }
            "data_root_locked"
            | "unsupported_ack_mode"
            | "execution_profile_mismatch"
            | "storage_error"
            | "reconciliation_error"
            | "invariant_violation"
            | "fatal"
            | "task_failed" => ErrorCategory::Internal,
            _ => ErrorCategory::Unknown,
        };
        Self {
            code,
            category,
            retryable,
        }
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub const fn category(&self) -> ErrorCategory {
        self.category
    }

    pub const fn retryable(&self) -> bool {
        self.retryable
    }
}

pub const RECOVERY_ANCHOR_FORMAT_VERSION: u32 = 2;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct LogHash([u8; 32]);

impl LogHash {
    pub const ZERO: Self = Self([0; 32]);

    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn digest(parts: &[&[u8]]) -> Self {
        let mut hasher = Sha256::new();
        for part in parts {
            hasher.update(part);
        }
        Self(hasher.finalize().into())
    }

    pub fn to_hex(self) -> String {
        let mut out = String::with_capacity(64);
        for byte in self.0 {
            out.push(hex_char(byte >> 4));
            out.push(hex_char(byte & 0x0f));
        }
        out
    }

    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 64 {
            return None;
        }

        let mut bytes = [0; 32];
        for (index, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
            bytes[index] = (hex_value(chunk[0])? << 4) | hex_value(chunk[1])?;
        }
        Some(Self(bytes))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogAnchor {
    index: LogIndex,
    hash: LogHash,
}

impl LogAnchor {
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

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StopBinding {
    Unbound,
    Bound {
        successor: SuccessorDescriptor,
        stop_command_hash: LogHash,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConfigurationState {
    Active {
        config_id: ConfigId,
        digest: LogHash,
    },
    Stopped {
        config_id: ConfigId,
        digest: LogHash,
        stop: LogAnchor,
        binding: StopBinding,
    },
}

impl ConfigurationState {
    pub const fn active(config_id: ConfigId, digest: LogHash) -> Self {
        Self::Active { config_id, digest }
    }

    pub const fn stopped(
        config_id: ConfigId,
        digest: LogHash,
        stop: LogAnchor,
        binding: StopBinding,
    ) -> Self {
        Self::Stopped {
            config_id,
            digest,
            stop,
            binding,
        }
    }

    pub const fn config_id(&self) -> ConfigId {
        match self {
            Self::Active { config_id, .. } | Self::Stopped { config_id, .. } => *config_id,
        }
    }

    pub const fn digest(&self) -> LogHash {
        match self {
            Self::Active { digest, .. } | Self::Stopped { digest, .. } => *digest,
        }
    }

    pub const fn stop(&self) -> Option<&LogAnchor> {
        match self {
            Self::Active { .. } => None,
            Self::Stopped { stop, .. } => Some(stop),
        }
    }

    pub const fn is_active(&self) -> bool {
        matches!(self, Self::Active { .. })
    }

    pub fn validate_entry(&self, entry: &LogEntry) -> Result<Self, ConfigurationTransitionError> {
        if entry.recompute_hash() != entry.hash {
            return Err(ConfigurationTransitionError::EntryHashMismatch);
        }
        let change = if entry.entry_type == EntryType::ConfigChange {
            Some(
                ConfigChange::recognize_parts(entry.entry_type, &entry.payload)
                    .map_err(|_| ConfigurationTransitionError::InvalidConfigChange)?,
            )
        } else {
            None
        };

        match (self, change) {
            (
                Self::Active { config_id, digest },
                Some(ConfigChange::Stop {
                    config_id: stop_config_id,
                    config_digest,
                }),
            ) if entry.config_id == *config_id
                && stop_config_id == *config_id
                && (*digest == LogHash::ZERO || config_digest == *digest) =>
            {
                Ok(Self::stopped(
                    *config_id,
                    config_digest,
                    LogAnchor::new(entry.index, entry.hash),
                    StopBinding::Unbound,
                ))
            }
            (Self::Active { config_id, digest }, Some(ConfigChange::BoundStop { successor }))
                if entry.cluster_id == successor.cluster_id
                    && entry.config_id == *config_id
                    && successor.predecessor_config_id == *config_id
                    && successor.predecessor_config_digest == *digest =>
            {
                let stop_command_hash = (ConfigChange::BoundStop {
                    successor: successor.clone(),
                })
                .to_stored_command()
                .hash();
                Ok(Self::Stopped {
                    config_id: *config_id,
                    digest: *digest,
                    stop: LogAnchor::new(entry.index, entry.hash),
                    binding: StopBinding::Bound {
                        successor,
                        stop_command_hash,
                    },
                })
            }
            (Self::Active { config_id, .. }, None) if entry.config_id == *config_id => {
                Ok(self.clone())
            }
            (Self::Active { .. }, _) => Err(ConfigurationTransitionError::ConfigurationMismatch),
            (
                Self::Stopped {
                    config_id: predecessor_id,
                    stop,
                    binding: StopBinding::Unbound,
                    ..
                },
                Some(ConfigChange::ActivationBarrier {
                    config_id,
                    config_digest,
                    stop_slot,
                    prefix_hash,
                }),
            ) if predecessor_id.checked_add(1) == Some(config_id)
                && entry.config_id == config_id
                && stop.index().checked_add(1) == Some(entry.index)
                && entry.prev_hash == stop.hash()
                && stop_slot == stop.index()
                && prefix_hash == stop.hash() =>
            {
                Ok(Self::active(config_id, config_digest))
            }
            (
                Self::Stopped {
                    config_id: predecessor_id,
                    digest: predecessor_digest,
                    stop,
                    binding:
                        StopBinding::Bound {
                            successor: authorized_successor,
                            stop_command_hash: authorized_stop_command_hash,
                        },
                },
                Some(ConfigChange::BoundActivationBarrier {
                    successor,
                    stop_slot,
                    prefix_hash,
                    stop_command_hash,
                }),
            ) if successor.predecessor_config_id == *predecessor_id
                && successor.predecessor_config_digest == *predecessor_digest
                && &successor == authorized_successor
                && entry.cluster_id == successor.cluster_id
                && entry.config_id == successor.config_id
                && stop.index().checked_add(1) == Some(entry.index)
                && entry.prev_hash == stop.hash()
                && stop_slot == stop.index()
                && prefix_hash == stop.hash()
                // Reject a deserialized state whose cached authorization hash
                // does not match its bound successor descriptor.
                && *authorized_stop_command_hash
                    == (ConfigChange::BoundStop {
                        successor: authorized_successor.clone(),
                    })
                    .to_stored_command()
                    .hash()
                && stop_command_hash == *authorized_stop_command_hash =>
            {
                Ok(Self::active(successor.config_id, successor.config_digest))
            }
            (Self::Stopped { .. }, _) => Err(ConfigurationTransitionError::InvalidActivation),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigurationTransitionError {
    EntryHashMismatch,
    InvalidConfigChange,
    ConfigurationMismatch,
    InvalidActivation,
}

impl std::fmt::Display for ConfigurationTransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid configuration transition: {self:?}")
    }
}

impl std::error::Error for ConfigurationTransitionError {}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotIdentity {
    snapshot_id: String,
    digest: LogHash,
    size_bytes: u64,
    executor_fingerprint: LogHash,
}

impl SnapshotIdentity {
    pub fn new(
        snapshot_id: impl Into<String>,
        digest: LogHash,
        size_bytes: u64,
        executor_fingerprint: LogHash,
    ) -> Self {
        Self {
            snapshot_id: snapshot_id.into(),
            digest,
            size_bytes,
            executor_fingerprint,
        }
    }

    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
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

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryAnchor {
    format_version: u32,
    cluster_id: ClusterId,
    epoch: Epoch,
    config_id: ConfigId,
    configuration_state: ConfigurationState,
    recovery_generation: u64,
    compacted: LogAnchor,
    snapshot: SnapshotIdentity,
}

impl RecoveryAnchor {
    pub fn new(
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        configuration_state: ConfigurationState,
        recovery_generation: u64,
        compacted: LogAnchor,
        snapshot: SnapshotIdentity,
    ) -> Self {
        Self {
            format_version: RECOVERY_ANCHOR_FORMAT_VERSION,
            cluster_id: cluster_id.into(),
            epoch,
            config_id: configuration_state.config_id(),
            configuration_state,
            recovery_generation,
            compacted,
            snapshot,
        }
    }

    pub const fn format_version(&self) -> u32 {
        self.format_version
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

    pub const fn configuration_state(&self) -> &ConfigurationState {
        &self.configuration_state
    }

    pub const fn recovery_generation(&self) -> u64 {
        self.recovery_generation
    }

    pub const fn compacted(&self) -> &LogAnchor {
        &self.compacted
    }

    pub const fn snapshot(&self) -> &SnapshotIdentity {
        &self.snapshot
    }

    pub const fn executor_fingerprint(&self) -> LogHash {
        self.snapshot.executor_fingerprint()
    }
}

impl<'de> serde::Deserialize<'de> for RecoveryAnchor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            format_version: u32,
            cluster_id: ClusterId,
            epoch: Epoch,
            config_id: ConfigId,
            configuration_state: ConfigurationState,
            recovery_generation: u64,
            compacted: LogAnchor,
            snapshot: SnapshotIdentity,
        }

        let wire = Wire::deserialize(deserializer)?;
        if wire.format_version != RECOVERY_ANCHOR_FORMAT_VERSION
            || wire.configuration_state.config_id() != wire.config_id
        {
            return Err(serde::de::Error::custom(
                "invalid recovery anchor configuration state",
            ));
        }
        Ok(Self {
            format_version: wire.format_version,
            cluster_id: wire.cluster_id,
            epoch: wire.epoch,
            config_id: wire.config_id,
            configuration_state: wire.configuration_state,
            recovery_generation: wire.recovery_generation,
            compacted: wire.compacted,
            snapshot: wire.snapshot,
        })
    }
}

fn hex_char(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + value - 10) as char,
        _ => unreachable!("nibble out of range"),
    }
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct EntryId {
    pub epoch: Epoch,
    pub index: LogIndex,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum EntryType {
    Command,
    ConfigChange,
    SnapshotBarrier,
    SnapshotPublished,
    Noop,
}

impl EntryType {
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Command => 1,
            Self::ConfigChange => 2,
            Self::SnapshotBarrier => 3,
            Self::SnapshotPublished => 4,
            Self::Noop => 5,
        }
    }

    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Command),
            2 => Some(Self::ConfigChange),
            3 => Some(Self::SnapshotBarrier),
            4 => Some(Self::SnapshotPublished),
            5 => Some(Self::Noop),
            _ => None,
        }
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionProfile {
    #[serde(rename = "sql")]
    Sqlite,
    Graph,
    Kv,
}

impl ExecutionProfile {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite => "sql",
            Self::Graph => "graph",
            Self::Kv => "kv",
        }
    }

    pub const fn wire_id(self) -> u8 {
        match self {
            Self::Sqlite => 1,
            Self::Graph => 2,
            Self::Kv => 3,
        }
    }

    pub const fn from_wire_id(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Sqlite),
            2 => Some(Self::Graph),
            3 => Some(Self::Kv),
            _ => None,
        }
    }
}

impl std::fmt::Display for ExecutionProfile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for ExecutionProfile {
    type Err = ExecutionProfileParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "sql" => Ok(Self::Sqlite),
            "graph" => Ok(Self::Graph),
            "kv" => Ok(Self::Kv),
            _ => Err(ExecutionProfileParseError),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionProfileParseError;

impl std::fmt::Display for ExecutionProfileParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("expected execution profile `sql`, `graph`, or `kv`")
    }
}

impl std::error::Error for ExecutionProfileParseError {}

#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ReadConsistency {
    Local,
    ReadBarrier,
    AppliedIndex(LogIndex),
}

const REPLICATED_COMMAND_MAGIC: &[u8; 4] = b"QCMD";
pub const REPLICATED_COMMAND_FORMAT_VERSION: u16 = 1;
const REPLICATED_COMMAND_HEADER_BYTES: usize = 15;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicatedCommandEnvelope {
    profile: ExecutionProfile,
    command_version: u16,
    request_id: String,
    body: Vec<u8>,
}

impl ReplicatedCommandEnvelope {
    pub fn new(
        profile: ExecutionProfile,
        command_version: u16,
        request_id: impl Into<String>,
        body: Vec<u8>,
    ) -> Result<Self, CommandEnvelopeError> {
        let request_id = request_id.into();
        validate_command_envelope_fields(command_version, &request_id, &body)?;
        Ok(Self {
            profile,
            command_version,
            request_id,
            body,
        })
    }

    pub const fn profile(&self) -> ExecutionProfile {
        self.profile
    }

    pub const fn command_version(&self) -> u16 {
        self.command_version
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn encode(&self) -> Result<Vec<u8>, CommandEnvelopeError> {
        validate_command_envelope_fields(self.command_version, &self.request_id, &self.body)?;
        let request_id_len = u16::try_from(self.request_id.len())
            .map_err(|_| CommandEnvelopeError::RequestIdTooLong)?;
        let body_len =
            u32::try_from(self.body.len()).map_err(|_| CommandEnvelopeError::BodyTooLong)?;
        let capacity = REPLICATED_COMMAND_HEADER_BYTES
            .checked_add(self.request_id.len())
            .and_then(|size| size.checked_add(self.body.len()))
            .ok_or(CommandEnvelopeError::LengthOverflow)?;
        let mut encoded = Vec::with_capacity(capacity);
        encoded.extend_from_slice(REPLICATED_COMMAND_MAGIC);
        encoded.extend_from_slice(&REPLICATED_COMMAND_FORMAT_VERSION.to_be_bytes());
        encoded.push(self.profile.wire_id());
        encoded.extend_from_slice(&self.command_version.to_be_bytes());
        encoded.extend_from_slice(&request_id_len.to_be_bytes());
        encoded.extend_from_slice(&body_len.to_be_bytes());
        encoded.extend_from_slice(self.request_id.as_bytes());
        encoded.extend_from_slice(&self.body);
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, CommandEnvelopeError> {
        if encoded.len() < REPLICATED_COMMAND_HEADER_BYTES {
            return Err(CommandEnvelopeError::Truncated);
        }
        if encoded.get(..4) != Some(REPLICATED_COMMAND_MAGIC) {
            return Err(CommandEnvelopeError::InvalidMagic);
        }
        let format_version = u16::from_be_bytes([encoded[4], encoded[5]]);
        if format_version != REPLICATED_COMMAND_FORMAT_VERSION {
            return Err(CommandEnvelopeError::UnsupportedFormatVersion(
                format_version,
            ));
        }
        let profile = ExecutionProfile::from_wire_id(encoded[6])
            .ok_or(CommandEnvelopeError::UnknownExecutionProfile(encoded[6]))?;
        let command_version = u16::from_be_bytes([encoded[7], encoded[8]]);
        let request_id_len = usize::from(u16::from_be_bytes([encoded[9], encoded[10]]));
        let body_len = usize::try_from(u32::from_be_bytes([
            encoded[11],
            encoded[12],
            encoded[13],
            encoded[14],
        ]))
        .map_err(|_| CommandEnvelopeError::LengthOverflow)?;
        let request_id_end = REPLICATED_COMMAND_HEADER_BYTES
            .checked_add(request_id_len)
            .ok_or(CommandEnvelopeError::LengthOverflow)?;
        let body_end = request_id_end
            .checked_add(body_len)
            .ok_or(CommandEnvelopeError::LengthOverflow)?;
        if encoded.len() < body_end {
            return Err(CommandEnvelopeError::Truncated);
        }
        if encoded.len() != body_end {
            return Err(CommandEnvelopeError::TrailingBytes);
        }
        let request_id = std::str::from_utf8(
            encoded
                .get(REPLICATED_COMMAND_HEADER_BYTES..request_id_end)
                .ok_or(CommandEnvelopeError::Truncated)?,
        )
        .map_err(|_| CommandEnvelopeError::InvalidRequestIdUtf8)?
        .to_owned();
        let body = encoded
            .get(request_id_end..body_end)
            .ok_or(CommandEnvelopeError::Truncated)?
            .to_vec();
        Self::new(profile, command_version, request_id, body)
    }
}

fn validate_command_envelope_fields(
    command_version: u16,
    request_id: &str,
    body: &[u8],
) -> Result<(), CommandEnvelopeError> {
    if command_version == 0 {
        return Err(CommandEnvelopeError::InvalidCommandVersion);
    }
    if request_id.is_empty() {
        return Err(CommandEnvelopeError::EmptyRequestId);
    }
    u16::try_from(request_id.len()).map_err(|_| CommandEnvelopeError::RequestIdTooLong)?;
    u32::try_from(body.len()).map_err(|_| CommandEnvelopeError::BodyTooLong)?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandEnvelopeError {
    InvalidMagic,
    UnsupportedFormatVersion(u16),
    UnknownExecutionProfile(u8),
    InvalidCommandVersion,
    EmptyRequestId,
    RequestIdTooLong,
    BodyTooLong,
    InvalidRequestIdUtf8,
    Truncated,
    LengthOverflow,
    TrailingBytes,
}

impl std::fmt::Display for CommandEnvelopeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMagic => formatter.write_str("replicated command magic is invalid"),
            Self::UnsupportedFormatVersion(version) => {
                write!(
                    formatter,
                    "unsupported replicated command format version {version}"
                )
            }
            Self::UnknownExecutionProfile(profile) => {
                write!(formatter, "unknown execution profile wire id {profile}")
            }
            Self::InvalidCommandVersion => {
                formatter.write_str("replicated command version must be positive")
            }
            Self::EmptyRequestId => formatter.write_str("replicated command request id is empty"),
            Self::RequestIdTooLong => {
                formatter.write_str("replicated command request id exceeds u16 length")
            }
            Self::BodyTooLong => formatter.write_str("replicated command body exceeds u32 length"),
            Self::InvalidRequestIdUtf8 => {
                formatter.write_str("replicated command request id is not UTF-8")
            }
            Self::Truncated => formatter.write_str("replicated command is truncated"),
            Self::LengthOverflow => formatter.write_str("replicated command length overflow"),
            Self::TrailingBytes => formatter.write_str("replicated command has trailing bytes"),
        }
    }
}

impl std::error::Error for CommandEnvelopeError {}

/// Clean-break prefix for the canonical external-effect command envelope.
pub const EXTERNAL_EFFECT_COMMAND_MAGIC: &[u8; 6] = b"QEFX\0\x01";
/// Immutable layout identity for [`ExternalEffectCommand`].
pub const EXTERNAL_EFFECT_COMMAND_FINGERPRINT: [u8; 32] = *b"rhiza:external-effect-command:v1";
pub const MAX_EXTERNAL_EFFECT_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_EXTERNAL_EFFECT_CHUNK_BYTES: usize = 256 * 1024;
pub const MAX_EXTERNAL_EFFECT_CHUNKS: usize = 256;
pub const MAX_EXTERNAL_EFFECT_PROFILE_MANIFEST_BYTES: usize = 512 * 1024;
pub const MAX_EXTERNAL_EFFECT_COMMAND_BYTES: usize = 512 * 1024;
const MAX_EXTERNAL_EFFECT_CLUSTER_ID_BYTES: usize = 256;

const EXTERNAL_EFFECT_CHUNK_DOMAIN: &[u8] = b"rhiza:external-effect:chunk:v1\0";
const EXTERNAL_EFFECT_BUNDLE_DOMAIN: &[u8] = b"rhiza:external-effect:bundle:v1\0";
const EXTERNAL_EFFECT_COMMAND_DOMAIN: &[u8] = b"rhiza:external-effect:command:v1\0";

/// The execution profile which interprets an external effect manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum ExternalEffectProfileKind {
    Sql,
}

/// Profile-owned manifest bytes carried by an external-effect command.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum ExternalEffectProfile {
    Sql { manifest: Vec<u8> },
}

impl ExternalEffectProfile {
    pub fn sql(manifest: Vec<u8>) -> Self {
        Self::Sql { manifest }
    }

    pub const fn kind(&self) -> ExternalEffectProfileKind {
        match self {
            Self::Sql { .. } => ExternalEffectProfileKind::Sql,
        }
    }

    pub fn manifest(&self) -> &[u8] {
        match self {
            Self::Sql { manifest } => manifest,
        }
    }
}

/// One ordered, content-addressed chunk in an external effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ExternalEffectChunk {
    digest: LogHash,
    encoded_len: u32,
}

impl ExternalEffectChunk {
    pub fn new(digest: LogHash, encoded_len: u32) -> Result<Self, ExternalEffectCommandError> {
        let chunk = Self {
            digest,
            encoded_len,
        };
        validate_external_effect_chunk(&chunk)?;
        Ok(chunk)
    }

    pub fn for_bytes(bytes: &[u8]) -> Result<Self, ExternalEffectCommandError> {
        let encoded_len =
            u32::try_from(bytes.len()).map_err(|_| ExternalEffectCommandError::ChunkTooLarge)?;
        Self::new(ExternalEffectCommand::chunk_digest(bytes), encoded_len)
    }

    pub const fn digest(&self) -> LogHash {
        self.digest
    }

    pub const fn encoded_len(&self) -> u32 {
        self.encoded_len
    }
}

/// The sole canonical replicated commitment to externally stored effect bytes.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ExternalEffectCommand {
    cluster_id: ClusterId,
    epoch: Epoch,
    config_id: ConfigId,
    config_digest: LogHash,
    intended_slot: LogIndex,
    prev_hash: LogHash,
    profile: ExternalEffectProfile,
    total_effect_bytes: u32,
    effect_digest: LogHash,
    chunks: Vec<ExternalEffectChunk>,
}

impl ExternalEffectCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        intended_slot: LogIndex,
        prev_hash: LogHash,
        profile: ExternalEffectProfile,
        chunks: Vec<ExternalEffectChunk>,
    ) -> Result<Self, ExternalEffectCommandError> {
        let (total_effect_bytes, effect_digest) = Self::effect_digest(&chunks)?;
        let command = Self {
            cluster_id: cluster_id.into(),
            epoch,
            config_id,
            config_digest,
            intended_slot,
            prev_hash,
            profile,
            total_effect_bytes,
            effect_digest,
            chunks,
        };
        command.validate()?;
        Ok(command)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_profile_bytes_and_chunks(
        cluster_id: impl Into<ClusterId>,
        epoch: Epoch,
        config_id: ConfigId,
        config_digest: LogHash,
        intended_slot: LogIndex,
        prev_hash: LogHash,
        profile: ExternalEffectProfile,
        chunks: &[Vec<u8>],
    ) -> Result<Self, ExternalEffectCommandError> {
        let chunks = chunks
            .iter()
            .map(|chunk| ExternalEffectChunk::for_bytes(chunk))
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(
            cluster_id,
            epoch,
            config_id,
            config_digest,
            intended_slot,
            prev_hash,
            profile,
            chunks,
        )
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
    pub const fn intended_slot(&self) -> LogIndex {
        self.intended_slot
    }
    pub const fn prev_hash(&self) -> LogHash {
        self.prev_hash
    }
    pub const fn profile(&self) -> &ExternalEffectProfile {
        &self.profile
    }
    pub const fn total_effect_bytes(&self) -> u32 {
        self.total_effect_bytes
    }
    pub const fn effect_digest_value(&self) -> LogHash {
        self.effect_digest
    }
    pub fn chunks(&self) -> &[ExternalEffectChunk] {
        &self.chunks
    }

    pub fn chunk_digest(bytes: &[u8]) -> LogHash {
        LogHash::digest(&[
            EXTERNAL_EFFECT_CHUNK_DOMAIN,
            &(bytes.len() as u64).to_be_bytes(),
            bytes,
        ])
    }

    pub fn effect_digest(
        chunks: &[ExternalEffectChunk],
    ) -> Result<(u32, LogHash), ExternalEffectCommandError> {
        validate_external_effect_chunks(chunks)?;
        let total = chunks.iter().try_fold(0usize, |total, chunk| {
            total
                .checked_add(chunk.encoded_len as usize)
                .ok_or(ExternalEffectCommandError::EffectTooLarge)
        })?;
        if total > MAX_EXTERNAL_EFFECT_BYTES {
            return Err(ExternalEffectCommandError::EffectTooLarge);
        }
        let mut encoded = Vec::with_capacity(8 + chunks.len() * 44);
        encoded.extend_from_slice(&(chunks.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&(total as u64).to_be_bytes());
        for (ordinal, chunk) in chunks.iter().enumerate() {
            encoded.extend_from_slice(&(ordinal as u64).to_be_bytes());
            encoded.extend_from_slice(&chunk.encoded_len.to_be_bytes());
            encoded.extend_from_slice(chunk.digest.as_bytes());
        }
        Ok((
            u32::try_from(total).map_err(|_| ExternalEffectCommandError::EffectTooLarge)?,
            LogHash::digest(&[EXTERNAL_EFFECT_BUNDLE_DOMAIN, &encoded]),
        ))
    }

    pub fn command_digest(&self) -> Result<LogHash, ExternalEffectCommandError> {
        Ok(LogHash::digest(&[
            EXTERNAL_EFFECT_COMMAND_DOMAIN,
            &self.encode()?,
        ]))
    }

    pub fn encode(&self) -> Result<Vec<u8>, ExternalEffectCommandError> {
        self.validate()?;
        let body = postcard::to_allocvec(self)
            .map_err(|error| ExternalEffectCommandError::Encode(error.to_string()))?;
        let mut encoded = Vec::with_capacity(
            EXTERNAL_EFFECT_COMMAND_MAGIC.len()
                + EXTERNAL_EFFECT_COMMAND_FINGERPRINT.len()
                + body.len(),
        );
        encoded.extend_from_slice(EXTERNAL_EFFECT_COMMAND_MAGIC);
        encoded.extend_from_slice(&EXTERNAL_EFFECT_COMMAND_FINGERPRINT);
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, ExternalEffectCommandError> {
        if encoded.len() > MAX_EXTERNAL_EFFECT_COMMAND_BYTES {
            return Err(ExternalEffectCommandError::EncodedTooLarge);
        }
        let body = encoded
            .strip_prefix(EXTERNAL_EFFECT_COMMAND_MAGIC)
            .ok_or(ExternalEffectCommandError::InvalidMagic)?
            .strip_prefix(&EXTERNAL_EFFECT_COMMAND_FINGERPRINT)
            .ok_or(ExternalEffectCommandError::InvalidFingerprint)?;
        if body.is_empty() {
            return Err(ExternalEffectCommandError::EmptyBody);
        }
        let command: Self = postcard::from_bytes(body)
            .map_err(|error| ExternalEffectCommandError::Decode(error.to_string()))?;
        command.validate()?;
        let canonical = postcard::to_allocvec(&command)
            .map_err(|error| ExternalEffectCommandError::Encode(error.to_string()))?;
        if canonical.as_slice() != body {
            return Err(ExternalEffectCommandError::NonCanonical);
        }
        Ok(command)
    }

    pub fn validate(&self) -> Result<(), ExternalEffectCommandError> {
        if self.cluster_id.is_empty()
            || self.cluster_id.len() > MAX_EXTERNAL_EFFECT_CLUSTER_ID_BYTES
        {
            return Err(ExternalEffectCommandError::EmptyClusterId);
        }
        if self.intended_slot == 0 {
            return Err(ExternalEffectCommandError::InvalidIntendedSlot);
        }
        if self.profile.manifest().is_empty() {
            return Err(ExternalEffectCommandError::EmptyProfileManifest);
        }
        if self.profile.manifest().len() > MAX_EXTERNAL_EFFECT_PROFILE_MANIFEST_BYTES {
            return Err(ExternalEffectCommandError::ProfileManifestTooLarge);
        }
        let (total_effect_bytes, effect_digest) = Self::effect_digest(&self.chunks)?;
        if self.total_effect_bytes != total_effect_bytes || self.effect_digest != effect_digest {
            return Err(ExternalEffectCommandError::EffectDigestMismatch);
        }
        let body = postcard::to_allocvec(self)
            .map_err(|error| ExternalEffectCommandError::Encode(error.to_string()))?;
        if EXTERNAL_EFFECT_COMMAND_MAGIC.len()
            + EXTERNAL_EFFECT_COMMAND_FINGERPRINT.len()
            + body.len()
            > MAX_EXTERNAL_EFFECT_COMMAND_BYTES
        {
            return Err(ExternalEffectCommandError::EncodedTooLarge);
        }
        Ok(())
    }
}

fn validate_external_effect_chunk(
    chunk: &ExternalEffectChunk,
) -> Result<(), ExternalEffectCommandError> {
    if chunk.digest == LogHash::ZERO {
        return Err(ExternalEffectCommandError::ZeroChunkDigest);
    }
    if chunk.encoded_len == 0 || chunk.encoded_len as usize > MAX_EXTERNAL_EFFECT_CHUNK_BYTES {
        return Err(ExternalEffectCommandError::ChunkTooLarge);
    }
    Ok(())
}

fn validate_external_effect_chunks(
    chunks: &[ExternalEffectChunk],
) -> Result<(), ExternalEffectCommandError> {
    if chunks.is_empty() || chunks.len() > MAX_EXTERNAL_EFFECT_CHUNKS {
        return Err(ExternalEffectCommandError::InvalidChunkCount);
    }
    for chunk in chunks {
        validate_external_effect_chunk(chunk)?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExternalEffectCommandError {
    InvalidMagic,
    InvalidFingerprint,
    EmptyBody,
    EmptyClusterId,
    InvalidIntendedSlot,
    EmptyProfileManifest,
    ProfileManifestTooLarge,
    InvalidChunkCount,
    ZeroChunkDigest,
    ChunkTooLarge,
    EffectTooLarge,
    EffectDigestMismatch,
    EncodedTooLarge,
    Encode(String),
    Decode(String),
    NonCanonical,
}

impl std::fmt::Display for ExternalEffectCommandError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMagic => formatter.write_str("external effect command magic is invalid"),
            Self::InvalidFingerprint => {
                formatter.write_str("external effect command fingerprint is invalid")
            }
            Self::EmptyBody => formatter.write_str("external effect command body is empty"),
            Self::EmptyClusterId => {
                formatter.write_str("external effect command cluster id is empty")
            }
            Self::InvalidIntendedSlot => {
                formatter.write_str("external effect command intended slot must be positive")
            }
            Self::EmptyProfileManifest => {
                formatter.write_str("external effect command profile manifest is empty")
            }
            Self::ProfileManifestTooLarge => formatter
                .write_str("external effect command profile manifest exceeds its byte bound"),
            Self::InvalidChunkCount => {
                formatter.write_str("external effect command chunk count is outside its bound")
            }
            Self::ZeroChunkDigest => {
                formatter.write_str("external effect command chunk digest is zero")
            }
            Self::ChunkTooLarge => {
                formatter.write_str("external effect command chunk length is outside its bound")
            }
            Self::EffectTooLarge => {
                formatter.write_str("external effect command exceeds its byte bound")
            }
            Self::EffectDigestMismatch => {
                formatter.write_str("external effect command effect digest does not match chunks")
            }
            Self::EncodedTooLarge => formatter
                .write_str("external effect command exceeds the replicated command byte bound"),
            Self::Encode(error) => {
                write!(formatter, "external effect command encode failed: {error}")
            }
            Self::Decode(error) => {
                write!(formatter, "external effect command decode failed: {error}")
            }
            Self::NonCanonical => {
                formatter.write_str("external effect command is not canonically encoded")
            }
        }
    }
}

impl std::error::Error for ExternalEffectCommandError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum CommandKind {
    Deterministic,
    ReadBarrier,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Command {
    kind: CommandKind,
    payload: Vec<u8>,
}

impl Command {
    pub fn new(kind: CommandKind, payload: Vec<u8>) -> Self {
        Self { kind, payload }
    }

    pub const fn kind(&self) -> CommandKind {
        self.kind
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct StoredCommand {
    pub entry_type: EntryType,
    pub payload: Vec<u8>,
}

impl StoredCommand {
    pub fn new(entry_type: EntryType, payload: Vec<u8>) -> Self {
        Self {
            entry_type,
            payload,
        }
    }

    pub fn hash(&self) -> LogHash {
        let entry_type = [self.entry_type.as_u8()];
        LogHash::digest(&[b"rhiza-command-v2", &entry_type, &self.payload])
    }
}

const CONFIG_CHANGE_MAGIC: &[u8; 4] = b"QCFG";

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct SuccessorDescriptor {
    cluster_id: ClusterId,
    predecessor_config_id: ConfigId,
    predecessor_config_digest: LogHash,
    config_id: ConfigId,
    config_digest: LogHash,
    members: Vec<NodeId>,
}

impl<'de> serde::Deserialize<'de> for SuccessorDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            cluster_id: ClusterId,
            predecessor_config_id: ConfigId,
            predecessor_config_digest: LogHash,
            config_id: ConfigId,
            config_digest: LogHash,
            members: Vec<NodeId>,
        }

        let wire = Wire::deserialize(deserializer)?;
        if !wire.members.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(serde::de::Error::custom(
                "successor members are not canonical",
            ));
        }
        let encoded_digest = wire.config_digest;
        let descriptor = Self::new(
            wire.cluster_id,
            wire.predecessor_config_id,
            wire.predecessor_config_digest,
            wire.config_id,
            wire.members,
        )
        .map_err(serde::de::Error::custom)?;
        if descriptor.config_digest != encoded_digest {
            return Err(serde::de::Error::custom(
                "successor membership digest mismatch",
            ));
        }
        Ok(descriptor)
    }
}

impl SuccessorDescriptor {
    pub fn new(
        cluster_id: impl Into<ClusterId>,
        predecessor_config_id: ConfigId,
        predecessor_config_digest: LogHash,
        config_id: ConfigId,
        members: Vec<NodeId>,
    ) -> Result<Self, ConfigChangeDecodeError> {
        let cluster_id = cluster_id.into();
        if cluster_id.is_empty() || cluster_id.len() > usize::from(u16::MAX) {
            return Err(ConfigChangeDecodeError);
        }
        if predecessor_config_id.checked_add(1) != Some(config_id) {
            return Err(ConfigChangeDecodeError);
        }
        let mut canonical = members;
        let original_len = canonical.len();
        canonical.sort();
        canonical.dedup();
        if canonical.len() != original_len
            || !(3..=7).contains(&canonical.len())
            || canonical
                .iter()
                .any(|member| member.is_empty() || member.len() > usize::from(u16::MAX))
        {
            return Err(ConfigChangeDecodeError);
        }
        let config_digest = canonical_membership_digest(&canonical)?;
        Ok(Self {
            cluster_id,
            predecessor_config_id,
            predecessor_config_digest,
            config_id,
            config_digest,
            members: canonical,
        })
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub const fn predecessor_config_id(&self) -> ConfigId {
        self.predecessor_config_id
    }

    pub const fn predecessor_config_digest(&self) -> LogHash {
        self.predecessor_config_digest
    }

    pub const fn config_id(&self) -> ConfigId {
        self.config_id
    }

    pub const fn digest(&self) -> LogHash {
        self.config_digest
    }

    pub fn members(&self) -> &[NodeId] {
        &self.members
    }
}

pub fn canonical_membership_digest(members: &[NodeId]) -> Result<LogHash, ConfigChangeDecodeError> {
    if !(3..=7).contains(&members.len())
        || members
            .iter()
            .any(|member| member.is_empty() || member.len() > usize::from(u16::MAX))
        || !members.windows(2).all(|pair| pair[0] < pair[1])
    {
        return Err(ConfigChangeDecodeError);
    }
    let encoded_len = 14 + members.len() * 8 + members.iter().map(String::len).sum::<usize>();
    let mut encoded = Vec::with_capacity(encoded_len);
    encoded.extend_from_slice(b"QMEM\0\x01");
    encoded.extend_from_slice(&(members.len() as u64).to_be_bytes());
    for member in members {
        encoded.extend_from_slice(&(member.len() as u64).to_be_bytes());
        encoded.extend_from_slice(member.as_bytes());
    }
    Ok(LogHash::digest(&[&encoded]))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigChange {
    Stop {
        config_id: ConfigId,
        config_digest: LogHash,
    },
    ActivationBarrier {
        config_id: ConfigId,
        config_digest: LogHash,
        stop_slot: LogIndex,
        prefix_hash: LogHash,
    },
    BoundStop {
        successor: SuccessorDescriptor,
    },
    BoundActivationBarrier {
        successor: SuccessorDescriptor,
        stop_slot: LogIndex,
        prefix_hash: LogHash,
        stop_command_hash: LogHash,
    },
}

impl ConfigChange {
    pub const fn stop(config_id: ConfigId, config_digest: LogHash) -> Self {
        Self::Stop {
            config_id,
            config_digest,
        }
    }

    pub const fn activation_barrier(
        config_id: ConfigId,
        config_digest: LogHash,
        stop_slot: LogIndex,
        prefix_hash: LogHash,
    ) -> Self {
        Self::ActivationBarrier {
            config_id,
            config_digest,
            stop_slot,
            prefix_hash,
        }
    }

    pub fn bound_stop(
        cluster_id: impl Into<ClusterId>,
        predecessor_config_id: ConfigId,
        predecessor_config_digest: LogHash,
        successor_config_id: ConfigId,
        successor_members: Vec<NodeId>,
    ) -> Result<Self, ConfigChangeDecodeError> {
        Ok(Self::BoundStop {
            successor: SuccessorDescriptor::new(
                cluster_id,
                predecessor_config_id,
                predecessor_config_digest,
                successor_config_id,
                successor_members,
            )?,
        })
    }

    pub const fn bound_activation_barrier(
        successor: SuccessorDescriptor,
        stop_slot: LogIndex,
        prefix_hash: LogHash,
        stop_command_hash: LogHash,
    ) -> Self {
        Self::BoundActivationBarrier {
            successor,
            stop_slot,
            prefix_hash,
            stop_command_hash,
        }
    }

    pub const fn successor(&self) -> Option<&SuccessorDescriptor> {
        match self {
            Self::BoundStop { successor } | Self::BoundActivationBarrier { successor, .. } => {
                Some(successor)
            }
            _ => None,
        }
    }

    pub fn to_stored_command(&self) -> StoredCommand {
        let mut payload = Vec::with_capacity(87);
        payload.extend_from_slice(CONFIG_CHANGE_MAGIC);
        match self {
            Self::Stop {
                config_id,
                config_digest,
            } => {
                payload.push(1);
                payload.extend_from_slice(&config_id.to_be_bytes());
                payload.extend_from_slice(config_digest.as_bytes());
            }
            Self::ActivationBarrier {
                config_id,
                config_digest,
                stop_slot,
                prefix_hash,
            } => {
                payload.push(2);
                payload.extend_from_slice(&config_id.to_be_bytes());
                payload.extend_from_slice(config_digest.as_bytes());
                payload.extend_from_slice(&stop_slot.to_be_bytes());
                payload.extend_from_slice(prefix_hash.as_bytes());
            }
            Self::BoundStop { successor } => {
                payload.push(3);
                encode_successor(&mut payload, successor);
            }
            Self::BoundActivationBarrier {
                successor,
                stop_slot,
                prefix_hash,
                stop_command_hash,
            } => {
                payload.push(4);
                encode_successor(&mut payload, successor);
                payload.extend_from_slice(&stop_slot.to_be_bytes());
                payload.extend_from_slice(prefix_hash.as_bytes());
                payload.extend_from_slice(stop_command_hash.as_bytes());
            }
        }
        StoredCommand::new(EntryType::ConfigChange, payload)
    }

    pub fn recognize(command: &StoredCommand) -> Result<Self, ConfigChangeDecodeError> {
        Self::recognize_parts(command.entry_type, &command.payload)
    }

    pub fn recognize_parts(
        entry_type: EntryType,
        payload: &[u8],
    ) -> Result<Self, ConfigChangeDecodeError> {
        if entry_type != EntryType::ConfigChange {
            return Err(ConfigChangeDecodeError);
        }
        let bytes = payload;
        if bytes.get(..4) != Some(CONFIG_CHANGE_MAGIC) {
            return Err(ConfigChangeDecodeError);
        }
        let kind = *bytes.get(4).ok_or(ConfigChangeDecodeError)?;
        if matches!(kind, 3 | 4) {
            let mut cursor = 5;
            let successor = decode_successor(bytes, &mut cursor)?;
            let change = match kind {
                3 => Self::BoundStop { successor },
                4 => Self::BoundActivationBarrier {
                    successor,
                    stop_slot: read_config_u64_at(bytes, &mut cursor)?,
                    prefix_hash: read_config_hash_at(bytes, &mut cursor)?,
                    stop_command_hash: read_config_hash_at(bytes, &mut cursor)?,
                },
                _ => return Err(ConfigChangeDecodeError),
            };
            if cursor != bytes.len() {
                return Err(ConfigChangeDecodeError);
            }
            return Ok(change);
        }
        let config_id = read_config_u64(bytes, 5)?;
        let config_digest = read_config_hash(bytes, 13)?;
        match kind {
            1 if bytes.len() == 45 => Ok(Self::stop(config_id, config_digest)),
            2 if bytes.len() == 85 => Ok(Self::activation_barrier(
                config_id,
                config_digest,
                read_config_u64(bytes, 45)?,
                read_config_hash(bytes, 53)?,
            )),
            _ => Err(ConfigChangeDecodeError),
        }
    }

    pub const fn binding(&self) -> (ConfigId, LogHash) {
        match self {
            Self::Stop {
                config_id,
                config_digest,
            }
            | Self::ActivationBarrier {
                config_id,
                config_digest,
                ..
            } => (*config_id, *config_digest),
            Self::BoundStop { successor } => (
                successor.predecessor_config_id,
                successor.predecessor_config_digest,
            ),
            Self::BoundActivationBarrier { successor, .. } => {
                (successor.config_id, successor.config_digest)
            }
        }
    }
}

fn encode_successor(out: &mut Vec<u8>, successor: &SuccessorDescriptor) {
    let cluster = successor.cluster_id.as_bytes();
    let encoded_len = 83
        + cluster.len()
        + successor
            .members
            .iter()
            .map(|member| 2 + member.len())
            .sum::<usize>();
    out.reserve(encoded_len);
    let cluster_length =
        u16::try_from(cluster.len()).expect("validated successor cluster length fits u16");
    out.extend_from_slice(&cluster_length.to_be_bytes());
    out.extend_from_slice(cluster);
    out.extend_from_slice(&successor.predecessor_config_id.to_be_bytes());
    out.extend_from_slice(successor.predecessor_config_digest.as_bytes());
    out.extend_from_slice(&successor.config_id.to_be_bytes());
    out.extend_from_slice(successor.config_digest.as_bytes());
    let member_count =
        u8::try_from(successor.members.len()).expect("validated successor member count fits u8");
    out.push(member_count);
    for member in &successor.members {
        let member_length =
            u16::try_from(member.len()).expect("validated successor member length fits u16");
        out.extend_from_slice(&member_length.to_be_bytes());
        out.extend_from_slice(member.as_bytes());
    }
}

fn decode_successor(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<SuccessorDescriptor, ConfigChangeDecodeError> {
    let cluster_id = read_config_string(bytes, cursor)?;
    let predecessor_config_id = read_config_u64_at(bytes, cursor)?;
    let predecessor_config_digest = read_config_hash_at(bytes, cursor)?;
    let config_id = read_config_u64_at(bytes, cursor)?;
    let encoded_digest = read_config_hash_at(bytes, cursor)?;
    let count = *bytes.get(*cursor).ok_or(ConfigChangeDecodeError)? as usize;
    if !(3..=7).contains(&count) {
        return Err(ConfigChangeDecodeError);
    }
    *cursor += 1;
    let members = (0..count)
        .map(|_| read_config_string(bytes, cursor))
        .collect::<Result<Vec<_>, _>>()?;
    if !members.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(ConfigChangeDecodeError);
    }
    let descriptor = SuccessorDescriptor::new(
        cluster_id,
        predecessor_config_id,
        predecessor_config_digest,
        config_id,
        members,
    )?;
    if descriptor.config_digest != encoded_digest {
        return Err(ConfigChangeDecodeError);
    }
    Ok(descriptor)
}

fn read_config_string(bytes: &[u8], cursor: &mut usize) -> Result<String, ConfigChangeDecodeError> {
    let length = read_config_u16(bytes, *cursor)? as usize;
    let value_start = cursor.checked_add(2).ok_or(ConfigChangeDecodeError)?;
    let value_end = value_start
        .checked_add(length)
        .ok_or(ConfigChangeDecodeError)?;
    let value = bytes
        .get(value_start..value_end)
        .ok_or(ConfigChangeDecodeError)?;
    *cursor = value_end;
    std::str::from_utf8(value)
        .map(str::to_owned)
        .map_err(|_| ConfigChangeDecodeError)
}

fn read_config_u64_at(bytes: &[u8], cursor: &mut usize) -> Result<u64, ConfigChangeDecodeError> {
    let value = read_config_u64(bytes, *cursor)?;
    *cursor = cursor.checked_add(8).ok_or(ConfigChangeDecodeError)?;
    Ok(value)
}

fn read_config_hash_at(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<LogHash, ConfigChangeDecodeError> {
    let value = read_config_hash(bytes, *cursor)?;
    *cursor = cursor.checked_add(32).ok_or(ConfigChangeDecodeError)?;
    Ok(value)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigChangeDecodeError;

impl std::fmt::Display for ConfigChangeDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid ConfigChange payload")
    }
}

impl std::error::Error for ConfigChangeDecodeError {}

fn read_config_u16(bytes: &[u8], offset: usize) -> Result<u16, ConfigChangeDecodeError> {
    let end = offset.checked_add(2).ok_or(ConfigChangeDecodeError)?;
    let bytes = bytes.get(offset..end).ok_or(ConfigChangeDecodeError)?;
    Ok(u16::from_be_bytes(
        bytes.try_into().expect("u16 slice length"),
    ))
}

fn read_config_u64(bytes: &[u8], offset: usize) -> Result<u64, ConfigChangeDecodeError> {
    let end = offset.checked_add(8).ok_or(ConfigChangeDecodeError)?;
    let bytes = bytes.get(offset..end).ok_or(ConfigChangeDecodeError)?;
    Ok(u64::from_be_bytes(
        bytes.try_into().expect("u64 slice length"),
    ))
}

fn read_config_hash(bytes: &[u8], offset: usize) -> Result<LogHash, ConfigChangeDecodeError> {
    let end = offset.checked_add(32).ok_or(ConfigChangeDecodeError)?;
    let bytes: [u8; 32] = bytes
        .get(offset..end)
        .ok_or(ConfigChangeDecodeError)?
        .try_into()
        .expect("hash slice length");
    Ok(LogHash::from_bytes(bytes))
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct LogEntry {
    pub cluster_id: ClusterId,
    pub epoch: Epoch,
    pub config_id: ConfigId,
    pub index: LogIndex,
    pub entry_type: EntryType,
    pub payload: Vec<u8>,
    pub prev_hash: LogHash,
    pub hash: LogHash,
}

impl LogEntry {
    pub fn calculate_hash(
        cluster_id: &str,
        index: LogIndex,
        epoch: Epoch,
        config_id: ConfigId,
        entry_type: EntryType,
        prev_hash: LogHash,
        payload: &[u8],
    ) -> LogHash {
        let cluster_length = (cluster_id.len() as u64).to_be_bytes();
        let index = index.to_be_bytes();
        let epoch = epoch.to_be_bytes();
        let config_id = config_id.to_be_bytes();
        let entry_type = [entry_type.as_u8()];
        let payload_hash = LogHash::digest(&[payload]);
        LogHash::digest(&[
            b"rhiza-log-entry-v3\0",
            &cluster_length,
            cluster_id.as_bytes(),
            &index,
            &epoch,
            &config_id,
            &entry_type,
            prev_hash.as_bytes(),
            payload_hash.as_bytes(),
        ])
    }

    pub fn recompute_hash(&self) -> LogHash {
        Self::calculate_hash(
            &self.cluster_id,
            self.index,
            self.epoch,
            self.config_id,
            self.entry_type,
            self.prev_hash,
            &self.payload,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotManifest {
    snapshot_id: String,
    cluster_id: ClusterId,
    config_id: ConfigId,
    configuration_state: ConfigurationState,
    epoch: Epoch,
    index: LogIndex,
    applied_hash: LogHash,
    schema_version: u64,
    created_by: NodeId,
    executor_fingerprint: LogHash,
}

impl SnapshotManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_id: impl Into<ClusterId>,
        configuration_state: ConfigurationState,
        epoch: Epoch,
        index: LogIndex,
        applied_hash: LogHash,
        schema_version: u64,
        created_by: impl Into<NodeId>,
        executor_fingerprint: LogHash,
    ) -> Self {
        Self {
            snapshot_id: format!("snapshot-{index:015}"),
            cluster_id: cluster_id.into(),
            config_id: configuration_state.config_id(),
            configuration_state,
            epoch,
            index,
            applied_hash,
            schema_version,
            created_by: created_by.into(),
            executor_fingerprint,
        }
    }

    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
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

    pub const fn configuration_state(&self) -> &ConfigurationState {
        &self.configuration_state
    }

    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    pub fn created_by(&self) -> &str {
        &self.created_by
    }

    pub const fn index(&self) -> LogIndex {
        self.index
    }

    pub const fn snapshot_index(&self) -> LogIndex {
        self.index
    }

    pub const fn applied_hash(&self) -> LogHash {
        self.applied_hash
    }

    pub const fn executor_fingerprint(&self) -> LogHash {
        self.executor_fingerprint
    }
}

impl<'de> serde::Deserialize<'de> for SnapshotManifest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            snapshot_id: String,
            cluster_id: ClusterId,
            config_id: ConfigId,
            configuration_state: ConfigurationState,
            epoch: Epoch,
            index: LogIndex,
            applied_hash: LogHash,
            schema_version: u64,
            created_by: NodeId,
            executor_fingerprint: LogHash,
        }

        let wire = Wire::deserialize(deserializer)?;
        if wire.configuration_state.config_id() != wire.config_id {
            return Err(serde::de::Error::custom(
                "snapshot configuration state does not match config_id",
            ));
        }
        Ok(Self {
            snapshot_id: wire.snapshot_id,
            cluster_id: wire.cluster_id,
            config_id: wire.config_id,
            configuration_state: wire.configuration_state,
            epoch: wire.epoch,
            index: wire.index,
            applied_hash: wire.applied_hash,
            schema_version: wire.schema_version,
            created_by: wire.created_by,
            executor_fingerprint: wire.executor_fingerprint,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    manifest: SnapshotManifest,
    db_bytes: Vec<u8>,
}

impl Snapshot {
    pub fn new(manifest: SnapshotManifest, db_bytes: Vec<u8>) -> Self {
        Self { manifest, db_bytes }
    }

    pub const fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }

    pub fn db_bytes(&self) -> &[u8] {
        &self.db_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::{
        read_config_hash, read_config_hash_at, read_config_string, read_config_u16,
        read_config_u64, read_config_u64_at, CommandEnvelopeError, ConfigChangeDecodeError,
        ErrorCategory, ErrorClassification, ExecutionProfile, ExecutionProfileParseError,
        ExternalEffectCommand, ExternalEffectCommandError, ReplicatedCommandEnvelope,
        MAX_EXTERNAL_EFFECT_COMMAND_BYTES,
    };

    #[test]
    fn server_error_codes_map_to_categories_and_preserve_unknown_values() {
        let cases = [
            ("invalid_request", ErrorCategory::InvalidRequest, false),
            ("invalid_json", ErrorCategory::InvalidRequest, false),
            ("invalid_content_type", ErrorCategory::InvalidRequest, false),
            ("unauthorized", ErrorCategory::Authentication, false),
            ("request_conflict", ErrorCategory::Conflict, false),
            ("precondition_failed", ErrorCategory::Conflict, false),
            ("unavailable", ErrorCategory::Unavailable, true),
            ("durability_unavailable", ErrorCategory::Unavailable, true),
            ("write_timeout", ErrorCategory::Unavailable, true),
            ("write_outcome_unknown", ErrorCategory::Unavailable, true),
            ("writes_unavailable", ErrorCategory::Unavailable, true),
            ("configuration_transition", ErrorCategory::Unavailable, true),
            ("contention", ErrorCategory::Unavailable, true),
            ("winner_limit_exceeded", ErrorCategory::Unavailable, true),
            ("leader_unavailable", ErrorCategory::Unavailable, true),
            ("snapshot_required", ErrorCategory::Unavailable, false),
            ("resource_exhausted", ErrorCategory::ResourceExhausted, true),
            ("overloaded", ErrorCategory::ResourceExhausted, true),
            ("payload_too_large", ErrorCategory::ResourceExhausted, false),
            ("data_root_locked", ErrorCategory::Internal, false),
            ("unsupported_ack_mode", ErrorCategory::Internal, false),
            ("execution_profile_mismatch", ErrorCategory::Internal, false),
            ("storage_error", ErrorCategory::Internal, false),
            ("reconciliation_error", ErrorCategory::Internal, false),
            ("invariant_violation", ErrorCategory::Internal, false),
            ("fatal", ErrorCategory::Internal, false),
            ("task_failed", ErrorCategory::Internal, false),
            ("future_code", ErrorCategory::Unknown, true),
        ];

        for (code, category, retryable) in cases {
            let classification = ErrorClassification::from_server_code(code, retryable);

            assert_eq!(classification.code(), code);
            assert_eq!(classification.category(), category);
            assert_eq!(classification.retryable(), retryable);
        }
    }

    #[test]
    fn execution_profile_has_stable_text_and_wire_ids() {
        assert_eq!(ExecutionProfile::Sqlite.as_str(), "sql");
        assert_eq!(ExecutionProfile::Graph.as_str(), "graph");
        assert_eq!(ExecutionProfile::Kv.as_str(), "kv");
        assert_eq!(ExecutionProfile::Sqlite.to_string(), "sql");
        assert_eq!(ExecutionProfile::Graph.to_string(), "graph");
        assert_eq!(ExecutionProfile::Kv.to_string(), "kv");
        assert_eq!("sql".parse(), Ok(ExecutionProfile::Sqlite));
        assert_eq!("graph".parse(), Ok(ExecutionProfile::Graph));
        assert_eq!("kv".parse(), Ok(ExecutionProfile::Kv));
        assert_eq!(
            "sqlite".parse::<ExecutionProfile>(),
            Err(ExecutionProfileParseError)
        );
        assert_eq!(
            serde_json::to_value(ExecutionProfile::Sqlite).unwrap(),
            serde_json::json!("sql")
        );
        assert_eq!(
            serde_json::from_value::<ExecutionProfile>(serde_json::json!("sql")).unwrap(),
            ExecutionProfile::Sqlite
        );
        assert!(serde_json::from_value::<ExecutionProfile>(serde_json::json!("sqlite")).is_err());
        assert_eq!(
            "ladybug".parse::<ExecutionProfile>(),
            Err(ExecutionProfileParseError)
        );
        assert_eq!(ExecutionProfile::Sqlite.wire_id(), 1);
        assert_eq!(ExecutionProfile::Graph.wire_id(), 2);
        assert_eq!(ExecutionProfile::Kv.wire_id(), 3);
        assert_eq!(
            ExecutionProfile::from_wire_id(1),
            Some(ExecutionProfile::Sqlite)
        );
        assert_eq!(
            ExecutionProfile::from_wire_id(2),
            Some(ExecutionProfile::Graph)
        );
        assert_eq!(
            ExecutionProfile::from_wire_id(3),
            Some(ExecutionProfile::Kv)
        );
        assert_eq!(ExecutionProfile::from_wire_id(0), None);
    }

    #[test]
    fn replicated_command_envelope_uses_a_stable_explicit_codec() {
        let envelope = ReplicatedCommandEnvelope::new(
            ExecutionProfile::Graph,
            3,
            "req-1",
            vec![0xaa, 0xbb, 0xcc],
        )
        .unwrap();
        let encoded = envelope.encode().unwrap();

        assert_eq!(
            encoded,
            [
                b'Q', b'C', b'M', b'D', 0, 1, 2, 0, 3, 0, 5, 0, 0, 0, 3, b'r', b'e', b'q', b'-',
                b'1', 0xaa, 0xbb, 0xcc,
            ]
        );
        assert_eq!(ReplicatedCommandEnvelope::decode(&encoded), Ok(envelope));
    }

    #[test]
    fn replicated_command_envelope_preserves_backend_bytes_unchanged() {
        let opaque_qsql = b"QSQL\0\x02{\"request_id\":\"same\"}".to_vec();
        let envelope = ReplicatedCommandEnvelope::new(
            ExecutionProfile::Sqlite,
            1,
            "same",
            opaque_qsql.clone(),
        )
        .unwrap();

        let decoded = ReplicatedCommandEnvelope::decode(&envelope.encode().unwrap()).unwrap();
        assert_eq!(decoded.body(), opaque_qsql);
    }

    #[test]
    fn replicated_command_envelope_rejects_noncanonical_or_invalid_inputs() {
        assert_eq!(
            ReplicatedCommandEnvelope::new(ExecutionProfile::Graph, 0, "req", Vec::new()),
            Err(CommandEnvelopeError::InvalidCommandVersion)
        );
        assert_eq!(
            ReplicatedCommandEnvelope::new(ExecutionProfile::Graph, 1, "", Vec::new()),
            Err(CommandEnvelopeError::EmptyRequestId)
        );

        let valid =
            ReplicatedCommandEnvelope::new(ExecutionProfile::Graph, 1, "req", vec![1, 2, 3])
                .unwrap()
                .encode()
                .unwrap();
        let mut unknown_profile = valid.clone();
        unknown_profile[6] = 9;
        assert_eq!(
            ReplicatedCommandEnvelope::decode(&unknown_profile),
            Err(CommandEnvelopeError::UnknownExecutionProfile(9))
        );

        let mut trailing = valid;
        trailing.push(0);
        assert_eq!(
            ReplicatedCommandEnvelope::decode(&trailing),
            Err(CommandEnvelopeError::TrailingBytes)
        );
    }

    #[test]
    fn replicated_command_envelope_rejects_malformed_wire_boundaries() {
        let valid =
            ReplicatedCommandEnvelope::new(ExecutionProfile::Graph, 1, "req", vec![1, 2, 3])
                .unwrap()
                .encode()
                .unwrap();

        let mut invalid_magic = valid.clone();
        invalid_magic[0] = b'X';
        assert_eq!(
            ReplicatedCommandEnvelope::decode(&invalid_magic),
            Err(CommandEnvelopeError::InvalidMagic)
        );

        let mut unsupported_version = valid.clone();
        unsupported_version[4..6].copy_from_slice(&2_u16.to_be_bytes());
        assert_eq!(
            ReplicatedCommandEnvelope::decode(&unsupported_version),
            Err(CommandEnvelopeError::UnsupportedFormatVersion(2))
        );

        assert_eq!(
            ReplicatedCommandEnvelope::decode(&valid[..14]),
            Err(CommandEnvelopeError::Truncated)
        );

        let mut truncated_body = valid.clone();
        truncated_body[11..15].copy_from_slice(&4_u32.to_be_bytes());
        assert_eq!(
            ReplicatedCommandEnvelope::decode(&truncated_body),
            Err(CommandEnvelopeError::Truncated)
        );

        let mut invalid_request_utf8 = valid.clone();
        invalid_request_utf8[15] = 0xff;
        assert_eq!(
            ReplicatedCommandEnvelope::decode(&invalid_request_utf8),
            Err(CommandEnvelopeError::InvalidRequestIdUtf8)
        );

        let mut oversized_request = valid.clone();
        oversized_request[9..11].copy_from_slice(&4_u16.to_be_bytes());
        assert_eq!(
            ReplicatedCommandEnvelope::decode(&oversized_request),
            Err(CommandEnvelopeError::Truncated)
        );

        let mut undersized_body = valid.clone();
        undersized_body[11..15].copy_from_slice(&2_u32.to_be_bytes());
        assert_eq!(
            ReplicatedCommandEnvelope::decode(&undersized_body),
            Err(CommandEnvelopeError::TrailingBytes)
        );
    }

    #[test]
    fn replicated_command_decoder_never_panics_and_successes_are_canonical() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        for length in 0..=256 {
            for _ in 0..8 {
                let mut bytes = Vec::with_capacity(length);
                for _ in 0..length {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    bytes.push((state >> 32) as u8);
                }
                let decoded =
                    std::panic::catch_unwind(|| ReplicatedCommandEnvelope::decode(&bytes));
                let decoded = decoded.expect("decoder must not panic for arbitrary bytes");
                if let Ok(envelope) = decoded {
                    assert_eq!(envelope.encode().unwrap(), bytes);
                }
            }
        }

        for sequence in 1_u16..=256 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let body = state.to_be_bytes()[..usize::from(sequence % 8)].to_vec();
            let profile = match sequence % 3 {
                0 => ExecutionProfile::Sqlite,
                1 => ExecutionProfile::Graph,
                _ => ExecutionProfile::Kv,
            };
            let envelope = ReplicatedCommandEnvelope::new(
                profile,
                sequence,
                format!("request-{sequence}"),
                body,
            )
            .unwrap();
            let encoded = envelope.encode().unwrap();
            let decoded = ReplicatedCommandEnvelope::decode(&encoded).unwrap();
            assert_eq!(decoded.encode().unwrap(), encoded);
        }
    }

    #[test]
    fn config_decoder_rejects_overflowing_offsets_without_panicking() {
        let bytes = [];
        assert_eq!(
            read_config_u16(&bytes, usize::MAX),
            Err(ConfigChangeDecodeError)
        );
        assert_eq!(
            read_config_u64(&bytes, usize::MAX),
            Err(ConfigChangeDecodeError)
        );
        assert_eq!(
            read_config_hash(&bytes, usize::MAX),
            Err(ConfigChangeDecodeError)
        );

        let mut cursor = usize::MAX;
        assert_eq!(
            read_config_string(&bytes, &mut cursor),
            Err(ConfigChangeDecodeError)
        );
        assert_eq!(
            read_config_u64_at(&bytes, &mut cursor),
            Err(ConfigChangeDecodeError)
        );
        assert_eq!(
            read_config_hash_at(&bytes, &mut cursor),
            Err(ConfigChangeDecodeError)
        );
    }

    #[test]
    fn external_effect_decoder_rejects_oversized_input_before_deserialization() {
        let encoded = vec![0_u8; MAX_EXTERNAL_EFFECT_COMMAND_BYTES + 1];
        assert_eq!(
            ExternalEffectCommand::decode(&encoded),
            Err(ExternalEffectCommandError::EncodedTooLarge)
        );
    }
}
