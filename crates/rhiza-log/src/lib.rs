use std::{
    fmt, fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, MutexGuard,
    },
};

use rhiza_core::{
    ConfigurationState, EntryType, LogAnchor, LogEntry, LogHash, LogIndex, RecoveryAnchor,
    SnapshotIdentity, StopBinding, SuccessorDescriptor, RECOVERY_ANCHOR_FORMAT_VERSION,
};

pub const QLOG_MAGIC: [u8; 4] = *b"QLOG";
pub const QLOG_FORMAT_VERSION: u16 = 1;
pub const QLOG_HEADER_LEN: usize = 76;
pub const QLOG_FRAME_MAGIC: [u8; 4] = *b"QFRM";
pub const QLOG_FOOTER_MAGIC: [u8; 4] = *b"QEND";
pub const OPEN_SEGMENT_MAX_BYTES: usize = 8 * 1024 * 1024;
pub const OPEN_SEGMENT_MAX_ENTRIES: usize = 4096;

const HEADER_WITHOUT_CRC_LEN: usize = 72;
const FRAME_PREFIX_LEN: usize = 108;
const FRAME_MIN_LEN: usize = 144;
const FOOTER_LEN: usize = 88;
const TRUNCATE_INTENT_FILE_NAME: &str = ".truncate-intent";
const TRUNCATE_INTENT_MAGIC: [u8; 4] = *b"QTRN";
const TRUNCATE_INTENT_VERSION: u16 = 1;
const TRUNCATE_INTENT_REPLACEMENT: u16 = 1;
const ANCHOR_FILE_NAME: &str = "recovery.anchor";
const ANCHOR_MAGIC: [u8; 4] = *b"QANC";
const ANCHOR_VERSION: u16 = 4;
const COMPACT_INTENT_FILE_NAME: &str = ".compact-intent";
const COMPACT_INTENT_MAGIC: [u8; 4] = *b"QCMP";
const COMPACT_INTENT_VERSION: u16 = 1;
const COMPACT_INTENT_PREVIOUS_ANCHOR: u16 = 1;
const COMPACT_INTENT_REPLACEMENT: u16 = 2;
static NEXT_TEMP_FILE_ID: AtomicU64 = AtomicU64::new(0);

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidIndexRange {
        start: LogIndex,
        end: LogIndex,
    },
    CompactionUnsupported,
    CompactionAboveTip {
        target: LogIndex,
        tip: Option<LogIndex>,
    },
    CompactionHashMismatch {
        index: LogIndex,
    },
    CompactionRegression {
        target: LogIndex,
        anchor: LogIndex,
    },
    CompactionConflict {
        index: LogIndex,
    },
    TruncateCompactedPrefix {
        from: LogIndex,
        anchor: LogIndex,
    },
    Decode(String),
    Io(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIndexRange { start, end } => {
                write!(f, "invalid index range: start {start} is after end {end}")
            }
            Self::CompactionUnsupported => {
                write!(f, "prefix compaction is unsupported by qlog v1")
            }
            Self::CompactionAboveTip { target, tip } => {
                write!(f, "compaction target {target} is above log tip {tip:?}")
            }
            Self::CompactionHashMismatch { index } => {
                write!(f, "compaction hash does not match log entry {index}")
            }
            Self::CompactionRegression { target, anchor } => write!(
                f,
                "compaction target {target} regresses persisted anchor {anchor}"
            ),
            Self::CompactionConflict { index } => {
                write!(f, "compaction replay conflicts at index {index}")
            }
            Self::TruncateCompactedPrefix { from, anchor } => write!(
                f,
                "cannot truncate from {from} at or below compacted anchor {anchor}"
            ),
            Self::Decode(message) => write!(f, "qlog decode failed: {message}"),
            Self::Io(message) => write!(f, "qlog io failed: {message}"),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexRange {
    start: LogIndex,
    end: LogIndex,
}

impl IndexRange {
    pub fn new(start: LogIndex, end: LogIndex) -> Result<Self> {
        if start > end {
            return Err(Error::InvalidIndexRange { start, end });
        }

        Ok(Self { start, end })
    }

    pub const fn start(&self) -> LogIndex {
        self.start
    }

    pub const fn end(&self) -> LogIndex {
        self.end
    }
}

pub fn segment_file_name(range: IndexRange) -> String {
    format!("{:020}-{:020}.qlog", range.start(), range.end())
}

pub fn encode_segment(entries: &[LogEntry]) -> Vec<u8> {
    encode_segment_inner(entries, true)
}

pub fn encode_open_segment(entries: &[LogEntry]) -> Vec<u8> {
    encode_segment_inner(entries, false)
}

fn encode_segment_inner(entries: &[LogEntry], closed: bool) -> Vec<u8> {
    let Some(first) = entries.first() else {
        return Vec::new();
    };
    let mut out = encode_header(first);
    let mut entry_hashes = Vec::with_capacity(entries.len() * 32);
    for entry in entries {
        out.extend_from_slice(&encode_frame(entry));
        entry_hashes.extend_from_slice(entry.hash.as_bytes());
    }
    if closed {
        let last = entries.last().expect("non-empty entries");
        out.extend_from_slice(&encode_footer(
            &out[..QLOG_HEADER_LEN],
            &entry_hashes,
            last.index,
            entries.len() as u64,
            last.hash,
        ));
    }
    out
}

pub fn decode_segment(bytes: &[u8]) -> Result<Vec<LogEntry>> {
    decode_segment_for_cluster(bytes, "")
}

pub fn decode_segment_for_cluster(bytes: &[u8], cluster_id: &str) -> Result<Vec<LogEntry>> {
    let (header, mut offset) = decode_header(bytes, cluster_id)?;
    let cluster_id = if cluster_id.is_empty() {
        header.cluster_id_hash.to_hex()
    } else {
        cluster_id.to_string()
    };
    let mut entries = Vec::new();
    let mut entry_hashes = Vec::new();

    loop {
        if offset >= bytes.len() {
            return Err(Error::Decode("missing qlog footer".into()));
        }
        if bytes.len() - offset >= 4 && bytes[offset..offset + 4] == QLOG_FOOTER_MAGIC {
            decode_footer(
                bytes,
                offset,
                &bytes[..QLOG_HEADER_LEN],
                &entry_hashes,
                &entries,
            )?;
            validate_entries(&header, &entries)?;
            return Ok(entries);
        }
        let (entry, next_offset) = decode_frame(bytes, offset, &cluster_id)?;
        entry_hashes.extend_from_slice(entry.hash.as_bytes());
        entries.push(entry);
        offset = next_offset;
    }
}

pub fn write_segment_file(dir: impl Into<PathBuf>, entries: &[LogEntry]) -> Result<PathBuf> {
    let dir = dir.into();
    fs::create_dir_all(&dir).map_err(|err| Error::Io(err.to_string()))?;
    publish_closed_segment(&dir, entries)
}

pub fn read_segment_file(path: impl Into<PathBuf>) -> Result<Vec<LogEntry>> {
    let bytes = fs::read(path.into()).map_err(|err| Error::Io(err.to_string()))?;
    decode_segment(&bytes)
}

pub fn recover_open_segment_file(
    path: impl AsRef<Path>,
    cluster_id: &str,
) -> Result<Vec<LogEntry>> {
    let path = path.as_ref();
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if !name.ends_with("-open.qlog") {
        return Err(Error::Decode(
            "refusing to recover non-open qlog segment".into(),
        ));
    }
    let bytes = fs::read(path).map_err(|err| Error::Io(err.to_string()))?;
    let (entries, valid_len) = recover_open_segment_prefix(&bytes, cluster_id)?;
    if valid_len < bytes.len() {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|err| Error::Io(err.to_string()))?;
        file.set_len(valid_len as u64)
            .map_err(|err| Error::Io(err.to_string()))?;
        file.sync_all().map_err(|err| Error::Io(err.to_string()))?;
    }
    Ok(entries)
}

fn recover_open_segment_prefix(bytes: &[u8], cluster_id: &str) -> Result<(Vec<LogEntry>, usize)> {
    let (header, mut offset) = decode_header(bytes, cluster_id)?;
    let mut entries = Vec::new();
    let mut valid_len = offset;
    while offset < bytes.len() {
        let (entry, next_offset) = match decode_frame(bytes, offset, cluster_id) {
            Ok(frame) => frame,
            Err(_) if final_frame_is_incomplete(bytes, offset)? => break,
            Err(err) => return Err(err),
        };
        validate_decoded_entry(&header, entries.len(), entries.last(), &entry)?;
        entries.push(entry);
        offset = next_offset;
        valid_len = offset;
    }
    Ok((entries, valid_len))
}

fn final_frame_is_incomplete(bytes: &[u8], offset: usize) -> Result<bool> {
    let tail = &bytes[offset..];
    let magic_prefix_len = tail.len().min(QLOG_FRAME_MAGIC.len());
    if tail[..magic_prefix_len] != QLOG_FRAME_MAGIC[..magic_prefix_len] {
        return Ok(false);
    }
    if tail.len() < 8 {
        return Ok(true);
    }

    let frame_len = read_u32(tail, 4)? as usize;
    if frame_len < FRAME_MIN_LEN {
        return Ok(false);
    }
    if tail.len() >= FRAME_PREFIX_LEN {
        let payload_len = read_u32(tail, 104)? as usize;
        if FRAME_MIN_LEN.checked_add(payload_len) != Some(frame_len) {
            return Ok(false);
        }
    }
    Ok(frame_len > tail.len())
}

fn encode_header(first: &LogEntry) -> Vec<u8> {
    let mut out = Vec::with_capacity(QLOG_HEADER_LEN);
    out.extend_from_slice(&QLOG_MAGIC);
    put_u16(&mut out, QLOG_FORMAT_VERSION);
    put_u16(&mut out, QLOG_HEADER_LEN as u16);
    put_u64(&mut out, first.index);
    put_u64(&mut out, first.epoch);
    put_u64(&mut out, first.config_id);
    out.extend_from_slice(LogHash::digest(&[first.cluster_id.as_bytes()]).as_bytes());
    put_u64(&mut out, 0);
    let crc = crc32c(&out[..HEADER_WITHOUT_CRC_LEN]);
    put_u32(&mut out, crc);
    out
}

fn decode_header(bytes: &[u8], cluster_id: &str) -> Result<(SegmentHeader, usize)> {
    if bytes.len() < QLOG_HEADER_LEN {
        return Err(Error::Decode("short qlog header".into()));
    }
    if bytes[0..4] != QLOG_MAGIC {
        return Err(Error::Decode("wrong qlog magic".into()));
    }
    let version = read_u16(bytes, 4)?;
    if version != QLOG_FORMAT_VERSION {
        return Err(Error::Decode("unsupported qlog version".into()));
    }
    let header_len = read_u16(bytes, 6)? as usize;
    if header_len != QLOG_HEADER_LEN {
        return Err(Error::Decode("invalid qlog header_len".into()));
    }
    let expected_crc = read_u32(bytes, HEADER_WITHOUT_CRC_LEN)?;
    if crc32c(&bytes[..HEADER_WITHOUT_CRC_LEN]) != expected_crc {
        return Err(Error::Decode("qlog header crc mismatch".into()));
    }
    let cluster_id_hash = read_hash(bytes, 32)?;
    if !cluster_id.is_empty() && LogHash::digest(&[cluster_id.as_bytes()]) != cluster_id_hash {
        return Err(Error::Decode("qlog cluster_id hash mismatch".into()));
    }
    Ok((
        SegmentHeader::new_with_config(
            cluster_id_hash,
            read_u64(bytes, 16)?,
            read_u64(bytes, 24)?,
            read_u64(bytes, 8)?,
            read_u64(bytes, 64)?,
        ),
        QLOG_HEADER_LEN,
    ))
}

fn encode_frame(entry: &LogEntry) -> Vec<u8> {
    let frame_len = FRAME_MIN_LEN + entry.payload.len();
    let mut out = Vec::with_capacity(frame_len);
    out.extend_from_slice(&QLOG_FRAME_MAGIC);
    put_u32(&mut out, frame_len as u32);
    put_u64(&mut out, entry.index);
    put_u64(&mut out, entry.epoch);
    put_u64(&mut out, entry.config_id);
    out.push(entry.entry_type.as_u8());
    out.extend_from_slice(&[0; 7]);
    out.extend_from_slice(entry.prev_hash.as_bytes());
    out.extend_from_slice(LogHash::digest(&[&entry.payload]).as_bytes());
    put_u32(&mut out, entry.payload.len() as u32);
    out.extend_from_slice(&entry.payload);
    out.extend_from_slice(entry.hash.as_bytes());
    let crc = crc32c(&out);
    put_u32(&mut out, crc);
    out
}

fn decode_frame(bytes: &[u8], offset: usize, cluster_id: &str) -> Result<(LogEntry, usize)> {
    if bytes.len().saturating_sub(offset) < FRAME_MIN_LEN {
        return Err(Error::Decode("short qlog frame".into()));
    }
    if bytes[offset..offset + 4] != QLOG_FRAME_MAGIC {
        return Err(Error::Decode("wrong qlog frame magic".into()));
    }
    let frame_len = read_u32(bytes, offset + 4)? as usize;
    let Some(frame_end) = offset.checked_add(frame_len) else {
        return Err(Error::Decode("invalid qlog frame_len".into()));
    };
    if frame_len < FRAME_MIN_LEN || frame_end > bytes.len() {
        return Err(Error::Decode("invalid qlog frame_len".into()));
    }
    let crc_offset = frame_end - 4;
    let expected_crc = read_u32(bytes, crc_offset)?;
    if crc32c(&bytes[offset..crc_offset]) != expected_crc {
        return Err(Error::Decode("qlog frame crc mismatch".into()));
    }
    let index = read_u64(bytes, offset + 8)?;
    let epoch = read_u64(bytes, offset + 16)?;
    let config_id = read_u64(bytes, offset + 24)?;
    let entry_type = EntryType::from_u8(bytes[offset + 32])
        .ok_or_else(|| Error::Decode("invalid qlog entry_type".into()))?;
    let prev_hash = read_hash(bytes, offset + 40)?;
    let payload_hash = read_hash(bytes, offset + 72)?;
    let payload_len = read_u32(bytes, offset + 104)? as usize;
    if FRAME_MIN_LEN.checked_add(payload_len) != Some(frame_len) {
        return Err(Error::Decode(
            "qlog frame_len does not match payload_len".into(),
        ));
    }
    let payload_start = offset + FRAME_PREFIX_LEN;
    let payload_end = payload_start
        .checked_add(payload_len)
        .ok_or_else(|| Error::Decode("invalid qlog payload_len".into()))?;
    let payload = bytes[payload_start..payload_end].to_vec();
    if LogHash::digest(&[&payload]) != payload_hash {
        return Err(Error::Decode("qlog payload_hash mismatch".into()));
    }
    let hash = read_hash(bytes, payload_end)?;
    let entry = LogEntry {
        cluster_id: cluster_id.to_string(),
        epoch,
        config_id,
        index,
        entry_type,
        payload,
        prev_hash,
        hash,
    };
    if entry.recompute_hash() != hash {
        return Err(Error::Decode("qlog entry_hash mismatch".into()));
    }
    Ok((entry, frame_end))
}

fn encode_footer(
    header: &[u8],
    entry_hashes: &[u8],
    end_index: LogIndex,
    entry_count: u64,
    last_entry_hash: LogHash,
) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(52);
    prefix.extend_from_slice(&QLOG_FOOTER_MAGIC);
    put_u64(&mut prefix, end_index);
    put_u64(&mut prefix, entry_count);
    prefix.extend_from_slice(last_entry_hash.as_bytes());
    let segment_hash = LogHash::digest(&[header, entry_hashes, &prefix]);
    let mut out = prefix;
    out.extend_from_slice(segment_hash.as_bytes());
    let crc = crc32c(&out);
    put_u32(&mut out, crc);
    out
}

fn decode_footer(
    bytes: &[u8],
    offset: usize,
    header: &[u8],
    entry_hashes: &[u8],
    entries: &[LogEntry],
) -> Result<()> {
    if bytes.len().saturating_sub(offset) != FOOTER_LEN {
        return Err(Error::Decode("invalid qlog footer length".into()));
    }
    let footer = &bytes[offset..offset + FOOTER_LEN];
    let expected_crc = read_u32(footer, FOOTER_LEN - 4)?;
    if crc32c(&footer[..FOOTER_LEN - 4]) != expected_crc {
        return Err(Error::Decode("qlog footer crc mismatch".into()));
    }
    let end_index = read_u64(footer, 4)?;
    let entry_count = read_u64(footer, 12)?;
    let last_hash = read_hash(footer, 20)?;
    let segment_hash = read_hash(footer, 52)?;
    let expected_segment_hash = LogHash::digest(&[header, entry_hashes, &footer[..52]]);
    if segment_hash != expected_segment_hash {
        return Err(Error::Decode("qlog segment_hash mismatch".into()));
    }
    if entries.len() as u64 != entry_count {
        return Err(Error::Decode("qlog footer entry_count mismatch".into()));
    }
    if let Some(last) = entries.last() {
        if last.index != end_index || last.hash != last_hash {
            return Err(Error::Decode("qlog footer last entry mismatch".into()));
        }
    } else if entry_count != 0 {
        return Err(Error::Decode("qlog empty footer mismatch".into()));
    }
    Ok(())
}

fn validate_entries(header: &SegmentHeader, entries: &[LogEntry]) -> Result<()> {
    for (position, entry) in entries.iter().enumerate() {
        validate_decoded_entry(
            header,
            position,
            position.checked_sub(1).map(|i| &entries[i]),
            entry,
        )?;
    }
    Ok(())
}

fn validate_decoded_entry(
    header: &SegmentHeader,
    position: usize,
    previous: Option<&LogEntry>,
    entry: &LogEntry,
) -> Result<()> {
    let expected_index = u64::try_from(position)
        .ok()
        .and_then(|position| header.start_index.checked_add(position));
    if expected_index != Some(entry.index) {
        return Err(Error::Decode("qlog index gap".into()));
    }
    if entry.epoch != header.epoch || entry.config_id != header.config_id {
        return Err(Error::Decode("qlog epoch/config mismatch".into()));
    }
    if previous.is_some_and(|previous| entry.prev_hash != previous.hash) {
        return Err(Error::Decode("qlog hash chain mismatch".into()));
    }
    Ok(())
}

fn read_hash(bytes: &[u8], offset: usize) -> Result<LogHash> {
    let slice = bytes
        .get(offset..offset + 32)
        .ok_or_else(|| Error::Decode("short qlog hash".into()))?;
    let mut out = [0; 32];
    out.copy_from_slice(slice);
    Ok(LogHash::from_bytes(out))
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    let slice = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| Error::Decode("short qlog u16".into()))?;
    Ok(u16::from_be_bytes(
        slice.try_into().expect("u16 slice length"),
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let slice = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| Error::Decode("short qlog u32".into()))?;
    Ok(u32::from_be_bytes(
        slice.try_into().expect("u32 slice length"),
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let slice = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| Error::Decode("short qlog u64".into()))?;
    Ok(u64::from_be_bytes(
        slice.try_into().expect("u64 slice length"),
    ))
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0x82f6_3b78 & mask);
        }
    }
    !crc
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentHeader {
    magic: [u8; 4],
    cluster_id_hash: LogHash,
    epoch: u64,
    config_id: u64,
    start_index: LogIndex,
    created_at_unix_ms: u64,
}

impl SegmentHeader {
    pub const fn new(
        cluster_id_hash: LogHash,
        epoch: u64,
        start_index: LogIndex,
        created_at_unix_ms: u64,
    ) -> Self {
        Self {
            magic: QLOG_MAGIC,
            cluster_id_hash,
            epoch,
            config_id: 0,
            start_index,
            created_at_unix_ms,
        }
    }

    pub const fn new_with_config(
        cluster_id_hash: LogHash,
        epoch: u64,
        config_id: u64,
        start_index: LogIndex,
        created_at_unix_ms: u64,
    ) -> Self {
        Self {
            magic: QLOG_MAGIC,
            cluster_id_hash,
            epoch,
            config_id,
            start_index,
            created_at_unix_ms,
        }
    }

    pub const fn magic(&self) -> [u8; 4] {
        self.magic
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn config_id(&self) -> u64 {
        self.config_id
    }

    pub const fn start_index(&self) -> LogIndex {
        self.start_index
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentFile {
    range: IndexRange,
    bytes: Vec<u8>,
}

impl SegmentFile {
    pub fn new(range: IndexRange, bytes: Vec<u8>) -> Self {
        Self { range, bytes }
    }

    pub const fn range(&self) -> IndexRange {
        self.range
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub trait LogStore {
    fn append(&self, entry: &LogEntry) -> Result<()>;
    fn append_batch(&self, entries: &[LogEntry]) -> Result<()>;
    fn read(&self, index: LogIndex) -> Result<Option<LogEntry>>;
    fn read_range(&self, range: IndexRange) -> Result<Vec<LogEntry>>;
    fn last_index(&self) -> Result<Option<LogIndex>>;
    fn truncate_suffix(&self, from: LogIndex) -> Result<()>;
    fn compact_prefix(&self, verified_snapshot_anchor: &RecoveryAnchor) -> Result<()>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogState {
    pub anchor: Option<RecoveryAnchor>,
    pub first_retained_index: LogIndex,
    pub tip: Option<LogAnchor>,
}

#[derive(Debug)]
pub struct FileLogStore {
    inner: Mutex<FileLogStoreInner>,
}

impl FileLogStore {
    pub fn open(
        dir: impl Into<PathBuf>,
        cluster_id: impl Into<String>,
        epoch: u64,
        config_id: u64,
    ) -> Result<Self> {
        Self::open_with_configuration(
            dir,
            cluster_id,
            epoch,
            ConfigurationState::active(config_id, LogHash::ZERO),
        )
    }

    pub fn open_with_configuration(
        dir: impl Into<PathBuf>,
        cluster_id: impl Into<String>,
        epoch: u64,
        initial_configuration: ConfigurationState,
    ) -> Result<Self> {
        let dir = dir.into();
        let cluster_id = cluster_id.into();
        if cluster_id.is_empty() {
            return Err(Error::Decode("cluster_id must not be empty".into()));
        }

        let existed = dir.exists();
        fs::create_dir_all(&dir).map_err(|err| Error::Io(err.to_string()))?;
        if !existed {
            if let Some(parent) = dir.parent() {
                sync_directory(parent)?;
            }
        }
        recover_truncate_intent(&dir)?;
        recover_compact_intent(&dir)?;
        let anchor = read_anchor(&dir)?;
        validate_anchor_identity(anchor.as_ref(), &cluster_id, epoch)?;
        let (segments, configuration_state) = scan_closed_segments(
            &dir,
            &cluster_id,
            epoch,
            &initial_configuration,
            anchor.as_ref(),
        )?;
        let (open_segment, configuration_state) = scan_open_segment(
            &dir,
            &cluster_id,
            epoch,
            anchor.as_ref(),
            &segments,
            configuration_state,
        )?;

        Ok(Self {
            inner: Mutex::new(FileLogStoreInner {
                dir,
                cluster_id,
                epoch,
                initial_configuration,
                configuration_state,
                anchor,
                segments,
                open_segment,
            }),
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, FileLogStoreInner>> {
        self.inner
            .lock()
            .map_err(|_| Error::Io("file log store lock poisoned".into()))
    }

    pub fn logical_state(&self) -> Result<LogState> {
        let inner = self.lock()?;
        let first_retained_index = match &inner.anchor {
            Some(anchor) => anchor
                .compacted()
                .index()
                .checked_add(1)
                .ok_or_else(|| Error::Decode("qlog anchor index overflow".into()))?,
            None => 1,
        };
        let tip = inner
            .open_segment
            .as_ref()
            .and_then(|segment| segment.entries.last())
            .map(|entry| LogAnchor::new(entry.index, entry.hash))
            .or_else(|| {
                inner.segments.last().map(|segment| {
                    let entry = segment.entries.last().expect("non-empty segment");
                    LogAnchor::new(entry.index, entry.hash)
                })
            })
            .or_else(|| inner.anchor.as_ref().map(|anchor| *anchor.compacted()));
        Ok(LogState {
            anchor: inner.anchor.clone(),
            first_retained_index,
            tip,
        })
    }

    pub fn configuration_state(&self) -> Result<ConfigurationState> {
        Ok(self.lock()?.configuration_state.clone())
    }

    pub fn install_recovery_anchor(
        &self,
        verified_anchor: &RecoveryAnchor,
        expected_recovery_generation: u64,
        expected_configuration: &ConfigurationState,
    ) -> Result<()> {
        let mut inner = self.lock()?;
        validate_anchor(verified_anchor)?;
        validate_anchor_identity(Some(verified_anchor), &inner.cluster_id, inner.epoch)?;
        if verified_anchor.recovery_generation() != expected_recovery_generation {
            return Err(Error::Decode(
                "recovery anchor generation does not match expected generation".into(),
            ));
        }
        if verified_anchor.configuration_state() != expected_configuration {
            return Err(Error::Decode(
                "recovery anchor configuration state does not match expected state".into(),
            ));
        }
        if inner.anchor.is_some() || !inner.segments.is_empty() || inner.open_segment.is_some() {
            return Err(Error::Decode(
                "recovery anchor installation requires an empty qlog store".into(),
            ));
        }

        install_anchor(
            &inner.dir,
            &CompactIntent {
                previous_anchor: None,
                anchor: verified_anchor.clone(),
                old_segment_names: Vec::new(),
                replacement: None,
            },
        )?;
        sync_directory(&inner.dir)?;
        inner.anchor = Some(verified_anchor.clone());
        inner.configuration_state = verified_anchor.configuration_state().clone();
        Ok(())
    }

    /// Appends validated entries without issuing a data sync.
    ///
    /// Call [`Self::sync`] before making the appended entries externally durable.
    pub fn append_batch_buffered(&self, entries: &[LogEntry]) -> Result<()> {
        let mut inner = self.lock()?;
        append_batch_to_open_segment(&mut inner, entries, false)
    }

    /// Syncs all buffered appends and returns the durable logical tip.
    pub fn sync(&self) -> Result<Option<LogIndex>> {
        let inner = self.lock()?;
        if let Some(open) = &inner.open_segment {
            open.file
                .sync_data()
                .map_err(|err| Error::Io(err.to_string()))?;
        }
        Ok(inner.last_index())
    }
}

impl LogStore for FileLogStore {
    fn append(&self, entry: &LogEntry) -> Result<()> {
        self.append_batch(std::slice::from_ref(entry))
    }

    fn append_batch(&self, entries: &[LogEntry]) -> Result<()> {
        let mut inner = self.lock()?;
        append_batch_to_open_segment(&mut inner, entries, true)
    }

    fn read(&self, index: LogIndex) -> Result<Option<LogEntry>> {
        let inner = self.lock()?;
        Ok(inner
            .segments
            .iter()
            .find(|segment| segment.start() <= index && index <= segment.end())
            .map(|segment| segment.entries[(index - segment.start()) as usize].clone())
            .or_else(|| {
                inner.open_segment.as_ref().and_then(|segment| {
                    segment
                        .entries
                        .first()
                        .filter(|first| first.index <= index)
                        .and_then(|first| segment.entries.get((index - first.index) as usize))
                        .cloned()
                })
            }))
    }

    fn read_range(&self, range: IndexRange) -> Result<Vec<LogEntry>> {
        let inner = self.lock()?;
        Ok(inner
            .segments
            .iter()
            .flat_map(|segment| segment.entries.iter())
            .chain(
                inner
                    .open_segment
                    .iter()
                    .flat_map(|segment| segment.entries.iter()),
            )
            .filter(|entry| range.start() <= entry.index && entry.index <= range.end())
            .cloned()
            .collect())
    }

    fn last_index(&self) -> Result<Option<LogIndex>> {
        let inner = self.lock()?;
        Ok(inner.last_index())
    }

    fn truncate_suffix(&self, from: LogIndex) -> Result<()> {
        let mut inner = self.lock()?;
        if let Some(anchor) = &inner.anchor {
            if from <= anchor.compacted().index() {
                return Err(Error::TruncateCompactedPrefix {
                    from,
                    anchor: anchor.compacted().index(),
                });
            }
        }
        seal_open_segment(&mut inner)?;
        truncate_suffix_with_hook(&mut inner, from, &mut |_| Ok(()))?;
        let (segments, configuration_state) = scan_closed_segments(
            &inner.dir,
            &inner.cluster_id,
            inner.epoch,
            &inner.initial_configuration,
            inner.anchor.as_ref(),
        )?;
        inner.segments = segments;
        inner.configuration_state = configuration_state;
        Ok(())
    }

    fn compact_prefix(&self, verified_snapshot_anchor: &RecoveryAnchor) -> Result<()> {
        let mut inner = self.lock()?;
        if !validate_compaction(&inner, verified_snapshot_anchor)? {
            return Ok(());
        }
        seal_open_segment(&mut inner)?;
        compact_prefix_with_hook(&mut inner, verified_snapshot_anchor, &mut |_| Ok(()))?;
        inner.anchor = read_anchor(&inner.dir)?;
        let (segments, configuration_state) = scan_closed_segments(
            &inner.dir,
            &inner.cluster_id,
            inner.epoch,
            &inner.initial_configuration,
            inner.anchor.as_ref(),
        )?;
        inner.segments = segments;
        inner.configuration_state = configuration_state;
        Ok(())
    }
}

#[derive(Debug)]
struct FileLogStoreInner {
    dir: PathBuf,
    cluster_id: String,
    epoch: u64,
    initial_configuration: ConfigurationState,
    configuration_state: ConfigurationState,
    anchor: Option<RecoveryAnchor>,
    segments: Vec<ClosedSegment>,
    open_segment: Option<OpenSegment>,
}

#[derive(Clone, Debug)]
struct ClosedSegment {
    entries: Vec<LogEntry>,
}

#[derive(Debug)]
struct OpenSegment {
    config_id: u64,
    path: PathBuf,
    file: fs::File,
    bytes_len: usize,
    entries: Vec<LogEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TruncateIntent {
    old_segment_names: Vec<String>,
    replacement: Option<TruncateReplacement>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TruncateReplacement {
    temp_name: String,
    final_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CompactIntent {
    previous_anchor: Option<RecoveryAnchor>,
    anchor: RecoveryAnchor,
    old_segment_names: Vec<String>,
    replacement: Option<TruncateReplacement>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompactPhase {
    ReplacementPrepared,
    IntentRenamed,
    IntentDurable,
    AnchorInstalled,
    AnchorDurable,
    OldSegmentRemoved(usize),
    ReplacementInstalled,
    AppliedDirectorySynced,
    IntentRemoved,
    CompleteDirectorySynced,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TruncatePhase {
    ReplacementPrepared,
    IntentRenamed,
    IntentDurable,
    OldSegmentRemoved(usize),
    ReplacementInstalled,
    AppliedDirectorySynced,
    IntentRemoved,
    CompleteDirectorySynced,
}

impl ClosedSegment {
    fn start(&self) -> LogIndex {
        self.entries.first().expect("non-empty segment").index
    }

    fn end(&self) -> LogIndex {
        self.entries.last().expect("non-empty segment").index
    }
}

impl FileLogStoreInner {
    fn last_index(&self) -> Option<LogIndex> {
        self.open_segment
            .as_ref()
            .and_then(|segment| segment.entries.last().map(|entry| entry.index))
            .or_else(|| self.segments.last().map(ClosedSegment::end))
            .or_else(|| {
                self.anchor
                    .as_ref()
                    .map(|anchor| anchor.compacted().index())
            })
    }
}

fn scan_closed_segments(
    dir: &Path,
    cluster_id: &str,
    epoch: u64,
    initial_configuration: &ConfigurationState,
    anchor: Option<&RecoveryAnchor>,
) -> Result<(Vec<ClosedSegment>, ConfigurationState)> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(dir).map_err(|err| Error::Io(err.to_string()))? {
        let entry = entry.map_err(|err| Error::Io(err.to_string()))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with("-open.qlog") || !name.ends_with(".qlog") {
            continue;
        }
        let range = parse_closed_segment_name(&name)?;
        paths.push((range, entry.path()));
    }
    paths.sort_by_key(|(range, _)| (range.start(), range.end()));

    let mut segments = Vec::with_capacity(paths.len());
    let mut configuration_state = anchor
        .map(|anchor| anchor.configuration_state().clone())
        .unwrap_or_else(|| initial_configuration.clone());
    for (range, path) in paths {
        let bytes = fs::read(&path).map_err(|err| Error::Io(err.to_string()))?;
        let entries = decode_segment_for_cluster(&bytes, cluster_id)?;
        let first = entries
            .first()
            .ok_or_else(|| Error::Decode("closed qlog segment is empty".into()))?;
        let last = entries.last().expect("non-empty segment");
        if first.index != range.start() || last.index != range.end() {
            return Err(Error::Decode(
                "qlog segment filename range does not match entries".into(),
            ));
        }
        if first.epoch != epoch {
            return Err(Error::Decode(
                "qlog segment does not match configured epoch".into(),
            ));
        }
        if segments.is_empty() {
            let (expected_index, expected_prev_hash) = match anchor {
                Some(anchor) => (
                    anchor
                        .compacted()
                        .index()
                        .checked_add(1)
                        .ok_or_else(|| Error::Decode("qlog anchor index overflow".into()))?,
                    anchor.compacted().hash(),
                ),
                None => (1, LogHash::ZERO),
            };
            if first.index != expected_index || first.prev_hash != expected_prev_hash {
                return Err(Error::Decode(
                    "qlog first retained entry does not match recovery anchor".into(),
                ));
            }
        }
        if let Some(previous) = segments.last() {
            let previous: &ClosedSegment = previous;
            if previous.end().checked_add(1) != Some(first.index) {
                return Err(Error::Decode("qlog index gap across segments".into()));
            }
            if first.prev_hash != previous.entries.last().expect("non-empty segment").hash {
                return Err(Error::Decode(
                    "qlog hash chain mismatch across segments".into(),
                ));
            }
        }
        for entry in &entries {
            configuration_state = configuration_state
                .validate_entry(entry)
                .map_err(|err| Error::Decode(err.to_string()))?;
        }
        segments.push(ClosedSegment { entries });
    }
    Ok((segments, configuration_state))
}

fn scan_open_segment(
    dir: &Path,
    cluster_id: &str,
    epoch: u64,
    anchor: Option<&RecoveryAnchor>,
    segments: &[ClosedSegment],
    mut configuration_state: ConfigurationState,
) -> Result<(Option<OpenSegment>, ConfigurationState)> {
    let mut paths = fs::read_dir(dir)
        .map_err(|err| Error::Io(err.to_string()))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            name.ends_with("-open.qlog").then_some((name, entry.path()))
        })
        .collect::<Vec<_>>();
    paths.sort_by(|left, right| left.0.cmp(&right.0));
    if paths.len() > 1 {
        return Err(Error::Decode("multiple open qlog segments".into()));
    }
    let Some((name, path)) = paths.pop() else {
        return Ok((None, configuration_state));
    };
    let start = parse_open_segment_name(&name)?;
    let bytes = fs::read(&path).map_err(|err| Error::Io(err.to_string()))?;
    let (header, _) = decode_header(&bytes, cluster_id)?;
    if header.start_index != start || header.epoch != epoch {
        return Err(Error::Decode(
            "open qlog filename or epoch does not match header".into(),
        ));
    }
    let entries = recover_open_segment_file(&path, cluster_id)?;

    if let Some(segment) = segments
        .iter()
        .find(|segment| segment.start() == start && segment.entries == entries)
    {
        if segment.end() != entries.last().map_or(start, |entry| entry.index) {
            return Err(Error::Decode("open qlog duplicate range mismatch".into()));
        }
        fs::remove_file(&path).map_err(|err| Error::Io(err.to_string()))?;
        sync_directory(dir)?;
        return Ok((None, configuration_state));
    }

    let (expected_index, expected_prev_hash) = match segments.last() {
        Some(segment) => (
            segment
                .end()
                .checked_add(1)
                .ok_or_else(|| Error::Decode("qlog index overflow".into()))?,
            segment.entries.last().expect("non-empty segment").hash,
        ),
        None => match anchor {
            Some(anchor) => (
                anchor
                    .compacted()
                    .index()
                    .checked_add(1)
                    .ok_or_else(|| Error::Decode("qlog anchor index overflow".into()))?,
                anchor.compacted().hash(),
            ),
            None => (1, LogHash::ZERO),
        },
    };
    if start != expected_index {
        return Err(Error::Decode(
            "open qlog does not start at the retained tip".into(),
        ));
    }
    if let Some(first) = entries.first() {
        if first.prev_hash != expected_prev_hash {
            return Err(Error::Decode(
                "qlog hash chain mismatch before open segment".into(),
            ));
        }
    }
    for entry in &entries {
        configuration_state = configuration_state
            .validate_entry(entry)
            .map_err(|err| Error::Decode(err.to_string()))?;
    }
    let file = fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .map_err(|err| Error::Io(err.to_string()))?;
    let bytes_len = usize::try_from(
        file.metadata()
            .map_err(|err| Error::Io(err.to_string()))?
            .len(),
    )
    .map_err(|_| Error::Io("open qlog segment is too large".into()))?;
    Ok((
        Some(OpenSegment {
            config_id: header.config_id,
            path,
            file,
            bytes_len,
            entries,
        }),
        configuration_state,
    ))
}

fn parse_closed_segment_name(name: &str) -> Result<IndexRange> {
    let stem = name
        .strip_suffix(".qlog")
        .ok_or_else(|| Error::Decode("invalid closed qlog segment filename".into()))?;
    let (start, end) = stem
        .split_once('-')
        .ok_or_else(|| Error::Decode("invalid closed qlog segment filename".into()))?;
    if start.len() != 20
        || end.len() != 20
        || !start.bytes().all(|byte| byte.is_ascii_digit())
        || !end.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(Error::Decode("invalid closed qlog segment filename".into()));
    }
    let start = start
        .parse()
        .map_err(|_| Error::Decode("invalid closed qlog segment filename".into()))?;
    let end = end
        .parse()
        .map_err(|_| Error::Decode("invalid closed qlog segment filename".into()))?;
    IndexRange::new(start, end)
}

fn parse_open_segment_name(name: &str) -> Result<LogIndex> {
    let start = name
        .strip_suffix("-open.qlog")
        .ok_or_else(|| Error::Decode("invalid open qlog segment filename".into()))?;
    if start.len() != 20 || !start.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::Decode("invalid open qlog segment filename".into()));
    }
    start
        .parse()
        .map_err(|_| Error::Decode("invalid open qlog segment filename".into()))
}

fn open_segment_file_name(start: LogIndex) -> String {
    format!("{start:020}-open.qlog")
}

fn validate_append(inner: &FileLogStoreInner, entries: &[LogEntry]) -> Result<ConfigurationState> {
    let open_tip = inner
        .open_segment
        .as_ref()
        .and_then(|segment| segment.entries.last());
    let (mut expected_index, mut expected_prev_hash) = match open_tip {
        Some(entry) => (
            entry
                .index
                .checked_add(1)
                .ok_or_else(|| Error::Decode("qlog index overflow".into()))?,
            entry.hash,
        ),
        None => match inner.segments.last() {
            Some(segment) => (
                segment
                    .end()
                    .checked_add(1)
                    .ok_or_else(|| Error::Decode("qlog index overflow".into()))?,
                segment.entries.last().expect("non-empty segment").hash,
            ),
            None => match &inner.anchor {
                Some(anchor) => (
                    anchor
                        .compacted()
                        .index()
                        .checked_add(1)
                        .ok_or_else(|| Error::Decode("qlog anchor index overflow".into()))?,
                    anchor.compacted().hash(),
                ),
                None => (1, LogHash::ZERO),
            },
        },
    };

    let mut configuration_state = inner.configuration_state.clone();
    for (position, entry) in entries.iter().enumerate() {
        if entry.cluster_id != inner.cluster_id || entry.epoch != inner.epoch {
            return Err(Error::Decode(
                "qlog entry does not match configured cluster/epoch".into(),
            ));
        }
        if entry.index != expected_index {
            return Err(Error::Decode("qlog append index is not contiguous".into()));
        }
        if entry.prev_hash != expected_prev_hash {
            return Err(Error::Decode("qlog append prev_hash mismatch".into()));
        }
        if entry.recompute_hash() != entry.hash {
            return Err(Error::Decode("qlog append entry_hash mismatch".into()));
        }
        configuration_state = configuration_state
            .validate_entry(entry)
            .map_err(|err| Error::Decode(err.to_string()))?;
        expected_prev_hash = entry.hash;
        if position + 1 < entries.len() {
            expected_index = expected_index
                .checked_add(1)
                .ok_or_else(|| Error::Decode("qlog index overflow".into()))?;
        }
    }
    Ok(configuration_state)
}

fn append_batch_to_open_segment(
    inner: &mut FileLogStoreInner,
    entries: &[LogEntry],
    sync: bool,
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    validate_append(inner, entries)?;
    for homogeneous in entries.chunk_by(|left, right| left.config_id == right.config_id) {
        let mut offset = 0;
        while offset < homogeneous.len() {
            ensure_open_segment(inner, &homogeneous[offset])?;
            let chunk_len = open_segment_chunk_len(
                inner.open_segment.as_ref().expect("open segment exists"),
                &homogeneous[offset..],
            )?;
            if chunk_len == 0 {
                seal_open_segment(inner)?;
                continue;
            }

            let chunk = &homogeneous[offset..offset + chunk_len];
            let bytes = chunk.iter().flat_map(encode_frame).collect::<Vec<_>>();
            let open = inner.open_segment.as_mut().expect("open segment exists");
            let old_len = open.bytes_len as u64;
            if let Err(write_err) = open.file.write_all(&bytes) {
                if let Err(rollback_err) = open.file.set_len(old_len) {
                    return Err(Error::Io(format!(
                        "{write_err}; failed to roll back partial qlog append: {rollback_err}"
                    )));
                }
                return Err(Error::Io(write_err.to_string()));
            }
            open.bytes_len += bytes.len();
            open.entries.extend_from_slice(chunk);
            for entry in chunk {
                inner.configuration_state = inner
                    .configuration_state
                    .validate_entry(entry)
                    .map_err(|err| Error::Decode(err.to_string()))?;
            }
            offset += chunk_len;
        }
    }
    if sync {
        inner
            .open_segment
            .as_ref()
            .expect("non-empty append has open segment")
            .file
            .sync_data()
            .map_err(|err| Error::Io(err.to_string()))?;
    }
    Ok(())
}

fn open_segment_chunk_len(open: &OpenSegment, entries: &[LogEntry]) -> Result<usize> {
    let mut bytes_len = open.bytes_len;
    let mut entry_count = open.entries.len();
    let mut chunk_len = 0;
    for entry in entries {
        let frame_len = FRAME_MIN_LEN
            .checked_add(entry.payload.len())
            .ok_or_else(|| Error::Decode("qlog frame length overflow".into()))?;
        let next_bytes_len = bytes_len
            .checked_add(frame_len)
            .ok_or_else(|| Error::Decode("qlog segment length overflow".into()))?;
        let next_entry_count = entry_count
            .checked_add(1)
            .ok_or_else(|| Error::Decode("qlog segment entry count overflow".into()))?;
        let oversized_first_entry = open.entries.is_empty() && chunk_len == 0;
        if !oversized_first_entry
            && (next_bytes_len > OPEN_SEGMENT_MAX_BYTES
                || next_entry_count > OPEN_SEGMENT_MAX_ENTRIES)
        {
            break;
        }
        bytes_len = next_bytes_len;
        entry_count = next_entry_count;
        chunk_len += 1;
    }
    Ok(chunk_len)
}

fn ensure_open_segment(inner: &mut FileLogStoreInner, first: &LogEntry) -> Result<()> {
    if inner
        .open_segment
        .as_ref()
        .is_some_and(|segment| segment.config_id == first.config_id)
    {
        return Ok(());
    }
    seal_open_segment(inner)?;

    let final_path = inner.dir.join(open_segment_file_name(first.index));
    if final_path.exists() {
        return Err(Error::Io(format!(
            "open qlog segment already exists: {}",
            final_path.display()
        )));
    }
    let (temp_path, mut file) = create_unique_temp_file(&inner.dir, &final_path)?;
    file.write_all(&encode_header(first))
        .and_then(|_| file.sync_all())
        .map_err(|err| Error::Io(err.to_string()))?;
    fs::rename(&temp_path, &final_path).map_err(|err| Error::Io(err.to_string()))?;
    sync_directory(&inner.dir)?;
    let file = fs::OpenOptions::new()
        .append(true)
        .open(&final_path)
        .map_err(|err| Error::Io(err.to_string()))?;
    inner.open_segment = Some(OpenSegment {
        config_id: first.config_id,
        path: final_path,
        file,
        bytes_len: QLOG_HEADER_LEN,
        entries: Vec::new(),
    });
    Ok(())
}

fn seal_open_segment(inner: &mut FileLogStoreInner) -> Result<()> {
    let Some(open) = &inner.open_segment else {
        return Ok(());
    };
    open.file
        .sync_data()
        .map_err(|err| Error::Io(err.to_string()))?;
    let entries = open.entries.clone();
    let open_path = open.path.clone();

    if entries.is_empty() {
        fs::remove_file(&open_path).map_err(|err| Error::Io(err.to_string()))?;
        inner.open_segment = None;
        sync_directory(&inner.dir)?;
        return Ok(());
    }

    let range = IndexRange::new(entries[0].index, entries.last().expect("non-empty").index)?;
    let final_path = inner.dir.join(segment_file_name(range));
    if final_path.exists() {
        let existing = fs::read(&final_path).map_err(|err| Error::Io(err.to_string()))?;
        if decode_segment_for_cluster(&existing, &inner.cluster_id)? != entries {
            return Err(Error::Decode(
                "open and closed qlog segments disagree".into(),
            ));
        }
    } else {
        publish_closed_segment(&inner.dir, &entries)?;
    }
    fs::remove_file(&open_path).map_err(|err| Error::Io(err.to_string()))?;
    inner.open_segment = None;
    inner.segments.push(ClosedSegment { entries });
    sync_directory(&inner.dir)
}

fn compact_prefix_with_hook(
    inner: &mut FileLogStoreInner,
    anchor: &RecoveryAnchor,
    hook: &mut impl FnMut(CompactPhase) -> Result<()>,
) -> Result<()> {
    if !validate_compaction(inner, anchor)? {
        return Ok(());
    }
    let target = anchor.compacted().index();

    let old_segments = inner
        .segments
        .iter()
        .take_while(|segment| segment.start() <= target)
        .collect::<Vec<_>>();
    let old_segment_names = old_segments
        .iter()
        .map(|segment| {
            segment_file_name(
                IndexRange::new(segment.start(), segment.end())
                    .expect("closed segment range is valid"),
            )
        })
        .collect::<Vec<_>>();
    let replacement_entries = old_segments
        .last()
        .filter(|segment| segment.end() > target)
        .map(|segment| {
            segment
                .entries
                .iter()
                .filter(|entry| entry.index > target)
                .cloned()
                .collect::<Vec<_>>()
        });
    let replacement = match replacement_entries {
        Some(entries) if !entries.is_empty() => {
            let first = entries.first().expect("replacement is non-empty");
            let last = entries.last().expect("replacement is non-empty");
            let final_name = segment_file_name(IndexRange::new(first.index, last.index)?);
            let final_path = inner.dir.join(&final_name);
            let (temp_path, mut file) = create_unique_temp_file(&inner.dir, &final_path)?;
            file.write_all(&encode_segment(&entries))
                .and_then(|_| file.sync_all())
                .map_err(|err| Error::Io(err.to_string()))?;
            drop(file);
            sync_directory(&inner.dir)?;
            hook(CompactPhase::ReplacementPrepared)?;
            Some(TruncateReplacement {
                temp_name: file_name(&temp_path)?,
                final_name,
            })
        }
        _ => None,
    };
    let intent = CompactIntent {
        previous_anchor: inner.anchor.clone(),
        anchor: anchor.clone(),
        old_segment_names,
        replacement,
    };
    publish_compact_intent(&inner.dir, &intent, hook)?;
    apply_compact_intent(&inner.dir, &intent, hook)
}

fn validate_compaction(inner: &FileLogStoreInner, anchor: &RecoveryAnchor) -> Result<bool> {
    validate_anchor(anchor)?;
    validate_anchor_identity(Some(anchor), &inner.cluster_id, inner.epoch)?;
    let target = anchor.compacted().index();
    let tip = inner.last_index();
    if tip.is_none_or(|tip| target > tip) {
        return Err(Error::CompactionAboveTip { target, tip });
    }
    if configuration_state_at(inner, target)? != *anchor.configuration_state() {
        return Err(Error::CompactionConflict { index: target });
    }

    if let Some(current) = &inner.anchor {
        let current_index = current.compacted().index();
        if target < current_index {
            return Err(Error::CompactionRegression {
                target,
                anchor: current_index,
            });
        }
        if target == current_index {
            return if current == anchor {
                Ok(false)
            } else {
                Err(Error::CompactionConflict { index: target })
            };
        }
        if anchor.recovery_generation() != current.recovery_generation() {
            return Err(Error::CompactionConflict { index: target });
        }
    }

    let entry = inner
        .segments
        .iter()
        .flat_map(|segment| &segment.entries)
        .chain(
            inner
                .open_segment
                .iter()
                .flat_map(|segment| &segment.entries),
        )
        .find(|entry| entry.index == target)
        .ok_or(Error::CompactionAboveTip { target, tip })?;
    if entry.hash != anchor.compacted().hash() {
        return Err(Error::CompactionHashMismatch { index: target });
    }
    Ok(true)
}

fn configuration_state_at(
    inner: &FileLogStoreInner,
    target: LogIndex,
) -> Result<ConfigurationState> {
    let (mut state, start) = match &inner.anchor {
        Some(anchor) => (
            anchor.configuration_state().clone(),
            anchor.compacted().index(),
        ),
        None => (inner.initial_configuration.clone(), 0),
    };
    if target == start {
        return Ok(state);
    }
    let mut found = false;
    for entry in inner
        .segments
        .iter()
        .flat_map(|segment| &segment.entries)
        .chain(
            inner
                .open_segment
                .iter()
                .flat_map(|segment| &segment.entries),
        )
    {
        if entry.index > target {
            break;
        }
        state = state
            .validate_entry(entry)
            .map_err(|err| Error::Decode(err.to_string()))?;
        found = entry.index == target;
    }
    if found {
        Ok(state)
    } else {
        Err(Error::CompactionAboveTip {
            target,
            tip: inner.last_index(),
        })
    }
}

fn read_anchor(dir: &Path) -> Result<Option<RecoveryAnchor>> {
    let path = dir.join(ANCHOR_FILE_NAME);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).map_err(|err| Error::Io(err.to_string()))?;
    decode_anchor(&bytes).map(Some)
}

fn validate_anchor_identity(
    anchor: Option<&RecoveryAnchor>,
    cluster_id: &str,
    epoch: u64,
) -> Result<()> {
    if let Some(anchor) = anchor {
        validate_anchor(anchor)?;
        if anchor.cluster_id() != cluster_id || anchor.epoch() != epoch {
            return Err(Error::Decode(
                "recovery anchor does not match configured cluster/epoch".into(),
            ));
        }
    }
    Ok(())
}

fn validate_anchor(anchor: &RecoveryAnchor) -> Result<()> {
    if anchor.format_version() != RECOVERY_ANCHOR_FORMAT_VERSION {
        return Err(Error::Decode("unsupported recovery anchor version".into()));
    }
    if anchor.cluster_id().is_empty()
        || anchor.recovery_generation() == 0
        || anchor.compacted().index() == 0
        || anchor.snapshot().snapshot_id().is_empty()
        || anchor.snapshot().size_bytes() == 0
    {
        return Err(Error::Decode("invalid recovery anchor identity".into()));
    }
    if anchor.configuration_state().config_id() != anchor.config_id()
        || anchor
            .configuration_state()
            .stop()
            .is_some_and(|stop| stop != anchor.compacted())
    {
        return Err(Error::Decode(
            "invalid recovery anchor configuration state".into(),
        ));
    }
    Ok(())
}

fn encode_anchor(anchor: &RecoveryAnchor) -> Result<Vec<u8>> {
    validate_anchor(anchor)?;
    let mut out = Vec::new();
    out.extend_from_slice(&ANCHOR_MAGIC);
    put_u16(&mut out, ANCHOR_VERSION);
    put_u16(&mut out, 0);
    put_u64(&mut out, anchor.epoch());
    put_u64(&mut out, anchor.config_id());
    put_u64(&mut out, anchor.recovery_generation());
    put_u64(&mut out, anchor.compacted().index());
    out.extend_from_slice(anchor.compacted().hash().as_bytes());
    out.extend_from_slice(anchor.snapshot().digest().as_bytes());
    put_u64(&mut out, anchor.snapshot().size_bytes());
    out.push(1);
    out.extend_from_slice(anchor.executor_fingerprint().as_bytes());
    encode_configuration_state(&mut out, anchor.configuration_state())?;
    put_string(&mut out, anchor.cluster_id(), "anchor cluster_id")?;
    put_string(
        &mut out,
        anchor.snapshot().snapshot_id(),
        "anchor snapshot_id",
    )?;
    let crc = crc32c(&out);
    put_u32(&mut out, crc);
    Ok(out)
}

fn decode_anchor(bytes: &[u8]) -> Result<RecoveryAnchor> {
    if bytes.len() < 120 || bytes.get(..4) != Some(ANCHOR_MAGIC.as_slice()) {
        return Err(Error::Decode("invalid recovery anchor magic".into()));
    }
    let crc_offset = bytes.len() - 4;
    if crc32c(&bytes[..crc_offset]) != read_u32(bytes, crc_offset)? {
        return Err(Error::Decode("recovery anchor crc mismatch".into()));
    }
    let version = read_u16(bytes, 4)?;
    if version != ANCHOR_VERSION || read_u16(bytes, 6)? != 0 {
        return Err(Error::Decode("unsupported recovery anchor version".into()));
    }
    if bytes.get(112) != Some(&1) {
        return Err(Error::Decode(
            "invalid executor fingerprint encoding".into(),
        ));
    }
    let executor_fingerprint = read_hash(bytes, 113)?;
    let mut cursor = 145;
    let config_id = read_u64(bytes, 16)?;
    let configuration_state =
        decode_configuration_state(bytes, &mut cursor, crc_offset, config_id)?;
    let cluster_id = read_string(bytes, &mut cursor, crc_offset, "anchor cluster_id")?;
    let snapshot_id = read_string(bytes, &mut cursor, crc_offset, "anchor snapshot_id")?;
    if cursor != crc_offset {
        return Err(Error::Decode("trailing recovery anchor bytes".into()));
    }
    let compacted = LogAnchor::new(read_u64(bytes, 32)?, read_hash(bytes, 40)?);
    let snapshot = SnapshotIdentity::new(
        snapshot_id,
        read_hash(bytes, 72)?,
        read_u64(bytes, 104)?,
        executor_fingerprint,
    );
    let anchor = RecoveryAnchor::new(
        cluster_id,
        read_u64(bytes, 8)?,
        configuration_state,
        read_u64(bytes, 24)?,
        compacted,
        snapshot,
    );
    validate_anchor(&anchor)?;
    Ok(anchor)
}

fn encode_configuration_state(out: &mut Vec<u8>, state: &ConfigurationState) -> Result<()> {
    match state {
        ConfigurationState::Active { digest, .. } => {
            out.push(1);
            out.extend_from_slice(digest.as_bytes());
        }
        ConfigurationState::Stopped {
            digest,
            stop,
            binding,
            ..
        } => {
            out.push(2);
            out.extend_from_slice(digest.as_bytes());
            put_u64(out, stop.index());
            out.extend_from_slice(stop.hash().as_bytes());
            match binding {
                StopBinding::Unbound => out.push(1),
                StopBinding::Bound {
                    successor,
                    stop_command_hash,
                } => {
                    out.push(2);
                    encode_successor_descriptor(out, successor)?;
                    out.extend_from_slice(stop_command_hash.as_bytes());
                }
            }
        }
    }
    Ok(())
}

fn decode_configuration_state(
    bytes: &[u8],
    cursor: &mut usize,
    end: usize,
    config_id: u64,
) -> Result<ConfigurationState> {
    let kind = *bytes
        .get(*cursor)
        .filter(|_| *cursor < end)
        .ok_or_else(|| Error::Decode("short recovery anchor configuration state".into()))?;
    *cursor += 1;
    let digest = read_state_hash(bytes, cursor, end)?;
    match kind {
        1 => Ok(ConfigurationState::active(config_id, digest)),
        2 => {
            let stop_index = read_state_u64(bytes, cursor, end)?;
            let stop_hash = read_state_hash(bytes, cursor, end)?;
            let binding = decode_stop_binding(bytes, cursor, end)?;
            Ok(ConfigurationState::Stopped {
                config_id,
                digest,
                stop: LogAnchor::new(stop_index, stop_hash),
                binding,
            })
        }
        _ => Err(Error::Decode(
            "invalid recovery anchor configuration state".into(),
        )),
    }
}

fn encode_successor_descriptor(out: &mut Vec<u8>, successor: &SuccessorDescriptor) -> Result<()> {
    put_string(out, successor.cluster_id(), "successor cluster_id")?;
    put_u64(out, successor.predecessor_config_id());
    out.extend_from_slice(successor.predecessor_config_digest().as_bytes());
    put_u64(out, successor.config_id());
    out.extend_from_slice(successor.digest().as_bytes());
    put_u16(out, successor.members().len() as u16);
    for member in successor.members() {
        put_string(out, member, "successor member")?;
    }
    Ok(())
}

fn decode_stop_binding(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<StopBinding> {
    let kind = read_state_u8(bytes, cursor, end)?;
    match kind {
        1 => Ok(StopBinding::Unbound),
        2 => {
            let cluster_id = read_string(bytes, cursor, end, "successor cluster_id")?;
            let predecessor_config_id = read_state_u64(bytes, cursor, end)?;
            let predecessor_config_digest = read_state_hash(bytes, cursor, end)?;
            let successor_config_id = read_state_u64(bytes, cursor, end)?;
            let encoded_digest = read_state_hash(bytes, cursor, end)?;
            let member_count = usize::from(read_state_u16(bytes, cursor, end)?);
            let mut members = Vec::with_capacity(member_count);
            for _ in 0..member_count {
                members.push(read_string(bytes, cursor, end, "successor member")?);
            }
            let successor = SuccessorDescriptor::new(
                cluster_id,
                predecessor_config_id,
                predecessor_config_digest,
                successor_config_id,
                members,
            )
            .map_err(|_| Error::Decode("invalid recovery anchor successor descriptor".into()))?;
            if successor.digest() != encoded_digest {
                return Err(Error::Decode(
                    "recovery anchor successor digest mismatch".into(),
                ));
            }
            let stop_command_hash = read_state_hash(bytes, cursor, end)?;
            Ok(StopBinding::Bound {
                successor,
                stop_command_hash,
            })
        }
        _ => Err(Error::Decode("invalid recovery anchor stop binding".into())),
    }
}

fn read_state_u8(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<u8> {
    let value = *bytes
        .get(*cursor)
        .filter(|_| *cursor < end)
        .ok_or_else(|| Error::Decode("short recovery anchor configuration state".into()))?;
    *cursor += 1;
    Ok(value)
}

fn read_state_u16(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<u16> {
    let next = cursor
        .checked_add(2)
        .filter(|next| *next <= end)
        .ok_or_else(|| Error::Decode("short recovery anchor configuration state".into()))?;
    let value = read_u16(bytes, *cursor)?;
    *cursor = next;
    Ok(value)
}

fn read_state_u64(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<u64> {
    let next = cursor
        .checked_add(8)
        .filter(|next| *next <= end)
        .ok_or_else(|| Error::Decode("short recovery anchor configuration state".into()))?;
    let value = read_u64(bytes, *cursor)?;
    *cursor = next;
    Ok(value)
}

fn read_state_hash(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<LogHash> {
    let next = cursor
        .checked_add(32)
        .filter(|next| *next <= end)
        .ok_or_else(|| Error::Decode("short recovery anchor configuration state".into()))?;
    let value = read_hash(bytes, *cursor)?;
    *cursor = next;
    Ok(value)
}

fn recover_compact_intent(dir: &Path) -> Result<()> {
    let path = dir.join(COMPACT_INTENT_FILE_NAME);
    if !path.exists() {
        return Ok(());
    }
    let bytes = fs::read(path).map_err(|err| Error::Io(err.to_string()))?;
    let intent = decode_compact_intent(&bytes)?;
    apply_compact_intent(dir, &intent, &mut |_| Ok(()))
}

fn publish_compact_intent(
    dir: &Path,
    intent: &CompactIntent,
    hook: &mut impl FnMut(CompactPhase) -> Result<()>,
) -> Result<()> {
    let final_path = dir.join(COMPACT_INTENT_FILE_NAME);
    if final_path.exists() {
        return Err(Error::Io("compact intent already exists".into()));
    }
    let (temp_path, mut file) = create_unique_temp_file(dir, &final_path)?;
    let bytes = encode_compact_intent(intent)?;
    if let Err(err) = file
        .write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|err| Error::Io(err.to_string()))
    {
        drop(file);
        let _ = fs::remove_file(&temp_path);
        return Err(err);
    }
    drop(file);
    fs::rename(&temp_path, &final_path).map_err(|err| Error::Io(err.to_string()))?;
    hook(CompactPhase::IntentRenamed)?;
    sync_directory(dir)?;
    hook(CompactPhase::IntentDurable)
}

fn apply_compact_intent(
    dir: &Path,
    intent: &CompactIntent,
    hook: &mut impl FnMut(CompactPhase) -> Result<()>,
) -> Result<()> {
    validate_compact_intent(intent)?;
    install_anchor(dir, intent)?;
    hook(CompactPhase::AnchorInstalled)?;
    sync_directory(dir)?;
    hook(CompactPhase::AnchorDurable)?;

    for (position, name) in intent.old_segment_names.iter().enumerate() {
        let path = dir.join(name);
        if path.exists() {
            fs::remove_file(path).map_err(|err| Error::Io(err.to_string()))?;
        }
        hook(CompactPhase::OldSegmentRemoved(position))?;
    }
    install_replacement(dir, intent.replacement.as_ref(), "compact")?;
    hook(CompactPhase::ReplacementInstalled)?;
    sync_directory(dir)?;
    hook(CompactPhase::AppliedDirectorySynced)?;

    let path = dir.join(COMPACT_INTENT_FILE_NAME);
    if path.exists() {
        fs::remove_file(path).map_err(|err| Error::Io(err.to_string()))?;
    }
    hook(CompactPhase::IntentRemoved)?;
    sync_directory(dir)?;
    hook(CompactPhase::CompleteDirectorySynced)
}

fn install_anchor(dir: &Path, intent: &CompactIntent) -> Result<()> {
    let current = read_anchor(dir)?;
    if current.as_ref() == Some(&intent.anchor) {
        return Ok(());
    }
    if current != intent.previous_anchor {
        return Err(Error::CompactionConflict {
            index: intent.anchor.compacted().index(),
        });
    }
    let final_path = dir.join(ANCHOR_FILE_NAME);
    let (temp_path, mut file) = create_unique_temp_file(dir, &final_path)?;
    file.write_all(&encode_anchor(&intent.anchor)?)
        .and_then(|_| file.sync_all())
        .map_err(|err| Error::Io(err.to_string()))?;
    drop(file);
    fs::rename(temp_path, final_path).map_err(|err| Error::Io(err.to_string()))
}

fn install_replacement(
    dir: &Path,
    replacement: Option<&TruncateReplacement>,
    operation: &str,
) -> Result<()> {
    let Some(replacement) = replacement else {
        return Ok(());
    };
    let temp_path = dir.join(&replacement.temp_name);
    let final_path = dir.join(&replacement.final_name);
    match (temp_path.exists(), final_path.exists()) {
        (true, false) => {
            fs::rename(temp_path, final_path).map_err(|err| Error::Io(err.to_string()))?;
        }
        (true, true) => {
            let temp = fs::read(&temp_path).map_err(|err| Error::Io(err.to_string()))?;
            let final_bytes = fs::read(&final_path).map_err(|err| Error::Io(err.to_string()))?;
            if temp != final_bytes {
                return Err(Error::Decode(format!(
                    "{operation} replacement files disagree"
                )));
            }
            fs::remove_file(temp_path).map_err(|err| Error::Io(err.to_string()))?;
        }
        (false, true) => {}
        (false, false) => {
            return Err(Error::Decode(format!(
                "{operation} replacement file is missing"
            )));
        }
    }
    Ok(())
}

fn truncate_suffix_with_hook(
    inner: &mut FileLogStoreInner,
    from: LogIndex,
    hook: &mut impl FnMut(TruncatePhase) -> Result<()>,
) -> Result<()> {
    let Some(position) = inner
        .segments
        .iter()
        .position(|segment| segment.end() >= from)
    else {
        return Ok(());
    };

    let old_segment_names = inner.segments[position..]
        .iter()
        .map(|segment| {
            segment_file_name(
                IndexRange::new(segment.start(), segment.end())
                    .expect("closed segment range is valid"),
            )
        })
        .collect::<Vec<_>>();

    let replacement_entries = (inner.segments[position].start() < from).then(|| {
        inner.segments[position]
            .entries
            .iter()
            .take_while(|entry| entry.index < from)
            .cloned()
            .collect::<Vec<_>>()
    });
    let replacement = match replacement_entries {
        Some(entries) if !entries.is_empty() => {
            let first = entries.first().expect("replacement is non-empty");
            let last = entries.last().expect("replacement is non-empty");
            let final_name = segment_file_name(IndexRange::new(first.index, last.index)?);
            let final_path = inner.dir.join(&final_name);
            let (temp_path, mut file) = create_unique_temp_file(&inner.dir, &final_path)?;
            file.write_all(&encode_segment(&entries))
                .and_then(|_| file.sync_all())
                .map_err(|err| Error::Io(err.to_string()))?;
            drop(file);
            sync_directory(&inner.dir)?;
            hook(TruncatePhase::ReplacementPrepared)?;
            Some(TruncateReplacement {
                temp_name: file_name(&temp_path)?,
                final_name,
            })
        }
        _ => None,
    };

    let intent = TruncateIntent {
        old_segment_names,
        replacement,
    };
    publish_truncate_intent(&inner.dir, &intent, hook)?;
    apply_truncate_intent(&inner.dir, &intent, hook)
}

fn recover_truncate_intent(dir: &Path) -> Result<()> {
    let path = dir.join(TRUNCATE_INTENT_FILE_NAME);
    if !path.exists() {
        return Ok(());
    }
    let bytes = fs::read(&path).map_err(|err| Error::Io(err.to_string()))?;
    let intent = decode_truncate_intent(&bytes)?;
    apply_truncate_intent(dir, &intent, &mut |_| Ok(()))
}

fn publish_truncate_intent(
    dir: &Path,
    intent: &TruncateIntent,
    hook: &mut impl FnMut(TruncatePhase) -> Result<()>,
) -> Result<()> {
    let final_path = dir.join(TRUNCATE_INTENT_FILE_NAME);
    if final_path.exists() {
        return Err(Error::Io("truncate intent already exists".into()));
    }
    let (temp_path, mut file) = create_unique_temp_file(dir, &final_path)?;
    let bytes = encode_truncate_intent(intent)?;
    if let Err(err) = file
        .write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|err| Error::Io(err.to_string()))
    {
        drop(file);
        let _ = fs::remove_file(&temp_path);
        return Err(err);
    }
    drop(file);
    fs::rename(&temp_path, &final_path).map_err(|err| Error::Io(err.to_string()))?;
    hook(TruncatePhase::IntentRenamed)?;
    sync_directory(dir)?;
    hook(TruncatePhase::IntentDurable)
}

fn apply_truncate_intent(
    dir: &Path,
    intent: &TruncateIntent,
    hook: &mut impl FnMut(TruncatePhase) -> Result<()>,
) -> Result<()> {
    validate_truncate_intent(intent)?;
    for (position, name) in intent.old_segment_names.iter().enumerate() {
        let path = dir.join(name);
        if path.exists() {
            fs::remove_file(&path).map_err(|err| Error::Io(err.to_string()))?;
        }
        hook(TruncatePhase::OldSegmentRemoved(position))?;
    }

    if let Some(replacement) = &intent.replacement {
        let temp_path = dir.join(&replacement.temp_name);
        let final_path = dir.join(&replacement.final_name);
        match (temp_path.exists(), final_path.exists()) {
            (true, false) => {
                fs::rename(&temp_path, &final_path).map_err(|err| Error::Io(err.to_string()))?;
            }
            (true, true) => {
                let temp = fs::read(&temp_path).map_err(|err| Error::Io(err.to_string()))?;
                let final_bytes =
                    fs::read(&final_path).map_err(|err| Error::Io(err.to_string()))?;
                if temp != final_bytes {
                    return Err(Error::Decode("truncate replacement files disagree".into()));
                }
                fs::remove_file(&temp_path).map_err(|err| Error::Io(err.to_string()))?;
            }
            (false, true) => {}
            (false, false) => {
                return Err(Error::Decode("truncate replacement file is missing".into()));
            }
        }
    }
    hook(TruncatePhase::ReplacementInstalled)?;
    sync_directory(dir)?;
    hook(TruncatePhase::AppliedDirectorySynced)?;

    let intent_path = dir.join(TRUNCATE_INTENT_FILE_NAME);
    if intent_path.exists() {
        fs::remove_file(&intent_path).map_err(|err| Error::Io(err.to_string()))?;
    }
    hook(TruncatePhase::IntentRemoved)?;
    sync_directory(dir)?;
    hook(TruncatePhase::CompleteDirectorySynced)
}

fn encode_truncate_intent(intent: &TruncateIntent) -> Result<Vec<u8>> {
    validate_truncate_intent(intent)?;
    let mut out = Vec::new();
    out.extend_from_slice(&TRUNCATE_INTENT_MAGIC);
    put_u16(&mut out, TRUNCATE_INTENT_VERSION);
    put_u16(
        &mut out,
        if intent.replacement.is_some() {
            TRUNCATE_INTENT_REPLACEMENT
        } else {
            0
        },
    );
    let count = u32::try_from(intent.old_segment_names.len())
        .map_err(|_| Error::Decode("too many truncate segments".into()))?;
    put_u32(&mut out, count);
    for name in &intent.old_segment_names {
        put_intent_name(&mut out, name)?;
    }
    if let Some(replacement) = &intent.replacement {
        put_intent_name(&mut out, &replacement.temp_name)?;
        put_intent_name(&mut out, &replacement.final_name)?;
    }
    let crc = crc32c(&out);
    put_u32(&mut out, crc);
    Ok(out)
}

fn decode_truncate_intent(bytes: &[u8]) -> Result<TruncateIntent> {
    if bytes.len() < 16 || bytes.get(..4) != Some(TRUNCATE_INTENT_MAGIC.as_slice()) {
        return Err(Error::Decode("invalid truncate intent magic".into()));
    }
    let crc_offset = bytes.len() - 4;
    if crc32c(&bytes[..crc_offset]) != read_u32(bytes, crc_offset)? {
        return Err(Error::Decode("truncate intent crc mismatch".into()));
    }
    if read_u16(bytes, 4)? != TRUNCATE_INTENT_VERSION {
        return Err(Error::Decode("unsupported truncate intent version".into()));
    }
    let flags = read_u16(bytes, 6)?;
    if flags & !TRUNCATE_INTENT_REPLACEMENT != 0 {
        return Err(Error::Decode("invalid truncate intent flags".into()));
    }
    let count = read_u32(bytes, 8)? as usize;
    let mut cursor = 12;
    let mut old_segment_names = Vec::with_capacity(count);
    for _ in 0..count {
        old_segment_names.push(read_intent_name(bytes, &mut cursor, crc_offset)?);
    }
    let replacement = if flags & TRUNCATE_INTENT_REPLACEMENT != 0 {
        Some(TruncateReplacement {
            temp_name: read_intent_name(bytes, &mut cursor, crc_offset)?,
            final_name: read_intent_name(bytes, &mut cursor, crc_offset)?,
        })
    } else {
        None
    };
    if cursor != crc_offset {
        return Err(Error::Decode("trailing truncate intent bytes".into()));
    }
    let intent = TruncateIntent {
        old_segment_names,
        replacement,
    };
    validate_truncate_intent(&intent)?;
    Ok(intent)
}

fn encode_compact_intent(intent: &CompactIntent) -> Result<Vec<u8>> {
    validate_compact_intent(intent)?;
    let mut flags = 0;
    if intent.previous_anchor.is_some() {
        flags |= COMPACT_INTENT_PREVIOUS_ANCHOR;
    }
    if intent.replacement.is_some() {
        flags |= COMPACT_INTENT_REPLACEMENT;
    }
    let mut out = Vec::new();
    out.extend_from_slice(&COMPACT_INTENT_MAGIC);
    put_u16(&mut out, COMPACT_INTENT_VERSION);
    put_u16(&mut out, flags);
    put_u32(
        &mut out,
        u32::try_from(intent.old_segment_names.len())
            .map_err(|_| Error::Decode("too many compact segments".into()))?,
    );
    put_blob(&mut out, &encode_anchor(&intent.anchor)?, "compact anchor")?;
    if let Some(previous) = &intent.previous_anchor {
        put_blob(
            &mut out,
            &encode_anchor(previous)?,
            "compact previous anchor",
        )?;
    }
    for name in &intent.old_segment_names {
        put_intent_name(&mut out, name)?;
    }
    if let Some(replacement) = &intent.replacement {
        put_intent_name(&mut out, &replacement.temp_name)?;
        put_intent_name(&mut out, &replacement.final_name)?;
    }
    let crc = crc32c(&out);
    put_u32(&mut out, crc);
    Ok(out)
}

fn decode_compact_intent(bytes: &[u8]) -> Result<CompactIntent> {
    if bytes.len() < 20 || bytes.get(..4) != Some(COMPACT_INTENT_MAGIC.as_slice()) {
        return Err(Error::Decode("invalid compact intent magic".into()));
    }
    let crc_offset = bytes.len() - 4;
    if crc32c(&bytes[..crc_offset]) != read_u32(bytes, crc_offset)? {
        return Err(Error::Decode("compact intent crc mismatch".into()));
    }
    if read_u16(bytes, 4)? != COMPACT_INTENT_VERSION {
        return Err(Error::Decode("unsupported compact intent version".into()));
    }
    let flags = read_u16(bytes, 6)?;
    if flags & !(COMPACT_INTENT_PREVIOUS_ANCHOR | COMPACT_INTENT_REPLACEMENT) != 0 {
        return Err(Error::Decode("invalid compact intent flags".into()));
    }
    let count = read_u32(bytes, 8)? as usize;
    let mut cursor = 12;
    let anchor = decode_anchor(read_blob(bytes, &mut cursor, crc_offset, "compact anchor")?)?;
    let previous_anchor = if flags & COMPACT_INTENT_PREVIOUS_ANCHOR != 0 {
        Some(decode_anchor(read_blob(
            bytes,
            &mut cursor,
            crc_offset,
            "compact previous anchor",
        )?)?)
    } else {
        None
    };
    let mut old_segment_names = Vec::with_capacity(count);
    for _ in 0..count {
        old_segment_names.push(read_intent_name(bytes, &mut cursor, crc_offset)?);
    }
    let replacement = if flags & COMPACT_INTENT_REPLACEMENT != 0 {
        Some(TruncateReplacement {
            temp_name: read_intent_name(bytes, &mut cursor, crc_offset)?,
            final_name: read_intent_name(bytes, &mut cursor, crc_offset)?,
        })
    } else {
        None
    };
    if cursor != crc_offset {
        return Err(Error::Decode("trailing compact intent bytes".into()));
    }
    let intent = CompactIntent {
        previous_anchor,
        anchor,
        old_segment_names,
        replacement,
    };
    validate_compact_intent(&intent)?;
    Ok(intent)
}

fn validate_compact_intent(intent: &CompactIntent) -> Result<()> {
    validate_anchor(&intent.anchor)?;
    if intent.old_segment_names.is_empty() {
        return Err(Error::Decode("compact intent has no old segments".into()));
    }
    if let Some(previous) = &intent.previous_anchor {
        validate_anchor(previous)?;
        if previous.cluster_id() != intent.anchor.cluster_id()
            || previous.epoch() != intent.anchor.epoch()
            || previous.recovery_generation() != intent.anchor.recovery_generation()
            || previous.compacted().index() >= intent.anchor.compacted().index()
        {
            return Err(Error::Decode(
                "compact intent anchor regression or conflict".into(),
            ));
        }
    }
    for (position, name) in intent.old_segment_names.iter().enumerate() {
        validate_closed_segment_name(name)?;
        if intent.old_segment_names[..position].contains(name) {
            return Err(Error::Decode("duplicate compact segment name".into()));
        }
    }
    if let Some(replacement) = &intent.replacement {
        validate_temp_name(&replacement.temp_name)?;
        validate_closed_segment_name(&replacement.final_name)?;
        if intent.old_segment_names.contains(&replacement.final_name) {
            return Err(Error::Decode(
                "compact replacement overlaps old segment name".into(),
            ));
        }
        let replacement_range = parse_closed_segment_name(&replacement.final_name)?;
        if replacement_range.start()
            != intent
                .anchor
                .compacted()
                .index()
                .checked_add(1)
                .ok_or_else(|| Error::Decode("compact anchor index overflow".into()))?
        {
            return Err(Error::Decode(
                "compact replacement does not start after anchor".into(),
            ));
        }
    }
    Ok(())
}

fn validate_truncate_intent(intent: &TruncateIntent) -> Result<()> {
    if intent.old_segment_names.is_empty() {
        return Err(Error::Decode("truncate intent has no old segments".into()));
    }
    for (position, name) in intent.old_segment_names.iter().enumerate() {
        validate_closed_segment_name(name)?;
        if intent.old_segment_names[..position].contains(name) {
            return Err(Error::Decode("duplicate truncate segment name".into()));
        }
    }
    if let Some(replacement) = &intent.replacement {
        validate_temp_name(&replacement.temp_name)?;
        validate_closed_segment_name(&replacement.final_name)?;
        if intent.old_segment_names.contains(&replacement.final_name) {
            return Err(Error::Decode(
                "truncate replacement overlaps old segment name".into(),
            ));
        }
    }
    Ok(())
}

fn validate_closed_segment_name(name: &str) -> Result<()> {
    validate_relative_name(name)?;
    let range = parse_closed_segment_name(name)?;
    if segment_file_name(range) != name {
        return Err(Error::Decode("non-canonical truncate segment name".into()));
    }
    Ok(())
}

fn validate_temp_name(name: &str) -> Result<()> {
    validate_relative_name(name)?;
    if !name.starts_with('.') || !name.ends_with(".tmp") {
        return Err(Error::Decode("invalid truncate temp name".into()));
    }
    Ok(())
}

fn validate_relative_name(name: &str) -> Result<()> {
    let mut components = Path::new(name).components();
    if name.is_empty()
        || !matches!(components.next(), Some(std::path::Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(Error::Decode("unsafe truncate intent path".into()));
    }
    Ok(())
}

fn put_intent_name(out: &mut Vec<u8>, name: &str) -> Result<()> {
    let len = u16::try_from(name.len())
        .map_err(|_| Error::Decode("truncate intent name is too long".into()))?;
    put_u16(out, len);
    out.extend_from_slice(name.as_bytes());
    Ok(())
}

fn put_string(out: &mut Vec<u8>, value: &str, field: &str) -> Result<()> {
    let len =
        u16::try_from(value.len()).map_err(|_| Error::Decode(format!("{field} is too long")))?;
    put_u16(out, len);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], cursor: &mut usize, end: usize, field: &str) -> Result<String> {
    let value = read_intent_name(bytes, cursor, end)?;
    if value.is_empty() {
        return Err(Error::Decode(format!("{field} is empty")));
    }
    Ok(value)
}

fn put_blob(out: &mut Vec<u8>, bytes: &[u8], field: &str) -> Result<()> {
    let len =
        u32::try_from(bytes.len()).map_err(|_| Error::Decode(format!("{field} is too large")))?;
    put_u32(out, len);
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_blob<'a>(bytes: &'a [u8], cursor: &mut usize, end: usize, field: &str) -> Result<&'a [u8]> {
    let len = read_u32(bytes, *cursor)? as usize;
    *cursor = cursor
        .checked_add(4)
        .ok_or_else(|| Error::Decode(format!("{field} cursor overflow")))?;
    let value_end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::Decode(format!("{field} length overflow")))?;
    if value_end > end {
        return Err(Error::Decode(format!("short {field}")));
    }
    let value = &bytes[*cursor..value_end];
    *cursor = value_end;
    Ok(value)
}

fn read_intent_name(bytes: &[u8], cursor: &mut usize, end: usize) -> Result<String> {
    let len = read_u16(bytes, *cursor)? as usize;
    *cursor = cursor
        .checked_add(2)
        .ok_or_else(|| Error::Decode("truncate intent cursor overflow".into()))?;
    let name_end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::Decode("truncate intent name overflow".into()))?;
    if name_end > end {
        return Err(Error::Decode("short truncate intent name".into()));
    }
    let name = std::str::from_utf8(&bytes[*cursor..name_end])
        .map_err(|err| Error::Decode(err.to_string()))?
        .to_string();
    *cursor = name_end;
    Ok(name)
}

fn file_name(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
        .ok_or_else(|| Error::Decode("qlog temp filename is not UTF-8".into()))
}

fn publish_closed_segment(dir: &Path, entries: &[LogEntry]) -> Result<PathBuf> {
    let first = entries
        .first()
        .ok_or_else(|| Error::Decode("cannot write empty segment".into()))?;
    let last = entries
        .last()
        .ok_or_else(|| Error::Decode("cannot write empty segment".into()))?;
    let range = IndexRange::new(first.index, last.index)?;
    let final_path = dir.join(segment_file_name(range));
    if final_path.exists() {
        return Err(Error::Io(format!(
            "qlog segment already exists: {}",
            final_path.display()
        )));
    }

    let bytes = encode_segment(entries);
    let (temp_path, mut file) = create_unique_temp_file(dir, &final_path)?;
    if let Err(err) = file
        .write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|err| Error::Io(err.to_string()))
    {
        drop(file);
        let _ = fs::remove_file(&temp_path);
        return Err(err);
    }
    drop(file);
    if let Err(err) = fs::rename(&temp_path, &final_path) {
        let _ = fs::remove_file(&temp_path);
        return Err(Error::Io(err.to_string()));
    }
    sync_directory(dir)?;
    Ok(final_path)
}

fn create_unique_temp_file(dir: &Path, final_path: &Path) -> Result<(PathBuf, fs::File)> {
    let final_name = final_path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("generated qlog filename is UTF-8");
    loop {
        let id = NEXT_TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!(".{final_name}.{}.{}.tmp", std::process::id(), id));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => return Ok((path, file)),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(Error::Io(err.to_string())),
        }
    }
}

fn sync_directory(dir: &Path) -> Result<()> {
    fs::File::open(dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|err| Error::Io(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    const INJECTED_CRASH: &str = "injected crash";

    #[test]
    fn legacy_anchor_binary_version_is_rejected() {
        let entry = chain(&[b"one"]).pop().unwrap();
        let anchor = recovery_anchor(&entry);
        let mut bytes = encode_anchor(&anchor).unwrap();
        bytes[4..6].copy_from_slice(&3_u16.to_be_bytes());
        let crc_offset = bytes.len() - 4;
        let crc = crc32c(&bytes[..crc_offset]);
        bytes[crc_offset..].copy_from_slice(&crc.to_be_bytes());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(ANCHOR_FILE_NAME);
        fs::write(&path, &bytes).unwrap();

        assert!(matches!(
            FileLogStore::open(dir.path(), "cluster-a", 1, 1),
            Err(Error::Decode(_))
        ));
        assert_eq!(fs::read(path).unwrap(), bytes);
    }

    #[test]
    fn compact_crash_before_durable_intent_preserves_genesis_log() {
        let dir = tempfile::tempdir().unwrap();
        let entries = chain(&[b"one", b"two", b"three", b"four", b"five", b"six"]);
        let store = segmented_store(dir.path(), &entries);
        let anchor = recovery_anchor(&entries[2]);

        inject_compact_crash(&store, &anchor, CompactPhase::ReplacementPrepared);
        drop(store);

        assert_reopens_with(dir.path(), &entries);
        assert!(read_anchor(dir.path()).unwrap().is_none());
        assert!(!dir.path().join(COMPACT_INTENT_FILE_NAME).exists());
    }

    #[test]
    fn compact_rolls_forward_from_every_committed_phase() {
        let phases = [
            CompactPhase::IntentRenamed,
            CompactPhase::IntentDurable,
            CompactPhase::AnchorInstalled,
            CompactPhase::AnchorDurable,
            CompactPhase::OldSegmentRemoved(0),
            CompactPhase::OldSegmentRemoved(1),
            CompactPhase::ReplacementInstalled,
            CompactPhase::AppliedDirectorySynced,
            CompactPhase::IntentRemoved,
            CompactPhase::CompleteDirectorySynced,
        ];

        for crash_phase in phases {
            let dir = tempfile::tempdir().unwrap();
            let entries = chain(&[b"one", b"two", b"three", b"four", b"five", b"six"]);
            let store = segmented_store(dir.path(), &entries);
            let anchor = recovery_anchor(&entries[2]);

            inject_compact_crash(&store, &anchor, crash_phase);
            drop(store);

            let reopened = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
            assert_eq!(read_anchor(dir.path()).unwrap(), Some(anchor.clone()));
            assert_eq!(
                reopened.read_range(IndexRange::new(1, 6).unwrap()).unwrap(),
                entries[3..]
            );
            assert_eq!(reopened.last_index().unwrap(), Some(6));
            assert_no_overlapping_closed_segments(dir.path());
            assert!(!dir.path().join(COMPACT_INTENT_FILE_NAME).exists());
        }
    }

    #[test]
    fn corrupted_compact_intent_is_fatal_without_deleting_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let entries = chain(&[b"one", b"two", b"three", b"four"]);
        let store = segmented_store(dir.path(), &entries);
        let anchor = recovery_anchor(&entries[2]);
        inject_compact_crash(&store, &anchor, CompactPhase::IntentDurable);
        drop(store);
        let files_before = closed_segment_bytes(dir.path());
        let intent_path = dir.path().join(COMPACT_INTENT_FILE_NAME);
        let mut bytes = fs::read(&intent_path).unwrap();
        bytes[4] ^= 1;
        fs::write(&intent_path, bytes).unwrap();

        assert!(FileLogStore::open(dir.path(), "cluster-a", 1, 1).is_err());
        assert_eq!(closed_segment_bytes(dir.path()), files_before);
        assert!(read_anchor(dir.path()).unwrap().is_none());
    }

    #[test]
    fn reopen_preserves_original_log_when_crash_precedes_durable_intent() {
        let dir = tempfile::tempdir().unwrap();
        let entries = chain(&[b"one", b"two", b"three", b"four", b"five", b"six"]);
        let store = segmented_store(dir.path(), &entries);

        inject_truncate_crash(&store, 4, TruncatePhase::ReplacementPrepared);
        drop(store);

        assert_reopens_with(dir.path(), &entries);
        assert_no_overlapping_closed_segments(dir.path());
        assert!(!dir.path().join(TRUNCATE_INTENT_FILE_NAME).exists());
    }

    #[test]
    fn reopen_recovers_exact_prefix_from_every_durable_intent_phase() {
        let phases = [
            TruncatePhase::IntentRenamed,
            TruncatePhase::IntentDurable,
            TruncatePhase::OldSegmentRemoved(0),
            TruncatePhase::OldSegmentRemoved(1),
            TruncatePhase::ReplacementInstalled,
            TruncatePhase::AppliedDirectorySynced,
            TruncatePhase::IntentRemoved,
            TruncatePhase::CompleteDirectorySynced,
        ];

        for crash_phase in phases {
            let dir = tempfile::tempdir().unwrap();
            let entries = chain(&[b"one", b"two", b"three", b"four", b"five", b"six"]);
            let store = segmented_store(dir.path(), &entries);

            inject_truncate_crash(&store, 4, crash_phase);
            drop(store);

            assert_reopens_with(dir.path(), &entries[..3]);
            assert_no_overlapping_closed_segments(dir.path());
            assert!(!dir.path().join(TRUNCATE_INTENT_FILE_NAME).exists());
        }
    }

    #[test]
    fn corrupted_truncate_intent_is_fatal_without_removing_segments() {
        let dir = tempfile::tempdir().unwrap();
        let entries = chain(&[b"one", b"two", b"three", b"four"]);
        let store = segmented_store(dir.path(), &entries);
        inject_truncate_crash(&store, 3, TruncatePhase::IntentDurable);
        drop(store);
        let files_before = closed_segment_bytes(dir.path());
        let intent_path = dir.path().join(TRUNCATE_INTENT_FILE_NAME);
        let mut bytes = fs::read(&intent_path).unwrap();
        bytes[4] ^= 1;
        fs::write(&intent_path, bytes).unwrap();

        assert!(FileLogStore::open(dir.path(), "cluster-a", 1, 1).is_err());
        assert_eq!(closed_segment_bytes(dir.path()), files_before);
    }

    #[test]
    fn unsafe_truncate_intent_name_is_fatal_without_path_traversal() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("log");
        fs::create_dir(&dir).unwrap();
        let victim = root.path().join("victim.qlog");
        fs::write(&victim, b"keep").unwrap();
        let bytes = encode_unchecked_test_intent("../victim.qlog");
        fs::write(dir.join(TRUNCATE_INTENT_FILE_NAME), bytes).unwrap();

        assert!(FileLogStore::open(&dir, "cluster-a", 1, 1).is_err());
        assert_eq!(fs::read(victim).unwrap(), b"keep");
    }

    fn inject_truncate_crash(store: &FileLogStore, from: LogIndex, crash_phase: TruncatePhase) {
        let mut inner = store.lock().unwrap();
        let err = truncate_suffix_with_hook(&mut inner, from, &mut |phase| {
            if phase == crash_phase {
                Err(Error::Io(INJECTED_CRASH.into()))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert_eq!(err, Error::Io(INJECTED_CRASH.into()));
    }

    fn inject_compact_crash(
        store: &FileLogStore,
        anchor: &RecoveryAnchor,
        crash_phase: CompactPhase,
    ) {
        let mut inner = store.lock().unwrap();
        let err = compact_prefix_with_hook(&mut inner, anchor, &mut |phase| {
            if phase == crash_phase {
                Err(Error::Io(INJECTED_CRASH.into()))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert_eq!(err, Error::Io(INJECTED_CRASH.into()));
    }

    fn segmented_store(dir: &Path, entries: &[LogEntry]) -> FileLogStore {
        fs::create_dir_all(dir).unwrap();
        publish_closed_segment(dir, &entries[..2]).unwrap();
        publish_closed_segment(dir, &entries[2..4]).unwrap();
        if entries.len() > 4 {
            publish_closed_segment(dir, &entries[4..]).unwrap();
        }
        FileLogStore::open(dir, "cluster-a", 1, 1).unwrap()
    }

    fn assert_reopens_with(dir: &Path, expected: &[LogEntry]) {
        let reopened = FileLogStore::open(dir, "cluster-a", 1, 1).unwrap();
        assert_eq!(
            reopened.read_range(IndexRange::new(1, 6).unwrap()).unwrap(),
            expected
        );
        assert_eq!(
            reopened.last_index().unwrap(),
            expected.last().map(|entry| entry.index)
        );
    }

    fn assert_no_overlapping_closed_segments(dir: &Path) {
        let mut ranges = fs::read_dir(dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name.ends_with(".qlog") && !name.ends_with("-open.qlog"))
            .map(|name| parse_closed_segment_name(&name).unwrap())
            .collect::<Vec<_>>();
        ranges.sort_by_key(IndexRange::start);
        assert!(ranges
            .windows(2)
            .all(|pair| pair[0].end() < pair[1].start()));
    }

    fn closed_segment_bytes(dir: &Path) -> Vec<(String, Vec<u8>)> {
        let mut files = fs::read_dir(dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter_map(|entry| {
                entry
                    .file_name()
                    .into_string()
                    .ok()
                    .map(|name| (name, entry.path()))
            })
            .filter(|(name, _)| name.ends_with(".qlog") && !name.ends_with("-open.qlog"))
            .map(|(name, path)| (name, fs::read(path).unwrap()))
            .collect::<Vec<_>>();
        files.sort_by(|left, right| left.0.cmp(&right.0));
        files
    }

    fn encode_unchecked_test_intent(old_name: &str) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&TRUNCATE_INTENT_MAGIC);
        put_u16(&mut out, TRUNCATE_INTENT_VERSION);
        put_u16(&mut out, 0);
        put_u32(&mut out, 1);
        put_u16(&mut out, old_name.len() as u16);
        out.extend_from_slice(old_name.as_bytes());
        let crc = crc32c(&out);
        put_u32(&mut out, crc);
        out
    }

    fn chain(payloads: &[&[u8]]) -> Vec<LogEntry> {
        let mut entries = Vec::new();
        let mut prev_hash = LogHash::ZERO;
        for (position, payload) in payloads.iter().enumerate() {
            let index = position as u64 + 1;
            let hash = LogEntry::calculate_hash(
                "cluster-a",
                index,
                1,
                1,
                EntryType::Command,
                prev_hash,
                payload,
            );
            entries.push(LogEntry {
                cluster_id: "cluster-a".into(),
                epoch: 1,
                config_id: 1,
                index,
                entry_type: EntryType::Command,
                payload: payload.to_vec(),
                prev_hash,
                hash,
            });
            prev_hash = hash;
        }
        entries
    }

    fn recovery_anchor(entry: &LogEntry) -> RecoveryAnchor {
        RecoveryAnchor::new(
            entry.cluster_id.clone(),
            entry.epoch,
            ConfigurationState::active(entry.config_id, LogHash::ZERO),
            1,
            LogAnchor::new(entry.index, entry.hash),
            SnapshotIdentity::new(
                format!("snapshot-{:015}", entry.index),
                LogHash::digest(&[b"snapshot", &entry.index.to_be_bytes()]),
                4096,
                LogHash::from_bytes([6; 32]),
            ),
        )
    }
}
