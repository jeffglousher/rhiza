use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use rhiza_core::{
    ExternalEffectCommand, ExternalEffectProfile, LogHash, LogIndex, MAX_EXTERNAL_EFFECT_BYTES,
    MAX_EXTERNAL_EFFECT_PROFILE_MANIFEST_BYTES,
};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use sha2::{Digest, Sha256};

use crate::page_state::StateIdentityV3;
#[cfg(test)]
use crate::page_state::{PageStateCacheV3, PageStatePatchV3};
use crate::{Error, Result};

pub const QWAL_V3_MAGIC: &[u8; 6] = b"QWAL\0\x04";
pub const MAX_QWAL_V3_BYTES: usize = 512 * 1024;
pub const MAX_QWAL_V3_RECEIPTS: usize = 1024;

#[allow(dead_code)] // Wired into the Recorder command boundary in the next integration step.
const QWAL_EFFECT_RECEIPT_DOMAIN: &[u8] = b"rhiza:qwal-v4:receipt-result\0";

const SQLITE_HEADER_BYTES: usize = 100;
const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
const MIN_SQLITE_PAGE_SIZE: u32 = 512;
const MAX_SQLITE_PAGE_SIZE: u32 = 65_536;
const MAX_ID_BYTES: usize = 256;
const MAX_FINGERPRINT_BYTES: usize = 4 * 1024;

/// A final page image in a closed SQLite database.
///
/// Page numbers are one-based, as they are in SQLite's file format.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QwalPageV3 {
    pub page_no: u32,
    pub after_image: Vec<u8>,
}

/// One successful member of a QWAL v3 batch.
///
/// The decided entry anchor is intentionally absent: every receipt in the
/// envelope is installed at the single anchor of the log entry carrying the
/// batch.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QwalReceiptV3 {
    pub request_id: String,
    pub request_digest: LogHash,
    pub result_blob: Vec<u8>,
}

/// Canonical QWAL v3 page effect decided by the replicated log.
///
/// The envelope deliberately contains no local path or WAL-index state. The
/// caller owns the control-sidecar intent and atomic installation protocol.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QwalEnvelopeV3 {
    pub cluster_id: String,
    pub epoch: u64,
    pub configuration_id: u64,
    pub recovery_generation: u64,
    pub base_index: LogIndex,
    pub base_hash: LogHash,
    pub base_state: StateIdentityV3,
    pub target_state: StateIdentityV3,
    pub materializer_fingerprint: String,
    pub receipts: Vec<QwalReceiptV3>,
    pub pages: Vec<QwalPageV3>,
}

impl QwalEnvelopeV3 {
    /// Validates every structural property that does not require the base file.
    pub fn validate(&self) -> Result<()> {
        validate_nonempty_bounded("cluster_id", &self.cluster_id, MAX_ID_BYTES)?;
        validate_nonempty_bounded(
            "materializer_fingerprint",
            &self.materializer_fingerprint,
            MAX_FINGERPRINT_BYTES,
        )?;
        if self.receipts.is_empty() || self.receipts.len() > MAX_QWAL_V3_RECEIPTS {
            return invalid(format!(
                "QWAL receipts must contain 1..={MAX_QWAL_V3_RECEIPTS} members"
            ));
        }
        let mut result_bytes = 0usize;
        let mut request_ids = HashSet::with_capacity(self.receipts.len());
        for receipt in &self.receipts {
            validate_nonempty_bounded("request_id", &receipt.request_id, MAX_ID_BYTES)?;
            if !request_ids.insert(receipt.request_id.as_str()) {
                return invalid("QWAL receipt request ids must be unique");
            }
            crate::validate_sql_result_blob_bounds(&receipt.result_blob)
                .map_err(|_| Error::InvalidEntry("QWAL receipt result is not canonical".into()))?;
            result_bytes = result_bytes
                .checked_add(receipt.result_blob.len())
                .ok_or_else(|| Error::ResourceExhausted("QWAL result bytes overflow".into()))?;
            if result_bytes > MAX_QWAL_V3_BYTES {
                return Err(Error::ResourceExhausted(format!(
                    "QWAL results exceed {MAX_QWAL_V3_BYTES} bytes"
                )));
            }
        }

        validate_qwal_page_effect(
            &self.base_state,
            &self.target_state,
            &self.pages,
            MAX_QWAL_V3_BYTES,
        )
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_qwal_v3(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode_qwal_v3(bytes)
    }
}

/// The placement of one request result in an external effect bundle.
///
/// Bundle bytes begin with the concatenation of these result blobs in receipt
/// order. `result_offset` and `result_len` name the exact slice in that prefix;
/// the remaining bytes are the materializer-owned page effect. Result blobs
/// are not copied into the manifest: their domain-separated digest and the
/// digest of the complete bundle bind them to this decision.
#[allow(dead_code)] // Wired into the Recorder command boundary in the next integration step.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QwalReceiptReferenceV4 {
    pub request_id: String,
    pub request_digest: LogHash,
    pub result_offset: u32,
    pub result_len: u32,
    pub result_digest: LogHash,
}

/// Canonically postcard-encoded page effect stored after the receipt prefix.
///
/// The external bundle has no untyped remainder: after all manifest-declared
/// result slices, these exact bytes are the only permitted page-effect value.
#[allow(dead_code)] // Wired into the Recorder command boundary in the next integration step.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QwalPageEffectV4 {
    pub pages: Vec<QwalPageV3>,
}

/// Small, canonical decision record for an externally stored SQLite effect.
///
/// The Recorder finalizes every referenced chunk before this manifest is
/// proposed. A resolver must verify every chunk by index, concatenate exactly
/// the listed chunks, then call `verify_effect_bytes` before exposing either
/// receipt results or page data. That makes reordered, missing, and extra
/// chunks fail before canonical state is changed; duplicate-content chunks are
/// allowed because their position and the full bundle digest remain binding.
#[allow(dead_code)] // Wired into the Recorder command boundary in the next integration step.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QwalEffectManifestV4 {
    // Placement and chunk commitments live only in core's QEFX envelope.
    // This is profile-owned SQL interpretation data.
    pub recovery_generation: u64,
    pub base_state: StateIdentityV3,
    pub target_state: StateIdentityV3,
    pub materializer_fingerprint: String,
    pub receipts: Vec<QwalReceiptReferenceV4>,
}

#[allow(dead_code)] // Wired into the Recorder command boundary in the next integration step.
impl QwalEffectManifestV4 {
    /// Validates every property that can be checked without fetching chunks.
    pub fn validate(&self) -> Result<()> {
        validate_nonempty_bounded(
            "materializer_fingerprint",
            &self.materializer_fingerprint,
            MAX_FINGERPRINT_BYTES,
        )?;
        state_file_bytes(&self.base_state, "base")?;
        state_file_bytes(&self.target_state, "target")?;
        if self.base_state.page_size != self.target_state.page_size {
            return invalid("QWAL manifest base and target page sizes differ");
        }
        if self.receipts.is_empty() || self.receipts.len() > MAX_QWAL_V3_RECEIPTS {
            return invalid(format!(
                "QWAL manifest receipts must contain 1..={MAX_QWAL_V3_RECEIPTS} members"
            ));
        }
        let mut request_ids = HashSet::with_capacity(self.receipts.len());
        let mut result_end = 0u32;
        for receipt in &self.receipts {
            validate_nonempty_bounded("request_id", &receipt.request_id, MAX_ID_BYTES)?;
            if !request_ids.insert(receipt.request_id.as_str()) {
                return invalid("QWAL manifest receipt request ids must be unique");
            }
            if receipt.result_len == 0 || receipt.result_digest == LogHash::ZERO {
                return invalid("QWAL manifest receipt result must be non-empty and hashed");
            }
            if receipt.result_offset != result_end {
                return invalid("QWAL manifest receipt results must be contiguous and ordered");
            }
            result_end = result_end.checked_add(receipt.result_len).ok_or_else(|| {
                Error::ResourceExhausted("QWAL receipt result length overflows".into())
            })?;
        }
        if result_end as usize > MAX_QWAL_V3_BYTES {
            return Err(Error::ResourceExhausted(format!(
                "QWAL manifest receipt prefix exceeds {MAX_QWAL_V3_BYTES} bytes"
            )));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let encoded = postcard::to_allocvec(self)
            .map_err(|error| Error::InvalidEntry(format!("QWAL profile encode failed: {error}")))?;
        if encoded.len() > MAX_EXTERNAL_EFFECT_PROFILE_MANIFEST_BYTES {
            return Err(Error::ResourceExhausted(
                "QWAL profile manifest exceeds core bound".into(),
            ));
        }
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_EXTERNAL_EFFECT_PROFILE_MANIFEST_BYTES {
            return invalid("QWAL profile manifest is outside its byte bound");
        }
        let manifest: Self = postcard::from_bytes(bytes)
            .map_err(|error| Error::InvalidEntry(format!("QWAL profile decode failed: {error}")))?;
        manifest.validate()?;
        if postcard::to_allocvec(&manifest).map_err(|error| {
            Error::InvalidEntry(format!("QWAL profile re-encode failed: {error}"))
        })? != bytes
        {
            return invalid("QWAL profile manifest is not canonically encoded");
        }
        Ok(manifest)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn external_command(
        &self,
        cluster_id: impl Into<String>,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        intended_slot: LogIndex,
        prev_hash: LogHash,
        chunks: &[Vec<u8>],
    ) -> Result<ExternalEffectCommand> {
        ExternalEffectCommand::from_profile_bytes_and_chunks(
            cluster_id,
            epoch,
            config_id,
            config_digest,
            intended_slot,
            prev_hash,
            ExternalEffectProfile::sql(self.encode()?),
            chunks,
        )
        .map_err(|error| Error::InvalidEntry(format!("QEFX construction failed: {error}")))
    }

    pub fn from_external_command(command: &ExternalEffectCommand) -> Result<Self> {
        let ExternalEffectProfile::Sql { manifest } = command.profile();
        Self::decode(manifest)
    }

    /// Verifies whole-bundle bytes against core's canonical QEFX commitment,
    /// then interprets the SQL-only profile payload.
    pub fn verify_external_bundle(
        command: &ExternalEffectCommand,
        bytes: &[u8],
    ) -> Result<VerifiedQwalEffectBundleV4> {
        let manifest = Self::from_external_command(command)?;
        if bytes.len() != command.total_effect_bytes() as usize {
            return invalid("QEFX effect bytes do not match their committed length");
        }
        let mut offset = 0usize;
        for chunk in command.chunks() {
            let end = offset
                .checked_add(chunk.encoded_len() as usize)
                .ok_or_else(|| {
                    Error::ResourceExhausted("QWAL effect chunk offset overflows".into())
                })?;
            let Some(bytes) = bytes.get(offset..end) else {
                return invalid("QEFX effect bundle ends before its committed chunks");
            };
            if ExternalEffectCommand::chunk_digest(bytes) != chunk.digest() {
                return invalid("QEFX effect bundle chunks are not in committed order");
            }
            offset = end;
        }
        if offset != bytes.len() {
            return invalid("QEFX effect bundle has trailing bytes");
        }
        let mut receipts = Vec::with_capacity(manifest.receipts.len());
        let mut page_effect_offset = 0usize;
        for receipt in &manifest.receipts {
            let end = page_effect_offset
                .checked_add(receipt.result_len as usize)
                .ok_or_else(|| Error::ResourceExhausted("QWAL receipt offset overflows".into()))?;
            let result_blob = bytes.get(page_effect_offset..end).ok_or_else(|| {
                Error::InvalidEntry("QWAL bundle ends in its receipt prefix".into())
            })?;
            if qwal_effect_receipt_digest(result_blob) != receipt.result_digest {
                return invalid("QWAL receipt result does not match its manifest reference");
            }
            crate::validate_sql_result_blob_bounds(result_blob)
                .map_err(|_| Error::InvalidEntry("QWAL receipt result is not canonical".into()))?;
            receipts.push(QwalReceiptV3 {
                request_id: receipt.request_id.clone(),
                request_digest: receipt.request_digest,
                result_blob: result_blob.to_vec(),
            });
            page_effect_offset = end;
        }
        let page_effect = QwalPageEffectV4::decode(
            bytes
                .get(page_effect_offset..)
                .ok_or_else(|| Error::InvalidEntry("QWAL page effect offset is invalid".into()))?,
            &manifest.base_state,
            &manifest.target_state,
        )?;
        Ok(VerifiedQwalEffectBundleV4 {
            command: command.clone(),
            manifest,
            receipts,
            page_effect_offset: u32::try_from(page_effect_offset).map_err(|_| {
                Error::ResourceExhausted("QWAL page effect offset overflows".into())
            })?,
            page_effect,
        })
    }
}

/// Fully checked, typed external effect. Production code must consume this
/// value rather than treating the bundle suffix as raw page bytes.
#[allow(dead_code)] // Wired into the Recorder command boundary in the next integration step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedQwalEffectBundleV4 {
    command: ExternalEffectCommand,
    manifest: QwalEffectManifestV4,
    receipts: Vec<QwalReceiptV3>,
    page_effect_offset: u32,
    page_effect: QwalPageEffectV4,
}

#[allow(dead_code)] // Wired into the Recorder command boundary in the next integration step.
impl VerifiedQwalEffectBundleV4 {
    pub fn command(&self) -> &ExternalEffectCommand {
        &self.command
    }
    /// The immutable context that was verified with these exact bundle bytes.
    pub fn manifest(&self) -> &QwalEffectManifestV4 {
        &self.manifest
    }

    pub fn receipts(&self) -> &[QwalReceiptV3] {
        &self.receipts
    }

    pub const fn page_effect_offset(&self) -> u32 {
        self.page_effect_offset
    }

    pub fn page_effect(&self) -> &QwalPageEffectV4 {
        &self.page_effect
    }
}

#[allow(dead_code)] // Wired into the Recorder command boundary in the next integration step.
impl QwalPageEffectV4 {
    pub fn encode(&self, base: &StateIdentityV3, target: &StateIdentityV3) -> Result<Vec<u8>> {
        validate_qwal_page_effect(base, target, &self.pages, MAX_EXTERNAL_EFFECT_BYTES)?;
        postcard::to_allocvec(self).map_err(|error| {
            Error::InvalidEntry(format!("QWAL page effect encode failed: {error}"))
        })
    }

    pub fn decode(bytes: &[u8], base: &StateIdentityV3, target: &StateIdentityV3) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_EXTERNAL_EFFECT_BYTES {
            return invalid("QWAL external page effect has invalid length");
        }
        let effect: Self = postcard::from_bytes(bytes).map_err(|error| {
            Error::InvalidEntry(format!("QWAL page effect decode failed: {error}"))
        })?;
        validate_qwal_page_effect(base, target, &effect.pages, MAX_EXTERNAL_EFFECT_BYTES)?;
        let canonical = postcard::to_allocvec(&effect).map_err(|error| {
            Error::InvalidEntry(format!("QWAL page effect re-encode failed: {error}"))
        })?;
        if canonical.as_slice() != bytes {
            return invalid("QWAL page effect is not canonically encoded");
        }
        Ok(effect)
    }
}

#[allow(dead_code)] // Wired into the Recorder command boundary in the next integration step.
pub fn qwal_effect_receipt_digest(bytes: &[u8]) -> LogHash {
    LogHash::digest(&[QWAL_EFFECT_RECEIPT_DOMAIN, bytes])
}

pub fn encode_qwal_v3(effect: &QwalEnvelopeV3) -> Result<Vec<u8>> {
    effect.validate()?;
    let body = postcard::to_allocvec(effect)
        .map_err(|error| Error::InvalidEntry(format!("QWAL encode failed: {error}")))?;
    let encoded_len = QWAL_V3_MAGIC
        .len()
        .checked_add(body.len())
        .ok_or_else(|| Error::ResourceExhausted("QWAL encoded length overflows".into()))?;
    if encoded_len > MAX_QWAL_V3_BYTES {
        return Err(Error::ResourceExhausted(format!(
            "QWAL envelope exceeds {MAX_QWAL_V3_BYTES} bytes"
        )));
    }
    let mut encoded = Vec::with_capacity(encoded_len);
    encoded.extend_from_slice(QWAL_V3_MAGIC);
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

pub fn decode_qwal_v3(bytes: &[u8]) -> Result<QwalEnvelopeV3> {
    if bytes.len() > MAX_QWAL_V3_BYTES {
        return Err(Error::ResourceExhausted(format!(
            "QWAL envelope exceeds {MAX_QWAL_V3_BYTES} bytes"
        )));
    }
    let Some(body) = bytes.strip_prefix(QWAL_V3_MAGIC) else {
        return invalid("invalid QWAL v3 magic");
    };
    if body.is_empty() {
        return invalid("empty QWAL v3 body");
    }
    let effect: QwalEnvelopeV3 = postcard::from_bytes(body)
        .map_err(|error| Error::InvalidEntry(format!("QWAL decode failed: {error}")))?;
    effect.validate()?;

    // Reject alternate integer encodings and any trailing bytes. This also
    // makes the qlog payload a unique byte representation of the envelope.
    let canonical = postcard::to_allocvec(&effect)
        .map_err(|error| Error::InvalidEntry(format!("QWAL re-encode failed: {error}")))?;
    if canonical.as_slice() != body {
        return invalid("QWAL body is not canonically encoded");
    }
    Ok(effect)
}

/// Test oracle for byte-for-byte database equality.
#[cfg(test)]
fn file_digest(path: impl AsRef<Path>) -> Result<LogHash> {
    let mut file = File::open(path.as_ref()).map_err(io_error)?;
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

/// Reads and validates the page size in a closed SQLite database header.
pub fn sqlite_page_size(path: impl AsRef<Path>) -> Result<u32> {
    let mut file = File::open(path.as_ref()).map_err(io_error)?;
    let mut header = [0u8; SQLITE_HEADER_BYTES];
    file.read_exact(&mut header).map_err(io_error)?;
    let file_bytes = file.metadata().map_err(io_error)?.len();
    sqlite_page_size_from_header(&header, file_bytes)
}

/// Test-only byte diff oracle for closed SQLite databases.
#[cfg(test)]
fn diff_closed_databases(
    base_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
) -> Result<Vec<QwalPageV3>> {
    let base_path = base_path.as_ref();
    let target_path = target_path.as_ref();
    let base_page_size = sqlite_page_size(base_path)?;
    let target_page_size = sqlite_page_size(target_path)?;
    if base_page_size != target_page_size {
        return invalid("SQLite base and target page sizes differ");
    }

    let page_size = base_page_size as usize;
    let mut base = File::open(base_path).map_err(io_error)?;
    let mut target = File::open(target_path).map_err(io_error)?;
    let target_bytes = target.metadata().map_err(io_error)?.len();
    let target_pages = target_bytes / u64::from(base_page_size);
    let mut base_page = vec![0; page_size];
    let mut target_page = vec![0; page_size];
    let mut pages = Vec::new();
    let mut changed_bytes = 0usize;

    for page_index in 0..target_pages {
        target.read_exact(&mut target_page).map_err(io_error)?;
        let base_has_page = read_page_or_eof(&mut base, &mut base_page)?;
        if !base_has_page || base_page != target_page {
            changed_bytes = changed_bytes
                .checked_add(page_size)
                .ok_or_else(|| Error::ResourceExhausted("QWAL diff size overflows".into()))?;
            if changed_bytes > MAX_QWAL_V3_BYTES {
                return Err(Error::ResourceExhausted(format!(
                    "QWAL changed pages exceed {MAX_QWAL_V3_BYTES} bytes"
                )));
            }
            pages.push(QwalPageV3 {
                page_no: u32::try_from(page_index + 1)
                    .map_err(|_| Error::ResourceExhausted("QWAL page count exceeds u32".into()))?,
                after_image: target_page.clone(),
            });
        }
    }
    Ok(pages)
}

/// Applies a structurally valid, preverified transition to the held canonical
/// database inode. The caller must keep SQLite connections and lifecycle gates
/// closed until this returns and the page-state binding is refreshed.
pub(crate) fn apply_preverified_qwal_in_place(
    file: &mut File,
    effect: &QwalEnvelopeV3,
    after_page_write: impl FnMut(u32) -> Result<()>,
) -> Result<()> {
    effect.validate()?;
    apply_preverified_qwal_page_effect_in_place(
        file,
        &effect.base_state,
        &effect.target_state,
        &effect.pages,
        MAX_QWAL_V3_BYTES,
        after_page_write,
    )
}

/// Applies one intrinsically bound verified external effect. No caller-supplied
/// manifest can reinterpret the base, target, or digest context.
#[allow(dead_code)] // Wired into the Recorder command boundary in the next integration step.
pub(crate) fn apply_verified_qwal_effect_in_place(
    file: &mut File,
    effect: &VerifiedQwalEffectBundleV4,
    after_page_write: impl FnMut(u32) -> Result<()>,
) -> Result<()> {
    effect.manifest.validate()?;
    apply_preverified_qwal_page_effect_in_place(
        file,
        &effect.manifest.base_state,
        &effect.manifest.target_state,
        &effect.page_effect.pages,
        MAX_EXTERNAL_EFFECT_BYTES,
        after_page_write,
    )
}

fn apply_preverified_qwal_page_effect_in_place(
    file: &mut File,
    base_state: &StateIdentityV3,
    target_state: &StateIdentityV3,
    pages: &[QwalPageV3],
    max_page_bytes: usize,
    mut after_page_write: impl FnMut(u32) -> Result<()>,
) -> Result<()> {
    validate_qwal_page_effect(base_state, target_state, pages, max_page_bytes)?;
    let base_bytes = file.metadata().map_err(io_error)?.len();
    if base_bytes != state_file_bytes(base_state, "base")? {
        return invalid("QWAL base file size mismatch");
    }
    if sqlite_page_size_from_file(file)? != base_state.page_size {
        return invalid("QWAL base page size mismatch");
    }

    file.set_len(state_file_bytes(target_state, "target")?)
        .map_err(io_error)?;
    let page_size = u64::from(target_state.page_size);
    for page in pages {
        let offset = u64::from(page.page_no - 1)
            .checked_mul(page_size)
            .ok_or_else(|| Error::InvalidEntry("QWAL page offset overflows".into()))?;
        file.seek(SeekFrom::Start(offset)).map_err(io_error)?;
        file.write_all(&page.after_image).map_err(io_error)?;
        after_page_write(page.page_no)?;
    }

    verify_installed_qwal_pages(file, target_state, pages)
}

#[cfg(test)]
fn verify_state_transition(base_state: &PageStateCacheV3, effect: &QwalEnvelopeV3) -> Result<()> {
    if base_state.identity() != effect.base_state {
        return invalid("QWAL base page state mismatch");
    }
    let patches: Vec<_> = effect
        .pages
        .iter()
        .map(|page| PageStatePatchV3::new(page.page_no, &page.after_image))
        .collect();
    let derived = base_state.overlay(effect.target_state.page_count, &patches)?;
    if derived != effect.target_state {
        return invalid("QWAL target page state mismatch");
    }
    Ok(())
}

fn verify_installed_qwal_pages(
    file: &mut File,
    target_state: &StateIdentityV3,
    pages: &[QwalPageV3],
) -> Result<()> {
    if file.metadata().map_err(io_error)?.len() != state_file_bytes(target_state, "target")? {
        return invalid("QWAL target file size mismatch");
    }
    if sqlite_page_size_from_file(file)? != target_state.page_size {
        return invalid("QWAL target page size mismatch");
    }

    let page_size = u64::from(target_state.page_size);
    let mut installed = vec![0; target_state.page_size as usize];
    for page in pages {
        let offset = u64::from(page.page_no - 1)
            .checked_mul(page_size)
            .ok_or_else(|| Error::InvalidEntry("QWAL page offset overflows".into()))?;
        file.seek(SeekFrom::Start(offset)).map_err(io_error)?;
        file.read_exact(&mut installed).map_err(io_error)?;
        if installed != page.after_image {
            return invalid("QWAL installed page does not match its after-image");
        }
    }
    Ok(())
}

#[cfg(test)]
fn read_page_or_eof(file: &mut File, page: &mut [u8]) -> Result<bool> {
    let mut read = 0;
    while read < page.len() {
        let count = file.read(&mut page[read..]).map_err(io_error)?;
        if count == 0 {
            if read == 0 {
                return Ok(false);
            }
            return invalid("SQLite base file ends in a partial page");
        }
        read += count;
    }
    Ok(true)
}

fn page_size_from_header(header: &[u8]) -> Result<u32> {
    if header.len() < SQLITE_HEADER_BYTES || &header[..SQLITE_MAGIC.len()] != SQLITE_MAGIC {
        return invalid("invalid SQLite database header");
    }
    let encoded = u16::from_be_bytes([header[16], header[17]]);
    let page_size = if encoded == 1 {
        MAX_SQLITE_PAGE_SIZE
    } else {
        u32::from(encoded)
    };
    validate_page_size(page_size)?;
    Ok(page_size)
}

pub(crate) fn sqlite_page_size_from_header(header: &[u8], file_bytes: u64) -> Result<u32> {
    let page_size = page_size_from_header(header)?;
    if file_bytes == 0 || !file_bytes.is_multiple_of(u64::from(page_size)) {
        return invalid("SQLite file size is not a non-zero page-size multiple");
    }
    validate_header_page_count(header, file_bytes, u64::from(page_size))?;
    Ok(page_size)
}

fn sqlite_page_size_from_file(file: &mut File) -> Result<u32> {
    let file_bytes = file.metadata().map_err(io_error)?.len();
    let mut header = [0u8; SQLITE_HEADER_BYTES];
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    file.read_exact(&mut header).map_err(io_error)?;
    sqlite_page_size_from_header(&header, file_bytes)
}

fn validate_page_size(page_size: u32) -> Result<()> {
    if !(MIN_SQLITE_PAGE_SIZE..=MAX_SQLITE_PAGE_SIZE).contains(&page_size)
        || !page_size.is_power_of_two()
    {
        return invalid("invalid SQLite page size");
    }
    Ok(())
}

fn state_file_bytes(state: &StateIdentityV3, label: &str) -> Result<u64> {
    validate_page_size(state.page_size)?;
    if state.page_count == 0 {
        return invalid(format!("QWAL {label} page count must be positive"));
    }
    if state.state_root == LogHash::ZERO {
        return invalid(format!("QWAL {label} state root must be non-zero"));
    }
    u64::from(state.page_size)
        .checked_mul(u64::from(state.page_count))
        .ok_or_else(|| Error::ResourceExhausted(format!("QWAL {label} file size overflows")))
}

fn validate_qwal_page_effect(
    base_state: &StateIdentityV3,
    target_state: &StateIdentityV3,
    pages: &[QwalPageV3],
    max_page_bytes: usize,
) -> Result<()> {
    let base_file_bytes = state_file_bytes(base_state, "base")?;
    let target_file_bytes = state_file_bytes(target_state, "target")?;
    if base_state.page_size != target_state.page_size {
        return invalid("QWAL base and target page sizes differ");
    }
    let page_size = u64::from(target_state.page_size);
    let mut previous = 0;
    let mut page_bytes = 0usize;
    for page in pages {
        if page.page_no == 0 {
            return invalid("QWAL page number must be one-based");
        }
        if page.page_no <= previous {
            return invalid("QWAL pages must be strictly ordered without duplicates");
        }
        if page.after_image.len() != target_state.page_size as usize {
            return invalid("QWAL after-image length does not match page size");
        }
        let page_end = u64::from(page.page_no)
            .checked_mul(page_size)
            .ok_or_else(|| Error::InvalidEntry("QWAL page offset overflows".into()))?;
        if page_end > target_file_bytes {
            return invalid("QWAL page lies outside the target file");
        }
        if page.page_no == 1 {
            validate_sqlite_header(&page.after_image, target_state.page_size)?;
            validate_header_page_count(&page.after_image, target_file_bytes, page_size)?;
        }
        page_bytes = page_bytes
            .checked_add(page.after_image.len())
            .ok_or_else(|| Error::ResourceExhausted("QWAL page bytes overflow".into()))?;
        if page_bytes > max_page_bytes {
            return Err(Error::ResourceExhausted(format!(
                "QWAL page images exceed {max_page_bytes} bytes"
            )));
        }
        previous = page.page_no;
    }

    // Growing via set_len alone would create an attacker-sized sparse file.
    // Every new suffix page must be present in the typed effect.
    let base_pages = base_state.page_count;
    let target_pages = target_state.page_count;
    debug_assert_eq!(u64::from(base_pages) * page_size, base_file_bytes);
    debug_assert_eq!(u64::from(target_pages) * page_size, target_file_bytes);
    if target_pages > base_pages {
        let first_new = pages.partition_point(|page| page.page_no <= base_pages);
        let supplied_new_pages = u32::try_from(pages.len() - first_new)
            .map_err(|_| Error::ResourceExhausted("QWAL page count overflows".into()))?;
        let required_new_pages = target_pages - base_pages;
        if supplied_new_pages != required_new_pages {
            return invalid("QWAL growth must include every newly allocated page");
        }
    }
    Ok(())
}

fn validate_sqlite_header(page: &[u8], expected_page_size: u32) -> Result<()> {
    let actual = page_size_from_header(page)?;
    if actual != expected_page_size {
        return invalid("QWAL page 1 changes the declared SQLite page size");
    }
    Ok(())
}

fn validate_header_page_count(header: &[u8], file_bytes: u64, page_size: u64) -> Result<()> {
    let declared = u32::from_be_bytes([header[28], header[29], header[30], header[31]]);
    // A zero page count is permitted by legacy SQLite files. When present, the
    // count must describe the closed file exactly.
    if declared != 0 && u64::from(declared) != file_bytes / page_size {
        return invalid("SQLite header page count does not match file size");
    }
    Ok(())
}

fn validate_nonempty_bounded(field: &str, value: &str, max: usize) -> Result<()> {
    if value.is_empty() || value.len() > max {
        return invalid(format!("QWAL {field} must contain 1..={max} bytes"));
    }
    Ok(())
}

fn io_error(error: std::io::Error) -> Error {
    Error::Io(error.to_string())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::InvalidEntry(message.into()))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rusqlite::{params, Connection};

    use super::*;

    #[test]
    fn generation_capacity_is_1024_receipts_and_512_kib() {
        assert_eq!(MAX_QWAL_V3_RECEIPTS, 1024);
        assert_eq!(MAX_QWAL_V3_BYTES, 512 * 1024);
    }

    fn sqlite_header(page_size: u32, page_count: u32) -> Vec<u8> {
        let mut page = vec![0; page_size as usize];
        page[..SQLITE_MAGIC.len()].copy_from_slice(SQLITE_MAGIC);
        let encoded = if page_size == MAX_SQLITE_PAGE_SIZE {
            1u16
        } else {
            page_size as u16
        };
        page[16..18].copy_from_slice(&encoded.to_be_bytes());
        page[18] = 1;
        page[19] = 1;
        page[20] = 0;
        page[21] = 64;
        page[22] = 32;
        page[23] = 32;
        page[28..32].copy_from_slice(&page_count.to_be_bytes());
        page
    }

    fn write_pages(path: &Path, page_size: u32, fills: &[u8]) {
        let mut bytes = Vec::with_capacity(page_size as usize * fills.len());
        for (index, fill) in fills.iter().enumerate() {
            let mut page = vec![*fill; page_size as usize];
            if index == 0 {
                page = sqlite_header(page_size, fills.len() as u32);
                page[100..].fill(*fill);
            }
            bytes.extend_from_slice(&page);
        }
        fs::write(path, bytes).unwrap();
    }

    fn page_state(path: &Path) -> PageStateCacheV3 {
        let page_size = sqlite_page_size(path).unwrap();
        let bytes = fs::read(path).unwrap();
        PageStateCacheV3::from_pages(page_size, bytes.chunks_exact(page_size as usize)).unwrap()
    }

    fn envelope(base: &Path, target: &Path, pages: Vec<QwalPageV3>) -> QwalEnvelopeV3 {
        QwalEnvelopeV3 {
            cluster_id: "cluster-a".into(),
            epoch: 3,
            configuration_id: 4,
            recovery_generation: 5,
            base_index: 8,
            base_hash: LogHash::digest(&[b"base-anchor"]),
            base_state: page_state(base).identity(),
            target_state: page_state(target).identity(),
            materializer_fingerprint: "sqlite-test-qwal-v3".into(),
            receipts: vec![QwalReceiptV3 {
                request_id: "request-a".into(),
                request_digest: LogHash::digest(&[b"request"]),
                result_blob: crate::encode_sql_result(&crate::SqlCommandResult {
                    statement_results: Vec::new(),
                })
                .unwrap(),
            }],
            pages,
        }
    }

    fn diff_pages(base: &Path, target: &Path) -> Vec<QwalPageV3> {
        diff_closed_databases(base, target).unwrap()
    }

    fn apply(base: &Path, temp: &Path, effect: &QwalEnvelopeV3) -> Result<()> {
        let mut cache = page_state(base);
        let patches = effect
            .pages
            .iter()
            .map(|page| PageStatePatchV3::new(page.page_no, &page.after_image))
            .collect::<Vec<_>>();
        if cache.apply_patch(effect.target_state.page_count, &patches)? != effect.target_state {
            return invalid("QWAL target page state mismatch");
        }
        fs::copy(base, temp).map_err(io_error)?;
        let mut file = File::options()
            .read(true)
            .write(true)
            .open(temp)
            .map_err(io_error)?;
        apply_preverified_qwal_in_place(&mut file, effect, |_| Ok(()))
    }

    #[test]
    fn qwal_roundtrips_through_its_only_canonical_encoding() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        write_pages(&base, 512, &[1, 2]);
        write_pages(&target, 512, &[1, 3]);
        let effect = envelope(&base, &target, diff_pages(&base, &target));

        let encoded = effect.encode().unwrap();
        assert!(encoded.starts_with(QWAL_V3_MAGIC));
        assert_eq!(QwalEnvelopeV3::decode(&encoded).unwrap(), effect);
    }

    #[test]
    fn decoder_rejects_legacy_trailing_and_corrupted_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        write_pages(&base, 512, &[1]);
        write_pages(&target, 512, &[2]);
        let effect = envelope(&base, &target, diff_pages(&base, &target));
        let mut trailing = effect.encode().unwrap();
        trailing.push(0);
        assert!(QwalEnvelopeV3::decode(&trailing).is_err());

        let mut corrupt = effect.encode().unwrap();
        corrupt[0] ^= 0xff;
        assert!(QwalEnvelopeV3::decode(&corrupt).is_err());

        for old_magic in [b"QWAL\0\x01", b"QWAL\0\x02", b"QWAL\0\x03"] {
            let mut old = old_magic.to_vec();
            old.extend_from_slice(&effect.encode().unwrap()[QWAL_V3_MAGIC.len()..]);
            assert!(QwalEnvelopeV3::decode(&old).is_err());
        }
    }

    #[test]
    fn receipts_preserve_order_and_reject_empty_duplicate_or_oversized_batches() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        write_pages(&base, 512, &[1]);
        let mut effect = envelope(&base, &base, Vec::new());
        effect.receipts = (0..MAX_QWAL_V3_RECEIPTS)
            .map(|index| QwalReceiptV3 {
                request_id: format!("request-{index:03}"),
                request_digest: LogHash::digest(&[&index.to_le_bytes()]),
                result_blob: crate::encode_sql_result(&crate::SqlCommandResult {
                    statement_results: Vec::new(),
                })
                .unwrap(),
            })
            .collect();
        let decoded = QwalEnvelopeV3::decode(&effect.encode().unwrap()).unwrap();
        assert_eq!(decoded.receipts, effect.receipts);

        effect.receipts[MAX_QWAL_V3_RECEIPTS - 1].request_id =
            effect.receipts[0].request_id.clone();
        assert!(matches!(
            effect.validate(),
            Err(Error::InvalidEntry(message)) if message.contains("unique")
        ));

        effect.receipts.clear();
        assert!(effect.validate().is_err());

        effect.receipts = vec![
            QwalReceiptV3 {
                request_id: "duplicate".into(),
                request_digest: LogHash::ZERO,
                result_blob: crate::encode_sql_result(&crate::SqlCommandResult {
                    statement_results: Vec::new(),
                })
                .unwrap(),
            },
            QwalReceiptV3 {
                request_id: "duplicate".into(),
                request_digest: LogHash::digest(&[b"different"]),
                result_blob: crate::encode_sql_result(&crate::SqlCommandResult {
                    statement_results: Vec::new(),
                })
                .unwrap(),
            },
        ];
        assert!(effect.validate().is_err());

        effect.receipts = (0..=MAX_QWAL_V3_RECEIPTS)
            .map(|index| QwalReceiptV3 {
                request_id: format!("request-{index}"),
                request_digest: LogHash::ZERO,
                result_blob: crate::encode_sql_result(&crate::SqlCommandResult {
                    statement_results: Vec::new(),
                })
                .unwrap(),
            })
            .collect();
        assert!(effect.validate().is_err());
    }

    #[test]
    fn validation_rejects_zero_duplicate_unordered_short_and_out_of_range_pages() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        write_pages(&base, 512, &[1, 2]);
        write_pages(&target, 512, &[2, 3]);
        let page = diff_pages(&base, &target)[0].clone();

        for pages in [
            vec![QwalPageV3 {
                page_no: 0,
                after_image: vec![0; 512],
            }],
            vec![page.clone(), page.clone()],
            vec![
                QwalPageV3 {
                    page_no: 2,
                    after_image: vec![0; 512],
                },
                page.clone(),
            ],
            vec![QwalPageV3 {
                page_no: 1,
                after_image: vec![0; 511],
            }],
            vec![QwalPageV3 {
                page_no: 3,
                after_image: vec![0; 512],
            }],
        ] {
            assert!(envelope(&base, &target, pages).validate().is_err());
        }
    }

    #[test]
    fn page_state_root_is_bound_to_base_contents_and_target_shape() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let wrong = dir.path().join("wrong.db");
        let target = dir.path().join("target.db");
        write_pages(&base, 512, &[1, 2]);
        write_pages(&wrong, 512, &[9, 2]);
        write_pages(&target, 512, &[1, 3]);
        let effect = envelope(&base, &target, diff_pages(&base, &target));

        assert!(verify_state_transition(&page_state(&wrong), &effect).is_err());

        let mut reshaped = effect.clone();
        reshaped.target_state.page_count += 1;
        reshaped.pages.push(QwalPageV3 {
            page_no: 3,
            after_image: vec![7; 512],
        });
        reshaped.validate().unwrap();
        assert!(verify_state_transition(&page_state(&base), &reshaped).is_err());

        let mut wrong_page_size = effect;
        wrong_page_size.base_state.page_size = 1024;
        wrong_page_size.target_state.page_size = 1024;
        assert!(wrong_page_size.validate().is_err());
    }

    #[test]
    fn validation_rejects_zero_roots_counts_and_mixed_page_sizes() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        write_pages(&base, 512, &[1]);
        let effect = envelope(&base, &base, Vec::new());

        let mut zero_root = effect.clone();
        zero_root.base_state.state_root = LogHash::ZERO;
        assert!(zero_root.validate().is_err());

        let mut zero_count = effect.clone();
        zero_count.target_state.page_count = 0;
        assert!(zero_count.validate().is_err());

        let mut mixed_sizes = effect;
        mixed_sizes.target_state.page_size = 1024;
        assert!(mixed_sizes.validate().is_err());
    }

    #[test]
    fn page_one_must_remain_a_valid_sqlite_header() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        write_pages(&base, 512, &[1]);
        write_pages(&target, 512, &[2]);
        let mut effect = envelope(&base, &target, diff_pages(&base, &target));
        effect.pages[0].after_image[0] = b'X';
        assert!(effect.validate().is_err());
    }

    #[test]
    fn closed_file_diff_and_apply_support_growth_and_shrink() {
        for (base_fills, target_fills) in [
            (vec![1], vec![2, 3, 4]),
            (vec![1, 2, 3], vec![4]),
            (vec![1, 2, 3], vec![1, 8, 3]),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let base = dir.path().join("base.db");
            let target = dir.path().join("target.db");
            let applied = dir.path().join("applied.db");
            write_pages(&base, 512, &base_fills);
            write_pages(&target, 512, &target_fills);
            let pages = diff_pages(&base, &target);
            let effect = envelope(&base, &target, pages);

            apply(&base, &applied, &effect).unwrap();
            assert_eq!(fs::read(&applied).unwrap(), fs::read(&target).unwrap());
        }
    }

    #[test]
    fn validation_rejects_sparse_growth_and_missing_new_pages_before_file_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        write_pages(&base, 512, &[1]);
        write_pages(&target, 512, &[2, 3, 4]);

        let mut gap = envelope(&base, &target, diff_pages(&base, &target));
        gap.pages.retain(|page| page.page_no != 2);
        assert!(gap.validate().is_err());

        let mut sparse = envelope(&base, &base, Vec::new());
        sparse.target_state.page_count = u32::MAX;
        let absent = dir.path().join("must-not-be-created.db");
        assert!(sparse.validate().is_err());
        assert!(apply(&base, &absent, &sparse).is_err());
        assert!(!absent.exists());
    }

    #[test]
    fn shrink_needs_no_images_for_pages_removed_by_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        let applied = dir.path().join("applied.db");
        write_pages(&base, 512, &[1, 2, 3]);
        write_pages(&target, 512, &[1]);
        let effect = envelope(&base, &target, diff_pages(&base, &target));
        assert!(effect.pages.iter().all(|page| page.page_no == 1));
        effect.validate().unwrap();
        apply(&base, &applied, &effect).unwrap();
        assert_eq!(fs::read(applied).unwrap(), fs::read(target).unwrap());
    }

    #[test]
    fn page_size_parser_accepts_every_sqlite_page_size_and_rejects_invalid_values() {
        let dir = tempfile::tempdir().unwrap();
        for exponent in 9..=16 {
            let size = 1u32 << exponent;
            let path = dir.path().join(format!("{size}.db"));
            write_pages(&path, size, &[1]);
            assert_eq!(sqlite_page_size(path).unwrap(), size);
        }
        let invalid = dir.path().join("invalid.db");
        let mut page = sqlite_header(512, 1);
        page[16..18].copy_from_slice(&768u16.to_be_bytes());
        fs::write(&invalid, page).unwrap();
        assert!(sqlite_page_size(invalid).is_err());
    }

    #[test]
    fn forged_target_root_is_rejected_before_any_write() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        let applied = dir.path().join("applied.db");
        write_pages(&base, 512, &[1]);
        write_pages(&target, 512, &[2]);
        let mut effect = envelope(&base, &target, diff_pages(&base, &target));
        effect.target_state.state_root = LogHash::digest(&[b"forged-target-root"]);

        assert!(apply(&base, &applied, &effect).is_err());
        assert!(!applied.exists());
    }

    #[test]
    fn captured_sqlite_effect_reproduces_native_features_byte_for_byte() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.db");
        let target = dir.path().join("target.db");
        let applied = dir.path().join("applied.db");
        {
            let connection = Connection::open(&base).unwrap();
            connection
                .execute_batch(
                    "PRAGMA page_size=4096;
                     PRAGMA journal_mode=DELETE;
                     PRAGMA foreign_keys=ON;
                     CREATE TABLE parent(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
                     CREATE TABLE child(
                       id INTEGER PRIMARY KEY,
                       parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE,
                       payload BLOB,
                       created_at TEXT DEFAULT CURRENT_TIMESTAMP
                     );
                     CREATE TABLE audit(message TEXT);
                     CREATE TRIGGER child_audit AFTER INSERT ON child BEGIN
                       INSERT INTO audit VALUES ('child:' || NEW.id);
                     END;",
                )
                .unwrap();
        }
        fs::copy(&base, &target).unwrap();
        {
            let mut connection = Connection::open(&target).unwrap();
            connection.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
            let transaction = connection.transaction().unwrap();
            let parent_id: i64 = transaction
                .query_row(
                    "INSERT INTO parent(name) VALUES ('native') RETURNING id",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let returned: (i64, Vec<u8>) = transaction
                .query_row(
                    "INSERT INTO child(id, parent_id, payload)
                     VALUES (7, ?1, randomblob(32)) RETURNING id, payload",
                    params![parent_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(returned.0, 7);
            assert_eq!(returned.1.len(), 32);
            transaction.commit().unwrap();
        }

        let pages = diff_pages(&base, &target);
        assert!(!pages.is_empty());
        let effect = envelope(&base, &target, pages);
        apply(&base, &applied, &effect).unwrap();

        assert_eq!(
            file_digest(&applied).unwrap(),
            file_digest(&target).unwrap()
        );
        assert_eq!(fs::read(&applied).unwrap(), fs::read(&target).unwrap());
        let inspection =
            Connection::open_with_flags(&applied, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        assert_eq!(
            inspection
                .query_row("SELECT message FROM audit", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "child:7"
        );
        assert_eq!(
            inspection
                .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
    }
}
