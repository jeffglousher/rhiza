use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, BufReader, Read, Seek, SeekFrom},
};

use crate::{Error, Result};

const WAL_HEADER_BYTES: u64 = 32;
const WAL_FRAME_HEADER_BYTES: u64 = 24;
const WAL_VERSION: u32 = 3_007_000;
const WAL_MAGIC_LITTLE_CHECKSUM: u32 = 0x377f_0682;
const WAL_MAGIC_BIG_CHECKSUM: u32 = 0x377f_0683;
const MIN_PAGE_SIZE: u32 = 512;
const MAX_PAGE_SIZE: u32 = 65_536;
const MAX_PAGE_NO: u32 = u32::MAX - 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WalCapture {
    NoChange,
    Committed(WalCommit),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalCommit {
    pub(crate) page_size: u32,
    pub(crate) target_db_pages: u32,
    pub(crate) target_file_bytes: u64,
    pub(crate) pages: Vec<WalPage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalPage {
    pub(crate) page_no: u32,
    pub(crate) after_image: Vec<u8>,
}

/// Parses a fresh SQLite WAL through the descriptor acquired while the staging
/// connection is still alive. The caller must prevent checkpoint-on-close and
/// seal the descriptor identity before using the returned page images.
pub(crate) fn capture_wal(
    wal: &mut File,
    base_db_pages: u32,
    max_changed_bytes: usize,
    max_raw_bytes: u64,
) -> Result<WalCapture> {
    if !(1..=MAX_PAGE_NO).contains(&base_db_pages) {
        return invalid("SQLite base database page count is out of range");
    }

    let metadata = wal.metadata().map_err(io_error)?;
    if !metadata.is_file() {
        return invalid("SQLite WAL path is not a regular file");
    }
    let file_bytes = metadata.len();
    if file_bytes == 0 {
        return Ok(WalCapture::NoChange);
    }
    if file_bytes > max_raw_bytes {
        return Err(Error::ResourceExhausted(format!(
            "SQLite WAL raw capture exceeds {max_raw_bytes} bytes"
        )));
    }
    if file_bytes < WAL_HEADER_BYTES {
        return invalid("SQLite WAL header is truncated");
    }

    wal.seek(SeekFrom::Start(0)).map_err(io_error)?;
    let mut reader = BufReader::new(wal);
    let mut header = [0u8; WAL_HEADER_BYTES as usize];
    read_exact(&mut reader, &mut header, "SQLite WAL header is truncated")?;

    let checksum_order = match be_u32(&header[0..4]) {
        WAL_MAGIC_LITTLE_CHECKSUM => ChecksumOrder::Little,
        WAL_MAGIC_BIG_CHECKSUM => ChecksumOrder::Big,
        _ => return invalid("SQLite WAL magic is invalid"),
    };
    if be_u32(&header[4..8]) != WAL_VERSION {
        return invalid("SQLite WAL format version is unsupported");
    }
    let page_size = be_u32(&header[8..12]);
    if !(MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&page_size) || !page_size.is_power_of_two() {
        return invalid("SQLite WAL page size is invalid");
    }

    let expected_header_checksum = checksum(checksum_order, &header[..24], [0, 0]);
    if expected_header_checksum != [be_u32(&header[24..28]), be_u32(&header[28..32])] {
        return invalid("SQLite WAL header checksum mismatch");
    }

    let frame_bytes = WAL_FRAME_HEADER_BYTES
        .checked_add(u64::from(page_size))
        .ok_or_else(|| Error::InvalidEntry("SQLite WAL frame size overflows".into()))?;
    let payload_bytes = file_bytes - WAL_HEADER_BYTES;
    if payload_bytes == 0 || !payload_bytes.is_multiple_of(frame_bytes) {
        return invalid("SQLite WAL does not contain exact complete frames");
    }
    let frame_count = payload_bytes / frame_bytes;
    let salts = [be_u32(&header[16..20]), be_u32(&header[20..24])];
    let mut rolling_checksum = expected_header_checksum;
    let mut pages = BTreeMap::<u32, Vec<u8>>::new();
    let mut final_db_pages = 0u32;
    let mut last_frame_committed = false;

    for _ in 0..frame_count {
        if last_frame_committed {
            return invalid("SQLite WAL contains a frame after its single commit marker");
        }
        let mut frame_header = [0u8; WAL_FRAME_HEADER_BYTES as usize];
        read_exact(
            &mut reader,
            &mut frame_header,
            "SQLite WAL frame header is truncated",
        )?;
        let page_no = be_u32(&frame_header[0..4]);
        if !(1..=MAX_PAGE_NO).contains(&page_no) {
            return invalid("SQLite WAL frame page number is out of range");
        }
        let commit_pages = be_u32(&frame_header[4..8]);
        if commit_pages > MAX_PAGE_NO {
            return invalid("SQLite WAL commit database size is out of range");
        }
        if [be_u32(&frame_header[8..12]), be_u32(&frame_header[12..16])] != salts {
            return invalid("SQLite WAL frame salt mismatch");
        }

        let mut after_image = vec![0u8; page_size as usize];
        read_exact(
            &mut reader,
            &mut after_image,
            "SQLite WAL frame page is truncated",
        )?;
        rolling_checksum = checksum(checksum_order, &frame_header[..8], rolling_checksum);
        rolling_checksum = checksum(checksum_order, &after_image, rolling_checksum);
        if rolling_checksum != [be_u32(&frame_header[16..20]), be_u32(&frame_header[20..24])] {
            return invalid("SQLite WAL frame checksum mismatch");
        }

        if !pages.contains_key(&page_no) {
            let retained_bytes = pages
                .len()
                .checked_add(1)
                .and_then(|count| count.checked_mul(page_size as usize))
                .ok_or_else(|| {
                    Error::ResourceExhausted("SQLite WAL changed bytes overflow".into())
                })?;
            if retained_bytes > max_changed_bytes {
                return Err(Error::ResourceExhausted(format!(
                    "SQLite WAL changed page images exceed {max_changed_bytes} bytes"
                )));
            }
        }
        pages.insert(page_no, after_image);
        last_frame_committed = commit_pages != 0;
        if last_frame_committed {
            final_db_pages = commit_pages;
        }
    }

    let mut trailing = [0u8; 1];
    if reader.read(&mut trailing).map_err(io_error)? != 0 {
        return invalid("SQLite WAL changed while it was being captured");
    }
    if !last_frame_committed || final_db_pages == 0 {
        return invalid("SQLite WAL ends without a commit marker");
    }

    pages.retain(|page_no, _| *page_no <= final_db_pages);
    if final_db_pages > base_db_pages {
        let required_growth_pages =
            usize::try_from(final_db_pages - base_db_pages).map_err(|_| {
                Error::ResourceExhausted("SQLite WAL growth page count overflows".into())
            })?;
        let supplied_growth_pages = pages.range((base_db_pages + 1)..).count();
        if supplied_growth_pages != required_growth_pages {
            return invalid("SQLite WAL growth is missing a newly allocated page");
        }
    }

    let target_file_bytes = u64::from(final_db_pages)
        .checked_mul(u64::from(page_size))
        .ok_or_else(|| Error::InvalidEntry("SQLite WAL target database size overflows".into()))?;
    Ok(WalCapture::Committed(WalCommit {
        page_size,
        target_db_pages: final_db_pages,
        target_file_bytes,
        pages: pages
            .into_iter()
            .map(|(page_no, after_image)| WalPage {
                page_no,
                after_image,
            })
            .collect(),
    }))
}

#[derive(Clone, Copy)]
enum ChecksumOrder {
    Big,
    Little,
}

fn checksum(order: ChecksumOrder, bytes: &[u8], mut state: [u32; 2]) -> [u32; 2] {
    debug_assert_eq!(bytes.len() % 8, 0);
    for chunk in bytes.chunks_exact(8) {
        let word = |bytes: [u8; 4]| match order {
            ChecksumOrder::Big => u32::from_be_bytes(bytes),
            ChecksumOrder::Little => u32::from_le_bytes(bytes),
        };
        let first = word(chunk[..4].try_into().expect("four-byte checksum word"));
        let second = word(chunk[4..].try_into().expect("four-byte checksum word"));
        state[0] = state[0].wrapping_add(first).wrapping_add(state[1]);
        state[1] = state[1].wrapping_add(second).wrapping_add(state[0]);
    }
    state
}

fn be_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().expect("four-byte WAL field"))
}

fn read_exact(reader: &mut impl Read, bytes: &mut [u8], truncated: &str) -> Result<()> {
    reader.read_exact(bytes).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            Error::InvalidEntry(truncated.into())
        } else {
            io_error(error)
        }
    })
}

fn io_error(error: io::Error) -> Error {
    Error::Io(error.to_string())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::InvalidEntry(message.into()))
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File, OpenOptions},
        io::{Seek, SeekFrom, Write},
    };

    use rusqlite::{config::DbConfig, Connection};
    use tempfile::TempDir;

    use super::{capture_wal, WalCapture};

    const MAGIC_BIG_CHECKSUM: u32 = 0x377f_0683;
    const MAGIC_LITTLE_CHECKSUM: u32 = 0x377f_0682;
    const WAL_VERSION: u32 = 3_007_000;
    const SALT: [u32; 2] = [0x1234_5678, 0x90ab_cdef];

    #[derive(Clone, Copy)]
    enum ChecksumOrder {
        Big,
        Little,
    }

    fn checksum(order: ChecksumOrder, bytes: &[u8], mut state: [u32; 2]) -> [u32; 2] {
        assert_eq!(bytes.len() % 8, 0);
        for chunk in bytes.chunks_exact(8) {
            let word = |bytes: [u8; 4]| match order {
                ChecksumOrder::Big => u32::from_be_bytes(bytes),
                ChecksumOrder::Little => u32::from_le_bytes(bytes),
            };
            let first = word(chunk[..4].try_into().unwrap());
            let second = word(chunk[4..].try_into().unwrap());
            state[0] = state[0].wrapping_add(first).wrapping_add(state[1]);
            state[1] = state[1].wrapping_add(second).wrapping_add(state[0]);
        }
        state
    }

    fn wal_header(page_size: u32, order: ChecksumOrder) -> Vec<u8> {
        let magic = match order {
            ChecksumOrder::Big => MAGIC_BIG_CHECKSUM,
            ChecksumOrder::Little => MAGIC_LITTLE_CHECKSUM,
        };
        let mut header = Vec::with_capacity(32);
        header.extend_from_slice(&magic.to_be_bytes());
        header.extend_from_slice(&WAL_VERSION.to_be_bytes());
        header.extend_from_slice(&page_size.to_be_bytes());
        header.extend_from_slice(&0u32.to_be_bytes());
        header.extend_from_slice(&SALT[0].to_be_bytes());
        header.extend_from_slice(&SALT[1].to_be_bytes());
        let sum = checksum(order, &header, [0, 0]);
        header.extend_from_slice(&sum[0].to_be_bytes());
        header.extend_from_slice(&sum[1].to_be_bytes());
        header
    }

    fn append_frame(
        wal: &mut Vec<u8>,
        page_size: u32,
        order: ChecksumOrder,
        state: &mut [u32; 2],
        page_no: u32,
        commit_pages: u32,
        fill: u8,
    ) {
        let mut header = Vec::with_capacity(24);
        header.extend_from_slice(&page_no.to_be_bytes());
        header.extend_from_slice(&commit_pages.to_be_bytes());
        header.extend_from_slice(&SALT[0].to_be_bytes());
        header.extend_from_slice(&SALT[1].to_be_bytes());
        let page = vec![fill; page_size as usize];
        *state = checksum(order, &header[..8], *state);
        *state = checksum(order, &page, *state);
        header.extend_from_slice(&state[0].to_be_bytes());
        header.extend_from_slice(&state[1].to_be_bytes());
        wal.extend_from_slice(&header);
        wal.extend_from_slice(&page);
    }

    fn synthetic_wal(page_size: u32, order: ChecksumOrder, frames: &[(u32, u32, u8)]) -> Vec<u8> {
        let mut wal = wal_header(page_size, order);
        let mut state = checksum(order, &wal[..24], [0, 0]);
        for &(page_no, commit_pages, fill) in frames {
            append_frame(
                &mut wal,
                page_size,
                order,
                &mut state,
                page_no,
                commit_pages,
                fill,
            );
        }
        wal
    }

    fn write_wal(dir: &TempDir, bytes: &[u8]) -> std::path::PathBuf {
        let path = dir.path().join("db.sqlite-wal");
        fs::write(&path, bytes).unwrap();
        path
    }

    fn capture_path(
        path: &std::path::Path,
        base_db_pages: u32,
        max_changed_bytes: usize,
    ) -> crate::Result<WalCapture> {
        let mut wal = File::open(path).unwrap();
        capture_wal(&mut wal, base_db_pages, max_changed_bytes, u64::MAX)
    }

    fn committed(capture: WalCapture) -> super::WalCommit {
        match capture {
            WalCapture::Committed(commit) => commit,
            WalCapture::NoChange => panic!("expected committed WAL"),
        }
    }

    #[test]
    fn capture_returns_explicit_no_change_when_wal_is_empty() {
        let dir = TempDir::new().unwrap();
        let empty = write_wal(&dir, &[]);
        assert_eq!(capture_path(&empty, 1, 4096).unwrap(), WalCapture::NoChange);
    }

    #[test]
    fn capture_keeps_sorted_last_committed_image_and_complete_growth_suffix() {
        let dir = TempDir::new().unwrap();
        let wal = synthetic_wal(512, ChecksumOrder::Big, &[(2, 0, 2), (2, 0, 22), (3, 3, 3)]);
        let path = write_wal(&dir, &wal);
        let capture = committed(capture_path(&path, 1, 1536).unwrap());

        assert_eq!(capture.page_size, 512);
        assert_eq!(capture.target_db_pages, 3);
        assert_eq!(capture.target_file_bytes, 1536);
        assert_eq!(
            capture
                .pages
                .iter()
                .map(|page| (page.page_no, page.after_image[0]))
                .collect::<Vec<_>>(),
            vec![(2, 22), (3, 3)]
        );
    }

    #[test]
    fn capture_drops_pages_removed_by_committed_shrink() {
        let dir = TempDir::new().unwrap();
        let wal = synthetic_wal(512, ChecksumOrder::Little, &[(3, 0, 3), (1, 1, 1)]);
        let path = write_wal(&dir, &wal);
        let capture = committed(capture_path(&path, 3, 1024).unwrap());

        assert_eq!(capture.target_db_pages, 1);
        assert_eq!(capture.pages.len(), 1);
        assert_eq!(capture.pages[0].page_no, 1);
    }

    #[test]
    fn capture_accepts_real_sqlite_wal_page_sizes() {
        for page_size in [512, 4096, 65_536] {
            let dir = TempDir::new().unwrap();
            let db = dir.path().join(format!("{page_size}.sqlite"));
            let connection = Connection::open(&db).unwrap();
            connection
                .execute_batch(&format!(
                    "PRAGMA page_size={page_size};\n\
                     VACUUM;\n\
                     PRAGMA journal_mode=WAL;\n\
                     PRAGMA wal_autocheckpoint=0;\n\
                     CREATE TABLE test(value BLOB);\n\
                     PRAGMA wal_checkpoint(TRUNCATE);"
                ))
                .unwrap();
            let base_pages = (fs::metadata(&db).unwrap().len() / u64::from(page_size)) as u32;
            connection
                .execute(
                    "INSERT INTO test VALUES (zeroblob(100))",
                    rusqlite::params![],
                )
                .unwrap();

            let wal = db.with_extension("sqlite-wal");
            let capture = committed(capture_path(&wal, base_pages, usize::MAX).unwrap());
            assert_eq!(capture.page_size, page_size);
            assert!(!capture.pages.is_empty());
            assert!(capture
                .pages
                .windows(2)
                .all(|pair| pair[0].page_no < pair[1].page_no));
            drop(connection);
        }
    }

    #[test]
    fn capture_rejects_truncated_header_and_frame() {
        let dir = TempDir::new().unwrap();
        let header = write_wal(&dir, &[0; 31]);
        assert!(capture_path(&header, 1, 4096).is_err());

        let mut wal = synthetic_wal(512, ChecksumOrder::Big, &[(1, 1, 1)]);
        wal.pop();
        let frame = write_wal(&dir, &wal);
        assert!(capture_path(&frame, 1, 4096).is_err());
    }

    #[test]
    fn capture_rejects_magic_version_page_size_header_checksum_and_salt_errors() {
        for offset in [0, 4, 8, 24, 40] {
            let dir = TempDir::new().unwrap();
            let mut wal = synthetic_wal(512, ChecksumOrder::Big, &[(1, 1, 1)]);
            wal[offset] ^= 1;
            let path = write_wal(&dir, &wal);
            assert!(capture_path(&path, 1, 4096).is_err(), "offset {offset}");
        }

        for page_size in [0, 1, 511, 513, 131_072] {
            let dir = TempDir::new().unwrap();
            let path = write_wal(&dir, &wal_header(page_size, ChecksumOrder::Big));
            assert!(
                capture_path(&path, 1, usize::MAX).is_err(),
                "page size {page_size}"
            );
        }
    }

    #[test]
    fn capture_rejects_frame_checksum_page_number_and_commit_errors() {
        let cases = [
            synthetic_wal(512, ChecksumOrder::Big, &[(0, 1, 1)]),
            synthetic_wal(512, ChecksumOrder::Big, &[(u32::MAX, 1, 1)]),
            synthetic_wal(512, ChecksumOrder::Big, &[(1, u32::MAX, 1)]),
            synthetic_wal(512, ChecksumOrder::Big, &[(1, 0, 1)]),
            synthetic_wal(512, ChecksumOrder::Big, &[(1, 1, 1), (1, 0, 2)]),
        ];
        for wal in cases {
            let dir = TempDir::new().unwrap();
            let path = write_wal(&dir, &wal);
            assert!(capture_path(&path, 1, usize::MAX).is_err());
        }

        let dir = TempDir::new().unwrap();
        let mut checksum = synthetic_wal(512, ChecksumOrder::Big, &[(1, 1, 1)]);
        checksum[48] ^= 1;
        let path = write_wal(&dir, &checksum);
        assert!(capture_path(&path, 1, usize::MAX).is_err());
    }

    #[test]
    fn capture_rejects_any_frame_after_the_single_commit_marker() {
        for frames in [[(1, 1, 1), (1, 0, 2)], [(1, 1, 1), (1, 1, 2)]] {
            let dir = TempDir::new().unwrap();
            let wal = synthetic_wal(512, ChecksumOrder::Big, &frames);
            let path = write_wal(&dir, &wal);
            assert!(capture_path(&path, 1, usize::MAX).is_err());
        }
    }

    #[test]
    fn capture_rejects_missing_growth_page_and_changed_byte_overflow() {
        let dir = TempDir::new().unwrap();
        let wal = synthetic_wal(512, ChecksumOrder::Big, &[(3, 3, 3)]);
        let path = write_wal(&dir, &wal);
        assert!(capture_path(&path, 1, usize::MAX).is_err());

        let dir = TempDir::new().unwrap();
        let wal = synthetic_wal(512, ChecksumOrder::Big, &[(1, 0, 1), (2, 2, 2)]);
        let path = write_wal(&dir, &wal);
        assert!(capture_path(&path, 1, 511).is_err());
    }

    #[test]
    fn capture_allows_repeated_frames_beyond_final_changed_byte_budget() {
        let dir = TempDir::new().unwrap();
        let wal = synthetic_wal(512, ChecksumOrder::Big, &[(1, 0, 1), (1, 0, 2), (1, 1, 3)]);
        let path = write_wal(&dir, &wal);

        let capture = committed(capture_path(&path, 1, 512).unwrap());
        assert_eq!(capture.pages.len(), 1);
        assert_eq!(capture.pages[0].after_image[0], 3);
    }

    #[test]
    fn capture_accepts_over_128_frames_overwriting_two_retained_pages() {
        let dir = TempDir::new().unwrap();
        let frames = (0..256)
            .map(|index| {
                let page_no = 1 + index % 2;
                let commit_pages = if index == 255 { 2 } else { 0 };
                (page_no, commit_pages, index as u8)
            })
            .collect::<Vec<_>>();
        let wal = synthetic_wal(4096, ChecksumOrder::Big, &frames);
        assert!(wal.len() as u64 > crate::MAX_SQL_EFFECT_BYTES as u64);
        let path = write_wal(&dir, &wal);
        let mut file = File::open(path).unwrap();

        let capture = committed(
            capture_wal(
                &mut file,
                2,
                crate::MAX_SQL_EFFECT_BYTES,
                crate::MAX_NATIVE_WAL_CAPTURE_BYTES,
            )
            .unwrap(),
        );

        assert_eq!(capture.pages.len(), 2);
        assert_eq!(capture.pages[0].after_image[0], 254);
        assert_eq!(capture.pages[1].after_image[0], 255);
    }

    #[test]
    fn capture_rejects_raw_wal_above_work_budget_before_parsing() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("oversized.sqlite-wal");
        File::create(&path)
            .unwrap()
            .set_len(crate::MAX_NATIVE_WAL_CAPTURE_BYTES + 1)
            .unwrap();
        let mut wal = File::open(path).unwrap();

        assert_eq!(
            capture_wal(
                &mut wal,
                1,
                crate::MAX_SQL_EFFECT_BYTES,
                crate::MAX_NATIVE_WAL_CAPTURE_BYTES,
            ),
            Err(crate::Error::ResourceExhausted(format!(
                "SQLite WAL raw capture exceeds {} bytes",
                crate::MAX_NATIVE_WAL_CAPTURE_BYTES
            )))
        );
    }

    #[test]
    fn capture_rejects_unique_pages_above_final_effect_budget() {
        let dir = TempDir::new().unwrap();
        let frames = (1..=1025)
            .map(|page_no| {
                let commit_pages = if page_no == 1025 { 1025 } else { 0 };
                (page_no, commit_pages, 1)
            })
            .collect::<Vec<_>>();
        let path = write_wal(&dir, &synthetic_wal(512, ChecksumOrder::Little, &frames));
        let mut wal = File::open(path).unwrap();

        assert!(matches!(
            capture_wal(
                &mut wal,
                1025,
                crate::MAX_SQL_EFFECT_BYTES,
                crate::MAX_NATIVE_WAL_CAPTURE_BYTES,
            ),
            Err(crate::Error::ResourceExhausted(message))
                if message.contains("changed page images")
        ));
    }

    #[test]
    fn capture_rejects_non_file_input() {
        let dir = TempDir::new().unwrap();
        let mut directory = File::open(dir.path()).unwrap();
        assert!(capture_wal(&mut directory, 1, 4096, u64::MAX).is_err());
    }

    #[test]
    fn captured_pages_reproduce_the_checkpointed_sqlite_database() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("source.sqlite");
        let base = dir.path().join("base.sqlite");
        let overlay = dir.path().join("overlay.sqlite");
        let connection = Connection::open(&db).unwrap();
        connection
            .execute_batch(
                "PRAGMA page_size=4096;\n\
                 PRAGMA journal_mode=WAL;\n\
                 PRAGMA wal_autocheckpoint=0;\n\
                 CREATE TABLE test(id INTEGER PRIMARY KEY, value BLOB);\n\
                 PRAGMA wal_checkpoint(TRUNCATE);",
            )
            .unwrap();
        fs::copy(&db, &base).unwrap();
        fs::copy(&base, &overlay).unwrap();
        let base_pages = (fs::metadata(&base).unwrap().len() / 4096) as u32;

        let transaction = connection.unchecked_transaction().unwrap();
        for id in 0..64 {
            transaction
                .execute(
                    "INSERT INTO test VALUES (?1, randomblob(300))",
                    rusqlite::params![id],
                )
                .unwrap();
        }
        transaction.commit().unwrap();

        let wal_path = db.with_extension("sqlite-wal");
        let mut wal = File::open(&wal_path).unwrap();
        let capture = committed(capture_wal(&mut wal, base_pages, usize::MAX, u64::MAX).unwrap());
        let mut target = OpenOptions::new().write(true).open(&overlay).unwrap();
        target.set_len(capture.target_file_bytes).unwrap();
        for page in capture.pages {
            target
                .seek(SeekFrom::Start(u64::from(page.page_no - 1) * 4096))
                .unwrap();
            target.write_all(&page.after_image).unwrap();
        }
        drop(target);

        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        assert_eq!(fs::read(&overlay).unwrap(), fs::read(&db).unwrap());
    }

    #[test]
    fn held_wal_descriptor_remains_parseable_after_no_checkpoint_close() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("held.sqlite");
        let connection = Connection::open(&db).unwrap();
        assert!(connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)
            .unwrap());
        assert!(connection
            .db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE)
            .unwrap());
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;\n\
                 PRAGMA wal_autocheckpoint=0;\n\
                 CREATE TABLE test(value TEXT);\n\
                 PRAGMA wal_checkpoint(TRUNCATE);\n\
                 INSERT INTO test VALUES ('held descriptor');",
            )
            .unwrap();
        let base_pages = (fs::metadata(&db).unwrap().len() / 4096) as u32;
        let wal_path = db.with_extension("sqlite-wal");
        let mut wal = File::open(&wal_path).unwrap();
        let sealed_len = wal.metadata().unwrap().len();

        drop(connection);

        assert_eq!(wal.metadata().unwrap().len(), sealed_len);
        let capture = committed(capture_wal(&mut wal, base_pages, usize::MAX, u64::MAX).unwrap());
        assert!(!capture.pages.is_empty());
    }
}
