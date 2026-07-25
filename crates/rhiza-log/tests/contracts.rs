use rhiza_core::{EntryType, LogEntry, LogHash};
use rhiza_log::{
    decode_segment_for_cluster, encode_open_segment, encode_segment, recover_open_segment_file,
    segment_file_name, IndexRange, SegmentHeader, QLOG_HEADER_LEN, QLOG_MAGIC,
};

#[test]
fn index_range_rejects_end_before_start() {
    let err = IndexRange::new(8, 7).unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid index range: start 8 is after end 7"
    );
}

#[test]
fn segment_file_name_is_zero_padded_qlog_range() {
    assert_eq!(
        segment_file_name(IndexRange::new(1, 1_000).unwrap()),
        "00000000000000000001-00000000000000001000.qlog"
    );
}

#[test]
fn segment_header_records_magic_epoch_and_start_index() {
    let header = SegmentHeader::new(LogHash::from_bytes([1; 32]), 2, 1_001, 1783619200000);

    assert_eq!(header.magic(), QLOG_MAGIC);
    assert_eq!(header.epoch(), 2);
    assert_eq!(header.start_index(), 1_001);
}

#[test]
fn segment_encode_decode_round_trips_entries() {
    let entry = entry(1, LogHash::ZERO, b"put\talpha\tbravo");

    let bytes = encode_segment(std::slice::from_ref(&entry));

    assert_eq!(&bytes[..4], &QLOG_MAGIC);
    assert_eq!(
        decode_segment_for_cluster(&bytes, "cluster-a").unwrap(),
        vec![entry]
    );
}

#[test]
fn segment_decode_rejects_hash_chain_mismatch() {
    let first = entry(1, LogHash::ZERO, b"put\talpha\tbravo");
    let second = entry(2, LogHash::ZERO, b"put\talpha\tcharlie");

    let err = decode_segment_for_cluster(&encode_segment(&[first, second]), "cluster-a")
        .unwrap_err()
        .to_string();

    assert!(err.contains("hash chain"));
}

#[test]
fn segment_decode_rejects_payload_tamper() {
    let first = entry(1, LogHash::ZERO, b"put\talpha\tbravo");
    let mut bytes = encode_segment(&[first]);
    let payload_offset = 76 + 108;
    bytes[payload_offset] ^= 1;

    let err = decode_segment_for_cluster(&bytes, "cluster-a")
        .unwrap_err()
        .to_string();

    assert!(err.contains("crc") || err.contains("payload_hash") || err.contains("entry_hash"));
}

#[test]
fn open_segment_recovery_truncates_partial_frame_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("00000000000000000001-open.qlog");
    let first = entry(1, LogHash::ZERO, b"put\talpha\tbravo");
    let second = entry(2, first.hash, b"put\talpha\tcharlie");
    let mut bytes = encode_open_segment(std::slice::from_ref(&first));
    let second_bytes = encode_open_segment(&[second]);
    bytes.extend_from_slice(&second_bytes[76..88]);
    std::fs::write(&path, bytes).unwrap();

    let recovered = recover_open_segment_file(&path, "cluster-a").unwrap();

    assert_eq!(recovered, vec![first]);
    assert_eq!(
        std::fs::read(path).unwrap(),
        encode_open_segment(&recovered)
    );
}

#[test]
fn open_segment_recovery_refuses_complete_corrupt_frame_without_truncating() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("00000000000000000001-open.qlog");
    let first = entry(1, LogHash::ZERO, b"put\talpha\tbravo");
    let second = entry(2, first.hash, b"put\talpha\tcharlie");
    let mut bytes = encode_open_segment(std::slice::from_ref(&first));
    let second_bytes = encode_open_segment(&[second]);
    bytes.extend_from_slice(&second_bytes[76..]);
    let second_payload_offset = encode_open_segment(std::slice::from_ref(&first)).len() + 108;
    bytes[second_payload_offset] ^= 1;
    std::fs::write(&path, &bytes).unwrap();

    let err = recover_open_segment_file(&path, "cluster-a")
        .unwrap_err()
        .to_string();

    assert!(err.contains("crc") || err.contains("hash"));
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

#[test]
fn open_segment_recovery_refuses_corrupt_non_final_frame_without_truncating() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("00000000000000000001-open.qlog");
    let first = entry(1, LogHash::ZERO, b"one");
    let second = entry(2, first.hash, b"two");
    let third = entry(3, second.hash, b"three");
    let second_frame_offset = encode_open_segment(std::slice::from_ref(&first)).len();
    let mut bytes = encode_open_segment(&[first, second, third]);
    bytes[second_frame_offset + 108] ^= 1;
    std::fs::write(&path, &bytes).unwrap();

    let err = recover_open_segment_file(&path, "cluster-a")
        .unwrap_err()
        .to_string();

    assert!(err.contains("crc") || err.contains("hash"));
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

#[test]
fn open_segment_recovery_refuses_torn_header_without_truncating() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("00000000000000000001-open.qlog");
    let bytes = encode_open_segment(&[entry(1, LogHash::ZERO, b"one")]);
    let torn = &bytes[..QLOG_HEADER_LEN - 1];
    std::fs::write(&path, torn).unwrap();

    let err = recover_open_segment_file(&path, "cluster-a")
        .unwrap_err()
        .to_string();

    assert!(err.contains("short qlog header"));
    assert_eq!(std::fs::read(path).unwrap(), torn);
}

#[test]
fn open_segment_recovery_refuses_corrupt_header_without_truncating() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("00000000000000000001-open.qlog");
    let mut bytes = encode_open_segment(&[entry(1, LogHash::ZERO, b"one")]);
    bytes[8] ^= 1;
    std::fs::write(&path, &bytes).unwrap();

    let err = recover_open_segment_file(&path, "cluster-a")
        .unwrap_err()
        .to_string();

    assert!(err.contains("header crc"));
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

#[test]
fn open_segment_recovery_refuses_complete_frame_with_malformed_length() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("00000000000000000001-open.qlog");
    let mut bytes = encode_open_segment(&[entry(1, LogHash::ZERO, b"one")]);
    let frame_len_offset = QLOG_HEADER_LEN + 4;
    let malformed_len = (bytes.len() - QLOG_HEADER_LEN + 1) as u32;
    bytes[frame_len_offset..frame_len_offset + 4].copy_from_slice(&malformed_len.to_be_bytes());
    std::fs::write(&path, &bytes).unwrap();

    let err = recover_open_segment_file(&path, "cluster-a")
        .unwrap_err()
        .to_string();

    assert!(err.contains("frame"));
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

#[test]
fn recovery_refuses_closed_segment_without_truncating() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("00000000000000000001-00000000000000000001.qlog");
    let mut bytes = encode_segment(&[entry(1, LogHash::ZERO, b"put\talpha\tbravo")]);
    let original_len = bytes.len();
    bytes.extend_from_slice(b"partial");
    std::fs::write(&path, &bytes).unwrap();

    let err = recover_open_segment_file(&path, "cluster-a")
        .unwrap_err()
        .to_string();

    assert!(err.contains("non-open"));
    assert_eq!(
        std::fs::read(path).unwrap().len(),
        original_len + b"partial".len()
    );
}

fn entry(index: u64, prev_hash: LogHash, payload: &[u8]) -> LogEntry {
    LogEntry {
        cluster_id: "cluster-a".into(),
        epoch: 1,
        config_id: 1,
        index,
        entry_type: EntryType::Command,
        payload: payload.to_vec(),
        prev_hash,
        hash: LogEntry::calculate_hash(
            "cluster-a",
            index,
            1,
            1,
            EntryType::Command,
            prev_hash,
            payload,
        ),
    }
}
