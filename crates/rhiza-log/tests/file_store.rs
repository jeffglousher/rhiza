use std::fs;

use rhiza_core::{
    ConfigChange, ConfigurationState, EntryType, LogAnchor, LogEntry, LogHash, RecoveryAnchor,
    SnapshotIdentity,
};
use rhiza_log::{
    encode_open_segment, encode_segment, segment_file_name, FileLogStore, IndexRange, LogState,
    LogStore, OPEN_SEGMENT_MAX_BYTES, OPEN_SEGMENT_MAX_ENTRIES, QLOG_HEADER_LEN,
};

#[test]
fn file_log_store_reopens_published_segments() {
    let dir = tempfile::tempdir().unwrap();
    let first = entry(1, LogHash::ZERO, b"one");
    let second = entry(2, first.hash, b"two");
    {
        let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
        store.append(&first).unwrap();
        store.append(&second).unwrap();
    }

    let reopened = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();

    assert_eq!(reopened.read(1).unwrap(), Some(first));
    assert_eq!(reopened.read(2).unwrap(), Some(second));
    assert_eq!(reopened.last_index().unwrap(), Some(2));
}

#[test]
fn file_log_store_recovers_open_segment_after_closed_canonical_segment() {
    let dir = tempfile::tempdir().unwrap();
    let first = entry(1, LogHash::ZERO, b"one");
    let second = entry(2, first.hash, b"two");
    write_closed_segment(dir.path(), std::slice::from_ref(&first));
    let open_path = dir.path().join("00000000000000000002-open.qlog");
    let open_bytes = encode_open_segment(std::slice::from_ref(&second));
    fs::write(&open_path, &open_bytes).unwrap();

    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();

    assert_eq!(store.last_index().unwrap(), Some(2));
    assert_eq!(store.read(2).unwrap(), Some(second));
    assert_eq!(fs::read(&open_path).unwrap(), open_bytes);
    assert!(open_path.exists());
}

#[test]
fn buffered_appends_share_one_open_segment_until_group_sync() {
    let dir = tempfile::tempdir().unwrap();
    let entries = chain(&[b"one", b"two", b"three"]);
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();

    store.append_batch_buffered(&entries[..1]).unwrap();
    store.append_batch_buffered(&entries[1..]).unwrap();

    assert_eq!(closed_segment_names(dir.path()), Vec::<String>::new());
    assert_eq!(open_segment_names(dir.path()), vec![open_segment_name(1)]);
    assert_eq!(store.sync().unwrap(), Some(3));
    drop(store);

    let reopened = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    assert_eq!(
        reopened.read_range(IndexRange::new(1, 3).unwrap()).unwrap(),
        entries
    );
}

#[test]
fn file_log_store_truncates_incomplete_open_frame_and_continues_hash_chain() {
    let entries = chain(&[b"one", b"two"]);
    let first_len = encode_open_segment(&entries[..1]).len();
    let complete = encode_open_segment(&entries);

    for cut in first_len + 1..complete.len() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(open_segment_name(1));
        fs::write(&path, &complete[..cut]).unwrap();

        let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();

        assert_eq!(store.last_index().unwrap(), Some(1), "cut={cut}");
        assert_eq!(fs::read(&path).unwrap(), encode_open_segment(&entries[..1]));
        store.append(&entries[1]).unwrap();
        drop(store);

        let reopened = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
        assert_eq!(
            reopened.read_range(IndexRange::new(1, 2).unwrap()).unwrap(),
            entries,
            "cut={cut}"
        );
    }
}

#[test]
fn file_log_store_recovers_large_open_segment() {
    let dir = tempfile::tempdir().unwrap();
    let entries = generated_chain(16_384, 16);
    let path = dir.path().join(open_segment_name(1));
    let bytes = encode_open_segment(&entries);
    fs::write(&path, &bytes).unwrap();

    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();

    assert_eq!(store.last_index().unwrap(), Some(entries.len() as u64));
    assert_eq!(store.read(1).unwrap(), entries.first().cloned());
    assert_eq!(
        store.read(entries.len() as u64).unwrap(),
        entries.last().cloned()
    );
    assert_eq!(fs::read(path).unwrap(), bytes);
}

#[test]
fn file_log_store_rolls_open_segments_at_entry_limit() {
    let dir = tempfile::tempdir().unwrap();
    let entries = generated_chain(OPEN_SEGMENT_MAX_ENTRIES + 1, 0);
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();

    store.append_batch(&entries).unwrap();

    assert_eq!(
        closed_segment_names(dir.path()),
        vec![segment_file_name(
            IndexRange::new(1, OPEN_SEGMENT_MAX_ENTRIES as u64).unwrap()
        )]
    );
    assert_eq!(
        open_segment_names(dir.path()),
        vec![open_segment_name(OPEN_SEGMENT_MAX_ENTRIES as u64 + 1)]
    );
    drop(store);

    let reopened = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    assert_eq!(
        reopened
            .read_range(IndexRange::new(1, entries.len() as u64).unwrap())
            .unwrap(),
        entries
    );
}

#[test]
fn file_log_store_rolls_open_segments_before_byte_limit() {
    let dir = tempfile::tempdir().unwrap();
    let payload_len = OPEN_SEGMENT_MAX_BYTES / 3;
    let entries = generated_chain(3, payload_len);
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();

    store.append_batch(&entries).unwrap();

    assert_eq!(
        closed_segment_names(dir.path()),
        vec![segment_file_name(IndexRange::new(1, 2).unwrap())]
    );
    assert_eq!(open_segment_names(dir.path()), vec![open_segment_name(3)]);
    for (_, bytes) in segment_files(dir.path()) {
        assert!(bytes.len() <= OPEN_SEGMENT_MAX_BYTES);
    }
}

#[test]
fn file_log_store_recovers_roll_crash_with_duplicate_closed_segment() {
    let dir = tempfile::tempdir().unwrap();
    let entries = generated_chain(OPEN_SEGMENT_MAX_ENTRIES, 0);
    write_closed_segment(dir.path(), &entries);
    let open_path = dir.path().join(open_segment_name(1));
    fs::write(&open_path, encode_open_segment(&entries)).unwrap();

    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();

    assert_eq!(store.last_index().unwrap(), Some(entries.len() as u64));
    assert!(!open_path.exists());
    let next = entry(
        entries.len() as u64 + 1,
        entries.last().unwrap().hash,
        b"next",
    );
    store.append(&next).unwrap();
    assert_eq!(store.read(next.index).unwrap(), Some(next));
}

#[test]
fn rolled_segments_preserve_compact_and_truncate_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let entries = generated_chain(OPEN_SEGMENT_MAX_ENTRIES + 5, 0);
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    store.append_batch(&entries).unwrap();
    let compacted_position = OPEN_SEGMENT_MAX_ENTRIES - 2;
    let anchor = recovery_anchor(&entries[compacted_position], 1);

    store.compact_prefix(&anchor).unwrap();
    store
        .truncate_suffix(OPEN_SEGMENT_MAX_ENTRIES as u64 + 4)
        .unwrap();

    assert_eq!(
        store
            .read_range(IndexRange::new(1, entries.len() as u64).unwrap())
            .unwrap(),
        entries[compacted_position + 1..OPEN_SEGMENT_MAX_ENTRIES + 3]
    );
    drop(store);

    let reopened = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    assert_eq!(reopened.logical_state().unwrap().anchor, Some(anchor));
    assert_eq!(
        reopened.last_index().unwrap(),
        Some(OPEN_SEGMENT_MAX_ENTRIES as u64 + 3)
    );
}

#[test]
fn file_log_store_fails_closed_on_complete_open_frame_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(open_segment_name(1));
    let mut bytes = encode_open_segment(&chain(&[b"one", b"two"]));
    bytes[QLOG_HEADER_LEN + 108] ^= 1;
    fs::write(&path, &bytes).unwrap();

    let err = FileLogStore::open(dir.path(), "cluster-a", 1, 1)
        .unwrap_err()
        .to_string();

    assert!(err.contains("crc") || err.contains("hash"));
    assert_eq!(fs::read(path).unwrap(), bytes);
}

#[test]
fn file_log_store_rejects_missing_leading_segment_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let entries = chain(&[b"one", b"two", b"three"]);
    for entry in &entries {
        write_closed_segment(dir.path(), std::slice::from_ref(entry));
    }
    fs::remove_file(
        dir.path()
            .join(segment_file_name(IndexRange::new(1, 1).unwrap())),
    )
    .unwrap();

    assert!(FileLogStore::open(dir.path(), "cluster-a", 1, 1).is_err());
}

#[test]
fn file_log_store_rejects_cross_segment_predecessor_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let first = entry(1, LogHash::ZERO, b"one");
    let second = entry(2, LogHash::ZERO, b"two");
    write_closed_segment(dir.path(), std::slice::from_ref(&first));
    write_closed_segment(dir.path(), std::slice::from_ref(&second));

    let err = FileLogStore::open(dir.path(), "cluster-a", 1, 1)
        .unwrap_err()
        .to_string();

    assert!(err.contains("hash chain"));
}

#[test]
fn file_log_store_refuses_complete_segment_corruption_without_modifying_it() {
    let dir = tempfile::tempdir().unwrap();
    let first = entry(1, LogHash::ZERO, b"one");
    let path = dir
        .path()
        .join(segment_file_name(IndexRange::new(1, 1).unwrap()));
    let mut bytes = encode_segment(&[first]);
    bytes[76 + 108] ^= 1;
    fs::write(&path, &bytes).unwrap();

    let err = FileLogStore::open(dir.path(), "cluster-a", 1, 1)
        .unwrap_err()
        .to_string();

    assert!(err.contains("crc") || err.contains("hash"));
    assert_eq!(fs::read(path).unwrap(), bytes);
}

#[test]
fn file_log_store_exposes_append_and_read_behavior() {
    let dir = tempfile::tempdir().unwrap();
    let entries = chain(&[b"one", b"two", b"three", b"four"]);
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();

    assert_eq!(store.last_index().unwrap(), None);
    store.append_batch(&entries).unwrap();
    assert_eq!(store.read(3).unwrap(), Some(entries[2].clone()));
    assert_eq!(
        store.read_range(IndexRange::new(2, 4).unwrap()).unwrap(),
        entries[1..].to_vec()
    );
    drop(store);

    let reopened = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    assert_eq!(
        reopened.read_range(IndexRange::new(1, 4).unwrap()).unwrap(),
        entries
    );
}

#[test]
fn file_log_store_truncates_suffix_and_reopens_exact_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let entries = chain(&[b"one", b"two", b"three", b"four", b"five", b"six"]);
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    store.append_batch(&entries[..2]).unwrap();
    store.append_batch(&entries[2..4]).unwrap();
    store.append_batch(&entries[4..]).unwrap();

    store.truncate_suffix(4).unwrap();

    assert_eq!(
        store.read_range(IndexRange::new(1, 6).unwrap()).unwrap(),
        entries[..3]
    );
    assert_eq!(store.last_index().unwrap(), Some(3));
    assert_eq!(
        closed_segment_names(dir.path()),
        vec![segment_file_name(IndexRange::new(1, 3).unwrap())]
    );
    drop(store);

    let reopened = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    assert_eq!(
        reopened.read_range(IndexRange::new(1, 6).unwrap()).unwrap(),
        entries[..3]
    );
    reopened.append_batch(&entries[3..]).unwrap();
    assert_eq!(reopened.last_index().unwrap(), Some(6));
}

#[test]
fn file_log_store_truncates_suffix_at_segment_boundary_without_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let entries = chain(&[b"one", b"two", b"three", b"four"]);
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    store.append_batch(&entries[..2]).unwrap();
    store.append_batch(&entries[2..]).unwrap();

    store.truncate_suffix(3).unwrap();

    assert_eq!(store.last_index().unwrap(), Some(2));
    assert_eq!(
        closed_segment_names(dir.path()),
        vec![segment_file_name(IndexRange::new(1, 2).unwrap()),]
    );
}

#[test]
fn file_log_store_compacts_verified_prefix_and_persists_logical_state() {
    let dir = tempfile::tempdir().unwrap();
    let entries = chain(&[b"one", b"two", b"three", b"four"]);
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    store.append_batch(&entries[..3]).unwrap();
    store.append(&entries[3]).unwrap();
    let anchor = recovery_anchor(&entries[1], 1);

    store.compact_prefix(&anchor).unwrap();

    assert_eq!(
        store.read_range(IndexRange::new(1, 3).unwrap()).unwrap(),
        entries[2..3]
    );
    assert_eq!(store.read(2).unwrap(), None);
    assert_eq!(store.last_index().unwrap(), Some(4));
    assert_eq!(
        store.logical_state().unwrap(),
        LogState {
            anchor: Some(anchor.clone()),
            first_retained_index: 3,
            tip: Some(LogAnchor::new(4, entries[3].hash)),
        }
    );
    assert_eq!(
        closed_segment_names(dir.path()),
        vec![segment_file_name(IndexRange::new(3, 4).unwrap())]
    );
    drop(store);

    let reopened = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    assert_eq!(reopened.logical_state().unwrap().anchor, Some(anchor));
    assert_eq!(reopened.read(3).unwrap(), Some(entries[2].clone()));
}

#[test]
fn fresh_file_log_store_installs_verified_recovery_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let configuration = ConfigurationState::active(1, LogHash::from_bytes([7; 32]));
    let anchor = RecoveryAnchor::new(
        "cluster-a",
        1,
        configuration.clone(),
        4,
        LogAnchor::new(9, LogHash::from_bytes([8; 32])),
        SnapshotIdentity::new(
            "snapshot-000000000000009",
            LogHash::from_bytes([9; 32]),
            4096,
            LogHash::from_bytes([10; 32]),
        ),
    );
    let store =
        FileLogStore::open_with_configuration(dir.path(), "cluster-a", 1, configuration.clone())
            .unwrap();

    store
        .install_recovery_anchor(&anchor, 4, &configuration)
        .unwrap();
    assert_eq!(
        store.logical_state().unwrap(),
        LogState {
            anchor: Some(anchor.clone()),
            first_retained_index: 10,
            tip: Some(*anchor.compacted()),
        }
    );
    drop(store);

    let reopened =
        FileLogStore::open_with_configuration(dir.path(), "cluster-a", 1, configuration).unwrap();
    assert_eq!(reopened.logical_state().unwrap().anchor, Some(anchor));
}

#[test]
fn recovery_anchor_binary_round_trip_preserves_bound_successor() {
    let digest = LogHash::from_bytes([4; 32]);
    let stop = LogAnchor::new(10, LogHash::from_bytes([5; 32]));
    let stop_change = ConfigChange::stop(
        "cluster-a",
        4,
        digest,
        5,
        vec!["node-a".into(), "node-b".into(), "node-c".into()],
    )
    .unwrap();
    let state = ConfigurationState::stopped(
        4,
        digest,
        stop,
        stop_change.successor().clone(),
        stop_change.to_stored_command().hash(),
    );
    let anchor = RecoveryAnchor::new(
        "cluster-a",
        1,
        state.clone(),
        7,
        stop,
        SnapshotIdentity::new(
            "snapshot-stop",
            LogHash::from_bytes([9; 32]),
            4096,
            LogHash::from_bytes([10; 32]),
        ),
    );
    let dir = tempfile::tempdir().unwrap();
    let store =
        FileLogStore::open_with_configuration(dir.path(), "cluster-a", 1, state.clone()).unwrap();

    store.install_recovery_anchor(&anchor, 7, &state).unwrap();
    drop(store);

    let reopened =
        FileLogStore::open_with_configuration(dir.path(), "cluster-a", 1, state.clone()).unwrap();
    assert_eq!(reopened.configuration_state().unwrap(), state);
    assert_eq!(reopened.logical_state().unwrap().anchor, Some(anchor));
}

#[test]
fn recovery_anchor_install_rejects_nonempty_or_mismatched_store_without_mutation() {
    let configuration = ConfigurationState::active(1, LogHash::from_bytes([7; 32]));
    let valid = RecoveryAnchor::new(
        "cluster-a",
        1,
        configuration.clone(),
        4,
        LogAnchor::new(9, LogHash::from_bytes([8; 32])),
        SnapshotIdentity::new(
            "snapshot-000000000000009",
            LogHash::from_bytes([9; 32]),
            4096,
            LogHash::from_bytes([10; 32]),
        ),
    );

    for invalid in [
        RecoveryAnchor::new(
            "cluster-b",
            1,
            configuration.clone(),
            4,
            *valid.compacted(),
            valid.snapshot().clone(),
        ),
        RecoveryAnchor::new(
            "cluster-a",
            2,
            configuration.clone(),
            4,
            *valid.compacted(),
            valid.snapshot().clone(),
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = FileLogStore::open_with_configuration(
            dir.path(),
            "cluster-a",
            1,
            configuration.clone(),
        )
        .unwrap();
        assert!(store
            .install_recovery_anchor(&invalid, 4, &configuration)
            .is_err());
        assert!(!dir.path().join("recovery.anchor").exists());
    }

    for (generation, expected_configuration) in [
        (5, configuration.clone()),
        (
            4,
            ConfigurationState::active(1, LogHash::from_bytes([6; 32])),
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = FileLogStore::open_with_configuration(
            dir.path(),
            "cluster-a",
            1,
            configuration.clone(),
        )
        .unwrap();
        assert!(store
            .install_recovery_anchor(&valid, generation, &expected_configuration)
            .is_err());
        assert!(!dir.path().join("recovery.anchor").exists());
    }

    let dir = tempfile::tempdir().unwrap();
    let store =
        FileLogStore::open_with_configuration(dir.path(), "cluster-a", 1, configuration.clone())
            .unwrap();
    store.append(&entry(1, LogHash::ZERO, b"one")).unwrap();
    let before = segment_files(dir.path());
    assert!(store
        .install_recovery_anchor(&valid, 4, &configuration)
        .is_err());
    assert_eq!(segment_files(dir.path()), before);
    assert!(!dir.path().join("recovery.anchor").exists());
}

#[test]
fn official_local_compaction_example_continues_hash_chain_after_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let entries = chain(&[b"one", b"two", b"three"]);
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    store.append_batch(&entries).unwrap();

    let verified_snapshot_anchor = recovery_anchor(&entries[2], 1);
    store.compact_prefix(&verified_snapshot_anchor).unwrap();

    let next = entry(4, verified_snapshot_anchor.compacted().hash(), b"four");
    store.append(&next).unwrap();
    assert_eq!(store.read(4).unwrap(), Some(next));
    assert_eq!(store.logical_state().unwrap().first_retained_index, 4);
}

#[test]
fn file_log_store_uses_anchor_as_tip_when_compaction_removes_entire_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let entries = chain(&[b"one", b"two"]);
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    store.append_batch(&entries).unwrap();
    let anchor = recovery_anchor(&entries[1], 1);

    store.compact_prefix(&anchor).unwrap();

    assert_eq!(store.last_index().unwrap(), Some(2));
    assert_eq!(
        store.logical_state().unwrap().tip,
        Some(*anchor.compacted())
    );
    let third = entry(3, entries[1].hash, b"three");
    store.append(&third).unwrap();
    assert_eq!(store.read(3).unwrap(), Some(third));
}

#[test]
fn file_log_store_rejects_unverified_or_regressing_compaction_without_changes() {
    let dir = tempfile::tempdir().unwrap();
    let entries = chain(&[b"one", b"two", b"three"]);
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    store.append_batch(&entries).unwrap();
    let files_before = segment_files(dir.path());

    let mut invalid = [
        recovery_anchor(&entries[2], 1),
        recovery_anchor(&entries[1], 1),
        recovery_anchor(&entries[0], 1),
    ];
    invalid[0] = RecoveryAnchor::new(
        "cluster-a",
        1,
        ConfigurationState::active(1, LogHash::ZERO),
        1,
        LogAnchor::new(4, entries[2].hash),
        invalid[0].snapshot().clone(),
    );
    invalid[1] = RecoveryAnchor::new(
        "cluster-a",
        1,
        ConfigurationState::active(1, LogHash::ZERO),
        1,
        LogAnchor::new(2, LogHash::ZERO),
        invalid[1].snapshot().clone(),
    );
    for anchor in &invalid[..2] {
        assert!(store.compact_prefix(anchor).is_err());
    }
    assert_eq!(segment_files(dir.path()), files_before);

    store
        .compact_prefix(&recovery_anchor(&entries[1], 1))
        .unwrap();
    assert!(store.compact_prefix(&invalid[2]).is_err());
    let conflicting = recovery_anchor(&entries[1], 2);
    assert!(store.compact_prefix(&conflicting).is_err());
    store
        .compact_prefix(&recovery_anchor(&entries[1], 1))
        .unwrap();
}

#[test]
fn file_log_store_rejects_truncate_at_or_below_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let entries = chain(&[b"one", b"two", b"three"]);
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    store.append_batch(&entries).unwrap();
    store
        .compact_prefix(&recovery_anchor(&entries[1], 1))
        .unwrap();

    assert!(store.truncate_suffix(1).is_err());
    assert!(store.truncate_suffix(2).is_err());
    assert_eq!(store.last_index().unwrap(), Some(3));
}

#[test]
fn file_log_store_rejects_corrupted_persisted_anchor_without_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let entries = chain(&[b"one", b"two"]);
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    store.append_batch(&entries).unwrap();
    store
        .compact_prefix(&recovery_anchor(&entries[0], 1))
        .unwrap();
    drop(store);
    let anchor_path = dir.path().join("recovery.anchor");
    let mut bytes = fs::read(&anchor_path).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 1;
    fs::write(&anchor_path, &bytes).unwrap();

    assert!(FileLogStore::open(dir.path(), "cluster-a", 1, 1).is_err());
    assert_eq!(fs::read(anchor_path).unwrap(), bytes);
}

#[test]
fn file_log_store_rejects_noncontiguous_batch_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let first = entry(1, LogHash::ZERO, b"one");
    let skipped = entry(3, first.hash, b"three");
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();

    let err = store
        .append_batch(&[first, skipped])
        .unwrap_err()
        .to_string();

    assert!(err.contains("index"));
    assert_eq!(store.last_index().unwrap(), None);
    assert_eq!(closed_segments(dir.path()), 0);
}

#[test]
fn file_log_store_rejects_invalid_entry_identity_and_hash_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let first = entry(1, LogHash::ZERO, b"one");
    let second = entry(2, first.hash, b"two");
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    store.append(&first).unwrap();

    let mut invalid_entries = Vec::new();
    let mut wrong_cluster = second.clone();
    wrong_cluster.cluster_id = "cluster-b".into();
    invalid_entries.push(wrong_cluster);
    let mut wrong_epoch = second.clone();
    wrong_epoch.epoch = 2;
    invalid_entries.push(wrong_epoch);
    let mut wrong_config = second.clone();
    wrong_config.config_id = 2;
    invalid_entries.push(wrong_config);
    let mut wrong_predecessor = second.clone();
    wrong_predecessor.prev_hash = LogHash::ZERO;
    invalid_entries.push(wrong_predecessor);
    let mut wrong_hash = second.clone();
    wrong_hash.hash = LogHash::ZERO;
    invalid_entries.push(wrong_hash);

    for invalid in invalid_entries {
        assert!(store.append(&invalid).is_err());
    }

    assert_eq!(store.last_index().unwrap(), Some(1));
    assert_eq!(closed_segments(dir.path()), 1);
    store.append(&second).unwrap();
}

#[test]
fn file_log_store_rejects_configured_identity_mismatch_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
    store.append(&entry(1, LogHash::ZERO, b"one")).unwrap();
    drop(store);

    assert!(FileLogStore::open(dir.path(), "cluster-b", 1, 1).is_err());
    assert!(FileLogStore::open(dir.path(), "cluster-a", 2, 1).is_err());
    assert!(FileLogStore::open(dir.path(), "cluster-a", 1, 2).is_err());
}

#[test]
fn file_log_store_persists_stop_activation_across_homogeneous_segments() {
    let dir = tempfile::tempdir().unwrap();
    let old = ConfigurationState::active(1, LogHash::from_bytes([1; 32]));
    let stop_change = canonical_stop(1, old.digest());
    let successor = stop_change.successor().clone();
    let stop_command_hash = stop_change.to_stored_command().hash();
    let stop = transition_entry(1, 1, LogHash::ZERO, stop_change);
    let activation = transition_entry(
        2,
        2,
        stop.hash,
        ConfigChange::activation_barrier(successor.clone(), 1, stop.hash, stop_command_hash),
    );
    let next = LogEntry {
        cluster_id: "cluster-a".into(),
        epoch: 1,
        config_id: 2,
        index: 3,
        entry_type: EntryType::Noop,
        payload: Vec::new(),
        prev_hash: activation.hash,
        hash: LogEntry::calculate_hash("cluster-a", 3, 1, 2, EntryType::Noop, activation.hash, &[]),
    };

    let store = FileLogStore::open_with_configuration(dir.path(), "cluster-a", 1, old).unwrap();
    store
        .append_batch(&[stop.clone(), activation.clone(), next.clone()])
        .unwrap();
    assert_eq!(closed_segments(dir.path()), 2);
    assert_eq!(
        store.configuration_state().unwrap(),
        ConfigurationState::active(2, successor.digest())
    );
    drop(store);

    let reopened = FileLogStore::open_with_configuration(
        dir.path(),
        "cluster-a",
        1,
        ConfigurationState::active(1, LogHash::from_bytes([1; 32])),
    )
    .unwrap();
    assert_eq!(reopened.read(3).unwrap(), Some(next));
}

#[test]
fn append_batch_advances_configuration_after_each_published_segment() {
    let dir = tempfile::tempdir().unwrap();
    let old = ConfigurationState::active(1, LogHash::from_bytes([1; 32]));
    let stop_change = canonical_stop(1, old.digest());
    let successor = stop_change.successor().clone();
    let stop_command_hash = stop_change.to_stored_command().hash();
    let stop = transition_entry(1, 1, LogHash::ZERO, stop_change);
    let activation = transition_entry(
        2,
        2,
        stop.hash,
        ConfigChange::activation_barrier(successor.clone(), 1, stop.hash, stop_command_hash),
    );
    let activation_path = dir.path().join(open_segment_name(2));
    let store = FileLogStore::open_with_configuration(dir.path(), "cluster-a", 1, old).unwrap();
    fs::write(&activation_path, b"block publication").unwrap();

    assert!(store.append_batch(&[stop.clone(), activation]).is_err());
    fs::remove_file(activation_path).unwrap();
    assert_eq!(
        store.configuration_state().unwrap(),
        ConfigurationState::stopped(
            1,
            LogHash::from_bytes([1; 32]),
            LogAnchor::new(1, stop.hash),
            successor,
            stop_command_hash,
        )
    );

    let old_config_entry = LogEntry {
        cluster_id: "cluster-a".into(),
        epoch: 1,
        config_id: 1,
        index: 2,
        entry_type: EntryType::Noop,
        payload: Vec::new(),
        prev_hash: stop.hash,
        hash: LogEntry::calculate_hash("cluster-a", 2, 1, 1, EntryType::Noop, stop.hash, &[]),
    };
    assert!(store.append(&old_config_entry).is_err());
    drop(store);
    FileLogStore::open_with_configuration(
        dir.path(),
        "cluster-a",
        1,
        ConfigurationState::active(1, LogHash::from_bytes([1; 32])),
    )
    .unwrap();
}

#[test]
fn repeated_compaction_crosses_stop_activation_into_successor_config() {
    let dir = tempfile::tempdir().unwrap();
    let old = ConfigurationState::active(1, LogHash::from_bytes([1; 32]));
    let stop_change = canonical_stop(1, old.digest());
    let successor = stop_change.successor().clone();
    let stop_command_hash = stop_change.to_stored_command().hash();
    let stop = transition_entry(1, 1, LogHash::ZERO, stop_change);
    let next_state = ConfigurationState::active(2, successor.digest());
    let activation = transition_entry(
        2,
        2,
        stop.hash,
        ConfigChange::activation_barrier(successor.clone(), 1, stop.hash, stop_command_hash),
    );
    let next = LogEntry {
        cluster_id: "cluster-a".into(),
        epoch: 1,
        config_id: 2,
        index: 3,
        entry_type: EntryType::Noop,
        payload: Vec::new(),
        prev_hash: activation.hash,
        hash: LogEntry::calculate_hash("cluster-a", 3, 1, 2, EntryType::Noop, activation.hash, &[]),
    };
    let store =
        FileLogStore::open_with_configuration(dir.path(), "cluster-a", 1, old.clone()).unwrap();
    store
        .append_batch(&[stop.clone(), activation.clone(), next.clone()])
        .unwrap();
    store
        .compact_prefix(&RecoveryAnchor::new(
            "cluster-a",
            1,
            ConfigurationState::stopped(
                1,
                old.digest(),
                LogAnchor::new(1, stop.hash),
                successor,
                stop_command_hash,
            ),
            1,
            LogAnchor::new(1, stop.hash),
            SnapshotIdentity::new(
                "snapshot-stop",
                LogHash::from_bytes([8; 32]),
                4096,
                LogHash::from_bytes([10; 32]),
            ),
        ))
        .unwrap();
    drop(store);

    let reopened =
        FileLogStore::open_with_configuration(dir.path(), "cluster-a", 1, old.clone()).unwrap();
    reopened
        .compact_prefix(&RecoveryAnchor::new(
            "cluster-a",
            1,
            next_state.clone(),
            1,
            LogAnchor::new(2, activation.hash),
            SnapshotIdentity::new(
                "snapshot-activation",
                LogHash::from_bytes([9; 32]),
                8192,
                LogHash::from_bytes([10; 32]),
            ),
        ))
        .unwrap();
    drop(reopened);

    let reopened = FileLogStore::open_with_configuration(dir.path(), "cluster-a", 1, old).unwrap();
    assert_eq!(reopened.configuration_state().unwrap(), next_state);
    assert_eq!(reopened.read(3).unwrap(), Some(next));
}

#[test]
fn compaction_at_stop_or_activation_preserves_transition_enforcement() {
    for compact_activation in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let old = ConfigurationState::active(1, LogHash::from_bytes([1; 32]));
        let stop_change = canonical_stop(1, old.digest());
        let successor = stop_change.successor().clone();
        let stop_command_hash = stop_change.to_stored_command().hash();
        let stop = transition_entry(1, 1, LogHash::ZERO, stop_change);
        let next_state = ConfigurationState::active(2, successor.digest());
        let activation = transition_entry(
            2,
            2,
            stop.hash,
            ConfigChange::activation_barrier(successor.clone(), 1, stop.hash, stop_command_hash),
        );
        let store = FileLogStore::open_with_configuration(dir.path(), "cluster-a", 1, old).unwrap();
        store
            .append_batch(&[stop.clone(), activation.clone()])
            .unwrap();
        let (target, state) = if compact_activation {
            (&activation, next_state.clone())
        } else {
            (
                &stop,
                ConfigurationState::stopped(
                    1,
                    LogHash::from_bytes([1; 32]),
                    LogAnchor::new(1, stop.hash),
                    successor,
                    stop_command_hash,
                ),
            )
        };
        store
            .compact_prefix(&RecoveryAnchor::new(
                "cluster-a",
                1,
                state.clone(),
                1,
                LogAnchor::new(target.index, target.hash),
                SnapshotIdentity::new(
                    "snapshot",
                    LogHash::from_bytes([9; 32]),
                    4096,
                    LogHash::from_bytes([10; 32]),
                ),
            ))
            .unwrap();
        drop(store);

        let reopened = FileLogStore::open_with_configuration(
            dir.path(),
            "cluster-a",
            1,
            ConfigurationState::active(1, LogHash::from_bytes([1; 32])),
        )
        .unwrap();
        assert_eq!(reopened.configuration_state().unwrap(), next_state);
    }
}

fn write_closed_segment(dir: &std::path::Path, entries: &[LogEntry]) {
    let range = IndexRange::new(entries[0].index, entries.last().unwrap().index).unwrap();
    fs::write(dir.join(segment_file_name(range)), encode_segment(entries)).unwrap();
}

fn closed_segments(dir: &std::path::Path) -> usize {
    fs::read_dir(dir)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "qlog"))
        .count()
}

fn closed_segment_names(dir: &std::path::Path) -> Vec<String> {
    let mut names = fs::read_dir(dir)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.ends_with(".qlog") && !name.ends_with("-open.qlog"))
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn open_segment_name(start: u64) -> String {
    format!("{start:020}-open.qlog")
}

fn open_segment_names(dir: &std::path::Path) -> Vec<String> {
    let mut names = fs::read_dir(dir)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.ends_with("-open.qlog"))
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn segment_files(dir: &std::path::Path) -> Vec<(std::ffi::OsString, Vec<u8>)> {
    let mut files = fs::read_dir(dir)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "qlog"))
        .map(|entry| (entry.file_name(), fs::read(entry.path()).unwrap()))
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn chain(payloads: &[&[u8]]) -> Vec<LogEntry> {
    let mut entries = Vec::new();
    let mut prev_hash = LogHash::ZERO;
    for (position, payload) in payloads.iter().enumerate() {
        let next = entry(position as u64 + 1, prev_hash, payload);
        prev_hash = next.hash;
        entries.push(next);
    }
    entries
}

fn generated_chain(count: usize, payload_len: usize) -> Vec<LogEntry> {
    let mut entries = Vec::with_capacity(count);
    let mut prev_hash = LogHash::ZERO;
    for position in 0..count {
        let payload = vec![(position % 251) as u8; payload_len];
        let next = entry(position as u64 + 1, prev_hash, &payload);
        prev_hash = next.hash;
        entries.push(next);
    }
    entries
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

fn recovery_anchor(entry: &LogEntry, recovery_generation: u64) -> RecoveryAnchor {
    RecoveryAnchor::new(
        entry.cluster_id.clone(),
        entry.epoch,
        ConfigurationState::active(entry.config_id, LogHash::ZERO),
        recovery_generation,
        LogAnchor::new(entry.index, entry.hash),
        SnapshotIdentity::new(
            format!("snapshot-{:015}", entry.index),
            LogHash::digest(&[b"verified snapshot", &entry.index.to_be_bytes()]),
            4096 + entry.index,
            LogHash::from_bytes([10; 32]),
        ),
    )
}

fn transition_entry(
    index: u64,
    config_id: u64,
    prev_hash: LogHash,
    change: ConfigChange,
) -> LogEntry {
    let command = change.to_stored_command();
    LogEntry {
        cluster_id: "cluster-a".into(),
        epoch: 1,
        config_id,
        index,
        entry_type: command.entry_type,
        hash: LogEntry::calculate_hash(
            "cluster-a",
            index,
            1,
            config_id,
            command.entry_type,
            prev_hash,
            &command.payload,
        ),
        payload: command.payload,
        prev_hash,
    }
}

fn canonical_stop(predecessor_config_id: u64, predecessor_digest: LogHash) -> ConfigChange {
    ConfigChange::stop(
        "cluster-a",
        predecessor_config_id,
        predecessor_digest,
        predecessor_config_id + 1,
        vec!["node-a".into(), "node-b".into(), "node-c".into()],
    )
    .unwrap()
}
