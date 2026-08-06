use std::path::Path;

use redb::{Database, ReadableDatabase, TableDefinition};
use rhiza_core::{
    ConfigChange, EntryType, ExecutionProfile, LogEntry, LogHash, ReplicatedCommandEnvelope,
};
use rhiza_kv::{
    encode_replicated_kv_command, kv_materializer_fingerprint, restore_snapshot_file, Error,
    KvCommandResultV1, KvCommandV1, RedbStateMachine,
};

const DATA_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__rhiza_kv_data_v1");
const REQUEST_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__rhiza_kv_requests_v1");
const PROGRESS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__rhiza_kv_progress_v1");

#[derive(Clone, Copy)]
enum PartialState {
    Metadata,
    Data,
    Receipt,
}

fn write_partial_state(path: &Path, state: PartialState) {
    let database = Database::create(path).unwrap();
    let write = database.begin_write().unwrap();
    match state {
        PartialState::Metadata => {
            let mut progress = write.open_table(PROGRESS_TABLE).unwrap();
            progress.insert("node_id", b"node-x".as_slice()).unwrap();
        }
        PartialState::Data => {
            let mut data = write.open_table(DATA_TABLE).unwrap();
            data.insert(b"key".as_slice(), b"value".as_slice()).unwrap();
        }
        PartialState::Receipt => {
            let mut requests = write.open_table(REQUEST_TABLE).unwrap();
            requests
                .insert(b"request-1".as_slice(), b"receipt-bytes".as_slice())
                .unwrap();
        }
    }
    write.commit().unwrap();
}

fn read_partial_bytes(path: &Path, state: PartialState) -> Vec<u8> {
    let database = Database::create(path).unwrap();
    let read = database.begin_read().unwrap();
    match state {
        PartialState::Metadata => read
            .open_table(PROGRESS_TABLE)
            .unwrap()
            .get("node_id")
            .unwrap()
            .unwrap()
            .value()
            .to_vec(),
        PartialState::Data => read
            .open_table(DATA_TABLE)
            .unwrap()
            .get(b"key".as_slice())
            .unwrap()
            .unwrap()
            .value()
            .to_vec(),
        PartialState::Receipt => read
            .open_table(REQUEST_TABLE)
            .unwrap()
            .get(b"request-1".as_slice())
            .unwrap()
            .unwrap()
            .value()
            .to_vec(),
    }
}

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

fn replicated(command: &KvCommandV1) -> Vec<u8> {
    encode_replicated_kv_command(command).unwrap()
}

#[test]
fn open_rejects_partial_reserved_state_without_changing_database_bytes() {
    let dir = tempfile::tempdir().unwrap();
    for (name, partial_state) in [
        ("metadata", PartialState::Metadata),
        ("data", PartialState::Data),
        ("receipt", PartialState::Receipt),
    ] {
        let path = dir.path().join(format!("{name}.redb"));
        write_partial_state(&path, partial_state);
        let before = read_partial_bytes(&path, partial_state);

        assert!(matches!(
            RedbStateMachine::open(&path, "cluster-1", "node-1", 7, 3),
            Err(Error::PartialInitialization)
        ));

        assert_eq!(read_partial_bytes(&path, partial_state), before);
    }
}

#[test]
fn put_and_delete_atomically_materialize_data_receipts_and_progress() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        RedbStateMachine::open(dir.path().join("state.redb"), "cluster-1", "node-1", 7, 3).unwrap();

    let put = KvCommandV1::put("put-1", b"alpha".to_vec(), b"one".to_vec()).unwrap();
    let put_payload = replicated(&put);
    let put_entry = entry(1, LogHash::ZERO, put_payload.clone());
    let put_outcome = state.apply_entry(&put_entry).unwrap();
    assert_eq!(
        put_outcome.result(),
        Some(&KvCommandResultV1::Put { replaced: false })
    );
    assert_eq!(state.get(b"alpha").unwrap(), Some(b"one".to_vec()));
    assert_eq!(state.applied_index().unwrap(), 1);
    assert_eq!(state.applied_hash().unwrap(), put_entry.hash);
    let receipt = state.check_request("put-1", &put_payload).unwrap().unwrap();
    assert_eq!(receipt.original_log_index(), 1);
    assert_eq!(receipt.original_log_hash(), put_entry.hash);

    let delete = KvCommandV1::delete("delete-1", b"alpha".to_vec()).unwrap();
    let delete_entry = entry(2, put_entry.hash, replicated(&delete));
    let delete_outcome = state.apply_entry(&delete_entry).unwrap();
    assert_eq!(
        delete_outcome.result(),
        Some(&KvCommandResultV1::Delete { existed: true })
    );
    assert_eq!(state.get(b"alpha").unwrap(), None);
    assert_eq!(state.applied_index().unwrap(), 2);
}

#[test]
fn exact_retry_replays_original_result_and_conflict_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        RedbStateMachine::open(dir.path().join("state.redb"), "cluster-1", "node-1", 7, 3).unwrap();
    let first = KvCommandV1::put("request-1", b"key".to_vec(), b"first".to_vec()).unwrap();
    let payload = replicated(&first);
    let first_entry = entry(1, LogHash::ZERO, payload.clone());
    state.apply_entry(&first_entry).unwrap();

    let retry_entry = entry(2, first_entry.hash, payload);
    let retry = state.apply_entry(&retry_entry).unwrap();
    assert_eq!(
        retry.result(),
        Some(&KvCommandResultV1::Put { replaced: false })
    );
    assert_eq!(retry.applied_index(), 2);

    let conflict = KvCommandV1::put("request-1", b"key".to_vec(), b"second".to_vec()).unwrap();
    let conflict_entry = entry(3, retry_entry.hash, replicated(&conflict));
    assert!(matches!(
        state.apply_entry(&conflict_entry),
        Err(Error::RequestConflict { .. })
    ));
    assert_eq!(state.applied_index().unwrap(), 2);
    assert_eq!(state.get(b"key").unwrap(), Some(b"first".to_vec()));
}

#[test]
fn reopen_preserves_receipts_and_replay_continuity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.redb");
    let command = KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap();
    let first_entry = entry(1, LogHash::ZERO, replicated(&command));
    {
        let state = RedbStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
        state.apply_entry(&first_entry).unwrap();
    }

    let state = RedbStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
    let replay = state.apply_entry(&first_entry).unwrap();
    assert_eq!(
        replay.result(),
        Some(&KvCommandResultV1::Put { replaced: false })
    );
    let gap = entry(3, first_entry.hash, replicated(&command));
    assert!(matches!(
        state.apply_entry(&gap),
        Err(Error::InvalidEntry(_))
    ));
    assert_eq!(state.applied_index().unwrap(), 1);

    let delete = KvCommandV1::delete("delete-1", b"key".to_vec()).unwrap();
    let second_entry = entry(2, first_entry.hash, replicated(&delete));
    state.apply_entry(&second_entry).unwrap();
    drop(state);
    let reopened = RedbStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
    assert_eq!(reopened.applied_index().unwrap(), 2);
    assert_eq!(reopened.applied_hash().unwrap(), second_entry.hash);
    assert_eq!(reopened.get(b"key").unwrap(), None);
}

#[test]
fn malformed_or_wrong_profile_commands_fail_closed_without_advancing() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        RedbStateMachine::open(dir.path().join("state.redb"), "cluster-1", "node-1", 7, 3).unwrap();
    let command = KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap();
    let sqlite_profile = ReplicatedCommandEnvelope::new(
        ExecutionProfile::Sqlite,
        1,
        command.request_id(),
        command.encode().unwrap(),
    )
    .unwrap()
    .encode()
    .unwrap();
    let unknown_version = ReplicatedCommandEnvelope::new(
        ExecutionProfile::Kv,
        2,
        command.request_id(),
        command.encode().unwrap(),
    )
    .unwrap()
    .encode()
    .unwrap();
    let mismatched_request = ReplicatedCommandEnvelope::new(
        ExecutionProfile::Kv,
        1,
        "different-request",
        command.encode().unwrap(),
    )
    .unwrap()
    .encode()
    .unwrap();
    let mut trailing = replicated(&command);
    trailing.push(0);

    for payload in [
        command.encode().unwrap(),
        sqlite_profile,
        unknown_version,
        mismatched_request,
        trailing,
    ] {
        let rejected = entry(1, LogHash::ZERO, payload);
        assert!(matches!(
            state.apply_entry(&rejected),
            Err(Error::InvalidCommand(_) | Error::Codec(_))
        ));
        assert_eq!(state.applied_index().unwrap(), 0);
        assert_eq!(state.get(b"key").unwrap(), None);
    }
}

#[test]
fn snapshot_restores_exact_point_with_receipts_and_accepts_the_next_entry() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.redb");
    let restored_path = dir.path().join("restored.redb");
    let source = RedbStateMachine::open(&source_path, "cluster-1", "node-1", 7, 3).unwrap();
    let first_command = KvCommandV1::put("put-1", b"alpha".to_vec(), b"one".to_vec()).unwrap();
    let first_payload = replicated(&first_command);
    let first_entry = entry(1, LogHash::ZERO, first_payload.clone());
    source.apply_entry(&first_entry).unwrap();

    let snapshot = source.create_snapshot(1).unwrap();
    assert_eq!(snapshot.cluster_id(), "cluster-1");
    assert_eq!(snapshot.created_by(), "node-1");
    assert_eq!(snapshot.epoch(), 7);
    assert_eq!(snapshot.config_id(), 3);
    assert_eq!(snapshot.applied_index(), 1);
    assert_eq!(snapshot.applied_hash(), first_entry.hash);
    assert_eq!(
        snapshot.materializer_fingerprint(),
        kv_materializer_fingerprint()
    );
    assert_ne!(snapshot.digest(), LogHash::ZERO);
    assert!(!snapshot.db_bytes().is_empty());

    let second_command = KvCommandV1::put("put-2", b"beta".to_vec(), b"two".to_vec()).unwrap();
    let second_payload = replicated(&second_command);
    let second_entry = entry(2, first_entry.hash, second_payload.clone());
    source.apply_entry(&second_entry).unwrap();

    restore_snapshot_file(&restored_path, &snapshot, "node-2").unwrap();
    let restored = RedbStateMachine::open(&restored_path, "cluster-1", "node-2", 7, 3).unwrap();
    assert_eq!(restored.applied_index().unwrap(), 1);
    assert_eq!(restored.applied_hash().unwrap(), first_entry.hash);
    assert_eq!(restored.get(b"alpha").unwrap(), Some(b"one".to_vec()));
    assert_eq!(restored.get(b"beta").unwrap(), None);
    assert!(restored
        .check_request("put-1", &first_payload)
        .unwrap()
        .is_some());
    assert!(restored
        .check_request("put-2", &second_payload)
        .unwrap()
        .is_none());

    restored.apply_entry(&second_entry).unwrap();
    assert_eq!(restored.get(b"beta").unwrap(), Some(b"two".to_vec()));
    assert_eq!(restored.applied_index().unwrap(), 2);
}

#[test]
fn snapshot_rejects_a_wrong_target_without_changing_source_state() {
    let dir = tempfile::tempdir().unwrap();
    let source =
        RedbStateMachine::open(dir.path().join("source.redb"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let command = KvCommandV1::put("put-1", b"alpha".to_vec(), b"one".to_vec()).unwrap();
    let payload = replicated(&command);
    let first_entry = entry(1, LogHash::ZERO, payload.clone());
    source.apply_entry(&first_entry).unwrap();

    assert!(matches!(
        source.create_snapshot(0),
        Err(Error::InvalidSnapshot(_))
    ));
    assert_eq!(source.applied_index().unwrap(), 1);
    assert_eq!(source.applied_hash().unwrap(), first_entry.hash);
    assert_eq!(source.get(b"alpha").unwrap(), Some(b"one".to_vec()));
    assert!(source.check_request("put-1", &payload).unwrap().is_some());
}

#[test]
fn restore_preserves_an_existing_target() {
    let dir = tempfile::tempdir().unwrap();
    let source =
        RedbStateMachine::open(dir.path().join("source.redb"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let snapshot = source.create_snapshot(0).unwrap();
    let target = dir.path().join("existing.redb");
    std::fs::write(&target, b"existing bytes").unwrap();

    assert!(matches!(
        restore_snapshot_file(&target, &snapshot, "node-2"),
        Err(Error::InvalidSnapshot(_))
    ));
    assert_eq!(std::fs::read(&target).unwrap(), b"existing bytes");
}

#[test]
fn lifecycle_entries_advance_progress_without_mutating_kv_data() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        RedbStateMachine::open(dir.path().join("state.redb"), "cluster-1", "node-1", 7, 3).unwrap();
    let put = KvCommandV1::put("put-1", b"key".to_vec(), b"value".to_vec()).unwrap();
    let first = entry(1, LogHash::ZERO, replicated(&put));
    state.apply_entry(&first).unwrap();

    let config_change = ConfigChange::bound_stop(
        "cluster-1",
        3,
        LogHash::from_bytes([7; 32]),
        4,
        vec!["node-1".into(), "node-2".into(), "node-3".into()],
    )
    .unwrap()
    .to_stored_command();
    let mut previous = first.hash;
    for (index, entry_type, payload) in [
        (2, config_change.entry_type, config_change.payload),
        (3, EntryType::SnapshotBarrier, b"barrier".to_vec()),
        (4, EntryType::SnapshotPublished, b"published".to_vec()),
    ] {
        let hash =
            LogEntry::calculate_hash("cluster-1", index, 7, 3, entry_type, previous, &payload);
        state
            .apply_entry(&LogEntry {
                cluster_id: "cluster-1".into(),
                epoch: 7,
                config_id: 3,
                index,
                entry_type,
                payload,
                prev_hash: previous,
                hash,
            })
            .unwrap();
        previous = hash;
    }

    assert_eq!(state.applied_index().unwrap(), 4);
    assert_eq!(state.applied_hash().unwrap(), previous);
    assert_eq!(state.get(b"key").unwrap(), Some(b"value".to_vec()));
}
