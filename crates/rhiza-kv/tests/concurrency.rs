use std::sync::Arc;
use std::thread;

use tempfile::tempdir;

use rhiza_core::{EntryType, LogEntry, LogHash, LogIndex};
use rhiza_kv::{encode_replicated_kv_command, KvCommandV1, RedbStateMachine};

fn replicated(command: &KvCommandV1) -> Vec<u8> {
    encode_replicated_kv_command(command).unwrap()
}

fn entry(index: LogIndex, prev_hash: LogHash, payload: Vec<u8>) -> LogEntry {
    let hash = LogEntry::calculate_hash(
        "cluster-1",
        index,
        1,
        1,
        EntryType::Command,
        prev_hash,
        &payload,
    );
    LogEntry {
        cluster_id: "cluster-1".into(),
        epoch: 1,
        config_id: 1,
        index,
        entry_type: EntryType::Command,
        payload,
        prev_hash,
        hash,
    }
}

/// Apply entries sequentially and return the final entry hash for chaining.
fn apply_entries(sm: &RedbStateMachine, entries: Vec<(LogIndex, Vec<u8>)>) -> LogHash {
    let mut prev_hash = LogHash::ZERO;
    for (index, payload) in entries {
        let e = entry(index, prev_hash, payload);
        sm.apply_entry(&e).unwrap();
        prev_hash = e.hash;
    }
    prev_hash
}

/// Build a sequentially-hashed batch of entries starting from `start_index` (1-based) and `prev_hash`.
fn build_entries(start_index: u64, prev_hash: LogHash, count: u64) -> Vec<(LogIndex, Vec<u8>)> {
    let mut entries = vec![];
    let mut prev = prev_hash;
    for i in start_index..start_index + count {
        let key = format!("key-{i:04}");
        let value = format!("value-{i:04}");
        let command =
            KvCommandV1::put(format!("req-{i}"), key.into_bytes(), value.into_bytes()).unwrap();
        let payload = replicated(&command);
        let e = entry(i, prev, payload);
        prev = e.hash;
        entries.push((i, e.payload.clone()));
    }
    entries
}

#[test]
fn concurrent_reads_after_writes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.ldb");

    let sm = Arc::new(
        RedbStateMachine::open(&path, "cluster-1", "node-1", 1, 1).unwrap(),
    );

    // Write some initial data sequentially (start at index 1 since initial applied_index is 0)
    let entries = build_entries(1, LogHash::ZERO, 100);
    apply_entries(&sm, entries);

    // Concurrent reads
    let mut handles = vec![];
    for _ in 0..4 {
        let sm_clone = Arc::clone(&sm);
        handles.push(thread::spawn(move || {
            for i in 1..=100 {
                let key = format!("key-{i:04}");
                let result = sm_clone.get(key.as_bytes()).unwrap();
                assert!(result.is_some(), "key-{i:04} should exist");
                let value = result.unwrap();
                assert!(
                    value.starts_with(b"value-"),
                    "unexpected value for key-{i:04}"
                );
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn concurrent_scan_during_writes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.ldb");

    let sm = Arc::new(
        RedbStateMachine::open(&path, "cluster-1", "node-1", 1, 1).unwrap(),
    );

    // Write initial data (start at index 1)
    let entries = build_entries(1, LogHash::ZERO, 50);
    let last_hash = apply_entries(&sm, entries);

    // Writer thread
    let sm_clone = Arc::clone(&sm);
    let writer_handle = thread::spawn(move || {
        let mut prev_hash = last_hash;
        for i in 51..100u64 {
            let key = format!("key-{i:04}");
            let value = format!("value-{i:04}");
            let command =
                KvCommandV1::put(format!("req-{i}"), key.into_bytes(), value.into_bytes())
                    .unwrap();
            let payload = replicated(&command);
            let e = entry(i, prev_hash, payload);
            sm_clone.apply_entry(&e).unwrap();
            prev_hash = e.hash;
        }
    });

    // Reader threads doing scans
    let mut handles = vec![];
    for _ in 0..2 {
        let sm_clone = Arc::clone(&sm);
        handles.push(thread::spawn(move || {
            for _ in 0..10 {
                let result = sm_clone.scan_range(b"key-0000", Some(b"key-0100"), 100, None);
                let scan_result = result.unwrap();
                let count = scan_result.rows().len();
                assert!(count <= 100, "scan should not exceed total key count, got {count}");
            }
        }));
    }

    writer_handle.join().unwrap();
    for handle in handles {
        handle.join().unwrap();
    }
}
