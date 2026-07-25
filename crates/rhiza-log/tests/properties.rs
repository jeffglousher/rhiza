use std::panic::{catch_unwind, AssertUnwindSafe};

use proptest::prelude::*;
use rhiza_core::{
    ConfigurationState, EntryType, LogAnchor, LogEntry, LogHash, RecoveryAnchor, SnapshotIdentity,
};
use rhiza_log::{
    decode_segment_for_cluster, encode_open_segment, encode_segment, FileLogStore, IndexRange,
    LogStore, QLOG_HEADER_LEN,
};

proptest! {
    #[test]
    fn generated_contiguous_entries_round_trip(
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..128), 1..16)
    ) {
        let entries = chain(&payloads);

        let decoded = decode_segment_for_cluster(&encode_segment(&entries), "cluster-a").unwrap();

        prop_assert_eq!(decoded, entries);
    }

    #[test]
    fn malformed_and_torn_segments_never_panic(
        malformed in prop::collection::vec(any::<u8>(), 0..2048),
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..128), 1..8),
        cut_seed in any::<usize>(),
    ) {
        let malformed_result = catch_unwind(AssertUnwindSafe(|| {
            let _ = decode_segment_for_cluster(&malformed, "cluster-a");
        }));
        prop_assert!(malformed_result.is_ok());

        let encoded = encode_segment(&chain(&payloads));
        let cut = cut_seed % (encoded.len() + 1);
        let torn_result = catch_unwind(AssertUnwindSafe(|| {
            let _ = decode_segment_for_cluster(&encoded[..cut], "cluster-a");
        }));
        prop_assert!(torn_result.is_ok());
    }

    #[test]
    fn torn_open_segment_recovers_exact_complete_prefix_and_can_continue(
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..128), 1..16),
        cut_seed in any::<usize>(),
    ) {
        let entries = chain(&payloads);
        let encoded = encode_open_segment(&entries);
        let cut = QLOG_HEADER_LEN + cut_seed % (encoded.len() - QLOG_HEADER_LEN + 1);
        let recovered_count = (0..=entries.len())
            .rev()
            .find(|count| encode_open_segment(&entries[..*count]).len() <= cut)
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("00000000000000000001-open.qlog");
        std::fs::write(&path, &encoded[..cut]).unwrap();

        let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();

        prop_assert_eq!(
            store.read_range(IndexRange::new(1, entries.len() as u64).unwrap()).unwrap(),
            entries[..recovered_count].to_vec(),
        );
        prop_assert_eq!(
            std::fs::read(&path).unwrap(),
            if recovered_count == 0 {
                encoded[..QLOG_HEADER_LEN].to_vec()
            } else {
                encode_open_segment(&entries[..recovered_count])
            },
        );
        store.append_batch_buffered(&entries[recovered_count..]).unwrap();
        prop_assert_eq!(store.sync().unwrap(), entries.last().map(|entry| entry.index));
        drop(store);

        let reopened = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
        prop_assert_eq!(
            reopened.read_range(IndexRange::new(1, entries.len() as u64).unwrap()).unwrap(),
            entries,
        );
    }

    #[test]
    fn torn_open_segment_after_closed_prefix_recovers_exact_complete_suffix(
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..128), 2..16),
        split_seed in any::<usize>(),
        cut_seed in any::<usize>(),
    ) {
        let entries = chain(&payloads);
        let split = 1 + split_seed % (entries.len() - 1);
        let closed = encode_segment(&entries[..split]);
        let open = encode_open_segment(&entries[split..]);
        let cut = QLOG_HEADER_LEN + cut_seed % (open.len() - QLOG_HEADER_LEN + 1);
        let recovered_suffix = (0..=entries.len() - split)
            .rev()
            .find(|count| encode_open_segment(&entries[split..split + *count]).len() <= cut)
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let closed_path = dir.path().join(format!(
            "{:020}-{:020}.qlog",
            1,
            split,
        ));
        let open_path = dir.path().join(format!("{:020}-open.qlog", split + 1));
        std::fs::write(closed_path, closed).unwrap();
        std::fs::write(&open_path, &open[..cut]).unwrap();

        let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();

        let recovered = split + recovered_suffix;
        prop_assert_eq!(store.last_index().unwrap(), Some(recovered as u64));
        prop_assert_eq!(
            store.read_range(IndexRange::new(1, entries.len() as u64).unwrap()).unwrap(),
            entries[..recovered].to_vec(),
        );
        store.append_batch_buffered(&entries[recovered..]).unwrap();
        prop_assert_eq!(store.sync().unwrap(), entries.last().map(|entry| entry.index));
        drop(store);

        let reopened = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
        prop_assert_eq!(
            reopened.read_range(IndexRange::new(1, entries.len() as u64).unwrap()).unwrap(),
            entries,
        );
    }

    #[test]
    fn compaction_retains_exact_generated_suffix(
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 1..16),
        target_seed in any::<usize>(),
    ) {
        let entries = chain(&payloads);
        let target = target_seed % entries.len();
        let dir = tempfile::tempdir().unwrap();
        let store = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
        store.append_batch(&entries).unwrap();
        let entry = &entries[target];
        let anchor = RecoveryAnchor::new(
            "cluster-a",
            1,
            ConfigurationState::active(1, LogHash::ZERO),
            1,
            LogAnchor::new(entry.index, entry.hash),
            SnapshotIdentity::new(
                format!("snapshot-{:015}", entry.index),
                LogHash::digest(&[b"snapshot", &entry.index.to_be_bytes()]),
                payloads.iter().map(Vec::len).sum::<usize>() as u64 + 1,
                LogHash::from_bytes([6; 32]),
            ),
        );

        store.compact_prefix(&anchor).unwrap();
        let retained = store
            .read_range(IndexRange::new(1, entries.len() as u64).unwrap())
            .unwrap();
        prop_assert_eq!(retained, entries[target + 1..].to_vec());
        prop_assert_eq!(store.last_index().unwrap(), entries.last().map(|entry| entry.index));
        drop(store);

        let reopened = FileLogStore::open(dir.path(), "cluster-a", 1, 1).unwrap();
        prop_assert_eq!(reopened.logical_state().unwrap().anchor, Some(anchor));
        prop_assert_eq!(
            reopened.read_range(IndexRange::new(1, entries.len() as u64).unwrap()).unwrap(),
            entries[target + 1..].to_vec(),
        );
    }
}

fn chain(payloads: &[Vec<u8>]) -> Vec<LogEntry> {
    let mut entries = Vec::with_capacity(payloads.len());
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
            payload: payload.clone(),
            prev_hash,
            hash,
        });
        prev_hash = hash;
    }
    entries
}
