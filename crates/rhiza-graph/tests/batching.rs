use rhiza_core::{EntryType, LogEntry, LogHash};
use rhiza_graph::{
    encode_replicated_graph_batch, encode_replicated_graph_command, Error, GraphCommandResultV1,
    GraphCommandV1, GraphValueV1, LadybugStateMachine, MAX_GRAPH_BATCH_MEMBERS,
};

#[test]
fn ordered_batch_atomically_applies_members_and_distinct_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let first =
        GraphCommandV1::put_document("first", "document", GraphValueV1::String("one".into()))
            .unwrap();
    let second =
        GraphCommandV1::put_document("second", "document", GraphValueV1::String("two".into()))
            .unwrap();
    let first_payload = encode_replicated_graph_command(&first).unwrap();
    let second_payload = encode_replicated_graph_command(&second).unwrap();
    let entry = entry(
        1,
        LogHash::ZERO,
        encode_replicated_graph_batch(&[first, second]).unwrap(),
    );

    let outcome = state.apply_entry(&entry).unwrap();

    assert_eq!(outcome.applied_index(), 1);
    assert_eq!(outcome.applied_hash(), entry.hash);
    assert_eq!(outcome.result(), None);
    assert_eq!(
        state.get_document("document").unwrap(),
        Some(GraphValueV1::String("two".into()))
    );
    let first = state
        .check_request("first", &first_payload)
        .unwrap()
        .unwrap();
    let second = state
        .check_request("second", &second_payload)
        .unwrap()
        .unwrap();
    assert_eq!(first.original_log_index(), 1);
    assert_eq!(second.original_log_index(), 1);
    assert_eq!(first.original_log_hash(), entry.hash);
    assert_eq!(second.original_log_hash(), entry.hash);
    assert_eq!(
        first.result(),
        &GraphCommandResultV1::PutDocument { created: true }
    );
    assert_eq!(
        second.result(),
        &GraphCommandResultV1::PutDocument { created: false }
    );
    assert_eq!(state.apply_entry(&entry).unwrap(), outcome);
}

#[test]
fn batch_tracks_document_existence_in_command_order() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let put = GraphCommandV1::put_document("put", "document", GraphValueV1::String("one".into()))
        .unwrap();
    let delete = GraphCommandV1::delete_document("delete", "document").unwrap();
    let recreate =
        GraphCommandV1::put_document("recreate", "document", GraphValueV1::U64(7)).unwrap();
    let payloads =
        [&put, &delete, &recreate].map(|command| encode_replicated_graph_command(command).unwrap());
    let entry = entry(
        1,
        LogHash::ZERO,
        encode_replicated_graph_batch(&[put, delete, recreate]).unwrap(),
    );

    state.apply_entry(&entry).unwrap();

    assert_eq!(
        state
            .check_request("put", &payloads[0])
            .unwrap()
            .unwrap()
            .result(),
        &GraphCommandResultV1::PutDocument { created: true }
    );
    assert_eq!(
        state
            .check_request("delete", &payloads[1])
            .unwrap()
            .unwrap()
            .result(),
        &GraphCommandResultV1::DeleteDocument { existed: true }
    );
    assert_eq!(
        state
            .check_request("recreate", &payloads[2])
            .unwrap()
            .unwrap()
            .result(),
        &GraphCommandResultV1::PutDocument { created: true }
    );
    assert_eq!(
        state.get_document("document").unwrap(),
        Some(GraphValueV1::U64(7))
    );
}

#[test]
fn request_conflict_rolls_back_every_member_and_the_applied_tip() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let original =
        GraphCommandV1::put_document("existing", "stable", GraphValueV1::String("one".into()))
            .unwrap();
    let original_entry = entry(
        1,
        LogHash::ZERO,
        encode_replicated_graph_command(&original).unwrap(),
    );
    state.apply_entry(&original_entry).unwrap();
    let new = GraphCommandV1::put_document("new", "new", GraphValueV1::U64(7)).unwrap();
    let new_payload = encode_replicated_graph_command(&new).unwrap();
    let conflict = GraphCommandV1::put_document(
        "existing",
        "stable",
        GraphValueV1::String("different".into()),
    )
    .unwrap();
    let conflicting_entry = entry(
        2,
        original_entry.hash,
        encode_replicated_graph_batch(&[new, conflict]).unwrap(),
    );

    assert!(matches!(
        state.apply_entry(&conflicting_entry),
        Err(Error::RequestConflict { request_id, .. }) if request_id == "existing"
    ));

    assert_eq!(state.applied_index().unwrap(), 1);
    assert_eq!(state.applied_hash().unwrap(), original_entry.hash);
    assert_eq!(state.get_document("new").unwrap(), None);
    assert_eq!(
        state.get_document("stable").unwrap(),
        Some(GraphValueV1::String("one".into()))
    );
    assert_eq!(state.check_request("new", &new_payload).unwrap(), None);
}

#[test]
fn malformed_duplicate_and_oversized_batches_are_rejected_without_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let command = GraphCommandV1::delete_document("same", "document").unwrap();
    assert!(matches!(
        encode_replicated_graph_batch(&[command.clone(), command.clone()]),
        Err(Error::InvalidCommand(_))
    ));
    assert!(matches!(
        encode_replicated_graph_batch(&[]),
        Err(Error::InvalidCommand(_))
    ));
    let oversized = (0..=MAX_GRAPH_BATCH_MEMBERS)
        .map(|index| {
            GraphCommandV1::delete_document(format!("request-{index}"), format!("document-{index}"))
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        encode_replicated_graph_batch(&oversized),
        Err(Error::InvalidCommand(_))
    ));
    let mut malformed = encode_replicated_graph_batch(&[
        GraphCommandV1::put_document("new", "new", GraphValueV1::U64(9)).unwrap(),
        GraphCommandV1::delete_document("delete", "document").unwrap(),
    ])
    .unwrap();
    let member_start = malformed
        .windows(6)
        .rposition(|window| window == b"RHGC\0\x01")
        .unwrap();
    let operation_tag = member_start + 6 + 4 + "delete".len();
    malformed[operation_tag] = u8::MAX;
    let malformed_entry = entry(1, LogHash::ZERO, malformed);

    assert!(state.apply_entry(&malformed_entry).is_err());
    assert_eq!(state.applied_index().unwrap(), 0);
    assert_eq!(state.applied_hash().unwrap(), LogHash::ZERO);
    assert_eq!(state.get_document("document").unwrap(), None);
    assert_eq!(state.get_document("new").unwrap(), None);
}

#[test]
#[ignore = "manual release-mode throughput benchmark"]
fn apply_throughput() {
    let batch_size = std::env::var("RHIZA_GRAPH_BENCH_BATCH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(8);
    let operations = std::env::var("RHIZA_GRAPH_BENCH_OPS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1_024);
    assert!((1..=MAX_GRAPH_BATCH_MEMBERS).contains(&batch_size));
    assert!(operations >= batch_size && operations.is_multiple_of(batch_size));

    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let mut index = 0;
    let mut hash = LogHash::ZERO;
    let run = |start: usize, count: usize, index: &mut u64, hash: &mut LogHash| {
        for offset in (start..start + count).step_by(batch_size) {
            let commands = (offset..offset + batch_size)
                .map(|operation| {
                    GraphCommandV1::put_document(
                        format!("request-{operation}"),
                        format!("document-{}", operation % 256),
                        GraphValueV1::String("x".repeat(128)),
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();
            let payload = if batch_size == 1 {
                encode_replicated_graph_command(&commands[0]).unwrap()
            } else {
                encode_replicated_graph_batch(&commands).unwrap()
            };
            *index += 1;
            let next = entry(*index, *hash, payload);
            state.apply_entry(&next).unwrap();
            *hash = next.hash;
        }
    };

    run(0, batch_size * 2, &mut index, &mut hash);
    let started = std::time::Instant::now();
    run(batch_size * 2, operations, &mut index, &mut hash);
    let elapsed = started.elapsed();
    println!(
        "batch_size={batch_size} operations={operations} elapsed_ms={:.3} operations_per_second={:.3}",
        elapsed.as_secs_f64() * 1_000.0,
        operations as f64 / elapsed.as_secs_f64()
    );
}

fn state(root: &std::path::Path) -> LadybugStateMachine {
    LadybugStateMachine::open(root.join("graph.lbug"), "cluster-1", "node-1", 7, 3).unwrap()
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
