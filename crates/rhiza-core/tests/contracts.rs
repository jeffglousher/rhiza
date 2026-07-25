use rhiza_core::{
    canonical_membership_digest, ConfigChange, ConfigurationState, EntryId, EntryType, LogAnchor,
    LogEntry, LogHash, RecoveryAnchor, SnapshotIdentity, SnapshotManifest, StoredCommand,
    SuccessorDescriptor, RECOVERY_ANCHOR_FORMAT_VERSION,
};

fn config_entry(index: u64, config_id: u64, prev_hash: LogHash, change: ConfigChange) -> LogEntry {
    let command = change.to_stored_command();
    let hash = LogEntry::calculate_hash(
        "cluster-a",
        index,
        7,
        config_id,
        command.entry_type,
        prev_hash,
        &command.payload,
    );
    LogEntry {
        cluster_id: "cluster-a".into(),
        epoch: 7,
        config_id,
        index,
        entry_type: command.entry_type,
        payload: command.payload,
        prev_hash,
        hash,
    }
}

#[test]
fn entry_id_records_epoch_and_index() {
    let id = EntryId {
        epoch: 7,
        index: 42,
    };

    assert_eq!(id.epoch, 7);
    assert_eq!(id.index, 42);
}

#[test]
fn log_entry_records_consensus_order_and_hash_chain() {
    let entry = LogEntry {
        cluster_id: "cluster-a".into(),
        epoch: 7,
        config_id: 3,
        index: 42,
        entry_type: EntryType::Command,
        payload: b"insert-user".to_vec(),
        prev_hash: LogHash::from_bytes([1; 32]),
        hash: LogHash::from_bytes([2; 32]),
    };

    assert_eq!(entry.cluster_id, "cluster-a");
    assert_eq!(entry.index, 42);
    assert_eq!(entry.epoch, 7);
    assert_eq!(entry.config_id, 3);
    assert_eq!(entry.prev_hash, LogHash::from_bytes([1; 32]));
    assert_eq!(entry.hash, LogHash::from_bytes([2; 32]));
}

#[test]
fn log_entry_hash_changes_when_any_identity_field_changes() {
    let base = LogEntry::calculate_hash(
        "cluster-a",
        42,
        7,
        3,
        EntryType::Command,
        LogHash::from_bytes([1; 32]),
        b"insert-user",
    );

    assert_ne!(
        base,
        LogEntry::calculate_hash(
            "cluster-a",
            43,
            7,
            3,
            EntryType::Command,
            LogHash::from_bytes([1; 32]),
            b"insert-user",
        )
    );
    assert_ne!(
        base,
        LogEntry::calculate_hash(
            "cluster-a",
            42,
            8,
            3,
            EntryType::Command,
            LogHash::from_bytes([1; 32]),
            b"insert-user",
        )
    );
    assert_ne!(
        base,
        LogEntry::calculate_hash(
            "cluster-a",
            42,
            7,
            4,
            EntryType::Command,
            LogHash::from_bytes([1; 32]),
            b"insert-user",
        )
    );
    assert_ne!(
        base,
        LogEntry::calculate_hash(
            "cluster-a",
            42,
            7,
            3,
            EntryType::Noop,
            LogHash::from_bytes([1; 32]),
            b"insert-user",
        )
    );
    assert_ne!(
        base,
        LogEntry::calculate_hash(
            "cluster-a",
            42,
            7,
            3,
            EntryType::Command,
            LogHash::from_bytes([2; 32]),
            b"insert-user",
        )
    );
    assert_ne!(
        base,
        LogEntry::calculate_hash(
            "cluster-a",
            42,
            7,
            3,
            EntryType::Command,
            LogHash::from_bytes([1; 32]),
            b"insert-admin",
        )
    );
    assert_ne!(
        base,
        LogEntry::calculate_hash(
            "cluster-b",
            42,
            7,
            3,
            EntryType::Command,
            LogHash::from_bytes([1; 32]),
            b"insert-user",
        )
    );
}

#[test]
fn log_entry_hash_uses_canonical_cluster_bound_encoding() {
    assert_eq!(
        LogEntry::calculate_hash(
            "cluster-a",
            42,
            7,
            3,
            EntryType::Command,
            LogHash::from_bytes([1; 32]),
            b"insert-user",
        )
        .to_hex(),
        "33a2d2a2109177da04e25ee549a117d7dcce6a0e8bdd31f9ff6790aff10cb250"
    );
}

#[test]
fn stored_command_hash_binds_entry_type_and_payload() {
    let command = StoredCommand::new(EntryType::Command, b"select-1".to_vec());
    let noop = StoredCommand::new(EntryType::Noop, b"select-1".to_vec());
    let other_payload = StoredCommand::new(EntryType::Command, b"select-2".to_vec());

    assert_ne!(command.hash(), noop.hash());
    assert_ne!(command.hash(), other_payload.hash());
}

#[test]
fn snapshot_manifest_id_uses_zero_padded_snapshot_index() {
    let manifest = SnapshotManifest::new(
        "cluster-a",
        ConfigurationState::active(3, LogHash::ZERO),
        7,
        104_200,
        LogHash::from_bytes([9; 32]),
        14,
        "node-2",
        LogHash::from_bytes([6; 32]),
    );

    assert_eq!(manifest.snapshot_id(), "snapshot-000000000104200");
    assert_eq!(manifest.snapshot_index(), 104_200);
}

#[test]
fn snapshot_manifest_round_trips_all_snapshot_identity_as_json() {
    let manifest = SnapshotManifest::new(
        "cluster-a",
        ConfigurationState::active(3, LogHash::ZERO),
        7,
        104_200,
        LogHash::from_bytes([9; 32]),
        14,
        "node-2",
        LogHash::from_bytes([6; 32]),
    );

    let json = serde_json::to_value(&manifest).unwrap();
    assert_eq!(json["cluster_id"], "cluster-a");
    assert_eq!(json["epoch"], 7);
    assert_eq!(json["config_id"], 3);
    assert_eq!(json["schema_version"], 14);
    assert_eq!(json["created_by"], "node-2");
    assert_eq!(json["index"], 104_200);
    assert_eq!(json["applied_hash"], serde_json::json!(vec![9; 32]));
    assert_eq!(json["snapshot_id"], "snapshot-000000000104200");

    let decoded: SnapshotManifest = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, manifest);
    assert_eq!(decoded.cluster_id(), "cluster-a");
    assert_eq!(decoded.epoch(), 7);
    assert_eq!(decoded.config_id(), 3);
    assert_eq!(decoded.schema_version(), 14);
    assert_eq!(decoded.created_by(), "node-2");
    assert_eq!(decoded.index(), 104_200);
    assert_eq!(decoded.applied_hash(), LogHash::from_bytes([9; 32]));
    assert_eq!(decoded.snapshot_id(), "snapshot-000000000104200");
}

#[test]
fn snapshot_manifest_round_trips_a_known_executor_fingerprint() {
    let executor_fingerprint = LogHash::from_bytes([6; 32]);
    let manifest = SnapshotManifest::new(
        "cluster-a",
        ConfigurationState::active(3, LogHash::ZERO),
        7,
        104_200,
        LogHash::from_bytes([9; 32]),
        14,
        "node-2",
        executor_fingerprint,
    );

    let json = serde_json::to_value(&manifest).unwrap();
    assert_eq!(json["executor_fingerprint"], serde_json::json!(vec![6; 32]));

    let decoded: SnapshotManifest = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, manifest);
    assert_eq!(decoded.executor_fingerprint(), executor_fingerprint);
}

#[test]
fn snapshot_manifest_rejects_missing_canonical_fields() {
    let incomplete = serde_json::json!({
        "snapshot_id": "snapshot-000000000104200",
        "cluster_id": "cluster-a",
        "config_id": 3,
        "configuration_state": {
            "phase": "active",
            "config_id": 3,
            "digest": vec![0; 32],
        },
        "epoch": 7,
        "index": 104_200,
        "applied_hash": vec![9; 32],
        "schema_version": 14,
        "created_by": "node-2",
    });

    assert!(serde_json::from_value::<SnapshotManifest>(incomplete).is_err());
}

#[test]
fn recovery_anchor_round_trips_canonical_log_and_snapshot_identity() {
    let executor_fingerprint = LogHash::from_bytes([6; 32]);
    let anchor = RecoveryAnchor::new(
        "cluster-a",
        7,
        ConfigurationState::active(3, LogHash::ZERO),
        4,
        LogAnchor::new(104_200, LogHash::from_bytes([8; 32])),
        SnapshotIdentity::new(
            "snapshot-000000000104200",
            LogHash::from_bytes([9; 32]),
            8192,
            executor_fingerprint,
        ),
    );

    let json = serde_json::to_value(&anchor).unwrap();
    assert_eq!(json["format_version"], RECOVERY_ANCHOR_FORMAT_VERSION);
    assert_eq!(json["cluster_id"], "cluster-a");
    assert_eq!(json["epoch"], 7);
    assert_eq!(json["config_id"], 3);
    assert_eq!(json["recovery_generation"], 4);
    assert_eq!(json["compacted"]["index"], 104_200);
    assert_eq!(json["compacted"]["hash"], serde_json::json!(vec![8; 32]));
    assert_eq!(json["snapshot"]["snapshot_id"], "snapshot-000000000104200");
    assert_eq!(json["snapshot"]["digest"], serde_json::json!(vec![9; 32]));
    assert_eq!(json["snapshot"]["size_bytes"], 8192);
    assert_eq!(
        json["snapshot"]["executor_fingerprint"],
        serde_json::json!(vec![6; 32])
    );

    let decoded: RecoveryAnchor = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, anchor);
    assert_eq!(decoded.format_version(), RECOVERY_ANCHOR_FORMAT_VERSION);
    assert_eq!(decoded.cluster_id(), "cluster-a");
    assert_eq!(decoded.epoch(), 7);
    assert_eq!(decoded.config_id(), 3);
    assert_eq!(decoded.recovery_generation(), 4);
    assert_eq!(decoded.compacted().index(), 104_200);
    assert_eq!(decoded.compacted().hash(), LogHash::from_bytes([8; 32]));
    assert_eq!(decoded.snapshot().snapshot_id(), "snapshot-000000000104200");
    assert_eq!(decoded.snapshot().digest(), LogHash::from_bytes([9; 32]));
    assert_eq!(decoded.snapshot().size_bytes(), 8192);
    assert_eq!(decoded.executor_fingerprint(), executor_fingerprint);
}

#[test]
fn recovery_anchor_rejects_missing_canonical_fields_and_unsupported_format() {
    let incomplete = serde_json::json!({
        "format_version": RECOVERY_ANCHOR_FORMAT_VERSION,
        "cluster_id": "cluster-a",
        "epoch": 7,
        "config_id": 3,
        "configuration_state": {
            "phase": "active",
            "config_id": 3,
            "digest": vec![0; 32],
        },
        "recovery_generation": 4,
        "compacted": {
            "index": 104_200,
            "hash": vec![8; 32],
        },
        "snapshot": {
            "snapshot_id": "snapshot-000000000104200",
            "digest": vec![9; 32],
            "size_bytes": 8192,
        },
    });

    assert!(serde_json::from_value::<RecoveryAnchor>(incomplete.clone()).is_err());

    let mut unsupported = incomplete;
    unsupported["snapshot"]["executor_fingerprint"] = serde_json::json!(vec![6; 32]);
    unsupported["format_version"] = 1.into();
    assert!(serde_json::from_value::<RecoveryAnchor>(unsupported).is_err());
}

#[test]
fn stop_round_trip_binds_cluster_predecessor_and_exact_successor() {
    let predecessor = LogHash::from_bytes([3; 32]);
    let stop = ConfigChange::stop(
        "cluster-a",
        4,
        predecessor,
        5,
        vec!["r3".into(), "r1".into(), "r2".into()],
    )
    .unwrap();
    let command = stop.to_stored_command();
    let decoded = ConfigChange::recognize(&command).unwrap();
    assert_eq!(decoded, stop);

    let descriptor = decoded.successor();
    assert_eq!(descriptor.cluster_id(), "cluster-a");
    assert_eq!(descriptor.predecessor_config_id(), 4);
    assert_eq!(descriptor.predecessor_config_digest(), predecessor);
    assert_eq!(descriptor.config_id(), 5);
    assert_eq!(descriptor.members(), ["r1", "r2", "r3"]);
    assert_eq!(
        descriptor.digest(),
        canonical_membership_digest(descriptor.members()).unwrap()
    );
}

#[test]
fn stop_binary_rejects_noncanonical_member_order() {
    let stop = ConfigChange::stop(
        "cluster-a",
        4,
        LogHash::ZERO,
        5,
        vec!["r1".into(), "r2".into(), "r3".into()],
    )
    .unwrap();
    let mut command = stop.to_stored_command();
    let member_start = 5 + 2 + "cluster-a".len() + 8 + 32 + 8 + 32 + 1;
    let (_, members) = command.payload.split_at_mut(member_start);
    let (first, rest) = members.split_at_mut(4);
    first.swap_with_slice(&mut rest[..4]);

    assert!(ConfigChange::recognize(&command).is_err());
}

#[test]
fn stop_rejects_skipped_successor_and_duplicate_members() {
    assert!(ConfigChange::stop(
        "cluster-a",
        4,
        LogHash::ZERO,
        6,
        vec!["r1".into(), "r2".into(), "r3".into()],
    )
    .is_err());
    assert!(ConfigChange::stop(
        "cluster-a",
        4,
        LogHash::ZERO,
        5,
        vec!["r1".into(), "r1".into(), "r3".into()],
    )
    .is_err());
}

#[test]
fn stop_accepts_max_wire_strings_and_rejects_one_byte_more() {
    let max = "x".repeat(u16::MAX as usize);
    let too_long = "x".repeat(u16::MAX as usize + 1);
    let members = vec!["r1".into(), "r2".into(), "r3".into()];

    let max_cluster = ConfigChange::stop(&max, 4, LogHash::ZERO, 5, members.clone()).unwrap();
    assert_eq!(
        ConfigChange::recognize(&max_cluster.to_stored_command()).unwrap(),
        max_cluster
    );
    assert!(ConfigChange::stop(&too_long, 4, LogHash::ZERO, 5, members).is_err());
    let max_member = ConfigChange::stop(
        "cluster-a",
        4,
        LogHash::ZERO,
        5,
        vec!["r1".into(), "r2".into(), max],
    )
    .unwrap();
    assert_eq!(
        ConfigChange::recognize(&max_member.to_stored_command()).unwrap(),
        max_member
    );
    assert!(ConfigChange::stop(
        "cluster-a",
        4,
        LogHash::ZERO,
        5,
        vec!["r1".into(), "r2".into(), too_long],
    )
    .is_err());
}

#[test]
fn membership_digest_rejects_members_larger_than_config_wire_limit() {
    let oversized = "z".repeat(u16::MAX as usize + 1);
    assert!(canonical_membership_digest(&["r1".into(), "r2".into(), oversized]).is_err());
}

#[test]
fn successor_descriptor_serde_rejects_invalid_or_oversized_data() {
    let valid_digest =
        canonical_membership_digest(&["r1".into(), "r2".into(), "r3".into()]).unwrap();
    let descriptor = |cluster_id: String, members: Vec<String>, config_digest: LogHash| {
        serde_json::json!({
            "cluster_id": cluster_id,
            "predecessor_config_id": 4,
            "predecessor_config_digest": LogHash::ZERO,
            "config_id": 5,
            "config_digest": config_digest,
            "members": members,
        })
    };

    for invalid in [
        descriptor(
            "x".repeat(u16::MAX as usize + 1),
            vec!["r1".into(), "r2".into(), "r3".into()],
            valid_digest,
        ),
        descriptor(
            "cluster-a".into(),
            vec!["r1".into(), "r2".into(), "x".repeat(u16::MAX as usize + 1)],
            valid_digest,
        ),
        descriptor(
            "cluster-a".into(),
            vec!["r2".into(), "r1".into(), "r3".into()],
            valid_digest,
        ),
        descriptor(
            "cluster-a".into(),
            vec!["r1".into(), "r2".into(), "r3".into()],
            LogHash::from_bytes([9; 32]),
        ),
    ] {
        assert!(serde_json::from_value::<SuccessorDescriptor>(invalid).is_err());
    }
}

#[test]
fn activation_requires_the_successor_and_stop_command_authorized_by_stop() {
    let predecessor = LogHash::from_bytes([3; 32]);
    let active = ConfigurationState::active(4, predecessor);
    let stop_change = ConfigChange::stop(
        "cluster-a",
        4,
        predecessor,
        5,
        vec!["r1".into(), "r2".into(), "r3".into()],
    )
    .unwrap();
    let authorized_successor = stop_change.successor().clone();
    let stop_command_hash = stop_change.to_stored_command().hash();
    let stop = config_entry(10, 4, LogHash::ZERO, stop_change);
    let stopped = active.validate_entry(&stop).unwrap();
    let serialized = serde_json::to_value(&stopped).unwrap();
    assert_eq!(
        serialized["stop_command_hash"],
        serde_json::to_value(stop_command_hash).unwrap()
    );
    let round_tripped: ConfigurationState = serde_json::from_value(serialized.clone()).unwrap();
    assert_eq!(round_tripped, stopped);

    let forged_stop_command_hash = LogHash::from_bytes([9; 32]);
    let mut forged_state = serialized;
    forged_state["stop_command_hash"] = serde_json::to_value(forged_stop_command_hash).unwrap();
    let forged_state: ConfigurationState = serde_json::from_value(forged_state).unwrap();
    let forged_activation = config_entry(
        11,
        5,
        stop.hash,
        ConfigChange::activation_barrier(
            authorized_successor.clone(),
            10,
            stop.hash,
            forged_stop_command_hash,
        ),
    );
    assert!(forged_state.validate_entry(&forged_activation).is_err());

    let other_successor = ConfigChange::stop(
        "cluster-a",
        4,
        predecessor,
        5,
        vec!["r1".into(), "r2".into(), "r4".into()],
    )
    .unwrap()
    .successor()
    .clone();
    for invalid in [
        ConfigChange::activation_barrier(other_successor, 10, stop.hash, stop_command_hash),
        ConfigChange::activation_barrier(
            authorized_successor.clone(),
            10,
            stop.hash,
            LogHash::from_bytes([9; 32]),
        ),
    ] {
        assert!(stopped
            .validate_entry(&config_entry(11, 5, stop.hash, invalid))
            .is_err());
    }

    let activation = config_entry(
        11,
        5,
        stop.hash,
        ConfigChange::activation_barrier(
            authorized_successor.clone(),
            10,
            stop.hash,
            stop_command_hash,
        ),
    );
    assert_eq!(
        stopped.validate_entry(&activation).unwrap(),
        ConfigurationState::active(5, authorized_successor.digest())
    );
}

#[test]
fn recovery_anchor_preserves_configuration_state_and_rejects_missing_state() {
    let stop_change = ConfigChange::stop(
        "cluster-a",
        4,
        LogHash::from_bytes([4; 32]),
        5,
        vec!["r1".into(), "r2".into(), "r3".into()],
    )
    .unwrap();
    let stopped = ConfigurationState::stopped(
        4,
        LogHash::from_bytes([4; 32]),
        LogAnchor::new(10, LogHash::from_bytes([5; 32])),
        stop_change.successor().clone(),
        stop_change.to_stored_command().hash(),
    );
    let anchor = RecoveryAnchor::new(
        "cluster-a",
        7,
        stopped.clone(),
        4,
        LogAnchor::new(10, LogHash::from_bytes([5; 32])),
        SnapshotIdentity::new(
            "snapshot-000000000000010",
            LogHash::from_bytes([9; 32]),
            8192,
            LogHash::from_bytes([6; 32]),
        ),
    );
    let json = serde_json::to_value(&anchor).unwrap();
    assert_eq!(anchor.configuration_state(), &stopped);
    assert_eq!(json["configuration_state"]["phase"], "stopped");

    let mut missing_state = json;
    missing_state
        .as_object_mut()
        .unwrap()
        .remove("configuration_state");
    assert!(serde_json::from_value::<RecoveryAnchor>(missing_state).is_err());
}

#[test]
fn stopped_state_rejects_missing_successor_authorization() {
    let incomplete = serde_json::json!({
        "phase": "stopped",
        "config_id": 4,
        "digest": LogHash::from_bytes([4; 32]),
        "stop": {
            "index": 10,
            "hash": LogHash::from_bytes([5; 32]),
        }
    });

    assert!(serde_json::from_value::<ConfigurationState>(incomplete).is_err());
}
