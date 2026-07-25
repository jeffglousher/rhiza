use rhiza_archive::{
    archive_lag, ArchiveManifest, CheckpointBase, CheckpointIdentity, CheckpointPublisherOptions,
    Error, ObjectArchiveStore, SnapshotRecord,
};
use rhiza_core::{
    ConfigChange, ConfigurationState, EntryType, LogAnchor, LogEntry, LogHash, RecoveryAnchor,
    Snapshot, SnapshotIdentity, SnapshotManifest,
};
use rhiza_log::{FileLogStore, IndexRange, LogStore, SegmentFile};
use rhiza_obj_store::{Error as ObjStoreError, ObjStore, ObjStoreConfig};

#[test]
fn archive_lag_is_committed_index_minus_archived_index() {
    assert_eq!(archive_lag(7_200, 5_000), 2_200);
    assert_eq!(archive_lag(5_000, 7_200), 0);
}

#[test]
fn multi_node_archive_rejects_local_object_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: dir.path().to_path_buf(),
    })
    .unwrap();

    assert!(matches!(
        ObjectArchiveStore::new(store, "cluster-a"),
        Err(Error::WeakCompareAndSwap)
    ));
}

#[tokio::test]
async fn single_process_archive_accepts_local_object_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: dir.path().to_path_buf(),
    })
    .unwrap();

    let archive = ObjectArchiveStore::new_for_single_process(store, "cluster-a");

    assert!(archive.load_manifest().await.unwrap().is_none());
}

#[tokio::test]
async fn segment_publication_is_immutable_and_verified_on_download() {
    let (_dir, store, archive) = local_archive();
    let segment = SegmentFile::new(IndexRange::new(1, 1_000).unwrap(), b"qlog-segment".to_vec());

    let record = archive.publish_segment(7, &segment).await.unwrap();

    assert_eq!(record.format_version(), 1);
    assert_eq!(record.cluster_id(), "cluster-a");
    assert_eq!(record.epoch(), 7);
    assert_eq!(record.start_index(), 1);
    assert_eq!(record.end_index(), 1_000);
    assert_eq!(record.size_bytes(), segment.bytes().len() as u64);
    assert_eq!(
        archive.download_segment(&record).await.unwrap(),
        segment.bytes()
    );

    assert_eq!(archive.publish_segment(7, &segment).await.unwrap(), record);

    let conflicting = SegmentFile::new(segment.range(), b"different-qlog-segment".to_vec());
    let conflict = archive.publish_segment(7, &conflicting).await;
    assert!(matches!(
        conflict,
        Err(Error::ObjectStore(ObjStoreError::AlreadyExists { .. }))
    ));
    assert_eq!(
        store.get(record.object_key()).await.unwrap(),
        segment.bytes()
    );
}

#[tokio::test]
async fn empty_checkpoint_initialization_is_idempotent_and_identity_bound() {
    let (_dir, store, archive) = local_checkpoint(checkpoint_identity());

    let first = archive.initialize_checkpoint().await.unwrap();
    let second = archive.initialize_checkpoint().await.unwrap();

    assert_eq!(first.manifest(), second.manifest());
    assert_eq!(first.manifest().identity(), &checkpoint_identity());
    assert_eq!(first.manifest().tip().index(), 0);
    assert_eq!(first.manifest().tip().hash(), LogHash::ZERO);
    assert!(first.manifest().segments().is_empty());

    let wrong = ObjectArchiveStore::new_checkpoint_for_single_process(
        store.clone(),
        CheckpointIdentity::new("cluster-a", 9, 1, 4),
    );
    let wrong_manifest = wrong.initialize_checkpoint().await.unwrap();
    store
        .put(
            &archive.checkpoint_manifest_key().unwrap(),
            serde_json::to_vec(wrong_manifest.manifest()).unwrap(),
        )
        .await
        .unwrap();

    assert!(matches!(
        archive.initialize_checkpoint().await,
        Err(Error::CheckpointIdentityMismatch { .. })
    ));
}

#[tokio::test]
async fn concurrent_publishers_with_different_batch_boundaries_converge() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    archive.initialize_checkpoint().await.unwrap();
    let entries = entries(1, 8, LogHash::ZERO);
    let first = archive.clone();
    let second = archive.clone();

    let (short, long) = tokio::join!(
        first.publish_committed(&entries[..3]),
        second.publish_committed(&entries)
    );

    short.unwrap();
    long.unwrap();
    let loaded = archive.load_checkpoint().await.unwrap().unwrap();
    assert_eq!(loaded.manifest().tip().index(), 8);
    assert_eq!(loaded.manifest().tip().hash(), entries[7].hash);
    assert_eq!(archive.restore_checkpoint().await.unwrap(), entries);
}

#[tokio::test]
async fn publication_retries_after_stale_manifest_cas() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    archive.initialize_checkpoint().await.unwrap();
    let entries = entries(1, 12, LogHash::ZERO);
    let publishers = (1..=12)
        .map(|end| {
            let archive = archive.clone();
            let batch = entries[..end].to_vec();
            tokio::spawn(async move { archive.publish_committed(&batch).await })
        })
        .collect::<Vec<_>>();

    for publisher in publishers {
        publisher.await.unwrap().unwrap();
    }

    assert_eq!(archive.restore_checkpoint().await.unwrap(), entries);
}

#[tokio::test]
async fn publisher_session_groups_concurrent_flushes_to_the_highest_requested_index() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    let publisher = std::sync::Arc::new(
        archive
            .open_checkpoint_publisher("publisher-a", CheckpointPublisherOptions::default())
            .await
            .unwrap(),
    );
    let committed = entries(1, 12, LogHash::ZERO);

    let mut flushes = Vec::new();
    for end in 1..=12 {
        let publisher = publisher.clone();
        let batch = committed[..end].to_vec();
        flushes.push(tokio::spawn(async move {
            publisher.publish_committed(&batch).await
        }));
    }
    for (end, flush) in flushes.into_iter().enumerate() {
        assert!(flush.await.unwrap().unwrap().manifest().tip().index() >= (end + 1) as u64);
    }
    let loaded = archive.load_checkpoint().await.unwrap().unwrap();
    assert_eq!(loaded.manifest().tip().index(), 12);
    assert_eq!(loaded.manifest().segments().len(), 1);
}

#[tokio::test]
async fn publisher_session_coalesces_adjacent_concurrent_suffixes() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    let publisher = std::sync::Arc::new(
        archive
            .open_checkpoint_publisher("publisher-a", CheckpointPublisherOptions::default())
            .await
            .unwrap(),
    );
    let committed = entries(1, 4, LogHash::ZERO);
    let first = publisher.clone();
    let second = publisher.clone();

    let (prefix, suffix) = tokio::join!(
        first.publish_committed(&committed[..2]),
        second.publish_committed(&committed[2..]),
    );

    assert_eq!(prefix.unwrap().manifest().tip().index(), 4);
    assert_eq!(suffix.unwrap().manifest().tip().index(), 4);
    assert_eq!(archive.restore_checkpoint().await.unwrap(), committed);
}

#[tokio::test]
async fn publisher_session_rejects_conflicting_concurrent_entries() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    let publisher = std::sync::Arc::new(
        archive
            .open_checkpoint_publisher("publisher-a", CheckpointPublisherOptions::default())
            .await
            .unwrap(),
    );
    let first = entries(1, 1, LogHash::ZERO);
    let mut conflicting = first.clone();
    conflicting[0].payload = b"conflict".to_vec();
    conflicting[0].hash = LogEntry::calculate_hash(
        &conflicting[0].cluster_id,
        conflicting[0].index,
        conflicting[0].epoch,
        conflicting[0].config_id,
        conflicting[0].entry_type,
        conflicting[0].prev_hash,
        &conflicting[0].payload,
    );
    let left = publisher.clone();
    let right = publisher.clone();

    let (first_result, conflicting_result) = tokio::join!(
        left.publish_committed(&first),
        right.publish_committed(&conflicting),
    );

    assert!(matches!(first_result, Err(Error::InvalidCheckpoint(_))));
    assert!(matches!(
        conflicting_result,
        Err(Error::InvalidCheckpoint(_))
    ));
    assert_eq!(
        archive
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .tip()
            .index(),
        0
    );
}

#[tokio::test]
async fn publisher_session_reloads_a_stale_manifest_cache_after_cas_conflict() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    let first = archive
        .open_checkpoint_publisher("publisher-a", CheckpointPublisherOptions::default())
        .await
        .unwrap();
    let stale = archive
        .open_checkpoint_publisher("publisher-b", CheckpointPublisherOptions::default())
        .await
        .unwrap();
    let committed = entries(1, 4, LogHash::ZERO);

    first.publish_committed(&committed[..2]).await.unwrap();
    assert_eq!(stale.cached_checkpoint().await.manifest().tip().index(), 0);
    let reloaded = stale.publish_committed(&committed).await.unwrap();

    assert_eq!(reloaded.manifest().tip().index(), 4);
    assert_eq!(archive.restore_checkpoint().await.unwrap(), committed);
    first.close().await.unwrap();
    stale.close().await.unwrap();
}

#[tokio::test]
async fn observing_a_remote_checkpoint_does_not_mutate_the_publisher_cache_before_acceptance() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    let writer = archive
        .open_checkpoint_publisher("publisher-a", CheckpointPublisherOptions::default())
        .await
        .unwrap();
    let observer = archive
        .open_checkpoint_publisher("publisher-b", CheckpointPublisherOptions::default())
        .await
        .unwrap();
    let committed = entries(1, 4, LogHash::ZERO);
    writer.publish_committed(&committed).await.unwrap();

    let observed = observer.observe_checkpoint().await.unwrap();

    assert_eq!(observed.manifest().tip().index(), 4);
    assert_eq!(
        observer.cached_checkpoint().await.manifest().tip().index(),
        0
    );
    observer.cache_observed_checkpoint(observed).await.unwrap();
    assert_eq!(
        observer.cached_checkpoint().await.manifest().tip().index(),
        4
    );
    writer.close().await.unwrap();
    observer.close().await.unwrap();
}

#[tokio::test]
async fn accepting_an_observed_checkpoint_fails_closed_on_rollback() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    let publisher = archive
        .open_checkpoint_publisher("publisher-a", CheckpointPublisherOptions::default())
        .await
        .unwrap();
    let genesis = publisher.cached_checkpoint().await;
    publisher
        .publish_committed(&entries(1, 2, LogHash::ZERO))
        .await
        .unwrap();

    let error = publisher
        .cache_observed_checkpoint(genesis)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("rolled back"));
    assert_eq!(
        publisher.cached_checkpoint().await.manifest().tip().index(),
        2
    );
    publisher.close().await.unwrap();
}

#[tokio::test]
async fn publisher_session_recommends_existing_snapshot_compaction_at_the_segment_limit() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    let publisher = archive
        .open_checkpoint_publisher(
            "publisher-a",
            CheckpointPublisherOptions::default().with_compaction_segment_limit(2),
        )
        .await
        .unwrap();
    let committed = entries(1, 2, LogHash::ZERO);

    publisher.publish_committed(&committed[..1]).await.unwrap();
    assert!(!publisher.compaction_recommended().await);
    publisher.publish_committed(&committed[1..]).await.unwrap();
    assert!(publisher.compaction_recommended().await);

    let bytes = b"snapshot-at-two";
    publisher
        .publish_checkpoint_snapshot(
            recovery_anchor(2, committed[1].hash, bytes, "snapshot-two"),
            bytes,
        )
        .await
        .unwrap();
    assert!(!publisher.compaction_recommended().await);
    assert!(publisher
        .cached_checkpoint()
        .await
        .manifest()
        .segments()
        .is_empty());
    publisher.close().await.unwrap();
}

#[tokio::test]
async fn restore_rejects_corrupt_segment_bytes() {
    let (_dir, store, archive) = local_checkpoint(checkpoint_identity());
    let entries = entries(1, 3, LogHash::ZERO);
    let published = archive.publish_committed(&entries).await.unwrap();
    let segment = &published.manifest().segments()[0];
    store.put(segment.object_key(), b"corrupt").await.unwrap();

    assert!(matches!(
        archive.restore_checkpoint().await,
        Err(Error::SizeMismatch { .. }) | Err(Error::ChecksumMismatch { .. })
    ));
}

#[tokio::test]
async fn restore_rejects_segment_identity_mismatch() {
    let (_dir, store, archive) = local_checkpoint(checkpoint_identity());
    let published = archive
        .publish_committed(&entries(1, 2, LogHash::ZERO))
        .await
        .unwrap();
    let segment = &published.manifest().segments()[0];
    let wrong_entries = entries_for("cluster-a", 2, 1, 1, 2, LogHash::ZERO);
    let wrong_bytes = rhiza_log::encode_segment(&wrong_entries);
    let mut json = serde_json::to_value(published.manifest()).unwrap();
    json["segments"][0]["size_bytes"] = (wrong_bytes.len() as u64).into();
    json["segments"][0]["sha256"] = LogHash::digest(&[&wrong_bytes]).to_hex().into();
    store.put(segment.object_key(), wrong_bytes).await.unwrap();
    store
        .put(
            &archive.checkpoint_manifest_key().unwrap(),
            serde_json::to_vec(&json).unwrap(),
        )
        .await
        .unwrap();

    assert!(matches!(
        archive.restore_checkpoint().await,
        Err(Error::CheckpointIdentityMismatch { .. })
    ));
}

#[tokio::test]
async fn checkpoint_manifest_rejects_gaps_overlaps_and_unknown_fields() {
    for (field, value, expected_message) in [
        ("start_index", 4_u64, "gap"),
        ("start_index", 2_u64, "overlap"),
    ] {
        let (_dir, store, archive) = local_checkpoint(checkpoint_identity());
        let first = entries(1, 2, LogHash::ZERO);
        let second = entries(3, 4, first.last().unwrap().hash);
        archive.publish_committed(&first).await.unwrap();
        let published = archive.publish_committed(&second).await.unwrap();
        let mut json = serde_json::to_value(published.manifest()).unwrap();
        json["segments"][1][field] = value.into();
        store
            .put(
                &archive.checkpoint_manifest_key().unwrap(),
                serde_json::to_vec(&json).unwrap(),
            )
            .await
            .unwrap();

        let error = archive.load_checkpoint().await.unwrap_err().to_string();
        assert!(error.contains(expected_message), "{error}");
    }

    let (_dir, store, archive) = local_checkpoint(checkpoint_identity());
    let initialized = archive.initialize_checkpoint().await.unwrap();
    let mut json = serde_json::to_value(initialized.manifest()).unwrap();
    json["unexpected"] = true.into();
    store
        .put(
            &archive.checkpoint_manifest_key().unwrap(),
            serde_json::to_vec(&json).unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        archive.load_checkpoint().await,
        Err(Error::Serialization(_))
    ));
}

#[tokio::test]
async fn restored_entries_rebuild_an_empty_file_log_store() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    let entries = entries(1, 6, LogHash::ZERO);
    archive.publish_committed(&entries[..2]).await.unwrap();
    archive.publish_committed(&entries[2..]).await.unwrap();

    let restored = archive.restore_checkpoint().await.unwrap();
    let log_dir = tempfile::tempdir().unwrap();
    let log = FileLogStore::open(log_dir.path(), "cluster-a", 7, 3).unwrap();
    log.append_batch(&restored).unwrap();
    drop(log);

    let reopened = FileLogStore::open(log_dir.path(), "cluster-a", 7, 3).unwrap();
    assert_eq!(
        reopened.read_range(IndexRange::new(1, 6).unwrap()).unwrap(),
        entries
    );
}

#[tokio::test]
async fn unsupported_manifest_format_is_rejected_without_overwriting_it() {
    let (_dir, store, archive) = local_checkpoint(checkpoint_identity());
    let published = archive
        .publish_committed(&entries(1, 2, LogHash::ZERO))
        .await
        .unwrap();
    let mut unsupported = serde_json::to_value(published.manifest()).unwrap();
    unsupported["format_version"] = 1.into();
    store
        .put(
            &archive.checkpoint_manifest_key().unwrap(),
            serde_json::to_vec(&unsupported).unwrap(),
        )
        .await
        .unwrap();

    let key = archive.checkpoint_manifest_key().unwrap();
    let before = store.get(&key).await.unwrap();
    assert!(matches!(
        archive.load_checkpoint().await,
        Err(Error::UnsupportedFormatVersion {
            object: "checkpoint manifest",
            version: 1,
        })
    ));
    assert!(matches!(
        archive.initialize_checkpoint().await,
        Err(Error::UnsupportedFormatVersion {
            object: "checkpoint manifest",
            version: 1,
        })
    ));
    assert!(matches!(
        archive
            .publish_committed(&entries(3, 3, LogHash::from_bytes([1; 32])))
            .await,
        Err(Error::UnsupportedFormatVersion {
            object: "checkpoint manifest",
            version: 1,
        })
    ));
    assert_eq!(store.get(&key).await.unwrap(), before);
}

#[tokio::test]
async fn snapshot_base_restores_snapshot_and_exact_tail() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    let first = entries(1, 2, LogHash::ZERO);
    let second = entries(3, 4, first.last().unwrap().hash);
    archive.publish_committed(&first).await.unwrap();
    archive.publish_committed(&second).await.unwrap();
    let bytes = b"snapshot-at-two";
    let anchor = recovery_anchor(2, first[1].hash, bytes, "snapshot-two");

    let advanced = archive
        .publish_checkpoint_snapshot(anchor.clone(), bytes)
        .await
        .unwrap();
    assert!(matches!(
        advanced.manifest().base(),
        CheckpointBase::Snapshot(_)
    ));
    let snapshot_key = advanced.manifest().base().snapshot().unwrap().object_key();
    assert!(snapshot_key.contains(
        "rhiza/cluster-a/checkpoints/epoch-00000000000000000007/config-00000000000000000003/generation-00000000000000000004/snapshots/00000000000000000002-"
    ));
    assert!(snapshot_key.ends_with(&format!(
        "-{}-{}.snapshot",
        LogHash::digest(&[bytes]).to_hex(),
        anchor.executor_fingerprint().to_hex(),
    )));
    assert_eq!(advanced.manifest().segments().len(), 1);
    assert_eq!(advanced.manifest().segments()[0].start_index(), 3);

    let restored = archive.restore_checkpoint_state().await.unwrap();
    assert_eq!(restored.snapshot().unwrap().anchor(), &anchor);
    assert_eq!(restored.snapshot().unwrap().bytes(), bytes);
    assert_eq!(restored.suffix(), second);
    assert_eq!(restored.tip().index(), 4);
    assert!(matches!(
        archive.restore_checkpoint().await,
        Err(Error::SnapshotBaseRequiresStructuredRestore)
    ));
}

#[tokio::test]
async fn snapshot_anchor_round_trips_configuration_state() {
    let (_dir, store, archive) = local_checkpoint(checkpoint_identity());
    let committed = entries(1, 1, LogHash::ZERO);
    archive.publish_committed(&committed).await.unwrap();
    let bytes = b"snapshot-at-stop";
    let compacted = LogAnchor::new(1, committed[0].hash);
    let stop_change = ConfigChange::stop(
        "cluster-a",
        3,
        LogHash::from_bytes([7; 32]),
        4,
        vec!["node-a".into(), "node-b".into(), "node-c".into()],
    )
    .unwrap();
    let configuration = ConfigurationState::stopped(
        3,
        LogHash::from_bytes([7; 32]),
        compacted,
        stop_change.successor().clone(),
        stop_change.to_stored_command().hash(),
    );
    let anchor = RecoveryAnchor::new(
        "cluster-a",
        7,
        configuration.clone(),
        4,
        compacted,
        SnapshotIdentity::new(
            "snapshot-stop",
            LogHash::digest(&[bytes]),
            bytes.len() as u64,
            LogHash::from_bytes([6; 32]),
        ),
    );

    archive
        .publish_checkpoint_snapshot(anchor.clone(), bytes)
        .await
        .unwrap();
    let restored = archive.restore_checkpoint_state().await.unwrap();
    assert_eq!(restored.snapshot().unwrap().anchor(), &anchor);
    assert_eq!(
        restored.snapshot().unwrap().anchor().configuration_state(),
        &configuration
    );

    let loaded = archive.load_checkpoint().await.unwrap().unwrap();
    let mut malformed = serde_json::to_value(loaded.manifest()).unwrap();
    malformed["base"]["snapshot"]["anchor"]["configuration_state"]["stop"]["index"] = 2.into();
    store
        .put(
            &archive.checkpoint_manifest_key().unwrap(),
            serde_json::to_vec(&malformed).unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        archive.load_checkpoint().await,
        Err(Error::InvalidCheckpoint(_))
    ));
}

#[tokio::test]
async fn checkpoint_snapshot_round_trip_preserves_executor_fingerprint() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    let committed = entries(1, 1, LogHash::ZERO);
    archive.publish_committed(&committed).await.unwrap();
    let bytes = b"fingerprinted-checkpoint";
    let executor_fingerprint = LogHash::from_bytes([6; 32]);
    let anchor = recovery_anchor_for_fingerprint(
        1,
        committed[0].hash,
        bytes,
        "snapshot-fingerprinted",
        executor_fingerprint,
    );

    let published = archive
        .publish_checkpoint_snapshot(anchor.clone(), bytes)
        .await
        .unwrap();
    let base = published.manifest().base().snapshot().unwrap();
    assert_eq!(base.executor_fingerprint(), executor_fingerprint);
    assert!(base
        .object_key()
        .ends_with(&format!("-{}.snapshot", executor_fingerprint.to_hex())));

    let restored = archive.restore_checkpoint_state().await.unwrap();
    assert_eq!(restored.snapshot().unwrap().anchor(), &anchor);
    assert_eq!(
        restored.snapshot().unwrap().anchor().executor_fingerprint(),
        executor_fingerprint
    );
}

#[tokio::test]
async fn recovery_generation_roll_preserves_snapshot_and_exact_suffix() {
    let (_dir, store, source) = local_checkpoint(checkpoint_identity());
    let prefix = entries(1, 2, LogHash::ZERO);
    let suffix = entries(3, 4, prefix.last().unwrap().hash);
    source.publish_committed(&prefix).await.unwrap();
    source.publish_committed(&suffix).await.unwrap();
    let bytes = b"snapshot-at-two-for-roll";
    source
        .publish_checkpoint_snapshot(
            recovery_anchor(2, prefix[1].hash, bytes, "snapshot-roll"),
            bytes,
        )
        .await
        .unwrap();
    let target_identity = CheckpointIdentity::new("cluster-a", 7, 3, 5);
    let target = ObjectArchiveStore::new_checkpoint_for_single_process(store, target_identity);

    source.roll_recovery_generation(&target).await.unwrap();

    let restored = target.restore_checkpoint_state().await.unwrap();
    assert_eq!(restored.snapshot().unwrap().bytes(), bytes);
    assert_eq!(
        restored.snapshot().unwrap().anchor().configuration_state(),
        source
            .restore_checkpoint_state()
            .await
            .unwrap()
            .snapshot()
            .unwrap()
            .anchor()
            .configuration_state()
    );
    assert_eq!(
        restored.snapshot().unwrap().anchor().recovery_generation(),
        5
    );
    assert_eq!(restored.suffix(), suffix);
    assert_eq!(
        target
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .segments()[0]
            .sha256(),
        source
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .segments()[0]
            .sha256()
    );
}

#[tokio::test]
async fn snapshot_base_requires_an_exact_segment_boundary() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    let all = entries(1, 4, LogHash::ZERO);
    archive.publish_committed(&all).await.unwrap();

    let error = archive
        .publish_checkpoint_snapshot(
            recovery_anchor(2, all[1].hash, b"snapshot", "snapshot-two"),
            b"snapshot",
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidCheckpoint(_)));
    assert!(error.to_string().contains("segment boundary"));
}

#[tokio::test]
async fn snapshot_restore_rejects_object_and_manifest_tamper() {
    let (_dir, store, archive) = local_checkpoint(checkpoint_identity());
    let committed = entries(1, 1, LogHash::ZERO);
    archive.publish_committed(&committed).await.unwrap();
    let anchor = recovery_anchor(1, committed[0].hash, b"snapshot", "snapshot-one");
    let advanced = archive
        .publish_checkpoint_snapshot(anchor, b"snapshot")
        .await
        .unwrap();
    let base = advanced.manifest().base().snapshot().unwrap();
    store.put(base.object_key(), b"tampered").await.unwrap();
    assert!(matches!(
        archive.restore_checkpoint_state().await,
        Err(Error::SizeMismatch { .. }) | Err(Error::ChecksumMismatch { .. })
    ));

    let mut manifest = serde_json::to_value(advanced.manifest()).unwrap();
    manifest["base"]["snapshot"]["object_key"] =
        base.object_key().replace(".snapshot", ".sqlite").into();
    store
        .put(
            &archive.checkpoint_manifest_key().unwrap(),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        archive.load_checkpoint().await,
        Err(Error::InvalidCheckpoint(_))
    ));

    let mut manifest = serde_json::to_value(advanced.manifest()).unwrap();
    manifest["base"]["snapshot"]["digest"] = serde_json::to_value(LogHash::ZERO).unwrap();
    store
        .put(
            &archive.checkpoint_manifest_key().unwrap(),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        archive.load_checkpoint().await,
        Err(Error::InvalidCheckpoint(_))
    ));

    let mut manifest = serde_json::to_value(advanced.manifest()).unwrap();
    manifest["base"]["snapshot"]["anchor"]["compacted"]["hash"] =
        serde_json::to_value(LogHash::ZERO).unwrap();
    store
        .put(
            &archive.checkpoint_manifest_key().unwrap(),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        archive.load_checkpoint().await,
        Err(Error::InvalidCheckpoint(_))
    ));
}

#[tokio::test]
async fn checkpoint_snapshot_rejects_executor_fingerprint_mismatch_and_tamper() {
    let (_dir, store, archive) = local_checkpoint(checkpoint_identity());
    let committed = entries(1, 1, LogHash::ZERO);
    archive.publish_committed(&committed).await.unwrap();
    let published = archive
        .publish_checkpoint_snapshot(
            recovery_anchor_for_fingerprint(
                1,
                committed[0].hash,
                b"snapshot",
                "snapshot-one",
                LogHash::from_bytes([6; 32]),
            ),
            b"snapshot",
        )
        .await
        .unwrap();

    let mut mismatch = serde_json::to_value(published.manifest()).unwrap();
    mismatch["base"]["snapshot"]["executor_fingerprint"] = serde_json::json!(vec![7; 32]);
    store
        .put(
            &archive.checkpoint_manifest_key().unwrap(),
            serde_json::to_vec(&mismatch).unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        archive.load_checkpoint().await,
        Err(Error::InvalidCheckpoint(_))
    ));

    let mut tampered = serde_json::to_value(published.manifest()).unwrap();
    tampered["base"]["snapshot"]["anchor"]["snapshot"]["executor_fingerprint"] =
        serde_json::json!(vec![8; 32]);
    store
        .put(
            &archive.checkpoint_manifest_key().unwrap(),
            serde_json::to_vec(&tampered).unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        archive.load_checkpoint().await,
        Err(Error::InvalidCheckpoint(_))
    ));
}

#[tokio::test]
async fn concurrent_snapshot_publishers_converge_idempotently() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    let committed = entries(1, 1, LogHash::ZERO);
    archive.publish_committed(&committed).await.unwrap();
    let anchor = recovery_anchor(1, committed[0].hash, b"snapshot", "snapshot-one");

    let (left, right) = tokio::join!(
        archive.publish_checkpoint_snapshot(anchor.clone(), b"snapshot"),
        archive.publish_checkpoint_snapshot(anchor, b"snapshot")
    );
    assert_eq!(left.unwrap().manifest(), right.unwrap().manifest());
}

#[tokio::test]
async fn tail_publish_and_snapshot_base_cas_race_preserves_the_suffix() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    let first = entries(1, 1, LogHash::ZERO);
    let second = entries(2, 2, first[0].hash);
    archive.publish_committed(&first).await.unwrap();
    let anchor = recovery_anchor(1, first[0].hash, b"snapshot", "snapshot-one");

    let (tail, base) = tokio::join!(
        archive.publish_committed(&second),
        archive.publish_checkpoint_snapshot(anchor, b"snapshot")
    );
    tail.unwrap();
    base.unwrap();

    let restored = archive.restore_checkpoint_state().await.unwrap();
    assert_eq!(restored.suffix(), second);
    assert_eq!(restored.tip().index(), 2);
}

#[tokio::test]
async fn snapshot_base_rejects_regression_and_same_index_conflict() {
    let (_dir, _store, archive) = local_checkpoint(checkpoint_identity());
    let first = entries(1, 1, LogHash::ZERO);
    let second = entries(2, 2, first[0].hash);
    archive.publish_committed(&first).await.unwrap();
    archive.publish_committed(&second).await.unwrap();
    archive
        .publish_checkpoint_snapshot(
            recovery_anchor(2, second[0].hash, b"snapshot-two", "snapshot-two"),
            b"snapshot-two",
        )
        .await
        .unwrap();

    assert!(matches!(
        archive
            .publish_checkpoint_snapshot(
                recovery_anchor(1, first[0].hash, b"snapshot-one", "snapshot-one"),
                b"snapshot-one",
            )
            .await,
        Err(Error::CheckpointBaseRegression { .. })
    ));
    assert!(matches!(
        archive
            .publish_checkpoint_snapshot(
                recovery_anchor(2, second[0].hash, b"other", "other"),
                b"other",
            )
            .await,
        Err(Error::CheckpointBaseConflict { .. })
    ));
}

#[tokio::test]
async fn archive_manifest_round_trips_and_updates_with_compare_and_swap() {
    let (_dir, store, archive) = local_archive();
    let segment = SegmentFile::new(IndexRange::new(1, 10).unwrap(), b"segment".to_vec());
    let segment_record = archive.publish_segment(3, &segment).await.unwrap();
    let mut manifest = ArchiveManifest::new("cluster-a");
    manifest.add_segment(segment_record);

    let first_version = archive.publish_manifest(&manifest, None).await.unwrap();
    let loaded = archive.load_manifest().await.unwrap().unwrap();
    assert_eq!(loaded.manifest(), &manifest);
    assert_eq!(loaded.version(), &first_version);

    let raw = store
        .get("rhiza/cluster-a/archive/manifest.json")
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(json["format_version"], 1);
    assert_eq!(json["segments"][0]["format_version"], 1);

    let snapshot = snapshot("cluster-a", 3, 10, b"snapshot-bytes");
    let snapshot_record = archive.publish_snapshot(&snapshot).await.unwrap();
    assert_eq!(snapshot_record.manifest(), snapshot.manifest());
    let mut updated = loaded.manifest().clone();
    updated.set_latest_snapshot(snapshot_record);
    archive
        .publish_manifest(&updated, Some(loaded.version().clone()))
        .await
        .unwrap();

    let current = archive.load_manifest().await.unwrap().unwrap();
    assert_eq!(current.manifest(), &updated);
    assert_eq!(current.manifest().latest_snapshot_index(), Some(10));
    assert_eq!(current.manifest().latest_archived_index(), Some(10));

    let stale = archive
        .publish_manifest(&manifest, Some(first_version))
        .await;
    assert!(matches!(
        stale,
        Err(Error::ObjectStore(ObjStoreError::Precondition { .. }))
    ));
}

#[tokio::test]
async fn archive_manifest_round_trip_preserves_executor_fingerprint() {
    let (_dir, _store, archive) = local_archive();
    let executor_fingerprint = LogHash::from_bytes([6; 32]);
    let snapshot =
        snapshot_for_fingerprint("cluster-a", 3, 10, b"snapshot-bytes", executor_fingerprint);
    let record = archive.publish_snapshot(&snapshot).await.unwrap();
    assert!(record
        .object_key()
        .ends_with(&format!("-{}.snapshot", executor_fingerprint.to_hex())));

    let mut manifest = ArchiveManifest::new("cluster-a");
    manifest.set_latest_snapshot(record);
    archive.publish_manifest(&manifest, None).await.unwrap();

    let loaded = archive.load_manifest().await.unwrap().unwrap();
    assert_eq!(loaded.manifest(), &manifest);
    assert_eq!(
        loaded
            .manifest()
            .latest_snapshot()
            .unwrap()
            .manifest()
            .executor_fingerprint(),
        executor_fingerprint
    );
}

#[tokio::test]
async fn corrupted_snapshot_downloads_fail_size_or_sha256_verification() {
    let (_dir, store, archive) = local_archive();

    let size_snapshot = snapshot("cluster-a", 4, 20, b"snapshot-size");
    let size_record = archive.publish_snapshot(&size_snapshot).await.unwrap();
    store.put(size_record.object_key(), b"short").await.unwrap();
    assert!(matches!(
        archive.download_snapshot(&size_record).await,
        Err(Error::SizeMismatch { .. })
    ));

    let hash_snapshot = snapshot("cluster-a", 4, 21, b"snapshot-hash");
    let hash_record = archive.publish_snapshot(&hash_snapshot).await.unwrap();
    store
        .put(hash_record.object_key(), b"Snapshot-hash")
        .await
        .unwrap();
    assert!(matches!(
        archive.download_snapshot(&hash_record).await,
        Err(Error::ChecksumMismatch { .. })
    ));
}

#[tokio::test]
async fn snapshot_publication_derives_identity_only_from_the_manifest() {
    let (_dir, _store, archive) = local_archive();
    let source_snapshot = snapshot("cluster-a", 7, 42, b"snapshot-bytes");

    let record = archive.publish_snapshot(&source_snapshot).await.unwrap();

    assert_eq!(record.manifest(), source_snapshot.manifest());
    assert_eq!(record.snapshot_index(), 42);
    assert_eq!(
        record.object_key(),
        "rhiza/cluster-a/archive/snapshots/epoch-00000000000000000007/snapshot-00000000000000000042-0606060606060606060606060606060606060606060606060606060606060606.snapshot"
    );

    let json = serde_json::to_value(&record).unwrap();
    assert_eq!(json["manifest"]["cluster_id"], "cluster-a");
    assert_eq!(json["manifest"]["epoch"], 7);
    assert_eq!(json["manifest"]["config_id"], 1);
    assert_eq!(json["manifest"]["schema_version"], 1);
    assert_eq!(json["manifest"]["created_by"], "node-1");
    assert_eq!(json["manifest"]["index"], 42);
    assert_eq!(json["manifest"]["snapshot_id"], "snapshot-000000000000042");

    let mut tampered_json = json.clone();
    tampered_json["object_key"] = record.object_key().replace(".snapshot", ".sqlite").into();
    let tampered_record: SnapshotRecord = serde_json::from_value(tampered_json).unwrap();
    assert!(matches!(
        archive.download_snapshot(&tampered_record).await,
        Err(Error::SnapshotIdentityMismatch {
            field: "object key",
            ..
        })
    ));

    let wrong_cluster = snapshot("cluster-b", 7, 42, b"snapshot-bytes");
    assert!(matches!(
        archive.publish_snapshot(&wrong_cluster).await,
        Err(Error::ClusterMismatch { .. })
    ));
}

#[tokio::test]
async fn snapshot_download_rejects_tampered_record_identity() {
    let (_dir, store, archive) = local_archive();
    let snapshot = snapshot_for_fingerprint(
        "cluster-a",
        7,
        42,
        b"snapshot-bytes",
        LogHash::from_bytes([6; 32]),
    );
    let record = archive.publish_snapshot(&snapshot).await.unwrap();

    let forged_key = "rhiza/cluster-a/archive/snapshots/forged.snapshot";
    store.put(forged_key, snapshot.db_bytes()).await.unwrap();
    let mut forged_key_json = serde_json::to_value(&record).unwrap();
    forged_key_json["object_key"] = forged_key.into();
    let forged_key_record: SnapshotRecord = serde_json::from_value(forged_key_json).unwrap();
    assert!(archive.download_snapshot(&forged_key_record).await.is_err());

    let mut forged_epoch_json = serde_json::to_value(&record).unwrap();
    forged_epoch_json["manifest"]["epoch"] = 8.into();
    let forged_epoch_record: SnapshotRecord = serde_json::from_value(forged_epoch_json).unwrap();
    assert!(archive
        .download_snapshot(&forged_epoch_record)
        .await
        .is_err());

    let mut forged_cluster_json = serde_json::to_value(&record).unwrap();
    forged_cluster_json["manifest"]["cluster_id"] = "cluster-b".into();
    let forged_cluster_record: SnapshotRecord =
        serde_json::from_value(forged_cluster_json).unwrap();
    assert!(matches!(
        archive.download_snapshot(&forged_cluster_record).await,
        Err(Error::ClusterMismatch { .. })
    ));

    let mut forged_fingerprint_json = serde_json::to_value(&record).unwrap();
    forged_fingerprint_json["manifest"]["executor_fingerprint"] = serde_json::json!(vec![7; 32]);
    let forged_fingerprint_record: SnapshotRecord =
        serde_json::from_value(forged_fingerprint_json).unwrap();
    assert!(matches!(
        archive.download_snapshot(&forged_fingerprint_record).await,
        Err(Error::SnapshotIdentityMismatch { .. })
    ));
}

fn local_archive() -> (tempfile::TempDir, ObjStore, ObjectArchiveStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: dir.path().to_path_buf(),
    })
    .unwrap();
    let archive = ObjectArchiveStore::new_for_single_process(store.clone(), "cluster-a");
    (dir, store, archive)
}

fn local_checkpoint(
    identity: CheckpointIdentity,
) -> (tempfile::TempDir, ObjStore, ObjectArchiveStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: dir.path().to_path_buf(),
    })
    .unwrap();
    let archive = ObjectArchiveStore::new_checkpoint_for_single_process(store.clone(), identity);
    (dir, store, archive)
}

fn checkpoint_identity() -> CheckpointIdentity {
    CheckpointIdentity::new("cluster-a", 7, 3, 4)
}

fn entries(start: u64, end: u64, prev_hash: LogHash) -> Vec<LogEntry> {
    entries_for("cluster-a", 7, 3, start, end, prev_hash)
}

fn entries_for(
    cluster_id: &str,
    epoch: u64,
    config_id: u64,
    start: u64,
    end: u64,
    mut prev_hash: LogHash,
) -> Vec<LogEntry> {
    (start..=end)
        .map(|index| {
            let payload = format!("entry-{index}").into_bytes();
            let hash = LogEntry::calculate_hash(
                cluster_id,
                index,
                epoch,
                config_id,
                EntryType::Command,
                prev_hash,
                &payload,
            );
            let entry = LogEntry {
                cluster_id: cluster_id.into(),
                epoch,
                config_id,
                index,
                entry_type: EntryType::Command,
                payload,
                prev_hash,
                hash,
            };
            prev_hash = hash;
            entry
        })
        .collect()
}

fn snapshot(cluster_id: &str, epoch: u64, index: u64, bytes: &[u8]) -> Snapshot {
    Snapshot::new(
        SnapshotManifest::new(
            cluster_id,
            ConfigurationState::active(1, LogHash::ZERO),
            epoch,
            index,
            LogHash::ZERO,
            1,
            "node-1",
            LogHash::from_bytes([6; 32]),
        ),
        bytes.to_vec(),
    )
}

fn snapshot_for_fingerprint(
    cluster_id: &str,
    epoch: u64,
    index: u64,
    bytes: &[u8],
    executor_fingerprint: LogHash,
) -> Snapshot {
    Snapshot::new(
        SnapshotManifest::new(
            cluster_id,
            ConfigurationState::active(1, LogHash::ZERO),
            epoch,
            index,
            LogHash::ZERO,
            1,
            "node-1",
            executor_fingerprint,
        ),
        bytes.to_vec(),
    )
}

fn recovery_anchor(index: u64, hash: LogHash, bytes: &[u8], snapshot_id: &str) -> RecoveryAnchor {
    RecoveryAnchor::new(
        "cluster-a",
        7,
        ConfigurationState::active(3, LogHash::ZERO),
        4,
        LogAnchor::new(index, hash),
        SnapshotIdentity::new(
            snapshot_id,
            LogHash::digest(&[bytes]),
            bytes.len() as u64,
            LogHash::from_bytes([6; 32]),
        ),
    )
}

fn recovery_anchor_for_fingerprint(
    index: u64,
    hash: LogHash,
    bytes: &[u8],
    snapshot_id: &str,
    executor_fingerprint: LogHash,
) -> RecoveryAnchor {
    RecoveryAnchor::new(
        "cluster-a",
        7,
        ConfigurationState::active(3, LogHash::ZERO),
        4,
        LogAnchor::new(index, hash),
        SnapshotIdentity::new(
            snapshot_id,
            LogHash::digest(&[bytes]),
            bytes.len() as u64,
            executor_fingerprint,
        ),
    )
}
