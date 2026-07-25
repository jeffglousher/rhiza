use std::{path::Path, sync::Arc, time::Duration};

use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
#[cfg(any(feature = "graph", feature = "kv"))]
use rhiza_core::ExecutionProfile;
use rhiza_core::{ConfigurationState, LogHash};
use rhiza_log::{FileLogStore, LogStore};
#[cfg(any(feature = "graph", feature = "kv"))]
use rhiza_node::effective_cluster_id;
#[cfg(feature = "kv")]
use rhiza_node::KvCommandV1;
use rhiza_node::{
    rehydrate_recorder_after_checkpoint, restore_checkpoint_to_fresh_data_dir,
    restore_checkpoint_to_fresh_data_dir_for_node, CheckpointCoordinator, DurabilityError,
    DurabilityHealth, DurabilityMode, NodeConfig, NodeRuntime, PeerConfig, ReadConsistency,
};
#[cfg(feature = "graph")]
use rhiza_node::{GraphCommandV1, GraphValueV1};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{Membership, RecorderFileStore, RecorderRpc, ThreeNodeConsensus};
use rhiza_sql::SqliteStateMachine;

#[test]
fn durability_mode_rejects_zero_intervals() {
    assert!(DurabilityMode::Sync.validate().is_ok());
    assert!(matches!(
        DurabilityMode::Bounded {
            max_lag: Duration::ZERO
        }
        .validate(),
        Err(DurabilityError::InvalidDuration { mode: "bounded" })
    ));
    assert!(matches!(
        DurabilityMode::Periodic {
            interval: Duration::ZERO
        }
        .validate(),
        Err(DurabilityError::InvalidDuration { mode: "periodic" })
    ));
}

#[tokio::test]
async fn coordinator_open_fails_closed_when_checkpoint_is_missing() {
    let archive_root = tempfile::tempdir().unwrap();
    let archive = checkpoint_store(archive_root.path());

    assert!(matches!(
        CheckpointCoordinator::open(archive, DurabilityMode::Sync).await,
        Err(DurabilityError::MissingCheckpoint)
    ));
}

#[tokio::test]
async fn coordinator_open_rejects_tampered_segment_checksum_metadata() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.path().join("archive"),
    })
    .unwrap();
    let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store.clone(),
        CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 1),
    );
    archive.initialize_checkpoint().await.unwrap();
    let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let runtime = runtime(root.path().join("node"));
    let committed = runtime.write("request-1", "alpha", "one").unwrap();
    coordinator.note_committed(committed.applied_index);
    coordinator
        .flush_runtime(&runtime, committed.applied_index)
        .await
        .unwrap();

    let loaded = archive.load_checkpoint().await.unwrap().unwrap();
    let checksum = loaded.manifest().segments()[0].sha256();
    let replacement = LogHash::digest(&[b"different valid checksum"]).to_hex();
    assert_ne!(checksum, replacement);
    let manifest_key = archive.checkpoint_manifest_key().unwrap();
    let manifest = String::from_utf8(store.get(&manifest_key).await.unwrap()).unwrap();
    assert_eq!(manifest.matches(checksum).count(), 1);
    store
        .put(&manifest_key, manifest.replacen(checksum, &replacement, 1))
        .await
        .unwrap();

    assert!(matches!(
        CheckpointCoordinator::open(archive, DurabilityMode::Sync).await,
        Err(DurabilityError::Archive(_))
    ));
}

#[tokio::test]
async fn sync_health_recovers_only_after_the_committed_tip_reaches_object_storage() {
    let root = tempfile::tempdir().unwrap();
    let archive_root = root.path().join("archive");
    let archive_backup = root.path().join("archive-backup");
    let archive = initialized_checkpoint(&archive_root).await;
    let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let runtime = bound_runtime(root.path().join("node"));
    let committed = runtime.write("request-1", "alpha", "one").unwrap();
    coordinator.note_committed(committed.applied_index);
    std::fs::rename(&archive_root, &archive_backup).unwrap();
    std::fs::write(&archive_root, b"archive unavailable").unwrap();

    assert!(coordinator
        .flush_runtime(&runtime, committed.applied_index)
        .await
        .is_err());
    assert_eq!(coordinator.health(), DurabilityHealth::Unavailable);
    assert_eq!(coordinator.durable_tip().index(), 0);

    std::fs::remove_file(&archive_root).unwrap();
    std::fs::rename(&archive_backup, &archive_root).unwrap();
    coordinator
        .flush_runtime(&runtime, committed.applied_index)
        .await
        .unwrap();

    assert_eq!(coordinator.health(), DurabilityHealth::Available);
    assert_eq!(coordinator.durable_tip().index(), committed.applied_index);
    assert_eq!(
        archive
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .tip()
            .index(),
        committed.applied_index
    );
}

#[test]
fn recorder_rehydration_restores_command_bytes_before_installing_decision_proof() {
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime(root.path().join("source"));
    let committed = runtime.write("request-1", "alpha", "one").unwrap();
    let membership = runtime.consensus().membership().clone();
    let recorder = RecorderFileStore::new_with_membership(
        root.path().join("fresh-recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
        membership,
    )
    .unwrap();

    rehydrate_recorder_after_checkpoint(&runtime, &recorder, 0).unwrap();

    let entry = runtime
        .log_store()
        .read(committed.applied_index)
        .unwrap()
        .unwrap();
    let command = rhiza_core::StoredCommand::new(entry.entry_type, entry.payload);
    assert_eq!(
        recorder.fetch_command(command.hash()).unwrap(),
        Some(command)
    );
}

#[tokio::test]
async fn bounded_mode_blocks_after_lag_limit_and_flush_unblocks_writes() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(root.path()).await;
    let coordinator = CheckpointCoordinator::open(
        archive,
        DurabilityMode::Bounded {
            max_lag: Duration::from_millis(10),
        },
    )
    .await
    .unwrap();
    let runtime = runtime(root.path().join("node"));
    let committed = runtime.write("request-1", "alpha", "one").unwrap();

    coordinator.note_committed(committed.applied_index);
    assert!(coordinator.write_allowed().is_ok());
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(matches!(
        coordinator.write_allowed(),
        Err(DurabilityError::LagExceeded {
            committed_index: 1,
            durable_index: 0,
            ..
        })
    ));

    let tip = coordinator
        .flush_runtime(&runtime, committed.applied_index)
        .await
        .unwrap();
    assert_eq!(tip.index(), 1);
    assert!(coordinator.write_allowed().is_ok());
}

#[tokio::test]
async fn flush_resumes_after_anchor_when_checkpoint_is_durable_through_snapshot_tip() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("checkpoint")).await;
    let coordinator = CheckpointCoordinator::open(archive, DurabilityMode::Sync)
        .await
        .unwrap();
    let runtime = runtime(root.path().join("node"));
    let first = runtime.write("request-1", "alpha", "one").unwrap();
    coordinator
        .flush_runtime(&runtime, first.applied_index)
        .await
        .unwrap();

    let snapshot = runtime.create_recovery_snapshot().unwrap();
    let snapshot_store = ObjStore::new(ObjStoreConfig::Local {
        root: root.path().join("snapshots"),
    })
    .unwrap();
    let publication =
        ObjectArchiveStore::new_for_single_process(snapshot_store, "rhiza:sql:cluster-a")
            .publish_snapshot(snapshot.snapshot())
            .await
            .unwrap();
    let verified = runtime
        .verify_snapshot_publication(&snapshot, &publication)
        .unwrap();
    runtime.compact_log(&verified).unwrap();

    let second = runtime.write("request-2", "beta", "two").unwrap();
    assert_eq!(
        coordinator
            .flush_runtime(&runtime, second.applied_index)
            .await
            .unwrap()
            .index(),
        second.applied_index
    );
}

#[tokio::test]
async fn flush_fails_with_snapshot_requirement_when_checkpoint_is_below_anchor() {
    let root = tempfile::tempdir().unwrap();
    let coordinator = CheckpointCoordinator::open(
        initialized_checkpoint(&root.path().join("checkpoint")).await,
        DurabilityMode::Sync,
    )
    .await
    .unwrap();
    let runtime = runtime(root.path().join("node"));
    runtime.write("request-1", "alpha", "one").unwrap();
    let snapshot = runtime.create_recovery_snapshot().unwrap();
    let snapshot_store = ObjStore::new(ObjStoreConfig::Local {
        root: root.path().join("snapshots"),
    })
    .unwrap();
    let publication =
        ObjectArchiveStore::new_for_single_process(snapshot_store, "rhiza:sql:cluster-a")
            .publish_snapshot(snapshot.snapshot())
            .await
            .unwrap();
    let verified = runtime
        .verify_snapshot_publication(&snapshot, &publication)
        .unwrap();
    runtime.compact_log(&verified).unwrap();

    assert!(matches!(
        coordinator.flush_runtime(&runtime, u64::MAX).await,
        Err(DurabilityError::SnapshotRequired { anchor }) if *anchor == *snapshot.anchor()
    ));
}

#[tokio::test]
async fn bounded_mode_blocks_recovered_lag_immediately_but_gives_new_commits_the_window() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(root.path()).await;
    let coordinator = CheckpointCoordinator::open(
        archive,
        DurabilityMode::Bounded {
            max_lag: Duration::from_secs(1),
        },
    )
    .await
    .unwrap();
    let runtime = runtime(root.path().join("node"));
    let recovered = runtime.write("request-1", "alpha", "one").unwrap();

    coordinator.note_recovered_committed(recovered.applied_index);
    assert!(matches!(
        coordinator.write_allowed(),
        Err(DurabilityError::LagExceeded {
            committed_index: 1,
            durable_index: 0,
            ..
        })
    ));
    coordinator
        .flush_runtime(&runtime, recovered.applied_index)
        .await
        .unwrap();
    assert!(coordinator.write_allowed().is_ok());

    let fresh = runtime.write("request-2", "beta", "two").unwrap();
    coordinator.note_committed(fresh.applied_index);
    assert!(coordinator.write_allowed().is_ok());
}

#[tokio::test]
async fn concurrent_flushes_are_serialized_idempotent_and_clamped_to_local_qlog() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(root.path()).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let runtime = Arc::new(runtime(root.path().join("node")));
    for index in 1..=6 {
        let committed = runtime
            .write(
                &format!("request-{index}"),
                &format!("key-{index}"),
                &format!("value-{index}"),
            )
            .unwrap();
        coordinator.note_committed(committed.applied_index);
    }

    let (first, second) = tokio::join!(
        coordinator.flush_runtime(&runtime, 4),
        coordinator.flush_runtime(&runtime, u64::MAX)
    );
    first.unwrap();
    second.unwrap();
    coordinator.flush_runtime(&runtime, u64::MAX).await.unwrap();

    assert_eq!(coordinator.durable_tip().index(), 6);
    assert_eq!(archive.restore_checkpoint().await.unwrap().len(), 6);
    assert_eq!(
        archive
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .tip()
            .index(),
        6
    );
}

#[tokio::test]
async fn periodic_background_flushes_in_bounded_batches() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(root.path()).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(
            archive.clone(),
            DurabilityMode::Periodic {
                interval: Duration::from_millis(5),
            },
        )
        .await
        .unwrap(),
    );
    let runtime = Arc::new(runtime(root.path().join("node")));
    for index in 1..=40 {
        let committed = runtime
            .write(
                &format!("request-{index}"),
                &format!("key-{index}"),
                "value",
            )
            .unwrap();
        coordinator.note_committed(committed.applied_index);
    }

    coordinator
        .clone()
        .run_background(runtime, tokio::time::sleep(Duration::from_millis(40)))
        .await
        .unwrap();

    let loaded = archive.load_checkpoint().await.unwrap().unwrap();
    assert_eq!(loaded.manifest().tip().index(), 40);
    assert!(loaded.manifest().segments().len() > 1);

    let restored_dir = root.path().join("restored-batches");
    let tip = restore_checkpoint_to_fresh_data_dir(archive, &restored_dir)
        .await
        .unwrap();
    assert_eq!(tip.index(), 40);
    let restored_log = FileLogStore::open(
        restored_dir.join("consensus/log"),
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    assert_eq!(restored_log.last_index().unwrap(), Some(40));
}

#[tokio::test]
async fn background_checkpoint_recovers_after_transient_storage_failure() {
    for (name, mode) in [
        (
            "periodic",
            DurabilityMode::Periodic {
                interval: Duration::from_millis(5),
            },
        ),
        (
            "bounded",
            DurabilityMode::Bounded {
                max_lag: Duration::from_millis(20),
            },
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let archive_root = root.path().join(format!("{name}-archive"));
        let archive = initialized_checkpoint(&archive_root).await;
        let coordinator = Arc::new(
            CheckpointCoordinator::open(archive.clone(), mode)
                .await
                .unwrap(),
        );
        let runtime = Arc::new(runtime(root.path().join(format!("{name}-node"))));
        let committed = runtime.write("request-1", "alpha", "one").unwrap();
        coordinator.note_committed(committed.applied_index);
        let archive_backup = root.path().join(format!("{name}-archive-backup"));
        std::fs::rename(&archive_root, &archive_backup).unwrap();
        std::fs::write(&archive_root, b"archive unavailable").unwrap();

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let worker = tokio::spawn(coordinator.clone().run_background(runtime, async move {
            if !*shutdown_rx.borrow() {
                let _ = shutdown_rx.changed().await;
            }
        }));
        tokio::time::timeout(Duration::from_secs(1), async {
            while coordinator.health() != DurabilityHealth::Unavailable {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        std::fs::remove_file(&archive_root).unwrap();
        std::fs::rename(&archive_backup, &archive_root).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while coordinator.durable_tip().index() < committed.applied_index {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(coordinator.health(), DurabilityHealth::Available);
        assert!(coordinator.write_allowed().is_ok());
        assert_eq!(
            archive
                .load_checkpoint()
                .await
                .unwrap()
                .unwrap()
                .manifest()
                .tip()
                .index(),
            committed.applied_index
        );
        shutdown_tx.send(true).unwrap();
        worker.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn bounded_background_flushes_at_half_lag_and_sync_does_not_flush() {
    let root = tempfile::tempdir().unwrap();
    let bounded_archive = initialized_checkpoint(&root.path().join("bounded-archive")).await;
    let bounded = Arc::new(
        CheckpointCoordinator::open(
            bounded_archive.clone(),
            DurabilityMode::Bounded {
                max_lag: Duration::from_millis(20),
            },
        )
        .await
        .unwrap(),
    );
    let bounded_runtime = Arc::new(runtime(root.path().join("bounded-node")));
    let committed = bounded_runtime.write("request-1", "alpha", "one").unwrap();
    bounded.note_committed(committed.applied_index);
    let bounded_shutdown = {
        let bounded = bounded.clone();
        async move {
            while bounded.durable_tip().index() < committed.applied_index {
                tokio::task::yield_now().await;
            }
        }
    };
    tokio::time::timeout(
        Duration::from_secs(1),
        bounded.run_background(bounded_runtime, bounded_shutdown),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        bounded_archive
            .load_checkpoint()
            .await
            .unwrap()
            .unwrap()
            .manifest()
            .tip()
            .index(),
        1
    );

    let sync_archive = initialized_checkpoint(&root.path().join("sync-archive")).await;
    let sync = Arc::new(
        CheckpointCoordinator::open(sync_archive.clone(), DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let sync_runtime = Arc::new(runtime(root.path().join("sync-node")));
    let committed = sync_runtime.write("request-1", "alpha", "one").unwrap();
    sync.note_committed(committed.applied_index);
    sync.run_background(sync_runtime, std::future::pending())
        .await
        .unwrap();
    assert_eq!(
        sync_archive
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
async fn restore_requires_an_existing_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let archive = checkpoint_store(&root.path().join("archive"));
    let data_dir = root.path().join("data");

    assert!(matches!(
        restore_checkpoint_to_fresh_data_dir(archive, &data_dir).await,
        Err(DurabilityError::MissingCheckpoint)
    ));
    assert!(!data_dir.exists());
}

#[tokio::test]
async fn restore_rejects_existing_state_without_mutation() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let data_dir = root.path().join("data");
    let recorder = data_dir.join("consensus/recorder/node-1");
    std::fs::create_dir_all(&recorder).unwrap();
    let sentinel = recorder.join("state.bin");
    std::fs::write(&sentinel, b"keep-me").unwrap();

    assert!(matches!(
        restore_checkpoint_to_fresh_data_dir(archive, &data_dir).await,
        Err(DurabilityError::DataDirNotFresh(_))
    ));
    assert_eq!(std::fs::read(&sentinel).unwrap(), b"keep-me");
    assert!(!data_dir.join("consensus/log").exists());
    assert!(!data_dir.join("sqlite").exists());
}

#[tokio::test]
async fn restore_roundtrip_replays_normally_through_node_runtime() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let source = runtime(root.path().join("source"));
    let first = source.write("request-1", "alpha", "one").unwrap();
    let second = source.write("request-2", "beta", "two").unwrap();
    coordinator.note_committed(second.applied_index);
    coordinator
        .flush_runtime(&source, second.applied_index)
        .await
        .unwrap();
    source.checkpoint_compact(&coordinator).await.unwrap();
    drop(source);

    let restored_dir = root.path().join("restored");
    let tip =
        restore_checkpoint_to_fresh_data_dir_for_node(archive.clone(), &restored_dir, "node-1")
            .await
            .unwrap();
    assert_eq!(tip.index(), second.applied_index);
    assert_eq!(tip.hash(), second.hash);
    assert_ne!(tip.hash(), first.hash);

    let restored = runtime(restored_dir);
    assert_eq!(restored.applied_index().unwrap(), 2);
    assert_eq!(
        restored
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("one")
    );
    assert_eq!(
        restored
            .read("beta", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("two")
    );

    let other_node_dir = root.path().join("restored-node-2");
    restore_checkpoint_to_fresh_data_dir_for_node(archive, &other_node_dir, "node-2")
        .await
        .unwrap();
    let other = SqliteStateMachine::open_with_configuration(
        other_node_dir.join("sqlite/db.sqlite"),
        "rhiza:sql:cluster-a",
        "node-2",
        1,
        ConfigurationState::active(
            1,
            Membership::new(["node-1", "node-2", "node-3"])
                .unwrap()
                .digest(),
        ),
    )
    .unwrap();
    assert_eq!(other.applied_index_value().unwrap(), second.applied_index);
}

#[tokio::test]
async fn empty_initialized_checkpoint_restores_as_genesis() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let data_dir = root.path().join("data");

    let tip = restore_checkpoint_to_fresh_data_dir(archive, &data_dir)
        .await
        .unwrap();

    assert_eq!(tip.index(), 0);
    assert_eq!(tip.hash(), LogHash::ZERO);
    assert!(!data_dir.join("consensus/log").exists());
}

#[tokio::test]
async fn restore_preserves_an_existing_empty_data_directory() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let data_dir = root.path().join("mounted-data");
    std::fs::create_dir(&data_dir).unwrap();

    let before = std::fs::metadata(&data_dir).unwrap();
    restore_checkpoint_to_fresh_data_dir(archive, &data_dir)
        .await
        .unwrap();
    let after = std::fs::metadata(&data_dir).unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(before.dev(), after.dev());
        assert_eq!(before.ino(), after.ino());
    }
    assert!(std::fs::read_dir(&data_dir).unwrap().next().is_none());
}

#[tokio::test]
async fn unsupported_restore_artifact_fails_without_mutation() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let data_dir = root.path().join("mounted-data");
    std::fs::create_dir_all(data_dir.join("consensus/partial")).unwrap();
    std::fs::create_dir(data_dir.join(".restore-stage-foreign")).unwrap();
    std::fs::write(
        data_dir.join(".rhiza-restore-unsupported"),
        b"unsupported restore artifact\n",
    )
    .unwrap();

    let before = std::fs::read(data_dir.join(".rhiza-restore-unsupported")).unwrap();
    assert!(restore_checkpoint_to_fresh_data_dir(archive, &data_dir)
        .await
        .is_err());
    assert_eq!(
        std::fs::read(data_dir.join(".rhiza-restore-unsupported")).unwrap(),
        before
    );
    assert!(data_dir.join("consensus/partial").is_dir());
    assert!(data_dir.join(".restore-stage-foreign").is_dir());
}

#[tokio::test]
async fn checkpoint_compact_publishes_canonical_snapshot_with_exact_suffix() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_checkpoint(&root.path().join("archive")).await;
    let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let source = runtime(root.path().join("node"));
    let first = source.write("request-1", "alpha", "one").unwrap();
    coordinator
        .flush_runtime(&source, first.applied_index)
        .await
        .unwrap();
    let anchor = source.checkpoint_compact(&coordinator).await.unwrap();
    let local = source.log_store().logical_state().unwrap();
    assert_eq!(anchor.format_version(), 2);
    assert_eq!(local.anchor, Some(anchor.clone()));
    assert!(source
        .log_store()
        .read(first.applied_index)
        .unwrap()
        .is_none());

    let second = source.write("request-2", "beta", "two").unwrap();
    coordinator
        .flush_runtime(&source, second.applied_index)
        .await
        .unwrap();
    let restored_dir = root.path().join("restored");
    let tip =
        restore_checkpoint_to_fresh_data_dir_for_node(archive.clone(), &restored_dir, "node-1")
            .await
            .unwrap();
    assert_eq!(tip.index(), second.applied_index);
    let restored_checkpoint = archive.restore_checkpoint_state().await.unwrap();
    assert_eq!(restored_checkpoint.snapshot().unwrap().anchor(), &anchor);
    assert_eq!(restored_checkpoint.suffix().len(), 1);
    assert_eq!(restored_checkpoint.suffix()[0].index, second.applied_index);

    let restored = runtime(restored_dir);
    assert_eq!(
        restored
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("one")
    );
    assert_eq!(
        restored
            .read("beta", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("two")
    );
}

#[cfg(feature = "graph")]
#[tokio::test]
async fn graph_checkpoint_restores_snapshot_and_exact_suffix_to_a_fresh_other_node() {
    let root = tempfile::tempdir().unwrap();
    let archive =
        initialized_profile_checkpoint(&root.path().join("archive"), ExecutionProfile::Graph).await;
    let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let source = profile_runtime(root.path(), "source", "n1", ExecutionProfile::Graph);
    let first = source
        .mutate_graph(
            GraphCommandV1::put_document("request-1", "first", GraphValueV1::U64(1)).unwrap(),
        )
        .unwrap();
    coordinator
        .flush_runtime(&source, first.applied_index())
        .await
        .unwrap();
    let anchor = source.checkpoint_compact(&coordinator).await.unwrap();
    let second = source
        .mutate_graph(
            GraphCommandV1::put_document("request-2", "second", GraphValueV1::U64(2)).unwrap(),
        )
        .unwrap();
    coordinator
        .flush_runtime(&source, second.applied_index())
        .await
        .unwrap();

    let remote = archive.restore_checkpoint_state().await.unwrap();
    assert_eq!(remote.snapshot().unwrap().anchor(), &anchor);
    assert_eq!(remote.suffix().len(), 1);
    assert_eq!(remote.suffix()[0].index, second.applied_index());

    let restored_dir = root.path().join("restored");
    restore_checkpoint_to_fresh_data_dir_for_node(archive.clone(), &restored_dir, "n2")
        .await
        .unwrap();
    assert!(restored_dir.join("ladybug/graph.lbug").is_file());
    let restored = profile_runtime_at(
        root.path(),
        "restored-recorders",
        restored_dir.clone(),
        "n2",
        ExecutionProfile::Graph,
    );
    assert_eq!(
        restored
            .get_graph_document("first", ReadConsistency::Local)
            .unwrap()
            .value,
        Some(GraphValueV1::U64(1))
    );
    assert_eq!(
        restored
            .get_graph_document("second", ReadConsistency::Local)
            .unwrap()
            .value,
        Some(GraphValueV1::U64(2))
    );
    drop(restored);
    assert!(matches!(
        restore_checkpoint_to_fresh_data_dir_for_node(archive, &restored_dir, "n2").await,
        Err(DurabilityError::DataDirNotFresh(_))
    ));
}

#[cfg(feature = "kv")]
#[tokio::test]
async fn kv_checkpoint_restores_snapshot_and_exact_suffix_to_a_fresh_other_node() {
    let root = tempfile::tempdir().unwrap();
    let archive =
        initialized_profile_checkpoint(&root.path().join("archive"), ExecutionProfile::Kv).await;
    let coordinator = CheckpointCoordinator::open(archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let source = profile_runtime(root.path(), "source", "n1", ExecutionProfile::Kv);
    let first = source
        .mutate_kv(KvCommandV1::put("request-1", b"first".to_vec(), b"one".to_vec()).unwrap())
        .unwrap();
    coordinator
        .flush_runtime(&source, first.applied_index())
        .await
        .unwrap();
    let anchor = source.checkpoint_compact(&coordinator).await.unwrap();
    let second = source
        .mutate_kv(KvCommandV1::put("request-2", b"second".to_vec(), b"two".to_vec()).unwrap())
        .unwrap();
    coordinator
        .flush_runtime(&source, second.applied_index())
        .await
        .unwrap();

    let remote = archive.restore_checkpoint_state().await.unwrap();
    assert_eq!(remote.snapshot().unwrap().anchor(), &anchor);
    assert_eq!(remote.suffix().len(), 1);
    assert_eq!(remote.suffix()[0].index, second.applied_index());

    let restored_dir = root.path().join("restored");
    restore_checkpoint_to_fresh_data_dir_for_node(archive.clone(), &restored_dir, "n2")
        .await
        .unwrap();
    assert!(restored_dir.join("kv/data.redb").is_file());
    let restored = profile_runtime_at(
        root.path(),
        "restored-recorders",
        restored_dir.clone(),
        "n2",
        ExecutionProfile::Kv,
    );
    assert_eq!(
        restored
            .get_kv(b"first", ReadConsistency::Local)
            .unwrap()
            .value,
        Some(b"one".to_vec())
    );
    assert_eq!(
        restored
            .get_kv(b"second", ReadConsistency::Local)
            .unwrap()
            .value,
        Some(b"two".to_vec())
    );
    drop(restored);
    assert!(matches!(
        restore_checkpoint_to_fresh_data_dir_for_node(archive, &restored_dir, "n2").await,
        Err(DurabilityError::DataDirNotFresh(_))
    ));
}

#[tokio::test]
async fn failed_snapshot_publication_leaves_local_qlog_prefix_intact() {
    let root = tempfile::tempdir().unwrap();
    let archive_root = root.path().join("archive");
    let archive = initialized_checkpoint(&archive_root).await;
    let coordinator = CheckpointCoordinator::open(archive, DurabilityMode::Sync)
        .await
        .unwrap();
    let runtime = runtime(root.path().join("node"));
    let committed = runtime.write("request-1", "alpha", "one").unwrap();
    coordinator
        .flush_runtime(&runtime, committed.applied_index)
        .await
        .unwrap();
    std::fs::remove_dir_all(&archive_root).unwrap();
    std::fs::write(&archive_root, b"publication blocked").unwrap();

    assert!(runtime.checkpoint_compact(&coordinator).await.is_err());
    let local = runtime.log_store().logical_state().unwrap();
    assert!(local.anchor.is_none());
    assert!(runtime
        .log_store()
        .read(committed.applied_index)
        .unwrap()
        .is_some());
}

async fn initialized_checkpoint(root: &Path) -> ObjectArchiveStore {
    let archive = checkpoint_store(root);
    archive.initialize_checkpoint().await.unwrap();
    archive
}

#[cfg(any(feature = "graph", feature = "kv"))]
async fn initialized_profile_checkpoint(
    root: &Path,
    profile: ExecutionProfile,
) -> ObjectArchiveStore {
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.to_path_buf(),
    })
    .unwrap();
    let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new(effective_cluster_id(profile, "cluster-a").unwrap(), 1, 1, 1),
    );
    archive.initialize_checkpoint().await.unwrap();
    archive
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn profile_runtime(
    root: &Path,
    name: &str,
    node_id: &str,
    profile: ExecutionProfile,
) -> NodeRuntime {
    profile_runtime_at(
        root,
        &format!("{name}-recorders"),
        root.join(name),
        node_id,
        profile,
    )
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn profile_runtime_at(
    root: &Path,
    recorder_name: &str,
    data_dir: std::path::PathBuf,
    node_id: &str,
    profile: ExecutionProfile,
) -> NodeRuntime {
    let cluster_id = effective_cluster_id(profile, "cluster-a").unwrap();
    let config = NodeConfig::new_embedded("cluster-a", node_id, data_dir, 1, 1, ["n1", "n2", "n3"])
        .unwrap()
        .with_execution_profile(profile)
        .unwrap();
    let recorder_root = root.join(recorder_name);
    NodeRuntime::open(
        config,
        Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                cluster_id,
                node_id,
                1,
                1,
                [
                    recorder_root.join("n1"),
                    recorder_root.join("n2"),
                    recorder_root.join("n3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        ),
        &[],
    )
    .unwrap()
}

fn checkpoint_store(root: &Path) -> ObjectArchiveStore {
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.to_path_buf(),
    })
    .unwrap();
    ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 1),
    )
}

fn runtime(data_dir: impl AsRef<Path>) -> NodeRuntime {
    let data_dir = data_dir.as_ref().to_path_buf();
    let consensus_root = data_dir.parent().unwrap_or(&data_dir).join(format!(
        "{}-recorders",
        data_dir.file_name().unwrap().to_string_lossy()
    ));
    NodeRuntime::open(
        NodeConfig::new(
            "rhiza:sql:cluster-a",
            "node-1",
            data_dir,
            1,
            1,
            [
                PeerConfig::new("node-1", "http://node-1", "peer-token-1").unwrap(),
                PeerConfig::new("node-2", "http://node-2", "peer-token-2").unwrap(),
                PeerConfig::new("node-3", "http://node-3", "peer-token-3").unwrap(),
            ],
            "client-token",
        )
        .unwrap(),
        Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                "rhiza:sql:cluster-a",
                "node-1",
                1,
                1,
                [
                    consensus_root.join("node-1"),
                    consensus_root.join("node-2"),
                    consensus_root.join("node-3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        ),
        &[],
    )
    .unwrap()
}

fn bound_runtime(data_dir: impl AsRef<Path>) -> NodeRuntime {
    let data_dir = data_dir.as_ref().to_path_buf();
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let recorder_root = data_dir.parent().unwrap().join("bound-recorders");
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            let recorder = RecorderFileStore::new_with_membership(
                recorder_root.join(id),
                id.clone(),
                "rhiza:sql:cluster-a",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>)
        })
        .collect();
    NodeRuntime::open(
        NodeConfig::new(
            "rhiza:sql:cluster-a",
            "node-1",
            data_dir,
            1,
            1,
            [
                PeerConfig::new("node-1", "http://node-1", "peer-token-1").unwrap(),
                PeerConfig::new("node-2", "http://node-2", "peer-token-2").unwrap(),
                PeerConfig::new("node-3", "http://node-3", "peer-token-3").unwrap(),
            ],
            "client-token",
        )
        .unwrap(),
        Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "rhiza:sql:cluster-a",
                "node-1",
                1,
                1,
                recorders,
            )
            .unwrap(),
        ),
        &[],
    )
    .unwrap()
}
