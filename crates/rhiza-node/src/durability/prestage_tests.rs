use super::{
    inspect_successor_prestage, prestage_successor_checkpoint, publish_successor_prestage,
    DurabilityError, SuccessorPrestageState, SUCCESSOR_PRESTAGE_INTENT_FILE,
    SUCCESSOR_PRESTAGE_READY_FILE,
};
use crate::{NodeConfig, PeerConfig, StopInformation};
use rhiza_archive::{
    CheckpointIdentity, CheckpointPublisherOptions, CheckpointTip, ObjectArchiveStore,
};
use rhiza_core::{
    ConfigChange, ConfigurationState, EntryType, ExecutionProfile, LogEntry, LogHash, StopBinding,
};
use rhiza_log::{FileLogStore, LogStore};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{
    AcceptedValue, DecisionProof, Membership, Proposal, ProposalPriority, RecorderSummary,
};

async fn initialized_archive(root: &std::path::Path) -> ObjectArchiveStore {
    let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        ObjStore::new(ObjStoreConfig::Local {
            root: root.to_path_buf(),
        })
        .unwrap(),
        CheckpointIdentity::new("rhiza:sql:cluster-a", 9, 4, 7),
    );
    archive.initialize_checkpoint().await.unwrap();
    archive
}

fn predecessor_configuration() -> ConfigurationState {
    ConfigurationState::active(
        4,
        Membership::new(["old-1", "old-2", "old-3"])
            .unwrap()
            .digest(),
    )
}

#[tokio::test]
async fn prestage_marker_binds_the_seed_and_exact_successor_target() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_archive(&root.path().join("archive")).await;
    let prestage_dir = root.path().join("prestage");
    let membership_digest = LogHash::digest(&[b"successor membership"]);

    let prestage = prestage_successor_checkpoint(
        archive.clone(),
        &prestage_dir,
        predecessor_configuration(),
        "node-2",
        ExecutionProfile::Sqlite,
        5,
        membership_digest,
    )
    .await
    .unwrap();

    assert_eq!(prestage.state(), SuccessorPrestageState::Ready);
    assert_eq!(prestage.identity().cluster_id(), "rhiza:sql:cluster-a");
    assert_eq!(prestage.identity().epoch(), 9);
    assert_eq!(prestage.identity().predecessor_config_id(), 4);
    assert_eq!(prestage.identity().predecessor_recovery_generation(), 7);
    assert_eq!(prestage.identity().node_id(), "node-2");
    assert_eq!(
        prestage.identity().execution_profile(),
        ExecutionProfile::Sqlite
    );
    assert_eq!(prestage.identity().target_config_id(), 5);
    assert_eq!(
        prestage.identity().target_membership_digest(),
        membership_digest
    );
    assert_eq!(
        prestage.identity().seed_anchor(),
        rhiza_core::LogAnchor::new(0, LogHash::ZERO)
    );

    let marker: serde_json::Value = serde_json::from_slice(
        &std::fs::read(prestage_dir.join(SUCCESSOR_PRESTAGE_READY_FILE)).unwrap(),
    )
    .unwrap();
    assert_eq!(marker["cluster_id"], "rhiza:sql:cluster-a");
    assert_eq!(marker["epoch"], 9);
    assert_eq!(marker["predecessor_config_id"], 4);
    assert_eq!(
        marker["predecessor_membership_digest"],
        predecessor_configuration().digest().to_hex()
    );
    assert_eq!(marker["predecessor_recovery_generation"], 7);
    assert_eq!(marker["node_id"], "node-2");
    assert_eq!(marker["execution_profile"], "sql");
    assert_eq!(marker["target_config_id"], 5);
    assert_eq!(
        marker["target_membership_digest"],
        membership_digest.to_hex()
    );
    assert_eq!(marker["seed_index"], 0);
    assert_eq!(marker["seed_hash"], LogHash::ZERO.to_hex());

    let expected_identity = prestage.identity().clone();
    drop(prestage);
    assert!(inspect_successor_prestage(
        &prestage_dir,
        ConfigurationState::active(4, LogHash::digest(&[b"foreign predecessor membership"])),
    )
    .is_err());
    let resumed = prestage_successor_checkpoint(
        archive,
        &prestage_dir,
        predecessor_configuration(),
        "node-2",
        ExecutionProfile::Sqlite,
        5,
        membership_digest,
    )
    .await
    .unwrap();
    assert_eq!(resumed.identity(), &expected_identity);
}

#[tokio::test]
async fn prestage_rejects_checkpoint_drift_without_rebinding_its_seed() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_archive(&root.path().join("archive")).await;
    let prestage_dir = root.path().join("prestage");
    let membership_digest = LogHash::digest(&[b"successor membership"]);
    let prestage = prestage_successor_checkpoint(
        archive.clone(),
        &prestage_dir,
        predecessor_configuration(),
        "node-2",
        ExecutionProfile::Sqlite,
        5,
        membership_digest,
    )
    .await
    .unwrap();
    let bound_seed = prestage.identity().seed_anchor();
    drop(prestage);

    let publisher = archive
        .open_checkpoint_publisher("drift-test", CheckpointPublisherOptions::default())
        .await
        .unwrap();
    let hash = LogEntry::calculate_hash(
        "rhiza:sql:cluster-a",
        1,
        9,
        4,
        EntryType::Noop,
        LogHash::ZERO,
        &[],
    );
    publisher
        .publish_committed(&[LogEntry {
            cluster_id: "rhiza:sql:cluster-a".into(),
            epoch: 9,
            config_id: 4,
            index: 1,
            entry_type: EntryType::Noop,
            payload: Vec::new(),
            prev_hash: LogHash::ZERO,
            hash,
        }])
        .await
        .unwrap();

    assert!(matches!(
        prestage_successor_checkpoint(
            archive,
            &prestage_dir,
            predecessor_configuration(),
            "node-2",
            ExecutionProfile::Sqlite,
            5,
            membership_digest,
        )
        .await,
        Err(DurabilityError::DataDirNotFresh(path)) if path == prestage_dir
    ));
    assert_eq!(
        inspect_successor_prestage(&prestage_dir, predecessor_configuration())
            .unwrap()
            .identity()
            .seed_anchor(),
        bound_seed
    );
}

#[tokio::test]
async fn prestage_copies_the_active_checkpoint_and_rebuilds_an_exact_interruption() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_archive(&root.path().join("archive")).await;
    let publisher = archive
        .open_checkpoint_publisher("prestage-test", CheckpointPublisherOptions::default())
        .await
        .unwrap();
    let hash = LogEntry::calculate_hash(
        "rhiza:sql:cluster-a",
        1,
        9,
        4,
        EntryType::Noop,
        LogHash::ZERO,
        &[],
    );
    publisher
        .publish_committed(&[LogEntry {
            cluster_id: "rhiza:sql:cluster-a".into(),
            epoch: 9,
            config_id: 4,
            index: 1,
            entry_type: EntryType::Noop,
            payload: Vec::new(),
            prev_hash: LogHash::ZERO,
            hash,
        }])
        .await
        .unwrap();
    let prestage_dir = root.path().join("prestage");
    let membership_digest = LogHash::digest(&[b"successor membership"]);

    let prestage = prestage_successor_checkpoint(
        archive.clone(),
        &prestage_dir,
        predecessor_configuration(),
        "node-2",
        ExecutionProfile::Sqlite,
        5,
        membership_digest,
    )
    .await
    .unwrap();
    assert_eq!(
        prestage.identity().seed_anchor(),
        rhiza_core::LogAnchor::new(1, hash)
    );
    let log = FileLogStore::open(
        prestage_dir.join("consensus/log"),
        "rhiza:sql:cluster-a",
        9,
        4,
    )
    .unwrap();
    assert_eq!(log.logical_state().unwrap().tip.unwrap().hash(), hash);
    drop(log);
    drop(prestage);

    std::fs::rename(
        prestage_dir.join(SUCCESSOR_PRESTAGE_READY_FILE),
        prestage_dir.join(SUCCESSOR_PRESTAGE_INTENT_FILE),
    )
    .unwrap();
    let resumed = prestage_successor_checkpoint(
        archive,
        &prestage_dir,
        predecessor_configuration(),
        "node-2",
        ExecutionProfile::Sqlite,
        5,
        membership_digest,
    )
    .await
    .unwrap();
    assert_eq!(resumed.state(), SuccessorPrestageState::Ready);
    assert_eq!(
        resumed.identity().seed_anchor(),
        rhiza_core::LogAnchor::new(1, hash)
    );

    let target_dir = root.path().join("node");
    let published = publish_successor_prestage(resumed, &target_dir).unwrap();
    let final_hash =
        LogEntry::calculate_hash("rhiza:sql:cluster-a", 2, 9, 4, EntryType::Noop, hash, &[]);
    let log = FileLogStore::open(
        target_dir.join("consensus/log"),
        "rhiza:sql:cluster-a",
        9,
        4,
    )
    .unwrap();
    log.append_batch(&[LogEntry {
        cluster_id: "rhiza:sql:cluster-a".into(),
        epoch: 9,
        config_id: 4,
        index: 2,
        entry_type: EntryType::Noop,
        payload: Vec::new(),
        prev_hash: hash,
        hash: final_hash,
    }])
    .unwrap();
    assert_eq!(log.logical_state().unwrap().tip.unwrap().hash(), final_hash);
    drop(log);
    assert_eq!(published.state(), SuccessorPrestageState::Published);
}

#[tokio::test]
async fn prestage_rejects_foreign_or_ownerless_artifacts_without_deleting_them() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_archive(&root.path().join("archive")).await;
    let prestage_dir = root.path().join("prestage");
    let membership_digest = LogHash::digest(&[b"successor membership"]);
    prestage_successor_checkpoint(
        archive,
        &prestage_dir,
        predecessor_configuration(),
        "node-2",
        ExecutionProfile::Sqlite,
        5,
        membership_digest,
    )
    .await
    .unwrap();
    let ownerless = prestage_dir.join(".restore-stage-4242-0");
    std::fs::create_dir(&ownerless).unwrap();
    std::fs::write(ownerless.join("keep"), b"foreign").unwrap();

    assert!(matches!(
        inspect_successor_prestage(&prestage_dir, predecessor_configuration()),
        Err(DurabilityError::DataDirNotFresh(path)) if path == prestage_dir
    ));
    assert_eq!(std::fs::read(ownerless.join("keep")).unwrap(), b"foreign");
}

#[tokio::test]
async fn publish_is_exact_and_crash_resumable() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_archive(&root.path().join("archive")).await;
    let prestage_dir = root.path().join("prestage");
    let target_dir = root.path().join("node");
    let membership_digest = LogHash::digest(&[b"successor membership"]);
    let prestage = prestage_successor_checkpoint(
        archive,
        &prestage_dir,
        predecessor_configuration(),
        "node-2",
        ExecutionProfile::Sqlite,
        5,
        membership_digest,
    )
    .await
    .unwrap();

    let published = publish_successor_prestage(prestage, &target_dir).unwrap();
    assert!(!prestage_dir.exists());
    assert_eq!(published.state(), SuccessorPrestageState::Published);
    drop(published);
    assert_eq!(
        inspect_successor_prestage(&target_dir, predecessor_configuration())
            .unwrap()
            .state(),
        SuccessorPrestageState::Published
    );

    let inspected = inspect_successor_prestage(&target_dir, predecessor_configuration()).unwrap();
    let resumed = publish_successor_prestage(inspected, &target_dir).unwrap();
    assert_eq!(resumed.state(), SuccessorPrestageState::Published);
}

#[tokio::test]
async fn publish_preserves_exact_conflicts() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_archive(&root.path().join("archive")).await;
    let prestage_dir = root.path().join("prestage");
    let target_dir = root.path().join("node");
    let membership_digest = LogHash::digest(&[b"successor membership"]);
    let prestage = prestage_successor_checkpoint(
        archive,
        &prestage_dir,
        predecessor_configuration(),
        "node-2",
        ExecutionProfile::Sqlite,
        5,
        membership_digest,
    )
    .await
    .unwrap();
    std::fs::create_dir(&target_dir).unwrap();
    std::fs::write(target_dir.join("foreign"), b"keep").unwrap();

    let prestage = publish_successor_prestage(prestage, &target_dir).unwrap_err();
    assert!(matches!(prestage, DurabilityError::DataDirNotFresh(path) if path == target_dir));
    assert_eq!(std::fs::read(target_dir.join("foreign")).unwrap(), b"keep");
    assert!(prestage_dir.exists());

    std::fs::remove_dir_all(&target_dir).unwrap();
    let published = publish_successor_prestage(
        inspect_successor_prestage(&prestage_dir, predecessor_configuration()).unwrap(),
        &target_dir,
    )
    .unwrap();
    assert_eq!(published.state(), SuccessorPrestageState::Published);
    drop(published);
    assert_eq!(
        inspect_successor_prestage(&target_dir, predecessor_configuration())
            .unwrap()
            .state(),
        SuccessorPrestageState::Published
    );
    assert!(!target_dir.join(SUCCESSOR_PRESTAGE_INTENT_FILE).exists());
}

#[tokio::test]
async fn finalized_prestage_adopts_the_existing_successor_receipt_without_recopying() {
    let root = tempfile::tempdir().unwrap();
    let archive = initialized_archive(&root.path().join("archive")).await;
    let prestage_dir = root.path().join("prestage");
    let data_dir = root.path().join("node");
    let predecessor = Membership::new(["old-1", "old-2", "old-3"]).unwrap();
    let predecessor_digest = predecessor.digest();
    let successor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let prestage = prestage_successor_checkpoint(
        archive,
        &prestage_dir,
        ConfigurationState::active(4, predecessor_digest),
        "node-2",
        ExecutionProfile::Sqlite,
        5,
        successor.digest(),
    )
    .await
    .unwrap();
    let published = publish_successor_prestage(prestage, &data_dir).unwrap();
    let change = ConfigChange::bound_stop(
        "rhiza:sql:cluster-a",
        4,
        predecessor_digest,
        5,
        successor.members().to_vec(),
    )
    .unwrap();
    let successor_descriptor = change.successor().unwrap().clone();
    let command = change.to_stored_command();
    let hash = LogEntry::calculate_hash(
        "rhiza:sql:cluster-a",
        1,
        9,
        4,
        command.entry_type,
        LogHash::ZERO,
        &command.payload,
    );
    let entry = LogEntry {
        cluster_id: "rhiza:sql:cluster-a".into(),
        epoch: 9,
        config_id: 4,
        index: 1,
        entry_type: command.entry_type,
        payload: command.payload.clone(),
        prev_hash: LogHash::ZERO,
        hash,
    };
    let value =
        AcceptedValue::from_command("rhiza:sql:cluster-a", 1, 9, 4, LogHash::ZERO, &command);
    let proposal = Proposal::new(ProposalPriority::MAX, "old-1", 1, value);
    let summaries = predecessor
        .members()
        .iter()
        .take(predecessor.quorum_size())
        .map(|node_id| RecorderSummary {
            recorder_id: node_id.clone(),
            slot: 1,
            step: 4,
            first_current: Some(proposal.clone()),
            aggregate_prior: None,
        })
        .collect();
    let stop = StopInformation {
        entry: entry.clone(),
        proof: DecisionProof::FastPath {
            cluster_id: "rhiza:sql:cluster-a".into(),
            slot: 1,
            epoch: 9,
            config_id: 4,
            config_digest: predecessor_digest,
            proposal,
            summaries,
        },
    };
    let mut forged_stop = stop.clone();
    match &mut forged_stop.proof {
        DecisionProof::FastPath { summaries, .. } | DecisionProof::Phase2 { summaries, .. } => {
            summaries.truncate(1)
        }
    }
    assert!(super::validate_successor_prestage_stop(
        published.identity(),
        &forged_stop,
        &predecessor,
    )
    .is_err());
    let mut duplicate_stop = stop.clone();
    match &mut duplicate_stop.proof {
        DecisionProof::FastPath { summaries, .. } | DecisionProof::Phase2 { summaries, .. } => {
            summaries[1] = summaries[0].clone()
        }
    }
    assert!(super::validate_successor_prestage_stop(
        published.identity(),
        &duplicate_stop,
        &predecessor,
    )
    .is_err());
    let log = FileLogStore::open_with_configuration(
        data_dir.join("consensus/log"),
        "rhiza:sql:cluster-a",
        9,
        ConfigurationState::active(4, predecessor_digest),
    )
    .unwrap();
    log.append_batch(std::slice::from_ref(&entry)).unwrap();
    drop(log);
    let finalized =
        super::finalize_successor_prestage_for_stop(published, &stop, &predecessor).unwrap();
    drop(finalized);
    let finalized = super::finalize_successor_prestage_for_stop(
        inspect_successor_prestage(&data_dir, ConfigurationState::active(4, predecessor_digest))
            .unwrap(),
        &stop,
        &predecessor,
    )
    .unwrap();
    assert_eq!(finalized.state(), SuccessorPrestageState::Finalized);
    let config = NodeConfig::new_with_configuration(
        "rhiza:sql:cluster-a",
        "node-2",
        data_dir.clone(),
        9,
        successor.clone(),
        ConfigurationState::stopped(
            4,
            predecessor_digest,
            rhiza_core::LogAnchor::new(1, hash),
            StopBinding::Bound {
                successor: successor_descriptor,
                stop_command_hash: command.hash(),
            },
        ),
        [
            PeerConfig::new("node-1", "http://node-1", "peer-1").unwrap(),
            PeerConfig::new("node-2", "http://node-2", "peer-2").unwrap(),
            PeerConfig::new("node-3", "http://node-3", "peer-3").unwrap(),
        ],
        "client-token",
    )
    .unwrap()
    .with_log_initial_configuration(ConfigurationState::active(4, predecessor_digest))
    .with_predecessor_stop_entry(entry)
    .with_recovery_generation(7)
    .unwrap();

    let receipt = serde_json::to_vec(&super::SuccessorRestoreIdentity {
        cluster_id: "rhiza:sql:cluster-a",
        epoch: 9,
        target_config_id: 5,
        recovery_generation: 7,
        node_id: "node-2",
        membership_digest: successor.digest().to_hex(),
        predecessor_config_id: 4,
        stop_index: 1,
        stop_hash: hash.to_hex(),
    })
    .unwrap();
    let mut foreign_receipt: serde_json::Value = serde_json::from_slice(&receipt).unwrap();
    foreign_receipt["target_config_id"] = serde_json::json!(6);
    std::fs::write(
        data_dir.join(super::SUCCESSOR_RESTORE_INTENT_FILE),
        serde_json::to_vec(&foreign_receipt).unwrap(),
    )
    .unwrap();
    std::fs::write(data_dir.join(super::SUCCESSOR_RESTORE_LOCK_FILE), []).unwrap();
    drop(finalized);
    assert!(matches!(
        inspect_successor_prestage(
            &data_dir,
            ConfigurationState::active(4, predecessor_digest),
        ),
        Err(DurabilityError::DataDirNotFresh(path)) if path == data_dir
    ));
    assert!(data_dir
        .join(super::SUCCESSOR_PRESTAGE_FINALIZED_FILE)
        .is_file());
    std::fs::write(
        data_dir.join(super::SUCCESSOR_RESTORE_INTENT_FILE),
        &receipt,
    )
    .unwrap();
    let resumed =
        inspect_successor_prestage(&data_dir, ConfigurationState::active(4, predecessor_digest))
            .unwrap();
    assert_eq!(resumed.state(), SuccessorPrestageState::Finalized);
    let preparation =
        super::adopt_finalized_successor_prestage(resumed, &config, &stop, &predecessor).unwrap();
    assert!(preparation.requires_recorder_install());
    assert_eq!(preparation.tip(), CheckpointTip::new(1, hash));
    assert!(data_dir.join("consensus/log").is_dir());
    assert!(!data_dir
        .join(super::SUCCESSOR_PRESTAGE_FINALIZED_FILE)
        .exists());
    assert!(data_dir
        .join(super::SUCCESSOR_RESTORE_INTENT_FILE)
        .is_file());
    drop(preparation);
    super::complete_adopted_successor_prestage(&data_dir, &receipt).unwrap();
    let complete_receipt =
        std::fs::read(data_dir.join(super::SUCCESSOR_RESTORE_COMPLETE_FILE)).unwrap();
    assert_eq!(complete_receipt, receipt);
    assert!(super::parse_successor_restore_receipt(&complete_receipt).is_some());
    assert!(!data_dir.join(super::SUCCESSOR_RESTORE_INTENT_FILE).exists());
}
